//! A key's run in one ordered file (RFD 025, Cost): the rows the key holds
//! in each row group its run spans, found from the resident first and last
//! key of each row group and the fences, with at most two key pages
//! decoded per row group; and the rows one Scan reads of it, the order
//! column and the requested value columns over the candidate rows that the
//! order fences place, in one round. A key-value table never reaches this
//! module.

use std::ops::Range;
use std::sync::Arc;

use bytes::Bytes;
use futures::FutureExt as _;
use futures::future::OptionFuture;
use metrics::Counter;
use tokio::sync::Semaphore;

use super::build::{ValueRows, checked_values, decode_run};
use super::order_fences::Pair;
use super::ordered::OrderRun;
use super::page_header::check_order_pages;
use super::{
    ColumnChunk, DeletedRows, FileFault, FileIndex, GroupRange, KeyResidency, OrderLeaf,
    PageSource, RowRef, file_fault, row_index, row_offset,
};
use crate::cache::{Issue, PageCache, PageOrigin};
use crate::error::{Error, Result};
use crate::key::Key;
use crate::local::DiskChunks;
use crate::order::{Direction, OrderValue, ScanBounds};
use crate::parquet_io::Segments;
use crate::scan::{FileAnswer, ScanRow};
use crate::storage::HedgeWindow;

/// The rows of a key's run in one row group.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct RunPart {
    /// The row group.
    pub(crate) row_group: usize,
    /// The run's rows within the row group, ascending by order value;
    /// never empty.
    pub(crate) rows: Range<i64>,
}

/// A key's run in one file: its rows in each row group it spans, in file
/// order, so ascending by order value; at least one part.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Run<'k> {
    /// The key whose run it is.
    pub(crate) key: &'k Key,
    /// The run's rows per row group.
    pub(crate) parts: Vec<RunPart>,
}

/// What one file answers a Scan of one key (`FileIndex::read_run`).
pub(crate) struct FileRows {
    /// The rows taken, in the Scan's direction, each with one value per
    /// requested column, `None` for a null and for a column the file does
    /// not hold (`FileIndex::value_leaf`), whose default the caller
    /// answers; and whether a live row inside the bounds lies past the
    /// last row taken.
    pub(crate) answer: FileAnswer,
    /// Whether any order or value page the read took came from object
    /// storage, a fetch it ran or joined.
    pub(crate) from_store: bool,
}

/// Where a run's pages come from beyond what the tier holds (RFD 025,
/// Caching and hedging; RFD 027): the page cache every table shares,
/// through which every GET of a page goes; the fan-out a GET holds one
/// permit of while it runs; the table's span hedge window, which span
/// GETs are hedged in; and the counters a GET counts in as it is issued,
/// each value column's at its position in the layout and the order
/// column's. A Bloom-tier key page is fetched through the cache under the
/// file's lookup window and counted nowhere.
#[derive(Clone, Copy)]
pub(crate) struct RunSource<'a> {
    /// The page cache every table shares.
    pub(crate) cache: &'a PageCache,
    /// The Scan's fan-out (`TableHandles::batch_fan_out`); `None` bounds
    /// nothing.
    pub(crate) fan_out: Option<&'a Arc<Semaphore>>,
    /// The table's span hedge window.
    pub(crate) window: &'a HedgeWindow,
    /// Per value column, by position in the layout, the counter its GETs
    /// count in; a position past the end counts nowhere.
    pub(crate) value_gets: &'a [Counter],
    /// The counter the order column's GETs count in.
    pub(crate) order_gets: Option<&'a Counter>,
}

impl FileIndex {
    /// The key's run: per row group it spans, its exact rows; `None` for a
    /// key the file does not hold. The resident first and last key of each
    /// row group give the row groups the run spans. Within one, the run
    /// lies from the last key page whose fence is below the key to the last
    /// whose fence is at or below it, and every page between those two
    /// holds only the key, so decoding the two gives the run's exact rows;
    /// neither is decoded where the row group's first or last key is the
    /// key, since the run then starts at the row group's first row or ends
    /// at its last. Full decodes the pages from the resident key chunks
    /// with no I/O; Disk reads each from the chunk file, one local read
    /// each and one more for the chunk's dictionary page when the key
    /// column has one, and no GET; Bloom screens each row group with its
    /// filter where the run's presence is not already known, then fetches
    /// the pages of every row group it spans concurrently through
    /// `source`'s cache, one GET each under the file's lookup window and
    /// the key column's dictionary page besides, each holding a permit of
    /// the fan-out. A key outside the file's range or between two row groups
    /// costs nothing in any tier; one inside a row group that the file does
    /// not hold costs one key-page decode in Full, one local read in Disk,
    /// and in Bloom a key-page GET at the filter's false-positive rate.
    pub(crate) async fn locate_run<'k>(
        &self,
        key: &'k Key,
        source: RunSource<'_>,
    ) -> Result<Option<Run<'k>>> {
        if !self.may_contain(key) {
            return Ok(None);
        }
        let first = self.groups.partition_point(|group| group.last < *key);
        let last = self.groups.partition_point(|group| group.first <= *key);
        let edges: Vec<GroupEdges> = self.groups[first..last.max(first)]
            .iter()
            .filter_map(|group| self.edges(group, key))
            .collect();
        let parts = futures::future::try_join_all(
            edges.iter().map(|edges| self.run_rows(edges, key, source)),
        )
        .await?;
        let parts: Vec<RunPart> = parts.into_iter().flatten().collect();
        Ok((!parts.is_empty()).then_some(Run { key, parts }))
    }

    /// Where the run of `key` can start and end in `group`, whose key range
    /// holds it: a row where the group's first or last key tells, a key page
    /// to decode otherwise. `None` where the Bloom tier's filter rules the
    /// key out, which it is asked only when neither end is known.
    fn edges(&self, group: &GroupRange, key: &Key) -> Option<GroupEdges> {
        let fences = &self.fences[group.row_group];
        let below = fences.partition_point(|fence| fence < key);
        let through = fences.partition_point(|fence| fence <= key);
        let start = match below.checked_sub(1) {
            None => Edge::Row(0),
            Some(page) => Edge::Page(page),
        };
        let end = if group.last == *key {
            Edge::Row(self.key_column().group(group.row_group).rows())
        } else {
            Edge::Page(through.checked_sub(1)?)
        };
        if let (Edge::Page(_), Edge::Page(_), KeyResidency::Filters(filters)) =
            (start, end, &self.keys)
            && !filters[group.row_group].check(&*key.as_bytes())
        {
            return None;
        }
        Some(GroupEdges {
            row_group: group.row_group,
            start,
            end,
        })
    }

    /// The run's rows in the row group `edges` describes, decoding the key
    /// pages its edges name, each once; `None` where the row group does not
    /// hold the key.
    async fn run_rows(
        &self,
        edges: &GroupEdges,
        key: &Key,
        source: RunSource<'_>,
    ) -> Result<Option<RunPart>> {
        let chunk = self.key_column().group(edges.row_group);
        let mut pages: Vec<usize> = [edges.start, edges.end]
            .iter()
            .filter_map(|edge| match edge {
                Edge::Page(page) => Some(*page),
                Edge::Row(_) => None,
            })
            .collect();
        pages.dedup();
        let segments = self.key_pages(chunk, &pages, source).await?;
        let mut held = Vec::with_capacity(pages.len());
        for &page in &pages {
            let rows = chunk.page_rows(page);
            let keys = decode_run(
                chunk,
                self.layout.key_shape.run_shape(),
                rows.clone(),
                Arc::clone(&segments),
            )?;
            let at = keys.equal_range(key);
            held.push((
                page,
                rows.start + row_offset(at.start)..rows.start + row_offset(at.end),
            ));
        }
        let rows_of = |page| {
            held.iter()
                .find(|(decoded, _)| *decoded == page)
                .map(|(_, rows)| rows.clone())
                .expect("every page an edge names is decoded")
        };
        let start = match edges.start {
            Edge::Row(row) => row,
            Edge::Page(page) => rows_of(page).start,
        };
        let end = match edges.end {
            Edge::Row(row) => row,
            Edge::Page(page) => rows_of(page).end,
        };
        Ok((start < end).then_some(RunPart {
            row_group: edges.row_group,
            rows: start..end,
        }))
    }

    /// The bytes key pages `pages` of `chunk` decode from: the resident
    /// chunks in Full; in Disk and Bloom each page and the chunk's
    /// dictionary page when it has one, read together, from the chunk file
    /// or through `source`'s cache under the file's lookup window. Nothing
    /// is read for no pages.
    async fn key_pages(
        &self,
        chunk: ColumnChunk<'_>,
        pages: &[usize],
        source: RunSource<'_>,
    ) -> Result<Arc<Segments>> {
        let from = match &self.keys {
            KeyResidency::Chunks(chunks) => return Ok(Arc::clone(chunks)),
            KeyResidency::Disk(disk) => PageSource::Disk(disk),
            KeyResidency::Filters(_) => PageSource::Store(
                Some(source.cache),
                Issue {
                    fan_out: source.fan_out,
                    counted: None,
                },
            ),
        };
        let mut ranges: Vec<Range<u64>> =
            pages.iter().map(|&page| chunk.page_range(page)).collect();
        if !ranges.is_empty() {
            ranges.extend(chunk.dictionary_range());
        }
        let parts = futures::future::try_join_all(ranges.into_iter().map(|range| async move {
            let (bytes, _origin) = self.read_from(from, range.clone()).await?;
            Ok::<_, Error>((range, bytes))
        }))
        .await?;
        Ok(Arc::new(Segments::new(parts, self.handle.size())))
    }

    /// The candidate rows of the run for one Scan: up to `bounds.limit` live
    /// rows from the direction's end, cut by the bounds, and whether one
    /// more live row lies past them. The limit is at least 1, as the table
    /// checked it. Live rows are counted against `deleted` with no I/O.
    ///
    /// The order fences place each bound on the one order page that holds
    /// it (`pieces`), so every other row of the run is known inside the
    /// bounds or outside them with no read, and no row outside is read in
    /// any column. From the direction's end, a live row known inside counts
    /// toward the limit and is taken up with its values; a live row on a
    /// page that holds a bound, whose order value alone places it, is taken
    /// up with its values while fewer than the limit's rows have counted,
    /// and counts nothing. Once the limit's rows have counted, the next live
    /// row is taken up for its order value alone, and since a row known
    /// inside then lies behind it, that row tells whether a live row lies
    /// past the rows taken whichever side of the far bound it lies on. So
    /// beyond the rows taken, a read takes up the values of at most as many
    /// rows as the pages that hold a bound hold of the run (RFD 025, Cost).
    ///
    /// Where the order values of a page that holds a bound are at hand with
    /// no GET, they place its rows exactly before the round (`known`):
    /// under Disk every order page the walk could need is read from the
    /// chunk file first, and otherwise a page that holds a bound is decoded
    /// from the page cache where memory holds it and its chunk's dictionary
    /// page. Rows a known order value places outside are passed over, and
    /// the values read are then those of the rows taken alone. Then, in one
    /// round, every order page over the rows taken up whose values are not
    /// known yet, and every page of each column in `columns`, positions in
    /// the layout, over the rows taken up with their values, are read
    /// together, and the bounds cut the rows exactly.
    ///
    /// Per column, the rows read are spans by its pages (`spans_of`), a
    /// row group's split where whole pages between two rows hold none of
    /// those read, as pages whose rows are all deleted do, so no such page
    /// is decoded; it is fetched with its neighbours where the pages
    /// between two spans take at most `SPAN_GAP_BRIDGE_BYTES` (`bridged`),
    /// and not at all otherwise. Per column and fetch, the pages are one
    /// span GET through
    /// `source`'s cache under its span window for every contiguous run the
    /// cache does not hold, and one GET for the chunk's dictionary page when
    /// it has one, each holding a permit of the fan-out and counted in the
    /// column's counter as it is issued; under Disk the order column's
    /// pages are read from the chunk file instead, one local read for the
    /// span and one for its dictionary page, and no GET. The headers of
    /// every page read are checked before parquet decodes it, as a value
    /// page's are (`checked_values`, `check_order_pages`); a
    /// span that decodes to fewer order values than rows is
    /// `FileFault::PageRowsMismatch`, and a string order value that is not
    /// UTF-8 `FileFault::OrderNotUtf8`.
    pub(crate) async fn read_run(
        &self,
        run: &Run<'_>,
        bounds: &ScanBounds,
        columns: &[usize],
        deleted: &dyn DeletedRows,
        source: RunSource<'_>,
    ) -> Result<FileRows> {
        let order = self.order_leaf_of();
        let cut = Cut {
            start: bounds.start.as_ref(),
            before: bounds.before.as_ref(),
        };
        let pieces = self.pieces(run, cut);
        let limit = usize::try_from(bounds.limit).unwrap_or(usize::MAX);
        let walk = Walk {
            index: self,
            pieces: &pieces,
            cut,
            direction: bounds.direction,
            limit,
            deleted,
        };
        let placed = walk.rows(&Spans::default());
        let known = self
            .known(order, &pieces, &placed, bounds.direction, source)
            .await?;
        let walked = if known.spans.is_empty() {
            placed
        } else {
            walk.rows(&known)
        };
        let order_rows = in_file_order(
            walked
                .iter()
                .filter(|walked| !known.holds(walked.row))
                .map(|walked| walked.row),
            bounds.direction,
        );
        let value_rows = in_file_order(
            walked
                .iter()
                .filter(|walked| walked.values)
                .map(|walked| walked.row),
            bounds.direction,
        );
        let (read, values) = futures::try_join!(
            self.read_orders(&order, &order_rows, source),
            self.read_values(columns, &value_rows, source),
        )?;
        let orders = known.and(read);
        let mut rows = Vec::new();
        let mut beyond = false;
        for walked in &walked {
            let (values_of, at) = orders.at(walked.row);
            if cut.fit_at(values_of, at) != Fit::Inside {
                continue;
            }
            if rows.len() == limit {
                beyond = true;
                break;
            }
            rows.push(ScanRow {
                order: values_of.value(at),
                values: values
                    .iter()
                    .map(|column| column.as_ref().and_then(|column| column.value(walked.row)))
                    .collect(),
            });
        }
        Ok(FileRows {
            answer: FileAnswer { rows, beyond },
            from_store: orders.from_store
                || values.iter().flatten().any(|column| column.from_store),
        })
    }

    /// The run's rows that can lie inside `cut`, in file order, as pieces
    /// of one row group each: rows known inside, and the rows of an order
    /// page that holds a bound, which their order values alone place (RFD
    /// 025, Cost). The fences place each bound, paired with the run's key,
    /// on the one page of the file whose pairs' range holds it
    /// (`OrderFences::place`), a page's pairs lying from its fence up to the
    /// next page's. Both bounds cut between the pairs below their pair and
    /// those at or above it: every pair on a page before the bound's lies
    /// below that pair, so every run row there is below the bound, and
    /// every pair on a page past it above, so every run row there is at or
    /// above it. A page's rows lie at or above its fence, so where the
    /// bound's fence is the pair itself its page holds no run row below the
    /// bound, and the page is wholly at or above `start` or wholly at or
    /// above `before`. The work is one binary search over the fences per
    /// bound and a step per row group the run spans, whatever the run's
    /// length.
    fn pieces(&self, run: &Run<'_>, cut: Cut<'_>) -> Vec<Piece> {
        let fences = self
            .order_fences()
            .expect("a Scan reads an ordered file, whose index holds its order fences");
        // The page that holds the bound's pair, and whether that page also
        // holds run rows below the bound.
        let place = |bound: &OrderValue| {
            let pair = Pair::of(run.key, bound);
            fences
                .place(pair)
                .map(|(row_group, page)| (row_group, page, fences.fence(row_group, page) < pair))
        };
        let start = cut.start.map(place);
        let before = cut.before.map(place);
        let mut pieces = Vec::new();
        for part in &run.parts {
            let chunk = self.order_chunk(part.row_group);
            let pages = PartPages {
                row_group: part.row_group,
                first: chunk.page_holding(row_index(part.rows.start)),
                last: chunk.page_holding(row_index(part.rows.end - 1)),
            };
            let low = match start {
                None | Some(None) => Some((pages.first, false)),
                Some(Some((row_group, page, across))) => match pages.locate((row_group, page)) {
                    PageWhere::Before => Some((pages.first, false)),
                    PageWhere::On(page) => Some((page, across)),
                    PageWhere::After => None,
                },
            };
            let high = match before {
                None => Some((pages.last, false)),
                Some(None) => None,
                Some(Some((row_group, page, across))) => match pages.locate((row_group, page)) {
                    PageWhere::Before => None,
                    PageWhere::On(page) if across => Some((page, true)),
                    PageWhere::On(page) => (page > pages.first).then(|| (page - 1, false)),
                    PageWhere::After => Some((pages.last, false)),
                },
            };
            let (Some((low, low_bound)), Some((high, high_bound))) = (low, high) else {
                continue;
            };
            if low > high {
                continue;
            }
            let within = |page: usize| {
                let held = chunk.page_rows(page);
                held.start.max(part.rows.start)..held.end.min(part.rows.end)
            };
            let rows = within(low).start..within(high).end;
            let piece = |rows: Range<i64>, bound: Option<usize>| Piece {
                row_group: part.row_group,
                rows,
                bound,
            };
            if low == high && (low_bound || high_bound) {
                pieces.push(piece(rows, Some(low)));
                continue;
            }
            let inside_from = if low_bound {
                within(low).end
            } else {
                rows.start
            };
            let inside_to = if high_bound {
                within(high).start
            } else {
                rows.end
            };
            if low_bound {
                pieces.push(piece(rows.start..inside_from, Some(low)));
            }
            if inside_from < inside_to {
                pieces.push(piece(inside_from..inside_to, None));
            }
            if high_bound {
                pieces.push(piece(inside_to..rows.end, Some(high)));
            }
        }
        pieces
    }

    /// The order values a read knows before its round: none where `placed`,
    /// the walk in `direction` the fences alone place, takes up no row on a
    /// page that holds a bound, since every row it takes up is then placed;
    /// under Disk, those of every row `placed` takes up, read from the
    /// chunk file, which cover every row a walk with them takes up, since
    /// such a walk counts at least as many rows inside as `placed` at every
    /// step and so stops no later; otherwise those of each piece of a page
    /// that holds a bound which `placed` takes up a row of, where the page
    /// cache holds the page and its chunk's dictionary page in memory, read
    /// with no GET (`cached_pages`).
    async fn known(
        &self,
        order: OrderLeaf,
        pieces: &[Piece],
        placed: &[Walked],
        direction: Direction,
        source: RunSource<'_>,
    ) -> Result<Spans<OrderRun>> {
        let reached = |at: usize| placed.iter().any(|walked| walked.piece == at);
        let bound_pieces: Vec<usize> = (0..pieces.len())
            .filter(|&at| pieces[at].bound.is_some() && reached(at))
            .collect();
        if bound_pieces.is_empty() {
            return Ok(Spans::default());
        }
        if self.disk_chunks().is_some() {
            let rows = in_file_order(placed.iter().map(|walked| walked.row), direction);
            return self.read_orders(&order, &rows, source).await;
        }
        let mut spans = Vec::new();
        for at in bound_pieces {
            let piece = &pieces[at];
            let chunk = self.order_chunk(piece.row_group);
            let pages = pages_holding(chunk, &piece.rows);
            let Some(segments) = self.cached_pages(chunk, pages.clone(), source) else {
                continue;
            };
            let values = self.decode_order_span(chunk, order, pages, &piece.rows, segments)?;
            spans.push((piece.row_group, piece.rows.start, values));
        }
        Ok(Spans {
            spans,
            from_store: false,
        })
    }

    /// The order values of the spans of `rows`, rows in file order, by the
    /// order column's pages (`read_spans`): from the chunk file under Disk,
    /// through `source` otherwise. A plain `fn` returning `read_spans`'
    /// future, its closures holding `order` by reference: the future is
    /// laid out in every file's share of a Scan, which `Generation::scan`
    /// allocates, and an `async fn` around it would add its own state.
    fn read_orders<'a>(
        &'a self,
        order: &'a OrderLeaf,
        rows: &[RowRef],
        source: RunSource<'a>,
    ) -> impl Future<Output = Result<Spans<OrderRun>>> + use<'a> {
        self.read_spans(
            rows,
            move |row_group| self.order_chunk(row_group),
            self.disk_chunks(),
            source,
            source.order_gets,
            move |chunk, pages, rows, segments| {
                self.decode_order_span(chunk, *order, pages, rows, Arc::clone(segments))
            },
        )
    }

    /// Rows `rows` of the order column chunk `chunk` decoded from
    /// `segments`, which hold `pages`, the pages holding those rows, and
    /// the chunk's dictionary page when it has one: the pages' headers
    /// checked first (`check_order_pages`), fewer values than rows
    /// `FileFault::PageRowsMismatch`, and a string value that is not UTF-8
    /// `FileFault::OrderNotUtf8`.
    fn decode_order_span(
        &self,
        chunk: ColumnChunk<'_>,
        order: OrderLeaf,
        pages: Range<usize>,
        rows: &Range<i64>,
        segments: Arc<Segments>,
    ) -> Result<OrderRun> {
        check_order_pages(chunk, order.shape, pages, &segments)?;
        let values = OrderRun(decode_run(
            chunk,
            order.shape.run_shape(),
            rows.clone(),
            segments,
        )?);
        if values.len() != row_index(rows.end - rows.start) {
            return Err(chunk.page_rows_mismatch(chunk.page_holding(row_index(rows.start))));
        }
        if !values.is_utf8() {
            return Err(file_fault(
                &self.handle.location,
                FileFault::OrderNotUtf8 {
                    row_group: chunk.row_group(),
                },
            ));
        }
        Ok(values)
    }

    /// The values of each column of `columns`, positions in the layout,
    /// at `rows`, rows in file order (`read_column_spans`), every column's
    /// fetches read together.
    async fn read_values(
        &self,
        columns: &[usize],
        rows: &[RowRef],
        source: RunSource<'_>,
    ) -> Result<Vec<Option<Spans<ValueRows>>>> {
        let read = columns
            .iter()
            .map(|&column| self.read_column_spans(column, rows, source));
        futures::future::try_join_all(read).await
    }

    /// The values of value column `column`, a position in the layout, at
    /// `rows`, rows in file order, over the spans of them by the column's
    /// pages (`read_spans`); `None` for a column the file does not hold.
    /// `read_spans`' future itself, its closures holding the column by
    /// reference, with no `async` block around it: `read_values`' join
    /// allocates one per column.
    fn read_column_spans<'a>(
        &'a self,
        column: usize,
        rows: &[RowRef],
        source: RunSource<'a>,
    ) -> impl Future<Output = Result<Option<Spans<ValueRows>>>> + use<'a> {
        let read = self.layout.values[column].as_ref().map(|value| {
            self.read_spans(
                rows,
                move |row_group| self.value_column(*value).group(row_group),
                None,
                source,
                source.value_gets.get(column),
                move |chunk, pages, rows, segments| {
                    checked_values(chunk, value.shape, pages, rows.clone(), segments)
                },
            )
        });
        OptionFuture::from(read).map(Option::transpose)
    }

    /// The spans of `rows`, rows in file order, by the pages of the column
    /// whose chunk in each row group `chunk_of` gives (`spans_of`), fetched
    /// as `bridged` joins them (`span_pages`), from `local` when given and
    /// through `source` otherwise, counted in `counted`, and decoded span
    /// by span by `decode` from the chunk, the pages holding the span, its
    /// rows, and the bytes fetched, which checks the pages' headers before
    /// parquet reads them; every fetch read together. The fetches are
    /// planned when it is called, so the future holds them and not `rows`,
    /// and each fetch's future is `span_pages`' mapped to its decode: an
    /// `async` block awaiting it would hold it twice, and the join
    /// allocates one per fetch.
    fn read_spans<'a, T, C, D>(
        &'a self,
        rows: &[RowRef],
        chunk_of: C,
        local: Option<&'a DiskChunks>,
        source: RunSource<'a>,
        counted: Option<&'a Counter>,
        decode: D,
    ) -> impl Future<Output = Result<Spans<T>>> + use<'a, T, C, D>
    where
        C: Fn(usize) -> ColumnChunk<'a> + Copy + 'a,
        D: Fn(ColumnChunk<'a>, Range<usize>, &Range<i64>, &Arc<Segments>) -> Result<T> + Copy + 'a,
    {
        let fetches = bridged(spans_of(rows, chunk_of), chunk_of);
        async move {
            let read = fetches.iter().map(|fetch| {
                let chunk = chunk_of(fetch.row_group);
                self.span_pages(chunk, fetch.pages.clone(), local, source, counted)
                    .map(move |read| {
                        let (segments, from_store) = read?;
                        let spans = fetch
                            .spans
                            .iter()
                            .map(|rows| {
                                let values =
                                    decode(chunk, pages_holding(chunk, rows), rows, &segments)?;
                                Ok((fetch.row_group, rows.start, values))
                            })
                            .collect::<Result<Vec<_>>>()?;
                        Ok::<_, Error>((spans, from_store))
                    })
            });
            let read = futures::future::try_join_all(read).await?;
            let from_store = read.iter().any(|(_, from_store)| *from_store);
            Ok(Spans {
                spans: read.into_iter().flat_map(|(spans, _)| spans).collect(),
                from_store,
            })
        }
    }

    /// Data pages `pages` of `chunk` and its dictionary page when it has
    /// one, as the bytes they decode from, and whether any came from object
    /// storage: from `local`, the chunk file, in one read for the pages and
    /// one for the dictionary page; otherwise through `source`'s cache, the
    /// pages it does not hold in span GETs under its span window and the
    /// dictionary page in a GET of its own, read together, each counted in
    /// `counted`.
    async fn span_pages(
        &self,
        chunk: ColumnChunk<'_>,
        pages: Range<usize>,
        local: Option<&DiskChunks>,
        source: RunSource<'_>,
        counted: Option<&Counter>,
    ) -> Result<(Arc<Segments>, bool)> {
        let size = self.handle.size();
        let dictionary = chunk.dictionary_range();
        if let Some(disk) = local {
            let span = chunk.page_range(pages.start).start..chunk.page_range(pages.end - 1).end;
            let read_dictionary = async {
                match dictionary.clone() {
                    Some(range) => disk.page(range).await.map(Some),
                    None => Ok(None),
                }
            };
            let (bytes, dictionary_bytes) =
                futures::try_join!(disk.page(span.clone()), read_dictionary)?;
            let mut parts = vec![(span, bytes)];
            parts.extend(dictionary.zip(dictionary_bytes));
            return Ok((Arc::new(Segments::new(parts, size)), false));
        }
        let issue = Issue {
            fan_out: source.fan_out,
            counted,
        };
        let ranges: Vec<Range<u64>> = pages.map(|page| chunk.page_range(page)).collect();
        let read_dictionary = async {
            match dictionary.clone() {
                Some(range) => self
                    .handle
                    .read_pages(
                        std::slice::from_ref(&range),
                        source.cache,
                        issue,
                        source.window,
                    )
                    .await
                    .map(Some),
                None => Ok(None),
            }
        };
        let ((bytes, origin), dictionary_read) = futures::try_join!(
            self.handle
                .read_pages(&ranges, source.cache, issue, source.window),
            read_dictionary
        )?;
        let mut from_store = origin == PageOrigin::Fetched;
        let mut parts: Vec<(Range<u64>, Bytes)> = ranges.into_iter().zip(bytes).collect();
        if let (Some(range), Some((bytes, origin))) = (dictionary, dictionary_read) {
            from_store |= origin == PageOrigin::Fetched;
            parts.extend(bytes.into_iter().map(|page| (range.clone(), page)));
        }
        Ok((Arc::new(Segments::new(parts, size)), from_store))
    }

    /// Data pages `pages` of `chunk` and its dictionary page when it has
    /// one, as the bytes they decode from, when the page cache holds every
    /// one of them in memory; `None` otherwise, with nothing fetched or
    /// claimed (`PageCache::held_span`).
    fn cached_pages(
        &self,
        chunk: ColumnChunk<'_>,
        pages: Range<usize>,
        source: RunSource<'_>,
    ) -> Option<Arc<Segments>> {
        let mut ranges: Vec<Range<u64>> = pages.map(|page| chunk.page_range(page)).collect();
        ranges.extend(chunk.dictionary_range());
        let held = self.handle.cached_pages(&ranges, source.cache)?;
        let parts = ranges.into_iter().zip(held).collect();
        Some(Arc::new(Segments::new(parts, self.handle.size())))
    }
}

/// What a test counts a Scan's work by on this thread: the order pages a
/// walk steps on, a page each time a walk steps onto a page other than
/// the one it stepped on last.
#[cfg(test)]
pub(super) mod testing {
    use std::cell::Cell;

    use super::{FileIndex, RowRef, row_index};

    thread_local! {
        static PAGES: Cell<usize> = const { Cell::new(0) };
        static LAST: Cell<Option<(usize, usize)>> = const { Cell::new(None) };
    }

    /// A walk steps on `row` of `index`.
    pub(super) fn visit_row(index: &FileIndex, row: RowRef) {
        let chunk = index.order_chunk(row.row_group);
        let page = (row.row_group, chunk.page_holding(row_index(row.row)));
        if LAST.replace(Some(page)) != Some(page) {
            PAGES.set(PAGES.get() + 1);
        }
    }

    /// The order pages walks stepped on since the last call.
    pub(super) fn take_pages() -> usize {
        LAST.set(None);
        PAGES.replace(0)
    }
}

/// The data pages of `chunk` holding `rows`, which are not empty.
fn pages_holding(chunk: ColumnChunk<'_>, rows: &Range<i64>) -> Range<usize> {
    chunk.page_holding(row_index(rows.start))..chunk.page_holding(row_index(rows.end - 1)) + 1
}

/// A Scan's half-open bounds on the order value: `start` inclusive,
/// `before` exclusive.
#[derive(Clone, Copy)]
struct Cut<'a> {
    start: Option<&'a OrderValue>,
    before: Option<&'a OrderValue>,
}

impl Cut<'_> {
    /// Where value `at` of `values` lies against the bounds, compared in
    /// place.
    fn fit_at(self, values: &OrderRun, at: usize) -> Fit {
        if self
            .start
            .is_some_and(|start| values.cmp_value(at, start).is_lt())
        {
            Fit::Below
        } else if self
            .before
            .is_some_and(|before| values.cmp_value(at, before).is_ge())
        {
            Fit::Above
        } else {
            Fit::Inside
        }
    }
}

/// Where an order value lies against a Scan's half-open bounds.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Fit {
    /// Below `start`.
    Below,
    /// At or above `start` and below `before`.
    Inside,
    /// At or above `before`.
    Above,
}

/// Where a run can start or end in one row group.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Edge {
    /// At this row, which the row group's first or last key tells.
    Row(i64),
    /// Inside this key page, which is decoded to tell.
    Page(usize),
}

/// The edges of a run in one row group (`FileIndex::edges`).
struct GroupEdges {
    row_group: usize,
    start: Edge,
    end: Edge,
}

/// The order pages of one row group that hold a part of a run, from
/// `first` to `last`.
struct PartPages {
    row_group: usize,
    first: usize,
    last: usize,
}

/// Where an order page of the file lies against a `PartPages`.
enum PageWhere {
    /// Before its first page.
    Before,
    /// On this page of it.
    On(usize),
    /// After its last page.
    After,
}

impl PartPages {
    /// Where page `page` of row group `row_group` lies against these pages,
    /// the file's pages in row group order and then page order.
    fn locate(&self, (row_group, page): (usize, usize)) -> PageWhere {
        match row_group.cmp(&self.row_group) {
            std::cmp::Ordering::Less => PageWhere::Before,
            std::cmp::Ordering::Greater => PageWhere::After,
            std::cmp::Ordering::Equal if page < self.first => PageWhere::Before,
            std::cmp::Ordering::Equal if page > self.last => PageWhere::After,
            std::cmp::Ordering::Equal => PageWhere::On(page),
        }
    }
}

/// Rows of a run in one row group that can lie inside a Scan's bounds
/// (`FileIndex::pieces`): every one of them known inside, or, with `bound`,
/// the rows of the order page `bound`, which holds a bound, and which
/// their order values alone place.
struct Piece {
    row_group: usize,
    rows: Range<i64>,
    bound: Option<usize>,
}

/// A row a read takes up, in the Scan's direction, from piece `piece`: one
/// that may be taken, whose values are read, or the one after the rows
/// taken, whose order value alone is.
struct Walked {
    row: RowRef,
    values: bool,
    piece: usize,
}

/// The rows of a run a read takes up (`Walk::rows`).
struct Walk<'a> {
    index: &'a FileIndex,
    pieces: &'a [Piece],
    cut: Cut<'a>,
    direction: Direction,
    limit: usize,
    deleted: &'a dyn DeletedRows,
}

impl Walk<'_> {
    /// From the direction's end, every live row of the pieces until `limit`
    /// rows inside are taken up with their values and one more row without
    /// them. A row of a piece known inside, or whose order value `known`
    /// places inside, counts toward `limit`; one `known` places outside is
    /// passed over on the near side of the bounds, and on the far side ends
    /// the walk, since every row after it lies there too; one whose order
    /// value is not known counts nothing, and is taken up with its values
    /// while fewer than `limit` rows have counted. Past `limit`, which is at
    /// least 1, the next live row is taken up with its order value alone,
    /// and ends the walk.
    fn rows(&self, known: &Spans<OrderRun>) -> Vec<Walked> {
        let mut walk = Taking {
            taken: Vec::new(),
            inside: 0,
            far: match self.direction {
                Direction::OldestFirst => Fit::Above,
                Direction::NewestFirst => Fit::Below,
            },
        };
        let count = self.pieces.len();
        for step in 0..count {
            let at = match self.direction {
                Direction::OldestFirst => step,
                Direction::NewestFirst => count - 1 - step,
            };
            if self.walk_piece(at, known, &mut walk) == Walking::Ended {
                break;
            }
        }
        walk.taken
    }

    /// The rows of piece `at` that `Walk::rows` takes up, in the walk's
    /// direction, into `walk`; whether the walk ends there. On entering
    /// each order page, the piece's rows on it are passed over in one step
    /// where every one of them is deleted, which the resident deletes tell
    /// with no I/O (`DeletedRows::all_deleted`).
    fn walk_piece(&self, at: usize, known: &Spans<OrderRun>, walk: &mut Taking) -> Walking {
        let piece = &self.pieces[at];
        let chunk = self.index.order_chunk(piece.row_group);
        let (mut row, step) = match self.direction {
            Direction::OldestFirst => (piece.rows.start, 1),
            Direction::NewestFirst => (piece.rows.end - 1, -1),
        };
        let position = |row| {
            self.index.position(RowRef {
                row_group: piece.row_group,
                row,
            })
        };
        let mut page_entry = row;
        while piece.rows.contains(&row) {
            if row == page_entry {
                let held = chunk.page_rows(chunk.page_holding(row_index(row)));
                let on_page = held.start.max(piece.rows.start)..held.end.min(piece.rows.end);
                page_entry = match self.direction {
                    Direction::OldestFirst => on_page.end,
                    Direction::NewestFirst => on_page.start - 1,
                };
                if self.deleted.deleted(position(row))
                    && self
                        .deleted
                        .all_deleted(position(on_page.start)..position(on_page.end))
                {
                    row = page_entry;
                    continue;
                }
            }
            let at_row = RowRef {
                row_group: piece.row_group,
                row,
            };
            #[cfg(test)]
            testing::visit_row(self.index, at_row);
            if !self.deleted.deleted(position(row))
                && walk.row(self, at, at_row, known) == Walking::Ended
            {
                return Walking::Ended;
            }
            row += step;
        }
        Walking::On
    }
}

/// What a walk has taken up so far (`Walk::rows`): the rows, how many of
/// them count toward the limit, and the side of the bounds past which it
/// ends.
struct Taking {
    taken: Vec<Walked>,
    inside: usize,
    far: Fit,
}

/// Whether a walk goes on.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Walking {
    On,
    Ended,
}

impl Taking {
    /// Live row `row` of piece `at` of `walk`, taken up as `Walk::rows`
    /// says; whether the walk ends at it.
    fn row(&mut self, walk: &Walk<'_>, at: usize, row: RowRef, known: &Spans<OrderRun>) -> Walking {
        let fit = match walk.pieces[at].bound {
            None => Some(Fit::Inside),
            Some(_) => known.fit(row, walk.cut),
        };
        let values = self.inside < walk.limit;
        let walked = Walked {
            row,
            values,
            piece: at,
        };
        match fit {
            Some(Fit::Inside) => {
                self.taken.push(walked);
                if !values {
                    return Walking::Ended;
                }
                self.inside += 1;
            }
            Some(out) if out == self.far => return Walking::Ended,
            Some(Fit::Below | Fit::Above) => {}
            None => {
                self.taken.push(walked);
                if !values {
                    return Walking::Ended;
                }
            }
        }
        Walking::On
    }
}

/// `rows`, which a walk took up in `direction` (`Walk::rows`), in file
/// order, by row group and then by row: as taken oldest first, and
/// reversed newest first, since a walk steps through its pieces, in file
/// order, and each piece's rows in its direction.
fn in_file_order(rows: impl Iterator<Item = RowRef>, direction: Direction) -> Vec<RowRef> {
    let mut rows: Vec<RowRef> = rows.collect();
    match direction {
        Direction::OldestFirst => {}
        Direction::NewestFirst => rows.reverse(),
    }
    debug_assert!(
        rows.is_sorted_by_key(|row| (row.row_group, row.row)),
        "a walk takes rows up in its direction"
    );
    rows
}

/// The spans of `rows`, rows in file order, that a read decodes, by the
/// pages of the column whose chunk in each row group `chunk_of` gives: per
/// row group, in row group order, each span from a row to the last of
/// `rows` after it with no whole page between them that holds none of
/// `rows`. So a page holding none of them is not read. A walk takes up
/// every live row between two it takes up, so the only such pages between
/// two of `rows` are pages whose rows are all deleted, or whose order
/// values the read knows already.
fn spans_of<'c>(
    rows: &[RowRef],
    chunk_of: impl Fn(usize) -> ColumnChunk<'c>,
) -> Vec<(usize, Range<i64>)> {
    let mut spans: Vec<(usize, Range<i64>)> = Vec::new();
    // The page holding the last span's last row, and the row past it.
    let mut last: Option<(usize, i64)> = None;
    for row in rows {
        let span = spans
            .last_mut()
            .filter(|(row_group, _)| *row_group == row.row_group);
        if let (Some((_, span)), Some((_, page_end))) = (span, last)
            && row.row < page_end
        {
            span.end = row.row + 1;
            continue;
        }
        let chunk = chunk_of(row.row_group);
        let page = chunk.page_holding(row_index(row.row));
        let page_end = chunk.page_rows(page).end;
        let span = spans
            .last_mut()
            .filter(|(row_group, _)| *row_group == row.row_group);
        match (span, last) {
            (Some((_, span)), Some((last_page, _))) if page == last_page + 1 => {
                span.end = row.row + 1;
            }
            (Some(_) | None, Some(_) | None) => spans.push((row.row_group, row.row..row.row + 1)),
        }
        last = Some((page, page_end));
    }
    spans
}

/// The most bytes of whole pages between two spans of one column chunk
/// that a read fetches with them, in one GET, rather than leaving them out
/// at the cost of a GET more (`bridged`): 256 KiB, what a span GET moves in
/// well under its round trip, so bridging a gap of deleted rows up to it
/// costs less than the request it saves, and a wider gap, as a long stretch
/// of deleted rows makes, is not fetched.
const SPAN_GAP_BRIDGE_BYTES: u64 = 256 * 1024;

/// One fetch of a read in one column chunk: the row group, the data pages
/// fetched together, and the spans of rows decoded from them, which hold
/// every page fetched but those between two spans.
struct Fetch {
    row_group: usize,
    pages: Range<usize>,
    spans: Vec<Range<i64>>,
}

/// `spans`, per row group in row group order (`spans_of`), as the fetches
/// that read them, by the pages of the column whose chunk in each row group
/// `chunk_of` gives: two neighbouring spans of a row group in one fetch
/// where the whole pages between them take at most
/// `SPAN_GAP_BRIDGE_BYTES`, by the compressed sizes the offset index gives
/// them, and in fetches of their own otherwise. The pages between are
/// fetched and cached, never checked or decoded.
fn bridged<'c>(
    spans: Vec<(usize, Range<i64>)>,
    chunk_of: impl Fn(usize) -> ColumnChunk<'c>,
) -> Vec<Fetch> {
    let mut fetches: Vec<Fetch> = Vec::new();
    for (row_group, rows) in spans {
        let chunk = chunk_of(row_group);
        let pages = pages_holding(chunk, &rows);
        if let Some(fetch) = fetches
            .last_mut()
            .filter(|fetch| fetch.row_group == row_group)
        {
            let gap: u64 = (fetch.pages.end..pages.start)
                .map(|page| {
                    let range = chunk.page_range(page);
                    range.end - range.start
                })
                .sum();
            if gap <= SPAN_GAP_BRIDGE_BYTES {
                fetch.pages.end = pages.end;
                fetch.spans.push(rows);
                continue;
            }
        }
        fetches.push(Fetch {
            row_group,
            pages,
            spans: vec![rows],
        });
    }
    fetches
}

/// The span of `spans` that holds `row`, and `row`'s position in it, where
/// one does, `of` giving a span's row group, first row, and row count:
/// spans in file order, by row group and then first row, no two holding
/// one row, so one binary search finds it, and a lookup visits a number of
/// spans logarithmic in theirs, however many spans a read decoded. Inline,
/// as `Spans::find` is: a Scan looks up each row it walks up to three
/// times, and a call per lookup is a measurable share of a bounded Scan.
#[inline]
fn span_holding<S>(
    spans: &[S],
    row: RowRef,
    of: impl Fn(&S) -> (usize, i64, usize),
) -> Option<(&S, usize)> {
    let past = spans.partition_point(|span| {
        let (row_group, start, _) = of(span);
        (row_group, start) <= (row.row_group, row.row)
    });
    let span = &spans[past.checked_sub(1)?];
    let (row_group, start, rows) = of(span);
    let at = (row_group == row.row_group).then(|| row_index(row.row - start))?;
    (at < rows).then_some((span, at))
}

/// Values a read decoded over a span of rows, one per row.
trait SpanValues {
    /// The rows decoded.
    fn rows(&self) -> usize;
}

impl SpanValues for OrderRun {
    fn rows(&self) -> usize {
        self.len()
    }
}

impl SpanValues for ValueRows {
    fn rows(&self) -> usize {
        self.len()
    }
}

/// One column's values a read decoded: spans of rows, each its row group,
/// its first row, and the values from it on, one per row, in file order,
/// as `span_holding` finds them; and whether any page they decode from
/// came from object storage.
struct Spans<T> {
    spans: Vec<(usize, i64, T)>,
    from_store: bool,
}

impl<T> Default for Spans<T> {
    fn default() -> Self {
        Self {
            spans: Vec::new(),
            from_store: false,
        }
    }
}

impl<T: SpanValues> Spans<T> {
    /// The decoded values holding `row` and its position among them, where
    /// a span holds it.
    #[inline]
    fn find(&self, row: RowRef) -> Option<(&T, usize)> {
        span_holding(&self.spans, row, |(row_group, start, values)| {
            (*row_group, *start, values.rows())
        })
        .map(|((_, _, values), at)| (values, at))
    }
}

impl Spans<OrderRun> {
    /// The decoded values holding `row`, which the read decoded, and its
    /// position among them.
    fn at(&self, row: RowRef) -> (&OrderRun, usize) {
        self.find(row)
            .expect("the order values of every row taken up are read")
    }

    /// Whether a span holds `row`.
    fn holds(&self, row: RowRef) -> bool {
        self.find(row).is_some()
    }

    /// Where `row`'s order value lies against `cut`, where a span holds it.
    fn fit(&self, row: RowRef, cut: Cut<'_>) -> Option<Fit> {
        self.find(row).map(|(values, at)| cut.fit_at(values, at))
    }

    /// These spans and `other`'s together, in file order: the spans a read
    /// knew before its round and those it read in it, each in file order
    /// and holding rows the other does not.
    fn and(mut self, other: Self) -> Self {
        let file_order = |(row_group, start, _): &(usize, i64, OrderRun)| (*row_group, *start);
        debug_assert!(
            self.spans.is_sorted_by_key(file_order) && other.spans.is_sorted_by_key(file_order),
            "a read decodes spans in file order"
        );
        self.spans.extend(other.spans);
        self.spans.sort_unstable_by_key(file_order);
        self.from_store |= other.from_store;
        self
    }
}

impl Spans<ValueRows> {
    /// The value of `row`, `None` for a null; the read decoded it.
    fn value(&self, row: RowRef) -> Option<Bytes> {
        let (values, at) = self
            .find(row)
            .expect("the values of every row taken are read");
        values.value(at)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;
    use std::path::Path;
    use std::sync::atomic::{AtomicU64, Ordering};

    use super::*;
    use crate::approx::count;
    use crate::config::{HedgeConfig, Residency};
    use crate::generation::testing::{self as generation_testing, cache};
    use rand::{RngExt as _, SeedableRng as _};

    use crate::deletes::FileDeletes;
    use crate::index::RowPosition;
    use crate::index::order_fences::take_steps;
    use crate::index::pages::Handed;
    use crate::index::testing::{
        EXTRA_COLUMN, Fixture, ORDER_FIELD_ID, Opened, Orders, long_orders, ordered_layout, store,
        store_gauged, value_for,
    };
    use crate::key::KeyType;
    use crate::manifest::testing::binary_column;
    use crate::order::OrderType;
    use crate::storage::IoStats;
    use crate::storage::testing::{DEADLINE_GUARD, Gauged};

    /// Rows per page of every column in every fixture, so pages of the
    /// key, value, and order columns hold the same rows.
    const PAGE: usize = 4;
    /// The gap between consecutive order values of one key's run; each
    /// key's run starts at zero, so neighbouring keys' values overlap, as
    /// timestamps of different users do.
    const ORDER_STEP: i64 = 10;
    /// The field id of the first extra value column, after the order
    /// column's.
    const EXTRA_FIELD_ID: i32 = ORDER_FIELD_ID + 1;
    /// A limit above every run a test reads, so the whole run is taken.
    const WHOLE_RUN: u32 = 1_000;

    /// Keys and row counts `runs`, in key order, written in row groups of
    /// `group` rows and pages of `PAGE`; the `j`th row of each key's run
    /// has order value `j * ORDER_STEP`, and row `i` of the file the value
    /// `value_for(i)` in every value column.
    fn runs_of(runs: &[(i64, usize)], group: usize) -> Fixture {
        let mut keys = Vec::new();
        let mut orders = Vec::new();
        for &(key, rows) in runs {
            for j in 0..rows {
                keys.push(key);
                orders.push(i64::try_from(j).unwrap() * ORDER_STEP);
            }
        }
        Fixture::ordered(&keys, long_orders(&orders)).in_pages(PAGE, group)
    }

    /// `fixture` with `extra` more value columns, written and served after
    /// the first.
    fn with_extra(mut fixture: Fixture, extra: usize) -> Fixture {
        fixture.extra_columns = extra;
        fixture
    }

    /// The index of `opened` in `residency` under the ordered layout with
    /// `extra` value columns after the first, a local directory under `dir`
    /// for the Disk tier.
    async fn index_in(
        opened: &Opened,
        residency: Residency,
        dir: &Path,
        extra: usize,
    ) -> FileIndex {
        index_of_type(opened, residency, dir, extra, OrderType::Long).await
    }

    /// `index_in` with an order column of `order_type`.
    async fn index_of_type(
        opened: &Opened,
        residency: Residency,
        dir: &Path,
        extra: usize,
        order_type: OrderType,
    ) -> FileIndex {
        let mut layout = ordered_layout(KeyType::Long, order_type);
        for at in 0..extra {
            let field_id = EXTRA_FIELD_ID + i32::try_from(at).unwrap();
            layout
                .value_columns
                .push(binary_column(&format!("{EXTRA_COLUMN}{at}"), field_id));
        }
        opened
            .build_under_in(&layout, residency, dir)
            .await
            .unwrap()
    }

    /// What a read runs through: a fresh page cache and span window.
    struct Reads {
        cache: PageCache,
        window: HedgeWindow,
    }

    impl Reads {
        async fn new() -> Self {
            Self {
                cache: cache().await,
                window: HedgeWindow::new(&HedgeConfig::default()),
            }
        }

        fn source(&self) -> RunSource<'_> {
            RunSource {
                cache: &self.cache,
                fan_out: None,
                window: &self.window,
                value_gets: &[],
                order_gets: None,
            }
        }

        /// The run of `RUN_KEY` in `index`, located through these reads.
        async fn run(&self, index: &FileIndex) -> Run<'static> {
            index
                .locate_run(&RUN_KEY, self.source())
                .await
                .unwrap()
                .unwrap()
        }

        /// One read of `run` in `index` through these reads, of the first
        /// value column, under `scan`.
        async fn read(
            &self,
            index: &FileIndex,
            run: &Run<'_>,
            scan: &ScanBounds,
            deleted: &dyn DeletedRows,
        ) -> FileRows {
            index
                .read_run(run, scan, &[0], deleted, self.source())
                .await
                .unwrap()
        }
    }

    /// The GETs and local reads that reading `pages` key pages costs in
    /// `residency`, with no dictionary page: none in Full, a local read
    /// each in Disk, a GET each in Bloom.
    fn key_page_costs(residency: Residency, pages: u64) -> (u64, u64) {
        match residency {
            Residency::Full => (0, 0),
            Residency::Disk => (0, pages),
            Residency::Bloom => (pages, 0),
        }
    }

    /// The GETs and local reads `stats` counted since `before`.
    fn since(stats: &IoStats, before: (u64, u64)) -> (u64, u64) {
        (stats.gets() - before.0, stats.local_reads() - before.1)
    }

    fn costs(stats: &IoStats) -> (u64, u64) {
        (stats.gets(), stats.local_reads())
    }

    /// The parts of the run `index` locates for `key`, and what locating
    /// it cost.
    async fn located(
        index: &FileIndex,
        stats: &IoStats,
        key: i64,
    ) -> (Option<Vec<RunPart>>, (u64, u64)) {
        let reads = Reads::new().await;
        let before = costs(stats);
        let key = Key::Long(key);
        let run = index.locate_run(&key, reads.source()).await.unwrap();
        (run.map(|run| run.parts), since(stats, before))
    }

    fn part(row_group: usize, rows: Range<i64>) -> RunPart {
        RunPart { row_group, rows }
    }

    fn run_of(parts: Vec<RunPart>) -> Option<Vec<RunPart>> {
        (!parts.is_empty()).then_some(parts)
    }

    /// The key every read test reads the run of.
    static RUN_KEY: Key = Key::Long(2);

    /// The order value of row `j` of a run.
    fn order_at(j: i64) -> OrderValue {
        OrderValue::Long(j * ORDER_STEP)
    }

    /// What a read of `columns` value columns answers for file row `row`,
    /// `j`th of its key's run: the row's value in each column.
    fn row_answer(row: i64, j: i64, columns: usize) -> ScanRow {
        let value = Bytes::from(value_for(usize::try_from(row).unwrap()));
        ScanRow {
            order: order_at(j),
            values: vec![Some(value); columns],
        }
    }

    /// A Scan of `limit` rows in `direction` with neither bound.
    fn bounds(direction: Direction, limit: u32) -> ScanBounds {
        ScanBounds {
            direction,
            limit,
            start: None,
            before: None,
        }
    }

    fn none_deleted(_: RowPosition) -> bool {
        false
    }

    /// A run inside one key page is that page's rows of the key: one key
    /// page read in Disk and Bloom, none in Full.
    #[tokio::test]
    async fn test_locate_run_inside_one_page() {
        let group = 12;
        let opened = store(&runs_of(&[(1, 1), (2, 2), (3, 5), (4, 4)], group)).await;
        for residency in Residency::ALL {
            let dir = tempfile::tempdir().unwrap();
            let index = index_in(&opened, residency, dir.path(), 0).await;
            let stats = opened.stats();
            let (run, cost) = located(&index, &stats, 2).await;
            assert_eq!(run, run_of(vec![part(0, 1..3)]), "{residency}");
            assert_eq!(cost, key_page_costs(residency, 1), "{residency}");
        }
    }

    /// A run across four key pages of one row group decodes the first and
    /// the last alone; the pages between hold only the key and are never
    /// read.
    #[tokio::test]
    async fn test_locate_run_spanning_pages_decodes_at_most_two() {
        let group = 16;
        let opened = store(&runs_of(&[(1, 2), (2, 12), (3, 2)], group)).await;
        for residency in Residency::ALL {
            let dir = tempfile::tempdir().unwrap();
            let index = index_in(&opened, residency, dir.path(), 0).await;
            assert_eq!(index.page_count(0), 4);
            let stats = opened.stats();
            let (run, cost) = located(&index, &stats, 2).await;
            assert_eq!(run, run_of(vec![part(0, 2..14)]), "{residency}");
            assert_eq!(cost, key_page_costs(residency, 2), "{residency}");
        }
    }

    /// A run that ends one row group, fills the next, and starts the one
    /// after at row zero is one part per row group, found from the resident
    /// first and last keys and one key page at each end; reading it whole
    /// returns every row in order, either way.
    #[tokio::test]
    async fn test_locate_run_spans_row_groups_from_row_zero() {
        let group = 8;
        let opened = store(&runs_of(&[(1, 5), (2, 16), (3, 3)], group)).await;
        for residency in Residency::ALL {
            let dir = tempfile::tempdir().unwrap();
            let index = index_in(&opened, residency, dir.path(), 0).await;
            let stats = opened.stats();
            let (run, cost) = located(&index, &stats, 2).await;
            let expected = vec![part(0, 5..8), part(1, 0..8), part(2, 0..5)];
            assert_eq!(run, run_of(expected), "{residency}");
            assert_eq!(cost, key_page_costs(residency, 2), "{residency}");
            let run = Run {
                key: &RUN_KEY,
                parts: run.unwrap(),
            };
            let reads = Reads::new().await;
            let all: Vec<_> = (5..21).map(|row| row_answer(row, row - 5, 1)).collect();
            let oldest = reads
                .read(
                    &index,
                    &run,
                    &bounds(Direction::OldestFirst, WHOLE_RUN),
                    &none_deleted,
                )
                .await;
            assert_eq!(oldest.answer.rows, all, "{residency}");
            assert!(!oldest.answer.beyond);
            let newest = reads
                .read(
                    &index,
                    &run,
                    &bounds(Direction::NewestFirst, WHOLE_RUN),
                    &none_deleted,
                )
                .await;
            assert_eq!(
                newest.answer.rows,
                all.into_iter().rev().collect::<Vec<_>>()
            );
            assert!(!newest.answer.beyond);
        }
    }

    /// A key that is a page's first key, the row group's first key, or a
    /// fence and the row group's last key is located exactly, the pages a
    /// fence or the resident last key rule out never decoded.
    #[tokio::test]
    async fn test_locate_run_for_a_key_equal_to_a_fence() {
        let group = 12;
        let opened = store(&runs_of(&[(1, 4), (2, 5), (3, 3)], group)).await;
        for residency in Residency::ALL {
            let dir = tempfile::tempdir().unwrap();
            let index = index_in(&opened, residency, dir.path(), 0).await;
            assert_eq!(
                index.fences(),
                [vec![Key::Long(1), Key::Long(2), Key::Long(2)]]
            );
            let stats = opened.stats();
            // Key 2 starts page 1: page 0 is decoded to see the run does
            // not start in it, and page 2 to find its end.
            let (run, cost) = located(&index, &stats, 2).await;
            assert_eq!(run, run_of(vec![part(0, 4..9)]), "{residency}");
            assert_eq!(cost, key_page_costs(residency, 2), "{residency}");
            // Key 1 is the row group's first key: its run starts at row 0.
            let (run, cost) = located(&index, &stats, 1).await;
            assert_eq!(run, run_of(vec![part(0, 0..4)]), "{residency}");
            assert_eq!(cost, key_page_costs(residency, 1), "{residency}");
            // Key 3 is the row group's last key: its run ends the group.
            let (run, cost) = located(&index, &stats, 3).await;
            assert_eq!(run, run_of(vec![part(0, 9..12)]), "{residency}");
            assert_eq!(cost, key_page_costs(residency, 1), "{residency}");
        }
    }

    /// A key outside the file, or between two row groups, costs nothing in
    /// any tier; one inside a row group costs no GET in Full or Disk, and
    /// in Bloom a key-page GET only where the filter passes it.
    #[tokio::test]
    async fn test_locate_run_misses_without_a_get() {
        let group = 12;
        let keys = 12;
        let step = 2;
        let runs: Vec<(i64, usize)> = (0..keys).map(|at| (at * step, 2)).collect();
        let opened = store(&runs_of(&runs, group)).await;
        let keys_per_group = group / 2;
        let between_groups = 11;
        let outside = [-1, keys * step];
        let inside: Vec<i64> = (0..keys - 1)
            .map(|at| at * step + 1)
            .filter(|key| *key != between_groups)
            .collect();
        for residency in Residency::ALL {
            let dir = tempfile::tempdir().unwrap();
            let index = index_in(&opened, residency, dir.path(), 0).await;
            let stats = opened.stats();
            for key in outside.into_iter().chain([between_groups]) {
                let (run, cost) = located(&index, &stats, key).await;
                assert_eq!(run, None, "{residency} {key}");
                assert_eq!(cost, (0, 0), "{residency} {key}: nothing read");
            }
            let filters = opened.filters(&index);
            let mut negatives = 0;
            for &key in &inside {
                let (run, cost) = located(&index, &stats, key).await;
                assert_eq!(run, None, "{residency} {key}");
                let group_of = usize::try_from(key / step).unwrap() / keys_per_group;
                let passes = filters[group_of].check(&*Key::Long(key).as_bytes());
                negatives += usize::from(!passes);
                let expected = match residency {
                    Residency::Full => (0, 0),
                    Residency::Disk => (0, 1),
                    Residency::Bloom => (u64::from(passes), 0),
                };
                assert_eq!(cost, expected, "{residency} {key}");
            }
            assert!(negatives >= 1, "no probed key was a filter negative");
        }
    }

    /// The rows of the fixture most read tests share: key 2's run of
    /// `RUN_ROWS` rows, file rows `FIRST_RUN_ROW` to `LAST_RUN_ROW` across
    /// four pages of one row group of 16, between one row of key 1 and one
    /// of key 3.
    fn one_long_run(extra: usize) -> Fixture {
        let run_rows = usize::try_from(RUN_ROWS).unwrap();
        with_extra(runs_of(&[(1, 1), (2, run_rows), (3, 1)], 16), extra)
    }

    /// The `j`th row of `one_long_run`'s run is file row `j + 1`.
    const FIRST_RUN_ROW: i64 = 1;

    /// The rows of `one_long_run`'s run.
    const RUN_ROWS: i64 = 14;

    /// The file row of the last row of `one_long_run`'s run.
    const LAST_RUN_ROW: i64 = FIRST_RUN_ROW + RUN_ROWS - 1;

    /// Newest first, the rows taken are the last live ones, deleted rows
    /// passed over with no I/O, and the GETs are one span of order pages
    /// and one of value pages: none for the order column under Disk.
    #[tokio::test]
    async fn test_read_run_newest_first_takes_the_last_live_rows() {
        let opened = store(&one_long_run(0)).await;
        let deleted =
            BTreeSet::from([LAST_RUN_ROW, LAST_RUN_ROW - 2].map(|row| u64::try_from(row).unwrap()));
        let file_deletes = FileDeletes::of_rows(deleted.iter().copied());
        let is_deleted = Some(&file_deletes);
        let limit = 3;
        for residency in Residency::ALL {
            let dir = tempfile::tempdir().unwrap();
            let index = index_in(&opened, residency, dir.path(), 0).await;
            let stats = opened.stats();
            let reads = Reads::new().await;
            let run = reads.run(&index).await;
            let before = stats.gets();
            let got = reads
                .read(
                    &index,
                    &run,
                    &bounds(Direction::NewestFirst, limit),
                    &is_deleted,
                )
                .await;
            let expected: Vec<_> = [LAST_RUN_ROW - 1, LAST_RUN_ROW - 3, LAST_RUN_ROW - 4]
                .into_iter()
                .map(|row| row_answer(row, row - FIRST_RUN_ROW, 1))
                .collect();
            assert_eq!(got.answer.rows, expected, "{residency}");
            assert!(got.answer.beyond);
            assert!(got.from_store);
            let order_gets = u64::from(residency != Residency::Disk);
            assert_eq!(stats.gets() - before, order_gets + 1, "{residency}");

            let before = stats.gets();
            let oldest = reads
                .read(
                    &index,
                    &run,
                    &bounds(Direction::OldestFirst, limit),
                    &is_deleted,
                )
                .await;
            let expected: Vec<_> = [1, 2, 3]
                .into_iter()
                .map(|row| row_answer(row, row - FIRST_RUN_ROW, 1))
                .collect();
            assert_eq!(oldest.answer.rows, expected, "{residency}");
            assert!(oldest.answer.beyond);
            assert_eq!(stats.gets() - before, order_gets + 1, "{residency}");
        }
    }

    /// Whether a live row lies past those taken is told by the next live
    /// row past deleted ones, read on an order page no row taken lies on,
    /// with no value page of it; a limit that takes every live row finds
    /// none; a read again is warm. Oldest first, a page of the rows before
    /// the deleted ones finds the live rows after them.
    #[tokio::test]
    async fn test_read_run_reports_the_next_live_row_past_the_rows_taken() {
        let opened = store(&one_long_run(0)).await;
        let deleted: BTreeSet<u64> = (8..=12).collect();
        let file_deletes = FileDeletes::of_rows(deleted.iter().copied());
        let is_deleted = Some(&file_deletes);
        let live = usize::try_from(RUN_ROWS).unwrap() - deleted.len();
        for residency in Residency::ALL {
            let dir = tempfile::tempdir().unwrap();
            let index = index_in(&opened, residency, dir.path(), 0).await;
            let stats = opened.stats();
            let reads = Reads::new().await;
            let run = reads.run(&index).await;
            let newest = bounds(Direction::NewestFirst, 2);
            let before = costs(&stats);
            let got = reads.read(&index, &run, &newest, &is_deleted).await;
            let expected: Vec<_> = [LAST_RUN_ROW, LAST_RUN_ROW - 1]
                .into_iter()
                .map(|row| row_answer(row, row - FIRST_RUN_ROW, 1))
                .collect();
            assert_eq!(got.answer.rows, expected, "{residency}");
            assert!(got.answer.beyond, "{residency}");
            // Order pages 1 and 3 are two spans, page 2 between them all
            // deleted and not decoded, but under the bridge, so fetched
            // with them in one span; the value page is page 3's.
            let (gets, local) = since(&stats, before);
            match residency {
                Residency::Full | Residency::Bloom => assert_eq!((gets, local), (2, 0)),
                Residency::Disk => assert_eq!((gets, local), (1, 1)),
            }
            let before = costs(&stats);
            let again = reads.read(&index, &run, &newest, &is_deleted).await;
            assert_eq!(again.answer.rows, got.answer.rows);
            assert_eq!(again.answer.beyond, got.answer.beyond);
            assert_eq!(since(&stats, before).0, 0, "{residency}: warm");
            assert!(!again.from_store, "{residency}");

            let whole = reads
                .read(
                    &index,
                    &run,
                    &bounds(Direction::NewestFirst, u32::try_from(live).unwrap()),
                    &is_deleted,
                )
                .await;
            assert_eq!(whole.answer.rows.len(), live);
            assert!(!whole.answer.beyond, "{residency}");
            let before_deleted = u32::try_from(*deleted.first().unwrap()).unwrap() - 1;
            let oldest = bounds(Direction::OldestFirst, before_deleted);
            let got = reads.read(&index, &run, &oldest, &is_deleted).await;
            assert_eq!(
                got.answer.rows.len(),
                usize::try_from(before_deleted).unwrap()
            );
            assert!(
                got.answer.beyond,
                "{residency}: a live row lies after the deleted ones"
            );
        }
    }

    /// Spans per row group of the span lookup test, and its row groups:
    /// enough that a scan through them would visit far more than a binary
    /// search does.
    const LOOKUP_SPANS: usize = 128;
    const LOOKUP_ROW_GROUPS: usize = 3;
    /// Rows each span of the span lookup test holds, and rows before each
    /// that no span holds.
    const LOOKUP_SPAN_ROWS: i64 = 3;
    const LOOKUP_GAP_ROWS: i64 = 2;

    /// Among many spans across row groups, `span_holding` finds every row a
    /// span holds, at its position in that span, and no row before a span,
    /// and each lookup visits at most the spans a binary search over them
    /// probes and the one it ends on.
    #[test]
    fn test_span_holding_finds_every_row_among_many_spans_in_a_binary_search() {
        let stride = LOOKUP_SPAN_ROWS + LOOKUP_GAP_ROWS;
        let spans: Vec<(usize, i64, usize)> = (0..LOOKUP_ROW_GROUPS)
            .flat_map(|row_group| {
                (0..LOOKUP_SPANS).map(move |at| {
                    let start = i64::try_from(at).unwrap() * stride + LOOKUP_GAP_ROWS;
                    (row_group, start, row_index(LOOKUP_SPAN_ROWS))
                })
            })
            .collect();
        // A binary search over n spans probes at most ceil(log2 n) + 1 of
        // them, and the lookup then reads the one before where it ended.
        let most_visits = usize::try_from(spans.len().next_power_of_two().ilog2()).unwrap() + 2;
        let visited = std::cell::Cell::new(0);
        let of = |span: &(usize, i64, usize)| {
            visited.set(visited.get() + 1);
            *span
        };
        let rows = i64::try_from(LOOKUP_SPANS).unwrap() * stride + LOOKUP_GAP_ROWS;
        let mut found = 0;
        for row_group in 0..LOOKUP_ROW_GROUPS {
            for row in 0..rows {
                visited.set(0);
                let got = span_holding(&spans, RowRef { row_group, row }, of);
                let offset = row % stride - LOOKUP_GAP_ROWS;
                let expected = (offset >= 0).then(|| {
                    let span = &spans[row_group * LOOKUP_SPANS + row_index(row / stride)];
                    (span, row_index(offset))
                });
                assert_eq!(got, expected, "row group {row_group}, row {row}");
                assert!(
                    visited.get() <= most_visits,
                    "row group {row_group}, row {row}: {} spans visited, above {most_visits}",
                    visited.get()
                );
                found += usize::from(got.is_some());
            }
        }
        assert_eq!(
            found,
            LOOKUP_ROW_GROUPS * LOOKUP_SPANS * row_index(LOOKUP_SPAN_ROWS)
        );
    }

    /// Order pages of the run of the deleted-pages read, `PAGE` rows each.
    const DELETED_RUN_PAGES: usize = 40;
    /// The last row of the deleted-pages read's run below its deleted rows.
    const BELOW_DELETED_ROW: i64 = 8;

    /// The bytes of each value of the deleted-pages read whose gap of
    /// deleted value pages is above `SPAN_GAP_BRIDGE_BYTES`: drawn at
    /// random, so zstd leaves them their size, and four rows a page put
    /// the gap's pages well above the bridge.
    const WIDE_VALUE_BYTES: usize = 4096;
    /// The seed of those values.
    const WIDE_VALUE_SEED: u64 = 25;

    /// The GETs of `ranges` that fetched a page of leaf `leaf`.
    fn gets_of(index: &FileIndex, leaf: usize, ranges: &[Range<u64>]) -> usize {
        ranges
            .iter()
            .filter(|range| !fetched(index, leaf, std::slice::from_ref(range)).is_empty())
            .count()
    }

    /// The pages of leaf `leaf` of row group 0 from the one holding the
    /// first of `rows` to the one holding the last.
    fn pages_from_first_to_last(index: &FileIndex, leaf: usize, rows: &[i64]) -> BTreeSet<PageAt> {
        let at = |row| page_of(index, leaf, RowRef { row_group: 0, row }).1;
        (at(rows[0])..=at(rows[rows.len() - 1]))
            .map(|page| (0, page))
            .collect()
    }

    /// Key 2's run of `DELETED_RUN_PAGES` pages, file rows 1 on, between one
    /// row of key 1 and one of key 3 in one row group, with values of
    /// `value_bytes` drawn at random, or `runs_of`'s own when `None`.
    fn deleted_pages_run(value_bytes: Option<usize>) -> Fixture {
        let rows = DELETED_RUN_PAGES * PAGE;
        let fixture = runs_of(&[(1, 1), (2, rows), (3, 1)], rows + 2);
        match value_bytes {
            Some(len) => with_random_values(fixture, len),
            None => fixture,
        }
    }

    /// `fixture` with every value `len` bytes drawn at random from
    /// `WIDE_VALUE_SEED`, so zstd leaves them their size.
    fn with_random_values(mut fixture: Fixture, len: usize) -> Fixture {
        let mut rng = rand::rngs::StdRng::seed_from_u64(WIDE_VALUE_SEED);
        for value in &mut fixture.values {
            *value = Some((0..len).map(|_| rng.random::<u8>()).collect());
        }
        fixture
    }

    /// Newest first at limit 2 over a run of `DELETED_RUN_PAGES` pages whose
    /// rows are all deleted, by the file's resident deletes, from the one
    /// after `BELOW_DELETED_ROW` to the one before the last; with values of
    /// `value_bytes` drawn at random, or the fixtures' own when `None`. The
    /// read takes the last row and `BELOW_DELETED_ROW` and, in every tier,
    /// decodes only the value pages holding those two and the order pages
    /// holding them and the next live row, the whole pages of deleted rows
    /// between them passed over by the walk in one step each and decoded in
    /// no column. Per column, the pages between are fetched with the rest
    /// in one GET where their bytes are at most `SPAN_GAP_BRIDGE_BYTES`,
    /// and left out where they are above it, as `bridged` says of the value
    /// column; the order column's gap is always under it. Every read's
    /// GETs run in one round.
    async fn assert_reads_across_deleted_pages(value_bytes: Option<usize>, bridged: bool) {
        let fixture = deleted_pages_run(value_bytes);
        let last = i64::try_from(DELETED_RUN_PAGES * PAGE).unwrap();
        let written = fixture.values.clone();
        let (opened, gauged) = store_gauged(&fixture).await;
        let position = |row: i64| u64::try_from(row).unwrap();
        let file_deletes = FileDeletes::of_rows(position(BELOW_DELETED_ROW + 1)..position(last));
        let deleted = Some(&file_deletes);
        let row = |row| RowRef { row_group: 0, row };
        let newest = bounds(Direction::NewestFirst, 2);
        for residency in Residency::ALL {
            let case = format!("{residency}, value bytes {value_bytes:?}");
            let probe = Probe::open(&opened, &gauged, residency, OrderType::Long).await;
            let index = &probe.index;
            testing::take_pages();
            let handed = Handed::now();
            let local_before = opened.stats().local_reads();
            let (got, ranges) = probe.read(&newest, &deleted).await;
            let decoded = Handed::now().since(&handed);
            let local_reads = opened.stats().local_reads() - local_before;
            let walked = testing::take_pages();
            let expected: Vec<_> = [last, BELOW_DELETED_ROW]
                .into_iter()
                .map(|at| ScanRow {
                    order: order_at(at - FIRST_RUN_ROW),
                    values: vec![written[row_index(at)].clone().map(Bytes::from)],
                })
                .collect();
            assert_eq!(got.answer.rows, expected, "{case}");
            assert!(got.answer.beyond, "{case}");
            let value_leaf = index.value_leaf(0).unwrap();
            let order_leaf = index.order_leaf().unwrap();
            let pages = |leaf, rows: &[i64]| -> BTreeSet<PageAt> {
                rows.iter()
                    .map(|&at| page_of(index, leaf, row(at)))
                    .collect()
            };
            let value_rows = [BELOW_DELETED_ROW, last];
            let order_rows = [BELOW_DELETED_ROW - 1, BELOW_DELETED_ROW, last];
            let value_decoded = pages(value_leaf, &value_rows);
            let order_decoded = pages(order_leaf, &order_rows);
            assert_eq!(
                (decoded.data_of(value_leaf), decoded.data_of(order_leaf)),
                (value_decoded.len(), order_decoded.len()),
                "{case}: the value and order pages decoded, those of the rows taken up alone"
            );
            assert_eq!(walked, order_decoded.len(), "{case}: pages walked");
            let (value_pages, value_gets) = if bridged {
                (pages_from_first_to_last(index, value_leaf, &value_rows), 1)
            } else {
                (value_decoded, 2)
            };
            assert_eq!(
                fetched(index, value_leaf, &ranges),
                value_pages,
                "{case}: value pages fetched"
            );
            assert_eq!(
                gets_of(index, value_leaf, &ranges),
                value_gets,
                "{case}: value GETs"
            );
            let (order_pages, order_gets, order_local) = match residency {
                Residency::Disk => (BTreeSet::new(), 0, 1),
                Residency::Full | Residency::Bloom => (
                    pages_from_first_to_last(index, order_leaf, &order_rows),
                    1,
                    0,
                ),
            };
            assert_eq!(
                fetched(index, order_leaf, &ranges),
                order_pages,
                "{case}: order pages fetched"
            );
            assert_eq!(
                gets_of(index, order_leaf, &ranges),
                order_gets,
                "{case}: order GETs"
            );
            assert_eq!(local_reads, order_local, "{case}: local reads");
            probe
                .assert_one_round(&newest, &deleted, ranges.len(), &case)
                .await;
        }
    }

    /// A gap of deleted pages whose bytes are under the bridge is fetched
    /// with the pages around it, one GET per column, and never decoded.
    #[tokio::test]
    async fn test_read_run_bridges_a_small_gap_of_deleted_pages_in_one_get() {
        assert_reads_across_deleted_pages(None, true).await;
    }

    /// A gap of deleted value pages whose bytes are above the bridge is
    /// left out of the fetch, two value GETs, while the order column's
    /// small gap is still bridged.
    #[tokio::test]
    async fn test_read_run_leaves_a_wide_gap_of_deleted_pages_unfetched() {
        assert_reads_across_deleted_pages(Some(WIDE_VALUE_BYTES), false).await;
    }

    /// The fixture of the bounded reads: key 2's run of 30 rows, file rows
    /// 1 to 30 across eight pages of one row group, with `extra` value
    /// columns after the first.
    fn bounded_run(extra: usize) -> Fixture {
        with_extra(runs_of(&[(1, 1), (2, 30), (3, 1)], 32), extra)
    }

    /// What a read of `columns` value columns of `bounded_run` answers
    /// oldest first from the order value of the run's row 19, limit 2:
    /// rows 19 and 20 of the run.
    fn start_answer(columns: usize) -> Vec<ScanRow> {
        [19, 20]
            .into_iter()
            .map(|j| row_answer(j + FIRST_RUN_ROW, j, columns))
            .collect()
    }

    /// A bounded read places the bound by the fences and reads the order
    /// pages and every requested column's pages together: one GET per
    /// column and one for the order column, all in flight at once, the
    /// order column read locally under Disk.
    #[tokio::test]
    async fn test_read_run_with_a_bound_reads_one_round() {
        let extra = 1;
        let columns = [0, 1];
        let (opened, gauged) = store_gauged(&bounded_run(extra)).await;
        for residency in Residency::ALL {
            let dir = tempfile::tempdir().unwrap();
            let index = index_in(&opened, residency, dir.path(), extra).await;
            let stats = opened.stats();
            let reads = Reads::new().await;
            let order_gets = usize::from(residency != Residency::Disk);
            let key_page_gets = usize::from(residency == Residency::Bloom) * 2;
            let before = stats.gets();
            let together = columns.len() + order_gets;
            gauged.fill_to(key_page_gets.max(1));
            let run = reads.run(&index).await;
            gauged.fill_to(together);
            let got = tokio::time::timeout(DEADLINE_GUARD, async {
                let start = ScanBounds {
                    start: Some(order_at(19)),
                    ..bounds(Direction::OldestFirst, 2)
                };
                index
                    .read_run(&run, &start, &columns, &none_deleted, reads.source())
                    .await
                    .unwrap()
            })
            .await
            .expect("every GET of the read is in flight at once");
            assert_eq!(got.answer.rows, start_answer(columns.len()), "{residency}");
            assert!(got.answer.beyond);
            assert_eq!(gauged.high_water(), together, "{residency}");
            assert_eq!(
                stats.gets() - before,
                count(key_page_gets + together),
                "{residency}"
            );
        }
    }

    /// Newest first before a bound, the fences rule out every page past the
    /// one that holds the bound, the run's last page with the next key's
    /// rows included: the read is one span of order pages and one of value
    /// pages, and under Disk, where the bound's page is placed exactly
    /// first, the value page of the rows taken alone.
    #[tokio::test]
    async fn test_read_run_newest_first_before_a_bound_reads_no_page_past_the_bound() {
        let opened = store(&bounded_run(0)).await;
        for residency in Residency::ALL {
            let dir = tempfile::tempdir().unwrap();
            let index = index_in(&opened, residency, dir.path(), 0).await;
            let stats = opened.stats();
            let reads = Reads::new().await;
            let run = reads.run(&index).await;
            let before_ten = ScanBounds {
                before: Some(order_at(10)),
                ..bounds(Direction::NewestFirst, 2)
            };
            let before = costs(&stats);
            let bytes_before = stats.bytes();
            let got = reads.read(&index, &run, &before_ten, &none_deleted).await;
            let expected: Vec<_> = [9, 8]
                .into_iter()
                .map(|j| row_answer(j + FIRST_RUN_ROW, j, 1))
                .collect();
            assert_eq!(got.answer.rows, expected, "{residency}");
            assert!(got.answer.beyond);
            let (gets, local) = since(&stats, before);
            match residency {
                Residency::Full | Residency::Bloom => assert_eq!((gets, local), (2, 0)),
                Residency::Disk => assert_eq!((gets, local), (1, 1)),
            }
            // Page 2 of each column holds the bound and rows 7 to 10 of the
            // run, page 1 rows 3 to 6: a read placed by the fences alone
            // takes up the bound's page and the limit's rows past it; one
            // that places the bound's page exactly first takes the rows
            // taken, on page 2 alone. None of the run's last page is read.
            let (value_bytes, order_bytes) = match residency {
                Residency::Full | Residency::Bloom => {
                    let read = 1..3;
                    (
                        page_bytes(&index, index.value_leaf(0).unwrap(), read.clone()),
                        page_bytes(&index, index.order_leaf().unwrap(), read),
                    )
                }
                Residency::Disk => (page_bytes(&index, index.value_leaf(0).unwrap(), 2..3), 0),
            };
            assert_eq!(
                stats.bytes() - bytes_before,
                value_bytes + order_bytes,
                "{residency}"
            );
        }
    }

    /// The bytes data pages `pages` of leaf `leaf` take in row group 0.
    fn page_bytes(index: &FileIndex, leaf: usize, pages: Range<usize>) -> u64 {
        bytes_of(index, leaf, &pages.map(|page| (0, page)).collect())
    }

    /// Dictionary-encoded columns cost their dictionary page beside each
    /// span, the key column's under Disk and Bloom, the order column's
    /// locally under Disk; a null value is `None`.
    #[tokio::test]
    async fn test_read_run_reads_each_dictionary_page_beside_its_span_and_a_null_as_none() {
        let mut fixture = one_long_run(0);
        fixture.layout.dictionary = generation_testing::Dictionary::All;
        let null_row = LAST_RUN_ROW - 1;
        fixture.values[usize::try_from(null_row).unwrap()] = None;
        let opened = store(&fixture).await;
        for residency in Residency::ALL {
            let dir = tempfile::tempdir().unwrap();
            let index = index_in(&opened, residency, dir.path(), 0).await;
            assert!(
                index
                    .metadata()
                    .row_group(0)
                    .column(index.key_leaf())
                    .dictionary_page_offset()
                    .is_some()
            );
            let stats = opened.stats();
            let (run, cost) = located(&index, &stats, 2).await;
            let pages_and_dictionary = 3;
            assert_eq!(
                cost,
                key_page_costs(residency, pages_and_dictionary),
                "{residency}"
            );
            let run = Run {
                key: &RUN_KEY,
                parts: run.unwrap(),
            };
            let reads = Reads::new().await;
            let before = costs(&stats);
            let got = reads
                .read(
                    &index,
                    &run,
                    &bounds(Direction::NewestFirst, 3),
                    &none_deleted,
                )
                .await;
            let expected: Vec<_> = [LAST_RUN_ROW, LAST_RUN_ROW - 1, LAST_RUN_ROW - 2]
                .into_iter()
                .map(|row| {
                    let ScanRow { order, values } = row_answer(row, row - FIRST_RUN_ROW, 1);
                    let values = if row == null_row { vec![None] } else { values };
                    ScanRow { order, values }
                })
                .collect();
            assert_eq!(got.answer.rows, expected, "{residency}");
            assert!(got.answer.beyond);
            let (gets, local) = since(&stats, before);
            match residency {
                Residency::Full | Residency::Bloom => assert_eq!((gets, local), (4, 0)),
                Residency::Disk => assert_eq!((gets, local), (2, 2)),
            }
        }
    }

    /// Every GET of a read counts once in its column's counter, the order
    /// column's in its own, and holds one permit of the fan-out while it
    /// runs, so a fan-out of one runs them one at a time and none waits for
    /// a permit while holding one.
    #[tokio::test]
    async fn test_read_run_counts_each_get_under_its_column_and_holds_one_permit() {
        let extra = 1;
        let (opened, gauged) = store_gauged(&one_long_run(extra)).await;
        let dir = tempfile::tempdir().unwrap();
        let index = index_in(&opened, Residency::Full, dir.path(), extra).await;
        let stats = opened.stats();
        let counters: Vec<Arc<AtomicU64>> = (0..3).map(|_| Arc::new(AtomicU64::new(0))).collect();
        let handles: Vec<Counter> = counters
            .iter()
            .map(|counter| Counter::from_arc(Arc::clone(counter)))
            .collect();
        let fan_out = Arc::new(Semaphore::new(1));
        let reads = Reads::new().await;
        let source = RunSource {
            fan_out: Some(&fan_out),
            value_gets: &handles[..2],
            order_gets: Some(&handles[2]),
            ..reads.source()
        };
        let run = index.locate_run(&RUN_KEY, source).await.unwrap().unwrap();
        gauged.fill_to(1);
        let before = stats.gets();
        let got = index
            .read_run(
                &run,
                &bounds(Direction::OldestFirst, 2),
                &[0, 1],
                &none_deleted,
                source,
            )
            .await
            .unwrap();
        let expected: Vec<_> = [0, 1]
            .into_iter()
            .map(|j| row_answer(j + FIRST_RUN_ROW, j, 2))
            .collect();
        assert_eq!(got.answer.rows, expected);
        assert_eq!(stats.gets() - before, 3);
        let counted: Vec<u64> = counters
            .iter()
            .map(|counter| counter.load(Ordering::Relaxed))
            .collect();
        assert_eq!(counted, [1, 1, 1], "each column's GET counted once");
        assert_eq!(gauged.high_water(), 1);
        assert_eq!(fan_out.available_permits(), 1, "every permit is back");
    }

    /// Bytes every byte-array order value of `prefixed_run` shares: longer
    /// than `TRUNCATED_INDEX_BYTES`, so every truncated column index entry
    /// of the order column is one and the same prefix, its maximum that
    /// prefix incremented, above every value of the column.
    const SHARED_PREFIX_BYTES: usize = 32;
    /// The column index truncation of a truncated `prefixed_run_of`.
    const TRUNCATED_INDEX_BYTES: usize = 16;
    /// The byte the shared prefix repeats.
    const PREFIX_BYTE: u8 = b'p';
    /// Rows of key 2's run in the short runs of `prefixed_run_of`.
    const PREFIXED_RUN_ROWS: usize = 30;

    /// The `j`th byte-array order value of a run in `prefixed_run`: the
    /// shared prefix, then `j` in four digits, ascending with `j`.
    fn prefixed(j: usize) -> Vec<u8> {
        let mut value = vec![PREFIX_BYTE; SHARED_PREFIX_BYTES];
        value.extend_from_slice(format!("{j:04}").as_bytes());
        value
    }

    /// The order value `bytes` travel as under `order_type`.
    fn travelling(order_type: OrderType, bytes: Vec<u8>) -> OrderValue {
        match order_type {
            OrderType::String => OrderValue::Str(String::from_utf8(bytes).unwrap()),
            OrderType::Int
            | OrderType::Long
            | OrderType::Date
            | OrderType::Timestamp
            | OrderType::TimestampTz
            | OrderType::TimestampNs
            | OrderType::TimestampTzNs
            | OrderType::Binary
            | OrderType::Fixed(_) => OrderValue::Bytes(bytes),
        }
    }

    /// Key 2's run of `run_rows` rows between one row each of keys 1 and
    /// 3, in pages of `PAGE` in one row group, every key's `j`th row
    /// ordered by `prefixed(j)`, with the column index whole.
    fn prefixed_run(run_rows: usize) -> Fixture {
        prefixed_run_of(None, run_rows, prefixed)
    }

    /// `prefixed_run` with every key's `j`th row ordered by `value(j)` and
    /// the column index truncated to `truncate` bytes, `None` leaving it
    /// whole.
    fn prefixed_run_of(
        truncate: Option<usize>,
        run_rows: usize,
        value: fn(usize) -> Vec<u8>,
    ) -> Fixture {
        let runs = [(1, 1), (2, run_rows), (3, 1)];
        let mut keys = Vec::new();
        let mut orders = Vec::new();
        for (key, rows) in runs {
            for j in 0..rows {
                keys.push(key);
                orders.push(Some(value(j)));
            }
        }
        let mut fixture = Fixture::ordered(&keys, Orders::Bytes(orders)).in_pages(PAGE, keys.len());
        fixture.layout.truncate = truncate;
        fixture
    }

    /// Rows of the long runs the bounded probe reads read: many pages
    /// beyond the few a small limit's rows lie on.
    const LONG_PREFIXED_RUN_ROWS: usize = 120;
    /// The limits the cost model reads every bound under: one, a few, and
    /// more than any run holds.
    const COST_LIMITS: [u32; 3] = [1, 3, WHOLE_RUN];
    /// How many bound points apart the two bounds of a read with both lie.
    const BOUND_GAPS: [usize; 2] = [1, 4];
    /// Half of `ORDER_STEP`: an order value between two rows of a run.
    const HALF_STEP: i64 = ORDER_STEP / 2;
    /// Every how many rows of a run the cost model deletes one, besides the
    /// run's first and last.
    const DELETE_EVERY: usize = 5;
    /// Which row of every `DELETE_EVERY` the cost model deletes.
    const DELETED_OF_EVERY: usize = 2;
    /// A byte above every byte the order values of `prefixed_run` start
    /// with, so a bound of it alone lies above every value.
    const ABOVE_EVERY_PREFIX: u8 = b'z';
    /// The byte the first `LOW_PREFIX_ROWS` values of `halved` repeat,
    /// below `PREFIX_BYTE`.
    const LOW_PREFIX_BYTE: u8 = b'a';
    /// Rows of a run whose order values `halved` starts with
    /// `LOW_PREFIX_BYTE`: a page's worth short of four pages, so the run's
    /// first page holds the previous key's row too.
    const LOW_PREFIX_ROWS: usize = 15;

    /// The `j`th order value of a run whose first `LOW_PREFIX_ROWS` rows
    /// share one prefix and the rest another: a 16-byte truncated entry of
    /// a page of either half is one prefix.
    fn halved(j: usize) -> Vec<u8> {
        split_at(LOW_PREFIX_ROWS, j)
    }

    /// Rows of a run whose order values `two_low_pages` starts with
    /// `LOW_PREFIX_BYTE`: the run's first page, which holds the previous
    /// key's row too, and exactly two whole pages.
    const TWO_LOW_PAGES_ROWS: usize = 11;

    /// The `j`th order value of a run whose first `TWO_LOW_PAGES_ROWS` rows
    /// share one prefix and the rest another, as `halved`'s do.
    fn two_low_pages(j: usize) -> Vec<u8> {
        split_at(TWO_LOW_PAGES_ROWS, j)
    }

    /// The `j`th order value of a run whose first `low_rows` rows repeat
    /// `LOW_PREFIX_BYTE` and the rest `PREFIX_BYTE`, then `j` in four
    /// digits.
    fn split_at(low_rows: usize, j: usize) -> Vec<u8> {
        let byte = if j < low_rows {
            LOW_PREFIX_BYTE
        } else {
            PREFIX_BYTE
        };
        let mut value = vec![byte; SHARED_PREFIX_BYTES];
        value.extend_from_slice(format!("{j:04}").as_bytes());
        value
    }

    /// A page of one column: its row group and its index in the chunk.
    type PageAt = (usize, usize);

    /// The page of leaf `leaf` that holds `row`.
    fn page_of(index: &FileIndex, leaf: usize, row: RowRef) -> PageAt {
        let locations =
            index.metadata().offset_index().unwrap()[row.row_group][leaf].page_locations();
        let page = locations.partition_point(|location| location.first_row_index <= row.row) - 1;
        (row.row_group, page)
    }

    /// The pages of leaf `leaf` holding `rows`, in file order, each once.
    fn pages_of(index: &FileIndex, leaf: usize, rows: &[RowRef]) -> Vec<PageAt> {
        let mut pages: Vec<PageAt> = rows.iter().map(|&row| page_of(index, leaf, row)).collect();
        pages.dedup();
        pages
    }

    /// The pages of leaf `leaf` that GETs over `ranges` fetched.
    fn fetched(index: &FileIndex, leaf: usize, ranges: &[Range<u64>]) -> BTreeSet<PageAt> {
        let mut pages = BTreeSet::new();
        let offsets = index.metadata().offset_index().unwrap();
        for (row_group, columns) in offsets.iter().enumerate() {
            for (page, location) in columns[leaf].page_locations().iter().enumerate() {
                let start = u64::try_from(location.offset).unwrap();
                let end = start + u64::try_from(location.compressed_page_size).unwrap();
                if ranges
                    .iter()
                    .any(|range| range.start <= start && end <= range.end)
                {
                    pages.insert((row_group, page));
                }
            }
        }
        pages
    }

    /// The bytes pages `pages` of leaf `leaf` take.
    fn bytes_of(index: &FileIndex, leaf: usize, pages: &BTreeSet<PageAt>) -> u64 {
        let offsets = index.metadata().offset_index().unwrap();
        pages
            .iter()
            .map(|&(row_group, page)| {
                let location = &offsets[row_group][leaf].page_locations()[page];
                u64::try_from(location.compressed_page_size).unwrap()
            })
            .sum()
    }

    /// The rows of `run`, in file order.
    fn rows_of(run: &Run<'_>) -> Vec<RowRef> {
        run.parts
            .iter()
            .flat_map(|part| {
                part.rows.clone().map(|row| RowRef {
                    row_group: part.row_group,
                    row,
                })
            })
            .collect()
    }

    /// Whether `value` lies inside `scan`'s half-open bounds.
    fn inside(scan: &ScanBounds, value: &OrderValue) -> bool {
        scan.start.as_ref().is_none_or(|start| value >= start)
            && scan.before.as_ref().is_none_or(|before| value < before)
    }

    /// The run positions of a run, `rows` of them, in `scan`'s direction.
    fn in_direction(rows: usize, scan: &ScanBounds) -> Vec<usize> {
        let mut walk: Vec<usize> = (0..rows).collect();
        if scan.direction == Direction::NewestFirst {
            walk.reverse();
        }
        walk
    }

    /// What the model answers one read of a run: the rows and whether a
    /// live row lies past them, and the run positions of the rows taken.
    struct Model {
        rows: Vec<ScanRow>,
        beyond: bool,
        taken: Vec<usize>,
    }

    /// The model of reading the run `rows`, ordered by `orders`, of a file
    /// whose value column holds `written` by file position, under `scan`
    /// with the run positions `deleted` deleted.
    fn model(
        index: &FileIndex,
        rows: &[RowRef],
        orders: &[OrderValue],
        written: &[Option<Vec<u8>>],
        deleted: &BTreeSet<usize>,
        scan: &ScanBounds,
    ) -> Model {
        let limit = usize::try_from(scan.limit).unwrap();
        let live: Vec<usize> = in_direction(rows.len(), scan)
            .into_iter()
            .filter(|at| inside(scan, &orders[*at]) && !deleted.contains(at))
            .collect();
        let taken: Vec<usize> = live.iter().copied().take(limit).collect();
        let answer = |at: usize| {
            let position = usize::try_from(index.position(rows[at]).0).unwrap();
            ScanRow {
                order: orders[at].clone(),
                values: vec![written[position].clone().map(Bytes::from)],
            }
        };
        Model {
            rows: taken.iter().map(|&at| answer(at)).collect(),
            beyond: live.len() > limit,
            taken,
        }
    }

    /// The run positions of the rows of the run of `RUN_KEY`, `rows`, on
    /// the order pages that hold one of `scan`'s bounds, found apart from
    /// the read: per bound, by a scan of every page of the run, the last
    /// whose fence is at or below the pair of the key and the bound, where
    /// that fence is below the pair. A page whose fence is the pair holds
    /// no run row below the bound, so its rows lie wholly on one side of it.
    fn bound_rows(index: &FileIndex, rows: &[RowRef], scan: &ScanBounds) -> BTreeSet<usize> {
        let fences = index.order_fences().unwrap();
        let leaf = index.order_leaf().unwrap();
        let run_pages = pages_of(index, leaf, rows);
        let mut held = BTreeSet::new();
        for bound in [&scan.start, &scan.before].into_iter().flatten() {
            let pair = Pair::of(&RUN_KEY, bound);
            let Some(&page) = run_pages
                .iter()
                .rev()
                .find(|&&(row_group, page)| fences.fence(row_group, page) <= pair)
            else {
                continue;
            };
            if fences.fence(page.0, page.1) == pair {
                continue;
            }
            held.extend((0..rows.len()).filter(|&at| page_of(index, leaf, rows[at]) == page));
        }
        held
    }

    /// The run positions, in the read's direction, of the rows `read`
    /// takes up when the fences alone place its bounds (`Walk::rows` with
    /// nothing known): from the direction's end, each live row inside the
    /// bounds or on a page that holds a bound (`bound_rows`), until the
    /// limit's rows off those pages, so known inside, have counted, and one
    /// live row more.
    fn placed_rows(read: &ReadOf<'_>) -> Vec<usize> {
        let limit = usize::try_from(read.scan.limit).unwrap();
        let bound = bound_rows(read.index, read.rows, read.scan);
        let mut placed = Vec::new();
        let mut counted = 0;
        for at in in_direction(read.rows.len(), read.scan) {
            let on_bound = bound.contains(&at);
            if read.deleted.contains(&at) || !(on_bound || inside(read.scan, &read.orders[at])) {
                continue;
            }
            placed.push(at);
            if counted == limit {
                break;
            }
            counted += usize::from(!on_bound);
        }
        placed
    }

    /// One read of a run as the cost model sees it: the index, the run's
    /// rows and their order values, the run positions deleted, and the
    /// bounds.
    struct ReadOf<'a> {
        index: &'a FileIndex,
        rows: &'a [RowRef],
        orders: &'a [OrderValue],
        deleted: &'a BTreeSet<usize>,
        scan: &'a ScanBounds,
    }

    impl ReadOf<'_> {
        /// Asserts that this read, under `residency`, of which the model
        /// takes the run positions `taken`, fetched within RFD 025's Cost
        /// over its GETs' byte ranges `ranges` and decoded the data pages
        /// `decoded` counts, as `assert_reads_within_the_cost_promise` says,
        /// and counts what it saw in `seen`; the value pages it fetched, and
        /// those of the rows taken (`taken_pages`).
        fn assert_pages_within_the_promise(
            &self,
            residency: Residency,
            taken: &[usize],
            ranges: &[Range<u64>],
            decoded: &Handed,
            seen: &mut Seen,
            case: &str,
        ) -> (BTreeSet<PageAt>, BTreeSet<PageAt>) {
            let order_leaf = self.index.order_leaf().unwrap();
            let value_leaf = self.index.value_leaf(0).unwrap();
            let values = fetched(self.index, value_leaf, ranges);
            let order_pages = fetched(self.index, order_leaf, ranges);
            let live = |leaf: usize, pages: &BTreeSet<PageAt>| {
                pages
                    .intersection(&live_pages(self.index, leaf, self.rows, self.deleted))
                    .count()
            };
            let live_values = live(value_leaf, &values);
            seen.values += usize::from(!values.is_empty());
            seen.bridged += usize::from(live_values < values.len());
            seen.left_out += usize::from(gapped(&values));
            let taken_pages = taken_pages(self.index, value_leaf, self.rows, taken);
            let promised_values = promised(self, value_leaf, taken.len(), 0);
            let promised_orders = promised(self, order_leaf, taken.len(), 1);
            let decoded = (decoded.data_of(value_leaf), decoded.data_of(order_leaf));
            if residency == Residency::Disk {
                assert_eq!(
                    values, taken_pages,
                    "{case}: the value pages of the rows taken"
                );
                assert_eq!(order_pages, BTreeSet::new(), "{case}: no order GET");
                let pages = |leaf: usize, at: &[usize]| -> BTreeSet<PageAt> {
                    at.iter()
                        .map(|&at| page_of(self.index, leaf, self.rows[at]))
                        .collect()
                };
                let placed = placed_rows(self);
                assert_eq!(
                    decoded,
                    (
                        pages(value_leaf, taken).len(),
                        pages(order_leaf, &placed).len()
                    ),
                    "{case}: the value pages decoded, those of the rows taken, and the order \
                     pages, those of the rows the fences alone place"
                );
                seen.exact += usize::from(promised_values != taken_pages);
            } else {
                assert!(
                    values.is_subset(&promised_values),
                    "{case}: value pages {values:?}, beyond {promised_values:?}"
                );
                assert!(
                    order_pages.is_subset(&promised_orders),
                    "{case}: order pages {order_pages:?}, beyond {promised_orders:?}"
                );
                assert_eq!(
                    decoded,
                    (live_values, live(order_leaf, &order_pages)),
                    "{case}: the value and order pages decoded, those fetched that hold a live row"
                );
                seen.beyond += usize::from(!values.is_subset(&taken_pages));
            }
            (values, taken_pages)
        }
    }

    /// The pages of leaf `leaf` RFD 025's Cost lets `read` take when the
    /// fences alone place it: of the run's live rows that lie inside the
    /// bounds or on a page that holds a bound (`bound_rows`), in the read's
    /// direction, as many as the `taken` rows taken and the live rows on
    /// those pages together, and `past` more, with the pages between two
    /// of them that a read fetches with them (`with_bridged_gaps`). Rows
    /// the fences cannot place count nothing toward the limit, so beyond
    /// the rows taken a read takes up at most as many rows as the bound's
    /// page holds, and in the order column the one row past them that
    /// tells whether rows remain. A walk takes up every live row between two it takes up, so
    /// the pages between two of those rows' pages hold deleted rows alone.
    fn promised(read: &ReadOf<'_>, leaf: usize, taken: usize, past: usize) -> BTreeSet<PageAt> {
        let bound = bound_rows(read.index, read.rows, read.scan);
        let beyond = bound.iter().filter(|at| !read.deleted.contains(at)).count();
        let held = in_direction(read.rows.len(), read.scan)
            .into_iter()
            .filter(|at| {
                !read.deleted.contains(at)
                    && (inside(read.scan, &read.orders[*at]) || bound.contains(at))
            })
            .take(taken + beyond + past)
            .map(|at| page_of(read.index, leaf, read.rows[at]))
            .collect();
        with_bridged_gaps(read.index, leaf, &held)
    }

    /// `pages` of leaf `leaf` and, between each two of them in one row
    /// group, the pages between where those take at most
    /// `SPAN_GAP_BRIDGE_BYTES` by the compressed sizes the offset index
    /// gives them: the pages RFD 025's Cost lets a read fetch, in the GET
    /// of the spans around them, without decoding them.
    fn with_bridged_gaps(
        index: &FileIndex,
        leaf: usize,
        pages: &BTreeSet<PageAt>,
    ) -> BTreeSet<PageAt> {
        let mut with = pages.clone();
        for (&(row_group, page), &(next_group, next)) in pages.iter().zip(pages.iter().skip(1)) {
            let gap: BTreeSet<PageAt> = (page + 1..next).map(|at| (row_group, at)).collect();
            if row_group == next_group && bytes_of(index, leaf, &gap) <= SPAN_GAP_BRIDGE_BYTES {
                with.extend(gap);
            }
        }
        with
    }

    /// The pages of leaf `leaf` holding a row of the run `rows` whose run
    /// position `deleted` does not hold.
    fn live_pages(
        index: &FileIndex,
        leaf: usize,
        rows: &[RowRef],
        deleted: &BTreeSet<usize>,
    ) -> BTreeSet<PageAt> {
        (0..rows.len())
            .filter(|at| !deleted.contains(at))
            .map(|at| page_of(index, leaf, rows[at]))
            .collect()
    }

    /// Whether `pages` leave out a page between two of them in one row
    /// group.
    fn gapped(pages: &BTreeSet<PageAt>) -> bool {
        pages
            .iter()
            .zip(pages.iter().skip(1))
            .any(|(&(row_group, page), &(next_group, next))| {
                row_group == next_group && next > page + 1
            })
    }

    /// Every bounded read the cost model runs at `points`, ascending order
    /// values whose first lies below and whose last above every value of
    /// the run: in each direction, each point as `start` and as `before`,
    /// each with the other bound at the extreme point beyond it, which
    /// excludes nothing, and each pair `BOUND_GAPS` apart as both.
    fn bounded_scans(points: &[OrderValue], limit: u32) -> Vec<ScanBounds> {
        let (first, last) = (points[0].clone(), points[points.len() - 1].clone());
        let mut cuts = Vec::new();
        for (at, point) in points.iter().enumerate() {
            cuts.push((Some(point.clone()), None));
            cuts.push((None, Some(point.clone())));
            cuts.push((Some(point.clone()), Some(last.clone())));
            cuts.push((Some(first.clone()), Some(point.clone())));
            for gap in BOUND_GAPS {
                if let Some(upper) = points.get(at + gap) {
                    cuts.push((Some(point.clone()), Some(upper.clone())));
                }
            }
        }
        [Direction::OldestFirst, Direction::NewestFirst]
            .into_iter()
            .flat_map(|direction| {
                cuts.iter().map(move |(start, before)| ScanBounds {
                    start: start.clone(),
                    before: before.clone(),
                    ..bounds(direction, limit)
                })
            })
            .collect()
    }

    /// The run positions the cost model deletes from a run of `rows` rows:
    /// its first and last, and one of every `DELETE_EVERY` between.
    fn some_deleted(rows: usize) -> BTreeSet<usize> {
        (0..rows)
            .filter(|&at| at == 0 || at == rows - 1 || at % DELETE_EVERY == DELETED_OF_EVERY)
            .collect()
    }

    /// The run positions of the middle half of a run of `rows` rows: over
    /// a run of more than a few pages, a gap of several whole pages of
    /// deleted rows alone between two of live rows.
    fn middle_half_deleted(rows: usize) -> BTreeSet<usize> {
        let quarter = rows / 4;
        (quarter..rows - quarter).collect()
    }

    /// The run positions of the rows of the run `rows` on every other page
    /// of leaf `leaf` it spans, from its second: gaps of one whole page of
    /// deleted rows alone, each between two pages of live rows but where
    /// the run's last page is one of them.
    fn alternate_pages_deleted(index: &FileIndex, leaf: usize, rows: &[RowRef]) -> BTreeSet<usize> {
        let every_other: BTreeSet<PageAt> = pages_of(index, leaf, rows)
            .into_iter()
            .skip(1)
            .step_by(2)
            .collect();
        (0..rows.len())
            .filter(|&at| every_other.contains(&page_of(index, leaf, rows[at])))
            .collect()
    }

    /// A file the cost model reads: key 2's run, whose `j`th row has order
    /// value `value(j)` under `order_type`, read at the bound values
    /// `points`.
    struct CostFixture {
        fixture: Fixture,
        order_type: OrderType,
        value: Box<dyn Fn(usize) -> OrderValue>,
        points: Vec<OrderValue>,
    }

    /// The run of `RUN_KEY` in an index whose file is read through
    /// `gauged`, the Disk tier's local directory kept beside it.
    struct Probe<'g> {
        index: FileIndex,
        run: Run<'static>,
        gauged: &'g Gauged,
        _dir: tempfile::TempDir,
    }

    impl<'g> Probe<'g> {
        /// The run in the index of `opened` in `residency` with an order
        /// column of `order_type`, located on a fresh cache.
        async fn open(
            opened: &Opened,
            gauged: &'g Gauged,
            residency: Residency,
            order_type: OrderType,
        ) -> Self {
            let dir = tempfile::tempdir().unwrap();
            let index = index_of_type(opened, residency, dir.path(), 0, order_type).await;
            let run = Reads::new().await.run(&index).await;
            Self {
                index,
                run,
                gauged,
                _dir: dir,
            }
        }

        /// One read on a fresh cache, and the byte ranges of its GETs.
        async fn read(
            &self,
            scan: &ScanBounds,
            deleted: &dyn DeletedRows,
        ) -> (FileRows, Vec<Range<u64>>) {
            let reads = Reads::new().await;
            self.read_through(&reads, scan, deleted).await
        }

        /// One read through `reads`, and the byte ranges of its GETs.
        async fn read_through(
            &self,
            reads: &Reads,
            scan: &ScanBounds,
            deleted: &dyn DeletedRows,
        ) -> (FileRows, Vec<Range<u64>>) {
            self.gauged.take_ranges();
            let got = reads.read(&self.index, &self.run, scan, deleted).await;
            (got, self.gauged.take_ranges())
        }

        /// Asserts that the read of `scan` runs its `gets` GETs in one
        /// round, all in flight at once, on a fresh cache.
        async fn assert_one_round(
            &self,
            scan: &ScanBounds,
            deleted: &dyn DeletedRows,
            gets: usize,
            case: &str,
        ) {
            let reads = Reads::new().await;
            self.gauged.fill_to(gets);
            tokio::time::timeout(
                DEADLINE_GUARD,
                reads.read(&self.index, &self.run, scan, deleted),
            )
            .await
            .unwrap_or_else(|_| panic!("{case}: the read's {gets} GETs are not in flight at once"));
            self.gauged.take_ranges();
        }
    }

    /// The pages of leaf `leaf` holding the rows of the run `rows` at the
    /// run positions `taken`, with the pages between two of them a read
    /// fetches with them (`with_bridged_gaps`); none for none.
    fn taken_pages(
        index: &FileIndex,
        leaf: usize,
        rows: &[RowRef],
        taken: &[usize],
    ) -> BTreeSet<PageAt> {
        let held = taken
            .iter()
            .map(|&at| page_of(index, leaf, rows[at]))
            .collect();
        with_bridged_gaps(index, leaf, &held)
    }

    /// What the cost model saw across its reads: reads that fetched a
    /// value page, reads whose GETs were asserted one round, reads placed
    /// by the fences alone that took value pages past the rows taken, Disk
    /// reads whose bound's page, placed exactly first, spared value pages a
    /// read by the fences alone could take, reads that fetched a value
    /// page of deleted rows alone with the spans around it, and reads that
    /// left a page between two value pages they fetched out.
    #[derive(Default)]
    struct Seen {
        values: usize,
        rounds: usize,
        beyond: usize,
        exact: usize,
        bridged: usize,
        left_out: usize,
    }

    /// Reads `cost`'s run at every bounded read of its points, every limit
    /// of `COST_LIMITS`, with no row deleted, with `some_deleted`, and with
    /// whole pages deleted by `middle_half_deleted` and
    /// `alternate_pages_deleted`, in every residency, and asserts the rows
    /// and `next` are the model's, every read with more than one GET runs
    /// them in one round, some read by the fences alone takes the bound's
    /// page, some Disk read places its bound exactly, and the pages read
    /// are within RFD 025's Cost, each column's decoded data pages counted
    /// apart: under Disk, where the order column is local and the bound's
    /// page is placed exactly first, the value pages fetched are those of
    /// the rows taken and the gaps between them the bridge takes
    /// (`taken_pages`), no order page is fetched, and the pages decoded are
    /// exactly the value pages of the rows taken and the order pages of the
    /// rows the fences alone place (`placed_rows`), which the chunk file
    /// is read for first; under Full and Bloom on a cold cache, the value
    /// pages lie within `promised`, the order pages within it with the row
    /// past, and each column's pages decoded are exactly its pages fetched
    /// that hold a live row of the run, so no page of deleted rows alone
    /// fetched with its neighbours is decoded.
    async fn assert_reads_within_the_cost_promise(cost: &CostFixture) -> Seen {
        let (opened, gauged) = store_gauged(&cost.fixture).await;
        let mut seen = Seen::default();
        for residency in Residency::ALL {
            let probe = Probe::open(&opened, &gauged, residency, cost.order_type).await;
            let (index, run) = (&probe.index, &probe.run);
            let rows = rows_of(run);
            let orders: Vec<OrderValue> = (0..rows.len()).map(&cost.value).collect();
            let value_leaf = index.value_leaf(0).unwrap();
            let deleted_sets = [
                BTreeSet::new(),
                some_deleted(rows.len()),
                middle_half_deleted(rows.len()),
                alternate_pages_deleted(index, value_leaf, &rows),
            ];
            for deleted in deleted_sets {
                let positions: BTreeSet<u64> = deleted
                    .iter()
                    .map(|&at| index.position(rows[at]).0)
                    .collect();
                let file_deletes = FileDeletes::of_rows(positions.iter().copied());
                let is_deleted = Some(&file_deletes);
                for limit in COST_LIMITS {
                    for scan in bounded_scans(&cost.points, limit) {
                        let case = format!(
                            "{residency} {} deleted {deleted:?} {scan:?}",
                            cost.order_type
                        );
                        let expected =
                            model(index, &rows, &orders, &cost.fixture.values, &deleted, &scan);
                        let handed = Handed::now();
                        let (got, ranges) = probe.read(&scan, &is_deleted).await;
                        let decoded = Handed::now().since(&handed);
                        assert_eq!(got.answer.rows, expected.rows, "{case}");
                        assert_eq!(got.answer.beyond, expected.beyond, "{case}");
                        let read = ReadOf {
                            index,
                            rows: &rows,
                            orders: &orders,
                            deleted: &deleted,
                            scan: &scan,
                        };
                        let (values, taken) = read.assert_pages_within_the_promise(
                            residency,
                            &expected.taken,
                            &ranges,
                            &decoded,
                            &mut seen,
                            &case,
                        );
                        if ranges.len() > 1 {
                            let case = format!("{case}: value pages {values:?}, taken {taken:?}");
                            probe
                                .assert_one_round(&scan, &is_deleted, ranges.len(), &case)
                                .await;
                            seen.rounds += 1;
                        }
                    }
                }
            }
        }
        assert!(seen.values > 0, "some read fetched a value page");
        assert!(seen.rounds > 0, "some read ran more than one GET");
        assert!(
            seen.beyond > 0,
            "some read by the fences alone took the bound's page"
        );
        assert!(seen.exact > 0, "some Disk read placed its bound exactly");
        seen
    }

    /// `points` for a run of `rows` rows of `runs_of`: every row's order
    /// value, one between each two, and one below and one above the run.
    fn long_points(rows: usize) -> Vec<OrderValue> {
        let rows = i64::try_from(rows).unwrap();
        (-1..=rows)
            .flat_map(|j| {
                [
                    OrderValue::Long(j * ORDER_STEP),
                    OrderValue::Long(j * ORDER_STEP + HALF_STEP),
                ]
            })
            .collect()
    }

    /// `points` for a run of `rows` rows ordered by `value` under
    /// `order_type`: one below every value, each row's value and one
    /// between it and the next, and one above every value.
    fn byte_points(
        order_type: OrderType,
        rows: usize,
        value: fn(usize) -> Vec<u8>,
    ) -> Vec<OrderValue> {
        let mut points = vec![travelling(order_type, Vec::new())];
        for j in 0..rows {
            let mut between = value(j);
            between.push(b'5');
            points.push(travelling(order_type, value(j)));
            points.push(travelling(order_type, between));
        }
        points.push(travelling(order_type, vec![ABOVE_EVERY_PREFIX]));
        points
    }

    /// The cost fixture of `fixture`, a run of `rows` rows of `runs_of`.
    fn long_cost(fixture: Fixture, rows: usize) -> CostFixture {
        CostFixture {
            fixture,
            order_type: OrderType::Long,
            value: Box::new(|j| order_at(i64::try_from(j).unwrap())),
            points: long_points(rows),
        }
    }

    /// The cost fixture of `prefixed_run_of(None, PREFIXED_RUN_ROWS, value)`
    /// read as `order_type`.
    fn byte_cost(order_type: OrderType, value: fn(usize) -> Vec<u8>) -> CostFixture {
        CostFixture {
            fixture: prefixed_run_of(None, PREFIXED_RUN_ROWS, value),
            order_type,
            value: Box::new(move |j| travelling(order_type, value(j))),
            points: byte_points(order_type, PREFIXED_RUN_ROWS, value),
        }
    }

    /// Under an integer order column, every bounded read of a run in one
    /// row group between two neighbouring keys' rows is one round within
    /// RFD 025's Cost, and some fetches a page of deleted rows with its
    /// spans.
    #[tokio::test]
    async fn test_read_run_keeps_the_cost_promise_under_integer_orders_in_one_row_group() {
        let seen = assert_reads_within_the_cost_promise(&long_cost(bounded_run(0), 30)).await;
        assert!(
            seen.bridged > 0,
            "some read fetched a page of deleted rows with its spans"
        );
    }

    /// Under an integer order column, every bounded read of a run over
    /// exactly two pages, both shared with a neighbouring key, is one round
    /// within RFD 025's Cost.
    #[tokio::test]
    async fn test_read_run_keeps_the_cost_promise_under_integer_orders_over_two_shared_pages() {
        let run = runs_of(&[(1, 3), (2, 4), (3, 3)], 10);
        assert_reads_within_the_cost_promise(&long_cost(run, 4)).await;
    }

    /// Under an integer order column, every bounded read of a run across
    /// three row groups is one round within RFD 025's Cost.
    #[tokio::test]
    async fn test_read_run_keeps_the_cost_promise_under_integer_orders_across_row_groups() {
        let run = runs_of(&[(1, 5), (2, 16), (3, 3)], 8);
        assert_reads_within_the_cost_promise(&long_cost(run, 16)).await;
    }

    /// The bytes of each value of the cost model's wide run: drawn at
    /// random, so a page of `PAGE` of them takes more than half
    /// `SPAN_GAP_BRIDGE_BYTES` and less than all of it, and a read fetches
    /// a gap of one page of deleted rows with the spans around it and
    /// leaves a gap of two out.
    const BRIDGE_PAGE_VALUE_BYTES: usize = 48 * 1024;

    /// Over a run whose value pages each take more than half the bridge,
    /// with whole pages of it deleted, every bounded read is one round
    /// within RFD 025's Cost: a gap of one page of deleted rows is fetched
    /// with the spans around it, a gap of several is left out, and neither
    /// is decoded.
    #[tokio::test]
    async fn test_read_run_across_deleted_pages_fetches_only_the_gaps_the_bridge_takes() {
        let wide = with_random_values(bounded_run(0), BRIDGE_PAGE_VALUE_BYTES);
        let seen = assert_reads_within_the_cost_promise(&long_cost(wide, 30)).await;
        assert!(
            seen.bridged > 0,
            "some read fetched a page of deleted rows with its spans"
        );
        assert!(
            seen.left_out > 0,
            "some read left a gap of deleted pages out"
        );
    }

    /// Asserts `assert_reads_within_the_cost_promise` over the run of
    /// `byte_cost(order_type, value)`, and that some read fetches a page of
    /// deleted rows with its spans.
    async fn assert_byte_orders_within_the_promise(
        order_type: OrderType,
        value: fn(usize) -> Vec<u8>,
    ) {
        let seen = assert_reads_within_the_cost_promise(&byte_cost(order_type, value)).await;
        assert!(
            seen.bridged > 0,
            "some read fetched a page of deleted rows with its spans"
        );
    }

    /// Under a string order column, every bounded read of a run whose values
    /// all share a long prefix is one round within RFD 025's Cost.
    #[tokio::test]
    async fn test_read_run_keeps_the_cost_promise_under_string_orders_of_one_prefix() {
        assert_byte_orders_within_the_promise(OrderType::String, prefixed).await;
    }

    /// Under a string order column, every bounded read of a run whose two
    /// halves have different prefixes is one round within RFD 025's Cost.
    #[tokio::test]
    async fn test_read_run_keeps_the_cost_promise_under_string_orders_of_two_prefixes() {
        assert_byte_orders_within_the_promise(OrderType::String, halved).await;
    }

    /// Under a string order column, every bounded read of a run whose first
    /// prefix fills exactly two whole pages is one round within RFD 025's
    /// Cost.
    #[tokio::test]
    async fn test_read_run_keeps_the_cost_promise_under_string_orders_with_two_low_pages() {
        assert_byte_orders_within_the_promise(OrderType::String, two_low_pages).await;
    }

    /// Under a binary order column, every bounded read of a run whose values
    /// all share a long prefix is one round within RFD 025's Cost.
    #[tokio::test]
    async fn test_read_run_keeps_the_cost_promise_under_binary_orders_of_one_prefix() {
        assert_byte_orders_within_the_promise(OrderType::Binary, prefixed).await;
    }

    /// Under a binary order column, every bounded read of a run whose two
    /// halves have different prefixes is one round within RFD 025's Cost.
    #[tokio::test]
    async fn test_read_run_keeps_the_cost_promise_under_binary_orders_of_two_prefixes() {
        assert_byte_orders_within_the_promise(OrderType::Binary, halved).await;
    }

    /// Under a binary order column, every bounded read of a run whose first
    /// prefix fills exactly two whole pages is one round within RFD 025's
    /// Cost.
    #[tokio::test]
    async fn test_read_run_keeps_the_cost_promise_under_binary_orders_with_two_low_pages() {
        assert_byte_orders_within_the_promise(OrderType::Binary, two_low_pages).await;
    }

    /// The run position, in `prefixed_run(LONG_PREFIXED_RUN_ROWS)`, of
    /// the first row of an order page in the middle of the run.
    const PAGE_START_ROW: usize = 63;
    /// The run position, in `prefixed_run(LONG_PREFIXED_RUN_ROWS)`, of
    /// a row in the middle of the page before the run's last page.
    const NEAR_THE_END_ROW: usize = 117;

    /// Newest first before the first value of an order page, over a long
    /// run: the page's fence is the bound itself, so the page and every
    /// page past it lie at or above the bound, and neither is read in any
    /// column; one round reads the page the rows taken lie on.
    #[tokio::test]
    async fn test_read_run_newest_first_reads_no_page_past_one_outside_the_bound() {
        let limit = 3;
        let fixture = prefixed_run(LONG_PREFIXED_RUN_ROWS);
        let (opened, gauged) = store_gauged(&fixture).await;
        for order_type in [OrderType::String, OrderType::Binary] {
            let value = |j: usize| travelling(order_type, prefixed(j));
            let scan = ScanBounds {
                before: Some(value(PAGE_START_ROW)),
                ..bounds(Direction::NewestFirst, limit)
            };
            let taken = [PAGE_START_ROW - 1, PAGE_START_ROW - 2, PAGE_START_ROW - 3];
            for residency in Residency::ALL {
                let probe = Probe::open(&opened, &gauged, residency, order_type).await;
                let (index, run) = (&probe.index, &probe.run);
                let rows = rows_of(run);
                let case = format!("{residency} {order_type}");
                let (got, ranges) = probe.read(&scan, &none_deleted).await;
                let orders: Vec<OrderValue> = (0..rows.len()).map(value).collect();
                let expected = model(
                    index,
                    &rows,
                    &orders,
                    &fixture.values,
                    &BTreeSet::new(),
                    &scan,
                );
                assert_eq!(expected.taken, taken);
                assert_eq!(got.answer.rows, expected.rows, "{case}");
                assert!(got.answer.beyond, "{case}");
                let value_leaf = index.value_leaf(0).unwrap();
                let held: BTreeSet<PageAt> =
                    pages_of(index, value_leaf, &rows[PAGE_START_ROW - 4..PAGE_START_ROW])
                        .into_iter()
                        .collect();
                let values = fetched(index, value_leaf, &ranges);
                let (read, needed) = (
                    bytes_of(index, value_leaf, &values),
                    bytes_of(index, value_leaf, &held),
                );
                assert!(needed > 0, "{case}");
                assert_eq!(
                    read, needed,
                    "{case}: value pages {values:?}, needed {held:?}"
                );
                if residency != Residency::Disk {
                    let order_leaf = index.order_leaf().unwrap();
                    assert_eq!(fetched(index, order_leaf, &ranges), held, "{case}");
                }
                probe
                    .assert_one_round(&scan, &none_deleted, ranges.len(), &case)
                    .await;
            }
        }
    }

    /// The value pages, of the run `rows`, that the rows `taken` lie on,
    /// and the bytes those pages take.
    fn taken_bytes(index: &FileIndex, rows: &[RowRef], taken: &[usize]) -> (BTreeSet<PageAt>, u64) {
        let value_leaf = index.value_leaf(0).unwrap();
        let pages = taken_pages(index, value_leaf, rows, taken);
        let bytes = bytes_of(index, value_leaf, &pages);
        (pages, bytes)
    }

    /// Newest first before a value in the middle of the page before the
    /// run's last, over a long run: the fences place the bound on that page
    /// and rule out the run's last page, which holds the next key's rows
    /// too, so in every residency one round reads the value pages the rows
    /// taken lie on, 476 bytes of string values where the column index's
    /// placement read the run's last page too, 640, and the order pages
    /// from the bound's page to the row past those taken.
    #[tokio::test]
    async fn test_read_run_beside_a_straddling_run_edge_page_reads_the_taken_rows_pages() {
        let limit = 3;
        let fixture = prefixed_run(LONG_PREFIXED_RUN_ROWS);
        let (opened, gauged) = store_gauged(&fixture).await;
        let string_value_bytes = 476;
        for order_type in [OrderType::String, OrderType::Binary] {
            let value = |j: usize| travelling(order_type, prefixed(j));
            let scan = ScanBounds {
                before: Some(value(NEAR_THE_END_ROW)),
                ..bounds(Direction::NewestFirst, limit)
            };
            for residency in Residency::ALL {
                let probe = Probe::open(&opened, &gauged, residency, order_type).await;
                let (index, run) = (&probe.index, &probe.run);
                let rows = rows_of(run);
                let case = format!("{residency} {order_type}");
                let (got, ranges) = probe.read(&scan, &none_deleted).await;
                let orders: Vec<OrderValue> = (0..rows.len()).map(value).collect();
                let expected = model(
                    index,
                    &rows,
                    &orders,
                    &fixture.values,
                    &BTreeSet::new(),
                    &scan,
                );
                assert_eq!(got.answer.rows, expected.rows, "{case}");
                assert!(got.answer.beyond, "{case}");
                let (held, needed) = taken_bytes(index, &rows, &expected.taken);
                assert_eq!(held.len(), 2, "{case}");
                if order_type == OrderType::String {
                    assert_eq!(needed, string_value_bytes, "{case}");
                }
                let value_leaf = index.value_leaf(0).unwrap();
                assert_eq!(fetched(index, value_leaf, &ranges), held, "{case}");
                if residency != Residency::Disk {
                    let order_leaf = index.order_leaf().unwrap();
                    let walked: BTreeSet<PageAt> = pages_of(
                        index,
                        order_leaf,
                        &rows[NEAR_THE_END_ROW - 6..=NEAR_THE_END_ROW + 1],
                    )
                    .into_iter()
                    .collect();
                    assert_eq!(fetched(index, order_leaf, &ranges), walked, "{case}");
                }
                probe
                    .assert_one_round(&scan, &none_deleted, ranges.len(), &case)
                    .await;
            }
        }
    }

    /// Under Disk, newest first with a limit of one before a value in the
    /// middle of a page or at a row's own, every bound of an integer run
    /// reads the one value page, 238 bytes, the row taken lies on: the
    /// bound's page is read from the chunk file first and places its rows
    /// exactly, where a placement by the fences alone takes up the bound's
    /// page and the row past it, on a second page for most bounds.
    #[tokio::test]
    async fn test_read_run_under_disk_reads_the_one_value_page_of_the_row_taken() {
        let limit = 1;
        let page_value_bytes = 238;
        let fixture = bounded_run(0);
        let (opened, gauged) = store_gauged(&fixture).await;
        let probe = Probe::open(&opened, &gauged, Residency::Disk, OrderType::Long).await;
        let (index, run) = (&probe.index, &probe.run);
        let rows = rows_of(run);
        let orders: Vec<OrderValue> = (0..rows.len())
            .map(|j| order_at(i64::try_from(j).unwrap()))
            .collect();
        let value_leaf = index.value_leaf(0).unwrap();
        let (mut bounds_read, mut beside_the_row) = (0, 0);
        for j in 1..i64::try_from(rows.len()).unwrap() {
            for before in [j * ORDER_STEP, j * ORDER_STEP + HALF_STEP] {
                let scan = ScanBounds {
                    before: Some(OrderValue::Long(before)),
                    ..bounds(Direction::NewestFirst, limit)
                };
                let case = format!("before {before}");
                let (got, ranges) = probe.read(&scan, &none_deleted).await;
                let expected = model(
                    index,
                    &rows,
                    &orders,
                    &fixture.values,
                    &BTreeSet::new(),
                    &scan,
                );
                assert_eq!(got.answer.rows, expected.rows, "{case}");
                assert_eq!(got.answer.beyond, expected.beyond, "{case}");
                let (held, needed) = taken_bytes(index, &rows, &expected.taken);
                assert_eq!(held.len(), 1, "{case}");
                assert_eq!(needed, page_value_bytes, "{case}");
                assert_eq!(fetched(index, value_leaf, &ranges), held, "{case}");
                let none = BTreeSet::new();
                let read = ReadOf {
                    index,
                    rows: &rows,
                    orders: &orders,
                    deleted: &none,
                    scan: &scan,
                };
                let promise = promised(&read, value_leaf, expected.taken.len(), 0);
                beside_the_row += usize::from(promise.len() > held.len());
                bounds_read += 1;
            }
        }
        assert!(
            beside_the_row * 2 > bounds_read,
            "for most bounds the fences alone take a second page: {beside_the_row} of {bounds_read}"
        );
    }

    /// Fresh reads whose page cache holds order page `page` of `index`, as
    /// a Scan that read it leaves it.
    async fn reads_holding(index: &FileIndex, (row_group, page): PageAt) -> Reads {
        let reads = Reads::new().await;
        let chunk = index.order_chunk(row_group);
        index
            .handle()
            .read_pages(
                &[chunk.page_range(page)],
                &reads.cache,
                Issue::default(),
                &reads.window,
            )
            .await
            .unwrap();
        reads
    }

    /// The page cache holds an order page beside a bound's when a Scan
    /// pages through a run by order value: with it in memory, a read places
    /// the bound exactly before its round and reads the value pages the rows
    /// taken lie on, where on a cold cache the limit's rows shifted past the
    /// bound's page reach one more value page; both one round, and the
    /// cached order page not fetched again.
    #[tokio::test]
    async fn test_read_run_places_a_bound_exactly_from_a_cached_order_page() {
        let limit = 6;
        let fixture = prefixed_run(LONG_PREFIXED_RUN_ROWS);
        let (opened, gauged) = store_gauged(&fixture).await;
        let order_type = OrderType::String;
        let value = |j: usize| travelling(order_type, prefixed(j));
        let scan = ScanBounds {
            before: Some(value(NEAR_THE_END_ROW)),
            ..bounds(Direction::NewestFirst, limit)
        };
        for residency in [Residency::Full, Residency::Bloom] {
            let probe = Probe::open(&opened, &gauged, residency, order_type).await;
            let (index, run) = (&probe.index, &probe.run);
            let rows = rows_of(run);
            let orders: Vec<OrderValue> = (0..rows.len()).map(value).collect();
            let expected = model(
                index,
                &rows,
                &orders,
                &fixture.values,
                &BTreeSet::new(),
                &scan,
            );
            let (held, _) = taken_bytes(index, &rows, &expected.taken);
            let (value_leaf, order_leaf) =
                (index.value_leaf(0).unwrap(), index.order_leaf().unwrap());
            let case = residency.to_string();

            let (cold, ranges) = probe.read(&scan, &none_deleted).await;
            assert_eq!(cold.answer.rows, expected.rows, "{case}");
            let shifted = fetched(index, value_leaf, &ranges);
            assert!(held.is_subset(&shifted), "{case}");
            assert_eq!(
                shifted.len(),
                held.len() + 1,
                "{case}: {shifted:?} {held:?}"
            );

            let bound_page = page_of(index, order_leaf, rows[NEAR_THE_END_ROW]);
            let reads = reads_holding(index, bound_page).await;
            let (warm, ranges) = probe.read_through(&reads, &scan, &none_deleted).await;
            assert_eq!(warm.answer.rows, expected.rows, "{case}");
            assert_eq!(warm.answer.beyond, expected.beyond, "{case}");
            assert_eq!(fetched(index, value_leaf, &ranges), held, "{case}");
            let orders_fetched = fetched(index, order_leaf, &ranges);
            assert!(
                !orders_fetched.contains(&bound_page),
                "{case}: {orders_fetched:?}"
            );
            assert!(
                !orders_fetched.is_empty(),
                "{case}: the rows past the bound's page"
            );

            let reads = reads_holding(index, bound_page).await;
            gauged.fill_to(ranges.len());
            tokio::time::timeout(
                DEADLINE_GUARD,
                probe.read_through(&reads, &scan, &none_deleted),
            )
            .await
            .expect("every GET of the warm read is in flight at once");
        }
    }

    /// `limit` as a read's limit.
    fn limit_u32(limit: usize) -> u32 {
        u32::try_from(limit).unwrap()
    }

    /// A run over exactly two order pages, each shared with a neighbouring
    /// key, is read in one round: the fences place the bound on the second,
    /// and the first holds no row above it.
    #[tokio::test]
    async fn test_read_run_over_two_shared_pages_reads_one_round() {
        let limit = 1;
        let (opened, gauged) = store_gauged(&runs_of(&[(1, 3), (2, 4), (3, 3)], 10)).await;
        let first_run_row = 3;
        let start_one = ScanBounds {
            start: Some(order_at(1)),
            ..bounds(Direction::OldestFirst, limit)
        };
        for residency in Residency::ALL {
            let probe = Probe::open(&opened, &gauged, residency, OrderType::Long).await;
            let index = &probe.index;
            let (got, ranges) = probe.read(&start_one, &none_deleted).await;
            assert_eq!(
                got.answer.rows,
                [row_answer(first_run_row + 1, 1, 1)],
                "{residency}"
            );
            assert!(got.answer.beyond, "{residency}");
            let order_gets = usize::from(residency != Residency::Disk);
            assert_eq!(ranges.len(), 1 + order_gets, "{residency}");
            let value_leaf = index.value_leaf(0).unwrap();
            assert_eq!(
                fetched(index, value_leaf, &ranges),
                [(0, 1)].into_iter().collect(),
                "{residency}"
            );
            let case = residency.to_string();
            probe
                .assert_one_round(&start_one, &none_deleted, ranges.len(), &case)
                .await;
        }
    }

    /// Whatever the order column index, the read is the same: the fences
    /// place the bounds, not the column index. Under a string or binary
    /// order column whose every value of a long run shares a 32-byte prefix
    /// with the bound, with the index full length, truncated short of that
    /// prefix, or none, newest first before the run's last value reads the
    /// value pages of the rows taken alone, and oldest first from its first
    /// value those of the rows on the bound's page and the limit's rows
    /// past it, since rows on the bound's page count nothing until their
    /// order values are read (RFD 025, Cost), except under Disk, where the
    /// bound's page is decoded first and the rows taken's pages alone are
    /// read; each reads the order pages from the bound's to the row past
    /// those taken, in one round, in every residency; under a long order
    /// column, with the
    /// index whole or none, so does a bounded read
    /// (`assert_long_read_whatever_the_index`).
    #[tokio::test]
    async fn test_read_run_reads_the_rows_taken_whatever_the_order_column_index() {
        let limit = 3;
        let last = LONG_PREFIXED_RUN_ROWS - 1;
        for (truncate, statistics) in [
            (Some(TRUNCATED_INDEX_BYTES), None),
            (None, None),
            (
                None,
                Some(parquet::file::properties::EnabledStatistics::Chunk),
            ),
        ] {
            for shape in [prefixed as fn(usize) -> Vec<u8>, halved] {
                let mut fixture = prefixed_run_of(truncate, LONG_PREFIXED_RUN_ROWS, shape);
                if let Some(statistics) = statistics {
                    fixture.layout.statistics = statistics;
                }
                let (opened, gauged) = store_gauged(&fixture).await;
                for order_type in [OrderType::String, OrderType::Binary] {
                    let value = |j: usize| travelling(order_type, shape(j));
                    // Each case: the bounds, the rows taken, the run rows
                    // whose order pages a cold read walks, and, where they
                    // differ from the rows taken's, the run rows whose
                    // value pages it reads before the bound's page is
                    // decoded, which under Disk it is first.
                    let cases = [
                        (
                            ScanBounds {
                                start: Some(value(0)),
                                ..bounds(Direction::OldestFirst, limit)
                            },
                            [0, 1, 2],
                            0..=limit_index(limit) * 2,
                            Some(0..limit_index(limit) * 2),
                        ),
                        (
                            ScanBounds {
                                before: Some(value(last)),
                                ..bounds(Direction::NewestFirst, limit)
                            },
                            [last - 1, last - 2, last - 3],
                            last - 4..=last - 1,
                            None,
                        ),
                    ];
                    for residency in Residency::ALL {
                        let probe = Probe::open(&opened, &gauged, residency, order_type).await;
                        let (index, run) = (&probe.index, &probe.run);
                        let rows = rows_of(run);
                        let orders: Vec<OrderValue> = (0..rows.len()).map(value).collect();
                        let order_leaf = index.order_leaf().unwrap();
                        let value_leaf = index.value_leaf(0).unwrap();
                        if statistics.is_some() {
                            assert_no_column_index(&opened, order_leaf).await;
                        }
                        for (scan, taken, order_rows, cold_value_rows) in &cases {
                            let case = format!(
                                "{residency} {order_type} {truncate:?} {statistics:?} {scan:?}"
                            );
                            let (got, ranges) = probe.read(scan, &none_deleted).await;
                            let expected = model(
                                index,
                                &rows,
                                &orders,
                                &fixture.values,
                                &BTreeSet::new(),
                                scan,
                            );
                            assert_eq!(expected.taken, taken, "{case}");
                            assert_eq!(got.answer.rows, expected.rows, "{case}");
                            assert!(got.answer.beyond, "{case}");
                            let (held, needed) = taken_bytes(index, &rows, &expected.taken);
                            let values = fetched(index, value_leaf, &ranges);
                            assert!(needed > 0, "{case}");
                            match cold_value_rows {
                                Some(read) if residency != Residency::Disk => {
                                    let read: BTreeSet<PageAt> =
                                        pages_of(index, value_leaf, &rows[read.clone()])
                                            .into_iter()
                                            .collect();
                                    assert!(
                                        held.is_subset(&read) && held != read,
                                        "{case}: a page past the rows taken's"
                                    );
                                    assert_eq!(values, read, "{case}");
                                }
                                Some(_) | None => assert_eq!(values, held, "{case}"),
                            }
                            if residency != Residency::Disk {
                                let walked: BTreeSet<PageAt> =
                                    pages_of(index, order_leaf, &rows[order_rows.clone()])
                                        .into_iter()
                                        .collect();
                                assert_eq!(fetched(index, order_leaf, &ranges), walked, "{case}");
                            }
                            probe
                                .assert_one_round(scan, &none_deleted, ranges.len(), &case)
                                .await;
                        }
                    }
                }
            }
        }
        assert_long_read_whatever_the_index().await;
    }

    /// Asserts that the file `opened` holds carries no column index on leaf
    /// `leaf`, as a writer of chunk statistics alone leaves it: the premise
    /// of a read without one.
    async fn assert_no_column_index(opened: &Opened, leaf: usize) {
        let loaded = opened.handle.load_metadata().await.unwrap();
        assert!(
            loaded
                .column_index()
                .is_none_or(|groups| groups.iter().all(|group| matches!(
                    group[leaf],
                    parquet::file::page_index::column_index::ColumnIndexMetaData::NONE
                ))),
            "leaf {leaf} has no column index"
        );
    }

    /// Under a long order column, with the column index whole or none,
    /// oldest first from the order value of `bounded_run`'s row 19 with a
    /// limit of 2 answers rows 19 and 20 and row 21's order value, in one
    /// round: a GET of the value pages and one of the order pages, the
    /// order pages read locally under Disk.
    async fn assert_long_read_whatever_the_index() {
        let start = ScanBounds {
            start: Some(order_at(19)),
            ..bounds(Direction::OldestFirst, 2)
        };
        for statistics in [
            None,
            Some(parquet::file::properties::EnabledStatistics::Chunk),
        ] {
            let mut fixture = bounded_run(0);
            if let Some(statistics) = statistics {
                fixture.layout.statistics = statistics;
            }
            let (opened, gauged) = store_gauged(&fixture).await;
            for residency in Residency::ALL {
                let case = format!("{residency} long {statistics:?}");
                let probe = Probe::open(&opened, &gauged, residency, OrderType::Long).await;
                if statistics.is_some() {
                    assert_no_column_index(&opened, probe.index.order_leaf().unwrap()).await;
                }
                let stats = opened.stats();
                let together = 1 + usize::from(residency != Residency::Disk);
                gauged.fill_to(together);
                let before = costs(&stats);
                let (got, _) =
                    tokio::time::timeout(DEADLINE_GUARD, probe.read(&start, &none_deleted))
                        .await
                        .unwrap_or_else(|_| {
                            panic!("{case}: every GET of the read is in flight at once")
                        });
                assert_eq!(got.answer.rows, start_answer(1), "{case}");
                assert!(got.answer.beyond, "{case}");
                let expected = match residency {
                    Residency::Full | Residency::Bloom => (2, 0),
                    Residency::Disk => (1, 1),
                };
                assert_eq!(since(&stats, before), expected, "{case}");
                assert_eq!(gauged.high_water(), together, "{case}");
            }
        }
    }

    /// `limit` as a run position.
    fn limit_index(limit: u32) -> usize {
        usize::try_from(limit).unwrap()
    }

    /// The run position, in `two_low_pages`, of the bound of the reads of
    /// `test_read_run_with_two_low_pages_at_a_bound_reads_one_round`.
    const LOW_BOUND_ROW: usize = 5;

    /// Under a 16-byte truncated index, oldest first from a value of the
    /// run's low prefix, whose two whole pages share one truncated entry:
    /// the fences place the bound on one page, so the read is one round and
    /// reads the value pages of the rows taken, and a `before` bound that
    /// excludes nothing changes neither.
    #[tokio::test]
    async fn test_read_run_with_two_low_pages_at_a_bound_reads_one_round() {
        let limit = 3;
        let fixture = prefixed_run_of(
            Some(TRUNCATED_INDEX_BYTES),
            PREFIXED_RUN_ROWS,
            two_low_pages,
        );
        let (opened, gauged) = store_gauged(&fixture).await;
        for order_type in [OrderType::String, OrderType::Binary] {
            let value = |j: usize| travelling(order_type, two_low_pages(j));
            let start = ScanBounds {
                start: Some(value(LOW_BOUND_ROW)),
                ..bounds(Direction::OldestFirst, limit)
            };
            let both = ScanBounds {
                before: Some(travelling(order_type, vec![ABOVE_EVERY_PREFIX])),
                ..start.clone()
            };
            let taken = [LOW_BOUND_ROW, LOW_BOUND_ROW + 1, LOW_BOUND_ROW + 2];
            for residency in Residency::ALL {
                let probe = Probe::open(&opened, &gauged, residency, order_type).await;
                let (index, run) = (&probe.index, &probe.run);
                let rows = rows_of(run);
                let orders: Vec<OrderValue> = (0..rows.len()).map(value).collect();
                let (held, needed) = taken_bytes(index, &rows, &taken);
                assert!(needed > 0);
                let value_leaf = index.value_leaf(0).unwrap();
                for scan in [&start, &both] {
                    let case = format!("{residency} {order_type} {scan:?}");
                    let (got, ranges) = probe.read(scan, &none_deleted).await;
                    let expected = model(
                        index,
                        &rows,
                        &orders,
                        &fixture.values,
                        &BTreeSet::new(),
                        scan,
                    );
                    assert_eq!(expected.taken, taken, "{case}");
                    assert_eq!(got.answer.rows, expected.rows, "{case}");
                    assert!(got.answer.beyond, "{case}");
                    let values = fetched(index, value_leaf, &ranges);
                    assert_eq!(
                        values, held,
                        "{case}: value pages {values:?}, taken {held:?}"
                    );
                    probe
                        .assert_one_round(scan, &none_deleted, ranges.len(), &case)
                        .await;
                }
            }
        }
    }

    /// Under a 16-byte truncated index whose every entry is one prefix,
    /// oldest first before a value of the run's second half, with a limit
    /// that the rows before the bound's page fill: the row past the limit
    /// is the next page's `start`, and no order page past the one it lies on
    /// is read, however many pages share the prefix past it.
    #[tokio::test]
    async fn test_read_run_reads_one_row_past_the_limit_for_the_next_page() {
        let limit = LOW_PREFIX_ROWS;
        let before_row = 20;
        let fixture = prefixed_run_of(Some(TRUNCATED_INDEX_BYTES), PREFIXED_RUN_ROWS, halved);
        let (opened, gauged) = store_gauged(&fixture).await;
        let order_type = OrderType::String;
        let value = |j: usize| travelling(order_type, halved(j));
        let scan = ScanBounds {
            before: Some(value(before_row)),
            ..bounds(Direction::OldestFirst, limit_u32(limit))
        };
        for residency in [Residency::Full, Residency::Bloom] {
            let probe = Probe::open(&opened, &gauged, residency, order_type).await;
            let (index, run) = (&probe.index, &probe.run);
            let rows = rows_of(run);
            let (got, ranges) = probe.read(&scan, &none_deleted).await;
            assert_eq!(got.answer.rows.len(), limit, "{residency}");
            assert!(got.answer.beyond, "{residency}");
            let order_leaf = index.order_leaf().unwrap();
            let walked: BTreeSet<PageAt> = pages_of(index, order_leaf, &rows[..=limit])
                .into_iter()
                .collect();
            assert_eq!(fetched(index, order_leaf, &ranges), walked, "{residency}");
        }
    }

    /// Order pages of the short run of the work test: a few.
    const SHORT_RUN_PAGES: usize = 10;
    /// Order pages of the long run of the work test: a thousand times the
    /// short one's.
    const LONG_RUN_PAGES: usize = 10_000;
    /// The limit of the work test's reads.
    const WORK_LIMIT: u32 = 10;

    /// What one warm read of a run costs in work: the order pages its walk
    /// steps on, and the steps its placement's binary searches over the
    /// fences take.
    #[derive(Debug, PartialEq, Eq)]
    struct Work {
        pages: usize,
        steps: usize,
    }

    /// The work of a warm read of `scan` of a run of `pages` order pages,
    /// given the run's rows, in Full: the read once to warm the cache, then
    /// counted. `scan` asks `WORK_LIMIT` rows, which the read must return.
    async fn warm_work(pages: usize, scan: impl Fn(usize) -> ScanBounds) -> Work {
        let rows = pages * PAGE;
        let opened = store(&runs_of(&[(1, 1), (2, rows), (3, 1)], rows + 2)).await;
        let dir = tempfile::tempdir().unwrap();
        let index = index_in(&opened, Residency::Full, dir.path(), 0).await;
        let reads = Reads::new().await;
        let run = reads.run(&index).await;
        let scan = scan(rows);
        let read = || index.read_run(&run, &scan, &[0], &none_deleted, reads.source());
        read().await.unwrap();
        let before = opened.stats().gets();
        testing::take_pages();
        take_steps();
        let got = read().await.unwrap();
        assert_eq!(opened.stats().gets(), before, "warm");
        assert_eq!(got.answer.rows.len(), limit_index(WORK_LIMIT));
        Work {
            pages: testing::take_pages(),
            steps: take_steps(),
        }
    }

    /// A warm Scan's work follows its limit, not its run's length: newest
    /// first with no bound and before a bound in the middle of the run, a
    /// run of `LONG_RUN_PAGES` order pages steps on as many pages as one of
    /// `SHORT_RUN_PAGES`, and its placement takes at most one more binary
    /// search step per bound for each doubling of the pages, and one for
    /// the rounding of the two searches' depths.
    #[tokio::test]
    async fn test_read_run_warm_work_follows_the_limit_not_the_run() {
        let newest = |_: usize| bounds(Direction::NewestFirst, WORK_LIMIT);
        let middle = |rows: usize| ScanBounds {
            before: Some(order_at(i64::try_from(rows / 2).unwrap())),
            ..bounds(Direction::NewestFirst, WORK_LIMIT)
        };
        let doublings = usize::try_from((LONG_RUN_PAGES / SHORT_RUN_PAGES).ilog2()).unwrap();
        let rounding = 1;
        let (short, long) = (
            warm_work(SHORT_RUN_PAGES, newest).await,
            warm_work(LONG_RUN_PAGES, newest).await,
        );
        assert_eq!(short, long, "no bound");
        assert_eq!(long.steps, 0, "no bound, no search");
        let (short, long) = (
            warm_work(SHORT_RUN_PAGES, middle).await,
            warm_work(LONG_RUN_PAGES, middle).await,
        );
        assert_eq!(short.pages, long.pages, "a bound in the middle");
        assert!(short.pages > 0);
        assert!(
            long.steps <= short.steps + doublings + rounding,
            "{long:?} against {short:?}"
        );
    }

    /// The futures `FileIndex` hands out for a Scan are `Send`, so a
    /// `Table` can run them on any worker.
    #[tokio::test]
    async fn test_run_futures_are_send() {
        fn is_send<T: Send>(_: &T) {}
        let opened = store(&one_long_run(0)).await;
        let dir = tempfile::tempdir().unwrap();
        let index = index_in(&opened, Residency::Full, dir.path(), 0).await;
        let reads = Reads::new().await;
        let key = Key::Long(2);
        let locate = index.locate_run(&key, reads.source());
        is_send(&locate);
        let run = locate.await.unwrap().unwrap();
        let newest = bounds(Direction::NewestFirst, 1);
        let read = index.read_run(&run, &newest, &[0], &none_deleted, reads.source());
        is_send(&read);
        assert_eq!(read.await.unwrap().answer.rows.len(), 1);
    }

    /// The most bytes `read_run`'s future may take: `Generation::scan`'s
    /// join allocates one for every file a Scan reads, so each byte is paid
    /// per file per key. 1,072 is its size with the order and value reads
    /// written out per column, which reading them through one `read_spans`
    /// must not exceed.
    const READ_RUN_FUTURE_BYTES: usize = 1_072;
    /// The most bytes `read_orders`' future may take, laid out twice in
    /// every `read_run` future: 320, its size with the read written out.
    const READ_ORDERS_FUTURE_BYTES: usize = 320;
    /// The most bytes `read_values`' future may take, laid out in every
    /// `read_run` future: 256, its size with the read written out.
    const READ_VALUES_FUTURE_BYTES: usize = 256;
    /// The most bytes one column's read may take (`read_column_spans`),
    /// which `read_values`' join allocates per column: 240, its size with
    /// the read written out.
    const COLUMN_READ_FUTURE_BYTES: usize = 240;

    /// A Scan's read futures stay within their bytes, as `read_spans` keeps
    /// them: no `async` layer around it holds state of its own.
    #[tokio::test]
    async fn test_read_run_futures_stay_within_their_bytes() {
        let opened = store(&one_long_run(0)).await;
        let dir = tempfile::tempdir().unwrap();
        let index = index_in(&opened, Residency::Full, dir.path(), 0).await;
        let reads = Reads::new().await;
        let run = reads.run(&index).await;
        let newest = bounds(Direction::NewestFirst, 1);
        let order = index.order_leaf_of();
        let rows = [RowRef {
            row_group: run.parts[0].row_group,
            row: run.parts[0].rows.start,
        }];
        let sizes = [
            (
                "read_run",
                size_of_val(&index.read_run(&run, &newest, &[0], &none_deleted, reads.source())),
                READ_RUN_FUTURE_BYTES,
            ),
            (
                "read_orders",
                size_of_val(&index.read_orders(&order, &rows, reads.source())),
                READ_ORDERS_FUTURE_BYTES,
            ),
            (
                "read_values",
                size_of_val(&index.read_values(&[0], &rows, reads.source())),
                READ_VALUES_FUTURE_BYTES,
            ),
            (
                "read_column_spans",
                size_of_val(&index.read_column_spans(0, &rows, reads.source())),
                COLUMN_READ_FUTURE_BYTES,
            ),
        ];
        for (future, bytes, most) in sizes {
            assert!(bytes <= most, "{future}: {bytes} bytes, above {most}");
        }
    }
}
