//! A generation (RFD 005, RFD 017, RFD 020, RFD 021): one snapshot of one
//! table, every live data file indexed with the rows its deletes remove,
//! the files in a deterministic order, the overlap groups among them
//! checked for a live key two files share (in an ordered table, the files
//! that can share a live key and order value), and the lookup level and the
//! resident-bytes budget checked at the footer stage before any key chunk
//! or delete is fetched. Immutable once built; a `Table` builds the next
//! one beside it, reusing every file whose path, size, and data sequence
//! number are unchanged and every applied delete whose sources are, and
//! swaps atomically.

mod ordered;

use std::borrow::Borrow;
use std::cmp::{Ordering, Reverse};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;

use bytes::Bytes;
use futures::stream::{self, StreamExt as _, TryStreamExt as _};
use iceberg::spec::{Snapshot, TableMetadata};
use metrics::Counter;
use tokio::sync::Semaphore;

use crate::approx;
use crate::blocking;
use crate::cache::{Issue, PageCache};
use crate::catalog::{Resolved, TableFault};
use crate::check::{
    self, Capping, Caps, ColumnCost, ColumnLevel, FILES_PROBED_ALONE, FileView, LayoutView,
    LookupCost,
};
use crate::config::{Residency, TableConfig};
use crate::deletes::{self, DataTarget, DeleteFault, DeleteSummary, FileDeletes};
use crate::error::{Budget, Error, RefusalReason, Result};
use crate::index::{
    self, DeletedRows as _, FileFault, FileIndex, Inspected, Keys, PageRef, RowPosition, RunSource,
};
use crate::key::Key;
use crate::level::Level;
use crate::local::{
    ChunkRules, DataFileIdentity, LocalDir, Reservation, ResidentPages, Unobserved,
};
use crate::manifest::{self, Bucketing, DataFileEntry, DeleteFileEntry, TableLayout};
use crate::metric::LookupResult;
use crate::order::ScanBounds;
use crate::parquet_io::{FileHandle, Segments};
use crate::row_cache::{Cached, RowCache};
use crate::scan::{self, Scanned};
use crate::shard;
use crate::snapshot::{SequenceNumber, SnapshotId};
use crate::storage::{HedgeWindow, Storage};
use crate::time;

/// Files one load keeps in flight per worker thread. A load's time is its
/// files' round trips over its concurrency until its CPU binds: an object
/// not read in the last moments answers a range GET in 90 ms or more, and a
/// file's decode, verification, and chunk-file write are CPU, whose total
/// does not change with the concurrency. Against real S3 the knee sits near
/// 128 files per core.
const LOAD_FILES_PER_WORKER: usize = 128;

/// The most files one load keeps in flight on any machine. Past the knee
/// more concurrency buys no time and holds more: each file in flight at the
/// chunk stage holds its key chunks in memory and a blocking-pool task over
/// them.
const LOAD_FILES_MAX: usize = 1024;

/// Files worked on at once while a generation loads: footers loaded at the
/// footer stage, then chunks fetched and verified at the chunk stage, then
/// the key chunks of one overlap group gathered for its merge; and files
/// checked at once by the doctor's bucket check. Each file in flight is one
/// GET or local read waiting, and at the chunk stage one file's key chunks
/// in memory and at most one blocking-pool task over them at a time (its
/// verification, then in the Disk tier its chunk file's write).
/// `LOAD_FILES_PER_WORKER` for each of the runtime's worker threads
/// (`shard::workers`), at most `LOAD_FILES_MAX`. It is per load: tables
/// loading at once each run their own. Public so a measurement of loads
/// records the bound it ran under rather than restating it.
#[must_use]
pub fn load_concurrency() -> usize {
    (shard::workers() * LOAD_FILES_PER_WORKER).min(LOAD_FILES_MAX)
}

/// One data file with its manifest entry and its resident index.
#[derive(Debug)]
pub struct LoadedFile {
    /// The file as its manifest recorded it.
    pub entry: DataFileEntry,
    /// The file's resident index.
    pub index: FileIndex,
}

/// One snapshot, fully indexed and ready to serve. `Send + Sync`: a
/// `Table` shares it across every lookup task.
#[derive(Debug)]
pub struct Generation {
    /// The table's configured name.
    pub table: String,
    /// The snapshot served.
    pub snapshot_id: SnapshotId,
    /// The served snapshot's sequence number; what
    /// `generation_sequence_number` reports.
    pub sequence_number: SequenceNumber,
    /// The metadata file the snapshot was read from; what the next poll
    /// compares against.
    pub metadata_location: String,
    /// The layout derived from the snapshot's schema.
    pub layout: TableLayout,
    /// Where every file's key chunks live.
    pub residency: Residency,
    /// Every live data file, sorted by (bucketing, bucket, exact first
    /// key, path).
    pub files: Vec<Arc<LoadedFile>>,
    /// Rows across every file, from the manifests.
    pub records: u64,
    /// Bytes resident across every file index, the rows its deletes
    /// remove, and what the deletes keep for the next load.
    pub index_bytes: usize,
    /// When the load finished.
    pub loaded_at: Instant,
    /// The snapshot's own timestamp, when the writer committed it; what
    /// `generation_age_seconds` measures against now.
    pub snapshot_timestamp: jiff::Timestamp,
    /// When the snapshot that committed the oldest applying delete was
    /// committed (RFD 021, Measurements): the snapshot whose sequence
    /// number is the least data sequence number of any delete in a file's
    /// applied rows, else, when it has expired from the metadata, the
    /// earliest listed snapshot after it, which understates the age
    /// (`committed_at`); `None` where no delete applies. What
    /// `table_oldest_delete_age_seconds` measures against now.
    pub oldest_delete_committed: Option<jiff::Timestamp>,
    /// The level the snapshot's layout achieves in `residency` (RFD 020),
    /// measured once as the generation is assembled (`check::layout_caps`);
    /// what the level gauges and a reload judge the generation by.
    pub level: Level,
    /// What one cold lookup pays at worst on the snapshot's layout in
    /// `residency`, from the same measurement as `level`; what the cost
    /// gauges show while the generation serves.
    pub lookup_cost: LookupCost,
    /// Each value column's level and cost, from the same measurement, in
    /// `layout`'s order (RFD 022); `level` is the dearest of them.
    pub column_costs: Vec<ColumnCost>,
    /// On an ordered table, the GETs one cold Scan at `max_scan_rows` of a
    /// key held in one file and one row group makes at worst, reading
    /// every configured value column, from the same measurement
    /// (`check::Caps::gets_per_scan`); `None` on a key-value table. What
    /// `icetray_table_gets_per_scan` shows (RFD 025, Metrics).
    pub gets_per_scan: Option<u64>,
    /// The distinct capping findings, by code and column, in the finding
    /// table's order.
    capping: Vec<Capping>,
    /// The probe ranks of `files` per bucketing the files carry and per
    /// bucket under it, ascending, which is the order a lookup probes them
    /// (`ranked_routes`); how a key finds its candidates.
    probe_routes: RankedRoutes,
    /// Per bucket a member of an overlap group is in, how many files a
    /// lookup there may probe, and on an ordered table how many files a
    /// shared edge key spans (`probe_widths`).
    widths: ProbeWidths,
    /// Overlap groups this load merged to prove key uniqueness, or on an
    /// ordered table pair uniqueness (`ordered::verify_rows`).
    verified_groups: usize,
    /// Per file of `files`, the rows its deletes remove (RFD 021); `None`
    /// where no delete applies.
    deletes: Arc<[Option<Arc<FileDeletes>>]>,
    /// What the next load's deletes need not read again, by index into
    /// `files`.
    delete_reads: deletes::Known,
    /// The bound on the bytes the delete state takes, from the manifests
    /// alone (`deletes::bound`), which the budget was checked against.
    delete_bound: u64,
}

/// Positions in a generation's files by bucketing and bucket. `None`
/// bucketing files are all under `manifest`'s unbucketed bucket.
type Routes = BTreeMap<Option<Bucketing>, Buckets>;

/// Positions in a generation's files by bucket, in file order.
type Buckets = BTreeMap<u32, Vec<usize>>;

/// `Routes` with each position as its probe rank, every bucket's ranks
/// ascending.
type RankedRoutes = BTreeMap<Option<Bucketing>, BTreeMap<u32, Vec<ProbeRank>>>;

impl Generation {
    /// The footer stage of a load of `snapshot` of `table`: derives the
    /// layout from the snapshot's schema, lists its live files
    /// (`manifest::list_files`, which refuses the data and delete files it
    /// documents), bounds the memory its deletes take from the manifests
    /// alone (`deletes::bound`), reuses from `previous` every file the snapshot
    /// still names by the same path at the same size and data sequence
    /// number (the same `Arc`, RFD 005; `reuse_or_open`) with the rows its
    /// deletes removed there, checks every reused file's columns against
    /// the snapshot's schema from the footer it keeps
    /// (`FileIndex::check_columns`, RFD 023), opens a `FileHandle` for
    /// every other file with the manifest's size and `hedge`, and inspects
    /// their footers, which resolves their columns the same way,
    /// `load_concurrency()` at a time. No key chunk or delete is read and
    /// nothing is written; `Planned::build` does both. Raised before any
    /// read: a `lookups` level the residency cannot reach, as
    /// `ConfigViolation::LookupsUnreachable`, and an ordered table's zero
    /// `max_scan_rows`, as `ConfigViolation::MaxScanRowsZero` (a
    /// configuration built by hand meets both here rather than at
    /// validation, `TableConfig::check_at_use`), and a snapshot timestamp
    /// jiff cannot represent, as `TableFault::SnapshotTimestampOutOfRange`.
    pub(crate) async fn plan_load(
        config: &TableConfig,
        resolved: &Resolved,
        snapshot: &Snapshot,
        storage: &Arc<Storage>,
        hedge: Arc<HedgeWindow>,
        previous: Option<&Generation>,
    ) -> Result<Planned> {
        let table = config.name.as_str();
        config.check_at_use()?;
        let snapshot_timestamp = snapshot_timestamp(table, snapshot)?;
        let layout = TableLayout::for_entry(config, resolved, snapshot)?;
        let files =
            manifest::list_files(table, &resolved.metadata, snapshot, &layout, storage).await?;
        let (reused, pending) =
            reuse_or_open(table, &layout, files.data, previous, storage, &hedge).await?;
        let targets: Vec<DataTarget> = reused
            .iter()
            .map(|file| &file.entry)
            .chain(pending.iter().map(|(entry, _)| entry))
            .map(DataTarget::of)
            .collect();
        let delete_bound = deletes::bound(&deletes::Sorted::of(&files.deletes), &targets);
        let previous_deletes = previous
            .map(|previous| previous.deletes_of(&reused))
            .unwrap_or_default();
        let inspected = inspect_all(table, &layout, pending).await?;
        Ok(Planned {
            config: config.clone(),
            snapshot: snapshot.clone(),
            snapshot_timestamp,
            metadata: Arc::clone(&resolved.metadata),
            metadata_location: resolved.location.clone(),
            layout,
            storage: Arc::clone(storage),
            reused,
            inspected,
            targets,
            deletes: files.deletes,
            previous_deletes,
            delete_bound,
        })
    }

    /// What a load reusing `reused`, files of this generation, starts its
    /// deletes from (`deletes::load`): this generation's applied rows of
    /// each, in `reused`'s order, and what its deletes read, moved from
    /// indices into `files` to indices into `reused`.
    fn deletes_of(&self, reused: &[Arc<LoadedFile>]) -> deletes::Previous {
        let at: HashMap<*const LoadedFile, usize> = self
            .files
            .iter()
            .enumerate()
            .map(|(at, file)| (Arc::as_ptr(file), at))
            .collect();
        let mut moved_to = vec![None; self.files.len()];
        let applied = (0_u32..)
            .zip(reused)
            .map(|(to, file)| {
                let from = *at
                    .get(&Arc::as_ptr(file))
                    .expect("a reused file is one of the previous generation's");
                moved_to[from] = Some(to);
                self.deletes[from].clone()
            })
            .collect();
        deletes::Previous {
            applied,
            known: self.delete_reads.reindexed(&moved_to),
        }
    }

    /// Everything after the files are indexed: sorts them, builds the
    /// routes, finds the overlap groups, and proves uniqueness across
    /// files where the snapshot changed it: a key-value table merges every
    /// group that a non-reused file belongs to, or a member whose deletes
    /// shrank, refusing a live key two files share (`verify_groups`); an
    /// ordered table merges, once per such group, the order pages of its
    /// files that can hold a pair two of them share where one is checked,
    /// refusing such a live pair (`ordered::verify_rows`). A
    /// row a file's deletes remove is not live (RFD 021). A data file with
    /// deletes whose manifest records another row count than its footer is
    /// `DeleteFault::RecordCountMismatch`, since its deletes' positions
    /// were checked against the manifest's.
    async fn assemble(
        table: &str,
        snapshot: &Snapshot,
        snapshot_timestamp: jiff::Timestamp,
        metadata_location: &str,
        layout: TableLayout,
        residency: Residency,
        mut members: Vec<Member>,
    ) -> Result<Self> {
        members.sort_by(|a, b| file_order(&a.file, &b.file));
        let files: Vec<Arc<LoadedFile>> = members
            .iter()
            .map(|member| Arc::clone(&member.file))
            .collect();
        let deletes: Arc<[Option<Arc<FileDeletes>>]> = members
            .iter()
            .map(|member| member.deletes.clone())
            .collect();
        refuse_record_count_mismatch(&files, &deletes)?;
        let spans: Vec<Span<'_>> = files.iter().map(|file| Span::of(file)).collect();
        let routes = routes(&spans);
        let probe_routes = ranked_routes(&files, &routes);
        let groups = overlap_groups(&spans, &routes);
        let widths = probe_widths(&spans, &groups, Edges::of(&layout));
        let caps = check::layout_caps(&LayoutView {
            layout: &layout,
            residency,
            files: files.iter().map(|file| FileView::loaded(file)).collect(),
            widths: &widths,
        });
        let verified_groups = match layout.order {
            Some(_) => {
                let check = ordered::verify_rows(
                    table,
                    &members,
                    &files,
                    &groups,
                    &deletes,
                    ordered::MERGE_READ_AHEAD_BYTES,
                )
                .await?;
                tracing::debug!(
                    table,
                    merges = check.merges,
                    compared = check.compared,
                    held_bytes = check.held,
                    "checked ordered rows across files"
                );
                check.merges
            }
            None => verify_groups(table, &members, &files, groups, &deletes).await?,
        };
        let records = files.iter().map(|file| file.entry.records).sum();
        let delete_bytes = deletes
            .iter()
            .flatten()
            .map(|rows| approx::length(rows.resident_bytes()))
            .sum::<usize>();
        let index_bytes = files
            .iter()
            .map(|file| file.index.resident_bytes())
            .sum::<usize>()
            + delete_bytes;
        tracing::debug!(
            table,
            snapshot_id = %snapshot.snapshot_id(),
            files = files.len(),
            index_bytes,
            overlap_groups_verified = verified_groups,
            "assembled generation"
        );
        Ok(Self {
            table: table.to_string(),
            snapshot_id: SnapshotId(snapshot.snapshot_id()),
            sequence_number: SequenceNumber(snapshot.sequence_number()),
            metadata_location: metadata_location.to_string(),
            layout,
            residency,
            files,
            records,
            index_bytes,
            loaded_at: Instant::now(),
            snapshot_timestamp,
            oldest_delete_committed: None,
            level: caps.level,
            lookup_cost: caps.cost,
            gets_per_scan: caps.gets_per_scan,
            capping: caps.capping(),
            column_costs: caps.columns,
            probe_routes,
            widths,
            verified_groups,
            deletes,
            delete_reads: deletes::Known::default(),
            delete_bound: 0,
        })
    }

    /// The level gate (`check::judge`) over the generation's measured level
    /// for a table that declares `declared`.
    pub(crate) fn judge(&self, declared: Level, strict: bool) -> Result<()> {
        check::judge(
            &self.table,
            self.level,
            &self.column_levels(),
            &self.capping,
            declared,
            strict,
        )
    }

    /// Each value column's level, as `judge` and the level gauges take
    /// them.
    pub(crate) fn column_levels(&self) -> Vec<ColumnLevel> {
        check::column_levels(&self.layout, &self.column_costs)
    }

    /// What the generation's delete state comes to (RFD 021): the rows its
    /// deletes remove, the deletes they came from, their share of
    /// `records`, and the bytes they hold, which `index_bytes` counts.
    pub(crate) fn delete_summary(&self) -> DeleteSummary {
        DeleteSummary::of(&self.deletes, &self.delete_reads, self.records)
    }

    /// The bound on the bytes the delete state takes, from the manifests
    /// alone, as the budget check counted it (RFD 021, Delete state); at
    /// least `delete_summary`'s `state_bytes`.
    pub(crate) fn delete_bound(&self) -> u64 {
        self.delete_bound
    }

    /// The files that may hold `key`, in probe order: under every bucketing
    /// the files carry, the files of the bucket `key` falls in under it
    /// (each bucketing routes the key to its own bucket), and every file
    /// without a bucketing, narrowed to those whose exact key range covers
    /// the key. A table whose files all carry one bucketing yields one
    /// bucket's files; after a spec evolution, one bucket's files per
    /// bucketing. They come in descending data sequence number across every
    /// bucketing, ties in file order (RFD 021): the newest copy of an
    /// updated key first. A key held by two files is refused at load, so
    /// the order changes no answer.
    pub fn candidates<'a>(
        &'a self,
        key: &'a Key,
    ) -> impl Iterator<Item = &'a Arc<LoadedFile>> + 'a {
        self.ranked_candidates(key).map(|(_, file)| file)
    }

    /// `candidates` with each file's position in `files`.
    fn ranked_candidates<'a>(
        &'a self,
        key: &'a Key,
    ) -> impl Iterator<Item = (usize, &'a Arc<LoadedFile>)> + 'a {
        ProbeOrder::new(&self.probe_routes, key)
            .map(|at| (at, &self.files[at]))
            .filter(move |(_, file)| file.index.may_contain(key))
    }

    /// Whether the deletes of the file at `at` in `files` remove the row at
    /// `position`.
    fn is_deleted(&self, at: usize, position: RowPosition) -> bool {
        self.deletes[at].as_deref().deleted(position)
    }

    /// Whether the deletes of the file at `at` in `files` remove the row
    /// `page` names. A page names its row (`PageRef::position`) only where
    /// the key page it searched is resident, as the Full tier's are; in the
    /// Disk and Bloom tiers naming the row takes a read, the page names
    /// none, and this is false.
    fn page_deleted(&self, at: usize, page: &PageRef) -> bool {
        page.position
            .is_some_and(|position| self.is_deleted(at, position))
    }

    /// Per bucket that an overlap group touches, how many files a lookup in
    /// that bucket may probe (RFD 007): the most of the group's files whose
    /// exact key ranges cover one key and whose buckets can hold it, which
    /// is fewer than the group holds where its files chain end to end
    /// (`probe_widths`). Files counted together across two bucketings
    /// are recorded under every one's bucket. On an ordered table, a bucket
    /// whose files one snapshot added meet only at edge keys is not
    /// recorded here: such a key is a shared edge (RFD 025).
    /// Keyed by the bucket number alone, as the doctor reports it: after a
    /// spec evolution, buckets of two bucketings that share a number share
    /// one entry, and an unbucketed file's `manifest::UNBUCKETED` is bucket
    /// zero's number.
    pub fn overlapping_buckets(&self) -> &BTreeMap<u32, usize> {
        &self.widths.overlapping
    }

    /// What the overlap sweep found (`probe_widths`): overlap, and on an
    /// ordered table shared edge keys, per bucket.
    pub(crate) fn probe_widths(&self) -> &ProbeWidths {
        &self.widths
    }

    /// The doctor's full bucket check (RFD 016): decodes every key of every
    /// bucketed file and refuses the first that hashes to another bucket
    /// than its file's manifest entry records, as
    /// `RefusalReason::KeyOutsideBucket`; an unbucketed file has no bucket
    /// to check. The load hashes each file's fences and its first and last
    /// key alone, which refuses a file recorded under the wrong bucket but
    /// serves one whose keys change bucket between two fences; this check
    /// refuses both, at the price of decoding and hashing every key. The
    /// chunks come as `FileIndex::key_chunks_for_verification` hands them
    /// out (Full: resident, Disk: one local read per row group, Bloom: one
    /// GET per row group), `load_concurrency()` files at a time, and are
    /// decoded one key page at a time, each file's check on the blocking
    /// pool (`blocking::on_blocking_pool`), so up to `load_concurrency()` checks
    /// run at once on their own threads. A canceled check is
    /// `Error::VerifyCanceled` for its file. The doctor calls it after a
    /// load; no load or refresh does.
    pub async fn verify_buckets(&self) -> Result<()> {
        let table = self.table.as_str();
        stream::iter(
            self.files
                .iter()
                .filter(|file| file.entry.bucketing.is_some())
                .cloned(),
        )
        .map(|file| async move {
            let chunks = file.index.key_chunks_for_verification().await?;
            let path = file.index.handle().location.clone();
            let owned_table = table.to_string();
            blocking::on_blocking_pool(move || {
                refuse_any_key_outside_bucket(&owned_table, &file, &chunks)
            })
            .await
            .map_err(|canceled| Error::verify_canceled(&path, canceled))?
        })
        .buffered(load_concurrency())
        .try_collect::<()>()
        .await
    }

    /// Overlap groups this load merged to prove key uniqueness: the groups
    /// that gained a file since `previous`, or every group on a first load.
    /// On an ordered table, the groups holding a file this load checked,
    /// new or reused with deletes that shrank, whose check merged it with
    /// another (RFD 025, Refresh).
    pub fn overlap_groups_verified(&self) -> usize {
        self.verified_groups
    }

    /// One lookup reading the value columns at the positions `columns`
    /// gives in `layout`, in that order, and the `result` label it earns:
    /// the candidates are probed in probe order and the first whose `find`
    /// answers with a row its file's deletes do not remove gives the row,
    /// checked against the deletes once whatever the columns (RFD 021); a
    /// deleted row is passed over with no value page read. Then each column
    /// named is read from `rows`, the row cache, when given (RFD 026), at
    /// the key of the file, the row, and the column's leaf there
    /// (`FileIndex::row_key`), so the index has chosen the file and row,
    /// and the deletes have spoken, before the cache is asked: a row
    /// deleted or rewritten into another file is never served from it. A
    /// column the row cache lacks is read from the row's value page through
    /// `cache`, every such column's read at once, each a GET of its own,
    /// hedged on its own (RFD 013, RFD 022), and its value, or its null, is
    /// inserted into `rows`; a row of a file written before a column was
    /// added answers that column's default, reads no page of it, and uses
    /// no entry (RFD 023). No column named answers presence alone and
    /// reads no value page. `Present` carries one value per column named,
    /// each its bytes or `None` for a null; `Absent` is a key no candidate
    /// holds. The GET cost is the index's: in the Full tier one GET per
    /// column named for a cold hit, two for a dictionary-encoded column,
    /// none for a warm hit, a row-cache hit, a default, presence alone, or
    /// a miss; the key is resolved the same way on a row-cache hit, a key
    /// page read in the Bloom tier. Each GET the lookup issues, a
    /// Bloom-tier key page's included, holds one permit of `fan_out`, the
    /// batch's, while it runs, and each GET of a value or dictionary page
    /// of the column at position `c` is counted under `value_gets[c]` as it
    /// is issued, where `value_gets` has it (`Issue`); a row-cache hit
    /// takes no permit. The label is `Miss` for an absent key; for a
    /// present one, `ColdHit` when a value or dictionary page of a column
    /// named came from outside the cache (a fetch this lookup ran or
    /// joined) and `WarmHit` otherwise, a hit that read no value page, a
    /// row-cache hit among them, included. Only the columns' pages decide
    /// it; a key-page GET in the Bloom tier does not.
    pub(crate) async fn lookup(
        &self,
        key: &Key,
        columns: &[usize],
        cache: &PageCache,
        rows: Option<&RowCache>,
        fan_out: Option<&Arc<Semaphore>>,
        value_gets: &[Counter],
    ) -> Result<(Row, LookupResult)> {
        let key_page = Issue {
            fan_out,
            counted: None,
        };
        for (at, file) in self.ranked_candidates(key) {
            if let Some(row) = file.index.find(key, Some(cache), key_page).await? {
                if self.is_deleted(at, file.index.position(row)) {
                    continue;
                }
                let reads = columns.iter().map(|&column| async move {
                    let cached = rows.and_then(|rows| {
                        file.index
                            .row_key(column, row)
                            .map(|row_key| (rows, row_key))
                    });
                    if let Some((row_cache, row_key)) = &cached {
                        match row_cache.get(row_key) {
                            Cached::Value(bytes) => return Ok((Some(bytes), false)),
                            Cached::Null => return Ok((None, false)),
                            Cached::Absent => {}
                        }
                    }
                    let issue = Issue {
                        fan_out,
                        counted: value_gets.get(column),
                    };
                    Ok::<_, Error>(
                        match file
                            .index
                            .read_column(column, row, Some(cache), issue)
                            .await?
                        {
                            Some(read) => {
                                if let Some((row_cache, row_key)) = cached {
                                    row_cache.insert(row_key, read.value.as_deref());
                                }
                                (read.value, read.from_store)
                            }
                            None => (self.layout.value_columns[column].default.clone(), false),
                        },
                    )
                });
                let (values, from_store): (Vec<Option<Bytes>>, Vec<bool>) =
                    futures::future::try_join_all(reads)
                        .await?
                        .into_iter()
                        .unzip();
                let result = if from_store.contains(&true) {
                    LookupResult::ColdHit
                } else {
                    LookupResult::WarmHit
                };
                return Ok((Row::Present(values), result));
            }
        }
        Ok((Row::Absent, LookupResult::Miss))
    }

    /// One Scan of `key` (RFD 025, Cost): its live rows at or above
    /// `bounds`' `start` and below its `before`, in `bounds.direction`, at
    /// most `bounds.limit` of them, which the table checked, each with one
    /// value per position of `columns` in the layout, and the next page's
    /// bound when live rows remain inside the bounds past the last
    /// returned. Every candidate file whose exact key range covers the key
    /// (`candidates`) finds the key's run (`FileIndex::locate_run`), and
    /// each file that holds it reads its candidate rows
    /// (`FileIndex::read_run`), up to the limit's live rows from the
    /// direction's end and whether one more lies past them, its
    /// rows counted live against the file's deletes with no I/O (RFD 021);
    /// the files are read together, each reading as soon as its run is
    /// found, so the read is one round across them, and under Bloom one
    /// round of key pages before it. Every GET holds one permit of
    /// `source.fan_out`, goes through `source.cache`, and counts in its
    /// column's counter. The files' rows are merged by order value, live
    /// pairs being unique across files (RFD 025, Uniqueness), and cut to
    /// the limit (`scan::merge`); a row past which no bound lies, followed
    /// by a live row, is `Error::NoNextBound`. A file written before a
    /// column was added answers that column's default (RFD 023). A key no
    /// file holds answers no rows and no `next`, with no read beyond
    /// finding that.
    pub(crate) async fn scan(
        &self,
        key: &Key,
        columns: &[usize],
        bounds: &ScanBounds,
        source: RunSource<'_>,
    ) -> Result<(Scanned, ScanRead)> {
        let reads = self.ranked_candidates(key).map(|(at, file)| async move {
            let Some(run) = file.index.locate_run(key, source).await? else {
                return Ok::<_, Error>(None);
            };
            let deleted = self.deletes[at].as_deref();
            let rows = file
                .index
                .read_run(&run, bounds, columns, &deleted, source)
                .await?;
            Ok(Some((file, rows)))
        });
        let read = futures::future::try_join_all(reads).await?;
        let mut answers = Vec::new();
        let mut from_store = false;
        for (file, mut rows) in read.into_iter().flatten() {
            from_store |= rows.from_store;
            // Per position of `columns` the file does not hold, the
            // column's default, set in each row's values in place.
            let defaults: Vec<(usize, &Option<Bytes>)> = columns
                .iter()
                .enumerate()
                .filter(|&(_, &column)| file.index.value_leaf(column).is_none())
                .map(|(at, &column)| (at, &self.layout.value_columns[column].default))
                .collect();
            for row in &mut rows.answer.rows {
                for &(at, default) in &defaults {
                    row.values[at].clone_from(default);
                }
            }
            answers.push(rows.answer);
        }
        let files = answers.len();
        let scanned = scan::merge(answers, bounds, &self.table, key)?;
        Ok((scanned, ScanRead { files, from_store }))
    }

    /// `lookup` naming the layout's first value column, alone and
    /// uncounted: the shape the single-column tests read.
    #[cfg(test)]
    pub(crate) async fn lookup_first(
        &self,
        key: &Key,
        cache: &PageCache,
    ) -> Result<(Row, LookupResult)> {
        self.lookup(key, &[0], cache, None, None, &[]).await
    }

    /// Which file and page each key would reach; the oracle for cost tests.
    /// Of the pages the key's candidates name, in the order a lookup probes
    /// them (`probed`), the first that names the key's row where its file's
    /// deletes do not remove it, with that page: in the Full tier that is
    /// the file holding the live key, wherever in an overlap group it sits.
    /// Each page's file reports, through `FileIndex::value_page_offset`,
    /// the value page a hit naming column `c` would GET, `None` where the
    /// file answers `c` by its default. When no candidate names such a row
    /// (the key is absent or deleted from every file whose fences cover it,
    /// or the tier is Disk or Bloom, where naming one would take a read),
    /// the first candidate that would decode a key page for it, the first
    /// probe. `None` for a key no candidate would decode a page for, which
    /// `lookup` answers `Absent` with no I/O.
    pub fn plan(&self, keys: &[Key]) -> Vec<Option<(Arc<LoadedFile>, PageRef)>> {
        keys.iter().map(|key| self.plan_one(key)).collect()
    }

    fn plan_one(&self, key: &Key) -> Option<(Arc<LoadedFile>, PageRef)> {
        let mut first_probe = None;
        for (at, file, page) in self.probed(key) {
            if page.names_row() && !self.page_deleted(at, &page) {
                return Some((Arc::clone(file), page));
            }
            first_probe.get_or_insert_with(|| (Arc::clone(file), page));
        }
        first_probe
    }

    /// Every page a lookup of `key` would decode were each probe to miss,
    /// with its file and whether the file's deletes remove the key's row
    /// there (`page_deleted`: meaningful in the Full tier alone, false
    /// elsewhere, where no page names its row), in probe order (`probed`).
    /// `plan` reaches the first of them that holds the key live; a cost
    /// test counts the GETs of the probes before it, deleted copies among
    /// them.
    #[cfg(test)]
    pub(crate) fn probes(&self, key: &Key) -> Vec<(Arc<LoadedFile>, PageRef, bool)> {
        self.probed(key)
            .map(|(at, file, page)| {
                let deleted = self.page_deleted(at, &page);
                (Arc::clone(file), page, deleted)
            })
            .collect()
    }

    /// The pages a lookup of `key` would decode were each probe to miss,
    /// with their files and the files' positions, in probe order
    /// (`candidates`): each candidate whose `page_of` names a key page.
    fn probed<'a>(
        &'a self,
        key: &'a Key,
    ) -> impl Iterator<Item = (usize, &'a Arc<LoadedFile>, PageRef)> {
        self.ranked_candidates(key)
            .filter_map(|(at, file)| file.index.page_of(key).map(|page| (at, file, page)))
    }

    /// The identity of every file's chunk file: on a key-value table the
    /// files `LocalDir::chunks_for` finds. Meaningful for Disk residency,
    /// where each file has a chunk file; the identities of a Full or Bloom
    /// generation name files that were never written.
    pub fn identities(&self) -> HashSet<DataFileIdentity> {
        self.files
            .iter()
            .map(|file| index::identity_of(file.index.handle()))
            .collect()
    }

    /// Every file's chunk file as `LocalDir::retain` names it, by identity
    /// and the rules this generation verified the file under: the union
    /// over every table sharing the directory is the live set (RFD 017).
    /// As with `identities`, a Full or Bloom generation writes no chunk
    /// files, so the names it gives refer to files that do not exist and
    /// keep nothing in the directory.
    pub(crate) fn live_chunk_files(
        &self,
    ) -> impl Iterator<Item = (DataFileIdentity, ChunkRules)> + '_ {
        self.files.iter().map(|file| {
            (
                index::identity_of(file.index.handle()),
                file.index.chunk_rules(),
            )
        })
    }

    /// Every file's chunk file, with where its chunks, an ordered table's
    /// order chunks among them, lie in it; the range ends at the file's
    /// length. A Full or Bloom generation has none.
    pub fn chunk_files(&self) -> impl Iterator<Item = (&Path, Range<u64>)> {
        self.files
            .iter()
            .filter_map(|file| file.index.disk_chunks())
            .map(|disk| (disk.path(), disk.chunks()))
    }

    /// What the operating system's page cache holds of every file's chunks
    /// in their chunk files, an ordered table's order chunks with its key
    /// chunks (`DiskChunks::resident`), summed in pages, so weighted by bytes
    /// (RFD 020); `Err` names the first chunk file whose residency cannot be
    /// observed. A Full or Bloom generation has no chunk file and counts no
    /// page. One transient mapping and one `mincore` call per file, no byte
    /// read: synchronous, for the blocking pool.
    pub fn chunk_residency(&self) -> std::result::Result<ResidentPages, Unobserved> {
        self.chunk_residency_in(&mut HashMap::new())
    }

    /// `chunk_residency`, taking each chunk file's sample from `sampled`
    /// when a generation sampled before it read the same file, and adding
    /// to it the files it samples itself, so a sweep over several tables
    /// samples each file once.
    pub(crate) fn chunk_residency_in(
        &self,
        sampled: &mut HashMap<PathBuf, std::result::Result<ResidentPages, Unobserved>>,
    ) -> std::result::Result<ResidentPages, Unobserved> {
        self.files
            .iter()
            .filter_map(|file| file.index.disk_chunks())
            .try_fold(ResidentPages::default(), |sum, disk| {
                let sample = sampled
                    .entry(disk.path().to_path_buf())
                    .or_insert_with(|| disk.resident())
                    .clone()?;
                Ok(sum.plus(sample))
            })
    }
}

/// A snapshot through the footer stage of its load
/// (`Generation::plan_load`): its layout, the files reused from the
/// previous generation, every other file's inspected footer, and its
/// readable deletes with the bound on the memory they take.
pub(crate) struct Planned {
    config: TableConfig,
    snapshot: Snapshot,
    snapshot_timestamp: jiff::Timestamp,
    /// The metadata the snapshot was read from, whose snapshots date the
    /// oldest applying delete.
    metadata: Arc<TableMetadata>,
    metadata_location: String,
    layout: TableLayout,
    /// What the deletes are read through.
    storage: Arc<Storage>,
    reused: Vec<Arc<LoadedFile>>,
    inspected: Vec<(DataFileEntry, Inspected)>,
    /// Every data file as the deletes see it: the reused files, then the
    /// inspected ones, in their orders.
    targets: Vec<DataTarget>,
    /// Every live delete file icetray can read.
    deletes: Vec<DeleteFileEntry>,
    /// The previous generation's applied deletes of the reused files.
    previous_deletes: deletes::Previous,
    /// The bytes the applied deletes take at most (`deletes::bound`).
    delete_bound: u64,
}

impl Planned {
    /// The level gate at the footer stage (RFD 020): under `strict`,
    /// `RefusalReason::BelowLevel` (`check::judge`) when the snapshot's
    /// layout puts the table below its `lookups`, judged before any key
    /// chunk is fetched and before the index budget is reserved, so a
    /// refused snapshot writes nothing to the local directory. The caps the
    /// footers, the manifest entries, and the residency set are judged
    /// together with overlap, from the reused files' exact key ranges and
    /// each new file's edge keys (`edge_ranges`), and on an ordered table
    /// with shared edge keys, told from overlap by those keys and the
    /// snapshot each manifest entry records as adding its file
    /// (`probe_widths`), so a refused snapshot's level and codes are the
    /// ones its build would measure. Overlap is
    /// left unread only where it cannot change the verdict or the level: a
    /// table declared `permissive`, which every layout meets, and footer
    /// caps that already reach `permissive`, the dearest level. A new file
    /// whose edge pages do not decode is left out of the overlap groups,
    /// which can only narrow them: if the rest already put the table below
    /// its level the snapshot is refused for it, and otherwise the build's
    /// verification refuses that file. Not strict, it reads nothing and
    /// passes.
    pub(crate) async fn judge_level(&mut self, local: Option<&Arc<LocalDir>>) -> Result<()> {
        let (declared, strict) = (self.config.lookups(), self.config.strict);
        if !strict {
            return Ok(());
        }
        let mut caps = self.caps(&ProbeWidths::default());
        if declared < Level::Permissive && caps.level < Level::Permissive {
            let widths = self.overlapping(local).await?;
            caps = self.caps(&widths);
        }
        check::judge(
            &self.config.name,
            caps.level,
            &check::column_levels(&self.layout, &caps.columns),
            &caps.capping(),
            declared,
            strict,
        )
    }

    /// The level the snapshot's layout achieves in the configured residency
    /// with `widths` as the most files a lookup probes per bucket
    /// (`check::layout_caps`), from the footers and manifest entries.
    fn caps(&self, widths: &ProbeWidths) -> Caps {
        let files = self
            .reused
            .iter()
            .map(|file| FileView::loaded(file))
            .chain(
                self.inspected
                    .iter()
                    .map(|(entry, inspected)| FileView::inspected(entry, inspected)),
            )
            .collect();
        check::layout_caps(&LayoutView {
            layout: &self.layout,
            residency: self.config.residency,
            files,
            widths,
        })
    }

    /// Per bucket an overlap group touches, the most files a lookup there
    /// may probe, and on an ordered table the most files a shared edge key
    /// spans (`probe_widths`), from the reused files' exact key ranges, the
    /// new files' edge keys, and the snapshot each file's manifest entry
    /// records as adding it. A new file with `Edge::Unknown` has no span,
    /// so the groups are those of the other files alone.
    async fn overlapping(&mut self, local: Option<&Arc<LocalDir>>) -> Result<ProbeWidths> {
        let edges = self.edge_ranges(local).await?;
        let mut spans: Vec<Span<'_>> = self.reused.iter().map(|file| Span::of(file)).collect();
        for ((entry, _), edge) in self.inspected.iter().zip(&edges) {
            match edge {
                Edge::Span(first, last) => spans.push(Span {
                    bucketing: entry.bucketing,
                    bucket: entry.bucket,
                    first,
                    last,
                    added_by: entry.added_by,
                }),
                Edge::Alone | Edge::Unknown => {}
            }
        }
        spans.sort_by(|a, b| {
            (a.bucketing, a.bucket, a.first).cmp(&(b.bucketing, b.bucket, b.first))
        });
        let groups = overlap_groups(&spans, &routes(&spans));
        Ok(probe_widths(&spans, &groups, Edges::of(&self.layout)))
    }

    /// Per new file, in plan order, where it stands in the overlap sweep
    /// (RFD 020, Settings): `Edge::Alone`, reading nothing, when no other
    /// file of the snapshot is in a bucket one of its keys can fall in
    /// (`sharing`), so a table with one file per bucket reads nothing;
    /// otherwise its first and last key. A Disk table's file whose chunk
    /// file in `local` is trusted takes them from the verification record
    /// it stores, with no GET, and keeps the chunk file for `build`
    /// (`Inspected::trusted_key_range`); any other reads its edge pages
    /// (`Inspected::edge_keys`). `load_concurrency()` files at a time.
    async fn edge_ranges(&mut self, local: Option<&Arc<LocalDir>>) -> Result<Vec<Edge>> {
        let shares = sharing(
            self.reused
                .iter()
                .map(|file| &file.entry)
                .chain(self.inspected.iter().map(|(entry, _)| entry)),
        );
        let local = match self.config.residency {
            Residency::Disk => local.map(Arc::as_ref),
            Residency::Full | Residency::Bloom => None,
        };
        let reads: Vec<_> = self
            .inspected
            .iter_mut()
            .zip(&shares[self.reused.len()..])
            .map(|((_, inspected), &shares)| edge_of(inspected, local, shares))
            .collect();
        stream::iter(reads)
            .buffered(load_concurrency())
            .try_collect()
            .await
    }

    /// The chunk stage: checks the budget the configuration and `local` set
    /// before any chunk or delete is fetched (Full: `resident_budget_bytes`
    /// against the reused files' resident bytes plus every new file's
    /// predicted full bytes plus the deletes' bound; Disk:
    /// `local.index_budget()` against the bytes `index/` holds, what other
    /// tables' loads have reserved, and what every new file would write
    /// there, reserved until the new indexes are built; over either is
    /// `RefusalReason::OverBudget`), then builds the new indexes
    /// `load_concurrency()` at a time, refusing a bucketed file whose
    /// fences or first or last key hash to another bucket than its manifest
    /// entry records (`RefusalReason::KeyOutsideBucket`; a reused file was
    /// checked when it was built, and every key is `verify_buckets`'s to
    /// check), and beside the builds applies the snapshot's deletes
    /// (`deletes::load`), the first failure of either stopping the other,
    /// on an ordered table checks the Full tier's budget again against the
    /// built files' exact resident bytes (`check_built_budget`), then
    /// assembles the generation: files sorted, overlap groups found,
    /// and every group that gained a file or whose deletes shrank merged to
    /// refuse a duplicate live key, or on an ordered table to refuse a
    /// repeated live pair. `Err` leaves nothing behind; the caller keeps
    /// serving the previous generation.
    pub(crate) async fn build(self, local: Option<&Arc<LocalDir>>) -> Result<Generation> {
        let table = self.config.name.as_str();
        let reservation = check_budget(
            table,
            &self.config,
            local,
            &self.reused,
            &self.inspected,
            self.delete_bound,
        )?;
        let (built, applied) = futures::try_join!(
            build_all(
                table,
                self.config.residency,
                local,
                reservation.as_ref(),
                self.inspected,
            ),
            deletes::load(
                table,
                &self.storage,
                self.targets,
                &self.deletes,
                self.previous_deletes,
            ),
        )?;
        drop(reservation);
        if self.layout.order.is_some() {
            check_built_budget(table, &self.config, &self.reused, &built, self.delete_bound)?;
        }
        let mut rows = applied.rows.into_iter();
        let mut shrank = applied.shrank.into_iter();
        let members: Vec<Member> = self
            .reused
            .into_iter()
            .map(|file| (file, true))
            .chain(built.into_iter().map(|file| (file, false)))
            .map(|(file, reused)| Member {
                file,
                reused,
                deletes: rows.next().flatten(),
                shrank: shrank.next().unwrap_or(false),
            })
            .collect();
        let targets: Vec<Arc<LoadedFile>> = members
            .iter()
            .map(|member| Arc::clone(&member.file))
            .collect();
        let mut generation = Generation::assemble(
            table,
            &self.snapshot,
            self.snapshot_timestamp,
            &self.metadata_location,
            self.layout,
            self.config.residency,
            members,
        )
        .await?;
        let at: HashMap<*const LoadedFile, u32> = (0_u32..)
            .zip(&generation.files)
            .map(|(at, file)| (Arc::as_ptr(file), at))
            .collect();
        let moved_to: Vec<Option<u32>> = targets
            .iter()
            .map(|file| at.get(&Arc::as_ptr(file)).copied())
            .collect();
        generation.delete_reads = applied.known.reindexed(&moved_to);
        generation.index_bytes += approx::length(generation.delete_reads.resident_bytes());
        generation.delete_bound = self.delete_bound;
        generation.oldest_delete_committed = applied.oldest.and_then(|sequence| {
            committed_at(self.metadata.snapshots().map(Arc::as_ref), sequence)
        });
        Ok(generation)
    }
}

/// Where a new file stands in the level gate's overlap sweep
/// (`Planned::edge_ranges`).
#[derive(Clone, Debug, PartialEq, Eq)]
enum Edge {
    /// No other file is in a bucket one of its keys can fall in, so it
    /// overlaps none; nothing was read.
    Alone,
    /// Its first and last key.
    Span(Key, Key),
    /// An edge page did not decode to a key; the file has no span in the
    /// sweep, and the build's verification refuses it.
    Unknown,
}

/// The `Edge` of a file: `Edge::Alone` unless it `shares` a bucket, and
/// then from the chunk file `local` holds for it when that is trusted,
/// else from its edge pages.
async fn edge_of(
    inspected: &mut Inspected,
    local: Option<&LocalDir>,
    shares: bool,
) -> Result<Edge> {
    if !shares {
        return Ok(Edge::Alone);
    }
    if let Some(local) = local
        && let Some((first, last)) = inspected.trusted_key_range(local).await?
    {
        return Ok(Edge::Span(first, last));
    }
    Ok(match inspected.edge_keys().await? {
        Some((first, last)) => Edge::Span(first, last),
        None => Edge::Unknown,
    })
}

/// Per entry, in order, whether another entry is in a bucket that one key
/// can fall in together with the entry's own (`buckets_may_share_a_key`):
/// another entry of the same bucketing and bucket, or one of another
/// bucketing whose bucket is congruent, or any entry when either has no
/// bucketing. Buckets of one bucketing are compared only with the
/// bucketing's other entries, so the pairwise pass runs across bucketings
/// alone, which a table carries only after a spec evolution.
fn sharing<'a>(entries: impl Iterator<Item = &'a DataFileEntry>) -> Vec<bool> {
    let places: Vec<(Option<Bucketing>, u32)> = entries
        .map(|entry| (entry.bucketing, entry.bucket))
        .collect();
    let mut held: BTreeMap<Option<Bucketing>, BTreeMap<u32, usize>> = BTreeMap::new();
    for &(bucketing, bucket) in &places {
        *held
            .entry(bucketing)
            .or_default()
            .entry(bucket)
            .or_default() += 1;
    }
    let shared = |(bucketing, bucket): (Option<Bucketing>, u32)| {
        held[&bucketing][&bucket] > 1
            || held.iter().any(|(&theirs, buckets)| {
                theirs != bucketing
                    && buckets
                        .keys()
                        .any(|&other| buckets_may_share_a_key((bucketing, bucket), (theirs, other)))
            })
    };
    places.into_iter().map(shared).collect()
}

/// A file of a generation being assembled, whether it came from the
/// previous generation, which verified every overlap group it was in, and
/// the rows its deletes remove, with whether they no longer hold every row
/// the previous generation's removed.
struct Member {
    file: Arc<LoadedFile>,
    reused: bool,
    deletes: Option<Arc<FileDeletes>>,
    shrank: bool,
}

/// `DeleteFault::RecordCountMismatch` for the first of `files` with
/// `deletes` whose manifest records another row count than its footer
/// gives: a delete's positions are checked against the manifest's count,
/// so the two must agree where a delete applies.
fn refuse_record_count_mismatch(
    files: &[Arc<LoadedFile>],
    deletes: &[Option<Arc<FileDeletes>>],
) -> Result<()> {
    for (file, _) in files
        .iter()
        .zip(deletes)
        .filter(|(_, deletes)| deletes.is_some())
    {
        let footer = file.index.rows();
        if u64::try_from(footer).ok() != Some(file.entry.records) {
            return Err(Error::File {
                path: file.entry.path.clone(),
                fault: FileFault::Delete(DeleteFault::RecordCountMismatch {
                    manifest: file.entry.records,
                    footer,
                }),
            });
        }
    }
    Ok(())
}

/// The order of a generation's files: bucketing, bucket, the exact first
/// key, then path, so two loads of one snapshot order the same and files
/// of one bucket are in key order for the overlap sweep.
fn file_order(a: &LoadedFile, b: &LoadedFile) -> Ordering {
    a.entry
        .bucketing
        .cmp(&b.entry.bucketing)
        .then(a.entry.bucket.cmp(&b.entry.bucket))
        .then_with(|| a.index.key_range().0.cmp(b.index.key_range().0))
        .then_with(|| a.entry.path.cmp(&b.entry.path))
}

/// One file as the overlap sweep sees it: the bucket its manifest entry
/// files it under, its exact first and last key, and the snapshot that
/// added it. A span's position in its slice stands for the file's.
#[derive(Clone, Copy, Debug)]
struct Span<'a> {
    bucketing: Option<Bucketing>,
    bucket: u32,
    first: &'a Key,
    last: &'a Key,
    /// The snapshot the file's manifest entry records as adding it
    /// (`DataFileEntry::added_by`), compared for equality alone: files one
    /// snapshot added may share an edge key on an ordered table.
    added_by: SnapshotId,
}

impl<'a> Span<'a> {
    /// The span of an indexed file, its key range decoded at build.
    fn of(file: &'a LoadedFile) -> Self {
        let (first, last) = file.index.key_range();
        Self {
            bucketing: file.entry.bucketing,
            bucket: file.entry.bucket,
            first,
            last,
            added_by: file.entry.added_by,
        }
    }

    /// The bucket the span is filed under, with its bucketing.
    fn place(&self) -> (Option<Bucketing>, u32) {
        (self.bucketing, self.bucket)
    }
}

/// The routes of `spans`, which are in file order: by bucketing, then
/// bucket, then first key.
fn routes(spans: &[Span<'_>]) -> Routes {
    let mut routes = Routes::new();
    for (at, span) in spans.iter().enumerate() {
        routes
            .entry(span.bucketing)
            .or_default()
            .entry(span.bucket)
            .or_default()
            .push(at);
    }
    routes
}

/// Where a file stands in probe order (RFD 021): its data sequence number,
/// highest first, then its position in file order. Positions are distinct,
/// so ranks are.
type ProbeRank = (Reverse<SequenceNumber>, usize);

/// `routes` with every position as its probe rank and every bucket's
/// ranks sorted, computed once per generation so that a lookup sorts
/// nothing and its merge compares ranks without reaching into `files`.
fn ranked_routes(files: &[Arc<LoadedFile>], routes: &Routes) -> RankedRoutes {
    routes
        .iter()
        .map(|(&bucketing, buckets)| {
            let ranked = buckets
                .iter()
                .map(|(&bucket, positions)| {
                    let mut ranks: Vec<ProbeRank> = positions
                        .iter()
                        .map(|&at| (Reverse(files[at].entry.sequence), at))
                        .collect();
                    ranks.sort_unstable();
                    (bucket, ranks)
                })
                .collect();
            (bucketing, ranked)
        })
        .collect()
}

/// The ranks, ascending, of the bucket `key` falls in under `bucketing`;
/// empty when no file is in it.
fn bucket_ranks<'a>(
    bucketing: Option<Bucketing>,
    buckets: &'a BTreeMap<u32, Vec<ProbeRank>>,
    key: &Key,
) -> &'a [ProbeRank] {
    let bucket = bucketing.map_or(manifest::UNBUCKETED, |bucketing| bucketing.bucket_of(key));
    buckets.get(&bucket).map_or(&[], Vec::as_slice)
}

/// The positions of a key's candidate files in probe order, before the key
/// range filter: per bucketing the files carry, the ranks of the key's
/// bucket (`bucket_ranks`), merged. Under one bucketing that list is walked
/// as it is. Under several, each step takes the least rank past the last
/// one taken, found by binary search in each list, so a lookup allocates
/// nothing. A position is under one bucketing alone, so no rank repeats.
struct ProbeOrder<'a> {
    routes: &'a RankedRoutes,
    key: &'a Key,
    /// The one list, when the files carry one bucketing or none.
    walk: Option<std::slice::Iter<'a, ProbeRank>>,
    /// The last rank the merge took.
    after: Option<ProbeRank>,
}

impl<'a> ProbeOrder<'a> {
    fn new(routes: &'a RankedRoutes, key: &'a Key) -> Self {
        let mut lists = routes.iter();
        let walk = match (lists.next(), lists.next()) {
            (None, _) => Some([].iter()),
            (Some((bucketing, buckets)), None) => {
                Some(bucket_ranks(*bucketing, buckets, key).iter())
            }
            (Some(_), Some(_)) => None,
        };
        Self {
            routes,
            key,
            walk,
            after: None,
        }
    }
}

impl Iterator for ProbeOrder<'_> {
    type Item = usize;

    fn next(&mut self) -> Option<usize> {
        if let Some(walk) = &mut self.walk {
            return walk.next().map(|&(_, at)| at);
        }
        let after = self.after;
        let next = self
            .routes
            .iter()
            .filter_map(|(bucketing, buckets)| {
                let list = bucket_ranks(*bucketing, buckets, self.key);
                let start = after.map_or(0, |after| list.partition_point(|&rank| rank <= after));
                list.get(start).copied()
            })
            .min()?;
        self.after = Some(next);
        Some(next.1)
    }
}

/// The snapshot's timestamp as jiff holds it; one outside jiff's range is
/// `TableFault::SnapshotTimestampOutOfRange` under `table`.
fn snapshot_timestamp(table: &str, snapshot: &Snapshot) -> Result<jiff::Timestamp> {
    time::timestamp_from_millis(snapshot.timestamp_ms()).map_err(|_out_of_range| {
        Error::table_fault(
            table,
            TableFault::SnapshotTimestampOutOfRange {
                snapshot_id: SnapshotId(snapshot.snapshot_id()),
            },
        )
    })
}

/// When the deletes at `sequence` were committed, from the metadata's
/// `snapshots`: the earliest, by timestamp then id, of those whose sequence
/// number it is; else, once those have expired, the earliest of those
/// whose sequence number is greater, which committed after the deletes and
/// so dates them no earlier than they were, a lower bound on their age.
/// A snapshot whose timestamp jiff cannot represent is passed over, since
/// it dates a gauge and never decides a load. `None` where no listed
/// snapshot qualifies.
fn committed_at<'a>(
    snapshots: impl IntoIterator<Item = &'a Snapshot>,
    sequence: SequenceNumber,
) -> Option<jiff::Timestamp> {
    let dated: Vec<(SequenceNumber, jiff::Timestamp, i64)> = snapshots
        .into_iter()
        .filter_map(|snapshot| {
            // intentional: an unrepresentable timestamp leaves the snapshot
            // out of the dating, never the load.
            let at = time::timestamp_from_millis(snapshot.timestamp_ms()).ok()?;
            Some((
                SequenceNumber(snapshot.sequence_number()),
                at,
                snapshot.snapshot_id(),
            ))
        })
        .collect();
    let earliest = |keep: &dyn Fn(SequenceNumber) -> bool| {
        dated
            .iter()
            .filter(|(at, ..)| keep(*at))
            .min_by_key(|&&(_, committed, id)| (committed, id))
            .map(|&(_, committed, _)| committed)
    };
    earliest(&|at| at == sequence).or_else(|| earliest(&|at| at > sequence))
}

/// A file the previous generation does not hold, open for inspection.
type Pending = (DataFileEntry, Arc<FileHandle>);

/// Splits the snapshot's files into those `previous` holds by the same path
/// at the same size and data sequence number, reused as the same `Arc`
/// (RFD 005), and the rest, each opened as a `FileHandle` with the
/// manifest's size and `hedge`. A data file is immutable, so a path whose
/// size changed is another file and its index would be stale; a path
/// committed again at another sequence number is another file to the
/// deletes, which apply by sequence number (RFD 021), so its entry would
/// be stale. A file kept is checked against `layout`, the snapshot's
/// (`FileIndex::check_columns`): a column it no longer holds as `layout`
/// means it refuses the snapshot under `table`, and one whose index was
/// built over other leaves than `layout` resolves to is opened afresh, so
/// no column is answered from another's pages. Opening sends nothing to
/// the store; the first S3 location a storage opens loads the SDK's
/// defaults (`Storage::locate`).
async fn reuse_or_open(
    table: &str,
    layout: &TableLayout,
    entries: Vec<DataFileEntry>,
    previous: Option<&Generation>,
    storage: &Arc<Storage>,
    hedge: &Arc<HedgeWindow>,
) -> Result<(Vec<Arc<LoadedFile>>, Vec<Pending>)> {
    let reusable: HashMap<&str, &Arc<LoadedFile>> = previous
        .map(|generation| {
            generation
                .files
                .iter()
                .map(|file| (file.entry.path.as_str(), file))
                .collect()
        })
        .unwrap_or_default();
    let mut reused = Vec::new();
    let mut pending = Vec::new();
    for entry in entries {
        let unchanged = reusable
            .get(entry.path.as_str())
            .filter(|file| file.entry.size == entry.size && file.entry.sequence == entry.sequence);
        let kept = match unchanged {
            Some(file) => file.index.check_columns(table, layout)?.then_some(file),
            None => None,
        };
        if let Some(file) = kept {
            reused.push(Arc::clone(file));
        } else {
            let handle = FileHandle::open(
                Arc::clone(storage),
                &entry.path,
                entry.size,
                Arc::clone(hedge),
            )
            .await?;
            pending.push((entry, Arc::new(handle)));
        }
    }
    Ok((reused, pending))
}

/// Inspects every pending file's footer under `layout`, `load_concurrency()`
/// at a time; the first refusal or fault ends the load.
async fn inspect_all(
    table: &str,
    layout: &TableLayout,
    pending: Vec<Pending>,
) -> Result<Vec<(DataFileEntry, Inspected)>> {
    stream::iter(pending)
        .map(|(entry, handle)| async move {
            let inspected = FileIndex::inspect(table, handle, layout).await?;
            Ok::<_, Error>((entry, inspected))
        })
        .buffered(load_concurrency())
        .try_collect()
        .await
}

/// Builds every inspected file's index in `residency`, `load_concurrency()`
/// at a time, after the budget check admitted them all, and refuses a
/// file whose fences or first or last key fall outside its recorded bucket
/// (`refuse_fences_outside_bucket`). Every Disk-tier chunk file write holds
/// `reservation`, the load's hold on the budget, until it ends.
async fn build_all(
    table: &str,
    residency: Residency,
    local: Option<&Arc<LocalDir>>,
    reservation: Option<&Arc<Reservation>>,
    inspected: Vec<(DataFileEntry, Inspected)>,
) -> Result<Vec<Arc<LoadedFile>>> {
    stream::iter(inspected)
        .map(|(entry, inspected)| async move {
            let index = inspected.build(residency, local, reservation).await?;
            let file = LoadedFile { entry, index };
            refuse_fences_outside_bucket(table, &file)?;
            Ok::<_, Error>(Arc::new(file))
        })
        .buffered(load_concurrency())
        .try_collect()
        .await
}

/// The budget the residency sets, checked before any key chunk or delete
/// is fetched (RFD 017, RFD 021). Full: `config.resident_budget_bytes`,
/// against the resident bytes of the reused files plus the predicted full
/// bytes of every new one plus `deletes`, the bound on the applied deletes,
/// which counts reused deletes too, so the check is a function of the
/// snapshot; the budget is the table's alone, and the check holds nothing.
/// Disk: `local.index_budget()`, shared by every table, reserved through
/// `LocalDir::reserve` for the bytes every new file would write, which is
/// nothing for a file whose chunk file is already there, against the bytes
/// `index/` holds (the reused files' chunk files among them) and every
/// other load's outstanding reservation. The reservation returned must be
/// held until the load has finished writing; every chunk file write shares
/// it and holds it until the write ends. Bloom takes no budget. Over is
/// `RefusalReason::OverBudget` carrying both numbers. The sums saturate, as
/// the predictions they add do: a need that would wrap stays at `u64::MAX`
/// instead of wrapping to a small number that fits the budget.
fn check_budget(
    table: &str,
    config: &TableConfig,
    local: Option<&Arc<LocalDir>>,
    reused: &[Arc<LoadedFile>],
    inspected: &[(DataFileEntry, Inspected)],
    deletes: u64,
) -> Result<Option<Arc<Reservation>>> {
    match config.residency {
        Residency::Full => {
            let new = inspected
                .iter()
                .map(|(_, inspected)| inspected.predicted().full);
            check_resident(table, config, reused, new, deletes)?;
            Ok(None)
        }
        Residency::Disk => match local {
            Some(local) if local.index_budget().is_some() => {
                let new = inspected.iter().try_fold(0, |sum: u64, (_, inspected)| {
                    Ok::<_, Error>(sum.saturating_add(inspected.disk_bytes_needed(local)?))
                })?;
                local.reserve(table, new).map(|held| Some(Arc::new(held)))
            }
            Some(_) | None => Ok(None),
        },
        Residency::Bloom => Ok(None),
    }
}

/// `config.resident_budget_bytes` of a Full-tier load, when it sets one,
/// against the resident bytes of the `reused` files, `new`, the new files'
/// resident bytes, and `deletes`, the bound on the applied deletes;
/// `RefusalReason::OverBudget` carrying both numbers over it, the sum
/// saturating (`check_budget`).
fn check_resident(
    table: &str,
    config: &TableConfig,
    reused: &[Arc<LoadedFile>],
    new: impl IntoIterator<Item = u64>,
    deletes: u64,
) -> Result<()> {
    let Some(budget) = config.resident_budget_bytes else {
        return Ok(());
    };
    let needed = index::saturating_sum(
        reused
            .iter()
            .map(|file| approx::count(file.index.resident_bytes()))
            .chain(new)
            .chain([deletes]),
    );
    if needed > budget {
        return Err(Error::refused(
            table,
            RefusalReason::OverBudget {
                of: Budget::Resident,
                needed,
                budget,
            },
        ));
    }
    Ok(())
}

/// The Full tier's budget checked again once an ordered table's new files
/// are built, with their exact resident bytes in place of the footer's
/// prediction (RFD 025, Load): an order fence of a string or binary value
/// keeps a prefix of its page's first value, which no footer number bounds
/// under a dictionary-encoded column, so the prediction can fall short of
/// the fences built. Over the budget is `RefusalReason::OverBudget` with
/// the exact bytes; the other tiers' memory takes no budget.
fn check_built_budget(
    table: &str,
    config: &TableConfig,
    reused: &[Arc<LoadedFile>],
    built: &[Arc<LoadedFile>],
    deletes: u64,
) -> Result<()> {
    match config.residency {
        Residency::Full => {
            let new = built
                .iter()
                .map(|file| approx::count(file.index.resident_bytes()));
            check_resident(table, config, reused, new, deletes)
        }
        Residency::Disk | Residency::Bloom => Ok(()),
    }
}

/// `RefusalReason::KeyOutsideBucket` under `table` for the first of `keys`
/// that hashes to another bucket under `entry`'s bucketing than the one
/// `entry` records; `Ok` for an unbucketed entry. Routing sends a key to
/// no bucketed file outside the bucket it hashes to, so a key held in one
/// would answer `Absent` though present (RFD 007).
fn refuse_outside_bucket<K: Borrow<Key>>(
    table: &str,
    entry: &DataFileEntry,
    keys: impl IntoIterator<Item = K>,
) -> Result<()> {
    let Some(bucketing) = entry.bucketing else {
        return Ok(());
    };
    for key in keys {
        let key = key.borrow();
        let computed = bucketing.bucket_of(key);
        if computed != entry.bucket {
            return Err(Error::refused(
                table,
                RefusalReason::KeyOutsideBucket {
                    path: entry.path.clone(),
                    key: key.clone(),
                    recorded: entry.bucket,
                    computed,
                },
            ));
        }
    }
    Ok(())
}

/// The load's bucket check: `refuse_outside_bucket` over `file`'s fences
/// (the first key of every key page) and its first and last key, all held
/// once the index is built, so the check costs one hash per key page and
/// no decode: O(pages), not O(rows). It refuses a file recorded under the
/// wrong bucket, whose keys all hash elsewhere, and a file whose keys
/// change bucket at a page boundary or at either end; it serves a file
/// whose keys change bucket between two fences, which a writer that
/// partitions by bucket does not produce. Hashing every key would add a
/// murmur3 hash per row to every load, tens of nanoseconds each, which is
/// seconds to minutes on a table of billions of rows, so that check is the
/// doctor's (`Generation::verify_buckets`).
fn refuse_fences_outside_bucket(table: &str, file: &LoadedFile) -> Result<()> {
    let (first, last) = file.index.key_range();
    let fences = file.index.fences().iter().flatten();
    refuse_outside_bucket(table, &file.entry, fences.chain([first, last]))
}

/// The doctor's per-file bucket check: `refuse_outside_bucket` over every
/// key of `file`, decoded from `chunks` one key page at a time.
fn refuse_any_key_outside_bucket(table: &str, file: &LoadedFile, chunks: &Segments) -> Result<()> {
    let index = &file.index;
    for row_group in 0..index.metadata().num_row_groups() {
        for page in 0..index.page_count(row_group) {
            let keys = index.decode_keys(row_group, Some(page), chunks)?;
            refuse_outside_bucket(table, &file.entry, (0..keys.len()).map(|at| keys.get(at)))?;
        }
    }
    Ok(())
}

/// Whether the key ranges of two spans intersect.
fn intersects(a: &Span<'_>, b: &Span<'_>) -> bool {
    a.first <= b.last && b.first <= a.last
}

/// A disjoint-set forest over positions in a slice of spans.
struct Components {
    parent: Vec<usize>,
}

impl Components {
    fn new(len: usize) -> Self {
        Self {
            parent: (0..len).collect(),
        }
    }

    /// The representative of `at`'s set, halving the path on the way.
    fn root(&mut self, mut at: usize) -> usize {
        while self.parent[at] != at {
            let grandparent = self.parent[self.parent[at]];
            self.parent[at] = grandparent;
            at = grandparent;
        }
        at
    }

    fn link(&mut self, a: usize, b: usize) {
        let (a, b) = (self.root(a), self.root(b));
        if a != b {
            self.parent[a] = b;
        }
    }

    /// The sets of more than one member, each in ascending position order,
    /// ordered by their first member.
    fn groups(mut self) -> Vec<Vec<usize>> {
        let mut by_root: BTreeMap<usize, Vec<usize>> = BTreeMap::new();
        for at in 0..self.parent.len() {
            let root = self.root(at);
            by_root.entry(root).or_default().push(at);
        }
        let mut groups: Vec<Vec<usize>> = by_root
            .into_values()
            .filter(|members| members.len() > 1)
            .collect();
        groups.sort_by_key(|members| members[0]);
        groups
    }
}

/// The overlap groups of `spans` (RFD 005), whose `routes` are given: the
/// connected components of "may hold a key in common", where two files may
/// when their key ranges intersect and one key can fall in both their buckets
/// (`buckets_may_share_a_key`). Within one bucket the files are in
/// first-key order, so a file joins the running group exactly when it
/// starts at or before the group's greatest last key; across two
/// bucketings, which a table carries only after a spec evolution, every
/// intersecting pair of files in buckets a key can share is linked, a
/// pairwise pass whose cost is accepted for that rare case.
fn overlap_groups(spans: &[Span<'_>], routes: &Routes) -> Vec<Vec<usize>> {
    let mut components = Components::new(spans.len());
    for buckets in routes.values() {
        for members in buckets.values() {
            let mut open: Option<(usize, &Key)> = None;
            for &at in members {
                let Span { first, last, .. } = spans[at];
                match open {
                    Some((member, group_last)) if first <= group_last => {
                        components.link(member, at);
                        if last > group_last {
                            open = Some((member, last));
                        }
                    }
                    Some(_) | None => open = Some((at, last)),
                }
            }
        }
    }
    let per_bucketing: Vec<(Option<Bucketing>, &Buckets)> = routes
        .iter()
        .map(|(bucketing, buckets)| (*bucketing, buckets))
        .collect();
    for (at, ours) in per_bucketing.iter().enumerate() {
        for theirs in &per_bucketing[at + 1..] {
            link_across_bucketings(&mut components, spans, *ours, *theirs);
        }
    }
    components.groups()
}

/// Links every pair of spans, one under each bucketing, whose key ranges
/// intersect and whose buckets one key can fall in at once.
fn link_across_bucketings(
    components: &mut Components,
    spans: &[Span<'_>],
    (ours, our_buckets): (Option<Bucketing>, &Buckets),
    (theirs, their_buckets): (Option<Bucketing>, &Buckets),
) {
    for (&our_bucket, our_members) in our_buckets {
        for (&their_bucket, their_members) in their_buckets {
            if !buckets_may_share_a_key((ours, our_bucket), (theirs, their_bucket)) {
                continue;
            }
            for &a in our_members {
                for &b in their_members {
                    if intersects(&spans[a], &spans[b]) {
                        components.link(a, b);
                    }
                }
            }
        }
    }
}

/// Whether one key can be in bucket `a` under one bucketing and bucket `b`
/// under another. A key's bucket under `bucket[n]` is its hash modulo `n`
/// (`bucket::bucket`), so under counts `m` and `n` its two buckets are
/// congruent modulo `gcd(m, n)`; files whose buckets are not can never
/// share a key, and linking them would fuse every file of an evolved table
/// into one group and make every later append re-verify the whole table,
/// the cost RFD 005 rejected. A file without a bucketing holds keys of
/// every bucket.
fn buckets_may_share_a_key(a: (Option<Bucketing>, u32), b: (Option<Bucketing>, u32)) -> bool {
    match (a.0, b.0) {
        (Some(ours), Some(theirs)) => {
            let common = gcd(ours.count, theirs.count);
            a.1 % common == b.1 % common
        }
        (None, _) | (_, None) => true,
    }
}

/// The greatest common divisor of two bucket counts, both above zero.
fn gcd(mut a: u32, mut b: u32) -> u32 {
    while b != 0 {
        (a, b) = (b, a % b);
    }
    a
}

/// How the overlap sweep counts files that meet only at an edge key, one
/// file's last key the other's first.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Edges {
    /// A key-value table's: every intersection of two key ranges is
    /// overlap.
    Overlap,
    /// An ordered table's (RFD 025, Levels and findings): files one snapshot
    /// added that meet only at an edge key share it, which is not overlap;
    /// any other intersection is.
    Shared,
}

impl Edges {
    /// The rule `layout`'s kind of table takes.
    fn of(layout: &TableLayout) -> Self {
        match layout.order {
            Some(_) => Self::Shared,
            None => Self::Overlap,
        }
    }
}

/// What the overlap sweep finds (`probe_widths`), per bucket number as
/// `Generation::overlapping_buckets` keys it.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct ProbeWidths {
    /// Per bucket where files overlap, the most files one lookup or Scan
    /// there may probe: what `overlapping_files` caps the level for.
    pub(crate) overlapping: BTreeMap<u32, usize>,
    /// On an ordered table, per bucket where files one snapshot added meet
    /// only at edge keys, the most files one such key spans: what
    /// `shared_edge_keys` caps the level for. Empty on a key-value table.
    pub(crate) shared_edges: BTreeMap<u32, usize>,
}

impl ProbeWidths {
    /// The most files any key's lookup or Scan probes, one where no files
    /// overlap and no edge key is shared.
    pub(crate) fn most_probed(&self) -> Option<usize> {
        self.overlapping
            .values()
            .chain(self.shared_edges.values())
            .copied()
            .max()
    }
}

/// Per bucket a group member is in, the most files one lookup there may
/// probe: a lookup probes the candidates whose exact key range covers its
/// key, so a group whose files chain end to end probes fewer files than it
/// holds. Each group is swept in first-key order, and at each member's
/// first key the members whose range still covers it are counted as
/// `OpenFiles::probed_together` counts them. Every key's candidates are
/// open at the last-swept of them, so no lookup probes more than the sweep
/// records. Under `Edges::Shared`, the count at a key goes under
/// `shared_edges` instead when the open members a key can reach there all
/// share it as an edge key (`Reach::shares_an_edge`), and a key one file
/// alone reaches is not counted: a bucket appears under `overlapping`
/// exactly when two of its files intersect other than at a shared edge
/// key, since such a pair is open together at the later one's first key.
/// The open members are kept counted per bucket (`OpenFiles`), so the work
/// is two sorts of the group and, per member, a step per bucket that holds
/// open members.
fn probe_widths(spans: &[Span<'_>], groups: &[Vec<usize>], edges: Edges) -> ProbeWidths {
    let mut widths = ProbeWidths::default();
    for group in groups {
        let mut by_first = group.clone();
        by_first.sort_by(|&a, &b| spans[a].first.cmp(spans[b].first));
        let mut by_last = group.clone();
        by_last.sort_by(|&a, &b| spans[a].last.cmp(spans[b].last));
        let mut open = OpenFiles::default();
        let mut closing = 0;
        for &at in &by_first {
            let step = spans[at];
            while let Some(&next) = by_last.get(closing) {
                if spans[next].last >= step.first {
                    break;
                }
                open.close(spans[next]);
                closing += 1;
            }
            open.open(step);
            let widest = match edges {
                Edges::Overlap => &mut widths.overlapping,
                Edges::Shared => {
                    let reached = open.reach(step);
                    if reached.files <= FILES_PROBED_ALONE {
                        continue;
                    }
                    if reached.shares_an_edge() {
                        &mut widths.shared_edges
                    } else {
                        &mut widths.overlapping
                    }
                }
            };
            open.probed_together(step, widest);
        }
    }
    widths
}

#[cfg(test)]
thread_local! {
    /// The steps this thread's overlap sweeps took over open files: what a
    /// test counts a sweep's work by.
    static SWEPT: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
}

/// The steps overlap sweeps took on this thread since the last call.
#[cfg(test)]
fn take_swept() -> u64 {
    SWEPT.replace(0)
}

/// The files that may end at a shared edge key, and the files that may
/// start at one: one each, since two that end at it, or two that start at
/// it, share more keys than it.
const ONE_SIDE_OF_AN_EDGE: usize = 1;

/// The open members of one bucket in an overlap sweep, counted: how many,
/// by the snapshot that added them, and by their last key, and of those
/// that start at the first key the latest of them starts at, how many, and
/// how many end there too.
#[derive(Default)]
struct OpenInBucket<'a> {
    files: usize,
    added_by: HashMap<SnapshotId, usize>,
    lasts: BTreeMap<&'a Key, usize>,
    start: Option<&'a Key>,
    at_start: usize,
    alone_at_start: usize,
}

/// One key as the open members of a bucket hold it: how many hold it
/// strictly inside their range, how many end at it having started before,
/// how many start at it and end after, and how many hold it alone.
#[derive(Clone, Copy, Default)]
struct Holding {
    inside: usize,
    ending: usize,
    starting: usize,
}

impl OpenInBucket<'_> {
    /// How the open members hold `key`, which every open member's range
    /// covers.
    fn holding(&self, key: &Key) -> Holding {
        let (at_start, alone) = if self.start == Some(key) {
            (self.at_start, self.alone_at_start)
        } else {
            (0, 0)
        };
        let at_last = self.lasts.get(key).copied().unwrap_or(0);
        Holding {
            inside: self.files + alone - at_start - at_last,
            ending: at_last - alone,
            starting: at_start - alone,
        }
    }
}

/// What the open members a key can reach hold of it, summed over the
/// buckets it can share (`OpenFiles::reach`).
struct Reach {
    files: usize,
    /// Whether one snapshot added every one of them.
    one_snapshot: bool,
    holding: Holding,
}

impl Reach {
    /// Whether the members pairwise meet only at the key as an edge key
    /// one snapshot wrote: every one added by one snapshot, none holding
    /// the key inside its range, at most one ending at it and at most one
    /// starting at it, and any number holding it alone. Two that end at it,
    /// or two that start at it, share more keys than it.
    fn shares_an_edge(&self) -> bool {
        self.one_snapshot
            && self.holding.inside == 0
            && self.holding.ending <= ONE_SIDE_OF_AN_EDGE
            && self.holding.starting <= ONE_SIDE_OF_AN_EDGE
    }
}

/// The open members of an overlap sweep, counted per bucket they are filed
/// under, with its bucketing; a bucket leaves once none of its members is
/// open.
#[derive(Default)]
struct OpenFiles<'a> {
    buckets: BTreeMap<(Option<Bucketing>, u32), OpenInBucket<'a>>,
}

impl<'a> OpenFiles<'a> {
    /// Opens `span`, which starts at or after every open member.
    fn open(&mut self, span: Span<'a>) {
        let bucket = self.buckets.entry(span.place()).or_default();
        bucket.files += 1;
        *bucket.added_by.entry(span.added_by).or_insert(0) += 1;
        *bucket.lasts.entry(span.last).or_insert(0) += 1;
        if bucket.start != Some(span.first) {
            bucket.start = Some(span.first);
            bucket.at_start = 0;
            bucket.alone_at_start = 0;
        }
        bucket.at_start += 1;
        if span.first == span.last {
            bucket.alone_at_start += 1;
        }
    }

    /// Closes `span`, an open member.
    fn close(&mut self, span: Span<'a>) {
        let place = span.place();
        let bucket = self
            .buckets
            .get_mut(&place)
            .expect("a member closed was opened in its bucket");
        bucket.files -= 1;
        if bucket.files == 0 {
            self.buckets.remove(&place);
            return;
        }
        let less = |count: &mut usize| {
            *count -= 1;
            *count > 0
        };
        if !bucket.added_by.get_mut(&span.added_by).is_some_and(less) {
            bucket.added_by.remove(&span.added_by);
        }
        if !bucket.lasts.get_mut(span.last).is_some_and(less) {
            bucket.lasts.remove(span.last);
        }
    }

    /// The buckets with open members, each with its bucketing and number.
    fn each(&self) -> impl Iterator<Item = (&(Option<Bucketing>, u32), &OpenInBucket<'a>)> {
        #[cfg(test)]
        SWEPT.set(SWEPT.get() + approx::count(self.buckets.len()));
        self.buckets.iter()
    }

    /// What the open members a key at `step`'s first key can reach hold of
    /// it: those in buckets one key can share with `step`'s.
    fn reach(&self, step: Span<'_>) -> Reach {
        let mut reach = Reach {
            files: 0,
            one_snapshot: true,
            holding: Holding::default(),
        };
        let mut snapshot = None;
        for (&place, bucket) in self.each() {
            if !buckets_may_share_a_key(step.place(), place) {
                continue;
            }
            reach.files += bucket.files;
            let holding = bucket.holding(step.first);
            reach.holding.inside += holding.inside;
            reach.holding.ending += holding.ending;
            reach.holding.starting += holding.starting;
            let mut added = bucket.added_by.keys();
            let only = added.next();
            if added.next().is_some() || (snapshot.is_some() && snapshot != only) {
                reach.one_snapshot = false;
            }
            snapshot = snapshot.or(only);
        }
        reach
    }

    /// Records in `widest`, per bucket, the files one key can reach among
    /// the open members, whose ranges cover `step`'s first key: every
    /// unbucketed member, the members of `step`'s own bucket under its
    /// bucketing, and under each other bucketing one bucket that can share
    /// a key with `step`'s. The count with each other bucketing's largest
    /// such bucket goes under the buckets of the unbucketed members and of
    /// `step`'s own, and each other bucket's count with it in place of its
    /// bucketing's largest goes under that bucket. Under at most one
    /// bucketing beside unbucketed files that is every candidate set a key
    /// reaching `step` can have; under two or more, the buckets taken from
    /// different bucketings are not checked against one another, so the
    /// count is an upper bound.
    fn probed_together(&self, step: Span<'_>, widest: &mut BTreeMap<u32, usize>) {
        let mut together: Vec<u32> = Vec::new();
        let mut together_files = 0;
        let mut others: Vec<(Bucketing, u32, usize)> = Vec::new();
        for (&(bucketing, bucket), open) in self.each() {
            match bucketing {
                None => {
                    together.push(bucket);
                    together_files += open.files;
                }
                Some(_) if bucketing == step.bucketing => {
                    if bucket == step.bucket {
                        together.push(bucket);
                        together_files += open.files;
                    }
                }
                Some(other) => {
                    if buckets_may_share_a_key(step.place(), (bucketing, bucket)) {
                        others.push((other, bucket, open.files));
                    }
                }
            }
        }
        let mut largest: BTreeMap<Bucketing, usize> = BTreeMap::new();
        for &(bucketing, _, files) in &others {
            let entry = largest.entry(bucketing).or_insert(0);
            *entry = (*entry).max(files);
        }
        let total = together_files + largest.values().sum::<usize>();
        let mut record = |bucket: u32, probed: usize| {
            let entry = widest.entry(bucket).or_insert(0);
            *entry = (*entry).max(probed);
        };
        for bucket in together {
            record(bucket, total);
        }
        for (bucketing, bucket, files) in others {
            record(bucket, total - largest[&bucketing] + files);
        }
    }
}

/// One file of an overlap group with the key chunks its merge decodes
/// from, owned, so that the merge can move to the blocking pool.
struct GroupMember {
    /// The file's position in the generation's files.
    at: usize,
    file: Arc<LoadedFile>,
    chunks: Arc<Segments>,
}

/// Every member of one `group` with its key chunks, in the group's order:
/// what its merge decodes from, gathered `load_concurrency()` members at a
/// time. Full hands out its resident chunks; Disk reads them back as local
/// reads; Bloom fetches them again (RFD 017). The positions are streamed by
/// value: a closure taking a reference and returning an `async` block makes
/// every future up the call chain unprovably `Send`, and a load must be
/// spawnable.
async fn gather_members(files: &[Arc<LoadedFile>], group: &[usize]) -> Result<Vec<GroupMember>> {
    stream::iter(group.iter().copied())
        .map(|at| async move {
            let file = Arc::clone(&files[at]);
            let chunks = file.index.key_chunks_for_verification().await?;
            Ok::<_, Error>(GroupMember { at, file, chunks })
        })
        .buffered(load_concurrency())
        .try_collect()
        .await
}

/// Which rows a merge passes over as deleted (RFD 021): `true` for the row
/// at a position in the file at a position in the generation's files.
type DeletedRows = dyn Fn(usize, RowPosition) -> bool + Send + Sync;

/// A cursor over one file's live keys in file order, one key page decoded
/// at a time, so a merge holds one page per file and never a row group.
/// It knows each key's position in the file, counted from the file's first
/// row as Iceberg's deletes count it, and passes over the rows `deleted`
/// names.
struct KeyCursor<'a> {
    /// The file's position in the generation's files.
    at: usize,
    file: &'a LoadedFile,
    chunks: &'a Arc<Segments>,
    deleted: Option<&'a DeletedRows>,
    row_group: usize,
    /// The next key page to decode in `row_group`.
    page: usize,
    keys: Keys,
    /// The position in the file of the first of `keys`.
    page_start: RowPosition,
    /// The head's index in `keys`.
    offset: usize,
}

impl<'a> KeyCursor<'a> {
    /// A cursor on the first live key of `member`'s file; `None` for a
    /// file with no live key, or no key page, which `FileIndex::build` does
    /// not produce.
    fn open(member: &'a GroupMember, deleted: Option<&'a DeletedRows>) -> Result<Option<Self>> {
        let mut cursor = Self {
            at: member.at,
            file: &member.file,
            chunks: &member.chunks,
            deleted,
            row_group: 0,
            page: 0,
            keys: Keys::Long(Vec::new()),
            page_start: RowPosition(0),
            offset: 0,
        };
        let more = cursor.fill()?;
        Ok(cursor.past_deleted(more)?.then_some(cursor))
    }

    /// The key under the cursor.
    fn head(&self) -> Key {
        self.keys.get(self.offset)
    }

    /// The position in the file of the row under the cursor.
    fn head_position(&self) -> RowPosition {
        RowPosition(self.page_start.0 + approx::count(self.offset))
    }

    /// Moves to the next live key; `false` once the file is exhausted.
    fn advance(&mut self) -> Result<bool> {
        let more = self.step()?;
        self.past_deleted(more)
    }

    /// Steps over deleted rows from the head while `more` holds; whether
    /// a live key is under the cursor.
    fn past_deleted(&mut self, mut more: bool) -> Result<bool> {
        if let Some(deleted) = self.deleted {
            while more && deleted(self.at, self.head_position()) {
                more = self.step()?;
            }
        }
        Ok(more)
    }

    /// Moves to the next key, deleted or not; `false` once the file is
    /// exhausted.
    fn step(&mut self) -> Result<bool> {
        self.offset += 1;
        if self.offset < self.keys.len() {
            return Ok(true);
        }
        self.fill()
    }

    /// Decodes the next key page that holds a key; `false` once the file
    /// is exhausted.
    fn fill(&mut self) -> Result<bool> {
        let index = &self.file.index;
        while self.row_group < index.metadata().num_row_groups() {
            if self.page >= index.page_count(self.row_group) {
                self.row_group += 1;
                self.page = 0;
                continue;
            }
            self.keys = index.decode_keys(self.row_group, Some(self.page), self.chunks)?;
            self.page_start = index.key_page_start(self.row_group, self.page);
            self.page += 1;
            self.offset = 0;
            if !self.keys.is_empty() {
                return Ok(true);
            }
        }
        Ok(false)
    }
}

/// The heads of cursors `a` and `b` compared in served order.
fn head_order(cursors: &[KeyCursor<'_>], a: usize, b: usize) -> Ordering {
    cursors[a]
        .keys
        .cmp_at(cursors[a].offset, &cursors[b].keys, cursors[b].offset)
}

/// Merges the sorted runs of live keys of a group's `members` and refuses
/// the first key two files share; a row `deleted` names is not live, so a
/// key whose other copies are all deleted is no duplicate (RFD 021). Every
/// file is sorted and free of duplicates on its own (`FileIndex::build`
/// checked), so two equal heads at the front of the merge are exactly a key
/// in two files. The cursors are kept in a vector ordered by head; each
/// step takes the least, advances it, and reinserts it by binary search.
/// The refusal names the key and the later of the two files in file order
/// (RFD 007).
fn refuse_duplicates(
    table: &str,
    members: &[GroupMember],
    deleted: Option<&DeletedRows>,
) -> Result<()> {
    let mut cursors: Vec<KeyCursor<'_>> = members
        .iter()
        .filter_map(|member| KeyCursor::open(member, deleted).transpose())
        .collect::<Result<_>>()?;
    let mut order: Vec<usize> = (0..cursors.len()).collect();
    order.sort_by(|&a, &b| head_order(&cursors, a, b));
    while order.len() > 1 {
        let (least, next) = (order[0], order[1]);
        if head_order(&cursors, least, next) == Ordering::Equal {
            let later = if cursors[least].at > cursors[next].at {
                least
            } else {
                next
            };
            return Err(Error::refused(
                table,
                RefusalReason::DuplicateKeys {
                    path: cursors[later].file.entry.path.clone(),
                    key: cursors[least].head(),
                },
            ));
        }
        order.remove(0);
        if cursors[least].advance()? {
            let insert_at = order
                .partition_point(|&cursor| head_order(&cursors, cursor, least) == Ordering::Less);
            order.insert(insert_at, least);
        }
    }
    Ok(())
}

/// Merges every overlap group of `groups` that a member of `members` not
/// reused, or reused with deletes that shrank, belongs to, refusing a live
/// key two files share (`refuse_duplicates`); `deletes` are the rows each
/// file's deletes remove. The groups are merged one at a time, each one's
/// key chunks gathered before its merge and dropped after, so the load
/// holds one group's chunks at once and not the table's (RFD 017: the Disk
/// tier reads them back from the chunk files, the Bloom tier fetches them
/// again). Each merge runs on the blocking pool
/// (`blocking::on_blocking_pool`), one at a time for this load; while it
/// runs, the async worker goes on polling other tasks, and the task goes on
/// polling the other loads it joins. A canceled merge is
/// `Error::VerifyCanceled` for the group's first file. How many groups were
/// merged.
async fn verify_groups(
    table: &str,
    members: &[Member],
    files: &[Arc<LoadedFile>],
    groups: Vec<Vec<usize>>,
    deletes: &Arc<[Option<Arc<FileDeletes>>]>,
) -> Result<usize> {
    let to_verify: Vec<Vec<usize>> = groups
        .into_iter()
        .filter(|group| {
            group
                .iter()
                .any(|&at| !members[at].reused || members[at].shrank)
        })
        .collect();
    for group in &to_verify {
        let members = gather_members(files, group).await?;
        let first = files[group[0]].index.handle().location.clone();
        let owned_table = table.to_string();
        let removed = Arc::clone(deletes);
        blocking::on_blocking_pool(move || {
            let gone =
                move |at: usize, position: RowPosition| removed[at].as_deref().deleted(position);
            refuse_duplicates(&owned_table, &members, Some(&gone))
        })
        .await
        .map_err(|canceled| Error::verify_canceled(&first, canceled))??;
    }
    Ok(to_verify.len())
}

/// What a lookup answers for one key; the wire carries the same answer
/// (RFD 010, RFD 022).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Row {
    /// The key is present, with one value per column the lookup named, in
    /// the order it named them: each the column's bytes unchanged, or
    /// `None` for a null, which is not a value of zero length. A lookup that
    /// named no column carries none.
    Present(Vec<Option<Bytes>>),
    /// No file holds the key.
    Absent,
}

/// What one Scan of a key read beside its answer (`Generation::scan`): the
/// files holding the key whose run it read, and whether any order or value
/// page came from object storage.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct ScanRead {
    /// Files holding the key.
    pub(crate) files: usize,
    /// Whether any order or value page came from object storage, a fetch
    /// the Scan ran or joined.
    pub(crate) from_store: bool,
}

/// One lookup's answer and the snapshot it came from.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Lookup {
    /// The outcome.
    pub row: Row,
    /// The snapshot the answer was read from.
    pub snapshot_id: SnapshotId,
}

/// Several lookups' answers from one snapshot, in the order of the keys
/// asked, each naming the same columns. Fails as a whole if any key's read
/// fails; partial results are a later, separate call.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MultiLookup {
    /// One outcome per key, in order.
    pub rows: Vec<Row>,
    /// The snapshot every answer was read from.
    pub snapshot_id: SnapshotId,
}

/// Iceberg tables in a temporary directory whose data files arrow writes
/// under typed layout knobs, followed through `Storage`'s `file://` store
/// and loaded through `Generation::plan_load` and `Planned::build`, with
/// the key and value derivations every fixture shares; for the tests of
/// this module and of `check`. Nothing here is compiled outside tests.
#[cfg(test)]
pub(crate) mod testing {
    use std::collections::{BTreeMap, HashMap};
    use std::sync::Arc;

    use arrow::array::{
        ArrayRef, BinaryArray, FixedSizeBinaryArray, Int64Array, RecordBatch, StringArray,
    };
    use arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
    use bytes::Bytes;
    use iceberg::io::FileIO;
    use iceberg::spec::{
        DataContentType, DataFile, DataFileFormat, ManifestContentType, NestedField, PrimitiveType,
        Schema, TableMetadata, TableMetadataBuilder, Type, UnboundPartitionSpec,
    };
    use parquet::arrow::ArrowWriter;
    use parquet::basic::{Compression, ZstdLevel};
    use parquet::file::properties::{WriterProperties, WriterPropertiesBuilder};
    use parquet::schema::types::ColumnPath;

    use super::{Generation, Planned, Row};
    use crate::approx::count;
    use crate::cache::PageCache;
    use crate::catalog::{TableFault, TableSource, VERSION_HINT, hint_location};
    use crate::config::{
        HedgeConfig, LocalConfig, Residency, SourceConfig, StorageConfig, TableConfig,
        ValueColumnConfig,
    };
    use crate::contract::PAGE_ROWS;
    use crate::error::{Error, RefusalReason, Result};
    use crate::key::{Key, KeyType, UUID_BYTES};
    use crate::local::LocalDir;
    use crate::manifest::Bucketing;
    use crate::manifest::testing::{
        self as manifest_testing, DATA_DIR, KEY_COLUMN, KEY_FIELD_ID, Partition, VALUE_COLUMN,
        Written,
    };
    use crate::storage::{HedgeWindow, IoStats, Storage};

    /// The table every fixture is named.
    pub(crate) const TABLE: &str = "kv";
    /// The memory budget of a test page cache or row cache: every page or
    /// value a test touches fits, so no test sees an eviction it did not
    /// ask for.
    pub(crate) const CACHE_BYTES: u64 = 64 * 1024 * 1024;
    /// The branch the fixture tables are followed on.
    pub(crate) const MAIN: &str = iceberg::spec::MAIN_BRANCH;
    /// The step between written long keys, so the key after any written key
    /// is absent and inside the file's range.
    pub(crate) const KEY_STEP: i64 = 3;
    /// A resident-bytes budget below any file's resident bytes, so a `full`
    /// load under it is refused at the footer stage.
    pub(crate) const TINY_BUDGET: u64 = 1;
    /// Bytes per value where a fixture does not say.
    pub(crate) const VALUE_BYTES: usize = 32;
    /// The mixers that make a value a function of its key or row and of the
    /// byte's position, distinct for neighboring keys: two small primes and
    /// a prime modulus under 256, so every byte is in range.
    const VALUE_KEY_MIX: i64 = 31;
    const VALUE_BYTE_MIX: i64 = 7;
    const VALUE_MODULUS: i64 = 251;
    /// The false-positive rate a `Bloom::Sized` filter is written at, the
    /// writer recipe's (RFD 002).
    pub(crate) const BLOOM_FPP: f64 = 0.01;
    /// Buckets of the two-bucket fixture.
    pub(crate) const TWO: u32 = 2;
    /// Keys in each end-to-end data file.
    pub(crate) const E2E_KEYS: i64 = 24;
    /// Keys in a one-file fixture: enough that a filter sized to them lands
    /// between `BLOOM_BITS_PER_KEY_MIN` and `BLOOM_BYTES_PER_KEY_ADVICE` and
    /// draws no finding of its own (the writer's smallest filter is 32
    /// bytes plus a header).
    pub(crate) const FILE_KEYS: i64 = 32;
    /// The snapshot ids and sequence numbers of the end-to-end snapshots.
    pub(crate) const FIRST_SNAPSHOT: i64 = 1_001;
    pub(crate) const SECOND_SNAPSHOT: i64 = 1_002;
    pub(crate) const THIRD_SNAPSHOT: i64 = 1_003;
    pub(crate) const FIRST_SEQUENCE: i64 = 1;
    pub(crate) const SECOND_SEQUENCE: i64 = 2;
    pub(crate) const THIRD_SEQUENCE: i64 = 3;
    /// The GETs a poll of a static table root costs when the version hint
    /// names the metadata location the table knows: the hint alone (RFD
    /// 005).
    pub(crate) const POLL_GETS: u64 = 1;
    /// The GETs resolving a table root costs: its version hint and the
    /// metadata file the hint names.
    pub(crate) const RESOLVE_GETS: u64 = 2;
    /// The metadata versions of the end-to-end snapshots.
    pub(crate) const FIRST_VERSION: u64 = 1;
    pub(crate) const SECOND_VERSION: u64 = 2;
    pub(crate) const THIRD_VERSION: u64 = 3;

    /// What a lookup naming one value column answers for a present key
    /// whose value is `bytes`.
    pub(crate) fn one_value(bytes: impl Into<Bytes>) -> Row {
        Row::Present(vec![Some(bytes.into())])
    }

    /// What a lookup naming one value column answers for a present key
    /// whose value is null.
    pub(crate) fn one_null() -> Row {
        Row::Present(vec![None])
    }

    /// The contract's page row limit as the writer takes it.
    pub(crate) fn page_rows() -> usize {
        usize::try_from(PAGE_ROWS).unwrap()
    }

    /// `VALUE_BYTES` bytes that are a function of `key`.
    pub(crate) fn value_for(key: i64) -> Vec<u8> {
        value_of_len(key, VALUE_BYTES)
    }

    /// `len` bytes that are a function of `mix`, distinct for neighboring
    /// mixes.
    pub(crate) fn value_of_len(mix: i64, len: usize) -> Vec<u8> {
        (0..len)
            .map(|at| {
                let at = i64::try_from(at).unwrap();
                u8::try_from((mix * VALUE_KEY_MIX + at * VALUE_BYTE_MIX).rem_euclid(VALUE_MODULUS))
                    .unwrap()
            })
            .collect()
    }

    /// Each key's value, none null.
    pub(crate) fn values_of(keys: &[i64]) -> Vec<Option<Vec<u8>>> {
        keys.iter().map(|key| Some(value_for(*key))).collect()
    }

    /// Keys `from`, `from + KEY_STEP`, ... `count` of them.
    pub(crate) fn keys_from(from: i64, count: i64) -> Vec<i64> {
        (0..count).map(|at| from + at * KEY_STEP).collect()
    }

    /// The keys among `keys` that `bucketing` routes to `bucket`.
    pub(crate) fn keys_in_bucket(keys: &[i64], bucketing: Bucketing, bucket: u32) -> Vec<i64> {
        keys.iter()
            .copied()
            .filter(|key| bucketing.bucket_of(&Key::Long(*key)) == bucket)
            .collect()
    }

    /// `bucket[count]` over the fixture key column.
    pub(crate) fn bucketing(count: u32) -> Bucketing {
        Bucketing {
            count,
            source_field_id: KEY_FIELD_ID,
        }
    }

    /// `keys` as long keys.
    pub(crate) fn long_keys(keys: &[i64]) -> Vec<Key> {
        keys.iter().map(|key| Key::Long(*key)).collect()
    }

    /// A memory-only page cache of `CACHE_BYTES`.
    pub(crate) async fn cache() -> PageCache {
        PageCache::new(CACHE_BYTES, None).await.unwrap()
    }

    /// The refusal `result` ended in, for `TABLE`.
    pub(crate) fn refusal_of<T: std::fmt::Debug>(result: Result<T>) -> RefusalReason {
        let Err(Error::Refused { table, reason }) = result else {
            panic!("expected a refusal, got {result:?}");
        };
        assert_eq!(table, TABLE);
        reason
    }

    /// The table fault `result` ended in, under `table`.
    pub(crate) fn table_fault_of<T: std::fmt::Debug>(result: Result<T>, table: &str) -> TableFault {
        let Err(Error::Table {
            table: named,
            fault,
        }) = result
        else {
            panic!("expected a table fault, got {result:?}");
        };
        assert_eq!(named, table);
        fault
    }

    /// Which fixture columns are dictionary-encoded.
    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    pub(crate) enum Dictionary {
        /// Neither: the contract's shape.
        Off,
        /// The key column alone, whose dictionary page costs nothing extra.
        Key,
        /// The value column alone, whose dictionary page is one more GET per
        /// hit.
        Value,
        /// Every column.
        All,
    }

    /// `builder` with dictionary encoding on the columns `dictionary` names
    /// and off elsewhere.
    pub(crate) fn with_dictionary(
        builder: WriterPropertiesBuilder,
        dictionary: Dictionary,
    ) -> WriterPropertiesBuilder {
        let builder = builder.set_dictionary_enabled(dictionary == Dictionary::All);
        let column = match dictionary {
            Dictionary::Off | Dictionary::All => None,
            Dictionary::Key => Some(KEY_COLUMN),
            Dictionary::Value => Some(VALUE_COLUMN),
        };
        match column {
            Some(column) => builder.set_column_dictionary_enabled(ColumnPath::from(column), true),
            None => builder,
        }
    }

    /// Whether and how the key column gets a bloom filter. The writer sizes
    /// a filter for the file's keys and folds it down to the smallest size
    /// that meets the false-positive rate, so the rate is the one knob that
    /// sets its bytes per key.
    #[derive(Clone, Copy, Debug, PartialEq)]
    pub(crate) enum Bloom {
        /// None.
        Off,
        /// Sized to the file's keys at `BLOOM_FPP`: the contract's filter.
        Sized,
        /// Sized to the file's keys at this false-positive rate: far below
        /// `BLOOM_FPP` an oversized filter, far above it an undersized one.
        Fpp(f64),
    }

    impl Bloom {
        /// The false-positive rate the filter is written at; `None` for no
        /// filter.
        pub(crate) fn fpp(self) -> Option<f64> {
            match self {
                Self::Off => None,
                Self::Sized => Some(BLOOM_FPP),
                Self::Fpp(fpp) => Some(fpp),
            }
        }
    }

    /// `builder` with a filter on the key column written at `bloom`'s rate
    /// for `ndv` distinct keys, or unchanged for no filter. The filter
    /// setters enable a filter on the column they name, so the value column
    /// never gets one.
    pub(crate) fn with_key_bloom(
        builder: WriterPropertiesBuilder,
        bloom: Bloom,
        ndv: u64,
    ) -> WriterPropertiesBuilder {
        let Some(fpp) = bloom.fpp() else {
            return builder;
        };
        let key = ColumnPath::from(KEY_COLUMN);
        builder
            .set_column_bloom_filter_enabled(key.clone(), true)
            .set_column_bloom_filter_fpp(key.clone(), fpp)
            .set_column_bloom_filter_ndv(key, ndv)
    }

    /// The writer's layout knobs; `contract()` is RFD 002's shape.
    #[derive(Clone, Copy, Debug, PartialEq)]
    pub(crate) struct WriterLayout {
        /// Rows per page, key and value alike; the write batch is the same,
        /// so the row limit binds exactly.
        pub(crate) page_rows: usize,
        /// Which column is dictionary-encoded.
        pub(crate) dictionary: Dictionary,
        /// The column index's truncation length; `None` keeps whole values.
        pub(crate) column_index_truncate: Option<usize>,
        /// The key column's bloom filter.
        pub(crate) bloom: Bloom,
        /// Rows per row group; `None` leaves the writer's default, one row
        /// group for every fixture this size.
        pub(crate) row_group_rows: Option<usize>,
        /// The pages' compression; uncompressed pages are decoded in place,
        /// so a value read from one is a slice of the page.
        pub(crate) compression: Compression,
    }

    impl WriterLayout {
        /// RFD 002's shape: contract pages, no dictionary, a whole column
        /// index, a sized filter, zstd pages.
        pub(crate) fn contract() -> Self {
            Self {
                page_rows: page_rows(),
                dictionary: Dictionary::Off,
                column_index_truncate: None,
                bloom: Bloom::Sized,
                row_group_rows: None,
                compression: Compression::ZSTD(ZstdLevel::default()),
            }
        }

        /// The writer properties for a file of `rows` rows, the filter
        /// sized to all of them.
        fn properties(&self, rows: usize) -> WriterProperties {
            let builder = WriterProperties::builder()
                .set_data_page_row_count_limit(self.page_rows)
                .set_write_batch_size(self.page_rows)
                .set_column_index_truncate_length(self.column_index_truncate)
                .set_compression(self.compression)
                .set_bloom_filter_enabled(false)
                .set_max_row_group_row_count(self.row_group_rows);
            let builder = with_dictionary(builder, self.dictionary);
            with_key_bloom(builder, self.bloom, count(rows)).build()
        }
    }

    /// How a fixture's key column is typed in arrow.
    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    pub(crate) enum KeyColumn {
        Int64,
        Utf8,
        Binary,
        FixedBinary(i32),
    }

    /// The arrow column a key type is written as: a uuid as a fixed binary
    /// of `UUID_BYTES`, as Parquet stores the type.
    pub(crate) fn column_of(key_type: KeyType) -> KeyColumn {
        match key_type {
            KeyType::Long => KeyColumn::Int64,
            KeyType::String => KeyColumn::Utf8,
            KeyType::Binary => KeyColumn::Binary,
            KeyType::Uuid => KeyColumn::FixedBinary(i32::try_from(UUID_BYTES).unwrap()),
        }
    }

    /// The arrow type of a key column.
    pub(crate) fn key_data_type(column: KeyColumn) -> DataType {
        match column {
            KeyColumn::Int64 => DataType::Int64,
            KeyColumn::Utf8 => DataType::Utf8,
            KeyColumn::Binary => DataType::Binary,
            KeyColumn::FixedBinary(width) => DataType::FixedSizeBinary(width),
        }
    }

    /// `keys` as the key column's array; a `None` is a null.
    pub(crate) fn key_array(column: KeyColumn, keys: &[Option<Key>]) -> ArrayRef {
        let raw = || {
            keys.iter()
                .map(|key| key.as_ref().map(|key| key.as_bytes().to_vec()))
        };
        match column {
            KeyColumn::Int64 => Arc::new(Int64Array::from(
                keys.iter()
                    .map(|key| {
                        key.as_ref().map(|key| {
                            let Key::Long(value) = key else {
                                panic!("a long fixture holds long keys");
                            };
                            *value
                        })
                    })
                    .collect::<Vec<_>>(),
            )),
            KeyColumn::Utf8 => Arc::new(
                keys.iter()
                    .map(|key| {
                        key.as_ref().map(|key| {
                            let Key::Str(text) = key else {
                                panic!("a string fixture holds string keys");
                            };
                            text.clone()
                        })
                    })
                    .collect::<StringArray>(),
            ),
            KeyColumn::Binary => Arc::new(raw().collect::<BinaryArray>()),
            KeyColumn::FixedBinary(width) => Arc::new(
                FixedSizeBinaryArray::try_from_sparse_iter_with_size(raw(), width).unwrap(),
            ),
        }
    }

    /// The second value column of a table `Table::with_detail` makes.
    pub(crate) const DETAIL_COLUMN: &str = "detail";
    /// Its field id, after the key's and the value's.
    const DETAIL_FIELD_ID: i32 = 3;

    /// What `DETAIL_COLUMN` holds for a row whose value is `value`: its
    /// bytes reversed, so no lookup of the one reads the other's bytes.
    pub(crate) fn detail_of(value: &[u8]) -> Vec<u8> {
        value.iter().rev().copied().collect()
    }

    /// A Parquet file of `keys` with `values` (a `None` is a null) laid out
    /// as `layout` says, one row group, with `DETAIL_COLUMN` holding each
    /// value's `detail_of` when `detail` is set.
    pub(crate) fn parquet(
        key_type: KeyType,
        keys: &[Key],
        values: &[Option<Vec<u8>>],
        detail: bool,
        layout: &WriterLayout,
    ) -> Bytes {
        let column = column_of(key_type);
        let mut fields = vec![
            Field::new(KEY_COLUMN, key_data_type(column), false),
            Field::new(VALUE_COLUMN, DataType::Binary, true),
        ];
        let keys: Vec<Option<Key>> = keys.iter().cloned().map(Some).collect();
        let mut arrays = vec![
            key_array(column, &keys),
            Arc::new(values.iter().cloned().collect::<BinaryArray>()) as ArrayRef,
        ];
        if detail {
            fields.push(Field::new(DETAIL_COLUMN, DataType::Binary, true));
            arrays.push(Arc::new(
                values
                    .iter()
                    .map(|value| value.as_deref().map(detail_of))
                    .collect::<BinaryArray>(),
            ));
        }
        let schema = Arc::new(ArrowSchema::new(fields));
        let batch = RecordBatch::try_new(Arc::clone(&schema), arrays).unwrap();
        let mut out = Vec::new();
        let mut writer =
            ArrowWriter::try_new(&mut out, schema, Some(layout.properties(keys.len()))).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        Bytes::from(out)
    }

    /// The order column of a table `Table::ordered` makes, a long.
    pub(crate) const ORDER_COLUMN: &str = "ts";
    /// Its field id, after the key's, the value's, and the detail's.
    const ORDER_FIELD_ID: i32 = 4;
    /// The mixer that makes an ordered row's value a function of its key
    /// and its order value, distinct for a key's neighboring rows.
    const ROW_KEY_MIX: i64 = 1_009;

    /// The value of an ordered fixture's row at (`key`, `order`).
    pub(crate) fn row_value_for(key: i64, order: i64) -> Vec<u8> {
        value_of_len(key * ROW_KEY_MIX + order, VALUE_BYTES)
    }

    /// A Parquet file of an ordered table holding `rows`, (key, order
    /// value) pairs: the long key, the value `row_value_for`, its
    /// `detail_of`, and the long order column, laid out as `layout` says.
    pub(crate) fn ordered_parquet(rows: &[(i64, i64)], layout: &WriterLayout) -> Bytes {
        let fields = vec![
            Field::new(KEY_COLUMN, DataType::Int64, false),
            Field::new(VALUE_COLUMN, DataType::Binary, true),
            Field::new(DETAIL_COLUMN, DataType::Binary, true),
            Field::new(ORDER_COLUMN, DataType::Int64, false),
        ];
        let values: Vec<Vec<u8>> = rows
            .iter()
            .map(|&(key, order)| row_value_for(key, order))
            .collect();
        let arrays: Vec<ArrayRef> = vec![
            Arc::new(Int64Array::from_iter_values(
                rows.iter().map(|&(key, _)| key),
            )),
            Arc::new(BinaryArray::from_iter_values(values.iter())),
            Arc::new(BinaryArray::from_iter_values(
                values.iter().map(|value| detail_of(value)),
            )),
            Arc::new(Int64Array::from_iter_values(
                rows.iter().map(|&(_, order)| order),
            )),
        ];
        let schema = Arc::new(ArrowSchema::new(fields));
        let batch = RecordBatch::try_new(Arc::clone(&schema), arrays).unwrap();
        let mut out = Vec::new();
        let mut writer =
            ArrowWriter::try_new(&mut out, schema, Some(layout.properties(rows.len()))).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        Bytes::from(out)
    }

    /// The Iceberg schema of a table keyed by `key_type` with binary values.
    pub(crate) fn schema_for(key_type: KeyType) -> Schema {
        let key = match key_type {
            KeyType::Long => PrimitiveType::Long,
            KeyType::String => PrimitiveType::String,
            KeyType::Binary => PrimitiveType::Binary,
            KeyType::Uuid => PrimitiveType::Uuid,
        };
        manifest_testing::schema(Type::Primitive(key), Type::Primitive(PrimitiveType::Binary))
    }

    /// What a fixture file's manifest entry records as its key bounds.
    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    pub(crate) enum Bounds {
        /// The exact least and greatest key, as
        /// `write.metadata.metrics.column.<key>=full` records them.
        Exact,
        /// No bounds.
        Missing,
        /// String bounds cut to this many bytes, as Iceberg's default
        /// metrics cut them (prototype findings, finding 8): the lower
        /// bound is the cut prefix, the upper the cut prefix with its last
        /// byte incremented, so both still bound the keys.
        Truncated(usize),
    }

    /// `bounds` as the manifest records them for a file whose least and
    /// greatest keys are `min` and `max`.
    fn recorded_bounds(bounds: Bounds, min: &Key, max: &Key) -> Option<(Key, Key)> {
        match bounds {
            Bounds::Exact => Some((min.clone(), max.clone())),
            Bounds::Missing => None,
            Bounds::Truncated(len) => Some((
                truncated_bound(min, len, false),
                truncated_bound(max, len, true),
            )),
        }
    }

    /// A string key cut to `len` bytes, the last byte incremented for an
    /// upper bound; fixture keys are ASCII, so the cut is text.
    fn truncated_bound(key: &Key, len: usize, upper: bool) -> Key {
        let Key::Str(text) = key else {
            panic!("truncated bounds are a string-key fixture");
        };
        let mut cut = text.as_bytes()[..len].to_vec();
        if upper {
            *cut.last_mut().unwrap() += 1;
        }
        Key::Str(String::from_utf8(cut).unwrap())
    }

    /// One data file to write and record for the next snapshot.
    pub(crate) struct FileSpec<'a> {
        /// The file's name under the table's data directory.
        pub(crate) name: &'a str,
        /// Its keys, ascending, all of the table's key type.
        pub(crate) keys: &'a [Key],
        /// Its values, one per key; a `None` is a null.
        pub(crate) values: Vec<Option<Vec<u8>>>,
        /// Its partition tuple under the table's default spec.
        pub(crate) partition: Partition,
        /// What its manifest entry records as key bounds.
        pub(crate) bounds: Bounds,
        /// How its Parquet is laid out.
        pub(crate) layout: WriterLayout,
    }

    /// An Iceberg table in a temporary directory: metadata from
    /// `TableMetadataBuilder`, data files arrow writes, manifests and
    /// snapshots iceberg-rust writes, all followed through `Storage`'s
    /// `file://` store.
    pub(crate) struct Table {
        dir: tempfile::TempDir,
        /// The one storage every read goes through; its counter is the oracle.
        pub(crate) storage: Arc<Storage>,
        /// The table's hedge window, handed to every load.
        pub(crate) hedge: Arc<HedgeWindow>,
        key_type: KeyType,
        /// Whether the table has `DETAIL_COLUMN` beside the value column.
        detail: bool,
        /// Whether the table is ordered by `ORDER_COLUMN` (`ordered`).
        ordered: bool,
        md: TableMetadata,
        /// The default spec, which `add_file` writes under.
        spec_id: i32,
        /// Every data file written and delete file recorded so far with the
        /// spec it was written under: its entry for the next manifest.
        pub(crate) files: Vec<(DataFile, i32)>,
        /// How later commits carry each path of `files` that an earlier
        /// commit first listed: as an existing entry at that commit's
        /// sequence number, added by its snapshot.
        committed: HashMap<String, Written>,
    }

    impl Table {
        /// A table keyed by `key_type` under `spec`, with no snapshot yet.
        pub(crate) fn new(key_type: KeyType, spec: UnboundPartitionSpec) -> Self {
            Self::of(key_type, spec, false, false)
        }

        /// `new` with `DETAIL_COLUMN` beside the value column, in the
        /// schema, in every file `add_file` writes, and in `config`'s
        /// value columns (RFD 022).
        pub(crate) fn with_detail(key_type: KeyType, spec: UnboundPartitionSpec) -> Self {
            Self::of(key_type, spec, true, false)
        }

        /// A long-keyed ordered table (RFD 025) bucketed `buckets` ways,
        /// with `DETAIL_COLUMN` beside the value column and the long
        /// `ORDER_COLUMN` after them, as `config` serves it; its files are
        /// written by `add_ordered_file`.
        pub(crate) fn ordered(buckets: u32) -> Self {
            Self::of(
                KeyType::Long,
                manifest_testing::bucket_spec(buckets),
                true,
                true,
            )
        }

        fn of(key_type: KeyType, spec: UnboundPartitionSpec, detail: bool, ordered: bool) -> Self {
            let dir = tempfile::tempdir().unwrap();
            let root = manifest_testing::file_root(dir.path());
            let mut schema = schema_for(key_type);
            let mut added = Vec::new();
            if detail {
                added.push(NestedField::optional(
                    DETAIL_FIELD_ID,
                    DETAIL_COLUMN,
                    Type::Primitive(PrimitiveType::Binary),
                ));
            }
            if ordered {
                added.push(NestedField::required(
                    ORDER_FIELD_ID,
                    ORDER_COLUMN,
                    Type::Primitive(PrimitiveType::Long),
                ));
            }
            if !added.is_empty() {
                let mut fields: Vec<_> = schema.as_struct().fields().to_vec();
                fields.extend(added.into_iter().map(Arc::new));
                schema = Schema::builder().with_fields(fields).build().unwrap();
            }
            let md = manifest_testing::metadata(&root, schema, spec);
            Self {
                dir,
                storage: Arc::new(Storage::new(StorageConfig::default())),
                hedge: Arc::new(HedgeWindow::new(&HedgeConfig::default())),
                key_type,
                detail,
                ordered,
                spec_id: md.default_partition_spec_id(),
                md,
                files: Vec::new(),
                committed: HashMap::new(),
            }
        }

        /// A long-keyed table bucketed `buckets` ways.
        pub(crate) fn bucketed(buckets: u32) -> Self {
            Self::new(KeyType::Long, manifest_testing::bucket_spec(buckets))
        }

        /// The table root as a `file://` location.
        pub(crate) fn root(&self) -> String {
            manifest_testing::file_root(self.dir.path())
        }

        /// The location data file `name` is written at.
        pub(crate) fn path_of(&self, name: &str) -> String {
            format!("{}/{DATA_DIR}/{name}.parquet", self.root())
        }

        /// A configuration following this table's main branch in `residency`
        /// with `budget` as its resident-bytes budget.
        pub(crate) fn config(&self, residency: Residency, budget: Option<u64>) -> TableConfig {
            TableConfig {
                name: TABLE.to_string(),
                source: SourceConfig::Static {
                    location: self.root(),
                },
                r#ref: MAIN.to_string(),
                key_column: KEY_COLUMN.to_string(),
                value_columns: if self.detail {
                    vec![
                        ValueColumnConfig::named(VALUE_COLUMN),
                        ValueColumnConfig::named(DETAIL_COLUMN),
                    ]
                } else {
                    vec![ValueColumnConfig::named(VALUE_COLUMN)]
                },
                order_column: self.ordered.then(|| ORDER_COLUMN.to_string()),
                max_scan_rows: None,
                strict: true,
                lookups: None,
                residency,
                resident_budget_bytes: budget,
                row_cache: None,
            }
        }

        /// Writes file `name` of an ordered table holding `rows`, (key,
        /// order value) pairs ascending, each row's value `row_value_for`
        /// and its detail that value's `detail_of`, in `partition`, laid
        /// out as `layout` says with exact key bounds, and records it for
        /// the next snapshot.
        pub(crate) async fn add_ordered_file(
            &mut self,
            name: &str,
            rows: &[(i64, i64)],
            partition: Partition,
            layout: WriterLayout,
        ) {
            assert!(self.ordered, "an ordered table's file");
            let path = self.path_of(name);
            let bytes = ordered_parquet(rows, &layout);
            let size = count(bytes.len());
            manifest_testing::write_file(&FileIO::new_with_fs(), &path, bytes).await;
            let keys: Vec<Key> = rows.iter().map(|&(key, _)| Key::Long(key)).collect();
            let min = keys.iter().min().unwrap();
            let max = keys.iter().max().unwrap();
            self.files.push((
                manifest_testing::data_file(
                    &path,
                    self.spec_id,
                    manifest_testing::partition(partition),
                    count(rows.len()),
                    size,
                    Some((min, max)),
                    DataContentType::Data,
                ),
                self.spec_id,
            ));
        }

        /// Writes `spec`'s file and records it for the next snapshot.
        pub(crate) async fn add_file(&mut self, spec: FileSpec<'_>) {
            let path = self.path_of(spec.name);
            let bytes = parquet(
                self.key_type,
                spec.keys,
                &spec.values,
                self.detail,
                &spec.layout,
            );
            let size = count(bytes.len());
            manifest_testing::write_file(&FileIO::new_with_fs(), &path, bytes).await;
            let min = spec.keys.iter().min().unwrap();
            let max = spec.keys.iter().max().unwrap();
            let bounds = recorded_bounds(spec.bounds, min, max);
            self.files.push((
                manifest_testing::data_file(
                    &path,
                    self.spec_id,
                    manifest_testing::partition(spec.partition),
                    count(spec.keys.len()),
                    size,
                    bounds.as_ref().map(|(min, max)| (min, max)),
                    DataContentType::Data,
                ),
                self.spec_id,
            ));
        }

        /// `add_file` of a contract-layout file of long `keys` with their
        /// values and exact bounds, in `bucket`.
        pub(crate) async fn add_long_file(&mut self, name: &str, keys: &[i64], bucket: u32) {
            let typed = long_keys(keys);
            self.add_file(FileSpec {
                name,
                keys: &typed,
                values: values_of(keys),
                partition: Partition::Bucket(bucket),
                bounds: Bounds::Exact,
                layout: WriterLayout::contract(),
            })
            .await;
        }

        /// Records a position-delete file `name` in Avro in `bucket` for the
        /// next snapshot, listed in a delete manifest. Nothing is written at
        /// its path: icetray does not read Avro position deletes, and
        /// refuses the entry from the manifest alone (RFD 021).
        pub(crate) fn add_avro_position_delete(&mut self, name: &str, bucket: u32) {
            self.files.push((
                manifest_testing::delete_file(
                    &self.path_of(name),
                    self.spec_id,
                    manifest_testing::partition(Partition::Bucket(bucket)),
                    manifest_testing::DeleteSpec::Positions(DataFileFormat::Avro),
                ),
                self.spec_id,
            ));
        }

        /// Writes a Parquet position delete file `name` in `bucket` that
        /// deletes `positions` of data file `data` and records it for the
        /// next snapshot, its entry naming `data` as its referenced data
        /// file; `data` need not be a file the table holds.
        pub(crate) async fn add_position_delete(
            &mut self,
            name: &str,
            data: &str,
            positions: &[i64],
            bucket: u32,
        ) {
            let path = self.path_of(name);
            let data = self.path_of(data);
            let rows: Vec<(&str, i64)> = positions
                .iter()
                .map(|&position| (data.as_str(), position))
                .collect();
            let bytes = crate::deletes::position_file(&rows);
            let size = count(bytes.len());
            manifest_testing::write_file(&FileIO::new_with_fs(), &path, Bytes::from(bytes)).await;
            self.files.push((
                manifest_testing::sized_delete_file(
                    &path,
                    self.spec_id,
                    manifest_testing::partition(Partition::Bucket(bucket)),
                    manifest_testing::DeleteSpec::ScopedPositions(DataFileFormat::Parquet, data),
                    count(positions.len()),
                    size,
                ),
                self.spec_id,
            ));
        }

        /// Removes snapshot `id` from the metadata and writes it as
        /// `version`, pointing the hint at it, as a writer's snapshot
        /// expiry leaves it.
        pub(crate) async fn expire(&mut self, id: i64, version: u64) {
            self.md = self
                .md
                .clone()
                .into_builder(None)
                .remove_snapshots(&[id])
                .build()
                .unwrap()
                .metadata;
            manifest_testing::write_metadata(&self.md, version).await;
        }

        /// When snapshot `id` was committed, as its metadata records it.
        pub(crate) fn committed(&self, id: i64) -> jiff::Timestamp {
            let millis = self.md.snapshot_by_id(id).unwrap().timestamp_ms();
            crate::time::timestamp_from_millis(millis).unwrap()
        }

        /// Drops the recorded file `name`, so the next snapshot no longer
        /// lists it or lists another file added under the same name.
        pub(crate) fn remove_file(&mut self, name: &str) {
            let path = self.path_of(name);
            self.files.retain(|(file, _)| file.file_path() != path);
            self.committed.remove(&path);
        }

        /// Deletes the version hint, so the next resolve of the root fails
        /// on the store's own error, as a poll does while the root is
        /// unreachable; `restore_hint` or `commit` writes it again.
        pub(crate) fn remove_hint(&self) {
            std::fs::remove_file(self.dir.path().join(VERSION_HINT)).unwrap();
        }

        /// Writes the version hint pointing at metadata `version` again.
        pub(crate) async fn restore_hint(&self, version: u64) {
            manifest_testing::write_file(
                &FileIO::new_with_fs(),
                &hint_location(&self.root()),
                Bytes::from(version.to_string()),
            )
            .await;
        }

        /// Writes the metadata as `version` without the main branch,
        /// pointing the hint at it, as a writer that dropped the branch
        /// leaves it; the next `commit` sets the branch again.
        pub(crate) async fn drop_main(&mut self, version: u64) {
            self.md = self
                .md
                .clone()
                .into_builder(None)
                .remove_ref(MAIN)
                .build()
                .unwrap()
                .metadata;
            manifest_testing::write_metadata(&self.md, version).await;
        }

        /// Evolves the partition spec to `buckets` buckets; files added
        /// from here on are written under the new spec.
        pub(crate) fn evolve(&mut self, buckets: u32) {
            self.md = self
                .md
                .clone()
                .into_builder(None)
                .add_partition_spec(manifest_testing::bucket_spec(buckets))
                .unwrap()
                .set_default_partition_spec(TableMetadataBuilder::LAST_ADDED)
                .unwrap()
                .build()
                .unwrap()
                .metadata;
            self.spec_id = self.md.default_partition_spec_id();
        }

        /// Commits every file added so far as snapshot `id`, one data
        /// manifest per spec the data files were written under and one
        /// delete manifest per spec the delete files were, and writes the
        /// metadata as `version`, pointing the hint at it. A file an earlier
        /// commit listed is an existing entry at that commit's sequence
        /// number, as a writer carries it; the rest are added at `sequence`.
        pub(crate) async fn commit(
            &mut self,
            id: i64,
            parent: Option<i64>,
            sequence: i64,
            version: u64,
        ) {
            let mut by_spec: BTreeMap<(i32, bool), Vec<(DataFile, Written)>> = BTreeMap::new();
            for (file, spec_id) in &self.files {
                let deletes = file.content_type() != DataContentType::Data;
                let written = self
                    .committed
                    .get(file.file_path())
                    .copied()
                    .unwrap_or(Written::Added);
                by_spec
                    .entry((*spec_id, deletes))
                    .or_default()
                    .push((file.clone(), written));
            }
            for (file, _) in &self.files {
                self.committed
                    .entry(file.file_path().to_string())
                    .or_insert(Written::Existing {
                        sequence,
                        added_by: id,
                    });
            }
            let manifests = by_spec
                .into_iter()
                .map(
                    |((spec_id, deletes), entries)| manifest_testing::ManifestSpec {
                        spec_id,
                        content: if deletes {
                            ManifestContentType::Deletes
                        } else {
                            ManifestContentType::Data
                        },
                        entries,
                    },
                )
                .collect();
            let snapshot = manifest_testing::write_snapshot(
                &self.md,
                id,
                parent,
                sequence,
                self.md.current_schema_id(),
                manifests,
            )
            .await;
            self.md = manifest_testing::with_snapshot(self.md.clone(), snapshot);
            manifest_testing::write_metadata(&self.md, version).await;
        }

        /// Resolves the root and plans a load of the main branch's
        /// snapshot through the footer stage.
        pub(crate) async fn plan_load(
            &self,
            config: &TableConfig,
            previous: Option<&Generation>,
        ) -> Result<Planned> {
            let source = TableSource::Static {
                location: self.root(),
            };
            let resolved = source.resolve(&self.storage).await?;
            let snapshot = resolved.snapshot_for_ref(TABLE, MAIN)?;
            Generation::plan_load(
                config,
                &resolved,
                &snapshot,
                &self.storage,
                Arc::clone(&self.hedge),
                previous,
            )
            .await
        }

        /// Resolves the root and loads the main branch's snapshot.
        pub(crate) async fn load(
            &self,
            config: &TableConfig,
            local: Option<&Arc<LocalDir>>,
            previous: Option<&Generation>,
        ) -> Result<Generation> {
            self.plan_load(config, previous).await?.build(local).await
        }

        /// GETs the table's storage has issued so far.
        pub(crate) fn gets(&self) -> u64 {
            self.storage.stats().gets()
        }
    }

    /// Two contract files in two buckets, committed as `FIRST_SNAPSHOT`,
    /// and every key they hold.
    pub(crate) async fn two_bucket_table() -> (Table, Vec<i64>) {
        let mut table = Table::bucketed(TWO);
        let all = keys_from(0, E2E_KEYS);
        for bucket in 0..TWO {
            let keys = keys_in_bucket(&all, bucketing(TWO), bucket);
            table
                .add_long_file(&format!("f{bucket}"), &keys, bucket)
                .await;
        }
        table
            .commit(FIRST_SNAPSHOT, None, FIRST_SEQUENCE, FIRST_VERSION)
            .await;
        (table, all)
    }

    /// A local directory under `dir` charging its reads to `stats`, with
    /// `budget` on its `index/` and no page cache disk level.
    pub(crate) fn local_dir(
        dir: &std::path::Path,
        stats: Arc<IoStats>,
        budget: Option<u64>,
    ) -> Arc<LocalDir> {
        Arc::new(
            LocalDir::open(
                &LocalConfig {
                    dir: dir.to_path_buf(),
                    index_budget_bytes: budget,
                    page_cache_disk_bytes: 0,
                },
                stats,
            )
            .unwrap(),
        )
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeSet, HashMap};

    use iceberg::spec::{Operation, Summary};
    use parquet::file::metadata::{PageIndexPolicy, ParquetMetaDataReader};

    use super::testing::{
        self, Bloom, Bounds, Dictionary, E2E_KEYS, FIRST_SEQUENCE, FIRST_SNAPSHOT, FIRST_VERSION,
        FileSpec, KEY_STEP, RESOLVE_GETS, SECOND_SEQUENCE, SECOND_SNAPSHOT, SECOND_VERSION, TABLE,
        THIRD_SEQUENCE, THIRD_SNAPSHOT, THIRD_VERSION, TINY_BUDGET, TWO, Table, WriterLayout,
        bucketing, cache, keys_from, keys_in_bucket, local_dir, long_keys, refusal_of,
        two_bucket_table, value_for, values_of,
    };
    use super::*;
    use crate::catalog::TableSource;
    use crate::check::thresholds::KEY_PAGE_ROWS_LARGE;
    use crate::config::{ConfigError, ConfigViolation, HedgeConfig, ValueFormat};
    use crate::index::{FileFault, testing as index_testing};
    use crate::key::KeyType;
    use crate::local::ChunkRules;
    use crate::local::testing as local_testing;
    use crate::manifest::UNBUCKETED;
    use crate::manifest::testing::{self as manifest_testing, DATA_DIR, Partition, unpartitioned};
    use crate::storage::FILE_PREFIX;
    use crate::storage::testing::{MEM_PREFIX, MemStore};

    /// Snapshot `id` at `sequence` committed at `millis`, for
    /// `committed_at`, which reads nothing else of it.
    fn dated_snapshot(id: i64, sequence: i64, millis: i64) -> Snapshot {
        Snapshot::builder()
            .with_snapshot_id(id)
            .with_sequence_number(sequence)
            .with_timestamp_ms(millis)
            .with_manifest_list(format!("{MEM_PREFIX}db/kv/metadata/snap-{id}.avro"))
            .with_summary(Summary {
                operation: Operation::Append,
                additional_properties: HashMap::new(),
            })
            .build()
    }

    /// Deletes are dated by the earliest snapshot at their sequence number;
    /// once those expire, by the earliest at a greater one, never by an
    /// older snapshot; and a timestamp jiff cannot represent is passed over.
    #[test]
    fn test_committed_at_dates_by_the_sequence_else_the_earliest_later_snapshot() {
        let deletes = SequenceNumber(SECOND_SEQUENCE);
        let (oldest, early, late) = (1_000, 2_000, 3_000);
        let unrepresentable = i64::MAX;
        let at = |millis| Some(time::timestamp_from_millis(millis).unwrap());
        let both_at_sequence = [
            dated_snapshot(FIRST_SNAPSHOT, SECOND_SEQUENCE, late),
            dated_snapshot(SECOND_SNAPSHOT, SECOND_SEQUENCE, early),
            dated_snapshot(THIRD_SNAPSHOT, THIRD_SEQUENCE, oldest),
        ];
        assert_eq!(committed_at(&both_at_sequence, deletes), at(early));
        let expired = [
            dated_snapshot(FIRST_SNAPSHOT, FIRST_SEQUENCE, oldest),
            dated_snapshot(SECOND_SNAPSHOT, THIRD_SEQUENCE, late),
            dated_snapshot(THIRD_SNAPSHOT, THIRD_SEQUENCE + 1, early),
        ];
        assert_eq!(committed_at(&expired, deletes), at(early));
        let out_of_range = [
            dated_snapshot(SECOND_SNAPSHOT, SECOND_SEQUENCE, unrepresentable),
            dated_snapshot(THIRD_SNAPSHOT, THIRD_SEQUENCE, late),
        ];
        assert_eq!(committed_at(&out_of_range, deletes), at(late));
        let older_only = [dated_snapshot(FIRST_SNAPSHOT, FIRST_SEQUENCE, oldest)];
        assert_eq!(committed_at(&older_only, deletes), None);
    }

    /// The metadata location the assembled generations claim.
    fn mem_metadata() -> String {
        format!("{MEM_PREFIX}db/kv/metadata/v1.metadata.json")
    }
    /// Bucket counts the routing tests use beside the fixture's `TWO`.
    const FOUR: u32 = 4;
    const EIGHT: u32 = 8;
    /// Keys per bucket file in the routing tests.
    const KEYS_PER_FILE: i64 = 16;
    /// Keys spread over the bucketed files of the mixed-bucketing fixture.
    const MIXED_BUCKETED_KEYS: i64 = 64;
    /// The key index the mixed fixture's unbucketed file starts after: late
    /// enough that every bucket's first key comes before it.
    const MIXED_UNBUCKETED_FROM: i64 = 16;
    /// A false-positive rate far above the contract's: the writer folds the
    /// filter to its one-block minimum, so with `BLOOM_KEYS` keys the
    /// filter passes about a third of the absent keys in range and rules
    /// out the rest, and both kinds of miss are reachable.
    const LOOSE_FPP: f64 = 0.5;
    /// Keys in the Bloom-tier fixture's one file: enough to fill a
    /// one-block filter to that rate, few enough that it rules keys out.
    const BLOOM_KEYS: i64 = 64;
    /// The GETs a Bloom-tier cold hit costs: the key page, then the value
    /// page (RFD 017).
    const BLOOM_HIT_GETS: u64 = 2;

    fn layout(bucketing: Option<Bucketing>) -> TableLayout {
        manifest_testing::layout(KeyType::Long, bucketing)
    }

    /// The snapshot the assembled generations are said to serve; nothing
    /// of it is read but its id and sequence number.
    fn placeholder_snapshot() -> Snapshot {
        Snapshot::builder()
            .with_snapshot_id(FIRST_SNAPSHOT)
            .with_sequence_number(FIRST_SEQUENCE)
            .with_timestamp_ms(0)
            .with_manifest_list(format!("{MEM_PREFIX}db/kv/metadata/snap.avro"))
            .with_summary(Summary {
                operation: Operation::Append,
                additional_properties: HashMap::new(),
            })
            .build()
    }

    /// An in-memory store behind a storage, and the files built over it.
    struct Mem {
        store: MemStore,
        hedge: Arc<HedgeWindow>,
    }

    impl Mem {
        fn new() -> Self {
            Self {
                store: MemStore::new(),
                hedge: Arc::new(HedgeWindow::new(&HedgeConfig::default())),
            }
        }

        fn gets(&self) -> u64 {
            self.store.storage.stats().gets()
        }

        fn location(name: &str) -> String {
            format!("{MEM_PREFIX}db/kv/{DATA_DIR}/{name}.parquet")
        }

        /// Writes `keys` with `values` as file `name` laid out as `layout`
        /// and indexes it in the Full tier, filed under `bucketing` and
        /// `bucket`.
        async fn file(
            &self,
            name: &str,
            keys: &[i64],
            values: &[Option<Vec<u8>>],
            (bucketing, bucket): (Option<Bucketing>, u32),
            layout: &WriterLayout,
        ) -> Arc<LoadedFile> {
            let bytes = testing::parquet(
                KeyType::Long,
                &testing::long_keys(keys),
                values,
                false,
                layout,
            );
            let handle = self
                .store
                .open(&Self::location(name), bytes, Arc::clone(&self.hedge))
                .await;
            let size = handle.size();
            let index = FileIndex::build(
                TABLE,
                handle,
                &manifest_testing::layout(KeyType::Long, None),
                Residency::Full,
                None,
            )
            .await
            .unwrap();
            let bounds = keys.iter().min().zip(keys.iter().max());
            Arc::new(LoadedFile {
                entry: DataFileEntry {
                    path: Self::location(name),
                    bucket,
                    bucketing,
                    records: approx::count(keys.len()),
                    sequence: SequenceNumber(FIRST_SEQUENCE),
                    added_by: SnapshotId(FIRST_SNAPSHOT),
                    size,
                    key_min: bounds.map(|(min, _)| Key::Long(*min)),
                    key_max: bounds.map(|(_, max)| Key::Long(*max)),
                    partition: manifest::Partition::unpartitioned(),
                },
                index,
            })
        }

        /// A file of `keys` with their values.
        async fn keyed(
            &self,
            name: &str,
            keys: &[i64],
            bucketing: Option<Bucketing>,
            bucket: u32,
        ) -> Arc<LoadedFile> {
            self.file(
                name,
                keys,
                &values_of(keys),
                (bucketing, bucket),
                &WriterLayout::contract(),
            )
            .await
        }
    }

    /// Assembles a generation from `files`, none reused.
    async fn assemble(files: Vec<Arc<LoadedFile>>, layout: TableLayout) -> Result<Generation> {
        assemble_with(
            files.into_iter().map(|file| (file, false)).collect(),
            layout,
        )
        .await
    }

    async fn assemble_with(
        members: Vec<(Arc<LoadedFile>, bool)>,
        layout: TableLayout,
    ) -> Result<Generation> {
        Generation::assemble(
            TABLE,
            &placeholder_snapshot(),
            jiff::Timestamp::UNIX_EPOCH,
            &mem_metadata(),
            layout,
            Residency::Full,
            members
                .into_iter()
                .map(|(file, reused)| Member {
                    file,
                    reused,
                    deletes: None,
                    shrank: false,
                })
                .collect(),
        )
        .await
    }

    /// Assembles a generation from `files`, none reused, each with the
    /// rows its deletes remove.
    async fn assemble_deleted(
        files: Vec<(Arc<LoadedFile>, Option<Arc<FileDeletes>>)>,
        layout: TableLayout,
    ) -> Result<Generation> {
        Generation::assemble(
            TABLE,
            &placeholder_snapshot(),
            jiff::Timestamp::UNIX_EPOCH,
            &mem_metadata(),
            layout,
            Residency::Full,
            files
                .into_iter()
                .map(|(file, deletes)| Member {
                    file,
                    reused: false,
                    deletes,
                    shrank: false,
                })
                .collect(),
        )
        .await
    }

    fn paths<'a>(files: impl Iterator<Item = &'a Arc<LoadedFile>>) -> Vec<String> {
        files.map(|file| file.entry.path.clone()).collect()
    }

    /// Every key of `keys` is `Present` with its value, through a fresh
    /// cache, at exactly one GET per distinct value page the plan names and
    /// none besides: the first hit on a page fetches it, every later hit on
    /// it is served warm.
    async fn check_present(generation: &Generation, storage: &Storage, keys: &[i64]) {
        let typed: Vec<Key> = keys.iter().map(|key| Key::Long(*key)).collect();
        let pages: HashSet<(String, u64)> = generation
            .plan(&typed)
            .into_iter()
            .map(|planned| {
                let (file, page) = planned.expect("a present key has a plan");
                let offset = file
                    .index
                    .value_page_offset(0, &page)
                    .expect("the Full tier names the value page of a present key");
                (file.entry.path.clone(), offset)
            })
            .collect();
        let distinct = approx::count(pages.len());
        assert!(distinct >= 1);
        let cache = cache().await;
        let before = storage.stats().gets();
        for (key, raw) in typed.iter().zip(keys) {
            assert_eq!(
                generation.lookup_first(key, &cache).await.unwrap().0,
                testing::one_value(Bytes::from(value_for(*raw))),
                "key {key}"
            );
            assert_eq!(
                storage.stats().gets(),
                before + cache.fetches(),
                "key {key}: every fetch is one GET and nothing else is"
            );
        }
        assert_eq!(cache.fetches(), distinct, "one fetch per value page");
        assert_eq!(storage.stats().gets(), before + distinct);
    }

    #[tokio::test]
    async fn test_candidates_route_by_bucket_under_one_bucketing() {
        let mem = Mem::new();
        let all = keys_from(0, KEYS_PER_FILE * i64::from(FOUR));
        let mut files = Vec::new();
        for bucket in 0..FOUR {
            let keys = keys_in_bucket(&all, bucketing(FOUR), bucket);
            assert!(keys.len() > 1, "bucket {bucket} holds keys");
            files.push(
                mem.keyed(&format!("b{bucket}"), &keys, Some(bucketing(FOUR)), bucket)
                    .await,
            );
        }
        let generation = assemble(files, layout(Some(bucketing(FOUR))))
            .await
            .unwrap();
        assert_eq!(generation.files.len(), approx::length(FOUR));
        assert_eq!(generation.overlapping_buckets().len(), 0);
        for key in &all {
            let bucket = bucketing(FOUR).bucket_of(&Key::Long(*key));
            assert_eq!(
                paths(generation.candidates(&Key::Long(*key))),
                [Mem::location(&format!("b{bucket}"))],
                "key {key} routes to its bucket's file alone"
            );
        }
        check_present(&generation, &mem.store.storage, &all).await;
        let cache = cache().await;
        let before = mem.gets();
        for key in &all {
            let absent = Key::Long(key + 1);
            assert_eq!(
                generation.lookup_first(&absent, &cache).await.unwrap().0,
                Row::Absent
            );
        }
        assert_eq!(mem.gets(), before, "a miss costs nothing");
    }

    #[tokio::test]
    async fn test_candidates_route_under_two_bucketings_and_unbucketed_files_always() {
        let mem = Mem::new();
        let all = keys_from(0, KEYS_PER_FILE * i64::from(EIGHT));
        let (under_four, under_eight) = all.split_at(all.len() / 2);
        let unbucketed_keys = keys_from(all.last().unwrap() + KEY_STEP, KEYS_PER_FILE);
        let mut files = Vec::new();
        for bucket in 0..FOUR {
            let keys = keys_in_bucket(under_four, bucketing(FOUR), bucket);
            files.push(
                mem.keyed(
                    &format!("four{bucket}"),
                    &keys,
                    Some(bucketing(FOUR)),
                    bucket,
                )
                .await,
            );
        }
        for bucket in 0..EIGHT {
            let keys = keys_in_bucket(under_eight, bucketing(EIGHT), bucket);
            files.push(
                mem.keyed(
                    &format!("eight{bucket}"),
                    &keys,
                    Some(bucketing(EIGHT)),
                    bucket,
                )
                .await,
            );
        }
        files.push(
            mem.keyed("flat", &unbucketed_keys, None, manifest::UNBUCKETED)
                .await,
        );
        let generation = assemble(files, layout(Some(bucketing(EIGHT))))
            .await
            .unwrap();

        // The unbucketed file comes first in file order and is a candidate
        // for every key in its range, whatever bucket the key hashes to.
        assert_eq!(generation.files[0].entry.path, Mem::location("flat"));
        for key in &unbucketed_keys {
            assert_eq!(
                paths(generation.candidates(&Key::Long(*key))),
                [Mem::location("flat")]
            );
        }
        // A key written under the four-bucket spec is probed in its bucket
        // under both bucketings when the eight-bucket file's range covers
        // it, and never in another bucket's file under either.
        for key in under_four {
            let key = Key::Long(*key);
            let four = bucketing(FOUR).bucket_of(&key);
            let eight = bucketing(EIGHT).bucket_of(&key);
            let candidates = paths(generation.candidates(&key));
            let eight_file = generation
                .files
                .iter()
                .find(|file| file.entry.path == Mem::location(&format!("eight{eight}")))
                .unwrap();
            let mut expected = vec![Mem::location(&format!("four{four}"))];
            if eight_file.index.may_contain(&key) {
                expected.push(Mem::location(&format!("eight{eight}")));
            }
            assert_eq!(candidates, expected, "key {key}");
        }
        check_present(&generation, &mem.store.storage, &all).await;
        check_present(&generation, &mem.store.storage, &unbucketed_keys).await;
    }

    /// `file` with its manifest entry at data sequence number `sequence`.
    fn at_sequence(file: Arc<LoadedFile>, sequence: i64) -> Arc<LoadedFile> {
        let mut file = Arc::into_inner(file).expect("the file is not shared yet");
        file.entry.sequence = SequenceNumber(sequence);
        Arc::new(file)
    }

    /// Every `files`-th of `keys` starting at `first`: disjoint key sets
    /// whose ranges interleave.
    fn every_nth(keys: &[i64], first: usize, files: usize) -> Vec<i64> {
        keys.iter().copied().skip(first).step_by(files).collect()
    }

    #[tokio::test]
    async fn test_candidates_probe_in_descending_sequence_number_ties_in_file_order() {
        let mem = Mem::new();
        let names = ["a", "b", "c", "d"];
        let sequences = [1, 3, 2, 3];
        let count = names.len();
        let all = keys_from(0, KEYS_PER_FILE * i64::try_from(count).unwrap());
        let mut files = Vec::new();
        for (first, (name, sequence)) in names.into_iter().zip(sequences).enumerate() {
            let keys = every_nth(&all, first, count);
            files.push(at_sequence(
                mem.keyed(name, &keys, None, manifest::UNBUCKETED).await,
                sequence,
            ));
        }
        let generation = assemble(files, layout(None)).await.unwrap();
        assert_eq!(paths(generation.files.iter()), names.map(Mem::location));
        // A key of the oldest file, which every file's range covers.
        let oldest = every_nth(&all, 0, count);
        let key = Key::Long(oldest[oldest.len() / 2]);
        let probe_order = ["b", "d", "c", "a"].map(Mem::location);
        assert_eq!(paths(generation.candidates(&key)), probe_order);
        let probes = generation.probes(&key);
        assert_eq!(paths(probes.iter().map(|(file, _, _)| file)), probe_order);
        let holding: Vec<&str> = probes
            .iter()
            .filter(|(_, page, _)| page.position.is_some())
            .map(|(file, _, _)| file.entry.path.as_str())
            .collect();
        assert_eq!(holding, [Mem::location("a")], "only the holder has a row");
        let (planned, _) = generation.plan(std::slice::from_ref(&key))[0]
            .clone()
            .unwrap();
        assert_eq!(planned.entry.path, Mem::location("a"));
        check_present(&generation, &mem.store.storage, &all).await;
    }

    #[tokio::test]
    async fn test_candidates_merge_bucketings_by_sequence_number() {
        let mem = Mem::new();
        let all = keys_from(0, KEYS_PER_FILE * i64::from(EIGHT) * i64::from(FOUR));
        // Absent from every file, inside every file's range.
        let key = Key::Long(all[all.len() / 2] + 1);
        let (four, eight) = (bucketing(FOUR), bucketing(EIGHT));
        let (in_four, in_eight) = (four.bucket_of(&key), eight.bucket_of(&key));
        let specs = [
            ("four_new", Some(four), in_four, 4),
            ("four_old", Some(four), in_four, 1),
            ("eight", Some(eight), in_eight, 3),
            ("flat", None, manifest::UNBUCKETED, 2),
        ];
        let (files, held) = sequenced_files(&mem, &all, &specs).await;
        let generation = assemble(files, layout(Some(eight))).await.unwrap();
        assert_eq!(
            paths(generation.files.iter()),
            ["flat", "four_old", "four_new", "eight"].map(Mem::location),
            "file order: bucketing, bucket, first key"
        );
        assert_eq!(
            paths(generation.candidates(&key)),
            ["four_new", "eight", "flat", "four_old"].map(Mem::location)
        );
        assert_eq!(
            generation
                .lookup_first(&key, &cache().await)
                .await
                .unwrap()
                .0,
            Row::Absent
        );
        check_present(&generation, &mem.store.storage, &held).await;
    }

    /// One file per spec of (name, bucketing, bucket, data sequence number),
    /// the `n`-th of `specs.len()` holding every such `n`-th of `all` that is
    /// in its bucket, with every key they hold.
    async fn sequenced_files(
        mem: &Mem,
        all: &[i64],
        specs: &[(&str, Option<Bucketing>, u32, i64)],
    ) -> (Vec<Arc<LoadedFile>>, Vec<i64>) {
        let mut files = Vec::new();
        let mut held = Vec::new();
        for (first, &(name, routed, bucket, sequence)) in specs.iter().enumerate() {
            let class = every_nth(all, first, specs.len());
            let keys = match routed {
                Some(routed) => keys_in_bucket(&class, routed, bucket),
                None => class,
            };
            held.extend(&keys);
            files.push(at_sequence(
                mem.keyed(name, &keys, routed, bucket).await,
                sequence,
            ));
        }
        (files, held)
    }

    #[tokio::test]
    async fn test_candidates_merge_bucketings_ties_in_file_order() {
        let mem = Mem::new();
        let all = keys_from(0, KEYS_PER_FILE * i64::from(EIGHT) * i64::from(FOUR));
        // Absent from every file, inside every file's range.
        let key = Key::Long(all[all.len() / 2] + 1);
        let (four, eight) = (bucketing(FOUR), bucketing(EIGHT));
        let (newest, older) = (3, 2);
        let specs = [
            ("x", Some(four), four.bucket_of(&key), newest),
            ("y", Some(eight), eight.bucket_of(&key), newest),
            ("z", None, manifest::UNBUCKETED, older),
        ];
        let (files, held) = sequenced_files(&mem, &all, &specs).await;
        let generation = assemble(files, layout(Some(eight))).await.unwrap();
        assert_eq!(
            paths(generation.files.iter()),
            ["z", "x", "y"].map(Mem::location),
            "file order: bucketing, bucket, first key"
        );
        assert_eq!(
            paths(generation.candidates(&key)),
            ["x", "y", "z"].map(Mem::location),
            "x and y tie on sequence number and come in file order"
        );
        check_present(&generation, &mem.store.storage, &held).await;
    }

    #[tokio::test]
    async fn test_candidates_merge_past_a_bucketing_whose_bucket_is_empty() {
        let mem = Mem::new();
        let all = keys_from(0, KEYS_PER_FILE * i64::from(EIGHT) * i64::from(FOUR));
        let key = Key::Long(all[all.len() / 2] + 1);
        let (four, eight) = (bucketing(FOUR), bucketing(EIGHT));
        // The eight-bucket file sits in another bucket than the key's, so
        // the key's bucket under that bucketing holds no file.
        let elsewhere = (eight.bucket_of(&key) + 1) % EIGHT;
        let in_four = four.bucket_of(&key);
        let specs = [
            ("four_old", Some(four), in_four, 1),
            ("eight", Some(eight), elsewhere, 3),
            ("four_new", Some(four), in_four, 2),
        ];
        let (files, held) = sequenced_files(&mem, &all, &specs).await;
        let generation = assemble(files, layout(Some(eight))).await.unwrap();
        assert_eq!(
            paths(generation.candidates(&key)),
            ["four_new", "four_old"].map(Mem::location)
        );
        assert_eq!(
            generation
                .lookup_first(&key, &cache().await)
                .await
                .unwrap()
                .0,
            Row::Absent
        );
        check_present(&generation, &mem.store.storage, &held).await;
    }

    #[tokio::test]
    async fn test_get_present_null_and_absent_at_the_promised_get_counts() {
        let mem = Mem::new();
        let keys = keys_from(0, KEYS_PER_FILE);
        let null_key = keys[2];
        let values: Vec<Option<Vec<u8>>> = keys
            .iter()
            .map(|key| (*key != null_key).then(|| value_for(*key)))
            .collect();
        let file = mem
            .file(
                "f",
                &keys,
                &values,
                (None, manifest::UNBUCKETED),
                &WriterLayout::contract(),
            )
            .await;
        let generation = assemble(vec![file], layout(None)).await.unwrap();
        assert_eq!(generation.snapshot_id, SnapshotId(FIRST_SNAPSHOT));
        assert_eq!(generation.records, approx::count(keys.len()));
        let cache = cache().await;
        let before = mem.gets();
        assert_eq!(
            generation
                .lookup_first(&Key::Long(keys[1]), &cache)
                .await
                .unwrap()
                .0,
            testing::one_value(Bytes::from(value_for(keys[1])))
        );
        assert_eq!(mem.gets(), before + 1, "a cold hit is one GET");
        assert_eq!(
            generation
                .lookup_first(&Key::Long(keys[0]), &cache)
                .await
                .unwrap()
                .0,
            testing::one_value(Bytes::from(value_for(keys[0])))
        );
        assert_eq!(mem.gets(), before + 1, "a warm hit is none");
        assert_eq!(
            generation
                .lookup_first(&Key::Long(null_key), &cache)
                .await
                .unwrap()
                .0,
            testing::one_null()
        );
        assert_eq!(mem.gets(), before + 1, "a null on the cached page is none");
        for absent in [keys[1] + 1, keys[0] - 1, keys.last().unwrap() + 1] {
            assert_eq!(
                generation
                    .lookup_first(&Key::Long(absent), &cache)
                    .await
                    .unwrap()
                    .0,
                Row::Absent
            );
        }
        assert_eq!(mem.gets(), before + 1, "a miss is none");
    }

    #[tokio::test]
    async fn test_lookup_labels_cold_hit_then_warm_hit_then_miss() {
        let (table, all) = two_bucket_table().await;
        let config = table.config(Residency::Full, None);
        let generation = table.load(&config, None, None).await.unwrap();
        let cache = cache().await;
        let key = Key::Long(all[0]);
        let present = testing::one_value(Bytes::from(value_for(all[0])));
        let before = table.gets();
        assert_eq!(
            generation.lookup_first(&key, &cache).await.unwrap(),
            (present.clone(), LookupResult::ColdHit)
        );
        assert_eq!(table.gets(), before + 1, "a cold hit is one GET");
        assert_eq!(
            generation.lookup_first(&key, &cache).await.unwrap(),
            (present, LookupResult::WarmHit)
        );
        assert_eq!(
            generation
                .lookup_first(&Key::Long(all[0] + 1), &cache)
                .await
                .unwrap(),
            (Row::Absent, LookupResult::Miss)
        );
        assert_eq!(table.gets(), before + 1, "a warm hit and a miss cost none");
    }

    /// Two lookups of `key` through `cache`: the first `ColdHit` at
    /// `hit_gets` GETs, the second `WarmHit` at none.
    async fn assert_cold_then_warm(
        table: &Table,
        generation: &Generation,
        cache: &PageCache,
        key: i64,
        hit_gets: u64,
    ) {
        let present = testing::one_value(Bytes::from(value_for(key)));
        let before = table.gets();
        assert_eq!(
            generation
                .lookup_first(&Key::Long(key), cache)
                .await
                .unwrap(),
            (present.clone(), LookupResult::ColdHit)
        );
        assert_eq!(table.gets(), before + hit_gets, "a cold hit");
        assert_eq!(
            generation
                .lookup_first(&Key::Long(key), cache)
                .await
                .unwrap(),
            (present, LookupResult::WarmHit)
        );
        assert_eq!(table.gets(), before + hit_gets, "a warm hit costs none");
    }

    #[tokio::test]
    async fn test_lookup_labels_follow_the_value_page_in_disk_and_bloom_residency() {
        let (table, all) = two_bucket_table().await;
        let scratch = tempfile::tempdir().unwrap();
        let local = local_dir(scratch.path(), table.storage.stats(), None);
        let disk = table
            .load(&table.config(Residency::Disk, None), Some(&local), None)
            .await
            .unwrap();
        let disk_cache = cache().await;
        assert_cold_then_warm(&table, &disk, &disk_cache, all[0], 1).await;
        let before = table.gets();
        assert_eq!(
            disk.lookup_first(&Key::Long(all[0] + 1), &disk_cache)
                .await
                .unwrap(),
            (Row::Absent, LookupResult::Miss)
        );
        assert_eq!(
            table.gets(),
            before,
            "a Disk miss reads the chunk file, no GET"
        );

        let mut table = Table::bucketed(1);
        let all = keys_from(0, BLOOM_KEYS);
        let keys = long_keys(&all);
        table
            .add_file(FileSpec {
                name: "f",
                keys: &keys,
                values: values_of(&all),
                partition: Partition::Bucket(0),
                bounds: Bounds::Exact,
                layout: WriterLayout {
                    bloom: Bloom::Fpp(LOOSE_FPP),
                    ..WriterLayout::contract()
                },
            })
            .await;
        table
            .commit(FIRST_SNAPSHOT, None, FIRST_SEQUENCE, FIRST_VERSION)
            .await;
        let bloom = table
            .load(&table.config(Residency::Bloom, None), None, None)
            .await
            .unwrap();
        let bloom_cache = cache().await;
        assert_cold_then_warm(&table, &bloom, &bloom_cache, all[0], BLOOM_HIT_GETS).await;
        let absent: Vec<Key> = all[..all.len() - 1]
            .iter()
            .map(|key| Key::Long(key + 1))
            .collect();
        let plans = bloom.plan(&absent);
        let passing = absent
            .iter()
            .zip(&plans)
            .find(|(_, plan)| plan.is_some())
            .map(|(key, _)| key.clone())
            .expect("an undersized filter passes some absent key in range");
        let ruled_out = absent
            .iter()
            .zip(&plans)
            .find(|(_, plan)| plan.is_none())
            .map(|(key, _)| key.clone())
            .expect("a filter rules out some absent key in range");
        // A fresh cache, so the key page the hit above fetched is not held.
        let miss_cache = cache().await;
        let before = table.gets();
        assert_eq!(
            bloom.lookup_first(&passing, &miss_cache).await.unwrap(),
            (Row::Absent, LookupResult::Miss)
        );
        assert_eq!(
            table.gets(),
            before + 1,
            "a filter-positive miss costs the key page and is still a miss"
        );
        assert_eq!(
            bloom.lookup_first(&ruled_out, &miss_cache).await.unwrap(),
            (Row::Absent, LookupResult::Miss)
        );
        assert_eq!(
            table.gets(),
            before + 1,
            "a filter-negative miss costs none"
        );
    }

    #[tokio::test]
    async fn test_plan_names_the_file_and_page_get_uses_and_none_for_an_absent_key() {
        let mem = Mem::new();
        let keys = keys_from(0, KEYS_PER_FILE);
        let file = mem.keyed("f", &keys, None, manifest::UNBUCKETED).await;
        let generation = assemble(vec![Arc::clone(&file)], layout(None))
            .await
            .unwrap();
        let present = Key::Long(keys[3]);
        let inside = Key::Long(keys[3] + 1);
        let outside = Key::Long(keys.last().unwrap() + KEY_STEP);
        let plan = generation.plan(&[present.clone(), inside, outside]);
        assert_eq!(plan.len(), 3);
        let (planned, page) = plan[0].as_ref().unwrap();
        assert!(Arc::ptr_eq(planned, &file));
        assert_eq!((page.row_group, page.key_page), (0, 0));
        assert!(planned.index.value_page_offset(0, page).is_some());
        let (inside_file, inside_page) = plan[1].as_ref().unwrap();
        assert_eq!(inside_file.index.value_page_offset(0, inside_page), None);
        assert!(plan[2].is_none());
        assert_cold_hit_reads_the_planned_page(&generation, &mem, planned, page, keys[3]).await;
    }

    /// The compressed size of the value page of `file`'s first row group
    /// at `offset`.
    fn value_page_size(file: &LoadedFile, offset: u64) -> u64 {
        file.index.metadata().offset_index().unwrap()[0][file.index.value_leaf(0).unwrap()]
            .page_locations()
            .iter()
            .find(|location| u64::try_from(location.offset).unwrap() == offset)
            .map(|location| u64::try_from(location.compressed_page_size).unwrap())
            .unwrap()
    }

    /// A cold `lookup` of `key` costs one GET of exactly the page `page` of
    /// `planned` names.
    async fn assert_cold_hit_reads_the_planned_page(
        generation: &Generation,
        mem: &Mem,
        planned: &LoadedFile,
        page: &PageRef,
        key: i64,
    ) {
        let page_size = value_page_size(planned, planned.index.value_page_offset(0, page).unwrap());
        let cache = cache().await;
        let (gets, bytes) = (mem.gets(), mem.store.storage.stats().bytes());
        assert_eq!(
            generation
                .lookup_first(&Key::Long(key), &cache)
                .await
                .unwrap()
                .0,
            testing::one_value(Bytes::from(value_for(key)))
        );
        assert_eq!(mem.gets(), gets + 1);
        assert_eq!(mem.store.storage.stats().bytes(), bytes + page_size);
    }

    #[tokio::test]
    async fn test_plan_names_the_later_overlapping_file_that_holds_the_key() {
        let mem = Mem::new();
        let first = keys_from(0, KEYS_PER_FILE);
        let interleaved: Vec<i64> = first.iter().map(|key| key + 1).collect();
        let a = mem.keyed("a", &first, Some(bucketing(1)), 0).await;
        let b = mem.keyed("b", &interleaved, Some(bucketing(1)), 0).await;
        let generation = assemble(
            vec![Arc::clone(&a), Arc::clone(&b)],
            layout(Some(bucketing(1))),
        )
        .await
        .unwrap();
        // `a` is probed first and its fences cover the key, but `b` holds it.
        let held_by_b = interleaved[interleaved.len() / 2];
        assert_eq!(
            paths(generation.candidates(&Key::Long(held_by_b))),
            [Mem::location("a"), Mem::location("b")]
        );
        let plan = generation.plan(&[Key::Long(held_by_b), Key::Long(held_by_b + 1)]);
        let (planned, page) = plan[0].as_ref().unwrap();
        assert!(
            Arc::ptr_eq(planned, &b),
            "the plan names the file holding the key"
        );
        assert!(planned.index.value_page_offset(0, page).is_some());
        assert_cold_hit_reads_the_planned_page(&generation, &mem, planned, page, held_by_b).await;
        // A key inside both fences and in neither file: the first probe,
        // with no value page.
        let (first_probe, page) = plan[1].as_ref().unwrap();
        assert!(Arc::ptr_eq(first_probe, &a));
        assert_eq!(first_probe.index.value_page_offset(0, page), None);
    }

    #[tokio::test]
    async fn test_files_are_ordered_by_bucketing_bucket_first_key_and_path() {
        let mem = Mem::new();
        // Five disjoint key runs, so no two files can share a key and the
        // order alone is under test.
        let run = |at: i64| keys_from(at * KEY_STEP * KEYS_PER_FILE, KEYS_PER_FILE);
        let four_one_high = mem.keyed("a", &run(1), Some(bucketing(FOUR)), 1).await;
        let four_one_low = mem.keyed("b", &run(0), Some(bucketing(FOUR)), 1).await;
        let four_zero = mem.keyed("c", &run(2), Some(bucketing(FOUR)), 0).await;
        let unbucketed = mem.keyed("d", &run(3), None, manifest::UNBUCKETED).await;
        let eight_zero = mem.keyed("e", &run(4), Some(bucketing(EIGHT)), 0).await;
        let expected = paths(
            [
                &unbucketed,
                &four_zero,
                &four_one_low,
                &four_one_high,
                &eight_zero,
            ]
            .into_iter(),
        );
        let forward = vec![
            four_one_high,
            four_one_low,
            four_zero,
            unbucketed,
            eight_zero,
        ];
        let mut backward = forward.clone();
        backward.reverse();
        for order in [forward, backward] {
            let generation = assemble(order, layout(Some(bucketing(FOUR))))
                .await
                .unwrap();
            assert_eq!(paths(generation.files.iter()), expected);
        }
    }

    /// Files by name with the given key ranges, all under `bucketing` in
    /// their given buckets, for the grouping tests; the files are not
    /// probed, so their keys are their bounds alone.
    async fn ranged(
        mem: &Mem,
        files: &[(&str, Option<Bucketing>, u32, i64, i64)],
    ) -> Vec<Arc<LoadedFile>> {
        let mut loaded = Vec::new();
        for (name, bucketing, bucket, first, last) in files {
            loaded.push(mem.keyed(name, &[*first, *last], *bucketing, *bucket).await);
        }
        loaded
    }

    /// The overlap groups of `files`, in file order as `assemble` puts
    /// them, as lists of paths.
    fn groups_of(files: &[Arc<LoadedFile>]) -> Vec<Vec<String>> {
        let mut files = files.to_vec();
        files.sort_by(|a, b| file_order(a, b));
        let files = &files;
        let spans: Vec<Span<'_>> = files.iter().map(|file| Span::of(file)).collect();
        overlap_groups(&spans, &routes(&spans))
            .into_iter()
            .map(|group| {
                group
                    .into_iter()
                    .map(|at| files[at].entry.path.clone())
                    .collect()
            })
            .collect()
    }

    #[tokio::test]
    async fn test_overlap_groups_disjoint_intersecting_transitive_and_across_buckets() {
        let mem = Mem::new();
        let one = Some(bucketing(1));
        let disjoint = ranged(&mem, &[("a", one, 0, 0, 10), ("b", one, 0, 20, 30)]).await;
        assert!(groups_of(&disjoint).is_empty());

        let pair = ranged(&mem, &[("a", one, 0, 0, 10), ("b", one, 0, 5, 15)]).await;
        assert_eq!(groups_of(&pair), [[Mem::location("a"), Mem::location("b")]]);

        let chain = ranged(
            &mem,
            &[
                ("a", one, 0, 0, 10),
                ("b", one, 0, 8, 20),
                ("c", one, 0, 18, 30),
                ("d", one, 0, 40, 50),
            ],
        )
        .await;
        assert_eq!(
            groups_of(&chain),
            [[Mem::location("a"), Mem::location("b"), Mem::location("c")]],
            "a and c meet through b; d stands alone"
        );

        let four = Some(bucketing(FOUR));
        let other_buckets = ranged(&mem, &[("a", four, 0, 0, 10), ("b", four, 1, 5, 15)]).await;
        assert!(
            groups_of(&other_buckets).is_empty(),
            "one bucketing, different buckets, never a group"
        );

        // Across bucketings a key's buckets are congruent modulo the gcd of
        // the counts (here four): bucket 0 of four and bucket 4 of eight can
        // share a key, bucket 0 of four and bucket 5 of eight cannot.
        let eight = Some(bucketing(EIGHT));
        let congruent = ranged(&mem, &[("a", four, 0, 0, 10), ("b", eight, 4, 5, 15)]).await;
        assert_eq!(
            groups_of(&congruent),
            [[Mem::location("a"), Mem::location("b")]],
            "different bucketings, congruent buckets, intersecting ranges group"
        );
        let incongruent = ranged(&mem, &[("a", four, 0, 0, 10), ("b", eight, 5, 5, 15)]).await;
        assert!(
            groups_of(&incongruent).is_empty(),
            "buckets no key falls in at once never group"
        );

        let flat = ranged(
            &mem,
            &[
                ("a", four, 2, 0, 10),
                ("b", None, manifest::UNBUCKETED, 5, 15),
                ("c", four, 3, 40, 50),
            ],
        )
        .await;
        assert_eq!(
            groups_of(&flat),
            [[Mem::location("b"), Mem::location("a")]],
            "an unbucketed file groups with any intersecting file"
        );
    }

    #[tokio::test]
    async fn test_assemble_refuses_a_duplicate_across_overlapping_files_naming_the_later_file() {
        let mem = Mem::new();
        let first = keys_from(0, KEYS_PER_FILE);
        let duplicate = first[first.len() / 2];
        let overlapping = [duplicate, first.last().unwrap() + KEY_STEP];
        let a = mem.keyed("a", &first, Some(bucketing(1)), 0).await;
        let b = mem.keyed("b", &overlapping, Some(bucketing(1)), 0).await;
        assert_eq!(
            refusal_of(
                assemble(
                    vec![Arc::clone(&b), Arc::clone(&a)],
                    layout(Some(bucketing(1)))
                )
                .await
            ),
            RefusalReason::DuplicateKeys {
                path: Mem::location("b"),
                key: Key::Long(duplicate),
            }
        );
    }

    /// Rows per page of the files the merge tests write: small, so a row
    /// group holds several key pages and a cursor crosses page boundaries.
    const SPLIT_PAGE_ROWS: usize = 4;
    /// Rows per row group of those files: four pages of `SPLIT_PAGE_ROWS`.
    const SPLIT_GROUP_ROWS: usize = 16;
    /// Keys in a merge test's file: two full row groups of
    /// `SPLIT_GROUP_ROWS` and a partial third, so a row of the third counts
    /// the rows of two earlier groups.
    const SPLIT_KEYS: i64 = 40;

    /// A file of `keys` in pages of `SPLIT_PAGE_ROWS` and row groups of
    /// `SPLIT_GROUP_ROWS`, as the merge member at position `at`.
    async fn split_member(mem: &Mem, name: &str, keys: &[i64], at: usize) -> GroupMember {
        let layout = WriterLayout {
            page_rows: SPLIT_PAGE_ROWS,
            row_group_rows: Some(SPLIT_GROUP_ROWS),
            ..WriterLayout::contract()
        };
        let file = mem
            .file(
                name,
                keys,
                &values_of(keys),
                (None, manifest::UNBUCKETED),
                &layout,
            )
            .await;
        let chunks = file.index.key_chunks_for_verification().await.unwrap();
        GroupMember { at, file, chunks }
    }

    /// Every live key under a cursor over `member`, with its position.
    fn walk(member: &GroupMember, deleted: Option<&DeletedRows>) -> Vec<(Key, RowPosition)> {
        let mut seen = Vec::new();
        let Some(mut cursor) = KeyCursor::open(member, deleted).unwrap() else {
            return seen;
        };
        loop {
            seen.push((cursor.head(), cursor.head_position()));
            if !cursor.advance().unwrap() {
                return seen;
            }
        }
    }

    #[tokio::test]
    async fn test_key_cursor_counts_positions_from_the_files_first_row_and_skips_deleted_rows() {
        let mem = Mem::new();
        let keys = keys_from(0, SPLIT_KEYS);
        let member = split_member(&mem, "a", &keys, 0).await;
        assert_eq!(
            member.file.index.metadata().num_row_groups(),
            3,
            "two full row groups and a partial third"
        );
        let positions = |range: std::ops::Range<usize>| -> Vec<(Key, RowPosition)> {
            range
                .map(|row| (Key::Long(keys[row]), RowPosition(approx::count(row))))
                .collect()
        };
        assert_eq!(walk(&member, None), positions(0..keys.len()));
        // The first row, a row inside the second row group, and the last.
        let gone =
            [0, SPLIT_GROUP_ROWS + 1, keys.len() - 1].map(|row| RowPosition(approx::count(row)));
        let deleted = move |at: usize, position: RowPosition| at == 0 && gone.contains(&position);
        let live: Vec<(Key, RowPosition)> = positions(0..keys.len())
            .into_iter()
            .filter(|(_, position)| !gone.contains(position))
            .collect();
        assert_eq!(walk(&member, Some(&deleted)), live);
        let everything = |_: usize, _: RowPosition| true;
        assert_eq!(walk(&member, Some(&everything)), []);
    }

    #[tokio::test]
    async fn test_refuse_duplicates_passes_a_key_whose_other_copy_is_deleted() {
        let mem = Mem::new();
        let older = keys_from(0, SPLIT_KEYS);
        // A row of the third row group, so its position spans two others.
        let row = 2 * SPLIT_GROUP_ROWS + 1;
        let shared = older[row];
        let newer = [shared - 1, shared, older.last().unwrap() + KEY_STEP];
        let members = [
            split_member(&mem, "older", &older, 0).await,
            split_member(&mem, "newer", &newer, 1).await,
        ];
        let refusal = RefusalReason::DuplicateKeys {
            path: Mem::location("newer"),
            key: Key::Long(shared),
        };
        assert_eq!(
            refusal_of(refuse_duplicates(TABLE, &members, None)),
            refusal
        );
        let older_copy = RowPosition(approx::count(row));
        let deleted = move |at: usize, position: RowPosition| at == 0 && position == older_copy;
        refuse_duplicates(TABLE, &members, Some(&deleted)).unwrap();
        // The row before it deleted instead leaves both copies live.
        let neighbor = RowPosition(older_copy.0 - 1);
        let wrong_row = move |at: usize, position: RowPosition| at == 0 && position == neighbor;
        assert_eq!(
            refusal_of(refuse_duplicates(TABLE, &members, Some(&wrong_row))),
            refusal
        );
        // The newer copy deleted serves the older one.
        let newer_row = newer.iter().position(|&key| key == shared).unwrap();
        let newer_copy = RowPosition(approx::count(newer_row));
        let deleted_newer =
            move |at: usize, position: RowPosition| at == 1 && position == newer_copy;
        refuse_duplicates(TABLE, &members, Some(&deleted_newer)).unwrap();
    }

    #[tokio::test]
    async fn test_assemble_serves_overlapping_files_without_duplicates_and_counts_them() {
        let mem = Mem::new();
        let first = keys_from(0, KEYS_PER_FILE);
        let interleaved: Vec<i64> = first.iter().map(|key| key + 1).collect();
        let a = mem.keyed("a", &first, Some(bucketing(1)), 0).await;
        let b = mem.keyed("b", &interleaved, Some(bucketing(1)), 0).await;
        let generation = assemble(vec![a, b], layout(Some(bucketing(1))))
            .await
            .unwrap();
        assert_eq!(generation.overlapping_buckets(), &BTreeMap::from([(0, 2)]));
        assert_eq!(generation.overlap_groups_verified(), 1);
        check_present(&generation, &mem.store.storage, &first).await;
        check_present(&generation, &mem.store.storage, &interleaved).await;
        let cache = cache().await;
        assert_eq!(
            generation
                .lookup_first(&Key::Long(first[1] + 2), &cache)
                .await
                .unwrap()
                .0,
            Row::Absent
        );
    }

    /// A file of one bucket, (first key, last key, the snapshot that
    /// added it), as the sweep tests describe one.
    type Described = (i64, i64, i64);

    /// What `probe_widths` finds over `files`, one group of one bucket,
    /// under `edges`, in file order.
    fn widths_of(files: &[Described], edges: Edges) -> ProbeWidths {
        let keys: Vec<(Key, Key)> = files
            .iter()
            .map(|&(first, last, _)| (Key::Long(first), Key::Long(last)))
            .collect();
        let spans: Vec<Span<'_>> = keys
            .iter()
            .zip(files)
            .map(|((first, last), &(_, _, added_by))| Span {
                bucketing: Some(bucketing(1)),
                bucket: 0,
                first,
                last,
                added_by: SnapshotId(added_by),
            })
            .collect();
        let groups = overlap_groups(&spans, &routes(&spans));
        probe_widths(&spans, &groups, edges)
    }

    /// Per bucket zero, `files` files.
    fn in_bucket_zero(files: usize) -> BTreeMap<u32, usize> {
        BTreeMap::from([(0, files)])
    }

    /// Files one snapshot added that meet only at edge keys, as a
    /// compaction rolling by size writes them, share those keys on an
    /// ordered table, with the most files one key spans, a file holding
    /// the key alone among them; on a key-value table they overlap.
    #[test]
    fn test_probe_widths_files_one_snapshot_added_share_edge_keys() {
        let (s, rolled, held_alone) = (FIRST_SNAPSHOT, 2, 3);
        for (files, spanned) in [
            (vec![(0, 5, s), (5, 9, s), (9, 12, s)], rolled),
            (vec![(0, 5, s), (5, 5, s), (5, 9, s)], held_alone),
        ] {
            let shared = widths_of(&files, Edges::Shared);
            assert_eq!(shared.overlapping, BTreeMap::new(), "{files:?}");
            assert_eq!(shared.shared_edges, in_bucket_zero(spanned), "{files:?}");
            assert_eq!(shared.most_probed(), Some(spanned));
            let overlap = widths_of(&files, Edges::Overlap);
            assert_eq!(overlap.overlapping, in_bucket_zero(spanned), "{files:?}");
            assert!(overlap.shared_edges.is_empty());
        }
    }

    /// Any other intersection is overlap on an ordered table too: a file of
    /// another snapshot touching an edge key alone, two files that end at
    /// one key, two that start at one, and a one-key file inside another's
    /// range. Where a bucket's edge key is shared and another of its keys
    /// overlaps, both are recorded.
    #[test]
    fn test_probe_widths_any_other_intersection_is_overlap() {
        let (s, t) = (FIRST_SNAPSHOT, SECOND_SNAPSHOT);
        for files in [
            vec![(0, 5, s), (5, 9, t)],
            vec![(0, 5, s), (3, 5, s)],
            vec![(5, 7, s), (5, 9, s)],
            vec![(0, 9, s), (5, 5, s)],
        ] {
            let widths = widths_of(&files, Edges::Shared);
            assert_eq!(widths.overlapping, in_bucket_zero(2), "{files:?}");
            assert!(widths.shared_edges.is_empty(), "{files:?}");
        }
        let both = widths_of(&[(0, 5, s), (5, 9, s), (7, 8, t)], Edges::Shared);
        assert_eq!(both.shared_edges, in_bucket_zero(2));
        assert_eq!(both.overlapping, in_bucket_zero(2));
        let disjoint = widths_of(&[(0, 4, s), (5, 9, t)], Edges::Shared);
        assert_eq!(disjoint, ProbeWidths::default());
        assert_eq!(disjoint.most_probed(), None);
    }

    /// Files of an appended bucket, each over the bucket's keys.
    const APPENDS: usize = 2_000;

    /// The most steps the overlap sweep of an appended bucket may take per
    /// file: a constant few, where counting the open files at each file
    /// takes about `APPENDS` squared halved.
    const SWEPT_PER_FILE: u64 = 8;

    /// The overlap sweep of a bucket of appends, every one over the
    /// bucket's keys and each added by its own snapshot, finds every file
    /// overlapping every other, in steps linear in the files.
    #[test]
    fn test_probe_widths_of_an_appended_bucket_is_linear() {
        let files: Vec<Described> = (0..APPENDS)
            .map(|at| {
                (
                    0,
                    KEYS_PER_FILE,
                    FIRST_SNAPSHOT + i64::try_from(at).unwrap(),
                )
            })
            .collect();
        for edges in [Edges::Overlap, Edges::Shared] {
            take_swept();
            let widths = widths_of(&files, edges);
            let swept = take_swept();
            assert_eq!(widths.overlapping, in_bucket_zero(APPENDS), "{edges:?}");
            assert!(widths.shared_edges.is_empty(), "{edges:?}");
            assert!(
                swept > 0 && swept <= SWEPT_PER_FILE * approx::count(APPENDS),
                "{edges:?}: {swept} steps over {APPENDS} files"
            );
        }
    }

    /// What `probe_widths` finds, by a scan of every open member at every
    /// step: the sweep's definition, against which the counted sweep is
    /// checked.
    fn widths_by_scan(spans: &[Span<'_>], groups: &[Vec<usize>], edges: Edges) -> ProbeWidths {
        let mut widths = ProbeWidths::default();
        for group in groups {
            let mut members = group.clone();
            members.sort_by(|&a, &b| spans[a].first.cmp(spans[b].first));
            let mut open: Vec<usize> = Vec::new();
            for at in members {
                let step = spans[at];
                open.retain(|&member| spans[member].last >= step.first);
                open.push(at);
                let widest = match edges {
                    Edges::Overlap => &mut widths.overlapping,
                    Edges::Shared => {
                        let reached: Vec<Span<'_>> = open
                            .iter()
                            .map(|&member| spans[member])
                            .filter(|span| buckets_may_share_a_key(step.place(), span.place()))
                            .collect();
                        if reached.len() <= FILES_PROBED_ALONE {
                            continue;
                        }
                        let one_snapshot = reached
                            .iter()
                            .all(|span| span.added_by == reached[0].added_by);
                        let inside = reached
                            .iter()
                            .filter(|span| span.first < step.first && span.last > step.first)
                            .count();
                        let ending = reached
                            .iter()
                            .filter(|span| span.first < step.first && span.last == step.first)
                            .count();
                        let starting = reached
                            .iter()
                            .filter(|span| span.first == step.first && span.last > step.first)
                            .count();
                        if one_snapshot
                            && inside == 0
                            && ending <= ONE_SIDE_OF_AN_EDGE
                            && starting <= ONE_SIDE_OF_AN_EDGE
                        {
                            &mut widths.shared_edges
                        } else {
                            &mut widths.overlapping
                        }
                    }
                };
                let mut together: Vec<u32> = Vec::new();
                let mut others: BTreeMap<(Bucketing, u32), usize> = BTreeMap::new();
                for &member in &open {
                    let span = spans[member];
                    match span.bucketing {
                        None => together.push(span.bucket),
                        Some(_) if span.bucketing == step.bucketing => {
                            if span.bucket == step.bucket {
                                together.push(span.bucket);
                            }
                        }
                        Some(bucketing) => {
                            if buckets_may_share_a_key(step.place(), span.place()) {
                                *others.entry((bucketing, span.bucket)).or_insert(0) += 1;
                            }
                        }
                    }
                }
                let mut largest: BTreeMap<Bucketing, usize> = BTreeMap::new();
                for (&(bucketing, _), &files) in &others {
                    let entry = largest.entry(bucketing).or_insert(0);
                    *entry = (*entry).max(files);
                }
                let total = together.len() + largest.values().sum::<usize>();
                let mut record = |bucket: u32, probed: usize| {
                    let entry = widest.entry(bucket).or_insert(0);
                    *entry = (*entry).max(probed);
                };
                for bucket in together {
                    record(bucket, total);
                }
                for (&(bucketing, bucket), &files) in &others {
                    record(bucket, total - largest[&bucketing] + files);
                }
            }
        }
        widths
    }

    /// Cases of the drawn sweep test, the fewest and the most files per
    /// case beyond the fewest, and the keys their ranges fall in, few, so
    /// ranges share edge keys often.
    const DRAWN_SWEEPS: u64 = 300;
    const FEWEST_SWEEP_FILES: u64 = 2;
    const DRAWN_SWEEP_FILES: u64 = 12;
    const DRAWN_SWEEP_KEYS: u64 = 16;

    /// A multiplier and an increment of a linear congruential sequence,
    /// which draws the sweep test's files without a seed.
    const DRAW_MIX: u64 = 6_364_136_223_846_793_005;
    const DRAW_STEP: u64 = 1_442_695_040_888_963_407;

    /// A drawn file of the sweep test: where it is filed, its first and
    /// last key, and the snapshot that added it.
    type DrawnSpan = ((Option<Bucketing>, u32), Key, Key, SnapshotId);

    /// The counted sweep finds what a scan of every open member at every
    /// step finds, under both edge rules, over drawn files of two
    /// snapshots in unbucketed files and buckets of two bucketings: shared
    /// edges, one-key files, and overlap.
    #[test]
    fn test_probe_widths_are_those_of_a_scan_of_the_open_files() {
        let mut state = 0_u64;
        let mut draw = |below: u64| {
            state = state.wrapping_mul(DRAW_MIX).wrapping_add(DRAW_STEP);
            (state >> 33) % below
        };
        let places = [
            (None, UNBUCKETED),
            (Some(bucketing(TWO)), 0),
            (Some(bucketing(TWO)), 1),
            (Some(bucketing(FOUR)), 0),
            (Some(bucketing(FOUR)), 1),
            (Some(bucketing(FOUR)), 2),
        ];
        let snapshots = [FIRST_SNAPSHOT, SECOND_SNAPSHOT];
        let (mut shared, mut overlapping) = (0, 0);
        for case in 0..DRAWN_SWEEPS {
            let files: Vec<DrawnSpan> = (0..FEWEST_SWEEP_FILES + draw(DRAWN_SWEEP_FILES))
                .map(|_| {
                    let place = places[usize::try_from(draw(approx::count(places.len()))).unwrap()];
                    let first = draw(DRAWN_SWEEP_KEYS);
                    let last = first + draw(DRAWN_SWEEP_KEYS - first).min(draw(DRAWN_SWEEP_KEYS));
                    let added_by =
                        snapshots[usize::try_from(draw(approx::count(snapshots.len()))).unwrap()];
                    (
                        place,
                        Key::Long(i64::try_from(first).unwrap()),
                        Key::Long(i64::try_from(last).unwrap()),
                        SnapshotId(added_by),
                    )
                })
                .collect();
            let mut spans: Vec<Span<'_>> = files
                .iter()
                .map(|((bucketing, bucket), first, last, added_by)| Span {
                    bucketing: *bucketing,
                    bucket: *bucket,
                    first,
                    last,
                    added_by: *added_by,
                })
                .collect();
            spans.sort_by(|a, b| {
                (a.bucketing, a.bucket, a.first).cmp(&(b.bucketing, b.bucket, b.first))
            });
            let groups = overlap_groups(&spans, &routes(&spans));
            for edges in [Edges::Overlap, Edges::Shared] {
                let found = probe_widths(&spans, &groups, edges);
                assert_eq!(
                    found,
                    widths_by_scan(&spans, &groups, edges),
                    "case {case}, {edges:?}"
                );
                if edges == Edges::Shared {
                    shared += found.shared_edges.len();
                    overlapping += found.overlapping.len();
                }
            }
        }
        assert!(
            shared > 0 && overlapping > 0,
            "{shared} shared, {overlapping} overlapping"
        );
    }

    /// An unbucketed file and one file per bucket of a bucketing, all
    /// starting at one key and covering one range: a key reaches the
    /// unbucketed file and its own bucket's, two files, whichever of them
    /// the sweep reaches first.
    #[test]
    fn test_probe_widths_mixed_unbucketed_and_bucketed_counts_one_bucket() {
        let (first, last) = (Key::Long(0), Key::Long(KEYS_PER_FILE));
        let unbucketed = Span {
            bucketing: None,
            bucket: UNBUCKETED,
            first: &first,
            last: &last,
            added_by: SnapshotId(FIRST_SNAPSHOT),
        };
        let bucketed: Vec<Span<'_>> = (0..FOUR)
            .map(|bucket| Span {
                bucketing: Some(bucketing(FOUR)),
                bucket,
                first: &first,
                last: &last,
                added_by: SnapshotId(FIRST_SNAPSHOT),
            })
            .collect();
        let unbucketed_first: Vec<Span<'_>> = std::iter::once(unbucketed)
            .chain(bucketed.clone())
            .collect();
        let unbucketed_last: Vec<Span<'_>> = bucketed
            .into_iter()
            .chain(std::iter::once(unbucketed))
            .collect();
        let one_key_reaches = 2;
        let expected: BTreeMap<u32, usize> =
            (0..FOUR).map(|bucket| (bucket, one_key_reaches)).collect();
        for spans in [unbucketed_first, unbucketed_last] {
            let group: Vec<usize> = (0..spans.len()).collect();
            for edges in [Edges::Overlap, Edges::Shared] {
                let widths = probe_widths(&spans, std::slice::from_ref(&group), edges);
                assert_eq!(widths.overlapping, expected, "{edges:?}");
                assert!(widths.shared_edges.is_empty(), "{edges:?}");
            }
        }
    }

    /// On a generation of an unbucketed file over the whole key space and
    /// one bucketed file per bucket, every bucket starting at or before the
    /// unbucketed file, the recorded files probed are the most candidates
    /// any key has, not the group's size.
    #[tokio::test]
    async fn test_overlapping_buckets_mixed_unbucketed_and_bucketed_is_the_candidates() {
        let mem = Mem::new();
        let bucketed_keys = keys_from(0, MIXED_BUCKETED_KEYS);
        let mut files = Vec::new();
        for bucket in 0..FOUR {
            let keys = keys_in_bucket(&bucketed_keys, bucketing(FOUR), bucket);
            files.push(
                mem.keyed(&format!("b{bucket}"), &keys, Some(bucketing(FOUR)), bucket)
                    .await,
            );
        }
        let unbucketed_keys = keys_from(1 + MIXED_UNBUCKETED_FROM * KEY_STEP, KEYS_PER_FILE);
        assert!(
            files
                .iter()
                .all(|file| *file.index.key_range().0 <= Key::Long(unbucketed_keys[0])),
            "every bucket starts at or before the unbucketed file"
        );
        files.push(mem.keyed("u", &unbucketed_keys, None, UNBUCKETED).await);
        let generation = assemble(files, layout(Some(bucketing(FOUR))))
            .await
            .unwrap();
        let most = (0..=*bucketed_keys.last().unwrap())
            .map(|key| generation.candidates(&Key::Long(key)).count())
            .max()
            .unwrap();
        let one_key_reaches = 2;
        assert_eq!(
            most, one_key_reaches,
            "a key reaches u and its own bucket's file"
        );
        assert_eq!(
            generation.overlapping_buckets(),
            &(0..FOUR)
                .map(|bucket| (bucket, most))
                .collect::<BTreeMap<_, _>>()
        );
        assert_eq!(generation.lookup_cost.files_probed, most);
    }

    #[tokio::test]
    async fn test_assemble_verifies_only_groups_that_gained_a_file() {
        let mem = Mem::new();
        let first = keys_from(0, KEYS_PER_FILE);
        let interleaved: Vec<i64> = first.iter().map(|key| key + 1).collect();
        let beyond = keys_from(first.last().unwrap() + KEY_STEP, KEYS_PER_FILE);
        let a = mem.keyed("a", &first, Some(bucketing(1)), 0).await;
        let b = mem.keyed("b", &interleaved, Some(bucketing(1)), 0).await;
        let c = mem.keyed("c", &beyond, Some(bucketing(1)), 0).await;
        let one = layout(Some(bucketing(1)));

        let fresh = assemble_with(
            vec![(Arc::clone(&a), false), (Arc::clone(&b), false)],
            one.clone(),
        )
        .await
        .unwrap();
        assert_eq!(fresh.overlap_groups_verified(), 1);

        let disjoint_append = assemble_with(
            vec![
                (Arc::clone(&a), true),
                (Arc::clone(&b), true),
                (Arc::clone(&c), false),
            ],
            one.clone(),
        )
        .await
        .unwrap();
        assert_eq!(disjoint_append.overlap_groups_verified(), 0);
        assert_eq!(
            disjoint_append.overlapping_buckets(),
            &BTreeMap::from([(0, 2)])
        );

        let d = mem
            .keyed("d", &[first[0] + 2, beyond[0] + 2], Some(bucketing(1)), 0)
            .await;
        let joined = assemble_with(
            vec![
                (Arc::clone(&a), true),
                (Arc::clone(&b), true),
                (Arc::clone(&c), true),
                (d, false),
            ],
            one,
        )
        .await
        .unwrap();
        assert_eq!(joined.overlap_groups_verified(), 1, "d bridges both groups");
        assert_eq!(
            joined.overlapping_buckets(),
            &BTreeMap::from([(0, 3)]),
            "no key is in all four ranges: a lookup probes a, b, and d, or c and d"
        );
        assert_eq!(
            joined.candidates(&Key::Long(first[0] + 2)).count(),
            3,
            "d's first key probes three files"
        );
    }

    /// The GETs a load makes before any data file: the hint, the metadata,
    /// the manifest list, and one manifest.
    const METADATA_GETS: u64 = 4;
    /// One more GET per further manifest a snapshot lists, one per spec
    /// its files were written under.
    const MANIFEST_GET: u64 = 1;
    /// The GETs a new single-row-group file costs: its footer and its chunk.
    const FILE_GETS: u64 = 2;

    /// Every key of `keys` is `Present` with its value.
    async fn assert_present(generation: &Generation, keys: &[i64]) {
        let cache = cache().await;
        for key in keys {
            assert_eq!(
                generation
                    .lookup_first(&Key::Long(*key), &cache)
                    .await
                    .unwrap()
                    .0,
                testing::one_value(Bytes::from(value_for(*key))),
                "key {key}"
            );
        }
    }

    /// The successor of every key of `keys` is `Absent`.
    async fn assert_successors_absent(generation: &Generation, keys: &[i64]) {
        let cache = cache().await;
        for key in keys {
            assert_eq!(
                generation
                    .lookup_first(&Key::Long(key + 1), &cache)
                    .await
                    .unwrap()
                    .0,
                Row::Absent,
                "key {}",
                key + 1
            );
        }
    }

    /// Appends to `table` one file in bucket 0 whose keys lie above every
    /// key of `all`, as `SECOND_SNAPSHOT`; returns the appended keys.
    async fn append_disjoint(table: &mut Table, all: &[i64]) -> Vec<i64> {
        let appended = keys_in_bucket(
            &keys_from(all.last().unwrap() + KEY_STEP, E2E_KEYS),
            bucketing(TWO),
            0,
        );
        table.add_long_file("f2", &appended, 0).await;
        table
            .commit(
                SECOND_SNAPSHOT,
                Some(FIRST_SNAPSHOT),
                SECOND_SEQUENCE,
                SECOND_VERSION,
            )
            .await;
        appended
    }

    #[tokio::test]
    async fn test_load_end_to_end_then_reuses_unchanged_files_on_a_disjoint_append() {
        let (mut table, all) = two_bucket_table().await;
        let config = table.config(Residency::Full, None);
        let first = table.load(&config, None, None).await.unwrap();
        assert_eq!(table.gets(), METADATA_GETS + 2 * FILE_GETS);
        assert_eq!(first.snapshot_id, SnapshotId(FIRST_SNAPSHOT));
        assert_eq!(first.sequence_number, SequenceNumber(FIRST_SEQUENCE));
        assert_eq!(
            first.metadata_location,
            crate::catalog::version_file(&table.root(), FIRST_VERSION)
        );
        assert_eq!(first.layout.bucketing, Some(bucketing(TWO)));
        assert_eq!(first.residency, Residency::Full);
        let committed = TableSource::Static {
            location: table.root(),
        }
        .resolve(&table.storage)
        .await
        .unwrap()
        .snapshot_for_ref(TABLE, testing::MAIN)
        .unwrap();
        assert_eq!(
            first.snapshot_timestamp,
            time::timestamp_from_millis(committed.timestamp_ms()).unwrap()
        );
        assert_eq!(first.files.len(), 2);
        assert_eq!(first.records, approx::count(all.len()));
        assert_eq!(
            first.index_bytes,
            first
                .files
                .iter()
                .map(|file| file.index.resident_bytes())
                .sum::<usize>()
        );
        assert_eq!(first.overlap_groups_verified(), 0);
        for file in &first.files {
            assert_eq!(file.entry.bucketing, Some(bucketing(TWO)));
            assert_eq!(
                (file.entry.key_min.as_ref(), file.entry.key_max.as_ref()),
                (
                    Some(file.index.key_range().0),
                    Some(file.index.key_range().1)
                ),
                "the manifest bounds are the exact range"
            );
        }
        assert_present(&first, &all).await;
        assert_successors_absent(&first, &all).await;

        let appended = append_disjoint(&mut table, &all).await;
        let before = table.gets();
        let second = table.load(&config, None, Some(&first)).await.unwrap();
        assert_eq!(
            table.gets(),
            before + METADATA_GETS + FILE_GETS,
            "one new file's footer and chunk"
        );
        assert_eq!(second.snapshot_id, SnapshotId(SECOND_SNAPSHOT));
        assert_eq!(second.files.len(), 3);
        for old in &first.files {
            assert!(
                second.files.iter().any(|file| Arc::ptr_eq(file, old)),
                "{} is reused by pointer",
                old.entry.path
            );
        }
        assert_eq!(
            second.overlap_groups_verified(),
            0,
            "a disjoint append verifies nothing"
        );
        assert_eq!(second.overlapping_buckets().len(), 0);
        assert_present(&second, &appended).await;
        assert_present(&second, &all).await;
        assert_successors_absent(&second, &appended).await;
    }

    /// A loaded table of one file holding `DETAIL_COLUMN` beside the value
    /// column, served under `config` with both, its fixture, its first
    /// generation, and its keys.
    async fn two_column_generation() -> (Table, TableConfig, Generation, Vec<i64>) {
        let mut table = Table::with_detail(KeyType::Long, manifest_testing::bucket_spec(1));
        let all = keys_from(0, E2E_KEYS);
        table.add_long_file("f", &all, 0).await;
        table
            .commit(FIRST_SNAPSHOT, None, FIRST_SEQUENCE, FIRST_VERSION)
            .await;
        let config = table.config(Residency::Full, None);
        let generation = table.load(&config, None, None).await.unwrap();
        (table, config, generation, all)
    }

    /// A lookup returns the columns at the positions it requests in the order
    /// it names them, whatever the layout's order (RFD 022).
    #[tokio::test]
    async fn test_lookup_answers_the_positions_named_in_their_order() {
        let (_table, _config, generation, all) = two_column_generation().await;
        let names: Vec<&str> = generation
            .layout
            .value_columns
            .iter()
            .map(|column| column.name.as_str())
            .collect();
        assert_eq!(
            names,
            [testing::DETAIL_COLUMN, manifest_testing::VALUE_COLUMN]
        );
        let value = value_for(all[0]);
        let (row, _) = generation
            .lookup(&Key::Long(all[0]), &[1, 0], &cache().await, None, None, &[])
            .await
            .unwrap();
        assert_ne!(testing::detail_of(&value), value, "the columns differ");
        assert_eq!(
            row,
            Row::Present(vec![
                Some(Bytes::from(value.clone())),
                Some(Bytes::from(testing::detail_of(&value))),
            ])
        );
    }

    /// A file the previous generation indexed over other columns than the
    /// snapshot's layout resolves to is not reused: it is opened and
    /// indexed afresh, so no column is answered from another's pages.
    #[tokio::test]
    async fn test_load_opens_afresh_a_file_indexed_over_other_columns() {
        let (table, both, _first, all) = two_column_generation().await;
        let value_only = TableConfig {
            value_columns: vec![crate::config::ValueColumnConfig::named(
                manifest_testing::VALUE_COLUMN,
            )],
            ..both.clone()
        };
        let previous = table.load(&value_only, None, None).await.unwrap();
        let planned = table.plan_load(&both, Some(&previous)).await.unwrap();
        assert!(planned.reused.is_empty(), "indexed over [value] alone");
        let second = planned.build(None).await.unwrap();
        let value = value_for(all[0]);
        let (row, _) = second
            .lookup(&Key::Long(all[0]), &[0, 1], &cache().await, None, None, &[])
            .await
            .unwrap();
        assert_eq!(
            row,
            Row::Present(vec![
                Some(Bytes::from(testing::detail_of(&value))),
                Some(Bytes::from(value)),
            ])
        );
    }

    #[tokio::test]
    async fn test_load_reloads_a_path_whose_size_changed_instead_of_reusing_it() {
        let (mut table, all) = two_bucket_table().await;
        let config = table.config(Residency::Full, None);
        let first = table.load(&config, None, None).await.unwrap();
        // Another file under f0's path: more keys of bucket 0, so its size
        // differs, and the manifest records the new size.
        let in_bucket_zero = keys_in_bucket(&all, bucketing(TWO), 0);
        let more = keys_in_bucket(
            &keys_from(all.last().unwrap() + KEY_STEP, E2E_KEYS),
            bucketing(TWO),
            0,
        );
        let replaced: Vec<i64> = in_bucket_zero.iter().chain(&more).copied().collect();
        table.remove_file("f0");
        table.add_long_file("f0", &replaced, 0).await;
        table
            .commit(
                SECOND_SNAPSHOT,
                Some(FIRST_SNAPSHOT),
                SECOND_SEQUENCE,
                SECOND_VERSION,
            )
            .await;
        let before = table.gets();
        let second = table.load(&config, None, Some(&first)).await.unwrap();
        assert_eq!(
            table.gets(),
            before + METADATA_GETS + FILE_GETS,
            "the replaced file is loaded anew, the other reused"
        );
        let by_name = |generation: &Generation, name: &str| {
            let path = table.path_of(name);
            generation
                .files
                .iter()
                .find(|file| file.entry.path == path)
                .cloned()
                .unwrap()
        };
        assert!(Arc::ptr_eq(&by_name(&first, "f1"), &by_name(&second, "f1")));
        assert!(!Arc::ptr_eq(
            &by_name(&first, "f0"),
            &by_name(&second, "f0")
        ));
        assert_ne!(
            by_name(&first, "f0").entry.size,
            by_name(&second, "f0").entry.size
        );
        assert_present(&second, &replaced).await;
        assert_present(&second, &all).await;
    }

    #[tokio::test]
    async fn test_load_reloads_a_path_added_again_at_another_sequence_number() {
        let (mut table, all) = two_bucket_table().await;
        let config = table.config(Residency::Full, None);
        let first = table.load(&config, None, None).await.unwrap();
        // The same bytes under f0's path, added again by the next commit:
        // the same size, a later data sequence number.
        let in_bucket_zero = keys_in_bucket(&all, bucketing(TWO), 0);
        table.remove_file("f0");
        table.add_long_file("f0", &in_bucket_zero, 0).await;
        table
            .commit(
                SECOND_SNAPSHOT,
                Some(FIRST_SNAPSHOT),
                SECOND_SEQUENCE,
                SECOND_VERSION,
            )
            .await;
        let before = table.gets();
        let second = table.load(&config, None, Some(&first)).await.unwrap();
        assert_eq!(
            table.gets(),
            before + METADATA_GETS + FILE_GETS,
            "the re-added file is loaded anew, the other reused"
        );
        let by_name = |generation: &Generation, name: &str| {
            let path = table.path_of(name);
            generation
                .files
                .iter()
                .find(|file| file.entry.path == path)
                .cloned()
                .unwrap()
        };
        let (old, new) = (by_name(&first, "f0"), by_name(&second, "f0"));
        assert!(!Arc::ptr_eq(&old, &new));
        assert_eq!(old.entry.size, new.entry.size);
        assert_eq!(old.entry.sequence, SequenceNumber(FIRST_SEQUENCE));
        assert_eq!(new.entry.sequence, SequenceNumber(SECOND_SEQUENCE));
        let kept = by_name(&second, "f1");
        assert!(Arc::ptr_eq(&by_name(&first, "f1"), &kept));
        assert_eq!(
            kept.entry.sequence,
            SequenceNumber(FIRST_SEQUENCE),
            "an existing entry keeps the sequence number it was added at"
        );
        assert_present(&second, &all).await;
    }

    #[tokio::test]
    async fn test_load_routes_files_under_an_evolved_bucket_count() {
        let (mut table, all) = two_bucket_table().await;
        let config = table.config(Residency::Full, None);
        let first = table.load(&config, None, None).await.unwrap();
        table.evolve(FOUR);
        let more = keys_from(all.last().unwrap() + KEY_STEP, E2E_KEYS);
        for bucket in 0..FOUR {
            let keys = keys_in_bucket(&more, bucketing(FOUR), bucket);
            assert!(!keys.is_empty(), "bucket {bucket} holds keys");
            table
                .add_long_file(&format!("g{bucket}"), &keys, bucket)
                .await;
        }
        table
            .commit(
                SECOND_SNAPSHOT,
                Some(FIRST_SNAPSHOT),
                SECOND_SEQUENCE,
                SECOND_VERSION,
            )
            .await;
        let before = table.gets();
        let second = table.load(&config, None, Some(&first)).await.unwrap();
        assert_eq!(
            table.gets(),
            before + METADATA_GETS + MANIFEST_GET + u64::from(FOUR) * FILE_GETS,
            "one manifest per spec, then the new files"
        );
        assert_eq!(
            second.layout.bucketing,
            Some(bucketing(FOUR)),
            "the default spec's"
        );
        let carried: HashSet<Option<Bucketing>> = second
            .files
            .iter()
            .map(|file| file.entry.bucketing)
            .collect();
        assert_eq!(
            carried,
            HashSet::from([Some(bucketing(TWO)), Some(bucketing(FOUR))])
        );
        for file in &second.files {
            let bucketing = file.entry.bucketing.unwrap();
            assert_eq!(
                file.entry.bucket,
                bucketing.bucket_of(file.index.key_range().0),
                "{} is in its bucket under its own spec",
                file.entry.path
            );
        }
        assert_eq!(
            second.overlap_groups_verified(),
            0,
            "the new keys lie above the old"
        );
        check_present(&second, &table.storage, &all).await;
        check_present(&second, &table.storage, &more).await;
        assert_successors_absent(&second, &more).await;
    }

    #[tokio::test]
    async fn test_load_verifies_the_group_a_new_file_joins() {
        let (mut table, all) = two_bucket_table().await;
        let config = table.config(Residency::Full, None);
        let first = table.load(&config, None, None).await.unwrap();
        let appended = append_disjoint(&mut table, &all).await;
        let second = table.load(&config, None, Some(&first)).await.unwrap();

        // A file inside bucket 0's existing range, without duplicates,
        // joins an overlap group that this load verifies.
        let interleaved: Vec<i64> = keys_in_bucket(&all, bucketing(TWO), 0)
            .iter()
            .map(|key| key + 1)
            .filter(|key| bucketing(TWO).bucket_of(&Key::Long(*key)) == 0)
            .collect();
        assert!(interleaved.len() > 1);
        table.add_long_file("f3", &interleaved, 0).await;
        table
            .commit(
                THIRD_SNAPSHOT,
                Some(SECOND_SNAPSHOT),
                THIRD_SEQUENCE,
                THIRD_VERSION,
            )
            .await;
        let third = table.load(&config, None, Some(&second)).await.unwrap();
        assert_eq!(third.files.len(), 4);
        assert_eq!(third.overlap_groups_verified(), 1);
        assert_eq!(third.overlapping_buckets(), &BTreeMap::from([(0, 2)]));
        assert_present(&third, &interleaved).await;
        assert_present(&third, &all).await;
        assert_present(&third, &appended).await;
    }

    #[tokio::test]
    async fn test_load_refuses_a_duplicate_key_added_by_a_later_snapshot() {
        let (mut table, all) = two_bucket_table().await;
        let config = table.config(Residency::Full, None);
        let first = table.load(&config, None, None).await.unwrap();
        // The duplicate file starts above f0's first key, so it is the
        // later file in file order and the one the refusal names.
        let in_bucket_zero = keys_in_bucket(&all, bucketing(TWO), 0);
        table.add_long_file("dup", &in_bucket_zero[1..3], 0).await;
        table
            .commit(
                SECOND_SNAPSHOT,
                Some(FIRST_SNAPSHOT),
                SECOND_SEQUENCE,
                SECOND_VERSION,
            )
            .await;
        assert_eq!(
            refusal_of(table.load(&config, None, Some(&first)).await),
            RefusalReason::DuplicateKeys {
                path: table.path_of("dup"),
                key: Key::Long(in_bucket_zero[1]),
            }
        );
        // The previous generation is untouched and keeps answering.
        assert_present(&first, &all).await;
        assert_successors_absent(&first, &all).await;
    }

    #[tokio::test]
    async fn test_plan_load_reads_no_key_chunk() {
        let (table, all) = two_bucket_table().await;
        let config = table.config(Residency::Full, None);
        let planned = table.plan_load(&config, None).await.unwrap();
        assert_eq!(table.gets(), METADATA_GETS + 2, "two footers and no chunk");
        let generation = planned.build(None).await.unwrap();
        assert_eq!(
            table.gets(),
            METADATA_GETS + 2 * FILE_GETS,
            "each file's chunk is fetched at build"
        );
        assert_present(&generation, &all).await;
    }

    /// Rows per key page of the edge-key fixture: several pages to a row
    /// group, so a file's first and last key are on different pages.
    const EDGE_PAGE_ROWS: usize = 4;

    /// Rows per row group of the edge-key fixture: several row groups to a
    /// file, so a file's first and last key are in different row groups.
    const EDGE_ROW_GROUP_ROWS: usize = 8;

    /// Keys drawn from for each bucket file of the edge-key fixture.
    const EDGE_KEYS: i64 = 48;

    /// A two-bucket table whose bucket 0 holds two files with interleaved
    /// key ranges, `a` and `b`, and whose bucket 1 holds `c` alone, laid
    /// out with `dictionary` over `EDGE_PAGE_ROWS`-row pages and
    /// `EDGE_ROW_GROUP_ROWS`-row row groups, committed as `FIRST_SNAPSHOT`.
    async fn edge_fixture(dictionary: Dictionary) -> Table {
        let mut table = Table::bucketed(TWO);
        let layout = WriterLayout {
            page_rows: EDGE_PAGE_ROWS,
            dictionary,
            row_group_rows: Some(EDGE_ROW_GROUP_ROWS),
            ..WriterLayout::contract()
        };
        let in_bucket = |from: i64, bucket: u32| {
            keys_in_bucket(&keys_from(from, EDGE_KEYS), bucketing(TWO), bucket)
        };
        for (name, keys, bucket) in [
            ("a", in_bucket(0, 0), 0),
            ("b", in_bucket(1, 0), 0),
            ("c", in_bucket(0, 1), 1),
        ] {
            let typed = long_keys(&keys);
            table
                .add_file(FileSpec {
                    name,
                    keys: &typed,
                    values: values_of(&keys),
                    partition: Partition::Bucket(bucket),
                    bounds: Bounds::Exact,
                    layout,
                })
                .await;
        }
        table
            .commit(FIRST_SNAPSHOT, None, FIRST_SEQUENCE, FIRST_VERSION)
            .await;
        table
    }

    /// Asserts `edges`, a plan's edges of the new files named `paths`, are
    /// each file's exact key range in `generation`, and `alone` is
    /// `Edge::Alone`.
    fn assert_edges_exact(generation: &Generation, paths: &[String], edges: &[Edge], alone: &str) {
        assert_eq!(paths.len(), edges.len());
        for (path, edge) in paths.iter().zip(edges) {
            if path == alone {
                assert_eq!(*edge, Edge::Alone, "{path}");
                continue;
            }
            let file = generation
                .files
                .iter()
                .find(|file| file.entry.path == *path)
                .unwrap();
            let (first, last) = file.index.key_range();
            assert_eq!(*edge, Edge::Span(first.clone(), last.clone()), "{path}");
        }
    }

    #[tokio::test]
    async fn test_edge_ranges_equal_the_exact_key_range() {
        for dictionary in [Dictionary::Off, Dictionary::Key] {
            let table = edge_fixture(dictionary).await;
            let alone = table.path_of("c");
            // The first and last page of each of `a` and `b`, in different
            // row groups, with each one's dictionary page when it has one.
            let dictionaries = if dictionary == Dictionary::Key { 2 } else { 0 };
            let edge_gets = 2 * (2 + dictionaries);
            for residency in Residency::ALL {
                let scratch = tempfile::tempdir().unwrap();
                let local = local_dir(scratch.path(), table.storage.stats(), None);
                let local = (residency == Residency::Disk).then_some(&local);
                let config = table.config(residency, None);
                let mut planned = table.plan_load(&config, None).await.unwrap();
                let paths: Vec<String> = planned
                    .inspected
                    .iter()
                    .map(|(entry, _)| entry.path.clone())
                    .collect();
                let before = table.gets();
                let edges = planned.edge_ranges(local).await.unwrap();
                assert_eq!(
                    table.gets() - before,
                    edge_gets,
                    "{residency} {dictionary:?}: two edge pages of each overlapping file"
                );
                let generation = planned.build(local).await.unwrap();
                assert_edges_exact(&generation, &paths, &edges, &alone);
                let Some(local) = local else {
                    continue;
                };
                // A restart over the chunk files the first load wrote takes
                // the edges from their records, and the build reuses them.
                let mut restarted = table.plan_load(&config, None).await.unwrap();
                let before = table.gets();
                let edges = restarted.edge_ranges(Some(local)).await.unwrap();
                assert_eq!(table.gets(), before, "a trusted chunk file costs no GET");
                let generation = restarted.build(Some(local)).await.unwrap();
                assert_eq!(table.gets(), before, "and the build fetches no chunk");
                assert_edges_exact(&generation, &paths, &edges, &alone);
            }
        }
    }

    /// An unpartitioned table of two contract files, committed as
    /// `FIRST_SNAPSHOT`.
    async fn two_unbucketed_files() -> Table {
        let mut table = Table::new(KeyType::Long, unpartitioned());
        for (name, from) in [("a", 0), ("b", E2E_KEYS * KEY_STEP)] {
            let keys = keys_from(from, E2E_KEYS);
            let typed = long_keys(&keys);
            table
                .add_file(FileSpec {
                    name,
                    keys: &typed,
                    values: values_of(&keys),
                    partition: Partition::None,
                    bounds: Bounds::Exact,
                    layout: WriterLayout::contract(),
                })
                .await;
        }
        table
            .commit(FIRST_SNAPSHOT, None, FIRST_SEQUENCE, FIRST_VERSION)
            .await;
        table
    }

    /// `two_bucket_table` evolved to `FOUR` buckets with a file in each
    /// whose keys interleave with the first bucketing's, so overlap links
    /// files across the two; committed as `SECOND_SNAPSHOT`.
    async fn interleaved_under_two_bucketings() -> Table {
        let (mut table, _) = two_bucket_table().await;
        table.evolve(FOUR);
        let interleaved = keys_from(1, E2E_KEYS);
        for bucket in 0..FOUR {
            let keys = keys_in_bucket(&interleaved, bucketing(FOUR), bucket);
            table
                .add_long_file(&format!("g{bucket}"), &keys, bucket)
                .await;
        }
        table
            .commit(
                SECOND_SNAPSHOT,
                Some(FIRST_SNAPSHOT),
                SECOND_SEQUENCE,
                SECOND_VERSION,
            )
            .await;
        table
    }

    /// The level the plan stage judges for `config` with every new file's
    /// edges read, beside the generation the plan then builds.
    async fn plan_stage_caps(
        table: &Table,
        config: &TableConfig,
        local: Option<&Arc<LocalDir>>,
    ) -> (Caps, Generation) {
        let mut planned = table.plan_load(config, None).await.unwrap();
        let overlapping = planned.overlapping(local).await.unwrap();
        let caps = planned.caps(&overlapping);
        (caps, planned.build(local).await.unwrap())
    }

    #[tokio::test]
    async fn test_layout_caps_at_the_plan_stage_equal_the_diagnosed_level() {
        let one_file = |layout: WriterLayout, keys: i64| async move {
            let mut table = Table::bucketed(1);
            let all = keys_from(0, keys);
            let typed = long_keys(&all);
            table
                .add_file(FileSpec {
                    name: "f",
                    keys: &typed,
                    values: values_of(&all),
                    partition: Partition::Bucket(0),
                    bounds: Bounds::Exact,
                    layout,
                })
                .await;
            table
                .commit(FIRST_SNAPSHOT, None, FIRST_SEQUENCE, FIRST_VERSION)
                .await;
            table
        };
        let large_pages = KEY_PAGE_ROWS_LARGE + 1;
        let cases = [
            ("contract", two_bucket_table().await.0, Residency::Full),
            ("bloom", two_bucket_table().await.0, Residency::Bloom),
            (
                "dictionary value",
                one_file(
                    WriterLayout {
                        dictionary: Dictionary::Value,
                        ..WriterLayout::contract()
                    },
                    E2E_KEYS,
                )
                .await,
                Residency::Full,
            ),
            (
                "large key pages",
                one_file(
                    WriterLayout {
                        page_rows: usize::try_from(large_pages).unwrap(),
                        ..WriterLayout::contract()
                    },
                    large_pages,
                )
                .await,
                Residency::Full,
            ),
            (
                "unpartitioned",
                two_unbucketed_files().await,
                Residency::Full,
            ),
            (
                "overlapping",
                edge_fixture(Dictionary::Off).await,
                Residency::Disk,
            ),
            (
                "two bucketings",
                interleaved_under_two_bucketings().await,
                Residency::Full,
            ),
        ];
        let mut levels = BTreeSet::new();
        for (name, table, residency) in cases {
            let scratch = tempfile::tempdir().unwrap();
            let local = local_dir(scratch.path(), table.storage.stats(), None);
            let config = table.config(residency, None);
            let (caps, generation) = plan_stage_caps(&table, &config, Some(&local)).await;
            let report = check::diagnose(&generation, config.lookups(), &ValueFormat::opaque, None);
            assert_eq!(caps.level, report.summary.level, "{name}");
            assert_eq!(caps.level, generation.level, "{name}");
            assert_eq!(caps.capping(), report.capping(), "{name}");
            assert_eq!(caps.capping(), generation.capping, "{name}");
            if name == "two bucketings" {
                assert_eq!(
                    caps.capping(),
                    [Capping {
                        code: crate::check::FindingCode::OverlappingFiles,
                        column: None,
                    }],
                    "the plan stage links files across bucketings"
                );
            }
            levels.insert(caps.level);
        }
        assert_eq!(levels.len(), 3, "the cases reach every level");
    }

    /// The spacing of one file's keys in `cost_fixture`: files interleave at
    /// offsets below it, and a key at offset `COST_STRIDE - 1` is in no file.
    const COST_STRIDE: i64 = 4;

    /// A table whose files start at `offsets`, each holding `BLOOM_KEYS`
    /// keys `COST_STRIDE` apart, in `layout`: bucketed one way, so files at
    /// offsets below `COST_STRIDE` overlap, or unpartitioned; committed as
    /// `FIRST_SNAPSHOT`.
    async fn cost_fixture(offsets: &[i64], bucketed: bool, layout: WriterLayout) -> Table {
        let (mut table, partition) = if bucketed {
            (Table::bucketed(1), Partition::Bucket(0))
        } else {
            (Table::new(KeyType::Long, unpartitioned()), Partition::None)
        };
        for (at, offset) in offsets.iter().enumerate() {
            let keys: Vec<i64> = (0..BLOOM_KEYS)
                .map(|row| offset + row * COST_STRIDE)
                .collect();
            let typed = long_keys(&keys);
            table
                .add_file(FileSpec {
                    name: &format!("f{at}"),
                    keys: &typed,
                    values: values_of(&keys),
                    partition,
                    bounds: Bounds::Exact,
                    layout,
                })
                .await;
        }
        table
            .commit(FIRST_SNAPSHOT, None, FIRST_SEQUENCE, FIRST_VERSION)
            .await;
        table
    }

    /// The GETs one lookup of `key` costs through a fresh cache, so cold.
    async fn cold_gets(table: &Table, generation: &Generation, key: i64) -> (Row, u64) {
        let cache = cache().await;
        let before = table.gets();
        let (value, _) = generation
            .lookup_first(&Key::Long(key), &cache)
            .await
            .unwrap();
        (value, table.gets() - before)
    }

    /// The keys of `keys` a lookup pays the worst case for: as many
    /// candidates as `files_probed`, each of which would decode a key page,
    /// its filter, under `bloom`, answering yes.
    fn worst_case(generation: &Generation, keys: impl Iterator<Item = i64>) -> Vec<i64> {
        keys.filter(|&key| {
            let key = Key::Long(key);
            let candidates: Vec<_> = generation.candidates(&key).collect();
            candidates.len() == generation.lookup_cost.files_probed
                && candidates
                    .iter()
                    .all(|file| file.index.page_of(&key).is_some())
        })
        .collect()
    }

    /// The cost prediction is what lookups pay. Per layout and residency:
    /// no key has more candidates than `files_probed` and some key has
    /// that many; a cold hit on a key the last of them holds, every filter
    /// answering yes, costs `gets_per_hit` GETs; and a miss on a key every
    /// candidate's filter passes costs `gets_per_miss`. The doctor's
    /// summary reports the same cost.
    #[tokio::test]
    async fn test_lookup_cost_per_residency_is_what_a_cold_lookup_pays() {
        let cost = |gets_per_hit, gets_per_miss, files_probed| LookupCost {
            gets_per_hit,
            gets_per_miss,
            files_probed,
        };
        let after_the_first = BLOOM_KEYS * COST_STRIDE;
        // Per case, the files' offsets, whether the table is bucketed, the
        // dictionary columns, and the cost under full, disk, and bloom.
        let cases = [
            (
                "contract",
                vec![0],
                true,
                Dictionary::Off,
                [cost(1, 0, 1), cost(1, 0, 1), cost(2, 1, 1)],
            ),
            (
                "dictionary value",
                vec![0],
                true,
                Dictionary::Value,
                [cost(2, 0, 1), cost(2, 0, 1), cost(3, 1, 1)],
            ),
            (
                "dictionary key",
                vec![0],
                true,
                Dictionary::Key,
                [cost(1, 0, 1), cost(1, 0, 1), cost(3, 2, 1)],
            ),
            (
                "two overlapping",
                vec![0, 1],
                true,
                Dictionary::Off,
                [cost(1, 0, 2), cost(1, 0, 2), cost(3, 2, 2)],
            ),
            (
                "three overlapping",
                vec![0, 1, 2],
                true,
                Dictionary::Off,
                [cost(1, 0, 3), cost(1, 0, 3), cost(4, 3, 3)],
            ),
            (
                "unbucketed",
                vec![0, after_the_first],
                false,
                Dictionary::Off,
                [cost(1, 0, 1), cost(1, 0, 1), cost(2, 1, 1)],
            ),
        ];
        for (name, offsets, bucketed, dictionary, expected) in cases {
            let layout = WriterLayout {
                dictionary,
                bloom: Bloom::Fpp(LOOSE_FPP),
                ..WriterLayout::contract()
            };
            let table = cost_fixture(&offsets, bucketed, layout).await;
            let last = *offsets.last().unwrap();
            let end = last + BLOOM_KEYS * COST_STRIDE;
            let held_by_last: Vec<i64> = (0..BLOOM_KEYS)
                .map(|row| last + row * COST_STRIDE)
                .collect();
            let absent = (0..end).filter(|key| key % COST_STRIDE == COST_STRIDE - 1);
            for (residency, expected) in Residency::ALL.into_iter().zip(expected) {
                let scratch = tempfile::tempdir().unwrap();
                let local = local_dir(scratch.path(), table.storage.stats(), None);
                let config = TableConfig {
                    strict: false,
                    ..table.config(residency, None)
                };
                let generation = table.load(&config, Some(&local), None).await.unwrap();
                assert_eq!(generation.lookup_cost, expected, "{name} {residency}");
                assert_eq!(
                    check::diagnose(&generation, config.lookups(), &ValueFormat::opaque, None)
                        .summary
                        .lookup_cost,
                    Some(expected),
                    "{name} {residency}: the doctor reports the gauges' cost"
                );
                let most = (0..end)
                    .map(|key| generation.candidates(&Key::Long(key)).count())
                    .max();
                assert_eq!(most, Some(expected.files_probed), "{name} {residency}");
                let hits = worst_case(&generation, held_by_last.iter().copied());
                let misses = worst_case(&generation, absent.clone());
                assert!(
                    !hits.is_empty() && !misses.is_empty(),
                    "{name} {residency}: the fixture reaches the worst case"
                );
                let (value, gets) = cold_gets(&table, &generation, hits[0]).await;
                assert_eq!(value, testing::one_value(Bytes::from(value_for(hits[0]))));
                assert_eq!(gets, expected.gets_per_hit, "{name} {residency}: a hit");
                let (value, gets) = cold_gets(&table, &generation, misses[0]).await;
                assert_eq!(value, Row::Absent);
                assert_eq!(gets, expected.gets_per_miss, "{name} {residency}: a miss");
            }
        }
    }

    /// The manifest entry of a file filed under `bucket` of `bucketing`.
    fn placed(bucketing: Option<Bucketing>, bucket: u32) -> DataFileEntry {
        DataFileEntry {
            path: format!("{DATA_DIR}/{bucket}.parquet"),
            bucket,
            bucketing,
            records: 1,
            sequence: SequenceNumber(FIRST_SEQUENCE),
            added_by: SnapshotId(FIRST_SNAPSHOT),
            size: 1,
            key_min: None,
            key_max: None,
            partition: manifest::Partition::unpartitioned(),
        }
    }

    #[test]
    fn test_sharing_marks_entries_one_key_can_reach_together() {
        let (two, four) = (Some(bucketing(TWO)), Some(bucketing(FOUR)));
        let cases = [
            (
                "the same bucket",
                vec![(two, 0), (two, 0), (two, 1)],
                vec![true, true, false],
            ),
            (
                "congruent buckets under two bucketings",
                vec![(two, 0), (four, 2)],
                vec![true, true],
            ),
            (
                "buckets under two bucketings no key reaches both of",
                vec![(two, 0), (four, 1), (four, 3)],
                vec![false, false, false],
            ),
            (
                "an unbucketed entry beside a bucketed one",
                vec![(None, UNBUCKETED), (two, 1)],
                vec![true, true],
            ),
            (
                "an unbucketed entry alone",
                vec![(None, UNBUCKETED)],
                vec![false],
            ),
        ];
        for (name, places, expected) in cases {
            let entries: Vec<DataFileEntry> = places
                .into_iter()
                .map(|(bucketing, bucket)| placed(bucketing, bucket))
                .collect();
            assert_eq!(sharing(entries.iter()), expected, "{name}");
        }
    }

    /// Bucket 0 of a one-bucket table: `a`, whose keys interleave with
    /// `b`'s when `with_b` adds it, and `x`, whose keys lie above both and
    /// whose first key page claims more values than it holds, so its edge
    /// pages do not decode; committed as `FIRST_SNAPSHOT`.
    async fn with_an_undecodable_edge(with_b: bool) -> Table {
        let mut table = Table::bucketed(1);
        table.add_long_file("a", &keys_from(0, E2E_KEYS), 0).await;
        if with_b {
            table.add_long_file("b", &keys_from(1, E2E_KEYS), 0).await;
        }
        let above = E2E_KEYS * KEY_STEP;
        table
            .add_long_file("x", &keys_from(above, E2E_KEYS), 0)
            .await;
        let path = table.path_of("x");
        let on_disk = std::path::Path::new(path.strip_prefix(FILE_PREFIX).unwrap());
        let bytes = Bytes::from(std::fs::read(on_disk).unwrap());
        let metadata = ParquetMetaDataReader::new()
            .with_page_index_policy(PageIndexPolicy::Required)
            .parse_and_finish(&bytes)
            .unwrap();
        let first_page = metadata.offset_index().unwrap()[0][0].page_locations()[0].offset;
        let at = usize::try_from(first_page).unwrap();
        let more_than_it_holds = i32::try_from(E2E_KEYS + 1).unwrap();
        local_testing::overwrite(
            on_disk,
            u64::try_from(first_page).unwrap(),
            &index_testing::with_first_page_claiming(&bytes[at..], more_than_it_holds),
        );
        table
            .commit(FIRST_SNAPSHOT, None, FIRST_SEQUENCE, FIRST_VERSION)
            .await;
        table
    }

    /// A file whose edge pages do not decode leaves the level gate's
    /// overlap to the build when the other files alone meet the level, and
    /// the build refuses the file for what its verification finds there.
    #[tokio::test]
    async fn test_judge_level_defers_an_undecodable_edge_to_the_build() {
        let table = with_an_undecodable_edge(false).await;
        let mut planned = table
            .plan_load(&table.config(Residency::Full, None), None)
            .await
            .unwrap();
        let edges = planned.edge_ranges(None).await.unwrap();
        assert!(edges.contains(&Edge::Unknown), "{edges:?}");
        planned.judge_level(None).await.unwrap();
        let error = planned.build(None).await.unwrap_err();
        assert!(
            matches!(
                &error,
                Error::File { path, fault: FileFault::PageRowsMismatch { .. } }
                    if *path == table.path_of("x")
            ),
            "{error:?}"
        );
    }

    /// A file whose edge pages do not decode beside two files that overlap
    /// is refused for its level at the footer stage: the overlap the other
    /// files show is enough, so no chunk file is written.
    #[tokio::test]
    async fn test_judge_level_refuses_overlap_beside_an_undecodable_edge() {
        let table = with_an_undecodable_edge(true).await;
        let scratch = tempfile::tempdir().unwrap();
        let local = local_dir(scratch.path(), table.storage.stats(), None);
        let mut planned = table
            .plan_load(&table.config(Residency::Disk, None), None)
            .await
            .unwrap();
        assert_eq!(
            refusal_of(planned.judge_level(Some(&local)).await),
            RefusalReason::BelowLevel {
                declared: Level::Optimal,
                achieved: Level::Permissive,
                columns: vec![crate::check::ColumnLevel {
                    column: crate::manifest::testing::VALUE_COLUMN.to_string(),
                    level: Level::Permissive,
                }],
                findings: vec![Capping {
                    code: crate::check::FindingCode::OverlappingFiles,
                    column: None,
                }],
            }
        );
        assert_eq!(local.index_bytes_in_use().unwrap(), 0, "no chunk file");
    }

    /// A configuration built by hand, which no validation saw, meets the
    /// violation at the loader before any data file is read.
    #[tokio::test]
    async fn test_plan_load_refuses_lookups_its_residency_cannot_reach() {
        let (table, _) = two_bucket_table().await;
        let config = TableConfig {
            lookups: Some(Level::Optimal),
            ..table.config(Residency::Bloom, None)
        };
        let Err(error) = table.plan_load(&config, None).await else {
            panic!("bloom residency cannot reach optimal");
        };
        assert!(
            matches!(
                error,
                Error::Config(ConfigError::Invalid(ConfigViolation::LookupsUnreachable {
                    index: None,
                    lookups: Level::Optimal,
                    residency: Residency::Bloom,
                    ..
                }))
            ),
            "{error:?}"
        );
        assert_eq!(
            table.gets(),
            RESOLVE_GETS,
            "no manifest and no footer was read"
        );
    }

    #[tokio::test]
    async fn test_load_full_budget_refuses_at_the_footer_stage() {
        let (table, _) = two_bucket_table().await;
        let config = table.config(Residency::Full, Some(TINY_BUDGET));
        let RefusalReason::OverBudget {
            of: Budget::Resident,
            needed,
            budget,
        } = refusal_of(table.load(&config, None, None).await)
        else {
            panic!("expected an over-budget refusal");
        };
        assert_eq!(budget, TINY_BUDGET);
        assert_eq!(table.gets(), METADATA_GETS + 2, "two footers and no chunk");
        // The refusal carries exactly what the two footers predicted.
        let predicted: u64 = {
            let mut total = 0;
            for (file, _) in &table.files {
                let handle = Arc::new(
                    FileHandle::open(
                        Arc::clone(&table.storage),
                        file.file_path(),
                        file.file_size_in_bytes(),
                        Arc::clone(&table.hedge),
                    )
                    .await
                    .unwrap(),
                );
                total += FileIndex::inspect(TABLE, handle, &layout(None))
                    .await
                    .unwrap()
                    .predicted()
                    .full;
            }
            total
        };
        assert_eq!(needed, predicted);
        let generous = table.config(Residency::Full, Some(predicted));
        let loaded = table.load(&generous, None, None).await.unwrap();
        assert_eq!(approx::count(loaded.index_bytes), predicted);
    }

    /// A Disk generation just loaded has just written its chunk files, so
    /// the page cache holds every page of their key chunks: from the page
    /// each file's first chunk starts in to its end.
    #[tokio::test]
    async fn test_chunk_residency_of_a_disk_load_is_every_chunk_page() {
        let (table, _) = two_bucket_table().await;
        let scratch = tempfile::tempdir().unwrap();
        let local = local_dir(scratch.path(), table.storage.stats(), None);
        let disk = table
            .load(&table.config(Residency::Disk, None), Some(&local), None)
            .await
            .unwrap();
        let page = crate::local::page_bytes().unwrap();
        let pages: u64 = disk
            .identities()
            .iter()
            .map(|identity| {
                let path = local.chunk_path(identity, ChunkRules::KeyValue);
                let start = local_testing::chunks_offset(&path);
                let end = std::fs::metadata(&path).unwrap().len();
                end.div_ceil(page) - start / page
            })
            .sum();
        assert_eq!(
            disk.chunk_residency().unwrap(),
            ResidentPages {
                pages,
                resident: pages
            }
        );
    }

    /// Two generations over the same chunk files, as two entries over one
    /// table hold, sample each file once: the second takes the first's
    /// samples, which here are made up, so a fresh sample would differ.
    #[tokio::test]
    async fn test_chunk_residency_in_takes_a_file_sampled_before_from_the_sweep() {
        let (table, _) = two_bucket_table().await;
        let scratch = tempfile::tempdir().unwrap();
        let local = local_dir(scratch.path(), table.storage.stats(), None);
        let config = table.config(Residency::Disk, None);
        let first = table.load(&config, Some(&local), None).await.unwrap();
        let second = table.load(&config, Some(&local), None).await.unwrap();
        let mut sampled = HashMap::new();
        first.chunk_residency_in(&mut sampled).unwrap();
        assert_eq!(sampled.len(), first.files.len());
        let made_up = ResidentPages {
            pages: 1,
            resident: 0,
        };
        for sample in sampled.values_mut() {
            *sample = Ok(made_up);
        }
        let files = approx::count(second.files.len());
        assert_eq!(
            second.chunk_residency_in(&mut sampled).unwrap(),
            ResidentPages {
                pages: files,
                resident: 0
            }
        );
        assert_eq!(sampled.len(), first.files.len());
    }

    /// A Disk generation has one chunk file per data file, its key chunks
    /// lying after its header and ending at the file's end.
    #[tokio::test]
    async fn test_chunk_files_of_a_disk_load_end_at_each_file_s_end() {
        let (table, _) = two_bucket_table().await;
        let scratch = tempfile::tempdir().unwrap();
        let local = local_dir(scratch.path(), table.storage.stats(), None);
        let disk = table
            .load(&table.config(Residency::Disk, None), Some(&local), None)
            .await
            .unwrap();
        let files: Vec<(&Path, Range<u64>)> = disk.chunk_files().collect();
        assert_eq!(files.len(), disk.files.len());
        for (path, chunks) in files {
            assert_eq!(chunks.start, local_testing::chunks_offset(path));
            assert_eq!(chunks.end, std::fs::metadata(path).unwrap().len());
        }
    }

    /// A Full generation has no chunk file and counts no page.
    #[tokio::test]
    async fn test_chunk_residency_of_a_full_load_counts_no_page() {
        let (table, _) = two_bucket_table().await;
        let full = table
            .load(&table.config(Residency::Full, None), None, None)
            .await
            .unwrap();
        assert_eq!(full.chunk_files().count(), 0);
        assert_eq!(full.chunk_residency().unwrap(), ResidentPages::default());
    }

    #[tokio::test]
    async fn test_load_disk_budget_refuses_at_the_footer_stage_and_reuses_chunk_files() {
        let (table, all) = two_bucket_table().await;
        let scratch = tempfile::tempdir().unwrap();
        let config = table.config(Residency::Disk, None);
        let tight = local_dir(scratch.path(), table.storage.stats(), Some(TINY_BUDGET));
        let RefusalReason::OverBudget {
            of: Budget::Index,
            needed,
            budget,
        } = refusal_of(table.load(&config, Some(&tight), None).await)
        else {
            panic!("expected an over-budget refusal");
        };
        assert_eq!(budget, TINY_BUDGET);
        assert!(needed > budget);
        assert_eq!(table.gets(), METADATA_GETS + 2, "two footers and no chunk");
        assert_eq!(
            tight.index_bytes_in_use().unwrap(),
            0,
            "nothing was written"
        );

        let unbounded = local_dir(scratch.path(), table.storage.stats(), None);
        let loaded = table.load(&config, Some(&unbounded), None).await.unwrap();
        assert_eq!(loaded.residency, Residency::Disk);
        let on_disk = unbounded.index_bytes_in_use().unwrap();
        assert!(on_disk > 0);
        assert_eq!(loaded.identities().len(), 2);
        assert_eq!(
            unbounded
                .retain(&loaded.live_chunk_files().collect())
                .unwrap(),
            0,
            "every chunk file is live"
        );
        let cache = cache().await;
        for key in &all {
            assert_eq!(
                loaded
                    .lookup_first(&Key::Long(*key), &cache)
                    .await
                    .unwrap()
                    .0,
                testing::one_value(Bytes::from(value_for(*key)))
            );
        }

        // A restart over the same directory writes nothing, so a budget of
        // exactly what is on disk admits it and the load fetches footers only.
        let exact = local_dir(scratch.path(), table.storage.stats(), Some(on_disk));
        let before = table.gets();
        let restarted = table.load(&config, Some(&exact), None).await.unwrap();
        assert_eq!(table.gets(), before + METADATA_GETS + 2, "footers only");
        assert_eq!(restarted.files.len(), 2);
        assert_eq!(exact.index_bytes_in_use().unwrap(), on_disk);
        assert_eq!(
            unbounded.retain(&HashSet::new()).unwrap(),
            on_disk,
            "nothing live frees all"
        );
    }

    /// The bucket `f`'s manifest entry records in the bucket-check tests,
    /// and the other bucket, which its keys hash to.
    const RECORDED: u32 = 1;
    const COMPUTED: u32 = 0;

    /// A two-bucket table with one file `f` of `keys` recorded under
    /// `RECORDED`, committed as `FIRST_SNAPSHOT`.
    async fn one_file_under(keys: &[i64]) -> Table {
        let mut table = Table::bucketed(TWO);
        table.add_long_file("f", keys, RECORDED).await;
        table
            .commit(FIRST_SNAPSHOT, None, FIRST_SEQUENCE, FIRST_VERSION)
            .await;
        table
    }

    /// A file whose keys all hash to `COMPUTED` but whose manifest records
    /// `RECORDED`: its first key is a fence, so every tier's load refuses it
    /// before it can answer one of its keys `Absent`.
    #[tokio::test]
    async fn test_load_a_file_recorded_under_another_bucket_is_refused() {
        let all = keys_from(0, E2E_KEYS);
        let keys = keys_in_bucket(&all, bucketing(TWO), COMPUTED);
        let table = one_file_under(&keys).await;
        let scratch = tempfile::tempdir().unwrap();
        let local = local_dir(scratch.path(), table.storage.stats(), None);
        for residency in Residency::ALL {
            let config = table.config(residency, None);
            assert_eq!(
                refusal_of(table.load(&config, Some(&local), None).await),
                RefusalReason::KeyOutsideBucket {
                    path: table.path_of("f"),
                    key: Key::Long(keys[0]),
                    recorded: RECORDED,
                    computed: COMPUTED,
                },
                "{residency}"
            );
        }
    }

    /// One key of `COMPUTED` among `RECORDED`'s, neither a fence nor the
    /// file's first or last key: the load, which hashes only those, serves
    /// the file and answers that key `Absent` though the file holds it, and
    /// `verify_buckets`, which hashes every key, refuses it in every tier.
    #[tokio::test]
    async fn test_verify_buckets_refuses_a_key_the_load_does_not_hash() {
        let all = keys_from(0, E2E_KEYS);
        let recorded = keys_in_bucket(&all, bucketing(TWO), RECORDED);
        let (first, last) = (recorded[0], recorded[recorded.len() - 1]);
        let stray = keys_in_bucket(&all, bucketing(TWO), COMPUTED)
            .into_iter()
            .find(|key| first < *key && *key < last)
            .unwrap();
        let mut keys = recorded.clone();
        keys.push(stray);
        keys.sort_unstable();
        let table = one_file_under(&keys).await;
        let scratch = tempfile::tempdir().unwrap();
        let local = local_dir(scratch.path(), table.storage.stats(), None);
        for residency in Residency::ALL {
            let config = table.config(residency, None);
            let generation = table.load(&config, Some(&local), None).await.unwrap();
            assert_eq!(
                generation.files[0].index.fences(),
                [vec![Key::Long(first)]],
                "one key page, so the stray key is no fence"
            );
            assert_eq!(
                generation
                    .lookup_first(&Key::Long(stray), &cache().await)
                    .await
                    .unwrap()
                    .0,
                Row::Absent,
                "{residency}: routed to the bucket it hashes to, which holds no file"
            );
            assert_eq!(
                refusal_of(generation.verify_buckets().await),
                RefusalReason::KeyOutsideBucket {
                    path: table.path_of("f"),
                    key: Key::Long(stray),
                    recorded: RECORDED,
                    computed: COMPUTED,
                },
                "{residency}"
            );
        }
    }

    /// A correctly bucketed table and an unbucketed one pass the full check.
    #[tokio::test]
    async fn test_verify_buckets_passes_bucketed_and_unbucketed_files() {
        let (bucketed, _) = two_bucket_table().await;
        let config = bucketed.config(Residency::Full, None);
        let generation = bucketed.load(&config, None, None).await.unwrap();
        generation.verify_buckets().await.unwrap();

        let mut unbucketed = Table::new(KeyType::Long, crate::manifest::testing::unpartitioned());
        let keys = keys_from(0, E2E_KEYS);
        unbucketed
            .add_file(FileSpec {
                name: "f",
                keys: &long_keys(&keys),
                values: values_of(&keys),
                partition: Partition::None,
                bounds: Bounds::Exact,
                layout: WriterLayout::contract(),
            })
            .await;
        unbucketed
            .commit(FIRST_SNAPSHOT, None, FIRST_SEQUENCE, FIRST_VERSION)
            .await;
        let config = unbucketed.config(Residency::Full, None);
        let generation = unbucketed.load(&config, None, None).await.unwrap();
        assert_eq!(generation.files[0].entry.bucketing, None);
        generation.verify_buckets().await.unwrap();
    }

    #[test]
    fn test_generation_and_lookups_are_send_and_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<Generation>();
        assert_send_sync::<LoadedFile>();
        assert_send_sync::<Row>();
        assert_send_sync::<Lookup>();
        assert_send_sync::<MultiLookup>();
    }

    /// Two worker threads keep twice `LOAD_FILES_PER_WORKER` files in
    /// flight.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_load_concurrency_scales_with_the_worker_threads() {
        assert_eq!(load_concurrency(), 2 * LOAD_FILES_PER_WORKER);
    }

    /// More workers than `LOAD_FILES_MAX` over `LOAD_FILES_PER_WORKER` keep
    /// `LOAD_FILES_MAX` in flight, not more.
    #[tokio::test(flavor = "multi_thread", worker_threads = 16)]
    async fn test_load_concurrency_stops_at_its_ceiling() {
        assert!(shard::workers() * LOAD_FILES_PER_WORKER > LOAD_FILES_MAX);
        assert_eq!(load_concurrency(), LOAD_FILES_MAX);
    }

    /// The value a newer copy of `key` holds, unlike its first.
    fn updated_value(key: i64) -> Vec<u8> {
        testing::value_of_len(key + KEY_STEP * E2E_KEYS, testing::VALUE_BYTES)
    }

    #[tokio::test]
    async fn test_lookup_and_plan_pass_over_a_deleted_copy_in_probe_order() {
        let mem = Mem::new();
        let keys = keys_from(0, E2E_KEYS);
        let updated = keys[keys.len() / 2];
        let (older, newer) = ("older", "newer");
        let old_file = at_sequence(mem.keyed(older, &keys, None, manifest::UNBUCKETED).await, 1);
        let new_file = at_sequence(
            mem.file(
                newer,
                &[updated],
                &[Some(updated_value(updated))],
                (None, manifest::UNBUCKETED),
                &WriterLayout::contract(),
            )
            .await,
            2,
        );
        let key = Key::Long(updated);
        let old_row = RowPosition(approx::count(keys.len() / 2));
        let at_row = |rows: &[u64]| Some(Arc::new(FileDeletes::of_rows(rows.iter().copied())));
        let both_live = assemble_deleted(
            vec![(Arc::clone(&old_file), None), (Arc::clone(&new_file), None)],
            layout(None),
        )
        .await;
        assert!(matches!(
            refusal_of(both_live),
            RefusalReason::DuplicateKeys { .. }
        ));

        // The older copy deleted: the newer answers, probed first.
        let generation = assemble_deleted(
            vec![
                (Arc::clone(&old_file), at_row(&[old_row.0])),
                (Arc::clone(&new_file), None),
            ],
            layout(None),
        )
        .await
        .unwrap();
        let probes = generation.probes(&key);
        let seen: Vec<(String, bool)> = probes
            .iter()
            .map(|(file, _, deleted)| (file.entry.path.clone(), *deleted))
            .collect();
        assert_eq!(
            seen,
            [(Mem::location(newer), false), (Mem::location(older), true)]
        );
        let (planned, _) = generation.plan(std::slice::from_ref(&key))[0]
            .clone()
            .unwrap();
        assert_eq!(planned.entry.path, Mem::location(newer));
        let cache = cache().await;
        assert_eq!(
            generation.lookup_first(&key, &cache).await.unwrap().0,
            testing::one_value(Bytes::from(updated_value(updated)))
        );

        // The newer copy deleted: the older answers, past the first probe,
        // with no value page of the deleted copy read.
        let generation = assemble_deleted(
            vec![
                (Arc::clone(&old_file), None),
                (Arc::clone(&new_file), at_row(&[0])),
            ],
            layout(None),
        )
        .await
        .unwrap();
        let (planned, page) = generation.plan(std::slice::from_ref(&key))[0]
            .clone()
            .unwrap();
        assert_eq!(planned.entry.path, Mem::location(older));
        let cache = testing::cache().await;
        let before = mem.store.storage.stats().gets();
        assert_eq!(
            generation.lookup_first(&key, &cache).await.unwrap().0,
            testing::one_value(Bytes::from(value_for(updated)))
        );
        assert_eq!(
            mem.store.storage.stats().gets(),
            before + 1,
            "the live copy's value page alone"
        );
        assert!(planned.index.value_page_offset(0, &page).is_some());

        // Both deleted: absent, with no GET in the Full tier.
        let generation = assemble_deleted(
            vec![
                (Arc::clone(&old_file), at_row(&[old_row.0])),
                (new_file, at_row(&[0])),
            ],
            layout(None),
        )
        .await
        .unwrap();
        let before = mem.store.storage.stats().gets();
        assert_eq!(
            generation.lookup_first(&key, &cache).await.unwrap().0,
            Row::Absent
        );
        assert_eq!(mem.store.storage.stats().gets(), before);
        let (first, page) = generation.plan(std::slice::from_ref(&key))[0]
            .clone()
            .unwrap();
        assert_eq!(first.entry.path, Mem::location(newer), "the first probe");
        assert!(page.position.is_some());
    }

    #[tokio::test]
    async fn test_assemble_refuses_a_file_with_deletes_whose_row_counts_disagree() {
        let mem = Mem::new();
        let keys = keys_from(0, E2E_KEYS);
        let file = mem.keyed("f", &keys, None, manifest::UNBUCKETED).await;
        let mut changed = Arc::into_inner(file).unwrap();
        changed.entry.records += 1;
        let file = Arc::new(changed);
        assemble_deleted(vec![(Arc::clone(&file), None)], layout(None))
            .await
            .unwrap();
        let deletes = Some(Arc::new(FileDeletes::of_rows([0])));
        let result = assemble_deleted(vec![(file, deletes)], layout(None)).await;
        let Err(Error::File { path, fault }) = result else {
            panic!("expected a file fault, got {result:?}");
        };
        assert_eq!(path, Mem::location("f"));
        assert_eq!(
            fault,
            FileFault::Delete(DeleteFault::RecordCountMismatch {
                manifest: approx::count(keys.len()) + 1,
                footer: i64::try_from(keys.len()).unwrap(),
            })
        );
    }
}
