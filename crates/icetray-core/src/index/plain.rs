//! `PLAIN` byte-array pages read in place: a `BYTE_ARRAY` column chunk of
//! `PLAIN` data pages and no dictionary page, neither nested nor repeated.
//! Each page is fetched and decompressed as for parquet's column reader
//! (`ChunkPages`), and its values are sliced from the decompressed page,
//! each a length prefix and the bytes it counts, so a value read costs no
//! allocation and no reference count. Two readers share the format: the
//! one `verify` checks the common key column with at load
//! (`checked_run`), whose values must all be present, and the one a Scan
//! and a lookup read value rows with (`value_rows`), nulls among them,
//! whose rows before the first asked for are passed over by walking their
//! length prefixes. A chunk of any other shape, and a chunk holding any
//! page a reader cannot read exactly, is left whole to the column reader,
//! which reads it as it reads every other chunk: each reader decodes
//! exactly the values the column reader decodes, from exactly the pages
//! the offset index names, or decodes none of them.

use std::cmp::Ordering;
use std::mem::size_of;
use std::ops::Range;
use std::panic::AssertUnwindSafe;
use std::sync::{Arc, Mutex, PoisonError};

use bytes::Bytes;
use parquet::basic::{Encoding, Type as PhysicalType};
use parquet::column::page::{Page, PageReader};

use super::build::{DECODE_BATCH_RECORDS, KeyBatch, RunCheck, is_utf8};
use super::page_header::{BYTE_ARRAY_LENGTH_PREFIX_BYTES, leading_varint, whole_varint};
use super::pages::{ChunkPages, PageTally};
use super::{ColumnChunk, row_index};
use crate::approx;
use crate::key::Key;
use crate::parquet_io::Segments;

/// The definition level of a present value in a column neither nested nor
/// repeated, and so the most such a column's levels reach: a nullable
/// top-level column's.
const PRESENT_LEVEL: i16 = 1;

/// The definition level of a null in a column neither nested nor
/// repeated.
const NULL_LEVEL: i16 = 0;

/// The bits an RLE/bit-packed hybrid run header shifts its count by: the
/// low bit says which kind of run follows, clear for an RLE run.
const RUN_KIND_BITS: u32 = 1;

/// The levels one group of a bit-packed run holds; at a level of one bit,
/// a group is one byte, its first level in the lowest bit.
const PACKED_GROUP_LEVELS: usize = u8::BITS as usize;

/// The bytes of a v1 data page's length of its RLE levels: a little-endian
/// `i32`, as parquet reads it.
const LEVELS_LENGTH_BYTES: usize = size_of::<i32>();

/// What a `BYTE_ARRAY` key column holds.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum ByteKeys {
    /// UTF-8 strings.
    Text,
    /// Opaque bytes.
    Bytes,
}

/// Whether `chunk` is one the plain readers read: a `BYTE_ARRAY` column
/// neither nested nor repeated, with no dictionary page. `Some` with
/// whether the column is required, so its pages carry no definition
/// levels.
fn plain_chunk(chunk: ColumnChunk<'_>) -> Option<bool> {
    let descr = chunk.column.descriptor();
    (descr.physical_type() == PhysicalType::BYTE_ARRAY
        && descr.max_rep_level() == 0
        && descr.max_def_level() <= PRESENT_LEVEL
        && chunk.dictionary_range().is_none())
    .then_some(descr.max_def_level() == 0)
}

/// Every key of row group `chunk`, a `BYTE_ARRAY` column holding `keys`,
/// read from `segments` plain and checked (`RunCheck`); `None` when the
/// chunk is not of the shape this reader reads, or any page of it is not
/// one it reads exactly, for the column reader to read instead. Exactly:
/// every data page `PLAIN` (`plain_page`), its definition levels, where
/// the column has them, one RLE run of the page's value count at
/// `PRESENT_LEVEL`, its values each a length prefix and the bytes it
/// counts, as many as the header's value count, with no byte left after
/// the last, and the pages together the ones
/// `ColumnChunk::check_pages_read` accepts for the row group's rows.
/// Anything the pages fault on ends this read with nothing reported, so
/// the column reader meets the fault and reports it (`read_plain`).
pub(super) fn checked_run<'c>(
    chunk: ColumnChunk<'c>,
    keys: ByteKeys,
    segments: &Arc<Segments>,
) -> Option<RunCheck<'c>> {
    let rows = 0..row_index(chunk.rows());
    read_plain(chunk, rows, segments, |pages, required| {
        let mut run = RunCheck::new(chunk);
        let decoded = read_pages(pages, keys, required, &mut run)?;
        Some((run, decoded))
    })
}

/// What a plain reader reads of rows `rows` of `chunk` from `segments`:
/// `read`'s answer over the chunk's pages holding them, given whether the
/// column is `required`, with the rows it decoded; `None` when the chunk
/// is not of the shape the plain readers read (`plain_chunk`), when `read`
/// meets a page it does not read exactly, and when the pages read are not
/// the ones `ColumnChunk::check_pages_read` accepts for the rows, for the
/// column reader to read the chunk instead. Anything the pages fault on (a
/// header, a decompression, a count past the offset index, a panic) ends
/// the read the same way, with nothing reported, so the column reader
/// meets the fault and reports it. The read runs under `catch_unwind` for
/// that reason: a panic in it is the column reader's to meet and report.
fn read_plain<T>(
    chunk: ColumnChunk<'_>,
    rows: Range<usize>,
    segments: &Arc<Segments>,
    read: impl FnOnce(ChunkPages, bool) -> Option<(T, usize)>,
) -> Option<T> {
    let required = plain_chunk(chunk)?;
    let segments = Arc::clone(segments);
    let tally = Arc::new(Mutex::new(PageTally::default()));
    // Unwind-safe: the pages are built inside the call and dropped with a
    // panic, the footer `chunk` borrows is only read, and what `read` and
    // the tally a panic can leave mid-page is dropped unread.
    let answer = std::panic::catch_unwind(AssertUnwindSafe(|| {
        // intentional: a chunk parquet's page reader cannot plan is the
        // column reader's to report.
        let pages = ChunkPages::new(chunk, rows.clone(), segments, Arc::clone(&tally)).ok()?;
        read(pages, required)
    }));
    // intentional: a panic is the column reader's to meet and report.
    let (answer, decoded) = answer.ok()??;
    let seen = std::mem::take(&mut tally.lock().unwrap_or_else(PoisonError::into_inner).seen);
    // intentional: pages that contradict the offset index are the column
    // reader's to refuse, as `FileFault::PageRowsMismatch`.
    chunk.check_pages_read(rows, &seen, Ok(decoded)).ok()?;
    Some(answer)
}

/// Every data page of `pages`, each read plain and handed to `run` a
/// decode batch at a time: the values read, or `None` at the first page
/// not read exactly. A batch holds at most `DECODE_BATCH_RECORDS` values
/// however many a header counts, so no buffer here is sized by a count
/// before the values it counts have been read.
fn read_pages(
    mut pages: ChunkPages,
    keys: ByteKeys,
    required: bool,
    run: &mut RunCheck<'_>,
) -> Option<usize> {
    let batch_records = row_index(DECODE_BATCH_RECORDS);
    let mut decoded = 0;
    // intentional: a page the page reader faults on is the column
    // reader's to report.
    while let Some(page) = pages.get_next_page().ok()? {
        let (mut rest, count) = key_values(&page, required)?;
        let mut left = count;
        while left > 0 {
            let take = left.min(batch_records);
            let mut values = Vec::with_capacity(take);
            for _ in 0..take {
                values.push(next_value(&mut rest)?);
            }
            left -= take;
            run.visit(&PlainKeys { keys, values });
        }
        if !rest.is_empty() {
            return None;
        }
        decoded += count;
    }
    Some(decoded)
}

/// What a plain search of key rows found (`find_key`): the offset within
/// the rows searched of the key sought, `None` where none of them holds it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct Searched(pub(super) Option<usize>);

/// Where `wanted` lies among rows `rows` of `chunk`, a `BYTE_ARRAY` key
/// column, read plain from `segments`, which hold the pages holding them:
/// the offset within `rows` of the key whose bytes equal `wanted`, if any
/// of the rows holds it (`Searched`). `None` when
/// the chunk is not of the shape the plain readers read (`plain_chunk`) or
/// any page holding the rows is not one `checked_run` reads exactly, for
/// the column reader to read instead (`read_plain`). Every value of each
/// page is walked by its length prefix, so a page is read whole, as
/// `checked_run` reads it, and a key never comes back from a page the
/// column reader would fault on.
pub(super) fn find_key(
    chunk: ColumnChunk<'_>,
    rows: Range<i64>,
    wanted: &[u8],
    segments: &Arc<Segments>,
) -> Option<Searched> {
    let rows = row_index(rows.start)..row_index(rows.end);
    read_plain(chunk, rows.clone(), segments, |pages, required| {
        find_in_pages(pages, rows, wanted, required)
    })
}

/// The data pages of `pages` holding `rows`, searched plain for `wanted`
/// (`find_key`): where among `rows` it lies, and the rows read; `None` at
/// the first page not read exactly. The rows read stop short of `rows`
/// where the pages run out first, for `ColumnChunk::check_pages_read` to
/// refuse.
fn find_in_pages(
    mut pages: ChunkPages,
    rows: Range<usize>,
    wanted: &[u8],
    required: bool,
) -> Option<(Searched, usize)> {
    let mut skip = rows.start.checked_sub(pages.first_row())?;
    let mut left = rows.len();
    let mut read = 0;
    let mut found = None;
    while left > 0 {
        // intentional: a page the page reader faults on is the column
        // reader's to report.
        let Some(page) = pages.get_next_page().ok()? else {
            break;
        };
        let (mut rest, count) = key_values(&page, required)?;
        let passed = skip.min(count);
        let here = (count - passed).min(left);
        let asked = passed..passed + here;
        for at in 0..count {
            let value = next_value(&mut rest)?;
            if found.is_none() && asked.contains(&at) && value == wanted {
                found = Some(read + at - passed);
            }
        }
        if !rest.is_empty() {
            return None;
        }
        skip -= passed;
        left -= here;
        read += here;
    }
    Some((Searched(found), read))
}

/// Rows `rows` of `chunk`, a value column, read plain from `segments`,
/// which hold the pages holding them: one entry per row, `None` for a
/// null. `None` when the chunk is not of the shape the plain readers read
/// (`plain_chunk`) or any page holding the rows is not one this reader
/// reads exactly, for the column reader to read instead. Exactly: every
/// data page `PLAIN` (`plain_page`); its definition levels, where the
/// column has them, read as parquet's level decoder reads them (`Levels`)
/// as far as the last row asked for on the page; a value for each
/// present level, each a length prefix and the bytes it counts, those of
/// the page's rows before the first asked for passed over by their
/// prefixes, and no more of them than the column reader decodes from the
/// page (`PlainPage::most_values`); and the pages together the ones
/// `ColumnChunk::check_pages_read` accepts for the rows. Nothing past the
/// last row asked for is read, as the column reader reads nothing past
/// it either. Anything the pages fault on ends this read with nothing
/// reported (`read_plain`).
pub(super) fn value_rows(
    chunk: ColumnChunk<'_>,
    rows: Range<i64>,
    segments: &Arc<Segments>,
) -> Option<PlainValues> {
    let rows = row_index(rows.start)..row_index(rows.end);
    read_plain(chunk, rows.clone(), segments, |pages, required| {
        let values = read_value_pages(pages, rows, required)?;
        let decoded = values.rows.len();
        Some((values, decoded))
    })
}

/// The data pages of `pages` holding `rows`, read plain (`value_rows`);
/// `None` at the first page not read exactly. The rows read stop short of
/// `rows` where the pages run out first, for
/// `ColumnChunk::check_pages_read` to refuse.
fn read_value_pages(
    mut pages: ChunkPages,
    rows: Range<usize>,
    required: bool,
) -> Option<PlainValues> {
    let mut skip = rows.start.checked_sub(pages.first_row())?;
    let mut left = rows.len();
    let mut read = PlainValues {
        pages: Vec::new(),
        rows: Vec::with_capacity(left),
    };
    while left > 0 {
        // intentional: a page the page reader faults on is the column
        // reader's to report.
        let Some(page) = pages.get_next_page().ok()? else {
            break;
        };
        let plain = plain_page(&page, required)?;
        let mut levels = match plain.levels {
            None => Levels::all_present(),
            Some(levels) => Levels::over(levels),
        };
        let mut rest = plain.values;
        let mut values = 0;
        let passed = skip.min(plain.count);
        let mut to_pass = passed;
        while to_pass > 0 {
            let (present, levels_read) = levels.stretch(to_pass)?;
            if present {
                for _ in 0..levels_read {
                    next_value(&mut rest)?;
                }
                values += levels_read;
            }
            to_pass -= levels_read;
        }
        skip -= passed;
        let here = (plain.count - passed).min(left);
        let held = read.pages.len();
        let mut to_read = here;
        while to_read > 0 {
            let (present, levels_read) = levels.stretch(to_read)?;
            if present {
                for _ in 0..levels_read {
                    let value = next_value(&mut rest)?;
                    let end = plain.values.len() - rest.len();
                    read.rows.push(Some((held, end - value.len()..end)));
                }
                values += levels_read;
            } else {
                read.rows.extend(std::iter::repeat_n(None, levels_read));
            }
            to_read -= levels_read;
        }
        if values > plain.most_values {
            return None;
        }
        if here > 0 {
            read.pages.push(page.buffer().slice_ref(plain.values));
        }
        left -= here;
    }
    Some(read)
}

/// The rows `value_rows` read, in order: each a range of the values of one
/// of the pages read, `None` for a null.
pub(super) struct PlainValues {
    /// The values of each page read, a slice of the page as decompressed.
    pages: Vec<Bytes>,
    /// Per row, the page among `pages` and the range of its values that
    /// the row's value is, `None` for a null.
    rows: Vec<Option<(usize, Range<usize>)>>,
}

impl PlainValues {
    /// The rows read.
    pub(super) fn len(&self) -> usize {
        self.rows.len()
    }

    /// The value of the `at`th row read, which is below `len`; `None` for a
    /// null.
    pub(super) fn value(&self, at: usize) -> Option<&[u8]> {
        self.rows[at]
            .as_ref()
            .map(|(page, range)| &self.pages[*page][range.clone()])
    }
}

/// A data page as the plain readers read it: `PLAIN` values after the
/// page's definition levels, when the column has them.
struct PlainPage<'p> {
    /// The RLE/bit-packed hybrid definition levels; `None` for a required
    /// column, whose pages hold none.
    levels: Option<&'p [u8]>,
    /// The page's bytes after its levels: its values.
    values: &'p [u8],
    /// The levels the header counts, one per row in a column neither
    /// nested nor repeated.
    count: usize,
    /// The most values the column reader decodes from the page: the
    /// header's count, less a v2 header's nulls, as parquet's column
    /// reader bounds its value decoder.
    most_values: usize,
}

/// Data page `page` as a `PlainPage`, when it is one the plain readers
/// read: `PLAIN`, and, where the column is not `required`, a v1 page's
/// levels RLE, a length, then that many bytes, ahead of the values, and a
/// v2 page's the header's definition level bytes, after its repetition
/// level bytes, which must be none, and its nulls no more than its values,
/// which parquet refuses otherwise. A v2 page of a required column holds
/// no level bytes. A dictionary page is not one.
fn plain_page(page: &Page, required: bool) -> Option<PlainPage<'_>> {
    match page {
        Page::DataPage {
            buf,
            num_values,
            encoding: Encoding::PLAIN,
            def_level_encoding,
            ..
        } => {
            let count = approx::length(*num_values);
            let page = |levels, values| PlainPage {
                levels,
                values,
                count,
                most_values: count,
            };
            if required {
                return Some(page(None, buf));
            }
            if *def_level_encoding != Encoding::RLE {
                return None;
            }
            let (length, rest) = buf.split_first_chunk::<LEVELS_LENGTH_BYTES>()?;
            let length = approx::length(u32::from_le_bytes(*length));
            let (levels, values) = rest.split_at_checked(length)?;
            Some(page(Some(levels), values))
        }
        Page::DataPageV2 {
            buf,
            num_values,
            encoding: Encoding::PLAIN,
            num_nulls,
            def_levels_byte_len,
            rep_levels_byte_len: 0,
            ..
        } => {
            let count = approx::length(*num_values);
            let most_values = count.checked_sub(approx::length(*num_nulls))?;
            let (levels, values) = buf.split_at_checked(approx::length(*def_levels_byte_len))?;
            if required && !levels.is_empty() {
                return None;
            }
            Some(PlainPage {
                levels: (!required).then_some(levels),
                values,
                count,
                most_values,
            })
        }
        Page::DataPage { .. } | Page::DataPageV2 { .. } | Page::DictionaryPage { .. } => None,
    }
}

/// The values of data page `page` and how many its header counts, when
/// the page is one `checked_run` reads: a `PlainPage` whose levels, where
/// the column has them, are one run of present values (`all_present`), and
/// whose header counts no null.
fn key_values(page: &Page, required: bool) -> Option<(&[u8], usize)> {
    let plain = plain_page(page, required)?;
    let present = plain
        .levels
        .is_none_or(|levels| all_present(levels, plain.count));
    (present && plain.most_values == plain.count).then_some((plain.values, plain.count))
}

/// Whether `levels`, the RLE/bit-packed hybrid definition levels of a
/// page of `count` values in a column whose levels take one bit, are
/// exactly one RLE run of `count` values at `PRESENT_LEVEL`: the run
/// header, a varint of the count shifted past the run-kind bit, which is
/// clear, then the level in one byte, and nothing after it.
fn all_present(levels: &[u8], count: usize) -> bool {
    let Some((level, header)) = levels.split_last() else {
        return false;
    };
    i16::from(*level) == PRESENT_LEVEL
        && whole_varint(header) == Some(approx::count(count) << RUN_KIND_BITS)
}

/// A page's definition levels in a column whose levels take one bit, read
/// as parquet's level decoder reads them, a stretch of equal levels at a
/// time (`stretch`): runs, each a
/// header, a varint of its count shifted past the run-kind bit, then for
/// an RLE run the level in one byte, and for a bit-packed run its count of
/// groups of `PACKED_GROUP_LEVELS` levels. A run is taken up whole, its
/// bytes all present, and only once the one before it is spent, so no
/// byte past the last level read is looked at, as parquet's decoder looks
/// at none.
struct Levels<'p> {
    /// The bytes after the run being read.
    rest: &'p [u8],
    run: Run<'p>,
}

/// The run of definition levels a `Levels` is reading.
enum Run<'p> {
    /// Levels `left` more times at one level, present or null: an RLE run,
    /// or a required column's levels, which are all present.
    Repeated { present: bool, left: usize },
    /// Bit-packed levels, one a bit from the lowest of the first byte; `at`
    /// of them are read.
    Packed { bits: &'p [u8], at: usize },
}

impl<'p> Levels<'p> {
    /// The levels of a required column's page: every level present.
    fn all_present() -> Self {
        Self {
            rest: &[],
            run: Run::Repeated {
                present: true,
                left: usize::MAX,
            },
        }
    }

    /// The levels `levels` hold, read from their first run.
    fn over(levels: &'p [u8]) -> Self {
        Self {
            rest: levels,
            run: Run::Repeated {
                present: false,
                left: 0,
            },
        }
    }

    /// The next levels, at most `most` of them and at least one where
    /// `most` is above zero, all present or all null: whether they are
    /// present, and how many. `None` where the levels end or their next
    /// run is not one `next_run` reads. An RLE run is given as one stretch,
    /// a bit-packed run a level at a time.
    fn stretch(&mut self, most: usize) -> Option<(bool, usize)> {
        loop {
            match &mut self.run {
                Run::Repeated { present, left } if *left > 0 => {
                    let levels = most.min(*left);
                    *left -= levels;
                    return Some((*present, levels));
                }
                Run::Packed { bits, at } if *at < bits.len() * PACKED_GROUP_LEVELS => {
                    let byte = bits[*at / PACKED_GROUP_LEVELS];
                    let level = (byte >> (*at % PACKED_GROUP_LEVELS)) & 1;
                    *at += 1;
                    return Some((i16::from(level) == PRESENT_LEVEL, 1));
                }
                Run::Repeated { .. } | Run::Packed { .. } => {
                    self.run = next_run(&mut self.rest)?;
                }
            }
        }
    }
}

/// The run of levels `rest` starts with, which then starts past it.
/// `None` where the header does not decode or counts no level, which
/// parquet's decoder takes for the end of the levels; where it counts more
/// levels than a `u32` holds, which parquet's decoder truncates the count
/// to; where an RLE run's level is neither `PRESENT_LEVEL` nor
/// `NULL_LEVEL`; and where `rest` ends before the run does.
fn next_run<'p>(rest: &mut &'p [u8]) -> Option<Run<'p>> {
    let (header, len) = leading_varint(rest)?;
    let after = rest.get(len..)?;
    let count = usize::try_from(header >> RUN_KIND_BITS).ok()?;
    let packed = header & 1 == 1;
    let levels = if packed {
        count.checked_mul(PACKED_GROUP_LEVELS)?
    } else {
        count
    };
    if levels == 0 || u32::try_from(levels).is_err() {
        return None;
    }
    if packed {
        let (bits, after) = after.split_at_checked(count)?;
        *rest = after;
        return Some(Run::Packed { bits, at: 0 });
    }
    let (&level, after) = after.split_first()?;
    let present = match i16::from(level) {
        PRESENT_LEVEL => true,
        NULL_LEVEL => false,
        _ => return None,
    };
    *rest = after;
    Some(Run::Repeated {
        present,
        left: count,
    })
}

/// The `PLAIN` byte array at the start of `rest`, which then starts past
/// it: `None` when `rest` holds less than its length prefix, or less after
/// the prefix than the bytes it counts.
fn next_value<'p>(rest: &mut &'p [u8]) -> Option<&'p [u8]> {
    let (prefix, after) = rest.split_first_chunk::<BYTE_ARRAY_LENGTH_PREFIX_BYTES>()?;
    let (value, after) = after.split_at_checked(approx::length(u32::from_le_bytes(*prefix)))?;
    *rest = after;
    Some(value)
}

/// One decode batch of a plain page's keys, in file order, each a slice of
/// the decompressed page.
struct PlainKeys<'p> {
    keys: ByteKeys,
    values: Vec<&'p [u8]>,
}

impl KeyBatch for PlainKeys<'_> {
    fn len(&self) -> usize {
        self.values.len()
    }

    fn is_utf8(&self) -> bool {
        match self.keys {
            ByteKeys::Text => self.values.iter().all(|value| is_utf8(value)),
            ByteKeys::Bytes => true,
        }
    }

    fn max_len(&self) -> usize {
        self.values
            .iter()
            .map(|value| value.len())
            .max()
            .unwrap_or(0)
    }

    fn key(&self, at: usize) -> Key {
        let value = self.values[at];
        match self.keys {
            ByteKeys::Text => Key::Str(String::from_utf8(value.to_vec()).expect(
                "RunCheck reads a key of a string batch back only once is_utf8 holds for it",
            )),
            ByteKeys::Bytes => Key::Bytes(value.to_vec()),
        }
    }

    fn first_not_increasing(&self) -> Option<(usize, Ordering)> {
        let at = self.values.windows(2).position(|pair| pair[0] >= pair[1])? + 1;
        Some((at, self.values[at].cmp(self.values[at - 1])))
    }
}

#[cfg(test)]
mod tests {
    use parquet::basic::Compression;
    use parquet::data_type::ByteArrayType;
    use parquet::file::metadata::{PageIndexPolicy, ParquetMetaData, ParquetMetaDataReader};
    use parquet::file::properties::WriterVersion;
    use rand::rngs::StdRng;
    use rand::{RngExt as _, SeedableRng as _};

    use super::*;
    use crate::error::Error;
    use crate::generation::testing::Dictionary;
    use crate::index::build::{ValueRows, decode_values, read_optional};
    use crate::index::testing::{Fixture, Layout, location};
    use crate::index::{Column, ValueShape, lies_within};
    use crate::key::KeyType;

    /// `values` as a `PLAIN` byte-array body: each length prefix, then its
    /// bytes.
    fn plain(values: &[&[u8]]) -> Vec<u8> {
        values
            .iter()
            .flat_map(|value| {
                let len = u32::try_from(value.len()).unwrap().to_le_bytes();
                len.into_iter().chain(value.iter().copied())
            })
            .collect()
    }

    /// One RLE run of `count` levels at `level`, as a writer encodes one
    /// of a single bit: the run header's varint, then the level's byte.
    fn run_of(count: u8, level: u8) -> Vec<u8> {
        vec![count << RUN_KIND_BITS, level]
    }

    fn v1(buf: Vec<u8>, count: u32, encoding: Encoding, levels: Encoding) -> Page {
        Page::DataPage {
            buf: Bytes::from(buf),
            num_values: count,
            encoding,
            def_level_encoding: levels,
            rep_level_encoding: Encoding::RLE,
            statistics: None,
        }
    }

    fn v2(buf: Vec<u8>, count: u32, def_levels: u32, nulls: u32, rep_levels: u32) -> Page {
        Page::DataPageV2 {
            buf: Bytes::from(buf),
            num_values: count,
            encoding: Encoding::PLAIN,
            num_nulls: nulls,
            num_rows: count,
            def_levels_byte_len: def_levels,
            rep_levels_byte_len: rep_levels,
            is_compressed: true,
            statistics: None,
        }
    }

    /// `levels` as a v1 page lays them out: their length, then the bytes.
    fn v1_levels(levels: &[u8]) -> Vec<u8> {
        let mut laid = u32::try_from(levels.len()).unwrap().to_le_bytes().to_vec();
        laid.extend_from_slice(levels);
        laid
    }

    /// Every value of `body` as `read_pages` reads a page of `count`, or
    /// `None` when it would leave the page to the column reader.
    fn read_exactly(mut body: &[u8], count: usize) -> Option<Vec<&[u8]>> {
        let values = (0..count)
            .map(|_| next_value(&mut body))
            .collect::<Option<Vec<_>>>()?;
        body.is_empty().then_some(values)
    }

    #[test]
    fn test_next_value_reads_exact_values_and_refuses_what_the_page_does_not_hold() {
        let values: [&[u8]; 3] = [b"a", b"", b"xyz"];
        let body = plain(&values);
        assert_eq!(read_exactly(&body, values.len()).unwrap(), values);
        assert_eq!(read_exactly(&[], 0).unwrap(), Vec::<&[u8]>::new());

        let mut past_end = body.clone();
        let last = body.len() - b"xyz".len() - BYTE_ARRAY_LENGTH_PREFIX_BYTES;
        past_end[last] += 1;
        assert_eq!(
            read_exactly(&past_end, values.len()),
            None,
            "one byte past the end"
        );
        let prefix_cut = &body[..last + BYTE_ARRAY_LENGTH_PREFIX_BYTES - 1];
        assert_eq!(
            read_exactly(prefix_cut, values.len()),
            None,
            "a prefix cut short"
        );
        let mut trailing = body.clone();
        trailing.push(0);
        assert_eq!(
            read_exactly(&trailing, values.len()),
            None,
            "a byte after the last"
        );
        assert_eq!(
            read_exactly(&body, values.len() + 1),
            None,
            "a value more than held"
        );
        assert_eq!(
            read_exactly(&body, values.len() - 1),
            None,
            "a value fewer than held"
        );
        let mut huge = body.clone();
        huge[..BYTE_ARRAY_LENGTH_PREFIX_BYTES].copy_from_slice(&u32::MAX.to_le_bytes());
        assert_eq!(
            read_exactly(&huge, values.len()),
            None,
            "a prefix of u32::MAX"
        );
    }

    #[test]
    fn test_all_present_takes_one_run_of_the_count_at_the_present_level_only() {
        let count = 5;
        let present = u8::try_from(PRESENT_LEVEL).unwrap();
        assert!(all_present(&run_of(count, present), usize::from(count)));
        assert!(
            !all_present(&run_of(count, 0), usize::from(count)),
            "a run of nulls"
        );
        assert!(!all_present(
            &run_of(count, present),
            usize::from(count) + 1
        ));
        assert!(!all_present(
            &run_of(count, present),
            usize::from(count) - 1
        ));
        let mut padded = run_of(count, present);
        padded.push(0);
        assert!(
            !all_present(&padded, usize::from(count)),
            "a byte after the run"
        );
        let bit_packed = [(1 << RUN_KIND_BITS) | 1, u8::MAX];
        assert!(
            !all_present(&bit_packed, usize::from(count)),
            "a bit-packed run"
        );
        assert!(!all_present(&[], 0), "no run at all");
        // A run of 300 has a header of two varint bytes: 600 is 0xd8 0x04.
        let long_count = 300;
        let long_run = [0xd8, 0x04, present];
        assert!(all_present(&long_run, long_count));
        assert!(
            !all_present(&long_run[1..], long_count),
            "a varint cut short"
        );
        assert!(
            !all_present(&[0xd8, present], long_count),
            "a varint that does not end"
        );
    }

    #[test]
    fn test_key_values_reads_plain_pages_whose_levels_are_all_present() {
        let values: [&[u8]; 2] = [b"a", b"b"];
        let count = u8::try_from(values.len()).unwrap();
        let body = plain(&values);
        let present = u8::try_from(PRESENT_LEVEL).unwrap();
        let levels = run_of(count, present);
        let values_of = |page: &Page, required| {
            key_values(page, required).map(|(values, count)| (values.to_vec(), count))
        };
        let expected = Some((body.clone(), values.len()));
        let with = |prefix: &[u8]| [prefix, &body].concat();
        let n = u32::from(count);

        assert_eq!(
            values_of(&v1(body.clone(), n, Encoding::PLAIN, Encoding::RLE), true),
            expected
        );
        let nullable = v1(with(&v1_levels(&levels)), n, Encoding::PLAIN, Encoding::RLE);
        assert_eq!(values_of(&nullable, false), expected);
        let with_null = v1(
            with(&v1_levels(&run_of(count, 0))),
            n,
            Encoding::PLAIN,
            Encoding::RLE,
        );
        assert_eq!(values_of(&with_null, false), None, "levels not all present");
        let other_levels = v1(
            with(&v1_levels(&levels)),
            n,
            Encoding::PLAIN,
            Encoding::PLAIN,
        );
        assert_eq!(values_of(&other_levels, false), None, "levels not RLE");
        let dictionary = v1(body.clone(), n, Encoding::RLE_DICTIONARY, Encoding::RLE);
        assert_eq!(values_of(&dictionary, true), None, "values not PLAIN");
        let mut levels_past_end = v1_levels(&levels);
        levels_past_end[0] = u8::MAX;
        let past = v1(with(&levels_past_end), n, Encoding::PLAIN, Encoding::RLE);
        assert_eq!(
            values_of(&past, false),
            None,
            "a level length past the page"
        );
        let no_length = v1(
            vec![0; LEVELS_LENGTH_BYTES - 1],
            n,
            Encoding::PLAIN,
            Encoding::RLE,
        );
        assert_eq!(
            values_of(&no_length, false),
            None,
            "a level length cut short"
        );

        let levels_len = u32::try_from(levels.len()).unwrap();
        assert_eq!(values_of(&v2(body.clone(), n, 0, 0, 0), true), expected);
        assert_eq!(
            values_of(&v2(with(&levels), n, levels_len, 0, 0), false),
            expected
        );
        assert_eq!(
            values_of(&v2(with(&levels), n, levels_len, 0, 0), true),
            None,
            "levels a required column has none of"
        );
        assert_eq!(
            values_of(&v2(with(&levels), n, levels_len, 1, 0), false),
            None,
            "a null counted"
        );
        assert_eq!(
            values_of(&v2(with(&levels), n, levels_len, 0, 1), false),
            None,
            "repetition levels"
        );
        assert_eq!(
            values_of(&v2(body.clone(), n, u32::MAX, 0, 0), false),
            None,
            "levels past the page"
        );
        let dictionary_page = Page::DictionaryPage {
            buf: Bytes::from(body),
            num_values: n,
            encoding: Encoding::PLAIN,
            is_sorted: true,
        };
        assert_eq!(values_of(&dictionary_page, true), None);
    }

    /// A v2 page bounds the values the column reader decodes by its nulls,
    /// and one counting more nulls than values is refused, as parquet
    /// refuses it; a v1 page bounds them by its count alone.
    #[test]
    fn test_plain_page_bounds_the_values_by_a_v2_pages_nulls() {
        let count = 4;
        let nulls = 1;
        let levels = run_of(u8::try_from(count).unwrap(), 0);
        let levels_len = u32::try_from(levels.len()).unwrap();
        let most = |page: &Page| plain_page(page, false).map(|plain| plain.most_values);
        assert_eq!(
            most(&v2(levels.clone(), count, levels_len, nulls, 0)),
            Some(approx::length(count - nulls))
        );
        assert_eq!(
            most(&v2(levels.clone(), count, levels_len, count + 1, 0)),
            None
        );
        let v1_page = v1(v1_levels(&levels), count, Encoding::PLAIN, Encoding::RLE);
        assert_eq!(most(&v1_page), Some(approx::length(count)));
    }

    /// A run of `count` levels at `level` written as an RLE run header and
    /// its level, for counts whose header takes more than one byte.
    fn rle_run(count: u64, level: u8) -> Vec<u8> {
        let mut run = varint_of(count << RUN_KIND_BITS);
        run.push(level);
        run
    }

    /// `groups` bit-packed groups of `bits`, one byte a group.
    fn packed_run(bits: &[u8]) -> Vec<u8> {
        let groups = u64::try_from(bits.len()).unwrap();
        let mut run = varint_of((groups << RUN_KIND_BITS) | 1);
        run.extend_from_slice(bits);
        run
    }

    fn varint_of(mut value: u64) -> Vec<u8> {
        let mut bytes = Vec::new();
        loop {
            let low = u8::try_from(value & u64::from(crate::varint::PAYLOAD)).unwrap();
            value >>= crate::varint::PAYLOAD_BITS;
            if value == 0 {
                bytes.push(low);
                return bytes;
            }
            bytes.push(low | crate::varint::CONTINUES);
        }
    }

    /// The first `count` levels of `levels` as `Levels` reads them, a
    /// stretch at a time, `None` where it stops short.
    fn read_levels(levels: &[u8], count: usize) -> Option<Vec<bool>> {
        let mut read = Levels::over(levels);
        let mut got = Vec::new();
        while got.len() < count {
            let (present, levels) = read.stretch(count - got.len())?;
            assert!(levels > 0, "a stretch of no level");
            got.extend(std::iter::repeat_n(present, levels));
        }
        Some(got)
    }

    #[test]
    fn test_levels_read_rle_and_bit_packed_runs_as_parquet_writes_them() {
        let present = u8::try_from(PRESENT_LEVEL).unwrap();
        let null = u8::try_from(NULL_LEVEL).unwrap();
        // Three present, then a group of eight packed lowest bit first,
        // then two nulls.
        let packed = 0b1010_0110;
        let levels = [rle_run(3, present), packed_run(&[packed]), rle_run(2, null)].concat();
        let mut expected = vec![true; 3];
        expected.extend((0..PACKED_GROUP_LEVELS).map(|bit| (packed >> bit) & 1 == 1));
        expected.extend([false; 2]);
        assert_eq!(read_levels(&levels, expected.len()), Some(expected.clone()));
        assert_eq!(
            read_levels(&levels, expected.len() + 1),
            None,
            "a level past the runs"
        );
        // A packed run's levels past the page's count are padding, never
        // read, and nothing past the levels read is looked at.
        let padded = [packed_run(&[packed]), vec![u8::MAX]].concat();
        assert_eq!(
            read_levels(&padded, 3),
            Some(vec![false, true, true]),
            "the levels a page counts of a padded group"
        );
        let long = 300;
        assert_eq!(
            read_levels(&rle_run(long, present), usize::try_from(long).unwrap()),
            Some(vec![true; usize::try_from(long).unwrap()])
        );
        let most = 5;
        assert_eq!(Levels::all_present().stretch(most), Some((true, most)));

        assert_eq!(read_levels(&rle_run(0, present), 1), None, "a run of none");
        assert_eq!(read_levels(&packed_run(&[]), 1), None, "no group");
        let two = u8::try_from(PRESENT_LEVEL + 1).unwrap();
        assert_eq!(read_levels(&rle_run(3, two), 1), None, "a level of 2");
        assert_eq!(
            read_levels(&rle_run(3, present)[..1], 1),
            None,
            "an RLE run without its level"
        );
        let mut short = packed_run(&[packed, packed]);
        short.pop();
        assert_eq!(read_levels(&short, 1), None, "a packed run cut short");
        let past_u32 = u64::from(u32::MAX) + 1;
        assert_eq!(
            read_levels(&rle_run(past_u32, present), 1),
            None,
            "a count parquet truncates"
        );
        let groups_past_u32 = varint_of(((past_u32 / 8) << RUN_KIND_BITS) | 1);
        assert_eq!(read_levels(&groups_past_u32, 1), None);
        assert_eq!(read_levels(&[], 1), None, "no run at all");
    }

    /// Rows of the fixtures the value reader is held to the column reader
    /// on, in row groups of `COMPARED_GROUP_ROWS` and pages of
    /// `COMPARED_PAGE_ROWS`: two row groups of several pages each, the last
    /// shorter than the others.
    const COMPARED_ROWS: usize = 200;
    const COMPARED_GROUP_ROWS: usize = 120;
    const COMPARED_PAGE_ROWS: usize = 16;
    /// A page byte bound that cuts pages of the compared values at uneven
    /// row counts.
    const COMPARED_PAGE_BYTES: usize = 256;
    /// The write batch under that bound, which the writer checks between
    /// batches.
    const COMPARED_WRITE_BATCH: usize = 4;
    /// The longest value the compared fixtures draw; one of every
    /// `EMPTY_ODDS` is empty besides.
    const LONGEST_VALUE: usize = 40;
    const EMPTY_ODDS: u32 = 5;
    /// Rows per stretch of the compared fixtures' nulls: none in the first,
    /// one of every `NULL_ODDS` in the second, which the writer bit-packs,
    /// and all but every `PRESENT_EVERY`th in the third, which it writes as
    /// runs.
    const NULL_STRETCH_ROWS: usize = 64;
    const NULL_ODDS: u32 = 3;
    const PRESENT_EVERY: usize = 20;
    /// The lengths of the row ranges compared from each first row, the
    /// last running to the row group's end.
    const RANGE_LENGTHS: [usize; 5] = [1, 2, 7, 17, 40];

    /// Random values of random lengths, some empty, and, when `nulls`,
    /// nulls in stretches of `NULL_STRETCH_ROWS`.
    fn drawn_values(seed: u64, nulls: bool) -> Vec<Option<Vec<u8>>> {
        let mut rng = StdRng::seed_from_u64(seed);
        (0..COMPARED_ROWS)
            .map(|row| {
                let null = nulls
                    && match row / NULL_STRETCH_ROWS {
                        0 => false,
                        1 => rng.random_range(0..NULL_ODDS) == 0,
                        _ => row % PRESENT_EVERY != 0,
                    };
                let len = if rng.random_range(0..EMPTY_ODDS) == 0 {
                    0
                } else {
                    rng.random_range(0..=LONGEST_VALUE)
                };
                (!null).then(|| (0..len).map(|_| rng.random::<u8>()).collect())
            })
            .collect()
    }

    /// A layout edit a compared fixture is written under, named for the
    /// messages, and whether it leaves the value column one the value
    /// reader reads: `PLAIN` pages and no dictionary.
    type Edit = (&'static str, fn(&mut Layout), bool);

    const VALUE_EDITS: &[Edit] = &[
        ("v1 zstd", |_| {}, true),
        (
            "v1 uncompressed",
            |layout| layout.compression = Compression::UNCOMPRESSED,
            true,
        ),
        (
            "v1 snappy",
            |layout| layout.compression = Compression::SNAPPY,
            true,
        ),
        (
            "v1 pages cut by bytes",
            |layout| {
                layout.page_bytes = COMPARED_PAGE_BYTES;
                layout.write_batch = COMPARED_WRITE_BATCH;
            },
            true,
        ),
        (
            "v2 plain zstd",
            |layout| {
                layout.writer_version = WriterVersion::PARQUET_2_0;
                layout.value_encoding = Some(Encoding::PLAIN);
            },
            true,
        ),
        (
            "v2 plain uncompressed",
            |layout| {
                layout.writer_version = WriterVersion::PARQUET_2_0;
                layout.value_encoding = Some(Encoding::PLAIN);
                layout.compression = Compression::UNCOMPRESSED;
            },
            true,
        ),
        (
            "v2 delta",
            |layout| layout.writer_version = WriterVersion::PARQUET_2_0,
            false,
        ),
        (
            "dictionary",
            |layout| layout.dictionary = Dictionary::Value,
            false,
        ),
    ];

    /// The fixture of `values` under `edit`: long keys, `COMPARED_ROWS` of
    /// them, the value column nullable where a value is null.
    fn compared_file(values: &[Option<Vec<u8>>], edit: fn(&mut Layout)) -> Bytes {
        let mut fixture = Fixture::contract(KeyType::Long, COMPARED_ROWS);
        fixture.values = values.to_vec();
        fixture.layout.row_group_rows = COMPARED_GROUP_ROWS;
        fixture.layout.page_rows = COMPARED_PAGE_ROWS;
        fixture.layout.write_batch = COMPARED_PAGE_ROWS;
        edit(&mut fixture.layout);
        fixture.write()
    }

    fn footer(file: &Bytes) -> ParquetMetaData {
        ParquetMetaDataReader::new()
            .with_page_index_policy(PageIndexPolicy::Required)
            .parse_and_finish(file)
            .unwrap()
    }

    /// A value as compared: its bytes, and whether it shares the file's
    /// buffer rather than being a copy (`value_bytes`).
    type Compared = Option<(Vec<u8>, bool)>;

    fn compared(value: Option<Bytes>, file: &Bytes) -> Compared {
        value.map(|value| (value.to_vec(), lies_within(file, &value)))
    }

    /// Rows `rows` of `chunk` as parquet's column reader decodes them,
    /// each kept as `decode_values` keeps it.
    fn by_column_reader(
        chunk: ColumnChunk<'_>,
        rows: Range<i64>,
        segments: &Arc<Segments>,
        file: &Bytes,
    ) -> Vec<Compared> {
        read_optional::<ByteArrayType>(chunk, rows, Arc::clone(segments))
            .unwrap()
            .iter()
            .map(|value| {
                let kept = value
                    .as_ref()
                    .map(|value| crate::index::value_bytes(segments, value.data()));
                compared(kept, file)
            })
            .collect()
    }

    /// Every range of `RANGE_LENGTHS` from every first row of every row
    /// group of `file`'s value column, read by `decode_values` and by the
    /// column reader: the same values, kept the same way, which are the
    /// values written; read plain where `plain`, by the column reader
    /// otherwise. The ranges compared.
    fn assert_value_rows_match_the_column_reader(
        file: &Bytes,
        written: &[Option<Vec<u8>>],
        plain: bool,
        case: &str,
    ) -> usize {
        let metadata = footer(file);
        let path = location();
        let column = Column {
            metadata: &metadata,
            leaf: VALUE_LEAF,
            path: &path,
        };
        let size = approx::count(file.len());
        let segments = Arc::new(Segments::new(vec![(0..size, file.clone())], size));
        let mut compared_ranges = 0;
        let mut group_start = 0;
        for row_group in 0..metadata.num_row_groups() {
            let chunk = column.group(row_group);
            let rows = chunk.rows();
            for start in 0..rows {
                for len in RANGE_LENGTHS {
                    let end = (start + i64::try_from(len).unwrap()).min(rows);
                    let range = start..end;
                    let case = format!("{case}: row group {row_group} rows {range:?}");
                    let decoded =
                        decode_values(chunk, ValueShape::Variable, range.clone(), &segments)
                            .unwrap();
                    assert_eq!(
                        matches!(decoded, ValueRows::Plain { .. }),
                        plain,
                        "{case}: the decoder"
                    );
                    let got: Vec<Compared> = (0..decoded.len())
                        .map(|at| compared(decoded.value(at), file))
                        .collect();
                    let expected = by_column_reader(chunk, range.clone(), &segments, file);
                    assert_eq!(got, expected, "{case}");
                    let first = group_start + row_index(range.start);
                    let values: Vec<Option<Vec<u8>>> = got
                        .into_iter()
                        .map(|value| value.map(|(bytes, _)| bytes))
                        .collect();
                    assert_eq!(values, written[first..first + values.len()], "{case}");
                    compared_ranges += 1;
                }
            }
            group_start += row_index(rows);
        }
        compared_ranges
    }

    /// The fixture's value column.
    const VALUE_LEAF: usize = 1;

    /// Seeds of the drawn values, one per fixture.
    const VALUE_SEEDS: u64 = 7;

    /// Random values, empty ones among them, with and without nulls, under
    /// every layout of `VALUE_EDITS`, read over ranges that start at every
    /// row, so on every page with a prefix of rows passed over: the plain
    /// reader reads exactly what the column reader decodes, kept the same
    /// way, a copy from a compressed page and a slice of the file from an
    /// uncompressed one.
    #[test]
    fn test_value_rows_matches_the_column_reader_on_random_values() {
        let mut shared = 0;
        for (name, edit, plain) in VALUE_EDITS {
            for nulls in [false, true] {
                let values = drawn_values(VALUE_SEEDS + u64::from(nulls), nulls);
                assert!(values.iter().flatten().any(Vec::is_empty), "an empty value");
                assert_eq!(values.iter().any(Option::is_none), nulls);
                let file = compared_file(&values, *edit);
                let case = format!("{name}, nulls {nulls}");
                let compared_ranges =
                    assert_value_rows_match_the_column_reader(&file, &values, *plain, &case);
                assert_eq!(
                    compared_ranges,
                    COMPARED_ROWS * RANGE_LENGTHS.len(),
                    "{case}"
                );
                let metadata = footer(&file);
                let chunk = metadata.row_group(0).column(VALUE_LEAF);
                if chunk.compression() == Compression::UNCOMPRESSED {
                    shared += 1;
                }
            }
        }
        assert!(shared > 0, "some layout kept values as slices of the file");
    }

    /// Every value empty, or every value null: pages whose values take no
    /// bytes, and pages that hold no value at all.
    #[test]
    fn test_value_rows_reads_pages_of_empty_values_and_of_nulls_alone() {
        let empty = vec![Some(Vec::new()); COMPARED_ROWS];
        let mut nulls = vec![None; COMPARED_ROWS];
        nulls[0] = Some(b"one".to_vec());
        for (name, values) in [("empty", empty), ("nulls", nulls)] {
            for (edit_name, edit, plain) in VALUE_EDITS {
                let file = compared_file(&values, *edit);
                let case = format!("{name}, {edit_name}");
                assert_value_rows_match_the_column_reader(&file, &values, *plain, &case);
            }
        }
    }

    /// A page the value reader does not read exactly leaves the rows to
    /// the column reader, which reports the fault: here a value's length
    /// prefix one past the page's end, in a page of a required column
    /// stored uncompressed, so the edit lands in the page's values.
    #[test]
    fn test_value_rows_leaves_a_page_it_cannot_read_to_the_column_reader() {
        let values: Vec<Option<Vec<u8>>> =
            (0..COMPARED_ROWS).map(|_| Some(vec![1, 2, 3])).collect();
        let file = compared_file(&values, |layout| {
            layout.compression = Compression::UNCOMPRESSED;
        });
        let metadata = footer(&file);
        let path = location();
        let chunk = Column {
            metadata: &metadata,
            leaf: VALUE_LEAF,
            path: &path,
        }
        .group(0);
        let range = chunk.page_range(0);
        let header = crate::index::page_header::PageHeader::read(
            &file[approx::length(range.start)..approx::length(range.end)],
        )
        .unwrap();
        let first_prefix = approx::length(range.start) + header.header_len;
        let mut edited = file.to_vec();
        let page_end = approx::length(range.end);
        let past = u32::try_from(page_end - first_prefix).unwrap();
        edited[first_prefix..first_prefix + BYTE_ARRAY_LENGTH_PREFIX_BYTES]
            .copy_from_slice(&past.to_le_bytes());
        let edited = Bytes::from(edited);
        let size = approx::count(edited.len());
        let segments = Arc::new(Segments::new(vec![(0..size, edited)], size));
        let rows = chunk.page_rows(0);
        assert!(value_rows(chunk, rows.clone(), &segments).is_none());
        let error = decode_values(chunk, ValueShape::Variable, rows, &segments)
            .err()
            .expect("the column reader meets the fault");
        assert!(matches!(error, Error::Parquet(_)), "{error:?}");
    }
}
