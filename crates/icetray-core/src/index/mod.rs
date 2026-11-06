//! The per-file resident index (RFD 004, RFD 006, RFD 017): the footer and
//! page indexes as kept, the exact first and last key of the file and of
//! every row group, the fence array (the first key of every key page), and
//! per residency tier the key chunks in memory, a handle on them in a local
//! chunk file, or the key column's bloom filters. A lookup chooses its row
//! group and key page from those exact keys and never from column
//! statistics, decodes exactly one key page with parquet's column reader
//! over bytes it already holds, and fetches one value page through the
//! page cache for each value column a hit names (RFD 022); a miss inside
//! the file's range fetches nothing.

mod build;
mod inspect;
mod order_fences;
mod ordered;
mod page_header;
mod pages;
mod pairs;
mod plain;
mod record;
mod run;
#[cfg(test)]
pub(crate) mod testing;

use std::cmp::Ordering;
use std::mem::size_of;
use std::ops::Range;
use std::panic::UnwindSafe;
use std::sync::Arc;

use bytes::Bytes;
use parquet::basic::Type as PhysicalType;
use parquet::bloom_filter::Sbbf;
use parquet::data_type::ByteArray;
use parquet::file::metadata::{ColumnChunkMetaData, ParquetMetaData};
use parquet::file::page_index::offset_index::PageLocation;
use parquet::schema::types::ColumnDescPtr;

use crate::approx;
use crate::cache::{Issue, PageCache, PageOrigin};
use crate::config::Residency;
use crate::deletes::DeleteFault;
use crate::error::{Error, Recurrence, Recurs, RefusalReason, Result};
use crate::key::{Key, KeyType, UUID_BYTES};
use crate::local::{ChunkRules, DataFileIdentity, DiskChunks};
use crate::manifest::TableLayout;
use crate::order::OrderFacts;
use crate::parquet_io::{FileHandle, Segments};
use crate::row_cache::RowKey;
use build::{check_utf8, checked_values, decode_run, fetch_chunk_parts, find_in_run};
pub(crate) use inspect::Inspected;
use inspect::{Layout, ResidentSet, column_chunk_ranges, kept_footer, tiers};
pub use order_fences::{OrderFences, Pair, PairPart};
use ordered::OrderVerified;
pub(crate) use page_header::check_chunk_page_headers;
pub use page_header::{ChunkPage, HeaderField, MOST_VALUE_PAGE_BYTES, PageHeaderFault, WireType};
pub(crate) use pairs::{PairChunks, PairRead, Pairs};
pub(crate) use run::RunSource;

/// A row's position in a file: its row group and its row within that group.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) struct RowRef {
    /// The row group holding the row.
    pub row_group: usize,
    /// The row's index within the row group.
    pub row: i64,
}

/// A row's position in a data file counted from the file's first row
/// across every row group: how Iceberg's position deletes and deletion
/// vectors name a row (RFD 021). A newtype, so that it is never taken for a
/// row within one row group.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub(crate) struct RowPosition(pub(crate) u64);

/// The rows of one data file a read counts as deleted, held resident and
/// asked with no I/O (RFD 021): row by row, and whether a whole range of
/// rows is, so a Scan passes over a page of deleted rows in one question.
pub(crate) trait DeletedRows: Sync {
    /// Whether the row at `position` is deleted.
    fn deleted(&self, position: RowPosition) -> bool;

    /// Whether every row of `positions` is deleted; true for none.
    fn all_deleted(&self, positions: Range<RowPosition>) -> bool;
}

/// A test's deleted rows as a predicate on positions, whose ranges are
/// asked row by row.
#[cfg(test)]
impl<F: Fn(RowPosition) -> bool + Sync> DeletedRows for F {
    fn deleted(&self, position: RowPosition) -> bool {
        self(position)
    }

    fn all_deleted(&self, positions: Range<RowPosition>) -> bool {
        (positions.start.0..positions.end.0).all(|position| self(RowPosition(position)))
    }
}

/// What makes a data file unservable beyond the contract's refusals. The set
/// is closed; a fault outside it is a library error, not a file fault.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum FileFault {
    /// The file has no offset index on the key, the order, or a value
    /// column; page locations are how a lookup names the one page it
    /// decodes, and how a Scan names the order pages it reads.
    #[error("no page index")]
    MissingPageIndex,
    /// The file has no rows, so it has no key range and cannot be routed to.
    #[error("no rows")]
    NoRows,
    /// The footer's row counts disagree with one another: the file's or a
    /// row group's count is negative, or the row groups do not sum to the
    /// file's.
    #[error("the footer's row counts disagree with one another")]
    InconsistentRowCounts,
    /// A column's offset index does not describe its pages in a row group:
    /// the column chunk the pages lie in has a negative offset or size or
    /// ends past the file's last byte, a row group with rows has no page, a page has a negative offset or
    /// size or lies outside its column chunk, the first page does not start
    /// at row zero, or the pages' first rows are not strictly increasing
    /// within the row group.
    #[error("the offset index of row group {row_group} does not describe its pages")]
    InconsistentOffsetIndex {
        /// The row group whose offset index is inconsistent.
        row_group: usize,
    },
    /// A data page holds a different number of rows than the offset index
    /// gives it, by its header's count or by what it decodes, so no row
    /// position in that row group can be trusted.
    #[error(
        "page {page} of row group {row_group} holds a different number of rows than its offset index says"
    )]
    PageRowsMismatch {
        /// The row group holding the page.
        row_group: usize,
        /// The page, counted among the column's data pages in the row group.
        page: usize,
    },
    /// A key page's header, an order page's at load, or that of a value
    /// page or value dictionary page a lookup fetched, does not decode as
    /// the compact-thrift `PageHeader` parquet reads it as, or is not a
    /// header of the page its place in the chunk holds; checked before
    /// parquet reads it (RFD 004).
    #[error("the header of {page} of row group {row_group} does not decode: {fault}")]
    MalformedPageHeader {
        /// The row group holding the page.
        row_group: usize,
        /// The page.
        page: ChunkPage,
        /// What is wrong with its header.
        fault: PageHeaderFault,
    },
    /// A page's header declares more than its page can hold: for a key
    /// page, an uncompressed size above what its rows could encode to at
    /// the key cap or above the key page ceiling of 64 MiB; for a value
    /// page or value dictionary page a lookup fetched, or an order page or
    /// order dictionary page at load, an uncompressed size above the value
    /// page ceiling of 64 MiB; for any, a dictionary of more values than
    /// the row group has rows or than the ceiling holds at
    /// the larger of the fewest bytes an entry encodes to and the bytes
    /// parquet holds a decoded entry in, a compressed size past the page's
    /// place in the chunk, or v2 level lengths whose sum is above the
    /// uncompressed size. Parquet sizes a buffer by the first two before it
    /// reads a byte of the page, and adds the level lengths as `i32`s, so
    /// the header is refused before parquet reads it (RFD 004).
    #[error(
        "{page} of row group {row_group} declares {field} {declared}, above the {allowed} it can hold"
    )]
    PageHeaderOverBound {
        /// The row group holding the page.
        row_group: usize,
        /// The page.
        page: ChunkPage,
        /// The header field that declares too much.
        field: HeaderField,
        /// What the header declares.
        declared: u64,
        /// The most the page can hold.
        allowed: u64,
    },
    /// A row position lies outside every page of its row group.
    #[error("row {row} is outside every page of row group {row_group}")]
    PageOutOfRange {
        /// The row position that no page covers.
        row: i64,
        /// The row group it was looked for in.
        row_group: usize,
    },
    /// A bloom filter that does not decode from exactly the bytes the
    /// footer bounds, or decodes to a bitset of no block.
    #[error("bloom filter of row group {row_group} does not decode")]
    CorruptBloomFilter {
        /// The row group whose filter is unreadable.
        row_group: usize,
    },
    /// The footer names a row group's bloom filter but places it nowhere the
    /// file holds: a negative offset, no length, a negative length, or an
    /// end past the file's last byte, so the filter cannot be read as one
    /// range of the file. A footer with no filter at all is a refusal, not a
    /// fault.
    #[error("bloom filter of row group {row_group} has no place the footer can hold")]
    UnplaceableBloomFilter {
        /// The row group whose filter the footer cannot place.
        row_group: usize,
    },
    /// A string key column holds a value that is not UTF-8. A `Key::Str` is
    /// text, and a lossy conversion would serve a different key than the
    /// one written.
    #[error("a key of row group {row_group} is not utf-8")]
    KeyNotUtf8 {
        /// The row group holding the value.
        row_group: usize,
    },
    /// A `string` order column holds a value that is not UTF-8 (RFD 025),
    /// as `KeyNotUtf8` is for a key: an `OrderValue::Str` is text.
    #[error("an order value of row group {row_group} is not utf-8")]
    OrderNotUtf8 {
        /// The row group holding the value.
        row_group: usize,
    },
    /// The order fences of a row group hold more bytes together than the
    /// `u32` offsets the index addresses them by reach (RFD 025, Load): a
    /// row group of more than about 2^20 order pages whose first keys are
    /// string or binary keys of `contract::MAX_KEY_BYTES`, or of dozens of
    /// pages whose first order values share tens of mebibytes. A long key
    /// and an integer order value are held as numbers, which never
    /// overflow.
    #[error("the order fences of row group {row_group} hold more than 4 GiB")]
    OrderFencesOverflow {
        /// The row group whose fences overflow.
        row_group: usize,
    },
    /// A value column's physical type is neither `BYTE_ARRAY` nor
    /// `FIXED_LEN_BYTE_ARRAY`. The snapshot's schema admitted the column as
    /// binary, fixed, or string (`TableLayout::derive` refuses anything else
    /// as `RefusalReason::ValueTypeUnsupported`), and a spec-conformant
    /// writer stores those three as byte arrays, so a file that does not is
    /// malformed.
    #[error("value column is stored as {got}, not a byte array")]
    ValuePhysicalType {
        /// The physical type the file stores the column as.
        got: PhysicalType,
    },
    /// The value column is a `FIXED_LEN_BYTE_ARRAY` of width 0, which holds
    /// no byte of any value and on which parquet's decoder asserts a width
    /// above zero when it decodes a value, so a lookup would panic there.
    #[error("value column is a FIXED_LEN_BYTE_ARRAY of width 0")]
    ZeroWidthValue,
    /// Parquet panicked decoding part of the file. Parquet panics, in
    /// release builds too, on malformed input no check ahead of it reads:
    /// a byte-array page whose body holds fewer values than its header
    /// counts, among others. The footer load, a bloom filter's parse, and
    /// the decode of a column chunk's pages run under `catch_unwind`, so
    /// such a panic ends that load or that lookup with this fault instead
    /// of the task running it.
    #[error("parquet panicked decoding {part}: {message}")]
    DecoderPanicked {
        /// What parquet was decoding.
        part: DecodedPart,
        /// The panic's message, for a person reading the log; nothing
        /// decides on it.
        message: String,
    },
    /// A delete file, or a data file's deletes, is unservable (RFD 021).
    #[error("{0}")]
    Delete(DeleteFault),
}

/// A fault the footer shows, or a bloom filter read from the data file,
/// is fixed for the file. A fault met decoding a page is not known to be:
/// the page may be a key chunk the Disk tier reads back from its chunk
/// file, for the overlap check or a lookup, with no checksum on that read,
/// and a chunk file changed on disk is not the data file. The fault does
/// not say which bytes it met.
impl Recurs for FileFault {
    fn recurrence(&self) -> Recurrence {
        match self {
            Self::MissingPageIndex
            | Self::NoRows
            | Self::InconsistentRowCounts
            | Self::InconsistentOffsetIndex { .. }
            | Self::UnplaceableBloomFilter { .. }
            | Self::ValuePhysicalType { .. }
            | Self::ZeroWidthValue
            // Bloom filters are read from the data file at every load and
            // never kept on local disk.
            | Self::CorruptBloomFilter { .. }
            // Parquet panics on the same bytes the same way, and the footer,
            // the bloom filters, and a position delete file are always the
            // file's own bytes.
            | Self::DecoderPanicked {
                part:
                    DecodedPart::Footer
                    | DecodedPart::BloomFilter { .. }
                    | DecodedPart::PositionDeletes,
                ..
            } => Recurrence::Deterministic,
            Self::Delete(fault) => fault.recurrence(),
            Self::PageRowsMismatch { .. }
            | Self::MalformedPageHeader { .. }
            | Self::PageHeaderOverBound { .. }
            | Self::KeyNotUtf8 { .. }
            | Self::OrderNotUtf8 { .. }
            // Found decoding the order pages, which may be a local copy.
            | Self::OrderFencesOverflow { .. }
            | Self::DecoderPanicked {
                part: DecodedPart::Pages { .. },
                ..
            }
            // A row a key page found, and that page may be a local copy.
            | Self::PageOutOfRange { .. } => Recurrence::Transient,
        }
    }
}

/// The part of a file parquet was decoding when it panicked.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DecodedPart {
    /// The footer: the thrift metadata and the page indexes.
    Footer,
    /// The key column's bloom filter of a row group.
    BloomFilter {
        /// The row group whose filter was decoding.
        row_group: usize,
    },
    /// Pages of a column chunk.
    Pages {
        /// The row group holding the chunk.
        row_group: usize,
        /// The last page the page reader took up for the column reader
        /// before the panic; `None` when it had taken up none.
        page: Option<ChunkPage>,
    },
    /// A Parquet position delete file, read whole.
    PositionDeletes,
}

impl std::fmt::Display for DecodedPart {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Footer => f.write_str("the footer"),
            Self::BloomFilter { row_group } => {
                write!(f, "the bloom filter of row group {row_group}")
            }
            Self::Pages {
                row_group,
                page: Some(page),
            } => write!(f, "row group {row_group} at {page}"),
            Self::Pages {
                row_group,
                page: None,
            } => write!(f, "row group {row_group} before its first page"),
            Self::PositionDeletes => f.write_str("the position delete file"),
        }
    }
}

/// Every tier's resident bytes for one file, from the footer alone (RFD
/// 017), so the budget check and the table owner see the numbers before any
/// key chunk is fetched, and the two parts the tiers differ by. `bloom`,
/// `filter_bytes`, and `bloom_bits_per_key` are `None` when a row group
/// lacks a key bloom filter or the footer does not bound one. Serializes
/// with these field names: the doctor's summary carries it.
#[derive(Clone, Copy, Debug, PartialEq, serde::Serialize)]
pub struct ResidentBytes {
    /// Footer, page indexes, key chunks, and fences, and an ordered
    /// table's order facts and order fences.
    pub full: u64,
    /// Footer, page indexes, and fences, and an ordered table's order
    /// facts and order fences: the memory the disk tier keeps.
    pub disk_memory: u64,
    /// The chunk file the disk tier writes, its envelope and header, its
    /// verification record, and the key chunks, with an ordered table's
    /// order chunks beside them: what `LocalConfig::index_budget_bytes` is
    /// charged for the file, so a budget sized from this figure admits the
    /// load.
    pub disk_bytes: u64,
    /// Footer, page indexes, filters, and fences, and an ordered table's
    /// order facts and order fences.
    pub bloom: Option<u64>,
    /// Filter bits per row of the file; the bloom tier's miss cost follows
    /// it. Per row, not per distinct key, on an ordered table too: the
    /// footer alone cannot count a row group's distinct keys, which the
    /// load counts and the bloom findings divide by (RFD 025).
    pub bloom_bits_per_key: Option<f64>,
    /// The key column's compressed chunk bytes: what the Full tier holds
    /// beyond the Disk tier's memory, and the chunk file's payload beside
    /// its record and an ordered table's order chunks.
    pub key_chunk_bytes: u64,
    /// The key column's bloom filter bytes as the footer bounds them: what
    /// the Bloom tier holds beyond the Disk tier's memory.
    pub filter_bytes: Option<u64>,
}

impl ResidentBytes {
    /// The memory `residency` holds: `full`, `disk_memory`, or `bloom`, the
    /// one mapping from a tier to its field, so a budget check and a test
    /// name the tier and not the field. `None` only for `Bloom` when
    /// `bloom` is.
    pub fn in_tier(&self, residency: Residency) -> Option<u64> {
        match residency {
            Residency::Full => Some(self.full),
            Residency::Disk => Some(self.disk_memory),
            Residency::Bloom => self.bloom,
        }
    }
}

/// One built file's resident memory by part (`FileIndex::resident_parts`),
/// under the definitions of `FileIndex::predicted_resident_bytes` with the
/// fences, the order facts, and the order fences exact. The parts sum to
/// `FileIndex::resident_bytes`, and a part the index's tier does not hold
/// is zero. Each page index part is the memory the kept footer loses
/// without it, the key column's column index taken out first, then the
/// offset indexes, and `footer` is what remains.
/// Serializes with these field names, for a reader that reports sizes by
/// part.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, serde::Serialize)]
pub struct ResidentParts {
    /// The footer as kept, without its page indexes.
    pub footer: u64,
    /// The key column's column index, the only column index the footer
    /// keeps.
    pub key_column_index: u64,
    /// Every offset index the footer keeps: the key column's, an ordered
    /// table's order column's, and each served value column's.
    pub offset_indexes: u64,
    /// The key fences, the first key of every key page.
    pub key_fences: u64,
    /// An ordered file's order facts; zero on a key-value table.
    pub order_facts: u64,
    /// An ordered file's order fences, one per order page; zero on a
    /// key-value table.
    pub order_fences: u64,
    /// The key column's chunks, which Full holds; zero in the other tiers.
    pub key_chunks: u64,
    /// The key column's bloom filters, which Bloom holds; zero in the
    /// other tiers.
    pub filters: u64,
}

impl ResidentParts {
    /// Every part summed, saturating: `FileIndex::resident_bytes`.
    pub fn total(&self) -> u64 {
        saturating_sum([
            self.footer,
            self.key_column_index,
            self.offset_indexes,
            self.key_fences,
            self.order_facts,
            self.order_fences,
            self.key_chunks,
            self.filters,
        ])
    }

    /// The parts of `self` and `other` added, saturating: two files'
    /// parts, or a table's over its files.
    #[must_use]
    pub fn plus(self, other: Self) -> Self {
        Self {
            footer: self.footer.saturating_add(other.footer),
            key_column_index: self.key_column_index.saturating_add(other.key_column_index),
            offset_indexes: self.offset_indexes.saturating_add(other.offset_indexes),
            key_fences: self.key_fences.saturating_add(other.key_fences),
            order_facts: self.order_facts.saturating_add(other.order_facts),
            order_fences: self.order_fences.saturating_add(other.order_fences),
            key_chunks: self.key_chunks.saturating_add(other.key_chunks),
            filters: self.filters.saturating_add(other.filters),
        }
    }
}

/// Why a built index's tier always has a figure: a Bloom-tier build
/// refuses a file whose footer does not bound every row group's filter.
const BOUNDED_FILTERS: &str =
    "a bloom index has a bounded filter per row group; build refused the file otherwise";

/// Filter bits per key: `bytes` of filter over `rows` keys, the unit RFD
/// 016 judges a filter's size by and `ResidentBytes::bloom_bits_per_key`
/// reports. Finite, zero for no rows.
pub(crate) fn bits_per_key(bytes: u64, rows: u64) -> f64 {
    approx::ratio(bytes.saturating_mul(u64::from(u8::BITS)), rows)
}

/// A total of per-row-group or per-file predictions that stays at
/// `u64::MAX` where the plain sum would overflow. A footer that lies can
/// saturate one file's prediction (see `ResidentSet`), so a total over two
/// such files would otherwise panic in a debug build and wrap to a small
/// number in a release one.
pub(crate) fn saturating_sum(values: impl IntoIterator<Item = u64>) -> u64 {
    values.into_iter().fold(0, u64::saturating_add)
}

/// `saturating_sum` of predictions any of which may be absent, as a
/// missing bloom filter makes one: `None` when any value is.
pub(crate) fn saturating_sum_all(values: impl IntoIterator<Item = Option<u64>>) -> Option<u64> {
    values
        .into_iter()
        .collect::<Option<Vec<u64>>>()
        .map(saturating_sum)
}

/// The page a key would decode and, when it can be named without I/O, the
/// row it holds the key at, whose value pages `FileIndex::value_page_offset`
/// names; for tests and the doctor.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PageRef {
    /// The row group the fences name.
    pub row_group: usize,
    /// The key page within that row group the fences name.
    pub key_page: usize,
    /// `key`'s row: `Some` only in the Full tier for a present key, where
    /// the key page is resident and decodes without I/O; `None` for an
    /// absent key, and in the Disk and Bloom tiers, where naming the row
    /// would take a read.
    pub(crate) row: Option<RowRef>,
    /// The position of `key`'s row in the file, with `row`.
    pub(crate) position: Option<RowPosition>,
}

impl PageRef {
    /// Whether the page names its key's row: in the Full tier, whether the
    /// key is present.
    pub fn names_row(&self) -> bool {
        self.row.is_some()
    }
}

/// A decoded run of keys in file order, one arm per key type, holding the
/// values parquet decoded so that a run of a thousand keys costs no
/// allocation per key. Order and search compare the served order of
/// `Key`: longs numerically, everything else as unsigned bytes. A run of
/// order values is one too (`ordered::OrderRun`), without uuids.
#[derive(Clone, Debug)]
pub(crate) enum Keys {
    /// Long keys, an `INT32` column's widened.
    Long(Vec<i64>),
    /// String keys as their UTF-8 bytes. Every run read back through `get`
    /// is checked first: by `verify` at build, then keys by `check_utf8`
    /// and order values by `OrderRun::is_utf8` wherever they decode
    /// (`PairCheck::visit`, `decode_pairs`, a Scan's `decode_order_span`).
    /// So `get` builds a `Key::Str` without a lossy conversion; a lookup's
    /// page is bytes `verify` checked and is only searched, never read back
    /// through `get`. A run built any other way must hold UTF-8 too.
    Str(Vec<ByteArray>),
    /// Binary keys, from a `BYTE_ARRAY` or a `FIXED_LEN_BYTE_ARRAY` column.
    Bin(Vec<ByteArray>),
    /// Uuid keys as the sixteen bytes parquet stores.
    Uuid(Vec<[u8; UUID_BYTES]>),
}

impl Keys {
    /// The number of keys.
    pub(crate) fn len(&self) -> usize {
        match self {
            Self::Long(run) => run.len(),
            Self::Str(run) | Self::Bin(run) => run.len(),
            Self::Uuid(run) => run.len(),
        }
    }

    /// Whether the run holds no key.
    pub(crate) fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// The run's first key; `None` for an empty run.
    pub(crate) fn first(&self) -> Option<Key> {
        (!self.is_empty()).then(|| self.get(0))
    }

    /// The run's last key; `None` for an empty run.
    pub(crate) fn last(&self) -> Option<Key> {
        self.len().checked_sub(1).map(|at| self.get(at))
    }

    /// The key at `at`, which is below `len`.
    pub(crate) fn get(&self, at: usize) -> Key {
        match self {
            Self::Long(run) => Key::Long(run[at]),
            Self::Str(run) => Key::Str(
                String::from_utf8(run[at].data().to_vec())
                    .expect("a Keys::Str run holds UTF-8: every run read back is checked first"),
            ),
            Self::Bin(run) => Key::Bytes(run[at].data().to_vec()),
            Self::Uuid(run) => Key::Uuid(run[at]),
        }
    }

    /// Key `at`, which is below `len`, as fences compare it.
    pub(crate) fn part(&self, at: usize) -> PairPart<'_> {
        match self {
            Self::Long(run) => PairPart::Long(run[at]),
            Self::Str(run) | Self::Bin(run) => PairPart::Bytes(run[at].data()),
            Self::Uuid(run) => PairPart::Bytes(&run[at]),
        }
    }

    /// The position of `key`, or where it would be inserted, over a run in
    /// served order. A key of another type than the run's is at position
    /// zero and absent: keys of different types never share a table.
    pub(crate) fn binary_search(&self, key: &Key) -> std::result::Result<usize, usize> {
        match (self, key) {
            (Self::Long(run), Key::Long(wanted)) => run.binary_search(wanted),
            (Self::Str(run), Key::Str(wanted)) => {
                run.binary_search_by(|held| held.data().cmp(wanted.as_bytes()))
            }
            (Self::Bin(run), Key::Bytes(wanted)) => {
                run.binary_search_by(|held| held.data().cmp(wanted.as_slice()))
            }
            (Self::Uuid(run), Key::Uuid(wanted)) => run.binary_search(wanted),
            (
                Self::Long(_) | Self::Str(_) | Self::Bin(_) | Self::Uuid(_),
                Key::Long(_) | Key::Str(_) | Key::Bytes(_) | Key::Uuid(_),
            ) => Err(0),
        }
    }

    /// The positions holding `key` in a run in served order where a key may
    /// repeat, as an ordered file's runs do: from the first key not below
    /// `key` to the first above it, empty where the run does not hold it. A
    /// key of another type than the run's is held nowhere, at position zero.
    pub(crate) fn equal_range(&self, key: &Key) -> Range<usize> {
        fn around<T>(run: &[T], cmp: impl Fn(&T) -> Ordering) -> Range<usize> {
            run.partition_point(|held| cmp(held).is_lt())
                ..run.partition_point(|held| cmp(held).is_le())
        }
        match (self, key) {
            (Self::Long(run), Key::Long(wanted)) => around(run, |held| held.cmp(wanted)),
            (Self::Str(run), Key::Str(wanted)) => {
                around(run, |held| held.data().cmp(wanted.as_bytes()))
            }
            (Self::Bin(run), Key::Bytes(wanted)) => {
                around(run, |held| held.data().cmp(wanted.as_slice()))
            }
            (Self::Uuid(run), Key::Uuid(wanted)) => around(run, |held| held.cmp(wanted)),
            (
                Self::Long(_) | Self::Str(_) | Self::Bin(_) | Self::Uuid(_),
                Key::Long(_) | Key::Str(_) | Key::Bytes(_) | Key::Uuid(_),
            ) => 0..0,
        }
    }

    /// Compares key `at` of this run with key `other_at` of `other` in
    /// served order. Runs of different types never meet within one table;
    /// when they do, strings against bytes compare bytewise, and any other
    /// two variants by `Key`'s derived order, which is total and carries
    /// no meaning.
    pub(crate) fn cmp_at(&self, at: usize, other: &Self, other_at: usize) -> Ordering {
        match (self, other) {
            (Self::Long(mine), Self::Long(theirs)) => mine[at].cmp(&theirs[other_at]),
            (Self::Str(mine) | Self::Bin(mine), Self::Str(theirs) | Self::Bin(theirs)) => {
                mine[at].data().cmp(theirs[other_at].data())
            }
            (Self::Uuid(mine), Self::Uuid(theirs)) => mine[at].cmp(&theirs[other_at]),
            (
                Self::Long(_) | Self::Str(_) | Self::Bin(_) | Self::Uuid(_),
                Self::Long(_) | Self::Str(_) | Self::Bin(_) | Self::Uuid(_),
            ) => self.get(at).cmp(&other.get(other_at)),
        }
    }

    /// Compares key `at` with key `other_at` of this run in served order.
    pub(crate) fn cmp_within(&self, at: usize, other_at: usize) -> Ordering {
        match self {
            Self::Long(run) => run[at].cmp(&run[other_at]),
            Self::Str(run) | Self::Bin(run) => run[at].data().cmp(run[other_at].data()),
            Self::Uuid(run) => run[at].cmp(&run[other_at]),
        }
    }

    /// The length in bytes of the longest key: eight for longs, sixteen for
    /// uuids, the longest value otherwise, and zero for an empty run.
    pub(crate) fn max_len(&self) -> usize {
        match self {
            Self::Long(run) => {
                if run.is_empty() {
                    0
                } else {
                    size_of::<i64>()
                }
            }
            Self::Str(run) | Self::Bin(run) => run.iter().map(ByteArray::len).max().unwrap_or(0),
            Self::Uuid(run) => {
                if run.is_empty() {
                    0
                } else {
                    UUID_BYTES
                }
            }
        }
    }

    /// The position of the first key that is not above the one before it,
    /// with how it compares to that key: `Equal` for a duplicate, `Less`
    /// for a key out of order. `None` for a strictly increasing run.
    pub(crate) fn first_not_increasing(&self) -> Option<(usize, Ordering)> {
        let at = match self {
            Self::Long(run) => run.windows(2).position(|pair| pair[0] >= pair[1]),
            Self::Str(run) | Self::Bin(run) => run
                .windows(2)
                .position(|pair| pair[0].data() >= pair[1].data()),
            Self::Uuid(run) => run.windows(2).position(|pair| pair[0] >= pair[1]),
        }? + 1;
        Some((at, self.cmp_at(at, self, at - 1)))
    }
}

/// How the key column's values decode, fixed at build from the key type and
/// the column's physical type, so every decode matches on these five and
/// none of its arms is impossible.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum KeyShape {
    /// `INT64`.
    Long,
    /// `BYTE_ARRAY` holding UTF-8.
    Str,
    /// `BYTE_ARRAY` holding opaque bytes.
    Bin,
    /// `FIXED_LEN_BYTE_ARRAY` of at least one byte holding opaque bytes.
    FixedBin,
    /// `FIXED_LEN_BYTE_ARRAY` of `UUID_BYTES`.
    Uuid,
}

impl KeyShape {
    /// The key type the shape decodes keys of.
    fn key_type(self) -> KeyType {
        match self {
            Self::Long => KeyType::Long,
            Self::Str => KeyType::String,
            Self::Bin | Self::FixedBin => KeyType::Binary,
            Self::Uuid => KeyType::Uuid,
        }
    }

    /// How the keys decode into a `Keys` run.
    fn run_shape(self) -> RunShape {
        match self {
            Self::Long => RunShape::Int64,
            Self::Str => RunShape::Str,
            Self::Bin => RunShape::Bin,
            Self::FixedBin => RunShape::FixedBin,
            Self::Uuid => RunShape::Uuid,
        }
    }
}

/// How the value column's values decode, fixed at build.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ValueShape {
    /// `BYTE_ARRAY`.
    Variable,
    /// `FIXED_LEN_BYTE_ARRAY`.
    Fixed,
}

/// The column a file holds under a configured value column's name (RFD
/// 023): its leaf and how its values decode. A file's columns never
/// change, so the column found by name is the same under every snapshot
/// that serves the file; whether that snapshot's schema means it is the
/// generation's to check (`column::resolve`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct ValueColumn {
    leaf: usize,
    shape: ValueShape,
}

/// How an ordered table's order column decodes, fixed at build from the
/// order type and the column's physical type (RFD 025): an `INT32` holds
/// an `int`, a `date`, or a `long` written before a promotion from `int`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum OrderShape {
    /// `INT32`, read as a long.
    Int32,
    /// `INT64`.
    Int64,
    /// `BYTE_ARRAY` holding UTF-8: a `string`.
    Str,
    /// `BYTE_ARRAY` holding opaque bytes: a `binary`.
    Bin,
    /// `FIXED_LEN_BYTE_ARRAY` of this width.
    Fixed(u32),
}

impl OrderShape {
    /// The heap an `OrderValue` of this shape holds, given `bytes`, the
    /// value's length: none for a long, which it holds inline.
    fn heap_bytes(self, bytes: u64) -> u64 {
        match self {
            Self::Int32 | Self::Int64 => 0,
            Self::Str | Self::Bin | Self::Fixed(_) => bytes,
        }
    }

    /// The bytes every order value of this shape takes: eight for an
    /// integer, read as a long, and the width for a fixed-width value;
    /// `None` for a byte array, whose values vary.
    fn fixed_bytes(self) -> Option<u64> {
        match self {
            Self::Int32 | Self::Int64 => Some(approx::count(size_of::<i64>())),
            Self::Fixed(width) => Some(u64::from(width)),
            Self::Str | Self::Bin => None,
        }
    }

    /// How the order values decode into a `Keys` run.
    fn run_shape(self) -> RunShape {
        match self {
            Self::Int32 => RunShape::Int32,
            Self::Int64 => RunShape::Int64,
            Self::Str => RunShape::Str,
            Self::Bin => RunShape::Bin,
            Self::Fixed(_) => RunShape::FixedBin,
        }
    }
}

/// How a key or order column's values decode into a `Keys` run
/// (`build::decode_run`, `build::RunBatches`): the physical type, and what
/// its values are read as.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum RunShape {
    /// `INT32`, read as longs: an order column's alone.
    Int32,
    /// `INT64`, read as longs.
    Int64,
    /// `BYTE_ARRAY` holding UTF-8.
    Str,
    /// `BYTE_ARRAY` holding opaque bytes.
    Bin,
    /// `FIXED_LEN_BYTE_ARRAY` holding opaque bytes.
    FixedBin,
    /// `FIXED_LEN_BYTE_ARRAY` of `UUID_BYTES`: a key column's alone.
    Uuid,
}

/// The column a file holds under an ordered table's order column: its
/// leaf, how its values decode, and the field id the snapshot's schema
/// gives it, which names the rules its chunk file is verified under
/// (`inspect::rules_of`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct OrderLeaf {
    leaf: usize,
    shape: OrderShape,
    field_id: i32,
}

/// What the tier keeps of the key column (RFD 017).
#[derive(Debug)]
enum KeyResidency {
    /// Full: every key column chunk, by file offset.
    Chunks(Arc<Segments>),
    /// Disk: the chunk file holding them; no key bytes in memory.
    Disk(DiskChunks),
    /// Bloom: one filter per row group; no key bytes anywhere local.
    Filters(Vec<Sbbf>),
}

/// The exact first and last key of a row group that has rows, decoded at
/// build. Row groups without rows have no entry.
#[derive(Clone, Debug, PartialEq, Eq)]
struct GroupRange {
    row_group: usize,
    first: Key,
    last: Key,
}

/// Where a read's page bytes come from: for a lookup, and for a Scan's key
/// pages, the store through the page cache (a lookup's value page in every
/// tier, the key page in the Bloom tier), each GET answering to the issue
/// given (`Issue`), or the disk tier's chunk file; for the doctor, the
/// store on the load path, one unhedged load GET per page that feeds no
/// table's hedge window (RFD 013, RFD 024).
#[derive(Clone, Copy)]
enum PageSource<'a> {
    Store(Option<&'a PageCache>, Issue<'a>),
    Disk(&'a DiskChunks),
    Load,
}

/// Where the pages one read took came from: the page it decodes, and the
/// chunk's dictionary page when it has one.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Origins {
    page: PageOrigin,
    dictionary: Option<PageOrigin>,
}

impl Origins {
    /// Whether the page or the dictionary page came from object storage, a
    /// fetch another lookup ran and this one joined included.
    fn any_fetched(self) -> bool {
        self.page == PageOrigin::Fetched || self.dictionary == Some(PageOrigin::Fetched)
    }
}

/// One value column's value at one row (`FileIndex::read_column`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ValueRead {
    /// The bytes unchanged; `None` for a null, distinct from an absent key.
    pub(crate) value: Option<Bytes>,
    /// Where the value page came from.
    pub(crate) origin: PageOrigin,
    /// Whether the column's value page or its dictionary page came from
    /// object storage for the read, a fetch another lookup ran and this one
    /// joined included.
    pub(crate) from_store: bool,
}

/// Everything resident for one data file, built once per generation and
/// shared by every lookup: `Send + Sync`, since a `Table` shares it across
/// its tasks.
#[derive(Debug)]
pub struct FileIndex {
    handle: Arc<FileHandle>,
    /// The footer with the page indexes of the key column, the offset index
    /// of an ordered table's order column, and the offset index of every
    /// value column the table serves; every other column index and offset
    /// index is dropped at build (`kept_footer`).
    metadata: Arc<ParquetMetaData>,
    /// The key leaf, how the keys decode, the value column the file holds
    /// per column of the table's layout, `None` for one the file was
    /// written before, whose rows answer that column's default, and an
    /// ordered table's order column.
    layout: Layout,
    keys: KeyResidency,
    /// Per row group, the first key of every key page.
    fences: Vec<Vec<Key>>,
    /// Ascending by first key; how a lookup chooses its row group.
    groups: Vec<GroupRange>,
    /// Per row group, the position in the file of its first row: the rows
    /// of every earlier row group, summed once at build so that `position`
    /// is one addition.
    group_starts: Vec<RowPosition>,
    /// The exact first and last key of the file.
    key_range: (Key, Key),
    max_key_bytes: usize,
    /// What verifying an ordered file's order column recorded, its order
    /// facts and its order fences (RFD 025); `None` on a key-value table.
    /// The column's offset index stays in the kept footer, its column
    /// index does not.
    order: Option<OrderVerified>,
    rows: i64,
}

impl FileIndex {
    /// Loads the footer (one GET), checks the file against the table, and
    /// streams the key column chunks once for verification in every tier.
    /// Refuses, typed under `Error::Refused` for `table`: a key or value
    /// column the file does not hold as `layout`, the snapshot's, means it
    /// (`RefusalReason::ColumnMismatch`, `column::resolve`), a key column
    /// whose physical type does not match the key type (`Binary` accepts
    /// `BYTE_ARRAY` and a `FIXED_LEN_BYTE_ARRAY` at least one byte wide,
    /// `Uuid` a `FIXED_LEN_BYTE_ARRAY` of `UUID_BYTES`), in the Bloom tier a row
    /// group without a key bloom filter, then, once the chunks are decoded, a null key, a
    /// key below the one before it or a row group starting below the
    /// previous one's last key, a key repeated, and a key over
    /// `contract::MAX_KEY_BYTES`. A file that is malformed for serving is
    /// `Error::File`: a value column that is neither
    /// `BYTE_ARRAY` nor `FIXED_LEN_BYTE_ARRAY` (the schema admitted its
    /// Iceberg type, so the file contradicts it) or is a
    /// `FIXED_LEN_BYTE_ARRAY` of width 0, no rows, row counts that
    /// disagree, no offset index on either column or one that does not
    /// describe its pages, an unbounded or undecodable bloom filter, a
    /// string key that is not UTF-8, a key page header parquet would not
    /// read to the numbers the index reads or that declares more than its
    /// page can hold, a key page holding other rows than its offset index
    /// says, a footer, key page, or bloom filter parquet panics decoding
    /// (`FileFault::DecoderPanicked`). Footer checks run before any chunk is
    /// fetched; `inspect` is that stage on its own. On an ordered table the
    /// order column resolves as a value column does, a file without it is
    /// refused (`ColumnMismatch::OrderAbsent`), one that stores it in a
    /// Parquet type its order type does not admit is
    /// `RefusalReason::OrderTypeMismatch`, and one without its offset index
    /// is `FileFault::MissingPageIndex`; once its chunks are decoded beside
    /// the keys under the ordered rules (`build::Rules::Ordered`), a key may
    /// repeat, and a key and order value together twice is
    /// `RefusalReason::DuplicateRows`, one below the pair before it
    /// `RefusalReason::Unsorted`, a null order value
    /// `RefusalReason::NullOrder`, and a string order value that is not
    /// UTF-8 a file fault (RFD 025). The Disk tier with no
    /// local directory is `Error::Config` with
    /// `ConfigViolation::DiskResidencyWithoutLocalDir`, before any chunk is
    /// fetched.
    ///
    /// An ordered table's order chunks are fetched with the key chunks, one
    /// GET per row group per column. Full keeps the key chunks in memory
    /// and drops the order chunks. Disk trusts a chunk file in `local`,
    /// named by the rules the file is verified under, whose identity,
    /// verification context, and payload checksum match and which stores
    /// exactly the key chunks, and an ordered table's order chunks, taking
    /// what verification established from the record it stores (no GET, no
    /// chunk read, no key decoded), or fetches the chunks, verifies them,
    /// and writes them with that record; it keeps no key bytes in memory.
    /// Bloom fetches and verifies the chunks, drops them, and keeps the
    /// filters. Every tier keeps the footer, both page indexes of the key
    /// column, the offset index of an ordered table's order column and of
    /// every value column the table serves, the fences, and an ordered
    /// table's order facts and order fences; the column index of every other
    /// column, the value columns' and the order column's among them, and the
    /// offset index of every column the table does not serve, are never
    /// resident (RFD 004, RFD 017, RFD 022, RFD 025).
    ///
    /// Only tests call this: the loader runs `inspect` and
    /// `Inspected::build` apart, so its budget check sits between them.
    #[cfg(test)]
    pub(crate) async fn build(
        table: &str,
        handle: Arc<FileHandle>,
        layout: &TableLayout,
        residency: Residency,
        local: Option<&Arc<crate::local::LocalDir>>,
    ) -> Result<Self> {
        Self::inspect(table, handle, layout)
            .await?
            .build(residency, local, None)
            .await
    }

    /// The footer stage of `build`, on its own so that a caller can check
    /// every file's predicted resident bytes against a budget before any key
    /// chunk is fetched (RFD 017): loads the footer (one GET), makes every
    /// footer check `build` documents, trims the footer to what the index
    /// keeps, and computes the prediction. Nothing past the footer is read.
    pub(crate) async fn inspect(
        table: &str,
        handle: Arc<FileHandle>,
        layout: &TableLayout,
    ) -> Result<Inspected> {
        let key_type = layout.key_type;
        let loaded = handle.load_metadata().await?;
        let layout = Layout::inspect(table, &handle, &loaded, layout)?;
        let value_leaves = layout.held_value_leaves();
        let order_leaf = layout.order_leaf();
        let metadata = Arc::new(kept_footer(
            &loaded,
            layout.key_leaf,
            order_leaf,
            &value_leaves,
        ));
        drop(loaded);
        let predicted = Self::predicted_resident_bytes(
            &metadata,
            layout.key_leaf,
            layout.order,
            &value_leaves,
            key_type,
            &handle,
        );
        tracing::debug!(
            path = %handle.location,
            full = predicted.full,
            disk_memory = predicted.disk_memory,
            disk_bytes = predicted.disk_bytes,
            bloom = ?predicted.bloom,
            "predicted resident bytes"
        );
        Ok(Inspected {
            table: table.to_string(),
            handle,
            metadata,
            layout,
            predicted,
            reuse: None,
        })
    }

    /// Every tier's resident bytes for the file `handle` opens, from its
    /// footer alone (RFD 017), the same given the footer as loaded or as
    /// `FileIndex` keeps it (`metadata()`): the column index of every column
    /// but the key and `order`, an ordered table's order column, and the
    /// offset index of every column but the key, `order`, and
    /// `value_leaves`, are never resident and are not counted either way, so
    /// a budget check at the footer stage predicts what `resident_bytes`
    /// will report. Exact: the footer and the page indexes kept
    /// (`ParquetMetaData::memory_size` of the footer as kept), the key
    /// chunks (the footer's compressed sizes, which Full holds and Disk
    /// writes byte for byte; a chunk the footer cannot place, by a negative
    /// field or an end past the file's size, counts as zero, and `inspect`
    /// refuses such a footer before any budget check reads this), and the
    /// filters (the footer's lengths, header included; the in-memory filter
    /// drops the header, a few bytes per row group; `bloom` is `None` when a
    /// row group's filter is absent or one a Bloom-tier `build` refuses as
    /// `FileFault::UnplaceableBloomFilter`), and the chunk file (the
    /// envelope and the header the Disk tier writes for the handle's
    /// identity over the key chunks, and on an ordered table the order
    /// chunks, under the rules it verifies them by, the verification record,
    /// then the chunks; the identity carries the footer digest, so `handle`
    /// has loaded its footer), except that the record, a key per key page,
    /// is exact only for keys of one width (longs, uuids, a fixed-width
    /// binary column) and estimated as the fences are for string and binary
    /// keys, `key_type` saying which, an ordered file's order facts in it
    /// are estimated at no less than what they take: each row group's
    /// distinct keys as its rows, the longest run as the file's rows, and
    /// its order fences, one per order page, as the order fences in memory
    /// are. Every
    /// total saturates at `u64::MAX` rather than wrapping. Estimated: the
    /// fences, one `Key` per key page plus, for `BYTE_ARRAY` keys, the
    /// page's minimum from the column index when the file has one, which a
    /// writer may truncate, else the chunk's mean encoded bytes per value,
    /// and for `FIXED_LEN_BYTE_ARRAY` keys the column's width, which
    /// over-counts a uuid key, held inline, by `UUID_BYTES` per page; and
    /// an ordered file's order facts, a `u64` per row group and the least
    /// and greatest order value, exact for integer and fixed-width order
    /// values and for string and binary ones at the order chunks'
    /// uncompressed bytes per value; and an ordered file's order fences,
    /// one per order page from the offset index, a key at the mean bytes
    /// the row group's key fences take per key page and an order value
    /// exact for an integer, at the column's width for a fixed-width value,
    /// and for a string or binary one at the order chunk's uncompressed
    /// bytes per value, the footer as kept holding no order column index;
    /// no footer number
    /// bounds a dictionary-encoded column's, so a load charges an ordered
    /// file's built fences exactly (RFD 025, Load). `resident_bytes` shares
    /// the definitions.
    pub(crate) fn predicted_resident_bytes(
        metadata: &ParquetMetaData,
        key_leaf: usize,
        order: Option<OrderLeaf>,
        value_leaves: &[usize],
        key_type: KeyType,
        handle: &FileHandle,
    ) -> ResidentBytes {
        tiers(&ResidentSet::from_footer(
            metadata,
            key_leaf,
            order,
            value_leaves,
            key_type,
            handle,
            None,
        ))
    }

    /// The tier the index was built in.
    pub(crate) fn residency(&self) -> Residency {
        match self.keys {
            KeyResidency::Chunks(_) => Residency::Full,
            KeyResidency::Disk(_) => Residency::Disk,
            KeyResidency::Filters(_) => Residency::Bloom,
        }
    }

    /// The chunk file holding the key chunks, in the Disk tier; `None` in
    /// the others, which keep no chunk file.
    pub(crate) fn disk_chunks(&self) -> Option<&DiskChunks> {
        match &self.keys {
            KeyResidency::Disk(disk) => Some(disk),
            KeyResidency::Chunks(_) | KeyResidency::Filters(_) => None,
        }
    }

    /// The file the index describes.
    pub(crate) fn handle(&self) -> &Arc<FileHandle> {
        &self.handle
    }

    /// The footer as kept: both page indexes of the key column and of an
    /// ordered table's order column, the offset index of every value column
    /// the table serves, and no other page index.
    pub fn metadata(&self) -> &Arc<ParquetMetaData> {
        &self.metadata
    }

    /// Rows in the file; above zero.
    pub(crate) fn rows(&self) -> i64 {
        self.rows
    }

    /// The key type the file's keys were verified as.
    pub(crate) fn key_type(&self) -> KeyType {
        self.layout.key_shape.key_type()
    }

    /// The leaf column index of the key.
    pub fn key_leaf(&self) -> usize {
        self.layout.key_leaf
    }

    /// The leaf column index of value column `column`, a position in the
    /// table's layout; `None` for a file written before that column was
    /// added, which holds no such column.
    pub fn value_leaf(&self, column: usize) -> Option<usize> {
        self.layout.values[column].map(|value| value.leaf)
    }

    /// Every value column's leaf, in the layout's order.
    pub fn value_leaves(&self) -> Vec<Option<usize>> {
        self.layout.value_leaves()
    }

    /// The leaves of the value columns the file holds: those whose offset
    /// index the footer keeps.
    pub(crate) fn held_value_leaves(&self) -> Vec<usize> {
        self.layout.held_value_leaves()
    }

    /// The order column's leaf, whose offset index the footer keeps;
    /// `None` on a key-value table.
    pub fn order_leaf(&self) -> Option<usize> {
        self.layout.order_leaf()
    }

    /// The order column as the index decodes it, for a prediction of the
    /// file's resident bytes (`predicted_resident_bytes`); `None` on a
    /// key-value table.
    pub(crate) fn order_column(&self) -> Option<OrderLeaf> {
        self.layout.order
    }

    /// What verifying the order column recorded: the least and greatest
    /// order value, each row group's distinct keys, and the longest run
    /// (RFD 025, Load); `None` on a key-value table.
    pub fn order_facts(&self) -> Option<&OrderFacts> {
        self.order.as_ref().map(|order| &order.facts)
    }

    /// The order fences: per row group, the fence of each order page, by
    /// which a pair of key and order value is placed on the page holding it
    /// (`OrderFences::page_of`, `OrderFences::page_pairs`; RFD 025, Load);
    /// `None` on a key-value table.
    pub fn order_fences(&self) -> Option<&OrderFences> {
        self.order.as_ref().map(|order| &order.fences)
    }

    /// The rules the file was verified under, which name its Disk-tier
    /// chunk file (RFD 025, Load): ordered by the order column's field id,
    /// or key-value.
    pub(crate) fn chunk_rules(&self) -> ChunkRules {
        self.layout.rules()
    }

    /// Checks that the file still holds its columns as `layout`, a later
    /// snapshot's, means them, from the footer as kept with no read (RFD
    /// 023): a file reused from the previous generation was inspected
    /// under that generation's schema, and a schema that drops a column and
    /// adds another under its name changes what the file's column means.
    /// Refuses as `column::resolve` does, a key column the snapshot
    /// types otherwise than the index decodes its keys as is
    /// `RefusalReason::KeyTypeMismatch`, and an order column whose type in
    /// `layout` does not admit the file's (`inspect::order_leaf`)
    /// `RefusalReason::OrderTypeMismatch`. `Ok(true)` when the key, the
    /// order column, and every value column, by position, resolve to the
    /// leaves the index was built over, the order column decoding as it was,
    /// so it holds no answer that changes; `Ok(false)` when they resolve to
    /// others, `layout` names another count of columns, or it sets an order
    /// column where the index has none or the reverse, an index that would
    /// answer a column from another's pages, which the caller does not
    /// reuse.
    pub(crate) fn check_columns(&self, table: &str, layout: &TableLayout) -> Result<bool> {
        let path = self.handle.location.as_str();
        let schema = self.metadata.file_metadata().schema_descr();
        let columns = crate::column::resolve(table, path, schema, layout)?;
        if layout.key_type != self.key_type() {
            return Err(Error::refused(
                table,
                RefusalReason::KeyTypeMismatch {
                    path: path.to_string(),
                    expected: layout.key_type,
                    got: self.key_column().descriptor().physical_type(),
                },
            ));
        }
        let order = inspect::order_leaf(
            table,
            path,
            schema,
            layout.order.as_ref(),
            columns.order_leaf,
        )?;
        Ok(columns.key_leaf == self.layout.key_leaf
            && columns.value_leaves == self.layout.value_leaves()
            && order == self.layout.order)
    }

    /// The first key of every key page, per row group; a row group without
    /// rows has no fences.
    pub(crate) fn fences(&self) -> &[Vec<Key>] {
        &self.fences
    }

    /// Key pages in `row_group`, which is below the footer's row group count.
    pub(crate) fn page_count(&self, row_group: usize) -> usize {
        self.fences[row_group].len()
    }

    /// The length in bytes of the file's longest key; at most
    /// `contract::MAX_KEY_BYTES`.
    pub(crate) fn max_key_bytes(&self) -> usize {
        self.max_key_bytes
    }

    /// The exact first and last key of the file, decoded at build; never a
    /// truncated bound.
    pub fn key_range(&self) -> (&Key, &Key) {
        (&self.key_range.0, &self.key_range.1)
    }

    /// Whether `key` lies within the file's exact key range.
    pub(crate) fn may_contain(&self, key: &Key) -> bool {
        let (first, last) = self.key_range();
        first <= key && key <= last
    }

    /// Bytes held in memory for this file: the footer and page indexes as
    /// kept, the tier's structure (chunks, filters, or nothing for Disk),
    /// and the fences, under the definitions of `predicted_resident_bytes`
    /// with the fences exact.
    pub(crate) fn resident_bytes(&self) -> usize {
        let mine = tiers(&self.resident_set())
            .in_tier(self.residency())
            .expect(BOUNDED_FILTERS);
        approx::length(mine)
    }

    /// `resident_bytes` by part (`ResidentParts`), for a reader that
    /// reports where an index's memory goes. Computes the footer parts
    /// from copies of the kept footer, so it costs a few footer copies:
    /// for reporting, not for a lookup.
    pub fn resident_parts(&self) -> ResidentParts {
        let leaves = self.held_value_leaves();
        let kept = kept_footer(
            &self.metadata,
            self.layout.key_leaf,
            self.order_leaf(),
            &leaves,
        );
        self.resident_set()
            .parts(&kept, self.residency())
            .expect(BOUNDED_FILTERS)
    }

    /// The parts `resident_bytes` and `resident_parts` are made of, with
    /// the fences, the order facts, and the order fences exact.
    fn resident_set(&self) -> ResidentSet {
        ResidentSet::from_footer(
            &self.metadata,
            self.layout.key_leaf,
            self.layout.order,
            &self.held_value_leaves(),
            self.key_type(),
            &self.handle,
            Some((&self.fences, self.order.as_ref())),
        )
    }

    /// The exact position of `key`, or `None` for a key the file does not
    /// hold, including one between two of its rows. Row group and key page
    /// come from the exact keys decoded at build, never from statistics.
    /// Full decodes the one resident key page and does no I/O. Disk reads
    /// that page from the chunk file: one local read, no GET (one more
    /// local read for the chunk's dictionary page when the column has one).
    /// Bloom asks the row group's filter first: a negative is `None` with no
    /// I/O; a positive fetches the one key page the fences name through
    /// `cache`, one GET, so a false positive costs exactly that (and a
    /// dictionary-encoded key column its dictionary page besides), each
    /// GET answering to `issue`. A key outside the file's range costs
    /// nothing in every tier.
    pub(crate) async fn find(
        &self,
        key: &Key,
        cache: Option<&PageCache>,
        issue: Issue<'_>,
    ) -> Result<Option<RowRef>> {
        let Some((row_group, page)) = self.locate(key) else {
            return Ok(None);
        };
        // Only the value pages decide a lookup's label; a key page's origin
        // is not kept.
        let chunk = self.key_column().group(row_group);
        let segments = match &self.keys {
            KeyResidency::Chunks(chunks) => Arc::clone(chunks),
            KeyResidency::Disk(disk) => {
                let (segments, _origins) = self
                    .fetch_pages(chunk, page, PageSource::Disk(disk))
                    .await?;
                segments
            }
            KeyResidency::Filters(filters) => {
                if !filters[row_group].check(&*key.as_bytes()) {
                    return Ok(None);
                }
                let (segments, _origins) = self
                    .fetch_pages(chunk, page, PageSource::Store(cache, issue))
                    .await?;
                segments
            }
        };
        self.search(key, row_group, page, segments)
    }

    /// The value of value column `column`, a position in the table's
    /// layout, at `row`, `None` for a file that does not hold that column
    /// (`value_leaf`): the one value page holding it through `cache` (one
    /// GET cold, none warm) and, when the column chunk has a dictionary
    /// page, that page too (the one extra GET of RFD 007's penalty), the
    /// two read together, each GET answering to `issue`, then exactly one
    /// row decoded. The value carries
    /// the bytes unchanged, a slice of the fetched page where parquet
    /// decoded them in place and a copy otherwise (`value_bytes`); `None` is
    /// a null value, distinct from an absent key. The origin is the value
    /// page's: `Fetched` for the GET of a cold hit, `Cached` for a warm
    /// one; `from_store` is whether either page came from object storage.
    /// A row outside every
    /// page of its row group is `FileFault::PageOutOfRange`. The headers of
    /// the value page and of its dictionary page are read from the fetched
    /// bytes before parquet decodes them, and one that parquet would not
    /// read to the same numbers or that declares more than its page can
    /// hold is `FileFault::MalformedPageHeader` or
    /// `FileFault::PageHeaderOverBound` (`checked_values`). A page
    /// parquet panics decoding is `FileFault::DecoderPanicked`.
    pub(crate) async fn read_column(
        &self,
        column: usize,
        row: RowRef,
        cache: Option<&PageCache>,
        issue: Issue<'_>,
    ) -> Result<Option<ValueRead>> {
        let Some(value) = self.layout.values[column] else {
            return Ok(None);
        };
        let page = self.value_page(value, row)?;
        let chunk = self.value_column(value).group(row.row_group);
        let (segments, origins) = self
            .fetch_pages(chunk, page, PageSource::Store(cache, issue))
            .await?;
        let value = checked_values(
            chunk,
            value.shape,
            page..page + 1,
            row.row..row.row + 1,
            &segments,
        )?
        .value(0);
        Ok(Some(ValueRead {
            value,
            origin: origins.page,
            from_store: origins.any_fetched(),
        }))
    }

    /// Every value of the first value page of value column `column`, a
    /// position in the table's layout, nulls left out: page 0 of the first
    /// row group that holds rows and a page, read with one load GET past
    /// the page cache, never hedged, and its dictionary page's when the
    /// chunk has one, its
    /// headers checked as `read_column` checks them. Empty for a file whose
    /// row groups hold no page, and for a page of only nulls. `None` for a
    /// file written before the column was added (RFD 023). What the doctor
    /// walks a protobuf column's values on (RFD 024).
    pub(crate) async fn first_page_values(&self, column: usize) -> Result<Option<Vec<Bytes>>> {
        const FIRST_PAGE: usize = 0;
        let Some(value) = self.layout.values[column] else {
            return Ok(None);
        };
        let column = self.value_column(value);
        let Some(chunk) = (0..self.metadata.num_row_groups())
            .map(|row_group| column.group(row_group))
            .find(|chunk| chunk.rows() > 0 && !chunk.page_locations().is_empty())
        else {
            return Ok(Some(Vec::new()));
        };
        let (segments, _) = self
            .fetch_pages(chunk, FIRST_PAGE, PageSource::Load)
            .await?;
        let rows = checked_values(
            chunk,
            value.shape,
            FIRST_PAGE..FIRST_PAGE + 1,
            chunk.page_rows(FIRST_PAGE),
            &segments,
        )?;
        Ok(Some(
            (0..rows.len()).filter_map(|at| rows.value(at)).collect(),
        ))
    }

    /// `read_column` of the layout's first value column, as the value and
    /// its page's origin: the shape the single-column tests read.
    #[cfg(test)]
    pub(crate) async fn read_value(
        &self,
        row: RowRef,
        cache: Option<&PageCache>,
    ) -> Result<Option<(Option<Bytes>, PageOrigin)>> {
        Ok(self
            .read_column(0, row, cache, Issue::default())
            .await?
            .map(|read| (read.value, read.origin)))
    }

    /// The file offset of the value page of value column `column`, a
    /// position in the table's layout, that a hit on the key `page` names
    /// would fetch: `Some` only where `page` names the key's row (in the
    /// Full tier, for a present key) in a file that holds the column;
    /// `None` for a file written before the column was added, which fetches no
    /// value page (RFD 023).
    pub fn value_page_offset(&self, column: usize, page: &PageRef) -> Option<u64> {
        let value = self.layout.values[column]?;
        let row = page.row?;
        self.value_page_index(value, row).map(|at| {
            self.value_column(value)
                .group(row.row_group)
                .page_range(at)
                .start
        })
    }

    /// The key page `find` would decode for `key` and, in the Full tier for
    /// a present key, the row it holds the key at, whose value pages
    /// `value_page_offset` names. `None` when
    /// `find` would decode nothing: the key is outside the file's range,
    /// between two row groups, or ruled out by the Bloom tier's filter.
    /// Synchronous and free of I/O in every tier, so `Generation::plan` can
    /// predict from it the GET a lookup issues for the key page.
    pub fn page_of(&self, key: &Key) -> Option<PageRef> {
        let (row_group, key_page) = self.locate(key)?;
        if let KeyResidency::Filters(filters) = &self.keys
            && !filters[row_group].check(&*key.as_bytes())
        {
            return None;
        }
        let row = match &self.keys {
            KeyResidency::Chunks(chunks) => self
                .search(key, row_group, key_page, Arc::clone(chunks))
                .expect("build decoded this key page from these resident bytes and checked it against its offset index; decoding it again cannot fail"),
            KeyResidency::Disk(_) | KeyResidency::Filters(_) => None,
        };
        Some(PageRef {
            row_group,
            key_page,
            row,
            position: row.map(|row| self.position(row)),
        })
    }

    /// The position in the file of the first row of key page `page` of
    /// `row_group`, which is below `page_count(row_group)`.
    pub(crate) fn key_page_start(&self, row_group: usize, page: usize) -> RowPosition {
        self.position(RowRef {
            row_group,
            row: self.key_column().group(row_group).page_rows(page).start,
        })
    }

    /// The row cache's key of value column `column`, a position in the
    /// table's layout, at `row` (RFD 026): the file's id, the row's
    /// position, and the column's leaf in the file. `None` for a file that
    /// does not hold the column (`value_leaf`), which answers it by its
    /// default and reads no page of it.
    pub(crate) fn row_key(&self, column: usize, row: RowRef) -> Option<RowKey> {
        self.value_leaf(column)
            .map(|leaf| RowKey::new(self.handle.file_id(), self.position(row), leaf))
    }

    /// The position of `row` in the file: the rows of every earlier row
    /// group, then the row within its own.
    pub(crate) fn position(&self, row: RowRef) -> RowPosition {
        RowPosition(self.group_starts[row.row_group].0 + approx::count(row_index(row.row)))
    }

    /// The key column of `row_group` decoded from `chunks`, or one page of
    /// it; no I/O, and a byte not in `chunks` is a `Segments` fault inside
    /// `Error::Parquet`. `row_group` is below the footer's row group count
    /// and `page` below `page_count(row_group)`.
    pub(crate) fn decode_keys(
        &self,
        row_group: usize,
        page: Option<usize>,
        chunks: &Segments,
    ) -> Result<Keys> {
        let chunk = self.key_column().group(row_group);
        let rows = match page {
            Some(page) => chunk.page_rows(page),
            None => 0..chunk.rows(),
        };
        let keys = decode_run(
            chunk,
            self.layout.key_shape.run_shape(),
            rows,
            Arc::new(chunks.clone()),
        )?;
        check_utf8(chunk, &keys)?;
        Ok(keys)
    }

    /// The key column chunks for overlap verification, which decodes every
    /// key of the file: Full hands out the resident chunks at no cost; Disk
    /// reads every chunk back from the chunk file, one counted local read
    /// per row group and no GET; Bloom holds no key bytes and fetches them
    /// again from storage, one counted load GET per row group.
    pub(crate) async fn key_chunks_for_verification(&self) -> Result<Arc<Segments>> {
        match &self.keys {
            KeyResidency::Chunks(chunks) => Ok(Arc::clone(chunks)),
            KeyResidency::Disk(_) | KeyResidency::Filters(_) => {
                let ranges =
                    column_chunk_ranges(&self.metadata, self.layout.key_leaf, self.handle.size());
                let parts = self.load_parts(&ranges).await?;
                Ok(Arc::new(Segments::new(parts, self.handle.size())))
            }
        }
    }

    /// The bytes of `ranges` of the file, in order, read at load: under
    /// Disk from the chunk file, one counted local read per range and no
    /// GET; under Full and Bloom by load GET (`fetch_chunk_parts`). For
    /// the overlap check, the doctor, and an ordered table's merge; a
    /// build never reads its chunks back.
    async fn load_parts(&self, ranges: &[Range<u64>]) -> Result<Vec<(Range<u64>, Bytes)>> {
        match &self.keys {
            KeyResidency::Disk(disk) => {
                let mut parts = Vec::with_capacity(ranges.len());
                for range in ranges {
                    parts.push((range.clone(), disk.page(range.clone()).await?));
                }
                Ok(parts)
            }
            KeyResidency::Chunks(_) | KeyResidency::Filters(_) => {
                fetch_chunk_parts(&self.handle, ranges).await
            }
        }
    }

    /// The file's column `leaf` as the decoders see it.
    fn column(&self, leaf: usize) -> Column<'_> {
        Column {
            metadata: &self.metadata,
            leaf,
            path: &self.handle.location,
        }
    }

    fn key_column(&self) -> Column<'_> {
        self.column(self.layout.key_leaf)
    }

    fn value_column(&self, value: ValueColumn) -> Column<'_> {
        self.column(value.leaf)
    }

    /// The order column of an ordered file.
    fn order_leaf_of(&self) -> OrderLeaf {
        self.layout
            .order
            .expect("a Scan or a pair read reads an ordered file")
    }

    /// The order column's chunk in `row_group` of an ordered file.
    fn order_chunk(&self, row_group: usize) -> ColumnChunk<'_> {
        self.column(self.order_leaf_of().leaf).group(row_group)
    }

    /// The row group and key page that can hold `key`, from the exact keys
    /// alone: the last row group whose first key is at or below `key`,
    /// provided its last key is at or above it, then the last page of that
    /// row group whose first key is at or below `key`. `None` for a key
    /// outside the file's range or between two row groups.
    fn locate(&self, key: &Key) -> Option<(usize, usize)> {
        if !self.may_contain(key) {
            return None;
        }
        let at = self.groups.partition_point(|group| group.first <= *key);
        let group = self.groups.get(at.checked_sub(1)?)?;
        if group.last < *key {
            return None;
        }
        let fences = &self.fences[group.row_group];
        let page = fences
            .partition_point(|fence| fence <= key)
            .checked_sub(1)?;
        Some((group.row_group, page))
    }

    /// Finds `key` in key page `page` of `row_group`, read from `segments`
    /// (`find_in_run`). The page's bytes are ones `verify` checked at
    /// build, so they are searched as read and not checked as UTF-8 again.
    fn search(
        &self,
        key: &Key,
        row_group: usize,
        page: usize,
        segments: Arc<Segments>,
    ) -> Result<Option<RowRef>> {
        let chunk = self.key_column().group(row_group);
        let rows = chunk.page_rows(page);
        let (found, _decoder) =
            find_in_run(chunk, self.layout.key_shape, rows.clone(), key, segments)?;
        Ok(found.map(|at| RowRef {
            row_group,
            row: rows.start + row_offset(at),
        }))
    }

    /// The page of `value` holding `row`, or `None` for a row outside its
    /// row group or a row group the file does not have.
    fn value_page_index(&self, value: ValueColumn, row: RowRef) -> Option<usize> {
        let rows = self.metadata.row_groups().get(row.row_group)?.num_rows();
        if row.row < 0 || row.row >= rows {
            return None;
        }
        self.value_column(value)
            .group(row.row_group)
            .page_locations()
            .partition_point(|location| location.first_row_index <= row.row)
            .checked_sub(1)
    }

    /// The page of `value` holding `row`, or `FileFault::PageOutOfRange`.
    fn value_page(&self, value: ValueColumn, row: RowRef) -> Result<usize> {
        self.value_page_index(value, row).ok_or_else(|| {
            file_fault(
                &self.handle.location,
                FileFault::PageOutOfRange {
                    row: row.row,
                    row_group: row.row_group,
                },
            )
        })
    }

    /// Page `page` of `chunk` and, when the column chunk has a dictionary
    /// page, that page too, from `source`, as the `Segments`
    /// the column reader decodes from without further I/O. The dictionary
    /// page is found the way the reader finds it, as the gap between the
    /// chunk's start and its first data page, so the reader asks for exactly
    /// what is held. Both ranges are known before either read starts, so
    /// the two reads run together: the dictionary costs its GET or local
    /// read, never a second round trip's wait. The origins returned are the
    /// page's and the dictionary page's.
    async fn fetch_pages(
        &self,
        chunk: ColumnChunk<'_>,
        page: usize,
        source: PageSource<'_>,
    ) -> Result<(Arc<Segments>, Origins)> {
        let range = chunk.page_range(page);
        let dictionary = chunk.dictionary_range();
        let read_dictionary = async {
            match dictionary.clone() {
                Some(dictionary) => self.read_from(source, dictionary).await.map(Some),
                None => Ok(None),
            }
        };
        let ((page_bytes, origin), dictionary_read) =
            futures::try_join!(self.read_from(source, range.clone()), read_dictionary)?;
        let mut parts = Vec::with_capacity(2);
        parts.push((range, page_bytes));
        let mut origins = Origins {
            page: origin,
            dictionary: None,
        };
        if let (Some(dictionary), Some((bytes, origin))) = (dictionary, dictionary_read) {
            parts.push((dictionary, bytes));
            origins.dictionary = Some(origin);
        }
        Ok((Arc::new(Segments::new(parts, self.handle.size())), origins))
    }

    /// One byte range of the file from `source`: a hedged lookup GET through
    /// the cache, one counted local read, or one load GET. A chunk-file read
    /// is `Cached`: it cost no GET, which is what the origin distinguishes.
    async fn read_from(
        &self,
        source: PageSource<'_>,
        range: Range<u64>,
    ) -> Result<(Bytes, PageOrigin)> {
        match source {
            PageSource::Store(cache, issue) => self.handle.read_page(range, cache, issue).await,
            PageSource::Disk(disk) => disk
                .page(range)
                .await
                .map(|bytes| (bytes, PageOrigin::Cached)),
            PageSource::Load => self
                .handle
                .read_range(range)
                .await
                .map(|bytes| (bytes, PageOrigin::Fetched)),
        }
    }
}

/// One column of the file as the decoders see it: the footer, the leaf,
/// and the path errors name.
#[derive(Clone, Copy)]
struct Column<'a> {
    metadata: &'a ParquetMetaData,
    leaf: usize,
    path: &'a str,
}

impl<'a> Column<'a> {
    fn descriptor(&self) -> ColumnDescPtr {
        self.metadata
            .file_metadata()
            .schema_descr()
            .column(self.leaf)
    }

    /// Column `leaf` of the same file.
    fn with_leaf(self, leaf: usize) -> Self {
        Self { leaf, ..self }
    }

    /// The column's chunk in `row_group`, which is below the footer's row
    /// group count. Every question about one row group is asked of the
    /// chunk, so the index is named once and cannot drift between calls.
    fn group(self, row_group: usize) -> ColumnChunk<'a> {
        ColumnChunk {
            column: self,
            row_group,
        }
    }
}

/// One column chunk: a `Column` in one row group, the row group index
/// carried once for every per-row-group read and check.
#[derive(Clone, Copy)]
struct ColumnChunk<'a> {
    column: Column<'a>,
    row_group: usize,
}

impl<'a> ColumnChunk<'a> {
    /// The row group the chunk lies in; what a fault names.
    fn row_group(&self) -> usize {
        self.row_group
    }

    /// The chunk's entry in the footer.
    fn metadata(&self) -> &'a ColumnChunkMetaData {
        self.column
            .metadata
            .row_group(self.row_group)
            .column(self.column.leaf)
    }

    fn rows(&self) -> i64 {
        self.column.metadata.row_group(self.row_group).num_rows()
    }

    /// The chunk's page locations; build required them.
    fn page_locations(&self) -> &'a [PageLocation] {
        self.column
            .metadata
            .offset_index()
            .expect("build required an offset index on the key and every value column")
            [self.row_group][self.column.leaf]
            .page_locations()
    }

    fn page_range(&self, page: usize) -> Range<u64> {
        page_range(&self.page_locations()[page])
    }

    /// The dictionary page's range when the chunk has one: the bytes between
    /// the chunk's start and its first data page.
    fn dictionary_range(&self) -> Option<Range<u64>> {
        let (start, _) = self.metadata().byte_range();
        let first = page_range(self.page_locations().first()?).start;
        (first != start).then_some(start..first)
    }

    /// The rows page `page` holds, within the row group.
    fn page_rows(&self, page: usize) -> Range<i64> {
        let locations = self.page_locations();
        let start = locations[page].first_row_index;
        let end = locations
            .get(page + 1)
            .map_or_else(|| self.rows(), |next| next.first_row_index);
        start..end
    }

    /// The page holding row `row`, which is inside the row group; the last
    /// page for a row at or past its end.
    fn page_holding(&self, row: usize) -> usize {
        let locations = self.page_locations();
        locations
            .partition_point(|location| row_index(location.first_row_index) <= row)
            .saturating_sub(1)
            .min(locations.len().saturating_sub(1))
    }

    /// Whether the data pages a reader took from the chunk are the ones
    /// the offset index names for rows `rows`, each holding the rows the
    /// index gives it: `seen` is the value count in the header of every
    /// data page the reader read, in order, starting at the page holding
    /// the first row (no page before it is read), and `decoded` is how many
    /// rows it produced, or the error the decode ended in. No rows wanted
    /// means no page read. A disagreement is `FileFault::PageRowsMismatch`
    /// naming the first page that does not fit. The header counts are
    /// checked before a decode error is returned, so a page that ran out
    /// of values short of the count its header claims is refused as the
    /// mismatch it is.
    fn check_pages_read(
        &self,
        rows: Range<usize>,
        seen: &[usize],
        decoded: parquet::errors::Result<usize>,
    ) -> Result<()> {
        let (first, expected) = if rows.is_empty() {
            (0, 0)
        } else {
            let first = self.page_holding(rows.start);
            let last = self.page_holding(rows.end - 1);
            (first, last - first + 1)
        };
        for (page, values) in (first..first + expected).zip(seen) {
            let span = self.page_rows(page);
            if *values != row_index(span.end - span.start) {
                return Err(self.page_rows_mismatch(page));
            }
        }
        let decoded = decoded?;
        if seen.len() != expected {
            return Err(self.page_rows_mismatch(first + seen.len().min(expected.saturating_sub(1))));
        }
        if decoded != rows.end - rows.start {
            return Err(self.page_rows_mismatch(self.page_holding(rows.start + decoded)));
        }
        Ok(())
    }

    /// `FileFault::PageRowsMismatch` for data page `page` of the chunk.
    fn page_rows_mismatch(&self, page: usize) -> Error {
        file_fault(
            self.column.path,
            FileFault::PageRowsMismatch {
                row_group: self.row_group,
                page,
            },
        )
    }
}

/// A column chunk's page starts, taken in page order as a check decodes
/// the chunk a window of rows at a time.
struct PageStarts<'a> {
    locations: &'a [PageLocation],
    /// The page whose start is taken next.
    next: usize,
}

impl<'a> PageStarts<'a> {
    fn of(chunk: ColumnChunk<'a>) -> Self {
        Self {
            locations: chunk.page_locations(),
            next: 0,
        }
    }

    /// The pages not yet taken that start in the `rows` rows from row
    /// `base`, each as its first row's offset among them, taken as yielded.
    fn within(&mut self, base: usize, rows: usize) -> impl Iterator<Item = usize> {
        std::iter::from_fn(move || {
            let location = self.locations.get(self.next)?;
            let at = row_index(location.first_row_index)
                .checked_sub(base)
                .filter(|at| *at < rows)?;
            self.next += 1;
            Some(at)
        })
    }
}

/// A decoded value as `Bytes`: a slice of the fetched page in `segments`
/// when parquet decoded it in place there, as it does from an uncompressed
/// page, so the value shares the page's buffer and holds it only while the
/// value is referenced; otherwise a copy, since a value decoded from a
/// compressed page lies in the buffer the page was decompressed into, and a
/// slice of it would hold the whole decompressed page for as long as the
/// value is referenced.
fn value_bytes(segments: &Segments, value: &[u8]) -> Bytes {
    segments
        .parts()
        .iter()
        .map(|(_, bytes)| bytes)
        .find(|bytes| lies_within(bytes, value))
        .map_or_else(
            || Bytes::copy_from_slice(value),
            |bytes| bytes.slice_ref(value),
        )
}

/// Whether `inner` is a subslice of `outer`'s memory.
fn lies_within(outer: &[u8], inner: &[u8]) -> bool {
    let outer = outer.as_ptr_range();
    let inner = inner.as_ptr_range();
    outer.start <= inner.start && inner.end <= outer.end
}

/// The identity a chunk file is named by and checked against, for a handle
/// whose footer has been loaded.
pub(crate) fn identity_of(handle: &FileHandle) -> DataFileIdentity {
    DataFileIdentity {
        path: handle.location.clone(),
        size: handle.size(),
        footer_digest: handle
            .footer_digest()
            .expect("load_metadata set the footer digest before the footer was inspected"),
    }
}

/// The byte range of a page from its offset-index location; build checked
/// every location's offset and size.
fn page_range(location: &PageLocation) -> Range<u64> {
    let start =
        u64::try_from(location.offset).expect("page offsets were checked non-negative at build");
    let len = u64::try_from(location.compressed_page_size)
        .expect("page sizes were checked non-negative at build");
    start..start + len
}

/// Per row group of `metadata`, the position in the file of its first row,
/// for `FileIndex::position`. Inspect checked every row count non-negative
/// and their sum within `i64`, so no sum overflows.
fn group_starts(metadata: &ParquetMetaData) -> Vec<RowPosition> {
    metadata
        .row_groups()
        .iter()
        .scan(0, |before, group| {
            let start = RowPosition(*before);
            *before += approx::count(row_index(group.num_rows()));
            Some(start)
        })
        .collect()
}

/// A row count or row position as the reader takes it; build checked every
/// one non-negative.
fn row_index(rows: i64) -> usize {
    usize::try_from(rows).expect("row counts and positions were checked non-negative at build")
}

/// A position within a page as a row offset.
fn row_offset(at: usize) -> i64 {
    i64::try_from(at).expect("a page's rows fit i64")
}

/// `decode`, a synchronous call that hands bytes of the file at `path` to
/// parquet, with a panic inside it as `FileFault::DecoderPanicked` at the
/// part `part` names once the panic is caught. The caller states why
/// `decode` is unwind-safe: what it mutates is dropped with the panic, or
/// left whole.
pub(crate) fn unless_panicked<T>(
    path: &str,
    decode: impl FnOnce() -> T + UnwindSafe,
    part: impl FnOnce() -> DecodedPart,
) -> Result<T> {
    std::panic::catch_unwind(decode).map_err(|payload| decoder_panicked(path, part(), &*payload))
}

/// `FileFault::DecoderPanicked` at `part` of the file at `path`, for a
/// panic whose payload is `payload`.
pub(crate) fn decoder_panicked(
    path: &str,
    part: DecodedPart,
    payload: &(dyn std::any::Any + Send),
) -> Error {
    let message = payload
        .downcast_ref::<&str>()
        .map(|message| (*message).to_string())
        .or_else(|| payload.downcast_ref::<String>().cloned())
        .unwrap_or_else(|| "a panic that carries no message".to_string());
    file_fault(path, FileFault::DecoderPanicked { part, message })
}

/// `Error::File` for the file at `path`.
fn file_fault(path: &str, fault: FileFault) -> Error {
    Error::File {
        path: path.to_string(),
        fault,
    }
}

#[cfg(test)]
mod tests {

    use parquet::file::page_index::column_index::ColumnIndexMetaData;

    use super::*;
    use crate::approx::count;
    use crate::config::HedgeConfig;
    use crate::contract::PAGE_ROWS;
    use crate::generation::testing::{self, Dictionary, KeyColumn, cache, column_of, page_rows};
    use crate::index::page_header::{MOST_VALUE_PAGE_BYTES, PageBody, PageHeader};
    use crate::index::testing::{
        BINARY_KEY_TAG, FEW_ROWS, FIXED_KEY_BYTES, Fixture, INFLATED_GROUP_ROWS, Opened,
        PAGES_PER_GROUP, ROW_GROUPS, SMALL_BATCH, VALUE_BYTES, ValueColumn, absent_after,
        contract_rows, fault_of, group_rows, header_at, key_for, location, long_orders,
        moved_by_header, pages_of, predicted_of, row_of, stepped, store, store_file,
        store_hedged_by, value_for, value_of_len, with_dictionary_one_entry_short, with_group_rows,
        with_header_claims, with_pages,
    };
    use crate::order::OrderType;
    use crate::storage::{HedgeWindow, IoStats};
    use std::time::Duration;

    use parquet::basic::{Compression, ZstdLevel};
    use parquet::file::properties::{EnabledStatistics, WriterVersion};

    /// Row groups in the fixtures that route across truncated or absent
    /// statistics, so an earlier row group's bound covers later ones.
    const MANY_ROW_GROUPS: usize = 4;

    /// Bytes per value in the misaligned fixture: enough that the page byte
    /// bound splits value pages far more often than the row limit.
    const MISALIGNED_VALUE_BYTES: usize = 512;
    /// Bytes every long-prefix key shares, longer than `TRUNCATE_BYTES`, so
    /// every truncated statistic of the file is one and the same prefix.
    const LONG_PREFIX_BYTES: usize = 32;
    /// The truncation length of the long-prefix fixture's statistics and
    /// column index.
    const TRUNCATE_BYTES: usize = 16;
    /// The byte the long binary prefix repeats.
    const PREFIX_BYTE: u8 = 0x01;
    /// The page byte bound of the misaligned fixture: above a key page of
    /// `PAGE_ROWS` longs, below one of `PAGE_ROWS` string keys, and far
    /// below `PAGE_ROWS` values of `MISALIGNED_VALUE_BYTES`.
    const MISALIGNED_PAGE_BYTES: usize = 16 * 1024;
    /// Every `NULL_EVERY`th value of the nullable fixture is null.
    const NULL_EVERY: usize = 7;
    /// A key below every key `key_for` produces.
    fn below_range(key_type: KeyType) -> Key {
        match key_type {
            KeyType::Long => Key::Long(-1),
            KeyType::String => Key::Str("a".to_string()),
            KeyType::Binary => Key::Bytes(vec![0]),
            KeyType::Uuid => Key::Uuid([0; UUID_BYTES]),
        }
    }

    /// A key above every key `key_for` produces.
    fn above_range(key_type: KeyType) -> Key {
        match key_type {
            KeyType::Long => Key::Long(i64::MAX),
            KeyType::String => Key::Str("v".to_string()),
            KeyType::Binary => Key::Bytes(vec![BINARY_KEY_TAG, BINARY_KEY_TAG]),
            KeyType::Uuid => Key::Uuid([u8::MAX; UUID_BYTES]),
        }
    }

    /// The length in bytes of every key `key_for` produces for `key_type`.
    fn key_bytes(key_type: KeyType) -> usize {
        key_for(0, key_type).as_bytes().len()
    }

    impl FileIndex {
        /// This index over `metadata` in place of the footer it was built
        /// with, so a lookup runs against a footer the file contradicts.
        fn with_metadata(self, metadata: ParquetMetaData) -> Self {
            Self {
                metadata: Arc::new(metadata),
                ..self
            }
        }
    }

    /// Whether `value` lies in the memory of one of the bytes `segments`
    /// holds.
    fn shares_a_buffer(segments: &Segments, value: &Bytes) -> bool {
        segments
            .parts()
            .iter()
            .any(|(_, bytes)| lies_within(bytes, value))
    }

    /// A value read from an uncompressed value page is a slice of the page
    /// the cache holds, not a copy; one read from a zstd page, the
    /// contract's codec, is a copy out of the buffer the page was
    /// decompressed into, and shares no fetched page.
    /// The rows the first page of value leaf `leaf` holds in `row_group`,
    /// from the offset index.
    fn first_page_rows(metadata: &ParquetMetaData, row_group: usize, leaf: usize) -> usize {
        let locations = metadata.offset_index().unwrap()[row_group][leaf].page_locations();
        let end = locations.get(1).map_or_else(
            || metadata.row_group(row_group).num_rows(),
            |next| next.first_row_index,
        );
        row_index(end - locations[0].first_row_index)
    }

    #[tokio::test]
    async fn test_first_page_values_reads_the_first_row_group_holding_rows_nulls_left_out() {
        let every_other = 2;
        let mut fixture = Fixture::contract(KeyType::Long, contract_rows());
        for (row, value) in fixture.values.iter_mut().enumerate() {
            if row % every_other == 1 {
                *value = None;
            }
        }
        // One sample short of a full window: a hedged lookup read would
        // fill it and give the window a delay; a load read records nothing.
        let hedge = HedgeConfig::default();
        let window = Arc::new(HedgeWindow::new(&hedge));
        for _ in 1..hedge.window {
            window.record(Duration::ZERO, false);
        }
        let opened = store_hedged_by(&fixture, Arc::clone(&window)).await;
        let mut index = opened.index(KeyType::Long).await;
        let leaf = index.value_leaf(0).unwrap();
        let page_of = |metadata: &ParquetMetaData, row_group: usize| -> Vec<Bytes> {
            let first = row_group * group_rows();
            (first..first + first_page_rows(metadata, row_group, leaf))
                .filter(|row| row % every_other == 0)
                .map(|row| Bytes::from(value_for(row)))
                .collect()
        };
        let expected = page_of(&index.metadata, 0);
        assert!(!expected.is_empty());
        let before = opened.stats().gets();
        assert_eq!(index.first_page_values(0).await.unwrap(), Some(expected));
        assert_eq!(
            opened.stats().gets() - before,
            1,
            "one GET past the page cache"
        );
        assert_eq!(window.delay(), None, "a load GET, kept out of the window");
        let second = page_of(&index.metadata, 1);
        index.metadata = Arc::new(with_pages(
            &with_group_rows(&index.metadata, 0, 0),
            leaf,
            Vec::new(),
        ));
        assert_eq!(
            index.first_page_values(0).await.unwrap(),
            Some(second),
            "a first row group of no rows is passed over"
        );
    }

    #[tokio::test]
    async fn test_read_value_shares_the_buffer_of_an_uncompressed_page() {
        for (compression, shared) in [
            (Compression::UNCOMPRESSED, true),
            (Compression::ZSTD(ZstdLevel::default()), false),
        ] {
            let mut fixture = Fixture::contract(KeyType::Long, contract_rows());
            fixture.layout.compression = compression;
            let opened = store(&fixture).await;
            let index = opened.index(KeyType::Long).await;
            let cache = cache().await;
            let at = page_rows() + 1;
            let row = index
                .find(&key_for(at, KeyType::Long), Some(&cache), Issue::default())
                .await
                .unwrap()
                .unwrap();
            let (value, _) = index.read_value(row, Some(&cache)).await.unwrap().unwrap();
            let value = value.unwrap();
            assert_eq!(value, value_for(at).as_slice());
            let value_column = index.layout.values[0].unwrap();
            let chunk = index.value_column(value_column).group(row.row_group);
            let page = index.value_page(value_column, row).unwrap();
            let (cached, origins) = index
                .fetch_pages(
                    chunk,
                    page,
                    PageSource::Store(Some(&cache), Issue::default()),
                )
                .await
                .unwrap();
            assert_eq!(origins.page, PageOrigin::Cached);
            assert_eq!(shares_a_buffer(&cached, &value), shared, "{compression}");
        }
    }

    /// Finds every written key at its row and reads its value back byte for
    /// byte through `cache`, checking that a find costs no GET, a cold hit
    /// one, and a warm hit none. Returns the distinct value pages fetched.
    async fn check_every_key(
        index: &FileIndex,
        stats: &IoStats,
        key_type: KeyType,
        rows: usize,
    ) -> usize {
        let cache = cache().await;
        let group_rows = usize::try_from(index.metadata().row_group(0).num_rows()).unwrap();
        let before = stats.gets();
        let mut fetched = 0;
        for i in 0..rows {
            let key = key_for(i, key_type);
            let row = index
                .find(&key, Some(&cache), Issue::default())
                .await
                .unwrap()
                .unwrap_or_else(|| panic!("key {key} not found"));
            assert_eq!(row, row_of(i, group_rows), "key {key}");
            assert_eq!(stats.gets(), before + fetched, "find costs no GET");
            let cold = cache.fetches() == fetched;
            let (value, origin) = index.read_value(row, Some(&cache)).await.unwrap().unwrap();
            assert_eq!(value.as_deref(), Some(value_for(i).as_slice()), "key {key}");
            if cache.fetches() > fetched {
                assert!(cold);
                assert_eq!(origin, PageOrigin::Fetched, "key {key}");
                fetched += 1;
            } else {
                assert_eq!(origin, PageOrigin::Cached, "key {key}");
            }
            assert_eq!(
                stats.gets(),
                before + fetched,
                "a hit costs one GET cold and none warm"
            );
        }
        assert_eq!(stats.hedges(), 0);
        assert_eq!(cache.fetches(), fetched);
        assert_eq!(cache.hits(), count(rows) - fetched);
        approx::length(fetched)
    }

    #[tokio::test]
    async fn test_find_and_read_value_every_key_per_type() {
        for key_type in KeyType::ALL {
            let rows = contract_rows();
            let opened = store(&Fixture::contract(key_type, rows)).await;
            let index = opened.index(key_type).await;
            let stats = opened.stats();
            assert_eq!(
                stats.gets(),
                1 + count(ROW_GROUPS),
                "footer, then one chunk per row group"
            );
            assert_eq!(index.residency(), Residency::Full);
            assert_eq!(index.rows(), i64::try_from(rows).unwrap());
            assert_eq!(index.fences().len(), ROW_GROUPS);
            for row_group in 0..ROW_GROUPS {
                assert_eq!(index.page_count(row_group), PAGES_PER_GROUP);
                assert_eq!(
                    index.fences()[row_group][0],
                    key_for(row_group * group_rows(), key_type)
                );
                assert_eq!(
                    index.key_column().group(row_group).page_rows(1),
                    PAGE_ROWS..PAGE_ROWS * 2
                );
            }
            assert_eq!(
                index.key_range(),
                (&key_for(0, key_type), &key_for(rows - 1, key_type))
            );
            assert_eq!(index.max_key_bytes(), key_bytes(key_type));
            assert_eq!(index.metadata().num_row_groups(), ROW_GROUPS);
            assert!(Arc::ptr_eq(index.handle(), &opened.handle));

            let pages = check_every_key(&index, &stats, key_type, rows).await;
            assert_eq!(pages, ROW_GROUPS * PAGES_PER_GROUP);
        }
    }

    #[tokio::test]
    async fn test_find_miss_costs_no_get_inside_or_outside_the_range_per_type() {
        for key_type in KeyType::ALL {
            let opened = store(&Fixture::contract(key_type, contract_rows())).await;
            let index = opened.index(key_type).await;
            let stats = opened.stats();
            let cache = cache().await;
            let before = stats.gets();
            let inside = [
                0,
                page_rows() - 1,
                page_rows(),
                group_rows() - 1,
                group_rows(),
                contract_rows() - 2,
            ];
            for i in inside {
                let key = absent_after(&key_for(i, key_type));
                assert!(index.may_contain(&key), "{key}");
                // The key after a row group's last key falls between two row
                // groups: no page can hold it, so none is named.
                let between_groups = (i + 1) % group_rows() == 0;
                assert_eq!(index.page_of(&key).is_some(), !between_groups, "{key}");
                assert_eq!(
                    index
                        .find(&key, Some(&cache), Issue::default())
                        .await
                        .unwrap(),
                    None,
                    "{key}"
                );
            }
            for key in [below_range(key_type), above_range(key_type)] {
                assert!(!index.may_contain(&key));
                assert_eq!(index.page_of(&key), None);
                assert_eq!(
                    index
                        .find(&key, Some(&cache), Issue::default())
                        .await
                        .unwrap(),
                    None
                );
            }
            assert_eq!(stats.gets(), before);
            assert_eq!(cache.fetches(), 0);
        }
    }

    /// `key_for` behind a shared prefix longer than the statistics
    /// truncation; longs and uuids have no prefix to share.
    fn long_prefix_key(i: usize, key_type: KeyType) -> Key {
        match key_for(i, key_type) {
            Key::Str(text) => Key::Str(format!("{}{text}", "p".repeat(LONG_PREFIX_BYTES))),
            Key::Bytes(bytes) => Key::Bytes([vec![PREFIX_BYTE; LONG_PREFIX_BYTES], bytes].concat()),
            Key::Long(_) | Key::Uuid(_) => panic!("prefixed keys are strings or bytes"),
        }
    }

    /// Every key of a `MANY_ROW_GROUPS` file whose keys share a prefix
    /// longer than `TRUNCATE_BYTES` is found in every tier: row-group and
    /// page choice is by the exact keys, never by the truncated bounds, which
    /// are one prefix for every row group.
    async fn check_long_prefix_lookups(key_type: KeyType, residency: Residency) {
        let rows = contract_rows();
        let dir = tempfile::tempdir().unwrap();
        let mut fixture = Fixture::with_keys(
            column_of(key_type),
            (0..rows)
                .map(|i| Some(long_prefix_key(i, key_type)))
                .collect(),
        );
        fixture.layout.truncate = Some(TRUNCATE_BYTES);
        fixture.layout.row_group_rows = rows / MANY_ROW_GROUPS;
        let opened = store(&fixture).await;
        let index = opened
            .build_in(key_type, residency, dir.path())
            .await
            .unwrap();
        assert_eq!(index.residency(), residency);
        assert_eq!(index.metadata().num_row_groups(), MANY_ROW_GROUPS);
        assert!(index.max_key_bytes() > LONG_PREFIX_BYTES);
        let statistics = index
            .metadata()
            .row_group(0)
            .column(index.key_leaf())
            .statistics()
            .unwrap();
        assert!(
            !statistics.max_is_exact()
                && statistics.max_bytes_opt().unwrap().len() <= TRUNCATE_BYTES
        );
        let cache = cache().await;
        for i in 0..rows {
            let key = long_prefix_key(i, key_type);
            let row = index
                .find(&key, Some(&cache), Issue::default())
                .await
                .unwrap()
                .unwrap_or_else(|| panic!("key {key} not found in {residency}"));
            assert_eq!(row, row_of(i, rows / MANY_ROW_GROUPS), "key {key}");
            let (value, _) = index.read_value(row, Some(&cache)).await.unwrap().unwrap();
            assert_eq!(value.as_deref(), Some(value_for(i).as_slice()));
        }
        let group_rows = rows / MANY_ROW_GROUPS;
        for i in [0, group_rows - 1, group_rows, rows - 2] {
            let key = absent_after(&long_prefix_key(i, key_type));
            assert_eq!(
                index
                    .find(&key, Some(&cache), Issue::default())
                    .await
                    .unwrap(),
                None,
                "{key}"
            );
        }
    }

    #[tokio::test]
    async fn test_find_long_prefix_string_keys_across_truncated_row_group_statistics() {
        for residency in Residency::ALL {
            check_long_prefix_lookups(KeyType::String, residency).await;
        }
    }

    #[tokio::test]
    async fn test_find_long_prefix_binary_keys_across_truncated_row_group_statistics() {
        for residency in Residency::ALL {
            check_long_prefix_lookups(KeyType::Binary, residency).await;
        }
    }

    #[tokio::test]
    async fn test_find_every_key_with_statistics_disabled_across_row_groups_per_type() {
        for key_type in KeyType::ALL {
            let rows = contract_rows();
            let mut fixture = Fixture::contract(key_type, rows);
            fixture.layout.statistics = EnabledStatistics::None;
            fixture.layout.row_group_rows = rows / MANY_ROW_GROUPS;
            let opened = store(&fixture).await;
            let index = opened.index(key_type).await;
            assert_eq!(index.metadata().num_row_groups(), MANY_ROW_GROUPS);
            let key_chunk = index.metadata().row_group(0).column(index.key_leaf());
            assert!(key_chunk.statistics().is_none(), "no row-group statistics");
            assert_eq!(
                index.metadata().column_index().unwrap()[0][index.key_leaf()],
                ColumnIndexMetaData::NONE,
                "no column index"
            );
            let pages = check_every_key(&index, &opened.stats(), key_type, rows).await;
            let group_rows = rows / MANY_ROW_GROUPS;
            assert_eq!(pages, MANY_ROW_GROUPS * group_rows.div_ceil(page_rows()));
            let cache = cache().await;
            for i in [group_rows - 1, group_rows, 2 * group_rows - 1] {
                let key = absent_after(&key_for(i, key_type));
                assert_eq!(
                    index
                        .find(&key, Some(&cache), Issue::default())
                        .await
                        .unwrap(),
                    None,
                    "{key}"
                );
            }
        }
    }

    #[tokio::test]
    async fn test_read_value_resolves_the_right_page_when_key_and_value_pages_misalign() {
        for key_type in [KeyType::Long, KeyType::String] {
            let rows = contract_rows();
            let mut fixture = Fixture::contract(key_type, rows);
            fixture.values = (0..rows)
                .map(|i| Some(value_of_len(i, MISALIGNED_VALUE_BYTES)))
                .collect();
            fixture.layout.page_bytes = MISALIGNED_PAGE_BYTES;
            fixture.layout.write_batch = SMALL_BATCH;
            let opened = store(&fixture).await;
            let index = opened.index(key_type).await;
            let offset_index = index.metadata().offset_index().unwrap();
            let key_pages = offset_index[0][index.key_leaf()].page_locations().len();
            let value_pages = offset_index[0][index.value_leaf(0).unwrap()]
                .page_locations()
                .len();
            assert!(
                value_pages > key_pages,
                "{value_pages} value pages against {key_pages} key pages"
            );
            let cache = cache().await;
            for i in 0..rows {
                let row = index
                    .find(&key_for(i, key_type), Some(&cache), Issue::default())
                    .await
                    .unwrap()
                    .unwrap();
                let (value, _) = index.read_value(row, Some(&cache)).await.unwrap().unwrap();
                assert_eq!(
                    value.as_deref(),
                    Some(value_of_len(i, MISALIGNED_VALUE_BYTES).as_slice())
                );
            }
            let total_value_pages: usize = (0..ROW_GROUPS)
                .map(|row_group| {
                    offset_index[row_group][index.value_leaf(0).unwrap()]
                        .page_locations()
                        .len()
                })
                .sum();
            assert_eq!(
                cache.fetches(),
                count(total_value_pages),
                "one GET per value page"
            );
        }
    }

    #[tokio::test]
    async fn test_read_value_dictionary_encoded_value_costs_one_extra_get_and_key_none() {
        let rows = contract_rows();
        let mut fixture = Fixture::contract(KeyType::Long, rows);
        fixture.layout.dictionary = Dictionary::All;
        let opened = store(&fixture).await;
        let index = opened.index(KeyType::Long).await;
        let stats = opened.stats();
        let group = index.metadata().row_group(0);
        assert!(
            group
                .column(index.key_leaf())
                .dictionary_page_offset()
                .is_some()
        );
        assert!(
            group
                .column(index.value_leaf(0).unwrap())
                .dictionary_page_offset()
                .is_some()
        );
        let cache = cache().await;
        let before = stats.gets();
        let first = page_rows() + 1;
        let row = index
            .find(
                &key_for(first, KeyType::Long),
                Some(&cache),
                Issue::default(),
            )
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            stats.gets(),
            before,
            "the key dictionary page is in the resident chunk"
        );
        let (value, origin) = index.read_value(row, Some(&cache)).await.unwrap().unwrap();
        assert_eq!(value.as_deref(), Some(value_for(first).as_slice()));
        assert_eq!(origin, PageOrigin::Fetched);
        assert_eq!(
            stats.gets(),
            before + 2,
            "the value page and its dictionary page"
        );
        let second = first + 1;
        let row = index
            .find(
                &key_for(second, KeyType::Long),
                Some(&cache),
                Issue::default(),
            )
            .await
            .unwrap()
            .unwrap();
        let (value, origin) = index.read_value(row, Some(&cache)).await.unwrap().unwrap();
        assert_eq!(value.as_deref(), Some(value_for(second).as_slice()));
        assert_eq!(origin, PageOrigin::Cached);
        assert_eq!(stats.gets(), before + 2, "both pages cached");
        let other_page = 0;
        let row = index
            .find(
                &key_for(other_page, KeyType::Long),
                Some(&cache),
                Issue::default(),
            )
            .await
            .unwrap()
            .unwrap();
        let (_, origin) = index.read_value(row, Some(&cache)).await.unwrap().unwrap();
        assert_eq!(origin, PageOrigin::Fetched);
        assert_eq!(
            stats.gets(),
            before + 3,
            "another value page, the dictionary cached"
        );
    }

    #[tokio::test]
    async fn test_read_value_null_is_none_at_the_same_get_count() {
        let rows = contract_rows();
        // Nulls every `NULL_EVERY`th row and at both sides of a page edge
        // and at a row group's last row, so the skip over definition levels
        // to the last row of a page and the whole-page skip to the first row
        // of the next both land on a null.
        let edge_nulls = [page_rows() - 1, page_rows(), group_rows() - 1];
        let is_null = |i: usize| i.is_multiple_of(NULL_EVERY) || edge_nulls.contains(&i);
        let mut fixture = Fixture::contract(KeyType::Long, rows);
        fixture.values = (0..rows)
            .map(|i| (!is_null(i)).then(|| value_for(i)))
            .collect();
        let opened = store(&fixture).await;
        let index = opened.index(KeyType::Long).await;
        let stats = opened.stats();
        let cache = cache().await;
        let before = stats.gets();
        let null_row = NULL_EVERY * 3;
        assert!(null_row < page_rows() && !edge_nulls.contains(&null_row));
        let row = index
            .find(
                &key_for(null_row, KeyType::Long),
                Some(&cache),
                Issue::default(),
            )
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            index.read_value(row, Some(&cache)).await.unwrap().unwrap(),
            (None, PageOrigin::Fetched)
        );
        assert_eq!(
            stats.gets(),
            before + 1,
            "a null costs the same one GET cold"
        );
        let present = null_row + 1;
        let row = index
            .find(
                &key_for(present, KeyType::Long),
                Some(&cache),
                Issue::default(),
            )
            .await
            .unwrap()
            .unwrap();
        let (value, origin) = index.read_value(row, Some(&cache)).await.unwrap().unwrap();
        assert_eq!(value.as_deref(), Some(value_for(present).as_slice()));
        assert_eq!(origin, PageOrigin::Cached);
        assert_eq!(stats.gets(), before + 1, "and none warm");
        let row = index
            .find(&key_for(0, KeyType::Long), Some(&cache), Issue::default())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            index.read_value(row, Some(&cache)).await.unwrap().unwrap(),
            (None, PageOrigin::Cached)
        );
        assert_eq!(stats.gets(), before + 1);
        // The last row of value page 0 (cached), the first row of value
        // page 1 (one GET), and the last row of the row group (page 1 again).
        for (i, extra_gets, origin) in [
            (page_rows() - 1, 0, PageOrigin::Cached),
            (page_rows(), 1, PageOrigin::Fetched),
            (group_rows() - 1, 1, PageOrigin::Cached),
        ] {
            let row = index
                .find(&key_for(i, KeyType::Long), Some(&cache), Issue::default())
                .await
                .unwrap()
                .unwrap();
            assert_eq!(
                index.read_value(row, Some(&cache)).await.unwrap().unwrap(),
                (None, origin),
                "row {i}"
            );
            assert_eq!(stats.gets(), before + 1 + extra_gets, "row {i}");
            let neighbor = i - 1;
            let row = index
                .find(
                    &key_for(neighbor, KeyType::Long),
                    Some(&cache),
                    Issue::default(),
                )
                .await
                .unwrap()
                .unwrap();
            let (value, _) = index.read_value(row, Some(&cache)).await.unwrap().unwrap();
            let expected = (!is_null(neighbor)).then(|| value_for(neighbor));
            assert_eq!(value.as_deref(), expected.as_deref(), "row {neighbor}");
        }
    }

    /// A dictionary-encoded key column's dictionary page is in the resident
    /// chunk in the Full tier and costs a lookup nothing; in the Disk tier
    /// it is one more local read per find, since a chunk file caches
    /// nothing, and in the Bloom tier one more GET on the first positive in
    /// a row group and none after, since the page cache keeps it by offset.
    /// The value column stays plain, so a hit's value page costs what it
    /// does in the contract layout.
    #[tokio::test]
    async fn test_find_dictionary_encoded_key_costs_per_tier() {
        let rows = contract_rows();
        let dir = tempfile::tempdir().unwrap();
        let mut fixture = Fixture::contract(KeyType::Long, rows);
        fixture.layout.dictionary = Dictionary::Key;
        let opened = store(&fixture).await;
        let stats = opened.stats();
        for residency in Residency::ALL {
            let index = opened
                .build_in(KeyType::Long, residency, dir.path())
                .await
                .unwrap();
            let group = index.metadata().row_group(1);
            assert!(
                group
                    .column(index.key_leaf())
                    .dictionary_page_offset()
                    .is_some()
            );
            assert!(
                group
                    .column(index.value_leaf(0).unwrap())
                    .dictionary_page_offset()
                    .is_none()
            );
            let cache = cache().await;
            // Two keys on different key pages of row group 1, so the second
            // find reads another key page but the same dictionary page.
            let first = group_rows() + 1;
            let second = group_rows() + page_rows() + 1;
            let (find_gets, find_reads, again_gets, again_reads) = match residency {
                Residency::Full => (0, 0, 0, 0),
                Residency::Disk => (0, 2, 0, 2),
                Residency::Bloom => (2, 0, 1, 0),
            };
            let (gets, reads) = (stats.gets(), stats.local_reads());
            let row = index
                .find(
                    &key_for(first, KeyType::Long),
                    Some(&cache),
                    Issue::default(),
                )
                .await
                .unwrap()
                .unwrap();
            assert_eq!(row, row_of(first, group_rows()));
            assert_eq!(
                (stats.gets(), stats.local_reads()),
                (gets + find_gets, reads + find_reads),
                "{residency}: a find with a dictionary-encoded key column"
            );
            let (value, origin) = index.read_value(row, Some(&cache)).await.unwrap().unwrap();
            assert_eq!(value.as_deref(), Some(value_for(first).as_slice()));
            assert_eq!(origin, PageOrigin::Fetched, "{residency}");
            assert_eq!(
                (stats.gets(), stats.local_reads()),
                (gets + find_gets + 1, reads + find_reads),
                "{residency}: the value page is one GET"
            );
            let (gets, reads) = (stats.gets(), stats.local_reads());
            let row = index
                .find(
                    &key_for(second, KeyType::Long),
                    Some(&cache),
                    Issue::default(),
                )
                .await
                .unwrap()
                .unwrap();
            assert_eq!(row, row_of(second, group_rows()));
            assert_eq!(
                (stats.gets(), stats.local_reads()),
                (gets + again_gets, reads + again_reads),
                "{residency}: another key page of the same row group"
            );
        }
    }

    /// A reused file is checked against a later snapshot's layout from the
    /// footer the index keeps: its columns by name, then the key type the
    /// index decodes, with no read.
    #[tokio::test]
    async fn test_check_columns_resolves_the_kept_footer_with_no_read() {
        let opened = store(&Fixture::contract(KeyType::Long, FEW_ROWS)).await;
        let index = opened.index(KeyType::Long).await;
        let stats = opened.stats();
        let before = stats.gets();
        let layout = crate::manifest::testing::layout(KeyType::Long, None);
        assert!(index.check_columns(testing::TABLE, &layout).unwrap());
        let renamed = TableLayout {
            value_columns: vec![crate::manifest::testing::binary_column(
                "renamed",
                crate::manifest::testing::VALUE_FIELD_ID,
            )],
            ..layout.clone()
        };
        assert_eq!(
            testing::refusal_of(index.check_columns(testing::TABLE, &renamed)),
            RefusalReason::ColumnMismatch {
                column: "renamed".to_string(),
                path: Some(opened.handle.location.clone()),
                mismatch: crate::column::ColumnMismatch::AbsentWithoutFieldIds,
            }
        );
        let retyped = TableLayout {
            key_type: KeyType::String,
            ..layout
        };
        assert_eq!(
            testing::refusal_of(index.check_columns(testing::TABLE, &retyped)),
            RefusalReason::KeyTypeMismatch {
                path: opened.handle.location.clone(),
                expected: KeyType::String,
                got: parquet::basic::Type::INT64,
            }
        );
        assert_eq!(stats.gets(), before, "the check reads nothing");
    }

    /// A layout whose columns resolve, in the same footer, to other leaves
    /// than the index was built over, or that names another count of
    /// columns, is not one the index can answer: the check says so rather
    /// than let one column be served from another's pages.
    #[tokio::test]
    async fn test_check_columns_a_layout_resolving_to_other_leaves_is_not_reusable() {
        let opened = store(&Fixture::contract(KeyType::Long, FEW_ROWS)).await;
        let index = opened.index(KeyType::Long).await;
        let layout = crate::manifest::testing::layout(KeyType::Long, None);
        let other_leaf = TableLayout {
            value_columns: vec![crate::manifest::testing::binary_column(
                &layout.key_column,
                crate::manifest::testing::KEY_FIELD_ID,
            )],
            ..layout.clone()
        };
        assert_eq!(
            crate::column::resolve(
                testing::TABLE,
                &opened.handle.location,
                index.metadata.file_metadata().schema_descr(),
                &other_leaf
            )
            .unwrap()
            .value_leaves,
            [Some(index.key_leaf())],
            "the column resolves, to the key's leaf"
        );
        assert!(!index.check_columns(testing::TABLE, &other_leaf).unwrap());
        let fewer = TableLayout {
            value_columns: Vec::new(),
            ..layout.clone()
        };
        assert!(!index.check_columns(testing::TABLE, &fewer).unwrap());
        let more = TableLayout {
            value_columns: [layout.value_columns.clone(), layout.value_columns.clone()].concat(),
            ..layout.clone()
        };
        assert!(!index.check_columns(testing::TABLE, &more).unwrap());
        assert!(index.check_columns(testing::TABLE, &layout).unwrap());
    }

    /// A reused file is reused under a later snapshot's layout only when
    /// its order column resolves to the leaf it was built over and decodes
    /// as it did, and an order type that no longer admits the file's column
    /// refuses it as the footer stage would (RFD 025): an index built for a
    /// key-value table is not one for an ordered table, nor the reverse.
    #[tokio::test]
    async fn test_check_columns_compares_the_order_column_the_index_was_built_over() {
        let mut fixture = Fixture::contract(KeyType::Long, FEW_ROWS);
        fixture.value_column = ValueColumn::Int64;
        let opened = store(&fixture).await;
        let ordered = |order_type| TableLayout {
            value_columns: Vec::new(),
            order: Some(crate::order::OrderColumnLayout {
                name: crate::manifest::testing::VALUE_COLUMN.to_string(),
                field_id: crate::manifest::testing::VALUE_FIELD_ID,
                order_type,
            }),
            ..crate::manifest::testing::layout(KeyType::Long, None)
        };
        let long = ordered(OrderType::Long);
        let index = FileIndex::build(
            testing::TABLE,
            Arc::clone(&opened.handle),
            &long,
            Residency::Full,
            None,
        )
        .await
        .unwrap();
        assert!(index.check_columns(testing::TABLE, &long).unwrap());
        let key_value = TableLayout {
            order: None,
            ..long.clone()
        };
        assert!(!index.check_columns(testing::TABLE, &key_value).unwrap());
        let unordered = FileIndex::build(
            testing::TABLE,
            Arc::clone(&opened.handle),
            &key_value,
            Residency::Full,
            None,
        )
        .await
        .unwrap();
        assert!(unordered.check_columns(testing::TABLE, &key_value).unwrap());
        assert!(!unordered.check_columns(testing::TABLE, &long).unwrap());
        assert!(matches!(
            testing::refusal_of(
                index.check_columns(testing::TABLE, &ordered(OrderType::Timestamp))
            ),
            RefusalReason::OrderTypeMismatch {
                expected: OrderType::Timestamp,
                ..
            }
        ));
    }

    #[tokio::test]
    async fn test_find_fixed_width_binary_keys_as_binary() {
        let keys: Vec<Option<Key>> = (0..FEW_ROWS)
            .map(|i| Some(Key::Bytes(count(stepped(i)).to_be_bytes().to_vec())))
            .collect();
        let opened = store(&Fixture::with_keys(
            KeyColumn::FixedBinary(FIXED_KEY_BYTES),
            keys.clone(),
        ))
        .await;
        let index = opened.index(KeyType::Binary).await;
        assert_eq!(
            index.max_key_bytes(),
            usize::try_from(FIXED_KEY_BYTES).unwrap()
        );
        for (i, key) in keys.iter().enumerate() {
            let key = key.as_ref().unwrap();
            let row = index
                .find(key, None, Issue::default())
                .await
                .unwrap()
                .unwrap();
            assert_eq!(
                row,
                RowRef {
                    row_group: 0,
                    row: i64::try_from(i).unwrap()
                }
            );
            let (value, origin) = index.read_value(row, None).await.unwrap().unwrap();
            assert_eq!(value.as_deref(), Some(value_for(i).as_slice()));
            assert_eq!(origin, PageOrigin::Fetched);
            assert_eq!(
                index
                    .find(&absent_after(key), None, Issue::default())
                    .await
                    .unwrap(),
                None
            );
        }
    }

    #[tokio::test]
    async fn test_read_value_fixed_width_values_byte_for_byte() {
        let mut fixture = Fixture::contract(KeyType::Long, FEW_ROWS);
        fixture.value_column = ValueColumn::FixedBinary(i32::try_from(VALUE_BYTES).unwrap());
        let opened = store(&fixture).await;
        let index = opened.index(KeyType::Long).await;
        for i in 0..FEW_ROWS {
            let row = index
                .find(&key_for(i, KeyType::Long), None, Issue::default())
                .await
                .unwrap()
                .unwrap();
            let (value, origin) = index.read_value(row, None).await.unwrap().unwrap();
            assert_eq!(value.as_deref(), Some(value_for(i).as_slice()));
            assert_eq!(origin, PageOrigin::Fetched);
        }
    }

    /// The same check on the lookup path, for the one key page a find
    /// decodes and the one value page a read fetches: a page boundary the
    /// offset index has shifted by one row is a fault, never a neighbor's
    /// row or value.
    #[tokio::test]
    async fn test_lookup_refuses_a_page_whose_rows_differ_from_its_offset_index() {
        let opened = store(&Fixture::contract(KeyType::Long, contract_rows())).await;
        let index = opened.index(KeyType::Long).await;
        let (key_leaf, value_leaf) = (index.key_leaf(), index.value_leaf(0).unwrap());
        let kept = (**index.metadata()).clone();
        let last_of_page_zero = page_rows() - 1;

        let mut shifted_values = pages_of(&kept, value_leaf);
        shifted_values[1].first_row_index -= 1;
        let index = index.with_metadata(with_pages(&kept, value_leaf, shifted_values));
        let row = index
            .find(
                &key_for(last_of_page_zero, KeyType::Long),
                None,
                Issue::default(),
            )
            .await
            .unwrap()
            .unwrap();
        assert_eq!(row, row_of(last_of_page_zero, group_rows()));
        assert_eq!(
            fault_of(index.read_value(row, None).await),
            FileFault::PageRowsMismatch {
                row_group: 0,
                page: 1,
            }
        );

        let mut shifted_keys = pages_of(&kept, key_leaf);
        shifted_keys[1].first_row_index -= 1;
        let index = index.with_metadata(with_pages(&kept, key_leaf, shifted_keys));
        assert_eq!(
            fault_of(
                index
                    .find(
                        &key_for(last_of_page_zero, KeyType::Long),
                        None,
                        Issue::default()
                    )
                    .await
            ),
            FileFault::PageRowsMismatch {
                row_group: 0,
                page: 0,
            }
        );
        let restored = index.with_metadata(kept);
        assert_eq!(
            restored
                .find(
                    &key_for(last_of_page_zero, KeyType::Long),
                    None,
                    Issue::default()
                )
                .await
                .unwrap(),
            Some(row)
        );
    }

    /// The Bloom tier's costs on a `MANY_ROW_GROUPS` contract fixture: a
    /// cold hit is exactly two GETs, a warm hit none, a miss the filter
    /// rules out none and one it passes one, a key between two row groups
    /// or outside the range none, with `page_of` naming a page exactly when
    /// `find` fetches one. At least one probed miss is a filter negative, so
    /// the no-GET branch is known to have run.
    async fn check_bloom_costs(key_type: KeyType) {
        let rows = contract_rows();
        let mut fixture = Fixture::contract(key_type, rows);
        fixture.layout.row_group_rows = rows / MANY_ROW_GROUPS;
        let opened = store(&fixture).await;
        let index = opened
            .build(key_type, Residency::Bloom, None)
            .await
            .unwrap();
        let stats = opened.stats();
        assert_eq!(index.residency(), Residency::Bloom);
        assert_eq!(
            stats.gets(),
            1 + 2 * count(MANY_ROW_GROUPS),
            "footer, chunks, filters"
        );
        let filters = opened.filters(&index);
        let cache = cache().await;
        let group_rows = rows / MANY_ROW_GROUPS;

        let before = stats.gets();
        let hit = group_rows + page_rows() + 1;
        let row = index
            .find(&key_for(hit, key_type), Some(&cache), Issue::default())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(row, row_of(hit, group_rows));
        assert_eq!(stats.gets(), before + 1, "a positive fetches its key page");
        let (value, origin) = index.read_value(row, Some(&cache)).await.unwrap().unwrap();
        assert_eq!(value.as_deref(), Some(value_for(hit).as_slice()));
        assert_eq!(origin, PageOrigin::Fetched);
        assert_eq!(stats.gets(), before + 2, "a cold hit is two GETs");
        let warm = hit + 1;
        let row = index
            .find(&key_for(warm, key_type), Some(&cache), Issue::default())
            .await
            .unwrap()
            .unwrap();
        let (value, origin) = index.read_value(row, Some(&cache)).await.unwrap().unwrap();
        assert_eq!(value.as_deref(), Some(value_for(warm).as_slice()));
        assert_eq!(origin, PageOrigin::Cached);
        assert_eq!(stats.gets(), before + 2, "a warm hit is none");

        let mut negatives = 0;
        for i in [0, page_rows() - 1, group_rows - 1, group_rows, rows - 2] {
            let key = absent_after(&key_for(i, key_type));
            // The key after a row group's last key falls between two row
            // groups and never reaches the filter.
            let between_groups = (i + 1) % group_rows == 0;
            let passes = filters[i / group_rows].check(&*key.as_bytes());
            let fetches_page = !between_groups && passes;
            negatives += usize::from(!between_groups && !passes);
            assert_eq!(index.page_of(&key).is_some(), fetches_page, "{key}");
            let before = stats.gets();
            assert_eq!(
                index
                    .find(&key, Some(&cache), Issue::default())
                    .await
                    .unwrap(),
                None,
                "{key}"
            );
            assert_eq!(
                stats.gets(),
                before + u64::from(fetches_page),
                "{key}: the filter decides"
            );
        }
        assert!(negatives >= 1, "no probed miss was a filter negative");
        let before = stats.gets();
        for key in [below_range(key_type), above_range(key_type)] {
            assert_eq!(index.page_of(&key), None);
            assert_eq!(
                index
                    .find(&key, Some(&cache), Issue::default())
                    .await
                    .unwrap(),
                None
            );
        }
        assert_eq!(
            stats.gets(),
            before,
            "a key outside the range never reaches the filter"
        );
        assert_eq!(stats.local_reads(), 0);
    }

    #[tokio::test]
    async fn test_bloom_tier_costs_per_type() {
        for key_type in KeyType::ALL {
            check_bloom_costs(key_type).await;
        }
    }

    #[tokio::test]
    async fn test_disk_tier_lookup_costs() {
        let rows = contract_rows();
        let dir = tempfile::tempdir().unwrap();
        let opened = store(&Fixture::contract(KeyType::Long, rows)).await;
        let stats = opened.stats();
        let local = testing::local_dir(dir.path(), opened.stats(), None);
        let index = opened
            .build(KeyType::Long, Residency::Disk, Some(&local))
            .await
            .unwrap();
        assert_eq!(index.residency(), Residency::Disk);
        assert_eq!(
            stats.gets(),
            1 + count(ROW_GROUPS),
            "footer, then the chunks"
        );
        assert_eq!(
            stats.local_reads(),
            0,
            "a fresh directory has nothing to read"
        );
        assert_eq!(
            count(index.resident_bytes()),
            predicted_of(&index).disk_memory
        );

        let cache = cache().await;
        let (gets, reads) = (stats.gets(), stats.local_reads());
        let hit = group_rows() + 1;
        let row = index
            .find(&key_for(hit, KeyType::Long), Some(&cache), Issue::default())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(row, row_of(hit, group_rows()));
        assert_eq!(
            (stats.gets(), stats.local_reads()),
            (gets, reads + 1),
            "a find is one local read"
        );
        let (value, origin) = index.read_value(row, Some(&cache)).await.unwrap().unwrap();
        assert_eq!(value.as_deref(), Some(value_for(hit).as_slice()));
        assert_eq!(origin, PageOrigin::Fetched);
        assert_eq!(
            (stats.gets(), stats.local_reads()),
            (gets + 1, reads + 1),
            "a cold hit adds one GET"
        );
        let warm = hit + 1;
        let row = index
            .find(
                &key_for(warm, KeyType::Long),
                Some(&cache),
                Issue::default(),
            )
            .await
            .unwrap()
            .unwrap();
        let (value, origin) = index.read_value(row, Some(&cache)).await.unwrap().unwrap();
        assert_eq!(value.as_deref(), Some(value_for(warm).as_slice()));
        assert_eq!(origin, PageOrigin::Cached);
        assert_eq!(
            (stats.gets(), stats.local_reads()),
            (gets + 1, reads + 2),
            "a warm hit is one local read for the key page and no GET"
        );
        let key = absent_after(&key_for(hit, KeyType::Long));
        assert_eq!(
            index
                .find(&key, Some(&cache), Issue::default())
                .await
                .unwrap(),
            None
        );
        assert_eq!(
            (stats.gets(), stats.local_reads()),
            (gets + 1, reads + 3),
            "a miss inside the range is one local read"
        );
        assert_eq!(
            index
                .find(&below_range(KeyType::Long), Some(&cache), Issue::default())
                .await
                .unwrap(),
            None
        );
        assert_eq!(
            (stats.gets(), stats.local_reads()),
            (gets + 1, reads + 3),
            "a miss outside costs nothing"
        );
    }

    #[tokio::test]
    async fn test_page_of_names_the_pages_find_decodes_and_read_value_fetches() {
        let rows = contract_rows();
        let opened = store(&Fixture::contract(KeyType::String, rows)).await;
        let index = opened.index(KeyType::String).await;
        let offset_index = index.metadata().offset_index().unwrap();
        for i in [0, 1, page_rows() - 1, page_rows(), group_rows(), rows - 1] {
            let key = key_for(i, KeyType::String);
            let page = index.page_of(&key).unwrap();
            let row = index
                .find(&key, None, Issue::default())
                .await
                .unwrap()
                .unwrap();
            assert_eq!(page.row_group, row.row_group);
            let position = RowPosition(approx::count(i));
            assert_eq!(
                index.position(row),
                position,
                "row {i} counts from the file's first row"
            );
            assert_eq!(page.position, Some(position));
            assert!(
                index
                    .key_column()
                    .group(page.row_group)
                    .page_rows(page.key_page)
                    .contains(&row.row)
            );
            let value_pages =
                offset_index[row.row_group][index.value_leaf(0).unwrap()].page_locations();
            let holding = value_pages
                .iter()
                .rev()
                .find(|location| location.first_row_index <= row.row)
                .unwrap();
            assert_eq!(
                index.value_page_offset(0, &page),
                Some(u64::try_from(holding.offset).unwrap())
            );
            let stats = opened.stats();
            let before = stats.bytes();
            index.read_value(row, None).await.unwrap().unwrap();
            assert_eq!(
                stats.bytes() - before,
                u64::try_from(holding.compressed_page_size).unwrap()
            );
        }
        let absent = absent_after(&key_for(page_rows(), KeyType::String));
        let page = index.page_of(&absent).unwrap();
        assert_eq!(
            (
                page.row_group,
                page.key_page,
                index.value_page_offset(0, &page),
                page.position
            ),
            (0, 1, None, None)
        );
    }

    #[tokio::test]
    async fn test_page_of_names_no_value_page_in_the_disk_and_bloom_tiers() {
        let dir = tempfile::tempdir().unwrap();
        let opened = store(&Fixture::contract(KeyType::Long, contract_rows())).await;
        for residency in [Residency::Disk, Residency::Bloom] {
            let index = opened
                .build_in(KeyType::Long, residency, dir.path())
                .await
                .unwrap();
            let key = key_for(group_rows() + page_rows(), KeyType::Long);
            assert_eq!(
                index.page_of(&key),
                Some(PageRef {
                    row_group: 1,
                    key_page: 1,
                    row: None,
                    position: None,
                })
            );
            assert_eq!(index.page_of(&above_range(KeyType::Long)), None);
        }
    }

    #[tokio::test]
    async fn test_key_chunks_for_verification_decodes_the_same_keys_in_every_tier() {
        let rows = contract_rows();
        let dir = tempfile::tempdir().unwrap();
        let opened = store(&Fixture::contract(KeyType::Uuid, rows)).await;
        let stats = opened.stats();
        for residency in Residency::ALL {
            let index = opened
                .build_in(KeyType::Uuid, residency, dir.path())
                .await
                .unwrap();
            let (gets, reads) = (stats.gets(), stats.local_reads());
            let chunks = index.key_chunks_for_verification().await.unwrap();
            let expected = match residency {
                Residency::Full => (gets, reads),
                Residency::Disk => (gets, reads + count(ROW_GROUPS)),
                Residency::Bloom => (gets + count(ROW_GROUPS), reads),
            };
            assert_eq!((stats.gets(), stats.local_reads()), expected, "{residency}");
            let mut decoded = 0;
            for row_group in 0..ROW_GROUPS {
                let keys = index.decode_keys(row_group, None, &chunks).unwrap();
                assert!(matches!(keys, Keys::Uuid(_)));
                assert_eq!(keys.len(), group_rows());
                assert_eq!(keys.first_not_increasing(), None);
                for at in 0..keys.len() {
                    assert_eq!(
                        keys.get(at),
                        key_for(row_group * group_rows() + at, KeyType::Uuid)
                    );
                    decoded += 1;
                }
                let page = index.decode_keys(row_group, Some(1), &chunks).unwrap();
                assert_eq!(page.len(), page_rows());
                assert_eq!(page.get(0), keys.get(page_rows()));
            }
            assert_eq!(decoded, rows);
        }
    }

    impl FileIndex {
        /// This index reading its pages from `handle`'s file over
        /// `metadata`, so a lookup meets page bytes the index was not built
        /// from.
        fn with_file(self, handle: Arc<FileHandle>, metadata: ParquetMetaData) -> Self {
            Self {
                handle,
                ..self.with_metadata(metadata)
            }
        }
    }

    /// `index`, built over `opened`'s file with footer `kept`, reading
    /// from a copy of the file in which the header of `page` of row group
    /// 0's value chunk claims each of `claims`, and over `kept` moved to
    /// match, its row group 0 claiming `group_rows` rows when that is
    /// `Some`. What lies past row group 0's value chunk in the file is not
    /// where that footer places it.
    async fn with_value_header(
        index: FileIndex,
        opened: &Opened,
        kept: &ParquetMetaData,
        page: ChunkPage,
        claims: &[(HeaderField, i32)],
        group_rows: Option<i64>,
    ) -> FileIndex {
        let value_leaf = index.value_leaf(0).unwrap();
        let (chunk_start, _) = kept.row_group(0).column(value_leaf).byte_range();
        let at = approx::length(chunk_start) + header_at(kept, value_leaf, page);
        let patched = with_header_claims(&opened.file, at, claims);
        let grown = i64::try_from(patched.len() - opened.file.len()).unwrap();
        let moved = moved_by_header(kept, value_leaf, page, grown);
        let metadata = match group_rows {
            Some(rows) => with_group_rows(&moved, 0, rows),
            None => moved,
        };
        let reopened = store_file(Bytes::from(patched)).await;
        index.with_file(Arc::clone(&reopened.handle), metadata)
    }

    /// The header of `page` of row group 0's value chunk in `opened`'s file.
    fn value_header(index: &FileIndex, opened: &Opened, page: ChunkPage) -> PageHeader {
        let value_leaf = index.value_leaf(0).unwrap();
        let metadata = index.metadata();
        let (chunk_start, _) = metadata.row_group(0).column(value_leaf).byte_range();
        let at = approx::length(chunk_start) + header_at(metadata, value_leaf, page);
        PageHeader::read(&opened.file[at..]).unwrap()
    }

    /// The fault `FileFault::PageHeaderOverBound` names for row group 0.
    fn over_bound(page: ChunkPage, field: HeaderField, declared: u64, allowed: u64) -> FileFault {
        FileFault::PageHeaderOverBound {
            row_group: 0,
            page,
            field,
            declared,
            allowed,
        }
    }

    /// A value page a lookup fetches whose header declares more than
    /// `MOST_VALUE_PAGE_BYTES` uncompressed, a compressed size past its
    /// place in the offset index, or v2 level lengths past its
    /// uncompressed size is refused as a file fault before parquet reads
    /// it: parquet would reserve the declared size, and add the level
    /// lengths as `i32`s. The same header rewritten to the numbers it held
    /// serves the value.
    #[tokio::test]
    async fn test_read_value_refuses_a_value_page_header_over_its_bounds() {
        let mut fixture = Fixture::contract(KeyType::Long, contract_rows());
        fixture.layout.writer_version = WriterVersion::PARQUET_2_0;
        let opened = store(&fixture).await;
        let page_at = 1;
        let page = ChunkPage::Data(page_at);
        let row = page_rows() * page_at;
        let at = row_of(row, group_rows());
        let index = opened.index(KeyType::Long).await;
        let written = value_header(&index, &opened, page);
        assert!(
            matches!(written.body, PageBody::DataV2 { .. }),
            "{:?}",
            written.body
        );
        let uncompressed = i32::try_from(written.uncompressed_page_size).unwrap();
        let compressed = i32::try_from(written.compressed_page_size).unwrap();
        let over_ceiling = i32::try_from(MOST_VALUE_PAGE_BYTES + 1).unwrap();
        let ceiling = |declared: i32| {
            over_bound(
                page,
                HeaderField::UncompressedPageSize,
                u64::from(declared.unsigned_abs()),
                MOST_VALUE_PAGE_BYTES,
            )
        };
        let cases = [
            (
                vec![
                    (HeaderField::UncompressedPageSize, uncompressed),
                    (HeaderField::CompressedPageSize, compressed),
                ],
                None,
            ),
            (
                vec![(HeaderField::UncompressedPageSize, over_ceiling)],
                Some(ceiling(over_ceiling)),
            ),
            (
                vec![(HeaderField::UncompressedPageSize, i32::MAX)],
                Some(ceiling(i32::MAX)),
            ),
            (
                vec![(HeaderField::CompressedPageSize, compressed + 1)],
                Some(over_bound(
                    page,
                    HeaderField::CompressedPageSize,
                    u64::from((compressed + 1).unsigned_abs()),
                    u64::from(compressed.unsigned_abs()),
                )),
            ),
            (
                vec![
                    (HeaderField::DefinitionLevelsByteLength, i32::MAX),
                    (HeaderField::RepetitionLevelsByteLength, i32::MAX),
                ],
                Some(over_bound(
                    page,
                    HeaderField::LevelsByteLength,
                    u64::from(i32::MAX.unsigned_abs()) + u64::from(i32::MAX.unsigned_abs()),
                    u64::from(uncompressed.unsigned_abs()),
                )),
            ),
        ];
        let kept = (**index.metadata()).clone();
        let mut index = index;
        for (claims, refused) in cases {
            index = with_value_header(index, &opened, &kept, page, &claims, None).await;
            let read = index.read_value(at, None).await;
            match refused {
                None => assert_eq!(
                    read.unwrap().unwrap().0.as_deref(),
                    Some(value_for(row).as_slice()),
                    "{claims:?} as written"
                ),
                Some(fault) => assert_eq!(fault_of(read), fault, "{claims:?}"),
            }
        }
    }

    /// A value column's dictionary page a lookup fetches whose header
    /// counts more entries than its row group has rows, or than
    /// `MOST_VALUE_PAGE_BYTES` holds at the bytes parquet keeps a decoded
    /// byte array in when the footer claims more rows than that, is refused
    /// before parquet sizes the dictionary by the count; one declaring more
    /// than the ceiling uncompressed is refused as well. The count as
    /// written serves the value.
    #[tokio::test]
    async fn test_read_value_refuses_a_value_dictionary_over_its_bounds() {
        let mut fixture = Fixture::contract(KeyType::Long, contract_rows());
        fixture.layout.dictionary = Dictionary::Value;
        let opened = store(&fixture).await;
        let index = opened.index(KeyType::Long).await;
        let page = ChunkPage::Dictionary;
        let PageBody::Dictionary { num_values } = value_header(&index, &opened, page).body else {
            panic!("the value chunk starts with its dictionary page");
        };
        let written = i32::try_from(num_values).unwrap();
        let rows = count(group_rows());
        let row = 0;
        let at = row_of(row, group_rows());
        let rows_claim = i32::try_from(rows).unwrap();
        let held = MOST_VALUE_PAGE_BYTES / count(size_of::<ByteArray>());
        let held_claim = i32::try_from(held + 1).unwrap();
        let over_ceiling = i32::try_from(MOST_VALUE_PAGE_BYTES + 1).unwrap();
        let cases = [
            (HeaderField::NumValues, written, None, None),
            (
                HeaderField::NumValues,
                rows_claim + 1,
                None,
                Some((rows_claim + 1, rows)),
            ),
            (
                HeaderField::UncompressedPageSize,
                over_ceiling,
                None,
                Some((over_ceiling, MOST_VALUE_PAGE_BYTES)),
            ),
            (
                HeaderField::NumValues,
                held_claim,
                Some(INFLATED_GROUP_ROWS),
                Some((held_claim, held)),
            ),
        ];
        let kept = (**index.metadata()).clone();
        let mut index = index;
        for (field, claimed, group_rows, refused) in cases {
            let claims = [(field, claimed)];
            index = with_value_header(index, &opened, &kept, page, &claims, group_rows).await;
            let read = index.read_value(at, None).await;
            match refused {
                None => assert_eq!(
                    read.unwrap().unwrap().0.as_deref(),
                    Some(value_for(row).as_slice()),
                    "{field} {claimed} as written"
                ),
                Some((declared, allowed)) => assert_eq!(
                    fault_of(read),
                    over_bound(page, field, u64::from(declared.unsigned_abs()), allowed),
                    "{field} {claimed}"
                ),
            }
        }
    }

    /// A value dictionary whose header counts one entry more than its body
    /// holds passes every header bound, the count below the row group's
    /// rows. Parquet panics decoding the entry that is not there; the
    /// lookup ends in `FileFault::DecoderPanicked` naming the dictionary
    /// page, and a lookup into another row group still serves.
    #[tokio::test]
    async fn test_read_value_a_value_dictionary_short_of_its_count_is_a_file_fault() {
        let mut fixture = Fixture::contract(KeyType::Long, contract_rows());
        fixture.layout.dictionary = Dictionary::Value;
        let value_leaf = 1;
        let (short, written) = with_dictionary_one_entry_short(&fixture.write(), value_leaf);
        assert!(u64::from(written) < count(group_rows()), "{written}");
        let opened = store_file(short).await;
        let index = opened.index(KeyType::Long).await;
        let fault = fault_of(index.read_value(row_of(0, group_rows()), None).await);
        assert!(
            matches!(
                fault,
                FileFault::DecoderPanicked {
                    part: DecodedPart::Pages {
                        row_group: 0,
                        page: Some(ChunkPage::Dictionary),
                    },
                    ..
                }
            ),
            "{fault:?}"
        );
        let elsewhere = group_rows();
        let (value, _) = index
            .read_value(row_of(elsewhere, group_rows()), None)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(value.as_deref(), Some(value_for(elsewhere).as_slice()));
    }

    #[tokio::test]
    async fn test_read_value_row_outside_every_page_is_a_file_fault() {
        let opened = store(&Fixture::contract(KeyType::Long, FEW_ROWS)).await;
        let index = opened.index(KeyType::Long).await;
        let rows = i64::try_from(FEW_ROWS).unwrap();
        for row in [
            RowRef {
                row_group: 0,
                row: rows,
            },
            RowRef {
                row_group: 0,
                row: -1,
            },
            RowRef {
                row_group: 1,
                row: 0,
            },
        ] {
            let result = index.read_value(row, None).await;
            let Err(Error::File { path, fault }) = result else {
                panic!("expected a file fault, got {result:?}");
            };
            assert_eq!(path, location());
            assert_eq!(
                fault,
                FileFault::PageOutOfRange {
                    row: row.row,
                    row_group: row.row_group,
                }
            );
        }
        assert_eq!(opened.stats().gets(), 2, "nothing was fetched for them");
    }

    #[test]
    fn test_keys_search_compare_and_order_per_arm() {
        let text = |value: &str| ByteArray::from(value.as_bytes().to_vec());
        let runs = [
            Keys::Long(vec![1, 3, 5]),
            Keys::Str(vec![text("a"), text("b"), text("é")]),
            Keys::Bin(vec![
                ByteArray::from(vec![1]),
                ByteArray::from(vec![1, 1]),
                ByteArray::from(vec![2]),
            ]),
            Keys::Uuid(vec![[0; UUID_BYTES], [1; UUID_BYTES], [2; UUID_BYTES]]),
        ];
        for run in &runs {
            assert_eq!(run.len(), 3);
            assert!(!run.is_empty());
            assert_eq!(run.first_not_increasing(), None);
            for at in 0..run.len() {
                assert_eq!(run.binary_search(&run.get(at)), Ok(at));
                assert_eq!(run.cmp_at(at, run, at), std::cmp::Ordering::Equal);
            }
            assert_eq!(run.cmp_at(0, run, 2), std::cmp::Ordering::Less);
            assert_eq!(run.binary_search(&absent_after(&run.get(0))), Err(1));
            for other in &runs {
                if std::mem::discriminant(other) != std::mem::discriminant(run) {
                    assert_eq!(run.binary_search(&other.get(1)), Err(0));
                }
            }
        }
        assert_eq!(runs[0].max_len(), size_of::<i64>());
        assert_eq!(runs[1].max_len(), "é".len());
        assert_eq!(runs[2].max_len(), 2);
        assert_eq!(runs[3].max_len(), UUID_BYTES);
        assert_eq!(Keys::Long(Vec::new()).max_len(), 0);
        assert!(Keys::Long(vec![1, 1]).first_not_increasing().is_some());
        assert!(
            Keys::Str(vec![text("b"), text("a")])
                .first_not_increasing()
                .is_some()
        );
        assert_eq!(
            Keys::Long(vec![1, 2, 2, 3]).first_not_increasing(),
            Some((2, std::cmp::Ordering::Equal))
        );
        assert_eq!(
            Keys::Str(vec![text("a"), text("c"), text("b")]).first_not_increasing(),
            Some((2, std::cmp::Ordering::Less))
        );
        assert_eq!(
            Keys::Uuid(vec![[1; UUID_BYTES], [1; UUID_BYTES]]).first_not_increasing(),
            Some((1, std::cmp::Ordering::Equal))
        );
        assert_eq!(runs[1].get(2), Key::Str("é".to_string()));
    }

    #[tokio::test]
    async fn test_file_index_and_its_futures_are_send_and_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        fn assert_send<T: Send>(_: &T) {}
        assert_send_sync::<FileIndex>();
        assert_send_sync::<Keys>();
        let opened = store(&Fixture::contract(KeyType::Long, FEW_ROWS)).await;
        let index = opened.index(KeyType::Long).await;
        let cache = cache().await;
        let key = key_for(1, KeyType::Long);
        let find = index.find(&key, Some(&cache), Issue::default());
        assert_send(&find);
        let row = find.await.unwrap().unwrap();
        let read = index.read_value(row, Some(&cache));
        assert_send(&read);
        let (value, _) = read.await.unwrap().unwrap();
        assert_eq!(value.as_deref(), Some(value_for(1).as_slice()));
        let chunks = index.key_chunks_for_verification();
        assert_send(&chunks);
        chunks.await.unwrap();
    }

    /// The parts `resident_parts` reports sum to `resident_bytes` in every
    /// tier, an ordered file's order facts and order fences among them and
    /// a key-value file's zero; each tier holds only its own key structure,
    /// the chunks under Full and the filters under Bloom.
    #[tokio::test]
    async fn test_resident_parts_sum_to_resident_bytes_in_every_tier() {
        const ROWS_PER_KEY: usize = 10;
        let rows = contract_rows();
        let long = |row: usize| i64::try_from(row).unwrap();
        let keys: Vec<i64> = (0..rows).map(|row| long(row / ROWS_PER_KEY)).collect();
        let orders: Vec<i64> = (0..rows).map(|row| long(row % ROWS_PER_KEY)).collect();
        let ordered = store(&Fixture::ordered(&keys, long_orders(&orders))).await;
        let keyed = store(&Fixture::contract(KeyType::Long, rows)).await;
        for residency in Residency::ALL {
            let dir = tempfile::tempdir().unwrap();
            let index = ordered
                .build_ordered_in(OrderType::Long, residency, dir.path())
                .await
                .unwrap();
            let parts = index.resident_parts();
            assert_eq!(parts.total(), count(index.resident_bytes()), "{residency}");
            for (part, bytes) in [
                ("footer", parts.footer),
                ("key column index", parts.key_column_index),
                ("offset indexes", parts.offset_indexes),
                ("key fences", parts.key_fences),
                ("order facts", parts.order_facts),
                ("order fences", parts.order_fences),
            ] {
                assert!(bytes > 0, "{residency}: {part}");
            }
            assert_eq!(parts.key_chunks > 0, residency == Residency::Full);
            assert_eq!(parts.filters > 0, residency == Residency::Bloom);

            let dir = tempfile::tempdir().unwrap();
            let index = keyed
                .build_in(KeyType::Long, residency, dir.path())
                .await
                .unwrap();
            let parts = index.resident_parts();
            assert_eq!(parts.total(), count(index.resident_bytes()), "{residency}");
            assert!(parts.key_column_index > 0 && parts.key_fences > 0);
            assert_eq!((parts.order_facts, parts.order_fences), (0, 0));
        }
    }

    /// The footer keeps no order column index (RFD 025, Load): an ordered
    /// file whose writer wrote one holds exactly what the same file without
    /// it holds, part by part, and the parts still sum to `resident_bytes`,
    /// the order column's offset index among them.
    #[tokio::test]
    async fn test_resident_parts_of_a_file_without_an_order_column_index() {
        const ROWS_PER_KEY: usize = 10;
        let rows = contract_rows();
        let long = |row: usize| i64::try_from(row).unwrap();
        let keys: Vec<i64> = (0..rows).map(|row| long(row / ROWS_PER_KEY)).collect();
        let orders: Vec<i64> = (0..rows).map(|row| long(row % ROWS_PER_KEY)).collect();
        let mut fixture = Fixture::ordered(&keys, long_orders(&orders));
        let indexed = store(&fixture).await;
        fixture.layout.order_statistics = Some(EnabledStatistics::Chunk);
        let unindexed = store(&fixture).await;
        for residency in Residency::ALL {
            let dir = tempfile::tempdir().unwrap();
            let with = indexed
                .build_ordered_in(OrderType::Long, residency, dir.path())
                .await
                .unwrap()
                .resident_parts();
            let dir = tempfile::tempdir().unwrap();
            let index = unindexed
                .build_ordered_in(OrderType::Long, residency, dir.path())
                .await
                .unwrap();
            let without = index.resident_parts();
            assert_eq!(
                without.total(),
                count(index.resident_bytes()),
                "{residency}"
            );
            assert_eq!(without, with, "{residency}");
            assert!(without.key_column_index > 0 && without.offset_indexes > 0);
            assert!(without.order_facts > 0 && without.order_fences > 0);
        }
    }
}
