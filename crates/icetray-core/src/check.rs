//! The doctor (RFD 007, RFD 016, RFD 020, RFD 021, RFD 022): the finding
//! codes, the levels they cap, penalties, and thresholds by which a table
//! is classified, applied identically at every load and in the `doctor`
//! subcommand. A value-column finding is made per column and names it, and
//! each column has a level of its own; the table's level is the dearest
//! column's. `diagnose` is a pure function of a loaded `Generation` and
//! the table's declared level: it reads the footers, page indexes, fences,
//! manifest entries, and applied delete state the generation holds and
//! nothing else, so the same generation always gets the same report.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;

use parquet::basic::Encoding;
use parquet::file::metadata::{ColumnChunkMetaData, ParquetMetaData, ParquetOffsetIndex};
use parquet::file::page_index::column_index::ColumnIndexMetaData;

use crate::approx;
use crate::config::{Residency, ValueFormat};
use crate::contract;
use crate::deletes::DeleteSummary;
use crate::error::{Error, RefusalReason, Result};
use crate::generation::{Generation, LoadedFile, ProbeWidths};
use crate::index::{
    FileIndex, Inspected, ResidentBytes, bits_per_key, saturating_sum, saturating_sum_all,
};
use crate::key::{Key, KeyType};
use crate::level::Level;
use crate::manifest::{DataFileEntry, TableLayout, ValueColumnLayout};
use crate::order::{OrderFacts, OrderType, OrderValue};
use crate::projection;
use crate::snapshot::SnapshotId;

/// The thresholds, in one place (RFD 016): each derives from `contract`
/// where a contract number exists, and each names the analysis section that
/// set it.
pub mod thresholds {
    use crate::contract;

    /// Key page rows above which `large_key_pages` caps the level: twice
    /// `contract::PAGE_ROWS`. A lookup decodes one key page, so its rows
    /// set the CPU a lookup spends (prototype findings, finding 1: the
    /// writer's default 17,900-row pages served at the same GET count and
    /// cost CPU per lookup instead). The slack up to twice the contract is a
    /// chosen margin, so a writer whose page rounding overshoots the
    /// contract draws no finding (RFD 016, consequences).
    pub const KEY_PAGE_ROWS_LARGE: i64 = 2 * contract::PAGE_ROWS;

    /// Mean uncompressed value page bytes above which `large_value_pages`
    /// is advice: 1 MiB, `VALUE_PAGE_ADVICE_FACTOR` times
    /// `contract::VALUE_PAGE_BYTES`. The one value page a cold hit fetches
    /// is the bytes it transfers and decompresses, and that cost is linear
    /// in the page's bytes with no step anywhere (icetray against real S3,
    /// results: 192 KB pages served at the same cold latency as 64
    /// KiB ones), so the line is drawn where the transfer itself reaches
    /// the order of a round trip: at object-store bandwidth a megabyte
    /// takes tens of milliseconds, which shows in a cold hit, while a few
    /// hundred kilobytes do not. A row limit alone leaves pages of many
    /// megabytes for large values (prototype findings, finding 6); the
    /// contract's byte bound of RFD 002 is the recipe for those, and
    /// `FileReport::avg_value_bytes` beside `avg_value_page_bytes` shows an
    /// owner how many values a hit fetches for the one it wanted.
    pub(crate) const VALUE_PAGE_BYTES_ADVICE: u64 =
        VALUE_PAGE_ADVICE_FACTOR * contract::VALUE_PAGE_BYTES;

    /// How many contract value pages the advice threshold spans: sixteen,
    /// which makes it 1 MiB.
    pub(crate) const VALUE_PAGE_ADVICE_FACTOR: u64 = 16;

    /// Mean uncompressed value page bytes above which
    /// `value_pages_over_ceiling` is advice: the lookup's own ceiling on a
    /// value page's declared size, 64 MiB, held where the lookup enforces
    /// it and not redeclared here, so the finding and the refusal it warns
    /// of cannot drift apart.
    pub use crate::index::MOST_VALUE_PAGE_BYTES;

    /// Data file bytes above which `large_file` is advice: twice
    /// `contract::TARGET_FILE_BYTES_MAX`. First-byte latency of a range GET
    /// is three to four times higher on objects of 256 MiB and above than
    /// on 64 MiB and below, and the slow mode begins somewhere in the
    /// unmeasured band between (range-GET latency, what the evidence
    /// implies); 128 MiB is the judgment inside that band RFD 016 records.
    pub const FILE_BYTES_ADVICE: u64 = 2 * contract::TARGET_FILE_BYTES_MAX;

    /// `many_files_per_bucket` is advice when a bucket holds more than this
    /// many times the files its bytes explain at
    /// `contract::TARGET_FILE_BYTES_MAX` per file. A sorted write task that
    /// rolls files at the target is not many files; a batch writer that
    /// splits a bucket across tasks is (prototype findings, finding 9).
    pub const FILES_PER_BUCKET_FACTOR: u64 = 2;

    /// Bloom filter bytes per key above which `bloom_oversized` is advice.
    /// A filter sized to its row group at one percent false positives is
    /// about 1.2 bytes per key; Iceberg 1.10.1 wrote a fixed 1 MiB per row
    /// group, 17 to 20 bytes per key at 62,500 rows and 856 on a small
    /// append file, and the bloom tier's memory follows it (bloom residency
    /// tier, observations; prototype findings, finding 7).
    pub const BLOOM_BYTES_PER_KEY_ADVICE: f64 = 4.0;

    /// Bloom filter bits per key below which `bloom_undersized` is advice.
    /// Four bits per key gave 29 false positives per 100 misses, each a GET
    /// the bloom tier's miss otherwise avoids; at 17 bits none appeared in
    /// 100 (bloom residency tier, observations).
    pub const BLOOM_BITS_PER_KEY_MIN: f64 = 8.0;

    /// The share of the key pages' distinct first keys that keep a distinct
    /// column-index minimum, below which `column_index_truncated` is
    /// advice (`distinct_mins_ratio`). parquet-mr cuts page
    /// minimums to 64 bytes by default, so on a table of 91-byte keys every
    /// page of a file reported one minimum; icetray routes by resident keys
    /// and served it at one GET (prototype findings, method and finding 1),
    /// but other readers of the same files cannot skip pages by such an
    /// index (RFD 002).
    pub const DISTINCT_MINS_RATIO_ADVICE: f64 = 0.5;

    /// Rows of one key's run in one file above which `long_runs` is advice:
    /// `DEFAULT_MAX_SCAN_ROWS`, the most rows one Scan returns at the
    /// default, so a longer run takes a caller more than one Scan at the
    /// default to read whole (RFD 025, Levels and findings). Fixed, as
    /// every threshold is (RFD 016): a table's own `max_scan_rows` does not
    /// move it. `large_runs` has no threshold of its own: it is judged
    /// against the table's `max_scan_rows` and `server.max_scan_bytes`
    /// (`ScanLimits`).
    pub const RUN_ROWS_ADVICE: u32 = crate::config::DEFAULT_MAX_SCAN_ROWS;
}

/// What a Scan of an ordered table is held to (RFD 025, Limits): its rows
/// and the encoded size of its response, which `large_runs` sizes a file's
/// runs against, and the values one `MultiScan` of it may request.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ScanLimits {
    /// The table's effective `max_scan_rows` (`TableConfig::scan_rows`).
    pub max_scan_rows: u32,
    /// `server.max_scan_bytes`.
    pub max_scan_bytes: usize,
    /// `server.max_multi_scan_values`.
    pub max_multi_scan_values: usize,
}

/// The GETs a cold lookup spends on a column chunk's dictionary page beside
/// the page it decodes: for the value column on a hit, the penalty of
/// `dictionary_encoded_value` (RFD 007); for the key column under `bloom`,
/// on each file whose filter answers yes (RFD 017).
const DICTIONARY_PAGE_GETS: u64 = 1;

/// The most data files the doctor's walk of a protobuf column reads a value
/// page of (RFD 024): the first files in generation order that hold the
/// column, until one's first value page holds a value that is not null.
/// Three bounds the walk of a sparse column to three GETs (six for
/// dictionary-encoded chunks) while letting it pass a file or two whose
/// first page is all nulls; a column null on the first page of every file
/// walked is not checked.
const MOST_WALKED_FILES: usize = 3;

/// The GETs a cold hit spends on the value page of each column it names
/// (RFD 006, RFD 022).
const VALUE_PAGE_GETS: u64 = 1;

/// The GETs a cold lookup under `bloom` spends on the key page of each file
/// it probes whose filter answers yes (RFD 017); `full` holds the page and
/// `disk` reads it from a chunk file, neither with a GET.
const KEY_PAGE_GETS: u64 = 1;

/// The key pages a Scan decodes per row group its run spans at most: the
/// run's first and last, since every page between them holds only the key
/// (RFD 025, Cost).
const RUN_EDGE_KEY_PAGES: u64 = 2;

/// The GETs a cold Scan under `full` or `bloom` spends on the order column
/// of a run in one row group: one span, the run's order pages being
/// contiguous (RFD 025, Cost).
const ORDER_SPAN_GETS: u64 = 1;

/// The files a lookup or Scan probes where no files overlap: the one its
/// key routes to.
pub(crate) const FILES_PROBED_ALONE: usize = 1;

/// The files a bucket's bytes explain at the least: one, however few bytes
/// it holds, so a bucket of one small file is never many files.
const MIN_EXPLAINED_FILES: u64 = 1;

/// The distinct page minimums ratio of a key column whose column index
/// cannot show truncation: no index, or an index that lists no page. Every
/// page counts as distinct, and the file draws no
/// `column_index_truncated`.
const DISTINCT_MINS_UNOBSERVABLE: f64 = 1.0;

/// A capping finding's penalty per lookup over the contract layout, in RFD
/// 007's three units; a unit the finding does not affect is `None`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, serde::Serialize)]
pub struct Penalty {
    /// GETs a hit makes beyond the one value page.
    pub extra_gets_per_hit: Option<u64>,
    /// Keys a lookup decodes, where the contract decodes `contract::PAGE_ROWS`.
    pub keys_decoded_per_lookup: Option<i64>,
    /// Files a lookup may probe, where the contract probes one.
    pub files_probed_per_lookup: Option<usize>,
}

impl Penalty {
    /// A penalty in files probed per lookup alone, the unit every
    /// table-level cap is measured in.
    pub(crate) fn files_probed(files: usize) -> Self {
        Self {
            files_probed_per_lookup: Some(files),
            ..Self::default()
        }
    }
}

/// What one cold lookup pays at worst on a layout in a residency (RFD 020):
/// the numbers behind its level, which rise before the level changes.
/// Observed traffic is `icetray_lookups_total`'s, where a warm hit pays
/// less than `gets_per_hit`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize)]
pub struct LookupCost {
    /// GETs a cold hit makes finding its row and reading the dearest value
    /// column (RFD 022): under `bloom` the key page of every file probed,
    /// each filter answering yes, with the key column's dictionary page
    /// when any file's is dictionary-encoded, and that column's value page
    /// and, when any file's chunk of it is dictionary-encoded, its
    /// dictionary page. A table of no value columns pays finding the row
    /// alone; a hit naming several columns pays finding the row once, which
    /// is `gets_per_miss`, and each column's `ColumnCost::gets_per_hit`.
    pub gets_per_hit: u64,
    /// GETs a miss makes: none under `full` and `disk`; under `bloom` the
    /// key pages a hit spends, every filter answering yes.
    pub gets_per_miss: u64,
    /// Files a lookup probes at most: the most in any bucket
    /// (`Generation::overlapping_buckets`, which takes unbucketed files as
    /// one bucket), or on an ordered table the most files a shared edge key
    /// spans where that is more, one where no files overlap; an upper bound
    /// where files were written under two or more bucketings.
    pub files_probed: usize,
}

/// One value column's level and cost on a layout in a residency (RFD 020,
/// RFD 022), what `icetray_table_column_level` and
/// `icetray_table_column_gets_per_hit` show.
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize)]
pub struct ColumnCost {
    /// The level a lookup naming the column achieves: the column's own caps
    /// together with those of finding the row and the residency's best
    /// level.
    pub level: Level,
    /// The GETs a cold hit spends on the column alone: its value page, and
    /// its dictionary page when any file's chunk of it is
    /// dictionary-encoded. A hit in a file written before the column was
    /// added spends none, which this worst case does not count.
    pub gets_per_hit: u64,
}

/// One value column's level for a snapshot's layout as a `below_level`
/// refusal carries it, from the measurement that gave the table's level
/// (RFD 020, RFD 022).
#[derive(Clone, Debug, PartialEq, Eq, Hash, serde::Serialize)]
pub struct ColumnLevel {
    /// The column's configured name.
    pub column: String,
    /// The level a lookup naming the column achieves.
    pub level: Level,
}

/// Each of `layout`'s value columns with its level in `costs`, which is in
/// the layout's order.
pub(crate) fn column_levels(layout: &TableLayout, costs: &[ColumnCost]) -> Vec<ColumnLevel> {
    layout
        .value_columns
        .iter()
        .zip(costs)
        .map(|(column, cost)| ColumnLevel {
            column: column.name.clone(),
            level: cost.level,
        })
        .collect()
}

/// A capping finding as a `below_level` refusal names it: its code and the
/// value column it is about, `None` for a finding about the row or the
/// table (RFD 022). Display gives the code, then the column in
/// parentheses when there is one.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Serialize)]
pub struct Capping {
    /// The finding's code.
    pub code: FindingCode,
    /// The value column the finding is about.
    pub column: Option<String>,
}

impl fmt::Display for Capping {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.column {
            Some(column) => write!(f, "{} (column {column})", self.code),
            None => self.code.fmt(f),
        }
    }
}

/// A finding's code (RFD 016). Display and serde give the `snake_case` code
/// that appears in the doctor's output and the `code` metric label; code
/// never compares the strings. The derived order is the finding table's
/// order, which the report sorts by after putting capping findings first.
// The serde names and Display are the same strings;
// `test_serialize_finding_code_matches_display` holds them together.
#[derive(
    Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Serialize, serde::Deserialize,
)]
#[serde(rename_all = "snake_case")]
pub enum FindingCode {
    /// A value column is dictionary-encoded; its dictionary page costs one
    /// extra GET per hit that names it.
    DictionaryEncodedValue,
    /// Key pages hold more rows than `thresholds::KEY_PAGE_ROWS_LARGE`;
    /// a lookup decodes them all.
    LargeKeyPages,
    /// No bucket transform on the key and more than one file; a lookup
    /// probes every file.
    NoBucketPartition,
    /// Files within one bucket have intersecting exact key ranges; a lookup
    /// probes each of them whose range covers its key. On an ordered table,
    /// files one snapshot added that meet only at an edge key are
    /// `SharedEdgeKeys` instead (RFD 025).
    OverlappingFiles,
    /// On an ordered table, files one snapshot added, as a compaction that
    /// rolls files by size writes them, meet where one file's last key is
    /// the next one's first; a Scan of such an edge key reads every file
    /// that holds it (RFD 025).
    SharedEdgeKeys,
    /// On an ordered table, the order column is dictionary-encoded; every
    /// Scan fetches its dictionary page besides the order pages.
    DictionaryEncodedOrder,
    /// A manifest lacks key bounds, or its bounds are shorter than the
    /// file's exact first or last key.
    BoundsMissing,
    /// The key column's column index is truncated: too few page minimums
    /// are distinct.
    ColumnIndexTruncated,
    /// A value column's mean uncompressed value page is above the advice
    /// threshold.
    LargeValuePages,
    /// A value column's mean uncompressed value page is above
    /// `thresholds::MOST_VALUE_PAGE_BYTES`, the most a value page a lookup
    /// reads may declare; a lookup into a page over it is refused with a
    /// file fault.
    ValuePagesOverCeiling,
    /// On an ordered table, a file holds a run of more than
    /// `thresholds::RUN_ROWS_ADVICE` rows of one key.
    LongRuns,
    /// On an ordered table, `max_scan_rows` rows of a file's configured
    /// value columns, at the footer's uncompressed bytes per row, exceed
    /// `server.max_scan_bytes` (`ScanLimits`), so a Scan at the full limit
    /// is refused.
    LargeRuns,
    /// A key is longer than `contract::KEY_BYTES_ADVICE`.
    LongKeys,
    /// A data file is larger than the advice threshold.
    LargeFile,
    /// A bucket holds more files than its bytes and the file size target
    /// explain.
    ManyFilesPerBucket,
    /// The key column has no bloom filter.
    NoBloomFilter,
    /// The key bloom filter spends more bytes per key than the advice
    /// threshold.
    BloomOversized,
    /// The key bloom filter has fewer bits per key than the minimum; the
    /// Bloom tier's miss cost rises.
    BloomUndersized,
    /// Deletes remove rows of the snapshot's data files (RFD 021); a
    /// compaction would remove them and their delete files, and the state
    /// they hold.
    DeletesPending,
    /// Data files written before a value column was added answer its
    /// default (RFD 023); a rewrite of them writes the column in.
    ColumnDefault,
    /// A value of a column declared protobuf, or the default it states, is
    /// not protobuf as a projection walks it (RFD 024); a lookup naming
    /// fields of it fails. Raised by the `doctor` command's walk of each
    /// protobuf column (`examine`), never at a load, which reads no value
    /// page.
    MalformedProtobuf,
}

impl FindingCode {
    /// Every code, in finding-table order, for a caller that keeps a series
    /// per code: the `findings` gauge is set for each of these a load raises
    /// (`raised_at_load`) on every admitted generation, so a code the
    /// generation does not raise reads zero. Complete: a successor match
    /// over every variant, which a new variant fails to compile, is
    /// compared to this list in tests.
    pub(crate) const ALL: [Self; 21] = [
        Self::DictionaryEncodedValue,
        Self::LargeKeyPages,
        Self::NoBucketPartition,
        Self::OverlappingFiles,
        Self::SharedEdgeKeys,
        Self::DictionaryEncodedOrder,
        Self::BoundsMissing,
        Self::ColumnIndexTruncated,
        Self::LargeValuePages,
        Self::ValuePagesOverCeiling,
        Self::LongRuns,
        Self::LargeRuns,
        Self::LongKeys,
        Self::LargeFile,
        Self::ManyFilesPerBucket,
        Self::NoBloomFilter,
        Self::BloomOversized,
        Self::BloomUndersized,
        Self::DeletesPending,
        Self::ColumnDefault,
        Self::MalformedProtobuf,
    ];

    /// The level a finding of this code caps a table at (RFD 020), `None`
    /// for advice, which caps nothing. A cost within one file caps it at
    /// `balanced`; more than one file probed, at `permissive`, except for
    /// the files a shared edge key spans, a cost fixed when one snapshot
    /// wrote them that no later commit grows, which caps it at `balanced`
    /// (RFD 025).
    pub fn cap(self) -> Option<Level> {
        match self {
            Self::DictionaryEncodedValue
            | Self::LargeKeyPages
            | Self::SharedEdgeKeys
            | Self::DictionaryEncodedOrder => Some(Level::Balanced),
            Self::NoBucketPartition | Self::OverlappingFiles => Some(Level::Permissive),
            Self::BoundsMissing
            | Self::ColumnIndexTruncated
            | Self::LargeValuePages
            | Self::ValuePagesOverCeiling
            | Self::LongRuns
            | Self::LargeRuns
            | Self::LongKeys
            | Self::LargeFile
            | Self::ManyFilesPerBucket
            | Self::NoBloomFilter
            | Self::BloomOversized
            | Self::BloomUndersized
            | Self::DeletesPending
            | Self::ColumnDefault
            | Self::MalformedProtobuf => None,
        }
    }

    /// Whether a load can raise a finding of this code: every code but
    /// `MalformedProtobuf`, which only the `doctor` command's walk raises.
    /// The `findings` gauge describes what a load found, so it carries the
    /// codes a load raises alone.
    pub fn raised_at_load(self) -> bool {
        match self {
            Self::MalformedProtobuf => false,
            Self::DictionaryEncodedValue
            | Self::LargeKeyPages
            | Self::NoBucketPartition
            | Self::OverlappingFiles
            | Self::SharedEdgeKeys
            | Self::DictionaryEncodedOrder
            | Self::BoundsMissing
            | Self::ColumnIndexTruncated
            | Self::LargeValuePages
            | Self::ValuePagesOverCeiling
            | Self::LongRuns
            | Self::LargeRuns
            | Self::LongKeys
            | Self::LargeFile
            | Self::ManyFilesPerBucket
            | Self::NoBloomFilter
            | Self::BloomOversized
            | Self::BloomUndersized
            | Self::DeletesPending
            | Self::ColumnDefault => true,
        }
    }

    /// Whether a finding of this code is about one value column, which it
    /// names (`Finding::column`), rather than about the row or the table
    /// (RFD 022): the codes `icetray_findings` carries a `column` label for.
    /// The order column's findings are about the table, since every Scan
    /// reads the order column, and so are the runs', which count a run's
    /// rows and sum every value column's bytes.
    pub fn per_column(self) -> bool {
        match self {
            Self::DictionaryEncodedValue
            | Self::LargeValuePages
            | Self::ValuePagesOverCeiling
            | Self::ColumnDefault
            | Self::MalformedProtobuf => true,
            Self::LargeKeyPages
            | Self::NoBucketPartition
            | Self::OverlappingFiles
            | Self::SharedEdgeKeys
            | Self::DictionaryEncodedOrder
            | Self::BoundsMissing
            | Self::ColumnIndexTruncated
            | Self::LongRuns
            | Self::LargeRuns
            | Self::LongKeys
            | Self::LargeFile
            | Self::ManyFilesPerBucket
            | Self::NoBloomFilter
            | Self::BloomOversized
            | Self::BloomUndersized
            | Self::DeletesPending => false,
        }
    }
}

impl fmt::Display for FindingCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let code = match self {
            Self::DictionaryEncodedValue => "dictionary_encoded_value",
            Self::LargeKeyPages => "large_key_pages",
            Self::NoBucketPartition => "no_bucket_partition",
            Self::OverlappingFiles => "overlapping_files",
            Self::SharedEdgeKeys => "shared_edge_keys",
            Self::DictionaryEncodedOrder => "dictionary_encoded_order",
            Self::BoundsMissing => "bounds_missing",
            Self::ColumnIndexTruncated => "column_index_truncated",
            Self::LargeValuePages => "large_value_pages",
            Self::ValuePagesOverCeiling => "value_pages_over_ceiling",
            Self::LongRuns => "long_runs",
            Self::LargeRuns => "large_runs",
            Self::LongKeys => "long_keys",
            Self::LargeFile => "large_file",
            Self::ManyFilesPerBucket => "many_files_per_bucket",
            Self::NoBloomFilter => "no_bloom_filter",
            Self::BloomOversized => "bloom_oversized",
            Self::BloomUndersized => "bloom_undersized",
            Self::DeletesPending => "deletes_pending",
            Self::ColumnDefault => "column_default",
            Self::MalformedProtobuf => "malformed_protobuf",
        };
        f.write_str(code)
    }
}

/// The writer properties the doctor names (RFD 016). Display and serde give
/// the Iceberg or Parquet property string a table owner sets, as the
/// prototype's writer recipe and the Iceberg documentation spell it; code
/// never compares the strings. A property set per column (`MetricsColumn`,
/// `BloomFilterEnabled`, `BloomFilterNdvColumn`) renders its prefix, up to
/// and including the dot before the column name, and the finding's message
/// appends the key column. `RowGroupCheckRecordCount` stands for the pair of
/// properties that set the writer's page-size check interval and renders
/// both; it appears in a message and never as a finding's `property`.
// The serde names and Display are the same strings;
// `test_serialize_writer_property_matches_display` holds them together.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, serde::Serialize)]
pub enum WriterProperty {
    /// parquet-mr's dictionary switch, passed through by Iceberg.
    #[serde(rename = "parquet.enable.dictionary")]
    EnableDictionary,
    /// parquet-mr's column index truncation length, passed through by Iceberg.
    #[serde(rename = "parquet.columnindex.truncate.length")]
    ColumnIndexTruncateLength,
    /// Iceberg's rows per page.
    #[serde(rename = "write.parquet.page-row-limit")]
    PageRowLimit,
    /// Iceberg's byte bound on a page.
    #[serde(rename = "write.parquet.page-size-bytes")]
    PageSizeBytes,
    /// Iceberg's minimum and maximum record count between page-size checks,
    /// named for row groups and setting the page-size check interval
    /// (prototype findings, finding 6).
    #[serde(
        rename = "write.parquet.row-group-check-min-record-count and write.parquet.row-group-check-max-record-count"
    )]
    RowGroupCheckRecordCount,
    /// Iceberg's data file size target.
    #[serde(rename = "write.target-file-size-bytes")]
    TargetFileSizeBytes,
    /// Iceberg's manifest metrics mode for one column; `full` keeps whole
    /// bounds (prototype findings, finding 8).
    #[serde(rename = "write.metadata.metrics.column.")]
    MetricsColumn,
    /// Iceberg's per-column bloom filter switch.
    #[serde(rename = "write.parquet.bloom-filter-enabled.column.")]
    BloomFilterEnabled,
    /// Iceberg's cap on a bloom filter's bytes. Iceberg 1.11 honors it (the
    /// golden table `bloom_max_bytes_100k`); 1.10.1 did not (prototype
    /// findings, finding 7).
    #[serde(rename = "write.parquet.bloom-filter-max-bytes")]
    BloomFilterMaxBytes,
    /// Iceberg's per-column distinct value count a bloom filter is sized
    /// for. Iceberg 1.11 honors it (the golden table `bloom_ndv_100k`);
    /// 1.10.1 did not (prototype findings, finding 7).
    #[serde(rename = "write.parquet.bloom-filter-ndv.column.")]
    BloomFilterNdvColumn,
    /// Spark's adaptive skew splitting, which spreads one bucket's write
    /// over several sorted tasks (prototype findings, finding 9).
    #[serde(rename = "spark.sql.adaptive.optimizeSkewsInRebalancePartitions.enabled")]
    SkewSplitting,
}

impl fmt::Display for WriterProperty {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let property = match self {
            Self::EnableDictionary => "parquet.enable.dictionary",
            Self::ColumnIndexTruncateLength => "parquet.columnindex.truncate.length",
            Self::PageRowLimit => "write.parquet.page-row-limit",
            Self::PageSizeBytes => "write.parquet.page-size-bytes",
            Self::RowGroupCheckRecordCount => {
                "write.parquet.row-group-check-min-record-count and write.parquet.row-group-check-max-record-count"
            }
            Self::TargetFileSizeBytes => "write.target-file-size-bytes",
            Self::MetricsColumn => "write.metadata.metrics.column.",
            Self::BloomFilterEnabled => "write.parquet.bloom-filter-enabled.column.",
            Self::BloomFilterMaxBytes => "write.parquet.bloom-filter-max-bytes",
            Self::BloomFilterNdvColumn => "write.parquet.bloom-filter-ndv.column.",
            Self::SkewSplitting => "spark.sql.adaptive.optimizeSkewsInRebalancePartitions.enabled",
        };
        f.write_str(property)
    }
}

/// One observation about a generation: its code decides what it means and
/// the level it caps, the message carries the numbers for a reader, and the
/// property names what a writer changes; `penalty` is `Some` exactly when
/// `caps` is. The fields are private and `cost` and `advice` are the only
/// constructors: `cost` takes `caps` from the code (`FindingCode::cap`)
/// with a penalty, `advice` sets neither, and each debug-asserts that its
/// code is one it builds, so a capping code's finding is a cost and every
/// other is advice. A finding about one value column names it
/// (`with_column`). Serializes with the field names of its getters, `caps`
/// as null for advice and `column` as null for a finding about the row or
/// the table.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize)]
pub struct Finding {
    caps: Option<Level>,
    code: FindingCode,
    message: String,
    file: Option<String>,
    column: Option<String>,
    penalty: Option<Penalty>,
    property: Option<WriterProperty>,
}

impl Finding {
    /// A capping finding, served with `penalty`; `file` names the data
    /// file, `None` for a table-level one.
    pub(crate) fn cost(
        code: FindingCode,
        file: Option<&str>,
        message: String,
        penalty: Penalty,
        property: Option<WriterProperty>,
    ) -> Self {
        debug_assert!(code.cap().is_some(), "{code} caps no level");
        Self {
            caps: code.cap(),
            code,
            message,
            file: file.map(str::to_string),
            column: None,
            penalty: Some(penalty),
            property,
        }
    }

    /// An advice finding: no answer is wrong and no lookup does more work,
    /// so it caps no level and carries no penalty. Public so a crate that
    /// renders findings can build one in its tests; `cost` is crate-private,
    /// since only the diagnosis measures a penalty.
    pub fn advice(
        code: FindingCode,
        file: Option<&str>,
        message: String,
        property: Option<WriterProperty>,
    ) -> Self {
        debug_assert!(code.cap().is_none(), "{code} caps a level");
        Self {
            caps: None,
            code,
            message,
            file: file.map(str::to_string),
            column: None,
            penalty: None,
            property,
        }
    }

    /// The finding about value column `column`; every per-column code's
    /// finding names its column (`FindingCode::per_column`).
    #[must_use]
    pub fn with_column(self, column: &str) -> Self {
        debug_assert!(self.code.per_column(), "{} names no column", self.code);
        Self {
            column: Some(column.to_string()),
            ..self
        }
    }

    /// The level the finding caps the table at; `None` for advice.
    pub fn caps(&self) -> Option<Level> {
        self.caps
    }

    /// The finding's code.
    pub fn code(&self) -> FindingCode {
        self.code
    }

    /// The numbers behind the finding and the fix, for a person; nothing
    /// decides on it.
    pub(crate) fn message(&self) -> &str {
        &self.message
    }

    /// The data file the finding is about; `None` for a table-level one.
    pub fn file(&self) -> Option<&str> {
        self.file.as_deref()
    }

    /// The value column the finding is about; `None` for one about the row
    /// or the table.
    pub fn column(&self) -> Option<&str> {
        self.column.as_deref()
    }

    /// The finding as a `below_level` refusal names it.
    pub(crate) fn capping(&self) -> Capping {
        Capping {
            code: self.code,
            column: self.column.clone(),
        }
    }

    /// The measured penalty; `Some` exactly when `caps` is.
    pub fn penalty(&self) -> Option<Penalty> {
        self.penalty
    }

    /// The writer property that fixes it, when one does; the partition spec
    /// and compaction are named in the message instead.
    pub fn property(&self) -> Option<WriterProperty> {
        self.property
    }
}

/// What the doctor measured about one data file, from its footer, page
/// indexes, fences, and manifest entry.
#[derive(Clone, Debug, PartialEq, serde::Serialize)]
pub struct FileReport {
    /// The file's location as the manifest recorded it.
    pub path: String,
    /// The bucket the file holds.
    pub bucket: u32,
    /// The table's key type.
    pub key_type: KeyType,
    /// Rows in the file.
    pub rows: i64,
    /// Row groups in the file.
    pub row_groups: usize,
    /// Data pages of the key column across every row group.
    pub key_pages: usize,
    /// Rows in the largest key page, from the key column's offset index: the
    /// keys a lookup in that page decodes.
    pub max_key_page_rows: i64,
    /// The length in bytes of the longest key.
    pub max_key_bytes: usize,
    /// One report per value column the table serves, in the layout's
    /// order (RFD 022).
    pub value_columns: Vec<ValueColumnReport>,
    /// The columns, key or value, that a row group dictionary-encodes: a
    /// dictionary page or a dictionary encoding among the pages' encodings.
    pub dictionary_columns: Vec<String>,
    /// The key column's bloom filter bytes as the footer bounds them,
    /// summed over row groups; `None` when a row group lacks a filter.
    pub bloom_bytes: Option<u64>,
    /// Filter bits per row of the file, from the footer alone, so per row
    /// on an ordered table too, where the bloom findings judge a filter by
    /// its row group's distinct keys; `None` with `bloom_bytes`.
    pub bloom_bits_per_key: Option<f64>,
    /// The key column's compressed chunk bytes: what the Full tier holds
    /// beyond the Disk tier's memory, and the payload of the chunk file the
    /// Disk tier writes, whose whole size, header included, is the
    /// prediction's `disk_bytes`.
    pub key_chunk_bytes: u64,
    /// The share of the distinct first keys of the key pages that the
    /// column index still tells apart (`distinct_mins_ratio`); 1.0 when
    /// every page is addressable by it, and 1.0 for a file whose column
    /// index cannot show truncation.
    pub distinct_page_mins_ratio: f64,
    /// Whether the manifest carries both key bounds and neither is shorter
    /// than the exact key it bounds, which a truncated bound is.
    pub bounds_present: bool,
    /// Bytes the file's index keeps resident in the tier it was built in.
    pub resident_bytes: usize,
    /// What the load recorded of the file's order column; `None` for a
    /// key-value table's file, whose line then carries no `order`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub order: Option<FileOrderReport>,
}

/// What the load recorded of one ordered file's order column (RFD 025,
/// Load), as `OrderFacts` holds it.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize)]
pub struct FileOrderReport {
    /// The least order value over all the file's rows, deleted rows
    /// included.
    pub min: OrderValue,
    /// The greatest order value over all the file's rows, deleted rows
    /// included.
    pub max: OrderValue,
    /// The rows of the file's longest run of one key.
    pub longest_run: u64,
    /// Per row group, in footer order, the distinct keys it holds: what
    /// sizes its bloom filter.
    pub distinct_keys: Vec<u64>,
}

impl FileOrderReport {
    /// The report of a file whose load recorded `facts`.
    fn of(facts: &OrderFacts) -> Self {
        Self {
            min: facts.min.clone(),
            max: facts.max.clone(),
            longest_run: facts.longest_run,
            distinct_keys: facts.distinct_keys.clone(),
        }
    }
}

/// What the doctor measured about one value column of one data file (RFD
/// 022). A file written before the column was added holds none of it: a
/// lookup returns the column's default for its rows, and every count is
/// zero.
#[derive(Clone, Debug, PartialEq, serde::Serialize)]
pub struct ValueColumnReport {
    /// The column's configured name.
    pub column: String,
    /// Whether the file was written before the column was added (RFD 023).
    pub predates_column: bool,
    /// Data pages of the column across every row group.
    pub value_pages: usize,
    /// Mean uncompressed bytes of a value page, the column's uncompressed
    /// chunk bytes over its data pages: what a hit naming it decompresses.
    /// A dictionary page's bytes are counted in: the footer gives its
    /// compressed extent and no uncompressed size, so taking them out would
    /// take a read of its header.
    pub avg_value_page_bytes: u64,
    /// Mean uncompressed bytes of one value, the column's uncompressed chunk
    /// bytes over the file's rows. Against `avg_value_page_bytes` it says
    /// how many values a hit fetches for the one it wanted; reported, never
    /// thresholded, since a membership set with empty values fetches a page
    /// for nothing and is not wrong.
    pub avg_value_bytes: u64,
    /// Whether every page of the column starts on the same row as a key
    /// page in every row group.
    pub pages_aligned: bool,
}

/// One value column's verdict in a table's summary (RFD 020, RFD 022).
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize)]
pub struct ColumnSummary {
    /// The column's configured name.
    pub column: String,
    /// The format the table's entry declares it in (RFD 024).
    pub format: ValueFormat,
    /// The level a lookup naming it achieves in the loaded tier.
    pub level: Level,
    /// The GETs a cold hit spends on it alone.
    pub gets_per_hit: u64,
    /// Data files written before the column was added, for whose rows a
    /// lookup returns the column's default (RFD 023).
    pub files_predating_column: usize,
    /// The column's uncompressed chunk bytes summed over the snapshot's
    /// files, the bytes `ValueColumnReport::avg_value_bytes` divides by each
    /// file's rows, so an owner compares a table's hot share with the
    /// cache budget (RFD 028). A file written before the column was added
    /// contributes none.
    pub value_bytes: u64,
}

/// An ordered table's order column in its summary (RFD 025, Doctor).
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize)]
pub struct OrderSummary {
    /// The order column's configured name.
    pub column: String,
    /// The column's Iceberg type in the snapshot.
    pub order_type: OrderType,
    /// The most rows one Scan returns for a key: the table's configured
    /// `max_scan_rows`, or the default.
    pub max_scan_rows: u32,
    /// The most values one `MultiScan` may request:
    /// `server.max_multi_scan_values`, as `TableInfo` reports it.
    pub max_multi_scan_values: usize,
    /// The encoded size above which a Scan or `MultiScan` response is
    /// refused: `server.max_scan_bytes`, as `TableInfo` reports it.
    pub max_scan_bytes: usize,
    /// The rows of the longest run of one key in any one file. A key that
    /// several files hold, as appends between compactions leave it, has
    /// more rows than its longest run, so this is a floor on its history.
    pub longest_run: u64,
    /// The GETs one cold Scan at `max_scan_rows` makes at worst in the
    /// loaded tier, of a key held in one file and one row group, reading
    /// every configured value column: the figure
    /// `icetray_table_gets_per_scan` publishes while the snapshot serves.
    /// An ordered table's cost of reading a key, which a lookup's
    /// (`TableSummary::lookup_cost`) is not.
    pub gets_per_scan: u64,
    /// The files one Scan reads at worst in the loaded tier, as
    /// `icetray_table_files_probed_per_lookup` publishes it.
    pub files_probed: usize,
}

/// The doctor's verdict on one generation and the numbers an owner chooses
/// a residency by. What the `doctor` and `info` subcommands serialize, so a
/// new measurement here reaches them without a change there.
#[derive(Clone, Debug, PartialEq, serde::Serialize)]
pub struct TableSummary {
    /// The table's configured name.
    pub table: String,
    /// The snapshot diagnosed.
    pub snapshot_id: SnapshotId,
    /// The level the snapshot's layout achieves in the loaded tier: the
    /// dearest cap any finding or the residency sets (RFD 020), which is the
    /// dearest value column's, or that of finding the row for a table of no
    /// value columns (RFD 022).
    pub level: Level,
    /// The level the table declares (`TableConfig::lookups`).
    pub lookups: Level,
    /// The tier the generation was loaded in.
    pub residency: Residency,
    /// Live data files in the snapshot.
    pub file_count: usize,
    /// The default spec's bucket count; `None` for a table without a bucket
    /// transform on the key.
    pub buckets: Option<u32>,
    /// Bucket numbers with at least one file.
    pub buckets_with_files: usize,
    /// Files in the fullest bucket.
    pub max_files_per_bucket: usize,
    /// Rows across every file, from the manifests.
    pub records: u64,
    /// Bytes resident across every file index, in the loaded tier.
    pub index_bytes: usize,
    /// Every tier's resident bytes, from the footers alone (RFD 017), so an
    /// owner chooses a tier from one run; each tier's memory counts the
    /// delete state at its bound from the manifests (RFD 021), as the
    /// `full` budget does.
    pub predicted: ResidentBytes,
    /// The level the layout achieves in every tier that can load the
    /// table: the dearest cap of the findings that tier raises and its best
    /// level together, so an owner sees which tiers reach a level beside
    /// what each costs in `predicted`. A tier that cannot load it is left
    /// out, as Bloom is when `predicted.bloom` is `None`.
    pub level_by_residency: BTreeMap<Residency, Level>,
    /// What one cold lookup pays at worst in the loaded tier, from the same
    /// measurement as `level`; `None` on an ordered table, which serves no
    /// lookup and whose `order` carries the cost of a Scan instead.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub lookup_cost: Option<LookupCost>,
    /// Each value column's level and cost, in the layout's order.
    pub value_columns: Vec<ColumnSummary>,
    /// What the snapshot's deletes come to (RFD 021), from the delete state
    /// the load applied: what the delete gauges show while it serves.
    pub deletes: DeleteSummary,
    /// Capping findings, the count of those whose `caps` is set.
    pub capping: usize,
    /// Advice findings, the count of those whose `caps` is `None`.
    pub advice: usize,
    /// An ordered table's order column and runs; `None` for a key-value
    /// table, whose line then carries no `order`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub order: Option<OrderSummary>,
}

impl TableSummary {
    /// Each value column's level, as `judge` takes them.
    pub fn column_levels(&self) -> Vec<ColumnLevel> {
        self.value_columns
            .iter()
            .map(|column| ColumnLevel {
                column: column.column.clone(),
                level: column.level,
            })
            .collect()
    }
}

/// The whole report: the summary, the capping findings first, and one
/// report per file in generation order.
#[derive(Clone, Debug, PartialEq, serde::Serialize)]
pub struct TableReport {
    /// The verdict and the table-level numbers.
    pub summary: TableSummary,
    /// Every finding, capping before advice, then in the finding table's
    /// order, then by file in generation order.
    pub findings: Vec<Finding>,
    /// One report per file, in generation order.
    pub files: Vec<FileReport>,
}

impl TableReport {
    /// The distinct capping findings by code and column, in the report's
    /// order: what `judge` chooses a `below_level` refusal's findings from.
    pub fn capping(&self) -> Vec<Capping> {
        capping(&self.findings)
    }
}

/// Diagnoses `g` (RFD 016) for a table that declares `lookups` and each
/// value column in the format `formats` gives for its name: measures every
/// file from what the generation holds, raises each finding of the finding
/// table whose trigger the measurements meet, the capping ones through
/// `layout_caps`, and sums every tier's predicted resident bytes from the
/// footers. `scan` is the table's `ScanLimits` (`TableConfig::scan_limits`),
/// `Some` exactly on an ordered table, which reports its order column with
/// it and is judged for `large_runs` by it. No I/O and no metrics.
pub fn diagnose(
    g: &Generation,
    lookups: Level,
    formats: &dyn Fn(&str) -> ValueFormat,
    scan: Option<ScanLimits>,
) -> TableReport {
    let caps = layout_caps(&LayoutView::of(g));
    let deletes = g.delete_summary();
    let facts: Vec<FileFacts> = g
        .files
        .iter()
        .map(|file| measure(file, &g.layout))
        .collect();
    let value_columns = g
        .layout
        .value_columns
        .iter()
        .zip(&caps.columns)
        .enumerate()
        .map(|(at, (column, cost))| ColumnSummary {
            column: column.name.clone(),
            format: formats(&column.name),
            level: cost.level,
            gets_per_hit: cost.gets_per_hit,
            files_predating_column: predating_column(g, at),
            value_bytes: saturating_sum(facts.iter().map(|file| file.column_bytes[at])),
        })
        .collect();
    let buckets = by_bucket(&facts);
    let pending = deletes_pending(g, &caps, deletes);
    let mut findings = caps.findings;
    findings.extend(
        facts
            .iter()
            .flat_map(|file| file_advice_findings(file, &g.layout, scan)),
    );
    findings.extend(many_files_findings(&buckets));
    findings.extend(pending);
    findings.extend(column_defaults(g));
    sort_findings(&mut findings);
    let predicted = with_deletes(predicted_total(&facts), g.delete_bound());
    let capping = findings
        .iter()
        .filter(|finding| finding.caps().is_some())
        .count();
    let order = g
        .layout
        .order
        .as_ref()
        .zip(scan)
        .zip(caps.gets_per_scan)
        .map(|((order, limits), gets_per_scan)| OrderSummary {
            column: order.name.clone(),
            order_type: order.order_type,
            max_scan_rows: limits.max_scan_rows,
            max_multi_scan_values: limits.max_multi_scan_values,
            max_scan_bytes: limits.max_scan_bytes,
            longest_run: longest_run_of(facts.iter().filter_map(|file| file.report.order.as_ref())),
            gets_per_scan,
            files_probed: caps.cost.files_probed,
        });
    let summary = TableSummary {
        table: g.table.clone(),
        snapshot_id: g.snapshot_id,
        level: caps.level,
        lookups,
        residency: g.residency,
        file_count: g.files.len(),
        buckets: g.layout.bucketing.map(|bucketing| bucketing.count),
        buckets_with_files: buckets.len(),
        max_files_per_bucket: buckets.values().map(Vec::len).max().unwrap_or(0),
        records: g.records,
        index_bytes: g.index_bytes,
        predicted,
        level_by_residency: level_by_residency(g, &predicted),
        lookup_cost: g.layout.order.is_none().then_some(caps.cost),
        value_columns,
        deletes,
        capping,
        advice: findings.len() - capping,
        order,
    };
    TableReport {
        summary,
        findings,
        files: facts.into_iter().map(|file| file.report).collect(),
    }
}

/// The most rows one key's run holds in any one of `files`; 0 for none.
fn longest_run_of<'a>(files: impl IntoIterator<Item = &'a FileOrderReport>) -> u64 {
    files
        .into_iter()
        .map(|file| file.longest_run)
        .max()
        .unwrap_or(0)
}

/// What the `doctor` command reports of a table (`examine`): the report,
/// and each protobuf column whose walk could not read its page.
#[derive(Debug)]
pub struct Examined {
    /// `diagnose`'s report with the walk's findings.
    pub report: TableReport,
    /// Each protobuf column whose walk failed, in layout order.
    pub unwalked: Vec<Unwalked>,
}

/// A protobuf column whose value page the doctor's walk could not read, so
/// its values went unchecked.
#[derive(Debug)]
pub struct Unwalked {
    /// The column's configured name.
    pub column: String,
    /// The error the read ended in.
    pub error: Error,
}

/// The `doctor` command's report on `g` (RFD 016, RFD 024): `diagnose`'s,
/// with the walk no load makes of each column `formats` declares protobuf.
/// The walk checks the column's stated default (RFD 023), with no GET,
/// when a file of `g` was written before the column was added (a file written after the
/// column was added holds the column, so a default no file answers is
/// never served), and every value on the first value page of the first file in
/// generation order that holds the column, read with one GET (two for a
/// dictionary-encoded chunk) past the page cache; a page of only nulls
/// passes the walk to the next such file, up to `MOST_WALKED_FILES`. A
/// default or a page holding a value that is not protobuf is
/// `malformed_protobuf` advice naming the column, and the page's file. A
/// read that fails leaves that column `Unwalked` and the report whole.
pub async fn examine(
    g: &Generation,
    lookups: Level,
    formats: &dyn Fn(&str) -> ValueFormat,
    scan: Option<ScanLimits>,
) -> Examined {
    let mut report = diagnose(g, lookups, formats, scan);
    let mut unwalked = Vec::new();
    for (at, column) in g.layout.value_columns.iter().enumerate() {
        match formats(&column.name) {
            ValueFormat::Protobuf => {
                if predating_column(g, at) > 0 {
                    report.findings.extend(walk_default(column));
                }
                match walk_protobuf_page(g, at, &column.name).await {
                    Ok(finding) => report.findings.extend(finding),
                    Err(error) => unwalked.push(Unwalked {
                        column: column.name.clone(),
                        error,
                    }),
                }
            }
            ValueFormat::Opaque => {}
        }
    }
    sort_findings(&mut report.findings);
    report.summary.advice = report.findings.len() - report.summary.capping;
    Examined { report, unwalked }
}

/// `malformed_protobuf` for `column` when the default it states (RFD 023)
/// is not protobuf; `None` when it walks, and when it states none.
fn walk_default(column: &ValueColumnLayout) -> Option<Finding> {
    let default = column.default.as_ref()?;
    let fault = projection::check(default).err()?;
    let name = &column.name;
    Some(
        Finding::advice(
            FindingCode::MalformedProtobuf,
            None,
            format!(
                "the initial-default of value column {name} is not protobuf, at {fault}; a lookup that names fields of the column fails for a row of a file written before the column was added, so declare the column opaque or state a protobuf default"
            ),
            None,
        )
        .with_column(name),
    )
}

/// `malformed_protobuf` for value column `at`, named `name`, when a value
/// on the first value page of a file of `g` that holds the column is not
/// protobuf, the page of the first such file that holds a value, of at
/// most `MOST_WALKED_FILES`; `None` when every value walks, and when no
/// page walked holds one.
async fn walk_protobuf_page(g: &Generation, at: usize, name: &str) -> Result<Option<Finding>> {
    let mut walked_files = 0;
    for file in &g.files {
        if walked_files == MOST_WALKED_FILES {
            break;
        }
        let Some(values) = file.index.first_page_values(at).await? else {
            continue;
        };
        walked_files += 1;
        if values.is_empty() {
            continue;
        }
        let mut faults = values
            .iter()
            .filter_map(|value| projection::check(value).err());
        let Some(first) = faults.next() else {
            return Ok(None);
        };
        let malformed = 1 + faults.count();
        let walked = values.len();
        return Ok(Some(
            Finding::advice(
                FindingCode::MalformedProtobuf,
                Some(&file.entry.path),
                format!(
                    "{malformed} of the {walked} values not null on the first value page of value column {name} are not protobuf, the first at {first}; a lookup that names fields of such a value fails, so declare the column opaque or rewrite its values"
                ),
                None,
            )
            .with_column(name),
        ));
    }
    Ok(None)
}

/// The level `g`'s layout achieves in each tier that can load the table:
/// the caps the findings that tier raises set, with its best level, since
/// an ordered table's order column findings cap `full` and `bloom` alone
/// (RFD 025). A tier `predicted` holds no figure for (bloom, when a row
/// group lacks a filter) is left out.
fn level_by_residency(g: &Generation, predicted: &ResidentBytes) -> BTreeMap<Residency, Level> {
    Residency::ALL
        .into_iter()
        .filter(|residency| predicted.in_tier(*residency).is_some())
        .map(|residency| {
            let caps = layout_caps(&LayoutView {
                residency,
                ..LayoutView::of(g)
            });
            (residency, caps.level)
        })
        .collect()
}

/// The level gate (RFD 020): `RefusalReason::BelowLevel` under `table`
/// when `level` does not meet `declared` and `strict` is on; `Ok`
/// otherwise, since a table below its level that is not strict is flagged
/// and served. A table's level is its dearest value column's, so one column
/// below the declared level refuses the whole snapshot (RFD 022). The
/// refusal names those of the capping findings `capping` whose cap does not
/// meet `declared`, never one the declared level accepts, each with the
/// column it is about, and carries `columns`, each value column's level
/// from the same measurement as `level`. The load decides whether a
/// snapshot is swapped in through it, and the doctor whether a configured
/// table is reported refused, so the two cannot disagree.
pub fn judge(
    table: &str,
    level: Level,
    columns: &[ColumnLevel],
    capping: &[Capping],
    declared: Level,
    strict: bool,
) -> Result<()> {
    if strict && !level.meets(declared) {
        Err(Error::refused(
            table,
            RefusalReason::BelowLevel {
                declared,
                achieved: level,
                columns: columns.to_vec(),
                findings: capping
                    .iter()
                    .filter(|capping| capping.code.cap().is_some_and(|cap| !cap.meets(declared)))
                    .cloned()
                    .collect(),
            },
        ))
    } else {
        Ok(())
    }
}

/// The distinct capping findings by code and column, in the order
/// `findings` gives them.
fn capping(findings: &[Finding]) -> Vec<Capping> {
    let mut capping = Vec::new();
    for finding in findings.iter().filter(|finding| finding.caps().is_some()) {
        let named = finding.capping();
        if !capping.contains(&named) {
            capping.push(named);
        }
    }
    capping
}

/// What a snapshot's level for its layout is judged from (RFD 020): the
/// table's layout and residency, each file's manifest entry and footer, and
/// per bucket the most files a lookup there probes. A built generation
/// gives every part exactly (`LayoutView::of`); none needs a key chunk.
pub(crate) struct LayoutView<'a> {
    pub(crate) layout: &'a TableLayout,
    pub(crate) residency: Residency,
    pub(crate) files: Vec<FileView<'a>>,
    /// Per bucket an overlap group touches, the most files a lookup there
    /// probes, as `Generation::overlapping_buckets` gives it, and on an
    /// ordered table the most files a shared edge key spans.
    pub(crate) widths: &'a ProbeWidths,
}

/// One file as a level is judged from it: its manifest entry and footer,
/// with the key and value columns' leaf positions in the footer's schema,
/// the value columns in the layout's order, and on an ordered table the
/// order column's; no value leaf for a column the file was written before,
/// for whose rows a lookup returns its default, and which makes no finding
/// of that column (RFD 023).
pub(crate) struct FileView<'a> {
    entry: &'a DataFileEntry,
    metadata: &'a ParquetMetaData,
    key_leaf: usize,
    value_leaves: Vec<Option<usize>>,
    order_leaf: Option<usize>,
}

impl<'a> FileView<'a> {
    /// The view of an indexed file.
    pub(crate) fn loaded(file: &'a LoadedFile) -> Self {
        Self {
            entry: &file.entry,
            metadata: file.index.metadata(),
            key_leaf: file.index.key_leaf(),
            value_leaves: file.index.value_leaves(),
            order_leaf: file.index.order_leaf(),
        }
    }

    /// The view of a file past its footer stage, whose manifest entry is
    /// `entry`.
    pub(crate) fn inspected(entry: &'a DataFileEntry, inspected: &'a Inspected) -> Self {
        Self {
            entry,
            metadata: inspected.metadata(),
            key_leaf: inspected.key_leaf(),
            value_leaves: inspected.value_leaves(),
            order_leaf: inspected.order_leaf(),
        }
    }
}

impl<'a> LayoutView<'a> {
    /// The view of a built generation.
    pub(crate) fn of(g: &'a Generation) -> Self {
        Self {
            layout: &g.layout,
            residency: g.residency,
            files: g.files.iter().map(|file| FileView::loaded(file)).collect(),
            widths: g.probe_widths(),
        }
    }
}

/// A snapshot's level for its layout and the findings that set it.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct Caps {
    /// The level in the view's residency: the dearest of `layout` and the
    /// residency's best level, which is the dearest column's level, or that
    /// of finding the row for a table of no value columns (RFD 022).
    pub(crate) level: Level,
    /// The dearest cap the findings set, whatever the residency; `optimal`
    /// for none.
    pub(crate) layout: Level,
    /// The capping findings in the finding table's order, then by file in
    /// view order.
    pub(crate) findings: Vec<Finding>,
    /// What one cold lookup pays at worst in the view's residency.
    pub(crate) cost: LookupCost,
    /// Each value column's level and cost in the view's residency, in the
    /// layout's order.
    pub(crate) columns: Vec<ColumnCost>,
    /// On an ordered table, what one cold Scan pays at worst in the view's
    /// residency (`scan_gets`); `None` on a key-value table.
    pub(crate) gets_per_scan: Option<u64>,
}

impl Caps {
    /// The distinct capping findings by code and column, in the finding
    /// table's order: what `judge` chooses a `below_level` refusal's
    /// findings from.
    pub(crate) fn capping(&self) -> Vec<Capping> {
        capping(&self.findings)
    }
}

/// The capping findings `view` raises, the level they and its residency
/// allow, per value column and for the table, and the cost of a cold lookup
/// there (RFD 020, RFD 022): the one function the doctor's report, the
/// load's level gate, and the cost gauges all measure a layout by, so none
/// can disagree. Per file, a dictionary-encoded value column, named, and
/// large key pages, and on an ordered table under `full` and `bloom` a
/// dictionary-encoded order column; per table, files written with no bucket
/// partition, overlapping files, and on an ordered table shared edge keys.
/// A column's level is its own findings' caps with those of finding the
/// row, the findings that name no column, and the residency's best level;
/// the table's is the dearest of them all. On an ordered table, the cost of
/// a cold Scan beside (`scan_gets`). No `f64` enters.
pub(crate) fn layout_caps(view: &LayoutView<'_>) -> Caps {
    let mut findings = Vec::new();
    for file in &view.files {
        file_capping_findings(file, view.layout, view.residency, &mut findings);
    }
    table_capping_findings(view, &mut findings);
    sort_findings(&mut findings);
    let dearest = |about: &dyn Fn(&Finding) -> bool| {
        findings
            .iter()
            .filter(|finding| about(finding))
            .filter_map(Finding::caps)
            .fold(Level::Optimal, Level::max)
    };
    let layout = dearest(&|_| true);
    let row = dearest(&|finding| finding.column().is_none());
    let best = view.residency.best_level();
    let row_gets = row_gets(view);
    let columns = view
        .layout
        .value_columns
        .iter()
        .map(|column| {
            let about = |finding: &Finding| finding.column() == Some(column.name.as_str());
            let extra = findings
                .iter()
                .filter(|finding| about(finding))
                .filter_map(|finding| finding.penalty()?.extra_gets_per_hit)
                .max()
                .unwrap_or(0);
            ColumnCost {
                level: row.max(dearest(&about)).max(best),
                gets_per_hit: VALUE_PAGE_GETS + extra,
            }
        })
        .collect::<Vec<_>>();
    let dearest_column = columns
        .iter()
        .map(|column| column.gets_per_hit)
        .max()
        .unwrap_or(0);
    let gets_per_scan = view
        .layout
        .order
        .as_ref()
        .map(|_| scan_gets(view.residency, &row_gets, &columns, &findings));
    let key_pages = row_gets.per_file() * approx::count(row_gets.files_probed);
    Caps {
        level: layout.max(best),
        layout,
        cost: LookupCost {
            gets_per_hit: key_pages + dearest_column,
            gets_per_miss: key_pages,
            files_probed: row_gets.files_probed,
        },
        columns,
        gets_per_scan,
        findings,
    }
}

/// What finding a row costs a cold lookup on `view`: under `bloom` the GET
/// of a key page in every file probed, each filter answering yes, with the
/// key column's dictionary page when any file's is dictionary-encoded, and
/// the files probed.
struct RowGets {
    /// The GETs of one key page: `KEY_PAGE_GETS` under `bloom`, none under
    /// `full` and `disk`.
    key_page: u64,
    /// The GETs of the key column's dictionary page in one file: one under
    /// `bloom` when any file's key column is dictionary-encoded, else none.
    key_dictionary: u64,
    files_probed: usize,
}

impl RowGets {
    /// The GETs finding the row spends in one file probed.
    fn per_file(&self) -> u64 {
        self.key_page + self.key_dictionary
    }
}

/// The cost of finding a row on `view` (`RowGets`). The files probed are
/// the most any bucket's lookup probes, the figure `overlapping_files`
/// carries, or on an ordered table `shared_edge_keys` where it is more;
/// `no_bucket_partition`'s figure counts every unbucketed file, while a
/// lookup probes only those whose exact key range covers its key, which
/// the overlap figure already narrows to.
fn row_gets(view: &LayoutView<'_>) -> RowGets {
    let files_probed = view.widths.most_probed().unwrap_or(FILES_PROBED_ALONE);
    let (key_page, key_dictionary) = match view.residency {
        Residency::Full | Residency::Disk => (0, 0),
        Residency::Bloom => {
            let key_dictionary = view
                .files
                .iter()
                .any(|file| column_chunk_facts(file.metadata, file.key_leaf).dictionary);
            let dictionary = if key_dictionary {
                DICTIONARY_PAGE_GETS
            } else {
                0
            };
            (KEY_PAGE_GETS, dictionary)
        }
    };
    RowGets {
        key_page,
        key_dictionary,
        files_probed,
    }
}

/// The GETs one cold Scan of every configured value column at
/// `max_scan_rows` makes at worst in `residency` on an ordered table, for
/// a key held in one file and one row group (RFD 025, Cost and Metrics):
/// under `bloom` the run's first and last key pages, `RUN_EDGE_KEY_PAGES`
/// once the run crosses a page boundary, with the key column's dictionary
/// page as `row` counts it; under `full` and `bloom` the order column's
/// span and, under `dictionary_encoded_order` among `findings`, its
/// dictionary page, which `disk` reads from the chunk file; and each value
/// column's span with its dictionary page, as `columns` gives it. The rows
/// one Scan takes within one row group are contiguous, so each column is
/// one span however many rows `max_scan_rows` lets it take.
fn scan_gets(
    residency: Residency,
    row: &RowGets,
    columns: &[ColumnCost],
    findings: &[Finding],
) -> u64 {
    let find_run = row.key_page * RUN_EDGE_KEY_PAGES + row.key_dictionary;
    let order = if order_from_store(residency) {
        let dictionary = findings
            .iter()
            .any(|finding| finding.code() == FindingCode::DictionaryEncodedOrder);
        ORDER_SPAN_GETS + if dictionary { DICTIONARY_PAGE_GETS } else { 0 }
    } else {
        0
    };
    let values: u64 = columns.iter().map(|column| column.gets_per_hit).sum();
    find_run + order + values
}

/// Everything measured about one file: its report, and what the advice
/// findings need beyond it (the manifest's size, the rows as a count, and
/// the footer's prediction).
struct FileFacts {
    report: FileReport,
    /// Each configured value column's uncompressed chunk bytes in the file,
    /// in the layout's order; zero for a column the file predates.
    column_bytes: Vec<u64>,
    size: u64,
    rows: u64,
    predicted: ResidentBytes,
    bloom_by_row_group: Option<BloomExtremes>,
    /// What an ordered table's advice needs of the file; `None` on a
    /// key-value table.
    order: Option<OrderAdviceFacts>,
}

/// What the advice on an ordered table's runs measures in one file (RFD
/// 025, Levels and findings).
struct OrderAdviceFacts {
    /// The rows of the file's longest run of one key, as the load counted
    /// them (`OrderFacts::longest_run`).
    longest_run: u64,
}

/// The key column's bloom filter sizing at the row groups that size it
/// worst. Filters are per row group and a miss consults only the row group
/// the fences name, so the file's mean can hide one undersized filter beside
/// well-sized ones (RFD 016 judges a filter by its row group's key count).
/// A row group's keys are its rows on a key-value table and its distinct
/// keys on an ordered one, which the load counts
/// (`OrderFacts::distinct_keys`), since a filter holds each key once
/// however many rows repeat it (RFD 025). `None` when any row group lacks
/// a bounded filter.
#[derive(Clone, Copy, Debug, PartialEq)]
struct BloomExtremes {
    /// The most filter bytes per key over the row groups, and that row group.
    most_bytes_per_key: (f64, usize),
    /// The fewest filter bits per key over the row groups, and that row group.
    fewest_bits_per_key: (f64, usize),
}

fn bloom_extremes(
    metadata: &ParquetMetaData,
    key_leaf: usize,
    distinct_keys: Option<&[u64]>,
) -> Option<BloomExtremes> {
    let mut most: Option<(f64, usize)> = None;
    let mut fewest: Option<(f64, usize)> = None;
    for (row_group, group) in metadata.row_groups().iter().enumerate() {
        let column = group.column(key_leaf);
        column.bloom_filter_offset()?;
        let length = column.bloom_filter_length()?;
        let bytes = approx::measured(length);
        let keys = match distinct_keys {
            Some(distinct) => *distinct.get(row_group).expect(
                "the order facts count one row group's distinct keys per row group of the footer",
            ),
            None => approx::measured(group.num_rows()),
        };
        let bytes_per_key = approx::ratio(bytes, keys);
        let bits_per_key = bits_per_key(bytes, keys);
        if most.is_none_or(|(value, _)| bytes_per_key > value) {
            most = Some((bytes_per_key, row_group));
        }
        if fewest.is_none_or(|(value, _)| bits_per_key < value) {
            fewest = Some((bits_per_key, row_group));
        }
    }
    Some(BloomExtremes {
        most_bytes_per_key: most?,
        fewest_bits_per_key: fewest?,
    })
}

/// The measured files of a generation by bucket number, in generation order
/// within a bucket. Keyed by number alone, as `overlapping_buckets` is:
/// after a spec evolution, buckets of two bucketings that share a number
/// share one entry.
type Buckets<'a> = BTreeMap<u32, Vec<&'a FileFacts>>;

fn by_bucket(facts: &[FileFacts]) -> Buckets<'_> {
    let mut buckets = Buckets::new();
    for file in facts {
        buckets.entry(file.report.bucket).or_default().push(file);
    }
    buckets
}

/// Measures `file` from its index and manifest entry.
fn measure(file: &LoadedFile, layout: &TableLayout) -> FileFacts {
    let index = &file.index;
    let metadata = index.metadata();
    let predicted = FileIndex::predicted_resident_bytes(
        metadata,
        index.key_leaf(),
        index.order_column(),
        &index.held_value_leaves(),
        index.key_type(),
        index.handle(),
    );
    let pages = key_page_facts(metadata, index.key_leaf());
    let order_facts = layout.order.as_ref().and(index.order_facts());
    let mut column_bytes = Vec::with_capacity(layout.value_columns.len());
    let mut dictionary_columns = Vec::new();
    if column_chunk_facts(metadata, index.key_leaf()).dictionary {
        dictionary_columns.push(layout.key_column.clone());
    }
    let value_columns = layout
        .value_columns
        .iter()
        .zip(index.value_leaves())
        .map(|(column, leaf)| {
            let Some(leaf) = leaf else {
                column_bytes.push(0);
                return ValueColumnReport {
                    column: column.name.clone(),
                    predates_column: true,
                    value_pages: 0,
                    avg_value_page_bytes: 0,
                    avg_value_bytes: 0,
                    pages_aligned: true,
                };
            };
            let chunks = column_chunk_facts(metadata, leaf);
            if chunks.dictionary {
                dictionary_columns.push(column.name.clone());
            }
            column_bytes.push(chunks.uncompressed_bytes);
            let (value_pages, pages_aligned) = value_page_facts(metadata, index.key_leaf(), leaf);
            ValueColumnReport {
                column: column.name.clone(),
                predates_column: false,
                value_pages,
                avg_value_page_bytes: chunks
                    .uncompressed_bytes
                    .checked_div(approx::count(value_pages))
                    .unwrap_or(0),
                avg_value_bytes: chunks
                    .uncompressed_bytes
                    .checked_div(rows_of(index))
                    .unwrap_or(0),
                pages_aligned,
            }
        })
        .collect();
    FileFacts {
        column_bytes,
        report: FileReport {
            path: file.entry.path.clone(),
            bucket: file.entry.bucket,
            key_type: layout.key_type,
            rows: index.rows(),
            row_groups: metadata.num_row_groups(),
            key_pages: pages.key_pages,
            max_key_page_rows: pages.max_key_page_rows,
            max_key_bytes: index.max_key_bytes(),
            value_columns,
            dictionary_columns,
            bloom_bytes: predicted.filter_bytes,
            bloom_bits_per_key: predicted.bloom_bits_per_key,
            key_chunk_bytes: predicted.key_chunk_bytes,
            distinct_page_mins_ratio: distinct_mins_ratio(
                metadata,
                index.key_leaf(),
                index.fences(),
            ),
            bounds_present: bounds_intact(&file.entry, index.key_range()),
            resident_bytes: index.resident_bytes(),
            order: order_facts.map(FileOrderReport::of),
        },
        size: file.entry.size,
        rows: rows_of(index),
        predicted,
        bloom_by_row_group: bloom_extremes(
            metadata,
            index.key_leaf(),
            order_facts.map(|facts| facts.distinct_keys.as_slice()),
        ),
        order: order_facts.map(|facts| OrderAdviceFacts {
            longest_run: facts.longest_run,
        }),
    }
}

/// The file's rows as a count.
fn rows_of(index: &FileIndex) -> u64 {
    u64::try_from(index.rows())
        .expect("FileIndex::build refused a negative row count as FileFault::InconsistentRowCounts")
}

/// What the key column's offset index says about a file's key pages.
struct KeyPageFacts {
    key_pages: usize,
    max_key_page_rows: i64,
}

/// The offset index of a file the index admitted.
fn offset_index(metadata: &ParquetMetaData) -> &ParquetOffsetIndex {
    metadata.offset_index().expect(
        "FileIndex::build refused a file without an offset index as FileFault::MissingPageIndex",
    )
}

/// Counts the key pages of every row group and finds the largest in rows (a
/// page's rows run to the next page's first row, or the row group's end).
fn key_page_facts(metadata: &ParquetMetaData, key_leaf: usize) -> KeyPageFacts {
    let mut facts = KeyPageFacts {
        key_pages: 0,
        max_key_page_rows: 0,
    };
    for (row_group, columns) in offset_index(metadata).iter().enumerate() {
        let key = columns[key_leaf].page_locations();
        facts.key_pages += key.len();
        let group_rows = metadata.row_group(row_group).num_rows();
        for (at, page) in key.iter().enumerate() {
            let end = key
                .get(at + 1)
                .map_or(group_rows, |next| next.first_row_index);
            facts.max_key_page_rows = facts.max_key_page_rows.max(end - page.first_row_index);
        }
    }
    facts
}

/// The pages of the value column at `value_leaf` across every row group,
/// and whether each starts on the same row as a key page.
fn value_page_facts(
    metadata: &ParquetMetaData,
    key_leaf: usize,
    value_leaf: usize,
) -> (usize, bool) {
    let mut pages = 0;
    let mut aligned = true;
    for columns in offset_index(metadata) {
        let key = columns[key_leaf].page_locations();
        let value = columns[value_leaf].page_locations();
        pages += value.len();
        aligned &= key.len() == value.len()
            && key.iter().zip(value).all(|(key_page, value_page)| {
                key_page.first_row_index == value_page.first_row_index
            });
    }
    (pages, aligned)
}

/// What one column's chunks say: whether some row group dictionary-encodes
/// it, and its uncompressed bytes.
struct ColumnChunkFacts {
    dictionary: bool,
    uncompressed_bytes: u64,
}

fn column_chunk_facts(metadata: &ParquetMetaData, leaf: usize) -> ColumnChunkFacts {
    let mut facts = ColumnChunkFacts {
        dictionary: false,
        uncompressed_bytes: 0,
    };
    for group in metadata.row_groups() {
        let chunk = group.column(leaf);
        facts.dictionary |= dictionary_encoded(chunk);
        facts.uncompressed_bytes += approx::measured(chunk.uncompressed_size());
    }
    facts
}

/// Whether a column chunk is dictionary-encoded: it has a dictionary page,
/// or a dictionary encoding is among its pages' encodings.
fn dictionary_encoded(chunk: &ColumnChunkMetaData) -> bool {
    let encodings = chunk.encodings_mask();
    chunk.dictionary_page_offset().is_some()
        || encodings.is_set(Encoding::RLE_DICTIONARY)
        || encodings.is_set(Encoding::PLAIN_DICTIONARY)
}

/// The share of the distinct first keys of the key column's pages that its
/// column index still tells apart: the distinct page minimums over the
/// distinct first keys the load decoded (`fences`, the first key of every
/// key page), 1.0 when every page is addressable by its minimum, lower when
/// a writer cut the minimums to a shared prefix. An untruncated minimum is
/// its page's first key, since the keys ascend, so the ratio is below 1.0
/// only where a minimum is shorter than the key it bounds. On a key-value
/// table every page starts a new key, so the distinct first keys are the
/// pages and this is the share of pages with a distinct minimum; on an
/// ordered table a key's run spans pages that legitimately share one
/// minimum, and counting pages would read a long run as truncation. The key
/// column is `INT64`, `BYTE_ARRAY`, or `FIXED_LEN_BYTE_ARRAY`
/// (`FileIndex::build` refused any other), and a page without a minimum (a
/// null page, which a key column never has) is not counted. A file without
/// a column index, or whose index lists no page, is
/// `DISTINCT_MINS_UNOBSERVABLE`: truncation is not observable there, so it
/// draws no finding.
fn distinct_mins_ratio(metadata: &ParquetMetaData, key_leaf: usize, fences: &[Vec<Key>]) -> f64 {
    let Some(index) = metadata.column_index() else {
        return DISTINCT_MINS_UNOBSERVABLE;
    };
    let (mut distinct, mut first_keys) = (0, 0);
    for (columns, group_fences) in index.iter().zip(fences) {
        distinct += match columns.get(key_leaf) {
            Some(ColumnIndexMetaData::INT64(native)) => {
                distinct_count(native.min_values_iter().flatten())
            }
            Some(
                ColumnIndexMetaData::BYTE_ARRAY(bytes)
                | ColumnIndexMetaData::FIXED_LEN_BYTE_ARRAY(bytes),
            ) => distinct_count(bytes.min_values_iter().flatten()),
            Some(
                ColumnIndexMetaData::NONE
                | ColumnIndexMetaData::BOOLEAN(_)
                | ColumnIndexMetaData::INT32(_)
                | ColumnIndexMetaData::INT96(_)
                | ColumnIndexMetaData::FLOAT(_)
                | ColumnIndexMetaData::DOUBLE(_),
            )
            | None => continue,
        };
        first_keys += distinct_count(group_fences.iter());
    }
    if distinct == 0 {
        DISTINCT_MINS_UNOBSERVABLE
    } else {
        approx::ratio(approx::count(distinct), approx::count(first_keys))
    }
}

/// How many of `values` differ: none exactly when there are none, as for
/// an index that lists no page.
fn distinct_count<T: Ord>(values: impl Iterator<Item = T>) -> usize {
    values.collect::<BTreeSet<_>>().len()
}

/// Whether the manifest's key bounds are present and not truncated: both
/// recorded, and each at least as long as the exact key it bounds, since a
/// truncated string or binary bound is a cut prefix, shorter than the key
/// (prototype findings, finding 8). Long and uuid bounds are fixed width and
/// pass once present. The bound and the key share the table's key type.
fn bounds_intact(entry: &DataFileEntry, (first, last): (&Key, &Key)) -> bool {
    match (&entry.key_min, &entry.key_max) {
        (Some(min), Some(max)) => {
            min.as_bytes().len() >= first.as_bytes().len()
                && max.as_bytes().len() >= last.as_bytes().len()
        }
        (None, _) | (_, None) => false,
    }
}

/// Every advice finding of the finding table that one file's
/// measurements trigger, in the table's order.
fn file_advice_findings(
    facts: &FileFacts,
    layout: &TableLayout,
    scan: Option<ScanLimits>,
) -> Vec<Finding> {
    let mut findings = Vec::new();
    file_layout_advice(facts, layout, scan, &mut findings);
    bloom_findings(facts, layout, &mut findings);
    findings
}

/// The per-file capping findings, from the file's footer and manifest
/// entry alone: each dictionary-encoded value column, named, large key
/// pages, and on an ordered table in `residency` the order column's
/// (`order_capping_findings`).
fn file_capping_findings(
    file: &FileView<'_>,
    layout: &TableLayout,
    residency: Residency,
    findings: &mut Vec<Finding>,
) {
    let path = &file.entry.path;
    for (column, leaf) in layout.value_columns.iter().zip(&file.value_leaves) {
        let Some(leaf) = *leaf else {
            continue;
        };
        if column_chunk_facts(file.metadata, leaf).dictionary {
            findings.push(dictionary_finding(path, column));
        }
    }
    let rows = key_page_facts(file.metadata, file.key_leaf).max_key_page_rows;
    if rows > thresholds::KEY_PAGE_ROWS_LARGE {
        findings.push(Finding::cost(
            FindingCode::LargeKeyPages,
            Some(path),
            format!(
                "key pages hold up to {rows} rows, above {limit}; a lookup decodes that many keys ({property}={contract_rows})",
                limit = thresholds::KEY_PAGE_ROWS_LARGE,
                property = WriterProperty::PageRowLimit,
                contract_rows = contract::PAGE_ROWS,
            ),
            Penalty {
                keys_decoded_per_lookup: Some(rows),
                ..Penalty::default()
            },
            Some(WriterProperty::PageRowLimit),
        ));
    }
    if let (Some(_), Some(order)) = (&layout.order, file.order_leaf) {
        order_capping_findings(file, order, residency, findings);
    }
}

/// Whether a Scan in `residency` reads an ordered table's order column
/// from the store: under `full` and `bloom` it does, and under `disk` the
/// chunk file holds the column, its dictionary page included, at no GET.
fn order_from_store(residency: Residency) -> bool {
    match residency {
        Residency::Full | Residency::Bloom => true,
        Residency::Disk => false,
    }
}

/// An ordered table's per-file capping finding on the order column at leaf
/// `order` (RFD 025, Levels and findings), where a Scan reads the column
/// from the store alone (`order_from_store`). A dictionary-encoded order
/// column is `dictionary_encoded_order`, since every Scan fetches its
/// dictionary page.
fn order_capping_findings(
    file: &FileView<'_>,
    order: usize,
    residency: Residency,
    findings: &mut Vec<Finding>,
) {
    if order_from_store(residency) && column_chunk_facts(file.metadata, order).dictionary {
        findings.push(dictionary_cost(
            FindingCode::DictionaryEncodedOrder,
            &file.entry.path,
            format!(
                "the order column is dictionary-encoded; every Scan fetches its dictionary page besides the order pages, {gets} extra GET per file ({property}=false)",
                gets = DICTIONARY_PAGE_GETS,
                property = WriterProperty::EnableDictionary,
            ),
        ));
    }
}

/// `dictionary_encoded_value` on value column `column` of the file at
/// `path`.
fn dictionary_finding(path: &str, column: &ValueColumnLayout) -> Finding {
    dictionary_cost(
        FindingCode::DictionaryEncodedValue,
        path,
        format!(
            "value column {value} is dictionary-encoded; a hit naming it fetches its dictionary page besides the value page, {gets} extra GET ({property}=false)",
            value = column.name,
            gets = DICTIONARY_PAGE_GETS,
            property = WriterProperty::EnableDictionary,
        ),
    )
    .with_column(&column.name)
}

/// The cost finding `code`, saying `message`, on the file at `path` for a
/// dictionary-encoded column: a hit fetches the column's dictionary page,
/// `DICTIONARY_PAGE_GETS` more GETs, which writing the column without a
/// dictionary removes.
fn dictionary_cost(code: FindingCode, path: &str, message: String) -> Finding {
    Finding::cost(
        code,
        Some(path),
        message,
        Penalty {
            extra_gets_per_hit: Some(DICTIONARY_PAGE_GETS),
            ..Penalty::default()
        },
        Some(WriterProperty::EnableDictionary),
    )
}

/// The per-file advice apart from the bloom filter: bounds, column index,
/// each value column's pages (over the advice line, and over the lookup's
/// ceiling), key length, file size, and on an ordered table the runs
/// (`order_advice`). The one `f64` comparison is on
/// `distinct_page_mins_ratio`, which `approx::ratio` makes finite (zero for
/// a zero denominator), so a plain comparison is total.
fn file_layout_advice(
    facts: &FileFacts,
    layout: &TableLayout,
    scan: Option<ScanLimits>,
    findings: &mut Vec<Finding>,
) {
    let report = &facts.report;
    if !report.bounds_present {
        findings.push(Finding::advice(
            FindingCode::BoundsMissing,
            Some(&report.path),
            format!(
                "the manifest's key bounds for this file are missing or shorter than its exact first or last key; every file of the bucket looks alike to a reader of the manifest ({property}{key}=full)",
                property = WriterProperty::MetricsColumn,
                key = layout.key_column,
            ),
            Some(WriterProperty::MetricsColumn),
        ));
    }
    if report.distinct_page_mins_ratio < thresholds::DISTINCT_MINS_RATIO_ADVICE {
        findings.push(Finding::advice(
            FindingCode::ColumnIndexTruncated,
            Some(&report.path),
            format!(
                "{ratio:.2} of the distinct first keys of key pages keep a distinct minimum in the column index, below {limit}; readers that route by it cannot tell the pages apart ({property}, at least the longest key)",
                ratio = report.distinct_page_mins_ratio,
                limit = thresholds::DISTINCT_MINS_RATIO_ADVICE,
                property = WriterProperty::ColumnIndexTruncateLength,
            ),
            Some(WriterProperty::ColumnIndexTruncateLength),
        ));
    }
    for column in &report.value_columns {
        value_page_advice(&report.path, column, findings);
    }
    if let Some(order) = &facts.order {
        let value_bytes = saturating_sum(facts.column_bytes.iter().copied());
        order_advice(&report.path, order, facts.rows, value_bytes, scan, findings);
    }
    if report.max_key_bytes > contract::KEY_BYTES_ADVICE {
        findings.push(Finding::advice(
            FindingCode::LongKeys,
            Some(&report.path),
            format!(
                "keys run to {bytes} bytes, above {limit}; resident bytes per key grow with key length (shorter keys, or a fixed-width long)",
                bytes = report.max_key_bytes,
                limit = contract::KEY_BYTES_ADVICE,
            ),
            None,
        ));
    }
    if facts.size > thresholds::FILE_BYTES_ADVICE {
        findings.push(Finding::advice(
            FindingCode::LargeFile,
            Some(&report.path),
            format!(
                "data file is {size} bytes, above {limit}; first-byte latency of a range GET rises with object size ({property} between {min} and {max})",
                size = facts.size,
                limit = thresholds::FILE_BYTES_ADVICE,
                property = WriterProperty::TargetFileSizeBytes,
                min = contract::TARGET_FILE_BYTES_MIN,
                max = contract::TARGET_FILE_BYTES_MAX,
            ),
            Some(WriterProperty::TargetFileSizeBytes),
        ));
    }
}

/// An ordered table's advice on one file (RFD 025, Levels and findings):
/// `long_runs` where a run is longer than `thresholds::RUN_ROWS_ADVICE`;
/// and, where the table's `scan` limits are known, `large_runs` where
/// `max_scan_rows` rows of the configured value columns, at the footer's
/// uncompressed bytes per row, `value_bytes` over the file's `rows`, are
/// more than `max_scan_bytes`, so a Scan at the full limit would be
/// refused, compared in integers.
fn order_advice(
    path: &str,
    order: &OrderAdviceFacts,
    rows: u64,
    value_bytes: u64,
    scan: Option<ScanLimits>,
    findings: &mut Vec<Finding>,
) {
    let run_rows = u64::from(thresholds::RUN_ROWS_ADVICE);
    if order.longest_run > run_rows {
        findings.push(Finding::advice(
            FindingCode::LongRuns,
            Some(path),
            format!(
                "a key holds a run of {run} rows, above {run_rows}; a caller reads its whole history in more than one Scan at the default max_scan_rows, paging by order value",
                run = order.longest_run,
            ),
            None,
        ));
    }
    let Some(limits) = scan else {
        return;
    };
    let scan_rows = u64::from(limits.max_scan_rows);
    let bound = approx::count(limits.max_scan_bytes);
    let scan_bytes = value_bytes.saturating_mul(scan_rows);
    if rows > 0 && scan_bytes > bound.saturating_mul(rows) {
        findings.push(Finding::advice(
            FindingCode::LargeRuns,
            Some(path),
            format!(
                "{scan_rows} rows of the configured value columns come to {bytes} uncompressed bytes at this file's {per_row} bytes per row, above server.max_scan_bytes {bound}, so a Scan at max_scan_rows {scan_rows} is refused as FAILED_PRECONDITION; the figure counts the value columns alone, so a table a little under the bound can still be refused, since each row also carries its order value and framing (lower max_scan_rows, have callers request fewer rows or columns, or raise server.max_scan_bytes where every client accepts larger messages)",
                bytes = scan_bytes / rows,
                per_row = value_bytes / rows,
            ),
            None,
        ));
    }
}

/// The advice on one value column's pages: over the advice line, and over
/// the lookup's ceiling, each naming the column.
fn value_page_advice(path: &str, column: &ValueColumnReport, findings: &mut Vec<Finding>) {
    let name = &column.column;
    if column.avg_value_page_bytes > thresholds::VALUE_PAGE_BYTES_ADVICE {
        findings.push(
            Finding::advice(
                FindingCode::LargeValuePages,
                Some(path),
                format!(
                    "pages of value column {name} decompress to {bytes} bytes each on average, above {limit}; bound them by bytes ({size}={bound}) and shorten the page-size check interval ({check}), which the writer applies between batches",
                    bytes = column.avg_value_page_bytes,
                    limit = thresholds::VALUE_PAGE_BYTES_ADVICE,
                    size = WriterProperty::PageSizeBytes,
                    bound = contract::VALUE_PAGE_BYTES,
                    check = WriterProperty::RowGroupCheckRecordCount,
                ),
                Some(WriterProperty::PageSizeBytes),
            )
            .with_column(name),
        );
    }
    if column.avg_value_page_bytes > thresholds::MOST_VALUE_PAGE_BYTES {
        findings.push(
            Finding::advice(
                FindingCode::ValuePagesOverCeiling,
                Some(path),
                format!(
                    "pages of value column {name} decompress to {bytes} bytes each on average, above the {ceiling}-byte ceiling on a value page a lookup reads; a lookup into a value page above it is refused with a file fault, so a lookup naming the column is not served. The trigger is the mean of the column's uncompressed bytes over its data pages, the dictionary page's bytes included: it can miss one oversized page among smaller ones, and a large value dictionary can raise it above the ceiling when no page is ({size}={bound})",
                    bytes = column.avg_value_page_bytes,
                    ceiling = thresholds::MOST_VALUE_PAGE_BYTES,
                    size = WriterProperty::PageSizeBytes,
                    bound = contract::VALUE_PAGE_BYTES,
                ),
                Some(WriterProperty::PageSizeBytes),
            )
            .with_column(name),
        );
    }
}

/// The bloom filter advice: none, oversized, or undersized. Sizing is
/// judged at the row group that sizes worst, since a miss consults one row
/// group's filter, per key as `bloom_extremes` counts a row group's keys;
/// the report keeps the file's figures, per row. Bytes per key and
/// bits per key come from `approx::ratio` and are finite (zero for a zero
/// denominator), so the plain comparisons are total.
fn bloom_findings(facts: &FileFacts, layout: &TableLayout, findings: &mut Vec<Finding>) {
    let report = &facts.report;
    let (Some(bytes), Some(extremes)) = (report.bloom_bytes, facts.bloom_by_row_group) else {
        findings.push(Finding::advice(
            FindingCode::NoBloomFilter,
            Some(&report.path),
            format!(
                "no bloom filter on {key}; the bloom residency tier cannot load this file ({property}{key}=true)",
                key = layout.key_column,
                property = WriterProperty::BloomFilterEnabled,
            ),
            Some(WriterProperty::BloomFilterEnabled),
        ));
        return;
    };
    let (bytes_per_key, largest) = extremes.most_bytes_per_key;
    if bytes_per_key > thresholds::BLOOM_BYTES_PER_KEY_ADVICE {
        findings.push(Finding::advice(
            FindingCode::BloomOversized,
            Some(&report.path),
            format!(
                "bloom filter of row group {largest} is {bytes_per_key:.1} bytes per key ({bytes} filter bytes in the file of {rows} rows), above {limit}; the bloom tier's memory follows it ({property}, Iceberg 1.11+)",
                rows = facts.rows,
                limit = thresholds::BLOOM_BYTES_PER_KEY_ADVICE,
                property = WriterProperty::BloomFilterMaxBytes,
            ),
            Some(WriterProperty::BloomFilterMaxBytes),
        ));
    }
    let (bits_per_key, smallest) = extremes.fewest_bits_per_key;
    if bits_per_key < thresholds::BLOOM_BITS_PER_KEY_MIN {
        findings.push(Finding::advice(
            FindingCode::BloomUndersized,
            Some(&report.path),
            format!(
                "bloom filter of row group {smallest} has {bits_per_key:.1} bits per key, below {limit}; the bloom tier's miss cost rises with the false-positive rate ({property}{key}, Iceberg 1.11+)",
                limit = thresholds::BLOOM_BITS_PER_KEY_MIN,
                property = WriterProperty::BloomFilterNdvColumn,
                key = layout.key_column,
            ),
            Some(WriterProperty::BloomFilterNdvColumn),
        ));
    }
}

/// The table-level capping findings: files written with no bucket
/// partition, overlapping files per bucket, and on an ordered table shared
/// edge keys per bucket.
fn table_capping_findings(view: &LayoutView<'_>, findings: &mut Vec<Finding>) {
    let key = &view.layout.key_column;
    let total = view.files.len();
    // Every file written under a spec with no bucket on the key is a
    // candidate for every lookup; a bucketed file joins them only for the
    // bucket the key hashes to. The default spec says nothing about which
    // files those are after a spec evolution.
    let unbucketed = view
        .files
        .iter()
        .filter(|file| file.entry.bucketing.is_none())
        .count();
    if unbucketed > 0 && total > 1 {
        let probed = unbucketed + usize::from(unbucketed < total);
        findings.push(Finding::cost(
            FindingCode::NoBucketPartition,
            None,
            format!(
                "{unbucketed} of {total} files were written under a spec with no bucket transform on {key}; a lookup probes every one of them whose key range covers the key, plus the bucketed file its key hashes to (partition by bucket(N, {key}) and rewrite the unbucketed files)",
            ),
            Penalty::files_probed(probed),
            None,
        ));
    }
    for (&bucket, &probed) in &view.widths.overlapping {
        findings.push(Finding::cost(
            FindingCode::OverlappingFiles,
            None,
            format!(
                "bucket {bucket} holds files with intersecting key ranges, checked free of shared keys at load; a lookup there probes up to {probed} files ({property}=false on the write, or a compaction)",
                property = WriterProperty::SkewSplitting,
            ),
            Penalty::files_probed(probed),
            Some(WriterProperty::SkewSplitting),
        ));
    }
    for (&bucket, &spanned) in &view.widths.shared_edges {
        findings.push(Finding::cost(
            FindingCode::SharedEdgeKeys,
            None,
            format!(
                "bucket {bucket} holds files one snapshot added that meet where one file's last key is the next one's first, as a compaction that rolls files by size writes them; a Scan of such an edge key reads up to {spanned} files, a cost no later commit grows (a compaction that writes the bucket in one file removes it)",
            ),
            Penalty::files_probed(spanned),
            None,
        ));
    }
}

/// The table-level advice: many files per bucket.
fn many_files_findings(buckets: &Buckets<'_>) -> Vec<Finding> {
    let mut findings = Vec::new();
    for (&bucket, files) in buckets {
        let bytes: u64 = files.iter().map(|file| file.size).sum();
        let explained = bytes
            .div_ceil(contract::TARGET_FILE_BYTES_MAX)
            .max(MIN_EXPLAINED_FILES);
        let held = approx::count(files.len());
        if held > thresholds::FILES_PER_BUCKET_FACTOR * explained {
            findings.push(Finding::advice(
                FindingCode::ManyFilesPerBucket,
                None,
                format!(
                    "bucket {bucket} holds {held} files where its {bytes} bytes explain {explained} at {target} bytes per file; loads and manifests grow with the file count (compact the bucket to the file size target)",
                    target = contract::TARGET_FILE_BYTES_MAX,
                ),
                None,
            ));
        }
    }
    findings
}

/// `deletes_pending` (RFD 021) when `deletes`, `g`'s delete summary,
/// removes a row: the rows and delete files a compaction would remove and,
/// where updates overlap, the level a compaction restores
/// (`compacted_level`).
/// Where the costs `deletes_pending` states were measured, named in its
/// message so an operator can read the method and the host behind them.
const MERGE_ON_READ_COSTS: &str = "measured in docs/analysis/2026-10-07-merge-on-read-costs.md";

fn deletes_pending(g: &Generation, caps: &Caps, deletes: DeleteSummary) -> Option<Finding> {
    if deletes.rows == 0 {
        return None;
    }
    let rows = deletes.rows;
    let files = deletes.files;
    let bytes = deletes.state_bytes;
    let restored = compacted_level(g, caps).map_or_else(String::new, |level| {
        format!("; files overlap, and with none overlapping the table reaches {level}")
    });
    let fetched = if g.residency == Residency::Bloom {
        " and fetches it, one GET each"
    } else {
        ""
    };
    Some(Finding::advice(
        FindingCode::DeletesPending,
        None,
        format!(
            "deletes remove {rows} rows through {files} position delete files and deletion vectors, holding {bytes} bytes of delete state{restored}; checking a row against them costs a few thousandths of a key-page decode, so a hit pays nothing for them, but a miss on a deleted key decodes the key page of every file holding a deleted copy{fetched} ({MERGE_ON_READ_COSTS}; a compaction rewrites the data files without the deleted rows and drops the position delete files and deletion vectors)",
        ),
        None,
    ))
}

/// Where `caps`, `g`'s layout caps, hold `overlapping_files`, the level
/// `layout_caps` gives `g` in its residency with no files overlapping,
/// which a compaction that rewrites the overlapping files restores, when it
/// is better than `caps`' level; `None` where no files overlap or another
/// cap holds the level where it is. Shared edge keys stay, as a compaction
/// that rolls files by size writes them again.
fn compacted_level(g: &Generation, caps: &Caps) -> Option<Level> {
    caps.findings
        .iter()
        .any(|finding| finding.code() == FindingCode::OverlappingFiles)
        .then(|| {
            let compacted = ProbeWidths {
                overlapping: BTreeMap::new(),
                shared_edges: g.probe_widths().shared_edges.clone(),
            };
            layout_caps(&LayoutView {
                widths: &compacted,
                ..LayoutView::of(g)
            })
            .level
        })
        .filter(|&compacted| compacted < caps.level)
}

/// The files of `g` written before value column `column`, a position in
/// its layout, was added, for whose rows a lookup returns the column's
/// default (RFD 023).
fn predating_column(g: &Generation, column: usize) -> usize {
    g.files
        .iter()
        .filter(|file| file.index.value_leaf(column).is_none())
        .count()
}

/// `column_default` (RFD 023), one per value column of `g` that has data
/// files written before it was added, naming it: how many files, of how
/// many, and the default a lookup returns for their rows. Such a file
/// costs no value-page GET for a hit requesting the column, so a table mid-evolution
/// costs less than its level and cost gauges predict, which describe the
/// files that hold the column.
fn column_defaults(g: &Generation) -> Vec<Finding> {
    let total = g.files.len();
    g.layout
        .value_columns
        .iter()
        .enumerate()
        .filter_map(|(at, column)| {
            let defaulted = predating_column(g, at);
            if defaulted == 0 {
                return None;
            }
            let name = &column.name;
            let default = column.default.as_ref().map_or_else(
                || "null".to_string(),
                |bytes| format!("its initial-default of {} bytes", bytes.len()),
            );
            Some(
                Finding::advice(
                    FindingCode::ColumnDefault,
                    None,
                    format!(
                        "{defaulted} of {total} data files were written before value column {name} was added, so a lookup returns {default} for their rows and reads no value page; a rewrite of those files, such as a compaction, writes the column into them",
                    ),
                    None,
                )
                .with_column(name),
            )
        })
        .collect()
}

/// `predicted` with each tier's memory grown by `delete_bound`, the bound on
/// the delete state's bytes, which every tier holds in memory beside its
/// index (RFD 021): a `full` table is budgeted for it, and `disk` and
/// `bloom` report it here. Saturates as the sums do.
fn with_deletes(predicted: ResidentBytes, delete_bound: u64) -> ResidentBytes {
    ResidentBytes {
        full: predicted.full.saturating_add(delete_bound),
        disk_memory: predicted.disk_memory.saturating_add(delete_bound),
        bloom: predicted
            .bloom
            .map(|bloom| bloom.saturating_add(delete_bound)),
        ..predicted
    }
}

/// Capping before advice, then the finding table's order, then by file in
/// generation order, which is the order the findings were raised in and
/// the stable sort keeps, so two runs over one generation print the same
/// report.
fn sort_findings(findings: &mut [Finding]) {
    findings.sort_by_key(|finding| (finding.caps().is_none(), finding.code()));
}

/// Every tier's resident bytes for the table, from the footers alone: each
/// file's prediction summed per tier, saturating at `u64::MAX` as each
/// file's does. `bloom` is `None` when any file's is, since the Bloom tier
/// could not load that file, and its bits per key is the ratio of the
/// summed filter bits to the summed rows.
fn predicted_total(facts: &[FileFacts]) -> ResidentBytes {
    let sum = |field: fn(&ResidentBytes) -> u64| {
        saturating_sum(facts.iter().map(|file| field(&file.predicted)))
    };
    let sum_all = |field: fn(&ResidentBytes) -> Option<u64>| {
        saturating_sum_all(facts.iter().map(|file| field(&file.predicted)))
    };
    let filters = sum_all(|predicted| predicted.filter_bytes);
    let rows = saturating_sum(facts.iter().map(|file| file.rows));
    ResidentBytes {
        full: sum(|predicted| predicted.full),
        disk_memory: sum(|predicted| predicted.disk_memory),
        disk_bytes: sum(|predicted| predicted.disk_bytes),
        bloom: sum_all(|predicted| predicted.bloom),
        bloom_bits_per_key: filters.map(|bytes| bits_per_key(bytes, rows)),
        key_chunk_bytes: sum(|predicted| predicted.key_chunk_bytes),
        filter_bytes: filters,
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use serde_json::Value as Json;

    use super::thresholds::{
        BLOOM_BITS_PER_KEY_MIN, BLOOM_BYTES_PER_KEY_ADVICE, DISTINCT_MINS_RATIO_ADVICE,
        FILE_BYTES_ADVICE, FILES_PER_BUCKET_FACTOR, KEY_PAGE_ROWS_LARGE, MOST_VALUE_PAGE_BYTES,
        RUN_ROWS_ADVICE, VALUE_PAGE_ADVICE_FACTOR, VALUE_PAGE_BYTES_ADVICE,
    };
    use super::*;
    use crate::config::TableConfig;
    use crate::error::Error;
    use crate::generation::testing::{
        self, Bloom, Bounds, Dictionary, FILE_KEYS, FIRST_SEQUENCE, FIRST_SNAPSHOT, FIRST_VERSION,
        FileSpec, KEY_STEP, SECOND_SEQUENCE, SECOND_SNAPSHOT, SECOND_VERSION, TABLE, TWO, Table,
        WriterLayout,
    };
    use crate::index::{FileFault, HeaderField};
    use crate::local::LocalDir;
    use crate::manifest::testing::{
        KEY_COLUMN, Partition, VALUE_COLUMN, bucket_spec, unpartitioned,
    };
    use crate::storage::FILE_PREFIX;

    /// A bucket count beside the fixture's `TWO`.
    const ONE: u32 = 1;
    /// Keys in the two-bucket contract fixture, and the fewest either
    /// bucket may get, for the same reason.
    const CONTRACT_KEYS: i64 = 96;
    const MIN_BUCKET_KEYS: usize = 16;
    /// Rows of the large-key-page fixture: one page just above the
    /// threshold where large key pages cap the level.
    const LARGE_PAGE_ROWS: i64 = KEY_PAGE_ROWS_LARGE + 1;
    /// How many advice thresholds the large-value-page fixture's one page
    /// spans: two, so its mean is above the line and not at it.
    const LARGE_VALUE_PAGE_FACTOR: u64 = 2;
    /// Bytes per value of the large-value-page fixture: `FILE_KEYS` of them
    /// make one page of `LARGE_VALUE_PAGE_FACTOR` times the advice threshold.
    const LARGE_VALUE_BYTES: u64 =
        LARGE_VALUE_PAGE_FACTOR * VALUE_PAGE_BYTES_ADVICE / FILE_KEYS.unsigned_abs();
    /// Bytes per value of the over-ceiling fixture: `FILE_KEYS` of them
    /// make one page just over `MOST_VALUE_PAGE_BYTES`.
    const OVER_CEILING_VALUE_BYTES: u64 = MOST_VALUE_PAGE_BYTES / FILE_KEYS.unsigned_abs() + 1;
    /// The over-ceiling fixture's column index truncation: its one page's
    /// minimum and maximum would otherwise be two whole values of megabytes.
    const OVER_CEILING_INDEX_TRUNCATE: usize = 16;
    /// The length of the long-key fixture's keys, just above the advice
    /// length.
    const LONG_KEY_BYTES: usize = contract::KEY_BYTES_ADVICE + 1;
    /// The prefix every key of the string fixtures shares, and the length
    /// the truncated fixtures cut their column index and bounds to, so
    /// every cut minimum and bound is this prefix.
    const KEY_PREFIX: &str = "user:";
    /// Rows per page in the truncated-column-index fixture: eight pages of
    /// `FILE_KEYS`, so the share of distinct minimums can fall to an eighth.
    const TRUNCATED_PAGE_ROWS: usize = 4;
    /// The false-positive rate of the oversized fixture's filter: one in a
    /// trillion takes tens of bytes per key.
    const OVERSIZED_FPP: f64 = 1e-12;
    /// The false-positive rate of the undersized fixture's filter and the
    /// rows of its file: one in two takes a few bits per key, once the
    /// writer's smallest filter is spread over that many rows.
    const UNDERSIZED_FPP: f64 = 0.5;
    const UNDERSIZED_ROWS: i64 = 128;
    /// Files in the many-files fixture: above what one bucket of a few
    /// kilobytes explains.
    const MANY_FILES: u64 = FILES_PER_BUCKET_FACTOR + 1;

    /// The Display of every writer property: the property string a table
    /// owner sets, per-column ones as their prefix.
    const PROPERTIES: &[(WriterProperty, &str)] = &[
        (
            WriterProperty::EnableDictionary,
            "parquet.enable.dictionary",
        ),
        (
            WriterProperty::ColumnIndexTruncateLength,
            "parquet.columnindex.truncate.length",
        ),
        (WriterProperty::PageRowLimit, "write.parquet.page-row-limit"),
        (
            WriterProperty::PageSizeBytes,
            "write.parquet.page-size-bytes",
        ),
        (
            WriterProperty::RowGroupCheckRecordCount,
            "write.parquet.row-group-check-min-record-count and write.parquet.row-group-check-max-record-count",
        ),
        (
            WriterProperty::TargetFileSizeBytes,
            "write.target-file-size-bytes",
        ),
        (
            WriterProperty::MetricsColumn,
            "write.metadata.metrics.column.",
        ),
        (
            WriterProperty::BloomFilterEnabled,
            "write.parquet.bloom-filter-enabled.column.",
        ),
        (
            WriterProperty::BloomFilterMaxBytes,
            "write.parquet.bloom-filter-max-bytes",
        ),
        (
            WriterProperty::BloomFilterNdvColumn,
            "write.parquet.bloom-filter-ndv.column.",
        ),
        (
            WriterProperty::SkewSplitting,
            "spark.sql.adaptive.optimizeSkewsInRebalancePartitions.enabled",
        ),
    ];

    /// The `snake_case` code of every finding, as the doctor's output and
    /// the `code` metric label carry it; the Display and serde tests pin
    /// each, and the order test holds the table's order.
    const CODES: &[(FindingCode, &str)] = &[
        (
            FindingCode::DictionaryEncodedValue,
            "dictionary_encoded_value",
        ),
        (FindingCode::LargeKeyPages, "large_key_pages"),
        (FindingCode::NoBucketPartition, "no_bucket_partition"),
        (FindingCode::OverlappingFiles, "overlapping_files"),
        (FindingCode::SharedEdgeKeys, "shared_edge_keys"),
        (
            FindingCode::DictionaryEncodedOrder,
            "dictionary_encoded_order",
        ),
        (FindingCode::BoundsMissing, "bounds_missing"),
        (FindingCode::ColumnIndexTruncated, "column_index_truncated"),
        (FindingCode::LargeValuePages, "large_value_pages"),
        (
            FindingCode::ValuePagesOverCeiling,
            "value_pages_over_ceiling",
        ),
        (FindingCode::LongRuns, "long_runs"),
        (FindingCode::LargeRuns, "large_runs"),
        (FindingCode::LongKeys, "long_keys"),
        (FindingCode::LargeFile, "large_file"),
        (FindingCode::ManyFilesPerBucket, "many_files_per_bucket"),
        (FindingCode::NoBloomFilter, "no_bloom_filter"),
        (FindingCode::BloomOversized, "bloom_oversized"),
        (FindingCode::BloomUndersized, "bloom_undersized"),
        (FindingCode::DeletesPending, "deletes_pending"),
        (FindingCode::ColumnDefault, "column_default"),
        (FindingCode::MalformedProtobuf, "malformed_protobuf"),
    ];

    /// `rows` values of `len` bytes each, none null.
    fn values_of_len(rows: usize, len: usize) -> Vec<Option<Vec<u8>>> {
        (0..rows)
            .map(|row| Some(testing::value_of_len(i64::try_from(row).unwrap(), len)))
            .collect()
    }

    fn small_values(rows: usize) -> Vec<Option<Vec<u8>>> {
        values_of_len(rows, testing::VALUE_BYTES)
    }

    /// A file of `keys` with small values and exact bounds.
    fn file<'a>(
        name: &'a str,
        keys: &'a [Key],
        partition: Partition,
        layout: WriterLayout,
    ) -> FileSpec<'a> {
        FileSpec {
            name,
            keys,
            values: small_values(keys.len()),
            partition,
            bounds: Bounds::Exact,
            layout,
        }
    }

    /// `count` string keys sharing `KEY_PREFIX`, ascending.
    fn string_keys(count: i64) -> Vec<Key> {
        (0..count)
            .map(|n| Key::Str(format!("{KEY_PREFIX}{n:06}")))
            .collect()
    }

    /// Commits every file added to `table` as `FIRST_SNAPSHOT` and loads it in
    /// `residency`.
    async fn load(
        table: &mut Table,
        residency: Residency,
        local: Option<&Arc<LocalDir>>,
    ) -> Generation {
        table
            .commit(FIRST_SNAPSHOT, None, FIRST_SEQUENCE, FIRST_VERSION)
            .await;
        let config = table.config(residency, None);
        table.load(&config, local, None).await.unwrap()
    }

    /// A one-bucket long-keyed table with one file `f` of `FILE_KEYS` keys
    /// laid out as `layout` says, loaded in the Full tier.
    async fn one_file(layout: WriterLayout) -> (Table, Generation) {
        let mut table = Table::bucketed(ONE);
        let keys = testing::long_keys(&testing::keys_from(0, FILE_KEYS));
        table
            .add_file(file("f", &keys, Partition::Bucket(0), layout))
            .await;
        let generation = load(&mut table, Residency::Full, None).await;
        (table, generation)
    }

    /// A one-bucket string-keyed table with one file `f` of `keys`, its
    /// bounds recorded as `bounds` says, loaded in the Full tier.
    async fn one_string_file(
        keys: &[Key],
        bounds: Bounds,
        layout: WriterLayout,
    ) -> (Table, Generation) {
        let mut table = Table::new(KeyType::String, bucket_spec(ONE));
        table
            .add_file(FileSpec {
                name: "f",
                keys,
                values: small_values(keys.len()),
                partition: Partition::Bucket(0),
                bounds,
                layout,
            })
            .await;
        let generation = load(&mut table, Residency::Full, None).await;
        (table, generation)
    }

    /// Two long-keyed files `a` and `b` with disjoint key runs, `a`'s
    /// below `b`'s, under `partition` in `table`.
    async fn two_disjoint_files(table: &mut Table, partition: Partition, layout: WriterLayout) {
        let first = testing::keys_from(0, FILE_KEYS);
        let second = testing::keys_from(first.last().unwrap() + KEY_STEP, FILE_KEYS);
        for (name, keys) in [("a", &first), ("b", &second)] {
            let typed = testing::long_keys(keys);
            table.add_file(file(name, &typed, partition, layout)).await;
        }
    }

    /// The contract table's files: one per bucket of `TWO`.
    const CONTRACT_FILES: usize = 2;
    /// Rows per row group in the several-row-group fixture; below the page
    /// row limit, so every row group is one key page.
    const ROW_GROUP_ROWS: usize = 4;
    /// Keys above every run `two_disjoint_files` writes, so a file added
    /// after a spec evolution intersects no unbucketed file's range.
    const EVOLVED_KEYS_START: i64 = 1_000_000;
    /// Bits per key just under the minimum, for the hand-built facts.
    const BITS_JUST_UNDER: f64 = 7.9;
    /// Bytes per key just over the advice, for the hand-built facts.
    const BYTES_JUST_OVER: f64 = 4.1;

    /// RFD 002's shape: two buckets, one contract-layout file per bucket
    /// with exact bounds.
    async fn contract_table() -> Table {
        let mut table = Table::bucketed(TWO);
        let all = testing::keys_from(0, CONTRACT_KEYS);
        for bucket in 0..TWO {
            let keys = testing::keys_in_bucket(&all, testing::bucketing(TWO), bucket);
            assert!(
                keys.len() >= MIN_BUCKET_KEYS,
                "bucket {bucket} holds {} keys",
                keys.len()
            );
            table
                .add_long_file(&format!("b{bucket}"), &keys, bucket)
                .await;
        }
        table
    }

    /// A finding as the tests assert it: everything but the message.
    type Shape = (
        FindingCode,
        Option<Level>,
        Option<Penalty>,
        Option<WriterProperty>,
        Option<String>,
    );

    /// The codes of `capping`, in order.
    fn codes(capping: &[Capping]) -> Vec<FindingCode> {
        capping.iter().map(|capping| capping.code).collect()
    }

    fn shapes_of(findings: &[Finding]) -> Vec<Shape> {
        findings
            .iter()
            .map(|finding| {
                (
                    finding.code(),
                    finding.caps(),
                    finding.penalty(),
                    finding.property(),
                    finding.file().map(str::to_string),
                )
            })
            .collect()
    }

    /// Asserts `report` raised exactly `expected`, in order, that a finding
    /// carries a penalty exactly when it caps the level, and that the
    /// summary's counts and level follow them in the loaded tier.
    fn assert_findings(report: &TableReport, expected: &[Shape]) {
        assert_eq!(shapes_of(&report.findings), expected);
        let capping = expected
            .iter()
            .filter(|(_, caps, ..)| caps.is_some())
            .count();
        assert_eq!(report.summary.capping, capping);
        assert_eq!(report.summary.advice, expected.len() - capping);
        let dearest = expected
            .iter()
            .filter_map(|(_, caps, ..)| *caps)
            .fold(report.summary.residency.best_level(), Level::max);
        assert_eq!(report.summary.level, dearest);
        for finding in &report.findings {
            assert_eq!(
                finding.penalty().is_some(),
                finding.caps().is_some(),
                "{:?}",
                finding.code
            );
            let named = finding.code().per_column().then_some(VALUE_COLUMN);
            assert_eq!(finding.column(), named, "{:?}", finding.code);
        }
    }

    #[test]
    fn test_cap_of_every_code_is_the_rfd_table() {
        let capped: Vec<(FindingCode, Level)> = FindingCode::ALL
            .into_iter()
            .filter_map(|code| code.cap().map(|cap| (code, cap)))
            .collect();
        assert_eq!(
            capped,
            [
                (FindingCode::DictionaryEncodedValue, Level::Balanced),
                (FindingCode::LargeKeyPages, Level::Balanced),
                (FindingCode::NoBucketPartition, Level::Permissive),
                (FindingCode::OverlappingFiles, Level::Permissive),
                (FindingCode::SharedEdgeKeys, Level::Balanced),
                (FindingCode::DictionaryEncodedOrder, Level::Balanced),
            ]
        );
    }

    #[test]
    fn test_thresholds_derive_from_contract() {
        assert_eq!(KEY_PAGE_ROWS_LARGE, 2 * contract::PAGE_ROWS);
        assert_eq!(
            VALUE_PAGE_BYTES_ADVICE,
            VALUE_PAGE_ADVICE_FACTOR * contract::VALUE_PAGE_BYTES
        );
        assert_eq!(FILE_BYTES_ADVICE, 2 * contract::TARGET_FILE_BYTES_MAX);
        const { assert!(FILES_PER_BUCKET_FACTOR > MIN_EXPLAINED_FILES) };
        // A filter cannot be oversized in bytes and undersized in bits at once.
        assert!(
            BLOOM_BYTES_PER_KEY_ADVICE * approx::gauge(u64::from(u8::BITS))
                > BLOOM_BITS_PER_KEY_MIN
        );
        const { assert!(DISTINCT_MINS_RATIO_ADVICE < DISTINCT_MINS_UNOBSERVABLE) };
        assert_eq!(RUN_ROWS_ADVICE, crate::config::DEFAULT_MAX_SCAN_ROWS);
    }

    /// A cold Scan of a key in one file and one row group (RFD 025, Cost):
    /// under `full` the order column's span and each value column's, under
    /// `disk` the value columns' alone, under `bloom` the run's two edge key
    /// pages first; a dictionary page adds one GET where it is fetched, the
    /// order column's only under `full` and `bloom`, where
    /// `dictionary_encoded_order` is raised.
    #[test]
    fn test_scan_gets_per_tier_counts_spans_dictionaries_and_edge_key_pages() {
        let columns = [
            ColumnCost {
                level: Level::Optimal,
                gets_per_hit: VALUE_PAGE_GETS,
            },
            ColumnCost {
                level: Level::Balanced,
                gets_per_hit: VALUE_PAGE_GETS + DICTIONARY_PAGE_GETS,
            },
        ];
        let values = 2 * VALUE_PAGE_GETS + DICTIONARY_PAGE_GETS;
        let dictionary_order = [Finding::cost(
            FindingCode::DictionaryEncodedOrder,
            None,
            String::new(),
            Penalty::default(),
            None,
        )];
        let row = |residency: Residency, key_dictionary: u64| RowGets {
            key_page: match residency {
                Residency::Bloom => KEY_PAGE_GETS,
                Residency::Full | Residency::Disk => 0,
            },
            key_dictionary,
            files_probed: FILES_PROBED_ALONE,
        };
        let cases = [
            (Residency::Full, 0, &[][..], ORDER_SPAN_GETS + values),
            (
                Residency::Full,
                0,
                &dictionary_order[..],
                ORDER_SPAN_GETS + DICTIONARY_PAGE_GETS + values,
            ),
            (Residency::Disk, 0, &[][..], values),
            (
                Residency::Bloom,
                0,
                &[][..],
                RUN_EDGE_KEY_PAGES * KEY_PAGE_GETS + ORDER_SPAN_GETS + values,
            ),
            (
                Residency::Bloom,
                DICTIONARY_PAGE_GETS,
                &dictionary_order[..],
                RUN_EDGE_KEY_PAGES * KEY_PAGE_GETS
                    + 2 * DICTIONARY_PAGE_GETS
                    + ORDER_SPAN_GETS
                    + values,
            ),
        ];
        for (residency, key_dictionary, findings, expected) in cases {
            assert_eq!(
                scan_gets(
                    residency,
                    &row(residency, key_dictionary),
                    &columns,
                    findings
                ),
                expected,
                "{residency}"
            );
        }
    }

    #[test]
    fn test_fmt_finding_code_is_the_code_column() {
        for &(code, name) in CODES {
            assert_eq!(code.to_string(), name);
        }
    }

    #[test]
    fn test_serialize_finding_code_matches_display() {
        for &(code, name) in CODES {
            let json = serde_json::to_string(&code).unwrap();
            assert_eq!(json, format!("\"{name}\""));
            let back: FindingCode = serde_json::from_str(&json).unwrap();
            assert_eq!(back, code);
        }
    }

    #[test]
    fn test_finding_code_order_is_the_finding_table() {
        assert!(CODES.windows(2).all(|pair| pair[0].0 < pair[1].0));
    }

    /// The code after `code` in the finding table, `None` after the last.
    /// The match names every variant, so a variant added to the enum fails
    /// to compile here until it is placed in the chain, and the chain is
    /// what `ALL` is checked against.
    fn after(code: FindingCode) -> Option<FindingCode> {
        match code {
            FindingCode::DictionaryEncodedValue => Some(FindingCode::LargeKeyPages),
            FindingCode::LargeKeyPages => Some(FindingCode::NoBucketPartition),
            FindingCode::NoBucketPartition => Some(FindingCode::OverlappingFiles),
            FindingCode::OverlappingFiles => Some(FindingCode::SharedEdgeKeys),
            FindingCode::SharedEdgeKeys => Some(FindingCode::DictionaryEncodedOrder),
            FindingCode::DictionaryEncodedOrder => Some(FindingCode::BoundsMissing),
            FindingCode::BoundsMissing => Some(FindingCode::ColumnIndexTruncated),
            FindingCode::ColumnIndexTruncated => Some(FindingCode::LargeValuePages),
            FindingCode::LargeValuePages => Some(FindingCode::ValuePagesOverCeiling),
            FindingCode::ValuePagesOverCeiling => Some(FindingCode::LongRuns),
            FindingCode::LongRuns => Some(FindingCode::LargeRuns),
            FindingCode::LargeRuns => Some(FindingCode::LongKeys),
            FindingCode::LongKeys => Some(FindingCode::LargeFile),
            FindingCode::LargeFile => Some(FindingCode::ManyFilesPerBucket),
            FindingCode::ManyFilesPerBucket => Some(FindingCode::NoBloomFilter),
            FindingCode::NoBloomFilter => Some(FindingCode::BloomOversized),
            FindingCode::BloomOversized => Some(FindingCode::BloomUndersized),
            FindingCode::BloomUndersized => Some(FindingCode::DeletesPending),
            FindingCode::DeletesPending => Some(FindingCode::ColumnDefault),
            FindingCode::ColumnDefault => Some(FindingCode::MalformedProtobuf),
            FindingCode::MalformedProtobuf => None,
        }
    }

    #[test]
    fn test_all_lists_every_code_once_in_table_order() {
        let mut chain = vec![FindingCode::ALL[0]];
        while let Some(next) = after(*chain.last().unwrap()) {
            chain.push(next);
        }
        assert_eq!(chain, FindingCode::ALL);
        assert!(FindingCode::ALL.windows(2).all(|pair| pair[0] < pair[1]));
        assert_eq!(
            FindingCode::ALL.to_vec(),
            CODES.iter().map(|(code, _)| *code).collect::<Vec<_>>()
        );
    }

    #[test]
    fn test_fmt_writer_property_is_the_property_string() {
        for &(property, name) in PROPERTIES {
            assert_eq!(property.to_string(), name);
        }
    }

    #[test]
    fn test_serialize_writer_property_matches_display() {
        for &(property, _) in PROPERTIES {
            assert_eq!(
                serde_json::to_value(property).unwrap(),
                Json::String(property.to_string())
            );
        }
    }

    #[test]
    fn test_bounds_intact_requires_both_bounds_at_least_as_long_as_the_exact_keys() {
        let keys = string_keys(FILE_KEYS);
        let (first, last) = (&keys[0], keys.last().unwrap());
        let entry = |key_min: Option<Key>, key_max: Option<Key>| DataFileEntry {
            path: String::new(),
            bucket: 0,
            bucketing: None,
            records: 0,
            sequence: crate::snapshot::SequenceNumber(0),
            added_by: crate::snapshot::SnapshotId(0),
            size: 0,
            key_min,
            key_max,
            partition: crate::manifest::Partition::unpartitioned(),
        };
        assert!(bounds_intact(
            &entry(Some(first.clone()), Some(last.clone())),
            (first, last)
        ));
        assert!(!bounds_intact(
            &entry(None, Some(last.clone())),
            (first, last)
        ));
        assert!(!bounds_intact(
            &entry(Some(first.clone()), None),
            (first, last)
        ));
        let cut = Key::Str(KEY_PREFIX.to_string());
        assert!(!bounds_intact(
            &entry(Some(cut.clone()), Some(last.clone())),
            (first, last)
        ));
        assert!(!bounds_intact(
            &entry(Some(first.clone()), Some(cut)),
            (first, last)
        ));
        let (low, high) = (Key::Long(0), Key::Long(FILE_KEYS));
        assert!(bounds_intact(
            &entry(Some(low.clone()), Some(high.clone())),
            (&low, &high)
        ));
    }

    #[tokio::test]
    async fn test_diagnose_contract_fixture_is_optimal_with_no_findings() {
        let mut table = contract_table().await;
        let generation = load(&mut table, Residency::Full, None).await;
        let report = diagnose(&generation, Level::Optimal, &ValueFormat::opaque, None);
        assert_findings(&report, &[]);
        let summary = &report.summary;
        assert_eq!(summary.level, Level::Optimal);
        assert_eq!(summary.table, TABLE);
        assert_eq!(summary.snapshot_id, SnapshotId(FIRST_SNAPSHOT));
        assert_eq!(summary.residency, Residency::Full);
        assert_eq!(summary.file_count, CONTRACT_FILES);
        assert_eq!(summary.buckets, Some(TWO));
        assert_eq!(summary.buckets_with_files, CONTRACT_FILES);
        assert_eq!(summary.max_files_per_bucket, 1);
        assert_eq!(summary.records, u64::try_from(CONTRACT_KEYS).unwrap());
        assert_eq!(summary.index_bytes, generation.index_bytes);
        assert_eq!(report.files.len(), CONTRACT_FILES);
        for (file, loaded) in report.files.iter().zip(&generation.files) {
            assert_eq!(file.path, loaded.entry.path);
            assert_eq!(file.bucket, loaded.entry.bucket);
            assert_eq!(file.key_type, KeyType::Long);
            assert_eq!(file.rows, loaded.index.rows());
            assert_eq!(file.row_groups, 1);
            assert_eq!(file.key_pages, 1);
            let [value] = &file.value_columns[..] else {
                panic!("one value column");
            };
            assert_eq!(value.column, VALUE_COLUMN);
            assert!(!value.predates_column);
            assert_eq!(value.value_pages, 1);
            assert_eq!(file.max_key_page_rows, loaded.index.rows());
            assert!(file.max_key_page_rows <= contract::PAGE_ROWS);
            assert_eq!(file.max_key_bytes, size_of::<i64>());
            assert!(value.avg_value_page_bytes <= VALUE_PAGE_BYTES_ADVICE);
            assert!(value.pages_aligned);
            assert!(file.dictionary_columns.is_empty());
            assert!(file.bloom_bytes.is_some());
            assert!(file.bloom_bits_per_key.unwrap() >= BLOOM_BITS_PER_KEY_MIN);
            assert!(file.key_chunk_bytes > 0);
            assert_eq!(
                file.distinct_page_mins_ratio.to_bits(),
                DISTINCT_MINS_UNOBSERVABLE.to_bits()
            );
            assert!(file.bounds_present);
            assert_eq!(file.resident_bytes, loaded.index.resident_bytes());
        }
    }

    #[tokio::test]
    async fn test_diagnose_dictionary_encoded_value_caps_balanced_by_one_extra_get() {
        let layout = WriterLayout {
            dictionary: Dictionary::Value,
            ..WriterLayout::contract()
        };
        let (table, generation) = one_file(layout).await;
        let report = diagnose(&generation, Level::Optimal, &ValueFormat::opaque, None);
        assert_findings(
            &report,
            &[(
                FindingCode::DictionaryEncodedValue,
                Some(Level::Balanced),
                Some(Penalty {
                    extra_gets_per_hit: Some(DICTIONARY_PAGE_GETS),
                    ..Penalty::default()
                }),
                Some(WriterProperty::EnableDictionary),
                Some(table.path_of("f")),
            )],
        );
        assert_eq!(report.files[0].dictionary_columns, [VALUE_COLUMN]);
    }

    #[tokio::test]
    async fn test_diagnose_dictionary_encoded_key_is_reported_and_not_a_finding() {
        let layout = WriterLayout {
            dictionary: Dictionary::Key,
            ..WriterLayout::contract()
        };
        let (_table, generation) = one_file(layout).await;
        let report = diagnose(&generation, Level::Optimal, &ValueFormat::opaque, None);
        assert_findings(&report, &[]);
        assert_eq!(report.files[0].dictionary_columns, [KEY_COLUMN]);
    }

    #[tokio::test]
    async fn test_diagnose_large_key_pages_penalty_is_the_rows_decoded() {
        let mut table = Table::bucketed(ONE);
        let keys = testing::long_keys(&testing::keys_from(0, LARGE_PAGE_ROWS));
        let layout = WriterLayout {
            page_rows: usize::try_from(LARGE_PAGE_ROWS).unwrap(),
            ..WriterLayout::contract()
        };
        table
            .add_file(file("f", &keys, Partition::Bucket(0), layout))
            .await;
        let report = diagnose(
            &load(&mut table, Residency::Full, None).await,
            Level::Optimal,
            &ValueFormat::opaque,
            None,
        );
        assert_findings(
            &report,
            &[(
                FindingCode::LargeKeyPages,
                Some(Level::Balanced),
                Some(Penalty {
                    keys_decoded_per_lookup: Some(LARGE_PAGE_ROWS),
                    ..Penalty::default()
                }),
                Some(WriterProperty::PageRowLimit),
                Some(table.path_of("f")),
            )],
        );
        assert_eq!(report.files[0].key_pages, 1);
        assert_eq!(report.files[0].max_key_page_rows, LARGE_PAGE_ROWS);
    }

    #[tokio::test]
    async fn test_diagnose_no_bucket_partition_penalty_is_every_file_probed() {
        let mut table = Table::new(KeyType::Long, unpartitioned());
        two_disjoint_files(&mut table, Partition::None, WriterLayout::contract()).await;
        let report = diagnose(
            &load(&mut table, Residency::Full, None).await,
            Level::Optimal,
            &ValueFormat::opaque,
            None,
        );
        assert_findings(
            &report,
            &[(
                FindingCode::NoBucketPartition,
                Some(Level::Permissive),
                Some(Penalty::files_probed(2)),
                None,
                None,
            )],
        );
        assert_eq!(report.summary.buckets, None);
        assert_eq!(report.summary.buckets_with_files, 1);
        assert_eq!(report.summary.max_files_per_bucket, 2);
    }

    #[tokio::test]
    async fn test_diagnose_no_bucket_partition_counts_the_files_every_lookup_probes() {
        let mut table = Table::new(KeyType::Long, unpartitioned());
        two_disjoint_files(&mut table, Partition::None, WriterLayout::contract()).await;
        table.evolve(TWO);
        // Bucket 1, so the bucketed file does not share bucket zero's number
        // with the unbucketed files in `many_files_per_bucket`'s count.
        let in_bucket_one: Vec<i64> = testing::keys_from(EVOLVED_KEYS_START, FILE_KEYS)
            .into_iter()
            .filter(|key| crate::bucket::bucket(&Key::Long(*key), TWO) == 1)
            .collect();
        assert!(!in_bucket_one.is_empty());
        table.add_long_file("g1", &in_bucket_one, 1).await;
        let report = diagnose(
            &load(&mut table, Residency::Full, None).await,
            Level::Optimal,
            &ValueFormat::opaque,
            None,
        );
        assert_findings(
            &report,
            &[(
                FindingCode::NoBucketPartition,
                Some(Level::Permissive),
                Some(Penalty::files_probed(3)),
                None,
                None,
            )],
        );
        assert_eq!(report.summary.buckets, Some(TWO), "the default spec's");
        assert_eq!(report.summary.file_count, 3);
    }

    #[tokio::test]
    async fn test_measure_several_row_groups_sizes_bloom_filters_per_row_group() {
        let layout = WriterLayout {
            row_group_rows: Some(ROW_GROUP_ROWS),
            ..WriterLayout::contract()
        };
        let (_table, generation) = one_file(layout).await;
        let facts = measure(&generation.files[0], &generation.layout);
        let groups = usize::try_from(FILE_KEYS).unwrap().div_ceil(ROW_GROUP_ROWS);
        assert!(groups > 1, "the fixture spans several row groups");
        assert_eq!(facts.report.row_groups, groups);
        assert_eq!(facts.report.key_pages, groups, "one page per row group");
        let extremes = facts.bloom_by_row_group.unwrap();
        let file_bits = facts.report.bloom_bits_per_key.unwrap();
        let file_bytes = approx::ratio(facts.report.bloom_bytes.unwrap(), facts.rows);
        assert!(extremes.fewest_bits_per_key.0 <= file_bits);
        assert!(extremes.most_bytes_per_key.0 >= file_bytes);
        assert!(extremes.fewest_bits_per_key.1 < groups);
        assert!(extremes.most_bytes_per_key.1 < groups);
    }

    #[tokio::test]
    async fn test_predicted_total_saturates_as_each_file_does() {
        let (_table, generation) = one_file(WriterLayout::contract()).await;
        let saturated = || {
            let mut facts = measure(&generation.files[0], &generation.layout);
            facts.rows = u64::MAX;
            facts.predicted = ResidentBytes {
                full: u64::MAX,
                disk_memory: u64::MAX,
                disk_bytes: u64::MAX,
                bloom: Some(u64::MAX),
                bloom_bits_per_key: None,
                key_chunk_bytes: u64::MAX,
                filter_bytes: Some(u64::MAX),
            };
            facts
        };
        let total = predicted_total(&[saturated(), saturated()]);
        assert_eq!(
            total,
            ResidentBytes {
                full: u64::MAX,
                disk_memory: u64::MAX,
                disk_bytes: u64::MAX,
                bloom: Some(u64::MAX),
                bloom_bits_per_key: Some(bits_per_key(u64::MAX, u64::MAX)),
                key_chunk_bytes: u64::MAX,
                filter_bytes: Some(u64::MAX),
            }
        );
    }

    #[tokio::test]
    async fn test_bloom_findings_judge_the_worst_row_group_not_the_file() {
        let (_table, generation) = one_file(WriterLayout::contract()).await;
        let mut facts = measure(&generation.files[0], &generation.layout);
        let healthy = facts.bloom_by_row_group.unwrap();
        assert!(bloom_shapes(&facts, &generation.layout).is_empty());
        facts.bloom_by_row_group = Some(BloomExtremes {
            fewest_bits_per_key: (BITS_JUST_UNDER, 1),
            ..healthy
        });
        assert_eq!(
            bloom_shapes(&facts, &generation.layout),
            [(
                FindingCode::BloomUndersized,
                None,
                None,
                Some(WriterProperty::BloomFilterNdvColumn),
                Some(facts.report.path.clone()),
            )],
            "the file's own bits per key are healthy; the row group decides"
        );
        facts.bloom_by_row_group = Some(BloomExtremes {
            most_bytes_per_key: (BYTES_JUST_OVER, 0),
            ..healthy
        });
        assert_eq!(
            bloom_shapes(&facts, &generation.layout),
            [(
                FindingCode::BloomOversized,
                None,
                None,
                Some(WriterProperty::BloomFilterMaxBytes),
                Some(facts.report.path.clone()),
            )]
        );
    }

    /// The shapes `bloom_findings` raises for `facts`.
    fn bloom_shapes(facts: &FileFacts, layout: &TableLayout) -> Vec<Shape> {
        let mut findings = Vec::new();
        bloom_findings(facts, layout, &mut findings);
        shapes_of(&findings)
    }

    #[tokio::test]
    async fn test_diagnose_overlapping_files_penalty_is_the_files_a_lookup_probes() {
        let mut table = Table::bucketed(ONE);
        let first = testing::keys_from(0, FILE_KEYS);
        let interleaved: Vec<i64> = first.iter().map(|key| key + 1).collect();
        table.add_long_file("a", &first, 0).await;
        table.add_long_file("b", &interleaved, 0).await;
        let generation = load(&mut table, Residency::Full, None).await;
        assert_eq!(generation.overlapping_buckets().len(), 1);
        let report = diagnose(&generation, Level::Optimal, &ValueFormat::opaque, None);
        assert_findings(
            &report,
            &[(
                FindingCode::OverlappingFiles,
                Some(Level::Permissive),
                Some(Penalty::files_probed(2)),
                Some(WriterProperty::SkewSplitting),
                None,
            )],
        );
    }

    #[tokio::test]
    async fn test_diagnose_bounds_missing_is_advice_naming_the_metrics_property() {
        let mut table = Table::bucketed(ONE);
        let keys = testing::long_keys(&testing::keys_from(0, FILE_KEYS));
        table
            .add_file(FileSpec {
                bounds: Bounds::Missing,
                ..file("f", &keys, Partition::Bucket(0), WriterLayout::contract())
            })
            .await;
        let generation = load(&mut table, Residency::Full, None).await;
        assert_eq!(generation.files[0].entry.key_min, None);
        let report = diagnose(&generation, Level::Optimal, &ValueFormat::opaque, None);
        assert_findings(
            &report,
            &[(
                FindingCode::BoundsMissing,
                None,
                None,
                Some(WriterProperty::MetricsColumn),
                Some(table.path_of("f")),
            )],
        );
        assert!(!report.files[0].bounds_present);
    }

    #[tokio::test]
    async fn test_diagnose_truncated_bounds_are_bounds_missing() {
        let keys = string_keys(FILE_KEYS);
        let (table, generation) = one_string_file(
            &keys,
            Bounds::Truncated(KEY_PREFIX.len()),
            WriterLayout::contract(),
        )
        .await;
        let entry = &generation.files[0].entry;
        assert!(entry.key_min.is_some() && entry.key_max.is_some());
        assert_eq!(generation.layout.key_type, KeyType::String);
        let report = diagnose(&generation, Level::Optimal, &ValueFormat::opaque, None);
        assert_findings(
            &report,
            &[(
                FindingCode::BoundsMissing,
                None,
                None,
                Some(WriterProperty::MetricsColumn),
                Some(table.path_of("f")),
            )],
        );
        assert!(!report.files[0].bounds_present);
    }

    #[tokio::test]
    async fn test_diagnose_column_index_truncated_is_advice() {
        let keys = string_keys(FILE_KEYS);
        let pages = usize::try_from(FILE_KEYS).unwrap() / TRUNCATED_PAGE_ROWS;
        let small_pages = WriterLayout {
            page_rows: TRUNCATED_PAGE_ROWS,
            ..WriterLayout::contract()
        };
        let (_table, whole) = one_string_file(&keys, Bounds::Exact, small_pages).await;
        let report = diagnose(&whole, Level::Optimal, &ValueFormat::opaque, None);
        assert_findings(&report, &[]);
        assert_eq!(report.files[0].key_pages, pages);
        assert_eq!(
            report.files[0].distinct_page_mins_ratio.to_bits(),
            DISTINCT_MINS_UNOBSERVABLE.to_bits()
        );

        let truncated = WriterLayout {
            column_index_truncate: Some(KEY_PREFIX.len()),
            ..small_pages
        };
        let (table, generation) = one_string_file(&keys, Bounds::Exact, truncated).await;
        let report = diagnose(&generation, Level::Optimal, &ValueFormat::opaque, None);
        assert_findings(
            &report,
            &[(
                FindingCode::ColumnIndexTruncated,
                None,
                None,
                Some(WriterProperty::ColumnIndexTruncateLength),
                Some(table.path_of("f")),
            )],
        );
        assert_eq!(report.files[0].key_pages, pages);
        assert_eq!(
            report.files[0].distinct_page_mins_ratio.to_bits(),
            approx::ratio(1, approx::count(pages)).to_bits(),
            "every cut minimum is the shared prefix"
        );
    }

    #[tokio::test]
    async fn test_distinct_mins_ratio_without_a_column_index_is_unobservable() {
        let (_table, generation) = one_file(WriterLayout::contract()).await;
        let index = &generation.files[0].index;
        let stripped = ParquetMetaData::clone(index.metadata())
            .into_builder()
            .set_column_index(None)
            .build();
        assert!(stripped.column_index().is_none());
        assert_eq!(
            distinct_mins_ratio(&stripped, index.key_leaf(), index.fences()).to_bits(),
            DISTINCT_MINS_UNOBSERVABLE.to_bits()
        );
    }

    #[tokio::test]
    async fn test_diagnose_large_value_pages_is_advice() {
        let mut table = Table::bucketed(ONE);
        let keys = testing::long_keys(&testing::keys_from(0, FILE_KEYS));
        table
            .add_file(FileSpec {
                values: values_of_len(keys.len(), approx::length(LARGE_VALUE_BYTES)),
                ..file("f", &keys, Partition::Bucket(0), WriterLayout::contract())
            })
            .await;
        let report = diagnose(
            &load(&mut table, Residency::Full, None).await,
            Level::Optimal,
            &ValueFormat::opaque,
            None,
        );
        assert_findings(
            &report,
            &[(
                FindingCode::LargeValuePages,
                None,
                None,
                Some(WriterProperty::PageSizeBytes),
                Some(table.path_of("f")),
            )],
        );
        assert_eq!(report.files[0].value_columns[0].value_pages, 1);
        assert!(report.files[0].value_columns[0].avg_value_page_bytes > VALUE_PAGE_BYTES_ADVICE);
    }

    /// The bytes of the value column's chunks over a file's row groups, read
    /// from the footer.
    fn uncompressed_chunk_bytes(file: &LoadedFile) -> u64 {
        let leaf = file.index.value_leaf(0).unwrap();
        file.index
            .metadata()
            .row_groups()
            .iter()
            .map(|group| approx::measured(group.column(leaf).uncompressed_size()))
            .sum()
    }

    #[tokio::test]
    async fn test_column_summary_sums_the_value_bytes_of_every_file() {
        let small: &[u8] = &[0x07; testing::VALUE_BYTES];
        let growth = 3;
        let large: &[u8] = &vec![0x07; testing::VALUE_BYTES * growth];
        let (_table, generation) = files_of_values(&[Some(small), Some(large)]).await;
        let report = diagnose(&generation, Level::Optimal, &ValueFormat::opaque, None);
        let per_file: Vec<u64> = generation
            .files
            .iter()
            .map(|file| uncompressed_chunk_bytes(file))
            .collect();
        assert_ne!(
            per_file[0], per_file[1],
            "the files' columns differ in size"
        );
        assert_eq!(
            report.summary.value_columns[0].value_bytes,
            per_file.iter().sum::<u64>(),
            "the snapshot's column is every file's uncompressed chunk bytes"
        );
        let mean_of_first = report.files[0].value_columns[0].avg_value_bytes;
        assert_eq!(
            mean_of_first,
            per_file[0] / u64::try_from(FILE_KEYS).unwrap(),
            "the sum is the bytes avg_value_bytes divides"
        );
    }

    /// A value page just over the lookup ceiling draws `value_pages_over_ceiling`
    /// beside `large_value_pages`, and a lookup into it is refused with the
    /// file fault the message names.
    #[tokio::test]
    async fn test_diagnose_value_pages_over_the_lookup_ceiling_is_advice() {
        let mut table = Table::bucketed(ONE);
        let keys = testing::long_keys(&testing::keys_from(0, FILE_KEYS));
        let layout = WriterLayout {
            column_index_truncate: Some(OVER_CEILING_INDEX_TRUNCATE),
            ..WriterLayout::contract()
        };
        table
            .add_file(FileSpec {
                values: values_of_len(keys.len(), approx::length(OVER_CEILING_VALUE_BYTES)),
                ..file("f", &keys, Partition::Bucket(0), layout)
            })
            .await;
        let generation = load(&mut table, Residency::Full, None).await;
        let report = diagnose(&generation, Level::Optimal, &ValueFormat::opaque, None);
        let path = Some(table.path_of("f"));
        assert_findings(
            &report,
            &[
                (
                    FindingCode::LargeValuePages,
                    None,
                    None,
                    Some(WriterProperty::PageSizeBytes),
                    path.clone(),
                ),
                (
                    FindingCode::ValuePagesOverCeiling,
                    None,
                    None,
                    Some(WriterProperty::PageSizeBytes),
                    path,
                ),
            ],
        );
        assert_eq!(report.files[0].value_columns[0].value_pages, 1);
        assert!(report.files[0].value_columns[0].avg_value_page_bytes > MOST_VALUE_PAGE_BYTES);
        let error = generation
            .lookup_first(&keys[0], &testing::cache().await)
            .await
            .unwrap_err();
        assert!(
            matches!(
                error,
                Error::File {
                    fault: FileFault::PageHeaderOverBound {
                        field: HeaderField::UncompressedPageSize,
                        allowed: MOST_VALUE_PAGE_BYTES,
                        ..
                    },
                    ..
                }
            ),
            "{error:?}"
        );
    }

    #[tokio::test]
    async fn test_diagnose_long_keys_is_advice_without_a_property() {
        let keys: Vec<Key> = (0..FILE_KEYS)
            .map(|n| Key::Str(format!("{n:0>LONG_KEY_BYTES$}")))
            .collect();
        let (table, generation) =
            one_string_file(&keys, Bounds::Exact, WriterLayout::contract()).await;
        let report = diagnose(&generation, Level::Optimal, &ValueFormat::opaque, None);
        assert_findings(
            &report,
            &[(
                FindingCode::LongKeys,
                None,
                None,
                None,
                Some(table.path_of("f")),
            )],
        );
        assert_eq!(report.files[0].max_key_bytes, LONG_KEY_BYTES);
    }

    #[tokio::test]
    async fn test_file_advice_findings_large_file_above_the_advice_size() {
        let (_table, generation) = one_file(WriterLayout::contract()).await;
        let mut facts = measure(&generation.files[0], &generation.layout);
        assert!(file_advice_findings(&facts, &generation.layout, None).is_empty());
        facts.size = FILE_BYTES_ADVICE;
        assert!(
            file_advice_findings(&facts, &generation.layout, None).is_empty(),
            "the threshold itself passes"
        );
        facts.size = FILE_BYTES_ADVICE + 1;
        assert_eq!(
            shapes_of(&file_advice_findings(&facts, &generation.layout, None)),
            [(
                FindingCode::LargeFile,
                None,
                None,
                Some(WriterProperty::TargetFileSizeBytes),
                Some(facts.report.path.clone()),
            )]
        );
    }

    #[tokio::test]
    async fn test_diagnose_many_files_per_bucket_is_advice() {
        let mut table = Table::bucketed(ONE);
        let mut from = 0;
        for at in 0..MANY_FILES {
            let keys = testing::keys_from(from, FILE_KEYS);
            from = keys.last().unwrap() + KEY_STEP;
            table.add_long_file(&format!("f{at}"), &keys, 0).await;
        }
        let generation = load(&mut table, Residency::Full, None).await;
        assert_eq!(generation.overlapping_buckets().len(), 0);
        let report = diagnose(&generation, Level::Optimal, &ValueFormat::opaque, None);
        assert_findings(
            &report,
            &[(FindingCode::ManyFilesPerBucket, None, None, None, None)],
        );
        assert_eq!(report.summary.buckets_with_files, 1);
        assert_eq!(
            report.summary.max_files_per_bucket,
            approx::length(MANY_FILES)
        );
    }

    #[tokio::test]
    async fn test_diagnose_no_bloom_filter_is_advice() {
        let layout = WriterLayout {
            bloom: Bloom::Off,
            ..WriterLayout::contract()
        };
        let (table, generation) = one_file(layout).await;
        let report = diagnose(&generation, Level::Optimal, &ValueFormat::opaque, None);
        assert_findings(
            &report,
            &[(
                FindingCode::NoBloomFilter,
                None,
                None,
                Some(WriterProperty::BloomFilterEnabled),
                Some(table.path_of("f")),
            )],
        );
        assert_eq!(report.files[0].bloom_bytes, None);
        assert_eq!(report.files[0].bloom_bits_per_key, None);
        assert_eq!(report.summary.predicted.bloom, None);
        assert_eq!(report.summary.predicted.bloom_bits_per_key, None);
    }

    /// The order report of a file whose longest run is `longest_run`.
    fn order_report(longest_run: u64) -> FileOrderReport {
        FileOrderReport {
            min: OrderValue::Long(0),
            max: OrderValue::Long(1),
            longest_run,
            distinct_keys: vec![1],
        }
    }

    #[test]
    fn test_longest_run_of_is_the_maximum_over_files_in_any_order() {
        let (long, short) = (7, 3);
        let both = [order_report(long), order_report(short)];
        assert_eq!(longest_run_of(&both), long);
        assert_eq!(longest_run_of(both.iter().rev()), long);
        assert_eq!(longest_run_of(&[order_report(short)]), short);
    }

    #[test]
    fn test_longest_run_of_no_files_is_zero() {
        assert_eq!(longest_run_of(&[]), 0);
    }

    #[tokio::test]
    async fn test_diagnose_bloom_oversized_is_advice() {
        let layout = WriterLayout {
            bloom: Bloom::Fpp(OVERSIZED_FPP),
            ..WriterLayout::contract()
        };
        let (table, generation) = one_file(layout).await;
        let report = diagnose(&generation, Level::Optimal, &ValueFormat::opaque, None);
        assert_findings(
            &report,
            &[(
                FindingCode::BloomOversized,
                None,
                None,
                Some(WriterProperty::BloomFilterMaxBytes),
                Some(table.path_of("f")),
            )],
        );
        let file = &report.files[0];
        let rows = u64::try_from(file.rows).unwrap();
        assert!(approx::ratio(file.bloom_bytes.unwrap(), rows) > BLOOM_BYTES_PER_KEY_ADVICE);
    }

    #[tokio::test]
    async fn test_diagnose_bloom_undersized_is_advice() {
        let mut table = Table::bucketed(ONE);
        let keys = testing::long_keys(&testing::keys_from(0, UNDERSIZED_ROWS));
        let layout = WriterLayout {
            bloom: Bloom::Fpp(UNDERSIZED_FPP),
            ..WriterLayout::contract()
        };
        table
            .add_file(file("f", &keys, Partition::Bucket(0), layout))
            .await;
        let report = diagnose(
            &load(&mut table, Residency::Full, None).await,
            Level::Optimal,
            &ValueFormat::opaque,
            None,
        );
        assert_findings(
            &report,
            &[(
                FindingCode::BloomUndersized,
                None,
                None,
                Some(WriterProperty::BloomFilterNdvColumn),
                Some(table.path_of("f")),
            )],
        );
        assert!(report.files[0].bloom_bits_per_key.unwrap() < BLOOM_BITS_PER_KEY_MIN);
    }

    #[tokio::test]
    async fn test_diagnose_orders_capping_before_advice_then_code_then_file() {
        let mut table = Table::new(KeyType::Long, unpartitioned());
        let layout = WriterLayout {
            dictionary: Dictionary::Value,
            bloom: Bloom::Off,
            ..WriterLayout::contract()
        };
        two_disjoint_files(&mut table, Partition::None, layout).await;
        let generation = load(&mut table, Residency::Full, None).await;
        let (a, b) = (table.path_of("a"), table.path_of("b"));
        assert_eq!(
            generation
                .files
                .iter()
                .map(|file| file.entry.path.clone())
                .collect::<Vec<_>>(),
            [a.clone(), b.clone()]
        );
        let report = diagnose(&generation, Level::Optimal, &ValueFormat::opaque, None);
        let dictionary = Some(Penalty {
            extra_gets_per_hit: Some(DICTIONARY_PAGE_GETS),
            ..Penalty::default()
        });
        assert_findings(
            &report,
            &[
                (
                    FindingCode::DictionaryEncodedValue,
                    Some(Level::Balanced),
                    dictionary,
                    Some(WriterProperty::EnableDictionary),
                    Some(a.clone()),
                ),
                (
                    FindingCode::DictionaryEncodedValue,
                    Some(Level::Balanced),
                    dictionary,
                    Some(WriterProperty::EnableDictionary),
                    Some(b.clone()),
                ),
                (
                    FindingCode::NoBucketPartition,
                    Some(Level::Permissive),
                    Some(Penalty::files_probed(2)),
                    None,
                    None,
                ),
                (
                    FindingCode::NoBloomFilter,
                    None,
                    None,
                    Some(WriterProperty::BloomFilterEnabled),
                    Some(a),
                ),
                (
                    FindingCode::NoBloomFilter,
                    None,
                    None,
                    Some(WriterProperty::BloomFilterEnabled),
                    Some(b),
                ),
            ],
        );
    }

    #[tokio::test]
    async fn test_diagnose_level_is_the_dearest_cap() {
        let advice_only = WriterLayout {
            bloom: Bloom::Off,
            ..WriterLayout::contract()
        };
        let (_table, generation) = one_file(advice_only).await;
        let report = diagnose(&generation, Level::Optimal, &ValueFormat::opaque, None);
        assert_eq!(report.summary.level, Level::Optimal, "advice caps nothing");
        assert_eq!((report.summary.capping, report.summary.advice), (0, 1));

        let dictionary = WriterLayout {
            dictionary: Dictionary::Value,
            ..WriterLayout::contract()
        };
        let (_table, generation) = one_file(dictionary).await;
        let report = diagnose(&generation, Level::Optimal, &ValueFormat::opaque, None);
        assert_eq!(report.summary.level, Level::Balanced);
        assert_eq!((report.summary.capping, report.summary.advice), (1, 0));

        let mut table = Table::new(KeyType::Long, unpartitioned());
        two_disjoint_files(&mut table, Partition::None, dictionary).await;
        let report = diagnose(
            &load(&mut table, Residency::Full, None).await,
            Level::Optimal,
            &ValueFormat::opaque,
            None,
        );
        assert_eq!(
            codes(&report.capping()),
            [
                FindingCode::DictionaryEncodedValue,
                FindingCode::NoBucketPartition
            ]
        );
        assert_eq!(
            report.summary.level,
            Level::Permissive,
            "a balanced cap and a permissive one leave the table permissive"
        );
    }

    #[tokio::test]
    async fn test_diagnose_bloom_residency_caps_balanced_with_no_finding() {
        let mut table = contract_table().await;
        let report = diagnose(
            &load(&mut table, Residency::Bloom, None).await,
            Level::Balanced,
            &ValueFormat::opaque,
            None,
        );
        assert_findings(&report, &[]);
        assert_eq!(report.summary.level, Level::Balanced);
        assert_eq!(
            report.summary.level_by_residency,
            BTreeMap::from([
                (Residency::Full, Level::Optimal),
                (Residency::Disk, Level::Optimal),
                (Residency::Bloom, Level::Balanced),
            ])
        );
    }

    #[tokio::test]
    async fn test_diagnose_level_by_residency_combines_the_layout_with_each_tier() {
        let (_table, report, _json) = dictionary_value_report().await;
        assert_eq!(
            report.summary.level_by_residency,
            BTreeMap::from([
                (Residency::Full, Level::Balanced),
                (Residency::Disk, Level::Balanced),
                (Residency::Bloom, Level::Balanced),
            ])
        );
        assert_eq!(report.summary.lookups, Level::Optimal);
        assert_eq!(
            diagnose(
                &one_file(WriterLayout::contract()).await.1,
                Level::Permissive,
                &ValueFormat::opaque,
                None
            )
            .summary
            .lookups,
            Level::Permissive,
            "the declared level is reported as given"
        );
    }

    #[tokio::test]
    async fn test_diagnose_level_by_residency_omits_a_tier_that_cannot_load() {
        let no_filter = WriterLayout {
            bloom: Bloom::Off,
            ..WriterLayout::contract()
        };
        let (_table, generation) = one_file(no_filter).await;
        let report = diagnose(&generation, Level::Optimal, &ValueFormat::opaque, None);
        assert_eq!(report.summary.predicted.bloom, None);
        assert_eq!(
            report.summary.level_by_residency,
            BTreeMap::from([
                (Residency::Full, Level::Optimal),
                (Residency::Disk, Level::Optimal),
            ])
        );
    }

    #[tokio::test]
    async fn test_judge_refuses_below_level_only_under_strict() {
        let (_table, report, _json) = dictionary_value_report().await;
        let summary = &report.summary;
        let codes = report.capping();
        judge(
            TABLE,
            summary.level,
            &summary.column_levels(),
            &codes,
            Level::Optimal,
            false,
        )
        .unwrap();
        let reason = testing::refusal_of(judge(
            TABLE,
            summary.level,
            &summary.column_levels(),
            &codes,
            Level::Optimal,
            true,
        ));
        assert_eq!(
            reason,
            RefusalReason::BelowLevel {
                declared: Level::Optimal,
                achieved: Level::Balanced,
                columns: vec![crate::check::ColumnLevel {
                    column: VALUE_COLUMN.to_string(),
                    level: Level::Balanced,
                }],
                findings: vec![Capping {
                    code: FindingCode::DictionaryEncodedValue,
                    column: Some(VALUE_COLUMN.to_string()),
                }],
            }
        );
        for declared in [Level::Balanced, Level::Permissive] {
            judge(
                TABLE,
                summary.level,
                &summary.column_levels(),
                &codes,
                declared,
                true,
            )
            .unwrap();
        }

        let advice_only = WriterLayout {
            bloom: Bloom::Off,
            ..WriterLayout::contract()
        };
        let (_table, generation) = one_file(advice_only).await;
        let report = diagnose(&generation, Level::Optimal, &ValueFormat::opaque, None);
        assert_eq!(report.summary.advice, 1);
        judge(
            TABLE,
            report.summary.level,
            &report.summary.column_levels(),
            &report.capping(),
            Level::Optimal,
            true,
        )
        .unwrap();
    }

    #[tokio::test]
    async fn test_judge_names_only_the_codes_the_declared_level_refuses() {
        let mut table = Table::bucketed(ONE);
        let first = testing::keys_from(0, FILE_KEYS);
        let interleaved: Vec<i64> = first.iter().map(|key| key + 1).collect();
        let layout = WriterLayout {
            dictionary: Dictionary::Value,
            ..WriterLayout::contract()
        };
        for (name, keys) in [("a", &first), ("b", &interleaved)] {
            let typed = testing::long_keys(keys);
            table
                .add_file(file(name, &typed, Partition::Bucket(0), layout))
                .await;
        }
        let report = diagnose(
            &load(&mut table, Residency::Full, None).await,
            Level::Balanced,
            &ValueFormat::opaque,
            None,
        );
        assert_eq!(
            codes(&report.capping()),
            [
                FindingCode::DictionaryEncodedValue,
                FindingCode::OverlappingFiles
            ]
        );
        let reason = testing::refusal_of(judge(
            TABLE,
            report.summary.level,
            &report.summary.column_levels(),
            &report.capping(),
            Level::Balanced,
            true,
        ));
        assert_eq!(
            reason,
            RefusalReason::BelowLevel {
                declared: Level::Balanced,
                achieved: Level::Permissive,
                columns: vec![crate::check::ColumnLevel {
                    column: VALUE_COLUMN.to_string(),
                    level: Level::Permissive,
                }],
                findings: vec![crate::check::Capping {
                    code: FindingCode::OverlappingFiles,
                    column: None,
                }],
            }
        );
    }

    #[tokio::test]
    async fn test_diagnose_predicted_bytes_per_tier_equal_the_loaded_index_bytes() {
        for residency in Residency::ALL {
            let scratch = tempfile::tempdir().unwrap();
            let mut table = contract_table().await;
            let local = testing::local_dir(scratch.path(), table.storage.stats(), None);
            let local = (residency == Residency::Disk).then_some(&local);
            let generation = load(&mut table, residency, local).await;
            let report = diagnose(&generation, Level::Optimal, &ValueFormat::opaque, None);
            assert_eq!(report.summary.residency, residency);
            assert_eq!(report.summary.index_bytes, generation.index_bytes);
            let predicted = report.summary.predicted;
            assert_eq!(
                predicted.in_tier(residency).unwrap(),
                approx::count(generation.index_bytes),
                "{residency}"
            );
            let resident: usize = report.files.iter().map(|file| file.resident_bytes).sum();
            assert_eq!(resident, generation.index_bytes, "{residency}");
            let per_file: Vec<ResidentBytes> = generation
                .files
                .iter()
                .map(|file| {
                    FileIndex::predicted_resident_bytes(
                        file.index.metadata(),
                        file.index.key_leaf(),
                        file.index.order_column(),
                        &file.index.held_value_leaves(),
                        file.index.key_type(),
                        file.index.handle(),
                    )
                })
                .collect();
            assert_eq!(
                predicted.full,
                per_file.iter().map(|file| file.full).sum::<u64>()
            );
            assert_eq!(
                predicted.disk_memory,
                per_file.iter().map(|file| file.disk_memory).sum::<u64>()
            );
            assert_eq!(
                predicted.disk_bytes,
                per_file.iter().map(|file| file.disk_bytes).sum::<u64>()
            );
            assert_eq!(
                predicted.key_chunk_bytes,
                report
                    .files
                    .iter()
                    .map(|file| file.key_chunk_bytes)
                    .sum::<u64>()
            );
            assert_eq!(
                predicted.bloom,
                per_file.iter().map(|file| file.bloom).sum::<Option<u64>>()
            );
            assert_eq!(
                predicted.filter_bytes,
                report
                    .files
                    .iter()
                    .map(|file| file.bloom_bytes)
                    .sum::<Option<u64>>()
            );
            assert_eq!(
                predicted.bloom.map(|bloom| bloom - predicted.disk_memory),
                predicted.filter_bytes,
                "the filters are what the bloom tier holds beyond the disk tier's memory"
            );
            assert!(predicted.bloom_bits_per_key.unwrap() >= BLOOM_BITS_PER_KEY_MIN);
        }
    }

    /// The report of a one-file table whose value column is
    /// dictionary-encoded, so it carries exactly one capping finding, with
    /// the JSON it serializes to.
    async fn dictionary_value_report() -> (Table, TableReport, Json) {
        let layout = WriterLayout {
            dictionary: Dictionary::Value,
            ..WriterLayout::contract()
        };
        let (table, generation) = one_file(layout).await;
        let report = diagnose(&generation, Level::Optimal, &ValueFormat::opaque, None);
        let json = serde_json::to_value(&report).unwrap();
        (table, report, json)
    }

    /// The rows of `deleted_file_table`'s file `f` its delete removes.
    const DELETED_ROWS: [i64; 2] = [1, 3];

    /// A one-bucket table holding one file `f` of `FILE_KEYS` long keys,
    /// committed as `FIRST_SNAPSHOT`, then as `SECOND_SNAPSHOT` beside a
    /// position delete file removing `DELETED_ROWS` of `f`, a dangling one
    /// naming a file the table never held, and `update`, when given, a
    /// file of the deleted rows' keys written again; each snapshot loaded
    /// in `residency` with `strict` off, the second over the first, the
    /// Disk tier's chunk files under `scratch`.
    async fn deleted_file_table(
        residency: Residency,
        update: bool,
        scratch: &std::path::Path,
    ) -> (Table, Generation, Generation) {
        let mut table = Table::bucketed(ONE);
        let local = testing::local_dir(scratch, table.storage.stats(), None);
        let local = (residency == Residency::Disk).then_some(&local);
        let keys = testing::long_keys(&testing::keys_from(0, FILE_KEYS));
        table
            .add_file(file(
                "f",
                &keys,
                Partition::Bucket(0),
                WriterLayout::contract(),
            ))
            .await;
        table
            .commit(FIRST_SNAPSHOT, None, FIRST_SEQUENCE, FIRST_VERSION)
            .await;
        let config = TableConfig {
            strict: false,
            ..table.config(residency, None)
        };
        let before = table.load(&config, local, None).await.unwrap();
        table.add_position_delete("d", "f", &DELETED_ROWS, 0).await;
        table
            .add_position_delete("dangling", "gone", &DELETED_ROWS, 0)
            .await;
        let updated: Vec<Key> = DELETED_ROWS
            .iter()
            .map(|&row| keys[approx::length(u64::try_from(row).unwrap())].clone())
            .collect();
        if update {
            table
                .add_file(file(
                    "u",
                    &updated,
                    Partition::Bucket(0),
                    WriterLayout::contract(),
                ))
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
        let after = table.load(&config, local, Some(&before)).await.unwrap();
        (table, before, after)
    }

    /// The `deletes_pending` findings of `report`.
    fn pending(report: &TableReport) -> Vec<&Finding> {
        report
            .findings
            .iter()
            .filter(|finding| finding.code() == FindingCode::DeletesPending)
            .collect()
    }

    /// Deletes that remove rows raise one `deletes_pending`, advice of the
    /// table, and the summary gives what the load applied: the rows and
    /// the one delete file that names a held file, not the dangling one.
    /// A snapshot with none raises none.
    #[tokio::test]
    async fn test_diagnose_deletes_pending_reports_the_applied_deletes() {
        let scratch = tempfile::tempdir().unwrap();
        let (_table, before, after) =
            deleted_file_table(Residency::Full, false, scratch.path()).await;
        let clean = diagnose(&before, Level::Optimal, &ValueFormat::opaque, None);
        assert!(pending(&clean).is_empty());
        assert_eq!(clean.summary.deletes, DeleteSummary::default());

        let report = diagnose(&after, Level::Optimal, &ValueFormat::opaque, None);
        let findings = pending(&report);
        assert_eq!(findings.len(), 1);
        assert_eq!(findings[0].caps(), None);
        assert_eq!(findings[0].penalty(), None);
        assert_eq!(findings[0].file(), None);
        assert_eq!(report.summary.level, Level::Optimal, "deletes cap nothing");
        assert_eq!(report.summary.advice, report.findings.len());
        let deletes = report.summary.deletes;
        assert_eq!(deletes, after.delete_summary());
        assert_eq!(deletes.rows, approx::count(DELETED_ROWS.len()));
        let applying_delete_files = 1;
        assert_eq!(deletes.files, applying_delete_files, "not the dangling one");
        assert_eq!(
            deletes.fraction.to_bits(),
            approx::ratio(deletes.rows, after.records).to_bits()
        );
        assert!(deletes.state_bytes > 0);
        assert_eq!(
            compacted_level(&after, &layout_caps(&LayoutView::of(&after))),
            None
        );
    }

    /// Every tier's predicted memory counts the delete state at its bound,
    /// which holds what the load applied; the chunk files on disk do not.
    #[tokio::test]
    async fn test_diagnose_predicted_memory_counts_the_delete_bound() {
        for residency in Residency::ALL {
            let scratch = tempfile::tempdir().unwrap();
            let (_table, before, after) =
                deleted_file_table(residency, false, scratch.path()).await;
            let bound = after.delete_bound();
            assert!(bound >= approx::count(after.delete_summary().state_bytes));
            assert_eq!(before.delete_bound(), 0);
            let without = diagnose(&before, Level::Optimal, &ValueFormat::opaque, None)
                .summary
                .predicted;
            let with = diagnose(&after, Level::Optimal, &ValueFormat::opaque, None)
                .summary
                .predicted;
            assert_eq!(with.full, without.full + bound, "{residency}");
            assert_eq!(with.disk_memory, without.disk_memory + bound, "{residency}");
            assert_eq!(
                with.bloom,
                without.bloom.map(|bloom| bloom + bound),
                "{residency}"
            );
            assert_eq!(with.disk_bytes, without.disk_bytes, "{residency}");
        }
    }

    /// An update, a newer file of the deleted rows' keys, overlaps the file
    /// it updates and caps the level at `permissive`; `deletes_pending`
    /// then names the level a compaction restores, the residency's best.
    #[tokio::test]
    async fn test_diagnose_deletes_pending_names_the_level_a_compaction_restores() {
        let scratch = tempfile::tempdir().unwrap();
        let (_table, _, after) = deleted_file_table(Residency::Full, true, scratch.path()).await;
        let caps = layout_caps(&LayoutView::of(&after));
        assert_eq!(caps.level, Level::Permissive);
        assert_eq!(codes(&caps.capping()), [FindingCode::OverlappingFiles]);
        assert_eq!(compacted_level(&after, &caps), Some(Level::Optimal));
        let report = diagnose(&after, Level::Optimal, &ValueFormat::opaque, None);
        assert_eq!(pending(&report).len(), 1);
        assert_eq!(
            report.summary.deletes.rows,
            approx::count(DELETED_ROWS.len())
        );
    }

    /// An update beside an unbucketed file: with no files overlapping,
    /// `no_bucket_partition` still caps the table at `permissive`, so a
    /// compaction of the overlap restores no level and `deletes_pending`
    /// names none.
    #[tokio::test]
    async fn test_diagnose_deletes_pending_names_no_level_where_another_cap_remains() {
        let mut table = Table::new(KeyType::Long, unpartitioned());
        let stray = testing::long_keys(&testing::keys_from(EVOLVED_KEYS_START, FILE_KEYS));
        table
            .add_file(file("s", &stray, Partition::None, WriterLayout::contract()))
            .await;
        table.evolve(ONE);
        let keys = testing::long_keys(&testing::keys_from(0, FILE_KEYS));
        let updated: Vec<Key> = DELETED_ROWS
            .iter()
            .map(|&row| keys[approx::length(u64::try_from(row).unwrap())].clone())
            .collect();
        for (name, keys) in [("f", &keys), ("u", &updated)] {
            table
                .add_file(file(
                    name,
                    keys,
                    Partition::Bucket(0),
                    WriterLayout::contract(),
                ))
                .await;
        }
        table.add_position_delete("d", "f", &DELETED_ROWS, 0).await;
        table
            .commit(FIRST_SNAPSHOT, None, FIRST_SEQUENCE, FIRST_VERSION)
            .await;
        let config = TableConfig {
            strict: false,
            ..table.config(Residency::Full, None)
        };
        let generation = table.load(&config, None, None).await.unwrap();
        let caps = layout_caps(&LayoutView::of(&generation));
        assert_eq!(caps.level, Level::Permissive);
        assert_eq!(
            codes(&caps.capping()),
            [
                FindingCode::NoBucketPartition,
                FindingCode::OverlappingFiles
            ]
        );
        assert_eq!(compacted_level(&generation, &caps), None);
        let report = diagnose(&generation, Level::Optimal, &ValueFormat::opaque, None);
        assert_eq!(pending(&report).len(), 1);
        assert_eq!(
            report.summary.deletes.rows,
            approx::count(DELETED_ROWS.len())
        );
    }

    #[tokio::test]
    async fn test_serialize_table_summary_snake_case_fields_with_display_strings() {
        let (_table, report, json) = dictionary_value_report().await;
        let summary = json["summary"].as_object().unwrap();
        let names: BTreeSet<&str> = summary.keys().map(String::as_str).collect();
        assert_eq!(
            names,
            BTreeSet::from([
                "table",
                "snapshot_id",
                "level",
                "lookups",
                "residency",
                "file_count",
                "buckets",
                "buckets_with_files",
                "max_files_per_bucket",
                "records",
                "index_bytes",
                "predicted",
                "level_by_residency",
                "lookup_cost",
                "value_columns",
                "deletes",
                "capping",
                "advice",
            ])
        );
        assert!(
            report.summary.value_columns[0].value_bytes > 0,
            "the column's pages are measured, so its bytes are not zero"
        );
        assert_eq!(
            summary["value_columns"],
            serde_json::json!([{
                "column": VALUE_COLUMN,
                "format": ValueFormat::Opaque.to_string(),
                "level": Level::Balanced.to_string(),
                "gets_per_hit": VALUE_PAGE_GETS + DICTIONARY_PAGE_GETS,
                "files_predating_column": 0,
                "value_bytes": report.summary.value_columns[0].value_bytes,
            }]),
            "the dictionary-encoded column is balanced at two GETs"
        );
        assert_eq!(
            summary["deletes"],
            serde_json::json!({"rows": 0, "files": 0, "fraction": 0.0, "state_bytes": 0}),
            "a snapshot with no deletes"
        );
        assert_eq!(
            summary["lookup_cost"],
            serde_json::json!({
                "gets_per_hit": VALUE_PAGE_GETS + DICTIONARY_PAGE_GETS,
                "gets_per_miss": 0,
                "files_probed": FILES_PROBED_ALONE,
            }),
            "a full hit fetches the value dictionary page beside the value page"
        );
        assert_eq!(summary["table"], Json::String(TABLE.to_string()));
        assert_eq!(
            summary["snapshot_id"],
            Json::String(FIRST_SNAPSHOT.to_string())
        );
        assert_eq!(summary["level"], Json::String(Level::Balanced.to_string()));
        assert_eq!(summary["lookups"], Json::String(Level::Optimal.to_string()));
        assert_eq!(
            summary["level_by_residency"][Residency::Bloom.name()],
            Json::String(Level::Balanced.to_string())
        );
        assert_eq!(
            summary["residency"],
            Json::String(Residency::Full.to_string())
        );
        assert_eq!(summary["buckets"], Json::from(ONE));
        assert_eq!(summary["capping"], Json::from(1));
        assert_eq!(summary["advice"], Json::from(0));
        assert_eq!(
            summary["predicted"]["full"],
            Json::from(report.summary.predicted.full)
        );
        assert!(summary["predicted"]["bloom_bits_per_key"].is_f64());
        assert_eq!(
            serde_json::to_value(&report.summary).unwrap(),
            json["summary"]
        );
    }

    #[tokio::test]
    async fn test_serialize_finding_snake_case_fields_with_display_strings() {
        let (table, _report, json) = dictionary_value_report().await;
        let finding = &json["findings"][0];
        assert_eq!(finding["caps"], Json::String(Level::Balanced.to_string()));
        assert_eq!(
            finding["code"],
            Json::String(FindingCode::DictionaryEncodedValue.to_string())
        );
        assert_eq!(
            finding["property"],
            Json::String(WriterProperty::EnableDictionary.to_string())
        );
        assert_eq!(
            finding["penalty"]["extra_gets_per_hit"],
            Json::from(DICTIONARY_PAGE_GETS)
        );
        assert_eq!(finding["penalty"]["files_probed_per_lookup"], Json::Null);
        assert_eq!(finding["file"], Json::String(table.path_of("f")));
        assert_eq!(finding["column"], Json::String(VALUE_COLUMN.to_string()));
        assert!(finding["message"].is_string());
        let advice = Finding::advice(FindingCode::LongKeys, None, String::new(), None);
        let advice = serde_json::to_value(&advice).unwrap();
        assert_eq!(advice["caps"], Json::Null);
        assert_eq!(
            advice["column"],
            Json::Null,
            "a finding about the row names no column"
        );
    }

    #[tokio::test]
    async fn test_serialize_file_report_snake_case_fields_with_display_strings() {
        let (_table, _report, json) = dictionary_value_report().await;
        let file = json["files"][0].as_object().unwrap();
        let file_names: BTreeSet<&str> = file.keys().map(String::as_str).collect();
        assert_eq!(
            file_names,
            BTreeSet::from([
                "path",
                "bucket",
                "key_type",
                "rows",
                "row_groups",
                "key_pages",
                "max_key_page_rows",
                "max_key_bytes",
                "value_columns",
                "dictionary_columns",
                "bloom_bytes",
                "bloom_bits_per_key",
                "key_chunk_bytes",
                "distinct_page_mins_ratio",
                "bounds_present",
                "resident_bytes",
            ])
        );
        assert_eq!(file["key_type"], Json::String(KeyType::Long.to_string()));
        assert_eq!(
            file["dictionary_columns"],
            Json::Array(vec![Json::String(VALUE_COLUMN.to_string())])
        );
        assert_eq!(file["bounds_present"], Json::Bool(true));
        let column = file["value_columns"][0].as_object().unwrap();
        let column_names: BTreeSet<&str> = column.keys().map(String::as_str).collect();
        assert_eq!(
            column_names,
            BTreeSet::from([
                "column",
                "predates_column",
                "value_pages",
                "avg_value_page_bytes",
                "avg_value_bytes",
                "pages_aligned",
            ])
        );
        assert_eq!(column["column"], Json::String(VALUE_COLUMN.to_string()));
        assert_eq!(column["predates_column"], Json::Bool(false));
    }

    /// A one-bucket table of one file per entry of `pages`, `FILE_KEYS`
    /// keys each in disjoint ascending runs, named `f0`, `f1`, ...: every
    /// value of a file null when its entry is `None`, else that entry's
    /// bytes. Loaded in the Full tier.
    async fn files_of_values(pages: &[Option<&[u8]>]) -> (Table, Generation) {
        let mut table = Table::bucketed(ONE);
        let mut from = 0;
        for (at, page) in pages.iter().enumerate() {
            let keys = testing::keys_from(from, FILE_KEYS);
            from = keys.last().unwrap() + KEY_STEP;
            let typed = testing::long_keys(&keys);
            table
                .add_file(FileSpec {
                    values: keys.iter().map(|_| page.map(<[u8]>::to_vec)).collect(),
                    ..file(
                        &format!("f{at}"),
                        &typed,
                        Partition::Bucket(0),
                        WriterLayout::contract(),
                    )
                })
                .await;
        }
        let generation = load(&mut table, Residency::Full, None).await;
        (table, generation)
    }

    /// The `malformed_protobuf` findings of `report`, each its file.
    fn malformed_files(report: &TableReport) -> Vec<Option<&str>> {
        report
            .findings
            .iter()
            .filter(|finding| finding.code() == FindingCode::MalformedProtobuf)
            .map(Finding::file)
            .collect()
    }

    #[tokio::test]
    async fn test_examine_passes_a_page_of_only_nulls_to_the_next_file_up_to_the_bound() {
        let malformed: &[u8] = &[0x07; testing::VALUE_BYTES];
        let protobuf = |_: &str| ValueFormat::Protobuf;

        let (table, generation) = files_of_values(&[None, Some(malformed)]).await;
        let before = table.gets();
        let examined = examine(&generation, Level::Optimal, &protobuf, None).await;
        assert!(examined.unwalked.is_empty());
        let second = table.path_of("f1");
        assert_eq!(
            malformed_files(&examined.report),
            [Some(second.as_str())],
            "the next file's page is walked"
        );
        assert_eq!(table.gets() - before, 2, "one GET per page walked");

        let mut pages = vec![None; MOST_WALKED_FILES];
        pages.push(Some(malformed));
        let (table, generation) = files_of_values(&pages).await;
        let before = table.gets();
        let examined = examine(&generation, Level::Optimal, &protobuf, None).await;
        assert!(malformed_files(&examined.report).is_empty());
        assert_eq!(
            table.gets() - before,
            approx::count(MOST_WALKED_FILES),
            "no file past the bound is read"
        );

        let (table, generation) = files_of_values(&[Some(malformed)]).await;
        let before = table.gets();
        let opaque = examine(&generation, Level::Optimal, &ValueFormat::opaque, None).await;
        assert!(malformed_files(&opaque.report).is_empty());
        assert_eq!(table.gets(), before, "an opaque column is not walked");
    }

    #[tokio::test]
    async fn test_examine_leaves_a_column_unwalked_when_its_page_read_fails() {
        let (table, generation) = files_of_values(&[Some(b"\x08\x01")]).await;
        let path = table.path_of("f0");
        std::fs::remove_file(path.strip_prefix(FILE_PREFIX).unwrap()).unwrap();
        let examined = examine(
            &generation,
            Level::Optimal,
            &|_| ValueFormat::Protobuf,
            None,
        )
        .await;
        let [unwalked] = &examined.unwalked[..] else {
            panic!("one column unwalked, got {:?}", examined.unwalked);
        };
        assert_eq!(unwalked.column, VALUE_COLUMN);
        assert_eq!(unwalked.error.kind(), crate::error::ErrorKind::ObjectStore);
        let whole = diagnose(&generation, Level::Optimal, &ValueFormat::opaque, None);
        assert_eq!(examined.report.summary.level, whole.summary.level);
        assert_eq!(examined.report.findings.len(), whole.findings.len());
    }
}
