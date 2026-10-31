//! The benchmark program, a program and not a test: the in-process lookup
//! and load paths measured over contract-shaped tables on the local
//! filesystem, so every number is icetray's own CPU and allocation cost with
//! no object-store latency in it. It writes a long-key and a string-key
//! table of `ROWS` rows each, once with random values and once with text
//! values (`Values`); two long-key tables of protobuf values beside a
//! second value column, one of `ROWS` rows at the other tables' value size
//! and one of `LARGE_VALUE_ROWS` rows of 4 KiB values, served with `value`
//! declared protobuf (RFD 022, RFD 024); ordered tables (RFD 025): one of
//! `ORDERED_KEYS` long keys of `ORDERED_ROWS_PER_KEY` rows each by a
//! timestamp order column, which the first Scan and `MultiScan` scenarios
//! read and beside which the key-value scenarios also run, two of
//! `LONG_RUN_ROWS` rows per key, one by a timestamp and one by a string
//! order column whose values share a long prefix inside which the writer
//! cut the column index, which the long-run Scan scenarios read, and two
//! only loaded and refreshed, one of files rolled by size that share edge
//! keys and one of overlapping appends of short runs; and, when `--table
//! scattered` names it, a table shaped like a serving table
//! (`SCATTERED_ROWS` rows of random-looking string keys and 4 KiB values in
//! byte-bounded pages, `SCATTERED_ROWS_PER_FILE` rows per file), through
//! the fixture writer (once, then reused from `--data-dir`), reports how
//! far zstd compressed each table's key and value columns, serves them
//! through `Table` over `Storage`'s `file://` store in every residency
//! tier, and reports latency percentiles, throughput, allocations per
//! operation, the bytes an operation read from the store and the local
//! directory, the value bytes it read and returned, and for a Scan the
//! rounds of GETs it waited on (`RoundsStore`); plus the wall time, CPU
//! time, allocations, reads, and peak resident set of a load, of a load
//! over the chunk files a Disk load left (a restart), and of a refresh,
//! with an ordered table's rounds of GETs and the bytes its uniqueness
//! merge held and had in flight, the resident index's bytes by part, and
//! the chunk files' bytes. Key-value tables read through the row cache
//! (RFD 026) under `--row-cache on` and through the page cache alone under
//! `off`, the default; `skewed-hit` asks keys drawn from a Zipf
//! distribution through a cache budget smaller than the table, and is
//! meant to be run both ways.
//! Results print as a table and are written as JSON under `--out-dir`. Run
//! with `cargo run -p icetray-testkit --example bench --release -- --help`;
//! a profile of one scenario narrows `--scenario`, `--tier`, `--table`,
//! `--values`, and `--mode`, and lengthens `--duration`.
//!
//! Reference mode. Under `--reference WAREHOUSE` the program writes and
//! reads none of its own tables and serves the golden reference tables
//! instead (`golden::GoldenTable::reference`, which `dev/golden-tables.py
//! --only reference` writes), from a warehouse whose `db/` holds them, such
//! as a copy on object storage read through `--rewrite-location` rules.
//! Per table and tier it measures Scans of the keys the golden tests sample
//! from each tier of rows per key (`golden::reference_tier_sample`), in
//! both directions, whole and over a window of each run's middle half, cold
//! (the page cache emptied before each operation,
//! outside its timing) and warm (every key paged through first);
//! `MultiScan`s of 10 and 100 keys at each key's newest row and a light
//! key's most rows (`reference_multi_limits`); and first loads with their
//! Disk restarts, whose rounds of GETs and held bytes show the
//! uniqueness merge of `ref_appended`'s overlap groups, the read-ahead
//! over `ref_large_files`' row groups, and both at once over
//! `ref_large_appended`'s files, more than a load of four worker threads
//! reads at once. Every request's answers are first
//! compared whole with the derivation (`golden::derived_scan`). Every
//! request of the warehouse's store goes through a `GetLog` beneath the
//! rounds count, and each run reports its window's GETs by the data pages
//! each spans and by its bytes, with each class's latency to the answer
//! and to the last byte. `--reference-table`, `--reference-scenario`, and
//! `--rows-tier` narrow it, `--reference-max-scan-rows` sets the tables'
//! `max_scan_rows`, at which the Scans run, so a raised one gives a heavy
//! key's spans more pages, and its JSON says what it is
//! (`"run": "reference"`), the limits it served at, and, per row, what each
//! figure is of.

use std::alloc::{GlobalAlloc, Layout};
use std::collections::hash_map::Entry;
use std::collections::{BTreeSet, HashMap};
use std::error::Error;
use std::io::Read as _;
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use clap::{Parser, ValueEnum};
use futures::future::OptionFuture;
use iceberg::spec::MAIN_BRANCH;
use icetray_core::ColumnRead;
use icetray_core::config::{
    Config, LocalConfig, Residency, RewriteRule, ServerConfig, SourceConfig, StorageConfig,
    TableConfig, ValueColumnConfig, ValueFormat, least_max_multi_scan_values, reads_rows,
};
use icetray_core::contract::{TARGET_FILE_BYTES_MAX, VALUE_PAGE_BYTES};
use icetray_core::{
    DEFAULT_MAX_SCAN_ROWS, Direction, FILE_PREFIX, FindingLog, Generation, Key, KeyType, Level,
    LocalDir, OrderValue, PageCache, ResidentParts, Row, RowCache, ScanBounds, Scanned, Storage,
    Table, TableHandles, TableSource,
};
use icetray_core::{approx, budget, catalog, time};
use icetray_testkit::fixture::{NAMESPACE, SECOND_VALUE_COLUMN, memory_catalog_local};
use icetray_testkit::golden::{
    self, GoldenTable, REFERENCE_TIER_SAMPLES, REFERENCE_VALUE_BYTES, RowsTier,
};
use icetray_testkit::keys::{
    FIRST_VERSION, SCATTERED_ID_SPACE, SCATTERED_KEY_FIXED_BYTES, STRING_KEY_FIXED_BYTES,
    VALUE_BYTES,
};
use icetray_testkit::latency::{Latencies, P50, P90, P99, micros};
use icetray_testkit::layout::{KEY_COLUMN, Layout as FileLayout, VALUE_COLUMN};
use icetray_testkit::protobuf::{
    CHECKSUM_FIELD, FILLER_FIELD, FLAGS_FIELD, GROUP_FIELD, ID_FIELD, NAME_FIELD, SCORES_FIELD,
};
use icetray_testkit::{
    FixtureSpec, GetLog, KeyShape, LoggedGet, MINUTE_MICROS, MemoryCatalog, ORDER_COLUMN,
    OrderKind, Ordered, RoundsStore, RowsPerKey, TestScan, ValueKind, absent_key_from, follow,
    key_at_shaped, middle_half, order_value, row_value, scan_model, scanned_of, value_of_kind,
    write_fixture,
};
use object_store::ObjectStore;
use parquet::file::page_index::offset_index::PageLocation;
use parquet::file::reader::{FileReader as _, SerializedFileReader};
use rand::rngs::StdRng;
use rand::seq::SliceRandom as _;
use rand::{RngExt, SeedableRng};
use serde_json::{Value as Json, json};
use tempfile::TempDir;
use tikv_jemallocator::Jemalloc;

/// Whatever failed; the program reports it and exits.
type BenchResult<T> = Result<T, Box<dyn Error + Send + Sync>>;

// ---------------------------------------------------------------------------
// Allocation counting.

/// The allocator every allocation reaches in the end: jemalloc, as the
/// binary runs, so what is counted is what the server would allocate.
static JEMALLOC: Jemalloc = Jemalloc;

/// jemalloc behind a counter of allocation calls and requested bytes.
#[global_allocator]
static GLOBAL: Counting = Counting;

/// Shards of the allocation counters. One shared counter would put every
/// allocation of every worker thread on one cache line and slow the
/// multi-thread runs by the contention; each thread adds to the shard its
/// marker's address hashes to, and a read sums the shards.
const COUNTER_SHARDS: usize = 64;

/// The multiplier of the shard hash: the golden ratio's fractional bits, a
/// Fibonacci hash, which spreads nearby thread-local addresses apart.
const SHARD_HASH: usize = 0x9e37_79b9_7f4a_7c15;

/// The shift that keeps the hash's top `log2(COUNTER_SHARDS)` bits.
const SHARD_SHIFT: u32 = usize::BITS - COUNTER_SHARDS.trailing_zeros();

/// One shard of the counters, alone on its cache line; 128 bytes is the
/// line of the Apple M-series cores and twice that of x86, so it separates
/// shards on both.
#[repr(align(128))]
struct Shard {
    allocations: AtomicU64,
    bytes: AtomicU64,
}

impl Shard {
    const fn new() -> Self {
        Self {
            allocations: AtomicU64::new(0),
            bytes: AtomicU64::new(0),
        }
    }
}

static SHARDS: [Shard; COUNTER_SHARDS] = [const { Shard::new() }; COUNTER_SHARDS];

thread_local! {
    /// A byte per thread whose address picks the thread's shard. Constant
    /// and without a destructor, so reading it from inside the allocator
    /// does not allocate, and it stays readable while the thread exits.
    static MARKER: u8 = const { 0 };
}

/// Counts one allocation call of `size` bytes on the calling thread's shard.
fn count_allocation(size: usize) {
    let address = MARKER.with(|marker| std::ptr::from_ref(marker).addr());
    let shard = &SHARDS[address.wrapping_mul(SHARD_HASH) >> SHARD_SHIFT];
    shard.allocations.fetch_add(1, Ordering::Relaxed);
    shard
        .bytes
        .fetch_add(approx::count(size), Ordering::Relaxed);
}

/// jemalloc, counting `alloc`, `alloc_zeroed`, and `realloc` as one
/// allocation each, at the bytes requested (a `realloc` at its new size).
struct Counting;

// SAFETY: every method forwards to jemalloc with the caller's arguments
// unchanged, so jemalloc's guarantees are the caller's; the counting beside
// it touches only atomics and a constant thread-local, and never allocates.
unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        count_allocation(layout.size());
        // SAFETY: the caller upholds `alloc`'s contract, forwarded as is.
        unsafe { JEMALLOC.alloc(layout) }
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        count_allocation(layout.size());
        // SAFETY: the caller upholds `alloc_zeroed`'s contract.
        unsafe { JEMALLOC.alloc_zeroed(layout) }
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        count_allocation(new_size);
        // SAFETY: the caller upholds `realloc`'s contract.
        unsafe { JEMALLOC.realloc(ptr, layout, new_size) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        // SAFETY: the caller upholds `dealloc`'s contract.
        unsafe { JEMALLOC.dealloc(ptr, layout) }
    }
}

/// The process's allocation calls and requested bytes so far.
#[derive(Clone, Copy, Debug, Default)]
struct Allocations {
    calls: u64,
    bytes: u64,
}

impl Allocations {
    fn now() -> Self {
        SHARDS.iter().fold(Self::default(), |sum, shard| Self {
            calls: sum.calls + shard.allocations.load(Ordering::Relaxed),
            bytes: sum.bytes + shard.bytes.load(Ordering::Relaxed),
        })
    }

    fn since(self, earlier: Self) -> Self {
        Self {
            calls: self.calls - earlier.calls,
            bytes: self.bytes - earlier.bytes,
        }
    }
}

// ---------------------------------------------------------------------------
// The command line.

/// What to measure.
#[derive(Clone, Copy, Debug, PartialEq, Eq, ValueEnum)]
enum Scenario {
    /// A hit whose value the warm cache budget holds: its row in the row
    /// cache under `--row-cache on`, its value page in the page cache
    /// under `off`.
    WarmHit,
    /// A hit whose value neither cache holds.
    ColdHit,
    /// A hit whose key is drawn from a Zipf distribution of exponent
    /// `ZIPF_EXPONENT` over the table's keys, ranked in a seeded random
    /// order so the hot keys lie scattered through the key order, through
    /// the `SKEWED_CACHE_BYTES` budget, which holds a share of the table's
    /// values, filled by a pass of the scenario's own keys before it is
    /// measured; through the row cache or the page cache alone as
    /// `--row-cache` says.
    SkewedHit,
    /// A key inside the table's range that no file holds.
    Miss,
    /// A `MultiGet` of 100 hits, warm.
    #[value(name = "multi-get-100")]
    MultiGet100,
    /// A `MultiGet` of 1,000 hits, warm.
    #[value(name = "multi-get-1000")]
    MultiGet1000,
    /// A table's first load.
    Load,
    /// A refresh onto an appended snapshot, and an unchanged poll.
    Refresh,
    /// Two tables served in one process: `--tasks` tasks of warm hits on
    /// the long table of random values beside one task looping a
    /// `MultiGet` of 1,000 warm hits on the string table of random values.
    /// Serves those two whatever `--table` and `--values` select.
    TwoTable,
    /// A warm hit naming both value columns, `value` and `detail`, whole:
    /// against `warm-hit` on the same table, the cost of a second column.
    /// Protobuf tables only.
    WarmHitBoth,
    /// A warm hit naming no column, which answers presence alone.
    PresenceHit,
    /// A warm hit projecting `value` to `PROJECT_FEW`: against `warm-hit`
    /// on the same table, the walk's cost and the bytes it saves. Protobuf
    /// tables only.
    WarmHitProjectFew,
    /// A warm hit projecting `value` to `PROJECT_MOST`, a walk that copies
    /// nearly the whole value. Protobuf tables only.
    WarmHitProjectMost,
    /// A `MultiGet` of 100 warm hits naming both value columns. Protobuf
    /// tables only.
    #[value(name = "multi-get-100-both")]
    MultiGet100Both,
    /// A `MultiGet` of 100 warm hits projecting `value` to `PROJECT_FEW`.
    /// Protobuf tables only.
    #[value(name = "multi-get-100-project")]
    MultiGet100Project,
    /// A cold hit naming both value columns: against `cold-hit`, what a
    /// second column's concurrent GET adds to the latency. Protobuf tables
    /// only.
    ColdHitBoth,
    /// A cold hit projecting `value` to `PROJECT_FEW`: against `cold-hit`,
    /// the walk's cost beside a GET. Protobuf tables only.
    ColdHitProjectFew,
    /// A `MultiGet` of 100 cold hits: the batch fan-out under GETs.
    #[value(name = "multi-get-100-cold")]
    MultiGet100Cold,
    /// A `MultiGet` of 100 cold hits naming both value columns, two GETs a
    /// key under one fan-out. Protobuf tables only.
    #[value(name = "multi-get-100-cold-both")]
    MultiGet100ColdBoth,
    /// A Scan of one key's whole run, newest first, whose pages the page
    /// cache holds. Ordered table only.
    ScanWarm,
    /// A Scan of one key's whole run through the `COLD_CACHE_BYTES` page
    /// cache, which holds almost none of the table's pages. Ordered table
    /// only.
    ScanCold,
    /// A Scan of `SCAN_PAGE_ROWS` rows, newest first, inside a window over
    /// the middle half of the key's run, warm: one page of a read paged by
    /// order value. Ordered table only.
    ScanBounded,
    /// A `MultiScan` of `MULTI_SCAN_KEYS` keys at the largest limit
    /// `server.max_multi_scan_values` admits for them, warm. Ordered table
    /// only.
    #[value(name = "multi-scan-10")]
    MultiScan10,
    /// `multi-scan-10` through the `COLD_CACHE_BYTES` page cache. Ordered
    /// table only.
    #[value(name = "multi-scan-10-cold")]
    MultiScan10Cold,
    /// A `MultiScan` of `SMALL_BATCH_KEYS` keys at a limit of
    /// `MULTI_SCAN_HEAD_ROWS`, warm: each key's newest row, at the scale a
    /// call's keys are spread over workers. Ordered table only.
    #[value(name = "multi-scan-100")]
    MultiScan100,
    /// `multi-scan-100` through the `COLD_CACHE_BYTES` page cache. Ordered
    /// table only.
    #[value(name = "multi-scan-100-cold")]
    MultiScan100Cold,
    /// `multi-scan-100` of `LARGE_BATCH_KEYS` keys. Ordered table only.
    #[value(name = "multi-scan-1000")]
    MultiScan1000,
    /// `multi-scan-1000` through the `COLD_CACHE_BYTES` page cache. Ordered
    /// table only.
    #[value(name = "multi-scan-1000-cold")]
    MultiScan1000Cold,
    /// The key-value scenarios of `BESIDE_ORDERED` on the long table of
    /// random values, in a process that also serves the ordered table over
    /// the same handles, the ordered table loaded, primed, and idle. Serves
    /// those two whatever `--table` and `--values` select.
    KvBesideOrdered,
    /// A Scan of `SCAN_PAGE_ROWS` rows, newest first, before an order value
    /// drawn from the middle half of a key's `LONG_RUN_ROWS`-row run
    /// (`bounded_in_run`), warm: a page deep in a long history. Long-run
    /// table only.
    ScanBoundedLong,
    /// `scan-bounded-long` through the `COLD_CACHE_BYTES` page cache.
    /// Long-run table only.
    ScanBoundedLongCold,
    /// A Scan of the `HEAD_ROWS` newest rows of a key's long run, no bound,
    /// warm. Long-run table only.
    ScanHeadLong,
    /// `scan-bounded-long` over the string-ordered table, whose order
    /// values share a long prefix inside which the writer truncated the
    /// order column's index. String-ordered table only.
    ScanBoundedString,
    /// `scan-bounded-string` through the `COLD_CACHE_BYTES` page cache.
    /// String-ordered table only.
    ScanBoundedStringCold,
}

impl Scenario {
    /// The key-value scenarios `kv-beside-ordered` runs on the long table
    /// beside the ordered table.
    const BESIDE_ORDERED: [Self; 4] = [Self::WarmHit, Self::ColdHit, Self::Miss, Self::MultiGet100];

    /// What the scenario measures, and what it needs of a table.
    const fn traits(self) -> Traits {
        match self {
            Self::WarmHit => Traits::warm("warm-hit"),
            Self::ColdHit => Traits::cold("cold-hit"),
            Self::SkewedHit => Traits::of("skewed-hit", Cache::Skewed),
            Self::Miss => Traits::unread("miss"),
            Self::MultiGet100 => Traits::warm("multi-get-100"),
            Self::MultiGet1000 => Traits::warm("multi-get-1000"),
            Self::Load => Traits::unread("load").apart(),
            Self::Refresh => Traits::unread("refresh").apart(),
            Self::TwoTable => Traits::warm("two-table").apart(),
            Self::WarmHitBoth => Traits::warm("warm-hit-both").protobuf(),
            Self::PresenceHit => Traits::warm("presence-hit"),
            Self::WarmHitProjectFew => Traits::warm("warm-hit-project-few").projecting(),
            Self::WarmHitProjectMost => Traits::warm("warm-hit-project-most").projecting(),
            Self::MultiGet100Both => Traits::warm("multi-get-100-both").protobuf(),
            Self::MultiGet100Project => Traits::warm("multi-get-100-project").projecting(),
            Self::ColdHitBoth => Traits::cold("cold-hit-both").protobuf(),
            Self::ColdHitProjectFew => Traits::cold("cold-hit-project-few").projecting(),
            Self::MultiGet100Cold => Traits::cold("multi-get-100-cold"),
            Self::MultiGet100ColdBoth => Traits::cold("multi-get-100-cold-both").protobuf(),
            Self::ScanWarm => Traits::warm("scan-warm").on(TableName::Ordered),
            Self::ScanCold => Traits::cold("scan-cold").on(TableName::Ordered),
            Self::ScanBounded => Traits::warm("scan-bounded").on(TableName::Ordered),
            Self::MultiScan10 => Traits::warm("multi-scan-10").on(TableName::Ordered),
            Self::MultiScan10Cold => Traits::cold("multi-scan-10-cold").on(TableName::Ordered),
            Self::MultiScan100 => Traits::warm("multi-scan-100").on(TableName::Ordered),
            Self::MultiScan100Cold => Traits::cold("multi-scan-100-cold").on(TableName::Ordered),
            Self::MultiScan1000 => Traits::warm("multi-scan-1000").on(TableName::Ordered),
            Self::MultiScan1000Cold => Traits::cold("multi-scan-1000-cold").on(TableName::Ordered),
            Self::KvBesideOrdered => Traits::warm("kv-beside-ordered").apart(),
            Self::ScanBoundedLong => Traits::warm("scan-bounded-long").on(TableName::OrderedLong),
            Self::ScanBoundedLongCold => {
                Traits::cold("scan-bounded-long-cold").on(TableName::OrderedLong)
            }
            Self::ScanHeadLong => Traits::warm("scan-head-long").on(TableName::OrderedLong),
            Self::ScanBoundedString => {
                Traits::warm("scan-bounded-string").on(TableName::OrderedString)
            }
            Self::ScanBoundedStringCold => {
                Traits::cold("scan-bounded-string-cold").on(TableName::OrderedString)
            }
        }
    }

    /// The scenario's name in the results.
    const fn name(self) -> &'static str {
        self.traits().name
    }
}

/// What a scenario measures, and what it needs of the table it runs on.
#[derive(Clone, Copy)]
struct Traits {
    /// The scenario's name in the results.
    name: &'static str,
    /// The cache budget it measures.
    cache: Cache,
    /// Whether `lookups` measures it, per table and tier; the others are
    /// loads, refreshes, and the scenarios of two tables, which `main`
    /// runs on their own.
    lookup: bool,
    /// Whether it reads `SECOND_VALUE_COLUMN` or projects `VALUE_COLUMN`,
    /// which only a protobuf table serves.
    protobuf: bool,
    /// Whether it projects `VALUE_COLUMN`, whose bytes read per key are
    /// then the whole value's, not what it returns.
    projects: bool,
    /// The ordered table a Scan or `MultiScan` scenario reads, the only one
    /// it runs on; `None` for the other scenarios, which only a key-value
    /// table serves.
    table: Option<TableName>,
}

/// The cache budget a scenario measures, split between the row cache and
/// the page cache as `--row-cache` says (`stack`).
#[derive(Clone, Copy, PartialEq, Eq)]
enum Cache {
    /// The `WARM_CACHE_BYTES` budget, primed to hold every value, which
    /// the table's must fit.
    Warm,
    /// The `COLD_CACHE_BYTES` budget, which holds almost none of the
    /// table's values.
    Cold,
    /// The `SKEWED_CACHE_BYTES` budget, which holds a share of the table's
    /// values, filled by the scenario's own skewed keys.
    Skewed,
    /// None it depends on: a miss, served warm, reads no value page, and a
    /// load or a refresh reads none through it.
    Unread,
}

impl Traits {
    /// A lookup of a key-value table through `cache`.
    const fn of(name: &'static str, cache: Cache) -> Self {
        Self {
            name,
            cache,
            lookup: true,
            protobuf: false,
            projects: false,
            table: None,
        }
    }

    /// A lookup through the warm cache.
    const fn warm(name: &'static str) -> Self {
        Self::of(name, Cache::Warm)
    }

    /// A lookup through the cold cache.
    const fn cold(name: &'static str) -> Self {
        Self::of(name, Cache::Cold)
    }

    /// A scenario that reads no value page through a cache.
    const fn unread(name: &'static str) -> Self {
        Self::of(name, Cache::Unread)
    }

    /// Not measured by `lookups`.
    const fn apart(self) -> Self {
        Self {
            lookup: false,
            ..self
        }
    }

    /// Reading `SECOND_VALUE_COLUMN`.
    const fn protobuf(self) -> Self {
        Self {
            protobuf: true,
            ..self
        }
    }

    /// Projecting `VALUE_COLUMN`, which only a protobuf table serves.
    const fn projecting(self) -> Self {
        Self {
            protobuf: true,
            projects: true,
            ..self
        }
    }

    /// A Scan or `MultiScan` of `table`.
    const fn on(self, table: TableName) -> Self {
        Self {
            table: Some(table),
            ..self
        }
    }
}

/// A residency tier (RFD 017).
#[derive(Clone, Copy, Debug, PartialEq, Eq, ValueEnum)]
enum Tier {
    Full,
    Disk,
    Bloom,
}

impl Tier {
    const ALL: [Self; 3] = [Self::Full, Self::Disk, Self::Bloom];

    /// The tier's name in the results.
    fn name(self) -> &'static str {
        match self {
            Self::Full => "full",
            Self::Disk => "disk",
            Self::Bloom => "bloom",
        }
    }

    fn residency(self) -> Residency {
        match self {
            Self::Full => Residency::Full,
            Self::Disk => Residency::Disk,
            Self::Bloom => Residency::Bloom,
        }
    }
}

/// A benchmark table, by its keys.
#[derive(Clone, Copy, Debug, PartialEq, Eq, ValueEnum)]
enum TableName {
    /// Long keys, contract layout; under `--values protobuf-large`, value
    /// pages bounded by bytes.
    Long,
    /// Sequential string keys sharing a long prefix, contract layout.
    String,
    /// A table shaped like a serving table: random-looking string keys,
    /// 4 KiB random values in 64 KiB value pages, and about 37,500 rows per
    /// file. Runs only when named, and only its load, refresh, cold-hit,
    /// miss, and skewed-hit scenarios: its value pages outrun the warm
    /// cache.
    Scattered,
    /// Long keys of `ORDERED_ROWS_PER_KEY` rows each by a timestamp order
    /// column, contract layout: the table the first Scan and `MultiScan`
    /// scenarios read, and the only one they read.
    Ordered,
    /// Long keys of `LONG_RUN_ROWS` rows each by a timestamp order column,
    /// contract layout, so a run spans many order and value pages: the
    /// table `scan-bounded-long` and `scan-head-long` read.
    OrderedLong,
    /// Long keys of `LONG_RUN_ROWS` rows each by a string order column of
    /// `STRING_ORDER_WIDTH` digits, whose values share their first
    /// `STRING_ORDER_PREFIX_BYTES` bytes, written with the column index cut
    /// to `ORDER_INDEX_TRUNCATE_BYTES`, inside that prefix: the table
    /// `scan-bounded-string` reads.
    OrderedString,
    /// The long-run table's rows in files rolled at `ROLLED_FILE_BYTES`, as
    /// a compaction that rolls files by size writes them: several files of
    /// one snapshot per bucket, which meet where one file's last key is the
    /// next one's first. Served at `balanced`; loads and refreshes only.
    OrderedRolled,
    /// `ORDERED_KEYS` long keys of `SHORT_RUN_ROWS` rows each, then
    /// `OVERLAPPING_APPENDS` appends of `SHORT_RUN_ROWS` later rows per key,
    /// each starting at a key's own offset (`overlapping_rows`), so every
    /// append's order range overlaps the one before it while no key's runs
    /// do; the second build is one more such append. Served at
    /// `permissive`; loads and refreshes only.
    OrderedAppended,
}

/// What a benchmark table's values are made of.
#[derive(Clone, Copy, Debug, PartialEq, Eq, ValueEnum)]
enum Values {
    /// Pseudo-random bytes, which zstd does not shrink: a value page's
    /// decompression is near its cheapest.
    Random,
    /// JSON-like records with repeated field names and a skewed vocabulary
    /// (`text_value_for_len`), which zstd compresses as structured values
    /// compress.
    Text,
    /// Protobuf messages of every wire type padded by a repeated filler
    /// field (`protobuf::value`), at the random table's value size, in
    /// `value` declared protobuf, beside `detail`, each row's value
    /// reversed, opaque.
    Protobuf,
    /// As `Protobuf`, at `SERVING_VALUE_BYTES` per value and
    /// `LARGE_VALUE_ROWS` rows, in value pages bounded by bytes as a
    /// serving table's are.
    ProtobufLarge,
}

impl Values {
    const ALL: [Self; 4] = [
        Self::Random,
        Self::Text,
        Self::Protobuf,
        Self::ProtobufLarge,
    ];

    /// The values' name in the results and, after the key column's, in a
    /// table's name.
    fn as_str(self) -> &'static str {
        match self {
            Self::Random => "random",
            Self::Text => "text",
            Self::Protobuf => "protobuf",
            Self::ProtobufLarge => "protobuf_large",
        }
    }

    fn kind(self) -> ValueKind {
        match self {
            Self::Random => ValueKind::Random,
            Self::Text => ValueKind::Text,
            Self::Protobuf | Self::ProtobufLarge => ValueKind::Protobuf,
        }
    }

    /// Whether the table holds `SECOND_VALUE_COLUMN` and is served with
    /// `VALUE_COLUMN` declared protobuf.
    fn is_protobuf(self) -> bool {
        match self {
            Self::Protobuf | Self::ProtobufLarge => true,
            Self::Random | Self::Text => false,
        }
    }
}

/// How many tasks drive the lookups.
#[derive(Clone, Copy, Debug, PartialEq, Eq, ValueEnum)]
enum Mode {
    /// One task, one lookup at a time: latency.
    Single,
    /// `--tasks` tasks at once on the multi-thread runtime: throughput.
    Multi,
}

impl Mode {
    const ALL: [Self; 2] = [Self::Single, Self::Multi];

    fn name(self) -> &'static str {
        match self {
            Self::Single => "single",
            Self::Multi => "multi",
        }
    }
}

/// Whether the run's key-value tables read through the row cache.
#[derive(Clone, Copy, Debug, PartialEq, Eq, ValueEnum)]
enum RowCacheFlag {
    /// Every key-value table reads through the row cache, which holds the
    /// default `server.row_cache_share` of each budget.
    On,
    /// Every key-value table sets `row_cache = false`, so the page cache
    /// holds the whole budget.
    Off,
}

impl RowCacheFlag {
    fn is_on(self) -> bool {
        match self {
            Self::On => true,
            Self::Off => false,
        }
    }
}

/// The benchmark's command line. Every list flag takes comma-separated
/// values and, when absent, means all of them.
#[derive(Parser, Debug)]
#[command(about = "Measures icetray's in-process lookup and load paths")]
struct Cli {
    /// Scenarios to run.
    #[arg(long, value_enum, value_delimiter = ',')]
    scenario: Vec<Scenario>,
    /// Residency tiers to run them in.
    #[arg(long, value_enum, value_delimiter = ',')]
    tier: Vec<Tier>,
    /// Tables to run them over, by keys; `scattered` runs only when named.
    #[arg(long, value_enum, value_delimiter = ',')]
    table: Vec<TableName>,
    /// Tables to run them over, by values; the protobuf values are on long
    /// keys alone.
    #[arg(long, value_enum, value_delimiter = ',')]
    values: Vec<Values>,
    /// Single-task latency, multi-task throughput, or both.
    #[arg(long, value_enum, value_delimiter = ',')]
    mode: Vec<Mode>,
    /// Seconds each lookup scenario is measured for.
    #[arg(long, default_value_t = 5, value_parser = clap::value_parser!(u64).range(1..))]
    duration: u64,
    /// Seconds each lookup scenario runs, unmeasured, before it is measured.
    #[arg(long, default_value_t = 1, value_parser = clap::value_parser!(u64).range(1..))]
    warmup: u64,
    /// Tasks of a multi-task run; the machine's cores when absent.
    #[arg(long)]
    tasks: Option<usize>,
    /// Loads, and refreshes, per table and tier.
    #[arg(long, default_value_t = 3, value_parser = clap::value_parser!(u32).range(1..))]
    load_rounds: u32,
    /// Rows per row group of the tables written, the writer's default when
    /// absent. The manifest records it, so a table recorded under another
    /// value is written again; another `--data-dir` keeps both.
    #[arg(
        long,
        value_parser = clap::builder::RangedU64ValueParser::<usize>::new().range(1..),
    )]
    row_group_rows: Option<usize>,
    /// Where the tables are written and, on later runs, reused from.
    #[arg(long, default_value = "target/bench/tables")]
    data_dir: PathBuf,
    /// Where the JSON results go.
    #[arg(long, default_value = "target/bench")]
    out_dir: PathBuf,
    /// Write the tables again even when `--data-dir` holds them; the
    /// directory is emptied first.
    #[arg(long)]
    rewrite: bool,
    /// Write or reuse the selected tables, print one line per table (name,
    /// key type, rows, and the first build's metadata file), and exit.
    #[arg(long)]
    prepare: bool,
    /// Measure without the binary's Prometheus recorder installed, so the
    /// difference against a run with it is the metrics' cost.
    #[arg(long)]
    no_metrics: bool,
    /// A location rewrite, `FROM=TO`, applied to every location the tables'
    /// metadata names, as `[[storage.rewrite]]` applies it; repeatable. A
    /// table written under a local directory and copied to object storage
    /// is read from the copy with
    /// `--rewrite-location file:///dir/=s3://bucket/dir/`.
    #[arg(long)]
    rewrite_location: Vec<RewriteRule>,
    /// The GETs one `MultiGet` holds in flight on every table of the run,
    /// as `server.batch_fan_out` bounds them; its default when absent.
    #[arg(
        long,
        default_value_t = ServerConfig::default().batch_fan_out,
        value_parser = clap::builder::RangedU64ValueParser::<usize>::new().range(1..),
    )]
    batch_fan_out: usize,
    /// Whether the key-value tables of the run read through the row cache
    /// (RFD 026). Off by default, so the warm scenarios measure the page
    /// cache holding the whole budget and compare with the published
    /// sweeps; `skewed-hit` is meant to be run both ways.
    #[arg(long, value_enum, default_value_t = RowCacheFlag::Off)]
    row_cache: RowCacheFlag,
    /// Serve the golden reference tables under WAREHOUSE, a location whose
    /// `db/` holds them, in place of the program's own tables, which are
    /// neither written nor read (the module doc's Reference mode). A copy
    /// of the warehouse is read with a `--rewrite-location` for each form
    /// of location its metadata names, `file:///dir` and `file:/dir`.
    #[arg(
        long,
        value_name = "WAREHOUSE",
        conflicts_with_all = [
            "scenario",
            "table",
            "values",
            "prepare",
            "rewrite",
            "row_group_rows",
            "row_cache",
        ],
    )]
    reference: Option<String>,
    /// The reference tables to serve: `ref_compacted`, `ref_appended`,
    /// `ref_string`, `ref_large_files`, `ref_large_appended`.
    #[arg(long, value_delimiter = ',', requires = "reference")]
    reference_table: Vec<GoldenTable>,
    /// The `max_scan_rows` of every served reference table, in place of
    /// the default; the Scan scenarios run at it, so a heavy key's Scans
    /// read longer spans. `server.max_multi_scan_values` rises to the least
    /// that admits it when its default does not. A value a server would
    /// refuse to start with is refused, and so is one below the `MultiScan`
    /// scenarios' limits when a `MultiScan` scenario runs.
    #[arg(
        long,
        value_name = "ROWS",
        requires = "reference",
        value_parser = clap::builder::RangedU64ValueParser::<u32>::new().range(1..),
    )]
    reference_max_scan_rows: Option<u32>,
    /// What to measure of each reference table.
    #[arg(long, value_enum, value_delimiter = ',', requires = "reference")]
    reference_scenario: Vec<ReferenceScenario>,
    /// The tiers of rows per key whose sampled keys the reference Scans
    /// read: heavy, medium, light.
    #[arg(long, value_delimiter = ',', requires = "reference")]
    rows_tier: Vec<RowsTier>,
}

/// How every table of the run is served: over the production storage
/// defaults with the run's `--rewrite-location` rules, under the run's
/// `--batch-fan-out`, and each reference table at the run's
/// `--reference-max-scan-rows`, under the `server.max_multi_scan_values`
/// that admits it, and each key-value table through the row cache when
/// the run's `--row-cache` is on.
#[derive(Clone, Debug)]
struct Serving {
    storage: StorageConfig,
    batch_fan_out: usize,
    /// A reference table's `max_scan_rows`; the default when `None`.
    reference_max_scan_rows: Option<u32>,
    max_multi_scan_values: usize,
    /// Whether a key-value table reads through the row cache.
    row_cache: bool,
}

impl Default for Serving {
    fn default() -> Self {
        let server = ServerConfig::default();
        Self {
            storage: StorageConfig::default(),
            batch_fan_out: server.batch_fan_out,
            reference_max_scan_rows: None,
            max_multi_scan_values: server.max_multi_scan_values,
            row_cache: false,
        }
    }
}

/// The run's `Serving`.
static SERVING: OnceLock<Serving> = OnceLock::new();

/// Sets `SERVING` from `cli`'s `--rewrite-location` rules,
/// `--batch-fan-out`, `--row-cache`, and `--reference-max-scan-rows`,
/// once: under the last, `server.max_multi_scan_values` is its default
/// or, when that is lower, the least a table at that `max_scan_rows` is
/// valid under.
fn set_serving(cli: &Cli) {
    let defaults = Serving::default();
    let max_multi_scan_values = match cli.reference_max_scan_rows {
        Some(rows) => defaults
            .max_multi_scan_values
            .max(least_max_multi_scan_values(rows)),
        None => defaults.max_multi_scan_values,
    };
    SERVING
        .set(Serving {
            storage: StorageConfig {
                rewrite: cli.rewrite_location.clone(),
                ..StorageConfig::default()
            },
            batch_fan_out: cli.batch_fan_out,
            reference_max_scan_rows: cli.reference_max_scan_rows,
            max_multi_scan_values,
            row_cache: cli.row_cache.is_on(),
        })
        .expect("main sets the serving configuration once");
}

/// `SERVING`, or the defaults before `main` sets it.
fn serving() -> Serving {
    SERVING.get().cloned().unwrap_or_default()
}

/// The run's `--row-group-rows`.
static ROW_GROUP_ROWS: OnceLock<Option<usize>> = OnceLock::new();

/// `ROW_GROUP_ROWS`, or the writer's default before `main` sets it.
fn row_group_rows() -> Option<usize> {
    ROW_GROUP_ROWS.get().copied().flatten()
}

// ---------------------------------------------------------------------------
// The tables.

/// Rows of each contract-shaped benchmark table's first build.
const ROWS: u64 = 2_000_000;

/// A table's first build over its second: the second appends one percent,
/// a small file per bucket, the refresh scenario's new snapshot.
const APPEND_DIVISOR: u64 = 100;

/// Rows the second build appends above the first's ids.
const APPENDED_ROWS: u64 = ROWS / APPEND_DIVISOR;

/// Rows per file of the scattered table: the mean of a serving table of
/// 1.2 billion rows over 32,768 buckets, so the table's per-file work
/// (footers, page indexes, key chunks) is in proportion to its rows as it
/// is there.
const SCATTERED_ROWS_PER_FILE: u64 = 37_500;

/// Buckets of the scattered table, one file each: enough that per-file
/// costs show in a load's profile, few enough that the table writes in a
/// few minutes and takes about 5 GB of disk.
const SCATTERED_BUCKETS: u64 = 32;

/// Rows of the scattered table's first build: `SCATTERED_ROWS_PER_FILE`
/// in each of `SCATTERED_BUCKETS`, 1.2 million.
const SCATTERED_ROWS: u64 = SCATTERED_BUCKETS * SCATTERED_ROWS_PER_FILE;

/// Rows the scattered table's second build appends.
const SCATTERED_APPENDED_ROWS: u64 = SCATTERED_ROWS / APPEND_DIVISOR;

// Both builds' ids have scattered keys.
const _: () = assert!(SCATTERED_ROWS + SCATTERED_APPENDED_ROWS <= SCATTERED_ID_SPACE);

/// Bytes per value of the scattered table and of the large protobuf
/// table: 4 KiB, a serving table's value, so a 64 KiB value page holds
/// about sixteen.
const SERVING_VALUE_BYTES: usize = 4_096;

/// Rows of the large protobuf table's first build: two columns of
/// `SERVING_VALUE_BYTES` per row, about 780 MiB of value pages, which
/// `WARM_CACHE_BYTES` holds with the key pages beside, so the table runs
/// warm, and about 800 MiB of disk.
const LARGE_VALUE_ROWS: u64 = 100_000;

/// Rows the large protobuf table's second build appends.
const LARGE_VALUE_APPENDED_ROWS: u64 = LARGE_VALUE_ROWS / APPEND_DIVISOR;

/// Rows per key of the ordered table: a time series of 100 points, a
/// tenth of the default `max_scan_rows`, so a Scan at that limit returns
/// the whole run, and a run of 100-byte values lies in one or two of the
/// contract's 1,000-row value pages.
const ORDERED_ROWS_PER_KEY: u64 = 100;

/// Keys of the ordered table's first build: `ROWS` rows at
/// `ORDERED_ROWS_PER_KEY` a key, as many rows as a key-value table holds.
const ORDERED_KEYS: u64 = ROWS / ORDERED_ROWS_PER_KEY;

/// Keys the ordered table's second build appends, each with its whole run.
const ORDERED_APPENDED_KEYS: u64 = ORDERED_KEYS / APPEND_DIVISOR;

/// The order column of the ordered table: every key holds
/// `ORDERED_ROWS_PER_KEY` rows a minute apart (`MINUTE_MICROS`) from
/// 2026-01-01T00:00:00Z (`golden::TS_START`), in microseconds, the same
/// order values for every key, so one answer of the model serves a Scan
/// scenario's every key (`ScanCase`).
const ORDERED: Ordered = Ordered {
    kind: OrderKind::Timestamp,
    rows_per_key: RowsPerKey::Each(ORDERED_ROWS_PER_KEY),
    start: golden::TS_START,
    step: MINUTE_MICROS,
};

/// Rows per key of the long-run tables: ten times the default
/// `max_scan_rows`, so a run spans ten of the contract's 1,000-row order
/// and value pages, and reading a whole run takes ten Scans paged by order
/// value.
const LONG_RUN_ROWS: u64 = 10_000;

/// Keys of each long-run table's first build: `ROWS` rows at
/// `LONG_RUN_ROWS` a key, as many rows as a key-value table holds.
const LONG_RUN_KEYS: u64 = ROWS / LONG_RUN_ROWS;

/// Keys a long-run table's second build appends, each with its whole run.
const LONG_RUN_APPENDED_KEYS: u64 = LONG_RUN_KEYS / APPEND_DIVISOR;

/// The order column of the long-run and rolled tables: `ORDERED`'s
/// timestamps, `LONG_RUN_ROWS` of them a key.
const LONG_RUN: Ordered = Ordered {
    rows_per_key: RowsPerKey::Each(LONG_RUN_ROWS),
    ..ORDERED
};

/// Decimal digits of the largest long, which `OrderKind::Str` pads to.
const LONG_DIGITS: u32 = i64::MAX.ilog10() + 1;

/// The bytes every order value of the string-ordered table shares at its
/// start: 64, four times `ORDER_INDEX_TRUNCATE_BYTES`, as a history whose
/// order values carry a long common prefix (a source, a partition path)
/// before the part that varies.
const STRING_ORDER_PREFIX_BYTES: u32 = 64;

/// The digits the string-ordered table's values are padded to:
/// `LONG_DIGITS` past the shared prefix, so every value starts with at
/// least `STRING_ORDER_PREFIX_BYTES` zeros.
const STRING_ORDER_WIDTH: u32 = STRING_ORDER_PREFIX_BYTES + LONG_DIGITS;

/// The column-index truncation of the string-ordered table: 16 bytes,
/// inside the shared prefix, so every order page's cut minimum and maximum
/// are the same, and the index places no bound on a page.
const ORDER_INDEX_TRUNCATE_BYTES: usize = 16;

/// The order column of the string-ordered table: `LONG_RUN`'s values as
/// strings of `STRING_ORDER_WIDTH` digits.
const LONG_STRING_RUN: Ordered = Ordered {
    kind: OrderKind::PaddedStr(STRING_ORDER_WIDTH),
    ..LONG_RUN
};

/// The file size target of the rolled table: 8 MiB, an eighth of the
/// contract's upper size, so the rolling writer splits each bucket's
/// ~58 MB into about seven files. It rolls between batches of a page's
/// rows, which a key's `LONG_RUN_ROWS` span ten of, so most files end
/// inside a key's run and share that key with the next.
const ROLLED_FILE_BYTES: usize = 8 * 1024 * 1024;

/// Rows per key in each file of the appended table: a short run, so a
/// 1,000-row page holds the runs of fifty keys.
const SHORT_RUN_ROWS: u64 = 20;

/// The appends the appended table's first build makes after its first
/// commit, each of every key's next `SHORT_RUN_ROWS` rows: five files per
/// bucket in all.
const OVERLAPPING_APPENDS: u64 = 4;

/// The order column of the appended table's first commit: `ORDERED`'s
/// timestamps, `SHORT_RUN_ROWS` of them a key; the appends that follow
/// write later ones (`overlapping_rows`).
const SHORT_RUN: Ordered = Ordered {
    rows_per_key: RowsPerKey::Each(SHORT_RUN_ROWS),
    ..ORDERED
};

/// The file prefix of an append within the appended table's first build,
/// followed by the append's number.
const APPEND_PREFIX: &str = "append";

/// The file size target of the scattered table: 256 MiB, above the
/// ~155 MB a bucket's incompressible rows take, so the rolling writer
/// keeps each bucket in one file of `SCATTERED_ROWS_PER_FILE` rows.
const SCATTERED_FILE_BYTES: usize = 256 * 1024 * 1024;

/// The column-index truncation of the scattered table: 256 bytes, what the
/// serving recipe sets (`parquet.columnindex.truncate.length`), so its
/// value column's index holds a truncated minimum and maximum per page as a
/// recipe-written file's does, not whole 4 KiB values.
const SCATTERED_COLUMN_INDEX_TRUNCATE: usize = 256;

/// The file prefix of the second build.
const SECOND_BUILD_PREFIX: &str = "build2";

/// The name of the file under `--data-dir` recording what was written.
const MANIFEST_FILE: &str = "manifest.json";

/// A benchmark table as written: its name, key type and shape, the bytes a
/// key and a value hold before compression, from which a contract table's
/// bucket count follows, what its values are made of, its two builds'
/// keys, and for an ordered table its order column and rows per key. A
/// key-value table has one row per key, so `rows` and `appended_rows` are
/// its rows. The appended table's second build adds rows to its first
/// build's keys, so its `appended_rows` are the keys it touches.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct TableShape {
    name: TableName,
    key_type: KeyType,
    key_shape: KeyShape,
    key_bytes: usize,
    value_bytes: usize,
    values: Values,
    rows: u64,
    appended_rows: u64,
    ordered: Option<Ordered>,
}

const SHAPES: [TableShape; 12] = [
    TableShape {
        name: TableName::Long,
        key_type: KeyType::Long,
        key_shape: KeyShape::Sequential,
        key_bytes: size_of::<i64>(),
        value_bytes: VALUE_BYTES,
        values: Values::Random,
        rows: ROWS,
        appended_rows: APPENDED_ROWS,
        ordered: None,
    },
    TableShape {
        name: TableName::String,
        key_type: KeyType::String,
        key_shape: KeyShape::Sequential,
        key_bytes: STRING_KEY_FIXED_BYTES,
        value_bytes: VALUE_BYTES,
        values: Values::Random,
        rows: ROWS,
        appended_rows: APPENDED_ROWS,
        ordered: None,
    },
    TableShape {
        name: TableName::Long,
        key_type: KeyType::Long,
        key_shape: KeyShape::Sequential,
        key_bytes: size_of::<i64>(),
        value_bytes: VALUE_BYTES,
        values: Values::Text,
        rows: ROWS,
        appended_rows: APPENDED_ROWS,
        ordered: None,
    },
    TableShape {
        name: TableName::String,
        key_type: KeyType::String,
        key_shape: KeyShape::Sequential,
        key_bytes: STRING_KEY_FIXED_BYTES,
        value_bytes: VALUE_BYTES,
        values: Values::Text,
        rows: ROWS,
        appended_rows: APPENDED_ROWS,
        ordered: None,
    },
    TableShape {
        name: TableName::Long,
        key_type: KeyType::Long,
        key_shape: KeyShape::Sequential,
        key_bytes: size_of::<i64>(),
        value_bytes: VALUE_BYTES,
        values: Values::Protobuf,
        rows: ROWS,
        appended_rows: APPENDED_ROWS,
        ordered: None,
    },
    TableShape {
        name: TableName::Long,
        key_type: KeyType::Long,
        key_shape: KeyShape::Sequential,
        key_bytes: size_of::<i64>(),
        value_bytes: SERVING_VALUE_BYTES,
        values: Values::ProtobufLarge,
        rows: LARGE_VALUE_ROWS,
        appended_rows: LARGE_VALUE_APPENDED_ROWS,
        ordered: None,
    },
    TableShape {
        name: TableName::Scattered,
        key_type: KeyType::String,
        key_shape: KeyShape::Scattered,
        key_bytes: SCATTERED_KEY_FIXED_BYTES,
        value_bytes: SERVING_VALUE_BYTES,
        values: Values::Random,
        rows: SCATTERED_ROWS,
        appended_rows: SCATTERED_APPENDED_ROWS,
        ordered: None,
    },
    TableShape {
        name: TableName::Ordered,
        key_type: KeyType::Long,
        key_shape: KeyShape::Sequential,
        key_bytes: size_of::<i64>(),
        value_bytes: VALUE_BYTES,
        values: Values::Random,
        rows: ORDERED_KEYS,
        appended_rows: ORDERED_APPENDED_KEYS,
        ordered: Some(ORDERED),
    },
    TableShape {
        name: TableName::OrderedLong,
        key_type: KeyType::Long,
        key_shape: KeyShape::Sequential,
        key_bytes: size_of::<i64>(),
        value_bytes: VALUE_BYTES,
        values: Values::Random,
        rows: LONG_RUN_KEYS,
        appended_rows: LONG_RUN_APPENDED_KEYS,
        ordered: Some(LONG_RUN),
    },
    TableShape {
        name: TableName::OrderedString,
        key_type: KeyType::Long,
        key_shape: KeyShape::Sequential,
        key_bytes: size_of::<i64>(),
        value_bytes: VALUE_BYTES,
        values: Values::Random,
        rows: LONG_RUN_KEYS,
        appended_rows: LONG_RUN_APPENDED_KEYS,
        ordered: Some(LONG_STRING_RUN),
    },
    TableShape {
        name: TableName::OrderedRolled,
        key_type: KeyType::Long,
        key_shape: KeyShape::Sequential,
        key_bytes: size_of::<i64>(),
        value_bytes: VALUE_BYTES,
        values: Values::Random,
        rows: LONG_RUN_KEYS,
        appended_rows: LONG_RUN_APPENDED_KEYS,
        ordered: Some(LONG_RUN),
    },
    TableShape {
        name: TableName::OrderedAppended,
        key_type: KeyType::Long,
        key_shape: KeyShape::Sequential,
        key_bytes: size_of::<i64>(),
        value_bytes: VALUE_BYTES,
        values: Values::Random,
        rows: ORDERED_KEYS,
        appended_rows: ORDERED_KEYS,
        ordered: Some(SHORT_RUN),
    },
];

/// What separates the key column's name from the values' in the name of
/// a table whose values are not random.
const VALUES_SEPARATOR: &str = "_";

impl TableName {
    fn as_str(self) -> &'static str {
        match self {
            Self::Long => "long",
            Self::String => "string",
            Self::Scattered => "scattered",
            Self::Ordered => "ordered",
            Self::OrderedLong => "ordered_long",
            Self::OrderedString => "ordered_string",
            Self::OrderedRolled => "ordered_rolled",
            Self::OrderedAppended => "ordered_appended",
        }
    }
}

impl TableShape {
    /// The table's name in the catalog and in the results: the key
    /// column's, followed after `VALUES_SEPARATOR` by the values' unless
    /// they are random.
    fn table_name(self) -> String {
        match self.values {
            Values::Random => self.name.as_str().to_string(),
            Values::Text | Values::Protobuf | Values::ProtobufLarge => format!(
                "{}{VALUES_SEPARATOR}{}",
                self.name.as_str(),
                self.values.as_str()
            ),
        }
    }

    /// Every value column the table serves, each read whole: `value`, and
    /// `detail` beside it on a protobuf table.
    fn whole_reads(self) -> &'static [ColumnRead<'static>] {
        if self.values.is_protobuf() {
            &BOTH_READS
        } else {
            &VALUE_READS
        }
    }

    /// The value columns a configuration serves: `value` opaque, or on a
    /// protobuf table `value` declared protobuf and `detail` opaque.
    fn value_columns(self) -> Vec<ValueColumnConfig> {
        if self.values.is_protobuf() {
            vec![
                ValueColumnConfig::of(VALUE_COLUMN, ValueFormat::Protobuf),
                ValueColumnConfig::named(SECOND_VALUE_COLUMN),
            ]
        } else {
            vec![ValueColumnConfig::named(VALUE_COLUMN)]
        }
    }

    /// The bytes of a row's values before compression: `value_bytes` for
    /// each column the table serves.
    fn row_value_bytes(self) -> usize {
        self.value_bytes * self.whole_reads().len()
    }

    /// The order column of an ordered table; refused for a key-value one.
    fn order(self) -> BenchResult<Ordered> {
        self.ordered
            .ok_or_else(|| format!("{} has no order column", self.table_name()).into())
    }

    /// Rows per key of the first commit: one in a key-value table, and in
    /// an ordered table as many as its first key has, which every key has.
    fn rows_per_key(self) -> u64 {
        self.ordered.map_or(1, |ordered| ordered.rows_per_key.of(0))
    }

    /// The appends of every key's later rows the first build makes after
    /// its first commit: `OVERLAPPING_APPENDS` for the appended table, none
    /// for the others.
    fn overlapping_appends(self) -> u64 {
        match self.name {
            TableName::OrderedAppended => OVERLAPPING_APPENDS,
            TableName::Long
            | TableName::String
            | TableName::Scattered
            | TableName::Ordered
            | TableName::OrderedLong
            | TableName::OrderedString
            | TableName::OrderedRolled => 0,
        }
    }

    /// The rows of the first build: its keys, the rows of each in the
    /// first commit, and those of every overlapping append.
    fn first_build_rows(self) -> u64 {
        self.rows * self.rows_per_key() * (1 + self.overlapping_appends())
    }

    /// The level the table is served at: `balanced` for the rolled table,
    /// whose files share edge keys, `permissive` for the appended one,
    /// whose files overlap, and the best its tier reaches for the others
    /// (RFD 020, RFD 025), so `strict` loads every table.
    fn lookups(self) -> Option<Level> {
        match self.name {
            TableName::OrderedRolled => Some(Level::Balanced),
            TableName::OrderedAppended => Some(Level::Permissive),
            TableName::Long
            | TableName::String
            | TableName::Scattered
            | TableName::Ordered
            | TableName::OrderedLong
            | TableName::OrderedString => None,
        }
    }

    /// The bytes of a row before compression: its key, its order value in
    /// an ordered table, and its values.
    fn row_bytes(self) -> usize {
        let order_bytes = self
            .ordered
            .map_or(0, |ordered| order_value_bytes(ordered.kind));
        self.key_bytes + order_bytes + self.row_value_bytes()
    }

    /// A contract table has one file per bucket at the contract's upper
    /// file size: the raw row bytes, its key, its order value in an ordered
    /// table, and every value column, over `TARGET_FILE_BYTES_MAX`, rounded
    /// up. Random values do not shrink under zstd, so those files come
    /// close to the raw size, inside the contract's 32 to 64 MiB. A text
    /// table keeps the bucket count of the random table with its key
    /// column, so the two differ in their values alone; its files come out
    /// smaller by the values' compression. The scattered table has one
    /// file of `SCATTERED_ROWS_PER_FILE` rows per bucket. The rolled and
    /// appended tables take the buckets a contract table of their first
    /// build's rows has, which a compaction of them would write.
    fn buckets(self) -> u32 {
        let buckets = match self.name {
            TableName::Long
            | TableName::String
            | TableName::Ordered
            | TableName::OrderedLong
            | TableName::OrderedString
            | TableName::OrderedRolled
            | TableName::OrderedAppended => {
                let row_bytes = approx::count(self.row_bytes());
                (self.first_build_rows() * row_bytes).div_ceil(TARGET_FILE_BYTES_MAX)
            }
            TableName::Scattered => self.rows.div_ceil(SCATTERED_ROWS_PER_FILE),
        };
        u32::try_from(buckets).expect("a benchmark table has a few dozen buckets at most")
    }

    /// The Parquet layout: the contract's, or for the scattered table and
    /// the large protobuf table value pages bounded at the contract's
    /// `VALUE_PAGE_BYTES` (the byte bound shortens the writer's page-size
    /// check to every two rows, so a page overshoots it by one value at
    /// most), and for the scattered table files rolled at
    /// `SCATTERED_FILE_BYTES`; for the string-ordered table the column
    /// index cut to `ORDER_INDEX_TRUNCATE_BYTES`, and for the rolled table
    /// files rolled at `ROLLED_FILE_BYTES`; every table's row groups of the
    /// run's `--row-group-rows` when it sets them.
    fn layout(self) -> FileLayout {
        FileLayout {
            row_group_rows: row_group_rows(),
            ..self.table_layout()
        }
    }

    /// `layout` before the run's `--row-group-rows`.
    fn table_layout(self) -> FileLayout {
        let byte_pages = FileLayout {
            page_bytes: Some(approx::length(VALUE_PAGE_BYTES)),
            ..FileLayout::contract()
        };
        match self.name {
            TableName::Ordered | TableName::OrderedLong | TableName::OrderedAppended => {
                FileLayout::contract()
            }
            TableName::OrderedString => FileLayout {
                column_index_truncate: Some(ORDER_INDEX_TRUNCATE_BYTES),
                ..FileLayout::contract()
            },
            TableName::OrderedRolled => FileLayout {
                target_file_bytes: ROLLED_FILE_BYTES,
                ..FileLayout::contract()
            },
            TableName::Long | TableName::String => match self.values {
                Values::Random | Values::Text | Values::Protobuf => FileLayout::contract(),
                Values::ProtobufLarge => byte_pages,
            },
            TableName::Scattered => FileLayout {
                target_file_bytes: SCATTERED_FILE_BYTES,
                column_index_truncate: Some(SCATTERED_COLUMN_INDEX_TRUNCATE),
                ..byte_pages
            },
        }
    }

    /// Ids between two keys of the pass that fills the warm cache: every
    /// id when value pages are bounded by bytes, since such a page holds a
    /// few rows of its bucket, which a stride would step over, and every id
    /// of the ordered table, whose page holds the runs of a few keys of its
    /// bucket; `PRIME_STRIDE` under the contract's pages otherwise.
    fn prime_stride(self) -> u64 {
        match (self.ordered, self.layout().page_bytes) {
            (Some(_), _) | (None, Some(_)) => EVERY_ID_STRIDE,
            (None, None) => PRIME_STRIDE,
        }
    }

    /// Whether the warm scenarios' page cache holds every value page of the
    /// table, of every column it serves, which their measurement assumes.
    fn fits_warm_cache(self) -> bool {
        self.first_build_rows() * approx::count(self.row_value_bytes()) <= WARM_CACHE_BYTES
    }

    /// The table's `i`-th key.
    fn key(self, i: u64) -> Key {
        key_at_shaped(i, self.key_type, 0, self.key_shape)
    }

    /// Whether the command line selects the table: named by `--table`, or
    /// `--table` absent and the table not the scattered one, and its values
    /// among `values`.
    fn selected(self, tables: &[TableName], values: &[Values]) -> bool {
        let named = if tables.is_empty() {
            self.name != TableName::Scattered
        } else {
            tables.contains(&self.name)
        };
        named && values.contains(&self.values)
    }
}

/// The bytes of one order value of `kind` before compression: a long's for
/// the integer and time kinds and for binary, the padded digits of a
/// string, and a fixed value's width.
fn order_value_bytes(kind: OrderKind) -> usize {
    let digits = |width: u32| approx::length(width.max(LONG_DIGITS));
    match kind {
        OrderKind::Long
        | OrderKind::Int
        | OrderKind::Date
        | OrderKind::Timestamp
        | OrderKind::TimestampTz
        | OrderKind::TimestampNs
        | OrderKind::TimestampTzNs
        | OrderKind::Bin => size_of::<i64>(),
        OrderKind::Str => digits(LONG_DIGITS),
        OrderKind::PaddedStr(width) => digits(width),
        OrderKind::Fixed(width) => approx::length(width),
    }
}

/// A written table: where its root is and the two builds' metadata files.
#[derive(Clone, Debug)]
struct Prepared {
    shape: TableShape,
    root: String,
    first: String,
    second: String,
}

impl Prepared {
    /// The table as the manifest records it; `rows` and `appended_rows`
    /// are keys, an ordered table records its `rows_per_key` beside them,
    /// the appended table its `overlapping_appends`, and a run that sets
    /// `--row-group-rows` that count.
    fn to_json(&self) -> Json {
        let mut table = json!({
            "name": self.shape.table_name(),
            "values": self.shape.values.as_str(),
            "buckets": self.shape.buckets(),
            "rows": self.shape.rows,
            "appended_rows": self.shape.appended_rows,
            "value_bytes": self.shape.value_bytes,
            "root": self.root,
            "first": self.first,
            "second": self.second,
        });
        if self.shape.ordered.is_some() {
            table["rows_per_key"] = json!(self.shape.rows_per_key());
        }
        if self.shape.overlapping_appends() > 0 {
            table["overlapping_appends"] = json!(self.shape.overlapping_appends());
        }
        if let Some(rows) = row_group_rows() {
            table["row_group_rows"] = json!(rows);
        }
        table
    }
}

/// `shapes` under `dir`, in that order: each reused when the manifest
/// records it as this program writes it now, else written, alone, in place
/// of whatever its directory held. `rewrite` empties `dir` first. The
/// manifest keeps every table it records that is still current, selected
/// or not, so selecting one table never rewrites another.
async fn prepare(dir: &Path, rewrite: bool, shapes: &[TableShape]) -> BenchResult<Vec<Prepared>> {
    if rewrite && dir.exists() {
        std::fs::remove_dir_all(dir)?;
    }
    let mut recorded = read_manifest(dir)?;
    std::fs::create_dir_all(dir)?;
    let catalog = memory_catalog_local(&std::path::absolute(dir)?).await?;
    let mut prepared = Vec::with_capacity(shapes.len());
    for &shape in shapes {
        if let Some(table) = recorded.iter().find(|table| table.shape == shape) {
            eprintln!("reusing {} under {}", shape.table_name(), dir.display());
            prepared.push(table.clone());
            continue;
        }
        let table = write_table(catalog.as_ref(), dir, shape).await?;
        recorded.retain(|other| other.shape.table_name() != shape.table_name());
        recorded.push(table.clone());
        write_manifest(dir, &recorded)?;
        prepared.push(table);
    }
    Ok(prepared)
}

/// Writes `shape` under `dir` through `catalog`, after removing what its
/// directory held: the first build of `shape.rows` keys, then the second,
/// `shape.appended_rows` more above them, each key with its rows; or, for
/// a table with overlapping appends, the first build's first commit and
/// its appends, then one more append as the second build
/// (`overlapping_rows`).
async fn write_table(
    catalog: &MemoryCatalog,
    dir: &Path,
    shape: TableShape,
) -> BenchResult<Prepared> {
    let name = shape.table_name();
    let table_dir = dir.join(NAMESPACE).join(&name);
    if table_dir.exists() {
        std::fs::remove_dir_all(&table_dir)?;
    }
    let started = Instant::now();
    let spec = FixtureSpec {
        buckets: shape.buckets(),
        ids: 0..shape.rows,
        key_shape: shape.key_shape,
        value_bytes: shape.value_bytes,
        value_kind: shape.values.kind(),
        layout: shape.layout(),
        second_value: shape.values.is_protobuf(),
        ordered: shape.ordered,
        ..FixtureSpec::small(&name, shape.key_type)
    };
    let mut fixture = write_fixture(catalog, NAMESPACE, spec).await?;
    let appends = shape.overlapping_appends();
    for append in 1..=appends {
        fixture
            .append_ordered(
                catalog,
                &overlapping_rows(shape, append)?,
                &format!("{APPEND_PREFIX}{append}"),
            )
            .await?;
    }
    let first = fixture.metadata_location().to_string();
    if appends > 0 {
        fixture
            .append_ordered(
                catalog,
                &overlapping_rows(shape, appends + 1)?,
                SECOND_BUILD_PREFIX,
            )
            .await?;
    } else {
        fixture
            .append(
                catalog,
                shape.rows..shape.rows + shape.appended_rows,
                SECOND_BUILD_PREFIX,
            )
            .await?;
    }
    eprintln!(
        "wrote {name} ({} keys of {} rows, {} overlapping appends, {} buckets, then a second build of {} keys): {:.1?}",
        shape.rows,
        shape.rows_per_key(),
        appends,
        shape.buckets(),
        shape.appended_rows,
        started.elapsed()
    );
    Ok(Prepared {
        shape,
        root: fixture.root().to_string(),
        first,
        second: fixture.metadata_location().to_string(),
    })
}

/// The rows append `append` (from one) of a table with overlapping appends
/// writes, as (id, raw order value), ascending: for each of the table's
/// keys, `SHORT_RUN_ROWS` consecutive order values starting
/// `append * SHORT_RUN_ROWS` steps past the first commit's first, plus the
/// key's own offset of `id % SHORT_RUN_ROWS` steps. Each key's runs follow
/// one another without a shared pair, while each append's order range
/// overlaps the one before it by the offsets.
fn overlapping_rows(shape: TableShape, append: u64) -> BenchResult<Vec<(u64, i64)>> {
    let ordered = shape.order()?;
    let mut rows = Vec::with_capacity(approx::length(shape.rows * SHORT_RUN_ROWS));
    for id in 0..shape.rows {
        let first = append * SHORT_RUN_ROWS + id % SHORT_RUN_ROWS;
        for step in first..first + SHORT_RUN_ROWS {
            let order = ordered
                .step
                .checked_mul(i64::try_from(step)?)
                .and_then(|offset| ordered.start.checked_add(offset))
                .ok_or_else(|| format!("append {append} of id {id} overflows its order value"))?;
            rows.push((id, order));
        }
    }
    Ok(rows)
}

/// Records `tables` as the manifest under `dir`.
fn write_manifest(dir: &Path, tables: &[Prepared]) -> BenchResult<()> {
    let manifest = json!({
        "tables": tables.iter().map(Prepared::to_json).collect::<Vec<_>>(),
    });
    std::fs::write(
        dir.join(MANIFEST_FILE),
        serde_json::to_vec_pretty(&manifest)?,
    )?;
    Ok(())
}

/// The tables the manifest under `dir` records as this program writes them
/// now, whose builds' metadata files are still there; none when it is
/// absent.
fn read_manifest(dir: &Path) -> BenchResult<Vec<Prepared>> {
    let path = dir.join(MANIFEST_FILE);
    if !path.exists() {
        return Ok(Vec::new());
    }
    let manifest: Json = serde_json::from_slice(&std::fs::read(path)?)?;
    let Some(tables) = manifest["tables"].as_array() else {
        return Ok(Vec::new());
    };
    let mut recorded = Vec::with_capacity(tables.len());
    for table in tables {
        let Some(shape) = SHAPES
            .into_iter()
            .find(|shape| table["name"].as_str() == Some(shape.table_name().as_str()))
        else {
            continue;
        };
        let text = |field: &str| table[field].as_str().map(str::to_string);
        let (Some(root), Some(first), Some(second)) = (text("root"), text("first"), text("second"))
        else {
            continue;
        };
        let current = table["values"].as_str() == Some(shape.values.as_str())
            && table["buckets"].as_u64() == Some(u64::from(shape.buckets()))
            && table["rows"].as_u64() == Some(shape.rows)
            && table["appended_rows"].as_u64() == Some(shape.appended_rows)
            && table["value_bytes"].as_u64() == Some(approx::count(shape.value_bytes))
            && table["rows_per_key"].as_u64() == shape.ordered.map(|_| shape.rows_per_key())
            && table["overlapping_appends"].as_u64()
                == Some(shape.overlapping_appends()).filter(|&appends| appends > 0)
            && table["row_group_rows"].as_u64() == row_group_rows().map(approx::count);
        if current && local_path(&first)?.exists() && local_path(&second)?.exists() {
            recorded.push(Prepared {
                shape,
                root,
                first,
                second,
            });
        }
    }
    Ok(recorded)
}

/// Reads every file under `dir` once, so the operating system's page cache
/// holds the data files and a cold hit's read is a memory copy, not a disk
/// seek: the benchmark measures icetray, not the drive.
fn prime_os_cache(dir: &Path) -> BenchResult<u64> {
    let mut read = 0;
    let mut pending = vec![dir.to_path_buf()];
    while let Some(dir) = pending.pop() {
        for entry in std::fs::read_dir(dir)? {
            let path = entry?.path();
            if path.is_dir() {
                pending.push(path);
            } else {
                read += std::io::copy(&mut std::fs::File::open(path)?, &mut std::io::sink())?;
            }
        }
    }
    Ok(read)
}

/// Bytes of one column across a table's data files, as their footers
/// record them: compressed on disk, and before compression.
#[derive(Clone, Copy, Debug, Default)]
struct ColumnBytes {
    compressed: u64,
    uncompressed: u64,
}

impl ColumnBytes {
    /// Uncompressed over compressed bytes: what zstd achieved.
    fn ratio(self) -> f64 {
        approx::ratio(self.uncompressed, self.compressed)
    }

    fn to_json(self) -> Json {
        json!({
            "compressed_bytes": self.compressed,
            "uncompressed_bytes": self.uncompressed,
            "ratio": self.ratio(),
        })
    }
}

/// The bytes of a table's columns over every data file of both builds.
#[derive(Clone, Copy, Debug, Default)]
struct TableBytes {
    key: ColumnBytes,
    value: ColumnBytes,
    /// `SECOND_VALUE_COLUMN`, zero on a table without it.
    detail: ColumnBytes,
}

/// The key and value columns' bytes over every data file of `prepared`
/// (both builds), summed from the files' footers.
fn column_bytes(prepared: &Prepared) -> BenchResult<TableBytes> {
    let mut bytes = TableBytes::default();
    let mut pending = vec![local_path(&prepared.root)?];
    while let Some(dir) = pending.pop() {
        for entry in std::fs::read_dir(dir)? {
            let path = entry?.path();
            if path.is_dir() {
                pending.push(path);
                continue;
            }
            if path
                .extension()
                .is_none_or(|extension| extension != PARQUET_EXTENSION)
            {
                continue;
            }
            let reader = SerializedFileReader::new(std::fs::File::open(&path)?)?;
            for group in reader.metadata().row_groups() {
                for column in group.columns() {
                    let sum = match column.column_descr().name() {
                        KEY_COLUMN => &mut bytes.key,
                        VALUE_COLUMN => &mut bytes.value,
                        SECOND_VALUE_COLUMN => &mut bytes.detail,
                        _ => continue,
                    };
                    sum.compressed += u64::try_from(column.compressed_size())?;
                    sum.uncompressed += u64::try_from(column.uncompressed_size())?;
                }
            }
        }
    }
    Ok(bytes)
}

/// Every table's key and value column bytes (`column_bytes`), printed and
/// as the report's JSON, `detail` among them on a protobuf table.
fn compression(prepared: &[Prepared]) -> BenchResult<Vec<Json>> {
    let mut report = Vec::with_capacity(prepared.len());
    for table in prepared {
        let TableBytes { key, value, detail } = column_bytes(table)?;
        let protobuf = table.shape.values.is_protobuf();
        let detail_ratio = if protobuf {
            format!(", detail column {:.2}x", detail.ratio())
        } else {
            String::new()
        };
        eprintln!(
            "{}: key column compresses {:.2}x, value column {:.2}x{detail_ratio} ({:.1} MiB on disk)",
            table.shape.table_name(),
            key.ratio(),
            value.ratio(),
            mib(key.compressed + value.compressed + detail.compressed)
        );
        let mut entry = json!({
            "table": table.shape.table_name(),
            "key": key.to_json(),
            "value": value.to_json(),
        });
        if protobuf {
            entry["detail"] = detail.to_json();
        }
        report.push(entry);
    }
    Ok(report)
}

/// The extension of a data file.
const PARQUET_EXTENSION: &str = "parquet";

/// A location the local catalog recorded, as the path it names.
fn local_path(location: &str) -> BenchResult<PathBuf> {
    location
        .strip_prefix(FILE_PREFIX)
        .map(PathBuf::from)
        .ok_or_else(|| format!("{location} is not a {FILE_PREFIX} location").into())
}

// ---------------------------------------------------------------------------
// Serving.

/// The cache budget of the warm and miss scenarios: large enough that,
/// under `--row-cache off`, the page cache holds every value page of a
/// table of 100-byte values (about 200 MB) and, under the Bloom tier,
/// every key page besides, and under `on` that its row cache part holds
/// every such value, so nothing is evicted. A table whose values do not
/// fit is not run warm (`fits_warm_cache`).
const WARM_CACHE_BYTES: u64 = 1 << 30;

/// The cache budget of the cold-hit scenario: one mebibyte, about ten of
/// a table's two thousand value pages, so nearly every uniformly random
/// hit misses both caches, reads its page from the file, and decodes it.
/// The run reports the page fetches it measured per lookup.
const COLD_CACHE_BYTES: u64 = 1 << 20;

/// The cache budget of the skewed scenario: 32 MiB, about a sixth of a
/// table of 100-byte values' 200 MB of value pages and, under
/// `--row-cache on`, room in the row cache's share, at its charge for such
/// a row and beside its ghost list's reserve, for about one row in
/// nineteen, so neither cache holds the whole table and the skew decides
/// which rows each serves.
const SKEWED_CACHE_BYTES: u64 = 1 << 25;

/// One table served over storage of its own, so its counters are its own,
/// beside at most one other table over the same handles, and a local
/// directory that lives as long as they do under the Disk tier.
struct Served {
    table: Arc<Table>,
    /// The table served beside `table` over the same storage, caches,
    /// local directory, and count of calls in flight, loaded and primed:
    /// idle unless a scenario drives it.
    beside: Option<Arc<Table>>,
    storage: Arc<Storage>,
    cache: PageCache,
    /// The local directory under the Disk tier, whose chunk files the
    /// sizing reports.
    local: Option<Arc<LocalDir>>,
    /// The store every `file://` read of an ordered table goes through,
    /// counting the rounds of a Scan's GETs (`count_rounds`); `None` for a
    /// key-value table, whose reads keep the store `Storage` builds, and
    /// under `--rewrite-location`, whose reads leave `file://`. A reference
    /// table's reads count their rounds through the store of its warehouse.
    rounds: Option<Arc<RoundsStore>>,
    /// The log of every GET of a reference table's warehouse
    /// (`serve_reference`); `None` for the program's own tables.
    gets: Option<Arc<GetLog>>,
    _local: Option<TempDir>,
}

/// The page cache shards `stack` resolves its budget for: one, the fewest,
/// since `PageCache::new` holds no minimum and the cold scenario's page
/// part is below the one a server's shards would hold it to.
const BUDGET_SHARDS: usize = 1;

/// What a process's tables are served over: the handles, and the local
/// directory under the Disk tier, which must live as long as they do.
struct Stack {
    handles: TableHandles,
    local: Option<Arc<LocalDir>>,
    dir: Option<TempDir>,
}

/// The handles of a process serving over `storage` in `tier`, as
/// `open_with` gives a process's tables: a cache budget of `cache_bytes`
/// split between the row cache and the page cache as a server splits it
/// (`budget::resolve`), the row cache's share held only when `rows_read`,
/// some table served reading through it, and the whole budget the page
/// cache's otherwise; the run's `--batch-fan-out` and
/// `server.max_multi_scan_values` (`Serving`), the server's defaults
/// otherwise, and under Disk a local directory with no page-cache disk
/// level, `reuse`'s when given (`local_for`).
async fn stack(
    storage: &Arc<Storage>,
    tier: Tier,
    cache_bytes: u64,
    rows_read: bool,
    reuse: Option<TempDir>,
) -> BenchResult<Stack> {
    let (local, dir) = local_for(tier, storage, reuse)?;
    let serving = serving();
    let mut server = ServerConfig::default();
    server.cache_bytes = Some(cache_bytes);
    server.batch_fan_out = serving.batch_fan_out;
    server.max_multi_scan_values = serving.max_multi_scan_values;
    let budget = budget::resolve(&server, None, rows_read, BUDGET_SHARDS)?;
    let handles = TableHandles::new(
        &server,
        Arc::clone(storage),
        PageCache::new(budget.page_bytes, None).await?,
        Some(RowCache::new(budget.row_bytes)),
        local.clone(),
        FindingLog::Events,
    );
    Ok(Stack {
        handles,
        local,
        dir,
    })
}

/// `prepared` in `tier` from `source`, over the production storage
/// defaults (hedging on) and a `cache_bytes` cache budget (`stack`), not
/// yet loaded; beside it, over the same handles as
/// `open_with` gives a process's tables, `beside` pinned, loaded, and
/// primed as the warm scenarios are. The Disk tier gets a fresh local
/// directory with no page-cache disk level, or `reuse`'s when given, whose
/// chunk files a load then reuses. An ordered table's `file://` reads go
/// through a `RoundsStore` when no location is rewritten.
async fn serve(
    prepared: &Prepared,
    beside: Option<&Prepared>,
    tier: Tier,
    cache_bytes: u64,
    source: SourceConfig,
    reuse: Option<TempDir>,
) -> BenchResult<Served> {
    let serving = serving();
    let counts_rounds = prepared.shape.ordered.is_some() && serving.storage.rewrite.is_empty();
    let storage = Arc::new(Storage::with_sdk_defaults(serving.storage));
    let rounds = counts_rounds.then(|| Arc::new(RoundsStore::local()));
    if let Some(store) = &rounds {
        storage.register(FILE_PREFIX, Arc::clone(store) as Arc<dyn ObjectStore>);
    }
    let beside = beside.map(|other| (other, table_config(other, tier, pinned(other))));
    let config = table_config(prepared, tier, source);
    let rows_read = reads_rows(beside.iter().map(|(_, entry)| entry).chain([&config]));
    let Stack {
        handles,
        local,
        dir,
    } = stack(&storage, tier, cache_bytes, rows_read, reuse).await?;
    let beside = match beside {
        Some((other, entry)) => {
            let table = table_of(entry, handles.clone()).await?;
            table.load().await?;
            prime_cache(&table, other.shape).await?;
            Some(Arc::new(table))
        }
        None => None,
    };
    let cache = handles.cache.clone();
    let table = table_of(config, handles).await?;
    Ok(Served {
        table: Arc::new(table),
        beside,
        storage,
        cache,
        local,
        rounds,
        gets: None,
        _local: dir,
    })
}

/// Under the Disk tier, a local directory over `storage` with no
/// page-cache disk level, in `reuse` when given or a fresh temporary
/// directory, and the directory, which must live as long as the tables
/// served over it; nothing under the other tiers.
fn local_for(
    tier: Tier,
    storage: &Storage,
    reuse: Option<TempDir>,
) -> BenchResult<(Option<Arc<LocalDir>>, Option<TempDir>)> {
    match tier {
        Tier::Disk => {
            let dir = match reuse {
                Some(dir) => dir,
                None => tempfile::tempdir()?,
            };
            let config = LocalConfig {
                dir: dir.path().to_path_buf(),
                index_budget_bytes: None,
                page_cache_disk_bytes: 0,
            };
            Ok((
                Some(Arc::new(LocalDir::open(&config, storage.stats())?)),
                Some(dir),
            ))
        }
        Tier::Full | Tier::Bloom => Ok((None, None)),
    }
}

/// The entry of `prepared` in `tier` from `source`, strict at its shape's
/// level (`TableShape::lookups`): a key-value table reading through the
/// row cache as the run's `--row-cache` says, an ordered table, which sets
/// no `row_cache`, with its order column at the default `max_scan_rows`.
fn table_config(prepared: &Prepared, tier: Tier, source: SourceConfig) -> TableConfig {
    TableConfig {
        name: prepared.shape.table_name(),
        source,
        r#ref: MAIN_BRANCH.to_string(),
        key_column: KEY_COLUMN.to_string(),
        value_columns: prepared.shape.value_columns(),
        order_column: prepared.shape.ordered.map(|_| ORDER_COLUMN.to_string()),
        max_scan_rows: None,
        strict: true,
        lookups: prepared.shape.lookups(),
        residency: tier.residency(),
        resident_budget_bytes: None,
        row_cache: prepared
            .shape
            .ordered
            .is_none()
            .then_some(serving().row_cache),
    }
}

/// The table `config` names over `handles`, under the server's default
/// deadlines.
async fn table_of(config: TableConfig, handles: TableHandles) -> BenchResult<Table> {
    let server = ServerConfig::default();
    let source = TableSource::from_config(&config.source).await?;
    Ok(Table::new(
        config,
        source,
        handles,
        server.load_deadline,
        server.refresh_deadline,
        server.get_deadline,
        server.scan_deadline,
    ))
}

/// The first build's metadata file, which never advances.
fn pinned(prepared: &Prepared) -> SourceConfig {
    SourceConfig::Static {
        location: prepared.first.clone(),
    }
}

// ---------------------------------------------------------------------------
// Lookups.

/// Keys in each scenario's pool; a run cycles through them. A million is
/// half a table of `ROWS`, so a warm run touches every page and no run repeats a
/// key before it has used many thousands of others.
const KEY_POOL: usize = 1 << 20;

/// The seed of the key pools, fixed so every run asks the same keys.
const KEY_SEED: u64 = 0x1ce7_4a7b_e4c4_0001;

/// Ids between two keys of the pass that fills the warm cache: a value page
/// of 100-byte values holds a thousand rows of one bucket, which span a few thousand ids, so
/// every page is asked many times over.
const PRIME_STRIDE: u64 = 100;

/// Ids between two keys of the pass that fills the warm cache of a table
/// whose value pages are bounded by bytes, or of the ordered table: every
/// id.
const EVERY_ID_STRIDE: u64 = 1;

/// The samples a run reserves beyond what its warm-up rate predicts,
/// relative to that prediction: twice, so a run faster than its warm-up
/// still records into memory reserved before it started.
const SAMPLE_HEADROOM: usize = 2;

/// The ids of the hit pool over `shape`'s key space, in its order: uniform
/// random ids of the first build.
fn hit_ids(shape: TableShape) -> Vec<u64> {
    let mut rng = StdRng::seed_from_u64(KEY_SEED);
    (0..KEY_POOL)
        .map(|_| rng.random_range(0..shape.rows))
        .collect()
}

/// The exponent of the skewed scenario's Zipf distribution: the key of
/// rank `r`, from one, is drawn in proportion to `1 / r^ZIPF_EXPONENT`.
/// At 0.9 over a table of `ROWS` keys, the hottest one percent of keys
/// take about half the lookups.
const ZIPF_EXPONENT: f64 = 0.9;

/// The seed of the skewed pools' ranking and draws, fixed so every run
/// asks the same keys.
const SKEW_SEED: u64 = 0x1ce7_4a7b_e4c4_0003;

/// Keys in the skewed scenario's measured pool: four times `KEY_POOL`, so
/// a run of both modes at the default duration asks each draw once and
/// never wraps. A wrapped pool would ask its first draws again a pool's
/// length later, which a cache serves better than it would fresh draws
/// of the same distribution.
const SKEWED_POOL: usize = 4 * KEY_POOL;

/// The skewed scenario's pools over `shape`'s key space: the `KEY_POOL`
/// keys of the pass that fills its caches (`fill_from`), then the
/// `SKEWED_POOL` keys it measures, independent draws of one distribution,
/// so the measured keys reach a cache already holding what that
/// distribution reads most and not the very keys it is about to ask. Each
/// draw is a Zipf rank by inverse CDF over the precomputed cumulative
/// weights, the rank the id it holds in a seeded shuffle of the first
/// build's ids, so the hot keys lie scattered through the key order and
/// so through the value pages.
fn skewed_pools(shape: TableShape) -> (Arc<Vec<Key>>, Arc<Vec<Key>>) {
    let mut rng = StdRng::seed_from_u64(SKEW_SEED);
    let mut by_rank: Vec<u64> = (0..shape.rows).collect();
    by_rank.shuffle(&mut rng);
    let mut total = 0.0;
    let cumulative: Vec<f64> = (1..=shape.rows)
        .map(|rank| {
            total += approx::ratio(1, rank).powf(ZIPF_EXPONENT);
            total
        })
        .collect();
    let last = by_rank.len() - 1;
    let mut draws = |count: usize| -> Arc<Vec<Key>> {
        Arc::new(
            (0..count)
                .map(|_| {
                    let drawn = rng.random_range(0.0..total);
                    let rank = cumulative.partition_point(|&sum| sum <= drawn).min(last);
                    shape.key(by_rank[rank])
                })
                .collect(),
        )
    };
    let fill = draws(KEY_POOL);
    (fill, draws(SKEWED_POOL))
}

/// Runs operations `0..ops` of `op`, `tasks` at a time, outside any
/// measured window: one pass over the skewed scenario's fill pool
/// (`skewed_pools`), which fills its caches with the rows and pages its
/// distribution reads most.
async fn fill_from(served: &Served, op: &Op, tasks: usize, ops: usize) -> BenchResult<()> {
    let handles: Vec<tokio::task::JoinHandle<BenchResult<()>>> = (0..tasks)
        .map(|task| {
            let table = Arc::clone(&served.table);
            let op = op.clone();
            tokio::spawn(async move {
                for i in (task..ops).step_by(tasks) {
                    op.run(&table, i).await?;
                }
                Ok(())
            })
        })
        .collect();
    for handle in handles {
        handle.await??;
    }
    Ok(())
}

/// Pools of hits and misses over `shape`'s key space: the `hit_ids` as
/// keys and as the absent key just above each.
fn key_pools(shape: TableShape) -> (Arc<Vec<Key>>, Arc<Vec<Key>>) {
    let hits: Vec<Key> = hit_ids(shape).into_iter().map(|id| shape.key(id)).collect();
    let misses = hits.iter().map(absent_key_from).collect();
    (Arc::new(hits), Arc::new(misses))
}

/// The top-level fields the few-field projections keep: two scalars and
/// a short text, none of the filler that is most of a value's bytes, so a
/// projection returns a few dozen bytes whatever the value's length.
const PROJECT_FEW: [u32; 3] = [ID_FIELD, NAME_FIELD, FLAGS_FIELD];

/// The top-level fields the most-field projection keeps: every field a
/// value holds but `NAME_FIELD`, the filler included, so the walk copies
/// nearly the whole value.
const PROJECT_MOST: [u32; 6] = [
    ID_FIELD,
    CHECKSUM_FIELD,
    SCORES_FIELD,
    FLAGS_FIELD,
    GROUP_FIELD,
    FILLER_FIELD,
];

/// `VALUE_COLUMN` whole: what every scenario reads unless it says
/// otherwise.
const VALUE_READS: [ColumnRead<'static>; 1] = [ColumnRead::whole(VALUE_COLUMN)];

/// Both value columns of a protobuf table, whole.
const BOTH_READS: [ColumnRead<'static>; 2] = [
    ColumnRead::whole(VALUE_COLUMN),
    ColumnRead::whole(SECOND_VALUE_COLUMN),
];

/// `VALUE_COLUMN` projected to `PROJECT_FEW`.
const FEW_READS: [ColumnRead<'static>; 1] = [ColumnRead {
    name: VALUE_COLUMN,
    fields: &PROJECT_FEW,
}];

/// `VALUE_COLUMN` projected to `PROJECT_MOST`.
const MOST_READS: [ColumnRead<'static>; 1] = [ColumnRead {
    name: VALUE_COLUMN,
    fields: &PROJECT_MOST,
}];

/// No column: a lookup that answers presence alone.
const NO_READS: [ColumnRead<'static>; 0] = [];

/// What an operation reads of each row it finds. The reads are static, so
/// naming them allocates nothing inside a measured operation.
#[derive(Clone, Debug)]
enum Reads {
    /// Columns read whole, or none: the bytes read are those returned.
    Whole(&'static [ColumnRead<'static>]),
    /// Columns of which `VALUE_COLUMN` is projected, and per key of the
    /// operation's pool its whole value's bytes, which the walk reads
    /// whatever it returns.
    Projected {
        columns: &'static [ColumnRead<'static>],
        whole: Arc<Vec<usize>>,
    },
}

/// `VALUE_COLUMN` read whole.
const WHOLE_VALUE: Reads = Reads::Whole(&VALUE_READS);

impl Reads {
    fn columns(&self) -> &'static [ColumnRead<'static>] {
        match self {
            Self::Whole(columns) | Self::Projected { columns, .. } => columns,
        }
    }

    /// The value bytes read for the pool keys `at`, which returned
    /// `returned`.
    fn read_bytes(&self, at: std::ops::Range<usize>, returned: usize) -> usize {
        match self {
            Self::Whole(_) => returned,
            Self::Projected { whole, .. } => whole[at].iter().sum(),
        }
    }
}

/// The value bytes a run's operations read and returned, summed over every
/// column they named. A projection reads the whole value and returns what
/// it keeps; a whole read returns what it reads.
#[derive(Clone, Copy, Debug, Default)]
struct ValueBytes {
    read: u64,
    returned: u64,
}

impl ValueBytes {
    fn of(read: usize, returned: usize) -> Self {
        Self {
            read: approx::count(read),
            returned: approx::count(returned),
        }
    }

    fn plus(self, other: Self) -> Self {
        Self {
            read: self.read + other.read,
            returned: self.returned + other.returned,
        }
    }
}

/// The bytes `row` returned over the `columns` values it carries, or `None`
/// when its key was absent or it does not carry a value for each column:
/// a null, which no benchmark table holds, or a wrong count.
fn returned_bytes(row: &Row, columns: usize) -> Option<usize> {
    match row {
        Row::Present(values) if values.len() == columns => values
            .iter()
            .try_fold(0, |sum, value| Some(sum + value.as_ref()?.len())),
        Row::Present(_) | Row::Absent => None,
    }
}

/// Keys of a `MultiScan` scenario's batch.
const MULTI_SCAN_KEYS: usize = 10;

/// Keys of the `-100` scenarios' batch, `MultiGet` and `MultiScan`: a
/// batch of heads a caller pages through, spread over several workers'
/// runs.
const SMALL_BATCH_KEYS: usize = 100;

/// Keys of the `-1000` scenarios' batch, `MultiGet` and `MultiScan`, and
/// of `two-table`'s: ten times as many, the order of the most
/// `server.max_multi_scan_values` admits at a limit of one row.
const LARGE_BATCH_KEYS: usize = 1_000;

/// Rows per key of the `multi-scan-100` and `multi-scan-1000` scenarios:
/// each key's newest row.
const MULTI_SCAN_HEAD_ROWS: u32 = 1;

/// Rows of the bounded Scan's page: a page of a read paged by order value.
const SCAN_PAGE_ROWS: u32 = 10;

/// Operations of a Scan scenario whose answers are compared whole with
/// the model's before it is measured (`verify_scans`).
const VERIFIED_OPS: usize = 100;

/// Operations of a Scan scenario whose rounds of GETs are counted, one at
/// a time, before it is measured (`count_rounds`), after the
/// `VERIFIED_OPS` that `verify_scans` ran, so a cold scenario counts
/// operations whose pages those did not just read.
const ROUNDS_OPS: usize = 100;

/// What the default `max_scan_rows` is divided by for the head Scan of a
/// long run: a tenth of a full page.
const HEAD_DIVISOR: u32 = 10;

/// Rows of the head Scan of a long run: the newest hundred, a tenth of
/// the default `max_scan_rows`.
const HEAD_ROWS: u32 = DEFAULT_MAX_SCAN_ROWS / HEAD_DIVISOR;

/// Bounds a long-run Scan scenario draws its `before` from, one per
/// request in turn (`bounded_in_run`): enough that the bounds fall all
/// over the middle half of a run, and the operations of a run cycle
/// through them.
const BOUND_POOL: usize = 1_024;

/// The seed of the long-run Scans' bounds, fixed so every run asks the
/// same requests.
const BOUND_SEED: u64 = 0x1ce7_4a7b_e4c4_0002;

/// A Scan scenario's request and the model's answer to it as order values
/// (`scan_model`), which is every key's answer: every key of an ordered
/// table the Scan scenarios read holds the same order values.
struct ScanCase {
    /// The request in raw order values, as the model takes it.
    test: TestScan,
    /// The request as the table takes it.
    bounds: ScanBounds,
    /// The order value of each row the model returns, in order.
    orders: Vec<OrderValue>,
    /// The next page's bound the model answers, present when live rows
    /// remain inside the bounds past the last.
    next: Option<OrderValue>,
}

impl ScanCase {
    /// The case of `test` over `shape`, which must be an ordered table.
    fn of(shape: TableShape, test: TestScan) -> BenchResult<Self> {
        let ordered = shape.order()?;
        let rows = ordered.orders_of(0)?.into_iter().map(|order| (order, ()));
        let encode = |order| order_value(ordered.kind, order);
        let model = scanned_of(scan_model(rows, &test), &test, encode, |_, ()| Vec::new());
        Ok(Self::of_scanned(test, test.bounds(ordered.kind), &model))
    }

    /// The case of `test`, asked as `bounds`, whose answer by the model is
    /// `model`: its order values and its `next`.
    fn of_scanned(test: TestScan, bounds: ScanBounds, model: &Scanned) -> Self {
        Self {
            test,
            bounds,
            orders: model.rows.iter().map(|row| row.order.clone()).collect(),
            next: model.next.clone(),
        }
    }

    /// The cases of `tests` over `shape`, which operation `i` takes in
    /// turn (`Op::case`).
    fn pool(shape: TableShape, tests: &[TestScan]) -> BenchResult<Arc<Vec<Self>>> {
        Ok(Arc::new(
            tests
                .iter()
                .map(|&test| Self::of(shape, test))
                .collect::<BenchResult<_>>()?,
        ))
    }

    /// The value bytes `scanned` returned, or what in it disagrees with the
    /// model: another `next`, another row count or order value, or a row
    /// without one value of `value_bytes` bytes. Allocates nothing when it
    /// agrees, so the check costs a measured operation no allocation.
    fn check(&self, scanned: &Scanned, value_bytes: usize) -> Result<usize, String> {
        if scanned.next != self.next || scanned.rows.len() != self.orders.len() {
            return Err(format!(
                "{} rows, next {:?}, where the model has {} rows, next {:?}",
                scanned.rows.len(),
                scanned.next,
                self.orders.len(),
                self.next
            ));
        }
        let mut returned = 0;
        for (row, order) in scanned.rows.iter().zip(&self.orders) {
            if row.order != *order {
                return Err(format!(
                    "a row at {} where the model has {order}",
                    row.order
                ));
            }
            match row.values.as_slice() {
                [Some(value)] if value.len() == value_bytes => returned += value.len(),
                values => return Err(format!("the row at {order} carries {values:?}")),
            }
        }
        Ok(returned)
    }
}

/// What a Scan of `shape`'s key `id` under `test` answers by the model:
/// `scan_model` over the rows the fixture writer wrote for the key, each
/// its order value and its `VALUE_COLUMN` value (`row_value` at
/// `FIRST_VERSION`).
fn modeled_scan(
    shape: TableShape,
    ordered: Ordered,
    id: u64,
    test: &TestScan,
) -> BenchResult<Scanned> {
    let key = shape.key(id);
    let rows = ordered.orders_of(id)?.into_iter().map(|order| {
        let value = row_value(
            &key,
            order,
            shape.value_bytes,
            shape.values.kind(),
            FIRST_VERSION,
        );
        (order, value)
    });
    Ok(scanned_of(
        scan_model(rows, test),
        test,
        |order| order_value(ordered.kind, order),
        |_, value| vec![Some(value.into())],
    ))
}

/// What one operation of a run does.
#[derive(Clone)]
enum Op {
    /// One `get` of the next pool key reading `reads`, which must answer
    /// present with a value per column, or absent when `present` is false.
    Get {
        keys: Arc<Vec<Key>>,
        present: bool,
        reads: Reads,
    },
    /// One `multi_get` of the next `batch` pool keys reading `reads`, all
    /// present with a value per column.
    MultiGet {
        keys: Arc<Vec<Key>>,
        batch: usize,
        reads: Reads,
    },
    /// One `scan` of the next pool key reading `VALUE_COLUMN` under the
    /// bounds of the operation's case (`Op::case`), which must return the
    /// model's rows, each with a value of `value_bytes` bytes
    /// (`ScanCase::check`).
    Scan {
        keys: Arc<Vec<Key>>,
        cases: Arc<Vec<ScanCase>>,
        value_bytes: usize,
    },
    /// One `multi_scan` of the next `batch` pool keys under the
    /// operation's case, each answered as `Scan` requires.
    MultiScan {
        keys: Arc<Vec<Key>>,
        batch: usize,
        cases: Arc<Vec<ScanCase>>,
        value_bytes: usize,
    },
    /// The next of `requests`, a `scan` or a `multi_scan` of a reference
    /// table reading `VALUE_COLUMN`, each key answering its own case as
    /// `Scan` requires; before it, out of its timing, the page cache
    /// `cold` emptied (`Op::before`, `PageCache::clear`), so it reads every
    /// page from where the tier keeps it.
    Reference {
        requests: Arc<Vec<Request>>,
        value_bytes: usize,
        cold: Option<PageCache>,
    },
}

/// One request of a reference op: the keys it asks, by key index and as
/// keys, whether it is a `MultiScan`, and per key its case and the
/// derivation's whole answer (`golden::derived_scan`), every case under the
/// request's one set of bounds.
struct Request {
    ids: Vec<u64>,
    keys: Vec<Key>,
    multi: bool,
    cases: Vec<ScanCase>,
    wants: Vec<Scanned>,
}

impl Request {
    /// The bounds every key of the request is read under.
    fn bounds(&self) -> &ScanBounds {
        &self.cases[0].bounds
    }
}

/// A `scan` of `keys`' one key, or with `multi` a `multi_scan` of them,
/// under `bounds` as operation `i`, each key's answer checked against its
/// case of `cases` in turn (`ScanCase::check`); the value bytes it read and
/// returned. Allocates nothing past the read's own answer when every key
/// agrees, so the check costs a measured operation no allocation.
async fn checked_scan<'a>(
    table: &Table,
    (keys, multi, bounds): (&[Key], bool, &ScanBounds),
    cases: impl Iterator<Item = &'a ScanCase>,
    value_bytes: usize,
    i: usize,
) -> BenchResult<ValueBytes> {
    let (single, batch);
    let (answers, what) = if multi {
        batch = table.multi_scan(keys, &VALUE_READS, bounds).await?;
        (batch.results.as_slice(), "batch")
    } else {
        single = table.scan(&keys[0], &VALUE_READS, bounds).await?;
        (std::slice::from_ref(&single.scanned), "scan")
    };
    if answers.len() != keys.len() {
        return Err(format!(
            "{what} {i} answered {} of {} keys",
            answers.len(),
            keys.len()
        )
        .into());
    }
    let mut returned = 0;
    for (scanned, case) in answers.iter().zip(cases) {
        returned += case
            .check(scanned, value_bytes)
            .map_err(|fault| format!("{what} {i}: {fault}"))?;
    }
    Ok(ValueBytes::of(returned, returned))
}

impl Op {
    /// The positions in the key pool that operation `i` asks: the next
    /// key, or the next batch, which wraps before it would run past the
    /// pool's end.
    fn pool_range(&self, i: usize) -> std::ops::Range<usize> {
        match self {
            Self::Get { keys, .. } | Self::Scan { keys, .. } => {
                let at = i % keys.len();
                at..at + 1
            }
            Self::Reference { requests, .. } => {
                let at = i % requests.len();
                at..at + 1
            }
            Self::MultiGet { keys, batch, .. } | Self::MultiScan { keys, batch, .. } => {
                let start = (i * batch) % (keys.len() - batch);
                start..start + batch
            }
        }
    }

    /// The case operation `i` of a Scan op asks, its pool's cases in turn.
    fn case(cases: &[ScanCase], i: usize) -> &ScanCase {
        &cases[i % cases.len()]
    }

    /// Lookups per operation: one, or a batch's keys.
    fn keys_per_op(&self) -> usize {
        match self {
            Self::Get { .. } | Self::Scan { .. } => 1,
            Self::MultiGet { batch, .. } | Self::MultiScan { batch, .. } => *batch,
            Self::Reference { requests, .. } => requests[0].keys.len(),
        }
    }

    /// What runs before each operation, outside its timing: a cold
    /// reference op's page cache emptied; nothing for any other op.
    fn before(&self) {
        match self {
            Self::Reference {
                cold: Some(cold), ..
            } => {
                cold.clear();
            }
            Self::Reference { cold: None, .. }
            | Self::Get { .. }
            | Self::MultiGet { .. }
            | Self::Scan { .. }
            | Self::MultiScan { .. } => {}
        }
    }

    /// Runs operation `i` and returns the value bytes it read and
    /// returned; an answer of the wrong presence, or a Scan's answer that
    /// disagrees with the model, is an error, so a run that measured
    /// nothing but failures cannot pass for fast.
    async fn run(&self, table: &Table, i: usize) -> BenchResult<ValueBytes> {
        let at = self.pool_range(i);
        match self {
            Self::Get {
                keys,
                present,
                reads,
            } => {
                let lookup = table.get(&keys[at.start], reads.columns()).await?;
                let returned = returned_bytes(&lookup.row, reads.columns().len());
                if returned.is_some() != *present {
                    return Err(format!("key {i} answered {:?}", lookup.row).into());
                }
                let returned = returned.unwrap_or(0);
                Ok(ValueBytes::of(reads.read_bytes(at, returned), returned))
            }
            Self::MultiGet { keys, reads, .. } => {
                let lookup = table.multi_get(&keys[at.clone()], reads.columns()).await?;
                let mut returned = 0;
                for row in &lookup.rows {
                    returned += returned_bytes(row, reads.columns().len())
                        .ok_or_else(|| format!("batch {i} answered a key absent"))?;
                }
                Ok(ValueBytes::of(reads.read_bytes(at, returned), returned))
            }
            Self::Scan {
                keys,
                cases,
                value_bytes,
            } => {
                let case = Self::case(cases, i);
                let asked = (&keys[at], false, &case.bounds);
                checked_scan(table, asked, std::iter::once(case), *value_bytes, i).await
            }
            Self::MultiScan {
                keys,
                cases,
                value_bytes,
                ..
            } => {
                let case = Self::case(cases, i);
                let repeated = std::iter::repeat_n(case, at.len());
                let asked = (&keys[at], true, &case.bounds);
                checked_scan(table, asked, repeated, *value_bytes, i).await
            }
            Self::Reference {
                requests,
                value_bytes,
                ..
            } => {
                let request = &requests[at.start];
                let asked = (request.keys.as_slice(), request.multi, request.bounds());
                checked_scan(table, asked, request.cases.iter(), *value_bytes, i).await
            }
        }
    }

    /// What operation `i` of a Scan op asks, for its verification
    /// (`verify_answers`): its keys, whether as one `MultiScan`, and the
    /// bounds every key is read under; `None` for a `Get` or `MultiGet` op.
    fn scan_request(&self, i: usize) -> Option<(&[Key], bool, &ScanBounds)> {
        let at = self.pool_range(i);
        match self {
            Self::Scan { keys, cases, .. } => {
                Some((&keys[at], false, &Self::case(cases, i).bounds))
            }
            Self::MultiScan { keys, cases, .. } => {
                Some((&keys[at], true, &Self::case(cases, i).bounds))
            }
            Self::Reference { requests, .. } => {
                let request = &requests[at.start];
                Some((&request.keys, request.multi, request.bounds()))
            }
            Self::Get { .. } | Self::MultiGet { .. } => None,
        }
    }
}

/// The first `ops` operations of a Scan, `MultiScan`, or reference op,
/// each key's answer compared whole, every value's bytes included, with
/// `want(i, at)`, which gives the key index and model answer of the `at`th
/// key of operation `i`, before the scenario is measured: `Op::run` checks
/// the order values and value lengths alone, so this is what fails a run
/// whose Scans return wrong bytes. Each operation is read as one `MultiScan` and
/// its first key as a Scan too, after the op's `before`, and the two must
/// agree. A `Get` or `MultiGet` op is checked by `Op::run` alone.
async fn verify_answers(
    op: &Op,
    table: &Table,
    ops: usize,
    want: impl Fn(usize, usize) -> BenchResult<(u64, Scanned)>,
) -> BenchResult<()> {
    for i in 0..ops {
        let Some((keys, _, bounds)) = op.scan_request(i) else {
            return Ok(());
        };
        op.before();
        let answers = table.multi_scan(keys, &VALUE_READS, bounds).await?.results;
        let single = table.scan(&keys[0], &VALUE_READS, bounds).await?.scanned;
        if answers.len() != keys.len() {
            return Err(format!(
                "operation {i} answered {} of {} keys",
                answers.len(),
                keys.len()
            )
            .into());
        }
        if single != answers[0] {
            return Err(
                format!("operation {i}: a Scan and a MultiScan of one key disagree").into(),
            );
        }
        for (at, answer) in answers.iter().enumerate() {
            let (id, want) = want(i, at)?;
            if *answer != want {
                return Err(format!(
                    "operation {i}: key index {id} under {bounds:?} disagrees with the model"
                )
                .into());
            }
        }
    }
    Ok(())
}

/// `verify_answers` of the first `VERIFIED_OPS` operations of a Scan or
/// `MultiScan` op over `shape`, against the model's answers
/// (`modeled_scan`).
async fn verify_scans(op: &Op, table: &Table, shape: TableShape) -> BenchResult<()> {
    let cases = match op {
        Op::Scan { cases, .. } | Op::MultiScan { cases, .. } => cases,
        Op::Get { .. } | Op::MultiGet { .. } | Op::Reference { .. } => return Ok(()),
    };
    let ordered = shape.order()?;
    let ids = hit_ids(shape);
    verify_answers(op, table, VERIFIED_OPS, |i, at| {
        let id = ids[op.pool_range(i)][at];
        Ok((
            id,
            modeled_scan(shape, ordered, id, &Op::case(cases, i).test)?,
        ))
    })
    .await
}

/// Whether a task loop gives its worker up between operations.
#[derive(Clone, Copy, Debug)]
enum Pace {
    /// One operation straight after the other. A warm lookup never waits,
    /// so the loop holds its worker until the run ends.
    Straight,
    /// A yield to the scheduler after each operation, untimed, as a
    /// server's requests end their tasks: other tasks queued on the worker
    /// run between operations.
    Yielding,
}

/// What a run's task loops did: the operations, their samples when
/// recorded, and the value bytes they read and returned.
type Done = (usize, Vec<Duration>, ValueBytes);

/// One task's share of a run whose operations start at `first`:
/// operations `first + task`, `first + task + tasks`, ... until `until`,
/// each timed into `samples` when it is given.
async fn task_loop(
    table: Arc<Table>,
    op: Op,
    (first, task, tasks): (usize, usize, usize),
    pace: Pace,
    until: Instant,
    mut samples: Option<Vec<Duration>>,
) -> BenchResult<Done> {
    let mut done = 0;
    let mut bytes = ValueBytes::default();
    while Instant::now() < until {
        let i = first + done * tasks + task;
        op.before();
        let started = Instant::now();
        let op_bytes = op.run(&table, i).await?;
        if let Some(samples) = samples.as_mut() {
            samples.push(started.elapsed());
        }
        bytes = bytes.plus(op_bytes);
        done += 1;
        if let Pace::Yielding = pace {
            tokio::task::yield_now().await;
        }
    }
    Ok((done, samples.unwrap_or_default(), bytes))
}

/// The task loops of a run, as they are spawned.
type Loops = Vec<tokio::task::JoinHandle<BenchResult<Done>>>;

/// `tasks` task loops of `op` over `table` at `pace` from operation
/// `first` until `until`, each reserving `reserve` samples when
/// recording, spawned and not yet joined.
fn spawn_loops(
    table: &Arc<Table>,
    (op, pace): (&Op, Pace),
    (first, tasks): (usize, usize),
    until: Instant,
    reserve: Option<usize>,
) -> Loops {
    (0..tasks)
        .map(|task| {
            tokio::spawn(task_loop(
                Arc::clone(table),
                op.clone(),
                (first, task, tasks),
                pace,
                until,
                reserve.map(Vec::with_capacity),
            ))
        })
        .collect()
}

/// `tasks` task loops from operation `first` for `length`, each
/// reserving `reserve` samples when recording; the operations done, the
/// samples, and the value bytes, merged.
async fn spawn_run(
    served: &Served,
    op: &Op,
    (first, tasks): (usize, usize),
    length: Duration,
    reserve: Option<usize>,
) -> BenchResult<Done> {
    let until = Instant::now() + length;
    join_loops(spawn_loops(
        &served.table,
        (op, Pace::Straight),
        (first, tasks),
        until,
        reserve,
    ))
    .await
}

/// Waits for every loop of `handles`; the operations they did, their
/// samples, and their value bytes, merged.
async fn join_loops(handles: Loops) -> BenchResult<Done> {
    let mut done = 0;
    let mut samples = Vec::new();
    let mut bytes = ValueBytes::default();
    for handle in handles {
        let (ops, mut task_samples, task_bytes) = handle.await??;
        done += ops;
        samples.append(&mut task_samples);
        bytes = bytes.plus(task_bytes);
    }
    Ok((done, samples, bytes))
}

/// Counters of a served table and the process, read before and after a
/// measured window; page fetches and hits are the page cache's.
#[derive(Clone, Copy, Debug, Default)]
struct Counters {
    allocations: Allocations,
    gets: u64,
    /// The bytes those GETs requested.
    get_bytes: u64,
    local_reads: u64,
    /// The bytes those local reads read.
    local_bytes: u64,
    hedges: u64,
    fetches: u64,
    hits: u64,
}

impl Counters {
    fn read(served: &Served) -> Self {
        let stats = served.storage.stats();
        Self {
            allocations: Allocations::now(),
            gets: stats.gets(),
            get_bytes: stats.bytes(),
            local_reads: stats.local_reads(),
            local_bytes: stats.local_bytes(),
            hedges: stats.hedges(),
            fetches: served.cache.fetches(),
            hits: served.cache.hits(),
        }
    }

    fn since(self, earlier: Self) -> Self {
        Self {
            allocations: self.allocations.since(earlier.allocations),
            gets: self.gets - earlier.gets,
            get_bytes: self.get_bytes - earlier.get_bytes,
            local_reads: self.local_reads - earlier.local_reads,
            local_bytes: self.local_bytes - earlier.local_bytes,
            hedges: self.hedges - earlier.hedges,
            fetches: self.fetches - earlier.fetches,
            hits: self.hits - earlier.hits,
        }
    }
}

/// The field of the load's debug event for the ordered uniqueness merge
/// that `HeldBytes` reads: the most bytes of chosen pages the merge held
/// and had in flight at once.
const HELD_FIELD: &str = "held_bytes";

/// The largest `HELD_FIELD` reported since `HeldBytes::take`.
static HELD_BYTES: AtomicU64 = AtomicU64::new(0);

/// Whether any merge reported `HELD_FIELD` since `HeldBytes::take`.
static HELD_SEEN: AtomicBool = AtomicBool::new(false);

/// A subscriber interested only in events that carry `HELD_FIELD`, so every
/// other callsite of the library stays disabled, as with no subscriber.
struct HeldBytes;

impl HeldBytes {
    fn reads(metadata: &tracing::Metadata<'_>) -> bool {
        metadata.is_event() && metadata.fields().field(HELD_FIELD).is_some()
    }

    /// The largest `HELD_FIELD` reported since the last take, `None` when
    /// none was, and starts over.
    fn take() -> Option<u64> {
        HELD_SEEN
            .swap(false, Ordering::Relaxed)
            .then(|| HELD_BYTES.swap(0, Ordering::Relaxed))
    }
}

/// Reads `HELD_FIELD` from an event's fields.
struct HeldField(Option<u64>);

impl tracing::field::Visit for HeldField {
    fn record_u64(&mut self, field: &tracing::field::Field, value: u64) {
        if field.name() == HELD_FIELD {
            self.0 = Some(value);
        }
    }

    fn record_debug(&mut self, _: &tracing::field::Field, _: &dyn std::fmt::Debug) {}
}

impl tracing::Subscriber for HeldBytes {
    fn register_callsite(
        &self,
        metadata: &'static tracing::Metadata<'static>,
    ) -> tracing::subscriber::Interest {
        if Self::reads(metadata) {
            tracing::subscriber::Interest::always()
        } else {
            tracing::subscriber::Interest::never()
        }
    }

    fn enabled(&self, metadata: &tracing::Metadata<'_>) -> bool {
        Self::reads(metadata)
    }

    fn new_span(&self, _: &tracing::span::Attributes<'_>) -> tracing::span::Id {
        tracing::span::Id::from_u64(1)
    }

    fn record(&self, _: &tracing::span::Id, _: &tracing::span::Record<'_>) {}

    fn record_follows_from(&self, _: &tracing::span::Id, _: &tracing::span::Id) {}

    fn event(&self, event: &tracing::Event<'_>) {
        let mut held = HeldField(None);
        event.record(&mut held);
        if let Some(bytes) = held.0 {
            HELD_BYTES.fetch_max(bytes, Ordering::Relaxed);
            HELD_SEEN.store(true, Ordering::Relaxed);
        }
    }

    fn enter(&self, _: &tracing::span::Id) {}

    fn exit(&self, _: &tracing::span::Id) {}
}

/// What a measured run is of.
#[derive(Clone, Copy, Debug)]
struct RunLabel {
    scenario: Scenario,
    table: TableShape,
    tier: Tier,
    mode: Mode,
    tasks: usize,
    /// The bound its tables were served under, `Serving::batch_fan_out`.
    batch_fan_out: usize,
    /// The table its process also served, idle, under `kv-beside-ordered`.
    beside: Option<TableShape>,
}

impl RunLabel {
    /// The measured table's name, followed after `BESIDE_SEPARATOR` by the
    /// name of the table served beside it.
    fn table_label(&self) -> String {
        match self.beside {
            Some(beside) => format!(
                "{}{BESIDE_SEPARATOR}{}",
                self.table.table_name(),
                beside.table_name()
            ),
            None => self.table.table_name(),
        }
    }
}

/// What separates a measured table's name from the one served beside it.
const BESIDE_SEPARATOR: &str = "+";

/// What one measured window of a run's operations did.
struct Window {
    ops: usize,
    elapsed: Duration,
    /// Every operation's latency.
    latencies: Latencies,
    counters: Counters,
    /// The value bytes the operations read and returned.
    bytes: ValueBytes,
}

/// One second, the unit of the throughput figure.
const SECOND: Duration = Duration::from_secs(1);

impl Window {
    fn ops_per_second(&self) -> f64 {
        approx::gauge(approx::count(self.ops)) * SECOND.div_duration_f64(self.elapsed)
    }

    fn per_op(&self, total: u64) -> f64 {
        approx::ratio(total, approx::count(self.ops))
    }

    /// The window's figures as a result row's JSON fields.
    fn to_json(&self) -> Json {
        let counters = self.counters;
        let fields = json!({
            "ops": self.ops,
            "elapsed_us": micros(self.elapsed),
            "ops_per_second": self.ops_per_second(),
            "allocations_per_op": self.per_op(counters.allocations.calls),
            "allocated_bytes_per_op": self.per_op(counters.allocations.bytes),
            "page_fetches_per_op": self.per_op(counters.fetches),
            "page_cache_hits_per_op": self.per_op(counters.hits),
            "gets_per_op": self.per_op(counters.gets),
            "get_bytes_per_op": self.per_op(counters.get_bytes),
            "local_reads_per_op": self.per_op(counters.local_reads),
            "local_bytes_per_op": self.per_op(counters.local_bytes),
            "hedges": counters.hedges,
            "value_bytes_read_per_op": self.per_op(self.bytes.read),
            "value_bytes_returned_per_op": self.per_op(self.bytes.returned),
        });
        merged(fields, [percentiles_json(&self.latencies)])
    }
}

/// A measured run.
struct Measured {
    label: RunLabel,
    /// Lookups per operation: one, or a `MultiGet`'s batch.
    keys_per_op: usize,
    window: Window,
    /// The rounds of GETs an operation waits on, counted apart from the
    /// window (`count_rounds`); `None` where they are not counted.
    rounds: Option<Rounds>,
}

impl Measured {
    /// `run` of an op of `keys_per_op` keys, labeled `label` with its mode
    /// and tasks.
    fn of(label: RunLabel, keys_per_op: usize, run: ModeRun) -> Self {
        Self {
            label: RunLabel {
                mode: run.mode,
                tasks: run.tasks,
                ..label
            },
            keys_per_op,
            window: run.window,
            rounds: run.rounds,
        }
    }

    fn to_json(&self) -> Json {
        merged(
            json!({
                "scenario": self.label.scenario.name(),
                "table": self.label.table.name.as_str(),
                "values": self.label.table.values.as_str(),
                "tier": self.label.tier.name(),
                "mode": self.label.mode.name(),
                "tasks": self.label.tasks,
                "batch_fan_out": self.label.batch_fan_out,
                "beside": self.label.beside.map(TableShape::table_name),
                "keys_per_op": self.keys_per_op,
                "row_cache": serving().row_cache,
            }),
            [self.window.to_json(), rounds_json(self.rounds)],
        )
    }
}

/// `first`'s fields and then each of `rest`'s, every one a JSON object; a
/// later field of one name replaces an earlier.
fn merged(mut first: Json, rest: impl IntoIterator<Item = Json>) -> Json {
    if let Json::Object(fields) = &mut first {
        for other in rest {
            if let Json::Object(more) = other {
                fields.extend(more);
            }
        }
    }
    first
}

/// The JSON fields of an operation's rounds of GETs, null where they are
/// not counted.
fn rounds_json(rounds: Option<Rounds>) -> Json {
    json!({
        "rounds_per_op": rounds.map(Rounds::per_op),
        "rounds_max": rounds.map(|rounds| rounds.most),
        "rounds_ops": rounds.map(|rounds| rounds.ops),
    })
}

/// Runs `op` over `served` with `tasks` tasks for the warm-up and then for
/// the measured window, the samples of the window reserved from the
/// warm-up's rate before the counters are first read, so recording
/// allocates nothing inside the window. The warm-up's operations start at
/// `first` and the window's follow them, so neither asks again the keys
/// an earlier run of the op just asked, which a cache smaller than the
/// table would still hold. Returns the window and the operation after its
/// last. A GET log over the served store (`Served::gets`) starts anew with
/// the window's counters.
async fn measure_window(
    served: &Served,
    op: &Op,
    (first, tasks): (usize, usize),
    settings: &RunSettings,
) -> BenchResult<(Window, usize)> {
    let (warm_ops, _, _) = spawn_run(served, op, (first, tasks), settings.warmup, None).await?;
    let windows = approx::length(settings.windows);
    let reserve = (warm_ops / tasks + 1) * windows * SAMPLE_HEADROOM;
    if let Some(log) = &served.gets {
        log.restart();
    }
    let before = Counters::read(served);
    let started = Instant::now();
    let (ops, samples, bytes) = spawn_run(
        served,
        op,
        (first + warm_ops, tasks),
        settings.duration,
        Some(reserve),
    )
    .await?;
    let elapsed = started.elapsed();
    let counters = Counters::read(served).since(before);
    let window = Window {
        ops,
        elapsed,
        latencies: Latencies::of(samples),
        counters,
        bytes,
    };
    Ok((window, first + warm_ops + ops))
}

/// Asks every page of the table, of every column it serves, at least once,
/// `prime_stride` ids apart, so the warm cache holds every value page (and
/// under Bloom every key page) before a warm run starts: a Get of each key
/// of a key-value table, and of an ordered table Scans of each key's whole
/// run, paged by order value as a caller pages a run longer than
/// `max_scan_rows`, which fill the page cache. A table that reads through
/// the row cache is asked every id, so the row cache holds every row: its
/// page cache part need not hold every value page.
async fn prime_cache(table: &Table, shape: TableShape) -> BenchResult<()> {
    let stride = if table.config().row_cache() {
        EVERY_ID_STRIDE
    } else {
        shape.prime_stride()
    };
    for id in (0..shape.rows).step_by(approx::length(stride)) {
        let key = shape.key(id);
        match shape.ordered {
            Some(_) => page_through(table, &key, shape.whole_reads()).await?,
            None => {
                table.get(&key, shape.whole_reads()).await?;
            }
        }
    }
    Ok(())
}

/// Scans every row of `key` in `table` reading `reads`, newest first at
/// the table's `max_scan_rows`, each page from the last one's `next`, as a
/// caller pages a run longer than one Scan returns.
async fn page_through(table: &Table, key: &Key, reads: &[ColumnRead<'static>]) -> BenchResult<()> {
    let mut page = ScanBounds {
        direction: Direction::NewestFirst,
        limit: table
            .config()
            .scan_rows()
            .ok_or("an ordered table has a max_scan_rows")?,
        start: None,
        before: None,
    };
    loop {
        let scanned = table.scan(key, reads, &page).await?.scanned;
        match scanned.next {
            Some(next) => follow(&mut page, next),
            None => return Ok(()),
        }
    }
}

/// Per key of the hit pool (`hit_ids`), the bytes of its whole
/// `VALUE_COLUMN` value as the first build wrote it (`value_of_kind`), which
/// a whole read of it returns: what a projection of that key reads. Each
/// row's value is built once, outside any measured window.
fn whole_lengths(shape: TableShape) -> Arc<Vec<usize>> {
    let by_id: Vec<usize> = (0..shape.rows)
        .map(|id| value_of_kind(&shape.key(id), shape.value_bytes, shape.values.kind()).len())
        .collect();
    Arc::new(
        hit_ids(shape)
            .into_iter()
            .map(|id| by_id[approx::length(id)])
            .collect(),
    )
}

/// Settings every lookup run shares.
struct RunSettings {
    modes: Vec<Mode>,
    tasks: usize,
    warmup: Duration,
    duration: Duration,
    /// `duration` over `warmup`, rounded up: how many warm-ups long the
    /// measured window is, which sizes its sample reservation.
    windows: u64,
}

/// One table served in one tier under each cache budget its selected
/// scenarios measure, loaded; `None` for a budget none of them does.
struct Budgets {
    /// Under `WARM_CACHE_BYTES`, primed (`prime_cache`), for the warm
    /// scenarios and the misses.
    warm: Option<Served>,
    /// Under `COLD_CACHE_BYTES`, for the cold scenarios.
    cold: Option<Served>,
    /// Under `SKEWED_CACHE_BYTES`, unfilled: the skewed scenario fills it
    /// with its own keys.
    skewed: Option<Served>,
}

impl Budgets {
    /// `prepared` beside `beside` in `tier` under each budget `scenarios`
    /// measure (`serve_loaded`).
    async fn serve(
        prepared: &Prepared,
        beside: Option<&Prepared>,
        tier: Tier,
        scenarios: &[Scenario],
    ) -> BenchResult<Self> {
        let uses = |cache: Cache| scenarios.iter().any(|s| s.traits().cache == cache);
        let serve_if = |wanted: bool, cache_bytes| {
            OptionFuture::from(wanted.then(|| serve_loaded(prepared, beside, tier, cache_bytes)))
        };
        let warm = serve_if(uses(Cache::Warm) || uses(Cache::Unread), WARM_CACHE_BYTES)
            .await
            .transpose()?;
        if let Some(served) = &warm {
            prime_cache(&served.table, prepared.shape).await?;
        }
        let cold = serve_if(uses(Cache::Cold), COLD_CACHE_BYTES)
            .await
            .transpose()?;
        let skewed = serve_if(uses(Cache::Skewed), SKEWED_CACHE_BYTES)
            .await
            .transpose()?;
        Ok(Self { warm, cold, skewed })
    }

    /// The table served under `cache`, which a selected scenario measures.
    fn of(&self, cache: Cache) -> &Served {
        match cache {
            Cache::Warm | Cache::Unread => &self.warm,
            Cache::Cold => &self.cold,
            Cache::Skewed => &self.skewed,
        }
        .as_ref()
        .expect("a table is served for every selected scenario")
    }
}

/// Every selected lookup scenario of one table in one tier, each in every
/// selected mode, over a warm table, for the cold scenarios a cold one,
/// and for the skewed scenario one filled by a pass of its own keys
/// (`fill_from`); each served beside `beside` when given (`serve`). A
/// Scan scenario is checked against the model whole (`verify_scans`)
/// before it is measured.
async fn lookups(
    prepared: &Prepared,
    beside: Option<&Prepared>,
    tier: Tier,
    scenarios: &[Scenario],
    settings: &RunSettings,
) -> BenchResult<Vec<Measured>> {
    let shape = prepared.shape;
    let (hits, misses) = key_pools(shape);
    let budgets = Budgets::serve(prepared, beside, tier, scenarios).await?;
    let whole = scenarios
        .iter()
        .any(|s| s.traits().projects)
        .then(|| whole_lengths(shape));
    let projected = |columns| Reads::Projected {
        columns,
        whole: Arc::clone(
            whole
                .as_ref()
                .expect("whole lengths are read when a projecting scenario is selected"),
        ),
    };
    let scan_pool =
        |tests: &[TestScan]| ScanCase::pool(shape, tests).map(|cases| scan(&hits, cases, shape));
    let scan_one = |test| scan_pool(&[test]);
    let scan_batch = |test, keys| {
        ScanCase::pool(shape, &[test]).map(|cases| multi_scan(&hits, keys, cases, shape))
    };
    let mut results = Vec::new();
    for &scenario in scenarios {
        let op = match scenario {
            Scenario::WarmHit | Scenario::ColdHit => get(&hits, true, WHOLE_VALUE),
            Scenario::SkewedHit => {
                let (fill, measured) = skewed_pools(shape);
                let served = budgets.of(Cache::Skewed);
                fill_from(
                    served,
                    &get(&fill, true, WHOLE_VALUE),
                    settings.tasks,
                    KEY_POOL,
                )
                .await?;
                get(&measured, true, WHOLE_VALUE)
            }
            Scenario::Miss => get(&misses, false, WHOLE_VALUE),
            Scenario::MultiGet100 | Scenario::MultiGet100Cold => {
                multi(&hits, SMALL_BATCH_KEYS, WHOLE_VALUE)
            }
            Scenario::MultiGet1000 => multi(&hits, LARGE_BATCH_KEYS, WHOLE_VALUE),
            Scenario::WarmHitBoth | Scenario::ColdHitBoth => {
                get(&hits, true, Reads::Whole(&BOTH_READS))
            }
            Scenario::PresenceHit => get(&hits, true, Reads::Whole(&NO_READS)),
            Scenario::WarmHitProjectFew | Scenario::ColdHitProjectFew => {
                get(&hits, true, projected(&FEW_READS))
            }
            Scenario::WarmHitProjectMost => get(&hits, true, projected(&MOST_READS)),
            Scenario::MultiGet100Both | Scenario::MultiGet100ColdBoth => {
                multi(&hits, SMALL_BATCH_KEYS, Reads::Whole(&BOTH_READS))
            }
            Scenario::MultiGet100Project => multi(&hits, SMALL_BATCH_KEYS, projected(&FEW_READS)),
            Scenario::ScanWarm | Scenario::ScanCold => scan_one(TestScan::default())?,
            Scenario::ScanBounded => scan_one(bounded_page()?)?,
            Scenario::MultiScan10 | Scenario::MultiScan10Cold => {
                scan_batch(multi_scan_whole_runs(), MULTI_SCAN_KEYS)?
            }
            Scenario::MultiScan100 | Scenario::MultiScan100Cold => {
                scan_batch(multi_scan_heads(), SMALL_BATCH_KEYS)?
            }
            Scenario::MultiScan1000 | Scenario::MultiScan1000Cold => {
                scan_batch(multi_scan_heads(), LARGE_BATCH_KEYS)?
            }
            Scenario::ScanBoundedLong
            | Scenario::ScanBoundedString
            | Scenario::ScanBoundedLongCold
            | Scenario::ScanBoundedStringCold => scan_pool(&bounded_in_run(shape)?)?,
            Scenario::ScanHeadLong => scan_one(head_of_run())?,
            Scenario::Load | Scenario::Refresh | Scenario::TwoTable | Scenario::KvBesideOrdered => {
                continue;
            }
        };
        let served = budgets.of(scenario.traits().cache);
        verify_scans(&op, &served.table, shape).await?;
        let rounds = count_rounds(served, &op).await?;
        let label = RunLabel {
            scenario,
            table: shape,
            tier,
            mode: Mode::Single,
            tasks: 1,
            batch_fan_out: serving().batch_fan_out,
            beside: beside.map(|other| other.shape),
        };
        for run in measure_modes(served, &op, rounds, settings).await? {
            let measured = Measured::of(label, op.keys_per_op(), run);
            eprintln!("{}", row(&measured));
            results.push(measured);
        }
    }
    Ok(results)
}

/// The rounds of GETs a Scan op's operations wait on, from
/// `served.rounds`: `ROUNDS_OPS` operations after the `VERIFIED_OPS` that
/// `verify_scans` ran, one at a time, each counted on its own after what
/// `Op::before` does. `None` for a `Get` or `MultiGet` op, for a table
/// served without the store that counts them, and for a `MultiScan`,
/// a reference one included: its keys' reads run concurrently
/// over one count, so a GET one key issues after another key's GET
/// answered lands in a later round although it waits on nothing, and the
/// count is no key's dependency depth.
async fn count_rounds(served: &Served, op: &Op) -> BenchResult<Option<Rounds>> {
    let one_key = match op {
        Op::Scan { .. } => true,
        Op::Reference { requests, .. } => !requests[0].multi,
        Op::Get { .. } | Op::MultiGet { .. } | Op::MultiScan { .. } => false,
    };
    let (Some(store), true) = (&served.rounds, one_key) else {
        return Ok(None);
    };
    let mut rounds = Rounds::default();
    for i in VERIFIED_OPS..VERIFIED_OPS + ROUNDS_OPS {
        op.before();
        store.restart();
        op.run(&served.table, i).await?;
        rounds.total += store.rounds();
        rounds.most = rounds.most.max(store.rounds());
    }
    rounds.ops = approx::count(ROUNDS_OPS);
    Ok(Some(rounds))
}

/// The rounds of GETs counted over a run's operations (`count_rounds`).
#[derive(Clone, Copy, Debug, Default)]
struct Rounds {
    ops: u64,
    total: u64,
    most: u64,
}

impl Rounds {
    fn per_op(self) -> f64 {
        approx::ratio(self.total, self.ops)
    }
}

/// `prepared` served beside `beside` in `tier` from its first build over
/// `cache_bytes` caches (`serve`), and loaded.
async fn serve_loaded(
    prepared: &Prepared,
    beside: Option<&Prepared>,
    tier: Tier,
    cache_bytes: u64,
) -> BenchResult<Served> {
    let served = serve(prepared, beside, tier, cache_bytes, pinned(prepared), None).await?;
    served.table.load().await?;
    Ok(served)
}

/// One mode of a measured op: the mode, its tasks, its window, the rounds
/// counted for it, and the GETs a log over the served store holds of its
/// window (`store_gets`).
struct ModeRun {
    mode: Mode,
    tasks: usize,
    window: Window,
    rounds: Option<Rounds>,
    gets: Option<Json>,
}

/// `op` over `served` measured in every selected mode, each mode's
/// operations following the last mode's (`measure_window`); the
/// single-task run carries `rounds`, counted one operation at a time as
/// that run runs them, and every run the GETs of its window when a log is
/// in place, with up to `GET_SAMPLES` of them one by one.
async fn measure_modes(
    served: &Served,
    op: &Op,
    rounds: Option<Rounds>,
    settings: &RunSettings,
) -> BenchResult<Vec<ModeRun>> {
    let pages = match &served.gets {
        Some(_) => Some(PageMap::of(&*served.table.serving()?)),
        None => None,
    };
    let mut runs = Vec::with_capacity(settings.modes.len());
    let mut next = 0;
    for &mode in &settings.modes {
        let (tasks, rounds) = match mode {
            Mode::Single => (1, rounds),
            Mode::Multi => (settings.tasks, None),
        };
        let (window, after) = measure_window(served, op, (next, tasks), settings).await?;
        next = after;
        let gets = served
            .gets
            .as_ref()
            .zip(pages.as_ref())
            .map(|(log, pages)| store_gets(log, pages, GET_SAMPLES));
        runs.push(ModeRun {
            mode,
            tasks,
            window,
            rounds,
            gets,
        });
    }
    Ok(runs)
}

/// The bounded Scan's request: `SCAN_PAGE_ROWS` rows, newest first, over
/// the middle half of a key's run (`middle_half`), both its edge rows
/// included, as the reference tables' window (`golden::reference_window`).
fn bounded_page() -> BenchResult<TestScan> {
    let orders = ORDERED.orders_of(0)?;
    let half = middle_half(orders.len());
    Ok(TestScan {
        direction: Direction::NewestFirst,
        limit: SCAN_PAGE_ROWS,
        start: Some(orders[*half.start()]),
        before: Some(orders[*half.end()] + 1),
    })
}

/// The long-run Scans' requests (`scan-bounded-long`,
/// `scan-bounded-string`): `SCAN_PAGE_ROWS` rows, newest first, before an
/// order value drawn uniformly from the middle half of a key's run of
/// `shape` (`middle_half`), `BOUND_POOL` of them, so a bound lands
/// anywhere in a run's pages.
fn bounded_in_run(shape: TableShape) -> BenchResult<Vec<TestScan>> {
    let ordered = shape.order()?;
    let orders = ordered.orders_of(0)?;
    let half = middle_half(orders.len());
    let mut rng = StdRng::seed_from_u64(BOUND_SEED);
    Ok((0..BOUND_POOL)
        .map(|_| TestScan {
            direction: Direction::NewestFirst,
            limit: SCAN_PAGE_ROWS,
            before: Some(orders[rng.random_range(half.clone())]),
            ..TestScan::default()
        })
        .collect())
}

/// The head Scan of a long run (`scan-head-long`): its `HEAD_ROWS` newest
/// rows, no bound.
fn head_of_run() -> TestScan {
    TestScan {
        direction: Direction::NewestFirst,
        limit: HEAD_ROWS,
        ..TestScan::default()
    }
}

/// The `MultiScan` scenarios' request: every row of each key, newest
/// first, at the largest limit `server.max_multi_scan_values` admits for
/// `MULTI_SCAN_KEYS` keys reading one column, each row counting its order
/// value beside the column's.
fn multi_scan_whole_runs() -> TestScan {
    let values_per_row = VALUE_READS.len() + 1;
    let limit = ServerConfig::default().max_multi_scan_values / (MULTI_SCAN_KEYS * values_per_row);
    TestScan {
        limit: u32::try_from(limit).expect("the default limit fits u32"),
        ..TestScan::default()
    }
}

/// The request of the `multi-scan-100` and `multi-scan-1000` scenarios:
/// each key's newest `MULTI_SCAN_HEAD_ROWS` rows.
fn multi_scan_heads() -> TestScan {
    TestScan {
        direction: Direction::NewestFirst,
        limit: MULTI_SCAN_HEAD_ROWS,
        ..TestScan::default()
    }
}

fn get(keys: &Arc<Vec<Key>>, present: bool, reads: Reads) -> Op {
    Op::Get {
        keys: Arc::clone(keys),
        present,
        reads,
    }
}

fn multi(keys: &Arc<Vec<Key>>, batch: usize, reads: Reads) -> Op {
    Op::MultiGet {
        keys: Arc::clone(keys),
        batch,
        reads,
    }
}

fn scan(keys: &Arc<Vec<Key>>, cases: Arc<Vec<ScanCase>>, shape: TableShape) -> Op {
    Op::Scan {
        keys: Arc::clone(keys),
        cases,
        value_bytes: shape.value_bytes,
    }
}

fn multi_scan(
    keys: &Arc<Vec<Key>>,
    batch: usize,
    cases: Arc<Vec<ScanCase>>,
    shape: TableShape,
) -> Op {
    Op::MultiScan {
        keys: Arc::clone(keys),
        batch,
        cases,
        value_bytes: shape.value_bytes,
    }
}

/// The table of random values named `name` among `prepared`.
fn random_table(prepared: &[Prepared], name: TableName) -> BenchResult<&Prepared> {
    prepared
        .iter()
        .find(|table| table.shape.name == name && table.shape.values == Values::Random)
        .ok_or_else(|| format!("no {} table of random values", name.as_str()).into())
}

/// The `kv-beside-ordered` scenario in `tier`: the key-value scenarios of
/// `Scenario::BESIDE_ORDERED` on the long table of random values, served
/// beside the ordered table over one storage, page cache, row cache, under
/// Disk one local directory, and one count of calls in flight, as
/// `open_with` opens a process serving both kinds. The ordered table is
/// loaded, primed, and idle, so a row against the same scenario on the long
/// table alone is what serving an ordered table beside it costs the
/// key-value path.
async fn kv_beside_ordered(
    prepared: &[Prepared],
    tier: Tier,
    settings: &RunSettings,
) -> BenchResult<Vec<Measured>> {
    let long = random_table(prepared, TableName::Long)?;
    let ordered = random_table(prepared, TableName::Ordered)?;
    lookups(
        long,
        Some(ordered),
        tier,
        &Scenario::BESIDE_ORDERED,
        settings,
    )
    .await
}

/// The two-table scenario in `tier`: `settings.tasks` loops of warm hits on
/// the long table beside one loop of `MultiGet` 1,000 warm hits on the
/// string table, both served over one storage, one `WARM_CACHE_BYTES`
/// cache budget, under Disk one local directory, and one count of calls
/// in flight, as `open_with` opens a process's tables, each loaded and
/// primed, run together for the warm-up and then for the measured window,
/// whose operations follow the warm-up's. Every loop yields between
/// operations (`Pace::Yielding`): the hit loops would otherwise hold every
/// worker, and the batch and its runs would wait for the window to end.
/// One row per table. Neither row carries counters: both tables'
/// operations run in one window over one storage and one cache, so no
/// count belongs to either row alone; each carries the value bytes its own
/// loops read and returned.
async fn two_tables(
    prepared: &[Prepared],
    tier: Tier,
    settings: &RunSettings,
) -> BenchResult<Vec<Measured>> {
    let long = random_table(prepared, TableName::Long)?;
    let string = random_table(prepared, TableName::String)?;
    let served = serve(
        long,
        Some(string),
        tier,
        WARM_CACHE_BYTES,
        pinned(long),
        None,
    )
    .await?;
    served.table.load().await?;
    prime_cache(&served.table, long.shape).await?;
    let long_table = &served.table;
    let string_table = served
        .beside
        .as_ref()
        .expect("the string table is served beside the long one");
    let hit_op = get(&key_pools(long.shape).0, true, WHOLE_VALUE);
    let batch_op = multi(&key_pools(string.shape).0, LARGE_BATCH_KEYS, WHOLE_VALUE);
    let hits = (&hit_op, Pace::Yielding);
    let batches = (&batch_op, Pace::Yielding);
    let tasks = settings.tasks;
    let one = 1;

    let until = Instant::now() + settings.warmup;
    let hit_loops = spawn_loops(long_table, hits, (0, tasks), until, None);
    let batch_loops = spawn_loops(string_table, batches, (0, one), until, None);
    let (warm_hits, _, _) = join_loops(hit_loops).await?;
    let (warm_batches, _, _) = join_loops(batch_loops).await?;

    let windows = approx::length(settings.windows);
    let reserve = |ops: usize, tasks: usize| Some((ops / tasks + 1) * windows * SAMPLE_HEADROOM);
    let started = Instant::now();
    let until = started + settings.duration;
    let hit_loops = spawn_loops(
        long_table,
        hits,
        (warm_hits, tasks),
        until,
        reserve(warm_hits, tasks),
    );
    let batch_loops = spawn_loops(
        string_table,
        batches,
        (warm_batches, one),
        until,
        reserve(warm_batches, one),
    );
    let (hit_ops, hit_samples, hit_bytes) = join_loops(hit_loops).await?;
    let (batch_ops, batch_samples, batch_bytes) = join_loops(batch_loops).await?;
    let elapsed = started.elapsed();

    let batch_fan_out = serving().batch_fan_out;
    let label = |table, mode, tasks| RunLabel {
        scenario: Scenario::TwoTable,
        table,
        tier,
        mode,
        tasks,
        batch_fan_out,
        beside: None,
    };
    let rows = vec![
        Measured {
            label: label(string.shape, Mode::Single, one),
            keys_per_op: LARGE_BATCH_KEYS,
            window: Window {
                ops: batch_ops,
                elapsed,
                latencies: Latencies::of(batch_samples),
                counters: Counters::default(),
                bytes: batch_bytes,
            },
            rounds: None,
        },
        Measured {
            label: label(long.shape, Mode::Multi, tasks),
            keys_per_op: 1,
            window: Window {
                ops: hit_ops,
                elapsed,
                latencies: Latencies::of(hit_samples),
                counters: Counters::default(),
                bytes: hit_bytes,
            },
            rounds: None,
        },
    ];
    for run in &rows {
        eprintln!("{}", row(run));
    }
    Ok(rows)
}

// ---------------------------------------------------------------------------
// Loads and refreshes.

/// How often the resident set is sampled while a load runs, for its peak.
const RSS_SAMPLE_INTERVAL: Duration = Duration::from_millis(1);

/// The file that counts the process's pages on Linux; its second field is
/// the resident set, in pages.
const STATM: &str = "/proc/self/statm";

/// The most bytes `STATM` holds: seven counts of pages, each at most
/// twenty digits, and their separators.
const STATM_BYTES: usize = 256;

/// The process's resident set sampled on a thread of its own until stopped;
/// the largest sample is the peak of the window. The thread's own CPU time
/// comes back beside the peak, so a load's CPU can leave it out.
struct RssSampler {
    stop: Arc<AtomicBool>,
    handle: JoinHandle<(u64, Duration)>,
}

/// What an `RssSampler` saw: the peak resident set, and the CPU time its
/// thread spent sampling.
struct Sampled {
    peak: u64,
    cpu: Duration,
}

impl RssSampler {
    fn start() -> Self {
        let stop = Arc::new(AtomicBool::new(false));
        let flag = Arc::clone(&stop);
        let handle = std::thread::spawn(move || {
            let (_, mut resident) = resident_reader();
            let mut peak = 0;
            while !flag.load(Ordering::Relaxed) {
                peak = peak.max(resident());
                std::thread::sleep(RSS_SAMPLE_INTERVAL);
            }
            (peak.max(resident()), thread_cpu())
        });
        Self { stop, handle }
    }

    fn stop(self) -> Sampled {
        self.stop.store(true, Ordering::Relaxed);
        let (peak, cpu) = self.handle.join().unwrap_or_default();
        Sampled { peak, cpu }
    }
}

/// The process's resident set now, or zero where the platform reports none.
fn resident_bytes() -> u64 {
    let (_, mut resident) = resident_reader();
    resident()
}

/// Where a `resident_reader` reads the resident set.
#[derive(Clone, Copy, Debug)]
enum RssSource {
    /// `STATM`'s resident pages times the page size.
    Statm {
        /// The page size `STATM` counts in.
        page_bytes: u64,
    },
    /// `memory_stats`' `physical_mem`.
    MemoryStats,
}

impl RssSource {
    /// The source as the settings JSON records it, with what its figures
    /// can be compared with.
    fn to_json(self) -> Json {
        match self {
            Self::Statm { page_bytes } => json!({
                "kind": "statm",
                "file": STATM,
                "page_bytes": page_bytes,
                "read": "one pread at offset 0 into a buffer on the stack per sample",
                "accuracy": "statm's resident pages are the kernel's per-process counters, which are approximate, where /proc/self/smaps sums every mapping exactly; rss_*_bytes are not byte-comparable with runs that read smaps through memory_stats, Stage 4's on S3 among them",
            }),
            Self::MemoryStats => json!({
                "kind": "memory_stats",
                "read": "physical_mem: task_info on macOS, /proc/self/smaps on Linux",
            }),
        }
    }
}

/// A reader of the process's resident set, or zero when it cannot be
/// read. Where `STATM` opens, as on Linux, it holds the file open and
/// reads it into a buffer on the stack, so a sample costs one `pread` and
/// allocates nothing the load's allocation count would see;
/// `/proc/self/smaps`, which `memory_stats` reads on Linux, walks every
/// mapping and costs the sampler seconds of kernel time over a long load.
/// Elsewhere it asks `memory_stats`, which on macOS is one `task_info`
/// call. The source comes back beside the reader, decided by the same
/// open, so the settings JSON names the one a sampler reads.
fn resident_reader() -> (RssSource, Box<dyn FnMut() -> u64>) {
    use std::os::unix::fs::FileExt as _;
    let Ok(statm) = std::fs::File::open(STATM) else {
        let reader =
            || memory_stats::memory_stats().map_or(0, |stats| approx::count(stats.physical_mem));
        return (RssSource::MemoryStats, Box::new(reader));
    };
    let page_bytes = page_bytes();
    let reader = move || {
        let mut buffer = [0_u8; STATM_BYTES];
        let Ok(read) = statm.read_at(&mut buffer, 0) else {
            return 0;
        };
        resident_pages(&buffer[..read]) * page_bytes
    };
    (RssSource::Statm { page_bytes }, Box::new(reader))
}

/// The resident pages `STATM`'s text `statm` counts, its second field, or
/// zero when it has none.
fn resident_pages(statm: &[u8]) -> u64 {
    std::str::from_utf8(statm)
        .ok()
        .and_then(|text| text.split_ascii_whitespace().nth(1))
        .and_then(|pages| pages.parse().ok())
        .unwrap_or(0)
}

/// The kernel's page size, which `STATM` counts in, or zero when the
/// system does not say.
fn page_bytes() -> u64 {
    // SAFETY: `sysconf` reads a system constant and touches no memory of
    // the caller's.
    u64::try_from(unsafe { libc::sysconf(libc::_SC_PAGESIZE) }).unwrap_or(0)
}

/// The calling thread's user and system CPU time so far, or zero when the
/// clock cannot be read.
fn thread_cpu() -> Duration {
    // SAFETY: `clock_gettime` writes one `timespec` into the zeroed struct
    // it is handed and reads nothing else.
    let now = unsafe {
        let mut now: libc::timespec = std::mem::zeroed();
        if libc::clock_gettime(libc::CLOCK_THREAD_CPUTIME_ID, &raw mut now) != 0 {
            return Duration::ZERO;
        }
        now
    };
    clock_time(now)
}

/// The process's user and system CPU time so far, and its peak resident
/// set over its whole life as the kernel reports it (bytes on macOS,
/// kibibytes on Linux, recorded as reported with the platform beside it).
#[derive(Clone, Copy, Debug, Default)]
struct Usage {
    user: Duration,
    system: Duration,
    max_rss: u64,
}

impl Usage {
    fn now() -> Self {
        // SAFETY: `getrusage` writes one `rusage` into the zeroed struct it
        // is handed and reads nothing else.
        let usage = unsafe {
            let mut usage: libc::rusage = std::mem::zeroed();
            if libc::getrusage(libc::RUSAGE_SELF, &raw mut usage) != 0 {
                return Self::default();
            }
            usage
        };
        Self {
            user: cpu_time(usage.ru_utime),
            system: cpu_time(usage.ru_stime),
            max_rss: u64::try_from(usage.ru_maxrss).unwrap_or(0),
        }
    }
}

/// A `timeval` from `getrusage` as a `Duration`, at the kernel's boundary,
/// where the time arrives as bare seconds and microseconds.
fn cpu_time(value: libc::timeval) -> Duration {
    Duration::from_secs(u64::try_from(value.tv_sec).unwrap_or(0))
        + Duration::from_micros(u64::try_from(value.tv_usec).unwrap_or(0))
}

/// A `timespec` from `clock_gettime` as a `Duration`, at the kernel's
/// boundary, where the time arrives as bare seconds and nanoseconds.
fn clock_time(value: libc::timespec) -> Duration {
    Duration::from_secs(u64::try_from(value.tv_sec).unwrap_or(0))
        + Duration::from_nanos(u64::try_from(value.tv_nsec).unwrap_or(0))
}

/// What a load is of: one of the program's own tables, or a golden
/// reference table.
#[derive(Clone, Copy, Debug)]
enum Subject {
    Bench(TableShape),
    Reference(GoldenTable),
}

impl Subject {
    /// The table's name in the printed rows.
    fn table_name(self) -> String {
        match self {
            Self::Bench(shape) => shape.table_name(),
            Self::Reference(table) => table.to_string(),
        }
    }

    /// The table's JSON fields: a program table by its keys and values, a
    /// reference table by its name.
    fn to_json(self) -> Json {
        match self {
            Self::Bench(shape) => json!({
                "table": shape.name.as_str(),
                "values": shape.values.as_str(),
            }),
            Self::Reference(table) => json!({ "table": table.to_string() }),
        }
    }
}

/// One measured load or refresh, and the size of the generation it left.
struct Loaded {
    what: &'static str,
    table: Subject,
    tier: Tier,
    round: u32,
    wall: Duration,
    /// The process's CPU time over the load, the resident-set sampler's
    /// own left out.
    cpu: Duration,
    /// The CPU time the resident-set sampler's thread spent.
    sampler_cpu: Duration,
    allocations: Allocations,
    rss_before: u64,
    rss_peak: u64,
    max_rss: u64,
    gets: u64,
    /// The bytes those GETs requested.
    get_bytes: u64,
    local_reads: u64,
    /// The bytes those local reads read.
    local_bytes: u64,
    /// The rounds of GETs the load or refresh waited on, counted through
    /// `served.rounds`; `None` where that store is not in place.
    get_rounds: Option<u64>,
    /// The most bytes of chosen order pages the uniqueness merge held at
    /// once (`HeldBytes`); `None` when no merge ran.
    held_bytes: Option<u64>,
    files: usize,
    index_bytes: usize,
    /// The resident memory of the generation's file indexes by part, summed
    /// over its files.
    parts: ResidentParts,
    /// Under Disk, the bytes of the chunk files in the local directory.
    chunk_file_bytes: Option<u64>,
    /// The GETs the served store logged (`store_gets`), where a log is in
    /// place.
    store_gets: Option<Json>,
}

impl Loaded {
    fn to_json(&self) -> Json {
        let mut fields = merged(self.table.to_json(), [self.figures()]);
        if let Some(gets) = &self.store_gets {
            fields["store_gets"] = gets.clone();
        }
        fields
    }

    /// Every field but the table's.
    fn figures(&self) -> Json {
        json!({
            "what": self.what,
            "tier": self.tier.name(),
            "round": self.round,
            "wall_us": micros(self.wall),
            "cpu_us": micros(self.cpu),
            "sampler_cpu_us": micros(self.sampler_cpu),
            "allocations": self.allocations.calls,
            "allocated_bytes": self.allocations.bytes,
            "rss_before_bytes": self.rss_before,
            "rss_peak_bytes": self.rss_peak,
            "rss_peak_delta_bytes": self.rss_peak.saturating_sub(self.rss_before),
            "getrusage_max_rss": self.max_rss,
            "gets": self.gets,
            "get_bytes": self.get_bytes,
            "local_reads": self.local_reads,
            "local_bytes": self.local_bytes,
            "get_rounds": self.get_rounds,
            "held_bytes": self.held_bytes,
            "files": self.files,
            "index_bytes": self.index_bytes,
            "resident_parts": self.parts,
            "resident_parts_bytes": self.parts.total(),
            "chunk_file_bytes": self.chunk_file_bytes,
        })
    }
}

/// Times `work` against `served`: wall and CPU time, allocations, the
/// resident set before and at its sampled peak, the storage reads, and for
/// an ordered table the rounds of GETs and the bytes its merge held; then,
/// outside the timing, the generation's resident parts and under Disk its
/// chunk files' bytes.
async fn timed<F: Future<Output = BenchResult<()>>>(
    served: &Served,
    what: &'static str,
    (table, tier, round): (Subject, Tier, u32),
    work: F,
) -> BenchResult<Loaded> {
    let rss_before = resident_bytes();
    if let Some(store) = &served.rounds {
        store.restart();
    }
    if let Some(log) = &served.gets {
        log.restart();
    }
    HeldBytes::take();
    let before = Counters::read(served);
    let usage = Usage::now();
    let sampler = RssSampler::start();
    let started = Instant::now();
    work.await?;
    let wall = started.elapsed();
    let seen = sampler.stop();
    let after = Usage::now();
    let counters = Counters::read(served).since(before);
    let get_rounds = served.rounds.as_ref().map(|store| store.rounds());
    let held_bytes = HeldBytes::take();
    let generation = served.table.serving()?;
    let parts = generation
        .files
        .iter()
        .fold(ResidentParts::default(), |sum, file| {
            sum.plus(file.index.resident_parts())
        });
    let chunk_file_bytes = served
        .local
        .as_ref()
        .map(|local| local.index_bytes_in_use())
        .transpose()?;
    let store_gets = served
        .gets
        .as_ref()
        .map(|log| store_gets(log, &PageMap::of(&generation), 0));
    Ok(Loaded {
        what,
        table,
        tier,
        round,
        wall,
        cpu: (after.user + after.system)
            .saturating_sub(usage.user + usage.system)
            .saturating_sub(seen.cpu),
        sampler_cpu: seen.cpu,
        allocations: counters.allocations,
        rss_before,
        rss_peak: seen.peak,
        max_rss: after.max_rss,
        gets: counters.gets,
        get_bytes: counters.get_bytes,
        local_reads: counters.local_reads,
        local_bytes: counters.local_bytes,
        get_rounds,
        held_bytes,
        files: generation.files.len(),
        index_bytes: generation.index_bytes,
        parts,
        chunk_file_bytes,
        store_gets,
    })
}

/// `rounds` first loads of `table` in `tier`, each over the fresh storage,
/// caches, and local directory `serve` gives it, here none to reuse; under
/// Disk, each followed by a load over the same directory, which reuses the
/// chunk files the first wrote.
async fn loads<F, Fut>(
    table: Subject,
    tier: Tier,
    rounds: u32,
    serve: F,
) -> BenchResult<Vec<Loaded>>
where
    F: Fn(Option<TempDir>) -> Fut,
    Fut: Future<Output = BenchResult<Served>>,
{
    let mut results = Vec::new();
    for round in 0..rounds {
        let key = (table, tier, round);
        let served = serve(None).await?;
        let run = timed(&served, "load", key, async {
            Ok(served.table.load().await?)
        })
        .await?;
        eprintln!("{}", load_row(&run));
        results.push(run);
        if tier == Tier::Disk {
            let Served { _local: dir, .. } = served;
            let again = serve(dir).await?;
            let run = timed(&again, "load-reusing-chunk-files", key, async {
                Ok(again.table.load().await?)
            })
            .await?;
            eprintln!("{}", load_row(&run));
            results.push(run);
        }
    }
    Ok(results)
}

/// `rounds` refreshes of `prepared` in `tier`: the table follows its root's
/// version hint, is loaded at the first build, and the hint is moved to the
/// second, whose appended files the refresh loads beside the reused ones;
/// then an unchanged poll, which reads the hint alone.
async fn refreshes(prepared: &Prepared, tier: Tier, rounds: u32) -> BenchResult<Vec<Loaded>> {
    let hint = local_path(&catalog::hint_location(&prepared.root))?;
    let source = SourceConfig::Static {
        location: prepared.root.clone(),
    };
    let mut results = Vec::new();
    for round in 0..rounds {
        let key = (Subject::Bench(prepared.shape), tier, round);
        std::fs::write(&hint, &prepared.first)?;
        let served = serve(prepared, None, tier, WARM_CACHE_BYTES, source.clone(), None).await?;
        served.table.load().await?;
        std::fs::write(&hint, &prepared.second)?;
        let run = timed(&served, "refresh-appended", key, async {
            if served.table.refresh().await? {
                Ok(())
            } else {
                Err("the moved hint did not swap a generation".into())
            }
        })
        .await?;
        eprintln!("{}", load_row(&run));
        results.push(run);
        let run = timed(&served, "refresh-unchanged", key, async {
            if served.table.refresh().await? {
                Err("an unchanged hint swapped a generation".into())
            } else {
                Ok(())
            }
        })
        .await?;
        eprintln!("{}", load_row(&run));
        results.push(run);
    }
    std::fs::write(&hint, &prepared.first)?;
    Ok(results)
}

// ---------------------------------------------------------------------------
// Reporting.

/// Bytes per mebibyte, for the printed sizes.
const MEBIBYTE: f64 = 1_048_576.0;

fn mib(bytes: u64) -> f64 {
    approx::gauge(bytes) / MEBIBYTE
}

/// The column headings of the lookup table.
fn header() -> String {
    format!(
        "{:<24} {:<19} {:<6} {:<7} {:>5} {}",
        "scenario",
        "table",
        "tier",
        "mode",
        "tasks",
        window_header()
    )
}

/// The headings of a window's figures (`window_cells`).
fn window_header() -> String {
    format!(
        "{:>11} {:>10} {:>10} {:>10} {:>10} {:>9} {:>10} {:>8} {:>8} {:>10} {:>10} {:>11} {:>6}",
        "ops/s",
        "p50",
        "p90",
        "p99",
        "max",
        "allocs/op",
        "bytes/op",
        "fetch/op",
        "reads/op",
        "read B/op",
        "ret B/op",
        "store B/op",
        "rounds"
    )
}

fn row(run: &Measured) -> String {
    format!(
        "{:<24} {:<19} {:<6} {:<7} {:>5} {}",
        run.label.scenario.name(),
        run.label.table_label(),
        run.label.tier.name(),
        run.label.mode.name(),
        run.label.tasks,
        window_cells(&run.window, run.rounds)
    )
}

/// A window's figures and its rounds, under `window_header`.
fn window_cells(window: &Window, rounds: Option<Rounds>) -> String {
    let counters = window.counters;
    format!(
        "{:>11.0} {:>10.2?} {:>10.2?} {:>10.2?} {:>10.2?} {:>9.1} {:>10.0} {:>8.3} {:>8.3} {:>10.0} {:>10.0} {:>11.0} {:>6}",
        window.ops_per_second(),
        window.latencies.percentile(P50),
        window.latencies.percentile(P90),
        window.latencies.percentile(P99),
        window.latencies.max(),
        window.per_op(counters.allocations.calls),
        window.per_op(counters.allocations.bytes),
        window.per_op(counters.fetches),
        window.per_op(counters.gets + counters.local_reads),
        window.per_op(window.bytes.read),
        window.per_op(window.bytes.returned),
        window.per_op(counters.get_bytes + counters.local_bytes),
        rounds.map_or_else(
            || "-".to_string(),
            |rounds| format!("{:.2}", rounds.per_op())
        ),
    )
}

fn load_header() -> String {
    format!(
        "{:<26} {:<19} {:<6} {:>5} {:>10} {:>10} {:>11} {:>10} {:>10} {:>9} {:>9} {:>6} {:>10} {:>10}",
        "what",
        "table",
        "tier",
        "round",
        "wall",
        "cpu",
        "allocs",
        "alloc MiB",
        "peak +MiB",
        "reads",
        "read MiB",
        "files",
        "index MiB",
        "chunk MiB"
    )
}

fn load_row(run: &Loaded) -> String {
    format!(
        "{:<26} {:<19} {:<6} {:>5} {:>10.1?} {:>10.1?} {:>11} {:>10.1} {:>10.1} {:>9} {:>9.1} {:>6} {:>10.1} {:>10}",
        run.what,
        run.table.table_name(),
        run.tier.name(),
        run.round,
        run.wall,
        run.cpu,
        run.allocations.calls,
        mib(run.allocations.bytes),
        mib(run.rss_peak.saturating_sub(run.rss_before)),
        run.gets + run.local_reads,
        mib(run.get_bytes + run.local_bytes),
        run.files,
        mib(approx::count(run.index_bytes)),
        run.chunk_file_bytes
            .map_or_else(|| "-".to_string(), |bytes| format!("{:.1}", mib(bytes))),
    )
}

/// The first line `program args` prints, trimmed, or `None` when it cannot
/// run or prints nothing.
fn command_line(program: &str, args: &[&str]) -> Option<String> {
    let output = Command::new(program).args(args).output().ok()?;
    let text = String::from_utf8(output.stdout).ok()?;
    let line = text.lines().next()?.trim().to_string();
    (!line.is_empty()).then_some(line)
}

/// The CPU model: `sysctl` on macOS, `/proc/cpuinfo` on Linux.
fn cpu_model() -> String {
    if let Some(model) = command_line("sysctl", &["-n", "machdep.cpu.brand_string"]) {
        return model;
    }
    let mut text = String::new();
    if let Ok(mut file) = std::fs::File::open("/proc/cpuinfo")
        && file.read_to_string(&mut text).is_ok()
        && let Some(line) = text.lines().find(|line| line.starts_with("model name"))
        && let Some((_, model)) = line.split_once(':')
    {
        return model.trim().to_string();
    }
    "unknown".to_string()
}

/// The machine and build the numbers were measured on.
fn machine(tasks: usize, metrics: bool) -> Json {
    json!({
        "cpu": cpu_model(),
        "cores": std::thread::available_parallelism().map_or(0, std::num::NonZero::get),
        "worker_threads": tokio::runtime::Handle::current().metrics().num_workers(),
        "tasks": tasks,
        "os": command_line("uname", &["-srm"]).unwrap_or_else(|| std::env::consts::OS.to_string()),
        "os_version": command_line("sw_vers", &["-productVersion"]),
        "memory_bytes": command_line("sysctl", &["-n", "hw.memsize"]),
        "commit": command_line("git", &["rev-parse", "--short", "HEAD"]),
        "optimized": !cfg!(debug_assertions),
        "metrics_recorder": metrics,
        "allocator": "jemalloc behind a counting wrapper",
    })
}

/// Writes `report` to `<out_dir>/<timestamp>.json` and returns the path.
fn write_report(out_dir: &Path, report: &Json) -> BenchResult<PathBuf> {
    std::fs::create_dir_all(out_dir)?;
    let stamp = time::log_timestamp(time::now()).replace(':', "-");
    let path = out_dir.join(format!("{stamp}.json"));
    std::fs::write(&path, serde_json::to_vec_pretty(report)?)?;
    Ok(path)
}

// ---------------------------------------------------------------------------
// Reference mode.

/// What `--reference` measures of each reference table.
#[derive(Clone, Copy, Debug, PartialEq, Eq, ValueEnum)]
enum ReferenceScenario {
    /// Scans of the keys sampled from each tier of rows per key, in both
    /// directions, whole and over a window of each run's middle half
    /// (`golden::reference_window`), at the table's `max_scan_rows`,
    /// through a page cache emptied before each operation
    /// (`PageCache::clear`, outside its timing), so every page comes from
    /// object storage, or under Disk the key and order pages from the
    /// local chunk files; in a multi-task run one task's operation may
    /// still find a page another task's fetched since, which
    /// `page_cache_hits_per_op` counts.
    ScanCold,
    /// The same Scans through a page cache primed with every sampled key's whole
    /// run.
    ScanWarm,
    /// `MultiScan`s of 10 and of 100 keys (`REFERENCE_MULTI_KEYS`) at
    /// each key's newest row and a light key's most rows
    /// (`reference_multi_limits`), newest first,
    /// unbounded, their keys the first of each tier in turn
    /// (`multi_scan_ids`), through the emptied page cache.
    MultiScanCold,
    /// The same `MultiScan`s through the primed page cache.
    MultiScanWarm,
    /// First loads, and under Disk each followed by a restart over the
    /// chunk files it left.
    Load,
}

impl ReferenceScenario {
    /// The scenario's name in the results.
    fn name(self) -> &'static str {
        match self {
            Self::ScanCold => "scan-cold",
            Self::ScanWarm => "scan-warm",
            Self::MultiScanCold => "multi-scan-cold",
            Self::MultiScanWarm => "multi-scan-warm",
            Self::Load => "load",
        }
    }

    /// Whether its reads go through the emptied page cache.
    fn cold(self) -> bool {
        match self {
            Self::ScanCold | Self::MultiScanCold => true,
            Self::ScanWarm | Self::MultiScanWarm | Self::Load => false,
        }
    }

    /// Whether it measures `MultiScan`s.
    fn multi(self) -> bool {
        match self {
            Self::MultiScanCold | Self::MultiScanWarm => true,
            Self::ScanCold | Self::ScanWarm | Self::Load => false,
        }
    }

    /// Whether it measures loads, not lookups.
    fn loads(self) -> bool {
        match self {
            Self::Load => true,
            Self::ScanCold | Self::ScanWarm | Self::MultiScanCold | Self::MultiScanWarm => false,
        }
    }
}

/// Slots of the GET log over a reference table's store, `GetLog::SLOT_BYTES`
/// each: more GETs than a measured window of a few seconds issues against
/// object storage, or than a load of a reference table's 64 to 320 files
/// does. A window over local files, whose GETs answer in microseconds, can
/// fill it, and the results count what it dropped.
const GET_LOG_SLOTS: usize = 1 << 16;

/// The most GETs of a lookup run listed one by one in its results, beside
/// the classes every GET of the window counts in (`store_gets`).
const GET_SAMPLES: usize = 4_096;

/// The classes of GETs by the data pages each spans, least and most, both
/// included; `None` has no upper bound.
const SPAN_CLASSES: [(u64, Option<u64>); 4] =
    [(1, Some(1)), (2, Some(4)), (5, Some(16)), (17, None)];

/// The least bytes of each class of GETs by bytes, ascending, each class
/// below the next one's least: from nothing through 4 KiB, a small page,
/// by fours to 4 MiB, a span of many value pages.
const BYTE_CLASS_FLOORS: [u64; 7] = [
    0,
    4 * 1024,
    16 * 1024,
    64 * 1024,
    256 * 1024,
    1024 * 1024,
    4 * 1024 * 1024,
];

/// Keys of the reference `MultiScan`s: a batch of ten, and of a hundred,
/// spread over several workers' runs.
const REFERENCE_MULTI_KEYS: [usize; 2] = [MULTI_SCAN_KEYS, SMALL_BATCH_KEYS];

/// Rows per key of the reference `MultiScan`s: each key's newest row, and
/// a short history, the most rows a light key of a reference table holds
/// (`golden::LIGHT_ROWS`). A hundred keys at the longer limit, one column
/// beside the order value, must fit `server.max_multi_scan_values` at its
/// default.
fn reference_multi_limits() -> [u32; 2] {
    let short = u32::try_from(*golden::LIGHT_ROWS.end()).expect("a light key's rows fit a limit");
    [MULTI_SCAN_HEAD_ROWS, short]
}

/// One lookup scenario of a reference table.
#[derive(Clone, Copy, Debug)]
struct RefLookup {
    scenario: ReferenceScenario,
    /// The tier whose sampled keys a Scan reads, one key an operation in
    /// turn; `None` for a `MultiScan`, whose keys span every tier.
    rows_tier: Option<RowsTier>,
    direction: Direction,
    /// Whether each key is read over its window, not its whole run.
    bounded: bool,
    limit: u32,
    /// Keys per operation: one for a Scan.
    keys: usize,
}

/// The lookup scenarios of `scenarios` over the keys of `rows_tiers`, Scans
/// at `max`, the table's `max_scan_rows`, in the order they run.
fn reference_lookups(
    scenarios: &[ReferenceScenario],
    rows_tiers: &[RowsTier],
    max: u32,
) -> Vec<RefLookup> {
    let mut lookups = Vec::new();
    for &scenario in scenarios.iter().filter(|scenario| !scenario.loads()) {
        if scenario.multi() {
            for keys in REFERENCE_MULTI_KEYS {
                for limit in reference_multi_limits() {
                    lookups.push(RefLookup {
                        scenario,
                        rows_tier: None,
                        direction: Direction::NewestFirst,
                        bounded: false,
                        limit,
                        keys,
                    });
                }
            }
            continue;
        }
        for &tier in rows_tiers {
            for direction in [Direction::NewestFirst, Direction::OldestFirst] {
                for bounded in [false, true] {
                    lookups.push(RefLookup {
                        scenario,
                        rows_tier: Some(tier),
                        direction,
                        bounded,
                        limit: max,
                        keys: 1,
                    });
                }
            }
        }
    }
    lookups
}

/// The key indexes of a reference `MultiScan` of `batch` keys below
/// `keys`: the first keys of each tier in turn, heaviest first
/// (`golden::reference_tier_sample`), so a batch of fifteen or more holds
/// the golden sample's tier keys.
fn multi_scan_ids(keys: u64, batch: usize) -> Vec<u64> {
    let per_tier = batch.div_ceil(RowsTier::ALL.len());
    let tiers = RowsTier::ALL.map(|tier| golden::reference_tier_sample(keys, tier, per_tier));
    (0..per_tier)
        .flat_map(|at| tiers.iter().filter_map(move |ids| ids.get(at).copied()))
        .take(batch)
        .collect()
}

/// The request of `lookup` for the keys `ids` of `table`: per key its case
/// and its answer from the derivation (`golden::derived_scan`), over its
/// window when the lookup is bounded.
fn reference_request(table: GoldenTable, ids: &[u64], lookup: &RefLookup) -> Request {
    let (cases, wants) = ids
        .iter()
        .map(|&id| {
            let rows = golden::ordered_rows(table, id);
            let (start, before) = if lookup.bounded {
                let (start, before) = golden::reference_window(&rows);
                (Some(start), Some(before))
            } else {
                (None, None)
            };
            let test = TestScan {
                direction: lookup.direction,
                limit: lookup.limit,
                start,
                before,
            };
            let (want, _) = golden::derived_scan(table, id, &rows, &test);
            let bounds = test.bounds_by(|order| golden::ordered_order_value(table, id, order));
            (ScanCase::of_scanned(test, bounds, &want), want)
        })
        .unzip();
    Request {
        ids: ids.to_vec(),
        keys: ids
            .iter()
            .map(|&id| golden::ordered_key(table, id))
            .collect(),
        multi: lookup.scenario.multi(),
        cases,
        wants,
    }
}

/// The key indexes each request of `lookup` over a table of `keys` keys
/// asks: each sampled key of its tier alone, or one `MultiScan`'s batch.
fn lookup_ids(keys: u64, lookup: &RefLookup) -> Vec<Vec<u64>> {
    match lookup.rows_tier {
        Some(tier) => golden::reference_tier_sample(keys, tier, REFERENCE_TIER_SAMPLES)
            .into_iter()
            .map(|id| vec![id])
            .collect(),
        None => vec![multi_scan_ids(keys, lookup.keys)],
    }
}

/// The op of `lookup` over `table` of `keys` keys: a Scan of each sampled
/// key of its tier in turn, or one `MultiScan` of its batch
/// (`lookup_ids`); `cold` names the page cache it empties before each
/// operation.
fn reference_op(table: GoldenTable, keys: u64, lookup: &RefLookup, cold: Option<PageCache>) -> Op {
    let requests = lookup_ids(keys, lookup)
        .iter()
        .map(|ids| reference_request(table, ids, lookup))
        .collect();
    Op::Reference {
        requests: Arc::new(requests),
        value_bytes: REFERENCE_VALUE_BYTES,
        cold,
    }
}

/// `verify_answers` of every request of a reference op, against the
/// derivation's answers each request holds.
async fn verify_reference(op: &Op, table: &Table, golden_table: GoldenTable) -> BenchResult<()> {
    let Op::Reference { requests, .. } = op else {
        return Ok(());
    };
    verify_answers(op, table, requests.len(), |i, at| {
        let request = &requests[i];
        Ok((request.ids[at], request.wants[at].clone()))
    })
    .await
    .map_err(|fault| format!("{golden_table} {fault}").into())
}

/// `table` of the reference warehouse `warehouse` in `tier`, as `serve`
/// serves the program's own tables, over the run's storage and
/// `cache_bytes` caches, with every request of the warehouse's store logged
/// (`GetLog`) and its GETs' rounds counted (`RoundsStore`), the log beneath
/// the count so a GET's logged latency is the store's alone; not loaded.
/// Under Disk, `reuse`'s local directory when given.
async fn serve_reference(
    warehouse: &str,
    table: GoldenTable,
    tier: Tier,
    cache_bytes: u64,
    reuse: Option<TempDir>,
) -> BenchResult<Served> {
    let storage = Arc::new(Storage::with_sdk_defaults(serving().storage));
    let mut logged = None;
    let mut counted = None;
    storage
        .wrap_store(&golden::location(warehouse, table), |inner| {
            let log = Arc::new(GetLog::over(inner, GET_LOG_SLOTS));
            let rounds = Arc::new(RoundsStore::over(Arc::clone(&log) as Arc<dyn ObjectStore>));
            logged = Some(log);
            counted = Some(Arc::clone(&rounds));
            rounds
        })
        .await?;
    let config = reference_config(warehouse, table, tier);
    let Stack {
        handles,
        local,
        dir,
    } = stack(&storage, tier, cache_bytes, reads_rows([&config]), reuse).await?;
    let cache = handles.cache.clone();
    let table = table_of(config, handles).await?;
    Ok(Served {
        table: Arc::new(table),
        beside: None,
        storage,
        cache,
        local,
        rounds: counted,
        gets: logged,
        _local: dir,
    })
}

/// `table` of the reference warehouse `warehouse` as the run serves it in
/// `tier`: the golden tests' configuration, at the run's
/// `--reference-max-scan-rows` when given.
fn reference_config(warehouse: &str, table: GoldenTable, tier: Tier) -> TableConfig {
    TableConfig {
        residency: tier.residency(),
        max_scan_rows: serving().reference_max_scan_rows,
        ..golden::table_config(warehouse, table, false)
    }
}

/// Refuses a run serving `tables` of `warehouse` as a server would refuse
/// to start serving them: each table's configuration under the run's
/// server settings, through the validation a configuration file goes
/// through (`Config::validated`), under the Full tier, whose settings every
/// tier shares. When `scenarios` hold a `MultiScan` one, refuses as well a
/// `--reference-max-scan-rows` below a `MultiScan` limit
/// (`reference_multi_limits`), which every `MultiScan` scenario would be
/// refused at.
fn validate_reference(
    warehouse: &str,
    tables: &[GoldenTable],
    scenarios: &[ReferenceScenario],
) -> BenchResult<()> {
    let serving = serving();
    if let Some(rows) = serving.reference_max_scan_rows
        && scenarios.iter().any(|scenario| scenario.multi())
    {
        let limits = reference_multi_limits();
        if limits.iter().any(|&limit| limit > rows) {
            return Err(format!(
                "--reference-max-scan-rows {rows} is below the MultiScan limits {limits:?}"
            )
            .into());
        }
    }
    let config = Config {
        server: {
            let mut server = ServerConfig::default();
            server.batch_fan_out = serving.batch_fan_out;
            server.max_multi_scan_values = serving.max_multi_scan_values;
            server
        },
        storage: serving.storage,
        tables: tables
            .iter()
            .map(|&table| reference_config(warehouse, table, Tier::Full))
            .collect(),
    };
    config.validated()?;
    Ok(())
}

/// The data pages of a generation's files, by `GetLog::name_of_location`
/// of each file: each page's byte range in its file, ascending, over every
/// column whose offset index the footer keeps.
struct PageMap {
    files: HashMap<u64, Vec<Range<u64>>>,
    /// Files whose names another file of the generation shares, so a GET
    /// of either is placed among both's pages; zero for the tables Spark
    /// writes, whose file names carry a UUID.
    shared_names: usize,
}

impl PageMap {
    fn of(generation: &Generation) -> Self {
        let mut files: HashMap<u64, Vec<Range<u64>>> = HashMap::new();
        let mut shared_names = 0;
        for file in &generation.files {
            let mut pages: Vec<Range<u64>> = file
                .index
                .metadata()
                .offset_index()
                .into_iter()
                .flatten()
                .flatten()
                .flat_map(|column| column.page_locations().iter().map(page_range))
                .collect();
            pages.sort_by_key(|page| page.start);
            match files.entry(GetLog::name_of_location(&file.entry.path)) {
                Entry::Occupied(mut held) => {
                    shared_names += 1;
                    held.get_mut().extend(pages);
                    held.get_mut().sort_by_key(|page| page.start);
                }
                Entry::Vacant(free) => {
                    free.insert(pages);
                }
            }
        }
        Self {
            files,
            shared_names,
        }
    }

    /// The pages `range` of the file named `name` overlaps; `None` for a
    /// file not in the generation, as a metadata file or manifest is.
    fn spanned(&self, name: u64, range: &Range<u64>) -> Option<u64> {
        let pages = self.files.get(&name)?;
        let first = pages.partition_point(|page| page.end <= range.start);
        let spanned = pages[first..]
            .iter()
            .take_while(|page| page.start < range.end)
            .count();
        Some(approx::count(spanned))
    }
}

/// A page's bytes in its file, header included, as its offset index
/// records them.
fn page_range(page: &PageLocation) -> Range<u64> {
    let start = u64::try_from(page.offset).unwrap_or(0);
    start..start + u64::try_from(page.compressed_page_size).unwrap_or(0)
}

/// The GETs `log` holds as JSON: by the data pages of `pages` each spans
/// (`SPAN_CLASSES`) and by its bytes (`BYTE_CLASS_FLOORS`), for each class
/// how many, their mean bytes, and the percentiles of their latency to the
/// store's answer and to their body's last byte; beside them the GETs no
/// class holds (never answered, a ranged read of several, or outside every
/// data page, as a footer, metadata, or manifest is), and up to `samples`
/// of the classed GETs one by one, as [pages, bytes, first-byte µs,
/// last-byte µs].
fn store_gets(log: &GetLog, pages: &PageMap, samples: usize) -> Json {
    let records = log.records();
    let mut spanned: Vec<(u64, &LoggedGet)> = Vec::new();
    let (mut unanswered, mut multi_range, mut outside) = (0_u64, 0_u64, 0_u64);
    for get in &records {
        match (&get.range, get.parts) {
            (None, _) => unanswered += 1,
            (Some(_), parts) if parts > 1 => multi_range += 1,
            (Some(range), _) => match pages.spanned(get.name, range) {
                Some(0) | None => outside += 1,
                Some(count) => spanned.push((count, get)),
            },
        }
    }
    let by_pages: Vec<Json> = SPAN_CLASSES
        .iter()
        .map(|&(least, most)| {
            let held = spanned
                .iter()
                .filter(|(count, _)| *count >= least && most.is_none_or(|most| *count <= most));
            class_json(
                json!({ "min_pages": least, "max_pages": most }),
                held.map(|(_, get)| *get),
            )
        })
        .collect();
    let by_bytes: Vec<Json> = BYTE_CLASS_FLOORS
        .iter()
        .enumerate()
        .map(|(at, &least)| {
            let below = BYTE_CLASS_FLOORS.get(at + 1).copied();
            let held = spanned.iter().filter(|(_, get)| {
                get.bytes() >= least && below.is_none_or(|below| get.bytes() < below)
            });
            class_json(
                json!({ "min_bytes": least, "below_bytes": below }),
                held.map(|(_, get)| *get),
            )
        })
        .collect();
    let one_by_one: Vec<Json> = spanned
        .iter()
        .take(samples)
        .map(|(count, get)| {
            json!([
                count,
                get.bytes(),
                get.first_byte.map(micros),
                get.last_byte.map(micros)
            ])
        })
        .collect();
    json!({
        "logged": records.len(),
        "dropped": log.dropped(),
        "unanswered": unanswered,
        "multi_range": multi_range,
        "outside_data_pages": outside,
        "data_page_gets": spanned.len(),
        "shared_file_names": pages.shared_names,
        "by_pages": by_pages,
        "by_bytes": by_bytes,
        "samples": one_by_one,
    })
}

/// One class of GETs as JSON: its `bounds`, how many `gets` it holds,
/// their mean bytes, and their latencies to the answer and to the last
/// byte.
fn class_json<'a>(bounds: Json, gets: impl Iterator<Item = &'a LoggedGet>) -> Json {
    let gets: Vec<&LoggedGet> = gets.collect();
    let bytes: u64 = gets.iter().map(|get| get.bytes()).sum();
    let first = Latencies::of(gets.iter().filter_map(|get| get.first_byte).collect());
    let last = Latencies::of(gets.iter().filter_map(|get| get.last_byte).collect());
    merged(
        bounds,
        [json!({
            "gets": gets.len(),
            "mean_bytes": approx::ratio(bytes, approx::count(gets.len())),
            "first_byte": latency_json(&first),
            "last_byte": latency_json(&last),
        })],
    )
}

/// `latencies`' count and percentiles in microseconds.
fn latency_json(latencies: &Latencies) -> Json {
    merged(
        json!({ "count": latencies.len() }),
        [percentiles_json(latencies)],
    )
}

/// `latencies`' percentiles and maximum in microseconds.
fn percentiles_json(latencies: &Latencies) -> Json {
    json!({
        "p50_us": micros(latencies.percentile(P50)),
        "p90_us": micros(latencies.percentile(P90)),
        "p99_us": micros(latencies.percentile(P99)),
        "max_us": micros(latencies.max()),
    })
}

/// The name of a Scan's direction in the results.
fn direction_name(direction: Direction) -> &'static str {
    match direction {
        Direction::NewestFirst => "newest_first",
        Direction::OldestFirst => "oldest_first",
    }
}

/// A measured lookup scenario of a reference table in one mode.
struct RefMeasured {
    table: GoldenTable,
    /// The keys the table was written with.
    table_keys: u64,
    tier: Tier,
    lookup: RefLookup,
    /// The key indexes the op asks, every request's in order.
    ids: Vec<u64>,
    run: ModeRun,
}

impl RefMeasured {
    fn to_json(&self) -> Json {
        let lookup = &self.lookup;
        merged(
            json!({
                "scenario": lookup.scenario.name(),
                "table": self.table.to_string(),
                "table_keys": self.table_keys,
                "tier": self.tier.name(),
                "mode": self.run.mode.name(),
                "tasks": self.run.tasks,
                "batch_fan_out": serving().batch_fan_out,
                "rows_tier": lookup.rows_tier.map(|tier| tier.to_string()),
                "direction": direction_name(lookup.direction),
                "bounded": lookup.bounded,
                "limit": lookup.limit,
                "keys_per_op": lookup.keys,
                "key_ids": self.ids,
                "key_rows": self.ids.iter().map(|&id| golden::reference_rows_of(id)).collect::<Vec<_>>(),
                "store_gets": self.run.gets,
            }),
            [self.run.window.to_json(), rounds_json(self.run.rounds)],
        )
    }
}

/// The column headings of the reference lookup table.
fn ref_header() -> String {
    format!(
        "{:<16} {:<15} {:<6} {:<7} {:>5} {:<6} {:<12} {:<6} {:>5} {:>4} {}",
        "scenario",
        "table",
        "tier",
        "mode",
        "tasks",
        "rows",
        "direction",
        "reads",
        "limit",
        "keys",
        window_header()
    )
}

fn ref_row(run: &RefMeasured) -> String {
    let lookup = &run.lookup;
    format!(
        "{:<16} {:<15} {:<6} {:<7} {:>5} {:<6} {:<12} {:<6} {:>5} {:>4} {}",
        lookup.scenario.name(),
        run.table.to_string(),
        run.tier.name(),
        run.run.mode.name(),
        run.run.tasks,
        lookup
            .rows_tier
            .map_or_else(|| "mixed".to_string(), |tier| tier.to_string()),
        direction_name(lookup.direction),
        if lookup.bounded { "window" } else { "whole" },
        lookup.limit,
        lookup.keys,
        window_cells(&run.run.window, run.run.rounds)
    )
}

/// What a reference run measures: its scenarios, the tiers of rows per key
/// its Scans read, and its settings.
struct RefSelection<'a> {
    scenarios: Vec<ReferenceScenario>,
    rows_tiers: Vec<RowsTier>,
    settings: &'a RunSettings,
}

/// Every selected lookup scenario of reference table `table` in `tier`:
/// served warm, primed with every key the warm scenarios read, and cold,
/// each loaded; per scenario its op checked whole against the derivation
/// (`verify_reference`), its rounds counted, and each mode measured with
/// the GETs of its window.
async fn reference_lookups_in(
    warehouse: &str,
    table: GoldenTable,
    tier: Tier,
    selection: &RefSelection<'_>,
) -> BenchResult<Vec<RefMeasured>> {
    let wanted = |cold: bool| {
        selection
            .scenarios
            .iter()
            .any(|scenario| !scenario.loads() && scenario.cold() == cold)
    };
    let mut warm = None;
    let mut cold = None;
    for (wanted, cache_bytes, served) in [
        (wanted(false), WARM_CACHE_BYTES, &mut warm),
        (wanted(true), COLD_CACHE_BYTES, &mut cold),
    ] {
        if wanted {
            let table_served = serve_reference(warehouse, table, tier, cache_bytes, None).await?;
            table_served.table.load().await?;
            *served = Some(table_served);
        }
    }
    let Some(any) = warm.as_ref().or(cold.as_ref()) else {
        return Ok(Vec::new());
    };
    let metadata = golden::served_metadata(&any.storage, &*any.table.serving()?).await?;
    let keys = golden::reference_keys(&metadata, table)?;
    let max = any
        .table
        .config()
        .scan_rows()
        .ok_or("an ordered table has a max_scan_rows")?;
    let lookups = reference_lookups(&selection.scenarios, &selection.rows_tiers, max);
    if let Some(warm) = &warm {
        let primed: BTreeSet<u64> = lookups
            .iter()
            .filter(|lookup| !lookup.scenario.cold())
            .flat_map(|lookup| lookup_ids(keys, lookup).into_iter().flatten())
            .collect();
        for &id in &primed {
            page_through(&warm.table, &golden::ordered_key(table, id), &VALUE_READS).await?;
        }
    }
    let mut results = Vec::new();
    for lookup in lookups {
        let served = if lookup.scenario.cold() { &cold } else { &warm };
        let served = served
            .as_ref()
            .expect("a table is served for every selected scenario");
        let cold_cache = lookup.scenario.cold().then(|| served.cache.clone());
        let op = reference_op(table, keys, &lookup, cold_cache);
        verify_reference(&op, &served.table, table).await?;
        let rounds = count_rounds(served, &op).await?;
        let ids: Vec<u64> = lookup_ids(keys, &lookup).into_iter().flatten().collect();
        for run in measure_modes(served, &op, rounds, selection.settings).await? {
            let measured = RefMeasured {
                table,
                table_keys: keys,
                tier,
                lookup,
                ids: ids.clone(),
                run,
            };
            eprintln!("{}", ref_row(&measured));
            results.push(measured);
        }
    }
    Ok(results)
}

/// The settings a reference report was measured under.
fn reference_settings(cli: &Cli, warehouse: &str, tables: &[GoldenTable]) -> Json {
    json!({
        "warehouse": warehouse,
        "rewrite_location": cli.rewrite_location.iter().map(ToString::to_string).collect::<Vec<_>>(),
        "tables": tables.iter().map(ToString::to_string).collect::<Vec<_>>(),
        "warmup_seconds": cli.warmup,
        "duration_seconds": cli.duration,
        "load_rounds": cli.load_rounds,
        "load_concurrency": icetray_core::load_concurrency(),
        "batch_fan_out": cli.batch_fan_out,
        "warm_cache_bytes": WARM_CACHE_BYTES,
        "cold_cache_bytes": COLD_CACHE_BYTES,
        "cold": "the page cache emptied before each operation, outside its timing",
        "warm": "every key a warm scenario reads paged through whole before it is measured",
        "verified": "every request's answers compared whole with the derivation before it is measured",
        "reference_max_scan_rows": cli.reference_max_scan_rows,
        "max_scan_rows": tables
            .first()
            .and_then(|&table| reference_config(warehouse, table, Tier::Full).scan_rows()),
        "max_multi_scan_values": serving().max_multi_scan_values,
        "max_scan_bytes": ServerConfig::default().max_scan_bytes,
        "scan_limit": "every Scan scenario at max_scan_rows, the served tables' limit",
        "tier_samples": REFERENCE_TIER_SAMPLES,
        "multi_scan_keys": REFERENCE_MULTI_KEYS,
        "multi_scan_limits": reference_multi_limits(),
        "rounds_ops": ROUNDS_OPS,
        "get_log": {
            "slots": GET_LOG_SLOTS,
            "slot_bytes": GetLog::SLOT_BYTES,
            "per_get": "one atomic increment, a hash of the file name, three clock reads, six relaxed stores, and one boxed body stream",
            "samples_per_run": GET_SAMPLES,
        },
        "span_classes": SPAN_CLASSES.map(|(least, most)| json!({ "min_pages": least, "max_pages": most })),
        "byte_class_floors": BYTE_CLASS_FLOORS,
        "rss_sampling": rss_sampling(),
    })
}

/// How a load's resident set is sampled, and what its CPU time leaves out.
fn rss_sampling() -> Json {
    json!({
        "source": resident_reader().0.to_json(),
        "interval_us": micros(RSS_SAMPLE_INTERVAL),
        "thread": "one per load, started after the CPU and allocation counts are read and joined before they are read again",
        "cpu": "the sampler thread's own CPU time (CLOCK_THREAD_CPUTIME_ID, read as it ends) is left out of a load's cpu_us and reported as sampler_cpu_us",
    })
}

/// The `--reference` run: every selected scenario of every selected
/// reference table in every selected tier, printed as it is measured and
/// written as JSON under `--out-dir`.
async fn reference_main(cli: &Cli, warehouse: &str, settings: &RunSettings) -> BenchResult<()> {
    let tables = or_all(&cli.reference_table, &GoldenTable::reference());
    if let Some(table) = tables.iter().find(|table| !table.is_reference()) {
        return Err(format!("{table} is not a reference table").into());
    }
    let scenarios = or_all(&cli.reference_scenario, ReferenceScenario::value_variants());
    validate_reference(warehouse, &tables, &scenarios)?;
    let selection = RefSelection {
        scenarios,
        rows_tiers: or_all(&cli.rows_tier, &RowsTier::ALL),
        settings,
    };
    let tiers = or_all(&cli.tier, &Tier::ALL);
    let machine = machine(settings.tasks, !cli.no_metrics);
    eprintln!("{machine}");
    eprintln!("{}", ref_header());
    let mut measured = Vec::new();
    let mut loaded = Vec::new();
    for &table in &tables {
        for &tier in &tiers {
            measured.extend(reference_lookups_in(warehouse, table, tier, &selection).await?);
            if selection.scenarios.contains(&ReferenceScenario::Load) {
                let serve_table =
                    |reuse| serve_reference(warehouse, table, tier, WARM_CACHE_BYTES, reuse);
                let subject = Subject::Reference(table);
                loaded.extend(loads(subject, tier, cli.load_rounds, serve_table).await?);
            }
        }
    }
    println!("{machine}");
    println!("{}", ref_header());
    for run in &measured {
        println!("{}", ref_row(run));
    }
    if !loaded.is_empty() {
        println!("{}", load_header());
        for run in &loaded {
            println!("{}", load_row(run));
        }
    }
    let report = json!({
        "run": "reference",
        "machine": machine,
        "settings": reference_settings(cli, warehouse, &tables),
        "lookups": measured.iter().map(RefMeasured::to_json).collect::<Vec<_>>(),
        "loads": loaded.iter().map(Loaded::to_json).collect::<Vec<_>>(),
    });
    let path = write_report(&cli.out_dir, &report)?;
    println!("results: {}", path.display());
    Ok(())
}

// ---------------------------------------------------------------------------
// The program.

/// A selection flag's values, or every value when the flag was not given.
fn or_all<T: Copy>(chosen: &[T], all: &[T]) -> Vec<T> {
    if chosen.is_empty() {
        all.to_vec()
    } else {
        chosen.to_vec()
    }
}

/// One line per table on stdout: its name, key type, rows, and the first
/// build's metadata file, tab-separated.
fn print_prepared(prepared: &[Prepared]) {
    for table in prepared {
        println!(
            "{}\t{:?}\t{}\t{}",
            table.shape.table_name(),
            table.shape.key_type,
            table.shape.rows,
            table.first
        );
    }
}

/// The tables the command line measures, and beside them every table to
/// prepare: those, under the two-table scenario the long and string tables
/// of random values it serves, and under `kv-beside-ordered` the long table
/// of random values and the ordered table.
fn select_shapes(cli: &Cli, scenarios: &[Scenario]) -> (Vec<TableShape>, Vec<TableShape>) {
    let values = or_all(&cli.values, &Values::ALL);
    let measured: Vec<TableShape> = SHAPES
        .into_iter()
        .filter(|shape| shape.selected(&cli.table, &values))
        .collect();
    let two_table = scenarios.contains(&Scenario::TwoTable) && !cli.prepare;
    let beside_ordered = scenarios.contains(&Scenario::KvBesideOrdered) && !cli.prepare;
    let needed = SHAPES
        .into_iter()
        .filter(|shape| {
            let random = shape.values == Values::Random;
            measured.contains(shape)
                || (two_table
                    && random
                    && matches!(shape.name, TableName::Long | TableName::String))
                || (beside_ordered
                    && random
                    && matches!(shape.name, TableName::Long | TableName::Ordered))
        })
        .collect();
    (measured, needed)
}

/// The lookup scenarios of `scenarios` that measure what they say on
/// `shape`: all of them, without the warm ones when the warm cache cannot
/// hold the table's value pages, without those reading a second column or
/// a projection when the table is not a protobuf table, and without the
/// Scan scenarios of another table (`Traits::table`) or the
/// key-value scenarios on an ordered table, each of which it says.
fn lookups_for(shape: TableShape, scenarios: &[Scenario]) -> Vec<Scenario> {
    let ordered = shape.ordered.is_some();
    let kind_kept: Vec<Scenario> = scenarios
        .iter()
        .copied()
        .filter(|scenario| {
            scenario
                .traits()
                .table
                .map_or(!ordered, |table| table == shape.name)
        })
        .collect();
    if kind_kept.len() < scenarios.len() {
        eprintln!(
            "{}: scenarios of another table or table kind skipped",
            shape.table_name()
        );
    }
    let scenarios = kind_kept.as_slice();
    let fits = shape.fits_warm_cache();
    let protobuf = shape.values.is_protobuf();
    let warm_kept: Vec<Scenario> = scenarios
        .iter()
        .copied()
        .filter(|scenario| fits || scenario.traits().cache != Cache::Warm)
        .collect();
    if warm_kept.len() < scenarios.len() {
        eprintln!(
            "{}: warm scenarios skipped, its value pages outrun the warm cache",
            shape.table_name()
        );
    }
    let kept: Vec<Scenario> = warm_kept
        .iter()
        .copied()
        .filter(|scenario| protobuf || !scenario.traits().protobuf)
        .collect();
    if kept.len() < warm_kept.len() {
        eprintln!(
            "{}: column and projection scenarios skipped, it is not a protobuf table",
            shape.table_name()
        );
    }
    kept
}

/// Installs the binary's metrics recorder and drains its histograms on the
/// binary's schedule, as `serve` does, so the measured paths pay what the
/// daemon's do.
fn install_recorder() -> BenchResult<()> {
    let handle = icetray::telemetry::install()?;
    let period = ServerConfig::default().poll_interval;
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(period);
        loop {
            ticker.tick().await;
            handle.run_upkeep();
        }
    });
    Ok(())
}

/// The machine, then a row per measured lookup run and per load or
/// refresh, on stdout.
fn print_results(machine: &Json, measured: &[Measured], loaded: &[Loaded]) {
    println!("{machine}");
    println!("{}", header());
    for run in measured {
        println!("{}", row(run));
    }
    if !loaded.is_empty() {
        println!("{}", load_header());
        for run in loaded {
            println!("{}", load_row(run));
        }
    }
}

/// The settings a report was measured under: the run's command line, the
/// constants that shape the tables and scenarios, and the tables as
/// written with their columns' compression.
fn report_settings(cli: &Cli, prepared: &[Prepared], compression: &[Json]) -> Json {
    json!({
        "key_pool": KEY_POOL,
        "key_seed": KEY_SEED,
        "warm_cache_bytes": WARM_CACHE_BYTES,
        "project_few_fields": PROJECT_FEW,
        "project_most_fields": PROJECT_MOST,
        "cold_cache_bytes": COLD_CACHE_BYTES,
        "skewed_cache_bytes": SKEWED_CACHE_BYTES,
        "zipf_exponent": ZIPF_EXPONENT,
        "skew_seed": SKEW_SEED,
        "row_cache": cli.row_cache.is_on(),
        "warmup_seconds": cli.warmup,
        "duration_seconds": cli.duration,
        "load_concurrency": icetray_core::load_concurrency(),
        "batch_fan_out": cli.batch_fan_out,
        "ordered_rows_per_key": ORDERED_ROWS_PER_KEY,
        "long_run_rows": LONG_RUN_ROWS,
        "string_order_prefix_bytes": STRING_ORDER_PREFIX_BYTES,
        "string_order_width": STRING_ORDER_WIDTH,
        "order_index_truncate_bytes": ORDER_INDEX_TRUNCATE_BYTES,
        "rolled_file_bytes": ROLLED_FILE_BYTES,
        "short_run_rows": SHORT_RUN_ROWS,
        "overlapping_appends": OVERLAPPING_APPENDS,
        "head_rows": HEAD_ROWS,
        "bound_pool": BOUND_POOL,
        "bound_seed": BOUND_SEED,
        "verified_ops": VERIFIED_OPS,
        "rounds_ops": ROUNDS_OPS,
        "scan_page_rows": SCAN_PAGE_ROWS,
        "multi_scan_keys": MULTI_SCAN_KEYS,
        "multi_scan_100_keys": SMALL_BATCH_KEYS,
        "multi_scan_1000_keys": LARGE_BATCH_KEYS,
        "multi_scan_head_rows": MULTI_SCAN_HEAD_ROWS,
        "multi_scan_limit": multi_scan_whole_runs().limit,
        "tables": prepared.iter().map(Prepared::to_json).collect::<Vec<_>>(),
        "compression": compression,
        "rss_sampling": rss_sampling(),
    })
}

/// The settings every lookup run of `cli` shares.
fn run_settings(cli: &Cli) -> RunSettings {
    RunSettings {
        modes: or_all(&cli.mode, &Mode::ALL),
        tasks: cli.tasks.unwrap_or_else(|| {
            std::thread::available_parallelism().map_or(1, std::num::NonZero::get)
        }),
        warmup: Duration::from_secs(cli.warmup),
        duration: Duration::from_secs(cli.duration),
        windows: cli.duration.div_ceil(cli.warmup),
    }
}

/// Fails on a scenario whose name in the results is not its spelling on
/// the command line, which `Scenario::traits` and `ReferenceScenario::name`
/// keep equal to clap's by hand.
fn check_scenario_names() -> BenchResult<()> {
    let names = Scenario::value_variants()
        .iter()
        .map(|scenario| (scenario.name(), scenario.to_possible_value()))
        .chain(
            ReferenceScenario::value_variants()
                .iter()
                .map(|scenario| (scenario.name(), scenario.to_possible_value())),
        );
    for (name, value) in names {
        let spelled = value.as_ref().map(clap::builder::PossibleValue::get_name);
        if spelled != Some(name) {
            return Err(format!("scenario {name} is {spelled:?} on the command line").into());
        }
    }
    Ok(())
}

#[tokio::main]
async fn main() -> BenchResult<()> {
    check_scenario_names()?;
    let cli = Cli::parse();
    set_serving(&cli);
    ROW_GROUP_ROWS
        .set(cli.row_group_rows)
        .expect("main sets the row groups once");
    if cfg!(debug_assertions) {
        eprintln!("warning: an unoptimized build; pass --release or --profile profiling");
    }
    let settings = run_settings(&cli);
    if let Some(warehouse) = &cli.reference {
        if !cli.no_metrics {
            install_recorder()?;
        }
        if cli.reference_scenario.is_empty()
            || cli.reference_scenario.contains(&ReferenceScenario::Load)
        {
            tracing::subscriber::set_global_default(HeldBytes)?;
        }
        return reference_main(&cli, warehouse, &settings).await;
    }
    let scenarios = or_all(&cli.scenario, Scenario::value_variants());
    let (measured_shapes, needed) = select_shapes(&cli, &scenarios);
    let prepared = prepare(&cli.data_dir, cli.rewrite, &needed).await?;
    let compression = compression(&prepared)?;
    if cli.prepare {
        print_prepared(&prepared);
        return Ok(());
    }
    if !cli.no_metrics {
        install_recorder()?;
    }
    if scenarios.contains(&Scenario::Load) || scenarios.contains(&Scenario::Refresh) {
        tracing::subscriber::set_global_default(HeldBytes)?;
    }
    let mut primed = 0;
    for table in &prepared {
        primed += prime_os_cache(&local_path(&table.root)?)?;
    }
    eprintln!("read {:.0} MiB to prime the OS page cache", mib(primed));
    let tasks = settings.tasks;
    let lookup_scenarios: Vec<Scenario> = scenarios
        .iter()
        .copied()
        .filter(|s| s.traits().lookup)
        .collect();
    let tiers = or_all(&cli.tier, &Tier::ALL);
    let tables: Vec<&Prepared> = prepared
        .iter()
        .filter(|table| measured_shapes.contains(&table.shape))
        .collect();
    let machine = machine(tasks, !cli.no_metrics);
    eprintln!("{machine}");
    eprintln!("{}", header());
    let mut measured = Vec::new();
    let mut loaded = Vec::new();
    for table in &tables {
        let table_lookups = lookups_for(table.shape, &lookup_scenarios);
        for &tier in &tiers {
            if !table_lookups.is_empty() {
                measured.extend(lookups(table, None, tier, &table_lookups, &settings).await?);
            }
            if scenarios.contains(&Scenario::Load) {
                let serve_table =
                    |reuse| serve(table, None, tier, WARM_CACHE_BYTES, pinned(table), reuse);
                let subject = Subject::Bench(table.shape);
                loaded.extend(loads(subject, tier, cli.load_rounds, serve_table).await?);
            }
            if scenarios.contains(&Scenario::Refresh) {
                loaded.extend(refreshes(table, tier, cli.load_rounds).await?);
            }
        }
    }
    if scenarios.contains(&Scenario::TwoTable) {
        for &tier in &tiers {
            measured.extend(two_tables(&prepared, tier, &settings).await?);
        }
    }
    if scenarios.contains(&Scenario::KvBesideOrdered) {
        for &tier in &tiers {
            measured.extend(kv_beside_ordered(&prepared, tier, &settings).await?);
        }
    }
    print_results(&machine, &measured, &loaded);
    let report = json!({
        "machine": machine,
        "settings": report_settings(&cli, &prepared, &compression),
        "lookups": measured.iter().map(Measured::to_json).collect::<Vec<_>>(),
        "loads": loaded.iter().map(Loaded::to_json).collect::<Vec<_>>(),
    });
    let path = write_report(&cli.out_dir, &report)?;
    println!("results: {}", path.display());
    Ok(())
}
