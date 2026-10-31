//! The metric and label names of the deployment contract (RFD 012). They
//! are a public surface: a deployer's dashboards and alerts name them, so
//! from 1.0.0 a rename here is a breaking change (Stability in
//! CONTRIBUTING.md). A metric name at a call site is one of these constants
//! and a label value is an enum's `Display`, never a literal, so the set of
//! series a process can emit is closed and readable in one place.

use std::fmt;

/// gRPC calls answered, by `call` and by `code`, the status each answered.
pub const REQUESTS_TOTAL: &str = "icetray_requests_total";
/// Object-store GETs issued, by `kind`; a hedge is a GET and counts here too.
pub const OBJECT_GETS_TOTAL: &str = "icetray_object_gets_total";
/// Bytes requested across those GETs, by `kind`.
pub const OBJECT_BYTES_TOTAL: &str = "icetray_object_bytes_total";
/// Reads served from the local directory (RFD 017).
pub const LOCAL_READS_TOTAL: &str = "icetray_local_reads_total";
/// Bytes of those local reads.
pub const LOCAL_BYTES_TOTAL: &str = "icetray_local_bytes_total";
/// Second requests issued because a lookup GET was slow (RFD 013).
pub const HEDGES_TOTAL: &str = "icetray_hedges_total";
/// Pages served from the page cache without a fetch.
pub const PAGE_CACHE_HITS_TOTAL: &str = "icetray_page_cache_hits_total";
/// Page fetches the page cache ran, after coalescing: a span fetch counts
/// once however many pages it holds.
pub const PAGE_CACHE_FETCHES_TOTAL: &str = "icetray_page_cache_fetches_total";
/// Value reads the row cache served without reading a page (RFD 026),
/// a cached null included.
pub const ROW_CACHE_HITS_TOTAL: &str = "icetray_row_cache_hits_total";
/// Value reads the row cache did not hold (RFD 026), each then read from
/// the page cache.
pub const ROW_CACHE_MISSES_TOTAL: &str = "icetray_row_cache_misses_total";
/// Lookups answered, by `table` and `result`.
pub const LOOKUPS_TOTAL: &str = "icetray_lookups_total";
/// Value-page GETs lookups and Scans issued to object storage, by `table`
/// and `column` (RFD 022, RFD 025): each GET of a column's value page or
/// dictionary page, or a span of its pages, counted once where it is
/// issued, whether or not its call then succeeds; a fetch another lookup
/// joins is not counted again, and hedges are `HEDGES_TOTAL`'s. An ordered
/// table's order column counts its GETs under its own name.
pub const VALUE_PAGE_GETS_TOTAL: &str = "icetray_value_page_gets_total";
/// Bytes of the values each lookup the core answered read, by `table` and
/// `column` (RFD 024): a present value's bytes as the page held them, a
/// default (RFD 023) as its bytes, a null and a miss as none. A lookup that
/// fails counts nothing, and a failed `MultiGet` counts the keys it answered
/// before the failure, as `LOOKUPS_TOTAL` does.
pub const VALUE_BYTES_READ_TOTAL: &str = "icetray_value_bytes_read_total";
/// Bytes of the values each lookup the core answered returned, by `table`
/// and `column` (RFD 024), counted as `VALUE_BYTES_READ_TOTAL` is: a value
/// read whole as it was read, a projected one as its trimmed bytes.
pub const VALUE_BYTES_RETURNED_TOTAL: &str = "icetray_value_bytes_returned_total";
/// Malformed values projected lookups met, by `table` and `column` (RFD
/// 024): each failed its lookup as `FAILED_PRECONDITION`.
pub const PROJECTION_ERRORS_TOTAL: &str = "icetray_projection_errors_total";
/// Refreshes that failed and left the previous generation serving, by
/// `table` and `reason`.
pub const REFRESH_ERRORS_TOTAL: &str = "icetray_refresh_errors_total";
/// Generations swapped in, by `table`.
pub const GENERATION_SWAPS_TOTAL: &str = "icetray_generation_swaps_total";
/// Configuration reloads that read a changed file, by `result`: applied or
/// refused (RFD 018).
pub const CONFIG_RELOADS_TOTAL: &str = "icetray_config_reloads_total";
/// Scans of one key an ordered table read, by `table` and `result`
/// (`ScanResult`): each key of a `MultiScan` counts one (RFD 025).
pub const SCANS_TOTAL: &str = "icetray_scans_total";

/// Lookup latency in seconds, by `table` and `result`.
pub const LOOKUP_SECONDS: &str = "icetray_lookup_seconds";
/// Object-store GET latency in seconds, by `kind`.
pub const OBJECT_GET_SECONDS: &str = "icetray_object_get_seconds";
/// Rows each answered Scan of one key returned, by `table` (RFD 025).
pub const SCAN_ROWS: &str = "icetray_scan_rows";
/// Files holding the key each answered Scan of one key read, by `table`
/// (RFD 025): one for a key held in one file, as on a table compacted to
/// one file per bucket; more for an edge key that a compaction rolling
/// files by size split across files, and for a key in files that appends
/// overlap.
pub const SCAN_FILES: &str = "icetray_scan_files";
/// Latency in seconds of each answered Scan of one key, by `table` and
/// `temperature` (`Temperature`, RFD 025).
pub const SCAN_SECONDS: &str = "icetray_scan_seconds";
/// The encoded size in bytes of each Scan or `MultiScan` response, by
/// `table`, one sample per call, one the server refused above
/// `server.max_scan_bytes` included (RFD 025, Limits).
pub const SCAN_RESPONSE_BYTES: &str = "icetray_scan_response_bytes";

// Every gauge below is written on the scrape path (the binary's
// `Scrape::publish`, through `Table::publish_gauges` and the like), never
// only set once on an event. The binary's recorder drops a gauge no scrape wrote
// since the scrape before, so a gauge set on an event reads in one scrape
// and is gone from the next. `test_scrape_keeps_every_gauge_the_scrape_before_exposed`
// in `crates/icetray/tests/serve.rs` scrapes twice and fails on such a gauge.

/// The resident index's live bytes, by `table`: what a residency tier sheds,
/// which process memory does not show on every platform.
pub const INDEX_BYTES: &str = "icetray_index_bytes";
/// The process's resident set size.
pub const PROCESS_RSS_BYTES: &str = "icetray_process_rss_bytes";
/// Bytes the program holds in live allocations, as jemalloc counts them.
pub const ALLOCATOR_ALLOCATED_BYTES: &str = "icetray_allocator_allocated_bytes";
/// Bytes in jemalloc's pages that hold live allocations: at least
/// `allocated`, the excess being space those pages hold free.
pub const ALLOCATOR_ACTIVE_BYTES: &str = "icetray_allocator_active_bytes";
/// Bytes of jemalloc's own bookkeeping.
pub const ALLOCATOR_METADATA_BYTES: &str = "icetray_allocator_metadata_bytes";
/// Bytes jemalloc has resident: active pages, its metadata, and freed pages
/// it has not yet returned to the operating system.
pub const ALLOCATOR_RESIDENT_BYTES: &str = "icetray_allocator_resident_bytes";
/// Bytes of address space jemalloc keeps mapped for reuse after returning
/// its pages: virtual, not resident.
pub const ALLOCATOR_RETAINED_BYTES: &str = "icetray_allocator_retained_bytes";
/// Bytes the page cache holds in memory.
pub const PAGE_CACHE_BYTES: &str = "icetray_page_cache_bytes";
/// Bytes the row cache holds against its budget (RFD 026): each entry's
/// value at its allocation size plus the per-entry overhead.
pub const ROW_CACHE_BYTES: &str = "icetray_row_cache_bytes";
/// Bytes by which the cache budget, with the resident indexes of the
/// tables served, overruns the memory limit (RFD 028): zero when they fit
/// or when no limit can be read.
pub const CACHE_BUDGET_SHORTFALL_BYTES: &str = "icetray_cache_budget_shortfall_bytes";
/// Findings the doctor raised on the serving generation, by `table` and
/// `code`, and by `column` for a code about one value column (RFD 022,
/// `FindingCode::per_column`).
pub const FINDINGS: &str = "icetray_findings";
/// Seconds from the served snapshot's timestamp to now, by `table`.
pub const GENERATION_AGE_SECONDS: &str = "icetray_generation_age_seconds";
/// The serving snapshot's sequence number, by `table`: one series per
/// table, which each swap overwrites.
pub const GENERATION_SEQUENCE_NUMBER: &str = "icetray_generation_sequence_number";
/// When the serving generation was swapped in, in seconds since the Unix
/// epoch, by `table`.
pub const GENERATION_SWAPPED_AT_SECONDS: &str = "icetray_generation_swapped_at_seconds";
/// 1 for the table's current state and 0 for the others, by `table` and `state`.
pub const TABLE_STATE: &str = "icetray_table_state";
/// 1 while the table is below its declared level, else 0, by `table` (RFD
/// 020): for its layout, the newest snapshot it measured, served or held;
/// for the page cache, the serving generation's chunk files.
pub const TABLE_BELOW_LEVEL: &str = "icetray_table_below_level";
/// The level the table achieves, by `table`: the newest measured
/// snapshot's for its layout, capped at balanced while the serving
/// generation's chunk files are short of the page cache; 0 for optimal, 1
/// for balanced, 2 for permissive (`Level::ordinal`).
pub const TABLE_LEVEL: &str = "icetray_table_level";
/// The level the table declares, by `table`, in `TABLE_LEVEL`'s numbers.
pub const TABLE_DECLARED_LEVEL: &str = "icetray_table_declared_level";
/// The level a lookup requesting one value column achieves, by `table`
/// and `column` (RFD 022, `ColumnCost::level`), on the snapshot
/// `TABLE_LEVEL` measures: the serving generation, or while the table is
/// held, the snapshot it refused. Capped by the page cache as
/// `TABLE_LEVEL` is, in its numbers; the table's level is the most
/// expensive column's.
pub const TABLE_COLUMN_LEVEL: &str = "icetray_table_column_level";
/// The fraction of the serving generation's chunk-file pages the kernel's
/// page cache held at the last sample, by `table`, present only while that
/// residency is observed (RFD 020, `Generation::chunk_residency`).
pub const TABLE_CHUNK_RESIDENT_FRACTION: &str = "icetray_table_chunk_resident_fraction";
/// GETs one cold hit makes at worst on the serving generation's layout and
/// residency finding its row and reading the most expensive value column, by
/// `table` (RFD 020, RFD 022, `LookupCost::gets_per_hit`); a key-value
/// table's alone.
pub const TABLE_GETS_PER_HIT: &str = "icetray_table_gets_per_hit";
/// GETs one cold hit spends at worst on one value column, by `table` and
/// `column` (RFD 022, `ColumnCost::gets_per_hit`): a hit naming several
/// columns costs `TABLE_GETS_PER_MISS`, finding the row, and each named
/// column's figure. A key-value table's alone.
pub const TABLE_COLUMN_GETS_PER_HIT: &str = "icetray_table_column_gets_per_hit";
/// GETs one miss makes at worst on the serving generation, by `table`
/// (`LookupCost::gets_per_miss`); a key-value table's alone.
pub const TABLE_GETS_PER_MISS: &str = "icetray_table_gets_per_miss";
/// GETs one cold Scan at `max_scan_rows` of a key held in one file and one
/// row group makes at worst on the serving generation's layout and
/// residency, reading every configured value column, by `table` (RFD 025,
/// `Generation::gets_per_scan`); an ordered table's alone.
pub const TABLE_GETS_PER_SCAN: &str = "icetray_table_gets_per_scan";
/// Files one lookup probes at most on the serving generation, by `table`
/// (`LookupCost::files_probed`).
pub const TABLE_FILES_PROBED_PER_LOOKUP: &str = "icetray_table_files_probed_per_lookup";
/// Rows the serving generation's deletes remove, by `table` (RFD 021,
/// `DeleteSummary::rows`).
pub const TABLE_DELETED_ROWS: &str = "icetray_table_deleted_rows";
/// Those rows over the serving snapshot's data rows, by `table`
/// (`DeleteSummary::fraction`).
pub const TABLE_DELETED_FRACTION: &str = "icetray_table_deleted_fraction";
/// Live position delete files and deletion vectors whose rows the serving
/// generation applies, by `table` (`DeleteSummary::files`).
pub const TABLE_DELETE_FILES: &str = "icetray_table_delete_files";
/// Bytes the serving generation's delete state holds, also counted in
/// `INDEX_BYTES`, by `table` (`DeleteSummary::state_bytes`).
pub const TABLE_DELETE_STATE_BYTES: &str = "icetray_table_delete_state_bytes";
/// Seconds from the commit of the snapshot that committed the oldest
/// applying delete to now, 0 with none, by `table`
/// (`Generation::oldest_delete_committed`).
pub const TABLE_OLDEST_DELETE_AGE_SECONDS: &str = "icetray_table_oldest_delete_age_seconds";
/// 1 while the configuration file differs from the configuration the
/// server applied, 0 once they match (RFD 018).
pub const CONFIG_PENDING: &str = "icetray_config_pending";
/// `Get`, `MultiGet`, `Scan`, and `MultiScan` calls admitted and not yet
/// ended, each holding one of the `max_in_flight` slots.
pub const REQUESTS_IN_FLIGHT: &str = "icetray_requests_in_flight";
/// The in-flight slots there are, `server.max_in_flight`: a call that finds
/// every one held is refused `RESOURCE_EXHAUSTED`.
pub const MAX_IN_FLIGHT: &str = "icetray_max_in_flight";

/// Which `metrics` describe macro names a metric, by what it records.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Kind {
    /// A count that only rises.
    Counter,
    /// A value that is set, by every scrape (see the gauge constants).
    Gauge,
    /// A distribution of samples, in buckets.
    Histogram,
}

/// Every metric a process can emit, with its kind and the one sentence its
/// `# HELP` line carries; the unit is in the name or the sentence.
pub const DESCRIPTIONS: [(&str, Kind, &str); 58] = [
    (
        REQUESTS_TOTAL,
        Kind::Counter,
        "gRPC calls answered, by call and the status code each answered.",
    ),
    (
        OBJECT_GETS_TOTAL,
        Kind::Counter,
        "Object-store GETs issued, hedges included, by read kind.",
    ),
    (
        OBJECT_BYTES_TOTAL,
        Kind::Counter,
        "Bytes requested by object-store GETs, by read kind.",
    ),
    (
        LOCAL_READS_TOTAL,
        Kind::Counter,
        "Reads served from the local directory.",
    ),
    (
        LOCAL_BYTES_TOTAL,
        Kind::Counter,
        "Bytes read from the local directory.",
    ),
    (
        HEDGES_TOTAL,
        Kind::Counter,
        "Second GETs issued because a lookup read was slow.",
    ),
    (
        PAGE_CACHE_HITS_TOTAL,
        Kind::Counter,
        "Pages served from the page cache without a fetch.",
    ),
    (
        PAGE_CACHE_FETCHES_TOTAL,
        Kind::Counter,
        "Page fetches the page cache ran, after coalescing.",
    ),
    (
        ROW_CACHE_HITS_TOTAL,
        Kind::Counter,
        "Value reads the row cache served without reading a page, cached nulls included.",
    ),
    (
        ROW_CACHE_MISSES_TOTAL,
        Kind::Counter,
        "Value reads the row cache did not hold and the page cache then served.",
    ),
    (
        LOOKUPS_TOTAL,
        Kind::Counter,
        "Lookups answered, by table and result.",
    ),
    (
        VALUE_PAGE_GETS_TOTAL,
        Kind::Counter,
        "Value-page, dictionary-page, and span GETs issued, hedges excluded, by table and column, an order column under its own name.",
    ),
    (
        VALUE_BYTES_READ_TOTAL,
        Kind::Counter,
        "Bytes of the values answered lookups read, by table and column.",
    ),
    (
        VALUE_BYTES_RETURNED_TOTAL,
        Kind::Counter,
        "Bytes of the values answered lookups returned, after projection, by table and column.",
    ),
    (
        PROJECTION_ERRORS_TOTAL,
        Kind::Counter,
        "Malformed values projected lookups met, by table and column.",
    ),
    (
        REFRESH_ERRORS_TOTAL,
        Kind::Counter,
        "Refreshes that failed and left the previous generation serving, by table and reason.",
    ),
    (
        GENERATION_SWAPS_TOTAL,
        Kind::Counter,
        "Generations a refresh swapped in, by table.",
    ),
    (
        CONFIG_RELOADS_TOTAL,
        Kind::Counter,
        "Configuration reloads that read a changed file, by result.",
    ),
    (
        SCANS_TOTAL,
        Kind::Counter,
        "Scans of one key an ordered table read, each key of a MultiScan one, by table and result.",
    ),
    (
        LOOKUP_SECONDS,
        Kind::Histogram,
        "Lookup latency in seconds, by table and result.",
    ),
    (
        OBJECT_GET_SECONDS,
        Kind::Histogram,
        "Object-store GET latency in seconds, by read kind.",
    ),
    (
        SCAN_ROWS,
        Kind::Histogram,
        "Rows each answered Scan of one key returned, by table.",
    ),
    (
        SCAN_FILES,
        Kind::Histogram,
        "Files holding the key each answered Scan of one key read, by table.",
    ),
    (
        SCAN_SECONDS,
        Kind::Histogram,
        "Latency in seconds of each answered Scan of one key, by table and temperature.",
    ),
    (
        SCAN_RESPONSE_BYTES,
        Kind::Histogram,
        "Encoded size in bytes of each Scan or MultiScan response, a refused one included, by table.",
    ),
    (
        INDEX_BYTES,
        Kind::Gauge,
        "Bytes the serving generation's resident index holds, by table.",
    ),
    (
        PROCESS_RSS_BYTES,
        Kind::Gauge,
        "The process's resident set size in bytes.",
    ),
    (
        ALLOCATOR_ALLOCATED_BYTES,
        Kind::Gauge,
        "Bytes in live allocations, as jemalloc counts them.",
    ),
    (
        ALLOCATOR_ACTIVE_BYTES,
        Kind::Gauge,
        "Bytes in jemalloc's pages that hold live allocations.",
    ),
    (
        ALLOCATOR_METADATA_BYTES,
        Kind::Gauge,
        "Bytes of jemalloc's own bookkeeping.",
    ),
    (
        ALLOCATOR_RESIDENT_BYTES,
        Kind::Gauge,
        "Bytes jemalloc holds resident, freed pages not yet returned included.",
    ),
    (
        ALLOCATOR_RETAINED_BYTES,
        Kind::Gauge,
        "Bytes of address space jemalloc keeps mapped for reuse, not resident.",
    ),
    (
        PAGE_CACHE_BYTES,
        Kind::Gauge,
        "Bytes the page cache holds in memory against its budget.",
    ),
    (
        ROW_CACHE_BYTES,
        Kind::Gauge,
        "Bytes the row cache holds against its budget.",
    ),
    (
        CACHE_BUDGET_SHORTFALL_BYTES,
        Kind::Gauge,
        "Bytes by which the cache budget and the resident indexes overrun the memory limit, zero when they fit.",
    ),
    (
        FINDINGS,
        Kind::Gauge,
        "Findings the doctor raised on the serving generation, by table and code, and by column for a code about one value column.",
    ),
    (
        GENERATION_AGE_SECONDS,
        Kind::Gauge,
        "Seconds since the serving snapshot was committed, by table.",
    ),
    (
        GENERATION_SEQUENCE_NUMBER,
        Kind::Gauge,
        "The serving snapshot's sequence number, by table.",
    ),
    (
        GENERATION_SWAPPED_AT_SECONDS,
        Kind::Gauge,
        "When the serving generation was swapped in, in seconds since the Unix epoch, by table.",
    ),
    (
        TABLE_STATE,
        Kind::Gauge,
        "1 for the table's lifecycle state and 0 for the others, by table and state.",
    ),
    (
        TABLE_BELOW_LEVEL,
        Kind::Gauge,
        "1 while the table is below its declared level for its newest measured snapshot's layout or for the page cache, else 0, by table.",
    ),
    (
        TABLE_LEVEL,
        Kind::Gauge,
        "The lookup level the table achieves, 0 optimal, 1 balanced, 2 permissive, by table.",
    ),
    (
        TABLE_DECLARED_LEVEL,
        Kind::Gauge,
        "The lookup level the table declares, 0 optimal, 1 balanced, 2 permissive, by table.",
    ),
    (
        TABLE_COLUMN_LEVEL,
        Kind::Gauge,
        "The lookup level a lookup naming one value column achieves, 0 optimal, 1 balanced, 2 permissive, by table and column.",
    ),
    (
        TABLE_CHUNK_RESIDENT_FRACTION,
        Kind::Gauge,
        "The fraction of the serving generation's chunk-file pages in the page cache at the last sample, by table.",
    ),
    (
        TABLE_GETS_PER_HIT,
        Kind::Gauge,
        "GETs one cold hit makes at worst finding its row and reading the most expensive value column on the serving generation, by table.",
    ),
    (
        TABLE_COLUMN_GETS_PER_HIT,
        Kind::Gauge,
        "GETs one cold hit spends at worst on one value column of the serving generation, by table and column.",
    ),
    (
        TABLE_GETS_PER_MISS,
        Kind::Gauge,
        "GETs one miss makes at worst on the serving generation's layout and residency, by table.",
    ),
    (
        TABLE_GETS_PER_SCAN,
        Kind::Gauge,
        "GETs one cold Scan of every value column at max_scan_rows, of a key in one file and one row group, makes at worst on the serving generation, by table.",
    ),
    (
        TABLE_FILES_PROBED_PER_LOOKUP,
        Kind::Gauge,
        "Files one lookup probes at most on the serving generation's layout, by table.",
    ),
    (
        TABLE_DELETED_ROWS,
        Kind::Gauge,
        "Rows the serving generation's deletes remove from the data files it holds, by table.",
    ),
    (
        TABLE_DELETED_FRACTION,
        Kind::Gauge,
        "Rows the serving generation's deletes remove over the rows of its data files, by table.",
    ),
    (
        TABLE_DELETE_FILES,
        Kind::Gauge,
        "Position delete files and deletion vectors whose rows the serving generation applies, by table.",
    ),
    (
        TABLE_DELETE_STATE_BYTES,
        Kind::Gauge,
        "Bytes the serving generation's delete state holds, also counted in icetray_index_bytes, by table.",
    ),
    (
        TABLE_OLDEST_DELETE_AGE_SECONDS,
        Kind::Gauge,
        "Seconds since the snapshot that committed the oldest applying delete, 0 with none, by table.",
    ),
    (
        CONFIG_PENDING,
        Kind::Gauge,
        "1 while the configuration file differs from the configuration applied, else 0.",
    ),
    (
        REQUESTS_IN_FLIGHT,
        Kind::Gauge,
        "Get, MultiGet, Scan, and MultiScan calls admitted and not yet ended.",
    ),
    (
        MAX_IN_FLIGHT,
        Kind::Gauge,
        "The in-flight slots there are, server.max_in_flight.",
    ),
];

/// Gives every metric of `DESCRIPTIONS` its description in the current
/// recorder, for its `# HELP` line; a process calls this once it has
/// installed its recorder.
pub fn describe() {
    for (name, kind, help) in DESCRIPTIONS {
        match kind {
            Kind::Counter => metrics::describe_counter!(name, help),
            Kind::Gauge => metrics::describe_gauge!(name, help),
            Kind::Histogram => metrics::describe_histogram!(name, help),
        }
    }
}

/// The `call` label: the RPC, from the RPC enum's `Display`.
pub const LABEL_CALL: &str = "call";
/// The `kind` label: the read path, from `ReadKind`.
pub const LABEL_KIND: &str = "kind";
/// The `table` label: the table's configured name.
pub const LABEL_TABLE: &str = "table";
/// The `column` label: a value column's configured name (RFD 022).
pub const LABEL_COLUMN: &str = "column";
/// The `result` label: a lookup's outcome, from `LookupResult`, or a
/// reload's.
pub const LABEL_RESULT: &str = "result";
/// The `reason` label: why a refresh failed, from `ErrorKind`, which gives
/// the refusal's own name for a refusal.
pub const LABEL_REASON: &str = "reason";
/// The `code` label: the finding, from `FindingCode`, or the gRPC status a
/// request answered, in the gRPC specification's name in lower snake case.
pub const LABEL_CODE: &str = "code";
/// The `state` label: the table's lifecycle state, from `TableState`.
pub const LABEL_STATE: &str = "state";
/// The `temperature` label: whether a Scan read a page from object
/// storage, from `Temperature`.
pub const LABEL_TEMPERATURE: &str = "temperature";

/// Which path a GET serves; the `kind` label. Load reads (footers, key
/// chunks, metadata) are bulk work nobody waits on and are never hedged;
/// lookup reads (value pages, and key pages under the bloom tier) are what a
/// client waits on and are the only ones hedged (RFD 013). Display gives the
/// label value.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum ReadKind {
    /// A read made while a generation is built.
    Load,
    /// A read made to answer a client's lookup.
    Lookup,
}

impl fmt::Display for ReadKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let name = match self {
            Self::Load => "load",
            Self::Lookup => "lookup",
        };
        f.write_str(name)
    }
}

/// The three outcomes of a lookup as the `result` metric label sees them.
/// Display gives the `snake_case` label value.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum LookupResult {
    /// The key was present and a value or dictionary page of a column it
    /// named came from object storage, a fetch it joined included.
    ColdHit,
    /// The key was present and no value page came from object storage:
    /// every one was cached, its file predates a column and answered that
    /// column's default with no page read (RFD 023), or the lookup named no
    /// column (RFD 022).
    WarmHit,
    /// The key was absent.
    Miss,
}

impl LookupResult {
    /// Every outcome, for the per-result series of a table.
    pub const ALL: [Self; 3] = [Self::ColdHit, Self::WarmHit, Self::Miss];
}

impl fmt::Display for LookupResult {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let name = match self {
            Self::ColdHit => "cold_hit",
            Self::WarmHit => "warm_hit",
            Self::Miss => "miss",
        };
        f.write_str(name)
    }
}

/// The three outcomes of a Scan of one key as the `result` label of
/// `SCANS_TOTAL` sees them. Display gives the label value.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum ScanResult {
    /// The Scan returned at least one row.
    Rows,
    /// The Scan returned no row: the key has none inside the bounds.
    Empty,
    /// The Scan's read failed.
    Error,
}

impl ScanResult {
    /// Every outcome, for the per-result series of a table.
    pub const ALL: [Self; 3] = [Self::Rows, Self::Empty, Self::Error];
}

impl fmt::Display for ScanResult {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let name = match self {
            Self::Rows => "rows",
            Self::Empty => "empty",
            Self::Error => "error",
        };
        f.write_str(name)
    }
}

/// Whether a Scan read any order or value page from object storage, a
/// fetch it ran or joined: the `temperature` label of `SCAN_SECONDS`.
/// Display gives the label value.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Temperature {
    /// Some page came from object storage.
    Cold,
    /// Every page came from the cache or, under `disk`, the chunk file.
    Warm,
}

impl Temperature {
    /// Every temperature, for the per-temperature series of a table.
    pub const ALL: [Self; 2] = [Self::Cold, Self::Warm];
}

impl fmt::Display for Temperature {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let name = match self {
            Self::Cold => "cold",
            Self::Warm => "warm",
        };
        f.write_str(name)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use super::*;

    /// Every description names a distinct metric under the `icetray_`
    /// prefix and is one sentence.
    #[test]
    fn test_descriptions_name_each_metric_once_in_one_sentence() {
        let mut names = HashSet::new();
        for (name, _, help) in DESCRIPTIONS {
            assert!(names.insert(name), "{name} described twice");
            assert!(name.starts_with("icetray_"), "{name}");
            assert!(help.ends_with('.'), "{name}: {help}");
            assert_eq!(help.matches(". ").count(), 0, "{name}: {help}");
        }
    }

    #[test]
    fn test_fmt_lookup_result_snake_case_names() {
        let cases = [
            (LookupResult::ColdHit, "cold_hit"),
            (LookupResult::WarmHit, "warm_hit"),
            (LookupResult::Miss, "miss"),
        ];
        for (result, name) in cases {
            assert_eq!(result.to_string(), name);
        }
    }

    #[test]
    fn test_fmt_scan_labels_snake_case_names() {
        let results = [
            (ScanResult::Rows, "rows"),
            (ScanResult::Empty, "empty"),
            (ScanResult::Error, "error"),
        ];
        for (result, name) in results {
            assert_eq!(result.to_string(), name);
        }
        for (temperature, name) in [(Temperature::Cold, "cold"), (Temperature::Warm, "warm")] {
            assert_eq!(temperature.to_string(), name);
        }
    }

    #[test]
    fn test_fmt_read_kind_label_values() {
        let cases = [(ReadKind::Load, "load"), (ReadKind::Lookup, "lookup")];
        for (kind, name) in cases {
            assert_eq!(kind.to_string(), name);
        }
    }
}
