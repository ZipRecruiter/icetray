//! Configuration (RFD 011): one schema read from TOML or JSON into one
//! structure and validated once, so no setting is accepted in one syntax
//! and refused in the other.

use std::borrow::Cow;
use std::collections::{BTreeMap, HashSet};
use std::fmt;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::PathBuf;
use std::time::Duration;

use serde::de::IgnoredAny;
use serde::{Deserialize, Serialize};
use serde_json::error::Category;

use crate::approx;
use crate::aws_catalog;
use crate::budget;
use crate::check::ScanLimits;
use crate::column;
use crate::contract::PAGE_CACHE_DISK_BLOCK_BYTES;
use crate::error::{Error, Result};
use crate::level::Level;
use crate::manifest::{self, ColumnRead, ValueType};
use crate::memory::MemoryLimit;
use crate::storage::{self, LocationError};
use crate::time;

/// The first non-whitespace character of a JSON document; a file that
/// starts with it is JSON and every other file is TOML.
const JSON_OPENING: char = '{';

/// The gRPC listener when the file names none. Loopback: RFD 012 has no
/// mode in which the port is safe to expose directly, so reaching it from
/// another host is a decision the deployer writes down. 7433 carries no
/// meaning beyond being the documented default; `DEFAULT_METRICS_LISTEN`
/// shares its last three digits so the pair reads as one.
const DEFAULT_LISTEN: SocketAddr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 7433);

/// The listener for `/metrics` and `/ready` (RFD 012) when the file names
/// none. Loopback for the same reason as `DEFAULT_LISTEN`; 9433 sits beside
/// the gRPC port so the pair is recognizable together.
const DEFAULT_METRICS_LISTEN: SocketAddr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 9433);

/// How often a table's ref is re-resolved when the file says nothing. An
/// unchanged poll costs one read (RFD 005), so ten seconds is cheap, and a
/// committed snapshot is serving within one poll of its commit.
const DEFAULT_POLL_INTERVAL: Duration = Duration::from_secs(10);

/// The shortest poll interval a configuration may name: one second. Each
/// table polls on the interval, and a poll of a catalog table is a catalog
/// call and a poll of a table root reads its version hint, so a shorter one
/// is a busy loop against the catalog that no freshness need justifies.
const MIN_POLL_INTERVAL: Duration = Duration::from_secs(1);

/// The longest poll interval a configuration may name: one day. A table
/// written once a day is still followed within a day of its commit, and the
/// gauges sampled every interval are at most a day old. A day is also far
/// inside what the runtime's `Instant` arithmetic holds, where the first
/// tick is scheduled as now plus the interval and an interval as long as
/// `Duration::MAX` overflows it.
const MAX_POLL_INTERVAL: Duration = Duration::from_hours(24);

/// The share of the process's memory limit (`memory::limit`) that
/// `server.cache_bytes` is when the file sets none: a half. The limit, not
/// the memory free, so every start of a replica gets the same budget; the
/// other half holds the resident indexes, a second generation while a
/// refresh loads, requests in flight, the allocator, and the kernel's cache
/// of `disk` tables' chunk files (RFD 028).
pub(crate) const DEFAULT_CACHE_SHARE_OF_LIMIT: f64 = 0.5;

/// `server.cache_bytes` when the file sets none and no memory limit can be
/// read: 256 mebibytes, a few thousand pages at the layout's 64-kibibyte
/// bound (RFD 002), enough for a warm working set on a small replica while
/// leaving memory to the resident index.
pub const FALLBACK_CACHE_BYTES: u64 = 256 * 1024 * 1024;

/// The share of the cache budget the row cache holds when the file sets no
/// `server.row_cache_share` and a table reads through it: nine tenths. On a
/// sampled production trace the whole budget given to rows missed least at
/// every row size and budget measured; the page cache's memory level keeps
/// the rest for the key and dictionary pages every lookup reads, a hit in
/// the row cache included, and for the pages on their way to the disk
/// level (RFD 028).
pub(crate) const DEFAULT_ROW_CACHE_SHARE: f64 = 0.9;

/// The page cache shards `Config::parse` checks a set `server.cache_bytes`
/// against: one, the fewest a cache has on any machine, since the worker
/// count that sets the real number is not known until the caches open on
/// a runtime, where `open_with` supplies it (`budget::resolve`).
const PARSE_CACHE_SHARDS: usize = 1;

/// `strict` when a table says nothing: off. No table is refused for its
/// cost by default; a table whose snapshot's layout falls below its
/// `lookups` level serves it and is flagged below its level (RFD 020).
const DEFAULT_STRICT: bool = false;

/// `row_cache` when a key-value table says nothing: on. Lookups are
/// expected to be skewed, a small share of keys taking most of them, and a
/// table whose callers keep their own cache in front of icetray turns it
/// off (RFD 026).
const DEFAULT_ROW_CACHE: bool = true;

/// Residency when a table says nothing: `full`, RFD 017's fastest tier, one
/// GET per hit and none per miss. The other tiers are chosen by the owner
/// with the doctor's numbers in front of them, never silently. The one home
/// for the default: the command line's `--residency` falls back to it too.
pub const DEFAULT_RESIDENCY: Residency = Residency::Full;

/// Hedging when the file says nothing: on. A minority of stalled GETs is
/// what hedging removes, and the ceiling bounds its cost when there is no
/// tail (RFD 013).
const DEFAULT_HEDGE_ENABLED: bool = true;

/// The latency percentile the hedge delay is taken at when the file says
/// nothing: the 95th, above the typical read and below the tail it exists
/// to cut. A starting point the first real measurement may move (RFD 013).
const DEFAULT_HEDGE_PERCENTILE: f64 = 0.95;

/// Lookup GETs in the per-table latency window when the file says nothing:
/// 256, enough to estimate a 95th percentile and short enough to follow a
/// stall episode (RFD 013).
const DEFAULT_HEDGE_WINDOW: usize = 256;

/// The share of lookup GETs that may be hedged when the file says nothing: five
/// percent, so the GET overhead stays small where reads are billed per GET
/// (RFD 013).
const DEFAULT_HEDGE_MAX_RATE: f64 = 0.05;

/// How long a `Get` may run when the file says nothing: five seconds. A
/// live lookup answers well under 200 ms, so a lookup still running at five
/// seconds is waiting on a wedged store, and failing it frees the caller
/// to fall back. A client's own shorter deadline is the gRPC layer's to
/// enforce, not this one's.
const DEFAULT_GET_DEADLINE: Duration = Duration::from_secs(5);

/// How long a `MultiGet` may run when the file says nothing: fifteen
/// seconds, flat whatever the batch size. A batch runs a bounded number of
/// lookups at a time, so a full batch of cold hits takes several GET round
/// trips; three `Get` deadlines covers that and still fails a wedged store
/// long before a caller's own patience runs out.
const DEFAULT_MULTI_GET_DEADLINE: Duration = Duration::from_secs(15);

/// Requests (`Get`, `MultiGet`, `Scan`, and `MultiScan` together) in flight
/// at once when the file says nothing: 1024. A request that finds every
/// slot taken is refused at once rather than queued, so a wedged store
/// cannot pile up requests until the process runs out of memory. A
/// single-pod guess, to revisit with measurement (RFD 013).
const DEFAULT_MAX_IN_FLIGHT: usize = 1024;

/// Values one `MultiGet` may ask for when the file says nothing: 1000, its
/// keys times the columns it names, a key of a presence-only call counting
/// one (RFD 022). A larger batch is refused before any lookup, so the work
/// one request can ask for, and the answer it holds in memory, is bounded.
/// A single-pod guess, to revisit with measurement (RFD 013).
const DEFAULT_MAX_MULTI_GET_VALUES: usize = 1000;

/// The most rows one Scan of an ordered table returns, the largest limit it
/// takes, when its entry sets no `max_scan_rows`: 1,000 (RFD 025). A
/// quarter of `DEFAULT_MAX_MULTI_SCAN_VALUES`, so a `MultiScan` of one key at
/// the full limit fits with up to three columns.
pub const DEFAULT_MAX_SCAN_ROWS: u32 = 1_000;

/// The least multiple of an ordered table's `max_scan_rows` that
/// `max_multi_scan_values` holds: a `MultiScan` of one key at the full
/// limit and one column counts each row once for its order value and once
/// for the column (RFD 025), so such a call is always valid.
const MULTI_SCAN_VALUES_PER_SCAN_ROW: usize = 2;

/// The least `server.max_multi_scan_values` under which an ordered table
/// of `max_scan_rows` is valid: `MULTI_SCAN_VALUES_PER_SCAN_ROW` values
/// for each row, so a `MultiScan` of one key at the full limit and one
/// column is always valid (RFD 025). The one statement of the rule, which
/// configuration validation and anything that picks a configuration
/// apply.
#[must_use]
pub fn least_max_multi_scan_values(max_scan_rows: u32) -> usize {
    approx::length(max_scan_rows).saturating_mul(MULTI_SCAN_VALUES_PER_SCAN_ROW)
}

/// GETs one batch (a `MultiGet`, a `Scan`, or a `MultiScan`) holds in
/// flight at once when the file says nothing: 64 (RFD 013, RFD 022, RFD
/// 025). Measured against S3, a lone cold batch keeps getting faster up to
/// about 128, while several concurrent batches reach the process's shared
/// GET throughput by 64, past which a larger bound only lets one batch take
/// more of it from the others
/// (docs/analysis/2026-10-08-multi-get-fan-out.md). A full batch of cold
/// hits, a GET for each of `DEFAULT_MAX_MULTI_GET_VALUES` values, takes
/// about sixteen GET round trips, inside `DEFAULT_MULTI_GET_DEADLINE`.
pub(crate) const DEFAULT_BATCH_FAN_OUT: usize = 64;

/// Values one `MultiScan` may ask for when the file says nothing: 4,000,
/// its keys times its limit times one more than its requested columns
/// (RFD 025). A larger call is refused before any key is read, so the work
/// one call can ask for, and the answer it holds in memory, is bounded.
/// Four times `DEFAULT_MAX_MULTI_GET_VALUES`, since a `MultiScan` reads
/// spans of rows where a `MultiGet` reads single values.
pub(crate) const DEFAULT_MAX_MULTI_SCAN_VALUES: usize = 4_000;

/// The encoded size in bytes of one `Scan` or `MultiScan` response the server
/// sends when the file says nothing: 4 MiB, a gRPC client's default message
/// limit (RFD 025). A larger response is refused as `FAILED_PRECONDITION`
/// instead of reaching a client that cannot receive it.
pub(crate) const DEFAULT_MAX_SCAN_BYTES: usize = 4 * 1024 * 1024;

/// How long a `Scan`, and each span GET of one, may run when the file says
/// nothing: five seconds, as long as a `Get` (RFD 025).
pub(crate) const DEFAULT_SCAN_DEADLINE: Duration = Duration::from_secs(5);

/// How long a `MultiScan` may run when the file says nothing: fifteen
/// seconds, flat whatever the call's size, as long as a `MultiGet`
/// (RFD 025).
pub(crate) const DEFAULT_MULTI_SCAN_DEADLINE: Duration = Duration::from_secs(15);

/// How long a build from no serving generation may run when the file says
/// nothing: thirty minutes. Such a build, a first load or a retry after
/// one failed, reads every file's footer and key chunks, so it is allowed
/// several times the largest first load measured (RFD 013). It is bounded
/// all the same: the build holds the table's load lock, which
/// `Opened::retain` waits on for every table in turn.
const DEFAULT_LOAD_DEADLINE: Duration = Duration::from_mins(30);

/// How long a refresh over a serving generation may run when the file says
/// nothing: five minutes. A refresh reuses every file the new snapshot
/// still names and reads only the rest, so it is short, and it is bounded
/// tighter than a first load for the same reason: it holds the table's
/// load lock.
const DEFAULT_REFRESH_DEADLINE: Duration = Duration::from_mins(5);

/// How long a shutdown waits for open connections when the file says
/// nothing: twenty seconds. A request runs at most `DEFAULT_MULTI_GET_DEADLINE`,
/// so twenty seconds lets the last of them finish and leaves time to flush
/// the page cache before an orchestrator's usual thirty-second kill.
const DEFAULT_DRAIN_DEADLINE: Duration = Duration::from_secs(20);

/// Retries of one failed object-store request when the file says nothing:
/// two. A transient error recovers within a retry or two; more retries
/// against a wedged store only hold the request open longer.
const DEFAULT_MAX_RETRIES: usize = 2;

/// How long an object-store connection may take to open when the file says
/// nothing: one second. A connection to the store opens in milliseconds, so
/// a second is already a failure, and it leaves the retries time to try
/// another address.
const DEFAULT_CONNECT_TIMEOUT: Duration = Duration::from_secs(1);

/// How long one catalog call may run when the file says nothing: thirty
/// seconds. A catalog call is on the background path, a poll or a load that
/// nobody waits on, so it is patient, but a catalog that never answers must
/// not hold a poll forever.
const DEFAULT_CATALOG_TIMEOUT: Duration = Duration::from_secs(30);

/// The schemes an AWS source's `endpoint` may start with.
const HTTP_SCHEMES: [&str; 2] = ["http://", "https://"];

/// What `S3Config`'s `Debug` prints in place of the secret access key.
const SECRET_PLACEHOLDER: &str = "[redacted]";

/// Why a configuration could not be read or acted on. A document that is
/// not TOML or JSON at all is `Toml` or `Json`; one that is, but whose
/// content does not fit the schema, is `Schema`; one that fits but breaks
/// a rule is `Invalid`.
#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    /// The configuration file at `path` could not be read, or its bytes are
    /// not UTF-8.
    #[error("cannot read configuration file {}: {source}", path.display())]
    Read {
        /// The file.
        path: PathBuf,
        /// The operating system's refusal, or the decoding failure.
        source: std::io::Error,
    },
    /// The text is not TOML; boxed, since a TOML error is large and every
    /// result that can fail on configuration carries the variant.
    #[error("configuration is not valid toml: {0}")]
    Toml(Box<TomlSyntax>),
    /// The text is not JSON.
    #[error("configuration is not valid json: {0}")]
    Json(serde_json::Error),
    /// The text is TOML or JSON, but the setting at `at` does not fit the
    /// schema: a required key missing, a key the schema does not know, a
    /// value of the wrong type or form. A missing key's `at` is the table
    /// that lacks it; the root is not named.
    #[error("invalid configuration: {}", schema_detail(at, fault))]
    Schema {
        /// Where in the document the fault is, as serde walked it:
        /// `tables[0]`, `server.poll_interval`.
        at: serde_path_to_error::Path,
        /// What does not fit; boxed, since a TOML error is large and every
        /// result that can fail on configuration carries the variant.
        fault: Box<SchemaFault>,
    },
    /// The configuration parsed but breaks a rule.
    #[error("invalid configuration: {0}")]
    Invalid(#[from] ConfigViolation),
}

/// Why a text is not TOML, on one line: the parser's message after the
/// line and column it stopped at. The parser's own `Display` prints the
/// source excerpt over several lines, which a report of one line per
/// failure cannot hold.
#[derive(Debug, thiserror::Error)]
#[error("{}", syntax_detail(*.at, .source))]
pub struct TomlSyntax {
    /// Where the parser stopped, when it says.
    pub at: Option<TextPosition>,
    /// The parser's error.
    pub source: toml::de::Error,
}

impl TomlSyntax {
    /// `source`, raised while parsing `text`, with the place it names.
    fn new(text: &str, source: toml::de::Error) -> Self {
        Self {
            at: source.span().map(|span| TextPosition::of(text, span.start)),
            source,
        }
    }
}

/// `TomlSyntax`'s message: `at` as `line L, column C: `, then the parser's
/// message.
fn syntax_detail(at: Option<TextPosition>, source: &toml::de::Error) -> String {
    match at {
        Some(TextPosition { line, column }) => {
            format!("line {line}, column {column}: {}", source.message())
        }
        None => source.message().to_string(),
    }
}

/// A place in a text, as an editor counts it: the line from one, and the
/// column from one in characters.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TextPosition {
    /// The line, from one.
    pub line: usize,
    /// The character in the line, from one.
    pub column: usize,
}

impl TextPosition {
    /// The place of byte `offset` in `text`; an offset past the end or
    /// inside a character is taken as the end of the text.
    fn of(text: &str, offset: usize) -> Self {
        let before = text.get(..offset).unwrap_or(text);
        let line_start = before.rfind('\n').map_or(0, |newline| newline + 1);
        Self {
            line: before.matches('\n').count() + 1,
            column: before[line_start..].chars().count() + 1,
        }
    }
}

/// What in a document does not fit the schema, in the syntax's own words.
#[derive(Debug, thiserror::Error)]
pub enum SchemaFault {
    /// From a TOML document; its message alone, without the source excerpt
    /// the TOML error prints, which frames every fault as a parse error.
    #[error("{}", .0.message())]
    Toml(toml::de::Error),
    /// From a JSON document, with the line and column it carries.
    #[error("{0}")]
    Json(serde_json::Error),
}

/// A schema fault prefixed by where it is, unless it is at the root: what
/// `ConfigError::Schema` displays after its framing, for a caller that
/// frames it with the file the text came from.
pub fn schema_detail(at: &serde_path_to_error::Path, fault: &SchemaFault) -> String {
    let place = place(at);
    if place.is_empty() {
        fault.to_string()
    } else {
        format!("{place}: {fault}")
    }
}

/// `at` as the file spells a setting's place: keys joined by `.`, a list
/// entry as `[index]`. A step serde could not name, such as a map key that
/// is not a string, is left out, so the place reads as far as it is known.
fn place(at: &serde_path_to_error::Path) -> String {
    let mut place = String::new();
    for segment in at {
        match segment {
            serde_path_to_error::Segment::Seq { index } => {
                place.push('[');
                place.push_str(&index.to_string());
                place.push(']');
            }
            serde_path_to_error::Segment::Map { key: name }
            | serde_path_to_error::Segment::Enum { variant: name } => {
                if !place.is_empty() {
                    place.push('.');
                }
                place.push_str(name);
            }
            serde_path_to_error::Segment::Unknown => {}
        }
    }
    place
}

/// A rule a parsed configuration breaks. The set is closed; a rule not
/// listed here is not checked.
#[derive(Clone, Debug, PartialEq, thiserror::Error)]
pub enum ConfigViolation {
    /// No table is configured: a server would serve nothing and report
    /// ready, and a one-shot would have nothing to open. The file is at
    /// fault, so `serve` refuses to start, a reload is refused, and a
    /// one-shot fails, as for any other violation.
    #[error("no table is configured; list at least one under [[tables]]")]
    NoTables,
    /// A table has an empty name.
    #[error("tables[{index}].name is empty")]
    EmptyTableName {
        /// The table's position in `tables`.
        index: usize,
    },
    /// A table lists a value column with an empty name (RFD 022).
    #[error("tables[{index}].value_columns lists an empty name")]
    EmptyValueColumn {
        /// The table's position in `tables`.
        index: usize,
    },
    /// A table's key column or one of its value columns has a name that is
    /// not a valid Avro name (`column::is_served_name`, RFD 023). Iceberg's
    /// Java writers store such a column under an escaped name, so the name
    /// in the files would differ from the schema's.
    #[error(
        "tables[{index}].{setting} names {column:?}, which icetray does not serve: a column name is ASCII letters, digits, and underscores, and does not start with a digit"
    )]
    ColumnNameRewritten {
        /// The table's position in `tables`.
        index: usize,
        /// The setting that names the column.
        setting: ColumnSetting,
        /// The name as configured.
        column: String,
    },
    /// A table lists one value column twice (RFD 022).
    #[error("tables[{index}].value_columns lists {column:?} more than once")]
    DuplicateValueColumn {
        /// The table's position in `tables`.
        index: usize,
        /// The column listed twice.
        column: String,
    },
    /// A table lists its key column as a value column (RFD 022): a lookup
    /// already names the key, so serving it again carries nothing.
    #[error("tables[{index}].value_columns lists the key column {column:?}")]
    KeyColumnAsValue {
        /// The table's position in `tables`.
        index: usize,
        /// The key column's name.
        column: String,
    },
    /// A table names its key column as its order column (RFD 025): the
    /// key already orders the rows, so it orders nothing within a key.
    #[error("tables[{index}].order_column names the key column")]
    OrderColumnIsKey {
        /// The table's position in `tables`.
        index: usize,
    },
    /// A table lists its order column as a value column (RFD 025): a Scan
    /// already returns each row's order value.
    #[error("tables[{index}].value_columns lists the order column {column:?}")]
    OrderColumnIsValue {
        /// The table's position in `tables`.
        index: usize,
        /// The order column's name.
        column: String,
    },
    /// A table sets `max_scan_rows` without an `order_column`: only an
    /// ordered table is scanned (RFD 025).
    #[error(
        "tables[{index}].max_scan_rows is set, but the table sets no order_column, and only an ordered table is scanned"
    )]
    MaxScanRowsOnKeyValueTable {
        /// The table's position in `tables`.
        index: usize,
    },
    /// An ordered table sets `row_cache`, to either value: a Scan reads a
    /// range of rows, which only the index and the pages can say is
    /// complete, so an ordered table never reads through the row cache
    /// (RFD 026).
    #[error(
        "tables[{index}].row_cache is set, but the table sets an order_column, and only a key-value table reads through the row cache"
    )]
    RowCacheOnOrderedTable {
        /// The table's position in `tables`.
        index: usize,
    },
    /// An ordered table's `max_scan_rows` is zero, so every Scan would
    /// return nothing. Raised by validation, which names the setting by the
    /// table's place in `tables`, and again by a load or a retune, which
    /// knows the table by name alone.
    #[error("{}", scan_rows_zero(table, *index))]
    MaxScanRowsZero {
        /// The table's name.
        table: String,
        /// The table's position in `tables`, when validation found it.
        index: Option<usize>,
    },
    /// `server.max_multi_scan_values` is below the least an ordered table's
    /// `max_scan_rows` allows (`least_max_multi_scan_values`), so a
    /// `MultiScan` of one key at the full limit and one column would be
    /// refused (RFD 025).
    #[error(
        "server.max_multi_scan_values {max_multi_scan_values} is below {least}, the least tables[{index}].max_scan_rows {max_scan_rows} allows, so a MultiScan of one key at the full limit and one column would be refused",
        least = least_max_multi_scan_values(*.max_scan_rows)
    )]
    MaxMultiScanValuesBelowScanRows {
        /// The ordered table's position in `tables`.
        index: usize,
        /// Its `max_scan_rows`, as `TableConfig::scan_rows` resolves it.
        max_scan_rows: u32,
        /// The configured `server.max_multi_scan_values`.
        max_multi_scan_values: usize,
    },
    /// A table has the name of a table before it.
    #[error("tables[{index}].name {name} is configured more than once")]
    DuplicateTableName {
        /// The later table's position in `tables`.
        index: usize,
        /// The name both carry.
        name: String,
    },
    /// One of the two static S3 keys is set without the other.
    #[error(
        "storage.s3.access_key_id and storage.s3.secret_access_key are set together or not at all"
    )]
    IncompleteStaticKeys,
    /// A resident-bytes budget on a table whose residency has nothing to budget.
    #[error(
        "tables[{index}].resident_budget_bytes {bytes} is set, but tables[{index}].residency {residency} takes none"
    )]
    BudgetWithoutResidency {
        /// The table's position in `tables`.
        index: usize,
        /// The configured budget.
        bytes: u64,
        /// Its residency.
        residency: Residency,
    },
    /// Disk residency with no local directory to write chunk files to.
    /// Raised by validation, which names the setting by the table's place
    /// in `tables`, and again by a load, which knows the table by name
    /// alone.
    #[error("{}", disk_without_local_dir(table, *index))]
    DiskResidencyWithoutLocalDir {
        /// The table's name.
        table: String,
        /// The table's position in `tables`, when validation found it.
        index: Option<usize>,
    },
    /// A `lookups` level the table's residency cannot reach (RFD 020):
    /// every snapshot would be below it. Raised by validation, which names
    /// the setting by the table's place in `tables`, and again by a load,
    /// which knows the table by name alone.
    #[error("{}", lookups_unreachable(table, *index, *lookups, *residency))]
    LookupsUnreachable {
        /// The table's name.
        table: String,
        /// The table's position in `tables`, when validation found it.
        index: Option<usize>,
        /// The declared level.
        lookups: Level,
        /// Its residency.
        residency: Residency,
    },
    /// The memory budget of the page cache, its memory level's part of
    /// `cache_bytes`, cannot hold one value page in every shard of its
    /// memory level, so the cache would keep a page per shard however small
    /// the budget, and the budget would bound nothing
    /// (`cache::min_memory_bytes`). `Config::parse` checks a set budget
    /// against one shard, the least any cache has; opening the caches
    /// checks it against the shards they have on the runtime they open on,
    /// one per worker thread rounded up to a power of two, and checks a
    /// budget derived from the memory limit only then.
    #[error("{}", page_cache_too_small(*bytes, *minimum, *shards, *limit))]
    PageCacheTooSmall {
        /// The budget.
        bytes: u64,
        /// The smallest budget that holds a value page in every shard: one
        /// contract value page with its entry overhead per shard.
        minimum: u64,
        /// The shards the minimum is for.
        shards: usize,
        /// The memory limit the budget was derived from, when the file
        /// sets no `cache_bytes`.
        limit: Option<MemoryLimit>,
    },
    /// A `[server]` key icetray no longer reads, which the file still sets:
    /// refused rather than ignored, naming the setting that replaced it, so
    /// a deployment does not run on a default it believes it configured.
    #[error("server.{setting} is no longer a setting; use server.{replacement}")]
    RemovedSetting {
        /// The removed key.
        setting: &'static str,
        /// The key that replaced it.
        replacement: &'static str,
    },
    /// `row_cache_share` is outside `[0, 1]` or not a number; it is the
    /// share of `cache_bytes` the row cache holds (RFD 028).
    #[error("server.row_cache_share {share} is not in [0, 1]")]
    RowCacheShareOutOfRange {
        /// The configured share.
        share: f64,
    },
    /// The server serves a table reading through the row cache beside
    /// tables reading pages alone, ordered tables or key-value tables with
    /// `row_cache = false`, with no page-cache disk level (`storage.local`
    /// with `page_cache_disk_bytes` above zero) and no `row_cache_share`
    /// (RFD 028). Those tables' pages would then live in the page cache's
    /// memory level alone, and no default share suits both kinds of table
    /// there, so the file chooses.
    #[error(
        "the server has no page-cache disk level and serves tables reading through the row cache beside tables reading pages alone ({}); set storage.local with page_cache_disk_bytes above zero, or set server.row_cache_share",
        pages_alone.join(", ")
    )]
    MixedServerNeedsShare {
        /// The names of the tables reading pages alone, in file order.
        pages_alone: Vec<String>,
    },
    /// A page cache disk level smaller than its one block, which would hold
    /// nothing (RFD 017); zero turns the level off instead.
    #[error(
        "storage.local.page_cache_disk_bytes {bytes} is below one block of {minimum} bytes; zero means no disk level"
    )]
    PageCacheDiskTooSmall {
        /// The configured size.
        bytes: u64,
        /// The smallest level that holds a page: one block.
        minimum: u64,
    },
    /// The hedge percentile is outside `(0, 1]`; the delay is that percentile
    /// of the latency window (RFD 013).
    #[error("storage.hedge.percentile {percentile} is not in (0, 1]")]
    HedgePercentileOutOfRange {
        /// The configured percentile.
        percentile: f64,
    },
    /// The hedge ceiling is outside `[0, 1]`; it is the share of lookup GETs
    /// that may be hedged (RFD 013).
    #[error("storage.hedge.max_rate {max_rate} is not in [0, 1]")]
    HedgeMaxRateOutOfRange {
        /// The configured ceiling.
        max_rate: f64,
    },
    /// The hedge window holds no samples, so it has no percentile.
    #[error("storage.hedge.window 0 holds no sample; it must be at least 1")]
    HedgeWindowEmpty,
    /// A poll interval below `MIN_POLL_INTERVAL`: each table's poll would
    /// call the catalog or read its version hint in a busy loop, and at
    /// zero the runtime's interval refuses the period.
    #[error(
        "server.poll_interval {} is below the minimum of {}",
        time::friendly(*interval),
        time::friendly(*minimum)
    )]
    PollIntervalTooShort {
        /// The configured interval.
        interval: Duration,
        /// `MIN_POLL_INTERVAL`.
        minimum: Duration,
    },
    /// A poll interval above `MAX_POLL_INTERVAL`, past which a table is not
    /// followed and the first tick's `Instant` can overflow.
    #[error(
        "server.poll_interval {} is above the maximum of {}",
        time::friendly(*interval),
        time::friendly(*maximum)
    )]
    PollIntervalTooLong {
        /// The configured interval.
        interval: Duration,
        /// `MAX_POLL_INTERVAL`.
        maximum: Duration,
    },
    /// A `Get` deadline of zero, which would fail every lookup unstarted.
    #[error("server.get_deadline 0s is not above zero")]
    GetDeadlineZero,
    /// A `MultiGet` deadline of zero, which would fail every batch unstarted.
    #[error("server.multi_get_deadline 0s is not above zero")]
    MultiGetDeadlineZero,
    /// No request may be in flight, so every request would be refused.
    #[error("server.max_in_flight 0 is not above zero")]
    MaxInFlightZero,
    /// No value may be asked for in a `MultiGet`, so every batch would be
    /// refused.
    #[error("server.max_multi_get_values 0 is not above zero")]
    MaxMultiGetValuesZero,
    /// No GET may be in flight for a batch, so every `MultiGet`, `Scan`,
    /// and `MultiScan` would wait for a permit until its deadline.
    #[error("server.batch_fan_out 0 is not above zero")]
    BatchFanOutZero,
    /// No value may be asked for in a `MultiScan`, so every call would be
    /// refused.
    #[error("server.max_multi_scan_values 0 is not above zero")]
    MaxMultiScanValuesZero,
    /// No response may be any size, so every `Scan` and `MultiScan` would be
    /// refused.
    #[error("server.max_scan_bytes 0 is not above zero")]
    MaxScanBytesZero,
    /// A `Scan` deadline of zero, which would fail every scan unstarted.
    #[error("server.scan_deadline 0s is not above zero")]
    ScanDeadlineZero,
    /// A `MultiScan` deadline of zero, which would fail every call
    /// unstarted.
    #[error("server.multi_scan_deadline 0s is not above zero")]
    MultiScanDeadlineZero,
    /// A load deadline of zero, which would fail every first load
    /// unstarted.
    #[error("server.load_deadline 0s is not above zero")]
    LoadDeadlineZero,
    /// A refresh deadline of zero, which would fail every refresh
    /// unstarted.
    #[error("server.refresh_deadline 0s is not above zero")]
    RefreshDeadlineZero,
    /// A drain deadline of zero, which would cut every open request off at
    /// shutdown.
    #[error("server.drain_deadline 0s is not above zero")]
    DrainDeadlineZero,
    /// An object-store connect timeout of zero, which would fail every
    /// connection before it opened.
    #[error("storage.connect_timeout 0s is not above zero")]
    ConnectTimeoutZero,
    /// A catalog timeout of zero, which would fail every catalog call
    /// unstarted.
    #[error("storage.catalog_timeout 0s is not above zero")]
    CatalogTimeoutZero,
    /// Two standard log fields emitted under one name, so every line would
    /// lose one of them (RFD 012).
    #[error(
        "server.log.fields: {first} and {second} are both emitted as {name}; each needs its own name"
    )]
    LogFieldCollision {
        /// The emitted name both fields map to.
        name: String,
        /// The field earlier in `StandardField::ALL`.
        first: StandardField,
        /// The field later in `StandardField::ALL`.
        second: StandardField,
    },
    /// The rewrite rule at `index` of `storage.rewrite` is one
    /// `RewriteRule::fault` refuses.
    #[error("storage.rewrite[{index}]: {fault}")]
    Rewrite {
        /// The rule's position in the file's list.
        index: usize,
        /// What is wrong with it.
        fault: RewriteFault,
    },
    /// A source setting is empty: a static location, which as a path would
    /// be the filesystem root, or an AWS source's database, namespace,
    /// table, table bucket ARN, or an optional setting that is present but
    /// names nothing.
    #[error("tables[{index}].source.{setting} is empty")]
    EmptySourceSetting {
        /// The table's position in `tables`.
        index: usize,
        /// The setting.
        setting: SourceSetting,
    },
    /// An AWS source's `endpoint` is not an `http://` or `https://` URL,
    /// so no request could be sent to it.
    #[error("tables[{index}].source.endpoint {endpoint} is not an http:// or https:// url")]
    SourceEndpoint {
        /// The table's position in `tables`.
        index: usize,
        /// The endpoint as configured.
        endpoint: String,
    },
    /// An `s3tables` source's `table_bucket_arn` is not a table bucket
    /// ARN, so it names no bucket and no region.
    #[error(
        "tables[{index}].source.table_bucket_arn {arn} is not a table bucket arn, as arn:aws:s3tables:us-east-1:111122223333:bucket/name is"
    )]
    TableBucketArn {
        /// The table's position in `tables`.
        index: usize,
        /// The ARN as configured.
        arn: String,
    },
    /// A static table's location, after the file's rewrite rules, names no
    /// store the process builds (RFD 003). The locations a table's
    /// metadata names, and a catalog table's, are only known as it loads,
    /// and are refused there as `Error::Location`.
    #[error(
        "tables[{index}].source.location {} {}",
        source.location,
        source.fault
    )]
    TableLocation {
        /// The table's position in `tables`.
        index: usize,
        /// The location as rewritten, and why no store serves it.
        source: LocationError,
    },
    /// Text that names no residency tier; raised by `Residency`'s `FromStr`.
    /// The command line reads the names it offers back through it, and
    /// clap refuses any other text before it gets here.
    #[error("unknown residency {text}; expected one of {}", Residency::names())]
    UnknownResidency {
        /// The text as given.
        text: String,
    },
    /// Text that names no value format; raised by `ValueFormat`'s
    /// `FromStr`, which the command line reads a column's format through.
    #[error(
        "unknown value format {text}; expected one of {}",
        value_format_names()
    )]
    UnknownValueFormat {
        /// The text as given.
        text: String,
    },
}

/// Every value format's name, comma-separated, for the message that refuses
/// a name; rendered from `ValueFormat::ALL` so the list cannot lag the enum.
fn value_format_names() -> String {
    ValueFormat::ALL
        .iter()
        .map(ToString::to_string)
        .collect::<Vec<_>>()
        .join(", ")
}

/// `ConfigViolation::DiskResidencyWithoutLocalDir`'s message: the setting
/// by its full path when the table's place is known, else the table by name.
fn disk_without_local_dir(table: &str, index: Option<usize>) -> String {
    match index {
        Some(index) => format!(
            "tables[{index}].residency {} needs a local directory under storage.local",
            Residency::Disk
        ),
        None => format!(
            "table {table}: {} residency needs a local directory under storage.local",
            Residency::Disk
        ),
    }
}

/// `ConfigViolation::MaxScanRowsZero`'s message: the setting by its full
/// path when the table's place is known, else the table by name.
fn scan_rows_zero(table: &str, index: Option<usize>) -> String {
    match index {
        Some(index) => format!("tables[{index}].max_scan_rows 0 is not above zero"),
        None => format!("table {table}: max_scan_rows 0 is not above zero"),
    }
}

/// `ConfigViolation::LookupsUnreachable`'s message: the setting by its full
/// path when the table's place is known, else the table by name, and the
/// best level the residency reaches.
fn lookups_unreachable(
    table: &str,
    index: Option<usize>,
    lookups: Level,
    residency: Residency,
) -> String {
    let best = residency.best_level();
    match index {
        Some(index) => format!(
            "tables[{index}].lookups {lookups} is above what tables[{index}].residency {residency} reaches; {residency} reaches {best} at best"
        ),
        None => format!(
            "table {table}: lookups {lookups} is above what {residency} residency reaches; {residency} reaches {best} at best"
        ),
    }
}

/// `ConfigViolation::PageCacheTooSmall`'s message: the budget by its
/// setting, and for a budget derived from the memory limit, the limit and
/// where it comes from, since the file names no number to raise.
fn page_cache_too_small(
    bytes: u64,
    minimum: u64,
    shards: usize,
    limit: Option<MemoryLimit>,
) -> String {
    let refusal = format!(
        "server.cache_bytes gives the page cache {bytes} bytes, below one value page per cache shard, {minimum} bytes for {shards} shards"
    );
    match limit {
        Some(limit) => format!(
            "{refusal}; server.cache_bytes is unset, so it is derived from the {} memory limit of {} bytes: set it, or raise the limit",
            limit.of, limit.bytes
        ),
        None => refusal,
    }
}

/// The whole configuration: the server, storage, and the tables it serves.
/// Every section defaults when absent; a key the schema does not know is an
/// error in every section.
#[derive(Clone, Debug, PartialEq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    /// Listeners, polling, the page cache, and logging.
    #[serde(default)]
    pub server: ServerConfig,
    /// Object storage, hedging, and the optional local directory.
    #[serde(default)]
    pub storage: StorageConfig,
    /// The tables to serve: at least one, with names unique and non-empty.
    #[serde(default)]
    pub tables: Vec<TableConfig>,
}

impl Config {
    /// Parses TOML or JSON, decided by content: a first non-whitespace `{`
    /// is JSON. The result is validated once, so the two syntaxes cannot
    /// disagree on what is accepted. The error is a `ConfigError` alone,
    /// so a caller that read the text from a file can name the file beside
    /// it.
    pub fn parse(text: &str) -> std::result::Result<Self, ConfigError> {
        let config = Self::from_text(text)?;
        validate(&config)?;
        Ok(config)
    }

    /// The text as either syntax reads it, not yet validated. The syntax
    /// is checked apart from the schema, so a document that parses is never
    /// reported as malformed TOML or JSON: TOML is parsed whole before it is
    /// deserialized, and a JSON error is sorted by `serde_json`'s category.
    fn from_text(text: &str) -> std::result::Result<Self, ConfigError> {
        if text.trim_start().starts_with(JSON_OPENING) {
            let mut document = serde_json::Deserializer::from_str(text);
            let config = serde_path_to_error::deserialize(&mut document).map_err(|error| {
                let at = error.path().clone();
                let inner = error.into_inner();
                match inner.classify() {
                    Category::Data => ConfigError::Schema {
                        at,
                        fault: Box::new(SchemaFault::Json(inner)),
                    },
                    Category::Io | Category::Syntax | Category::Eof => ConfigError::Json(inner),
                }
            })?;
            document.end().map_err(ConfigError::Json)?;
            Ok(config)
        } else {
            let document = toml::Deserializer::parse(text)
                .map_err(|error| ConfigError::Toml(Box::new(TomlSyntax::new(text, error))))?;
            Ok(
                serde_path_to_error::deserialize(document).map_err(|error| {
                    ConfigError::Schema {
                        at: error.path().clone(),
                        fault: Box::new(SchemaFault::Toml(error.into_inner())),
                    }
                })?,
            )
        }
    }

    /// The configuration, checked against every rule `parse` checks, so a
    /// configuration built in code is held to the same rules as one read
    /// from a file; the first rule it breaks is `ConfigError::Invalid`.
    pub fn validated(self) -> Result<Self> {
        validate(&self).map_err(ConfigError::Invalid)?;
        Ok(self)
    }

    /// Whether any configured table reads through the row cache
    /// (`reads_rows`).
    pub(crate) fn reads_rows(&self) -> bool {
        reads_rows(&self.tables)
    }

    /// The settings outside `tables` whose value in `next` differs from
    /// this configuration's, by their names in the file, in file order:
    /// what a reload refuses, since the server builds them once at start
    /// (RFD 018). Empty when only the tables differ. Every field of
    /// `[server]` and `[storage]` is named in a destructuring with no rest
    /// pattern, so a setting added later does not compile until it is
    /// compared.
    pub fn restart_settings(&self, next: &Self) -> Vec<&'static str> {
        let mut differing = self.server.differing_settings(&next.server);
        differing.extend(self.storage.differing_settings(&next.storage));
        differing
    }
}

impl ServerConfig {
    /// The names, in file order, of the settings whose value in `next`
    /// differs from this one's.
    fn differing_settings(&self, next: &Self) -> Vec<&'static str> {
        let ServerConfig {
            listen,
            metrics_listen,
            poll_interval,
            cache_bytes,
            row_cache_share,
            removed_page_cache_bytes: _,
            removed_scan_page_cache_bytes: _,
            log,
            get_deadline,
            multi_get_deadline,
            max_in_flight,
            max_multi_get_values,
            batch_fan_out,
            max_multi_scan_values,
            scan_deadline,
            multi_scan_deadline,
            max_scan_bytes,
            load_deadline,
            refresh_deadline,
            drain_deadline,
        } = self;
        let server = next;
        let compared = [
            ("server.listen", *listen == server.listen),
            (
                "server.metrics_listen",
                *metrics_listen == server.metrics_listen,
            ),
            (
                "server.poll_interval",
                *poll_interval == server.poll_interval,
            ),
            ("server.cache_bytes", *cache_bytes == server.cache_bytes),
            (
                "server.row_cache_share",
                row_cache_share.map(f64::to_bits) == server.row_cache_share.map(f64::to_bits),
            ),
            ("server.log", *log == server.log),
            ("server.get_deadline", *get_deadline == server.get_deadline),
            (
                "server.multi_get_deadline",
                *multi_get_deadline == server.multi_get_deadline,
            ),
            (
                "server.max_in_flight",
                *max_in_flight == server.max_in_flight,
            ),
            (
                "server.max_multi_get_values",
                *max_multi_get_values == server.max_multi_get_values,
            ),
            (
                "server.batch_fan_out",
                *batch_fan_out == server.batch_fan_out,
            ),
            (
                "server.max_multi_scan_values",
                *max_multi_scan_values == server.max_multi_scan_values,
            ),
            (
                "server.scan_deadline",
                *scan_deadline == server.scan_deadline,
            ),
            (
                "server.multi_scan_deadline",
                *multi_scan_deadline == server.multi_scan_deadline,
            ),
            (
                "server.max_scan_bytes",
                *max_scan_bytes == server.max_scan_bytes,
            ),
            (
                "server.load_deadline",
                *load_deadline == server.load_deadline,
            ),
            (
                "server.refresh_deadline",
                *refresh_deadline == server.refresh_deadline,
            ),
            (
                "server.drain_deadline",
                *drain_deadline == server.drain_deadline,
            ),
        ];
        compared
            .into_iter()
            .filter(|(_, same)| !same)
            .map(|(name, _)| name)
            .collect()
    }
}

impl StorageConfig {
    /// The names, in file order, of the settings whose value in `next`
    /// differs from this one's.
    fn differing_settings(&self, next: &Self) -> Vec<&'static str> {
        let StorageConfig {
            s3,
            rewrite,
            hedge,
            local,
            max_retries,
            connect_timeout,
            catalog_timeout,
        } = self;
        let storage = next;
        let compared = [
            ("storage.s3", *s3 == storage.s3),
            ("storage.rewrite", *rewrite == storage.rewrite),
            ("storage.hedge", *hedge == storage.hedge),
            ("storage.local", *local == storage.local),
            ("storage.max_retries", *max_retries == storage.max_retries),
            (
                "storage.connect_timeout",
                *connect_timeout == storage.connect_timeout,
            ),
            (
                "storage.catalog_timeout",
                *catalog_timeout == storage.catalog_timeout,
            ),
        ];
        compared
            .into_iter()
            .filter(|(_, same)| !same)
            .map(|(name, _)| name)
            .collect()
    }
}

/// The first rule the configuration breaks, or `Ok` when it breaks none.
fn validate(config: &Config) -> std::result::Result<(), ConfigViolation> {
    let removed = [
        (
            "page_cache_bytes",
            config.server.removed_page_cache_bytes.is_some(),
        ),
        (
            "scan_page_cache_bytes",
            config.server.removed_scan_page_cache_bytes.is_some(),
        ),
    ];
    if let Some((setting, _)) = removed.into_iter().find(|&(_, set)| set) {
        return Err(ConfigViolation::RemovedSetting {
            setting,
            replacement: "cache_bytes",
        });
    }
    validate_bounds(&config.server, &config.storage)?;
    validate_hedge(&config.storage.hedge)?;
    validate_log(&config.server.log)?;
    if let Some((index, fault)) = config
        .storage
        .rewrite
        .iter()
        .enumerate()
        .find_map(|(index, rule)| rule.fault().map(|fault| (index, fault)))
    {
        return Err(ConfigViolation::Rewrite { index, fault });
    }
    if let Some(s3) = &config.storage.s3
        && s3.access_key_id.is_some() != s3.secret_access_key.is_some()
    {
        return Err(ConfigViolation::IncompleteStaticKeys);
    }
    if let Some(share) = config.server.row_cache_share
        && !(0.0..=1.0).contains(&share)
    {
        return Err(ConfigViolation::RowCacheShareOutOfRange { share });
    }
    if config.server.cache_bytes.is_some() {
        budget::resolve(
            &config.server,
            None,
            config.reads_rows(),
            PARSE_CACHE_SHARDS,
        )?;
    }
    if let Some(local) = &config.storage.local
        && local.page_cache_disk_bytes > 0
        && local.page_cache_disk_bytes < PAGE_CACHE_DISK_BLOCK_BYTES
    {
        return Err(ConfigViolation::PageCacheDiskTooSmall {
            bytes: local.page_cache_disk_bytes,
            minimum: PAGE_CACHE_DISK_BLOCK_BYTES,
        });
    }
    if config.tables.is_empty() {
        return Err(ConfigViolation::NoTables);
    }
    validate_tables(config)?;
    validate_split(config)
}

/// Whether any of `tables` reads through the row cache
/// (`TableConfig::row_cache`). The row cache's part of the budget is zero
/// when none does (`budget::resolve`).
pub(crate) fn reads_rows<'a>(tables: impl IntoIterator<Item = &'a TableConfig>) -> bool {
    tables.into_iter().any(TableConfig::row_cache)
}

/// A server with no page-cache disk level that serves a table reading
/// through the row cache beside tables reading pages alone sets
/// `row_cache_share` (RFD 028).
fn validate_split(config: &Config) -> std::result::Result<(), ConfigViolation> {
    let disk_level = config
        .storage
        .local
        .as_ref()
        .is_some_and(|local| local.page_cache_disk_bytes > 0);
    if disk_level || config.server.row_cache_share.is_some() || !config.reads_rows() {
        return Ok(());
    }
    let pages_alone: Vec<String> = config
        .tables
        .iter()
        .filter(|table| !table.row_cache())
        .map(|table| table.name.clone())
        .collect();
    if pages_alone.is_empty() {
        return Ok(());
    }
    Err(ConfigViolation::MixedServerNeedsShare { pages_alone })
}

/// Each table has a name of its own, a residency its settings fit, a key
/// column, value columns, and an order column of names Iceberg's writers
/// keep, value columns listed once each and apart from the key, an order
/// column apart from both, a `max_scan_rows` only when it has an order
/// column, above zero and within half of `max_multi_scan_values`, a
/// `row_cache` only when it has none, a `lookups` level its residency
/// reaches, and a source whose settings `validate_source` accepts.
fn validate_tables(config: &Config) -> std::result::Result<(), ConfigViolation> {
    let mut names = HashSet::new();
    for (index, table) in config.tables.iter().enumerate() {
        if table.name.is_empty() {
            return Err(ConfigViolation::EmptyTableName { index });
        }
        if !names.insert(table.name.as_str()) {
            return Err(ConfigViolation::DuplicateTableName {
                index,
                name: table.name.clone(),
            });
        }
        match table.residency {
            Residency::Full => {}
            Residency::Disk | Residency::Bloom => {
                if let Some(bytes) = table.resident_budget_bytes {
                    return Err(ConfigViolation::BudgetWithoutResidency {
                        index,
                        bytes,
                        residency: table.residency,
                    });
                }
            }
        }
        if table.residency == Residency::Disk && config.storage.local.is_none() {
            return Err(ConfigViolation::DiskResidencyWithoutLocalDir {
                table: table.name.clone(),
                index: Some(index),
            });
        }
        table.check_columns(index)?;
        table.check_scan_rows(index, config.server.max_multi_scan_values)?;
        if table.order_column.is_some() && table.row_cache.is_some() {
            return Err(ConfigViolation::RowCacheOnOrderedTable { index });
        }
        table.check_lookups(Some(index))?;
        validate_source(index, &table.source, &config.storage)?;
    }
    Ok(())
}

/// A static source's location is not empty and some store this process
/// builds serves it once the file's rewrite rules apply
/// (`StorageConfig::location_error`); locations a command line's rules
/// would rewrite are not taken into account, since a file means the same
/// whatever command reads it. An AWS source's required settings are not
/// empty, its optional ones are not empty when present, its endpoint is an
/// HTTP URL, and an `s3tables` source's ARN is a table bucket's
/// (`aws_catalog::table_bucket_region`). A REST source is checked when its
/// catalog first answers.
fn validate_source(
    index: usize,
    source: &SourceConfig,
    storage: &StorageConfig,
) -> std::result::Result<(), ConfigViolation> {
    let (required, optional, table_bucket_arn, endpoint) = match source {
        SourceConfig::Static { location } => {
            not_empty(index, &[(SourceSetting::Location, location)], &[])?;
            return match storage.location_error(location) {
                Some(source) => Err(ConfigViolation::TableLocation { index, source }),
                None => Ok(()),
            };
        }
        SourceConfig::Rest { .. } => return Ok(()),
        SourceConfig::Glue {
            database,
            table,
            catalog_id,
            region,
            endpoint,
        } => (
            vec![
                (SourceSetting::Database, database),
                (SourceSetting::Table, table),
            ],
            vec![
                (SourceSetting::CatalogId, catalog_id),
                (SourceSetting::Region, region),
                (SourceSetting::Endpoint, endpoint),
            ],
            None,
            endpoint,
        ),
        SourceConfig::S3Tables {
            table_bucket_arn,
            namespace,
            table,
            region,
            endpoint,
        } => (
            vec![
                (SourceSetting::TableBucketArn, table_bucket_arn),
                (SourceSetting::Namespace, namespace),
                (SourceSetting::Table, table),
            ],
            vec![
                (SourceSetting::Region, region),
                (SourceSetting::Endpoint, endpoint),
            ],
            Some(table_bucket_arn),
            endpoint,
        ),
    };
    not_empty(index, &required, &optional)?;
    if let Some(arn) = table_bucket_arn
        && aws_catalog::table_bucket_region(arn).is_none()
    {
        return Err(ConfigViolation::TableBucketArn {
            index,
            arn: arn.clone(),
        });
    }
    match endpoint {
        Some(endpoint)
            if !HTTP_SCHEMES
                .iter()
                .any(|scheme| endpoint.starts_with(scheme)) =>
        {
            Err(ConfigViolation::SourceEndpoint {
                index,
                endpoint: endpoint.clone(),
            })
        }
        Some(_) | None => Ok(()),
    }
}

/// The first of `required`, or of `optional` where present, that is empty,
/// as `ConfigViolation::EmptySourceSetting`.
fn not_empty(
    index: usize,
    required: &[(SourceSetting, &String)],
    optional: &[(SourceSetting, &Option<String>)],
) -> std::result::Result<(), ConfigViolation> {
    let present = optional
        .iter()
        .filter_map(|(setting, value)| value.as_ref().map(|value| (*setting, value)));
    match required
        .iter()
        .copied()
        .chain(present)
        .find(|(_, value)| value.is_empty())
    {
        Some((setting, _)) => Err(ConfigViolation::EmptySourceSetting { index, setting }),
        None => Ok(()),
    }
}

/// The poll interval is within `MIN_POLL_INTERVAL..=MAX_POLL_INTERVAL`,
/// and every other deadline, timeout, and limit that a zero would turn into
/// "refuse everything" is above zero. `max_retries` is not among them: zero
/// retries is a request tried once.
fn validate_bounds(
    server: &ServerConfig,
    storage: &StorageConfig,
) -> std::result::Result<(), ConfigViolation> {
    let interval = server.poll_interval;
    if interval < MIN_POLL_INTERVAL {
        return Err(ConfigViolation::PollIntervalTooShort {
            interval,
            minimum: MIN_POLL_INTERVAL,
        });
    }
    if interval > MAX_POLL_INTERVAL {
        return Err(ConfigViolation::PollIntervalTooLong {
            interval,
            maximum: MAX_POLL_INTERVAL,
        });
    }
    let durations = [
        (server.get_deadline, ConfigViolation::GetDeadlineZero),
        (
            server.multi_get_deadline,
            ConfigViolation::MultiGetDeadlineZero,
        ),
        (server.scan_deadline, ConfigViolation::ScanDeadlineZero),
        (
            server.multi_scan_deadline,
            ConfigViolation::MultiScanDeadlineZero,
        ),
        (server.load_deadline, ConfigViolation::LoadDeadlineZero),
        (
            server.refresh_deadline,
            ConfigViolation::RefreshDeadlineZero,
        ),
        (server.drain_deadline, ConfigViolation::DrainDeadlineZero),
        (storage.connect_timeout, ConfigViolation::ConnectTimeoutZero),
        (storage.catalog_timeout, ConfigViolation::CatalogTimeoutZero),
    ];
    let limits = [
        (server.max_in_flight, ConfigViolation::MaxInFlightZero),
        (
            server.max_multi_get_values,
            ConfigViolation::MaxMultiGetValuesZero,
        ),
        (server.batch_fan_out, ConfigViolation::BatchFanOutZero),
        (
            server.max_multi_scan_values,
            ConfigViolation::MaxMultiScanValuesZero,
        ),
        (server.max_scan_bytes, ConfigViolation::MaxScanBytesZero),
    ];
    if let Some((_, violation)) = durations
        .into_iter()
        .find(|(duration, _)| duration.is_zero())
    {
        return Err(violation);
    }
    match limits.into_iter().find(|(limit, _)| *limit == 0) {
        Some((_, violation)) => Err(violation),
        None => Ok(()),
    }
}

/// Each standard field is emitted under a name of its own; the first pair
/// in `StandardField::ALL` order that shares one is the violation.
fn validate_log(log: &LogConfig) -> std::result::Result<(), ConfigViolation> {
    for (at, &second) in StandardField::ALL.iter().enumerate() {
        let name = log.emitted_name(second);
        if let Some(&first) = StandardField::ALL[..at]
            .iter()
            .find(|&&earlier| log.emitted_name(earlier) == name)
        {
            return Err(ConfigViolation::LogFieldCollision {
                name: name.to_string(),
                first,
                second,
            });
        }
    }
    Ok(())
}

/// The hedge settings are checked whether or not hedging is enabled, so a
/// mistyped value is caught before the day it is switched on.
fn validate_hedge(hedge: &HedgeConfig) -> std::result::Result<(), ConfigViolation> {
    let percentile_in_range = hedge.percentile > 0.0 && hedge.percentile <= 1.0;
    if !percentile_in_range {
        return Err(ConfigViolation::HedgePercentileOutOfRange {
            percentile: hedge.percentile,
        });
    }
    if !(0.0..=1.0).contains(&hedge.max_rate) {
        return Err(ConfigViolation::HedgeMaxRateOutOfRange {
            max_rate: hedge.max_rate,
        });
    }
    if hedge.window == 0 {
        return Err(ConfigViolation::HedgeWindowEmpty);
    }
    Ok(())
}

/// Listeners, polling, the page cache, logging, and the bounds of the live
/// and background paths (RFD 013). `metrics_listen` is always bound: it
/// serves `/metrics` and `/ready`, and RFD 012 makes both part of the
/// surface. Every duration is written as a friendly duration such as
/// `"5s"`. `Config::validated` holds `poll_interval` between one second
/// and one day, and every other duration, `max_in_flight`,
/// `max_multi_get_values`, `batch_fan_out`, `max_multi_scan_values`, and
/// `max_scan_bytes` above zero.
#[derive(Clone, Debug, PartialEq, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct ServerConfig {
    /// The gRPC listener.
    pub listen: SocketAddr,
    /// The `/metrics` and `/ready` listener.
    pub metrics_listen: SocketAddr,
    /// How often each table's ref is re-resolved (RFD 005); written as a
    /// friendly duration such as `"10s"`, from `MIN_POLL_INTERVAL` (one
    /// second) to `MAX_POLL_INTERVAL` (one day).
    #[serde(with = "time::friendly_duration")]
    pub poll_interval: Duration,
    /// The bytes the row cache and the page cache's memory level hold
    /// together (RFD 028). `None` derives it from the process's memory
    /// limit when the caches open: `DEFAULT_CACHE_SHARE_OF_LIMIT` of it, or
    /// `FALLBACK_CACHE_BYTES` when no limit can be read
    /// (`budget::resolve`). The page cache's part must hold one contract
    /// value page with its entry overhead per shard of its memory level
    /// (`cache::min_memory_bytes`), or the budget would bound nothing; the
    /// shards are one per worker thread rounded up to a power of two, so
    /// parsing checks one shard's minimum and opening the caches checks the
    /// rest.
    pub cache_bytes: Option<u64>,
    /// The share of `cache_bytes` the row cache holds when a table reads
    /// through it, in `[0, 1]`, the rest being the page cache's memory
    /// level; `None` is `DEFAULT_ROW_CACHE_SHARE`. When no table reads
    /// through the row cache, the page cache holds the whole budget.
    pub row_cache_share: Option<f64>,
    /// The removed `page_cache_bytes`, read under its name so that
    /// validation refuses a file that sets it, naming `cache_bytes`
    /// (`ConfigViolation::RemovedSetting`), where an unknown key would
    /// fail the parse without the replacement.
    #[serde(rename = "page_cache_bytes")]
    pub(crate) removed_page_cache_bytes: Option<IgnoredAny>,
    /// The removed `scan_page_cache_bytes`, read and refused as
    /// `removed_page_cache_bytes` is: ordered tables' pages are cached in
    /// the one page cache `cache_bytes` budgets (RFD 027).
    #[serde(rename = "scan_page_cache_bytes")]
    pub(crate) removed_scan_page_cache_bytes: Option<IgnoredAny>,
    /// Log field renames and additions.
    pub log: LogConfig,
    /// How long a `Get` may run before it is `DEADLINE_EXCEEDED`, and how
    /// long each page fetch of a lookup may run. A client's own deadline is
    /// enforced by the gRPC layer and reported as the client's.
    #[serde(with = "time::friendly_duration")]
    pub get_deadline: Duration,
    /// How long a `MultiGet` may run before it is `DEADLINE_EXCEEDED`,
    /// whatever its size. A client's own deadline is enforced by the gRPC
    /// layer and reported as the client's.
    #[serde(with = "time::friendly_duration")]
    pub multi_get_deadline: Duration,
    /// Requests, `Get`, `MultiGet`, `Scan`, and `MultiScan` together, in
    /// flight at once; one more is refused at once as `RESOURCE_EXHAUSTED`,
    /// never queued.
    pub max_in_flight: usize,
    /// Values one `MultiGet` may ask for: its keys times the columns it
    /// names, a key of a call that names none counting one (RFD 022); more
    /// is `INVALID_ARGUMENT` before any lookup.
    pub max_multi_get_values: usize,
    /// GETs one `MultiGet`, `Scan`, or `MultiScan` holds in flight at once,
    /// across every task the batch runs on: each GET a lookup issues, a key
    /// page under `bloom`, a value page, or a dictionary page, and each span
    /// GET of a Scan, holds one until it ends (RFD 013, RFD 022, RFD 025).
    /// Like `max_in_flight`, it is set for the process: every table is built
    /// under it at start, so a reload that changes it is refused and a
    /// restart applies it (RFD 018).
    pub batch_fan_out: usize,
    /// Values one `MultiScan` may ask for: its keys times its limit times
    /// one more than its requested columns; more is `INVALID_ARGUMENT`
    /// before any key is read (RFD 025).
    pub max_multi_scan_values: usize,
    /// How long a `Scan` may run before it is `DEADLINE_EXCEEDED`, and how
    /// long each GET of any Scan, span or key page, may run, in a
    /// `MultiScan` too (RFD 025).
    #[serde(with = "time::friendly_duration")]
    pub scan_deadline: Duration,
    /// How long a `MultiScan` may run before it is `DEADLINE_EXCEEDED`,
    /// whatever its size (RFD 025).
    #[serde(with = "time::friendly_duration")]
    pub multi_scan_deadline: Duration,
    /// The encoded size in bytes above which a `Scan` or `MultiScan`
    /// response is refused as `FAILED_PRECONDITION`, returning no rows
    /// (RFD 025). The default is a gRPC client's default message limit; an
    /// owner whose clients accept larger messages raises it. Like
    /// `max_in_flight`, it is set for the process, so a reload that changes
    /// it is refused and a restart applies it (RFD 018).
    pub max_scan_bytes: usize,
    /// How long a build from no serving generation, a table's first load
    /// or a retry after one failed, may run before it fails and the table
    /// is `Degraded`; the next poll tries again.
    #[serde(with = "time::friendly_duration")]
    pub load_deadline: Duration,
    /// How long one refresh over a serving generation may run before it
    /// fails and the table is `Degraded`, the generation still serving; the
    /// next poll tries again.
    #[serde(with = "time::friendly_duration")]
    pub refresh_deadline: Duration,
    /// How long a shutdown waits for open requests before it stops waiting,
    /// closes the page cache, and ends the run.
    #[serde(with = "time::friendly_duration")]
    pub drain_deadline: Duration,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            listen: DEFAULT_LISTEN,
            metrics_listen: DEFAULT_METRICS_LISTEN,
            poll_interval: DEFAULT_POLL_INTERVAL,
            cache_bytes: None,
            row_cache_share: None,
            removed_page_cache_bytes: None,
            removed_scan_page_cache_bytes: None,
            log: LogConfig::default(),
            get_deadline: DEFAULT_GET_DEADLINE,
            multi_get_deadline: DEFAULT_MULTI_GET_DEADLINE,
            max_in_flight: DEFAULT_MAX_IN_FLIGHT,
            max_multi_get_values: DEFAULT_MAX_MULTI_GET_VALUES,
            batch_fan_out: DEFAULT_BATCH_FAN_OUT,
            max_multi_scan_values: DEFAULT_MAX_MULTI_SCAN_VALUES,
            scan_deadline: DEFAULT_SCAN_DEADLINE,
            multi_scan_deadline: DEFAULT_MULTI_SCAN_DEADLINE,
            max_scan_bytes: DEFAULT_MAX_SCAN_BYTES,
            load_deadline: DEFAULT_LOAD_DEADLINE,
            refresh_deadline: DEFAULT_REFRESH_DEADLINE,
            drain_deadline: DEFAULT_DRAIN_DEADLINE,
        }
    }
}

/// Logs are JSON only (RFD 012). `fields` renames the standard fields and
/// `static_fields` are added to every event; both are empty by default. A
/// key of `fields` that names no standard field fails the parse, and
/// `Config::validated` refuses two standard fields emitted under one name
/// (`ConfigViolation::LogFieldCollision`).
#[derive(Clone, Debug, Default, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct LogConfig {
    /// Standard field to the name it is emitted under.
    pub fields: BTreeMap<StandardField, String>,
    /// Field name to a constant value added to every event.
    pub static_fields: BTreeMap<String, String>,
}

impl LogConfig {
    /// The name `field` is emitted under: the name `fields` maps it to, or
    /// its own.
    pub fn emitted_name(&self, field: StandardField) -> &str {
        self.fields
            .get(&field)
            .map_or_else(|| field.name(), String::as_str)
    }
}

/// The standard fields of every log event (RFD 012), each renamable through
/// `LogConfig.fields`. Display and serde give the lowercase name the file
/// uses, which is also the name a field is emitted under by default.
// The serde names and Display are the same strings;
// `test_serialize_standard_field_matches_display` holds them together.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum StandardField {
    /// When the event happened, RFC 3339 in UTC.
    Time,
    /// The event's level, lowercase.
    Level,
    /// The event's message.
    Message,
    /// The module the event came from.
    Target,
}

impl StandardField {
    /// Every standard field, for the collision check and the tests that run
    /// a case per field.
    pub(crate) const ALL: [Self; 4] = [Self::Time, Self::Level, Self::Message, Self::Target];

    /// The field's lowercase name.
    pub(crate) const fn name(self) -> &'static str {
        match self {
            Self::Time => "time",
            Self::Level => "level",
            Self::Message => "message",
            Self::Target => "target",
        }
    }
}

impl fmt::Display for StandardField {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.name())
    }
}

/// Hedging of lookup GETs (RFD 013). The delay is `percentile` of the last
/// `window` lookup GETs of the table, and no hedge is issued while hedges
/// would exceed `max_rate` of requests. Load reads are never hedged.
#[derive(Clone, Debug, PartialEq, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct HedgeConfig {
    /// Whether lookup GETs are hedged at all.
    pub enabled: bool,
    /// The latency percentile the delay is taken at, in `(0, 1]`.
    pub percentile: f64,
    /// Lookup GETs in the per-table latency window; at least one.
    pub window: usize,
    /// The largest share of the lookup GETs in the window that may be
    /// hedged, in `[0, 1]`.
    pub max_rate: f64,
}

impl Default for HedgeConfig {
    fn default() -> Self {
        Self {
            enabled: DEFAULT_HEDGE_ENABLED,
            percentile: DEFAULT_HEDGE_PERCENTILE,
            window: DEFAULT_HEDGE_WINDOW,
            max_rate: DEFAULT_HEDGE_MAX_RATE,
        }
    }
}

/// Explicit S3 client settings, for S3-compatible stores and tests. Absent,
/// the storage layer uses the SDK's default chain (RFD 003). Static keys
/// come as a pair or not at all. Never serializable: a rendering of this
/// struct other than `Debug` would carry the secret in plain text.
#[derive(Clone, Default, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct S3Config {
    /// Endpoint URL, for stores other than AWS.
    pub endpoint: Option<String>,
    /// Region name.
    pub region: Option<String>,
    /// Static access key id; set together with `secret_access_key`.
    pub access_key_id: Option<String>,
    /// Static secret key; never printed.
    pub secret_access_key: Option<String>,
    /// Permit plain HTTP endpoints.
    pub allow_http: bool,
    /// Use path-style addressing, `host/bucket/key`.
    pub path_style: bool,
}

impl fmt::Debug for S3Config {
    /// The secret is rendered as a fixed placeholder so no log or panic
    /// message carries it.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("S3Config")
            .field("endpoint", &self.endpoint)
            .field("region", &self.region)
            .field("access_key_id", &self.access_key_id)
            .field(
                "secret_access_key",
                &self.secret_access_key.as_ref().map(|_| SECRET_PLACEHOLDER),
            )
            .field("allow_http", &self.allow_http)
            .field("path_style", &self.path_style)
            .finish()
    }
}

/// A location prefix replaced before any read (RFD 003), for tables whose
/// metadata records paths from another host or bucket.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RewriteRule {
    /// The prefix as it appears in the Iceberg metadata.
    pub from: String,
    /// The prefix to read from instead.
    pub to: String,
}

impl RewriteRule {
    /// What makes the rule unusable, or `None`: the one check of a rule,
    /// which a file's rules and the command line's both go through. An
    /// empty `from` is a prefix of every location, so the rule would
    /// rewrite all of them; an empty `to` turns every location it matches
    /// into a relative path, which names no store.
    pub fn fault(&self) -> Option<RewriteFault> {
        if self.from.is_empty() {
            Some(RewriteFault::EmptyFrom)
        } else if self.to.is_empty() {
            Some(RewriteFault::EmptyTo)
        } else {
            None
        }
    }
}

/// What separates the two halves of a rule's `FROM=TO` text; the first
/// occurrence, since a location may itself carry one.
pub const REWRITE_SEPARATOR: char = '=';

/// A rule from `FROM=TO`, split at the first `REWRITE_SEPARATOR` and checked
/// by `RewriteRule::fault`, the check a file's rules go through, so text
/// the file would refuse is refused here: the one parser of a rule given
/// as text, on the command line or in an environment variable.
impl std::str::FromStr for RewriteRule {
    type Err = RewriteTextFault;

    fn from_str(text: &str) -> std::result::Result<Self, RewriteTextFault> {
        let (from, to) = text
            .split_once(REWRITE_SEPARATOR)
            .ok_or(RewriteTextFault::NoSeparator)?;
        let rule = Self {
            from: from.to_string(),
            to: to.to_string(),
        };
        match rule.fault() {
            None => Ok(rule),
            Some(fault) => Err(fault.into()),
        }
    }
}

/// The rule as `FROM=TO`, the text `FromStr` reads back.
impl fmt::Display for RewriteRule {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}{REWRITE_SEPARATOR}{}", self.from, self.to)
    }
}

/// Why `FROM=TO` text is not a rewrite rule (`RewriteRule::from_str`): no
/// separator, or a fault `RewriteRule::fault` finds in a file's rule.
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
pub enum RewriteTextFault {
    /// No `=` separates `FROM` from `TO`.
    #[error("no = separates FROM from TO")]
    NoSeparator,
    /// The halves make a rule the file would refuse, named as the text
    /// names them: `FROM` or `TO`.
    #[error("{} is empty", .0.half().to_ascii_uppercase())]
    Rule(RewriteFault),
}

/// By hand, not `#[from]`: thiserror makes a `#[from]` field the error's
/// `source()`, and a reporter that walks the chain would print the fault a
/// second time under the variant's own message.
impl From<RewriteFault> for RewriteTextFault {
    fn from(fault: RewriteFault) -> Self {
        Self::Rule(fault)
    }
}

/// Why a rewrite rule is unusable (`RewriteRule::fault`).
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
pub enum RewriteFault {
    /// The `from` prefix is empty.
    #[error("from is empty")]
    EmptyFrom,
    /// The `to` prefix is empty.
    #[error("to is empty")]
    EmptyTo,
}

impl RewriteFault {
    /// The field of the rule that is empty, as a file names it.
    fn half(self) -> &'static str {
        match self {
            Self::EmptyFrom => "from",
            Self::EmptyTo => "to",
        }
    }
}

/// The optional local directory (RFD 017, RFD 012): chunk files for disk
/// residency and the page cache's disk level. Everything in it is rebuilt
/// from object storage.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LocalConfig {
    /// The directory.
    pub dir: PathBuf,
    /// The bytes chunk files may occupy across every table; unbounded when absent.
    pub index_budget_bytes: Option<u64>,
    /// The size bound of the page cache's disk level: zero means no disk
    /// level, anything else at least `contract::PAGE_CACHE_DISK_BLOCK_BYTES`,
    /// the level's one block.
    pub page_cache_disk_bytes: u64,
}

/// Object storage, hedging, the local directory, and the bounds on every
/// request to the object store and the catalog (RFD 013). The two
/// durations are written as friendly durations such as `"1s"` and are
/// above zero; `max_retries` may be zero.
#[derive(Clone, Debug, PartialEq, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct StorageConfig {
    /// Explicit S3 settings; absent means the SDK's default chain.
    pub s3: Option<S3Config>,
    /// Location prefix rewrites, applied in order.
    pub rewrite: Vec<RewriteRule>,
    /// Hedging of lookup GETs.
    pub hedge: HedgeConfig,
    /// The local directory; required by disk residency.
    pub local: Option<LocalConfig>,
    /// Retries of one failed S3 request, on the live and background paths
    /// alike; zero tries each request once.
    pub max_retries: usize,
    /// How long an S3 connection may take to open.
    #[serde(with = "time::friendly_duration")]
    pub connect_timeout: Duration,
    /// How long one catalog call may run before the poll or load that made
    /// it fails.
    #[serde(with = "time::friendly_duration")]
    pub catalog_timeout: Duration,
}

impl StorageConfig {
    /// `location` with the first rule of `rewrite` whose `from` it starts
    /// with applied, or `location` itself. A rewritten location is not
    /// rewritten again.
    pub fn rewritten<'a>(&self, location: &'a str) -> Cow<'a, str> {
        self.rewrite
            .iter()
            .find_map(|rule| {
                location
                    .strip_prefix(rule.from.as_str())
                    .map(|rest| Cow::Owned(format!("{}{rest}", rule.to)))
            })
            .unwrap_or(Cow::Borrowed(location))
    }

    /// Why no store this process builds serves `location` once rewritten,
    /// with the rewritten location; `None` when one does. What a static
    /// table's location is checked against before anything is read.
    pub fn location_error(&self, location: &str) -> Option<LocationError> {
        let rewritten = self.rewritten(location);
        storage::unbuildable(&rewritten).map(|fault| LocationError {
            location: rewritten.into_owned(),
            fault,
        })
    }
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            s3: None,
            rewrite: Vec::new(),
            hedge: HedgeConfig::default(),
            local: None,
            max_retries: DEFAULT_MAX_RETRIES,
            connect_timeout: DEFAULT_CONNECT_TIMEOUT,
            catalog_timeout: DEFAULT_CATALOG_TIMEOUT,
        }
    }
}

/// One served table. Two entries are equal when every setting is, `lookups`
/// compared as `TableConfig::lookups` resolves it, `max_scan_rows` as
/// `TableConfig::scan_rows` does, and `row_cache` as
/// `TableConfig::row_cache` does, so an entry that spells out a default
/// equals one that leaves it out, and `value_columns`
/// compared as the set of columns it lists, each a name and a format, so a
/// reordered list is no change.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TableConfig {
    /// The name clients use in requests; unique and non-empty.
    pub name: String,
    /// Where the table's current metadata comes from.
    pub source: SourceConfig,
    /// The Iceberg ref (branch or tag) that is served; `ref` in the file.
    pub r#ref: String,
    /// The key column; its Iceberg type is one of `KeyType`.
    pub key_column: String,
    /// The columns the table serves, each Iceberg binary, fixed, or string
    /// and read only when a lookup names it (RFD 022), with the format its
    /// values are declared in (RFD 024); empty for a table that answers
    /// only whether a key is present. A name listed twice, or the key column
    /// listed, is a violation. Order carries no meaning: two entries that
    /// list the same columns in another order are equal.
    pub value_columns: Vec<ValueColumnConfig>,
    /// The order column of an ordered table (RFD 025): a top-level column
    /// of an order type (`OrderType`), neither the key column nor a value
    /// column, by which each key's rows are sorted after the key. `None`
    /// for a key-value table. Setting, changing, or removing it reloads the
    /// whole table.
    pub order_column: Option<String>,
    /// The most rows one Scan of an ordered table, or one key of a
    /// `MultiScan`, returns, the largest limit either takes (RFD 025);
    /// above zero, and set only on an ordered table. `None` there takes
    /// `DEFAULT_MAX_SCAN_ROWS` (`TableConfig::scan_rows`). A change applies
    /// in place.
    pub max_scan_rows: Option<u32>,
    /// Whether falling below `lookups` for a reason in the snapshot's
    /// layout is an error rather than a warning (RFD 020): a strict table
    /// refuses such a snapshot, failing the start when it is the first and
    /// otherwise holding the last snapshot that met the level. Off by
    /// default, when the table serves the snapshot and is flagged below its
    /// level.
    #[serde(default = "default_strict")]
    pub strict: bool,
    /// The lookup level the table declares it accepts (RFD 020); `None`
    /// takes the best its residency reaches (`TableConfig::lookups`).
    pub lookups: Option<Level>,
    /// Where the key chunks live (RFD 017).
    #[serde(default = "default_residency")]
    pub residency: Residency,
    /// The resident-bytes budget a `full` load is held to (RFD 017); only
    /// that tier takes one.
    pub resident_budget_bytes: Option<u64>,
    /// Whether a key-value table's lookups read each value through the row
    /// cache (RFD 026); set only on a key-value table. `None` there takes
    /// `DEFAULT_ROW_CACHE` (`TableConfig::row_cache`). A change reloads the
    /// table.
    pub row_cache: Option<bool>,
}

/// One column a table serves (RFD 022), written in the file as its name
/// alone, which declares it opaque, or as a table of `name` and `format`
/// (RFD 024): `{ name = "detail", format = "protobuf" }`. A key the table
/// form does not know is a schema fault. The derived order is by name, then
/// format.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ValueColumnConfig {
    /// The column's top-level name in the table's schema.
    pub name: String,
    /// What the column's values are, which says whether a lookup may trim
    /// them.
    pub format: ValueFormat,
}

impl ValueColumnConfig {
    /// The opaque column named `name`.
    pub fn named(name: &str) -> Self {
        Self::of(name, ValueFormat::Opaque)
    }

    /// The column named `name`, declared `format`.
    pub fn of(name: &str, format: ValueFormat) -> Self {
        Self {
            name: name.to_string(),
            format,
        }
    }
}

/// A value column's table form in the file, `name` and an optional
/// `format`, read by the derived deserializer so a key it does not know is
/// refused where serde finds it.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ValueColumnTable {
    name: String,
    #[serde(default)]
    format: ValueFormat,
}

impl<'de> Deserialize<'de> for ValueColumnConfig {
    /// A string is the column's name, opaque; a map is its table form
    /// (`ValueColumnTable`), read from the deserializer in place, so a
    /// fault inside it is reported at the key at fault.
    fn deserialize<D: serde::Deserializer<'de>>(
        deserializer: D,
    ) -> std::result::Result<Self, D::Error> {
        deserializer.deserialize_any(ValueColumnVisitor)
    }
}

/// `ValueColumnConfig`'s visitor: a string, or a map.
struct ValueColumnVisitor;

impl<'de> serde::de::Visitor<'de> for ValueColumnVisitor {
    type Value = ValueColumnConfig;

    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("a column name, or a table with name and format")
    }

    fn visit_str<E: serde::de::Error>(self, name: &str) -> std::result::Result<Self::Value, E> {
        Ok(ValueColumnConfig::named(name))
    }

    fn visit_map<A: serde::de::MapAccess<'de>>(
        self,
        map: A,
    ) -> std::result::Result<Self::Value, A::Error> {
        let table =
            ValueColumnTable::deserialize(serde::de::value::MapAccessDeserializer::new(map))?;
        Ok(ValueColumnConfig {
            name: table.name,
            format: table.format,
        })
    }
}

/// What a value column's values are (RFD 024). Display and serde give the
/// lowercase name the file spells, which the doctor's output and `TableInfo`
/// carry.
// The serde names and Display are the same strings;
// `test_serialize_value_format_matches_display` holds them together.
#[derive(
    Clone, Copy, Debug, Default, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize,
)]
#[serde(rename_all = "lowercase")]
pub enum ValueFormat {
    /// Bytes icetray never looks into, served whole.
    #[default]
    Opaque,
    /// A serialized protobuf message, which a lookup may trim to the
    /// top-level fields it names; only a `binary` column may be declared
    /// so.
    Protobuf,
}

impl ValueFormat {
    /// Every format, for the parser and its refusal message.
    pub const ALL: [Self; 2] = [Self::Opaque, Self::Protobuf];

    /// `Opaque` whatever column `_column` names: the formats of a table
    /// that declares no column protobuf, as `diagnose` and `examine` take
    /// them.
    pub fn opaque(_column: &str) -> Self {
        Self::Opaque
    }

    /// Whether a column of `value_type` may be declared in this format: an
    /// opaque column takes every type a layout admits, and a protobuf
    /// column must be `binary`, since a trimmed value has no fixed width
    /// and a protobuf is not text (RFD 024).
    pub fn admits(self, value_type: ValueType) -> bool {
        match (self, value_type) {
            (Self::Opaque, _) | (Self::Protobuf, ValueType::Binary) => true,
            (Self::Protobuf, ValueType::Fixed { .. } | ValueType::String) => false,
        }
    }

    /// The format's lowercase name, as the file spells it.
    pub const fn name(self) -> &'static str {
        match self {
            Self::Opaque => "opaque",
            Self::Protobuf => "protobuf",
        }
    }
}

impl fmt::Display for ValueFormat {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.name())
    }
}

impl std::str::FromStr for ValueFormat {
    type Err = ConfigViolation;

    /// Exactly the lowercase names `Display` prints; the one boundary parser
    /// for a format given on the command line, where the file's are read by
    /// serde under the same names.
    fn from_str(text: &str) -> std::result::Result<Self, Self::Err> {
        Self::ALL
            .into_iter()
            .find(|format| format.name() == text)
            .ok_or_else(|| ConfigViolation::UnknownValueFormat {
                text: text.to_string(),
            })
    }
}

impl TableConfig {
    /// The value columns' names, ascending: the order a generation's layout
    /// holds them in and a lookup names them by position, so that the order
    /// the file lists them in, which carries no meaning, never reaches a
    /// lookup.
    pub fn value_column_names(&self) -> Vec<&str> {
        let mut names: Vec<&str> = self
            .value_columns
            .iter()
            .map(|column| column.name.as_str())
            .collect();
        names.sort_unstable();
        names
    }

    /// The value columns, ascending by name, each with its format: what
    /// two entries compare (`PartialEq`).
    fn sorted_value_columns(&self) -> Vec<&ValueColumnConfig> {
        let mut columns: Vec<&ValueColumnConfig> = self.value_columns.iter().collect();
        columns.sort_unstable();
        columns
    }

    /// The format the column named `name` is declared in; `None` for a
    /// name the table does not serve.
    pub fn format_of(&self, name: &str) -> Option<ValueFormat> {
        self.value_columns
            .iter()
            .find(|column| column.name == name)
            .map(|column| column.format)
    }

    /// The declared format of each column by name, opaque for a name the
    /// table does not serve: what the doctor and the wire read a column's
    /// format through.
    pub fn formats(&self) -> impl Fn(&str) -> ValueFormat + '_ {
        |name| self.format_of(name).unwrap_or_default()
    }

    /// Each value column's format, in `value_column_names`' order, which is
    /// a generation's layout's: what the doctor reports beside each column.
    pub fn value_formats(&self) -> Vec<ValueFormat> {
        self.sorted_value_columns()
            .into_iter()
            .map(|column| column.format)
            .collect()
    }

    /// Checks the column reads `reads` names for a lookup of this table,
    /// first on their own (`manifest::check_column_reads`) and then against
    /// the configured value columns and their formats, as a lookup checks
    /// them against the serving generation's (`Error::ColumnRequest`), so a
    /// caller can refuse a request before a load.
    pub fn check_column_request(&self, reads: &[ColumnRead<'_>]) -> Result<()> {
        let reads = manifest::check_column_reads(&self.name, reads)?;
        let served = self.value_column_names();
        manifest::select_columns(&self.name, reads, |name| {
            served
                .iter()
                .position(|&column| column == name)
                .zip(self.format_of(name))
        })
        .map(drop)
    }

    /// `ConfigViolation::ColumnNameRewritten` for a key, value, or order
    /// column whose name is not a valid Avro name,
    /// `ConfigViolation::EmptyValueColumn` for a value column of an empty
    /// name, `ConfigViolation::DuplicateValueColumn` for a column listed
    /// twice, `ConfigViolation::KeyColumnAsValue` for the key column
    /// listed as a value, and `ConfigViolation::OrderColumnIsKey` and
    /// `ConfigViolation::OrderColumnIsValue` for an order column that is
    /// the key or a value column; `index` is the table's place in
    /// `tables`.
    fn check_columns(&self, index: usize) -> std::result::Result<(), ConfigViolation> {
        let rewritten = |setting, column: &str| ConfigViolation::ColumnNameRewritten {
            index,
            setting,
            column: column.to_string(),
        };
        if !column::is_served_name(&self.key_column) {
            return Err(rewritten(ColumnSetting::KeyColumn, &self.key_column));
        }
        let mut seen = HashSet::new();
        for column in &self.value_columns {
            if column.name.is_empty() {
                return Err(ConfigViolation::EmptyValueColumn { index });
            }
            if !column::is_served_name(&column.name) {
                return Err(rewritten(ColumnSetting::ValueColumns, &column.name));
            }
            if column.name == self.key_column {
                return Err(ConfigViolation::KeyColumnAsValue {
                    index,
                    column: column.name.clone(),
                });
            }
            if !seen.insert(column.name.as_str()) {
                return Err(ConfigViolation::DuplicateValueColumn {
                    index,
                    column: column.name.clone(),
                });
            }
        }
        let Some(order) = &self.order_column else {
            return Ok(());
        };
        if !column::is_served_name(order) {
            return Err(rewritten(ColumnSetting::OrderColumn, order));
        }
        if *order == self.key_column {
            return Err(ConfigViolation::OrderColumnIsKey { index });
        }
        if seen.contains(order.as_str()) {
            return Err(ConfigViolation::OrderColumnIsValue {
                index,
                column: order.clone(),
            });
        }
        Ok(())
    }

    /// `ConfigViolation::MaxScanRowsOnKeyValueTable` for `max_scan_rows` on
    /// a table without an order column, `ConfigViolation::MaxScanRowsZero`
    /// for a zero one, and `ConfigViolation::MaxMultiScanValuesBelowScanRows`
    /// when `max_multi_scan_values` is below `least_max_multi_scan_values`
    /// of the table's `scan_rows`, so a `MultiScan` of one key at the full
    /// limit and one column is always valid (RFD 025); `index` is the
    /// table's place in `tables`.
    fn check_scan_rows(
        &self,
        index: usize,
        max_multi_scan_values: usize,
    ) -> std::result::Result<(), ConfigViolation> {
        let Some(max_scan_rows) = self.scan_rows() else {
            return match self.max_scan_rows {
                Some(_) => Err(ConfigViolation::MaxScanRowsOnKeyValueTable { index }),
                None => Ok(()),
            };
        };
        self.check_scan_rows_above_zero(Some(index))?;
        if max_multi_scan_values < least_max_multi_scan_values(max_scan_rows) {
            return Err(ConfigViolation::MaxMultiScanValuesBelowScanRows {
                index,
                max_scan_rows,
                max_multi_scan_values,
            });
        }
        Ok(())
    }

    /// The declared lookup level: `lookups` when set, else the best level
    /// the residency reaches (`Residency::best_level`).
    pub fn lookups(&self) -> Level {
        self.lookups.unwrap_or_else(|| self.residency.best_level())
    }

    /// Whether `other` differs from this entry in `lookups`, `strict`, the
    /// formats of the same value columns, and `max_scan_rows` alone,
    /// `lookups` and `max_scan_rows` compared as resolved: a reload applies
    /// such a change to the serving generation in place, since the level is
    /// judged from the layout the generation already holds (RFD 020), a
    /// format is read by each lookup from the entry (RFD 024), and the limit
    /// by each Scan (RFD 025). True for equal entries too.
    pub(crate) fn same_but_tuning(&self, other: &Self) -> bool {
        self.value_column_names() == other.value_column_names()
            && *self
                == Self {
                    lookups: self.lookups,
                    strict: self.strict,
                    value_columns: self.value_columns.clone(),
                    max_scan_rows: self.max_scan_rows,
                    ..other.clone()
                }
    }

    /// Whether the table's lookups read through the row cache: `row_cache`
    /// or `DEFAULT_ROW_CACHE` when unset, on a key-value table; false on an
    /// ordered table, whose Scans read pages.
    pub fn row_cache(&self) -> bool {
        self.order_column.is_none() && self.row_cache.unwrap_or(DEFAULT_ROW_CACHE)
    }

    /// The most rows one Scan returns, `max_scan_rows` or
    /// `DEFAULT_MAX_SCAN_ROWS` when unset, on an ordered table; `None` on
    /// a key-value table, which is never scanned.
    pub fn scan_rows(&self) -> Option<u32> {
        self.order_column
            .as_ref()
            .map(|_| self.max_scan_rows.unwrap_or(DEFAULT_MAX_SCAN_ROWS))
    }

    /// The limits a Scan of this table is held to, `max_scan_bytes` and
    /// `max_multi_scan_values` being the server's settings of those names,
    /// on an ordered table; `None` on a key-value table, which is never
    /// scanned.
    pub fn scan_limits(
        &self,
        max_scan_bytes: usize,
        max_multi_scan_values: usize,
    ) -> Option<ScanLimits> {
        self.scan_rows().map(|max_scan_rows| ScanLimits {
            max_scan_rows,
            max_scan_bytes,
            max_multi_scan_values,
        })
    }

    /// `ConfigViolation::MaxScanRowsZero` for an ordered table whose
    /// `max_scan_rows` is zero; `index` is the table's place in `tables`
    /// when validation asks, `None` when a load or retune does
    /// (`check_at_use`).
    fn check_scan_rows_above_zero(
        &self,
        index: Option<usize>,
    ) -> std::result::Result<(), ConfigViolation> {
        match self.scan_rows() {
            Some(0) => Err(ConfigViolation::MaxScanRowsZero {
                table: self.name.clone(),
                index,
            }),
            Some(_) | None => Ok(()),
        }
    }

    /// `check_lookups` and `check_scan_rows_above_zero` where the entry is
    /// used rather than validated, by a load and by a retune: a
    /// configuration built by hand meets an unreachable `lookups` or a zero
    /// `max_scan_rows` here, as `Error::Config`.
    pub(crate) fn check_at_use(&self) -> Result<()> {
        self.check_lookups(None)
            .and_then(|()| self.check_scan_rows_above_zero(None))
            .map_err(|violation| Error::Config(ConfigError::Invalid(violation)))
    }

    /// `ConfigViolation::LookupsUnreachable` when `lookups` is a level the
    /// residency cannot reach; `index` is the table's place in `tables`
    /// when validation asks, `None` when a load or retune does
    /// (`check_at_use`).
    pub(crate) fn check_lookups(
        &self,
        index: Option<usize>,
    ) -> std::result::Result<(), ConfigViolation> {
        let lookups = self.lookups();
        if self.residency.best_level().meets(lookups) {
            Ok(())
        } else {
            Err(ConfigViolation::LookupsUnreachable {
                table: self.name.clone(),
                index,
                lookups,
                residency: self.residency,
            })
        }
    }
}

impl PartialEq for TableConfig {
    fn eq(&self, other: &Self) -> bool {
        let Self {
            name,
            source,
            r#ref,
            key_column,
            value_columns: _,
            order_column,
            max_scan_rows: _,
            strict,
            lookups: _,
            residency,
            resident_budget_bytes,
            row_cache: _,
        } = self;
        *name == other.name
            && *source == other.source
            && *r#ref == other.r#ref
            && *key_column == other.key_column
            && self.sorted_value_columns() == other.sorted_value_columns()
            && *order_column == other.order_column
            && self.scan_rows() == other.scan_rows()
            && *strict == other.strict
            && self.lookups() == other.lookups()
            && *residency == other.residency
            && *resident_budget_bytes == other.resident_budget_bytes
            && self.row_cache() == other.row_cache()
    }
}

impl Eq for TableConfig {}

fn default_strict() -> bool {
    DEFAULT_STRICT
}

fn default_residency() -> Residency {
    DEFAULT_RESIDENCY
}

/// Where a table keeps its key chunks (RFD 017), ranked by lookup speed.
/// Chosen by the table owner and never by the loader. Display and serde give
/// the lowercase name that appears in the file, the doctor's output, and
/// logs.
// The serde names and Display are the same strings;
// `test_serialize_residency_matches_display` holds them together.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Residency {
    /// Key chunks in memory: one GET per column a hit names, none per miss.
    Full,
    /// Key chunks in a local file, fences in memory: one GET per column a
    /// hit names and one local read per hit, one local read per miss.
    Disk,
    /// Bloom filters in memory, no local disk: the key page's GET and one
    /// per column a hit names, a GET per miss at the filter's
    /// false-positive rate.
    Bloom,
}

impl Residency {
    /// Every tier, for the parser, the refusal message, and the tests that
    /// run a case per tier.
    pub const ALL: [Self; 3] = [Self::Full, Self::Disk, Self::Bloom];

    /// Every tier's name, comma-separated, for the message that refuses a
    /// name; rendered from `ALL` so the list cannot lag the enum.
    fn names() -> String {
        Self::ALL
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>()
            .join(", ")
    }

    /// The tier's lowercase name, as the file spells it.
    pub const fn name(self) -> &'static str {
        match self {
            Self::Full => "full",
            Self::Disk => "disk",
            Self::Bloom => "bloom",
        }
    }

    /// The best lookup level the tier reaches (RFD 020): `optimal` for
    /// `full` and `disk`; `balanced` for `bloom`, whose hit fetches its key
    /// page besides its value pages.
    pub(crate) const fn best_level(self) -> Level {
        match self {
            Self::Full | Self::Disk => Level::Optimal,
            Self::Bloom => Level::Balanced,
        }
    }
}

impl fmt::Display for Residency {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.name())
    }
}

impl std::str::FromStr for Residency {
    type Err = ConfigViolation;

    /// Exactly the lowercase names `Display` prints; the one boundary parser
    /// for the setting when it arrives from the command line rather than
    /// the file, where serde reads the same names.
    fn from_str(text: &str) -> std::result::Result<Self, Self::Err> {
        Self::ALL
            .into_iter()
            .find(|residency| residency.to_string() == text)
            .ok_or_else(|| ConfigViolation::UnknownResidency {
                text: text.to_string(),
            })
    }
}

/// Where a table's current metadata is read from (RFD 005). Tagged by a
/// `type` key in the file: `static`, `rest`, `glue`, or `s3tables`.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum SourceConfig {
    /// A metadata file location, or a table root whose `metadata/` carries
    /// a version hint.
    Static {
        /// The metadata file or table root.
        location: String,
    },
    /// An Iceberg REST catalog.
    Rest {
        /// The catalog's base URI.
        uri: String,
        /// The warehouse, when the catalog needs one.
        warehouse: Option<String>,
        /// The namespace, dot-separated.
        namespace: String,
        /// The table name within the namespace.
        table: String,
    },
    /// A table in AWS Glue (RFD 019).
    Glue {
        /// The Glue database.
        database: String,
        /// The table within the database.
        table: String,
        /// The id of another account's catalog; the caller's own when
        /// absent.
        catalog_id: Option<String>,
        /// The region of the Glue endpoint, over `[storage.s3]`'s and the
        /// SDK's.
        region: Option<String>,
        /// The endpoint URL, for a Glue-compatible service other than AWS's.
        endpoint: Option<String>,
    },
    /// A table in an S3 Tables table bucket (RFD 019).
    #[serde(rename = "s3tables")]
    S3Tables {
        /// The table bucket's ARN.
        table_bucket_arn: String,
        /// The namespace within the bucket.
        namespace: String,
        /// The table within the namespace.
        table: String,
        /// The region of the S3 Tables endpoint, over `[storage.s3]`'s and
        /// the ARN's.
        region: Option<String>,
        /// The endpoint URL, for an S3 Tables-compatible service other than
        /// AWS's.
        endpoint: Option<String>,
    },
}

/// A table setting that names columns, by its key in the file, for a
/// violation that names it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ColumnSetting {
    /// `key_column`.
    KeyColumn,
    /// `value_columns`.
    ValueColumns,
    /// `order_column`.
    OrderColumn,
}

impl fmt::Display for ColumnSetting {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::KeyColumn => "key_column",
            Self::ValueColumns => "value_columns",
            Self::OrderColumn => "order_column",
        })
    }
}

/// A setting of a table's source, by its key in the file, for a violation
/// that names it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SourceSetting {
    /// `location`, of a `static` source.
    Location,
    /// `database`, of a `glue` source.
    Database,
    /// `table`, of a `glue` or `s3tables` source.
    Table,
    /// `catalog_id`, of a `glue` source.
    CatalogId,
    /// `table_bucket_arn`, of an `s3tables` source.
    TableBucketArn,
    /// `namespace`, of an `s3tables` source.
    Namespace,
    /// `region`, of a `glue` or `s3tables` source.
    Region,
    /// `endpoint`, of a `glue` or `s3tables` source.
    Endpoint,
}

impl fmt::Display for SourceSetting {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Location => "location",
            Self::Database => "database",
            Self::Table => "table",
            Self::CatalogId => "catalog_id",
            Self::TableBucketArn => "table_bucket_arn",
            Self::Namespace => "namespace",
            Self::Region => "region",
            Self::Endpoint => "endpoint",
        })
    }
}

#[cfg(test)]
mod tests {
    use serde_path_to_error::Segment;

    use super::*;
    use crate::cache::min_memory_bytes;
    use crate::contract::VALUE_PAGE_BYTES;
    use crate::error::Error;
    use crate::storage::LocationFault;

    /// The gRPC listener the full fixture names.
    const FIXTURE_LISTEN: SocketAddr = SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), 7433);
    /// The metrics listener the full fixture names.
    const FIXTURE_METRICS_LISTEN: SocketAddr =
        SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), 9433);
    /// The poll interval the full fixture names, `"30s"`.
    const FIXTURE_POLL_INTERVAL: Duration = Duration::from_secs(30);
    /// The cache budget the full fixture names: 128 mebibytes.
    const FIXTURE_CACHE_BYTES: u64 = 128 * 1024 * 1024;
    /// The row cache's share of it the full fixture names.
    const FIXTURE_ROW_CACHE_SHARE: f64 = 0.75;
    /// The hedge percentile the full fixture names.
    const FIXTURE_HEDGE_PERCENTILE: f64 = 0.9;
    /// The hedge window the full fixture names.
    const FIXTURE_HEDGE_WINDOW: usize = 128;
    /// The hedge ceiling the full fixture names.
    const FIXTURE_HEDGE_MAX_RATE: f64 = 0.1;
    /// The chunk-file budget the full fixture names: 4 gibibytes.
    const FIXTURE_INDEX_BUDGET_BYTES: u64 = 4 * 1024 * 1024 * 1024;
    /// The disk page cache bound the full fixture names: 8 gibibytes.
    const FIXTURE_PAGE_CACHE_DISK_BYTES: u64 = 8 * 1024 * 1024 * 1024;
    /// The resident budget the full fixture puts on its `full` table: 1 gibibyte.
    const FIXTURE_RESIDENT_BUDGET_BYTES: u64 = 1024 * 1024 * 1024;
    /// A hedge percentile above the range.
    const PERCENTILE_TOO_HIGH: f64 = 1.5;
    /// A hedge ceiling above the range.
    const MAX_RATE_TOO_HIGH: f64 = 1.5;
    /// A hedge ceiling below the range.
    const MAX_RATE_NEGATIVE: f64 = -0.5;
    /// The `Get` deadline the full fixture names, `"2s"`.
    const FIXTURE_GET_DEADLINE: Duration = Duration::from_secs(2);
    /// The `MultiGet` deadline the full fixture names, `"8s"`.
    const FIXTURE_MULTI_GET_DEADLINE: Duration = Duration::from_secs(8);
    /// The in-flight request limit the full fixture names.
    const FIXTURE_MAX_IN_FLIGHT: usize = 512;
    /// The `MultiGet` value limit the full fixture names.
    const FIXTURE_MAX_MULTI_GET_VALUES: usize = 500;
    /// The batch fan-out the full fixture names.
    const FIXTURE_BATCH_FAN_OUT: usize = 16;
    /// The `MultiScan` value limit the full fixture names.
    const FIXTURE_MAX_MULTI_SCAN_VALUES: usize = 2_000;
    /// The `Scan` deadline the full fixture names, `"3s"`.
    const FIXTURE_SCAN_DEADLINE: Duration = Duration::from_secs(3);
    /// The `MultiScan` deadline the full fixture names, `"9s"`.
    const FIXTURE_MULTI_SCAN_DEADLINE: Duration = Duration::from_secs(9);
    /// The `max_scan_bytes` the full fixture names.
    const FIXTURE_MAX_SCAN_BYTES: usize = 8 * 1024 * 1024;
    /// The `max_scan_rows` the full fixture's ordered table names, below
    /// half of `FIXTURE_MAX_MULTI_SCAN_VALUES`.
    const FIXTURE_MAX_SCAN_ROWS: u32 = 500;
    /// The load deadline the full fixture names, `"45m"`.
    const FIXTURE_LOAD_DEADLINE: Duration = Duration::from_mins(45);
    /// The refresh deadline the full fixture names, `"10m"`.
    const FIXTURE_REFRESH_DEADLINE: Duration = Duration::from_mins(10);
    /// The drain deadline the full fixture names, `"25s"`.
    const FIXTURE_DRAIN_DEADLINE: Duration = Duration::from_secs(25);
    /// The S3 retry count the full fixture names.
    const FIXTURE_MAX_RETRIES: usize = 3;
    /// The S3 connect timeout the full fixture names, `"500ms"`.
    const FIXTURE_CONNECT_TIMEOUT: Duration = Duration::from_millis(500);
    /// The catalog timeout the full fixture names, `"1m"`.
    const FIXTURE_CATALOG_TIMEOUT: Duration = Duration::from_secs(60);
    /// The top of both hedge ranges, which is inside each.
    const HEDGE_RANGE_TOP: f64 = 1.0;
    /// The bottom of the ceiling's range, which is inside it.
    const MAX_RATE_BOTTOM: f64 = 0.0;
    /// The table bucket the full fixture's `s3tables` table is in.
    const FIXTURE_TABLE_BUCKET_ARN: &str =
        "arn:aws:s3tables:us-example-1:111122223333:bucket/example";

    /// `TOML_MINIMAL`'s table with `source` as its inline source table.
    fn minimal_with_source(source: &str) -> String {
        TOML_MINIMAL.replace(
            r#"{ type = "static", location = "s3://example-warehouse/db/kv" }"#,
            source,
        )
    }

    /// A Glue source with only its required keys, then `extra`.
    fn glue_source(extra: &str) -> String {
        minimal_with_source(&format!(
            r#"{{ type = "glue", database = "db", table = "kv"{extra} }}"#
        ))
    }

    /// An S3 Tables source with only its required keys, the ARN `arn`,
    /// then `extra`.
    fn s3tables_source(arn: &str, extra: &str) -> String {
        minimal_with_source(&format!(
            r#"{{ type = "s3tables", table_bucket_arn = "{arn}", namespace = "db", table = "kv"{extra} }}"#
        ))
    }

    /// One complete configuration: every optional section present and four
    /// tables, one per source kind, each with every key it takes. `json_full`
    /// is the same document. Every number is a `FIXTURE_*` constant, so the
    /// value has one home.
    fn toml_full() -> String {
        format!(
            r#"
[server]
listen = "{FIXTURE_LISTEN}"
metrics_listen = "{FIXTURE_METRICS_LISTEN}"
poll_interval = "30s"
cache_bytes = {FIXTURE_CACHE_BYTES}
row_cache_share = {FIXTURE_ROW_CACHE_SHARE}
get_deadline = "2s"
multi_get_deadline = "8s"
max_in_flight = {FIXTURE_MAX_IN_FLIGHT}
max_multi_get_values = {FIXTURE_MAX_MULTI_GET_VALUES}
batch_fan_out = {FIXTURE_BATCH_FAN_OUT}
max_multi_scan_values = {FIXTURE_MAX_MULTI_SCAN_VALUES}
scan_deadline = "3s"
multi_scan_deadline = "9s"
max_scan_bytes = {FIXTURE_MAX_SCAN_BYTES}
load_deadline = "45m"
refresh_deadline = "10m"
drain_deadline = "25s"

[server.log]
fields = {{ time = "ts", level = "severity" }}
static_fields = {{ service = "icetray" }}

[storage]
max_retries = {FIXTURE_MAX_RETRIES}
connect_timeout = "500ms"
catalog_timeout = "1m"

[storage.s3]
endpoint = "http://object-store.example:9000"
region = "us-example-1"
access_key_id = "AKIAEXAMPLE"
secret_access_key = "secret-example"
allow_http = true
path_style = true

[[storage.rewrite]]
from = "s3://old-warehouse/"
to = "s3://example-warehouse/"

[storage.hedge]
enabled = true
percentile = {FIXTURE_HEDGE_PERCENTILE:?}
window = {FIXTURE_HEDGE_WINDOW}
max_rate = {FIXTURE_HEDGE_MAX_RATE:?}

[storage.local]
dir = "/var/lib/icetray"
index_budget_bytes = {FIXTURE_INDEX_BUDGET_BYTES}
page_cache_disk_bytes = {FIXTURE_PAGE_CACHE_DISK_BYTES}

[[tables]]
name = "orders"
ref = "main"
key_column = "id"
value_columns = ["summary", "payload"]
strict = true
lookups = "balanced"
residency = "full"
resident_budget_bytes = {FIXTURE_RESIDENT_BUDGET_BYTES}

[tables.source]
type = "static"
location = "s3://example-warehouse/db/orders/metadata/v3.metadata.json"

[[tables]]
name = "profiles"
ref = "serving"
key_column = "user_id"
value_columns = ["vector"]
residency = "disk"

[tables.source]
type = "rest"
uri = "http://catalog.example:8181"
warehouse = "example"
namespace = "db"
table = "profiles"

[[tables]]
name = "sessions"
ref = "main"
key_column = "id"
value_columns = ["payload"]

[tables.source]
type = "glue"
database = "db"
table = "sessions"
catalog_id = "111122223333"
region = "us-example-1"
endpoint = "https://glue.example"

[[tables]]
name = "events"
ref = "main"
key_column = "id"
value_columns = ["payload"]
order_column = "ts"
max_scan_rows = {FIXTURE_MAX_SCAN_ROWS}

[tables.source]
type = "s3tables"
table_bucket_arn = "{FIXTURE_TABLE_BUCKET_ARN}"
namespace = "db"
table = "events"
region = "us-example-1"
endpoint = "https://s3tables.example"
"#
        )
    }

    /// The document `toml_full` renders, as JSON.
    fn json_full() -> String {
        serde_json::json!({
            "server": {
                "listen": FIXTURE_LISTEN.to_string(),
                "metrics_listen": FIXTURE_METRICS_LISTEN.to_string(),
                "poll_interval": "30s",
                "cache_bytes": FIXTURE_CACHE_BYTES,
                "row_cache_share": FIXTURE_ROW_CACHE_SHARE,
                "get_deadline": "2s",
                "multi_get_deadline": "8s",
                "max_in_flight": FIXTURE_MAX_IN_FLIGHT,
                "max_multi_get_values": FIXTURE_MAX_MULTI_GET_VALUES,
                "batch_fan_out": FIXTURE_BATCH_FAN_OUT,
                "max_multi_scan_values": FIXTURE_MAX_MULTI_SCAN_VALUES,
                "scan_deadline": "3s",
                "multi_scan_deadline": "9s",
                "max_scan_bytes": FIXTURE_MAX_SCAN_BYTES,
                "load_deadline": "45m",
                "refresh_deadline": "10m",
                "drain_deadline": "25s",
                "log": {
                    "fields": {"time": "ts", "level": "severity"},
                    "static_fields": {"service": "icetray"}
                }
            },
            "storage": {
                "max_retries": FIXTURE_MAX_RETRIES,
                "connect_timeout": "500ms",
                "catalog_timeout": "1m",
                "s3": {
                    "endpoint": "http://object-store.example:9000",
                    "region": "us-example-1",
                    "access_key_id": "AKIAEXAMPLE",
                    "secret_access_key": "secret-example",
                    "allow_http": true,
                    "path_style": true
                },
                "rewrite": [{"from": "s3://old-warehouse/", "to": "s3://example-warehouse/"}],
                "hedge": {
                    "enabled": true,
                    "percentile": FIXTURE_HEDGE_PERCENTILE,
                    "window": FIXTURE_HEDGE_WINDOW,
                    "max_rate": FIXTURE_HEDGE_MAX_RATE
                },
                "local": {
                    "dir": "/var/lib/icetray",
                    "index_budget_bytes": FIXTURE_INDEX_BUDGET_BYTES,
                    "page_cache_disk_bytes": FIXTURE_PAGE_CACHE_DISK_BYTES
                }
            },
            "tables": json_tables()
        })
        .to_string()
    }

    /// The tables of `json_full`, one per source kind.
    fn json_tables() -> serde_json::Value {
        serde_json::json!([
            {
                "name": "orders",
                "source": {
                    "type": "static",
                    "location": "s3://example-warehouse/db/orders/metadata/v3.metadata.json"
                },
                "ref": "main",
                "key_column": "id",
                "value_columns": ["summary", "payload"],
                "strict": true,
                "lookups": "balanced",
                "residency": "full",
                "resident_budget_bytes": FIXTURE_RESIDENT_BUDGET_BYTES
            },
            {
                "name": "profiles",
                "source": {
                    "type": "rest",
                    "uri": "http://catalog.example:8181",
                    "warehouse": "example",
                    "namespace": "db",
                    "table": "profiles"
                },
                "ref": "serving",
                "key_column": "user_id",
                "value_columns": ["vector"],
                "residency": "disk"
            },
            {
                "name": "sessions",
                "source": {
                    "type": "glue",
                    "database": "db",
                    "table": "sessions",
                    "catalog_id": "111122223333",
                    "region": "us-example-1",
                    "endpoint": "https://glue.example"
                },
                "ref": "main",
                "key_column": "id",
                "value_columns": ["payload"]
            },
            {
                "name": "events",
                "source": {
                    "type": "s3tables",
                    "table_bucket_arn": FIXTURE_TABLE_BUCKET_ARN,
                    "namespace": "db",
                    "table": "events",
                    "region": "us-example-1",
                    "endpoint": "https://s3tables.example"
                },
                "ref": "main",
                "key_column": "id",
                "value_columns": ["payload"],
                "order_column": "ts",
                "max_scan_rows": FIXTURE_MAX_SCAN_ROWS
            }
        ])
    }

    /// The smallest valid configuration: one table, everything else defaulted.
    /// The source is an inline table so that a test can append table keys
    /// after it and have them land in the table, not in the source.
    const TOML_MINIMAL: &str = r#"
[[tables]]
name = "kv"
source = { type = "static", location = "s3://example-warehouse/db/kv" }
ref = "main"
key_column = "key"
value_columns = ["value"]
"#;

    /// The value `toml_full` and `json_full` both denote.
    fn full_config() -> Config {
        Config {
            server: ServerConfig {
                listen: FIXTURE_LISTEN,
                metrics_listen: FIXTURE_METRICS_LISTEN,
                poll_interval: FIXTURE_POLL_INTERVAL,
                cache_bytes: Some(FIXTURE_CACHE_BYTES),
                row_cache_share: Some(FIXTURE_ROW_CACHE_SHARE),
                removed_page_cache_bytes: None,
                removed_scan_page_cache_bytes: None,
                log: LogConfig {
                    fields: BTreeMap::from([
                        (StandardField::Time, "ts".to_string()),
                        (StandardField::Level, "severity".to_string()),
                    ]),
                    static_fields: BTreeMap::from([("service".to_string(), "icetray".to_string())]),
                },
                get_deadline: FIXTURE_GET_DEADLINE,
                multi_get_deadline: FIXTURE_MULTI_GET_DEADLINE,
                max_in_flight: FIXTURE_MAX_IN_FLIGHT,
                max_multi_get_values: FIXTURE_MAX_MULTI_GET_VALUES,
                batch_fan_out: FIXTURE_BATCH_FAN_OUT,
                max_multi_scan_values: FIXTURE_MAX_MULTI_SCAN_VALUES,
                scan_deadline: FIXTURE_SCAN_DEADLINE,
                multi_scan_deadline: FIXTURE_MULTI_SCAN_DEADLINE,
                max_scan_bytes: FIXTURE_MAX_SCAN_BYTES,
                load_deadline: FIXTURE_LOAD_DEADLINE,
                refresh_deadline: FIXTURE_REFRESH_DEADLINE,
                drain_deadline: FIXTURE_DRAIN_DEADLINE,
            },
            storage: StorageConfig {
                s3: Some(S3Config {
                    endpoint: Some("http://object-store.example:9000".to_string()),
                    region: Some("us-example-1".to_string()),
                    access_key_id: Some("AKIAEXAMPLE".to_string()),
                    secret_access_key: Some("secret-example".to_string()),
                    allow_http: true,
                    path_style: true,
                }),
                rewrite: vec![RewriteRule {
                    from: "s3://old-warehouse/".to_string(),
                    to: "s3://example-warehouse/".to_string(),
                }],
                hedge: HedgeConfig {
                    enabled: true,
                    percentile: FIXTURE_HEDGE_PERCENTILE,
                    window: FIXTURE_HEDGE_WINDOW,
                    max_rate: FIXTURE_HEDGE_MAX_RATE,
                },
                local: Some(LocalConfig {
                    dir: PathBuf::from("/var/lib/icetray"),
                    index_budget_bytes: Some(FIXTURE_INDEX_BUDGET_BYTES),
                    page_cache_disk_bytes: FIXTURE_PAGE_CACHE_DISK_BYTES,
                }),
                max_retries: FIXTURE_MAX_RETRIES,
                connect_timeout: FIXTURE_CONNECT_TIMEOUT,
                catalog_timeout: FIXTURE_CATALOG_TIMEOUT,
            },
            tables: full_tables(),
        }
    }

    /// The tables of `full_config`, one per source kind.
    fn full_tables() -> Vec<TableConfig> {
        vec![
            TableConfig {
                name: "orders".to_string(),
                source: SourceConfig::Static {
                    location: "s3://example-warehouse/db/orders/metadata/v3.metadata.json"
                        .to_string(),
                },
                r#ref: "main".to_string(),
                key_column: "id".to_string(),
                value_columns: vec![
                    ValueColumnConfig::named("summary"),
                    ValueColumnConfig::named("payload"),
                ],
                order_column: None,
                max_scan_rows: None,
                strict: true,
                lookups: Some(Level::Balanced),
                residency: Residency::Full,
                resident_budget_bytes: Some(FIXTURE_RESIDENT_BUDGET_BYTES),
                row_cache: None,
            },
            TableConfig {
                name: "profiles".to_string(),
                source: SourceConfig::Rest {
                    uri: "http://catalog.example:8181".to_string(),
                    warehouse: Some("example".to_string()),
                    namespace: "db".to_string(),
                    table: "profiles".to_string(),
                },
                r#ref: "serving".to_string(),
                key_column: "user_id".to_string(),
                value_columns: vec![ValueColumnConfig::named("vector")],
                order_column: None,
                max_scan_rows: None,
                strict: DEFAULT_STRICT,
                lookups: None,
                residency: Residency::Disk,
                resident_budget_bytes: None,
                row_cache: None,
            },
            TableConfig {
                name: "sessions".to_string(),
                source: SourceConfig::Glue {
                    database: "db".to_string(),
                    table: "sessions".to_string(),
                    catalog_id: Some("111122223333".to_string()),
                    region: Some("us-example-1".to_string()),
                    endpoint: Some("https://glue.example".to_string()),
                },
                r#ref: "main".to_string(),
                key_column: "id".to_string(),
                value_columns: vec![ValueColumnConfig::named("payload")],
                order_column: None,
                max_scan_rows: None,
                strict: DEFAULT_STRICT,
                lookups: None,
                residency: DEFAULT_RESIDENCY,
                resident_budget_bytes: None,
                row_cache: None,
            },
            TableConfig {
                name: "events".to_string(),
                source: SourceConfig::S3Tables {
                    table_bucket_arn: FIXTURE_TABLE_BUCKET_ARN.to_string(),
                    namespace: "db".to_string(),
                    table: "events".to_string(),
                    region: Some("us-example-1".to_string()),
                    endpoint: Some("https://s3tables.example".to_string()),
                },
                r#ref: "main".to_string(),
                key_column: "id".to_string(),
                value_columns: vec![ValueColumnConfig::named("payload")],
                order_column: Some("ts".to_string()),
                max_scan_rows: Some(FIXTURE_MAX_SCAN_ROWS),
                strict: DEFAULT_STRICT,
                lookups: None,
                residency: DEFAULT_RESIDENCY,
                resident_budget_bytes: None,
                row_cache: None,
            },
        ]
    }

    /// The violation `text` parses to; anything else fails the test.
    fn violation(text: &str) -> ConfigViolation {
        let result = Config::parse(text);
        let Err(ConfigError::Invalid(violation)) = &result else {
            panic!("expected a configuration violation, got {result:?}");
        };
        violation.clone()
    }

    #[test]
    fn test_parse_toml_and_json_are_equivalent() {
        let from_toml = Config::parse(&toml_full()).unwrap();
        let from_json = Config::parse(&json_full()).unwrap();
        assert_eq!(from_toml, from_json);
        assert_eq!(from_toml, full_config());
    }

    #[test]
    fn test_parse_applies_defaults_when_absent() {
        let config = Config::parse(TOML_MINIMAL).unwrap();
        assert_eq!(config.server, ServerConfig::default());
        assert_eq!(config.server.listen, DEFAULT_LISTEN);
        assert_eq!(config.server.metrics_listen, DEFAULT_METRICS_LISTEN);
        assert_eq!(config.server.poll_interval, DEFAULT_POLL_INTERVAL);
        assert_eq!(config.server.cache_bytes, None);
        assert_eq!(config.server.row_cache_share, None);
        assert!(config.server.log.fields.is_empty());
        assert!(config.server.log.static_fields.is_empty());
        assert_eq!(config.server.get_deadline, DEFAULT_GET_DEADLINE);
        assert_eq!(config.server.multi_get_deadline, DEFAULT_MULTI_GET_DEADLINE);
        assert_eq!(config.server.max_in_flight, DEFAULT_MAX_IN_FLIGHT);
        assert_eq!(
            config.server.max_multi_get_values,
            DEFAULT_MAX_MULTI_GET_VALUES
        );
        assert_eq!(config.server.batch_fan_out, DEFAULT_BATCH_FAN_OUT);
        assert_eq!(config.server.load_deadline, DEFAULT_LOAD_DEADLINE);
        assert_eq!(config.server.refresh_deadline, DEFAULT_REFRESH_DEADLINE);
        assert_eq!(config.server.drain_deadline, DEFAULT_DRAIN_DEADLINE);
        assert_eq!(config.storage, StorageConfig::default());
        assert_eq!(config.storage.max_retries, DEFAULT_MAX_RETRIES);
        assert_eq!(config.storage.connect_timeout, DEFAULT_CONNECT_TIMEOUT);
        assert_eq!(config.storage.catalog_timeout, DEFAULT_CATALOG_TIMEOUT);
        assert_eq!(config.storage.s3, None);
        assert!(config.storage.rewrite.is_empty());
        assert_eq!(config.storage.hedge, HedgeConfig::default());
        assert!(config.storage.hedge.enabled);
        assert_eq!(config.storage.hedge.window, DEFAULT_HEDGE_WINDOW);
        assert_eq!(config.storage.local, None);
        assert_eq!(config.tables.len(), 1);
        assert!(!config.tables[0].strict);
        assert_eq!(config.tables[0].lookups, None);
        assert_eq!(config.tables[0].lookups(), Level::Optimal);
        assert_eq!(config.tables[0].residency, Residency::Full);
        assert_eq!(config.tables[0].resident_budget_bytes, None);
    }

    #[test]
    fn test_parse_s3_defaults_allow_http_and_path_style_off() {
        let config = Config::parse(&format!(
            "{TOML_MINIMAL}\n[storage.s3]\nregion = \"us-example-1\"\n"
        ))
        .unwrap();
        let s3 = config.storage.s3.unwrap();
        assert!(!s3.allow_http);
        assert!(!s3.path_style);
        assert_eq!(s3.access_key_id, None);
        assert_eq!(s3.secret_access_key, None);
    }

    /// One step of a schema fault's place, comparable.
    #[derive(Debug, PartialEq, Eq)]
    enum At {
        /// A key of a table or object.
        Key(String),
        /// An entry of a list.
        Index(usize),
        /// A variant of an externally tagged enum.
        Variant(String),
        /// A step serde could not name.
        Unknown,
    }

    /// Where `text`'s schema fault is, as the steps serde walked to it.
    /// Anything but a schema fault fails the test.
    fn schema_fault_at(text: &str) -> Vec<At> {
        match Config::parse(text) {
            Err(ConfigError::Schema { at, .. }) => at
                .iter()
                .map(|segment| match segment {
                    Segment::Map { key } => At::Key(key.clone()),
                    Segment::Seq { index } => At::Index(*index),
                    Segment::Enum { variant } => At::Variant(variant.clone()),
                    Segment::Unknown => At::Unknown,
                })
                .collect(),
            other => panic!("expected a schema fault, got {other:?}"),
        }
    }

    /// The step to `key`.
    fn key(key: &str) -> At {
        At::Key(key.to_string())
    }

    #[test]
    fn test_parse_rejects_unknown_field_as_a_schema_fault() {
        let toml = format!("{TOML_MINIMAL}\n[server]\nlisten_addr = \"127.0.0.1:1\"\n");
        assert_eq!(schema_fault_at(&toml), [key("server"), key("listen_addr")]);
        let nested = format!("{TOML_MINIMAL}\n[storage.hedge]\npercentil = 0.5\n");
        assert_eq!(
            schema_fault_at(&nested),
            [key("storage"), key("hedge"), key("percentil")]
        );
        let json = r#"{"server": {"listen_addr": "127.0.0.1:1"}}"#;
        assert_eq!(schema_fault_at(json), [key("server"), key("listen_addr")]);
        // The source is the one internally tagged enum in the schema, which
        // serde buffers, so the fault is placed at the source itself.
        let source = r#"
[[tables]]
name = "kv"
ref = "main"
key_column = "key"
value_columns = ["value"]

[tables.source]
type = "static"
location = "s3://example-warehouse/db/kv"
locaton = "typo"
"#;
        assert_eq!(
            schema_fault_at(source),
            [key("tables"), At::Index(0), key("source")]
        );
    }

    #[test]
    fn test_parse_reports_a_missing_key_at_its_table_in_either_syntax() {
        let toml = TOML_MINIMAL.replace("ref = \"main\"\n", "");
        let at_first_table = [key("tables"), At::Index(0)];
        assert_eq!(schema_fault_at(&toml), at_first_table);
        let json = r#"{"tables": [{"name": "kv", "source": {"type": "static", "location": "s3://example-warehouse/db/kv"}, "key_column": "key", "value_columns": ["value"]}]}"#;
        assert_eq!(schema_fault_at(json), at_first_table);
    }

    #[test]
    fn test_parse_reports_a_value_of_the_wrong_form_at_its_key() {
        let toml = format!("{TOML_MINIMAL}\n[server]\npoll_interval = \"10\"\n");
        assert_eq!(
            schema_fault_at(&toml),
            [key("server"), key("poll_interval")]
        );
        let json = r#"{"server": {"max_in_flight": "many"}}"#;
        assert_eq!(schema_fault_at(json), [key("server"), key("max_in_flight")]);
    }

    #[test]
    fn test_text_position_of_counts_lines_and_characters_from_one() {
        let text = "ab\nçd\n";
        let cases = [
            (0, 1, 1),
            (2, 1, 3),
            (3, 2, 1),
            (5, 2, 2),
            (text.len(), 3, 1),
        ];
        for (offset, line, column) in cases {
            assert_eq!(
                TextPosition::of(text, offset),
                TextPosition { line, column },
                "{offset}"
            );
        }
    }

    #[test]
    fn test_place_spells_a_setting_as_the_file_does() {
        let text = TOML_MINIMAL.replace("ref = \"main\"\n", "ref = \"main\"\nstrict = 1\n");
        let Err(ConfigError::Schema { at, .. }) = Config::parse(&text) else {
            panic!("strict = 1 is the wrong type");
        };
        assert_eq!(place(&at), "tables[0].strict");
        let log = format!("{TOML_MINIMAL}\n[server.log.fields]\ntimestamp = \"ts\"\n");
        let Err(ConfigError::Schema { at, .. }) = Config::parse(&log) else {
            panic!("timestamp names no standard field");
        };
        assert_eq!(place(&at), "server.log.fields");
    }

    #[test]
    fn test_parse_reports_malformed_text_as_its_syntax_s_error() {
        let toml = Config::parse("[[tables]\n");
        assert!(matches!(toml, Err(ConfigError::Toml(_))), "{toml:?}");
        let unquoted = Config::parse("[server]\nlisten = \n");
        let Err(ConfigError::Toml(syntax)) = unquoted else {
            panic!("expected a toml syntax error, got {unquoted:?}");
        };
        assert_eq!(
            syntax.at,
            Some(TextPosition {
                line: 2,
                column: 10
            })
        );
        for json in [
            "{\"tables\": [",
            "{\"tables\": []} trailing",
            "{\"tables\" []}",
        ] {
            let result = Config::parse(json);
            assert!(
                matches!(result, Err(ConfigError::Json(_))),
                "{json}: {result:?}"
            );
        }
    }

    #[test]
    fn test_parse_rejects_a_log_field_that_names_no_standard_field() {
        let toml = format!("{TOML_MINIMAL}\n[server.log.fields]\ntimestamp = \"ts\"\n");
        let at_fields = [key("server"), key("log"), key("fields"), At::Unknown];
        assert_eq!(schema_fault_at(&toml), at_fields);
        let json = r#"{"server": {"log": {"fields": {"timestamp": "ts"}}}}"#;
        assert_eq!(schema_fault_at(json), at_fields);
    }

    #[test]
    fn test_parse_refuses_two_log_fields_under_one_name() {
        let onto_a_default = format!("{TOML_MINIMAL}\n[server.log.fields]\nmessage = \"level\"\n");
        assert_eq!(
            violation(&onto_a_default),
            ConfigViolation::LogFieldCollision {
                name: "level".to_string(),
                first: StandardField::Level,
                second: StandardField::Message,
            }
        );
        let two_onto_one =
            format!("{TOML_MINIMAL}\n[server.log.fields]\ntime = \"x\"\ntarget = \"x\"\n");
        assert_eq!(
            violation(&two_onto_one),
            ConfigViolation::LogFieldCollision {
                name: "x".to_string(),
                first: StandardField::Time,
                second: StandardField::Target,
            }
        );
        let swapped = format!(
            "{TOML_MINIMAL}\n[server.log.fields]\nlevel = \"message\"\nmessage = \"level\"\n"
        );
        assert!(Config::parse(&swapped).is_ok());
    }

    #[test]
    fn test_emitted_name_is_the_mapped_name_or_the_field_s_own() {
        let log = full_config().server.log;
        assert_eq!(log.emitted_name(StandardField::Time), "ts");
        assert_eq!(log.emitted_name(StandardField::Level), "severity");
        assert_eq!(log.emitted_name(StandardField::Message), "message");
        assert_eq!(log.emitted_name(StandardField::Target), "target");
    }

    #[test]
    fn test_from_text_dispatches_on_first_non_whitespace_brace() {
        let json = Config::from_text("  \n\t {\"tables\": []}").unwrap();
        assert!(json.tables.is_empty());
        let toml = Config::from_text("  \n\t tables = []\n").unwrap();
        assert!(toml.tables.is_empty());
        // A brace later in the text does not make TOML into JSON.
        let inline = Config::from_text("storage = { rewrite = [] }\n").unwrap();
        assert!(inline.storage.rewrite.is_empty());
        let malformed_json = Config::from_text(" { not json");
        assert!(matches!(malformed_json, Err(ConfigError::Json(_))));
        let malformed_toml = Config::from_text("not = toml = either");
        assert!(matches!(malformed_toml, Err(ConfigError::Toml(_))));
    }

    #[test]
    fn test_parse_refuses_a_configuration_with_no_table() {
        for text in ["", "tables = []\n", "{}", "{\"tables\": []}", "[server]\n"] {
            assert_eq!(violation(text), ConfigViolation::NoTables, "{text:?}");
        }
    }

    #[test]
    fn test_parse_refuses_empty_table_name() {
        let text = TOML_MINIMAL.replace("name = \"kv\"", "name = \"\"");
        assert_eq!(
            violation(&text),
            ConfigViolation::EmptyTableName { index: 0 }
        );
    }

    /// Two configurations that differ in their tables alone need no
    /// restart; each other setting that differs is named as the file
    /// names it.
    #[test]
    fn test_restart_settings_names_what_differs_outside_the_tables() {
        let running = Config::parse(TOML_MINIMAL).unwrap();
        let mut next = running.clone();
        next.tables.clear();
        assert!(running.restart_settings(&next).is_empty());
        next.server.poll_interval += Duration::from_secs(1);
        next.server.batch_fan_out += 1;
        next.server.max_multi_scan_values += 1;
        next.server.scan_deadline += Duration::from_secs(1);
        next.server.multi_scan_deadline += Duration::from_secs(1);
        next.server.max_scan_bytes += 1;
        next.storage.max_retries += 1;
        assert_eq!(
            running.restart_settings(&next),
            [
                "server.poll_interval",
                "server.batch_fan_out",
                "server.max_multi_scan_values",
                "server.scan_deadline",
                "server.multi_scan_deadline",
                "server.max_scan_bytes",
                "storage.max_retries"
            ]
        );
    }

    #[test]
    fn test_restart_settings_names_cache_bytes_and_row_cache_share() {
        let running = Config::parse(TOML_MINIMAL).unwrap();
        let mut next = running.clone();
        next.server.cache_bytes = Some(FIXTURE_CACHE_BYTES);
        next.server.row_cache_share = Some(FIXTURE_ROW_CACHE_SHARE);
        assert_eq!(
            running.restart_settings(&next),
            ["server.cache_bytes", "server.row_cache_share"]
        );
    }

    /// Asserts that a file setting the removed `[server]` key `setting` is
    /// refused naming `cache_bytes`, in either syntax, rather than failing
    /// the parse as an unknown key.
    fn assert_refused_naming_cache_bytes(setting: &'static str) {
        let toml = format!("[server]\n{setting} = 1048576\n{TOML_MINIMAL}");
        let json = format!(
            r#"{{"server": {{"{setting}": 1048576}}, "tables": [{{"name": "kv", "source": {{"type": "static", "location": "s3://example-warehouse/db/kv"}}, "ref": "main", "key_column": "key", "value_columns": ["value"]}}]}}"#
        );
        for text in [toml, json] {
            assert_eq!(
                violation(&text),
                ConfigViolation::RemovedSetting {
                    setting,
                    replacement: "cache_bytes",
                },
                "{text}"
            );
        }
    }

    #[test]
    fn test_page_cache_bytes_is_refused_naming_cache_bytes() {
        assert_refused_naming_cache_bytes("page_cache_bytes");
    }

    #[test]
    fn test_scan_page_cache_bytes_is_refused_naming_cache_bytes() {
        assert_refused_naming_cache_bytes("scan_page_cache_bytes");
    }

    #[test]
    fn test_row_cache_share_outside_zero_to_one_is_refused() {
        let with_share = |share: f64| {
            let mut config = Config::parse(TOML_MINIMAL).unwrap();
            config.server.row_cache_share = Some(share);
            config.validated()
        };
        for share in [-0.1, 1.5, f64::NAN] {
            let refused = with_share(share);
            assert!(
                matches!(
                    refused,
                    Err(Error::Config(ConfigError::Invalid(
                        ConfigViolation::RowCacheShareOutOfRange { share: refused }
                    ))) if refused.to_bits() == share.to_bits()
                ),
                "{share}: {refused:?}"
            );
        }
        for share in [0.0, 1.0] {
            assert!(with_share(share).is_ok(), "{share}");
        }
        let toml = format!("[server]\nrow_cache_share = 1.5\n{TOML_MINIMAL}");
        assert_eq!(
            violation(&toml),
            ConfigViolation::RowCacheShareOutOfRange { share: 1.5 }
        );
    }

    #[test]
    fn test_parse_refuses_duplicate_table_name() {
        let text = format!("{TOML_MINIMAL}{TOML_MINIMAL}");
        assert_eq!(
            violation(&text),
            ConfigViolation::DuplicateTableName {
                index: 1,
                name: "kv".to_string(),
            }
        );
    }

    /// `TOML_MINIMAL` with its value columns listed as `columns`, a TOML
    /// array.
    fn with_value_columns(columns: &str) -> String {
        TOML_MINIMAL.replace(
            r#"value_columns = ["value"]"#,
            &format!("value_columns = {columns}"),
        )
    }

    #[test]
    fn test_parse_value_column_is_a_name_or_a_table_of_name_and_format() {
        let mixed = with_value_columns(
            r#"["value", { name = "detail", format = "protobuf" }, { name = "blob" }, { name = "raw", format = "opaque" }]"#,
        );
        let toml = Config::parse(&mixed).unwrap().tables[0].clone();
        assert_eq!(
            toml.value_columns,
            [
                ValueColumnConfig::named("value"),
                ValueColumnConfig::of("detail", ValueFormat::Protobuf),
                ValueColumnConfig::named("blob"),
                ValueColumnConfig::named("raw"),
            ]
        );
        assert_eq!(
            toml.value_formats(),
            [
                ValueFormat::Opaque,
                ValueFormat::Protobuf,
                ValueFormat::Opaque,
                ValueFormat::Opaque
            ],
            "by name: blob, detail, raw, value"
        );
        assert_eq!(toml.format_of("detail"), Some(ValueFormat::Protobuf));
        assert_eq!(toml.format_of("absent"), None);
        let json = r#"{"tables": [{"name": "kv", "source": {"type": "static", "location": "s3://example-warehouse/db/kv"}, "ref": "main", "key_column": "key", "value_columns": ["value", {"name": "detail", "format": "protobuf"}, {"name": "blob"}, {"name": "raw", "format": "opaque"}]}]}"#;
        assert_eq!(Config::parse(json).unwrap().tables[0], toml);
    }

    #[test]
    fn test_parse_value_column_table_refuses_an_unknown_key_or_format_where_it_is() {
        let unknown = with_value_columns(r#"["value", { name = "detail", fromat = "protobuf" }]"#);
        assert_eq!(
            schema_fault_at(&unknown),
            [
                key("tables"),
                At::Index(0),
                key("value_columns"),
                At::Index(1),
                key("fromat")
            ]
        );
        let format = with_value_columns(r#"[{ name = "detail", format = "json" }]"#);
        assert_eq!(
            schema_fault_at(&format),
            [
                key("tables"),
                At::Index(0),
                key("value_columns"),
                At::Index(0),
                key("format")
            ]
        );
        let nameless = with_value_columns(r#"[{ format = "protobuf" }]"#);
        assert_eq!(
            schema_fault_at(&nameless),
            [
                key("tables"),
                At::Index(0),
                key("value_columns"),
                At::Index(0)
            ]
        );
        let json = r#"{"tables": [{"name": "kv", "source": {"type": "static", "location": "s3://example-warehouse/db/kv"}, "ref": "main", "key_column": "key", "value_columns": [{"name": "detail", "fromat": "protobuf"}]}]}"#;
        assert_eq!(
            schema_fault_at(json),
            [
                key("tables"),
                At::Index(0),
                key("value_columns"),
                At::Index(0),
                key("fromat")
            ]
        );
        let number = with_value_columns("[7]");
        assert_eq!(
            schema_fault_at(&number),
            [
                key("tables"),
                At::Index(0),
                key("value_columns"),
                At::Index(0)
            ]
        );
    }

    #[test]
    fn test_from_str_value_format_reads_display_and_refuses_other_text() {
        for format in ValueFormat::ALL {
            assert_eq!(format.to_string().parse::<ValueFormat>(), Ok(format));
        }
        assert_eq!(
            "Protobuf".parse::<ValueFormat>(),
            Err(ConfigViolation::UnknownValueFormat {
                text: "Protobuf".to_string()
            })
        );
    }

    #[test]
    fn test_serialize_value_format_matches_display() {
        for format in ValueFormat::ALL {
            assert_eq!(
                serde_json::to_value(format).unwrap(),
                serde_json::Value::String(format.to_string())
            );
        }
    }

    #[test]
    fn test_eq_and_same_but_tuning_compare_columns_as_a_set_and_formats_as_tuning() {
        let entry =
            |columns: &str| Config::parse(&with_value_columns(columns)).unwrap().tables[0].clone();
        let opaque = entry(r#"["value", "detail"]"#);
        let reordered = entry(r#"["detail", "value"]"#);
        assert_eq!(opaque, reordered, "order carries no meaning");
        assert!(opaque.same_but_tuning(&reordered));
        assert_eq!(opaque.value_column_names(), ["detail", "value"]);
        assert_eq!(reordered.value_column_names(), ["detail", "value"]);
        let protobuf = entry(r#"[{ name = "detail", format = "protobuf" }, "value"]"#);
        assert_ne!(opaque, protobuf, "a format change is a change");
        assert!(opaque.same_but_tuning(&protobuf), "and is applied in place");
        assert!(protobuf.same_but_tuning(&opaque));
        assert_eq!(
            protobuf,
            entry(r#"["value", { name = "detail", format = "protobuf" }]"#),
            "whichever order the formats are listed in"
        );
        let retuned = TableConfig {
            lookups: Some(Level::Permissive),
            strict: !protobuf.strict,
            ..protobuf.clone()
        };
        assert!(opaque.same_but_tuning(&retuned), "with lookups and strict");
        let fewer = entry(r#"[{ name = "detail", format = "protobuf" }]"#);
        assert_ne!(opaque, fewer, "a column removed is a change");
        assert!(!opaque.same_but_tuning(&fewer), "that reloads");
        let renamed = entry(r#"["value", { name = "details", format = "protobuf" }]"#);
        assert!(
            !opaque.same_but_tuning(&renamed),
            "a column renamed reloads"
        );
        assert_ne!(entry(r#"["value"]"#), entry("[]"));
    }

    #[test]
    fn test_parse_value_columns_may_be_empty_and_refuse_an_empty_name_a_repeat_or_the_key() {
        let presence_only = Config::parse(&with_value_columns("[]")).unwrap();
        assert!(presence_only.tables[0].value_columns.is_empty());
        assert_eq!(
            violation(&with_value_columns(r#"["value", ""]"#)),
            ConfigViolation::EmptyValueColumn { index: 0 }
        );
        assert_eq!(
            violation(&with_value_columns(r#"["value", "detail", "value"]"#)),
            ConfigViolation::DuplicateValueColumn {
                index: 0,
                column: "value".to_string(),
            }
        );
        assert_eq!(
            violation(&with_value_columns(r#"["value", "key"]"#)),
            ConfigViolation::KeyColumnAsValue {
                index: 0,
                column: "key".to_string(),
            }
        );
    }

    /// A configuration of one table keyed by `key` serving `value`, in TOML
    /// and in JSON, the value listed as a bare name in one and as a table in
    /// the other.
    fn named_columns(key: &str, value: &str) -> [String; 2] {
        let toml = TOML_MINIMAL
            .replace(r#"key_column = "key""#, &format!("key_column = {key:?}"))
            .replace(
                r#"value_columns = ["value"]"#,
                &format!("value_columns = [{{ name = {value:?} }}]"),
            );
        let json = format!(
            r#"{{"tables": [{{"name": "kv", "source": {{"type": "static", "location": "s3://example-warehouse/db/kv"}}, "ref": "main", "key_column": {key:?}, "value_columns": [{value:?}]}}]}}"#
        );
        [toml, json]
    }

    #[test]
    fn test_parse_refuses_a_column_name_a_java_writer_rewrites_in_toml_and_json() {
        for text in named_columns("user_id", "_payload2") {
            let config = Config::parse(&text).unwrap();
            assert_eq!(config.tables[0].key_column, "user_id");
            assert_eq!(config.tables[0].value_column_names(), ["_payload2"]);
        }
        for name in ["user-id", "a.b", "1st", "día"] {
            for text in named_columns(name, "value") {
                assert_eq!(
                    violation(&text),
                    ConfigViolation::ColumnNameRewritten {
                        index: 0,
                        setting: ColumnSetting::KeyColumn,
                        column: name.to_string(),
                    },
                    "{text}"
                );
            }
            for text in named_columns("key", name) {
                assert_eq!(
                    violation(&text),
                    ConfigViolation::ColumnNameRewritten {
                        index: 0,
                        setting: ColumnSetting::ValueColumns,
                        column: name.to_string(),
                    },
                    "{text}"
                );
            }
        }
        for text in named_columns("key", "") {
            assert_eq!(
                violation(&text),
                ConfigViolation::EmptyValueColumn { index: 0 },
                "an empty value column keeps its own violation"
            );
        }
    }

    #[test]
    fn test_parse_refuses_incomplete_static_keys() {
        let id_only = format!("{TOML_MINIMAL}\n[storage.s3]\naccess_key_id = \"AKIAEXAMPLE\"\n");
        assert_eq!(violation(&id_only), ConfigViolation::IncompleteStaticKeys);
        let secret_only =
            format!("{TOML_MINIMAL}\n[storage.s3]\nsecret_access_key = \"secret-example\"\n");
        assert_eq!(
            violation(&secret_only),
            ConfigViolation::IncompleteStaticKeys
        );
    }

    #[test]
    fn test_parse_lookups_defaults_to_the_best_level_of_its_residency() {
        let with =
            |residency: &str| format!("{}\nresidency = \"{residency}\"\n", TOML_MINIMAL.trim_end());
        let local = format!(
            "\n[storage.local]\ndir = \"/var/lib/icetray\"\npage_cache_disk_bytes = {PAGE_CACHE_DISK_BLOCK_BYTES}\n"
        );
        for residency in Residency::ALL {
            let text = format!("{}{local}", with(residency.name()));
            let config = Config::parse(&text).unwrap();
            assert_eq!(config.tables[0].lookups, None);
            assert_eq!(
                config.tables[0].lookups(),
                residency.best_level(),
                "{residency}"
            );
        }
        let declared = format!("{}\nlookups = \"permissive\"\n", TOML_MINIMAL.trim_end());
        let config = Config::parse(&declared).unwrap();
        assert_eq!(config.tables[0].lookups(), Level::Permissive);
    }

    #[test]
    fn test_best_level_is_optimal_but_for_bloom() {
        assert_eq!(Residency::Full.best_level(), Level::Optimal);
        assert_eq!(Residency::Disk.best_level(), Level::Optimal);
        assert_eq!(Residency::Bloom.best_level(), Level::Balanced);
    }

    #[test]
    fn test_parse_refuses_lookups_its_residency_cannot_reach() {
        let text = format!(
            "{}\nresidency = \"bloom\"\nlookups = \"optimal\"\n",
            TOML_MINIMAL.trim_end()
        );
        assert_eq!(
            violation(&text),
            ConfigViolation::LookupsUnreachable {
                table: "kv".to_string(),
                index: Some(0),
                lookups: Level::Optimal,
                residency: Residency::Bloom,
            }
        );
        for lookups in [Level::Balanced, Level::Permissive] {
            let text = format!(
                "{}\nresidency = \"bloom\"\nlookups = \"{lookups}\"\n",
                TOML_MINIMAL.trim_end()
            );
            assert_eq!(Config::parse(&text).unwrap().tables[0].lookups(), lookups);
        }
    }

    #[test]
    fn test_eq_table_config_compares_the_lookups_it_resolves() {
        let config = Config::parse(TOML_MINIMAL).unwrap();
        let absent = config.tables[0].clone();
        let spelled_out = TableConfig {
            lookups: Some(absent.residency.best_level()),
            ..absent.clone()
        };
        assert_eq!(
            absent, spelled_out,
            "the default spelled out is the default"
        );
        let other = TableConfig {
            lookups: Some(Level::Permissive),
            ..absent.clone()
        };
        assert_ne!(absent, other);
        let flipped = TableConfig {
            strict: !absent.strict,
            ..absent.clone()
        };
        assert_ne!(absent, flipped);
    }

    #[test]
    fn test_same_but_tuning_is_true_for_lookups_and_strict_alone() {
        let config = Config::parse(TOML_MINIMAL).unwrap();
        let entry = config.tables[0].clone();
        let retuned = TableConfig {
            lookups: Some(Level::Permissive),
            strict: !entry.strict,
            ..entry.clone()
        };
        assert!(entry.same_but_tuning(&retuned));
        assert!(entry.same_but_tuning(&entry));
        let moved = TableConfig {
            r#ref: format!("{}-next", entry.r#ref),
            ..retuned.clone()
        };
        assert!(!entry.same_but_tuning(&moved));
        let budgeted = TableConfig {
            resident_budget_bytes: Some(1),
            ..retuned
        };
        assert!(!entry.same_but_tuning(&budgeted));
    }

    /// `TOML_MINIMAL` as an ordered table whose order column is `ts`, with
    /// `extra` lines appended to its table.
    fn ordered(extra: &str) -> String {
        format!(
            "{}\norder_column = \"ts\"\n{extra}\n",
            TOML_MINIMAL.trim_end()
        )
    }

    #[test]
    fn test_parse_reads_order_column_and_max_scan_rows_in_toml_and_json() {
        let defaulted = Config::parse(&ordered("")).unwrap().tables[0].clone();
        assert_eq!(defaulted.order_column.as_deref(), Some("ts"));
        assert_eq!(defaulted.max_scan_rows, None);
        assert_eq!(defaulted.scan_rows(), Some(DEFAULT_MAX_SCAN_ROWS));
        let rows = DEFAULT_MAX_SCAN_ROWS / 2;
        let set = Config::parse(&ordered(&format!("max_scan_rows = {rows}"))).unwrap();
        assert_eq!(set.tables[0].scan_rows(), Some(rows));
        let json = format!(
            r#"{{"tables": [{{"name": "kv", "source": {{"type": "static", "location": "s3://example-warehouse/db/kv"}}, "ref": "main", "key_column": "key", "value_columns": ["value"], "order_column": "ts", "max_scan_rows": {rows}}}]}}"#
        );
        assert_eq!(Config::parse(&json).unwrap(), set);
        let key_value = Config::parse(TOML_MINIMAL).unwrap().tables[0].clone();
        assert_eq!(key_value.order_column, None);
        assert_eq!(
            key_value.scan_rows(),
            None,
            "a key-value table is never scanned"
        );
    }

    #[test]
    fn test_parse_refuses_the_key_column_as_the_order_column() {
        assert_eq!(
            violation(&TOML_MINIMAL.replace(
                "value_columns = [\"value\"]",
                "value_columns = [\"value\"]\norder_column = \"key\""
            )),
            ConfigViolation::OrderColumnIsKey { index: 0 }
        );
    }

    #[test]
    fn test_parse_refuses_a_value_column_as_the_order_column() {
        assert_eq!(
            violation(&ordered("").replace(
                "value_columns = [\"value\"]",
                "value_columns = [\"value\", \"ts\"]"
            )),
            ConfigViolation::OrderColumnIsValue {
                index: 0,
                column: "ts".to_string(),
            }
        );
    }

    #[test]
    fn test_parse_refuses_an_order_column_name_a_java_writer_rewrites() {
        for name in ["", "user-id", "a.b", "1st"] {
            let text = TOML_MINIMAL.replace(
                "value_columns = [\"value\"]",
                &format!("value_columns = [\"value\"]\norder_column = {name:?}"),
            );
            assert_eq!(
                violation(&text),
                ConfigViolation::ColumnNameRewritten {
                    index: 0,
                    setting: ColumnSetting::OrderColumn,
                    column: name.to_string(),
                },
                "{text}"
            );
        }
    }

    #[test]
    fn test_parse_refuses_max_scan_rows_on_a_key_value_table() {
        let text = format!("{}\nmax_scan_rows = 10\n", TOML_MINIMAL.trim_end());
        assert_eq!(
            violation(&text),
            ConfigViolation::MaxScanRowsOnKeyValueTable { index: 0 }
        );
    }

    /// An ordered table that sets `row_cache` is refused whichever value it
    /// sets, since its Scans read pages (RFD 026).
    #[test]
    fn test_row_cache_on_an_ordered_table_is_refused() {
        for value in [true, false] {
            assert_eq!(
                violation(&ordered(&format!("row_cache = {value}"))),
                ConfigViolation::RowCacheOnOrderedTable { index: 0 },
                "row_cache = {value}"
            );
        }
        assert!(
            Config::parse(&ordered("")).is_ok(),
            "left unset it is valid"
        );
    }

    /// `row_cache` defaults to on for a key-value table, reads off where an
    /// entry sets it so, and is off for an ordered table; only tables with
    /// it on read rows, so a server whose one key-value table sets it off
    /// gives the row cache no part of the budget.
    #[test]
    fn test_parse_reads_row_cache_and_its_default() {
        let defaulted = Config::parse(TOML_MINIMAL).unwrap();
        assert_eq!(defaulted.tables[0].row_cache, None);
        assert!(defaulted.tables[0].row_cache());
        assert!(defaulted.reads_rows());
        let off =
            Config::parse(&format!("{}\nrow_cache = false\n", TOML_MINIMAL.trim_end())).unwrap();
        assert_eq!(off.tables[0].row_cache, Some(false));
        assert!(!off.tables[0].row_cache());
        assert!(!off.reads_rows());
        let ordered = Config::parse(&ordered("")).unwrap();
        assert!(!ordered.tables[0].row_cache());
        assert!(!ordered.reads_rows());
    }

    /// An entry that spells out `row_cache = true` equals one that leaves
    /// it out, and one that turns it off is a changed entry, which a reload
    /// loads anew rather than applying in place.
    #[test]
    fn test_a_changed_row_cache_is_a_changed_entry() {
        let entry = Config::parse(TOML_MINIMAL).unwrap().tables[0].clone();
        let spelled_out = TableConfig {
            row_cache: Some(true),
            ..entry.clone()
        };
        assert_eq!(entry, spelled_out);
        let off = TableConfig {
            row_cache: Some(false),
            ..entry.clone()
        };
        assert_ne!(entry, off);
        assert!(!entry.same_but_tuning(&off));
        assert!(!off.same_but_tuning(&entry));
    }

    /// The name of the second table `mixed` serves.
    const SECOND_TABLE: &str = "events";

    /// `TOML_MINIMAL`'s key-value table, which reads through the row cache,
    /// then a second key-value table named `SECOND_TABLE` with `second`
    /// among its keys, then `tail`, a section of its own.
    fn mixed(second: &str, tail: &str) -> String {
        format!(
            "{}\n\n[[tables]]\nname = \"{SECOND_TABLE}\"\nsource = {{ type = \"static\", location = \"s3://example-warehouse/db/events\" }}\nref = \"main\"\nkey_column = \"key\"\nvalue_columns = [\"value\"]\n{second}\n\n{tail}",
            TOML_MINIMAL.trim_end()
        )
    }

    /// The second table's key that makes it an ordered table.
    const ORDERED_KEY: &str = "order_column = \"ts\"";

    /// A `storage.local` with a page-cache disk level of `bytes`.
    fn local_with_disk_level(bytes: u64) -> String {
        format!("[storage.local]\ndir = \"/var/lib/icetray\"\npage_cache_disk_bytes = {bytes}\n")
    }

    /// The refusal of a mixed server whose second table reads pages alone.
    fn second_reads_pages_alone() -> ConfigViolation {
        ConfigViolation::MixedServerNeedsShare {
            pages_alone: vec![SECOND_TABLE.to_string()],
        }
    }

    /// A key-value table reading through the row cache beside an ordered
    /// table, with no `storage.local` and no `row_cache_share`, is refused,
    /// naming the ordered table.
    #[test]
    fn test_a_mixed_server_without_a_disk_level_is_refused() {
        assert_eq!(
            violation(&mixed(ORDERED_KEY, "")),
            second_reads_pages_alone()
        );
    }

    /// A local directory whose `page_cache_disk_bytes` is zero has no disk
    /// level, so the mixed server is refused as without one.
    #[test]
    fn test_a_mixed_server_whose_disk_level_is_zero_is_refused() {
        assert_eq!(
            violation(&mixed(ORDERED_KEY, &local_with_disk_level(0))),
            second_reads_pages_alone()
        );
    }

    /// A key-value table with `row_cache = false` reads pages alone as an
    /// ordered table does, so beside one that reads rows it is refused.
    #[test]
    fn test_a_mixed_server_with_a_row_cache_false_table_is_refused() {
        assert_eq!(
            violation(&mixed("row_cache = false", "")),
            second_reads_pages_alone()
        );
    }

    #[test]
    fn test_a_mixed_server_setting_row_cache_share_is_accepted() {
        let share = format!("[server]\nrow_cache_share = {FIXTURE_ROW_CACHE_SHARE}\n");
        for second in [ORDERED_KEY, "row_cache = false"] {
            let config = Config::parse(&mixed(second, &share)).unwrap();
            assert_eq!(config.tables.len(), 2, "{second}");
        }
    }

    #[test]
    fn test_a_mixed_server_with_a_disk_level_is_accepted() {
        let disk = local_with_disk_level(PAGE_CACHE_DISK_BLOCK_BYTES);
        for second in [ORDERED_KEY, "row_cache = false"] {
            let config = Config::parse(&mixed(second, &disk)).unwrap();
            assert_eq!(config.tables.len(), 2, "{second}");
        }
    }

    /// A server whose tables all read through the row cache, or all read
    /// pages alone, has one kind of table for the split to suit.
    #[test]
    fn test_a_server_of_one_kind_of_table_needs_no_share() {
        for second in ["", "row_cache = true"] {
            Config::parse(&mixed(second, "")).unwrap();
        }
        let pages_alone = mixed(ORDERED_KEY, "").replacen(
            "value_columns = [\"value\"]\n",
            "value_columns = [\"value\"]\nrow_cache = false\n",
            1,
        );
        let config = Config::parse(&pages_alone).unwrap();
        assert!(!config.reads_rows());
    }

    #[test]
    fn test_parse_refuses_max_scan_rows_zero() {
        assert_eq!(
            violation(&ordered("max_scan_rows = 0")),
            ConfigViolation::MaxScanRowsZero {
                table: "kv".to_string(),
                index: Some(0),
            }
        );
    }

    #[test]
    fn test_parse_refuses_max_multi_scan_values_below_twice_max_scan_rows() {
        let with_values = |values: usize, extra: &str| {
            format!(
                "[server]\nmax_multi_scan_values = {values}\n{}",
                ordered(extra)
            )
        };
        let defaulted = approx::length(DEFAULT_MAX_SCAN_ROWS);
        let exactly_twice = MULTI_SCAN_VALUES_PER_SCAN_ROW * defaulted;
        assert!(Config::parse(&with_values(exactly_twice, "")).is_ok());
        assert_eq!(
            violation(&with_values(exactly_twice - 1, "")),
            ConfigViolation::MaxMultiScanValuesBelowScanRows {
                index: 0,
                max_scan_rows: DEFAULT_MAX_SCAN_ROWS,
                max_multi_scan_values: exactly_twice - 1,
            },
            "against the default max_scan_rows"
        );
        let rows =
            u32::try_from(DEFAULT_MAX_MULTI_SCAN_VALUES / MULTI_SCAN_VALUES_PER_SCAN_ROW + 1)
                .unwrap();
        assert_eq!(
            violation(&format!("{}max_scan_rows = {rows}\n", ordered(""))),
            ConfigViolation::MaxMultiScanValuesBelowScanRows {
                index: 0,
                max_scan_rows: rows,
                max_multi_scan_values: DEFAULT_MAX_MULTI_SCAN_VALUES,
            },
            "against the default max_multi_scan_values"
        );
        assert!(
            Config::parse(&with_values(1, "").replace("order_column = \"ts\"\n", "")).is_ok(),
            "a key-value table holds the limit to nothing"
        );
    }

    #[test]
    fn test_same_but_tuning_admits_a_max_scan_rows_change() {
        let entry = Config::parse(&ordered("")).unwrap().tables[0].clone();
        let spelled_out = TableConfig {
            max_scan_rows: Some(DEFAULT_MAX_SCAN_ROWS),
            ..entry.clone()
        };
        assert_eq!(entry, spelled_out, "the default spelled out is the default");
        let retuned = TableConfig {
            max_scan_rows: Some(DEFAULT_MAX_SCAN_ROWS / 2),
            ..entry.clone()
        };
        assert_ne!(entry, retuned, "a limit change is a change");
        assert!(entry.same_but_tuning(&retuned), "and is applied in place");
        let reordered = TableConfig {
            order_column: Some("seq".to_string()),
            ..entry.clone()
        };
        assert!(
            !entry.same_but_tuning(&reordered),
            "another order column reloads"
        );
        let key_value = TableConfig {
            order_column: None,
            max_scan_rows: None,
            ..entry.clone()
        };
        assert!(!entry.same_but_tuning(&key_value), "so does removing it");
        assert!(!key_value.same_but_tuning(&entry), "and setting it");
    }

    #[test]
    fn test_parse_refuses_budget_without_residency() {
        let bloom = format!(
            "{}\nresidency = \"bloom\"\nresident_budget_bytes = 1\n",
            TOML_MINIMAL.trim_end()
        );
        assert_eq!(
            violation(&bloom),
            ConfigViolation::BudgetWithoutResidency {
                index: 0,
                bytes: 1,
                residency: Residency::Bloom,
            }
        );
        // Disk has its local directory, so the budget is the only fault.
        let disk = format!(
            "{}\nresidency = \"disk\"\nresident_budget_bytes = 1\n\n[storage.local]\ndir = \"/var/lib/icetray\"\npage_cache_disk_bytes = {PAGE_CACHE_DISK_BLOCK_BYTES}\n",
            TOML_MINIMAL.trim_end()
        );
        assert_eq!(
            violation(&disk),
            ConfigViolation::BudgetWithoutResidency {
                index: 0,
                bytes: 1,
                residency: Residency::Disk,
            }
        );
    }

    /// A set `cache_bytes` whose page part, the whole of it at a row
    /// share of zero, is held to one shard's minimum; a `cache_bytes` left
    /// to the memory limit is checked only when the caches open.
    #[test]
    fn test_parse_refuses_a_page_cache_below_a_value_page_per_shard() {
        let minimum = min_memory_bytes(PARSE_CACHE_SHARDS);
        let with_budget = |bytes: u64| {
            format!(
                "[server]\ncache_bytes = {bytes}\nrow_cache_share = 0.0\n{}",
                TOML_MINIMAL.trim_start()
            )
        };
        for bytes in [0, VALUE_PAGE_BYTES, minimum - 1] {
            assert_eq!(
                violation(&with_budget(bytes)),
                ConfigViolation::PageCacheTooSmall {
                    bytes,
                    minimum,
                    shards: PARSE_CACHE_SHARDS,
                    limit: None,
                },
                "{bytes} bytes"
            );
        }
        Config::parse(&with_budget(minimum)).unwrap();
        let rows_take_most = format!(
            "[server]\ncache_bytes = {minimum}\n{}",
            TOML_MINIMAL.trim_start()
        );
        assert!(
            matches!(
                violation(&rows_take_most),
                ConfigViolation::PageCacheTooSmall { bytes, .. } if bytes < minimum
            ),
            "the row cache's share leaves the page cache under the minimum"
        );
    }

    #[test]
    fn test_parse_refuses_page_cache_disk_below_one_block() {
        let with_disk_level = |bytes: u64| {
            format!(
                "{}\n\n[storage.local]\ndir = \"/var/lib/icetray\"\npage_cache_disk_bytes = {bytes}\n",
                TOML_MINIMAL.trim_end()
            )
        };
        let just_below = PAGE_CACHE_DISK_BLOCK_BYTES - 1;
        assert_eq!(
            violation(&with_disk_level(just_below)),
            ConfigViolation::PageCacheDiskTooSmall {
                bytes: just_below,
                minimum: PAGE_CACHE_DISK_BLOCK_BYTES,
            }
        );
        assert_eq!(
            violation(&with_disk_level(1)),
            ConfigViolation::PageCacheDiskTooSmall {
                bytes: 1,
                minimum: PAGE_CACHE_DISK_BLOCK_BYTES,
            }
        );
        let one_block = Config::parse(&with_disk_level(PAGE_CACHE_DISK_BLOCK_BYTES)).unwrap();
        assert_eq!(
            one_block.storage.local.unwrap().page_cache_disk_bytes,
            PAGE_CACHE_DISK_BLOCK_BYTES
        );
        let no_level = Config::parse(&with_disk_level(0)).unwrap();
        assert_eq!(no_level.storage.local.unwrap().page_cache_disk_bytes, 0);
    }

    /// A static location is checked after the file's rewrite rules: one no
    /// built store serves is refused at its setting with the location as
    /// rewritten, and a rule that moves it to a store makes it pass.
    #[test]
    fn test_parse_reads_aws_sources_with_their_optional_keys_absent() {
        let glue = Config::parse(&glue_source("")).unwrap();
        assert_eq!(
            glue.tables[0].source,
            SourceConfig::Glue {
                database: "db".to_string(),
                table: "kv".to_string(),
                catalog_id: None,
                region: None,
                endpoint: None,
            }
        );
        let s3tables = Config::parse(&s3tables_source(FIXTURE_TABLE_BUCKET_ARN, "")).unwrap();
        assert_eq!(
            s3tables.tables[0].source,
            SourceConfig::S3Tables {
                table_bucket_arn: FIXTURE_TABLE_BUCKET_ARN.to_string(),
                namespace: "db".to_string(),
                table: "kv".to_string(),
                region: None,
                endpoint: None,
            }
        );
    }

    /// A required key missing, a key the kind does not take, and a key of
    /// another kind are all schema faults at the source; a kind no source
    /// has is one at its `type`.
    #[test]
    fn test_parse_refuses_aws_sources_missing_or_unknown_keys() {
        let at_source = [key("tables"), At::Index(0), key("source")];
        for text in [
            minimal_with_source(r#"{ type = "glue", table = "kv" }"#),
            minimal_with_source(r#"{ type = "glue", database = "db" }"#),
            glue_source(r#", catalog = "111122223333""#),
            glue_source(r#", namespace = "db""#),
            minimal_with_source(&format!(
                r#"{{ type = "s3tables", table_bucket_arn = "{FIXTURE_TABLE_BUCKET_ARN}", table = "kv" }}"#
            )),
            minimal_with_source(r#"{ type = "s3tables", namespace = "db", table = "kv" }"#),
            s3tables_source(FIXTURE_TABLE_BUCKET_ARN, r#", catalog_id = "111122223333""#),
            s3tables_source(FIXTURE_TABLE_BUCKET_ARN, r#", database = "db""#),
        ] {
            assert_eq!(schema_fault_at(&text), at_source, "{text}");
        }
        let misspelled =
            minimal_with_source(r#"{ type = "s3_tables", namespace = "db", table = "kv" }"#);
        assert_eq!(
            schema_fault_at(&misspelled),
            [key("tables"), At::Index(0), key("source"), key("type")]
        );
    }

    #[test]
    fn test_parse_refuses_an_empty_aws_source_setting_by_its_path() {
        let empty = |setting| ConfigViolation::EmptySourceSetting { index: 0, setting };
        let cases = [
            (
                minimal_with_source(r#"{ type = "glue", database = "", table = "kv" }"#),
                SourceSetting::Database,
            ),
            (
                minimal_with_source(r#"{ type = "glue", database = "db", table = "" }"#),
                SourceSetting::Table,
            ),
            (
                glue_source(r#", catalog_id = """#),
                SourceSetting::CatalogId,
            ),
            (glue_source(r#", region = """#), SourceSetting::Region),
            (glue_source(r#", endpoint = """#), SourceSetting::Endpoint),
            (s3tables_source("", ""), SourceSetting::TableBucketArn),
            (
                minimal_with_source(&format!(
                    r#"{{ type = "s3tables", table_bucket_arn = "{FIXTURE_TABLE_BUCKET_ARN}", namespace = "", table = "kv" }}"#
                )),
                SourceSetting::Namespace,
            ),
            (
                s3tables_source(FIXTURE_TABLE_BUCKET_ARN, r#", region = """#),
                SourceSetting::Region,
            ),
        ];
        for (text, setting) in cases {
            assert_eq!(violation(&text), empty(setting), "{text}");
        }
    }

    #[test]
    fn test_parse_refuses_an_s3tables_source_whose_arn_is_not_a_table_bucket_s() {
        for arn in [
            "warehouse",
            "arn:aws:s3:::warehouse",
            "arn:aws:s3tables:us-east-1:111122223333:table/warehouse",
            "arn:aws:s3tables:us-east-1:1111:bucket/warehouse",
        ] {
            assert_eq!(
                violation(&s3tables_source(arn, "")),
                ConfigViolation::TableBucketArn {
                    index: 0,
                    arn: arn.to_string(),
                },
                "{arn}"
            );
        }
    }

    #[test]
    fn test_parse_refuses_an_aws_source_endpoint_that_is_not_http() {
        for endpoint in ["glue.example", "s3://glue.example", "ftp://glue.example"] {
            let expected = ConfigViolation::SourceEndpoint {
                index: 0,
                endpoint: endpoint.to_string(),
            };
            let extra = format!(r#", endpoint = "{endpoint}""#);
            assert_eq!(violation(&glue_source(&extra)), expected, "{endpoint}");
            assert_eq!(
                violation(&s3tables_source(FIXTURE_TABLE_BUCKET_ARN, &extra)),
                expected,
                "{endpoint}"
            );
        }
        for endpoint in ["http://127.0.0.1:4566", "https://glue.example"] {
            let extra = format!(r#", endpoint = "{endpoint}""#);
            assert!(Config::parse(&glue_source(&extra)).is_ok(), "{endpoint}");
        }
    }

    #[test]
    fn test_parse_refuses_a_static_location_no_built_store_serves() {
        let at = |location: &str| TOML_MINIMAL.replace("s3://example-warehouse/db/kv", location);
        let refused = |location: &str, fault| ConfigViolation::TableLocation {
            index: 0,
            source: LocationError {
                location: location.to_string(),
                fault,
            },
        };
        assert_eq!(
            violation(&at("gs://b/kv")),
            refused("gs://b/kv", LocationFault::NoStore)
        );
        assert_eq!(
            violation(&at("db/kv")),
            refused("db/kv", LocationFault::NotAbsolute)
        );
        assert_eq!(
            violation(&at("")),
            ConfigViolation::EmptySourceSetting {
                index: 0,
                setting: SourceSetting::Location,
            }
        );
        let rule = |to: &str| format!("\n[[storage.rewrite]]\nfrom = \"gs://b/\"\nto = \"{to}\"\n");
        let to_s3 = format!("{}{}", at("gs://b/kv"), rule("s3://b/"));
        assert!(Config::parse(&to_s3).is_ok());
        let to_nothing = format!("{}{}", at("gs://b/kv"), rule("mem://x/"));
        assert_eq!(
            violation(&to_nothing),
            refused("mem://x/kv", LocationFault::NoStore)
        );
    }

    #[test]
    fn test_parse_refuses_disk_residency_without_local_dir() {
        let text = format!("{}\nresidency = \"disk\"\n", TOML_MINIMAL.trim_end());
        assert_eq!(
            violation(&text),
            ConfigViolation::DiskResidencyWithoutLocalDir {
                table: "kv".to_string(),
                index: Some(0),
            }
        );
    }

    /// The smallest step a friendly duration can take, so a bound plus or
    /// minus it is the first interval past the bound.
    const SMALLEST_STEP: Duration = Duration::from_nanos(1);

    /// The violation `validated` finds in the minimal configuration with
    /// `poll_interval` set, or `None` when it accepts it.
    fn poll_interval_violation(interval: Duration) -> Option<ConfigViolation> {
        let mut config = Config::parse(TOML_MINIMAL).unwrap();
        config.server.poll_interval = interval;
        match config.validated() {
            Ok(_) => None,
            Err(Error::Config(ConfigError::Invalid(violation))) => Some(violation),
            Err(other) => panic!("expected a configuration violation, got {other:?}"),
        }
    }

    #[test]
    fn test_parse_refuses_poll_interval_zero() {
        let text = format!("{TOML_MINIMAL}\n[server]\npoll_interval = \"0s\"\n");
        assert_eq!(
            violation(&text),
            ConfigViolation::PollIntervalTooShort {
                interval: Duration::ZERO,
                minimum: MIN_POLL_INTERVAL,
            }
        );
    }

    #[test]
    fn test_validated_holds_poll_interval_between_its_bounds() {
        let below = MIN_POLL_INTERVAL.checked_sub(SMALLEST_STEP).unwrap();
        assert_eq!(
            poll_interval_violation(below),
            Some(ConfigViolation::PollIntervalTooShort {
                interval: below,
                minimum: MIN_POLL_INTERVAL,
            })
        );
        assert_eq!(poll_interval_violation(MIN_POLL_INTERVAL), None);
        assert_eq!(poll_interval_violation(MAX_POLL_INTERVAL), None);
        let above = MAX_POLL_INTERVAL + SMALLEST_STEP;
        assert_eq!(
            poll_interval_violation(above),
            Some(ConfigViolation::PollIntervalTooLong {
                interval: above,
                maximum: MAX_POLL_INTERVAL,
            })
        );
        assert_eq!(
            poll_interval_violation(Duration::MAX),
            Some(ConfigViolation::PollIntervalTooLong {
                interval: Duration::MAX,
                maximum: MAX_POLL_INTERVAL,
            })
        );
    }

    #[test]
    fn test_parse_refuses_a_zero_bound() {
        let cases = [
            ("server", "get_deadline", ConfigViolation::GetDeadlineZero),
            (
                "server",
                "multi_get_deadline",
                ConfigViolation::MultiGetDeadlineZero,
            ),
            ("server", "scan_deadline", ConfigViolation::ScanDeadlineZero),
            (
                "server",
                "multi_scan_deadline",
                ConfigViolation::MultiScanDeadlineZero,
            ),
            ("server", "load_deadline", ConfigViolation::LoadDeadlineZero),
            (
                "server",
                "refresh_deadline",
                ConfigViolation::RefreshDeadlineZero,
            ),
            (
                "server",
                "drain_deadline",
                ConfigViolation::DrainDeadlineZero,
            ),
            (
                "storage",
                "connect_timeout",
                ConfigViolation::ConnectTimeoutZero,
            ),
            (
                "storage",
                "catalog_timeout",
                ConfigViolation::CatalogTimeoutZero,
            ),
        ];
        for (section, key, expected) in cases {
            let text = format!("{TOML_MINIMAL}\n[{section}]\n{key} = \"0s\"\n");
            assert_eq!(violation(&text), expected, "{section}.{key}");
        }
        let limits = [
            ("max_in_flight", ConfigViolation::MaxInFlightZero),
            (
                "max_multi_get_values",
                ConfigViolation::MaxMultiGetValuesZero,
            ),
            ("batch_fan_out", ConfigViolation::BatchFanOutZero),
            (
                "max_multi_scan_values",
                ConfigViolation::MaxMultiScanValuesZero,
            ),
            ("max_scan_bytes", ConfigViolation::MaxScanBytesZero),
        ];
        for (key, expected) in limits {
            let text = format!("{TOML_MINIMAL}\n[server]\n{key} = 0\n");
            assert_eq!(violation(&text), expected, "server.{key}");
        }
    }

    /// `server.batch_fan_out` as TOML and as JSON names it.
    fn with_fan_out(fan_out: usize) -> [String; 2] {
        let toml = format!("{TOML_MINIMAL}\n[server]\nbatch_fan_out = {fan_out}\n");
        let json = format!(
            r#"{{"server": {{"batch_fan_out": {fan_out}}}, "tables": [{{"name": "kv", "source": {{"type": "static", "location": "s3://example-warehouse/db/kv"}}, "ref": "main", "key_column": "key", "value_columns": ["value"]}}]}}"#
        );
        [toml, json]
    }

    #[test]
    fn test_parse_reads_batch_fan_out_and_refuses_zero_in_toml_and_json() {
        let fan_out = 4;
        for text in with_fan_out(fan_out) {
            let config = Config::parse(&text).unwrap();
            assert_eq!(config.server.batch_fan_out, fan_out, "{text}");
        }
        for text in with_fan_out(0) {
            assert_eq!(violation(&text), ConfigViolation::BatchFanOutZero, "{text}");
        }
    }

    #[test]
    fn test_parse_reads_the_scan_settings_and_their_defaults() {
        let config = Config::parse(TOML_MINIMAL).unwrap();
        assert_eq!(config.server.batch_fan_out, DEFAULT_BATCH_FAN_OUT);
        assert_eq!(
            config.server.max_multi_scan_values,
            DEFAULT_MAX_MULTI_SCAN_VALUES
        );
        assert_eq!(config.server.scan_deadline, DEFAULT_SCAN_DEADLINE);
        assert_eq!(
            config.server.multi_scan_deadline,
            DEFAULT_MULTI_SCAN_DEADLINE
        );
        assert_eq!(config.server.max_scan_bytes, DEFAULT_MAX_SCAN_BYTES);
    }

    /// The old name of `server.batch_fan_out` is unknown, so a file that
    /// still uses it fails the parse as a schema fault at that key instead
    /// of serving the default.
    #[test]
    fn test_parse_refuses_multi_get_fan_out_as_unknown() {
        let toml = format!("{TOML_MINIMAL}\n[server]\nmulti_get_fan_out = 8\n");
        let json = r#"{"server": {"multi_get_fan_out": 8}, "tables": [{"name": "kv", "source": {"type": "static", "location": "s3://example-warehouse/db/kv"}, "ref": "main", "key_column": "key", "value_columns": ["value"]}]}"#;
        for text in [toml.as_str(), json] {
            assert_eq!(
                schema_fault_at(text),
                [key("server"), key("multi_get_fan_out")],
                "{text}"
            );
        }
    }

    #[test]
    fn test_parse_accepts_zero_retries() {
        let text = format!("{TOML_MINIMAL}\n[storage]\nmax_retries = 0\n");
        assert_eq!(Config::parse(&text).unwrap().storage.max_retries, 0);
    }

    #[test]
    fn test_parse_refuses_a_rewrite_rule_with_an_empty_prefix() {
        let rule = |from: &str, to: &str| {
            format!("\n[[storage.rewrite]]\nfrom = \"{from}\"\nto = \"{to}\"\n")
        };
        let usable = rule("s3://old/", "s3://new/");
        let cases = [
            (rule("", "s3://new/"), RewriteFault::EmptyFrom),
            (rule("s3://old/", ""), RewriteFault::EmptyTo),
            (rule("", ""), RewriteFault::EmptyFrom),
        ];
        for (unusable, fault) in cases {
            let text = format!("{TOML_MINIMAL}{usable}{unusable}");
            assert_eq!(
                violation(&text),
                ConfigViolation::Rewrite { index: 1, fault },
                "{unusable}"
            );
        }
        assert!(Config::parse(&format!("{TOML_MINIMAL}{usable}")).is_ok());
    }

    #[test]
    fn test_from_str_rewrite_rule_splits_at_the_first_separator_and_displays_back() {
        let text = "s3://a/=s3://b/?x=1";
        let rule: RewriteRule = text.parse().unwrap();
        assert_eq!(
            rule,
            RewriteRule {
                from: "s3://a/".to_string(),
                to: "s3://b/?x=1".to_string(),
            }
        );
        assert_eq!(rule.to_string(), text);
    }

    #[test]
    fn test_from_str_rewrite_rule_refuses_a_rule_the_file_would_refuse() {
        let cases = [
            ("no-separator", RewriteTextFault::NoSeparator),
            ("=s3://b/", RewriteTextFault::Rule(RewriteFault::EmptyFrom)),
            ("s3://a/=", RewriteTextFault::Rule(RewriteFault::EmptyTo)),
            ("=", RewriteTextFault::Rule(RewriteFault::EmptyFrom)),
        ];
        for (text, fault) in cases {
            assert_eq!(text.parse::<RewriteRule>(), Err(fault), "{text:?}");
        }
    }

    /// The text fault's message already names the empty half, so the fault
    /// it carries is not also its source, which a chain would print again.
    #[test]
    fn test_source_rewrite_text_fault_carries_no_source() {
        for fault in [RewriteFault::EmptyFrom, RewriteFault::EmptyTo] {
            let text_fault = RewriteTextFault::from(fault);
            assert_eq!(text_fault, RewriteTextFault::Rule(fault));
            assert!(
                std::error::Error::source(&text_fault).is_none(),
                "{fault:?}"
            );
        }
    }

    #[test]
    fn test_parse_refuses_hedge_percentile_out_of_range() {
        let text = format!("{TOML_MINIMAL}\n[storage.hedge]\npercentile = {PERCENTILE_TOO_HIGH}\n");
        assert_eq!(
            violation(&text),
            ConfigViolation::HedgePercentileOutOfRange {
                percentile: PERCENTILE_TOO_HIGH
            }
        );
        let zero = format!("{TOML_MINIMAL}\n[storage.hedge]\npercentile = 0.0\n");
        assert_eq!(
            violation(&zero),
            ConfigViolation::HedgePercentileOutOfRange { percentile: 0.0 }
        );
    }

    #[test]
    fn test_parse_refuses_hedge_max_rate_out_of_range() {
        let negative = format!("{TOML_MINIMAL}\n[storage.hedge]\nmax_rate = {MAX_RATE_NEGATIVE}\n");
        assert_eq!(
            violation(&negative),
            ConfigViolation::HedgeMaxRateOutOfRange {
                max_rate: MAX_RATE_NEGATIVE
            }
        );
        let too_high = format!("{TOML_MINIMAL}\n[storage.hedge]\nmax_rate = {MAX_RATE_TOO_HIGH}\n");
        assert_eq!(
            violation(&too_high),
            ConfigViolation::HedgeMaxRateOutOfRange {
                max_rate: MAX_RATE_TOO_HIGH
            }
        );
    }

    #[test]
    fn test_parse_refuses_hedge_window_empty() {
        let text = format!("{TOML_MINIMAL}\n[storage.hedge]\nwindow = 0\n");
        assert_eq!(violation(&text), ConfigViolation::HedgeWindowEmpty);
    }

    #[test]
    fn test_parse_accepts_hedge_range_ends() {
        let top = format!(
            "{TOML_MINIMAL}\n[storage.hedge]\npercentile = {HEDGE_RANGE_TOP:?}\nmax_rate = {HEDGE_RANGE_TOP:?}\nwindow = 1\n"
        );
        assert_eq!(
            Config::parse(&top).unwrap().storage.hedge,
            HedgeConfig {
                enabled: DEFAULT_HEDGE_ENABLED,
                percentile: HEDGE_RANGE_TOP,
                window: 1,
                max_rate: HEDGE_RANGE_TOP,
            }
        );
        let bottom = format!("{TOML_MINIMAL}\n[storage.hedge]\nmax_rate = {MAX_RATE_BOTTOM:?}\n");
        assert_eq!(
            Config::parse(&bottom).unwrap().storage.hedge,
            HedgeConfig {
                max_rate: MAX_RATE_BOTTOM,
                ..HedgeConfig::default()
            }
        );
    }

    #[test]
    fn test_fmt_s3_config_debug_hides_secret() {
        let s3 = full_config().storage.s3.unwrap();
        let secret = s3.secret_access_key.clone().unwrap();
        let rendered = format!("{s3:?}");
        assert!(!rendered.contains(&secret));
        assert!(rendered.contains(SECRET_PLACEHOLDER));
        let without = S3Config::default();
        assert!(!format!("{without:?}").contains(SECRET_PLACEHOLDER));
    }

    #[test]
    fn test_fmt_residency_lowercase_names() {
        let cases = [
            (Residency::Full, "full"),
            (Residency::Disk, "disk"),
            (Residency::Bloom, "bloom"),
        ];
        for (residency, name) in cases {
            assert_eq!(residency.to_string(), name);
        }
    }

    #[test]
    fn test_serialize_residency_matches_display() {
        for residency in Residency::ALL {
            let json = serde_json::to_string(&residency).unwrap();
            assert_eq!(json, format!("\"{residency}\""));
            let back: Residency = serde_json::from_str(&json).unwrap();
            assert_eq!(back, residency);
        }
    }

    #[test]
    fn test_serialize_standard_field_matches_display() {
        for field in StandardField::ALL {
            let json = serde_json::to_string(&field).unwrap();
            assert_eq!(json, format!("\"{field}\""));
            let back: StandardField = serde_json::from_str(&json).unwrap();
            assert_eq!(back, field);
        }
    }

    #[test]
    fn test_from_str_residency_round_trips_display() {
        for residency in Residency::ALL {
            let parsed: Residency = residency.to_string().parse().unwrap();
            assert_eq!(parsed, residency);
        }
    }

    #[test]
    fn test_from_str_residency_refuses_another_word() {
        for text in ["memory", "Full", "FULL", "", " full"] {
            assert_eq!(
                text.parse::<Residency>(),
                Err(ConfigViolation::UnknownResidency {
                    text: text.to_string(),
                }),
                "{text:?}"
            );
        }
    }
}
