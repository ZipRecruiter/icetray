//! The table lifecycle (RFD 005, RFD 007, RFD 020): a `Table` follows one
//! ref, builds each new snapshot as a `Generation` beside the one it
//! serves, runs the doctor on the new generation and refuses one below the
//! table's level under `strict`, then swaps it in atomically. Loads and
//! refreshes of one table are serialized; a failed one leaves the last
//! generation serving and the table `Degraded`, or `Held` when the snapshot
//! was refused for its level. A lookup takes one generation and answers
//! from it alone, so a swap during a batch cannot mix snapshots. A `disk`
//! table's level also follows how much of its chunk files the kernel's
//! page cache holds, a condition of the server that `strict` never acts
//! on.

use std::collections::{BTreeMap, HashMap};
use std::fmt;
use std::future::Future;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, Instant};

use arc_swap::{ArcSwap, ArcSwapOption};
use futures::{StreamExt as _, TryStreamExt as _, stream};
use iceberg::spec::Snapshot;
use metrics::{Counter, Histogram};
use tokio::sync::{MutexGuard, Semaphore};
use tokio::task::{self, AbortHandle, JoinSet};

use crate::approx;
use crate::blocking::on_blocking_pool;
use crate::cache::PageCache;
use crate::catalog::{Resolved, TableSource};
use crate::check::{self, Capping, ColumnCost, ColumnLevel, Finding, FindingCode, LookupCost};
use crate::config::{Residency, TableConfig};
use crate::deletes::DeleteSummary;
use crate::error::{Error, ErrorKind, Recurrence, Recurs as _, RefusalReason, Result};
use crate::generation::{Generation, Lookup, MultiLookup, Row};
use crate::index::RunSource;
use crate::key::{Key, KeyType};
use crate::level::{Level, PageCacheCondition};
use crate::local::{LocalDir, ResidentPages, Unobserved};
use crate::manifest::{CheckedReads, ColumnRead, Selection, TableLayout, check_column_reads};
use crate::metric::{self, LookupResult, ScanResult, Temperature};
use crate::order::{OrderValue, ScanBounds};
use crate::projection;
use crate::row_cache::RowCache;
use crate::scan::{self, MultiScanLookup, Oversize, ScanLookup, ScanRequestFault, Scanned};
use crate::shard::{self, Line, Sharded};
use crate::snapshot::{SequenceNumber, SnapshotId};
use crate::storage::{HedgeWindow, Storage};
use crate::time;

/// The keys per run of a split `multi_get`: 8, one run for each 8 keys
/// or part of 8, up to one per worker (`Lookups::runs`). A warm hit's
/// decode is on the order of ten microseconds, so eight keys hold about a
/// hundred microseconds of work, far more than spawning a run costs, and a
/// batch of a hundred keys still spreads over a dozen workers.
const MULTI_GET_TASK_KEYS_MIN: usize = 8;

/// The distinct keys per run of a split `multi_scan`: 32, one run for
/// each 32 distinct keys or part of 32, up to one per worker
/// (`Lookups::runs`), so a call of 32 distinct keys or fewer stays on its
/// task. A warm Scan of one key costs about what a warm hit does, and a
/// spread `multi_scan` pays for its spawns, the wakeups across workers,
/// and workers spinning before they park on every run. Measured over warm
/// Scans at limit 1, splitting so cut a lone call's latency to about a
/// third at 100 keys and a fifth at 1,000, for 10% and 70% more CPU per
/// key, spent only while workers would otherwise sit idle.
const MULTI_SCAN_TASK_KEYS_MIN: usize = 32;

/// Where a table is in its lifecycle. Display gives the `state` metric
/// label: the variant's name alone, `loading`, `serving`, `degraded`, or
/// `held`.
#[derive(Clone, Debug)]
pub enum TableState {
    /// Constructed and not yet loaded; nothing has been read.
    Loading,
    /// A generation answers lookups and the last load or refresh succeeded.
    Serving {
        /// The snapshot it serves.
        snapshot_id: SnapshotId,
        /// When the state began: the swap, or the poll that ended a
        /// `Degraded` or `Held` state by finding the table in step with its
        /// ref.
        since: Instant,
    },
    /// The last load or refresh failed. The generation loaded before it, if
    /// any, keeps answering (RFD 005). The state ends at the next swap, or
    /// at the next poll that finds the table in step with its ref.
    Degraded {
        /// The snapshot still serving, or `None` after a failed first load.
        serving: Option<SnapshotId>,
        /// Why the load or refresh failed, as the failure's kind: a refusal
        /// with its reason, a table or file fault, or the
        /// library or local-directory failure's variant. The failure itself
        /// went to the caller of `load` or `refresh`, which logs it; `Error`
        /// is not `Clone`, and the kind is what a reader of the state acts on.
        reason: ErrorKind,
        /// When the failure happened.
        since: Instant,
    },
    /// The newest snapshot was refused under `strict` for its level
    /// (`RefusalReason::BelowLevel`, RFD 020), and the last snapshot that
    /// met the level keeps serving. Not a fault: a writer's layout put the
    /// table there, and a poll that finds the source moved builds again. A
    /// transient failure of a later poll makes the table `Degraded` until a
    /// poll finds the source unchanged, which makes it `Held` again; the
    /// state ends at the next swap, or at a poll that finds the ref back on
    /// the serving snapshot.
    Held {
        /// The snapshot still serving.
        serving: SnapshotId,
        /// The snapshot refused.
        refused: SnapshotId,
        /// The level the refused snapshot's layout achieves.
        level: Level,
        /// When the refusal was made.
        since: Instant,
    },
}

impl TableState {
    /// The state's name, for the `state` label.
    fn name(&self) -> StateName {
        match self {
            Self::Loading => StateName::Loading,
            Self::Serving { .. } => StateName::Serving,
            Self::Degraded { .. } => StateName::Degraded,
            Self::Held { .. } => StateName::Held,
        }
    }
}

impl fmt::Display for TableState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.name().fmt(f)
    }
}

/// A `TableState`'s name without its fields: the `state` label's values,
/// so the `table_state` gauge is set to zero for the states a table is not
/// in. Display gives the label value.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum StateName {
    /// `TableState::Loading`.
    Loading,
    /// `TableState::Serving`.
    Serving,
    /// `TableState::Degraded`.
    Degraded,
    /// `TableState::Held`.
    Held,
}

impl StateName {
    /// Every state, for the gauge's series.
    const ALL: [Self; 4] = [Self::Loading, Self::Serving, Self::Degraded, Self::Held];
}

impl fmt::Display for StateName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let name = match self {
            Self::Loading => "loading",
            Self::Serving => "serving",
            Self::Degraded => "degraded",
            Self::Held => "held",
        };
        f.write_str(name)
    }
}

/// What a table is built over beside its configuration and source: the
/// storage its reads go through, the page cache, the row cache when its
/// lookups read through one, the local directory (RFD 017) when one is
/// configured, the count of `get` and `multi_get` calls
/// in flight, and the bound of GETs one `multi_get` holds in flight.
/// `open_with` builds one and hands every table it opens a clone, so a
/// process's tables share one of each. Clones share what they hold.
#[derive(Clone)]
pub struct TableHandles {
    /// The storage the table's reads go through.
    pub storage: Arc<Storage>,
    /// The page cache the table's lookups read pages through.
    pub cache: PageCache,
    /// The row cache a key-value table's lookups read values through (RFD
    /// 026); `None` reads every value from its page.
    pub row_cache: Option<RowCache>,
    /// The local directory, when `storage.local` is set.
    pub local: Option<Arc<LocalDir>>,
    /// The calls in flight on every table holding this count, which a
    /// `multi_get` reads to decide whether to split.
    pub calls: CallsInFlight,
    /// What the tables do with the doctor's findings on each generation
    /// they admit.
    pub findings: FindingLog,
    /// The GETs one `multi_get` holds in flight at once, across every task
    /// the batch runs on, from `server.batch_fan_out` (RFD 013, RFD
    /// 022); above zero, as `Config::validated` holds that setting, and
    /// held to tokio's `Semaphore::MAX_PERMITS`. Each GET a lookup issues,
    /// a Bloom-tier key page, a column's value page, and its dictionary
    /// page each one, takes one of the batch's permits on the lookup's own
    /// task before it asks the page cache for a page not in memory, and the
    /// GET holds it until it ends, on the task the page cache runs it on
    /// (`cache::Issue`); a page the cache serves, or a fetch the lookup
    /// joins, frees it unused. No GET holds one while waiting for another,
    /// so a lookup naming more columns than the bound still runs, and a
    /// lookup waiting for one has nothing queued in the cache, so a `get`
    /// of the same page never waits behind the batch. Each GET has at most
    /// one hedge beside it, so a batch holds at most twice this many GETs
    /// open on the store however many keys and columns it carries. The
    /// wait for a permit is not charged against a fetch's `get_deadline`.
    /// One `scan` or `multi_scan` holds the same bound across every file
    /// and key it reads (RFD 025, Limits).
    pub batch_fan_out: usize,
    /// `server.max_multi_scan_values`: the values one `multi_scan` may ask
    /// for, its keys times its limit times one more than its columns.
    pub max_multi_scan_values: usize,
    /// `server.max_scan_bytes`: the encoded size above which the server
    /// refuses a Scan or `MultiScan` response, which the doctor holds a
    /// table's runs against (`large_runs`).
    pub max_scan_bytes: usize,
}

/// Which calls a table serves (RFD 025, Configuration): a table entry that
/// sets `order_column` is ordered, every other is key-value. Display gives
/// the kind as a message names it.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum TableKind {
    /// At most one row per key, served by `get` and `multi_get`.
    KeyValue,
    /// Many rows per key in order, served by `scan` and `multi_scan`.
    Ordered,
}

impl fmt::Display for TableKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let name = match self {
            Self::KeyValue => "key-value",
            Self::Ordered => "ordered",
        };
        f.write_str(name)
    }
}

/// What a table does with the doctor's findings on each generation it
/// admits (RFD 016).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FindingLog {
    /// Each finding is a log event, at warn when it caps the table below
    /// its declared level and at info otherwise: the daemon's choice, whose
    /// findings reach no other output.
    Events,
    /// Each finding is a debug event: for a caller that prints the
    /// findings as data, which a log line would only repeat.
    Debug,
}

/// The `get` and `multi_get` calls in flight on every table built over a
/// clone of it, sharded as the lookup counts are, so a call does not write
/// a line every other worker writes too: a call adds one to its thread's
/// shard as it starts and takes it back from that same shard as it ends,
/// so the shards sum to the calls in flight. Clones share the count.
#[derive(Clone)]
pub struct CallsInFlight(Arc<Sharded<AtomicUsize>>);

impl CallsInFlight {
    /// No calls, over one shard per worker thread of the calling runtime
    /// (`shard::count`).
    pub fn new() -> Self {
        Self(Arc::new(Sharded::new()))
    }

    /// Counts one call in flight until the returned guard drops.
    pub(crate) fn enter(&self) -> InFlight<'_> {
        let shard = self.0.mine();
        shard.fetch_add(1, Ordering::Relaxed);
        InFlight(shard)
    }

    /// The calls in flight, summed over the shards without synchronization:
    /// a load signal, which may lag a call starting or ending as it is read.
    pub(crate) fn sum(&self) -> usize {
        self.0
            .iter()
            .map(|shard| shard.load(Ordering::Relaxed))
            .sum()
    }
}

impl Default for CallsInFlight {
    fn default() -> Self {
        Self::new()
    }
}

/// One served table: its configuration and source, the generation it
/// serves, and its lifecycle state. Owns the table's `HedgeWindow`, built
/// from the storage's hedge settings and handed to every
/// `Generation::plan_load`, so one table's delay is set from its own reads
/// and never another's (RFD 013). Shared across tasks behind an `Arc`.
pub struct Table {
    /// The table's entry. A reload that changes only its `lookups` or
    /// `strict` stores another in place (`retune`) while `serial` is held,
    /// so a load or refresh reads one entry throughout.
    config: ArcSwap<TableConfig>,
    /// `config`'s name, which no retune changes.
    name: String,
    source: TableSource,
    storage: Arc<Storage>,
    local: Option<Arc<LocalDir>>,
    hedge: Arc<HedgeWindow>,
    /// The window a Scan's span GETs are hedged in (RFD 025, Caching and
    /// hedging), apart from `hedge`, which the lookups and a Bloom-tier
    /// Scan's key pages use. It takes `hedge`'s configuration, and every
    /// span GET races its one delay and enters it with its whole elapsed
    /// time, whatever its pages (`Storage::read_range_hedged`); only the
    /// GETs that feed it differ.
    span_hedge: Arc<HedgeWindow>,
    /// What a Scan needs beside its generation, on an ordered table;
    /// `None` on a key-value table, which no Scan reads.
    scans: Option<Arc<Scans>>,
    /// `TableHandles::max_multi_scan_values`.
    max_multi_scan_values: usize,
    /// `TableHandles::max_scan_bytes`.
    max_scan_bytes: usize,
    /// The serving generation. A `get` borrows it through one `load` guard
    /// for the lookup's whole duration, so the lookup does not see a swap,
    /// and a swap meanwhile keeps the borrowed generation alive until the
    /// guard drops. The guard takes one of its thread's eight fast slots
    /// (arc-swap's debts) and writes no shared reference count; a lookup
    /// that reads nothing completes without suspending and frees its slot
    /// at once, while one waiting on a read holds it through the wait. A
    /// load on a thread whose slots are all held falls back to a counted
    /// reference, what `load_full` costs.
    current: ArcSwapOption<Generation>,
    /// Never held across an `await`.
    state: Mutex<TableState>,
    /// The metadata location a poll compares the source against: the one
    /// the table last admitted or found its serving snapshot at, so a
    /// metadata change that did not move the followed ref does not make
    /// every later poll read the metadata again, or the one where a poll
    /// over a serving generation met a failure fixed for the snapshot
    /// (`Recurrence::Deterministic`): a refusal, or a table or file fault of
    /// the metadata or a data file. That snapshot is not built again until
    /// the source names another location. A transient failure does not
    /// advance it: the next poll reads the metadata again and retries.
    /// `None` until the first successful build. A reload that changes the
    /// table's entry forgets every refusal: one that changes only `lookups`,
    /// `strict`, or column formats resets this to the serving generation's
    /// location (`retune`), and one that changes another field builds a new
    /// `Table`, which remembers nothing, so a refusal that depended on the
    /// old entry is tried again under the new one (RFD 005, RFD 018).
    known: Mutex<Option<Known>>,
    /// Serializes `load`, `refresh`, and `retune`; `Opened::retain` takes
    /// every table's in turn and `Opened::apply` every retuned table's, each
    /// while holding `Opened`'s membership lock, and nothing else ever
    /// takes two.
    serial: tokio::sync::Mutex<()>,
    /// How long a `load` or `refresh` with no generation serving, which
    /// builds from nothing, may hold `serial` doing its work, from
    /// `server.load_deadline`.
    load_deadline: Duration,
    /// How long a `load` or `refresh` over a serving generation may hold
    /// `serial` doing its work, from `server.refresh_deadline`.
    refresh_deadline: Duration,
    lookups: Arc<Lookups>,
    /// `generation_swaps_total` under the table's name, registered when the
    /// table is built, so the series reads 0 from then on rather than
    /// appearing at the first swap.
    swaps: Counter,
    /// Where the doctor's findings on an admitted generation go.
    findings: FindingLog,
    /// What the table's state and generation gauges read, and whether it
    /// shows them. A table a reload loads beside the one serving under its
    /// name shows none until the reload swaps it in, so the serving table's
    /// series describe the serving table (RFD 018). Held while a gauge is
    /// written, so a withdrawal and a publish never interleave; never held
    /// across an `await`.
    gauges: Mutex<Gauges>,
}

/// The metadata location a table's polls compare their source against,
/// and whether the ref there was refused.
#[derive(Clone, Debug)]
struct Known {
    /// The metadata file's location.
    location: String,
    /// The failure fixed for the snapshot the ref names at `location`, or
    /// for the ref itself when it names none there; `None` when the table
    /// serves that snapshot.
    refused: Option<Refusal>,
}

/// A failure fixed for the snapshot that a table remembers until its source
/// names another metadata location.
#[derive(Clone, Debug)]
struct Refusal {
    /// The snapshot refused; `None` when the metadata lacks the ref
    /// (`TableFault::RefNotFound`), so no snapshot was named.
    snapshot_id: Option<SnapshotId>,
    /// Why, as the failure's kind.
    reason: ErrorKind,
    /// When the refusal was made.
    since: Instant,
}

/// A failed load or poll as `fail` records it: the error, boxed so a step's
/// result stays small, and the snapshot the failure was met building,
/// which a `below_level` refusal holds.
struct Failed {
    error: Box<Error>,
    /// The snapshot `build_and_admit` was building; `None` for a failure
    /// before the ref named one.
    snapshot_id: Option<SnapshotId>,
}

impl From<Error> for Failed {
    fn from(error: Error) -> Self {
        Self {
            error: Box::new(error),
            snapshot_id: None,
        }
    }
}

/// What a step of a load or poll returns: its value, or the failure.
type Attempt<T> = std::result::Result<T, Failed>;

/// A table's gauges: whether they are shown, what its serving generation
/// set them to, and the level series, `None` until the first swap.
struct Gauges {
    shown: bool,
    generation: Option<GenerationGauges>,
    level: Option<LevelGauges>,
    /// Whether the last page-cache sample failed to observe a chunk file,
    /// so the failure, and the recovery from it, are each logged once.
    page_cache_unobserved: bool,
}

/// What the level gauges show (RFD 020): the level the newest snapshot the
/// table measured achieves, the one serving or the one held, and each of
/// its value columns' (RFD 022), with the page cache's condition of the
/// serving generation, against the level the table declares. The layout's
/// levels are set together, from one measurement, at a swap and at a
/// `below_level` refusal, so the column levels describe the snapshot the
/// table level does; a refusal for any other reason measured nothing new
/// and leaves them.
#[derive(Clone, Debug, PartialEq, Eq)]
struct LevelGauges {
    /// The newest measured snapshot's level for its layout.
    measured: Level,
    /// The newest measured snapshot's level for each value column, from
    /// the measurement that gave `measured`, in the layout's order.
    columns: Vec<ColumnLevel>,
    /// The table's `lookups`.
    declared: Level,
    /// The page cache's condition of the serving generation's chunk files,
    /// `None` until a `disk` table's first sample. A swap carries it to
    /// the new generation, which keeps most of the old one's chunk files,
    /// until a sample of the new one replaces it.
    page_cache: Option<PageCacheCondition>,
}

impl LevelGauges {
    /// The level the table achieves: its layout's, capped by the page
    /// cache's condition. The one place a server condition joins the
    /// layout's level; `strict` reads the layout's alone.
    fn achieved(&self) -> Level {
        self.measured.max(
            self.page_cache
                .map_or(Level::Optimal, PageCacheCondition::cap),
        )
    }

    /// Whether the table is below its declared level.
    fn below_level(&self) -> bool {
        !self.achieved().meets(self.declared)
    }
}

/// What a swap sets the generation gauges to, kept so a table shown after
/// its swap publishes them then.
struct GenerationGauges {
    index_bytes: usize,
    findings: FindingCounts,
    sequence_number: SequenceNumber,
    swapped_at: f64,
    lookup_cost: LookupCost,
    /// Each value column's name and cost, in the layout's order.
    columns: Vec<(String, ColumnCost)>,
    /// What a cold Scan costs on an ordered table; `None` on a key-value
    /// table, which publishes the per-hit and per-miss costs instead.
    gets_per_scan: Option<u64>,
    deletes: DeleteSummary,
}

/// An admitted generation's findings counted by code and, for a code about
/// one value column, by that column (`FindingCode::per_column`).
type FindingCounts = BTreeMap<Capping, u64>;

/// What a lookup or a Scan needs beside its generation: the table's page
/// cache handle, which a lookup reads through, its lookup metrics, whose
/// value page GET counters a Scan's GETs count in too, and the calls in
/// flight both count among. Behind an `Arc`, so the tasks a `multi_get` or
/// `multi_scan` spawns hold it for as long as they run.
struct Lookups {
    /// The process's page cache, through a handle whose fetches end at the
    /// table's `get_deadline`.
    cache: PageCache,
    /// The process's row cache, when the table's entry has `row_cache` on
    /// (`TableConfig::row_cache`); `None` reads every value from its page.
    /// A change of the setting builds the table anew, so it is fixed here.
    row_cache: Option<RowCache>,
    metrics: LookupMetrics,
    /// The calls in flight on every table sharing the table's handles, its
    /// own among them.
    in_flight: CallsInFlight,
    /// `TableHandles::batch_fan_out`, held to `Semaphore::MAX_PERMITS`.
    batch_fan_out: usize,
}

/// One call counted in a `CallsInFlight` while held, on the shard it was
/// counted on; dropped as the call returns or is dropped.
pub(crate) struct InFlight<'a>(&'a AtomicUsize);

impl Drop for InFlight<'_> {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::Relaxed);
    }
}

impl Lookups {
    /// Counts one call in flight until the returned guard drops.
    fn enter(&self) -> InFlight<'_> {
        self.in_flight.enter()
    }

    /// The runs a batch of `keys` keys is split into: one for each
    /// `keys_min` keys or part of them (`MULTI_GET_TASK_KEYS_MIN` for a
    /// `multi_get`, `MULTI_SCAN_TASK_KEYS_MIN` for a `multi_scan`), up to
    /// one per worker thread (`shard::workers`), while fewer calls than
    /// worker threads are in flight on the tables sharing this one's count,
    /// the batch's own included; one run otherwise. The caller splits its
    /// keys evenly over the runs, so a run may hold fewer than `keys_min`:
    /// 33 keys under a `keys_min` of 32 make two runs of 17 and 16. A split
    /// batch waits on its slowest run, and on busy workers its runs queue
    /// behind other calls' work, so splitting pays while some workers would
    /// sit idle and costs once they are all busy. Calls to every table of
    /// the process run on the same workers, so their count is the signal,
    /// not this table's alone.
    fn runs(&self, keys: usize, keys_min: usize) -> usize {
        let workers = shard::workers();
        if self.in_flight.sum() < workers {
            workers.min(keys.div_ceil(keys_min))
        } else {
            1
        }
    }

    /// One lookup through `generation` reading the value columns
    /// `selection` names, each from the row cache when the table reads
    /// through one and from its page otherwise, timed and counted under its
    /// result, each value or dictionary page GET it issues counted under
    /// its column's `value_page_gets_total` and, in a batch, holding a
    /// permit of `fan_out` while it runs (`Generation::lookup`), then each
    /// value a read names fields of trimmed to them (`trim`), after the row
    /// cache, so callers naming different fields share one entry; the one
    /// path `get` and `multi_get` share.
    async fn one(
        &self,
        generation: &Generation,
        key: &Key,
        selection: &Selection,
        fan_out: Option<&Arc<Semaphore>>,
    ) -> Result<Row> {
        let started = Instant::now();
        let (row, result) = generation
            .lookup(
                key,
                &selection.positions,
                &self.cache,
                self.row_cache.as_ref(),
                fan_out,
                &self.metrics.value_page_gets,
            )
            .await?;
        let row = self.trim(generation, key, selection, row)?;
        self.metrics.record(result, started.elapsed());
        Ok(row)
    }

    /// `row` with each value a read of `selection` names fields of trimmed
    /// to them (RFD 024), after the page cache and the row cache, so pages
    /// and entries stay whole for every caller; a null stays null and a
    /// value holding none of the fields is empty. The bytes each column
    /// read and returned are counted for an answered lookup alone, a null
    /// as none. A malformed value fails the lookup as
    /// `Error::MalformedValue` and counts one `projection_errors_total` for
    /// its column.
    fn trim(
        &self,
        generation: &Generation,
        key: &Key,
        selection: &Selection,
        mut row: Row,
    ) -> Result<Row> {
        let Row::Present(values) = &mut row else {
            return Ok(row);
        };
        let length = |value: &Option<bytes::Bytes>| value.as_ref().map_or(0, bytes::Bytes::len);
        let read: Option<Vec<usize>> = selection
            .projects()
            .then(|| values.iter().map(length).collect());
        self.project(generation, key, None, selection, values)?;
        let counts = self.metrics.counts.mine();
        for (at, (value, &column)) in values.iter().zip(&selection.positions).enumerate() {
            let returned = length(value);
            let read = read.as_ref().map_or(returned, |read| read[at]);
            counts.columns[column].0.add(read, returned);
        }
        Ok(row)
    }

    /// `values`, one per read of `selection` in its order, each trimmed to
    /// the fields its read names (RFD 024), a null staying null: what a
    /// lookup's row and each row a Scan returns are trimmed by. A malformed
    /// value is `Error::MalformedValue` for `key`, and for a Scan's row the
    /// row's `order` value, and counts one `projection_errors_total` for its
    /// column.
    fn project(
        &self,
        generation: &Generation,
        key: &Key,
        order: Option<&OrderValue>,
        selection: &Selection,
        values: &mut [Option<bytes::Bytes>],
    ) -> Result<()> {
        for ((value, &column), fields) in values
            .iter_mut()
            .zip(&selection.positions)
            .zip(&selection.fields)
        {
            if let (Some(bytes), Some(fields)) = (value.as_mut(), fields) {
                *bytes = projection::project(bytes, fields).map_err(|fault| {
                    self.metrics.projection_errors[column].increment(1);
                    Error::MalformedValue {
                        table: generation.table.clone(),
                        column: generation.layout.value_columns[column].name.clone(),
                        key: Box::new(key.clone()),
                        order: order.cloned().map(Box::new),
                        fault,
                    }
                })?;
            }
        }
        Ok(())
    }

    /// `keys` looked up through `generation`, each reading the value
    /// columns `selection` names, and answered in their order. Every GET a
    /// lookup issues holds one permit of `fan_out`, the batch's bound
    /// (`TableHandles::batch_fan_out`), while it runs, so the batch's
    /// tasks together hold at most that many GETs in flight; this task
    /// waits on at most that many lookups itself. A failing run fails with
    /// its first failing key's failure in key order, once every key before
    /// it has passed, and ends there.
    async fn run(
        &self,
        generation: &Generation,
        keys: &[Key],
        selection: &Selection,
        fan_out: &Arc<Semaphore>,
    ) -> Result<Vec<Row>> {
        // The lookups are built before the stream, unpolled, so no closure
        // taking a key by reference lives in the returned future; one would
        // make it unprovably `Send`, and the gRPC service needs it `Send`.
        let lookups: Vec<_> = keys
            .iter()
            .map(|key| self.one(generation, key, selection, Some(fan_out)))
            .collect();
        stream::iter(lookups)
            .buffered(self.batch_fan_out)
            .try_collect()
            .await
    }
}

/// A table's lookup metric handles, one pair per `LookupResult` under the
/// table's name and per value column its `value_page_gets_total`, its
/// value bytes read and returned, and its `projection_errors_total`,
/// registered once when the table is built, with the recorder current
/// then, so a lookup clones no name, formats no label, and looks up no
/// series. A lookup's count and the bytes it answered go to its thread's
/// shard of `counts` and reach their counters at `publish`, which a scrape
/// runs first and a dropped table runs last, so a scrape reads every
/// lookup counted before it. The latency histogram is recorded per lookup,
/// as it happens, and so are a value-page GET and a malformed value, far
/// rarer than a lookup.
struct LookupMetrics {
    cold_hit: ResultMetrics,
    warm_hit: ResultMetrics,
    miss: ResultMetrics,
    /// One shard per worker thread when the workers are the threads that
    /// count.
    counts: Sharded<Counts>,
    /// Per value column, by its position in the layout, which is
    /// `TableConfig::value_column_names`' order, the column's
    /// `value_page_gets_total`.
    value_page_gets: Vec<Counter>,
    /// Per value column, as `value_page_gets`, its
    /// `value_bytes_read_total`, which `publish` moves `counts` into.
    value_bytes_read: Vec<Counter>,
    /// Per value column, as `value_page_gets`, its
    /// `value_bytes_returned_total`, which `publish` moves `counts` into.
    value_bytes_returned: Vec<Counter>,
    /// Per value column, as `value_page_gets`, its
    /// `projection_errors_total`, counted as each malformed value is met.
    projection_errors: Vec<Counter>,
}

/// `lookups_total` and `lookup_seconds` of one table and result.
struct ResultMetrics {
    total: Counter,
    seconds: Histogram,
}

/// One shard of a table's lookup counts, not yet published, per result,
/// and per value column the bytes it answered.
struct Counts {
    cold_hit: AtomicU64,
    warm_hit: AtomicU64,
    miss: AtomicU64,
    /// By the column's position in the layout, each on a line of its own,
    /// since the slice is allocated apart from the shard's line.
    columns: Box<[Line<ColumnBytes>]>,
}

/// One value column's bytes in one shard, not yet published.
#[derive(Default)]
struct ColumnBytes {
    read: AtomicU64,
    returned: AtomicU64,
}

impl ColumnBytes {
    /// One value answered: `read` bytes read, `returned` bytes returned.
    fn add(&self, read: usize, returned: usize) {
        self.read.fetch_add(approx::count(read), Ordering::Relaxed);
        self.returned
            .fetch_add(approx::count(returned), Ordering::Relaxed);
    }
}

impl Counts {
    /// A shard of counts for a table of `columns` value columns.
    fn new(columns: usize) -> Self {
        Self {
            cold_hit: AtomicU64::new(0),
            warm_hit: AtomicU64::new(0),
            miss: AtomicU64::new(0),
            columns: (0..columns).map(|_| Line::default()).collect(),
        }
    }

    fn of(&self, result: LookupResult) -> &AtomicU64 {
        match result {
            LookupResult::ColdHit => &self.cold_hit,
            LookupResult::WarmHit => &self.warm_hit,
            LookupResult::Miss => &self.miss,
        }
    }
}

impl LookupMetrics {
    /// The handles of `table`, whose value columns `columns` names in the
    /// layout's order.
    fn new(table: &str, columns: &[&str]) -> Self {
        let of = |result: LookupResult| ResultMetrics {
            total: metrics::counter!(
                metric::LOOKUPS_TOTAL,
                metric::LABEL_TABLE => table.to_string(),
                metric::LABEL_RESULT => result.to_string()
            ),
            seconds: metrics::histogram!(
                metric::LOOKUP_SECONDS,
                metric::LABEL_TABLE => table.to_string(),
                metric::LABEL_RESULT => result.to_string()
            ),
        };
        let per_column = |name: &'static str| -> Vec<Counter> {
            columns
                .iter()
                .map(|&column| column_counter(name, table, column))
                .collect()
        };
        Self {
            cold_hit: of(LookupResult::ColdHit),
            warm_hit: of(LookupResult::WarmHit),
            miss: of(LookupResult::Miss),
            counts: Sharded::from_fn(|| Counts::new(columns.len())),
            value_page_gets: per_column(metric::VALUE_PAGE_GETS_TOTAL),
            value_bytes_read: per_column(metric::VALUE_BYTES_READ_TOTAL),
            value_bytes_returned: per_column(metric::VALUE_BYTES_RETURNED_TOTAL),
            projection_errors: per_column(metric::PROJECTION_ERRORS_TOTAL),
        }
    }

    fn of(&self, result: LookupResult) -> &ResultMetrics {
        match result {
            LookupResult::ColdHit => &self.cold_hit,
            LookupResult::WarmHit => &self.warm_hit,
            LookupResult::Miss => &self.miss,
        }
    }

    /// One lookup of `result` that took `elapsed`.
    fn record(&self, result: LookupResult, elapsed: Duration) {
        self.counts
            .mine()
            .of(result)
            .fetch_add(1, Ordering::Relaxed);
        self.of(result).seconds.record(time::seconds(elapsed));
    }

    /// Moves the counts every shard holds into `lookups_total` and the
    /// value bytes counters. Each shard's count is taken with a swap to
    /// zero, so a lookup counted meanwhile is published by this call or the
    /// next, and never twice.
    fn publish(&self) {
        let taken = |count: &AtomicU64| count.swap(0, Ordering::Relaxed);
        let publish = |counter: &Counter, counted: u64| {
            if counted > 0 {
                counter.increment(counted);
            }
        };
        for result in LookupResult::ALL {
            let counted = self
                .counts
                .iter()
                .map(|shard| taken(shard.of(result)))
                .sum();
            publish(&self.of(result).total, counted);
        }
        for (column, (read, returned)) in self
            .value_bytes_read
            .iter()
            .zip(&self.value_bytes_returned)
            .enumerate()
        {
            let shards = || self.counts.iter().map(|shard| &shard.columns[column].0);
            publish(read, shards().map(|bytes| taken(&bytes.read)).sum());
            publish(returned, shards().map(|bytes| taken(&bytes.returned)).sum());
        }
    }
}

/// The counter `name` of `column` of `table`.
fn column_counter(name: &'static str, table: &str, column: &str) -> Counter {
    metrics::counter!(
        name,
        metric::LABEL_TABLE => table.to_string(),
        metric::LABEL_COLUMN => column.to_string()
    )
}

impl Drop for LookupMetrics {
    /// Publishes the lookups counted since the last `publish`, so a table
    /// dropped between scrapes loses none of its counts.
    fn drop(&mut self) {
        self.publish();
    }
}

/// What an ordered table's Scan needs beside its generation (RFD 025): the
/// page cache every table shares (RFD 027), through a handle whose fetches
/// end at `scan_deadline`, and the scan metrics.
struct Scans {
    cache: PageCache,
    metrics: ScanMetrics,
}

/// An ordered table's scan metric handles under its name, registered once
/// when the table is built, at zero, with the recorder current then: each
/// `ScanResult`'s `scans_total`, the rows and files histograms, each
/// `Temperature`'s `scan_seconds`, and the order column's
/// `value_page_gets_total`, under the order column's name, so a caller
/// budgets a Scan's GETs from that series as from the value columns' (RFD
/// 025, Metrics). A Scan is far rarer and dearer than a lookup, so each is
/// counted on the shared series as it ends. `response_bytes` is the size of
/// each answered or refused call's response (RFD 025, Limits).
#[derive(Clone)]
struct ScanMetrics {
    rows: Counter,
    empty: Counter,
    error: Counter,
    returned: Histogram,
    files: Histogram,
    response_bytes: Histogram,
    cold: Histogram,
    warm: Histogram,
    order_gets: Counter,
}

impl ScanMetrics {
    /// The handles of `table`, whose order column is `order_column`.
    fn new(table: &str, order_column: &str) -> Self {
        let total = |result: ScanResult| {
            metrics::counter!(
                metric::SCANS_TOTAL,
                metric::LABEL_TABLE => table.to_string(),
                metric::LABEL_RESULT => result.to_string()
            )
        };
        let seconds = |temperature: Temperature| {
            metrics::histogram!(
                metric::SCAN_SECONDS,
                metric::LABEL_TABLE => table.to_string(),
                metric::LABEL_TEMPERATURE => temperature.to_string()
            )
        };
        Self {
            rows: total(ScanResult::Rows),
            empty: total(ScanResult::Empty),
            error: total(ScanResult::Error),
            returned: metrics::histogram!(
                metric::SCAN_ROWS,
                metric::LABEL_TABLE => table.to_string()
            ),
            files: metrics::histogram!(
                metric::SCAN_FILES,
                metric::LABEL_TABLE => table.to_string()
            ),
            response_bytes: metrics::histogram!(
                metric::SCAN_RESPONSE_BYTES,
                metric::LABEL_TABLE => table.to_string()
            ),
            cold: seconds(Temperature::Cold),
            warm: seconds(Temperature::Warm),
            order_gets: column_counter(metric::VALUE_PAGE_GETS_TOTAL, table, order_column),
        }
    }

    /// One Scan of one key answered as `tally` says.
    fn record(&self, tally: &ScanTally) {
        if tally.rows == 0 {
            self.empty.increment(1);
        } else {
            self.rows.increment(1);
        }
        self.returned.record(approx::gauge_len(tally.rows));
        self.files.record(approx::gauge_len(tally.files));
        let seconds = if tally.cold { &self.cold } else { &self.warm };
        seconds.record(time::seconds(tally.elapsed));
    }
}

/// What one Scan of one key read, held by its call's `ScanReceipt` until the
/// call's response is known.
#[derive(Clone, Copy)]
struct ScanTally {
    /// Rows the Scan returned.
    rows: usize,
    /// Files holding the key that it read.
    files: usize,
    /// Whether an order or value page came from object storage.
    cold: bool,
    /// How long the Scan ran.
    elapsed: Duration,
}

/// The count of the Scans of one `scan_receipt` or `multi_scan_receipt`
/// call, held for the caller who builds the response (RFD 025, Limits): a
/// response the server refuses above `server.max_scan_bytes` counts each
/// key as an error, not as the rows it read. `sent` counts every key read
/// as answered, `refused` as an error, and the size goes into
/// `scan_response_bytes` either way. A receipt dropped unsettled, as when
/// a key's read fails, the call's deadline passes, or its client cancels,
/// counts the keys that finished as answered, as a failed `MultiGet`
/// counts the keys it completed; a key that failed counted as an error
/// when it did. A failing call returns only once every key before its
/// first failing one has finished, spread or not, so those keys count as
/// answered; a key after it counts only if it ended before the call
/// returned. Whichever settles the receipt first ends its count,
/// so a key of a spread `multi_scan` that finishes or fails on another
/// task after the call ended counts nowhere, as it would had the call read
/// it on its own task.
#[must_use = "a receipt settles the call's scan counts: `sent` or `refused` it"]
pub struct ScanReceipt {
    /// The table's configured name.
    table: String,
    /// The call's counts, shared with the runs a spread `multi_scan`
    /// spawns.
    counts: Arc<ScanCounts>,
}

/// The Scans of one call as they end, shared by every task that reads the
/// call's keys until its `ScanReceipt` settles them.
struct ScanCounts {
    /// The table's scan metric handles.
    metrics: ScanMetrics,
    /// The keys read and not yet counted, pushed as their Scans finish, on
    /// the call's task or on the runs it spawned; `None` once the receipt
    /// has settled, after which nothing more is counted.
    pending: Mutex<Option<Vec<ScanTally>>>,
}

impl ScanCounts {
    fn lock(&self) -> std::sync::MutexGuard<'_, Option<Vec<ScanTally>>> {
        self.pending.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// The Scan of one key has finished.
    fn finished(&self, tally: ScanTally) {
        if let Some(pending) = self.lock().as_mut() {
            pending.push(tally);
        }
    }

    /// The Scan of one key has failed: one `error`, counted at once.
    fn failed(&self) {
        let pending = self.lock();
        if pending.is_some() {
            self.metrics.error.increment(1);
        }
    }

    /// The keys finished and not yet counted, taken, and the count ended.
    fn settle(&self) -> Vec<ScanTally> {
        self.lock().take().unwrap_or_default()
    }
}

impl ScanReceipt {
    fn new(table: &str, metrics: &ScanMetrics) -> Self {
        Self {
            table: table.to_string(),
            counts: Arc::new(ScanCounts {
                metrics: metrics.clone(),
                pending: Mutex::new(Some(Vec::new())),
            }),
        }
    }

    /// Counts every key read as answered, with no response size: for a
    /// caller that builds no response.
    pub fn answered(self) {
        drop(self);
    }

    /// The response of `bytes` encoded bytes goes out: counts every key
    /// read as answered and records the size.
    pub fn sent(self, bytes: usize) {
        self.counts
            .metrics
            .response_bytes
            .record(approx::gauge_len(bytes));
    }

    /// The response is refused as `oversize`: counts every key read as an
    /// error, records the size it would have had, and gives
    /// `Error::ResponseTooLarge` to return.
    pub fn refused(self, oversize: Oversize) -> Error {
        let keys = self.counts.settle().len();
        let metrics = &self.counts.metrics;
        metrics.error.increment(approx::count(keys));
        metrics
            .response_bytes
            .record(approx::gauge_len(oversize.bytes));
        Error::ResponseTooLarge {
            table: self.table.clone(),
            oversize,
        }
    }
}

impl Drop for ScanReceipt {
    fn drop(&mut self) {
        for tally in self.counts.settle() {
            self.counts.metrics.record(&tally);
        }
    }
}

/// A Scan call checked and ready to read (`Table::scan_call`).
struct ScanCall {
    reads: ScanReads,
    receipt: ScanReceipt,
}

/// What every key's Scan of one `scan` or `multi_scan` call reads through:
/// the generation, the table's lookup and scan handles and span hedge
/// window, the columns the checked request reads, the call's fan-out, and
/// its counts. Each
/// run a spread `multi_scan` spawns reads through a clone of its own, as
/// each run of a `multi_get` holds its own handles.
#[derive(Clone)]
struct ScanReads {
    generation: Arc<Generation>,
    lookups: Arc<Lookups>,
    scans: Arc<Scans>,
    window: Arc<HedgeWindow>,
    selection: Selection,
    fan_out: Arc<Semaphore>,
    counts: Arc<ScanCounts>,
}

/// A call's `ScanReads` under the request's bounds.
#[derive(Clone, Copy)]
struct ScanThrough<'a> {
    reads: &'a ScanReads,
    bounds: &'a ScanBounds,
}

impl ScanThrough<'_> {
    /// One Scan of `key`, its rows trimmed (`Lookups::project`): the one
    /// path `scan` and `multi_scan` share. A failed read counts under
    /// `ScanResult::Error` at once; an answered one is handed to the
    /// call's counts as its tally, rows, files, and temperature, cold when
    /// any order or value page came from object storage, for the receipt
    /// to count.
    async fn key(self, key: &Key) -> Result<Scanned> {
        let started = Instant::now();
        let reads = self.reads;
        let metrics = &reads.scans.metrics;
        let source = RunSource {
            cache: &reads.scans.cache,
            fan_out: Some(&reads.fan_out),
            window: &reads.window,
            value_gets: &reads.lookups.metrics.value_page_gets,
            order_gets: Some(&metrics.order_gets),
        };
        let selection = &reads.selection;
        let answered = async {
            let (mut scanned, read) = reads
                .generation
                .scan(key, &selection.positions, self.bounds, source)
                .await?;
            for row in &mut scanned.rows {
                reads.lookups.project(
                    &reads.generation,
                    key,
                    Some(&row.order),
                    selection,
                    &mut row.values,
                )?;
            }
            Ok::<_, Error>((scanned, read))
        }
        .await;
        match answered {
            Ok((scanned, read)) => {
                reads.counts.finished(ScanTally {
                    rows: scanned.rows.len(),
                    files: read.files,
                    cold: read.from_store,
                    elapsed: started.elapsed(),
                });
                Ok(scanned)
            }
            Err(error) => {
                reads.counts.failed();
                Err(error)
            }
        }
    }

    /// Scans of `keys`, answered in their order, at most
    /// `TableHandles::batch_fan_out` read at once on this task, every GET
    /// holding a permit of the call's fan-out. A failing run fails as
    /// `Lookups::run` fails: with its first failing key's failure in key
    /// order.
    async fn keys(self, keys: &[&Key]) -> Result<Vec<Scanned>> {
        // Built before the stream, unpolled, as `Lookups::run` builds its
        // lookups, so the returned future stays provably `Send`.
        let scans: Vec<_> = keys.iter().map(|key| self.key(key)).collect();
        stream::iter(scans)
            .buffered(self.reads.lookups.batch_fan_out)
            .try_collect()
            .await
    }
}

impl Table {
    /// Constructs in `Loading`; nothing is read until `load`. Every `load`
    /// and `refresh` with no generation serving runs within
    /// `load_deadline`, since it reads every file, and every one over a
    /// serving generation within `refresh_deadline`. Every page fetch a
    /// lookup runs through `cache` ends at `get_deadline` from its start,
    /// failing as `CacheError::FetchDeadline` for every lookup waiting on
    /// it (RFD 013); load reads do not go through the cache and are not
    /// bounded by it. Every `get` and `multi_get` counts among `handles`'
    /// calls in flight while it runs. The lookup metrics and
    /// `generation_swaps_total` are registered here, at zero, with the
    /// recorder current then: a process installs its recorder first. On an
    /// ordered table every page a Scan reads, a Bloom-tier key page
    /// included, goes through the same `cache` (RFD 027), and every fetch
    /// a Scan runs there ends at `scan_deadline` from its start; the scan
    /// metrics are registered here too. The table shows its gauges
    /// from here on.
    pub fn new(
        config: TableConfig,
        source: TableSource,
        handles: TableHandles,
        load_deadline: Duration,
        refresh_deadline: Duration,
        get_deadline: Duration,
        scan_deadline: Duration,
    ) -> Self {
        let table = Self::hidden(
            config,
            source,
            handles,
            load_deadline,
            refresh_deadline,
            get_deadline,
            scan_deadline,
        );
        table.show();
        table
    }

    /// `new`, with the table's state and generation gauges not shown until
    /// `show`: what a reload builds beside a table serving under the same
    /// name, whose series it must leave alone until the swap (RFD 018).
    pub(crate) fn hidden(
        config: TableConfig,
        source: TableSource,
        handles: TableHandles,
        load_deadline: Duration,
        refresh_deadline: Duration,
        get_deadline: Duration,
        scan_deadline: Duration,
    ) -> Self {
        let TableHandles {
            storage,
            cache,
            row_cache,
            local,
            calls,
            findings,
            batch_fan_out,
            max_multi_scan_values,
            max_scan_bytes,
        } = handles;
        let hedge = Arc::new(HedgeWindow::new(storage.hedge_config()));
        let span_hedge = Arc::new(HedgeWindow::new(storage.hedge_config()));
        let scans = config.order_column.as_deref().map(|order_column| {
            Arc::new(Scans {
                cache: cache.clone().with_fetch_deadline(scan_deadline),
                metrics: ScanMetrics::new(&config.name, order_column),
            })
        });
        let lookups = Arc::new(Lookups {
            cache: cache.with_fetch_deadline(get_deadline),
            row_cache: row_cache.filter(|_| config.row_cache()),
            metrics: LookupMetrics::new(&config.name, &config.value_column_names()),
            in_flight: calls,
            batch_fan_out: batch_fan_out.min(Semaphore::MAX_PERMITS),
        });
        let swaps = metrics::counter!(
            metric::GENERATION_SWAPS_TOTAL,
            metric::LABEL_TABLE => config.name.clone()
        );
        Self {
            name: config.name.clone(),
            config: ArcSwap::from_pointee(config),
            source,
            storage,
            local,
            hedge,
            span_hedge,
            scans,
            max_multi_scan_values,
            max_scan_bytes,
            current: ArcSwapOption::empty(),
            state: Mutex::new(TableState::Loading),
            known: Mutex::new(None),
            serial: tokio::sync::Mutex::new(()),
            load_deadline,
            refresh_deadline,
            lookups,
            swaps,
            findings,
            gauges: Mutex::new(Gauges {
                shown: false,
                generation: None,
                level: None,
                page_cache_unobserved: false,
            }),
        }
    }

    /// Shows the table's gauges from now on and publishes them: its state,
    /// and its serving generation's when it has one.
    pub(crate) fn show(&self) {
        let mut gauges = self.gauges.lock().unwrap_or_else(PoisonError::into_inner);
        gauges.shown = true;
        self.write_gauges(&gauges);
    }

    /// Stops showing the table's gauges, for a table a reload removed or
    /// replaced (RFD 018): from here on neither a state change, a swap,
    /// nor `publish_gauges` writes one. The binary's recorder drops a gauge
    /// that no scrape has written since the last, so a removed table's
    /// gauges leave the exposition at the next scrape instead of reading
    /// stale values; a replacement shown after this writes its own over
    /// them.
    pub(crate) fn withdraw(&self) {
        let mut gauges = self.gauges.lock().unwrap_or_else(PoisonError::into_inner);
        gauges.shown = false;
    }

    /// Writes the table's state gauges, its serving generation's gauges,
    /// and the level gauges (`icetray_table_below_level`,
    /// `icetray_table_level`, `icetray_table_declared_level`, and
    /// `icetray_table_chunk_resident_fraction` while judged) again as they
    /// stand, while the table shows them, as `show` does; a scrape calls
    /// this for every table served, since the binary's recorder drops a
    /// gauge no scrape wrote.
    pub fn publish_gauges(&self) {
        let gauges = self.gauges.lock().unwrap_or_else(PoisonError::into_inner);
        if gauges.shown {
            self.write_gauges(&gauges);
        }
    }

    /// Writes the state gauges and, when there is a serving generation,
    /// its gauges and the level gauges; `gauges` is the held lock.
    fn write_gauges(&self, gauges: &Gauges) {
        self.write_state(gauges);
        if let Some(generation) = &gauges.generation {
            self.write_generation(generation);
        }
        if let Some(level) = &gauges.level {
            self.write_level(level);
        }
    }

    /// Takes `sample` of `sampled`'s chunk files as the page cache's
    /// condition, unless another generation has been swapped in since the
    /// sample began: it describes the generation it read, and the next
    /// sample judges the one serving. A sample that observed nothing
    /// (`Err`, or no page counted) makes the condition unjudged, which caps
    /// nothing and withdraws the resident fraction. The level gauges are
    /// written while the table shows them. A failure to observe a chunk
    /// file is logged at warn when the sample before observed, and the
    /// first sample that observes again after one is too; a sample that
    /// counted no page is unjudged without a log.
    fn observe_page_cache(
        &self,
        sampled: &Arc<Generation>,
        sample: &std::result::Result<ResidentPages, Unobserved>,
    ) {
        let mut gauges = self.gauges.lock().unwrap_or_else(PoisonError::into_inner);
        let serving = self.current.load();
        if !serving
            .as_ref()
            .is_some_and(|serving| Arc::ptr_eq(serving, sampled))
        {
            return;
        }
        let shown = gauges.shown;
        let Some(level) = gauges.level.as_mut() else {
            return;
        };
        // intentional: an error is logged below when the sample before
        // observed, and not on every sample after.
        let now = level
            .page_cache
            .unwrap_or(PageCacheCondition::Unjudged)
            .next(sample.as_ref().ok().copied());
        level.page_cache = Some(now);
        if shown {
            self.write_level(level);
        }
        let was_unobserved = gauges.page_cache_unobserved;
        gauges.page_cache_unobserved = sample.is_err();
        drop(gauges);
        match (was_unobserved, sample) {
            (false, Err(error)) => tracing::warn!(
                table = self.name(),
                %error,
                "chunk-file residency cannot be observed, so the page cache caps no level"
            ),
            (true, Ok(_)) => tracing::warn!(
                table = self.name(),
                "chunk-file residency is observed again"
            ),
            (false, Ok(_)) | (true, Err(_)) => {}
        }
    }

    /// The table's entry as it stands; a later retune does not change the
    /// one returned.
    pub fn config(&self) -> Arc<TableConfig> {
        self.config.load_full()
    }

    /// `server.max_multi_scan_values`, which `multi_scan` holds a request to
    /// and `TableInfo` reports.
    pub fn max_multi_scan_values(&self) -> usize {
        self.max_multi_scan_values
    }

    /// `server.max_scan_bytes`, the encoded size above which a Scan or
    /// `MultiScan` response is refused and `TableInfo` reports.
    pub fn max_scan_bytes(&self) -> usize {
        self.max_scan_bytes
    }

    /// First load: resolves the source, builds the snapshot the ref names,
    /// runs the doctor on it, and swaps it in, within the load deadline;
    /// past it the load is dropped as `Error::LoadDeadline`. `Err` leaves
    /// the table `Degraded` with no generation; a first-load failure is
    /// fatal to the process (RFD 005) and is the caller's to report, so it
    /// is not counted under `refresh_errors_total`. On a table that already
    /// serves, a full re-resolve and reload, reusing every file the new
    /// snapshot still names by the same path and size, within the refresh
    /// deadline; a failure leaves the table as a poll's would (`fail`),
    /// `Held` for a `below_level` refusal.
    pub async fn load(&self) -> Result<()> {
        let _serialized = self.serial.lock().await;
        self.within_deadline(self.reload())
            .await
            .map_err(|failed| self.fail(failed))
    }

    /// One poll, serialized per table. `Ok(true)` when a new generation was
    /// swapped in. With no current generation, a full load (a table left
    /// `Degraded` with no generation recovers on the next poll); otherwise
    /// one small read when nothing moved (RFD 005), and a build with reuse
    /// when the ref advanced. `Err` leaves the current generation serving,
    /// the table `Degraded`, or `Held` for a `below_level` refusal, and
    /// counts one `refresh_errors_total` with the error's kind as its
    /// reason; `Ok(false)` after such a failure ends the state, since the
    /// table is in step with its ref again. A failure over a serving
    /// generation that is fixed for the snapshot is the exception
    /// (`Recurrence::Deterministic`): the table remembers it, and while the
    /// source names the same metadata location each poll costs only the
    /// small read, returns `Ok(false)`, counts nothing, and leaves the table
    /// in the state that failure set (`fail`). A transient failure is tried
    /// again on the next poll.
    /// The poll runs within the refresh deadline, or with no generation
    /// serving within the load deadline; past it the poll is dropped as
    /// `Error::RefreshDeadline` or `Error::LoadDeadline`, a failure like any
    /// other.
    pub async fn refresh(&self) -> Result<bool> {
        let _serialized = self.serial.lock().await;
        let outcome = self
            .within_deadline(async {
                match self.current() {
                    None => self.reload().await.map(|()| true),
                    Some(current) => self.poll(&current).await,
                }
            })
            .await;
        match outcome {
            Ok(swapped) => {
                if swapped {
                    self.swaps.increment(1);
                }
                Ok(swapped)
            }
            Err(failed) => {
                metrics::counter!(
                    metric::REFRESH_ERRORS_TOTAL,
                    metric::LABEL_TABLE => self.name.clone(),
                    metric::LABEL_REASON => failed.error.kind().to_string()
                )
                .increment(1);
                Err(self.fail(failed))
            }
        }
    }

    /// The table's configured name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// The lifecycle state now.
    pub fn state(&self) -> TableState {
        self.state
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    /// The serving generation, or `None` before the first successful load.
    pub fn current(&self) -> Option<Arc<Generation>> {
        self.current.load_full()
    }

    /// `current`, or `Error::NotReady`: the one refusal for a table with no
    /// generation to answer from, which `get`, `key_type`, the gRPC
    /// service, and the one-shot subcommands all raise.
    pub fn serving(&self) -> Result<Arc<Generation>> {
        self.current().ok_or_else(|| self.not_ready())
    }

    /// The serving generation's key type, or `Error::NotReady`: the type a
    /// caller reads a key from the wire under before `get` or `multi_get`. Read through a `load` guard dropped before it
    /// returns, so it writes no reference count on the shared generation. A
    /// swap onto a generation of another key type between this and the
    /// lookup is refused by the lookup's own check, as `Error::KeyType`.
    pub fn key_type(&self) -> Result<KeyType> {
        self.current
            .load()
            .as_deref()
            .map(|generation| generation.layout.key_type)
            .ok_or_else(|| self.not_ready())
    }

    /// The layout of the snapshot the ref names now, its key type and an
    /// ordered table's order type among it, from that snapshot's own
    /// schema, with no data file read: a table root's version hint and
    /// metadata file, a metadata file alone, or one catalog call. What a
    /// caller parses command-line keys and bounds under before it pays for
    /// a `load`. The schema and partition-spec faults a load would refuse
    /// are refused here first, as the load refuses them. A load afterward
    /// resolves the source again, and a key of a type its generation no
    /// longer has is refused at lookup as `Error::KeyType`, a bound of
    /// another variant at Scan as `Error::ScanRequest`. Serialized with
    /// `load` and `refresh`, and bounded as a load from nothing is.
    pub async fn resolve_layout(&self) -> Result<TableLayout> {
        let _serialized = self.serial.lock().await;
        self.within_deadline(async {
            let resolved = self.source.resolve(&self.storage).await?;
            let snapshot = self.ref_snapshot(&resolved)?;
            let config = self.config.load();
            Ok(TableLayout::for_entry(&config, &resolved, &snapshot)?)
        })
        .await
        .map_err(|failed| *failed.error)
    }

    /// One lookup against the serving generation, reading the value
    /// columns `columns` names, answered in that order, each from the row
    /// cache when the table's entry has `row_cache` on and the cache holds
    /// it (RFD 026) and from its value page otherwise, and each value
    /// trimmed to the fields its read names (RFD 024); none answers
    /// presence alone (RFD 022). Refused before any read, in this order:
    /// the faults `check_column_reads` names, a column named twice or a
    /// field list out of range or with a number named twice, whatever the
    /// table holds; `Error::WrongTableKind` on an ordered table, which
    /// `scan` serves (RFD 025), whether or not it has loaded;
    /// `Error::NotReady` with no generation; `Error::KeyType` for a key of
    /// another type than the table's; and `Error::ColumnRequest` for a
    /// column the generation does not serve or a field list the entry's
    /// format for it does not take (`TableLayout::select`).
    /// `Error::MalformedValue` for a value that is not the protobuf its
    /// column declares, when its read names fields.
    pub async fn get(&self, key: &Key, columns: &[ColumnRead<'_>]) -> Result<Lookup> {
        let _in_flight = self.lookups.enter();
        let columns = check_column_reads(&self.name, columns)?;
        self.check_kind(TableKind::KeyValue)?;
        // A guard held across the lookup's awaits stays correct wherever
        // the task resumes. While held it occupies one of the loading
        // thread's few fast slots (arc-swap's debts), and a load on a
        // thread whose slots are all taken takes arc-swap's slower path,
        // about what `load_full` costs.
        let current = self.current.load();
        let generation = current.as_deref().ok_or_else(|| self.not_ready())?;
        self.check_key_type(generation, key)?;
        let selection = generation.layout.select(&self.config.load(), columns)?;
        let row = self.lookups.one(generation, key, &selection, None).await?;
        Ok(Lookup {
            row,
            snapshot_id: generation.snapshot_id,
        })
    }

    /// Moves the lookups counted since the last call into `lookups_total`.
    /// A lookup is counted on its thread's shard, not on the shared series,
    /// so a scrape calls this first to read every lookup counted before it;
    /// the binary's `/metrics` does, and a dropped table does it last.
    pub fn publish_lookup_counts(&self) {
        self.lookups.metrics.publish();
    }

    /// The calls in flight this table counts among, for the tests of which
    /// tables share one.
    #[cfg(test)]
    pub(crate) fn calls_in_flight(&self) -> &CallsInFlight {
        &self.lookups.in_flight
    }

    /// Several lookups against one generation, each reading the value
    /// columns `columns` names, trimmed as `get` trims them, answered in
    /// key order (RFD 022). The columns, the table's kind, its readiness,
    /// every key's type, and the columns against the generation are checked
    /// before any read, in the order `get` checks them, so a mismatch
    /// anywhere fails the whole call and issues nothing; any failure fails
    /// the whole call (RFD 010), a malformed value of any key included (RFD
    /// 024), and the call answers its first failing key's failure in key
    /// order, however the keys are read (`spawned`). While fewer `get` and
    /// `multi_get` calls than worker threads are in flight on the tables
    /// sharing this one's `TableHandles::calls`, this one included, the
    /// keys are split, in order and evenly, into one run for each
    /// `MULTI_GET_TASK_KEYS_MIN` keys or part of them, up to one per worker
    /// thread (`shard::workers`), each spawned as a task so a batch's
    /// decodes run on several cores (`Lookups::runs`). A batch that makes
    /// one run, as a batch does once that many calls are in flight, is
    /// looked up here, unspawned. `open_with` gives every table of a
    /// process one count, so calls to any of them count toward holding a
    /// batch back from splitting. The runs share one bound of
    /// `TableHandles::batch_fan_out` GETs in flight, each GET holding a
    /// permit while it runs (`Lookups::run`). Dropping the returned future, as a
    /// deadline does, aborts the runs still going; a GET already issued
    /// runs on, holding its permit, and a lookup still waiting for one
    /// issues nothing. A run's panic is resumed at once, ahead of any
    /// failure held, as a key's panic unwinds a call read on its own task;
    /// a run canceled under a runtime shutdown is `Error::LookupCanceled`.
    pub async fn multi_get(&self, keys: &[Key], columns: &[ColumnRead<'_>]) -> Result<MultiLookup> {
        let _in_flight = self.lookups.enter();
        let columns = check_column_reads(&self.name, columns)?;
        self.check_kind(TableKind::KeyValue)?;
        let generation = self.serving()?;
        for key in keys {
            self.check_key_type(&generation, key)?;
        }
        let selection = generation.layout.select(&self.config.load(), columns)?;
        let fan_out = Arc::new(Semaphore::new(self.lookups.batch_fan_out));
        let runs = self.lookups.runs(keys.len(), MULTI_GET_TASK_KEYS_MIN);
        let rows = if runs <= 1 {
            self.lookups
                .run(&generation, keys, &selection, &fan_out)
                .await?
        } else {
            let selection = Arc::new(selection);
            self.spawned(keys, runs, |part| {
                let lookups = Arc::clone(&self.lookups);
                let generation = Arc::clone(&generation);
                let selection = Arc::clone(&selection);
                let fan_out = Arc::clone(&fan_out);
                let part = part.to_vec();
                async move { lookups.run(&generation, &part, &selection, &fan_out).await }
            })
            .await?
        };
        Ok(MultiLookup {
            rows,
            snapshot_id: generation.snapshot_id,
        })
    }

    /// One Scan of `key` against the serving generation (RFD 025): its live
    /// rows at or above `bounds`' `start` and below its `before`, in the
    /// direction asked, at most the limit, which is from 1 to the table's
    /// `max_scan_rows`, each its order value and one value per
    /// column of `columns`, trimmed to the fields its read names as `get`
    /// trims them, and the next page's bound, `Scanned::next`, when live
    /// rows remain inside the bounds past the last returned. Raised before
    /// any read, in this order: the faults `scan::check_scan_request` names,
    /// whatever the table holds; those `check_scan` names, from the table's
    /// entry, `Error::WrongTableKind` on a key-value table among them,
    /// whether or not it has loaded; `Error::NotReady` with no generation;
    /// and the faults `select_scan` names against the generation. Every
    /// file holding the key is read in one round (`Generation::scan`) through the page cache,
    /// each fetch ending at `scan_deadline`, its span GETs hedged in the
    /// table's span window, and every GET holding one of
    /// `TableHandles::batch_fan_out` permits while it runs. Counted under
    /// `scans_total` by result, `scan_rows`, `scan_files`, and
    /// `scan_seconds` by temperature, as answered.
    pub async fn scan(
        &self,
        key: &Key,
        columns: &[ColumnRead<'_>],
        bounds: &ScanBounds,
    ) -> Result<ScanLookup> {
        let (lookup, receipt) = self.scan_receipt(key, columns, bounds).await?;
        receipt.answered();
        Ok(lookup)
    }

    /// `scan`, with the count of the Scan left to the caller, who knows
    /// whether the response it builds from the answer goes out
    /// (`ScanReceipt`).
    pub async fn scan_receipt(
        &self,
        key: &Key,
        columns: &[ColumnRead<'_>],
        bounds: &ScanBounds,
    ) -> Result<(ScanLookup, ScanReceipt)> {
        let _in_flight = self.lookups.enter();
        let call = self.scan_call(std::slice::from_ref(key), columns, bounds, false)?;
        let through = ScanThrough {
            reads: &call.reads,
            bounds,
        };
        let scanned = through.key(key).await?;
        let lookup = ScanLookup {
            scanned,
            snapshot_id: call.reads.generation.snapshot_id,
        };
        Ok((lookup, call.receipt))
    }

    /// Scans of `keys` from one generation, each answered as `scan`
    /// answers it, in key order, a key listed twice at each position (RFD
    /// 025). The request is checked before any read as `scan` checks it,
    /// with `check_multi_scan` in place of `check_scan`, which holds its
    /// size too: keys times the limit times one more than the columns
    /// above `server.max_multi_scan_values` is `Error::ScanRequest` with
    /// `ScanRequestFault::TooManyValues`, whether or not the table has
    /// loaded. A key listed more than once is read once and its answer
    /// given at each position. While fewer calls than worker threads are in flight on
    /// the tables sharing this one's `TableHandles::calls`, this one
    /// included, the distinct keys are split, in order and evenly, into one
    /// run for each `MULTI_SCAN_TASK_KEYS_MIN` distinct keys or part of
    /// them, up to one per worker thread (`shard::workers`), each spawned
    /// as a task, as `multi_get` splits its keys (`Lookups::runs`); keys
    /// that make one run are read here, unspawned. Each run reads at most
    /// `TableHandles::batch_fan_out` keys at once, and the runs share one
    /// bound of that many GETs in flight; any failure fails the whole call
    /// (RFD 010), and the call answers its first failing key's failure in
    /// key order, however the keys are read (`spawned`). Dropping the
    /// returned future aborts the runs still going, and a key one of them
    /// finishes after that counts nowhere (`ScanReceipt`). A run's panic is
    /// resumed at once, ahead of any failure held, as a key's panic unwinds
    /// a call read on its own task; a run canceled under a runtime shutdown
    /// is `Error::LookupCanceled`.
    pub async fn multi_scan(
        &self,
        keys: &[Key],
        columns: &[ColumnRead<'_>],
        bounds: &ScanBounds,
    ) -> Result<MultiScanLookup> {
        let (lookup, receipt) = self.multi_scan_receipt(keys, columns, bounds).await?;
        receipt.answered();
        Ok(lookup)
    }

    /// `multi_scan`, with the count of the Scans left to the caller as for
    /// `scan_receipt`: each distinct key read counts once.
    pub async fn multi_scan_receipt(
        &self,
        keys: &[Key],
        columns: &[ColumnRead<'_>],
        bounds: &ScanBounds,
    ) -> Result<(MultiScanLookup, ScanReceipt)> {
        let _in_flight = self.lookups.enter();
        let call = self.scan_call(keys, columns, bounds, true)?;
        // Each distinct key is read once, and every position that lists it
        // takes that answer.
        let mut distinct: Vec<&Key> = Vec::with_capacity(keys.len());
        let mut first_at: HashMap<&Key, usize> = HashMap::with_capacity(keys.len());
        let positions: Vec<usize> = keys
            .iter()
            .map(|key| {
                *first_at.entry(key).or_insert_with(|| {
                    distinct.push(key);
                    distinct.len() - 1
                })
            })
            .collect();
        let runs = self.lookups.runs(distinct.len(), MULTI_SCAN_TASK_KEYS_MIN);
        let answered: Vec<Scanned> = if runs <= 1 {
            let through = ScanThrough {
                reads: &call.reads,
                bounds,
            };
            through.keys(&distinct).await?
        } else {
            self.spawned(&distinct, runs, |part| {
                let reads = call.reads.clone();
                let bounds = bounds.clone();
                let part: Vec<Key> = part.iter().map(|&key| key.clone()).collect();
                async move {
                    let keys: Vec<&Key> = part.iter().collect();
                    let through = ScanThrough {
                        reads: &reads,
                        bounds: &bounds,
                    };
                    through.keys(&keys).await
                }
            })
            .await?
        };
        // Each answer moves to the last position that lists its key and is
        // cloned only for the positions before it.
        let mut last_at = vec![0; answered.len()];
        for (position, &at) in positions.iter().enumerate() {
            last_at[at] = position;
        }
        let mut answered: Vec<Option<Scanned>> = answered.into_iter().map(Some).collect();
        let results = positions
            .iter()
            .enumerate()
            .map(|(position, &at)| {
                let answer = if last_at[at] == position {
                    answered[at].take()
                } else {
                    answered[at].clone()
                };
                answer.expect("an answer moves out at its key's last position alone")
            })
            .collect();
        let lookup = MultiScanLookup {
            results,
            snapshot_id: call.reads.generation.snapshot_id,
        };
        Ok((lookup, call.receipt))
    }

    /// A Scan call of `keys` reading `columns` under `bounds` (`multi` for
    /// a `MultiScan`), ready to read: the faults `scan::check_scan_request`
    /// names, then those of `check_scan` or, for a `MultiScan`,
    /// `check_multi_scan`, then `Error::NotReady` with no generation, then
    /// the faults `select_scan` names, all before any read; otherwise the
    /// serving generation, the checked request, a fan-out of
    /// `TableHandles::batch_fan_out` permits, and the call's receipt.
    fn scan_call(
        &self,
        keys: &[Key],
        columns: &[ColumnRead<'_>],
        bounds: &ScanBounds,
        multi: bool,
    ) -> Result<ScanCall> {
        let columns = scan::check_scan_request(&self.name, columns, bounds)?;
        if multi {
            self.check_multi_scan(keys.len(), columns.reads().len(), bounds)?;
        } else {
            self.check_scan(bounds)?;
        }
        let scans = self.ordered()?;
        let generation = self.serving()?;
        let selection = self.select_scan(&generation, keys, columns, bounds)?;
        let fan_out = Arc::new(Semaphore::new(self.lookups.batch_fan_out));
        let receipt = ScanReceipt::new(&self.name, &scans.metrics);
        Ok(ScanCall {
            reads: ScanReads {
                generation,
                lookups: Arc::clone(&self.lookups),
                scans: Arc::clone(scans),
                window: Arc::clone(&self.span_hedge),
                selection,
                fan_out,
                counts: Arc::clone(&receipt.counts),
            },
            receipt,
        })
    }

    /// `keys` split in order into `runs` runs of nearly equal length, each
    /// run spawned on its own task as the future `run` builds over it,
    /// which owns what it reads, and the answers put back in key order.
    /// A failing call answers the failure of its first failing run in key
    /// order, which is its first failing key, as a call read on its own
    /// task answers: a run's failure is held until every run before it has
    /// passed, and a run that fails before it takes its place. The runs
    /// after the first failure known are stopped, since none of them can
    /// change the answer. The tasks live in a `JoinSet`, so returning, or
    /// being dropped, aborts the rest. A run's panic is resumed at once,
    /// ahead of any failure held, as a key's panic unwinds a call read on
    /// its own task; a run canceled under a runtime shutdown fails as
    /// `Error::LookupCanceled`.
    async fn spawned<K, T, F>(
        &self,
        keys: &[K],
        runs: usize,
        mut run: impl FnMut(&[K]) -> F,
    ) -> Result<Vec<T>>
    where
        F: Future<Output = Result<Vec<T>>> + Send + 'static,
        T: Send + 'static,
    {
        let per_run = keys.len().div_ceil(runs);
        let mut tasks = JoinSet::new();
        let handles: Vec<AbortHandle> = keys
            .chunks(per_run)
            .map(|part| tasks.spawn(run(part)))
            .collect();
        // `handles` keeps each run's id unique until the call ends: tokio
        // reuses a task id only once no handle to its task is active.
        let run_of = |id: task::Id| {
            handles
                .iter()
                .position(|handle| handle.id() == id)
                .expect("every task joined is a run spawned here")
        };
        let mut answered: Vec<Option<Vec<T>>> = std::iter::repeat_with(|| None)
            .take(handles.len())
            .collect();
        let mut failed: Option<(usize, Error)> = None;
        while let Some(joined) = tasks.join_next_with_id().await {
            let (index, values) = match joined {
                Ok((id, values)) => (run_of(id), values),
                Err(error) => {
                    let index = run_of(error.id());
                    match error.try_into_panic() {
                        Ok(payload) => std::panic::resume_unwind(payload),
                        Err(canceled) => (
                            index,
                            Err(Error::LookupCanceled {
                                table: self.name.clone(),
                                source: canceled,
                            }),
                        ),
                    }
                }
            };
            match values {
                Ok(values) => answered[index] = Some(values),
                Err(error) => {
                    if failed.as_ref().is_none_or(|(first, _)| index < *first) {
                        for later in &handles[index + 1..] {
                            later.abort();
                        }
                        failed = Some((index, error));
                    }
                }
            }
            if let Some((first, _)) = &failed
                && answered[..*first].iter().all(Option::is_some)
            {
                let (_, error) = failed.take().expect("a failure is held");
                return Err(error);
            }
        }
        Ok(answered
            .into_iter()
            .flat_map(|values| values.expect("every run passed, or the call has failed"))
            .collect())
    }

    /// Holds the table's load and refresh lock, so no load is in flight
    /// while the holder reads the current generation's identities or
    /// retunes the table; for `Opened::retain`, which takes every table's
    /// in turn, and `Opened::apply`, which takes every retuned table's.
    pub(crate) async fn serialized(&self) -> MutexGuard<'_, ()> {
        self.serial.lock().await
    }

    /// Whether `config`, an entry that differs from the table's in
    /// `lookups`, `strict`, value column formats, and `max_scan_rows` alone
    /// (`TableConfig::same_but_tuning`), can be applied in place: a
    /// `lookups` the residency cannot reach and an ordered table's zero
    /// `max_scan_rows` are refused as a load refuses them
    /// (`TableConfig::check_at_use`), a format the serving layout's type
    /// for its column does not take as `RefusalReason::ValueTypeUnsupported`
    /// (`TableLayout::check_formats`), and the serving generation is judged
    /// against the entry (`Generation::judge`), so a strict entry whose
    /// level the serving snapshot does not meet is refused as
    /// `RefusalReason::BelowLevel`, each as a load under it would refuse
    /// that snapshot. With no generation serving there is nothing to
    /// judge. Only a judgement made while holding `serial` (`serialized`)
    /// through `retune` decides: without it a build may swap another
    /// generation in after the judgement.
    pub(crate) fn judge_retune(&self, config: &TableConfig) -> Result<()> {
        config.check_at_use()?;
        match self.current() {
            Some(current) => {
                current.layout.check_formats(config)?;
                current.judge(config.lookups(), config.strict)
            }
            None => Ok(()),
        }
    }

    /// Applies `config`, which `judge_retune` admitted under the same held
    /// `serial`, in place (RFD 020, RFD 024, RFD 025): the serving
    /// generation keeps serving and nothing is read, the next lookup checks
    /// its field lists against the new formats, and the next Scan reads up
    /// to the new `max_scan_rows`, which validation held within
    /// `max_multi_scan_values`. The level gauges declare the new
    /// `lookups` at once, against the newest measured snapshot's level,
    /// which a retune does not change. Every remembered refusal is
    /// forgotten, and logged as forgotten: the known location becomes the
    /// serving generation's, so the next poll builds the snapshot the ref
    /// names under the new entry (RFD 005). A `Held` state ends in
    /// `Serving` when the new entry would admit the held snapshot's level,
    /// which the next poll then swaps in, and stays `Held` until that poll
    /// refuses it again otherwise. A `Degraded` state stays until the next
    /// poll ends it, since the last load or refresh failed whatever the
    /// entry.
    pub(crate) fn retune(&self, config: TableConfig, _serialized: &MutexGuard<'_, ()>) {
        let (declared, strict) = (config.lookups(), config.strict);
        self.config.store(Arc::new(config));
        {
            let mut gauges = self.gauges.lock().unwrap_or_else(PoisonError::into_inner);
            let shown = gauges.shown;
            if let Some(level) = gauges.level.as_mut() {
                level.declared = declared;
                if shown {
                    self.write_level(level);
                }
            }
        }
        let Some(current) = self.current() else {
            return;
        };
        let previous = self
            .known
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .replace(Known {
                location: current.metadata_location.clone(),
                refused: None,
            });
        if let Some(Known {
            location,
            refused: Some(past),
        }) = previous
        {
            tracing::info!(
                table = self.name(),
                snapshot_id = past.snapshot_id.map(tracing::field::display),
                reason = %past.reason,
                location,
                "retune forgot a refused snapshot"
            );
        }
        if let TableState::Held { level, .. } = self.state()
            && (!strict || level.meets(declared))
        {
            self.serve_state(current.snapshot_id);
        }
    }

    /// `work` within the deadline its build takes: with no generation
    /// serving, a build from nothing, the load deadline, or
    /// `Error::LoadDeadline` once it passes; over a serving generation,
    /// which reuses every unchanged file, the refresh deadline, or
    /// `Error::RefreshDeadline`. Called with `serial` held, so no other
    /// build swaps a generation in between the choice and the work. The
    /// work is dropped at the deadline, which cancels its reads and
    /// releases what it reserved, so the caller returns, and releases
    /// `serial`, once the deadline passes, and the next poll or
    /// `Opened::retain` can take it. A location error from the work is
    /// given the table's name (`Error::for_table`).
    async fn within_deadline<T>(&self, work: impl Future<Output = Attempt<T>>) -> Attempt<T> {
        let table = &self.name;
        let outcome = if self.current().is_none() {
            let after = self.load_deadline;
            tokio::time::timeout(after, work)
                .await
                .map_err(|_elapsed| Error::LoadDeadline {
                    table: table.clone(),
                    after,
                })?
        } else {
            let after = self.refresh_deadline;
            tokio::time::timeout(after, work)
                .await
                .map_err(|_elapsed| Error::RefreshDeadline {
                    table: table.clone(),
                    after,
                })?
        };
        outcome.map_err(|Failed { error, snapshot_id }| Failed {
            error: Box::new(error.for_table(table)),
            snapshot_id,
        })
    }

    /// Resolves the source and builds the snapshot the ref names over the
    /// current generation, whether or not one serves.
    async fn reload(&self) -> Attempt<()> {
        let resolved = self.source.resolve(&self.storage).await?;
        let snapshot = self.ref_snapshot(&resolved)?;
        let previous = self.current();
        self.build_and_admit(&resolved, &snapshot, previous.as_ref())
            .await
    }

    /// One poll over `current`: the source is asked whether its metadata
    /// moved from the known location, and the table advances to what the
    /// ref names there (`advance`). A failure of that step fixed for the
    /// snapshot is remembered against the location, with the snapshot it
    /// was met building, so an unchanged source then builds nothing.
    async fn poll(&self, current: &Arc<Generation>) -> Attempt<bool> {
        let known = self
            .known
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
            .expect("a serving generation was admitted, which recorded its metadata location");
        let Some(resolved) = self
            .source
            .resolve_if_changed(&self.storage, Some(&known.location))
            .await?
        else {
            match &known.refused {
                Some(refusal) => self.held_back(refusal),
                None => self.in_step(current),
            }
            return Ok(false);
        };
        self.advance(&resolved, current)
            .await
            .inspect_err(|failed| {
                if failed.error.recurrence() == Recurrence::Deterministic {
                    let reason = failed.error.kind();
                    let refusal = Refusal {
                        snapshot_id: failed.snapshot_id,
                        since: self.refused_since(failed.snapshot_id, &reason),
                        reason,
                    };
                    self.remember(&resolved.location, Some(refusal));
                }
            })
    }

    /// Moves the table onto the snapshot the ref names in `resolved`: only
    /// a snapshot with another id than `current`'s is built. `Ok(true)`
    /// when one was swapped in.
    async fn advance(&self, resolved: &Resolved, current: &Arc<Generation>) -> Attempt<bool> {
        let snapshot = self.ref_snapshot(resolved)?;
        if SnapshotId(snapshot.snapshot_id()) == current.snapshot_id {
            self.remember(&resolved.location, None);
            self.in_step(current);
            return Ok(false);
        }
        self.build_and_admit(resolved, &snapshot, Some(current))
            .await?;
        Ok(true)
    }

    /// The snapshot the configured ref names in `resolved`, under the
    /// table's name.
    fn ref_snapshot(&self, resolved: &Resolved) -> Result<Arc<Snapshot>> {
        resolved.snapshot_for_ref(&self.name, &self.config.load().r#ref)
    }

    /// Plans `snapshot` over `previous` at the footer stage, judges its
    /// level there, builds it, gates it, and swaps it in, all under one
    /// entry. A failure carries `snapshot`'s id.
    async fn build_and_admit(
        &self,
        resolved: &Resolved,
        snapshot: &Snapshot,
        previous: Option<&Arc<Generation>>,
    ) -> Attempt<()> {
        let config = self.config();
        let admitted: Result<()> = async {
            // The footer stage and the chunk stage are boxed: inline, the
            // larger of them puts a refresh's future over the size
            // clippy::large_futures allows a future that is moved.
            let mut planned = Box::pin(Generation::plan_load(
                &config,
                resolved,
                snapshot,
                &self.storage,
                Arc::clone(&self.hedge),
                previous.map(Arc::as_ref),
            ))
            .await?;
            planned.judge_level(self.local.as_ref()).await?;
            let generation = Box::pin(planned.build(self.local.as_ref())).await?;
            let findings = self.gate(&generation, &config)?;
            self.swap(generation, previous, &findings, config.lookups());
            self.remember(&resolved.location, None);
            Ok(())
        }
        .await;
        admitted.map_err(|error| Failed {
            error: Box::new(error),
            snapshot_id: Some(SnapshotId(snapshot.snapshot_id())),
        })
    }

    /// The poll found the table in step with its ref: `current` is the
    /// snapshot the ref names, so it is the newest snapshot and the level
    /// gauges measure it, in place of a snapshot a `below_level` refusal
    /// held, whatever the state (a retune can end `Held` with the held
    /// snapshot's level still measured). A `Degraded` or `Held` state an
    /// earlier poll left ends, since the last refresh succeeded.
    fn in_step(&self, current: &Generation) {
        self.measured(current.level, current.column_levels());
        if matches!(
            self.state(),
            TableState::Degraded { .. } | TableState::Held { .. }
        ) {
            self.serve_state(current.snapshot_id);
        }
    }

    /// Records that `snapshot_id` serves from now; the one writer of
    /// `TableState::Serving`.
    fn serve_state(&self, snapshot_id: SnapshotId) {
        self.set_state(TableState::Serving {
            snapshot_id,
            since: Instant::now(),
        });
    }

    /// The doctor's verdict on a built generation before it is swapped in
    /// (RFD 016), under the entry `config` it was built for. Every finding
    /// is logged as the table's `FindingLog` says.
    /// The level was judged at the footer stage (`Planned::judge_level`);
    /// the generation's measured level is judged again under `strict` as a
    /// backstop, by the same `check::judge`, so a generation below the
    /// table's `lookups` is never swapped in whatever the footer stage
    /// missed. `Ok` carries the admitted generation's findings per code,
    /// for the `findings` gauge; a refused generation's reach the logs and
    /// the error and never the gauge, which describes the serving
    /// generation.
    fn gate(&self, generation: &Generation, config: &TableConfig) -> Result<FindingCounts> {
        let declared = config.lookups();
        let report = check::diagnose(
            generation,
            declared,
            &config.formats(),
            config.scan_limits(self.max_scan_bytes, self.max_multi_scan_values),
        );
        let mut counts = FindingCounts::new();
        for finding in &report.findings {
            *counts.entry(finding.capping()).or_default() += 1;
            log_finding(self.name(), finding, self.findings, declared);
        }
        generation.judge(declared, config.strict)?;
        Ok(counts)
    }

    /// Makes `generation` the serving one. The generation gauges it sets,
    /// `index_bytes`, `findings` from `findings`,
    /// `generation_sequence_number`, `generation_swapped_at_seconds` from
    /// the clock, the cost gauges from its `lookup_cost` and, on an
    /// ordered table, its `gets_per_scan`, and the delete gauges from its
    /// `delete_summary`, are kept and written while
    /// the table shows them
    /// (`write_generation`), and the level gauges take the generation's
    /// level against `declared`, keeping the page cache's condition.
    fn swap(
        &self,
        generation: Generation,
        previous: Option<&Arc<Generation>>,
        findings: &FindingCounts,
        declared: Level,
    ) {
        let reused = previous.map_or(0, |previous| {
            generation
                .files
                .iter()
                .filter(|file| previous.files.iter().any(|old| Arc::ptr_eq(file, old)))
                .count()
        });
        tracing::info!(
            table = self.name(),
            snapshot_id = %generation.snapshot_id,
            files = generation.files.len(),
            reused,
            records = generation.records,
            residency = %generation.residency,
            index_bytes = generation.index_bytes,
            "swapped in a generation"
        );
        let published = GenerationGauges {
            index_bytes: generation.index_bytes,
            findings: findings.clone(),
            sequence_number: generation.sequence_number,
            swapped_at: time::unix_seconds(time::now()),
            lookup_cost: generation.lookup_cost,
            columns: generation
                .layout
                .value_columns
                .iter()
                .map(|column| column.name.clone())
                .zip(generation.column_costs.iter().copied())
                .collect(),
            gets_per_scan: generation.gets_per_scan,
            deletes: generation.delete_summary(),
        };
        let snapshot_id = generation.snapshot_id;
        let replaced = {
            let mut gauges = self.gauges.lock().unwrap_or_else(PoisonError::into_inner);
            let level = LevelGauges {
                measured: generation.level,
                columns: generation.column_levels(),
                declared,
                page_cache: gauges.level.as_ref().and_then(|level| level.page_cache),
            };
            if gauges.shown {
                self.write_generation(&published);
                self.write_level(&level);
            }
            gauges.generation = Some(published);
            gauges.level = Some(level);
            // Stored under the lock, so a page-cache sample of the
            // generation this replaces is seen to be stale
            // (`observe_page_cache`).
            self.current.swap(Some(Arc::new(generation)))
        };
        drop(replaced);
        self.serve_state(snapshot_id);
    }

    /// Records the failure as the table's state and hands its error back
    /// to the caller, which logs it. The current generation is untouched. A
    /// `below_level` refusal measured a snapshot, so the level gauges take
    /// its level and its columns' once the table has swapped one in, and it
    /// makes the table
    /// `Held` on the snapshot it was met building whenever a generation
    /// serves, from a poll or a `load` alike (`failed_state`); every other
    /// failure, and a first load below its level, makes it `Degraded`.
    fn fail(&self, Failed { error, snapshot_id }: Failed) -> Error {
        let reason = error.kind();
        if let ErrorKind::Refused(RefusalReason::BelowLevel {
            achieved, columns, ..
        }) = &reason
        {
            self.measured(*achieved, columns.clone());
        }
        self.set_state(self.failed_state(&Refusal {
            snapshot_id,
            since: self.refused_since(snapshot_id, &reason),
            reason,
        }));
        *error
    }

    /// When the refusal of `snapshot_id` for `reason` counts from: a `Held`
    /// table refused below its level again for the snapshot it holds keeps
    /// the `since` of the first refusal, so the state's age is how long that
    /// snapshot has been held; any other refusal counts from now.
    fn refused_since(&self, snapshot_id: Option<SnapshotId>, reason: &ErrorKind) -> Instant {
        if let TableState::Held { refused, since, .. } = self.state()
            && let ErrorKind::Refused(RefusalReason::BelowLevel { .. }) = reason
            && snapshot_id == Some(refused)
        {
            since
        } else {
            Instant::now()
        }
    }

    /// The poll found the source still naming the location where `refusal`
    /// was met, so the table stays in the state that refusal set
    /// (`failed_state`). The state is set again when it is another: a
    /// transient failure of a poll since the refusal replaced it.
    fn held_back(&self, refusal: &Refusal) {
        let state = self.failed_state(refusal);
        let already = match (&state, self.state()) {
            (
                TableState::Held { refused, .. },
                TableState::Held {
                    refused: current, ..
                },
            ) => *refused == current,
            (
                TableState::Degraded { reason, .. },
                TableState::Degraded {
                    reason: current, ..
                },
            ) => *reason == current,
            (
                TableState::Loading
                | TableState::Serving { .. }
                | TableState::Degraded { .. }
                | TableState::Held { .. },
                _,
            ) => false,
        };
        if !already {
            self.set_state(state);
        }
    }

    /// The state `refusal` leaves the table in: `Held` for a `below_level`
    /// refusal of a named snapshot while a generation serves (RFD 020),
    /// `Degraded` for anything else.
    fn failed_state(&self, refusal: &Refusal) -> TableState {
        let serving = self.current().map(|generation| generation.snapshot_id);
        match (&refusal.reason, serving, refusal.snapshot_id) {
            (
                ErrorKind::Refused(RefusalReason::BelowLevel { achieved, .. }),
                Some(serving),
                Some(refused),
            ) => TableState::Held {
                serving,
                refused,
                level: *achieved,
                since: refusal.since,
            },
            _ => TableState::Degraded {
                serving,
                reason: refusal.reason.clone(),
                since: refusal.since,
            },
        }
    }

    /// Takes `measured` and `columns` as the newest measured snapshot's
    /// level and its value columns' for the level gauges, once a swap made
    /// them appear, and writes them while the table shows its gauges and a
    /// level changed.
    fn measured(&self, measured: Level, columns: Vec<ColumnLevel>) {
        let mut gauges = self.gauges.lock().unwrap_or_else(PoisonError::into_inner);
        let shown = gauges.shown;
        let Some(level) = gauges.level.as_mut() else {
            return;
        };
        if level.measured == measured && level.columns == columns {
            return;
        }
        level.measured = measured;
        level.columns = columns;
        if shown {
            self.write_level(level);
        }
    }

    /// Records the metadata location the table is now in step with, or
    /// where `refused` was met. Replacing a remembered refusal with another
    /// location logs, once, that the source moved past it, with the
    /// refused snapshot when the ref named one.
    fn remember(&self, location: &str, refused: Option<Refusal>) {
        let next = Known {
            location: location.to_string(),
            refused,
        };
        let previous = self
            .known
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .replace(next);
        if let Some(Known {
            location: was,
            refused: Some(past),
        }) = previous
            && was != location
        {
            tracing::info!(
                table = self.name(),
                snapshot_id = past.snapshot_id.map(tracing::field::display),
                reason = %past.reason,
                location,
                "source moved past a refused snapshot"
            );
        }
    }

    /// Replaces the state and publishes it.
    fn set_state(&self, state: TableState) {
        *self.state.lock().unwrap_or_else(PoisonError::into_inner) = state;
        self.publish_state();
    }

    /// Sets the `table_state` gauge to 1 for the current state and 0 for
    /// the others, while the table shows its gauges; every
    /// state the table is ever in, the initial `Loading` included, passes
    /// through here or through `show`.
    fn publish_state(&self) {
        let gauges = self.gauges.lock().unwrap_or_else(PoisonError::into_inner);
        if gauges.shown {
            self.write_state(&gauges);
        }
    }

    /// Writes the `table_state` gauge; `_shown` is the held lock that
    /// orders the write against `withdraw`.
    fn write_state(&self, _shown: &Gauges) {
        let current = self.state().name();
        for name in StateName::ALL {
            metrics::gauge!(
                metric::TABLE_STATE,
                metric::LABEL_TABLE => self.name.clone(),
                metric::LABEL_STATE => name.to_string()
            )
            .set(approx::gauge(u64::from(name == current)));
        }
    }

    /// Writes the generation gauges from `generation`; called with the
    /// gauges' lock held. The `findings` gauge is set for every code a load
    /// raises (`FindingCode::raised_at_load`), zero for the absent ones, so
    /// a code that cleared on the swap reads zero:
    /// a code about the row or the table under `{table, code}`, and a code
    /// about one value column (`FindingCode::per_column`) under `{table,
    /// code, column}` for every column the table serves (RFD 022). A
    /// key-value table's cost gauges are per hit and per miss
    /// (`write_lookup_costs`), an ordered table's per Scan; both publish the
    /// files one call probes.
    fn write_generation(&self, generation: &GenerationGauges) {
        metrics::gauge!(
            metric::INDEX_BYTES,
            metric::LABEL_TABLE => self.name.clone()
        )
        .set(approx::gauge_len(generation.index_bytes));
        let count = |code: FindingCode, column: Option<&str>| {
            let capping = Capping {
                code,
                column: column.map(str::to_string),
            };
            approx::gauge(generation.findings.get(&capping).copied().unwrap_or(0))
        };
        for code in FindingCode::ALL
            .into_iter()
            .filter(|code| code.raised_at_load())
        {
            if code.per_column() {
                for (column, _) in &generation.columns {
                    metrics::gauge!(
                        metric::FINDINGS,
                        metric::LABEL_TABLE => self.name.clone(),
                        metric::LABEL_CODE => code.to_string(),
                        metric::LABEL_COLUMN => column.clone()
                    )
                    .set(count(code, Some(column)));
                }
            } else {
                metrics::gauge!(
                    metric::FINDINGS,
                    metric::LABEL_TABLE => self.name.clone(),
                    metric::LABEL_CODE => code.to_string()
                )
                .set(count(code, None));
            }
        }
        metrics::gauge!(
            metric::GENERATION_SEQUENCE_NUMBER,
            metric::LABEL_TABLE => self.name.clone()
        )
        .set(approx::signed_gauge(generation.sequence_number.0));
        metrics::gauge!(
            metric::GENERATION_SWAPPED_AT_SECONDS,
            metric::LABEL_TABLE => self.name.clone()
        )
        .set(generation.swapped_at);
        let cost = generation.lookup_cost;
        match generation.gets_per_scan {
            None => self.write_lookup_costs(cost, &generation.columns),
            Some(gets) => {
                metrics::gauge!(
                    metric::TABLE_GETS_PER_SCAN,
                    metric::LABEL_TABLE => self.name.clone()
                )
                .set(approx::gauge(gets));
            }
        }
        metrics::gauge!(
            metric::TABLE_FILES_PROBED_PER_LOOKUP,
            metric::LABEL_TABLE => self.name.clone()
        )
        .set(approx::gauge_len(cost.files_probed));
        let deletes = generation.deletes;
        metrics::gauge!(
            metric::TABLE_DELETED_ROWS,
            metric::LABEL_TABLE => self.name.clone()
        )
        .set(approx::gauge(deletes.rows));
        metrics::gauge!(
            metric::TABLE_DELETED_FRACTION,
            metric::LABEL_TABLE => self.name.clone()
        )
        .set(deletes.fraction);
        metrics::gauge!(
            metric::TABLE_DELETE_FILES,
            metric::LABEL_TABLE => self.name.clone()
        )
        .set(approx::gauge(deletes.files));
        metrics::gauge!(
            metric::TABLE_DELETE_STATE_BYTES,
            metric::LABEL_TABLE => self.name.clone()
        )
        .set(approx::gauge_len(deletes.state_bytes));
    }

    /// Writes a key-value table's per-hit and per-miss cost gauges from
    /// `cost`, and each of `columns`' per-hit cost (RFD 020, RFD 022); an
    /// ordered table serves no hit and publishes none of them (RFD 025).
    fn write_lookup_costs(&self, cost: LookupCost, columns: &[(String, ColumnCost)]) {
        metrics::gauge!(
            metric::TABLE_GETS_PER_HIT,
            metric::LABEL_TABLE => self.name.clone()
        )
        .set(approx::gauge(cost.gets_per_hit));
        metrics::gauge!(
            metric::TABLE_GETS_PER_MISS,
            metric::LABEL_TABLE => self.name.clone()
        )
        .set(approx::gauge(cost.gets_per_miss));
        for (column, cost) in columns {
            metrics::gauge!(
                metric::TABLE_COLUMN_GETS_PER_HIT,
                metric::LABEL_TABLE => self.name.clone(),
                metric::LABEL_COLUMN => column.clone()
            )
            .set(approx::gauge(cost.gets_per_hit));
        }
    }

    /// Writes the level gauges from `level`, each value column's level
    /// capped by the page cache's condition as the table's is, and the
    /// resident fraction of the chunk files while the page cache's
    /// condition is judged, which the binary's recorder then drops at the
    /// next scrape; called with the gauges' lock held.
    fn write_level(&self, level: &LevelGauges) {
        let cap = level
            .page_cache
            .map_or(Level::Optimal, PageCacheCondition::cap);
        for column in &level.columns {
            metrics::gauge!(
                metric::TABLE_COLUMN_LEVEL,
                metric::LABEL_TABLE => self.name.clone(),
                metric::LABEL_COLUMN => column.column.clone()
            )
            .set(column.level.max(cap).gauge());
        }
        metrics::gauge!(
            metric::TABLE_BELOW_LEVEL,
            metric::LABEL_TABLE => self.name.clone()
        )
        .set(approx::gauge(u64::from(level.below_level())));
        metrics::gauge!(
            metric::TABLE_LEVEL,
            metric::LABEL_TABLE => self.name.clone()
        )
        .set(level.achieved().gauge());
        metrics::gauge!(
            metric::TABLE_DECLARED_LEVEL,
            metric::LABEL_TABLE => self.name.clone()
        )
        .set(level.declared.gauge());
        if let Some(fraction) = level.page_cache.and_then(PageCacheCondition::fraction) {
            metrics::gauge!(
                metric::TABLE_CHUNK_RESIDENT_FRACTION,
                metric::LABEL_TABLE => self.name.clone()
            )
            .set(fraction);
        }
    }

    /// Which calls the table serves, fixed by its entry's `order_column`
    /// when it is built: known before any generation loads, so a call of
    /// the wrong kind is refused ahead of the readiness check
    /// (`check_kind`).
    pub fn kind(&self) -> TableKind {
        match self.scans {
            Some(_) => TableKind::Ordered,
            None => TableKind::KeyValue,
        }
    }

    /// `Error::WrongTableKind` for a call that serves tables of the
    /// `served` kind when this table is of the other (RFD 025): the gRPC
    /// service runs it before it admits a call, and `get`, `multi_get`,
    /// `scan`, and `multi_scan` before they look for a generation, each
    /// after the faults the request holds on its own, so a call that can
    /// never be answered is never `NotReady`, which a caller retries.
    pub fn check_kind(&self, served: TableKind) -> Result<()> {
        if self.kind() == served {
            Ok(())
        } else {
            Err(self.wrong_kind())
        }
    }

    /// What a Scan needs beside its generation, or `Error::WrongTableKind`
    /// on a key-value table: `check_kind` for the calls that use it.
    fn ordered(&self) -> Result<&Arc<Scans>> {
        self.scans.as_ref().ok_or_else(|| self.wrong_kind())
    }

    /// `Error::WrongTableKind` naming this table's own kind.
    fn wrong_kind(&self) -> Error {
        Error::WrongTableKind {
            table: self.name.clone(),
            kind: self.kind(),
        }
    }

    /// `Error::NotReady` for this table.
    fn not_ready(&self) -> Error {
        Error::NotReady {
            table: self.name.clone(),
        }
    }

    /// `Error::WrongTableKind` on a key-value table, and `Error::ScanRequest`
    /// for a limit above the entry's `max_scan_rows` (`scan::check_limit`):
    /// what a Scan is held to by the table's configuration alone, so the
    /// gRPC service checks it before it admits the call or reads the
    /// table's readiness, and `scan` checks it again. Each check reads the
    /// entry as it stands, so a retune that lowers `max_scan_rows` between
    /// the two can refuse at `scan` a limit the first check admitted; one
    /// that raises it leaves the first check's refusal standing.
    pub fn check_scan(&self, bounds: &ScanBounds) -> Result<()> {
        self.check_kind(TableKind::Ordered)?;
        // An ordered table's entry names its order column; one that does
        // not is not served as ordered.
        let Some(max_scan_rows) = self.config.load().scan_rows() else {
            return Err(Error::WrongTableKind {
                table: self.name.clone(),
                kind: TableKind::KeyValue,
            });
        };
        scan::check_limit(bounds, max_scan_rows).map_err(|fault| self.scan_refused(fault))
    }

    /// `check_scan`, then `Error::ScanRequest` with
    /// `ScanRequestFault::TooManyValues` for `keys` keys at the limit
    /// reading `columns` columns above `server.max_multi_scan_values`
    /// (`scan::check_multi_scan_values`): what a `MultiScan` is held to by
    /// the table's configuration and the server's alone.
    pub fn check_multi_scan(&self, keys: usize, columns: usize, bounds: &ScanBounds) -> Result<()> {
        self.check_scan(bounds)?;
        scan::check_multi_scan_values(keys, bounds.limit, columns, self.max_multi_scan_values)
            .map_err(|fault| self.scan_refused(fault))
    }

    /// `Error::ScanRequest` for `fault` on this table.
    fn scan_refused(&self, fault: ScanRequestFault) -> Error {
        Error::ScanRequest {
            table: self.name.clone(),
            fault,
        }
    }

    /// A Scan of `keys` reading `columns` under `bounds`, which
    /// `scan::check_scan_request` and `check_scan` have passed, checked
    /// against `generation` before any read (RFD 025, Wire), in this order:
    /// `Error::KeyType` for a key of another type; `Error::ScanRequest` for
    /// a bound of another variant than the order type's
    /// (`scan::check_bound_variants`); and `Error::ColumnRequest` for a
    /// column the generation does not serve or a field list its format does
    /// not take, as `get` checks the columns. Every position the selection
    /// holds is one of the generation's columns, which every file's index
    /// holds a slot for.
    fn select_scan(
        &self,
        generation: &Generation,
        keys: &[Key],
        columns: CheckedReads<'_>,
        bounds: &ScanBounds,
    ) -> Result<Selection> {
        // An ordered table's generation names its order column; one that
        // does not is not served as ordered.
        let Some(order) = generation.layout.order.as_ref() else {
            return Err(Error::WrongTableKind {
                table: self.name.clone(),
                kind: TableKind::KeyValue,
            });
        };
        for key in keys {
            self.check_key_type(generation, key)?;
        }
        scan::check_bound_variants(bounds, order.order_type.variant())
            .map_err(|fault| self.scan_refused(fault))?;
        generation.layout.select(&self.config.load(), columns)
    }

    /// `Error::KeyType` for a key of another type than the generation's.
    fn check_key_type(&self, generation: &Generation, key: &Key) -> Result<()> {
        let expected = generation.layout.key_type;
        let got = key.key_type();
        if got == expected {
            Ok(())
        } else {
            Err(Error::KeyType {
                table: self.name.clone(),
                expected,
                got,
            })
        }
    }
}

/// Samples which pages of the serving generation's chunk files the
/// kernel's page cache holds, for every one of `tables` (RFD 020), on one
/// blocking task, sampling each chunk file once however many of `tables`
/// read it, as two entries over one Iceberg table do. Each table takes its
/// sample as the page cache's condition (`Table::observe_page_cache`):
/// the table falls below optimal while the resident fraction is short and
/// recovers once it is not, with hysteresis (`PageCacheCondition::next`).
/// A table with no generation serving, or not in the Disk tier, samples
/// nothing. The condition caps the level the gauges show and nothing else:
/// whether a snapshot is refused or held, and how a retune is judged, read
/// the layout's level alone, since the page cache depends on the server
/// and its other tables, not on the snapshot.
pub(crate) async fn sample_page_cache(tables: &[&Table]) {
    let serving: Vec<(&Table, Arc<Generation>)> = tables
        .iter()
        .filter_map(|table| {
            let generation = table.current()?;
            (generation.residency == Residency::Disk).then_some((*table, generation))
        })
        .collect();
    if serving.is_empty() {
        return;
    }
    let generations: Vec<Arc<Generation>> = serving
        .iter()
        .map(|(_, generation)| Arc::clone(generation))
        .collect();
    let Ok(samples) = on_blocking_pool(move || {
        let mut sampled = HashMap::new();
        generations
            .iter()
            .map(|generation| generation.chunk_residency_in(&mut sampled))
            .collect::<Vec<_>>()
    })
    .await
    else {
        // intentional: the runtime is shutting down, and every condition
        // stays as the last sample left it.
        return;
    };
    for ((table, generation), sample) in serving.iter().zip(&samples) {
        table.observe_page_cache(generation, sample);
    }
}

impl fmt::Debug for Table {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Table")
            .field("name", &self.name)
            .field("state", &self.state())
            .finish_non_exhaustive()
    }
}

/// One finding as a log event, with the table, code, cap, and file as
/// fields, the code and cap in the names the doctor's JSON lines carry
/// (`caps` absent for advice), and the finding's message as the event's:
/// under `FindingLog::Events` at warn when its cap is below `declared`, the
/// table's level, and at info otherwise; under `FindingLog::Debug` at
/// debug.
fn log_finding(table: &str, finding: &Finding, log: FindingLog, declared: Level) {
    let message = finding.message();
    let code = finding.code();
    let caps = finding.caps().map(Level::name);
    let file = finding.file();
    let below = finding.caps().is_some_and(|cap| !cap.meets(declared));
    match (log, below) {
        (FindingLog::Events, true) => {
            tracing::warn!(table, code = %code, caps, file, "{message}");
        }
        (FindingLog::Events, false) => {
            tracing::info!(table, code = %code, caps, file, "{message}");
        }
        (FindingLog::Debug, true | false) => {
            tracing::debug!(table, code = %code, caps, file, "{message}");
        }
    }
}

#[cfg(test)]
mod tests {
    use std::future::Future;
    use std::ops::Range;
    use std::sync::Arc;

    use bytes::Bytes;
    use iceberg::io::FileIO;
    use metrics::{Key as MetricKey, SharedString, Unit};
    use metrics_util::CompositeKey;
    use metrics_util::debugging::{DebugValue, DebuggingRecorder, Snapshotter};

    use object_store::local::LocalFileSystem;
    use object_store::throttle::{ThrottleConfig, ThrottledStore};

    use super::*;
    use crate::cache::CacheError;
    use crate::catalog::TableFault;
    use crate::config::{
        ConfigError, ConfigViolation, DEFAULT_BATCH_FAN_OUT, DEFAULT_MAX_SCAN_ROWS, Residency,
        ServerConfig, ValueColumnConfig, ValueFormat,
    };
    use crate::error::{Budget, ErrorKind, UnsupportedDelete};
    use crate::generation::testing::{self, DETAIL_COLUMN, detail_of};
    use crate::generation::testing::{
        Bloom, Bounds, Dictionary, E2E_KEYS, FILE_KEYS, FIRST_SEQUENCE, FIRST_SNAPSHOT,
        FIRST_VERSION, FileSpec, KEY_STEP, POLL_GETS, RESOLVE_GETS, SECOND_SEQUENCE,
        SECOND_SNAPSHOT, SECOND_VERSION, TABLE, THIRD_SEQUENCE, THIRD_SNAPSHOT, THIRD_VERSION,
        TINY_BUDGET, TWO, Table as Fixture, WriterLayout, bucketing, cache, keys_from,
        keys_in_bucket, local_dir, long_keys, parquet, refusal_of, two_bucket_table, value_for,
        values_of,
    };
    use crate::index::testing::{with_dictionary_one_entry_short, with_first_page_claiming};
    use crate::index::{DecodedPart, FileFault};
    use crate::key::KeyType;
    use crate::level::{FALL_PER_MILLE, PER_MILLE};
    use crate::local::ChunkRules;
    #[cfg(target_os = "linux")]
    use crate::local::testing::evict;
    use crate::local::testing::{chunks_offset, overwrite};
    use crate::manifest::ColumnRequestFault;
    use crate::manifest::testing::{self as manifest_testing, Partition, VALUE_COLUMN, whole};
    use crate::metric::LookupResult;
    use crate::order::{Direction, OrderValue, OrderVariant};
    use crate::projection::MAX_FIELD_NUMBER;
    use crate::row_cache::weigh;
    use crate::scan::{BoundSide, Fits, ScanRequestFault, ScanRow, check_scan_request};
    use crate::storage::testing::{DEADLINE_GUARD, Gauged, STALL};
    use crate::storage::{FILE_PREFIX, LocationError, LocationFault};

    /// The positions in bucket 0's key list that the duplicate file
    /// repeats.
    const DUP_KEYS: Range<usize> = 1..3;
    /// The name of a second, strict table in the metrics tests, beside the
    /// fixture's `TABLE`.
    const STRICT_TABLE: &str = "strict";
    /// The refresh deadline of the tests that stall a refresh: short
    /// enough to keep the test quick.
    const TEST_REFRESH_DEADLINE: Duration = Duration::from_secs(1);
    /// The load deadline of the tests that stall a load: long enough for
    /// the fixtures' first load, and other than `TEST_REFRESH_DEADLINE`,
    /// so a test tells which deadline ended the work.
    const TEST_LOAD_DEADLINE: Duration = Duration::from_secs(2);

    /// A one-bucket fixture with one file of `FILE_KEYS` keys laid out as
    /// `layout` says, committed as `FIRST_SNAPSHOT`.
    async fn one_file_fixture(layout: WriterLayout) -> Fixture {
        let mut fixture = Fixture::bucketed(1);
        let keys = long_keys(&keys_from(0, FILE_KEYS));
        fixture
            .add_file(FileSpec {
                name: "f",
                keys: &keys,
                values: values_of(&keys_from(0, FILE_KEYS)),
                partition: Partition::Bucket(0),
                bounds: Bounds::Exact,
                layout,
            })
            .await;
        fixture
            .commit(FIRST_SNAPSHOT, None, FIRST_SEQUENCE, FIRST_VERSION)
            .await;
        fixture
    }

    /// A table following `fixture`'s main branch under `config`, with no
    /// local directory and the default load, refresh, and get deadlines,
    /// in `Loading`.
    async fn table_over(fixture: &Fixture, config: TableConfig) -> Table {
        let defaults = ServerConfig::default();
        table_within(
            fixture,
            config,
            defaults.load_deadline,
            defaults.refresh_deadline,
        )
        .await
    }

    /// `table_over` with `load_deadline` on every build from no generation
    /// and `refresh_deadline` on every build over one.
    async fn table_within(
        fixture: &Fixture,
        config: TableConfig,
        load_deadline: Duration,
        refresh_deadline: Duration,
    ) -> Table {
        let handles = handles_over(fixture).await;
        table_sharing(fixture, config, handles, load_deadline, refresh_deadline)
    }

    /// Handles over `fixture`'s storage, a fresh page cache, no row cache,
    /// so a lookup reads every value from its page, no local directory, and
    /// a count of calls in flight of their own.
    async fn handles_over(fixture: &Fixture) -> TableHandles {
        TableHandles {
            storage: Arc::clone(&fixture.storage),
            cache: cache().await,
            row_cache: None,
            local: None,
            calls: CallsInFlight::new(),
            findings: FindingLog::Events,
            batch_fan_out: DEFAULT_BATCH_FAN_OUT,
            max_multi_scan_values: ServerConfig::default().max_multi_scan_values,
            max_scan_bytes: ServerConfig::default().max_scan_bytes,
        }
    }

    /// A table following `fixture`'s main branch under `config`, built over
    /// `handles`, which other tables may share, in `Loading`.
    fn table_sharing(
        fixture: &Fixture,
        config: TableConfig,
        handles: TableHandles,
        load_deadline: Duration,
        refresh_deadline: Duration,
    ) -> Table {
        table_fetching_within(
            fixture,
            config,
            handles,
            [load_deadline, refresh_deadline],
            ServerConfig::default().get_deadline,
        )
    }

    /// `table_sharing` with its load and refresh deadlines in `builds`, in
    /// that order, and whose Gets end each page fetch at `get_deadline`.
    fn table_fetching_within(
        fixture: &Fixture,
        config: TableConfig,
        handles: TableHandles,
        [load_deadline, refresh_deadline]: [Duration; 2],
        get_deadline: Duration,
    ) -> Table {
        Table::new(
            config,
            TableSource::Static {
                location: fixture.root(),
            },
            handles,
            load_deadline,
            refresh_deadline,
            get_deadline,
            ServerConfig::default().scan_deadline,
        )
    }

    /// Replaces the store `fixture`'s storage reads `file://` locations
    /// through with one whose every GET waits `STALL`, as a wedged store
    /// does. Handles a loaded generation opened keep the store they were
    /// opened over.
    fn stall_reads(fixture: &Fixture) {
        let stalled = ThrottledStore::new(
            LocalFileSystem::new(),
            ThrottleConfig {
                wait_get_per_call: STALL,
                ..ThrottleConfig::default()
            },
        );
        fixture.storage.register(FILE_PREFIX, Arc::new(stalled));
    }

    /// Runs `work`, which a deadline should end, under `DEADLINE_GUARD`,
    /// and expects the error of the deadline `kind` names, for `TABLE`,
    /// after `deadline`.
    async fn ends_at_deadline<T: fmt::Debug>(
        work: impl Future<Output = Result<T>>,
        kind: ErrorKind,
        deadline: Duration,
    ) {
        let outcome = tokio::time::timeout(DEADLINE_GUARD, work)
            .await
            .expect("the work ends at its deadline, before the guard");
        let (table, after) = match outcome {
            Err(Error::LoadDeadline { table, after }) if kind == ErrorKind::LoadDeadline => {
                (table, after)
            }
            Err(Error::RefreshDeadline { table, after }) if kind == ErrorKind::RefreshDeadline => {
                (table, after)
            }
            other => panic!("expected {kind:?}, got {other:?}"),
        };
        assert_eq!(table, TABLE);
        assert_eq!(after, deadline);
    }

    /// Appends to `fixture` one file `name` in bucket 0 whose keys lie above
    /// every key of `all`, laid out as `layout` says, committed as
    /// `SECOND_SNAPSHOT`; returns the appended keys.
    async fn append_second(
        fixture: &mut Fixture,
        all: &[i64],
        name: &str,
        layout: WriterLayout,
    ) -> Vec<i64> {
        let appended = keys_in_bucket(
            &keys_from(all.last().unwrap() + KEY_STEP, E2E_KEYS),
            bucketing(TWO),
            0,
        );
        let keys = long_keys(&appended);
        fixture
            .add_file(FileSpec {
                name,
                keys: &keys,
                values: values_of(&appended),
                partition: Partition::Bucket(0),
                bounds: Bounds::Exact,
                layout,
            })
            .await;
        fixture
            .commit(
                SECOND_SNAPSHOT,
                Some(FIRST_SNAPSHOT),
                SECOND_SEQUENCE,
                SECOND_VERSION,
            )
            .await;
        appended
    }

    /// Adds to `fixture` a file `dup` in bucket 0 repeating two keys of
    /// `all` that bucket 0's file already holds, committed as `snapshot`
    /// over `parent`; returns the reason a load of it is refused for.
    async fn commit_duplicate(
        fixture: &mut Fixture,
        all: &[i64],
        snapshot: i64,
        parent: i64,
        sequence: i64,
        version: u64,
    ) -> RefusalReason {
        let in_bucket_zero = keys_in_bucket(all, bucketing(TWO), 0);
        fixture
            .add_long_file("dup", &in_bucket_zero[DUP_KEYS], 0)
            .await;
        fixture
            .commit(snapshot, Some(parent), sequence, version)
            .await;
        RefusalReason::DuplicateKeys {
            path: fixture.path_of("dup"),
            key: Key::Long(in_bucket_zero[DUP_KEYS.start]),
        }
    }

    fn present(key: i64) -> Row {
        testing::one_value(Bytes::from(value_for(key)))
    }

    /// The snapshot id a `Serving` state carries; any other state fails.
    fn serving_id(table: &Table) -> SnapshotId {
        let TableState::Serving { snapshot_id, .. } = table.state() else {
            panic!("expected Serving, got {:?}", table.state());
        };
        snapshot_id
    }

    /// The snapshots a `Held` state names, serving then refused, and the
    /// refused snapshot's level; any other state fails.
    fn held(table: &Table) -> (SnapshotId, SnapshotId, Level) {
        let TableState::Held {
            serving,
            refused,
            level,
            ..
        } = table.state()
        else {
            panic!("expected Held, got {:?}", table.state());
        };
        (serving, refused, level)
    }

    /// The `Held` state of a table serving `FIRST_SNAPSHOT` whose refresh
    /// to `SECOND_SNAPSHOT` was refused at balanced.
    const HELD_AT_BALANCED: (SnapshotId, SnapshotId, Level) = (
        SnapshotId(FIRST_SNAPSHOT),
        SnapshotId(SECOND_SNAPSHOT),
        Level::Balanced,
    );

    /// The fields of a `Degraded` state; any other state fails.
    fn degraded(table: &Table) -> (Option<SnapshotId>, ErrorKind) {
        let TableState::Degraded {
            serving, reason, ..
        } = table.state()
        else {
            panic!("expected Degraded, got {:?}", table.state());
        };
        (serving, reason)
    }

    /// A location no store serves, met as the table loads, is refused
    /// naming the table, which storage alone cannot.
    #[tokio::test]
    async fn test_load_names_the_table_of_a_location_no_store_serves() {
        let (fixture, _) = two_bucket_table().await;
        let defaults = ServerConfig::default();
        let table = Table::new(
            fixture.config(Residency::Full, None),
            TableSource::Static {
                location: "gs://bucket-a/db/kv".to_string(),
            },
            handles_over(&fixture).await,
            defaults.load_deadline,
            defaults.refresh_deadline,
            defaults.get_deadline,
            defaults.scan_deadline,
        );
        let error = table.load().await.unwrap_err();
        assert!(
            matches!(
                &error,
                Error::Location {
                    table: Some(name),
                    source: LocationError {
                        fault: LocationFault::NoStore,
                        ..
                    },
                } if name == TABLE
            ),
            "{error:?}"
        );
    }

    #[tokio::test]
    async fn test_get_before_load_is_not_ready() {
        let (fixture, all) = two_bucket_table().await;
        let table = table_over(&fixture, fixture.config(Residency::Full, None)).await;
        assert!(matches!(table.state(), TableState::Loading));
        assert!(table.current().is_none());
        let error = table
            .get(&Key::Long(all[0]), &[ColumnRead::whole(VALUE_COLUMN)])
            .await
            .unwrap_err();
        assert!(matches!(error, Error::NotReady { table: ref name } if name == TABLE));
        assert_eq!(fixture.gets(), 0);
    }

    #[tokio::test]
    async fn test_key_type_is_not_ready_before_load_then_the_generations() {
        let (fixture, _) = two_bucket_table().await;
        let table = table_over(&fixture, fixture.config(Residency::Full, None)).await;
        assert!(matches!(
            table.key_type(),
            Err(Error::NotReady { table: ref name }) if name == TABLE
        ));
        table.load().await.unwrap();
        assert_eq!(table.key_type().unwrap(), KeyType::Long);
        assert_eq!(
            table.key_type().unwrap(),
            table.current().unwrap().layout.key_type
        );
    }

    #[tokio::test]
    async fn test_resolve_layout_reads_the_metadata_alone_and_loads_nothing() {
        let (fixture, _) = two_bucket_table().await;
        let table = table_over(&fixture, fixture.config(Residency::Full, None)).await;
        let layout = table.resolve_layout().await.unwrap();
        assert_eq!(layout.key_type, KeyType::Long);
        assert_eq!(layout.order, None, "a key-value table has no order column");
        assert_eq!(fixture.gets(), RESOLVE_GETS, "no data file is read");
        assert!(table.current().is_none());
        assert!(matches!(table.state(), TableState::Loading));
    }

    #[tokio::test]
    async fn test_resolve_layout_gives_an_ordered_table_s_order_type_before_any_load() {
        let rows = ordered_rows(&[KEY_STEP], SCAN_ROWS_PER_KEY);
        let (ordered, _fixture, _gauged) =
            unloaded_ordered_table_over(&[&rows], WriterLayout::contract(), DEFAULT_BATCH_FAN_OUT)
                .await;
        let layout = ordered.resolve_layout().await.unwrap();
        assert_eq!(
            layout.order.map(|order| order.order_type),
            Some(crate::order::OrderType::Long)
        );
        assert!(ordered.current().is_none());
    }

    #[tokio::test]
    async fn test_load_serves_the_committed_snapshot() {
        let (fixture, all) = two_bucket_table().await;
        let table = table_over(&fixture, fixture.config(Residency::Full, None)).await;
        table.load().await.unwrap();
        let snapshot_id = serving_id(&table);
        assert_eq!(snapshot_id, SnapshotId(FIRST_SNAPSHOT));
        assert_eq!(table.current().unwrap().snapshot_id, snapshot_id);
        assert_eq!(
            table
                .get(&Key::Long(all[0]), &[ColumnRead::whole(VALUE_COLUMN)])
                .await
                .unwrap(),
            Lookup {
                row: present(all[0]),
                snapshot_id,
            }
        );
        assert_eq!(
            table
                .get(&Key::Long(all[0] + 1), &[ColumnRead::whole(VALUE_COLUMN)])
                .await
                .unwrap(),
            Lookup {
                row: Row::Absent,
                snapshot_id,
            }
        );
    }

    /// The daemon spawns every load, poll, and lookup onto a multi-thread
    /// runtime, and the gRPC service runs batches there too, so their
    /// futures are `Send`; `tokio::spawn` refuses one that is not at compile
    /// time.
    #[tokio::test]
    async fn test_load_refresh_get_and_multi_get_are_spawnable() {
        let (fixture, all) = two_bucket_table().await;
        let table = Arc::new(table_over(&fixture, fixture.config(Residency::Full, None)).await);
        let loaded = tokio::spawn({
            let table = Arc::clone(&table);
            async move { table.load().await }
        });
        loaded.await.unwrap().unwrap();
        let polled = tokio::spawn({
            let table = Arc::clone(&table);
            async move { table.refresh().await }
        });
        assert!(!polled.await.unwrap().unwrap(), "nothing moved");
        let batch = tokio::spawn({
            let table = Arc::clone(&table);
            let keys = vec![Key::Long(all[0])];
            async move {
                table
                    .multi_get(&keys, &[ColumnRead::whole(VALUE_COLUMN)])
                    .await
            }
        });
        assert_eq!(batch.await.unwrap().unwrap().rows, [present(all[0])]);
        let key = Key::Long(all[0]);
        let looked_up =
            tokio::spawn(async move { table.get(&key, &[ColumnRead::whole(VALUE_COLUMN)]).await });
        assert_eq!(looked_up.await.unwrap().unwrap().row, present(all[0]));
    }

    #[tokio::test]
    async fn test_get_refuses_a_key_of_another_type_before_any_read() {
        let (fixture, _) = two_bucket_table().await;
        let table = table_over(&fixture, fixture.config(Residency::Full, None)).await;
        table.load().await.unwrap();
        let before = fixture.gets();
        let error = table
            .get(
                &Key::Str("k".to_string()),
                &[ColumnRead::whole(VALUE_COLUMN)],
            )
            .await
            .unwrap_err();
        assert!(matches!(
            error,
            Error::KeyType {
                expected: KeyType::Long,
                got: KeyType::String,
                ..
            }
        ));
        assert_eq!(fixture.gets(), before);
    }

    #[tokio::test]
    async fn test_multi_get_answers_in_key_order_from_one_snapshot() {
        let (fixture, all) = two_bucket_table().await;
        let table = table_over(&fixture, fixture.config(Residency::Full, None)).await;
        table.load().await.unwrap();
        let keys = [Key::Long(all[0]), Key::Long(all[0] + 1), Key::Long(all[1])];
        assert_eq!(
            table
                .multi_get(&keys, &[ColumnRead::whole(VALUE_COLUMN)])
                .await
                .unwrap(),
            MultiLookup {
                rows: vec![present(all[0]), Row::Absent, present(all[1])],
                snapshot_id: SnapshotId(FIRST_SNAPSHOT),
            }
        );
    }

    #[tokio::test]
    async fn test_multi_get_runs_at_most_the_fan_out_bound_at_once_in_key_order() {
        fills_the_fan_out_bound_in_key_order(DEFAULT_BATCH_FAN_OUT).await;
    }

    /// A table built under a bound other than the default holds that one.
    #[tokio::test]
    async fn test_multi_get_under_a_configured_fan_out_holds_that_bound() {
        let configured = 4;
        fills_the_fan_out_bound_in_key_order(configured).await;
    }

    /// On several workers the batch is split over spawned runs, and the
    /// runs together hold the one bound.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_multi_get_on_several_workers_runs_at_most_the_fan_out_bound_in_key_order() {
        assert!(
            shard::workers() > 1,
            "the batch is split only over several workers"
        );
        fills_the_fan_out_bound_in_key_order(DEFAULT_BATCH_FAN_OUT).await;
    }

    /// A loaded table of `key_count` keys, one row per page, so every key
    /// is a cold hit on a page of its own and costs a GET no other key
    /// shares; its keys; and the gauge its storage reads through.
    async fn one_row_per_page(key_count: usize) -> (Table, Fixture, Vec<i64>, Arc<Gauged>) {
        let (fixture, all, gauged) = one_row_per_page_fixture(key_count).await;
        let table = table_over(&fixture, lenient(&fixture)).await;
        table.load().await.unwrap();
        (table, fixture, all, gauged)
    }

    /// `fixture`'s table in the Full tier with `strict` off: as the
    /// one-row-per-page tables are served, and wherever a test checks a
    /// refusal or a decode that the level gate would otherwise meet first.
    /// A duplicate key, for one, is only possible across overlapping files,
    /// which strict refuses as `below_level` before the merge that finds
    /// it.
    fn lenient(fixture: &Fixture) -> TableConfig {
        TableConfig {
            strict: false,
            ..fixture.config(Residency::Full, None)
        }
    }

    /// The fixture `one_row_per_page` serves, its keys, and the gauge its
    /// storage reads through.
    async fn one_row_per_page_fixture(key_count: usize) -> (Fixture, Vec<i64>, Arc<Gauged>) {
        let all = keys_from(0, i64::try_from(key_count).unwrap());
        let keys = long_keys(&all);
        let mut fixture = Fixture::bucketed(1);
        fixture
            .add_file(FileSpec {
                name: "f",
                keys: &keys,
                values: values_of(&all),
                partition: Partition::Bucket(0),
                bounds: Bounds::Exact,
                layout: WriterLayout {
                    page_rows: 1,
                    ..WriterLayout::contract()
                },
            })
            .await;
        fixture
            .commit(FIRST_SNAPSHOT, None, FIRST_SEQUENCE, FIRST_VERSION)
            .await;
        let gauged = Arc::new(Gauged::new(Arc::new(LocalFileSystem::new())));
        fixture
            .storage
            .register(FILE_PREFIX, Arc::<Gauged>::clone(&gauged));
        (fixture, all, gauged)
    }

    /// A batch of cold hits, each on a page of its own, on a table built
    /// under a fan-out of `bound`, reaches exactly `bound` GETs in flight at
    /// once and answers in key order.
    async fn fills_the_fan_out_bound_in_key_order(bound: usize) {
        let batches_of_the_bound = 3;
        let (fixture, all, gauged) = one_row_per_page_fixture(bound * batches_of_the_bound).await;
        let handles = TableHandles {
            batch_fan_out: bound,
            ..handles_over(&fixture).await
        };
        let defaults = ServerConfig::default();
        let table = table_sharing(
            &fixture,
            lenient(&fixture),
            handles,
            defaults.load_deadline,
            defaults.refresh_deadline,
        );
        table.load().await.unwrap();
        let keys = long_keys(&all);
        // A batch that never has the bound's lookups in flight at once
        // never passes the gate: its page fetches end at the get deadline
        // and the call fails.
        gauged.fill_to(bound);
        let answered = table
            .multi_get(&keys, &[ColumnRead::whole(VALUE_COLUMN)])
            .await
            .unwrap();
        let expected: Vec<Row> = all.iter().map(|key| present(*key)).collect();
        assert_eq!(answered.rows, expected);
        assert_eq!(
            gauged.high_water(),
            bound,
            "the batch fills the bound and never passes it"
        );
    }

    /// `one_row_per_page` with `DETAIL_COLUMN` beside the value column:
    /// every key's two values each on a page of its own.
    async fn two_columns_one_row_per_page(
        key_count: usize,
    ) -> (Table, Fixture, Vec<i64>, Arc<Gauged>) {
        two_column_table_of(
            i64::try_from(key_count).unwrap(),
            WriterLayout {
                page_rows: 1,
                ..WriterLayout::contract()
            },
        )
        .await
    }

    /// A loaded table of one file holding `DETAIL_COLUMN` beside the value
    /// column, `key_count` keys written under `layout`, its file read
    /// through a `Gauged` store, which passes every GET until a test gates
    /// it.
    async fn two_column_table_of(
        key_count: i64,
        layout: WriterLayout,
    ) -> (Table, Fixture, Vec<i64>, Arc<Gauged>) {
        let (fixture, all, gauged) = two_column_fixture(key_count, layout).await;
        let table = table_over(&fixture, lenient(&fixture)).await;
        table.load().await.unwrap();
        (table, fixture, all, gauged)
    }

    /// The fixture `two_column_table_of` serves, its keys, and the gauge
    /// its storage reads through.
    async fn two_column_fixture(
        key_count: i64,
        layout: WriterLayout,
    ) -> (Fixture, Vec<i64>, Arc<Gauged>) {
        let all = keys_from(0, key_count);
        let keys = long_keys(&all);
        let mut fixture = Fixture::with_detail(KeyType::Long, manifest_testing::bucket_spec(1));
        fixture
            .add_file(FileSpec {
                name: "f",
                keys: &keys,
                values: values_of(&all),
                partition: Partition::Bucket(0),
                bounds: Bounds::Exact,
                layout,
            })
            .await;
        fixture
            .commit(FIRST_SNAPSHOT, None, FIRST_SEQUENCE, FIRST_VERSION)
            .await;
        let gauged = Arc::new(Gauged::new(Arc::new(LocalFileSystem::new())));
        fixture
            .storage
            .register(FILE_PREFIX, Arc::<Gauged>::clone(&gauged));
        (fixture, all, gauged)
    }

    /// What a lookup naming the value column and then `DETAIL_COLUMN`
    /// answers for the fixture key `key`.
    fn both(key: i64) -> Row {
        let value = value_for(key);
        Row::Present(vec![
            Some(Bytes::from(detail_of(&value))),
            Some(Bytes::from(value)),
        ])
    }

    /// A batch naming two columns holds one permit per column a lookup
    /// reads, so it reaches exactly `DEFAULT_BATCH_FAN_OUT` value-page GETs in
    /// flight, half as many lookups, and never passes it (RFD 022).
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_multi_get_naming_two_columns_holds_the_fan_out_bound_of_gets() {
        let batches_of_the_bound = 3;
        let (table, _fixture, all, gauged) =
            two_columns_one_row_per_page(DEFAULT_BATCH_FAN_OUT * batches_of_the_bound).await;
        let keys = long_keys(&all);
        gauged.fill_to(DEFAULT_BATCH_FAN_OUT);
        let answered = table
            .multi_get(
                &keys,
                &[
                    ColumnRead::whole(DETAIL_COLUMN),
                    ColumnRead::whole(VALUE_COLUMN),
                ],
            )
            .await
            .unwrap();
        let expected: Vec<Row> = all.iter().map(|key| both(*key)).collect();
        assert_eq!(answered.rows, expected, "every key, both columns, in order");
        assert_eq!(
            gauged.high_water(),
            DEFAULT_BATCH_FAN_OUT,
            "the batch fills the bound of GETs and never passes it"
        );
    }

    /// The samples the hedging test fills its table's window with, so the
    /// delay, their percentile, is this.
    const FAST_SAMPLE: Duration = Duration::from_micros(1);

    /// A cold hit naming two columns hedges each column's GET on its own:
    /// its two value-page GETs are held until four GETs are in flight,
    /// which only a hedge beside each brings, and `value_page_gets_total`
    /// counts the two GETs and neither hedge (RFD 013, RFD 022).
    #[test]
    fn test_get_hedges_each_columns_get_on_its_own_and_counts_no_hedge() {
        let ((gets, hedges), series) = recorded(async {
            let mut fixture = Fixture::with_detail(KeyType::Long, manifest_testing::bucket_spec(1));
            let all = keys_from(0, FILE_KEYS);
            fixture
                .add_file(FileSpec {
                    name: "f",
                    keys: &long_keys(&all),
                    values: values_of(&all),
                    partition: Partition::Bucket(0),
                    bounds: Bounds::Exact,
                    layout: WriterLayout::contract(),
                })
                .await;
            fixture
                .commit(FIRST_SNAPSHOT, None, FIRST_SEQUENCE, FIRST_VERSION)
                .await;
            let gauged = Arc::new(Gauged::new(Arc::new(LocalFileSystem::new())));
            fixture
                .storage
                .register(FILE_PREFIX, Arc::<Gauged>::clone(&gauged));
            let table = table_over(&fixture, lenient(&fixture)).await;
            table.load().await.unwrap();
            for _ in 0..table.storage.hedge_config().window {
                table.hedge.record(FAST_SAMPLE, false);
            }
            let hedged_columns = 2;
            let gets_per_hedged_column = 2;
            gauged.fill_to(hedged_columns * gets_per_hedged_column);
            let stats = fixture.storage.stats();
            let before = (stats.gets(), stats.hedges());
            assert_eq!(
                table
                    .get(
                        &Key::Long(all[0]),
                        &[
                            ColumnRead::whole(DETAIL_COLUMN),
                            ColumnRead::whole(VALUE_COLUMN)
                        ]
                    )
                    .await
                    .unwrap()
                    .row,
                both(all[0])
            );
            table.publish_lookup_counts();
            (stats.gets() - before.0, stats.hedges() - before.1)
        });
        assert_eq!(hedges, 2, "one hedge per column's GET");
        assert_eq!(gets, 4, "two value-page GETs and their hedges");
        for column in [VALUE_COLUMN, DETAIL_COLUMN] {
            assert_eq!(
                series.counter(
                    metric::VALUE_PAGE_GETS_TOTAL,
                    &[table_label(), (metric::LABEL_COLUMN, column.to_string())]
                ),
                1,
                "{column}: its GET, not its hedge"
            );
        }
    }

    /// `keys` of `table` looked up as one run naming `columns` under a
    /// fan-out of `permits`, the bound `multi_get` gives a batch.
    async fn run_under(table: &Table, keys: &[Key], columns: &[&str], permits: usize) -> Vec<Row> {
        let generation = table.current().unwrap();
        let reads = whole(columns);
        let columns = generation
            .layout
            .select(
                &table.config(),
                check_column_reads(table.name(), &reads).unwrap(),
            )
            .unwrap();
        let fan_out = Arc::new(Semaphore::new(permits));
        table
            .lookups
            .run(&generation, keys, &columns, &fan_out)
            .await
            .unwrap()
    }

    /// Each GET takes a permit of its own and none waits holding one, so a
    /// lookup naming more columns than the fan-out has permits runs, never
    /// holding more GETs in flight than the permits; with a permit per
    /// column its GETs run together (RFD 022).
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_run_a_lookup_naming_more_columns_than_the_fan_out_stays_within_it() {
        let names = [DETAIL_COLUMN, VALUE_COLUMN];
        for permits in [1, names.len()] {
            let (table, _fixture, all, gauged) = two_columns_one_row_per_page(1).await;
            gauged.fill_to(permits);
            assert_eq!(
                run_under(&table, &long_keys(&all), &names, permits).await,
                [both(all[0])]
            );
            assert_eq!(gauged.high_water(), permits, "{permits} permits");
        }
    }

    /// A dictionary-encoded column's value page and dictionary page are two
    /// GETs, each holding a permit of its own: under one permit they run
    /// one after the other, under two together (RFD 022).
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_run_counts_a_dictionary_page_as_a_get_against_the_fan_out() {
        let pages = 2;
        for permits in [1, pages] {
            let (table, fixture, all, gauged) = two_column_table_of(
                1,
                WriterLayout {
                    page_rows: 1,
                    dictionary: Dictionary::Value,
                    ..WriterLayout::contract()
                },
            )
            .await;
            gauged.fill_to(permits);
            let before = fixture.gets();
            assert_eq!(
                run_under(&table, &long_keys(&all), &[VALUE_COLUMN], permits).await,
                [Row::Present(vec![Some(Bytes::from(value_for(all[0])))])]
            );
            assert_eq!(fixture.gets() - before, approx::count(pages));
            assert_eq!(gauged.high_water(), permits, "{permits} permits");
        }
    }

    /// Waits until `gauged` has held `gets` GETs in flight at once,
    /// failing past the get deadline, at which the held fetches would end
    /// and the calls that issued them with them.
    async fn reach(gauged: &Gauged, gets: usize) {
        tokio::time::timeout(ServerConfig::default().get_deadline, async {
            while gauged.high_water() < gets {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap_or_else(|_elapsed| panic!("{gets} GETs never reached the store at once"));
    }

    /// `get` of `key` naming the value column alone, failing past the get
    /// deadline: the shape of a call that must not wait behind a batch.
    async fn get_within_the_deadline(table: &Table, key: &Key) -> Row {
        tokio::time::timeout(
            ServerConfig::default().get_deadline,
            table.get(key, &[ColumnRead::whole(VALUE_COLUMN)]),
        )
        .await
        .expect("the get waits behind no batch")
        .unwrap()
        .row
    }

    /// Both columns of the two-column fixtures, the reads a batch over
    /// them names so that each lookup issues two GETs.
    const BOTH_READS: [ColumnRead<'static>; 2] = [
        ColumnRead::whole(DETAIL_COLUMN),
        ColumnRead::whole(VALUE_COLUMN),
    ];

    /// A `get` of a page whose batch lookup is still waiting for a permit
    /// issues its own GET rather than waiting behind the batch: the store
    /// holds the `DEFAULT_BATCH_FAN_OUT` GETs of the batch's first half until
    /// one more is in flight, which only the `get` brings (RFD 013). The
    /// store's high-water mark is then exactly one above the fan-out: the
    /// batch never holds more than the fan-out, and the `get` adds its one.
    /// The store's GET count is not asserted, since once the store
    /// releases, the batch issues its next GETs while the `get` returns.
    #[tokio::test]
    async fn test_get_of_a_page_a_batch_has_queued_is_not_held_behind_its_permits() {
        let (table, _fixture, all, gauged) =
            two_columns_one_row_per_page(DEFAULT_BATCH_FAN_OUT).await;
        let keys = long_keys(&all);
        let last = all[DEFAULT_BATCH_FAN_OUT - 1];
        gauged.fill_to(DEFAULT_BATCH_FAN_OUT + 1);
        let get_beside = async {
            reach(&gauged, DEFAULT_BATCH_FAN_OUT).await;
            get_within_the_deadline(&table, &Key::Long(last)).await
        };
        let (batch, row) = tokio::join!(table.multi_get(&keys, &BOTH_READS), get_beside);
        assert_eq!(row, present(last));
        assert_eq!(
            gauged.high_water(),
            DEFAULT_BATCH_FAN_OUT + 1,
            "the get's GET in flight beside the batch's"
        );
        let expected: Vec<Row> = all.iter().map(|key| both(*key)).collect();
        assert_eq!(batch.unwrap().rows, expected);
    }

    /// A batch dropped while half its lookups wait for permits issues no
    /// GET after the drop beyond the `DEFAULT_BATCH_FAN_OUT` it admitted: a
    /// `get` of its last page issues the one GET that releases the store,
    /// and once the admitted GETs have ended, which a batch of the admitted
    /// lookups waits for by joining them, nothing else has been issued.
    #[tokio::test]
    async fn test_multi_get_dropped_issues_no_get_beyond_those_admitted() {
        let (table, fixture, all, gauged) =
            two_columns_one_row_per_page(DEFAULT_BATCH_FAN_OUT).await;
        let keys = long_keys(&all);
        let admitted_lookups = DEFAULT_BATCH_FAN_OUT / BOTH_READS.len();
        gauged.fill_to(DEFAULT_BATCH_FAN_OUT + 1);
        let at_drop = tokio::select! {
            answered = table.multi_get(&keys, &BOTH_READS) => {
                panic!("the store holds every GET: {answered:?}")
            }
            () = reach(&gauged, DEFAULT_BATCH_FAN_OUT) => fixture.gets(),
        };
        let last = all[DEFAULT_BATCH_FAN_OUT - 1];
        assert_eq!(
            get_within_the_deadline(&table, &Key::Long(last)).await,
            present(last)
        );
        let admitted = table
            .multi_get(&keys[..admitted_lookups], &BOTH_READS)
            .await
            .unwrap();
        let expected: Vec<Row> = all[..admitted_lookups]
            .iter()
            .map(|key| both(*key))
            .collect();
        assert_eq!(admitted.rows, expected);
        assert_eq!(
            fixture.gets() - at_drop,
            1,
            "the get's GET and none of the dropped batch's"
        );
    }

    /// `value_page_gets_total` counts the GETs issued, where they are
    /// issued: a dictionary-encoded column's two, and a fetch two lookups
    /// share once, while both lookups are cold hits (RFD 022).
    #[test]
    fn test_get_counts_each_value_page_get_once_where_it_is_issued() {
        let (gets, series) = recorded(async {
            let (table, fixture, all, gauged) = two_column_table_of(
                FILE_KEYS,
                WriterLayout {
                    dictionary: Dictionary::Value,
                    ..WriterLayout::contract()
                },
            )
            .await;
            let before = fixture.gets();
            let both_reads = [
                ColumnRead::whole(DETAIL_COLUMN),
                ColumnRead::whole(VALUE_COLUMN),
            ];
            let key = Key::Long(all[0]);
            gauged.hold();
            let mut first = std::pin::pin!(table.get(&key, &both_reads));
            assert!(
                futures::poll!(&mut first).is_pending(),
                "the first lookup's GETs are held at the gate"
            );
            let mut second = std::pin::pin!(table.get(&key, &both_reads));
            assert!(
                futures::poll!(&mut second).is_pending(),
                "the second lookup joins the fetch in flight"
            );
            gauged.release();
            let (first, second) = futures::join!(first, second);
            assert_eq!(first.unwrap().row, both(all[0]));
            assert_eq!(second.unwrap().row, both(all[0]));
            table.publish_lookup_counts();
            fixture.gets() - before
        });
        assert_eq!(gets, 3, "detail's page, value's page and its dictionary");
        let counted = |column: &str| {
            series.counter(
                metric::VALUE_PAGE_GETS_TOTAL,
                &[table_label(), (metric::LABEL_COLUMN, column.to_string())],
            )
        };
        assert_eq!(counted(DETAIL_COLUMN), 1);
        assert_eq!(counted(VALUE_COLUMN), 2);
        assert_eq!(
            series.counter(
                metric::LOOKUPS_TOTAL,
                &[
                    table_label(),
                    (metric::LABEL_RESULT, LookupResult::ColdHit.to_string())
                ],
            ),
            2,
            "a joined fetch is a cold hit"
        );
    }

    /// A lookup reads the columns it names in the order it names them, one
    /// GET per column cold and none warm; naming none answers presence
    /// alone with no read; and each column's GETs are counted under its
    /// own `value_page_gets_total` (RFD 022).
    #[test]
    fn test_get_reads_the_columns_named_in_order_and_counts_their_gets() {
        let ((), series) = recorded(async {
            let (table, fixture, all, _gauged) =
                two_column_table_of(FILE_KEYS, WriterLayout::contract()).await;
            let key = Key::Long(all[0]);
            let before = fixture.gets();
            let read = table
                .get(
                    &key,
                    &[
                        ColumnRead::whole(DETAIL_COLUMN),
                        ColumnRead::whole(VALUE_COLUMN),
                    ],
                )
                .await
                .unwrap();
            assert_eq!(read.row, both(all[0]));
            assert_eq!(fixture.gets() - before, 2, "one GET per column cold");
            let Row::Present(mut reversed) = both(all[0]) else {
                unreachable!("both is present");
            };
            reversed.reverse();
            assert_eq!(
                table
                    .get(
                        &key,
                        &[
                            ColumnRead::whole(VALUE_COLUMN),
                            ColumnRead::whole(DETAIL_COLUMN)
                        ]
                    )
                    .await
                    .unwrap()
                    .row,
                Row::Present(reversed),
                "the other order answers the other order"
            );
            assert_eq!(
                table.get(&key, &[]).await.unwrap().row,
                Row::Present(Vec::new()),
                "presence alone"
            );
            assert_eq!(
                table.get(&Key::Long(all[0] + 1), &[]).await.unwrap().row,
                Row::Absent
            );
            assert_eq!(
                fixture.gets() - before,
                2,
                "warm hits and presence read nothing"
            );
            table.publish_lookup_counts();
        });
        for column in [VALUE_COLUMN, DETAIL_COLUMN] {
            assert_eq!(
                series.counter(
                    metric::VALUE_PAGE_GETS_TOTAL,
                    &[table_label(), (metric::LABEL_COLUMN, column.to_string())]
                ),
                1,
                "{column}"
            );
        }
        let lookups = |result: LookupResult| {
            series.counter(
                metric::LOOKUPS_TOTAL,
                &[table_label(), (metric::LABEL_RESULT, result.to_string())],
            )
        };
        assert_eq!(lookups(LookupResult::ColdHit), 1);
        assert_eq!(
            lookups(LookupResult::WarmHit),
            2,
            "a presence-only hit is warm"
        );
        assert_eq!(lookups(LookupResult::Miss), 1);
    }

    /// A column the table does not serve, a column named twice, a field
    /// list on a column not declared protobuf, and on a protobuf column a
    /// field number out of range or given twice are each refused before
    /// any read, on `get` and `multi_get` alike (RFD 022, RFD 024).
    #[tokio::test]
    async fn test_get_and_multi_get_refuse_each_column_request_fault_before_any_read() {
        let (fixture, all, _gauged) = two_column_fixture(FILE_KEYS, WriterLayout::contract()).await;
        let config = TableConfig {
            value_columns: vec![
                ValueColumnConfig::of(VALUE_COLUMN, ValueFormat::Protobuf),
                ValueColumnConfig::named(DETAIL_COLUMN),
            ],
            ..lenient(&fixture)
        };
        let table = table_over(&fixture, config).await;
        table.load().await.unwrap();
        let key = Key::Long(all[0]);
        let before = fixture.gets();
        let one = [KEY_FIELD];
        let zero = [0];
        let past_the_range = [MAX_FIELD_NUMBER + 1];
        let twice = [KEY_FIELD, FILLER_FIELD, KEY_FIELD];
        let cases = [
            (whole(&["absent"]), "absent", ColumnRequestFault::NotServed),
            (
                whole(&[VALUE_COLUMN, DETAIL_COLUMN, VALUE_COLUMN]),
                VALUE_COLUMN,
                ColumnRequestFault::Repeated,
            ),
            (
                vec![ColumnRead {
                    name: DETAIL_COLUMN,
                    fields: &one,
                }],
                DETAIL_COLUMN,
                ColumnRequestFault::NotProtobuf,
            ),
            (
                vec![ColumnRead {
                    name: VALUE_COLUMN,
                    fields: &zero,
                }],
                VALUE_COLUMN,
                ColumnRequestFault::FieldOutOfRange { field: 0 },
            ),
            (
                vec![ColumnRead {
                    name: VALUE_COLUMN,
                    fields: &past_the_range,
                }],
                VALUE_COLUMN,
                ColumnRequestFault::FieldOutOfRange {
                    field: MAX_FIELD_NUMBER + 1,
                },
            ),
            (
                vec![ColumnRead {
                    name: VALUE_COLUMN,
                    fields: &twice,
                }],
                VALUE_COLUMN,
                ColumnRequestFault::RepeatedField { field: KEY_FIELD },
            ),
        ];
        for (reads, column, fault) in cases {
            let refused = [
                table.get(&key, &reads).await.unwrap_err(),
                table
                    .multi_get(std::slice::from_ref(&key), &reads)
                    .await
                    .unwrap_err(),
            ];
            for error in refused {
                let Error::ColumnRequest {
                    column: named,
                    fault: got,
                    ..
                } = error
                else {
                    panic!("expected a column request fault, got {error:?}");
                };
                assert_eq!((named.as_str(), got), (column, fault));
            }
        }
        assert_eq!(fixture.gets(), before, "a refused request reads nothing");
    }

    /// One column dictionary-encoded beside one that is not: each column
    /// has its own level and cost, the table's level is the dearest
    /// column's, the findings name the column, and under `strict` the one
    /// column below the level refuses the table (RFD 020, RFD 022).
    #[tokio::test]
    async fn test_levels_and_findings_are_per_column() {
        let (table, _fixture, _all, _gauged) = two_column_table_of(
            FILE_KEYS,
            WriterLayout {
                dictionary: Dictionary::Value,
                ..WriterLayout::contract()
            },
        )
        .await;
        let generation = table.current().unwrap();
        assert_eq!(
            generation
                .layout
                .value_columns
                .iter()
                .map(|column| column.name.as_str())
                .collect::<Vec<_>>(),
            [DETAIL_COLUMN, VALUE_COLUMN],
            "the layout holds the columns by name"
        );
        assert_eq!(
            generation.column_costs,
            [
                ColumnCost {
                    level: Level::Optimal,
                    gets_per_hit: 1,
                },
                ColumnCost {
                    level: Level::Balanced,
                    gets_per_hit: 2,
                },
            ]
        );
        assert_eq!(generation.level, Level::Balanced, "the dearest column's");
        assert_eq!(generation.lookup_cost.gets_per_hit, 2);
        let report = check::diagnose(&generation, Level::Optimal, &ValueFormat::opaque, None);
        let dictionary: Vec<Option<&str>> = report
            .findings
            .iter()
            .filter(|finding| finding.code() == FindingCode::DictionaryEncodedValue)
            .map(Finding::column)
            .collect();
        assert_eq!(dictionary, [Some(VALUE_COLUMN)]);
        assert_eq!(
            generation
                .judge(Level::Optimal, true)
                .map_err(|error| error.kind()),
            Err(ErrorKind::Refused(RefusalReason::BelowLevel {
                declared: Level::Optimal,
                achieved: Level::Balanced,
                columns: vec![
                    crate::check::ColumnLevel {
                        column: DETAIL_COLUMN.to_string(),
                        level: Level::Optimal,
                    },
                    crate::check::ColumnLevel {
                        column: VALUE_COLUMN.to_string(),
                        level: Level::Balanced,
                    },
                ],
                findings: vec![Capping {
                    code: FindingCode::DictionaryEncodedValue,
                    column: Some(VALUE_COLUMN.to_string()),
                }],
            })),
            "one column below the level refuses the table under strict"
        );
    }

    /// The per-column series of a table of one dictionary-encoded column
    /// beside one that is not: `icetray_findings` carries the column for
    /// a column's code, and each column's level and cost gauges read its
    /// own, beside the table's (RFD 022).
    #[test]
    fn test_metrics_levels_findings_and_costs_are_per_column() {
        let ((), series) = recorded(async {
            let (table, _fixture, _all, _gauged) = two_column_table_of(
                FILE_KEYS,
                WriterLayout {
                    dictionary: Dictionary::Value,
                    ..WriterLayout::contract()
                },
            )
            .await;
            table.publish_gauges();
        });
        let column = |name: &str| (metric::LABEL_COLUMN, name.to_string());
        let gauge = |name: &str, labels: &[(&str, String)]| series.gauge(name, labels).to_bits();
        let dictionary = (
            metric::LABEL_CODE,
            FindingCode::DictionaryEncodedValue.to_string(),
        );
        let cases = [
            (
                metric::FINDINGS,
                vec![table_label(), dictionary.clone(), column(VALUE_COLUMN)],
                approx::gauge(1),
            ),
            (
                metric::FINDINGS,
                vec![table_label(), dictionary, column(DETAIL_COLUMN)],
                approx::gauge(0),
            ),
            (
                metric::TABLE_COLUMN_LEVEL,
                vec![table_label(), column(VALUE_COLUMN)],
                Level::Balanced.gauge(),
            ),
            (
                metric::TABLE_COLUMN_LEVEL,
                vec![table_label(), column(DETAIL_COLUMN)],
                Level::Optimal.gauge(),
            ),
            (
                metric::TABLE_LEVEL,
                vec![table_label()],
                Level::Balanced.gauge(),
            ),
            (
                metric::TABLE_COLUMN_GETS_PER_HIT,
                vec![table_label(), column(VALUE_COLUMN)],
                approx::gauge(2),
            ),
            (
                metric::TABLE_COLUMN_GETS_PER_HIT,
                vec![table_label(), column(DETAIL_COLUMN)],
                approx::gauge(1),
            ),
            (
                metric::TABLE_GETS_PER_HIT,
                vec![table_label()],
                approx::gauge(2),
            ),
        ];
        for (name, labels, expected) in cases {
            assert_eq!(
                gauge(name, &labels),
                expected.to_bits(),
                "{name} {labels:?}"
            );
        }
        let walked_only = FindingCode::MalformedProtobuf.to_string();
        let doctor_only = series
            .0
            .iter()
            .filter(|(key, ..)| {
                key.key().name() == metric::FINDINGS
                    && key.key().labels().any(|label| {
                        label.key() == metric::LABEL_CODE && label.value() == walked_only
                    })
            })
            .count();
        assert_eq!(doctor_only, 0, "no load raises the doctor's walk's code");
        assert!(series.named(metric::FINDINGS) > 0);
    }

    /// A table that serves no value column answers whether a key is
    /// present and reads no value page, at the level and cost of finding
    /// the row (RFD 022).
    #[tokio::test]
    async fn test_a_table_of_no_value_columns_answers_presence_alone() {
        let (fixture, all) = two_bucket_table().await;
        let config = TableConfig {
            value_columns: Vec::new(),
            ..fixture.config(Residency::Full, None)
        };
        let table = table_over(&fixture, config).await;
        table.load().await.unwrap();
        let generation = table.current().unwrap();
        assert!(generation.column_costs.is_empty());
        assert_eq!(generation.level, Level::Optimal);
        assert_eq!(
            generation.lookup_cost.gets_per_hit, 0,
            "finding the row alone"
        );
        let before = fixture.gets();
        let keys = [Key::Long(all[0]), Key::Long(all[0] + 1)];
        assert_eq!(
            table.multi_get(&keys, &[]).await.unwrap().rows,
            [Row::Present(Vec::new()), Row::Absent]
        );
        assert_eq!(fixture.gets(), before, "no value page is read");
        assert!(matches!(
            table
                .get(&keys[0], &[ColumnRead::whole(VALUE_COLUMN)])
                .await,
            Err(Error::ColumnRequest {
                fault: ColumnRequestFault::NotServed,
                ..
            })
        ));
        let report = check::diagnose(&generation, Level::Optimal, &ValueFormat::opaque, None);
        assert!(report.summary.value_columns.is_empty());
        assert!(
            report
                .files
                .iter()
                .all(|file| file.value_columns.is_empty())
        );
    }

    /// Workers of the runtime the split tests run on: enough that a batch
    /// splits into several runs, and that some calls in flight still leave
    /// it room to.
    const SPLIT_WORKERS: usize = 4;

    /// The name of the second table over a fixture, beside `TABLE`.
    const OTHER_TABLE: &str = "other";

    /// The holders of a serving generation's `Arc` while one batch reads
    /// it, beside the runs the batch spawned: the table's own, the
    /// batch's, and `runs_alive`'s read.
    const HOLDERS_BESIDE_THE_RUNS: usize = 3;

    /// The spawned runs of a `multi_get` or `multi_scan` in flight on
    /// `table` that are still alive: each holds the generation's `Arc`
    /// until it ends.
    fn runs_alive(table: &Table) -> usize {
        Arc::strong_count(&table.current().unwrap()) - HOLDERS_BESIDE_THE_RUNS
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_multi_get_splits_while_fewer_calls_than_workers_are_in_flight() {
        assert_eq!(shard::workers(), SPLIT_WORKERS);
        let idle = 0;
        assert_eq!(runs_spawned_beside(idle).await, SPLIT_WORKERS);
        let others_leaving_room = SPLIT_WORKERS - 2;
        assert_eq!(
            runs_spawned_beside(others_leaving_room).await,
            SPLIT_WORKERS,
            "with the batch, one fewer call in flight than workers"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_multi_get_runs_inline_once_as_many_calls_as_workers_are_in_flight() {
        assert_eq!(shard::workers(), SPLIT_WORKERS);
        let others_filling_the_workers = SPLIT_WORKERS - 1;
        assert_eq!(runs_spawned_beside(others_filling_the_workers).await, 0);
        let others_past_the_workers = SPLIT_WORKERS;
        assert_eq!(runs_spawned_beside(others_past_the_workers).await, 0);
    }

    /// Calls held in flight on one table hold a batch on another back from
    /// splitting when the two share their handles, as `open_with`'s tables
    /// do: every table's calls run on the same workers.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_multi_get_counts_the_calls_in_flight_on_every_table_sharing_its_handles() {
        assert_eq!(shard::workers(), SPLIT_WORKERS);
        let others_leaving_room = SPLIT_WORKERS - 2;
        assert_eq!(
            runs_spawned_beside_another_table(others_leaving_room).await,
            SPLIT_WORKERS,
            "with the batch, one fewer call in flight than workers"
        );
        let others_filling_the_workers = SPLIT_WORKERS - 1;
        assert_eq!(
            runs_spawned_beside_another_table(others_filling_the_workers).await,
            0
        );
    }

    /// The position, in `file_at`'s order, of the file a failing-batch test
    /// lays its first key out in, alone.
    const FIRST_KEY_FILE: usize = 0;
    /// The position of the file a failing-batch test lays its last key out
    /// in, alone; the keys between are in a file of their own between the
    /// two.
    const LAST_KEY_FILE: usize = 2;

    /// `keys` split as the failing-batch tests lay them out: the first, the
    /// ones between, and the last.
    fn first_between_last(keys: &[i64]) -> [&[i64]; 3] {
        let last = keys.len() - 1;
        [&keys[..1], &keys[1..last], &keys[last..]]
    }

    /// The name the fixtures of several files give their file at `at`:
    /// `first_and_last_key_on_their_own`'s and `ordered_table_over`'s.
    fn file_at(at: usize) -> String {
        format!("f{at}")
    }

    /// The path on disk of `fixture`'s file at `at`.
    fn on_disk(fixture: &Fixture, at: usize) -> String {
        fixture
            .path_of(&file_at(at))
            .strip_prefix(FILE_PREFIX)
            .unwrap()
            .to_string()
    }

    /// Holds every GET of `fixture`'s file at `at`, and of no other file.
    fn hold_file_at(fixture: &Fixture, gauged: &Gauged, at: usize) {
        let path = fixture.path_of(&file_at(at));
        gauged.hold_file(path.rsplit('/').next().unwrap());
    }

    /// Holds every GET of `FIRST_KEY_FILE` and removes it, and empties
    /// `LAST_KEY_FILE`, both after the load read them: the first key's
    /// read waits on the store and, once released, fails with its file
    /// gone, while the store fails the last key's read at once, its range
    /// past the end of an empty file.
    fn first_key_slow_last_key_fast(fixture: &Fixture, gauged: &Gauged) {
        hold_file_at(fixture, gauged, FIRST_KEY_FILE);
        std::fs::remove_file(on_disk(fixture, FIRST_KEY_FILE)).unwrap();
        std::fs::write(on_disk(fixture, LAST_KEY_FILE), []).unwrap();
    }

    /// Whether `error` is the page fetch of a file that is gone, as the
    /// first key's read fails after `first_key_slow_last_key_fast`; the
    /// last key's fails as an `object_store::Error::Generic`, the range
    /// refused.
    fn file_gone(error: &Error) -> bool {
        let Error::Cache(CacheError::Fetch(failed)) = error else {
            return false;
        };
        matches!(
            **failed,
            Error::ObjectStore(object_store::Error::NotFound { .. })
        )
    }

    /// Drives `call`, a batch over a table broken by
    /// `first_key_slow_last_key_fast`, until the store has failed the last
    /// key's read and every run but the first key's has ended, then
    /// releases the first key's read: the error the call answers, and the
    /// runs alive at the release, the first key's alone when the call
    /// spread and none when it read on its own task. A call that answers
    /// before the release answered the last key's failure, or the first
    /// key's at the table's fetch deadline: the held read fails as
    /// `Error::FetchDeadline` once that deadline passes, well inside
    /// `DEADLINE_GUARD`, so the fetch deadline is the wait's real bound.
    async fn first_key_fails_after_the_last<T: fmt::Debug>(
        table: &Table,
        gauged: &Gauged,
        call: impl Future<Output = Result<T>>,
    ) -> (Error, usize) {
        let mut call = std::pin::pin!(call);
        let spread_runs_left = 1;
        let last_failed = async {
            tokio::time::timeout(DEADLINE_GUARD, async {
                while gauged.failed() == 0 || runs_alive(table) > spread_runs_left {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .expect("the last key fails while the first waits on the store");
            runs_alive(table)
        };
        let (early, runs) = tokio::select! {
            biased;
            answered = call.as_mut() => (Some(answered), None),
            runs = last_failed => (None, Some(runs)),
        };
        gauged.release();
        let answered = match early {
            Some(answered) => answered,
            None => call.await,
        };
        let error = answered.expect_err("the first and the last key fail");
        let runs = runs.unwrap_or_else(|| {
            panic!(
                "answered before the release, the last key's failure or the first key's \
                 at its fetch deadline: {error:?}"
            )
        });
        (error, runs)
    }

    /// A loaded key-value table of `keys` laid out in three files of the
    /// one bucket as `first_between_last` splits them, whose Gets end each
    /// page fetch at `get_deadline`, and the store its storage reads
    /// through.
    async fn first_and_last_key_on_their_own(
        keys: &[i64],
        get_deadline: Duration,
    ) -> (Table, Fixture, Arc<Gauged>) {
        key_value_table_of_files(&first_between_last(keys), get_deadline).await
    }

    /// A loaded key-value table of one file per entry of `files`, each of
    /// its keys in order, in the one bucket and named `file_at` its
    /// position, whose Gets end each page fetch at `get_deadline`, and the
    /// store its storage reads through. Files' key ranges may overlap: the
    /// table is lenient.
    async fn key_value_table_of_files(
        files: &[&[i64]],
        get_deadline: Duration,
    ) -> (Table, Fixture, Arc<Gauged>) {
        let mut fixture = Fixture::bucketed(1);
        for (at, part) in files.iter().enumerate() {
            fixture
                .add_file(FileSpec {
                    name: &file_at(at),
                    keys: &long_keys(part),
                    values: values_of(part),
                    partition: Partition::Bucket(0),
                    bounds: Bounds::Exact,
                    layout: WriterLayout::contract(),
                })
                .await;
        }
        fixture
            .commit(FIRST_SNAPSHOT, None, FIRST_SEQUENCE, FIRST_VERSION)
            .await;
        let gauged = Arc::new(Gauged::new(Arc::new(LocalFileSystem::new())));
        fixture
            .storage
            .register(FILE_PREFIX, Arc::<Gauged>::clone(&gauged));
        let defaults = ServerConfig::default();
        let table = table_fetching_within(
            &fixture,
            lenient(&fixture),
            handles_over(&fixture).await,
            [defaults.load_deadline, defaults.refresh_deadline],
            get_deadline,
        );
        table.load().await.unwrap();
        (table, fixture, gauged)
    }

    /// The error a `MultiGet` answers whose first key fails after its last
    /// one, as `first_key_fails_after_the_last` drives it, while `others`
    /// calls are counted in flight, and the runs alive once the first
    /// key's read alone was left. Enough keys for a run on every worker of
    /// the split runtime, and few enough for one run's fan-out to read
    /// them all at once.
    async fn multi_get_failing_first_and_last(others: usize) -> (Error, usize) {
        let keys = keys_from(
            0,
            i64::try_from(MULTI_GET_TASK_KEYS_MIN * SPLIT_WORKERS).unwrap(),
        );
        assert!(
            keys.len() <= DEFAULT_BATCH_FAN_OUT,
            "one run reads every key at once"
        );
        let (table, fixture, gauged) =
            first_and_last_key_on_their_own(&keys, ServerConfig::default().get_deadline).await;
        let _others: Vec<InFlight<'_>> = (0..others).map(|_| table.lookups.enter()).collect();
        first_key_slow_last_key_fast(&fixture, &gauged);
        let asked = long_keys(&keys);
        let value = [ColumnRead::whole(VALUE_COLUMN)];
        first_key_fails_after_the_last(&table, &gauged, table.multi_get(&asked, &value)).await
    }

    /// A failing `MultiGet` answers the failure of its first failing key in
    /// key order, spread over idle workers as on its own task, though a
    /// later key in another run failed first: the first key's read is held
    /// until the last key has failed and every other run has ended.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_multi_get_answers_its_first_failing_key_spread_or_inline() {
        assert_eq!(shard::workers(), SPLIT_WORKERS);
        let idle = 0;
        let (error, runs) = multi_get_failing_first_and_last(idle).await;
        assert_eq!(runs, 1, "the first key read on a spawned run");
        assert!(file_gone(&error), "the first key's failure: {error:?}");
        let others_filling_the_workers = SPLIT_WORKERS - 1;
        let (error, runs) = multi_get_failing_first_and_last(others_filling_the_workers).await;
        assert_eq!(runs, 0, "the first key read on the call's task");
        assert!(file_gone(&error), "the first key's failure: {error:?}");
    }

    /// Whether `error` is the page fetch of a range past the end of its
    /// file, as the store fails a read of a file emptied after the load.
    fn range_refused(error: &Error) -> bool {
        let Error::Cache(CacheError::Fetch(failed)) = error else {
            return false;
        };
        matches!(
            **failed,
            Error::ObjectStore(object_store::Error::Generic { .. })
        )
    }

    /// The holders of a serving generation's `Arc` once a batch has
    /// returned, beside the runs it spawned that are still alive: the
    /// table's own and the read.
    const HOLDERS_ONCE_ANSWERED: usize = HOLDERS_BESIDE_THE_RUNS - 1;

    /// A spread `MultiGet` whose first key fails answers that failure
    /// though a later run cannot finish, and stops that run: the store
    /// holds every GET of the last key's file, and the Gets end no page
    /// fetch before `STALL`, so a call that waited on every run would wait
    /// past `DEADLINE_GUARD`, and the last key's run ends only when it is
    /// aborted.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_multi_get_answers_its_first_key_failing_at_once_and_stops_the_later_runs() {
        assert_eq!(shard::workers(), SPLIT_WORKERS);
        let keys = keys_from(
            0,
            i64::try_from(MULTI_GET_TASK_KEYS_MIN * SPLIT_WORKERS).unwrap(),
        );
        let (table, fixture, gauged) = first_and_last_key_on_their_own(&keys, STALL).await;
        hold_file_at(&fixture, &gauged, LAST_KEY_FILE);
        std::fs::write(on_disk(&fixture, FIRST_KEY_FILE), []).unwrap();
        let value = [ColumnRead::whole(VALUE_COLUMN)];
        let answered =
            tokio::time::timeout(DEADLINE_GUARD, table.multi_get(&long_keys(&keys), &value))
                .await
                .expect(
                    "the call answers its first key's failure though a later run cannot finish",
                );
        let error = answered.expect_err("the first key's file is empty");
        assert!(range_refused(&error), "the first key's failure: {error:?}");
        tokio::time::timeout(DEADLINE_GUARD, async {
            while Arc::strong_count(&table.current().unwrap()) > HOLDERS_ONCE_ANSWERED {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("the run of the last key, held on the store, is stopped");
        gauged.release();
    }

    /// A spread `MultiGet` whose middle run fails while its first run is
    /// still reading stops the runs after the failure at once, though it
    /// holds the failure until the first run ends: the first run's first
    /// key and the last run's first key are alone in a file the store
    /// holds, every other key is in a file emptied after the load, and the
    /// Gets end no page fetch before `STALL`. Each run reads its keys in
    /// order, so the first and the last run wait on the held file, and the
    /// middle runs fail. Only the abort ends the last run before the
    /// release.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_multi_get_stops_the_runs_after_a_failure_held_behind_an_earlier_run() {
        assert_eq!(shard::workers(), SPLIT_WORKERS);
        let per_run = MULTI_GET_TASK_KEYS_MIN;
        let keys = keys_from(0, i64::try_from(per_run * SPLIT_WORKERS).unwrap());
        assert!(
            keys.len() <= DEFAULT_BATCH_FAN_OUT,
            "one run reads every key at once"
        );
        let last_run_at = per_run * (SPLIT_WORKERS - 1);
        let held = [keys[0], keys[last_run_at]];
        let emptied: Vec<i64> = keys
            .iter()
            .copied()
            .filter(|key| !held.contains(key))
            .collect();
        let held_file = 0;
        let emptied_file = 1;
        let (table, fixture, gauged) = key_value_table_of_files(&[&held, &emptied], STALL).await;
        hold_file_at(&fixture, &gauged, held_file);
        std::fs::write(on_disk(&fixture, emptied_file), []).unwrap();
        let value = [ColumnRead::whole(VALUE_COLUMN)];
        let asked = long_keys(&keys);
        let mut call = std::pin::pin!(table.multi_get(&asked, &value));
        let first_run_alone = 1;
        let later_runs_stopped = async {
            tokio::time::timeout(DEADLINE_GUARD, async {
                while gauged.failed() == 0 || runs_alive(&table) > first_run_alone {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .expect("the runs after the middle run's failure are stopped");
        };
        tokio::select! {
            biased;
            answered = call.as_mut() => panic!("the first run is held on the store: {answered:?}"),
            () = later_runs_stopped => {}
        }
        gauged.release();
        let error = tokio::time::timeout(DEADLINE_GUARD, call)
            .await
            .expect("the first run ends once released")
            .expect_err("every key but two is in an emptied file");
        assert!(range_refused(&error), "the first run's failure: {error:?}");
    }

    /// The runs a batch of `DEFAULT_BATCH_FAN_OUT` cold hits on one table
    /// spawns while `others` cold-hit `get`s are in flight on another table
    /// over the same handles and file, each on a page of its own. The store
    /// holds every GET of both, so the `get`s stay in flight while the
    /// batch starts; each spawned run holds the batch's generation's `Arc`,
    /// so the holders beyond the batch table's own, the batch's, and this
    /// read's are the runs.
    async fn runs_spawned_beside_another_table(others: usize) -> usize {
        let (fixture, all, gauged) = one_row_per_page_fixture(DEFAULT_BATCH_FAN_OUT + others).await;
        let handles = handles_over(&fixture).await;
        let defaults = ServerConfig::default();
        let batch_table = table_sharing(
            &fixture,
            lenient(&fixture),
            handles.clone(),
            defaults.load_deadline,
            defaults.refresh_deadline,
        );
        let other = Arc::new(table_sharing(
            &fixture,
            TableConfig {
                name: OTHER_TABLE.to_string(),
                ..lenient(&fixture)
            },
            handles,
            defaults.load_deadline,
            defaults.refresh_deadline,
        ));
        batch_table.load().await.unwrap();
        other.load().await.unwrap();
        let (batch, beside) = all.split_at(DEFAULT_BATCH_FAN_OUT);
        let keys = long_keys(batch);
        // One GET per key and no hedge, since neither table has recorded a
        // lookup GET to take a delay from, so one more GET than both ask
        // is never reached and the store holds every one until the test
        // drops them.
        gauged.fill_to(DEFAULT_BATCH_FAN_OUT + others + 1);
        let mut held = JoinSet::new();
        for key in long_keys(beside) {
            let other = Arc::clone(&other);
            held.spawn(async move { other.get(&key, &[ColumnRead::whole(VALUE_COLUMN)]).await });
        }
        reach(&gauged, others).await;
        let every_get_held = async {
            reach(&gauged, others + DEFAULT_BATCH_FAN_OUT).await;
            runs_alive(&batch_table)
        };
        let value = [ColumnRead::whole(VALUE_COLUMN)];
        tokio::select! {
            answered = batch_table.multi_get(&keys, &value) => panic!("the store holds every GET: {answered:?}"),
            spawned = every_get_held => spawned,
        }
    }

    /// The runs a batch of `DEFAULT_BATCH_FAN_OUT` cold hits spawns while
    /// `others` calls are counted in flight on its table, which the test
    /// holds directly rather than running them. Read while the store holds
    /// every GET of the batch: each spawned run holds the generation's
    /// `Arc`, so the holders beyond the table's own, the batch's, and this
    /// read's are the runs.
    async fn runs_spawned_beside(others: usize) -> usize {
        let (table, _fixture, all, gauged) = one_row_per_page(DEFAULT_BATCH_FAN_OUT).await;
        let keys = long_keys(&all);
        let _others: Vec<InFlight<'_>> = (0..others).map(|_| table.lookups.enter()).collect();
        // The batch issues one GET per key and no hedge, since its table
        // has recorded no lookup GET to take a delay from, so one more GET
        // than it has keys is never reached and the store holds every one
        // until the batch is dropped.
        gauged.fill_to(DEFAULT_BATCH_FAN_OUT + 1);
        let every_get_held = async {
            reach(&gauged, DEFAULT_BATCH_FAN_OUT).await;
            runs_alive(&table)
        };
        let value = [ColumnRead::whole(VALUE_COLUMN)];
        tokio::select! {
            answered = table.multi_get(&keys, &value) => panic!("the store holds every GET: {answered:?}"),
            spawned = every_get_held => spawned,
        }
    }

    #[tokio::test]
    async fn test_multi_get_refuses_a_mistyped_key_anywhere_and_issues_no_get() {
        let (fixture, all) = two_bucket_table().await;
        let table = table_over(&fixture, fixture.config(Residency::Full, None)).await;
        table.load().await.unwrap();
        let before = fixture.gets();
        let keys = [Key::Long(all[2]), Key::Str("k".to_string())];
        let error = table
            .multi_get(&keys, &[ColumnRead::whole(VALUE_COLUMN)])
            .await
            .unwrap_err();
        assert!(matches!(
            error,
            Error::KeyType {
                expected: KeyType::Long,
                got: KeyType::String,
                ..
            }
        ));
        assert_eq!(fixture.gets(), before);
    }

    #[tokio::test]
    async fn test_load_refused_leaves_the_table_degraded_with_no_generation() {
        let (fixture, all) = two_bucket_table().await;
        let table = table_over(&fixture, fixture.config(Residency::Full, Some(TINY_BUDGET))).await;
        let error = table.load().await.unwrap_err();
        let Error::Refused {
            reason: reason @ RefusalReason::OverBudget { .. },
            ..
        } = error
        else {
            panic!("expected an over-budget refusal, got {error:?}");
        };
        let (serving, kept) = degraded(&table);
        assert_eq!(serving, None);
        assert_eq!(kept, ErrorKind::Refused(reason));
        assert!(table.current().is_none());
        let error = table
            .get(&Key::Long(all[0]), &[ColumnRead::whole(VALUE_COLUMN)])
            .await
            .unwrap_err();
        assert!(matches!(error, Error::NotReady { .. }));
    }

    #[tokio::test]
    async fn test_load_strict_refuses_a_snapshot_below_its_level_and_only_that() {
        let balanced = WriterLayout {
            dictionary: Dictionary::Value,
            ..WriterLayout::contract()
        };
        let fixture = one_file_fixture(balanced).await;
        let strict = table_over(&fixture, fixture.config(Residency::Full, None)).await;
        assert!(strict.config().strict);
        let reason = refusal_of(strict.load().await);
        assert_eq!(reason, dictionary_below_optimal());
        assert!(strict.current().is_none());
        let (serving, kept) = degraded(&strict);
        assert_eq!(serving, None);
        assert_eq!(kept, ErrorKind::Refused(reason));

        let accepting = table_over(
            &fixture,
            TableConfig {
                lookups: Some(Level::Balanced),
                ..fixture.config(Residency::Full, None)
            },
        )
        .await;
        accepting.load().await.unwrap();
        assert_eq!(serving_id(&accepting), SnapshotId(FIRST_SNAPSHOT));

        let lenient = table_over(&fixture, lenient(&fixture)).await;
        lenient.load().await.unwrap();
        assert_eq!(serving_id(&lenient), SnapshotId(FIRST_SNAPSHOT));

        let advice_only = WriterLayout {
            bloom: Bloom::Off,
            ..WriterLayout::contract()
        };
        let fixture = one_file_fixture(advice_only).await;
        let table = table_over(&fixture, fixture.config(Residency::Full, None)).await;
        table.load().await.unwrap();
        assert_eq!(serving_id(&table), SnapshotId(FIRST_SNAPSHOT));
    }

    #[tokio::test]
    async fn test_load_fails_at_the_load_deadline_and_releases_the_lock() {
        let (fixture, _) = two_bucket_table().await;
        stall_reads(&fixture);
        let table = table_within(
            &fixture,
            fixture.config(Residency::Full, None),
            TEST_LOAD_DEADLINE,
            TEST_REFRESH_DEADLINE,
        )
        .await;
        ends_at_deadline(table.load(), ErrorKind::LoadDeadline, TEST_LOAD_DEADLINE).await;
        assert_eq!(degraded(&table), (None, ErrorKind::LoadDeadline));
        // The load dropped its lock with its work: the next poll takes the
        // lock and, with no generation serving, builds from nothing under
        // the load deadline too.
        ends_at_deadline(table.refresh(), ErrorKind::LoadDeadline, TEST_LOAD_DEADLINE).await;
        assert_eq!(degraded(&table), (None, ErrorKind::LoadDeadline));
    }

    #[tokio::test]
    async fn test_refresh_fails_at_the_refresh_deadline_and_keeps_serving() {
        let (fixture, all) = two_bucket_table().await;
        let table = table_within(
            &fixture,
            fixture.config(Residency::Full, None),
            TEST_LOAD_DEADLINE,
            TEST_REFRESH_DEADLINE,
        )
        .await;
        table.load().await.unwrap();
        stall_reads(&fixture);
        ends_at_deadline(
            table.refresh(),
            ErrorKind::RefreshDeadline,
            TEST_REFRESH_DEADLINE,
        )
        .await;
        let serving = Some(SnapshotId(FIRST_SNAPSHOT));
        assert_eq!(degraded(&table), (serving, ErrorKind::RefreshDeadline));
        assert_eq!(
            table
                .get(&Key::Long(all[0]), &[ColumnRead::whole(VALUE_COLUMN)])
                .await
                .unwrap()
                .row,
            present(all[0]),
            "the generation loaded before the stall keeps answering"
        );
        ends_at_deadline(
            table.refresh(),
            ErrorKind::RefreshDeadline,
            TEST_REFRESH_DEADLINE,
        )
        .await;
        assert_eq!(degraded(&table), (serving, ErrorKind::RefreshDeadline));
    }

    #[tokio::test]
    async fn test_refresh_unchanged_is_false_at_one_get() {
        let (fixture, _) = two_bucket_table().await;
        let table = table_over(&fixture, fixture.config(Residency::Full, None)).await;
        table.load().await.unwrap();
        let before = fixture.gets();
        assert!(!table.refresh().await.unwrap());
        assert_eq!(fixture.gets(), before + POLL_GETS);
        assert_eq!(serving_id(&table), SnapshotId(FIRST_SNAPSHOT));
        assert_eq!(
            table.current().unwrap().snapshot_id,
            SnapshotId(FIRST_SNAPSHOT)
        );
    }

    #[tokio::test]
    async fn test_refresh_swaps_in_a_commit_reusing_unchanged_files() {
        let (mut fixture, all) = two_bucket_table().await;
        let table = table_over(&fixture, fixture.config(Residency::Full, None)).await;
        table.load().await.unwrap();
        let first = table.current().unwrap();
        let appended = append_second(&mut fixture, &all, "f2", WriterLayout::contract()).await;
        assert!(table.refresh().await.unwrap());
        let second = table.current().unwrap();
        assert_eq!(second.snapshot_id, SnapshotId(SECOND_SNAPSHOT));
        assert_eq!(second.files.len(), first.files.len() + 1);
        for old in &first.files {
            assert!(
                second.files.iter().any(|file| Arc::ptr_eq(file, old)),
                "{} is reused by pointer",
                old.entry.path
            );
        }
        assert_eq!(serving_id(&table), SnapshotId(SECOND_SNAPSHOT));
        assert_eq!(
            table
                .get(&Key::Long(appended[0]), &[ColumnRead::whole(VALUE_COLUMN)])
                .await
                .unwrap(),
            Lookup {
                row: present(appended[0]),
                snapshot_id: SnapshotId(SECOND_SNAPSHOT),
            }
        );
    }

    #[tokio::test]
    async fn test_refresh_refused_keeps_the_previous_generation_then_recovers() {
        let (mut fixture, all) = two_bucket_table().await;
        let table = table_over(&fixture, lenient(&fixture)).await;
        table.load().await.unwrap();
        let reason = commit_duplicate(
            &mut fixture,
            &all,
            SECOND_SNAPSHOT,
            FIRST_SNAPSHOT,
            SECOND_SEQUENCE,
            SECOND_VERSION,
        )
        .await;
        let error = table.refresh().await.unwrap_err();
        assert!(matches!(error, Error::Refused { reason: ref got, .. } if *got == reason));
        assert_eq!(
            table.current().unwrap().snapshot_id,
            SnapshotId(FIRST_SNAPSHOT)
        );
        assert_eq!(
            table
                .get(&Key::Long(all[0]), &[ColumnRead::whole(VALUE_COLUMN)])
                .await
                .unwrap(),
            Lookup {
                row: present(all[0]),
                snapshot_id: SnapshotId(FIRST_SNAPSHOT),
            }
        );
        let (serving, kept) = degraded(&table);
        assert_eq!(serving, Some(SnapshotId(FIRST_SNAPSHOT)));
        assert_eq!(kept, ErrorKind::Refused(reason.clone()));

        fixture.remove_file("dup");
        fixture
            .commit(
                THIRD_SNAPSHOT,
                Some(SECOND_SNAPSHOT),
                THIRD_SEQUENCE,
                THIRD_VERSION,
            )
            .await;
        assert!(table.refresh().await.unwrap());
        assert_eq!(serving_id(&table), SnapshotId(THIRD_SNAPSHOT));
        assert_eq!(
            table.current().unwrap().snapshot_id,
            SnapshotId(THIRD_SNAPSHOT)
        );
    }

    /// A refresh onto a new data file that cannot be read fails transiently,
    /// and the next poll builds the snapshot again rather than remembering
    /// the failure; once the file reads, the snapshot is swapped in.
    #[tokio::test]
    async fn test_refresh_tries_a_transient_failure_again_on_the_next_poll() {
        let (mut fixture, all) = two_bucket_table().await;
        let table = table_over(&fixture, fixture.config(Residency::Full, None)).await;
        table.load().await.unwrap();
        append_second(&mut fixture, &all, "f2", WriterLayout::contract()).await;
        let location = fixture.path_of("f2");
        let on_disk = location.strip_prefix(FILE_PREFIX).unwrap();
        let written = std::fs::read(on_disk).unwrap();
        std::fs::remove_file(on_disk).unwrap();

        let unreadable = |error: &Error| {
            matches!(
                error,
                Error::ObjectStore(object_store::Error::NotFound { .. })
            )
        };
        let error = table.refresh().await.unwrap_err();
        assert!(unreadable(&error), "{error:?}");
        assert_eq!(error.recurrence(), Recurrence::Transient);
        let before = fixture.gets();
        let error = table.refresh().await.unwrap_err();
        assert!(unreadable(&error), "{error:?}");
        assert!(
            fixture.gets() > before + POLL_GETS,
            "the poll read the snapshot's metadata and manifests again"
        );
        assert_eq!(
            degraded(&table),
            (Some(SnapshotId(FIRST_SNAPSHOT)), ErrorKind::ObjectStore)
        );

        std::fs::write(on_disk, written).unwrap();
        assert!(table.refresh().await.unwrap());
        assert_eq!(serving_id(&table), SnapshotId(SECOND_SNAPSHOT));
    }

    /// A key chunk changed on disk under a Disk-tier table is a file fault
    /// of the local directory's copy, not of the data file: the overlap
    /// check meets it reading the chunk back, the next poll builds the
    /// snapshot again rather than remembering the fault, and once the chunk
    /// file holds the data file's bytes again the snapshot is swapped in.
    #[tokio::test]
    async fn test_refresh_tries_a_file_fault_of_a_local_chunk_file_again_on_the_next_poll() {
        let mut fixture = Fixture::bucketed(1);
        let first = keys_from(0, FILE_KEYS);
        fixture.add_long_file("f0", &first, 0).await;
        fixture
            .commit(FIRST_SNAPSHOT, None, FIRST_SEQUENCE, FIRST_VERSION)
            .await;
        let scratch = tempfile::tempdir().unwrap();
        let local = local_dir(scratch.path(), fixture.storage.stats(), None);
        let handles = TableHandles {
            local: Some(Arc::clone(&local)),
            ..handles_over(&fixture).await
        };
        // Overlapping files cap the level at permissive, which strict refuses.
        let config = TableConfig {
            residency: Residency::Disk,
            ..lenient(&fixture)
        };
        let defaults = ServerConfig::default();
        let table = table_sharing(
            &fixture,
            config,
            handles,
            defaults.load_deadline,
            defaults.refresh_deadline,
        );
        table.load().await.unwrap();
        let identity = table
            .current()
            .unwrap()
            .identities()
            .into_iter()
            .next()
            .unwrap();
        let chunk_file = local.chunk_path(&identity, ChunkRules::KeyValue);
        let at = chunks_offset(&chunk_file);
        let held = std::fs::read(&chunk_file).unwrap()[approx::length(at)..].to_vec();
        let more_than_the_file_holds = i32::try_from(FILE_KEYS + 1).unwrap();
        overwrite(
            &chunk_file,
            at,
            &with_first_page_claiming(&held, more_than_the_file_holds),
        );
        let interleaved: Vec<i64> = first.iter().map(|key| key + 1).collect();
        fixture.add_long_file("f1", &interleaved, 0).await;
        fixture
            .commit(
                SECOND_SNAPSHOT,
                Some(FIRST_SNAPSHOT),
                SECOND_SEQUENCE,
                SECOND_VERSION,
            )
            .await;

        let changed = fixture.path_of("f0");
        let of_the_copy = |error: &Error| {
            matches!(
                error,
                Error::File {
                    path,
                    fault: FileFault::PageRowsMismatch { .. },
                } if *path == changed
            )
        };
        let error = table.refresh().await.unwrap_err();
        assert!(of_the_copy(&error), "{error:?}");
        assert_eq!(error.recurrence(), Recurrence::Transient);
        let before = fixture.gets();
        let error = table.refresh().await.unwrap_err();
        assert!(of_the_copy(&error), "{error:?}");
        assert!(
            fixture.gets() > before + POLL_GETS,
            "the poll read the snapshot's metadata and manifests again"
        );
        assert_eq!(
            degraded(&table),
            (Some(SnapshotId(FIRST_SNAPSHOT)), ErrorKind::File)
        );

        overwrite(&chunk_file, at, &held);
        assert!(table.refresh().await.unwrap());
        assert_eq!(serving_id(&table), SnapshotId(SECOND_SNAPSHOT));
    }

    /// String keys for `ids`, each written `repeats` times in a row.
    fn string_keys(ids: Range<usize>, repeats: usize) -> Vec<Key> {
        ids.flat_map(|id| std::iter::repeat_n(Key::Str(format!("key:{id:06}")), repeats))
            .collect()
    }

    /// An unpartitioned contract-layout file of `keys` with `dictionary`
    /// encoding, the value at each position `value_for` of it.
    fn string_file<'a>(name: &'a str, keys: &'a [Key], dictionary: Dictionary) -> FileSpec<'a> {
        FileSpec {
            name,
            keys,
            values: (0..keys.len())
                .map(|at| Some(value_for(i64::try_from(at).unwrap())))
                .collect(),
            partition: Partition::None,
            bounds: Bounds::Exact,
            layout: WriterLayout {
                dictionary,
                ..WriterLayout::contract()
            },
        }
    }

    /// A refresh onto a file parquet panics decoding, a string key
    /// dictionary counting one entry more than its body holds, fails with
    /// `FileFault::DecoderPanicked` instead of panicking the task that runs
    /// it, and the previous generation keeps serving.
    #[tokio::test]
    async fn test_refresh_onto_a_file_parquet_panics_decoding_keeps_serving() {
        let file_keys = usize::try_from(FILE_KEYS).unwrap();
        let mut fixture = Fixture::new(KeyType::String, manifest_testing::unpartitioned());
        let served = string_keys(0..file_keys, 1);
        fixture
            .add_file(string_file("f", &served, Dictionary::Off))
            .await;
        fixture
            .commit(FIRST_SNAPSHOT, None, FIRST_SEQUENCE, FIRST_VERSION)
            .await;
        // Two unbucketed files cap the table at permissive, which strict
        // refuses as below_level before any key page is decoded.
        let table = table_over(&fixture, lenient(&fixture)).await;
        table.load().await.unwrap();

        let repeats = 2;
        let repeated = string_keys(file_keys..2 * file_keys, repeats);
        let short = string_file("short", &repeated, Dictionary::Key);
        let written = parquet(
            KeyType::String,
            short.keys,
            &short.values,
            false,
            &short.layout,
        );
        fixture.add_file(short).await;
        let key_leaf = 0;
        let (patched, _) = with_dictionary_one_entry_short(&written, key_leaf);
        let path = fixture.path_of("short");
        manifest_testing::write_file(&FileIO::new_with_fs(), &path, patched).await;
        fixture
            .commit(
                SECOND_SNAPSHOT,
                Some(FIRST_SNAPSHOT),
                SECOND_SEQUENCE,
                SECOND_VERSION,
            )
            .await;

        let error = table.refresh().await.unwrap_err();
        assert!(
            matches!(
                error,
                Error::File {
                    path: ref at,
                    fault: FileFault::DecoderPanicked {
                        part: DecodedPart::Pages { row_group: 0, .. },
                        ..
                    },
                } if *at == path
            ),
            "{error:?}"
        );
        assert_eq!(
            table.current().unwrap().snapshot_id,
            SnapshotId(FIRST_SNAPSHOT)
        );
        let (serving, kept) = degraded(&table);
        assert_eq!(serving, Some(SnapshotId(FIRST_SNAPSHOT)));
        assert_eq!(kept, ErrorKind::File);
        let first = served[0].clone();
        assert_eq!(
            table
                .get(&first, &[ColumnRead::whole(VALUE_COLUMN)])
                .await
                .unwrap(),
            Lookup {
                row: present(0),
                snapshot_id: SnapshotId(FIRST_SNAPSHOT),
            }
        );
    }

    /// The refusal a strict table declared `optimal` meets on a snapshot
    /// whose value column is dictionary-encoded.
    fn dictionary_below_optimal() -> RefusalReason {
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
    }

    /// Appends to `fixture` a file `dict` in bucket 0 whose value column is
    /// dictionary-encoded and whose keys interleave with bucket 0's file's,
    /// committed as `SECOND_SNAPSHOT`.
    async fn append_dictionary_overlap(fixture: &mut Fixture) {
        add_dictionary_overlap(fixture).await;
        fixture
            .commit(
                SECOND_SNAPSHOT,
                Some(FIRST_SNAPSHOT),
                SECOND_SEQUENCE,
                SECOND_VERSION,
            )
            .await;
    }

    /// Records in `fixture` the file `append_dictionary_overlap` commits,
    /// for the next snapshot.
    async fn add_dictionary_overlap(fixture: &mut Fixture) {
        let interleaved = keys_in_bucket(&keys_from(1, E2E_KEYS), bucketing(TWO), 0);
        let keys = long_keys(&interleaved);
        fixture
            .add_file(FileSpec {
                name: "dict",
                keys: &keys,
                values: values_of(&interleaved),
                partition: Partition::Bucket(0),
                bounds: Bounds::Exact,
                layout: WriterLayout {
                    dictionary: Dictionary::Value,
                    ..WriterLayout::contract()
                },
            })
            .await;
    }

    /// A footer cap that already puts a strict table below its level does
    /// not leave overlap unread: the refusal is at the level the snapshot
    /// achieves and names every code below the declared level.
    #[tokio::test]
    async fn test_refresh_below_level_names_overlap_beside_a_footer_cap() {
        let (mut fixture, _) = two_bucket_table().await;
        let table = table_over(&fixture, fixture.config(Residency::Full, None)).await;
        table.load().await.unwrap();
        append_dictionary_overlap(&mut fixture).await;
        assert_eq!(
            refusal_of(table.refresh().await),
            RefusalReason::BelowLevel {
                declared: Level::Optimal,
                achieved: Level::Permissive,
                columns: vec![crate::check::ColumnLevel {
                    column: VALUE_COLUMN.to_string(),
                    level: Level::Permissive,
                }],
                findings: vec![
                    Capping {
                        code: FindingCode::DictionaryEncodedValue,
                        column: Some(VALUE_COLUMN.to_string()),
                    },
                    Capping {
                        code: FindingCode::OverlappingFiles,
                        column: None,
                    },
                ],
            }
        );
        assert_eq!(
            held(&table),
            (
                SnapshotId(FIRST_SNAPSHOT),
                SnapshotId(SECOND_SNAPSHOT),
                Level::Permissive
            )
        );
    }

    /// A strict Disk table below its level under a local directory too
    /// small for its chunk files is refused for its level, not its budget:
    /// the level is judged at the footer stage, before the budget is
    /// reserved, and the refusal writes nothing.
    #[tokio::test]
    async fn test_level_gate_runs_before_the_index_budget_is_reserved() {
        let fixture = one_file_fixture(WriterLayout {
            dictionary: Dictionary::Value,
            ..WriterLayout::contract()
        })
        .await;
        let scratch = tempfile::tempdir().unwrap();
        let local = local_dir(scratch.path(), fixture.storage.stats(), Some(TINY_BUDGET));
        let handles = TableHandles {
            local: Some(Arc::clone(&local)),
            ..handles_over(&fixture).await
        };
        let defaults = ServerConfig::default();
        let strict = table_sharing(
            &fixture,
            fixture.config(Residency::Disk, None),
            handles,
            defaults.load_deadline,
            defaults.refresh_deadline,
        );
        assert!(strict.config().strict);
        assert_eq!(refusal_of(strict.load().await), dictionary_below_optimal());
        assert_eq!(
            local.index_bytes_in_use().unwrap(),
            0,
            "nothing was written"
        );

        let lenient = table_sharing(
            &fixture,
            TableConfig {
                residency: Residency::Disk,
                ..lenient(&fixture)
            },
            TableHandles {
                local: Some(Arc::clone(&local)),
                ..handles_over(&fixture).await
            },
            defaults.load_deadline,
            defaults.refresh_deadline,
        );
        assert!(
            matches!(
                refusal_of(lenient.load().await),
                RefusalReason::OverBudget {
                    of: Budget::Index,
                    ..
                }
            ),
            "the budget refuses it once the level does not"
        );
    }

    #[tokio::test]
    async fn test_refresh_below_level_under_strict_holds_the_last_snapshot_at_level() {
        let (mut fixture, all) = two_bucket_table().await;
        let table = table_over(&fixture, fixture.config(Residency::Full, None)).await;
        table.load().await.unwrap();
        let balanced = WriterLayout {
            dictionary: Dictionary::Value,
            ..WriterLayout::contract()
        };
        append_second(&mut fixture, &all, "dict", balanced).await;
        let reason = refusal_of(table.refresh().await);
        assert_eq!(reason, dictionary_below_optimal());
        assert_eq!(
            table.current().unwrap().snapshot_id,
            SnapshotId(FIRST_SNAPSHOT)
        );
        assert_eq!(
            table
                .get(&Key::Long(all[0]), &[ColumnRead::whole(VALUE_COLUMN)])
                .await
                .unwrap()
                .row,
            present(all[0])
        );
        assert_eq!(held(&table), HELD_AT_BALANCED);
    }

    /// A transient failure of a poll while held makes the table `Degraded`,
    /// and the next poll that finds the source unchanged makes it `Held`
    /// for the remembered refusal again, building nothing.
    #[tokio::test]
    async fn test_refresh_transient_fault_while_held_reads_degraded_until_an_unchanged_poll() {
        let (mut fixture, all) = two_bucket_table().await;
        let table = table_over(&fixture, fixture.config(Residency::Full, None)).await;
        table.load().await.unwrap();
        let balanced = WriterLayout {
            dictionary: Dictionary::Value,
            ..WriterLayout::contract()
        };
        append_second(&mut fixture, &all, "dict", balanced).await;
        table.refresh().await.unwrap_err();
        assert_eq!(held(&table), HELD_AT_BALANCED);

        fixture.remove_hint();
        let error = table.refresh().await.unwrap_err();
        assert!(matches!(error, Error::ObjectStore(_)), "{error:?}");
        let (serving, reason) = degraded(&table);
        assert_eq!(serving, Some(SnapshotId(FIRST_SNAPSHOT)));
        assert_eq!(reason, ErrorKind::ObjectStore);

        fixture.restore_hint(SECOND_VERSION).await;
        let before = fixture.gets();
        assert!(!table.refresh().await.unwrap());
        assert_eq!(fixture.gets(), before + POLL_GETS, "the version hint alone");
        assert_eq!(held(&table), HELD_AT_BALANCED);
    }

    /// A first load below its level has no snapshot to hold: the table is
    /// `Degraded` with nothing serving, which `serve` exits on.
    #[tokio::test]
    async fn test_load_below_level_with_nothing_serving_is_degraded() {
        let fixture = one_file_fixture(WriterLayout {
            dictionary: Dictionary::Value,
            ..WriterLayout::contract()
        })
        .await;
        let table = table_over(&fixture, fixture.config(Residency::Full, None)).await;
        let reason = refusal_of(table.load().await);
        assert_eq!(degraded(&table), (None, ErrorKind::Refused(reason)));
    }

    /// A `load` over a serving generation that meets a snapshot below its
    /// level holds it as a poll does, naming the snapshot the load refused.
    #[tokio::test]
    async fn test_load_below_level_over_a_serving_generation_is_held() {
        let (mut fixture, all) = two_bucket_table().await;
        let table = table_over(&fixture, fixture.config(Residency::Full, None)).await;
        table.load().await.unwrap();
        let balanced = WriterLayout {
            dictionary: Dictionary::Value,
            ..WriterLayout::contract()
        };
        append_second(&mut fixture, &all, "dict", balanced).await;
        assert_eq!(refusal_of(table.load().await), dictionary_below_optimal());
        assert_eq!(held(&table), HELD_AT_BALANCED);
    }

    /// When a `Held` state began; any other state fails.
    fn held_since(table: &Table) -> Instant {
        let TableState::Held { since, .. } = table.state() else {
            panic!("expected Held, got {:?}", table.state());
        };
        since
    }

    /// A held table refused below its level again for the snapshot it
    /// holds, by a `load` here, stays held from the first refusal.
    #[tokio::test]
    async fn test_load_below_level_again_for_the_held_snapshot_keeps_its_since() {
        let (mut fixture, all) = two_bucket_table().await;
        let table = table_over(&fixture, fixture.config(Residency::Full, None)).await;
        table.load().await.unwrap();
        let balanced = WriterLayout {
            dictionary: Dictionary::Value,
            ..WriterLayout::contract()
        };
        append_second(&mut fixture, &all, "dict", balanced).await;
        table.refresh().await.unwrap_err();
        let first = held_since(&table);

        assert_eq!(refusal_of(table.load().await), dictionary_below_optimal());
        assert_eq!(held(&table), HELD_AT_BALANCED);
        assert_eq!(held_since(&table), first);
    }

    /// A refresh onto a file whose keys hash to bucket 0 but whose manifest
    /// records bucket 1 is refused as `KeyOutsideBucket`, and the previous
    /// generation keeps serving.
    #[tokio::test]
    async fn test_refresh_onto_a_file_outside_its_bucket_keeps_serving() {
        let (mut fixture, all) = two_bucket_table().await;
        let table = table_over(&fixture, fixture.config(Residency::Full, None)).await;
        table.load().await.unwrap();
        let (hashed, recorded) = (0, 1);
        let appended = keys_in_bucket(
            &keys_from(all.last().unwrap() + KEY_STEP, E2E_KEYS),
            bucketing(TWO),
            hashed,
        );
        fixture
            .add_long_file("misplaced", &appended, recorded)
            .await;
        fixture
            .commit(
                SECOND_SNAPSHOT,
                Some(FIRST_SNAPSHOT),
                SECOND_SEQUENCE,
                SECOND_VERSION,
            )
            .await;
        let error = table.refresh().await.unwrap_err();
        let expected = RefusalReason::KeyOutsideBucket {
            path: fixture.path_of("misplaced"),
            key: Key::Long(appended[0]),
            recorded,
            computed: hashed,
        };
        assert!(
            matches!(error, Error::Refused { ref reason, .. } if *reason == expected),
            "{error:?}"
        );
        assert_eq!(
            table.current().unwrap().snapshot_id,
            SnapshotId(FIRST_SNAPSHOT)
        );
        let (serving, kept) = degraded(&table);
        assert_eq!(serving, Some(SnapshotId(FIRST_SNAPSHOT)));
        assert_eq!(kept, ErrorKind::Refused(expected));
        assert_eq!(
            table
                .get(&Key::Long(all[0]), &[ColumnRead::whole(VALUE_COLUMN)])
                .await
                .unwrap()
                .row,
            present(all[0])
        );
    }

    #[tokio::test]
    async fn test_refresh_concurrent_polls_swap_once() {
        let (mut fixture, all) = two_bucket_table().await;
        let table = table_over(&fixture, fixture.config(Residency::Full, None)).await;
        table.load().await.unwrap();
        append_second(&mut fixture, &all, "f2", WriterLayout::contract()).await;
        let (a, b) = tokio::join!(table.refresh(), table.refresh());
        let outcomes = [a.unwrap(), b.unwrap()];
        assert_eq!(outcomes.iter().filter(|swapped| **swapped).count(), 1);
        assert_eq!(
            table.current().unwrap().snapshot_id,
            SnapshotId(SECOND_SNAPSHOT)
        );
    }

    /// The series a debugging recorder captured, looked up by name and
    /// exact label set.
    struct Series(Vec<(CompositeKey, Option<Unit>, Option<SharedString>, DebugValue)>);

    impl Series {
        fn find(&self, name: &str, labels: &[(&str, String)]) -> &DebugValue {
            let found: Vec<&DebugValue> = self
                .0
                .iter()
                .filter(|(key, ..)| labeled(key.key(), name, labels))
                .map(|(.., value)| value)
                .collect();
            assert_eq!(found.len(), 1, "{name} {labels:?}");
            found[0]
        }

        fn counter(&self, name: &str, labels: &[(&str, String)]) -> u64 {
            match self.find(name, labels) {
                DebugValue::Counter(value) => *value,
                DebugValue::Gauge(_) | DebugValue::Histogram(_) => panic!("{name} is a counter"),
            }
        }

        fn gauge(&self, name: &str, labels: &[(&str, String)]) -> f64 {
            match self.find(name, labels) {
                DebugValue::Gauge(value) => value.into_inner(),
                DebugValue::Counter(_) | DebugValue::Histogram(_) => panic!("{name} is a gauge"),
            }
        }

        /// The samples a histogram recorded, in order.
        fn histogram(&self, name: &str, labels: &[(&str, String)]) -> Vec<f64> {
            match self.find(name, labels) {
                DebugValue::Histogram(samples) => {
                    samples.iter().map(|sample| sample.into_inner()).collect()
                }
                DebugValue::Counter(_) | DebugValue::Gauge(_) => {
                    panic!("{name} is a histogram")
                }
            }
        }

        /// The series named `name`, of any labels.
        fn named(&self, name: &str) -> usize {
            self.0
                .iter()
                .filter(|(key, ..)| key.key().name() == name)
                .count()
        }

        /// The series, of any name, that carry `label`.
        fn labeled_with(&self, label: &str) -> usize {
            self.0
                .iter()
                .filter(|(key, ..)| key.key().labels().any(|have| have.key() == label))
                .count()
        }

        fn samples(&self, name: &str, labels: &[(&str, String)]) -> usize {
            match self.find(name, labels) {
                DebugValue::Histogram(values) => values.len(),
                DebugValue::Counter(_) | DebugValue::Gauge(_) => panic!("{name} is a histogram"),
            }
        }
    }

    /// Whether `key` is `name` with exactly `labels`.
    fn labeled(key: &MetricKey, name: &str, labels: &[(&str, String)]) -> bool {
        key.name() == name
            && key.labels().count() == labels.len()
            && labels.iter().all(|(label, value)| {
                key.labels()
                    .any(|have| have.key() == *label && have.value() == value)
            })
    }

    fn table_label() -> (&'static str, String) {
        (metric::LABEL_TABLE, TABLE.to_string())
    }

    /// The label no generation metric carries: a series per snapshot would
    /// grow with every swap.
    const LABEL_SNAPSHOT_ID: &str = "snapshot_id";

    #[test]
    fn test_metrics_follow_lookups_swaps_and_failures() {
        let started = time::unix_seconds(time::now());
        let (reason, series) = recorded(async {
            let (mut fixture, all) = two_bucket_table().await;
            let table = table_over(&fixture, lenient(&fixture)).await;
            table.load().await.unwrap();
            table
                .get(&Key::Long(all[0]), &[ColumnRead::whole(VALUE_COLUMN)])
                .await
                .unwrap();
            table
                .get(&Key::Long(all[0]), &[ColumnRead::whole(VALUE_COLUMN)])
                .await
                .unwrap();
            table
                .get(&Key::Long(all[0] + 1), &[ColumnRead::whole(VALUE_COLUMN)])
                .await
                .unwrap();
            append_second(&mut fixture, &all, "f2", WriterLayout::contract()).await;
            assert!(table.refresh().await.unwrap());
            let reason = commit_duplicate(
                &mut fixture,
                &all,
                THIRD_SNAPSHOT,
                SECOND_SNAPSHOT,
                THIRD_SEQUENCE,
                THIRD_VERSION,
            )
            .await;
            assert!(table.refresh().await.is_err());
            reason
        });

        for result in [
            LookupResult::ColdHit,
            LookupResult::WarmHit,
            LookupResult::Miss,
        ] {
            let labels = [table_label(), (metric::LABEL_RESULT, result.to_string())];
            assert_eq!(
                series.counter(metric::LOOKUPS_TOTAL, &labels),
                1,
                "{result}"
            );
            assert_eq!(
                series.samples(metric::LOOKUP_SECONDS, &labels),
                1,
                "{result}"
            );
        }
        assert_eq!(
            series.counter(metric::GENERATION_SWAPS_TOTAL, &[table_label()]),
            1
        );
        assert_eq!(
            series
                .gauge(metric::GENERATION_SEQUENCE_NUMBER, &[table_label()])
                .to_bits(),
            approx::signed_gauge(SECOND_SEQUENCE).to_bits(),
            "the swapped-in snapshot's, not the refused one's"
        );
        let swapped_at = series.gauge(metric::GENERATION_SWAPPED_AT_SECONDS, &[table_label()]);
        assert!(
            (started..=time::unix_seconds(time::now())).contains(&swapped_at),
            "{swapped_at} is not between the test's start and now"
        );
        assert!(series.labeled_with(metric::LABEL_TABLE) > 0);
        assert_eq!(series.labeled_with(LABEL_SNAPSHOT_ID), 0);
        assert_eq!(
            series.counter(
                metric::REFRESH_ERRORS_TOTAL,
                &[table_label(), (metric::LABEL_REASON, reason.to_string())]
            ),
            1
        );
        let state = |name: StateName| state_gauge(&series, TABLE, name);
        assert_eq!(
            state(StateName::Degraded).to_bits(),
            approx::gauge(1).to_bits()
        );
        assert_eq!(
            state(StateName::Serving).to_bits(),
            approx::gauge(0).to_bits()
        );
        assert_eq!(
            state(StateName::Loading).to_bits(),
            approx::gauge(0).to_bits()
        );
    }

    /// The field a protobuf fixture value holds its key in, as a varint.
    const KEY_FIELD: u32 = 1;
    /// The field a protobuf fixture value holds its filler in.
    const FILLER_FIELD: u32 = 2;
    /// The filler bytes of a protobuf fixture value.
    const FILLER_BYTES: usize = 40;

    /// A protobuf message for `key`: `KEY_FIELD` holding the key, then
    /// `FILLER_FIELD` holding `FILLER_BYTES` bytes, and the length of the
    /// first field's record.
    fn protobuf_value(key: i64) -> (Vec<u8>, usize) {
        let mut value = Vec::new();
        prost::encoding::encode_key(KEY_FIELD, prost::encoding::WireType::Varint, &mut value);
        prost::encoding::encode_varint(key.cast_unsigned(), &mut value);
        let key_record = value.len();
        prost::encoding::bytes::encode(FILLER_FIELD, &vec![0xAB; FILLER_BYTES], &mut value);
        (value, key_record)
    }

    /// A one-bucket fixture of one file of `FILE_KEYS` keys whose values
    /// are `protobuf_value`'s, committed as `FIRST_SNAPSHOT`, and its keys.
    async fn protobuf_fixture() -> (Fixture, Vec<i64>) {
        let mut fixture = Fixture::bucketed(1);
        let all = keys_from(0, FILE_KEYS);
        fixture
            .add_file(FileSpec {
                name: "f",
                keys: &long_keys(&all),
                values: all.iter().map(|&key| Some(protobuf_value(key).0)).collect(),
                partition: Partition::Bucket(0),
                bounds: Bounds::Exact,
                layout: WriterLayout::contract(),
            })
            .await;
        fixture
            .commit(FIRST_SNAPSHOT, None, FIRST_SEQUENCE, FIRST_VERSION)
            .await;
        (fixture, all)
    }

    /// `misses` lookups of `absent`, then one of `present` trimmed to
    /// `KEY_FIELD`: the length of the value it answers.
    async fn misses_then_a_projected_hit(
        table: &Table,
        absent: &Key,
        misses: usize,
        present: i64,
    ) -> Option<usize> {
        for _ in 0..misses {
            let row = table
                .get(absent, &[ColumnRead::whole(VALUE_COLUMN)])
                .await
                .unwrap()
                .row;
            assert_eq!(row, Row::Absent);
        }
        let projected = table
            .get(
                &Key::Long(present),
                &[ColumnRead {
                    name: VALUE_COLUMN,
                    fields: &[KEY_FIELD],
                }],
            )
            .await
            .unwrap()
            .row;
        let Row::Present(values) = projected else {
            panic!("the key is present");
        };
        values[0].as_ref().map(Bytes::len)
    }

    /// A loaded table of `fixture` under `config` reading through a row
    /// cache of its own, which comes back beside it with the page cache
    /// the table reads through.
    async fn table_reading_rows(
        fixture: &Fixture,
        config: TableConfig,
    ) -> (Table, RowCache, PageCache) {
        let rows = RowCache::new(approx::count(testing::CACHE_BYTES));
        let handles = TableHandles {
            row_cache: Some(rows.clone()),
            ..handles_over(fixture).await
        };
        let pages = handles.cache.clone();
        let defaults = ServerConfig::default();
        let table = table_sharing(
            fixture,
            config,
            handles,
            defaults.load_deadline,
            defaults.refresh_deadline,
        );
        table.load().await.unwrap();
        (table, rows, pages)
    }

    /// The one value a lookup naming one column answers.
    fn only_value(row: Row) -> Option<Bytes> {
        let Row::Present(mut values) = row else {
            panic!("the key is present");
        };
        assert_eq!(values.len(), 1, "one column named");
        values.remove(0)
    }

    /// The lookups `series` counts under `result` for the fixture's table.
    fn lookups_counted(series: &Series, result: LookupResult) -> u64 {
        series.counter(
            metric::LOOKUPS_TOTAL,
            &[table_label(), (metric::LABEL_RESULT, result.to_string())],
        )
    }

    /// A repeated Get is served from the row cache: with the page cache
    /// emptied between the two, the second reads no value page and is a
    /// warm hit (RFD 026).
    #[test]
    fn test_a_row_cache_hit_reads_no_value_page() {
        let (gets, series) = recorded(async {
            let fixture = one_file_fixture(WriterLayout::contract()).await;
            let (table, rows, pages) = table_reading_rows(&fixture, lenient(&fixture)).await;
            let first = keys_from(0, FILE_KEYS)[0];
            let read = [ColumnRead::whole(VALUE_COLUMN)];
            let key = Key::Long(first);
            assert_eq!(table.get(&key, &read).await.unwrap().row, present(first));
            assert!(rows.usage() > 0, "the value was cached");
            pages.clear();
            let before = fixture.gets();
            assert_eq!(table.get(&key, &read).await.unwrap().row, present(first));
            table.publish_lookup_counts();
            fixture.gets() - before
        });
        assert_eq!(gets, 0, "no value page is read");
        assert_eq!(lookups_counted(&series, LookupResult::ColdHit), 1);
        assert_eq!(lookups_counted(&series, LookupResult::WarmHit), 1);
    }

    /// The value a hit answers from the row cache is a copy of its own,
    /// outside the page it was read from, though a value read from an
    /// uncompressed page is a slice of it, so the cache keeps no page
    /// alive.
    #[tokio::test]
    async fn test_a_cached_value_owns_its_bytes() {
        let fixture = one_file_fixture(WriterLayout {
            compression: parquet::basic::Compression::UNCOMPRESSED,
            ..WriterLayout::contract()
        })
        .await;
        let (table, _rows, pages) = table_reading_rows(&fixture, lenient(&fixture)).await;
        let first = keys_from(0, FILE_KEYS)[0];
        let key = Key::Long(first);
        let read = [ColumnRead::whole(VALUE_COLUMN)];
        let missed = only_value(table.get(&key, &read).await.unwrap().row).unwrap();
        let generation = table.current().unwrap();
        let (file, page_ref) = generation.plan(std::slice::from_ref(&key))[0]
            .clone()
            .unwrap();
        let offset = file.index.value_page_offset(0, &page_ref).unwrap();
        let page = pages
            .held_page(file.index.handle().file_id(), offset)
            .expect("the value page is in memory");
        let within_page = |value: &Bytes| page.as_ptr_range().contains(&value.as_ptr());
        assert!(
            within_page(&missed),
            "a value read from an uncompressed page is a slice of it"
        );
        let hit = only_value(table.get(&key, &read).await.unwrap().row).unwrap();
        assert!(!within_page(&hit), "the cached value is a copy");
        assert_eq!(hit.len(), value_for(first).len());
        assert_eq!(hit, missed);
    }

    /// Tables over one source listing their value columns in opposite
    /// orders, and one listing the value column alone, which a table's
    /// layout places first where the others place it second, share one row
    /// cache and each read each column's own bytes: an entry is keyed by
    /// the column's leaf in the file, not its place in a table's layout.
    #[tokio::test]
    async fn test_tables_listing_columns_in_another_order_read_their_own_columns() {
        const VALUE_ALONE_TABLE: &str = "value_alone";
        let (fixture, all, _gauged) = two_column_fixture(FILE_KEYS, WriterLayout::contract()).await;
        let rows = RowCache::new(approx::count(testing::CACHE_BYTES));
        let handles = TableHandles {
            row_cache: Some(rows.clone()),
            ..handles_over(&fixture).await
        };
        let listed = lenient(&fixture);
        let reversed = TableConfig {
            name: STRICT_TABLE.to_string(),
            value_columns: listed.value_columns.iter().rev().cloned().collect(),
            ..listed.clone()
        };
        let value_alone = TableConfig {
            name: VALUE_ALONE_TABLE.to_string(),
            value_columns: vec![ValueColumnConfig::named(VALUE_COLUMN)],
            ..listed.clone()
        };
        let defaults = ServerConfig::default();
        let tables = [value_alone, listed, reversed].map(|config| {
            table_sharing(
                &fixture,
                config,
                handles.clone(),
                defaults.load_deadline,
                defaults.refresh_deadline,
            )
        });
        for table in &tables {
            table.load().await.unwrap();
        }
        let key = Key::Long(all[0]);
        let value = value_for(all[0]);
        let detail = detail_of(&value);
        for (column, bytes) in [(VALUE_COLUMN, &value), (DETAIL_COLUMN, &detail)] {
            for table in tables
                .iter()
                .filter(|table| table.config().format_of(column).is_some())
            {
                assert_eq!(
                    only_value(
                        table
                            .get(&key, &[ColumnRead::whole(column)])
                            .await
                            .unwrap()
                            .row
                    ),
                    Some(Bytes::from(bytes.clone())),
                    "{} reads {column}",
                    table.name
                );
            }
        }
        assert_eq!(
            rows.usage(),
            weigh(Some(value.len())) + weigh(Some(detail.len())),
            "one entry per column, shared by every table"
        );
    }

    /// Projection trims the value after the row cache: two Gets naming
    /// different fields answer different bytes from one entry holding the
    /// whole value (RFD 024).
    #[tokio::test]
    async fn test_projection_trims_the_cached_value() {
        let (fixture, all) = protobuf_fixture().await;
        let config = TableConfig {
            value_columns: vec![ValueColumnConfig::of(VALUE_COLUMN, ValueFormat::Protobuf)],
            ..lenient(&fixture)
        };
        let (table, rows, _pages) = table_reading_rows(&fixture, config).await;
        let key = Key::Long(all[0]);
        let (value, key_record) = protobuf_value(all[0]);
        let trimmed_to = |field: &'static [u32]| {
            let table = &table;
            let key = &key;
            async move {
                only_value(
                    table
                        .get(
                            key,
                            &[ColumnRead {
                                name: VALUE_COLUMN,
                                fields: field,
                            }],
                        )
                        .await
                        .unwrap()
                        .row,
                )
                .unwrap()
            }
        };
        let key_field = trimmed_to(&[KEY_FIELD]).await;
        let filler_field = trimmed_to(&[FILLER_FIELD]).await;
        assert_eq!(key_field, Bytes::copy_from_slice(&value[..key_record]));
        assert_eq!(filler_field, Bytes::copy_from_slice(&value[key_record..]));
        assert_eq!(
            rows.usage(),
            weigh(Some(value.len())),
            "one entry, of the whole value"
        );
    }

    /// A table whose entry sets `row_cache = false` reads every value from
    /// its page: with the page cache emptied, a repeated Get reads its
    /// value page again and is a cold hit, and the row cache holds nothing.
    #[test]
    fn test_row_cache_false_reads_pages_alone() {
        let ((gets, usage), series) = recorded(async {
            let fixture = one_file_fixture(WriterLayout::contract()).await;
            let config = TableConfig {
                row_cache: Some(false),
                ..lenient(&fixture)
            };
            let (table, rows, pages) = table_reading_rows(&fixture, config).await;
            let first = keys_from(0, FILE_KEYS)[0];
            let read = [ColumnRead::whole(VALUE_COLUMN)];
            let key = Key::Long(first);
            assert_eq!(table.get(&key, &read).await.unwrap().row, present(first));
            pages.clear();
            let before = fixture.gets();
            assert_eq!(table.get(&key, &read).await.unwrap().row, present(first));
            table.publish_lookup_counts();
            (fixture.gets() - before, rows.usage())
        });
        assert_eq!(gets, 1, "the value page is read again");
        assert_eq!(usage, 0);
        assert_eq!(lookups_counted(&series, LookupResult::ColdHit), 2);
    }

    /// Lookups spread over a multi-thread runtime's workers reach
    /// `lookups_total` at `publish_lookup_counts`, each once, and the bytes
    /// each read and returned reach the value bytes counters the same way:
    /// a snapshot of the debugging recorder takes the counters' values and
    /// resets them, so each snapshot reads what was published since the one
    /// before.
    #[test]
    fn test_publish_lookup_counts_moves_every_task_count_once() {
        let workers = 4;
        let lookups_per_task = 8;
        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(workers)
            .enable_all()
            .build()
            .unwrap();
        let result =
            |result: LookupResult| [table_label(), (metric::LABEL_RESULT, result.to_string())];
        let column = [
            table_label(),
            (metric::LABEL_COLUMN, VALUE_COLUMN.to_string()),
        ];
        let published = || {
            let series = Series(snapshotter.snapshot().into_vec());
            let counter = |name, labels: &[(&str, String)]| series.counter(name, labels);
            (
                counter(metric::LOOKUPS_TOTAL, &result(LookupResult::Miss)),
                counter(metric::LOOKUPS_TOTAL, &result(LookupResult::ColdHit))
                    + counter(metric::LOOKUPS_TOTAL, &result(LookupResult::WarmHit)),
                counter(metric::VALUE_BYTES_READ_TOTAL, &column),
                counter(metric::VALUE_BYTES_RETURNED_TOTAL, &column),
            )
        };
        metrics::with_local_recorder(&recorder, || {
            runtime.block_on(async {
                let (fixture, all) = protobuf_fixture().await;
                let config = TableConfig {
                    value_columns: vec![ValueColumnConfig::of(VALUE_COLUMN, ValueFormat::Protobuf)],
                    ..fixture.config(Residency::Full, None)
                };
                let table = Arc::new(table_over(&fixture, config).await);
                table.load().await.unwrap();
                let absent = Key::Long(all.iter().max().unwrap() + 1);
                let present = all[0];
                let (value, key_record) = protobuf_value(present);
                let tasks: Vec<_> = (0..workers)
                    .map(|_| {
                        let table = Arc::clone(&table);
                        let absent = absent.clone();
                        tokio::spawn(async move {
                            misses_then_a_projected_hit(&table, &absent, lookups_per_task, present)
                                .await
                        })
                    })
                    .collect();
                for task in tasks {
                    assert_eq!(task.await.unwrap(), Some(key_record));
                }
                assert_eq!(
                    published(),
                    (0, 0, 0, 0),
                    "counted on the shards until published"
                );
                table.publish_lookup_counts();
                let workers_count = approx::count(workers);
                assert_eq!(
                    published(),
                    (
                        approx::count(workers * lookups_per_task),
                        workers_count,
                        workers_count * approx::count(value.len()),
                        workers_count * approx::count(key_record),
                    ),
                    "a miss reads no bytes; a projected hit reads its value and returns the field"
                );
                table.publish_lookup_counts();
                assert_eq!(published(), (0, 0, 0, 0), "nothing is published twice");
                table
                    .get(&Key::Long(present), &[ColumnRead::whole(VALUE_COLUMN)])
                    .await
                    .unwrap();
                drop(table);
                let whole = approx::count(value.len());
                assert_eq!(
                    published(),
                    (0, 1, whole, whole),
                    "a dropped table publishes what it counted, a whole read as read"
                );
            });
        });
    }

    /// A debugging recorder's snapshot of what `body` emitted, run on a
    /// current-thread runtime, since the local recorder is thread-local.
    fn recorded<T>(body: impl Future<Output = T>) -> (T, Series) {
        recorded_with(|_| body)
    }

    /// `recorded`, with `body` given a snapshotter of the same recorder, to
    /// read the series while it runs. A snapshot zeroes every series it
    /// reads, so a later one holds only what was written after it.
    fn recorded_with<T, F: Future<Output = T>>(body: impl FnOnce(Snapshotter) -> F) -> (T, Series) {
        recorded_on(tokio::runtime::Builder::new_current_thread(), body)
    }

    /// `recorded_with` on a runtime of `SPLIT_WORKERS` worker threads, where
    /// a batch of enough keys spreads over spawned runs. `body` runs on the
    /// thread the local recorder is set on, so a table it builds registers
    /// its series there, and their handles count from every worker.
    fn recorded_on_split_workers<T, F: Future<Output = T>>(
        body: impl FnOnce(Snapshotter) -> F,
    ) -> (T, Series) {
        let mut runtime = tokio::runtime::Builder::new_multi_thread();
        runtime.worker_threads(SPLIT_WORKERS);
        recorded_on(runtime, body)
    }

    /// `body` run on the runtime `runtime` builds, under a debugging
    /// recorder, and a snapshot of what it emitted.
    fn recorded_on<T, F: Future<Output = T>>(
        mut runtime: tokio::runtime::Builder,
        body: impl FnOnce(Snapshotter) -> F,
    ) -> (T, Series) {
        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();
        let runtime = runtime.enable_all().build().unwrap();
        let out = metrics::with_local_recorder(&recorder, || {
            runtime.block_on(body(recorder.snapshotter()))
        });
        (out, Series(snapshotter.snapshot().into_vec()))
    }

    /// The `table_state` gauge of `name` for the table `table`.
    fn state_gauge(series: &Series, table: &str, name: StateName) -> f64 {
        series.gauge(
            metric::TABLE_STATE,
            &[
                (metric::LABEL_TABLE, table.to_string()),
                (metric::LABEL_STATE, name.to_string()),
            ],
        )
    }

    /// The level gauges of `table`, as bits: below the declared level, the
    /// level achieved, and the level declared.
    fn level_gauges(series: &Series) -> [u64; 3] {
        let labels = [table_label()];
        [
            metric::TABLE_BELOW_LEVEL,
            metric::TABLE_LEVEL,
            metric::TABLE_DECLARED_LEVEL,
        ]
        .map(|name| series.gauge(name, &labels).to_bits())
    }

    /// What `level_gauges` reads for `below`, 1 or 0, at `achieved` against
    /// `declared`.
    fn level_bits(below: u64, achieved: Level, declared: Level) -> [u64; 3] {
        [
            approx::gauge(below).to_bits(),
            achieved.gauge().to_bits(),
            declared.gauge().to_bits(),
        ]
    }

    #[test]
    fn test_metrics_level_series_appear_at_first_swap() {
        let ((), before) = recorded(async {
            let (fixture, _) = two_bucket_table().await;
            let table = table_over(&fixture, fixture.config(Residency::Full, None)).await;
            fixture.remove_hint();
            table.load().await.unwrap_err();
        });
        for name in [
            metric::TABLE_BELOW_LEVEL,
            metric::TABLE_LEVEL,
            metric::TABLE_DECLARED_LEVEL,
        ] {
            assert_eq!(before.named(name), 0, "{name} before any swap");
        }
        let ((), after) = recorded(async {
            let (fixture, _) = two_bucket_table().await;
            let config = TableConfig {
                lookups: Some(Level::Balanced),
                ..fixture.config(Residency::Full, None)
            };
            table_over(&fixture, config).await.load().await.unwrap();
        });
        assert_eq!(
            level_gauges(&after),
            level_bits(0, Level::Optimal, Level::Balanced)
        );
    }

    /// The lookup-cost gauges of `table`, as bits: GETs per hit, GETs per
    /// miss, and files probed per lookup.
    fn cost_gauges(series: &Series) -> [u64; 3] {
        let labels = [table_label()];
        [
            metric::TABLE_GETS_PER_HIT,
            metric::TABLE_GETS_PER_MISS,
            metric::TABLE_FILES_PROBED_PER_LOOKUP,
        ]
        .map(|name| series.gauge(name, &labels).to_bits())
    }

    /// What `cost_gauges` reads for `cost`.
    fn cost_bits(cost: LookupCost) -> [u64; 3] {
        [
            approx::gauge(cost.gets_per_hit).to_bits(),
            approx::gauge(cost.gets_per_miss).to_bits(),
            approx::gauge_len(cost.files_probed).to_bits(),
        ]
    }

    /// The cost of a cold lookup on `two_bucket_table` in `full`.
    const CONTRACT_COST: LookupCost = LookupCost {
        gets_per_hit: 1,
        gets_per_miss: 0,
        files_probed: 1,
    };

    /// The cost on `two_bucket_table` after `append_dictionary_overlap`, in
    /// `full`: the value dictionary page beside the value page, and the
    /// two files of bucket 0 probed.
    const DICTIONARY_OVERLAP_COST: LookupCost = LookupCost {
        gets_per_hit: 2,
        gets_per_miss: 0,
        files_probed: 2,
    };

    /// The lookup-cost gauges appear with the first swap and follow the
    /// serving generation: a swap to a layout with overlapping files and a
    /// dictionary-encoded value column raises them.
    #[test]
    fn test_metrics_lookup_cost_appears_at_first_swap_and_follows_the_swaps() {
        let ((), before) = recorded(async {
            let (fixture, _) = two_bucket_table().await;
            let table = table_over(&fixture, lenient(&fixture)).await;
            fixture.remove_hint();
            table.load().await.unwrap_err();
        });
        for name in [
            metric::TABLE_GETS_PER_HIT,
            metric::TABLE_GETS_PER_MISS,
            metric::TABLE_FILES_PROBED_PER_LOOKUP,
        ] {
            assert_eq!(before.named(name), 0, "{name} before any swap");
        }
        let (first, after) = recorded_with(|snapshotter| async move {
            let (mut fixture, _) = two_bucket_table().await;
            let table = table_over(&fixture, lenient(&fixture)).await;
            table.load().await.unwrap();
            assert_eq!(table.current().unwrap().lookup_cost, CONTRACT_COST);
            let first = Series(snapshotter.snapshot().into_vec());
            append_dictionary_overlap(&mut fixture).await;
            assert!(table.refresh().await.unwrap());
            assert_eq!(
                table.current().unwrap().lookup_cost,
                DICTIONARY_OVERLAP_COST
            );
            first
        });
        assert_eq!(cost_gauges(&first), cost_bits(CONTRACT_COST));
        assert_eq!(cost_gauges(&after), cost_bits(DICTIONARY_OVERLAP_COST));
    }

    /// A strict table held on a snapshot that costs more keeps the serving
    /// generation's cost: the gauges say what lookups pay now.
    #[test]
    fn test_metrics_lookup_cost_is_the_serving_generations_while_held() {
        let ((), series) = recorded(async {
            let (mut fixture, _) = two_bucket_table().await;
            let table = table_over(&fixture, fixture.config(Residency::Full, None)).await;
            table.load().await.unwrap();
            append_dictionary_overlap(&mut fixture).await;
            table.refresh().await.unwrap_err();
            assert_eq!(
                held(&table),
                (
                    SnapshotId(FIRST_SNAPSHOT),
                    SnapshotId(SECOND_SNAPSHOT),
                    Level::Permissive
                )
            );
            table.publish_gauges();
        });
        assert_eq!(cost_gauges(&series), cost_bits(CONTRACT_COST));
        assert_eq!(
            level_gauges(&series),
            level_bits(1, Level::Permissive, Level::Optimal),
            "the level gauges judge the held snapshot"
        );
    }

    /// The rows of `two_bucket_table`'s bucket 0 file that `commit_deletes`
    /// deletes.
    const F0_DELETED: [i64; 2] = [0, 1];
    /// The rows of its bucket 1 file that `commit_deletes` deletes.
    const F1_DELETED: [i64; 1] = [0];
    /// The position delete files of `commit_deletes` that apply.
    const APPLYING_DELETE_FILES: u64 = 2;

    /// Commits to `two_bucket_table`'s `fixture`, as `SECOND_SNAPSHOT`, a
    /// position delete file per data file deleting `F0_DELETED` and
    /// `F1_DELETED`, and a dangling one naming a file the table never held.
    async fn commit_deletes(fixture: &mut Fixture) {
        fixture
            .add_position_delete("d0", "f0", &F0_DELETED, 0)
            .await;
        fixture
            .add_position_delete("d1", "f1", &F1_DELETED, 1)
            .await;
        fixture
            .add_position_delete("dangling", "gone", &F1_DELETED, 1)
            .await;
        fixture
            .commit(
                SECOND_SNAPSHOT,
                Some(FIRST_SNAPSHOT),
                SECOND_SEQUENCE,
                SECOND_VERSION,
            )
            .await;
    }

    /// The delete gauges of `table`, as bits: deleted rows, deleted
    /// fraction, delete files, and delete state bytes.
    fn delete_gauges(series: &Series) -> [u64; 4] {
        let labels = [table_label()];
        DELETE_GAUGES.map(|name| series.gauge(name, &labels).to_bits())
    }

    /// The gauges a swap writes from `Generation::delete_summary`.
    const DELETE_GAUGES: [&str; 4] = [
        metric::TABLE_DELETED_ROWS,
        metric::TABLE_DELETED_FRACTION,
        metric::TABLE_DELETE_FILES,
        metric::TABLE_DELETE_STATE_BYTES,
    ];

    /// What `delete_gauges` reads for `summary`.
    fn delete_bits(summary: DeleteSummary) -> [u64; 4] {
        [
            approx::gauge(summary.rows).to_bits(),
            summary.fraction.to_bits(),
            approx::gauge(summary.files).to_bits(),
            approx::gauge_len(summary.state_bytes).to_bits(),
        ]
    }

    /// The delete summary of `generation` is what `commit_deletes` applies:
    /// its rows, of the snapshot's rows, from the two files that name a
    /// data file the snapshot holds and not the dangling one, holding the
    /// bytes `index_bytes` counts past the file indexes.
    fn assert_commit_deletes_applied(generation: &Generation) -> DeleteSummary {
        let summary = generation.delete_summary();
        let deleted = approx::count(F0_DELETED.len() + F1_DELETED.len());
        assert_eq!(summary.rows, deleted);
        assert_eq!(summary.files, APPLYING_DELETE_FILES);
        assert_eq!(
            summary.fraction.to_bits(),
            approx::ratio(deleted, generation.records).to_bits()
        );
        let file_bytes: usize = generation
            .files
            .iter()
            .map(|file| file.index.resident_bytes())
            .sum();
        assert!(summary.state_bytes > 0);
        assert_eq!(generation.index_bytes, file_bytes + summary.state_bytes);
        summary
    }

    /// The delete gauges appear with the first swap, at zero on a snapshot
    /// with no deletes, and follow the swaps: a snapshot with deletes
    /// writes what its applying deletes come to, a dangling delete counted
    /// in none.
    #[test]
    fn test_metrics_delete_gauges_appear_at_first_swap_and_count_applying_deletes() {
        let ((), before) = recorded(async {
            let (fixture, _) = two_bucket_table().await;
            let table = table_over(&fixture, lenient(&fixture)).await;
            fixture.remove_hint();
            table.load().await.unwrap_err();
        });
        for name in DELETE_GAUGES {
            assert_eq!(before.named(name), 0, "{name} before any swap");
        }
        let ((first, summary), after) = recorded_with(|snapshotter| async move {
            let (mut fixture, _) = two_bucket_table().await;
            let table = table_over(&fixture, fixture.config(Residency::Full, None)).await;
            table.load().await.unwrap();
            let generation = table.current().unwrap();
            assert_eq!(generation.delete_summary(), DeleteSummary::default());
            assert_eq!(generation.oldest_delete_committed, None);
            let first = Series(snapshotter.snapshot().into_vec());
            commit_deletes(&mut fixture).await;
            assert!(table.refresh().await.unwrap());
            let summary = assert_commit_deletes_applied(&table.current().unwrap());
            (first, summary)
        });
        assert_eq!(delete_gauges(&first), delete_bits(DeleteSummary::default()));
        assert_eq!(delete_gauges(&after), delete_bits(summary));
    }

    /// A strict table held on a snapshot below its level keeps the serving
    /// generation's delete gauges, though the held snapshot deletes more.
    #[test]
    fn test_metrics_delete_gauges_are_the_serving_generations_while_held() {
        let (summary, series) = recorded(async {
            let (mut fixture, _) = two_bucket_table().await;
            commit_deletes(&mut fixture).await;
            let table = table_over(&fixture, fixture.config(Residency::Full, None)).await;
            table.load().await.unwrap();
            let summary = assert_commit_deletes_applied(&table.current().unwrap());
            add_dictionary_overlap(&mut fixture).await;
            fixture.add_position_delete("d2", "f1", &[1], 1).await;
            fixture
                .commit(
                    THIRD_SNAPSHOT,
                    Some(SECOND_SNAPSHOT),
                    THIRD_SEQUENCE,
                    THIRD_VERSION,
                )
                .await;
            table.refresh().await.unwrap_err();
            assert_eq!(
                held(&table),
                (
                    SnapshotId(SECOND_SNAPSHOT),
                    SnapshotId(THIRD_SNAPSHOT),
                    Level::Permissive
                )
            );
            table.publish_gauges();
            summary
        });
        assert_eq!(delete_gauges(&series), delete_bits(summary));
    }

    /// The oldest applying delete is dated by the snapshot that committed
    /// it, found by its data sequence number; once that snapshot has
    /// expired, by the earliest later snapshot, never by an older one that
    /// predates the delete.
    #[tokio::test]
    async fn test_load_dates_the_oldest_delete_by_its_snapshot_or_the_next_listed() {
        let (mut fixture, _) = two_bucket_table().await;
        commit_deletes(&mut fixture).await;
        fixture
            .commit(
                THIRD_SNAPSHOT,
                Some(SECOND_SNAPSHOT),
                THIRD_SEQUENCE,
                THIRD_VERSION,
            )
            .await;
        let table = table_over(&fixture, fixture.config(Residency::Full, None)).await;
        table.load().await.unwrap();
        let generation = table.current().unwrap();
        assert_eq!(generation.snapshot_id, SnapshotId(THIRD_SNAPSHOT));
        assert_eq!(
            generation.oldest_delete_committed,
            Some(fixture.committed(SECOND_SNAPSHOT))
        );

        let expired_version = THIRD_VERSION + 1;
        fixture.expire(SECOND_SNAPSHOT, expired_version).await;
        let table = table_over(&fixture, fixture.config(Residency::Full, None)).await;
        table.load().await.unwrap();
        let generation = table.current().unwrap();
        assert_commit_deletes_applied(&generation);
        assert_ne!(
            fixture.committed(FIRST_SNAPSHOT),
            fixture.committed(THIRD_SNAPSHOT),
            "the older snapshot would date the delete otherwise"
        );
        assert_eq!(
            generation.oldest_delete_committed,
            Some(fixture.committed(THIRD_SNAPSHOT))
        );
    }

    /// A sample of `PER_MILLE` pages, `resident` of them in the page
    /// cache, so a resident count reads as thousandths.
    fn sample_at(resident: u64) -> ResidentPages {
        ResidentPages {
            pages: PER_MILLE,
            resident,
        }
    }

    /// A sample just short of the page cache: one page below the fall.
    fn short_sample() -> ResidentPages {
        sample_at(FALL_PER_MILLE - 1)
    }

    /// `short_sample`'s fraction as the gauge reads it.
    fn short_fraction() -> u64 {
        approx::ratio(FALL_PER_MILLE - 1, PER_MILLE).to_bits()
    }

    /// `config` in the Disk tier over `fixture`, its chunk files in a fresh
    /// local directory that the returned guard keeps, in `Loading`.
    async fn disk_table(fixture: &Fixture, config: TableConfig) -> (Table, tempfile::TempDir) {
        let scratch = tempfile::tempdir().unwrap();
        let local = local_dir(scratch.path(), fixture.storage.stats(), None);
        let handles = TableHandles {
            local: Some(local),
            ..handles_over(fixture).await
        };
        let defaults = ServerConfig::default();
        let table = table_sharing(
            fixture,
            TableConfig {
                residency: Residency::Disk,
                ..config
            },
            handles,
            defaults.load_deadline,
            defaults.refresh_deadline,
        );
        (table, scratch)
    }

    /// Takes `sample` as the page cache's condition of `table`'s serving
    /// generation.
    fn observe(table: &Table, sample: &std::result::Result<ResidentPages, Unobserved>) {
        table.observe_page_cache(&table.current().unwrap(), sample);
    }

    /// The resident fraction gauge of `TABLE`, as bits.
    fn fraction_gauge(series: &Series) -> u64 {
        series
            .gauge(metric::TABLE_CHUNK_RESIDENT_FRACTION, &[table_label()])
            .to_bits()
    }

    /// A disk table whose chunk files fall short of the page cache is below
    /// optimal for it, its layout's level notwithstanding, and recovers once
    /// they are back; the resident fraction is the sample's.
    #[test]
    fn test_metrics_a_disk_table_short_of_the_page_cache_is_below_optimal() {
        let (short, recovered) = recorded_with(|snapshotter| async move {
            let (fixture, _) = two_bucket_table().await;
            let (table, _scratch) = disk_table(&fixture, lenient(&fixture)).await;
            table.load().await.unwrap();
            observe(&table, &Ok(short_sample()));
            let short = Series(snapshotter.snapshot().into_vec());
            observe(&table, &Ok(sample_at(PER_MILLE)));
            assert_eq!(table.current().unwrap().level, Level::Optimal);
            short
        });
        assert_eq!(
            level_gauges(&short),
            level_bits(1, Level::Balanced, Level::Optimal)
        );
        assert_eq!(fraction_gauge(&short), short_fraction());
        assert_eq!(
            level_gauges(&recovered),
            level_bits(0, Level::Optimal, Level::Optimal)
        );
        assert_eq!(fraction_gauge(&recovered), 1.0_f64.to_bits());
    }

    /// A table that declares balanced is at its level with its chunk files
    /// short of the page cache: device reads of key chunks are a balanced
    /// cost.
    #[test]
    fn test_metrics_a_balanced_disk_table_short_of_the_page_cache_stays_at_its_level() {
        let ((), series) = recorded(async {
            let (fixture, _) = two_bucket_table().await;
            let config = TableConfig {
                lookups: Some(Level::Balanced),
                ..lenient(&fixture)
            };
            let (table, _scratch) = disk_table(&fixture, config).await;
            table.load().await.unwrap();
            observe(&table, &Ok(short_sample()));
        });
        assert_eq!(
            level_gauges(&series),
            level_bits(0, Level::Balanced, Level::Balanced)
        );
    }

    /// `strict` never acts on the page cache: a strict table short of it
    /// stays serving, a refresh swaps in the next snapshot, and the swap
    /// and a retune both keep the condition until the next sample.
    #[test]
    fn test_refresh_of_a_strict_disk_table_short_of_the_page_cache_swaps_and_keeps_it() {
        let (swapped, retuned) = recorded_with(|snapshotter| async move {
            let (mut fixture, all) = two_bucket_table().await;
            let strict = fixture.config(Residency::Disk, None);
            assert!(strict.strict);
            let (table, _scratch) = disk_table(&fixture, strict.clone()).await;
            table.load().await.unwrap();
            observe(&table, &Ok(short_sample()));
            assert_eq!(serving_id(&table), SnapshotId(FIRST_SNAPSHOT));
            append_second(&mut fixture, &all, "f2", WriterLayout::contract()).await;
            assert!(table.refresh().await.unwrap());
            assert_eq!(serving_id(&table), SnapshotId(SECOND_SNAPSHOT));
            let swapped = Series(snapshotter.snapshot().into_vec());
            let serialized = table.serialized().await;
            table.retune(
                TableConfig {
                    lookups: Some(Level::Balanced),
                    ..strict
                },
                &serialized,
            );
            swapped
        });
        assert_eq!(
            level_gauges(&swapped),
            level_bits(1, Level::Balanced, Level::Optimal)
        );
        assert_eq!(fraction_gauge(&swapped), short_fraction());
        assert_eq!(
            level_gauges(&retuned),
            level_bits(0, Level::Balanced, Level::Balanced)
        );
        assert_eq!(fraction_gauge(&retuned), short_fraction());
    }

    /// A sample of a generation swapped out since it began describes chunk
    /// files the table no longer serves from, and is dropped.
    #[test]
    fn test_observe_page_cache_drops_a_sample_of_a_generation_swapped_out() {
        let ((), series) = recorded(async {
            let (mut fixture, all) = two_bucket_table().await;
            let (table, _scratch) = disk_table(&fixture, lenient(&fixture)).await;
            table.load().await.unwrap();
            let stale = table.current().unwrap();
            append_second(&mut fixture, &all, "f2", WriterLayout::contract()).await;
            assert!(table.refresh().await.unwrap());
            table.observe_page_cache(&stale, &Ok(short_sample()));
            table.publish_gauges();
        });
        assert_eq!(
            level_gauges(&series),
            level_bits(0, Level::Optimal, Level::Optimal)
        );
        assert_eq!(series.named(metric::TABLE_CHUNK_RESIDENT_FRACTION), 0);
    }

    /// A sample that cannot observe a chunk file makes the condition
    /// unjudged: the cap is lifted, and the resident fraction is no longer
    /// written. A debugging snapshot sets every gauge it reads to zero, so
    /// the fraction reads zero after one only when nothing wrote it since.
    #[test]
    fn test_metrics_an_unobserved_sample_lifts_the_cap_and_stops_the_fraction() {
        let (short, unjudged) = recorded_with(|snapshotter| async move {
            let (fixture, _) = two_bucket_table().await;
            let (table, _scratch) = disk_table(&fixture, lenient(&fixture)).await;
            table.load().await.unwrap();
            observe(&table, &Ok(short_sample()));
            let short = Series(snapshotter.snapshot().into_vec());
            let unobserved = Err(Unobserved {
                path: std::path::PathBuf::from("chunk"),
                why: std::io::Error::from(std::io::ErrorKind::PermissionDenied).into(),
            });
            observe(&table, &unobserved);
            table.publish_gauges();
            short
        });
        assert_eq!(
            level_gauges(&short),
            level_bits(1, Level::Balanced, Level::Optimal)
        );
        assert_eq!(
            level_gauges(&unjudged),
            level_bits(0, Level::Optimal, Level::Optimal)
        );
        assert_eq!(fraction_gauge(&unjudged), 0.0_f64.to_bits());
    }

    /// A disk table just loaded has just written its chunk files, so the
    /// page cache holds them whole and the table is at its layout's level;
    /// a full table has no chunk file and is never judged.
    #[test]
    fn test_sample_page_cache_reads_a_just_loaded_disk_table_whole_and_skips_a_full_one() {
        let ((), series) = recorded(async {
            let (fixture, _) = two_bucket_table().await;
            let (table, _scratch) = disk_table(&fixture, lenient(&fixture)).await;
            table.load().await.unwrap();
            sample_page_cache(&[&table]).await;
            let full = table_over(
                &fixture,
                TableConfig {
                    name: STRICT_TABLE.to_string(),
                    ..lenient(&fixture)
                },
            )
            .await;
            full.load().await.unwrap();
            sample_page_cache(&[&full]).await;
        });
        assert_eq!(
            level_gauges(&series),
            level_bits(0, Level::Optimal, Level::Optimal)
        );
        assert_eq!(fraction_gauge(&series), 1.0_f64.to_bits());
        assert_eq!(series.named(metric::TABLE_CHUNK_RESIDENT_FRACTION), 1);
    }

    /// Evicting every chunk file of a disk table puts it below optimal at
    /// the next sample, and reading them back recovers it.
    #[cfg(target_os = "linux")]
    #[test]
    fn test_sample_page_cache_follows_chunk_files_evicted_and_read_back() {
        let (evicted, read_back) = recorded_with(|snapshotter| async move {
            let (fixture, _) = two_bucket_table().await;
            let (table, scratch) = disk_table(&fixture, lenient(&fixture)).await;
            table.load().await.unwrap();
            let local = local_dir(scratch.path(), fixture.storage.stats(), None);
            let paths: Vec<_> = table
                .current()
                .unwrap()
                .identities()
                .iter()
                .map(|identity| local.chunk_path(identity, ChunkRules::KeyValue))
                .collect();
            for path in &paths {
                evict(path);
            }
            sample_page_cache(&[&table]).await;
            let evicted = Series(snapshotter.snapshot().into_vec());
            for path in &paths {
                std::fs::read(path).unwrap();
            }
            sample_page_cache(&[&table]).await;
            evicted
        });
        assert_eq!(
            level_gauges(&evicted),
            level_bits(1, Level::Balanced, Level::Optimal)
        );
        assert_eq!(fraction_gauge(&evicted), 0.0_f64.to_bits());
        assert_eq!(
            level_gauges(&read_back),
            level_bits(0, Level::Optimal, Level::Optimal)
        );
        assert_eq!(fraction_gauge(&read_back), 1.0_f64.to_bits());
    }

    /// A retune of a lenient table's `lookups` declares the new level at
    /// once, before any poll: a table serving a balanced snapshot reads
    /// below its declared `optimal`, and retuned to `balanced` reads at its
    /// level.
    #[test]
    fn test_metrics_retune_declares_the_new_level_at_once() {
        let (before, after) = recorded_with(|snapshotter| async move {
            let fixture = one_file_fixture(WriterLayout {
                dictionary: Dictionary::Value,
                ..WriterLayout::contract()
            })
            .await;
            let table = table_over(&fixture, lenient(&fixture)).await;
            table.load().await.unwrap();
            let before = Series(snapshotter.snapshot().into_vec());
            let serialized = table.serialized().await;
            table.retune(
                TableConfig {
                    lookups: Some(Level::Balanced),
                    ..lenient(&fixture)
                },
                &serialized,
            );
            before
        });
        assert_eq!(
            level_gauges(&before),
            level_bits(1, Level::Balanced, Level::Optimal)
        );
        assert_eq!(
            level_gauges(&after),
            level_bits(0, Level::Balanced, Level::Balanced)
        );
    }

    /// A held table retuned to `strict` off serves on with the held
    /// snapshot's level measured, below its level; once its ref goes back
    /// to the serving snapshot, the poll finds it in step and the level
    /// gauges measure the serving snapshot again, though no state ended.
    #[test]
    fn test_refresh_in_step_after_a_retune_out_of_held_measures_the_serving_snapshot() {
        let (during, after) = recorded_with(|snapshotter| async move {
            let (mut fixture, all) = two_bucket_table().await;
            let table = table_over(&fixture, fixture.config(Residency::Full, None)).await;
            table.load().await.unwrap();
            let balanced = WriterLayout {
                dictionary: Dictionary::Value,
                ..WriterLayout::contract()
            };
            append_second(&mut fixture, &all, "dict", balanced).await;
            table.refresh().await.unwrap_err();
            assert_eq!(held(&table), HELD_AT_BALANCED);
            {
                let serialized = table.serialized().await;
                table.retune(lenient(&fixture), &serialized);
            }
            assert_eq!(serving_id(&table), SnapshotId(FIRST_SNAPSHOT));
            let during = Series(snapshotter.snapshot().into_vec());

            fixture.restore_hint(FIRST_VERSION).await;
            assert!(!table.refresh().await.unwrap());
            assert_eq!(serving_id(&table), SnapshotId(FIRST_SNAPSHOT));
            table.publish_gauges();
            during
        });
        assert_eq!(
            level_gauges(&during),
            level_bits(1, Level::Balanced, Level::Optimal)
        );
        assert_eq!(
            level_gauges(&after),
            level_bits(0, Level::Optimal, Level::Optimal)
        );
    }

    #[test]
    fn test_metrics_below_level_judges_the_held_snapshot() {
        let ((), series) = recorded(async {
            let (mut fixture, all) = two_bucket_table().await;
            let table = table_over(&fixture, fixture.config(Residency::Full, None)).await;
            table.load().await.unwrap();
            let balanced = WriterLayout {
                dictionary: Dictionary::Value,
                ..WriterLayout::contract()
            };
            append_second(&mut fixture, &all, "dict", balanced).await;
            table.refresh().await.unwrap_err();
            assert_eq!(held(&table), HELD_AT_BALANCED);
        });
        assert_eq!(
            level_gauges(&series),
            level_bits(1, Level::Balanced, Level::Optimal),
            "the held snapshot is below the level, though the serving one meets it"
        );
        assert_eq!(
            state_gauge(&series, TABLE, StateName::Held).to_bits(),
            approx::gauge(1).to_bits()
        );
        assert_eq!(
            state_gauge(&series, TABLE, StateName::Degraded).to_bits(),
            approx::gauge(0).to_bits()
        );
    }

    /// While a strict table is held on a snapshot one of whose columns
    /// turned dictionary-encoded, each column's level gauge describes the
    /// held snapshot, as the table's does: the table and that column read
    /// balanced, the other column optimal (RFD 020, RFD 022).
    #[test]
    fn test_metrics_column_levels_judge_the_held_snapshot() {
        /// A contract-layout file of `keys`, the long keys `all`, in bucket 0,
        /// with the columns `dictionary` names dictionary-encoded.
        fn file<'a>(
            name: &'a str,
            keys: &'a [Key],
            all: &[i64],
            dictionary: Dictionary,
        ) -> FileSpec<'a> {
            FileSpec {
                name,
                keys,
                values: values_of(all),
                partition: Partition::Bucket(0),
                bounds: Bounds::Exact,
                layout: WriterLayout {
                    dictionary,
                    ..WriterLayout::contract()
                },
            }
        }
        let ((), series) = recorded(async {
            let mut fixture = Fixture::with_detail(KeyType::Long, manifest_testing::bucket_spec(1));
            let all = keys_from(0, FILE_KEYS);
            fixture
                .add_file(file("f", &long_keys(&all), &all, Dictionary::Off))
                .await;
            fixture
                .commit(FIRST_SNAPSHOT, None, FIRST_SEQUENCE, FIRST_VERSION)
                .await;
            let table = table_over(&fixture, fixture.config(Residency::Full, None)).await;
            table.load().await.unwrap();
            let appended = keys_from(all.last().unwrap() + KEY_STEP, FILE_KEYS);
            fixture
                .add_file(file(
                    "dict",
                    &long_keys(&appended),
                    &appended,
                    Dictionary::Value,
                ))
                .await;
            fixture
                .commit(
                    SECOND_SNAPSHOT,
                    Some(FIRST_SNAPSHOT),
                    SECOND_SEQUENCE,
                    SECOND_VERSION,
                )
                .await;
            assert!(matches!(
                refusal_of(table.refresh().await),
                RefusalReason::BelowLevel { .. }
            ));
            assert!(matches!(table.state(), TableState::Held { .. }));
        });
        let level =
            |labels: &[(&str, String)]| series.gauge(metric::TABLE_COLUMN_LEVEL, labels).to_bits();
        let column = |name: &str| (metric::LABEL_COLUMN, name.to_string());
        assert_eq!(
            level_gauges(&series),
            level_bits(1, Level::Balanced, Level::Optimal)
        );
        assert_eq!(
            level(&[table_label(), column(VALUE_COLUMN)]),
            Level::Balanced.gauge().to_bits(),
            "the column dictionary-encoded in the held snapshot"
        );
        assert_eq!(
            level(&[table_label(), column(DETAIL_COLUMN)]),
            Level::Optimal.gauge().to_bits()
        );
    }

    #[test]
    fn test_metrics_refusal_outside_the_levels_leaves_the_level_series() {
        let ((), series) = recorded(async {
            let (mut fixture, all) = two_bucket_table().await;
            let table = table_over(&fixture, lenient(&fixture)).await;
            table.load().await.unwrap();
            commit_duplicate(
                &mut fixture,
                &all,
                SECOND_SNAPSHOT,
                FIRST_SNAPSHOT,
                SECOND_SEQUENCE,
                SECOND_VERSION,
            )
            .await;
            let reason = refusal_of(table.refresh().await);
            assert!(matches!(reason, RefusalReason::DuplicateKeys { .. }));
        });
        assert_eq!(
            level_gauges(&series),
            level_bits(0, Level::Optimal, Level::Optimal),
            "the refused snapshot overlaps, but only the serving one was measured"
        );
    }

    /// A held table whose ref goes back to the serving snapshot is in step
    /// with it without a swap: it serves, and the level gauges measure the
    /// serving snapshot again.
    #[test]
    fn test_refresh_ref_back_on_the_serving_snapshot_ends_held_at_its_level() {
        let ((), series) = recorded(async {
            let (mut fixture, all) = two_bucket_table().await;
            let table = table_over(&fixture, fixture.config(Residency::Full, None)).await;
            table.load().await.unwrap();
            let balanced = WriterLayout {
                dictionary: Dictionary::Value,
                ..WriterLayout::contract()
            };
            append_second(&mut fixture, &all, "dict", balanced).await;
            table.refresh().await.unwrap_err();
            assert_eq!(held(&table), HELD_AT_BALANCED);

            fixture.restore_hint(FIRST_VERSION).await;
            assert!(!table.refresh().await.unwrap());
            assert_eq!(serving_id(&table), SnapshotId(FIRST_SNAPSHOT));
        });
        assert_eq!(
            level_gauges(&series),
            level_bits(0, Level::Optimal, Level::Optimal)
        );
        assert_eq!(
            state_gauge(&series, TABLE, StateName::Serving).to_bits(),
            approx::gauge(1).to_bits()
        );
        assert_eq!(
            state_gauge(&series, TABLE, StateName::Held).to_bits(),
            approx::gauge(0).to_bits()
        );
    }

    #[test]
    fn test_refresh_unchanged_after_a_failed_poll_returns_to_serving() {
        let ((), series) = recorded(async {
            let (fixture, all) = two_bucket_table().await;
            let table = table_over(&fixture, fixture.config(Residency::Full, None)).await;
            table.load().await.unwrap();
            fixture.remove_hint();
            let error = table.refresh().await.unwrap_err();
            assert!(matches!(error, Error::ObjectStore(_)), "{error:?}");
            let (serving, kept) = degraded(&table);
            assert_eq!(serving, Some(SnapshotId(FIRST_SNAPSHOT)));
            assert_eq!(kept, ErrorKind::ObjectStore);
            assert_eq!(
                table
                    .get(&Key::Long(all[0]), &[ColumnRead::whole(VALUE_COLUMN)])
                    .await
                    .unwrap()
                    .row,
                present(all[0])
            );

            fixture.restore_hint(FIRST_VERSION).await;
            let before = fixture.gets();
            assert!(!table.refresh().await.unwrap());
            assert_eq!(fixture.gets(), before + POLL_GETS);
            assert_eq!(serving_id(&table), SnapshotId(FIRST_SNAPSHOT));
        });
        assert_eq!(
            state_gauge(&series, TABLE, StateName::Serving).to_bits(),
            approx::gauge(1).to_bits()
        );
        assert_eq!(
            state_gauge(&series, TABLE, StateName::Degraded).to_bits(),
            approx::gauge(0).to_bits()
        );
        assert_eq!(
            series.counter(
                metric::REFRESH_ERRORS_TOTAL,
                &[
                    table_label(),
                    (metric::LABEL_REASON, ErrorKind::ObjectStore.to_string())
                ]
            ),
            1
        );
    }

    /// A snapshot refused for a delete file icetray cannot read (RFD 021)
    /// is remembered against its metadata location: while the version hint
    /// names that location each poll reads the hint alone, counts no error,
    /// and leaves the table degraded for the refusal, a transient failure
    /// in between included; once the hint names another location, its
    /// snapshot is built.
    #[test]
    fn test_refresh_does_not_build_a_refused_snapshot_again_until_its_location_moves() {
        let deletes = "deletes";
        let serving = Some(SnapshotId(FIRST_SNAPSHOT));
        let (refused, series) = recorded(async {
            let (mut fixture, all) = two_bucket_table().await;
            let refused = ErrorKind::Refused(RefusalReason::DeleteKindUnsupported {
                path: fixture.path_of(deletes),
                kind: UnsupportedDelete::AvroPositions,
            });
            let table = table_over(&fixture, fixture.config(Residency::Full, None)).await;
            table.load().await.unwrap();
            fixture.add_avro_position_delete(deletes, 0);
            fixture
                .commit(
                    SECOND_SNAPSHOT,
                    Some(FIRST_SNAPSHOT),
                    SECOND_SEQUENCE,
                    SECOND_VERSION,
                )
                .await;

            let before = fixture.gets();
            let error = table.refresh().await.unwrap_err();
            assert_eq!(error.kind(), refused, "{error:?}");
            assert!(
                fixture.gets() > before + POLL_GETS,
                "the first poll reads the snapshot's metadata and manifests"
            );
            let polls_while_unchanged = 3;
            for _ in 0..polls_while_unchanged {
                let before = fixture.gets();
                assert!(!table.refresh().await.unwrap());
                assert_eq!(fixture.gets(), before + POLL_GETS, "the version hint alone");
                assert_eq!(degraded(&table), (serving, refused.clone()));
            }

            fixture.remove_hint();
            let error = table.refresh().await.unwrap_err();
            assert!(matches!(error, Error::ObjectStore(_)), "{error:?}");
            assert_eq!(degraded(&table), (serving, ErrorKind::ObjectStore));
            fixture.restore_hint(SECOND_VERSION).await;
            let before = fixture.gets();
            assert!(!table.refresh().await.unwrap());
            assert_eq!(fixture.gets(), before + POLL_GETS);
            assert_eq!(degraded(&table), (serving, refused.clone()));
            assert_eq!(
                table
                    .get(&Key::Long(all[0]), &[ColumnRead::whole(VALUE_COLUMN)])
                    .await
                    .unwrap(),
                Lookup {
                    row: present(all[0]),
                    snapshot_id: SnapshotId(FIRST_SNAPSHOT),
                }
            );

            fixture.remove_file(deletes);
            fixture
                .commit(
                    THIRD_SNAPSHOT,
                    Some(SECOND_SNAPSHOT),
                    THIRD_SEQUENCE,
                    THIRD_VERSION,
                )
                .await;
            assert!(table.refresh().await.unwrap());
            assert_eq!(serving_id(&table), SnapshotId(THIRD_SNAPSHOT));
            let before = fixture.gets();
            assert!(!table.refresh().await.unwrap());
            assert_eq!(fixture.gets(), before + POLL_GETS);
            refused
        });
        let errors = |reason: &ErrorKind| {
            series.counter(
                metric::REFRESH_ERRORS_TOTAL,
                &[table_label(), (metric::LABEL_REASON, reason.to_string())],
            )
        };
        assert_eq!(errors(&refused), 1, "counted once, when made");
        assert_eq!(errors(&ErrorKind::ObjectStore), 1);
        assert_eq!(
            state_gauge(&series, TABLE, StateName::Serving).to_bits(),
            approx::gauge(1).to_bits()
        );
    }

    /// A snapshot a strict table refuses as below its level is remembered
    /// against its metadata location like any refusal: while the version
    /// hint names that location each poll reads the hint alone, counts no
    /// error, and leaves the table held for the refusal.
    #[test]
    fn test_refresh_does_not_judge_a_snapshot_below_its_level_again_until_its_location_moves() {
        let refused = ErrorKind::Refused(dictionary_below_optimal());
        let ((), series) = recorded(async {
            let (mut fixture, all) = two_bucket_table().await;
            let table = table_over(&fixture, fixture.config(Residency::Full, None)).await;
            assert!(table.config().strict);
            table.load().await.unwrap();
            let balanced = WriterLayout {
                dictionary: Dictionary::Value,
                ..WriterLayout::contract()
            };
            append_second(&mut fixture, &all, "dict", balanced).await;

            let before = fixture.gets();
            let error = table.refresh().await.unwrap_err();
            assert_eq!(error.kind(), refused, "{error:?}");
            assert!(
                fixture.gets() > before + POLL_GETS,
                "the first poll reads the snapshot's metadata and footers"
            );
            let polls_while_unchanged = 3;
            for _ in 0..polls_while_unchanged {
                let before = fixture.gets();
                assert!(!table.refresh().await.unwrap());
                assert_eq!(fixture.gets(), before + POLL_GETS, "the version hint alone");
                assert_eq!(held(&table), HELD_AT_BALANCED);
            }
        });
        assert_eq!(
            series.counter(
                metric::REFRESH_ERRORS_TOTAL,
                &[table_label(), (metric::LABEL_REASON, refused.to_string())],
            ),
            1,
            "counted once, when made"
        );
    }

    /// A ref the metadata does not have is a table fault fixed for that
    /// metadata file, and is remembered against its location like a
    /// refusal: while the version hint names it each poll reads the hint
    /// alone, counts no error, and leaves the table degraded for the
    /// fault; once the hint names metadata holding the ref again, its
    /// snapshot is built.
    #[test]
    fn test_refresh_does_not_read_metadata_lacking_the_ref_again_until_its_location_moves() {
        let faulted = ErrorKind::Table;
        let serving = Some(SnapshotId(FIRST_SNAPSHOT));
        let ((), series) = recorded(async {
            let (mut fixture, _) = two_bucket_table().await;
            let table = table_over(&fixture, fixture.config(Residency::Full, None)).await;
            table.load().await.unwrap();
            fixture.drop_main(SECOND_VERSION).await;

            let before = fixture.gets();
            let error = table.refresh().await.unwrap_err();
            assert!(
                matches!(
                    error,
                    Error::Table {
                        fault: TableFault::RefNotFound { .. },
                        ..
                    }
                ),
                "{error:?}"
            );
            assert!(
                fixture.gets() > before + POLL_GETS,
                "the first poll reads the metadata file"
            );
            let polls_while_unchanged = 3;
            for _ in 0..polls_while_unchanged {
                let before = fixture.gets();
                assert!(!table.refresh().await.unwrap());
                assert_eq!(fixture.gets(), before + POLL_GETS, "the version hint alone");
                assert_eq!(degraded(&table), (serving, faulted.clone()));
            }

            fixture
                .commit(
                    SECOND_SNAPSHOT,
                    Some(FIRST_SNAPSHOT),
                    SECOND_SEQUENCE,
                    THIRD_VERSION,
                )
                .await;
            assert!(table.refresh().await.unwrap());
            assert_eq!(serving_id(&table), SnapshotId(SECOND_SNAPSHOT));
        });
        let errors = series.counter(
            metric::REFRESH_ERRORS_TOTAL,
            &[table_label(), (metric::LABEL_REASON, faulted.to_string())],
        );
        assert_eq!(errors, 1, "counted once, when met");
    }

    /// The `findings` gauge of `code` for the table `table`.
    fn findings_gauge(series: &Series, table: &str, code: FindingCode) -> f64 {
        let mut labels = vec![
            (metric::LABEL_TABLE, table.to_string()),
            (metric::LABEL_CODE, code.to_string()),
        ];
        if code.per_column() {
            labels.push((metric::LABEL_COLUMN, VALUE_COLUMN.to_string()));
        }
        series.gauge(metric::FINDINGS, &labels)
    }

    #[test]
    fn test_metrics_findings_and_index_bytes_describe_the_serving_generation() {
        let balanced = WriterLayout {
            dictionary: Dictionary::Value,
            ..WriterLayout::contract()
        };
        let ((lenient_bytes, strict_bytes), series) = recorded(async {
            let fixture = one_file_fixture(balanced).await;
            let lenient = table_over(&fixture, lenient(&fixture)).await;
            lenient.load().await.unwrap();
            let lenient_bytes = lenient.current().unwrap().index_bytes;

            let (mut fixture, all) = two_bucket_table().await;
            let strict = table_over(
                &fixture,
                TableConfig {
                    name: STRICT_TABLE.to_string(),
                    ..fixture.config(Residency::Full, None)
                },
            )
            .await;
            strict.load().await.unwrap();
            let strict_bytes = strict.current().unwrap().index_bytes;
            append_second(&mut fixture, &all, "dict", balanced).await;
            assert!(matches!(
                strict.refresh().await.unwrap_err(),
                Error::Refused {
                    reason: RefusalReason::BelowLevel { .. },
                    ..
                }
            ));
            (lenient_bytes, strict_bytes)
        });
        assert!(lenient_bytes > 0);
        assert_eq!(
            findings_gauge(&series, TABLE, FindingCode::DictionaryEncodedValue).to_bits(),
            approx::gauge(1).to_bits()
        );
        assert_eq!(
            findings_gauge(&series, TABLE, FindingCode::NoBloomFilter).to_bits(),
            approx::gauge(0).to_bits()
        );
        assert_eq!(
            series
                .gauge(metric::INDEX_BYTES, &[table_label()])
                .to_bits(),
            approx::gauge_len(lenient_bytes).to_bits()
        );
        assert_eq!(
            findings_gauge(&series, STRICT_TABLE, FindingCode::DictionaryEncodedValue).to_bits(),
            approx::gauge(0).to_bits(),
            "a refused generation's findings never reach the gauge"
        );
        assert_eq!(
            series
                .gauge(
                    metric::INDEX_BYTES,
                    &[(metric::LABEL_TABLE, STRICT_TABLE.to_string())]
                )
                .to_bits(),
            approx::gauge_len(strict_bytes).to_bits()
        );
    }

    /// A lookup that reads nothing, a warm hit or a Full-tier miss,
    /// completes on its first poll, so the generation guard it holds is
    /// never parked on its thread's fast slots; a cold hit, which waits on
    /// its page's read, holds one across the wait.
    #[tokio::test]
    async fn test_a_lookup_that_reads_nothing_completes_on_its_first_poll() {
        use futures::FutureExt as _;

        let (fixture, all) = two_bucket_table().await;
        // A plain local read can finish on the blocking pool before the poll
        // that started it looks, and let a cold hit be ready at once; reads
        // go through a gate the test closes, so the cold hit waits.
        let gate = Arc::new(Gauged::new(Arc::new(LocalFileSystem::new())));
        fixture
            .storage
            .register(FILE_PREFIX, Arc::clone(&gate) as _);
        let table = table_over(&fixture, fixture.config(Residency::Full, None)).await;
        table.load().await.unwrap();
        let key = Key::Long(all[0]);
        gate.fill_to(2);
        assert!(
            table
                .get(&key, &[ColumnRead::whole(VALUE_COLUMN)])
                .now_or_never()
                .is_none(),
            "a cold hit waits on its page's read"
        );
        gate.fill_to(0);
        table
            .get(&key, &[ColumnRead::whole(VALUE_COLUMN)])
            .await
            .unwrap();
        let warm = table
            .get(&key, &[ColumnRead::whole(VALUE_COLUMN)])
            .now_or_never();
        assert_eq!(
            warm.expect("a warm hit is ready").unwrap().row,
            present(all[0])
        );
        let absent = Key::Long(all.iter().max().unwrap() + 1);
        let miss = table
            .get(&absent, &[ColumnRead::whole(VALUE_COLUMN)])
            .now_or_never();
        assert_eq!(miss.expect("a miss is ready").unwrap().row, Row::Absent);
    }

    /// A table built hidden over `fixture`, as a reload builds one.
    async fn hidden_over(fixture: &Fixture) -> Table {
        let defaults = ServerConfig::default();
        Table::hidden(
            fixture.config(Residency::Full, None),
            TableSource::Static {
                location: fixture.root(),
            },
            handles_over(fixture).await,
            defaults.load_deadline,
            defaults.refresh_deadline,
            defaults.get_deadline,
            defaults.scan_deadline,
        )
    }

    /// A table's counters are registered at zero when it is built, before
    /// any lookup or swap, so each series exists from the start.
    #[test]
    fn test_new_registers_the_table_s_counters_at_zero() {
        let ((), series) = recorded(async {
            let (fixture, _) = two_bucket_table().await;
            table_over(&fixture, fixture.config(Residency::Full, None)).await;
        });
        for result in LookupResult::ALL {
            let labels = [table_label(), (metric::LABEL_RESULT, result.to_string())];
            assert_eq!(
                series.counter(metric::LOOKUPS_TOTAL, &labels),
                0,
                "{result}"
            );
        }
        assert_eq!(
            series.counter(metric::GENERATION_SWAPS_TOTAL, &[table_label()]),
            0
        );
    }

    /// A hidden table writes no gauge while it loads, nor when a scrape
    /// publishes, so the table serving under its name keeps its series;
    /// `show` then publishes its state, its generation's gauges, and the
    /// level gauges as the swap set them.
    #[test]
    fn test_a_hidden_table_writes_its_gauges_once_shown() {
        let (index_bytes, loading) = recorded(async {
            let (fixture, _) = two_bucket_table().await;
            let table = hidden_over(&fixture).await;
            table.load().await.unwrap();
            table.publish_gauges();
            table.current().unwrap().index_bytes
        });
        let gauges = [
            metric::TABLE_STATE,
            metric::INDEX_BYTES,
            metric::FINDINGS,
            metric::GENERATION_SEQUENCE_NUMBER,
            metric::GENERATION_SWAPPED_AT_SECONDS,
            metric::TABLE_BELOW_LEVEL,
            metric::TABLE_LEVEL,
            metric::TABLE_DECLARED_LEVEL,
            metric::TABLE_GETS_PER_HIT,
            metric::TABLE_GETS_PER_MISS,
            metric::TABLE_FILES_PROBED_PER_LOOKUP,
            metric::TABLE_DELETED_ROWS,
            metric::TABLE_DELETED_FRACTION,
            metric::TABLE_DELETE_FILES,
            metric::TABLE_DELETE_STATE_BYTES,
        ];
        for name in gauges {
            assert_eq!(loading.named(name), 0, "{name}");
        }

        let ((), shown) = recorded(async {
            let (fixture, _) = two_bucket_table().await;
            let table = hidden_over(&fixture).await;
            table.load().await.unwrap();
            table.show();
        });
        assert_eq!(
            state_gauge(&shown, TABLE, StateName::Serving).to_bits(),
            approx::gauge(1).to_bits()
        );
        assert_eq!(
            shown.gauge(metric::INDEX_BYTES, &[table_label()]).to_bits(),
            approx::gauge_len(index_bytes).to_bits()
        );
        assert_eq!(
            level_gauges(&shown),
            level_bits(0, Level::Optimal, Level::Optimal)
        );
        assert_eq!(cost_gauges(&shown), cost_bits(CONTRACT_COST));
        assert_eq!(delete_gauges(&shown), delete_bits(DeleteSummary::default()));
    }

    /// A withdrawn table writes no gauge: a refresh that fails after the
    /// withdrawal, which would read `Degraded`, and a scrape's publish
    /// leave its series as the withdrawal found them, for the binary's
    /// recorder to drop.
    #[test]
    fn test_a_withdrawn_table_writes_no_gauge() {
        let ((), series) = recorded(async {
            let (fixture, _) = two_bucket_table().await;
            let table = table_over(&fixture, fixture.config(Residency::Full, None)).await;
            table.load().await.unwrap();
            table.withdraw();
            fixture.remove_hint();
            table.refresh().await.unwrap_err();
            table.publish_gauges();
        });
        for name in StateName::ALL {
            let shown = u64::from(name == StateName::Serving);
            assert_eq!(
                state_gauge(&series, TABLE, name).to_bits(),
                approx::gauge(shown).to_bits(),
                "{name}"
            );
        }
    }

    #[test]
    fn test_fmt_table_state_label_values() {
        let cases = [
            (TableState::Loading, "loading"),
            (
                TableState::Serving {
                    snapshot_id: SnapshotId(FIRST_SNAPSHOT),
                    since: Instant::now(),
                },
                "serving",
            ),
            (
                TableState::Degraded {
                    serving: None,
                    reason: ErrorKind::ObjectStore,
                    since: Instant::now(),
                },
                "degraded",
            ),
            (
                TableState::Held {
                    serving: SnapshotId(FIRST_SNAPSHOT),
                    refused: SnapshotId(SECOND_SNAPSHOT),
                    level: Level::Balanced,
                    since: Instant::now(),
                },
                "held",
            ),
        ];
        for (state, name) in cases {
            assert_eq!(state.to_string(), name);
        }
    }

    // Scans of an ordered table (RFD 025).

    /// The fan-out the scan fan-out tests build their tables under: small,
    /// so a few files' worth of row groups outnumber it several times.
    const SCAN_FAN_OUT: usize = 4;

    /// Rows per key of the scan fan-out tests, each in a row group of its
    /// own, so each costs one GET per column and one for the order column.
    const SCAN_ROWS_PER_KEY: i64 = 6;

    /// The first order value of a key's run, and the step between its
    /// rows.
    const ORDER_START: i64 = 100;
    const ORDER_STEP: i64 = 10;

    /// The rows of `keys`, each `rows` rows ascending by order value from
    /// `ORDER_START`, in file order.
    fn ordered_rows(keys: &[i64], rows: i64) -> Vec<(i64, i64)> {
        keys.iter()
            .flat_map(|&key| (0..rows).map(move |at| (key, ORDER_START + at * ORDER_STEP)))
            .collect()
    }

    /// A loaded ordered table of one file holding `rows`, written under
    /// `layout`, built under a fan-out of `fan_out`, its file read through
    /// a `Gauged` store, which passes every GET until a test gates it.
    async fn ordered_table_of(
        rows: &[(i64, i64)],
        layout: WriterLayout,
        fan_out: usize,
    ) -> (Table, Fixture, Arc<Gauged>) {
        ordered_table_over(&[rows], layout, fan_out).await
    }

    /// `ordered_table_of` over one file per entry of `files`, each in the
    /// one bucket, in that order.
    async fn ordered_table_over(
        files: &[&[(i64, i64)]],
        layout: WriterLayout,
        fan_out: usize,
    ) -> (Table, Fixture, Arc<Gauged>) {
        let (table, fixture, gauged) = unloaded_ordered_table_over(files, layout, fan_out).await;
        table.load().await.unwrap();
        (table, fixture, gauged)
    }

    /// `ordered_table_over`, built and not loaded: it serves no generation.
    async fn unloaded_ordered_table_over(
        files: &[&[(i64, i64)]],
        layout: WriterLayout,
        fan_out: usize,
    ) -> (Table, Fixture, Arc<Gauged>) {
        let mut fixture = Fixture::ordered(1);
        for (at, rows) in files.iter().enumerate() {
            fixture
                .add_ordered_file(&file_at(at), rows, Partition::Bucket(0), layout)
                .await;
        }
        fixture
            .commit(FIRST_SNAPSHOT, None, FIRST_SEQUENCE, FIRST_VERSION)
            .await;
        let gauged = Arc::new(Gauged::new(Arc::new(LocalFileSystem::new())));
        fixture
            .storage
            .register(FILE_PREFIX, Arc::<Gauged>::clone(&gauged));
        let handles = TableHandles {
            batch_fan_out: fan_out,
            ..handles_over(&fixture).await
        };
        let defaults = ServerConfig::default();
        let table = table_sharing(
            &fixture,
            lenient(&fixture),
            handles,
            defaults.load_deadline,
            defaults.refresh_deadline,
        );
        (table, fixture, gauged)
    }

    /// One row a row group of its own, one row a page.
    fn row_per_row_group() -> WriterLayout {
        WriterLayout {
            page_rows: 1,
            row_group_rows: Some(1),
            ..WriterLayout::contract()
        }
    }

    /// Every row, oldest first, at the test tables' `max_scan_rows`.
    fn oldest_first() -> ScanBounds {
        ScanBounds {
            direction: Direction::OldestFirst,
            limit: DEFAULT_MAX_SCAN_ROWS,
            start: None,
            before: None,
        }
    }

    /// What a Scan naming `DETAIL_COLUMN` and then the value column
    /// answers for `rows`, (key, order value) pairs of one key, in that
    /// order.
    fn scanned_both(rows: &[(i64, i64)]) -> Vec<ScanRow> {
        rows.iter()
            .map(|&(key, order)| {
                let value = testing::row_value_for(key, order);
                ScanRow {
                    order: OrderValue::Long(order),
                    values: vec![
                        Some(Bytes::from(detail_of(&value))),
                        Some(Bytes::from(value)),
                    ],
                }
            })
            .collect()
    }

    /// A Scan of a key whose run spans many row groups holds at most
    /// `batch_fan_out` GETs in flight across them, and fills it: every row
    /// group's order span and each column's span are GETs of their own,
    /// three times the bound's worth (RFD 025, Limits).
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_scan_holds_at_most_batch_fan_out_gets() {
        let rows = ordered_rows(&[KEY_STEP], i64::try_from(SCAN_FAN_OUT).unwrap());
        let (table, fixture, gauged) =
            ordered_table_of(&rows, row_per_row_group(), SCAN_FAN_OUT).await;
        let gets_per_row_group = BOTH_READS.len() + 1;
        gauged.fill_to(SCAN_FAN_OUT);
        let before = fixture.gets();
        let answered = table
            .scan(&Key::Long(KEY_STEP), &BOTH_READS, &oldest_first())
            .await
            .unwrap();
        assert_eq!(answered.scanned.rows, scanned_both(&rows));
        assert_eq!(answered.scanned.next, None);
        assert_eq!(
            fixture.gets() - before,
            approx::count(rows.len() * gets_per_row_group),
            "one GET per row group for each column and the order column"
        );
        assert_eq!(
            gauged.high_water(),
            SCAN_FAN_OUT,
            "the Scan fills the bound and never passes it"
        );
    }

    /// A `MultiScan`'s keys share one bound of `batch_fan_out` GETs in
    /// flight, which they fill, and answer in key order, a key listed
    /// twice at each position (RFD 025, Limits).
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_multi_scan_holds_at_most_batch_fan_out_gets() {
        let keys = keys_from(0, i64::try_from(SCAN_FAN_OUT).unwrap());
        let rows = ordered_rows(&keys, SCAN_ROWS_PER_KEY);
        let (table, _fixture, gauged) =
            ordered_table_of(&rows, row_per_row_group(), SCAN_FAN_OUT).await;
        let mut asked = long_keys(&keys);
        asked.push(Key::Long(keys[0]));
        gauged.fill_to(SCAN_FAN_OUT);
        let every_row = ScanBounds {
            limit: u32::try_from(SCAN_ROWS_PER_KEY).unwrap(),
            ..oldest_first()
        };
        let answered = table
            .multi_scan(&asked, &BOTH_READS, &every_row)
            .await
            .unwrap();
        let expected: Vec<Vec<ScanRow>> = asked
            .iter()
            .map(|key| {
                let Key::Long(key) = key else {
                    unreachable!("long keys")
                };
                scanned_both(&ordered_rows(&[*key], SCAN_ROWS_PER_KEY))
            })
            .collect();
        let got: Vec<Vec<ScanRow>> = answered
            .results
            .iter()
            .map(|scanned| scanned.rows.clone())
            .collect();
        assert_eq!(got, expected, "every key in order, the repeat again");
        assert_eq!(
            gauged.high_water(),
            SCAN_FAN_OUT,
            "the keys fill the bound together and never pass it"
        );
    }

    /// Keys of the coalescing tests, all in one page of each column of
    /// one row group, so every key's Scan reads the same pages.
    const KEYS_IN_ONE_PAGE: i64 = 4;

    /// The GETs one cold Scan of a key in one file and one page costs in
    /// the Full tier: one span per column it names and one for the order
    /// column.
    fn gets_per_cold_scan() -> usize {
        BOTH_READS.len() + 1
    }

    /// Waits, under `DEADLINE_GUARD`, until two span reads of the same
    /// `gets` pages have both reached them while `gauged` holds every GET:
    /// the second waits on the first's fetches, which `cache` counts as
    /// joins, or, were nothing coalesced, holds GETs of its own beside the
    /// first's.
    async fn both_reached(gauged: &Gauged, cache: &PageCache, gets: usize) {
        tokio::time::timeout(DEADLINE_GUARD, async {
            loop {
                let joined =
                    gauged.high_water() >= gets && cache.span_joins() >= approx::count(gets);
                if joined || gauged.high_water() >= 2 * gets {
                    return;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("both reads reach their pages");
    }

    /// Two Scans of one cold key at once fetch each page once between them
    /// (RFD 025, Caching and hedging): the second waits on the first's
    /// span fetches rather than issuing its own.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_concurrent_scans_of_one_cold_key_fetch_each_page_once() {
        let rows = ordered_rows(&keys_from(0, KEYS_IN_ONE_PAGE), SCAN_ROWS_PER_KEY);
        let (table, fixture, gauged) =
            ordered_table_of(&rows, WriterLayout::contract(), DEFAULT_BATCH_FAN_OUT).await;
        let cache = &table.scans.as_ref().unwrap().cache;
        let key = Key::Long(KEY_STEP);
        let gets = gets_per_cold_scan();
        let bounds = oldest_first();
        gauged.hold();
        let before = fixture.gets();
        let (first, second, ()) = tokio::join!(
            table.scan(&key, &BOTH_READS, &bounds),
            table.scan(&key, &BOTH_READS, &bounds),
            async {
                both_reached(&gauged, cache, gets).await;
                gauged.release();
            }
        );
        let expected = scanned_both(&ordered_rows(&[KEY_STEP], SCAN_ROWS_PER_KEY));
        assert_eq!(first.unwrap().scanned.rows, expected);
        assert_eq!(second.unwrap().scanned.rows, expected);
        assert_eq!(
            fixture.gets() - before,
            approx::count(gets),
            "one GET per page between the two Scans"
        );
        assert_eq!(gauged.high_water(), gets);
    }

    /// A `MultiScan` of two keys whose runs share every page fetches each
    /// page once: the second key's read waits on the first's.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_multi_scan_of_keys_sharing_a_page_fetches_it_once() {
        let rows = ordered_rows(&keys_from(0, KEYS_IN_ONE_PAGE), SCAN_ROWS_PER_KEY);
        let (table, fixture, gauged) =
            ordered_table_of(&rows, WriterLayout::contract(), DEFAULT_BATCH_FAN_OUT).await;
        let cache = &table.scans.as_ref().unwrap().cache;
        let keys = [Key::Long(KEY_STEP), Key::Long(2 * KEY_STEP)];
        let every_row = ScanBounds {
            limit: u32::try_from(SCAN_ROWS_PER_KEY).unwrap(),
            ..oldest_first()
        };
        let gets = gets_per_cold_scan();
        gauged.hold();
        let before = fixture.gets();
        let (answered, ()) =
            tokio::join!(table.multi_scan(&keys, &BOTH_READS, &every_row), async {
                both_reached(&gauged, cache, gets).await;
                gauged.release();
            });
        let got: Vec<Vec<ScanRow>> = answered
            .unwrap()
            .results
            .into_iter()
            .map(|scanned| scanned.rows)
            .collect();
        assert_eq!(
            got,
            [
                scanned_both(&ordered_rows(&[KEY_STEP], SCAN_ROWS_PER_KEY)),
                scanned_both(&ordered_rows(&[2 * KEY_STEP], SCAN_ROWS_PER_KEY)),
            ]
        );
        assert_eq!(
            fixture.gets() - before,
            approx::count(gets),
            "one GET per shared page"
        );
    }

    /// An ordered table's `max_scan_rows` of zero in a configuration built
    /// by hand, which no validation saw, is refused as validation refuses
    /// it, by the load and by a retune, so no Scan answers no rows with a
    /// `next` bound.
    #[tokio::test]
    async fn test_load_and_retune_refuse_a_max_scan_rows_of_zero() {
        let is_zero = |error: &Error| {
            matches!(
                error,
                Error::Config(ConfigError::Invalid(ConfigViolation::MaxScanRowsZero {
                    index: None,
                    ..
                }))
            )
        };
        let rows = ordered_rows(&keys_from(0, KEYS_IN_ONE_PAGE), SCAN_ROWS_PER_KEY);
        let (serving, fixture, _gauged) =
            ordered_table_of(&rows, WriterLayout::contract(), DEFAULT_BATCH_FAN_OUT).await;
        let zero = TableConfig {
            max_scan_rows: Some(0),
            ..lenient(&fixture)
        };
        let refused = serving.judge_retune(&zero).unwrap_err();
        assert!(is_zero(&refused), "{refused:?}");

        let defaults = ServerConfig::default();
        let table = table_sharing(
            &fixture,
            zero,
            handles_over(&fixture).await,
            defaults.load_deadline,
            defaults.refresh_deadline,
        );
        let refused = table.load().await.unwrap_err();
        assert!(is_zero(&refused), "{refused:?}");
        let scanned = table
            .scan(&Key::Long(KEY_STEP), &BOTH_READS, &oldest_first())
            .await;
        assert!(scanned.is_err(), "{scanned:?}");
    }

    /// A `MultiScan` that lists a key twice reads it once and answers it at
    /// both positions: one Scan counted, one cold Scan's GETs.
    #[test]
    fn test_multi_scan_answers_a_repeated_key_from_one_read() {
        let rows = ordered_rows(&keys_from(0, KEYS_IN_ONE_PAGE), SCAN_ROWS_PER_KEY);
        let ((gets, results), series) = recorded(async {
            let (table, fixture, _gauged) =
                ordered_table_of(&rows, WriterLayout::contract(), DEFAULT_BATCH_FAN_OUT).await;
            let key = Key::Long(KEY_STEP);
            let every_row = ScanBounds {
                limit: u32::try_from(SCAN_ROWS_PER_KEY).unwrap(),
                ..oldest_first()
            };
            let before = fixture.gets();
            let answered = table
                .multi_scan(&[key.clone(), key], &BOTH_READS, &every_row)
                .await
                .unwrap();
            (fixture.gets() - before, answered.results)
        });
        let expected = scanned_both(&ordered_rows(&[KEY_STEP], SCAN_ROWS_PER_KEY));
        assert_eq!(results.len(), 2);
        for scanned in &results {
            assert_eq!(scanned.rows, expected);
        }
        assert_eq!(gets, approx::count(gets_per_cold_scan()));
        assert_eq!(
            series.counter(
                metric::SCANS_TOTAL,
                &[
                    table_label(),
                    (metric::LABEL_RESULT, ScanResult::Rows.to_string())
                ],
            ),
            1,
            "one read of the repeated key"
        );
    }

    /// A Scan of a key two files hold reads both files in one round (RFD
    /// 025, Cost): the store holds every GET until both files' order and
    /// value spans are in flight together, which a file-by-file read never
    /// reaches.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_scan_reads_every_file_holding_the_key_in_one_round() {
        let keys = keys_from(0, KEYS_IN_ONE_PAGE);
        let first = ordered_rows(&keys, SCAN_ROWS_PER_KEY);
        let later_start = ORDER_START + SCAN_ROWS_PER_KEY * ORDER_STEP;
        let second: Vec<(i64, i64)> = keys
            .iter()
            .flat_map(|&key| {
                (0..SCAN_ROWS_PER_KEY).map(move |at| (key, later_start + at * ORDER_STEP))
            })
            .collect();
        let (table, _fixture, gauged) = ordered_table_over(
            &[&first, &second],
            WriterLayout::contract(),
            DEFAULT_BATCH_FAN_OUT,
        )
        .await;
        let files = 2;
        let both_rounds = files * gets_per_cold_scan();
        gauged.fill_to(both_rounds);
        let answered = tokio::time::timeout(
            DEADLINE_GUARD,
            table.scan(&Key::Long(KEY_STEP), &BOTH_READS, &oldest_first()),
        )
        .await
        .expect("both files' GETs are in flight together")
        .unwrap();
        let mut expected = scanned_both(&ordered_rows(&[KEY_STEP], SCAN_ROWS_PER_KEY));
        expected.extend(scanned_both(
            &second
                .iter()
                .copied()
                .filter(|&(key, _)| key == KEY_STEP)
                .collect::<Vec<_>>(),
        ));
        assert_eq!(answered.scanned.rows, expected);
        assert_eq!(gauged.high_water(), both_rounds);
    }

    /// A Get or `MultiGet` of an ordered table, and a Scan or `MultiScan` of a
    /// key-value one, is `Error::WrongTableKind` naming the table's kind,
    /// before any read (RFD 025, Wire).
    #[tokio::test]
    async fn test_calls_of_the_wrong_kind_are_refused_before_any_read() {
        let rows = ordered_rows(&[KEY_STEP], SCAN_ROWS_PER_KEY);
        let (ordered, fixture, _gauged) =
            ordered_table_of(&rows, WriterLayout::contract(), DEFAULT_BATCH_FAN_OUT).await;
        let key = Key::Long(KEY_STEP);
        let before = fixture.gets();
        let wrong = |error: Error, expected: TableKind| {
            assert!(
                matches!(&error, Error::WrongTableKind { table, kind } if table == TABLE && *kind == expected),
                "{error:?}"
            );
        };
        wrong(
            ordered.get(&key, &BOTH_READS).await.unwrap_err(),
            TableKind::Ordered,
        );
        wrong(
            ordered
                .multi_get(std::slice::from_ref(&key), &BOTH_READS)
                .await
                .unwrap_err(),
            TableKind::Ordered,
        );
        assert_eq!(fixture.gets(), before, "no read");
        let (kv, all) = two_bucket_table().await;
        let plain = table_over(&kv, lenient(&kv)).await;
        plain.load().await.unwrap();
        let before = kv.gets();
        let key = Key::Long(all[0]);
        let reads = [ColumnRead::whole(VALUE_COLUMN)];
        wrong(
            plain.scan(&key, &reads, &oldest_first()).await.unwrap_err(),
            TableKind::KeyValue,
        );
        wrong(
            plain
                .multi_scan(std::slice::from_ref(&key), &reads, &oldest_first())
                .await
                .unwrap_err(),
            TableKind::KeyValue,
        );
        assert_eq!(kv.gets(), before, "no read");
    }

    /// The kind check runs before the readiness check: a call of the wrong
    /// kind to a table with no generation is `Error::WrongTableKind`, which
    /// no retry changes, never `Error::NotReady`, which a caller retries; and
    /// a call of the right kind to it is still `Error::NotReady`.
    #[tokio::test]
    async fn test_calls_of_the_wrong_kind_are_refused_before_the_table_is_ready() {
        let rows = ordered_rows(&[KEY_STEP], SCAN_ROWS_PER_KEY);
        let (ordered, _fixture, _gauged) =
            unloaded_ordered_table_over(&[&rows], WriterLayout::contract(), DEFAULT_BATCH_FAN_OUT)
                .await;
        let key = Key::Long(KEY_STEP);
        assert_eq!(ordered.kind(), TableKind::Ordered);
        assert!(ordered.current().is_none());
        let wrong = |error: Error, expected: TableKind| {
            assert!(
                matches!(&error, Error::WrongTableKind { table, kind } if table == TABLE && *kind == expected),
                "{error:?}"
            );
        };
        wrong(
            ordered.get(&key, &BOTH_READS).await.unwrap_err(),
            TableKind::Ordered,
        );
        wrong(
            ordered
                .multi_get(std::slice::from_ref(&key), &BOTH_READS)
                .await
                .unwrap_err(),
            TableKind::Ordered,
        );
        assert!(matches!(
            ordered.scan(&key, &BOTH_READS, &oldest_first()).await,
            Err(Error::NotReady { .. })
        ));
        assert!(matches!(
            ordered
                .multi_scan(std::slice::from_ref(&key), &BOTH_READS, &oldest_first())
                .await,
            Err(Error::NotReady { .. })
        ));
        let (kv, all) = two_bucket_table().await;
        let plain = table_over(&kv, lenient(&kv)).await;
        assert!(plain.current().is_none());
        assert_eq!(plain.kind(), TableKind::KeyValue);
        let key = Key::Long(all[0]);
        let reads = [ColumnRead::whole(VALUE_COLUMN)];
        wrong(
            plain.scan(&key, &reads, &oldest_first()).await.unwrap_err(),
            TableKind::KeyValue,
        );
        wrong(
            plain
                .multi_scan(std::slice::from_ref(&key), &reads, &oldest_first())
                .await
                .unwrap_err(),
            TableKind::KeyValue,
        );
        assert!(matches!(
            plain.get(&key, &reads).await,
            Err(Error::NotReady { .. })
        ));
        assert!(matches!(
            plain.multi_get(std::slice::from_ref(&key), &reads).await,
            Err(Error::NotReady { .. })
        ));
        let repeated = [reads[0], reads[0]];
        let is_repeated = |outcome: Result<()>| {
            assert!(
                matches!(
                    outcome,
                    Err(Error::ColumnRequest {
                        fault: ColumnRequestFault::Repeated,
                        ..
                    })
                ),
                "a column named twice, ahead of the kind and the readiness: {:?}",
                outcome.map(drop)
            );
        };
        is_repeated(plain.get(&key, &repeated).await.map(drop));
        is_repeated(
            plain
                .multi_get(std::slice::from_ref(&key), &repeated)
                .await
                .map(drop),
        );
        let key = Key::Long(KEY_STEP);
        is_repeated(ordered.get(&key, &repeated).await.map(drop));
        is_repeated(
            ordered
                .multi_get(std::slice::from_ref(&key), &repeated)
                .await
                .map(drop),
        );
    }

    /// A Scan or `MultiScan` request holds the faults of its own and of the
    /// table's entry before the table's readiness, so a table with no
    /// generation refuses them as a served one does, and is `NotReady` only
    /// to a request whose faults need the generation: a key of another
    /// type, a bound of another variant, a column it does not serve.
    #[tokio::test]
    async fn test_scan_requests_the_entry_refuses_are_refused_before_the_table_is_ready() {
        let rows = ordered_rows(&[KEY_STEP], SCAN_ROWS_PER_KEY);
        let (ordered, _fixture, _gauged) =
            unloaded_ordered_table_over(&[&rows], WriterLayout::contract(), DEFAULT_BATCH_FAN_OUT)
                .await;
        assert!(ordered.current().is_none());
        let key = Key::Long(KEY_STEP);
        let keys = std::slice::from_ref(&key);
        let max = DEFAULT_MAX_SCAN_ROWS;
        let at = |limit| ScanBounds {
            limit,
            ..oldest_first()
        };
        let empty = ScanBounds {
            start: Some(OrderValue::Long(ORDER_START)),
            before: Some(OrderValue::Long(ORDER_START)),
            ..oldest_first()
        };
        let repeated = [
            ColumnRead::whole(VALUE_COLUMN),
            ColumnRead::whole(VALUE_COLUMN),
        ];
        let refused = [
            (at(0), &BOTH_READS[..], ScanRequestFault::LimitZero),
            (
                at(max + 1),
                &BOTH_READS[..],
                ScanRequestFault::LimitAboveMax {
                    limit: max + 1,
                    max,
                },
            ),
            (empty, &BOTH_READS[..], ScanRequestFault::EmptyWindow),
        ];
        for (bounds, reads, expected) in refused {
            for error in [
                ordered.scan(&key, reads, &bounds).await.unwrap_err(),
                ordered.multi_scan(keys, reads, &bounds).await.unwrap_err(),
            ] {
                assert_eq!(scan_request_fault(error), expected);
            }
        }
        for error in [
            ordered
                .scan(&key, &repeated, &oldest_first())
                .await
                .unwrap_err(),
            ordered
                .multi_scan(keys, &repeated, &oldest_first())
                .await
                .unwrap_err(),
        ] {
            assert!(
                matches!(
                    error,
                    Error::ColumnRequest {
                        fault: ColumnRequestFault::Repeated,
                        ..
                    }
                ),
                "{error:?}"
            );
        }
        let max_values = ServerConfig::default().max_multi_scan_values;
        let too_many = vec![key.clone(); max_values / (2 * approx::length(max)) + 1];
        let reads = [ColumnRead::whole(VALUE_COLUMN)];
        let over = ordered
            .multi_scan(&too_many, &reads, &oldest_first())
            .await
            .unwrap_err();
        assert!(matches!(
            scan_request_fault(over),
            ScanRequestFault::TooManyValues { .. }
        ));
        let wrong_variant = ScanBounds {
            start: Some(OrderValue::Str("a".to_string())),
            ..oldest_first()
        };
        let needs_the_generation = [
            ordered
                .scan(&Key::Str("a".to_string()), &reads, &oldest_first())
                .await,
            ordered.scan(&key, &reads, &wrong_variant).await,
            ordered
                .scan(&key, &[ColumnRead::whole("absent")], &oldest_first())
                .await,
        ];
        for outcome in needs_the_generation {
            assert!(
                matches!(outcome, Err(Error::NotReady { .. })),
                "{outcome:?}"
            );
        }
    }

    /// The checks the gRPC service runs before it admits a Scan are the
    /// ones a Scan runs: a limit of 0 is the request's own fault, and one
    /// above `max_scan_rows` the entry's, on a table with no generation.
    #[tokio::test]
    async fn test_the_service_s_scan_checks_give_the_faults_a_scan_gives() {
        let rows = ordered_rows(&[KEY_STEP], SCAN_ROWS_PER_KEY);
        let (ordered, _fixture, _gauged) =
            unloaded_ordered_table_over(&[&rows], WriterLayout::contract(), DEFAULT_BATCH_FAN_OUT)
                .await;
        let max = DEFAULT_MAX_SCAN_ROWS;
        let at = |limit| ScanBounds {
            limit,
            ..oldest_first()
        };
        assert_eq!(
            scan_request_fault(check_scan_request(TABLE, &BOTH_READS, &at(0)).unwrap_err()),
            ScanRequestFault::LimitZero
        );
        assert!(
            ordered.check_scan(&at(0)).is_ok(),
            "no limit of the entry's"
        );
        assert_eq!(
            scan_request_fault(ordered.check_scan(&at(max + 1)).unwrap_err()),
            ScanRequestFault::LimitAboveMax {
                limit: max + 1,
                max
            }
        );
    }

    /// The fault of `error`, a refused Scan request of the test table.
    fn scan_request_fault(error: Error) -> ScanRequestFault {
        let Error::ScanRequest { table, fault } = error else {
            panic!("expected a scan request fault, got {error:?}");
        };
        assert_eq!(table, TABLE);
        fault
    }

    /// A limit of 0 and one above `max_scan_rows` are refused before any
    /// read, by a Scan and by a `MultiScan` alike, each of which sets its
    /// limit per key (RFD 025, Wire).
    #[tokio::test]
    async fn test_scan_and_multi_scan_refuse_a_limit_of_zero_or_above_max_before_any_read() {
        let rows = ordered_rows(&[KEY_STEP], SCAN_ROWS_PER_KEY);
        let (table, fixture, _gauged) =
            ordered_table_of(&rows, WriterLayout::contract(), DEFAULT_BATCH_FAN_OUT).await;
        let key = Key::Long(KEY_STEP);
        let max = DEFAULT_MAX_SCAN_ROWS;
        let before = fixture.gets();
        for (limit, expected) in [
            (0, ScanRequestFault::LimitZero),
            (
                max + 1,
                ScanRequestFault::LimitAboveMax {
                    limit: max + 1,
                    max,
                },
            ),
        ] {
            let bounds = ScanBounds {
                limit,
                ..oldest_first()
            };
            let scan = table.scan(&key, &BOTH_READS, &bounds).await.unwrap_err();
            assert_eq!(scan_request_fault(scan), expected, "Scan limit {limit}");
            let multi = table
                .multi_scan(std::slice::from_ref(&key), &BOTH_READS, &bounds)
                .await
                .unwrap_err();
            assert_eq!(
                scan_request_fault(multi),
                expected,
                "MultiScan limit {limit}"
            );
        }
        assert_eq!(fixture.gets(), before, "no request read anything");
    }

    /// A malformed Scan request is refused before any read, as
    /// `Error::ScanRequest` for its fault, and a key or a column the
    /// generation does not take as `get` refuses it (RFD 025, Wire,
    /// Limits).
    #[tokio::test]
    async fn test_scan_refuses_a_malformed_request_before_any_read() {
        let rows = ordered_rows(&[KEY_STEP], SCAN_ROWS_PER_KEY);
        let (table, fixture, _gauged) =
            ordered_table_of(&rows, WriterLayout::contract(), DEFAULT_BATCH_FAN_OUT).await;
        let key = Key::Long(KEY_STEP);
        let max = DEFAULT_MAX_SCAN_ROWS;
        let before = fixture.gets();
        let fault = scan_request_fault;
        let wrong_variant = ScanBounds {
            start: Some(OrderValue::Str("a".to_string())),
            ..oldest_first()
        };
        assert_eq!(
            fault(
                table
                    .scan(&key, &BOTH_READS, &wrong_variant)
                    .await
                    .unwrap_err()
            ),
            ScanRequestFault::BoundVariant {
                bound: BoundSide::Start,
                expected: OrderVariant::Long,
                got: OrderVariant::Str,
            }
        );
        let empty = ScanBounds {
            start: Some(OrderValue::Long(ORDER_START)),
            before: Some(OrderValue::Long(ORDER_START)),
            ..oldest_first()
        };
        assert_eq!(
            fault(table.scan(&key, &BOTH_READS, &empty).await.unwrap_err()),
            ScanRequestFault::EmptyWindow
        );
        let max_values = ServerConfig::default().max_multi_scan_values;
        let keys_at_the_full_limit = max_values / (2 * approx::length(max)) + 1;
        let keys = vec![key.clone(); keys_at_the_full_limit];
        let reads = [ColumnRead::whole(VALUE_COLUMN)];
        assert_eq!(
            fault(
                table
                    .multi_scan(&keys, &reads, &oldest_first())
                    .await
                    .unwrap_err()
            ),
            ScanRequestFault::TooManyValues {
                keys: keys_at_the_full_limit,
                limit: max,
                columns: reads.len(),
                values: keys_at_the_full_limit * approx::length(max) * (reads.len() + 1),
                max: max_values
            }
        );
        assert!(matches!(
            table
                .scan(&Key::Str("a".to_string()), &reads, &oldest_first())
                .await,
            Err(Error::KeyType { .. })
        ));
        assert!(matches!(
            table
                .scan(&key, &[ColumnRead::whole("absent")], &oldest_first())
                .await,
            Err(Error::ColumnRequest {
                fault: ColumnRequestFault::NotServed,
                ..
            })
        ));
        assert_eq!(fixture.gets(), before, "no request read anything");
        assert!(
            table
                .multi_scan(&keys[1..], &reads, &oldest_first())
                .await
                .is_ok(),
            "one key fewer is within max_multi_scan_values"
        );
    }

    /// `icetray_table_gets_per_scan` predicts what a cold Scan of every
    /// value column costs for a key whose run lies in one file and one row
    /// group (RFD 025, Metrics): under `full`, one span GET per column and
    /// one for the order column, the GETs the Scan then makes. A key-value
    /// table predicts no Scan.
    #[tokio::test]
    async fn test_gets_per_scan_is_what_a_cold_scan_of_one_file_costs() {
        let rows = ordered_rows(&[KEY_STEP], SCAN_ROWS_PER_KEY);
        let (table, fixture, _gauged) =
            ordered_table_of(&rows, WriterLayout::contract(), DEFAULT_BATCH_FAN_OUT).await;
        let predicted = table
            .current()
            .unwrap()
            .gets_per_scan
            .expect("an ordered table predicts a Scan");
        assert_eq!(
            predicted,
            approx::count(BOTH_READS.len() + 1),
            "one span per column and one for the order column"
        );
        let before = fixture.gets();
        let answered = table
            .scan(&Key::Long(KEY_STEP), &BOTH_READS, &oldest_first())
            .await
            .unwrap();
        assert_eq!(answered.scanned.rows, scanned_both(&rows));
        assert_eq!(fixture.gets() - before, predicted);

        let generation = table.current().unwrap();
        let config = table.config();
        let summary = check::diagnose(
            &generation,
            config.lookups(),
            &ValueFormat::opaque,
            config.scan_limits(
                ServerConfig::default().max_scan_bytes,
                ServerConfig::default().max_multi_scan_values,
            ),
        )
        .summary;
        assert_eq!(
            summary.lookup_cost, None,
            "an ordered table's doctor line prices no lookup"
        );
        let order = summary
            .order
            .expect("an ordered table's line has its order");
        assert_eq!(
            order.gets_per_scan, predicted,
            "the doctor's Scan prediction"
        );
        assert_eq!(order.files_probed, generation.lookup_cost.files_probed);

        let (key_value, _) = two_bucket_table().await;
        let table = table_over(&key_value, key_value.config(Residency::Full, None)).await;
        table.load().await.unwrap();
        assert_eq!(table.current().unwrap().gets_per_scan, None);
    }

    /// A Scan counts in `scans_total` by result, `scan_rows` and
    /// `scan_files`, and `scan_seconds` by temperature: cold while a page
    /// came from object storage, warm once the page cache holds them. Its
    /// GETs count in `value_page_gets_total` under each column's name and
    /// the order column's, and its spans are fetched once through the one
    /// page cache (RFD 025, Metrics; RFD 027).
    #[test]
    fn test_scan_counts_result_rows_files_temperature_and_gets_per_column() {
        let rows = ordered_rows(&[KEY_STEP], SCAN_ROWS_PER_KEY);
        let (fetches, series) = recorded(async {
            let (table, _fixture, _gauged) =
                ordered_table_of(&rows, WriterLayout::contract(), DEFAULT_BATCH_FAN_OUT).await;
            let key = Key::Long(KEY_STEP);
            for _ in 0..2 {
                let answered = table
                    .scan(&key, &BOTH_READS, &oldest_first())
                    .await
                    .unwrap();
                assert_eq!(answered.scanned.rows, scanned_both(&rows));
            }
            let absent = table
                .scan(&Key::Long(KEY_STEP + 1), &BOTH_READS, &oldest_first())
                .await
                .unwrap();
            assert!(absent.scanned.rows.is_empty() && absent.scanned.next.is_none());
            table.scans.as_ref().unwrap().cache.fetches()
        });
        assert_eq!(
            fetches,
            approx::count(BOTH_READS.len() + 1),
            "one span fetch per column and one for the order column, once"
        );
        let result = |result: ScanResult| {
            series.counter(
                metric::SCANS_TOTAL,
                &[table_label(), (metric::LABEL_RESULT, result.to_string())],
            )
        };
        assert_eq!(result(ScanResult::Rows), 2);
        assert_eq!(result(ScanResult::Empty), 1);
        assert_eq!(result(ScanResult::Error), 0);
        let seconds = |temperature: Temperature| {
            series
                .histogram(
                    metric::SCAN_SECONDS,
                    &[
                        table_label(),
                        (metric::LABEL_TEMPERATURE, temperature.to_string()),
                    ],
                )
                .len()
        };
        assert_eq!(seconds(Temperature::Cold), 1, "the first Scan");
        assert_eq!(seconds(Temperature::Warm), 2, "the repeat and the miss");
        let row_counts = series.histogram(metric::SCAN_ROWS, &[table_label()]);
        let held = approx::gauge_len(rows.len());
        assert_eq!(row_counts, [held, held, 0.0]);
        assert_eq!(
            series.histogram(metric::SCAN_FILES, &[table_label()]),
            [1.0, 1.0, 0.0]
        );
        for column in [VALUE_COLUMN, DETAIL_COLUMN, testing::ORDER_COLUMN] {
            assert_eq!(
                series.counter(
                    metric::VALUE_PAGE_GETS_TOTAL,
                    &[table_label(), (metric::LABEL_COLUMN, column.to_string())]
                ),
                1,
                "{column}: one span GET, cold, and none warm"
            );
        }
    }

    /// The result counters of `series`: rows, empty, and error, in that
    /// order.
    fn scan_results(series: &Series) -> [u64; 3] {
        [ScanResult::Rows, ScanResult::Empty, ScanResult::Error].map(|result| {
            series.counter(
                metric::SCANS_TOTAL,
                &[table_label(), (metric::LABEL_RESULT, result.to_string())],
            )
        })
    }

    /// A table of two files, each holding one key, built under a fan-out of
    /// one so a `MultiScan` reads its keys one after the other, and the
    /// two keys in file order.
    async fn two_keys_in_two_files() -> (Table, Fixture, Arc<Gauged>, [Key; 2]) {
        let first = ordered_rows(&[KEY_STEP], SCAN_ROWS_PER_KEY);
        let second = ordered_rows(&[2 * KEY_STEP], SCAN_ROWS_PER_KEY);
        let (table, fixture, gauged) =
            ordered_table_over(&[&first, &second], WriterLayout::contract(), 1).await;
        (
            table,
            fixture,
            gauged,
            [Key::Long(KEY_STEP), Key::Long(2 * KEY_STEP)],
        )
    }

    /// A `MultiScan` of two keys whose second key's file is gone fails
    /// whole, and the key it finished before the failure still counts as
    /// answered beside the one that failed, as a failed `MultiGet` counts
    /// the keys it completed (operating.md, Metrics).
    #[test]
    fn test_multi_scan_counts_the_keys_it_finished_before_a_key_failed() {
        let ((), series) = recorded(async {
            let (table, fixture, _gauged, keys) = two_keys_in_two_files().await;
            let second_key_file = 1;
            std::fs::remove_file(on_disk(&fixture, second_key_file)).unwrap();
            let bounds = ScanBounds {
                limit: u32::try_from(SCAN_ROWS_PER_KEY).unwrap(),
                ..oldest_first()
            };
            let failed = table.multi_scan(&keys, &BOTH_READS, &bounds).await;
            assert!(failed.is_err(), "the second key's file is gone");
        });
        assert_eq!(
            scan_results(&series),
            [1, 0, 1],
            "the finished key answered, the failed key an error"
        );
    }

    /// A `MultiScan` dropped while its second key waits on the store, as at
    /// its deadline or a client's cancel, still counts the key it finished.
    #[test]
    fn test_multi_scan_counts_the_keys_it_finished_before_it_was_dropped() {
        let ((), series) = recorded(async {
            let (table, _fixture, gauged, keys) = two_keys_in_two_files().await;
            let bounds = ScanBounds {
                limit: u32::try_from(SCAN_ROWS_PER_KEY).unwrap(),
                ..oldest_first()
            };
            // The first key's pages are cached, so only the second key
            // reads the store.
            table.scan(&keys[0], &BOTH_READS, &bounds).await.unwrap();
            gauged.hold();
            let mut call = Box::pin(table.multi_scan(&keys, &BOTH_READS, &bounds));
            let held = async {
                tokio::time::timeout(DEADLINE_GUARD, async {
                    while gauged.high_water() == 0 {
                        tokio::task::yield_now().await;
                    }
                })
                .await
                .expect("the second key's first GET is held");
            };
            tokio::select! {
                _ = &mut call => panic!("the second key's GETs are held"),
                () = held => {}
            }
            drop(call);
            gauged.release();
        });
        assert_eq!(
            scan_results(&series),
            [2, 0, 0],
            "the warming Scan and the dropped call's finished key"
        );
    }

    /// Distinct keys of the spread `MultiScan` tests: one run of
    /// `MULTI_SCAN_TASK_KEYS_MIN` keys for each worker of the split runtime.
    const SPREAD_KEYS: usize = MULTI_SCAN_TASK_KEYS_MIN * SPLIT_WORKERS;

    /// The keys of the spread tests' first file, `SPREAD_KEYS` of them.
    fn spread_keys() -> Vec<i64> {
        keys_from(0, i64::try_from(SPREAD_KEYS).unwrap())
    }

    /// A key between two of `spread_keys`, which no file holds.
    const ABSENT_KEY: i64 = 1;

    /// The spread keys a `MultiScan` lists twice in the answer test.
    const SPREAD_REPEATS: usize = 8;

    /// The answer test's limit: below `SCAN_ROWS_PER_KEY`, so every key
    /// held answers with a `next` bound, and small enough that the listed keys
    /// stay within `max_multi_scan_values`.
    const SPREAD_LIMIT: usize = 4;

    /// A `MultiScan` of `asked` under `bounds`, over a fresh cold table of
    /// `spread_keys` while `others` calls are counted in flight on it: its
    /// answers, and the runs it spawned, counted while the store holds
    /// every GET, so no run has ended.
    async fn multi_scan_counting_runs(
        asked: &[Key],
        bounds: &ScanBounds,
        others: usize,
    ) -> (Vec<Scanned>, usize) {
        let rows = ordered_rows(&spread_keys(), SCAN_ROWS_PER_KEY);
        let (table, _fixture, gauged) =
            ordered_table_of(&rows, WriterLayout::contract(), DEFAULT_BATCH_FAN_OUT).await;
        let _others: Vec<InFlight<'_>> = (0..others).map(|_| table.lookups.enter()).collect();
        gauged.hold();
        let mut call = Box::pin(table.multi_scan(asked, &BOTH_READS, bounds));
        let runs = tokio::select! {
            biased;
            answered = &mut call => panic!("the store holds every GET: {answered:?}"),
            runs = async {
                reach(&gauged, 1).await;
                runs_alive(&table)
            } => runs,
        };
        gauged.release();
        (call.await.unwrap().results, runs)
    }

    /// The runs a `MultiScan` of `distinct` of the spread keys, each listed
    /// `listed` times, spawns while `others` calls are counted in flight on
    /// its table.
    async fn multi_scan_runs_beside(distinct: usize, listed: usize, others: usize) -> usize {
        let keys = long_keys(&spread_keys()[..distinct]);
        let asked: Vec<Key> = (0..listed).flat_map(|_| keys.iter().cloned()).collect();
        let head = ScanBounds {
            limit: 1,
            ..oldest_first()
        };
        multi_scan_counting_runs(&asked, &head, others).await.1
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_multi_scan_splits_while_fewer_calls_than_workers_are_in_flight() {
        assert_eq!(shard::workers(), SPLIT_WORKERS);
        let (once, idle) = (1, 0);
        assert_eq!(
            multi_scan_runs_beside(SPREAD_KEYS, once, idle).await,
            SPLIT_WORKERS
        );
        let others_leaving_room = SPLIT_WORKERS - 2;
        assert_eq!(
            multi_scan_runs_beside(SPREAD_KEYS, once, others_leaving_room).await,
            SPLIT_WORKERS,
            "with the call, one fewer call in flight than workers"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_multi_scan_runs_inline_once_as_many_calls_as_workers_are_in_flight() {
        assert_eq!(shard::workers(), SPLIT_WORKERS);
        let once = 1;
        let others_filling_the_workers = SPLIT_WORKERS - 1;
        assert_eq!(
            multi_scan_runs_beside(SPREAD_KEYS, once, others_filling_the_workers).await,
            0
        );
        let others_past_the_workers = SPLIT_WORKERS;
        assert_eq!(
            multi_scan_runs_beside(SPREAD_KEYS, once, others_past_the_workers).await,
            0
        );
    }

    /// A `MultiScan` makes one run for each `MULTI_SCAN_TASK_KEYS_MIN`
    /// distinct keys or part of them: a key listed twice is read once, so
    /// it counts once, and one key past the minimum makes a second run.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_multi_scan_makes_a_run_for_each_task_minimum_of_distinct_keys() {
        assert_eq!(shard::workers(), SPLIT_WORKERS);
        let (once, twice, idle) = (1, 2, 0);
        let below = MULTI_SCAN_TASK_KEYS_MIN - 1;
        assert_eq!(
            multi_scan_runs_beside(below, twice, idle).await,
            0,
            "{below} distinct keys at {} positions",
            below * twice
        );
        assert_eq!(
            multi_scan_runs_beside(MULTI_SCAN_TASK_KEYS_MIN, once, idle).await,
            0
        );
        let two_runs = 2;
        assert_eq!(
            multi_scan_runs_beside(MULTI_SCAN_TASK_KEYS_MIN + 1, once, idle).await,
            two_runs
        );
    }

    /// A `MultiScan` of more than `MULTI_SCAN_TASK_KEYS_MIN` keys answers
    /// spread over idle workers exactly as on its own task, in either
    /// direction: every key's rows and `next` in key order, a key listed
    /// twice at each position, and a key no file holds empty.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_multi_scan_answers_spread_over_workers_as_on_its_own_task() {
        assert_eq!(shard::workers(), SPLIT_WORKERS);
        let held = spread_keys();
        let mut asked = long_keys(&held);
        asked.insert(SPREAD_KEYS / 2, Key::Long(ABSENT_KEY));
        asked.extend(long_keys(&held[..SPREAD_REPEATS]));
        for direction in [Direction::OldestFirst, Direction::NewestFirst] {
            let bounds = ScanBounds {
                direction,
                limit: u32::try_from(SPREAD_LIMIT).unwrap(),
                start: None,
                before: None,
            };
            let expected: Vec<Scanned> = asked
                .iter()
                .map(|key| {
                    let Key::Long(key) = *key else {
                        unreachable!("long keys")
                    };
                    if key == ABSENT_KEY {
                        return Scanned {
                            rows: Vec::new(),
                            next: None,
                        };
                    }
                    let mut rows = ordered_rows(&[key], SCAN_ROWS_PER_KEY);
                    if direction == Direction::NewestFirst {
                        rows.reverse();
                    }
                    let (_, last) = rows[SPREAD_LIMIT - 1];
                    let next = match direction {
                        Direction::OldestFirst => last + 1,
                        Direction::NewestFirst => last,
                    };
                    rows.truncate(SPREAD_LIMIT);
                    Scanned {
                        rows: scanned_both(&rows),
                        next: Some(OrderValue::Long(next)),
                    }
                })
                .collect();
            let idle = 0;
            let (spread, runs) = multi_scan_counting_runs(&asked, &bounds, idle).await;
            assert_eq!(
                runs, SPLIT_WORKERS,
                "{direction:?}: spread over every worker"
            );
            let others_filling_the_workers = SPLIT_WORKERS - 1;
            let (inline, runs) =
                multi_scan_counting_runs(&asked, &bounds, others_filling_the_workers).await;
            assert_eq!(runs, 0, "{direction:?}: read on the call's task");
            assert_eq!(spread, inline, "{direction:?}");
            assert_eq!(spread, expected, "{direction:?}");
        }
    }

    /// A table of two files under a fan-out of one, the first holding
    /// `spread_keys` with their pages cached and the second one key, cold,
    /// past them; and a `MultiScan` of every key, the cold one last, so
    /// each run reads its keys one after the other and the cold key, the
    /// last of the last run, is the only one to reach the store.
    async fn spread_keys_then_a_cold_one() -> (Table, Fixture, Arc<Gauged>, Vec<Key>) {
        let held = spread_keys();
        let cold = held.last().unwrap() + KEY_STEP;
        let warm = ordered_rows(&held, SCAN_ROWS_PER_KEY);
        let last = ordered_rows(&[cold], SCAN_ROWS_PER_KEY);
        let (table, fixture, gauged) =
            ordered_table_over(&[&warm, &last], WriterLayout::contract(), 1).await;
        let mut keys = long_keys(&held);
        table
            .multi_scan(&keys, &BOTH_READS, &spread_bounds())
            .await
            .unwrap();
        keys.push(Key::Long(cold));
        (table, fixture, gauged, keys)
    }

    /// Every row of a spread key, oldest first.
    fn spread_bounds() -> ScanBounds {
        ScanBounds {
            limit: u32::try_from(SCAN_ROWS_PER_KEY).unwrap(),
            ..oldest_first()
        }
    }

    /// Polls `call`, a `MultiScan` of `spread_keys_then_a_cold_one`'s keys,
    /// until every key but the cold one has finished and the store holds
    /// the cold key's GET: the GET is issued after every other key of its
    /// run, and every other run has ended. The runs still alive then, the
    /// cold key's alone when the call spread and none when it read on its
    /// own task.
    async fn until_only_the_cold_key_waits<F: Future>(
        table: &Table,
        gauged: &Gauged,
        call: &mut std::pin::Pin<Box<F>>,
    ) -> usize {
        let cold_key_alone = async {
            reach(gauged, 1).await;
            tokio::time::timeout(DEADLINE_GUARD, async {
                while runs_alive(table) > 1 {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .expect("every run but the cold key's ends");
            runs_alive(table)
        };
        tokio::select! {
            biased;
            _ = call.as_mut() => panic!("the cold key's GET is held"),
            runs = cold_key_alone => runs,
        }
    }

    /// The scan results counted by a `MultiScan` of
    /// `spread_keys_then_a_cold_one`'s keys whose cold key fails, its file
    /// gone, while `others` calls are counted in flight, and the runs alive
    /// once the cold key alone was left.
    fn failed_cold_key_counts(others: usize) -> ([u64; 3], usize) {
        let (runs, series) = recorded_on_split_workers(|snapshotter| async move {
            let (table, fixture, gauged, keys) = spread_keys_then_a_cold_one().await;
            drop(snapshotter.snapshot());
            let _others: Vec<InFlight<'_>> = (0..others).map(|_| table.lookups.enter()).collect();
            gauged.hold();
            let bounds = spread_bounds();
            let mut call = Box::pin(table.multi_scan(&keys, &BOTH_READS, &bounds));
            let runs = until_only_the_cold_key_waits(&table, &gauged, &mut call).await;
            let cold_key_file = 1;
            std::fs::remove_file(on_disk(&fixture, cold_key_file)).unwrap();
            gauged.release();
            let failed = call.await;
            let Err(Error::Cache(CacheError::Fetch(failed))) = failed else {
                panic!("the cold key's file is gone: {failed:?}");
            };
            assert!(
                matches!(
                    *failed,
                    Error::ObjectStore(object_store::Error::NotFound { .. })
                ),
                "{failed:?}"
            );
            runs
        });
        (scan_results(&series), runs)
    }

    /// A `MultiScan` whose last key fails counts the same spread over idle
    /// workers as on its own task: every key it finished before the
    /// failure as answered, and the failed key as one error.
    #[test]
    fn test_multi_scan_counts_a_failing_key_spread_as_on_its_own_task() {
        let finished = approx::count(SPREAD_KEYS);
        let idle = 0;
        let (spread, runs) = failed_cold_key_counts(idle);
        assert_eq!(runs, 1, "the cold key read on a spawned run");
        assert_eq!(spread, [finished, 0, 1]);
        let others_filling_the_workers = SPLIT_WORKERS - 1;
        let (inline, runs) = failed_cold_key_counts(others_filling_the_workers);
        assert_eq!(runs, 0, "the cold key read on the call's task");
        assert_eq!(inline, spread);
    }

    /// The scan results counted by a `MultiScan` of
    /// `spread_keys_then_a_cold_one`'s keys dropped while its cold key's
    /// GET is held, as at its deadline or a client's cancel, while `others`
    /// calls are counted in flight, and the runs alive once the cold key
    /// alone was left.
    fn dropped_call_counts(others: usize) -> ([u64; 3], usize) {
        let (runs, series) = recorded_on_split_workers(|snapshotter| async move {
            let (table, _fixture, gauged, keys) = spread_keys_then_a_cold_one().await;
            drop(snapshotter.snapshot());
            let _others: Vec<InFlight<'_>> = (0..others).map(|_| table.lookups.enter()).collect();
            gauged.hold();
            let bounds = spread_bounds();
            let mut call = Box::pin(table.multi_scan(&keys, &BOTH_READS, &bounds));
            let runs = until_only_the_cold_key_waits(&table, &gauged, &mut call).await;
            drop(call);
            gauged.release();
            runs
        });
        (scan_results(&series), runs)
    }

    /// A `MultiScan` dropped while its last key waits on the store counts
    /// the same spread over idle workers as on its own task: every key it
    /// finished, and nothing for the key still reading.
    #[test]
    fn test_multi_scan_counts_a_dropped_call_spread_as_on_its_own_task() {
        let finished = approx::count(SPREAD_KEYS);
        let idle = 0;
        let (spread, runs) = dropped_call_counts(idle);
        assert_eq!(runs, 1, "the cold key read on a spawned run");
        assert_eq!(spread, [finished, 0, 0]);
        let others_filling_the_workers = SPLIT_WORKERS - 1;
        let (inline, runs) = dropped_call_counts(others_filling_the_workers);
        assert_eq!(runs, 0, "the cold key read on the call's task");
        assert_eq!(inline, spread);
    }

    /// The error a `MultiScan` answers whose first key fails after its last
    /// one, as `first_key_fails_after_the_last` drives it, while `others`
    /// calls are counted in flight, and the runs alive once the first
    /// key's read alone was left: enough keys for two runs on the split
    /// runtime, and few enough for one run's fan-out to read them all at
    /// once.
    async fn multi_scan_failing_first_and_last(others: usize) -> (Error, usize) {
        let runs = 2;
        let keys = keys_from(0, i64::try_from(MULTI_SCAN_TASK_KEYS_MIN * runs).unwrap());
        assert!(
            keys.len() <= DEFAULT_BATCH_FAN_OUT,
            "one run reads every key at once"
        );
        let files = first_between_last(&keys).map(|part| ordered_rows(part, SCAN_ROWS_PER_KEY));
        let files: Vec<&[(i64, i64)]> = files.iter().map(Vec::as_slice).collect();
        let (table, fixture, gauged) =
            ordered_table_over(&files, WriterLayout::contract(), DEFAULT_BATCH_FAN_OUT).await;
        let _others: Vec<InFlight<'_>> = (0..others).map(|_| table.lookups.enter()).collect();
        first_key_slow_last_key_fast(&fixture, &gauged);
        let asked = long_keys(&keys);
        let bounds = spread_bounds();
        let call = table.multi_scan(&asked, &BOTH_READS, &bounds);
        first_key_fails_after_the_last(&table, &gauged, call).await
    }

    /// A failing `MultiScan` answers the failure of its first failing key
    /// in key order, spread over idle workers as on its own task, though a
    /// later key in another run failed first: the first key's read is held
    /// until the last key has failed and every other run has ended.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_multi_scan_answers_its_first_failing_key_spread_or_inline() {
        assert_eq!(shard::workers(), SPLIT_WORKERS);
        let idle = 0;
        let (error, runs) = multi_scan_failing_first_and_last(idle).await;
        assert_eq!(runs, 1, "the first key read on a spawned run");
        assert!(file_gone(&error), "the first key's failure: {error:?}");
        let others_filling_the_workers = SPLIT_WORKERS - 1;
        let (error, runs) = multi_scan_failing_first_and_last(others_filling_the_workers).await;
        assert_eq!(runs, 0, "the first key read on the call's task");
        assert!(file_gone(&error), "the first key's failure: {error:?}");
    }

    /// A Scan that ends after its call's receipt settled, as a run of a
    /// spread `MultiScan` still going on another worker when the call
    /// ended can, counts nowhere: neither a finished key as answered nor a
    /// failed one as an error.
    #[test]
    fn test_scan_receipt_counts_no_scan_that_ends_after_it_settles() {
        let tally = ScanTally {
            rows: 1,
            files: 1,
            cold: false,
            elapsed: Duration::ZERO,
        };
        let ((), series) = recorded(async {
            let metrics = ScanMetrics::new(TABLE, testing::ORDER_COLUMN);
            let receipt = ScanReceipt::new(TABLE, &metrics);
            let counts = Arc::clone(&receipt.counts);
            counts.finished(tally);
            counts.failed();
            receipt.answered();
            counts.finished(tally);
            counts.failed();
            drop(counts);
        });
        assert_eq!(
            scan_results(&series),
            [1, 0, 1],
            "the Scans that ended before the receipt settled"
        );
    }

    /// A `MultiScan` of keys the caller settles as `sent` counts each
    /// distinct key as answered and records the size; one it settles as
    /// `refused` returns `Error::ResponseTooLarge` carrying the `Oversize`,
    /// counts each distinct key as an error and none as answered, and
    /// records the refused size; a plain `scan` counts as answered and
    /// records no size. A key listed twice counts once.
    #[test]
    fn test_scan_receipt_counts_sent_and_refused_calls_and_records_their_size() {
        const SENT_BYTES: usize = 1_234;
        const REFUSED_BYTES: usize = 5_678;
        const BOUND: usize = 1_000;
        const DISTINCT_KEYS: u64 = 2;
        let refusal = Oversize {
            bytes: REFUSED_BYTES,
            bound: BOUND,
            fits: Fits::Keys(1),
        };
        let rows = ordered_rows(&[KEY_STEP, 2 * KEY_STEP], SCAN_ROWS_PER_KEY);
        let ((), series) = recorded(async {
            let (table, _fixture, _gauged) =
                ordered_table_of(&rows, WriterLayout::contract(), DEFAULT_BATCH_FAN_OUT).await;
            let first = Key::Long(KEY_STEP);
            let second = Key::Long(2 * KEY_STEP);
            let keys = [first.clone(), second, first.clone()];
            let bounds = ScanBounds {
                limit: u32::try_from(SCAN_ROWS_PER_KEY).unwrap(),
                ..oldest_first()
            };

            let (sent, receipt) = table
                .multi_scan_receipt(&keys, &BOTH_READS, &bounds)
                .await
                .unwrap();
            assert_eq!(sent.results.len(), keys.len());
            receipt.sent(SENT_BYTES);

            let (_, receipt) = table
                .multi_scan_receipt(&keys, &BOTH_READS, &bounds)
                .await
                .unwrap();
            let refused = receipt.refused(refusal);
            assert!(
                matches!(
                    refused,
                    Error::ResponseTooLarge { oversize, .. } if oversize == refusal
                ),
                "{refused:?}"
            );

            let (_, receipt) = table
                .scan_receipt(&first, &BOTH_READS, &bounds)
                .await
                .unwrap();
            assert!(matches!(
                receipt.refused(refusal),
                Error::ResponseTooLarge { .. }
            ));
            table.scan(&first, &BOTH_READS, &bounds).await.unwrap();
        });
        assert_eq!(
            scan_results(&series),
            [DISTINCT_KEYS + 1, 0, DISTINCT_KEYS + 1],
            "answered: the sent MultiScan's keys and the plain Scan; errors: the refused MultiScan's keys and the refused Scan"
        );
        assert_eq!(
            series.histogram(metric::SCAN_RESPONSE_BYTES, &[table_label()]),
            [
                approx::gauge_len(SENT_BYTES),
                approx::gauge_len(REFUSED_BYTES),
                approx::gauge_len(REFUSED_BYTES),
            ],
            "a sent call's size and each refused call's, none for the plain Scan"
        );
        assert_eq!(
            series.histogram(metric::SCAN_ROWS, &[table_label()]).len(),
            usize::try_from(DISTINCT_KEYS + 1).unwrap(),
            "a refused call counts no rows"
        );
    }

    /// A key-value table registers no scan series: every ordered-only rule
    /// is gated on the order column.
    #[test]
    fn test_a_key_value_table_registers_no_scan_series() {
        let ((), series) = recorded(async {
            let (fixture, _) = two_bucket_table().await;
            let table = table_over(&fixture, lenient(&fixture)).await;
            table.load().await.unwrap();
        });
        for name in [
            metric::SCANS_TOTAL,
            metric::SCAN_ROWS,
            metric::SCAN_FILES,
            metric::SCAN_SECONDS,
        ] {
            assert_eq!(series.named(name), 0, "{name}");
        }
    }

    #[test]
    fn test_scan_futures_are_send() {
        fn assert_send<T: Send>(_: &T) {}
        let table: Option<&Table> = None;
        if let Some(table) = table {
            let key = Key::Long(0);
            let bounds = oldest_first();
            assert_send(&table.scan(&key, &BOTH_READS, &bounds));
            assert_send(&table.scan_receipt(&key, &BOTH_READS, &bounds));
            assert_send(&table.multi_scan(std::slice::from_ref(&key), &BOTH_READS, &bounds));
            assert_send(&table.multi_scan_receipt(
                std::slice::from_ref(&key),
                &BOTH_READS,
                &bounds,
            ));
        }
    }

    #[test]
    fn test_fmt_table_kind_names_each_kind() {
        assert_eq!(TableKind::KeyValue.to_string(), "key-value");
        assert_eq!(TableKind::Ordered.to_string(), "ordered");
    }

    #[test]
    fn test_table_and_state_are_send_and_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<Table>();
        assert_send_sync::<TableState>();
    }
}
