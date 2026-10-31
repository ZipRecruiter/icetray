//! Opening every configured table (RFD 012): one storage, one row cache
//! and one page cache within one budget (RFD 028), the optional local
//! directory, one count of calls in flight, and every table in `Loading`
//! over them, built the same way for the daemon and the one-shot
//! subcommands, so a subcommand reads a table exactly as `serve` does.
//! Loading is the caller's step. A running daemon replaces its set of
//! tables through `Opened::apply` (RFD 018).

use std::collections::{HashMap, HashSet};
use std::ops::Deref;
use std::path::PathBuf;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use arc_swap::{ArcSwap, Guard};
use futures::future::join_all;

use crate::approx;
use crate::budget::{self, Budget, Shortfall};
use crate::cache::{self, PageCache};
use crate::catalog::TableSource;
use crate::config::{Config, ConfigError, ConfigViolation, ServerConfig, TableConfig};
use crate::error::{Error, Result};
use crate::local::{ChunkRules, DataFileIdentity, LocalDir};
use crate::memory::{self, MemoryLimit};
use crate::row_cache::RowCache;
use crate::storage::Storage;
use crate::table::{self, CallsInFlight, FindingLog, Table, TableHandles};

/// Everything a process opened: the storage every read goes through, the
/// one row cache and one page cache every table shares, the local
/// directory when configured, and the tables in configuration order.
pub struct Opened {
    /// The storage every table reads through.
    pub storage: Arc<Storage>,
    /// The page cache every table shares.
    pub cache: PageCache,
    /// The row cache every key-value table shares (RFD 026).
    pub row_cache: RowCache,
    /// What the row cache and the page cache's memory level hold now (RFD
    /// 028): what `open_with` resolved, until `fit_caches` changes the
    /// split. Never held across an `await`.
    budget: Mutex<Budget>,
    /// What the last check of the budget against the memory limit found
    /// missing, which a scrape publishes.
    pub shortfall: Shortfall,
    /// The server settings the budget was resolved under, which a reload
    /// cannot change (RFD 018).
    server: ServerConfig,
    /// The memory limit read when the caches opened: `fit_caches` resolves
    /// under it, so the whole budget stays the one the server started with.
    limit: Option<MemoryLimit>,
    /// The local directory (RFD 017), when `storage.local` is set.
    pub local: Option<Arc<LocalDir>>,
    /// The tables served, each in `Loading` until the caller loads it.
    /// Read without a lock; `apply` replaces the set whole.
    tables: ArcSwap<Tables>,
    /// The tables an `apply` has built and not yet swapped in or dropped.
    /// Their loads write chunk files no serving generation references, so
    /// `retain` counts them live. Never held across an `await`.
    pending: Mutex<Vec<Arc<Table>>>,
    /// Held by `retain` from computing its live set to its deletes, and by
    /// `apply` to add to `pending` and from taking the retuned tables' load
    /// locks to the swap, so no table joins the set between the two;
    /// retains run one at a time. Either takes load locks only while
    /// holding it. A refused or dropped `apply` takes its tables out of
    /// `pending` without it: a table leaving only makes a retain in
    /// progress keep more than it needs.
    membership: tokio::sync::Mutex<()>,
    /// Held by `apply` throughout: one reload at a time, each diffed
    /// against the set the last one left.
    applying: tokio::sync::Mutex<()>,
    /// What every table is built over, for the tables `apply` adds.
    handles: TableHandles,
    /// The server's deadlines every table is built under.
    deadlines: Deadlines,
    /// Whether `tables` is every configured table: `only` narrowed nothing,
    /// or named every one. `retain`'s live set is complete only then.
    complete: bool,
}

/// The tables an `Opened` serves: a slice in configuration order, through
/// `Deref`, and by name through `get`. Immutable; `Opened::apply` swaps in
/// another.
pub struct Tables {
    in_order: Vec<Arc<Table>>,
    /// Each table's position in `in_order`, by its configured name.
    by_name: HashMap<String, usize>,
}

impl Tables {
    /// `in_order`, indexed by name. The names are distinct: configuration
    /// validation refuses a repeated one.
    fn new(in_order: Vec<Arc<Table>>) -> Self {
        let by_name = in_order
            .iter()
            .enumerate()
            .map(|(at, table)| (table.name().to_string(), at))
            .collect();
        Self { in_order, by_name }
    }

    /// The table served under `name`, if any.
    pub fn get(&self, name: &str) -> Option<&Arc<Table>> {
        self.by_name.get(name).map(|&at| &self.in_order[at])
    }
}

impl Deref for Tables {
    type Target = [Arc<Table>];

    fn deref(&self) -> &Self::Target {
        &self.in_order
    }
}

/// The server's deadlines a table is built under (`Table::new`).
#[derive(Clone, Copy)]
struct Deadlines {
    load: Duration,
    refresh: Duration,
    get: Duration,
    scan: Duration,
}

/// What `Opened::apply` changed: the tables it loaded and swapped in, the
/// tables it swapped out, the tables it retuned in place, and how many it
/// kept as they were. A table whose configuration changed in a field other
/// than `lookups` and `strict` is in both `started` and `stopped`, under one
/// name.
pub struct Applied {
    /// The tables swapped in, each serving its first generation.
    pub started: Vec<Arc<Table>>,
    /// The tables swapped out, whose gauges are withdrawn.
    pub stopped: Vec<Arc<Table>>,
    /// The tables whose configuration changed in `lookups`, `strict`, or
    /// value column formats alone, which serve on in place under the new
    /// entry (`Table::retune`), their generation and refresh untouched.
    pub retuned: Vec<Arc<Table>>,
    /// The tables kept, whose configuration is unchanged.
    pub kept: usize,
}

/// Why `Opened::apply` changed nothing: every table it could not open or
/// load, or whose new `lookups`, `strict`, or column formats its serving
/// generation does not meet, by name, in configuration order.
#[derive(Debug)]
pub struct Refused {
    /// Each table that failed, with its failure.
    pub failures: Vec<(String, Error)>,
}

/// Tables `apply` added to `Opened::pending`, taken out again when this
/// drops, whether the tables were swapped in or dropped with the reload.
struct Registered<'a> {
    opened: &'a Opened,
    tables: Vec<Arc<Table>>,
}

impl Drop for Registered<'_> {
    fn drop(&mut self) {
        self.opened
            .pending
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .retain(|pending| !self.tables.iter().any(|table| Arc::ptr_eq(table, pending)));
    }
}

impl Opened {
    /// The tables served: the set as it stands, which a later `apply`
    /// does not change.
    pub fn tables(&self) -> Arc<Tables> {
        self.tables.load_full()
    }

    /// The tables served, borrowed through one `ArcSwap` load guard: what
    /// a call holds for its whole duration, and a swap meanwhile keeps the
    /// borrowed set alive until the guard drops, as `Table::get` holds its
    /// generation. The guard takes a fast slot as that one does, and falls
    /// back to a counted reference when its thread's slots are all held.
    pub fn serving_tables(&self) -> Guard<Arc<Tables>> {
        self.tables.load()
    }

    /// Samples the page cache's condition of every table served
    /// (`table::sample_page_cache`) on one blocking task, each chunk file
    /// once however many tables read it. The daemon runs it on a fixed
    /// interval.
    pub async fn sample_page_cache(&self) {
        let tables = self.tables();
        let tables: Vec<&Table> = tables.iter().map(Arc::as_ref).collect();
        table::sample_page_cache(&tables).await;
    }

    /// The bytes the kernel's page cache must hold for every served `disk`
    /// table to read its key pages at memory speed (RFD 020): every chunk
    /// file a served table's generation reads, whole, since the kernel
    /// caches its header too, each counted once however many tables share
    /// it, as two entries over one Iceberg table do; plus the page cache's
    /// disk level, whose files are read through the kernel's cache too.
    pub fn page_cache_needed_bytes(&self) -> u64 {
        let mut files: HashMap<PathBuf, u64> = HashMap::new();
        for generation in self.tables().iter().filter_map(|table| table.current()) {
            for (path, chunks) in generation.chunk_files() {
                files.insert(path.to_path_buf(), chunks.end);
            }
        }
        let disk_level = self
            .local
            .as_ref()
            .and_then(|local| local.page_cache_level())
            .map_or(0, |level| level.capacity_bytes());
        files.values().sum::<u64>().saturating_add(disk_level)
    }

    /// What the row cache and the page cache's memory level hold now (RFD
    /// 028).
    pub fn budget(&self) -> Budget {
        *self.budget.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// The resident index bytes of each table served, as
    /// `icetray_index_bytes` publishes them: one entry per table that
    /// serves a generation.
    pub fn index_bytes(&self) -> Vec<u64> {
        self.tables()
            .iter()
            .filter_map(|table| table.current())
            .map(|generation| approx::count(generation.index_bytes))
            .collect()
    }

    /// The part of the budget the caches have not yet filled: the whole
    /// less what the page cache's memory level and the row cache hold now.
    /// The resident set holds only what they have filled, so a check of
    /// memory room adds this to what it needs (RFD 028).
    pub fn unfilled_cache_bytes(&self) -> u64 {
        let filled = self.cache.usage().saturating_add(self.row_cache.usage());
        self.budget().whole().saturating_sub(filled)
    }

    /// The split the caches take serving `tables` (RFD 028), which
    /// `fit_caches` applies once they are the tables served: what a start
    /// with them resolves, under the settings and the memory limit the
    /// caches opened with. A page part under the minimum for the page
    /// cache's shards on this runtime is
    /// `ConfigViolation::PageCacheTooSmall`, which `Config::parse` checks
    /// against one shard only, so a reload checks its tables here before it
    /// applies them and is refused whole, as a start with its file would be.
    pub fn check_split(
        &self,
        tables: &[TableConfig],
    ) -> std::result::Result<Budget, ConfigViolation> {
        self.split(tables.iter().any(TableConfig::row_cache))
    }

    /// The budget under the settings and the memory limit the caches
    /// opened with, with the row cache's part when `rows_read`.
    fn split(&self, rows_read: bool) -> std::result::Result<Budget, ConfigViolation> {
        budget::resolve(&self.server, self.limit, rows_read, cache::memory_shards())
    }

    /// Resizes the row cache and the page cache's memory level to the split
    /// the tables served now call for (RFD 028): the budget resolved under
    /// the settings and the memory limit the caches opened with, so its
    /// whole is unchanged, with the row cache's part when a table served
    /// reads through it and none otherwise. Nothing changes when the row
    /// part is as it was. The cache that shrinks is resized first, so the
    /// two never hold more than the whole between the resizes, and a resize
    /// blocks until its evictions are done. A new page part under the
    /// minimum for the page cache's shards is `Error::Config` with
    /// `ConfigViolation::PageCacheTooSmall` and changes nothing, which the
    /// tables a reload checked with `check_split` never meet; a failure
    /// of the page cache's level is `CacheError::Backend`, after a row
    /// cache that shrinks has shrunk, and leaves the split recorded as it
    /// was, so the next call resizes again. The serve loop
    /// calls it after every reload that applied, and the split is logged
    /// as `cache budget` when it changes.
    pub fn fit_caches(&self) -> Result<()> {
        let rows_read = self.tables().iter().any(|table| table.config().row_cache());
        let next = self
            .split(rows_read)
            .map_err(|violation| Error::Config(ConfigError::Invalid(violation)))?;
        let mut budget = self.budget.lock().unwrap_or_else(PoisonError::into_inner);
        if next.row_bytes == budget.row_bytes {
            return Ok(());
        }
        if next.row_bytes < budget.row_bytes {
            self.row_cache.resize(next.row_bytes);
            self.cache.resize_memory(next.page_bytes)?;
        } else {
            self.cache.resize_memory(next.page_bytes)?;
            self.row_cache.resize(next.row_bytes);
        }
        *budget = next;
        log_budget(&next, self.limit);
        Ok(())
    }

    /// Replaces the tables served with `configs`, all or nothing (RFD
    /// 018). Tables are matched by name: one whose configuration is equal
    /// in every field is kept as it is, generation and refresh untouched;
    /// one whose configuration differs in `lookups`, `strict`, value
    /// column formats, or `max_scan_rows` alone is retuned in place at the
    /// swap, with no load (RFD 020, RFD 024, RFD 025); every other is
    /// built and loaded beside the set serving, the serving table of its
    /// name included, and neither is touched until every one has loaded.
    /// Each retuned table's serving generation is judged against its new
    /// entry (`Table::judge_retune`) twice. First as the tables are
    /// matched, before any table is loaded, so a retune that would be
    /// refused costs no load. Then at the swap, holding the membership lock
    /// and every retuned table's load lock, which is the judgement that
    /// decides, since a refresh may swap in another generation between the
    /// two. Only when all of them pass is anything changed: the retuned
    /// tables take their new entries (`Table::retune`), the set is swapped
    /// at once, the tables no longer configured are withdrawn
    /// (`Table::withdraw`) and dropped from the set, the new ones shown,
    /// and a call that looks a table up after the swap finds the new set; a
    /// call in flight finishes on the generation it holds. A retuned
    /// table's load lock is waited for behind a load or refresh in flight,
    /// which gives it up within its own deadline, as `retain` waits. When
    /// any table fails to open or load, or a retuned one's serving snapshot
    /// is below its new level under `strict`, the tables built are dropped,
    /// the set serving and every entry are untouched, and `Refused` names
    /// every failure of the stage that refused: a retune refused at the
    /// first judgement is named with every table that failed to open, and
    /// every load runs to its end or its deadline, so a refusal holds what
    /// the loads reserved until the slowest ends. The caller spawns and
    /// stops the refreshes, and runs `retain` after either outcome, to free
    /// what the dropped tables held or what the refused ones wrote.
    /// `configs` is taken as validated: the names are distinct. An `Opened`
    /// that `only` narrowed stays narrowed, and its `retain` still frees
    /// nothing.
    pub async fn apply(&self, configs: &[TableConfig]) -> std::result::Result<Applied, Refused> {
        let _applying = self.applying.lock().await;
        let serving = self.tables();
        let mut next = Vec::with_capacity(configs.len());
        let mut started = Vec::new();
        let mut retuned = Vec::new();
        let mut failures = Vec::new();
        for config in configs {
            if let Some(same) = serving.get(&config.name) {
                let entry = same.config();
                if *entry == *config {
                    next.push(Arc::clone(same));
                    continue;
                }
                if entry.same_but_tuning(config) {
                    if let Err(error) = same.judge_retune(config) {
                        failures.push((config.name.clone(), error));
                    }
                    retuned.push((Arc::clone(same), config.clone()));
                    next.push(Arc::clone(same));
                    continue;
                }
            }
            match self.build(config).await {
                Ok(table) => {
                    let table = Arc::new(table);
                    started.push(Arc::clone(&table));
                    next.push(table);
                }
                Err(error) => failures.push((config.name.clone(), error)),
            }
        }
        if !failures.is_empty() {
            return Err(Refused { failures });
        }
        let registered = self.register(&started).await;
        let loads = join_all(started.iter().map(|table| table.load())).await;
        let failures: Vec<(String, Error)> = started
            .iter()
            .zip(loads)
            .filter_map(|(table, loaded)| {
                loaded.err().map(|error| (table.name().to_string(), error))
            })
            .collect();
        if !failures.is_empty() {
            return Err(Refused { failures });
        }
        let stopped: Vec<Arc<Table>> = serving
            .iter()
            .filter(|table| !next.iter().any(|kept| Arc::ptr_eq(kept, table)))
            .cloned()
            .collect();
        let kept = next.len() - started.len() - retuned.len();
        {
            let _members = self.membership.lock().await;
            let mut serialized = Vec::with_capacity(retuned.len());
            for (table, _) in &retuned {
                serialized.push(table.serialized().await);
            }
            let failures: Vec<(String, Error)> = retuned
                .iter()
                .filter_map(|(table, config)| {
                    table
                        .judge_retune(config)
                        .err()
                        .map(|error| (table.name().to_string(), error))
                })
                .collect();
            if !failures.is_empty() {
                return Err(Refused { failures });
            }
            for ((table, config), held) in retuned.iter().zip(&serialized) {
                table.retune(config.clone(), held);
            }
            drop(serialized);
            for table in &stopped {
                table.withdraw();
            }
            self.tables.store(Arc::new(Tables::new(next)));
            drop(registered);
            for table in &started {
                table.show();
            }
        }
        Ok(Applied {
            started,
            stopped,
            retuned: retuned.into_iter().map(|(table, _)| table).collect(),
            kept,
        })
    }

    /// A table for `config` over this process's handles and deadlines, not
    /// showing its gauges (`Table::hidden`).
    async fn build(&self, config: &TableConfig) -> Result<Table> {
        build(config, &self.handles, self.deadlines).await
    }

    /// Adds `tables` to `pending` while no `retain` runs, until the
    /// returned registration drops.
    async fn register(&self, tables: &[Arc<Table>]) -> Registered<'_> {
        let _members = self.membership.lock().await;
        self.pending
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .extend(tables.iter().cloned());
        Registered {
            opened: self,
            tables: tables.to_vec(),
        }
    }

    /// Deletes the chunk files no table's serving generation references and
    /// returns the bytes freed; `Ok(0)` with no local directory. The live
    /// set is the union of `Generation::identities` (RFD 017) over every
    /// table served and every table an `apply` is loading, computed and
    /// applied while every one's load lock is held, in `tables` order and
    /// then the pending ones, so no load is in flight anywhere meanwhile,
    /// and while `membership` is held, so no table joins the set. An
    /// `Opened` that `only` narrowed frees nothing: the tables it left out
    /// share the directory, and their chunk files are live to them. This
    /// is the one place chunk files are deleted: the serve loop calls it
    /// after every table's first `load`, after any `refresh` that swapped,
    /// and after an `apply` that did, and a one-shot subcommand may call it
    /// before exit.
    ///
    /// Every lock is taken because a load in flight has written chunk files
    /// that no serving generation references yet, and a live set computed
    /// beside it would delete them from under the generation about to swap
    /// in. The locks are taken one at a time, and nothing that holds one
    /// waits for `membership`, so no lock is waited for in a cycle. The cost
    /// is that a slow table holds retention up, and with it the next load
    /// or refresh of every table whose lock is already held. A load or
    /// refresh gives its lock up once its deadline has passed since it took
    /// it: the load deadline for a build with no generation serving
    /// (`server.load_deadline`, thirty minutes by default), the refresh
    /// deadline otherwise (`server.refresh_deadline`, five minutes). The
    /// lock is fair, so a request queued behind it comes next: each lock is
    /// waited for at most one deadline, and one slow table holds the others
    /// up at most that long, plus the deletes; while a table's first load
    /// runs, that is its load deadline. A table
    /// whose lock is not yet taken can start a refresh while an earlier one
    /// is waited for, so when several tables are slow, a table early in
    /// `tables` can be held up one deadline for each slow table after it.
    pub async fn retain(&self) -> Result<u64> {
        let Some(local) = &self.local else {
            return Ok(0);
        };
        if !self.complete {
            return Ok(0);
        }
        let _members = self.membership.lock().await;
        let pending = self
            .pending
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone();
        let tables: Vec<Arc<Table>> = self.tables().iter().cloned().chain(pending).collect();
        let mut held = Vec::with_capacity(tables.len());
        for table in &tables {
            held.push(table.serialized().await);
        }
        let live: HashSet<(DataFileIdentity, ChunkRules)> = tables
            .iter()
            .filter_map(|table| table.current())
            .flat_map(|generation| generation.live_chunk_files().collect::<Vec<_>>())
            .collect();
        let freed = local.retain(&live)?;
        drop(held);
        Ok(freed)
    }
}

/// Builds storage over the SDK's default chain, which it loads before its
/// first S3 read and never for a process that reads none, one cache, one
/// count of calls in flight, and every configured table in `Loading` over
/// them, each logging the doctor's findings as `findings` says; `only`
/// narrows by name, and a name that
/// matches no table yields `Ok` with empty `tables`, whose message and exit
/// code are the caller's. Nothing is read from any table: `open` issues no
/// GET.
pub async fn open(config: &Config, only: Option<&str>, findings: FindingLog) -> Result<Opened> {
    let storage = Arc::new(Storage::with_sdk_defaults(config.storage.clone()));
    open_with(config, only, storage, findings).await
}

/// `open` over a storage the caller built, for the testkit and embedders:
/// a test constructs one with `Storage::new` and never touches the SDK
/// chain, and registers its own stores on it. The binary's subcommands
/// use `open`; the daemon's in-process tests start it over this. Before
/// anything is opened, the cache budget is resolved under the process's
/// memory limit (`budget::resolve`) and logged, and its page part is
/// checked against the minimum of the shards a cache has on the calling
/// runtime, which parsing could not know: less is `Error::Config` with
/// `ConfigViolation::PageCacheTooSmall`.
pub async fn open_with(
    config: &Config,
    only: Option<&str>,
    storage: Arc<Storage>,
    findings: FindingLog,
) -> Result<Opened> {
    let limit = memory::limit();
    let shards = cache::memory_shards();
    let budget = budget::resolve(&config.server, limit, config.reads_rows(), shards)
        .map_err(|violation| Error::Config(ConfigError::Invalid(violation)))?;
    log_budget(&budget, limit);
    let local = match &config.storage.local {
        Some(local) => Some(Arc::new(LocalDir::open(local, storage.stats())?)),
        None => None,
    };
    let cache = PageCache::new(
        budget.page_bytes,
        local.as_ref().and_then(|local| local.page_cache_level()),
    )
    .await?;
    let row_cache = RowCache::new(budget.row_bytes);
    let handles = TableHandles {
        storage: Arc::clone(&storage),
        cache: cache.clone(),
        row_cache: Some(row_cache.clone()),
        local: local.clone(),
        calls: CallsInFlight::new(),
        findings,
        batch_fan_out: config.server.batch_fan_out,
        max_multi_scan_values: config.server.max_multi_scan_values,
        max_scan_bytes: config.server.max_scan_bytes,
    };
    let deadlines = Deadlines {
        load: config.server.load_deadline,
        refresh: config.server.refresh_deadline,
        get: config.server.get_deadline,
        scan: config.server.scan_deadline,
    };
    let mut tables = Vec::with_capacity(config.tables.len());
    for table in config
        .tables
        .iter()
        .filter(|table| only.is_none_or(|name| table.name == name))
    {
        let table = build(table, &handles, deadlines).await?;
        table.show();
        tables.push(Arc::new(table));
    }
    let complete = tables.len() == config.tables.len();
    Ok(Opened {
        storage,
        cache,
        row_cache,
        budget: Mutex::new(budget),
        shortfall: Shortfall::default(),
        server: config.server.clone(),
        limit,
        local,
        tables: ArcSwap::from_pointee(Tables::new(tables)),
        pending: Mutex::new(Vec::new()),
        membership: tokio::sync::Mutex::new(()),
        applying: tokio::sync::Mutex::new(()),
        handles,
        deadlines,
        complete,
    })
}

/// Logs `budget`, resolved under the memory `limit`, as `cache budget`.
fn log_budget(budget: &Budget, limit: Option<MemoryLimit>) {
    tracing::info!(
        cache_bytes = budget.whole(),
        row_bytes = budget.row_bytes,
        page_bytes = budget.page_bytes,
        limit = limit.map(|limit| limit.bytes),
        limit_source = limit.map(|limit| tracing::field::display(limit.of)),
        "cache budget"
    );
}

/// A table for `config` over `handles`, under `deadlines`, in `Loading` and
/// not showing its gauges (`Table::hidden`).
async fn build(
    config: &TableConfig,
    handles: &TableHandles,
    deadlines: Deadlines,
) -> Result<Table> {
    let source = TableSource::from_config(&config.source).await?;
    Ok(Table::hidden(
        config.clone(),
        source,
        handles.clone(),
        deadlines.load,
        deadlines.refresh,
        deadlines.get,
        deadlines.scan,
    ))
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use futures::FutureExt as _;
    use object_store::local::LocalFileSystem;
    use object_store::throttle::{ThrottleConfig, ThrottledStore};

    use super::*;
    use crate::check::FindingCode;
    use crate::config::{
        ConfigError, ConfigViolation, DEFAULT_MAX_SCAN_ROWS, LocalConfig, Residency, ServerConfig,
        SourceConfig, StorageConfig, TableConfig,
    };
    use crate::contract::{PAGE_CACHE_DISK_BLOCK_BYTES, VALUE_PAGE_BYTES};
    use crate::error::{Budget, ErrorKind, RefusalReason};
    use crate::generation::testing::{
        E2E_KEYS, FIRST_SEQUENCE, FIRST_SNAPSHOT, FIRST_VERSION, KEY_STEP, POLL_GETS,
        SECOND_SEQUENCE, SECOND_SNAPSHOT, SECOND_VERSION, TABLE, TWO, Table as Fixture,
        WriterLayout, bucketing, keys_from, keys_in_bucket, refusal_of, two_bucket_table,
    };
    use crate::key::Key;
    use crate::level::Level;
    use crate::manifest::ColumnRead;
    use crate::manifest::testing::{Partition, VALUE_COLUMN, whole};
    use crate::order::{Direction, ScanBounds};
    use crate::snapshot::SnapshotId;
    use crate::storage::FILE_PREFIX;
    use crate::storage::testing::STALL;
    use crate::table::TableState;

    /// The names of the two configured tables.
    const FIRST_TABLE: &str = "a";
    const SECOND_TABLE: &str = "b";
    /// A name no table is configured under.
    const NO_TABLE: &str = "nobody";
    /// A third table's name, for one served in the Full tier beside two
    /// in the Disk tier.
    const FULL_TABLE: &str = "c";

    /// Two contract files in two buckets, committed as `FIRST_SNAPSHOT`.
    async fn two_file_fixture() -> Fixture {
        two_bucket_table().await.0
    }

    /// A table named `name` following `fixture` in `residency`.
    fn named(name: &str, fixture: &Fixture, residency: Residency) -> TableConfig {
        TableConfig {
            name: name.to_string(),
            ..fixture.config(residency, None)
        }
    }

    /// A configuration with no local directory over `tables`.
    fn config(tables: Vec<TableConfig>) -> Config {
        Config {
            server: ServerConfig::default(),
            storage: StorageConfig::default(),
            tables,
        }
    }

    /// A configuration with a local directory at `dir` and no page cache
    /// disk level, over `tables`.
    fn config_with_local(dir: &Path, tables: Vec<TableConfig>) -> Config {
        Config {
            storage: StorageConfig {
                local: Some(LocalConfig {
                    dir: dir.to_path_buf(),
                    index_budget_bytes: None,
                    page_cache_disk_bytes: 0,
                }),
                ..StorageConfig::default()
            },
            ..config(tables)
        }
    }

    fn storage_for(config: &Config) -> Arc<Storage> {
        Arc::new(Storage::new(config.storage.clone()))
    }

    fn names(opened: &Opened) -> Vec<String> {
        names_of(&opened.tables())
    }

    /// The table `opened` serves under `name`, if any.
    fn table_named(opened: &Opened, name: &str) -> Option<Arc<Table>> {
        opened.tables().get(name).cloned()
    }

    #[tokio::test]
    async fn test_open_with_builds_every_table_loading_with_no_get() {
        let first = two_file_fixture().await;
        let second = two_file_fixture().await;
        let config = config(vec![
            named(FIRST_TABLE, &first, Residency::Full),
            named(SECOND_TABLE, &second, Residency::Full),
        ]);
        let storage = storage_for(&config);
        let opened = open_with(&config, None, Arc::clone(&storage), FindingLog::Events)
            .await
            .unwrap();
        assert_eq!(names(&opened), [FIRST_TABLE, SECOND_TABLE]);
        for table in opened.tables().iter() {
            assert!(matches!(table.state(), TableState::Loading));
            assert!(table.current().is_none());
        }
        assert!(Arc::ptr_eq(&opened.storage, &storage));
        assert!(opened.local.is_none());
        assert_eq!(storage.stats().gets(), 0);
    }

    /// A budget `Config::parse` admits, one shard's minimum, is refused
    /// on a runtime of two workers, whose cache has two shards, before any
    /// table is opened.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_open_with_refuses_a_page_cache_below_the_runtime_shards_minimum() {
        let workers = 2;
        let fixture = two_file_fixture().await;
        let one_shard = cache::min_memory_bytes(1);
        let two_shards = cache::min_memory_bytes(workers);
        let mut config = config(vec![named(FIRST_TABLE, &fixture, Residency::Full)]);
        config.server.row_cache_share = Some(0.0);
        config.server.cache_bytes = Some(one_shard);
        let error = open_with(&config, None, storage_for(&config), FindingLog::Events)
            .await
            .err()
            .expect("a budget below two shards' minimum");
        assert!(
            matches!(
                error,
                Error::Config(ConfigError::Invalid(ConfigViolation::PageCacheTooSmall {
                    bytes,
                    minimum,
                    shards,
                    limit: None,
                })) if bytes == one_shard && minimum == two_shards && shards == workers
            ),
            "{error}"
        );
        config.server.cache_bytes = Some(two_shards);
        let opened = open_with(&config, None, storage_for(&config), FindingLog::Events)
            .await
            .unwrap();
        assert_eq!(names(&opened), [FIRST_TABLE]);
    }

    /// Rows of `KEY_STEP` in the ordered fixture's one file.
    const ORDERED_ROWS: usize = 6;

    /// An ordered table of one file holding `ORDERED_ROWS` rows of
    /// `KEY_STEP`, ascending by order value, committed as `FIRST_SNAPSHOT`.
    async fn ordered_fixture() -> Fixture {
        let mut fixture = Fixture::ordered(1);
        let rows: Vec<(i64, i64)> = (0..i64::try_from(ORDERED_ROWS).unwrap())
            .map(|order| (KEY_STEP, order))
            .collect();
        fixture
            .add_ordered_file("f1", &rows, Partition::Bucket(0), WriterLayout::contract())
            .await;
        fixture
            .commit(FIRST_SNAPSHOT, None, FIRST_SEQUENCE, FIRST_VERSION)
            .await;
        fixture
    }

    /// The rows a Scan of `KEY_STEP` naming the value column answers, every
    /// row oldest first.
    async fn scan_every_row(table: &Table) -> usize {
        let bounds = ScanBounds {
            direction: Direction::OldestFirst,
            limit: DEFAULT_MAX_SCAN_ROWS,
            start: None,
            before: None,
        };
        table
            .scan(
                &Key::Long(KEY_STEP),
                &[ColumnRead::whole(VALUE_COLUMN)],
                &bounds,
            )
            .await
            .unwrap()
            .scanned
            .rows
            .len()
    }

    /// The first table `opened` serves, loaded.
    async fn loaded_first(opened: &Opened) -> Arc<Table> {
        let table = table_named(opened, FIRST_TABLE).unwrap();
        table.load().await.unwrap();
        table
    }

    /// An ordered table's Scans read through the one page cache every
    /// table shares (RFD 027): the first Scan's spans are fetches of
    /// `Opened.cache`, and the second's are its hits, with no GET.
    #[tokio::test]
    async fn test_a_scan_reads_through_the_one_page_cache() {
        let fixture = ordered_fixture().await;
        let config = config(vec![lenient(FIRST_TABLE, &fixture)]);
        let storage = storage_for(&config);
        let opened = open_with(&config, None, Arc::clone(&storage), FindingLog::Events)
            .await
            .unwrap();
        let table = loaded_first(&opened).await;
        assert_eq!(scan_every_row(&table).await, ORDERED_ROWS);
        let fetched = opened.cache.fetches();
        assert!(fetched > 0, "the cold Scan's spans are fetched through it");
        let (gets, hits) = (storage.stats().gets(), opened.cache.hits());
        assert_eq!(scan_every_row(&table).await, ORDERED_ROWS);
        assert_eq!(opened.cache.fetches(), fetched, "nothing fetched again");
        assert_eq!(storage.stats().gets(), gets, "no GET");
        assert!(
            opened.cache.hits() > hits,
            "the warm Scan's pages are hits of the one page cache"
        );
    }

    /// With a disk level, a Scan whose pages memory let go of reads them
    /// from it (RFD 027, A span read): the repeated Scan issues no GET and
    /// reads its pages locally.
    #[tokio::test]
    async fn test_a_repeated_scan_is_served_from_the_disk_level() {
        let fixture = ordered_fixture().await;
        let dir = tempfile::tempdir().unwrap();
        let mut config = config_with_local(dir.path(), vec![lenient(FIRST_TABLE, &fixture)]);
        config.storage.local.as_mut().unwrap().page_cache_disk_bytes = PAGE_CACHE_DISK_BLOCK_BYTES;
        let storage = storage_for(&config);
        let opened = open_with(&config, None, Arc::clone(&storage), FindingLog::Events)
            .await
            .unwrap();
        let table = loaded_first(&opened).await;
        assert_eq!(scan_every_row(&table).await, ORDERED_ROWS);
        opened.cache.clear();
        opened.cache.disk_writes_done().await;
        let stats = storage.stats();
        let (gets, reads) = (stats.gets(), stats.local_reads());
        assert_eq!(scan_every_row(&table).await, ORDERED_ROWS);
        assert_eq!(stats.gets(), gets, "no GET");
        assert!(
            stats.local_reads() > reads,
            "the pages come back from the disk level"
        );
    }

    /// The budget follows the tables: a key-value table gives the row
    /// cache its share, and the row cache opened is the one every table's
    /// handles hold.
    #[tokio::test]
    async fn test_open_with_splits_the_budget_for_a_key_value_table() {
        let fixture = two_file_fixture().await;
        let mut config = config(vec![named(FIRST_TABLE, &fixture, Residency::Full)]);
        config.server.cache_bytes = Some(crate::config::FALLBACK_CACHE_BYTES);
        let opened = open_with(&config, None, storage_for(&config), FindingLog::Events)
            .await
            .unwrap();
        assert_eq!(
            opened.budget(),
            budget::resolve(&config.server, None, true, cache::memory_shards()).unwrap()
        );
        assert!(opened.budget().row_bytes > opened.budget().page_bytes);
        assert!(opened.handles.row_cache.is_some());
    }

    /// A whole budget whose page part, at the default row share, holds
    /// about one value page in each shard of the page cache's memory level,
    /// and so ten times that once no table reads through the row cache.
    fn tight_budget() -> u64 {
        let pages_per_shard_without_rows = 10;
        pages_per_shard_without_rows * cache::min_memory_bytes(cache::memory_shards())
    }

    /// `table` reading pages alone.
    fn pages_alone(table: TableConfig) -> TableConfig {
        TableConfig {
            row_cache: Some(false),
            ..table
        }
    }

    /// Fills the page cache of `opened` with twice its whole budget of
    /// value pages, under keys no table reads.
    async fn fill_pages(opened: &Opened) {
        let page = bytes::Bytes::from(vec![0_u8; approx::length(VALUE_PAGE_BYTES)]);
        let twice_the_budget = 2 * tight_budget() / VALUE_PAGE_BYTES;
        opened.cache.fill(0, twice_the_budget, &page).await;
    }

    /// Whether the row cache of `opened` keeps a value inserted into it.
    fn row_cache_keeps_a_value(opened: &Opened) -> bool {
        let file: Arc<str> = Arc::from("mem://w/db/t/data/rows.parquet");
        let key = crate::row_cache::RowKey::new(&file, crate::index::RowPosition(0), 0);
        opened.row_cache.insert(key.clone(), Some(b"a value"));
        opened.row_cache.get(&key) != crate::row_cache::Cached::Absent
    }

    /// A reload that drops the last table reading through the row cache
    /// gives the page cache's memory level the whole budget, which it then
    /// fills past the part it had, and the row cache nothing.
    #[tokio::test]
    async fn test_a_reload_dropping_the_last_row_cache_table_gives_pages_the_budget() {
        let fixture = two_file_fixture().await;
        let first = named(FIRST_TABLE, &fixture, Residency::Full);
        let mut config = config(vec![first.clone()]);
        config.server.cache_bytes = Some(tight_budget());
        let opened = loaded(&config).await;
        let before = opened.budget();
        assert!(before.row_bytes > 0, "the key-value table reads rows");
        assert!(row_cache_keeps_a_value(&opened));

        opened.apply(&[pages_alone(first)]).await.unwrap();
        opened.fit_caches().unwrap();
        assert_eq!(
            opened.budget(),
            budget::Budget {
                row_bytes: 0,
                page_bytes: tight_budget(),
                derived_from: None,
            }
        );
        fill_pages(&opened).await;
        let held = opened.cache.usage();
        assert!(held > before.page_bytes, "{held} within the part pages had");
        assert!(held <= tight_budget(), "{held} over the whole budget");
        assert_eq!(opened.row_cache.usage(), 0);
        assert!(!row_cache_keeps_a_value(&opened));
    }

    /// A reload that serves the first table reading through the row cache,
    /// on a server whose tables read pages alone until then, gives the row
    /// cache its share, which caches again, and the page cache the rest.
    #[tokio::test]
    async fn test_a_reload_adding_the_first_row_cache_table_gives_rows_their_share() {
        let fixture = two_file_fixture().await;
        let first = named(FIRST_TABLE, &fixture, Residency::Full);
        let mut config = config(vec![pages_alone(first.clone())]);
        config.server.cache_bytes = Some(tight_budget());
        let opened = loaded(&config).await;
        assert_eq!(opened.budget().row_bytes, 0);
        assert!(!row_cache_keeps_a_value(&opened));

        opened.apply(&[first]).await.unwrap();
        opened.fit_caches().unwrap();
        let shards = cache::memory_shards();
        assert_eq!(
            opened.budget(),
            budget::resolve(&config.server, None, true, shards).unwrap()
        );
        assert!(row_cache_keeps_a_value(&opened));
        fill_pages(&opened).await;
        let held = opened.cache.usage();
        assert!(
            held <= opened.budget().page_bytes,
            "{held} over the page part"
        );
    }

    /// A budget whose page part with a row-reading table, a tenth, holds
    /// one shard's minimum and not two shards', which parsing admits and
    /// a two-worker runtime refuses; whole, it holds two shards' minimum.
    fn one_shard_s_tenth() -> u64 {
        let tenths = 10;
        let shards_and_a_half = 3;
        tenths * shards_and_a_half * cache::min_memory_bytes(1) / 2
    }

    /// On a server whose tables read pages alone, tables that read rows
    /// give a split whose page part is under the minimum for the runtime's
    /// two shards, which a start with them refuses, so `check_split`
    /// refuses it; the tables served pass it.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_check_split_refuses_a_page_part_below_the_runtime_shards_minimum() {
        let workers = 2;
        let fixture = two_file_fixture().await;
        let first = named(FIRST_TABLE, &fixture, Residency::Full);
        let mut config = config(vec![pages_alone(first.clone())]);
        config.server.cache_bytes = Some(one_shard_s_tenth());
        let opened = open_with(&config, None, storage_for(&config), FindingLog::Events)
            .await
            .unwrap();
        assert_eq!(opened.check_split(&config.tables), Ok(opened.budget()));
        let refused = opened.check_split(&[first]).unwrap_err();
        assert!(
            matches!(
                refused,
                ConfigViolation::PageCacheTooSmall { bytes, minimum, shards, limit: None }
                    if bytes >= cache::min_memory_bytes(1)
                        && minimum == cache::min_memory_bytes(workers)
                        && shards == workers
            ),
            "{refused:?}"
        );
    }

    /// A call in flight on one table is counted where every other table
    /// of the process reads it: `open_with` gives them one count.
    #[tokio::test]
    async fn test_open_with_gives_every_table_one_count_of_calls_in_flight() {
        let first = two_file_fixture().await;
        let second = two_file_fixture().await;
        let config = config(vec![
            named(FIRST_TABLE, &first, Residency::Full),
            named(SECOND_TABLE, &second, Residency::Full),
        ]);
        let opened = open_with(&config, None, storage_for(&config), FindingLog::Events)
            .await
            .unwrap();
        let tables = opened.tables();
        let [first, second] = &tables[..] else {
            panic!("two tables open");
        };
        assert_eq!(second.calls_in_flight().sum(), 0);
        let held = first.calls_in_flight().enter();
        assert_eq!(second.calls_in_flight().sum(), 1);
        drop(held);
        assert_eq!(second.calls_in_flight().sum(), 0);
    }

    #[tokio::test]
    async fn test_open_with_only_narrows_by_name() {
        let first = two_file_fixture().await;
        let second = two_file_fixture().await;
        let config = config(vec![
            named(FIRST_TABLE, &first, Residency::Full),
            named(SECOND_TABLE, &second, Residency::Full),
        ]);
        let one = open_with(
            &config,
            Some(SECOND_TABLE),
            storage_for(&config),
            FindingLog::Events,
        )
        .await
        .unwrap();
        assert_eq!(names(&one), [SECOND_TABLE]);
        let none = open_with(
            &config,
            Some(NO_TABLE),
            storage_for(&config),
            FindingLog::Events,
        )
        .await
        .unwrap();
        assert!(none.tables().is_empty());
    }

    #[tokio::test]
    async fn test_open_with_local_directory_loads_disk_tables_into_chunk_files() {
        let first = two_file_fixture().await;
        let second = two_file_fixture().await;
        let dir = tempfile::tempdir().unwrap();
        let config = config_with_local(
            dir.path(),
            vec![
                named(FIRST_TABLE, &first, Residency::Disk),
                named(SECOND_TABLE, &second, Residency::Disk),
            ],
        );
        let opened = open_with(&config, None, storage_for(&config), FindingLog::Events)
            .await
            .unwrap();
        let local = opened.local.as_ref().unwrap();
        for table in opened.tables().iter() {
            table.load().await.unwrap();
            assert!(matches!(table.state(), TableState::Serving { .. }));
            let generation = table.current().unwrap();
            assert_eq!(generation.residency, Residency::Disk);
            let identities = generation.identities();
            assert_eq!(identities.len(), crate::approx::length(TWO));
            for identity in &identities {
                assert!(
                    local.chunks_for(identity).unwrap().is_some(),
                    "{} has a chunk file",
                    identity.path
                );
            }
        }
    }

    /// Two entries over one table read the same chunk files, which the
    /// page cache holds once, so they count once: as much as the one
    /// table's, which is everything `index/` holds, beside the disk level.
    #[tokio::test]
    async fn test_page_cache_needed_bytes_counts_a_file_two_entries_share_once() {
        let fixture = two_file_fixture().await;
        let dir = tempfile::tempdir().unwrap();
        let mut config = config_with_local(
            dir.path(),
            vec![
                named(FIRST_TABLE, &fixture, Residency::Disk),
                named(SECOND_TABLE, &fixture, Residency::Disk),
                named(FULL_TABLE, &fixture, Residency::Full),
            ],
        );
        let disk_level = PAGE_CACHE_DISK_BLOCK_BYTES;
        config.storage.local.as_mut().unwrap().page_cache_disk_bytes = disk_level;
        let opened = open_with(&config, None, storage_for(&config), FindingLog::Events)
            .await
            .unwrap();
        assert_eq!(
            opened.page_cache_needed_bytes(),
            disk_level,
            "nothing loaded"
        );
        for table in opened.tables().iter() {
            table.load().await.unwrap();
        }
        let one_table: u64 = table_named(&opened, FIRST_TABLE)
            .unwrap()
            .current()
            .unwrap()
            .chunk_files()
            .map(|(_, chunks)| chunks.end)
            .sum();
        assert!(one_table > 0);
        assert_eq!(opened.page_cache_needed_bytes(), one_table + disk_level);
        let local = opened.local.as_ref().unwrap();
        assert_eq!(one_table, local.index_bytes_in_use().unwrap());
    }

    /// Each table served reports its generation's index bytes, which
    /// `icetray_index_bytes` publishes, and none before it loads; a budget
    /// nothing has read is entirely unfilled.
    #[tokio::test]
    async fn test_index_bytes_lists_each_table_served_and_the_budget_is_unfilled() {
        let fixture = two_file_fixture().await;
        let dir = tempfile::tempdir().unwrap();
        let config = config_with_local(
            dir.path(),
            vec![
                named(FIRST_TABLE, &fixture, Residency::Full),
                named(FULL_TABLE, &fixture, Residency::Bloom),
            ],
        );
        let opened = open_with(&config, None, storage_for(&config), FindingLog::Events)
            .await
            .unwrap();
        assert!(opened.index_bytes().is_empty(), "nothing loaded");
        assert_eq!(opened.unfilled_cache_bytes(), opened.budget().whole());
        for table in opened.tables().iter() {
            table.load().await.unwrap();
        }
        let expected: Vec<u64> = opened
            .tables()
            .iter()
            .map(|table| crate::approx::count(table.current().unwrap().index_bytes))
            .collect();
        assert_eq!(opened.index_bytes(), expected);
        assert!(expected.iter().all(|&bytes| bytes > 0));
    }

    #[tokio::test]
    async fn test_retain_keeps_what_any_table_still_serves() {
        let mut fixture = two_file_fixture().await;
        let dir = tempfile::tempdir().unwrap();
        let config = config_with_local(
            dir.path(),
            vec![
                named(FIRST_TABLE, &fixture, Residency::Disk),
                named(SECOND_TABLE, &fixture, Residency::Disk),
            ],
        );
        let opened = open_with(&config, None, storage_for(&config), FindingLog::Events)
            .await
            .unwrap();
        for table in opened.tables().iter() {
            table.load().await.unwrap();
        }
        let local = opened.local.as_ref().unwrap();
        assert_eq!(opened.retain().await.unwrap(), 0);

        let before = opened.tables()[0].current().unwrap().identities();
        fixture.remove_file("f1");
        fixture
            .commit(
                SECOND_SNAPSHOT,
                Some(FIRST_SNAPSHOT),
                SECOND_SEQUENCE,
                SECOND_VERSION,
            )
            .await;
        assert!(opened.tables()[0].refresh().await.unwrap());
        let after = opened.tables()[0].current().unwrap().identities();
        let dropped: Vec<&DataFileIdentity> = before.difference(&after).collect();
        assert_eq!(dropped.len(), 1);
        assert!(local.chunks_for(dropped[0]).unwrap().is_some());
        assert_eq!(
            opened.retain().await.unwrap(),
            0,
            "the second table still serves the dropped file"
        );

        assert!(opened.tables()[1].refresh().await.unwrap());
        assert!(opened.retain().await.unwrap() > 0);
        assert!(local.chunks_for(dropped[0]).unwrap().is_none());
        assert_eq!(opened.retain().await.unwrap(), 0, "nothing left to free");
    }

    #[tokio::test]
    async fn test_retain_on_a_narrowed_open_frees_nothing() {
        let first = two_file_fixture().await;
        let second = two_file_fixture().await;
        let dir = tempfile::tempdir().unwrap();
        let config = config_with_local(
            dir.path(),
            vec![
                named(FIRST_TABLE, &first, Residency::Disk),
                named(SECOND_TABLE, &second, Residency::Disk),
            ],
        );
        let every = open_with(&config, None, storage_for(&config), FindingLog::Events)
            .await
            .unwrap();
        for table in every.tables().iter() {
            table.load().await.unwrap();
        }
        assert!(every.complete);
        let second_files = every.tables()[1].current().unwrap().identities();
        drop(every);

        let narrowed = open_with(
            &config,
            Some(FIRST_TABLE),
            storage_for(&config),
            FindingLog::Events,
        )
        .await
        .unwrap();
        assert!(!narrowed.complete);
        assert_eq!(names(&narrowed), [FIRST_TABLE]);
        narrowed.tables()[0].load().await.unwrap();
        assert_eq!(narrowed.retain().await.unwrap(), 0);
        let local = narrowed.local.as_ref().unwrap();
        for identity in &second_files {
            assert!(
                local.chunks_for(identity).unwrap().is_some(),
                "{} is live to the table left out",
                identity.path
            );
        }
    }

    /// The daemon runs retention on a spawned task after every load and
    /// swap, so `retain`'s future is `Send`; `tokio::spawn` refuses one
    /// that is not at compile time.
    #[tokio::test]
    async fn test_retain_is_spawnable() {
        let fixture = two_file_fixture().await;
        let dir = tempfile::tempdir().unwrap();
        let config = config_with_local(
            dir.path(),
            vec![named(FIRST_TABLE, &fixture, Residency::Disk)],
        );
        let opened = Arc::new(
            open_with(&config, None, storage_for(&config), FindingLog::Events)
                .await
                .unwrap(),
        );
        opened.tables()[0].load().await.unwrap();
        let retained = tokio::spawn(async move { opened.retain().await });
        assert_eq!(retained.await.unwrap().unwrap(), 0);
    }

    /// The name the reload tests add a table under.
    const THIRD_TABLE: &str = "c";

    /// `names` of `tables`.
    fn names_of(tables: &[Arc<Table>]) -> Vec<String> {
        tables
            .iter()
            .map(|table| table.name().to_string())
            .collect()
    }

    /// Opens `config` and loads every table.
    async fn loaded(config: &Config) -> Opened {
        let opened = open_with(config, None, storage_for(config), FindingLog::Events)
            .await
            .unwrap();
        for table in opened.tables().iter() {
            table.load().await.unwrap();
        }
        opened
    }

    /// A table whose source names a root with no table in it, so its
    /// first load fails.
    fn missing(name: &str, fixture: &Fixture) -> TableConfig {
        TableConfig {
            source: SourceConfig::Static {
                location: format!("{}/missing", fixture.root()),
            },
            ..named(name, fixture, Residency::Full)
        }
    }

    /// A table whose configuration is unchanged is kept as it serves; one
    /// no longer configured is dropped and withdrawn; one newly configured
    /// is loaded and swapped in serving, in configuration order.
    #[tokio::test]
    async fn test_apply_keeps_unchanged_tables_adds_new_ones_and_drops_the_rest() {
        let fixture = two_file_fixture().await;
        let first = named(FIRST_TABLE, &fixture, Residency::Full);
        let config = config(vec![
            first.clone(),
            named(SECOND_TABLE, &fixture, Residency::Full),
        ]);
        let opened = loaded(&config).await;
        let before = opened.tables();
        let serving = before[0].current().unwrap();

        let applied = opened
            .apply(&[named(THIRD_TABLE, &fixture, Residency::Full), first])
            .await
            .unwrap();
        assert_eq!(applied.kept, 1);
        assert_eq!(names_of(&applied.started), [THIRD_TABLE]);
        assert_eq!(names_of(&applied.stopped), [SECOND_TABLE]);
        assert_eq!(names(&opened), [THIRD_TABLE, FIRST_TABLE]);
        let kept = table_named(&opened, FIRST_TABLE).unwrap();
        assert!(Arc::ptr_eq(&kept, &before[0]));
        assert!(Arc::ptr_eq(&kept.current().unwrap(), &serving));
        assert!(matches!(
            table_named(&opened, THIRD_TABLE).unwrap().state(),
            TableState::Serving { .. }
        ));
        assert!(table_named(&opened, SECOND_TABLE).is_none());
        assert!(opened.pending.lock().unwrap().is_empty());
    }

    /// A table whose configuration changed in a field other than `lookups`
    /// and `strict` is built and loaded as a new table under its name, and
    /// the one it replaces is dropped, even when `lookups` and `strict`
    /// changed beside it.
    #[tokio::test]
    async fn test_apply_replaces_a_changed_table_under_its_name() {
        let fixture = two_file_fixture().await;
        let first = named(FIRST_TABLE, &fixture, Residency::Full);
        let opened = loaded(&config(vec![first.clone()])).await;
        let before = table_named(&opened, FIRST_TABLE).unwrap();
        // The changed field is a budget the snapshot fits exactly, so the
        // new table's load succeeds and the replacement completes.
        let index_bytes = before.current().unwrap().index_bytes;
        let changed = TableConfig {
            resident_budget_bytes: Some(approx::count(index_bytes)),
            strict: !first.strict,
            lookups: Some(Level::Balanced),
            ..first
        };

        let applied = opened.apply(std::slice::from_ref(&changed)).await.unwrap();
        assert_eq!(applied.kept, 0);
        assert!(applied.retuned.is_empty());
        assert_eq!(names_of(&applied.started), [FIRST_TABLE]);
        assert!(Arc::ptr_eq(&applied.stopped[0], &before));
        let after = table_named(&opened, FIRST_TABLE).unwrap();
        assert!(!Arc::ptr_eq(&after, &before));
        assert_eq!(*after.config(), changed);
        assert!(after.current().is_some());
    }

    /// The value columns `columns` names, as an entry lists them.
    fn listing(columns: &[&str]) -> Vec<crate::config::ValueColumnConfig> {
        columns
            .iter()
            .map(|&column| crate::config::ValueColumnConfig::named(column))
            .collect()
    }

    /// A loaded table of one file holding `DETAIL_COLUMN` beside the value
    /// column, opened from one entry serving `columns`: its fixture, the
    /// opened tables, the entry, and a key it holds.
    async fn two_column_table(columns: &[&str]) -> (Fixture, Opened, TableConfig, crate::key::Key) {
        let mut fixture = Fixture::with_detail(
            crate::key::KeyType::Long,
            crate::manifest::testing::bucket_spec(1),
        );
        let keys = keys_from(0, E2E_KEYS);
        fixture.add_long_file("f", &keys, 0).await;
        fixture
            .commit(
                FIRST_SNAPSHOT,
                None,
                crate::generation::testing::FIRST_SEQUENCE,
                crate::generation::testing::FIRST_VERSION,
            )
            .await;
        let entry = TableConfig {
            value_columns: listing(columns),
            ..named(FIRST_TABLE, &fixture, Residency::Full)
        };
        let opened = loaded(&config(vec![entry.clone()])).await;
        (fixture, opened, entry, crate::key::Key::Long(keys[0]))
    }

    /// A reload that only reorders a table's value columns changes nothing:
    /// the table keeps serving its generation, and a retune beside the
    /// reorder still reads each column by name (RFD 018, RFD 022).
    #[tokio::test]
    async fn test_apply_reordering_value_columns_is_no_change() {
        let value = crate::manifest::testing::VALUE_COLUMN;
        let detail = crate::generation::testing::DETAIL_COLUMN;
        let (_fixture, opened, both, key) = two_column_table(&[value, detail]).await;
        let before = table_named(&opened, FIRST_TABLE).unwrap();
        let serving = before.current().unwrap();
        let expected = before
            .get(&key, &whole(&[detail, value]))
            .await
            .unwrap()
            .row;

        let reordered = TableConfig {
            value_columns: listing(&[detail, value]),
            ..both
        };
        let applied = opened
            .apply(std::slice::from_ref(&reordered))
            .await
            .unwrap();
        assert_eq!((applied.kept, applied.started.len()), (1, 0), "no change");
        assert!(applied.retuned.is_empty());
        let after = table_named(&opened, FIRST_TABLE).unwrap();
        assert!(Arc::ptr_eq(&after, &before));
        assert!(Arc::ptr_eq(&after.current().unwrap(), &serving));

        let retuned = TableConfig {
            lookups: Some(Level::Permissive),
            ..reordered
        };
        let applied = opened.apply(std::slice::from_ref(&retuned)).await.unwrap();
        assert_eq!(applied.retuned.len(), 1, "a retune beside the reorder");
        assert_eq!(
            after.get(&key, &whole(&[detail, value])).await.unwrap().row,
            expected,
            "the columns still answer by name"
        );
    }

    /// A reload that adds a value column, `[value]` to `[value, detail]`,
    /// builds and loads the table anew and serves the added column; one
    /// that removes it does too, and refuses the column it removed (RFD
    /// 018, RFD 022).
    #[tokio::test]
    async fn test_apply_adding_or_removing_a_value_column_reloads() {
        let value = crate::manifest::testing::VALUE_COLUMN;
        let detail = crate::generation::testing::DETAIL_COLUMN;
        let (_fixture, opened, value_only, key) = two_column_table(&[value]).await;
        let before = table_named(&opened, FIRST_TABLE).unwrap();
        assert!(matches!(
            before.get(&key, &whole(&[detail])).await,
            Err(crate::error::Error::ColumnRequest { .. })
        ));

        let both = TableConfig {
            value_columns: listing(&[value, detail]),
            ..value_only.clone()
        };
        let applied = opened.apply(std::slice::from_ref(&both)).await.unwrap();
        assert_eq!(
            names_of(&applied.started),
            [FIRST_TABLE],
            "a column added reloads"
        );
        let added = table_named(&opened, FIRST_TABLE).unwrap();
        assert!(!Arc::ptr_eq(&added, &before));
        let crate::generation::Row::Present(values) =
            added.get(&key, &whole(&[detail, value])).await.unwrap().row
        else {
            panic!("the key is present");
        };
        let value_bytes = values[1].clone().expect("a present value");
        assert_eq!(
            values,
            [
                Some(bytes::Bytes::from(crate::generation::testing::detail_of(
                    &value_bytes
                ))),
                Some(value_bytes.clone()),
            ]
        );
        assert_ne!(values[0], values[1], "the columns differ");

        let applied = opened
            .apply(std::slice::from_ref(&value_only))
            .await
            .unwrap();
        assert_eq!(
            names_of(&applied.started),
            [FIRST_TABLE],
            "a column removed reloads"
        );
        let removed = table_named(&opened, FIRST_TABLE).unwrap();
        assert!(!Arc::ptr_eq(&removed, &added));
        assert!(matches!(
            removed.get(&key, &whole(&[detail])).await,
            Err(crate::error::Error::ColumnRequest { .. })
        ));
    }

    /// The snapshot a `Serving` state carries; any other state fails.
    fn serving_snapshot(table: &Table) -> SnapshotId {
        let TableState::Serving { snapshot_id, .. } = table.state() else {
            panic!("expected Serving, got {:?}", table.state());
        };
        snapshot_id
    }

    /// A table whose configuration changed in `lookups` alone is retuned in
    /// place: the same table serves the same generation under the new
    /// entry, and the reload reads nothing.
    #[tokio::test]
    async fn test_apply_changes_lookups_in_place_without_a_load() {
        let fixture = two_file_fixture().await;
        let first = named(FIRST_TABLE, &fixture, Residency::Full);
        let opened = loaded(&config(vec![first.clone()])).await;
        let before = table_named(&opened, FIRST_TABLE).unwrap();
        let serving = before.current().unwrap();
        let retuned = TableConfig {
            lookups: Some(Level::Balanced),
            ..first
        };
        let stats = opened.storage.stats();
        let gets = stats.gets();

        let applied = opened.apply(std::slice::from_ref(&retuned)).await.unwrap();
        assert_eq!(stats.gets(), gets, "the reload reads nothing");
        assert_eq!(applied.kept, 0);
        assert!(applied.started.is_empty());
        assert!(applied.stopped.is_empty());
        assert!(Arc::ptr_eq(&applied.retuned[0], &before));
        let after = table_named(&opened, FIRST_TABLE).unwrap();
        assert!(Arc::ptr_eq(&after, &before));
        assert!(Arc::ptr_eq(&after.current().unwrap(), &serving));
        assert_eq!(*after.config(), retuned);
        assert_eq!(after.config().lookups(), Level::Balanced);
        assert_eq!(serving_snapshot(&after), SnapshotId(FIRST_SNAPSHOT));
    }

    /// Appends to `fixture` a file in bucket 0 whose keys interleave with
    /// bucket 0's file's, committed as `SECOND_SNAPSHOT`: the files overlap,
    /// which caps the table at permissive.
    async fn append_overlap(fixture: &mut Fixture) {
        let interleaved = keys_in_bucket(&keys_from(1, E2E_KEYS), bucketing(TWO), 0);
        fixture.add_long_file("f2", &interleaved, 0).await;
        fixture
            .commit(
                SECOND_SNAPSHOT,
                Some(FIRST_SNAPSHOT),
                SECOND_SEQUENCE,
                SECOND_VERSION,
            )
            .await;
    }

    /// `name` following `fixture` in the Full tier, not strict.
    fn lenient(name: &str, fixture: &Fixture) -> TableConfig {
        TableConfig {
            strict: false,
            ..named(name, fixture, Residency::Full)
        }
    }

    /// A reload that would make a table strict while its serving snapshot
    /// is below its level is refused whole, as a start under it would be,
    /// and before any table is loaded: the table it would have added is
    /// never loaded, so the reload reads nothing, and the retuned table
    /// keeps its entry.
    #[tokio::test]
    async fn test_apply_refuses_whole_when_strict_leaves_the_serving_snapshot_below_its_level() {
        let (mut fixture, _) = two_bucket_table().await;
        append_overlap(&mut fixture).await;
        let first = lenient(FIRST_TABLE, &fixture);
        let opened = loaded(&config(vec![first.clone()])).await;
        let before = opened.tables();
        let strict = TableConfig {
            strict: true,
            ..first.clone()
        };
        let stats = opened.storage.stats();
        let gets = stats.gets();

        let refused = opened
            .apply(&[strict, lenient(SECOND_TABLE, &fixture)])
            .await
            .err()
            .expect("the serving snapshot is below the level strict holds it to");
        assert_eq!(stats.gets(), gets, "the added table is never loaded");
        let [(name, error)] = &refused.failures[..] else {
            panic!("one failure: {:?}", refused.failures);
        };
        assert_eq!(name, FIRST_TABLE);
        assert!(
            matches!(
                error,
                Error::Refused {
                    reason: RefusalReason::BelowLevel {
                        declared: Level::Optimal,
                        achieved: Level::Permissive,
                        ..
                    },
                    ..
                }
            ),
            "{error:?}"
        );
        assert!(Arc::ptr_eq(&opened.tables(), &before));
        assert!(table_named(&opened, SECOND_TABLE).is_none());
        assert!(opened.pending.lock().unwrap().is_empty());
        let table = table_named(&opened, FIRST_TABLE).unwrap();
        assert_eq!(*table.config(), first);
        assert_eq!(serving_snapshot(&table), SnapshotId(SECOND_SNAPSHOT));
    }

    /// A retune to a `lookups` its residency cannot reach, in entries no
    /// validation saw, is refused as a load under it would be, and the
    /// table keeps its entry.
    #[tokio::test]
    async fn test_apply_refuses_a_retune_to_lookups_its_residency_cannot_reach() {
        let fixture = two_file_fixture().await;
        let bloom = named(FIRST_TABLE, &fixture, Residency::Bloom);
        let opened = loaded(&config(vec![bloom.clone()])).await;
        let unreachable = TableConfig {
            lookups: Some(Level::Optimal),
            ..bloom.clone()
        };

        let refused = opened
            .apply(std::slice::from_ref(&unreachable))
            .await
            .err()
            .expect("bloom residency cannot reach optimal");
        let [(name, error)] = &refused.failures[..] else {
            panic!("one failure: {:?}", refused.failures);
        };
        assert_eq!(name, FIRST_TABLE);
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
        assert_eq!(*table_named(&opened, FIRST_TABLE).unwrap().config(), bloom);
    }

    /// The `Held` state's snapshots, serving then refused, and the refused
    /// snapshot's level; any other state fails.
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

    /// What `held` reads for a table serving `FIRST_SNAPSHOT` that refused
    /// `SECOND_SNAPSHOT` at permissive.
    const HELD_AT_PERMISSIVE: (SnapshotId, SnapshotId, Level) = (
        SnapshotId(FIRST_SNAPSHOT),
        SnapshotId(SECOND_SNAPSHOT),
        Level::Permissive,
    );

    /// A strict table, `TABLE`, opened and loaded over `fixture`'s first
    /// snapshot, then held at permissive on the overlapping second; with
    /// the table.
    async fn held_table(fixture: &mut Fixture) -> (Opened, Arc<Table>) {
        let opened = loaded(&config(vec![fixture.config(Residency::Full, None)])).await;
        let table = table_named(&opened, TABLE).unwrap();
        assert!(table.config().strict);
        append_overlap(fixture).await;
        assert!(matches!(
            refusal_of(table.refresh().await),
            RefusalReason::BelowLevel { .. }
        ));
        assert_eq!(held(&table), HELD_AT_PERMISSIVE);
        (opened, table)
    }

    /// A held table retuned to a level its refused snapshot still falls
    /// below stays held, and the next poll builds that snapshot again under
    /// the new entry and refuses it again; retuned to a level the snapshot
    /// meets, it serves on, and the next poll builds the snapshot and swaps
    /// it in.
    #[tokio::test]
    async fn test_apply_relaxing_a_held_table_builds_its_snapshot_on_the_next_poll() {
        let (mut fixture, _) = two_bucket_table().await;
        let (opened, table) = held_table(&mut fixture).await;
        let entry = table.config();
        let balanced = TableConfig {
            lookups: Some(Level::Balanced),
            ..(*entry).clone()
        };
        let applied = opened.apply(std::slice::from_ref(&balanced)).await.unwrap();
        assert!(Arc::ptr_eq(&applied.retuned[0], &table));
        assert_eq!(held(&table), HELD_AT_PERMISSIVE);
        assert_eq!(
            refusal_of(table.refresh().await),
            RefusalReason::BelowLevel {
                declared: Level::Balanced,
                achieved: Level::Permissive,
                columns: vec![crate::check::ColumnLevel {
                    column: crate::manifest::testing::VALUE_COLUMN.to_string(),
                    level: Level::Permissive,
                }],
                findings: vec![crate::check::Capping {
                    code: FindingCode::OverlappingFiles,
                    column: None,
                }],
            },
            "the refused snapshot is built again under the new entry"
        );
        assert_eq!(held(&table), HELD_AT_PERMISSIVE);

        let permissive = TableConfig {
            lookups: Some(Level::Permissive),
            ..balanced
        };
        opened
            .apply(std::slice::from_ref(&permissive))
            .await
            .unwrap();
        assert_eq!(serving_snapshot(&table), SnapshotId(FIRST_SNAPSHOT));
        assert_eq!(
            table.current().unwrap().snapshot_id,
            SnapshotId(FIRST_SNAPSHOT),
            "nothing is built until the next poll"
        );
        assert!(table.refresh().await.unwrap());
        assert_eq!(serving_snapshot(&table), SnapshotId(SECOND_SNAPSHOT));
    }

    /// Turning `strict` off ends a hold at once, whatever the held
    /// snapshot's level: the table serves on, and the next poll swaps in
    /// the snapshot it held back.
    #[tokio::test]
    async fn test_apply_turning_strict_off_serves_the_held_snapshot() {
        let (mut fixture, _) = two_bucket_table().await;
        let (opened, table) = held_table(&mut fixture).await;
        let lenient = TableConfig {
            strict: false,
            ..(*table.config()).clone()
        };
        opened.apply(std::slice::from_ref(&lenient)).await.unwrap();
        assert_eq!(serving_snapshot(&table), SnapshotId(FIRST_SNAPSHOT));
        assert!(table.refresh().await.unwrap());
        assert_eq!(serving_snapshot(&table), SnapshotId(SECOND_SNAPSHOT));
    }

    /// A retune forgets a remembered refusal that has nothing to do with
    /// the level, here over budget: the table stays degraded until the next
    /// poll, which builds the refused snapshot again rather than passing it
    /// over.
    #[tokio::test]
    async fn test_apply_retune_forgets_a_remembered_over_budget_refusal() {
        let (mut fixture, all) = two_bucket_table().await;
        let unbounded = fixture.config(Residency::Full, None);
        let opened = loaded(&config(vec![unbounded.clone()])).await;
        let first_bytes = table_named(&opened, TABLE)
            .unwrap()
            .current()
            .unwrap()
            .index_bytes;
        let bounded = TableConfig {
            resident_budget_bytes: Some(approx::count(first_bytes)),
            ..unbounded
        };
        opened.apply(std::slice::from_ref(&bounded)).await.unwrap();
        let table = table_named(&opened, TABLE).unwrap();
        let above = keys_from(all.last().unwrap() + KEY_STEP, E2E_KEYS);
        fixture
            .add_long_file("f2", &keys_in_bucket(&above, bucketing(TWO), 0), 0)
            .await;
        fixture
            .commit(
                SECOND_SNAPSHOT,
                Some(FIRST_SNAPSHOT),
                SECOND_SEQUENCE,
                SECOND_VERSION,
            )
            .await;
        table.refresh().await.unwrap_err();
        poll_held_back(&opened, &table).await;

        let flipped = TableConfig {
            strict: !bounded.strict,
            ..bounded
        };
        let applied = opened.apply(std::slice::from_ref(&flipped)).await.unwrap();
        assert!(Arc::ptr_eq(&applied.retuned[0], &table));
        assert!(matches!(
            table.state(),
            TableState::Degraded {
                reason: ErrorKind::Refused(RefusalReason::OverBudget { .. }),
                ..
            }
        ));
        let stats = opened.storage.stats();
        let before = stats.gets();
        assert!(matches!(
            refusal_of(table.refresh().await),
            RefusalReason::OverBudget {
                of: Budget::Resident,
                ..
            }
        ));
        assert!(
            stats.gets() > before + POLL_GETS,
            "the refused snapshot is built again"
        );
    }

    /// One poll of `table` whose source still names the location of a
    /// snapshot it refused over budget: the version hint alone is read
    /// through `opened`'s storage, nothing is swapped, and the table stays
    /// degraded for the refusal.
    async fn poll_held_back(opened: &Opened, table: &Table) {
        let stats = opened.storage.stats();
        let before = stats.gets();
        assert!(!table.refresh().await.unwrap());
        assert_eq!(stats.gets(), before + POLL_GETS, "the version hint alone");
        assert!(
            matches!(
                table.state(),
                TableState::Degraded {
                    reason: ErrorKind::Refused(RefusalReason::OverBudget { .. }),
                    ..
                }
            ),
            "{:?}",
            table.state()
        );
    }

    /// A refusal that depends on the table's entry, here its resident
    /// budget, is remembered by the table that made it, and a reload that
    /// leaves the entry as it is keeps that table and what it remembers; a
    /// reload that changes the entry builds the refused snapshot again
    /// under the new one.
    #[tokio::test]
    async fn test_apply_tries_a_remembered_refusal_again_under_a_changed_entry() {
        let (mut fixture, all) = two_bucket_table().await;
        let unbounded = named(FIRST_TABLE, &fixture, Residency::Full);
        let opened = loaded(&config(vec![unbounded.clone()])).await;
        let first_bytes = table_named(&opened, FIRST_TABLE)
            .unwrap()
            .current()
            .unwrap()
            .index_bytes;
        let bounded = TableConfig {
            resident_budget_bytes: Some(approx::count(first_bytes)),
            ..unbounded.clone()
        };
        opened.apply(std::slice::from_ref(&bounded)).await.unwrap();
        let table = table_named(&opened, FIRST_TABLE).unwrap();
        let above = keys_from(all.last().unwrap() + KEY_STEP, E2E_KEYS);
        let appended = keys_in_bucket(&above, bucketing(TWO), 0);
        fixture.add_long_file("f2", &appended, 0).await;
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
                Error::Refused {
                    reason: RefusalReason::OverBudget {
                        of: Budget::Resident,
                        ..
                    },
                    ..
                }
            ),
            "{error:?}"
        );
        poll_held_back(&opened, &table).await;

        let applied = opened.apply(std::slice::from_ref(&bounded)).await.unwrap();
        assert_eq!(applied.kept, 1);
        assert!(Arc::ptr_eq(
            &table_named(&opened, FIRST_TABLE).unwrap(),
            &table
        ));
        poll_held_back(&opened, &table).await;

        let applied = opened
            .apply(std::slice::from_ref(&unbounded))
            .await
            .unwrap();
        assert_eq!(names_of(&applied.started), [FIRST_TABLE]);
        assert!(applied.retuned.is_empty());
        let replaced = table_named(&opened, FIRST_TABLE).unwrap();
        assert_eq!(
            replaced.current().unwrap().snapshot_id,
            SnapshotId(SECOND_SNAPSHOT)
        );
    }

    /// A table that fails its first load refuses the whole reload: the set
    /// serving is the one before it, a table the reload would have added
    /// is dropped, and nothing is left pending.
    #[tokio::test]
    async fn test_apply_refuses_every_change_when_one_table_fails_to_load() {
        let fixture = two_file_fixture().await;
        let first = named(FIRST_TABLE, &fixture, Residency::Full);
        let opened = loaded(&config(vec![first.clone()])).await;
        let before = opened.tables();

        let refused = opened
            .apply(&[
                named(SECOND_TABLE, &fixture, Residency::Full),
                missing(THIRD_TABLE, &fixture),
            ])
            .await
            .err()
            .expect("the missing table fails its load");
        let failed: Vec<&str> = refused
            .failures
            .iter()
            .map(|(name, _)| name.as_str())
            .collect();
        assert_eq!(failed, [THIRD_TABLE]);
        assert!(Arc::ptr_eq(&opened.tables(), &before));
        assert!(table_named(&opened, SECOND_TABLE).is_none());
        assert!(opened.pending.lock().unwrap().is_empty());
        assert!(matches!(
            table_named(&opened, FIRST_TABLE).unwrap().state(),
            TableState::Serving { .. }
        ));
    }

    /// A reload dropped while its new table loads leaves nothing behind:
    /// nothing pending, and the set serving as it was.
    #[tokio::test]
    async fn test_apply_dropped_while_loading_leaves_the_set_as_it_was() {
        let fixture = two_file_fixture().await;
        let first = named(FIRST_TABLE, &fixture, Residency::Full);
        let opened = loaded(&config(vec![first.clone()])).await;
        let before = opened.tables();
        let configs = [first, named(SECOND_TABLE, &fixture, Residency::Full)];
        // Every read from here on stalls, so the new table's load is
        // waiting when the reload is dropped; a plain local read can finish
        // within the first poll and let the whole reload complete.
        opened.storage.register(
            FILE_PREFIX,
            Arc::new(ThrottledStore::new(
                LocalFileSystem::new(),
                ThrottleConfig {
                    wait_get_per_call: STALL,
                    ..ThrottleConfig::default()
                },
            )),
        );

        let mut applying = Box::pin(opened.apply(&configs));
        assert!(
            futures::poll!(applying.as_mut()).is_pending(),
            "the new table's load waits on its reads"
        );
        assert_eq!(
            opened.pending.lock().unwrap().len(),
            1,
            "the reload registered its table"
        );
        drop(applying);
        assert!(opened.pending.lock().unwrap().is_empty());
        assert!(Arc::ptr_eq(&opened.tables(), &before));
        assert!(table_named(&opened, SECOND_TABLE).is_none());
    }

    /// `retain` waits for the load lock of a table still loading for a
    /// reload before it computes its live set, and the chunk files that
    /// table wrote are live until the reload lets it go, and freed after.
    #[tokio::test]
    async fn test_retain_waits_for_and_keeps_what_a_pending_table_loads() {
        let first = two_file_fixture().await;
        let second = two_file_fixture().await;
        let dir = tempfile::tempdir().unwrap();
        let config = config_with_local(
            dir.path(),
            vec![named(FIRST_TABLE, &first, Residency::Disk)],
        );
        let opened = loaded(&config).await;
        let local = opened.local.as_ref().unwrap();
        let pending = Arc::new(
            opened
                .build(&named(SECOND_TABLE, &second, Residency::Disk))
                .await
                .unwrap(),
        );
        let registered = opened.register(std::slice::from_ref(&pending)).await;
        let loading = pending.serialized().await;
        assert!(
            opened.retain().now_or_never().is_none(),
            "retain waits for the pending table's load"
        );
        drop(loading);
        pending.load().await.unwrap();
        let written = pending.current().unwrap().identities();

        assert_eq!(opened.retain().await.unwrap(), 0);
        for identity in &written {
            assert!(local.chunks_for(identity).unwrap().is_some());
        }
        drop(registered);
        assert!(opened.retain().await.unwrap() > 0);
        for identity in &written {
            assert!(local.chunks_for(identity).unwrap().is_none());
        }
    }

    /// A reload that drops a disk table lets `retain` free its chunk files.
    #[tokio::test]
    async fn test_retain_after_apply_frees_a_dropped_table_s_chunk_files() {
        let first = two_file_fixture().await;
        let second = two_file_fixture().await;
        let dir = tempfile::tempdir().unwrap();
        let kept = named(FIRST_TABLE, &first, Residency::Disk);
        let config = config_with_local(
            dir.path(),
            vec![kept.clone(), named(SECOND_TABLE, &second, Residency::Disk)],
        );
        let opened = loaded(&config).await;
        let dropped = table_named(&opened, SECOND_TABLE)
            .unwrap()
            .current()
            .unwrap()
            .identities();

        opened.apply(&[kept]).await.unwrap();
        assert!(opened.retain().await.unwrap() > 0);
        let local = opened.local.as_ref().unwrap();
        for identity in &dropped {
            assert!(local.chunks_for(identity).unwrap().is_none());
        }
    }

    #[tokio::test]
    async fn test_retain_without_local_directory_is_zero() {
        let fixture = two_file_fixture().await;
        let config = config(vec![named(FIRST_TABLE, &fixture, Residency::Full)]);
        let opened = open_with(&config, None, storage_for(&config), FindingLog::Events)
            .await
            .unwrap();
        opened.tables()[0].load().await.unwrap();
        assert_eq!(opened.retain().await.unwrap(), 0);
    }
}
