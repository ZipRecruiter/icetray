//! The daemon (RFD 012, RFD 005): both listeners bound and the health
//! service registered before any table loads, every table loaded
//! concurrently, readiness and `SERVING` once every table serves, one
//! refresh task per table and the recorder's upkeep on the poll interval, a
//! sample of the page cache's hold on the chunk files on its own interval, the
//! configuration followed when the run was given one to follow (RFD 018),
//! and a drain on shutdown that ends health watches, waits up to the drain
//! deadline for in-flight calls, and flushes the page cache's disk level.
//! A first-load failure keeps health `NOT_SERVING` for a grace period, so
//! an orchestrator's probe sees it, then ends the run; a periodic task that
//! panics ends the run at once, with the task named.

use std::collections::{HashMap, HashSet};
use std::convert::Infallible;
use std::fmt;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use futures::future::join_all;
use icetray_core::config::{Config, Residency, ServerConfig};
use icetray_core::memory::{self, MemoryLimit};
use icetray_core::open::open_with;
use icetray_core::{
    ErrorKind, FindingLog, Opened, PAGE_CACHE_SAMPLE_INTERVAL, Storage, Table, approx, open,
};
use icetray_proto::v1::icetray_server::{IcetrayServer, SERVICE_NAME};
use metrics_exporter_prometheus::PrometheusHandle;
use tokio::net::TcpListener;
use tokio::sync::{Notify, watch};
use tokio::task::{AbortHandle, Id, JoinError, JoinHandle, JoinSet};
use tokio::time::MissedTickBehavior;
use tonic::transport::Server;
use tonic::transport::server::TcpIncoming;
use tonic_health::ServingStatus;
use tonic_health::server::HealthReporter;

use crate::counted::Counted;
use crate::error::ServeError;
use crate::listen::serve_http;
use crate::reload::{Pending, Reload, Reloader};
use crate::service::Service;
use crate::telemetry::{Requests, Scrape};

/// How long a first-load failure stays visible on the health service before
/// the process exits: long enough for an orchestrator's probe, at the usual
/// ten-second period, to see `NOT_SERVING` beside the logged reason (RFD
/// 012), so the restart it then sees has a cause on record.
pub const START_FAILURE_GRACE: Duration = Duration::from_secs(10);

/// The gRPC health protocol's name for the server as a whole.
pub const OVERALL_SERVICE: &str = "";

/// Every name the health service answers for: the server as a whole and
/// the `Icetray` service, which change status together.
const HEALTH_NAMES: [&str; 2] = [OVERALL_SERVICE, SERVICE_NAME];

/// What a caller of `Serving::start` chooses; the binary follows its
/// configuration file and takes the default grace, and tests shorten the
/// grace.
#[derive(Clone, Debug)]
pub struct ServeOptions {
    /// How long a first-load failure stays on the health service before the
    /// run ends.
    pub start_failure_grace: Duration,
    /// Where the run reads its configuration again once every table
    /// serves (RFD 018); `None` serves the tables it started with until it
    /// ends.
    pub reload: Option<Reload>,
}

impl Default for ServeOptions {
    fn default() -> Self {
        Self {
            start_failure_grace: START_FAILURE_GRACE,
            reload: None,
        }
    }
}

/// A running daemon: the addresses it bound, what it opened, and the
/// handles to wait on it and to stop it.
pub struct Serving {
    listen: SocketAddr,
    metrics: SocketAddr,
    opened: Arc<Opened>,
    ready: watch::Receiver<bool>,
    shutdown: watch::Sender<bool>,
    /// Asks the run to read its configuration again now.
    reload: Arc<Notify>,
    task: JoinHandle<Result<(), ServeError>>,
}

/// Both listeners, bound before any table is opened, so a port that is
/// taken is reported before a local directory or a cache is touched.
struct Listeners {
    grpc: TcpListener,
    metrics: TcpListener,
    /// The address the gRPC listener bound.
    listen: SocketAddr,
    /// The address the metrics and readiness listener bound.
    metrics_addr: SocketAddr,
}

impl Listeners {
    /// Binds `server`'s two addresses, naming the one that fails.
    async fn bind(server: &ServerConfig) -> Result<Self, ServeError> {
        let grpc = bind(server.listen).await?;
        let metrics = bind(server.metrics_listen).await?;
        let listen = local_addr(&grpc, server.listen)?;
        let metrics_addr = local_addr(&metrics, server.metrics_listen)?;
        Ok(Self {
            grpc,
            metrics,
            listen,
            metrics_addr,
        })
    }
}

impl Serving {
    /// Binds both listeners, opens every table through `open` (storage from
    /// the SDK's default chain, RFD 003), registers health, starts the gRPC
    /// and HTTP servers, then spawns the run that loads, refreshes, and
    /// drains. Returns once both listeners accept and the health service
    /// answers `NOT_SERVING`; the loads run behind it (RFD 012).
    pub async fn start(
        config: Config,
        handle: PrometheusHandle,
        options: ServeOptions,
    ) -> Result<Self, ServeError> {
        let listeners = Listeners::bind(&config.server).await?;
        let opened = open(&config, None, FindingLog::Events).await?;
        Ok(Self::start_serving(config, listeners, opened, handle, options).await)
    }

    /// `start` over a storage the caller built, through `open_with`, so a
    /// test registers its own stores and never touches the SDK chain.
    pub async fn start_over(
        config: Config,
        storage: Arc<Storage>,
        handle: PrometheusHandle,
        options: ServeOptions,
    ) -> Result<Self, ServeError> {
        let listeners = Listeners::bind(&config.server).await?;
        let opened = open_with(&config, None, storage, FindingLog::Events).await?;
        Ok(Self::start_serving(config, listeners, opened, handle, options).await)
    }

    /// Registers health as `NOT_SERVING`, starts the servers on `listeners`
    /// over `opened`, logs `listening`, and spawns the run.
    async fn start_serving(
        config: Config,
        listeners: Listeners,
        opened: Opened,
        handle: PrometheusHandle,
        mut options: ServeOptions,
    ) -> Self {
        let (health, health_service) = tonic_health::server::health_reporter();
        set_health(&health, ServingStatus::NotServing).await;

        let opened = Arc::new(opened);
        let service = Service::new(&opened, &config.server);
        let pending = options.reload.is_some().then(Pending::default);
        let scrape = Scrape::new(
            handle.clone(),
            Arc::clone(&opened),
            service.slots().clone(),
            pending.clone(),
        );
        let (shutdown, _stop) = watch::channel(false);
        let (ready_tx, ready) = watch::channel(false);

        let grpc = tokio::spawn(
            Server::builder()
                .add_service(health_service)
                .add_service(Counted::new(
                    IcetrayServer::new(service),
                    Requests::register(),
                ))
                .serve_with_incoming_shutdown(
                    grpc_incoming(listeners.grpc),
                    stop(shutdown.subscribe()),
                ),
        );
        let http = tokio::spawn(serve_http(
            listeners.metrics,
            scrape,
            ready.clone(),
            shutdown.subscribe(),
        ));
        tracing::info!(
            grpc = %listeners.listen,
            metrics = %listeners.metrics_addr,
            tables = opened.tables().len(),
            "listening"
        );

        let reload = Arc::new(Notify::new());
        let poll_interval = config.server.poll_interval;
        let drain_deadline = config.server.drain_deadline;
        let reloader = options.reload.take().zip(pending).map(|(source, pending)| {
            Reloader::new(source, config, poll_interval, Arc::clone(&reload), pending)
        });
        let run = Run {
            opened: Arc::clone(&opened),
            health,
            handle,
            options,
            poll_interval,
            drain_deadline,
            reloader,
            shutdown: shutdown.clone(),
            ready: ready_tx,
            grpc,
            http,
        };
        let task = tokio::spawn(run.run());
        Self {
            listen: listeners.listen,
            metrics: listeners.metrics_addr,
            opened,
            ready,
            shutdown,
            reload,
            task,
        }
    }

    /// The address the gRPC listener bound.
    pub fn listen_addr(&self) -> SocketAddr {
        self.listen
    }

    /// The address the metrics and readiness listener bound.
    pub fn metrics_addr(&self) -> SocketAddr {
        self.metrics
    }

    /// The storage, cache, local directory, and tables the daemon serves.
    pub fn opened(&self) -> &Arc<Opened> {
        &self.opened
    }

    /// Completes once every table serves a generation, or at once when the
    /// run has already ended, which a first-load failure does.
    pub async fn ready(&mut self) {
        let mut stopped = self.shutdown.subscribe();
        tokio::select! {
            _ = self.ready.wait_for(|ready| *ready) => {}
            _ = stopped.wait_for(|stop| *stop) => {}
        }
    }

    /// Completes when the run is ending: a caller asked for `shutdown`, or
    /// the run failed its first loads and signaled it itself.
    pub async fn stopped(&self) {
        stop(self.shutdown.subscribe()).await;
    }

    /// Asks the run to read its configuration again now, as a poll would
    /// but also retrying a configuration the last reload refused (RFD
    /// 018); the binary calls it on SIGHUP. A request made before every
    /// table serves is taken up once they do. Without `ServeOptions::reload`
    /// it does nothing.
    pub fn reload(&self) {
        self.reload.notify_one();
    }

    /// Asks the run to drain: the servers stop accepting, in-flight calls
    /// finish or the drain stops waiting for them at its deadline, the
    /// refresh tasks end, and the page cache's disk level is flushed.
    /// `join` reports how it went.
    pub fn shutdown(&self) {
        self.shutdown.send_modify(|stop| *stop = true);
    }

    /// Waits for the run to end: `Ok` after a drain, `DrainOverran` after a
    /// drain that passed its deadline, `FirstLoad` when a table failed its
    /// first load, or the failure that ended it.
    pub async fn join(self) -> Result<(), ServeError> {
        self.task.await?
    }
}

/// Binds `addr`, naming it in the failure.
async fn bind(addr: SocketAddr) -> Result<TcpListener, ServeError> {
    TcpListener::bind(addr)
        .await
        .map_err(|source| ServeError::Bind { addr, source })
}

/// The gRPC listener as tonic serves it, with Nagle off on every accepted
/// connection. The builder's `tcp_nodelay` applies only to a listener tonic
/// binds itself; one bound here, before any table loads, carries the
/// setting on the incoming or goes without it, and without it an answer
/// larger than one segment waits out the client's delayed acknowledgment.
fn grpc_incoming(listener: TcpListener) -> TcpIncoming {
    TcpIncoming::from(listener).with_nodelay(Some(true))
}

/// The address `listener` bound, which differs from `addr` when `addr`'s
/// port is 0.
fn local_addr(listener: &TcpListener, addr: SocketAddr) -> Result<SocketAddr, ServeError> {
    listener
        .local_addr()
        .map_err(|source| ServeError::Bind { addr, source })
}

/// Completes when `shutdown` becomes true. A closed channel completes it
/// too: the sender lives in the `Serving` and its run, and when both are
/// gone whatever awaits this has nothing left to serve.
async fn stop(mut shutdown: watch::Receiver<bool>) {
    // intentional: a closed channel is the daemon gone, which ends the wait
    // the same way a shutdown does.
    let _ = shutdown.wait_for(|stop| *stop).await;
}

/// A task the run keeps on an interval, named so the failure that ends one
/// says which. Display gives the name the log and `ServeError`
/// carry.
#[derive(Debug, PartialEq, Eq)]
pub enum PeriodicTask {
    /// The refresh of one table's ref.
    Refresh {
        /// The table's configured name.
        table: String,
    },
    /// The recorder's upkeep, which drains its histogram samples.
    Upkeep,
    /// The sample of which chunk-file pages the page cache holds, for
    /// every `disk` table's level (RFD 020).
    PageCacheSample,
}

impl fmt::Display for PeriodicTask {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Refresh { table } => write!(f, "refresh of table {table}"),
            Self::Upkeep => f.write_str("recorder upkeep"),
            Self::PageCacheSample => f.write_str("page cache sample"),
        }
    }
}

/// The run's periodic tasks, each under its name. Every loop runs until it
/// is aborted, so a task that ends on its own ends by a `JoinError`: a
/// panic inside it, or an abort by `stop_refresh`.
struct Periodic {
    tasks: JoinSet<Infallible>,
    names: HashMap<Id, PeriodicTask>,
    /// Each served table's refresh task, to stop when a reload drops the
    /// table.
    refreshes: Vec<(Arc<Table>, AbortHandle)>,
    /// Refresh tasks `stop_refresh` aborted, whose cancellation `ended`
    /// passes over.
    stopped: HashSet<Id>,
}

impl Periodic {
    fn new() -> Self {
        Self {
            tasks: JoinSet::new(),
            names: HashMap::new(),
            refreshes: Vec::new(),
            stopped: HashSet::new(),
        }
    }

    /// Spawns `future` as `task`.
    fn spawn(
        &mut self,
        task: PeriodicTask,
        future: impl Future<Output = Infallible> + Send + 'static,
    ) -> AbortHandle {
        let handle = self.tasks.spawn(future);
        self.names.insert(handle.id(), task);
        handle
    }

    /// Spawns the refresh of `table`, over `opened`, every `poll_interval`.
    fn spawn_refresh(&mut self, table: &Arc<Table>, opened: &Arc<Opened>, poll_interval: Duration) {
        let handle = self.spawn(
            PeriodicTask::Refresh {
                table: table.name().to_string(),
            },
            refresh_loop(Arc::clone(table), Arc::clone(opened), poll_interval),
        );
        self.refreshes.push((Arc::clone(table), handle));
    }

    /// Aborts the refresh of `table`, which a reload dropped. A poll in
    /// flight is dropped where it waits, as a shutdown drops it.
    fn stop_refresh(&mut self, table: &Arc<Table>) {
        let Some(at) = self
            .refreshes
            .iter()
            .position(|(refreshed, _)| Arc::ptr_eq(refreshed, table))
        else {
            return;
        };
        let (_, handle) = self.refreshes.swap_remove(at);
        self.stopped.insert(handle.id());
        handle.abort();
    }

    /// Completes when a task ends other than by `stop_refresh`, naming it.
    /// The upkeep is in the set until `shutdown`, so there is always a
    /// task to wait on.
    async fn ended(&mut self) -> (PeriodicTask, JoinError) {
        loop {
            let outcome = self
                .tasks
                .join_next()
                .await
                .expect("the upkeep is in the set until shutdown");
            if let Err(error) = &outcome
                && error.is_cancelled()
                && self.stopped.contains(&error.id())
            {
                self.named(outcome);
                continue;
            }
            return self.named(outcome);
        }
    }

    /// Aborts every task and waits for them to end. Returns the first task
    /// that ended by a panic rather than by the abort, with its panic: one
    /// that panicked before the abort, or while running on another thread
    /// as the abort landed, is reported instead of discarded.
    async fn shutdown(&mut self) -> Option<(PeriodicTask, JoinError)> {
        self.tasks.abort_all();
        let mut panicked = None;
        while let Some(outcome) = self.tasks.join_next().await {
            let (task, error) = self.named(outcome);
            if panicked.is_none() && error.is_panic() {
                panicked = Some((task, error));
            }
        }
        panicked
    }

    /// The name of the task that ended with `outcome`, taken out of the
    /// names, beside how it ended.
    fn named(&mut self, outcome: Result<Infallible, JoinError>) -> (PeriodicTask, JoinError) {
        match outcome {
            Ok(never) => match never {},
            Err(error) => {
                self.stopped.remove(&error.id());
                let task = self
                    .names
                    .remove(&error.id())
                    .expect("every task in the set was spawned under a name");
                (task, error)
            }
        }
    }
}

/// The run behind a `Serving`: the loads, the readiness flip, the periodic
/// tasks, and the drain.
struct Run {
    opened: Arc<Opened>,
    health: HealthReporter,
    handle: PrometheusHandle,
    options: ServeOptions,
    poll_interval: Duration,
    drain_deadline: Duration,
    /// Follows the configuration once every table serves, when the run
    /// was given one to follow.
    reloader: Option<Reloader>,
    shutdown: watch::Sender<bool>,
    ready: watch::Sender<bool>,
    grpc: JoinHandle<Result<(), tonic::transport::Error>>,
    http: JoinHandle<()>,
}

impl Run {
    /// The loads, then either the failed start (the grace, the drain, and
    /// `FirstLoad`) or the serving run until shutdown. A shutdown asked for
    /// while the tables load ends the loads where they are waiting and
    /// drains: no table serves, so a lookup is refused at once rather than
    /// held open, the drain ends `Ok` well inside its deadline, and the page
    /// cache is closed before an orchestrator's grace runs out. A
    /// dropped load swaps nothing in, releases its disk budget reservation,
    /// and leaves no partial chunk file, since a chunk file is written
    /// under a temporary name and renamed; what it did write, the next
    /// start's retention deletes unless a table references it. A shutdown
    /// asked for once the loads have failed cuts the grace short. A
    /// periodic task that panics before the shutdown aborts it, a pending
    /// shutdown included, ends the run as `Periodic`, so the process exits
    /// non-zero instead of serving on with nothing refreshing. A reload
    /// that applies stops the refreshes of the tables it dropped and
    /// starts those of the tables it added, then runs retention; a
    /// shutdown drops a reload in flight, which swaps nothing in.
    async fn run(mut self) -> Result<(), ServeError> {
        // Biased, the loads first: when they have ended and a shutdown is
        // pending at the same poll, their outcome stands.
        let loaded = tokio::select! {
            biased;
            failures = load_all(&self.opened) => Some(failures),
            () = stop(self.shutdown.subscribe()) => None,
        };
        let Some(failures) = loaded else {
            tracing::info!("shutdown during the first loads");
            return self.drain().await;
        };
        if !failures.is_empty() {
            tokio::select! {
                () = tokio::time::sleep(self.options.start_failure_grace) => {}
                () = stop(self.shutdown.subscribe()) => {}
            }
            return self.fail(ServeError::FirstLoad { failures }).await;
        }
        if *self.shutdown.borrow() {
            return self.drain().await;
        }

        retain(&self.opened).await;
        warn_memory_room(&self.opened);
        set_health(&self.health, ServingStatus::Serving).await;
        self.ready.send_modify(|ready| *ready = true);

        let mut periodic = Periodic::new();
        for table in self.opened.tables().iter() {
            periodic.spawn_refresh(table, &self.opened, self.poll_interval);
        }
        periodic.spawn(
            PeriodicTask::Upkeep,
            upkeep_loop(self.handle.clone(), self.poll_interval),
        );
        periodic.spawn(
            PeriodicTask::PageCacheSample,
            page_cache_loop(Arc::clone(&self.opened)),
        );

        // Biased, the ended task first: when a task has ended and a
        // shutdown is pending at the same poll, the failure is the outcome;
        // then the shutdown, over a reload that finished at the same poll.
        let mut reloader = self.reloader.take();
        let ended = loop {
            tokio::select! {
                biased;
                ended = periodic.ended() => break Some(ended),
                () = stop(self.shutdown.subscribe()) => break None,
                applied = next_reload(reloader.as_mut(), &self.opened) => {
                    for table in &applied.stopped {
                        periodic.stop_refresh(table);
                    }
                    for table in &applied.started {
                        periodic.spawn_refresh(table, &self.opened, self.poll_interval);
                    }
                    tokio::select! {
                        () = retain(&self.opened) => {}
                        () = stop(self.shutdown.subscribe()) => break None,
                    }
                    if starts_a_disk_table(&applied.started) {
                        warn_memory_room(&self.opened);
                    }
                }
            }
        };
        let raced = periodic.shutdown().await;
        let Some((task, source)) = ended.or(raced) else {
            return self.drain().await;
        };
        tracing::error!(%task, %source, "periodic task ended");
        self.fail(ServeError::Periodic { task, source }).await
    }

    /// Ends the run with `error`: the shutdown is signaled, so the servers
    /// and the readiness listener stop, and the drain's own failure is
    /// logged, since `error` is the outcome that names the cause.
    async fn fail(self, error: ServeError) -> Result<(), ServeError> {
        self.shutdown.send_modify(|stop| *stop = true);
        if let Err(drain) = self.drain().await {
            tracing::error!(error = %drain, "drain after a failed run");
        }
        Err(error)
    }

    /// Ends every health watch, waits up to the drain deadline for the
    /// servers to finish their in-flight work, and closes the page cache, so
    /// the disk level is flushed before the process exits (RFD 017). tonic's
    /// graceful shutdown waits for every connection to close, and a `Watch`
    /// stream the health service still feeds would hold its connection open
    /// without end. Past the deadline both server tasks are aborted, the
    /// cache is still closed, and the drain is `DrainOverran`, the close's
    /// own failure logged beside it. tonic serves each connection on a task
    /// of its own, which the abort does not reach: a call still open ends
    /// with the process.
    async fn drain(self) -> Result<(), ServeError> {
        let Self {
            mut health,
            mut grpc,
            mut http,
            opened,
            drain_deadline,
            ..
        } = self;
        withdraw_health(&mut health).await;
        let servers = tokio::time::timeout(drain_deadline, async {
            (&mut grpc).await??;
            (&mut http).await?;
            Ok::<(), ServeError>(())
        })
        .await;
        let Ok(served) = servers else {
            grpc.abort();
            http.abort();
            if let Err(error) = opened.cache.close().await {
                tracing::error!(%error, "page cache close after the drain overran");
            }
            return Err(ServeError::DrainOverran {
                after: drain_deadline,
            });
        };
        served?;
        opened.cache.close().await?;
        Ok(())
    }
}

/// Sets every health name to `status`, which every watch of it is sent.
async fn set_health(health: &HealthReporter, status: ServingStatus) {
    for name in HEALTH_NAMES {
        health.set_service_status(name, status).await;
    }
}

/// Sets every health name `NOT_SERVING`, then removes it. Removing a name
/// drops its status sender, so every `Watch` stream on it ends after the
/// `NOT_SERVING` it has not yet sent; a check or watch that arrives later
/// is answered `NOT_FOUND` at once instead of opening a stream.
async fn withdraw_health(health: &mut HealthReporter) {
    set_health(health, ServingStatus::NotServing).await;
    for name in HEALTH_NAMES {
        health.clear_service_status(name).await;
    }
}

/// The next reload `reloader` applies; without one, never.
async fn next_reload(reloader: Option<&mut Reloader>, opened: &Opened) -> icetray_core::Applied {
    match reloader {
        Some(reloader) => reloader.next(opened).await,
        None => std::future::pending().await,
    }
}

/// Loads every table concurrently and logs each outcome; returns the
/// tables that failed with the kind of their failure.
async fn load_all(opened: &Opened) -> Vec<(String, ErrorKind)> {
    let tables = opened.tables();
    let outcomes = join_all(tables.iter().map(|table| table.load())).await;
    let mut failures = Vec::new();
    for (table, outcome) in tables.iter().zip(outcomes) {
        match outcome {
            Ok(()) => log_serving(table),
            Err(error) => {
                tracing::error!(
                    table = table.name(),
                    reason = %error.kind(),
                    %error,
                    "table failed to load"
                );
                failures.push((table.name().to_string(), error.kind()));
            }
        }
    }
    failures
}

/// One `info` event for the generation `table` serves.
fn log_serving(table: &Table) {
    if let Some(generation) = table.current() {
        tracing::info!(
            table = table.name(),
            snapshot_id = %generation.snapshot_id,
            files = generation.files.len(),
            records = generation.records,
            residency = %generation.residency,
            index_bytes = generation.index_bytes,
            "table serving"
        );
    }
}

/// Deletes the chunk files no table serves any more and logs what it freed;
/// a failure here leaves the daemon serving and is logged.
pub(crate) async fn retain(opened: &Opened) {
    match opened.retain().await {
        Ok(freed) if freed > 0 => tracing::info!(freed_bytes = freed, "chunk files retained"),
        Ok(_) => {}
        Err(error) => {
            tracing::warn!(reason = %error.kind(), %error, "chunk file retention failed");
        }
    }
}

/// What the memory-room check found (`room`).
#[derive(Debug, PartialEq, Eq)]
enum Room {
    /// Nothing needs the kernel's page cache: no chunk file is served and
    /// there is no disk level.
    NotNeeded,
    /// No memory limit, or no resident set, to compare with.
    Unknown {
        /// The bytes needed.
        needed: u64,
    },
    /// What is needed fits.
    Fits,
    /// It does not.
    Short {
        /// The bytes needed.
        needed: u64,
        /// The limit compared with.
        limit: MemoryLimit,
        /// The resident set beside it.
        rss: u64,
        /// What does not fit.
        missing_bytes: u64,
    },
}

/// Whether `needed` bytes of page cache fit under the memory limit `limit`
/// reads beside the resident set `rss` reads. Neither is read when nothing
/// is needed.
fn room(
    needed: u64,
    limit: impl FnOnce() -> Option<MemoryLimit>,
    rss: impl FnOnce() -> Option<u64>,
) -> Room {
    if needed == 0 {
        return Room::NotNeeded;
    }
    let (Some(limit), Some(rss)) = (limit(), rss()) else {
        return Room::Unknown { needed };
    };
    match memory::shortfall(needed, limit.bytes, rss) {
        Some(missing_bytes) => Room::Short {
            needed,
            limit,
            rss,
            missing_bytes,
        },
        None => Room::Fits,
    }
}

/// Warns when the page cache's disk level and the chunk files of every
/// table served, each counted once (`Opened::page_cache_needed_bytes`),
/// need more of the kernel's page cache than the memory limit leaves
/// beside the process's resident set (RFD 020), so an undersized
/// deployment is told at start, before a sample finds its `disk` tables
/// short of the page cache. The run calls it once the first loads have
/// served and retention has run, and after a reload that started a `disk`
/// table (`starts_a_disk_table`). A resident set measured right after
/// loads can include memory the allocator has freed and not yet returned,
/// so the warning can overstate what is missing. Without a resident set or
/// a limit to compare, nothing is checked, and that is logged at debug.
fn warn_memory_room(opened: &Opened) {
    let rss = || memory_stats::memory_stats().map(|stats| approx::count(stats.physical_mem));
    match room(opened.page_cache_needed_bytes(), memory::limit, rss) {
        Room::NotNeeded | Room::Fits => {}
        Room::Unknown { needed } => tracing::debug!(
            needed,
            "no memory limit or resident set to check the page cache's room against"
        ),
        Room::Short {
            needed,
            limit,
            rss,
            missing_bytes,
        } => tracing::warn!(
            needed,
            limit = limit.bytes,
            limit_source = %limit.of,
            rss,
            missing_bytes,
            "the chunk files and the page cache's disk level need more page cache than the memory limit leaves beside the process"
        ),
    }
}

/// Whether a reload that swapped in `started` added a `disk` table, which
/// the memory-room check then runs again for.
fn starts_a_disk_table(started: &[Arc<Table>]) -> bool {
    started
        .iter()
        .any(|table| table.config().residency == Residency::Disk)
}

/// An interval of `period` whose first tick is one period from now, not at
/// once, and which delays rather than bursts after a missed tick.
pub(crate) fn ticker(period: Duration) -> tokio::time::Interval {
    let start = tokio::time::Instant::now() + period;
    let mut interval = tokio::time::interval_at(start, period);
    interval.set_missed_tick_behavior(MissedTickBehavior::Delay);
    interval
}

/// Polls `table`'s ref every `poll_interval` (RFD 005): a swap is logged and
/// followed by retention; a failure is logged with the snapshot still
/// served, which core keeps serving and counts under `refresh_errors_total`.
/// A failure fixed for the snapshot, a refusal or a table or file fault of
/// it, fails one poll, so it is logged once: core remembers it, and the
/// polls after it return `Ok(false)` until the source names another
/// metadata location.
/// The poll's outcome is bound before anything else awaits, so the finished
/// poll future is dropped first and the loop stays `Send`.
async fn refresh_loop(
    table: Arc<Table>,
    opened: Arc<Opened>,
    poll_interval: Duration,
) -> Infallible {
    let mut ticker = ticker(poll_interval);
    loop {
        ticker.tick().await;
        let outcome = table.refresh().await;
        if report_refresh(&table, outcome) {
            retain(&opened).await;
        }
    }
}

/// Logs one poll's outcome; true when a new generation was swapped in.
fn report_refresh(table: &Table, outcome: icetray_core::Result<bool>) -> bool {
    match outcome {
        Ok(true) => {
            if let Some(generation) = table.current() {
                tracing::info!(
                    table = table.name(),
                    snapshot_id = %generation.snapshot_id,
                    "table advanced"
                );
            }
            true
        }
        Ok(false) => false,
        Err(error) => {
            if let Some(generation) = table.current() {
                tracing::warn!(
                    table = table.name(),
                    reason = %error.kind(),
                    %error,
                    snapshot_id = %generation.snapshot_id,
                    "refresh failed"
                );
            } else {
                tracing::warn!(
                    table = table.name(),
                    reason = %error.kind(),
                    %error,
                    "refresh failed"
                );
            }
            false
        }
    }
}

/// Runs the recorder's upkeep every `poll_interval`, which drains the
/// histogram samples recorded since, so they do not pile up between
/// scrapes, or without any. The gauges are written by the scrape itself
/// (`Scrape::render`).
async fn upkeep_loop(handle: PrometheusHandle, poll_interval: Duration) -> Infallible {
    let mut ticker = ticker(poll_interval);
    loop {
        ticker.tick().await;
        handle.run_upkeep();
    }
}

/// Samples the page cache's hold on every served table's chunk files at
/// once, then every `PAGE_CACHE_SAMPLE_INTERVAL` (RFD 020), so a `disk`
/// table's level reflects its chunk files from the first scrape after the
/// tables serve. A sample of the tables a reload swapped in follows from
/// the next tick, since each reads the set served then.
async fn page_cache_loop(opened: Arc<Opened>) -> Infallible {
    let mut ticker = ticker(PAGE_CACHE_SAMPLE_INTERVAL);
    loop {
        opened.sample_page_cache().await;
        ticker.tick().await;
    }
}

#[cfg(test)]
mod tests {
    use icetray_core::budget;
    use icetray_core::config::{SourceConfig, StorageConfig, TableConfig};
    use icetray_core::memory::LimitSource;
    use icetray_core::{CallsInFlight, PageCache, RowCache, TableHandles, TableSource};
    use tokio::sync::oneshot;

    use super::*;

    /// A table name for the refresh task that never ends.
    const TABLE: &str = "kv";

    /// Every connection the gRPC listener accepts has Nagle off: an answer
    /// larger than one segment otherwise waits out the client's delayed
    /// acknowledgment, about 40 ms on Linux, on every call.
    #[tokio::test]
    async fn test_grpc_connections_are_accepted_with_nodelay() {
        use futures::StreamExt as _;

        let listener = TcpListener::bind(SocketAddr::from(([127, 0, 0, 1], 0)))
            .await
            .expect("a loopback port binds");
        let addr = listener.local_addr().expect("the bound address");
        let mut incoming = grpc_incoming(listener);
        let _client = tokio::net::TcpStream::connect(addr)
            .await
            .expect("the client connects");
        let accepted = incoming
            .next()
            .await
            .expect("the listener yields the connection")
            .expect("the connection is accepted");
        assert!(accepted.nodelay().expect("TCP_NODELAY reads back"));
    }

    /// A limit of a gibibyte, from the cgroup.
    const GIB: MemoryLimit = MemoryLimit {
        bytes: 1 << 30,
        of: LimitSource::Cgroup,
    };

    /// Nothing needed is never short and reads neither the limit nor the
    /// resident set; with either unread the room is unknown; otherwise
    /// what does not fit beside the resident set is short.
    #[test]
    fn test_room_skips_nothing_needed_and_names_what_is_missing() {
        let unread_limit = || -> Option<MemoryLimit> { panic!("the limit was read") };
        let unread_rss = || -> Option<u64> { panic!("the resident set was read") };
        assert_eq!(room(0, unread_limit, unread_rss), Room::NotNeeded);
        let half = GIB.bytes / 2;
        assert_eq!(
            room(half, || None, || Some(half)),
            Room::Unknown { needed: half }
        );
        assert_eq!(
            room(half, || Some(GIB), || None),
            Room::Unknown { needed: half }
        );
        assert_eq!(room(half, || Some(GIB), || Some(half)), Room::Fits);
        assert_eq!(
            room(GIB.bytes, || Some(GIB), || Some(half)),
            Room::Short {
                needed: GIB.bytes,
                limit: GIB,
                rss: half,
                missing_bytes: half,
            }
        );
    }

    /// A table in `residency`, never loaded, over the default cache budget
    /// split as a server with a table reading rows splits it.
    async fn table_in(residency: Residency) -> Arc<Table> {
        let server = ServerConfig::default();
        let page_cache_shards = 1;
        let budget = budget::resolve(&server, None, true, page_cache_shards).unwrap();
        let source = SourceConfig::Static {
            location: "file:///nowhere".to_string(),
        };
        let config = TableConfig {
            name: TABLE.to_string(),
            source: source.clone(),
            r#ref: "main".to_string(),
            key_column: "key".to_string(),
            value_columns: vec![icetray_core::ValueColumnConfig::named("value")],
            order_column: None,
            max_scan_rows: None,
            strict: false,
            lookups: None,
            residency,
            resident_budget_bytes: None,
            row_cache: None,
        };
        let handles = TableHandles {
            storage: Arc::new(Storage::new(StorageConfig::default())),
            cache: PageCache::new(approx::length(budget.page_bytes), None)
                .await
                .unwrap(),
            row_cache: Some(RowCache::new(budget.row_bytes)),
            local: None,
            calls: CallsInFlight::new(),
            findings: FindingLog::Debug,
            batch_fan_out: server.batch_fan_out,
            max_multi_scan_values: server.max_multi_scan_values,
            max_scan_bytes: server.max_scan_bytes,
        };
        Arc::new(Table::new(
            config,
            TableSource::from_config(&source).await.unwrap(),
            handles,
            server.load_deadline,
            server.refresh_deadline,
            server.get_deadline,
            server.scan_deadline,
        ))
    }

    /// A reload checks the memory room again when it starts a `disk`
    /// table, and only then.
    #[tokio::test]
    async fn test_starts_a_disk_table_only_for_a_disk_table_started() {
        let full = table_in(Residency::Full).await;
        let bloom = table_in(Residency::Bloom).await;
        let disk = table_in(Residency::Disk).await;
        assert!(!starts_a_disk_table(&[]));
        assert!(!starts_a_disk_table(&[
            Arc::clone(&full),
            Arc::clone(&bloom)
        ]));
        assert!(starts_a_disk_table(&[full, disk]));
    }

    /// A task that has already panicked when the shutdown aborts the set is
    /// reported by name with its panic, not discarded with the aborted ones.
    #[tokio::test]
    async fn test_shutdown_reports_a_task_that_panicked_before_it() {
        let mut periodic = Periodic::new();
        let (unwound, panicked) = oneshot::channel::<()>();
        periodic.spawn(PeriodicTask::Upkeep, async move {
            let _unwound = unwound;
            panic!("the upkeep fails");
        });
        periodic.spawn(
            PeriodicTask::Refresh {
                table: TABLE.to_string(),
            },
            std::future::pending(),
        );
        // The sender drops while the panic unwinds; on this current-thread
        // runtime the task's poll, and so its completion, ends before this
        // task runs again.
        assert!(panicked.await.is_err());

        let reported = periodic.shutdown().await;
        let Some((task, source)) = reported else {
            panic!("expected the panicked task, got none");
        };
        assert_eq!(task, PeriodicTask::Upkeep);
        assert!(source.is_panic(), "{source}");
    }
}
