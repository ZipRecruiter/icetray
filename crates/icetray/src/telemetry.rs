//! The binary's share of the metrics (RFD 012): the Prometheus
//! recorder with the latency buckets, the `requests_total` counter with its
//! `call` and `code` labels, registered once as one handle per pair, and
//! the scrape, which writes every gauge a
//! live table, the page cache, the service, the reload, and the process
//! hold before it renders, so the recorder drops the gauges of a table no
//! longer served. Core's modules emit every other series.

use std::fmt;
use std::sync::{Arc, Mutex, OnceLock, PoisonError};
use std::time::Duration;

use icetray_core::{Opened, Tables, approx, metric, time};
use icetray_proto::v1::icetray_server::SERVICE_NAME;
use metrics::Counter;
use metrics_exporter_prometheus::{Matcher, PrometheusBuilder, PrometheusHandle};
use metrics_util::MetricKindMask;
use tikv_jemalloc_ctl::{epoch, stats};
use tonic::Code;

use crate::error::ServeError;
use crate::reload::Pending;
use crate::service::Slots;

/// Histogram bucket bounds in seconds for `lookup_seconds` and
/// `object_get_seconds`, from 100 microseconds to 2 seconds: sized from
/// measured object-store first-byte latency, whose typical read sits in the
/// tens of milliseconds and whose tail runs to seconds (RFD 013), with a
/// 1-2.5-5 progression so each decade has three bounds.
pub const LATENCY_BUCKETS: [f64; 14] = [
    0.0001, 0.00025, 0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.0,
];

/// Histogram bucket bounds in seconds for `scan_seconds`: `LATENCY_BUCKETS`
/// extended to 10 seconds, past the default `server.scan_deadline`
/// (`DEFAULT_SCAN_DEADLINE`). A cold Scan waits on its slowest span GET
/// across every file that holds the key, after the key-page round under
/// `bloom` (RFD 025, Cost).
pub const SCAN_LATENCY_BUCKETS: [f64; 16] = [
    0.0001, 0.00025, 0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.0,
    5.0, 10.0,
];

/// Histogram bucket bounds for `scan_rows`: 0 for a key with no rows inside
/// the bounds, then a 1-2.5-5 progression through the default
/// `max_scan_rows` (`DEFAULT_MAX_SCAN_ROWS`) and two bounds past it, for a
/// table that raises it.
pub const SCAN_ROWS_BUCKETS: [f64; 14] = [
    0.0, 1.0, 2.0, 5.0, 10.0, 25.0, 50.0, 100.0, 250.0, 500.0, 1000.0, 2500.0, 5000.0, 10000.0,
];

/// Histogram bucket bounds for `scan_files`: 0 for a key no file holds, 1
/// for a key held in one file, and then the counts shared edge keys,
/// appends, and overlap reach, up to the 64 files past which a table is
/// far from any level.
pub const SCAN_FILES_BUCKETS: [f64; 11] =
    [0.0, 1.0, 2.0, 3.0, 4.0, 6.0, 8.0, 12.0, 16.0, 32.0, 64.0];

/// Histogram bucket bounds in bytes for `scan_response_bytes`: fourfold from
/// 1 KiB to 1 MiB, then doubling to 16 MiB through the default
/// `server.max_scan_bytes` (`DEFAULT_MAX_SCAN_BYTES`), so a response at the
/// bound and one just past it fall in different buckets, then fourfold to
/// 256 MiB for the size of a refused response.
pub const SCAN_RESPONSE_BUCKETS: [f64; 12] = [
    1_024.0,
    4_096.0,
    16_384.0,
    65_536.0,
    262_144.0,
    1_048_576.0,
    2_097_152.0,
    4_194_304.0,
    8_388_608.0,
    16_777_216.0,
    67_108_864.0,
    268_435_456.0,
];

/// The bucket bounds of every histogram the binary bounds, by metric name:
/// what `install` sets, and the one list the tests read.
const HISTOGRAM_BUCKETS: [(&str, &[f64]); 6] = [
    (metric::LOOKUP_SECONDS, &LATENCY_BUCKETS),
    (metric::OBJECT_GET_SECONDS, &LATENCY_BUCKETS),
    (metric::SCAN_SECONDS, &SCAN_LATENCY_BUCKETS),
    (metric::SCAN_ROWS, &SCAN_ROWS_BUCKETS),
    (metric::SCAN_FILES, &SCAN_FILES_BUCKETS),
    (metric::SCAN_RESPONSE_BYTES, &SCAN_RESPONSE_BUCKETS),
];

/// How long a gauge no one writes stays in the exposition: none, so a
/// render drops every gauge left unwritten since the render before it.
/// Every render writes each live gauge first (`render_after`), so what it
/// drops is a gauge nothing holds any more, such as a removed table's,
/// which would otherwise read its last value until the process ends.
/// Counters and histograms never expire.
const GAUGE_IDLE_TIMEOUT: Duration = Duration::ZERO;

/// The handle of the one recorder a process installs.
static HANDLE: OnceLock<PrometheusHandle> = OnceLock::new();

/// Serializes every render with the gauge writes before it, so one render
/// cannot find another's writes already seen and drop them as idle.
static RENDERING: Mutex<()> = Mutex::new(());

/// Serializes `install`, so two callers racing to install do not both build
/// a recorder and have the second refused by the `metrics` crate.
static INSTALLING: Mutex<()> = Mutex::new(());

/// Installs the Prometheus recorder once per process and returns its handle;
/// every later call returns the same handle, since a `metrics` recorder is
/// global and the daemon's tests start several servers in one process.
/// Histograms use `LATENCY_BUCKETS`, or the scan sets for the scan
/// histograms; gauges expire after
/// `GAUGE_IDLE_TIMEOUT`; every metric is described (`metric::describe`).
pub fn install() -> Result<PrometheusHandle, ServeError> {
    let _installing = INSTALLING.lock().unwrap_or_else(PoisonError::into_inner);
    if let Some(handle) = HANDLE.get() {
        return Ok(handle.clone());
    }
    let handle = HISTOGRAM_BUCKETS
        .iter()
        .try_fold(PrometheusBuilder::new(), |builder, &(name, buckets)| {
            builder.set_buckets_for_metric(Matcher::Full(name.to_string()), buckets)
        })?
        .idle_timeout(MetricKindMask::GAUGE, Some(GAUGE_IDLE_TIMEOUT))
        .install_recorder()?;
    metric::describe();
    Ok(HANDLE.get_or_init(|| handle).clone())
}

/// What separates the parts of a gRPC request's path,
/// `/<service>/<method>`.
const PATH_SEPARATOR: char = '/';

/// The RPC a request was: the `call` label of `requests_total`. Display
/// gives the label value.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Call {
    /// `Icetray.Get`.
    Get,
    /// `Icetray.MultiGet`.
    MultiGet,
    /// `Icetray.TableInfo`.
    TableInfo,
    /// `Icetray.Scan`.
    Scan,
    /// `Icetray.MultiScan`.
    MultiScan,
}

impl Call {
    /// Every call, for the series registered at start, each at its
    /// `index`.
    pub const ALL: [Self; 5] = [
        Self::Get,
        Self::MultiGet,
        Self::TableInfo,
        Self::Scan,
        Self::MultiScan,
    ];

    /// The call's place in `ALL`.
    const fn index(self) -> usize {
        match self {
            Self::Get => 0,
            Self::MultiGet => 1,
            Self::TableInfo => 2,
            Self::Scan => 3,
            Self::MultiScan => 4,
        }
    }

    /// The method's name in the contract, the last part of its path.
    pub const fn method(self) -> &'static str {
        match self {
            Self::Get => "Get",
            Self::MultiGet => "MultiGet",
            Self::TableInfo => "TableInfo",
            Self::Scan => "Scan",
            Self::MultiScan => "MultiScan",
        }
    }

    /// The call a request's `path` names, `/<service>/<method>`; `None`
    /// for a path of another service or a method the contract does not
    /// have, which is not a call.
    pub fn of_path(path: &str) -> Option<Self> {
        let method = path
            .strip_prefix(PATH_SEPARATOR)?
            .strip_prefix(SERVICE_NAME)?
            .strip_prefix(PATH_SEPARATOR)?;
        Self::ALL.into_iter().find(|call| call.method() == method)
    }
}

impl fmt::Display for Call {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let name = match self {
            Self::Get => "get",
            Self::MultiGet => "multi_get",
            Self::TableInfo => "table_info",
            Self::Scan => "scan",
            Self::MultiScan => "multi_scan",
        };
        f.write_str(name)
    }
}

/// Every gRPC status code, each at the place of its number, so a code's
/// number indexes it (`code_index`).
const CODES: [Code; 17] = [
    Code::Ok,
    Code::Cancelled,
    Code::Unknown,
    Code::InvalidArgument,
    Code::DeadlineExceeded,
    Code::NotFound,
    Code::AlreadyExists,
    Code::PermissionDenied,
    Code::ResourceExhausted,
    Code::FailedPrecondition,
    Code::Aborted,
    Code::OutOfRange,
    Code::Unimplemented,
    Code::Internal,
    Code::Unavailable,
    Code::DataLoss,
    Code::Unauthenticated,
];

/// `code`'s place in `CODES`: its number, which the gRPC specification
/// fixes from 0 to 16.
fn code_index(code: Code) -> usize {
    usize::try_from(i32::from(code)).expect("a gRPC status code's number is not negative")
}

/// A gRPC status code as the `code` label of `requests_total`. Display
/// gives the code's name in the gRPC specification, in lower snake case.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CodeLabel(pub Code);

impl fmt::Display for CodeLabel {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let name = match self.0 {
            Code::Ok => "ok",
            Code::Cancelled => "cancelled",
            Code::Unknown => "unknown",
            Code::InvalidArgument => "invalid_argument",
            Code::DeadlineExceeded => "deadline_exceeded",
            Code::NotFound => "not_found",
            Code::AlreadyExists => "already_exists",
            Code::PermissionDenied => "permission_denied",
            Code::ResourceExhausted => "resource_exhausted",
            Code::FailedPrecondition => "failed_precondition",
            Code::Aborted => "aborted",
            Code::OutOfRange => "out_of_range",
            Code::Unimplemented => "unimplemented",
            Code::Internal => "internal",
            Code::Unavailable => "unavailable",
            Code::DataLoss => "data_loss",
            Code::Unauthenticated => "unauthenticated",
        };
        f.write_str(name)
    }
}

/// The `requests_total` series of every call answering every code,
/// registered once with the recorder current then, so counting a request
/// formats no label, builds no label list, and looks up no series. Every
/// series is exposed from the start, at zero until its first request. A
/// process installs its recorder (`install`) before it registers these:
/// series registered earlier record to no recorder. Clone shares the
/// handles.
#[derive(Clone, Debug)]
pub struct Requests {
    /// By `Call::index`, then by `code_index`.
    counters: Arc<[[Counter; CODES.len()]; Call::ALL.len()]>,
}

impl Requests {
    /// Registers every call's series of every code.
    pub fn register() -> Self {
        Self {
            counters: Arc::new(Call::ALL.map(|call| CODES.map(|code| requests_total(call, code)))),
        }
    }

    /// Counts one request of `call` that answered `code`.
    pub fn count(&self, call: Call, code: Code) {
        self.counters[call.index()][code_index(code)].increment(1);
    }
}

/// The `requests_total` series of `call` answering `code`, registered.
fn requests_total(call: Call, code: Code) -> Counter {
    metrics::counter!(
        metric::REQUESTS_TOTAL,
        metric::LABEL_CALL => call.to_string(),
        metric::LABEL_CODE => CodeLabel(code).to_string()
    )
}

/// What a scrape of `/metrics` reads: the recorder, the tables and page
/// cache whose counts and gauges it publishes first, the service's
/// in-flight slots, and the reload's pending flag when the run follows its
/// configuration. Clone: every metrics connection holds one.
#[derive(Clone)]
pub struct Scrape {
    handle: PrometheusHandle,
    opened: Arc<Opened>,
    slots: Slots,
    pending: Option<Pending>,
}

impl Scrape {
    /// A scrape of `handle`'s recorder over `opened`, `slots`, and
    /// `pending`.
    pub fn new(
        handle: PrometheusHandle,
        opened: Arc<Opened>,
        slots: Slots,
        pending: Option<Pending>,
    ) -> Self {
        Self {
            handle,
            opened,
            slots,
            pending,
        }
    }

    /// The exposition, after `publish`.
    pub fn render(&self) -> String {
        render_after(&self.handle, || self.publish())
    }

    /// Writes every series a scrape reads as it stands: each table's lookup
    /// counts (`Table::publish_lookup_counts`) and gauges
    /// (`Table::publish_gauges`), the page cache's hit counts and bytes,
    /// the row cache's hits, misses, and bytes, the cache budget's
    /// shortfall, the in-flight gauges from the slots, `config_pending`,
    /// and the sampled gauges (`sample`). The counts are published so a
    /// scrape reads every lookup answered and every page and row hit before
    /// it; the gauges are written so the recorder keeps them
    /// (`GAUGE_IDLE_TIMEOUT`).
    fn publish(&self) {
        let tables = self.opened.tables();
        for table in tables.iter() {
            table.publish_lookup_counts();
            table.publish_gauges();
        }
        self.opened.cache.publish_hit_counts();
        self.opened.cache.publish_usage();
        self.opened.row_cache.publish();
        self.opened.shortfall.publish();
        metrics::gauge!(metric::REQUESTS_IN_FLIGHT).set(approx::gauge_len(self.slots.admitted()));
        metrics::gauge!(metric::MAX_IN_FLIGHT).set(approx::gauge_len(self.slots.limit()));
        if let Some(pending) = &self.pending {
            pending.publish();
        }
        sample(&tables);
    }
}

/// `handle`'s exposition, rendered right after `publish` writes the gauges
/// it keeps, with no other render between them. Every render in the
/// process goes through here.
fn render_after(handle: &PrometheusHandle, publish: impl FnOnce()) -> String {
    let _rendering = RENDERING.lock().unwrap_or_else(PoisonError::into_inner);
    publish();
    handle.render()
}

/// Samples the gauges the binary owns: for every one of `tables` serving a
/// generation, `generation_age_seconds` from the snapshot's own timestamp
/// against now and `table_oldest_delete_age_seconds` from the commit of
/// the oldest applying delete's snapshot, 0 with none;
/// `process_rss_bytes` when the platform reports a resident set size; and
/// the allocator gauges. A gauge left unsampled drops out of the
/// exposition.
fn sample(tables: &Tables) {
    for table in tables.iter() {
        if let Some(generation) = table.current() {
            metrics::gauge!(
                metric::GENERATION_AGE_SECONDS,
                metric::LABEL_TABLE => table.name().to_string()
            )
            .set(time::age_seconds(generation.snapshot_timestamp));
            metrics::gauge!(
                metric::TABLE_OLDEST_DELETE_AGE_SECONDS,
                metric::LABEL_TABLE => table.name().to_string()
            )
            .set(
                generation
                    .oldest_delete_committed
                    .map_or(0.0, time::age_seconds),
            );
        }
    }
    if let Some(stats) = memory_stats::memory_stats() {
        metrics::gauge!(metric::PROCESS_RSS_BYTES).set(approx::gauge_len(stats.physical_mem));
    }
    sample_allocator();
}

/// Sets the allocator gauges from jemalloc's statistics. jemalloc refreshes
/// them only when its epoch advances, so the epoch is advanced first. A
/// statistic jemalloc cannot read is logged at debug and left unset: the
/// gauges explain `process_rss_bytes`, and serving does not depend on them.
fn sample_allocator() {
    if let Err(error) = epoch::advance() {
        tracing::debug!(%error, "jemalloc statistics are unavailable");
        return;
    }
    let gauges = [
        (metric::ALLOCATOR_ALLOCATED_BYTES, stats::allocated::read()),
        (metric::ALLOCATOR_ACTIVE_BYTES, stats::active::read()),
        (metric::ALLOCATOR_METADATA_BYTES, stats::metadata::read()),
        (metric::ALLOCATOR_RESIDENT_BYTES, stats::resident::read()),
        (metric::ALLOCATOR_RETAINED_BYTES, stats::retained::read()),
    ];
    for (name, bytes) in gauges {
        match bytes {
            Ok(bytes) => metrics::gauge!(name).set(approx::gauge_len(bytes)),
            Err(error) => tracing::debug!(%error, gauge = name, "jemalloc statistic unavailable"),
        }
    }
}

#[cfg(test)]
mod tests {
    use icetray_testkit::verify::series_value;

    use super::*;

    /// The lowest bucket bound the contract fixes: 100 microseconds.
    const FIRST_BUCKET: f64 = 1e-4;

    /// The highest bucket bound the contract fixes: 2 seconds.
    const LAST_BUCKET: f64 = 2.0;

    #[test]
    fn test_fmt_call_label_values() {
        let cases = [
            (Call::Get, "get"),
            (Call::MultiGet, "multi_get"),
            (Call::TableInfo, "table_info"),
            (Call::Scan, "scan"),
            (Call::MultiScan, "multi_scan"),
        ];
        for (call, name) in cases {
            assert_eq!(call.to_string(), name);
        }
    }

    #[test]
    fn test_fmt_code_label_names_every_grpc_code() {
        let cases = [
            (Code::Ok, "ok"),
            (Code::Cancelled, "cancelled"),
            (Code::Unknown, "unknown"),
            (Code::InvalidArgument, "invalid_argument"),
            (Code::DeadlineExceeded, "deadline_exceeded"),
            (Code::NotFound, "not_found"),
            (Code::AlreadyExists, "already_exists"),
            (Code::PermissionDenied, "permission_denied"),
            (Code::ResourceExhausted, "resource_exhausted"),
            (Code::FailedPrecondition, "failed_precondition"),
            (Code::Aborted, "aborted"),
            (Code::OutOfRange, "out_of_range"),
            (Code::Unimplemented, "unimplemented"),
            (Code::Internal, "internal"),
            (Code::Unavailable, "unavailable"),
            (Code::DataLoss, "data_loss"),
            (Code::Unauthenticated, "unauthenticated"),
        ];
        for (code, name) in cases {
            assert_eq!(CodeLabel(code).to_string(), name, "{code:?}");
        }
    }

    /// Once registered, every call's series of every code is exposed,
    /// before any request of it.
    #[test]
    fn test_register_exposes_every_call_answering_every_code() {
        let handle = install().unwrap();
        Requests::register();
        let exposition = render_after(&handle, || {});
        for call in Call::ALL {
            for code in CODES {
                let (call, code) = (call.to_string(), CodeLabel(code).to_string());
                assert!(
                    series_value(
                        &exposition,
                        metric::REQUESTS_TOTAL,
                        &[(metric::LABEL_CALL, &call), (metric::LABEL_CODE, &code)],
                    )
                    .is_some(),
                    "{call} {code} in {exposition}"
                );
            }
        }
    }

    #[test]
    fn test_of_path_names_the_contract_s_calls_alone() {
        for call in Call::ALL {
            let path = format!("/{SERVICE_NAME}/{}", call.method());
            assert_eq!(Call::of_path(&path), Some(call), "{path}");
        }
        for path in [
            format!("/{SERVICE_NAME}/Nothing"),
            format!("/{SERVICE_NAME}/Get/more"),
            "/grpc.health.v1.Health/Check".to_string(),
            format!("{SERVICE_NAME}/Get"),
        ] {
            assert_eq!(Call::of_path(&path), None, "{path}");
        }
    }

    /// Each call and each code has a place of its own in the handles.
    #[test]
    fn test_index_and_code_index_are_each_one_s_place() {
        for (place, call) in Call::ALL.into_iter().enumerate() {
            assert_eq!(call.index(), place, "{call}");
        }
        for (place, code) in CODES.into_iter().enumerate() {
            assert_eq!(code_index(code), place, "{code:?}");
        }
    }

    /// A gauge written before one render and not since is gone from the
    /// next, while one written before each stays: what drops a removed
    /// table's gauges. The gauges are named for this test alone.
    #[test]
    fn test_render_after_drops_a_gauge_no_render_wrote() {
        const KEPT: &str = "icetray_test_kept";
        const DROPPED: &str = "icetray_test_dropped";
        let handle = install().unwrap();
        let first = render_after(&handle, || {
            metrics::gauge!(KEPT).set(1.0);
            metrics::gauge!(DROPPED).set(1.0);
        });
        for name in [KEPT, DROPPED] {
            assert!(
                series_value(&first, name, &[]).is_some(),
                "{name} in {first}"
            );
        }
        let second = render_after(&handle, || metrics::gauge!(KEPT).set(1.0));
        assert!(series_value(&second, KEPT, &[]).is_some(), "{second}");
        assert_eq!(series_value(&second, DROPPED, &[]), None, "{second}");
    }

    #[test]
    fn test_histogram_buckets_strictly_increase() {
        for (name, buckets) in HISTOGRAM_BUCKETS {
            assert!(
                buckets
                    .windows(2)
                    .all(|pair| pair[0].total_cmp(&pair[1]).is_lt()),
                "{name}: {buckets:?}"
            );
        }
        assert_eq!(
            SCAN_LATENCY_BUCKETS[..LATENCY_BUCKETS.len()],
            LATENCY_BUCKETS,
            "a Scan's latency is bucketed as a lookup's, and further"
        );
    }

    /// A histogram recorded under a table is exposed as buckets, with its
    /// own bounds, not as the summary a histogram with no bounds gets.
    #[test]
    fn test_install_buckets_every_histogram() {
        const TABLE: &str = "icetray_test_buckets";
        let handle = install().unwrap();
        for (name, bounds) in HISTOGRAM_BUCKETS {
            metrics::histogram!(name, metric::LABEL_TABLE => TABLE).record(bounds[0]);
            let exposition = render_after(&handle, || {});
            let le = bounds[0].to_string();
            let bucket = format!("{name}_bucket");
            assert_eq!(
                series_value(
                    &exposition,
                    &bucket,
                    &[(metric::LABEL_TABLE, TABLE), ("le", &le)]
                ),
                Some(1.0),
                "{name} in {exposition}"
            );
        }
    }

    #[test]
    fn test_latency_buckets_strictly_increase_from_first_to_last() {
        assert!(
            LATENCY_BUCKETS
                .windows(2)
                .all(|pair| pair[0].total_cmp(&pair[1]).is_lt())
        );
        assert_eq!(LATENCY_BUCKETS[0].to_bits(), FIRST_BUCKET.to_bits());
        assert_eq!(
            LATENCY_BUCKETS[LATENCY_BUCKETS.len() - 1].to_bits(),
            LAST_BUCKET.to_bits()
        );
    }

    /// Each allocator gauge is set, and they nest as jemalloc defines them:
    /// live bytes within active pages within what it holds resident.
    #[test]
    fn test_sample_allocator_sets_every_gauge_in_order() {
        let handle = install().unwrap();
        let exposition = render_after(&handle, sample_allocator);
        let read = |name: &str| {
            series_value(&exposition, name, &[]).unwrap_or_else(|| panic!("{name} in {exposition}"))
        };
        let allocated = read(metric::ALLOCATOR_ALLOCATED_BYTES);
        let active = read(metric::ALLOCATOR_ACTIVE_BYTES);
        let resident = read(metric::ALLOCATOR_RESIDENT_BYTES);
        read(metric::ALLOCATOR_METADATA_BYTES);
        read(metric::ALLOCATOR_RETAINED_BYTES);
        assert!(
            allocated <= active,
            "allocated {allocated} > active {active}"
        );
        assert!(active <= resident, "active {active} > resident {resident}");
    }

    #[test]
    fn test_install_returns_one_handle_per_process() {
        let first = install().unwrap();
        let second = install().unwrap();
        // Two handles onto one recorder both see a count made through the
        // global recorder; the whole expositions are not compared, since
        // another test in this process may count between two renders.
        Requests::register().count(Call::TableInfo, Code::Ok);
        let call = Call::TableInfo.to_string();
        let code = CodeLabel(Code::Ok).to_string();
        for handle in [&first, &second] {
            let exposition = render_after(handle, || {});
            let value = series_value(
                &exposition,
                metric::REQUESTS_TOTAL,
                &[(metric::LABEL_CALL, &call), (metric::LABEL_CODE, &code)],
            )
            .unwrap_or_else(|| panic!("{call} in {exposition}"));
            assert!(value >= 1.0, "{value}");
        }
    }
}
