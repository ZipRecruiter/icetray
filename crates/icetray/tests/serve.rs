//! The daemon in-process over the in-process S3 server (RFD 014, P tier):
//! health and readiness before and after the first loads, the wire contract
//! of RFD 010 with its error mapping, request counting, refresh within the
//! poll interval, the drain, the start-or-fail rule of RFD 005, and the
//! configuration reload of RFD 018.

use std::collections::BTreeSet;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use hyper::StatusCode;
use icetray::listen::{METRICS_PATH, NOT_READY_BODY, READY_BODY, READY_PATH};
use icetray::reload::{ConfigSource, Digest, Reading, Reload, ReloadResult};
use icetray::serve::{OVERALL_SERVICE, PeriodicTask};
use icetray::telemetry::{self, Call, CodeLabel};
use icetray::{ServeError, ServeOptions, Serving};
use icetray_core::approx;
use icetray_core::config::{
    Config, LocalConfig, Residency, ServerConfig, SourceConfig, TableConfig, ValueFormat,
};
use icetray_core::key::UUID_BYTES;
use icetray_core::metric::{self, CacheName, Kind, LookupResult, ScanResult, Temperature};
use icetray_core::{
    ColumnRead, ErrorKind, Generation, IoStats, Key, KeyType, MAX_FIELD_NUMBER, Row, Storage,
    TableState,
};
use icetray_proto::v1::icetray_client::IcetrayClient;
use icetray_proto::v1::icetray_server::SERVICE_NAME;
use icetray_proto::v1::{
    self, GetRequest, GetResponse, MultiGetRequest, MultiGetResponse, MultiScanRequest,
    MultiScanResponse, ScanRequest, ScanResponse, TableInfoRequest, TableInfoResponse,
};
use icetray_testkit::fixture::{
    NAMESPACE, ONE_BUCKET, SAMPLE, SECOND_VALUE_COLUMN, SMALL_BUCKETS, SMALL_IDS,
};
use icetray_testkit::protobuf;
use icetray_testkit::verify::{expected_cold_gets, series_sum, series_value};
use icetray_testkit::{
    DeleteForm, Dictionary, Fixture, FixtureSpec, Layout, MemoryCatalog, Nulls, OrderKind, Ordered,
    S3Fixture, ScanRows, TestScan, ValueKind, memory_catalog_s3, write_fixture,
};
use prost::Message;
use tokio::task::JoinHandle;
use tonic::transport::Channel;
use tonic::{Code, Status};
use tonic_health::pb::HealthCheckRequest;
use tonic_health::pb::health_check_response::ServingStatus;
use tonic_health::pb::health_client::HealthClient;

mod common;

use common::{
    BoxError, NULL_EVERY, NULL_ID, PRESENT_ID, SHUTDOWN_WAIT, health_status, http_get, poll_until,
};
use icetray_testkit::layout::VALUE_COLUMN;

/// What every test returns.
type TestResult = Result<(), BoxError>;

/// The fixture server's bucket and the warehouse prefix inside it.
const BUCKET: &str = "icetray-serve";
const WAREHOUSE: &str = "warehouse";

/// The name of the served table in the one-table tests.
const TABLE: &str = "kv";

/// The name of the uuid-keyed table beside `TABLE`.
const UUID_TABLE: &str = "kv_uuid";

/// A name no table is served under.
const NOBODY: &str = "nobody";

/// A table no other test serves, so its counters read this test's alone.
const FRESH_TABLE: &str = "kv_fresh";

/// A table no other test serves, so its gauges are this test's alone.
const SCRAPED_TABLE: &str = "kv_scraped";

/// An ordered table no other test serves, so its gauges are this test's
/// alone.
const SCRAPED_ORDERED_TABLE: &str = "events_scraped";

/// A disk table no other test serves, so its page-cache gauges are this
/// test's alone.
const SAMPLED_TABLE: &str = "kv_sampled";

/// How long the page-cache test waits for the first sample: the start's
/// sample runs as the tables serve, so this is room for a loaded machine.
const FIRST_SAMPLE_WAIT: Duration = Duration::from_secs(10);

/// The name of the ordered table beside `TABLE`.
const ORDERED_TABLE: &str = "events";

/// An ordered table no other test serves, so its scan counters read this
/// test's alone.
const FRESH_ORDERED_TABLE: &str = "events_fresh";

/// The ids an ordered fixture of these tests writes: few, so each test
/// writes it quickly.
const ORDERED_IDS: std::ops::Range<u64> = 0..32;

/// The rows every key of an ordered fixture of these tests has.
const ROWS_PER_KEY: usize = 7;

/// The raw order value of a key's first row, and the distance between
/// consecutive rows.
const ORDER_START: i64 = 1_000;
const ORDER_STEP: i64 = 10;

/// The rows a Scan page of the paging test asks for: fewer than a key has.
const PAGE_ROWS: u32 = 3;

/// The `max_scan_rows` the limit tests configure: fewer than a key's rows,
/// so a limit of it leaves rows behind.
const SCAN_CAP: u32 = 5;

/// How many keys the refused-before-any-read test asks a `MultiScan` for
/// beside the one that fits: the product with the limit and the columns
/// passes `max_multi_scan_values`.
const SCAN_KEYS_OVER: usize = 3;

/// A column no table of the tests serves.
const UNSERVED_COLUMN: &str = "unserved";

/// The `max_multi_scan_values` the `MultiScan` limit test configures: two
/// keys at `SCAN_CAP` rows and one value column, each row counting its
/// order value and its column.
const MULTI_SCAN_VALUES: usize = 2 * SCAN_CAP as usize * 2;

/// The `max_scan_rows` of a table that sets none: the default, which
/// bounds every Scan the tests ask in full.
const SCAN_CAP_DEFAULT: u32 = icetray_core::config::DEFAULT_MAX_SCAN_ROWS;

/// A direction number the contract does not name.
const UNKNOWN_DIRECTION: i32 = 99;

/// The `scans_total` keys of `MultiScan` the counting test reads, and the
/// `Scan` before them.
const COUNTED_SCAN_KEYS: usize = 2;

/// A table served with deletes beside `TABLE`.
const DELETED_TABLE: &str = "kv_deleted";

/// Loopback with an OS-assigned port, for both listeners.
const LOOPBACK_ANY_PORT: SocketAddr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0);

/// The poll interval the served tables refresh at: short, so a refresh
/// test finishes in well under a second.
const TEST_POLL_INTERVAL: Duration = Duration::from_millis(200);

/// How many polls a refresh test waits for the table to advance.
const REFRESH_POLLS: u32 = 10;

/// How long a refresh test waits for the table to advance.
const REFRESH_WAIT: Duration = TEST_POLL_INTERVAL.saturating_mul(REFRESH_POLLS);

/// How long the first-load test holds that the failed run stays up once
/// the failure is confirmed.
const STAYS_UP_FOR: Duration = Duration::from_millis(100);

/// How many `STAYS_UP_FOR` intervals the failed first load's grace spans
/// in the test: the one checked, and the rest as room for the poll that
/// confirms the failure and for a loaded machine.
const GRACE_INTERVALS: u32 = 5;

/// The grace a failed first load stays on the health service in the test.
const FAILURE_GRACE: Duration = STAYS_UP_FOR.saturating_mul(GRACE_INTERVALS);

/// How many `SAMPLE` ids the cold-cost test looks up; they share no page,
/// so the oracle prices them at one GET each.
const COLD_IDS: usize = 3;

/// How many `SAMPLE` ids the batch tests send beside the absent key.
const BATCH_IDS: usize = 4;

/// How many `Get`s the counting test issues.
const COUNTED_GETS: u64 = 3;

/// How many ids the refresh test appends.
const APPENDED_IDS: u64 = 16;

/// The ids the refresh test appends: above every id the small fixture
/// wrote, so the new files overlap nothing.
const NEW_IDS: std::ops::Range<u64> = SMALL_IDS.end..SMALL_IDS.end + APPENDED_IDS;

/// The file prefix of the refresh test's append.
const APPEND_PREFIX: &str = "build2";

/// The scheme every S3 location under the fixture bucket carries.
const S3_SCHEME: &str = "s3://";

/// The `Get` deadline the deadline tests configure: short, so a call over a
/// held store ends quickly.
const SHORT_DEADLINE: Duration = Duration::from_millis(300);

/// The `Get` deadline of the test that sets its `MultiGet` deadline to
/// `SHORT_DEADLINE`: longer, since a page fetch ends at the `Get` deadline,
/// so a `MultiGet` over a held store is seen to end at its own only when
/// its own comes first.
const LONGER_DEADLINE: Duration = SHORT_DEADLINE.saturating_mul(2);

/// A client deadline shorter than every deadline the server is configured
/// with in the test that sends it.
const CLIENT_DEADLINE: Duration = Duration::from_millis(200);

/// How long past its deadline a call may take to be answered: the timer's
/// wake-up and one loopback round trip, with room for a loaded machine.
const DEADLINE_MARGIN: Duration = Duration::from_millis(500);

/// The in-flight cap the admission tests configure: two, so one `Get` and
/// one `MultiGet` fill it.
const IN_FLIGHT_CAP: usize = 2;

/// How soon a refused call is answered: one loopback round trip, far
/// inside `SHORT_DEADLINE`, so the refusal is seen not to wait for a slot.
const REFUSED_WITHIN: Duration = Duration::from_millis(100);

/// How soon a shutdown during the first loads ends the run: the drain of a
/// daemon that never served, with room for a loaded machine, and far inside
/// the refresh deadline the held load would otherwise wait out.
const FIRST_LOAD_SHUTDOWN_WITHIN: Duration = Duration::from_secs(1);

/// The drain deadline the overrun test configures: short, and far inside
/// the default `get_deadline`, so the held call outlasts the drain.
const SHORT_DRAIN: Duration = Duration::from_millis(300);

/// The `metrics` recorder is process-global, so `requests_total` is one
/// counter for every test in this binary. Every test that issues an RPC
/// holds this lock, so the counting test's deltas are exact.
static SERIAL: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

/// A health `Watch` being collected: every status the stream sent, once
/// the server has ended it.
type Watching = JoinHandle<Result<Vec<ServingStatus>, BoxError>>;

/// A running daemon over fixtures in the in-process S3 server, with the
/// clients to drive it, the catalog the fixtures were written through, and
/// the storage counters to price it.
struct Stack {
    s3: S3Fixture,
    catalog: Arc<MemoryCatalog>,
    fixtures: Vec<Fixture>,
    serving: Serving,
    client: IcetrayClient<Channel>,
    health: HealthClient<Channel>,
    stats: Arc<IoStats>,
    /// The local directory `Setup::with_local_dir` made, kept for as long
    /// as the daemon serves from it.
    _local: Option<tempfile::TempDir>,
}

/// What `Stack::start_with` hands a test before the daemon starts: the
/// written fixtures, the configuration serving each from its pinned
/// metadata file at `Residency::Full` and not strict, and the default
/// options. The configuration is built directly, so nothing validates it
/// before the load does, and a test reaches what `Config::parse` refuses.
struct Setup {
    fixtures: Vec<Fixture>,
    config: Config,
    options: ServeOptions,
    /// Holds the in-process store before the daemon starts, so the first
    /// loads wait on it until it is released.
    hold_store: bool,
    /// The local directory the configuration names, when a test gave it
    /// one.
    local: Option<tempfile::TempDir>,
}

impl Setup {
    /// Gives the configuration a fresh local directory with no page cache
    /// disk level and serves every table from it in the Disk tier.
    fn with_local_dir(&mut self) -> Result<(), BoxError> {
        let dir = tempfile::tempdir()?;
        self.config.storage.local = Some(LocalConfig {
            dir: dir.path().to_path_buf(),
            index_budget_bytes: None,
            page_cache_disk_bytes: 0,
        });
        for table in &mut self.config.tables {
            table.residency = Residency::Disk;
        }
        self.local = Some(dir);
        Ok(())
    }
}

/// A configuration over `s3` serving `tables` with both listeners on
/// loopback at OS-assigned ports and the test poll interval.
fn config_over(s3: &S3Fixture, tables: Vec<TableConfig>) -> Config {
    Config {
        server: {
            let mut server = ServerConfig::default();
            server.listen = LOOPBACK_ANY_PORT;
            server.metrics_listen = LOOPBACK_ANY_PORT;
            server.poll_interval = TEST_POLL_INTERVAL;
            server
        },
        storage: s3.storage_config(),
        tables,
    }
}

impl Stack {
    /// Writes one fixture per spec and serves each from its pinned metadata
    /// file, not yet loaded.
    async fn start(specs: Vec<FixtureSpec>) -> Result<Self, BoxError> {
        Self::start_with(specs, async |_| Ok(())).await
    }

    /// `start`, with `configure` run over the `Setup` between the fixture
    /// writes and the daemon's start; the loads run behind the returned
    /// stack.
    async fn start_with(
        specs: Vec<FixtureSpec>,
        configure: impl AsyncFnOnce(&mut Setup) -> Result<(), BoxError>,
    ) -> Result<Self, BoxError> {
        let s3 = S3Fixture::start(BUCKET).await?;
        let catalog = memory_catalog_s3(&s3, WAREHOUSE).await?;
        let mut fixtures = Vec::with_capacity(specs.len());
        for spec in specs {
            fixtures.push(write_fixture(catalog.as_ref(), NAMESPACE, spec).await?);
        }
        let tables = fixtures
            .iter()
            .map(|fixture| fixture.config(Residency::Full, false))
            .collect();
        let mut setup = Setup {
            fixtures,
            config: config_over(&s3, tables),
            options: ServeOptions::default(),
            hold_store: false,
            local: None,
        };
        configure(&mut setup).await?;
        let Setup {
            fixtures,
            config,
            options,
            hold_store,
            local,
        } = setup;
        if hold_store {
            s3.hold();
        }
        let recorder = telemetry::install()?;
        let storage = Arc::new(Storage::new(s3.storage_config()));
        let stats = storage.stats();
        let serving = Serving::start_over(config, storage, recorder, options).await?;
        let channel = Channel::from_shared(format!("http://{}", serving.listen_addr()))?
            .connect()
            .await?;
        Ok(Self {
            s3,
            catalog,
            fixtures,
            serving,
            client: IcetrayClient::new(channel.clone()),
            health: HealthClient::new(channel),
            stats,
            _local: local,
        })
    }

    /// `start` with one small long-keyed fixture named `TABLE`, loaded.
    async fn ready_small() -> Result<Self, BoxError> {
        let mut stack = Self::start(vec![FixtureSpec::small(TABLE, KeyType::Long)]).await?;
        stack.serving.ready().await;
        Ok(stack)
    }

    /// The health status of `service`.
    async fn health_status(&mut self, service: &str) -> Result<ServingStatus, BoxError> {
        health_status(&mut self.health, service).await
    }

    /// Opens a `Watch` of `service` and collects, in a task, every status
    /// it sends until the server ends the stream. The watch is registered
    /// with the health service when this returns.
    async fn watch_health(&mut self, service: &str) -> Result<Watching, BoxError> {
        let mut stream = self
            .health
            .watch(HealthCheckRequest {
                service: service.to_string(),
            })
            .await?
            .into_inner();
        Ok(tokio::spawn(async move {
            let mut statuses = Vec::new();
            while let Some(response) = stream.message().await? {
                statuses.push(ServingStatus::try_from(response.status)?);
            }
            Ok(statuses)
        }))
    }

    /// One `Get` of `key` on `table` naming `VALUE_COLUMN`.
    async fn get(&mut self, table: &str, key: Option<v1::Key>) -> Result<GetResponse, Status> {
        self.get_naming(table, key, &[VALUE_COLUMN]).await
    }

    /// One `Get` of `key` on `table` naming `columns`, each read whole.
    async fn get_naming(
        &mut self,
        table: &str,
        key: Option<v1::Key>,
        columns: &[&str],
    ) -> Result<GetResponse, Status> {
        self.get_reading(table, key, wire_columns(columns)).await
    }

    /// One `Get` of `key` on `table` with the column reads `columns`.
    async fn get_reading(
        &mut self,
        table: &str,
        key: Option<v1::Key>,
        columns: Vec<v1::ColumnRead>,
    ) -> Result<GetResponse, Status> {
        self.client
            .get(GetRequest {
                table: table.to_string(),
                key,
                columns,
            })
            .await
            .map(tonic::Response::into_inner)
    }

    /// One `MultiGet` of `keys` on `table` naming `VALUE_COLUMN`.
    async fn multi_get(&mut self, table: &str, keys: &[Key]) -> Result<MultiGetResponse, Status> {
        self.multi_get_naming(table, keys, &[VALUE_COLUMN]).await
    }

    /// One `MultiGet` of `keys` on `table` naming `columns`, each read
    /// whole.
    async fn multi_get_naming(
        &mut self,
        table: &str,
        keys: &[Key],
        columns: &[&str],
    ) -> Result<MultiGetResponse, Status> {
        self.multi_get_reading(table, keys, wire_columns(columns))
            .await
    }

    /// One `MultiGet` of `keys` on `table` with the column reads `columns`.
    async fn multi_get_reading(
        &mut self,
        table: &str,
        keys: &[Key],
        columns: Vec<v1::ColumnRead>,
    ) -> Result<MultiGetResponse, Status> {
        self.client
            .multi_get(MultiGetRequest {
                table: table.to_string(),
                keys: keys.iter().map(wire_key).collect(),
                columns,
            })
            .await
            .map(tonic::Response::into_inner)
    }

    /// One `Scan`.
    async fn scan(&mut self, request: ScanRequest) -> Result<ScanResponse, Status> {
        self.client
            .scan(request)
            .await
            .map(tonic::Response::into_inner)
    }

    /// One `MultiScan`.
    async fn multi_scan(&mut self, request: MultiScanRequest) -> Result<MultiScanResponse, Status> {
        self.client
            .multi_scan(request)
            .await
            .map(tonic::Response::into_inner)
    }

    /// `start` with the ordered table `ORDERED_TABLE` and a key-value table
    /// `TABLE` beside it, `configure` run over the setup, loaded.
    async fn ready_ordered_with(
        configure: impl AsyncFnOnce(&mut Setup) -> Result<(), BoxError>,
    ) -> Result<Self, BoxError> {
        let mut stack = Self::start_with(
            vec![
                FixtureSpec::small(TABLE, KeyType::Long),
                ordered_spec(ORDERED_TABLE),
            ],
            configure,
        )
        .await?;
        stack.serving.ready().await;
        Ok(stack)
    }

    /// The generation the table `name` holds.
    fn generation_of(&self, name: &str) -> Arc<Generation> {
        self.serving
            .opened()
            .tables()
            .get(name)
            .expect("the table is served")
            .current()
            .expect("the table was loaded")
    }

    /// The ordered fixture `ready_ordered_with` serves.
    fn ordered(&self) -> &Fixture {
        &self.fixtures[1]
    }

    /// One `TableInfo` of `table`.
    async fn table_info(&mut self, table: &str) -> Result<TableInfoResponse, Status> {
        self.client
            .table_info(TableInfoRequest {
                table: table.to_string(),
            })
            .await
            .map(tonic::Response::into_inner)
    }

    /// The `max_scan_rows` `TableInfo` reports for `table`: the largest
    /// limit a Scan of it, or each key of a `MultiScan`, takes.
    async fn max_scan_rows(&mut self, table: &str) -> Result<u32, Status> {
        Ok(self.table_info(table).await?.max_scan_rows)
    }

    /// The generation the first served table holds.
    fn generation(&self) -> Arc<Generation> {
        self.serving.opened().tables()[0]
            .current()
            .expect("the table was loaded")
    }

    /// `GET path` on the metrics listener.
    async fn http_get(&self, path: &str) -> Result<(StatusCode, String), BoxError> {
        http_get(self.serving.metrics_addr(), path).await
    }

    /// One scrape of `/metrics`, which answers 200.
    async fn scrape(&self) -> Result<String, BoxError> {
        let (status, exposition) = self.http_get(METRICS_PATH).await?;
        assert_eq!(status, StatusCode::OK);
        Ok(exposition)
    }
}

/// `columns` as a request carries them.
fn wire_columns(columns: &[&str]) -> Vec<v1::ColumnRead> {
    columns
        .iter()
        .map(|&column| v1::ColumnRead {
            name: column.to_string(),
            fields: Vec::new(),
        })
        .collect()
}

/// The bytes of the one value `values` carries, `None` for a null.
///
/// Panics on any number of values but one, or a value with no variant.
fn one_wire_value(values: &[v1::ColumnValue]) -> Option<Vec<u8>> {
    let [value] = values else {
        panic!("one value, got {values:?}");
    };
    wire_value(value)
}

/// The bytes `value` carries, `None` for a null.
///
/// Panics on a value with no variant.
fn wire_value(value: &v1::ColumnValue) -> Option<Vec<u8>> {
    match &value.kind {
        Some(v1::column_value::Kind::Value(bytes)) => Some(bytes.to_vec()),
        Some(v1::column_value::Kind::NullValue(_)) => None,
        None => panic!("a value with no variant"),
    }
}

/// `key` as the wire carries it.
fn wire_key(key: &Key) -> v1::Key {
    let variant = match key {
        Key::Long(value) => v1::key::Key::Long(*value),
        Key::Str(text) => v1::key::Key::Str(text.clone()),
        Key::Bytes(bytes) => v1::key::Key::Bin(bytes.clone()),
        Key::Uuid(bytes) => v1::key::Key::Bin(bytes.to_vec()),
    };
    v1::Key { key: Some(variant) }
}

/// An ordered table named `name` of `ORDERED_IDS` keys, each with
/// `ROWS_PER_KEY` rows of a long order column.
fn ordered_spec(name: &str) -> FixtureSpec {
    FixtureSpec {
        ids: ORDERED_IDS,
        buckets: ONE_BUCKET,
        ..FixtureSpec::ordered(name, KeyType::Long, ordered())
    }
}

/// The order column of `ordered_spec`: `ROWS_PER_KEY` long order values
/// per key, `ORDER_STEP` apart from `ORDER_START`.
fn ordered() -> Ordered {
    let rows = u64::try_from(ROWS_PER_KEY).expect("a row count fits u64");
    Ordered::each(OrderKind::Long, rows, ORDER_START, ORDER_STEP)
}

/// The raw order value of a key's `index`-th row, oldest first.
fn order_at(index: u64) -> i64 {
    ordered()
        .order_at(index)
        .expect("a row index's order value fits a long")
}

/// A raw long order value as the wire carries it.
fn wire_order(raw: i64) -> v1::OrderValue {
    v1::OrderValue {
        value: Some(v1::order_value::Value::Long(raw)),
    }
}

/// A Scan of `key` on `table` reading `VALUE_COLUMN` newest first, at most
/// `limit` rows, unbounded: the request a test changes the one field it is
/// about in. A test reading every row a Scan may return passes the
/// `max_scan_rows` `TableInfo` reports (`Stack::max_scan_rows`).
fn scan_of(table: &str, key: &Key, limit: u32) -> ScanRequest {
    ScanRequest {
        table: table.to_string(),
        key: Some(wire_key(key)),
        columns: wire_columns(&[VALUE_COLUMN]),
        direction: i32::from(v1::Direction::NewestFirst),
        limit,
        start: None,
        before: None,
    }
}

/// A `MultiScan` of `keys` on `table` as `scan_of` reads one.
fn multi_scan_of(table: &str, keys: &[Key], limit: u32) -> MultiScanRequest {
    let one = scan_of(table, &keys[0], limit);
    MultiScanRequest {
        table: one.table,
        keys: keys.iter().map(wire_key).collect(),
        columns: one.columns,
        direction: one.direction,
        limit: one.limit,
        start: None,
        before: None,
    }
}

/// The rows a Scan answered as the fixture model states them: each raw
/// order value and its one value.
///
/// Panics on an order value that is not a long.
fn scan_rows_of(rows: &[v1::ScanRow]) -> ScanRows {
    rows.iter()
        .map(|row| {
            let Some(v1::order_value::Value::Long(order)) =
                row.order.as_ref().and_then(|order| order.value.as_ref())
            else {
                panic!("a long order value: {row:?}");
            };
            (*order, vec![one_wire_value(&row.values)])
        })
        .collect()
}

/// What the model answers for a Scan of `key` in `fixture` under `scan`,
/// reading `VALUE_COLUMN`.
fn expected(fixture: &Fixture, key: &Key, scan: &TestScan) -> (ScanRows, Option<i64>) {
    fixture.expected_scan(key, &[VALUE_COLUMN], scan)
}

/// The model's request of every row, newest first, at a limit of
/// `max_scan_rows`, the table's.
fn every_row(max_scan_rows: u32) -> TestScan {
    TestScan {
        limit: max_scan_rows,
        ..TestScan::default()
    }
}

/// The `next` the model says a Scan under `scan` answers after a page
/// whose last row is at raw order value `last`, as the wire carries it: the
/// order value newest first and one above it oldest first
/// (`TestScan::next_after`), in the fixtures' long order column.
///
/// Panics on a next that is not a long.
fn model_next(scan: &TestScan, last: Option<i64>) -> Option<v1::OrderValue> {
    last.and_then(|last| scan.next_after(OrderKind::Long, last))
        .map(|next| match next {
            icetray_core::OrderValue::Long(next) => wire_order(next),
            other @ (icetray_core::OrderValue::Str(_) | icetray_core::OrderValue::Bytes(_)) => {
                panic!("a long next: {other:?}")
            }
        })
}

/// The `requests_total` count of `call` answering `code` in `exposition`:
/// zero before the first such answer, when the series is not yet exposed.
fn calls(exposition: &str, call: Call, code: Code) -> f64 {
    series_value(
        exposition,
        metric::REQUESTS_TOTAL,
        &[
            (metric::LABEL_CALL, &call.to_string()),
            (metric::LABEL_CODE, &CodeLabel(code).to_string()),
        ],
    )
    .unwrap_or(0.0)
}

/// Asserts that every metric `exposition` types has a `# HELP` line, and
/// that it types some.
fn assert_every_metric_described(exposition: &str) {
    let named = |prefix: &str| -> Vec<String> {
        exposition
            .lines()
            .filter_map(|line| line.strip_prefix(prefix)?.split_whitespace().next())
            .map(str::to_string)
            .collect()
    };
    let typed = named("# TYPE ");
    let described = named("# HELP ");
    assert!(!typed.is_empty(), "{exposition}");
    for name in &typed {
        assert!(
            described.contains(name),
            "{name} has no HELP in {exposition}"
        );
    }
}

/// Whether `exposition` exposes the `requests_total` series of `call`
/// answering `code`.
fn calls_exposed(exposition: &str, call: Call, code: Code) -> bool {
    series_value(
        exposition,
        metric::REQUESTS_TOTAL,
        &[
            (metric::LABEL_CALL, &call.to_string()),
            (metric::LABEL_CODE, &CodeLabel(code).to_string()),
        ],
    )
    .is_some()
}

/// Asserts that `after` counts `expected` more `call`s answering `code`
/// than `before`. A counter's integer is exact in the float the exposition
/// carries it as, so the difference compares exactly.
fn assert_counted(before: &str, after: &str, call: Call, code: Code, expected: u64) {
    let added = calls(after, call, code) - calls(before, call, code);
    assert_eq!(
        added.to_bits(),
        approx::gauge(expected).to_bits(),
        "{call} {code:?}: {added} counted, {expected} expected"
    );
}

/// The value of the unlabeled series `name` in `exposition`, which must
/// expose it.
fn gauge(exposition: &str, name: &str) -> f64 {
    series_value(exposition, name, &[]).unwrap_or_else(|| panic!("{name} in {exposition}"))
}

/// Asserts that `exposition` reads `admitted` calls in flight of `limit`
/// slots.
fn assert_in_flight(exposition: &str, admitted: usize, limit: usize) {
    for (name, expected) in [
        (metric::REQUESTS_IN_FLIGHT, admitted),
        (metric::MAX_IN_FLIGHT, limit),
    ] {
        assert_eq!(
            gauge(exposition, name).to_bits(),
            approx::gauge_len(expected).to_bits(),
            "{name}"
        );
    }
}

/// `call`'s outcome beside how long it took.
async fn timed<T>(call: impl Future<Output = T>) -> (T, Duration) {
    let started = tokio::time::Instant::now();
    let outcome = call.await;
    (outcome, started.elapsed())
}

/// Asserts that a call answered `code` no sooner than `deadline` and within
/// `DEADLINE_MARGIN` of it.
fn assert_ended_at<T: std::fmt::Debug>(
    outcome: Result<T, Status>,
    elapsed: Duration,
    deadline: Duration,
    code: Code,
) {
    assert_eq!(code_of(outcome), code);
    assert!(
        elapsed >= deadline && elapsed < deadline + DEADLINE_MARGIN,
        "answered after {elapsed:?}, deadline {deadline:?}"
    );
}

/// The code of a failed call.
fn code_of<T: std::fmt::Debug>(result: Result<T, Status>) -> Code {
    result.expect_err("the call fails").code()
}

/// The local file of an object under the fixture server's root.
fn object_file(s3: &S3Fixture, location: &str) -> PathBuf {
    let under_bucket = location
        .strip_prefix(S3_SCHEME)
        .expect("fixture objects live under s3://");
    s3.root().join(under_bucket)
}

#[tokio::test(flavor = "multi_thread")]
async fn test_health_is_not_serving_until_every_table_serves() -> TestResult {
    let _serial = SERIAL.lock().await;
    let mut stack = Stack::start_with(
        vec![FixtureSpec::small(TABLE, KeyType::Long)],
        async |setup| {
            setup.hold_store = true;
            Ok(())
        },
    )
    .await?;
    assert_eq!(
        stack.health_status(OVERALL_SERVICE).await?,
        ServingStatus::NotServing,
        "the listener accepts before the load completes"
    );
    let (status, body) = stack.http_get(READY_PATH).await?;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(body, NOT_READY_BODY);

    stack.s3.release();
    stack.serving.ready().await;
    assert_eq!(
        stack.health_status(OVERALL_SERVICE).await?,
        ServingStatus::Serving
    );
    assert_eq!(
        stack.health_status(SERVICE_NAME).await?,
        ServingStatus::Serving
    );
    let (status, body) = stack.http_get(READY_PATH).await?;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body, READY_BODY);
    let (status, _) = stack.http_get("/elsewhere").await?;
    assert_eq!(status, StatusCode::NOT_FOUND);
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_table_info_describes_the_served_snapshot() -> TestResult {
    let _serial = SERIAL.lock().await;
    let mut stack = Stack::ready_small().await?;
    let generation = stack.generation();
    let info = stack.table_info(TABLE).await?;
    assert_eq!(info.table, TABLE);
    assert_eq!(info.buckets, SMALL_BUCKETS);
    assert_eq!(info.data_files, u64::from(SMALL_BUCKETS));
    assert_eq!(info.records, SMALL_IDS.end - SMALL_IDS.start);
    assert_eq!(info.key_type, i32::from(v1::KeyType::Long));
    assert_eq!(info.snapshot_id, generation.snapshot_id.0);
    assert_eq!(info.index_bytes, approx::count(generation.index_bytes));
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_get_costs_one_get_cold_and_none_warm_or_absent() -> TestResult {
    let _serial = SERIAL.lock().await;
    let mut stack = Stack::ready_small().await?;
    let generation = stack.generation();
    let keys = stack.fixtures[0].keys_at(&SAMPLE[..COLD_IDS]);
    let expected = expected_cold_gets(&generation, &keys, &[VALUE_COLUMN])?;
    assert_eq!(expected, approx::count(COLD_IDS), "the ids share no page");

    let before = stack.stats.gets();
    for key in &keys {
        let response = stack.get(TABLE, Some(wire_key(key))).await?;
        assert_eq!(response.presence, i32::from(v1::Presence::Present), "{key}");
        assert_eq!(
            one_wire_value(&response.values),
            stack.fixtures[0].value(key),
            "{key}"
        );
        assert_eq!(response.snapshot_id, generation.snapshot_id.0);
    }
    assert_eq!(
        stack.stats.gets() - before,
        expected,
        "one GET per cold hit"
    );

    let before = stack.stats.gets();
    for key in &keys {
        let response = stack.get(TABLE, Some(wire_key(key))).await?;
        assert_eq!(response.presence, i32::from(v1::Presence::Present));
    }
    assert_eq!(stack.stats.gets(), before, "a warm hit costs nothing");

    let absent = stack.fixtures[0].absent_near(SAMPLE[0]);
    let before = stack.stats.gets();
    let response = stack.get(TABLE, Some(wire_key(&absent))).await?;
    assert_eq!(response.presence, i32::from(v1::Presence::Absent));
    assert!(response.values.is_empty());
    assert_eq!(response.snapshot_id, generation.snapshot_id.0);
    assert_eq!(stack.stats.gets(), before, "a miss costs nothing");
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_get_serves_a_null_value_as_present_with_null() -> TestResult {
    let _serial = SERIAL.lock().await;
    let spec = FixtureSpec {
        nulls: Nulls::EveryNth(NULL_EVERY),
        ..FixtureSpec::small(TABLE, KeyType::Long)
    };
    let mut stack = Stack::start(vec![spec]).await?;
    stack.serving.ready().await;
    let null_key = stack.fixtures[0].key(NULL_ID);
    assert_eq!(stack.fixtures[0].value(&null_key), None);
    let present_key = stack.fixtures[0].key(PRESENT_ID);
    assert!(stack.fixtures[0].value(&present_key).is_some());

    let before = stack.stats.gets();
    let null = stack.get(TABLE, Some(wire_key(&null_key))).await?;
    assert_eq!(null.presence, i32::from(v1::Presence::Present));
    assert_eq!(one_wire_value(&null.values), None, "the value is null");
    assert_eq!(stack.stats.gets() - before, 1, "a null costs its page cold");

    let present = stack.get(TABLE, Some(wire_key(&present_key))).await?;
    assert_eq!(present.presence, i32::from(v1::Presence::Present));
    assert_eq!(
        one_wire_value(&present.values),
        stack.fixtures[0].value(&present_key)
    );

    let absent = stack
        .get(
            TABLE,
            Some(wire_key(&stack.fixtures[0].absent_near(NULL_ID))),
        )
        .await?;
    assert_eq!(absent.presence, i32::from(v1::Presence::Absent));
    assert!(absent.values.is_empty());
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_multi_get_answers_in_order_from_one_snapshot() -> TestResult {
    let _serial = SERIAL.lock().await;
    let mut stack = Stack::ready_small().await?;
    let generation = stack.generation();
    let present = stack.fixtures[0].keys_at(&SAMPLE[..BATCH_IDS]);
    let absent = stack.fixtures[0].absent_near(SAMPLE[0]);
    let mut keys = present.clone();
    keys.push(absent);
    let expected = expected_cold_gets(&generation, &present, &[VALUE_COLUMN])?;

    let before = stack.stats.gets();
    let response = stack.multi_get(TABLE, &keys).await?;
    assert_eq!(stack.stats.gets() - before, expected);
    assert_eq!(response.snapshot_id, generation.snapshot_id.0);
    assert_eq!(response.results.len(), keys.len());
    for (key, hit) in present.iter().zip(&response.results) {
        assert_eq!(hit.presence, i32::from(v1::Presence::Present), "{key}");
        assert_eq!(
            one_wire_value(&hit.values),
            stack.fixtures[0].value(key),
            "{key}"
        );
    }
    let last = &response.results[keys.len() - 1];
    assert_eq!(last.presence, i32::from(v1::Presence::Absent));
    assert!(last.values.is_empty());
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_get_maps_a_missing_or_mismatched_key_to_invalid_argument() -> TestResult {
    let _serial = SERIAL.lock().await;
    let mut stack = Stack::start(vec![
        FixtureSpec::small(TABLE, KeyType::Long),
        FixtureSpec::small(UUID_TABLE, KeyType::Uuid),
    ])
    .await?;
    stack.serving.ready().await;

    let scraped = stack.scrape().await?;
    let before = stack.stats.gets();
    assert_eq!(code_of(stack.get(TABLE, None).await), Code::InvalidArgument);
    assert_eq!(
        code_of(stack.get(TABLE, Some(v1::Key { key: None })).await),
        Code::InvalidArgument
    );
    let str_key = v1::Key {
        key: Some(v1::key::Key::Str("nope".to_string())),
    };
    assert_eq!(
        code_of(stack.get(TABLE, Some(str_key)).await),
        Code::InvalidArgument
    );
    let short = v1::Key {
        key: Some(v1::key::Key::Bin(vec![0u8; UUID_BYTES - 1])),
    };
    assert_eq!(
        code_of(stack.get(UUID_TABLE, Some(short)).await),
        Code::InvalidArgument
    );
    assert_eq!(stack.stats.gets(), before, "a refused key issues no GET");
    let refused_keys = 4;
    assert_counted(
        &scraped,
        &stack.scrape().await?,
        Call::Get,
        Code::InvalidArgument,
        refused_keys,
    );

    let uuid_key = stack.fixtures[1].key(SAMPLE[1]);
    let Key::Uuid(bytes) = &uuid_key else {
        panic!("{uuid_key} is a uuid");
    };
    assert_eq!(bytes.len(), UUID_BYTES);
    let response = stack.get(UUID_TABLE, Some(wire_key(&uuid_key))).await?;
    assert_eq!(response.presence, i32::from(v1::Presence::Present));
    assert_eq!(
        one_wire_value(&response.values),
        stack.fixtures[1].value(&uuid_key)
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_unknown_table_is_not_found() -> TestResult {
    let _serial = SERIAL.lock().await;
    let mut stack = Stack::ready_small().await?;
    let key = stack.fixtures[0].key(SAMPLE[0]);
    let before = stack.scrape().await?;
    assert_eq!(code_of(stack.table_info(NOBODY).await), Code::NotFound);
    assert_eq!(
        code_of(stack.get(NOBODY, Some(wire_key(&key))).await),
        Code::NotFound
    );
    assert_eq!(
        code_of(stack.multi_get(NOBODY, std::slice::from_ref(&key)).await),
        Code::NotFound
    );
    let after = stack.scrape().await?;
    for call in [Call::Get, Call::MultiGet, Call::TableInfo] {
        assert_counted(&before, &after, call, Code::NotFound, 1);
        assert_counted(&before, &after, call, Code::Ok, 0);
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_storage_failure_is_unavailable_and_fails_a_batch_whole() -> TestResult {
    let _serial = SERIAL.lock().await;
    let mut stack = Stack::ready_small().await?;
    let generation = stack.generation();
    let sample = stack.fixtures[0].keys_at(&SAMPLE);
    let plan = generation.plan(&sample);
    let (doomed, _) = plan[0].as_ref().expect("a fixture key has a value page");
    let doomed_path = doomed.entry.path.clone();
    let doomed_key = sample[0].clone();
    let others: Vec<Key> = sample
        .iter()
        .zip(&plan)
        .filter_map(|(key, planned)| {
            let (file, _) = planned.as_ref()?;
            (file.entry.path != doomed_path).then(|| key.clone())
        })
        .take(BATCH_IDS)
        .collect();
    assert_eq!(others.len(), BATCH_IDS, "the sample spans several files");
    std::fs::remove_file(object_file(&stack.s3, &doomed_path))?;

    let before = stack.scrape().await?;
    assert_eq!(
        code_of(stack.get(TABLE, Some(wire_key(&doomed_key))).await),
        Code::Unavailable
    );
    let mut mixed = others.clone();
    mixed.push(doomed_key);
    assert_eq!(
        code_of(stack.multi_get(TABLE, &mixed).await),
        Code::Unavailable,
        "one failed read fails the batch"
    );
    let after = stack.scrape().await?;
    assert_counted(&before, &after, Call::Get, Code::Unavailable, 1);
    assert_counted(&before, &after, Call::MultiGet, Code::Unavailable, 1);
    let answered = stack.multi_get(TABLE, &others).await?;
    assert_eq!(answered.results.len(), others.len());
    for (key, hit) in others.iter().zip(&answered.results) {
        assert_eq!(hit.presence, i32::from(v1::Presence::Present), "{key}");
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_requests_total_counts_every_call() -> TestResult {
    let _serial = SERIAL.lock().await;
    let mut stack = Stack::ready_ordered_with(async |_| Ok(())).await?;
    let before = stack.scrape().await?;

    let key = stack.fixtures[0].key(SAMPLE[0]);
    for _ in 0..COUNTED_GETS {
        stack.get(TABLE, Some(wire_key(&key))).await?;
    }
    stack.multi_get(TABLE, std::slice::from_ref(&key)).await?;
    stack.table_info(TABLE).await?;

    let after = stack.scrape().await?;
    assert_every_metric_described(&after);
    assert_counted(&before, &after, Call::Get, Code::Ok, COUNTED_GETS);
    assert_counted(&before, &after, Call::MultiGet, Code::Ok, 1);
    assert_counted(&before, &after, Call::TableInfo, Code::Ok, 1);
    let lookups = |exposition: &str| {
        series_sum(
            exposition,
            metric::LOOKUPS_TOTAL,
            &[(metric::LABEL_TABLE, TABLE)],
        )
    };
    let looked_up = lookups(&after) - lookups(&before);
    assert_eq!(
        looked_up.to_bits(),
        approx::gauge(COUNTED_GETS + 1).to_bits(),
        "every lookup answered before the scrape is in it: {looked_up}"
    );
    // On the Full tier the first lookup of the key misses the row cache
    // and takes its value page from the page cache once, a hit or a fetch;
    // every later one is a row-cache hit.
    let delta = |name: &str| series_sum(&after, name, &[]) - series_sum(&before, name, &[]);
    let served = delta(metric::PAGE_CACHE_HITS_TOTAL) + delta(metric::PAGE_CACHE_FETCHES_TOTAL);
    assert_eq!(
        served.to_bits(),
        approx::gauge(1).to_bits(),
        "every page hit before the scrape is in it: {served}"
    );
    let row_misses = delta(metric::ROW_CACHE_MISSES_TOTAL);
    assert_eq!(
        row_misses.to_bits(),
        approx::gauge(1).to_bits(),
        "every row-cache miss before the scrape is in it: {row_misses}"
    );
    let row_hits = delta(metric::ROW_CACHE_HITS_TOTAL);
    assert_eq!(
        row_hits.to_bits(),
        approx::gauge(COUNTED_GETS).to_bits(),
        "every row-cache hit before the scrape is in it: {row_hits}"
    );
    let rss = series_value(&after, metric::PROCESS_RSS_BYTES, &[])
        .expect("the scrape samples the resident set size");
    assert!(rss > 0.0, "a running process holds memory: {rss}");
    let age = series_value(
        &after,
        metric::GENERATION_AGE_SECONDS,
        &[(metric::LABEL_TABLE, TABLE)],
    )
    .expect("the serving generation's age is sampled");
    assert!(
        age >= 0.0,
        "the snapshot was committed before the scrape: {age}"
    );

    // Scan, MultiScan, and a refused Scan are counted as the others are,
    // and the scans' own series count each key read.
    let max = stack.max_scan_rows(ORDERED_TABLE).await?;
    let keys: Vec<Key> = (ORDERED_IDS.start..)
        .take(COUNTED_SCAN_KEYS)
        .map(|id| stack.ordered().key(id))
        .collect();
    stack.scan(scan_of(ORDERED_TABLE, &keys[0], max)).await?;
    stack
        .multi_scan(multi_scan_of(ORDERED_TABLE, &keys, max))
        .await?;
    let refused = ScanRequest {
        direction: i32::from(v1::Direction::Unspecified),
        ..scan_of(ORDERED_TABLE, &keys[0], max)
    };
    assert_eq!(code_of(stack.scan(refused).await), Code::InvalidArgument);
    let scanned = stack.scrape().await?;
    assert_every_metric_described(&scanned);
    assert_counted(&after, &scanned, Call::Scan, Code::Ok, 1);
    assert_counted(&after, &scanned, Call::MultiScan, Code::Ok, 1);
    assert_counted(&after, &scanned, Call::Scan, Code::InvalidArgument, 1);
    let table = [(metric::LABEL_TABLE, ORDERED_TABLE)];
    let read = |exposition: &str, name: &str| series_sum(exposition, name, &table);
    let keys_read = approx::gauge(1 + COUNTED_SCAN_KEYS as u64);
    for name in [
        metric::SCANS_TOTAL,
        &format!("{}_count", metric::SCAN_ROWS),
        &format!("{}_count", metric::SCAN_FILES),
    ] {
        let counted = read(&scanned, name) - read(&after, name);
        assert_eq!(counted.to_bits(), keys_read.to_bits(), "{name}");
    }
    Ok(())
}

/// The scrape samples each table's oldest applying delete's age beside its
/// generation's: 0 on a table with no deletes; on one whose deletes were
/// committed in two snapshots, the later served, above 0 and no less than
/// the generation's age, since the oldest delete's snapshot is no newer
/// than the one served.
#[tokio::test(flavor = "multi_thread")]
async fn test_scrape_samples_the_oldest_delete_age_beside_the_generation_age() -> TestResult {
    /// The ids deleted, the first in one snapshot, the second in the next.
    const DELETED_IDS: [u64; 2] = [SAMPLE[0], SAMPLE[1]];
    let _serial = SERIAL.lock().await;
    let specs = vec![
        FixtureSpec::small(TABLE, KeyType::Long),
        FixtureSpec::small(DELETED_TABLE, KeyType::Long),
    ];
    let mut stack = Stack::start_with(specs, async |setup| {
        let fixture = &mut setup.fixtures[1];
        fixture
            .delete(&DELETED_IDS[..1], DeleteForm::ScopedPositions, "first")
            .await?;
        fixture
            .delete(&DELETED_IDS[1..], DeleteForm::ScopedPositions, "second")
            .await?;
        setup.config.tables[1] = fixture.config(Residency::Full, false);
        Ok(())
    })
    .await?;
    stack.serving.ready().await;
    let scrape = stack.scrape().await?;
    let gauge = |name: &str, table: &str| {
        series_value(&scrape, name, &[(metric::LABEL_TABLE, table)])
            .unwrap_or_else(|| panic!("{name} of {table} is sampled"))
    };
    assert_eq!(
        gauge(metric::TABLE_OLDEST_DELETE_AGE_SECONDS, TABLE).to_bits(),
        approx::gauge(0).to_bits(),
        "no delete applies"
    );
    let oldest = gauge(metric::TABLE_OLDEST_DELETE_AGE_SECONDS, DELETED_TABLE);
    let generation = gauge(metric::GENERATION_AGE_SECONDS, DELETED_TABLE);
    assert!(
        oldest > 0.0,
        "the deletes were committed before the scrape: {oldest}"
    );
    assert!(
        oldest >= generation,
        "the oldest delete is no newer than the served snapshot: {oldest} < {generation}"
    );
    assert_eq!(
        gauge(metric::TABLE_DELETED_ROWS, DELETED_TABLE).to_bits(),
        approx::gauge_len(DELETED_IDS.len()).to_bits()
    );
    Ok(())
}

/// A request body that is no gRPC message: its first byte, the
/// compression flag, is neither 0 nor 1, so tonic answers `INTERNAL`
/// before any handler runs, as `curl --http2-prior-knowledge` sending it
/// is answered.
const MALFORMED_BODY: &[u8] = b"\xff\xff\xff";

/// A request tonic answers before its handler runs, a body that is no gRPC
/// message, is counted under the status the client received, and a valid
/// request beside it under `OK`: each exactly once.
#[tokio::test(flavor = "multi_thread")]
async fn test_requests_total_counts_a_request_answered_before_its_handler() -> TestResult {
    let _serial = SERIAL.lock().await;
    let mut stack = Stack::ready_small().await?;
    let before = stack.scrape().await?;

    let mut channel = Channel::from_shared(format!("http://{}", stack.serving.listen_addr()))?
        .connect()
        .await?;
    let malformed = hyper::Request::builder()
        .method(hyper::Method::POST)
        .uri(format!("/{SERVICE_NAME}/{}", Call::Get.method()))
        .header(hyper::header::CONTENT_TYPE, "application/grpc")
        .header(hyper::header::TE, "trailers")
        .body(tonic::body::Body::new(http_body_util::Full::new(
            bytes::Bytes::from_static(MALFORMED_BODY),
        )))?;
    std::future::poll_fn(|cx| tower::Service::poll_ready(&mut channel, cx)).await?;
    let response = tower::Service::call(&mut channel, malformed).await?;
    let answered = response
        .headers()
        .get("grpc-status")
        .map(|code| Code::from_bytes(code.as_bytes()));
    assert_eq!(answered, Some(Code::Internal), "{:?}", response.headers());

    let key = stack.fixtures[0].key(SAMPLE[0]);
    stack.get(TABLE, Some(wire_key(&key))).await?;

    let after = stack.scrape().await?;
    assert_counted(&before, &after, Call::Get, Code::Internal, 1);
    assert_counted(&before, &after, Call::Get, Code::Ok, 1);
    let total = |exposition: &str| series_sum(exposition, metric::REQUESTS_TOTAL, &[]);
    assert_eq!(
        (total(&after) - total(&before)).to_bits(),
        2.0_f64.to_bits(),
        "nothing else is counted"
    );
    Ok(())
}

/// Asserts that `scrape` exposes an ordered table's scan series at zero, the
/// histograms bucketed, before any Scan, and a key-value table's none.
fn assert_scan_series_exposed_at_zero(scrape: &str) {
    let table = (metric::LABEL_TABLE, FRESH_TABLE);
    let ordered = (metric::LABEL_TABLE, FRESH_ORDERED_TABLE);
    for result in ScanResult::ALL {
        let result = result.to_string();
        assert_eq!(
            series_value(
                scrape,
                metric::SCANS_TOTAL,
                &[ordered, (metric::LABEL_RESULT, result.as_str())]
            ),
            Some(0.0),
            "{result}"
        );
    }
    let infinite = ("le", "+Inf");
    for name in [
        metric::SCAN_ROWS,
        metric::SCAN_FILES,
        metric::SCAN_RESPONSE_BYTES,
    ] {
        assert_eq!(
            series_value(scrape, &format!("{name}_bucket"), &[ordered, infinite]),
            Some(0.0),
            "{name}"
        );
    }
    for temperature in Temperature::ALL {
        let temperature = temperature.to_string();
        assert_eq!(
            series_value(
                scrape,
                &format!("{}_bucket", metric::SCAN_SECONDS),
                &[
                    ordered,
                    (metric::LABEL_TEMPERATURE, temperature.as_str()),
                    infinite
                ]
            ),
            Some(0.0),
            "{temperature}"
        );
    }
    let scan_series = [
        metric::SCANS_TOTAL.to_string(),
        format!("{}_bucket", metric::SCAN_ROWS),
        format!("{}_bucket", metric::SCAN_FILES),
        format!("{}_bucket", metric::SCAN_RESPONSE_BYTES),
        format!("{}_bucket", metric::SCAN_SECONDS),
    ];
    for name in &scan_series {
        assert_eq!(
            series_value(scrape, name, &[table]),
            None,
            "a key-value table exposes no {name}"
        );
    }
}

/// Every series an alert keys on is exposed once the server serves, before
/// any call, swap, or reload: a new table's counters at zero, each call's
/// `OK` count, each reload result, the page cache's counters and bytes,
/// nothing in flight of the default slots, nothing pending, and the table
/// neither degraded, held, nor below its level, at the optimal level it
/// declares (RFD 020). The process-wide counters other tests also count
/// are checked for presence.
#[tokio::test(flavor = "multi_thread")]
async fn test_alerting_series_are_exposed_from_the_start() -> TestResult {
    let _serial = SERIAL.lock().await;
    let mut stack = Stack::start_with(
        vec![
            FixtureSpec::small(FRESH_TABLE, KeyType::Long),
            ordered_spec(FRESH_ORDERED_TABLE),
        ],
        async |setup| {
            setup.options.reload = Some(Versions::new(setup.config.clone()).reload());
            Ok(())
        },
    )
    .await?;
    stack.serving.ready().await;
    let scrape = stack.scrape().await?;
    assert_every_metric_described(&scrape);

    let table = (metric::LABEL_TABLE, FRESH_TABLE);
    for result in LookupResult::ALL {
        let result = result.to_string();
        let labels = [table, (metric::LABEL_RESULT, result.as_str())];
        assert_eq!(
            series_value(&scrape, metric::LOOKUPS_TOTAL, &labels),
            Some(0.0),
            "{result}"
        );
    }
    assert_eq!(
        series_value(&scrape, metric::GENERATION_SWAPS_TOTAL, &[table]),
        Some(0.0)
    );
    assert_scan_series_exposed_at_zero(&scrape);
    // The state label's values are spelled out: `StateName` is private to
    // icetray-core.
    for name in [FRESH_TABLE, FRESH_ORDERED_TABLE] {
        let table = (metric::LABEL_TABLE, name);
        for state in ["degraded", "held"] {
            assert_eq!(
                series_value(
                    &scrape,
                    metric::TABLE_STATE,
                    &[table, (metric::LABEL_STATE, state)]
                ),
                Some(0.0),
                "{name} {state}"
            );
        }
        for gauge_name in [
            metric::TABLE_BELOW_LEVEL,
            metric::TABLE_LEVEL,
            metric::TABLE_DECLARED_LEVEL,
        ] {
            assert_eq!(
                series_value(&scrape, gauge_name, &[table]),
                Some(0.0),
                "{name} {gauge_name}"
            );
        }
    }
    for call in Call::ALL {
        assert!(calls_exposed(&scrape, call, Code::Ok), "{call} in {scrape}");
    }
    for result in ReloadResult::ALL {
        let result = result.to_string();
        let labels = [(metric::LABEL_RESULT, result.as_str())];
        assert!(
            series_value(&scrape, metric::CONFIG_RELOADS_TOTAL, &labels).is_some(),
            "{result} in {scrape}"
        );
    }
    for name in [
        metric::PAGE_CACHE_FETCHES_TOTAL,
        metric::PAGE_CACHE_HITS_TOTAL,
        metric::PAGE_CACHE_BYTES,
    ] {
        gauge(&scrape, name);
    }
    for cache in [CacheName::Lookup, CacheName::Scan] {
        let cache = cache.to_string();
        for name in [
            metric::PAGE_CACHE_FETCHES_TOTAL,
            metric::PAGE_CACHE_HITS_TOTAL,
        ] {
            assert!(
                series_value(&scrape, name, &[(metric::LABEL_CACHE, cache.as_str())]).is_some(),
                "{name} of the {cache} cache in {scrape}"
            );
        }
    }
    assert_in_flight(&scrape, 0, ServerConfig::default().max_in_flight);
    assert_eq!(
        series_value(&scrape, metric::CONFIG_PENDING, &[]),
        Some(0.0)
    );
    Ok(())
}

/// Two scrapes with nothing between them expose the same gauges: every
/// series of a described gauge the first exposed, the second exposes too.
/// The recorder drops a gauge no scrape wrote since the scrape before, so a
/// gauge set once on an event instead of on the scrape path fails here. The
/// server serves a key-value and an ordered disk table whose page cache it
/// has sampled and follows its configuration, so the first scrape holds
/// every described gauge on the kind of table that publishes it, and the
/// test checks that it does: a gauge this server does not expose would pass
/// the comparison unheld. The per-hit and per-miss cost gauges are a
/// key-value table's alone, and the per-Scan one an ordered table's alone
/// (RFD 025, Metrics).
#[tokio::test(flavor = "multi_thread")]
async fn test_scrape_keeps_every_gauge_the_scrape_before_exposed() -> TestResult {
    let mut stack = Stack::start_with(
        vec![
            FixtureSpec::small(SCRAPED_TABLE, KeyType::Long),
            ordered_spec(SCRAPED_ORDERED_TABLE),
        ],
        async |setup| {
            setup.with_local_dir()?;
            setup.options.reload = Some(Versions::new(setup.config.clone()).reload());
            Ok(())
        },
    )
    .await?;
    stack.serving.ready().await;
    stack.serving.opened().sample_page_cache().await;
    let first = stack.scrape().await?;
    let second = stack.scrape().await?;

    let key_value_only = [
        metric::TABLE_GETS_PER_HIT,
        metric::TABLE_GETS_PER_MISS,
        metric::TABLE_COLUMN_GETS_PER_HIT,
    ];
    let ordered_only = [metric::TABLE_GETS_PER_SCAN];
    for table in [SCRAPED_TABLE, SCRAPED_ORDERED_TABLE] {
        let first_series = gauge_series(&first, table);
        let (published, withheld): (&[&str], &[&str]) = if table == SCRAPED_TABLE {
            (&key_value_only, &ordered_only)
        } else {
            (&ordered_only, &key_value_only)
        };
        let exposed = |name: &str| first_series.iter().any(|series| series.name == name);
        for (name, kind, _) in metric::DESCRIPTIONS {
            if kind == Kind::Gauge && !withheld.contains(&name) {
                assert!(exposed(name), "{name} of {table} in {first}");
            }
        }
        for name in published {
            assert!(exposed(name), "{name} of {table}");
        }
        for name in withheld {
            assert!(!exposed(name), "{name} is not published for {table}");
        }
        let second_series = gauge_series(&second, table);
        let dropped: Vec<_> = first_series.difference(&second_series).collect();
        assert!(
            dropped.is_empty(),
            "gauges of {table} the second scrape dropped: {dropped:?}"
        );
    }
    Ok(())
}

/// The daemon samples a disk table's chunk files as soon as the tables
/// serve, with no call or scrape asking: the resident fraction is exposed
/// without waiting out `PAGE_CACHE_SAMPLE_INTERVAL`, and a table that has
/// just written its chunk files has them whole in the page cache, at its
/// layout's level.
#[tokio::test(flavor = "multi_thread")]
async fn test_page_cache_is_sampled_as_the_tables_serve() -> TestResult {
    let mut stack = Stack::start_with(
        vec![FixtureSpec::small(SAMPLED_TABLE, KeyType::Long)],
        async |setup| setup.with_local_dir(),
    )
    .await?;
    stack.serving.ready().await;
    let labels = [(metric::LABEL_TABLE, SAMPLED_TABLE)];
    let fraction = poll_until(
        FIRST_SAMPLE_WAIT,
        "the first page cache sample",
        async || {
            let exposition = stack.scrape().await?;
            Ok(series_value(
                &exposition,
                metric::TABLE_CHUNK_RESIDENT_FRACTION,
                &labels,
            ))
        },
    )
    .await?;
    assert_eq!(fraction.to_bits(), 1.0_f64.to_bits());
    let exposition = stack.scrape().await?;
    assert_eq!(
        series_value(&exposition, metric::TABLE_BELOW_LEVEL, &labels).map(f64::to_bits),
        Some(0.0_f64.to_bits())
    );
    Ok(())
}

/// One sample line's series: its metric name and its label text.
#[derive(Debug, PartialEq, Eq, PartialOrd, Ord)]
struct Series<'a> {
    name: &'a str,
    labels: &'a str,
}

/// The series of every gauge `metric::DESCRIPTIONS` names in `exposition`,
/// leaving out those labeled with a table other than `table`, which belong
/// to another test's server.
fn gauge_series<'a>(exposition: &'a str, table: &str) -> BTreeSet<Series<'a>> {
    let gauges: Vec<&str> = metric::DESCRIPTIONS
        .iter()
        .filter(|(_, kind, _)| *kind == Kind::Gauge)
        .map(|(name, ..)| *name)
        .collect();
    exposition
        .lines()
        .filter(|line| !line.starts_with('#'))
        .filter_map(|line| {
            let (series, _value) = line.rsplit_once(' ')?;
            let (name, labels) = series.split_once('{').map_or((series, ""), |(name, rest)| {
                (name, rest.trim_end_matches('}'))
            });
            Some(Series { name, labels })
        })
        .filter(|series| gauges.contains(&series.name))
        .filter(|series| {
            series.labels.split(',').all(|pair| {
                pair.split_once('=').is_none_or(|(label, quoted)| {
                    label != metric::LABEL_TABLE || quoted.trim_matches('"') == table
                })
            })
        })
        .collect()
}

#[tokio::test(flavor = "multi_thread")]
async fn test_refresh_follows_the_table_within_the_poll_interval() -> TestResult {
    let _serial = SERIAL.lock().await;
    let mut stack = Stack::start_with(
        vec![FixtureSpec::small(TABLE, KeyType::Long)],
        async |setup| {
            // Followed from the table root through its version hint, so an
            // append after the start is what the poll finds.
            setup.fixtures[0].write_hint().await?;
            setup.config.tables[0].source = setup.fixtures[0].root_source();
            Ok(())
        },
    )
    .await?;
    stack.serving.ready().await;
    let first = stack.table_info(TABLE).await?.snapshot_id;

    stack.fixtures[0]
        .append(stack.catalog.as_ref(), NEW_IDS, APPEND_PREFIX)
        .await?;
    stack.fixtures[0].write_hint().await?;
    let advanced = poll_until(REFRESH_WAIT, "the table advancing", async || {
        let info = stack.table_info(TABLE).await?;
        Ok((info.snapshot_id != first).then_some(info))
    })
    .await?;
    assert_eq!(
        advanced.snapshot_id,
        stack.fixtures[0]
            .metadata()
            .current_snapshot_id()
            .expect("the append committed a snapshot")
    );

    let new_key = stack.fixtures[0].key(NEW_IDS.start);
    let response = stack.get(TABLE, Some(wire_key(&new_key))).await?;
    assert_eq!(response.presence, i32::from(v1::Presence::Present));
    assert_eq!(
        one_wire_value(&response.values),
        stack.fixtures[0].value(&new_key)
    );
    assert_eq!(response.snapshot_id, advanced.snapshot_id);

    let (status, scrape) = stack.http_get(METRICS_PATH).await?;
    assert_eq!(status, StatusCode::OK);
    let sequence_number = stack.fixtures[0]
        .metadata()
        .current_snapshot()
        .expect("the append committed a snapshot")
        .sequence_number();
    assert_eq!(
        series_value(
            &scrape,
            metric::GENERATION_SEQUENCE_NUMBER,
            &[(metric::LABEL_TABLE, TABLE)]
        )
        .map(f64::to_bits),
        Some(approx::signed_gauge(sequence_number).to_bits()),
        "the served snapshot's sequence number"
    );
    let swapped_at = series_value(
        &scrape,
        metric::GENERATION_SWAPPED_AT_SECONDS,
        &[(metric::LABEL_TABLE, TABLE)],
    )
    .expect("a swap sets the swap time");
    assert!(swapped_at > 0.0, "{swapped_at}");
    assert!(labeled_with(&scrape, metric::LABEL_TABLE) > 0);
    assert_eq!(
        labeled_with(&scrape, SNAPSHOT_ID_LABEL),
        0,
        "a series per snapshot would grow with every swap"
    );
    Ok(())
}

/// The label no metric carries: the snapshot id is in the logs and the
/// table-info reply, and a series per snapshot would grow with every swap.
const SNAPSHOT_ID_LABEL: &str = "snapshot_id";

/// The sample lines of a Prometheus text exposition whose labels include
/// `label`.
fn labeled_with(exposition: &str, label: &str) -> usize {
    exposition
        .lines()
        .filter(|line| !line.starts_with('#'))
        .filter_map(|line| line.split_once('{')?.1.split_once('}'))
        .filter(|(labels, _)| {
            labels
                .split(',')
                .any(|pair| pair.split_once('=').is_some_and(|(name, _)| name == label))
        })
        .count()
}

/// A `Get` in flight when `shutdown` is asked for is answered, and the
/// drain ends `Ok`. The store is held until the call has reached it, so the
/// call is open on the server when the drain starts; a call still open when
/// the drain deadline passes is `test_drain_stops_waiting_at_its_deadline`.
#[tokio::test(flavor = "multi_thread")]
async fn test_shutdown_drains_and_returns_ok() -> TestResult {
    let _serial = SERIAL.lock().await;
    let stack = Stack::ready_small().await?;
    let key = stack.fixtures[0].key(SAMPLE[0]);
    stack.s3.hold();
    let mut client = stack.client.clone();
    let in_flight = tokio::spawn(async move {
        client
            .get(GetRequest {
                table: TABLE.to_string(),
                key: Some(wire_key(&key)),
                columns: wire_columns(&[VALUE_COLUMN]),
            })
            .await
    });
    poll_until(SHUTDOWN_WAIT, "the call reaching the store", async || {
        Ok((stack.s3.held() == 1).then_some(()))
    })
    .await?;
    stack.serving.shutdown();
    stack.s3.release();
    let response = in_flight.await??.into_inner();
    assert_eq!(response.presence, i32::from(v1::Presence::Present));
    tokio::time::timeout(SHUTDOWN_WAIT, stack.serving.join()).await??;
    Ok(())
}

/// A client watching health holds a stream open, and tonic's graceful
/// shutdown waits for every connection to close; the drain sends
/// `NOT_SERVING` on both health names and ends their watches, so the run
/// returns.
#[tokio::test(flavor = "multi_thread")]
async fn test_shutdown_ends_health_watches_so_the_drain_finishes() -> TestResult {
    let _serial = SERIAL.lock().await;
    let mut stack = Stack::ready_small().await?;
    let watches = [
        stack.watch_health(OVERALL_SERVICE).await?,
        stack.watch_health(SERVICE_NAME).await?,
    ];
    stack.serving.shutdown();
    tokio::time::timeout(SHUTDOWN_WAIT, stack.serving.join()).await??;
    for watch in watches {
        let statuses = tokio::time::timeout(SHUTDOWN_WAIT, watch).await???;
        assert_eq!(statuses.first(), Some(&ServingStatus::Serving));
        assert_eq!(statuses.last(), Some(&ServingStatus::NotServing));
    }
    Ok(())
}

/// A failed start ends the run on its own once the grace is over, and the
/// watches opened during the grace end with it rather than hold the drain.
#[tokio::test(flavor = "multi_thread")]
async fn test_failed_start_ends_health_watches_so_the_run_ends() -> TestResult {
    let _serial = SERIAL.lock().await;
    let mut stack = Stack::start_with(
        vec![FixtureSpec::small(TABLE, KeyType::Long)],
        async |setup| {
            setup.config.tables[0].residency = Residency::Disk;
            setup.options.start_failure_grace = REFRESH_WAIT;
            Ok(())
        },
    )
    .await?;
    let watches = [
        stack.watch_health(OVERALL_SERVICE).await?,
        stack.watch_health(SERVICE_NAME).await?,
    ];
    let outcome = tokio::time::timeout(SHUTDOWN_WAIT, stack.serving.join()).await?;
    assert!(
        matches!(outcome, Err(ServeError::FirstLoad { .. })),
        "expected a first-load failure, got {outcome:?}"
    );
    for watch in watches {
        let statuses = tokio::time::timeout(SHUTDOWN_WAIT, watch).await???;
        assert!(
            statuses
                .iter()
                .all(|status| *status == ServingStatus::NotServing),
            "{statuses:?}"
        );
        assert!(!statuses.is_empty());
    }
    Ok(())
}

/// A first load that fails leaves the run up through the grace with the
/// failure on the health service, then ends it with `FirstLoad`. The check
/// starts once the table is `Degraded`, which the load sets in the poll
/// that ends the loads and starts the grace, and holds that the run stays
/// up for `STAYS_UP_FOR`: an interval, since "stays up" is the claim, and
/// short, a fifth of the grace, so the rest is room for the poll's delay.
#[tokio::test(flavor = "multi_thread")]
async fn test_first_load_failure_stays_visible_then_fails() -> TestResult {
    let _serial = SERIAL.lock().await;
    let mut stack = Stack::start_with(
        vec![FixtureSpec::small(TABLE, KeyType::Long)],
        async |setup| {
            // Disk residency with no `storage.local`: the configuration is
            // built here rather than parsed, so the violation `Config::parse`
            // would refuse is met at the first load.
            setup.config.tables[0].residency = Residency::Disk;
            setup.options.start_failure_grace = FAILURE_GRACE;
            Ok(())
        },
    )
    .await?;
    let table = Arc::clone(&stack.serving.opened().tables()[0]);
    poll_until(REFRESH_WAIT, "the first load failing", async || {
        Ok(matches!(table.state(), TableState::Degraded { .. }).then_some(()))
    })
    .await?;

    assert!(
        tokio::time::timeout(STAYS_UP_FOR, stack.serving.stopped())
            .await
            .is_err(),
        "the run stays up through the grace"
    );
    assert_eq!(
        stack.health_status(OVERALL_SERVICE).await?,
        ServingStatus::NotServing,
        "the failure stays visible through the grace"
    );
    assert_eq!(
        stack.health_status(SERVICE_NAME).await?,
        ServingStatus::NotServing
    );
    stack.serving.ready().await;
    let outcome = tokio::time::timeout(SHUTDOWN_WAIT, stack.serving.join()).await?;
    let Err(ServeError::FirstLoad { failures }) = outcome else {
        panic!("expected a first-load failure, got {outcome:?}");
    };
    assert_eq!(failures, vec![(TABLE.to_string(), ErrorKind::Config)]);
    Ok(())
}

/// A periodic task that panics ends the run with the task named, so the
/// process exits non-zero instead of answering `/ready` 200 from tables
/// nothing refreshes. A poll interval of zero is what `Config::parse`
/// refuses as `PollIntervalTooShort`, and what tokio's interval refuses with a
/// panic at the first tick of the refresh and upkeep tasks; the
/// configuration is built directly, so the panic is reached. Both tasks
/// panic at once, and whichever the run joins first is the one named.
#[tokio::test(flavor = "multi_thread")]
async fn test_periodic_task_panic_ends_the_run_naming_the_task() -> TestResult {
    let _serial = SERIAL.lock().await;
    let mut stack = Stack::start_with(
        vec![FixtureSpec::small(TABLE, KeyType::Long)],
        async |setup| {
            setup.config.server.poll_interval = Duration::ZERO;
            Ok(())
        },
    )
    .await?;
    stack.serving.ready().await;
    let outcome = tokio::time::timeout(SHUTDOWN_WAIT, stack.serving.join()).await?;
    let Err(ServeError::Periodic { task, source }) = outcome else {
        panic!("expected a periodic task failure, got {outcome:?}");
    };
    assert!(source.is_panic(), "{source}");
    match task {
        PeriodicTask::Refresh { table } => assert_eq!(table, TABLE),
        PeriodicTask::Upkeep => {}
        PeriodicTask::PageCacheSample => {
            panic!("the page cache sample runs on its own interval, not the poll interval")
        }
    }
    Ok(())
}

/// A shutdown asked for once the first loads have failed cuts the grace
/// short: a failed start whose grace is `SHUTDOWN_WAIT` long reports its
/// failure well inside the grace instead of sleeping it out. The shutdown
/// waits for the table to be `Degraded`, which the load sets on the run's
/// task in the poll that ends the loads, so the loads' outcome is in hand
/// before the shutdown is asked for.
#[tokio::test(flavor = "multi_thread")]
async fn test_shutdown_during_the_grace_ends_the_run_without_waiting() -> TestResult {
    let _serial = SERIAL.lock().await;
    let stack = Stack::start_with(
        vec![FixtureSpec::small(TABLE, KeyType::Long)],
        async |setup| {
            setup.config.tables[0].residency = Residency::Disk;
            setup.options.start_failure_grace = SHUTDOWN_WAIT;
            Ok(())
        },
    )
    .await?;
    let table = Arc::clone(&stack.serving.opened().tables()[0]);
    poll_until(REFRESH_WAIT, "the first load failing", async || {
        Ok(matches!(table.state(), TableState::Degraded { .. }).then_some(()))
    })
    .await?;
    stack.serving.shutdown();
    let outcome = tokio::time::timeout(REFRESH_WAIT, stack.serving.join()).await?;
    let Err(ServeError::FirstLoad { failures }) = outcome else {
        panic!("expected a first-load failure, got {outcome:?}");
    };
    assert_eq!(failures.len(), 1);
    Ok(())
}

/// A shutdown asked for while a first load waits on a store that has
/// stopped answering ends the load and the run at once, `Ok`, instead of
/// waiting for the load to reach its refresh deadline.
#[tokio::test(flavor = "multi_thread")]
async fn test_shutdown_during_the_first_loads_ends_them() -> TestResult {
    let _serial = SERIAL.lock().await;
    assert!(ServerConfig::default().refresh_deadline > SHUTDOWN_WAIT);
    let stack = Stack::start_with(
        vec![FixtureSpec::small(TABLE, KeyType::Long)],
        async |setup| {
            setup.hold_store = true;
            Ok(())
        },
    )
    .await?;
    poll_until(
        SHUTDOWN_WAIT,
        "the first load reaching the store",
        async || Ok((stack.s3.held() > 0).then_some(())),
    )
    .await?;

    stack.serving.shutdown();
    let (outcome, elapsed) =
        tokio::time::timeout(SHUTDOWN_WAIT, timed(stack.serving.join())).await?;
    outcome?;
    assert!(
        elapsed < FIRST_LOAD_SHUTDOWN_WITHIN,
        "the run ended {elapsed:?} after the shutdown"
    );
    Ok(())
}

/// Over a store that has stopped answering, a `Get` ends at the configured
/// `get_deadline` and a `MultiGet` at a shorter `multi_get_deadline`, each
/// with `DEADLINE_EXCEEDED`, instead of waiting on the store.
#[tokio::test(flavor = "multi_thread")]
async fn test_get_and_multi_get_end_at_their_deadlines_over_a_held_store() -> TestResult {
    let _serial = SERIAL.lock().await;
    let mut stack = Stack::start_with(
        vec![FixtureSpec::small(TABLE, KeyType::Long)],
        async |setup| {
            setup.config.server.get_deadline = LONGER_DEADLINE;
            setup.config.server.multi_get_deadline = SHORT_DEADLINE;
            Ok(())
        },
    )
    .await?;
    stack.serving.ready().await;
    let keys = stack.fixtures[0].keys_at(&SAMPLE[..BATCH_IDS]);
    stack.s3.hold();

    let (outcome, elapsed) = tokio::time::timeout(
        SHUTDOWN_WAIT,
        timed(stack.get(TABLE, Some(wire_key(&keys[0])))),
    )
    .await?;
    assert_ended_at(outcome, elapsed, LONGER_DEADLINE, Code::DeadlineExceeded);

    let (outcome, elapsed) =
        tokio::time::timeout(SHUTDOWN_WAIT, timed(stack.multi_get(TABLE, &keys[1..]))).await?;
    assert_ended_at(outcome, elapsed, SHORT_DEADLINE, Code::DeadlineExceeded);
    assert!(stack.s3.held() > 0, "the calls reached the held store");
    Ok(())
}

/// Over a store that has stopped answering, a `Scan` ends at the configured
/// `scan_deadline` and a `MultiScan` at a shorter `multi_scan_deadline`,
/// each with `DEADLINE_EXCEEDED`, instead of waiting on the store.
#[tokio::test(flavor = "multi_thread")]
async fn test_scan_and_multi_scan_end_at_their_deadlines_over_a_held_store() -> TestResult {
    let _serial = SERIAL.lock().await;
    let mut stack = Stack::ready_ordered_with(async |setup| {
        setup.config.server.scan_deadline = LONGER_DEADLINE;
        setup.config.server.multi_scan_deadline = SHORT_DEADLINE;
        Ok(())
    })
    .await?;
    let keys = stack
        .ordered()
        .keys_at(&[ORDERED_IDS.start, ORDERED_IDS.start + 1]);
    let max = stack.max_scan_rows(ORDERED_TABLE).await?;
    stack.s3.hold();

    let (outcome, elapsed) = tokio::time::timeout(
        SHUTDOWN_WAIT,
        timed(stack.scan(scan_of(ORDERED_TABLE, &keys[0], max))),
    )
    .await?;
    assert_ended_at(outcome, elapsed, LONGER_DEADLINE, Code::DeadlineExceeded);

    let (outcome, elapsed) = tokio::time::timeout(
        SHUTDOWN_WAIT,
        timed(stack.multi_scan(multi_scan_of(ORDERED_TABLE, &keys[1..], max))),
    )
    .await?;
    assert_ended_at(outcome, elapsed, SHORT_DEADLINE, Code::DeadlineExceeded);
    assert!(stack.s3.held() > 0, "the calls reached the held store");
    Ok(())
}

/// A `Scan` and a `MultiScan` hold in-flight slots as `Get` and `MultiGet`
/// do: with both slots taken by scans held on a stalled store, one more call
/// of any kind is refused at once with `RESOURCE_EXHAUSTED`, and the held
/// scans end at their deadlines and give their slots back, so the next
/// Scan is admitted and answers.
#[tokio::test(flavor = "multi_thread")]
async fn test_scans_hold_in_flight_slots_and_a_call_beyond_them_is_refused() -> TestResult {
    let _serial = SERIAL.lock().await;
    let mut stack = Stack::ready_ordered_with(async |setup| {
        setup.config.server.max_in_flight = IN_FLIGHT_CAP;
        setup.config.server.scan_deadline = LONGER_DEADLINE;
        setup.config.server.multi_scan_deadline = LONGER_DEADLINE;
        Ok(())
    })
    .await?;
    let keys = stack.ordered().keys_at(&[
        ORDERED_IDS.start,
        ORDERED_IDS.start + 1,
        ORDERED_IDS.start + 2,
    ]);
    let max = stack.max_scan_rows(ORDERED_TABLE).await?;
    stack.s3.hold();

    let mut client = stack.client.clone();
    let held_scan = tokio::spawn({
        let request = scan_of(ORDERED_TABLE, &keys[0], max);
        async move { client.scan(request).await.map(|_| ()) }
    });
    let mut client = stack.client.clone();
    let held_batch = tokio::spawn({
        let request = multi_scan_of(ORDERED_TABLE, &keys[1..2], max);
        async move { client.multi_scan(request).await.map(|_| ()) }
    });
    let before = poll_until(SHUTDOWN_WAIT, "both scans taking a slot", async || {
        let exposition = stack.scrape().await?;
        let taken = gauge(&exposition, metric::REQUESTS_IN_FLIGHT).to_bits();
        Ok((taken == approx::gauge_len(IN_FLIGHT_CAP).to_bits()).then_some(exposition))
    })
    .await?;
    assert_in_flight(&before, IN_FLIGHT_CAP, IN_FLIGHT_CAP);
    let (outcome, elapsed) = timed(stack.scan(scan_of(ORDERED_TABLE, &keys[2], max))).await;
    assert_eq!(code_of(outcome), Code::ResourceExhausted);
    assert!(elapsed < REFUSED_WITHIN, "refused after {elapsed:?}");
    let (outcome, elapsed) =
        timed(stack.multi_scan(multi_scan_of(ORDERED_TABLE, &keys[2..], max))).await;
    assert_eq!(code_of(outcome), Code::ResourceExhausted);
    assert!(elapsed < REFUSED_WITHIN, "refused after {elapsed:?}");

    for held in [held_scan, held_batch] {
        let outcome = tokio::time::timeout(SHUTDOWN_WAIT, held).await??;
        assert_eq!(code_of(outcome), Code::DeadlineExceeded);
    }
    let after = stack.scrape().await?;
    for call in [Call::Scan, Call::MultiScan] {
        assert_counted(&before, &after, call, Code::ResourceExhausted, 1);
        assert_counted(&before, &after, call, Code::DeadlineExceeded, 1);
    }
    assert_in_flight(&after, 0, IN_FLIGHT_CAP);
    stack.s3.release();
    let request = scan_of(ORDERED_TABLE, &keys[2], whole_key().limit);
    let response = stack.scan(request).await?;
    assert_eq!(response, model_scan_response(stack.ordered(), &keys[2]));
    assert_in_flight(&stack.scrape().await?, 0, IN_FLIGHT_CAP);
    Ok(())
}

/// A `Get` past its deadline takes its page fetch with it: over a held
/// store the call ends `DEADLINE_EXCEEDED` at `get_deadline`, and the GET
/// behind it is given up within `DEADLINE_MARGIN` of that instead of
/// waiting on the store under the S3 client's own bounds. A `MultiGet`'s
/// page fetches have the same deadline, so under a longer
/// `multi_get_deadline` the batch ends at `get_deadline` too. Nothing from
/// an abandoned fetch is cached: once the store answers again, the same key
/// runs a fetch of its own.
#[tokio::test(flavor = "multi_thread")]
async fn test_a_get_past_its_deadline_ends_its_page_fetch() -> TestResult {
    let _serial = SERIAL.lock().await;
    let mut stack = Stack::start_with(
        vec![FixtureSpec::small(TABLE, KeyType::Long)],
        async |setup| {
            setup.config.server.get_deadline = SHORT_DEADLINE;
            Ok(())
        },
    )
    .await?;
    assert!(ServerConfig::default().multi_get_deadline > SHORT_DEADLINE + DEADLINE_MARGIN);
    stack.serving.ready().await;
    let keys = stack.fixtures[0].keys_at(&SAMPLE[..BATCH_IDS]);
    let cache = stack.serving.opened().cache.clone();
    let fetched = cache.fetches();
    stack.s3.hold();

    let (outcome, elapsed) = tokio::time::timeout(
        SHUTDOWN_WAIT,
        timed(stack.get(TABLE, Some(wire_key(&keys[0])))),
    )
    .await?;
    assert_ended_at(outcome, elapsed, SHORT_DEADLINE, Code::DeadlineExceeded);
    assert_eq!(stack.s3.held(), 1, "the call reached the held store");
    assert_eq!(cache.fetches(), fetched + 1);
    poll_until(
        DEADLINE_MARGIN,
        "the page GET ending with the call",
        async || Ok((stack.s3.waiting() == 0).then_some(())),
    )
    .await?;

    let (outcome, elapsed) =
        tokio::time::timeout(SHUTDOWN_WAIT, timed(stack.multi_get(TABLE, &keys[1..]))).await?;
    assert_ended_at(outcome, elapsed, SHORT_DEADLINE, Code::DeadlineExceeded);
    poll_until(
        DEADLINE_MARGIN,
        "the batch's page GETs ending with it",
        async || Ok((stack.s3.waiting() == 0).then_some(())),
    )
    .await?;

    stack.s3.release();
    let fetched = cache.fetches();
    let response = stack.get(TABLE, Some(wire_key(&keys[0]))).await?;
    assert_eq!(response.presence, i32::from(v1::Presence::Present));
    assert_eq!(
        cache.fetches(),
        fetched + 1,
        "the abandoned fetch cached nothing"
    );
    Ok(())
}

/// With every slot taken by calls held on a stalled store, one more `Get`
/// or `MultiGet` is refused at once with `RESOURCE_EXHAUSTED` rather than
/// queued; the held calls end at their deadline and give their slots back,
/// so the next call is admitted.
#[tokio::test(flavor = "multi_thread")]
async fn test_a_call_beyond_max_in_flight_is_refused_at_once() -> TestResult {
    let _serial = SERIAL.lock().await;
    let mut stack = Stack::start_with(
        vec![FixtureSpec::small(TABLE, KeyType::Long)],
        async |setup| {
            setup.config.server.max_in_flight = IN_FLIGHT_CAP;
            setup.config.server.get_deadline = SHORT_DEADLINE;
            setup.config.server.multi_get_deadline = SHORT_DEADLINE;
            Ok(())
        },
    )
    .await?;
    stack.serving.ready().await;
    let keys = stack.fixtures[0].keys_at(&SAMPLE[..BATCH_IDS]);
    stack.s3.hold();

    let mut client = stack.client.clone();
    let held_get = tokio::spawn({
        let request = GetRequest {
            table: TABLE.to_string(),
            key: Some(wire_key(&keys[0])),
            columns: wire_columns(&[VALUE_COLUMN]),
        };
        async move { client.get(request).await.map(|_| ()) }
    });
    let mut client = stack.client.clone();
    let held_batch = tokio::spawn({
        let request = MultiGetRequest {
            table: TABLE.to_string(),
            keys: vec![wire_key(&keys[1])],
            columns: wire_columns(&[VALUE_COLUMN]),
        };
        async move { client.multi_get(request).await.map(|_| ()) }
    });
    poll_until(SHUTDOWN_WAIT, "both calls reaching the store", async || {
        Ok((stack.s3.held() == IN_FLIGHT_CAP).then_some(()))
    })
    .await?;

    let before = stack.scrape().await?;
    assert_in_flight(&before, IN_FLIGHT_CAP, IN_FLIGHT_CAP);
    let (outcome, elapsed) = timed(stack.get(TABLE, Some(wire_key(&keys[2])))).await;
    assert_eq!(code_of(outcome), Code::ResourceExhausted);
    assert!(elapsed < REFUSED_WITHIN, "refused after {elapsed:?}");
    let (outcome, elapsed) = timed(stack.multi_get(TABLE, &keys[2..])).await;
    assert_eq!(code_of(outcome), Code::ResourceExhausted);
    assert!(elapsed < REFUSED_WITHIN, "refused after {elapsed:?}");
    assert_eq!(
        stack.s3.held(),
        IN_FLIGHT_CAP,
        "a refused call reads nothing"
    );

    for held in [held_get, held_batch] {
        let outcome = tokio::time::timeout(SHUTDOWN_WAIT, held).await??;
        assert_eq!(code_of(outcome), Code::DeadlineExceeded);
    }
    let after = stack.scrape().await?;
    for call in [Call::Get, Call::MultiGet] {
        assert_counted(&before, &after, call, Code::ResourceExhausted, 1);
        assert_counted(&before, &after, call, Code::DeadlineExceeded, 1);
    }
    assert_in_flight(&after, 0, IN_FLIGHT_CAP);
    stack.s3.release();
    let response = stack.get(TABLE, Some(wire_key(&keys[2]))).await?;
    assert_eq!(response.presence, i32::from(v1::Presence::Present));
    assert_in_flight(&stack.scrape().await?, 0, IN_FLIGHT_CAP);
    Ok(())
}

/// A call that ends at its client's deadline gives its slot back then, not
/// at the server's longer deadline: tonic's server layer drops the call at
/// the client's `grpc-timeout`, and the client reports the expiry as its
/// own, `CANCELLED`. With one slot, a second call made just after the first
/// ended reaches the store instead of being refused.
#[tokio::test(flavor = "multi_thread")]
async fn test_a_client_deadline_frees_the_slot_it_held() -> TestResult {
    let _serial = SERIAL.lock().await;
    let mut stack = Stack::start_with(
        vec![FixtureSpec::small(TABLE, KeyType::Long)],
        async |setup| {
            setup.config.server.max_in_flight = 1;
            Ok(())
        },
    )
    .await?;
    assert!(ServerConfig::default().get_deadline > CLIENT_DEADLINE + DEADLINE_MARGIN);
    stack.serving.ready().await;
    let keys = stack.fixtures[0].keys_at(&SAMPLE[..BATCH_IDS]);
    stack.s3.hold();
    let before = stack.scrape().await?;

    let timed_out = 2;
    for (sent, key) in keys[..timed_out].iter().enumerate() {
        let mut get = tonic::Request::new(GetRequest {
            table: TABLE.to_string(),
            key: Some(wire_key(key)),
            columns: wire_columns(&[VALUE_COLUMN]),
        });
        get.set_timeout(CLIENT_DEADLINE);
        let (outcome, elapsed) =
            tokio::time::timeout(SHUTDOWN_WAIT, timed(stack.client.get(get))).await?;
        assert_eq!(code_of(outcome), Code::Cancelled, "call {sent}");
        assert!(elapsed < CLIENT_DEADLINE + DEADLINE_MARGIN, "call {sent}");
        assert_eq!(stack.s3.held(), sent + 1, "call {sent} reached the store");
        // The server's own timer on the client's deadline fires a loopback
        // hop after the client's. It drops the call, and the slot and the
        // page GET with it, so once the store sees the GET given up, the
        // slot is back.
        poll_until(
            SHUTDOWN_WAIT,
            "the call's page GET ending with it",
            async || Ok((stack.s3.waiting() == 0).then_some(())),
        )
        .await?;
    }
    // A call tonic dropped at its client's deadline was answered
    // `CANCELLED` by tonic, and counts so.
    assert_counted(
        &before,
        &stack.scrape().await?,
        Call::Get,
        Code::Cancelled,
        approx::count(timed_out),
    );
    Ok(())
}

/// A `MultiGet` of `max_multi_get_values` values is answered, one column of
/// each key or each key alone; one more value is `INVALID_ARGUMENT` before
/// any lookup, so it issues no GET.
#[tokio::test(flavor = "multi_thread")]
async fn test_multi_get_over_max_values_is_invalid_argument_before_any_lookup() -> TestResult {
    let _serial = SERIAL.lock().await;
    let mut stack = Stack::start_with(
        vec![FixtureSpec::small(TABLE, KeyType::Long)],
        async |setup| {
            setup.config.server.max_multi_get_values = BATCH_IDS;
            Ok(())
        },
    )
    .await?;
    stack.serving.ready().await;
    let keys = stack.fixtures[0].keys_at(&SAMPLE[..=BATCH_IDS]);

    let before = stack.stats.gets();
    assert_eq!(
        code_of(stack.multi_get(TABLE, &keys).await),
        Code::InvalidArgument
    );
    assert_eq!(stack.stats.gets(), before, "a refused batch issues no GET");

    let at_limit = &keys[..BATCH_IDS];
    let response = stack.multi_get(TABLE, at_limit).await?;
    assert_eq!(response.results.len(), BATCH_IDS);
    for hit in &response.results {
        assert_eq!(hit.presence, i32::from(v1::Presence::Present));
    }
    let presence_only = stack.multi_get_naming(TABLE, at_limit, &[]).await?;
    assert_eq!(
        presence_only.results.len(),
        BATCH_IDS,
        "a presence-only key counts one value"
    );
    let before = stack.stats.gets();
    assert_eq!(
        code_of(
            stack
                .multi_get_naming(TABLE, &keys[..=BATCH_IDS], &[])
                .await
        ),
        Code::InvalidArgument
    );
    assert_eq!(stack.stats.gets(), before, "a refused batch issues no GET");
    Ok(())
}

/// A column the table does not serve, and a column named twice, are the
/// caller's faults on `Get` and `MultiGet` alike, refused before any read;
/// a presence-only call reads no value page.
#[tokio::test(flavor = "multi_thread")]
async fn test_unknown_or_repeated_column_is_invalid_argument_and_presence_only_reads_nothing()
-> TestResult {
    let _serial = SERIAL.lock().await;
    let mut stack = Stack::ready_small().await?;
    let key = stack.fixtures[0].key(SAMPLE[0]);
    let before = stack.stats.gets();
    for columns in [&["unserved"][..], &[VALUE_COLUMN, VALUE_COLUMN]] {
        assert_eq!(
            code_of(stack.get_naming(TABLE, Some(wire_key(&key)), columns).await),
            Code::InvalidArgument,
            "{columns:?}"
        );
        assert_eq!(
            code_of(
                stack
                    .multi_get_naming(TABLE, std::slice::from_ref(&key), columns)
                    .await
            ),
            Code::InvalidArgument,
            "{columns:?}"
        );
    }
    let present = stack.get_naming(TABLE, Some(wire_key(&key)), &[]).await?;
    assert_eq!(present.presence, i32::from(v1::Presence::Present));
    assert!(present.values.is_empty(), "presence alone carries no value");
    let absent = stack.fixtures[0].absent_near(SAMPLE[0]);
    let absent = stack
        .get_naming(TABLE, Some(wire_key(&absent)), &[])
        .await?;
    assert_eq!(absent.presence, i32::from(v1::Presence::Absent));
    assert_eq!(stack.stats.gets(), before, "no value page is read");
    Ok(())
}

/// A table serving two value columns, `VALUE_COLUMN` holding values of
/// `kind` and declared `format`, beside `SECOND_VALUE_COLUMN`, opaque.
async fn two_column_stack(kind: ValueKind, format: ValueFormat) -> Result<Stack, BoxError> {
    let spec = FixtureSpec {
        second_value: true,
        value_kind: kind,
        ..FixtureSpec::small(TABLE, KeyType::Long)
    };
    let mut stack = Stack::start_with(vec![spec], async |setup| {
        setup.config.tables[0] =
            setup.fixtures[0].config_declaring(Residency::Full, VALUE_COLUMN, format);
        Ok(())
    })
    .await?;
    stack.serving.ready().await;
    Ok(stack)
}

/// `TableInfo` lists the value columns a lookup may name, in name order,
/// each with its type and, for a `fixed` column alone, a width.
#[tokio::test(flavor = "multi_thread")]
async fn test_table_info_lists_the_value_columns() -> TestResult {
    let _serial = SERIAL.lock().await;
    let mut stack = two_column_stack(ValueKind::Random, ValueFormat::Opaque).await?;
    let info = stack.table_info(TABLE).await?;
    let binary = |name: &str| v1::ValueColumn {
        name: name.to_string(),
        value_type: i32::from(v1::ValueType::Binary),
        width: 0,
        format: i32::from(v1::ValueFormat::Opaque),
    };
    assert_eq!(
        info.value_columns,
        [binary(SECOND_VALUE_COLUMN), binary(VALUE_COLUMN)],
        "in name order"
    );
    Ok(())
}

/// `Get` and `MultiGet` naming two columns carry one value per column, in
/// the order the request names them, over the wire.
#[tokio::test(flavor = "multi_thread")]
async fn test_get_and_multi_get_carry_two_columns_in_request_order() -> TestResult {
    let _serial = SERIAL.lock().await;
    let mut stack = two_column_stack(ValueKind::Random, ValueFormat::Opaque).await?;
    let fixture = &stack.fixtures[0];
    let keys = fixture.keys_at(&SAMPLE);
    let expected = |key: &Key, columns: &[&str]| -> Vec<Option<Vec<u8>>> {
        columns
            .iter()
            .map(|&column| match column {
                VALUE_COLUMN => fixture.value(key),
                _ => fixture.second_value(key),
            })
            .collect()
    };
    let orders = [
        [VALUE_COLUMN, SECOND_VALUE_COLUMN],
        [SECOND_VALUE_COLUMN, VALUE_COLUMN],
    ];
    let wanted: Vec<_> = orders
        .iter()
        .map(|order| expected(&keys[0], order))
        .collect();
    assert_ne!(wanted[0], wanted[1], "the two orders differ");
    let batches: Vec<Vec<Vec<Option<Vec<u8>>>>> = orders
        .iter()
        .map(|order| keys.iter().map(|key| expected(key, order)).collect())
        .collect();
    for (order, (single, batch)) in orders.iter().zip(wanted.iter().zip(&batches)) {
        let got = stack
            .get_naming(TABLE, Some(wire_key(&keys[0])), order)
            .await?;
        assert_eq!(got.presence, i32::from(v1::Presence::Present));
        let values: Vec<_> = got.values.iter().map(wire_value).collect();
        assert_eq!(&values, single, "{order:?}");
        let answered = stack.multi_get_naming(TABLE, &keys, order).await?;
        let values: Vec<Vec<_>> = answered
            .results
            .iter()
            .map(|hit| hit.values.iter().map(wire_value).collect())
            .collect();
        assert_eq!(&values, batch, "{order:?}");
    }
    Ok(())
}

/// The column read of `name` keeping `fields`.
fn wire_read(name: &str, fields: &[u32]) -> v1::ColumnRead {
    v1::ColumnRead {
        name: name.to_string(),
        fields: fields.to_vec(),
    }
}

/// The value-bytes counters of `column` of `TABLE` in `exposition`: read,
/// then returned.
fn value_bytes(exposition: &str, column: &str) -> (f64, f64) {
    let labels = [(metric::LABEL_TABLE, TABLE), (metric::LABEL_COLUMN, column)];
    (
        series_sum(exposition, metric::VALUE_BYTES_READ_TOTAL, &labels),
        series_sum(exposition, metric::VALUE_BYTES_RETURNED_TOTAL, &labels),
    )
}

/// The bytes of `values`, nulls as none, as the value-bytes counters count
/// them.
fn length_of(values: &[Option<Vec<u8>>]) -> f64 {
    let bytes: usize = values.iter().flatten().map(Vec::len).sum();
    approx::gauge_len(bytes)
}

/// `TableInfo` reports each column's format, and `Get` and `MultiGet`
/// trim a protobuf column to the fields a read names while an opaque one
/// beside it is read whole; the value-bytes counters count what each
/// column read and returned (RFD 024).
#[tokio::test(flavor = "multi_thread")]
async fn test_get_and_multi_get_trim_a_protobuf_column_and_count_its_bytes() -> TestResult {
    let _serial = SERIAL.lock().await;
    let mut stack = two_column_stack(ValueKind::Protobuf, ValueFormat::Protobuf).await?;
    let info = stack.table_info(TABLE).await?;
    let formats: Vec<(String, i32)> = info
        .value_columns
        .iter()
        .map(|column| (column.name.clone(), column.format))
        .collect();
    assert_eq!(
        formats,
        [
            (
                SECOND_VALUE_COLUMN.to_string(),
                i32::from(v1::ValueFormat::Opaque)
            ),
            (
                VALUE_COLUMN.to_string(),
                i32::from(v1::ValueFormat::Protobuf)
            ),
        ]
    );
    let fixture = &stack.fixtures[0];
    let keys = fixture.keys_at(&SAMPLE);
    let fields = [protobuf::ID_FIELD, protobuf::SCORES_FIELD];
    let reads = [
        ColumnRead {
            name: VALUE_COLUMN,
            fields: &fields,
        },
        ColumnRead::whole(SECOND_VALUE_COLUMN),
    ];
    let wire_reads = || {
        vec![
            wire_read(VALUE_COLUMN, &fields),
            wire_read(SECOND_VALUE_COLUMN, &[]),
        ]
    };
    let wanted: Vec<Vec<Option<Vec<u8>>>> = keys
        .iter()
        .map(|key| {
            let Row::Present(values) = fixture.expected_reads(key, &reads) else {
                unreachable!("a sampled key is present");
            };
            values
                .into_iter()
                .map(|value| value.map(|bytes| bytes.to_vec()))
                .collect()
        })
        .collect();
    let whole: Vec<Option<Vec<u8>>> = keys.iter().map(|key| fixture.value(key)).collect();
    let before = stack.scrape().await?;
    for (key, expected) in keys.iter().zip(&wanted) {
        let got = stack
            .get_reading(TABLE, Some(wire_key(key)), wire_reads())
            .await?;
        let values: Vec<_> = got.values.iter().map(wire_value).collect();
        assert_eq!(&values, expected, "{key}");
    }
    let answered = stack.multi_get_reading(TABLE, &keys, wire_reads()).await?;
    let batch: Vec<Vec<_>> = answered
        .results
        .iter()
        .map(|hit| hit.values.iter().map(wire_value).collect())
        .collect();
    assert_eq!(batch, wanted);
    let after = stack.scrape().await?;
    let looked_up_twice = 2.0;
    let trimmed: Vec<Option<Vec<u8>>> = wanted.iter().map(|values| values[0].clone()).collect();
    let detail: Vec<Option<Vec<u8>>> = wanted.iter().map(|values| values[1].clone()).collect();
    assert!(
        length_of(&trimmed) < length_of(&whole),
        "the projection saves bytes"
    );
    let delta = |column| {
        let (read_after, returned_after) = value_bytes(&after, column);
        let (read_before, returned_before) = value_bytes(&before, column);
        (read_after - read_before, returned_after - returned_before)
    };
    assert_eq!(
        delta(VALUE_COLUMN),
        (
            looked_up_twice * length_of(&whole),
            looked_up_twice * length_of(&trimmed)
        )
    );
    assert_eq!(
        delta(SECOND_VALUE_COLUMN),
        (
            looked_up_twice * length_of(&detail),
            looked_up_twice * length_of(&detail)
        ),
        "a whole read counts the same in both"
    );
    Ok(())
}

/// Every field list the column's format or the protobuf range does not
/// take is `INVALID_ARGUMENT` on `Get` and `MultiGet`, before any read.
#[tokio::test(flavor = "multi_thread")]
async fn test_a_field_list_the_column_does_not_take_is_invalid_argument() -> TestResult {
    let _serial = SERIAL.lock().await;
    let mut stack = two_column_stack(ValueKind::Protobuf, ValueFormat::Protobuf).await?;
    let key = stack.fixtures[0].key(SAMPLE[0]);
    let before = stack.stats.gets();
    let cases = [
        wire_read(SECOND_VALUE_COLUMN, &[protobuf::ID_FIELD]),
        wire_read(VALUE_COLUMN, &[0]),
        wire_read(VALUE_COLUMN, &[MAX_FIELD_NUMBER + 1]),
        wire_read(VALUE_COLUMN, &[protobuf::ID_FIELD, protobuf::ID_FIELD]),
    ];
    for read in cases {
        assert_eq!(
            code_of(
                stack
                    .get_reading(TABLE, Some(wire_key(&key)), vec![read.clone()])
                    .await
            ),
            Code::InvalidArgument,
            "{read:?}"
        );
        assert_eq!(
            code_of(
                stack
                    .multi_get_reading(TABLE, std::slice::from_ref(&key), vec![read.clone()])
                    .await
            ),
            Code::InvalidArgument,
            "{read:?}"
        );
    }
    assert_eq!(
        stack.stats.gets(),
        before,
        "a refused request reads nothing"
    );
    Ok(())
}

/// A value of a protobuf column that is not protobuf fails a `Get` and a
/// `MultiGet` holding its key as `FAILED_PRECONDITION`, a `MultiGet`
/// whole beside a key that answers, counted under `projection_errors_total`
/// and under no value-bytes counter, and is served whole to a read naming
/// no fields.
#[tokio::test(flavor = "multi_thread")]
async fn test_a_malformed_value_is_failed_precondition_and_counted() -> TestResult {
    let _serial = SERIAL.lock().await;
    let mut stack = two_column_stack(ValueKind::Random, ValueFormat::Protobuf).await?;
    let fixture = &stack.fixtures[0];
    let (malformed, _) = protobuf::malformed_key(fixture)
        .expect("some random value starts with an undefined wire type");
    let malformed_value = fixture.value(&malformed);
    let absent = fixture.absent_near(SAMPLE[0]);
    let errors = |exposition: &str| {
        series_sum(
            exposition,
            metric::PROJECTION_ERRORS_TOTAL,
            &[
                (metric::LABEL_TABLE, TABLE),
                (metric::LABEL_COLUMN, VALUE_COLUMN),
            ],
        )
    };
    let before = stack.scrape().await?;
    let trimmed = || vec![wire_read(VALUE_COLUMN, &[protobuf::ID_FIELD])];
    assert_eq!(
        code_of(
            stack
                .get_reading(TABLE, Some(wire_key(&malformed)), trimmed())
                .await
        ),
        Code::FailedPrecondition
    );
    assert_eq!(
        code_of(
            stack
                .multi_get_reading(TABLE, std::slice::from_ref(&malformed), trimmed())
                .await
        ),
        Code::FailedPrecondition
    );
    let failed = stack.scrape().await?;
    assert_eq!(
        value_bytes(&failed, VALUE_COLUMN),
        value_bytes(&before, VALUE_COLUMN),
        "a failed lookup counts no bytes"
    );
    assert_eq!(
        code_of(
            stack
                .multi_get_reading(TABLE, &[absent, malformed.clone()], trimmed())
                .await
        ),
        Code::FailedPrecondition,
        "a batch fails whole beside a key that answers"
    );
    let whole = stack
        .get_naming(TABLE, Some(wire_key(&malformed)), &[VALUE_COLUMN])
        .await?;
    assert_eq!(one_wire_value(&whole.values), malformed_value);
    let after = stack.scrape().await?;
    let failed_lookups: usize = 3;
    assert_eq!(
        (errors(&after) - errors(&before)).to_bits(),
        approx::gauge_len(failed_lookups).to_bits()
    );
    Ok(())
}

/// A call held on a stalled store when the shutdown arrives keeps its
/// connection open past the drain deadline; the drain stops waiting at the
/// deadline and the run ends with `DrainOverran` instead of waiting out the
/// call. The page cache is closed on that path too, which a memory-only
/// cache gives no way to observe.
#[tokio::test(flavor = "multi_thread")]
async fn test_drain_stops_waiting_at_its_deadline() -> TestResult {
    let _serial = SERIAL.lock().await;
    let stack = Stack::start_with(
        vec![FixtureSpec::small(TABLE, KeyType::Long)],
        async |setup| {
            setup.config.server.drain_deadline = SHORT_DRAIN;
            Ok(())
        },
    )
    .await?;
    assert!(ServerConfig::default().get_deadline > SHORT_DRAIN + DEADLINE_MARGIN);
    let mut serving = stack.serving;
    serving.ready().await;
    let key = stack.fixtures[0].key(SAMPLE[0]);
    stack.s3.hold();
    let mut client = stack.client.clone();
    let in_flight = tokio::spawn(async move {
        client
            .get(GetRequest {
                table: TABLE.to_string(),
                key: Some(wire_key(&key)),
                columns: wire_columns(&[VALUE_COLUMN]),
            })
            .await
    });
    poll_until(SHUTDOWN_WAIT, "the call reaching the store", async || {
        Ok((stack.s3.held() == 1).then_some(()))
    })
    .await?;

    serving.shutdown();
    let (outcome, elapsed) = tokio::time::timeout(SHUTDOWN_WAIT, timed(serving.join())).await?;
    let Err(ServeError::DrainOverran { after }) = outcome else {
        panic!("expected the drain to overrun, got {outcome:?}");
    };
    assert_eq!(after, SHORT_DRAIN);
    assert!(
        elapsed >= SHORT_DRAIN && elapsed < SHORT_DRAIN + DEADLINE_MARGIN,
        "the run ended after {elapsed:?}, drain deadline {SHORT_DRAIN:?}"
    );
    assert!(
        !in_flight.is_finished(),
        "the drain did not wait out the call"
    );
    Ok(())
}

/// A configuration a reload test changes under the running daemon, read as
/// the daemon reads its file: each version under a digest of its own.
struct Versions {
    current: std::sync::Mutex<(u64, Config)>,
    /// Reads the daemon made.
    reads: std::sync::atomic::AtomicU64,
}

impl Versions {
    /// Version 0, `config`.
    fn new(config: Config) -> Arc<Self> {
        Arc::new(Self {
            current: std::sync::Mutex::new((0, config)),
            reads: std::sync::atomic::AtomicU64::new(0),
        })
    }

    /// What the daemon follows these versions through, from version 0.
    fn reload(self: &Arc<Self>) -> Reload {
        Reload {
            source: Arc::clone(self) as Arc<dyn ConfigSource>,
            started_from: Self::digest(0),
        }
    }

    /// Makes `config` the next version.
    fn set(&self, config: Config) {
        let mut current = self.current.lock().expect("no test panics holding it");
        *current = (current.0 + 1, config);
    }

    /// The configuration as it stands.
    fn config(&self) -> Config {
        self.current
            .lock()
            .expect("no test panics holding it")
            .1
            .clone()
    }

    /// Reads the daemon made so far.
    fn reads(&self) -> u64 {
        self.reads.load(std::sync::atomic::Ordering::SeqCst)
    }

    fn digest(version: u64) -> Digest {
        Digest::of(&version.to_le_bytes())
    }
}

impl ConfigSource for Versions {
    fn read(&self) -> std::io::Result<Reading> {
        self.reads.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let (version, config) = self
            .current
            .lock()
            .expect("no test panics holding it")
            .clone();
        Ok(Reading {
            digest: Self::digest(version),
            config: Ok(config),
        })
    }
}

/// A stack over two small fixtures, `TABLE` and `UUID_TABLE`, serving
/// `TABLE` alone and following `Versions` from there, loaded.
async fn following() -> Result<(Stack, Arc<Versions>), BoxError> {
    following_over(vec![
        FixtureSpec::small(TABLE, KeyType::Long),
        FixtureSpec::small(UUID_TABLE, KeyType::Uuid),
    ])
    .await
}

/// A stack over `specs`, serving the first alone and following `Versions`
/// from there, loaded.
async fn following_over(specs: Vec<FixtureSpec>) -> Result<(Stack, Arc<Versions>), BoxError> {
    let mut followed = None;
    let mut stack = Stack::start_with(specs, async |setup| {
        setup.config.tables.truncate(1);
        let versions = Versions::new(setup.config.clone());
        setup.options.reload = Some(versions.reload());
        followed = Some(versions);
        Ok(())
    })
    .await?;
    stack.serving.ready().await;
    let versions = followed.ok_or("the setup ran")?;
    Ok((stack, versions))
}

impl Stack {
    /// `UUID_TABLE`'s configuration as `start_with` would serve it.
    fn uuid_table(&self) -> TableConfig {
        self.fixtures[1].config(Residency::Full, false)
    }

    /// `config_reloads_total` of `result` in a scrape now; zero before the
    /// first, when the series is not yet exposed.
    async fn reloads(&self, result: ReloadResult) -> Result<f64, BoxError> {
        let (_, scrape) = self.http_get(METRICS_PATH).await?;
        Ok(series_value(
            &scrape,
            metric::CONFIG_RELOADS_TOTAL,
            &[(metric::LABEL_RESULT, &result.to_string())],
        )
        .unwrap_or(0.0))
    }

    /// `config_pending` in a scrape now.
    async fn pending(&self) -> Result<Option<f64>, BoxError> {
        let (_, scrape) = self.http_get(METRICS_PATH).await?;
        Ok(series_value(&scrape, metric::CONFIG_PENDING, &[]))
    }

    /// Waits for `config_reloads_total` of `result` to pass `before`.
    async fn reloaded(&self, result: ReloadResult, before: f64) -> Result<(), BoxError> {
        poll_until(REFRESH_WAIT, "a reload counted", async || {
            Ok((self.reloads(result).await? > before).then_some(()))
        })
        .await
    }
}

/// Polls the retry test waits for after a refusal: two whose attempts it
/// checks, and the one that shows they ended.
const POLLS_OVER_A_REFUSAL: u64 = 3;

/// A table no fixture holds: its first load fails.
fn missing_table(fixture: &Fixture) -> TableConfig {
    TableConfig {
        name: "missing".to_string(),
        source: SourceConfig::Static {
            location: format!("{S3_SCHEME}{BUCKET}/{WAREHOUSE}/missing/metadata/v1.metadata.json"),
        },
        ..fixture.config(Residency::Full, false)
    }
}

/// A reload that adds one table and removes another swaps both at once:
/// the added table answers, the removed one is `NOT_FOUND`, readiness
/// stays, and nothing is left pending.
#[tokio::test(flavor = "multi_thread")]
async fn test_reload_adds_and_removes_tables_on_request() -> TestResult {
    let _serial = SERIAL.lock().await;
    let (mut stack, versions) = following().await?;
    let before = stack.reloads(ReloadResult::Applied).await?;
    let mut next = versions.config();
    next.tables = vec![stack.uuid_table()];
    versions.set(next);
    stack.serving.reload();
    stack.reloaded(ReloadResult::Applied, before).await?;

    let uuid_key = stack.fixtures[1].key(SAMPLE[0]);
    let response = stack.get(UUID_TABLE, Some(wire_key(&uuid_key))).await?;
    assert_eq!(response.presence, i32::from(v1::Presence::Present));
    assert_eq!(code_of(stack.table_info(TABLE).await), Code::NotFound);
    let (status, _) = stack.http_get(READY_PATH).await?;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(stack.pending().await?, Some(0.0));

    // The removed table's gauges leave the exposition rather than read
    // their last values; its counters stay, and the added table's gauges
    // are there.
    let removed = (metric::LABEL_TABLE, TABLE);
    let scrape = poll_until(
        REFRESH_WAIT,
        "the removed table's gauges dropped",
        async || {
            let scrape = stack.scrape().await?;
            let left = TABLE_GAUGES
                .iter()
                .any(|name| series_value(&scrape, name, &[removed]).is_some());
            Ok((!left).then_some(scrape))
        },
    )
    .await?;
    assert!(series_value(&scrape, metric::LOOKUPS_TOTAL, &[removed]).is_some());
    for name in TABLE_GAUGES {
        let added = [(metric::LABEL_TABLE, UUID_TABLE)];
        assert!(series_value(&scrape, name, &added).is_some(), "{name}");
    }
    Ok(())
}

/// Every gauge a serving table exports under its name.
const TABLE_GAUGES: [&str; 17] = [
    metric::TABLE_STATE,
    metric::INDEX_BYTES,
    metric::FINDINGS,
    metric::GENERATION_SEQUENCE_NUMBER,
    metric::GENERATION_SWAPPED_AT_SECONDS,
    metric::GENERATION_AGE_SECONDS,
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
    metric::TABLE_OLDEST_DELETE_AGE_SECONDS,
];

/// A changed file is found on the poll, with no request.
#[tokio::test(flavor = "multi_thread")]
async fn test_reload_follows_the_file_on_the_poll() -> TestResult {
    let _serial = SERIAL.lock().await;
    let (mut stack, versions) = following().await?;
    let mut next = versions.config();
    next.tables.push(stack.uuid_table());
    versions.set(next);
    poll_until(REFRESH_WAIT, "the added table serving", async || {
        Ok(stack.table_info(UUID_TABLE).await.ok())
    })
    .await?;
    stack.table_info(TABLE).await?;
    Ok(())
}

/// A change outside the tables refuses the whole reload: the table it
/// would have added is not served, and `config_pending` reads 1.
#[tokio::test(flavor = "multi_thread")]
async fn test_reload_refuses_a_change_outside_the_tables() -> TestResult {
    let _serial = SERIAL.lock().await;
    let (mut stack, versions) = following().await?;
    let before = stack.reloads(ReloadResult::Refused).await?;
    let mut next = versions.config();
    next.tables.push(stack.uuid_table());
    next.server.max_in_flight += 1;
    versions.set(next);
    stack.serving.reload();
    stack.reloaded(ReloadResult::Refused, before).await?;

    assert_eq!(code_of(stack.table_info(UUID_TABLE).await), Code::NotFound);
    stack.table_info(TABLE).await?;
    assert_eq!(stack.pending().await?, Some(1.0));
    Ok(())
}

/// A table that fails its load refuses every change the reload carries,
/// and a poll does not try the refused file again; a request does.
#[tokio::test(flavor = "multi_thread")]
async fn test_reload_refused_for_a_failed_load_is_retried_on_request_only() -> TestResult {
    let _serial = SERIAL.lock().await;
    let (mut stack, versions) = following().await?;
    let before = stack.reloads(ReloadResult::Refused).await?;
    let mut next = versions.config();
    next.tables.push(stack.uuid_table());
    next.tables.push(missing_table(&stack.fixtures[0]));
    versions.set(next);
    stack.serving.reload();
    stack.reloaded(ReloadResult::Refused, before).await?;
    assert_eq!(code_of(stack.table_info(UUID_TABLE).await), Code::NotFound);
    stack.table_info(TABLE).await?;

    // A reader's attempts run one after another, so once three more polls
    // have read, the attempts of the first two have ended.
    let refused = stack.reloads(ReloadResult::Refused).await?;
    let reads = versions.reads();
    poll_until(REFRESH_WAIT, "three more polls", async || {
        Ok((versions.reads() >= reads + POLLS_OVER_A_REFUSAL).then_some(()))
    })
    .await?;
    assert_eq!(
        stack.reloads(ReloadResult::Refused).await?.to_bits(),
        refused.to_bits(),
        "the polls pass the refused file over"
    );
    stack.serving.reload();
    stack.reloaded(ReloadResult::Refused, refused).await?;
    Ok(())
}

/// A table whose configuration changed is refreshed as the new
/// configuration says: the old table, pinned to its metadata file, is
/// replaced by one following the table root, which the poll then advances.
/// The old table's refresh, stopped by the reload, does not end the run.
#[tokio::test(flavor = "multi_thread")]
async fn test_reload_refreshes_a_changed_table_under_its_new_configuration() -> TestResult {
    let _serial = SERIAL.lock().await;
    let (mut stack, versions) = following().await?;
    let first = stack.table_info(TABLE).await?.snapshot_id;
    let before = stack.reloads(ReloadResult::Applied).await?;
    stack.fixtures[0].write_hint().await?;
    let mut next = versions.config();
    next.tables[0].source = stack.fixtures[0].root_source();
    versions.set(next);
    stack.serving.reload();
    stack.reloaded(ReloadResult::Applied, before).await?;
    assert_eq!(
        stack.table_info(TABLE).await?.snapshot_id,
        first,
        "the changed table starts from the snapshot it served"
    );

    stack.fixtures[0]
        .append(stack.catalog.as_ref(), NEW_IDS, APPEND_PREFIX)
        .await?;
    stack.fixtures[0].write_hint().await?;
    poll_until(REFRESH_WAIT, "the changed table advancing", async || {
        let info = stack.table_info(TABLE).await?;
        Ok((info.snapshot_id != first).then_some(()))
    })
    .await?;
    let (status, _) = stack.http_get(READY_PATH).await?;
    assert_eq!(status, StatusCode::OK);
    Ok(())
}

/// A reload that changes only a table's `strict` applies to the table
/// serving in place, with no load: the same table serves the same
/// generation under the new entry, and the reload is counted applied.
#[tokio::test(flavor = "multi_thread")]
async fn test_reload_applies_a_strict_change_in_place() -> TestResult {
    let _serial = SERIAL.lock().await;
    let (mut stack, versions) = following().await?;
    let table = Arc::clone(&stack.serving.opened().tables()[0]);
    let generation = stack.generation();
    let before = stack.reloads(ReloadResult::Applied).await?;
    let mut next = versions.config();
    assert!(!next.tables[0].strict);
    next.tables[0].strict = true;
    versions.set(next);
    stack.serving.reload();
    stack.reloaded(ReloadResult::Applied, before).await?;

    let after = Arc::clone(&stack.serving.opened().tables()[0]);
    assert!(
        Arc::ptr_eq(&after, &table),
        "the table serving is retuned, not replaced"
    );
    assert!(Arc::ptr_eq(&stack.generation(), &generation));
    assert!(after.config().strict);
    stack.table_info(TABLE).await?;
    Ok(())
}

/// A reload that would make a table strict while its serving snapshot is
/// below its level is refused and counted refused, as a start under it
/// would fail: the table serves on under its entry, and `config_pending`
/// reads 1.
#[tokio::test(flavor = "multi_thread")]
async fn test_reload_refuses_a_strict_change_the_serving_snapshot_is_below() -> TestResult {
    let _serial = SERIAL.lock().await;
    let balanced = FixtureSpec {
        layout: Layout {
            dictionary: Dictionary::Value,
            ..Layout::contract()
        },
        ..FixtureSpec::small(TABLE, KeyType::Long)
    };
    let (mut stack, versions) = following_over(vec![balanced]).await?;
    let table = Arc::clone(&stack.serving.opened().tables()[0]);
    let before = stack.reloads(ReloadResult::Refused).await?;
    let mut next = versions.config();
    assert!(!next.tables[0].strict);
    next.tables[0].strict = true;
    versions.set(next);
    stack.serving.reload();
    stack.reloaded(ReloadResult::Refused, before).await?;

    assert!(Arc::ptr_eq(&stack.serving.opened().tables()[0], &table));
    assert!(!table.config().strict, "the refused entry is not taken");
    assert_eq!(stack.pending().await?, Some(1.0));
    stack.table_info(TABLE).await?;
    Ok(())
}

/// A Scan answers a key's rows as the model states them, in the direction
/// asked, with `next` set while rows remain past the page and the id of the
/// snapshot served; a page that ends at the key's last row, at the limit or
/// under it, and a key with no rows answer no `next`.
#[tokio::test(flavor = "multi_thread")]
async fn test_scan_returns_rows_next_and_the_snapshot_id() -> TestResult {
    let _serial = SERIAL.lock().await;
    let mut stack = Stack::ready_ordered_with(async |_| Ok(())).await?;
    let key = stack.ordered().key(ORDERED_IDS.start);
    let snapshot = stack.generation_of(ORDERED_TABLE).snapshot_id.0;
    let max = stack.max_scan_rows(ORDERED_TABLE).await?;

    let gets = stack.stats.gets();
    let response = stack.scan(scan_of(ORDERED_TABLE, &key, PAGE_ROWS)).await?;
    let scan = TestScan {
        limit: PAGE_ROWS,
        ..TestScan::default()
    };
    let (rows, next) = expected(stack.ordered(), &key, &scan);
    assert_eq!(rows.len(), PAGE_ROWS as usize);
    assert!(next.is_some(), "the key has rows past the page");
    assert_eq!(scan_rows_of(&response.rows), rows);
    assert_eq!(response.next, model_next(&scan, next));
    assert_eq!(response.snapshot_id, snapshot);
    assert!(stack.stats.gets() > gets, "a cold Scan reads the store");

    // Oldest first, every row, nothing left behind; at a limit of exactly
    // the key's rows, still no `next`.
    let whole = u32::try_from(ROWS_PER_KEY).expect("a row count fits u32");
    for limit in [max, whole] {
        let oldest = ScanRequest {
            direction: i32::from(v1::Direction::OldestFirst),
            ..scan_of(ORDERED_TABLE, &key, limit)
        };
        let response = stack.scan(oldest).await?;
        let scan = TestScan {
            direction: icetray_core::Direction::OldestFirst,
            limit,
            ..TestScan::default()
        };
        let (rows, next) = expected(stack.ordered(), &key, &scan);
        assert_eq!(rows.len(), ROWS_PER_KEY, "limit {limit}");
        assert_eq!(scan_rows_of(&response.rows), rows, "limit {limit}");
        assert!(next.is_none() && response.next.is_none(), "limit {limit}");
    }

    // A key no row holds.
    let absent = stack.ordered().absent_near(ORDERED_IDS.end);
    let response = stack.scan(scan_of(ORDERED_TABLE, &absent, max)).await?;
    assert!(response.rows.is_empty());
    assert!(response.next.is_none());
    assert_eq!(response.snapshot_id, snapshot);
    Ok(())
}

/// Paging by `next` over the wire, in either direction, returns every row
/// of the key once and in order: each `next` follows the page's last row,
/// itself newest first and one above it oldest first, and each request
/// passes it back, as `start` reading oldest first and as `before` reading
/// newest first, until a response carries none (RFD 025, Paging).
#[tokio::test(flavor = "multi_thread")]
async fn test_scan_paging_by_next_returns_every_row_once_in_either_direction() -> TestResult {
    let _serial = SERIAL.lock().await;
    let mut stack = Stack::ready_ordered_with(async |_| Ok(())).await?;
    let key = stack.ordered().key(ORDERED_IDS.start);
    let max = stack.max_scan_rows(ORDERED_TABLE).await?;
    for (direction, wire) in [
        (
            icetray_core::Direction::NewestFirst,
            v1::Direction::NewestFirst,
        ),
        (
            icetray_core::Direction::OldestFirst,
            v1::Direction::OldestFirst,
        ),
    ] {
        let mut request = ScanRequest {
            direction: i32::from(wire),
            ..scan_of(ORDERED_TABLE, &key, PAGE_ROWS)
        };
        let mut paged = Vec::new();
        let mut requests = 0;
        loop {
            let page = stack.scan(request.clone()).await?;
            requests += 1;
            let rows = scan_rows_of(&page.rows);
            let Some(next) = page.next else {
                paged.extend(rows);
                break;
            };
            let (last, _) = rows.last().expect("a page with a next ends at a row");
            let follows = match direction {
                icetray_core::Direction::NewestFirst => *last,
                icetray_core::Direction::OldestFirst => last + 1,
            };
            assert_eq!(
                next,
                wire_order(follows),
                "{direction:?}: next follows the last row"
            );
            paged.extend(rows);
            match direction {
                icetray_core::Direction::OldestFirst => request.start = Some(next),
                icetray_core::Direction::NewestFirst => request.before = Some(next),
            }
        }
        let whole = TestScan {
            direction,
            ..every_row(max)
        };
        let (rows, _) = expected(stack.ordered(), &key, &whole);
        assert_eq!(paged, rows, "{direction:?}");
        assert_eq!(
            requests,
            ROWS_PER_KEY.div_ceil(PAGE_ROWS as usize),
            "{direction:?}: no page past the last row"
        );
    }
    Ok(())
}

/// A Scan that sets no direction, or one the contract does not name, is
/// `INVALID_ARGUMENT` before any read.
#[tokio::test(flavor = "multi_thread")]
async fn test_scan_with_unspecified_direction_is_invalid_argument() -> TestResult {
    let _serial = SERIAL.lock().await;
    let mut stack = Stack::ready_ordered_with(async |_| Ok(())).await?;
    let key = stack.ordered().key(ORDERED_IDS.start);
    let gets = stack.stats.gets();
    for direction in [i32::from(v1::Direction::Unspecified), UNKNOWN_DIRECTION] {
        let scan = ScanRequest {
            direction,
            ..scan_of(ORDERED_TABLE, &key, PAGE_ROWS)
        };
        assert_eq!(
            code_of(stack.scan(scan).await),
            Code::InvalidArgument,
            "{direction}"
        );
        let multi = MultiScanRequest {
            direction,
            ..multi_scan_of(ORDERED_TABLE, std::slice::from_ref(&key), PAGE_ROWS)
        };
        assert_eq!(
            code_of(stack.multi_scan(multi).await),
            Code::InvalidArgument,
            "{direction}"
        );
    }
    assert_eq!(stack.stats.gets(), gets, "a refused Scan issues no GET");
    Ok(())
}

/// A limit of 0 and a limit above the table's `max_scan_rows`, as
/// `TableInfo` reports it, are `INVALID_ARGUMENT` before any read, for a
/// Scan and for a `MultiScan`; a limit of exactly `max_scan_rows` is
/// answered.
#[tokio::test(flavor = "multi_thread")]
async fn test_scan_with_a_limit_of_zero_or_above_max_scan_rows_is_invalid_argument() -> TestResult {
    let _serial = SERIAL.lock().await;
    let mut stack = Stack::ready_ordered_with(async |setup| {
        setup.config.tables[1].max_scan_rows = Some(SCAN_CAP);
        Ok(())
    })
    .await?;
    let key = stack.ordered().key(ORDERED_IDS.start);
    let max = stack.max_scan_rows(ORDERED_TABLE).await?;
    assert_eq!(max, SCAN_CAP);

    let gets = stack.stats.gets();
    for limit in [0, max + 1] {
        let scan = scan_of(ORDERED_TABLE, &key, limit);
        assert_eq!(
            code_of(stack.scan(scan).await),
            Code::InvalidArgument,
            "Scan limit {limit}"
        );
        let multi = multi_scan_of(ORDERED_TABLE, std::slice::from_ref(&key), limit);
        assert_eq!(
            code_of(stack.multi_scan(multi).await),
            Code::InvalidArgument,
            "MultiScan limit {limit}"
        );
    }
    assert_eq!(stack.stats.gets(), gets, "a refused Scan issues no GET");

    let response = stack.scan(scan_of(ORDERED_TABLE, &key, max)).await?;
    let (rows, next) = expected(stack.ordered(), &key, &every_row(max));
    assert_eq!(rows.len(), SCAN_CAP as usize);
    assert_eq!(scan_rows_of(&response.rows), rows);
    assert!(next.is_some());
    assert_eq!(response.next, model_next(&every_row(max), next));
    Ok(())
}

/// A `start` at or above `before`, an empty window, a bound of another
/// variant than the order type takes, and a bound that sets no variant are
/// `INVALID_ARGUMENT` before any read, for a Scan and a `MultiScan` alike; a window in order is answered, its
/// `start` row included and its `before` row left out.
#[tokio::test(flavor = "multi_thread")]
async fn test_scan_with_an_empty_window_is_invalid_argument() -> TestResult {
    let _serial = SERIAL.lock().await;
    let mut stack = Stack::ready_ordered_with(async |_| Ok(())).await?;
    let key = stack.ordered().key(ORDERED_IDS.start);
    let max = stack.max_scan_rows(ORDERED_TABLE).await?;
    let (low, high) = (order_at(1), order_at(5));
    let wrong_variant = v1::OrderValue {
        value: Some(v1::order_value::Value::Str("not a long".to_string())),
    };
    let no_variant = v1::OrderValue { value: None };
    let gets = stack.stats.gets();
    let refused = [
        (Some(wire_order(high)), Some(wire_order(low))),
        (Some(wire_order(low)), Some(wire_order(low))),
        (Some(wrong_variant.clone()), None),
        (None, Some(wrong_variant)),
        (Some(no_variant.clone()), None),
        (None, Some(no_variant)),
    ];
    for (start, before) in refused {
        let request = ScanRequest {
            start: start.clone(),
            before: before.clone(),
            ..scan_of(ORDERED_TABLE, &key, max)
        };
        assert_eq!(
            code_of(stack.scan(request).await),
            Code::InvalidArgument,
            "Scan {start:?} {before:?}"
        );
        let multi = MultiScanRequest {
            start: start.clone(),
            before: before.clone(),
            ..multi_scan_of(ORDERED_TABLE, std::slice::from_ref(&key), max)
        };
        assert_eq!(
            code_of(stack.multi_scan(multi).await),
            Code::InvalidArgument,
            "MultiScan {start:?} {before:?}"
        );
    }
    assert_eq!(stack.stats.gets(), gets, "a refused call issues no GET");

    let request = ScanRequest {
        start: Some(wire_order(low)),
        before: Some(wire_order(high)),
        ..scan_of(ORDERED_TABLE, &key, max)
    };
    let response = stack.scan(request).await?;
    let scan = TestScan {
        start: Some(low),
        before: Some(high),
        ..every_row(max)
    };
    let (rows, next) = expected(stack.ordered(), &key, &scan);
    let orders: Vec<i64> = rows.iter().map(|(order, _)| *order).collect();
    assert_eq!(
        orders.last(),
        Some(&low),
        "newest first, the start row is last"
    );
    assert!(!orders.contains(&high), "the before row is left out");
    assert_eq!(scan_rows_of(&response.rows), rows);
    assert_eq!(response.next, model_next(&scan, next));
    Ok(())
}

/// A Scan or `MultiScan` of a key-value table is `INVALID_ARGUMENT`.
#[tokio::test(flavor = "multi_thread")]
async fn test_scan_on_a_key_value_table_is_invalid_argument() -> TestResult {
    let _serial = SERIAL.lock().await;
    let mut stack = Stack::ready_ordered_with(async |_| Ok(())).await?;
    let key = stack.fixtures[0].key(SAMPLE[0]);
    assert_eq!(
        code_of(stack.scan(scan_of(TABLE, &key, PAGE_ROWS)).await),
        Code::InvalidArgument
    );
    assert_eq!(
        code_of(
            stack
                .multi_scan(multi_scan_of(TABLE, std::slice::from_ref(&key), PAGE_ROWS))
                .await
        ),
        Code::InvalidArgument
    );
    Ok(())
}

/// A Get or `MultiGet` of an ordered table is `INVALID_ARGUMENT`, and the
/// key-value table beside it still answers.
#[tokio::test(flavor = "multi_thread")]
async fn test_get_on_an_ordered_table_is_invalid_argument() -> TestResult {
    let _serial = SERIAL.lock().await;
    let mut stack = Stack::ready_ordered_with(async |_| Ok(())).await?;
    let key = stack.ordered().key(ORDERED_IDS.start);
    assert_eq!(
        code_of(stack.get(ORDERED_TABLE, Some(wire_key(&key))).await),
        Code::InvalidArgument
    );
    assert_eq!(
        code_of(
            stack
                .multi_get(ORDERED_TABLE, std::slice::from_ref(&key))
                .await
        ),
        Code::InvalidArgument
    );
    let kv = stack.fixtures[0].key(SAMPLE[0]);
    let response = stack.get(TABLE, Some(wire_key(&kv))).await?;
    assert_eq!(response.presence, i32::from(v1::Presence::Present));
    Ok(())
}

/// A request of any of the four lookup calls, to send as it is.
#[derive(Clone, Debug)]
enum Sent {
    /// A `Get`.
    Get(GetRequest),
    /// A `MultiGet`.
    MultiGet(MultiGetRequest),
    /// A Scan.
    Scan(ScanRequest),
    /// A `MultiScan`.
    MultiScan(MultiScanRequest),
}

impl Stack {
    /// The code `call` is answered with, `Code::Ok` for a response.
    async fn answered(&mut self, call: Sent) -> Code {
        fn code<T>(result: Result<T, Status>) -> Code {
            result.map_or_else(|status| status.code(), |_| Code::Ok)
        }
        match call {
            Sent::Get(request) => code(self.client.get(request).await),
            Sent::MultiGet(request) => code(self.client.multi_get(request).await),
            Sent::Scan(request) => code(self.scan(request).await),
            Sent::MultiScan(request) => code(self.multi_scan(request).await),
        }
    }
}

/// A `MultiGet` of `keys` on `table` naming `VALUE_COLUMN`.
fn multi_get_of(table: &str, keys: &[Key]) -> MultiGetRequest {
    MultiGetRequest {
        table: table.to_string(),
        keys: keys.iter().map(wire_key).collect(),
        columns: wire_columns(&[VALUE_COLUMN]),
    }
}

/// Every way a request is wrong for the table it names that the service
/// refuses over the wire is `INVALID_ARGUMENT` and reads nothing: a bound of
/// another variant than the order type takes, in either position, and one
/// that sets no variant; a `MultiGet` of an ordered table; a `MultiScan` of
/// a key-value table; a Scan naming a column the table does not serve or
/// naming one twice; and a `MultiScan` key of another variant than the key
/// type.
#[tokio::test(flavor = "multi_thread")]
async fn test_scan_and_multi_get_requests_the_table_refuses_are_invalid_argument() -> TestResult {
    let _serial = SERIAL.lock().await;
    let mut stack = Stack::ready_ordered_with(async |_| Ok(())).await?;
    let key = stack.ordered().key(ORDERED_IDS.start);
    let kv_key = stack.fixtures[0].key(SAMPLE[0]);
    let str_bound = v1::OrderValue {
        value: Some(v1::order_value::Value::Str("not a long".to_string())),
    };
    let empty_bound = v1::OrderValue { value: None };
    let cases = vec![
        Sent::Scan(ScanRequest {
            start: Some(str_bound.clone()),
            ..scan_of(ORDERED_TABLE, &key, PAGE_ROWS)
        }),
        Sent::Scan(ScanRequest {
            before: Some(str_bound.clone()),
            ..scan_of(ORDERED_TABLE, &key, PAGE_ROWS)
        }),
        Sent::MultiScan(MultiScanRequest {
            start: Some(str_bound),
            ..multi_scan_of(ORDERED_TABLE, std::slice::from_ref(&key), PAGE_ROWS)
        }),
        Sent::Scan(ScanRequest {
            start: Some(empty_bound.clone()),
            ..scan_of(ORDERED_TABLE, &key, PAGE_ROWS)
        }),
        Sent::Scan(ScanRequest {
            before: Some(empty_bound.clone()),
            ..scan_of(ORDERED_TABLE, &key, PAGE_ROWS)
        }),
        Sent::MultiScan(MultiScanRequest {
            before: Some(empty_bound),
            ..multi_scan_of(ORDERED_TABLE, std::slice::from_ref(&key), PAGE_ROWS)
        }),
        Sent::MultiGet(multi_get_of(ORDERED_TABLE, std::slice::from_ref(&key))),
        Sent::MultiScan(multi_scan_of(
            TABLE,
            std::slice::from_ref(&kv_key),
            PAGE_ROWS,
        )),
        Sent::Scan(ScanRequest {
            columns: wire_columns(&[UNSERVED_COLUMN]),
            ..scan_of(ORDERED_TABLE, &key, PAGE_ROWS)
        }),
        Sent::Scan(ScanRequest {
            columns: wire_columns(&[VALUE_COLUMN, VALUE_COLUMN]),
            ..scan_of(ORDERED_TABLE, &key, PAGE_ROWS)
        }),
        Sent::MultiScan(MultiScanRequest {
            columns: wire_columns(&[UNSERVED_COLUMN]),
            ..multi_scan_of(ORDERED_TABLE, std::slice::from_ref(&key), PAGE_ROWS)
        }),
        Sent::MultiScan(MultiScanRequest {
            keys: vec![
                wire_key(&key),
                v1::Key {
                    key: Some(v1::key::Key::Str("not a long".to_string())),
                },
            ],
            ..multi_scan_of(ORDERED_TABLE, std::slice::from_ref(&key), PAGE_ROWS)
        }),
    ];
    let gets = stack.stats.gets();
    for (at, case) in cases.into_iter().enumerate() {
        assert_eq!(
            stack.answered(case).await,
            Code::InvalidArgument,
            "case {at}"
        );
    }
    assert_eq!(stack.stats.gets(), gets, "a refused call issues no GET");
    Ok(())
}

/// The four lookup calls of the validation-order test.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Lookup {
    Get,
    MultiGet,
    Scan,
    MultiScan,
}

impl Lookup {
    const ALL: [Self; 4] = [Self::Get, Self::MultiGet, Self::Scan, Self::MultiScan];

    /// Whether the call carries many keys.
    fn batch(self) -> bool {
        matches!(self, Self::MultiGet | Self::MultiScan)
    }

    /// Whether the call reads an ordered table, with a direction, a limit,
    /// and bounds.
    fn scans(self) -> bool {
        matches!(self, Self::Scan | Self::MultiScan)
    }
}

/// The state of the table a request of the validation-order test names.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Target {
    /// A name no table is served under.
    Unknown,
    /// A served table whose first load has not finished.
    Loading,
    /// A served table that has loaded.
    Served,
}

/// The step of the service's validation order a request's fault is found
/// at (`service.rs`): the slot and the readiness steps are the server's
/// and the table's state, not the request's.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Step {
    /// What the request holds on its own, whatever its table.
    Request,
    /// What the table's configuration holds the request to.
    Configuration,
    /// What needs the serving generation.
    Generation,
}

/// A fault the validation-order test puts in an otherwise good request.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Fault {
    /// No fault: the request a served table answers.
    None,
    MissingKey,
    UnsetDirection,
    UnknownDirection,
    BoundWithNoVariant,
    EmptyWindow,
    RepeatedColumn,
    FieldOutOfRange,
    RepeatedField,
    WrongKind,
    LimitZero,
    LimitAboveMax,
    TooManyValues,
    KeyVariant,
    UuidLength,
    BoundVariant,
    UnservedColumn,
    FieldsOfAnOpaqueColumn,
}

impl Fault {
    const ALL: [Self; 18] = [
        Self::None,
        Self::MissingKey,
        Self::UnsetDirection,
        Self::UnknownDirection,
        Self::BoundWithNoVariant,
        Self::EmptyWindow,
        Self::RepeatedColumn,
        Self::FieldOutOfRange,
        Self::RepeatedField,
        Self::WrongKind,
        Self::LimitZero,
        Self::LimitAboveMax,
        Self::TooManyValues,
        Self::KeyVariant,
        Self::UuidLength,
        Self::BoundVariant,
        Self::UnservedColumn,
        Self::FieldsOfAnOpaqueColumn,
    ];

    /// The step the fault is found at; `None` for no fault.
    fn step(self) -> Option<Step> {
        match self {
            Self::None => None,
            Self::MissingKey
            | Self::UnsetDirection
            | Self::UnknownDirection
            | Self::BoundWithNoVariant
            | Self::LimitZero
            | Self::EmptyWindow
            | Self::RepeatedColumn
            | Self::FieldOutOfRange
            | Self::RepeatedField => Some(Step::Request),
            Self::WrongKind | Self::LimitAboveMax | Self::TooManyValues => {
                Some(Step::Configuration)
            }
            Self::KeyVariant
            | Self::UuidLength
            | Self::BoundVariant
            | Self::UnservedColumn
            | Self::FieldsOfAnOpaqueColumn => Some(Step::Generation),
        }
    }

    /// Whether a request of `kind` can hold the fault. A uuid key's length
    /// is a fault on a uuid-keyed table alone, which the test serves as a
    /// key-value table, so only a `Get` and a `MultiGet` hold it.
    fn applies_to(self, kind: Lookup) -> bool {
        match self {
            Self::None
            | Self::MissingKey
            | Self::RepeatedColumn
            | Self::FieldOutOfRange
            | Self::RepeatedField
            | Self::WrongKind
            | Self::KeyVariant
            | Self::UnservedColumn
            | Self::FieldsOfAnOpaqueColumn => true,
            Self::UnsetDirection
            | Self::UnknownDirection
            | Self::BoundWithNoVariant
            | Self::EmptyWindow
            | Self::LimitZero
            | Self::LimitAboveMax
            | Self::BoundVariant => kind.scans(),
            Self::TooManyValues => kind.batch(),
            Self::UuidLength => !kind.scans(),
        }
    }

    /// The code a request holding the fault is answered with when it names
    /// a table in `target` on a server with a slot free, or with none
    /// (`slot`): the code of the earliest step that finds a fault, the
    /// unknown table being the configuration step's, the slot step's
    /// `RESOURCE_EXHAUSTED`, and the loading table the readiness step's.
    fn answer(self, target: Target, slot: bool) -> Code {
        let step = self.step();
        let steps = [
            (step == Some(Step::Request), Code::InvalidArgument),
            (target == Target::Unknown, Code::NotFound),
            (step == Some(Step::Configuration), Code::InvalidArgument),
            (!slot, Code::ResourceExhausted),
            (target == Target::Loading, Code::Unavailable),
            (step == Some(Step::Generation), Code::InvalidArgument),
        ];
        steps
            .into_iter()
            .find_map(|(found, code)| found.then_some(code))
            .unwrap_or(Code::Ok)
    }

    /// `good`, a request of `kind` that a served table answers, with the
    /// fault put in; `other` is the table of the other kind. A uuid key's
    /// length moves a request on a served table to `UUID_TABLE`, its one
    /// key a `bin` too short for a uuid, so the request holds no other
    /// fault; on an unknown table it keeps the name.
    fn put_in(self, good: &Parts, kind: Lookup, other: &str) -> Parts {
        let mut parts = good.clone();
        let mut bad_key = |key: v1::Key| {
            if kind.batch() {
                parts.keys.push(key);
            } else {
                parts.keys = vec![key];
            }
        };
        match self {
            Self::None => {}
            Self::MissingKey => bad_key(v1::Key { key: None }),
            Self::KeyVariant => bad_key(v1::Key {
                key: Some(v1::key::Key::Str("not a long".to_string())),
            }),
            Self::UuidLength => {
                if parts.table != NOBODY {
                    parts.table = UUID_TABLE.to_string();
                }
                parts.keys = vec![v1::Key {
                    key: Some(v1::key::Key::Bin(vec![0; SHORT_UUID_BYTES])),
                }];
            }
            Self::UnsetDirection => parts.direction = i32::from(v1::Direction::Unspecified),
            Self::UnknownDirection => parts.direction = UNKNOWN_DIRECTION,
            Self::BoundWithNoVariant => parts.start = Some(v1::OrderValue { value: None }),
            Self::EmptyWindow => {
                parts.start = Some(wire_order(ORDER_START));
                parts.before = Some(wire_order(ORDER_START));
            }
            Self::BoundVariant => {
                parts.start = Some(v1::OrderValue {
                    value: Some(v1::order_value::Value::Str("not a long".to_string())),
                });
            }
            Self::RepeatedColumn => parts.columns = wire_columns(&[VALUE_COLUMN, VALUE_COLUMN]),
            Self::FieldOutOfRange => parts.columns = vec![wire_read(VALUE_COLUMN, &[0])],
            Self::RepeatedField => parts.columns = vec![wire_read(VALUE_COLUMN, &[1, 1])],
            Self::UnservedColumn => parts.columns = wire_columns(&[UNSERVED_COLUMN]),
            Self::FieldsOfAnOpaqueColumn => parts.columns = vec![wire_read(VALUE_COLUMN, &[1])],
            Self::WrongKind => parts.table = other.to_string(),
            Self::LimitZero => parts.limit = 0,
            Self::LimitAboveMax => parts.limit = SCAN_CAP + 1,
            Self::TooManyValues => {
                let keys = match kind {
                    Lookup::MultiGet => BATCH_IDS + 1,
                    Lookup::Get | Lookup::Scan | Lookup::MultiScan => ORDER_TEST_SCAN_KEYS_OVER,
                };
                parts.keys = vec![parts.keys[0].clone(); keys];
            }
        }
        parts
    }
}

/// The length of a `bin` key the validation-order test sends a uuid-keyed
/// table: fewer bytes than a uuid's.
const SHORT_UUID_BYTES: usize = 3;

/// The keys a `MultiScan` of the validation-order test asks for to pass
/// `MULTI_SCAN_VALUES` at `PAGE_ROWS` rows of one column and the order
/// value.
const ORDER_TEST_SCAN_KEYS_OVER: usize = MULTI_SCAN_VALUES / (PAGE_ROWS as usize * 2) + 1;

/// What a request of the validation-order test carries, each call taking
/// the fields it has: a `Get` or Scan the first key.
#[derive(Clone, Debug)]
struct Parts {
    table: String,
    keys: Vec<v1::Key>,
    columns: Vec<v1::ColumnRead>,
    direction: i32,
    limit: u32,
    start: Option<v1::OrderValue>,
    before: Option<v1::OrderValue>,
}

impl Parts {
    /// The parts of a request on `table` for `key` that a served table of
    /// the call's kind answers: `VALUE_COLUMN`, newest first, `PAGE_ROWS`
    /// rows, unbounded.
    fn good(table: &str, key: &Key) -> Self {
        Self {
            table: table.to_string(),
            keys: vec![wire_key(key)],
            columns: wire_columns(&[VALUE_COLUMN]),
            direction: i32::from(v1::Direction::NewestFirst),
            limit: PAGE_ROWS,
            start: None,
            before: None,
        }
    }

    /// The request of `kind` these parts make.
    fn request(self, kind: Lookup) -> Sent {
        let Self {
            table,
            keys,
            columns,
            direction,
            limit,
            start,
            before,
        } = self;
        let key = keys.first().cloned();
        match kind {
            Lookup::Get => Sent::Get(GetRequest {
                table,
                key,
                columns,
            }),
            Lookup::MultiGet => Sent::MultiGet(MultiGetRequest {
                table,
                keys,
                columns,
            }),
            Lookup::Scan => Sent::Scan(ScanRequest {
                table,
                key,
                columns,
                direction,
                limit,
                start,
                before,
            }),
            Lookup::MultiScan => Sent::MultiScan(MultiScanRequest {
                table,
                keys,
                columns,
                direction,
                limit,
                start,
                before,
            }),
        }
    }
}

/// `Get`, `MultiGet`, `Scan`, and `MultiScan` check a request in one order
/// (`service.rs`): its own faults, then its table's configuration (the
/// unknown table among them), then a slot, then the table's readiness, then
/// the generation. Every fault of each step, in every call it applies to,
/// is sent to an unknown table, a loading one, and a served one, on a
/// server with slots free and on one with none, and each is answered the
/// code of the earliest step that finds a fault. A server with no slot free
/// is one configured with a `max_in_flight` of 0, which `Config::parse`
/// refuses and `Setup` reaches, so no held call has to fill the slots; a
/// loading table is one whose store is held from the start.
#[tokio::test(flavor = "multi_thread")]
async fn test_each_call_checks_its_request_in_the_one_order() -> TestResult {
    let _serial = SERIAL.lock().await;
    let mut sent = 0;
    for (target, slot) in [
        (Target::Loading, true),
        (Target::Served, true),
        (Target::Loading, false),
        (Target::Served, false),
    ] {
        let mut stack = Stack::start_with(
            vec![
                FixtureSpec::small(TABLE, KeyType::Long),
                ordered_spec(ORDERED_TABLE),
                FixtureSpec::small(UUID_TABLE, KeyType::Uuid),
            ],
            async |setup| {
                setup.config.tables[1].max_scan_rows = Some(SCAN_CAP);
                setup.config.server.max_multi_get_values = BATCH_IDS;
                setup.config.server.max_multi_scan_values = MULTI_SCAN_VALUES;
                if !slot {
                    setup.config.server.max_in_flight = 0;
                }
                setup.hold_store = target == Target::Loading;
                Ok(())
            },
        )
        .await?;
        if target == Target::Served {
            stack.serving.ready().await;
        } else {
            assert_eq!(
                stack.health_status(OVERALL_SERVICE).await?,
                ServingStatus::NotServing,
                "the tables are loading"
            );
        }
        let kv_key = stack.fixtures[0].key(SAMPLE[0]);
        let ordered_key = stack.ordered().key(ORDERED_IDS.start);
        for named in [Target::Unknown, target] {
            for kind in Lookup::ALL {
                let (own, other, key) = match (named, kind.scans()) {
                    (Target::Unknown, false) => (NOBODY, NOBODY, &kv_key),
                    (Target::Unknown, true) => (NOBODY, NOBODY, &ordered_key),
                    (Target::Loading | Target::Served, false) => (TABLE, ORDERED_TABLE, &kv_key),
                    (Target::Loading | Target::Served, true) => {
                        (ORDERED_TABLE, TABLE, &ordered_key)
                    }
                };
                let good = Parts::good(own, key);
                for fault in Fault::ALL {
                    if !fault.applies_to(kind) {
                        continue;
                    }
                    let request = fault.put_in(&good, kind, other).request(kind);
                    assert_eq!(
                        stack.answered(request.clone()).await,
                        fault.answer(named, slot),
                        "{fault:?} to {named:?}, a slot free: {slot}, {request:?}"
                    );
                    sent += 1;
                }
            }
        }
        if target == Target::Loading {
            stack.s3.release();
        }
    }
    // Counting no fault: 10 for a `Get`, 11 for a `MultiGet`, which can ask
    // for too many values, 16 for a Scan, which has a direction, a limit,
    // and bounds but reads no uuid-keyed table, and 17 for a `MultiScan`.
    let cases_per_target = 10 + 11 + 16 + 17;
    let servers = 4;
    let targets_per_server = 2;
    assert_eq!(
        sent,
        servers * targets_per_server * cases_per_target,
        "every case was sent"
    );
    Ok(())
}

/// A `MultiScan` whose keys times its limit times one more than its columns
/// pass `max_multi_scan_values` is `INVALID_ARGUMENT` before any read; one
/// at the bound is answered.
#[tokio::test(flavor = "multi_thread")]
async fn test_multi_scan_over_max_values_is_invalid_argument_before_any_read() -> TestResult {
    let _serial = SERIAL.lock().await;
    let mut stack = Stack::ready_ordered_with(async |setup| {
        setup.config.tables[1].max_scan_rows = Some(SCAN_CAP);
        setup.config.server.max_multi_scan_values = MULTI_SCAN_VALUES;
        Ok(())
    })
    .await?;
    let keys: Vec<Key> = (ORDERED_IDS.start..)
        .take(2 + SCAN_KEYS_OVER)
        .map(|id| stack.ordered().key(id))
        .collect();
    let fits = &keys[..2];

    let max = stack.max_scan_rows(ORDERED_TABLE).await?;
    let gets = stack.stats.gets();
    let over = multi_scan_of(ORDERED_TABLE, &keys, max);
    assert_eq!(code_of(stack.multi_scan(over).await), Code::InvalidArgument);
    assert_eq!(stack.stats.gets(), gets, "a refused call issues no GET");

    let response = stack
        .multi_scan(multi_scan_of(ORDERED_TABLE, fits, max))
        .await?;
    assert_eq!(response.results.len(), fits.len());
    for (key, result) in fits.iter().zip(&response.results) {
        let (rows, next) = expected(stack.ordered(), key, &every_row(max));
        assert_eq!(scan_rows_of(&result.rows), rows, "{key}");
        assert_eq!(result.next, model_next(&every_row(max), next), "{key}");
    }
    assert!(stack.stats.gets() > gets, "the calls at the bound read");
    assert_eq!(
        stack.table_info(ORDERED_TABLE).await?.max_multi_scan_values,
        approx::count(MULTI_SCAN_VALUES),
        "a caller learns the bound it was held to"
    );
    Ok(())
}

/// A `MultiScan` answers one result per key in request order from one
/// snapshot, and a key listed twice is answered at each position.
#[tokio::test(flavor = "multi_thread")]
async fn test_multi_scan_answers_a_repeated_key_at_each_position() -> TestResult {
    let _serial = SERIAL.lock().await;
    let mut stack = Stack::ready_ordered_with(async |_| Ok(())).await?;
    let first = stack.ordered().key(ORDERED_IDS.start);
    let second = stack.ordered().key(ORDERED_IDS.start + 1);
    let absent = stack.ordered().absent_near(ORDERED_IDS.end);
    let keys = [first.clone(), second.clone(), first.clone(), absent.clone()];
    let snapshot = stack.generation_of(ORDERED_TABLE).snapshot_id.0;

    let request = multi_scan_of(ORDERED_TABLE, &keys, PAGE_ROWS);
    let response = stack.multi_scan(request).await?;
    assert_eq!(response.snapshot_id, snapshot);
    assert_eq!(response.results.len(), keys.len());
    let scan = TestScan {
        limit: PAGE_ROWS,
        ..TestScan::default()
    };
    for (position, (key, result)) in keys.iter().zip(&response.results).enumerate() {
        let (rows, next) = expected(stack.ordered(), key, &scan);
        assert_eq!(scan_rows_of(&result.rows), rows, "position {position}");
        assert_eq!(result.next, model_next(&scan, next), "position {position}");
    }
    assert_eq!(response.results[0], response.results[2]);
    assert_ne!(response.results[0], response.results[1]);
    assert!(response.results[3].rows.is_empty());
    assert!(response.results[3].next.is_none());
    Ok(())
}

/// The distinct keys of the `MultiScan` the response-bound test sends: two
/// with rows and one with none, the first listed twice.
const BOUND_TEST_DISTINCT_KEYS: u64 = 3;

/// `scans_total` increments, in `ScanResult::ALL` order, of a `Scan` sent
/// whole: one key with rows.
const SCAN_SENT: [u64; 3] = [1, 0, 0];

/// Of a refused `Scan`: its one key an error.
const SCAN_REFUSED: [u64; 3] = [0, 0, 1];

/// Of the `MultiScan` sent whole: two keys with rows, one with none.
const MULTI_SCAN_SENT: [u64; 3] = [2, 1, 0];

/// Of the refused `MultiScan`: each distinct key an error, none answered.
const MULTI_SCAN_REFUSED: [u64; 3] = [0, 0, BOUND_TEST_DISTINCT_KEYS];

/// The scan the response-bound tests send: every row of a key, newest
/// first.
fn whole_key() -> TestScan {
    TestScan {
        limit: u32::try_from(ROWS_PER_KEY).expect("a row count fits u32"),
        ..TestScan::default()
    }
}

/// `rows` as the server sends them, built from the fixture model's own
/// rows rather than from the server's conversion.
fn model_wire_rows(rows: ScanRows) -> Vec<v1::ScanRow> {
    rows.into_iter()
        .map(|(order, values)| v1::ScanRow {
            order: Some(wire_order(order)),
            values: values
                .into_iter()
                .map(|value| v1::ColumnValue {
                    kind: Some(match value {
                        Some(bytes) => v1::column_value::Kind::Value(bytes.into()),
                        None => v1::column_value::Kind::NullValue(v1::NullValue {}),
                    }),
                })
                .collect(),
        })
        .collect()
}

/// The response the model says a Scan of `key` in `fixture` answers under
/// `whole_key`, from the snapshot the fixture's table holds.
fn model_scan_response(fixture: &Fixture, key: &Key) -> ScanResponse {
    let (rows, next) = expected(fixture, key, &whole_key());
    ScanResponse {
        rows: model_wire_rows(rows),
        next: model_next(&whole_key(), next),
        snapshot_id: fixture.snapshot_id().expect("a committed table"),
    }
}

/// The response the model says a `MultiScan` of `keys` answers under
/// `whole_key`, one result per position.
fn model_multi_scan_response(fixture: &Fixture, keys: &[Key]) -> MultiScanResponse {
    MultiScanResponse {
        results: keys
            .iter()
            .map(|key| {
                let (rows, next) = expected(fixture, key, &whole_key());
                v1::ScanResult {
                    rows: model_wire_rows(rows),
                    next: model_next(&whole_key(), next),
                }
            })
            .collect(),
        snapshot_id: fixture.snapshot_id().expect("a committed table"),
    }
}

/// The `scans_total` of `ORDERED_TABLE` under `result` in `exposition`.
fn scans_counted(exposition: &str, result: ScanResult) -> f64 {
    series_sum(
        exposition,
        metric::SCANS_TOTAL,
        &[
            (metric::LABEL_TABLE, ORDERED_TABLE),
            (metric::LABEL_RESULT, &result.to_string()),
        ],
    )
}

/// What `scan_response_bytes` of `ORDERED_TABLE` holds in `exposition`: its
/// samples and their sum in bytes.
fn response_bytes(exposition: &str) -> (f64, f64) {
    let read = |suffix: &str| {
        series_sum(
            exposition,
            &format!("{}_{suffix}", metric::SCAN_RESPONSE_BYTES),
            &[(metric::LABEL_TABLE, ORDERED_TABLE)],
        )
    };
    (read("count"), read("sum"))
}

/// Asserts that `later` counts `expected` more Scans of `ORDERED_TABLE`
/// under each of `ScanResult`'s results than `earlier`, in `ScanResult::ALL`
/// order.
fn assert_scans_counted(earlier: &str, later: &str, expected: [u64; 3]) {
    for (result, expected) in ScanResult::ALL.into_iter().zip(expected) {
        let added = scans_counted(later, result) - scans_counted(earlier, result);
        assert_eq!(
            added.to_bits(),
            approx::gauge(expected).to_bits(),
            "{result}: {added} counted, {expected} expected"
        );
    }
}

/// Asserts what `later` counts beyond `earlier` for one `call` whose
/// response of `size` bytes the bound sent, `over` zero, or refused, `over`
/// one: one sample of `size` bytes in the response sizes either way; the
/// Scans of `scans`, sent or refused, by `ScanResult`; and the call `OK`,
/// or `FAILED_PRECONDITION` and not `OK`.
fn assert_bound_counted(
    earlier: &str,
    later: &str,
    over: usize,
    size: usize,
    call: Call,
    [sent, refused]: [[u64; 3]; 2],
) {
    let (samples, bytes) = response_bytes(later);
    let (samples_before, bytes_before) = response_bytes(earlier);
    assert_eq!(
        (samples - samples_before).to_bits(),
        approx::gauge(1).to_bits(),
        "over {over}: one sample"
    );
    assert_eq!(
        (bytes - bytes_before).to_bits(),
        approx::gauge_len(size).to_bits(),
        "over {over}: the size of the response, sent or refused"
    );
    if over == 0 {
        assert_scans_counted(earlier, later, sent);
        assert_counted(earlier, later, call, Code::Ok, 1);
    } else {
        assert_scans_counted(earlier, later, refused);
        assert_counted(earlier, later, call, Code::FailedPrecondition, 1);
        assert_counted(earlier, later, call, Code::Ok, 0);
    }
}

/// A Scan whose response encodes to exactly `server.max_scan_bytes` is
/// sent, and one a single byte over is `FAILED_PRECONDITION` with no rows:
/// the call counts as an `error` in `scans_total` and not as `rows`, the
/// response's size is recorded either way, and the code is counted in
/// `requests_total`. The sizes come from the fixture model's rows, whose
/// values the table's writer gave known lengths.
#[tokio::test(flavor = "multi_thread")]
async fn test_scan_response_at_the_bound_is_sent_and_one_byte_over_is_refused() -> TestResult {
    let _serial = SERIAL.lock().await;
    for over in [0, 1] {
        let mut size = 0;
        let mut stack = Stack::ready_ordered_with(async |setup| {
            let key = setup.fixtures[1].key(ORDERED_IDS.start);
            size = model_scan_response(&setup.fixtures[1], &key).encoded_len();
            setup.config.server.max_scan_bytes = size - over;
            Ok(())
        })
        .await?;
        let key = stack.ordered().key(ORDERED_IDS.start);
        let request = scan_of(ORDERED_TABLE, &key, whole_key().limit);
        let before = stack.scrape().await?;
        let answered = stack.scan(request).await;
        let after = stack.scrape().await?;

        let scans = [SCAN_SENT, SCAN_REFUSED];
        assert_bound_counted(&before, &after, over, size, Call::Scan, scans);
        if over == 0 {
            let response = answered?;
            assert_eq!(response, model_scan_response(stack.ordered(), &key));
            assert_eq!(response.encoded_len(), size);
        } else {
            assert_eq!(code_of(answered), Code::FailedPrecondition);
        }
    }
    Ok(())
}

/// A `MultiScan` is held to the bound as a whole: a response of exactly
/// `server.max_scan_bytes` is sent, and one byte over is
/// `FAILED_PRECONDITION` with no results, each distinct key it read counted
/// as an `error` in `scans_total`, a key listed twice once.
#[tokio::test(flavor = "multi_thread")]
async fn test_multi_scan_response_at_the_bound_is_sent_and_one_byte_over_is_refused() -> TestResult
{
    let _serial = SERIAL.lock().await;
    for over in [0, 1] {
        let keys_of = |fixture: &Fixture| {
            let first = fixture.key(ORDERED_IDS.start);
            let second = fixture.key(ORDERED_IDS.start + 1);
            let absent = fixture.absent_near(ORDERED_IDS.end);
            [first.clone(), second, first, absent]
        };
        let mut size = 0;
        let mut stack = Stack::ready_ordered_with(async |setup| {
            let keys = keys_of(&setup.fixtures[1]);
            size = model_multi_scan_response(&setup.fixtures[1], &keys).encoded_len();
            setup.config.server.max_scan_bytes = size - over;
            Ok(())
        })
        .await?;
        let keys = keys_of(stack.ordered());
        let request = multi_scan_of(ORDERED_TABLE, &keys, whole_key().limit);
        let before = stack.scrape().await?;
        let answered = stack.multi_scan(request).await;
        let after = stack.scrape().await?;

        let scans = [MULTI_SCAN_SENT, MULTI_SCAN_REFUSED];
        assert_bound_counted(&before, &after, over, size, Call::MultiScan, scans);
        if over == 0 {
            let response = answered?;
            assert_eq!(response, model_multi_scan_response(stack.ordered(), &keys));
            assert_eq!(response.encoded_len(), size);
        } else {
            assert_eq!(code_of(answered), Code::FailedPrecondition);
        }
    }
    Ok(())
}

/// `TableInfo` of an ordered table names its order column and type, the
/// width of a fixed one, `max_scan_rows`, `max_multi_scan_values`, and
/// `max_scan_bytes`; of a key-value table it leaves those unset.
#[tokio::test(flavor = "multi_thread")]
async fn test_table_info_reports_the_order_column_and_max_scan_rows() -> TestResult {
    /// The width of the second ordered table's `fixed` order column.
    const FIXED_WIDTH: u32 = 12;
    /// The second ordered table.
    const FIXED_TABLE: &str = "events_fixed";
    let _serial = SERIAL.lock().await;
    let fixed = FixtureSpec {
        ordered: Some(Ordered {
            kind: OrderKind::Fixed(FIXED_WIDTH),
            ..ordered_spec(FIXED_TABLE).ordered.expect("an ordered spec")
        }),
        ..ordered_spec(FIXED_TABLE)
    };
    let mut stack = Stack::start_with(
        vec![
            FixtureSpec::small(TABLE, KeyType::Long),
            ordered_spec(ORDERED_TABLE),
            fixed,
        ],
        async |setup| {
            setup.config.tables[1].max_scan_rows = Some(SCAN_CAP);
            Ok(())
        },
    )
    .await?;
    stack.serving.ready().await;

    let info = stack.table_info(ORDERED_TABLE).await?;
    assert_eq!(info.order_column, icetray_testkit::ORDER_COLUMN);
    assert_eq!(info.order_type(), v1::OrderType::Long);
    assert_eq!(info.order_width, 0);
    assert_eq!(info.max_scan_rows, SCAN_CAP);
    assert_eq!(
        info.max_multi_scan_values,
        approx::count(ServerConfig::default().max_multi_scan_values)
    );
    assert_eq!(
        info.max_scan_bytes,
        approx::count(ServerConfig::default().max_scan_bytes)
    );

    let info = stack.table_info(FIXED_TABLE).await?;
    assert_eq!(info.order_column, icetray_testkit::ORDER_COLUMN);
    assert_eq!(info.order_type(), v1::OrderType::Fixed);
    assert_eq!(info.order_width, FIXED_WIDTH);
    assert_eq!(info.max_scan_rows, SCAN_CAP_DEFAULT);

    let info = stack.table_info(TABLE).await?;
    assert_eq!(info.order_column, "");
    assert_eq!(info.order_type(), v1::OrderType::Unspecified);
    assert_eq!(info.order_width, 0);
    assert_eq!(info.max_scan_rows, 0);
    assert_eq!(
        info.max_multi_scan_values, 0,
        "a key-value table is not scanned"
    );
    assert_eq!(info.max_scan_bytes, 0, "a key-value table is not scanned");
    Ok(())
}
