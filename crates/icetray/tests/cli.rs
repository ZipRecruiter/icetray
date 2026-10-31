//! The built binary end to end (RFD 016's exit codes, the JSON lines of
//! the output contract in `docs/configuration.md`, the twelve-factor start
//! of RFD 012): fixtures on
//! local disk, the binary run as a process, its stdout parsed line by line.

use std::io::{BufRead as _, BufReader};
use std::net::SocketAddr;
use std::path::Path;
use std::process::{Child, ChildStdout, Command, ExitStatus, Output, Stdio};
use std::time::{Duration, Instant};

use base64::Engine as _;
use hyper::StatusCode;
use icetray::cli::ENV_CONFIG;
use icetray::error::{ERROR_PREFIX, HINT_PREFIX};
use icetray::listen::{METRICS_PATH, NOT_READY_BODY, READY_BODY, READY_PATH};
use icetray::reload::ReloadResult;
use icetray::serve::OVERALL_SERVICE;
use icetray::{EXIT_BELOW_LEVEL, EXIT_FAILURE, EXIT_OK, EXIT_USAGE};
use icetray_core::approx;
use icetray_core::config::{DEFAULT_MAX_SCAN_ROWS, ServerConfig};
use icetray_core::contract::VALUE_PAGE_BYTES;
use icetray_core::metric::{
    ALLOCATOR_ALLOCATED_BYTES, ALLOCATOR_RESIDENT_BYTES, CONFIG_PENDING, CONFIG_RELOADS_TOTAL,
    LABEL_RESULT, LABEL_STATE, LABEL_TABLE, PROCESS_RSS_BYTES, TABLE_STATE,
};
use icetray_core::{
    ColumnRead, Direction, Key, KeyType, MAX_FIELD_NUMBER, OrderValue, Row, SnapshotId, TableState,
    bucket,
};
use icetray_testkit::aws_catalog::{CatalogEntry, FakeAwsCatalog};
use icetray_testkit::fixture::{
    IDS_2K, IDS_4K, NAMESPACE, ONE_BUCKET, SAMPLE, SECOND_VALUE_COLUMN, SMALL_BUCKETS, SMALL_IDS,
    STRAY_IDS, TWO_BUCKETS, UNPARTITIONED_FILE_BYTES,
};
use icetray_testkit::layout::{Layout, VALUE_COLUMN};
use icetray_testkit::protobuf;
use icetray_testkit::verify::{data_files, remove_data_files, series_value, zero_column_chunks};
use icetray_testkit::{
    DeleteForm, Fixture, FixtureSpec, Nulls, ORDER_COLUMN, Order, OrderKind, Ordered, ScanRows,
    TestScan, ValueKind, memory_catalog_local, order_value, write_fixture,
};
use serde_json::Value as Json;
use tonic::transport::Channel;
use tonic_health::pb::health_check_response::ServingStatus;
use tonic_health::pb::health_client::HealthClient;

mod common;

use common::{
    BoxError, NULL_EVERY, NULL_ID, PRESENT_ID, SHUTDOWN_WAIT, health_status, http_get, poll_until,
};

/// What every test returns.
type TestResult = Result<(), BoxError>;

/// The built binary under test.
const BIN: &str = env!("CARGO_BIN_EXE_icetray");

/// Present keys the broken-pipe test asks for: at a few hundred bytes a
/// line, well over the 64 KiB a pipe buffers, so the binary blocks on a
/// write until the reader closes and then meets the broken pipe.
const MANY_KEYS: u64 = 1_000;

/// How long `serve` may take to become ready.
const READY_WAIT: Duration = Duration::from_secs(10);

/// The names of the two tables in the two-table configuration.
const TABLE_A: &str = "a";
const TABLE_B: &str = "b";

/// The renamed log fields and the static field the serve test configures.
const TIME_FIELD: &str = "ts";
const LEVEL_FIELD: &str = "lvl";
const MESSAGE_FIELD: &str = "msg";
const SERVICE_FIELD: &str = "service";
const SERVICE_VALUE: &str = "icetray";

/// Runs the binary with `args` and `env`, with the configuration variable
/// cleared first: a run with no target must be a usage error whatever the
/// developer's shell exports, and a test that wants the variable passes it
/// in `env`.
fn run(args: &[&str], env: &[(&str, &str)]) -> Output {
    Command::new(BIN)
        .args(args)
        .env_remove(ENV_CONFIG)
        .envs(env.iter().copied())
        .output()
        .expect("the binary runs")
}

/// The exit code of a finished run.
fn code(output: &Output) -> i32 {
    output.status.code().expect("the binary exited")
}

/// Asserts the run exited with `expected`, showing its stderr when it did not.
fn assert_exit(output: &Output, expected: u8) {
    assert_eq!(code(output), i32::from(expected), "{}", stderr(output));
}

/// `stdout` parsed line by line.
fn lines(stdout: &[u8]) -> Vec<Json> {
    String::from_utf8_lossy(stdout)
        .lines()
        .map(|line| serde_json::from_str(line).unwrap_or_else(|error| panic!("{line}: {error}")))
        .collect()
}

/// The stderr of a run, for a failing assertion's message.
fn stderr(output: &Output) -> String {
    String::from_utf8_lossy(&output.stderr).into_owned()
}

/// Asserts the run ended by reporting its failure as plain text on stderr:
/// after whatever warnings it logged there, each a JSON log event, one
/// error line in clap's form, then at most one hint line. Returns whether
/// the hint line is there.
fn assert_plain_error(output: &Output) -> bool {
    let text = stderr(output);
    let is_event =
        |line: &str| serde_json::from_str::<Json>(line).is_ok_and(|json| json.is_object());
    let lines: Vec<&str> = text.lines().collect();
    let report = lines
        .iter()
        .position(|line| !is_event(line))
        .map_or(&[][..], |first| &lines[first..]);
    assert!(
        report
            .first()
            .is_some_and(|line| line.starts_with(ERROR_PREFIX)),
        "{text}"
    );
    match &report[1..] {
        [] => false,
        [hint] if hint.starts_with(HINT_PREFIX) => true,
        _ => panic!("more than an error and a hint after the log events: {text}"),
    }
}

/// The arguments a clap usage error on stderr lists as missing: the
/// first word of each line between its `error:` line and the blank line
/// after it.
fn missing_arguments(output: &Output) -> Vec<String> {
    let text = stderr(output);
    let mut lines = text.lines();
    assert!(
        lines
            .next()
            .is_some_and(|line| line.starts_with(ERROR_PREFIX)),
        "{text}"
    );
    lines
        .take_while(|line| !line.is_empty())
        .filter_map(|line| line.split_whitespace().next().map(str::to_string))
        .collect()
}

/// Writes `spec` as a fixture under `dir` through a memory catalog.
async fn local_fixture(dir: &Path, spec: FixtureSpec) -> Result<Fixture, BoxError> {
    let catalog = memory_catalog_local(dir).await?;
    Ok(write_fixture(catalog.as_ref(), NAMESPACE, spec).await?)
}

/// `--location <the fixture's metadata file>`.
fn location(fixture: &Fixture) -> [&str; 4] {
    [
        "--location",
        fixture.metadata_location(),
        "--value-column",
        VALUE_COLUMN,
    ]
}

/// The flags of a `get` that reads the fixture's value column.
const READ_VALUE: [&str; 2] = ["--column", VALUE_COLUMN];

/// A table of the two-table configuration, in TOML, pinned to `fixture`,
/// with `strict` left out, so the table takes the default, off.
fn toml_table(name: &str, fixture: &Fixture) -> String {
    format!(
        "[[tables]]\nname = \"{name}\"\nsource = {{ type = \"static\", location = \"{}\" }}\nref = \"main\"\nkey_column = \"key\"\nvalue_columns = [\"value\"]\n",
        fixture.metadata_location()
    )
}

/// `toml_table` with `strict` on.
fn toml_table_strict(name: &str, fixture: &Fixture) -> String {
    format!("{}strict = true\n", toml_table(name, fixture))
}

/// A table of a JSON configuration, pinned to `fixture`, with `strict`
/// left out, as `toml_table` leaves it.
fn json_table(name: &str, fixture: &Fixture) -> Json {
    serde_json::json!({
        "name": name,
        "source": {"type": "static", "location": fixture.metadata_location()},
        "ref": "main",
        "key_column": "key",
        "value_columns": ["value"]
    })
}

/// A one-table configuration pinned to `fixture`, as JSON, with the
/// listeners on loopback at OS-assigned ports and the renamed log fields.
fn json_config(name: &str, fixture: &Fixture) -> String {
    serde_json::json!({
        "server": {
            "listen": "127.0.0.1:0",
            "metrics_listen": "127.0.0.1:0",
            "log": {
                "fields": {"time": TIME_FIELD, "level": LEVEL_FIELD, "message": MESSAGE_FIELD},
                "static_fields": {SERVICE_FIELD: SERVICE_VALUE}
            }
        },
        "tables": [json_table(name, fixture)]
    })
    .to_string()
}

/// A spawned `icetray serve`, killed when the test ends however it ends, so
/// a failed assertion leaves no daemon serving on loopback.
struct Daemon(Child);

impl Drop for Daemon {
    fn drop(&mut self) {
        // intentional: the child may already have exited, and a kill or a
        // wait that fails then has nothing left to clean up.
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

/// Writes `text` to `name` under `dir` and returns its path as text.
fn write_file(dir: &Path, name: &str, text: &str) -> Result<String, BoxError> {
    let path = dir.join(name);
    std::fs::write(&path, text)?;
    Ok(path
        .to_str()
        .expect("a temporary path is UTF-8")
        .to_string())
}

/// Reads a daemon's log lines up to its `listening` line, checking each,
/// and returns the gRPC and metrics addresses it names.
fn until_listening(
    reader: &mut BufReader<ChildStdout>,
) -> Result<(SocketAddr, SocketAddr), BoxError> {
    loop {
        let mut text = String::new();
        assert!(
            reader.read_line(&mut text)? > 0,
            "the daemon exited before listening"
        );
        let line: Json = serde_json::from_str(&text)?;
        check_log_line(&line);
        if line[MESSAGE_FIELD] == "listening" {
            let address = |field: &str| line[field].as_str().map(str::parse::<SocketAddr>);
            let grpc = address("grpc").expect("the grpc address")?;
            let metrics = address("metrics").expect("the metrics address")?;
            return Ok((grpc, metrics));
        }
    }
}

/// Waits for `child` to exit, up to `wait`.
async fn wait_within(child: &mut Child, wait: Duration) -> Result<ExitStatus, BoxError> {
    poll_until(wait, "the child exiting", async || Ok(child.try_wait()?)).await
}

/// A `--location` that is a relative path opens the table under the
/// working directory, named after its root, while a configuration file
/// holding the same relative path is refused: a file means the same from
/// any directory.
#[tokio::test]
async fn test_info_resolves_a_relative_location_but_not_a_file_s() -> TestResult {
    let dir = tempfile::tempdir()?;
    let name = "kv_relative";
    let fixture = local_fixture(dir.path(), FixtureSpec::small(name, KeyType::Long)).await?;
    let under_dir = format!("file://{}/", dir.path().display());
    let relative = fixture
        .metadata_location()
        .strip_prefix(&under_dir)
        .ok_or("the fixture lives under the directory")?;
    let in_dir = |args: &[&str]| {
        Command::new(BIN)
            .args(args)
            .current_dir(dir.path())
            .env_remove(ENV_CONFIG)
            .output()
    };

    let located = in_dir(&["info", "--location", relative])?;
    assert_exit(&located, EXIT_OK);
    let info = lines(&located.stdout);
    assert_eq!(info.len(), 1);
    assert_eq!(info[0]["table"], name);

    let text = toml_table(TABLE_A, &fixture).replace(fixture.metadata_location(), relative);
    let config = write_file(dir.path(), "relative.toml", &text)?;
    let configured = in_dir(&["info", "--config", &config])?;
    assert_exit(&configured, EXIT_FAILURE);
    assert!(configured.stdout.is_empty());
    Ok(())
}

/// A `file:` location names a path on this host in each RFC 8089 form,
/// `file://localhost/` included, and the table after its root's last
/// directory; `file:` before a relative path is a usage error, since a
/// URI is never resolved against the working directory.
#[tokio::test]
async fn test_info_reads_a_localhost_file_uri_and_refuses_a_relative_one() -> TestResult {
    let dir = tempfile::tempdir()?;
    let name = "kv_file_uri";
    let fixture = local_fixture(dir.path(), FixtureSpec::small(name, KeyType::Long)).await?;
    let under_dir = format!("file://{}/", dir.path().display());
    let relative = fixture
        .metadata_location()
        .strip_prefix(&under_dir)
        .ok_or("the fixture lives under the directory")?;

    let localhost = format!("file://localhost{}/{relative}", dir.path().display());
    let output = run(&["info", "--location", &localhost], &[]);
    assert_exit(&output, EXIT_OK);
    assert_eq!(lines(&output.stdout)[0]["table"], name);

    let scheme_relative = Command::new(BIN)
        .args(["info", "--location", &format!("file:{relative}")])
        .current_dir(dir.path())
        .env_remove(ENV_CONFIG)
        .output()?;
    assert_exit(&scheme_relative, EXIT_USAGE);
    assert!(scheme_relative.stdout.is_empty());
    assert!(
        !assert_plain_error(&scheme_relative),
        "{}",
        stderr(&scheme_relative)
    );
    Ok(())
}

#[tokio::test]
async fn test_info_serializes_the_summary() -> TestResult {
    let dir = tempfile::tempdir()?;
    let fixture = local_fixture(dir.path(), FixtureSpec::small("kv_info", KeyType::Long)).await?;
    let output = run(&[&["info"][..], &location(&fixture)].concat(), &[]);
    assert_exit(&output, EXIT_OK);
    let raw = String::from_utf8(output.stdout.clone())?;
    assert!(
        raw.starts_with(&format!(
            "{{\"kind\":\"table\",\"table\":\"{}\",",
            fixture.ident().name()
        )),
        "{raw}"
    );
    let info = lines(&output.stdout);
    assert_eq!(info.len(), 1);
    assert_eq!(info[0]["kind"], "table");
    assert_eq!(info[0]["table"], fixture.ident().name());
    assert_eq!(info[0]["file_count"], Json::from(SMALL_BUCKETS));
    assert_eq!(
        info[0]["records"],
        Json::from(SMALL_IDS.end - SMALL_IDS.start)
    );
    assert_eq!(info[0]["level"], "optimal");
    assert_eq!(info[0]["residency"], "full");
    Ok(())
}

/// `info` on a Glue source runs end to end against the stand-in catalog:
/// the binary asks Glue for the table's metadata location through the
/// source's endpoint, signed with `[storage.s3]`'s keys, and reads the
/// table from the local warehouse the location names. The AWS
/// configuration files point nowhere and the instance metadata service is
/// off, so the run reads nothing of the host's AWS setup.
#[tokio::test]
async fn test_info_reads_a_glue_table_through_its_catalog() -> TestResult {
    let dir = tempfile::tempdir()?;
    let fixture = local_fixture(dir.path(), FixtureSpec::small("kv_glue", KeyType::Long)).await?;
    let catalog = FakeAwsCatalog::start()?;
    let name = fixture.ident().name();
    catalog.set(
        NAMESPACE,
        name,
        CatalogEntry::Iceberg(fixture.metadata_location().to_string()),
    );
    let key_id = "AKIAICETRAYCLI";
    let text = format!(
        "[storage.s3]\naccess_key_id = \"{key_id}\"\nsecret_access_key = \"icetray-cli-secret\"\n\n[[tables]]\nname = \"{name}\"\nsource = {{ type = \"glue\", database = \"{NAMESPACE}\", table = \"{name}\", region = \"us-test-1\", endpoint = \"{}\" }}\nref = \"main\"\nkey_column = \"key\"\nvalue_columns = [\"value\"]\n",
        catalog.url()
    );
    let config = write_file(dir.path(), "glue.toml", &text)?;
    let nowhere = dir.path().join("no-aws-config");
    let nowhere = nowhere.to_str().expect("a temporary path is UTF-8");
    let output = run(
        &["info", "--config", &config],
        &[
            ("AWS_CONFIG_FILE", nowhere),
            ("AWS_SHARED_CREDENTIALS_FILE", nowhere),
            ("AWS_EC2_METADATA_DISABLED", "true"),
        ],
    );
    assert_exit(&output, EXIT_OK);
    let info = lines(&output.stdout);
    assert_eq!(info.len(), 1);
    assert_eq!(info[0]["table"], name);
    assert_eq!(info[0]["file_count"], Json::from(SMALL_BUCKETS));
    assert_eq!(
        info[0]["records"],
        Json::from(SMALL_IDS.end - SMALL_IDS.start)
    );
    let key_ids = catalog.key_ids();
    assert!(!key_ids.is_empty(), "the binary asked the catalog");
    assert!(key_ids.iter().all(|seen| seen == key_id), "{key_ids:?}");
    Ok(())
}

#[tokio::test]
async fn test_doctor_reports_the_contract_fixture_at_optimal() -> TestResult {
    let dir = tempfile::tempdir()?;
    let fixture = local_fixture(dir.path(), FixtureSpec::small("kv_doctor", KeyType::Long)).await?;
    let output = run(
        &[&["doctor"][..], &location(&fixture), &["--files"]].concat(),
        &[],
    );
    assert_exit(&output, EXIT_OK);
    let report = lines(&output.stdout);
    assert_eq!(report[0]["kind"], "table");
    assert_eq!(report[0]["table"], fixture.ident().name());
    assert_eq!(report[0]["level"], "optimal");
    assert_eq!(report[0]["lookups"], "optimal");
    assert_eq!(report[0]["capping"], Json::from(0));
    assert_eq!(report[0]["advice"], Json::from(0));
    assert!(report.iter().all(|line| line["kind"] != "finding"));
    assert_eq!(
        report.iter().filter(|line| line["kind"] == "file").count(),
        approx::length(SMALL_BUCKETS)
    );
    Ok(())
}

/// The doctor reports the deletes its load applied (RFD 021): the table
/// line's `deletes`, the rows and one delete file per data file touched,
/// and one `deletes_pending` advice finding, which leaves the level and the
/// exit code as they were.
#[tokio::test]
async fn test_doctor_reports_the_deletes_its_load_applied() -> TestResult {
    let dir = tempfile::tempdir()?;
    let mut fixture =
        local_fixture(dir.path(), FixtureSpec::small("kv_deletes", KeyType::Long)).await?;
    let deleted_ids = 3;
    let deleted = &SAMPLE[..deleted_ids];
    fixture
        .delete(deleted, DeleteForm::ScopedPositions, "deletes")
        .await?;
    let touched: std::collections::BTreeSet<u32> = deleted
        .iter()
        .map(|&id| bucket(&fixture.key(id), SMALL_BUCKETS))
        .collect();
    let output = run(&[&["doctor"][..], &location(&fixture)].concat(), &[]);
    assert_exit(&output, EXIT_OK);
    let report = lines(&output.stdout);
    assert_eq!(report[0]["kind"], "table");
    assert_eq!(report[0]["level"], "optimal");
    let summary = &report[0]["deletes"];
    assert_eq!(summary["rows"], Json::from(deleted.len()));
    assert_eq!(summary["files"], Json::from(touched.len()));
    assert!(
        summary["state_bytes"]
            .as_u64()
            .is_some_and(|bytes| bytes > 0)
    );
    assert_eq!(report[0]["advice"], Json::from(1));
    let pending: Vec<&Json> = report
        .iter()
        .filter(|line| line["kind"] == "finding" && line["code"] == "deletes_pending")
        .collect();
    assert_eq!(pending.len(), 1, "{report:?}");
    assert_eq!(pending[0]["caps"], Json::Null);
    Ok(())
}

/// A small fixture with no bucket partition, whose layout the doctor finds
/// permissive.
async fn unpartitioned_fixture(dir: &Path, name: &str) -> Result<Fixture, BoxError> {
    let spec = FixtureSpec {
        buckets: 0,
        layout: Layout {
            target_file_bytes: UNPARTITIONED_FILE_BYTES,
            ..Layout::contract()
        },
        ..FixtureSpec::small(name, KeyType::Long)
    };
    local_fixture(dir, spec).await
}

/// Asserts `report` opens with a permissive `table` line declared
/// `lookups` and carries the `no_bucket_partition` finding capping it at
/// permissive.
fn assert_unpartitioned_findings(report: &[Json], lookups: &str) {
    assert_eq!(report[0]["kind"], "table");
    assert_eq!(report[0]["level"], "permissive");
    assert_eq!(report[0]["lookups"], lookups);
    assert!(
        report.iter().any(|line| line["kind"] == "finding"
            && line["code"] == "no_bucket_partition"
            && line["caps"] == "permissive"),
        "{report:?}"
    );
}

#[tokio::test]
async fn test_doctor_reports_an_unpartitioned_table_below_its_level_and_exits_2() -> TestResult {
    let dir = tempfile::tempdir()?;
    let fixture = unpartitioned_fixture(dir.path(), "kv_flat").await?;
    let output = run(&[&["doctor"][..], &location(&fixture)].concat(), &[]);
    assert_exit(&output, EXIT_BELOW_LEVEL);
    let report = lines(&output.stdout);
    assert_unpartitioned_findings(&report, "optimal");
    assert!(report.iter().all(|line| line["kind"] != "refusal"));
    Ok(())
}

/// A configured table keeps its `strict`: with `strict` on, the doctor
/// reports the findings of a table below its level and then refuses it as
/// `below_level`, as `info` and `serve` do, with exit 1; under the default,
/// off, it is below its level and served, exit 2.
#[tokio::test]
async fn test_doctor_refuses_a_strict_table_below_its_level() -> TestResult {
    let dir = tempfile::tempdir()?;
    let fixture = unpartitioned_fixture(dir.path(), "kv_flat_strict").await?;
    let strict = write_file(
        dir.path(),
        "strict.toml",
        &toml_table_strict(TABLE_A, &fixture),
    )?;
    let output = run(&["doctor", "--config", &strict], &[]);
    assert_exit(&output, EXIT_FAILURE);
    let report = lines(&output.stdout);
    assert_unpartitioned_findings(&report, "optimal");
    let refused: Vec<&Json> = report
        .iter()
        .filter(|line| line["kind"] == "refusal")
        .collect();
    assert_eq!(refused.len(), 1, "{report:?}");
    assert_eq!(refused[0]["table"], TABLE_A);
    assert_eq!(refused[0]["reason"], "below_level");
    assert_eq!(
        report.last(),
        Some(refused[0]),
        "the refusal follows the findings"
    );
    assert_exit(&run(&["info", "--config", &strict], &[]), EXIT_FAILURE);

    let lenient = write_file(dir.path(), "lenient.toml", &toml_table(TABLE_A, &fixture))?;
    let output = run(&["doctor", "--config", &lenient], &[]);
    assert_exit(&output, EXIT_BELOW_LEVEL);
    let report = lines(&output.stdout);
    assert_unpartitioned_findings(&report, "optimal");
    assert!(report.iter().all(|line| line["kind"] != "refusal"));
    Ok(())
}

/// A table that declares `permissive` meets it with a permissive layout:
/// the findings are reported and the doctor exits 0, under `strict` as
/// well, since nothing is below the level it declares.
#[tokio::test]
async fn test_doctor_exits_0_for_a_table_at_a_permissive_declared_level() -> TestResult {
    let dir = tempfile::tempdir()?;
    let fixture = unpartitioned_fixture(dir.path(), "kv_flat_permissive").await?;
    let permissive = write_file(
        dir.path(),
        "permissive.toml",
        &format!(
            "{}lookups = \"permissive\"\n",
            toml_table_strict(TABLE_A, &fixture)
        ),
    )?;
    let output = run(&["doctor", "--config", &permissive], &[]);
    assert_exit(&output, EXIT_OK);
    let report = lines(&output.stdout);
    assert_unpartitioned_findings(&report, "permissive");
    assert!(report.iter().all(|line| line["kind"] != "refusal"));
    Ok(())
}

/// A small fixture written in descending key order, which every load
/// refuses as `unsorted`.
fn unsorted_spec(name: &str) -> FixtureSpec {
    FixtureSpec {
        ids: IDS_4K,
        order: Order::Descending,
        ..FixtureSpec::small(name, KeyType::Long)
    }
}

#[tokio::test]
async fn test_doctor_reports_a_refusal_and_exits_1() -> TestResult {
    let dir = tempfile::tempdir()?;
    let fixture = local_fixture(dir.path(), unsorted_spec("kv_unsorted")).await?;
    let output = run(&[&["doctor"][..], &location(&fixture)].concat(), &[]);
    assert_exit(&output, EXIT_FAILURE);
    let report = lines(&output.stdout);
    assert_eq!(report.len(), 1);
    assert_eq!(report[0]["kind"], "refusal");
    assert_eq!(report[0]["table"], fixture.ident().name());
    assert_eq!(report[0]["reason"], "unsorted");
    assert!(report[0].get("error_kind").is_none(), "{report:?}");
    assert!(report[0]["message"].is_string());
    Ok(())
}

/// The rows every key of the ordered fixtures here has.
const ORDER_ROWS_PER_KEY: u64 = 7;

/// The raw order value of a key's first row, and the distance between
/// consecutive rows.
const ORDER_START: i64 = 1_000;
const ORDER_STEP: i64 = 10;

/// The `max_scan_rows` the info test configures: fewer than the default,
/// and within `server.max_multi_scan_values`.
const CONFIGURED_SCAN_ROWS: u32 = 50;

/// The `server.max_multi_scan_values` the info test configures: fewer than
/// the default, and at least twice `CONFIGURED_SCAN_ROWS`.
const CONFIGURED_MULTI_SCAN_VALUES: usize = 600;

/// The `server.max_scan_bytes` the info test configures: 1 MiB, below the
/// default.
const CONFIGURED_SCAN_BYTES: usize = 1024 * 1024;

/// An ordered fixture of `IDS_2K` in `TWO_BUCKETS`, each key with
/// `ORDER_ROWS_PER_KEY` timestamps `ORDER_STEP` apart from `ORDER_START`,
/// or all at `ORDER_START` when `step` is zero, which repeats every pair.
fn ordered_spec(name: &str, step: i64) -> FixtureSpec {
    ordered_spec_of(name, OrderKind::Timestamp, ORDER_START, step)
}

/// `ordered_spec` with the order column's kind and first value given.
fn ordered_spec_of(name: &str, kind: OrderKind, start: i64, step: i64) -> FixtureSpec {
    FixtureSpec {
        ids: IDS_2K,
        buckets: TWO_BUCKETS,
        ..FixtureSpec::ordered(
            name,
            KeyType::Long,
            Ordered::each(kind, ORDER_ROWS_PER_KEY, start, step),
        )
    }
}

/// `toml_table` with the order column and, when set, `max_scan_rows`.
fn toml_ordered_table(name: &str, fixture: &Fixture, max_scan_rows: Option<u32>) -> String {
    let rows = max_scan_rows.map_or_else(String::new, |rows| format!("max_scan_rows = {rows}\n"));
    format!(
        "{}order_column = \"{ORDER_COLUMN}\"\n{rows}",
        toml_table(name, fixture)
    )
}

/// A config file `{stem}.toml` under `dir` serving `fixture` alone, as
/// `TABLE_A` with its order column; its path.
fn ordered_config(dir: &Path, stem: &str, fixture: &Fixture) -> Result<String, BoxError> {
    write_file(
        dir,
        &format!("{stem}.toml"),
        &toml_ordered_table(TABLE_A, fixture, None),
    )
}

/// The doctor reports an ordered table's order column on its `table` line
/// and each file's order facts on its `file` line (RFD 025, Doctor): the
/// column, its type, the effective `max_scan_rows`, and the longest run
/// across files; per file the least and greatest order value, the longest
/// run, and the distinct keys of each row group. A key-value table's lines
/// carry none of it.
#[tokio::test]
async fn test_doctor_reports_an_ordered_table_s_order_facts() -> TestResult {
    let dir = tempfile::tempdir()?;
    let fixture = local_fixture(dir.path(), ordered_spec("events_doctor", ORDER_STEP)).await?;
    let config = ordered_config(dir.path(), "ordered", &fixture)?;
    let output = run(&["doctor", "--config", &config, "--files"], &[]);
    assert_exit(&output, EXIT_OK);
    let report = lines(&output.stdout);
    let order = &report[0]["order"];
    assert_eq!(report[0]["kind"], "table");
    assert_eq!(order["column"], ORDER_COLUMN);
    assert_eq!(order["order_type"], "timestamp");
    assert_eq!(order["max_scan_rows"], Json::from(DEFAULT_MAX_SCAN_ROWS));
    let server = ServerConfig::default();
    assert_eq!(
        order["max_multi_scan_values"],
        Json::from(server.max_multi_scan_values)
    );
    assert_eq!(order["max_scan_bytes"], Json::from(server.max_scan_bytes));
    assert_eq!(order["longest_run"], Json::from(ORDER_ROWS_PER_KEY));
    let files: Vec<&Json> = report
        .iter()
        .filter(|line| line["kind"] == "file")
        .collect();
    assert_eq!(files.len(), approx::length(TWO_BUCKETS));
    let last = ORDER_START + ORDER_STEP * i64::try_from(ORDER_ROWS_PER_KEY - 1)?;
    let mut distinct = 0;
    for file in files {
        let facts = &file["order"];
        assert_eq!(facts["min"], Json::from(ORDER_START));
        assert_eq!(facts["max"], Json::from(last));
        assert_eq!(facts["longest_run"], Json::from(ORDER_ROWS_PER_KEY));
        let per_row_group = facts["distinct_keys"]
            .as_array()
            .expect("distinct keys by row group");
        assert_eq!(
            per_row_group.len(),
            approx::length(file["row_groups"].as_u64().unwrap())
        );
        distinct += per_row_group
            .iter()
            .map(|keys| keys.as_u64().expect("a count"))
            .sum::<u64>();
    }
    let keys = IDS_2K.end - IDS_2K.start;
    assert!(
        distinct >= keys,
        "every key is in a row group: {distinct} of {keys}"
    );
    Ok(())
}

/// A key-value table's `table` and `file` lines carry no order field.
#[tokio::test]
async fn test_doctor_leaves_a_key_value_table_s_lines_without_order() -> TestResult {
    let dir = tempfile::tempdir()?;
    let fixture =
        local_fixture(dir.path(), FixtureSpec::small("kv_no_order", KeyType::Long)).await?;
    let output = run(
        &[&["doctor"][..], &location(&fixture), &["--files"]].concat(),
        &[],
    );
    assert_exit(&output, EXIT_OK);
    let report = lines(&output.stdout);
    assert!(report.iter().any(|line| line["kind"] == "file"));
    assert!(report.iter().all(|line| line.get("order").is_none()));
    Ok(())
}

/// The greatest long a reader that parses JSON numbers as doubles holds
/// exactly.
const EXACT_ORDER: i64 = 1 << f64::MANTISSA_DIGITS;

/// The log field the precision warning carries the table's name in.
const WARNING_TABLE_FIELD: &str = "table";

/// The log field the precision warning carries the exact limit in.
const WARNING_LIMIT_FIELD: &str = "max_exact";

/// The tables the precision warnings of a run on stderr name, in the order
/// the run logged them: the warn-level events carrying the limit field.
fn precision_warnings(output: &Output) -> Vec<String> {
    let limit = Json::from(EXACT_ORDER);
    stderr(output)
        .lines()
        .filter_map(|line| serde_json::from_str::<Json>(line).ok())
        .filter(|event| event["level"] == "warn" && event[WARNING_LIMIT_FIELD] == limit)
        .map(|event| {
            event[WARNING_TABLE_FIELD]
                .as_str()
                .expect("the warning names its table")
                .to_string()
        })
        .collect()
}

/// An ordered table of `ORDER_ROWS_PER_KEY` timestamps in nanoseconds a
/// step of one apart, the last at `last`.
async fn nanosecond_fixture(dir: &Path, name: &str, last: i64) -> Result<Fixture, BoxError> {
    let first = last - i64::try_from(ORDER_ROWS_PER_KEY - 1)?;
    local_fixture(dir, ordered_spec_of(name, OrderKind::TimestampNs, first, 1)).await
}

/// A `doctor --files` run under `config`, which must exit 0.
fn doctor_files(config: &str, extra: &[&str], env: &[(&str, &str)]) -> Output {
    let output = run(
        &[&["doctor", "--config", config, "--files"], extra].concat(),
        env,
    );
    assert_exit(&output, EXIT_OK);
    output
}

/// The warning appears for a long order value of 2^53 + 1 and not for 2^53,
/// and either way stdout carries the value as the exact number.
#[tokio::test]
async fn test_doctor_warns_when_a_file_line_carries_an_order_past_2_pow_53() -> TestResult {
    assert_precision_cases(&[
        ("at_limit", EXACT_ORDER, false),
        ("past_limit", EXACT_ORDER + 1, true),
    ])
    .await
}

/// The negative side of the limit: a file whose least order value is
/// -2^53 does not warn and one at -2^53 - 1 does, and either way stdout
/// carries the greatest as the exact number.
#[tokio::test]
async fn test_doctor_warns_when_a_file_line_carries_an_order_below_minus_2_pow_53() -> TestResult {
    let span = i64::try_from(ORDER_ROWS_PER_KEY - 1)?;
    assert_precision_cases(&[
        ("negative_at_limit", -EXACT_ORDER + span, false),
        ("negative_past_limit", -EXACT_ORDER - 1 + span, true),
    ])
    .await
}

/// For each case, named, of a `nanosecond_fixture` whose last order value
/// is `last`: a `doctor --files` run warns of `TABLE_A` exactly when
/// `warns`, and its `file` line carries `last` as the greatest order value.
async fn assert_precision_cases(cases: &[(&str, i64, bool)]) -> TestResult {
    let dir = tempfile::tempdir()?;
    for &(name, last, warns) in cases {
        let fixture = nanosecond_fixture(dir.path(), name, last).await?;
        let config = ordered_config(dir.path(), name, &fixture)?;
        let output = doctor_files(&config, &[], &[]);
        let expected: &[String] = if warns { &[TABLE_A.to_string()] } else { &[] };
        assert_eq!(precision_warnings(&output), expected, "{name}");
        let max = lines(&output.stdout)
            .into_iter()
            .find(|line| line["kind"] == "file")
            .map(|line| line["order"]["max"].clone());
        assert_eq!(max, Some(Json::from(last)), "{name}");
    }
    Ok(())
}

/// The warning goes to stderr alone: with the log stream silenced, stdout
/// is the same bytes, and every stdout line is still one of the output
/// contract's JSON lines.
#[tokio::test]
async fn test_doctor_warning_leaves_stdout_unchanged() -> TestResult {
    let dir = tempfile::tempdir()?;
    let fixture = nanosecond_fixture(dir.path(), "events_wide", EXACT_ORDER + 1).await?;
    let config = ordered_config(dir.path(), "wide", &fixture)?;
    let warned = doctor_files(&config, &[], &[]);
    let silenced = doctor_files(&config, &[], &[("RUST_LOG", "off")]);
    assert_eq!(precision_warnings(&warned), [TABLE_A.to_string()]);
    assert!(precision_warnings(&silenced).is_empty());
    assert!(!warned.stdout.is_empty());
    assert_eq!(warned.stdout, silenced.stdout);
    assert!(
        lines(&warned.stdout)
            .iter()
            .all(|line| line["kind"].is_string() && line["table"] == TABLE_A)
    );
    Ok(())
}

/// A run over two tables that both carry such a value warns once for
/// each, naming each table once; a third table that does not is not named.
#[tokio::test]
async fn test_doctor_warns_once_for_each_table_that_carries_an_order_past_2_pow_53() -> TestResult {
    let dir = tempfile::tempdir()?;
    let first = nanosecond_fixture(dir.path(), "wide_first", EXACT_ORDER + 1).await?;
    let second = nanosecond_fixture(dir.path(), "wide_second", EXACT_ORDER + 1).await?;
    let narrow = nanosecond_fixture(dir.path(), "narrow", EXACT_ORDER).await?;
    let text = format!(
        "{}{}{}",
        toml_ordered_table(TABLE_A, &first, None),
        toml_ordered_table(TABLE_B, &second, None),
        toml_ordered_table("c", &narrow, None),
    );
    let config = write_file(dir.path(), "three.toml", &text)?;
    let output = doctor_files(&config, &[], &[]);
    let mut named = precision_warnings(&output);
    named.sort();
    assert_eq!(named, [TABLE_A.to_string(), TABLE_B.to_string()]);
    Ok(())
}

/// A table's lines carry an order value only on `file` lines, so a `doctor`
/// without `--files` and `info` print no such number and warn of none,
/// whatever the table holds.
#[tokio::test]
async fn test_doctor_and_info_without_file_lines_never_warn_of_an_order() -> TestResult {
    let dir = tempfile::tempdir()?;
    let fixture = nanosecond_fixture(dir.path(), "events_quiet", EXACT_ORDER + 1).await?;
    let config = ordered_config(dir.path(), "quiet", &fixture)?;
    let doctor = run(&["doctor", "--config", &config], &[]);
    assert_exit(&doctor, EXIT_OK);
    assert!(precision_warnings(&doctor).is_empty());
    assert!(
        lines(&doctor.stdout)
            .iter()
            .all(|line| line["kind"] != "file")
    );
    let info = run(&["info", "--config", &config], &[]);
    assert_exit(&info, EXIT_OK);
    assert!(precision_warnings(&info).is_empty());
    Ok(())
}

/// A string order column is JSON text and never warns, whatever it holds.
#[tokio::test]
async fn test_doctor_does_not_warn_of_a_string_order() -> TestResult {
    let dir = tempfile::tempdir()?;
    let fixture = local_fixture(
        dir.path(),
        ordered_spec_of("events_text", OrderKind::Str, EXACT_ORDER + 1, 1),
    )
    .await?;
    let config = ordered_config(dir.path(), "text", &fixture)?;
    let output = doctor_files(&config, &[], &[]);
    assert!(precision_warnings(&output).is_empty());
    Ok(())
}

/// `info` reports the order column, its type, the configured
/// `max_scan_rows`, `server.max_multi_scan_values`, and
/// `server.max_scan_bytes`, as `TableInfo` does, and the longest run.
#[tokio::test]
async fn test_info_reports_the_order_column() -> TestResult {
    let dir = tempfile::tempdir()?;
    let fixture = local_fixture(dir.path(), ordered_spec("events_info", ORDER_STEP)).await?;
    let server = format!(
        "[server]\nmax_multi_scan_values = {CONFIGURED_MULTI_SCAN_VALUES}\nmax_scan_bytes = {CONFIGURED_SCAN_BYTES}\n"
    );
    let config = write_file(
        dir.path(),
        "ordered.toml",
        &format!(
            "{server}{}",
            toml_ordered_table(TABLE_A, &fixture, Some(CONFIGURED_SCAN_ROWS))
        ),
    )?;
    let output = run(&["info", "--config", &config], &[]);
    assert_exit(&output, EXIT_OK);
    let info = lines(&output.stdout);
    assert_eq!(info.len(), 1);
    let order = &info[0]["order"];
    assert_eq!(order["column"], ORDER_COLUMN);
    assert_eq!(order["order_type"], "timestamp");
    assert_eq!(order["max_scan_rows"], Json::from(CONFIGURED_SCAN_ROWS));
    assert_eq!(
        order["max_multi_scan_values"],
        Json::from(CONFIGURED_MULTI_SCAN_VALUES)
    );
    assert_eq!(order["max_scan_bytes"], Json::from(CONFIGURED_SCAN_BYTES));
    assert_eq!(order["longest_run"], Json::from(ORDER_ROWS_PER_KEY));
    Ok(())
}

/// A key's pair written twice refuses the table as `duplicate_rows`, and
/// the doctor exits 1 with the refusal as its only line.
#[tokio::test]
async fn test_doctor_reports_duplicate_rows_with_exit_failure() -> TestResult {
    let dir = tempfile::tempdir()?;
    let spec = FixtureSpec {
        buckets: ONE_BUCKET,
        ..ordered_spec("events_duplicate", 0)
    };
    let fixture = local_fixture(dir.path(), spec).await?;
    let config = ordered_config(dir.path(), "ordered", &fixture)?;
    let output = run(&["doctor", "--config", &config], &[]);
    assert_exit(&output, EXIT_FAILURE);
    let report = lines(&output.stdout);
    assert_eq!(report.len(), 1, "{report:?}");
    assert_eq!(report[0]["kind"], "refusal");
    assert_eq!(report[0]["table"], TABLE_A);
    assert_eq!(report[0]["reason"], "duplicate_rows");
    Ok(())
}

/// A refusal outranks a table below its level: over one table refused and
/// another only below its level, the doctor reports both and exits 1.
#[tokio::test]
async fn test_doctor_exits_1_when_one_table_is_refused_and_another_below_level() -> TestResult {
    let refused_dir = tempfile::tempdir()?;
    let below_dir = tempfile::tempdir()?;
    let unsorted = local_fixture(refused_dir.path(), unsorted_spec("kv_unsorted_two")).await?;
    let flat = unpartitioned_fixture(below_dir.path(), "kv_flat_two").await?;
    let toml = format!(
        "{}\n{}",
        toml_table(TABLE_A, &unsorted),
        toml_table(TABLE_B, &flat)
    );
    let config = write_file(refused_dir.path(), "two.toml", &toml)?;
    let output = run(&["doctor", "--config", &config], &[]);
    assert_exit(&output, EXIT_FAILURE);
    let report = lines(&output.stdout);
    let refused: Vec<&Json> = report
        .iter()
        .filter(|line| line["kind"] == "refusal")
        .collect();
    assert_eq!(refused.len(), 1, "{report:?}");
    assert_eq!(refused[0]["table"], TABLE_A);
    assert_eq!(refused[0]["reason"], "unsorted");
    let below: Vec<&Json> = report
        .iter()
        .filter(|line| line["kind"] == "table" && line["table"] == TABLE_B)
        .collect();
    assert_eq!(below.len(), 1, "{report:?}");
    assert_eq!(below[0]["level"], "permissive");
    assert_eq!(below[0]["lookups"], "optimal");
    Ok(())
}

/// A `--location` table takes its declared level from `--residency`: in
/// the bloom tier the contract fixture is balanced, as the tier is at best,
/// and meets it.
#[tokio::test]
async fn test_doctor_location_in_the_bloom_tier_declares_balanced_and_exits_0() -> TestResult {
    let dir = tempfile::tempdir()?;
    let fixture = local_fixture(dir.path(), FixtureSpec::small("kv_bloom", KeyType::Long)).await?;
    let output = run(
        &[
            &["doctor"][..],
            &location(&fixture),
            &["--residency", "bloom"],
        ]
        .concat(),
        &[],
    );
    assert_exit(&output, EXIT_OK);
    let report = lines(&output.stdout);
    assert_eq!(report[0]["kind"], "table");
    assert_eq!(report[0]["residency"], "bloom");
    assert_eq!(report[0]["level"], "balanced");
    assert_eq!(report[0]["lookups"], "balanced");
    assert!(report.iter().all(|line| line["kind"] != "refusal"));
    Ok(())
}

/// A raw file of bucket-1 keys with one bucket-0 key between its first
/// and last, recorded under bucket 1, beside a fixture of two buckets: the
/// stray key is no fence, so the load serves the table and `get` answers
/// that key absent, and the doctor's full bucket check refuses the table.
#[tokio::test]
async fn test_doctor_refuses_a_key_outside_its_bucket_that_the_load_serves() -> TestResult {
    let dir = tempfile::tempdir()?;
    let catalog = memory_catalog_local(dir.path()).await?;
    let spec = FixtureSpec {
        ids: IDS_2K,
        buckets: TWO_BUCKETS,
        ..FixtureSpec::small("kv_stray", KeyType::Long)
    };
    let mut fixture = write_fixture(catalog.as_ref(), NAMESPACE, spec).await?;
    let (recorded, hashed) = (1, 0);
    let buckets: Vec<(u64, u32)> = STRAY_IDS
        .map(|id| (id, bucket(&fixture.key(id), TWO_BUCKETS)))
        .collect();
    let mut ids: Vec<u64> = buckets
        .iter()
        .filter(|(_, at)| *at == recorded)
        .map(|(id, _)| *id)
        .collect();
    let (first, last) = (ids[0], ids[ids.len() - 1]);
    let stray = buckets
        .iter()
        .find(|(id, at)| *at == hashed && first < *id && *id < last)
        .map(|(id, _)| *id)
        .expect("a bucket-0 id between the first and last bucket-1 id");
    ids.push(stray);
    ids.sort_unstable();
    let raw = fixture.raw_file_in_bucket("stray.parquet", &ids, i32::try_from(recorded)?)?;
    fixture.append_raw(catalog.as_ref(), raw).await?;
    let Key::Long(stray_key) = fixture.key(stray) else {
        panic!("a long-keyed fixture");
    };

    let get = run(
        &[&["get"][..], &location(&fixture), &[&stray_key.to_string()]].concat(),
        &[],
    );
    assert_exit(&get, EXIT_OK);
    assert_eq!(lines(&get.stdout)[0]["presence"], "absent");

    let output = run(&[&["doctor"][..], &location(&fixture)].concat(), &[]);
    assert_exit(&output, EXIT_FAILURE);
    let report = lines(&output.stdout);
    assert_eq!(report.len(), 1);
    assert_eq!(report[0]["kind"], "refusal");
    assert_eq!(report[0]["table"], fixture.ident().name());
    assert_eq!(report[0]["reason"], "key_outside_bucket");
    Ok(())
}

#[tokio::test]
async fn test_get_answers_present_null_and_absent() -> TestResult {
    let dir = tempfile::tempdir()?;
    let spec = FixtureSpec {
        nulls: Nulls::EveryNth(NULL_EVERY),
        ..FixtureSpec::small("kv_nulls", KeyType::Long)
    };
    let fixture = local_fixture(dir.path(), spec).await?;
    let present = fixture.key(PRESENT_ID);
    let null = fixture.key(NULL_ID);
    let absent = fixture.absent_near(PRESENT_ID);
    let value = fixture.value(&present).expect("a present value");
    assert_eq!(fixture.value(&null), None);
    let keys = [present.to_string(), null.to_string(), absent.to_string()];
    let output = run(
        &[
            &["get"][..],
            &location(&fixture),
            &READ_VALUE,
            &keys.iter().map(String::as_str).collect::<Vec<_>>(),
        ]
        .concat(),
        &[],
    );
    assert_exit(&output, EXIT_OK);
    let got = lines(&output.stdout);
    assert_eq!(got.len(), keys.len());
    for (line, key) in got.iter().zip(&keys) {
        assert_eq!(line["kind"], "lookup");
        assert_eq!(line["table"], fixture.ident().name());
        assert_eq!(line["key"], Json::String(key.clone()));
        assert_eq!(line["snapshot_id"], got[0]["snapshot_id"]);
    }
    assert_eq!(got[0]["presence"], "present");
    assert_eq!(
        got[0]["values"][VALUE_COLUMN]["base64"],
        Json::String(base64::engine::general_purpose::STANDARD.encode(&value))
    );
    assert_eq!(
        got[0]["values"][VALUE_COLUMN]["bytes"],
        Json::from(value.len())
    );
    assert_eq!(got[0]["gets"], Json::from(1));
    assert_eq!(got[1]["presence"], "present");
    assert_eq!(got[1]["values"][VALUE_COLUMN], Json::Null, "a null is null");
    assert_eq!(got[1]["gets"], Json::from(1));
    assert_eq!(got[2]["presence"], "absent");
    assert!(got[2].get("values").is_none());
    assert_eq!(got[2]["gets"], Json::from(0));

    let presence_only = run(
        &[
            &["get"][..],
            &location(&fixture),
            &keys.iter().map(String::as_str).collect::<Vec<_>>(),
        ]
        .concat(),
        &[],
    );
    assert_exit(&presence_only, EXIT_OK);
    let got = lines(&presence_only.stdout);
    assert_eq!(got[0]["presence"], "present");
    assert_eq!(got[0]["values"], serde_json::json!({}));
    assert_eq!(got[1]["presence"], "present");
    assert_eq!(got[2]["presence"], "absent");
    for line in &got {
        assert_eq!(
            line["gets"],
            Json::from(0),
            "no --column reads no value page"
        );
    }
    Ok(())
}

/// The `key` of a lookup line is the text `get` takes for that key: passed
/// back to `get`, it looks up the same key, for every key type.
#[tokio::test]
async fn test_get_key_field_passed_back_looks_up_the_same_key() -> TestResult {
    let dir = tempfile::tempdir()?;
    let cases = [
        ("kv_long", KeyType::Long),
        ("kv_str", KeyType::String),
        ("kv_bin", KeyType::Binary),
        ("kv_uuid", KeyType::Uuid),
    ];
    for (name, key_type) in cases {
        let fixture = local_fixture(dir.path(), FixtureSpec::small(name, key_type)).await?;
        let key = fixture.key(SAMPLE[1]);
        let text = match &key {
            Key::Str(text) => text.clone(),
            Key::Long(_) | Key::Bytes(_) | Key::Uuid(_) => key.to_string(),
        };
        let first = run(
            &[&["get"][..], &location(&fixture), &READ_VALUE, &[&text]].concat(),
            &[],
        );
        assert_exit(&first, EXIT_OK);
        let first = lines(&first.stdout);
        assert_eq!(first.len(), 1, "{name}");
        assert_eq!(first[0]["presence"], "present", "{name}");
        assert_eq!(first[0]["key"], Json::String(text.clone()), "{name}");
        assert_eq!(first[0]["gets"], Json::from(1), "{name}");

        let Json::String(printed) = &first[0]["key"] else {
            panic!("{name}: the key is a JSON string in {}", first[0]);
        };
        let again = run(
            &[&["get"][..], &location(&fixture), &READ_VALUE, &[printed]].concat(),
            &[],
        );
        assert_exit(&again, EXIT_OK);
        let again = lines(&again.stdout);
        assert_eq!(again.len(), 1, "{name}");
        assert_eq!(again[0]["presence"], "present", "{name}");
        assert_eq!(again[0]["key"], first[0]["key"], "{name}");
        assert_eq!(again[0]["values"], first[0]["values"], "{name}");

        if let Key::Bytes(_) = &key {
            let uppercase = text.trim_start_matches("0x").to_uppercase();
            let output = run(
                &[&["get"][..], &location(&fixture), &[&uppercase]].concat(),
                &[],
            );
            assert_exit(&output, EXIT_OK);
            let got = lines(&output.stdout);
            assert_eq!(got[0]["presence"], "present");
            assert_eq!(got[0]["key"], Json::String(text.clone()));
        }
    }
    Ok(())
}

/// A key that does not parse under the table's key type is a usage error
/// found from the metadata alone, before the load: with every data file
/// gone, so a load fails with exit 1, a bad key still exits 64.
#[tokio::test]
async fn test_get_rejects_a_key_that_does_not_parse_before_the_load() -> TestResult {
    let dir = tempfile::tempdir()?;
    let fixture = local_fixture(dir.path(), FixtureSpec::small("kv_badkey", KeyType::Long)).await?;
    let output = run(&[&["get"][..], &location(&fixture), &["abc"]].concat(), &[]);
    assert_exit(&output, EXIT_USAGE);
    assert!(output.stdout.is_empty());
    assert!(assert_plain_error(&output), "the key's form is the hint");

    assert!(remove_data_files(dir.path())? > 0);
    let unloadable = run(&[&["get"][..], &location(&fixture), &["1"]].concat(), &[]);
    assert_exit(&unloadable, EXIT_FAILURE);
    let bad_key = run(&[&["get"][..], &location(&fixture), &["abc"]].concat(), &[]);
    assert_exit(&bad_key, EXIT_USAGE);
    assert!(bad_key.stdout.is_empty());
    Ok(())
}

/// A `--column` the table does not serve, or one given twice, is a usage
/// error found from the configuration alone, before the load: with every
/// data file gone, so a load fails with exit 1, it still exits 64.
#[tokio::test]
async fn test_get_rejects_an_unserved_or_repeated_column_before_the_load() -> TestResult {
    let dir = tempfile::tempdir()?;
    let fixture = local_fixture(
        dir.path(),
        FixtureSpec::small("kv_badcolumn", KeyType::Long),
    )
    .await?;
    assert!(remove_data_files(dir.path())? > 0);
    let key = fixture.key(SAMPLE[0]).to_string();
    let unserved = ["--column", "absent"];
    let repeated = ["--column", VALUE_COLUMN, "--column", VALUE_COLUMN];
    for columns in [&unserved[..], &repeated[..]] {
        let output = run(
            &[&["get"][..], &location(&fixture), columns, &[&key]].concat(),
            &[],
        );
        assert_exit(&output, EXIT_USAGE);
        assert!(output.stdout.is_empty());
        assert_plain_error(&output);
    }
    Ok(())
}

/// `--location` with `VALUE_COLUMN` declared protobuf.
fn protobuf_location(fixture: &Fixture) -> [String; 4] {
    [
        "--location".to_string(),
        fixture.metadata_location().to_string(),
        "--value-column".to_string(),
        format!("{VALUE_COLUMN}:protobuf"),
    ]
}

/// `get --column NAME=FIELDS` on a column declared protobuf prints the
/// value trimmed to those fields, in the line's usual shape, at the GET a
/// whole read costs; `NAME=` and `NAME` read it whole.
#[tokio::test]
async fn test_get_trims_a_protobuf_column_to_the_fields_named() -> TestResult {
    let dir = tempfile::tempdir()?;
    let spec = FixtureSpec {
        value_kind: ValueKind::Protobuf,
        ..FixtureSpec::small("kv_project_cli", KeyType::Long)
    };
    let fixture = local_fixture(dir.path(), spec).await?;
    let key = fixture.key(SAMPLE[0]);
    let fields = [protobuf::ID_FIELD, protobuf::SCORES_FIELD];
    let encoded =
        |bytes: &[u8]| Json::String(base64::engine::general_purpose::STANDARD.encode(bytes));
    let trimmed = fixture.expected_reads(
        &key,
        &[ColumnRead {
            name: VALUE_COLUMN,
            fields: &fields,
        }],
    );
    let Row::Present(values) = trimmed else {
        unreachable!("a sampled key is present");
    };
    let trimmed = values[0].clone().expect("a present value");
    let whole = fixture.value(&key).expect("a present value");
    assert!(trimmed.len() < whole.len());
    let located = protobuf_location(&fixture);
    let located: Vec<&str> = located.iter().map(String::as_str).collect();
    let named = format!(
        "{VALUE_COLUMN}={}",
        fields.map(|field| field.to_string()).join(",")
    );
    let empty_list = format!("{VALUE_COLUMN}=");
    for (column, expected) in [
        (named.as_str(), &trimmed[..]),
        (empty_list.as_str(), &whole[..]),
        (VALUE_COLUMN, &whole[..]),
    ] {
        let output = run(
            &[
                &["get"][..],
                &located,
                &["--column", column],
                &[&key.to_string()],
            ]
            .concat(),
            &[],
        );
        assert_exit(&output, EXIT_OK);
        let got = lines(&output.stdout);
        assert_eq!(
            got[0]["values"][VALUE_COLUMN]["base64"],
            encoded(expected),
            "{column}"
        );
        assert_eq!(
            got[0]["values"][VALUE_COLUMN]["bytes"],
            Json::from(expected.len())
        );
        assert_eq!(got[0]["gets"], Json::from(1), "{column}: one value page");
    }
    Ok(())
}

/// `doctor` on a table declaring a protobuf column reports the column's
/// format, and for values that are not protobuf a `malformed_protobuf`
/// finding naming the column, which is advice: the run exits 0. A column
/// of protobuf values draws none.
#[tokio::test]
async fn test_doctor_reports_each_column_s_format_and_malformed_protobuf() -> TestResult {
    let dir = tempfile::tempdir()?;
    for (kind, malformed) in [(ValueKind::Random, 1), (ValueKind::Protobuf, 0)] {
        let spec = FixtureSpec {
            value_kind: kind,
            ..FixtureSpec::small(&format!("kv_doctor_{kind:?}"), KeyType::Long)
        };
        let fixture = local_fixture(dir.path(), spec).await?;
        let located = protobuf_location(&fixture);
        let located: Vec<&str> = located.iter().map(String::as_str).collect();
        let output = run(&[&["doctor"][..], &located].concat(), &[]);
        assert_exit(&output, EXIT_OK);
        let report = lines(&output.stdout);
        assert_eq!(report[0]["kind"], "table");
        assert_eq!(report[0]["value_columns"][0]["column"], VALUE_COLUMN);
        assert_eq!(report[0]["value_columns"][0]["format"], "protobuf");
        assert_eq!(report[0]["advice"], Json::from(malformed), "{kind:?}");
        let found: Vec<&Json> = report
            .iter()
            .filter(|line| line["kind"] == "finding" && line["code"] == "malformed_protobuf")
            .collect();
        assert_eq!(found.len(), malformed, "{kind:?}: {report:?}");
        for finding in found {
            assert_eq!(finding["column"], VALUE_COLUMN);
            assert!(finding["caps"].is_null(), "advice");
        }
        assert!(
            report
                .iter()
                .all(|line| line["kind"] != "refusal" && line["kind"] != "unwalked")
        );
    }
    Ok(())
}

/// `doctor` reports a protobuf column whose walked page it cannot read as
/// an `unwalked` line naming the column, and still emits the table's
/// report: the walk is advice, so the run exits 0 and nothing is refused.
#[tokio::test]
async fn test_doctor_reports_an_unreadable_walk_as_unwalked_and_the_report_whole() -> TestResult {
    let dir = tempfile::tempdir()?;
    let spec = FixtureSpec {
        value_kind: ValueKind::Protobuf,
        ..FixtureSpec::small("kv_doctor_unwalked", KeyType::Long)
    };
    let fixture = local_fixture(dir.path(), spec).await?;
    let mut zeroed = 0;
    for path in data_files(dir.path())? {
        zeroed += zero_column_chunks(&path, VALUE_COLUMN)?;
    }
    assert!(zeroed > 0, "the value chunks are zeroed");
    let located = protobuf_location(&fixture);
    let located: Vec<&str> = located.iter().map(String::as_str).collect();
    let output = run(&[&["doctor"][..], &located].concat(), &[]);
    assert_exit(&output, EXIT_OK);
    let report = lines(&output.stdout);
    assert_eq!(report[0]["kind"], "table", "{report:?}");
    let unwalked: Vec<&Json> = report
        .iter()
        .filter(|line| line["kind"] == "unwalked")
        .collect();
    let [line] = unwalked[..] else {
        panic!("one unwalked line, got {report:?}");
    };
    assert_eq!(line["column"], VALUE_COLUMN);
    assert!(report.iter().all(|line| line["kind"] != "refusal"));
    Ok(())
}

/// A field list the table's configured format does not take, a field
/// number out of range or given twice, and text that is no field list or
/// no format are usage errors before the load: with every data file gone,
/// so a load fails with exit 1, each still exits 64.
#[tokio::test]
async fn test_get_rejects_a_bad_field_list_or_format_before_the_load() -> TestResult {
    let dir = tempfile::tempdir()?;
    let fixture = local_fixture(
        dir.path(),
        FixtureSpec::small("kv_badfields", KeyType::Long),
    )
    .await?;
    assert!(remove_data_files(dir.path())? > 0);
    let key = fixture.key(SAMPLE[0]).to_string();
    let opaque = location(&fixture);
    let protobuf = protobuf_location(&fixture);
    let protobuf: Vec<&str> = protobuf.iter().map(String::as_str).collect();
    let fields_of = |list: &str| format!("{VALUE_COLUMN}={list}");
    let cases = [
        (&opaque[..], fields_of("1")),
        (&protobuf[..], fields_of("0")),
        (
            &protobuf[..],
            fields_of(&(MAX_FIELD_NUMBER + 1).to_string()),
        ),
        (&protobuf[..], fields_of("1,2,1")),
        (&protobuf[..], fields_of("1,x")),
        (
            &protobuf[..],
            fields_of(&(u64::from(u32::MAX) + 1).to_string()),
        ),
    ];
    for (located, column) in cases {
        let output = run(
            &[&["get"][..], located, &["--column", &column], &[&key]].concat(),
            &[],
        );
        assert_exit(&output, EXIT_USAGE);
        assert!(output.stdout.is_empty(), "{column}");
    }
    let unknown_format = format!("{VALUE_COLUMN}:json");
    let output = run(
        &[
            "get",
            "--location",
            fixture.metadata_location(),
            "--value-column",
            &unknown_format,
            &key,
        ],
        &[],
    );
    assert_exit(&output, EXIT_USAGE);
    Ok(())
}

/// `get` naming two columns prints each one's value under its name, in
/// the order named, and a cold hit costs one GET per column.
#[tokio::test]
async fn test_get_two_columns_prints_their_values_in_the_order_named() -> TestResult {
    let dir = tempfile::tempdir()?;
    let spec = FixtureSpec {
        second_value: true,
        ..FixtureSpec::small("kv_two_columns", KeyType::Long)
    };
    let fixture = local_fixture(dir.path(), spec).await?;
    let key = fixture.key(SAMPLE[0]);
    let encoded = |bytes: Option<Vec<u8>>| {
        Json::String(
            base64::engine::general_purpose::STANDARD.encode(bytes.expect("a present value")),
        )
    };
    let value = encoded(fixture.value(&key));
    let detail = encoded(fixture.second_value(&key));
    assert_ne!(value, detail, "the columns hold different bytes");
    for order in [
        [VALUE_COLUMN, SECOND_VALUE_COLUMN],
        [SECOND_VALUE_COLUMN, VALUE_COLUMN],
    ] {
        let output = run(
            &[
                &["get"][..],
                &location(&fixture),
                &["--value-column", SECOND_VALUE_COLUMN],
                &["--column", order[0], "--column", order[1]],
                &[&key.to_string()],
            ]
            .concat(),
            &[],
        );
        assert_exit(&output, EXIT_OK);
        let got = lines(&output.stdout);
        assert_eq!(got[0]["values"][VALUE_COLUMN]["base64"], value);
        assert_eq!(got[0]["values"][SECOND_VALUE_COLUMN]["base64"], detail);
        assert_eq!(got[0]["gets"], Json::from(order.len()), "a GET per column");
        let text = String::from_utf8(output.stdout)?;
        let at = |column: &str| text.find(&format!("\"{column}\":"));
        assert!(at(order[0]) < at(order[1]), "{order:?} in {text}");
        assert!(at(order[0]).is_some());
    }
    Ok(())
}

#[tokio::test]
async fn test_get_needs_a_table_name_when_several_are_configured() -> TestResult {
    let dir = tempfile::tempdir()?;
    let catalog = memory_catalog_local(dir.path()).await?;
    let a = write_fixture(
        catalog.as_ref(),
        NAMESPACE,
        FixtureSpec::small("kv_a", KeyType::Long),
    )
    .await?;
    let b = write_fixture(
        catalog.as_ref(),
        NAMESPACE,
        FixtureSpec::small("kv_b", KeyType::Long),
    )
    .await?;
    let toml = format!("{}\n{}", toml_table(TABLE_A, &a), toml_table(TABLE_B, &b));
    let config = write_file(dir.path(), "two.toml", &toml)?;
    let key = a.key(SAMPLE[0]).to_string();

    let unnamed = run(&["get", "--config", &config, &key], &[]);
    assert_exit(&unnamed, EXIT_USAGE);
    assert!(unnamed.stdout.is_empty());
    assert!(assert_plain_error(&unnamed), "the tables are the hint");

    let unknown = run(&["get", "--config", &config, "--table", "c", &key], &[]);
    assert_exit(&unknown, EXIT_USAGE);
    assert!(unknown.stdout.is_empty());
    assert!(assert_plain_error(&unknown), "the tables are the hint");

    let named = run(&["get", "--config", &config, "--table", TABLE_A, &key], &[]);
    assert_exit(&named, EXIT_OK);
    let got = lines(&named.stdout);
    assert_eq!(got.len(), 1);
    assert_eq!(got[0]["table"], TABLE_A);
    assert_eq!(got[0]["presence"], "present");
    Ok(())
}

/// The rows a `scan` page of the paging tests holds: fewer than a key's
/// `ORDER_ROWS_PER_KEY`, and not a divisor of them, so the last page is
/// short.
const SCAN_PAGE_ROWS: u32 = 3;

/// The pages a key of `ORDER_ROWS_PER_KEY` rows takes at
/// `SCAN_PAGE_ROWS`, past which a paging loop has run away.
const SCAN_PAGES_AT_MOST: usize = 3;

/// The word `scan --direction` takes for `direction`.
fn direction_flag(direction: Direction) -> &'static str {
    match direction {
        Direction::NewestFirst => "newest",
        Direction::OldestFirst => "oldest",
    }
}

/// The text `--start` and `--before` take for `value`, built without the
/// code under test: a long's digits, a string as it is, which a fixture's
/// zero-padded digits need no escape for, and bytes as `OrderValue`
/// displays them, `0x` and lowercase hex.
fn bound_text(value: &OrderValue) -> String {
    match value {
        OrderValue::Long(value) => value.to_string(),
        OrderValue::Str(text) => {
            assert!(!text.contains(['\\', '\0']), "{text:?} needs no escape");
            text.clone()
        }
        OrderValue::Bytes(_) => value.to_string(),
    }
}

/// The `next` a `scan` line prints for `value`, built without the code under
/// test: a long as a JSON number, bytes as `0x` and lowercase hex, and a
/// string as its text with a zero byte as `\0`; a fixture's strings hold
/// no backslash, the one other character `scan` escapes.
fn next_json(value: &OrderValue) -> Json {
    match value {
        OrderValue::Long(value) => Json::from(*value),
        OrderValue::Str(text) => {
            assert!(!text.contains('\\'), "{text:?} holds no backslash");
            Json::String(text.replace('\0', "\\0"))
        }
        OrderValue::Bytes(_) => Json::String(value.to_string()),
    }
}

/// A `scan` line's `next` as a caller pastes it back, what `jq -r .next`
/// prints: a number's digits or a string's contents.
fn pasted(next: &Json) -> String {
    match next {
        Json::Number(number) => number.to_string(),
        Json::String(text) => text.clone(),
        other @ (Json::Null | Json::Bool(_) | Json::Array(_) | Json::Object(_)) => {
            panic!("next is a number or a string, got {other}")
        }
    }
}

/// One `scan` of `key` reading `VALUE_COLUMN` on the table `target` names,
/// as `scan` asks, the bounds given as `start` and `before` text.
fn scan_run(
    target: &[&str],
    key: &str,
    scan: &TestScan,
    start: Option<&str>,
    before: Option<&str>,
) -> Output {
    let limit = scan.limit.to_string();
    let mut args = vec!["scan"];
    args.extend_from_slice(target);
    args.extend([
        "--direction",
        direction_flag(scan.direction),
        "--limit",
        &limit,
        "--column",
        VALUE_COLUMN,
    ]);
    for (flag, text) in [("--start", start), ("--before", before)] {
        if let Some(text) = text {
            args.extend([flag, text]);
        }
    }
    args.push(key);
    run(&args, &[])
}

/// A `scan` run's stdout: its `row` lines, each of `table` and `key`, and
/// its one `scan` line after them, which counts them.
fn scan_lines(output: &Output, table: &str, key: &str) -> (Vec<Json>, Json) {
    let mut got = lines(&output.stdout);
    let call = got.pop().expect("a scan line");
    assert_eq!(call["kind"], "scan", "{call}");
    assert_eq!(call["table"], table);
    assert_eq!(call["key"], key);
    assert_eq!(call["rows"], Json::from(got.len()), "{call}");
    assert!(call["snapshot_id"].is_string(), "{call}");
    assert!(call["gets"].is_u64() && call["elapsed_micros"].is_u64());
    for row in &got {
        assert_eq!(row["kind"], "row", "{row}");
        assert_eq!(row["table"], table);
        assert_eq!(row["key"], key);
    }
    (got, call)
}

/// Asserts `rows`, a run's `row` lines, are the model's `expected` rows of
/// a fixture ordered by `kind`: each row's order value as the doctor's lines
/// carry one, and its `VALUE_COLUMN` value in base64 with its length, or
/// `null` for a null.
fn assert_rows(rows: &[Json], expected: &ScanRows, kind: OrderKind, context: &str) {
    assert_eq!(rows.len(), expected.len(), "{context}");
    for (row, (order, values)) in rows.iter().zip(expected) {
        assert_eq!(
            row["order"],
            serde_json::to_value(order_value(kind, *order)).expect("an order value serializes"),
            "{context}"
        );
        let value = &row["values"][VALUE_COLUMN];
        match &values[0] {
            Some(bytes) => {
                assert_eq!(
                    value["base64"],
                    Json::String(base64::engine::general_purpose::STANDARD.encode(bytes)),
                    "{context}"
                );
                assert_eq!(value["bytes"], Json::from(bytes.len()), "{context}");
            }
            None => assert_eq!(*value, Json::Null, "{context}"),
        }
    }
}

/// Reads `key`'s rows inside `window` (raw order values) in `direction`,
/// one page of `SCAN_PAGE_ROWS` at a time, each page after the first
/// bounded by the `next` the page before printed, pasted back as `scan`
/// says: as `--before` newest first and as `--start` oldest first. The
/// first page and its `next` are the model's (`Fixture::expected_scan`),
/// and the pages together are every row of the window, as the model gives
/// them unpaged.
fn assert_pages_to_the_end(
    target: &[&str],
    table: &str,
    fixture: &Fixture,
    kind: OrderKind,
    direction: Direction,
    window: (Option<i64>, Option<i64>),
) {
    let key = fixture.key(SAMPLE[1]);
    let key_text = key.to_string();
    let page = TestScan {
        direction,
        limit: SCAN_PAGE_ROWS,
        start: window.0,
        before: window.1,
    };
    let context = format!("{kind:?} {direction:?} {window:?}");
    let text = |raw: Option<i64>| raw.map(|raw| bound_text(&order_value(kind, raw)));
    let (mut start, mut before) = (text(page.start), text(page.before));
    let (first_rows, first_last) = fixture.expected_scan(&key, &[VALUE_COLUMN], &page);
    let mut read: Vec<Json> = Vec::new();
    for at in 0.. {
        assert!(
            at < SCAN_PAGES_AT_MOST,
            "{context}: the pages ran past the end"
        );
        let output = scan_run(
            target,
            &key_text,
            &page,
            start.as_deref(),
            before.as_deref(),
        );
        assert_exit(&output, EXIT_OK);
        let (rows, call) = scan_lines(&output, table, &key_text);
        if at == 0 {
            assert_rows(&rows, &first_rows, kind, &context);
            let expected = first_last.and_then(|last| page.next_after(kind, last));
            match (expected, call.get("next")) {
                (None, None) => {}
                (Some(expected), Some(next)) => {
                    assert_eq!(next, &next_json(&expected), "{context}");
                }
                (expected, next) => panic!("{context}: next {next:?}, the model's {expected:?}"),
            }
        }
        read.extend(rows);
        let Some(next) = call.get("next") else {
            break;
        };
        match direction {
            Direction::NewestFirst => before = Some(pasted(next)),
            Direction::OldestFirst => start = Some(pasted(next)),
        }
    }
    let every_row = TestScan {
        limit: DEFAULT_MAX_SCAN_ROWS,
        ..page
    };
    let (all, last) = fixture.expected_scan(&key, &[VALUE_COLUMN], &every_row);
    assert_eq!(
        last, None,
        "{context}: the model's one page holds every row"
    );
    assert!(!all.is_empty(), "{context}: the window holds rows");
    assert_rows(&read, &all, kind, &context);
}

/// `scan` reads a key of an ordered table in either direction, inside
/// every kind of window, and paging by each `scan` line's `next`, pasted
/// back, reads every row of the window exactly once, as the model holds
/// them, ending on a page with no `next`.
#[tokio::test]
async fn test_scan_pages_by_next_to_the_end_in_both_directions_and_windows() -> TestResult {
    let dir = tempfile::tempdir()?;
    let kind = OrderKind::Timestamp;
    let fixture = local_fixture(dir.path(), ordered_spec("events_scan", ORDER_STEP)).await?;
    let config = ordered_config(dir.path(), "scan", &fixture)?;
    let target = ["--config", config.as_str()];
    let second = ORDER_START + ORDER_STEP;
    let last = ORDER_START + ORDER_STEP * i64::try_from(ORDER_ROWS_PER_KEY - 1)?;
    let windows = [
        (None, None),
        (Some(second), None),
        (None, Some(last)),
        (Some(second), Some(last)),
        (Some(second + 1), Some(last - 1)),
    ];
    for direction in [Direction::NewestFirst, Direction::OldestFirst] {
        for window in windows {
            assert_pages_to_the_end(&target, TABLE_A, &fixture, kind, direction, window);
        }
    }
    Ok(())
}

/// The width of the fixed order column the paging test reads: the fewest
/// bytes that hold every raw order value of its fixture.
const FIXED_ORDER_BYTES: u32 = 2;

/// A string order column pages oldest first through a `next` that ends in
/// a zero byte, printed as `\0`, a binary one through hex, a fixed one
/// through hex one byte wider than the column oldest first, and a date,
/// an int32 kind, through its days, on a table a configuration file names
/// and on one `--location` opens with `--order-column`.
#[tokio::test]
async fn test_scan_pages_string_binary_fixed_and_date_orders_by_their_next() -> TestResult {
    let dir = tempfile::tempdir()?;
    let kinds = [
        (OrderKind::Str, "events_str"),
        (OrderKind::Bin, "events_bin"),
        (OrderKind::Fixed(FIXED_ORDER_BYTES), "events_fixed"),
        (OrderKind::Date, "events_date"),
    ];
    for (kind, name) in kinds {
        let fixture = local_fixture(
            dir.path(),
            ordered_spec_of(name, kind, ORDER_START, ORDER_STEP),
        )
        .await?;
        let config = ordered_config(dir.path(), name, &fixture)?;
        let located = [
            "--location",
            fixture.metadata_location(),
            "--value-column",
            VALUE_COLUMN,
            "--order-column",
            ORDER_COLUMN,
        ];
        let targets = [
            (&["--config", config.as_str()][..], TABLE_A),
            (&located[..], fixture.ident().name()),
        ];
        for (target, table) in targets {
            for direction in [Direction::NewestFirst, Direction::OldestFirst] {
                assert_pages_to_the_end(target, table, &fixture, kind, direction, (None, None));
            }
        }
    }
    let fixture = local_fixture(
        dir.path(),
        ordered_spec_of("events_text", OrderKind::Str, ORDER_START, ORDER_STEP),
    )
    .await?;
    let config = ordered_config(dir.path(), "text", &fixture)?;
    let key = fixture.key(SAMPLE[1]).to_string();
    let oldest = TestScan {
        direction: Direction::OldestFirst,
        limit: 1,
        start: None,
        before: None,
    };
    let output = scan_run(&["--config", &config], &key, &oldest, None, None);
    assert_exit(&output, EXIT_OK);
    let (rows, call) = scan_lines(&output, TABLE_A, &key);
    let Json::String(first) = &rows[0]["order"] else {
        panic!("a string order value: {}", rows[0]);
    };
    assert_eq!(
        call["next"],
        Json::String(format!("{first}\\0")),
        "the least string above the first, its zero byte escaped"
    );
    Ok(())
}

/// A key the table holds no row of is an answer: a `scan` line of no rows
/// and no `next`, exit 0.
#[tokio::test]
async fn test_scan_of_a_key_with_no_rows_prints_one_scan_line() -> TestResult {
    let dir = tempfile::tempdir()?;
    let fixture = local_fixture(dir.path(), ordered_spec("events_none", ORDER_STEP)).await?;
    let config = ordered_config(dir.path(), "none", &fixture)?;
    let key = fixture.key(IDS_2K.end).to_string();
    let output = scan_run(
        &["--config", &config],
        &key,
        &TestScan::default(),
        None,
        None,
    );
    assert_exit(&output, EXIT_OK);
    let (rows, call) = scan_lines(&output, TABLE_A, &key);
    assert!(rows.is_empty());
    assert_eq!(call["rows"], Json::from(0));
    assert!(call.get("next").is_none(), "{call}");
    Ok(())
}

/// `get` of an ordered table and `scan` of a key-value one are usage
/// errors found from the configuration, before the load: with every data
/// file gone, so a load fails with exit 1, each exits 64 with a hint
/// naming the command that reads the table. A `scan` of a `--location`
/// table with no `--order-column` is clap's usage error, which lists
/// that flag alone as missing.
#[tokio::test]
async fn test_get_of_an_ordered_table_and_scan_of_a_key_value_one_are_usage_errors() -> TestResult {
    let ordered_dir = tempfile::tempdir()?;
    let ordered =
        local_fixture(ordered_dir.path(), ordered_spec("events_kind", ORDER_STEP)).await?;
    let config = ordered_config(ordered_dir.path(), "kind", &ordered)?;
    assert!(remove_data_files(ordered_dir.path())? > 0);
    let key = ordered.key(SAMPLE[1]).to_string();
    let get = run(&["get", "--config", &config, &key], &[]);
    assert_exit(&get, EXIT_USAGE);
    assert!(get.stdout.is_empty());
    assert!(assert_plain_error(&get), "a hint follows the error");
    let located_get = run(
        &[
            "get",
            "--location",
            ordered.metadata_location(),
            "--order-column",
            ORDER_COLUMN,
            &key,
        ],
        &[],
    );
    assert_exit(&located_get, EXIT_USAGE);
    let scan_ordered = scan_run(
        &["--config", &config],
        &key,
        &TestScan::default(),
        None,
        None,
    );
    assert_exit(&scan_ordered, EXIT_FAILURE);

    let kv_dir = tempfile::tempdir()?;
    let kv = local_fixture(kv_dir.path(), FixtureSpec::small("kv_kind", KeyType::Long)).await?;
    let kv_config = write_file(kv_dir.path(), "kv.toml", &toml_table(TABLE_A, &kv))?;
    assert!(remove_data_files(kv_dir.path())? > 0);
    let key = kv.key(SAMPLE[0]).to_string();
    let scan = scan_run(
        &["--config", &kv_config],
        &key,
        &TestScan::default(),
        None,
        None,
    );
    assert_exit(&scan, EXIT_USAGE);
    assert!(scan.stdout.is_empty());
    assert!(assert_plain_error(&scan), "a hint follows the error");
    let located_scan = scan_run(&location(&kv), &key, &TestScan::default(), None, None);
    assert_exit(&located_scan, EXIT_USAGE);
    assert!(located_scan.stdout.is_empty());
    assert_eq!(missing_arguments(&located_scan), ["--order-column"]);
    Ok(())
}

/// Every fault of a `scan` request that needs no data file is a usage
/// error before the load: a direction or a limit clap does not take, a
/// limit of 0 or above the table's `max_scan_rows`, a bound or a key that
/// is not text of the table's type, and an empty window. With every data
/// file gone, the same request without its fault exits 1, so each 64 came
/// before the load.
#[tokio::test]
async fn test_scan_refuses_a_bad_direction_limit_bound_or_key_before_the_load() -> TestResult {
    let dir = tempfile::tempdir()?;
    let fixture = local_fixture(dir.path(), ordered_spec("events_bad", ORDER_STEP)).await?;
    let config = ordered_config(dir.path(), "bad", &fixture)?;
    assert!(remove_data_files(dir.path())? > 0);
    let key = fixture.key(SAMPLE[1]).to_string();
    let above = (DEFAULT_MAX_SCAN_ROWS + 1).to_string();
    let start = ORDER_START.to_string();
    let base = ["scan", "--config", config.as_str()];
    let cases: [(&[&str], bool); 9] = [
        (&["--direction", "up", "--limit", "1", &key], false),
        (&["--direction", "newest", "--limit", "x", &key], false),
        (&["--direction", "newest", "--limit", "0", &key], false),
        (&["--direction", "newest", "--limit", &above, &key], false),
        (
            &[
                "--direction",
                "newest",
                "--limit",
                "1",
                "--start",
                "abc",
                &key,
            ],
            true,
        ),
        (
            &[
                "--direction",
                "oldest",
                "--limit",
                "1",
                "--before",
                "1.5",
                &key,
            ],
            true,
        ),
        (
            &[
                "--direction",
                "oldest",
                "--limit",
                "1",
                "--start",
                &start,
                "--before",
                &start,
                &key,
            ],
            false,
        ),
        (&["--direction", "newest", "--limit", "1", "abc"], true),
        (&["--direction", "newest", "--limit", "1"], false),
    ];
    for (extra, hinted) in cases {
        let output = run(&[&base[..], extra].concat(), &[]);
        assert_exit(&output, EXIT_USAGE);
        assert!(output.stdout.is_empty(), "{extra:?}");
        if hinted {
            assert!(
                assert_plain_error(&output),
                "{extra:?}: the form is the hint"
            );
        }
    }
    let unloadable = run(
        &[&base[..], &["--direction", "newest", "--limit", "1", &key]].concat(),
        &[],
    );
    assert_exit(&unloadable, EXIT_FAILURE);
    Ok(())
}

/// 2026-01-01T00:00:00Z in each unit a time order type counts since the
/// epoch: days, microseconds, nanoseconds.
const NEW_YEAR_2026_DAYS: i64 = 20_454;
const NEW_YEAR_2026_MICROS: i64 = 1_767_225_600_000_000;
const NEW_YEAR_2026_NANOS: i64 = 1_767_225_600_000_000_000;

/// Per time order kind: the raw value of a key's first row, its rows one
/// unit apart, and the ISO text of its second row's value and of its last
/// row's, the window `--start` and `--before` bound. The last row is row
/// 6, one unit per row after the first, since a key has
/// `ORDER_ROWS_PER_KEY` rows, which the test asserts.
const ISO_WINDOWS: [(OrderKind, i64, &str, &str); 5] = [
    (
        OrderKind::Date,
        NEW_YEAR_2026_DAYS,
        "2026-01-02",
        "2026-01-07",
    ),
    (
        OrderKind::Timestamp,
        NEW_YEAR_2026_MICROS,
        "2026-01-01T00:00:00.000001Z",
        "2026-01-01T00:00:00.000006Z",
    ),
    (
        OrderKind::TimestampTz,
        NEW_YEAR_2026_MICROS,
        "2026-01-01T01:00:00.000001+01:00",
        "2026-01-01T00:00:00.000006000Z",
    ),
    (
        OrderKind::TimestampNs,
        NEW_YEAR_2026_NANOS,
        "2026-01-01T00:00:00.000000001Z",
        "2026-01-01T00:00:00.000000006Z",
    ),
    (
        OrderKind::TimestampTzNs,
        NEW_YEAR_2026_NANOS,
        "2025-12-31T19:00:00.000000001-05:00",
        "2026-01-01T00:00:00.000000006Z",
    ),
];

/// A date or timestamp bound given as ISO 8601 text bounds the same window
/// as the long of the column's unit it names: per time type and direction,
/// the two runs print the same `row` lines, the model's rows inside the
/// window, and the same `next`, a long either way.
#[tokio::test]
async fn test_scan_takes_iso_bounds_for_time_orders_as_the_longs_they_name() -> TestResult {
    let dir = tempfile::tempdir()?;
    assert_eq!(ORDER_ROWS_PER_KEY, 7, "ISO_WINDOWS names row 6 as the last");
    let after_first = 1;
    let after_last = i64::try_from(ORDER_ROWS_PER_KEY - 1)?;
    for (kind, first, iso_start, iso_before) in ISO_WINDOWS {
        let name = format!("events_iso_{kind:?}").to_lowercase();
        let fixture = local_fixture(dir.path(), ordered_spec_of(&name, kind, first, 1)).await?;
        let config = ordered_config(dir.path(), &name, &fixture)?;
        let key = fixture.key(SAMPLE[1]);
        let key_text = key.to_string();
        let (start, before) = (first + after_first, first + after_last);
        let (long_start, long_before) = (start.to_string(), before.to_string());
        for direction in [Direction::NewestFirst, Direction::OldestFirst] {
            let page = TestScan {
                direction,
                limit: SCAN_PAGE_ROWS,
                start: Some(start),
                before: Some(before),
            };
            let context = format!("{kind:?} {direction:?}");
            let target = ["--config", config.as_str()];
            let by_iso = scan_run(&target, &key_text, &page, Some(iso_start), Some(iso_before));
            let by_long = scan_run(
                &target,
                &key_text,
                &page,
                Some(&long_start),
                Some(&long_before),
            );
            assert_exit(&by_iso, EXIT_OK);
            assert_exit(&by_long, EXIT_OK);
            let (iso_rows, iso_call) = scan_lines(&by_iso, TABLE_A, &key_text);
            let (long_rows, long_call) = scan_lines(&by_long, TABLE_A, &key_text);
            let (expected, last) = fixture.expected_scan(&key, &[VALUE_COLUMN], &page);
            assert!(!expected.is_empty(), "{context}: the window holds rows");
            assert_rows(&iso_rows, &expected, kind, &context);
            assert_eq!(iso_rows, long_rows, "{context}");
            let next = last
                .and_then(|last| page.next_after(kind, last))
                .map(|next| next_json(&next));
            assert!(next.is_some(), "{context}: rows remain past the page");
            assert_eq!(iso_call.get("next"), next.as_ref(), "{context}");
            assert_eq!(long_call.get("next"), next.as_ref(), "{context}");
        }
    }
    Ok(())
}

/// Digits alone on a `date` column are a day count, whatever date it
/// falls on: `20260101` is day 20,260,101, past every row of a fixture
/// whose rows start at 2026-01-01 and past the dates ISO text names, not
/// 2026-01-01, so as `--start` it reads no row and as `--before` every
/// row, as the model holds them.
#[tokio::test]
async fn test_scan_reads_digits_alone_on_a_date_column_as_a_day_count() -> TestResult {
    let dir = tempfile::tempdir()?;
    let kind = OrderKind::Date;
    let fixture = local_fixture(
        dir.path(),
        ordered_spec_of("events_day_count", kind, NEW_YEAR_2026_DAYS, 1),
    )
    .await?;
    let config = ordered_config(dir.path(), "day_count", &fixture)?;
    let key = fixture.key(SAMPLE[1]);
    let key_text = key.to_string();
    let day_count = 20_260_101;
    let text = day_count.to_string();
    let limit = u32::try_from(ORDER_ROWS_PER_KEY)?;
    for (start, before) in [(Some(day_count), None), (None, Some(day_count))] {
        let page = TestScan {
            direction: Direction::OldestFirst,
            limit,
            start,
            before,
        };
        let output = scan_run(
            &["--config", &config],
            &key_text,
            &page,
            start.map(|_| text.as_str()),
            before.map(|_| text.as_str()),
        );
        assert_exit(&output, EXIT_OK);
        let (rows, call) = scan_lines(&output, TABLE_A, &key_text);
        let (expected, _) = fixture.expected_scan(&key, &[VALUE_COLUMN], &page);
        let context = format!("start {start:?} before {before:?}");
        assert_eq!(
            expected.len(),
            if start.is_some() {
                0
            } else {
                approx::length(ORDER_ROWS_PER_KEY)
            },
            "{context}"
        );
        assert_rows(&rows, &expected, kind, &context);
        assert!(call.get("next").is_none(), "{call}");
    }
    Ok(())
}

/// ISO 8601 text a time bound's type cannot hold, ISO text in any form
/// but the one, and ISO text for a `long` order column are usage errors
/// before the load, with the form as the hint: digits below a microsecond for a `timestamp`, an instant
/// without an offset, with a space for `T`, or with a zone annotation, a
/// leap second, and a year past 9999.
#[tokio::test]
async fn test_scan_refuses_iso_bounds_a_column_s_type_cannot_hold() -> TestResult {
    let dir = tempfile::tempdir()?;
    let cases = [
        (
            OrderKind::Timestamp,
            "--start",
            "2026-01-01T00:00:00.0000001Z",
        ),
        (OrderKind::Timestamp, "--before", "2026-01-01T00:00:00"),
        (OrderKind::Timestamp, "--start", "2026-01-01 00:00:00Z"),
        (
            OrderKind::TimestampTz,
            "--start",
            "2026-01-01T00:00:00Z[America/New_York]",
        ),
        (OrderKind::TimestampTz, "--before", "2016-12-31T23:59:60Z"),
        (OrderKind::Date, "--start", "10000-01-01"),
        (OrderKind::TimestampNs, "--before", "2263-01-01T00:00:00Z"),
        (OrderKind::Long, "--start", "2026-01-01T00:00:00Z"),
        (OrderKind::Long, "--before", "2026-01-01"),
    ];
    for (at, (kind, flag, text)) in cases.into_iter().enumerate() {
        let name = format!("events_iso_bad_{at}");
        let fixture = local_fixture(
            dir.path(),
            ordered_spec_of(&name, kind, ORDER_START, ORDER_STEP),
        )
        .await?;
        let config = ordered_config(dir.path(), &name, &fixture)?;
        assert!(remove_data_files(dir.path())? > 0);
        let key = fixture.key(SAMPLE[1]).to_string();
        let output = run(
            &[
                "scan",
                "--config",
                &config,
                "--direction",
                "newest",
                "--limit",
                "1",
                flag,
                text,
                &key,
            ],
            &[],
        );
        assert_exit(&output, EXIT_USAGE);
        assert!(output.stdout.is_empty(), "{kind:?} {flag} {text}");
        assert!(
            assert_plain_error(&output),
            "{kind:?} {flag} {text}: the form is the hint"
        );
    }
    Ok(())
}

/// A `scan` whose lines carry a long order value past 2^53, on a `row`
/// line or as `next`, logs the one warning for its table on stderr, and
/// one whose values stay within it logs none; stdout carries the exact
/// number either way.
#[tokio::test]
async fn test_scan_warns_when_a_line_carries_an_order_past_2_pow_53() -> TestResult {
    let dir = tempfile::tempdir()?;
    for (name, last, warns) in [
        ("scan_at_limit", EXACT_ORDER, false),
        ("scan_past_limit", EXACT_ORDER + 1, true),
    ] {
        let fixture = nanosecond_fixture(dir.path(), name, last).await?;
        let config = ordered_config(dir.path(), name, &fixture)?;
        let key = fixture.key(SAMPLE[1]).to_string();
        let newest = TestScan {
            limit: 1,
            ..TestScan::default()
        };
        let output = scan_run(&["--config", &config], &key, &newest, None, None);
        assert_exit(&output, EXIT_OK);
        let expected: &[String] = if warns { &[TABLE_A.to_string()] } else { &[] };
        assert_eq!(precision_warnings(&output), expected, "{name}");
        let (rows, call) = scan_lines(&output, TABLE_A, &key);
        assert_eq!(rows[0]["order"], Json::from(last), "{name}");
        assert_eq!(call["next"], Json::from(last), "{name}");
    }
    Ok(())
}

#[tokio::test]
async fn test_config_toml_and_json_open_the_same_table() -> TestResult {
    let dir = tempfile::tempdir()?;
    let fixture = local_fixture(dir.path(), FixtureSpec::small("kv_both", KeyType::Long)).await?;
    let toml = write_file(dir.path(), "one.toml", &toml_table(TABLE_A, &fixture))?;
    let json = serde_json::json!({ "tables": [json_table(TABLE_A, &fixture)] }).to_string();
    let json = write_file(dir.path(), "one.json", &json)?;
    let from_toml = run(&["info", "--config", &toml], &[]);
    let from_json = run(&["info", "--config", &json], &[]);
    assert_exit(&from_toml, EXIT_OK);
    assert_exit(&from_json, EXIT_OK);
    assert_eq!(from_toml.stdout, from_json.stdout);
    let info = lines(&from_toml.stdout);
    assert_eq!(info.len(), 1);
    assert_eq!(info[0]["table"], TABLE_A);
    Ok(())
}

/// Flags that describe a located table, no target at all, and `--table`
/// beside `--location`, which opens one table rather than selecting among
/// configured ones, are each a usage error before anything is opened.
#[tokio::test]
async fn test_location_only_flags_are_rejected_under_config() -> TestResult {
    let dir = tempfile::tempdir()?;
    let fixture = local_fixture(dir.path(), FixtureSpec::small("kv_flags", KeyType::Long)).await?;
    let config = write_file(dir.path(), "one.toml", &toml_table(TABLE_A, &fixture))?;

    let flagged = run(&["info", "--config", &config, "--key-column", "key"], &[]);
    assert_exit(&flagged, EXIT_USAGE);
    assert!(flagged.stdout.is_empty());

    let targetless = run(&["info"], &[]);
    assert_exit(&targetless, EXIT_USAGE);
    assert!(targetless.stdout.is_empty());

    let selected = run(
        &[&["info"][..], &location(&fixture), &["--table", "kv_flags"]].concat(),
        &[],
    );
    assert_exit(&selected, EXIT_USAGE);
    assert!(selected.stdout.is_empty());
    assert!(
        assert_plain_error(&selected),
        "the choice between the two is the hint"
    );
    Ok(())
}

/// A `--location` run reads no configuration file, so the variable that
/// names one is ignored; a run with no target flag at all reads it.
#[tokio::test]
async fn test_location_ignores_the_environment_and_a_bare_run_reads_it() -> TestResult {
    let dir = tempfile::tempdir()?;
    let fixture = local_fixture(dir.path(), FixtureSpec::small("kv_env", KeyType::Long)).await?;
    let missing = dir.path().join("missing.toml");
    let missing = missing.to_str().expect("a utf-8 temporary path");
    let located = run(
        &[&["info"][..], &location(&fixture)].concat(),
        &[(ENV_CONFIG, missing)],
    );
    assert_exit(&located, EXIT_OK);
    assert_eq!(lines(&located.stdout)[0]["table"], fixture.ident().name());

    let config = write_file(dir.path(), "env.toml", &toml_table(TABLE_A, &fixture))?;
    let bare = run(&["info"], &[(ENV_CONFIG, &config)]);
    assert_exit(&bare, EXIT_OK);
    assert_eq!(lines(&bare.stdout)[0]["table"], TABLE_A);
    Ok(())
}

/// With no `--config` and `ICETRAY_CONFIG` unset or empty, every command,
/// `serve` and each one-shot, has no configuration: a usage error, exit 64,
/// before anything is opened. A path that names no file is the file's
/// failure, exit 1, whether the flag or the variable named it.
#[test]
fn test_every_command_treats_an_empty_config_variable_as_unset() -> TestResult {
    let dir = tempfile::tempdir()?;
    let missing = dir.path().join("missing.toml");
    let missing = missing.to_str().expect("a utf-8 temporary path");
    let commands: [&[&str]; 4] = [&["serve"], &["doctor"], &["info"], &["get", "1"]];
    for args in commands {
        for env in [&[][..], &[(ENV_CONFIG, "")]] {
            let output = run(args, env);
            assert_exit(&output, EXIT_USAGE);
            assert!(output.stdout.is_empty(), "{args:?} {env:?}");
            assert_plain_error(&output);
        }
        let from_env = run(args, &[(ENV_CONFIG, missing)]);
        assert_exit(&from_env, EXIT_FAILURE);
        assert_plain_error(&from_env);
        let from_flag = run(&[args, &["--config", missing]].concat(), &[]);
        assert_exit(&from_flag, EXIT_FAILURE);
        assert_plain_error(&from_flag);
    }
    Ok(())
}

/// A `--location` table runs full or bloom; `disk` is not among the
/// residencies it offers, so clap refuses it as a usage error before
/// anything is opened. A file's disk table with no local directory is the
/// configuration violation, exit 1.
#[tokio::test]
async fn test_location_offers_full_and_bloom_and_refuses_disk_as_usage() -> TestResult {
    let dir = tempfile::tempdir()?;
    let fixture = local_fixture(dir.path(), FixtureSpec::small("kv_disk", KeyType::Long)).await?;
    let located = |residency: &str| {
        run(
            &[
                &["info"][..],
                &location(&fixture),
                &["--residency", residency],
            ]
            .concat(),
            &[],
        )
    };
    let disk = located("disk");
    assert_exit(&disk, EXIT_USAGE);
    assert!(disk.stdout.is_empty(), "{}", stderr(&disk));
    for residency in ["full", "bloom"] {
        let output = located(residency);
        assert_exit(&output, EXIT_OK);
        assert_eq!(lines(&output.stdout)[0]["residency"], residency);
    }

    let disk_table = format!("{}residency = \"disk\"\n", toml_table(TABLE_A, &fixture));
    let config = write_file(dir.path(), "disk.toml", &disk_table)?;
    let from_file = run(&["doctor", "--config", &config], &[]);
    assert_exit(&from_file, EXIT_FAILURE);
    assert!(from_file.stdout.is_empty(), "{}", stderr(&from_file));
    Ok(())
}

/// A log field mapping the configuration refuses, a key that names no
/// standard field or two standard fields under one name, is a configuration
/// mistake like any other: refused before anything is opened, exit 1.
#[tokio::test]
async fn test_a_bad_log_field_mapping_exits_1() -> TestResult {
    let dir = tempfile::tempdir()?;
    let fixture = local_fixture(dir.path(), FixtureSpec::small("kv_log", KeyType::Long)).await?;
    let mappings = [
        ("unknown.toml", "timestamp = \"ts\"\n"),
        ("collision.toml", "time = \"x\"\nlevel = \"x\"\n"),
    ];
    for (name, fields) in mappings {
        let text = format!(
            "[server.log.fields]\n{fields}\n{}",
            toml_table(TABLE_A, &fixture)
        );
        let config = write_file(dir.path(), name, &text)?;
        let output = run(&["info", "--config", &config], &[]);
        assert_exit(&output, EXIT_FAILURE);
        assert!(output.stdout.is_empty(), "{name}: {}", stderr(&output));
    }
    Ok(())
}

/// A file that is not TOML is the file's failure, exit 1, reported as one
/// plain error line like every other: the parser's place and message, not
/// its excerpt of the file over several lines.
#[test]
fn test_a_toml_syntax_error_is_one_error_line() -> TestResult {
    let dir = tempfile::tempdir()?;
    let config = write_file(dir.path(), "broken.toml", "[server]\nlisten = \n")?;
    let output = run(&["info", "--config", &config], &[]);
    assert_exit(&output, EXIT_FAILURE);
    assert!(output.stdout.is_empty(), "{}", stderr(&output));
    assert!(!assert_plain_error(&output), "{}", stderr(&output));
    Ok(())
}

/// A location no store serves, one under a scheme icetray builds no store
/// for, is the command line's fault as `--location`, exit 64, and the
/// file's as a table's location, exit 1; both reported before anything is
/// read, as one plain error line.
#[test]
fn test_a_location_no_store_serves_is_usage_on_the_command_line_and_failure_in_the_file()
-> TestResult {
    let dir = tempfile::tempdir()?;
    let unserved = "gs://bucket/db/kv";
    let located = run(&["info", "--location", unserved], &[]);
    assert_exit(&located, EXIT_USAGE);
    assert!(!assert_plain_error(&located), "{}", stderr(&located));
    let text = format!(
        "[[tables]]\nname = \"kv\"\nsource = {{ type = \"static\", location = \"{unserved}\" }}\nref = \"main\"\nkey_column = \"key\"\nvalue_columns = [\"value\"]\n"
    );
    let config = write_file(dir.path(), "unserved.toml", &text)?;
    let configured = run(&["info", "--config", &config], &[]);
    assert_exit(&configured, EXIT_FAILURE);
    assert!(!assert_plain_error(&configured), "{}", stderr(&configured));
    Ok(())
}

/// An empty location, which as a path would be the filesystem root, is
/// refused before anything is read: as `--location`, a usage error, exit
/// 64; as a table's location in a file, the file's, exit 1.
#[test]
fn test_an_empty_location_is_usage_on_the_command_line_and_failure_in_the_file() -> TestResult {
    let dir = tempfile::tempdir()?;
    let located = run(&["info", "--location", ""], &[]);
    assert_exit(&located, EXIT_USAGE);
    assert!(located.stdout.is_empty(), "{}", stderr(&located));
    let text = "[[tables]]\nname = \"kv\"\nsource = { type = \"static\", location = \"\" }\nref = \"main\"\nkey_column = \"key\"\nvalue_columns = [\"value\"]\n";
    let config = write_file(dir.path(), "empty-location.toml", text)?;
    let configured = run(&["info", "--config", &config], &[]);
    assert_exit(&configured, EXIT_FAILURE);
    assert!(!assert_plain_error(&configured), "{}", stderr(&configured));
    Ok(())
}

/// A `--rewrite` rule for a prefix the file also maps is tried first, so it
/// wins: the file's rule alone sends every read to a directory that does not
/// exist, and the command line's rule for the same prefix keeps the reads
/// where the table is.
#[tokio::test]
async fn test_a_command_line_rewrite_takes_precedence_over_the_file_s() -> TestResult {
    let dir = tempfile::tempdir()?;
    let fixture =
        local_fixture(dir.path(), FixtureSpec::small("kv_rewrite", KeyType::Long)).await?;
    let warehouse = format!("file://{}/", dir.path().display());
    let moved = format!("file://{}/", dir.path().join("moved").display());
    let text = format!(
        "[[storage.rewrite]]\nfrom = \"{warehouse}\"\nto = \"{moved}\"\n\n{}",
        toml_table(TABLE_A, &fixture)
    );
    let config = write_file(dir.path(), "rewrite.toml", &text)?;

    let file_rule_only = run(&["info", "--config", &config], &[]);
    assert_exit(&file_rule_only, EXIT_FAILURE);

    let identity = format!("{warehouse}={warehouse}");
    let overridden = run(&["info", "--config", &config, "--rewrite", &identity], &[]);
    assert_exit(&overridden, EXIT_OK);
    assert_eq!(lines(&overridden.stdout)[0]["table"], TABLE_A);
    Ok(())
}

/// A `--rewrite` rule that is not `FROM=TO` with both halves set is a
/// usage error, refused before anything is opened with exit 64; the same
/// rule in the file is the file's fault, exit 1.
#[tokio::test]
async fn test_an_unusable_rewrite_is_usage_on_the_command_line_and_failure_in_the_file()
-> TestResult {
    let dir = tempfile::tempdir()?;
    let fixture = local_fixture(dir.path(), FixtureSpec::small("kv_empty", KeyType::Long)).await?;
    for rule in [
        "=file:///elsewhere/",
        "file:///elsewhere/=",
        "file:///elsewhere/",
    ] {
        let output = run(
            &[&["info"][..], &location(&fixture), &["--rewrite", rule]].concat(),
            &[],
        );
        assert_exit(&output, EXIT_USAGE);
        assert!(output.stdout.is_empty(), "{rule}: {}", stderr(&output));
    }
    let text = format!(
        "[[storage.rewrite]]\nfrom = \"\"\nto = \"file:///elsewhere/\"\n\n{}",
        toml_table(TABLE_A, &fixture)
    );
    let config = write_file(dir.path(), "empty-from.toml", &text)?;
    let output = run(&["info", "--config", &config], &[]);
    assert_exit(&output, EXIT_FAILURE);
    assert!(output.stdout.is_empty(), "{}", stderr(&output));
    Ok(())
}

#[tokio::test]
async fn test_broken_pipe_ends_the_run_with_exit_0() -> TestResult {
    let dir = tempfile::tempdir()?;
    let fixture = local_fixture(dir.path(), FixtureSpec::small("kv_pipe", KeyType::Long)).await?;
    let keys: Vec<String> = (0..MANY_KEYS)
        .map(|id| fixture.key(id).to_string())
        .collect();
    let mut child = Command::new(BIN)
        .arg("get")
        .args(location(&fixture))
        .args(&keys)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()?;
    let stdout = child.stdout.take().expect("stdout is piped");
    let mut reader = BufReader::new(stdout);
    let mut first = String::new();
    reader.read_line(&mut first)?;
    let line: Json = serde_json::from_str(&first)?;
    assert_eq!(line["kind"], "lookup");
    drop(reader);
    let output = child.wait_with_output()?;
    assert_exit(&output, EXIT_OK);
    Ok(())
}

/// A failure reported to a stderr whose reader is gone ends with the
/// failure's own exit code: the usage code for a run with no target, the
/// failure code for a location that cannot be read. The pipe's read end is
/// closed before the binary starts, so every write to stderr fails.
#[test]
fn test_a_closed_stderr_keeps_the_failure_s_exit_code() -> TestResult {
    let dir = tempfile::tempdir()?;
    let missing = dir.path().join("missing").join("kv");
    let missing = missing.to_str().expect("a utf-8 temporary path");
    let cases: [(&[&str], u8); 2] = [
        (&["info"], EXIT_USAGE),
        (&["info", "--location", missing], EXIT_FAILURE),
    ];
    for (args, expected) in cases {
        let (reader, writer) = std::io::pipe()?;
        drop(reader);
        let status = Command::new(BIN)
            .args(args)
            .env_remove(ENV_CONFIG)
            .stdout(Stdio::null())
            .stderr(writer)
            .status()?;
        assert_eq!(status.code(), Some(i32::from(expected)), "{args:?}");
    }
    Ok(())
}

/// One line of the daemon's log, checked for the configured field names.
fn check_log_line(line: &Json) {
    let object = line.as_object().expect("a JSON object per line");
    for field in [TIME_FIELD, LEVEL_FIELD, MESSAGE_FIELD] {
        assert!(object.contains_key(field), "{line}");
    }
    assert_eq!(line[SERVICE_FIELD], SERVICE_VALUE, "{line}");
    for default in ["time", "level", "message"] {
        assert!(!object.contains_key(default), "{line}");
    }
}

/// A daemon that cannot bind its gRPC address fails its start, and the
/// failure reaches exit 1 like any other: the address is held by this test
/// for the length of the run.
#[tokio::test(flavor = "multi_thread")]
async fn test_serve_exits_1_when_its_listener_is_taken() -> TestResult {
    let dir = tempfile::tempdir()?;
    let fixture = local_fixture(dir.path(), FixtureSpec::small("kv_bind", KeyType::Long)).await?;
    let taken = std::net::TcpListener::bind("127.0.0.1:0")?;
    let mut config: Json = serde_json::from_str(&json_config(TABLE_A, &fixture))?;
    config["server"]["listen"] = Json::String(taken.local_addr()?.to_string());
    let path = write_file(dir.path(), "serve.json", &config.to_string())?;
    let output = run(&["serve", "--config", &path], &[]);
    assert_exit(&output, EXIT_FAILURE);
    Ok(())
}

/// The variable tokio's multi-thread runtime reads its worker count from.
const TOKIO_WORKER_THREADS: &str = "TOKIO_WORKER_THREADS";

/// A cache budget the file admits, given whole to the page cache, which
/// holds a value page for one shard of the cache but not for the two a
/// two-worker runtime gives it, is refused as the daemon opens the cache,
/// and the failure reaches exit 1.
#[tokio::test(flavor = "multi_thread")]
async fn test_serve_exits_1_when_the_page_cache_is_below_its_shards_minimum() -> TestResult {
    let workers = 2;
    let dir = tempfile::tempdir()?;
    let fixture = local_fixture(dir.path(), FixtureSpec::small("kv_cache", KeyType::Long)).await?;
    let mut config: Json = serde_json::from_str(&json_config(TABLE_A, &fixture))?;
    let one_shard_not_two = VALUE_PAGE_BYTES * workers;
    config["server"]["cache_bytes"] = Json::from(one_shard_not_two);
    config["server"]["row_cache_share"] = Json::from(0.0);
    let path = write_file(dir.path(), "serve.json", &config.to_string())?;
    // Spawned and waited on within a bound rather than run to its end, so
    // a daemon that wrongly starts serving fails the test instead of
    // holding it open.
    let mut daemon = Daemon(
        Command::new(BIN)
            .args(["serve", "--config", &path])
            .env_remove(ENV_CONFIG)
            .env(TOKIO_WORKER_THREADS, workers.to_string())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()?,
    );
    let status = wait_within(&mut daemon.0, READY_WAIT).await?;
    assert_eq!(status.code(), Some(i32::from(EXIT_FAILURE)));
    Ok(())
}

/// A configuration with no table, `[[tables]]` absent or an empty list, is
/// the file's fault: every one-shot refuses it with exit 1 before anything
/// is opened, not with the usage code.
#[tokio::test]
async fn test_a_configuration_with_no_table_exits_1() -> TestResult {
    let dir = tempfile::tempdir()?;
    let configs = [
        ("absent.toml", "[server]\npoll_interval = \"30s\"\n"),
        ("empty.json", "{\"tables\": []}"),
    ];
    for (name, text) in configs {
        let config = write_file(dir.path(), name, text)?;
        for args in [
            &["doctor", "--config", &config][..],
            &["info", "--config", &config],
            &["get", "--config", &config, "1"],
        ] {
            let output = run(args, &[]);
            assert_exit(&output, EXIT_FAILURE);
            assert!(output.stdout.is_empty(), "{args:?}: {}", stderr(&output));
            assert_plain_error(&output);
        }
    }
    Ok(())
}

/// `serve` refuses to start on a configuration with no table, exit 1,
/// rather than report ready while serving nothing. The refusal comes before
/// logging starts, so it is plain text on stderr and the log stream,
/// stdout, carries nothing.
#[tokio::test(flavor = "multi_thread")]
async fn test_serve_exits_1_on_a_configuration_with_no_table() -> TestResult {
    let dir = tempfile::tempdir()?;
    let config = serde_json::json!({
        "server": {"listen": "127.0.0.1:0", "metrics_listen": "127.0.0.1:0"},
        "tables": []
    });
    let path = write_file(dir.path(), "serve.json", &config.to_string())?;
    // Spawned and waited on within a bound rather than run to its end, so
    // a daemon that wrongly starts serving fails the test instead of
    // holding it open.
    let mut daemon = Daemon(
        Command::new(BIN)
            .args(["serve", "--config", &path])
            .env_remove(ENV_CONFIG)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()?,
    );
    let status = wait_within(&mut daemon.0, READY_WAIT).await?;
    assert_eq!(status.code(), Some(i32::from(EXIT_FAILURE)));
    let mut output = Output {
        status,
        stdout: Vec::new(),
        stderr: Vec::new(),
    };
    if let Some(mut stdout) = daemon.0.stdout.take() {
        std::io::Read::read_to_end(&mut stdout, &mut output.stdout)?;
    }
    if let Some(mut stderr) = daemon.0.stderr.take() {
        std::io::Read::read_to_end(&mut stderr, &mut output.stderr)?;
    }
    assert!(output.stdout.is_empty(), "{}", stderr(&output));
    assert_plain_error(&output);
    Ok(())
}

/// The `state` label value of a table that serves, as core renders the
/// state; the state's fields do not reach the label, so any snapshot and
/// instant give it.
fn serving_state() -> String {
    TableState::Serving {
        snapshot_id: SnapshotId(0),
        since: Instant::now(),
    }
    .to_string()
}

/// The conformance run of RFD 012, which the workspace test run carries:
/// the binary started as an orchestrator starts it (the configuration path
/// from the environment, a JSON file, no flags), its logs under the
/// configured field names, the health service answering while the table
/// loads, readiness turning on once it serves, the metrics scrape carrying
/// the table's state and the process's memory, and the drain on SIGTERM.
#[tokio::test(flavor = "multi_thread")]
async fn test_serve_conformance_from_the_environment() -> TestResult {
    let dir = tempfile::tempdir()?;
    let fixture = local_fixture(dir.path(), FixtureSpec::small("kv_serve", KeyType::Long)).await?;
    let config = write_file(dir.path(), "serve.json", &json_config(TABLE_A, &fixture))?;
    let mut daemon = Daemon(
        Command::new(BIN)
            .arg("serve")
            .env(ENV_CONFIG, &config)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()?,
    );
    let stdout = daemon.0.stdout.take().expect("stdout is piped");
    let mut reader = BufReader::new(stdout);
    let (grpc, metrics) = until_listening(&mut reader)?;

    // The health service answers as soon as the listener is up, while the
    // table may still be loading: the answer is one of the two states,
    // never a transport failure.
    let channel = Channel::from_shared(format!("http://{grpc}"))?
        .connect()
        .await?;
    let mut health = HealthClient::new(channel);
    assert!(matches!(
        health_status(&mut health, OVERALL_SERVICE).await?,
        ServingStatus::NotServing | ServingStatus::Serving
    ));

    poll_until(READY_WAIT, "readiness", async || {
        let (status, body) = http_get(metrics, READY_PATH).await?;
        if status == StatusCode::OK {
            assert_eq!(body, READY_BODY);
            return Ok(Some(()));
        }
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(body, NOT_READY_BODY);
        Ok(None)
    })
    .await?;
    assert_eq!(
        health_status(&mut health, OVERALL_SERVICE).await?,
        ServingStatus::Serving
    );

    let (status, exposition) = http_get(metrics, METRICS_PATH).await?;
    assert_eq!(status, StatusCode::OK);
    let rss = series_value(&exposition, PROCESS_RSS_BYTES, &[])
        .expect("the scrape carries the process's memory");
    assert!(rss > 0.0, "a running process holds memory: {rss}");
    let allocated = series_value(&exposition, ALLOCATOR_ALLOCATED_BYTES, &[])
        .expect("the scrape carries jemalloc's live bytes");
    let resident = series_value(&exposition, ALLOCATOR_RESIDENT_BYTES, &[])
        .expect("the scrape carries jemalloc's resident bytes");
    assert!(
        allocated > 0.0 && allocated <= resident,
        "the server allocates through jemalloc: {allocated} allocated, {resident} resident"
    );
    assert_eq!(
        series_value(
            &exposition,
            TABLE_STATE,
            &[(LABEL_TABLE, TABLE_A), (LABEL_STATE, &serving_state())]
        )
        .map(f64::to_bits),
        Some(1.0_f64.to_bits()),
        "the scrape carries the table's state"
    );

    send(&daemon, "-TERM")?;
    let status = wait_within(&mut daemon.0, SHUTDOWN_WAIT).await?;
    assert_eq!(status.code(), Some(i32::from(EXIT_OK)));
    let mut drained = String::new();
    std::io::Read::read_to_string(&mut reader, &mut drained)?;
    for line in &lines(drained.as_bytes()) {
        check_log_line(line);
    }
    Ok(())
}

/// A poll interval no test outlasts, so only a SIGHUP makes the daemon read
/// its file again.
const POLL_NEVER: &str = "24h";

/// `json_config` over `tables`, each pinned to its fixture, polling every
/// `POLL_NEVER`.
fn json_config_polling_never(tables: &[(&str, &Fixture)]) -> String {
    let mut config: Json =
        serde_json::from_str(&json_config(tables[0].0, tables[0].1)).expect("json_config is JSON");
    config["server"]["poll_interval"] = Json::from(POLL_NEVER);
    config["tables"] = tables
        .iter()
        .map(|(name, fixture)| json_table(name, fixture))
        .collect();
    config.to_string()
}

/// A daemon serving the configuration at `config`, spawned and waited on
/// until ready: the daemon, the reader of its log, kept open so the
/// daemon's log writes never meet a closed pipe, and its metrics address.
async fn serve_ready(
    config: &str,
) -> Result<(Daemon, BufReader<ChildStdout>, SocketAddr), BoxError> {
    let mut daemon = Daemon(
        Command::new(BIN)
            .args(["serve", "--config", config])
            .env_remove(ENV_CONFIG)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()?,
    );
    let stdout = daemon.0.stdout.take().expect("stdout is piped");
    let mut reader = BufReader::new(stdout);
    let (_, metrics) = until_listening(&mut reader)?;
    poll_until(READY_WAIT, "readiness", async || {
        let (status, _) = http_get(metrics, READY_PATH).await?;
        Ok((status == StatusCode::OK).then_some(()))
    })
    .await?;
    Ok((daemon, reader, metrics))
}

/// Sends `signal` (`-HUP`, `-TERM`) to `daemon` through `kill`.
fn send(daemon: &Daemon, signal: &str) -> TestResult {
    let sent = Command::new("kill")
        .arg(signal)
        .arg(daemon.0.id().to_string())
        .status()?;
    assert!(sent.success(), "kill {signal}");
    Ok(())
}

/// Waits for the scrape at `metrics` to count a reload of `result`, whose
/// series reads 0 from the start, and returns that scrape.
async fn reload_counted(metrics: SocketAddr, result: ReloadResult) -> Result<String, BoxError> {
    let label = result.to_string();
    poll_until(READY_WAIT, "a reload counted", async || {
        let (_, exposition) = http_get(metrics, METRICS_PATH).await?;
        let reloads = series_value(&exposition, CONFIG_RELOADS_TOTAL, &[(LABEL_RESULT, &label)]);
        Ok(reloads
            .is_some_and(|count| count > 0.0)
            .then_some(exposition))
    })
    .await
}

/// Waits for `config_pending` to read `pending`. A scrape publishes the
/// flag before it renders the counters, so the scrape that first counts a
/// reload can still read the flag from just before it.
async fn pending_reads(metrics: SocketAddr, pending: bool) -> TestResult {
    let want = f64::from(u8::from(pending)).to_bits();
    poll_until(
        READY_WAIT,
        "config_pending to follow the reload",
        async || {
            let (_, exposition) = http_get(metrics, METRICS_PATH).await?;
            Ok(
                (series_value(&exposition, CONFIG_PENDING, &[]).map(f64::to_bits) == Some(want))
                    .then_some(()),
            )
        },
    )
    .await
}

/// Ends `daemon` with SIGTERM and asserts its drain exits 0.
async fn drain(mut daemon: Daemon) -> TestResult {
    send(&daemon, "-TERM")?;
    let status = wait_within(&mut daemon.0, SHUTDOWN_WAIT).await?;
    assert_eq!(status.code(), Some(i32::from(EXIT_OK)));
    Ok(())
}

/// SIGHUP makes the daemon read its file at once, with no poll due, and
/// apply the table it adds; the process lives through the signal, whose
/// default action would end it, and drains on SIGTERM as before.
#[tokio::test(flavor = "multi_thread")]
async fn test_serve_reloads_its_file_on_sighup() -> TestResult {
    let dir = tempfile::tempdir()?;
    let first = local_fixture(dir.path(), FixtureSpec::small("kv_first", KeyType::Long)).await?;
    let second = local_fixture(dir.path(), FixtureSpec::small("kv_second", KeyType::Long)).await?;
    let config = write_file(
        dir.path(),
        "serve.json",
        &json_config_polling_never(&[(TABLE_A, &first)]),
    )?;
    let (mut daemon, _log, metrics) = serve_ready(&config).await?;

    std::fs::write(
        &config,
        json_config_polling_never(&[(TABLE_A, &first), (TABLE_B, &second)]),
    )?;
    send(&daemon, "-HUP")?;
    let exposition = reload_counted(metrics, ReloadResult::Applied).await?;
    assert_eq!(
        series_value(
            &exposition,
            CONFIG_RELOADS_TOTAL,
            &[(LABEL_RESULT, &ReloadResult::Applied.to_string())]
        )
        .map(f64::to_bits),
        Some(1.0_f64.to_bits())
    );
    assert_eq!(
        series_value(
            &exposition,
            TABLE_STATE,
            &[(LABEL_TABLE, TABLE_B), (LABEL_STATE, &serving_state())]
        )
        .map(f64::to_bits),
        Some(1.0_f64.to_bits()),
        "the added table serves"
    );
    pending_reads(metrics, false).await?;
    assert!(
        daemon.0.try_wait()?.is_none(),
        "the daemon lives through SIGHUP"
    );
    drain(daemon).await
}

/// A reload to a file with no table is refused like any file that does
/// not validate: the table serving serves on, readiness stays, and
/// `config_pending` reads 1 while the file differs from what is applied.
#[tokio::test(flavor = "multi_thread")]
async fn test_serve_refuses_a_reload_to_a_file_with_no_table() -> TestResult {
    let dir = tempfile::tempdir()?;
    let fixture = local_fixture(dir.path(), FixtureSpec::small("kv_kept", KeyType::Long)).await?;
    let text = json_config_polling_never(&[(TABLE_A, &fixture)]);
    let config = write_file(dir.path(), "serve.json", &text)?;
    let (daemon, _log, metrics) = serve_ready(&config).await?;

    let mut empty: Json = serde_json::from_str(&text)?;
    empty["tables"] = Json::Array(Vec::new());
    std::fs::write(&config, empty.to_string())?;
    send(&daemon, "-HUP")?;
    let exposition = reload_counted(metrics, ReloadResult::Refused).await?;
    pending_reads(metrics, true).await?;
    assert_eq!(
        series_value(
            &exposition,
            TABLE_STATE,
            &[(LABEL_TABLE, TABLE_A), (LABEL_STATE, &serving_state())]
        )
        .map(f64::to_bits),
        Some(1.0_f64.to_bits()),
        "the table serving serves on"
    );
    let (status, _) = http_get(metrics, READY_PATH).await?;
    assert_eq!(status, StatusCode::OK);
    drain(daemon).await
}
