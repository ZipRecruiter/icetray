//! The merge-on-read measurements, a program and not a test: the three
//! measurements RFD 021 (Merge-on-read) makes before release, over
//! contract-layout tables of long keys that the fixture writer builds under
//! `--work-dir`, served in process through `Table` over `Storage`'s
//! `file://` store, so no gRPC and no object-store latency is in them.
//!
//! - `probe`: hit and miss latency at each of `--depths` files per bucket in
//!   each residency tier, over three tables grown one overlapping file per
//!   commit (`Variant`). Every key is asked once unmeasured first, so the
//!   page cache and the kernel's hold what the measured lookups read. In
//!   the Full tier the two steps of each probe are then timed apart over
//!   the same lookups (`micro`): the key-page decode, through
//!   `FileIndex::page_of`, and the deleted-row check, over each file's
//!   deleted rows built as a generation builds them (`served_rows`),
//!   against RFD 021's threshold of a tenth of a decode.
//! - `memory`: the delete state's bytes against the bound the budget counts
//!   for it, at each of `--deleted-per-mille`, in each delete form and
//!   pattern, with the deleted-row check's time over each state.
//! - `load`: a first load with each of `--delete-files` live delete files,
//!   and a refresh onto the snapshot with one more, timed, with their GETs.
//!
//! Results print as tables and are written as JSON under `--out-dir`. Run
//! with `cargo run -p icetray-testkit --example deletes --release -- --help`.

use std::collections::HashMap;
use std::error::Error;
use std::hint::black_box;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use clap::{Parser, ValueEnum};
use iceberg::spec::DataFile;
use icetray_core::ColumnRead;
use icetray_core::config::{
    FALLBACK_CACHE_BYTES, Residency, SourceConfig, TableConfig, ValueFormat,
};
use icetray_core::{
    Generation, Key, KeyType, PER_MILLE, Row, TableSource, TableSummary, approx, diagnose, time,
};
use icetray_testkit::fixture::{NAMESPACE, memory_catalog_local};
use icetray_testkit::keys::FIRST_VERSION;
use icetray_testkit::latency::{Latencies, P50, P90, P99, micros};
use icetray_testkit::layout::VALUE_COLUMN;
use icetray_testkit::verify::{self, Served};
use icetray_testkit::{
    DeleteForm, Fixture, FixtureSpec, HandSnapshot, absent_key_from, write_fixture,
};
use rand::rngs::StdRng;
use rand::{RngExt as _, SeedableRng as _};
use roaring::RoaringTreemap;
use serde_json::{Value as Json, json};
use tempfile::TempDir;

/// Whatever failed; the program reports it and exits.
type HarnessResult<T> = Result<T, Box<dyn Error + Send + Sync>>;

/// The seed of the default run, any fixed value, so two runs ask the same
/// keys and delete the same rows.
const DEFAULT_SEED: u64 = 7;

/// Files per bucket the probe measures at by default: RFD 021's 1, 2, 4,
/// 8, and 16.
const DEFAULT_DEPTHS: [u64; 5] = [1, 2, 4, 8, 16];

/// Rows per data file of the probe tables by default: fifty contract key
/// pages, enough that a key's page is found through the page index as in
/// a serving file, few enough that sixteen files of each of three tables
/// write in seconds.
const DEFAULT_PROBE_ROWS: u64 = 50_000;

/// Hits, and as many misses, per measured run by default: enough that the
/// 99th percentile rests on a hundred lookups.
const DEFAULT_LOOKUPS: usize = 10_000;

/// One row in this many of the probe tables' first file has its key
/// deleted from every file that holds it: the keys the `updated` and
/// `shadowed` misses ask. One in eight leaves more than 4,096 deleted rows
/// in each 65,536-row block, so the first file's deleted rows are a bitset
/// container, and the probe rows must be at least this many.
const DELETED_KEY_EVERY: u64 = 8;

/// The fewest deleted-row checks one timing of them runs, repeating the
/// lookups' checks as often as it takes: about four million, enough that
/// the clock's resolution is lost in a check of a few nanoseconds.
const MIN_TIMED_CHECKS: usize = 1 << 22;

/// Random positions per data file the memory measurement times the
/// deleted-row check at: enough to touch every container of a
/// million-row file several times.
const CHECKED_POSITIONS: usize = 4_096;

/// RFD 021: past a tenth of a key-page decode per check, the uncapped
/// deleted-row check is revisited.
const REVISIT_RATIO: f64 = 0.1;

/// Deleted fractions the memory measurement runs at by default, in
/// thousandths: RFD 021's 0.1% to 50%.
const DEFAULT_DELETED_PER_MILLE: [u64; 6] = [1, 10, 50, 100, 250, 500];

/// Data files of the memory table by default, one bucket each.
const DEFAULT_MEMORY_FILES: u32 = 4;

/// Rows per data file of the memory table by default: about fifteen
/// 65,536-row blocks, so the per-block costs of the bound show.
const DEFAULT_MEMORY_ROWS: u64 = 1_000_000;

/// Bytes per value of the memory and load tables: the delete state and a
/// load's delete reads do not depend on values, and small ones keep the
/// tables quick to write.
const SMALL_VALUE_BYTES: usize = 8;

/// Live delete files the load measurement runs at by default: RFD 021's
/// up to 1,000, and none, the load of the data files alone beside them.
const DEFAULT_DELETE_FILES: [u64; 5] = [0, 1, 10, 100, 1_000];

/// Rows per data file of the load table by default: one contract key page,
/// so the load's data-file work stays small beside its deletes.
const DEFAULT_LOAD_ROWS: u64 = 1_000;

/// Loads, and refreshes, per delete-file count by default.
const DEFAULT_LOAD_ROUNDS: u32 = 3;

/// Rows each delete file of the load table deletes, from its data file's
/// first.
const ROWS_PER_DELETE_FILE: u64 = 10;

/// What to measure.
#[derive(Clone, Copy, Debug, PartialEq, Eq, ValueEnum)]
enum Measure {
    /// Lookup latency by files per bucket, with deleted rows and without.
    Probe,
    /// Delete-state bytes against their bound by deleted fraction.
    Memory,
    /// Load and refresh time by live delete files.
    Load,
}

impl Measure {
    const ALL: [Self; 3] = [Self::Probe, Self::Memory, Self::Load];
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

    fn residency(self) -> Residency {
        match self {
            Self::Full => Residency::Full,
            Self::Disk => Residency::Disk,
            Self::Bloom => Residency::Bloom,
        }
    }
}

/// How a probe table's newer files overlap its first. Every file holds one
/// row per id of one residue modulo the stride (`ProbeTable`), so all span
/// the same key range and every lookup's candidates are every file.
#[derive(Clone, Copy, Debug, PartialEq, Eq, ValueEnum)]
enum Variant {
    /// Each newer file holds the ids of another residue, nothing is
    /// deleted, and a lookup decodes a key page of every newer file before
    /// it reaches its key: the probes without the deleted-row checks.
    Control,
    /// Each commit rewrites the first file's ids in a new file and deletes
    /// every older copy, as a merge-on-read `UPDATE` does: a hit finds the
    /// live row in the newest file, on its first probe.
    Updated,
    /// Each newer file holds copies of the first file's ids, every row
    /// deleted in the commit that adds it: a hit passes a deleted row in
    /// every newer file before the live one in the first, RFD 021's
    /// deleted rows first in the probe order.
    Shadowed,
}

impl Variant {
    const ALL: [Self; 3] = [Self::Control, Self::Updated, Self::Shadowed];

    fn name(self) -> &'static str {
        match self {
            Self::Control => "control",
            Self::Updated => "updated",
            Self::Shadowed => "shadowed",
        }
    }
}

/// How the memory and load tables' deletes are written (`DeleteForm`).
#[derive(Clone, Copy, Debug, PartialEq, Eq, ValueEnum)]
enum Form {
    /// A deletion vector per data file.
    Vectors,
    /// A Parquet position delete file per data file, naming it.
    ScopedPositions,
    /// A Parquet position delete file per partition, naming no data file.
    Positions,
}

impl Form {
    const ALL: [Self; 3] = [Self::Vectors, Self::ScopedPositions, Self::Positions];

    fn name(self) -> &'static str {
        match self {
            Self::Vectors => "vectors",
            Self::ScopedPositions => "scoped-positions",
            Self::Positions => "positions",
        }
    }

    fn delete_form(self) -> DeleteForm {
        match self {
            Self::Vectors => DeleteForm::Vectors,
            Self::ScopedPositions => DeleteForm::ScopedPositions,
            Self::Positions => DeleteForm::Positions,
        }
    }
}

/// Where in a data file the memory measurement's deleted rows fall.
#[derive(Clone, Copy, Debug, PartialEq, Eq, ValueEnum)]
enum Pattern {
    /// One run of consecutive rows at a random start, as a delete of a key
    /// range leaves in a key-sorted file.
    Dense,
    /// Rows chosen uniformly at random, as deletes of unrelated keys leave.
    Scattered,
}

impl Pattern {
    const ALL: [Self; 2] = [Self::Dense, Self::Scattered];

    fn name(self) -> &'static str {
        match self {
            Self::Dense => "dense",
            Self::Scattered => "scattered",
        }
    }
}

/// What a probe run looks up.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Kind {
    Hit,
    Miss,
}

impl Kind {
    fn name(self) -> &'static str {
        match self {
            Self::Hit => "hit",
            Self::Miss => "miss",
        }
    }
}

/// The program's command line. Every list flag takes comma-separated values
/// and, where it has no default, means all of them when absent.
#[derive(Parser, Debug)]
#[command(about = "Measures merge-on-read lookups, delete state, and delete loads (RFD 021)")]
struct Cli {
    /// Measurements to run.
    #[arg(long, value_enum, value_delimiter = ',')]
    measure: Vec<Measure>,
    /// The seed of the keys asked and the rows deleted.
    #[arg(long, default_value_t = DEFAULT_SEED)]
    seed: u64,
    /// Where the JSON results go.
    #[arg(long, default_value = "target/merge-on-read")]
    out_dir: PathBuf,
    /// Where the tables are written, in a directory of their own removed at
    /// exit.
    #[arg(long, default_value = "target/merge-on-read/work")]
    work_dir: PathBuf,
    /// Probe: files per bucket to measure at.
    #[arg(
        long,
        value_delimiter = ',',
        default_values_t = DEFAULT_DEPTHS,
        value_parser = clap::value_parser!(u64).range(1..)
    )]
    depths: Vec<u64>,
    /// Probe: residency tiers to measure in.
    #[arg(long, value_enum, value_delimiter = ',')]
    tier: Vec<Tier>,
    /// Probe: tables to measure.
    #[arg(long, value_enum, value_delimiter = ',')]
    variant: Vec<Variant>,
    /// Probe: rows per data file.
    #[arg(
        long,
        default_value_t = DEFAULT_PROBE_ROWS,
        value_parser = clap::value_parser!(u64).range(DELETED_KEY_EVERY..)
    )]
    probe_rows: u64,
    /// Probe: hits, and as many misses, per measured run.
    #[arg(long, default_value_t = DEFAULT_LOOKUPS)]
    lookups: usize,
    /// Memory: deleted fractions of each data file, in thousandths.
    #[arg(
        long,
        value_delimiter = ',',
        default_values_t = DEFAULT_DELETED_PER_MILLE,
        value_parser = clap::value_parser!(u64).range(1..=PER_MILLE)
    )]
    deleted_per_mille: Vec<u64>,
    /// Memory and load: delete forms to write.
    #[arg(long, value_enum, value_delimiter = ',')]
    form: Vec<Form>,
    /// Memory: where the deleted rows fall.
    #[arg(long, value_enum, value_delimiter = ',')]
    pattern: Vec<Pattern>,
    /// Memory: data files, one bucket each.
    #[arg(
        long,
        default_value_t = DEFAULT_MEMORY_FILES,
        value_parser = clap::value_parser!(u32).range(1..)
    )]
    memory_files: u32,
    /// Memory: rows per data file, about; buckets hash unevenly.
    #[arg(
        long,
        default_value_t = DEFAULT_MEMORY_ROWS,
        value_parser = clap::value_parser!(u64).range(1..)
    )]
    memory_rows: u64,
    /// Load: live delete files to load with, each on its own data file;
    /// the table has one data file more than the most.
    #[arg(long, value_delimiter = ',', default_values_t = DEFAULT_DELETE_FILES)]
    delete_files: Vec<u64>,
    /// Load: rows per data file, about; buckets hash unevenly.
    #[arg(
        long,
        default_value_t = DEFAULT_LOAD_ROWS,
        value_parser = clap::value_parser!(u64).range(ROWS_PER_DELETE_FILE..)
    )]
    load_rows: u64,
    /// Load: loads, and refreshes, per delete-file count.
    #[arg(
        long,
        default_value_t = DEFAULT_LOAD_ROUNDS,
        value_parser = clap::value_parser!(u32).range(1..)
    )]
    load_rounds: u32,
}

/// `chosen`, or every one of `all` when nothing is chosen.
fn or_all<T: Copy>(chosen: &[T], all: &[T]) -> Vec<T> {
    if chosen.is_empty() {
        all.to_vec()
    } else {
        chosen.to_vec()
    }
}

/// `values` ascending, each once.
fn ascending(values: &[u64]) -> Vec<u64> {
    let mut values = values.to_vec();
    values.sort_unstable();
    values.dedup();
    values
}

#[tokio::main]
async fn main() -> HarnessResult<()> {
    let cli = Cli::parse();
    if cfg!(debug_assertions) {
        eprintln!("warning: an unoptimized build; pass --release");
    }
    std::fs::create_dir_all(&cli.work_dir)?;
    let work = TempDir::new_in(&cli.work_dir)?;
    let mut report = serde_json::Map::new();
    report.insert("config".to_string(), config_json(&cli));
    for measure in or_all(&cli.measure, &Measure::ALL) {
        match measure {
            Measure::Probe => {
                let (runs, steps) = probe(&cli, work.path()).await?;
                report.insert(
                    "probe".to_string(),
                    runs.iter().map(ProbeRun::to_json).collect(),
                );
                report.insert(
                    "probe_steps".to_string(),
                    steps.iter().map(Steps::to_json).collect(),
                );
                report.insert("check_verdict".to_string(), verdict(&steps));
            }
            Measure::Memory => {
                let runs = memory(&cli, work.path()).await?;
                report.insert(
                    "memory".to_string(),
                    runs.iter().map(MemoryRun::to_json).collect(),
                );
            }
            Measure::Load => {
                let runs = load(&cli, work.path()).await?;
                report.insert(
                    "load".to_string(),
                    runs.iter().map(LoadRun::to_json).collect(),
                );
            }
        }
    }
    let path = write_json(&cli.out_dir, &Json::Object(report))?;
    println!("results: {}", path.display());
    Ok(())
}

/// The run's settings, as the JSON records them.
fn config_json(cli: &Cli) -> Json {
    json!({
        "optimized": !cfg!(debug_assertions),
        "seed": cli.seed,
        "depths": ascending(&cli.depths),
        "probe_rows": cli.probe_rows,
        "lookups": cli.lookups,
        "deleted_key_every": DELETED_KEY_EVERY,
        "deleted_per_mille": ascending(&cli.deleted_per_mille),
        "memory_files": cli.memory_files,
        "memory_rows": cli.memory_rows,
        "delete_files": ascending(&cli.delete_files),
        "load_rows": cli.load_rows,
        "load_rounds": cli.load_rounds,
        "rows_per_delete_file": ROWS_PER_DELETE_FILE,
        "key_type": KeyType::Long.to_string(),
    })
}

/// `report` under `dir`, named by the time; the path.
fn write_json(dir: &Path, report: &Json) -> HarnessResult<PathBuf> {
    std::fs::create_dir_all(dir)?;
    let stamp = time::log_timestamp(time::now()).replace(':', "-");
    let path = dir.join(format!("merge-on-read-{stamp}.json"));
    std::fs::write(&path, serde_json::to_vec_pretty(report)?)?;
    Ok(path)
}

/// `fixture` in `residency` from `source` over its own hedge-free storage
/// (`verify::storage_config_for_tests`), so every GET counted is one the
/// lookup or load planned, with the server's default page cache and, in
/// the Disk tier, chunk files under `local`; built, not loaded.
async fn build(
    fixture: &Fixture,
    residency: Residency,
    source: &SourceConfig,
    local: &Path,
) -> HarnessResult<Served> {
    let config = TableConfig {
        source: source.clone(),
        ..fixture.config(residency, false)
    };
    Ok(verify::build_in(
        config,
        TableSource::from_config(source).await?,
        local,
        FALLBACK_CACHE_BYTES,
    )
    .await?)
}

/// The doctor's summary of `served`'s generation (`diagnose`): the delete
/// state's bytes and the predicted memory that counts their bound.
fn summary_of(served: &Served, fixture: &Fixture) -> TableSummary {
    let lookups = fixture.config(Residency::Full, false).lookups();
    diagnose(&served.current(), lookups, &ValueFormat::opaque, None).summary
}

/// `rows` as a generation holds a data file's deleted rows (icetray-core's
/// `deletes::compact`): optimized, then read back through roaring's
/// serialization, so each container is of the kind, and in the allocation,
/// it serves in.
fn served_rows(rows: &RoaringTreemap) -> HarnessResult<RoaringTreemap> {
    let mut optimized = rows.clone();
    optimized.optimize();
    let mut bytes = Vec::with_capacity(optimized.serialized_size());
    optimized.serialize_into(&mut bytes)?;
    Ok(RoaringTreemap::deserialize_unchecked_from(
        bytes.as_slice(),
    )?)
}

/// Microseconds per deleted-row check over `checks`, each a file's deleted
/// rows and a position to check, run in order as often as it takes to
/// reach `MIN_TIMED_CHECKS`; `None` for no checks.
fn check_micros(checks: &[(&RoaringTreemap, u64)]) -> Option<f64> {
    if checks.is_empty() {
        return None;
    }
    let rounds = MIN_TIMED_CHECKS.div_ceil(checks.len());
    let mut deleted = 0_usize;
    let started = Instant::now();
    for _ in 0..rounds {
        for &(rows, position) in checks {
            deleted += usize::from(black_box(rows).contains(black_box(position)));
        }
    }
    let elapsed = started.elapsed();
    black_box(deleted);
    Some(per_operation(elapsed, rounds * checks.len()))
}

/// Microseconds per operation of `operations` taking `elapsed` in all.
fn per_operation(elapsed: Duration, operations: usize) -> f64 {
    micros(elapsed) / approx::gauge_len(operations)
}

// ---------------------------------------------------------------------------
// Probe depth.

/// One probe table as it grows: its fixture, the stride its ids are
/// written at, its rows per file, and per data file in commit order what
/// the program knows of it. Row `q` of every file holds id
/// `q * stride + class` of its file's class, so every file spans the same
/// range; the stride is the deepest depth measured, so `control` has a
/// class for each of its files. Every row is written at the first version,
/// whose values zstd cannot shrink, so a hit's value page costs the same
/// whichever file holds the live row: a later version's values compress
/// (`value_of_version`), and decompressing them would outweigh the probes
/// measured here.
struct ProbeTable {
    variant: Variant,
    fixture: Fixture,
    stride: u64,
    rows: u64,
    files: Vec<Layer>,
}

/// One data file of a probe table: its entry, its ids' residue modulo the
/// stride, and the positions its deletes remove.
struct Layer {
    entry: DataFile,
    class: u64,
    deleted: RoaringTreemap,
}

/// One key a probe run asks: the class-zero row its key is, or `None` for
/// an absent key, and the answer it must get.
struct Ask {
    key: Key,
    row: Option<u64>,
    expected: Row,
}

impl ProbeTable {
    /// The variant's first file, `rows` rows of class zero, written under
    /// `dir`; in `updated` and `shadowed` with every `DELETED_KEY_EVERY`-th
    /// row deleted.
    async fn create(variant: Variant, rows: u64, stride: u64, dir: &Path) -> HarnessResult<Self> {
        let catalog = memory_catalog_local(dir).await?;
        let spec = FixtureSpec {
            buckets: 0,
            ids: 0..stride * rows,
            id_stride: stride,
            ..FixtureSpec::small(variant.name(), KeyType::Long)
        };
        let fixture = write_fixture(catalog.as_ref(), NAMESPACE, spec).await?;
        let entry = one_file(fixture.files(), rows)?;
        let mut table = Self {
            variant,
            fixture,
            stride,
            rows,
            files: vec![Layer {
                entry,
                class: 0,
                deleted: RoaringTreemap::new(),
            }],
        };
        match variant {
            Variant::Control => {}
            Variant::Updated | Variant::Shadowed => {
                table.files[0].deleted = table.deleted_keys();
                table.commit(&[0], "first").await?;
            }
        }
        Ok(table)
    }

    /// The rows whose keys are deleted from every file that holds them.
    fn deleted_keys(&self) -> RoaringTreemap {
        (0..self.rows)
            .filter(|row| row.is_multiple_of(DELETED_KEY_EVERY))
            .collect()
    }

    /// Every row of a file.
    fn every_row(&self) -> RoaringTreemap {
        let mut rows = RoaringTreemap::new();
        rows.insert_range(0..self.rows);
        rows
    }

    /// Adds one file overlapping every other, as the variant describes,
    /// in one commit with its deletes.
    async fn grow(&mut self) -> HarnessResult<()> {
        let at = self.files.len();
        let name = format!("layer{at}");
        let class = match self.variant {
            Variant::Control => approx::count(at),
            Variant::Updated | Variant::Shadowed => 0,
        };
        let ids: Vec<u64> = (0..self.rows)
            .map(|row| row * self.stride + class)
            .collect();
        let written = self.fixture.write_rows(&ids, FIRST_VERSION, &name).await?;
        let entry = one_file(&written, self.rows)?;
        let (deleted, changed) = match self.variant {
            Variant::Control => (RoaringTreemap::new(), Vec::new()),
            Variant::Shadowed => (self.every_row(), vec![at]),
            Variant::Updated => {
                self.files[at - 1].deleted = self.every_row();
                (self.deleted_keys(), vec![at - 1, at])
            }
        };
        self.files.push(Layer {
            entry,
            class,
            deleted,
        });
        self.commit(&changed, &name).await
    }

    /// Commits by hand every file, beside one Puffin file with a vector for
    /// each file of `changed` holding all its deleted rows, which replaces
    /// any vector that file had; every other vector stays listed.
    async fn commit(&mut self, changed: &[usize], name: &str) -> HarnessResult<()> {
        let replaced: Vec<&str> = changed
            .iter()
            .map(|&at| self.files[at].entry.file_path())
            .collect();
        let mut deletes: Vec<DataFile> = self
            .fixture
            .deletes()
            .iter()
            .filter(|delete| {
                delete
                    .referenced_data_file()
                    .is_none_or(|path| !replaced.contains(&path.as_str()))
            })
            .cloned()
            .collect();
        if !changed.is_empty() {
            let targets = changed
                .iter()
                .map(|&at| (self.files[at].entry.clone(), self.files[at].deleted.clone()))
                .collect();
            deletes.extend(
                self.fixture
                    .write_deletes(targets, DeleteForm::Vectors, name)
                    .await?,
            );
        }
        let data = self.files.iter().map(|layer| layer.entry.clone()).collect();
        self.fixture
            .commit_by_hand(HandSnapshot { data, deletes })
            .await?;
        Ok(())
    }

    /// The key of class-zero row `row`.
    fn key(&self, row: u64) -> Key {
        self.fixture.key(row * self.stride)
    }

    /// What a lookup of class-zero row `row` answers: its value where a
    /// file holds the row live, else absent.
    fn expected(&self, row: u64) -> Row {
        let live = self
            .files
            .iter()
            .any(|layer| layer.class == 0 && !layer.deleted.contains(row));
        if live {
            self.fixture.expected(&self.key(row))
        } else {
            Row::Absent
        }
    }

    /// The hits on `rows`, at the current depth.
    fn hits(&self, rows: &[u64]) -> Vec<Ask> {
        rows.iter()
            .map(|&row| Ask {
                key: self.key(row),
                row: Some(row),
                expected: self.expected(row),
            })
            .collect()
    }

    /// The misses at `rows`, which are deleted-key rows: in `control`, which
    /// deletes nothing, the absent key just above each row's; elsewhere the
    /// row's key, deleted from every file.
    fn misses(&self, rows: &[u64]) -> Vec<Ask> {
        rows.iter()
            .map(|&row| match self.variant {
                Variant::Control => Ask {
                    key: absent_key_from(&self.key(row)),
                    row: None,
                    expected: Row::Absent,
                },
                Variant::Updated | Variant::Shadowed => Ask {
                    key: self.key(row),
                    row: Some(row),
                    expected: Row::Absent,
                },
            })
            .collect()
    }
}

/// The one data file `written` holds, of `rows` rows, so that its row `q`
/// is the `q`-th id it was handed; anything else is an error.
fn one_file(written: &[DataFile], rows: u64) -> HarnessResult<DataFile> {
    match written {
        [file] if file.record_count() == rows => Ok(file.clone()),
        [_] | [] | [_, _, ..] => Err(format!(
            "expected one data file of {rows} rows, the writer wrote {written:?}"
        )
        .into()),
    }
}

/// The rows the probe asks, the same at every depth and in every variant:
/// `count` hit rows, none a deleted-key row, and `count` deleted-key rows
/// for the misses.
fn asked_rows(rows: u64, count: usize, seed: u64) -> (Vec<u64>, Vec<u64>) {
    let mut rng = StdRng::seed_from_u64(seed);
    let blocks = rows / DELETED_KEY_EVERY;
    let hits = (0..count)
        .map(|_| {
            rng.random_range(0..blocks) * DELETED_KEY_EVERY + rng.random_range(1..DELETED_KEY_EVERY)
        })
        .collect();
    let deleted = rows.div_ceil(DELETED_KEY_EVERY);
    let misses = (0..count)
        .map(|_| rng.random_range(0..deleted) * DELETED_KEY_EVERY)
        .collect();
    (hits, misses)
}

/// One measured probe run.
struct ProbeRun {
    variant: Variant,
    tier: Tier,
    depth: u64,
    kind: Kind,
    latencies: Latencies,
    gets_per_lookup: f64,
}

impl ProbeRun {
    fn header() -> String {
        format!(
            "{:<8} {:<5} {:>5} {:<4} {:>9} {:>9} {:>9} {:>9} {:>6}",
            "variant", "tier", "depth", "kind", "p50_us", "p90_us", "p99_us", "max_us", "gets"
        )
    }

    fn row(&self) -> String {
        format!(
            "{:<8} {:<5} {:>5} {:<4} {:>9.2} {:>9.2} {:>9.2} {:>9.2} {:>6.3}",
            self.variant.name(),
            self.tier.residency().to_string(),
            self.depth,
            self.kind.name(),
            micros(self.latencies.percentile(P50)),
            micros(self.latencies.percentile(P90)),
            micros(self.latencies.percentile(P99)),
            micros(self.latencies.max()),
            self.gets_per_lookup,
        )
    }

    fn to_json(&self) -> Json {
        json!({
            "variant": self.variant.name(),
            "tier": self.tier.residency().to_string(),
            "depth": self.depth,
            "kind": self.kind.name(),
            "lookups": self.latencies.len(),
            "p50_us": micros(self.latencies.percentile(P50)),
            "p90_us": micros(self.latencies.percentile(P90)),
            "p99_us": micros(self.latencies.percentile(P99)),
            "max_us": micros(self.latencies.max()),
            "gets_per_lookup": self.gets_per_lookup,
        })
    }
}

/// The Full tier's probes of one variant and depth, timed by step.
struct Steps {
    variant: Variant,
    depth: u64,
    probes_per_lookup: f64,
    checks_per_lookup: f64,
    decode_us: Option<f64>,
    check_us: Option<f64>,
}

impl Steps {
    /// A check's time over a decode's, which RFD 021 bounds by
    /// `REVISIT_RATIO`.
    fn check_over_decode(&self) -> Option<f64> {
        self.check_us
            .zip(self.decode_us)
            .map(|(check, decode)| check / decode)
    }

    fn header() -> String {
        format!(
            "{:<8} {:>5} {:>7} {:>7} {:>10} {:>10} {:>8}",
            "variant", "depth", "probes", "checks", "decode_us", "check_us", "ratio"
        )
    }

    fn row(&self) -> String {
        let shown =
            |value: Option<f64>| value.map_or_else(|| "-".to_string(), |v| format!("{v:.4}"));
        format!(
            "{:<8} {:>5} {:>7.2} {:>7.2} {:>10} {:>10} {:>8}",
            self.variant.name(),
            self.depth,
            self.probes_per_lookup,
            self.checks_per_lookup,
            shown(self.decode_us),
            shown(self.check_us),
            shown(self.check_over_decode()),
        )
    }

    fn to_json(&self) -> Json {
        json!({
            "variant": self.variant.name(),
            "depth": self.depth,
            "probes_per_lookup": self.probes_per_lookup,
            "checks_per_lookup": self.checks_per_lookup,
            "decode_us": self.decode_us,
            "check_us": self.check_us,
            "check_over_decode": self.check_over_decode(),
        })
    }
}

/// The probe measurement: every chosen variant grown to every chosen depth,
/// measured in every chosen tier at each.
async fn probe(cli: &Cli, work: &Path) -> HarnessResult<(Vec<ProbeRun>, Vec<Steps>)> {
    let depths = ascending(&cli.depths);
    let stride = depths.last().copied().unwrap_or(1);
    let (hit_rows, miss_rows) = asked_rows(cli.probe_rows, cli.lookups, cli.seed);
    let tiers = or_all(&cli.tier, &Tier::ALL);
    println!("{}", ProbeRun::header());
    let mut runs = Vec::new();
    let mut steps = Vec::new();
    for variant in or_all(&cli.variant, &Variant::ALL) {
        let dir = work.join("probe").join(variant.name());
        let mut table = ProbeTable::create(variant, cli.probe_rows, stride, &dir).await?;
        for &depth in &depths {
            while approx::count(table.files.len()) < depth {
                table.grow().await?;
            }
            let hits = table.hits(&hit_rows);
            let misses = table.misses(&miss_rows);
            for &tier in &tiers {
                let local = dir.join(format!("local-{depth}"));
                let source = table.fixture.pinned_source();
                let served = build(&table.fixture, tier.residency(), &source, &local)
                    .await?
                    .load()
                    .await?;
                asked(&served, &hits).await?;
                asked(&served, &misses).await?;
                for (kind, asks) in [(Kind::Hit, &hits), (Kind::Miss, &misses)] {
                    let (latencies, gets_per_lookup) = asked(&served, asks).await?;
                    let run = ProbeRun {
                        variant,
                        tier,
                        depth,
                        kind,
                        latencies,
                        gets_per_lookup,
                    };
                    println!("{}", run.row());
                    runs.push(run);
                }
                if tier == Tier::Full {
                    let asks: Vec<&Ask> = hits.iter().chain(&misses).collect();
                    steps.push(timed_steps(&served.current(), &table, depth, &asks)?);
                }
            }
        }
    }
    println!();
    println!("{}", Steps::header());
    for step in &steps {
        println!("{}", step.row());
    }
    print_verdict(&steps);
    Ok((runs, steps))
}

/// Looks up every one of `asks`, one at a time; each lookup's latency and
/// the GETs per lookup. A lookup answering other than its ask expects fails
/// the run.
async fn asked(served: &Served, asks: &[Ask]) -> HarnessResult<(Latencies, f64)> {
    let stats = served.storage.stats();
    let before = stats.gets();
    let mut took = Vec::with_capacity(asks.len());
    for ask in asks {
        let started = Instant::now();
        let found = served
            .table
            .get(&ask.key, &[ColumnRead::whole(VALUE_COLUMN)])
            .await?;
        took.push(started.elapsed());
        if found.row != ask.expected {
            return Err(format!(
                "{:?} answered present {}, expected present {}",
                ask.key,
                verify::one_value(&found.row).is_some(),
                verify::one_value(&ask.expected).is_some(),
            )
            .into());
        }
    }
    let gets = stats.gets() - before;
    Ok((
        Latencies::of(took),
        approx::ratio(gets, approx::count(asks.len())),
    ))
}

/// The two steps of the Full tier's probes for `asks`, timed apart, as
/// `Generation::lookup` takes them: each candidate in probe order decodes
/// its key page (`FileIndex::page_of`, which in the Full tier decodes the
/// resident page and searches it), and where the candidate holds the key
/// and has deletes, checks the row against them, stopping at a live row.
/// The checks run over each file's deleted rows built as the generation
/// builds them (`served_rows`), at the rows the lookups check.
fn timed_steps(
    generation: &Generation,
    table: &ProbeTable,
    depth: u64,
    asks: &[&Ask],
) -> HarnessResult<Steps> {
    let at_path: HashMap<&str, usize> = table
        .files
        .iter()
        .enumerate()
        .map(|(at, layer)| (layer.entry.file_path(), at))
        .collect();
    let served = table
        .files
        .iter()
        .map(|layer| served_rows(&layer.deleted))
        .collect::<HarnessResult<Vec<_>>>()?;
    let mut decodes = Vec::new();
    let mut checks = Vec::new();
    for ask in asks {
        for file in generation.candidates(&ask.key) {
            decodes.push((file, &ask.key));
            let at = *at_path.get(file.entry.path.as_str()).ok_or_else(|| {
                format!("the generation serves {}, never written", file.entry.path)
            })?;
            let layer = &table.files[at];
            let Some(row) = ask.row.filter(|_| layer.class == 0) else {
                continue;
            };
            if !layer.deleted.is_empty() {
                checks.push((&served[at], row));
            }
            if !layer.deleted.contains(row) {
                break;
            }
        }
    }
    let started = Instant::now();
    for (file, key) in &decodes {
        black_box(file.index.page_of(black_box(key)));
    }
    let decode_us = (!decodes.is_empty()).then(|| per_operation(started.elapsed(), decodes.len()));
    let lookups = approx::count(asks.len());
    Ok(Steps {
        variant: table.variant,
        depth,
        probes_per_lookup: approx::ratio(approx::count(decodes.len()), lookups),
        checks_per_lookup: approx::ratio(approx::count(checks.len()), lookups),
        decode_us,
        check_us: check_micros(&checks),
    })
}

/// The steps whose check costs most against its decode, and its ratio.
fn worst(steps: &[Steps]) -> Option<(&Steps, f64)> {
    steps
        .iter()
        .filter_map(|step| step.check_over_decode().map(|ratio| (step, ratio)))
        .max_by(|a, b| a.1.total_cmp(&b.1))
}

/// Prints the worst check over decode against RFD 021's threshold.
fn print_verdict(steps: &[Steps]) {
    match worst(steps) {
        Some((step, ratio)) => println!(
            "worst check over decode: {ratio:.4} ({} at depth {}); RFD 021 revisits the uncapped check above {REVISIT_RATIO}: {}",
            step.variant.name(),
            step.depth,
            if ratio > REVISIT_RATIO {
                "above"
            } else {
                "within"
            },
        ),
        None => println!("no deleted-row check was timed"),
    }
}

/// The worst check over decode as the JSON records it.
fn verdict(steps: &[Steps]) -> Json {
    worst(steps).map_or(Json::Null, |(step, ratio)| {
        json!({
            "variant": step.variant.name(),
            "depth": step.depth,
            "check_over_decode": ratio,
            "revisit_ratio": REVISIT_RATIO,
            "within": ratio <= REVISIT_RATIO,
        })
    })
}

// ---------------------------------------------------------------------------
// Delete-state memory.

/// One delete state measured.
struct MemoryRun {
    form: Form,
    pattern: Pattern,
    per_mille: u64,
    deleted_rows: u64,
    delete_files: u64,
    state_bytes: u64,
    bound_bytes: u64,
    check_us: Option<f64>,
}

impl MemoryRun {
    fn header() -> String {
        format!(
            "{:<16} {:<9} {:>6} {:>9} {:>6} {:>11} {:>11} {:>6} {:>9} {:>9}",
            "form",
            "pattern",
            "pm",
            "rows",
            "files",
            "state_B",
            "bound_B",
            "ratio",
            "B/row",
            "check_us"
        )
    }

    fn row(&self) -> String {
        format!(
            "{:<16} {:<9} {:>6} {:>9} {:>6} {:>11} {:>11} {:>6.3} {:>9.3} {:>9.4}",
            self.form.name(),
            self.pattern.name(),
            self.per_mille,
            self.deleted_rows,
            self.delete_files,
            self.state_bytes,
            self.bound_bytes,
            approx::ratio(self.state_bytes, self.bound_bytes),
            approx::ratio(self.state_bytes, self.deleted_rows),
            self.check_us.unwrap_or_default(),
        )
    }

    fn to_json(&self) -> Json {
        json!({
            "form": self.form.name(),
            "pattern": self.pattern.name(),
            "deleted_per_mille": self.per_mille,
            "deleted_rows": self.deleted_rows,
            "delete_files": self.delete_files,
            "delete_state_bytes": self.state_bytes,
            "bound_bytes": self.bound_bytes,
            "state_over_bound": approx::ratio(self.state_bytes, self.bound_bytes),
            "bytes_per_deleted_row": approx::ratio(self.state_bytes, self.deleted_rows),
            "check_us": self.check_us,
        })
    }
}

/// The memory measurement: one table, and for every form, pattern, and
/// fraction a snapshot carrying only those deletes, loaded fresh in the
/// Full tier (`MemoryTable::measure`).
async fn memory(cli: &Cli, work: &Path) -> HarnessResult<Vec<MemoryRun>> {
    let mut table = MemoryTable::create(cli, &work.join("memory")).await?;
    let mut rng = StdRng::seed_from_u64(cli.seed);
    println!();
    println!("{}", MemoryRun::header());
    let mut runs = Vec::new();
    for form in or_all(&cli.form, &Form::ALL) {
        for pattern in or_all(&cli.pattern, &Pattern::ALL) {
            for per_mille in ascending(&cli.deleted_per_mille) {
                let run = table.measure(form, pattern, per_mille, &mut rng).await?;
                println!("{}", run.row());
                runs.push(run);
            }
        }
    }
    Ok(runs)
}

/// The memory measurement's table: its fixture, its data files, the
/// doctor's predicted Full memory with no delete, and the Disk tier's
/// directory, which the Full tier's loads never open.
struct MemoryTable {
    fixture: Fixture,
    data: Vec<DataFile>,
    base_full: u64,
    local: PathBuf,
}

impl MemoryTable {
    /// `--memory-files` data files of about `--memory-rows` rows under
    /// `dir`, one bucket each, loaded once with no delete.
    async fn create(cli: &Cli, dir: &Path) -> HarnessResult<Self> {
        let catalog = memory_catalog_local(dir).await?;
        let spec = FixtureSpec {
            buckets: cli.memory_files,
            ids: 0..u64::from(cli.memory_files) * cli.memory_rows,
            value_bytes: SMALL_VALUE_BYTES,
            ..FixtureSpec::small("memory", KeyType::Long)
        };
        let fixture = write_fixture(catalog.as_ref(), NAMESPACE, spec).await?;
        let local = dir.join("local");
        let served = build(&fixture, Residency::Full, &fixture.pinned_source(), &local)
            .await?
            .load()
            .await?;
        let base_full = summary_of(&served, &fixture).predicted.full;
        Ok(Self {
            data: fixture.files().to_vec(),
            fixture,
            base_full,
            local,
        })
    }

    /// One delete state: `per_mille` of every data file's rows deleted in
    /// `form` as `pattern` places them, committed alone, loaded fresh. The
    /// bound is the doctor's predicted Full memory less the table's with no
    /// delete: icetray-core's `with_deletes` adds the bound to the files'
    /// own prediction, which deletes do not change.
    async fn measure(
        &mut self,
        form: Form,
        pattern: Pattern,
        per_mille: u64,
        rng: &mut StdRng,
    ) -> HarnessResult<MemoryRun> {
        let targets: Vec<(DataFile, RoaringTreemap)> = self
            .data
            .iter()
            .map(|file| {
                let rows = deleted_rows(pattern, file.record_count(), per_mille, rng);
                (file.clone(), rows)
            })
            .collect();
        let check_us = state_check_micros(&targets, rng)?;
        let removed: u64 = targets.iter().map(|(_, rows)| rows.len()).sum();
        let name = format!("memory-{}-{}-{per_mille}", form.name(), pattern.name());
        let entries = self
            .fixture
            .write_deletes(targets, form.delete_form(), &name)
            .await?;
        self.fixture
            .commit_by_hand(HandSnapshot {
                data: self.data.clone(),
                deletes: entries,
            })
            .await?;
        let source = self.fixture.pinned_source();
        let served = build(&self.fixture, Residency::Full, &source, &self.local)
            .await?
            .load()
            .await?;
        let summary = summary_of(&served, &self.fixture);
        if summary.deletes.rows != removed {
            return Err(format!(
                "{name}: the generation deletes {} rows, the delete files {removed}",
                summary.deletes.rows
            )
            .into());
        }
        Ok(MemoryRun {
            form,
            pattern,
            per_mille,
            deleted_rows: removed,
            delete_files: summary.deletes.files,
            state_bytes: approx::count(summary.deletes.state_bytes),
            bound_bytes: summary.predicted.full.saturating_sub(self.base_full),
            check_us,
        })
    }
}

/// `per_mille` thousandths of a file of `records` rows, at least one row,
/// placed as `pattern` says.
fn deleted_rows(
    pattern: Pattern,
    records: u64,
    per_mille: u64,
    rng: &mut StdRng,
) -> RoaringTreemap {
    let deleted = (records * per_mille / PER_MILLE).max(1);
    match pattern {
        Pattern::Dense => {
            let start = rng.random_range(0..=records - deleted);
            let mut rows = RoaringTreemap::new();
            rows.insert_range(start..start + deleted);
            rows
        }
        Pattern::Scattered => {
            rand::seq::index::sample(rng, approx::length(records), approx::length(deleted))
                .into_iter()
                .map(approx::count)
                .collect()
        }
    }
}

/// Microseconds per deleted-row check over `targets`' rows as a generation
/// holds them, at `CHECKED_POSITIONS` random positions of each file.
fn state_check_micros(
    targets: &[(DataFile, RoaringTreemap)],
    rng: &mut StdRng,
) -> HarnessResult<Option<f64>> {
    let served = targets
        .iter()
        .map(|(_, rows)| served_rows(rows))
        .collect::<HarnessResult<Vec<_>>>()?;
    let checks: Vec<(&RoaringTreemap, u64)> = targets
        .iter()
        .zip(&served)
        .flat_map(|((file, _), rows)| {
            let records = file.record_count();
            (0..CHECKED_POSITIONS)
                .map(|_| (rows, rng.random_range(0..records)))
                .collect::<Vec<_>>()
        })
        .collect();
    Ok(check_micros(&checks))
}

// ---------------------------------------------------------------------------
// Load and refresh.

/// One delete-file count's loads and refreshes.
struct LoadRun {
    form: Form,
    delete_files: u64,
    loads: Latencies,
    load_gets: Vec<u64>,
    refreshes: Latencies,
    refresh_gets: Vec<u64>,
}

impl LoadRun {
    fn header() -> String {
        format!(
            "{:<16} {:>6} {:>12} {:>12} {:>8} {:>12} {:>12} {:>8}",
            "form",
            "files",
            "load_p50_us",
            "load_max_us",
            "gets",
            "refresh_p50",
            "refresh_max",
            "gets"
        )
    }

    fn row(&self) -> String {
        format!(
            "{:<16} {:>6} {:>12.0} {:>12.0} {:>8} {:>12.0} {:>12.0} {:>8}",
            self.form.name(),
            self.delete_files,
            micros(self.loads.percentile(P50)),
            micros(self.loads.max()),
            self.load_gets.first().copied().unwrap_or_default(),
            micros(self.refreshes.percentile(P50)),
            micros(self.refreshes.max()),
            self.refresh_gets.first().copied().unwrap_or_default(),
        )
    }

    fn to_json(&self) -> Json {
        json!({
            "form": self.form.name(),
            "delete_files": self.delete_files,
            "load_p50_us": micros(self.loads.percentile(P50)),
            "load_max_us": micros(self.loads.max()),
            "load_gets": self.load_gets,
            "refresh_p50_us": micros(self.refreshes.percentile(P50)),
            "refresh_max_us": micros(self.refreshes.max()),
            "refresh_gets": self.refresh_gets,
        })
    }
}

/// The load measurement: one table of a data file per bucket, one more
/// than the most delete files, and per form one delete file per data file,
/// each the only file of its kind its writer commits (a deletion vector in
/// a Puffin file of its own), deleting `ROWS_PER_DELETE_FILE` rows. For
/// every count `n`, a snapshot carrying the first `n` and one carrying the
/// first `n + 1`; each round moves the ref to the first, loads it fresh in
/// the Full tier through the table root, moves the ref to the second, and
/// refreshes onto it.
async fn load(cli: &Cli, work: &Path) -> HarnessResult<Vec<LoadRun>> {
    let counts = ascending(&cli.delete_files);
    let files = counts.last().copied().unwrap_or_default() + 1;
    let dir = work.join("load");
    let catalog = memory_catalog_local(&dir).await?;
    let spec = FixtureSpec {
        buckets: u32::try_from(files)?,
        ids: 0..files * cli.load_rows,
        value_bytes: SMALL_VALUE_BYTES,
        ..FixtureSpec::small("load", KeyType::Long)
    };
    let mut fixture = write_fixture(catalog.as_ref(), NAMESPACE, spec).await?;
    let data = fixture.files().to_vec();
    if approx::count(data.len()) != files {
        return Err(format!("{files} buckets wrote {} data files", data.len()).into());
    }
    println!();
    println!("{}", LoadRun::header());
    let mut runs = Vec::new();
    for form in or_all(&cli.form, &Form::ALL) {
        let mut written: Vec<DataFile> = Vec::new();
        for &count in &counts {
            let count = approx::length(count);
            while written.len() <= count {
                let file = &data[written.len()];
                let mut rows = RoaringTreemap::new();
                rows.insert_range(0..ROWS_PER_DELETE_FILE.min(file.record_count()));
                let name = format!("load-{}-{}", form.name(), written.len());
                written.extend(
                    fixture
                        .write_deletes(vec![(file.clone(), rows)], form.delete_form(), &name)
                        .await?,
                );
            }
            let run = load_rounds(cli, &mut fixture, &data, &written, count, form, &dir).await?;
            println!("{}", run.row());
            runs.push(run);
        }
    }
    Ok(runs)
}

/// `cli.load_rounds` loads of `fixture` with the first `count` of
/// `written` live beside `data`, each followed by a refresh onto one
/// more.
async fn load_rounds(
    cli: &Cli,
    fixture: &mut Fixture,
    data: &[DataFile],
    written: &[DataFile],
    count: usize,
    form: Form,
    dir: &Path,
) -> HarnessResult<LoadRun> {
    let mut snapshots = Vec::with_capacity(2);
    for listed in [count, count + 1] {
        fixture
            .commit_by_hand(HandSnapshot {
                data: data.to_vec(),
                deletes: written[..listed].to_vec(),
            })
            .await?;
        snapshots.push(
            fixture
                .snapshot_id()
                .ok_or("a by-hand commit leaves a snapshot")?,
        );
    }
    let (mut loads, mut load_gets, mut refreshes, mut refresh_gets) =
        (Vec::new(), Vec::new(), Vec::new(), Vec::new());
    for _ in 0..cli.load_rounds {
        fixture.move_ref(snapshots[0]).await?;
        let served = build(fixture, Residency::Full, &fixture.root_source(), dir).await?;
        let stats = served.storage.stats();
        let (before, started) = (stats.gets(), Instant::now());
        served.table.load().await?;
        loads.push(started.elapsed());
        load_gets.push(stats.gets() - before);
        expect_delete_files(&served, fixture, count)?;
        fixture.move_ref(snapshots[1]).await?;
        let (before, started) = (stats.gets(), Instant::now());
        let swapped = served.table.refresh().await?;
        refreshes.push(started.elapsed());
        refresh_gets.push(stats.gets() - before);
        if !swapped {
            return Err("the refresh found no new snapshot".into());
        }
        expect_delete_files(&served, fixture, count + 1)?;
    }
    Ok(LoadRun {
        form,
        delete_files: approx::count(count),
        loads: Latencies::of(loads),
        load_gets,
        refreshes: Latencies::of(refreshes),
        refresh_gets,
    })
}

/// That `served`'s generation applies `count` delete files.
fn expect_delete_files(served: &Served, fixture: &Fixture, count: usize) -> HarnessResult<()> {
    let applied = summary_of(served, fixture).deletes.files;
    if applied == approx::count(count) {
        Ok(())
    } else {
        Err(format!("the generation applies {applied} delete files, expected {count}").into())
    }
}
