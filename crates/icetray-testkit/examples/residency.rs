//! The residency sweep, a program and not a test: how a `disk` table's
//! lookup latency tracks the share of its chunk files the kernel's page
//! cache holds, the measurement that sets the page-cache condition's
//! thresholds and sample interval (RFD 020). It serves the benchmark
//! program's scattered table at `disk` residency, as
//! `dev/residency-sweep.sh` prepares it, warms every chunk file, and for
//! each eviction block size and each resident fraction drops the rest of
//! every chunk region from the page cache in seeded-random blocks
//! (`posix_fadvise` with `POSIX_FADV_DONTNEED`). It then reads the fraction
//! with the server's own sampler (`Generation::chunk_residency`), timing
//! the sample, and times hits and misses one at a time and `--concurrency`
//! at once, evicting the same blocks again every `--reevict-every` lookups,
//! since the lookups fault pages back in. Results print as a table and are
//! written as JSON under `--out-dir`.
//!
//! Linux only: no other platform drops a file's cached pages on request,
//! and elsewhere the program prints `unsupported` and exits. The local
//! directory must be on a filesystem whose cached reads do not block, such
//! as ext4 or xfs. Under a virtual machine such as colima, keep it and the
//! table on the machine's own disk, not a mount of the host's files, whose
//! reads the host's page cache can serve and so understate a device read.

use std::error::Error;
use std::fmt;
use std::fs::File;
use std::io;
use std::ops::Range;
use std::os::unix::fs::FileExt as _;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use clap::Parser;
use iceberg::spec::MAIN_BRANCH;
use icetray_core::ColumnRead;
use icetray_core::config::{
    FALLBACK_CACHE_BYTES, LocalConfig, Residency, SourceConfig, StorageConfig, TableConfig,
};
use icetray_core::{
    FALL_PER_MILLE, Generation, Key, KeyType, LocalDir, PER_MILLE, RECOVER_PER_MILLE, Storage,
    TableSource, approx, time,
};
use icetray_testkit::latency::{Latencies, P50, P90, P99, micros};
use icetray_testkit::layout::{KEY_COLUMN, VALUE_COLUMN};
use icetray_testkit::verify::{self, Served};
use icetray_testkit::{KeyShape, absent_key_from, key_at_shaped};
use rand::rngs::StdRng;
use rand::seq::SliceRandom as _;
use rand::{RngExt as _, SeedableRng as _};
use serde_json::{Value as Json, json};

/// Whatever failed; the program reports it and exits.
type SweepResult<T> = Result<T, Box<dyn Error + Send + Sync>>;

/// The name the table is served under.
const TABLE: &str = "scattered";

/// The steps of the default sweep's coarse descent: every tenth of the
/// chunk files resident, from all of them to none.
const TENTHS: u64 = 10;

/// The larger default eviction block: 64 KiB, about a key page of the
/// scattered table, so a block is a key page whole or in part. The smaller
/// is one page, the unit the kernel caches in.
const KEY_PAGE_BLOCK_BYTES: u64 = 64 * 1024;

/// Hits, and as many misses, per measured run by default: enough that the
/// 99th percentile rests on two hundred lookups.
const DEFAULT_LOOKUPS: usize = 20_000;

/// Lookups between evictions by default: few enough that the pages a run
/// faults back in move the resident fraction by a small part of a step.
const DEFAULT_REEVICT_EVERY: u64 = 1_000;

/// The seed of the default sweep, any fixed value, so two runs evict the
/// same blocks and ask the same keys.
const DEFAULT_SEED: u64 = 7;

/// The read size of the pass that warms a chunk file: a mebibyte, large
/// enough for the disk's throughput, one buffer however large the file.
const WARM_READ_BYTES: u64 = 1024 * 1024;

/// Bytes in a gibibyte, for the sampler's cost per gibibyte.
const GIBIBYTE: u64 = 1 << 30;

/// What a run looks up.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Kind {
    /// Keys the table holds: a key page and a value page each.
    Hit,
    /// Keys just above them, which it does not: a key page each.
    Miss,
}

impl fmt::Display for Kind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Hit => "hit",
            Self::Miss => "miss",
        })
    }
}

/// The sweep's command line.
#[derive(Parser, Debug)]
#[command(about = "Measures disk-tier lookup latency against chunk-file residency (Linux only)")]
struct Cli {
    /// The table's metadata file, as `bench --prepare --table scattered`
    /// prints it.
    #[arg(long)]
    location: String,
    /// Rows of that build, which the hits are drawn from.
    #[arg(long, value_parser = clap::value_parser!(u64).range(1..))]
    rows: u64,
    /// The local directory the chunk files are written to: on ext4 or xfs.
    #[arg(long, default_value = "target/residency/local")]
    local_dir: PathBuf,
    /// Where the JSON results go.
    #[arg(long, default_value = "target/residency")]
    out_dir: PathBuf,
    /// Resident fractions to measure at, in thousandths; when absent, every
    /// tenth and the points around the page-cache condition's thresholds.
    #[arg(
        long,
        value_delimiter = ',',
        value_parser = clap::value_parser!(u64).range(..=PER_MILLE)
    )]
    resident_per_mille: Vec<u64>,
    /// Sizes of the blocks evicted, in bytes, each at least the page size;
    /// when absent, one page and `KEY_PAGE_BLOCK_BYTES`.
    #[arg(long, value_delimiter = ',')]
    evict_block: Vec<u64>,
    /// Hits, and as many misses, per measured run.
    #[arg(long, default_value_t = DEFAULT_LOOKUPS)]
    lookups: usize,
    /// Lookups at once in the concurrent runs; the machine's cores when
    /// absent.
    #[arg(long)]
    concurrency: Option<usize>,
    /// Lookups between evictions of the same blocks again.
    #[arg(
        long,
        default_value_t = DEFAULT_REEVICT_EVERY,
        value_parser = clap::value_parser!(u64).range(1..)
    )]
    reevict_every: u64,
    /// The seed of the blocks chosen and the keys asked.
    #[arg(long, default_value_t = DEFAULT_SEED)]
    seed: u64,
}

/// The resident fractions the sweep measures at by default, in
/// thousandths, descending: every tenth, and around the page-cache
/// condition's thresholds each threshold, one point past it, and the
/// midpoints beside them, where latency is expected to bend.
fn default_points() -> Vec<u64> {
    let mut points: Vec<u64> = (0..=TENTHS)
        .map(|tenth| PER_MILLE * tenth / TENTHS)
        .collect();
    points.extend([
        u64::midpoint(RECOVER_PER_MILLE, PER_MILLE),
        RECOVER_PER_MILLE + 1,
        RECOVER_PER_MILLE,
        u64::midpoint(FALL_PER_MILLE, RECOVER_PER_MILLE),
        FALL_PER_MILLE,
        FALL_PER_MILLE - 1,
    ]);
    points.sort_unstable_by(|a, b| b.cmp(a));
    points.dedup();
    points
}

/// The eviction blocks to measure: `chosen`, each at least `page` bytes,
/// or one page and `KEY_PAGE_BLOCK_BYTES` when none is chosen.
fn blocks(chosen: &[u64], page: u64) -> SweepResult<Vec<u64>> {
    if chosen.is_empty() {
        return Ok(vec![page, KEY_PAGE_BLOCK_BYTES.max(page)]);
    }
    if let Some(small) = chosen.iter().find(|block| **block < page) {
        return Err(
            format!("an eviction block of {small} bytes is below the {page}-byte page").into(),
        );
    }
    Ok(chosen.to_vec())
}

/// The platform's page size in bytes.
fn page_bytes() -> io::Result<u64> {
    // SAFETY: `sysconf` takes a constant name and reads no memory of ours.
    let page = unsafe { libc::sysconf(libc::_SC_PAGESIZE) };
    match u64::try_from(page) {
        Ok(0) => Err(io::Error::from(io::ErrorKind::InvalidData)),
        Ok(page) => Ok(page),
        Err(_) => Err(io::Error::last_os_error()),
    }
}

/// One chunk file's key chunks, open for eviction and warming.
struct Region {
    file: File,
    chunks: Range<u64>,
}

/// What one sweep step evicts: blocks of the regions, by region index.
type Eviction = Vec<(usize, Range<u64>)>;

/// One measured run: its step, its lookups, and what it measured.
struct Run {
    block: u64,
    target_per_mille: u64,
    kind: Kind,
    concurrency: usize,
    fraction_before: f64,
    fraction_after: f64,
    latencies: Latencies,
    local_reads_per_lookup: f64,
    sample_us_per_gib: f64,
}

impl Run {
    fn to_json(&self) -> Json {
        json!({
            "evict_block": self.block,
            "target_per_mille": self.target_per_mille,
            "kind": self.kind.to_string(),
            "concurrency": self.concurrency,
            "fraction_before": self.fraction_before,
            "fraction_after": self.fraction_after,
            "p50_us": micros(self.latencies.percentile(P50)),
            "p90_us": micros(self.latencies.percentile(P90)),
            "p99_us": micros(self.latencies.percentile(P99)),
            "max_us": micros(self.latencies.max()),
            "local_reads_per_lookup": self.local_reads_per_lookup,
            "sample_us_per_gib": self.sample_us_per_gib,
        })
    }

    fn row(&self) -> String {
        format!(
            "{:>7} {:>6} {:<4} {:>3} {:>7.4} {:>7.4} {:>9.1} {:>9.1} {:>9.1} {:>9.1} {:>6.2} {:>9.1}",
            self.block,
            self.target_per_mille,
            self.kind,
            self.concurrency,
            self.fraction_before,
            self.fraction_after,
            micros(self.latencies.percentile(P50)),
            micros(self.latencies.percentile(P90)),
            micros(self.latencies.percentile(P99)),
            micros(self.latencies.max()),
            self.local_reads_per_lookup,
            self.sample_us_per_gib,
        )
    }
}

/// The table's column headings, in `Run::row`'s widths.
fn header() -> String {
    format!(
        "{:>7} {:>6} {:<4} {:>3} {:>7} {:>7} {:>9} {:>9} {:>9} {:>9} {:>6} {:>9}",
        "block",
        "target",
        "kind",
        "c",
        "before",
        "after",
        "p50_us",
        "p90_us",
        "p99_us",
        "max_us",
        "reads",
        "us/GiB",
    )
}

#[tokio::main]
async fn main() -> SweepResult<()> {
    let cli = Cli::parse();
    if !cfg!(target_os = "linux") {
        println!(
            "unsupported: the residency sweep drops pages with posix_fadvise, which only Linux honors"
        );
        return Ok(());
    }
    if cfg!(debug_assertions) {
        eprintln!("warning: an unoptimized build; pass --release");
    }
    let served = Arc::new(serve(&cli).await?);
    let generation = served.current();
    let regions = regions(&generation)?;
    let region_bytes: u64 = regions
        .iter()
        .map(|region| region.chunks.end - region.chunks.start)
        .sum();
    eprintln!(
        "{} chunk files, {:.1} GiB of key chunks",
        regions.len(),
        approx::ratio(region_bytes, GIBIBYTE)
    );
    let mut rng = StdRng::seed_from_u64(cli.seed);
    let hits: Arc<Vec<Key>> = Arc::new(
        (0..cli.lookups)
            .map(|_| {
                key_at_shaped(
                    rng.random_range(0..cli.rows),
                    KeyType::String,
                    0,
                    KeyShape::Scattered,
                )
            })
            .collect(),
    );
    let misses: Arc<Vec<Key>> = Arc::new(hits.iter().map(absent_key_from).collect());
    let concurrency = cli
        .concurrency
        .unwrap_or_else(|| std::thread::available_parallelism().map_or(1, std::num::NonZero::get))
        .max(1);
    println!("{}", header());
    let mut runs = Vec::new();
    let points = if cli.resident_per_mille.is_empty() {
        default_points()
    } else {
        cli.resident_per_mille.clone()
    };
    for block in blocks(&cli.evict_block, page_bytes()?)? {
        for &target in &points {
            let eviction = choose(&regions, block, target, &mut rng);
            for (kind, keys) in [(Kind::Hit, &hits), (Kind::Miss, &misses)] {
                for tasks in [1, concurrency] {
                    warm(&regions)?;
                    evict(&regions, &eviction)?;
                    let (fraction_before, sample) = sampled(&generation)?;
                    let reads_before = served.storage.stats().local_reads();
                    let latencies = lookups(
                        &served,
                        keys,
                        kind,
                        tasks,
                        cli.reevict_every,
                        &regions,
                        &eviction,
                    )
                    .await?;
                    let reads = served.storage.stats().local_reads() - reads_before;
                    let (fraction_after, _) = sampled(&generation)?;
                    let run = Run {
                        block,
                        target_per_mille: target,
                        kind,
                        concurrency: tasks,
                        fraction_before,
                        fraction_after,
                        local_reads_per_lookup: approx::ratio(reads, approx::count(keys.len())),
                        sample_us_per_gib: micros(sample) * approx::ratio(GIBIBYTE, region_bytes),
                        latencies,
                    };
                    println!("{}", run.row());
                    runs.push(run);
                }
            }
        }
    }
    let path = write_report(&cli, &runs)?;
    println!("results: {}", path.display());
    Ok(())
}

/// The scattered table at `disk` residency over production storage
/// defaults, its chunk files under `--local-dir`, lenient, loaded.
async fn serve(cli: &Cli) -> SweepResult<Served> {
    std::fs::create_dir_all(&cli.local_dir)?;
    let storage = Arc::new(Storage::new(StorageConfig::default()));
    let local = LocalDir::open(
        &LocalConfig {
            dir: cli.local_dir.clone(),
            index_budget_bytes: None,
            page_cache_disk_bytes: 0,
        },
        storage.stats(),
    )?;
    let source = SourceConfig::Static {
        location: cli.location.clone(),
    };
    let config = TableConfig {
        name: TABLE.to_string(),
        source: source.clone(),
        r#ref: MAIN_BRANCH.to_string(),
        key_column: KEY_COLUMN.to_string(),
        value_columns: vec![icetray_core::ValueColumnConfig::named(VALUE_COLUMN)],
        order_column: None,
        max_scan_rows: None,
        strict: false,
        lookups: None,
        residency: Residency::Disk,
        resident_budget_bytes: None,
    };
    let served = verify::build(
        storage,
        config,
        TableSource::from_config(&source).await?,
        Some(Arc::new(local)),
        approx::length(FALLBACK_CACHE_BYTES),
    )
    .await?;
    Ok(served.load().await?)
}

/// Every chunk file of `generation`, open, written back so that every page
/// of it can be dropped.
fn regions(generation: &Generation) -> SweepResult<Vec<Region>> {
    let mut regions = Vec::new();
    for (path, chunks) in generation.chunk_files() {
        let file = File::open(path)?;
        file.sync_all()?;
        regions.push(Region { file, chunks });
    }
    Ok(regions)
}

/// The blocks to evict for `target` thousandths of every region to stay
/// resident: each region cut into blocks of `block` bytes aligned to its
/// file's offsets, and of them the share `target` leaves out, chosen at
/// random.
fn choose(regions: &[Region], block: u64, target: u64, rng: &mut StdRng) -> Eviction {
    let mut eviction = Vec::new();
    for (at, region) in regions.iter().enumerate() {
        let Range { start, end } = region.chunks;
        let mut blocks: Vec<Range<u64>> = (start / block..end.div_ceil(block))
            .map(|index| (index * block).max(start)..((index + 1) * block).min(end))
            .collect();
        blocks.shuffle(rng);
        let evicted =
            approx::length(approx::count(blocks.len()) * (PER_MILLE - target) / PER_MILLE);
        eviction.extend(blocks.into_iter().take(evicted).map(|range| (at, range)));
    }
    eviction
}

/// Reads every region whole, so the page cache holds all of it.
fn warm(regions: &[Region]) -> io::Result<()> {
    let mut buf = Vec::new();
    for region in regions {
        let mut offset = region.chunks.start;
        while offset < region.chunks.end {
            let take = (region.chunks.end - offset).min(WARM_READ_BYTES);
            buf.resize(approx::length(take), 0);
            region.file.read_exact_at(&mut buf, offset)?;
            offset += take;
        }
    }
    Ok(())
}

/// Drops `eviction`'s blocks from the page cache.
fn evict(regions: &[Region], eviction: &Eviction) -> io::Result<()> {
    for (at, range) in eviction {
        drop_pages(&regions[*at].file, range)?;
    }
    Ok(())
}

/// Drops the pages wholly inside `range` of `file` from the page cache.
#[cfg(target_os = "linux")]
fn drop_pages(file: &File, range: &Range<u64>) -> io::Result<()> {
    use std::os::fd::AsRawFd as _;
    let offset = libc::off_t::try_from(range.start).map_err(io::Error::other)?;
    let len = libc::off_t::try_from(range.end - range.start).map_err(io::Error::other)?;
    // SAFETY: `posix_fadvise` takes no pointer; the descriptor is `file`'s,
    // open for the call.
    let advised =
        unsafe { libc::posix_fadvise(file.as_raw_fd(), offset, len, libc::POSIX_FADV_DONTNEED) };
    if advised == 0 {
        Ok(())
    } else {
        Err(io::Error::from_raw_os_error(advised))
    }
}

/// No platform but Linux drops a file's pages on request.
#[cfg(not(target_os = "linux"))]
fn drop_pages(_file: &File, _range: &Range<u64>) -> io::Result<()> {
    Err(io::Error::from(io::ErrorKind::Unsupported))
}

/// The resident fraction the server's sampler reads, and how long the
/// sample took.
fn sampled(generation: &Generation) -> SweepResult<(f64, Duration)> {
    let started = Instant::now();
    let residency = generation.chunk_residency()?;
    Ok((residency.fraction(), started.elapsed()))
}

/// Looks up every one of `keys`, `tasks` at once, evicting `eviction`'s
/// blocks again before every `every` lookups; each lookup's latency. Each
/// key must answer as `kind` says, present for a hit and absent for a
/// miss, or the run fails.
async fn lookups(
    served: &Arc<Served>,
    keys: &Arc<Vec<Key>>,
    kind: Kind,
    tasks: usize,
    every: u64,
    regions: &[Region],
    eviction: &Eviction,
) -> SweepResult<Latencies> {
    let mut samples = Vec::with_capacity(keys.len());
    for round in keys.chunks(approx::length(every)) {
        evict(regions, eviction)?;
        let share = round.len().div_ceil(tasks);
        let mut running = Vec::with_capacity(tasks);
        for part in round.chunks(share) {
            let served = Arc::clone(served);
            let part = part.to_vec();
            running.push(tokio::spawn(async move {
                let mut took = Vec::with_capacity(part.len());
                for key in &part {
                    let started = Instant::now();
                    let found = served
                        .table
                        .get(key, &[ColumnRead::whole(VALUE_COLUMN)])
                        .await?;
                    took.push(started.elapsed());
                    if verify::one_value(&found.row).is_some() != (kind == Kind::Hit) {
                        return Err(format!("{key:?} answered as no {kind}").into());
                    }
                }
                Ok::<_, Box<dyn Error + Send + Sync>>(took)
            }));
        }
        for task in running {
            samples.extend(task.await??);
        }
    }
    Ok(Latencies::of(samples))
}

/// Writes `runs` as JSON under `--out-dir`, named by the time; the path.
fn write_report(cli: &Cli, runs: &[Run]) -> SweepResult<PathBuf> {
    let report = json!({
        "location": cli.location,
        "rows": cli.rows,
        "lookups": cli.lookups,
        "reevict_every": cli.reevict_every,
        "seed": cli.seed,
        "runs": runs.iter().map(Run::to_json).collect::<Vec<_>>(),
    });
    write_json(&cli.out_dir, &report)
}

/// `report` under `dir`, named by the time.
fn write_json(dir: &Path, report: &Json) -> SweepResult<PathBuf> {
    std::fs::create_dir_all(dir)?;
    let stamp = time::log_timestamp(time::now()).replace(':', "-");
    let path = dir.join(format!("residency-{stamp}.json"));
    std::fs::write(&path, serde_json::to_vec_pretty(report)?)?;
    Ok(path)
}
