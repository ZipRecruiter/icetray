//! The shared value-page cache (RFD 004): pages keyed by file and page
//! offset, never invalidated because a data file never changes, evicted by
//! size alone, with concurrent misses on one page coalesced into one fetch.
//! With a local directory (RFD 017) the cache has a disk level: a page
//! evicted from memory is written there, served back with a local read and
//! no GET, and recovered when the next process opens the same directory.

use std::collections::HashMap;
use std::fmt;
use std::future::Future;
use std::hash::{BuildHasher, RandomState};
use std::io::{self, Read, Write};
use std::mem::size_of;
use std::ops::Range;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use bytes::Bytes;
use foyer::{
    BlockEngineConfig, Cache, CacheBuilder, Code, DeviceBuilder as _, ErrorKind, FsDeviceBuilder,
    HybridCache, HybridCacheBuilder, HybridCachePolicy, RecoverMode, S3FifoConfig, Source,
};
use metrics::Counter;
use tokio::sync::{OwnedSemaphorePermit, Semaphore, watch};

use crate::approx;
use crate::config::ConfigViolation;
use crate::contract::{PAGE_CACHE_DISK_BLOCK_BYTES, VALUE_PAGE_BYTES};
use crate::descriptors;
use crate::error::{Error, Recurrence, Recurs, Result};
use crate::local::DiskLevel;
use crate::memory::MemoryLimit;
use crate::metric;
use crate::shard::{self, Sharded, Tally};
use crate::storage::IoStats;

/// The name foyer files its own log lines under. Its metrics stay on its
/// no-op registry; the crate exports the page cache's counters itself.
const CACHE_NAME: &str = "icetray_pages";

/// Bytes charged against the memory budget per entry on top of the page
/// bytes: foyer's record (hash, weight, reference count, list links), the
/// key's `Arc<str>` pointer and offset, and the `Bytes` handle come to about
/// this on a 64-bit target. The file id's own bytes are shared by every
/// page of the file through the `Arc` and are not charged per entry.
const ENTRY_OVERHEAD_BYTES: usize = 64;

/// The shards of the memory level: `shard::count()`, the runtime's worker
/// threads rounded up to a power of two. foyer splits the memory budget
/// evenly across its shards and evicts within each, so a page is held only
/// beside the pages of its own shard, in that shard's share; and an insert
/// takes its shard's lock exclusively, so fewer shards than workers makes
/// concurrent fetches wait on each other. `min_memory_bytes` is computed
/// from the same count.
pub(crate) fn memory_shards() -> usize {
    shard::count()
}

/// The memory level's eviction: S3-FIFO. A page enters a small queue, a
/// tenth of its shard, and moves to the main queue only when it is hit
/// again before it reaches the small queue's end, so a pass over many
/// pages read once displaces those pages and not the ones that keep being
/// asked for. A hit takes its shard's lock shared, to count itself on the
/// page; an LRU takes it exclusively on every hit to move the page.
fn eviction() -> S3FifoConfig {
    S3FifoConfig::default()
}

/// The smallest memory budget a page cache of `shards` shards is given:
/// one contract value page (`contract::VALUE_PAGE_BYTES`) weighed with its
/// entry overhead, for each shard. Below it a shard's share cannot hold one
/// such page, and foyer keeps the shard's latest page alone whatever its
/// share, so the cache would hold a page per shard however small the budget
/// and the configured bytes would bound nothing. `Config::parse` refuses
/// less than one shard's minimum, which no machine goes below, and
/// `open_with` refuses less than the minimum of the shards the cache will
/// have on the runtime it opens on (`check_capacity`).
pub(crate) fn min_memory_bytes(shards: usize) -> u64 {
    approx::count(shards) * (VALUE_PAGE_BYTES + approx::count(ENTRY_OVERHEAD_BYTES))
}

/// `ConfigViolation::PageCacheTooSmall` for a memory budget of `bytes`
/// below `min_memory_bytes(shards)`, naming the memory `limit` the budget
/// was derived from, if any. Parsing calls it with one shard; `open_with`
/// with `memory_shards()` before it opens a cache, since the worker count
/// is not known when the configuration is parsed.
pub(crate) fn check_capacity(
    bytes: u64,
    shards: usize,
    limit: Option<MemoryLimit>,
) -> std::result::Result<(), ConfigViolation> {
    let minimum = min_memory_bytes(shards);
    if bytes < minimum {
        return Err(ConfigViolation::PageCacheTooSmall {
            bytes,
            minimum,
            shards,
            limit,
        });
    }
    Ok(())
}

/// The width of the length prefix on an encoded file id: a `u32`, since a
/// file id is a location and never approaches four gibibytes.
const FILE_ID_LEN_BYTES: usize = size_of::<u32>();

/// The width of an encoded page offset: a `u64`, little-endian.
const OFFSET_BYTES: usize = size_of::<u64>();

/// Why the page cache could not produce a page.
#[derive(Debug, thiserror::Error)]
pub enum CacheError {
    /// The fetch the cache ran for the caller failed. Every waiter coalesced
    /// onto that fetch receives the same failure, so it is shared.
    #[error("fetch failed: {0}")]
    Fetch(Arc<Error>),
    /// The fetch the cache ran for the caller did not finish within the
    /// fetch deadline of the handle that ran it, so the fetch was dropped,
    /// and an S3 GET it had open was canceled with it. Every waiter
    /// coalesced onto that fetch receives this, and nothing is cached for
    /// the page.
    #[error("fetch of {path} at offset {offset} did not finish within {after:?}")]
    FetchDeadline {
        /// The data file's location.
        path: String,
        /// The page's byte offset in the file.
        offset: u64,
        /// The deadline that passed.
        after: Duration,
    },
    /// The cache itself failed: the disk level could not be opened or read,
    /// or foyer refused an operation. foyer's error is kept whole, since it
    /// is `std::error::Error + Send + Sync` and carries its kind for a
    /// caller that matches on it.
    #[error("backend: {0}")]
    Backend(#[from] foyer::Error),
    /// Opening the disk level ran out of file descriptors: the level holds
    /// one open file per block, and the process's open-file limit is below
    /// what the level needs. No load runs while the level opens, so the
    /// estimate is the level's alone; the start's warning counts the loads
    /// too. The operating system's error is kept; foyer's own wrapping of
    /// it is not, since it names no cause.
    #[error(
        "open-file limit {limit} is below the {needed} descriptors the disk level needs: {source}"
    )]
    DescriptorLimit {
        /// The limit in force when opening failed (`descriptors::limit`).
        limit: u64,
        /// The estimate for this disk level with no load running
        /// (`descriptors::needed`).
        needed: u64,
        /// The operating system's refusal: too many open files in the
        /// process, or in the system.
        source: io::Error,
    },
}

impl Recurs for CacheError {
    fn recurrence(&self) -> Recurrence {
        match self {
            Self::Fetch(error) => error.recurrence(),
            Self::FetchDeadline { .. } | Self::Backend(_) | Self::DescriptorLimit { .. } => {
                Recurrence::Transient
            }
        }
    }
}

/// A page's identity: the file it belongs to and the byte offset of its
/// header, which is where the offset index places it, so the offset alone
/// distinguishes pages within a file. A file never changes, so a key never
/// goes stale. Encoded for the disk level as the file id behind a `u32`
/// length and the offset as eight little-endian bytes.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct PageKey(Arc<str>, u64);

impl Code for PageKey {
    fn encode(&self, writer: &mut impl Write) -> foyer::Result<()> {
        let len = u32::try_from(self.0.len()).map_err(|error| {
            foyer::Error::new(
                ErrorKind::OutOfRange,
                format!(
                    "file id of {} bytes is longer than its length prefix carries",
                    self.0.len()
                ),
            )
            .with_source(error)
        })?;
        writer
            .write_all(&len.to_le_bytes())
            .map_err(foyer::Error::io_error)?;
        writer
            .write_all(self.0.as_bytes())
            .map_err(foyer::Error::io_error)?;
        writer
            .write_all(&self.1.to_le_bytes())
            .map_err(foyer::Error::io_error)
    }

    fn decode(reader: &mut impl Read) -> foyer::Result<Self> {
        let mut len = [0; FILE_ID_LEN_BYTES];
        reader
            .read_exact(&mut len)
            .map_err(foyer::Error::io_error)?;
        let len = u64::from(u32::from_le_bytes(len));
        // Sized by the bytes present, never by the prefix: a prefix read
        // off a damaged block must not size an allocation.
        let mut id = Vec::new();
        reader
            .by_ref()
            .take(len)
            .read_to_end(&mut id)
            .map_err(foyer::Error::io_error)?;
        if approx::count(id.len()) != len {
            return Err(foyer::Error::io_error(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                format!("file id of {len} bytes ends after {} bytes", id.len()),
            )));
        }
        let id = String::from_utf8(id).map_err(|error| {
            foyer::Error::new(ErrorKind::Parse, "file id is not utf-8").with_source(error)
        })?;
        let mut offset = [0; OFFSET_BYTES];
        reader
            .read_exact(&mut offset)
            .map_err(foyer::Error::io_error)?;
        Ok(Self(Arc::from(id), u64::from_le_bytes(offset)))
    }

    fn estimated_size(&self) -> usize {
        FILE_ID_LEN_BYTES + self.0.len() + OFFSET_BYTES
    }
}

/// A page's bytes as the object store returned them, compressed as the file
/// holds them. Encoded for the disk level as the raw bytes and nothing
/// else: foyer hands the decoder exactly the bytes it wrote.
#[derive(Clone, Debug)]
struct PageValue(Bytes);

impl Code for PageValue {
    fn encode(&self, writer: &mut impl Write) -> foyer::Result<()> {
        writer.write_all(&self.0).map_err(foyer::Error::io_error)
    }

    fn decode(reader: &mut impl Read) -> foyer::Result<Self> {
        let mut bytes = Vec::new();
        reader
            .read_to_end(&mut bytes)
            .map_err(foyer::Error::io_error)?;
        Ok(Self(Bytes::from(bytes)))
    }

    fn estimated_size(&self) -> usize {
        self.0.len()
    }
}

/// Why a fetch the cache ran produced no page. It travels through foyer as
/// the source of foyer's own error, so that every coalesced waiter recovers
/// it whole, the one `Arc<Error>` of a failure included, and it is never
/// flattened to text.
#[derive(Clone, Debug, thiserror::Error)]
enum FetchFailed {
    /// The caller's fetch future failed.
    #[error(transparent)]
    Failed(Arc<Error>),
    /// The fetch ran past the deadline of the handle that ran it and was
    /// dropped.
    #[error("fetch of {} at offset {} did not finish within {after:?}", .page.0, .page.1)]
    Deadline { page: PageKey, after: Duration },
}

/// The cache's levels: memory alone, or memory over a disk level. Both are
/// one type to the rest of the crate; which one runs is decided once, by
/// whether a local directory is configured (RFD 017).
#[derive(Debug)]
enum Level {
    /// Memory only: an evicted page is gone, and the next request for it
    /// fetches it again.
    Memory(Cache<PageKey, PageValue>),
    /// Memory over a disk level in the local directory. A page evicted from
    /// memory is written there, served back with a local read charged to
    /// `stats`, and recovered when the level is reopened.
    Hybrid {
        cache: HybridCache<PageKey, PageValue>,
        stats: Arc<IoStats>,
    },
}

/// Where one call's page came from, as the `result` label of a lookup
/// distinguishes it: fetched from outside, or served by the cache. A caller
/// that joined another caller's fetch counts as `Fetched`, since it waited
/// on the GET; a page from the disk level is `Cached`, since it cost no GET.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum PageOrigin {
    /// The page came from the caller's fetch, run or joined.
    Fetched,
    /// The page was in memory or on the disk level.
    Cached,
}

/// The origin of a page from where foyer says the entry came.
fn origin_of(source: Source) -> PageOrigin {
    match source {
        Source::Outer => PageOrigin::Fetched,
        Source::Memory | Source::Disk => PageOrigin::Cached,
    }
}

/// The shared value-page cache. Clones share one cache and one set of
/// counters: every table of a process holds a clone of the same cache,
/// whose pages a key-value table's lookups and an ordered table's Scans
/// alike read through, from memory and then from the disk level (RFD
/// 027). A clone may carry a fetch deadline of its own
/// (`with_fetch_deadline`), which bounds the fetches that clone runs.
#[derive(Clone, Debug)]
pub struct PageCache {
    level: Arc<Level>,
    hits: Arc<Tally>,
    fetches: Arc<Fetches>,
    /// The pages span reads are fetching now, which another span read
    /// that needs one of them waits for rather than fetching it again
    /// (`get_span`).
    spans: Arc<SpanFlights>,
    /// How long a fetch this handle runs may take; `None` is no bound.
    fetch_deadline: Option<Duration>,
}

/// The pages span reads of one cache are fetching (`PageCache::get_span`),
/// split by file over `shard::count()` shards, so that span reads of
/// different files, and so of different tables, mostly take different
/// locks; and the count of pages a span read found in flight, here or as
/// a lookup's fetch (`PageCache::read_disk`), and waited for.
struct SpanFlights {
    shards: Sharded<FlightShard>,
    /// Picks a file's shard from its id.
    hasher: RandomState,
    joined: AtomicU64,
}

impl SpanFlights {
    fn new() -> Self {
        Self {
            shards: Sharded::new(),
            hasher: RandomState::new(),
            joined: AtomicU64::new(0),
        }
    }

    /// The shard that holds the pages of `file` in flight.
    fn shard(&self, file: &Arc<str>) -> &FlightShard {
        self.shards.of(self.hasher.hash_one(&**file))
    }
}

impl fmt::Debug for SpanFlights {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SpanFlights")
            .field("joined", &self.joined.load(Ordering::Relaxed))
            .finish_non_exhaustive()
    }
}

/// `FlightShard` in a module of its own, so that its pages are reachable
/// only through `FlightShard::lock`, which counts every lock the lock
/// tests assert on.
mod flight_shard {
    #[cfg(test)]
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::{Mutex, MutexGuard, PoisonError};

    use super::InFlight;

    /// The pages in flight of the files one shard of `SpanFlights` holds:
    /// per page, the outcome its fetch will publish.
    #[derive(Default)]
    pub(super) struct FlightShard {
        /// Never held across an `await`.
        pages: Mutex<InFlight>,
        /// How many times `lock` has taken `pages`: what the lock tests
        /// count.
        #[cfg(test)]
        pub(super) locked: AtomicU64,
    }

    impl FlightShard {
        /// The shard's pages in flight, locked.
        pub(super) fn lock(&self) -> MutexGuard<'_, InFlight> {
            #[cfg(test)]
            self.locked.fetch_add(1, Ordering::Relaxed);
            self.pages.lock().unwrap_or_else(PoisonError::into_inner)
        }
    }
}
use flight_shard::FlightShard;

/// What a span fetch publishes for one of its pages: the page's bytes, or
/// why the fetch produced none.
type SpanOutcome = std::result::Result<Bytes, FetchFailed>;

/// Pages in flight, each with the receiver its fetch publishes to.
type InFlight = HashMap<PageKey, watch::Receiver<Option<SpanOutcome>>>;

impl PageCache {
    /// Opens a cache holding at most `capacity_bytes` of pages in memory,
    /// each weighed as its bytes plus a fixed per-entry overhead, split
    /// evenly over `memory_shards()`; `open_with` checks the budget against
    /// `min_memory_bytes` of that count first (`check_capacity`), and a
    /// smaller budget given here directly is taken as it is. `disk` is
    /// the local level of RFD 017: a page evicted from memory is written
    /// there, served back with a local read and no GET, and recovered when
    /// a new cache opens over the same directory, so it survives a restart.
    /// Opening the level opens its files, one per block and each held open
    /// for the life of the cache, and reads back what they hold, which is
    /// why this is async and can fail: running out of descriptors is
    /// `CacheError::DescriptorLimit`, any other failure
    /// `CacheError::Backend`.
    pub async fn new(capacity_bytes: usize, disk: Option<DiskLevel>) -> Result<Self> {
        let level = match disk {
            None => memory_level(capacity_bytes),
            Some(disk) => Level::Hybrid {
                cache: open_hybrid(capacity_bytes, &disk)
                    .await
                    .map_err(|error| open_error(error, &disk))?,
                stats: Arc::clone(disk.stats()),
            },
        };
        Ok(Self {
            level: Arc::new(level),
            hits: Arc::new(Tally::new(metrics::counter!(metric::PAGE_CACHE_HITS_TOTAL))),
            fetches: Arc::new(Fetches::new()),
            spans: Arc::new(SpanFlights::new()),
            fetch_deadline: None,
        })
    }

    /// This handle on the same cache and counters, its fetches ending at
    /// `deadline` from their start: past it the fetch future is dropped,
    /// canceling an S3 GET it has open, and every caller waiting on it receives
    /// `CacheError::FetchDeadline`. A `Table` holds its cache this way,
    /// bounded by `server.get_deadline`.
    pub(crate) fn with_fetch_deadline(self, deadline: Duration) -> Self {
        Self {
            fetch_deadline: Some(deadline),
            ..self
        }
    }

    /// The page at `key` and where it came from: from memory, from the disk
    /// level, or from `fetch`. Concurrent callers for one key share one
    /// fetch and one disk read. `fetch` may be called for a caller that then
    /// joins another caller's fetch; the future it returns runs only when
    /// the cache needs the page from outside, and `fetches` counts those
    /// runs. A disk level that fails to read the page falls through to the
    /// fetch (RFD 017: the directory is disposable). A failure of the fetch
    /// future reaches every waiting caller as `CacheError::Fetch` and leaves
    /// nothing cached; a failure of the cache itself is `CacheError::Backend`.
    /// foyer runs the fetch on a task of its own, so a caller that stops
    /// waiting does not end it; on a handle with a fetch deadline the fetch
    /// ends at that deadline from its start, its future dropped, and every
    /// waiting caller receives `CacheError::FetchDeadline` with nothing
    /// cached. The disk level's read is not bounded by it.
    /// The origin is what the `result` label of a lookup is decided by, per
    /// call rather than by differencing the process-wide counters, which
    /// concurrent lookups would confuse. A page in memory is served on the
    /// caller's task with nothing taken of `issue`; any other waits here
    /// for `issue`'s permit (`Issue`) before the cache is asked for it, so
    /// a caller dropped while it waits leaves nothing queued in the cache.
    pub(crate) async fn get_or_fetch<F, Fut>(
        &self,
        key: (Arc<str>, u64),
        issue: Issue<'_>,
        fetch: F,
    ) -> Result<(Bytes, PageOrigin)>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<Bytes>> + Send + 'static,
    {
        let key = PageKey(key.0, key.1);
        if let Some(page) = self.in_memory(&key) {
            self.record_hit();
            return Ok((page, PageOrigin::Cached));
        }
        // foyer calls `fetch` on every call, so the run, and the permit in
        // it, is built as well for a page that reached memory while the
        // permit was awaited, a page on the disk level, and a fetch joined,
        // none of which runs it (`Issue` says when each frees the permit).
        let run = self.run_for(key.clone(), issue.permit().await, issue);
        // foyer's `get_or_fetch` and not its `get` past memory: `get`
        // reports a disk level that fails to read as an error while
        // `get_or_fetch` moves on to the fetch, which is what a disposable
        // directory calls for (RFD 017). The levels' entries differ in
        // type, so each arm reduces its own to the source and the page.
        let (served, stats) = match self.level.as_ref() {
            Level::Memory(cache) => (
                cache
                    .get_or_fetch(&key, || run.fetch(fetch()))
                    .await
                    .map(|entry| (entry.source(), entry.value().0.clone())),
                None,
            ),
            Level::Hybrid { cache, stats } => (
                cache
                    .get_or_fetch(&key, || run.fetch(fetch()))
                    .await
                    .map(|entry| (entry.source(), entry.value().0.clone())),
                Some(stats.as_ref()),
            ),
        };
        let (source, page) = served.map_err(cache_error)?;
        self.record(source, stats, &page);
        Ok((page, origin_of(source)))
    }

    /// The pages at `pages`, ranges of the file `file` in ascending order
    /// that do not overlap, each page's bytes in that order, and whether any
    /// came from outside. A page in memory is served from it, counted as a
    /// hit, without the in-flight lock, so a read memory holds whole takes
    /// no lock at all (`pending`). A page another span read of this cache
    /// is fetching is waited for and served from that fetch, counted in
    /// neither `hits` nor `fetches`, so concurrent span reads that need one
    /// page issue one GET for it, as `get_or_fetch` coalesces a lookup's
    /// page (RFD 025, Caching and hedging); its fetch's failure is this
    /// read's. A page neither in memory nor in flight that the disk level
    /// holds is read from it, the read's disk reads together, and counted
    /// a hit and a local read as a lookup's disk hit is (RFD 027, A span
    /// read); one the level fails to read is fetched (RFD 017), and one a
    /// lookup is fetching is served from that fetch, counted as a page
    /// joined in flight is and from outside (`read_disk`). Every
    /// maximal run of the remaining pages whose ranges meet end to start is
    /// one call of `fetch` over the run's whole range, its bytes split by
    /// page and each page inserted into memory as its own entry, keyed by
    /// `file` and the page's offset as `get_or_fetch` keys a page, so that
    /// a later read of any one of them finds it alone, and reaching the
    /// disk level when it leaves memory, then handed to every read waiting
    /// on it. The runs are fetched together, and the waits run beside them.
    /// Each run takes one permit of `issue` before it claims its pages as in
    /// flight and holds it until it ends, so none waits for a permit while
    /// it holds one (RFD 022), and no read waits on a run still queued for
    /// its permit: a read of another call fetches such a page itself, as
    /// `get_or_fetch` asks the cache only once it holds its permit (RFD 013).
    /// A run claims what of it is still neither in memory nor in flight once
    /// it holds its permit, and the read takes stock again for the rest
    /// (`claim_run`). A run counts once in `fetches` and in `issue`'s counter
    /// as it starts, a run that then fails included, and ends at the
    /// handle's fetch deadline from its start, its future dropped, as
    /// `CacheError::FetchDeadline` at the run's first offset. A failed run
    /// is `CacheError::Fetch` and caches nothing of it. A read returns at
    /// its first failure, of a run of its own or of a fetch it waits on,
    /// without waiting for the rest: its runs still in flight are dropped
    /// with it, releasing their pages as a dropped read does, so a read
    /// that joined one of those runs fetches its pages again. A read dropped
    /// before its runs end releases their pages unserved, and a read
    /// waiting on one of them claims it again itself, asking the disk level
    /// first. A run whose bytes come back shorter than its range is
    /// `CacheError::Fetch` of a parquet end-of-file error.
    pub(crate) async fn get_span<F, Fut>(
        &self,
        file: &Arc<str>,
        pages: &[Range<u64>],
        issue: Issue<'_>,
        fetch: F,
    ) -> Result<(Vec<Bytes>, PageOrigin)>
    where
        F: Fn(Range<u64>) -> Fut,
        Fut: Future<Output = Result<Bytes>>,
    {
        let mut served: Vec<Option<Bytes>> = vec![None; pages.len()];
        let mut from_outside = false;
        loop {
            let Pending {
                runs,
                waits,
                joined_fetch,
            } = self.pending(file, pages, &mut served).await;
            from_outside |= joined_fetch;
            if runs.is_empty() && waits.is_empty() {
                break;
            }
            let fetching = futures::future::try_join_all(
                runs.into_iter()
                    .map(|run| self.fetch_run(file, pages, run, issue, &fetch)),
            );
            let waiting = futures::future::try_join_all(waits.into_iter().map(
                |(at, mut flight)| async move {
                    let outcome = flight
                        .wait_for(Option::is_some)
                        .await
                        .ok()
                        .and_then(|outcome| outcome.clone());
                    match outcome {
                        Some(Ok(bytes)) => Ok(Some((at, bytes))),
                        Some(Err(failed)) => Err(fetch_failed(failed)),
                        // The fetch it waited on was dropped unfinished:
                        // the next pass claims the page.
                        None => Ok(None),
                    }
                },
            ));
            let (fetched, waited) = futures::try_join!(fetching, waiting)?;
            for (at, bytes) in fetched
                .into_iter()
                .flatten()
                .chain(waited.into_iter().flatten())
            {
                served[at] = Some(bytes);
                from_outside = true;
            }
        }
        let origin = if from_outside {
            PageOrigin::Fetched
        } else {
            PageOrigin::Cached
        };
        let pages = served
            .into_iter()
            .map(|page| page.expect("every page is served from memory, a fetch, or a join"))
            .collect();
        Ok((pages, origin))
    }

    /// One run of `get_span`'s pages, at `run` of `pages` of `file`: waits
    /// for `issue`'s permit holding no claim, claims what of the run is
    /// still neither in memory nor in flight (`claim_run`), and fetches it
    /// with `fetch` under the permit, inserting each page and handing the
    /// outcome to every read waiting on one. The positions and bytes of the
    /// pages it fetched, none when other reads took the whole run while it
    /// waited.
    async fn fetch_run<F, Fut>(
        &self,
        file: &Arc<str>,
        pages: &[Range<u64>],
        run: Range<usize>,
        issue: Issue<'_>,
        fetch: &F,
    ) -> Result<Vec<(usize, Bytes)>>
    where
        F: Fn(Range<u64>) -> Fut,
        Fut: Future<Output = Result<Bytes>>,
    {
        let permit = issue.permit().await;
        let Some(claim) = self.claim_run(file, pages, run) else {
            return Ok(Vec::new());
        };
        let run = claim.run.clone();
        let span = pages[run.start].start..pages[run.end - 1].end;
        let fetched = fetch(span.clone());
        let run_of = self.run_for(PageKey(Arc::clone(file), span.start), permit, issue);
        let outcome = match run_of.run(fetched, std::convert::identity).await {
            Ok(bytes) => split_span(file, &pages[run.clone()], &bytes),
            Err(failed) => Err(failed),
        };
        if let Ok(split) = &outcome {
            for (page, bytes) in pages[run.clone()].iter().zip(split) {
                self.insert(PageKey(Arc::clone(file), page.start), bytes.clone());
            }
        }
        claim.publish(&outcome);
        outcome
            .map(|split| run.zip(split).collect())
            .map_err(fetch_failed)
    }

    /// Every page of `pages` of `file`, keyed as `get_span` keys them, when
    /// memory holds them all, each counted a hit; `None` when memory lacks
    /// one, with nothing counted, claimed, or fetched. A page in memory is
    /// whole and final, so no in-flight lock is taken. What a Scan reads to
    /// place a bound exactly from pages it holds before its round to object
    /// storage (RFD 025, Cost).
    pub(crate) fn held_span(&self, file: &Arc<str>, pages: &[Range<u64>]) -> Option<Vec<Bytes>> {
        let held = pages
            .iter()
            .map(|page| self.in_memory(&PageKey(Arc::clone(file), page.start)))
            .collect::<Option<Vec<Bytes>>>()?;
        for _ in &held {
            self.record_hit();
        }
        Some(held)
    }

    /// Takes stock of `pages` of `file` that `served` does not hold yet: a
    /// page in memory is served and counted a hit, a page another read is
    /// fetching is joined and counted in `span_joins`, a page the disk
    /// level holds is served from it (`read_disk`), and every maximal run
    /// of the rest whose ranges meet end to start is to be fetched. Memory
    /// is read first without the in-flight lock, since a page in memory is
    /// whole and final, so a read memory holds whole takes no lock; the
    /// lock of `file`'s shard is taken only for the pages memory lacks, and
    /// memory is read again under it for a page not in flight, whose fetch
    /// may have inserted it and given up its claim in between. The disk
    /// level is asked only for the pages neither in memory nor in flight
    /// under the lock, after it is released, so a page another read is
    /// fetching is joined and never read from disk. Nothing is claimed
    /// here: a run claims its pages only once it holds its permit
    /// (`claim_run`), and so skips a page another read claimed while this
    /// one read the disk level.
    async fn pending(
        &self,
        file: &Arc<str>,
        pages: &[Range<u64>],
        served: &mut [Option<Bytes>],
    ) -> Pending {
        let mut missing = false;
        for (at, page) in pages.iter().enumerate() {
            if served[at].is_some() {
                continue;
            }
            match self.in_memory(&PageKey(Arc::clone(file), page.start)) {
                Some(held) => {
                    self.record_hit();
                    served[at] = Some(held);
                }
                None => missing = true,
            }
        }
        if !missing {
            return Pending {
                runs: Vec::new(),
                waits: Vec::new(),
                joined_fetch: false,
            };
        }
        let mut waits = Vec::new();
        let mut wanted = vec![false; pages.len()];
        {
            let flights = self.spans.shard(file).lock();
            for (at, page) in pages.iter().enumerate() {
                if served[at].is_some() {
                    continue;
                }
                let key = PageKey(Arc::clone(file), page.start);
                if let Some(flight) = flights.get(&key) {
                    self.spans.joined.fetch_add(1, Ordering::Relaxed);
                    waits.push((at, flight.clone()));
                } else if let Some(held) = self.in_memory(&key) {
                    self.record_hit();
                    served[at] = Some(held);
                } else {
                    wanted[at] = true;
                }
            }
        }
        let joined_fetch = self.read_disk(file, pages, &mut wanted, served).await;
        Pending {
            runs: wanted_runs(pages, &wanted),
            waits,
            joined_fetch,
        }
    }

    /// Serves from the disk level every page of `pages` of `file` that
    /// `wanted` marks and the level holds, unmarking it: the pages' reads
    /// run together, each through foyer's hybrid read, so it lands in
    /// memory as a lookup's disk hit does, and concurrent reads of one page
    /// share one local read. Each page is counted as `record` counts a
    /// lookup served from the level, a hit and a local read of its length;
    /// a page that reached memory before its read is a hit alone. A page
    /// the level lacks, or fails to read, stays marked for the fetch, as
    /// `get_or_fetch` moves on to the fetch (RFD 017: the directory is
    /// disposable). A memory-only level has nothing to read.
    /// foyer's read of a page a `get_or_fetch` lookup is fetching waits on
    /// that lookup's flight, and a lookup of a page this read is asking the
    /// level for joins this read's, its fetch run if the level lacks the
    /// page: either way such a page comes from object storage, under the
    /// lookup's one counted fetch. It is counted as a page joined in
    /// flight is, in `span_joins` and neither `hits` nor `fetches`, and
    /// `true` is returned so the read is labelled `PageOrigin::Fetched`.
    async fn read_disk(
        &self,
        file: &Arc<str>,
        pages: &[Range<u64>],
        wanted: &mut [bool],
        served: &mut [Option<Bytes>],
    ) -> bool {
        let (cache, stats) = match self.level.as_ref() {
            Level::Memory(_) => return false,
            Level::Hybrid { cache, stats } => (cache, stats.as_ref()),
        };
        let mut joined_fetch = false;
        let reads = (0..pages.len()).filter(|at| wanted[*at]).map(|at| {
            let key = PageKey(Arc::clone(file), pages[at].start);
            async move { (at, cache.get(&key).await) }
        });
        for (at, read) in futures::future::join_all(reads).await {
            // intentional: a page the level fails to read is fetched
            // instead, since the directory is disposable (RFD 017).
            let Ok(Some(entry)) = read else {
                continue;
            };
            let page = entry.value().0.clone();
            match entry.source() {
                Source::Outer => {
                    self.spans.joined.fetch_add(1, Ordering::Relaxed);
                    joined_fetch = true;
                }
                source @ (Source::Memory | Source::Disk) => {
                    self.record(source, Some(stats), &page);
                }
            }
            wanted[at] = false;
            served[at] = Some(page);
        }
        joined_fetch
    }

    /// Claims as in flight, under the lock of `file`'s shard so no page is
    /// claimed twice, the first stretch of `run` of `pages` of `file` that is
    /// neither in memory nor in flight now; `None` when no page of `run` is.
    /// The pages of `run` before and after that stretch are left to the
    /// read's next pass, which joins a page another read claimed since
    /// `pending` took stock, while this read waited on the disk level or on
    /// its permit. Memory is checked under the lock, and a fetch inserts
    /// its pages before it gives up its claim, so a page leaves the
    /// in-flight set only once memory holds it.
    fn claim_run(
        &self,
        file: &Arc<str>,
        pages: &[Range<u64>],
        run: Range<usize>,
    ) -> Option<Claim<'_>> {
        let shard = self.spans.shard(file);
        let mut flights = shard.lock();
        let free = |at: &usize| {
            let key = PageKey(Arc::clone(file), pages[*at].start);
            !flights.contains_key(&key) && self.in_memory(&key).is_none()
        };
        let start = run.clone().find(free)?;
        let end = (start..run.end).find(|at| !free(at)).unwrap_or(run.end);
        let senders = pages[start..end]
            .iter()
            .map(|page| {
                let (sender, receiver) = watch::channel(None);
                flights.insert(PageKey(Arc::clone(file), page.start), receiver);
                (PageKey(Arc::clone(file), page.start), sender)
            })
            .collect();
        Some(Claim {
            flights: shard,
            run: start..end,
            senders,
        })
    }

    /// Lookups served without running a fetch: from memory, or from the
    /// disk level with a local read. Counted per lookup served, so several
    /// lookups coalesced onto one disk read each count one; a lookup that
    /// joined another's fetch counts under neither `hits` nor `fetches`.
    pub fn hits(&self) -> u64 {
        self.hits.counted()
    }

    /// Moves the hits counted since the last call into
    /// `page_cache_hits_total`. A hit is counted on its thread's shard, not
    /// on the shared series, so a scrape calls this first to read every hit
    /// counted before it; the binary's `/metrics` does, and the last clone
    /// of a cache dropped does it last.
    pub fn publish_hit_counts(&self) {
        self.hits.publish();
    }

    /// Pages a span read found another read fetching, a span read or a
    /// lookup, and waited for rather than fetching again: what the
    /// coalescing tests observe.
    #[cfg(test)]
    pub(crate) fn span_joins(&self) -> u64 {
        self.spans.joined.load(Ordering::Relaxed)
    }

    /// The page of `file` at `offset` when memory holds it, uncounted:
    /// what a test compares a value's bytes against.
    #[cfg(test)]
    pub(crate) fn held_page(&self, file: &Arc<str>, offset: u64) -> Option<Bytes> {
        self.in_memory(&PageKey(Arc::clone(file), offset))
    }

    /// Fetch futures run, after coalescing: concurrent misses on one key run
    /// one and count one.
    pub fn fetches(&self) -> u64 {
        self.fetches.count.load(Ordering::Relaxed)
    }

    /// Sets `page_cache_bytes` to the bytes the memory budget holds now
    /// (`usage`); a scrape calls this, so the gauge reads evictions as well
    /// as inserts.
    pub fn publish_usage(&self) {
        metrics::gauge!(metric::PAGE_CACHE_BYTES).set(approx::gauge_len(self.usage()));
    }

    /// Bytes charged against the memory budget: each page in memory at its
    /// length plus the per-entry overhead. The disk level is not counted.
    pub(crate) fn usage(&self) -> usize {
        match self.level.as_ref() {
            Level::Memory(cache) => cache.usage(),
            Level::Hybrid { cache, .. } => cache.memory().usage(),
        }
    }

    /// Evicts every page memory holds, as eviction does: a memory-only
    /// cache drops it, so the next request for it fetches it again, and a
    /// cache with a disk level writes it there. What a benchmark's cold read
    /// starts from. The counters are unchanged. Eviction, not foyer's
    /// `clear`, which leaves the usage it charged in place.
    pub fn clear(&self) {
        match self.level.as_ref() {
            Level::Memory(cache) => cache.evict_all(),
            Level::Hybrid { cache, .. } => cache.memory().evict_all(),
        }
    }

    /// Waits until the disk level has written every page memory let go of
    /// before the call, by eviction or `clear`; a memory-only cache returns
    /// at once. What a test that reads a page back from the disk level
    /// waits on.
    #[cfg(test)]
    pub(crate) async fn disk_writes_done(&self) {
        if let Level::Hybrid { cache, .. } = self.level.as_ref() {
            cache.storage().wait().await;
        }
    }

    /// Flushes the disk level and closes it, so what memory still held
    /// reaches disk before the process exits; `serve` calls it while
    /// draining (RFD 012). A memory-only cache has nothing to flush and
    /// returns `Ok`. A failure of the level is `CacheError::Backend`.
    pub async fn close(&self) -> Result<()> {
        match self.level.as_ref() {
            Level::Memory(_) => Ok(()),
            Level::Hybrid { cache, .. } => cache.close().await.map_err(backend),
        }
    }

    /// Counts how a page was served. From memory: a hit. From the disk
    /// level, which only `Level::Hybrid` has (`stats` is its counter;
    /// `Level::Memory` is never told `Disk`): a hit and one local read of the
    /// page's length, charged beside GETs so a lookup's whole cost is one
    /// structure (RFD 003). Both are counted per lookup served: foyer
    /// reports the source to every lookup coalesced onto one disk read
    /// alike and names no leader, so several lookups served by one read
    /// each count a hit and a local read, as each would under
    /// `DiskChunks::page`; GETs, by contrast, are counted per fetch run.
    /// From a fetch: nothing here, since `Run::fetch` counted the one run for
    /// every lookup it served.
    fn record(&self, source: Source, stats: Option<&IoStats>, page: &Bytes) {
        match source {
            Source::Outer => {}
            Source::Memory => self.record_hit(),
            Source::Disk => {
                self.record_hit();
                if let Some(stats) = stats {
                    stats.record_local_read(approx::count(page.len()));
                }
            }
        }
    }

    /// The page at `key` when memory holds it, counted by foyer as a hit
    /// of its eviction policy as `get_or_fetch` would count it.
    fn in_memory(&self, key: &PageKey) -> Option<Bytes> {
        match self.level.as_ref() {
            Level::Memory(cache) => cache.get(key).map(|entry| entry.value().0.clone()),
            Level::Hybrid { cache, .. } => {
                cache.memory().get(key).map(|entry| entry.value().0.clone())
            }
        }
    }

    /// Holds `page` at `key` in memory, as a fetch's page is held.
    fn insert(&self, key: PageKey, page: Bytes) {
        match self.level.as_ref() {
            Level::Memory(cache) => {
                cache.insert(key, PageValue(page));
            }
            Level::Hybrid { cache, .. } => {
                cache.insert(key, PageValue(page));
            }
        }
    }

    /// One page served without a fetch.
    fn record_hit(&self) {
        self.hits.increment();
    }

    /// What a fetch for `page` runs with through this handle: its permit
    /// `permit`, taken of `issue`, and `issue`'s counter.
    fn run_for(
        &self,
        page: PageKey,
        permit: Option<OwnedSemaphorePermit>,
        issue: Issue<'_>,
    ) -> Run {
        Run {
            page,
            fetches: Arc::clone(&self.fetches),
            deadline: self.fetch_deadline,
            permit,
            counted: issue.counted.cloned(),
        }
    }
}

/// The positions in `pages` of every maximal run of pages `wanted` marks
/// whose ranges meet end to start, in order.
fn wanted_runs(pages: &[Range<u64>], wanted: &[bool]) -> Vec<Range<usize>> {
    let mut runs: Vec<Range<usize>> = Vec::new();
    for (at, page) in pages.iter().enumerate() {
        if !wanted[at] {
            continue;
        }
        match runs.last_mut() {
            Some(run) if run.end == at && pages[at - 1].end == page.start => run.end = at + 1,
            Some(_) | None => runs.push(at..at + 1),
        }
    }
    runs
}

/// What one pass of `PageCache::get_span` takes on: the runs of pages, by
/// position, it is to fetch, and the pages, by position, whose fetch by
/// another read it waits for; and whether a page it served already came
/// from a lookup's fetch it joined on the way to the disk level
/// (`read_disk`).
struct Pending {
    runs: Vec<Range<usize>>,
    waits: Vec<(usize, watch::Receiver<Option<SpanOutcome>>)>,
    joined_fetch: bool,
}

/// A run of pages one span read claimed as in flight: the run's positions
/// in the read's pages, and per page its key and the sender its waiters
/// listen on. Dropped, it takes its pages out of the in-flight set, so a
/// read dropped before its fetch ends leaves its waiters to claim the
/// pages again, their senders gone with no outcome.
struct Claim<'a> {
    flights: &'a FlightShard,
    run: Range<usize>,
    senders: Vec<(PageKey, watch::Sender<Option<SpanOutcome>>)>,
}

impl Claim<'_> {
    /// Hands `outcome`, the run's pages split or the run's failure, to
    /// every read waiting on one of its pages.
    fn publish(&self, outcome: &std::result::Result<Vec<Bytes>, FetchFailed>) {
        for (at, (_, sender)) in self.senders.iter().enumerate() {
            let page = match outcome {
                Ok(split) => Ok(split[at].clone()),
                Err(failed) => Err(failed.clone()),
            };
            sender.send_replace(Some(page));
        }
    }
}

impl Drop for Claim<'_> {
    fn drop(&mut self) {
        let mut flights = self.flights.lock();
        for (key, _) in &self.senders {
            flights.remove(key);
        }
    }
}

/// `bytes`, the answer of a span fetch of `pages` of `file`, split into
/// one copy per page, each in an allocation of its own so an entry keeps
/// only its page alive; bytes shorter than the span are a parquet
/// end-of-file failure of the fetch.
fn split_span(
    file: &Arc<str>,
    pages: &[Range<u64>],
    bytes: &Bytes,
) -> std::result::Result<Vec<Bytes>, FetchFailed> {
    let base = pages[0].start;
    pages
        .iter()
        .map(|page| {
            let start = approx::length(page.start - base);
            let end = approx::length(page.end - base);
            bytes.get(start..end).map(Bytes::copy_from_slice).ok_or_else(|| {
                FetchFailed::Failed(Arc::new(Error::Parquet(
                    parquet::errors::ParquetError::EOF(format!(
                        "a span fetch of {file} from offset {base} returned {} bytes, short of page {}..{}",
                        bytes.len(),
                        page.start,
                        page.end
                    )),
                )))
            })
        })
        .collect()
}

/// A cache's fetch runs, counted in `count` for `PageCache::fetches` and in
/// `page_cache_fetches_total` through a handle registered once in
/// `PageCache::new`, so the series reads 0 from then on rather than
/// appearing at the first fetch.
#[derive(Debug)]
struct Fetches {
    count: AtomicU64,
    total: Counter,
}

impl Fetches {
    fn new() -> Self {
        Self {
            count: AtomicU64::new(0),
            total: metrics::counter!(metric::PAGE_CACHE_FETCHES_TOTAL),
        }
    }

    /// One fetch run.
    fn record(&self) {
        self.count.fetch_add(1, Ordering::Relaxed);
        self.total.increment(1);
    }
}

/// A memory level of `capacity_bytes`, split over `memory_shards()`, each
/// page weighed as its bytes plus the entry overhead and evicted by
/// `eviction()`.
fn memory_level(capacity_bytes: usize) -> Level {
    Level::Memory(
        CacheBuilder::new(capacity_bytes)
            .with_name(CACHE_NAME)
            .with_shards(memory_shards())
            .with_weighter(weigh)
            .with_eviction_config(eviction())
            .build(),
    )
}

/// The hybrid cache over `disk`: memory of `capacity_bytes` evicted by
/// `eviction()`, and a block engine on a file device in `disk.dir()`
/// bounded by `disk.capacity_bytes()`, written on eviction (RFD 017: a
/// page leaves memory for disk, never for nowhere), and recovered from
/// whatever the directory holds before the first request is served.
/// Recovery is quiet: a block that does not decode is skipped, since
/// everything here is rebuilt from object storage.
/// The block is `PAGE_CACHE_DISK_BLOCK_BYTES`, and `disk.capacity_bytes()`
/// holds at least one (`Config::parse` checks it), so the engine has a
/// block to write into.
async fn open_hybrid(
    capacity_bytes: usize,
    disk: &DiskLevel,
) -> foyer::Result<HybridCache<PageKey, PageValue>> {
    let device = FsDeviceBuilder::new(disk.dir())
        .with_capacity(approx::length(disk.capacity_bytes()))
        .build()?;
    HybridCacheBuilder::new()
        .with_name(CACHE_NAME)
        .with_policy(HybridCachePolicy::WriteOnEviction)
        .memory(capacity_bytes)
        .with_shards(memory_shards())
        .with_weighter(weigh)
        .with_eviction_config(eviction())
        .storage()
        .with_engine_config(
            BlockEngineConfig::new(device)
                .with_block_size(approx::length(PAGE_CACHE_DISK_BLOCK_BYTES)),
        )
        .with_recover_mode(RecoverMode::Quiet)
        .build()
        .await
}

/// A failure to open `disk` as the crate's error: running out of
/// descriptors is `CacheError::DescriptorLimit` with the limit and the
/// estimate for the level, anything else the backend's error.
fn open_error(error: foyer::Error, disk: &DiskLevel) -> Error {
    match descriptors_exhausted(&error) {
        Some(source) => Error::Cache(CacheError::DescriptorLimit {
            limit: descriptors::limit(),
            needed: descriptors::needed(disk.capacity_bytes(), 0),
            source,
        }),
        None => backend(error),
    }
}

/// The operating system's error anywhere in `error`'s chain when it is
/// `EMFILE` or `ENFILE`, too many open files in the process or in the
/// system.
fn descriptors_exhausted(error: &foyer::Error) -> Option<io::Error> {
    let mut cause: Option<&(dyn std::error::Error + 'static)> = Some(error);
    while let Some(current) = cause {
        if let Some(code) = current
            .downcast_ref::<io::Error>()
            .and_then(io::Error::raw_os_error)
            .filter(|code| matches!(*code, libc::EMFILE | libc::ENFILE))
        {
            return Some(io::Error::from_raw_os_error(code));
        }
        cause = current.source();
    }
    None
}

/// What an entry costs the memory budget.
fn weigh(_: &PageKey, value: &PageValue) -> usize {
    value.0.len() + ENTRY_OVERHEAD_BYTES
}

/// What a GET of a lookup or a Scan answers to beside the fetch deadline
/// of the handle it goes through (RFD 013, RFD 022). A page served from
/// memory takes nothing of it; any other page takes the permit on the
/// reader's own task before the page cache is asked for it, and the GET
/// counts as it is issued.
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct Issue<'a> {
    /// The fan-out of the call the read is part of
    /// (`TableHandles::batch_fan_out`), a `multi_get`, `scan`, or
    /// `multi_scan`: one of its permits is taken before the page cache is
    /// asked, so a read still waiting for one has nothing queued there for
    /// another call to join, and the wait is not charged against the
    /// deadline. The permit moves into the fetch and is held until the GET
    /// ends, on the task the GET runs on, so a read dropped first frees
    /// nothing while its GET runs on; its hedge runs under the same permit.
    /// Through `PageCache::get_or_fetch`, a page the disk level serves, or
    /// one another caller fetched meanwhile, drops it unused, and so does a
    /// read that joins a fetch in flight, save the first to join: foyer
    /// keeps that one's fetch beside the running one until it ends, and the
    /// permit with it. That permit narrows the batch's GETs in flight and
    /// cannot stall them, since the fetch it waits on took its own before it
    /// started. `PageCache::get_span` takes a permit per run of pages. `None`
    /// for a `get`, one call of its own.
    pub(crate) fan_out: Option<&'a Arc<Semaphore>>,
    /// Counted once as the GET is issued, its hedge not: a value or order
    /// column's `value_page_gets_total` for the column's value or dictionary
    /// page, `None` for a key page.
    pub(crate) counted: Option<&'a Counter>,
}

impl Issue<'_> {
    /// Waits for one permit of the fan-out when there is one.
    async fn permit(self) -> Option<OwnedSemaphorePermit> {
        match self.fan_out {
            Some(fan_out) => Some(
                Arc::clone(fan_out)
                    .acquire_owned()
                    .await
                    .expect("a batch's fan-out semaphore is never closed"),
            ),
            None => None,
        }
    }

    /// Waits for the GET's permit when it has a fan-out, then counts the
    /// GET: a GET issued without the page cache. The GET holds the
    /// returned permit while it runs.
    pub(crate) async fn admit(self) -> Option<OwnedSemaphorePermit> {
        let permit = self.permit().await;
        if let Some(counted) = self.counted {
            counted.increment(1);
        }
        permit
    }
}

/// What one caller's fetch runs with, should foyer run it: the page it is
/// for, the counter it counts in, the deadline of the handle it came
/// through, the permit its issue took, and the counter its GET counts in.
struct Run {
    page: PageKey,
    fetches: Arc<Fetches>,
    deadline: Option<Duration>,
    permit: Option<OwnedSemaphorePermit>,
    counted: Option<Counter>,
}

impl Run {
    /// Runs a caller's fetch once foyer has decided the page must come from
    /// outside: counted as it starts, in `fetches` and in its issue's
    /// counter, so that a fetch that fails still counts. With a deadline,
    /// `fut` is dropped when it passes, measured from the start, and the
    /// run fails as `FetchFailed::Deadline`. The permit is held until the
    /// run ends either way.
    ///
    /// A plain `fn` returning `run`'s future: foyer stores this future
    /// three times per page fetched, and an `async fn` around `run` would
    /// hold a further copy of `fut` in its own state.
    fn fetch<Fut>(
        self,
        fut: Fut,
    ) -> impl Future<Output = std::result::Result<PageValue, FetchFailed>>
    where
        Fut: Future<Output = Result<Bytes>>,
    {
        // Copied into an allocation of its own: the store can hand back a
        // slice of a larger buffer, such as the HTTP connection's read
        // buffer, which the entry would keep alive while `weigh` charges
        // only the slice.
        self.run(fut, |page| PageValue(Bytes::copy_from_slice(&page)))
    }

    /// `fetch`'s run, the bytes as `fut` returned them passed through
    /// `finish`: counted, bounded by the deadline, and holding the permit
    /// until it ends.
    async fn run<Fut, T>(
        self,
        fut: Fut,
        finish: impl FnOnce(Bytes) -> T,
    ) -> std::result::Result<T, FetchFailed>
    where
        Fut: Future<Output = Result<Bytes>>,
    {
        let Self {
            page,
            fetches,
            deadline,
            permit: _permit,
            counted,
        } = self;
        fetches.record();
        if let Some(counted) = counted {
            counted.increment(1);
        }
        let fetched = match deadline {
            None => fut.await,
            Some(after) => tokio::time::timeout(after, fut)
                .await
                .map_err(|_elapsed| FetchFailed::Deadline { page, after })?,
        };
        fetched
            .map(finish)
            .map_err(|error| FetchFailed::Failed(Arc::new(error)))
    }
}

/// A foyer failure as the crate's error: the caller's own fetch failure or
/// its deadline when that is what foyer carries, otherwise the backend's.
fn cache_error(error: foyer::Error) -> Error {
    match error.downcast_ref::<FetchFailed>() {
        Some(failed) => fetch_failed(failed.clone()),
        None => backend(error),
    }
}

/// A fetch the cache ran that produced no page, as the crate's error.
fn fetch_failed(failed: FetchFailed) -> Error {
    match failed {
        FetchFailed::Failed(failed) => Error::Cache(CacheError::Fetch(failed)),
        FetchFailed::Deadline { page, after } => Error::Cache(CacheError::FetchDeadline {
            path: page.0.to_string(),
            offset: page.1,
            after,
        }),
    }
}

/// A failure of the cache itself as the crate's error.
fn backend(error: foyer::Error) -> Error {
    Error::Cache(CacheError::Backend(error))
}

#[cfg(test)]
mod tests {
    use std::fs::{self, OpenOptions};
    use std::os::unix::fs::FileExt;
    use std::path::Path;
    use std::sync::atomic::AtomicBool;

    use futures::future::{join, join_all};
    use metrics_util::debugging::{DebugValue, DebuggingRecorder};
    use tokio::sync::Notify;

    use super::*;
    use crate::approx::count;
    use crate::config::LocalConfig;
    use crate::generation::testing::TABLE;
    use crate::local::LocalDir;

    /// A memory budget with room for every page a test inserts.
    const ROOMY_BYTES: usize = 1024 * 1024;
    /// A memory budget below one page's weight: an insert evicts whatever
    /// its shard held, so a page leaves memory as soon as the next page
    /// lands in its shard.
    const TINY_BYTES: usize = 1;
    /// A disk level of sixteen blocks: room for every page the disk tests
    /// evict into it, so a page that left memory is on disk when read back.
    const DISK_BYTES: u64 = 16 * PAGE_CACHE_DISK_BLOCK_BYTES;
    /// Callers racing for one page: enough that coalescing is exercised and
    /// a fetch run per caller would show in the count.
    const CONCURRENT: usize = 8;
    /// The file every test page is keyed under; the cache never reads it,
    /// so any location serves.
    const FILE_ID: &str = "mem://w/db/t/data/f0.parquet";
    /// The page offset the first test page is keyed by; a key never reaches
    /// the file, so the value is arbitrary.
    const FIRST_OFFSET: u64 = 4;
    /// A second offset in the same file, so two pages share a file id and
    /// differ in offset alone.
    const SECOND_OFFSET: u64 = 4_096;
    /// The bytes the first test page holds; served pages are compared to it.
    const PAGE: &[u8] = b"page-bytes";
    /// The second test page's bytes, a different length from `PAGE`.
    const OTHER_PAGE: &[u8] = b"other-page-bytes";
    /// The fetch deadline the deadline test bounds its handle by: short, so
    /// the stalled fetch ends quickly.
    const FETCH_DEADLINE: Duration = Duration::from_millis(50);
    /// How long the deadline test lets its callers wait before it fails:
    /// far past `FETCH_DEADLINE`, so only a fetch that runs on reaches it.
    const FETCH_GUARD: Duration = Duration::from_secs(10);
    /// Offsets tried to find one whose key shares a shard with an anchor's;
    /// with one shard per worker, the first few dozen suffice.
    const SHARD_SEARCH_OFFSETS: u64 = 1024;
    /// Pages of `PAGE`'s weight one memory shard holds in the hit-again
    /// test: enough that the shard's small queue, a tenth of it, holds
    /// more than one, so a page read once leaves from there and not from
    /// the main queue.
    const SHARD_PAGES: usize = 20;
    /// Bytes cut from the end of an encoded key so that its file id ends
    /// one byte short of its length prefix.
    const ONE_ID_BYTE_SHORT: usize = OFFSET_BYTES + 1;

    /// The bytes a padded fetch future carries: enough that one copy of it
    /// outweighs every other field of the futures that hold it.
    const PADDED_FETCH_BYTES: usize = 4_096;
    /// The copies of the caller's fetch future that `Run::fetch`'s future
    /// holds: the one `run` is given and the one it awaits under the
    /// deadline.
    const FETCH_COPIES: usize = 2;

    fn key(offset: u64) -> (Arc<str>, u64) {
        (Arc::from(FILE_ID), offset)
    }

    fn page_bytes(page: &[u8]) -> u64 {
        count(page.len())
    }

    /// A fetch whose future holds `PADDED_FETCH_BYTES` bytes, so its size
    /// shows in the size of a future that holds it.
    fn padded() -> impl Future<Output = Result<Bytes>> + Send + 'static {
        let pad = [0_u8; PADDED_FETCH_BYTES];
        async move { Ok(Bytes::copy_from_slice(&pad)) }
    }

    /// A run with a deadline, its fetch never started.
    fn deadline_run() -> Run {
        Run {
            page: PageKey(Arc::from(FILE_ID), FIRST_OFFSET),
            fetches: Arc::new(Fetches::new()),
            deadline: Some(FETCH_DEADLINE),
            permit: None,
            counted: None,
        }
    }

    /// A fetch whose future counts its runs in `runs` and answers `page`.
    fn answering(
        page: &'static [u8],
        runs: &Arc<AtomicU64>,
    ) -> impl Future<Output = Result<Bytes>> + Send + 'static {
        let runs = Arc::clone(runs);
        async move {
            runs.fetch_add(1, Ordering::Relaxed);
            Ok(Bytes::from_static(page))
        }
    }

    /// A fetch whose future counts its run, waits for `gate`, then answers
    /// `page`; concurrent callers all enqueue before it answers.
    fn gated(
        page: &'static [u8],
        runs: &Arc<AtomicU64>,
        gate: &Arc<Notify>,
    ) -> impl Future<Output = Result<Bytes>> + Send + 'static {
        let runs = Arc::clone(runs);
        let gate = Arc::clone(gate);
        async move {
            runs.fetch_add(1, Ordering::Relaxed);
            gate.notified().await;
            Ok(Bytes::from_static(page))
        }
    }

    /// A fetch whose future counts its run, waits for `gate`, then fails
    /// with a typed error.
    fn failing(
        runs: &Arc<AtomicU64>,
        gate: &Arc<Notify>,
    ) -> impl Future<Output = Result<Bytes>> + Send + 'static {
        let runs = Arc::clone(runs);
        let gate = Arc::clone(gate);
        async move {
            runs.fetch_add(1, Ordering::Relaxed);
            gate.notified().await;
            Err(Error::NotReady {
                table: TABLE.to_string(),
            })
        }
    }

    /// A fetch whose future counts its run and never answers, as a GET to a
    /// store that has stopped answering; `dropped` is set when the future is
    /// dropped.
    fn stalled(
        runs: &Arc<AtomicU64>,
        dropped: &Arc<AtomicBool>,
    ) -> impl Future<Output = Result<Bytes>> + Send + 'static {
        let runs = Arc::clone(runs);
        let probe = DropProbe(Arc::clone(dropped));
        async move {
            let _probe = probe;
            runs.fetch_add(1, Ordering::Relaxed);
            std::future::pending().await
        }
    }

    /// Sets its flag when dropped.
    struct DropProbe(Arc<AtomicBool>);

    impl Drop for DropProbe {
        fn drop(&mut self) {
            self.0.store(true, Ordering::Relaxed);
        }
    }

    /// A fetch that must not run: a page the cache holds is served without
    /// it, and a run fails the test.
    async fn never() -> Result<Bytes> {
        panic!("fetch ran for a page the cache holds")
    }

    /// A disk level over `dir` with a fresh counter, as `LocalDir` hands it out.
    fn disk_level(dir: &Path) -> (DiskLevel, Arc<IoStats>) {
        let stats = Arc::new(IoStats::default());
        let config = LocalConfig {
            dir: dir.to_path_buf(),
            index_budget_bytes: None,
            page_cache_disk_bytes: DISK_BYTES,
        };
        let local = LocalDir::open(&config, Arc::clone(&stats)).unwrap();
        (local.page_cache_level().unwrap(), stats)
    }

    /// The hybrid cache behind a `PageCache` built with a disk level.
    fn hybrid(cache: &PageCache) -> &HybridCache<PageKey, PageValue> {
        let Level::Hybrid { cache, .. } = cache.level.as_ref() else {
            panic!("a cache with a disk level is hybrid");
        };
        cache
    }

    /// The first offset after `anchor`'s whose key `shard_of` places in the
    /// same memory shard as `anchor`. Eviction is per shard, so a page is
    /// displaced by a page of its own shard and by nothing else.
    fn offset_sharing_shard(shard_of: impl Fn(&PageKey) -> u64, anchor: &PageKey) -> u64 {
        (anchor.1 + 1..anchor.1 + 1 + SHARD_SEARCH_OFFSETS)
            .find(|offset| shard_of(&PageKey(Arc::clone(&anchor.0), *offset)) == shard_of(anchor))
            .unwrap()
    }

    /// Runs `CONCURRENT` callers for one key against `cache`, each with its
    /// own fetch from `fetch`, all enqueued before `gate` opens.
    async fn race<Fut>(
        cache: &PageCache,
        offset: u64,
        gate: &Arc<Notify>,
        fetch: impl Fn() -> Fut,
    ) -> Vec<Result<(Bytes, PageOrigin)>>
    where
        Fut: Future<Output = Result<Bytes>> + Send + 'static,
    {
        let callers = (0..CONCURRENT).map(|_| {
            let fut = fetch();
            async move {
                cache
                    .get_or_fetch(key(offset), Issue::default(), move || fut)
                    .await
            }
        });
        let (answers, ()) = join(join_all(callers), async { gate.notify_one() }).await;
        answers
    }

    /// Puts `PAGE` at `FIRST_OFFSET` through `cache`, displaces it from
    /// memory with `OTHER_PAGE` at an offset of the same shard, and waits
    /// until the level has written it; two fetches run. Deterministic on
    /// the current-thread test runtime: the eviction reaches the level's
    /// queue inside foyer's fetch task before this task resumes, so the
    /// wait is queued after it. A multi-thread runtime would race the two.
    async fn evict_to_disk(cache: &PageCache, runs: &Arc<AtomicU64>) {
        cache
            .get_or_fetch(key(FIRST_OFFSET), Issue::default(), || {
                answering(PAGE, runs)
            })
            .await
            .unwrap();
        let memory = hybrid(cache).memory();
        let shards = count(memory.shards());
        let displacing = offset_sharing_shard(
            |key| memory.hash(key) % shards,
            &PageKey(Arc::from(FILE_ID), FIRST_OFFSET),
        );
        cache
            .get_or_fetch(key(displacing), Issue::default(), || {
                answering(OTHER_PAGE, runs)
            })
            .await
            .unwrap();
        cache.disk_writes_done().await;
    }

    /// Overwrites every copy of `page` in the level's files under
    /// `cache_dir` with its bitwise complement, so the entry's checksum no
    /// longer matches what its block holds; returns how many were
    /// overwritten. Each copy is written in place: foyer sizes a partition
    /// file to the level's capacity as a sparse file, and rewriting the
    /// whole file would make every hole real and fill the temp volume when
    /// tests run side by side.
    fn corrupt_on_disk(cache_dir: &Path, page: &[u8]) -> usize {
        let mut copies = 0;
        for entry in fs::read_dir(cache_dir).unwrap() {
            let path = entry.unwrap().path();
            let bytes = fs::read(&path).unwrap();
            let file = OpenOptions::new().write(true).open(&path).unwrap();
            let flipped: Vec<u8> = page.iter().map(|byte| !byte).collect();
            let mut from = 0;
            while let Some(found) = bytes[from..]
                .windows(page.len())
                .position(|window| window == page)
            {
                let at = from + found;
                file.write_all_at(&flipped, count(at))
                    .unwrap_or_else(|error| panic!("writing {}: {error}", path.display()));
                copies += 1;
                from = at + page.len();
            }
        }
        copies
    }

    #[test]
    fn test_encode_page_key_round_trips() {
        let cases = [
            PageKey(Arc::from(""), 0),
            PageKey(Arc::from(FILE_ID), SECOND_OFFSET),
            PageKey(Arc::from(FILE_ID), u64::MAX),
        ];
        for key in cases {
            let mut encoded = Vec::new();
            key.encode(&mut encoded).unwrap();
            assert_eq!(encoded.len(), key.estimated_size());
            assert_eq!(PageKey::decode(&mut encoded.as_slice()).unwrap(), key);
        }
    }

    #[test]
    fn test_decode_page_key_refuses_a_file_id_shorter_than_its_prefix() {
        let key = PageKey(Arc::from(FILE_ID), SECOND_OFFSET);
        let mut encoded = Vec::new();
        key.encode(&mut encoded).unwrap();
        encoded.truncate(encoded.len() - ONE_ID_BYTE_SHORT);
        let error = PageKey::decode(&mut encoded.as_slice()).unwrap_err();
        assert_eq!(error.kind(), ErrorKind::Io);
    }

    #[test]
    fn test_encode_page_value_is_the_raw_bytes() {
        let block = vec![0; approx::length(PAGE_CACHE_DISK_BLOCK_BYTES)];
        let cases: [&[u8]; 3] = [PAGE, b"", &block];
        for page in cases {
            let value = PageValue(Bytes::copy_from_slice(page));
            let mut encoded = Vec::new();
            value.encode(&mut encoded).unwrap();
            assert_eq!(encoded, page);
            assert_eq!(encoded.len(), value.estimated_size());
            assert_eq!(PageValue::decode(&mut encoded.as_slice()).unwrap().0, page);
        }
    }

    /// The minimum is one value page per shard, and a cache opened on a
    /// runtime has one shard per worker, rounded up to a power of two.
    #[test]
    fn test_check_capacity_refuses_less_than_a_page_per_shard() {
        let workers: usize = 3;
        let shards = workers.next_power_of_two();
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(workers)
            .build()
            .unwrap();
        assert_eq!(runtime.block_on(async { memory_shards() }), shards);
        let minimum = min_memory_bytes(shards);
        assert_eq!(
            check_capacity(minimum - 1, shards, None),
            Err(ConfigViolation::PageCacheTooSmall {
                bytes: minimum - 1,
                minimum,
                shards,
                limit: None,
            })
        );
        assert_eq!(check_capacity(minimum, shards, None), Ok(()));
        assert_eq!(check_capacity(minimum - 1, 1, None), Ok(()));
    }

    /// A cache's counters are registered at zero when it opens, before
    /// any lookup, and a fetch counts in `page_cache_fetches_total` as it
    /// runs; `publish_usage` sets `page_cache_bytes` to what memory holds.
    #[test]
    fn test_new_registers_the_counters_and_publish_usage_sets_the_bytes() {
        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let value = |name: &str| {
            snapshotter
                .snapshot()
                .into_vec()
                .into_iter()
                .find(|(key, ..)| key.key().name() == name)
                .map(|(.., value)| value)
        };
        metrics::with_local_recorder(&recorder, || {
            runtime.block_on(async {
                let cache = PageCache::new(ROOMY_BYTES, None).await.unwrap();
                for name in [
                    metric::PAGE_CACHE_HITS_TOTAL,
                    metric::PAGE_CACHE_FETCHES_TOTAL,
                ] {
                    assert_eq!(value(name), Some(DebugValue::Counter(0)), "{name}");
                }
                assert_eq!(value(metric::PAGE_CACHE_BYTES), None, "set by a scrape");
                let runs = Arc::new(AtomicU64::new(0));
                cache
                    .get_or_fetch(key(FIRST_OFFSET), Issue::default(), || {
                        answering(PAGE, &runs)
                    })
                    .await
                    .unwrap();
                assert_eq!(
                    value(metric::PAGE_CACHE_FETCHES_TOTAL),
                    Some(DebugValue::Counter(1))
                );
                cache.publish_usage();
                let Some(DebugValue::Gauge(bytes)) = value(metric::PAGE_CACHE_BYTES) else {
                    panic!("page_cache_bytes is a gauge");
                };
                assert_eq!(
                    bytes.into_inner().to_bits(),
                    approx::gauge_len(cache.usage()).to_bits()
                );
                assert!(cache.usage() > 0, "the fetched page is held");
            });
        });
    }

    /// The pages the span coalescing tests read: two that meet.
    const SPAN: [Range<u64>; 2] = [0..10, 10..20];

    /// The bytes of the test file the span tests read pages of.
    fn span_file() -> Bytes {
        (0..=u8::MAX).collect()
    }

    /// The bytes `span` covers of `file`, as a store answers a span GET.
    fn span_bytes(file: &Bytes, span: &Range<u64>) -> Bytes {
        file.slice(approx::length(span.start)..approx::length(span.end))
    }

    /// Each page of `pages` as `span_bytes` of `file` answers it.
    fn pages_of(file: &Bytes, pages: &[Range<u64>]) -> Vec<Bytes> {
        pages.iter().map(|page| span_bytes(file, page)).collect()
    }

    /// Waits until span reads of `cache` have joined `joins` pages in
    /// flight, failing past `DEADLINE_GUARD`.
    async fn joined(cache: &PageCache, joins: u64) {
        tokio::time::timeout(crate::storage::testing::DEADLINE_GUARD, async {
            while cache.span_joins() < joins {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("a read joins the pages another read is fetching");
    }

    /// A span read that needs pages another span read is fetching waits on
    /// that fetch and runs none of its own (RFD 025, Caching and hedging):
    /// one fetch serves both reads.
    #[tokio::test]
    async fn test_get_span_joins_the_pages_another_read_is_fetching() {
        let cache = PageCache::new(ROOMY_BYTES, None).await.unwrap();
        let id: Arc<str> = Arc::from(FILE_ID);
        let file = span_file();
        let gate = Notify::new();
        let slow = |span: Range<u64>| {
            let bytes = span_bytes(&file, &span);
            let gate = &gate;
            async move {
                gate.notified().await;
                Ok(bytes)
            }
        };
        let (first, second, ()) = tokio::join!(
            cache.get_span(&id, &SPAN, Issue::default(), slow),
            cache.get_span(&id, &SPAN, Issue::default(), |_| never()),
            async {
                joined(&cache, count(SPAN.len())).await;
                gate.notify_one();
            }
        );
        let (first, second) = (first.unwrap(), second.unwrap());
        assert_eq!(first, second);
        assert_eq!(
            second.1,
            PageOrigin::Fetched,
            "a joined fetch is from outside"
        );
        assert_eq!(cache.fetches(), 1, "one fetch between the two reads");
        assert_eq!(cache.hits(), 0);
    }

    /// A read waiting on pages whose fetching read is dropped unfinished
    /// claims them and fetches them itself.
    #[tokio::test]
    async fn test_get_span_claims_the_pages_of_a_read_dropped_while_fetching() {
        let cache = PageCache::new(ROOMY_BYTES, None).await.unwrap();
        let id: Arc<str> = Arc::from(FILE_ID);
        let file = span_file();
        let mut dropped = Box::pin(cache.get_span(&id, &SPAN, Issue::default(), |_| {
            std::future::pending::<Result<Bytes>>()
        }));
        assert!(
            futures::poll!(dropped.as_mut()).is_pending(),
            "it claims the pages"
        );
        let answering = |span: Range<u64>| std::future::ready(Ok(span_bytes(&file, &span)));
        let mut waiting = Box::pin(cache.get_span(&id, &SPAN, Issue::default(), answering));
        assert!(
            futures::poll!(waiting.as_mut()).is_pending(),
            "it waits on them"
        );
        assert_eq!(cache.span_joins(), count(SPAN.len()));
        drop(dropped);
        let (served, origin) = waiting.await.unwrap();
        assert_eq!(served, pages_of(&file, &SPAN));
        assert_eq!(origin, PageOrigin::Fetched);
        assert_eq!(
            cache.fetches(),
            2,
            "the dropped read's run, and the waiter's own"
        );
    }

    /// A read reads `SPAN` of `file` through `cache` with a fetch that
    /// answers every span whole, and returns what it served; the pages a
    /// failure left uncached are fetched again.
    async fn read_again(cache: &PageCache, id: &Arc<str>, file: &Bytes) -> Vec<Bytes> {
        let fetches = cache.fetches();
        let (served, origin) = cache
            .get_span(id, &SPAN, Issue::default(), |span| {
                std::future::ready(Ok(span_bytes(file, &span)))
            })
            .await
            .unwrap();
        assert_eq!(origin, PageOrigin::Fetched);
        assert_eq!(
            cache.fetches() - fetches,
            1,
            "nothing was cached, so the next read fetches the span again"
        );
        served
    }

    /// A failed run's error reaches a read that joined its pages, the one
    /// `Arc<Error>` both receive, and caches none of them.
    #[tokio::test]
    async fn test_get_span_fetch_error_reaches_a_joined_read_and_caches_nothing() {
        let cache = PageCache::new(ROOMY_BYTES, None).await.unwrap();
        let id: Arc<str> = Arc::from(FILE_ID);
        let file = span_file();
        let runs = Arc::new(AtomicU64::new(0));
        let gate = Arc::new(Notify::new());
        let (claimer, joiner, ()) = tokio::join!(
            cache.get_span(&id, &SPAN, Issue::default(), |_| failing(&runs, &gate)),
            cache.get_span(&id, &SPAN, Issue::default(), |_| never()),
            async {
                joined(&cache, count(SPAN.len())).await;
                gate.notify_one();
            }
        );
        let Err(Error::Cache(CacheError::Fetch(failure))) = claimer else {
            panic!("the claimer receives its fetch's failure: {claimer:?}");
        };
        let Err(Error::Cache(CacheError::Fetch(shared))) = joiner else {
            panic!("the joined read receives the claimer's failure: {joiner:?}");
        };
        assert!(matches!(*failure, Error::NotReady { .. }));
        assert!(Arc::ptr_eq(&failure, &shared), "one failure for both");
        assert_eq!(runs.load(Ordering::Relaxed), 1);
        assert_eq!(cache.fetches(), 1);
        assert_eq!(read_again(&cache, &id, &file).await, pages_of(&file, &SPAN));
    }

    /// A run that outlasts its handle's fetch deadline is dropped at it, and
    /// a read that joined its pages receives the deadline at the run's first
    /// offset as the claimer does; nothing is cached.
    #[tokio::test]
    async fn test_get_span_ends_a_run_at_its_deadline_for_a_joined_read() {
        let cache = PageCache::new(ROOMY_BYTES, None)
            .await
            .unwrap()
            .with_fetch_deadline(FETCH_DEADLINE);
        let id: Arc<str> = Arc::from(FILE_ID);
        let file = span_file();
        let runs = Arc::new(AtomicU64::new(0));
        let dropped = Arc::new(AtomicBool::new(false));
        let (claimer, joiner) = tokio::time::timeout(FETCH_GUARD, async {
            tokio::join!(
                cache.get_span(&id, &SPAN, Issue::default(), |_| stalled(&runs, &dropped)),
                cache.get_span(&id, &SPAN, Issue::default(), |_| never()),
            )
        })
        .await
        .expect("the run ends at its deadline, before the guard");
        assert_eq!(
            cache.span_joins(),
            count(SPAN.len()),
            "the second read joined"
        );
        for answer in [claimer, joiner] {
            let Err(Error::Cache(CacheError::FetchDeadline {
                path,
                offset,
                after,
            })) = answer
            else {
                panic!("both reads receive the deadline: {answer:?}");
            };
            assert_eq!((path.as_str(), offset), (FILE_ID, SPAN[0].start));
            assert_eq!(after, FETCH_DEADLINE);
        }
        assert!(
            dropped.load(Ordering::Relaxed),
            "the run's fetch was dropped"
        );
        assert_eq!(runs.load(Ordering::Relaxed), 1);
        assert_eq!(cache.fetches(), 1);
        assert_eq!(read_again(&cache, &id, &file).await, pages_of(&file, &SPAN));
    }

    /// A run answered with fewer bytes than its range fails as a parquet
    /// end-of-file error and caches none of its pages, the ones its bytes
    /// cover included.
    #[tokio::test]
    async fn test_get_span_of_a_run_answered_short_fails_and_caches_nothing() {
        let short_by = 5;
        let cache = PageCache::new(ROOMY_BYTES, None).await.unwrap();
        let id: Arc<str> = Arc::from(FILE_ID);
        let file = span_file();
        let short = |span: Range<u64>| {
            let whole = span_bytes(&file, &span);
            std::future::ready(Ok(whole.slice(..whole.len() - short_by)))
        };
        let answer = cache.get_span(&id, &SPAN, Issue::default(), short).await;
        let Err(Error::Cache(CacheError::Fetch(error))) = answer else {
            panic!("a short run is a fetch failure: {answer:?}");
        };
        assert!(matches!(
            *error,
            Error::Parquet(parquet::errors::ParquetError::EOF(_))
        ));
        assert_eq!(cache.fetches(), 1);
        assert_eq!(read_again(&cache, &id, &file).await, pages_of(&file, &SPAN));
    }

    /// A read whose own run fails returns its failure while a page it
    /// joined is still being fetched by another read, rather than waiting
    /// for that fetch to end.
    #[tokio::test]
    async fn test_get_span_returns_a_failed_run_without_waiting_on_its_joins() {
        let cache = PageCache::new(ROOMY_BYTES, None).await.unwrap();
        let id: Arc<str> = Arc::from(FILE_ID);
        let mut fetching = Box::pin(cache.get_span(&id, &SPAN[1..], Issue::default(), |_| {
            std::future::pending::<Result<Bytes>>()
        }));
        assert!(
            futures::poll!(fetching.as_mut()).is_pending(),
            "it claims the second page"
        );
        let failed = tokio::time::timeout(
            crate::storage::testing::DEADLINE_GUARD,
            cache.get_span(&id, &SPAN, Issue::default(), |_| {
                std::future::ready(Err(Error::NotReady {
                    table: TABLE.to_string(),
                }))
            }),
        )
        .await
        .expect("a failed read returns while a page it joined is in flight");
        let Err(Error::Cache(CacheError::Fetch(error))) = failed else {
            panic!("the read receives its run's failure: {failed:?}");
        };
        assert!(matches!(*error, Error::NotReady { .. }));
        assert_eq!(cache.span_joins(), 1, "it joined the second page");
        assert_eq!(cache.fetches(), 2, "the other read's run, and its own");
        assert!(
            futures::poll!(fetching.as_mut()).is_pending(),
            "the other read's fetch runs on"
        );
    }

    /// A read of pages another read is still waiting to take a fan-out
    /// permit for fetches them itself rather than waiting behind that
    /// read's queue: a run is claimed only once it holds its permit, as
    /// `get_or_fetch` asks the cache only once it holds its own (RFD 013).
    /// The queued read, once it has its permit, is served from what the
    /// other read cached and fetches nothing.
    #[tokio::test]
    async fn test_get_span_of_pages_another_read_has_queued_is_not_held_behind_its_permits() {
        let cache = PageCache::new(ROOMY_BYTES, None).await.unwrap();
        let id: Arc<str> = Arc::from(FILE_ID);
        let file = span_file();
        let full = Arc::new(Semaphore::new(1));
        let taken = Arc::clone(&full).acquire_owned().await.unwrap();
        let answering = |span: Range<u64>| std::future::ready(Ok(span_bytes(&file, &span)));
        let queued_issue = Issue {
            fan_out: Some(&full),
            counted: None,
        };
        let mut queued = Box::pin(cache.get_span(&id, &SPAN, queued_issue, answering));
        assert!(
            futures::poll!(queued.as_mut()).is_pending(),
            "it waits for its permit"
        );
        let (alone, origin) = tokio::time::timeout(
            crate::storage::testing::DEADLINE_GUARD,
            cache.get_span(&id, &SPAN, Issue::default(), answering),
        )
        .await
        .expect("a read of another call is not held behind a full fan-out")
        .unwrap();
        assert_eq!(alone, pages_of(&file, &SPAN));
        assert_eq!(origin, PageOrigin::Fetched);
        assert_eq!(cache.span_joins(), 0, "nothing was in flight to join");
        drop(taken);
        let (served, origin) = queued.await.unwrap();
        assert_eq!(served, alone);
        assert_eq!(origin, PageOrigin::Cached);
        assert_eq!(cache.fetches(), 1, "the other read's run alone");
    }

    /// A run whose pages another read claimed while it waited for its
    /// permit fetches the stretch still free at its start, and the read's
    /// next pass joins the claimed page and fetches the rest as a run of
    /// its own: each page is fetched once.
    #[tokio::test]
    async fn test_get_span_of_a_run_partly_claimed_while_it_queued_fetches_the_rest() {
        let pages = [0..10, 10..20, 20..30];
        let claimed_elsewhere = 1;
        let cache = PageCache::new(ROOMY_BYTES, None).await.unwrap();
        let id: Arc<str> = Arc::from(FILE_ID);
        let file = span_file();
        let full = Arc::new(Semaphore::new(1));
        let taken = Arc::clone(&full).acquire_owned().await.unwrap();
        let spans = std::sync::Mutex::new(Vec::new());
        let recording = |span: Range<u64>| {
            spans.lock().unwrap().push(span.clone());
            std::future::ready(Ok(span_bytes(&file, &span)))
        };
        let queued_issue = Issue {
            fan_out: Some(&full),
            counted: None,
        };
        let mut queued = Box::pin(cache.get_span(&id, &pages, queued_issue, recording));
        assert!(
            futures::poll!(queued.as_mut()).is_pending(),
            "it waits for its permit"
        );
        let gate = Notify::new();
        let slow = |span: Range<u64>| {
            let bytes = span_bytes(&file, &span);
            let gate = &gate;
            async move {
                gate.notified().await;
                Ok(bytes)
            }
        };
        let middle = &pages[claimed_elsewhere..=claimed_elsewhere];
        let mut other = Box::pin(cache.get_span(&id, middle, Issue::default(), slow));
        assert!(
            futures::poll!(other.as_mut()).is_pending(),
            "it claims the middle page"
        );
        drop(taken);
        let (queued, other, ()) = tokio::join!(queued, other, async {
            joined(&cache, 1).await;
            gate.notify_one();
        });
        let (served, origin) = queued.unwrap();
        assert_eq!(served, pages_of(&file, &pages));
        assert_eq!(origin, PageOrigin::Fetched);
        assert_eq!(other.unwrap().0, pages_of(&file, middle));
        assert_eq!(
            *spans.lock().unwrap(),
            [pages[0].clone(), pages[2].clone()],
            "the queued read's runs, either side of the claimed page"
        );
        assert_eq!(cache.span_joins(), 1, "it joined the claimed page");
        assert_eq!(cache.fetches(), 3, "each page fetched once");
    }

    /// File ids tried to find one whose in-flight shard differs from an
    /// anchor's; with one shard per test worker, the first few suffice.
    const SHARD_SEARCH_FILES: usize = 1024;
    /// Times a cold span read of `SPAN` takes its shard's lock: to take
    /// stock, to claim its one run, and to release the claim.
    const COLD_SPAN_LOCKS: u64 = 3;
    /// Warm span reads each concurrent test task makes.
    const WARM_READS: usize = 64;

    /// A file id whose pages in flight fall in a shard other than
    /// `anchor`'s.
    fn file_of_another_shard(cache: &PageCache, anchor: &Arc<str>) -> Arc<str> {
        (0..SHARD_SEARCH_FILES)
            .map(|n| Arc::<str>::from(format!("{FILE_ID}.{n}")))
            .find(|id| !std::ptr::eq(cache.spans.shard(id), cache.spans.shard(anchor)))
            .unwrap()
    }

    /// Span reads of two files, as Scans of two tables make, the second
    /// file chosen so that its pages fall in another shard than the
    /// first's (`file_of_another_shard`), never take one lock: a cold read
    /// takes only its own file's shard's lock, and warm reads running
    /// together on several workers take no in-flight lock at all, since
    /// memory holds every page they read.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_get_span_of_two_files_takes_no_shared_lock_and_warm_reads_none() {
        let cache = PageCache::new(ROOMY_BYTES, None).await.unwrap();
        let file = span_file();
        let first: Arc<str> = Arc::from(FILE_ID);
        let second = file_of_another_shard(&cache, &first);
        let locked = |id: &Arc<str>| cache.spans.shard(id).locked.load(Ordering::Relaxed);
        let answering = |span: Range<u64>| std::future::ready(Ok(span_bytes(&file, &span)));
        cache
            .get_span(&first, &SPAN, Issue::default(), answering)
            .await
            .unwrap();
        assert_eq!(
            (locked(&first), locked(&second)),
            (COLD_SPAN_LOCKS, 0),
            "a cold read locks its own file's shard alone"
        );
        cache
            .get_span(&second, &SPAN, Issue::default(), answering)
            .await
            .unwrap();
        assert_eq!(
            (locked(&first), locked(&second)),
            (COLD_SPAN_LOCKS, COLD_SPAN_LOCKS)
        );
        let hits = cache.hits();
        let files = [&first, &second];
        let warm = files.map(|id| {
            let (cache, id) = (cache.clone(), Arc::clone(id));
            tokio::spawn(async move {
                for _ in 0..WARM_READS {
                    let (_, origin) = cache
                        .get_span(&id, &SPAN, Issue::default(), |_| never())
                        .await
                        .unwrap();
                    assert_eq!(origin, PageOrigin::Cached);
                }
            })
        });
        for task in warm {
            task.await.unwrap();
        }
        assert_eq!(
            (locked(&first), locked(&second)),
            (COLD_SPAN_LOCKS, COLD_SPAN_LOCKS),
            "warm reads take no in-flight lock"
        );
        assert_eq!(
            cache.hits() - hits,
            count(files.len() * WARM_READS * SPAN.len())
        );
        assert_eq!(cache.fetches(), 2, "one run per file");
    }

    /// The one page cache's series carry no label (RFD 027, Counted): a
    /// fetch counts in the unlabeled `page_cache_fetches_total`, and
    /// `publish_usage` sets the one unlabeled `page_cache_bytes`.
    #[test]
    fn test_the_page_cache_series_carry_no_label() {
        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let series = metrics::with_local_recorder(&recorder, || {
            runtime.block_on(async {
                let cache = PageCache::new(ROOMY_BYTES, None).await.unwrap();
                let runs = Arc::new(AtomicU64::new(0));
                cache
                    .get_or_fetch(key(FIRST_OFFSET), Issue::default(), || {
                        answering(PAGE, &runs)
                    })
                    .await
                    .unwrap();
                cache.publish_usage();
                snapshotter.snapshot().into_vec()
            })
        });
        let names = [
            metric::PAGE_CACHE_HITS_TOTAL,
            metric::PAGE_CACHE_FETCHES_TOTAL,
            metric::PAGE_CACHE_BYTES,
        ];
        for name in names {
            let found: Vec<_> = series
                .iter()
                .filter(|(key, ..)| key.key().name() == name)
                .collect();
            assert_eq!(found.len(), 1, "one {name} series");
            assert_eq!(found[0].0.key().labels().count(), 0, "{name} has no label");
        }
        let fetched = series
            .iter()
            .find(|(key, ..)| key.key().name() == metric::PAGE_CACHE_FETCHES_TOTAL)
            .map(|(.., value)| value);
        assert_eq!(fetched, Some(&DebugValue::Counter(1)));
    }

    /// Hits spread over a multi-thread runtime's workers reach
    /// `page_cache_hits_total` at `publish_hit_counts`, each once, and the
    /// last clone of the cache dropped publishes the rest, while `hits`
    /// reads each as it is counted. A snapshot of the debugging recorder
    /// takes the counter's value and resets it, so each snapshot reads what
    /// was published since the one before.
    #[test]
    fn test_publish_hit_counts_moves_every_thread_count_once() {
        let workers = 4;
        let hits_per_task = 8;
        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(workers)
            .enable_all()
            .build()
            .unwrap();
        let published = || {
            snapshotter
                .snapshot()
                .into_vec()
                .into_iter()
                .find(|(key, ..)| key.key().name() == metric::PAGE_CACHE_HITS_TOTAL)
                .map_or(0, |(.., value)| match value {
                    DebugValue::Counter(value) => value,
                    other @ (DebugValue::Gauge(_) | DebugValue::Histogram(_)) => {
                        panic!("hits are a counter, got {other:?}")
                    }
                })
        };
        metrics::with_local_recorder(&recorder, || {
            runtime.block_on(async {
                let cache = PageCache::new(ROOMY_BYTES, None).await.unwrap();
                let runs = Arc::new(AtomicU64::new(0));
                cache
                    .get_or_fetch(key(FIRST_OFFSET), Issue::default(), || {
                        answering(PAGE, &runs)
                    })
                    .await
                    .unwrap();
                let tasks: Vec<_> = (0..workers)
                    .map(|_| {
                        let cache = cache.clone();
                        tokio::spawn(async move {
                            for _ in 0..hits_per_task {
                                cache
                                    .get_or_fetch(key(FIRST_OFFSET), Issue::default(), never)
                                    .await
                                    .unwrap();
                            }
                        })
                    })
                    .collect();
                for task in tasks {
                    task.await.unwrap();
                }
                let hit = count(workers * hits_per_task);
                assert_eq!(cache.hits(), hit);
                assert_eq!(published(), 0, "counted on the shards until published");
                cache.publish_hit_counts();
                assert_eq!(published(), hit);
                cache.publish_hit_counts();
                assert_eq!(published(), 0, "nothing is published twice");
                cache
                    .get_or_fetch(key(FIRST_OFFSET), Issue::default(), never)
                    .await
                    .unwrap();
                let clone = cache.clone();
                drop(cache);
                assert_eq!(published(), 0, "a clone still holds the counts");
                drop(clone);
                assert_eq!(published(), 1, "the last clone dropped publishes the rest");
            });
        });
    }

    /// A fetch that answers with a slice of a larger buffer, as hyper's
    /// connection read buffer is, is cached in an allocation of its own:
    /// the entry is charged the slice's length, so it must not keep the
    /// rest of the buffer alive.
    #[tokio::test]
    async fn test_fetched_slice_is_cached_apart_from_its_buffer() {
        let cache = PageCache::new(ROOMY_BYTES, None).await.unwrap();
        let buffer = Bytes::from([PAGE, OTHER_PAGE].concat());
        let slice = buffer.slice(..PAGE.len());
        let (page, _) = cache
            .get_or_fetch(key(FIRST_OFFSET), Issue::default(), move || async move {
                Ok(slice)
            })
            .await
            .unwrap();
        assert_eq!(page, PAGE);
        assert!(
            !buffer.as_ptr_range().contains(&page.as_ptr()),
            "the cached page points into the fetch's buffer"
        );
    }

    #[tokio::test]
    async fn test_new_without_disk_is_memory_only() {
        let cache = PageCache::new(ROOMY_BYTES, None).await.unwrap();
        assert!(matches!(cache.level.as_ref(), Level::Memory(_)));
        let runs = Arc::new(AtomicU64::new(0));
        let (page, origin) = cache
            .get_or_fetch(key(FIRST_OFFSET), Issue::default(), || {
                answering(PAGE, &runs)
            })
            .await
            .unwrap();
        assert_eq!(&page[..], PAGE);
        assert_eq!(origin, PageOrigin::Fetched);
        let (again, origin) = cache
            .get_or_fetch(key(FIRST_OFFSET), Issue::default(), never)
            .await
            .unwrap();
        assert_eq!(&again[..], PAGE);
        assert_eq!(origin, PageOrigin::Cached);
        assert_eq!(runs.load(Ordering::Relaxed), 1);
        assert_eq!(cache.fetches(), 1);
        assert_eq!(cache.hits(), 1);
    }

    /// A page hit again soon after it was cached outlives any number of
    /// pages read once into its shard after it: a pass over many pages, as
    /// a large `MultiGet` makes, does not push out a page that keeps being
    /// asked for, while the pages it read once do leave.
    #[tokio::test]
    async fn test_page_hit_again_outlives_pages_read_once() {
        let first = PageKey(Arc::from(FILE_ID), FIRST_OFFSET);
        let weight = weigh(&first, &PageValue(Bytes::from_static(PAGE)));
        let cache = PageCache::new(memory_shards() * SHARD_PAGES * weight, None)
            .await
            .unwrap();
        let Level::Memory(memory) = cache.level.as_ref() else {
            panic!("a cache without a disk level is memory only");
        };
        let shards = count(memory.shards());
        let runs = Arc::new(AtomicU64::new(0));
        cache
            .get_or_fetch(key(FIRST_OFFSET), Issue::default(), || {
                answering(PAGE, &runs)
            })
            .await
            .unwrap();
        cache
            .get_or_fetch(key(FIRST_OFFSET), Issue::default(), never)
            .await
            .unwrap();
        let mut read_once = Vec::new();
        let mut offset = FIRST_OFFSET;
        for _ in 0..2 * SHARD_PAGES {
            offset = offset_sharing_shard(
                |key| memory.hash(key) % shards,
                &PageKey(Arc::from(FILE_ID), offset),
            );
            cache
                .get_or_fetch(key(offset), Issue::default(), || answering(PAGE, &runs))
                .await
                .unwrap();
            read_once.push(offset);
        }
        assert!(memory.contains(&first), "the page hit again was evicted");
        assert!(
            !memory.contains(&PageKey(Arc::from(FILE_ID), read_once[0])),
            "the shard held every page read once, so nothing was displaced"
        );
    }

    #[tokio::test]
    async fn test_close_memory_level_is_ok_and_keeps_serving() {
        let cache = PageCache::new(ROOMY_BYTES, None).await.unwrap();
        let runs = Arc::new(AtomicU64::new(0));
        cache
            .get_or_fetch(key(FIRST_OFFSET), Issue::default(), || {
                answering(PAGE, &runs)
            })
            .await
            .unwrap();
        cache.close().await.unwrap();
        let (again, _) = cache
            .get_or_fetch(key(FIRST_OFFSET), Issue::default(), never)
            .await
            .unwrap();
        assert_eq!(&again[..], PAGE);
        assert_eq!(cache.fetches(), 1);
        assert_eq!(cache.hits(), 1);
    }

    /// A span read fetches each maximal run of uncached pages that meet
    /// end to start with one call, splits it, and caches every page as its
    /// own entry at its offset: each is then served alone, and a second
    /// span read runs no fetch.
    #[tokio::test]
    async fn test_get_span_fetches_contiguous_uncached_pages_once_and_caches_each() {
        let cache = PageCache::new(ROOMY_BYTES, None).await.unwrap();
        let file: Bytes = (0..=u8::MAX).collect();
        let pages = [0..10, 10..26, 26..30, 30..40, 100..110];
        let cached = 1;
        let cached_page = file.slice(10..26);
        cache
            .get_or_fetch(key(pages[cached].start), Issue::default(), || {
                std::future::ready(Ok(cached_page))
            })
            .await
            .unwrap();
        let fetches = cache.fetches();
        let spans = std::sync::Mutex::new(Vec::new());
        let fetch = |span: Range<u64>| {
            spans.lock().unwrap().push(span.clone());
            let bytes = file.slice(approx::length(span.start)..approx::length(span.end));
            std::future::ready(Ok(bytes))
        };
        let id: Arc<str> = Arc::from(FILE_ID);
        let (served, origin) = cache
            .get_span(&id, &pages, Issue::default(), fetch)
            .await
            .unwrap();
        assert_eq!(origin, PageOrigin::Fetched);
        assert_eq!(
            *spans.lock().unwrap(),
            [0..10, 26..40, 100..110],
            "one span per run"
        );
        assert_eq!(
            cache.fetches() - fetches,
            3,
            "one fetch per run of uncached pages"
        );
        for (page, bytes) in pages.iter().zip(&served) {
            let start = approx::length(page.start);
            let end = approx::length(page.end);
            assert_eq!(bytes, &file.slice(start..end));
        }
        let fetches = cache.fetches();
        for (page, bytes) in pages.iter().zip(&served) {
            let (alone, origin) = cache
                .get_or_fetch(key(page.start), Issue::default(), never)
                .await
                .unwrap();
            assert_eq!(&alone, bytes);
            assert_eq!(origin, PageOrigin::Cached);
        }
        let (again, origin) = cache
            .get_span(&id, &pages, Issue::default(), |_| never())
            .await
            .unwrap();
        assert_eq!(again, served);
        assert_eq!(origin, PageOrigin::Cached);
        assert_eq!(
            cache.fetches(),
            fetches,
            "every page is served alone or again"
        );
    }

    /// `Run::fetch`'s future holds `FETCH_COPIES` copies of the caller's
    /// fetch future and no more: foyer stores the run's future three times
    /// for each page a lookup fetches, so each further copy adds three
    /// times the fetch future's size to every cold page fetch.
    #[test]
    fn test_fetch_future_holds_two_copies_of_the_callers_fetch() {
        let fetch = size_of_val(&padded());
        let run = size_of_val(&deadline_run().fetch(padded()));
        assert!(fetch >= PADDED_FETCH_BYTES, "{fetch} bytes");
        assert!(
            run < (FETCH_COPIES + 1) * fetch,
            "{run} bytes over a fetch of {fetch}"
        );
    }

    #[tokio::test]
    async fn test_get_or_fetch_coalesces_concurrent_misses() {
        let cache = PageCache::new(ROOMY_BYTES, None).await.unwrap();
        let runs = Arc::new(AtomicU64::new(0));
        let gate = Arc::new(Notify::new());
        let answers = race(&cache, FIRST_OFFSET, &gate, || gated(PAGE, &runs, &gate)).await;
        assert_eq!(answers.len(), CONCURRENT);
        for answer in answers {
            let (page, origin) = answer.unwrap();
            assert_eq!(&page[..], PAGE);
            assert_eq!(origin, PageOrigin::Fetched);
        }
        assert_eq!(runs.load(Ordering::Relaxed), 1);
        assert_eq!(cache.fetches(), 1);
        assert_eq!(cache.hits(), 0);

        let (again, origin) = cache
            .get_or_fetch(key(FIRST_OFFSET), Issue::default(), never)
            .await
            .unwrap();
        assert_eq!(&again[..], PAGE);
        assert_eq!(origin, PageOrigin::Cached);
        assert_eq!(cache.fetches(), 1);
        assert_eq!(cache.hits(), 1);
    }

    #[tokio::test]
    async fn test_get_or_fetch_fetch_error_reaches_every_waiter() {
        let cache = PageCache::new(ROOMY_BYTES, None).await.unwrap();
        let runs = Arc::new(AtomicU64::new(0));
        let gate = Arc::new(Notify::new());
        let answers = race(&cache, FIRST_OFFSET, &gate, || failing(&runs, &gate)).await;
        assert_eq!(answers.len(), CONCURRENT);
        let mut shared: Vec<Arc<Error>> = Vec::with_capacity(CONCURRENT);
        for answer in answers {
            let Err(Error::Cache(CacheError::Fetch(error))) = answer else {
                panic!("every waiter receives the fetch failure");
            };
            assert!(matches!(*error, Error::NotReady { .. }));
            shared.push(error);
        }
        assert!(shared.iter().all(|error| Arc::ptr_eq(error, &shared[0])));
        assert_eq!(runs.load(Ordering::Relaxed), 1);
        assert_eq!(cache.fetches(), 1);
        assert_eq!(cache.hits(), 0);

        let (page, _) = cache
            .get_or_fetch(key(FIRST_OFFSET), Issue::default(), || {
                answering(PAGE, &runs)
            })
            .await
            .unwrap();
        assert_eq!(&page[..], PAGE);
        assert_eq!(cache.fetches(), 2);
    }

    /// A fetch that outlasts its handle's deadline is dropped at it, and
    /// every coalesced waiter receives the typed deadline error for the
    /// page; nothing is cached, so the next caller runs a fetch of its own.
    #[tokio::test]
    async fn test_get_or_fetch_ends_a_fetch_at_its_deadline_for_every_waiter() {
        let cache = PageCache::new(ROOMY_BYTES, None)
            .await
            .unwrap()
            .with_fetch_deadline(FETCH_DEADLINE);
        let runs = Arc::new(AtomicU64::new(0));
        let dropped = Arc::new(AtomicBool::new(false));
        let gate = Arc::new(Notify::new());
        let started = tokio::time::Instant::now();
        let answers = tokio::time::timeout(
            FETCH_GUARD,
            race(&cache, FIRST_OFFSET, &gate, || stalled(&runs, &dropped)),
        )
        .await
        .expect("the fetch ends at its deadline, before the guard");
        assert!(started.elapsed() >= FETCH_DEADLINE);
        assert_eq!(answers.len(), CONCURRENT);
        for answer in answers {
            let Err(Error::Cache(CacheError::FetchDeadline {
                path,
                offset,
                after,
            })) = answer
            else {
                panic!("every waiter receives the deadline: {answer:?}");
            };
            assert_eq!((path.as_str(), offset), (FILE_ID, FIRST_OFFSET));
            assert_eq!(after, FETCH_DEADLINE);
        }
        assert!(dropped.load(Ordering::Relaxed), "the fetch was dropped");
        assert_eq!(runs.load(Ordering::Relaxed), 1);
        assert_eq!(cache.fetches(), 1);

        let (page, origin) = cache
            .get_or_fetch(key(FIRST_OFFSET), Issue::default(), || {
                answering(PAGE, &runs)
            })
            .await
            .unwrap();
        assert_eq!(&page[..], PAGE);
        assert_eq!(origin, PageOrigin::Fetched);
        assert_eq!(cache.fetches(), 2);
    }

    #[tokio::test]
    async fn test_usage_grows_by_page_bytes_plus_overhead() {
        let cache = PageCache::new(ROOMY_BYTES, None).await.unwrap();
        assert_eq!(cache.usage(), 0);
        let runs = Arc::new(AtomicU64::new(0));
        cache
            .get_or_fetch(key(FIRST_OFFSET), Issue::default(), || {
                answering(PAGE, &runs)
            })
            .await
            .unwrap();
        assert_eq!(cache.usage(), PAGE.len() + ENTRY_OVERHEAD_BYTES);
        cache
            .get_or_fetch(key(SECOND_OFFSET), Issue::default(), || {
                answering(OTHER_PAGE, &runs)
            })
            .await
            .unwrap();
        assert_eq!(
            cache.usage(),
            PAGE.len() + OTHER_PAGE.len() + 2 * ENTRY_OVERHEAD_BYTES
        );
    }

    #[tokio::test]
    async fn test_clear_drops_every_page_so_the_next_request_fetches() {
        let cache = PageCache::new(ROOMY_BYTES, None).await.unwrap();
        let runs = Arc::new(AtomicU64::new(0));
        for _ in 0..2 {
            cache
                .get_or_fetch(key(FIRST_OFFSET), Issue::default(), || {
                    answering(PAGE, &runs)
                })
                .await
                .unwrap();
        }
        assert_eq!((cache.fetches(), cache.hits()), (1, 1));
        cache.clear();
        assert_eq!(cache.usage(), 0);
        let (page, origin) = cache
            .get_or_fetch(key(FIRST_OFFSET), Issue::default(), || {
                answering(PAGE, &runs)
            })
            .await
            .unwrap();
        assert_eq!(&page[..], PAGE);
        assert_eq!(origin, PageOrigin::Fetched);
        assert_eq!((cache.fetches(), cache.hits()), (2, 1));
    }

    #[tokio::test]
    async fn test_get_or_fetch_serves_a_hybrid_page_from_memory_before_eviction() {
        let dir = tempfile::tempdir().unwrap();
        let (level, stats) = disk_level(dir.path());
        let cache = PageCache::new(ROOMY_BYTES, Some(level)).await.unwrap();
        let runs = Arc::new(AtomicU64::new(0));
        cache
            .get_or_fetch(key(FIRST_OFFSET), Issue::default(), || {
                answering(PAGE, &runs)
            })
            .await
            .unwrap();
        assert_eq!(cache.fetches(), 1);
        assert_eq!(cache.hits(), 0);

        let (page, origin) = cache
            .get_or_fetch(key(FIRST_OFFSET), Issue::default(), never)
            .await
            .unwrap();
        assert_eq!(&page[..], PAGE);
        assert_eq!(origin, PageOrigin::Cached);
        assert_eq!(runs.load(Ordering::Relaxed), 1);
        assert_eq!(cache.fetches(), 1);
        assert_eq!(cache.hits(), 1);
        assert_eq!(stats.local_reads(), 0);
    }

    #[tokio::test]
    async fn test_get_or_fetch_serves_evicted_page_from_disk() {
        let dir = tempfile::tempdir().unwrap();
        let (level, stats) = disk_level(dir.path());
        let cache = PageCache::new(TINY_BYTES, Some(level)).await.unwrap();
        let runs = Arc::new(AtomicU64::new(0));
        evict_to_disk(&cache, &runs).await;
        assert_eq!(cache.fetches(), 2);
        assert_eq!(cache.hits(), 0);
        assert_eq!(stats.local_reads(), 0);

        let (page, origin) = cache
            .get_or_fetch(key(FIRST_OFFSET), Issue::default(), never)
            .await
            .unwrap();
        assert_eq!(&page[..], PAGE);
        assert_eq!(origin, PageOrigin::Cached);
        assert_eq!(runs.load(Ordering::Relaxed), 2);
        assert_eq!(cache.fetches(), 2);
        assert_eq!(cache.hits(), 1);
        assert_eq!(stats.local_reads(), 1);
        assert_eq!(stats.local_bytes(), page_bytes(PAGE));
    }

    /// Lookups coalesced onto one disk read each count a hit and a local
    /// read: the count is per lookup served, as under `DiskChunks::page`.
    /// Deterministic on the current-thread runtime for the same reason as
    /// `race`: every caller enqueues before foyer's fetch task runs.
    #[tokio::test]
    async fn test_get_or_fetch_charges_every_lookup_coalesced_onto_one_disk_read() {
        let dir = tempfile::tempdir().unwrap();
        let (level, stats) = disk_level(dir.path());
        let cache = PageCache::new(TINY_BYTES, Some(level)).await.unwrap();
        let runs = Arc::new(AtomicU64::new(0));
        evict_to_disk(&cache, &runs).await;
        let gate = Arc::new(Notify::new());

        let answers = race(&cache, FIRST_OFFSET, &gate, never).await;
        assert_eq!(answers.len(), CONCURRENT);
        for answer in answers {
            let (page, origin) = answer.unwrap();
            assert_eq!(&page[..], PAGE);
            assert_eq!(origin, PageOrigin::Cached);
        }
        assert_eq!(runs.load(Ordering::Relaxed), 2);
        assert_eq!(cache.fetches(), 2);
        assert_eq!(cache.hits(), count(CONCURRENT));
        assert_eq!(stats.local_reads(), count(CONCURRENT));
        assert_eq!(stats.local_bytes(), count(CONCURRENT) * page_bytes(PAGE));
    }

    /// The disk level as production reaches it: the page leaves memory by
    /// eviction, is written by the level, and a new cache over the same
    /// directory serves it with a local read and no fetch. Eviction, not
    /// the close, puts it there: it has left memory before the first cache
    /// closes, so the close can flush only the displacing page.
    #[tokio::test]
    async fn test_new_recovers_the_disk_level_on_restart() {
        let dir = tempfile::tempdir().unwrap();
        let (level, first_stats) = disk_level(dir.path());
        let first = PageCache::new(TINY_BYTES, Some(level)).await.unwrap();
        let runs = Arc::new(AtomicU64::new(0));
        evict_to_disk(&first, &runs).await;
        assert_eq!(first_stats.local_reads(), 0);
        first.close().await.unwrap();
        drop(first);

        let (level, stats) = disk_level(dir.path());
        let second = PageCache::new(ROOMY_BYTES, Some(level)).await.unwrap();
        let (page, origin) = second
            .get_or_fetch(key(FIRST_OFFSET), Issue::default(), never)
            .await
            .unwrap();
        assert_eq!(&page[..], PAGE);
        assert_eq!(origin, PageOrigin::Cached);
        assert_eq!(runs.load(Ordering::Relaxed), 2);
        assert_eq!(second.fetches(), 0);
        assert_eq!(second.hits(), 1);
        assert_eq!(stats.local_reads(), 1);
        assert_eq!(stats.local_bytes(), page_bytes(PAGE));

        let (again, _) = second
            .get_or_fetch(key(FIRST_OFFSET), Issue::default(), never)
            .await
            .unwrap();
        assert_eq!(&again[..], PAGE);
        assert_eq!(second.hits(), 2);
        assert_eq!(stats.local_reads(), 1);
    }

    /// A page whose disk copy no longer matches its checksum is refused by
    /// the level and fetched instead, with no error to the caller (RFD 017:
    /// the directory is disposable). The copy is damaged after the first
    /// cache closes, so the level's index still names it when the second
    /// cache recovers.
    #[tokio::test]
    async fn test_get_or_fetch_fetches_a_page_whose_disk_copy_is_corrupt() {
        let dir = tempfile::tempdir().unwrap();
        let (level, _) = disk_level(dir.path());
        let cache_dir = level.dir().to_path_buf();
        let first = PageCache::new(TINY_BYTES, Some(level)).await.unwrap();
        let runs = Arc::new(AtomicU64::new(0));
        evict_to_disk(&first, &runs).await;
        first.close().await.unwrap();
        drop(first);
        assert!(corrupt_on_disk(&cache_dir, PAGE) >= 1);

        let (level, stats) = disk_level(dir.path());
        let second = PageCache::new(ROOMY_BYTES, Some(level)).await.unwrap();
        let (page, origin) = second
            .get_or_fetch(key(FIRST_OFFSET), Issue::default(), || {
                answering(PAGE, &runs)
            })
            .await
            .unwrap();
        assert_eq!(&page[..], PAGE);
        assert_eq!(origin, PageOrigin::Fetched);
        assert_eq!(runs.load(Ordering::Relaxed), 3);
        assert_eq!(second.fetches(), 1);
        assert_eq!(second.hits(), 0);
        assert_eq!(stats.local_reads(), 0);
    }

    /// The pages the disk-level span tests read: three that meet.
    const DISK_SPAN: [Range<u64>; 3] = [0..10, 10..20, 20..30];

    /// A hybrid cache with room in memory for every page, its disk level in
    /// `dir`, holding `DISK_SPAN` of `file` on the disk level alone: the
    /// span is fetched, then every page leaves memory for disk (`clear`),
    /// and the level has written them. One fetch runs.
    async fn span_on_disk(dir: &Path, id: &Arc<str>, file: &Bytes) -> (PageCache, Arc<IoStats>) {
        let (level, stats) = disk_level(dir);
        let cache = PageCache::new(ROOMY_BYTES, Some(level)).await.unwrap();
        cache
            .get_span(id, &DISK_SPAN, Issue::default(), |span| {
                std::future::ready(Ok(span_bytes(file, &span)))
            })
            .await
            .unwrap();
        cache.clear();
        cache.disk_writes_done().await;
        for page in &DISK_SPAN {
            assert_eq!(cache.held_page(id, page.start), None, "left memory");
        }
        assert_eq!(cache.fetches(), 1);
        (cache, stats)
    }

    /// Drops the page of `id` at each of `offsets` from both levels.
    async fn drop_from_both_levels(cache: &PageCache, id: &Arc<str>, offsets: &[u64]) {
        for offset in offsets {
            hybrid(cache).remove(&PageKey(Arc::clone(id), *offset));
        }
        cache.disk_writes_done().await;
    }

    /// Reads `DISK_SPAN` of `file` through `cache`, recording every span its
    /// fetch is called for, and returns what it served and those spans.
    async fn read_recording(
        cache: &PageCache,
        id: &Arc<str>,
        file: &Bytes,
    ) -> ((Vec<Bytes>, PageOrigin), Vec<Range<u64>>) {
        let spans = std::sync::Mutex::new(Vec::new());
        let served = cache
            .get_span(id, &DISK_SPAN, Issue::default(), |span| {
                spans.lock().unwrap().push(span.clone());
                std::future::ready(Ok(span_bytes(file, &span)))
            })
            .await
            .unwrap();
        (served, spans.into_inner().unwrap())
    }

    /// A span read takes the pages the disk level holds with local reads,
    /// each counted a hit and a local read of its length, and fetches
    /// nothing (RFD 027, A span read); the pages it read are in memory
    /// after it, as foyer's hybrid read places them.
    #[tokio::test]
    async fn test_a_span_the_disk_level_holds_is_read_without_a_fetch() {
        let dir = tempfile::tempdir().unwrap();
        let id: Arc<str> = Arc::from(FILE_ID);
        let file = span_file();
        let (cache, stats) = span_on_disk(dir.path(), &id, &file).await;
        let hits = cache.hits();

        let (served, origin) = cache
            .get_span(&id, &DISK_SPAN, Issue::default(), |_| never())
            .await
            .unwrap();
        assert_eq!(served, pages_of(&file, &DISK_SPAN));
        assert_eq!(origin, PageOrigin::Cached);
        assert_eq!(cache.fetches(), 1, "the first read's fetch alone");
        assert_eq!(cache.hits() - hits, count(DISK_SPAN.len()));
        assert_eq!(stats.local_reads(), count(DISK_SPAN.len()));
        assert_eq!(
            stats.local_bytes(),
            DISK_SPAN
                .iter()
                .map(|page| page.end - page.start)
                .sum::<u64>()
        );
        for (page, bytes) in DISK_SPAN.iter().zip(&served) {
            assert_eq!(cache.held_page(&id, page.start).as_ref(), Some(bytes));
        }
    }

    /// A span read whose middle page neither level holds fetches that page
    /// alone and reads its neighbors from the disk level.
    #[tokio::test]
    async fn test_a_span_missing_one_page_from_both_levels_fetches_that_page_alone() {
        let dir = tempfile::tempdir().unwrap();
        let id: Arc<str> = Arc::from(FILE_ID);
        let file = span_file();
        let (cache, stats) = span_on_disk(dir.path(), &id, &file).await;
        drop_from_both_levels(&cache, &id, &[DISK_SPAN[1].start]).await;
        let hits = cache.hits();

        let ((served, origin), spans) = read_recording(&cache, &id, &file).await;
        assert_eq!(served, pages_of(&file, &DISK_SPAN));
        assert_eq!(origin, PageOrigin::Fetched);
        assert_eq!(spans, &DISK_SPAN[1..2], "the missing page alone");
        assert_eq!(cache.fetches(), 2);
        assert_eq!(cache.hits() - hits, 2, "the two pages on disk");
        assert_eq!(stats.local_reads(), 2);
    }

    /// A span read whose first and last pages neither level holds fetches
    /// each as a run of its own: the page on disk between them breaks the
    /// run.
    #[tokio::test]
    async fn test_a_span_missing_its_ends_fetches_two_runs() {
        let dir = tempfile::tempdir().unwrap();
        let id: Arc<str> = Arc::from(FILE_ID);
        let file = span_file();
        let (cache, stats) = span_on_disk(dir.path(), &id, &file).await;
        drop_from_both_levels(&cache, &id, &[DISK_SPAN[0].start, DISK_SPAN[2].start]).await;
        let hits = cache.hits();

        let ((served, origin), spans) = read_recording(&cache, &id, &file).await;
        assert_eq!(served, pages_of(&file, &DISK_SPAN));
        assert_eq!(origin, PageOrigin::Fetched);
        assert_eq!(spans, [DISK_SPAN[0].clone(), DISK_SPAN[2].clone()]);
        assert_eq!(cache.fetches(), 3, "one fetch per end");
        assert_eq!(cache.hits() - hits, 1, "the middle page on disk");
        assert_eq!(stats.local_reads(), 1);
    }

    /// Concurrent span reads of a page the disk level holds each read it
    /// once, counted a hit and a local read apiece as coalesced lookups
    /// are, and none fetches or joins a fetch. Deterministic on the
    /// current-thread runtime for the same reason as `race`: every read
    /// asks the level before its read runs.
    #[tokio::test]
    async fn test_concurrent_spans_over_a_page_on_disk_read_it_once_each_and_fetch_nothing() {
        let dir = tempfile::tempdir().unwrap();
        let (level, stats) = disk_level(dir.path());
        let cache = PageCache::new(TINY_BYTES, Some(level)).await.unwrap();
        let runs = Arc::new(AtomicU64::new(0));
        evict_to_disk(&cache, &runs).await;
        let id: Arc<str> = Arc::from(FILE_ID);
        let span = FIRST_OFFSET..FIRST_OFFSET + page_bytes(PAGE);
        let page = std::slice::from_ref(&span);
        let hits = cache.hits();

        let reads =
            (0..CONCURRENT).map(|_| cache.get_span(&id, page, Issue::default(), |_| never()));
        for answer in join_all(reads).await {
            let (served, origin) = answer.unwrap();
            assert_eq!(served, [Bytes::from_static(PAGE)]);
            assert_eq!(origin, PageOrigin::Cached);
        }
        assert_eq!(cache.fetches(), 2, "evict_to_disk's fetches alone");
        assert_eq!(cache.span_joins(), 0);
        assert_eq!(cache.hits() - hits, count(CONCURRENT));
        assert_eq!(stats.local_reads(), count(CONCURRENT));
    }

    /// A page another span read claims while this read is asking the disk
    /// level for it is joined, not fetched a second time: the run claims
    /// only what is still neither in memory nor in flight once the disk
    /// reads are done (`claim_run`). The other read's claim is made by hand
    /// while this read waits on the level, and its outcome published as
    /// `fetch_run` publishes one.
    #[tokio::test]
    async fn test_get_span_joins_a_page_claimed_while_it_read_the_disk_level() {
        let dir = tempfile::tempdir().unwrap();
        let (level, stats) = disk_level(dir.path());
        let cache = PageCache::new(ROOMY_BYTES, Some(level)).await.unwrap();
        let id: Arc<str> = Arc::from(FILE_ID);
        let file = span_file();
        let page = &SPAN[..1];
        let mut reading = Box::pin(cache.get_span(&id, page, Issue::default(), |_| never()));
        assert!(
            futures::poll!(reading.as_mut()).is_pending(),
            "it asks the disk level"
        );
        let claim = cache
            .claim_run(&id, page, 0..page.len())
            .expect("the page is neither in memory nor in flight");
        let bytes = pages_of(&file, page);
        let (answer, ()) = tokio::join!(reading, async {
            joined(&cache, 1).await;
            cache.insert(PageKey(Arc::clone(&id), page[0].start), bytes[0].clone());
            claim.publish(&Ok(bytes.clone()));
            drop(claim);
        });
        let (served, origin) = answer.unwrap();
        assert_eq!(cache.span_joins(), 1);
        assert_eq!(served, bytes);
        assert_eq!(origin, PageOrigin::Fetched);
        assert_eq!(cache.fetches(), 0);
        assert_eq!(cache.hits(), 0);
        assert_eq!(stats.local_reads(), 0);
    }

    /// Checks a span read of `PAGE` that shared a lookup's fetch of it:
    /// both are served the lookup's fetch, the read labelled fetched and
    /// counted as a join, with one fetch run between them and nothing read
    /// from the disk level or counted a hit.
    fn assert_shared_the_lookups_fetch(
        cache: &PageCache,
        stats: &IoStats,
        runs: &AtomicU64,
        looked_up: Result<(Bytes, PageOrigin)>,
        read: Result<(Vec<Bytes>, PageOrigin)>,
    ) {
        assert_eq!(
            looked_up.unwrap(),
            (Bytes::from_static(PAGE), PageOrigin::Fetched)
        );
        let (served, origin) = read.unwrap();
        assert_eq!(served, [Bytes::from_static(PAGE)]);
        assert_eq!(origin, PageOrigin::Fetched, "the page came from outside");
        assert_eq!(cache.span_joins(), 1, "the read joined the lookup's fetch");
        assert_eq!(runs.load(Ordering::Relaxed), 1);
        assert_eq!(cache.fetches(), 1);
        assert_eq!(cache.hits(), 0);
        assert_eq!(stats.local_reads(), 0);
    }

    /// A span read of a page a lookup is fetching joins the lookup's fetch
    /// on its way to the disk level, since memory does not show foyer's
    /// flights: the page is fetched once, and the read is labelled fetched
    /// and counted as a join. The lookup's fetch is held behind `gate`
    /// until both wait on it.
    #[tokio::test]
    async fn test_get_span_of_a_page_a_lookup_is_fetching_joins_that_fetch() {
        let dir = tempfile::tempdir().unwrap();
        let (level, stats) = disk_level(dir.path());
        let cache = PageCache::new(ROOMY_BYTES, Some(level)).await.unwrap();
        let id: Arc<str> = Arc::from(FILE_ID);
        let span = FIRST_OFFSET..FIRST_OFFSET + page_bytes(PAGE);
        let page = std::slice::from_ref(&span);
        let runs = Arc::new(AtomicU64::new(0));
        let gate = Arc::new(Notify::new());
        let mut lookup = Box::pin(cache.get_or_fetch(key(FIRST_OFFSET), Issue::default(), || {
            gated(PAGE, &runs, &gate)
        }));
        assert!(
            futures::poll!(lookup.as_mut()).is_pending(),
            "the lookup's flight is open"
        );
        let mut reading = Box::pin(cache.get_span(&id, page, Issue::default(), |_| never()));
        assert!(
            futures::poll!(reading.as_mut()).is_pending(),
            "the read waits on the lookup's flight"
        );
        gate.notify_one();
        let (looked_up, read) = tokio::join!(lookup, reading);
        assert_shared_the_lookups_fetch(&cache, &stats, &runs, looked_up, read);
    }

    /// A lookup of a page a span read is asking the disk level for joins
    /// the read's flight with its fetch, which runs when the level lacks
    /// the page: the read is served that fetch, labelled fetched and
    /// counted as a join, and the page is fetched once.
    #[tokio::test]
    async fn test_get_span_of_a_page_a_lookup_joins_on_disk_is_served_its_fetch() {
        let dir = tempfile::tempdir().unwrap();
        let (level, stats) = disk_level(dir.path());
        let cache = PageCache::new(ROOMY_BYTES, Some(level)).await.unwrap();
        let id: Arc<str> = Arc::from(FILE_ID);
        let span = FIRST_OFFSET..FIRST_OFFSET + page_bytes(PAGE);
        let page = std::slice::from_ref(&span);
        let runs = Arc::new(AtomicU64::new(0));
        let gate = Arc::new(Notify::new());
        let mut reading = Box::pin(cache.get_span(&id, page, Issue::default(), |_| never()));
        assert!(
            futures::poll!(reading.as_mut()).is_pending(),
            "the read asks the disk level"
        );
        let mut lookup = Box::pin(cache.get_or_fetch(key(FIRST_OFFSET), Issue::default(), || {
            gated(PAGE, &runs, &gate)
        }));
        assert!(
            futures::poll!(lookup.as_mut()).is_pending(),
            "the lookup joins the read's flight"
        );
        gate.notify_one();
        let (looked_up, read) = tokio::join!(lookup, reading);
        assert_shared_the_lookups_fetch(&cache, &stats, &runs, looked_up, read);
    }

    /// `held_span` reads memory alone: a span the disk level holds is not
    /// held, and nothing is counted or read; once a span read has brought
    /// the pages into memory it is held, with no local read of its own.
    #[tokio::test]
    async fn test_held_span_still_reads_memory_alone() {
        let dir = tempfile::tempdir().unwrap();
        let id: Arc<str> = Arc::from(FILE_ID);
        let file = span_file();
        let (cache, stats) = span_on_disk(dir.path(), &id, &file).await;
        let hits = cache.hits();

        assert_eq!(cache.held_span(&id, &DISK_SPAN), None);
        assert_eq!(cache.hits(), hits);
        assert_eq!(stats.local_reads(), 0);

        cache
            .get_span(&id, &DISK_SPAN, Issue::default(), |_| never())
            .await
            .unwrap();
        let reads = stats.local_reads();
        let hits = cache.hits();
        assert_eq!(
            cache.held_span(&id, &DISK_SPAN),
            Some(pages_of(&file, &DISK_SPAN))
        );
        assert_eq!(cache.hits() - hits, count(DISK_SPAN.len()));
        assert_eq!(stats.local_reads(), reads);
    }

    /// A disk level of more blocks than `LOWERED_LIMIT` leaves descriptors
    /// for, so opening it runs out partway.
    const MANY_BLOCKS_BYTES: u64 = 128 * PAGE_CACHE_DISK_BLOCK_BYTES;

    /// The soft limit the child test lowers itself to: room for the test
    /// harness and a runtime, not for `MANY_BLOCKS_BYTES` of blocks.
    const LOWERED_LIMIT: u64 = 64;

    #[test]
    fn test_open_error_out_of_descriptors_is_descriptor_limit() {
        let dir = tempfile::tempdir().unwrap();
        let (level, _) = disk_level(dir.path());
        for code in [libc::EMFILE, libc::ENFILE] {
            let error = foyer::Error::io_error(io::Error::from_raw_os_error(code));
            let Error::Cache(CacheError::DescriptorLimit {
                limit,
                needed,
                source,
            }) = open_error(error, &level)
            else {
                panic!("os error {code} is not a descriptor limit");
            };
            assert_eq!(limit, descriptors::limit());
            assert_eq!(needed, descriptors::needed(DISK_BYTES, 0));
            assert_eq!(source.raw_os_error(), Some(code));
        }
    }

    #[test]
    fn test_open_error_finds_descriptors_under_a_wrapping_error() {
        let dir = tempfile::tempdir().unwrap();
        let (level, _) = disk_level(dir.path());
        let inner = foyer::Error::io_error(io::Error::from_raw_os_error(libc::EMFILE));
        let outer =
            foyer::Error::new(ErrorKind::Recover, "recovering the level").with_source(inner);
        assert!(matches!(
            open_error(outer, &level),
            Error::Cache(CacheError::DescriptorLimit { .. })
        ));
    }

    #[test]
    fn test_open_error_other_failures_stay_backend() {
        let dir = tempfile::tempdir().unwrap();
        let (level, _) = disk_level(dir.path());
        let no_space = foyer::Error::io_error(io::Error::from_raw_os_error(libc::ENOSPC));
        assert!(matches!(
            open_error(no_space, &level),
            Error::Cache(CacheError::Backend(_))
        ));
        let config = foyer::Error::new(ErrorKind::Config, "refused");
        assert!(matches!(
            open_error(config, &level),
            Error::Cache(CacheError::Backend(_))
        ));
    }

    /// Opening a real disk level under a soft limit below its blocks fails
    /// as foyer runs out of descriptors, and that failure arrives as
    /// `DescriptorLimit` with the limit in force. In a child process, since
    /// the test lowers the process's own limit.
    #[test]
    fn test_new_with_too_few_descriptors_is_descriptor_limit() {
        descriptors::testing::in_child(
            "cache::tests::test_new_with_too_few_descriptors_is_descriptor_limit",
            || {
                let dir = tempfile::tempdir().unwrap();
                let config = LocalConfig {
                    dir: dir.path().to_path_buf(),
                    index_budget_bytes: None,
                    page_cache_disk_bytes: MANY_BLOCKS_BYTES,
                };
                let local = LocalDir::open(&config, Arc::new(IoStats::default())).unwrap();
                let level = local.page_cache_level().unwrap();
                let runtime = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .unwrap();
                descriptors::set(LOWERED_LIMIT, descriptors::current().rlim_max).unwrap();
                let opened = runtime.block_on(PageCache::new(ROOMY_BYTES, Some(level)));
                let Err(Error::Cache(CacheError::DescriptorLimit {
                    limit,
                    needed,
                    source,
                })) = opened
                else {
                    panic!("opened under {LOWERED_LIMIT} descriptors: {opened:?}");
                };
                assert_eq!(limit, LOWERED_LIMIT);
                let expected =
                    runtime.block_on(async { descriptors::needed(MANY_BLOCKS_BYTES, 0) });
                assert_eq!(needed, expected);
                assert_eq!(source.raw_os_error(), Some(libc::EMFILE));
            },
        );
    }
}
