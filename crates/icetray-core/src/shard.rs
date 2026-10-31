//! Splitting state that concurrent lookups write into per-core shards: how
//! many shards, from the runtime's worker threads, and which shard a thread
//! writes, so lookups running on different workers do not contend for one
//! cache line or one lock.

use std::fmt;
use std::num::NonZero;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

use metrics::Counter;

/// The threads that run lookups: the worker threads of the tokio runtime
/// the caller runs on, or the machine's available parallelism when called
/// outside a runtime (or 1 when the platform does not report it). At least
/// one.
pub(crate) fn workers() -> usize {
    tokio::runtime::Handle::try_current()
        .map_or_else(
            |_outside| std::thread::available_parallelism().map_or(1, NonZero::get),
            |runtime| runtime.metrics().num_workers(),
        )
        .max(1)
}

/// The shard count for state that every worker thread writes: `workers()`
/// rounded up to a power of two, so that a thread's `slot` masks into it.
pub(crate) fn count() -> usize {
    workers().next_power_of_two()
}

/// The next slot a thread draws on its first `slot` call.
static NEXT_SLOT: AtomicUsize = AtomicUsize::new(0);

thread_local! {
    /// The calling thread's slot, drawn from `NEXT_SLOT` on first use.
    static SLOT: usize = NEXT_SLOT.fetch_add(1, Ordering::Relaxed);
}

/// The calling thread's slot: a number drawn once per thread, in the order
/// threads first ask. Masked by `count() - 1`, it gives the first threads
/// to ask distinct shards, so a runtime's workers write shards of their own
/// when they are the threads that draw.
pub(crate) fn slot() -> usize {
    SLOT.with(|slot| *slot)
}

/// One `T` per shard, `count()` of them when built, each on a cache line of
/// its own. `mine` is the calling thread's, the one its `slot()` masks to,
/// so threads that drew distinct slots and write through `mine` write
/// distinct lines. A reader visits them all (`iter`).
pub(crate) struct Sharded<T> {
    shards: Box<[Line<T>]>,
}

/// A shard's value, aligned to 128 bytes: the cache line of the Apple
/// M-series cores and twice that of x86, so two shards share no line on
/// either. A shard whose state is allocated apart from its line holds each
/// part a thread writes in a `Line` too, so no part shares a line with
/// another shard's.
#[repr(align(128))]
#[derive(Default)]
pub(crate) struct Line<T>(pub(crate) T);

impl<T: Default> Sharded<T> {
    /// `count()` shards, each `T::default()`.
    pub(crate) fn new() -> Self {
        Self::from_fn(T::default)
    }
}

impl<T> Sharded<T> {
    /// `count()` shards, each what `make` returns.
    pub(crate) fn from_fn(mut make: impl FnMut() -> T) -> Self {
        Self {
            shards: (0..count()).map(|_| Line(make())).collect(),
        }
    }
}

impl<T> Sharded<T> {
    /// The calling thread's shard.
    pub(crate) fn mine(&self) -> &T {
        &self.shards[slot() & (self.shards.len() - 1)].0
    }

    /// The shard `hash` masks to, for state split by a key rather than by
    /// thread: callers whose keys' hashes differ in their low bits use
    /// distinct shards, whichever thread they run on.
    pub(crate) fn of(&self, hash: u64) -> &T {
        &self.shards[crate::approx::length(hash) & (self.shards.len() - 1)].0
    }

    /// Every shard, for a reader that sums or takes them.
    pub(crate) fn iter(&self) -> impl Iterator<Item = &T> {
        self.shards.iter().map(|line| &line.0)
    }
}

/// A count of events, kept on the calling thread's shard so concurrent
/// events on different workers write different lines, and published into a
/// counter handle registered once by the owner, with the recorder current
/// then, so an event looks up no series.
pub(crate) struct Tally {
    /// Each shard's events since the tally was made; they only grow.
    shards: Sharded<AtomicU64>,
    /// The shards' sum as of the last `publish`, what the counter has been
    /// given so far.
    published: AtomicU64,
    total: Counter,
}

impl Tally {
    /// A tally of no events that publishes into `total`.
    pub(crate) fn new(total: Counter) -> Self {
        Self {
            shards: Sharded::new(),
            published: AtomicU64::new(0),
            total,
        }
    }

    /// Counts one event on the calling thread's shard.
    pub(crate) fn increment(&self) {
        self.shards.mine().fetch_add(1, Ordering::Relaxed);
    }

    /// The events counted so far, summed over the shards.
    pub(crate) fn counted(&self) -> u64 {
        self.shards
            .iter()
            .map(|shard| shard.load(Ordering::Relaxed))
            .sum()
    }

    /// Adds to the counter what the shards have counted beyond what it was
    /// given. `published` only rises, by `fetch_max`, and the counter is
    /// given the rise alone, so two publishes at once give it each event
    /// once between them.
    pub(crate) fn publish(&self) {
        let counted = self.counted();
        let given = self.published.fetch_max(counted, Ordering::Relaxed);
        if counted > given {
            self.total.increment(counted - given);
        }
    }
}

impl Drop for Tally {
    /// Publishes the events counted since the last `publish`, so an owner
    /// dropped between scrapes loses none of its counts.
    fn drop(&mut self) {
        self.publish();
    }
}

impl fmt::Debug for Tally {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Tally")
            .field("counted", &self.counted())
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_count_is_a_power_of_two_outside_a_runtime() {
        assert!(count().is_power_of_two());
    }

    #[test]
    fn test_count_follows_the_runtime_workers_rounded_up() {
        let threads = 3;
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(threads)
            .build()
            .unwrap();
        assert_eq!(runtime.block_on(async { workers() }), threads);
        assert_eq!(
            runtime.block_on(async { count() }),
            threads.next_power_of_two()
        );
        let single = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();
        assert_eq!(single.block_on(async { count() }), 1);
    }

    #[test]
    fn test_sharded_mine_is_one_of_its_shards_and_stable_on_a_thread() {
        let sharded: Sharded<AtomicUsize> = Sharded::new();
        assert_eq!(sharded.iter().count(), count());
        sharded.mine().fetch_add(1, Ordering::Relaxed);
        sharded.mine().fetch_add(1, Ordering::Relaxed);
        let written: Vec<usize> = sharded
            .iter()
            .map(|shard| shard.load(Ordering::Relaxed))
            .filter(|&value| value > 0)
            .collect();
        assert_eq!(written, [2], "both writes reach one shard");
    }

    #[test]
    fn test_sharded_of_is_the_shard_a_hash_masks_to() {
        let sharded: Sharded<AtomicUsize> = Sharded::new();
        let shards: Vec<&AtomicUsize> = sharded.iter().collect();
        let wrapped = 2 * shards.len();
        for hash in 0..wrapped {
            let shard = sharded.of(crate::approx::count(hash));
            assert!(std::ptr::eq(shard, shards[hash % shards.len()]));
        }
    }

    #[test]
    fn test_slot_is_stable_on_a_thread_and_distinct_across_threads() {
        let here = slot();
        assert_eq!(slot(), here);
        let there = std::thread::spawn(slot).join().unwrap();
        assert_ne!(there, here);
    }
}
