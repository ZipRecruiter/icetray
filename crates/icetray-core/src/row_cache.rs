//! The row cache (RFD 026): the values of key-value lookups, keyed by data
//! file, row position, and column, so a repeated hit reads no value page.
//! One cache serves every table of a server that reads through it, holds
//! each value as a copy of its own, and evicts by the bytes the values and
//! their entries take, within one budget. Nothing is invalidated: a data
//! file never changes, and a row position names the same row for as long as
//! the file exists.

use std::fmt;
use std::sync::Arc;

use bytes::Bytes;
use quick_cache::Weighter;
use quick_cache::sync::Cache;

use crate::approx;
use crate::contract::VALUE_PAGE_BYTES;
use crate::index::RowPosition;
use crate::metric;
use crate::shard::Tally;

/// The largest value, in bytes, the cache keeps: one contract value page.
/// A larger value is read from its pages each time, so one wide row does not
/// displace thousands of narrow ones.
pub(crate) const ROW_CACHE_MAX_VALUE_BYTES: u64 = VALUE_PAGE_BYTES;

/// Bytes charged per entry on top of the value's allocation: `quick_cache`'s
/// slot (key, hash, links, `Bytes` handle) and the key's `Arc<str>`
/// pointer, measured at about 120 bytes for rows of 100 bytes once the
/// allocator's rounding of the value is taken out. The file id's own bytes
/// are shared by every entry of the file and are not charged per entry.
pub(crate) const ROW_ENTRY_OVERHEAD_BYTES: u64 = 128;

/// The weight of an average entry, which `quick_cache` divides the budget by
/// to choose how many shards to split into: a value of about a hundred
/// bytes in its size class plus `ROW_ENTRY_OVERHEAD_BYTES`. It sets the
/// shard count only; the budget is enforced by weight.
const ESTIMATED_ENTRY_BYTES: u64 = 256;

/// The smallest allocation jemalloc makes and the finest spacing of its
/// size classes: sizes up to this round to it, and sizes above round to a
/// multiple of at least this.
const MIN_SIZE_CLASS_BYTES: usize = 16;

/// jemalloc splits each doubling of the size into `1 << SIZE_CLASS_BITS`
/// classes, four.
const SIZE_CLASS_BITS: u32 = 2;

/// The bytes jemalloc allocates for a request of `len` bytes: `len` rounded
/// up to its size class, a multiple of `MIN_SIZE_CLASS_BYTES` for small
/// sizes and of a quarter of the size's power of two above them. Nothing is
/// allocated for no bytes.
fn size_class(len: usize) -> usize {
    if len == 0 {
        return 0;
    }
    if len <= MIN_SIZE_CLASS_BYTES {
        return MIN_SIZE_CLASS_BYTES;
    }
    let spacing =
        (1_usize << (len - 1).ilog2().saturating_sub(SIZE_CLASS_BITS)).max(MIN_SIZE_CLASS_BYTES);
    len.next_multiple_of(spacing)
}

/// What an entry holding a value of `value_len` bytes (`None` for a cached
/// null) is charged against the budget: the allocation the value's copy
/// takes, in jemalloc's size class, plus `ROW_ENTRY_OVERHEAD_BYTES`.
pub(crate) fn weigh(value_len: Option<usize>) -> u64 {
    approx::count(size_class(value_len.unwrap_or(0))) + ROW_ENTRY_OVERHEAD_BYTES
}

/// One cached value's address: a data file's id, a row's position in it,
/// and a value column's leaf in the file's schema. The leaf, not the
/// column's place in a table's layout, so two tables listing the same
/// file's columns in different orders share entries.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) struct RowKey {
    file: Arc<str>,
    position: u64,
    leaf: u32,
}

impl RowKey {
    /// The key of the value at `leaf` of the row at `position` in `file`.
    #[cfg_attr(
        not(test),
        expect(dead_code, reason = "read by the lookup path, a later task")
    )]
    pub(crate) fn new(file: &Arc<str>, position: RowPosition, leaf: usize) -> Self {
        Self {
            file: Arc::clone(file),
            position: position.0,
            leaf: u32::try_from(leaf).expect("a schema's leaf index fits a u32"),
        }
    }
}

/// What `RowCache::get` finds at a key: a row the cache does not hold, a
/// null it holds, or a value it holds. A null is a value the lookup read, so
/// it is told apart from a row never read.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum Cached {
    /// The cache holds nothing at the key.
    Absent,
    /// The cache holds a null at the key.
    Null,
    /// The cache holds these bytes at the key.
    Value(Bytes),
}

/// Weighs an entry by `weigh`.
#[derive(Clone)]
struct RowWeighter;

impl Weighter<RowKey, Option<Bytes>> for RowWeighter {
    fn weight(&self, _key: &RowKey, value: &Option<Bytes>) -> u64 {
        weigh(value.as_ref().map(Bytes::len))
    }
}

/// The cache's state, shared by its clones.
struct Inner {
    cache: Cache<RowKey, Option<Bytes>, RowWeighter>,
    hits: Tally,
    misses: Tally,
}

/// The row cache (RFD 026): values keyed by file, row, and column,
/// within a byte budget, evicting what is read least. A clone is another
/// handle on the same cache.
#[derive(Clone)]
pub struct RowCache(Arc<Inner>);

impl RowCache {
    /// A cache that holds entries up to `capacity_bytes` as `weigh` charges
    /// them. Its hit and miss counters are registered at zero in the
    /// recorder current now. It splits into shards chosen for
    /// `capacity_bytes` and kept for its life, each holding its share of
    /// the budget, so an entry is cached only when it fits its shard's
    /// share: a budget under one `ROW_CACHE_MAX_VALUE_BYTES` value per
    /// shard caches only smaller values, and a budget of zero, a server
    /// none of whose tables reads through the cache, caches nothing.
    #[must_use]
    pub fn new(capacity_bytes: u64) -> Self {
        let estimated_items = approx::length(capacity_bytes / ESTIMATED_ENTRY_BYTES).max(1);
        Self(Arc::new(Inner {
            cache: Cache::with_weighter(estimated_items, capacity_bytes, RowWeighter),
            hits: Tally::new(metrics::counter!(metric::ROW_CACHE_HITS_TOTAL)),
            misses: Tally::new(metrics::counter!(metric::ROW_CACHE_MISSES_TOTAL)),
        }))
    }

    /// What the cache holds at `key`. Counts a hit, for a null as for a
    /// value, or a miss on the calling thread's shard.
    #[cfg_attr(
        not(test),
        expect(dead_code, reason = "read by the lookup path, a later task")
    )]
    pub(crate) fn get(&self, key: &RowKey) -> Cached {
        match self.0.cache.get(key) {
            Some(Some(bytes)) => {
                self.0.hits.increment();
                Cached::Value(bytes)
            }
            Some(None) => {
                self.0.hits.increment();
                Cached::Null
            }
            None => {
                self.0.misses.increment();
                Cached::Absent
            }
        }
    }

    /// Caches a copy of `value` at `key`, a null for `None`, so the entry
    /// keeps none of the page the value was read from alive. A value over
    /// `ROW_CACHE_MAX_VALUE_BYTES` is not cached.
    #[cfg_attr(
        not(test),
        expect(dead_code, reason = "read by the lookup path, a later task")
    )]
    pub(crate) fn insert(&self, key: RowKey, value: Option<&[u8]>) {
        if value.is_some_and(|bytes| approx::count(bytes.len()) > ROW_CACHE_MAX_VALUE_BYTES) {
            return;
        }
        self.0.cache.insert(key, value.map(Bytes::copy_from_slice));
    }

    /// Sets the budget to `capacity_bytes`, evicting entries until what is
    /// held fits. The shards stay those `new` chose, so a budget shrunk
    /// under one `ROW_CACHE_MAX_VALUE_BYTES` value per shard caches only
    /// smaller values until it grows again.
    pub fn resize(&self, capacity_bytes: u64) {
        self.0.cache.set_capacity(capacity_bytes);
    }

    /// Bytes charged against the budget now: each entry as `weigh` charges
    /// it.
    #[must_use]
    pub fn usage(&self) -> u64 {
        self.0.cache.weight()
    }

    /// Moves the hits and misses counted since the last call into
    /// `row_cache_hits_total` and `row_cache_misses_total`, and sets
    /// `row_cache_bytes` to `usage`; a scrape calls this, so the counters
    /// read every lookup before it and the gauge reads evictions as well as
    /// inserts. The last clone dropped publishes the counts that remain.
    pub fn publish(&self) {
        self.0.hits.publish();
        self.0.misses.publish();
        metrics::gauge!(metric::ROW_CACHE_BYTES).set(approx::gauge(self.usage()));
    }
}

impl fmt::Debug for RowCache {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RowCache")
            .field("usage", &self.usage())
            .field("capacity", &self.0.cache.capacity())
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use bytes::Bytes;
    use metrics_util::debugging::{DebugValue, DebuggingRecorder};

    use super::*;
    use crate::index::RowPosition;

    const FILE: &str = "s3://bucket/data/a.parquet";
    /// A budget that holds every entry a test makes, in each of the shards
    /// it splits into.
    const ROOMY_BYTES: u64 = 1 << 30;
    const KIB: usize = 1024;
    /// Entries of 1 KiB values a cache sized to hold this many rows of
    /// them is told to expect, so it splits into several shards.
    const SHARD_ROWS_ESTIMATE: u64 = 4096;

    fn file() -> Arc<str> {
        Arc::from(FILE)
    }

    fn key(position: u64, leaf: usize) -> RowKey {
        RowKey::new(&file(), RowPosition(position), leaf)
    }

    #[test]
    fn test_a_value_inserted_is_read_back_whole() {
        let cache = RowCache::new(ROOMY_BYTES);
        let value = b"a value of some bytes";
        cache.insert(key(7, 2), Some(value));
        assert_eq!(
            cache.get(&key(7, 2)),
            Cached::Value(Bytes::from_static(value))
        );
    }

    #[test]
    fn test_a_null_is_cached_apart_from_a_miss() {
        let cache = RowCache::new(ROOMY_BYTES);
        assert_eq!(cache.get(&key(7, 2)), Cached::Absent);
        cache.insert(key(7, 2), None);
        assert_eq!(cache.get(&key(7, 2)), Cached::Null);
    }

    #[test]
    fn test_keys_differing_only_in_file_position_or_leaf_are_apart() {
        let cache = RowCache::new(ROOMY_BYTES);
        let other_file: Arc<str> = Arc::from("s3://bucket/data/b.parquet");
        let keys = [
            key(7, 2),
            RowKey::new(&other_file, RowPosition(7), 2),
            key(8, 2),
            key(7, 3),
        ];
        for (index, key) in keys.iter().enumerate() {
            cache.insert(key.clone(), Some(&[u8::try_from(index).unwrap()]));
        }
        for (index, key) in keys.iter().enumerate() {
            let expected = Bytes::copy_from_slice(&[u8::try_from(index).unwrap()]);
            assert_eq!(cache.get(key), Cached::Value(expected));
        }
    }

    #[test]
    fn test_a_value_over_the_bound_is_not_cached() {
        let cache = RowCache::new(ROOMY_BYTES);
        let over = vec![0_u8; approx::length(ROW_CACHE_MAX_VALUE_BYTES) + 1];
        cache.insert(key(1, 0), Some(&over));
        assert_eq!(cache.get(&key(1, 0)), Cached::Absent);
        assert_eq!(cache.usage(), 0);
        let at = vec![0_u8; approx::length(ROW_CACHE_MAX_VALUE_BYTES)];
        cache.insert(key(2, 0), Some(&at));
        assert!(
            cache.get(&key(2, 0)) != Cached::Absent,
            "the bound itself is cached"
        );
    }

    #[test]
    fn test_the_cache_holds_its_budget() {
        let capacity = 64 * weigh(Some(KIB));
        let cache = RowCache::new(capacity);
        let value = vec![0_u8; KIB];
        for position in 0..10 * capacity / approx::count(KIB) {
            cache.insert(key(position, 0), Some(&value));
        }
        assert!(cache.usage() > 0, "the cache holds something");
        assert!(
            cache.usage() <= capacity,
            "{} over {capacity}",
            cache.usage()
        );
    }

    #[test]
    fn test_weigh_rounds_to_jemalloc_size_classes() {
        assert_eq!(weigh(Some(100)), 112 + 128);
        assert_eq!(weigh(Some(1029)), 1280 + 128);
        assert_eq!(weigh(None), 128);
    }

    /// A row hit again after it was cached outlives any number of rows read
    /// once into its shard after it, while a row cached beside it and never
    /// read again leaves: a pass over many rows, as a large `MultiGet`
    /// makes, does not push out a row that keeps being asked for.
    #[test]
    fn test_a_row_read_again_outlives_rows_read_once() {
        let cache = RowCache::new(SHARD_ROWS_ESTIMATE * weigh(Some(KIB)));
        let value = vec![0_u8; KIB];
        let shard = cache.0.cache.shard_index(&key(0, 0));
        let shard_rows = cache.0.cache.shard_capacity() / weigh(Some(KIB));
        let mut next_position = 0;
        let mut insert_in_shard = |rows: u64| {
            let mut inserted = Vec::new();
            while approx::count(inserted.len()) < rows {
                next_position += 1;
                let candidate = key(next_position, 0);
                if cache.0.cache.shard_index(&candidate) == shard {
                    cache.insert(candidate.clone(), Some(&value));
                    inserted.push(candidate);
                }
            }
            inserted
        };
        insert_in_shard(shard_rows);
        let [hit_again, read_once] = <[RowKey; 2]>::try_from(insert_in_shard(2)).unwrap();
        assert_ne!(cache.get(&hit_again), Cached::Absent);
        let pass = insert_in_shard(2 * shard_rows);
        assert!(
            pass.iter().any(|row| cache.get(row) == Cached::Absent),
            "the shard held every row of the pass, so nothing was displaced"
        );
        assert!(
            cache.get(&read_once) == Cached::Absent,
            "the row read once was kept"
        );
        assert!(
            cache.get(&hit_again) != Cached::Absent,
            "the row hit again was evicted"
        );
    }

    #[test]
    fn test_resize_shrinks_usage_to_the_new_capacity() {
        let capacity = 256 * weigh(Some(KIB));
        let cache = RowCache::new(capacity);
        let value = vec![0_u8; KIB];
        for position in 0..capacity / approx::count(KIB) {
            cache.insert(key(position, 0), Some(&value));
        }
        let full = cache.usage();
        let smaller = capacity / 4;
        assert!(full > smaller, "the cache filled past the new capacity");
        cache.resize(smaller);
        assert!(cache.usage() <= smaller, "{} over {smaller}", cache.usage());
        assert!(cache.usage() > 0, "the cache keeps what fits");
    }

    /// A budget of zero caches nothing, a null included.
    #[test]
    fn test_a_zero_budget_caches_nothing() {
        let cache = RowCache::new(0);
        cache.insert(key(1, 0), Some(b"a value"));
        cache.insert(key(2, 0), None);
        assert_eq!(cache.get(&key(1, 0)), Cached::Absent);
        assert_eq!(cache.get(&key(2, 0)), Cached::Absent);
        assert_eq!(cache.usage(), 0);
    }

    /// A large cache shrunk to zero empties and caches nothing; at half of
    /// one largest value per shard it caches small values and not the
    /// largest; grown back it caches the largest again.
    #[test]
    fn test_resize_to_zero_and_back_caches_what_each_budget_holds() {
        let cache = RowCache::new(ROOMY_BYTES);
        let largest = vec![0_u8; approx::length(ROW_CACHE_MAX_VALUE_BYTES)];
        let small = b"a value";
        let shards = approx::count(cache.0.cache.num_shards());
        cache.insert(key(1, 0), Some(&largest));
        assert_ne!(cache.get(&key(1, 0)), Cached::Absent);

        cache.resize(0);
        assert_eq!(cache.usage(), 0);
        cache.insert(key(2, 0), Some(small));
        assert_eq!(cache.get(&key(2, 0)), Cached::Absent);

        cache.resize(shards * weigh(Some(largest.len())) / 2);
        cache.insert(key(3, 0), Some(&largest));
        assert_eq!(cache.get(&key(3, 0)), Cached::Absent);
        cache.insert(key(4, 0), Some(small));
        assert_eq!(
            cache.get(&key(4, 0)),
            Cached::Value(Bytes::from_static(small))
        );

        cache.resize(ROOMY_BYTES);
        cache.insert(key(5, 0), Some(&largest));
        assert_ne!(cache.get(&key(5, 0)), Cached::Absent);
    }

    /// The counters are registered at zero when the cache is made, a scrape's
    /// `publish` moves the hits and misses counted into them once, and sets
    /// `row_cache_bytes` to `usage`.
    #[test]
    fn test_publish_moves_the_counts_into_the_series() {
        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();
        // A snapshot takes the counters it reads, so each test step reads
        // every series from one.
        let series = || -> HashMap<String, DebugValue> {
            snapshotter
                .snapshot()
                .into_vec()
                .into_iter()
                .map(|(key, .., value)| (key.key().name().to_owned(), value))
                .collect()
        };
        metrics::with_local_recorder(&recorder, || {
            let cache = RowCache::new(ROOMY_BYTES);
            let registered = series();
            for name in [metric::ROW_CACHE_HITS_TOTAL, metric::ROW_CACHE_MISSES_TOTAL] {
                assert_eq!(registered[name], DebugValue::Counter(0), "{name}");
            }
            assert!(
                !registered.contains_key(metric::ROW_CACHE_BYTES),
                "set by a scrape"
            );
            assert_eq!(cache.get(&key(1, 0)), Cached::Absent);
            cache.insert(key(1, 0), None);
            let hits = 2;
            for _ in 0..hits {
                assert_eq!(cache.get(&key(1, 0)), Cached::Null);
            }
            cache.publish();
            cache.publish();
            let published = series();
            assert_eq!(
                published[metric::ROW_CACHE_HITS_TOTAL],
                DebugValue::Counter(hits)
            );
            assert_eq!(
                published[metric::ROW_CACHE_MISSES_TOTAL],
                DebugValue::Counter(1)
            );
            let DebugValue::Gauge(bytes) = published[metric::ROW_CACHE_BYTES] else {
                panic!("row_cache_bytes is a gauge");
            };
            assert_eq!(
                bytes.into_inner().to_bits(),
                approx::gauge(weigh(None)).to_bits()
            );
        });
    }
}
