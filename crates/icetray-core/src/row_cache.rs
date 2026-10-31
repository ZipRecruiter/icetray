//! The row cache (RFD 026): the values of key-value lookups, keyed by data
//! file, row position, and column, so a repeated hit reads no value page.
//! One cache serves every table of a server that reads through it, holds
//! each value as a copy of its own, and evicts by the bytes the values and
//! their entries take, within one budget. Nothing is invalidated: a data
//! file never changes, and a row position names the same row for as long as
//! the file exists.

use std::fmt;
use std::sync::Arc;

use arc_swap::ArcSwap;
use bytes::Bytes;
use quick_cache::sync::{Cache, DefaultLifecycle};
use quick_cache::{DefaultHashBuilder, OptionsBuilder, Weighter};

use crate::approx;
use crate::contract::VALUE_PAGE_BYTES;
use crate::index::RowPosition;
use crate::metric;
use crate::shard::Tally;

/// The largest value, in bytes, the cache keeps: one contract value page.
/// A larger value is read from its pages each time, so one wide row does not
/// displace thousands of narrow ones.
pub(crate) const ROW_CACHE_MAX_VALUE_BYTES: u64 = VALUE_PAGE_BYTES;

/// Bytes of a `quick_cache` slot: its key, hash, links, and `Bytes`
/// handle, and the key's `Arc<str>` pointer, measured at about 120 bytes
/// for rows of 100 bytes once the allocator's rounding of the value is
/// taken out. The file id's own bytes are shared by every entry of the file
/// and are not charged per entry. A ghost keeps its slot and no value, so
/// it is charged this much.
const ROW_SLOT_BYTES: u64 = 128;

/// Bytes of the shared header (24, in a 32-byte jemalloc size class) that
/// the first clone of a cached `Bytes` allocates, which every entry that is
/// read pays.
const SHARED_HEADER_BYTES: u64 = 32;

/// Bytes charged per entry on top of the value's allocation: its slot
/// (`ROW_SLOT_BYTES`) and the shared header a read allocates
/// (`SHARED_HEADER_BYTES`).
pub(crate) const ROW_ENTRY_OVERHEAD_BYTES: u64 = ROW_SLOT_BYTES + SHARED_HEADER_BYTES;

/// The weight of an average entry, which `quick_cache` divides the budget by
/// to choose how many shards to split into: a value of about a hundred
/// bytes in its size class plus `ROW_ENTRY_OVERHEAD_BYTES`. It sets the
/// shard count only; the budget is enforced by weight.
const ESTIMATED_ENTRY_BYTES: u64 = 256;

/// The ghost list's share of the entries `quick_cache` is told to expect
/// (`ESTIMATED_ENTRY_BYTES`), as the divisor of one: an eighth. A ghost is
/// the hash of a key evicted from the cold queue, kept so the key comes
/// back as hot when it is read again soon; it holds a slot of the cache's
/// own, which no entry's weight pays for, so every slot the list can hold
/// is reserved out of the budget (`GhostReserve`). An eighth reserves about
/// a sixteenth of the budget, and remembers about an eighth as many keys as
/// the cache holds rows of a hundred bytes, and more keys than it holds
/// rows from two kilobytes up.
const GHOST_ITEMS_DIVISOR: u64 = 8;

/// The part of the budget held back for the ghost list: the budget the
/// cache was built for and the bytes held back from it. In each shard
/// `quick_cache` keeps at most the shard's share of the expected entries,
/// rounded up, times the ghost share, and a resize scales that count with
/// the entries' capacity. So `Built::new` holds back that many ghosts at
/// `ROW_SLOT_BYTES` each, since a ghost keeps its entry's slot
/// and hash while dropping its key and value, and every resize keeps the
/// bytes held back in that ratio to the entries' capacity.
#[derive(Clone, Copy, Debug)]
struct GhostReserve {
    budget: u64,
    bytes: u64,
}

impl GhostReserve {
    /// The reserve for a cache given `budget` and told to expect
    /// `estimated_items` entries over `shards` shards; none for a budget of
    /// zero, whose shard is told to expect one entry and keeps no ghost.
    fn new(budget: u64, estimated_items: u64, shards: u64) -> Self {
        let ghosts = if budget == 0 {
            0
        } else {
            (estimated_items + shards).div_ceil(GHOST_ITEMS_DIVISOR)
        };
        Self {
            budget,
            bytes: ghosts * ROW_SLOT_BYTES,
        }
    }

    /// The entries' capacity within `capacity_bytes`: the most that, with
    /// the reserve at its ratio to it, `capacity_bytes` holds.
    fn entries_within(self, capacity_bytes: u64) -> u64 {
        if self.bytes == 0 {
            return capacity_bytes;
        }
        let entries = u128::from(capacity_bytes) * u128::from(self.budget)
            / (u128::from(self.budget) + u128::from(self.bytes));
        u64::try_from(entries).expect("the entries' capacity is at most capacity_bytes")
    }

    /// The bytes reserved beside an entries' capacity of `entries`.
    #[cfg(test)]
    fn beside(self, entries: u64) -> u64 {
        if self.bytes == 0 {
            return 0;
        }
        let bytes =
            (u128::from(entries) * u128::from(self.bytes)).div_ceil(u128::from(self.budget));
        u64::try_from(bytes).expect("the reserve is at most the budget beside it")
    }
}

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

/// One `quick_cache` and the part of its budget its ghost list holds back.
struct Built {
    cache: Cache<RowKey, Option<Bytes>, RowWeighter>,
    ghosts: GhostReserve,
}

impl Built {
    /// A cache for `capacity_bytes`, as `RowCache::new` describes.
    fn new(capacity_bytes: u64) -> Self {
        let estimated_items = (capacity_bytes / ESTIMATED_ENTRY_BYTES).max(1);
        let options = OptionsBuilder::new()
            .estimated_items_capacity(approx::length(estimated_items))
            .weight_capacity(capacity_bytes)
            .ghost_allocation(approx::ratio(1, GHOST_ITEMS_DIVISOR))
            .build()
            .expect("both capacities are set and the ghost share is within [0, 1]");
        let cache = Cache::with_options(
            options,
            RowWeighter,
            DefaultHashBuilder::default(),
            DefaultLifecycle::default(),
        );
        let ghosts = GhostReserve::new(
            capacity_bytes,
            estimated_items,
            approx::count(cache.num_shards()),
        );
        let built = Self { cache, ghosts };
        built.set_capacity(capacity_bytes);
        built
    }

    /// Sets the entries' capacity to what `capacity_bytes` leaves beside
    /// the ghost list's reserve at its ratio (`GhostReserve::entries_within`),
    /// rounded down to a multiple of the shards, evicting until what is
    /// held fits.
    fn set_capacity(&self, capacity_bytes: u64) {
        let shards = approx::count(self.cache.num_shards());
        let entries = self.ghosts.entries_within(capacity_bytes);
        self.cache.set_capacity(entries / shards * shards);
    }
}

/// The cache's state, shared by its clones.
struct Inner {
    /// The cache now, read once per call. A resize up from an entries'
    /// capacity of zero replaces it whole (`RowCache::resize`); every
    /// other resize changes it in place.
    built: ArcSwap<Built>,
    hits: Tally,
    misses: Tally,
}

/// The row cache (RFD 026): values keyed by file, row, and column,
/// within a byte budget, evicting what is read least. A clone is another
/// handle on the same cache.
#[derive(Clone)]
pub struct RowCache(Arc<Inner>);

impl RowCache {
    /// A cache whose entries, as `weigh` charges them, and ghost list
    /// together take at most `capacity_bytes`: the ghost list's slots are
    /// reserved out of it (`GhostReserve`) and the entries hold the rest.
    /// Its hit and miss counters are registered at zero in the recorder
    /// current now. It splits into shards chosen for `capacity_bytes`, each
    /// holding its share of the entries' part, so an entry is cached only
    /// when it fits its shard's share: a budget under one
    /// `ROW_CACHE_MAX_VALUE_BYTES` value per shard caches only smaller
    /// values, and a budget of zero, a server none of whose tables reads
    /// through the cache, caches nothing.
    #[must_use]
    pub fn new(capacity_bytes: u64) -> Self {
        Self(Arc::new(Inner {
            built: ArcSwap::from_pointee(Built::new(capacity_bytes)),
            hits: Tally::new(metrics::counter!(metric::ROW_CACHE_HITS_TOTAL)),
            misses: Tally::new(metrics::counter!(metric::ROW_CACHE_MISSES_TOTAL)),
        }))
    }

    /// What the cache holds at `key`. Counts a hit, for a null as for a
    /// value, or a miss on the calling thread's shard.
    pub(crate) fn get(&self, key: &RowKey) -> Cached {
        match self.0.built.load().cache.get(key) {
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
    pub(crate) fn insert(&self, key: RowKey, value: Option<&[u8]>) {
        if value.is_some_and(|bytes| approx::count(bytes.len()) > ROW_CACHE_MAX_VALUE_BYTES) {
            return;
        }
        self.0
            .built
            .load()
            .cache
            .insert(key, value.map(Bytes::copy_from_slice));
    }

    /// Sets the budget to `capacity_bytes`, evicting entries until what is
    /// held fits (`Built::set_capacity`). The shards stay those chosen when
    /// the cache was built, so a budget shrunk under one
    /// `ROW_CACHE_MAX_VALUE_BYTES` value per shard caches only smaller
    /// values until it grows again. A cache whose entries' capacity is zero
    /// holds nothing, and `quick_cache` grows such a cache with no room for
    /// a ghost list ever again, so growing it from zero builds the cache
    /// `new` would for `capacity_bytes` in its place instead, which every
    /// clone reads from its next call on.
    pub fn resize(&self, capacity_bytes: u64) {
        let built = self.0.built.load();
        if built.cache.capacity() > 0 {
            built.set_capacity(capacity_bytes);
        } else if capacity_bytes > 0 {
            self.0.built.store(Arc::new(Built::new(capacity_bytes)));
        }
    }

    /// How the cache is built now: what a test compares with a freshly
    /// built cache's.
    #[cfg(test)]
    fn shape(&self) -> Shape {
        let built = self.0.built.load();
        Shape {
            shards: built.cache.num_shards(),
            entries_capacity: built.cache.capacity(),
            ghost_bytes: built.ghosts.bytes,
        }
    }

    /// Bytes charged against the budget now: each entry as `weigh` charges
    /// it.
    #[must_use]
    pub fn usage(&self) -> u64 {
        self.0.built.load().cache.weight()
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

/// How a cache is built: its shards, its entries' capacity, and the bytes
/// its ghost list holds back.
#[cfg(test)]
#[derive(Debug, PartialEq, Eq)]
struct Shape {
    shards: usize,
    entries_capacity: u64,
    ghost_bytes: u64,
}

impl fmt::Debug for RowCache {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RowCache")
            .field("usage", &self.usage())
            .field("capacity", &self.0.built.load().cache.capacity())
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use bytes::Bytes;

    use super::*;
    use crate::approx::units::KIB;
    use crate::index::RowPosition;
    use crate::metric::testing::{Series, recorded_with};

    const FILE: &str = "s3://bucket/data/a.parquet";
    /// A budget that holds every entry a test makes, in each of the shards
    /// it splits into.
    const ROOMY_BYTES: u64 = 1 << 30;
    /// Entries of 1 KiB values a cache sized to hold this many rows of
    /// them is told to expect, so it splits into several shards.
    const SHARD_ROWS_ESTIMATE: u64 = 4096;

    fn file() -> Arc<str> {
        Arc::from(FILE)
    }

    fn key(position: u64, leaf: usize) -> RowKey {
        RowKey::new(&file(), RowPosition(position), leaf)
    }

    /// Rows of a kibibyte inserted into one shard of a cache, the shard of
    /// `key(0, 0)`, each at a position not inserted before: eviction is per
    /// shard, so these rows displace one another and nothing else.
    struct OneShard<'a> {
        cache: &'a RowCache,
        shard: usize,
        /// The rows of a kibibyte the shard holds.
        rows: u64,
        next_position: u64,
    }

    impl<'a> OneShard<'a> {
        fn of(cache: &'a RowCache) -> Self {
            let built = cache.0.built.load();
            Self {
                cache,
                shard: built.cache.shard_index(&key(0, 0)),
                rows: built.cache.shard_capacity() / weigh(Some(approx::length(KIB))),
                next_position: 0,
            }
        }

        /// Inserts `rows` new rows into the shard and returns their keys.
        fn insert(&mut self, rows: u64) -> Vec<RowKey> {
            let value = vec![0_u8; approx::length(KIB)];
            let mut inserted = Vec::new();
            while approx::count(inserted.len()) < rows {
                self.next_position += 1;
                let candidate = key(self.next_position, 0);
                if self.cache.0.built.load().cache.shard_index(&candidate) == self.shard {
                    self.cache.insert(candidate.clone(), Some(&value));
                    inserted.push(candidate);
                }
            }
            inserted
        }
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
        let capacity = 64 * weigh(Some(approx::length(KIB)));
        let cache = RowCache::new(capacity);
        let value = vec![0_u8; approx::length(KIB)];
        for position in 0..10 * capacity / KIB {
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
        assert_eq!(weigh(Some(100)), 112 + 160);
        assert_eq!(weigh(Some(1029)), 1280 + 160);
        assert_eq!(weigh(None), 160);
    }

    /// A row hit again after it was cached outlives any number of rows read
    /// once into its shard after it, while a row cached beside it and never
    /// read again leaves: a pass over many rows, as a large `MultiGet`
    /// makes, does not push out a row that keeps being asked for.
    #[test]
    fn test_a_row_read_again_outlives_rows_read_once() {
        let cache = RowCache::new(SHARD_ROWS_ESTIMATE * weigh(Some(approx::length(KIB))));
        let mut shard = OneShard::of(&cache);
        let shard_rows = shard.rows;
        shard.insert(shard_rows);
        let [hit_again] = <[RowKey; 1]>::try_from(shard.insert(1)).unwrap();
        assert_ne!(cache.get(&hit_again), Cached::Absent);
        let [read_once] = <[RowKey; 1]>::try_from(shard.insert(1)).unwrap();
        let pass = shard.insert(2 * shard_rows);
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
        let capacity = 256 * weigh(Some(approx::length(KIB)));
        let cache = RowCache::new(capacity);
        let value = vec![0_u8; approx::length(KIB)];
        for position in 0..capacity / KIB {
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

    /// A large cache shrunk to zero empties and caches nothing, and grown
    /// back it caches the largest value again; shrunk to half of one
    /// largest value per shard it caches small values and not the largest,
    /// and grown back it caches the largest again.
    #[test]
    fn test_resize_to_zero_and_back_caches_what_each_budget_holds() {
        let largest = vec![0_u8; approx::length(ROW_CACHE_MAX_VALUE_BYTES)];
        let small = b"a value";
        let cache = RowCache::new(ROOMY_BYTES);
        cache.insert(key(1, 0), Some(&largest));
        assert_ne!(cache.get(&key(1, 0)), Cached::Absent);
        cache.resize(0);
        assert_eq!(cache.usage(), 0);
        cache.insert(key(2, 0), Some(small));
        assert_eq!(cache.get(&key(2, 0)), Cached::Absent);
        cache.resize(ROOMY_BYTES);
        cache.insert(key(3, 0), Some(&largest));
        assert_ne!(cache.get(&key(3, 0)), Cached::Absent);

        let cache = RowCache::new(ROOMY_BYTES);
        let built = cache.0.built.load_full();
        let shards = approx::count(built.cache.num_shards());
        let half_a_largest_per_shard = shards * weigh(Some(largest.len())) / 2;
        cache.resize(
            half_a_largest_per_shard + built.ghosts.beside(half_a_largest_per_shard) + shards,
        );
        cache.insert(key(4, 0), Some(&largest));
        assert_eq!(cache.get(&key(4, 0)), Cached::Absent);
        cache.insert(key(5, 0), Some(small));
        assert_eq!(
            cache.get(&key(5, 0)),
            Cached::Value(Bytes::from_static(small))
        );
        cache.resize(ROOMY_BYTES);
        cache.insert(key(6, 0), Some(&largest));
        assert_ne!(cache.get(&key(6, 0)), Cached::Absent);
    }

    /// The ghost list's slots are reserved out of the budget at what
    /// `quick_cache` keeps at most, about a sixteenth of it, so the entries
    /// and the ghosts together hold no more than the budget, at the budget
    /// `new` was given and at a smaller or a larger one alike, since the
    /// ghost list scales with the entries.
    #[test]
    fn test_the_ghost_list_is_reserved_out_of_the_budget() {
        let cache = RowCache::new(ROOMY_BYTES);
        let shards = approx::count(cache.0.built.load().cache.num_shards());
        let ghosts = (ROOMY_BYTES / ESTIMATED_ENTRY_BYTES + shards).div_ceil(GHOST_ITEMS_DIVISOR);
        assert_eq!(cache.0.built.load().ghosts.bytes, ghosts * ROW_SLOT_BYTES);
        let under_a_fifteenth = 15;
        assert!(
            cache.0.built.load().ghosts.bytes * under_a_fifteenth < ROOMY_BYTES,
            "the reserve is about a sixteenth of the budget"
        );
        for budget in [ROOMY_BYTES, ROOMY_BYTES / 2, 2 * ROOMY_BYTES] {
            cache.resize(budget);
            let entries = cache.0.built.load().cache.capacity();
            let held = entries + cache.0.built.load().ghosts.beside(entries);
            assert!(held <= budget, "{held} over {budget}");
            assert!(
                budget - held <= shards,
                "the entries hold the rest, down to a multiple of the shards"
            );
        }
    }

    /// A budget of zero reserves nothing: its cache holds nothing, and so
    /// remembers none of what it evicts.
    #[test]
    fn test_a_zero_budget_reserves_no_ghosts() {
        let cache = RowCache::new(0);
        assert_eq!(cache.0.built.load().ghosts.bytes, 0);
    }

    /// Whether `cache` remembers a row evicted from its cold queue: the row,
    /// cached again soon after its eviction, comes back as hot and outlives
    /// a pass of rows read once twice its shard's size, which a row cached
    /// cold does not.
    fn readmits_a_ghost_as_hot(cache: &RowCache) -> bool {
        let mut shard = OneShard::of(cache);
        let shard_rows = shard.rows;
        shard.insert(shard_rows);
        let [evicted] = <[RowKey; 1]>::try_from(shard.insert(1)).unwrap();
        let mut pushed = 0;
        while cache.0.built.load().cache.contains_key(&evicted) {
            assert!(
                pushed <= 2 * shard_rows,
                "the row never left the cold queue"
            );
            shard.insert(1);
            pushed += 1;
        }
        cache.insert(evicted.clone(), Some(&vec![0_u8; approx::length(KIB)]));
        shard.insert(2 * shard_rows);
        cache.0.built.load().cache.contains_key(&evicted)
    }

    /// A cache resized up from zero is the cache `new` builds for the new
    /// budget: its shards, its entries' capacity, and its ghost list's
    /// reserve, and its ghost list remembers evicted rows, whether it was
    /// built at zero, as for a server whose tables read pages alone, or
    /// shrunk to zero from a budget of its own.
    #[test]
    fn test_resize_from_zero_builds_the_cache_new_builds() {
        let budget = SHARD_ROWS_ESTIMATE * weigh(Some(approx::length(KIB)));
        let fresh = RowCache::new(budget);
        assert!(
            readmits_a_ghost_as_hot(&fresh),
            "a fresh cache keeps ghosts"
        );
        let built_at_zero = RowCache::new(0);
        let shrunk_to_zero = RowCache::new(budget);
        shrunk_to_zero.resize(0);
        for (grown, from) in [(built_at_zero, "built at zero"), (shrunk_to_zero, "shrunk")] {
            grown.resize(budget);
            assert_eq!(grown.shape(), RowCache::new(budget).shape(), "{from}");
            assert!(readmits_a_ghost_as_hot(&grown), "{from}");
        }
    }

    /// The counters are registered at zero when the cache is made, a scrape's
    /// `publish` moves the hits and misses counted into them once, and sets
    /// `row_cache_bytes` to `usage`.
    #[test]
    fn test_publish_moves_the_counts_into_the_series() {
        let hits = 2;
        let ((), published) = recorded_with(|snapshotter| async move {
            let cache = RowCache::new(ROOMY_BYTES);
            let registered = Series::of(&snapshotter);
            for name in [metric::ROW_CACHE_HITS_TOTAL, metric::ROW_CACHE_MISSES_TOTAL] {
                assert_eq!(registered.counter(name, &[]), 0, "{name}");
            }
            assert_eq!(
                registered.named(metric::ROW_CACHE_BYTES),
                0,
                "set by a scrape"
            );
            assert_eq!(cache.get(&key(1, 0)), Cached::Absent);
            cache.insert(key(1, 0), None);
            for _ in 0..hits {
                assert_eq!(cache.get(&key(1, 0)), Cached::Null);
            }
            cache.publish();
            cache.publish();
        });
        assert_eq!(published.counter(metric::ROW_CACHE_HITS_TOTAL, &[]), hits);
        assert_eq!(published.counter(metric::ROW_CACHE_MISSES_TOTAL, &[]), 1);
        assert_eq!(
            published.gauge(metric::ROW_CACHE_BYTES, &[]).to_bits(),
            approx::gauge(weigh(None)).to_bits()
        );
    }
}
