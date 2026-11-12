//! The row cache (RFD 026): the values of key-value lookups, and the
//! presence of rows presence-only lookups found, keyed by table, table key,
//! and column, so a repeated hit resolves no key and reads no page. Each
//! entry records the loaded file and the row it was read from, and a
//! lookup serves it only while its generation holds that file and has not
//! deleted the row (`Generation::lookup`), so nothing is invalidated. One
//! cache serves every table of a server that reads through it, holds each
//! entry as one allocation of its own, and evicts by the bytes the entries
//! take, within one budget.

use std::collections::HashMap;
use std::fmt;
use std::hash::{Hash, Hasher};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, PoisonError};

use arc_swap::ArcSwap;
use bytes::Bytes;
use icetray_slab::jemalloc_class;
use quick_cache::sync::{Cache, DefaultLifecycle};
use quick_cache::{DefaultHashBuilder, Equivalent, OptionsBuilder, Weighter};

use crate::approx;
use crate::contract::VALUE_PAGE_BYTES;
use crate::generation::FileNumber;
use crate::index::RowPosition;
use crate::key::Key;
use crate::metric;
use crate::shard::Tally;

/// The largest value, in bytes, the cache keeps: one contract value page.
/// A larger value is read from its pages each time, so one wide row does not
/// displace thousands of narrow ones.
pub(crate) const ROW_CACHE_MAX_VALUE_BYTES: u64 = VALUE_PAGE_BYTES;

/// Bytes of a `quick_cache` slot beside the entry's key and value: its
/// hash and links. A slot holding two 32-byte handles measured about 120
/// bytes for rows of 100 bytes once the allocator's rounding of the value
/// was taken out, so about 56, rounded up.
const QUICK_CACHE_SLOT_BYTES: usize = 64;

/// Bytes of a slot: `quick_cache`'s own (`QUICK_CACHE_SLOT_BYTES`), the
/// entry's key handle, and its value, which holds where the value was read
/// and a handle on the value's bytes. A ghost keeps its slot and no entry,
/// so it is charged this much.
const ROW_SLOT_BYTES: u64 =
    approx::count(QUICK_CACHE_SLOT_BYTES + size_of::<EntryKey>() + size_of::<EntryValue>());

/// Bytes of the shared header the first clone of a cached value's `Bytes`
/// allocates, which every value entry that is read pays: `bytes`' `Shared`,
/// a buffer pointer, its capacity, and a reference count.
const SHARED_HEADER_LEN: usize = 24;

/// What the shared header takes in jemalloc's size class
/// (`SHARED_HEADER_LEN`).
const SHARED_HEADER_BYTES: u64 = approx::count(jemalloc_class(SHARED_HEADER_LEN));

/// The weight of an average entry, which `quick_cache` divides the budget by
/// to choose how many shards to split into: what a value of about a
/// hundred bytes is charged (`charged`), rounded down to twice `ROW_SLOT_BYTES`,
/// so the ghost list's reserve is a sixteenth of the budget
/// (`GHOST_ITEMS_DIVISOR`). It sets the shard count and the reserve only;
/// the budget is enforced by weight.
const ESTIMATED_ENTRY_BYTES: u64 = 2 * ROW_SLOT_BYTES;

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

/// What an entry is charged against the budget: its key's allocation of
/// `key_len` bytes and its value's of `value_len` bytes (`None` for a
/// cached null or a presence entry, which allocates no value), each in
/// jemalloc's size class, its slot (`ROW_SLOT_BYTES`), and, for a value,
/// the shared header a read allocates (`SHARED_HEADER_BYTES`).
fn charged(key_len: usize, value_len: Option<usize>) -> u64 {
    let value = value_len.map_or(0, |len| {
        approx::count(jemalloc_class(len)) + SHARED_HEADER_BYTES
    });
    approx::count(jemalloc_class(key_len)) + value + ROW_SLOT_BYTES
}

/// What an entry for a table key of `key_len` bytes and a value of
/// `value_len` bytes (`None` for a cached null or a presence entry) is
/// charged (`charged`): its key's allocation holds `KEY_HEAD_BYTES` before
/// the table key.
#[cfg(test)]
pub(crate) fn weigh(key_len: usize, value_len: Option<usize>) -> u64 {
    charged(KEY_HEAD_BYTES + key_len, value_len)
}

/// Bytes of an entry key's allocation before its table key: the table's
/// number and the part of the row the entry is for (`Part::code`), four
/// bytes each.
const KEY_HEAD_BYTES: usize = 8;

/// What of a table key's row an entry is for: one of the table's value
/// columns, by its place in the table's layout, or, for a presence-only
/// lookup, the row itself.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Part {
    /// The value column at this place in the layout.
    Column(usize),
    /// The row, for a presence-only lookup.
    Presence,
}

impl Part {
    /// The part as an entry stores it: a column's place, or `u32::MAX`
    /// for the row.
    fn code(self) -> u32 {
        match self {
            Self::Column(column) => {
                let code = u32::try_from(column).expect("a table's value columns fit a u32");
                assert!(code < u32::MAX, "a table's value columns fit a u32");
                code
            }
            Self::Presence => u32::MAX,
        }
    }
}

/// Where an entry's value was read: the loaded file, by its number
/// (`FileNumber`), and the row's position in it. A lookup serves the entry
/// only while its generation holds that file and has not deleted the row.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Source {
    /// The loaded file the row was read from.
    pub(crate) file: FileNumber,
    /// The row's position in the file.
    pub(crate) position: RowPosition,
}

/// What an entry holds for its part of the row: a column's null, a
/// column's value, or, for a presence entry, that the row is there.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum Held {
    /// The column is null in the row.
    Null,
    /// The column's value in the row.
    Value(Bytes),
    /// The row is there; a presence entry holds no value.
    Present,
}

/// What a lookup caches for one part of a row, borrowed from what it read:
/// a column's value, `None` for its null, or a row's presence.
#[derive(Clone, Copy, Debug)]
pub(crate) enum Caching<'a> {
    /// A column's value, or `None` for its null.
    Column(Option<&'a [u8]>),
    /// The row's presence, for a presence-only lookup.
    Presence,
}

/// Whether `TableRows::insert` caches `held`: everything but a value over
/// `ROW_CACHE_MAX_VALUE_BYTES`.
pub(crate) fn caches(held: Caching<'_>) -> bool {
    match held {
        Caching::Column(Some(bytes)) => approx::count(bytes.len()) <= ROW_CACHE_MAX_VALUE_BYTES,
        Caching::Column(None) | Caching::Presence => true,
    }
}

/// One entry: where it was read and what it holds.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Entry {
    /// Where the entry was read.
    pub(crate) source: Source,
    /// What it holds.
    pub(crate) held: Held,
}

/// Feeds `hasher` an entry's address the one way both `EntryKey` and
/// `Address` hash it, so a borrowed address finds a stored key.
fn hash_address<H: Hasher>(table: u32, part: u32, key: &[u8], hasher: &mut H) {
    hasher.write_u32(table);
    hasher.write_u32(part);
    hasher.write(key);
}

/// A stored entry's key: its table's number, its part's code, and the
/// table key's bytes (`Key::as_bytes`), in an allocation apart from the
/// value's.
#[derive(Clone, Debug, PartialEq, Eq)]
struct EntryKey(Box<[u8]>);

impl EntryKey {
    /// The four bytes at `at` as a number.
    fn word(&self, at: usize) -> u32 {
        let bytes: [u8; 4] = self.0[at..at + 4]
            .try_into()
            .expect("an entry key holds its head");
        u32::from_le_bytes(bytes)
    }

    fn table(&self) -> u32 {
        self.word(0)
    }

    fn part(&self) -> u32 {
        self.word(4)
    }

    fn key(&self) -> &[u8] {
        &self.0[KEY_HEAD_BYTES..]
    }
}

impl Hash for EntryKey {
    fn hash<H: Hasher>(&self, hasher: &mut H) {
        hash_address(self.table(), self.part(), self.key(), hasher);
    }
}

/// An entry's address as a lookup holds it, borrowed, so finding an entry
/// allocates nothing.
#[derive(Clone, Copy)]
struct Address<'a> {
    table: u32,
    part: u32,
    key: &'a [u8],
}

impl Hash for Address<'_> {
    fn hash<H: Hasher>(&self, hasher: &mut H) {
        hash_address(self.table, self.part, self.key, hasher);
    }
}

impl Equivalent<EntryKey> for Address<'_> {
    fn equivalent(&self, stored: &EntryKey) -> bool {
        stored.table() == self.table && stored.part() == self.part && stored.key() == self.key
    }
}

/// A stored entry's value: where it was read and what it holds, its value's
/// bytes in an allocation of their own, so a value whose length is a size
/// class, as a 4 KiB value's is, takes that class and no more.
#[derive(Clone, Debug)]
struct EntryValue(Entry);

/// An entry of `table`'s `part` at `key`, read at `source`, holding
/// `held`, its value copied, so the entry keeps nothing of the page its
/// value was read from.
fn entry(
    table: u32,
    part: Part,
    key: &[u8],
    source: Source,
    held: Caching<'_>,
) -> (EntryKey, EntryValue) {
    let mut address = Vec::with_capacity(KEY_HEAD_BYTES + key.len());
    address.extend_from_slice(&table.to_le_bytes());
    address.extend_from_slice(&part.code().to_le_bytes());
    address.extend_from_slice(key);
    let held = match held {
        Caching::Presence => Held::Present,
        Caching::Column(None) => Held::Null,
        Caching::Column(Some(value)) => Held::Value(Bytes::copy_from_slice(value)),
    };
    (
        EntryKey(address.into_boxed_slice()),
        EntryValue(Entry { source, held }),
    )
}

/// Weighs an entry by `charged`.
#[derive(Clone)]
struct RowWeighter;

impl Weighter<EntryKey, EntryValue> for RowWeighter {
    fn weight(&self, key: &EntryKey, value: &EntryValue) -> u64 {
        let value_len = match &value.0.held {
            Held::Value(bytes) => Some(bytes.len()),
            Held::Null | Held::Present => None,
        };
        charged(key.0.len(), value_len)
    }
}

/// One `quick_cache`, the part of its budget its ghost list holds back,
/// and its entries' capacity as last set.
struct Built {
    cache: Cache<EntryKey, EntryValue, RowWeighter>,
    ghosts: GhostReserve,
    /// The entries' capacity `set_capacity` last gave `cache`, kept apart
    /// because `quick_cache` sums its capacity under a read lock of every
    /// shard, and every lookup asks it (`RowCache::is_off`).
    capacity: AtomicU64,
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
        let built = Self {
            cache,
            ghosts,
            capacity: AtomicU64::new(0),
        };
        built.set_capacity(capacity_bytes);
        built
    }

    /// Sets the entries' capacity to what `capacity_bytes` leaves beside
    /// the ghost list's reserve at its ratio (`GhostReserve::entries_within`),
    /// rounded down to a multiple of the shards, evicting until what is
    /// held fits.
    fn set_capacity(&self, capacity_bytes: u64) {
        let shards = approx::count(self.cache.num_shards());
        let entries = self.ghosts.entries_within(capacity_bytes) / shards * shards;
        self.cache.set_capacity(entries);
        self.capacity.store(entries, Ordering::Relaxed);
    }

    /// The entries' capacity as last set (`set_capacity`).
    fn capacity(&self) -> u64 {
        self.capacity.load(Ordering::Relaxed)
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
    stale: Tally,
    /// Each table name's number in the cache, given the first time a table
    /// of that name asks (`RowCache::for_table`) and kept for the process,
    /// so two tables' equal keys hold separate entries.
    tables: Mutex<HashMap<String, u32>>,
}

/// The row cache (RFD 026): entries keyed by table, table key, and column,
/// each checked against the generation it is served from, within a byte
/// budget, evicting what is read least. A clone is another handle on the
/// same cache.
#[derive(Clone)]
pub struct RowCache(Arc<Inner>);

impl RowCache {
    /// A cache whose entries, as `charged` charges them, and ghost list
    /// together take at most `capacity_bytes`: the ghost list's slots are
    /// reserved out of it (`GhostReserve`) and the entries hold the rest.
    /// Its hit, miss, and stale counters are registered at zero in the
    /// recorder current now. It splits into shards chosen for
    /// `capacity_bytes`, each holding its share of the entries' part, so
    /// an entry is cached only when it fits its shard's share: a budget
    /// under one `ROW_CACHE_MAX_VALUE_BYTES` value per shard caches only
    /// smaller values, and a budget of zero, a server none of whose tables
    /// reads through the cache, caches nothing.
    #[must_use]
    pub fn new(capacity_bytes: u64) -> Self {
        Self(Arc::new(Inner {
            built: ArcSwap::from_pointee(Built::new(capacity_bytes)),
            hits: Tally::new(metrics::counter!(metric::ROW_CACHE_HITS_TOTAL)),
            misses: Tally::new(metrics::counter!(metric::ROW_CACHE_MISSES_TOTAL)),
            stale: Tally::new(metrics::counter!(metric::ROW_CACHE_STALE_TOTAL)),
            tables: Mutex::new(HashMap::new()),
        }))
    }

    /// The handle a table named `name` reads the cache through, under the
    /// name's number: the number an earlier table of the name was given,
    /// else the next.
    pub(crate) fn for_table(&self, name: &str) -> TableRows {
        let mut tables = self.0.tables.lock().unwrap_or_else(PoisonError::into_inner);
        let next = u32::try_from(tables.len()).expect("a process names fewer than 2^32 tables");
        let table = *tables.entry(name.to_string()).or_insert(next);
        TableRows {
            cache: self.clone(),
            table,
        }
    }

    /// Whether the cache holds nothing now and can hold nothing: its
    /// entries' capacity is zero, as at a budget of zero, whether `new` was
    /// given it or a resize set it. A lookup asks this once and then skips
    /// the cache, so it counts nothing and copies no value it cannot keep.
    pub(crate) fn is_off(&self) -> bool {
        self.0.built.load().capacity() == 0
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
    pub(crate) fn resize(&self, capacity_bytes: u64) {
        let built = self.0.built.load();
        if built.capacity() > 0 {
            built.set_capacity(capacity_bytes);
        } else if capacity_bytes > 0 {
            self.0.built.store(Arc::new(Built::new(capacity_bytes)));
        }
    }

    /// How the cache is built now: what a test compares with a freshly
    /// built cache's. The entries' capacity kept beside the cache must be
    /// `quick_cache`'s own.
    #[cfg(test)]
    fn shape(&self) -> Shape {
        let built = self.0.built.load();
        assert_eq!(built.capacity(), built.cache.capacity(), "kept capacity");
        Shape {
            shards: built.cache.num_shards(),
            entries_capacity: built.capacity(),
            ghost_bytes: built.ghosts.bytes,
        }
    }

    /// Bytes charged against the budget now: each entry as `charged` charges
    /// it.
    #[must_use]
    pub fn usage(&self) -> u64 {
        self.0.built.load().cache.weight()
    }

    /// Entries held now, of every table.
    #[must_use]
    pub fn entries(&self) -> u64 {
        approx::count(self.0.built.load().cache.len())
    }

    /// Parts of lookups icetray served from the cache since it was built,
    /// summed over the threads' shards. `publish` moves no count out of
    /// these, so a caller reads them the same with or without a scrape
    /// between.
    #[must_use]
    pub fn hits(&self) -> u64 {
        self.0.hits.counted()
    }

    /// Parts of lookups icetray read through the index since the cache was
    /// built, a stale entry's among them (`stale`), counted as `hits` is.
    #[must_use]
    pub fn misses(&self) -> u64 {
        self.0.misses.counted()
    }

    /// Entries icetray found for a lookup and could not serve since the
    /// cache was built, because their file or row had left the lookup's
    /// generation, counted as `hits` is.
    #[must_use]
    pub fn stale(&self) -> u64 {
        self.0.stale.counted()
    }

    /// Moves the hits, misses, and stale entries counted since the last call
    /// into `row_cache_hits_total`, `row_cache_misses_total`, and
    /// `row_cache_stale_total`, and sets
    /// `row_cache_bytes` to `usage`; a scrape calls this, so the counters
    /// read every lookup before it and the gauge reads evictions as well as
    /// inserts. The last clone dropped publishes the counts that remain.
    pub fn publish(&self) {
        self.0.hits.publish();
        self.0.misses.publish();
        self.0.stale.publish();
        metrics::gauge!(metric::ROW_CACHE_BYTES).set(approx::gauge(self.usage()));
    }
}

/// One table's handle on the row cache: the cache and the table's number
/// in it (`RowCache::for_table`). Entries are found by the table key's
/// bytes (`Key::as_bytes`), unique within the table's one key type.
#[derive(Clone)]
pub(crate) struct TableRows {
    cache: RowCache,
    table: u32,
}

impl TableRows {
    /// Whether the cache is off (`RowCache::is_off`).
    pub(crate) fn is_off(&self) -> bool {
        self.cache.is_off()
    }

    /// The entry for `part` of `key`'s row, if the cache holds one, counted
    /// nowhere: the lookup counts what it served (`count`) once it knows.
    pub(crate) fn get(&self, key: &Key, part: Part) -> Option<Entry> {
        let key = key.as_bytes();
        let address = Address {
            table: self.table,
            part: part.code(),
            key: &key,
        };
        self.cache
            .0
            .built
            .load()
            .cache
            .get(&address)
            .map(|value| value.0)
    }

    /// Caches `part` of `key`'s row as read at `source`, holding `held`,
    /// replacing any entry there, unless `caches` refuses `held`, when it
    /// leaves the cache as it was.
    pub(crate) fn insert(&self, key: &Key, part: Part, source: Source, held: Caching<'_>) {
        if !caches(held) {
            return;
        }
        let (key, value) = entry(self.table, part, &key.as_bytes(), source, held);
        self.cache.0.built.load().cache.insert(key, value);
    }

    /// Drops the entry for `part` of `key`'s row if it is still the one
    /// read at `source`, leaving one a later lookup cached in its place.
    pub(crate) fn remove(&self, key: &Key, part: Part, source: Source) {
        let key = key.as_bytes();
        let address = Address {
            table: self.table,
            part: part.code(),
            key: &key,
        };
        self.cache
            .0
            .built
            .load()
            .cache
            .remove_if(&address, |value| value.0.source == source);
    }

    /// Counts what one lookup did with the cache, on the calling thread's
    /// shard: `hits` parts it served, `misses` it did not, and `stale`
    /// entries it found and could not serve, which the caller counts among
    /// `misses` too.
    pub(crate) fn count(&self, hits: usize, misses: usize, stale: usize) {
        let inner = &self.cache.0;
        for (tally, events) in [
            (&inner.hits, hits),
            (&inner.misses, misses),
            (&inner.stale, stale),
        ] {
            if events > 0 {
                tally.add(approx::count(events));
            }
        }
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
            .field("capacity", &self.0.built.load().capacity())
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use super::*;
    use crate::approx::units::{GIB, KIB};
    use crate::index::RowPosition;
    use crate::metric::testing::{Series, recorded_with};

    /// The table the tests' entries are cached for.
    const TABLE: &str = "a_table";
    /// The bytes of a long table key (`Key::as_bytes`).
    const LONG_KEY_BYTES: usize = 8;
    /// A budget that holds every entry a test makes, in each of the shards
    /// it splits into.
    const ROOMY_BYTES: u64 = GIB;
    /// Entries of 1 KiB values a cache sized to hold this many rows of
    /// them is told to expect, so it splits into several shards.
    const SHARD_ROWS_ESTIMATE: u64 = 4096;

    /// An entry's place in the tests: `column` of the row of long key `n`
    /// in `TABLE`.
    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    struct At {
        n: u64,
        column: usize,
    }

    fn key(n: u64, column: usize) -> At {
        At { n, column }
    }

    fn long(n: u64) -> Key {
        Key::Long(i64::try_from(n).unwrap())
    }

    /// Where the tests' entries were read: row `position` of one file.
    fn source(position: u64) -> Source {
        Source {
            file: FileNumber::from_le_bytes(7_u64.to_le_bytes()),
            position: RowPosition(position),
        }
    }

    /// What a test reads back at an entry's place: nothing, a null, or a
    /// value.
    #[derive(Clone, Debug, PartialEq, Eq)]
    enum Cached {
        Absent,
        Null,
        Value(Bytes),
    }

    /// A cache read and written at `At`s through `TABLE`'s handle, each
    /// entry read at `source(at.n)`.
    trait AtRows {
        fn insert(&self, at: At, value: Option<&[u8]>);
        /// What the cache holds at `at`, counted a hit or a miss.
        fn get(&self, at: &At) -> Cached;
        /// Whether the cache holds `at`, counted nowhere and leaving its
        /// place in the queues as it was.
        fn holds(&self, at: &At) -> bool;
        fn shard_of(&self, at: &At) -> usize;
    }

    impl AtRows for RowCache {
        fn insert(&self, at: At, value: Option<&[u8]>) {
            self.for_table(TABLE).insert(
                &long(at.n),
                Part::Column(at.column),
                source(at.n),
                Caching::Column(value),
            );
        }

        fn get(&self, at: &At) -> Cached {
            let rows = self.for_table(TABLE);
            let got = rows.get(&long(at.n), Part::Column(at.column));
            rows.count(usize::from(got.is_some()), usize::from(got.is_none()), 0);
            match got.map(|entry| entry.held) {
                None => Cached::Absent,
                Some(Held::Null) => Cached::Null,
                Some(Held::Value(bytes)) => Cached::Value(bytes),
                Some(Held::Present) => panic!("a column's entry holds a value or a null"),
            }
        }

        fn holds(&self, at: &At) -> bool {
            let rows = self.for_table(TABLE);
            let long = long(at.n);
            let key = long.as_bytes();
            let address = Address {
                table: rows.table,
                part: Part::Column(at.column).code(),
                key: &key,
            };
            self.0.built.load().cache.contains_key(&address)
        }

        fn shard_of(&self, at: &At) -> usize {
            let rows = self.for_table(TABLE);
            let long = long(at.n);
            let key = long.as_bytes();
            let address = Address {
                table: rows.table,
                part: Part::Column(at.column).code(),
                key: &key,
            };
            self.0.built.load().cache.shard_index(&address)
        }
    }

    /// Rows of a kibibyte inserted into one shard of a cache, the shard of
    /// `key(0, 0)`, each at a key not inserted before: eviction is per
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
            Self {
                cache,
                shard: cache.shard_of(&key(0, 0)),
                rows: cache.0.built.load().cache.shard_capacity()
                    / weigh(LONG_KEY_BYTES, Some(approx::length(KIB))),
                next_position: 0,
            }
        }

        /// Inserts `rows` new rows into the shard and returns their places.
        fn insert(&mut self, rows: u64) -> Vec<At> {
            let value = vec![0_u8; approx::length(KIB)];
            let mut inserted = Vec::new();
            while approx::count(inserted.len()) < rows {
                self.next_position += 1;
                let candidate = key(self.next_position, 0);
                if self.cache.shard_of(&candidate) == self.shard {
                    self.cache.insert(candidate, Some(&value));
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

    /// Entries differing only in their table, their table key, or the part
    /// of the row they are for are apart, and a presence entry is apart
    /// from every column's.
    #[test]
    fn test_entries_differing_only_in_table_key_or_part_are_apart() {
        let cache = RowCache::new(ROOMY_BYTES);
        let (ours, theirs) = (cache.for_table("ours"), cache.for_table("theirs"));
        let places = [
            (&ours, 7, Part::Column(2)),
            (&theirs, 7, Part::Column(2)),
            (&ours, 8, Part::Column(2)),
            (&ours, 7, Part::Column(3)),
            (&ours, 7, Part::Presence),
        ];
        for (index, &(rows, n, part)) in places.iter().enumerate() {
            let value = [u8::try_from(index).unwrap()];
            let held = if part == Part::Presence {
                Caching::Presence
            } else {
                Caching::Column(Some(&value[..]))
            };
            rows.insert(&long(n), part, source(u64::try_from(index).unwrap()), held);
        }
        for (index, &(rows, n, part)) in places.iter().enumerate() {
            let held = if part == Part::Presence {
                Held::Present
            } else {
                Held::Value(Bytes::copy_from_slice(&[u8::try_from(index).unwrap()]))
            };
            let expected = Entry {
                source: source(u64::try_from(index).unwrap()),
                held,
            };
            assert_eq!(rows.get(&long(n), part), Some(expected), "{index}");
        }
    }

    /// A table name keeps the number it was first given, and another name
    /// takes the next.
    #[test]
    fn test_a_table_name_keeps_its_number() {
        let cache = RowCache::new(ROOMY_BYTES);
        let first = cache.for_table("first").table;
        let second = cache.for_table("second").table;
        assert_ne!(first, second);
        assert_eq!(cache.for_table("first").table, first);
    }

    /// An entry read again at another source replaces the one there, and
    /// a removal for the old source leaves it.
    #[test]
    fn test_remove_drops_only_the_entry_read_at_its_source() {
        let cache = RowCache::new(ROOMY_BYTES);
        let rows = cache.for_table(TABLE);
        let (key, part) = (long(1), Part::Column(0));
        rows.insert(&key, part, source(10), Caching::Column(Some(b"old")));
        rows.insert(&key, part, source(20), Caching::Column(Some(b"new")));
        rows.remove(&key, part, source(10));
        assert_eq!(
            rows.get(&key, part),
            Some(Entry {
                source: source(20),
                held: Held::Value(Bytes::from_static(b"new")),
            })
        );
        rows.remove(&key, part, source(20));
        assert_eq!(rows.get(&key, part), None);
        assert_eq!(cache.usage(), 0);
    }

    /// An entry is charged its key's and value's bytes in one allocation,
    /// and a presence entry its key's alone.
    #[test]
    fn test_usage_charges_each_entry_its_key_and_value() {
        let cache = RowCache::new(ROOMY_BYTES);
        let rows = cache.for_table(TABLE);
        let text = Key::Str("a string key of some length".to_string());
        rows.insert(
            &text,
            Part::Column(0),
            source(1),
            Caching::Column(Some(&[0_u8; 300])),
        );
        assert_eq!(cache.usage(), weigh(text.as_bytes().len(), Some(300)));
        rows.insert(&long(1), Part::Presence, source(1), Caching::Presence);
        assert_eq!(
            cache.usage(),
            weigh(text.as_bytes().len(), Some(300)) + weigh(LONG_KEY_BYTES, None)
        );
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
        let capacity = 64 * weigh(LONG_KEY_BYTES, Some(approx::length(KIB)));
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

    /// An entry is charged its key's and its value's allocations, each in
    /// jemalloc's size class, so a 4 KiB value takes its class and no more,
    /// plus its slot, and a value the shared header a read allocates.
    #[test]
    fn test_charged_rounds_to_jemalloc_size_classes() {
        let overhead = ROW_SLOT_BYTES + SHARED_HEADER_BYTES;
        assert_eq!(charged(16, Some(100)), 16 + 112 + overhead);
        assert_eq!(charged(16, Some(1029)), 16 + 1280 + overhead);
        assert_eq!(charged(16, Some(4096)), 16 + 4096 + overhead);
        assert_eq!(charged(17, Some(4097)), 32 + 5120 + overhead);
        assert_eq!(
            weigh(LONG_KEY_BYTES, Some(4096)),
            charged(KEY_HEAD_BYTES + LONG_KEY_BYTES, Some(4096))
        );
        assert_eq!(weigh(LONG_KEY_BYTES, None), 16 + ROW_SLOT_BYTES);
    }

    /// A row hit again after it was cached outlives any number of rows read
    /// once into its shard after it, while a row cached beside it and never
    /// read again leaves: a pass over many rows, as a large `MultiGet`
    /// makes, does not push out a row that keeps being asked for.
    #[test]
    fn test_a_row_read_again_outlives_rows_read_once() {
        let cache =
            RowCache::new(SHARD_ROWS_ESTIMATE * weigh(LONG_KEY_BYTES, Some(approx::length(KIB))));
        let mut shard = OneShard::of(&cache);
        let shard_rows = shard.rows;
        shard.insert(shard_rows);
        let [hit_again] = <[At; 1]>::try_from(shard.insert(1)).unwrap();
        assert_ne!(cache.get(&hit_again), Cached::Absent);
        let [read_once] = <[At; 1]>::try_from(shard.insert(1)).unwrap();
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
        let capacity = 256 * weigh(LONG_KEY_BYTES, Some(approx::length(KIB)));
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

    /// The cache is off while its budget is zero, whether built at zero or
    /// resized to it, and on again once a resize gives it a budget; a
    /// resize in place keeps it on. Each answer agrees with `quick_cache`'s
    /// own capacity (`shape`).
    #[test]
    fn test_is_off_follows_the_budget_now() {
        let cache = RowCache::new(0);
        assert!(cache.is_off(), "built at zero");
        cache.resize(ROOMY_BYTES);
        assert!(!cache.is_off(), "grown from zero");
        cache.resize(ROOMY_BYTES / 2);
        assert!(!cache.is_off(), "shrunk in place");
        assert!(cache.shape().entries_capacity > 0, "shrunk in place");
        cache.resize(0);
        assert!(cache.is_off(), "shrunk to zero");
        assert_eq!(cache.shape().entries_capacity, 0, "shrunk to zero");
        assert!(!RowCache::new(ROOMY_BYTES).is_off());
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
        let half_a_largest_per_shard = shards * weigh(LONG_KEY_BYTES, Some(largest.len())) / 2;
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
        let under_a_fifteenth = 15;
        assert!(
            cache.0.built.load().ghosts.bytes * under_a_fifteenth < ROOMY_BYTES,
            "the reserve is about a sixteenth of the budget"
        );
        for budget in [ROOMY_BYTES, ROOMY_BYTES / 2, 2 * ROOMY_BYTES] {
            cache.resize(budget);
            let entries = cache.shape().entries_capacity;
            let held = entries + cache.0.built.load().ghosts.beside(entries);
            assert!(held <= budget, "{held} over {budget}");
            let ghosts = cache.0.built.load().ghosts;
            assert!(
                budget - held <= shards + ghosts.beside(shards) + 1,
                "the entries hold the rest, down to a multiple of the shards \
                 and the reserve beside them"
            );
        }
    }

    /// Whether `cache` remembers a row evicted from its cold queue: the row,
    /// cached again soon after its eviction, comes back as hot and outlives
    /// a pass of rows read once twice its shard's size, which a row cached
    /// cold does not.
    fn readmits_a_ghost_as_hot(cache: &RowCache) -> bool {
        let mut shard = OneShard::of(cache);
        let shard_rows = shard.rows;
        shard.insert(shard_rows);
        let [evicted] = <[At; 1]>::try_from(shard.insert(1)).unwrap();
        let mut pushed = 0;
        while cache.holds(&evicted) {
            assert!(
                pushed <= 2 * shard_rows,
                "the row never left the cold queue"
            );
            shard.insert(1);
            pushed += 1;
        }
        cache.insert(evicted, Some(&vec![0_u8; approx::length(KIB)]));
        shard.insert(2 * shard_rows);
        cache.holds(&evicted)
    }

    /// A cache resized up from zero is the cache `new` builds for the new
    /// budget: its shards, its entries' capacity, and its ghost list's
    /// reserve, and its ghost list remembers evicted rows, whether it was
    /// built at zero, as for a server whose tables read pages alone, or
    /// shrunk to zero from a budget of its own.
    #[test]
    fn test_resize_from_zero_builds_the_cache_new_builds() {
        let budget = SHARD_ROWS_ESTIMATE * weigh(LONG_KEY_BYTES, Some(approx::length(KIB)));
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
    /// `publish` moves the hits, misses, and stale entries counted into them
    /// once, and sets
    /// `row_cache_bytes` to `usage`.
    #[test]
    fn test_publish_moves_the_counts_into_the_series() {
        let hits = 2;
        let ((), published) = recorded_with(|snapshotter| async move {
            let cache = RowCache::new(ROOMY_BYTES);
            let registered = Series::of(&snapshotter);
            for name in [
                metric::ROW_CACHE_HITS_TOTAL,
                metric::ROW_CACHE_MISSES_TOTAL,
                metric::ROW_CACHE_STALE_TOTAL,
            ] {
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
            cache.for_table(TABLE).count(0, 1, 1);
            cache.publish();
            cache.publish();
        });
        assert_eq!(published.counter(metric::ROW_CACHE_HITS_TOTAL, &[]), hits);
        assert_eq!(published.counter(metric::ROW_CACHE_MISSES_TOTAL, &[]), 2);
        assert_eq!(published.counter(metric::ROW_CACHE_STALE_TOTAL, &[]), 1);
        assert_eq!(
            published.gauge(metric::ROW_CACHE_BYTES, &[]).to_bits(),
            approx::gauge(weigh(LONG_KEY_BYTES, None)).to_bits()
        );
    }

    #[test]
    fn test_hits_misses_and_stale_count_every_lookup_through_publish() {
        let cache = RowCache::new(ROOMY_BYTES);
        let rows = cache.for_table(TABLE);
        assert_eq!(
            (cache.hits(), cache.misses(), cache.stale()),
            (0, 0, 0),
            "a new cache has counted nothing"
        );
        assert_eq!(cache.get(&key(1, 0)), Cached::Absent);
        cache.insert(key(1, 0), Some(b"value"));
        let hits = 3;
        for _ in 0..hits {
            assert_eq!(
                cache.get(&key(1, 0)),
                Cached::Value(Bytes::from_static(b"value"))
            );
        }
        rows.count(0, 2, 2);
        cache.publish();
        assert_eq!(cache.hits(), hits, "a publish leaves the hits counted");
        assert_eq!(cache.misses(), 3, "one absent, two stale");
        assert_eq!(cache.stale(), 2);
        cache.publish();
        rows.count(1, 0, 0);
        assert_eq!(
            (cache.hits(), cache.misses(), cache.stale()),
            (hits + 1, 3, 2),
            "counts keep rising across publishes"
        );
    }

    #[test]
    fn test_hits_are_summed_over_the_threads_that_counted_them() {
        let cache = RowCache::new(ROOMY_BYTES);
        let threads = 4;
        let per_thread = 1_000;
        std::thread::scope(|scope| {
            for _ in 0..threads {
                let rows = cache.for_table(TABLE);
                scope.spawn(move || {
                    for _ in 0..per_thread {
                        rows.count(1, 1, 0);
                    }
                });
            }
        });
        assert_eq!(cache.hits(), threads * per_thread);
        assert_eq!(cache.misses(), threads * per_thread);
        assert_eq!(cache.stale(), 0);
    }

    #[test]
    fn test_entries_follow_inserts_removals_and_evictions() {
        let cache = RowCache::new(ROOMY_BYTES);
        assert_eq!(cache.entries(), 0);
        let inserted = 10;
        for n in 0..inserted {
            cache.insert(key(n, 0), Some(b"value"));
        }
        cache.insert(key(0, 0), Some(b"other"));
        assert_eq!(cache.entries(), inserted, "a replaced entry is one entry");
        cache
            .for_table(TABLE)
            .remove(&long(0), Part::Column(0), source(0));
        assert_eq!(cache.entries(), inserted - 1);
        cache.resize(0);
        assert_eq!(cache.entries(), 0, "a budget of zero holds no entry");
        assert_eq!(cache.usage(), 0);
    }
}
