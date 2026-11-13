//! A pool of slabs, one per size class, which copies a value into a slot
//! once and hands it out as a `Bytes` that reads the slot in place.

use std::collections::BTreeSet;
use std::iter;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError, TryLockError};
use std::time::Instant;

use bytes::Bytes;

use crate::chunk::{Chunk, ChunkId, Release, Reserved, Returning, owner_handle_len};
use crate::class::{
    MIN_SLAB_BYTES, class_index, in_slabs, jemalloc_class, slab_class, slots_per_chunk,
};

/// Bytes jemalloc allocates for the handle every `Bytes` that `Slabs::store`
/// returns reads its slot through: what `Bytes::from_owner` boxes, the
/// slot's owner and a reference count, in jemalloc's size class. Each value
/// in a slab pays it once, however many clones share it, beside its slot.
pub const SLOT_HANDLE_BYTES: usize = jemalloc_class(owner_handle_len::<Pool<()>>());

/// Slabs for values from `MIN_SLAB_BYTES` to a longest length, one per slab
/// class, each recording an `R` for every value it holds. A clone shares
/// the slabs.
pub struct Slabs<R> {
    pool: Arc<Pool<R>>,
}

impl<R> Clone for Slabs<R> {
    fn clone(&self) -> Self {
        Slabs {
            pool: Arc::clone(&self.pool),
        }
    }
}

/// A slab class, by its index among its slabs' classes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ClassId(u16);

impl ClassId {
    /// The class at `index` among its slabs' classes.
    fn new(index: usize) -> ClassId {
        ClassId(u16::try_from(index).expect("slabs hold fewer than 2^16 classes"))
    }

    /// The class's index among its slabs' classes.
    fn index(self) -> usize {
        usize::from(self.0)
    }
}

/// A held slot of a chunk that `Slabs::empty_sparsest` marked as being
/// emptied.
#[derive(Debug)]
pub struct Emptied<R> {
    /// The record the slot was stored with.
    pub record: R,
    /// The slot's address, which `is_slot` compares with a `Bytes`'.
    pub at: usize,
}

/// Whether `bytes` start at `at`, the address of a slot in an `Emptied`:
/// whether they are that slot's bytes, so a cache removes an entry only
/// while it still holds the slot. Once the slot's chunk is freed, other
/// memory may take its address, so a match can name another value; the
/// cost of that is one entry evicted.
pub fn is_slot(bytes: &[u8], at: usize) -> bool {
    bytes.as_ptr().addr() == at
}

impl<R: Clone + Send + Sync + 'static> Slabs<R> {
    /// Slabs for values from `MIN_SLAB_BYTES` to `max_bytes` long, holding
    /// no chunks yet.
    pub fn new(max_bytes: usize) -> Slabs<R> {
        let classes = if max_bytes < MIN_SLAB_BYTES {
            Vec::new()
        } else {
            let last = slab_class(max_bytes);
            iter::successors(Some(slab_class(MIN_SLAB_BYTES)), |&class| {
                (class < last).then(|| slab_class(class + 1))
            })
            .map(Class::new)
            .collect()
        };
        Slabs {
            pool: Arc::new(Pool {
                max_bytes,
                classes,
                chunk_bytes: AtomicU64::new(0),
                held_slots: AtomicU64::new(0),
                uncharged_bytes: AtomicU64::new(0),
                waiting_bytes: AtomicU64::new(0),
                lock_wait_nanos: AtomicU64::new(0),
            }),
        }
    }

    /// `bytes` copied into a slot of their class, with `record` as the
    /// slot's record, as a `Bytes` exactly as long as `bytes` that reads
    /// the slot in place; the slot is free again when the last clone of
    /// that `Bytes` drops. `None`, with nothing stored, for a length below
    /// `MIN_SLAB_BYTES` or above the slabs' longest.
    pub fn store(&self, bytes: &[u8], record: R) -> Option<Bytes> {
        if !in_slabs(bytes.len(), self.pool.max_bytes) {
            return None;
        }
        let class = class_index(slab_class(bytes.len()));
        let (reserved, chunk) = self.pool.reserve(class, record);
        Some(reserved.fill(Arc::clone(&self.pool), class, chunk, bytes))
    }

    /// Bytes of every chunk the slabs hold, their free slots included.
    pub fn chunk_bytes(&self) -> u64 {
        self.pool.chunk_bytes.load(Ordering::Relaxed)
    }

    /// Slots that a `Bytes` holds.
    pub fn held_slots(&self) -> u64 {
        self.pool.held_slots.load(Ordering::Relaxed)
    }

    /// Bytes the slabs hold that no value is charged for: the free slots
    /// of chunks not being emptied, and the whole of every chunk being
    /// emptied. A cache holds a reserve of its budget for these, and
    /// empties chunks when they pass it.
    pub fn uncharged_bytes(&self) -> u64 {
        self.pool.uncharged_bytes.load(Ordering::Relaxed)
    }

    /// Bytes of the chunks being emptied, which wait for their last held
    /// slot to drop.
    pub fn waiting_bytes(&self) -> u64 {
        self.pool.waiting_bytes.load(Ordering::Relaxed)
    }

    /// Of the classes whose free slots in chunks not being emptied hold
    /// more than `free_bytes`, the one whose free slots hold the most.
    /// `None` when no class's do.
    pub fn class_over(&self, free_bytes: u64) -> Option<ClassId> {
        self.pool
            .classes
            .iter()
            .map(|class| class.free_bytes.load(Ordering::Relaxed))
            .enumerate()
            .filter(|&(_, free)| free > free_bytes)
            .max_by_key(|&(_, free)| free)
            .map(|(index, _)| ClassId::new(index))
    }

    /// Marks the chunk of `class` with the fewest held slots, among those
    /// with a free slot and not being emptied, as being emptied, and
    /// returns each of its held slots' records and addresses. No store
    /// lands in it again, and the slabs free it when its last held slot
    /// drops, which the caller hastens by dropping the entries that hold
    /// the slots. A chunk with no free slot strands no memory, so when
    /// every chunk of the class is full, or it has none, nothing is
    /// emptied and the result is empty. `class` is one of these slabs',
    /// from `class_over`.
    pub fn empty_sparsest(&self, class: ClassId) -> Vec<Emptied<R>> {
        self.pool.empty_sparsest(class.index())
    }

    /// Whether `bytes`, a value `store` returned or a part of one, lie in a
    /// chunk of these slabs being emptied: a cache that cached a value
    /// while its chunk was being marked asks this after caching it, and
    /// drops the entry when the answer is yes, since the mark handed out
    /// the slot's record before the entry existed. False for bytes of any
    /// other allocation.
    ///
    /// It reads the class's count of chunks being emptied, which changes
    /// under the class's lock, without the lock, and takes the lock, to
    /// find the chunk among those being emptied, only while the count is
    /// not zero. Every emptying that happens before the call raised the
    /// count before it, so the call sees it; an emptying that does not
    /// happen before the call finds the caller's entry by the slot's
    /// record instead, as `empty_sparsest` hands it out.
    pub fn is_emptying(&self, bytes: &[u8]) -> bool {
        if !in_slabs(bytes.len(), self.pool.max_bytes) {
            return false;
        }
        let class = class_index(slab_class(bytes.len()));
        self.pool.is_emptying(class, bytes.as_ptr().addr())
    }

    /// Nanoseconds that stores, frees, and emptying spent waiting for a
    /// class's lock, counted only from a `try_lock` that found it held.
    /// Emptying includes `is_emptying`, which takes the lock only while a
    /// chunk of its class is being emptied.
    pub fn lock_wait_nanos(&self) -> u64 {
        self.pool.lock_wait_nanos.load(Ordering::Relaxed)
    }
}

/// The slabs and their totals. The totals change under a class's lock and
/// are read without one, as gauges: nothing else synchronizes on them.
struct Pool<R> {
    max_bytes: usize,
    /// One per slab class, indexed by `class_index`.
    classes: Vec<Class<R>>,
    /// Bytes of every chunk, held slots or not.
    chunk_bytes: AtomicU64,
    /// Slots held, in every chunk.
    held_slots: AtomicU64,
    /// Bytes of free slots in chunks not being emptied, and of every chunk
    /// being emptied.
    uncharged_bytes: AtomicU64,
    /// Bytes of every chunk being emptied.
    waiting_bytes: AtomicU64,
    /// Nanoseconds spent waiting for a class's lock that `try_lock` found
    /// held.
    lock_wait_nanos: AtomicU64,
}

impl<R: Send + 'static> Pool<R> {
    /// The lock of class `class`, timing the wait when another thread
    /// holds it.
    fn lock(&self, class: usize) -> MutexGuard<'_, ClassState<R>> {
        let state = &self.classes[class].state;
        match state.try_lock() {
            Ok(guard) => guard,
            Err(TryLockError::Poisoned(poisoned)) => poisoned.into_inner(),
            Err(TryLockError::WouldBlock) => {
                let waiting = Instant::now();
                let guard = state.lock().unwrap_or_else(PoisonError::into_inner);
                let waited = u64::try_from(waiting.elapsed().as_nanos()).unwrap_or(u64::MAX);
                self.lock_wait_nanos.fetch_add(waited, Ordering::Relaxed);
                guard
            }
        }
    }

    /// A free slot of class `class`, now held with `record`, and its chunk:
    /// the fullest chunk with room, or a new chunk when none has room. The
    /// caller fills the slot after the class's lock is released, since
    /// nothing refers to it until its store returns. Between reserving the
    /// slot and releasing the lock nothing can panic, since a panic there
    /// would strand the slot. Any record the slot still had drops after the
    /// lock is released.
    fn reserve(&self, class: usize, record: R) -> (Reserved, ChunkId) {
        let slab = &self.classes[class];
        let (reserved, id, _replaced) = {
            let mut guard = self.lock(class);
            let state = &mut *guard;
            let id = if let Some(&(_, id)) = state.room.last() {
                id
            } else {
                let chunk = ChunkState::new(Chunk::new(slab.slot_bytes, slab.slots), slab.slots);
                self.chunk_bytes
                    .fetch_add(slab.chunk_charge, Ordering::Relaxed);
                self.add_free(slab, slab.chunk_charge);
                state.add(chunk)
            };
            let chunk = state.chunks[id.index()]
                .as_mut()
                .expect("a chunk in the set of chunks with room is in its class");
            let held = chunk.chunk.held();
            let reserved = chunk
                .chunk
                .reserve()
                .expect("a chunk with room, or a new one, has a free slot");
            let replaced = chunk.records[reserved.index()].replace(record);
            state.room.remove(&(held, id));
            if chunk.chunk.has_free() {
                state.room.insert((chunk.chunk.held(), id));
            }
            self.held_slots.fetch_add(1, Ordering::Relaxed);
            self.sub_free(slab, slab.slot_charge);
            (reserved, id, replaced)
        };
        (reserved, id)
    }

    /// `Slabs::empty_sparsest` for class `class`. The records leave in the
    /// result, so none drops under the lock.
    fn empty_sparsest(&self, class: usize) -> Vec<Emptied<R>> {
        let slab = &self.classes[class];
        let mut guard = self.lock(class);
        let state = &mut *guard;
        let Some(sparsest) = state.room.pop_first() else {
            return Vec::new();
        };
        let (held, id) = sparsest;
        let chunk = state.chunks[id.index()]
            .as_mut()
            .expect("a chunk in the set of chunks with room is in its class");
        chunk.emptying = true;
        state.emptying.push(id);
        slab.emptying_chunks.fetch_add(1, Ordering::Relaxed);
        let free = slab.chunk_charge - u64::from(held) * slab.slot_charge;
        slab.free_bytes.fetch_sub(free, Ordering::Relaxed);
        self.uncharged_bytes
            .fetch_add(slab.chunk_charge - free, Ordering::Relaxed);
        self.waiting_bytes
            .fetch_add(slab.chunk_charge, Ordering::Relaxed);
        let ChunkState { chunk, records, .. } = chunk;
        records
            .iter_mut()
            .enumerate()
            .filter_map(|(index, record)| {
                record.take().map(|record| Emptied {
                    record,
                    at: chunk.slot_addr(index),
                })
            })
            .collect()
    }

    /// `Slabs::is_emptying` for an address `at` in class `class`.
    fn is_emptying(&self, class: usize, at: usize) -> bool {
        let slab = &self.classes[class];
        if slab.emptying_chunks.load(Ordering::Relaxed) == 0 {
            return false;
        }
        let guard = self.lock(class);
        let chunk_len = slab.slot_bytes * slab.slots;
        guard.emptying.iter().any(|id| {
            let chunk = guard.chunks[id.index()]
                .as_ref()
                .expect("a chunk being emptied is in its class until it is freed");
            let start = chunk.chunk.slot_addr(0);
            (start..start + chunk_len).contains(&at)
        })
    }

    /// Counts `bytes` more of class `slab`'s free slots in chunks not being
    /// emptied.
    fn add_free(&self, slab: &Class<R>, bytes: u64) {
        slab.free_bytes.fetch_add(bytes, Ordering::Relaxed);
        self.uncharged_bytes.fetch_add(bytes, Ordering::Relaxed);
    }

    /// Counts `bytes` fewer of class `slab`'s free slots in chunks not
    /// being emptied.
    fn sub_free(&self, slab: &Class<R>, bytes: u64) {
        slab.free_bytes.fetch_sub(bytes, Ordering::Relaxed);
        self.uncharged_bytes.fetch_sub(bytes, Ordering::Relaxed);
    }
}

impl<R: Send + 'static> Release for Pool<R> {
    /// Returns the slot to its chunk and clears its record, and frees the
    /// chunk when no slot in it is held. The record and the chunk drop
    /// after the class's lock is released.
    fn release(&self, returning: Returning<'_, Self>) {
        let class = returning.class();
        let slab = &self.classes[class];
        let (_record, _freed) = {
            let mut guard = self.lock(class);
            let state = &mut *guard;
            let id = returning.chunk();
            let entry = &mut state.chunks[id.index()];
            let chunk = entry
                .as_mut()
                .expect("a held slot's chunk stays in its class");
            let held = chunk.chunk.held();
            let index = chunk.chunk.release(returning);
            let record = chunk.records[index].take();
            state.room.remove(&(held, id));
            self.held_slots.fetch_sub(1, Ordering::Relaxed);
            let emptying = chunk.emptying;
            if !emptying {
                self.add_free(slab, slab.slot_charge);
            }
            let freed = if chunk.chunk.held() == 0 {
                self.chunk_bytes
                    .fetch_sub(slab.chunk_charge, Ordering::Relaxed);
                if emptying {
                    if let Some(at) = state.emptying.iter().position(|&being| being == id) {
                        state.emptying.swap_remove(at);
                    }
                    slab.emptying_chunks.fetch_sub(1, Ordering::Relaxed);
                    self.waiting_bytes
                        .fetch_sub(slab.chunk_charge, Ordering::Relaxed);
                    self.uncharged_bytes
                        .fetch_sub(slab.chunk_charge, Ordering::Relaxed);
                } else {
                    self.sub_free(slab, slab.chunk_charge);
                }
                state.free_ids.push(id);
                entry.take()
            } else {
                if !emptying {
                    state.room.insert((chunk.chunk.held(), id));
                }
                None
            };
            (record, freed)
        };
    }
}

/// One slab class's chunks, under one lock, and its free slots' total.
struct Class<R> {
    /// The bytes of each slot.
    slot_bytes: usize,
    /// The slots in each chunk.
    slots: usize,
    /// `slot_bytes` as the totals count it.
    slot_charge: u64,
    /// The bytes of each chunk as the totals count them.
    chunk_charge: u64,
    /// Bytes of free slots in the class's chunks not being emptied. Changes
    /// under `state`'s lock, read without it.
    free_bytes: AtomicU64,
    /// The class's chunks being emptied (`ClassState::emptying`). Changes
    /// under `state`'s lock, read without it by `Slabs::is_emptying`.
    emptying_chunks: AtomicU64,
    state: Mutex<ClassState<R>>,
}

impl<R> Class<R> {
    /// The class of `slot_bytes`-byte slots, holding no chunks.
    fn new(slot_bytes: usize) -> Class<R> {
        let slots = slots_per_chunk(slot_bytes);
        Class {
            slot_bytes,
            slots,
            slot_charge: count(slot_bytes),
            chunk_charge: count(slot_bytes * slots),
            free_bytes: AtomicU64::new(0),
            emptying_chunks: AtomicU64::new(0),
            state: Mutex::new(ClassState {
                chunks: Vec::new(),
                free_ids: Vec::new(),
                room: BTreeSet::new(),
                emptying: Vec::new(),
            }),
        }
    }
}

/// A class's chunks and which of them a store may take a slot from.
struct ClassState<R> {
    /// Each chunk at its `ChunkId`'s index; `None` where a chunk was freed.
    chunks: Vec<Option<ChunkState<R>>>,
    /// The ids of freed chunks, which new chunks take before the end of
    /// `chunks`.
    free_ids: Vec<ChunkId>,
    /// (held slots, id) of every chunk with a free slot that is not being
    /// emptied, so the first is the sparsest and the last the fullest.
    room: BTreeSet<(u32, ChunkId)>,
    /// The ids of the chunks being emptied, until each is freed.
    emptying: Vec<ChunkId>,
}

impl<R> ClassState<R> {
    /// Adds `chunk` at a freed id, or at the end, and returns its id.
    fn add(&mut self, chunk: ChunkState<R>) -> ChunkId {
        if let Some(id) = self.free_ids.pop() {
            self.chunks[id.index()] = Some(chunk);
            id
        } else {
            self.chunks.push(Some(chunk));
            ChunkId::new(self.chunks.len() - 1)
        }
    }
}

/// A chunk and what the pool keeps about it.
struct ChunkState<R> {
    chunk: Chunk,
    /// Whether the chunk is being emptied: it takes no new store, and is
    /// freed when its last held slot drops.
    emptying: bool,
    /// Each slot's record, `None` while the slot is free and once its
    /// chunk is being emptied, which hands the records out.
    records: Vec<Option<R>>,
}

impl<R> ChunkState<R> {
    /// `chunk`, of `slots` free slots, not being emptied.
    fn new(chunk: Chunk, slots: usize) -> ChunkState<R> {
        ChunkState {
            chunk,
            emptying: false,
            records: iter::repeat_with(|| None).take(slots).collect(),
        }
    }
}

/// A count of bytes or slots as the totals keep it.
fn count(n: usize) -> u64 {
    u64::try_from(n).expect("a count fits a u64")
}

#[cfg(test)]
mod tests {
    use std::sync::Barrier;
    use std::thread;

    use super::*;
    use crate::{MIN_SLAB_BYTES, slab_class, slots_per_chunk};

    /// The longest value the tests' slabs keep.
    const MAX_BYTES: usize = 128 * 1024;

    /// A length of the 4,352-byte class, whose chunk holds 15 slots.
    const LEN_4352_CLASS: usize = 4242;

    /// `len` bytes that change from offset to offset and differ, at every
    /// offset, from another `seed`'s.
    fn pattern(len: usize, seed: u8) -> Vec<u8> {
        (0..len).map(|i| i.to_le_bytes()[0] ^ seed).collect()
    }

    /// The bytes of one chunk of the class a value of `len` bytes takes.
    fn chunk_of(len: usize) -> u64 {
        let class = slab_class(len);
        u64::try_from(class * slots_per_chunk(class)).unwrap()
    }

    /// The class a value of `len` bytes takes.
    fn class_of(len: usize) -> ClassId {
        ClassId::new(class_index(slab_class(len)))
    }

    /// The bytes of one slot of the class a value of `len` bytes takes.
    fn slot_of(len: usize) -> u64 {
        u64::try_from(slab_class(len)).unwrap()
    }

    /// The records `emptied` names, in order.
    fn records<R: Ord>(emptied: Vec<Emptied<R>>) -> Vec<R> {
        let mut records: Vec<R> = emptied.into_iter().map(|e| e.record).collect();
        records.sort();
        records
    }

    /// `is_emptying` answers yes for the slots, and a part of a slot, of a
    /// chunk being emptied, and no for a chunk of the class not being
    /// emptied, for another class, for bytes outside the slabs, and once
    /// the chunk is freed.
    #[test]
    fn test_is_emptying_names_only_slots_of_chunks_being_emptied() {
        let slabs = Slabs::new(MAX_BYTES);
        let first = slabs.store(&pattern(LEN_4352_CLASS, 1), 1).unwrap();
        let second = slabs.store(&pattern(LEN_4352_CLASS, 2), 2).unwrap();
        let other = slabs.store(&pattern(MIN_SLAB_BYTES, 3), 3).unwrap();
        let outside = pattern(LEN_4352_CLASS, 4);
        for bytes in [&first, &second, &other] {
            assert!(!slabs.is_emptying(bytes), "no chunk is being emptied");
        }
        let emptied = slabs.empty_sparsest(class_of(LEN_4352_CLASS));
        assert_eq!(records(emptied), vec![1, 2]);
        assert!(slabs.is_emptying(&first));
        assert!(slabs.is_emptying(&second[1..]), "a part of a slot");
        assert!(!slabs.is_emptying(&other), "another class");
        assert!(!slabs.is_emptying(&outside), "bytes outside the slabs");
        let later = slabs.store(&pattern(LEN_4352_CLASS, 5), 5).unwrap();
        assert!(
            !slabs.is_emptying(&later),
            "a chunk of the class not being emptied"
        );
        drop((first, second));
        assert_eq!(slabs.waiting_bytes(), 0, "the emptied chunk is freed");
        assert!(!slabs.is_emptying(&later));
        let reused = slabs.store(&pattern(LEN_4352_CLASS, 6), 6).unwrap();
        assert!(!slabs.is_emptying(&reused), "after the chunk is freed");
    }

    /// The handle's length is the slot's owner and a reference count,
    /// whatever the records the pool keeps, and its charge is that length
    /// in jemalloc's size class.
    #[test]
    fn test_slot_handle_bytes_is_the_owner_and_its_count() {
        let len = owner_handle_len::<Pool<()>>();
        assert_eq!(len, owner_handle_len::<Pool<[u64; 64]>>());
        assert!(len > size_of::<usize>() + size_of::<Arc<Pool<()>>>());
        assert_eq!(SLOT_HANDLE_BYTES, jemalloc_class(len));
        assert!(SLOT_HANDLE_BYTES >= len);
    }

    #[test]
    fn test_store_reads_back_the_bytes_stored() {
        let slabs = Slabs::new(MAX_BYTES);
        let inputs: Vec<Vec<u8>> = [MIN_SLAB_BYTES, LEN_4352_CLASS, MAX_BYTES]
            .into_iter()
            .zip(1..)
            .map(|(len, seed)| pattern(len, seed))
            .collect();
        let stored: Vec<Bytes> = inputs
            .iter()
            .enumerate()
            .map(|(record, input)| slabs.store(input, record).unwrap())
            .collect();
        for (bytes, input) in stored.iter().zip(&inputs) {
            assert_eq!(bytes.len(), input.len());
            assert!(bytes[..] == input[..], "length {}", input.len());
        }
    }

    #[test]
    fn test_store_refuses_lengths_outside_the_classes() {
        let slabs = Slabs::new(MAX_BYTES);
        assert!(slabs.store(&pattern(MIN_SLAB_BYTES - 1, 1), 0).is_none());
        assert!(slabs.store(&pattern(MAX_BYTES + 1, 2), 1).is_none());
        assert_eq!(slabs.chunk_bytes(), 0);
    }

    #[test]
    fn test_a_freed_slot_is_reused_before_a_chunk_is_added() {
        let slabs = Slabs::new(MAX_BYTES);
        let slots = slots_per_chunk(slab_class(LEN_4352_CLASS));
        let input = pattern(LEN_4352_CLASS, 3);
        let mut held: Vec<Bytes> = (0..slots)
            .map(|record| slabs.store(&input, record).unwrap())
            .collect();
        assert_eq!(slabs.chunk_bytes(), chunk_of(LEN_4352_CLASS));

        held.pop();
        let refill = pattern(LEN_4352_CLASS, 4);
        held.push(slabs.store(&refill, slots).unwrap());

        assert_eq!(slabs.chunk_bytes(), chunk_of(LEN_4352_CLASS));
        assert_eq!(slabs.held_slots(), count(slots));
        let (last, rest) = held.split_last().unwrap();
        assert_eq!(last[..], refill[..]);
        assert!(rest.iter().all(|bytes| bytes[..] == input[..]));
    }

    #[test]
    fn test_a_chunk_is_freed_when_its_last_slot_drops() {
        let slabs = Slabs::new(MAX_BYTES);
        let held: Vec<Bytes> = [MIN_SLAB_BYTES, MIN_SLAB_BYTES, LEN_4352_CLASS]
            .into_iter()
            .zip(1..)
            .map(|(len, seed)| slabs.store(&pattern(len, seed), len).unwrap())
            .collect();
        assert_eq!(
            slabs.chunk_bytes(),
            chunk_of(MIN_SLAB_BYTES) + chunk_of(LEN_4352_CLASS)
        );
        assert_eq!(slabs.held_slots(), count(held.len()));

        drop(held);

        assert_eq!(slabs.chunk_bytes(), 0);
        assert_eq!(slabs.held_slots(), 0);
    }

    #[test]
    fn test_a_clone_keeps_its_slot() {
        let slabs = Slabs::new(MAX_BYTES);
        let input = pattern(LEN_4352_CLASS, 5);
        let original = slabs.store(&input, 0).unwrap();
        let clone = original.clone();

        drop(original);

        assert_eq!(clone[..], input[..]);
        assert_eq!(slabs.held_slots(), 1);
        assert_eq!(slabs.chunk_bytes(), chunk_of(LEN_4352_CLASS));
    }

    #[test]
    fn test_stores_fill_the_fullest_chunk_first() {
        let slabs = Slabs::new(MAX_BYTES);
        let slots = slots_per_chunk(slab_class(MIN_SLAB_BYTES));
        let input = pattern(MIN_SLAB_BYTES, 6);
        let mut first: Vec<Bytes> = (0..slots)
            .map(|record| slabs.store(&input, record).unwrap())
            .collect();
        let mut second: Vec<Bytes> = (slots..2 * slots)
            .map(|record| slabs.store(&input, record).unwrap())
            .collect();
        assert_eq!(slabs.chunk_bytes(), 2 * chunk_of(MIN_SLAB_BYTES));

        let dropped_from_first = 4;
        let dropped_from_second = 20;
        first.truncate(slots - dropped_from_first);
        second.truncate(slots - dropped_from_second);
        let one_more = slabs.store(&input, 2 * slots).unwrap();
        drop(second);

        assert_eq!(slabs.chunk_bytes(), chunk_of(MIN_SLAB_BYTES));
        assert_eq!(slabs.held_slots(), count(first.len() + 1));
        assert_eq!(one_more[..], input[..]);
    }

    #[test]
    fn test_uncharged_counts_free_slots_and_waiting_chunks() {
        let slabs = Slabs::new(MAX_BYTES);
        let input = pattern(MIN_SLAB_BYTES, 7);
        let held: Vec<Bytes> = (0..3)
            .map(|record| slabs.store(&input, record).unwrap())
            .collect();
        assert_eq!(slabs.chunk_bytes(), 32 * 1024);
        assert_eq!(slabs.uncharged_bytes(), 29 * 1024);
        assert_eq!(slabs.waiting_bytes(), 0);

        let emptied = slabs.empty_sparsest(class_of(MIN_SLAB_BYTES));

        assert_eq!(emptied.len(), 3);
        assert_eq!(slabs.uncharged_bytes(), 32 * 1024);
        assert_eq!(slabs.waiting_bytes(), 32 * 1024);
        drop(held);
        assert_eq!(slabs.uncharged_bytes(), 0);
        assert_eq!(slabs.waiting_bytes(), 0);
    }

    #[test]
    fn test_empty_sparsest_names_the_held_slots_and_takes_no_new_store() {
        let slabs = Slabs::new(MAX_BYTES);
        let input = pattern(MIN_SLAB_BYTES, 8);
        let held: Vec<Bytes> = (0..3)
            .map(|record| slabs.store(&input, record).unwrap())
            .collect();

        let emptied = slabs.empty_sparsest(class_of(MIN_SLAB_BYTES));
        let after = slabs.store(&input, 3).unwrap();

        assert_eq!(records(emptied), [0, 1, 2]);
        assert_eq!(slabs.chunk_bytes(), 2 * chunk_of(MIN_SLAB_BYTES));
        assert_eq!(slabs.held_slots(), 4);
        assert_eq!(
            slabs.uncharged_bytes(),
            chunk_of(MIN_SLAB_BYTES) + chunk_of(MIN_SLAB_BYTES) - slot_of(MIN_SLAB_BYTES)
        );
        drop(held);
        assert_eq!(slabs.chunk_bytes(), chunk_of(MIN_SLAB_BYTES));
        assert_eq!(after[..], input[..]);
    }

    #[test]
    fn test_an_emptied_chunk_is_freed_when_its_last_reader_drops() {
        let slabs = Slabs::new(MAX_BYTES);
        let mut held: Vec<Bytes> = (0..3)
            .map(|seed| slabs.store(&pattern(MIN_SLAB_BYTES, seed), seed).unwrap())
            .collect();
        let kept = held.pop().unwrap();

        let emptied = slabs.empty_sparsest(class_of(MIN_SLAB_BYTES));
        drop(emptied);
        drop(held);

        assert_eq!(kept[..], pattern(MIN_SLAB_BYTES, 2)[..]);
        assert_eq!(slabs.waiting_bytes(), chunk_of(MIN_SLAB_BYTES));
        assert_eq!(slabs.chunk_bytes(), chunk_of(MIN_SLAB_BYTES));
        assert_eq!(slabs.held_slots(), 1);

        drop(kept);

        assert_eq!(slabs.chunk_bytes(), 0);
        assert_eq!(slabs.waiting_bytes(), 0);
        assert_eq!(slabs.uncharged_bytes(), 0);
        assert_eq!(slabs.held_slots(), 0);
    }

    #[test]
    fn test_class_over_ignores_chunks_being_emptied() {
        let slabs = Slabs::new(MAX_BYTES);
        let small: Vec<Bytes> = (0..3)
            .map(|record| slabs.store(&pattern(MIN_SLAB_BYTES, 1), record).unwrap())
            .collect();
        let large = slabs.store(&pattern(LEN_4352_CLASS, 2), 3).unwrap();
        let small_free = 29 * slot_of(MIN_SLAB_BYTES);
        let large_free = 14 * slot_of(LEN_4352_CLASS);
        assert!(small_free < large_free);
        assert_eq!(slabs.class_over(0), Some(class_of(LEN_4352_CLASS)));
        assert_eq!(slabs.class_over(small_free), Some(class_of(LEN_4352_CLASS)));
        assert_eq!(slabs.class_over(large_free), None);

        let emptied = slabs.empty_sparsest(class_of(LEN_4352_CLASS));

        assert_eq!(records(emptied), [3]);
        assert_eq!(slabs.class_over(0), Some(class_of(MIN_SLAB_BYTES)));
        assert_eq!(
            slabs.class_over(small_free - 1),
            Some(class_of(MIN_SLAB_BYTES))
        );
        assert_eq!(slabs.class_over(small_free), None);
        drop((small, large));
        assert_eq!(slabs.class_over(0), None);
    }

    #[test]
    fn test_is_slot_matches_only_its_own_slot() {
        let slabs = Slabs::new(MAX_BYTES);
        let input = pattern(MIN_SLAB_BYTES, 9);
        let held: Vec<Bytes> = (0..3)
            .map(|record| slabs.store(&input, record).unwrap())
            .collect();

        let emptied = slabs.empty_sparsest(class_of(MIN_SLAB_BYTES));

        assert_eq!(emptied.len(), held.len());
        for Emptied { record, at } in &emptied {
            for (index, bytes) in held.iter().enumerate() {
                assert_eq!(is_slot(bytes, *at), index == *record, "{index} {record}");
            }
            let own = &held[*record];
            assert!(!is_slot(&Bytes::copy_from_slice(own), *at));
            assert!(!is_slot(&own.slice(1..), *at));
            assert!(is_slot(&own.clone(), *at));
        }
    }

    #[test]
    fn test_empty_sparsest_takes_the_chunk_with_the_fewest_held_slots() {
        let slabs = Slabs::new(MAX_BYTES);
        let slots = slots_per_chunk(slab_class(MIN_SLAB_BYTES));
        let input = pattern(MIN_SLAB_BYTES, 10);
        let mut first: Vec<Bytes> = (0..slots)
            .map(|record| slabs.store(&input, record).unwrap())
            .collect();
        let mut second: Vec<Bytes> = (slots..2 * slots)
            .map(|record| slabs.store(&input, record).unwrap())
            .collect();
        first.truncate(slots - 4);
        second.truncate(slots - 20);

        let emptied = slabs.empty_sparsest(class_of(MIN_SLAB_BYTES));

        assert_eq!(records(emptied), (slots..slots + 12).collect::<Vec<_>>());
        let refill: Vec<Bytes> = (0..4)
            .map(|record| slabs.store(&input, record).unwrap())
            .collect();
        assert_eq!(slabs.chunk_bytes(), 2 * chunk_of(MIN_SLAB_BYTES));
        assert_eq!(slabs.uncharged_bytes(), chunk_of(MIN_SLAB_BYTES));
        drop(second);
        assert_eq!(slabs.chunk_bytes(), chunk_of(MIN_SLAB_BYTES));
        assert_eq!(slabs.uncharged_bytes(), 0);
        drop((first, refill));
        assert_eq!(slabs.chunk_bytes(), 0);
    }

    #[test]
    fn test_empty_sparsest_of_a_class_without_room_empties_nothing() {
        let slabs = Slabs::new(MAX_BYTES);
        let class = class_of(MIN_SLAB_BYTES);
        assert!(slabs.empty_sparsest(class).is_empty());

        let slots = slots_per_chunk(slab_class(MIN_SLAB_BYTES));
        let input = pattern(MIN_SLAB_BYTES, 11);
        let mut full: Vec<Bytes> = (0..slots)
            .map(|record| slabs.store(&input, record).unwrap())
            .collect();

        assert!(slabs.empty_sparsest(class).is_empty());

        assert_eq!(slabs.waiting_bytes(), 0);
        assert_eq!(slabs.uncharged_bytes(), 0);
        full.pop();
        full.push(slabs.store(&input, slots).unwrap());
        assert_eq!(slabs.chunk_bytes(), chunk_of(MIN_SLAB_BYTES));
        drop(full);
        assert_eq!(slabs.chunk_bytes(), 0);
    }

    #[test]
    fn test_a_record_holding_a_slot_of_its_class_drops_outside_the_lock() {
        let slabs: Slabs<Option<Bytes>> = Slabs::new(MAX_BYTES);
        let input = pattern(MIN_SLAB_BYTES, 12);
        let inner = slabs.store(&input, None).unwrap();
        let outer = slabs.store(&input, Some(inner)).unwrap();
        drop(outer);
        assert_eq!(slabs.held_slots(), 0);

        let inner = slabs.store(&input, None).unwrap();
        let outer = slabs.store(&input, Some(inner)).unwrap();
        let emptied = slabs.empty_sparsest(class_of(MIN_SLAB_BYTES));
        assert_eq!(emptied.len(), 2);
        drop(emptied);
        assert_eq!(slabs.held_slots(), 1);
        drop(outer);

        assert_eq!(slabs.held_slots(), 0);
        assert_eq!(slabs.chunk_bytes(), 0);
    }

    #[test]
    fn test_an_uncontended_lock_adds_no_wait() {
        let slabs = Slabs::new(MAX_BYTES);
        let input = pattern(MIN_SLAB_BYTES, 13);
        let held: Vec<Bytes> = (0..3)
            .map(|record| slabs.store(&input, record).unwrap())
            .collect();
        drop(slabs.empty_sparsest(class_of(MIN_SLAB_BYTES)));
        drop(held);
        assert_eq!(slabs.lock_wait_nanos(), 0);
    }

    #[test]
    fn test_a_store_that_waits_on_its_class_lock_adds_its_wait() {
        let slabs = Slabs::new(MAX_BYTES);
        let input = pattern(MIN_SLAB_BYTES, 14);
        let class = class_index(slab_class(MIN_SLAB_BYTES));
        // A round's store waits only if it reaches the lock before the
        // test thread releases it, which nothing orders; the rounds make a
        // store that never waits vanishingly unlikely.
        let rounds = if cfg!(miri) { 20 } else { 1000 };
        for _ in 0..rounds {
            let barrier = Arc::new(Barrier::new(2));
            let guard = slabs.pool.classes[class].state.lock().unwrap();
            let storer = thread::spawn({
                let (slabs, barrier, input) = (slabs.clone(), Arc::clone(&barrier), input.clone());
                move || {
                    barrier.wait();
                    drop(slabs.store(&input, 0));
                }
            });
            barrier.wait();
            for _ in 0..100 {
                thread::yield_now();
            }
            drop(guard);
            storer.join().unwrap();
            if slabs.lock_wait_nanos() > 0 {
                return;
            }
        }
        panic!("no store waited on its held class lock in {rounds} rounds");
    }
}
