//! A pool of slabs, one per size class, which copies a value into a slot
//! once and hands it out as a `Bytes` that reads the slot in place.

use std::collections::BTreeSet;
use std::iter;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use bytes::Bytes;

use crate::chunk::{Chunk, ChunkId, Release, Reserved, Returning};
use crate::class::{MIN_SLAB_BYTES, class_index, in_slabs, slab_class, slots_per_chunk};

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
}

impl<R: Send + 'static> Pool<R> {
    /// A free slot of class `class`, now held with `record`, and its chunk:
    /// the fullest chunk with room, or a new chunk when none has room. The
    /// caller fills the slot after the class's lock is released, since
    /// nothing refers to it until its store returns. Any record the slot
    /// still had drops after the lock is released.
    fn reserve(&self, class: usize, record: R) -> (Reserved, ChunkId) {
        let slab = &self.classes[class];
        let (reserved, id, _replaced) = {
            let mut guard = slab.lock();
            let state = &mut *guard;
            let id = if let Some(&(_, id)) = state.room.last() {
                id
            } else {
                let chunk = ChunkState::new(Chunk::new(slab.slot_bytes, slab.slots), slab.slots);
                self.chunk_bytes
                    .fetch_add(count(chunk.chunk.bytes()), Ordering::Relaxed);
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
            (reserved, id, replaced)
        };
        (reserved, id)
    }
}

impl<R: Send + 'static> Release for Pool<R> {
    /// Returns the slot to its chunk and clears its record, and frees the
    /// chunk when no slot in it is held. The record and the chunk drop
    /// after the class's lock is released.
    fn release(&self, returning: Returning<'_, Self>) {
        let (_record, _freed) = {
            let mut guard = self.classes[returning.class()].lock();
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
            let freed = if chunk.chunk.held() == 0 {
                self.chunk_bytes
                    .fetch_sub(count(chunk.chunk.bytes()), Ordering::Relaxed);
                state.free_ids.push(id);
                entry.take()
            } else {
                if !chunk.emptying {
                    state.room.insert((chunk.chunk.held(), id));
                }
                None
            };
            (record, freed)
        };
    }
}

/// One slab class's chunks, under one lock.
struct Class<R> {
    /// The bytes of each slot.
    slot_bytes: usize,
    /// The slots in each chunk.
    slots: usize,
    state: Mutex<ClassState<R>>,
}

impl<R> Class<R> {
    /// The class of `slot_bytes`-byte slots, holding no chunks.
    fn new(slot_bytes: usize) -> Class<R> {
        Class {
            slot_bytes,
            slots: slots_per_chunk(slot_bytes),
            state: Mutex::new(ClassState {
                chunks: Vec::new(),
                free_ids: Vec::new(),
                room: BTreeSet::new(),
            }),
        }
    }

    fn lock(&self) -> MutexGuard<'_, ClassState<R>> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
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
    /// emptied, so the last is the fullest chunk with room.
    room: BTreeSet<(u32, ChunkId)>,
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
    /// Each slot's record, `None` while the slot is free.
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
}
