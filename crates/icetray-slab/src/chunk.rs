//! Chunks and slots: the slabs' memory, and all of the crate's `unsafe`
//! (RFD 030 (Cache slabs), under RFD 029 (Unsafe code)).
//!
//! A chunk is one allocation holding one class's slots side by side. A store
//! reserves a free slot of a chunk under its class's lock, then, outside the
//! lock, fills it and wraps it in the `Bytes` it returns, which reads the
//! slot in place; the slot returns to its chunk when the last clone of that
//! `Bytes` drops. Every `unsafe` block and impl here relies on one of three
//! invariants:
//!
//! 1. **A slot is written only while nothing refers to it.** `Chunk::reserve`
//!    hands out a slot only while its chunk marks it free, and marks it held
//!    as it does. `Reserved::fill` writes it once, before the slot's owner
//!    exists, and nothing writes it again until that owner drops and marks
//!    it free.
//! 2. **A chunk outlives every slot in it.** A chunk frees its memory on
//!    drop only when it marks every slot free; dropped with a slot held, it
//!    leaks the memory instead, which the slot goes on reading.
//! 3. **Slots lie inside their chunk, apart from each other.** Slot `i` of a
//!    chunk of `slot_bytes`-byte slots starts `i * slot_bytes` bytes in, `i`
//!    is below the chunk's slot count, and a slot's length is at most
//!    `slot_bytes`.
//!
//! The rest of the crate reaches a slot only through this module's API,
//! which admits storing bytes and dropping the `Bytes` that reads them, and
//! learning a slot's address as a number, which reaches nothing. A
//! `Reserved` slot has one use, `fill`, which returns the `Bytes`; the
//! owner that reads the slot is private here and exists only filled. A slot
//! is marked free only through `Chunk::release`, whose `Returning` only an
//! owner's drop makes, once, and which checks that the slot lies in the
//! chunk it returns to. So the pool's bookkeeping, in the `slabs` module,
//! can strand memory or panic, but it cannot break an invariant.

use std::mem::MaybeUninit;
use std::ptr::{self, NonNull};
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;

use bytes::Bytes;

use crate::MAX_SLOTS_PER_CHUNK;

/// The unit a chunk's memory is allocated in, sixteen bytes aligned to
/// sixteen, so every slot, starting at a multiple of its class, which is a
/// multiple of sixteen, is as aligned as an allocation of its own would be.
#[repr(C, align(16))]
struct Unit([u8; 16]);

/// The alignment, and the size, of a chunk's allocation unit.
const UNIT_BYTES: usize = align_of::<Unit>();

const _: () = assert!(size_of::<Unit>() == UNIT_BYTES);

/// A chunk's free slots, one bit per slot.
type FreeMask = u32;

const _: () = assert!(MAX_SLOTS_PER_CHUNK <= FreeMask::BITS as usize);

/// A chunk's place among its class's chunks, which a slot carries back to
/// the pool when it drops.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct ChunkId(u32);

impl ChunkId {
    /// The chunk at `index` among its class's chunks.
    pub(crate) fn new(index: usize) -> ChunkId {
        ChunkId(u32::try_from(index).expect("a class holds fewer than 2^32 chunks"))
    }

    /// The chunk's index among its class's chunks.
    pub(crate) fn index(self) -> usize {
        usize::try_from(self.0).expect("a chunk id fits a usize")
    }
}

/// One allocation of `slots` slots of `slot_bytes` bytes each, and which of
/// them are free.
pub(crate) struct Chunk {
    /// The memory, from `Box::into_raw` of `units` units; freed on drop.
    base: NonNull<MaybeUninit<Unit>>,
    units: usize,
    slot_bytes: usize,
    slots: u32,
    free: FreeMask,
}

impl Chunk {
    /// A chunk of `slots` free slots of `slot_bytes` bytes each.
    /// `slot_bytes` is a nonzero multiple of sixteen, and `slots` is from 1
    /// to `MAX_SLOTS_PER_CHUNK`.
    pub(crate) fn new(slot_bytes: usize, slots: usize) -> Chunk {
        assert!(
            slot_bytes > 0 && slot_bytes.is_multiple_of(UNIT_BYTES),
            "a slot of {slot_bytes} bytes is a nonzero multiple of {UNIT_BYTES}"
        );
        assert!(
            (1..=MAX_SLOTS_PER_CHUNK).contains(&slots),
            "a chunk of {slots} slots holds from 1 to {MAX_SLOTS_PER_CHUNK}"
        );
        let units = slot_bytes / UNIT_BYTES * slots;
        let memory = Box::into_raw(Box::<[Unit]>::new_uninit_slice(units));
        let slots = u32::try_from(slots).expect("a chunk holds at most 32 slots");
        Chunk {
            base: NonNull::new(memory.cast::<MaybeUninit<Unit>>())
                .expect("Box::into_raw is never null"),
            units,
            slot_bytes,
            slots,
            free: all_free(slots),
        }
    }

    /// The slots held.
    pub(crate) fn held(&self) -> u32 {
        self.slots - self.free.count_ones()
    }

    /// Whether a slot is free.
    pub(crate) fn has_free(&self) -> bool {
        self.free != 0
    }

    /// A free slot, now held until the `Bytes` its `fill` returns drops.
    /// `None` when every slot is held.
    pub(crate) fn reserve(&mut self) -> Option<Reserved> {
        if self.free == 0 {
            return None;
        }
        let index = self.free.trailing_zeros();
        self.free &= !(1 << index);
        Some(Reserved {
            index,
            ptr: self.slot_ptr(index),
            slot_bytes: self.slot_bytes,
        })
    }

    /// Marks the slot that `returning` returns free and gives its index.
    /// Panics unless that slot lies in this chunk and is held.
    pub(crate) fn release<P: Release>(&mut self, returning: Returning<'_, P>) -> usize {
        let slot = returning.into_slot();
        assert!(
            ptr::eq(slot.ptr, self.slot_ptr(slot.index)),
            "a slot returns to the chunk it lies in"
        );
        let bit = 1 << slot.index;
        assert!(self.free & bit == 0, "a slot is released once");
        self.free |= bit;
        slot_index(slot.index)
    }

    /// The address of slot `index`'s first byte, as a number: it names the
    /// slot without a way to read, write, or release it.
    pub(crate) fn slot_addr(&self, index: usize) -> usize {
        let index = u32::try_from(index).expect("a slot index fits a u32");
        self.slot_ptr(index).addr()
    }

    /// The first byte of slot `index`, which is below the chunk's slot
    /// count, so the slot lies inside the chunk (invariant 3).
    fn slot_ptr(&self, index: u32) -> *mut u8 {
        assert!(index < self.slots, "slot {index} of {}", self.slots);
        self.base
            .as_ptr()
            .cast::<u8>()
            .wrapping_add(slot_index(index) * self.slot_bytes)
    }
}

impl Drop for Chunk {
    fn drop(&mut self) {
        if self.free != all_free(self.slots) {
            return;
        }
        let memory = ptr::slice_from_raw_parts_mut(self.base.as_ptr(), self.units);
        // SAFETY: invariant 2. Every slot is free, so no slot's owner points
        // into the memory, and `memory` is the pointer and length
        // `Box::into_raw` returned in `new`, so the `Box` rebuilt here is the
        // one allocated.
        drop(unsafe { Box::from_raw(memory) });
    }
}

// SAFETY: invariant 2. A chunk owns its memory outright and frees it only
// when no slot in it is held, from whichever thread drops it.
unsafe impl Send for Chunk {}

/// Every slot of a chunk of `slots` slots, as free slots.
fn all_free(slots: u32) -> FreeMask {
    FreeMask::MAX >> (FreeMask::BITS - slots)
}

/// A slot index as a `usize`.
fn slot_index(index: u32) -> usize {
    usize::try_from(index).expect("a slot index fits a usize")
}

/// A slot `Chunk::reserve` marked held, not yet filled. Its one use is
/// `fill`; dropped unfilled, it keeps its slot held, so the slot's chunk
/// is never freed.
pub(crate) struct Reserved {
    index: u32,
    ptr: *mut u8,
    slot_bytes: usize,
}

impl Reserved {
    /// The slot's index in its chunk.
    pub(crate) fn index(&self) -> usize {
        slot_index(self.index)
    }

    /// `bytes`, at most a slot long, copied into the slot, as a `Bytes`
    /// exactly as long that reads them in place. The slot returns to
    /// `pool`, which names its chunk as `class` and `chunk`, when the last
    /// clone of the `Bytes` drops, or at once if `bytes` do not fit.
    pub(crate) fn fill<P: Release>(
        self,
        pool: Arc<P>,
        class: usize,
        chunk: ChunkId,
        bytes: &[u8],
    ) -> Bytes {
        let slot = Slot {
            pool,
            class,
            chunk,
            index: self.index,
            ptr: self.ptr,
            len: bytes.len(),
        };
        assert!(
            slot.len <= self.slot_bytes,
            "a value of {} bytes fits a slot of {}",
            slot.len,
            self.slot_bytes
        );
        // SAFETY: invariant 1: the slot was free when `reserve` made `self`,
        // which `fill` consumes, and `slot`, the only owner, is not yet in a
        // `Bytes`, so nothing reads the slot. Invariant 2: the chunk lives
        // while its slot is held. Invariant 3: the slot's `len` bytes lie
        // inside its chunk and apart from every other slot, so `bytes`,
        // which nothing writes while it is borrowed and which may lie in
        // another slot, does not overlap them.
        unsafe { ptr::copy_nonoverlapping(bytes.as_ptr(), slot.ptr, slot.len) };
        Bytes::from_owner(slot)
    }
}

/// The pool a slot returns to when it drops.
pub(crate) trait Release: Send + Sync + Sized + 'static {
    /// Returns the slot `returning` names to its chunk, through
    /// `Chunk::release`.
    fn release(&self, returning: Returning<'_, Self>);
}

/// A dropping slot, on its way back to its chunk. Only a slot's drop makes
/// one, and `Chunk::release` consumes it, so a slot is marked free only
/// once its owner is gone.
pub(crate) struct Returning<'a, P: Release> {
    slot: &'a Slot<P>,
}

impl<'a, P: Release> Returning<'a, P> {
    /// The dropping slot, taking `self`, so one `Returning` marks one slot
    /// free once.
    fn into_slot(self) -> &'a Slot<P> {
        self.slot
    }

    /// The index of the slot's class in its pool.
    pub(crate) fn class(&self) -> usize {
        self.slot.class
    }

    /// The slot's chunk among its class's chunks.
    pub(crate) fn chunk(&self) -> ChunkId {
        self.slot.chunk
    }
}

/// Bytes of what `Bytes::from_owner` allocates for a slot of pool `P`'s:
/// `bytes`' own reference count beside the slot, its owner. The owner holds
/// the pool by an `Arc`, a pointer whatever the pool, so the length is the
/// same for every pool.
pub(crate) const fn owner_handle_len<P: Release>() -> usize {
    size_of::<AtomicUsize>() + size_of::<Slot<P>>()
}

/// A filled slot: the owner of the `Bytes` that `Reserved::fill` returns,
/// which reads its `len` bytes in place and returns the slot to its pool on
/// drop.
struct Slot<P: Release> {
    pool: Arc<P>,
    class: usize,
    chunk: ChunkId,
    index: u32,
    ptr: *mut u8,
    len: usize,
}

impl<P: Release> AsRef<[u8]> for Slot<P> {
    fn as_ref(&self) -> &[u8] {
        // SAFETY: invariant 1: `fill` wrote all `len` bytes before the
        // `Bytes` that calls this existed, and nothing writes them until
        // this slot drops. Invariant 2: the chunk lives while the slot is
        // held. Invariant 3: the bytes lie inside the chunk and apart from
        // the other slots, which other threads write.
        unsafe { std::slice::from_raw_parts(self.ptr, self.len) }
    }
}

impl<P: Release> Drop for Slot<P> {
    fn drop(&mut self) {
        P::release(&self.pool, Returning { slot: self });
    }
}

// SAFETY: invariants 1 and 2. The pointer is the one field that is not
// `Send` of itself: its bytes are only read once `fill` has written them,
// from any thread, and stay allocated until the slot drops, from any
// thread, which returns it through the pool's lock.
unsafe impl<P: Release> Send for Slot<P> {}

#[cfg(test)]
mod tests {
    use std::panic::{self, AssertUnwindSafe};
    use std::sync::{Mutex, PoisonError};

    use super::*;

    /// The bytes of each slot of the tests' chunks.
    const SLOT_BYTES: usize = 1024;

    /// A pool of one chunk.
    struct OneChunk(Mutex<Chunk>);

    impl Release for OneChunk {
        fn release(&self, returning: Returning<'_, Self>) {
            self.0
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .release(returning);
        }
    }

    fn one_chunk(slots: usize) -> Arc<OneChunk> {
        Arc::new(OneChunk(Mutex::new(Chunk::new(SLOT_BYTES, slots))))
    }

    fn reserve(pool: &OneChunk) -> Reserved {
        pool.0.lock().unwrap().reserve().unwrap()
    }

    #[test]
    fn test_reserve_marks_the_slot_held_until_its_bytes_drop() {
        let pool = one_chunk(2);
        let value = vec![7; SLOT_BYTES];
        let bytes = reserve(&pool).fill(Arc::clone(&pool), 0, ChunkId::new(0), &value);
        assert_eq!(pool.0.lock().unwrap().held(), 1);
        assert_eq!(bytes[..], value[..]);

        drop(bytes);

        assert_eq!(pool.0.lock().unwrap().held(), 0);
    }

    #[test]
    fn test_reserve_refuses_a_full_chunk() {
        let pool = one_chunk(1);
        let reserved = reserve(&pool);
        assert!(pool.0.lock().unwrap().reserve().is_none());
        let bytes = reserved.fill(Arc::clone(&pool), 0, ChunkId::new(0), &[1; SLOT_BYTES]);
        drop(bytes);
        assert!(pool.0.lock().unwrap().has_free());
    }

    #[test]
    fn test_fill_that_does_not_fit_returns_its_slot() {
        let pool = one_chunk(2);
        let reserved = reserve(&pool);
        let first = reserved.index();
        let too_long = vec![1; SLOT_BYTES + 1];

        let filled = panic::catch_unwind(AssertUnwindSafe(|| {
            reserved.fill(Arc::clone(&pool), 0, ChunkId::new(0), &too_long)
        }));

        assert!(filled.is_err());
        assert_eq!(pool.0.lock().unwrap().held(), 0);
        let again = reserve(&pool);
        assert_eq!(again.index(), first);
        drop(again.fill(Arc::clone(&pool), 0, ChunkId::new(0), &[1; SLOT_BYTES]));
    }
}
