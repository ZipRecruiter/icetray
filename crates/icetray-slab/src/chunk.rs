//! Chunks and slots: the slabs' memory, and all of the crate's `unsafe`
//! (RFD 030 (Cache slabs), under RFD 029 (Unsafe code)).
//!
//! A chunk is one allocation holding one class's slots side by side. A store
//! claims a free slot from its chunk, fills it once, and wraps it in the
//! `Bytes` it returns, which reads the slot in place; the slot returns to its
//! chunk when the last clone of that `Bytes` drops. Every `unsafe` block and
//! impl here relies on one of three invariants:
//!
//! 1. **A slot is written only while nothing refers to it.** `Chunk::claim`
//!    hands out a slot only while its chunk marks it free and marks it held
//!    as it does, so one `Slot` at a time owns it. `Slot::fill` writes it,
//!    taking that `Slot` by value before any `Bytes` holds it, and nothing
//!    writes it again until the `Slot` drops and marks it free.
//! 2. **A chunk outlives every slot in it.** A chunk frees its memory on
//!    drop only when it marks every slot free; dropped with a slot held, it
//!    leaks the memory instead, which the slot goes on reading.
//! 3. **Slots lie inside their chunk, apart from each other.** Slot `i` of a
//!    chunk of `slot_bytes`-byte slots starts `i * slot_bytes` bytes in, `i`
//!    is below the chunk's slot count, and a slot's length is at most
//!    `slot_bytes`.
//!
//! These hold whatever the pool in the `slabs` module does with its
//! bookkeeping: `Chunk::release` checks that a slot returns to the chunk it
//! lies in and is released once, so routing a slot to the wrong chunk
//! panics instead of freeing a slot that is still read.

use std::mem::MaybeUninit;
use std::ptr::{self, NonNull};
use std::sync::Arc;

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

    /// The bytes the chunk allocated.
    pub(crate) fn bytes(&self) -> usize {
        self.units * UNIT_BYTES
    }

    /// The slots held.
    pub(crate) fn held(&self) -> u32 {
        self.slots - self.free.count_ones()
    }

    /// Whether a slot is free.
    pub(crate) fn has_free(&self) -> bool {
        self.free != 0
    }

    /// A free slot, now held, for a value of `len` bytes, which returns to
    /// `pool` when it drops; `class` and `chunk` name this chunk to the
    /// pool. `None` when every slot is held.
    pub(crate) fn claim<P: Release>(
        &mut self,
        pool: &Arc<P>,
        class: usize,
        chunk: ChunkId,
        len: usize,
    ) -> Option<Slot<P>> {
        assert!(
            len <= self.slot_bytes,
            "a value of {len} bytes fits a slot of {}",
            self.slot_bytes
        );
        if self.free == 0 {
            return None;
        }
        let index = self.free.trailing_zeros();
        self.free &= !(1 << index);
        Some(Slot {
            pool: Arc::clone(pool),
            class,
            chunk,
            index,
            ptr: self.slot_ptr(index),
            len,
        })
    }

    /// Marks `slot` free and returns its index. Panics unless `slot` lies
    /// in this chunk and is held.
    pub(crate) fn release<P: Release>(&mut self, slot: &Slot<P>) -> usize {
        assert!(
            ptr::eq(slot.ptr, self.slot_ptr(slot.index)),
            "a slot returns to the chunk it lies in"
        );
        let bit = 1 << slot.index;
        assert!(self.free & bit == 0, "a slot is released once");
        self.free |= bit;
        slot.index()
    }

    /// The first byte of slot `index`, which is below the chunk's slot
    /// count, so the slot lies inside the chunk (invariant 3).
    fn slot_ptr(&self, index: u32) -> *mut u8 {
        assert!(index < self.slots, "slot {index} of {}", self.slots);
        let index = usize::try_from(index).expect("a slot index fits a usize");
        self.base
            .as_ptr()
            .cast::<u8>()
            .wrapping_add(index * self.slot_bytes)
    }
}

impl Drop for Chunk {
    fn drop(&mut self) {
        if self.free != all_free(self.slots) {
            return;
        }
        let memory = ptr::slice_from_raw_parts_mut(self.base.as_ptr(), self.units);
        // SAFETY: invariant 2. Every slot is free, so no `Slot` points into
        // the memory, and `memory` is the pointer and length `Box::into_raw`
        // returned in `new`, so the `Box` rebuilt here is the one allocated.
        drop(unsafe { Box::from_raw(memory) });
    }
}

// SAFETY: invariant 2. A chunk owns its memory outright and frees it only
// when no slot in it is held, from whichever thread drops it.
unsafe impl Send for Chunk {}

// SAFETY: invariants 1 and 3. A shared chunk reads only its own fields; the
// slots' bytes are reached only through `Slot`s.
unsafe impl Sync for Chunk {}

/// Every slot of a chunk of `slots` slots, as free slots.
fn all_free(slots: u32) -> FreeMask {
    FreeMask::MAX >> (FreeMask::BITS - slots)
}

/// The pool a slot returns to when it drops.
pub(crate) trait Release: Send + Sync + Sized + 'static {
    /// Returns `slot` to its chunk, through `Chunk::release`.
    fn release(&self, slot: &Slot<Self>);
}

/// One held slot: the owner of the `Bytes` a store returns, which reads its
/// `len` bytes in place and returns the slot to its pool on drop.
pub(crate) struct Slot<P: Release> {
    pool: Arc<P>,
    class: usize,
    chunk: ChunkId,
    index: u32,
    ptr: *mut u8,
    len: usize,
}

impl<P: Release> Slot<P> {
    /// The index of the slot's class in its pool.
    pub(crate) fn class(&self) -> usize {
        self.class
    }

    /// The slot's chunk among its class's chunks.
    pub(crate) fn chunk(&self) -> ChunkId {
        self.chunk
    }

    /// The slot's index in its chunk.
    pub(crate) fn index(&self) -> usize {
        usize::try_from(self.index).expect("a slot index fits a usize")
    }

    /// `bytes`, which are as long as the slot was claimed for, copied into
    /// the slot, as a `Bytes` that reads them in place.
    pub(crate) fn fill(self, bytes: &[u8]) -> Bytes {
        assert_eq!(
            bytes.len(),
            self.len,
            "a slot is filled with the length it was claimed for"
        );
        // SAFETY: invariant 1: `self` is the slot's only `Slot`, taken by
        // value, and no `Bytes` holds it yet, so nothing reads the slot.
        // Invariant 2: the chunk lives while `self` does. Invariant 3: the
        // slot's `len` bytes lie inside its chunk and apart from every other
        // slot, so `bytes`, which nothing writes while it is borrowed and
        // which may lie in another slot, does not overlap them.
        unsafe { ptr::copy_nonoverlapping(bytes.as_ptr(), self.ptr, self.len) };
        Bytes::from_owner(self)
    }
}

impl<P: Release> AsRef<[u8]> for Slot<P> {
    fn as_ref(&self) -> &[u8] {
        // SAFETY: invariant 1: `fill` wrote all `len` bytes before the
        // `Bytes` that reads them existed, and nothing writes them until
        // this slot drops. Invariant 2: the chunk lives while the slot
        // does. Invariant 3: the bytes lie inside the chunk and apart from
        // the other slots, which other threads write.
        unsafe { std::slice::from_raw_parts(self.ptr, self.len) }
    }
}

impl<P: Release> Drop for Slot<P> {
    fn drop(&mut self) {
        P::release(&self.pool, self);
    }
}

// SAFETY: invariants 1 and 2. The pointer is the one field that is not
// `Send` of itself: its bytes are only read once `fill` returns, from any
// thread, and stay allocated until the slot drops, from any thread, which
// returns it through the pool's lock.
unsafe impl<P: Release> Send for Slot<P> {}

// SAFETY: invariant 1. A shared slot only reads its bytes, which nothing
// writes while it lives.
unsafe impl<P: Release> Sync for Slot<P> {}
