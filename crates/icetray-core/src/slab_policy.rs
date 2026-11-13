//! What the row cache and the page cache share about their slabs (RFD 030
//! (Cache slabs)): where a value lives and what it is charged there, the
//! floor under which a cache keeps every value in an allocation of its
//! own, the reserve of its budget held for the slabs' bytes no entry is
//! charged for, and how a cache empties chunks to stay within it.

use icetray_slab::{
    ClassId, Emptied, SLOT_HANDLE_BYTES, Slabs, classes, jemalloc_class, slab_class,
    slots_per_chunk,
};

use crate::approx;

/// The share of a budget held in reserve for the slabs' bytes that no
/// entry is charged for, their free slots and the chunks being emptied, as
/// the divisor of one: a sixteenth. A cache's entries take the budget less
/// the reserve, set once per budget, since `quick_cache` shrinks its ghost
/// list on every change of capacity and foyer starts a thread per shard
/// for one. When the slabs' uncharged bytes pass the reserve, the cache
/// empties chunks until they are back within it (`fit`), so a free slot
/// within the reserve costs nothing beyond it, and the reserve costs that
/// share of the entries.
///
/// A cache keeps values in slabs only while its budget is at least this
/// many of one chunk of every class in its slabs' range (`floor_bytes`),
/// so the reserve holds a chunk being filled in each class in use; under
/// that, values stay in allocations of their own and no reserve is held.
pub(crate) const SLAB_RESERVE_DIVISOR: u64 = 16;

/// Bytes of the shared header the first clone of a `Bytes` over an
/// allocation of its own allocates, which every cached value or page that
/// is read pays: `bytes`' `Shared`, a buffer pointer, its capacity, and a
/// reference count.
const SHARED_HEADER_LEN: usize = 24;

/// What the shared header takes in jemalloc's size class
/// (`SHARED_HEADER_LEN`).
pub(crate) const SHARED_HEADER_BYTES: u64 = approx::count(jemalloc_class(SHARED_HEADER_LEN));

/// Where a cached value's bytes live: in a slot of the cache's slabs, or in
/// an allocation of their own. A value's home is fixed when it is cached,
/// by its length and the budget then, and its entry records it: the
/// budget can cross the slab floor before the value leaves, and
/// `quick_cache` weighs an entry again when it evicts it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Home {
    /// A slot of the slabs, read in place.
    Slot,
    /// An allocation of the value's own.
    Own,
}

impl Home {
    /// What `len` bytes held here take: in a slot, the slot's class and the
    /// handle its `Bytes` reads the slot through
    /// (`icetray_slab::SLOT_HANDLE_BYTES`); in an allocation of their own,
    /// jemalloc's size class and the shared header a read allocates
    /// (`SHARED_HEADER_BYTES`).
    pub(crate) fn charged(self, len: usize) -> u64 {
        match self {
            Self::Slot => approx::count(slab_class(len) + SLOT_HANDLE_BYTES),
            Self::Own => approx::count(jemalloc_class(len)) + SHARED_HEADER_BYTES,
        }
    }
}

/// The smallest budget under which a cache whose slabs hold values up to
/// `max_bytes` long keeps values there, its slab floor:
/// `SLAB_RESERVE_DIVISOR` times the bytes of one chunk of every slab class
/// from `MIN_SLAB_BYTES`'s to that of `max_bytes`, so the reserve holds a
/// chunk being filled in every class at once. A class a store first
/// takes a slot of allocates a chunk whose other slots are free, and while
/// the reserve could not hold such a chunk in each class in use, a full
/// cache's free slots would pass it with each such chunk, and the
/// emptying would take that chunk, the sparsest of the class with the
/// most free slots, and with it the value just cached. About 261 MiB for
/// the row cache's slabs, to 64 KiB, and 455 MiB for the page cache's, to
/// 128 KiB. A cache computes it once (`RowCache`'s and `PageCache`'s
/// `slab_floor_bytes`), since it sums over every class.
pub(crate) fn floor_bytes(max_bytes: usize) -> u64 {
    let chunks: usize = classes(max_bytes)
        .map(|class| slots_per_chunk(class) * class)
        .sum();
    SLAB_RESERVE_DIVISOR * approx::count(chunks)
}

/// Whether `budget` keeps values in the slabs of a cache whose slab floor
/// is `floor` (`floor_bytes`).
pub(crate) fn uses_slabs(budget: u64, floor: u64) -> bool {
    budget >= floor
}

/// The bytes of `budget` held in reserve for the slabs of a cache whose
/// slab floor is `floor` (`floor_bytes`): a sixteenth
/// (`SLAB_RESERVE_DIVISOR`) while the budget keeps values in slabs, else
/// none, since no store then fills a chunk.
pub(crate) fn reserve(budget: u64, floor: u64) -> u64 {
    if uses_slabs(budget, floor) {
        budget / SLAB_RESERVE_DIVISOR
    } else {
        0
    }
}

/// A budget from which on the part beside the reserve of a cache whose
/// slab floor is `floor` (`floor_bytes`) holds at least `entries` bytes:
/// `entries` itself, unless the floor's part is under it, since a budget
/// at the floor holds back a reserve that one just under it does not; then
/// the budget whose part beside its sixteenth holds `entries`.
pub(crate) fn budget_holding(entries: u64, floor: u64) -> u64 {
    if entries <= floor - floor / SLAB_RESERVE_DIVISOR {
        entries
    } else {
        (entries * SLAB_RESERVE_DIVISOR).div_ceil(SLAB_RESERVE_DIVISOR - 1)
    }
}

/// Brings the uncharged bytes of `slabs` back within `reserve`: while they
/// pass it, and the chunks being emptied alone do not, so the free slots
/// are what pushes them over, empties the sparsest chunk of the class whose
/// free slots hold the most, calling `leave` with the class and the record
/// and address of each slot held in it, once per chunk, for the cache to
/// take out the entries that still hold those slots. A chunk that no reader holds is freed then; one a
/// reader holds waits for the reader to drop its slot, counted among the
/// chunks being emptied, and nothing waits for it here. Each round takes
/// one chunk out of those a store may fill, so the rounds end. Under a
/// reserve of zero, a budget under the slab floor, it empties every chunk
/// with a free slot while none waits.
pub(crate) fn fit<R: Clone + Send + Sync + 'static>(
    slabs: &Slabs<R>,
    reserve: u64,
    mut leave: impl FnMut(ClassId, Vec<Emptied<R>>),
) {
    while slabs.uncharged_bytes() > reserve && slabs.waiting_bytes() <= reserve {
        let Some(class) = slabs.class_over(0) else {
            break;
        };
        let emptied = slabs.empty_sparsest(class);
        if emptied.is_empty() {
            break;
        }
        leave(class, emptied);
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use super::*;

    /// The longest value the tests' slabs hold, the row cache's.
    const MAX_BYTES: usize = 64 * 1024;

    /// The floor is the divisor times one chunk of every class in range:
    /// what slabs holding one value of each class allocate. Pinned for the
    /// row cache's range, to 64 KiB, and the page cache's, to 128 KiB,
    /// which the docs state.
    #[test]
    fn test_the_floor_holds_a_chunk_of_every_class() {
        let row_cache_floor = 274_171_904;
        let page_cache_floor = 477_595_648;
        for (max_bytes, floor) in [(MAX_BYTES, row_cache_floor), (128 * 1024, page_cache_floor)] {
            assert_eq!(floor_bytes(max_bytes), floor, "to {max_bytes}");
            let slabs = Slabs::new(max_bytes);
            let values: Vec<Bytes> = classes(max_bytes)
                .map(|class| slabs.store(&vec![1; class], ()).unwrap())
                .collect();
            assert_eq!(values.len(), classes(max_bytes).count());
            assert_eq!(SLAB_RESERVE_DIVISOR * slabs.chunk_bytes(), floor);
        }
    }

    #[test]
    fn test_reserve_is_a_sixteenth_from_the_floor_and_none_under_it() {
        let floor = floor_bytes(MAX_BYTES);
        assert_eq!(reserve(floor - 1, floor), 0);
        assert_eq!(reserve(floor, floor), floor / SLAB_RESERVE_DIVISOR);
        assert!(!uses_slabs(floor - 1, floor));
        assert!(uses_slabs(floor, floor));
    }

    /// From the budget `budget_holding` names on, every budget's part
    /// beside its reserve holds the entries asked for, under the floor and
    /// across it alike, and a part the floor's holds needs no more budget.
    #[test]
    fn test_budget_holding_holds_the_entries_from_there_on() {
        let floor = floor_bytes(MAX_BYTES);
        let beside = |budget: u64| budget - reserve(budget, floor);
        for entries in [1, floor / 2, beside(floor), floor - 1, floor, 3 * floor] {
            let budget = budget_holding(entries, floor);
            assert!(budget >= entries, "{entries}");
            for at in [budget, budget + 1, floor - 1, floor, floor + 1, 4 * floor] {
                if at >= budget {
                    assert!(beside(at) >= entries, "{entries} at {at}");
                }
            }
        }
        assert_eq!(budget_holding(beside(floor), floor), beside(floor));
        assert!(budget_holding(floor - 1, floor) > floor);
    }

    #[test]
    fn test_charged_follows_the_home() {
        let len = 4242;
        assert_eq!(
            Home::Slot.charged(len),
            approx::count(slab_class(len) + SLOT_HANDLE_BYTES)
        );
        assert_eq!(
            Home::Own.charged(len),
            approx::count(jemalloc_class(len)) + SHARED_HEADER_BYTES
        );
        assert_eq!(Home::Own.charged(0), SHARED_HEADER_BYTES);
    }
}
