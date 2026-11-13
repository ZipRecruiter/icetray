//! What the row cache and the page cache share about their slabs (RFD 030
//! (Cache slabs)): where a value lives and what it is charged there, the
//! floor under which a cache keeps every value in an allocation of its
//! own, the reserve of its budget held for the slabs' bytes no entry is
//! charged for, and how a cache empties chunks to stay within it.

use icetray_slab::{
    ClassId, Emptied, SLOT_HANDLE_BYTES, Slabs, jemalloc_class, slab_class, slots_per_chunk,
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
/// many of its largest chunk (`floor_bytes`), so the reserve holds a chunk
/// of any class; under that, values stay in allocations of their own and
/// no reserve is held.
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
/// `max_bytes` long keeps values there: `SLAB_RESERVE_DIVISOR` times the
/// bytes of a chunk of the class of `max_bytes`, the largest, so the
/// reserve holds one chunk of any class.
pub(crate) fn floor_bytes(max_bytes: usize) -> u64 {
    let largest = slab_class(max_bytes);
    SLAB_RESERVE_DIVISOR * approx::count(slots_per_chunk(largest) * largest)
}

/// Whether `budget` keeps values in slabs holding up to `max_bytes`
/// (`floor_bytes`).
pub(crate) fn uses_slabs(budget: u64, max_bytes: usize) -> bool {
    budget >= floor_bytes(max_bytes)
}

/// The bytes of `budget` held in reserve for slabs holding up to
/// `max_bytes`: a sixteenth (`SLAB_RESERVE_DIVISOR`) while the budget keeps
/// values in slabs, else none, since no store then fills a chunk.
pub(crate) fn reserve(budget: u64, max_bytes: usize) -> u64 {
    if uses_slabs(budget, max_bytes) {
        budget / SLAB_RESERVE_DIVISOR
    } else {
        0
    }
}

/// A budget from which on the part beside the reserve of slabs holding up
/// to `max_bytes` holds at least `entries` bytes: `entries` itself, unless
/// the floor's part is under it, since a budget at the floor holds back a
/// reserve that one just under it does not; then the budget whose part
/// beside its sixteenth holds `entries`.
pub(crate) fn budget_holding(entries: u64, max_bytes: usize) -> u64 {
    let floor = floor_bytes(max_bytes);
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
    use super::*;
    use crate::approx::units::MIB;

    /// The longest value the tests' slabs hold.
    const MAX_BYTES: usize = 64 * 1024;

    #[test]
    fn test_reserve_is_a_sixteenth_from_the_floor_and_none_under_it() {
        let floor = floor_bytes(MAX_BYTES);
        assert_eq!(floor, 4 * MIB);
        assert_eq!(reserve(floor - 1, MAX_BYTES), 0);
        assert_eq!(reserve(floor, MAX_BYTES), floor / SLAB_RESERVE_DIVISOR);
        assert!(!uses_slabs(floor - 1, MAX_BYTES));
        assert!(uses_slabs(floor, MAX_BYTES));
    }

    /// From the budget `budget_holding` names on, every budget's part
    /// beside its reserve holds the entries asked for, under the floor and
    /// across it alike, and a part the floor's holds needs no more budget.
    #[test]
    fn test_budget_holding_holds_the_entries_from_there_on() {
        let floor = floor_bytes(MAX_BYTES);
        let beside = |budget: u64| budget - reserve(budget, MAX_BYTES);
        for entries in [1, floor / 2, beside(floor), floor - 1, floor, 3 * floor] {
            let budget = budget_holding(entries, MAX_BYTES);
            assert!(budget >= entries, "{entries}");
            for at in [budget, budget + 1, floor - 1, floor, floor + 1, 4 * floor] {
                if at >= budget {
                    assert!(beside(at) >= entries, "{entries} at {at}");
                }
            }
        }
        assert_eq!(budget_holding(beside(floor), MAX_BYTES), beside(floor));
        assert!(budget_holding(floor - 1, MAX_BYTES) > floor);
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
