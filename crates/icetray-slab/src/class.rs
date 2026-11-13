//! The size classes: jemalloc's, which the caches charge for allocations
//! outside the slabs, and the slabs' own.

use std::iter;

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
/// allocated for no bytes. A `const fn`, so a constant's size class is
/// derived from its length.
pub const fn jemalloc_class(len: usize) -> usize {
    if len == 0 {
        return 0;
    }
    if len <= MIN_SIZE_CLASS_BYTES {
        return MIN_SIZE_CLASS_BYTES;
    }
    let spacing = 1_usize << (len - 1).ilog2().saturating_sub(SIZE_CLASS_BITS);
    let spacing = if spacing < MIN_SIZE_CLASS_BYTES {
        MIN_SIZE_CLASS_BYTES
    } else {
        spacing
    };
    len.next_multiple_of(spacing)
}

/// The shortest value a slab holds. Below it, an entry's fixed costs, its
/// key and its slot in the cache, are already a large share of what it
/// costs, and each doubling covered adds 32 classes.
pub const MIN_SLAB_BYTES: usize = 1024;

/// Each doubling of the length is split into `1 << CLASSES_PER_DOUBLING_BITS`
/// slab classes, thirty-two, so a slab rounds a length up by under a
/// thirty-second. Finer than jemalloc's four per doubling, so every
/// jemalloc class boundary is also a slab class boundary.
const CLASSES_PER_DOUBLING_BITS: u32 = 5;

/// The bytes a chunk aims to hold, which `slots_per_chunk` divides by the
/// class. Sixty-four KiB keeps a chunk small enough to empty and free
/// soon, and large enough that a class of a few KiB fills many slots.
pub const CHUNK_TARGET_BYTES: usize = 64 * 1024;

/// The fewest slots a chunk has, so a class near the largest slab length
/// still shares a chunk's allocation among several slots.
pub const MIN_SLOTS_PER_CHUNK: usize = 4;

/// The most slots a chunk has, which bounds the bookkeeping of one chunk
/// and how many values a freed chunk takes down with it.
pub const MAX_SLOTS_PER_CHUNK: usize = 32;

/// The slab class of a value of `len` bytes: `len` rounded up to a multiple
/// of a thirty-second of the power of two at or below `len - 1`. Defined
/// for `len >= MIN_SLAB_BYTES`.
pub const fn slab_class(len: usize) -> usize {
    let spacing = (1_usize << (len - 1).ilog2()) >> CLASSES_PER_DOUBLING_BITS;
    len.next_multiple_of(spacing)
}

/// A slab class's place among the slab classes from `MIN_SLAB_BYTES`'s,
/// which is 0, so consecutive classes take consecutive places. Defined for
/// a `class` that `slab_class` returns.
pub(crate) const fn class_index(class: usize) -> usize {
    class_ordinal(class) - class_ordinal(slab_class(MIN_SLAB_BYTES))
}

/// A slab class's place among every slab class: the classes of the
/// doublings below its own, then its step above the power of two that
/// starts its doubling.
const fn class_ordinal(class: usize) -> usize {
    let doubling = (class - 1).ilog2();
    let floor = 1_usize << doubling;
    let spacing = floor >> CLASSES_PER_DOUBLING_BITS;
    ((doubling as usize) << CLASSES_PER_DOUBLING_BITS) + (class - floor) / spacing
}

/// The slots in a chunk of slots of `class` bytes: as many as fit in
/// `CHUNK_TARGET_BYTES`, within `MIN_SLOTS_PER_CHUNK` and
/// `MAX_SLOTS_PER_CHUNK`. Written without `clamp`, which is not yet a
/// `const fn`.
pub const fn slots_per_chunk(class: usize) -> usize {
    let fit = CHUNK_TARGET_BYTES / class;
    if fit < MIN_SLOTS_PER_CHUNK {
        MIN_SLOTS_PER_CHUNK
    } else if fit > MAX_SLOTS_PER_CHUNK {
        MAX_SLOTS_PER_CHUNK
    } else {
        fit
    }
}

/// The slab classes of slabs holding values from `MIN_SLAB_BYTES` to
/// `max_bytes` long, shortest first: none when `max_bytes` is under
/// `MIN_SLAB_BYTES`.
pub fn classes(max_bytes: usize) -> impl Iterator<Item = usize> {
    let first = (max_bytes >= MIN_SLAB_BYTES).then(|| slab_class(MIN_SLAB_BYTES));
    iter::successors(first, move |&class| {
        (class < max_bytes).then(|| slab_class(class + 1))
    })
}

/// Whether a value of `len` bytes is stored in a slab, given the longest
/// value `max` the cache keeps there.
pub fn in_slabs(len: usize, max: usize) -> bool {
    MIN_SLAB_BYTES <= len && len <= max
}

#[cfg(test)]
mod tests {
    use super::*;

    const MAX: usize = 128 * 1024;

    #[test]
    fn test_slab_class_rounds_up_by_at_most_a_thirty_second() {
        for len in MIN_SLAB_BYTES..=MAX {
            let class = slab_class(len);
            assert!(class >= len, "{len} -> {class}");
            assert!(class - len < len / 32 + 1, "{len} -> {class}");
        }
    }

    #[test]
    fn test_every_jemalloc_boundary_is_a_slab_boundary() {
        for len in MIN_SLAB_BYTES..=MAX {
            assert!(slab_class(len) <= jemalloc_class(len), "{len}");
        }
    }

    #[test]
    fn test_slab_class_of_measured_lengths() {
        assert_eq!(slab_class(1024), 1024);
        assert_eq!(slab_class(4096), 4096);
        assert_eq!(slab_class(4242), 4352);
        assert_eq!(slab_class(65_536), 65_536);
        assert_eq!(slab_class(65_637), 67_584);
    }

    #[test]
    fn test_class_index_counts_the_classes_in_order() {
        let mut previous = slab_class(MIN_SLAB_BYTES);
        let mut expected = 0;
        for len in MIN_SLAB_BYTES..=MAX {
            let class = slab_class(len);
            if class != previous {
                previous = class;
                expected += 1;
            }
            assert_eq!(class_index(class), expected, "{len} -> {class}");
        }
        assert_eq!(class_index(slab_class(MIN_SLAB_BYTES)), 0);
    }

    #[test]
    fn test_slots_per_chunk_stays_within_its_bounds() {
        assert_eq!(slots_per_chunk(1024), 32);
        assert_eq!(slots_per_chunk(4352), 15);
        assert_eq!(slots_per_chunk(67_584), 4);
        assert_eq!(slots_per_chunk(128 * 1024), 4);
    }

    #[test]
    fn test_classes_are_every_slab_class_in_the_range_in_order() {
        let mut expected: Vec<usize> = (MIN_SLAB_BYTES..=MAX).map(slab_class).collect();
        expected.dedup();
        assert_eq!(classes(MAX).collect::<Vec<_>>(), expected);
        assert_eq!(
            classes(MIN_SLAB_BYTES).collect::<Vec<_>>(),
            [MIN_SLAB_BYTES]
        );
        assert_eq!(classes(MIN_SLAB_BYTES - 1).count(), 0);
        assert_eq!(classes(0).count(), 0);
    }

    #[test]
    fn test_in_slabs_edges() {
        assert!(!in_slabs(1023, MAX));
        assert!(in_slabs(1024, MAX));
        assert!(in_slabs(MAX, MAX));
        assert!(!in_slabs(MAX + 1, MAX));
        assert!(!in_slabs(0, MAX));
    }
}
