//! What jemalloc allocates for a request, which the caches charge their
//! entries against the budget: the row cache a value and its key, the page
//! cache a page.

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
/// derived from its length. A large allocation's page of padding is not
/// counted: the workspace builds jemalloc with `cache_oblivious:false`
/// (`.cargo/config.toml`).
pub(crate) const fn size_class(len: usize) -> usize {
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

/// Bytes of the shared header the first clone of a cached `Bytes` that owns
/// its buffer allocates, which every cached value or page that is read pays
/// once: `bytes`' `Shared`, a buffer pointer, its capacity, and a reference
/// count.
const SHARED_HEADER_LEN: usize = 24;

/// What the shared header takes in jemalloc's size class
/// (`SHARED_HEADER_LEN`).
pub(crate) const SHARED_HEADER_BYTES: usize = size_class(SHARED_HEADER_LEN);

#[cfg(test)]
mod tests {
    use super::*;

    /// Sizes round up within a doubling's four classes, and to sixteen bytes
    /// at least.
    #[test]
    fn test_size_class_rounds_to_jemalloc_classes() {
        assert_eq!(size_class(0), 0);
        assert_eq!(size_class(1), 16);
        assert_eq!(size_class(17), 32);
        assert_eq!(size_class(100), 112);
        assert_eq!(size_class(1029), 1280);
        assert_eq!(size_class(4096), 4096);
        assert_eq!(size_class(4097), 5120);
        assert_eq!(SHARED_HEADER_BYTES, 32);
    }
}
