//! Slabs for the row cache and the page cache (RFD 030 (Cache slabs)).
//!
//! A slab holds values of one size class in fixed-size chunks, so a cache
//! charges a value its class and no more. This crate holds the size classes
//! and, in its `chunk` module, the only `unsafe` the slabs need (RFD 029
//! (Unsafe code)).

mod class;

pub use class::{
    CHUNK_TARGET_BYTES, MAX_SLOTS_PER_CHUNK, MIN_SLAB_BYTES, MIN_SLOTS_PER_CHUNK, in_slabs,
    jemalloc_class, slab_class, slots_per_chunk,
};
