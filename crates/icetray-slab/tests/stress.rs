//! Many threads storing into, emptying, and reading one set of slabs at
//! once, every read checked against the bytes its key was stored with
//! (RFD 030 (Cache slabs)). Under Miri it runs fewer rounds, which still
//! interleave the threads' stores, frees, and emptying.

use std::collections::VecDeque;
use std::sync::{Arc, Barrier};
use std::thread;

use bytes::Bytes;
use icetray_slab::{ClassId, Emptied, MIN_SLAB_BYTES, Slabs, is_slot};

/// The threads that share the slabs.
const THREADS: usize = 8;

/// The values each thread stores. Miri checks every step it runs, so it
/// takes fifty, which still have every thread store into every class and
/// empty a chunk ten times.
const ROUNDS: usize = if cfg!(miri) { 50 } else { 10_000 };

/// The values each thread keeps at once: each store past it drops the
/// oldest, so chunks fill, drain, and are freed while other threads store
/// into them.
const WINDOW: usize = 8;

/// A thread empties the shared class's sparsest chunk once in this many
/// rounds.
const EMPTY_EVERY: usize = 5;

/// The lengths stored in turn, one in each of three classes: the
/// 1,024-byte class, whose chunk holds 32 slots, the 4,352-byte class,
/// 15, and the 16,128-byte class, 4.
const LENS: [usize; 3] = [MIN_SLAB_BYTES, 4242, 16_000];

/// The longest value the slabs keep: the longest stored.
const MAX_BYTES: usize = LENS[LENS.len() - 1];

/// Multiplies a word's index into its value, so words of one value differ
/// and a read at the wrong offset fails.
const WORD_SPREAD: u64 = 0x9e37_79b9_7f4a_7c15;

/// Which thread stored a value, in which round: the record each slot keeps.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Key {
    thread: usize,
    round: usize,
}

/// A value a thread still holds and the bytes it was stored with.
struct Kept {
    key: Key,
    bytes: Bytes,
    stored: Vec<u8>,
}

/// `len` bytes that differ, word by word, from every other key's and
/// from every other offset of their own. Written a word at a time, since
/// Miri runs each step of a byte-at-a-time loop.
fn value(key: Key, len: usize) -> Vec<u8> {
    let thread = u64::try_from(key.thread).expect("a thread index fits a u64");
    let round = u64::try_from(key.round).expect("a round fits a u64");
    let tag = (thread << u32::BITS) | round;
    let mut value = vec![0; len];
    for (word, bytes) in (0_u64..).zip(value.chunks_mut(size_of::<u64>())) {
        let word = tag
            .wrapping_add(word.wrapping_mul(WORD_SPREAD))
            .to_le_bytes();
        bytes.copy_from_slice(&word[..bytes.len()]);
    }
    value
}

/// One thread's rounds: store a value, drop the oldest past the window,
/// every few rounds empty `class`'s sparsest chunk and drop this thread's
/// values in it, and read back every value still held. Returns how many of
/// its values it dropped because it emptied their chunk.
fn run(slabs: &Slabs<Key>, class: ClassId, thread: usize) -> usize {
    let mut window: VecDeque<Kept> = VecDeque::with_capacity(WINDOW + 1);
    let mut evicted = 0;
    for round in 0..ROUNDS {
        let key = Key { thread, round };
        let stored = value(key, LENS[round % LENS.len()]);
        let bytes = slabs
            .store(&stored, key)
            .expect("every length is in the slabs");
        window.push_back(Kept { key, bytes, stored });
        if window.len() > WINDOW {
            window.pop_front();
        }
        if round % EMPTY_EVERY == 0 {
            for Emptied { record, at, .. } in slabs.empty_sparsest(class) {
                if record.thread != thread {
                    continue;
                }
                let held = window
                    .iter()
                    .position(|kept| kept.key == record)
                    .expect("a slot holding this thread's record is in its window");
                assert!(is_slot(&window[held].bytes, at), "{record:?}");
                window.remove(held);
                evicted += 1;
            }
        }
        for kept in &window {
            assert!(kept.bytes[..] == kept.stored[..], "{:?}", kept.key);
        }
    }
    evicted
}

#[test]
fn test_many_threads_store_drop_and_read_their_own_bytes() {
    let slabs: Slabs<Key> = Slabs::new(MAX_BYTES);
    let probe = Key {
        thread: THREADS,
        round: 0,
    };
    let probed = slabs.store(&value(probe, LENS[0]), probe).unwrap();
    let shared = slabs
        .class_over(0)
        .expect("the only class holding a chunk has free slots");
    drop(probed);
    assert_eq!(slabs.chunk_bytes(), 0);

    let barrier = Arc::new(Barrier::new(THREADS));
    let threads: Vec<_> = (0..THREADS)
        .map(|thread| {
            let (slabs, barrier) = (slabs.clone(), Arc::clone(&barrier));
            thread::spawn(move || {
                barrier.wait();
                run(&slabs, shared, thread)
            })
        })
        .collect();
    let evicted: usize = threads
        .into_iter()
        .map(|handle| handle.join().unwrap())
        .sum();

    assert!(evicted > 0, "no thread emptied a chunk holding its values");
    assert_eq!(slabs.held_slots(), 0);
    assert_eq!(slabs.chunk_bytes(), 0);
    assert_eq!(slabs.uncharged_bytes(), 0);
    assert_eq!(slabs.waiting_bytes(), 0);
}
