# Cache slabs: implementation plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** The row cache and the page cache's memory level hold values and
pages of 1 KiB or more in slabs of icetray's own size classes, 32 per
doubling, each slab charged in full against its cache's budget and each
hit handed a reference to its slot, with the `unsafe` this needs confined
to one crate that Miri tests.

**Architecture:** A new crate, `icetray-slab`, owns the size classes and
a `Slabs<R>` pool: per class, chunks of a few to a few dozen slots, a
lock per class, and for each held slot the record `R` of the entry that
holds it. `Slabs::store` copies bytes into a slot and returns a `Bytes`
that reads the slot in place (`Bytes::from_owner`) and frees it when the
last clone drops. Each cache owns one `Slabs`, sets its own capacity to
its budget less what its slabs hold uncharged (free slots, and chunks
being emptied), and evicts the entries of a class's sparsest chunk when
that class's free slots pass a share of the budget. The `unsafe` lives in
`icetray-slab`'s `chunk` module alone.

**Tech Stack:** Rust 1.98.1 (the pinned stable), `bytes` (existing,
`Bytes::from_owner`), quick_cache (row cache, existing), foyer 0.22.6
(page cache, existing), a pinned nightly with the `miri` component for
the Miri step only.

**Spec:** `docs/rfd/030-cache-slabs.md`, under `docs/rfd/029-unsafe-code.md`.
Evidence: `docs/analysis/2026-11-11-cache-slabs.md`. Revised by this
work: RFDs 026, 027, 028, `docs/operating.md`.

## Global Constraints

- `CONTRIBUTING.md` and `AGENTS.md` apply in full. Read both first.
- cargo runs through flox: `flox activate -- cargo ...`, under `nice`.
- Versions come from the registry: `cargo add`, never a version from
  memory. The nightly's date is the newest nightly that rustup's
  component history lists with `miri` for both `aarch64-apple-darwin`
  and `x86_64-unknown-linux-gnu` on the day Task 4 runs. Installing it
  is a download: ask the maintainer first, naming the toolchain.
- Never `#[allow(...)]`. `#[expect(dead_code, reason = "...")]` only for
  code a later task reads, removed in that task.
- `unsafe` only in `crates/icetray-slab/src/chunk.rs`, every block with
  a `// SAFETY:` comment naming one of the three invariants below.
  RFD 029's lint gate (`undocumented_unsafe_blocks`) already enforces
  the comment's presence.
- The three invariants (RFD 030), stated in `chunk.rs`'s module doc and
  named by every `SAFETY` comment:
  1. A slot is written only while nothing refers to it.
  2. A chunk outlives every slot in it.
  3. Slots lie inside their chunk, apart from each other.
- No sleeps in tests. Concurrency tests synchronize on `Barrier`s and
  joins, never on time.
- Comments state constraints; history goes in commit messages.
- Conventional Commits, no co-author or generated-by trailers, never
  bypass the pre-commit hook, never push.
- A user-visible change updates `docs/operating.md` (metrics) and the
  RFDs it changes in the same commit.
- Constants this plan fixes, each a named constant with a doc comment
  giving its reason:
  - `icetray-slab`: `MIN_SLAB_BYTES: usize = 1024`;
    `CLASSES_PER_DOUBLING_BITS: u32 = 5` (32 classes);
    `CHUNK_TARGET_BYTES: usize = 64 * 1024`;
    `MIN_SLOTS_PER_CHUNK: usize = 4`; `MAX_SLOTS_PER_CHUNK: usize = 32`.
  - `icetray-core`: `SLAB_RESERVE_DIVISOR: u64 = 16` (a cache's entries
    get its budget less a sixteenth, set only when the budget changes,
    and the slabs' uncharged bytes past that reserve empty the sparsest
    chunk of the class with the most free slots; the maintainer chose
    this on 2026-11-12, since quick_cache shrinks its ghost list on every
    capacity change); a cache uses slabs only while its budget is at
    least `SLAB_RESERVE_DIVISOR` times its largest chunk; `MAX_SLAB_PAGE_BYTES: usize = 2 * VALUE_PAGE_BYTES`
    (the largest page the page cache keeps in slabs: a byte-bounded page
    closes after the value that crosses 64 KiB, and a value is at most
    64 KiB).
- Gate per task: `cargo test -p <crate>`. At the end: `dev/ci-checks.sh`,
  `cargo test --workspace`, `dev/miri.sh`, all read to completion, and
  the workspace tests on Linux in the colima container with `TMPDIR` on
  an ext4 volume.

## Review Focus

1. A `Bytes` a lookup returned is read after its entry was evicted and
   its chunk chosen for emptying: it reads the bytes stored, and the
   chunk is freed only when that `Bytes` drops (Task 3).
2. A budget smaller than one chunk of the class a value takes: the cache
   is not reported off, a value that does not fit is not cached, and the
   chunk it was copied into is freed (Task 5).
3. Lengths at the edges: 1,023 bytes stays on jemalloc; 1,024 takes the
   1,024 class; 4,242 takes 4,352; 65,536 takes 65,536; `MAX + 1` stays
   on jemalloc; an empty value stays on jemalloc (Tasks 1 and 5).
4. Under the Hybrid tier, a page whose chunk is emptied leaves memory as
   an evicted page does and is then served from the disk level with no
   fetch (Task 6).
5. A budget resized to zero and back: the slabs, which outlive the
   quick_cache a resize from zero rebuilds, free every chunk at zero
   and the cache caches again after growing (Task 5).

---

### Task 1: The `icetray-slab` crate and its size classes

**Files:**
- Create: `crates/icetray-slab/Cargo.toml`, `crates/icetray-slab/src/lib.rs`,
  `crates/icetray-slab/src/class.rs`
- Modify: `crates/icetray-core/src/row_cache.rs` (use the moved
  `jemalloc_class`, delete `size_class` and its constants)
- Modify: `docs/rfd/030-cache-slabs.md` (the `unsafe` bullet names the
  crate and its `chunk` module)

**Interfaces:**
- Produces, in `icetray_slab`:
  - `pub const fn jemalloc_class(len: usize) -> usize`: today's
    `row_cache::size_class`, moved unchanged with its tests.
  - `pub const fn slab_class(len: usize) -> usize`: `len` rounded up to a
    multiple of `2^(ilog2(len - 1)) >> CLASSES_PER_DOUBLING_BITS`, for
    `len >= MIN_SLAB_BYTES`.
  - `pub const fn slots_per_chunk(class: usize) -> usize`:
    `(CHUNK_TARGET_BYTES / class).clamp(MIN_SLOTS_PER_CHUNK, MAX_SLOTS_PER_CHUNK)`.
  - `pub fn in_slabs(len: usize, max: usize) -> bool`:
    `MIN_SLAB_BYTES <= len && len <= max`.

- [ ] **Step 1:** Create the crate with `[lints] workspace = true`, the
  workspace package fields, and `bytes` added by `cargo add bytes -p
  icetray-slab`, which takes the workspace's declaration. `lib.rs` starts with a `//!` header naming RFD 030 and
  RFD 029. Add `icetray-slab` to `icetray-core` with `cargo add --path`.
- [ ] **Step 2: Write the failing tests** in `class.rs`:
  - `test_slab_class_rounds_up_by_at_most_a_thirty_second`: for every
    `len` in `MIN_SLAB_BYTES..=128 * 1024`, `slab_class(len) >= len` and
    `slab_class(len) - len < len / 32 + 1`.
  - `test_every_jemalloc_boundary_is_a_slab_boundary`: for every `len`
    in that range, `slab_class(len) <= jemalloc_class(len)`.
  - `test_slab_class_of_measured_lengths`: `slab_class(1024) == 1024`,
    `slab_class(4096) == 4096`, `slab_class(4242) == 4352`,
    `slab_class(65_536) == 65_536`, `slab_class(65_637) == 67_584`.
  - `test_slots_per_chunk_stays_within_its_bounds`:
    `slots_per_chunk(1024) == 32`, `slots_per_chunk(4352) == 15`,
    `slots_per_chunk(67_584) == 4`, `slots_per_chunk(128 * 1024) == 4`.
  - `test_in_slabs_edges`: `!in_slabs(1023, M)`, `in_slabs(1024, M)`,
    `in_slabs(M, M)`, `!in_slabs(M + 1, M)`, `!in_slabs(0, M)`.
- [ ] **Step 3:** Run `cargo test -p icetray-slab`; the tests fail to
  compile. Implement; they pass. `cargo test -p icetray-core` passes
  with `row_cache` using `icetray_slab::jemalloc_class`.
- [ ] **Step 4:** RFD 030's `unsafe` bullet: "allowed in the
  `icetray-slab` crate's `chunk` module under RFD 029 (Unsafe code)".
- [ ] **Step 5: Commit** `feat(icetray-slab): the slab size classes, 32
  per doubling from 1 KiB`.

### Task 2: Chunks, slots, and `Slabs::store`

**Files:**
- Create: `crates/icetray-slab/src/chunk.rs` (all of the crate's
  `unsafe`), `crates/icetray-slab/src/slabs.rs`

**Interfaces:**
- Produces:
  - `pub struct Slabs<R>` (an `Arc` inside, `Clone`), `R: Clone + Send +
    Sync + 'static`, with `pub fn new(max_bytes: usize) -> Slabs<R>`.
  - `pub fn store(&self, bytes: &[u8], record: R) -> Option<Bytes>`:
    `None` when `!in_slabs(bytes.len(), max_bytes)`; otherwise a `Bytes`
    of exactly `bytes.len()` that reads its slot in place.
  - `pub fn chunk_bytes(&self) -> u64`, `pub fn held_slots(&self) -> u64`.
- `chunk.rs` holds `Chunk` (one allocation of `class * slots` bytes,
  aligned to 16, freed on drop) and `Slot` (the `Bytes` owner: the pool's
  `Arc`, class index, chunk id, slot index, pointer, length). `Slot`
  implements `AsRef<[u8]>` over `len` bytes and returns its slot on drop.
  `Chunk` and `Slot` are `Send` and `Sync` by `unsafe impl`, each
  justified by the invariants.

Per class, under one `Mutex`: chunk states (`Vec<Option<ChunkState<R>>>`
indexed by chunk id, with freed ids reused), and a
`BTreeSet<(u32, ChunkId)>` of (held slots, id) over the chunks that have
a free slot and are not being emptied. A chunk state holds its `Chunk`,
its free slot indices, its held count, whether it is being emptied, and
a `Vec<Option<R>>` of each slot's record. A store takes the set's last
element, the fullest chunk with room, or allocates a chunk when the set
is empty; pops a free index; records `R`; updates the set; releases the
lock; then copies the bytes into the slot (invariant 1: nothing refers
to the slot until `store` returns). A slot's drop takes the lock,
returns the index, clears the record, decrements the held count, and
frees the chunk when no slot is held (invariant 2).

- [ ] **Step 1: Write the failing tests** in `slabs.rs`:
  - `test_store_reads_back_the_bytes_stored`: lengths 1,024, 4,242, and
    `max_bytes`, each a distinct pattern; each `Bytes` equals its input.
  - `test_store_refuses_lengths_outside_the_classes`: 1,023 and
    `max_bytes + 1` give `None`, and `chunk_bytes() == 0`.
  - `test_a_freed_slot_is_reused_before_a_chunk_is_added`: fill one
    chunk of the 4,352 class, drop one `Bytes`, store again:
    `chunk_bytes()` is one chunk.
  - `test_a_chunk_is_freed_when_its_last_slot_drops`: store, drop every
    `Bytes`: `chunk_bytes() == 0`, `held_slots() == 0`.
  - `test_a_clone_keeps_its_slot`: clone a `Bytes`, drop the original,
    the clone reads the bytes and `held_slots() == 1`.
  - `test_stores_fill_the_fullest_chunk_first`: 64 values of the 1,024
    class fill two chunks; drop 4 of the first chunk's and 20 of the
    second's; store one more; drop the second chunk's other 12:
    `chunk_bytes()` is one chunk, so the store went to the first.
- [ ] **Step 2:** Run them; they fail. Implement; they pass.
- [ ] **Step 3: Commit** `feat(icetray-slab): slabs store bytes in slots
  and hand them out without copying`.

### Task 3: Emptying, accounting, and lock waits

**Files:**
- Modify: `crates/icetray-slab/src/slabs.rs`

**Interfaces:**
- Produces:
  - `#[derive(Clone, Copy, Debug, PartialEq, Eq)] pub struct ClassId(u16)`,
    a class's index in its pool.
  - `pub fn uncharged_bytes(&self) -> u64`: bytes of free slots in chunks
    not being emptied, plus the whole of every chunk being emptied. A
    cache's capacity is its budget less this.
  - `pub fn waiting_bytes(&self) -> u64`: bytes of the chunks being
    emptied (the gauge).
  - `pub fn class_over(&self, free_bytes: u64) -> Option<ClassId>`: a
    class whose free slots in chunks not being emptied pass
    `free_bytes`.
  - `pub fn empty_sparsest(&self, class: ClassId) -> Vec<Emptied<R>>`:
    marks the class's chunk with the fewest held slots as being emptied,
    takes it out of the set so no store lands in it, and returns each
    held slot's record. `Emptied<R> { pub record: R, pub at: usize }`,
    `at` the slot's address, which `pub fn is_slot(bytes: &[u8], at:
    usize) -> bool` compares, so a cache drops an entry only if it still
    holds that slot.
  - `pub fn lock_wait_nanos(&self) -> u64`: time stores and frees spent
    waiting on class locks, measured only when `try_lock` fails.
- Per-class and pool totals are atomics updated under the class lock,
  read without it.

- [ ] **Step 1: Write the failing tests**:
  - `test_uncharged_counts_free_slots_and_waiting_chunks`: one chunk of
    32 slots of the 1,024 class with 3 held: `uncharged_bytes() == 29 *
    1024`; after `empty_sparsest`, `uncharged_bytes() == 32 * 1024` and
    `waiting_bytes() == 32 * 1024`.
  - `test_empty_sparsest_names_the_held_slots_and_takes_no_new_store`:
    the returned records are the three stored; a store after it
    allocates a new chunk.
  - `test_an_emptied_chunk_is_freed_when_its_last_reader_drops`: keep
    one returned `Bytes` alive past `empty_sparsest` and the drops of
    the others: it still reads its bytes, `waiting_bytes()` is one
    chunk; drop it: `chunk_bytes() == 0`, `waiting_bytes() == 0`.
  - `test_class_over_ignores_chunks_being_emptied`.
  - `test_is_slot_matches_only_its_own_slot`.
- [ ] **Step 2:** Run them; they fail. Implement; they pass.
- [ ] **Step 3: Commit** `feat(icetray-slab): slabs empty their sparsest
  chunk on request and count what they hold uncharged`.

### Task 4: Miri, the stress test, and the second toolchain

**Files:**
- Create: `dev/miri.sh`, `crates/icetray-slab/tests/stress.rs`
- Modify: `.github/workflows/ci.yml` (a `miri` job), `CONTRIBUTING.md`
  (Toolchain: a paragraph on the nightly pin, as its Formatting section
  says a second toolchain arrives)

**Interfaces:**
- `dev/miri.sh` names the pinned nightly once (`NIGHTLY=nightly-YYYY-MM-DD`,
  chosen as Global Constraints says), installs it with the `miri` and
  `rust-src` components when missing, and runs
  `cargo +$NIGHTLY miri test -p icetray-slab`. The CI job runs the same
  script after `rustup toolchain install` of the stable pin.

- [ ] **Step 1:** Ask the maintainer to approve installing the nightly,
  naming it. Then write `stress.rs`:
  `test_many_threads_store_drop_and_read_their_own_bytes`: eight threads
  released by one `Barrier`, each storing values whose bytes derive from
  (thread, round) at lengths across three classes, keeping a bounded
  window of `Bytes`, dropping the oldest, calling `empty_sparsest` on a
  shared class every few rounds, and checking every kept `Bytes`
  against the bytes it was stored with. Rounds: 10,000 natively, 50
  under `cfg(miri)`. At the end every `Bytes` is dropped and
  `chunk_bytes() == 0`.
- [ ] **Step 2:** `dev/miri.sh` passes: Miri runs every test in the
  crate, the unit tests and the stress test, with no undefined
  behavior reported. A test that is too slow under Miri takes a smaller
  count under `cfg(miri)`, never `#[cfg_attr(miri, ignore)]`.
- [ ] **Step 3:** The CI job, and the CONTRIBUTING paragraph: the
  nightly is pinned in `dev/miri.sh`, used for Miri alone, bumped in a
  commit of its own, and every crate whose `unsafe` manages memory
  (RFD 029) runs under it.
- [ ] **Step 4: Commit** `ci: Miri runs the slab crate's tests on a
  pinned nightly`.

### Task 5: The row cache on slabs

**Files:**
- Modify: `crates/icetray-core/src/row_cache.rs`, `crates/icetray-core/src/metric.rs`,
  `docs/operating.md`, `docs/rfd/026-row-cache.md`

**Interfaces:**
- Consumes: Tasks 2 and 3.
- `EntryKey` holds `Arc<[u8]>` so a slot's record shares the key's
  allocation; `Inner` gains `slabs: Slabs<EntryKey>` with
  `max_bytes = ROW_CACHE_MAX_VALUE_BYTES`, kept across a resize from
  zero; `Built` keeps the budget it was given apart from the entries'
  capacity, and `is_off` reads the budget.
- `entry` stores a value through `Slabs::store` when it is in the slabs'
  range, else as today. The weigher charges a slab value its slot class
  plus the `Bytes::from_owner` header, `jemalloc_class` otherwise, and
  the key's `Arc` allocation.
- After each insert and each `resize`, the cache empties the sparsest
  chunk of any class over `budget / SLAB_FREE_SHARE_DIVISOR`, removing
  each returned record with `remove_if` where the value `is_slot`, and
  re-sets its capacity to `budget - uncharged_bytes()` when that has
  moved by more than `budget / SLAB_CAPACITY_GRANULE_DIVISOR` since it
  was last set.
- Metrics: `icetray_row_cache_waiting_bytes` (gauge) and
  `icetray_row_cache_slab_wait_seconds_total` (counter), published in
  `RowCache::publish`; `icetray_row_cache_bytes` reports entries plus
  uncharged slab bytes.

- [ ] **Step 1: Write the failing tests** in `row_cache.rs`:
  - `test_a_slab_value_is_charged_its_slot`: a 4,242-byte value's entry
    weighs `slab_class(4242)` plus the header plus the key, not
    `jemalloc_class(4242)`.
  - `test_values_under_a_kibibyte_stay_on_jemalloc`: 1,023 bytes and an
    empty value are charged as before.
  - `test_the_budget_counts_free_slots`: fill to the budget, evict half
    by inserting another class: `usage()` (entries plus uncharged) stays
    at or under the budget plus one granule.
  - `test_a_budget_under_one_chunk_is_not_off`: a budget of 2 KiB, one
    1,024-byte value: `is_off()` is false, the value is not cached, and
    the slabs hold no chunk afterward.
  - `test_resize_to_zero_frees_every_chunk_and_growing_caches_again`.
  - `test_a_hit_hands_out_the_slot`: two hits return `Bytes` with the
    same `as_ptr()`.
  - Update `test_a_value_over_the_bound_is_not_cached` and the charge
    tests to the new weights.
- [ ] **Step 2:** Run them; they fail. Implement; they pass with the
  rest of `cargo test -p icetray-core`.
- [ ] **Step 3:** `docs/operating.md` rows for the two new series and
  the revised bytes row; RFD 026's update note and "What an entry
  weighs" in the present tense.
- [ ] **Step 4: Commit** `feat(icetray-core): the row cache holds values
  of 1 KiB or more in slabs`.

### Task 6: The page cache on slabs

**Files:**
- Modify: `crates/icetray-core/src/cache.rs`, `crates/icetray-core/src/metric.rs`,
  `docs/operating.md`, `docs/rfd/027-one-page-cache.md`,
  `docs/rfd/030-cache-slabs.md`

**Interfaces:**
- Consumes: Tasks 2 and 3, Task 5's accounting pattern.
- `PageCache` owns `Slabs<PageKey>` with `max_bytes = MAX_SLAB_PAGE_BYTES`.
  `PageValue` says whether its bytes are a slot. `insert` stores a page
  in a slot when it is in range. Every page the memory level holds is
  charged what it costs: its slot plus the header, or `jemalloc_class`
  of its own allocation plus `ENTRY_OVERHEAD_BYTES` (a page foyer
  decodes from the disk level into memory included).
- The memory level's capacity is the page part less its reserve, set
  only when the budget changes, through `cache.memory().resize`; past the
  reserve, the cache empties chunks as Task 5's row cache does, and below
  the slab floor (16 times the largest page chunk, 8 MiB) pages stay on
  jemalloc. Charges use `icetray_slab`'s exported owner-handle size.
- Emptying a chunk removes its pages from the memory level the way foyer
  evicts them, so under Hybrid they reach the disk level. Read foyer
  0.22.6's source for the call that does this; if none does, write the
  page to the disk level before removing it from memory.
- RFD 028's floor stands: a page part under the slab floor keeps pages
  on jemalloc, so the floor needs no chunk term. RFD 030's Consequences
  sentence that RFD 028's smallest budget is revised becomes that.
- Metrics: `icetray_page_cache_waiting_bytes`,
  `icetray_page_cache_slab_wait_microseconds_total` (waits by stores,
  frees, and emptying), published with
  `publish_usage`.

- [ ] **Step 1: Write the failing tests** in `cache.rs`:
  - `test_a_page_just_over_64_kib_is_charged_its_slot`: a 65,637-byte
    page weighs `slab_class(65_637)` plus the header, where it weighed
    its length plus `ENTRY_OVERHEAD_BYTES`.
  - `test_a_page_outside_the_classes_is_charged_its_jemalloc_class`.
  - `test_an_emptied_page_is_served_from_the_disk_level` (Hybrid): fill,
    force a chunk's emptying, read the page: no fetch, served by the
    disk level.
  - `test_a_memory_hit_hands_out_the_slot`.
  - `test_a_page_part_under_the_slab_floor_keeps_pages_on_jemalloc`.
  - `test_the_memory_level_capacity_changes_only_with_the_budget`.
  - Update the `usage` tests to the new charges.
- [ ] **Step 2:** Run them; they fail. Implement; they pass with
  `cargo test -p icetray-core` and `cargo test -p icetray-testkit`.
- [ ] **Step 3:** RFD 027's note and Memory bullet in the present tense;
  RFD 030's Consequences sentence on RFD 028's floor as above;
  `docs/operating.md` rows.
- [ ] **Step 4: Commit** `feat(icetray-core): the page cache's memory
  level holds pages in slabs and charges each page what it costs`.

### Task 7: Measure the decided design

**Files:**
- Create: `docs/analysis/YYYY-MM-DD-cache-slabs-decided.md` (the run's date)
- Modify: `docs/rfd/030-cache-slabs.md` (Consequences: what was measured)

- [ ] **Step 1:** With the maintainer's applications closed, rebuild
  the release bench and run the replay of
  `docs/analysis/2026-11-11-cache-slabs.md` on the full-size embeddings
  table: quarter sample, 4 GiB and 8 GiB, one budget per invocation,
  row cache on, both modes, twice at 8 GiB. Record entries, row hit
  rate, fetches per request, ops per second, allocated memory over the
  fill, and the waiting and slab-wait series.
- [ ] **Step 2:** The analysis compares these with the earlier
  analysis's baseline and exact-length rows, and says how far the
  decided classes fall short of exact lengths. A row that stopped for
  memory is rerun, not reported.
- [ ] **Step 3:** RFD 030's Consequences replaces "still to be
  measured" with what the run showed, linking the analysis.
- [ ] **Step 4: Commit** `docs(analysis): the decided slab classes on
  the embedding trace`.

### Task 8: The held performance section

**Files:**
- Modify: `docs/performance.md` (the uncommitted "Realistic workloads,
  measured" section)

- [ ] **Step 1:** Rerun the section's campaign at HEAD, as the section
  describes it: the embedding-batch and per-user replays at a quarter
  sample on the 128-file `scattered` table, the documents unsampled,
  and the fitted workloads beside each, at 1, 2, 4, and 8 GiB, with the
  row cache on and off, the maintainer's applications closed for the
  `scattered` runs. Replace every figure the slabs or 27042a4 changed:
  the hit rates, keys per second, bytes per entry, and allocated bytes
  per byte of budget.
- [ ] **Step 2:** Commit `docs(performance): realistic workloads,
  measured`.
