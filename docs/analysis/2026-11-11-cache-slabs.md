# Row cache capacity in exact-length slots

Measured 2026-11-11 on a laptop with the benchmark program, comparing the
row cache as it is, each value in an allocation of its own, with a
throwaway build that keeps values in slots of exactly their length. The
measurement is the evidence behind
[RFD 030](../rfd/030-cache-slabs.md), which decides on finer size classes
than these exact lengths; what that changes is under
[what they do not show](#what-they-do-not-show).

A 4,242-byte value, an embedding's size, takes jemalloc's 5,120-byte size
class, so the row cache charged an entry 5,336 bytes. In slots of exactly
the value's length an entry cost 4,474 bytes, so a budget held 19% more
entries in the same allocated memory, and on a production trace's replay
at a full-size 8 GiB budget the share of lookups that missed the row
cache fell from 9.6% to 7.4%. Throughput then depended on how a hit hands
out its value. A hit that copied the value out of its slot lost 8 to 17%
of single-task throughput, despite the higher hit rate. A hit that handed
out a reference to the slot, as a hit hands out a reference to its
allocation today, gained 12 to 17% single-task and 18 to 34% across 11
tasks on the full-size table at 8 GiB, and brought the embeddings' hit rate to within 0.4 points
at 8 GiB and 0.7 points at 4 GiB of a table whose values fit a size class
exactly.

## Why it was measured

Replays of the trace below on the 2026-11-10 night had shown the
embeddings table's row cache holding 0.81 times the entries the scattered
table's held at each budget, and hitting at each budget about as often as
the scattered table's curve, interpolated between the measured budgets,
does at 0.8 times that budget, within about a point, from 2 to 8 GiB. No
scattered run was made at 0.8 times a budget. The scattered table's
values are 4,096 bytes, a size class of their own; the embeddings' are
4,242, which round to 5,120, and 4,096 over 5,120 is 0.8. The rounding is
jemalloc's: four size classes per doubling, so a value just over a power
of two loses up to a fifth of its allocation. Embeddings often land
there, since a vector of a power of two of 4-byte floats plus a small
header is just over a power of two.

## Method

- **Host.** An Apple M3 Pro (11 cores, 36 GiB) on macOS 26.6. The
  benchmark program counts allocations through jemalloc and stops a run
  once its faults read more than 16 MiB of the process back into RAM, so
  a run reports either icetray's own time or nothing.
- **Program.** `crates/icetray-testkit/examples/bench.rs` in a release
  build of a throwaway branch off `main` at `92453be`, which added the
  slots to `crates/icetray-core/src/row_cache.rs` under an environment
  variable. Every row without slots is that binary with the variable
  unset; the baselines from the night before are a `main` build whose
  results record no commit. The branch was not merged; its slab is
  described below so the result does not depend on it.
- **Tables.** The benchmark's embeddings table: 4.8 million random-looking
  string keys in 128 files of 37,500 rows, 4,242-byte values that zstd
  compresses 1.61 times, as embeddings do, about 12 GiB of Parquet. A
  smaller copy of 16 files and 600,000 rows served the first two rounds.
- **Workloads.** At full size, a replay of a quarter sample of a
  production trace of a recommendation service's embedding-batch lookups,
  each cache budget scaled by the sample rate so a row stands for its
  full-size budget, as the benchmark's `replay` does. On the small table,
  the benchmark's `fitted` embedding-batch workload over its 600,000 keys,
  its budgets scaled the same way. Every run read through the row cache,
  under the benchmark's default split of the budget, one budget per
  invocation. Each run filled the caches unmeasured, then measured up to
  60 seconds single-task and up to 60 across 11 tasks. At 8 GiB the
  workload ran out first: the windows ended after 17 to 34 seconds on the
  small table, and across 11 tasks after 31 to 42 seconds on the full
  one. Where the workload ran out, both configurations of a pair replayed
  the same requests and the faster one ended sooner; a window that ran
  its full 60 seconds measured as many requests as fit in it.
- **Conditions.** The small-table runs ran with desktop applications open;
  the full-size runs with them closed, as the 2026-11-10 baselines were.
  Throughput is compared only between runs made in the same conditions.
  At 8 GiB each configuration ran twice, alternating, and the tables
  give both runs.
- **The slots.** One slab per value length, of 1 MiB chunks holding as
  many slots as fit, a free list of slots, and one mutex per slab taken
  to store a value, including its copy into the slot, and to free a slot,
  with a second mutex over the map of slabs on every store. A value of
  1 KiB or more went into its length's slab; smaller values, nulls, and
  presence entries stayed as they are. An entry was charged its key, its
  slot in the cache, the handle on its value, and the value's exact
  length. No chunk was ever freed. The first build's hit copied the value
  out of its slot into a new allocation; the second wrapped each slot
  once, at insert, in a `Bytes` that reads the slot in place and frees it
  when the last clone drops, so a hit cloned that `Bytes` as a hit clones
  its value today.

## Results

### Small table, copying hits

| Budget (full-size) | Slots | Bytes per entry | Entries | Row hit rate | Fetches per request | Single-task ops/s | Single-task p50 |
|---|---|---|---|---|---|---|---|
| 8 GiB | off | 5,336 | 171,000 | 89.7% | 32 | 996, 962 | 237 µs, 217 µs |
| 8 GiB | on | 4,458 | 205,000 | 92.2% | 24 | 829, 876 | 561 µs, 540 µs |
| 2 GiB | off | 5,336 | 42,816 | 55.4% | 146 | 248 | 1.49 ms |
| 2 GiB | on | 4,458 | 51,244 | 60.4% | 130 | 228 | 1.57 ms |

At 8 GiB the median more than doubled. A request of about 340 keys took
about 300 row hits, and each hit now took the slab's lock, allocated
4,242 bytes, and copied the value, which at about a microsecond a hit
accounts for the difference; the cost was not profiled. The machine was
busy for the 2 GiB pair: its 11-task figures are left out, since the
baseline's 11 tasks ran slower than its one, and the two runs took 8 and
32 minutes, against under 4 each when the pair ran again later. Its
single-task figures are from the same busy runs.

### Small table, hits that read the slot in place

| Budget (full-size) | Slots | Bytes per entry | Entries | Row hit rate | Fetches per request | Single-task ops/s | 11-task ops/s | Single-task p50 | Single-task p99 |
|---|---|---|---|---|---|---|---|---|---|
| 8 GiB | off | 5,336 | 171,340 | 89.7% | 32 | 1,017, 1,025 | 1,235, 1,210 | 225 µs, 225 µs | 13.5 ms, 13.6 ms |
| 8 GiB | on | 4,474 | 204,396 | 92.2% | 24 | 1,264, 1,170 | 1,658, 1,607 | 193 µs, 209 µs | 11.2 ms, 12.2 ms |
| 2 GiB | off | 5,336 | 42,816 | 55.5% | 146 | 253 | 261 | 1.46 ms | 31.6 ms |
| 2 GiB | on | 4,474 | 51,136 | 60.7% | 129 | 260 | 290 | 1.23 ms | 32.7 ms |

jemalloc's allocated bytes after the fill were within 2% between the two
configurations at both budgets (1,213 and 1,212 MiB at 8 GiB, 429 and
435 MiB at 2 GiB), and the resident set was no larger with slots.

### Full-size table, hits that read the slot in place

| Budget | Slots | Entries | Row hit rate | Fetches per request | Single-task ops/s | 11-task ops/s | Single-task p50 | Single-task p99 | Allocated over the fill |
|---|---|---|---|---|---|---|---|---|---|
| 8 GiB | off | 340,740 | 90.4% | 7.9 | 2,819, 2,908 | 4,704, 4,881 | 181 µs, 179 µs | 4.3 ms, 4.1 ms | 2,082, 2,077 MiB |
| 8 GiB | on | 406,499 | 92.6% | 6.1 | 3,257, 3,301 | 5,779, 6,306 | 159 µs, 156 µs | 3.7 ms, 3.6 ms | 2,101, 2,055 MiB |
| 4 GiB | off | 170,213 | 76.7% | 19.8 | 1,528 | 1,960 | 309 µs | 6.6 ms | 1,079 MiB |
| 4 GiB | on | 203,250 | 81.0% | 16.1 | 1,703 | 2,407 | 280 µs | 6.3 ms | 1,053 MiB |

The rows without slots match the `main` build's replay of the night
before in entries and hit rate, within 3% single-task, and within 9%
across 11 tasks. The scattered table's row cache at the same budgets,
from that night, hit 93.0% at 8 GiB and 81.7% at 4 GiB, and served 3,909
single-task and 6,234 11-task operations per second at 8 GiB. One run with
slots at 8 GiB stopped in its fill, the kernel having read 537 MiB of the
process back into memory; its rerun completed and is the second figure.

## What the measurements show

- **The rounding was nearly all of the gap.** With slots the embeddings'
  row cache came within 0.4 points of the scattered table's hit rate at
  8 GiB and 0.7 points at 4 GiB, having been 2.6 and 5.0 points short;
  the 146 bytes by which an embedding is longer account for some of the
  rest.
- **A hit must not copy.** Copying a 4 KiB value per hit cost more than
  the higher hit rate saved on this workload. With hits that read their
  slot in place, throughput rose along with the hit rate and the tail
  improved; no run separates the cost of such a hit from the gain of the
  hit rate.
- **One lock per slab held up at 11 tasks.** One mutex for every
  embedding, held through each store's copy, did not keep 11-task
  throughput from rising. No run counted time spent waiting on it.
- **The page cache rounds the same way.** The page cache charges a page
  its length and 64 bytes and allocates each page on its own. The
  scattered table's byte-bounded pages close after the value that crosses
  64 KiB: the runs charged 65,701 bytes a page, so a page is 65,637 bytes,
  about 64.1 KiB, which jemalloc rounds to 80 KiB, and a page holds about
  1.25 times its charge. This was read from the code and the runs' page
  counts, not measured as memory.

## What they do not show

The decided design was not run: RFD 030's classes of 32 per doubling,
which give a 4,242-byte value a 4,352-byte slot, so a budget would hold
about a sixth more entries rather than a fifth; the free slots its budget
counts; the record of its entry each slot keeps; chunks freed and
emptied; and the page cache in slabs beside the corrected page charge.
Every cached value in these tables had one length, and no run tested
values of many lengths, a budget that shrinks, a table that leaves the
server, more than 11 tasks, or a host other than this laptop. A fetch
here read a local file the kernel held in memory, so avoiding a fetch
saved little; on object storage it saves a GET.
