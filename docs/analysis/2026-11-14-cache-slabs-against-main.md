# The decided cache slabs against main

Measured 2026-11-13 and 2026-11-14 on a laptop with the benchmark program,
comparing `main` at `3b95ba5` with the slabs RFD 030 decided on at the
time, built on a branch from it and left unmerged. The measurement is the
evidence behind setting the slabs aside in
[RFD 030](../rfd/030-cache-slabs.md); the earlier measurement of slots of
exactly an embedding's length is in
[exact-length slots](2026-11-11-cache-slabs.md).

On the embeddings table the slabs raised the row cache's hit rate by 2.2
to 2.5 points at 2 and 4 GiB and throughput by 11 to 20%, about what main
gains from a tenth more budget. On a table of 4,096-byte values, which
already fit a jemalloc class, they lowered it by 1.8 to 2.0 points, about
what main loses from a fourteenth less. At equal budgets the slabs' build
held 3 to 10% more resident memory than main, and all of it was the page
of padding jemalloc gives a large allocation, which every 64 KiB chunk
carried; with the padding off it held less than main. Main's own page
cache held about 15% more than it charged.

## Method

- **Host.** An Apple M3 Pro (11 cores, 36 GiB, 16 KiB pages) on macOS
  26.6. The benchmark program counts allocations through jemalloc and
  stops a run once its faults read more than 16 MiB of the process back
  into RAM.
- **Program.** `crates/icetray-testkit/examples/bench.rs` in release
  builds of `main` at `3b95ba5` and of the slabs at the branch's last
  commit: 32 classes per doubling from 1 KiB, chunks of 4 to 32 slots
  near 64 KiB, a sixteenth of each cache's part held in reserve for free
  slots, chunks emptied to stay within it, and no slabs in a cache part
  below the least that holds one chunk of every class.
- **Tables.** The benchmark's embeddings table, 4,242-byte values in 128
  files, about 12 GiB of Parquet, and its scattered table, 4,096-byte
  values in 128 files, about 19 GiB.
- **Workloads.** A replay of a quarter sample of a production trace of a
  recommendation service's embedding-batch lookups, each budget scaled by
  the sample rate as the benchmark's `replay` does, for 60 seconds, with
  the row cache on. Each pair below ran back to back, main first.
- **Memory.** jemalloc's own statistics after the fill: what it had
  allocated, the pages its live allocations spanned (active), and, from a
  diagnostic build of each binary, the same split by size class.

## Results

Single-task rows, with the 11-task throughput beside them:

| Table, budget | Build | Ops/s, 1 / 11 tasks | Row hit % | Fetches per op | RSS MiB | Allocated MiB |
|---|---|---|---|---|---|---|
| Embeddings, 4 GiB | main | 1,460 / 1,777 | 76.8 | 19.75 | 1,528 | 1,368 |
| Embeddings, 4 GiB | slabs | 1,622 / 2,137 | 79.0 | 17.92 | 1,677 | 1,323 |
| Embeddings, 2 GiB | main | 807 / 950 | 58.1 | 36.62 | 1,017 | 839 |
| Embeddings, 2 GiB | slabs | 904 / 1,119 | 60.6 | 34.70 | 1,052 | 798 |
| Scattered, 4 GiB | main | 1,794 / 2,198 | 81.6 | 15.72 | 1,646 | 1,366 |
| Scattered, 4 GiB | slabs | 1,782 / 2,216 | 79.8 | 17.26 | 1,719 | 1,293 |

Runs made at other times of the same night, comparable in hit rate only,
put the scattered table at 64.0% on main and 62.0% with slabs at 2 GiB,
and 93.0% and 92.2% at 8 GiB. Hit rates repeated within a tenth of a point
across runs of one build. The slabs' row cache held fifteen sixteenths of
main's charged bytes at every budget, the rest being the reserve.

Main's hit rate on the embeddings table rose about 19 points from 2 to
4 GiB, so the slabs' 2.2 points at 4 GiB are what main would get from
about 8.5% more budget; on the scattered table, 17.6 points from 2 to
4 GiB put the slabs' loss of 1.8 points at about 7% less.

### Where the slabs' extra memory was

After the fill on the embeddings table at 4 GiB, jemalloc's active pages
beyond its allocated bytes were:

| | main | slabs | slabs, `cache_oblivious:false` |
|---|---|---|---|
| Small-class fragmentation | 55.6 MiB | 28.8 MiB | about 28 MiB |
| Padding on large allocations | 4.1 MiB, 261 of them | 196.8 MiB, 12,598 of them | none |
| RSS after the fill | 1,528 MiB | 1,677 MiB | 1,470 MiB |

jemalloc 5.3.1 gives each large allocation, 64 KiB and up with 16 KiB
pages, one page more when `cache_oblivious` is on, its default. The
slabs' 12,338 chunks of 64 KiB each spanned 80 KiB. With the option off
the slabs' build held 58 MiB less than main, at the same hit rate of
79.0% and 1,646 operations a second.

### Main's page cache

With the row cache off, on the embeddings table at 4 GiB, main's page
cache charged 1,024 MiB while the fill grew jemalloc's allocations by
about 1,178 MiB: each page owns an allocation of its size class, and the
charge was its length and 64 bytes. Turning the padding off on main
changed its resident memory by 12 MiB and its throughput not at all,
since on this host a page under 56 KiB is a small allocation.

## What they do not show

No 8 GiB run of the embeddings table completed on either build: after
74 days of uptime the system's compressed memory left too little room
for the table in the file cache beside an 8 GiB budget, and the runs
stopped. Nothing ran on Linux, where pages of 4 KiB make every
allocation of 16 KiB or more large and its padding 4 KiB. The slabs were
measured without charging their chunks' padding or the size class of
each chunk's allocation, which would have lowered their gain. As in the
earlier measurement, a fetch read a local file the kernel held in
memory, so a cache miss cost less than it does against object storage.
