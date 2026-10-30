# The performance sweep

Measured 2026-09-25 and 26 on one Apple M3 Pro and, for the disk tier, in
a Linux VM on the same machine. The sweep started from a baseline of the
in-process lookup and load paths, the gRPC path, and CPU profiles of each
([the baseline](#the-baseline)). It made twelve changes, each measured
against the commit before it. Nine were kept because the numbers moved.
The other three are changes no benchmark here could move, kept on the cost
they remove, which a profile, the allocation count, or the code shows.

The biggest gains came from reading only the page a lookup needs and
decompressing it once, with a zstd context reused per thread. On one task
a warm hit went from 22.2 to 12.8 µs and a Full-tier miss from 10.4 to
6.5 µs. On eleven tasks warm hits went from 254,000 to 497,000 a second.
An isolated 1,000-key MultiGet went from 22.7 to 2.6 ms, and a string-key
load from 19.0 to 13.1 ms, allocating under a third of the memory.

Three findings are left open. On macOS the disk tier's throughput on many
tasks did not move: its limit is the blocking-pool handoff, and in the
Linux VM that handoff costs more still. zstd is now most of every
Full-tier hit and miss. And a table whose values compress, unlike the
random bytes the baseline measured, spends 84% of a warm hit decompressing
its value page. Whether to cache decompressed pages or decoded keys is the
open question this document leaves for the maintainer, with the evidence
below.

## Why it was measured

Before the sweep, the lookup path had been verified for what it costs in
requests and memory: GETs per lookup, resident bytes, and the doctor's
verdicts ([real S3 verification](2026-09-21-real-s3-verification.md)). They had not
been measured for CPU. The prototype's findings said decompression
dominated a warm hit; nobody had measured how much of the rest was
icetray's own work, or how a lookup scaled across cores.

## Method

- **Machine.** Apple M3 Pro, 11 cores (5 performance, 6 efficiency),
  36 GiB, macOS 26.6.2. Other work shared the machine throughout. The load
  average read 5 to 11 during the changes and 7 to 16 during the closing
  runs, so every comparison alternated its before and after binaries.
- **Program.** `crates/icetray-testkit/examples/bench.rs`, built
  `--release`. It serves contract-shaped tables through `Table` over
  `Storage`'s `file://` store, so no number includes object-store latency.
  It uses jemalloc behind an allocation counter, and the binary's Prometheus
  recorder is installed. Each lookup scenario has a 1 s warm-up and a 5 s
  window. "1 task" is one lookup at a time; "11 tasks" is one task per
  core. A load or refresh is five rounds, and its figure is the median of
  rounds 1 to 4. Data files are read once beforehand, so a "cold" read is a
  copy out of the OS page cache, not a disk read.
- **Tables.** 2,000,000 rows each, written by the fixture writer in the
  contract layout (1,000-row pages, zstd at parquet's default level, key
  Bloom filter), in 4 buckets, one file per bucket, plus a 20,000-row
  append for the refresh scenario. A long-key and a string-key table
  (21-byte keys), each with two kinds of 100-byte value:
  - **random** bytes, which zstd stores as they are (value column ratio
    1.00). The baseline had only these.
  - **text**, JSON-like records added for the closing run: a 32-bit id, a
    timestamp, a score, then word fields (`status`, `region`, `plan`, ...)
    whose words come from a 16-word vocabulary drawn with a skew.
    zstd compresses the value column 4.38 times.

  The key columns compress 6.77 times (long) and 9.92 times (string) in
  both kinds.
- **Comparisons.** Each change ran 3 times per binary, alternating before
  and after, and the tables report medians. The closing matrix is three
  uninterrupted runs of every scenario, tier, table, and value kind, also
  reported as medians.
- **Profiles.** samply on the `profiling` build (release code generation
  with symbols). Each profile is one scenario on one task for 20 s. Samples
  are weighted by each worker thread's CPU time, so a parked thread counts
  nothing.
- **gRPC.** `dev/bench-grpc.sh` runs the release `icetray serve` over the
  same tables, with ghz on the same machine. It warms the cache for 15 s
  with Gets, then makes three 30 s runs against the long table: Get at
  20,000 a second, Get at saturation, and MultiGet of 100 keys at
  saturation.
- **Linux.** colima's VM (Apple's hypervisor, 4 vCPUs, 8 GiB, Ubuntu
  kernel 6.8) ran a `rust:1.98.1-bookworm` container, with the repository
  mounted read-only and its target directory inside the container. The
  tables and the disk tier's local directories were on the container's own
  filesystem. It was the same program at `6d85774`, whose core is the final
  tree's, with 4 tasks for "multi".

## The baseline

Commit `d3e4c2a`, random values. The items are numbered in the baseline's
ranking, which the changes below cite. On one task, a warm hit took 22.2 µs and allocated 37 times
(270 KiB) to return 100 bytes. A Full-tier miss took 10.4 µs. The profiles
put the cost here:

1. About 27% of every hit and miss went to positioning parquet's column
   reader on the one page it needed. It copied the chunk's ~500 page
   locations, built a table of every page's row count from the offset
   index, and skipped page after page from the row group's start.
2. Parquet built two zstd contexts per page reader, four per hit. It
   decompressed each page into one buffer and copied it into another. On
   eleven tasks, creating and freeing those contexts waited on the macOS
   allocator's lock, 9% of all CPU.
3. Decompression itself: 36% of a warm hit and 53% of a miss.
4. A MultiGet ran on one task, so a 1,000-key batch took 1,000 hits' time
   on one core.
5. Shared state written on every lookup: the generation's reference
   count, the lookup counter, and the latency histogram, beside two clock
   reads. On eleven tasks they capped the Bloom-tier miss at 1.35 times
   one task.
6. The disk tier's key-page read went through the blocking pool. About
   half of a Disk miss's worker CPU was parking and waking threads.
7. foyer takes a memory shard's lock on every hit, and the cache had 8
   shards for 11 workers.
8. to 12. Smaller items: gRPC saturation that belonged to the client, load
   verification's per-key UTF-8 check and the key `Vec` it grew, the
   `file://` cold hit's thread handoffs, the hedge window's sort on every
   GET, and an allocation for a long key's bytes.

## The changes

Each row's numbers are from its commit message: its parent against it, 3
runs each alternating, medians. "1 task" and "11 tasks" are the bench's
modes, on the long table with random values in the Full tier unless the
row says otherwise.

| commit | change | what moved |
|---|---|---|
| `53af717` | A lookup plans only the page holding its row, and the skip counts from that page's first row (baseline 1) | warm hit, 1 task: 22.6 → 15.1 µs; Full miss: 10.5 → 6.9 µs; MultiGet 1,000, 1 task: 23.2 → 15.7 ms |
| `b2c3fcb` | icetray decompresses zstd pages itself into one buffer, with one context per thread (baseline 2) | warm hit, 11 tasks: 322,491 → 488,087/s; 1 task: 15.1 → 12.8 µs; 37 → 29 allocations and 239 → 130 KiB per hit; Full miss, 11 tasks: 777,858 → 1,089,772/s |
| `1ccce23` | A Get borrows the generation through a guard; lookup counts go to per-thread shards, published at scrape (baseline 5) | Bloom miss, 11 tasks: 6.23M → 8.05M/s (+29%); hits unchanged |
| `9c4bd9b` | A MultiGet's keys are split into runs spawned on several workers, under one 32-lookup bound (baseline 4) | MultiGet 1,000, 1 task: 13.88 → 2.94 ms; on 11 busy tasks, p99 49.45 → 106.47 ms |
| `0590cd9` | The page cache has one memory shard per worker, rounded up to a power of two, and the budget minimum follows (baseline 7) | gRPC MultiGet 100 at saturation, one profiled run each: 2,585 → 2,806 rps; foyer's exclusive-lock spin 31.4% → 25.5% of server CPU |
| `d8b5f1b` | A long key's bytes are held inline, not allocated (baseline 12) | Bloom miss, 1 task: 4.55M → 4.90M/s, 2 → 0 allocations |
| `a27b3e3` | The hedge delay is recomputed every window/16 GETs (baseline 11) | `HedgeWindow` 1.43% → 0.08% of cold-hit CPU; throughput within the runs' spread |
| `559ebe3` | Load verification checks a row group one decode batch at a time, with an ASCII fast path for UTF-8 (baseline 9) | string load: 21.76 → 16.43 ms wall, 74.9 → 54.5 ms CPU, 190 → 67 MiB allocated |
| `605370d` | A MultiGet splits only while the table has fewer calls in flight than workers | MultiGet 1,000, 11 tasks: p99 94.61 → 33.17 ms, p50 16.21 → 24.67 ms; 1 task unchanged |
| `e71ce56` | The gRPC handlers read the key type through a guard, not a clone of the generation | no benchmark moved beyond its spread; kept on the two shared reference-count writes per Get it removes |
| `359137b` | A page cache hit counts on its own shard; the counter handle is registered once | one allocation fewer per page hit; Bloom-tier p50 down in every run (long 19.2–19.3 → 18.3–18.5 µs); throughput within the spread |
| `6d85774` | The split's count is one per process, shared by every table through `TableHandles` | two tables in one process (below): the other table's warm hits 115,837 → 224,766/s; the string table's MultiGet 1,000, 184 → 131 batches/s, p99 8.06 → 43.50 ms |

Beside these, five commits only measure: the `profiling` build profile
(`7321686`), the bench program (`5c5ec13`), the gRPC script (`6d56c9a`),
the two-table scenario (`31936b7`), and the text values (`1c020ab`).

Three of the changes are one decision made in steps, and the steps are
worth reading together. Splitting a batch across cores (`9c4bd9b`) made an
isolated batch 3.7 to 5.2 times faster. When every core was already busy,
though, the 1,000-key p99 doubled to tripled, because a batch waits for its
slowest run and its runs queue behind other batches. `605370d` split a batch
only while the table had fewer calls in flight than workers, which brought
the p99 below its level before the split (as measured in the earlier
session). `6d85774` made that count one per
process, so another table's Gets hold a batch back too.

The two-table scenario measures that last step: 11 loops of warm hits on the
long table beside one loop of MultiGet 1,000 on the string table, every
loop yielding between operations as a server's requests end their tasks.
A batch is held back rarely: 5 to 7% of them, in two diagnostic runs. A warm Get ends in
one poll, so it is in flight only while a worker runs it, and the count
reaches the worker count only when every other worker is inside a call at
once. The batches held back take 40 to 45 ms inline, about a third of the
window, and meanwhile the other table's Gets have ten workers to
themselves. So the change moves CPU from the batch to the other table:

| two tables, Full, 11 workers | before `6d85774` | after |
|---|--:|--:|
| string MultiGet 1,000, batches/s | 184 | 131 |
| its p50 / p90 / p99 | 5.21 / 6.02 / 8.06 ms | 5.21 / 6.44 / 43.50 ms |
| long warm hits beside it, ops/s | 115,837 | 224,766 |
| their p50 / p99 | 16.62 / 45.50 µs | 16.88 / 42.62 µs |

Batches on one table alone did not move (MultiGet 1,000, 1 task: 2.57 →
2.56 ms; 11 tasks: 23.88 → 23.39 ms).

The shard lock that `0590cd9` spread over more shards was later taken off
the hit path. foyer's LRU takes a shard's lock exclusively on every hit, to
move the page; the memory level now evicts by S3-FIFO, whose hit takes the
lock shared and counts itself on the page. Measured on 2026-10-30 against
the tree of `4b1487a`, where the exclusive lock was 8.7% of server CPU
inside the gRPC MultiGet 100 saturation run (one profiled run each, the
same 28 s window as `0590cd9`):

| | LRU, 16 shards | S3-FIFO, 16 shards |
|---|--:|--:|
| foyer, inclusive, % of server CPU | 10.4 | 1.1 |
| `lock_exclusive_slow` under foyer | 8.7 | 0.0 |
| MultiGet 100 saturation, profiled run | 3,325 rps | 3,417 rps |

Throughput moved less than the CPU freed, and within the runs' spread. Three
release runs per variant, alternating, medians: gRPC Get at saturation
60,486 → 63,304 rps, MultiGet 100 at saturation 3,381 → 3,474 rps, warm
hits on 11 tasks 503,398 → 523,416/s; 1 task unchanged. The same matrix with
64 shards moved nothing beyond the spread under either policy, so the
count stays one per worker.

## The closing numbers

The final tree is `1c020ab`. The results record it as `5aff4e8`, the same
tree before that commit's message was reworded. These are medians of three
runs of the whole matrix. p50 is on one task; ops/s is on eleven tasks.

| scenario | table | Full p50 | Full ops/s | Disk p50 | Disk ops/s | Bloom p50 | Bloom ops/s |
|---|---|--:|--:|--:|--:|--:|--:|
| warm hit | long | 12.8 µs | 497,459 | 22.5 µs | 134,477 | 13.5 µs | 454,682 |
| warm hit | string | 26.1 µs | 271,886 | 40.5 µs | 128,406 | 26.9 µs | 260,081 |
| warm hit | long_text | 71.2 µs | 109,362 | 85.0 µs | 86,821 | 71.9 µs | 108,221 |
| warm hit | string_text | 84.6 µs | 90,767 | 99.8 µs | 76,060 | 85.6 µs | 89,360 |
| cold hit | long | 63.5 µs | 60,176 | 72.8 µs | 43,430 | 105.8 µs | 30,820 |
| cold hit | string | 79.5 µs | 57,074 | 91.2 µs | 43,342 | 123.7 µs | 29,879 |
| cold hit | long_text | 121.2 µs | 50,158 | 132.8 µs | 42,013 | 164.4 µs | 30,002 |
| cold hit | string_text | 135.0 µs | 46,495 | 149.5 µs | 38,353 | 182.5 µs | 29,355 |
| miss | long | 6.5 µs | 972,725 | 14.2 µs | 135,387 | 0.2 µs | 8,925,278 |
| miss | string | 19.5 µs | 372,927 | 34.9 µs | 130,643 | 0.3 µs | 8,298,225 |
| miss | long_text | 6.4 µs | 1,107,654 | 14.0 µs | 136,862 | 0.2 µs | 8,861,632 |
| miss | string_text | 19.5 µs | 374,228 | 34.9 µs | 137,606 | 0.3 µs | 8,159,853 |
| MultiGet 100 | long | 0.38 ms | 4,101 | 0.85 ms | 1,164 | 0.36 ms | 4,355 |
| MultiGet 100 | string | 0.53 ms | 2,634 | 1.07 ms | 1,142 | 0.55 ms | 2,549 |
| MultiGet 100 | long_text | 1.20 ms | 1,077 | 2.14 ms | 817 | 1.21 ms | 1,067 |
| MultiGet 100 | string_text | 1.40 ms | 915 | 2.46 ms | 739 | 1.42 ms | 894 |
| MultiGet 1,000 | long | 2.57 ms | 470 | 10.86 ms | 113 | 2.72 ms | 432 |
| MultiGet 1,000 | string | 4.59 ms | 255 | 16.98 ms | 101 | 4.69 ms | 251 |
| MultiGet 1,000 | long_text | 10.94 ms | 106 | 39.77 ms | 81 | 10.93 ms | 106 |
| MultiGet 1,000 | string_text | 12.56 ms | 90 | 46.08 ms | 73 | 12.69 ms | 89 |

For a MultiGet, ops/s is batches per second. Allocations per warm hit are
27 (129.5 KiB, long); per Full miss, 10 (24.0 KiB). A text table's miss
costs what a random table's does, because a miss reads only the key page.
Its warm hit costs five and a half times as much (71.2 against 12.8 µs on
the long table), and the value page is the difference.

Against the baseline, on the random tables it measured:

| path | baseline | now |
|---|--:|--:|
| warm hit, long, Full, 1 task | 22.2 µs, 37 allocations, 270 KiB | 12.8 µs, 27, 129.5 KiB |
| warm hit, long, Full, 11 tasks | 254,143/s | 497,459/s |
| warm hit, string, Full, 11 tasks | 184,969/s | 271,886/s |
| miss, long, Full, 1 task | 10.4 µs, 15 allocations | 6.5 µs, 10 |
| miss, long, Full, 11 tasks | 595,771/s | 972,725/s |
| miss, long, Bloom, 11 tasks | 6,307,164/s | 8,925,278/s |
| MultiGet 1,000, long, Full, 1 task | 22.71 ms | 2.57 ms |
| MultiGet 1,000, long, Full, 11 tasks | 253/s, p50 43.0 ms | 470/s, p50 23.3 ms, p99 32.4 ms |
| cold hit, long, Full, 1 task | 76.8 µs | 63.5 µs |
| miss, long, Disk, 1 task | 20.3 µs | 14.2 µs |
| miss, long, Disk, 11 tasks | 136,519/s | 135,387/s |
| warm hit, long, Disk, 11 tasks | 130,349/s | 134,477/s |
| load, long, Full | 4.93 ms wall, 16.8 ms CPU, 72.3 MiB | 4.27 ms, 14.9 ms, 26.3 MiB |
| load, string, Full | 18.99 ms wall, 72.4 ms CPU, 238.0 MiB | 13.08 ms, 49.7 ms, 67.3 MiB |
| refresh onto the append, long, Full | 1.15 ms | 0.98 ms |

The disk tier on eleven tasks sits at about 135,000 lookups a second
whatever the table or scenario, before the sweep and after it. That ceiling
is the blocking pool's, which the next section takes up.

Two tables in one process (the two-table scenario above, final tree):

| tier | string MultiGet 1,000 | its p50 / p99 | long warm hits beside it |
|---|--:|--:|--:|
| Full | 142/s | 5.21 / 42.57 ms | 202,983/s |
| Disk | 30/s | 32.04 / 78.69 ms | 89,890/s |
| Bloom | 120/s | 5.42 / 46.85 ms | 204,700/s |

gRPC, three runs of `dev/bench-grpc.sh` on the final tree, medians, against
the baseline's one run. The final server also serves the two text tables,
which ghz does not ask:

| run | baseline | now | now, range of the three runs |
|---|--:|--:|--:|
| Get at 20,000/s: p50 / p99 | 84 / 345 µs | 69 / 301 µs | p99 from 0.29 to 0.47 ms |
| Get at saturation | 60,358 rps, p50 783 µs, p99 4.20 ms | 65,769 rps, p50 629 µs, p99 3.86 ms | 65,432 to 66,749 rps |
| MultiGet 100 at saturation | 2,084 rps, p50 58.9 ms, p99 134 ms | 3,331 rps, p50 36.4 ms, p99 86.0 ms | 3,247 to 3,336 rps |

Every response that was not OK (none or one a run at the fixed rate, 5
to 17 at Get saturation, 123 to 126 at MultiGet saturation) came from ghz
closing its connections as a run ended ("use of closed network
connection", or "the client connection is closing").
Get at saturation is still the pair's limit, not the server's, since ghz
runs on the same machine (baseline item 8).

## The disk tier on Linux

The baseline asked (item 6) whether the disk tier's cost was macOS's. On
macOS about half of a Disk miss's worker CPU was park and wake around the
blocking-pool read. The decision was to measure on Linux first and change
nothing. The ratio of each disk-tier figure to the Full tier's, on the same
machine and build:

| scenario | table | Linux, 1 task p50, Full → Disk | Disk/Full | macOS Disk/Full | Linux Full/Disk ops/s, 4 tasks | macOS Full/Disk ops/s, 11 tasks |
|---|---|--:|--:|--:|--:|--:|
| warm hit | long | 14.4 → 56.9 µs | 3.95 | 1.76 | 3.34 | 3.70 |
| warm hit | string | 29.2 → 74.4 µs | 2.54 | 1.55 | 2.07 | 2.12 |
| cold hit | long | 119.7 → 155.0 µs | 1.30 | 1.15 | 1.28 | 1.39 |
| cold hit | string | 134.0 → 172.5 µs | 1.29 | 1.15 | 1.27 | 1.32 |
| miss | long | 6.5 → 43.5 µs | 6.69 | 2.18 | 5.19 | 7.18 |
| miss | string | 20.7 → 63.8 µs | 3.08 | 1.79 | 3.00 | 2.85 |

On one task the disk tier costs Linux 37 µs more than the Full tier for a
long-key miss, against 7.7 µs on macOS. The Full tier's CPU-bound paths
cost about the same on both (warm hit 14.4 against 12.8 µs, miss 6.5
against 6.5 µs), so the VM does not slow the work itself. What it slows is
the handoff. Profiles of the long-key miss, one task, 20 s:

| | Linux Disk | Linux Full | macOS Disk |
|---|--:|--:|--:|
| worker and blocking-pool CPU per miss | 33 µs | 7.4 µs | 18 µs |
| futex (Linux) or `__psynch_cvwait` (macOS) | 30.8% | under 1% | 38.3% |
| `kevent` (macOS's I/O driver park) | | | 18.7% |
| tokio scheduling a task (`Handle::schedule`) | 10.1% | | |
| `pread` | 5.9% | | 0.1% |
| zstd | 23.9% | 83.5% | 29.0% |

The Linux profiles came from a profiling build of the working tree while
the text fixture was not yet committed. Each ran both long tables, random
and text, for 20 s each; a miss reads only the key page, which is the same
in both. The macOS profile is of the final tree.

The read itself is small on both systems. The rest of the disk tier's extra
cost is the two wakeups around it: the worker hands the read to a
blocking-pool thread and parks, and that thread wakes the worker when the
read is done. In a VM, waking a thread parked on another vCPU goes through
the hypervisor, which makes a futex wake dearer than on bare metal; that is
how virtualization works in general, and this sweep did not measure it
apart. So these Linux ratios are likely an upper bound on bare-metal
Linux's, not a measurement of it. The VM bounds the numbers in three ways: 4 vCPUs, so
"multi" is 4 tasks, not 11; vCPUs are host threads, scheduled among the
host's own load; and cross-vCPU wakeups are virtualized. It does not settle
how far below the macOS figures bare-metal Linux would land.

The disk tier's multi-task throughput hardly depends on the lookup: 135,000
a second on macOS whether it is a warm hit, a miss, or either table. The
limit is the handoff rate, not the work. The options the baseline named,
reading key pages inline on the worker or a dedicated reader thread with
batched submission, are against RFD 017's "runs on the blocking pool so no
worker stalls on disk". That is a decision these numbers inform and do not
make. A bare-metal Linux run is the measurement that would.

The decision followed these numbers. On Linux, a disk-tier page read is now
tried on the worker with `preadv2` and `RWF_NOWAIT`, and goes to the
blocking pool only when the page cache cannot serve it without blocking,
which keeps RFD 017's rule that no worker waits for data from the disk. With
the directory on ext4 in the same VM, the Disk tier now costs about what the
Full tier does:

| scenario, long table | Disk/Full p50, before | after | Disk p50, 1 task, after |
|---|--:|--:|--:|
| warm hit | 4.03 | 1.06 | 14.7 µs |
| cold hit | 1.31 | 1.02 | 124.7 µs |
| miss | 7.47 | 1.05 | 6.8 µs |

On overlayfs, which refuses the non-blocking read, nothing moves: every
read falls back, at a cost of about a microsecond for the refused attempt.
The local directory belongs on a mounted volume, as [Operating icetray](../operating.md#the-local-directory) says.

## Decompression: the open question

Two deferred decisions bear on decompression. RFD 006 left open a
decompressed-page cache level, and RFD 004 deferred decoded key arrays.
After the sweep, zstd is most of every lookup in the Full tier. The
profiles below are of the long table, Full tier, one task, 20 s, on the
final tree:

| | random values | text values |
|---|--:|--:|
| warm hit p50 | 12.8 µs | 71.2 µs |
| zstd share of the warm hit | 61.5% | 92.4% |
| key page (`FileIndex::search`), share of the warm hit | 50.0% | 9.4% |
| value page (`read_one`), share of the warm hit | 40.0% | 88.1% |
| miss p50 | 6.5 µs | 6.4 µs |
| zstd share of the miss | 84.4% | 83.4% |

A miss decompresses one key page, and 84% of it is zstd for either kind of
value. The key page costs a warm hit what it costs a miss (6.4 µs of the
random warm hit, 6.7 µs of the text one). zstd accounts for 84% of it, so
about 5.5 µs. The rest of the warm hit is the value page. With random
values zstd copies it block by block, about 2.3 µs. With text values it is
decoded for real, about 60 µs, and that is 84% of the warm hit. These
splits come from the shares above, so they are estimates.

What each deferred option would save and cost follows. **All of it is
estimated** from the measured numbers above, not measured.

**A decompressed-page cache level (RFD 006).** A warm hit whose value page
is cached decompressed skips that page's zstd.
- Saves: about 2.3 µs of a 12.8 µs warm hit with random values (to about
  10.5 µs), and about 60 µs of a 71.2 µs warm hit with text values (to
  about 11 µs, some six times the throughput per core). Nothing on a miss
  or a cold hit's GET.
- Costs: a page held decompressed takes its compression ratio in memory.
  For random values that is 1.00, so the level costs nothing and saves
  little. For text values it is 4.38: these tables' value pages are 45.7
  MiB compressed and 200 MiB decompressed. At a fixed budget, the cache
  holds 4.38 times fewer pages.
- The trade: each hit lost to the smaller cache becomes a cold hit. On S3
  that is a GET of about 25 to 30 ms at the in-region median
  ([S3 range GET latency](2026-09-10-s3-range-get-latency.md)), the saving
  of some 400 to 500 warm hits. So the level pays for compressible values
  when the hot set fits the budget decompressed, and loses when fitting it
  would cost more than about 0.2% of lookups in extra misses. Which of the
  two a real table is depends on the hit-rate curve under a skewed key
  distribution, which RFD 006 already names as unmeasured and this sweep
  did not measure either.
- One arrangement the RFD could weigh, not built or measured: a small
  decompressed level in front of the compressed cache, holding the hottest
  pages only.

**Decoded key arrays (RFD 004).** A lookup on a table with decoded keys
binary-searches its key run with no page to decompress.
- Saves: the key page's decode on every Full-tier lookup. A miss goes from
  6.5 µs to under a microsecond (a Bloom-tier miss, which decodes nothing,
  is 0.2 µs). A warm hit saves about 6.4 µs (random: 12.8 to about 6.5 µs;
  text: 71.2 to about 65 µs), and a MultiGet about the same per key.
- Costs: a decoded key takes its uncompressed size. The key columns here
  are 16.2 MB (long) and 50.5 MB (string) uncompressed, against a Full
  resident index of 2.49 and 5.18 MiB. So decoded keys would take about
  15.4 MiB (8 bytes a key) and 48 MiB (about 25 bytes a key) for these
  tables: 6 and 9 times today's index, near the key columns' compression
  ratios of 6.77 and 9.92. (The column sizes cover both builds, 2.02
  million keys; the index figures cover the first build.)
- The trade: a table whose lookups are mostly misses, or whose values are
  random, gains most of its lookup time back. A table with compressible
  values and mostly hits gains a tenth of it, since its warm hit waits on
  the value page.

Neither option touches the other's cost: one is the value page, the other
the key page. Where both are wanted, the savings add, and so does the
memory.

## What this does and does not establish

It establishes where a lookup's CPU goes on this machine after the sweep:
zstd first, then the disk tier's handoff for that tier, then parquet's
value skip and `Bytes` refcounting. It establishes that compressible values
make the value page's decompression the largest single cost of a warm hit,
and that everything this sweep changed was icetray's own overhead around
that cost.

It does not establish:
- Linux on bare metal. The VM numbers are likely an upper bound for the
  disk tier's handoff, not a measurement of it.
- Object-store latency, S3's request path, or hedging against a slow store.
  Every read here was local.
- The server's own gRPC saturation for Get. ghz shares the machine, and at
  Get saturation the server used about 3 of 11 cores in the baseline's
  profile.
- Real values. The text fixture compresses 4.38 times at parquet's default
  zstd level. Real values may compress more or less, and a value page's
  decompression time depends on its size and on how it compresses.
- The page cache's hit-rate curve under skewed keys, which decides the
  decompressed-page question.
- Memory on Linux, which RFD 017 cares about. The resident-set samples here
  are macOS's.
