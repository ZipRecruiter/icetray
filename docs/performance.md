# Performance and tuning

An icetray lookup does three kinds of work, and their costs differ by
orders of magnitude. A GET to object storage takes tens of milliseconds,
and longer for a file no lookup has read for hours. A read from the local
directory is a memory copy when the operating system's page cache holds
the file, and a device read when it does not. A lookup's CPU work,
decoding one key page and decompressing one value page per column it
reads, takes microseconds to tens of microseconds. Most tuning sets how
many lookups wait on a GET, and how much memory or local disk icetray
spends serving the rest.

Three outcomes recur below. A **cold hit** is a lookup that finds its key
and fetches a value page from object storage. A **warm hit** is one that
fetches none: each value it requests comes from the row cache or its
value page from the page cache. A **miss** is one whose
key is absent. Terms such as [residency](glossary.md), [key
chunk](glossary.md), and [fence array](glossary.md) are defined in the
glossary.

## What a lookup costs

| Residency | Cold hit | Warm hit | Miss |
|---|---|---|---|
| `full` | 1 GET | nothing outside memory | nothing outside memory |
| `disk` | 1 local read and 1 GET | 1 local read | 1 local read |
| `bloom` | 2 GETs: the key page, then the value page | nothing outside memory | nothing, or 1 GET when the filter passes an absent key |

The table counts a lookup that requests one value column. Each further
column it requests adds one value-page GET to a cold hit, two for a
dictionary-encoded column, and none for a column added after the row's
data file was written, whose value icetray returns from the column's
default. A lookup that requests no column returns only whether its key is
present, and costs only finding the row: nothing outside memory under
`full`, a local read under `disk`, and the key page's GET under `bloom`.

A cold hit spends nearly all of its time waiting on its GET, so its
latency is the object store's. A warm hit and a miss under `full` stay in
memory and take microseconds. Under `disk` they add a local read of one
key page, a memory copy while the chunk file is in the kernel's page
cache and a device read when it is not. `icetray get` prints each
lookup's `gets`, so an owner can measure a residency's GET count on a
real table before configuring it.

### MultiGet fan-out

icetray holds at most `server.batch_fan_out` GETs in flight for one
`MultiGet` ([Configuration](configuration.md#server)), and each GET a
lookup issues holds one slot until it ends. A lookup takes its slot before it asks the page cache for
a page not in memory, so a page in memory costs no slot, and a `Get` of a
page a batch is still waiting to fetch issues its own GET rather than
waiting behind the batch. A batch whose keys are all cold therefore waits
about one GET's latency for every `batch_fan_out` GETs. Under `full` or
`disk`, that is every `batch_fan_out` keys when it requests one column,
every `batch_fan_out` / 2 when it requests two, and fewer when a column is
dictionary-encoded, since its dictionary page is a GET of its own. Under
`bloom` each key's key page takes a slot as well. A larger bound shortens
a cold batch's latency, and each batch then holds more GETs open on the
object store, up to twice the bound with a hedge beside each GET.

Measured against S3 on a 4-vCPU host, a cold batch of 100 keys stops
getting faster at a fan-out of about 128, where it takes about three GET
round trips. Several concurrent batches reach the process's shared GET
throughput sooner, by 64 for two-column batches, and past that point a
larger bound speeds no one up: it only lets one batch take more of the
shared throughput from the others. The default keeps most of a lone
batch's gain and holds one caller to a fair share. A deployment with few
concurrent batch callers can raise it toward 128; above that, the
process's throughput is the limit
([the measurements](analysis/2026-10-08-multi-get-fan-out.md)).

## What a Scan costs

A `Scan` of an [ordered table](table-layout.md#ordered-tables) finds the
key's run in each file that holds the key, then reads the order column
and each requested column over the run's rows, in one round across every
file. For a key held in one file, with its run in one row group and none
of its pages cached, a cold `Scan` of one value column costs:

| Residency | Finding the run | Reading the rows |
|---|---|---|
| `full` | a decode of the run's first and last key pages in the row group, in memory | 2 GETs: the value column's span and the order column's |
| `disk` | the same decode, from the local chunk file | 1 GET for the value column's span; the order column is read from the chunk file |
| `bloom` | the filter screens the file, then a GET of each of those key pages | 2 GETs, as under `full`, after the key-page round |

Within a row group, a column's rows for one `Scan` are contiguous, so
icetray fetches each column's uncached pages over the rows it reads as
one span GET, however many rows the limit takes, and a
dictionary-encoded column adds its dictionary page. A `Scan` therefore
costs about what a cold `Get` with one more column does, and its bytes
grow with the rows it returns and their values' size. A run that spans
row groups costs a span per row group, and a run the cache holds in part
costs one GET per uncached span. Whole pages of deleted rows between the
rows a `Scan` reads are never decoded: where they take at most 256 KiB,
the span GET fetches them with the pages around them, and a wider stretch
splits the span into two GETs. With a bound, icetray places the bound on
one order page by the order fences it holds resident, and reads that
page in the same round as the value pages.

The files that hold the key multiply the cost. A cold `Scan` of a key in
K files costs K times the GETs of a compacted one and waits on the
slowest of them, so an ordered table's latency follows its compaction
cadence. `icetray_scan_files` reports the files each `Scan` read, and
`icetray_table_gets_per_scan` the GETs a cold `Scan` at `max_scan_rows`
of a key in one file makes at worst.

Measured on local files, which leave out a GET's latency to object
storage, a warm `Scan` of a key's 100 rows took about 16 µs under `full`
and `bloom` and 34 µs under `disk`, where the order column is a local
read, and a page of 10 of those rows took about 12 µs. A page of 10 rows
from the middle of a 10,000-row run took about 24 µs, and about 44 µs
over a string order column whose values share 64 leading bytes and whose
column index the writer cut short. Decompressing the pages a `Scan`
reads from memory took half of the long run's warm `Scan` and
over a third of the string one's, the largest share of each. A cold
bounded `Scan` took one round of GETs whatever the writer's order column
index held, after the key-page round under `bloom`, and made the GETs in
the table above. A `MultiScan` of 10 keys took about 18 µs a key warm and
50 µs a key cold, where its keys' GETs overlap under
`server.batch_fan_out`. Against S3, each cold figure is a GET's latency,
tens of milliseconds
([the measurements](analysis/2026-10-14-order-fences.md)).

A load of an ordered table reads each file's order column once per row
group to verify it, one more GET per row group than a key-value table's
load. Resident memory adds the order column's offset index, a few facts
per file, and the order fences, one pair of key and order value per
order page, and a `Scan` reads the order values from their pages.

### MultiScan across worker threads

While fewer calls than worker threads are in flight, icetray splits a
`MultiScan`'s distinct keys, in order and evenly, into one run for each
32 distinct keys or part of 32, up to one run per worker thread, and
reads the runs on several cores at once, as it splits a `MultiGet`'s
keys into one run for each 8 keys or part of 8. On 11 worker threads, 33
distinct keys make two runs of 17 and 16, and 100 make four runs of 25.
A `MultiScan` of 32 distinct keys or fewer, and any `MultiScan` that
arrives while as many calls as worker threads are in flight, reads its
keys on one task. The runs share the call's `server.batch_fan_out`
slots, and the answer, the response bound, and the metrics are the same
either way. So is the error of a failing call, a `MultiGet` or a
`MultiScan`: that of its first failing key in request order, though a
key of a later run failed first.

Spreading trades CPU for latency. Measured warm under `full` on local
files, at a limit of 1, a lone `MultiScan` took a median of 0.32 ms at
100 keys where one task took 1.07 ms, and 2.35 ms at 1,000 keys where
one task took 10.5 ms, for 10% and 70% more CPU per key: the cost of
spawning the runs, waking workers on other cores, and workers spinning
before they park. With 11 callers at once, one per core, the difference
was within noise, since the calls in flight keep most `MultiScan`s from
splitting. The extra CPU is spent only while cores would otherwise sit
idle, so it lowers the latency of a lightly loaded server without
taking throughput from a busy one
([the measurements](analysis/2026-10-14-order-fences.md#multiscan)).

### The page cache and scans

icetray caches an ordered table's pages in the page cache every table
shares, each page a span GET fetches as its own entry, keyed by file and
offset. A later `Scan` of the same key takes each page from memory, then
from the page cache's disk level when the local directory has one, a
local read in place of a GET, and fetches only the runs of pages neither
holds, one GET per run. The memory level keeps a pass over pages read
once from displacing pages read again. `server.cache_bytes` bounds the
memory level ([Configuration](configuration.md#server)) and
`storage.local.page_cache_disk_bytes` the disk level
([Configuration](configuration.md#storagelocal)). Scans and lookups share
both levels, so a long `Scan` can push a key-value table's pages off the
disk level, though not its rows out of the row cache. Their hits and
fetches count together in the page-cache metrics
([Metrics](operating.md#metrics)).

## Packing or splitting a row

A table can be configured with several [value columns](glossary.md), and
a lookup reads only the columns it requests
([RFD 022](rfd/022-value-columns.md)). The choice of columns sets which
pages a lookup reads, and so its GETs. icetray issues the GETs of a cold
hit on several columns concurrently and hedges each on its own. The hit
waits on the slowest of them, so its tail is wider than a one-column
hit's even with every GET hedged. Columns share the page cache, which is
keyed by file and offset.

- One packed column performs best. A writer that packs the row into one
  `binary` column, such as a serialized protobuf, gives every cold hit
  one GET. It is the right layout for a table read by one kind of caller.
- For a table read by several kinds of caller, the writer splits the row
  along how it is read. Fields read together share a column. A large
  part read on its own, such as an embedding, gets a column of its own,
  so a caller that requests only the small fields never fetches or
  decodes it, and the page cache holds more of the small fields' hot
  rows.
- A caller that needs most of a row is better served by one packed
  column than by requesting several, since each costs a GET.

`icetray_value_page_gets_total` counts each column's GETs, and
`icetray_table_column_gets_per_hit` gives each column's cold cost. A
request's cold cost is `icetray_table_gets_per_miss`, for finding the
row, plus the figure of each column it requests
([Metrics](operating.md#metrics)).

## Splitting a row or projecting it

For a column declared protobuf, a request can list top-level field
numbers, and icetray trims each value to those fields
([RFD 024](rfd/024-field-projection.md)). Projection is the finer tool
and the cheaper change, since the table needs no rewrite. It saves
different things from a split:

- Splitting the row into columns changes which pages a lookup reads, and
  so its GETs. A lookup that requests one column fetches and caches only
  that column's pages, which saves the bytes each GET transfers and
  page-cache space. The writer fixes the split at write time for every
  caller, and a hit that requests several columns costs a GET for each.
- Projection trims a value's bytes after the page cache, so a projected
  lookup makes the same GETs and caches the same pages as a whole one.
  It saves the bytes on the wire and the caller's decode, which a
  protobuf runtime spends on every field it receives. The caller lists
  the fields per request. icetray's walk is linear in the value's bytes,
  small beside the page decompression every lookup pays for a value much
  smaller than its page.

So split a row where a large part is read on its own, such as an
embedding beside a few hundred properties, and project within a column
where callers read different small sets of fields of one message. The
two combine: a protobuf column of properties, projected per request,
beside a separate column for the embedding. Per column, the ratio of
`icetray_value_bytes_returned_total` to `icetray_value_bytes_read_total`
is what projection saves.

## Choosing a residency

A table's residency, configured by its owner, sets where icetray stores
the table's key chunks, and so trades memory against GETs and local
disk. Lookups return the same results under every residency. The full
comparison is in
[Choosing a residency](configuration.md#choosing-a-residency), and the
decision behind the three tiers is
[RFD 017](rfd/017-residency-tiers.md).

- **`full`** keeps every key chunk in memory. Its memory is the key
  column's compressed size, which depends on how well the sorted keys
  compress: about a byte per key for sequential integers, about eight for
  random 64-bit integers, and fourteen to eighteen for UUIDs, so a
  billion UUID keys take 14 to 18 GB. It makes the fewest GETs and serves
  a hit fastest, and it is the right choice whenever its bytes fit in
  memory beside the page cache.
- **`disk`** stores the key chunks in chunk files in the local
  directory, one per data file, and keeps only footers, page indexes,
  and fences resident: a fraction of a byte per key at the optimal
  layout's page sizes. The chunk files occupy about what `full` would
  hold in memory. A hit still costs one GET and a miss none, so `disk`
  serves tables too large for memory at the same GET count, as long as
  the replica has a local volume and memory beyond the process for the
  kernel to cache the chunk files
  ([below](#the-memory-limit-and-the-kernels-page-cache)).
- **`bloom`** keeps the key column's bloom filters in memory in place of
  the key chunks and needs no local disk. Its memory is the filters,
  whose size in bits per key the writer sets, plus the same fraction of a
  byte per key. A cold hit makes a second GET, for the key page, and a
  miss the filter passes makes one. Writers often size the filter per row
  group rather than per key, so `bloom` can hold more memory than `full`.
  It suits a table whose keys do not fit in memory, on a replica that
  cannot mount a volume.

Resident memory also has a part per data file: icetray keeps each file's
footer and page indexes resident under every residency. At the optimal
layout's file sizes that part is small beside the keys. Many small files
raise it, and so do values of kilobytes, since each value page then
holds only a few values and each value column's offset index carries an
entry per page. icetray keeps resident only the offset indexes of the
key and of the configured value columns and drops every other column's
at load, so a column the table is not configured to serve costs no
memory.

## Sizing from the doctor's predicted bytes

`icetray info` and `icetray doctor` print each residency's resident bytes
under `predicted`, computed from the data files' footers before icetray
reads any key chunk. For the golden table `long_2m`, 2 million long keys:

```json
"records":2000000,"index_bytes":2623999,"predicted":{"full":2623999,"disk_memory":239082,"disk_bytes":2405869,"bloom":4433458,"bloom_bits_per_key":16.777504,"key_chunk_bytes":2384917,"filter_bytes":4194376}
```

- `predicted.full`, `predicted.disk_memory`, and `predicted.bloom` are the
  memory each residency would hold. Divided by `records` they give bytes
  per key, which hold roughly for a larger table of the same key shape
  and layout.
- `predicted.disk_bytes` is what the chunk files of `disk` would occupy on
  local disk, as `storage.local.index_budget_bytes` counts them.
- `predicted.key_chunk_bytes` and `predicted.filter_bytes` are the key
  column's compressed bytes and its bloom filters' bytes, the parts that
  `full` and `bloom` add to the footers, page indexes, and fences every
  residency keeps resident.
- `predicted.bloom_bits_per_key` is the filter's size per key. Under
  `bloom`, a filter below about 8 bits per key passes a noticeable share
  of misses through to a GET, and one above 4 bytes per key spends memory
  for little gain; the doctor reports either as advice.

Here `full` holds about 1.3 bytes per key, `disk` about a tenth of a byte
in memory and about 1.2 bytes on disk, and `bloom` more than `full`,
because the writer gave each file a 1 MiB filter whatever its key count.

[Memory](operating.md#memory) turns these figures into a container memory
limit, which is set first and which the cache budget, half of it unless
`server.cache_bytes` sets it, follows: the predicted bytes for each
table's residency, the largest table's once more, and the budget, plus a
third of that total for the process. icetray checks that sum against the
limit once the tables serve.

## The row cache

A key-value table reads each value through the row cache: one value of
one row, copied out of its page, keyed by the data file, the row's
position in it, and the column. A repeated hit finds its values there and
reads no value page, so it neither decompresses one nor waits on a GET.
It still finds its key as any lookup does, through the resident key page
under `full`, a local read under `disk`, and the key page's GET or the
page cache under `bloom`. A miss in the row cache reads the page as
before and caches the value, and a lookup that requests no column, or a
column answered by its default, uses no entry.

The same memory holds many more hot rows than pages: a value page holds a
hot row beside every row stored next to it, so where hot keys are
scattered through the key order, as hashed or arbitrarily assigned keys
are, most of a cached page is rows nobody asked for. Each entry is charged
its value at its allocation size plus 128 bytes of overhead, so rows of a
few hundred bytes or more spend most of the budget on values. A value
larger than one 64 KiB value page is not cached, and a row read once
leaves before rows that keep being read.

icetray decides which file and row a key reads, and whether a delete
removes it, before it asks the row cache, so a deleted or rewritten row
is never served from it, and nothing is ever invalidated. A refresh keeps
the entries of every file the new snapshot still lists, and entries of
files no snapshot lists age out. A restarted replica starts with an empty
row cache and fills it from its traffic.

The row cache holds `server.row_cache_share` of `server.cache_bytes`
([Configuration](configuration.md#server)) when any table reads through
it, and every key-value table does unless its entry sets `row_cache =
false` ([Tables](configuration.md#tables)). Turn it off for a table whose
callers keep their own cache in front of icetray, so its repeats rarely
reach the server, or whose keys are read about evenly, so few rows repeat
within the cache's reach; the budget then goes to the tables that reuse
it. `icetray_row_cache_hits_total` against
`icetray_row_cache_misses_total` gives its hit rate
([Metrics](operating.md#metrics)).

### Skewed keys, measured

The benchmark program's `skewed-hit` scenario asks keys drawn from a Zipf
distribution of exponent 0.9 over the 2,000,000 keys of its `long`
table, under which the hottest one percent of keys take about half the
lookups. The keys are ranked in a random order, so the hot ones are
scattered through the key order and so through the value pages, and the
table's 100-byte values sit about a thousand to a page. The scenario
serves the table under `full` through a 32 MiB cache budget, about a
sixth of its 200 MB of value pages, fills the caches with a pass of keys
of the same distribution, and then measures fresh draws. With
`--row-cache on` the budget is split by the default `row_cache_share`;
with `--row-cache off` the page cache holds all of it. The table is on
the local filesystem, so each value-page GET it counts is a file read;
against object storage each would be a GET of tens of milliseconds.

```sh
cargo run --release -p icetray-testkit --example bench -- \
  --scenario skewed-hit --tier full --table long --values random \
  --mode single,multi --duration 5 --row-cache on
```

One run on an Apple M3 Pro laptop with 11 cores, five seconds per mode,
gave the figures below. They show what the row cache changes on this
workload; they are not a guarantee for another table, machine, or skew.

| | Value-page GETs per lookup | Lookups/s, 1 task | Lookups/s, 11 tasks | Median latency, 1 task |
|---|---|---|---|---|
| Row cache off | 0.68 | 20,900 | 72,100 | 61 µs |
| Row cache on | 0.39 | 33,300 | 137,800 | 10 µs |

The row cache held about one row in sixteen and still served about 61
percent of lookups, where the page cache alone, holding about one page
in six, served about 32 percent: each cached page held a few hot rows
among a thousand cold ones. A table's own gain depends on its skew, its
row size, and its budget, and its hit rate is the one to watch.

## The page cache

icetray caches value pages compressed, keyed by file and offset. Iceberg
data files are immutable, so a cached page never goes stale, and icetray
evicts pages by size alone. Concurrent lookups that need the same
uncached page share one GET.

- **The memory level**, its part of `server.cache_bytes`
  ([Configuration](configuration.md#server)), holds the hottest pages. A hit there costs no I/O. The level is split
  into shards, one per worker thread rounded up to a power of two, and
  each shard evicts within its own share of the budget. A newly cached
  page that is not asked for again soon leaves before the pages that
  are, so a pass over many pages once does not push the hot ones out.
- **The disk level**, `storage.local.page_cache_disk_bytes`, stores the
  pages the memory level evicts, in 4 MiB blocks in the local directory.
  A hit there costs one local read and no GET, and the level persists
  across a restart, so a restarted replica keeps its warm pages. Each
  block holds one open file descriptor
  ([File descriptors](operating.md#file-descriptors)).

A cold hit costs a GET of tens of milliseconds, and a warm hit from
memory costs microseconds, so the cache's hit rate sets most of a table's
average lookup latency. icetray caches the whole value page a hit reads,
so the page size sets how much of the budget one hot key occupies. The
optimal layout bounds value pages by bytes for large values for that
reason ([Page and file sizes](#page-and-file-sizes)).

`icetray_lookups_total` by `result` gives the split of cold hits, warm
hits, and misses, and `icetray_page_cache_hits_total` against
`icetray_page_cache_fetches_total` gives the hit rate at a replica's
budget ([Metrics](operating.md#metrics)). When a larger budget raises one
replica's hit rate, raise the budget on every replica.

## Hedging slow GETs

When a lookup GET has not completed after a delay, icetray sends it a
second time and uses whichever response arrives first. The delay is a
high percentile of the table's recent lookup GET latencies over a window
of the latest GETs, so it tracks the object store's latency as it drifts.
A ceiling caps the share of the window's lookup GETs that icetray hedges. icetray never hedges load
reads. A `Scan`'s span GETs are hedged under the same settings in a
window of their own per table, and every span GET races that window's
one delay whatever its pages, since spans of up to 16 pages share a
one-page GET's first-byte latency on S3 ([ordered rows on
S3](analysis/2026-10-19-ordered-rows-on-s3.md#span-latency-5-to-16-pages)).

- A lower percentile hedges sooner and cuts more of the tail, at the
  cost of more GETs. A higher one sends fewer redundant GETs and lets a
  stall run longer.
- icetray hedges a table's GETs only once the table's latency window is
  full of lookup GETs, so a rarely read table may never be hedged; a
  smaller `window` starts hedging sooner, with a noisier delay.
- `icetray_hedges_total` against `icetray_object_gets_total{kind="lookup"}`
  shows what hedging spends.

The settings, `percentile`, `window`, and `max_rate`, are under
[`[storage.hedge]`](configuration.md#storagehedge) with their values,
and the decision is [RFD 013](rfd/013-tail-latency.md).

## Load time

A first load reads every data file's footer and key chunks, two GETs for
a file in the optimal layout, and verifies every key. Past enough reads
in flight, its time goes to CPU: decoding and verifying key chunks, and
for `disk` tables writing chunk files. icetray runs one worker thread per
core the process sees, or `TOKIO_WORKER_THREADS`, and each load keeps up
to 128 reads in flight per worker, at most 1,024. Load time therefore
scales with the cores the process is given, and a tight CPU limit
stretches a load toward `server.load_deadline` ([CPU](operating.md#cpu)).

- A refresh reuses the index of every data file the previous generation
  held, so its cost is the data files that are new.
- A restarted replica with `disk` tables reuses every chunk file whose
  checksum matches, and reads only those data files' footers.
- The doctor decodes and hashes every key to check its bucket, which a
  load does not, so it takes longer than `info` on the same table.

## Deletes

icetray holds a merge-on-read table's deleted rows in memory, and a
lookup checks them before it reads a value page
([RFD 021](rfd/021-merge-on-read.md)). The check takes 7 to 58 ns, at
most three thousandths of the key-page decode beside it, so a lookup's
cost under deletes is the files it probes, one key-page decode each
([analysis](analysis/2026-10-07-merge-on-read-costs.md)):

- A hit on an updated key probes one file however many commits updated
  it, because icetray probes the newest copy first.
- A miss on a deleted key probes every file that holds a deleted copy.
  It does under `bloom` too, since a deleted row stays in its file's
  filter, and each probe there costs a key-page GET when the page is not
  cached. Each commit that updates the key adds such a file until a
  compaction rewrites the bucket; `icetray_table_files_probed_per_lookup`
  shows how many files a lookup probes at worst.
- The delete state occupies two to three bytes a deleted row when deletes
  are scattered through a block, and a small fraction of a byte when they
  fall in runs, as a delete of a key range leaves them. A `full` table's
  resident budget counts an upper bound, which is exact for scattered
  deletes and generous for runs and, at low fractions, for position
  delete files that name no data file. `icetray_table_delete_state_bytes`
  reports the bytes held.
- Each position delete file adds one read to a load, and each deletion
  vector two, its Puffin file's footer and its blob. A refresh reads only
  the new delete files.

## File descriptors

A replica's descriptor count grows with three things: the disk level's
size, one descriptor per 4 MiB block; the number of tables, since every
configured table loads at once at start with up to 1,024 reads each; and
the data files of `disk` tables, one chunk file each, twice that during a
refresh that rewrites them. icetray raises its soft limit to the hard
limit at start and warns when the hard limit is below what the disk
level and the start's loads need. The rule and the messages are in
[File descriptors](operating.md#file-descriptors).

## Page and file sizes

The writer fixes several costs when it writes the table, and the
[optimal layout](table-layout.md) sets each one where a lookup is
cheapest:

- **Key pages of 1,000 rows.** A lookup decodes one key page, so the
  page's row count sets the keys a lookup decodes. The doctor reports
  pages above 2,000 rows as `large_key_pages`, which caps the table's
  level at `balanced`.
- **Value pages bounded by bytes.** A hit fetches, decompresses, and
  caches one whole value page for each column it requests. For values of
  kilobytes the optimal layout bounds pages at 64 KiB; a page bounded by
  rows alone grows to megabytes, which every hit transfers, and the
  doctor reports a column whose mean value page is over 1 MiB as advice.
- **Data files of 32 to 64 MiB.** Each file adds a footer read at load,
  resident index overhead, and under `disk` a descriptor. A range GET's
  first-byte time also varies with the size of the file it reads.
- **The bucket count.** `bucket(N, key)` lets a lookup probe one file. `N`
  near the table's compressed size divided by 64 MiB gives one
  target-sized file per bucket;
  [Choosing the bucket count](table-layout.md#choosing-the-bucket-count)
  covers the trade-offs.
- **The key bloom filter.** Its size per key sets `bloom` residency's
  memory and the GETs of its misses; only `bloom` residency's costs
  depend on it.

## The memory limit and the kernel's page cache

A `disk` table's local reads are memory copies only while the kernel's
page cache holds its chunk files. On Linux that cache counts against a
container's memory limit, so a limit sized for the process alone leaves
the chunk files nowhere to stay cached, and every lookup waits on the
device. Raise the limit by the `disk` tables' `predicted.disk_bytes`. The
extra is cache the kernel reclaims under pressure, so a scheduler's
reservation, such as a Kubernetes memory request, can stay at the
process's own size ([Memory](operating.md#memory)).

The server checks both ends of this. Once the tables serve, and after a
reload that adds a `disk` table, it warns when the chunk files and the
disk level need more than the limit leaves beside the process. While it
serves, it samples how much of each `disk` table's chunk files the page
cache holds, `icetray_table_chunk_resident_fraction`. Below 0.95 the
table's level falls to `balanced`, below a configured `optimal`, and it
returns above 0.99. The thresholds come from a sweep of `disk` lookups
over uniform keys with chunk-file pages evicted at random, the worst case
for a cache ([analysis](analysis/2026-10-07-page-cache-residency.md)). A
miss there took about 100 µs with its key page in the page cache and
about 730 µs with the key page read from the device. The miss's 90th
percentile moved to the device latency once one page in twenty was
missing, and above 0.97 resident only its 99th percentile had moved.
Under skewed access the kernel evicts the coldest pages first, so at the
same fraction fewer of a real table's lookups wait on the device. The
fraction shows whether the chunk files stay cached;
`icetray_lookup_seconds` measures the latency itself.

Put the local directory on a mounted volume with a filesystem such as
ext4. On a container's overlay filesystem icetray cannot serve a cached
read without blocking, so every local read there moves to another thread
([The local directory](operating.md#the-local-directory)).

## The decisions behind these trade-offs

- [RFD 004](rfd/004-resident-key-chunk-index.md): what stays resident per
  data file, and why a hit costs one GET per column it reads and a miss
  none.
- [RFD 006](rfd/006-lookup-hot-path.md): the CPU a lookup spends decoding
  its key page and its value pages.
- [RFD 013](rfd/013-tail-latency.md): hedging, deadlines, and the bounds on
  loads.
- [RFD 017](rfd/017-residency-tiers.md): the three residencies, the local
  directory, and the descriptors they hold.
- [RFD 002](rfd/002-serving-layout-contract.md): the optimal layout and
  the reason for each of its requirements.
- [RFD 022](rfd/022-value-columns.md): value columns, and when to split a
  row across them.
- [RFD 024](rfd/024-field-projection.md): field projection, and when to
  project within a column rather than split.
- [RFD 026](rfd/026-row-cache.md): the row cache, what it is keyed by,
  and why it is never invalidated.
- [RFD 025](rfd/025-ordered-rows.md): ordered tables, what a `Scan`
  reads in each residency, and spreading a `MultiScan` across idle
  worker threads.
- [RFD 027](rfd/027-one-page-cache.md): one page cache for every table,
  and a `Scan`'s span read from memory and the disk level.
