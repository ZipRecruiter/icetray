# Ordered rows per key

> Updated 2026-10-20: `icetray scan` takes a bound of a `date` or
> timestamp column as ISO 8601 text as well as the long, since a person
> reading a time series types a date or an instant, converted through
> jiff to the column's unit. Each type takes exactly one form, so no
> text jiff also reads passes for another time: `YYYY-MM-DD` for
> `date`, and `YYYY-MM-DDTHH:MM:SS[.fraction]` with `Z` or `±HH:MM` for
> every timestamp type, `timestamp` included, so no local time zone is
> consulted; a `timestamp` bound is the instant's UTC wall-clock time.
> A value that would move the window is refused rather than rounded: a
> leap second, which jiff reads as the second before, and digits below
> a microsecond type's unit. Only the ISO form is limited to jiff's
> dates: digits alone stay the long, over every value the column holds,
> so a printed `next`, a long, always pastes back, and `next` and
> `order` stay longs. No other order type takes ISO text. The
> configuration reference gives the forms and ranges.

> Updated 2026-10-20: the span hedge window stays separate from the
> lookup window, decided under Caching and hedging, and one hedge window
> per table joins the rejected alternatives.

> Updated 2026-10-20: every span GET races its window's one delay and
> enters the window with its whole elapsed time, whatever its pages:
> spans of 5 to 16 pages share a one-page GET's first-byte latency on
> S3, so the delay no longer scales with a span's pages.

> Updated 2026-10-20: a Scan's or MultiScan's own faults and those its
> table's configuration finds, its limit and a MultiScan's values among
> them, are answered before the in-flight bound and the table's
> readiness, in the order RFD 010 (Transport and client contract) gives.

> Updated 2026-10-20: a failing MultiScan answers the error of its first
> failing key in request order, whether its keys are spread over worker
> threads or read on one task.

> Updated 2026-10-20: a limit of 0 is a fault of the request alone,
> answered whatever its table, unknown or loading included, so its
> message names no `max_scan_rows`; a limit above `max_scan_rows` stays
> a fault the table's configuration finds.

> Updated 2026-10-20: `icetray scan` runs one Scan from the command line
> through the same checks, a wrong-kind table refused from its
> configuration before any load, and `get` of an ordered table likewise.
> Its printed `next` and a row's `order` take one text form per order
> type, a time type's as a long in Iceberg's unit, as the wire carries
> it; its bounds take that form and, for `date` and the timestamps, ISO
> 8601 text too. Not every value has an ISO form: jiff, icetray's time
> library, spans about the years -9999 to 9999, so the extreme values
> of a `date` column (about 5.9 million years from the epoch) or a
> microsecond one (about 292,000) have none, though every nanosecond
> value has one. The printed `next` must round-trip exactly, and printed
> ISO would give `timestamp` and `timestamptz` different forms. A string
> is its text with a backslash written `\\` and a zero byte `\0`, since
> no argument holds the zero byte an oldest-first `next` ends in. The
> command does not apply `server.max_scan_bytes`, which bounds a gRPC
> response.

> Updated 2026-10-30: an ordered table's pages are cached in the one page
> cache every table shares, and a span read takes each page from memory,
> then from the disk level, fetching only the runs neither holds;
> `server.scan_page_cache_bytes` is removed. A cache of ordered tables'
> pages alone, kept out of the disk level, joins the rejected
> alternatives, since a key-value table's hot rows now sit in the row
> cache (RFD 026). See RFD 027.

## Problem

icetray serves tables with at most one row per key, and that remains its
primary use. A second shape is common: many rows per key in a defined
order, such as a time series ordered by key and timestamp, an activity
log per user, or the versions of an entity. Callers request the last N
rows of a key, every row of a key in order, or the rows on one side of
an order value, for one key or for many in a batch, at request time.
Most keys in such a table are cold for the reason most keys in a
key-value table are: a small active set beside a long tail whose history
is read rarely and unpredictably.

Under RFD 002 (Serving layout) the load refuses this shape: a key
repeated within a file or across live files is `duplicate_keys`, and a
file sorted by key alone leaves the order of a key's rows undefined. A
writer can pack each key's history into one value, at the costs
Alternatives Considered sets out.

The resident index already bounds where a key's rows lie. Each file's
fences hold the first key of every key page, so they bound the pages a
key's rows occupy, and each column's offset index maps a range of rows
to that column's pages.

## Decision

Ordered rows per key will be served from an opt-in table kind, the
ordered table, which the owner configures with an order column and
writes sorted by key and then order value, through two new calls, Scan
and MultiScan, with which a caller requests a key's rows in either
direction under a limit and optional bounds on the order value.

- **Configuration.** A table entry that sets `order_column` is an
  ordered table; every other table is a key-value table. Configuring the
  key column or a value column as the order column is a configuration
  error. Setting, changing, or removing `order_column` reloads the whole
  table under RFD 018 (Configuration reload). `max_scan_rows`, set only
  on an ordered table, is the most rows one Scan returns, and a change
  to it applies in place. `value_columns` and their formats follow RFD
  022 (Value columns) and RFD 024 (Field projection).
- **Order values.** The order column is Iceberg `int`, `long`, `date`,
  `timestamp`, `timestamptz`, `timestamp_ns`, `timestamptz_ns`,
  `string`, `binary`, or `fixed`, and the load refuses any other type as
  `order_type_unsupported`. A file that stores the order column in
  another Parquet type than Iceberg's, such as INT96 or millisecond
  timestamps, is refused as `order_type_mismatch`, the order column's
  counterpart of `key_type_mismatch`. Integer and time values compare as
  signed 64-bit integers in Iceberg's unit (days, microseconds,
  nanoseconds), and string, binary, and fixed values compare as their
  bytes, the order RFD 008 (Keys) gives keys. The column may hold any
  quantity with a total order in one of these types, such as a
  timestamp, a sequence number, a version, or a log offset. A file
  written before Iceberg promoted the column from `int` to `long` is
  served, its values read as longs, and a promotion that changes the
  unit, such as `date` to `timestamp`, is `order_type_mismatch`.
- **Uniqueness.** A key and order value appear together at most once
  within a data file, deleted rows included, and at most once across
  live rows, so every Scan has one correct response in either direction.
  A writer deduplicates before the write, or upserts with `MERGE` on key
  and order value, whose deletes the server applies under RFD 021
  (Merge-on-read). Rows of one key that share a timestamp need a
  tiebreak inside the order value, such as a sequence number or a time
  with a counter.
- **Wire.** `Scan` takes a table, a key, the value columns to read as
  `Get` takes them, a direction (`NEWEST_FIRST` or `OLDEST_FIRST`), a
  limit from 1 to `max_scan_rows`, and optional half-open bounds on the
  order value, `start` inclusive and `before` exclusive. A Scan and each
  key of a MultiScan set their limit: there is no default, so the rows a
  call asks for, and the values a MultiScan counts against
  `server.max_multi_scan_values`, are what its request states. The
  response carries the rows in the requested direction, each its order
  value and one value per requested column, `next`, the bound of the
  next page (Paging), set exactly when live rows remain inside the
  bounds past the last row returned, and the snapshot id. For a key with
  no rows the server returns no rows and no `next`. An order value
  travels as an `OrderValue`, a long, a string, or bytes, with time
  types sent as a long in Iceberg's unit, so a bound past an `int` or
  `date` column's range admits every row on its side. A MultiScan
  carries one set of columns, direction, limit, and bounds for many
  keys; the server returns one result per key, each with its own rows
  and `next`, in request order from one snapshot, a key listed twice
  answered at each position as `MultiGet` answers it, and fails the
  whole call when any key fails, as for `MultiGet`. `TableInfo` reports
  the order column, its type, `max_scan_rows`,
  `server.max_multi_scan_values`, and `server.max_scan_bytes`. Errors
  follow RFD 010 (Transport and client contract) as for `Get` and
  `MultiGet`, and a malformed request is `INVALID_ARGUMENT`: an
  unspecified direction, a limit of 0 or above `max_scan_rows`, a key
  or bound of the wrong variant, a `start` at or above `before`, which
  is an empty window, a MultiScan above `max_multi_scan_values`, or a
  call of the wrong kind for the table. The server checks a request in
  the order RFD 010 gives: the request's own faults, such as an
  unspecified direction, a limit of 0, which no table answers, or a
  `start` at or above a `before` of its variant; then the table's
  configuration, a table not served first, then the call's kind, a limit
  above `max_scan_rows`, and a MultiScan's values against
  `server.max_multi_scan_values`; then the in-flight bound; then the
  table's readiness; then a key or bound of the wrong variant and a
  column the table does not serve, which need the loaded snapshot. So a
  call of the wrong kind, or one whose limit or values are out of bounds,
  is `INVALID_ARGUMENT` even while the table loads, and never the
  retryable `UNAVAILABLE`. A value that is not
  the protobuf its column declares is `FAILED_PRECONDITION` as for `Get`
  (RFD 024), naming the order value of its row, and so is a response
  above `server.max_scan_bytes` (Limits).
- **Paging.** A caller pages by passing `next` back unchanged, every
  other field of the request as it was. `next` follows the last row
  returned: reading newest first it is that row's order value, and the
  next request sets `before` to it; reading oldest first it is the least
  order value above that row's, one more for an integer or time type and
  the value with one zero byte appended for a string, binary, or fixed
  type, and the next request sets `start` to it. The server sets `next`
  only when it has read a live row past the page, so a caller stops at a
  response without one and never asks for a page past the last row.
  Within one snapshot, uniqueness makes the next response exact, and
  paging is stateless on the server. After a refresh between two requests
  the server responds from the new snapshot, whose id the response
  carries: the next page holds every row of the new snapshot past the
  last row returned, a row committed there included, so nothing between
  two pages is skipped, and a row committed on the side the caller has
  already read appears only to a Scan that starts over.
- **Layout.** The optimal layout for an ordered table is RFD 002's with
  two changes: each file is sorted locally by key and then order value,
  ascending, and a key may repeat within a file. A key's rows in one
  file are therefore consecutive, a run sorted by order value, which may
  span key pages and row groups. Partitioning stays `bucket(N, key)`, so
  within a bucketing all of a key's rows share a bucket. Appends write
  files that overlap a bucket's existing ones, and a compaction merges
  them. The order column carries an offset index, which Iceberg's
  writers write by default; the server does not read its column index.
- **Load.** The load verifies that each file's rows strictly increase by
  key and then order value, across row-group boundaries included: an
  equal pair is `duplicate_rows`, naming the key and the order value, a
  decrease is `unsorted`, and a null order value is `null_order`, and a
  `string` order value that is not UTF-8 is a file fault, as a string
  key that is not UTF-8 is. To verify, it reads the order column's
  chunks, one GET per row group, and records per file the exact minimum
  and maximum order value over all its rows, its distinct keys per row
  group, its longest run, and the order fences: for each order page, the
  shortest pair of key and order value that sorts above the last row of
  the page before it and at or below the page's first row. Integer and
  time values in a fence are exact, and string, binary, and fixed values
  are cut to the shortest prefix that keeps that order. Rows are sorted
  by key and then order value, so a page holds exactly the pairs from
  its fence up to the next page's, and the fences place any pair on its
  page whatever the writer's column index holds or leaves out. Resident
  per file are those facts, the fences, and the order column's offset
  index. A file without an order column offset index is a file fault, as
  a key or value column without one is. Order pages get RFD 004's
  value-page header checks and its value-page ceiling, at load and in a
  Scan. Under `disk` every chunk file is named by its data file and the
  rules it was verified under, the key-value rules or the ordered rules
  with the order column's field id, and its verification record names
  those rules. An ordered table's chunk file holds its key chunks, its
  order chunks, those facts, and the fences, so a restart that trusts it
  restores them without decoding a value. Entries that serve one table
  with different order columns, or with none, therefore each keep their
  own chunk file, and none trusts a file verified under other rules. The
  chunk-file budget and the resident budget reserve an ordered file from
  the footer's sizes, estimating string and binary order values, in the
  facts and in each order fence, at the order chunk's uncompressed bytes
  per value. No footer number bounds the fences of a dictionary-encoded
  order column, so the exact fences are charged once a file is built:
  the chunk file's exact length is reserved before it is written, a
  `full` table's exact resident bytes are checked against its budget,
  and a load over either is refused as `over_budget`. Column resolution
  under RFD 023 (Column resolution) applies to the order column, and a
  file written before the order column was added is refused as
  `column_mismatch`, since its default would give every row of the file
  one order value.
- **Refresh.** Across files, the load checks for a repeated live pair
  only among files where one can occur. A file is checked when it is
  new, or reused with deletes that shrank: deleted rows that are not a
  superset of those the previous generation applied (RFD 021). It meets
  another file, new or reused, when the two can share a key under RFD
  005's overlap rule across buckets and bucketings and their key ranges
  and order ranges both intersect, ranges whose ends touch included. A
  checked file that meets no other, as an append of later rows does,
  needs no merge and is read no further. In each overlap group that
  holds a checked file meeting another, the load runs one k-way merge by
  key and order value over only the order pages that can hold a pair
  another file of the group also holds, judged by the fences: a page's
  pairs lie between its fence and the next page's. It takes the pages of
  a checked file that meet a page of any other file of the group, and
  the pages of any other file that meet a page of a checked file. It
  reads each such page at most once: by GET under `full`, from the
  ordered chunk file under `disk`, and with the key pages they span
  under `bloom`. The overlap groups merge at once, and fail as a serial
  check would: the first group in group order that fails, by a refusal
  or a read error, names the failure, and the groups after it stop.
  Each merge reads a file's next row group when it reaches it, whatever
  that takes, and beyond those reads later row groups ahead of need,
  across its files in the order it reaches them, while the bytes every
  merge of the load holds and has in flight stay within one fixed
  read-ahead bound ([Ordered tables](../operating.md#ordered-tables)
  gives it). The load therefore holds at most that bound beyond one row
  group's chosen pages per file, and a file whose row groups are larger
  than the bound merges a row group at a time. Where the chosen pages of
  the groups merging at once fit within the bound, and their reads
  within the load's concurrency, a merge over files of several row
  groups waits on about the rounds of GETs a merge over files of one row
  group does. A merge that refuses or ends first may have read row
  groups ahead that it never reached, and a read error on a row group
  read ahead fails the load as any read error does, even where the merge
  would have refused first. Each run of consecutive chosen pages costs
  one GET or local read for the order column, and under `disk` and
  `bloom` one for the key column, and a dictionary page one more. The
  load refuses a live pair present twice as `duplicate_rows`, naming the
  later file in file order, as `duplicate_keys` does. A refresh that
  appends later rows therefore reads only its new files, files a
  compaction rolled by size cost the pages at the edge keys they share,
  and an append of rows inside an existing file's order range costs the
  pages those rows fall among, in the new file and the old.
- **Cost.** Per file that holds the key, a Scan finds the run, then
  reads the order column and each requested column over the run's rows,
  in one round across all files:

  | Step | `full` | `disk` | `bloom` |
  |---|---|---|---|
  | Find the run | decode of at most two key pages per row group it spans, from memory | the same, read from the local file | the filter screens the file, then GETs of those key pages, concurrently, and of the key column's dictionary page when it has one |
  | Read the rows | per column and row group, one GET per contiguous span of uncached pages over the rows read, and the column's dictionary page when it is dictionary-encoded | the same, with the order column read from the local chunk file | the same as `full` |
  | A file without the key | one key-page decode | one local read | a key-page GET at the filter's false-positive rate |

  Within a row group, the run lies between the first and last key pages
  the fences allow for the key, and every page between them holds only
  the key, so decoding those two gives the run's exact rows; the
  resident first and last key of each row group give the row groups a
  run spans. The server takes up to the limit of the run's newest or
  oldest live rows, counted against the resident deleted rows without
  I/O. It passes over a whole order page of deleted rows in one step and
  decodes no page that holds none of the rows it reads, so such pages
  split a column's span; where the pages between two spans are few
  enough bytes that fetching them costs less than the GET it saves, one
  GET fetches them with both spans ([What a Scan
  costs](../performance.md#what-a-scan-costs) gives the size). With a
  bound, a binary search over the run's fences places the bound on one
  order page, and the server reads that page with the value pages in the
  same round, so beyond the rows the limit selects, a bounded Scan reads
  in each requested column at most the rows of the order page that holds
  the bound. Where that page is local, under `disk`, or already cached,
  the server decodes it first and reads only the value pages of the rows
  taken, at no more rounds to object storage. A Scan's work follows its
  limit, not its run's length. Across K files that hold the key, a Scan
  with limit L reads up to K·L rows and returns L; to set `next`
  exactly, the server reads the order value of one more live row in each
  file. A Scan of a key held in one file, with none of its pages cached,
  no dictionary-encoded column, and its run in one row group, costs one
  GET per requested column and, under `full` and `bloom`, one for the
  order column, as a cold Get with one more column does, with the
  key-page GETs first under `bloom`, and its bytes grow with the limit
  and the values' size.
- **Caching and hedging.** Each page a span GET fetches is cached as its
  own entry, keyed by file and offset, in the page cache every table
  shares, and a span read takes a page the disk level holds from it
  before it fetches (RFD 027 (One page cache)). Concurrent Scans
  that need one page issue one GET for it, as concurrent lookups do. A
  span GET takes its fan-out slot before it claims its pages, as a
  lookup's GET does, so a Scan of another call waits only on pages a GET
  is fetching, never on a GET still waiting for its slot. Span GETs are
  hedged under RFD 013 (Tail latency) in a window of their own per
  table. The window's one delay applies to every span, whatever its
  pages, and each span enters it with its whole elapsed time: on S3,
  spans of 1 to 16 pages share one first-byte latency, the transfer of
  a 269 KiB span adding about 2 ms at p50 and 13 ms at p99 ([span
  latency, 5 to 16
  pages](../analysis/2026-10-19-ordered-rows-on-s3.md#span-latency-5-to-16-pages)).
  Key-page GETs under `bloom` use the table's lookup window. The two
  windows stay separate: a table usually serves one kind of call,
  ordered or key-value, so in practice one window is fed, and separate
  windows keep one call type from spending the other's hedge ceiling
  and span transfer times out of the lookup percentile.
- **Limits.** `server.batch_fan_out` bounds the GETs one MultiGet, one
  Scan, or one MultiScan holds in flight. A MultiScan spreads its keys
  across idle worker threads as a MultiGet does: while fewer calls than
  worker threads are in flight, its distinct keys are split in order and
  evenly into as many runs as a fixed count of distinct keys per run
  requires, up to one run per worker ([MultiScan across worker
  threads](../performance.md#multiscan-across-worker-threads) gives the
  count), and the runs are read on several cores at once under the one
  bound. A MultiScan of no more distinct keys than that count, or one
  that arrives while as many calls as worker threads are in flight,
  reads its keys on one task. Its answers, their order, its counts, and
  the response bound are the same either way, and so is the error of a
  failing MultiScan: that of its first failing key in request order,
  whichever key failed first. A run's failure is held until every run
  before it has ended, and the runs after it are stopped, so the error a
  caller sees, and whether it retries, does not depend on scheduling.
  Spreading cuts a lone MultiScan's latency for more CPU per key, spent
  only while cores would otherwise sit idle.
  `server.max_multi_scan_values` bounds a MultiScan's keys times its
  limit times one more than its requested columns, and the server
  rejects a larger call before it reads any key. A configuration whose
  `max_multi_scan_values` is below twice an ordered table's
  `max_scan_rows` is refused, so a MultiScan of one key at the full
  limit and one column is always valid. That limit applies to a
  MultiScan of one key as to any other, and a caller reading one key's
  full limit over many columns uses Scan. `server.scan_deadline` bounds
  one Scan and each span GET, and `server.multi_scan_deadline` one
  MultiScan. `server.max_scan_bytes`, 4 MiB by default, a gRPC client's
  default message limit, bounds the encoded size of one Scan or
  MultiScan response. The server refuses a larger response as
  `FAILED_PRECONDITION`, returning no rows and naming its size, the
  bound, and the rows that would fit, and counts it as an error; a
  caller lowers its limit, and an owner whose clients accept larger
  messages raises the bound. The bound is on the response, not on what
  a call holds while it reads: each file that holds the key reads up to
  the limit's rows of every requested column before the merge cuts them
  back to the limit, so one call holds about the files that hold the key
  times its limit times its requested columns in values, and reads all
  of them before it refuses an oversized response. A MultiScan holds
  that for each key it is reading, up to `server.batch_fan_out` keys at
  once in each of its runs (at most one run per worker thread), and the
  merged rows of every key until it responds. The server accepts this
  rather than merge order values across files before reading any value,
  which would hold one limit's rows but add a round of GETs to a Scan of
  a key in several files. `large_runs` (Levels and findings) names each
  file on which a Scan at the full limit would be refused, and an owner
  bounds what a call holds with `max_scan_rows`, and with compaction,
  which lowers the files that hold a key. `server.max_in_flight` counts
  every Get, MultiGet, Scan, and MultiScan, and TableInfo takes none of
  its slots.
- **Levels and findings.** RFD 020 (Lookup levels) applies, read for a
  Scan of one key. When no two files in a bucket have key ranges that
  intersect, ends included, every Scan reads one file, and the table can
  reach `optimal`; decoding two key pages per row group, and a run that
  spans row groups, leave it there. Files that one snapshot added, as a
  compaction that rolls files by size writes them, may meet where one
  file's last key is the next file's first, so a Scan of such an edge
  key reads every file that holds it. That cost follows from how those
  files were written and does not grow with later commits, so
  `shared_edge_keys` caps the table at `balanced` and reports the most
  files one edge key spans. Any other overlap, as appends write, is
  `permissive` under `overlapping_files`, including a file of another
  snapshot that touches only an edge key. The strict gate tells the
  three apart from key ranges, edge keys, and the snapshot each manifest
  entry records as adding its file, inherited from its manifest when the
  entry leaves it null, as RFD 021 inherits sequence numbers, before any
  chunk is built. `lookups` defaults as RFD 020 sets it, so an owner
  whose ordered table takes appends between compactions sets `lookups =
  "permissive"` to keep it from being reported below its level.
  `no_bucket_partition` and `large_key_pages` apply unchanged. Under
  `full` and `bloom`, `dictionary_encoded_order` caps the table at
  `balanced`, for a dictionary-encoded order column, whose dictionary
  page every Scan also fetches; under `disk` the dictionary page is a
  local read. The advice `long_runs` reports a file holding a run longer
  than the default `max_scan_rows`, which a caller reads in more than
  one Scan, and `large_runs` a file where `max_scan_rows` rows of the
  configured value columns, at the footer's uncompressed bytes per row,
  would exceed `server.max_scan_bytes`, so a Scan at the full limit
  would be refused. The bloom findings divide a row group's filter bits
  by its distinct keys, which the load counts.
- **Metrics.** `icetray_scans_total` by table and result (`rows`,
  `empty`, or `error`), `icetray_scan_rows` and `icetray_scan_files` as
  histograms, `icetray_scan_seconds`, cold and warm, and
  `icetray_scan_response_bytes`, the encoded size of each Scan and
  MultiScan response, a refused one included. A Scan is cold
  when an order or value page it reads, or such a column's dictionary
  page, came from object storage, as a Get's cold hit counts its value
  pages (RFD 022); its key-page GETs under `bloom` leave it warm.
  `icetray_value_page_gets_total` counts the order column's GETs under
  its column name. `icetray_table_gets_per_scan` predicts a cold Scan at
  `max_scan_rows` of one key held in one file and one row group.
  `icetray_table_files_probed_per_lookup` and the per-column levels are
  published for ordered tables as for key-value ones, RFD 020's per-hit
  and per-miss gauges are published for key-value tables only.

## Alternatives Considered

Each key's history packed into one value, served by `Get`. It works with
no change. Rejected as the design, because every read fetches the whole
history, the number of rows a caller receives is fixed when the table is
built, a value is bounded by the page ceiling and the message size, and
each append rewrites every touched key's history.

A range scan over a composite key, with the key column encoding key and
order value together. Rejected because `bucket(N, key)` over the
composite would scatter one key's rows across buckets, so the table
would still need a separate column to partition on, and every caller
would encode ranges itself.

Ties within a key broken by file and row position. Rejected because the
order that results carries no meaning, and a retried duplicate would be
served as a second row.

Server-streaming calls. Rejected for the first version, because a limit
bounds every response and paging by order value reads a longer history
with no state on the server.

Opaque cursor tokens. Rejected because uniqueness makes the order value
an exact cursor that the caller already holds.

A snapshot id on the request, refused once the server has moved on.
Rejected for the first version, because the response's snapshot id
already shows a caller that a refresh came between two requests.

The order column decoded and resident. Rejected because its memory grows
with rows, which in an ordered table outnumber keys, while one fence per
order page places any bound on its page.

Bounds placed by the writer's order column index, the minimum and
maximum of each page. Rejected because a writer may truncate string
entries or leave the index out, and either costs a bounded Scan a
second round, because a page that holds several keys' rows mixes their
order values, so the index prunes the uniqueness merge poorly where runs
are short, and because the load already decodes every pair the fences
need.

A two-phase read across files, order values first and then values only
for the rows returned. Not chosen for the first version, because it adds
a round to every Scan of a key in several files, while the one round
reads an excess that a compaction removes.

One GET per page in place of span GETs. Rejected because a full run of
small values spans dozens of value pages, and each GET is a round trip
billed on its own.

A memory cache of ordered tables' pages alone, kept out of the disk
level, so a long run could not evict a key-value table's hot pages.
Chosen first, and rejected by RFD 027 once those tables' hot rows moved
to the row cache of RFD 026 (Row cache): it kept Scans off the local
disk to protect key-value pages whose hot rows the row cache holds.

A re-merge of every overlap group that gains a file, as a key-value
table's refresh does. Rejected because appends make every file in a
touched bucket overlap, so each refresh would re-read the order column
of every file in those buckets.

Order values normalized from other units, such as millisecond
timestamps. Rejected because Iceberg writers store Iceberg's units, and
a file in another unit comes from a migration that a rewrite repairs.

`bloom` residency refused for ordered tables. Rejected because a bloom
filter covers a column's distinct values, so a key repeated across rows
is one entry, and the fences bound the key pages a Scan needs.

Exclusive bounds on both sides, `after` and `before`, with a flag for
whether rows remain, a caller paging by the last order value it was
returned. Rejected: half-open windows tile a key's rows, a window ending
where the next begins with no row in both or in neither, and the common
request, the rows at or after a time, is a `start` as the caller states
it.

A limit of 0 that reads up to `max_scan_rows`. Rejected: a request that
leaves its limit unset would ask for as many rows as the table's
configuration allows at the time, and a MultiScan would count that
against `server.max_multi_scan_values` for every key, so a caller sets
the limit it means.

Truncation by response bytes. Rejected in favor of a row limit the owner
sizes against value size, a response bound above which the server
refuses, and an advice finding for runs too large for that bound: a
truncated response would hand a caller that does not check `next` fewer
rows than it asked for, with no sign.

One hedge window per table, span GETs and lookup GETs feeding one
percentile. Rejected: on an ordered table the lookup window holds its
dictionary-page GETs and, under `bloom`, its key-page GETs, so one
window would change three things. One `max_rate` would cap span GETs
and those GETs together, where each now hedges up to its own ceiling.
Spans would hedge as soon as the one window fills, with no warm-up of
`window` span GETs of their own. And the one percentile would mix span
transfer times, about 13 ms more at p99 for the spans of 5 to 16 pages
measured, into a window of one-page GETs, raising their delay. A table
usually serves one kind of call, so in practice one window is fed
either way.

Bounds per key in a MultiScan. Deferred: one set of bounds serves the
common call, the newest rows of many keys after one order value, and a
caller paging several keys' histories continues with a Scan per key
whose result carries `next`, which costs the server what the MultiScan
would. Optional bounds per key that replace the shared ones can be added
later without changing a caller.

One Scan across several tables that share a key type, merged by order
value. Deferred: a caller can issue one Scan per table and merge by the
order values each row carries, and a later decision can add such a read
as a call of its own or an extension of these.

## Consequences

A key-value table keeps every check, layout requirement, and cost per
call. The ordered rules run only for a table with an order column, and
its chunk files are named by the ordered rules they were verified under.
A process that serves both kinds shares `server.max_in_flight`, its GET
throughput, its page cache (RFD 027), and resident memory across them,
and one `server.batch_fan_out` setting governs both. A regression
benchmark of warm and cold Get, MultiGet, and misses, on a
process serving key-value tables alone and on one serving both kinds,
holds the key-value path to its current costs.

The wire contract grows by two calls and an order value, and
`server.multi_get_fan_out` is renamed `server.batch_fan_out`, which
before 1.0 is allowed in place (`CONTRIBUTING.md`, Stability).

A cold Scan of a key in K files costs K times the GETs of a compacted
one and waits on the slowest of them, so an ordered table's latency
follows its compaction cadence; `icetray_scan_files` reports the files
each Scan reads. Under `disk`, the residency sample of RFD 020 covers an
ordered chunk file's order chunks with its key chunks, since every Scan
reads both. A long run costs bytes in proportion to the rows read, a run
the cache holds in part costs one GET per uncached span, and under
`bloom` a Scan waits on a key-page round before its row round. A
MultiScan at the full limit with one value column covers
`max_multi_scan_values` over twice `max_scan_rows` keys, few at the
defaults, so a caller reading many keys requests a smaller limit. The
span hedge window's one delay applies to every span: on S3, spans of 1
to 16 pages shared one first-byte latency ([ordered rows on
S3](../analysis/2026-10-19-ordered-rows-on-s3.md#span-latency-5-to-16-pages)),
and a span past 16 pages is unmeasured. The span window has the lookup
window's configuration and mechanism and differs from it only in the
GETs that feed it: on an ordered table the lookup window holds its
dictionary-page GETs and, under `bloom`, its key-page GETs.

A load reads the order column once per row group, so a file costs one
more GET per row group than RFD 004 (Resident key-chunk index) counts,
and the resident budget counts the order column's offset index, its
fences, and per-file facts. A table whose writer leaves files unsorted,
as streaming appends often do, is refused as `unsorted` until a
compaction sorts them.

Earlier RFDs are revised to match: RFD 002 (the optimal layout for an
ordered table); RFD 004 and RFD 017 (Residency tiers) (the order
column's resident indexes and per-file facts, an ordered table's chunk
file, its name, record, and budget, and the descriptor count and
retention that cover it, and finding a run in each tier); RFD 006
(Lookup hot path) (a Scan's steps); RFD 022 (entries over one table
share chunk files only within one kind and order column); RFD 023 (the
order column's resolution and its promotion); RFD 005 (Generations and
refresh) and RFD 021 (which files a refresh checks, and live rows
counted against deleted rows); RFD 007 (Serving tiers) and RFD 016
(Doctor) (the new refusals and findings, and bloom sizing by distinct
keys); RFD 010 (Scan, MultiScan, `OrderValue`, and their errors); RFD
011 (Configuration) and RFD 018 (`order_column`, `max_scan_rows` applied
in place, and the new and renamed server settings); RFD 013 (the batch
fan-out, the scan deadlines, and the span hedge window); and RFD 020
(the levels read for a Scan, and `balanced` extended to edge keys shared
by files one snapshot added). The table layout, the configuration
reference, the calling guide, and the operating guide describe ordered
tables.
