# Operating icetray

Terms such as generation, residency, and degraded are defined in the
[glossary](glossary.md). The trade-offs behind the sizing on this page,
and the settings that adjust them, are in
[Performance and tuning](performance.md).

## Starting and stopping

```sh
icetray serve --config icetray.toml
ICETRAY_CONFIG=/etc/icetray/icetray.toml icetray serve
```

`serve` takes only the configuration path, from `-c, --config` or from
`ICETRAY_CONFIG`, and treats an empty variable as unset. It reads every
other setting from the file ([Configuration](configuration.md)).

- With neither, `serve` exits 64 with
  `no configuration: pass --config or set ICETRAY_CONFIG`.
- When the file cannot be read or fails validation (a file that lists no
  table fails it), `serve` reports the error on stderr as plain text and
  exits 1 before it opens anything. The message names the file and
  whether its path came from `--config` or `ICETRAY_CONFIG`. For an
  invalid file it also names the setting by its full path and the value
  given:

  ```
  error: invalid configuration file /etc/icetray/icetray.toml (from ICETRAY_CONFIG): server.poll_interval 0s is below the minimum of 1s
  ```
- Once `serve` has read the file, it starts logging, and it logs every
  later failure as a JSON event on stdout with the message `run failed`
  and the error in `error`.

At start, `serve` binds the gRPC and metrics listeners, registers gRPC
health as `NOT_SERVING`, and loads every table concurrently. The
listeners accept connections while the tables load.

- Once every table is serving, `serve` sets health to `SERVING`, and
  `/ready` on the metrics listener returns 200. Until then it returns 503.
- A failed first load of any table is fatal: `serve` logs the reason,
  keeps health at `NOT_SERVING` for ten seconds, and exits 1. A strict
  table whose first snapshot is below its `lookups` level fails its first
  load this way, refused as `below_level`, since there is no earlier
  snapshot to hold. So does a load that runs past `server.load_deadline`.
- SIGTERM or SIGINT starts a drain. Health turns `NOT_SERVING`, the gRPC
  listener stops accepting, and the metrics listener closes. icetray waits
  up to `server.drain_deadline` for open calls to finish, then flushes the
  page cache's disk level and exits 0. Past the deadline, it still flushes
  the page cache and exits 1.
- A signal during the first loads cancels them and starts the drain.
- SIGHUP reloads the configuration file
  ([Refresh and reload](#refresh-and-reload)).

`serve` exits with:

- 0 after a drain that finished within `server.drain_deadline`;
- 1 for a configuration that cannot be read or fails validation, a first
  load that failed, a listener that failed to bind, a page cache that
  failed to open (as when the [descriptor limit](#file-descriptors) is too
  low), or a drain that ran past its deadline;
- 64 for a missing configuration path or a command line that does not
  parse.

## The gRPC contract

Callers speak gRPC through
[`icetray.proto`](../crates/icetray-proto/proto/icetray.proto).
[Calling icetray](calling.md) covers requests, responses, the limits a
call meets, and which errors a client retries. The operator sets these
limits in `[server]` ([Configuration](configuration.md#server)):
`max_in_flight` calls at once, past which a call is `RESOURCE_EXHAUSTED`;
`max_multi_get_values` per `MultiGet`; `batch_fan_out` GETs in flight
per `MultiGet`, `Scan`, or `MultiScan`, past which the next GET waits for
one to end; `max_multi_scan_values` per `MultiScan`; `max_scan_bytes`, the
encoded size above which a `Scan` or `MultiScan` response is refused as
`FAILED_PRECONDITION`; and the
`get_deadline`, `multi_get_deadline`, `scan_deadline`, and
`multi_scan_deadline` past which a call is `DEADLINE_EXCEEDED`.

The listener serves plaintext gRPC and authenticates no caller. TLS,
authentication, and access policy are left to the platform: run icetray
on a private network or behind a service mesh or sidecar, and do not
expose it directly. Set `server.listen` and `server.metrics_listen`
([Configuration](configuration.md#server)) to the interfaces that policy
covers.

## Metrics

The metrics listener serves `/metrics` in the Prometheus text format, and
`/ready`. Every metric has a `# HELP` line. Every label value comes from a
closed set, listed below.

Counters and histograms:

| Metric | Labels | Exposed from start | What it counts |
|---|---|---|---|
| `icetray_requests_total` | `call`, `code` | every call and code | gRPC calls, each counted once, by the status the client received. |
| `icetray_object_gets_total` | `kind` | every kind | Object-store GETs, hedges included. |
| `icetray_object_bytes_total` | `kind` | every kind | Bytes those GETs requested. |
| `icetray_local_reads_total` | | yes | Reads from the local directory. |
| `icetray_local_bytes_total` | | yes | Bytes of those reads. |
| `icetray_hedges_total` | | yes | Second GETs issued because a lookup GET or a `Scan`'s span GET was slow. |
| `icetray_page_cache_hits_total` | | yes | Pages served from the page cache without a fetch, from memory or the disk level, for lookups and Scans alike. |
| `icetray_page_cache_fetches_total` | | yes | Page fetches the page cache ran, after coalescing. A `Scan`'s span GET counts once, however many pages it holds. |
| `icetray_row_cache_hits_total` | | yes | Value reads the row cache served without reading a page, a cached null included (RFD 026). |
| `icetray_row_cache_misses_total` | | yes | Value reads the row cache did not hold, each then read from the page cache. |
| `icetray_lookups_total` | `table`, `result` | every table and result | Lookups that completed. |
| `icetray_value_page_gets_total` | `table`, `column` | every table and value column, and an ordered table's order column | Value-page, dictionary-page, and span GETs issued for the column by lookups and Scans, each counted once when it is issued, whether or not its call succeeds. An ordered table's order column counts its GETs under its own name. A fetch another lookup joins is not counted again, and hedges are counted in `icetray_hedges_total`. A caller budgets a column's GETs, and a Scan's, by this counter. |
| `icetray_value_bytes_read_total` | `table`, `column` | every table and value column | Bytes of the column's values that each completed lookup read: a present value as its page stores it, a default as its bytes, and a null or a miss as zero. A lookup that fails counts nothing; a failed `MultiGet` counts the keys it completed before the failure, as `icetray_lookups_total` does. |
| `icetray_value_bytes_returned_total` | `table`, `column` | every table and value column | Bytes of those values that the same lookups returned: a value read whole counts its bytes as read, a projected value its trimmed bytes. Its ratio to `icetray_value_bytes_read_total` measures what projection saves. |
| `icetray_projection_errors_total` | `table`, `column` | every table and value column | Values that did not parse as protobuf when a lookup requested fields of their column; each fails its call with `FAILED_PRECONDITION`. A rise means the column is declared protobuf and its writer stored values that are not. |
| `icetray_refresh_errors_total` | `table`, `reason` | no: from the first error | Refreshes that failed and left the previous generation serving. icetray counts a failure it remembers for a snapshot once, when it first meets it ([Refresh and reload](#refresh-and-reload)). |
| `icetray_generation_swaps_total` | `table` | every table | Generations a refresh swapped in. The first load is not counted. |
| `icetray_config_reloads_total` | `result` | every result | Reloads that read a changed configuration file. |
| `icetray_scans_total` | `table`, `result` | every ordered table and result | Scans of one key that an ordered table read; each key of a `MultiScan` counts one, and a key listed twice counts once. A request refused before any read is not counted. A call whose response the server refused above `server.max_scan_bytes` counts each key it read as `error`. A `MultiScan` that fails, runs past its deadline, or is canceled counts the keys it finished by their results and each key whose read failed as `error`, as a failed `MultiGet` counts the keys it completed; a failed one first finishes every key before its first failing key in request order. |
| `icetray_lookup_seconds` (histogram) | `table`, `result` | every table and result | Lookup latency. |
| `icetray_object_get_seconds` (histogram) | `kind` | every kind | Object-store GET latency. |
| `icetray_scan_rows` (histogram) | `table` | every ordered table | Rows each answered Scan of one key returned. |
| `icetray_scan_files` (histogram) | `table` | every ordered table | Files holding the key that each answered Scan of one key read: 1 for a key held in one file, as on a table compacted to one file per bucket; more for an edge key that a compaction rolling files by size split across files, and for a key in files that appends overlap. |
| `icetray_scan_seconds` (histogram) | `table`, `temperature` | every ordered table and temperature | Latency of each answered Scan of one key. |
| `icetray_scan_response_bytes` (histogram) | `table` | every ordered table | The encoded size of each `Scan` or `MultiScan` response the server built, one sample per call, a response it refused above `server.max_scan_bytes` included at the size it would have been. A sample in the buckets past `server.max_scan_bytes`, or a rise in `icetray_requests_total{code="failed_precondition"}` of `scan` or `multi_scan`, means callers request more than the bound allows. |

The latency histograms' buckets run from 100 microseconds to 2 seconds,
and `icetray_scan_seconds`' to 10. `icetray_scan_rows`' run from 0 to
10,000 rows and `icetray_scan_files`' from 0 to 64 files.
`icetray_scan_response_bytes`' run from 1 KiB to 256 MiB, with a bound at
2, 4, 8, and 16 MiB.

`icetray_requests_total` counts every call to `Get`, `MultiGet`, `Scan`,
`MultiScan`, and `TableInfo`, including those the gRPC layer fails before icetray's
handler runs: a body that does not decode (`internal`), a message over the
4 MiB limit (`out_of_range`), and an encoding the server does not support
(`unimplemented`). A call dropped before it completes, as when its client
disconnects or its `grpc-timeout` passes, counts as `cancelled`, and so
does a call whose request the server could not finish reading from its
connection, since its client is gone, whatever the gRPC layer answered.
A response whose body ends without a status counts as `unknown`. A call
to a method outside the contract is not counted.

Gauges, written at each scrape:

| Metric | Labels | Value |
|---|---|---|
| `icetray_requests_in_flight` | | `Get`, `MultiGet`, `Scan`, and `MultiScan` calls admitted and not yet ended. |
| `icetray_max_in_flight` | | `server.max_in_flight`. A call that finds every slot held is refused with `RESOURCE_EXHAUSTED`. |
| `icetray_table_state` | `table`, `state` | 1 for the table's current state, 0 for the others. |
| `icetray_table_below_level` | `table` | 1 while the table is below its `lookups` level, else 0. icetray judges the layout on the newest snapshot it measured: the serving one or, while the table is held, the refused one. It judges the kernel's page cache on the serving generation's chunk files. |
| `icetray_table_level` | `table` | The [level](configuration.md#choosing-a-level) the table achieves: the level of that snapshot's layout, which is its most expensive value column's, raised to at least `balanced` while too few of a `disk` table's chunk-file pages are resident in the kernel's page cache (`icetray_table_chunk_resident_fraction`); 0 for `optimal`, 1 for `balanced`, 2 for `permissive`. |
| `icetray_table_column_level` | `table`, `column` | The level a lookup that requests the column achieves on the snapshot `icetray_table_level` measures, with the same kernel page-cache cap, in the same numbers. |
| `icetray_table_declared_level` | `table` | The table's `lookups`, in the same numbers. |
| `icetray_table_chunk_resident_fraction` | `table` | The fraction of a `disk` table's chunk-file pages, in its serving generation, that the kernel's page cache held at the last sample. icetray caps the table's level at `balanced` when the fraction drops below 0.95, and lifts the cap when it rises above 0.99. Absent while icetray cannot observe the residency. |
| `icetray_table_gets_per_hit` | `table` | A key-value table's alone: GETs one cold hit makes at worst on the serving generation's layout and residency, to find the row and read the most expensive value column: that column's value page, and its dictionary page when it is dictionary-encoded, and under `bloom` the key page of every file probed, with a dictionary-encoded key column's dictionary page. For a table with no value columns, only the GETs that find the row. Counted before hedges, unlike `icetray_object_gets_total`. The observed GETs are in `icetray_lookups_total` and `icetray_value_page_gets_total`, where warm hits bring the rate below this prediction. |
| `icetray_table_column_gets_per_hit` | `table`, `column` | A key-value table's alone: the value-page GETs one cold hit spends on the column alone: 1, or 2 when any file's chunk of it is dictionary-encoded. A request's cold cost is `icetray_table_gets_per_miss`, the GETs that find the row, plus this figure for each column it requests. |
| `icetray_table_gets_per_miss` | `table` | A key-value table's alone: GETs one miss makes at worst on the serving generation's layout and residency: 0 under `full` and `disk`, and under `bloom` the key page of every file probed whose Bloom filter does not exclude the key, with a dictionary-encoded key column's dictionary page. Counted before hedges, unlike `icetray_object_gets_total`. |
| `icetray_table_gets_per_scan` | `table` | An ordered table's alone: GETs one cold `Scan` at `max_scan_rows`, of a key held in one file and one row group, makes at worst on the serving generation's layout and residency, reading every configured value column. It counts each column's span and its dictionary page when it is dictionary-encoded, the order column's span and dictionary page under `full` and `bloom`, and under `bloom` the run's first and last key pages with the key column's dictionary page. A key held in several files costs this in each. |
| `icetray_table_files_probed_per_lookup` | `table` | Files one cold lookup, or one `Scan` of an ordered table, probes at worst on the serving generation: the files its key routes to whose exact ranges cover it, 1 where no files overlap. On a table whose files were written under two or more bucketings it is an upper bound. |
| `icetray_table_deleted_rows` | `table` | Rows the serving generation's [deletes](table-layout.md) remove, counting only deletes that apply to a data file it holds, so the dangling deletes a compaction leaves behind are excluded. |
| `icetray_table_deleted_fraction` | `table` | Those rows over the rows of the serving snapshot's data files, so one threshold fits tables of any size. |
| `icetray_table_delete_files` | `table` | Live position delete files and deletion vectors whose rows the serving generation applies. A position delete file that a deletion vector masks is not counted. |
| `icetray_table_delete_state_bytes` | `table` | Bytes the serving generation's delete state holds: each data file's deleted rows and the deletes they came from, and what the deletes keep for the next refresh. Also counted in `icetray_index_bytes`. |
| `icetray_table_oldest_delete_age_seconds` | `table` | Seconds since the commit of the snapshot that committed the oldest applying delete, which icetray finds by the delete's data sequence number; 0 with no applying delete. It grows while compaction stalls. When that snapshot has expired, icetray dates the delete by the earliest listed snapshot with a greater sequence number, which understates the age, and a compaction that keeps its starting sequence number dates a delete to an older snapshot, which overstates it. icetray passes over a snapshot whose timestamp cannot be read, and the gauge reads 0 when no listed snapshot dates the delete. |
| `icetray_generation_age_seconds` | `table` | Seconds since the serving snapshot was committed, by its timestamp. For a table written once a day it reaches up to a day between writes; it measures the age of the published data, which differs from replication lag. |
| `icetray_generation_sequence_number` | `table` | The serving snapshot's sequence number. |
| `icetray_generation_swapped_at_seconds` | `table` | When the serving generation was swapped in, in Unix seconds. |
| `icetray_findings` | `table`, `code`, and `column` for a code about one value column | The number of findings of each code that icetray raised on the serving generation, 0 for a code not raised. A code about one value column has one series per configured column. |
| `icetray_index_bytes` | `table` | The resident index's live bytes. |
| `icetray_page_cache_bytes` | | Bytes the page cache's memory level holds after evictions, against its part of `server.cache_bytes`. |
| `icetray_row_cache_bytes` | | Bytes the row cache holds after evictions: each entry's value at its allocation size plus the per-entry overhead. It stays about a sixteenth below the row cache's part of the budget, which is reserved for the keys of rows evicted recently, so a row read again soon after its eviction is kept longer. |
| `icetray_cache_budget_shortfall_bytes` | | Bytes by which the cache budget, with the resident indexes of the tables served and the largest of them once more, and a third of that sum, overruns the memory limit; 0 when they fit, and when no limit can be read. icetray measures it once the tables first serve and after every reload that applies, not on a refresh. A value above 0 means the limit is too low for the budget or the tables ([Memory](#memory)). |
| `icetray_config_pending` | | 1 while the configuration file differs from the configuration applied, else 0. |
| `icetray_process_rss_bytes` | | The process's resident set size. |
| `icetray_allocator_allocated_bytes`, `_active_bytes`, `_metadata_bytes`, `_resident_bytes`, `_retained_bytes` | | jemalloc's own counts, which split the resident set. `allocated` is live data; `active` minus `allocated` is free space inside pages in use; `resident` minus `active` and `metadata` is freed memory not yet returned to the operating system; `retained` is address space only. |

icetray exposes the level gauges, `icetray_table_column_level` included,
from the table's first swap, as it does the generation gauges. icetray
measures a snapshot before it refuses the snapshot as `below_level`, so
the gauges take that snapshot's level and each of its columns' levels; a
snapshot refused for any other reason leaves them unchanged. A reload
that changes `lookups` updates `icetray_table_declared_level` at once.

Once the tables serve, and every 15 seconds after that, icetray samples
which pages of every `disk` table's chunk files the kernel's page cache
holds. A sample maps each chunk file, without reading it, for the
duration of one `mincore` call. Residency in the kernel's page cache is a
property of the replica, which a new snapshot cannot change, so `strict`
never refuses or holds a snapshot for it. When icetray cannot observe a
chunk file's residency, the kernel's page cache caps no level,
`icetray_table_chunk_resident_fraction` is absent, and icetray logs a
warning, and logs another when it can observe the residency again. On
Linux, icetray samples a chunk file that the server's user neither owns
nor may write only when it runs as root: for such a file, Linux reports
every page resident, which would hide a shortfall.

The label values:

- `call`: `get`, `multi_get`, `scan`, `multi_scan`, `table_info`.
- `code`: the gRPC status in lower snake case, every one of the 17 gRPC
  codes. icetray's handlers return `ok`, `invalid_argument`,
  `failed_precondition`, `deadline_exceeded`, `not_found`,
  `resource_exhausted`, `unavailable`, and `internal`; the gRPC layer adds
  `cancelled`, `out_of_range`, `unimplemented`, and `unknown`, as above.
- `kind`: `load` (footers, key chunks, order chunks, and metadata read
  while a generation is built) and `lookup` (value pages, a `Scan`'s span
  GETs, and key pages under `bloom` residency).
- `result` of `icetray_lookups_total` and `icetray_lookup_seconds`:
  `cold_hit` (a value or dictionary page of a requested column came from
  object storage, including through a fetch that another lookup started),
  `warm_hit` (otherwise: the page cache served every page, icetray
  returned a column's default with no page read, or the lookup requested
  no column), `miss` (the key is absent).
- `result` of `icetray_config_reloads_total`: `applied`, `refused`.
- `result` of `icetray_scans_total`: `rows` (the Scan returned at least
  one row), `empty` (it returned none), `error` (its read failed, or its
  call's response was above `server.max_scan_bytes` and refused).
- `temperature`: `cold` (an order or value page of the Scan came from
  object storage, including through a fetch another call started),
  `warm` (otherwise).
- `cache`: `lookup` (the value-page cache key-value lookups read
  through), `scan` (the memory cache of ordered tables' pages, which no
  key-value table reads).
- `state`: `loading`, `serving`, `degraded`, `held` ([Refresh and
  reload](#refresh-and-reload)).
- `code` of `icetray_findings`: `dictionary_encoded_value`,
  `large_key_pages`, `no_bucket_partition`, `overlapping_files`,
  `bounds_missing`, `column_index_truncated`, `large_value_pages`,
  `value_pages_over_ceiling`, `long_keys`, `large_file`,
  `many_files_per_bucket`, `no_bloom_filter`, `bloom_oversized`,
  `bloom_undersized`, `deletes_pending`, `column_default`, and the codes
  of [ordered tables](#ordered-tables), `shared_edge_keys`,
  `dictionary_encoded_order`, `long_runs`, and `large_runs`. icetray
  publishes every load-time code for every table, so a key-value table
  carries a series of 0 for each of the four ordered codes.
  `dictionary_encoded_value`, `large_value_pages`,
  `value_pages_over_ceiling`, and `column_default` are about one value
  column and carry the `column` label; the other codes carry none.
  `column_default` is one finding per column, so
  `icetray_findings{code="column_default",column="..."}` reads 1 however
  many data files predate the column and are read with its default; the
  file count is in the finding's message and in the doctor's
  `files_predating_column`. The doctor's `malformed_protobuf` has no
  series: a load reads no values, so only the `doctor` command raises it.
- `column`: a configured value column's name, or for
  `icetray_value_page_gets_total` an ordered table's order column's.
- `reason` of `icetray_refresh_errors_total`: a refusal reason
  (`unsorted`, `null_keys`, `duplicate_keys`, `duplicate_rows`,
  `null_order`, `key_outside_bucket`, `key_type_mismatch`,
  `key_type_unsupported`, `order_type_unsupported`,
  `order_type_mismatch`, `column_mismatch`, `value_type_unsupported`,
  `data_format_unsupported`, `equality_deletes`,
  `delete_kind_unsupported`, `key_too_long`, `over_budget`,
  `no_bloom_filter`, `below_level`), or an
  error kind (`iceberg`, `parquet`, `object_store`, `object_store_path`,
  `json`, `config`, `location`, `cache`, `key_type`, `table`, `file`,
  `not_ready`, `local`, `catalog_timeout`, `catalog`, `load_deadline`,
  `refresh_deadline`, `verify_canceled`, `lookup_canceled`). There are
  too many to expose for every table at start, so icetray creates each
  series at its first error; alert on `icetray_table_state` instead
  ([What to alert on](#what-to-alert-on)). A table whose source still
  names a refused snapshot counts the failure once, so its rate falls to
  zero while the table stays degraded or held.

A table that a reload adds gets its series when the reload adds it. A
table that a reload removes loses its gauges at the next scrape, and
keeps its counters and histograms until the process restarts. The
snapshot id is in the swap's log line and in `TableInfo` and in no
metric, since as a gauge it would lose precision and as a label it would
add a series with every swap.

## Logs

`serve` logs JSON on stdout, one object per line. The standard fields are
`time` (RFC 3339, UTC), `level`, `message`, and `target`, and each event
adds its own, such as `table`, `snapshot_id`, or `error`.
`server.log.fields` renames the standard fields, and
`server.log.static_fields` adds constant fields to every event
([Configuration](configuration.md#serverlog)). An invalid mapping fails
validation with the rest of the configuration, and `serve` exits 1.

With `RUST_LOG` unset, the filter passes icetray's own events at `info`
for `serve` and at `warn` for a one-shot, and every other crate's at
`warn`. The exception is the AWS SDK's profile normalizer
(`aws_runtime::env_config::normalize`), limited to `error` because it
warns about every profile it cannot use. The SDK's warning that a file
named by `AWS_CONFIG_FILE` or `AWS_SHARED_CREDENTIALS_FILE` does not
exist still appears. A valid `RUST_LOG`, such as `debug` or
`icetray_core=debug`, replaces the default filter entirely; icetray
treats an empty, blank, or invalid one as unset.

Events worth knowing:

- `open-file limit`, at start, with the `limit` in force, and a warning
  with `limit` and `needed` when that limit is too low
  ([File descriptors](#file-descriptors)).
- `cache budget`, at start, with `cache_bytes`, the whole budget, split
  into `row_bytes` for the row cache and `page_bytes` for the page
  cache's memory level, and the memory `limit` with its `limit_source`
  (`cgroup` or `host`), both absent when no limit can be read; and again
  after a reload that changes the split
  ([Refresh and reload](#refresh-and-reload)). The
  budget is derived from the limit unless `server.cache_bytes` sets it
  ([Configuration](configuration.md#server)).
- A warning, `the cache budget and the resident indexes of the tables
  served need more memory than the limit allows`, with `needed`, `limit`,
  `limit_source` (`cgroup` or `host`), and `missing_bytes`, once the
  tables first serve and after a reload that applies, while
  `icetray_cache_budget_shortfall_bytes` is above 0
  ([Memory](#memory)).
- `listening`, with both addresses and the table count; then
  `table serving` for each table as its first load completes.
- `swapped in a generation`, with the snapshot id and the files reused,
  on every load and refresh.
- A finding on a load, with `code`, `caps` (absent for advice), and
  `file`: at `warn` for a finding whose cap is below the table's
  `lookups` level, and at `info` for the rest, advice included.
- `configuration reloaded`, at `info`, with the file's `digest`, the
  tables `added`, `changed`, `retuned`, and `removed` by name, and the
  count `kept`.
- `configuration reload refused`, at `warn`, with the `reason`, after a
  `table refused for a reload` for each refused table
  ([Refresh and reload](#refresh-and-reload)).
- `draining`, when a signal starts the drain.
- `run failed`, at `error`, with the `error` that ended the run.
- `jemalloc background purging is unavailable`, a warning on Linux only.

The one-shots log the same JSON to stderr at `warn`, so that their stdout
carries only their output, and they report their final failure as plain
text, `error: <message>` with an optional `  hint: <fix>` line. A write
to a closed stderr is dropped, and the exit code is unchanged. The doctor
writes its findings as its output and does not log them; `info` and
`get` log at `warn` the findings that put a table below its `lookups`
level.

## The local directory

`storage.local.dir` sets an optional local directory. It holds:

- The chunk files of tables with `residency = "disk"`: each data file's
  key chunks, and for an ordered table its order chunks too, while its
  footer, page indexes, and fence array, and an ordered table's order
  fences and order facts, stay in memory. A key-value entry and an
  ordered entry over the same data file, or two ordered entries
  with different order columns, each keep a chunk file of their own,
  named by the rules it was verified under.
- The disk level of the value-page cache, bounded by
  `storage.local.page_cache_disk_bytes`. Zero turns the level off; any
  other value must be at least one 4 MiB block.

`index_budget_bytes` bounds the chunk files of every table together, and
icetray checks it when a load begins and, for an ordered table with a
`string` or `binary` order column, again as it writes each chunk file
([Configuration](configuration.md#storagelocal)). icetray keeps the serving
generation's chunk files until the new generation has swapped in, so a
table rewritten wholesale needs about twice its steady footprint, which
the doctor reports as `predicted.disk_bytes`. A load that writes a chunk
file over one already on disk counts that file's bytes twice until the
load ends, so a load can be refused as `over_budget` when the budget is
within about one chunk file of full; the chunk files never exceed the
budget.

A lookup in a `disk` table costs one local read and, on a cold hit, one
GET per value column it requests. The local read is a memory copy while
the kernel's page cache holds the chunk file, and a device read when it
does not, so a `disk` table serves at memory speed only when that cache
has room for its chunk files ([Memory](#memory)). On Linux, put the
directory on a mounted volume with a filesystem such as ext4; on a
container's overlay filesystem, icetray sends every local read to the
blocking pool.

Everything in the directory is a copy of bytes in object storage, which
icetray checks against its data file's identity and rebuilds on demand. A restarted replica reuses every chunk file whose data file is still
referenced and deletes the rest, and it keeps the page cache's disk level when a release of the same
minor version wrote it. A chunk file of another format, or a disk level
another minor release wrote, is discarded and rebuilt, so a minor or
major upgrade starts the disk level cold and a patch release keeps it. Each
replica has its own directory, which needs only capacity and no backup;
losing it costs one cold load.

The disk level's `cache.format` marker is written whole or not at all, and
after the emptying it vouches for is on disk, so a crash during an upgrade
leaves either the old marker over the old files or no marker, which empties
the level again at the next start. One case the marker cannot see: rolling
back to a build from before the marker existed, which writes its own cache
files under the newer build's marker; rolling forward again trusts them. That
is harmless only when the two builds store the level in the same layout,
and it ends once every deployed build writes the marker. Delete the
directory's `cache/` and `cache.format` when a rollback crosses a release that
changed the level's layout.

The trade-offs between `full`, `disk`, and `bloom` are in
[Choosing a residency](configuration.md#choosing-a-residency).

## File descriptors

A replica holds these open at once:

- one chunk file per data file of each `disk` table, for the life of its
  generation, and twice that while a refresh that rewrote the table loads
  beside the serving generation;
- one descriptor per 4 MiB block of the page cache's disk level, for the
  life of the process: `page_cache_disk_bytes` / 4 MiB, 2,048 for 8 GiB;
- up to 128 reads per worker thread, at most 1,024, for each table that is
  loading, each read a local file or an S3 socket. Every configured table
  loads at once at start, so at start icetray holds that many for every
  table;
- client connections, the object store's and the catalog's connection
  pools, the standard streams, and the one chunk file retention opens at a
  time.

Size the limit to at least twice the data files of `disk` tables, plus
`page_cache_disk_bytes` / 4 MiB, plus one load's reads (1,024 on a machine
of eight or more worker threads) for each configured table, plus headroom
for the connections. At start icetray raises its soft limit to the hard
limit, so set the hard limit: `ulimit -Hn`, systemd's `LimitNOFILE`, or
the container runtime's `nofile` limit. Kubernetes has no per-pod
setting, so the container runtime's or the node's limit applies.

`serve` logs the limit in force at start. It warns, with `limit` and
`needed`, when the limit is below what the disk level and a load of every
configured table need (those terms plus 256). The warning leaves out the
chunk files of `disk` tables, which icetray cannot count until their
tables load. icetray opens the page cache before any table loads, so a
limit too low for the disk level alone (its blocks plus 256) fails at
that step, with exit 1:

```
page cache: open-file limit 1500 is below the 2304 descriptors the disk level needs: Too many open files (os error 24)
```

## Refresh and reload

icetray re-resolves each table's ref every `server.poll_interval`; what a
poll reads depends on the table's source
([Configuration](configuration.md#tablessource)).

- A poll that finds the metadata unchanged costs one read or one catalog
  call.
- When a poll finds a new snapshot, icetray builds its generation beside
  the current one, loading only the data files the previous generation
  did not hold. For every file it keeps, icetray checks the file's columns
  against the new snapshot's schema, which reads nothing
  ([RFD 023](rfd/023-column-resolution.md)). It then swaps the new
  generation in, and `icetray_generation_swaps_total` counts the swap.
- When a refresh fails, the last good generation keeps serving, and
  icetray counts the failure in `icetray_refresh_errors_total`, logs
  `refresh failed` at `warn`, and moves the table to degraded, or to held
  for a `below_level` refusal. Readiness does not change.
- When a strict table's new snapshot is below its `lookups` level,
  icetray refuses it as `below_level` and keeps serving the last snapshot
  that met the level. The table's state is held, which marks a refusal
  the writer's layout caused; degraded marks a fault.
  `icetray_table_state{state="held"}` reads 1, and so does
  `icetray_table_below_level`, which measures the refused snapshot. The
  table leaves held at the next swap, as when a compaction commits a
  snapshot that meets the level, or at a poll that finds the ref naming
  the serving snapshot again. A reload that relaxes `lookups` to a level
  the refused snapshot meets, or turns `strict` off, also ends held, and
  the next poll swaps that snapshot in. A transient poll failure while
  the table is held makes it degraded until a poll finds the source
  unchanged, which makes it held again.
- icetray remembers a failure of the snapshot itself, which is one of
  three kinds. The first is any refusal reason, `below_level` included,
  except `over_budget` against `storage.local.index_budget_bytes`. The
  second is a `table` fault in the snapshot's metadata: a ref, schema,
  partition spec, or column it lacks, zero buckets, a data file with no
  usable bucket value or one out of range, a manifest entry with no data
  sequence number of its own and none to inherit, a deletion vector entry
  that does not place its blob in its Puffin file, or a snapshot
  timestamp outside the range icetray can represent. The third is a
  `file` fault that a data file's footer or Bloom filters show, or that a
  delete file shows against the manifests
  ([RFD 021](rfd/021-merge-on-read.md)): a deletion vector its Puffin footer
  does not list where its entry places it, that the footer places before
  the end of the file's opening magic or past the footer's start, that is
  compressed, that names another data file or another row count than its
  entry, or that passes its checksum but is not a portable 64-bit roaring
  bitmap filling its blob; two manifest entries of one Puffin file that
  record different file sizes; two deletion vectors that apply to one data
  file; a position delete file without the `file_path` and `pos` columns,
  with a null in either, with a row naming another data file than its
  entry, or with another row count than its entry; a negative position, or
  one at or past its data file's row count; or a data file with deletes
  whose manifest entry records another row count than its footer. These
  depend only on the snapshot and the table's entry, so a retry fails the
  same way. icetray counts and logs the failure once, when it first meets
  it, and does not build that snapshot again while the table's source
  names the same metadata location: each poll costs only the one read or
  catalog call above and is neither counted nor logged, and the table
  stays degraded or held. When the source names another metadata
  location, icetray builds its snapshot. Once the table serves that
  snapshot, or refuses it in turn, icetray logs `source moved past a
  refused snapshot` at `info` with the `reason` and, when the ref named
  one, the refused `snapshot_id`. A reload that changes a setting of the
  table's entry other than `lookups`, `strict`, column formats, and
  `max_scan_rows` loads the table anew under the new entry, the refused
  snapshot included, so fixing `ref` or a column name or raising
  `resident_budget_bytes` takes effect without waiting for a new
  snapshot; if icetray still refuses the snapshot, it refuses the
  reload. A reload that changes only `lookups`, `strict`, the formats of
  the table's value columns, or an ordered table's `max_scan_rows`
  retunes the table in place and loads nothing. The retune forgets every
  remembered refusal, whatever its reason, and icetray logs `retune
  forgot a refused snapshot` at `info`, so the next poll builds the
  snapshot the ref names under the new entry. icetray then refuses once
  more a snapshot it had refused for a reason other than its level, and
  remembers it again.
- icetray retries every other failure on every poll, and counts and logs
  it each time: a store, catalog, cache, or local-directory error; a
  timeout or deadline; `over_budget` against the shared
  `index_budget_bytes`, which other tables' chunk files also fill; a
  version hint or catalog record that names no usable metadata; a
  `parquet` or `iceberg` library error; a `file` fault found while
  decoding a data file's pages, which a `disk` table may have read from
  its local chunk file instead of the data file, such as a string key or
  order value that is not UTF-8, or a row group whose order fences
  together pass the 4 GiB their offsets address; and a delete file whose
  framing is invalid: a Puffin footer whose magics or length are wrong or
  whose payload is not its JSON, a deletion vector blob whose length,
  magic, or checksum is wrong, or a position delete file's page header
  that overstates its page. For these last two, icetray cannot tell a
  damaged read from a damaged file.
- When the catalog refuses or fails a `glue` or `s3tables` call, for a
  missing permission, a table that does not exist, throttling, or an
  unreachable endpoint, icetray logs a `refresh failed` event at `warn`
  with `reason` `catalog` and an `error` that names the table as the
  catalog does (`db.kv`, or `ns.kv in <table bucket ARN>`) and carries the
  service's error code. A call that runs past `storage.catalog_timeout`
  has `reason` `catalog_timeout`. icetray counts each under its reason in
  `icetray_refresh_errors_total`. A Glue table that is not Iceberg, or a
  table without a metadata location, has `reason` `table`. The same
  failure on a first load ends `serve` with `run failed` and exit 1, and
  during a reload refuses the reload.
- Snapshot expiry removes snapshots by age, and a replica still serving
  an expired snapshot reads data files that are gone. Keep the retention
  on the served ref longer than the largest
  `icetray_generation_age_seconds` a replica can reach while a newer
  snapshot exists, as when its table is degraded. Expiry never removes
  the snapshot a ref currently names.

icetray rereads the configuration file on the same interval, and
immediately on SIGHUP, and applies a change to its tables without a
restart ([RFD 018](rfd/018-configuration-reload.md)).

- A table whose entry is unchanged keeps serving untouched. An entry
  that spells out the default `lookups` equals one that omits it, and an
  entry whose `value_columns` lists the same columns in another order
  equals the original. A change to the set of value columns changes the
  entry, and icetray loads the table anew.
- icetray retunes in place a table whose entry changed only in `lookups`,
  `strict`, the formats of its value columns, or an ordered table's
  `max_scan_rows`: its generation keeps serving and icetray loads
  nothing, since it judges the level from the layout the generation
  already holds, and a lookup reads each column's format, and a `Scan`
  its limit, from the entry. icetray accepts or refuses a field list by
  the new format from the next call on. icetray refuses the reload when a
  strict table's serving snapshot is below its new level, as it would
  refuse to start with that file, and when the reload declares a protobuf
  format on a column whose type in the serving snapshot is not `binary`.
- icetray loads added and changed tables beside the serving tables. Once
  every one of them serves, icetray swaps the whole set at once, and the
  retuned tables take their new entries in the same swap. A request for a
  removed table then returns `NOT_FOUND`. icetray logs the outcome as
  `configuration reloaded` ([Logs](#logs)).
- When the swap changes whether any table served reads through the row
  cache, icetray resizes the row cache and the page cache's memory level
  within the same `server.cache_bytes`: the last such table gone, the
  page cache holds the whole budget and the row cache nothing; the first
  one added, the row cache takes `server.row_cache_share` of it, and the
  page cache evicts down to the rest, writing what it evicts to the disk
  level when there is one. icetray logs the new split as `cache budget`.
  A file that mixes the two kinds of table with no page-cache disk level
  and no `row_cache_share` fails validation, so such a reload is refused
  ([Configuration](configuration.md#server)). icetray also refuses a
  reload whose new split leaves the page cache a part under the minimum
  for its shards on this machine, which a start with that file would
  refuse.
- icetray refuses a reload whole when any of those tables fails to load
  or a retuned table is refused, when the file does not parse or fails
  validation (a file that lists no table fails it), when the caches'
  split for its tables is one a start with it would refuse, or when any
  setting outside `[[tables]]` changed, since those apply only on
  restart. icetray logs the refusal as `configuration reload refused`
  and counts it, and `icetray_config_pending` reads 1 until the file
  matches the applied configuration. The refusal's `reason` names what
  to fix:
  - for a file that does not parse or fails validation, the error, naming
    the file and the setting by its full path;
  - for a restart-only change, the setting, as in
    `server.listen changed, which only a restart applies`;
  - for a split a start would refuse, the page cache part and the
    minimum for the machine's shards, as the start's refusal names them;
  - for refused tables, their names, as in `tables refused: kv`. icetray
    logs each table's own error just before, as `table refused for a
    reload` with `table`, `reason`, and `error`: a load's failure, or
    `below_level` for a retuned table.

  The server keeps serving. The next poll retries once the file changes,
  and SIGHUP retries immediately.
- Readiness does not change on a reload.
- icetray never sees a change to a single-file mount fixed at start, such
  as a Kubernetes `subPath`.
- Replace the file atomically, by a rename. A read during an in-place
  write can see a truncated file that still parses.

## Ordered tables

An [ordered table](table-layout.md#ordered-tables) serves `Scan` and
`MultiScan` beside the key-value tables of the same process. It shares
`server.max_in_flight`, `server.batch_fan_out`, the process's GET
throughput, and its resident memory with them, and reads its pages
through [the page cache](performance.md#the-page-cache-and-scans) every
table shares.

**The page cache.** A `Scan` takes each page from the page cache's
memory, then from its disk level when the local directory has one, and
fetches only the runs of pages neither holds. Its hits and fetches count
in `icetray_page_cache_hits_total` and `icetray_page_cache_fetches_total`
beside every lookup's, and a page read from the disk level counts in
`icetray_local_reads_total`. A long `Scan` can push another table's pages
off the disk level, though not its rows out of the row cache.

**Levels.** A table whose writer appends between compactions is
`permissive` under `overlapping_files`, so its owner configures
`lookups = "permissive"`; left at the default, icetray reports such a
table below its level after every append. `icetray_scan_files` shows how
many files each `Scan` reads, and its rise between compactions measures
what the appends cost. The levels an ordered table reaches, and the
findings that cap them, are in [Levels of an ordered
table](table-layout.md#levels-of-an-ordered-table) and [Requirements of
an ordered table](table-layout.md#requirements-of-an-ordered-table).

**Refresh.** A refresh merges a file only with the files that can share
a live pair of key and order value with it, and reads only the order
pages that can hold one ([RFD 025](rfd/025-ordered-rows.md), Refresh).
A refresh that appends later rows therefore reads only its new files;
one that adds files a compaction rolled by size reads the pages at the
edge keys they share; and an append of rows inside an existing file's
order range reads the order pages those rows fall among, in the new file
and the old. With `RUST_LOG=icetray_core=debug`, each load logs
`assembled generation` with `overlap_groups_verified`, the overlap
groups it merged, which on an ordered table counts the groups merged by
key and order value. An ordered table's load also logs `checked ordered
rows across files` with `merges`, the same count; `compared`, the
comparisons the check made, of files' ranges and buckets, of pages'
pairs, and of pairs in the merge; and `held_bytes`, the most bytes of
chosen pages the load's merges held and had in flight at once, which
leaves out the key chunks `full` keeps resident. The overlap groups merge
at once and share one read-ahead bound, so a load holds at most 64 MiB
beyond one row group's chosen pages per file. Every group's files read
their first row group at once, so the most a load holds follows the
chosen pages of all its files, not of one group's.

## Deploying

Every deployment runs the same icetray build and configures it only
through settings, mounted files, and environment variables.

- Each release publishes Linux binaries for x86-64 and arm64 and a
  container image, `ghcr.io/<owner>/icetray:<version>`, for both: the
  release binary on Ubuntu 24.04, running as the image's `ubuntu` user,
  uid 1000, with `icetray serve` as its command. The image also includes
  `doctor`, `get`, `scan`, and `info`. In the image's name, `<owner>` is the
  GitHub owner of the repository that publishes the release, in lower
  case, and `<version>` is the release's tag without its `v`, such as
  `0.2.0`. The tag `0.2` follows the newest `0.2.x` release, and from 1.0
  on, `1` follows the newest `1.x`. There is no `latest` tag: below 1.0 a
  minor release may break a deployment.
- icetray reads the configuration path from `ICETRAY_CONFIG`, so a pod
  passes no flags. icetray reads TOML and JSON into the same schema. A
  container's configuration sets `listen` and `metrics_listen` to
  addresses its pod network reaches ([Configuration](configuration.md#server)).
- icetray resolves S3 credentials once from the AWS SDK's default chain
  (environment variables, profiles and single sign-on, credential
  helpers, web identity, and container or instance roles, pod identity
  among them), and caches them until shortly before they expire. It
  loads the chain only when a location is `s3://` or `s3a://`
  ([Configuration](configuration.md#storages3)). Static keys in
  `storage.s3` take precedence over the chain and are the path for
  S3-compatible endpoints, with `endpoint`, `allow_http`, and
  `path_style`. icetray has no secret format of its own.
- icetray signs calls to Glue and S3 Tables with the same cached
  credentials as its S3 requests, so it does not run the chain again for
  them. It calls a REST catalog without credentials
  ([Configuration](configuration.md#tablessource)).
- The credentials need `s3:GetObject` on every location a table reads:
  its metadata files, manifest lists, manifests, and data files. A `glue`
  source also needs `glue:GetTable` on the catalog, the database, and the
  table; for a table in another account's catalog, that catalog's resource
  policy must grant it as well. An `s3tables` source needs
  `s3tables:GetTableMetadataLocation` on the table, and icetray reads its
  files under `s3tables:GetTableData` on the table, which S3 Tables checks
  for reads of a table bucket's objects. icetray does not use credentials
  that a catalog or Lake Formation vends, so the role's own permissions
  must allow every read.

### Memory

- Run each replica under a container memory limit, and set the limit
  first: the cache budget follows it, half of it unless
  `server.cache_bytes` sets it ([Configuration](configuration.md#server)).
  The limit has to hold the resident indexes of the tables the replica
  serves, which are the sum, over its tables, of the doctor's `predicted`
  bytes for each table's residency (`predicted.full`,
  `predicted.disk_memory`, or `predicted.bloom`); the largest table's
  once more, since a refresh that rewrites it wholesale loads its next
  generation beside it; the cache budget; and a third of that total for
  the process, requests in flight, a load's transient chunks, and the
  allocator's fragmentation and bookkeeping. With the default budget the
  condition reads as a rule of thumb: the limit is at least four times
  the indexes and the largest table's once more. With `cache_bytes` set,
  it may be at most three quarters of the limit less those indexes. The
  `icetray_allocator_*` gauges show how much of the resident set the
  allocator accounts for. To judge whether the budget suits a table,
  compare the doctor's `value_columns[].value_bytes`, each column
  chunk's uncompressed bytes in the snapshot with its page headers and a
  dictionary page's compressed extent, with the row cache's share of
  `cache_bytes`, which every table the server serves shares: a table
  whose hot rows are a small share of those bytes fits a small budget,
  and one that is read evenly wants the budget near their sum.
- Once the tables first serve, and after every reload that applies,
  icetray applies that rule to itself: it adds each table's resident
  index bytes (`icetray_index_bytes`), the largest once more, and the
  cache budget, adds a third of the sum, and compares it with the memory
  limit, which is the smaller of the container's cgroup limit and the
  host's memory. When the limit is lower, icetray logs a warning with
  `needed`, `limit`, `limit_source` (`cgroup` or `host`), and
  `missing_bytes`, and sets `icetray_cache_budget_shortfall_bytes` to the
  bytes missing; the gauge reads 0 when the budget fits, and when no
  limit can be read. A refresh does not measure it again, so an index
  that grows with its table's snapshots shows in the gauge at the next
  applied reload or restart. It is a warning and not a refusal, since it
  depends on the replica's memory, which the doctor does not see. Raise
  the limit, or lower `server.cache_bytes`; a reload measures it again.
- `server.max_scan_bytes` bounds the response a `Scan` or `MultiScan`
  sends, not what the call holds while it reads. Each file that holds
  the key reads up to the limit's rows of every requested column before
  the merge cuts them back to the limit, so one call holds about the
  files that hold the key times its limit times its requested columns in
  values, and reads all of them before it refuses an oversized response.
  A `MultiScan` holds that for each key it is reading, up to
  `server.batch_fan_out` keys at once in each of its runs (at most one
  run per worker thread), and the merged rows of every key until it
  responds. Up to `server.max_in_flight` such calls run at once. The
  doctor's `large_runs` advice names each file on which a `Scan` at the
  full limit would be refused. The owner bounds what a call holds with
  the table's `max_scan_rows` ([Configuration](configuration.md#tables)),
  and with compaction, which lowers the files that hold a key.
- For `disk` tables, raise the limit by their chunk files' size, the
  doctor's `predicted.disk_bytes`. On Linux, the kernel charges the page
  cache of the files a container reads to the container's memory limit,
  so a limit sized for the process alone leaves no room to keep the chunk
  files cached: their reads go to the device, and each lookup waits on
  it. The kernel reclaims this cache under pressure, so a scheduler's
  reservation, such as a Kubernetes memory request, can stay at the sum
  above.
- Once the tables serve, and after a reload that adds a `disk` table,
  icetray compares its memory limit, less its resident set, with what the
  kernel's page cache needs to hold: every `disk` table's chunk files,
  each file counted once however many tables read it, plus
  `storage.local.page_cache_disk_bytes`, whose files the kernel caches
  too, plus the part of the cache budget the caches have not yet filled,
  since the resident set holds only what they have filled so far. The
  limit is the smaller of the container's cgroup limit, read
  from cgroup v2's `memory.max` or v1's `memory.limit_in_bytes`, and the
  host's memory. On a shortfall icetray logs a warning with `needed`,
  `unfilled_budget_bytes` (the part of `needed` that is the unfilled
  budget), `limit`, `limit_source` (`cgroup` or `host`), `rss`, and
  `missing_bytes`. The resident set right after loads can include memory
  the allocator has freed and not yet returned to the operating system,
  so the warning can overstate what is missing;
  `icetray_table_chunk_resident_fraction` shows whether the chunk files
  stay cached.
- A load refuses a key page header, and a lookup a value page header,
  that declares more than its page can hold or more than a 64 MiB
  ceiling, and icetray refuses a file whose decoding panics inside
  parquet. Inside a delta-encoded page, parquet still sizes buffers from
  lengths the file declares; there the memory limit is the backstop, and
  a file that exceeds it gets every replica that loads it OOM-killed.

### CPU

A load is CPU-bound: once enough reads are in flight, it spends its time
decoding and verifying key chunks, and for `disk` tables writing chunk
files. icetray runs one tokio worker thread per core the process sees,
which under a CPU limit follows the limit in whole cores, and
`TOKIO_WORKER_THREADS` overrides the count. The worker count sets the
page cache's shard count and a load's concurrency (128 reads per worker,
at most 1,024), so load time and the lookup rate both scale with it.

- As a starting point, plan about one core per few thousand lookups a
  second, then measure under the real key distribution. Warm hits on
  small values cost far less; cold hits over TLS to S3 and large values to
  decompress cost more.
- Avoid a tight CPU limit. A first load or a wholesale refresh uses every
  core it is given, and throttling stretches it toward
  `server.load_deadline` or `server.refresh_deadline`. Set the CPU request
  to the serving load and the limit well above it, or leave the limit
  off.

### The local volume

- A local volume is optional. Mount one for `disk` tables or to keep the
  page cache's disk level across restarts and patch releases.
- Put it on instance-store NVMe, such as AWS's `d`-suffixed instance
  types or `i4i`. The directory is disposable: icetray rebuilds its chunk
  files from object storage, so losing an instance store's data with its
  instance costs one cold load, whose GETs re-fetch the key chunks. Its
  IOPS come with the instance, and its device latency sets the cost of a
  lookup whose key page misses the kernel's page cache (measured on EBS
  in the [page-cache residency analysis](analysis/2026-10-07-page-cache-residency.md)).
- Format it ext4 or xfs. Both support `RWF_NOWAIT`, which lets icetray
  run a `disk` read that the kernel's page cache holds on the calling
  worker, without handing it to the blocking pool.
- It must be writable by uid 1000.
- In Kubernetes an `emptyDir` on the node's disk is enough, since nothing
  in it needs to outlive the pod. On EKS, configure the node group to put
  ephemeral storage, which backs `emptyDir`, on the instance-store NVMe.
  Avoid `medium: Memory`, which counts against the memory limit.

## Kubernetes

The pod spec below follows the advice above. Its image is the
placeholder that [Deploying](#deploying) explains, and its resource sizes
are placeholders too; the paragraphs after the spec say how to choose
them.

```yaml
spec:
  terminationGracePeriodSeconds: 45
  securityContext:
    runAsNonRoot: true
    runAsUser: 1000
    runAsGroup: 1000
    fsGroup: 1000
  containers:
    - name: icetray
      image: ghcr.io/<owner>/icetray:<version>
      env:
        - name: ICETRAY_CONFIG
          value: /etc/icetray/icetray.toml
      ports:
        - name: grpc
          containerPort: 7433
        - name: metrics
          containerPort: 9433
      startupProbe:
        httpGet:
          path: /ready
          port: metrics
        periodSeconds: 10
        failureThreshold: 190
      readinessProbe:
        httpGet:
          path: /ready
          port: metrics
        periodSeconds: 5
      livenessProbe:
        tcpSocket:
          port: metrics
        periodSeconds: 10
        failureThreshold: 3
      resources:
        requests:
          cpu: "4"
          memory: 6Gi
        limits:
          memory: 8Gi
      volumeMounts:
        - name: config
          mountPath: /etc/icetray
        - name: local
          mountPath: /var/lib/icetray
  volumes:
    - name: config
      configMap:
        name: icetray
    - name: local
      emptyDir:
        sizeLimit: 20Gi
```

**Probes.** `/ready` returns 503 until every table serves its first
generation, and 200 from then on, through refresh failures and reloads,
until a drain closes the metrics listener.

- The startup probe sets the first load's time budget. Make
  `failureThreshold` times `periodSeconds` longer than
  `server.load_deadline` plus the ten seconds that health stays
  `NOT_SERVING` after a failed start, so that a load that cannot finish
  ends with icetray's own error in the log before the kubelet kills the
  container. The example allows 1,900 seconds, which covers a
  `server.load_deadline` of 30 minutes.
- The readiness probe on `/ready` keeps traffic away until the tables
  serve. A Kubernetes gRPC probe against the health service reports the
  same state.
- The liveness probe checks only that the process accepts connections,
  so it is a TCP check on the metrics port, whose accept loop keeps
  running through descriptor shortages. gRPC health reports whether a
  replica should receive traffic: it reads `NOT_SERVING` through a first
  load and through a drain, so as a liveness check it would make the
  kubelet restart a replica that is loading or draining as it should.

**Termination.** On SIGTERM icetray drains for up to
`server.drain_deadline` ([Configuration](configuration.md#server)) and
then flushes the page cache's disk level. `terminationGracePeriodSeconds`
must cover both, or the kubelet's kill cuts the flush short; Kubernetes'
default of 30 seconds leaves the flush only what remains after the drain. The time from the `draining`
event to the process's exit shows how long a drain and flush take. A
`preStop` delay, if one is used to let endpoints update first, adds to
the time needed.

**Security context.** The image runs as uid 1000. With `fsGroup: 1000`,
Kubernetes makes the volumes whose ownership it manages, such as an
`emptyDir` or most persistent volumes, writable by that user. icetray
fails with permission denied on a local directory the user cannot write,
such as a root-owned volume or `/var/lib/icetray` in the image's own
filesystem.

**Configuration.** Mount the ConfigMap as a directory and point
`ICETRAY_CONFIG` at the file inside it. Kubernetes updates such a mount by
swapping a symlink, which icetray picks up at the next poll or on SIGHUP.
Kubernetes never updates a `subPath` mount.

**Resources.** Size the memory limit as in [Memory](#memory), with the
kernel page-cache headroom for `disk` tables' chunk files above the
request. Size CPU as in [CPU](#cpu), without a tight limit. The node or
the container runtime sets the descriptor limit
([File descriptors](#file-descriptors)).

## What to alert on

Alert on the conditions below. Each of the first three compares one
series with a constant, so its alert routes to its owner without a join.

- `icetray_table_below_level == 1`: a table costs more per lookup than
  its `lookups` level allows. Usually its newest snapshot's layout is the
  cause: a writer changed the layout, or a compaction is overdue, and
  this alert goes to the table's owner. It reads the same with `strict`
  on or off, since it measures the newest snapshot whether that snapshot
  serves or was refused. `icetray_table_level` and the table's findings
  show the cause, and `icetray_table_column_level` shows which value
  column of the same snapshot, since a table's level is its most
  expensive column's. When no finding caps the level, the cause is the
  replica: a `disk` table's chunk files do not fit in the kernel's page
  cache beside everything else under the memory limit
  ([Memory](#memory)). Such a table fell below its level when
  `icetray_table_chunk_resident_fraction` dropped under 0.95, and stays
  below until the fraction rises above 0.99, so a fraction between the
  two can read either way.
- `icetray_table_state{state="degraded"} == 1` for longer than a few
  polls: a table's refreshes are failing, or its ref names a snapshot
  icetray refused for a reason other than its level, and the table serves
  an old snapshot. This alert goes to whoever runs the server: it fires
  for faults in storage and catalogs and for snapshots icetray cannot
  serve correctly. A layout that only costs more shows as below its level
  instead, and under `strict` as held.
- `icetray_table_state{state="held"} == 1`: icetray refused a strict
  table's newest snapshot for its level and serves the last one that met
  it, so the served data falls behind the writer until the writer commits
  a snapshot that meets the level. `icetray_generation_age_seconds`, the
  next alert, shows how far behind.
- `icetray_generation_age_seconds` above the table's publishing interval
  plus a margin: the served data is older than its writer's schedule
  allows.
- `icetray_config_pending == 1` for more than a few minutes: icetray
  refused a reload, or a restart-only setting changed.
- A rising share of `icetray_requests_total` with `code` `unavailable`,
  `internal`, or `deadline_exceeded`.
- Any `icetray_requests_total{code="resource_exhausted"}`, or
  `icetray_requests_in_flight` near `icetray_max_in_flight`: the replica
  is saturated.
- `icetray_cache_budget_shortfall_bytes > 0`: the cache budget and the
  resident indexes need more memory than the limit allows, so the
  replica can be killed for exceeding it once the caches fill. Raise the
  limit or lower `server.cache_bytes` ([Memory](#memory)).
- `icetray_process_rss_bytes` near the container's memory limit.
- Failing readiness or restarting pods, from the orchestrator.

## Running the doctor against a deployment

Every sizing figure above comes from the doctor's `predicted` bytes, and
the doctor is the tool that explains a refusal. It loads each table the
way `serve` does, at the table's configured residency, and then decodes
and hashes every key to check its bucket, so its CPU time grows with the
row count and its memory matches a load's.

Run it as a separate Job or Pod from the same image, with the same
configuration and credentials, instead of inside a serving pod. Inside
the pod it competes with the server for CPU and memory, and it shares the
server's local directory: it opens the same page cache disk level and
writes chunk files beside the server's. Give the doctor's pod its own
`emptyDir` for `storage.local.dir`, or run it with `--location`, which
uses no local directory and checks only the value columns its
`--value-column` flags name.
