# The optimal layout

The optimal layout is the Iceberg and Parquet layout at which icetray
serves a cold hit with one GET for each requested column and a miss with
none. A table written to the optimal layout reaches the `optimal` lookup
level under `full` and `disk` residency. icetray serves other layouts
less efficiently and refuses a layout it would serve incorrectly. It
checks the files a writer produced, whatever properties the writer was
given, at every load and in `icetray doctor`. An
[ordered table](#ordered-tables), which holds many rows per key, uses
this layout with each file sorted by key and then order value.

## The recipe

A writer that sets these table properties and this sort order produces a
table that meets every requirement below. This Spark SQL sets the
properties [`dev/golden-tables.py`](../dev/golden-tables.py) uses to
write the golden tables:

```sql
CREATE TABLE db.kv (key BIGINT, value BINARY) USING iceberg
PARTITIONED BY (bucket(16, key))
TBLPROPERTIES (
  'parquet.enable.dictionary' = 'false',
  'parquet.columnindex.truncate.length' = '256',
  'write.parquet.page-row-limit' = '1000',
  'write.parquet.page-size-bytes' = '8388608',
  'write.parquet.row-group-size-bytes' = '134217728',
  'write.parquet.compression-codec' = 'zstd',
  'write.parquet.bloom-filter-enabled.column.key' = 'true',
  'write.parquet.bloom-filter-fpp.column.key' = '0.01',
  'write.metadata.metrics.column.key' = 'full',
  'write.distribution-mode' = 'hash',
  'write.target-file-size-bytes' = '67108864'
);
ALTER TABLE db.kv WRITE LOCALLY ORDERED BY key;
```

The sort is local on purpose. `WRITE ORDERED BY key`, without `LOCALLY`,
also sets `write.distribution-mode` to `range`, replacing the `hash` above,
so Spark sorts the whole write and may split a bucket across several
files.

For values of kilobytes per key, the recipe lowers
`write.parquet.page-size-bytes` to `65536` and sets
`write.parquet.row-group-check-min-record-count` and
`write.parquet.row-group-check-max-record-count` to small values, so the
writer checks the page size often enough for the byte bound to take
effect.

Iceberg's default write mode for `DELETE`, `UPDATE`, and `MERGE` is
copy-on-write. When any of them is set to merge-on-read, the writer
writes delete files, which icetray serves when they are deletion vectors
or Parquet position deletes ([RFD 021](rfd/021-merge-on-read.md)). Under
`full` and `disk`, a delete adds no GET to a lookup; under `bloom`, a
miss on a deleted key costs the key-page GET that a bloom filter's false
positive costs. An update writes the new row to a new data file whose key
range overlaps the older file's, which icetray reports as
`overlapping_files`, capping the table at `permissive` until a compaction
rewrites the bucket. A compaction must use the same file-size target to
preserve the layout.

## Requirements

When a table misses a requirement, icetray responds in one of three ways:

- **Refused**: icetray refuses the snapshot, because a response could be
  wrong or icetray cannot hold the table as configured. The load fails
  with the reason in the second column.
- **Caps a level**: every response is correct, and some lookups cost
  more. The doctor reports the finding with its measured penalty, in
  extra GETs per hit, keys decoded per lookup, or files probed per
  lookup, and the [level](configuration.md#choosing-a-level) it caps the
  table at: `balanced` for a cost within one file, and for the files of
  one snapshot that share an edge key of an ordered table; `permissive`
  for any other extra files probed. icetray serves a table capped below
  its `lookups` level and flags it; under `strict`, icetray refuses the
  snapshot as `below_level`.
- **Advice**: the doctor names the writer property to change. Advice
  never affects whether icetray serves the table.

The second column gives the refusal reason or the finding code as the
doctor's output and the metrics spell it.

| Requirement | If missed | What fixes it |
|---|---|---|
| Data files in Parquet | Refused: `data_format_unsupported`, naming the file and the format its manifest entry records. Iceberg also allows ORC and Avro data files; icetray reads rows from Parquet alone and refuses the snapshot from its manifests, before it reads the file | `'write.format.default' = 'parquet'`, Iceberg's default, then a compaction to rewrite the files written in another format |
| One key column of Iceberg type `long`, `string`, `binary`, `fixed` (served as binary), or `uuid` | Refused: `key_type_unsupported`. A key column whose Parquet type contradicts the schema is refused as `key_type_mismatch` | Declare the key with one of those types. The writer and the caller encode a composite key into one column. |
| Keys of at most 4,096 bytes | Refused: `key_too_long`. Keys over 128 bytes draw the advice `long_keys` | A shorter key encoding |
| No null keys | Refused: `null_keys` | Filter nulls before the write |
| A key at most once within a data file, deleted rows included | Refused: `duplicate_keys` | Deduplicate before the write |
| A live key in at most one data file, where a row removed by a delete is not live | Refused: `duplicate_keys` | Deduplicate before the write, or upsert with `MERGE`, which rewrites or deletes the older row |
| Each configured value column of type `binary`, `fixed`, or `string`, nulls allowed | Refused: `value_type_unsupported`, naming the column | Serialize typed fields into a binary column, such as a protobuf message. See [Value columns](#value-columns) for when to pack fields into one column and when to split them. |
| Each column declared protobuf of type `binary`, with every value a serialized protobuf message | A `fixed` or `string` column declared protobuf is refused: `value_type_unsupported`, naming the column and the format. A value that is not protobuf draws the advice `malformed_protobuf` from `icetray doctor`, naming the column, and icetray fails every lookup that requests fields from that value | Declare protobuf only on a `binary` column whose writer serializes protobuf messages into it. See [Protobuf columns](#protobuf-columns). |
| Each configured column stored under the name and field id the snapshot's schema gives it, in data files that record a field id on every column, as Iceberg's writers do | Refused: `column_mismatch`, for a column stored under another field id (a swap of names, or a name dropped and added again), its field id stored under another name (a rename), a file that records no field ids and lacks the column, a file without its key column, a file that stores the name as a nested group, a `fixed` key or value column stored at another width, a configured name that is not a top-level field, or a default in the schema that does not parse as its column's type. icetray serves a file that records field ids and was written before its value column was added, returning the column's default for its rows, and the doctor reports the advice `column_default` with the count of such files | Write through an Iceberg writer, and change the schema as [Changing the schema](#changing-the-schema) describes; a compaction rewrites the files that predate a column |
| No equality delete that may apply to a data file | Refused: `equality_deletes`, for an equality delete newer than the snapshot's oldest data file, which may apply to one (RFD 021). icetray ignores an equality delete that no data file is older than, since it changes no response. | For a writer that leaves equality deletes, configure the table to serve a tag or branch that the compaction job moves after it rewrites the deletes into data files. |
| Only deletes icetray reads: deletion vectors and Parquet position deletes | Served (RFD 021). Refused: `delete_kind_unsupported`, for an Avro or ORC position delete file that may apply to a data file no deletion vector masks, a Puffin blob other than a deletion vector, or a Puffin footer that is compressed or sets reserved flags. icetray ignores an Avro or ORC position delete file that applies to no data file in the snapshot, or only to data files a deletion vector masks, since it changes no response. A deletion vector that applies to a data file masks that file's position deletes. | A merge-on-read writer that writes deletion vectors or Parquet position deletes |
| Each data file sorted by key | Refused: `unsorted` | `ALTER TABLE ... WRITE LOCALLY ORDERED BY key`. A local sort suffices. |
| Every key in the bucket its file's manifest entry records | Refused: `key_outside_bucket` | Let Iceberg's bucket transform assign files to partitions, and do not register files under a partition by hand |
| `bucket(N, key)` partitioning, and no other partition field | Caps at `permissive`: `no_bucket_partition`, files probed per lookup | `PARTITIONED BY (bucket(N, key))`, then rewrite the files written without it. See [Choosing the bucket count](#choosing-the-bucket-count). |
| Within a bucket, files with disjoint key ranges | Caps at `permissive`: `overlapping_files`, files probed per lookup | `'write.distribution-mode' = 'hash'` with `spark.sql.adaptive.optimizeSkewsInRebalancePartitions.enabled=false` on the write session, or a compaction |
| Dictionary encoding off on each value column | Caps the column at `balanced`: `dictionary_encoded_value`, naming the column, one extra GET per hit that requests it | `'parquet.enable.dictionary' = 'false'` |
| Key pages of 1,000 rows | Caps at `balanced` above 2,000 rows: `large_key_pages`, keys decoded per lookup | `'write.parquet.page-row-limit' = '1000'` |
| Value pages bounded by bytes for large values | Advice, per column, when a file's mean value page for that column is over 1 MiB: `large_value_pages`. Over 64 MiB: `value_pages_over_ceiling`, and icetray fails a lookup into a page that large with an error | `'write.parquet.page-size-bytes' = '65536'` with a short page-size check interval, as the recipe above describes |
| Full-length manifest metrics on the key | Advice: `bounds_missing` | `'write.metadata.metrics.column.key' = 'full'` |
| A full-length column index on the key | Advice: `column_index_truncated`, when under half of a file's key pages have a distinct column-index minimum, which a writer's cut to a shared prefix causes; an [ordered table](#requirements-of-an-ordered-table) is judged by its first keys instead | `'parquet.columnindex.truncate.length'` at least the key length, such as `'256'` |
| A bloom filter on the key | Advice: `no_bloom_filter`. Under `bloom` residency, icetray refuses the table with the same reason | `'write.parquet.bloom-filter-enabled.column.key' = 'true'` |
| A bloom filter sized for its keys | Advice: `bloom_oversized` above 4 bytes per key, `bloom_undersized` below 8 bits per key | `write.parquet.bloom-filter-ndv.column.key` set to the keys per row group, or `write.parquet.bloom-filter-max-bytes` capping the filter. Iceberg 1.11 honors both, as the golden tables check; earlier writers have ignored one or the other |
| Deletes compacted into the data files | Advice while deletes remove rows: `deletes_pending`, with the rows, delete files, and delete state a compaction would remove and, where files overlap, the level the table reaches once none do | A compaction that rewrites the data files the deletes apply to |
| Data files of 32 to 64 MiB | Advice: `large_file` above 128 MiB, and `many_files_per_bucket` when a bucket holds more than twice the files its bytes would fill at 64 MiB each | `'write.target-file-size-bytes' = '67108864'`, and the same target in compaction |
| zstd compression | Not checked | `'write.parquet.compression-codec' = 'zstd'` |

icetray also refuses a load that exceeds a configured budget, as
`over_budget`, before it fetches any key chunk or delete: a `full` table
whose resident bytes exceed its `resident_budget_bytes`, or a load of
`disk` tables whose chunk files would exceed
`storage.local.index_budget_bytes`. A `full` table's resident bytes count
its delete state at a bound taken from the manifests.
For an ordered table with a `string` or `binary` order column, whose
order fences the footer gives only by estimate, icetray checks again
once the files are built: the exact resident bytes against
`resident_budget_bytes`, and each chunk file at its exact length against
`index_budget_bytes` before it writes it.
`resident_budget_bytes` is configured in
[`[[tables]]`](configuration.md#tables) and `index_budget_bytes` in
[`[storage.local]`](configuration.md#storagelocal). A `disk` table
configured without a local directory makes the configuration file
invalid.

The doctor prints a per-column property in a finding's `property` field
as its prefix up to the column name, such as
`write.metadata.metrics.column.`, and names the column in the finding's
message.

A finding about one value column names it in its `column` field. Each
value column has a level of its own, set by the caps of its own findings
together with those of every finding about the row or the table. The
table's level is that of its costliest column, so under `strict`, one
column below the declared level refuses the snapshot for the whole
table.

## Value columns

icetray serves the value columns a table's
[`value_columns`](configuration.md#tables) lists, and a lookup reads only
the columns its request lists ([RFD 022](rfd/022-value-columns.md)). The
owner can configure none, for a table that returns only whether a key is
present, and a lookup that requests no columns costs only finding the
row. On a cold hit, each requested column costs a GET of its own, issued
concurrently with the others, so a hit that requests several columns
waits on the slowest GET.

For a table read by one kind of caller, one packed column performs best:
the writer serializes the row into one `binary` column, such as a
protobuf message whose field additions carry schema evolution, and a
cold hit costs one GET. For a table read by several kinds of caller, the
writer splits the row along how callers read it:

- Store fields read together in one column.
- Store a large field read on its own, such as an embedding, in its own
  column. A caller that requests only the small fields then skips
  fetching and decoding it, and each value page of the other columns
  holds more rows.
- For a caller that reads most of a row, one packed column costs one GET,
  where requesting several columns costs one GET per column.

icetray resolves each column in each data file independently, so a
lookup may return one column's default from a file that stores the
others.

## Protobuf columns

The owner can declare a `binary` column of serialized protobuf messages
as protobuf, `{ name = "detail", format = "protobuf" }` in
[`value_columns`](configuration.md#tables). A request can then list
top-level field numbers for the column, and icetray trims each value to
those fields ([RFD 024](rfd/024-field-projection.md)). The trimmed value
keeps every occurrence of each requested field, in the value's order:
each element of a repeated field, each record of a packed one, each map
entry, and each occurrence of a singular field, so a scalar's last write
still wins and a singular message still merges. icetray refuses a
`fixed` or `string` column declared protobuf as
`value_type_unsupported`. Projection reduces the bytes on the wire and
the caller's decode time, and costs the same GETs as reading the whole
value.

The writer must ensure that every value of the column parses as
protobuf, and so does the column's default in the schema, if it has one.
icetray returns a value that does not parse whole to a request that
lists no fields of the column, and fails a request that lists fields
with `FAILED_PRECONDITION`, on every lookup until the writer rewrites
the value.
`icetray doctor` walks the column's default, when a file written before
the column returns it, and the values of one value page of each protobuf
column, and reports a value that does not parse as `malformed_protobuf`.
The page is the first value page of the first file that stores the
column; when that page holds only nulls, the doctor moves to the next
such file, up to three files. A malformed value elsewhere, or in a
column whose walked pages hold only nulls, surfaces when a lookup first
requests fields from it, and `icetray_projection_errors_total` counts
it.

The caller must follow two rules, which icetray cannot check because it
has no protobuf schema:

- **Oneofs.** A caller that requests a member of a oneof requests every
  member of it. Setting one member clears the others, so when a value
  sets one member and later another, a projection that requests only the
  first returns it set and reports no error.
- **Required fields.** A projection of a message with proto2 `required`
  fields, or `LEGACY_REQUIRED` under editions, can omit them, so the
  caller parses it partially: Go's
  `proto.UnmarshalOptions{AllowPartial: true}`, Java's
  `parser().parsePartialFrom`, C++'s `ParsePartialFromString`.

A field the request does not list reads as unset, which for a field
without presence is its zero value. A requested number the message does
not define is valid: the projection holds whatever the value carries
under that number, and the caller's runtime keeps it as unknown fields.

## Changing the schema

icetray finds each configured column in a data file by its top-level name
and checks it against the field id the snapshot's schema gives that name
([RFD 023](rfd/023-column-resolution.md)). Iceberg's writers record a
field id on every column of every file they write. For a file written
without field ids, icetray serves the columns it stores by name,
unchecked, and refuses the snapshot when the file lacks a configured
column, since nothing in the file says whether that column was added
later or renamed. icetray treats a file with field ids on only some
columns as one without them, except that it refuses a column found by
name whose field id differs from the schema's. icetray serves a key or
value column whose name is a valid Avro name: ASCII letters, digits, and
underscores, not starting with a digit. Configuration validation refuses
any other name, since Iceberg's Java writers store such a column under an
escaped name that differs from the schema's.

- The owner can add a column to a served table without a rewrite. For
  the rows of a file written before the column, icetray returns the
  column's `initial-default`, or null when the schema gives none, and
  reads no value page for it. The doctor reports such files as
  `column_default` until a compaction rewrites them with the column.
- To serve a column under a new name, add a new column beside it, write
  it, and move the callers to it; then drop the old one from the
  clients, then from the configuration, then from the schema. icetray
  refuses a rename: under the old name as a column the schema lacks (a
  table fault), and under the new name as `column_mismatch`. It refuses
  a swap of two names as `column_mismatch`. In each case icetray keeps
  serving the previous snapshot.
- icetray refuses a column dropped and added again under the same name
  the same way, since the files written before store the old column
  under that name with another field id.

## Choosing the bucket count

The writer fixes `N` in `bucket(N, key)` when it writes the table, and
changing it requires rewriting every file.

- Choose `N` so each bucket receives about one target-sized file per full
  write: near the table's compressed size divided by 64 MiB. Hash
  distribution sends each bucket to one write task, so `N` also sets the
  write's parallelism.
- Fewer buckets than that leave lookup cost unchanged. A sorted write
  task rolls a new file at the size target, so the files of one bucket
  cover disjoint key ranges and a lookup still probes one of them. The
  cost is larger write tasks.
- More buckets than that produce small files. Each file adds a footer
  read at load, resident index overhead, and under `disk` residency an
  open file descriptor, and the doctor reports a bucket of many small
  files as `many_files_per_bucket`.
- Each append adds up to one file per bucket. For a table appended to
  often, fewer buckets or a regular compaction keep the file count down.
- Partition by the key's bucket alone. A second partition field, such as
  a date, splits one bucket's keys across files whose key ranges
  intersect, which the doctor reports as `overlapping_files`.

## Ordered tables

An ordered table holds many rows per key, each with an order value, such
as a time series by key and timestamp or the activity of each user. The
owner configures its [`order_column`](configuration.md#tables), and
callers read a key's rows newest or oldest first with `Scan` and
`MultiScan` ([Calling icetray](calling.md#reading-an-ordered-table)). Its
layout is the optimal layout above with two changes: each file is sorted
by key and then by order value, ascending, and a key repeats within a
file. A key's rows in one file are therefore consecutive, a run sorted by
order value, which may span key pages and row groups. Partitioning stays
`bucket(N, key)`, so all of a key's rows share a bucket.

### The recipe for an ordered table

The table properties of [the recipe](#the-recipe) apply unchanged, with
the order column after the key in the local sort order:

```sql
CREATE TABLE db.events (key BIGINT, ts TIMESTAMP, payload BINARY) USING iceberg
PARTITIONED BY (bucket(16, key))
TBLPROPERTIES (
  'parquet.enable.dictionary' = 'false',
  'parquet.columnindex.truncate.length' = '256',
  'write.parquet.page-row-limit' = '1000',
  'write.parquet.page-size-bytes' = '8388608',
  'write.parquet.row-group-size-bytes' = '134217728',
  'write.parquet.compression-codec' = 'zstd',
  'write.parquet.bloom-filter-enabled.column.key' = 'true',
  'write.parquet.bloom-filter-fpp.column.key' = '0.01',
  'write.metadata.metrics.column.key' = 'full',
  'write.distribution-mode' = 'hash',
  'write.target-file-size-bytes' = '67108864'
);
ALTER TABLE db.events WRITE LOCALLY ORDERED BY key, ts;
```

- **Uniqueness.** A key and order value appear together at most once in
  the table's live rows. The writer deduplicates before the write, or
  upserts with `MERGE` on the key and the order value, whose deletes
  icetray applies like any other
  ([RFD 021](rfd/021-merge-on-read.md)):

  ```sql
  MERGE INTO db.events t
  USING updates s
  ON t.key = s.key AND t.ts = s.ts
  WHEN MATCHED THEN UPDATE SET *
  WHEN NOT MATCHED THEN INSERT *;
  ```

- **Ties.** Rows of one key that share a timestamp need a tiebreak
  inside the order value, such as a `timestamp_ns` with a counter in its
  low digits, or a `long` sequence number in place of the timestamp.
- **Compaction.** A compaction sorts by the same order and keeps the
  file-size target:

  ```sql
  CALL catalog.system.rewrite_data_files(
    table => 'db.events',
    strategy => 'sort',
    sort_order => 'key ASC, ts ASC',
    options => map('target-file-size-bytes', '67108864')
  );
  ```

- **Streaming writers.** A writer that leaves its files unsorted, as
  streaming appends often do, writes snapshots icetray refuses as
  `unsorted`. Serve a tag or branch that the compaction job moves after
  it sorts the files, so icetray serves only sorted snapshots.
- **Page indexes.** icetray locates the order column's pages through
  its offset index, which Iceberg's writers write by default. A `Scan`
  places its bounds, and a refresh chooses the order pages it compares,
  by the order fences icetray records from the rows at load, one per
  order page, so neither depends on whether the writer keeps the order
  column's column index whole, truncates it, or leaves it out, and the
  column index draws no finding.

### Requirements of an ordered table

Every requirement of [the optimal layout](#requirements) applies to an
ordered table, except the two that keep a key at most once, the sort by
key alone, and disjoint key ranges within a bucket, which the rows below
replace.

| Requirement | If missed | What fixes it |
|---|---|---|
| Each data file sorted by key and then order value, ascending, across row-group boundaries | Refused: `unsorted` | `ALTER TABLE ... WRITE LOCALLY ORDERED BY key, ts`, and the same sort order in compaction |
| A key and order value at most once within a data file, deleted rows included | Refused: `duplicate_rows`, naming the key and the order value | Deduplicate before the write, with a tiebreak inside the order value for rows that share a timestamp |
| A live key and order value in at most one data file, where a row removed by a delete is not live | Refused: `duplicate_rows`, naming the later file | Deduplicate before the write, or upsert with `MERGE` on the key and the order value |
| An order column of Iceberg type `int`, `long`, `date`, `timestamp`, `timestamptz`, `timestamp_ns`, `timestamptz_ns`, `string`, `binary`, or `fixed` | Refused: `order_type_unsupported`. An order column stored in another Parquet type than Iceberg's, such as INT96 or millisecond timestamps, or promoted to a type of another unit, such as `date` to `timestamp`, is refused as `order_type_mismatch`. A file written before Iceberg promoted the column from `int` to `long` is served | Declare the order column with one of those types and write it through an Iceberg writer; rewrite files a migration left in another unit |
| No null order values | Refused: `null_order` | Filter nulls before the write |
| The order column in every data file | Refused: `column_mismatch`, for a file written before the order column was added, since a default would give every row of the file one order value | A compaction that rewrites those files with the column |
| An offset index on the order column | Fails the load as a `file` fault, since icetray locates every order page through it | An Iceberg writer, which writes it by default |
| Dictionary encoding off on the order column | Under `full` and `bloom`, caps at `balanced`: `dictionary_encoded_order`, one extra GET per `Scan` per file for the dictionary page. Under `disk` the dictionary page is a local read | `'parquet.enable.dictionary' = 'false'` |
| Within a bucket, files with disjoint key ranges | Caps at `balanced`: `shared_edge_keys`, for files one snapshot added that meet only where one file's last key is the next file's first, as a compaction that rolls files by size writes them; a `Scan` of such an edge key reads every file that holds it. Caps at `permissive`: `overlapping_files`, for any other overlap, as appends write it | A compaction, which merges the appended files; a bucket compacted into one file has no edge keys |
| Runs of at most 1,000 rows | Advice: `long_runs`, naming the longest run; a caller reads a longer history in more than one `Scan` at the default `max_scan_rows` | Nothing in the layout: callers page with `next` ([Calling icetray](calling.md#paging-with-next)) |
| `max_scan_rows` rows of the configured value columns within `server.max_scan_bytes` ([Configuration](configuration.md#server)) | Advice: `large_runs`, from the footer's uncompressed bytes per row; a `Scan` at the full limit would be refused as `FAILED_PRECONDITION` | A lower `max_scan_rows`, or callers that request fewer rows or columns; an owner whose clients accept larger messages raises `server.max_scan_bytes`. The advice counts the value columns' bytes alone: each row's order value and framing add more, 10 to 20 bytes for an integer or time order value, so a table just under the bound can still be refused |

The bloom findings `bloom_oversized` and `bloom_undersized` divide a row
group's filter bits by the distinct keys it holds, which the load counts,
so a repeated key counts once.

The `column_index_truncated` advice counts a key's repeated pages once. A
key's run spans many key pages that share one minimum, so a count of
distinct page minimums over pages would read a long run as a truncated
index. The doctor compares the distinct page minimums with the distinct
first keys of the pages, which the load decodes, and draws the advice when
fewer than half of those first keys still differ in the column index. An
untruncated column index holds each page's first key as its minimum, so
an ordered table whose runs span pages draws nothing, and one whose writer
cut a long string key's minimums to a shared prefix draws the advice.

### Levels of an ordered table

The [levels](configuration.md#choosing-a-level) of an ordered table are
read for a `Scan` of one key. A table where every `Scan` reads one file
can reach `optimal`, whatever the length of its runs: decoding a run's
edge key pages, and a run that spans row groups, leave it there. How the
writer commits to the table sets the level it reaches:

- One file per bucket, or files with disjoint key ranges, reaches
  `optimal`.
- A compaction that rolls a bucket's files by size writes files that may
  share an edge key, which reaches `balanced` under `shared_edge_keys`.
  That cost is fixed when the compaction writes the files and grows with
  no later commit. An owner whose buckets hold more than one
  target-sized file configures `lookups = "balanced"`.
- Appends between compactions write files that overlap a bucket's
  existing ones, and a `Scan` reads one file per append that holds the
  key until a compaction merges them. Such a table is `permissive` under
  `overlapping_files`, so its owner configures `lookups = "permissive"`,
  and `icetray_scan_files` shows the files each `Scan` reads.

## The checks

`icetray doctor` runs the check and prints the findings;
[Configuration and the command line](configuration.md) covers its
command line and output. The decisions behind the optimal layout are
recorded in [RFD 002](rfd/002-serving-layout-contract.md) (the layout),
[RFD 007](rfd/007-serving-tiers.md) (refusals, costs, and advice, with
their penalties), [RFD 009](rfd/009-writes-between-builds.md) (writes
between builds), [RFD 016](rfd/016-doctor.md) (the doctor), [RFD
020](rfd/020-lookup-levels.md) (lookup levels), [RFD
021](rfd/021-merge-on-read.md) (delete files), [RFD
022](rfd/022-value-columns.md) (value columns), [RFD
023](rfd/023-column-resolution.md) (column resolution), [RFD
024](rfd/024-field-projection.md) (field projection), and [RFD
025](rfd/025-ordered-rows.md) (ordered tables). The
[glossary](glossary.md) defines terms such as residency, level, and
strict.
