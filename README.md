# icetray

<p align="center">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="docs/images/icetray-dark.svg">
    <img src="docs/images/icetray.svg" width="480" alt="Cubes break off an iceberg and fall into an ice tray">
  </picture>
</p>

Batch-frozen data, served one cube at a time.

icetray is a Rust server for key lookups on Apache Iceberg tables in object
storage. Once a batch job writes a table and publishes it to Iceberg,
icetray can serve it over gRPC straight from the table's Parquet
files:

- **`Get`** and **`MultiGet`**: the value columns of one key, or of many
  keys in one call.
- **`Scan`** and **`MultiScan`**: a key's rows in order, newest or oldest
  first, for a table that holds many rows per key.
- **`TableInfo`**: a table's snapshot, key type, and value columns, and an
  ordered table's order column and scan limits.

It suits data that is computed in batch and read by key, and is ideal for a
table with a hot working set and a long, rarely read tail: hot rows are
served from icetray's row cache and hot pages from its page cache, and a
cold page costs one range GET.

## Why icetray

- **Tables written in batches and read by key are common.** Model scores,
  embeddings, and features that a service computes for its users change
  only in batches but are read at random. A service with millions of users
  sees a small, active head, but any user in the long tail can come back
  at any moment. Teams usually write these tables to the data lake and
  then load them into an online key-value store to serve them, and those
  stores, built for writes, are expensive. icetray serves the tables
  directly from the lake.
- **Each table trades memory for GETs and latency.** A table's residency
  determines where its key chunks are stored, and its lookup level caps what a
  lookup may cost, which icetray checks on every snapshot
  ([Residency and lookup level](#residency-and-lookup-level)). A team
  weighs memory against GETs and latency, and all three against the
  effort of writing the optimal layout.
- **It follows how batch writers publish.** A writer commits a snapshot
  and moves the ref icetray follows, a branch or tag, and icetray loads
  the snapshot on its next poll. Between rebuilds, a writer that updates
  rows by merge-on-read leaves deletion vectors or Parquet position
  deletes beside the data files, and icetray applies them on every lookup
  until a compaction rewrites them into the data files.

## How it works

### A table written to the optimal layout costs one GET per hit and none per miss

icetray's optimal layout is the Iceberg and Parquet layout that makes a
point lookup cheap:

- `bucket(N, key)` partitioning;
- files sorted by key;
- 1,000-row pages;
- dictionary encoding off on the value column;
- a Bloom filter on the key.

A standard Iceberg writer produces it from table properties and a local
sort order, as in this Spark SQL excerpt:

```sql
CREATE TABLE db.kv (key BIGINT, value BINARY) USING iceberg
PARTITIONED BY (bucket(16, key))
TBLPROPERTIES (
  'parquet.enable.dictionary' = 'false',
  'write.parquet.page-row-limit' = '1000',
  'write.parquet.bloom-filter-enabled.column.key' = 'true',
  'write.target-file-size-bytes' = '67108864'
  -- and the rest of the recipe
);
ALTER TABLE db.kv WRITE LOCALLY ORDERED BY key;
```

A lookup finds its bucket with the Iceberg bucket transform, its file by
key range, and its row by binary search over the key chunks, so a miss
costs no GET under `full` and `disk`. icetray serves tables in other
layouts less efficiently, and refuses a layout it would serve incorrectly,
such as unsorted files.

`icetray doctor` checks a table against the optimal layout. It reads the
table's metadata and data files and reports each deviation, its cost per
lookup, and the writer property that fixes it, and the server runs the
same check on every load. [The optimal layout](docs/table-layout.md) has
the full recipe and every check.

### A lookup reads only the value columns it requests

A table can serve several value columns, and each request lists the
columns the caller reads, so callers with different access patterns share
one wide table: one reads the embedding column, another the properties
column. On a cold hit, each requested column costs one range GET, issued
concurrently, and a lookup that requests no column returns only the key's
presence. A table read by one kind of caller is fastest with the whole row
serialized into one value column.
[Value columns](docs/table-layout.md#value-columns) covers when to split a
row into columns.

### A protobuf column can be projected to the requested fields

A `binary` column that holds serialized protobuf messages can be declared
`format = "protobuf"`. A lookup can then project its value to a list of
top-level field numbers, so the caller receives and decodes only those
fields. Projection runs after the page cache, so it reduces the bytes on
the wire and the caller's decode time, and costs the same GETs as reading
the whole value, as in this `Get` request:

```json
{
  "table": "profiles",
  "key": {"long": "42"},
  "columns": [
    {"name": "properties", "fields": [1, 4, 9]},
    {"name": "embedding"}
  ]
}
```

[Calling icetray](docs/calling.md) covers requests, responses, and
errors, including the rules for projecting a oneof or a message with
required fields.

### An ordered table serves each key's rows in order

A table configured with an order column, such as a timestamp, holds many
rows per key, written sorted by key and then order value: a time series by
key, or the activity of each user. A `Scan` returns a key's rows newest or
oldest first, up to a limit and between optional bounds on the order
value, and a caller pages through a longer history by passing back the
`next` bound each response carries. A compaction that writes one file per
bucket holds each key's rows in one file, so under `full` a cold `Scan`
costs a GET per requested column and one for the order column, as in this
`Scan` request:

```json
{
  "table": "events",
  "key": {"long": "42"},
  "columns": [{"name": "payload"}],
  "direction": "DIRECTION_NEWEST_FIRST",
  "limit": 100
}
```

`icetray scan` runs the same `Scan` from the command line and prints each
row as a JSON line, then a line whose `next` is passed back as `--before`
for the next page:

```sh
icetray scan --config icetray.toml --table events --direction newest --limit 100 --column payload 42
```

[Ordered tables](docs/table-layout.md#ordered-tables) covers the writer
recipe, [Calling icetray](docs/calling.md#reading-an-ordered-table) the
calls and paging, and [the command line](docs/configuration.md#the-command-line)
`scan`'s flags and lines.

### Every response comes from one published snapshot

When a writer moves the ref, icetray loads the new snapshot alongside the
serving one, reuses the index of every unchanged data file, verifies the
new files, and swaps the snapshot in atomically. Every response carries
the id of the snapshot it was read from, and a `MultiGet` or `MultiScan`
reads all of its keys from one snapshot. icetray refuses a snapshot it
would serve incorrectly, such as one with unsorted files, duplicate keys,
or equality deletes, and keeps serving the last good snapshot.
[Refresh and reload](docs/operating.md#refresh-and-reload) covers the
snapshot lifecycle and the configuration changes a running server applies
without a restart.

### Residency and lookup level

A table's **residency** determines where its key chunks are stored:

| Residency | Key chunks | Resident memory per key | Cold hit | Miss |
|---|---|---|---|---|
| `full` | in memory, the lowest latency | about a byte for sequential keys, 8 for random integers, 14 to 18 for UUIDs | 1 GET | none |
| `disk` | in local files | a fraction of a byte | 1 GET and 1 local read | 1 local read |
| `bloom` | only the writer's Bloom filters in memory; key pages read from object storage | the Bloom filter | 2 GETs | none, or 1 GET for a false positive |

Resident memory is proportional to the key count and independent of
value size.

A table's **lookup level**, `lookups`, caps what a lookup may cost:

| Level | A lookup costs |
|---|---|
| `optimal` | one GET per requested column on a hit, none on a miss |
| `balanced` | a fixed extra cost per lookup, such as `bloom`'s second GET |
| `permissive` | whatever a layout icetray serves correctly costs, including probing several files |

When a snapshot's layout falls below the table's level, the
`icetray_table_below_level` gauge reads 1, and the load logs each finding
that caps the level. With `strict = true`, icetray refuses such a snapshot
and keeps serving the last one that met the level. `icetray doctor`
predicts each residency's resident memory and level from a table's
metadata and file footers, before deployment.
[Choosing a residency](docs/configuration.md#choosing-a-residency),
[Choosing a level](docs/configuration.md#choosing-a-level), and
[Performance and tuning](docs/performance.md) go deeper.

### Caches and latency

```mermaid
flowchart TB
    get([Get]) --> find["Find the row<br/>full: in memory<br/>disk: a local read<br/>bloom: the filter, then the key page"]
    find -- key absent --> miss([Miss])
    find -- key present --> row[Row cache]
    row -- cache miss --> mem[Page cache, memory level]
    mem -- cache miss --> disk[Page cache, disk level]
    disk -- cache miss --> s3[GET from object storage]
    row -- hit --> done([Value])
    mem -- hit --> done
    disk -- hit --> done
    s3 --> done
```

| Served from | Typical latency | Sized by |
|---|---|---|
| The row cache, in memory | about 10 µs | `server.row_cache_share` of `server.cache_bytes` |
| The page cache's memory level | tens of µs | the rest of `server.cache_bytes` |
| The page cache's disk level | a local read: about 0.1 ms from the kernel's page cache, 0.7 ms from the device | `storage.local.page_cache_disk_bytes` |
| Object storage, a cold hit | about 80 ms at the median and 170 ms at the 99th percentile in-region, for a file not read lately | not cached |

A table's median latency depends on its cache hit rate, and its tail on
its cold hits. A replica restarted on the same local volume keeps the
disk level's pages, and its memory caches start empty and fill from its
traffic.

`disk` residency adds a local key-page read to every lookup, and `bloom`
adds a key-page GET to a cold one.

The figures come from the
[row cache's skewed-key benchmark](docs/performance.md#skewed-keys-measured),
[disk-tier latency](docs/analysis/2026-10-07-page-cache-residency.md), and
[range-GET latency to an object not read recently](docs/analysis/2026-09-29-cold-range-get-latency.md);
[Performance and tuning](docs/performance.md) covers the settings.

## Limits and workarounds

icetray serves exact-key lookups, and reads of one key's rows in order, on
tables a batch job writes. Writes, scans across keys, and range queries
over keys need another tool. Each limit below has a workaround at write
time or in the platform:

| Limit | Workaround |
|---|---|
| **Data files in ORC or Avro.** icetray reads Parquet and refuses a snapshot that lists an ORC or Avro data file, which Iceberg also allows. | Write the table in Parquet, Iceberg's default format (`'write.format.default' = 'parquet'`). |
| **Equality deletes.** icetray serves deletion vectors and Parquet position deletes, and refuses a snapshot with equality deletes. | Serve a tag or branch that a compaction job moves after it rewrites the equality deletes into data files. |
| **Unsorted ordered files.** An ordered table's files must each be sorted by key and then order value; icetray refuses a file that is not as `unsorted`, as streaming appends often leave them. | Serve a tag or branch that the compaction job moves after it sorts the files ([the recipe for an ordered table](docs/table-layout.md#the-recipe-for-an-ordered-table)). |
| **Typed values.** Each value column is served as bytes, of Iceberg type `binary`, `fixed`, or `string`. | Serialize a number or a struct into a binary column at write time; a protobuf column can be projected to its top-level fields. |
| **Encryption and access control.** The gRPC port is plaintext and authenticates no client. | Run it on a private network, behind a service mesh, or with a sidecar that terminates TLS and enforces access policy. |
| **REST catalogs that need credentials.** The `rest` source sends no credentials. | Use a catalog that needs none, or a proxy that adds them. Tables in AWS Glue and S3 Tables have their own sources, signed with the S3 credentials. |

## Try it locally

You need a Rust toolchain, and a container runtime or Spark to write the
test table. This example reads the table from local disk.

**1. Build the binary.** `rust-toolchain.toml` pins the toolchain, which
rustup installs on the first `cargo` run:

```sh
cargo build --release
export PATH="$PWD/target/release:$PATH"
```

**2. Write a test table** of 2 million long keys with the repository's
golden-table script, which runs Spark in the `apache/spark` container
image. [Building and testing](docs/development.md#the-development-environment)
covers the container runtime and how to run the script with PySpark
instead:

```sh
dev/golden-tables.sh --only long_2m
```

**3. Look up a key.** The table's keys are multiples of three, so 42 is
present and 43 is absent. `--value-column` configures the column the table
serves, and `--column` requests it for each key:

```sh
icetray get --location target/golden-warehouse/db/long_2m --value-column value --column value 42 43
```

The hit costs one GET and the miss none. The values below are shortened,
and your snapshot ids and timings will differ:

```json
{"kind":"lookup","table":"long_2m","key":"42","presence":"present","values":{"value":{"base64":"NzM0NzVj…","utf8":"73475cb4…","bytes":192}},"snapshot_id":"7128216514704790069","gets":1,"elapsed_micros":342}
{"kind":"lookup","table":"long_2m","key":"43","presence":"absent","snapshot_id":"7128216514704790069","gets":0,"elapsed_micros":8}
```

Running `icetray doctor` with the same `--location` and `--value-column`
reports the table at the `optimal` level.

**4. Serve the table.** Save this configuration as `icetray.toml`,
replacing `/path/to/icetray` with the absolute path of your checkout,
since a static location must be absolute:

```toml
[[tables]]
name = "long_2m"
ref = "main"
key_column = "key"
value_columns = ["value"]

[tables.source]
type = "static"
location = "/path/to/icetray/target/golden-warehouse/db/long_2m"
```

```sh
icetray serve --config icetray.toml
```

The server listens for gRPC on `server.listen` and serves `/ready` and
`/metrics` on `server.metrics_listen`. This file sets neither, so the
values are those in the [reference](docs/configuration.md#server).

**5. Call it over gRPC.** From another terminal in the repository, look a
key up with [grpcurl](https://github.com/fullstorydev/grpcurl), where
`127.0.0.1:7433` is that `listen` value:

```sh
grpcurl -plaintext -proto crates/icetray-proto/proto/icetray.proto \
  -d '{"table": "long_2m", "key": {"long": "42"}, "columns": [{"name": "value"}]}' \
  127.0.0.1:7433 icetray.v1.Icetray/Get
```

Ctrl-C drains in-flight requests and stops the server.

## Deploying

- **Artifacts.** Each release publishes Linux binaries and a multi-arch
  container image to the GitHub Container Registry.
- **Configuration and credentials.** icetray reads its configuration from
  one TOML or JSON file and takes S3 credentials from the AWS SDK's
  default chain.
- **Observability.** It logs JSON to stdout and serves Prometheus metrics,
  `/ready`, and gRPC health.
- **Sizing.** Size a replica's memory limit from the doctor's predicted
  resident bytes, and put the local directory for `disk` residency on
  instance-store NVMe.
- **Stability.** Before 1.0.0, any release may change configuration keys,
  metric and log field names, the doctor's output contract, protobuf
  fields and their numbers, or on-disk formats
  ([Stability](CONTRIBUTING.md#stability)).

[Operating icetray](docs/operating.md) covers sizing, Kubernetes, and what
to alert on.

## Documentation

| If you | Read |
|---|---|
| call icetray | [Calling icetray](docs/calling.md): requests, responses, and errors |
| write the tables | [The optimal layout](docs/table-layout.md) |
| run icetray | [Operating](docs/operating.md), [performance and tuning](docs/performance.md), and [configuration and the command line](docs/configuration.md) |
| want a term defined | [The glossary](docs/glossary.md) |
| contribute | [Building and testing](docs/development.md), [contributing](CONTRIBUTING.md), [design decisions](docs/rfd/README.md), and the [measurements](docs/analysis/) behind them |

## Prior art

[ElephantDB](https://github.com/nathanmarz/elephantdb) established building
a dataset in batch and serving it read-only. icetray applies that shape to
Iceberg tables.

## License

MIT or Apache-2.0, at your option.
