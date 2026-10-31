# Configuration and the command line

## The file

The configuration is one file, TOML or JSON, read by `serve` and by the
[one-shots](glossary.md) (`doctor`, `get`, `scan`, and `info`). A
document whose first non-whitespace character is `{` is JSON; anything
else is TOML.
icetray parses both into the same schema.

- Every section is optional, and a section left out takes its defaults.
  Within a section, a key marked required below must be present.
- At least one table is required. A file that lists none is invalid.
- A key outside the schema is an error, and the message lists the keys
  that section accepts.
- icetray validates the whole file before it opens anything. An error names
  the file, where its path came from, and the setting by its full path,
  such as `invalid configuration file icetray.toml (from --config):
  server.poll_interval 0s is below the minimum of 1s`, and the run exits 1.
  icetray reports a file that is not valid TOML in one line, with the line
  and column, such as
  `` is not valid toml: line 1, column 10: unclosed array table, expected `]` ``.

### An example

This example configures one table read from a table root and one read
through a REST catalog. Its values suit a container and differ from the
defaults: the default listeners are loopback, and `storage.local` has no
default.

```toml
[server]
listen = "0.0.0.0:7433"
metrics_listen = "0.0.0.0:9433"
poll_interval = "30s"

[storage.local]
dir = "/var/lib/icetray"
page_cache_disk_bytes = 8589934592

[[tables]]
name = "kv"
ref = "main"
key_column = "key"
value_columns = ["value"]

[tables.source]
type = "static"
location = "s3://warehouse/db/kv"

[[tables]]
name = "profiles"
ref = "serving"
key_column = "user_id"
value_columns = [{ name = "properties", format = "protobuf" }, "embedding"]
residency = "disk"

[tables.source]
type = "rest"
uri = "http://catalog.example:8181"
warehouse = "example"
namespace = "db"
table = "profiles"
```

### Value formats

- **Durations** are strings in a unit-suffixed form: `"10s"`, `"250ms"`,
  `"100us"`, `"1.5s"`, `"1m30s"`, `"1m 30s"`, `"2h 30m"`, and
  `"10 seconds"` are all accepted. The largest unit is the hour, so
  `"24h"` is accepted and `"1d"` is not. A bare number, such as `10` or
  `"10"`, a negative duration, and ISO 8601 text such as `"PT10S"` are
  refused.
- **Byte sizes and counts** are TOML or JSON integers, in bytes for a
  size. TOML's digit separators work (`268_435_456`). Suffixes such as
  `"256MiB"` and floating-point numbers are refused.
- **Addresses** are an IP address and a port: `"127.0.0.1:7433"`,
  `"0.0.0.0:7433"`, `"[::]:7433"`. A host name is refused.
- **Locations** are URIs with the scheme `s3://`, `s3a://`, or `file:`,
  or absolute local paths. A `file:` URI is `file:///data/kv`,
  `file://localhost/data/kv`, or `file:/data/kv`; one with a relative path
  (`file:data/kv`) or another host (`file://otherhost/data/kv`) is
  refused. icetray checks a `static` table's location when it validates
  the file, after applying the file's own rewrite rules: it refuses an
  empty location, a relative path, and a location no store serves, such as
  `gs://` with no rule rewriting it, as `tables[0].source.location`, with
  exit 1. Validation applies only the file's rules, so a `--rewrite` on the
  command line cannot make such a location valid. icetray never resolves
  the file's locations against a working directory. (It does resolve a
  relative `--location` on the command line; see
  [One table without a file](#one-table-without-a-file).) The locations
  that a table's metadata records or its catalog returns are known only at
  load, and one that no store serves fails the load with the error kind
  `location`.

### `[server]`

| Key | Type | Default | Meaning |
|---|---|---|---|
| `listen` | address | `"127.0.0.1:7433"` | The gRPC listener. |
| `metrics_listen` | address | `"127.0.0.1:9433"` | The HTTP listener for `/metrics` and `/ready`. |
| `poll_interval` | duration | `"10s"` | How often each table's ref is re-resolved and the configuration file re-read. At least `"1s"` and at most `"24h"`. |
| `cache_bytes` | bytes | half the memory limit, else `268435456` (256 MiB) | The memory the row cache and the page cache's memory level hold together. The page cache holds every table's pages, a key-value table's and an ordered table's alike. Unset, it is half the process's memory limit, the smaller of its cgroup's limit and the host's memory, and 256 MiB when neither can be read. See the note below. |
| `row_cache_share` | number | `0.9` | The share of `cache_bytes` the row cache holds, in [0, 1]; the page cache's memory level holds the rest. When no table reads through the row cache, the page cache holds all of `cache_bytes`. A server with no page-cache disk level that serves a table reading through the row cache beside one reading pages alone must set it; see the note below. A value outside the range is refused, as `server.row_cache_share 1.5 is not in [0, 1]`. |
| `get_deadline` | duration | `"5s"` | The longest a `Get` may run, and the longest each page fetch of any lookup may run, in a `MultiGet` too. Past it the call fails with `DEADLINE_EXCEEDED`. |
| `multi_get_deadline` | duration | `"15s"` | The longest a whole `MultiGet` may run, whatever its size. |
| `max_in_flight` | count | `1024` | The most `Get`, `MultiGet`, `Scan`, and `MultiScan` calls in flight at once. icetray refuses one more immediately with `RESOURCE_EXHAUSTED`; calls are never queued. |
| `max_multi_get_values` | count | `1000` | The most values one `MultiGet` may request: its keys times the columns it requests, a presence-only request counting one per key. icetray rejects a larger request with `INVALID_ARGUMENT` before any lookup. Zero is invalid, as `server.max_multi_get_values 0 is not above zero`. |
| `batch_fan_out` | count | `64` | The most GETs one `MultiGet`, `Scan`, or `MultiScan` holds in flight at once, across every key and column it requests. Each GET holds one slot until it ends: for a lookup, a key page under `bloom`, a value page, or a dictionary page; for a `Scan`, a span of pages, a dictionary page, or a key page under `bloom`. A page served from the page cache takes none. When a batch needs more GETs than this, icetray issues the rest as slots free. Each GET has at most one hedge beside it. Above about 128, a cold batch against S3 gets no faster ([Performance and tuning](performance.md#multiget-fan-out)). Zero is invalid, as `server.batch_fan_out 0 is not above zero`. |
| `max_multi_scan_values` | count | `4000` | The most values one `MultiScan` may request: its keys times its limit times one more than its requested columns. icetray rejects a larger call with `INVALID_ARGUMENT` before it reads any key. Zero is invalid, as `server.max_multi_scan_values 0 is not above zero`, and so is a value below twice any ordered table's `max_scan_rows`, defaulted ones included, as `server.max_multi_scan_values 1000 is below twice tables[0].max_scan_rows 1000, so a MultiScan of one key at the full limit and one column would be refused`. |
| `max_scan_bytes` | bytes | `4194304` (4 MiB) | The most bytes one `Scan` or `MultiScan` response may encode to, the default being a gRPC client's default limit on a message it receives. icetray measures the response it would send and refuses a larger one with `FAILED_PRECONDITION`, sending no rows and naming the size, the bound, and the rows (`Scan`) or leading keys' results (`MultiScan`) that would fit; a `Scan` counts as an `error` in `icetray_scans_total`, and a `MultiScan` counts each key it read as one. `TableInfo` reports it in `max_scan_bytes` for every ordered table. An owner whose clients accept larger messages raises it. Like every setting outside `[[tables]]`, a change applies on restart. Zero is invalid, as `server.max_scan_bytes 0 is not above zero`. |
| `scan_deadline` | duration | `"5s"` | The longest a `Scan` may run, and the longest each GET of any Scan, span or key page, may run, in a `MultiScan` too. Past it the call fails with `DEADLINE_EXCEEDED`. |
| `multi_scan_deadline` | duration | `"15s"` | The longest a whole `MultiScan` may run, whatever its size. |
| `load_deadline` | duration | `"30m"` | The longest a build may run while no generation of the table is serving: a first load, or a retry after one failed. |
| `refresh_deadline` | duration | `"5m"` | The longest one refresh over a serving generation may run. Past it the table is [degraded](glossary.md) and the next poll retries. |
| `drain_deadline` | duration | `"20s"` | How long a shutdown waits for open calls to finish. |

Every duration and every count must be above zero. The gRPC layer
enforces a client's own deadline, and its expiry is reported as the
client's.

icetray sizes the caches as they open at start, and logs the result
([Operating icetray](operating.md#logs)). A key-value table reads
through the row cache unless it sets `row_cache = false`. When at least
one table the server serves reads through the row cache, the row cache
holds `row_cache_share` of `cache_bytes`; when none does, the page cache
holds all of it. A reload that changes whether any table served reads
through the row cache resizes both caches to match, within the same
`cache_bytes`; any other change to either setting applies on restart.
The default follows the memory limit rather than the memory free, so
every start of a replica gets the same budget, and the other half of the
limit holds the resident indexes, a second generation while a refresh
loads, and requests in flight ([Memory](operating.md#memory)); icetray
checks the budget and the indexes against the limit once the tables
serve, and warns when they do not fit. The row cache splits into shards
that each hold their share of its part, so a part below one 64 KiB value
per shard caches only smaller values.

The page cache has one shard per tokio worker thread, rounded up to a
power of two, and each shard must hold one 64 KiB value page plus its
64-byte entry: 65,600 bytes. icetray refuses a page cache part of a set
`cache_bytes` under one shard's minimum when it parses the file, and one
under the total for the machine's shards (1,049,600 bytes for the 16
shards of an 11-core machine) when it opens the cache, with exit 1,
before any table loads. A `cache_bytes` derived from the memory limit is
checked when the cache opens, and the refusal names the limit and where
it was read. A file that still sets the removed `page_cache_bytes` or
`scan_page_cache_bytes` is refused, naming `cache_bytes`, as
`server.scan_page_cache_bytes is no longer a setting; use server.cache_bytes`.
The worker count is the number of cores the process sees, unless
`TOKIO_WORKER_THREADS` sets it.

A server without a page-cache disk level (one is `storage.local` with a
`page_cache_disk_bytes` above zero) that serves a table reading through
the row cache beside tables reading pages alone, ordered tables or
key-value tables with `row_cache = false`, must set `row_cache_share`.
Those tables' pages live in the page cache's memory level alone, and no
default share suits both kinds of table there, so icetray refuses the
file, naming the tables reading pages alone, as `the server has no
page-cache disk level and serves tables reading through the row cache
beside tables reading pages alone (events); set storage.local with
page_cache_disk_bytes above zero, or set server.row_cache_share`. `serve`
refuses it at start with exit 1, the doctor reports it the same way, and
a reload to such a file is refused whole.

### `[server.log]`

Logs are JSON, one object per line, with the standard fields `time`,
`level`, `message`, and `target`
([Operating icetray](operating.md#logs)).

| Key | Type | Default | Meaning |
|---|---|---|---|
| `fields` | table of standard field to string | empty | Renames standard fields: `time = "ts"` emits the time as `ts`. A key other than the four standard fields is refused, and so are two fields mapped to one name. |
| `static_fields` | table of string to string | empty | Constant fields added to every event. |

```toml
[server.log.fields]
time = "ts"
level = "severity"

[server.log.static_fields]
service = "icetray"
```

### `[storage]`

| Key | Type | Default | Meaning |
|---|---|---|---|
| `max_retries` | count | `2` | Retries of a failed S3 request. Zero tries each request once. |
| `connect_timeout` | duration | `"1s"` | The longest an S3 connection may take to open. |
| `catalog_timeout` | duration | `"30s"` | The longest one catalog call may run before the poll or load that made it fails: a `rest` table load, a `glue` `GetTable`, or an `s3tables` `GetTableMetadataLocation`. |
| `s3` | table | absent | Explicit S3 client settings, below. |
| `rewrite` | array of tables | empty | Location prefix rewrites, below. |
| `hedge` | table | hedging on | Hedging of lookup GETs, below. |
| `local` | table | absent | The local directory, below. Required by `disk` residency. |

### `[storage.s3]`

Without this table, S3 requests are signed with credentials from the AWS
SDK's default chain: environment variables, profiles and single sign-on,
credential helpers, web identity, and container or instance roles. The
chain and the region are loaded only when some location, after rewriting,
is `s3://` or `s3a://`, or when a table has a `glue` or `s3tables`
source; a run over `file://` tables from `static` or `rest` sources reads
no AWS configuration and never contacts the instance metadata service.
Calls to Glue and S3 Tables are signed with the same credentials as S3
requests: the static keys here when they are set, otherwise the chain,
which is resolved once for both.

| Key | Type | Default | Meaning |
|---|---|---|---|
| `endpoint` | string | AWS | The endpoint URL, for S3-compatible stores. |
| `region` | string | the SDK's | The region. A `glue` or `s3tables` source that names no region of its own uses it too. |
| `access_key_id` | string | none | A static access key id. Set with `secret_access_key` or not at all. Static keys take precedence over the chain. |
| `secret_access_key` | string | none | The static secret key. icetray never prints it. |
| `allow_http` | boolean | `false` | Permit a plain HTTP endpoint. |
| `path_style` | boolean | `false` | Address buckets as `host/bucket/key` rather than by virtual host. |

### `[[storage.rewrite]]`

Each rule replaces a location prefix before any read, for tables whose
metadata records paths from another host or bucket. icetray tries the
rules in file order on every location the metadata records, and applies
the first rule whose `from` matches.

| Key | Type | Default | Meaning |
|---|---|---|---|
| `from` | string | required | The prefix as the metadata records it. Must not be empty. |
| `to` | string | required | The prefix to read from instead. Must not be empty. |

### `[storage.hedge]`

When a lookup GET has no response after the delay, icetray sends it a
second time and uses whichever response arrives first. Load reads are
never hedged. A `Scan`'s span GETs are hedged under the same settings in
a window of their own per table; every span GET races that window's one
delay, whatever its pages, and enters it with its whole elapsed time.

| Key | Type | Default | Meaning |
|---|---|---|---|
| `enabled` | boolean | `true` | Whether lookup GETs are hedged. |
| `percentile` | number | `0.95` | The delay is this percentile of the table's recent lookup GET latencies. In (0, 1]. |
| `window` | count | `256` | Lookup GETs in each table's latency window. At least 1. |
| `max_rate` | number | `0.05` | The largest share of the lookup GETs in the window that may be hedged. In [0, 1]. |

The ranges are checked even when `enabled` is `false`.

### `[storage.local]`

The optional local directory: the chunk files of `disk` tables and the
page cache's disk level. icetray rebuilds everything in it from object
storage ([Operating icetray](operating.md#the-local-directory)).

| Key | Type | Default | Meaning |
|---|---|---|---|
| `dir` | path | required | The directory. A relative path is taken from the working directory. |
| `page_cache_disk_bytes` | bytes | required | The size bound of the page cache's disk level, which holds the pages of every table, key-value and ordered, that leave memory. `0` turns the level off; any other value is at least one 4 MiB block, 4194304. Without the level, a server that serves tables reading through the row cache beside tables reading pages alone sets `server.row_cache_share` ([`[server]`](#server)). |
| `index_budget_bytes` | bytes | unbounded | The bytes chunk files may occupy across every table. icetray checks it when a load begins and refuses a load that would exceed it as `over_budget`. The footer gives an ordered table's string and binary order fences only by estimate, so icetray reserves each of its chunk files at the exact length before it writes it, and refuses the load then when that length does not fit. |

The disk level holds one open file descriptor per 4 MiB block, so
`page_cache_disk_bytes` sets part of the descriptor limit a replica needs
([Operating icetray](operating.md#file-descriptors)).

### `[[tables]]`

| Key | Type | Default | Meaning |
|---|---|---|---|
| `name` | string | required | The name clients request the table by. Unique and not empty. |
| `ref` | string | required | The Iceberg branch or tag to serve, such as `"main"`. |
| `key_column` | string | required | The key column: a top-level field of the snapshot's schema, which icetray matches in each data file by its name and field id ([Changing the schema](table-layout.md#changing-the-schema)). The name is a valid Avro name: ASCII letters, digits, and underscores, not starting with a digit. Iceberg's Java writers store a column of any other name under an escaped name, so such a name makes the file invalid, as `tables[0].key_column names "user-id", which icetray does not serve: a column name is ASCII letters, digits, and underscores, and does not start with a digit`. |
| `value_columns` | list of strings or tables | required | The value columns icetray serves for the table. Each is a top-level field matched the same way, and icetray reads it only when a request lists it ([Value columns](table-layout.md#value-columns)). An entry is the column's name, `"payload"`, or a table of its `name` and its `format`, `{ name = "detail", format = "protobuf" }`; in JSON, `"payload"` or `{"name": "detail", "format": "protobuf"}`. `format` is `"opaque"`, the default, for bytes icetray returns as stored, or `"protobuf"`, for a serialized protobuf message that icetray can trim to the top-level fields a request lists ([Protobuf columns](table-layout.md#protobuf-columns)). An unknown key in the table form is a schema error reported at its path, as ``tables[0].value_columns[1].fromat: unknown field `fromat`, expected `name` or `format` ``, and so is an unknown format, as ``tables[0].value_columns[1].format: unknown variant `json`, expected `opaque` or `protobuf` ``. The list may be empty, `value_columns = []`, for a table served for key presence only. Each column must be of Iceberg type `binary`, `fixed`, or `string`, and a protobuf column of type `binary`; icetray refuses a snapshot that breaks this as `value_type_unsupported`, naming the column. Each name follows `key_column`'s rule, and a name outside it makes the file invalid, as `tables[0].value_columns names "user-id", ...`. An empty name makes the file invalid, as `tables[0].value_columns lists an empty name`; so does a name listed twice, as `tables[0].value_columns lists "payload" more than once`, and the key column, as `tables[0].value_columns lists the key column "key"`. The order of the list is not significant: a reload that only reorders it changes nothing, one that changes only formats applies to the serving snapshot in place, and one that changes the set of columns loads the table anew. |
| `order_column` | string | none | The order column of an [ordered table](table-layout.md#ordered-tables): a top-level field of Iceberg type `int`, `long`, `date`, `timestamp`, `timestamptz`, `timestamp_ns`, `timestamptz_ns`, `string`, `binary`, or `fixed`, matched as `key_column` is and following its name rule, by which each key's rows are sorted after the key. A table that sets it is an ordered table, which `Scan` and `MultiScan` read; a table without it is a key-value table, which `Get` and `MultiGet` read. Naming the key column makes the file invalid, as `tables[0].order_column names the key column`, and so does listing it in `value_columns`, as `tables[0].value_columns lists the order column "ts"`. Setting, changing, or removing it loads the table anew. |
| `max_scan_rows` | count | `1000` on an ordered table | The most rows one `Scan`, or one key of a `MultiScan`, returns, as long as the response fits `server.max_scan_bytes`; the doctor's `large_runs` advice names each file whose rows at this limit would not. A call sets its limit from 1 to this value: a limit of 0, or one above it, is `INVALID_ARGUMENT`. Set only on an ordered table: on a key-value table it makes the file invalid, as `tables[0].max_scan_rows is set, but the table sets no order_column, and only an ordered table is scanned`. Zero is invalid, and so is a value above half of `server.max_multi_scan_values`, so a `MultiScan` of one key at the full limit and one column is always valid. A change applies to the serving table in place. |
| `row_cache` | boolean | `true` on a key-value table | Whether the table's lookups read each value through the row cache ([The row cache](performance.md#the-row-cache)), which keeps a repeated hit off its value page. Turn it off for a table whose callers keep their own cache in front of icetray, so the row cache's memory goes to tables that reuse it; such a table reads pages alone. Set only on a key-value table: on an ordered table it makes the file invalid, to either value, as `tables[0].row_cache is set, but the table sets an order_column, and only a key-value table reads through the row cache`. A change loads the table anew. A reload that changes whether any table served reads through the row cache resizes the row cache and the page cache's memory level ([`[server]`](#server)). |
| `source` | table | required | Where icetray reads the table's metadata from, below. |
| `lookups` | string | the best level `residency` reaches | The lookup cost the owner accepts for the table: `"optimal"`, `"balanced"`, or `"permissive"`; see [Choosing a level](#choosing-a-level). `full` and `disk` default to `"optimal"` and `bloom` to `"balanced"`. A level the residency cannot reach makes the file invalid, as `tables[0].lookups optimal is above what tables[0].residency bloom reaches; bloom reaches balanced at best`. |
| `strict` | boolean | `false` | Whether icetray refuses a snapshot whose layout falls below the table's `lookups` level. When a strict table's first snapshot is below its level, the start fails; icetray refuses a later one and [holds](glossary.md) the table, and the last snapshot that met the level keeps serving. With `strict` off, icetray serves the snapshot and flags the table below its level. |
| `residency` | string | `"full"` | `"full"`, `"disk"`, or `"bloom"`; see [Choosing a residency](#choosing-a-residency). `"disk"` requires `[storage.local]`; without it the file is invalid, as `tables[0].residency disk needs a local directory under storage.local`. |
| `resident_budget_bytes` | bytes | unbounded | For `full` residency only: the most resident bytes a load may hold, its delete state counted at a bound taken from the manifests. icetray refuses a load that would exceed it as `over_budget` once the footers are read, before it fetches any key chunk or delete. On an ordered table, whose string and binary order fences the footer gives only by estimate, icetray checks the exact resident bytes again once the files are built, and refuses the load then when they exceed it. Set on a `disk` or `bloom` table, it makes the file invalid. |

An ordered table of each user's activity, read by user and timestamp,
whose writer appends between compactions:

```toml
[[tables]]
name = "events"
ref = "main"
key_column = "user_id"
value_columns = ["payload"]
order_column = "ts"
max_scan_rows = 500
lookups = "permissive"

[tables.source]
type = "static"
location = "s3://warehouse/db/events"
```

### `[tables.source]`

The `type` key selects the source kind. On every poll, icetray finds the
table's current metadata, resolves the configured ref in it, and builds a
new [generation](glossary.md) when the ref points to a new snapshot.

| `type` | Keys | What a poll follows |
|---|---|---|
| `"static"` | `location`, required: a table root or a metadata file | For a table root, icetray reads its `metadata/version-hint.text`, as the Hadoop catalog writes it; an unchanged hint costs that one read. A bare version `N` in the hint names `metadata/vN.metadata.json`, or for gzip-compressed metadata `vN.gz.metadata.json` or `vN.metadata.json.gz`, the first that exists, as Iceberg's Java Hadoop tables read it. A location ending in `.metadata.json` or `.metadata.json.gz` is a single metadata file, which never changes, so the table never advances to a new snapshot. |
| `"rest"` | `uri`, required: the catalog's base URI; `warehouse`, optional; `namespace`, required, its levels separated by dots (`"db.schema"`); `table`, required | Each poll loads the table from the catalog, one call within `storage.catalog_timeout`, and follows the metadata location the catalog returns. An unchanged location costs that call and no read; a new one costs one read of its metadata file, for the column defaults the catalog's client does not report ([Changing the schema](table-layout.md#changing-the-schema)). |
| `"glue"` | `database` and `table`, required; `catalog_id`, `region`, and `endpoint`, optional; [below](#glue) | Each poll calls Glue's `GetTable`, one call within `storage.catalog_timeout`, and follows the table's `metadata_location` parameter. An unchanged location costs that call and no read. |
| `"s3tables"` | `table_bucket_arn`, `namespace`, and `table`, required; `region` and `endpoint`, optional; [below](#s3tables) | Each poll calls S3 Tables' `GetTableMetadataLocation`, one call within `storage.catalog_timeout`, and follows the location it returns. An unchanged location costs that call and no read. |

The `rest` source sends no credentials. Its requests carry no
`Authorization` header, so a catalog that requires a bearer token, OAuth2
client credentials, or SigV4 signing cannot be used directly. The source
works with REST catalogs that require no credentials, and with catalogs
behind a proxy that adds them. A catalog that responds to an
unauthenticated request with 401 fails the table's first load, and the
server exits 1. icetray also ignores storage credentials a catalog vends:
every object read goes through `[storage]` and its credentials.

For a `glue` or `s3tables` source, icetray requests only the location of
the table's current metadata file from the catalog. It reads the metadata
file and every file it lists through `[storage]`, its rewrite rules
included, as it does for a `static` table. The catalog calls are signed
with the credentials `[storage.s3]` describes. A call that the catalog
rejects, or that fails, fails the load or poll with the error kind
`catalog`; one that runs past `storage.catalog_timeout` fails it with
`catalog_timeout`.

The region of either source is its own `region` when set, else
`storage.s3.region`, else, for `s3tables`, the region in the table
bucket's ARN. For a `glue` source with none of these, icetray falls back
to the SDK's own region resolution: `AWS_REGION`, the profile, or the
instance metadata service.

#### `glue`

| Key | Type | Default | Meaning |
|---|---|---|---|
| `database` | string | required | The Glue database. Not empty. |
| `table` | string | required | The table in the database. Not empty. |
| `catalog_id` | string | the caller's account | The id of the catalog to read, for a table in another account's catalog. Not empty when set. |
| `region` | string | as above | The region of the Glue endpoint. Not empty when set. |
| `endpoint` | string | AWS | An `http://` or `https://` endpoint URL, for a Glue-compatible service or a VPC endpoint. |

The table must be an Iceberg table: its `table_type` parameter is
`ICEBERG`, in any letter case, and its `metadata_location` parameter holds
the location of the current metadata file. icetray refuses a table of
another type with the error kind `table`, and one without a
`metadata_location` too.

```toml
[tables.source]
type = "glue"
database = "db"
table = "kv"
```

#### `s3tables`

| Key | Type | Default | Meaning |
|---|---|---|---|
| `table_bucket_arn` | string | required | The table bucket's ARN, `arn:aws:s3tables:<region>:<account>:bucket/<name>`. Any other ARN makes the file invalid, as `tables[0].source.table_bucket_arn ... is not a table bucket arn`. |
| `namespace` | string | required | The namespace in the bucket. Not empty. |
| `table` | string | required | The table in the namespace. Not empty. |
| `region` | string | as above | The region of the S3 Tables endpoint. Not empty when set. |
| `endpoint` | string | AWS | An `http://` or `https://` endpoint URL, for an S3 Tables-compatible service or a VPC endpoint. |

icetray refuses a table that has no metadata location in the service,
such as one created and never written, with the error kind `table`.

```toml
[tables.source]
type = "s3tables"
table_bucket_arn = "arn:aws:s3tables:us-east-1:111122223333:bucket/warehouse"
namespace = "db"
table = "kv"
```

A required key left out, or a key outside the source kind's schema, is
reported at `tables[0].source`; an empty value is reported at the
setting's full path, as `tables[0].source.database is empty`.

## Choosing a residency

A table's [residency](glossary.md) sets where icetray stores its key
chunks, and with it the table's memory, its disk use, and its GETs per
lookup. Every residency returns the same results. `icetray info` and
`icetray doctor` print each residency's resident bytes under `predicted`,
computed from the footers alone, so the owner can choose with the numbers
in hand.

| | `full` | `disk` | `bloom` |
|---|---|---|---|
| Memory | `predicted.full`: footers, page indexes, fences, and key chunks | `predicted.disk_memory`: footers, page indexes, and fences | `predicted.bloom`: footers, page indexes, fences, and bloom filters |
| Local disk | none | `predicted.disk_bytes` of chunk files | none |
| Cold hit | 1 GET | 1 local read and 1 GET | 2 GETs: the key page, then the value page |
| Warm hit | none | 1 local read | none |
| Miss | none | 1 local read | none, or 1 GET when an absent key passes the filter |
| Needs | memory for the key chunks | `[storage.local]` on a mounted volume | a bloom filter on the key in every file |

- **`full`**, the default, is the fastest on a hit. Use it whenever
  `predicted.full` fits in the replica's memory beside the page cache.
  With `resident_budget_bytes` set, icetray refuses a table that outgrows
  its share before it fetches the key chunks.
- **`disk`** holds a fraction of a byte per key in memory and stores the
  key chunks in a local file. Use it for tables whose key chunks do not
  fit in memory, on a replica with a local volume. A local read is a
  memory copy while the operating system's page cache holds the chunk
  file, so size the memory limit with room for `predicted.disk_bytes`
  ([Operating icetray](operating.md#memory)).
- **`bloom`** requires no local disk and costs a second GET on every cold
  hit and a GET for each false positive. The share of misses that cost a
  GET depends on the writer's filter size, which
  `predicted.bloom_bits_per_key` reports: at 8 bits per key or more, few
  do. Writers often size the filter per row group rather than per key,
  which can make `predicted.bloom` larger than `predicted.full`; compare
  the two before choosing `bloom`. A `bloom` load fetches the key chunks
  to verify them and holds them until verification ends, so its memory during
  the load exceeds what it retains. Use it when the keys do not fit in
  memory and no local volume can be mounted.

On an ordered table the residency also sets where a `Scan` reads the
order column. `full` and `bloom` fetch its pages by GET beside the value
columns', and `disk` stores its chunks in the chunk file beside the key
chunks, so a cold `Scan` reads it locally and `predicted.disk_bytes`
counts it. Under `bloom` a `Scan` fetches the key pages that bound the
key's run before it reads the rows. Every residency also holds the
order column's offset index, each file's order facts, and its order
fences, one per order page. `predicted` estimates them from the footer,
a string or binary order value at the order chunk's uncompressed bytes
per value, and the budgets charge the exact fences once a file is
built.

The decision is recorded in [RFD 017](rfd/017-residency-tiers.md).

## Choosing a level

A table's `lookups` is the lookup cost its owner accepts. A level is
stated in what a lookup costs, so it means the same on every table:

- `optimal`: a hit costs one GET for each column it reads and a miss
  none, and a lookup probes one file and decodes one key page of ordinary
  size.
- `balanced`: a lookup or `Scan` still probes one file, with a fixed
  extra cost: an extra GET per hit, a GET for a miss that passes the
  bloom filter, larger key pages, or key pages read from the device
  instead of the page cache. The exception is a `Scan` of an edge key
  shared by files one snapshot added, as a compaction that rolls files
  by size writes them, which reads every file that holds the key
  ([Levels of an ordered table](table-layout.md#levels-of-an-ordered-table)).
- `permissive`: any snapshot icetray serves correctly, including one where
  a lookup probes more than one file.

The level a snapshot achieves is the costliest level that its residency
or any of its findings caps it at. `bloom` residency caps it at
`balanced`; the findings that cap it, and at which level, are in [the
optimal layout's requirements](table-layout.md#requirements). Advice
caps nothing. Each value column has a level of its own, which combines
the caps of its own findings, such as `dictionary_encoded_value` on that
column, with those of every finding about the row or the table. The
table's level is its costliest column's, and a table with no value
columns has the level of locating the row. Under `strict`, icetray
refuses the snapshot for the whole table when one column is below the
declared level. The doctor prints the level as `level`, each column's
under `value_columns`, and under `level_by_residency` the level each
residency would reach with the same files, beside what each costs under
`predicted`. A serving `disk` table also falls to `balanced` while the
kernel's page cache holds too little of its chunk files
([Metrics](operating.md#metrics)). That state follows from the server's
memory and is independent of the snapshot, so the doctor does not report
it and `strict` never acts on it.

The line between `balanced` and `permissive` is the one that matters for
operations. A cost within one file follows from how the files are written
and stays constant. Extra files probed follow from how writers commit to
the table, and their number grows with every append until a compaction
merges the files.

Choose the level from the latency the table's readers need and the
layout its writer can produce:

- Keep the default, the best level the residency reaches, when the
  table's lookups are on a latency-sensitive path and its owner should be
  alerted when they get slower.
- Declare `balanced` for a table whose writer cannot turn off dictionary
  encoding on the value column or shrink its key pages. Left at the
  default, such a table is below its level from its first load
  ([What to alert on](operating.md#what-to-alert-on)).
- Declare `permissive` for a table that receives appends with no
  compaction schedule, whose lookups slow down until a compaction runs.

An ordered table's levels are read for a `Scan` of one key, and its
owner chooses them by how the writer commits: `permissive` for a table
that takes appends between compactions, `balanced` for one whose
compaction writes more than one file per bucket, and the default for one
file per bucket ([Levels of an ordered
table](table-layout.md#levels-of-an-ordered-table)).

`strict` sets what icetray does when a snapshot falls below the level.
With `strict` off, the default, icetray serves the snapshot and flags the
table. With it on, icetray refuses a snapshot whose layout is below the
level as `below_level`: at start `serve` exits 1, and on a refresh
icetray holds the table and the last snapshot that met the level keeps
serving ([Refresh and reload](operating.md#refresh-and-reload)). icetray
judges a strict table's level before it fetches any key chunk, from the
manifests, the footers, and the first and last key page of each new file
that shares a bucket with another, with the key's dictionary page when it
has one, so a refused snapshot writes nothing to the local directory.

The decision is recorded in [RFD 020](rfd/020-lookup-levels.md).

## The command line

```
icetray <COMMAND>

Commands:
  serve   Run the gRPC server
  doctor  Check tables against the optimal layout
  get     Look up keys in one table
  scan    Read one key's rows in an ordered table
  info    Describe each table's current snapshot
```

`-h` prints a command's summary and `--help` its full help. `icetray
--help` ends with the environment variables, the exit statuses, and
examples. `icetray --version` prints the version.

```sh
icetray serve --config icetray.toml
icetray doctor --config icetray.toml
icetray info --config icetray.toml --table kv
icetray get --config icetray.toml --table kv --column value 42 43
icetray get --location s3://warehouse/db/kv --value-column payload --column payload 42 43
icetray scan --config icetray.toml --table events --direction newest --limit 10 --column payload 42
```

- `serve` takes `-c, --config <PATH>` and nothing else. It is described
  in [Operating icetray](operating.md).
- `doctor` loads each table, checks it against the
  [optimal layout](table-layout.md), and prints a `table` line, a
  `finding` line per finding, and a `refusal` line for a table that
  icetray refuses. `--files` adds a `file` line per data file. The doctor
  decodes and hashes every key to check that it belongs in its file's
  bucket, a check a load skips, so the doctor takes longer than `info` on
  the same table. For each column declared protobuf it also walks the
  column's default, which costs no GET, and every value on the first
  value page of the first file holding the column, which costs one GET,
  or two for a dictionary-encoded chunk. When that page holds only nulls,
  it moves to the next file holding the column, up to three files. The
  doctor reports a value or default that does not parse as protobuf as
  `malformed_protobuf` advice, and a page it cannot read as an `unwalked`
  line; the rest of the table's report is unaffected.
- `info` loads each table and prints its `table` line.
- `get` loads one table and prints a `lookup` line per key, in the order
  given. It takes a repeatable `--column <NAME[=FIELDS]>`, the value
  columns to read for every key, in the order given. The flag has no
  default: with none, each line reports only whether its key is present,
  and `get` reads no value page. `FIELDS` is a comma-separated list of
  top-level field numbers, such as `--column detail=1,4,9`, for a column
  declared protobuf: `get` trims each value to those fields and prints the
  trimmed bytes in the usual shape. `NAME` alone reads the whole value,
  and so does `NAME=`, the form for a name that contains an `=`. A column
  the table does not serve, one listed twice, a field list for a column
  not declared protobuf, a number outside 1 to 536870911, or one given
  twice exits 64 before the table loads, and so does a field list that is
  not numbers. A value that does not parse as protobuf fails the run with
  exit 1, and the error names the table, column, and key. `get` reads a
  key-value table; on an ordered table, one whose entry sets
  `order_column`, it exits 64 before the table loads, with a hint to use
  `scan`.
- `scan` loads one ordered table and reads one key's rows with one
  `Scan` ([Calling icetray](calling.md#reading-an-ordered-table)),
  printing a [`row` line](#row) per row in the direction asked and then
  one [`scan` line](#scan). It takes `--column` as `get` does, applied
  to every row, and:

  | Flag | Meaning |
  |---|---|
  | `--direction <DIRECTION>` | Required: `newest`, the highest order values first, or `oldest`, the lowest. |
  | `--limit <N>` | Required: the most rows to print, from 1 to the table's [`max_scan_rows`](#tables). |
  | `--start <ORDER>` | Only rows at or above this order value, in the form for the table's order type ([Order values](#order-values)). |
  | `--before <ORDER>` | Only rows below this order value, in the same form. |
  | `<KEY>` | The one key whose rows to read, in the form `get` takes ([Keys](#keys)). |

  The `scan` line's `next`, present when rows remain past the last one
  printed, is in the form `--start` and `--before` take: passed back as
  `--before` reading newest first, or as `--start` reading oldest first,
  with the other flags unchanged, it prints the next page. A key with no
  rows prints only the `scan` line, with `rows` 0, and exits 0. `scan`
  holds the request to the checks a `Scan` meets
  ([Errors and retries](calling.md#errors-and-retries)), and exits 64
  before the table loads for each one that needs no data file: a
  key-value table, with a hint to use `get`; a direction other than
  `newest` or `oldest`; a limit that is not a number, 0, or above
  `max_scan_rows`; a bound or a key that does not parse as the table's
  type; a `--start` at or above `--before`; and a `--column` that `get`
  refuses. A failure the loaded table finds, such as a value that is not
  protobuf, exits 1. `server.max_scan_bytes` bounds a gRPC response,
  which `scan` does not send, so `scan` prints every row its limit
  admits.

### Choosing the tables

The one-shots share one set of flags, grouped in their help as follows.

**Table selection**

| Flag | Meaning |
|---|---|
| `-c, --config <PATH>` | The configuration file. |
| `--location <URI>` | One table opened without a file; see below. Cannot be combined with `--config`. |
| `--rewrite <FROM=TO>` | A location prefix rewrite, repeatable, under either target. |
| `-t, --table <NAME>` | Selects a configured table, so it requires a configuration file. On `doctor` and `info` it limits the run to that table; on `get` and `scan` it is required when the file lists more than one. |

**Located table (with --location)**

| Flag | Default | Meaning |
|---|---|---|
| `--key-column <COLUMN>` | `key` | The key column, a name under `key_column`'s rule. |
| `--value-column <NAME[:FORMAT]>` | none | A value column the table serves, with its format, `opaque` by default or `protobuf`; repeatable. A name holding a `:` takes its format after it, as `a:b:opaque`. An unknown format exits 64. The name follows `key_column`'s rule. With none, the table is served for key presence only. |
| `--order-column <COLUMN>` | none | The order column, which makes the table an [ordered table](#tables) that `scan` reads, at the default `max_scan_rows`. The name follows `key_column`'s rule. With none, the table is a key-value table, so `scan --location` without it exits 64 naming the missing flag. |
| `--ref <REF>` | `main` | The branch or tag to read. |
| `--residency <RESIDENCY>` | `full` | `full` or `bloom`. `disk` requires a local directory, so it is available only through a configuration file. |

**Output** (`doctor` only)

| Flag | Meaning |
|---|---|
| `--files` | Also print one `file` line per data file. |

icetray reads the configuration file named by `--config`, or, with
neither `--config` nor `--location`, by the `ICETRAY_CONFIG` environment
variable. An empty `ICETRAY_CONFIG` counts as unset. A `--location` run
reads no configuration file and ignores `ICETRAY_CONFIG`. With no flag and
no variable, the run exits 64 with
`no configuration: pass --config or --location, or set ICETRAY_CONFIG`.
When a file cannot be read, or does not parse or validate, the error
names it with where its path came from, `--config` or `ICETRAY_CONFIG`.

A `--table` name that matches no configured table exits 64, and so does
`--table` given with `--location`. Both errors print a hint: the first
lists the configured names, and the second says to drop `--table` or to
pass `--config` instead of `--location`.

### One table without a file

`--location` takes a table root or a metadata file, as a URI or as a
local path. An empty `--location`, a `file:` URI with a relative path or
another host, and a location no store serves exit 64. A relative local
path is resolved against the working directory, so
`--location target/golden-warehouse/db/long_2m` works from the repository
root. The table is named after the table root's last directory: `kv` for
`s3://warehouse/db/kv` and for
`s3://warehouse/db/kv/metadata/v3.metadata.json`. A location with no
directory to name the table after, such as a bare bucket, opens as a
table named `table`.

A located table runs with every `[server]` and `[storage]` default. Its
value columns are the ones the `--value-column` flags list, or none, so
the doctor checks only those columns' layout. A located table is a
key-value table unless `--order-column` names its order column; icetray
refuses an ordered table opened without it as `duplicate_keys`, since
its keys repeat. Its
`lookups` is the best level its `--residency` reaches, `optimal` for
`full` and `balanced` for `bloom`, and it is never strict, so the doctor
reports a located table below that level with exit 2 rather than refusing
it.

```sh
icetray doctor --location s3://warehouse/db/kv --value-column value
icetray get --location s3://warehouse/db/kv/metadata/v3.metadata.json --value-column value --column value 42
icetray info --location target/golden-warehouse/db/long_2m --value-column value
icetray scan --location s3://warehouse/db/events --value-column payload --order-column ts --direction oldest --limit 100 --column payload 42
```

### Rewrite rules on the command line

`--rewrite FROM=TO` splits at the first `=`, and both halves must be
non-empty: `--rewrite =x` or `--rewrite x` exits 64, and the error names
the flag and the value. A `TO` that is a relative local path is resolved
against the working directory. Under `--config`, icetray puts the command
line's rules ahead of the file's, so they take precedence for the same
prefix. `serve` takes no `--rewrite`; it reads its rules from the file.

### Keys

Keys are given in the form for the table's key type:

- a long as its digits, with a leading `-` when negative;
- a string as its text;
- a binary or fixed key as hex, two digits per byte in either case, with
  or without the `0x` a lookup line prints;
- a uuid hyphenated or as 32 hex digits.

A [`lookup` line](#lookup)'s `key` is in this form, so passed back to
`get` it looks up the same key, and so is the `key` of a `row` and a
`scan` line.

A key that starts with `-` and is not a number goes after `--`:
`icetray get --location ... -- -abc`. `get` and `scan` read the key type
from the table's metadata and parse every key as that type before they
load any data file, so a key that does not parse exits 64 at once, with a
hint giving the form.

### Order values

`scan --start` and `--before` take an order value in the form for the
table's order type, which `scan` reads from the table's metadata before
it loads any data file:

- an `int`, `long`, `date`, or timestamp as a long in the type's Iceberg
  unit: days since 1970-01-01 for a `date`, microseconds since the epoch
  for `timestamp` and `timestamptz`, and nanoseconds for `timestamp_ns`
  and `timestamptz_ns`, as `Scan` takes it on the wire and the doctor's
  lines print it. The long is one form for every value a column can
  hold, `timestamp` and `timestamptz` alike, so a `row` line's `order`
  and a `scan` line's `next` print it, and a `next` pasted back reads
  exactly the next page;
- a `date` or timestamp also as ISO 8601 text, converted to the same
  long, in exactly one form per type:
  - a `date` as `YYYY-MM-DD`, such as `2026-01-01`, from `0000-01-01` to
    `9999-12-31`;
  - each of the four timestamp types as `YYYY-MM-DDTHH:MM:SS`, an
    optional `.` and one to nine fraction digits, then `Z`, `+HH:MM`, or
    `-HH:MM`, such as `2026-01-01T00:00:00Z` or
    `2026-01-01T09:00:00.5+09:00`, from the year 0000 to
    `9999-12-30T22:00:00.999999999Z`, where jiff, icetray's time
    library, ends, and for `timestamp_ns` and `timestamptz_ns` from
    `1677-09-21T00:12:43.145224192Z` to `2262-04-11T23:47:16.854775807Z`,
    the range a long of nanoseconds spans.

  The `T` and `Z` are uppercase and the year has four digits, so a
  year before 0 is given as the long. The offset is required for
  `timestamp` too, so no local time zone is ever consulted: a
  `timestamp` column stores a wall-clock reading with no zone, and its
  bound is the instant's wall-clock time in UTC, so a writer's local
  time is given with `Z`, as in `2026-01-01T09:00:00Z`, not with its
  offset, which shifts the bound by the offset. Every other form exits
  64: a space for `T`, lowercase `t` or `z`, an hour-only offset, one
  with seconds, or one past 23 hours, a zone annotation such as
  `[Europe/Paris]`, the basic form `20260101T000000Z`, a comma for the
  decimal point, and a time without seconds or without an offset. So
  do a leap second, `:60`, and an instant with digits below the
  column's unit that are not zero, such as `.0000001` for a
  microsecond type: neither is rounded. Only the ISO form is limited
  to these ranges; the long takes every value the column holds, a
  `date`'s whole `int` range of days included. Digits alone are always
  the long: on a `date` column `2026` is day 2026, 1975-07-20, and
  `20260101` is day 20,260,101, some 55,000 years from now, not
  2026-01-01. An `int` or `long` column, and every column below, takes
  no ISO text;
- a `string` as its text, with two escapes: `\\` for a backslash and
  `\0` for a zero byte. No command-line argument can hold a zero byte,
  and a page read oldest first from a string column ends in one: its
  `next` is the last row's value with a zero byte appended
  ([Paging with `next`](calling.md#paging-with-next)). Any other
  backslash exits 64, so `C:\temp` is written `C:\\temp`;
- a `binary` or `fixed` value as hex, two digits per byte in either case,
  with or without `0x`, as a binary key is given.

A bound that does not parse exits 64, its message naming the order type
and the forms it takes, with a hint giving the form. A
[`scan` line](#scan)'s `next` is in this form, a long for a `date` or
timestamp whichever form the bound was given in.

## Output

The one-shots print JSON on stdout, one object per line, and nothing
else. Each line's first key is `kind`, which names what the line
describes, and its second is `table`. Warnings and other log events go to
stderr as JSON lines. A run's final failure goes to stderr as plain text:

```
error: no table named nope is configured
  hint: pass --table with one of: long_2m, str_2m
```

icetray prints the `hint` line when the failure points to a clear fix.
When a reader closes stdout early, as `head` does, the run stops printing
and exits 0.

### The output contract, version 1

The line kinds and keys below are version 1 of the output contract.
Within a version, new keys and new line kinds may be added, so a reader
should ignore what it does not recognize. From icetray 1.0.0, removing or
renaming a key, changing a value's type or meaning, or removing a line
kind makes a new version, which this page will name. Before 1.0.0,
version 1 changes in place with any release
([Stability](../CONTRIBUTING.md#stability)).

Three conventions hold for the lines below:

- A snapshot id is a decimal string, `"7128216514704790069"`, because an
  id above 2^53 loses digits as a JSON number in most readers.
- A key that does not apply is `null` rather than absent, except where a
  line kind below says otherwise, as the `table` line does for `order`
  and `lookup_cost`, and the `file` line for `order`.
- An order value is a JSON number for an order type that travels as a
  long (`int`, `long`, `date`, and the timestamp types), in the type's
  unit; a JSON string for a `string` order column; and a string of `0x`
  and lowercase hex for `binary` and `fixed`. The number is exact as
  text, and a reader that parses JSON numbers as doubles loses precision
  above 2^53, as `timestamp_ns` and `timestamptz_ns` values are, so such
  a reader parses these keys as 64-bit integers. A [`file` line](#file)
  carries order values in `order.min` and `order.max`, a [`row`
  line](#row) in `order`, and a [`scan` line](#scan) in `next`, which
  holds a string's text with its escapes ([Order values](#order-values)).
  When a `doctor --files` or `scan` run prints one whose magnitude exceeds
  2^53 (9,007,199,254,740,992), it logs one `warn` event on stderr for
  the table, with the table's name in `table` and 2^53 in `max_exact`,
  saying such values are exact as JSON text but lose precision in readers
  that parse numbers as doubles. A table is named once per run, however
  many of its lines carry such a value, and stdout is the same with or
  without the event. `doctor` without `--files` and `info` print no
  order value and never warn.

#### `table`

One per table, from `info` and from `doctor`:

```json
{"kind":"table","table":"long_2m","snapshot_id":"7128216514704790069","level":"optimal","lookups":"optimal","residency":"full","file_count":4,"buckets":4,"buckets_with_files":4,"max_files_per_bucket":1,"records":2000000,"index_bytes":2623999,"predicted":{"full":2623999,"disk_memory":239082,"disk_bytes":2405869,"bloom":4433458,"bloom_bits_per_key":16.777504,"key_chunk_bytes":2384917,"filter_bytes":4194376},"level_by_residency":{"full":"optimal","disk":"optimal","bloom":"balanced"},"lookup_cost":{"gets_per_hit":1,"gets_per_miss":0,"files_probed":1},"value_columns":[{"column":"value","format":"opaque","level":"optimal","gets_per_hit":1,"files_predating_column":0,"value_bytes":216000000}],"deletes":{"rows":0,"files":0,"fraction":0.0,"state_bytes":0},"capping":0,"advice":0}
```

| Key | Value |
|---|---|
| `snapshot_id` | The snapshot loaded. |
| `level` | The [level](#choosing-a-level) the snapshot achieves in the loaded residency: `optimal`, `balanced`, or `permissive`, the costliest level that the residency or any finding caps it at, which is its costliest value column's. |
| `lookups` | The table's declared level: its configured `lookups`, or for a located table the best level its residency reaches. |
| `residency` | The residency the table was loaded in. |
| `file_count` | Live data files in the snapshot. |
| `buckets` | The bucket count of the key's bucket transform, `null` for a table without one. |
| `buckets_with_files` | Buckets holding at least one file. |
| `max_files_per_bucket` | Files in the fullest bucket. |
| `records` | Rows across every file, from the manifests. |
| `index_bytes` | Resident bytes of the index in the loaded residency. |
| `predicted.full` | Memory the `full` residency would hold, its delete state counted at the bound the resident budget is checked against. |
| `predicted.disk_memory` | Memory the `disk` residency would hold, its delete state counted at the same bound. |
| `predicted.disk_bytes` | The chunk files the `disk` residency would write, as `index_budget_bytes` counts them. |
| `predicted.bloom` | Memory the `bloom` residency would hold, its delete state counted at the same bound; `null` when a file has no key bloom filter. |
| `predicted.bloom_bits_per_key` | Filter bits per row; `null` when `predicted.bloom` is. |
| `predicted.key_chunk_bytes` | The key column's compressed bytes. |
| `predicted.filter_bytes` | The key column's bloom filter bytes; `null` when `predicted.bloom` is. |
| `level_by_residency` | For each residency that can load the table, the level its files would reach there. A residency that cannot load the table is left out, as `bloom` is when `predicted.bloom` is `null`. |
| `lookup_cost` | Absent on an ordered table, which serves no lookup and prices its `Scan` under `order` instead. Otherwise an object of the three keys below. |
| `lookup_cost.gets_per_hit` | GETs one cold hit makes at worst in the loaded residency, locating the row and reading the costliest value column, as `icetray_table_gets_per_hit` reports it while the snapshot serves. For a table with no value columns, locating the row alone. |
| `lookup_cost.gets_per_miss` | GETs one miss makes at worst, as `icetray_table_gets_per_miss` reports it. |
| `lookup_cost.files_probed` | Files one lookup probes at worst, as `icetray_table_files_probed_per_lookup` reports it. |
| `value_columns` | One object per configured value column, in name order, names compared as their UTF-8 bytes; empty for a table with none. |
| `value_columns[].column` | The column's name. |
| `value_columns[].format` | The column's format as configured in the table's entry: `opaque` or `protobuf`. |
| `value_columns[].level` | The level a lookup requesting the column achieves, as `icetray_table_column_level` reports it. |
| `value_columns[].gets_per_hit` | The value-page GETs a cold hit spends on the column alone, 1, or 2 when a file dictionary-encodes it, as `icetray_table_column_gets_per_hit` reports it. A request's cold cost is `lookup_cost.gets_per_miss` plus the figures of the columns it requests. On an ordered table, the GETs a cold `Scan` spends on the column's span, which `order.gets_per_scan` sums over the configured columns. |
| `value_columns[].files_predating_column` | Data files written before the column was added, for which a lookup returns the column's default. |
| `value_columns[].value_bytes` | The column chunks' uncompressed bytes in the snapshot, as each data file's footer records them, page headers and a dictionary page's compressed extent included, summed over the files; a file written before the column was added contributes none. The same bytes `value_columns[].avg_value_bytes` of `doctor --files` divides by each file's rows. Compare it with the row cache's share of `server.cache_bytes`, which every table the server serves shares, to see how much of the column the budget could hold. |
| `deletes.rows` | Rows the snapshot's deletes remove from the data files it holds, as `icetray_table_deleted_rows` reports it while the snapshot serves. |
| `deletes.files` | Position delete files and deletion vectors whose rows the load applied, as `icetray_table_delete_files` reports it. |
| `deletes.fraction` | `deletes.rows` over `records`, as `icetray_table_deleted_fraction` reports it. |
| `deletes.state_bytes` | Bytes the delete state holds, counted in `index_bytes`, as `icetray_table_delete_state_bytes` reports it. |
| `capping` | The number of findings that cap the level. |
| `advice` | The number of advice findings. |
| `order` | Present only on an ordered table: an object of the keys below. |
| `order.column` | The order column's name. |
| `order.order_type` | The order column's Iceberg type in the snapshot: `int`, `long`, `date`, `timestamp`, `timestamptz`, `timestamp_ns`, `timestamptz_ns`, `string`, `binary`, or `fixed(N)` with its width. |
| `order.max_scan_rows` | The table's `max_scan_rows`, configured or the default. |
| `order.max_multi_scan_values` | `server.max_multi_scan_values`, the most values one `MultiScan` may request, as `TableInfo` reports it. |
| `order.max_scan_bytes` | `server.max_scan_bytes`, the encoded size above which a `Scan` or `MultiScan` response is refused, as `TableInfo` reports it. |
| `order.longest_run` | The rows of the longest run of one key in any one file. A key that several files hold, as appends between compactions leave it, has more rows than its longest run in one file. |
| `order.gets_per_scan` | GETs one cold `Scan` at `max_scan_rows` makes at worst in the loaded residency, of a key held in one file and one row group, reading every configured value column, as `icetray_table_gets_per_scan` reports it while the snapshot serves. The figure an owner compares residencies by, in place of `lookup_cost`. A key held in several files costs this in each. |
| `order.files_probed` | Files one `Scan` reads at worst, as `icetray_table_files_probed_per_lookup` reports it. |

#### `finding`

One per finding, from `doctor`, capping findings first:

| Key | Value |
|---|---|
| `caps` | The level the finding caps the table at, `balanced` or `permissive`; `null` for advice. |
| `code` | The finding's code, such as `large_key_pages`; the codes are in [the optimal layout's requirements](table-layout.md#requirements). |
| `message` | What was measured, for a person. |
| `file` | The data file's location, or `null` for a table-level finding. |
| `column` | The value column the finding is about, for `dictionary_encoded_value`, `large_value_pages`, `value_pages_over_ceiling`, `column_default`, and `malformed_protobuf`; `null` for a finding about the row or the table. |
| `penalty` | `null` for advice. For a capping finding, an object of `extra_gets_per_hit`, `keys_decoded_per_lookup`, and `files_probed_per_lookup`, each a number or `null` when the finding does not affect it. On an ordered table, `extra_gets_per_hit` counts the extra GETs one `Scan` makes in each file it reads, and `files_probed_per_lookup` the files one `Scan` reads. |
| `property` | The writer property that corrects the layout, or `null`. A per-column property is printed up to the column name, such as `write.metadata.metrics.column.`. |

#### `file`

One per data file, from `doctor --files`:

| Key | Value |
|---|---|
| `path` | The file's location as the manifest records it. |
| `bucket` | The bucket the file belongs to. |
| `key_type` | `long`, `string`, `binary`, or `uuid`. |
| `rows`, `row_groups` | Rows and row groups in the file. |
| `key_pages` | Data pages of the key column across every row group. |
| `max_key_page_rows` | Rows in the largest key page: the keys a lookup there decodes. |
| `max_key_bytes` | The longest key, in bytes. |
| `value_columns` | One object per configured value column, in name order, names compared as their UTF-8 bytes. |
| `value_columns[].column` | The column's name. |
| `value_columns[].predates_column` | Whether the file was written before the column was added, so a lookup returns the column's default for its rows; its counts are then zero. |
| `value_columns[].value_pages` | Data pages of the column across every row group. |
| `value_columns[].avg_value_page_bytes` | Mean uncompressed bytes of the column's value page: what a cold hit requesting it decompresses. |
| `value_columns[].avg_value_bytes` | Mean uncompressed bytes of one of the column's values. |
| `value_columns[].pages_aligned` | Whether every page of the column starts on the same row as a key page. |
| `dictionary_columns` | The columns, key or value, that are dictionary-encoded in a row group. |
| `bloom_bytes`, `bloom_bits_per_key` | The key's bloom filter size and bits per row; `null` when a row group has none. |
| `key_chunk_bytes` | The key column's compressed bytes. |
| `distinct_page_mins_ratio` | The share of the distinct first keys of the key pages that the key column's index still tells apart; low when the column index is truncated. On a key-value table every page starts a new key, so this is the share of key pages with a distinct column-index minimum; on an ordered table a key's pages share one minimum without truncation, so it is 1.0 unless a minimum is cut. |
| `bounds_present` | Whether the manifest carries full-length key bounds. |
| `resident_bytes` | The bytes this file's index holds in the loaded residency. |
| `order` | Present only on an ordered table: an object of the keys below, which the load recorded from every order value in the file. |
| `order.min`, `order.max` | The least and greatest order value over all the file's rows, deleted rows included, in the form [above](#the-output-contract-version-1). |
| `order.longest_run` | The rows of the file's longest run of one key. |
| `order.distinct_keys` | Per row group, in the footer's order, the distinct keys it holds: what the bloom findings divide its filter bits by. |

#### `refusal`

One per table the doctor refused, after any `table` and `finding` lines
for it:

```json
{"kind":"refusal","table":"unpart","reason":"below_level","message":"table unpart refused: below_level (lookups optimal, the layout achieves permissive: no_bucket_partition)"}
```

| Key | Value |
|---|---|
| `reason` | A refusal reason from [the optimal layout's requirements](table-layout.md#requirements) and [an ordered table's](table-layout.md#requirements-of-an-ordered-table) (`unsorted`, `null_keys`, `duplicate_keys`, `duplicate_rows`, `null_order`, `key_outside_bucket`, `key_type_mismatch`, `key_type_unsupported`, `order_type_unsupported`, `order_type_mismatch`, `column_mismatch`, `value_type_unsupported`, `data_format_unsupported`, `equality_deletes`, `delete_kind_unsupported`, `key_too_long`, `over_budget`, `no_bloom_filter`); `below_level` for a strict table whose layout is below its `lookups`, with the codes that cap it in `message`, a column's finding with its column, such as `dictionary_encoded_value (column payload)`; or `load_failed` when an error rather than a rule stopped the load. |
| `error_kind` | Present only with `load_failed`: what failed, one of `iceberg`, `parquet`, `object_store`, `object_store_path`, `json`, `config`, `location`, `cache`, `key_type`, `column_request`, `scan_request`, `response_too_large`, `wrong_table_kind`, `malformed_value`, `no_next_bound`, `table`, `file`, `not_ready`, `local`, `catalog_timeout`, `catalog`, `load_deadline`, `refresh_deadline`, `verify_canceled`, `lookup_canceled`. |
| `message` | The error, for a person. |

`reason` and `error_kind` use the same names as the `reason` label of
`icetray_refresh_errors_total`.

#### `unwalked`

One per column declared protobuf whose value page the doctor could not
read, after the table's `finding` lines. The doctor leaves the column's
values unchecked and does not refuse the table for it; the table's other
lines set the exit code:

```json
{"kind":"unwalked","table":"kv","column":"detail","error_kind":"object_store","message":"object store: Object at location db/kv/data/00000-0.parquet not found"}
```

| Key | Value |
|---|---|
| `column` | The column whose page was not read. |
| `error_kind` | What failed, one of the `error_kind` values of `refusal`. |
| `message` | The error, for a person. |

#### `lookup`

One per key, from `get`:

```json
{"kind":"lookup","table":"long_2m","key":"42","presence":"present","values":{"value":{"base64":"NzM0NzVj…","utf8":"73475cb4…","bytes":192}},"snapshot_id":"7128216514704790069","gets":1,"elapsed_micros":342}
{"kind":"lookup","table":"long_2m","key":"43","presence":"absent","snapshot_id":"7128216514704790069","gets":0,"elapsed_micros":0}
```

| Key | Value |
|---|---|
| `key` | A string holding the key in the form `get` takes for it ([Keys](#keys)): a long as its digits, with a leading `-` when negative; a string as its text, with no escapes but JSON's own (`"abc"`, so `jq -r .key` gives `abc`); a binary key as `0x` and lowercase hex; a uuid hyphenated. Passed back to `get`, it looks up the same key. |
| `presence` | `present` or `absent`. |
| `values` | For a present key, an object keyed by the `--column` names in the order given, each value an object or `null` for a null value; `{}` when no column was named. Absent for an absent key. |
| `values.<column>.base64` | The value in standard base64 with padding. |
| `values.<column>.utf8` | The value as text. Present only when its bytes are UTF-8. |
| `values.<column>.bytes` | The value's length in bytes; `0` for a zero-length value, which is distinct from `null`. |
| `snapshot_id` | The snapshot the lookup read. |
| `gets` | The GETs this lookup issued. |
| `elapsed_micros` | The lookup's time, in microseconds. |

#### `row`

One per row, from `scan`, in the direction asked:

```json
{"kind":"row","table":"events","key":"42","order":1760918400000000,"values":{"payload":{"base64":"eyJ0eXBlIjoi…","utf8":"{\"type\":\"…","bytes":96}}}
```

| Key | Value |
|---|---|
| `key` | The key, in the form `get` and `scan` take ([Keys](#keys)). |
| `order` | The row's order value, in the form [above](#the-output-contract-version-1): a string as the value itself, without `scan`'s escapes. |
| `values` | An object keyed by the `--column` names in the order given, each value as a [`lookup` line](#lookup)'s, or `null` for a null value; `{}` when no column was named. |

#### `scan`

One per run, from `scan`, after its `row` lines:

```json
{"kind":"scan","table":"events","key":"42","rows":10,"next":1760918340000000,"snapshot_id":"7128216514704790069","gets":3,"elapsed_micros":512}
```

| Key | Value |
|---|---|
| `key` | The key, in the form `get` and `scan` take ([Keys](#keys)). |
| `rows` | The `row` lines printed. |
| `next` | Present when rows remain past the last one printed: the next page's bound, in the form `--start` and `--before` take ([Order values](#order-values)), a number for an integer or time order type and a string otherwise. Reading newest first it is the last row's order value, passed back as `--before`; reading oldest first the least value above it, passed back as `--start`. Absent on the last page. |
| `snapshot_id` | The snapshot the `Scan` read. |
| `gets` | The GETs the `Scan` issued. |
| `elapsed_micros` | The `Scan`'s time, in microseconds. |

## Exit codes

The same codes apply to every command; the root help's footer lists them.

- **0**: the run completed what was requested. An absent key is a valid
  result, so a `get` of absent keys exits 0. The doctor exits 0 when every
  table meets its `lookups` level; advice does not change the code, and
  neither does a capping finding within the declared level. `serve` exits 0 after a drain that
  finished within `server.drain_deadline`.
- **1**: the run failed. A configuration file that cannot be read, is not
  TOML or JSON, does not fit the schema (a key missing, unknown, or of the
  wrong type), or breaks a rule (no table listed, a `lookups` level its
  residency cannot reach, a rewrite rule with an empty half, an empty or
  unservable location, a bad log field mapping, and the rest); a relative
  path on the command line with no readable working directory; a load or
  read error; a table refused, which for the doctor is a `refusal` line,
  `below_level` under `strict` among them, and takes precedence over 2;
  and for `serve`, a first load that failed, a listener that failed to
  bind, or a drain that passed its deadline.
- **2**: the doctor found a table below its `lookups` level that it did
  not refuse: a configured table with `strict` off, the default, or a
  `--location` table, which is never strict. A strict table below its
  level is refused instead, exit 1, as `serve` would refuse it.
- **64**: the command line could not be used: no configuration; both
  `--config` and `--location`; a `--location` that is empty, a `file:` URI
  with a relative path or another host, or one no store serves; a
  located-table flag without `--location` or with `--config`; a
  `--residency` other than `full` or `bloom`; a malformed `--rewrite`;
  `--table` with `--location`; a `--table` that no configured table has;
  `get` or `scan` without `--table` when several tables are configured;
  `get` of an ordered table or `scan` of a key-value one; `scan` with
  `--location` and no `--order-column`; a key or a
  `scan` bound that does not parse; a `--column` the table refuses; or a
  `scan` direction, limit, or window that a `Scan` refuses.