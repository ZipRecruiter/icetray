# Calling icetray

A service calls icetray over gRPC with a client generated from
[`icetray.proto`](../crates/icetray-proto/proto/icetray.proto), whose
comments are the reference this page summarizes. There are five calls.
`Get` looks up one key and `MultiGet` many keys from one snapshot, in a
key-value table. `Scan` reads one key's rows in order and `MultiScan`
many keys' rows from one snapshot, in an [ordered
table](#reading-an-ordered-table). `TableInfo` describes a table the
server serves, and a table whose `order_column` it reports set is an
ordered table. The [glossary](glossary.md) defines terms such as
snapshot and residency.

The server rejects most malformed requests as `INVALID_ARGUMENT`, but a
few rules are the caller's to keep, and the server cannot detect a break
in them. Each column a request lists adds a GET to a cold hit, whether
the caller uses the value or discards it. A caller that reads an absent
key as a null value, or a null as a zero-length value, misreads the row.
A projection that lists only one member of a oneof can return a member
the whole value cleared, and a projected message without its required
fields fails a strict parse. A retry of an error that recurs fails again
and adds load. So a caller requests only the columns, and for a protobuf
column only the fields, that it uses; reads presence and null as separate
facts; lists every member of a oneof and parses a projected value
partially; and retries only the transient errors.

## Connecting

icetray serves gRPC on the address `server.listen` sets
([Configuration](configuration.md#server)), in plaintext, with the
standard `grpc.health.v1` health service for readiness. The port
authenticates no client, so a deployment provides encryption and access
control with a private network, a service mesh, or a sidecar.

Generate a client from `icetray.proto` with your language's usual
toolchain. The file declares the package `icetray.v1` and sets no
language package options, so a Go build maps it to an import path with
`protoc-gen-go`'s `M` option or a `buf.gen.yaml` override. The server
offers no reflection, so pass the file to a tool such as
[grpcurl](https://github.com/fullstorydev/grpcurl) with `-proto`.

## Looking up a key

A `Get` request carries the table, the key, and the value columns to
read:

```sh
grpcurl -plaintext -proto crates/icetray-proto/proto/icetray.proto \
  -d '{"table": "profiles", "key": {"long": "42"}, "columns": [{"name": "embedding"}]}' \
  127.0.0.1:7433 icetray.v1.Icetray/Get
```

```json
{
  "presence": "PRESENCE_PRESENT",
  "values": [
    {"value": "AAAAPwAAgD8…"}
  ],
  "snapshotId": "7128216514704790069"
}
```

The key has three variants, and the caller sets the one that matches the
table's key type, which `TableInfo` reports: `long` for a long key, `str` for a
string key, and `bin` for a binary, fixed, or uuid key, carrying its
bytes (a uuid as its sixteen bytes). The caller encodes a composite key
into `bin` itself.

A response states two separate facts: whether the key is present, and,
for a present key, whether each value is null. Presence is
`PRESENCE_PRESENT` or `PRESENCE_ABSENT`, and a miss is a successful call.
The response for a present key carries one value per column requested,
in the order requested, and the response for an absent key carries none.
Each value is either the column's bytes, in `value`, or a null, in
`null_value`. A zero-length value is a `value` holding no bytes, distinct
from a null. The table's owner defines what a null means.

The same call in Go, with the package generated from `icetray.proto`
imported as `icetrayv1`:

```go
resp, err := client.Get(ctx, &icetrayv1.GetRequest{
	Table:   "profiles",
	Key:     &icetrayv1.Key{Key: &icetrayv1.Key_Long{Long: 42}},
	Columns: []*icetrayv1.ColumnRead{{Name: "embedding"}},
})
if err != nil {
	return err
}
if resp.GetPresence() == icetrayv1.Presence_PRESENCE_PRESENT {
	embedding := resp.GetValues()[0]
	if embedding.GetNullValue() == nil {
		use(embedding.GetValue())
	}
}
```

## Choosing columns

`TableInfo` lists the value columns a table serves, in name order, each
with its type (`binary`, `fixed` with its width, or `string`) and its
format (`opaque`, or `protobuf` for a column whose values a request may
project to some of their fields). A request lists each column it reads,
and the server reads no column by default.

Request only the columns the caller uses. On a cold hit, each requested
column costs one GET to object storage (two for a column the writer
dictionary-encoded), and the server issues the GETs for one key
concurrently, so a hit waits on the slowest of them. A request that lists
no column returns only the key's presence and reads no value. The
table's owner decides how to split a row into columns, as
[Value columns](table-layout.md#value-columns) describes.

A key-value table serves a repeated hit from memory: the server keeps
each value a lookup reads in its row cache, so a later lookup of the same
key and column reads no value page and issues no value-page GET,
whichever fields it projects. The first lookup of a key after a server
starts, or after its row has left the cache, costs what a cold or warm
hit does. A caller that keeps its own cache of icetray's answers can ask
the table's owner to turn the row cache off for the table, so the
server's memory goes to tables whose callers do not
([The row cache](performance.md#the-row-cache)). A value a refresh
deletes or rewrites is never answered from the cache.

## Projecting protobuf fields

A column the table's owner declares `protobuf` holds serialized protobuf
messages, and a request may project its value to a list of top-level
field numbers, so the caller receives and decodes only those fields. The
server projects a value after its page cache and its row cache, so
projection saves bytes
on the wire and in the caller's decode, and costs the same GETs as
reading the whole value:

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

An empty `fields` returns the whole value. An empty `columns`, one level
up, returns no value.

Two rules are the caller's to keep, since icetray holds no schema and
cannot check them:

- **List every member of a oneof, or none.** Setting one member of a
  oneof clears the others. When a value sets one member and later
  another, a projection listing only the first returns it set, with no
  error.
- **Parse partially when the message has required fields.** A projected
  value lacks every field the request did not list, including a proto2
  `required` field or a `LEGACY_REQUIRED` field under editions, so parse
  with `proto.UnmarshalOptions{AllowPartial: true}` in Go,
  `parser().parsePartialFrom` in Java, or `ParsePartialFromString` in C++.

The projected value keeps every occurrence of each listed field, in the
value's order, and parses as the caller's message type: each listed field
reads as it does in the whole value, and every other field reads as
unset, which is its zero value for a field without presence. The server
returns a value that holds none of the listed fields as a `value` of zero
bytes, and returns a null as a null.

The server accepts a number the message does not define and returns
whatever the value carries under it, which the caller's runtime keeps as
unknown fields. A value that does not parse as protobuf fails the call as
`FAILED_PRECONDITION`, as [Errors and retries](#errors-and-retries)
describes.

Take field numbers from the generated code. Java generates a constant
per field, such as `Profile.TITLE_FIELD_NUMBER`. In Go, the descriptor
reports each number:

```go
fields := (&profilepb.Profile{}).ProtoReflect().Descriptor().Fields()
title := uint32(fields.ByName("title").Number())
```

## Looking up many keys

A `MultiGet` request carries a list of keys and one list of columns,
applied to every key. The server reads every key from one snapshot and
returns the results in request order, with one snapshot id for the
batch, so the results are consistent with each other. When the lookup of
any key fails, the whole call fails, so every response covers every key,
and the error is that of the first failing key in request order
([Errors and retries](#errors-and-retries)).
The caller then retries the whole batch when the error's row in
[Errors and retries](#errors-and-retries) says to. On
`FAILED_PRECONDITION`, the caller drops the key the error names or reads
that column whole.

A batch may request up to `server.max_multi_get_values` values
([Configuration](configuration.md#server)), counting its keys times its
columns, with a presence-only batch counting one per key. The server
rejects a larger batch as `INVALID_ARGUMENT` before it looks up any key,
so a caller with more values splits them across batches. The server caps the GETs a batch holds
in flight at `server.batch_fan_out`
([Configuration](configuration.md#server)), so a large batch
takes longer and its load on object storage stays within that cap.

## Reading an ordered table

An ordered table holds many rows per key, each with an order value, such
as a time series by key and timestamp or the activity of each user. The
table's owner configures its order column, and `TableInfo` reports it in
`order_column`, its type in `order_type`, its width, for `fixed`, in
`order_width`, the most rows one `Scan` returns in `max_scan_rows`, the
most values one `MultiScan` may ask for in `max_multi_scan_values`, and
the most bytes one `Scan` or `MultiScan` response may encode to in
`max_scan_bytes`. A key-value table reports an empty `order_column` and a
`max_scan_rows`, `max_multi_scan_values`, and `max_scan_bytes` of 0. These
limits can change while a caller holds them: the owner changes a table's
`max_scan_rows` in place with a reload, and `max_multi_scan_values` and
`max_scan_bytes`, which are the server's, with a restart. A caller that
keeps them re-reads `TableInfo` when a call it sized by them fails as
`INVALID_ARGUMENT` for its limit or its values, or as
`FAILED_PRECONDITION` for its response size. The server rejects a `Get`
or `MultiGet` of an ordered table, and a `Scan` or `MultiScan` of a
key-value table, as `INVALID_ARGUMENT`. It decides this from the table's
configuration, so it answers `INVALID_ARGUMENT` even while the table is
still loading, where a call of the right kind meets `UNAVAILABLE` until
the load completes; a caller does not retry it.
[Errors and retries](#errors-and-retries) gives the order in which the
server checks a request.

A `Scan` request carries the table, the key, the value columns to read
as `Get` reads them, a direction, a limit, and optional bounds:

```sh
grpcurl -plaintext -proto crates/icetray-proto/proto/icetray.proto \
  -d '{"table": "events", "key": {"long": "42"}, "columns": [{"name": "payload"}], "direction": "DIRECTION_NEWEST_FIRST", "limit": 2}' \
  127.0.0.1:7433 icetray.v1.Icetray/Scan
```

```json
{
  "rows": [
    {"order": {"long": "1760400060000000"}, "values": [{"value": "CgVjbGljaw…"}]},
    {"order": {"long": "1760400000000000"}, "values": [{"value": "CgR2aWV3…"}]}
  ],
  "next": {"long": "1760400000000000"},
  "snapshotId": "7128216514704790069"
}
```

- **Direction.** `DIRECTION_NEWEST_FIRST` returns the highest order
  values first, `DIRECTION_OLDEST_FIRST` the lowest. The request must set
  one.
- **Limit.** The most rows to return, from 1 to the table's
  `max_scan_rows`. The request must set it: a limit of 0, like one above
  `max_scan_rows`, is `INVALID_ARGUMENT`, and a caller that wants as many
  rows as one response may hold sets it to the `max_scan_rows` that
  `TableInfo` reports. The response must also fit `server.max_scan_bytes`
  ([Response size](#response-size)), so a caller whose values are large
  sets a limit that fits.
- **Bounds.** The bounds are half-open: `start` is inclusive and `before`
  exclusive, so a row is returned when its order value is at or above
  `start` and below `before`. Either may be left unset. When both are set,
  `start` must be below `before`: a `start` at or above `before` is an
  empty window, and `INVALID_ARGUMENT`.
- **Order values.** An order value is an `OrderValue` whose variant
  follows the order type: `long` for `int`, `long`, `date`, and the
  timestamp types, `str` for `string`, and `bin` for `binary` and
  `fixed`. A date travels as days since the epoch, a `timestamp` or
  `timestamptz` as microseconds, and a `timestamp_ns` or
  `timestamptz_ns` as nanoseconds. Integer and time values compare as
  signed 64-bit integers, and strings and bytes as their bytes. A bound
  outside an `int` or `date` column's range admits every row on its side.

The response carries the key's rows in the requested direction, each
with its order value and one value per requested column, in the order
requested, read as a `Get` response's values are. No two rows of one
response share an order value. `next` is set exactly when the key has
rows within the bounds past the last row returned, and it is the bound
of the next page ([Paging with `next`](#paging-with-next)). A response
whose last row is the key's last within the bounds leaves it unset, even
when it holds `limit` rows. For a key with no rows, the server returns
no rows and leaves `next` unset, and that is a successful call. A
request that lists no columns returns the order values alone.

The same call in Go:

```go
resp, err := client.Scan(ctx, &icetrayv1.ScanRequest{
	Table:     "events",
	Key:       &icetrayv1.Key{Key: &icetrayv1.Key_Long{Long: 42}},
	Columns:   []*icetrayv1.ColumnRead{{Name: "payload"}},
	Direction: icetrayv1.Direction_DIRECTION_NEWEST_FIRST,
	Limit:     100,
})
```

In each file that holds the key, a cold `Scan` costs a GET per
contiguous span of uncached pages of each requested column over the rows
it reads, so a caller requests only the columns it uses. How many files
hold a key follows from how the writer commits to the table ([Levels of
an ordered table](table-layout.md#levels-of-an-ordered-table)), and a
`Scan` reads every one of them ([Performance and
tuning](performance.md#what-a-scan-costs)).

### Response size

The server refuses to send a `Scan` or `MultiScan` response whose encoded
size is above `server.max_scan_bytes`
([Configuration](configuration.md#server)), which `TableInfo` reports in
`max_scan_bytes`. Its default is set to a gRPC client's default limit on a
message it receives. The call fails as `FAILED_PRECONDITION`,
with no rows, and its message names the response's size, the bound, and
how many rows of a `Scan`, or how many leading keys' results of a
`MultiScan`, would have fit. The size is the
exact encoded size of the response message, which is what a client's
message limit measures.

Nothing about the same request changes on a retry, so the caller lowers
its `limit` or requests fewer columns. The rows reported to fit are
counted with the `next` a response of that many rows would carry, so a
`Scan` at that limit is within the bound. A caller that sets a limit of
100 and pages with `next` (below) reads the same history in pages of at
most 100 rows, each of which fits when 100 rows of the requested columns
come to less than the bound. An owner whose clients accept larger
messages raises `server.max_scan_bytes`
([Configuration](configuration.md#server)) and the clients raise their
own receive limit to match, as grpc-go's `MaxCallRecvMsgSize` does; the
doctor's `large_runs` advice tells the owner which files, and so which
tables, would be refused at the full limit.

When no row fits, the first row's values alone are above the bound, and
a lower `limit` cannot help. The caller requests fewer columns, or trims
a protobuf column to the fields it uses. To step past that row instead,
it reads its order value with a `Scan` that lists no columns at a limit
of 1, and steps past it as [Paging with `next`](#paging-with-next)
describes for a malformed value. When not even the first key's result of
a `MultiScan` fits, the caller reads that key with a `Scan`, whose refusal
counts the rows that fit.

## Paging with `next`

A caller reads a history longer than one response in pages. Each
response that leaves rows behind carries `next`, the bound of the next
page, and the caller passes it back unchanged:

- **Newest first,** `next` is the order value of the last row returned,
  the oldest, and the next request sets `before` to it.
- **Oldest first,** `next` is the least order value above the last row
  returned, the newest: one more for an integer or time type, and the
  row's value with one zero byte appended for a string, binary, or
  `fixed` type, so for `fixed` it is one byte longer than the width. The
  next request sets `start` to it.

Every other field, the other bound included, stays the same. The caller
stops when a response has no `next`, so it never asks for a page past
the last row:

```go
req := &icetrayv1.ScanRequest{
	Table:     "events",
	Key:       &icetrayv1.Key{Key: &icetrayv1.Key_Long{Long: 42}},
	Columns:   []*icetrayv1.ColumnRead{{Name: "payload"}},
	Direction: icetrayv1.Direction_DIRECTION_NEWEST_FIRST,
	Limit:     100,
}
for {
	resp, err := client.Scan(ctx, req)
	if err != nil {
		return err
	}
	for _, row := range resp.GetRows() {
		use(row.GetOrder().GetLong(), row.GetValues()[0])
	}
	if resp.GetNext() == nil {
		return nil
	}
	req.Before = resp.GetNext() // reading oldest first: req.Start = resp.GetNext()
}
```

The caller does not compute the bound itself, so paging works the same
for every order type, a string or bytes order value included.

A key and order value appear together at most once among a snapshot's
live rows, so within one snapshot `next` is an exact cursor: the next
page starts at the row after the last one returned, with no row repeated
or skipped, and the server keeps no state between requests. A refresh
between two requests moves the next page to the new snapshot, whose id
the response carries. The next page holds every row of the new snapshot
past the last row returned, a row the writer committed there included,
so nothing between two pages is skipped. A row the writer committed on
the side the caller has already read appears only to a `Scan` that
starts over, so a caller that needs every row of one snapshot compares
the ids and starts over when they differ.

A caller steps past one row, such as a row whose value is not the
protobuf its column declares (`FAILED_PRECONDITION`, which names the
row's order value), in two reads, since the call that fails returns no
rows, not even the ones before the bad row. The first read takes those
rows, between the bound the page started from and the bad row: newest
first, `start` set to the least value above the bad row's order value
and `before` kept; oldest first, `before` set to the bad row's order
value and `start` kept. The second goes on past the bad row, its bound
built as `next` is built: newest first, `before` set to the bad row's
order value and `start` kept; oldest first, `start` set to the least
value above it and `before` kept. The least value above an order value
is the one `next` would carry: one more for an integer or time type, and
the value with one zero byte appended for a string, binary, or `fixed`
type. An integer or time value at its type's greatest has none, and no
row lies above it, so the read that would start there is skipped.
Neither read returns the bad row, and together they return every other
row of the window. Either read whose `start` would not be below its
`before` holds no rows and is skipped, and either may carry `next`,
which the caller follows as for any page.

## Scanning many keys

A `MultiScan` request carries a list of keys and one set of columns,
direction, limit, and bounds, applied to every key. The server reads
every key from one snapshot and returns one result per key in request
order, each with its own rows and `next`, with one snapshot id for the
call. For a key listed twice, the server returns a result at each
position. When the read of any key fails, the whole call fails, as for
`MultiGet`.

A `MultiScan` may request up to `server.max_multi_scan_values` values
([Configuration](configuration.md#server)), which `TableInfo` reports in
`max_multi_scan_values`.
They are counted as its keys times its limit times one more
than its columns, the one being the order value. A `MultiScan` sets its
limit per key, as a `Scan` does: a limit of 0 is `INVALID_ARGUMENT`. The
server rejects a larger call as
`INVALID_ARGUMENT` before it reads any key. A `MultiScan` at the
table's full `max_scan_rows` with one column covers
`server.max_multi_scan_values` divided by twice `max_scan_rows` keys, and
one with a limit of 20 and one column covers
`server.max_multi_scan_values` divided by 40. A caller
reading many keys requests a smaller limit and pages each key whose
result has `next` set with a `Scan` of that key, passing that `next` as
its bound, and a caller reading one key's full limit over
several columns uses `Scan`. The GETs one `Scan` or `MultiScan` holds in
flight are capped at `server.batch_fan_out`, as for `MultiGet`.

The whole `MultiScan` response is held to `server.max_scan_bytes`
([Response size](#response-size)), and a key listed twice counts at each
position among the leading keys' results that would have fit. The caller
requests fewer keys or a smaller limit.

## Snapshots and consistency

Every `Get` and `Scan` response carries the id of the snapshot the server
read it from, and every `MultiGet` and `MultiScan` response carries one
for the call. A table
advances when its writer publishes a new snapshot, and icetray swaps the
new one in at once, so two calls made a moment apart can read from
different snapshots. A caller that combines responses from several calls
and needs them consistent compares their ids, and repeats the call with
the older id when they differ. `TableInfo` reports the snapshot being
served.

## Errors and retries

Three codes are transient and worth a retry: `RESOURCE_EXHAUSTED` with
backoff, `UNAVAILABLE`, and `DEADLINE_EXCEEDED` when the caller's own
deadline allows. Every other code recurs until the request, the value,
or the snapshot changes. `RESOURCE_EXHAUSTED` is the server's overload
alone: a response too large is never sent, but refused as
`FAILED_PRECONDITION`. A client library raises an error of its own for a
response above its receive limit, `RESOURCE_EXHAUSTED` in grpc-go and
grpc-java and `OUT_OF_RANGE` in tonic; it is not the server's and not
worth a retry, and the client raises its limit, or the caller lowers its
`limit`.

| Code | Meaning | Retry |
|---|---|---|
| `NOT_FOUND` | The server does not serve a table of that name. | No |
| `INVALID_ARGUMENT` | The request is invalid: a missing key, a key variant that does not match the table's key type, a uuid key that is not sixteen bytes, a column the table does not serve or one listed twice, a field list for a column that is not protobuf, a field number outside 1 to 536,870,911 or one listed twice, a `MultiGet` or `MultiScan` over its values limit, a `Scan` or `MultiScan` that sets no direction, sets a limit of 0 or one above the table's `max_scan_rows`, or sets a bound of the wrong variant, a bound with no variant, or a `start` at or above `before` (an empty window), or a call of the wrong kind for the table (`Get` or `MultiGet` of an ordered table, `Scan` or `MultiScan` of a key-value table). | No; fix the request |
| `FAILED_PRECONDITION` | A request listed fields of a value that is not protobuf. The error names the table, column, and key, and for a `Scan` the order value of the row that holds the value, which a caller paging the key's rows steps past ([Paging with `next`](#paging-with-next)). Or a `Scan` or `MultiScan` response would encode to more than `server.max_scan_bytes` ([Response size](#response-size)). | No; it recurs until the writer rewrites the value. Read that column whole, or drop the key. For a response size, lower `limit` or request fewer columns or keys, or, when nothing fits, as [Response size](#response-size) describes. |
| `RESOURCE_EXHAUSTED` | The server is at `server.max_in_flight` calls and refused this one at once. | Yes, with backoff |
| `UNAVAILABLE` | A read from object storage or local disk failed, the table is not ready yet, or a lookup was canceled. | Yes |
| `DEADLINE_EXCEEDED` | A `Get`, or a page fetch within any lookup, ran past `server.get_deadline`, a `MultiGet` ran past `server.multi_get_deadline`, a `Scan`, or a GET within any `Scan` or `MultiScan`, of a span of pages or of a key page under `bloom`, ran past `server.scan_deadline`, or a `MultiScan` ran past `server.multi_scan_deadline`. [Configuration](configuration.md#server) gives each deadline. | Yes, if the caller's own deadline allows |
| `INTERNAL` | The table's data is faulty, such as a malformed file, or two live rows of a key at one order value, which a load refuses. | Only after the table advances to another snapshot |

The server checks a `Get`, `MultiGet`, `Scan`, or `MultiScan` request in
one order and answers the first fault it finds:

1. Faults in the request itself: a missing key, no direction or one the
   contract does not name, a bound with no variant, a limit of 0, a
   `start` at or above a `before` of the same variant, a column listed
   twice, and a field number outside 1 to 536,870,911 or listed twice.
   `INVALID_ARGUMENT`.
2. Faults against the table's configuration: first a table the server
   does not serve (`NOT_FOUND`), then a call of the wrong kind, a limit
   above `max_scan_rows`, and a `MultiGet` or `MultiScan` over its
   values limit (`INVALID_ARGUMENT`).
3. The server's in-flight limit: `RESOURCE_EXHAUSTED` when all
   `server.max_in_flight` slots are taken.
4. The table's readiness: `UNAVAILABLE` until its first load completes.
5. Faults that need the loaded table: a key variant that does not match
   the key type, a uuid key that is not sixteen bytes, a bound of another
   variant than the order type takes, a column the table does not serve,
   and a field list for a column that is not protobuf.
   `INVALID_ARGUMENT`.

So the faults of steps 1 and 2 come back whatever the table's state,
while it loads included, and cost no in-flight slot. The one exception
is a reload that lowers `max_scan_rows` while a call is checked: a limit
the server admitted under the old value can then be refused, still
`INVALID_ARGUMENT` and naming the new value, after the call took its
slot. A request
with a fault of step 5 is `UNAVAILABLE` while the table loads and
`INVALID_ARGUMENT` once it serves, so a caller that retries
`UNAVAILABLE` sees the fault after the load.

A `MultiGet` or `MultiScan` that passes these checks and then fails a
read answers the error of its first failing key in request order,
whichever key's read failed first. The code a caller decides its retry
on does not depend on how the server scheduled the reads, and when that
error is `FAILED_PRECONDITION`, it names that key. A failing call waits
on the keys before its first failing one, up to their fetch deadline:
a key earlier in the request whose read stalls answers its own
`DEADLINE_EXCEEDED` once its fetch passes `server.get_deadline` (a
`MultiGet`) or `server.scan_deadline` (a `MultiScan`), even when a later
key has already failed with a code that is not retried.

The gRPC layer enforces a deadline the client sends and reports its
expiry to the client (a tonic client sees `CANCELLED`), and the server
drops the call at that moment.

The gRPC layer also answers two faults of a request before the server
reads it, with codes of its own:

- A request message above 4 MiB encoded, the server's receive limit, is
  `OUT_OF_RANGE`, as a `MultiGet` or `MultiScan` of many long keys can
  be. The caller splits it into smaller calls.
- A request whose `Key.str` or `OrderValue.str` is not valid UTF-8 does
  not decode, and is `INTERNAL`. The fault is the request's, not the
  table's, so the caller fixes the request rather than waiting for the
  table to advance.

The server hedges a slow GET to object storage with a second one to cut
tail latency, under the settings in
[`[storage.hedge]`](configuration.md#storagehedge), so a caller can leave
hedging a `Get` or a `Scan` to the server.
