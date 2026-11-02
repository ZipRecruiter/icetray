# Glossary

The terms the other pages use, each defined once.

**Advice.** A doctor finding that names a writer property worth changing
for memory, load time, or tuning. Advice caps no level and never affects
whether icetray serves a table. See [the optimal layout](table-layout.md).

**Degraded.** The state of a table whose last refresh failed. icetray
keeps serving the last good generation,
`icetray_table_state{state="degraded"}` reads 1, and the next poll tries
the refresh again. A failure of the snapshot itself, a refusal or a fault
of its metadata or data files, is tried again only once the table's
source names another metadata location or a reload changes the table's
entry. Readiness does not change. A strict table whose snapshot is
refused for its level is held instead. See
[Refresh and reload](operating.md#refresh-and-reload).

**Delete state.** The in-memory record of the rows that a generation's
deletion vectors and position delete files remove from each of its data
files. icetray keeps it resident under every residency, and a lookup
checks it before reading a value page. Its bytes count toward
`icetray_index_bytes`, and a `full` table's resident budget counts it at
a bound taken from the manifests. See
[RFD 021](rfd/021-merge-on-read.md).

**Deletion vector.** A roaring bitmap of the deleted row positions of one
data file, stored as a blob in a Puffin file, which Iceberg format v3
writers use in place of position delete files. icetray applies them to
every lookup.

**Fence array.** The array of the first key of every key page in a data
file, resident under every residency. A lookup binary-searches it to find
the one key page that can hold its key.

**Generation.** One snapshot of a table as icetray serves it: the
snapshot's data files with their resident index. icetray builds a new
snapshot's generation beside the serving one and swaps it in whole, so
every response comes from exactly one snapshot and carries its id.

**Held.** The state of a strict table whose newest snapshot icetray
refused as `below_level`, while it keeps serving the last snapshot that
met the level. `icetray_table_state{state="held"}` reads 1. Held is
distinct from degraded, which follows a failed refresh: a writer's layout
causes it. It ends at the next swap, as when a compaction commits a
snapshot that meets the level, when the ref returns to the serving
snapshot, or when a reload relaxes `lookups` to a level the refused
snapshot meets or turns `strict` off. See
[Refresh and reload](operating.md#refresh-and-reload).

**Key chunk.** The compressed bytes of a data file's key column in one row
group. A load reads every key chunk once to verify the keys; `full`
residency keeps them in memory and `disk` residency in a chunk file.

**Level.** A class of lookup cost, one of `optimal`, `balanced`, or
`permissive`. The owner configures the level a table must meet as its
`lookups`, and icetray measures the level each snapshot reaches.
`optimal` means one GET per column a hit reads and none per miss, with a
lookup probing one file and decoding one key page of ordinary size;
`balanced` means one file probed at a fixed extra cost, except a Scan of
a shared edge key; `permissive` covers any snapshot icetray serves
correctly, including one where a lookup probes several files. Residency,
the doctor's capping findings, and, for a serving `disk` table, the
kernel's page cache determine the level reached. Each value column has a level, the
table's is its dearest column's, and `icetray_table_below_level` reads 1
while a table is below its configured level. See [Choosing a
level](configuration.md#choosing-a-level).

**Next bound.** The order value a `Scan` response, or each key's result
of a `MultiScan`, carries as `next` when the key has rows inside the
bounds past the last row returned, and leaves unset otherwise. It
follows the last row returned: reading newest first it is that row's
order value, sent as the next request's `before`, and reading oldest
first the least order value above it, sent as the next request's
`start`. See [Paging with
`next`](calling.md#paging-with-next).

**One-shot.** A subcommand that opens tables, prints JSON lines, and exits:
`doctor`, `get`, `scan`, and `info`. Each loads its tables the way `serve`
does.
See [the command line](configuration.md#the-command-line).

**Optimal layout.** The Iceberg and Parquet layout at which icetray
serves a table at one GET per column a cold hit reads and none per miss:
`bucket(N, key)` partitioning, files sorted by key, 1,000-row key pages,
dictionary encoding off on each value column, and a bloom filter on the
key, among other requirements. A table in the optimal layout reaches the
`optimal` level under `full` and `disk` residency. icetray serves other
layouts at a cost the doctor reports, and refuses a layout it would serve
incorrectly. See [the optimal layout](table-layout.md).

**Order column.** The column of an ordered table by which each key's
rows are sorted after the key, configured as `order_column`: an Iceberg
`int`, `long`, `date`, timestamp, `string`, `binary`, or `fixed` column,
such as a timestamp, a sequence number, or a version. A key and order
value appear together at most once among a table's live rows, so a
caller pages by order value, with the next bound each response carries.
See
[Reading an ordered table](calling.md#reading-an-ordered-table).

**Order fence.** For each order page of an ordered table's data file,
the shortest pair of key and order value that sorts above the last row
of the page before it and at or below the page's first row. A load
records the fences from the rows, and icetray keeps them in memory under
every residency. A `Scan` places each bound on one order page by a binary
search over them, and a refresh compares them to choose the order pages
that can hold a pair another file holds. See
[RFD 025](rfd/025-ordered-rows.md).

**Ordered table.** A table configured with an order column, which holds
many rows per key and which `Scan` and `MultiScan` read, newest or oldest
first, under a limit and optional half-open bounds on the order value:
`start` inclusive and `before` exclusive. Its writer
sorts each file by key and then order value. Every other table is a
key-value table, which holds at most one row per key and which `Get` and
`MultiGet` read. See [Ordered tables](table-layout.md#ordered-tables).

**Projection.** The trimming of a value in a column declared protobuf to
the top-level field numbers a request lists, which icetray performs after
the row cache (see [RFD 026](rfd/026-row-cache.md)) and the page cache, so
the caches hold whole values. It saves the bytes on the wire and the
caller's decode, and makes the same GETs as reading the whole value. See
[Protobuf columns](table-layout.md#protobuf-columns).

**Refused.** The outcome for a snapshot icetray will not serve, for a
named reason: a lookup could return a wrong result (unsorted files,
duplicate keys, equality deletes, and the like), icetray cannot hold the
table as its residency requires, or, under `strict`, the snapshot's
layout is below the table's level (`below_level`).

**Residency.** The configured storage of a table's key chunks: `full` (in
memory), `disk` (in a chunk file in the local directory, with footers,
page indexes, and the fence array in memory), or `bloom` (the key
column's bloom filters in memory in place of the key chunks). See
[Choosing a residency](configuration.md#choosing-a-residency).

**Run.** The consecutive rows of one key in one data file of an ordered
table, sorted by order value. A run may span key pages and row groups,
and a `Scan` reads one run in each file that holds its key.

**Shared edge key.** In an ordered table, a key that is one file's last
key and the next file's first, among files of one bucket that one
snapshot added, as a compaction that rolls files by size writes them. A
`Scan` of such a key reads every file that holds it, and the doctor
reports it as `shared_edge_keys`, which caps the table at `balanced`.

**Strict.** A per-table setting
([Configuration](configuration.md#tables)) under which icetray
refuses a snapshot whose layout is below the table's level; without it,
icetray serves such a snapshot and flags the table. icetray keeps serving
a strict table's last snapshot that met the level, and a strict table
whose first snapshot is below it fails to start.

**Value column.** A Parquet column that icetray returns as bytes, listed
in the table's `value_columns` and read only when a request lists it. A
table configured with no value columns returns only whether a key is
present. Each column a cold hit requests costs a GET of its own, so one
packed column performs best. See
[Value columns](table-layout.md#value-columns).
