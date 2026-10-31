# Lookup hot path

> Updated 2026-10-07: a lookup reads the value page of each column it
> names, concurrently, and none for a presence-only lookup; a column a
> file answers by its default costs no read. See RFD 022.

> Updated 2026-10-07: a lookup that names fields of a protobuf column
> walks each such value's top-level fields after decoding it, linear in
> the value's bytes, and returns the fields named. See RFD 024.

> Updated 2026-10-14: a Scan of an ordered table finds the key's run
> by decoding at most two key pages per row group it spans, places its
> bounds on order pages by the order fences, then reads the order column
> and each requested column over the rows it reads, one GET per
> contiguous span of uncached pages, in one round. Whole pages of
> deleted rows between the rows it reads split a column's span, and one
> GET fetches them, undecoded, with the spans beside them when fetching
> them costs less than the GET it saves. See RFD 025.

> Updated 2026-10-14: a lookup and a Scan read a value page of plain
> byte arrays, in a column neither nested nor repeated and with no
> dictionary page, with icetray's own plain reader, which slices each
> value from the decompressed page in place, where the column reader
> builds and reference-counts each value it decodes. The column reader
> reads every other page, and any page the plain reader cannot read
> exactly. See RFD 025.

> Updated 2026-10-30: a key-value lookup reads each value it names from
> the row cache before the page cache, and a hit reads no value page.
> The open question of a decompressed-page cache is answered by caching
> rows instead. See RFD 026.

## Problem

RFD 004 (Resident key-chunk index) settles what stays in memory for each
data file, and the page cache settles where a value page comes from.
Between them sits the work a lookup does with the bytes it already
holds. It has to find the key's row inside one key page and then pull
one row out of the value page of each column the lookup names. Two
things about that work are undecided: which decoder does it, and what
happens when the decoder wants a byte it was not given.

Both bear on what a lookup spends. The workloads icetray is built for
are billed per GET and are meant to pay nothing for a miss ([workload
archetypes, the cost
shape](../analysis/2026-09-09-workload-archetypes.md#the-cost-shape)),
and RFD 004 promises one GET per named column on a cold hit and none per
miss, a promise the prototype kept on every layout ([prototype findings,
finding
1](../analysis/2026-09-10-prototype-findings.md#1-one-get-per-cold-hit-none-per-miss-on-every-layout)).
A general-purpose Parquet reader fetches whatever it needs, and a single
such fetch turns an exact count into an estimate. The decoder also sets
the CPU a lookup spends beside its GET.

## Decision

Lookups will decode keys with the Parquet column reader, and values with
icetray's plain reader where a value page holds plain byte arrays and with
the column reader otherwise, over the in-memory byte segments they are
handed (a resident key chunk, a page from
the page cache, or a key page from a disk-tier chunk file), keeping Arrow
out of the path and treating any attempt to read a byte outside those
segments as an error rather than a fetch.

The path is the same for every table whose key chunks are resident:

1. The fence array picks the one key page that can hold the key.
2. The column reader decodes that page from the resident key chunk and
   either finds the row or answers the miss. A miss ends here, before any
   value page is read, and so does a presence-only lookup, which names no
   column.
3. For each column the lookup names (RFD 022 (Value columns)), a
   key-value table's row cache supplies the value when it holds the row's
   value for that column (RFD 026 (Row cache)), and otherwise that
   column's offset index locates its value page and the page cache
   supplies it, the columns concurrently. A cold read costs one GET per
   column, a warm one costs none, and concurrent lookups for one page
   share one GET. A column the file answers by its default (RFD 023
   (Column resolution)) reads nothing.
4. One row is decoded from each page read, by the plain reader for a
   page of plain byte arrays in a column neither nested nor repeated and
   with no dictionary page, and by the column reader for every other
   page, and its value is inserted into the row cache; the values return
   in the order the lookup named their columns.
5. A value whose read names fields of a column declared protobuf (RFD
   024 (Field projection)) is walked over its top-level fields and
   trimmed to the ones named, after the row cache and the page cache, so
   the cached value and page stay whole for every caller. The walk is
   linear in the value's bytes and costs no read; a value it cannot walk
   fails the lookup.

Two more segments are intended reads, counted as reads rather than raised
as errors. One is a value column's dictionary page when one exists, which
is the extra GET that RFD 007 (Serving tiers) counts as its penalty. The other is the key page
itself under the disk and bloom tiers of RFD 017 (Residency tiers), which
fixes where that page comes from and what it costs.

The column reader is handed exactly those segments, and a request for
anything beyond them fails the lookup with an error. That keeps cost
accounting exact. The only GETs are the ones the design intends, so the
request counter of RFD 003 (One storage stack) and the penalties of RFD 007
(Serving tiers) describe the server as it actually runs.

## Alternatives Considered

The Arrow record-batch stream per lookup. It assembles a whole batch to
hand back one row, and it puts Arrow in the hot path. On the same fixtures
the column reader was faster in every case
([prototype findings, finding 4](../analysis/2026-09-10-prototype-findings.md#4-lookup-latency)).

A hand-written decoder for every encoding, over the key chunk and the
value pages. It would have to re-grow the encodings, compression, and
definition-level handling the column reader already has, and every layout
the doctor accepts would need its own path. The plain reader covers one
layout, the plain byte-array value page most value columns use, where it
slices each value in place where the column reader builds and
reference-counts it, and it hands every other page, and any page it
cannot read exactly, to the column reader.

Decoded key arrays kept in memory, so that no key page is decoded per
lookup. Deferred: RFD 004 (Resident key-chunk index) leaves them open
pending a per-table memory budget, and that budget is itself undecided.

A reader that fetches on demand. It would serve every layout without
ceremony, and in doing so it would hide the GETs that make one layout
cheaper than another.

## Consequences

Per-lookup CPU is one key page decoded and one value page decompressed per
named column the row cache does not hold, and, for a projected read, a
walk of the one value, small beside the page it came from. On a warm hit
of one column from the page cache nearly all of the time goes to decoding
those two pages, and little to the lookup itself;
which page dominates depends on whether the values compress ([prototype
findings, finding
4](../analysis/2026-09-10-prototype-findings.md#4-lookup-latency)). That is
why RFD 002 (Serving layout) bounds value pages by bytes for large
values, and why large key pages are an inefficient finding measured in RFD
007.

A layout the resident index does not cover fails loudly, as an error in
the counting tests of RFD 014 (Test layering), instead of showing up as an
unexplained GET in production.

This RFD left open a decompressed-page cache level, and RFD 026 (Row
cache) answers it with a cache of rows instead. The case for the level
was that decompression is most of a warm hit, so caching decompressed
pages would bring a repeated hit down to the lookup logic alone. The
case against was that a decompressed page costs its compression ratio in
extra memory, so the same budget holds fewer pages. Whether hot pages
repeat often enough to pay for that depended on the page cache's
hit-rate curve under a skewed key distribution
([prototype findings, what was not verified](../analysis/2026-09-10-prototype-findings.md#what-was-not-verified)).

Decompression is over half of a warm hit in the Full tier when the values
do not compress, and nearly all of one when they compress well
([performance sweep, decompression](../analysis/2026-09-26-performance-sweep.md#decompression-the-open-question)).
It is most of a miss either way, since a miss decompresses only the key
page. The value page is the smaller part of a warm hit with incompressible
values, where the key page's decode dominates, and most of one with
compressible values. By estimate from the sweep, the level would
multiply the throughput per core of warm hits on compressible values
several times and barely change it for incompressible ones. It would save
nothing on a miss or on a cold hit's GET.

What the level costs is the ratio. For values that do not compress that
is nothing. For values that do, each page takes its compression ratio in
extra memory, so the same budget holds that many times fewer pages. A page
the smaller cache loses comes back as a cold hit, one S3 GET of around a
tenth of a second for a file not read recently
([cold range-GET latency](../analysis/2026-09-29-cold-range-get-latency.md)),
against tens of microseconds that the level saves on one warm hit of
compressible values. One extra cold hit undoes more than a thousand warm
hits' savings, so the level pays only while it turns well under one lookup
in a thousand into an extra cold hit. A row cache skips the same
decompression on a hit and keeps a hot row without its cold neighbors, so
the same memory holds more of the hot set than pages, decompressed or
not, and a hit-rate curve under skewed keys showed rows ahead of pages at
every budget tried (RFD 028 (Cache budget)).
