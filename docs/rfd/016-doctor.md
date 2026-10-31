# Doctor

> Updated 2026-10-04: cost findings are called inefficient findings, as
> RFD 007 now names their tier, and a finding line's `tier` reads
> `inefficient`.

> Updated 2026-10-04: the doctor judges a configured table under its
> configured `strict`, so a table `serve` would refuse is reported
> refused, with exit 1, after its findings.

> Updated 2026-10-04: a refusal's reason is only ever a refusal code; a
> table whose load fails with an error rather than a rule is refused as
> `load_failed`, with the error's kind beside it.

> Updated 2026-10-05: the doctor reports lookup levels in place of
> serviceability, and its exit codes follow the declared level. See RFD
> 020 (Lookup levels).

> Updated 2026-10-07: the advice `column_default` reports how many data
> files were written before the value column was added and answer its
> default, and such a file draws no value-column finding, since it holds
> no such column. See RFD 023.

> Updated 2026-10-07: the value-column findings and the advice
> `column_default` are reported per configured column and name it, every
> finding line carries a `column` field, and the table and per-file
> reports list each value column, a change to the doctor's output
> contract. Each column has a level and the table's is the dearest
> column's. See RFD 022.

> Updated 2026-10-07: the `doctor` command, and not the check at every
> load, walks the default and the values of one value page of each
> column declared protobuf and reports one that is not protobuf as the
> advice `malformed_protobuf`, naming the column; the table's report
> gives each column's format. See RFD 024.

> Updated 2026-10-15: on an ordered table the doctor prices a Scan in
> place of a lookup, its `table` line carrying an `order` object in place
> of `lookup_cost` and each `file` line the file's order facts; the
> ordered findings and advice are reported with their penalties, and the
> bloom findings divide a row group's filter bits by its distinct keys.
> See RFD 025.

> Updated 2026-10-30: the doctor reads a configuration file through the
> checks `serve` applies, so it refuses as `serve` does a file with no
> page-cache disk level that serves a table reading through the row
> cache beside one reading pages alone and sets no
> `server.row_cache_share`, before a deploy; and the table's report
> gives each value column's bytes in the snapshot, from the footers it
> already reads, so an owner can compare a table's hot share with the
> cache budget. See RFD 028.

## Problem

RFD 002 (Serving layout) leaves to this RFD what the doctor
checks and how it reports, and RFD 007 (Serving tiers) leaves it the exit
code a build pipeline can gate on. The check is necessary because a
writer's output cannot be inferred from its properties. The prototype
found writer properties ignored without error
([prototype findings, finding 7](../analysis/2026-09-10-prototype-findings.md#7-two-iceberg-1101-writer-properties-are-no-ops)),
a row limit that left value pages of many megabytes
([finding 6](../analysis/2026-09-10-prototype-findings.md#6-a-row-limit-alone-does-not-bound-page-size-for-large-values-and-both-writers-check-the-byte-limit-lazily)),
and a batch writer at default settings splitting a bucket into overlapping
sorted files
([finding 9](../analysis/2026-09-10-prototype-findings.md#9-spark-writes-several-sorted-files-per-bucket-sometimes-overlapping)).
A table builder needs the verdict before a table ships. The server needs
it at every generation load, because `strict` turns an inefficient
finding into a refusal, and a refusal needs a named reason. One checker
has to answer both, or the two answers drift apart.

## Decision

A `doctor` command, and the same check at every generation load, will
report the lookup level a table's layout achieves, the inefficient
findings that cap it with their penalties, and its advice with the writer property to change, against
thresholds that are measured defaults kept in one documented table.

The refusal reasons and inefficient findings are RFD 007's lists,
grouped by its tiers, and the doctor reports each inefficient finding
with the penalty RFD 007 assigns it. The advice covers missing or
truncated manifest bounds, large value pages, value pages over the
lookup's value-page ceiling (RFD 004 (Resident key-chunk index)), long
keys, large data files, a truncated column index, more files per bucket
than the table's size and file-size target explain, a key bloom filter
that is absent, oversized, or undersized for its row group's key count,
reported as the filter's bits per key, and data files written before a
value column was added, which answer its default (RFD 023 (Column
resolution)). Such a file does not hold that column, so it draws no
finding for it, and its rows cost no value-page GET for it, which the
level and the cost gauges, predicting the worst case, leave out. The
optimal layout's [requirements](../table-layout.md#requirements) are the
documented table, and they give each threshold.

A table serves the value columns its configuration lists (RFD 022
(Value columns)), and
the findings about a value column (dictionary encoding, value-page size,
the value-page ceiling, and `column_default`) are made per configured
column and name it: on the finding's line, in a `below_level` refusal,
and in the `column` label of `icetray_findings`. A finding about the row
or the table, such as key pages, buckets, overlap, or bloom filters,
names no column. Each column has a level, its own findings' caps with
the row's and the residency's best level; the table's level is the
dearest column's, since any column may be asked for, and a table of no
value columns has the level of finding the row. The table's report lists
each configured column with its level, its cost, and its bytes in the
snapshot, summed from the footers, which an owner compares with the
cache budget (RFD 028 (Cache budget)), and the per-file report
lists each column's value pages and whether the file answers it by its
default. The table's report gives each column's format as its entry
declares it (RFD 024 (Field projection)).

Only the command walks values. For each column declared protobuf it
walks the default the column states (RFD 023), with no GET, and every
value on the first value page of the first file that holds the column,
one GET past the page cache (two for a dictionary-encoded chunk), as a
projection would, passing to the next file that holds the column when
that page holds only nulls, up to a fixed number of files ([the command
line](../configuration.md#the-command-line) gives it). A value or
default that does not walk is the advice `malformed_protobuf`, naming
the column and, for a page, the file, which catches a column declared
protobuf that holds another format. A page the command cannot read is
reported for its column, and the table's report and judgment stand
without it. A malformed value on a page the walk does not read is found
by the first lookup that names fields of it. The check at every load
reads no value page, so it does not walk, and the `findings` gauge
carries no series for that code.

The value-page line concerns bytes fetched per hit. That cost grows
linearly with the page's size and has no step anywhere, so the line is
drawn where the transfer itself reaches the order of a round trip
instead of at the optimal layout's value-page bound. The bound remains
the recipe for large values, where a row limit alone leaves pages of
megabytes, and the doctor reports each column's mean value size beside
its mean page size so an owner sees how many values a hit fetches for
the one it wanted.

The ceiling line warns of refusals rather than slowdowns: a lookup into a
value page over the ceiling is refused with a file fault. It is reported
as advice for two reasons. RFD 007's penalty units measure answers served,
and a refused lookup serves none. And its trigger is too coarse for a
refusal: it is the file's mean value page size, the dictionary page's
bytes included (the footer gives no uncompressed size for that page on its
own), which can miss one oversized page among small ones and can pass the
ceiling on a large value dictionary when no page does.

An advice names the Iceberg or Parquet writer property that fixes it,
where one exists. Long keys and many files per bucket come from the
owner's data and its compaction, which no writer property sets, so those
two name none. The summary also carries each residency tier's resident
bytes, computed from footers alone, so an owner chooses a tier (RFD 017
(Residency tiers)) from one run
([bloom residency tier, results](../analysis/2026-09-10-bloom-residency-tier.md#results)).

The command exits 0 for a table that meets its declared lookup level
(advice does not change the code), 2 for one below it that would be
served, and 1 with the refusal reason when the table cannot be served.
The reason is always a refusal code: one of RFD 007's refusal reasons,
`below_level` under `strict`, or `load_failed` when an error rather than a rule stopped
the load (an object store failure, a table that cannot be opened), with
the error's kind reported beside it. A pipeline that gates on the reason
then matches a closed set of rules, and a new error kind lands under
`load_failed` instead of reading as a new rule. A table from a
configuration file is judged under its configured `strict`, as `serve`
judges it: a strict table below its level is reported with every
finding and then refused as `below_level`, with exit 1, so the doctor
never passes with 2 a table that `serve` refuses. The file itself goes
through the checks `serve` applies before any table loads, so a file
`serve` refuses, such as one mixing tables that read through the row
cache with tables that read pages alone, with no page-cache disk level
and no `server.row_cache_share` (RFD 028), is refused by the doctor
too. A table located directly, with no configuration, declares the best
level its residency reaches and is never strict, since nothing
configures it to be.

Only the doctor checks every key's bucket. After the load it decodes every
key of every bucketed file, hashes it under the file's own bucketing, and
refuses the table under RFD 007's `key_outside_bucket` when a key falls
outside the bucket the file's manifest entry records, with that refusal's
line and exit code. Such a key answers absent though it is present, which
makes it a refusal rather than an inefficiency or advice. The load hashes only each
file's fences and its first and last key (RFD 007), since hashing every
row would add tens of nanoseconds per key to every load, while the doctor
runs before a table ships and can afford the time. At load the same
findings decide the level, honor `strict`, and are reported under the names
RFD 007 fixes. The command is a subcommand of the serving binary (RFD 012
(Deployment contract)).

The thresholds live in one table, shared by the documentation and the
code, and each row cites the analysis behind it. The key-length rows rest
on [finding 13](../analysis/2026-09-10-prototype-findings.md#13-key-length),
and the file-size row on
[range-GET latency, what the evidence implies](../analysis/2026-09-10-s3-range-get-latency.md#what-the-evidence-implies),
which leaves the exact point inside an unmeasured band to judgment. RFD
014 (Test layering) supplies the fixture that exercises each finding.

## Alternatives Considered

Checks only in the subcommand. Rejected: the server must know the tier at
load to honor `strict`, and only the check at load sees the snapshot
actually served.

A pass/fail checker. Rejected by RFD 007: layouts other than the optimal
one serve correctly at the same GET count
([finding 1](../analysis/2026-09-10-prototype-findings.md#1-one-get-per-cold-hit-none-per-miss-on-every-layout)),
so a single bit would either refuse tables that work or hide the penalty
of tables that are inefficient.

Thresholds as configuration. Rejected for v1: the thresholds are measured
facts about object-store latency and Parquet decoding, and a deployer who
moved them would make a finding's name mean something different at each
site.

## Consequences

Table builders get a verdict and a fix before a table ships, and a
pipeline gets a loud failure from the exit code before the table reaches
a server, where `strict` would refuse it at load. Key pages above the
optimal layout's 1,000 rows and within the `large_key_pages` threshold
draw no finding. The slack is a chosen margin, so a writer whose page
rounding overshoots the optimal layout does not trip the doctor.

The thresholds rest on a few measurements from limited vantage points
([range-GET latency, what this does and does not establish](../analysis/2026-09-10-s3-range-get-latency.md#what-this-does-and-does-not-establish)).
Keeping them in one cited table makes a moved threshold a one-row edit
with its evidence beside it, and it also makes a wrong threshold wrong
everywhere at once.

The doctor needs a full load, because sort order, key length, and
duplicate keys across overlapping files are read from the decoded key
chunks, and its bucket check decodes every key a second time.

Open question
([prototype findings, what was not verified](../analysis/2026-09-10-prototype-findings.md#what-was-not-verified)):
whether to add a metadata-only mode that reads footers and manifests only.
In its favor, a full load is a heavy way to answer a layout question on a
very large table, and footers and manifests carry most clauses. Against
it, the clauses that decide refusal live in the key chunks, so a
metadata-only verdict could pass a table the load then refuses, and two
verdicts under one name are the confusion a single checker exists to
prevent.
