# Configuration

> Updated 2026-10-04: a configuration lists at least one table; one that
> lists none is invalid.

> Updated 2026-10-05: a table entry also has `lookups`, the lookup level
> it accepts, which defaults to the best its residency reaches, and
> `strict` takes the meaning RFD 020 (Lookup levels) gives it, off by
> default. A `lookups` the residency cannot reach is invalid.

> Updated 2026-10-07: a table entry lists its value columns in
> `value_columns`, which replaces `value_column` and may be empty; a name
> listed twice, or the key column listed, is invalid. The server's
> MultiGet limit is `server.max_multi_get_values`. See RFD 022.

> Updated 2026-10-07: a `value_columns` entry is a name, or a table of
> `name` and `format`, `opaque` by default or `protobuf`, a key the table
> form does not know being a schema fault. See RFD 024.

> Updated 2026-10-13: a file of another format version is treated as
> absent and never converted. See RFD 017.

> Updated 2026-10-14: a table entry that sets `order_column` is an
> ordered table and may set `max_scan_rows`; `server.multi_get_fan_out`
> is renamed `server.batch_fan_out`, and the server adds
> `max_multi_scan_values`, `max_scan_bytes`, `scan_deadline`,
> `multi_scan_deadline`, and `scan_page_cache_bytes`. See RFD 025.

> Updated 2026-10-30: the server's `cache_bytes`, by default a share of
> the process's memory limit, and `row_cache_share` replace
> `page_cache_bytes` and `scan_page_cache_bytes`, and a file that sets
> either is refused (RFD 027, RFD 028). A key-value table entry may set
> `row_cache`, which on an ordered table is invalid (RFD 026). A file
> with no page-cache disk level that serves a table reading through the
> row cache beside one reading pages alone must set `row_cache_share`
> (RFD 028).

## Problem

icetray owns the format of three kinds of bytes: configuration a person
edits, configuration a pipeline renders, and state icetray may one day
persist for itself. The person wants comments and unambiguous types. The
pipeline renders the same settings from templates and orchestrator values,
and most such pipelines emit JSON. A format chosen for one author becomes a
translation step for the other, and across that step copies drift until a
setting is valid on one side and refused on the other. Either way the
settings are structured: a list of tables, each with several fields,
beside server and storage settings that other RFDs define.

The first thing icetray persists for itself is the header of a chunk file
under the disk residency of RFD 017 (Residency tiers). An artifact whose
format is picked under shipping pressure gets whatever format is at hand,
and choosing the envelope beforehand is cheap.

## Decision

User-editable configuration will be TOML, the same schema will be accepted
as JSON so that machine-produced configuration can be consumed without a
translation step, and any state icetray persists for itself will be a
length-prefixed protobuf behind a magic byte and a version.

There is one schema, and a single parser reads both syntaxes into the same
structure and validates it once. No setting can therefore be accepted in
one syntax and refused in the other, and a test holds the two equivalent
by loading one configuration through both. The syntax might be chosen by
file extension or declared alongside the path; RFD 012 (Deployment
contract) says where the path comes from.

The configuration covers the server, storage, and tables. For the server
it holds the listen addresses, the interval at which RFD 005 (Generations
and refresh) polls the ref, the cache budget and the row cache's share
of it (RFD 028 (Cache budget)), the MultiGet
limit `server.max_multi_get_values` of RFD 010 (Transport and client
contract), which must be above zero, and the scan settings of RFD 025
(Ordered rows per key). For storage it
holds the endpoint and region, the prefix map for location rewriting, the
hedging percentile, window, and ceiling of RFD 013 (Tail latency), the
static keys RFD 003 (One storage stack) lets configuration supply, and the
optional local directory of RFD 012 (Deployment contract). Tables are a
list of at least one, and each entry has a name, a source, a ref, a key
column, `value_columns`, `lookups` and `strict` (whose meanings are RFD
020 (Lookup levels)), the `residency` and resident-bytes budget of RFD
017 (Residency tiers), for a key-value table `row_cache` (RFD 026 (Row
cache)), and for an ordered table `order_column` and `max_scan_rows`
(RFD 025). `value_columns` is a required list of the columns the table
serves (RFD 022 (Value columns)). It may be empty, for a table that
answers only whether a key is present. Each entry is a
column's name, or a table of its `name` and its `format`, `opaque` by
default or `protobuf` (RFD 024 (Field projection)), so
`{ name = "detail", format = "protobuf" }`; a key the table form does not
know is a schema fault, placed at the key. A name listed twice, or the
key column listed as a value, is invalid. Order carries no meaning, so
two entries that list the same columns, each with its format, in another
order are equal. The source is one of four kinds: a static table
root or metadata file, an Iceberg REST catalog, or a table in Glue or in S3
Tables (RFD 019 (AWS catalog sources)). The prototype's TOML configuration had this shape
([prototype findings, what was built](../analysis/2026-09-10-prototype-findings.md#what-was-built)).

A configuration that lists no table is invalid. A server started on it
would report ready while serving nothing, and the mistake is in the file,
so it is refused there like any other rule a configuration breaks: `serve`
does not start, a reload is refused, and a one-shot fails rather than
reporting a usage error.

A running server applies a change to the tables and refuses a change to
anything else, which takes a restart (RFD 018 (Configuration reload)).

The envelope rule exists so that the first persisted artifact does not
invent a format. The magic byte identifies a file as icetray's before any
of it is parsed. The version lets a build recognize a file of another format, which it
treats as absent rather than misreading it (RFD 017 (Residency tiers)). The length prefix frames the message, so
truncation is detected and several messages can share one file. Protobuf
is already in the build for RFD 010 (Transport and client contract), so
the rule adds no dependency.

## Alternatives Considered

YAML for the user-editable file. Rejected because an unquoted value can
parse as a number or a boolean when a string was meant, and configuration
is where that surprise costs the most.

Environment variables only. Rejected: a list of tables is structured data,
and flattening records into variable names invents a format of its own,
one that has neither comments nor types.

JSON only. Rejected because humans edit this file, and JSON allows neither
comments nor a trailing comma. Accepting JSON serves the pipeline;
requiring it would tax the person.

Serializing the in-memory structure as it stands for persisted state.
Rejected: the bytes would change whenever the structure does, and nothing
in them would say which build wrote them, so a file could not be read
across builds.

## Consequences

Deployers whose pipelines emit JSON hand icetray the file directly, under
the surface RFD 012 (Deployment contract) defines. Nothing in this
repository converts between the syntaxes.

The chunk file of RFD 017 (Residency tiers) is the first artifact under the
envelope: its magic byte, version byte, and length-prefixed
`ChunkFileHeader` message are the envelope, and the core crate's chunk
file format reads and writes them. A later persisted artifact follows the
same rule under a magic byte of its own.

Open question: whether individual settings may also be set from the
environment, layered above the file. For: a container often
varies one or two values per environment, such as a listen address or a
region, and setting them without rendering a file is the twelve-factor
habit. Against: two sources for one setting need a precedence rule and
give an operator a second place to look when a value is wrong, and a
pipeline that emits JSON can already vary a value inside the file.
