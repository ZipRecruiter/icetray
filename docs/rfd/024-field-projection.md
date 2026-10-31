# Field projection

> Updated 2026-10-30: a key-value lookup reads each value from the row
> cache before the page cache, and the trim follows both, so the row
> cache keeps each value whole and callers naming different fields share
> one entry. See RFD 026.

## Problem

RFD 002 (Serving layout) recommends packing a row into one
binary column as a serialized protobuf, and RFD 022 (Value columns) lets
a table split a row into columns along how it is read. A column is cut
once, at write time, for every caller. Within one column, a caller that
wants two fields of a message holding a thousand still receives all of
it and decodes all of it, on every lookup. The decode is the larger
cost: a protobuf runtime parses every field it is handed, so a caller's
CPU per lookup follows the size of the message, not of what it reads.
Callers who cannot afford that build a narrower copy of the table for
themselves, or serve from another store.

The protobuf wire format can be trimmed without its schema. Every
field begins with a tag carrying its number and wire type, and the wire
type gives the field's length, so the top-level fields of a message can
be walked, kept, or skipped by number alone. Icetray already holds the
value's bytes after the row cache or the page cache, before it answers.

## Decision

Callers that read a few fields of a protobuf value will be served by
trimming, at request time, the value of a column declared protobuf to
the top-level field numbers the request names, after the row cache and
the page cache, with the whole value returned when the request names
none.

- **Declared per column.** A `binary` column's entry in `value_columns`
  may declare `format = "protobuf"`; on a `fixed` or `string` column the
  declaration is a configuration error, since a trimmed value has no
  fixed width and a protobuf is not text. TableInfo reports each
  column's format as the entry declares it when the call is answered,
  which a retune changes under the same snapshot id. A field list sent
  for a column not declared protobuf is `INVALID_ARGUMENT`. A change of
  format touches no layout, so it is applied in place, as RFD 020
  (Lookup levels) applies `lookups`. A declaration of protobuf on a
  column the serving snapshot types `fixed` or `string` refuses the
  reload, as RFD 020 refuses a strict level the snapshot misses.
- **Named per request.** Each column a Get or MultiGet reads carries a
  list of field numbers, applied to every key of the call, and empty
  for the whole value; there is no default list in the configuration.
  So the request's list of columns becomes a list of per-column
  messages, and an empty list of columns still means presence alone
  while an empty list of fields means the whole value. A number outside
  1 to 536,870,911, protobuf's range, or a number given twice, is
  `INVALID_ARGUMENT`. `server.max_multi_get_values` counts as RFD 022
  has it.
- **What is kept.** The value's top-level fields whose numbers were
  named are copied in the order the value holds them, every occurrence
  kept: each element of a repeated field, each record of a packed field
  (an encoder may split one), each map entry, and each occurrence of a
  singular field, so a scalar's last write still wins and a singular
  message's occurrences still merge when the caller parses. A named
  field is copied whole, a sub-message or a group from its start tag to
  its end tag. Nothing inside a length-delimited field is examined.
- **What the caller can rely on.** A projected value parses as the
  caller's type whenever the whole value does, and holds what the whole
  value holds for each named field, with one exception the caller
  avoids: a caller that names a member of a oneof names every member of
  it, since setting one member clears the others, and a member the
  whole value later cleared would otherwise come back set. A field not
  named reads as unset, which for a field without presence is its zero
  value. A message with proto2 required fields, or `LEGACY_REQUIRED`
  under editions, may arrive without them, and the caller parses it
  partially, as Go's `UnmarshalOptions{AllowPartial: true}` or Java's
  `parser().parsePartialFrom` do. Icetray holds no schema, so it knows
  neither oneofs nor required fields, and does not check that a named
  number exists.
- **Malformed values.** The walk reads the framing as prost's decoder
  does: a tag is a varint whose value fits 32 bits, and a varint is at
  most 10 bytes, its tenth at most 1. A value is malformed when its
  top-level walk meets wire type 6 or 7; a field number of 0, or a tag
  that does not fit 32 bits, which is a field number above 536,870,911;
  a varint over 10 bytes or past 64 bits; a tag, a varint, or a fixed
  value cut off by the value's end, or a length past it; a group whose
  end tag is missing or names another number; or an end tag with no
  group open. That is exactly where prost's decoder refuses the
  framing, except that the walk refuses no depth of nesting. A
  malformed value fails a projected lookup as `FAILED_PRECONDITION`,
  naming the table, column, and key: the bytes are what the writer
  wrote, and it is the declaration they do not meet. The failure is the
  same on every attempt, so it is not retried, and under RFD 010
  (Transport and client contract) it fails every MultiGet that projects
  that key. A lookup that names no fields returns the bytes unchanged.
  The walk is linear in the value's bytes, groups nested in it
  included, with no recursion, so no value can make it costlier than
  its length.
- **The rest of a value.** A null stays null, and a value holding none
  of the named fields is present with zero bytes, distinct from null. A
  default RFD 023 (Column resolution) answers is projected as a value.
- **After the cache.** The row cache keeps each value whole (RFD 026
  (Row cache)) and the page cache keeps pages as the store returned
  them, so every caller shares both whatever fields it names. A
  projection changes no GET, no level, and no lookup-cost gauge.
- **Counted.** `icetray_value_bytes_read_total` and
  `icetray_value_bytes_returned_total` count, by table and column, the
  bytes of the values a lookup read and the bytes it returned, a
  whole-value lookup counting the same in both, so an owner can see
  what projection saves. They count each lookup that answers, a miss
  and a null counting none and a default its bytes; a lookup that
  fails, a malformed value included, counts no bytes, and a MultiGet
  that fails counts the keys it answered before the failure, as
  `icetray_lookups_total` does. `icetray_projection_errors_total` counts
  malformed values by table and column.
- **Checked by the doctor.** The `doctor` command walks the default
  each protobuf column states (RFD 023), with no GET, when a file
  answers the column by it (a file written after the column was added
  holds the column, so a default no file answers is never served), and
  the values of
  one value page of the column, one GET, two when the column's chunk is
  dictionary-encoded, and reports a malformed value or default as the
  advice `malformed_protobuf`, which catches a column declared protobuf
  that holds another format. The page is the first of the first file
  holding the column; when it holds only nulls the walk passes to the
  next such file, up to three. A page the doctor cannot read is reported
  as an error for its column, and the doctor goes on with the table's
  other findings and its judgement. `malformed_protobuf` is advice only
  the doctor raises, so the findings gauge, which reports what a load
  found, has no series for it. The check at every load walks neither,
  since it reads no value page.

## Alternatives Considered

A default field list in the configuration, overridden by the request.
Rejected because a request that names no fields would then mean
whatever the configuration lists, a behavior the caller never stated,
and the whole value would stop being what an empty list returns. If
callers repeat one list, a named list a request asks for by name is
the intentional form, and can be added without changing this one.

Nested field paths, such as a field of a sub-message. Rejected for now.
On the wire a sub-message, a string, bytes, and a packed repeated field
are all length-delimited, so descending into one needs the schema to
say it is a message. Registering descriptors per column would also let
icetray keep a oneof whole, and can be decided later if top-level
projection or the oneof rule proves too coarse.

Fields named rather than numbered. Rejected because a name needs the
schema, and the number is the stable part of the protobuf contract: a
rename keeps it, and a caller's generated code carries it.

Projection in the caller. Rejected because the bytes still cross the
wire and icetray ships no client (RFD 010).

Narrower copies of the table, or a column per caller under RFD 022.
Kept for a row split by a few large parts, but rejected as the answer
to fine-grained reads: a copy per caller is the cost this decision
exists to avoid.

Other formats. Protobuf only, for now. Avro carries no field tags, so
trimming it needs the writer's schema; JSON can be trimmed only by
parsing it; FlatBuffers is already read in place. The `format` key
leaves room for another.

Checking every value at load. Rejected because it reads value pages a
load otherwise never fetches; the doctor's one page per column catches
the likeliest error, a wrong declaration, for one GET.

A cap on group nesting, as protobuf runtimes set one. Rejected because
the walk does not recurse, and a cap would fail projected a value that
serves whole.

## Consequences

A projected cold hit costs the same GETs as a whole one. Icetray spends
a walk linear in the value's bytes, small beside the page decompression
every lookup already pays for a value much smaller than its page. What
projection saves is the bytes on the wire and the caller's decode.

The oneof rule is the caller's to keep, and breaking it returns a
cleared member as set, with no error. Callers project oneofs with care,
and `docs/table-layout.md` says so.

A malformed value that the doctor's page did not hold is found when a
caller first projects it, and fails every projected batch that holds it
until it is rewritten or the caller stops projecting it. The error
counter is how an owner learns of it. The doctor checks nothing of a
column whose first page is all nulls in each file it walks.

Removing a column's declaration reaches the clients first, since a field
list for an undeclared column is `INVALID_ARGUMENT`.

Earlier RFDs and documents are revised to match: RFD 002 (projection is
decided here, not as a tier under RFD 007 (Serving tiers)); RFD 006
(Lookup hot path) (a projected lookup walks its value); RFD 010 (the
per-column field list, `FAILED_PRECONDITION`, and a deterministic
failure not retried); RFD 011 (a column's `format`); RFD 016 (Doctor)
(the walk of one page per protobuf column); RFD 018 (Configuration
reload) (a format change applies in place); RFD 022 (a column entry
carries its format, and its projection alternative points here); the
optimal layout (the declaration, the oneof rule, and the partial-parse
rule); and the performance guide (when to split a row into columns and
when to project one).

Property tests pin the walker under RFD 014 (Test layering): over typed
fixture messages with repeated, packed and split-packed, map, oneof,
group, and nested fields, the named fields of the parsed projection
equal those of the parsed whole value, and every malformed shape above
fails. Fixtures pin a null, a value holding none of the named fields,
and a projected column default.
