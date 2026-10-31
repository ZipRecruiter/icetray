# Configuration reload

> Updated 2026-10-04: a file that lists no table fails validation, so a
> reload to it is refused and the tables serving serve on.

> Updated 2026-10-04: a removed table's gauges leave the metrics at the
> next scrape instead of keeping their last values.

> Updated 2026-10-05: a change to only a table's `lookups` or `strict`
> applies to its serving generation in place, with no load, since the
> level is judged from the layout the generation holds; the reload is
> refused whole when a strict table's serving snapshot is below its new
> level. See RFD 020 (Lookup levels).

> Updated 2026-10-07: a change to the set of a table's value columns
> loads the whole table anew, and a reordered `value_columns` is no
> change. See RFD 022.

> Updated 2026-10-07: a change to only the formats of a table's value
> columns, alone or with `lookups` or `strict`, applies in place as a
> change of those does, since a lookup reads each column's format from
> the entry; a format the serving layout's type for its column does not
> take refuses the reload. See RFD 024.

> Updated 2026-10-14: a change to only an ordered table's
> `max_scan_rows` applies in place, as a change to `lookups` does, and
> setting, changing, or removing `order_column` loads the table anew;
> the new scan settings take a restart. See RFD 025.

> Updated 2026-10-30: a reload that changes whether any table served
> reads through the row cache resizes the row cache and the page cache's
> memory level within the same budget, and one whose split a start with
> its file would refuse is refused; `server.cache_bytes` and
> `server.row_cache_share` still apply on restart. A reload that applies
> checks the budget against the memory limit again. See RFD 028.

## Problem

The tables a server serves are fixed when it starts. Adding one, removing
one, or changing one's settings means editing the configuration file and
restarting the process, and a restart reloads every table rather than the
one that changed. The tables that did not change pay a first load, or
under the disk residency of RFD 017 (Residency tiers) a restart's reuse of
their trusted chunk files, and a replica with no local directory pays a
first load of all of them. A server that already follows its tables' refs
(RFD 005 (Generations and refresh)) has everything it needs to load a
table beside the ones serving.

Two mechanisms are common for this. A management API that loads and
unloads tables makes the running process the record of what it serves. A
reload makes the file that record and has the process follow it. The
choice decides where the list of tables lives, which makes it expensive to
reverse.

## Decision

A change to the configuration file's tables will be applied to a running
server when the file's contents change or the process receives SIGHUP, by
loading every added or changed table beside the tables serving and
swapping the whole set at once; a reload that cannot be applied in full
will change nothing.

The server reads the file on its poll interval (RFD 005) and compares a
digest of its bytes with the digest of the file it applied, and SIGHUP
makes it read the file at once. When the bytes are unchanged the read
costs nothing further. A change that fails to parse or validate (RFD 011
(Configuration)) is refused as a whole, and a file that lists no table
fails validation, so emptying the file's tables never empties the
server. A poll does not try a refused file again until its bytes change,
but SIGHUP does, because a table's load can fail for a reason that has
since passed.

A reload applies the tables only. A listener, the caches, and the
storage client are built once at start, so a file whose server, storage,
or log settings differ from the running ones is refused as a whole, with a
reason that names the setting and says a restart applies it. The caches
keep the budget `server.cache_bytes` gave them, but a reload that changes
whether any table served reads through the row cache moves that budget
between the row cache and the page cache's memory level as a start with
the file would split it, and is refused when a start would refuse that
split (RFD 028 (Cache budget)).

Tables are matched by name. A table whose entry is equal in every field
keeps its generation, its refresh, and its reservations untouched.
`value_columns` is compared as a set, so a list that only reorders its
columns is equal (RFD 022 (Value columns)), and a change to the set
makes the table changed, which loads it whole. A table whose entry
differs in `lookups`, `strict`, the formats of the same columns (RFD
024 (Field projection)), or an ordered table's `max_scan_rows` (RFD 025
(Ordered rows per key)) alone keeps its generation and takes the new
entry in place at the swap, since none of them changes the layout the
generation holds; the reload is refused when the serving snapshot would
refuse the new entry, a strict level it does not meet or a protobuf
format on a column it types other than `binary`. An
added table loads under the load deadline, as at start. A changed table
loads as a new table beside the one serving under its name, and the old
one serves until the swap. A removed table stops refreshing at the swap. A
call in flight finishes on the generation it holds, and a later call for
the name is `NOT_FOUND`, as for any name not served. After the swap the
local directory's retention (RFD 017) deletes the chunk files that no
serving generation references, as it does after a refresh.

The swap waits until every added and changed table serves its first
generation. If any of them fails to load, the reload is refused, the
tables it loaded are dropped, and the set that was serving serves on. This
carries the start-or-fail rule of RFD 005 to a running server: at any
moment the server serves exactly the tables of one configuration it
applied.

One reload runs at a time. A change that arrives during a reload is read
when the reload ends, so the next reload starts from the newest file and
the intermediate versions are never applied on their own.

Each reload's outcome is logged with the digest of the file it read and,
for a refusal, the reason. Metrics count reloads by result. A digest
exported as a gauge would lose digits
([prototype findings, finding 14](../analysis/2026-09-10-prototype-findings.md#14-metrics)),
so instead a gauge reads 1 while the file on disk differs from the
configuration applied, which lets an operator see that a replica serves a
configuration other than its file without comparing digests. Readiness
does not change, because a refused reload leaves the replica serving the
configuration it applied. A table being loaded for a reload writes its
per-table gauges only from the swap on, so until then the table serving
under its name keeps its own. A removed table's gauges leave the metrics at
the next scrape: every scrape writes the gauges of the tables serving, and
the recorder drops a gauge no scrape wrote, so no series goes on reading a
table that is gone. Its counters stay, as a counter of something gone does
in Prometheus.

## Alternatives Considered

**A management API that loads and unloads tables.** A call reaches one
replica, and every other replica, including every replica started later,
knows nothing of it unless icetray persists and replicates what the call
did. That record would be a second source for the list of tables beside
the file, and it would drift from the file. A call that makes a replica
fetch and hold tens of gigabytes also needs authentication and access
policy, which RFD 012 (Deployment contract) leaves to the platform. Search
engines that work this way own the data they serve, whereas icetray serves
tables whose record is the catalog.

**Watching the file for filesystem events.** This lost as the trigger. An
orchestrator that updates a mounted file by swapping a symlink and an
editor that saves by rename both replace the file under a watch on it. A
digest compared on the poll interval sees every change within one
interval, whatever wrote it, at the cost of one small local read per poll.

**SIGHUP alone.** This lost as the only trigger because an orchestrator
that updates a mounted file does not signal the process, so every deployer
would need a component that does. SIGHUP stays as the way to apply a
change at once.

**Applying the tables that loaded and reporting the rest.** A table
missing from a running server is the silent failure that RFD 005 refuses
at start.

**Reloading server and storage settings as well.** This is left out for
now. Each setting is its own problem (rebinding a listener, changing the
cache budget `server.cache_bytes` that the row cache and the page cache
share, replacing the storage client under loads in flight), and a restart
applies them. A reload does resize the two caches within that budget when
whether any table reads rows changes (see the 2026-10-30 note above).

## Consequences

Adding or changing a table leaves the other tables serving as they were,
and a restart is needed only for server, storage, and log settings.

A replica must have the memory for a reload's new tables beside the
serving ones, the same headroom RFD 012 sizes for a refresh that rewrites
a table, and after each reload that applies icetray checks that room
against the memory limit again and warns of a shortfall (RFD 028). A
reload that exceeds the memory limit ends the process. Every
replica reads the same file, so every replica reloads at once, and the
restart that follows loads the same file: an oversized table crash-loops
the fleet just as it would have on a restart. The doctor's predicted
resident bytes (RFD 016 (Doctor)) are the check to run before the change.

The file a replica applies can lag or differ from the file on disk: by up
to one poll interval while it waits, for as long as a load takes, or until
the file is fixed after a refusal. The gauge and the log make the
difference visible but leave it open. A file refused because it does not
parse or validate, or because one of its tables cannot load for a reason
that persists, would fail the next start too, so such a refusal warns that
the next restart will fail. A file refused only for a setting outside the
tables is one a restart applies.

Every replica reads its own copy of the file, so replicas apply a change
at different moments, within the orchestrator's delay in updating mounted
files plus one poll interval. In that window a client can see a table from
one replica and get `NOT_FOUND` from another.

A deployer that mounts the file where the orchestrator never updates it,
such as a single-file mount fixed at start, gets no reload from a change
and must restart as before. That follows from the deployer's choice of
mount, and icetray does not detect it.

The file must be replaced whole, by a rename or an orchestrator's symlink
swap, and never written in place. A read in the middle of an in-place
write can see a prefix of the new file, and a prefix that ends between two
tables parses and validates as a configuration without the rest, so one
poll would drop those tables and the next would load them again.
