# One page cache

## Problem

A process keeps two page caches. Key-value tables read value pages,
dictionary pages, and `bloom` key pages through the page cache, whose
memory level sits in front of the local directory's disk level (RFD 017
(Residency tiers)). Ordered tables' span GETs fill a second cache of
their own, held in memory alone (RFD 025 (Ordered rows per key)), so a
Scan never reads from the local disk, and every page a scan's cache
evicts costs a GET the next time it is read.

RFD 025 kept the two apart so a long run could not evict a key-value
table's hot pages. Two things have changed. A table serves one kind of
call, Get and MultiGet on a key-value table or Scan and MultiScan on an
ordered one, refused otherwise before any read, so separating the caches
by call type only separated tables, which key-value tables never were
from each other. And a key-value table's hot rows now sit in the row
cache (RFD 026 (Row cache)), out of a scan's reach.

## Decision

Every table's pages will be cached in one page cache, its memory level
in front of the disk level, and a Scan's span read will take each page
from memory, then from the disk level, and fetch from object storage
only the runs of pages neither holds.

- **What is shared.** Value pages, dictionary pages, and key pages of
  both kinds of table, keyed by file and offset as both caches were.
  The memory level's eviction keeps a pass over pages read once from
  displacing pages read again; the disk level evicts its oldest blocks
  first, as it always has between key-value tables.
- **A span read.** A page memory holds is served; a page the disk level
  holds is read locally, the reads of one span together; what is left
  is fetched as before, one GET per maximal run of contiguous pages
  neither level holds, coalesced with concurrent reads of the same pages
  as RFD 025 has them. A Scan's rounds to object storage can only fall.
  Pages a span GET fetches go into memory and reach the disk level when
  they leave it, as a lookup's do. Span GETs keep their own hedge window
  (RFD 013 (Tail latency), RFD 025).
- **Settings.** `server.scan_page_cache_bytes` is removed, and a file
  that sets it is refused with a message naming the replacement, the
  one budget of RFD 028 (Cache budget). Without a disk level, which is
  the local directory with a nonzero `page_cache_disk_bytes`, every page
  lives in the memory level alone, as a key-value table's do today.
- **Counted.** The page cache's series lose their `cache` label, one page
  cache remaining.

## Alternatives Considered

A memory cache of ordered tables' pages alone, kept out of the disk
level, which RFD 025 chose. Rejected because it keeps Scans off the
local disk to protect key-value pages whose hot rows the row cache now
holds.

A scan cache kept apart with a disk level of its own. Rejected because
it is a second disk budget to size for the same protection.

One cache, with span reads left to memory alone. Rejected because pages
a span GET fetched would reach the disk level and never be read from it,
filling the disk with pages that cost a GET anyway.

## Consequences

A Scan whose pages the disk level holds costs local reads in place of
GETs, so an ordered table on a replica with a local volume serves its
repeated runs from local disk once memory lets them go.

Ordered and key-value tables served by one process share page memory
and the disk level, so a long Scan can push another table's pages off
the disk level, though not its rows out of the row cache. A replica
with a disk level larger than the tables it serves sees little of it.

Earlier RFDs and documents are revised to match: RFD 017 (the disk level
holds ordered tables' pages); RFD 025 (one page cache); the operating
guide's cache series.
