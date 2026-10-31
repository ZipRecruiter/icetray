# Row cache

## Problem

A key-value lookup that finds its value page in the page cache still
decompresses and decodes that page for the one value it wants, and the
page cache spends its memory on whole pages. A page holds the row a
caller asked for beside every row stored next to it. When the hot keys
lie scattered through the key order, as they do wherever keys are hashed
or assigned without regard to how often they are read, most of each
cached page is rows nobody asked for. A table of rows of a few
kilobytes, such as embeddings, fits about a dozen to a value page, so
its page cache holds about a twelfth of the hot rows the same bytes
could hold as rows, and the rest of the hot set costs a read from the
disk level or object storage.

Lookups of things people act on, such as the jobs a recommendation ranks
or the users active right now, are skewed: a small share of keys takes
most of the lookups. That is the case icetray's defaults should serve
well. RFD 006 (Lookup hot path) left open whether a cache of decoded
data would pay for its memory, on a hit-rate curve under skewed keys
that had not been measured.

## Decision

Key-value lookups will read each value from a row cache in memory, keyed
by the data file and the row's position and column in it and on by
default, before they read the page cache.

- **What is cached.** An entry holds one column's value for one row: its
  bytes, copied out of the page so the entry holds no page, or its null.
  Its key is the data file's path, the row's position in the file, and
  the column's index in the file, so tables that list or name their
  columns differently over the same files share entries, and an entry
  means the same thing in every generation. A file's path names one
  file's bytes: Iceberg never rewrites a data file in place, and the
  page cache already relies on it. A presence-only lookup, and a column
  a file answers by its default (RFD 023 (Column resolution)), read no
  page and use no entry.
- **Where it sits.** A lookup resolves its key through the generation's
  index and checks the row against its file's deleted rows (RFD 021
  (Merge-on-read)) first, then reads the row cache for each column it
  names, and goes to the page cache only for a column the row cache
  lacks; a value read from a page is inserted. The index decides which
  file and row a key reads before the cache is asked, so a row deleted
  or rewritten into another file is never served from it, and nothing is
  ever invalidated. A refresh keeps every entry of the files the new
  generation reuses (RFD 005 (Generations and refresh)), and entries of
  files no generation holds age out.
- **What a hit saves.** A hit reads no value page and takes no fan-out
  slot; the key is resolved as before, a key page decoded under `full`
  (RFD 006) and the key pages read first under `bloom`. So a hit's CPU
  falls most where values compress, since decompression is most of a
  warm hit's value page, and least where they do not, where the key
  page's decode dominates. The larger gain is the hit rate: the same
  memory holds many more hot rows than pages (RFD 028 (Cache budget)).
  A hit changes no level (RFD 020 (Lookup levels)) and no lookup-cost
  gauge, which describe the layout, and counts as a warm hit.
- **After the cache.** Projection (RFD 024 (Field projection)) trims the
  cached value, so callers naming different fields share one entry.
- **On by default.** Every key-value table reads through the row cache
  unless its entry sets `row_cache = false`, as a table whose callers
  keep their own cache in front of icetray would; such a table reads
  pages alone, as before. On an ordered table the setting is invalid: a
  Scan reads a range of rows, which only the index and the pages can say
  is complete, so it reads pages.
- **Nothing warms it.** A started or rebuilt server fills the row cache
  from its traffic. A service that must start warm keeps a cache in
  front of icetray, or its caller prefetches.
- **What an entry weighs.** An entry is charged its value's bytes and
  what the allocator spends to hold it, the rounding of its allocation
  and the entry's overhead, so the budget bounds the memory the cache
  takes; RFD 012 (Deployment contract) found allocator overhead on small
  entries the largest error in sizing a replica. A value above a bound
  is not cached, so one large value cannot empty a shard. Eviction drops
  a row read once before rows that keep being read.
- **The library.** quick_cache, chosen in a bake-off against foyer and
  segcache over rows of 100 bytes to 8 KiB. It was the fastest, and the
  only one whose throughput grew under hot keys; a read costs hundreds
  of nanoseconds. segcache held small rows more densely, for about a
  tenth fewer misses at 100 bytes and none from a kilobyte up, but
  refused fresh keys once its hash table filled. foyer's shard locks
  held it to about a fifth of quick_cache's throughput. foyer stays the
  page cache, for its disk level and its recovery.

## Alternatives Considered

A cache of decompressed pages, RFD 006's open question. Rejected because
it keeps a hot row's cold neighbors too, at their decompressed size, so
the same memory holds less of the hot set than rows do.

Rows keyed by table key and generation. Rejected because every new
snapshot would empty the cache, and writes accumulating between full
rebuilds are the expected way a table changes, so the cache would rarely
be warm.

Rows keyed by table key alone, cleared or left stale on refresh. Rejected
because a delete would need the cache cleared to take effect, and
staleness would serve a key its owner deleted. Keying by file makes that
impossible at no cost, since a lookup resolves its key through the index
either way.

foyer for rows, so one library caches both. Rejected on the bake-off: it
held the most memory per row and its shard locks capped its throughput.

segcache. Rejected for now: its density gain is about a tenth fewer
misses at 100-byte rows and nothing from a kilobyte up, against refusing
fresh keys in a full table. Rows never change within a file, so an arena
of icetray's own could match its density if traces show the gain
matters.

Rows on the local disk as well. Rejected because the page cache's disk
level already holds every page a row-cache miss needs, and a second disk
level of rows would duplicate it to save a local page read.

Warming from a saved hot set, or carrying hot keys from one generation to
the next. Rejected as more machinery than it saves: keying by file keeps
rows across a refresh, and services that must start warm already keep a
cache in front.

The row cache off by default. Rejected because skewed lookups are the
expected case, and a table whose callers cache in front of it opts out
with one setting.

## Consequences

A repeated hit skips its value page. A row's first lookup costs what it
did, plus a copy and an insert. A started server serves its first
minutes from the page cache, the disk level, and object storage while
the row cache fills. Caching decoded keys, which RFD 006 defers, would
take the key page off a hit too, and is decided separately.

The row cache reports its hits, misses, and bytes as series of its own.
A benchmark scenario over skewed keys measures it, and the counting
tests of RFD 014 (Test layering) pin a row-cache hit at no value-page
GET, a deleted or rewritten row never served from the cache across a
refresh, and a reused file's entries kept.

Earlier RFDs and documents are revised to match: RFD 006 (its open
question answered here); RFD 011 (Configuration) (`row_cache`); RFD 024
(values are trimmed after the row cache); the configuration reference,
the operating guide, and the performance guide.
