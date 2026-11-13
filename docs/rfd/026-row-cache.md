# Row cache

> Updated 2026-11-07: icetray keys entries by table key and column and
> checks each hit against the generation, so it reads no key page for a
> hit, and it caches presence-only lookups too.

> Updated 2026-11-12: values of 1 KiB or more live in slabs of icetray's
> own size classes, a hit hands out a reference to the value's slot, and
> a sixteenth of the budget is held in reserve for the slabs' free slots.
> See RFD 030 (Cache slabs).

## Problem

A lookup is for one row, but icetray reads a whole key page to find the
row and a whole value page to fetch it. When the hot keys are scattered
through the key order, as they are wherever keys are hashed or assigned
without regard to how often they are read, the rest of each page is rows
no caller requested. Decoding them costs the lookup time, and holding
them costs the page cache memory. A value page of a table with rows of a
few kilobytes, such as embeddings, has about a dozen rows, so a page
cache has room for about a twelfth of the hot rows that the same memory
could cache as rows.

## Decision

icetray will answer key-value lookups from a row cache in memory before
it consults the index, keyed by table key and column and on by default,
and will check each hit against the generation's files and deletes.

- **What is cached.** An entry is one column's value for one key, or its
  null, copied out of the page, with the data file and row position
  icetray read it from. When icetray finds the row for a presence-only
  lookup, it caches an entry with no value. It caches nothing for an
  absent key.
- **When a hit counts.** icetray serves an entry only when the entry's
  file is in the lookup's generation and its row is not deleted there.
  That row is then the key's row, because Iceberg never changes a data
  file in place and icetray refuses a snapshot with two live rows for a
  key (RFD 005 (Generations and refresh), RFD 021 (Merge-on-read)).
  Otherwise icetray reads the row as it would without the cache and
  replaces the entry with what it read, or drops the entry when the key
  has no live row, the column takes its default, or the value read is
  too large to cache. A refresh invalidates no entry: after a
  merge-on-read update only the changed keys' entries fail the check,
  after a rewrite only the replaced files' entries, and a failed entry no
  lookup reaches again is evicted like any other.
- **What a hit costs.** A hash and the check. icetray reads no page for a
  hit and uses no fan-out slot; the hit counts as a warm hit and does not
  change the table's level (RFD 020 (Lookup levels)).
- **After the cache.** icetray applies projection (RFD 024 (Field
  projection)) to the cached value, so callers naming different fields
  share one entry.
- **On by default.** An operator whose callers already cache in front of
  icetray sets `row_cache = false` on the table, and icetray reads its
  pages alone. icetray always reads pages for an ordered table, since
  only the index and the pages show that a Scan's range is complete.
- **What an entry weighs.** icetray charges an entry its key's and
  value's bytes at what their allocations cost, plus its overhead, so
  the budget bounds the cache's memory (RFD 028 (Cache budget)). While
  the budget is at least 4 MiB, a value of 1 KiB or more lives in a slot
  of the cache's slabs and is charged its slab's size class (RFD 030
  (Cache slabs)); a shorter value, any value under a smaller budget, and
  the key are charged jemalloc's size class. The slabs' free slots, which
  no entry is charged for, take a sixteenth of the budget held in reserve
  for them, and the entries the rest. icetray does not cache a value
  above a bound.
- **The library.** quick_cache, the fastest in a bake-off against foyer
  and segcache over rows of 100 bytes to 8 KiB, and the only one whose
  throughput grew under hot keys.

## Alternatives Considered

A cache of decompressed pages. Rejected because it would cache a hot
row's cold neighbors too.

Rows keyed by data file and row position, read after icetray resolves the
key through the index, as first decided. Rejected because icetray still
decoded a key page for every hit, which on scattered hot keys was most of
a warm hit's time.

Rows keyed by table key without the check. Rejected because icetray
would have to empty the cache on every refresh or serve deleted rows.

Decoded key arrays (RFD 004 (Resident key-chunk index)) instead.
Rejected because they cost memory for every key of the table, where the
row cache uses memory only for hot rows.

foyer for rows. Rejected on the bake-off: the most memory per row, and
shard locks that capped its throughput. segcache. Rejected for now: it
packs small rows more densely but refuses fresh keys once full, and an
arena of icetray's own could match its density.

Rows on the local disk as well. Rejected because the page cache's disk
level already has the pages icetray reads on a cache miss.

## Consequences

After a start, icetray fills the row cache from live traffic; a service
that must start warm keeps a cache in front of icetray. A lookup without
a valid entry costs what it did, plus a copy and an insert. Tables over
the same files do not share entries.

The cache is correct only while icetray serves at most one live row per
key. Allowing two, as equality deletes would, means changing the check
too.

icetray reports the row cache's hits, cache misses, stale entries, and
bytes, and its slabs' chunks waiting to be freed and the time their
locks were waited for.
The counting tests of RFD 014 (Test layering) check that a hit reads no
page, and that icetray never serves a deleted or rewritten row, keeps a
reused file's entries, and never serves a replaced table's entries to
the table that replaces it.
