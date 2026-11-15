# Allocator rounding in the caches

> Updated 2026-11-14: slabs of icetray's own size classes were built,
> measured against main, and set aside; the caches charge jemalloc's size
> class instead, and jemalloc is built without its large-allocation
> padding. The title was Cache slabs.

## Problem

The row cache and the page cache each keep an entry's bytes in an
allocation of its own, and jemalloc rounds each allocation up to its size
class: four classes per doubling, so a length just over a power of two
loses up to a fifth of its allocation. Embeddings often sit there, since a
vector of a power of two of 4-byte floats plus a small header is just over
a power of two. The row cache charges the class, as RFD 026 (Row cache)
has it, so on a trace of embedding batches a budget held about four
fifths of the entries a table of values a few percent shorter held, and
hit about as often as that table's cache did with four fifths of the
budget. The page cache rounds the same way and did not charge it: it
charged a page its length and an overhead, so its memory level held more
than its part of the budget (RFD 028 (Cache budget)). jemalloc also gives
every large allocation a page of padding, which neither cache charged.

## Decision

The allocator's rounding of cached values and pages will be charged
against the budget, not removed: each cached value and page is charged
jemalloc's size class for its length, and jemalloc is built without the
page of padding it adds to large allocations.

- **The charge.** The row cache charges a value and its key their size
  classes, and the page cache charges a page its size class, each with
  the shared header a first read allocates and the entry's overhead, so
  the budget bounds what the caches allocate.
- **No padding.** jemalloc's `cache_oblivious` option, on by default,
  gives every large allocation an extra page so its start lands at a
  random cache line. icetray's builds compile it off, so a cached page or
  value that large costs its size class and no more. An operator can turn
  it back on through jemalloc's environment variable, and the caches then
  hold up to a page more per large allocation than they charge.

## Alternatives Considered

Slabs of icetray's own size classes, 32 per doubling from 1 KiB, each hit
handed a reference to its slot, chunks emptied to give memory back, and
a sixteenth of the budget held in reserve for free slots. This was the
decision until it was built and measured against main. On embedding-like
values it bought about what a tenth more cache memory buys; on values
that already fit a jemalloc class it cost about as much, through the
reserve. It needed `unsafe` that manages shared memory, Miri on a pinned
nightly, emptying that evicts entries the eviction policy did not choose
and that races inserts, a floor below which a cache keeps no slabs, and
accounting tied to jemalloc's page size and padding. A gain of that size
does not pay for that much code and operation. The implementation is
kept at the tag `shelved/cache-slabs`. An earlier measurement of
slots of exactly an embedding's length showed that a hit must hand out a
reference rather than copy the value, and that one lock per class held
up at 11 tasks
([exact-length slots](../analysis/2026-11-11-cache-slabs.md),
[the decided design against main](../analysis/2026-11-14-cache-slabs-against-main.md)).

Keeping jemalloc's padding and charging it. Rejected because the caches
would need jemalloc's page size and option at run time to charge it, and
turning the padding off cost no throughput in the runs that measured it.

Writer guidance to keep values and pages under a size class boundary.
Not a decision icetray can make for its tables; the charge makes the
cost of the rounding visible in the cache's bytes and hit rate instead.

## Consequences

A value or page just over a power of two still costs up to a quarter
more than its length, and a budget holds that many fewer of them than of
values that fit a class. The page cache's memory level holds fewer pages
at a budget than it did, about a seventh fewer on the measured table, and
now holds them within its part.

Large allocations no longer start at a random cache line. No run showed
a throughput change from it.

The bench, the tests, and the server build with the same jemalloc
options, from the workspace's Cargo configuration.
