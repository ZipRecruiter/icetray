# Cache slabs

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
budget. The page cache rounds the same way and does not charge it: it
charges a page its length and an overhead, and a byte-bounded page of
4 KiB values, closed after the value that crosses 64 KiB, takes the class
a quarter above 64 KiB, so the page cache's memory level holds more than
its part of the budget (RFD 028 (Cache budget)).

## Decision

The allocator's rounding of cached values and pages will be removed by
holding them in slabs of icetray's own fine size classes, each slab
charged in full against the budget and each cached value or page handed
out as a reference to its slot rather than a copy.

- **Size classes.** Each power of two from 1 KiB up to the largest length
  a cache keeps in slabs, the row cache's value bound and a largest page
  the page cache names the same way, is split into 32 classes, so a length rounds up
  by at most about 3%. Every jemalloc class boundary in that range is also
  a slab class boundary, so no length rounds further than it does today.
  Below 1 KiB an entry's fixed costs, its key and its slot in the cache,
  are already a large share of what it costs, and each doubling covered
  adds 32 classes, so a value or page under 1 KiB stays in an allocation
  of its own, as does one above the largest class; both are charged
  jemalloc's size class. A cache keeps values in slabs only while its
  budget is at least 16 times one chunk of every class in its range,
  274,171,904 bytes (about 261 MiB) for the row cache and 477,595,648
  bytes (about 455 MiB) for the page cache, whose largest page in slabs
  is twice a value page, so its reserve holds a chunk being filled in
  every class at once; under that, values stay on jemalloc and no reserve
  is held, so a chunk a larger budget left with a free slot is emptied.
- **Chunks.** A slab holds one class's slots in chunks, each allocated
  whole and holding a small fixed number of slots: a few for the largest
  classes, a few dozen for the smallest.
- **Hits without copies.** The slab wraps a slot once, when a store fills
  it, in a `Bytes` that reads the slot in place. A hit clones that
  `Bytes`, as a hit clones a value's handle today, and the slot returns to
  its slab's free list when the last clone drops.
- **The budget counts slabs.** Every slot of every chunk counts against
  the budget, free or held, so the budget bounds the caches' memory
  however the slabs fill. A cache holds a fixed share of its budget, a
  sixteenth, in reserve for its slabs' free slots and the chunks being
  emptied, and its entries take the rest, a capacity that changes only
  with the budget. A free slot within the reserve costs nothing beyond
  it, and the reserve costs that share of the entries.
- **Chunks go back.** The slab frees a chunk whose slots are all free.
  When the free slots and the chunks being emptied pass the reserve, and
  the free slots are what push them over, the cache evicts the entries of
  the chunk with the fewest held slots in the class whose free slots hold
  the most, and frees the chunk, until they are back within it, so it
  gives back memory that a shift in lengths left stranded. The page cache
  evicts such a page as its memory level evicts any, to the disk level
  when there is one, so a later read of it costs a local read and no GET.
  Each slot records which entry holds it, so the cache can find the
  entries to evict.
- **Held slots.** A slot stays held as long as anything holds its
  `Bytes`: a response until gRPC has encoded it, which for a Get or
  MultiGet holds the values it returns, any reader of a page until it is
  done with it, and the disk level's write queue until it has written an
  evicted page. A chunk being emptied takes no new values, and the slab
  frees it when its last held slot drops; a gauge reports the bytes in
  chunks waiting. Until then such a chunk counts whole against the
  reserve, and while readers hold slots of enough of them, the cache's
  memory passes the reserve, and the budget, by those chunks. An entry an
  insert caches while its slot's chunk is being marked is dropped by that
  insert. A page foyer itself caches in such a
  chunk, a fetch's it inserts after the mark or one it takes back from
  the disk level's write queue while the chunk waits, is moved to a slot
  of another chunk by the read that asked for it. Should that read be
  gone, the page cache keeps each chunk it marked while the chunk is
  being emptied, and each of its later fits, which follow each fetch it
  caches the pages of, each page it reads back from the disk level, and
  each resize, looks again in about a chunk's worth of the kept slots and
  takes out any page memory holds in one. Such a page holds its chunk
  until those fits reach its slot, counted among the chunks waiting until
  then: the other way the cache's memory passes the reserve and the
  budget.
- **Locking.** A slab takes its lock to claim and to free a slot, and a
  store copies its bytes into the slot outside the lock, since nothing
  refers to a slot until its store returns. A hit takes no slab lock.
- **The `unsafe`.** Reading a slot in place needs `unsafe`, allowed in the
  `icetray-slab` crate's `chunk` module under RFD 029 (Unsafe code),
  behind an API whose only ways in are storing bytes and dropping the
  `Bytes` it returns. Its invariants: a slot is written only while
  nothing refers to it; a chunk's memory outlives every slot in it; and
  slots lie inside their chunk, apart from each other. Tests that Miri
  runs check each, and a test of many threads storing, evicting, and
  reading at once checks every read against the bytes its key was stored
  with.

## Alternatives Considered

Classes of exactly the lengths a table's values have, which the
measurement used. Rejected because a column of varying lengths would have
a slab per length, most of each chunk empty. A length each table
declares, or one icetray detects recurring, was rejected for the same
column and for the setting or detector it adds.

Copying a value out of its slot on each hit. Rejected because the copy
cost more than the higher hit rate saved: single-task throughput fell
where it rose with hits that read the slot in place.

Charging only the slots that hold an entry. Rejected because a slab's free
slots would then be memory the budget does not see.

Lowering the entries' capacity as free slots grow. Rejected because
quick_cache shrinks its ghost list on every capacity change.

Compacting a sparse chunk by moving its entries into free slots elsewhere.
Rejected because a value a caller holds cannot move, and emptying a chunk
loses only the few dozen entries it holds at most.

A third-party slab. Rejected because none known supports both handing out
`Bytes` without a copy and emptying a chosen chunk, and its `unsafe`
would be read as icetray's own under RFD 029 anyway.

Sharding each slab's free list. Deferred until a measurement asks for it:
icetray counts the time stores, frees, and emptying wait on slab locks, and a
run with more tasks than the measurement used shows whether one lock per
class holds up. Shards would multiply the chunks being filled in each class.

Charging the page cache's pages their jemalloc size class and leaving
them on jemalloc. Rejected as a step of its own, since the slabs charge
pages what they cost and recover the rounding as well, and both land
together.

Writer guidance to keep pages under a power of two. Rejected because it
helps only tables whose value lengths are known and alike, and the slabs
make it unnecessary.

## Consequences

On the measured trace, slots of exactly an embedding's length let a
budget hold about a fifth more embeddings, and lookups that missed the
row cache fell by more than a fifth at the largest budget, with
throughput rising
([cache slabs, results](../analysis/2026-11-11-cache-slabs.md#results)).
That is an upper bound for this length: its 32-per-doubling class is
about 3% longer than the value, so a budget holds about a sixth more
entries, before the slot's record of its entry and the free slots the
budget now counts. The decided classes, chunk emptying, and the page
cache in slabs, measured with the page charge corrected beside them, are
still to be measured.

Values whose lengths already fit a jemalloc class gain nothing, and pay
the slot's bookkeeping and their class's free slots. Every entry in a
slab still makes one small allocation for its `Bytes` handle. The page
cache's memory level holds about as many pages at a budget as before,
now within it, since a slot costs about what a page was charged.

A class's free slots lie in whichever chunks the cache evicted from, so
many chunks of a class can be partly filled at once, and a cache whose
lengths spread over many classes holds a chunk being filled in each. A
store into a class with no chunk being filled allocates one, whose other
slots are free; a full cache's free slots sit near the reserve, and with
a reserve too small to hold such a chunk in every class in use, the new
chunk would pass it, be the sparsest chunk of the class with the most
free slots, and be emptied by the fit that follows the store, taking the
value just cached with it. Spread lengths, such as compressed value
pages, would then have the cache evict much of what it cached at
budgets of tens of MiB. That is why the slab floor is a chunk of every
class, and why budgets under about 261 MiB for the row cache and 455 MiB
for the page cache's memory level keep jemalloc's rounding and gain
nothing from the slabs. RFD 028's smallest budget, one value page per
shard, stands: a page part under the slab floor keeps its pages on
jemalloc and holds no reserve, so the smallest budget needs no room for
chunks.
Emptying a chunk evicts entries the cache's eviction policy did not
choose, hot ones included, and a class whose free slots hover at the
threshold would empty chunks repeatedly.

A reader that holds a value or a page keeps its chunk from going back,
and nothing bounds that wait.

The caches gain the first `unsafe` in icetray that manages shared memory,
under RFD 029's conditions, including the nightly toolchain its Miri tests
need.

RFDs 026 (what an entry weighs) and 027 (pages live in slabs) are revised
with this decision to say what the slabs will change. The operating
guide's cache bytes series change with the slabs, and the guide gains
each cache's waiting gauge and slab wait counter.
