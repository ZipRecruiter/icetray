# Cache budget

## Problem

The page cache's memory budget is a fixed size in the file, small enough
for a laptop and a small fraction of a production table, and ordered
tables' pages had a second budget beside it. A deployer already sets the
one number that bounds the process, its memory limit (RFD 012
(Deployment contract)), and then has to work out by hand what share of
it the caches may take. With the row cache of RFD 026 (Row cache) in
front of one page cache (RFD 027 (One page cache)), there is one budget
to set and a split to choose between rows and pages.

## Decision

The memory icetray caches in will be one budget, by default a fixed
share of the process's memory limit, split between the row cache and
the page cache's memory level with most of it going to rows.

- **Settings.** `server.cache_bytes` bounds the memory both caches hold,
  and `server.row_cache_share` is the part of it the row cache holds,
  the rest being the page cache's memory level.
  `server.page_cache_bytes` is removed, and a file that sets it is
  refused with a message naming the replacement.
- **Sized from the limit.** By default `server.cache_bytes` is a fixed
  share of the process's memory limit, the cgroup's or the host's as RFD
  020 (Lookup levels) reads it, and a fixed size when neither can be
  read. The limit and never the memory free sets it, so every start of a
  replica gets the same budget. The rest of the limit holds the resident
  indexes, a second generation while a refresh loads, requests in
  flight, the allocator, and the kernel's cache of `disk` tables' chunk
  files. A budget, set or derived, whose page share cannot hold one
  value page per shard is refused when the caches open, naming the
  limit it came from when it was derived.
- **Rows get most of it.** On a sampled production trace of a
  recommendation service's lookups, simulated against quick_cache in
  front of foyer's page cache, the whole budget given to rows missed
  least at every row size from 512 bytes to 16 KiB and every budget from
  1 to 25 percent of the table. That held with the hottest rows packed
  onto the same pages, the page cache's best case, because a page also
  holds its cold neighbors. At a tenth of the table, rows served about
  twice the lookups from memory that pages alone did. The page cache's
  memory level keeps a share, for a few points of misses, to hold
  `bloom` key pages, which every lookup of such a table reads, row-cache
  hits included, dictionary pages, a request's neighboring keys, and the
  pages on their way to the disk level.
- **Following the tables.** When no table the server serves reads
  through the row cache, the page cache's memory level holds the whole
  budget. Both settings apply on restart, as every server setting does
  (RFD 018 (Configuration reload)); a reload that changes whether any
  table reads through the row cache resizes the two caches.
- **A mixed server chooses its split.** Without a disk level, which is
  the local directory with a nonzero `page_cache_disk_bytes`, a file
  that serves a table reading through the row cache beside one reading
  pages alone, an ordered table or a key-value table with `row_cache =
  false`, is refused unless it sets `server.row_cache_share`, with a
  message saying to configure the disk level or set the share. Those
  pages then live in the page cache's memory level alone, and no default
  share suits both kinds of table there. The doctor reads the same file
  through the same checks, so it reports the refusal before a deploy.
- **Checked against the limit.** The caches open before any table loads,
  so once the tables load icetray applies RFD 012's sizing rule in
  reverse: the predicted resident bytes of every table, plus the largest
  table's once more, since a refresh that rewrites it wholesale loads its
  next generation beside it, plus the budget, plus a third of that sum,
  may not exceed the limit. A shortfall is logged as a warning naming
  the bytes, and a gauge holds the bytes missing, zero when none are, so
  an operator alerts on one series as RFD 020 has every alert; a reload
  measures it again. It is a warning and not a refusal, since it depends
  on the replica's memory, which the doctor does not see. RFD 020's
  check of `disk` chunk files counts the budget in place of the caches'
  current fill.
- **Reported by the doctor.** The doctor reports the bytes of each value
  column in the snapshot, from the footers it already reads, so an owner
  can compare a table's hot share with the budget.

## Alternatives Considered

A fixed default budget. Rejected because no fixed size suits both a
laptop and a production replica, and the fixed default was a laptop's.

A budget sized from the memory free. Rejected because free memory varies
with load, so a restart would get a different budget.

A budget per cache, as before. Rejected because the deployer would have
to choose a split the trace shows has one good answer for lookups.

A split chosen from the tables served, with no refusal. Rejected because
a server mixing row-cache tables with pages-only tables and no disk
level has no share that suits both, and a guess would hide the choice.

A warning in place of the mixed-server refusal. Rejected because a log
line alone goes unread, and the case is decidable from the file.

## Consequences

A replica's memory limit is now set first and the budget follows it:
RFD 012's sizing rule, which added the page cache's budget to the
predicted indexes, becomes the check above. A configuration that sets
`page_cache_bytes` fails at start, which before 1.0 is allowed in place
(`CONTRIBUTING.md`, Stability). The largest table counted twice can
overstate the need of a server whose refreshes only append; the gauge
says how much is missing, and an operator who knows better sets
`cache_bytes`.

Earlier RFDs and documents are revised to match: RFD 011 (Configuration)
(the settings); RFD 012 (the sizing rule and its gauge); RFD 016
(Doctor) (the mixed-server refusal and the value bytes); RFD 018 (a
reload may resize the caches); RFD 020 (the room it checks under the
limit counts the budget); the configuration reference and the operating
guide.
