# Design decisions

Each RFD records one decision and why it was made;
[RFD 001](001-how-to-rfd.md) describes the format. The number identifies
an RFD and implies nothing about order.

| RFD | Title | Decision |
|---|---|---|
| [001](001-how-to-rfd.md) | How to RFD | Decisions are recorded as short files here, each stating its decision in one affirmative sentence and updated in place when it changes. |
| [002](002-serving-layout-contract.md) | Serving layout | Tables are served from a layout their writer produces, the optimal layout (bucketed, sorted, 1,000-row pages, no value dictionary, a key bloom filter), checked on the written files. |
| [003](003-one-storage-stack.md) | One storage stack | Every byte read from object storage passes through one layer that owns location rewriting, request counting, and credentials. |
| [004](004-resident-key-chunk-index.md) | Resident key-chunk index | Each data file's footer, page indexes, key chunks, exact key range, and fence array stay in memory, so a hit fetches at most one value page per column it names and a miss fetches nothing. |
| [005](005-generations-and-refresh.md) | Generations and refresh | A table follows one ref, builds each new snapshot beside the current one while reusing unchanged files, and swaps it in atomically. |
| [006](006-lookup-hot-path.md) | Lookup hot path | Lookups decode keys and values with the Parquet column reader over in-memory byte segments, with Arrow kept off the path. |
| [007](007-serving-tiers.md) | Serving tiers | A table that cannot be served correctly is refused, a finding that adds work per lookup is served with its penalty measured and caps the table's lookup level (RFD 020), and advice never affects serving. |
| [008](008-keys.md) | Keys | One key column of type long, string, binary, or uuid, moved as bytes unchanged, with keys over 4,096 bytes refused. |
| [009](009-writes-between-builds.md) | Writes between builds | Appends and copy-on-write snapshots are served, with a ref advanced behind compaction as the pattern for merge-on-read tables that need `optimal`; its refusal of every delete file is replaced by RFD 021. |
| [010](010-transport-and-client-contract.md) | Transport and client contract | Clients speak gRPC through the checked-in protobuf service (Get, MultiGet, Scan, MultiScan, TableInfo), and every answer carries its snapshot id. |
| [011](011-configuration.md) | Configuration | Configuration is TOML, the same schema is accepted as JSON, and state icetray persists for itself is versioned protobuf. |
| [012](012-deployment-contract.md) | Deployment contract | icetray is operated through a twelve-factor surface alone (a config path, platform credentials, JSON logs, Prometheus metrics, health and readiness) with no deployer-specific code. |
| [013](013-tail-latency.md) | Tail latency | Lookup GETs are hedged after a delay taken from observed latency and capped by a hedge-rate ceiling, and load reads are never hedged. |
| [014](014-test-layering.md) | Test layering | Correctness and cost are proven in layers, from unit tests to golden tables written by Spark, with the GET counter as the oracle. |
| [015](015-lint-policy.md) | Lint policy | Clippy's pedantic group is denied everywhere with four named exceptions in one workspace table and two scoped carve-outs. |
| [016](016-doctor.md) | Doctor | A `doctor` command and the same check at every load report the lookup level a table's layout achieves, the findings that cap it with their penalties, and advice with the writer property to change. |
| [017](017-residency-tiers.md) | Residency tiers | Where a table keeps its key chunks (`full`, `disk`, or `bloom`) is an explicit per-table setting chosen by the owner, with a local directory for the disk files and the page cache's disk level. |
| [018](018-configuration-reload.md) | Configuration reload | A change to the file's tables is applied to a running server by loading the changed tables beside the serving ones and swapping the whole set, or refused whole. |
| [019](019-aws-catalog-sources.md) | AWS catalog sources | Tables in Glue and S3 Tables are resolved to their metadata location through the AWS SDK with the storage credentials, and their metadata is read through the one storage layer. |
| [020](020-lookup-levels.md) | Lookup levels | Each table declares the lookup cost it accepts as `optimal`, `balanced`, or `permissive`; icetray measures the level each table achieves, flags a table below its level, and under `strict` refuses the snapshot whose layout put it there, with every alert condition a single series. |
| [021](021-merge-on-read.md) | Merge-on-read | Snapshots carrying deletion vectors or Parquet position deletes are served, with each data file's deleted rows held in memory and checked before a lookup reads a value page; equality deletes stay refused, and the level follows RFD 020 with no cap of its own. |
| [022](022-value-columns.md) | Value columns | A table names the value columns it serves, none or several, each read only when a lookup names it, from one key index and one generation; one packed column performs best, and a row read by several kinds of caller splits along how it is read. |
| [023](023-column-resolution.md) | Column resolution | A configured column is found in each data file by top-level name, checked against the snapshot schema's field id where the file records field ids; a column the file provably lacks answers its Iceberg default, and every other mismatch refuses the snapshot. |
| [024](024-field-projection.md) | Field projection | A request may trim a value of a column declared protobuf to the top-level field numbers it names, after the page cache, with the whole value returned when it names none; a caller that names a oneof member names every member. |
| [025](025-ordered-rows.md) | Ordered rows per key | A table configured with an order column serves many rows per key, sorted by key and order value and unique as a pair, which a caller reads newest or oldest first under a limit and optional order bounds through Scan and MultiScan; a table without one is a key-value table with every check and cost it had. |
| [026](026-row-cache.md) | Row cache | Key-value lookups read each value from a row cache in memory, keyed by data file, row position, and the file's column and on by default, before the page cache; a deleted or rewritten row is never served from it, since the index resolves the row first. |
| [027](027-one-page-cache.md) | One page cache | Every table's pages share one page cache, its memory level in front of the disk level, and a Scan takes each page from memory, then the disk level, fetching only the runs neither holds. |
| [028](028-cache-budget.md) | Cache budget | The caches share one budget, by default a share of the memory limit, split mostly to rows, with a mixed server without a disk level choosing its split and a gauge for a budget the limit cannot hold. |
