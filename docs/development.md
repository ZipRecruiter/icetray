# Building and testing

`rust-toolchain.toml` pins the stable toolchain and the `rustfmt` and
`clippy` components. The build carries its own protobuf compiler.

```sh
cargo build --release
cargo test --workspace
dev/ci-checks.sh
```

`dev/ci-checks.sh` is the lint gate: `cargo fmt --all --check` and `cargo
clippy --workspace --all-targets --locked -- -D warnings`. CI runs that
script and `cargo test --workspace --locked`.

## The development environment

`.flox/env/manifest.toml` declares an optional [flox](https://flox.dev)
environment. `flox activate` puts on PATH rustup, which installs the
pinned toolchain; colima, the docker client, and docker-compose, which
`dev/runtime.sh` drives for the container-gated suites and the golden
tables; grpcurl, for calls to a running server; and ghz and samply, which
the performance sweep uses to load the gRPC server and profile it.
Nothing in CI, the hooks, or the `dev/` scripts needs it, and any of the
same tools installed another way work as well.

```sh
flox activate -- cargo test --workspace
flox activate -- grpcurl -plaintext -proto crates/icetray-proto/proto/icetray.proto \
  -d '{"table": "long_2m"}' 127.0.0.1:7433 icetray.v1.Icetray/TableInfo
```

The server offers no gRPC reflection, so grpcurl and ghz are given the
proto file (`-proto` for grpcurl, `--proto` for ghz).

## Platform notes

On Linux, the tests of `disk` residency need their temporary directory on
a filesystem that serves non-blocking reads of cached pages, such as ext4,
xfs, or btrfs. Where `/tmp` is tmpfs or overlayfs, as on some
distributions and in most containers, point `TMPDIR` at a directory on
disk.

On macOS, a machine that once ran Docker Desktop keeps
`"credsStore": "desktop"` in `~/.docker/config.json`. Every `docker pull`
from the host then waits on Docker Desktop's credential helper, which
hangs while Desktop is stopped, even with colima as the daemon: pulls
stall with no output while `docker run` of a local image works. Remove
that line, or point `DOCKER_CONFIG` at a directory whose `config.json`
holds `{}`. `dev/runtime.sh up` warns when the config it uses names that
helper.

## Gated suites

Two suites need fixtures and show as ignored under a plain `cargo test`.

- `dev/test-int.sh` runs the REST catalog tests against a catalog at
  `ICETRAY_REST_URI` (default `http://127.0.0.1:8181`). `dev/runtime.sh up`
  then `dev/rest-fixture.sh up` starts `apache/iceberg-rest-fixture:1.10.1`
  in a container.
- `dev/test-golden.sh` runs the layout tests against the golden tables
  `dev/golden-tables.py` writes: one table per key type in the optimal
  layout, one in table format version 3 (the rest are version 2, the
  writer's default), the other layouts whose capping and advice
  findings the doctor reports, the contract tables Spark then changes with
  merge-on-read, and the ordered tables (many rows per key, each with an
  order value) that Spark appends to, MERGEs into, and compacts. With
  `--reference` it runs the reference group's tests instead (below).

```sh
dev/golden-tables.sh
ICETRAY_GOLDEN_WAREHOUSE=file://$PWD/target/golden-warehouse dev/test-golden.sh
```

`dev/golden-tables.sh` runs the recipe with Spark 4.1.3 and Iceberg 1.11.0
(`iceberg-spark-runtime-4.1_2.13`) in the `apache/spark:4.1.3-python3`
image, which carries Java 21, so the host needs a container runtime
(`dev/runtime.sh up`) and `python3`. `--only contract` (the tables in the
optimal layout), `--only off-contract` (the other layouts), `--only mor`
(the merge-on-read tables), or `--only ordered` (the ordered tables, about
100 MB together) writes one group, and `--only` with a table's name writes
that table alone: `--only long_2m` writes about 80 MB.
Three ordered tables set a writer property the others leave at the recipe's
contract value, each to produce the layout it exists to test:
`ordered_overlap_100k` lowers `write.spark.advisory-partition-size-bytes` to
1 MiB, since skew splitting never cuts a bucket of 100,000 small rows at
Iceberg's own size; `ordered_string_100k` sets
`parquet.columnindex.truncate.length` to 64, the writer's default, since the
contract's 256 would not cut its 80-byte order values; and
`ordered_rolled_100k` compacts with a `target-file-size-bytes` of 512 KiB,
about a quarter of a bucket, so each bucket rolls into several files that
share edge keys. The recipe also
runs on the host with PySpark 4.1 and Java 17 or later, and writes to any
object store or local directory:

```sh
pip install 'pyspark==4.1.*'
python3 dev/golden-tables.py --warehouse "file://$PWD/target/golden-warehouse"
```

The first run of either downloads the Iceberg runtime from Maven
Central. The groups together are about 4.4 GB, most of it the two
tables of 16 KiB values.

A full run leaves out one more group, `reference`: five ordered tables
near a production table's size, which `--only reference` writes. Each
holds 200,000 long keys in 64 buckets and about 20 million rows: most
keys hold 1 to 20 rows, about one in ten holds hundreds, and about one in
170 holds 1,000 to 10,000. Each row's order value is a microsecond
instant, and its value 128 bytes of hex digests; the testkit's
`golden.rs` derives every row from its key's index, and the tests read
the key count from the table property `golden.reference-keys`. Spark
writes each table as an initial load and appends of later rows of some
keys, four of them, eight in `ref_large_appended`, then `ref_compacted`,
`ref_string`, and `ref_large_files` compact it with a sort by key and
order value:

- `ref_compacted`: the recipe's sizes, one file of one row group a bucket.
- `ref_appended`: the writes uncompacted, five files a bucket.
- `ref_string`: `ref_compacted` with each instant as ISO-8601 UTC text.
- `ref_large_files`: row groups of 6 MiB, about four in each bucket's file.
- `ref_large_appended`: the initial load in `ref_large_files`' sizes, then
  eight appends uncompacted: nine files a bucket, 576 in all, the first
  of three or four row groups and each append's of one, 2.2 GB, most of
  the appends' bytes their 1 MiB bloom filters. A load's uniqueness
  merges start a cursor on each of its files, more than the reads a load
  of four worker threads holds in flight: four times icetray-core's
  `LOAD_FILES_PER_WORKER` (`load_concurrency()`).

After each compaction the recipe expires the older snapshots and
removes the files no snapshot lists, so a table root holds only its live
files. On the 4-CPU, 8 GiB colima VM `dev/runtime.sh` starts, the group
takes an estimated five minutes, about a minute a table: the four other
tables took about four minutes together, and `ref_large_appended` alone
(`--only ref_large_appended`) took about 50 seconds, but the five have
not been timed as one run. The warehouse grows by about 9 GB. The VM's
disk also grows while Spark spills, and does not shrink, so keep about
twice that free before writing the group.
`--reference-keys` writes fewer keys for a quick run, and the tests
derive the rows of whatever count the tables record; below the full size
they check the advice as a floor and leave the row groups unchecked,
since both follow the bytes per bucket. `dev/test-golden.sh` skips the
group's tests, `test_golden_reference_*`, and `dev/test-golden.sh
--reference` runs them alone:

```sh
dev/golden-tables.sh --only reference
dev/golden-tables.sh --only reference --reference-keys 20000
ICETRAY_GOLDEN_WAREHOUSE=file://$PWD/target/golden-warehouse dev/test-golden.sh --reference
```

The tables' metadata names every file by the absolute location it was
written at, so a copy of the warehouse elsewhere, such as on object
storage, is read through rewrite rules. `ICETRAY_GOLDEN_REWRITE` holds
`FROM=TO` rules separated by `;`, applied to every location as
`[[storage.rewrite]]` applies them, first match first; the Rust tests
add them to their configuration, and the binary's tests pass each as a
`--rewrite`. Spark names a local warehouse's tables as `file:///dir` and
their manifest lists and data files as `file:/dir`, so a copy takes a
rule for each form:

```sh
ICETRAY_GOLDEN_WAREHOUSE=s3a://bucket/copy \
ICETRAY_GOLDEN_REWRITE="file://$PWD/target/golden-warehouse=s3a://bucket/copy;file:$PWD/target/golden-warehouse=s3a://bucket/copy" \
dev/test-golden.sh --reference
```

The benchmark program serves the same tables from a warehouse or its
copy with `--reference`, and measures their Scans, `MultiScan`s, and
loads in every tier, with each GET's latency by the pages it spans; its
module doc and `--help` give what it runs, and it takes the copy's rules
as repeated `--rewrite-location` flags:

```sh
cargo run --profile profiling -p icetray-testkit --example bench -- \
  --reference s3a://bucket/copy \
  --rewrite-location "file://$PWD/target/golden-warehouse=s3a://bucket/copy" \
  --rewrite-location "file:$PWD/target/golden-warehouse=s3a://bucket/copy"
```

A load's resident-set peak comes from a sampler thread, whose own CPU
is left out of the load's `cpu_us` and reported as `sampler_cpu_us`.
On Linux it reads `/proc/self/statm`, whose resident count is the
kernel's approximate per-process counter, so a peak from it is not
byte-comparable with one an earlier run read from `/proc/self/smaps`,
as Stage 4 of [ordered rows on
S3](analysis/2026-10-19-ordered-rows-on-s3.md) did. The report's
`settings.rss_sampling` says which source a run read.

The tables are also the quickest way to try the binary: `icetray get
--location target/golden-warehouse/db/long_2m --value-column value
--column value 42` answers from one of them, and
the README's [Try it locally](../README.md#try-it-locally) walks through
`get`, `doctor`, and `serve`.

## Scale probe

An example program that writes 1,500 files under a temporary directory,
loads them, and times lookups and a refresh:

```sh
cargo run -p icetray-testkit --example scale --release
```

## Residency sweep

An example program that measures how `disk` lookups slow as the kernel's
page cache drops a table's chunk files, the measurement behind the
thresholds of the page-cache condition in
[RFD 020](rfd/020-lookup-levels.md). It runs on Linux only, with its
directories on ext4 or xfs, and first prepares the benchmark program's
scattered table, several gigabytes, under `target/bench/tables`:

```sh
dev/residency-sweep.sh
```

`ICETRAY_RESIDENCY_TABLES` moves the table's directory and
`ICETRAY_RESIDENCY_LOCAL` the chunk files' (default
`target/residency/local`); keep each on a local ext4 or xfs filesystem.
Arguments after the script's name go to the sweep, as
`--resident-per-mille 1000,950,900` narrows it.

## Merge-on-read measurements

An example program that makes the measurements
[RFD 021](rfd/021-merge-on-read.md) calls for before release, in process
and over local tables it writes and removes:

```sh
dev/merge-on-read-bench.sh
```

- `probe` times hits and misses at 1, 2, 4, 8, and 16 files per bucket in
  every tier, over three tables: `control`, whose newer files hold other
  keys of the same range and nothing is deleted; `updated`, whose every
  commit rewrites the keys and deletes the older copies; and `shadowed`,
  whose newer files hold deleted copies of the keys, so a hit passes a
  deleted row in every newer file before the live one. It then times a
  key-page decode and a deleted-row check apart and prints their ratio
  beside the RFD's threshold of a tenth.
- `memory` compares the delete state's bytes with the bound the budget
  counts, at deleted fractions from 0.1% to 50%, for each delete form,
  with the deleted rows in one run or scattered.
- `load` times a first load with up to 1,000 live delete files and a
  refresh onto one more, with their GETs.

It runs anywhere and finishes in minutes on four cores at its defaults;
numbers for `docs/performance.md` come from the Linux host. The tables go
under `ICETRAY_MOR_WORK` (default `target/merge-on-read/work`) and the
results under `target/merge-on-read`. Arguments after the script's name go
to the program, as `--measure probe --depths 1,16` narrows it;
`cargo run -p icetray-testkit --example deletes --release -- --help` lists
them.

## Releases

A release is a tag. Set `version` under `[workspace.package]` in
`Cargo.toml` and run `cargo check`, which moves the workspace crates'
entries in `Cargo.lock` to the new version. Commit `Cargo.toml` and
`Cargo.lock` together: CI and the release build pass `--locked`, so a
bump that leaves the lock behind fails both. Then tag that commit with
the same version and push the tag:

```sh
version=0.2.0
git tag "v$version"
git push origin "v$version"
```

The release workflow (`.github/workflows/release.yml`) refuses a tag that
differs from the crate version. It builds `icetray` with `--release
--locked` for `x86_64-unknown-linux-gnu` and `aarch64-unknown-linux-gnu`,
each on Ubuntu 24.04, so the binaries run on that release's glibc or
newer, and publishes a GitHub Release with one archive per target and its
SHA-256 sum. Each archive holds the binary, the README, and both licenses.
It also pushes a container image for both architectures to the GitHub
Container Registry, `ghcr.io/<owner>/icetray:<version>`
([Deploying](operating.md#deploying) explains the placeholders), built
from the same binaries on Ubuntu 24.04 by the `Dockerfile`. A release
`X.Y.Z` also moves the `X.Y` tag, and from 1.0 on the `X` tag, when no
release of that line is newer, checked against the repository's `v*`
tags; a pre-release moves neither. Run by
hand from the Actions tab, the workflow builds the same archives as run
artifacts and the same image, and publishes nothing.
