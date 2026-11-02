#!/usr/bin/env python3
"""Write the golden Iceberg tables the layout tests read.

One contract-shaped table per key type, one more in table format version 3,
the off-contract layouts whose serving cost RFD 007 prices and whose
findings RFD 016 reports, and contract tables Spark then changes with
merge-on-read (RFD 021): position deletes in format version 2, deletion
vectors in version 3, and ordered tables (RFD 025): many rows per key,
each with an order value, written by appends, skew-split writes, a MERGE,
and a sorting compaction. The reference group, written only when --only
names it, holds ordered tables near a production table's size: about 20
million rows each, about 9 GB together. Run it against any object store
or local directory; the tests locate the result through
ICETRAY_GOLDEN_WAREHOUSE.

    pip install 'pyspark==4.1.*'
    python3 dev/golden-tables.py --warehouse s3a://bucket/prefix/warehouse
    python3 dev/golden-tables.py --warehouse file:///tmp/icetray-golden
    python3 dev/golden-tables.py --only long_2m --warehouse file:///tmp/icetray-golden
    python3 dev/golden-tables.py --only reference --reference-keys 20000 --warehouse file:///tmp/icetray-golden

Without Spark on the host, dev/golden-tables.sh runs this script with Spark in
a container against a warehouse under target/.

Needs Spark 4.1 (Java 17 or later) with the Iceberg runtime on its classpath;
--packages adds it from Maven when it is absent. 4.1 is the newest Spark line
Iceberg 1.11 ships a runtime for, and the runtime's Scala suffix (2.13) must
match the Spark build's. A Hadoop catalog is used so that a table root
and its version hint are the only handles a reader needs, with no catalog
service to run.

Keys are spaced by three, so key + 1 is always absent and inside the table's
key range: that is the miss a test wants. Values are a function of the key, so
an answer is checkable without a stored oracle; in a table merge-on-read
changes, of the key and the version that wrote it (RFD 014), so a lookup
that answered from a deleted copy is told apart from one that answered from
the live row.

What this script cannot write, and the in-process writer does instead: a
column of Iceberg's own `uuid` type (Spark SQL has no UUID type to declare),
an unsorted file, a null key, a key column whose physical type contradicts
the schema, and an equality delete, which Spark's row-level commands never
write.
"""

import argparse
import time
from datetime import datetime, timezone
from typing import NamedTuple, Optional

ICEBERG_PACKAGE = "org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.11.0"
CATALOG = "golden"

# Decimal gigabytes, the unit the per-table line prints file bytes in.
GIGABYTE = 1_000_000_000

# The shapes below mirror the testkit's golden.rs and keys.rs constants of
# the same names; a test derives keys and values from those, so a change
# here without one there breaks the golden tests.

# Keys are spaced by this, so key + 1 is always absent inside the range.
KEY_STEP = 3
# Padding before a string key, so a table has long keys sharing a prefix.
STRING_PREFIX_LEN = 70
# Digits a string key's number is zero-padded to, so string order is
# numeric order.
STRING_DIGITS = 16
# A bijection for id below the modulus (keys unique and uncorrelated with
# input order): the multiplier is the 32-bit golden-ratio constant, the
# modulus a prime above every id the recipe writes.
PERMUTATION_MULTIPLIER = 2_654_435_761
PERMUTATION_MODULUS = 1_000_000_007
# 64-byte digests per value: a contract value is small, a wide one 16 KiB,
# the size a row limit alone leaves in pages of megabytes.
CONTRACT_REPEATS = 3
WIDE_REPEATS = 256

# Rows per table; the wide tables take fewer, since each of their rows
# carries 16 KiB.
ROWS_2M = 2_000_000
ROWS_1M = 1_000_000
ROWS_500K = 500_000
ROWS_250K = 250_000
ROWS_100K = 100_000
ROWS_50K = 50_000
# Bucket counts: four for the contract tables, two for the wide and overlap
# tables, none for the unpartitioned table.
FOUR_BUCKETS = 4
TWO_BUCKETS = 2
UNPARTITIONED = 0

# RFD 002. Every entry is a standard Iceberg property or a parquet-mr key that
# Iceberg passes through unchanged.
CONTRACT = {
    "parquet.enable.dictionary": "false",
    "parquet.columnindex.truncate.length": "256",
    "write.parquet.page-row-limit": "1000",
    "write.parquet.page-size-bytes": "8388608",
    "write.parquet.row-group-size-bytes": "134217728",
    "write.parquet.compression-codec": "zstd",
    "write.parquet.bloom-filter-enabled.column.key": "true",
    "write.parquet.bloom-filter-fpp.column.key": "0.01",
    "write.metadata.metrics.column.key": "full",
    "write.distribution-mode": "hash",
    "write.target-file-size-bytes": "67108864",
}

# Large values need a page byte bound, and the bound only binds if the writer
# checks it often enough: RFD 002, prototype findings finding 6.
WIDE = {
    "write.parquet.page-size-bytes": "65536",
    "write.parquet.row-group-check-min-record-count": "4",
    "write.parquet.row-group-check-max-record-count": "16",
}

# Iceberg 1.11 creates a table in format version 2 unless told otherwise;
# this asks for version 3 (row lineage, deletion vectors), the version a
# table owner on a current writer may already have chosen.
FORMAT_V3 = {"format-version": "3"}
# Version 2 named outright for the merge-on-read tables, whose delete kind
# (position delete files, not deletion vectors) follows from it.
FORMAT_V2 = {"format-version": "2"}

# Every row-level command records its change as deletes beside new data
# files rather than rewriting the files it touches: Parquet position deletes
# in format version 2, deletion vectors in Puffin files in version 3.
MERGE_ON_READ = {
    "write.delete.mode": "merge-on-read",
    "write.update.mode": "merge-on-read",
    "write.merge.mode": "merge-on-read",
}

# The bloom tables' keys per file: their rows over their buckets, one file and
# one row group per bucket. Iceberg's default filter, 1 MiB whatever the key
# count, is far above what these need at the contract's fpp, which the
# doctor reports as `bloom_oversized`.
BLOOM_KEYS_PER_FILE = ROWS_100K // FOUR_BUCKETS
# A cap under that default and above what `BLOOM_KEYS_PER_FILE` keys need at
# the contract's fpp, so a writer that honors it writes a filter the doctor
# finds sized for its keys, and one that ignores it writes the default.
BLOOM_MAX_BYTES = 65_536

# The ids a merge-on-read table's DELETE removes and its UPDATE rewrites,
# half-open; disjoint, inside the table's rows, and each spread over every
# bucket. The golden tests derive every answer from these and the versions.
MOR_DELETED_IDS = range(10_000, 20_000)
MOR_UPDATED_IDS = range(50_000, 60_000)
# The version a merge-on-read table's INSERT writes and the one its UPDATE
# writes: a value is derived from "v<version>:<key>".
MOR_FIRST_VERSION = 1
MOR_UPDATE_VERSION = 2

# An ordered table holds this many keys of this many rows each, so ROWS_100K
# rows. A key's rows are written at order values ORDER_START + j * ORDER_STEP
# for j below the row count, and the step leaves room between two rows for
# the rows a MERGE inserts there.
ORDERED_KEYS = 1_000
ORDERED_ROWS_PER_KEY = 100
ORDER_START = 1_000
ORDER_STEP = 10
# The order column's name; the table is sorted by the key and then by it.
ORDER_COLUMN = "ts"
# Rows per key the append adds, at the order values after the key's last.
APPEND_ROWS_PER_KEY = 10
# The ids whose keys a MERGE changes, half-open: for each, every second row
# is matched and updated, and one row in ten is inserted between two rows,
# MERGE_INSERT_OFFSET above the row before it.
MERGE_KEY_IDS = range(300, 400)
MERGE_UPDATE_EVERY = 2
MERGE_INSERT_EVERY = 10
MERGE_INSERT_OFFSET = 5
# The bytes of a string order value's prefix, derived from its key and above
# Iceberg 1.11's default `parquet.columnindex.truncate.length`, so the
# writer cuts the order column's column index; the prefix is followed by the
# row's order value zero-padded to ORDER_STRING_DIGITS.
ORDER_PREFIX_LEN = 64
ORDER_STRING_DIGITS = 16
COLUMN_INDEX_TRUNCATE_DEFAULT = 64
# The bytes of shuffled data past which a skew-splitting write cuts a bucket's
# partition into another sorted task: Iceberg's own default is sized for
# files of a target size, far above what a hundred thousand small rows
# amount to, so the overlap table lowers it to a few tasks per bucket.
SPLIT_PARTITION_BYTES = 1_048_576
# The compaction's target file size for the rolled table: about a quarter of
# a bucket's bytes, so the compaction writes several files per bucket.
ROLLED_TARGET_FILE_BYTES = 524_288

# The reference tables: ordered tables near a production table's size,
# every row a function of its key's index `id` and its row index `j`
# within the key, in integer arithmetic the testkit's golden.rs mirrors.
# Keys are `LONG_KEY` of the index. The table property names the key count
# the recipe wrote, so a test derives a table `--reference-keys` scaled down.
REFERENCE_KEYS = 200_000
REFERENCE_BUCKETS = 64
REFERENCE_KEYS_PROPERTY = "golden.reference-keys"
# A reference value is two distinct digests, 128 bytes that zstd reduces
# about twofold.
REFERENCE_REPEATS = 2
# Rows per key, heavy-tailed: `PERMUTED_KEY` of the index, below
# `TIER_SCALE`, picks the tier, and the hash's higher digits pick the count
# within the tier's inclusive range. About 0.6% of keys are heavy, 10% hold
# hundreds of rows, and the rest at most twenty; about 20 million rows.
TIER_SCALE = 1_000
HEAVY_TIER = 6
MEDIUM_TIER = 110
HEAVY_ROWS = (1_000, 10_000)
MEDIUM_ROWS = (100, 999)
LIGHT_ROWS = (1, 20)
# Each key's last rows (one in `APPEND_TAIL_DIVISOR`, rounded down, so keys
# of fewer rows have none) are written by the table's appends after the
# initial load, `APPEND_BATCHES` of them unless it sets its own count, the
# tail split evenly in order among them, so an append holds later rows of
# some keys only.
APPEND_TAIL_DIVISOR = 5
APPEND_BATCHES = 4
# A row's order value in microseconds: `TS_START`, 2026-01-01T00:00:00Z,
# plus `TS_STEP` per row index plus a jitter below `TS_STEP` hashed from the
# row, so a key's values strictly increase with gaps under two steps.
# `ROW_SLOTS` is at least any key's row count, so the value hashed,
# `id * ROW_SLOTS + j`, is unique to the row.
TS_START = 1_767_225_600_000_000
TS_STEP = 1_000_000
ROW_SLOTS = HEAVY_ROWS[1]
# The most keys whose row hash stays below 2^63 before its `pmod`, as Spark's
# ANSI arithmetic requires.
REFERENCE_KEYS_MAX = (2**63 - 1) // PERMUTATION_MULTIPLIER // ROW_SLOTS
# Rows of a reference table at `REFERENCE_KEYS`, the sum of every key's
# count; golden.rs pins the same sum.
REFERENCE_ROWS = 19_938_958
# A string order value: the instant as fixed-width ISO-8601 UTC text, to the
# microsecond, which sorts as the instants do. Rendered in the session's
# zone, which the session sets to UTC.
ISO_INSTANT = "yyyy-MM-dd'T'HH:mm:ss.SSSSSS'Z'"
# `ref_large_files` keeps each bucket, about 24 MB of data at 20 million
# rows, in one file below this target, in row groups of about a quarter of
# it, so the file holds about four.
LARGE_FILE_BYTES = 33_554_432
LARGE_ROW_GROUP_BYTES = 6_291_456
# `ref_large_appended`'s appends: with its initial load's file, nine files a
# bucket, so its 64 buckets' uniqueness merges start 576 cursors, more than
# the reads a load of four worker threads holds in flight, which is four
# times icetray-core's `LOAD_FILES_PER_WORKER` (`load_concurrency()`).
LARGE_APPEND_BATCHES = 8

LONG_KEY = f"id * {KEY_STEP}"
STRING_KEY = (
    f"concat(repeat('p', {STRING_PREFIX_LEN}), 'user:', "
    f"lpad(CAST(id * {KEY_STEP} AS STRING), {STRING_DIGITS}, '0'))"
)
# The shape that makes a writer emit overlapping files.
PERMUTED_KEY = f"pmod(id * {PERMUTATION_MULTIPLIER}, {PERMUTATION_MODULUS})"
# Sixteen bytes that do not compress, the shape a UUID key has on the wire.
# Deterministic in id, unlike Spark's uuid(), so a value derived from the key
# agrees with it. A column of Iceberg's own `uuid` type is written by the
# in-process writer instead: Spark SQL has no UUID type to declare.
BIN16_KEY = "unhex(md5(CAST(id AS STRING)))"


def value_expr(key_expr, repeats, incompressible=False, version=None, order_expr=None):
    """64 bytes per repeat, derived from the key, or with a version from
    the text "v<version>:<key>"; in an ordered table from
    "v<version>:<key>:<order>".

    The compressible form repeats one digest; the incompressible form
    concatenates distinct digests, which zstd reduces about twofold.
    """
    text = f"CAST({key_expr} AS STRING)"
    if order_expr is not None:
        text = f"concat({text}, ':', CAST({order_expr} AS STRING))"
    if version is not None:
        text = f"concat('v{version}:', {text})"
    if not incompressible:
        return f"CAST(repeat(sha2({text}, 256), {repeats}) AS BINARY)"
    return (
        f"CAST(concat_ws('', transform(sequence(1, {repeats}), "
        f"i -> sha2(concat({text}, ':', CAST(i AS STRING)), 256))) AS BINARY)"
    )


def properties(extra=None):
    props = dict(CONTRACT, **(extra or {}))
    return ", ".join(f"'{k}' = '{v}'" for k, v in props.items())


def create(spark, table, key_type, buckets, extra=None, sorted_by_key=True, order_type=None):
    name = f"{CATALOG}.db.{table}"
    spark.sql(f"DROP TABLE IF EXISTS {name}")
    partition = f"PARTITIONED BY (bucket({buckets}, key))" if buckets else ""
    order = f"{ORDER_COLUMN} {order_type}, " if order_type else ""
    spark.sql(
        f"CREATE TABLE {name} (key {key_type}, {order}value BINARY) USING iceberg "
        f"{partition} TBLPROPERTIES ({properties(extra)})"
    )
    if sorted_by_key:
        # Sorted within each file, which is what a reader needs; a global sort
        # is not required and costs a shuffle. An ordered table sorts by the
        # order column after the key.
        by = f"key, {ORDER_COLUMN}" if order_type else "key"
        spark.sql(f"ALTER TABLE {name} WRITE LOCALLY ORDERED BY {by}")
    return name


def fill(spark, name, rows, key_expr, repeats, incompressible=False, version=None):
    spark.sql(
        f"INSERT INTO {name} SELECT {key_expr} AS key, "
        f"{value_expr(key_expr, repeats, incompressible, version)} AS value FROM range({rows})"
    )


class OrderShape(NamedTuple):
    """How an ordered table's keys and order values are derived."""

    # Keys are the permutation of the key's index, uncorrelated with the
    # order rows are written in, instead of spaced by `KEY_STEP`.
    permuted: bool = False
    # The order column is a string: a prefix derived from the key, then the
    # raw order value zero-padded; for a timed shape, the instant's ISO text.
    string: bool = False
    # The reference tables' shape: a microsecond instant per row
    # (`reference_ts`), and a varying row count per key.
    timed: bool = False


def keyed_hash(expr):
    """`PERMUTED_KEY` of `expr`: a stable hash for values below the modulus,
    exact in BIGINT for values below 2^63 over the multiplier."""
    return f"pmod(({expr}) * {PERMUTATION_MULTIPLIER}, {PERMUTATION_MODULUS})"


def reference_ts(key_index, row_index):
    """The microsecond instant of row `row_index` of the key at `key_index`
    in a reference table."""
    jitter = f"{keyed_hash(f'({key_index}) * {ROW_SLOTS} + ({row_index})')} % {TS_STEP}"
    return f"({TS_START} + ({row_index}) * {TS_STEP} + {jitter})"


def ordered_exprs(shape, key_index, row_index, offset=0):
    """The key and order expressions of row `row_index` of the key at
    `key_index`, its raw order value `offset` above the row's own."""
    key = keyed_hash(key_index) if shape.permuted else f"({key_index}) * {KEY_STEP}"
    if shape.timed:
        raw = reference_ts(key_index, row_index)
        if not shape.string:
            return key, raw
        return key, f"date_format(timestamp_micros({raw}), \"{ISO_INSTANT}\")"
    raw = f"({ORDER_START} + ({row_index}) * {ORDER_STEP} + {offset})"
    if not shape.string:
        return key, raw
    prefix = f"lpad(CAST({key} AS STRING), {ORDER_PREFIX_LEN}, 'k')"
    return key, f"concat({prefix}, lpad(CAST({raw} AS STRING), {ORDER_STRING_DIGITS}, '0'))"


def ordered_select(table, key_index, row_index, source, version, offset=0):
    """The rows of an ordered table, one per row of the query `source`, each
    with the key index and row index the expressions over its columns give,
    at `version`."""
    key, order = ordered_exprs(table.order, key_index, row_index, offset)
    value = value_expr(key, table.repeats, table.incompressible, version, order_expr=order)
    return f"SELECT {key} AS key, {order} AS {ORDER_COLUMN}, {value} AS value FROM {source}"


def fill_ordered(spark, name, table):
    """The table's first rows: `ORDERED_ROWS_PER_KEY` per key, at
    `MOR_FIRST_VERSION`."""
    select = ordered_select(
        table,
        f"id DIV {ORDERED_ROWS_PER_KEY}",
        f"id % {ORDERED_ROWS_PER_KEY}",
        f"range({table.rows})",
        MOR_FIRST_VERSION,
    )
    spark.sql(f"INSERT INTO {name} {select}")


def reference_rows():
    """A reference key's row count from its hash `h`, by tier."""
    tier = f"h % {TIER_SCALE}"
    spread = f"h DIV {TIER_SCALE}"

    def within(rows):
        low, high = rows
        return f"{low} + {spread} % {high - low + 1}"

    return (
        f"CASE WHEN {tier} < {HEAVY_TIER} THEN {within(HEAVY_ROWS)} "
        f"WHEN {tier} < {MEDIUM_TIER} THEN {within(MEDIUM_ROWS)} "
        f"ELSE {within(LIGHT_ROWS)} END"
    )


def reference_insert(spark, name, table, write):
    """An INSERT of each reference key's rows in the half-open row index
    range that `write` (0 the initial load, `b` the `b`th append) holds, at
    `MOR_FIRST_VERSION`: the initial load the rows below the key's tail,
    and each of the table's `appends` appends its share of the tail."""
    tail = f"(r DIV {APPEND_TAIL_DIVISOR})"
    base = f"(r - {tail})"
    if write == 0:
        low, high = "0", base
    else:
        batch = write - 1
        low = f"({base} + {tail} * {batch} DIV {table.appends})"
        high = f"({base} + {tail} * {batch + 1} DIV {table.appends})"
    counts = f"(SELECT id AS k, {reference_rows()} AS r FROM (SELECT id, {keyed_hash('id')} AS h FROM range({table.keys})))"
    source = (
        f"(SELECT k, explode(sequence({low}, {high} - 1)) AS j FROM {counts} WHERE {high} > {low})"
    )
    select = ordered_select(table, "k", "j", source, MOR_FIRST_VERSION)
    spark.sql(f"INSERT INTO {name} {select}")


def fill_reference(spark, name, table):
    """A reference table's initial load: most of every key's rows."""
    reference_insert(spark, name, table, 0)


def append_batches(spark, name, table):
    """The table's `appends` INSERTs of later rows of the keys with a
    tail: up to a file per bucket each, over the bucket's earlier files'
    key range."""
    assert table.appends > 0, name
    for write in range(1, table.appends + 1):
        reference_insert(spark, name, table, write)


def summarize(spark, name, started):
    """One line per table: its live rows, its data files, and its deletes."""
    counts = spark.sql(f"SELECT count(*) AS n, count(DISTINCT key) AS d FROM {name}").first()
    files = spark.sql(
        f"SELECT count(*) AS f, sum(file_size_in_bytes) AS b FROM {name}.data_files"
    ).first()
    deletes = spark.sql(
        f"SELECT count(*) AS f, sum(record_count) AS r FROM {name}.delete_files"
    ).first()
    applied = f", {deletes.f} delete files of {deletes.r:,} rows" if deletes.f else ""
    print(
        f"{name}: {counts.n:,} rows ({counts.d:,} distinct), {files.f} files, "
        f"{(files.b or 0) / GIGABYTE:.2f} GB{applied}, {time.monotonic() - started:.0f}s",
        flush=True,
    )


def ids_where(ids):
    """The keys of the half-open id range `ids` as a predicate on `key`."""
    return f"key >= {ids.start * KEY_STEP} AND key < {ids.stop * KEY_STEP}"


def delete_ids(spark, name, table):
    """A DELETE of `MOR_DELETED_IDS`: deletes alone, no data file."""
    spark.sql(f"DELETE FROM {name} WHERE {ids_where(MOR_DELETED_IDS)}")


def update_ids(spark, name, table):
    """An UPDATE of `MOR_UPDATED_IDS` to `MOR_UPDATE_VERSION`: deletes of the
    old rows plus new data files whose key ranges overlap the old files'."""
    value = value_expr("key", table.repeats, table.incompressible, MOR_UPDATE_VERSION)
    spark.sql(f"UPDATE {name} SET value = {value} WHERE {ids_where(MOR_UPDATED_IDS)}")


def rewrite_data(spark, name, table):
    """Rewrites every data file in key order, one per bucket, at the data
    sequence number the rewrite started from. In format version 2 the
    position delete files stay listed, naming files the snapshot no longer
    holds; in version 3 the commit drops the deletion vectors of the files
    it rewrote."""
    spark.sql(
        f"CALL {CATALOG}.system.rewrite_data_files(table => '{qualified(name)}', "
        f"strategy => 'sort', options => map('rewrite-all', 'true'))"
    )


def rewrite_deletes(spark, name, table):
    """Rewrites every position delete file, dropping the deletes that apply
    to no data file: the second half of an owner's compaction job."""
    spark.sql(
        f"CALL {CATALOG}.system.rewrite_position_delete_files(table => '{qualified(name)}', "
        f"options => map('rewrite-all', 'true'))"
    )


def append_later(spark, name, table):
    """An INSERT of `APPEND_ROWS_PER_KEY` more rows for every key, at the
    order values after its last: a file per bucket over the older file's key
    range, whose rows come after the older file's."""
    select = ordered_select(
        table,
        f"id DIV {APPEND_ROWS_PER_KEY}",
        f"{ORDERED_ROWS_PER_KEY} + id % {APPEND_ROWS_PER_KEY}",
        f"range({ORDERED_KEYS * APPEND_ROWS_PER_KEY})",
        MOR_FIRST_VERSION,
    )
    spark.sql(f"INSERT INTO {name} {select}")


def merge_rows(spark, name, table):
    """A MERGE on the key and the order value over `MERGE_KEY_IDS`: every
    `MERGE_UPDATE_EVERY`-th row of each key is matched and takes
    `MOR_UPDATE_VERSION`'s value, and a row at `MERGE_INSERT_OFFSET` above
    every `MERGE_INSERT_EVERY`-th row is inserted at that version."""
    keys = len(MERGE_KEY_IDS)
    updated = ORDERED_ROWS_PER_KEY // MERGE_UPDATE_EVERY
    inserted = ORDERED_ROWS_PER_KEY // MERGE_INSERT_EVERY
    matched = ordered_select(
        table,
        f"{MERGE_KEY_IDS.start} + id DIV {updated}",
        f"(id % {updated}) * {MERGE_UPDATE_EVERY}",
        f"range({keys * updated})",
        MOR_UPDATE_VERSION,
    )
    new = ordered_select(
        table,
        f"{MERGE_KEY_IDS.start} + id DIV {inserted}",
        f"(id % {inserted}) * {MERGE_INSERT_EVERY}",
        f"range({keys * inserted})",
        MOR_UPDATE_VERSION,
        offset=MERGE_INSERT_OFFSET,
    )
    spark.sql(f"CREATE OR REPLACE TEMPORARY VIEW merge_source AS {matched} UNION ALL {new}")
    spark.sql(
        f"MERGE INTO {name} t USING merge_source s "
        f"ON t.key = s.key AND t.{ORDER_COLUMN} = s.{ORDER_COLUMN} "
        f"WHEN MATCHED THEN UPDATE SET value = s.value "
        f"WHEN NOT MATCHED THEN INSERT *"
    )


def rewrite_sorted(spark, name, table):
    """Rewrites every data file sorted by the key and then the order value,
    as an owner's compaction job does."""
    spark.sql(
        f"CALL {CATALOG}.system.rewrite_data_files(table => '{qualified(name)}', "
        f"strategy => 'sort', sort_order => 'key ASC, {ORDER_COLUMN} ASC', "
        f"options => map('rewrite-all', 'true'))"
    )


def rewrite_rolled(spark, name, table):
    """`rewrite_sorted` with a target file size far below a bucket's bytes,
    so each bucket rolls into several files whose edges fall where the sort
    cuts, inside a key's run."""
    spark.sql(
        f"CALL {CATALOG}.system.rewrite_data_files(table => '{qualified(name)}', "
        f"strategy => 'sort', sort_order => 'key ASC, {ORDER_COLUMN} ASC', "
        f"options => map('rewrite-all', 'true', "
        f"'target-file-size-bytes', '{ROLLED_TARGET_FILE_BYTES}'))"
    )


def expire_replaced(spark, name, table):
    """Expires every snapshot but the current one, which deletes the files
    only they listed, then removes any file no snapshot lists, so the table
    root holds only the live files a compaction left. The cutoff is now:
    the recipe is the table's only writer while it runs. The
    `remove_orphan_files` procedure refuses a cutoff under a day, so the
    orphan pass goes through Iceberg's action API, which takes any."""
    now = datetime.now(timezone.utc)
    spark.sql(
        f"CALL {CATALOG}.system.expire_snapshots(table => '{qualified(name)}', "
        f"older_than => TIMESTAMP '{now.strftime('%Y-%m-%d %H:%M:%S.%f')}', retain_last => 1)"
    )
    jvm = spark._jvm
    iceberg_table = jvm.org.apache.iceberg.spark.Spark3Util.loadIcebergTable(spark._jsparkSession, name)
    actions = jvm.org.apache.iceberg.spark.actions.SparkActions.get(spark._jsparkSession)
    actions.deleteOrphanFiles(iceberg_table).olderThan(int(now.timestamp() * 1000)).execute()


def qualified(name):
    """The table's name within the catalog, as a procedure takes it."""
    return name.removeprefix(f"{CATALOG}.")


def skew_splitting(spark, enabled):
    """Whether one bucket's write may be split across several sorted tasks.

    With it off a bucket's files never overlap; with it on they may, which is
    the cost RFD 007 prices. Prototype findings finding 9.
    """
    spark.conf.set(
        "spark.sql.adaptive.optimizeSkewsInRebalancePartitions.enabled",
        "true" if enabled else "false",
    )


# Which group a table belongs to: the contract-shaped tables, the
# off-contract layouts, the contract tables merge-on-read changes, or the
# ordered tables. `--only` takes a group or one table's name.
CONTRACT_GROUP = "contract"
OFF_CONTRACT_GROUP = "off-contract"
MOR_GROUP = "mor"
ORDERED_GROUP = "ordered"
# Written only when `--only` names it or one of its tables: about 9 GB.
REFERENCE_GROUP = "reference"


class Golden(NamedTuple):
    """One golden table: its name and group, and how it is written."""

    name: str
    group: str
    key_type: str
    buckets: int
    rows: int
    key_expr: Optional[str]
    repeats: int
    extra: Optional[dict] = None
    incompressible: bool = False
    skew: bool = False
    # The commands run after the fill, in order; a table with any is filled
    # at `MOR_FIRST_VERSION`.
    changes: tuple = ()
    # Set on an ordered table, whose `key_expr` is unused: how its keys and
    # order values are derived, and so the order column's type.
    order: Optional[OrderShape] = None
    # A reference table's key count, `--reference-keys`; its `rows` is the
    # total at `REFERENCE_KEYS`, for the summary to be read against.
    keys: int = 0
    # A reference table's appends after its initial load, which
    # `append_batches` writes; zero outside the reference group.
    appends: int = 0


# Every golden table, in the order a full run writes them.
TABLES = [
    Golden("long_2m", CONTRACT_GROUP, "BIGINT", FOUR_BUCKETS, ROWS_2M, LONG_KEY, CONTRACT_REPEATS),
    Golden("str_2m", CONTRACT_GROUP, "STRING", FOUR_BUCKETS, ROWS_2M, STRING_KEY, CONTRACT_REPEATS),
    Golden("bin16_500k", CONTRACT_GROUP, "BINARY", FOUR_BUCKETS, ROWS_500K, BIN16_KEY, CONTRACT_REPEATS),
    Golden(
        "long_wide_250k",
        CONTRACT_GROUP,
        "BIGINT",
        TWO_BUCKETS,
        ROWS_250K,
        LONG_KEY,
        WIDE_REPEATS,
        extra=WIDE,
        incompressible=True,
    ),
    Golden(
        "long_v3_50k",
        CONTRACT_GROUP,
        "BIGINT",
        FOUR_BUCKETS,
        ROWS_50K,
        LONG_KEY,
        CONTRACT_REPEATS,
        extra=FORMAT_V3,
    ),
    # The writer's defaults: large key pages misaligned with the value pages,
    # no bloom filter, dictionary-encoded columns where the data allows.
    Golden(
        "default_layout_1m",
        OFF_CONTRACT_GROUP,
        "BIGINT",
        FOUR_BUCKETS,
        ROWS_1M,
        LONG_KEY,
        CONTRACT_REPEATS,
        extra={
            "parquet.enable.dictionary": "true",
            "write.parquet.page-row-limit": "20000",
            "write.parquet.bloom-filter-enabled.column.key": "false",
            "write.metadata.metrics.column.key": "truncate(16)",
            "parquet.columnindex.truncate.length": "16",
        },
    ),
    # The contract at `BLOOM_KEYS_PER_FILE` keys per file, under Iceberg's
    # default bloom filter, then under each of the two properties that size
    # it: whether this writer honors each is what the pair records.
    Golden("bloom_default_100k", OFF_CONTRACT_GROUP, "BIGINT", FOUR_BUCKETS, ROWS_100K, LONG_KEY, CONTRACT_REPEATS),
    Golden(
        "bloom_ndv_100k",
        OFF_CONTRACT_GROUP,
        "BIGINT",
        FOUR_BUCKETS,
        ROWS_100K,
        LONG_KEY,
        CONTRACT_REPEATS,
        extra={"write.parquet.bloom-filter-ndv.column.key": str(BLOOM_KEYS_PER_FILE)},
    ),
    Golden(
        "bloom_max_bytes_100k",
        OFF_CONTRACT_GROUP,
        "BIGINT",
        FOUR_BUCKETS,
        ROWS_100K,
        LONG_KEY,
        CONTRACT_REPEATS,
        extra={"write.parquet.bloom-filter-max-bytes": str(BLOOM_MAX_BYTES)},
    ),
    # No bucket partition: every file is a candidate, narrowed by key range.
    Golden(
        "unpartitioned_500k",
        OFF_CONTRACT_GROUP,
        "BIGINT",
        UNPARTITIONED,
        ROWS_500K,
        LONG_KEY,
        CONTRACT_REPEATS,
    ),
    # Overlapping files without duplicate keys: several sorted tasks per
    # bucket over keys uncorrelated with input order.
    Golden(
        "overlap_250k",
        OFF_CONTRACT_GROUP,
        "BIGINT",
        TWO_BUCKETS,
        ROWS_250K,
        PERMUTED_KEY,
        WIDE_REPEATS,
        extra=WIDE,
        incompressible=True,
        skew=True,
    ),
    # A DELETE then an UPDATE, each recorded as Parquet position deletes.
    Golden(
        "long_mor_v2_100k",
        MOR_GROUP,
        "BIGINT",
        FOUR_BUCKETS,
        ROWS_100K,
        LONG_KEY,
        CONTRACT_REPEATS,
        extra=dict(FORMAT_V2, **MERGE_ON_READ),
        changes=(delete_ids, update_ids),
    ),
    # The same commands recorded as deletion vectors.
    Golden(
        "long_mor_v3_100k",
        MOR_GROUP,
        "BIGINT",
        FOUR_BUCKETS,
        ROWS_100K,
        LONG_KEY,
        CONTRACT_REPEATS,
        extra=dict(FORMAT_V3, **MERGE_ON_READ),
        changes=(delete_ids, update_ids),
    ),
    # The version 2 table with its data files rewritten and its position
    # delete files left listed, naming none of them.
    Golden(
        "long_mor_v2_compacted_100k",
        MOR_GROUP,
        "BIGINT",
        FOUR_BUCKETS,
        ROWS_100K,
        LONG_KEY,
        CONTRACT_REPEATS,
        extra=dict(FORMAT_V2, **MERGE_ON_READ),
        changes=(delete_ids, update_ids, rewrite_data),
    ),
    # The version 3 table compacted as an owner's job does it.
    Golden(
        "long_mor_v3_compacted_100k",
        MOR_GROUP,
        "BIGINT",
        FOUR_BUCKETS,
        ROWS_100K,
        LONG_KEY,
        CONTRACT_REPEATS,
        extra=dict(FORMAT_V3, **MERGE_ON_READ),
        changes=(delete_ids, update_ids, rewrite_data, rewrite_deletes),
    ),
    # Ordered tables: ORDERED_ROWS_PER_KEY rows per key, sorted by the key and
    # then the order value. One file per bucket after a sorting compaction.
    Golden(
        "ordered_compacted_100k",
        ORDERED_GROUP,
        "BIGINT",
        FOUR_BUCKETS,
        ROWS_100K,
        None,
        CONTRACT_REPEATS,
        changes=(rewrite_sorted,),
        order=OrderShape(),
    ),
    # Rows appended after the order values already written: a second file per
    # bucket over the first file's key range.
    Golden(
        "ordered_appended_100k",
        ORDERED_GROUP,
        "BIGINT",
        FOUR_BUCKETS,
        ROWS_100K,
        None,
        CONTRACT_REPEATS,
        changes=(append_later,),
        order=OrderShape(),
    ),
    # Keys uncorrelated with the write order, split across sorted tasks.
    Golden(
        "ordered_overlap_100k",
        ORDERED_GROUP,
        "BIGINT",
        TWO_BUCKETS,
        ROWS_100K,
        None,
        CONTRACT_REPEATS,
        extra={"write.spark.advisory-partition-size-bytes": str(SPLIT_PARTITION_BYTES)},
        skew=True,
        order=OrderShape(permuted=True),
    ),
    # A compaction that rolls each bucket into several files by size.
    Golden(
        "ordered_rolled_100k",
        ORDERED_GROUP,
        "BIGINT",
        FOUR_BUCKETS,
        ROWS_100K,
        None,
        CONTRACT_REPEATS,
        changes=(rewrite_rolled,),
        order=OrderShape(),
    ),
    # A MERGE on the key and the order value, recorded as Parquet position
    # deletes.
    Golden(
        "ordered_merge_v2_100k",
        ORDERED_GROUP,
        "BIGINT",
        FOUR_BUCKETS,
        ROWS_100K,
        None,
        CONTRACT_REPEATS,
        extra=dict(FORMAT_V2, **MERGE_ON_READ),
        changes=(merge_rows,),
        order=OrderShape(),
    ),
    # The same MERGE recorded as deletion vectors.
    Golden(
        "ordered_merge_v3_100k",
        ORDERED_GROUP,
        "BIGINT",
        FOUR_BUCKETS,
        ROWS_100K,
        None,
        CONTRACT_REPEATS,
        extra=dict(FORMAT_V3, **MERGE_ON_READ),
        changes=(merge_rows,),
        order=OrderShape(),
    ),
    # The version 3 table compacted as an owner's job does it.
    Golden(
        "ordered_merge_v3_compacted_100k",
        ORDERED_GROUP,
        "BIGINT",
        FOUR_BUCKETS,
        ROWS_100K,
        None,
        CONTRACT_REPEATS,
        extra=dict(FORMAT_V3, **MERGE_ON_READ),
        changes=(merge_rows, rewrite_sorted, rewrite_deletes),
        order=OrderShape(),
    ),
    # A string order column whose values share a long prefix per key, under
    # the writer's default column index truncation, so the column index cuts
    # them.
    Golden(
        "ordered_string_100k",
        ORDERED_GROUP,
        "BIGINT",
        FOUR_BUCKETS,
        ROWS_100K,
        None,
        CONTRACT_REPEATS,
        extra={"parquet.columnindex.truncate.length": str(COLUMN_INDEX_TRUNCATE_DEFAULT)},
        changes=(rewrite_sorted,),
        order=OrderShape(string=True),
    ),
    # The reference tables: `REFERENCE_KEYS` keys in `REFERENCE_BUCKETS`
    # buckets, an initial load and appends of later rows, then a sorting
    # compaction to the recipe's sizes, keeping only the files it left.
    Golden(
        "ref_compacted",
        REFERENCE_GROUP,
        "BIGINT",
        REFERENCE_BUCKETS,
        REFERENCE_ROWS,
        None,
        REFERENCE_REPEATS,
        incompressible=True,
        changes=(append_batches, rewrite_sorted, expire_replaced),
        order=OrderShape(timed=True),
        appends=APPEND_BATCHES,
    ),
    # The same writes left uncompacted: up to one file a bucket per write.
    Golden(
        "ref_appended",
        REFERENCE_GROUP,
        "BIGINT",
        REFERENCE_BUCKETS,
        REFERENCE_ROWS,
        None,
        REFERENCE_REPEATS,
        incompressible=True,
        changes=(append_batches,),
        order=OrderShape(timed=True),
        appends=APPEND_BATCHES,
    ),
    # `ref_compacted` with each instant as ISO-8601 text.
    Golden(
        "ref_string",
        REFERENCE_GROUP,
        "BIGINT",
        REFERENCE_BUCKETS,
        REFERENCE_ROWS,
        None,
        REFERENCE_REPEATS,
        incompressible=True,
        changes=(append_batches, rewrite_sorted, expire_replaced),
        order=OrderShape(timed=True, string=True),
        appends=APPEND_BATCHES,
    ),
    # `ref_compacted` with each bucket's file in row groups of about a
    # quarter of it.
    Golden(
        "ref_large_files",
        REFERENCE_GROUP,
        "BIGINT",
        REFERENCE_BUCKETS,
        REFERENCE_ROWS,
        None,
        REFERENCE_REPEATS,
        extra={
            "write.target-file-size-bytes": str(LARGE_FILE_BYTES),
            "write.parquet.row-group-size-bytes": str(LARGE_ROW_GROUP_BYTES),
        },
        incompressible=True,
        changes=(append_batches, rewrite_sorted, expire_replaced),
        order=OrderShape(timed=True),
        appends=APPEND_BATCHES,
    ),
    # The initial load in `ref_large_files`' sizes, a file a bucket of
    # several row groups, then `LARGE_APPEND_BATCHES` appends left
    # uncompacted, each a file a bucket over the initial file's key range.
    Golden(
        "ref_large_appended",
        REFERENCE_GROUP,
        "BIGINT",
        REFERENCE_BUCKETS,
        REFERENCE_ROWS,
        None,
        REFERENCE_REPEATS,
        extra={
            "write.target-file-size-bytes": str(LARGE_FILE_BYTES),
            "write.parquet.row-group-size-bytes": str(LARGE_ROW_GROUP_BYTES),
        },
        incompressible=True,
        changes=(append_batches,),
        order=OrderShape(timed=True),
        appends=LARGE_APPEND_BATCHES,
    ),
]


def write(spark, table):
    skew_splitting(spark, table.skew)
    started = time.monotonic()
    order_type = None
    if table.order:
        order_type = "STRING" if table.order.string else "BIGINT"
    extra = table.extra
    if table.group == REFERENCE_GROUP:
        extra = dict(extra or {}, **{REFERENCE_KEYS_PROPERTY: str(table.keys)})
    name = create(spark, table.name, table.key_type, table.buckets, extra=extra, order_type=order_type)
    if table.order and table.order.timed:
        fill_reference(spark, name, table)
    elif table.order:
        fill_ordered(spark, name, table)
    else:
        version = MOR_FIRST_VERSION if table.changes else None
        fill(
            spark,
            name,
            table.rows,
            table.key_expr,
            table.repeats,
            incompressible=table.incompressible,
            version=version,
        )
    for change in table.changes:
        change(spark, name, table)
    summarize(spark, name, started)


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--warehouse", required=True, help="s3a://bucket/prefix/warehouse or file:///path")
    parser.add_argument(
        "--only",
        choices=[
            CONTRACT_GROUP,
            OFF_CONTRACT_GROUP,
            MOR_GROUP,
            ORDERED_GROUP,
            REFERENCE_GROUP,
            *(table.name for table in TABLES),
        ],
        help=f"write one group, or one table by name, instead of every table but the {REFERENCE_GROUP} group",
    )
    parser.add_argument(
        "--reference-keys",
        type=int,
        default=REFERENCE_KEYS,
        help=f"the keys of each {REFERENCE_GROUP} table, fewer for a quick run; its rows scale with them",
    )
    parser.add_argument("--packages", default=ICEBERG_PACKAGE, help="Maven coordinates for the Iceberg runtime; empty if it is already on the classpath")
    parser.add_argument("--shuffle-partitions", type=int, default=64)
    args = parser.parse_args()
    if not 0 < args.reference_keys <= REFERENCE_KEYS_MAX:
        parser.error(f"--reference-keys must be from 1 to {REFERENCE_KEYS_MAX}")

    from pyspark.sql import SparkSession

    builder = (
        SparkSession.builder.appName("icetray-golden-tables")
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config(f"spark.sql.catalog.{CATALOG}", "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{CATALOG}.type", "hadoop")
        .config(f"spark.sql.catalog.{CATALOG}.warehouse", args.warehouse)
        .config("spark.sql.shuffle.partitions", str(args.shuffle_partitions))
        .config("spark.sql.session.timeZone", "UTC")
    )
    if args.packages:
        builder = builder.config("spark.jars.packages", args.packages)
    spark = builder.getOrCreate()

    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG}.db")
    print(f"spark {spark.version}, warehouse {args.warehouse}", flush=True)

    for table in TABLES:
        default = args.only is None and table.group != REFERENCE_GROUP
        if default or args.only in (table.group, table.name):
            write(spark, table._replace(keys=args.reference_keys))

    print(f"\nset ICETRAY_GOLDEN_WAREHOUSE={args.warehouse} for the golden tests", flush=True)
    spark.stop()


if __name__ == "__main__":
    main()
