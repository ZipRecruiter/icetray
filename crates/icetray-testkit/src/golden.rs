//! The golden tables `dev/golden-tables.py` writes (RFD 014): where they
//! are, what each holds, and the key and value expressions of the recipe as
//! Rust, so a G-tier test checks an answer without a stored oracle. The
//! tables live outside the tree, named by `ICETRAY_GOLDEN_WAREHOUSE`; a run
//! without it fails on `Precondition::GoldenUnset` rather than passing. A
//! copy of the warehouse elsewhere, whose metadata still names the original
//! locations, is read through the rewrite rules `ICETRAY_GOLDEN_REWRITE`
//! holds.

use std::collections::BTreeSet;
use std::fmt::{self, Write as _};
use std::ops::{Range, RangeInclusive};

use bytes::Bytes;
use iceberg::spec::TableMetadata;

use icetray_core::config::{
    Config, RewriteRule, ServerConfig, SourceConfig, StorageConfig, TableConfig,
};
use icetray_core::key::HEX_DIGITS_PER_BYTE;
use icetray_core::{Generation, Key, KeyType, OrderValue, Residency, Scanned, Storage};
use sha2::digest::OutputSizeUser;
use sha2::digest::typenum::Unsigned as _;
use sha2::{Digest as _, Sha256};

use crate::error::{Precondition, Result, required_var};
use crate::keys::{key_at, key_at_padded};
use crate::layout::{KEY_COLUMN, VALUE_COLUMN};
use crate::ordered::{ORDER_COLUMN, OrderKind, TestScan, middle_half, scan_model, scanned_of};

/// The environment variable naming the warehouse the recipe wrote into: an
/// `s3a://` or `file://` location whose `db/` holds the tables.
pub const WAREHOUSE_VAR: &str = "ICETRAY_GOLDEN_WAREHOUSE";

/// The environment variable holding the rewrite rules of a copied
/// warehouse, `FROM=TO` rules separated by `REWRITE_LIST_SEPARATOR`, each
/// applied as `[[storage.rewrite]]` applies it; unset or empty is none.
/// Spark names a local warehouse's files both as `file:///dir` (a table's
/// location) and as `file:/dir` (its manifest lists and data files), so a
/// copy of one takes a rule for each form.
pub const REWRITE_VAR: &str = "ICETRAY_GOLDEN_REWRITE";

/// What separates two rules in `REWRITE_VAR`: a character no location
/// holds, unlike the `=` inside a rule and the `,` of an S3 key.
pub const REWRITE_LIST_SEPARATOR: char = ';';

/// The namespace the recipe creates every table in (`CATALOG.db`).
const NAMESPACE: &str = "db";

/// The branch every golden table is read on.
const BRANCH: &str = iceberg::spec::MAIN_BRANCH;

/// Rows of `long_2m` and `str_2m` (the recipe's `fill(..., 2_000_000, ...)`).
const ROWS_2M: u64 = 2_000_000;

/// Rows of `default_layout_1m`.
const ROWS_1M: u64 = 1_000_000;

/// Rows of `bin16_500k` and `unpartitioned_500k`.
const ROWS_500K: u64 = 500_000;

/// Rows of `long_wide_250k` and `overlap_250k`.
const ROWS_250K: u64 = 250_000;

/// Rows of the merge-on-read tables before their changes.
const ROWS_100K: u64 = 100_000;

/// Rows of `long_v3_50k`.
const ROWS_50K: u64 = 50_000;

/// Buckets of the contract tables, `long_v3_50k`, the merge-on-read tables,
/// and `default_layout_1m` (`create(..., 4)`).
const FOUR_BUCKETS: u32 = 4;

/// Buckets of the wide and overlap tables (`create(..., 2, extra=WIDE)`).
const TWO_BUCKETS: u32 = 2;

/// `create(..., 0)`: no bucket partition.
const UNPARTITIONED: u32 = 0;

/// The recipe's `repeat('p', 70)` before a string key: the `key_pad` of
/// `keys::key_at_padded`, whose `user:` infix and sixteen zero-padded digits
/// are the rest of the recipe's `STRING_KEY`.
const STRING_PREFIX_LEN: usize = 70;

/// The recipe's `PERMUTED_KEY = pmod(id * 2654435761, 1000000007)`: a
/// bijection for ids below the modulus.
const PERMUTATION_MULTIPLIER: u64 = 2_654_435_761;
const PERMUTATION_MODULUS: u64 = 1_000_000_007;

/// The recipe's `repeat(sha2(...), 3)`: a contract value is the key's hex
/// digest three times.
pub const CONTRACT_REPEATS: usize = 3;

/// The recipe's `sequence(1, 256)`: a wide value concatenates this many
/// distinct digests.
pub const WIDE_REPEATS: usize = 256;

/// The recipe's `MOR_DELETED_IDS`: the ids a merge-on-read table's DELETE
/// removes, half-open.
pub const MOR_DELETED_IDS: Range<u64> = 10_000..20_000;

/// The recipe's `MOR_UPDATED_IDS`: the ids a merge-on-read table's UPDATE
/// rewrites at `MOR_UPDATE_VERSION`, half-open.
pub const MOR_UPDATED_IDS: Range<u64> = 50_000..60_000;

/// The recipe's `MOR_FIRST_VERSION`: the version a merge-on-read table's
/// INSERT writes every id at.
pub const MOR_FIRST_VERSION: u64 = 1;

/// The recipe's `MOR_UPDATE_VERSION`: the version its UPDATE writes.
pub const MOR_UPDATE_VERSION: u64 = 2;

/// The recipe's `ORDERED_KEYS`: the keys of an ordered table.
pub const ORDERED_KEYS: u64 = 1_000;

/// The recipe's `ORDERED_ROWS_PER_KEY`: the rows each key of an ordered
/// table starts with.
pub const ORDERED_ROWS_PER_KEY: u64 = 100;

/// The recipe's `ORDER_START`: the raw order value of a key's first row.
pub const ORDER_START: i64 = 1_000;

/// The recipe's `ORDER_STEP`: the raw distance between a key's consecutive
/// rows, leaving room for the rows a MERGE inserts between two.
pub const ORDER_STEP: i64 = 10;

/// The recipe's `APPEND_ROWS_PER_KEY`: the rows `ordered_appended_100k`
/// adds to every key, after its last.
pub const APPEND_ROWS_PER_KEY: u64 = 10;

/// The recipe's `MERGE_KEY_IDS`: the ids whose keys an ordered table's
/// MERGE changes, half-open.
pub const MERGE_KEY_IDS: Range<u64> = 300..400;

/// The recipe's `MERGE_UPDATE_EVERY`: of each merged key's rows, every
/// row at an index divisible by this is matched and updated.
pub const MERGE_UPDATE_EVERY: u64 = 2;

/// The recipe's `MERGE_INSERT_EVERY`: after each row of a merged key at
/// an index divisible by this, the MERGE inserts a row.
pub const MERGE_INSERT_EVERY: u64 = 10;

/// The recipe's `MERGE_INSERT_OFFSET`: the raw order value an inserted row
/// lies above the row before it.
pub const MERGE_INSERT_OFFSET: i64 = 5;

/// The recipe's `ORDER_PREFIX_LEN`: the bytes of a string order value's
/// prefix, the key's text left-padded with `k`.
const ORDER_PREFIX_LEN: usize = 64;

/// The recipe's `ORDER_STRING_DIGITS`: the digits a string order value's raw
/// part is zero-padded to.
const ORDER_STRING_DIGITS: usize = 16;

/// The padding of a string order value's prefix, as `lpad(.., 64, 'k')`.
const ORDER_PREFIX_PAD: char = 'k';

/// The recipe's `REFERENCE_KEYS`: the keys of a reference table at its full
/// size. `--reference-keys` writes fewer, and the table records the count
/// under `REFERENCE_KEYS_PROPERTY`.
pub const REFERENCE_KEYS: u64 = 200_000;

/// The recipe's `REFERENCE_KEYS_PROPERTY`: the table property holding the
/// key count of a reference table, so a test derives a scaled table's rows.
pub const REFERENCE_KEYS_PROPERTY: &str = "golden.reference-keys";

/// The recipe's `REFERENCE_ROWS`: a reference table's rows at
/// `REFERENCE_KEYS`, every key's `reference_rows_of` summed.
pub const REFERENCE_ROWS: u64 = 19_938_958;

/// The key indexes of a reference table's three longest runs at
/// `REFERENCE_KEYS`, longest first.
pub const REFERENCE_HEAVIEST_IDS: [u64; 3] = [109_154, 74_792, 183_946];

/// The recipe's `REFERENCE_BUCKETS`.
const REFERENCE_BUCKETS: u32 = 64;

/// The recipe's `REFERENCE_REPEATS`: a reference value concatenates this
/// many distinct digests.
const REFERENCE_REPEATS: usize = 2;

/// The bytes of every reference value: `REFERENCE_REPEATS` hex digests.
pub const REFERENCE_VALUE_BYTES: usize = REFERENCE_REPEATS * HEX_DIGEST_BYTES;

/// The keys `reference_sample` takes from each tier of rows per key
/// (`reference_tier_sample`), so the sample spans the skew whatever share
/// of the keys each tier holds.
pub const REFERENCE_TIER_SAMPLES: usize = 5;

/// The recipe's `TIER_SCALE`: the hash of a key's index, modulo this, picks
/// the key's tier, and the hash divided by it the count within the tier.
const TIER_SCALE: u64 = 1_000;

/// The recipe's `HEAVY_TIER` and `MEDIUM_TIER`: a tier below the first is
/// heavy, below the second medium, and any other light.
const HEAVY_TIER: u64 = 6;
const MEDIUM_TIER: u64 = 110;

/// The recipe's `HEAVY_ROWS`: the row counts of a heavy key, the fewest
/// keys.
pub const HEAVY_ROWS: RangeInclusive<u64> = 1_000..=10_000;

/// The recipe's `MEDIUM_ROWS`: the row counts of a medium key.
pub const MEDIUM_ROWS: RangeInclusive<u64> = 100..=999;

/// The recipe's `LIGHT_ROWS`: the row counts of a light key, the most keys.
pub const LIGHT_ROWS: RangeInclusive<u64> = 1..=20;

/// The recipe's `APPEND_TAIL_DIVISOR`: one in this many of a key's rows,
/// rounded down, are its tail, the rows the appends write.
const APPEND_TAIL_DIVISOR: u64 = 5;

/// The recipe's `APPEND_BATCHES`: the appends after a reference table's
/// initial load, which split each key's tail evenly in order, unless the
/// table sets its own count (`OrderedRecipe::appends`).
pub const APPEND_BATCHES: u64 = 4;

/// The recipe's `LARGE_APPEND_BATCHES`: `ref_large_appended`'s appends,
/// so that with its initial load's file each of its buckets holds nine
/// files, and a load's uniqueness merges start more cursors than the reads
/// a load of four worker threads holds in flight.
pub const LARGE_APPEND_BATCHES: u64 = 8;

/// The recipe's `TS_START`: 2026-01-01T00:00:00Z in microseconds, the
/// earliest instant of a reference table, where the benchmark's tables of
/// timestamps start too.
pub const TS_START: i64 = 1_767_225_600_000_000;

/// The recipe's `TS_STEP`: the microseconds per row index, and the bound
/// of a row's jitter, so a key's instants strictly increase.
const TS_STEP: i64 = 1_000_000;

/// The recipe's `ROW_SLOTS`: at least any key's row count, so
/// `id * ROW_SLOTS + j` is unique to the row whose jitter it hashes.
const ROW_SLOTS: u64 = *HEAVY_ROWS.end();

/// The recipe's `ISO_INSTANT` as a strftime pattern: fixed-width ISO-8601
/// UTC text to the microsecond.
const ISO_INSTANT: &str = "%Y-%m-%dT%H:%M:%S%.6fZ";

/// The ASCII bytes of one hex SHA-256 digest: the digest's width in hex
/// digits.
pub const HEX_DIGEST_BYTES: usize =
    <Sha256 as OutputSizeUser>::OutputSize::USIZE * HEX_DIGITS_PER_BYTE;

/// The tables the recipe writes. Display gives the table name it uses.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum GoldenTable {
    /// `long_2m`: the contract layout, long keys.
    Long2m,
    /// `str_2m`: the contract layout, string keys.
    Str2m,
    /// `bin16_500k`: the contract layout, sixteen-byte binary keys.
    Bin16_500k,
    /// `long_wide_250k`: the contract layout with byte-bounded pages and
    /// sixteen-kibibyte values.
    LongWide250k,
    /// `long_v3_50k`: the contract layout, long keys, in table format
    /// version 3 (the recipe's `FORMAT_V3`).
    LongV3_50k,
    /// `default_layout_1m`: the writer's defaults, whose large key pages
    /// the doctor finds cap the level at balanced.
    DefaultLayout1m,
    /// `unpartitioned_500k`: no bucket partition.
    Unpartitioned500k,
    /// `overlap_250k`: permuted keys written with skew splitting on, so a
    /// bucket may hold overlapping sorted files.
    Overlap250k,
    /// `long_mor_v2_100k`: the contract layout in format version 2, then a
    /// merge-on-read DELETE of `MOR_DELETED_IDS` and UPDATE of
    /// `MOR_UPDATED_IDS`, recorded as Parquet position deletes.
    LongMorV2_100k,
    /// `long_mor_v3_100k`: the same in format version 3, recorded as
    /// deletion vectors.
    LongMorV3_100k,
    /// `long_mor_v2_compacted_100k`: `long_mor_v2_100k`'s commands, then
    /// `rewrite_data_files`, which leaves its position deletes listed and
    /// dangling.
    LongMorV2Compacted100k,
    /// `long_mor_v3_compacted_100k`: `long_mor_v3_100k`'s commands, then
    /// `rewrite_data_files` and `rewrite_position_delete_files`.
    LongMorV3Compacted100k,
    /// `ordered_compacted_100k`: an ordered table of long keys and a long
    /// order column, rewritten by a sorting compaction.
    OrderedCompacted100k,
    /// `ordered_rolled_100k`: the same rows, then a sorting compaction with
    /// a target file size far below a bucket's bytes, which rolls each bucket
    /// into several files.
    OrderedRolled100k,
    /// `ordered_appended_100k`: the same rows, then `APPEND_ROWS_PER_KEY`
    /// more for every key at later order values, in a second file per bucket.
    OrderedAppended100k,
    /// `ordered_overlap_100k`: permuted keys written with skew splitting
    /// on, in two buckets.
    OrderedOverlap100k,
    /// `ordered_merge_v2_100k`: a merge-on-read MERGE on the key and the
    /// order value, recorded as Parquet position deletes.
    OrderedMergeV2_100k,
    /// `ordered_merge_v3_100k`: the same MERGE, recorded as deletion vectors.
    OrderedMergeV3_100k,
    /// `ordered_merge_v3_compacted_100k`: the version 3 MERGE, then a sorting
    /// compaction and `rewrite_position_delete_files`.
    OrderedMergeV3Compacted100k,
    /// `ordered_string_100k`: a string order column whose values share a
    /// 64-byte prefix per key, compacted by a sorting compaction.
    OrderedString100k,
    /// `ref_compacted`: a reference table, an initial load and
    /// `APPEND_BATCHES` appends, then a sorting compaction to the recipe's
    /// sizes.
    RefCompacted,
    /// `ref_appended`: the same writes, left uncompacted.
    RefAppended,
    /// `ref_string`: `ref_compacted` with each instant as ISO-8601 UTC text.
    RefString,
    /// `ref_large_files`: `ref_compacted` with each bucket's file in row
    /// groups of about a quarter of it.
    RefLargeFiles,
    /// `ref_large_appended`: the initial load in `ref_large_files`' sizes,
    /// a file of several row groups a bucket, then `LARGE_APPEND_BATCHES`
    /// appends, left uncompacted.
    RefLargeAppended,
}

impl GoldenTable {
    /// Every table the recipe writes.
    pub const ALL: [Self; 25] = [
        Self::Long2m,
        Self::Str2m,
        Self::Bin16_500k,
        Self::LongWide250k,
        Self::LongV3_50k,
        Self::DefaultLayout1m,
        Self::Unpartitioned500k,
        Self::Overlap250k,
        Self::LongMorV2_100k,
        Self::LongMorV3_100k,
        Self::LongMorV2Compacted100k,
        Self::LongMorV3Compacted100k,
        Self::OrderedCompacted100k,
        Self::OrderedRolled100k,
        Self::OrderedAppended100k,
        Self::OrderedOverlap100k,
        Self::OrderedMergeV2_100k,
        Self::OrderedMergeV3_100k,
        Self::OrderedMergeV3Compacted100k,
        Self::OrderedString100k,
        Self::RefCompacted,
        Self::RefAppended,
        Self::RefString,
        Self::RefLargeFiles,
        Self::RefLargeAppended,
    ];

    /// What the recipe writes the table as: every fact a test reads of it,
    /// one arm per table.
    pub const fn spec(self) -> GoldenSpec {
        use GoldenSpec as Spec;
        use KeyType::{Binary, Long};
        let (long, string) = (OrderKind::Long, OrderKind::Str);
        match self {
            Self::Long2m => Spec::kv("long_2m", Long, FOUR_BUCKETS, ROWS_2M),
            Self::Str2m => Spec::kv("str_2m", KeyType::String, FOUR_BUCKETS, ROWS_2M),
            Self::Bin16_500k => Spec::kv("bin16_500k", Binary, FOUR_BUCKETS, ROWS_500K),
            Self::LongWide250k => Spec::kv("long_wide_250k", Long, TWO_BUCKETS, ROWS_250K),
            Self::LongV3_50k => Spec::kv("long_v3_50k", Long, FOUR_BUCKETS, ROWS_50K),
            Self::DefaultLayout1m => Spec::kv("default_layout_1m", Long, FOUR_BUCKETS, ROWS_1M),
            Self::Unpartitioned500k => {
                Spec::kv("unpartitioned_500k", Long, UNPARTITIONED, ROWS_500K)
            }
            Self::Overlap250k => Spec::kv("overlap_250k", Long, TWO_BUCKETS, ROWS_250K),
            Self::LongMorV2_100k => Spec::mor("long_mor_v2_100k"),
            Self::LongMorV3_100k => Spec::mor("long_mor_v3_100k"),
            Self::LongMorV2Compacted100k => Spec::mor("long_mor_v2_compacted_100k"),
            Self::LongMorV3Compacted100k => Spec::mor("long_mor_v3_compacted_100k"),
            Self::OrderedCompacted100k => Spec::ordered("ordered_compacted_100k", long),
            Self::OrderedRolled100k => Spec::ordered("ordered_rolled_100k", long),
            Self::OrderedAppended100k => Spec::ordered("ordered_appended_100k", long).appended(),
            Self::OrderedOverlap100k => Spec::ordered("ordered_overlap_100k", long).permuted(),
            Self::OrderedMergeV2_100k => Spec::ordered("ordered_merge_v2_100k", long).merged(),
            Self::OrderedMergeV3_100k => Spec::ordered("ordered_merge_v3_100k", long).merged(),
            Self::OrderedMergeV3Compacted100k => {
                Spec::ordered("ordered_merge_v3_compacted_100k", long).merged()
            }
            Self::OrderedString100k => Spec::ordered("ordered_string_100k", string),
            Self::RefCompacted => Spec::reference("ref_compacted", long),
            Self::RefAppended => Spec::reference("ref_appended", long),
            Self::RefString => Spec::reference("ref_string", string),
            Self::RefLargeFiles => Spec::reference("ref_large_files", long),
            Self::RefLargeAppended => {
                Spec::reference("ref_large_appended", long).appended_in(LARGE_APPEND_BATCHES)
            }
        }
    }

    /// Every table the recipe's `ordered` group writes: the ordered tables
    /// outside the `reference` group.
    pub fn ordered() -> Vec<Self> {
        Self::ALL
            .into_iter()
            .filter(|table| table.spec().order.is_some_and(|order| !order.reference))
            .collect()
    }

    /// Every table the recipe's `reference` group writes.
    pub fn reference() -> Vec<Self> {
        Self::ALL
            .into_iter()
            .filter(|table| table.is_reference())
            .collect()
    }

    /// Whether the recipe's `reference` group writes the table.
    pub const fn is_reference(self) -> bool {
        matches!(
            self.spec().order,
            Some(OrderedRecipe {
                reference: true,
                ..
            })
        )
    }

    /// The order column's type, or `None` for a key-value table.
    pub fn order_kind(self) -> Option<OrderKind> {
        self.spec().order.map(|order| order.kind)
    }

    /// The recipe of an ordered table.
    ///
    /// Panics on a key-value table.
    fn recipe(self) -> OrderedRecipe {
        self.spec()
            .order
            .unwrap_or_else(|| panic!("{self} is not an ordered table"))
    }
}

/// What the recipe writes one golden table as (`GoldenTable::spec`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct GoldenSpec {
    /// The table name the recipe writes it under, its Display.
    pub name: &'static str,
    /// The Iceberg type of the key column.
    pub key_type: KeyType,
    /// The bucket count the table was created with; zero is unpartitioned.
    pub buckets: u32,
    /// The rows the recipe filled the table with, before any change; for a
    /// reference table, every row its writes hold at the full size,
    /// `REFERENCE_KEYS`. A reference table written with `--reference-keys`
    /// holds `reference_rows` of the count it records under
    /// `REFERENCE_KEYS_PROPERTY`, which a test reads from the table.
    pub rows: u64,
    /// How an ordered table's rows are written; `None` for a key-value
    /// table.
    pub order: Option<OrderedRecipe>,
}

/// How the recipe writes an ordered golden table.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct OrderedRecipe {
    /// The order column's type.
    pub kind: OrderKind,
    /// The key of each key index.
    pub key: KeyExpr,
    /// Whether the recipe ends in a MERGE over `MERGE_KEY_IDS`.
    pub merged: bool,
    /// Whether `APPEND_ROWS_PER_KEY` more rows follow every key's first
    /// fill, at later order values.
    pub appended: bool,
    /// Whether the recipe's `reference` group writes the table: its keys'
    /// rows are `reference_rows_of` rows at `reference_ts`.
    pub reference: bool,
    /// The appends after a reference table's initial load, which split
    /// each key's tail (`reference_writes`); zero outside the `reference`
    /// group.
    pub appends: u64,
}

/// The recipe's key expression of an ordered table.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum KeyExpr {
    /// `LONG_KEY` of the key index (`long_key`).
    Long,
    /// `PERMUTED_KEY` of the key index (`permuted_key`).
    Permuted,
}

impl GoldenSpec {
    /// A key-value table.
    const fn kv(name: &'static str, key_type: KeyType, buckets: u32, rows: u64) -> Self {
        Self {
            name,
            key_type,
            buckets,
            rows,
            order: None,
        }
    }

    /// A merge-on-read table: `ROWS_100K` rows of long keys in four
    /// buckets before its changes.
    const fn mor(name: &'static str) -> Self {
        Self::kv(name, KeyType::Long, FOUR_BUCKETS, ROWS_100K)
    }

    /// An ordered table of the `ordered` group: `ROWS_100K` rows of
    /// `LONG_KEY` keys in four buckets, neither appended nor merged.
    const fn ordered(name: &'static str, kind: OrderKind) -> Self {
        Self {
            order: Some(OrderedRecipe {
                kind,
                key: KeyExpr::Long,
                merged: false,
                appended: false,
                reference: false,
                appends: 0,
            }),
            ..Self::kv(name, KeyType::Long, FOUR_BUCKETS, ROWS_100K)
        }
    }

    /// A table of the `reference` group, of long keys in
    /// `REFERENCE_BUCKETS` buckets, written in `APPEND_BATCHES` appends
    /// after its initial load.
    const fn reference(name: &'static str, kind: OrderKind) -> Self {
        Self {
            order: Some(OrderedRecipe {
                kind,
                key: KeyExpr::Long,
                merged: false,
                appended: false,
                reference: true,
                appends: APPEND_BATCHES,
            }),
            ..Self::kv(name, KeyType::Long, REFERENCE_BUCKETS, REFERENCE_ROWS)
        }
    }

    /// This ordered table of `PERMUTED_KEY` keys in two buckets, written
    /// with skew splitting on.
    const fn permuted(mut self) -> Self {
        self.buckets = TWO_BUCKETS;
        if let Some(order) = &mut self.order {
            order.key = KeyExpr::Permuted;
        }
        self
    }

    /// This ordered table with `APPEND_ROWS_PER_KEY` rows appended per key.
    const fn appended(mut self) -> Self {
        if let Some(order) = &mut self.order {
            order.appended = true;
        }
        self
    }

    /// This reference table written in `batches` appends after its
    /// initial load.
    const fn appended_in(mut self, batches: u64) -> Self {
        if let Some(order) = &mut self.order {
            order.appends = batches;
        }
        self
    }

    /// This ordered table ending in the recipe's MERGE.
    const fn merged(mut self) -> Self {
        if let Some(order) = &mut self.order {
            order.merged = true;
        }
        self
    }
}

impl fmt::Display for GoldenTable {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.spec().name)
    }
}

/// A table by the name the recipe writes it under, its Display: the one
/// parser of a golden table's name, for a program's command line.
impl std::str::FromStr for GoldenTable {
    type Err = UnknownName;

    fn from_str(text: &str) -> std::result::Result<Self, UnknownName> {
        Self::ALL
            .into_iter()
            .find(|table| table.to_string() == text)
            .ok_or_else(|| UnknownName(text.to_string()))
    }
}

/// Text that names no golden table, or no tier of rows per key
/// (`FromStr`).
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
#[error("{0:?} is not one of the names")]
pub struct UnknownName(pub String);

/// Where the golden tables are read from: the warehouse the recipe wrote
/// into or a copy of it, and the rewrite rules that send the locations its
/// metadata names to the copy.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Warehouse {
    /// The location whose `db/` holds the tables.
    pub location: String,
    /// The rules every location is rewritten by, first match first.
    pub rewrite: Vec<RewriteRule>,
}

impl Warehouse {
    /// The warehouse `WAREHOUSE_VAR` names, read through the rules
    /// `REWRITE_VAR` holds; `Precondition::GoldenUnset` when the warehouse
    /// variable is absent or not text, `Precondition::GoldenRewrite` for a
    /// rule that is not `FROM=TO`.
    pub fn from_env() -> Result<Self> {
        let location = required_var(WAREHOUSE_VAR, Precondition::GoldenUnset)?;
        let rewrite = std::env::var(REWRITE_VAR)
            .map_or_else(|_| Ok(Vec::new()), |text| rewrite_rules(&text))?;
        Ok(Self { location, rewrite })
    }
}

/// The rules of `text`, `FROM=TO` rules separated by
/// `REWRITE_LIST_SEPARATOR`, in order; an empty entry, as a trailing
/// separator leaves, is skipped.
fn rewrite_rules(text: &str) -> Result<Vec<RewriteRule>> {
    text.split(REWRITE_LIST_SEPARATOR)
        .filter(|rule| !rule.is_empty())
        .map(|rule| {
            rule.parse().map_err(|fault| {
                Precondition::GoldenRewrite {
                    rule: rule.to_string(),
                    fault,
                }
                .into()
            })
        })
        .collect()
}

/// The root of `table` under `warehouse`: `{warehouse}/db/{name}`, as the
/// Hadoop catalog lays it out; a `Static` source, and so `--location`,
/// follows it through its version hint.
pub fn location(warehouse: &str, table: GoldenTable) -> String {
    format!("{warehouse}/{NAMESPACE}/{table}")
}

/// `table` under `warehouse` as a `Static` source at its `location`.
pub fn table_config(warehouse: &str, table: GoldenTable, strict: bool) -> TableConfig {
    TableConfig {
        name: table.to_string(),
        source: SourceConfig::Static {
            location: location(warehouse, table),
        },
        r#ref: BRANCH.to_string(),
        key_column: KEY_COLUMN.to_string(),
        value_columns: vec![icetray_core::ValueColumnConfig::named(VALUE_COLUMN)],
        order_column: table.order_kind().map(|_| ORDER_COLUMN.to_string()),
        max_scan_rows: None,
        strict,
        lookups: None,
        residency: Residency::Full,
        resident_budget_bytes: None,
        row_cache: None,
    }
}

/// A configuration serving `table` of `warehouse` alone with default server
/// and storage settings and the warehouse's rewrite rules; an `s3a://`
/// warehouse needs the SDK's credential chain, so the G tests open it
/// through `icetray_core::open`.
pub fn config(warehouse: &Warehouse, table: GoldenTable, strict: bool) -> Config {
    Config {
        server: ServerConfig::default(),
        storage: StorageConfig {
            rewrite: warehouse.rewrite.clone(),
            ..StorageConfig::default()
        },
        tables: vec![table_config(&warehouse.location, table, strict)],
    }
}

/// The recipe's `LONG_KEY` for `id`: the fixture derivation's long key,
/// whose `KEY_STEP` the recipe shares.
pub fn long_key(id: u64) -> Key {
    key_at(id, KeyType::Long)
}

/// The recipe's `STRING_KEY` for `id`: the fixture derivation's string key
/// under `STRING_PREFIX_LEN` bytes of padding.
pub fn string_key(id: u64) -> Key {
    key_at_padded(id, KeyType::String, STRING_PREFIX_LEN)
}

/// The recipe's `PERMUTED_KEY` for `id`, in `u64` arithmetic.
pub fn permuted_key(id: u64) -> Key {
    Key::Long(i64::try_from(keyed_hash(id)).expect("a value below the modulus fits i64"))
}

/// The recipe's `keyed_hash` of `value`, `PERMUTED_KEY`'s arithmetic: a
/// bijection below the modulus, exact for the values the recipe hashes,
/// whose product stays below 2^63 as Spark's ANSI arithmetic requires.
fn keyed_hash(value: u64) -> u64 {
    (value * PERMUTATION_MULTIPLIER) % PERMUTATION_MODULUS
}

/// A contract table's value for the key whose Spark string form is
/// `key_text` (a long's decimal digits, a string itself): the lowercase hex
/// SHA-256 of that text, `CONTRACT_REPEATS` times, as ASCII bytes.
pub fn value(key_text: &str) -> Vec<u8> {
    hex_digest(key_text).repeat(CONTRACT_REPEATS).into_bytes()
}

/// A merge-on-read table's value for the key whose Spark string form is
/// `key_text` at `version`: `value` of `v{version}:{key_text}`, the recipe's
/// `value_expr(..., version=version)`.
pub fn versioned_value(key_text: &str, version: u64) -> Vec<u8> {
    value(&format!("v{version}:{key_text}"))
}

/// What a lookup of `long_key(id)` answers in a merge-on-read table once
/// the recipe's commands ran: absent for an id in `MOR_DELETED_IDS`, the
/// `MOR_UPDATE_VERSION` value for one in `MOR_UPDATED_IDS`, and the
/// `MOR_FIRST_VERSION` value for every other id the table was filled with.
pub fn merge_on_read_value(id: u64) -> Option<Vec<u8>> {
    if MOR_DELETED_IDS.contains(&id) {
        return None;
    }
    let version = if MOR_UPDATED_IDS.contains(&id) {
        MOR_UPDATE_VERSION
    } else {
        MOR_FIRST_VERSION
    };
    Some(versioned_value(&long_key_text(id), version))
}

/// The decimal text Spark casts `long_key(id)` to.
fn long_key_text(id: u64) -> String {
    long_key_text_of(&long_key(id))
}

/// One row of a golden ordered table's key, as the recipe's commands leave
/// it: its raw order value and the version that wrote it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct OrderedRow {
    /// The raw order value: the recipe's `ORDER_START + j * ORDER_STEP`, at
    /// the row's index `j`, and `MERGE_INSERT_OFFSET` above that for a row
    /// the MERGE inserted.
    pub order: i64,
    /// The version whose value the row holds: `MOR_FIRST_VERSION`, or
    /// `MOR_UPDATE_VERSION` for a row the MERGE wrote.
    pub version: u64,
}

/// The raw order value of the recipe's row index `index` of a key.
fn order_at(index: u64) -> i64 {
    ORDER_START + i64::try_from(index).expect("row indexes are small") * ORDER_STEP
}

/// The key of key index `id` in an ordered golden table: the recipe's
/// `ordered_exprs` key, `LONG_KEY` of the index or, in the overlap table,
/// `PERMUTED_KEY` of it.
pub fn ordered_key(table: GoldenTable, id: u64) -> Key {
    match table.recipe().key {
        KeyExpr::Long => long_key(id),
        KeyExpr::Permuted => permuted_key(id),
    }
}

/// The live rows of the key at key index `id` in an ordered golden table
/// once the recipe's commands ran, ascending by order value: in a reference
/// table its `reference_rows_of` rows at `reference_ts`; otherwise the
/// `ORDERED_ROWS_PER_KEY` rows of the first fill, `APPEND_ROWS_PER_KEY` more
/// after them in the appended table, and in a merged table, for a key in
/// `MERGE_KEY_IDS`, the matched rows at `MOR_UPDATE_VERSION` and the
/// inserted ones between.
pub fn ordered_rows(table: GoldenTable, id: u64) -> Vec<OrderedRow> {
    let recipe = table.recipe();
    if recipe.reference {
        return (0..reference_rows_of(id))
            .map(|index| OrderedRow {
                order: reference_ts(id, index),
                version: MOR_FIRST_VERSION,
            })
            .collect();
    }
    let appended = if recipe.appended {
        APPEND_ROWS_PER_KEY
    } else {
        0
    };
    let merged = recipe.merged && MERGE_KEY_IDS.contains(&id);
    let mut rows = Vec::new();
    for index in 0..ORDERED_ROWS_PER_KEY + appended {
        let updated = merged && index % MERGE_UPDATE_EVERY == 0;
        rows.push(OrderedRow {
            order: order_at(index),
            version: if updated {
                MOR_UPDATE_VERSION
            } else {
                MOR_FIRST_VERSION
            },
        });
        if merged && index % MERGE_INSERT_EVERY == 0 {
            rows.push(OrderedRow {
                order: order_at(index) + MERGE_INSERT_OFFSET,
                version: MOR_UPDATE_VERSION,
            });
        }
    }
    rows
}

/// The text of the order value with raw value `order` for the key at key
/// index `id`: its decimal digits in a long order column, in `ref_string`
/// the instant's `iso_instant`, and in `ordered_string_100k` the recipe's
/// `ORDER_PREFIX_LEN` bytes of the key's text left-padded with `k`, then
/// the raw value zero-padded to `ORDER_STRING_DIGITS`.
pub fn ordered_order_text(table: GoldenTable, id: u64, order: i64) -> String {
    if table == GoldenTable::RefString {
        return iso_instant(order);
    }
    if table != GoldenTable::OrderedString100k {
        return order.to_string();
    }
    let key = long_key_text_of(&ordered_key(table, id));
    let padding = ORDER_PREFIX_PAD
        .to_string()
        .repeat(ORDER_PREFIX_LEN.saturating_sub(key.len()));
    format!("{padding}{key}{order:0ORDER_STRING_DIGITS$}")
}

/// The order value a Scan states or returns for raw order value `order` of
/// the key at key index `id`: a long, or the string `ordered_order_text`.
pub fn ordered_order_value(table: GoldenTable, id: u64, order: i64) -> OrderValue {
    match table.order_kind() {
        Some(OrderKind::Str) => OrderValue::Str(ordered_order_text(table, id, order)),
        Some(_) => OrderValue::Long(order),
        None => panic!("{table} is not an ordered table"),
    }
}

/// An ordered golden table's value for the row at raw order value `order`
/// of the key at key index `id`, written at `version`, from the text
/// `v{version}:{key}:{order text}`, the recipe's `value_expr(...,
/// order_expr=...)`: `value` of it, or in a reference table
/// `REFERENCE_REPEATS` distinct digests of it.
pub fn ordered_value(table: GoldenTable, id: u64, order: i64, version: u64) -> Vec<u8> {
    let key = long_key_text_of(&ordered_key(table, id));
    let text = format!("{key}:{}", ordered_order_text(table, id, order));
    if table.is_reference() {
        digests(&format!("v{version}:{text}"), REFERENCE_REPEATS)
    } else {
        versioned_value(&text, version)
    }
}

/// The recipe's `reference_rows`: the rows the key at key index `id` holds
/// in a reference table, by the tier its hash picks.
pub fn reference_rows_of(id: u64) -> u64 {
    let hash = keyed_hash(id);
    let (tier, spread) = (hash % TIER_SCALE, hash / TIER_SCALE);
    let rows = RowsTier::of_tier(tier).rows();
    rows.start() + spread % (rows.end() - rows.start() + 1)
}

/// The tiers of rows per key a reference table's keys fall in, by the
/// hash of the key's index (`reference_rows_of`). Display gives its name in
/// lowercase.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum RowsTier {
    /// `HEAVY_ROWS`, the fewest keys.
    Heavy,
    /// `MEDIUM_ROWS`.
    Medium,
    /// `LIGHT_ROWS`, the most keys.
    Light,
}

impl RowsTier {
    /// Every tier, heaviest first.
    pub const ALL: [Self; 3] = [Self::Heavy, Self::Medium, Self::Light];

    /// The tier the recipe's `tier`, a hash modulo `TIER_SCALE`, picks.
    fn of_tier(tier: u64) -> Self {
        if tier < HEAVY_TIER {
            Self::Heavy
        } else if tier < MEDIUM_TIER {
            Self::Medium
        } else {
            Self::Light
        }
    }

    /// The row counts of a key in the tier.
    pub fn rows(self) -> RangeInclusive<u64> {
        match self {
            Self::Heavy => HEAVY_ROWS,
            Self::Medium => MEDIUM_ROWS,
            Self::Light => LIGHT_ROWS,
        }
    }
}

/// A tier by its Display name: the one parser of a tier's name, for a
/// program's command line.
impl std::str::FromStr for RowsTier {
    type Err = UnknownName;

    fn from_str(text: &str) -> std::result::Result<Self, UnknownName> {
        Self::ALL
            .into_iter()
            .find(|tier| tier.to_string() == text)
            .ok_or_else(|| UnknownName(text.to_string()))
    }
}

impl fmt::Display for RowsTier {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Heavy => "heavy",
            Self::Medium => "medium",
            Self::Light => "light",
        })
    }
}

/// The first `count` key indexes below `keys` whose rows fall in `tier`,
/// ascending; fewer when the table holds fewer.
pub fn reference_tier_sample(keys: u64, tier: RowsTier, count: usize) -> Vec<u64> {
    (0..keys)
        .filter(|&id| tier.rows().contains(&reference_rows_of(id)))
        .take(count)
        .collect()
}

/// The key indexes below `keys` whose Scans reference table `table` is
/// checked on: the first `REFERENCE_TIER_SAMPLES` keys of each tier, the
/// heaviest, the last key, the first key with no appended rows, with rows
/// in some of its appends only, and with rows in every append, and
/// `extra`.
pub fn reference_sample(table: GoldenTable, keys: u64, extra: &BTreeSet<u64>) -> BTreeSet<u64> {
    let mut sample: BTreeSet<u64> = RowsTier::ALL
        .into_iter()
        .flat_map(|tier| reference_tier_sample(keys, tier, REFERENCE_TIER_SAMPLES))
        .collect();
    sample.extend(REFERENCE_HEAVIEST_IDS.iter().filter(|&&id| id < keys));
    sample.insert(keys - 1);
    let appended = |id: u64| -> Vec<bool> {
        reference_writes(table, id)[1..]
            .iter()
            .map(|write| !write.is_empty())
            .collect()
    };
    let shapes: [fn(&[bool]) -> bool; 3] = [
        |batches| batches.iter().all(|&held| !held),
        |batches| batches.iter().any(|&held| held) && batches.iter().any(|&held| !held),
        |batches| batches.iter().all(|&held| held),
    ];
    for shape in shapes {
        sample.extend((0..keys).find(|&id| shape(&appended(id))));
    }
    sample.extend(extra.iter().filter(|&&id| id < keys));
    sample
}

/// The bounds of a reference Scan's window over a key holding `rows`,
/// ascending, as raw order values: over the run's `middle_half`, both its
/// edge rows included, `start` and `before`.
pub fn reference_window(rows: &[OrderedRow]) -> (i64, i64) {
    let half = middle_half(rows.len());
    (rows[*half.start()].order, rows[*half.end()].order + 1)
}

/// What a Scan of key index `id` of `table` answers under `scan`, from the
/// recipe's derivation of the key's live `rows` (`ordered_rows`), every
/// value included, and the raw order value of the last row when rows
/// remain past it.
pub fn derived_scan(
    table: GoldenTable,
    id: u64,
    rows: &[OrderedRow],
    scan: &TestScan,
) -> (Scanned, Option<i64>) {
    let live = rows.iter().map(|row| (row.order, row.version));
    let answer = scan_model(live, scan);
    let last = answer.1;
    let scanned = scanned_of(
        answer,
        scan,
        |order| ordered_order_value(table, id, order),
        |order, version| vec![Some(Bytes::from(ordered_value(table, id, order, version)))],
    );
    (scanned, last)
}

/// The table metadata of the snapshot `generation` serves, read through
/// `storage` and parsed as Iceberg's own reader parses it.
pub async fn served_metadata(storage: &Storage, generation: &Generation) -> Result<TableMetadata> {
    Ok(serde_json::from_slice(
        &storage.read_all(&generation.metadata_location).await?,
    )?)
}

/// The keys reference table `table` was written with, from the table
/// property `REFERENCE_KEYS_PROPERTY` in `metadata`: `REFERENCE_KEYS`, or
/// fewer under `--reference-keys`; `Precondition::ReferenceKeysUnrecorded`
/// when the property is absent or not a count.
pub fn reference_keys(metadata: &TableMetadata, table: GoldenTable) -> Result<u64> {
    metadata
        .properties()
        .get(REFERENCE_KEYS_PROPERTY)
        .and_then(|keys| keys.parse().ok())
        .ok_or_else(|| {
            Precondition::ReferenceKeysUnrecorded {
                table: table.to_string(),
            }
            .into()
        })
}

/// The rows of a reference table of `keys` keys, every key's
/// `reference_rows_of` summed: `REFERENCE_ROWS` at `REFERENCE_KEYS`.
pub fn reference_rows(keys: u64) -> u64 {
    (0..keys).map(reference_rows_of).sum()
}

/// The recipe's `reference_insert` ranges: the half-open row indexes of
/// the key at key index `id` of reference table `table` that each write
/// holds, the initial load first and then each of the table's appends
/// (`OrderedRecipe::appends`). An append's range is empty for a key
/// whose tail it holds none of.
pub fn reference_writes(table: GoldenTable, id: u64) -> Vec<Range<u64>> {
    let batches = table.recipe().appends;
    assert!(batches > 0, "{table} is not a reference table");
    let rows = reference_rows_of(id);
    let tail = rows / APPEND_TAIL_DIVISOR;
    let base = rows - tail;
    let cuts: Vec<u64> = std::iter::once(0)
        .chain((0..=batches).map(|batch| base + tail * batch / batches))
        .collect();
    cuts.windows(2).map(|pair| pair[0]..pair[1]).collect()
}

/// The rows each write of reference table `table` of `keys` keys holds,
/// the initial load first, every key's `reference_writes` summed write by
/// write.
pub fn reference_write_rows(table: GoldenTable, keys: u64) -> Vec<u64> {
    let writes_per_key = 1 + usize::try_from(table.recipe().appends).expect("a few appends");
    let mut totals = vec![0; writes_per_key];
    for id in 0..keys {
        for (total, write) in totals.iter_mut().zip(reference_writes(table, id)) {
            *total += write.end - write.start;
        }
    }
    totals
}

/// The recipe's `reference_ts`: the microsecond instant of row `index` of the
/// key at key index `id`, `TS_STEP` per index after `TS_START` plus a
/// jitter below `TS_STEP` hashed from the row.
pub fn reference_ts(id: u64, index: u64) -> i64 {
    let jitter = keyed_hash(id * ROW_SLOTS + index) % TS_STEP.unsigned_abs();
    TS_START
        + i64::try_from(index).expect("row indexes are below ROW_SLOTS") * TS_STEP
        + i64::try_from(jitter).expect("the jitter is below TS_STEP")
}

/// The instant `micros` microseconds after the epoch as the recipe's
/// `ISO_INSTANT`: fixed-width ISO-8601 UTC text, which sorts as the
/// instants do.
fn iso_instant(micros: i64) -> String {
    jiff::Timestamp::from_microsecond(micros)
        .expect("a reference instant is within jiff's range")
        .strftime(ISO_INSTANT)
        .to_string()
}

/// The decimal text Spark casts a long `key` to.
fn long_key_text_of(key: &Key) -> String {
    let Key::Long(value) = key else {
        panic!("an ordered golden table has long keys, got {key}");
    };
    value.to_string()
}

/// A wide table's value for `key_text`: `WIDE_REPEATS` digests of it.
pub fn wide_value(key_text: &str) -> Vec<u8> {
    digests(key_text, WIDE_REPEATS)
}

/// The recipe's incompressible `value_expr` of `text`: the hex SHA-256 of
/// `{text}:{i}` for `i` in `1..=repeats`, concatenated, as ASCII bytes.
fn digests(text: &str, repeats: usize) -> Vec<u8> {
    (1..=repeats)
        .map(|i| hex_digest(&format!("{text}:{i}")))
        .collect::<String>()
        .into_bytes()
}

/// The lowercase hex SHA-256 of `text`, as Spark's `sha2(_, 256)` renders it.
fn hex_digest(text: &str) -> String {
    Sha256::digest(text.as_bytes()).iter().fold(
        String::with_capacity(HEX_DIGEST_BYTES),
        |mut hex, byte| {
            write!(hex, "{byte:0HEX_DIGITS_PER_BYTE$x}")
                .expect("formatting into a String cannot fail");
            hex
        },
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::TestkitError;
    use crate::fixture::{
        FixtureSpec, NAMESPACE as FIXTURE_NAMESPACE, memory_catalog_local, write_fixture,
    };
    use crate::keys::{STRING_KEY_FIXED_BYTES, STRING_PAD};
    use icetray_core::approx::count;
    use icetray_core::config::RewriteTextFault;
    use icetray_core::{ColumnRead, FILE_PREFIX, FindingLog, Row, ScanRow, next_bound};

    /// Ids the permutation is checked over: small next to the modulus, so
    /// the recipe's bijection claim holds.
    const PERMUTATION_SAMPLE: u64 = 10_000;

    /// A sample id and its recipe key texts.
    const SAMPLE_ID: u64 = 12_345;

    /// The SHA-256 of `"0"`, as `sha2('0', 256)` gives it.
    const SHA256_OF_ZERO: &str = "5feceb66ffc86f38d952786c6d696c79c2dbc239dd4e91b46729d73a27fb57e9";

    #[test]
    fn test_ordered_and_reference_are_the_recipe_s_groups() {
        let ordered = GoldenTable::ordered();
        let reference = GoldenTable::reference();
        assert_eq!(ordered.len(), 8);
        assert_eq!(reference.len(), 5);
        assert!(
            ordered
                .iter()
                .all(|table| table.to_string().starts_with("ordered_"))
        );
        assert!(
            reference
                .iter()
                .all(|table| table.to_string().starts_with("ref_"))
        );
        assert_eq!(
            reference
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>(),
            [
                "ref_compacted",
                "ref_appended",
                "ref_string",
                "ref_large_files",
                "ref_large_appended"
            ],
            "the names `dev/golden-tables.py --only reference` writes"
        );
    }

    #[test]
    fn test_from_str_golden_table_and_tier_read_display_and_refuse_other_text() {
        for table in GoldenTable::ALL {
            assert_eq!(table.to_string().parse(), Ok(table));
        }
        for tier in RowsTier::ALL {
            assert_eq!(tier.to_string().parse(), Ok(tier));
        }
        let unknown = "ref_unknown";
        assert_eq!(
            unknown.parse::<GoldenTable>(),
            Err(UnknownName(unknown.to_string()))
        );
        assert_eq!(
            "Heavy".parse::<RowsTier>(),
            Err(UnknownName("Heavy".to_string()))
        );
    }

    #[test]
    fn test_table_config_names_the_hadoop_root() {
        let config = table_config("file:///w", GoldenTable::Str2m, false);
        assert_eq!(
            config.source,
            SourceConfig::Static {
                location: "file:///w/db/str_2m".to_string()
            }
        );
        assert_eq!(config.name, "str_2m");
        assert!(!config.strict);
        assert_eq!(config.residency, Residency::Full);
    }

    /// A rule of the form a local warehouse needs, and its manifests'.
    const LOCAL_RULE: &str = "file:///w/=s3a://bucket/copy/";
    const MANIFEST_RULE: &str = "file:/w/=s3a://bucket/copy/";

    #[test]
    fn test_rewrite_rules_reads_rules_in_order_skipping_empty_entries() {
        let text = format!("{LOCAL_RULE};{MANIFEST_RULE};");
        let rules = rewrite_rules(&text).unwrap();
        assert_eq!(
            rules,
            [LOCAL_RULE, MANIFEST_RULE].map(|rule| rule.parse::<RewriteRule>().unwrap())
        );
        assert_eq!(rewrite_rules("").unwrap(), []);
    }

    #[test]
    fn test_rewrite_rules_refuses_a_rule_that_is_not_from_to() {
        let text = format!("{LOCAL_RULE};file:///w/");
        assert!(matches!(
            rewrite_rules(&text),
            Err(TestkitError::Precondition(Precondition::GoldenRewrite {
                ref rule,
                fault: RewriteTextFault::NoSeparator,
            })) if rule == "file:///w/"
        ));
    }

    #[test]
    fn test_config_carries_the_warehouse_rewrites() {
        let warehouse = Warehouse {
            location: "s3a://bucket/copy".to_string(),
            rewrite: rewrite_rules(LOCAL_RULE).unwrap(),
        };
        let config = config(&warehouse, GoldenTable::Long2m, false);
        assert_eq!(config.storage.rewrite, warehouse.rewrite);
        assert_eq!(
            config.tables[0].source,
            SourceConfig::Static {
                location: "s3a://bucket/copy/db/long_2m".to_string()
            }
        );
    }

    /// The table a test writes under one directory and moves to a
    /// warehouse in another: a key-value table of long keys under a golden
    /// name, so `config` serves it.
    const MOVED_TABLE: GoldenTable = GoldenTable::Long2m;

    /// A table moved out of the directory its metadata names is read from
    /// its new place through the warehouse's rewrite, and not without it.
    #[tokio::test]
    async fn test_config_reads_a_moved_warehouse_through_its_rewrites() {
        let written = tempfile::tempdir().unwrap();
        let catalog = memory_catalog_local(written.path()).await.unwrap();
        let spec = FixtureSpec::small(&MOVED_TABLE.to_string(), KeyType::Long);
        let fixture = write_fixture(catalog.as_ref(), FIXTURE_NAMESPACE, spec)
            .await
            .unwrap();
        fixture.write_hint().await.unwrap();
        let moved = tempfile::tempdir().unwrap();
        let moved_root = moved.path().join(NAMESPACE).join(MOVED_TABLE.to_string());
        copy_tree(
            &written
                .path()
                .join(FIXTURE_NAMESPACE)
                .join(MOVED_TABLE.to_string()),
            &moved_root,
        );
        drop(written);
        let location = format!("{FILE_PREFIX}{}", moved.path().display());
        let rewrite = RewriteRule {
            from: fixture.root().to_string(),
            to: format!("{FILE_PREFIX}{}", moved_root.display()),
        };
        let unrewritten = Warehouse {
            location: location.clone(),
            rewrite: Vec::new(),
        };
        let opened = icetray_core::open(
            &config(&unrewritten, MOVED_TABLE, false),
            None,
            FindingLog::Events,
        )
        .await
        .unwrap();
        let unread = opened.tables()[0].load().await;
        assert!(
            matches!(
                unread,
                Err(icetray_core::Error::ObjectStore(
                    object_store::Error::NotFound { .. }
                ))
            ),
            "the metadata names the directory the table left: {unread:?}"
        );
        let warehouse = Warehouse {
            location,
            rewrite: vec![rewrite],
        };
        let opened = icetray_core::open(
            &config(&warehouse, MOVED_TABLE, false),
            None,
            FindingLog::Events,
        )
        .await
        .unwrap();
        let table = &opened.tables()[0];
        table.load().await.unwrap();
        let key = fixture.key(0);
        let lookup = table
            .get(&key, &[ColumnRead::whole(VALUE_COLUMN)])
            .await
            .unwrap();
        assert_eq!(lookup.row, fixture.expected(&key));
        assert!(matches!(lookup.row, Row::Present(_)));
    }

    /// Copies the directory tree at `from` to `to`, creating `to`.
    fn copy_tree(from: &std::path::Path, to: &std::path::Path) {
        std::fs::create_dir_all(to).unwrap();
        for entry in std::fs::read_dir(from).unwrap() {
            let path = entry.unwrap().path();
            let target = to.join(path.file_name().unwrap());
            if path.is_dir() {
                copy_tree(&path, &target);
            } else {
                std::fs::copy(&path, &target).unwrap();
            }
        }
    }

    #[test]
    fn test_permuted_key_is_a_bijection_over_a_small_range() {
        let mut seen = std::collections::HashSet::new();
        for id in 0..PERMUTATION_SAMPLE {
            let Key::Long(value) = permuted_key(id) else {
                panic!("permuted keys are long");
            };
            assert!(value >= 0);
            assert!(seen.insert(value), "id {id} repeats {value}");
        }
    }

    #[test]
    fn test_string_key_has_the_recipe_shape() {
        let key = string_key(SAMPLE_ID);
        assert_eq!(
            key,
            key_at_padded(SAMPLE_ID, KeyType::String, STRING_PREFIX_LEN)
        );
        let Key::Str(text) = key else {
            panic!("string keys are strings");
        };
        assert_eq!(text.len(), STRING_PREFIX_LEN + STRING_KEY_FIXED_BYTES);
        assert!(text.starts_with(&STRING_PAD.to_string().repeat(STRING_PREFIX_LEN)));
        assert_eq!(long_key(SAMPLE_ID), key_at(SAMPLE_ID, KeyType::Long));
    }

    #[test]
    fn test_value_has_contract_repeats_of_the_hex_digest() {
        let value = value("0");
        assert_eq!(value.len(), CONTRACT_REPEATS * HEX_DIGEST_BYTES);
        assert_eq!(value, SHA256_OF_ZERO.repeat(CONTRACT_REPEATS).into_bytes());
    }

    #[test]
    fn test_merge_on_read_value_follows_the_recipe_commands() {
        assert_eq!(merge_on_read_value(MOR_DELETED_IDS.start), None);
        assert_eq!(merge_on_read_value(MOR_DELETED_IDS.end - 1), None);
        let updated = MOR_UPDATED_IDS.start;
        let updated_text = long_key_text(updated);
        assert_eq!(
            merge_on_read_value(updated),
            Some(value(&format!("v{MOR_UPDATE_VERSION}:{updated_text}")))
        );
        let untouched = MOR_DELETED_IDS.end;
        assert_eq!(
            merge_on_read_value(untouched),
            Some(value(&format!(
                "v{MOR_FIRST_VERSION}:{}",
                long_key_text(untouched)
            )))
        );
        assert_ne!(
            versioned_value(&updated_text, MOR_FIRST_VERSION),
            versioned_value(&updated_text, MOR_UPDATE_VERSION),
        );
    }

    #[test]
    fn test_wide_value_concatenates_distinct_digests() {
        let value = wide_value("0");
        assert_eq!(value.len(), WIDE_REPEATS * HEX_DIGEST_BYTES);
        let first = &value[..HEX_DIGEST_BYTES];
        let second = &value[HEX_DIGEST_BYTES..2 * HEX_DIGEST_BYTES];
        assert_ne!(first, second);
        assert_eq!(first, hex_digest("0:1").as_bytes());
    }

    /// A merged key of `MERGE_KEY_IDS`, and an id outside them.
    const MERGED_ID: u64 = 350;
    const UNMERGED_ID: u64 = 5;

    #[test]
    fn test_ordered_rows_are_ascending_and_follow_the_recipe_commands() {
        let rows_per_key = usize::try_from(ORDERED_ROWS_PER_KEY).unwrap();
        for table in GoldenTable::ordered() {
            let recipe = table.recipe();
            for id in [UNMERGED_ID, MERGED_ID] {
                let rows = ordered_rows(table, id);
                assert!(
                    rows.windows(2).all(|pair| pair[0].order < pair[1].order),
                    "{table} {id}"
                );
                let first = rows.iter().filter(|row| row.version == MOR_FIRST_VERSION);
                let updated = rows.iter().filter(|row| row.version == MOR_UPDATE_VERSION);
                let merged = recipe.merged && id == MERGED_ID;
                let appended = usize::try_from(APPEND_ROWS_PER_KEY).unwrap();
                if merged {
                    let matched = rows_per_key / usize::try_from(MERGE_UPDATE_EVERY).unwrap();
                    let inserted = rows_per_key / usize::try_from(MERGE_INSERT_EVERY).unwrap();
                    assert_eq!(updated.count(), matched + inserted, "{table}");
                    assert_eq!(first.count(), rows_per_key - matched, "{table}");
                    assert_eq!(rows.len(), rows_per_key + inserted, "{table}");
                } else {
                    assert_eq!(updated.count(), 0, "{table} {id}");
                    let expected = if recipe.appended {
                        rows_per_key + appended
                    } else {
                        rows_per_key
                    };
                    assert_eq!(rows.len(), expected, "{table} {id}");
                }
            }
        }
    }

    #[test]
    fn test_ordered_rows_insert_between_two_rows_of_the_first_fill() {
        let rows = ordered_rows(GoldenTable::OrderedMergeV2_100k, MERGED_ID);
        let inserted = order_at(0) + MERGE_INSERT_OFFSET;
        let at = rows
            .iter()
            .position(|row| row.order == inserted)
            .expect("the MERGE inserts a row after the first");
        assert_eq!(rows[at - 1].order, order_at(0));
        assert_eq!(rows[at + 1].order, order_at(1));
        assert_eq!(rows[at].version, MOR_UPDATE_VERSION);
    }

    #[test]
    fn test_ordered_order_text_of_the_string_table_shares_a_long_prefix_per_key() {
        let table = GoldenTable::OrderedString100k;
        let first = ordered_order_text(table, SAMPLE_ID, order_at(0));
        let last = ordered_order_text(table, SAMPLE_ID, order_at(ORDERED_ROWS_PER_KEY - 1));
        assert_eq!(first.len(), ORDER_PREFIX_LEN + ORDER_STRING_DIGITS);
        assert_eq!(first[..ORDER_PREFIX_LEN], last[..ORDER_PREFIX_LEN]);
        assert!(first < last);
        let other = ordered_order_text(table, SAMPLE_ID + 1, order_at(0));
        assert_ne!(first[..ORDER_PREFIX_LEN], other[..ORDER_PREFIX_LEN]);
        assert_eq!(
            ordered_order_text(GoldenTable::OrderedCompacted100k, SAMPLE_ID, order_at(0)),
            order_at(0).to_string()
        );
    }

    #[test]
    fn test_ordered_value_is_a_function_of_key_order_and_version() {
        let table = GoldenTable::OrderedCompacted100k;
        let order = order_at(3);
        let value = ordered_value(table, SAMPLE_ID, order, MOR_FIRST_VERSION);
        let text = format!("v{MOR_FIRST_VERSION}:{}:{order}", long_key_text(SAMPLE_ID));
        assert_eq!(value, super::value(&text));
        assert_ne!(
            value,
            ordered_value(table, SAMPLE_ID, order, MOR_UPDATE_VERSION)
        );
        assert_ne!(
            value,
            ordered_value(table, SAMPLE_ID, order_at(4), MOR_FIRST_VERSION)
        );
    }

    #[test]
    fn test_table_config_names_the_order_column_of_an_ordered_table_only() {
        for table in GoldenTable::ordered() {
            let config = table_config("file:///w", table, false);
            assert_eq!(config.order_column.as_deref(), Some(ORDER_COLUMN));
        }
        let config = table_config("file:///w", GoldenTable::Long2m, false);
        assert_eq!(config.order_column, None);
    }

    /// A light key, and a row of it, whose derivation is pinned by hand.
    const PINNED_ID: u64 = 1;
    const PINNED_INDEX: u64 = 3;

    /// The keys of each tier at `REFERENCE_KEYS`, heavy, medium, and light.
    const TIER_KEYS: [u64; 3] = [1_197, 20_805, 177_998];

    /// The rows each write of a table of `APPEND_BATCHES` appends holds at
    /// `REFERENCE_KEYS`: the initial load, then each append.
    const WRITE_ROWS: [u64; 5] = [16_031_167, 901_945, 1_001_949, 951_947, 1_051_950];

    fn reference_ids() -> Range<u64> {
        0..REFERENCE_KEYS
    }

    #[test]
    fn test_reference_rows_of_sums_to_the_recipe_rows_over_heavy_tailed_tiers() {
        assert_eq!(
            [0, 1, 2].map(reference_rows_of),
            [1_000, 16, 12],
            "the hash of 0 is 0, a heavy tier"
        );
        let counts: Vec<u64> = reference_ids().map(reference_rows_of).collect();
        assert_eq!(reference_rows(REFERENCE_KEYS), REFERENCE_ROWS);
        let tiers = [&HEAVY_ROWS, &MEDIUM_ROWS, &LIGHT_ROWS]
            .map(|rows| count(counts.iter().filter(|n| rows.contains(n)).count()));
        assert_eq!(tiers, TIER_KEYS);
    }

    #[test]
    fn test_reference_heaviest_ids_hold_the_longest_runs() {
        let mut ids: Vec<u64> = reference_ids().collect();
        ids.sort_by_key(|&id| std::cmp::Reverse(reference_rows_of(id)));
        assert_eq!(ids[..REFERENCE_HEAVIEST_IDS.len()], REFERENCE_HEAVIEST_IDS);
        assert_eq!(reference_rows_of(REFERENCE_HEAVIEST_IDS[0]), 9_998);
    }

    /// The rows each write of reference table `table` holds at
    /// `REFERENCE_KEYS`, after checking that its writes tile each key's
    /// rows in order, the initial load the rows below the key's tail.
    fn reference_write_totals(table: GoldenTable) -> Vec<u64> {
        let writes_per_key = 1 + usize::try_from(table.recipe().appends).unwrap();
        for id in reference_ids() {
            let writes = reference_writes(table, id);
            assert_eq!(writes.len(), writes_per_key, "{table}");
            assert_eq!(writes[0].start, 0, "{table} {id}");
            assert!(
                writes.windows(2).all(|pair| pair[0].end == pair[1].start),
                "{table} {id} {writes:?}"
            );
            let rows = reference_rows_of(id);
            assert_eq!(writes[writes.len() - 1].end, rows, "{table} {id}");
            assert_eq!(
                writes[0].end,
                rows - rows / APPEND_TAIL_DIVISOR,
                "{table} {id}"
            );
        }
        reference_write_rows(table, REFERENCE_KEYS)
    }

    #[test]
    fn test_reference_writes_tile_each_key_s_rows_in_order() {
        assert_eq!(reference_write_totals(GoldenTable::RefAppended), WRITE_ROWS);
    }

    /// `ref_large_appended` splits the same tails among more appends: its
    /// initial load and its appends together hold what the other tables'
    /// do.
    #[test]
    fn test_reference_writes_of_more_appends_split_the_same_tails() {
        let table = GoldenTable::RefLargeAppended;
        let totals = reference_write_totals(table);
        assert_eq!(
            count(totals.len()),
            1 + LARGE_APPEND_BATCHES,
            "the initial load and each append"
        );
        assert_eq!(totals[0], WRITE_ROWS[0]);
        assert_eq!(
            totals[1..].iter().sum::<u64>(),
            WRITE_ROWS[1..].iter().sum::<u64>()
        );
        assert!(totals[1..].iter().all(|&rows| rows > 0), "{totals:?}");
    }

    #[test]
    fn test_reference_ts_strictly_increases_within_two_steps() {
        assert_eq!(reference_ts(0, 0), TS_START, "the hash of 0 is 0");
        assert_eq!(reference_ts(PINNED_ID, PINNED_INDEX), 1_767_225_603_731_419);
        let id = REFERENCE_HEAVIEST_IDS[0];
        let instants: Vec<i64> = (0..reference_rows_of(id))
            .map(|index| reference_ts(id, index))
            .collect();
        assert!(
            instants
                .windows(2)
                .all(|pair| pair[0] < pair[1] && pair[1] - pair[0] < 2 * TS_STEP)
        );
    }

    #[test]
    fn test_iso_instant_is_fixed_width_utc_text_that_sorts_as_the_instants() {
        assert_eq!(iso_instant(TS_START), "2026-01-01T00:00:00.000000Z");
        let new_year: jiff::Timestamp = "2026-01-01T00:00:00Z".parse().unwrap();
        assert_eq!(new_year.as_microsecond(), TS_START);
        let pinned = reference_ts(PINNED_ID, PINNED_INDEX);
        assert_eq!(iso_instant(pinned), "2026-01-01T00:00:03.731419Z");
        let id = REFERENCE_HEAVIEST_IDS[0];
        let texts: Vec<String> = (0..reference_rows_of(id))
            .map(|index| iso_instant(reference_ts(id, index)))
            .collect();
        assert!(texts.windows(2).all(|pair| pair[0] < pair[1]));
        assert!(texts.iter().all(|text| text.len() == texts[0].len()));
    }

    #[test]
    fn test_reference_value_is_two_digests_of_key_and_order_text() {
        let order = reference_ts(PINNED_ID, PINNED_INDEX);
        let long = ordered_value(
            GoldenTable::RefCompacted,
            PINNED_ID,
            order,
            MOR_FIRST_VERSION,
        );
        assert_eq!(long.len(), REFERENCE_REPEATS * HEX_DIGEST_BYTES);
        assert_eq!(
            &long[..HEX_DIGEST_BYTES],
            hex_digest("v1:3:1767225603731419:1").as_bytes()
        );
        assert!(long[HEX_DIGEST_BYTES..].starts_with(b"5a18283958290e70"));
        let text = ordered_value(GoldenTable::RefString, PINNED_ID, order, MOR_FIRST_VERSION);
        assert!(text.starts_with(b"c5d2dcfe069b40f9"));
        assert_eq!(
            ordered_order_value(GoldenTable::RefString, PINNED_ID, order),
            OrderValue::Str("2026-01-01T00:00:03.731419Z".to_string())
        );
        assert_eq!(
            ordered_order_value(GoldenTable::RefLargeFiles, PINNED_ID, order),
            OrderValue::Long(order)
        );
    }

    #[test]
    fn test_reference_tier_sample_takes_the_first_keys_of_the_tier() {
        for tier in RowsTier::ALL {
            let sample = reference_tier_sample(REFERENCE_KEYS, tier, REFERENCE_TIER_SAMPLES);
            assert_eq!(sample.len(), REFERENCE_TIER_SAMPLES, "{tier}");
            assert!(sample.windows(2).all(|pair| pair[0] < pair[1]), "{tier}");
            assert!(
                sample
                    .iter()
                    .all(|&id| tier.rows().contains(&reference_rows_of(id))),
                "{tier}"
            );
            let first = sample[0];
            assert!(
                (0..first).all(|id| !tier.rows().contains(&reference_rows_of(id))),
                "{tier}: no earlier key is in the tier"
            );
        }
        let one_key = 1;
        assert_eq!(
            reference_tier_sample(one_key, RowsTier::Heavy, REFERENCE_TIER_SAMPLES),
            [0],
            "the hash of 0 is 0, a heavy tier, and the table holds no other key"
        );
    }

    #[test]
    fn test_reference_sample_holds_each_tier_s_sample_and_the_heaviest() {
        let table = GoldenTable::RefAppended;
        let sample = reference_sample(table, REFERENCE_KEYS, &BTreeSet::new());
        for tier in RowsTier::ALL {
            let tier_sample = reference_tier_sample(REFERENCE_KEYS, tier, REFERENCE_TIER_SAMPLES);
            assert!(tier_sample.iter().all(|id| sample.contains(id)), "{tier}");
        }
        assert!(REFERENCE_HEAVIEST_IDS.iter().all(|id| sample.contains(id)));
        assert!(sample.contains(&(REFERENCE_KEYS - 1)));
        let heavy = sample
            .iter()
            .filter(|&&id| HEAVY_ROWS.contains(&reference_rows_of(id)))
            .count();
        let tier_keys = RowsTier::ALL.len() * REFERENCE_TIER_SAMPLES;
        let last_key = 1;
        assert_eq!(
            sample.len(),
            tier_keys + REFERENCE_HEAVIEST_IDS.len() + last_key,
            "each append shape's first key, 4, 1, and 0, is a tier key"
        );
        assert_eq!(
            heavy,
            REFERENCE_TIER_SAMPLES + REFERENCE_HEAVIEST_IDS.len(),
            "the heaviest keys are heavy and the last key is not"
        );
        let edge = BTreeSet::from([REFERENCE_KEYS / 2]);
        let with_edge = reference_sample(table, REFERENCE_KEYS, &edge);
        assert_eq!(with_edge, &sample | &edge, "the extra keys join the sample");
    }

    #[test]
    fn test_reference_window_spans_the_middle_half_of_a_run() {
        let id = REFERENCE_HEAVIEST_IDS[0];
        let rows = ordered_rows(GoldenTable::RefCompacted, id);
        let (start, before) = reference_window(&rows);
        let inside = rows
            .iter()
            .filter(|row| (start..before).contains(&row.order))
            .count();
        assert_eq!(
            (rows.len(), inside),
            (9_998, 5_000),
            "rows 2,499 to 7,498 of the heaviest key"
        );
        let light = ordered_rows(GoldenTable::RefCompacted, PINNED_ID);
        let (start, before) = reference_window(&light[..1]);
        assert_eq!((start, before), (light[0].order, light[0].order + 1));
    }

    #[test]
    fn test_derived_scan_returns_the_derived_rows_and_the_next_page_s_bound() {
        let table = GoldenTable::RefString;
        let rows = ordered_rows(table, PINNED_ID);
        let whole = TestScan {
            limit: u32::try_from(rows.len()).unwrap(),
            ..TestScan::default()
        };
        let (scanned, last) = derived_scan(table, PINNED_ID, &rows, &whole);
        assert_eq!(
            (scanned.rows.len(), scanned.next, last),
            (rows.len(), None, None)
        );
        let newest = &rows[rows.len() - 1];
        assert_eq!(
            scanned.rows[0],
            ScanRow {
                order: ordered_order_value(table, PINNED_ID, newest.order),
                values: vec![Some(Bytes::from(ordered_value(
                    table,
                    PINNED_ID,
                    newest.order,
                    MOR_FIRST_VERSION
                )))],
            },
            "newest first by default"
        );
        let one = TestScan {
            limit: 1,
            ..TestScan::default()
        };
        let (scanned, last) = derived_scan(table, PINNED_ID, &rows, &one);
        assert_eq!(last, Some(newest.order));
        assert_eq!(
            scanned.next,
            next_bound(
                &ordered_order_value(table, PINNED_ID, newest.order),
                one.direction
            )
        );
        assert!(scanned.rows.iter().all(|row| {
            row.values[0]
                .as_ref()
                .is_some_and(|value| value.len() == REFERENCE_VALUE_BYTES)
        }));
    }

    #[test]
    fn test_ordered_rows_of_a_reference_table_are_its_instants() {
        for table in GoldenTable::reference() {
            let rows = ordered_rows(table, PINNED_ID);
            assert_eq!(count(rows.len()), reference_rows_of(PINNED_ID), "{table}");
            assert_eq!(
                rows[usize::try_from(PINNED_INDEX).unwrap()].order,
                reference_ts(PINNED_ID, PINNED_INDEX)
            );
            assert!(rows.iter().all(|row| row.version == MOR_FIRST_VERSION));
            assert_eq!(ordered_key(table, PINNED_ID), long_key(PINNED_ID));
            assert!(table.is_reference() && !GoldenTable::ordered().contains(&table));
        }
    }
}
