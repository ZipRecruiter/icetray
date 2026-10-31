//! Fixture tables written with the Iceberg library's own writer (RFD 014):
//! a `bucket(N, key)`-partitioned, key-sorted table in the serving layout of
//! RFD 002, or in any layout a test names through `Layout`. Values are a
//! function of the key and of the version that wrote it, so any answer is
//! checkable without a stored oracle, and a live row is told apart from a
//! deleted copy of its key.
//! An ordered fixture (RFD 025) writes many rows per key, sorted by key and
//! then order value, and tracks each row by its id and order value.
//! What the library's writer cannot produce (a delete file, a dropped data
//! file, a null key, a key column whose Parquet type contradicts the schema)
//! is committed as a raw file or as a snapshot written by hand.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::ops::Range;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use arrow::array::{
    ArrayRef, BinaryArray, FixedSizeBinaryArray, Int32Array, Int64Array, RecordBatch, StringArray,
    new_null_array,
};
use arrow::buffer::Buffer;
use arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
use bytes::Bytes;
use iceberg::io::{FileIO, LocalFsStorageFactory, StorageFactory};
use iceberg::memory::{MEMORY_CATALOG_WAREHOUSE, MemoryCatalog, MemoryCatalogBuilder};
use iceberg::spec::{
    DataContentType, DataFile, DataFileBuilder, DataFileFormat, Datum, FormatVersion, Literal,
    MAIN_BRANCH, ManifestFile, ManifestListWriter, ManifestWriterBuilder, NestedField,
    NestedFieldRef, NullOrder, Operation, PartitionKey, PartitionSpec, PrimitiveType, Schema,
    Snapshot, SnapshotReference, SnapshotRetention, SortDirection, SortField, SortOrder, Struct,
    Summary, TableMetadata, Transform, Type, UnboundPartitionSpec,
};
use iceberg::table::Table;
use iceberg::transaction::{ApplyTransactionAction as _, Transaction};
use iceberg::transform::create_transform_function;
use iceberg::writer::base_writer::data_file_writer::DataFileWriterBuilder;
use iceberg::writer::file_writer::ParquetWriterBuilder;
use iceberg::writer::file_writer::location_generator::{
    DefaultFileNameGenerator, DefaultLocationGenerator,
};
use iceberg::writer::file_writer::rolling_writer::RollingFileWriterBuilder;
use iceberg::writer::partitioning::PartitioningWriter as _;
use iceberg::writer::partitioning::clustered_writer::ClusteredWriter;
use iceberg::{
    Catalog, CatalogBuilder as _, ErrorKind, NamespaceIdent, Runtime, TableCreation, TableIdent,
};
use iceberg_storage_opendal::OpenDalStorageFactory;
use icetray_core::approx::count;
use icetray_core::catalog::{TableSource, hint_location, version_file};
use icetray_core::config::{Residency, SourceConfig, TableConfig, ValueColumnConfig, ValueFormat};
use icetray_core::contract::KEY_BYTES_ADVICE;
use icetray_core::key::UUID_BYTES;
use icetray_core::time::millis_after;
use icetray_core::{ColumnRead, Key, KeyType, Row, Scanned};
use parquet::arrow::{ArrowWriter, PARQUET_FIELD_ID_META_KEY};
use parquet::file::properties::WriterProperties;
use roaring::RoaringTreemap;
use tempfile::TempDir;

use crate::delete_files::{PositionDeletes, Vector, write_position_deletes, write_vectors};
use crate::error::{Result, SpecFault};
use crate::keys::{
    FIRST_VERSION, KeyShape, SCATTERED_ID_SPACE, STRING_KEY_FIXED_BYTES, VALUE_BYTES, ValueKind,
    absent_key_from, id_of, key_at_shaped, value_of_version,
};
use crate::layout::{KEY_COLUMN, Layout, VALUE_COLUMN};
use crate::merge_oracle::{MergeOracle, RowCopy};
use crate::ordered::{
    LiveRows, ORDER_COLUMN, ORDER_FIELD_ID, Ordered, RowId, ScanRows, TestScan, order_value,
    row_value, scan_model, scanned_of,
};
use crate::protobuf;
use crate::s3::S3Fixture;

/// Ids of the small fixture: twenty thousand rows, about 1,250 per bucket
/// of `SMALL_BUCKETS`, so every bucket's file holds two contract pages.
pub const SMALL_IDS: Range<u64> = 0..20_000;

/// Buckets of the small fixture: one file per bucket at the contract's
/// file size target.
pub const SMALL_BUCKETS: u32 = 16;

/// Ids sampled on the small fixture: spread over buckets and pages so that
/// under every key type no two land on one value page, and on distinct
/// thousands so the same holds for an unpartitioned fixture of 1,000-row
/// files; a cold batch over it costs one GET per id, which the lookup suite
/// proves through the plan.
pub const SAMPLE: [u64; 12] = [
    0, 1_500, 2_134, 3_783, 5_000, 6_402, 7_372, 8_191, 9_894, 10_000, 12_416, 17_000,
];

/// The namespace every in-process fixture is written in. Each test opens a
/// catalog of its own, so one name serves every fixture.
pub const NAMESPACE: &str = "ns";

/// Ids of the fixtures whose files hold no more than one contract page:
/// 4,000 ids over `FOUR_BUCKETS` or more is at most a thousand rows per
/// file.
pub const IDS_4K: Range<u64> = 0..4_000;

/// Ids of the two-bucket fixtures: a thousand rows, one contract page, per
/// file, the smallest fixture with more than one file.
pub const IDS_2K: Range<u64> = 0..2_000;

/// Ids above `IDS_2K` a test draws the keys of a raw file from when it
/// records them under a bucket of its choosing
/// (`Fixture::raw_file_in_bucket`): none is one `IDS_2K` wrote, and each of
/// two buckets gets dozens.
pub const STRAY_IDS: Range<u64> = IDS_2K.end..IDS_2K.end + STRAY_ID_COUNT;

/// How many ids `STRAY_IDS` spans.
const STRAY_ID_COUNT: u64 = 100;

/// Ids sampled on an `IDS_4K` fixture: both ends and the middle, so the
/// sample reaches every bucket's file.
pub const SAMPLE_4K: [u64; 5] = [0, 1, 1_234, 2_999, 3_999];

/// An append to an `IDS_4K` fixture repeating the upper half of its ids:
/// every bucket gains a file whose keys the first build already holds.
pub const OVERLAPPING_APPEND: Range<u64> = 2_000..6_000;

/// An append to an `IDS_4K` fixture above every id it holds: new files
/// that overlap nothing.
pub const APPENDED_4K_6K: Range<u64> = 4_000..6_000;

/// An id `APPENDED_4K_6K` adds, absent before the append.
pub const NEW_ID: u64 = 5_000;

/// An id every first build holds, far from any bucket's edge.
pub const OLD_ID: u64 = 10;

/// One bucket: every id in one file, for a fixture whose one file is the
/// subject.
pub const ONE_BUCKET: u32 = 1;

/// Two buckets: an `IDS_2K` fixture at a thousand rows per file.
pub const TWO_BUCKETS: u32 = 2;

/// Four buckets: an `IDS_4K` fixture at a thousand rows per file.
pub const FOUR_BUCKETS: u32 = 4;

/// A resident-bytes or chunk-file budget no file fits in.
pub const TINY_BUDGET: u64 = 1;

/// A file size target below one write batch of contract pages, so the
/// rolling writer starts a new file at every batch and an unpartitioned
/// fixture holds one file per thousand ids.
pub const UNPARTITIONED_FILE_BYTES: usize = 64 * 1024;

/// A file size target below one write batch of the small fixture, so the
/// rolling writer starts a second file in every bucket.
pub const SMALL_FILE_BYTES: usize = 40 * 1024;

/// Bytes per value in the wide-value fixtures: sixteen kibibytes, so a
/// row-bounded page of a thousand values is sixteen mebibytes, far over
/// the value page advice, while a byte-bounded page holds a few.
pub const WIDE_VALUE_BYTES: usize = 16 * 1024;

/// Bytes past `KEY_BYTES_ADVICE` every key of the long-key fixture carries:
/// well over the advice length and far under `MAX_KEY_BYTES`, so the keys
/// draw advice and no refusal.
pub const LONG_KEY_EXTRA: usize = 93;

/// The length of every key of the long-key fixture.
pub const LONG_KEY_BYTES: usize = KEY_BYTES_ADVICE + LONG_KEY_EXTRA;

/// The `key_pad` that makes a string key `LONG_KEY_BYTES` long.
pub const LONG_KEY_PAD: usize = LONG_KEY_BYTES - STRING_KEY_FIXED_BYTES;

/// Ids of the interleaved fixture, written at `INTERLEAVE_STRIDE` so the
/// other residue classes (`residue_class`) can be appended between them.
pub const INTERLEAVED_IDS: Range<u64> = 0..8_000;

/// The stride of the interleaved fixture's first build: four residue
/// classes, one written and three left for appends.
pub const INTERLEAVE_STRIDE: u64 = 4;

/// Buckets of the interleaved fixture: one file per bucket per build, so an
/// appended class widens every bucket's overlap group by one file.
pub const INTERLEAVED_BUCKETS: u32 = 4;

/// The residue class appended to the interleaved fixture after its first
/// build: the `2 mod 4` ids, which widen every bucket's group to two files.
pub const SECOND_CLASS: u64 = 2;

/// The residue class appended after `SECOND_CLASS`: the `1 mod 4` ids,
/// which widen every bucket's group to three files.
pub const THIRD_CLASS: u64 = 1;

/// Files per bucket of the interleaved fixture after `SECOND_CLASS`.
pub const TWO_FILES: usize = 2;

/// Files per bucket of the interleaved fixture after `THIRD_CLASS`.
pub const THREE_FILES: usize = 3;

/// A range-disjoint append to the interleaved fixture: above every id of
/// `INTERLEAVED_IDS`, so its files join no overlap group.
pub const TAIL: Range<u64> = 8_000..9_000;

/// Ids sampled on the interleaved fixture after `SECOND_CLASS`: both
/// classes, both ends.
pub const INTERLEAVED_SAMPLE: [u64; 6] = [0, 2, 1_234, 2_998, 4_002, 7_998];

/// Ids only the newest files of the interleaved fixture hold after
/// `THIRD_CLASS` and `TAIL`: three of the third class and one of the tail.
pub const NEWEST_SAMPLE: [u64; 4] = [1, 4_001, 7_997, 8_500];

/// The field id of the key column in every fixture schema.
pub const KEY_FIELD_ID: i32 = 1;

/// The field id of the value column in every fixture schema.
pub const VALUE_FIELD_ID: i32 = 2;

/// The field id of `SECOND_VALUE_COLUMN` in a schema created with it.
pub const SECOND_VALUE_FIELD_ID: i32 = 3;

/// A second binary value column, written into every file whose schema
/// holds it: from the first build when `FixtureSpec::second_value` is set,
/// or from the `write_rows` after `Fixture::evolve` adds it. Each row's
/// `value` bytes reversed, null where `value` is, so a table entry naming
/// it reads bytes no entry naming `value` returns.
pub const SECOND_VALUE_COLUMN: &str = "detail";

/// The schema id every fixture table is created with.
const SCHEMA_ID: i32 = 0;

/// The partition spec id every fixture table is created with.
const SPEC_ID: i32 = 0;

/// The sort order id of the key sort every fixture table declares.
const SORT_ORDER_ID: i64 = 1;

/// The partition field's name under a bucket spec.
const KEY_BUCKET_FIELD: &str = "key_bucket";

/// The bucket every file of an unpartitioned table is grouped under.
const UNPARTITIONED_BUCKET: i32 = 0;

/// The file prefix of a fixture's first commit.
const FIRST_BUILD_PREFIX: &str = "build1";

/// The directory under a table root holding data files: the library's
/// default, where raw files are uploaded beside the writer's and where a
/// by-hand snapshot's delete file is named.
pub const DATA_DIR: &str = "data";

/// The directory under a table root holding manifests and manifest lists,
/// beside the metadata files, as every Iceberg writer lays it out.
const MANIFEST_DIR: &str = "metadata";

/// The extension of a manifest or manifest list.
const MANIFEST_SUFFIX: &str = ".avro";

/// The id of a by-hand snapshot on a table with none.
const FIRST_SNAPSHOT_ID: i64 = 1;

/// The time a by-hand snapshot is stamped past the metadata's last update:
/// one millisecond, the least step Iceberg's timestamps can hold, which keeps
/// snapshot timestamps increasing, as Iceberg requires of a commit.
const HAND_COMMIT_STEP: Duration = Duration::from_millis(1);

/// The name the memory catalog is registered under; nothing reads it back.
const MEMORY_CATALOG_NAME: &str = "memory";

/// What to write.
#[derive(Clone, Debug, PartialEq)]
pub struct FixtureSpec {
    /// The table name within the namespace.
    pub name: String,
    /// The key column's type.
    pub key_type: KeyType,
    /// Buckets of the `bucket(N, key)` spec; zero writes an unpartitioned
    /// table.
    pub buckets: u32,
    /// The ids written, in fixture key space (`key_at_shaped`).
    pub ids: Range<u64>,
    /// Write every `id_stride`-th id of `ids`; one (or zero) writes every
    /// id. A later `append` of the skipped ids interleaves keys into the
    /// existing files' ranges.
    pub id_stride: u64,
    /// The order ids are handed to the writer within each bucket.
    pub order: Order,
    /// Extra prefix bytes on every string or binary key, for long keys; a
    /// long or uuid key takes none.
    pub key_pad: usize,
    /// How string keys are spelled; a key of another type takes the
    /// sequential shape alone.
    pub key_shape: KeyShape,
    /// Bytes per value.
    pub value_bytes: usize,
    /// What the values are made of.
    pub value_kind: ValueKind,
    /// Which ids get a null value.
    pub nulls: Nulls,
    /// The Parquet layout every data file is written in.
    pub layout: Layout,
    /// The table's Iceberg format version: the catalog creates it at this
    /// version, and a by-hand commit writes its manifests and manifest
    /// list at it.
    pub format_version: FormatVersion,
    /// Whether the table also holds `SECOND_VALUE_COLUMN`.
    pub second_value: bool,
    /// The rows per id and the order column of an ordered table (RFD 025);
    /// `None` writes a key-value table, one row per id.
    pub ordered: Option<Ordered>,
}

/// The order ids are handed to the writer within each bucket. `Descending`
/// produces files whose keys descend, which the reader refuses as unsorted.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Order {
    /// Keys ascend within every file: the layout's clause.
    Ascending,
    /// Keys descend within every file.
    Descending,
}

/// Which ids get a null value.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Nulls {
    /// Every value is present.
    None,
    /// Every id that is a multiple of this gets a null; zero names no id.
    EveryNth(u64),
}

impl FixtureSpec {
    /// The small fixture: `SMALL_IDS` in `SMALL_BUCKETS`, every id, ascending,
    /// sequential keys with no padding, `VALUE_BYTES` random bytes per
    /// value, no nulls, the contract layout.
    /// Every other shape is a struct update on it.
    pub fn small(name: &str, key_type: KeyType) -> Self {
        Self {
            name: name.to_string(),
            key_type,
            buckets: SMALL_BUCKETS,
            ids: SMALL_IDS,
            id_stride: 1,
            order: Order::Ascending,
            key_pad: 0,
            key_shape: KeyShape::Sequential,
            value_bytes: VALUE_BYTES,
            value_kind: ValueKind::Random,
            nulls: Nulls::None,
            layout: Layout::contract(),
            format_version: FormatVersion::V2,
            second_value: false,
            ordered: None,
        }
    }

    /// The small fixture as an ordered table: every id has the rows
    /// `ordered` names, sorted by key and then order value, and every other
    /// shape is a struct update on it as for `small`.
    pub fn ordered(name: &str, key_type: KeyType, ordered: Ordered) -> Self {
        Self {
            ordered: Some(ordered),
            ..Self::small(name, key_type)
        }
    }

    /// The interleaved fixture: every `INTERLEAVE_STRIDE`-th id of
    /// `INTERLEAVED_IDS` in `INTERLEAVED_BUCKETS` buckets, long keys,
    /// otherwise the small fixture's shape. An `append` of another
    /// `residue_class` writes one file per bucket whose range interleaves
    /// with the first build's, so every bucket holds an overlap group.
    pub fn interleaved(name: &str) -> Self {
        Self {
            ids: INTERLEAVED_IDS,
            id_stride: INTERLEAVE_STRIDE,
            buckets: INTERLEAVED_BUCKETS,
            ..Self::small(name, KeyType::Long)
        }
    }

    /// The `i`-th key in this specification's key type, padding, and shape.
    fn key(&self, i: u64) -> Key {
        key_at_shaped(i, self.key_type, self.key_pad, self.key_shape)
    }

    /// `SpecFault::IdOutsideScatteredSpace` for the first of `ids` that has
    /// no scattered key, when the keys are scattered.
    fn check_ids(&self, ids: &[u64]) -> Result<()> {
        if self.key_shape != KeyShape::Scattered {
            return Ok(());
        }
        match ids.iter().find(|&&id| id >= SCATTERED_ID_SPACE) {
            Some(&id) => Err(SpecFault::IdOutsideScatteredSpace { id }.into()),
            None => Ok(()),
        }
    }

    /// Whether `id`'s value is null.
    fn is_null(&self, id: u64) -> bool {
        match self.nulls {
            Nulls::None => false,
            Nulls::EveryNth(n) => id.checked_rem(n) == Some(0),
        }
    }

    /// The rows of `ids`, each id's in the order they are written: the one
    /// row of a key-value table, or the rows the ordered specification
    /// gives the id.
    fn rows_of(&self, ids: &[u64]) -> Result<Vec<RowId>> {
        let Some(ordered) = &self.ordered else {
            return Ok(ids.iter().map(|&id| RowId::keyed(id)).collect());
        };
        let mut rows = Vec::new();
        for &id in ids {
            rows.extend(
                ordered
                    .orders_of(id)?
                    .into_iter()
                    .map(|order| RowId::at(id, order)),
            );
        }
        Ok(rows)
    }

    /// The ids the first commit writes, in `order`.
    fn written_ids(&self) -> Vec<u64> {
        let stride = self.id_stride.max(1);
        let start = self.ids.start;
        let mut ids: Vec<u64> = self
            .ids
            .clone()
            .filter(|id| (id - start).is_multiple_of(stride))
            .collect();
        if self.order == Order::Descending {
            ids.reverse();
        }
        ids
    }
}

/// A Parquet file to commit as it is, for what the library's writer cannot
/// produce: a key column whose Parquet type contradicts the schema, a null
/// key, a table of a type icetray refuses, or keys recorded under a bucket
/// they do not hash to. `partition` is the tuple of the table's default
/// spec: `Struct::empty()` on an unpartitioned table, the bucket on a
/// bucketed one (`Fixture::raw_file_in_bucket`).
#[derive(Clone, Debug)]
pub struct RawFile {
    /// The file's name under the table's data directory, extension included.
    pub name: String,
    /// The file's bytes.
    pub bytes: Bytes,
    /// The rows the manifest records for it.
    pub records: u64,
    /// The partition tuple the manifest records for it.
    pub partition: Struct,
    /// The key bounds the manifest records, when any.
    pub bounds: Option<(Key, Key)>,
}

impl RawFile {
    /// `batch` written as one Parquet file under `layout`'s properties.
    pub fn from_batch(
        name: &str,
        batch: &RecordBatch,
        layout: &Layout,
        partition: Struct,
        bounds: Option<(Key, Key)>,
    ) -> Result<Self> {
        Self::written(
            name,
            batch,
            layout.properties(batch.num_rows()),
            partition,
            bounds,
        )
    }

    /// `batch` written as one Parquet file under `properties`.
    fn written(
        name: &str,
        batch: &RecordBatch,
        properties: WriterProperties,
        partition: Struct,
        bounds: Option<(Key, Key)>,
    ) -> Result<Self> {
        let mut out = Vec::new();
        let mut writer = ArrowWriter::try_new(&mut out, batch.schema(), Some(properties))?;
        writer.write(batch)?;
        writer.close()?;
        Ok(Self {
            name: name.to_string(),
            bytes: Bytes::from(out),
            records: count(batch.num_rows()),
            partition,
            bounds,
        })
    }
}

/// An arrow field that a Parquet writer records under `field_id`, as
/// Iceberg's writers record every column (RFD 023): a raw file built from
/// fields like this records field ids, one built from plain fields records
/// none.
pub fn field_with_id(name: &str, data_type: DataType, nullable: bool, field_id: i32) -> Field {
    Field::new(name, data_type, nullable).with_metadata(HashMap::from([(
        PARQUET_FIELD_ID_META_KEY.to_string(),
        field_id.to_string(),
    )]))
}

/// One change `Fixture::evolve` makes to the current schema's top-level
/// columns, as a writer's schema evolution does: every column keeps its
/// field id across a rename, and an added column takes one above every id
/// the table has assigned.
#[derive(Clone, Debug, PartialEq)]
pub enum SchemaChange {
    /// Adds an optional column `name` of `ty` stating `default`.
    Add {
        /// The new column's name.
        name: String,
        /// Its type.
        ty: PrimitiveType,
        /// The `initial-default` the schema states for it.
        default: StatedDefault,
    },
    /// Renames the column `from` to `to`.
    Rename {
        /// The column's name before.
        from: String,
        /// Its name after.
        to: String,
    },
    /// Drops the column `name`.
    Drop {
        /// The column's name.
        name: String,
    },
}

/// The `initial-default` an added column states.
#[derive(Clone, Debug, PartialEq)]
pub enum StatedDefault {
    /// None: rows written before the column read null.
    None,
    /// This literal, which the iceberg crate writes in the spec's JSON form.
    Literal(Literal),
    /// This JSON, written into every metadata file from then on as it is,
    /// past the iceberg crate, which cannot write a default it cannot read:
    /// how a test states a default that does not read as its column's type.
    Raw(serde_json::Value),
}

/// A snapshot to commit by hand: the data files it lists and the delete
/// files it lists (`Fixture::commit_by_hand`).
#[derive(Clone, Debug, Default)]
pub struct HandSnapshot {
    /// Every data file of the snapshot.
    pub data: Vec<DataFile>,
    /// The delete files of the snapshot.
    pub deletes: Vec<DataFile>,
}

/// How `Fixture::delete` and `Fixture::update` record the rows they
/// remove (RFD 021).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DeleteForm {
    /// A deletion vector per data file touched, all in one new Puffin file.
    /// A data file that already has a vector gets a new one holding its old
    /// rows and the new ones, and the old entry leaves the snapshot, as a
    /// v3 writer replaces it; its Puffin file may still hold other live
    /// vectors.
    Vectors,
    /// A Parquet position delete file per data file touched, naming it as
    /// its referenced data file.
    ScopedPositions,
    /// A Parquet position delete file per partition touched, naming no
    /// referenced data file.
    Positions,
}

/// The commit that added a file: what an existing entry carries it at.
#[derive(Clone, Copy, Debug)]
struct AddedBy {
    snapshot_id: i64,
    sequence: i64,
}

/// A written fixture: the table as this fixture last committed it, and the
/// one source of truth for what it holds.
#[derive(Debug)]
pub struct Fixture {
    /// The table after the last catalog commit: its `FileIO`, location,
    /// schema, and spec.
    table: Table,
    ident: TableIdent,
    spec: FixtureSpec,
    /// Every id written so far; `keys`, `ids`, and `id_range` derive from it.
    ids: BTreeSet<u64>,
    /// The current snapshot as this fixture knows it, catalog or by hand.
    metadata: TableMetadata,
    metadata_location: String,
    /// Every data file the current snapshot lists.
    files: Vec<DataFile>,
    /// Every delete file the current snapshot lists.
    deletes: Vec<DataFile>,
    /// The commit that added each file the current snapshot lists, data or
    /// delete, by path: what a by-hand commit carries the file at as an
    /// existing entry, as a writer does.
    added: HashMap<String, AddedBy>,
    /// The version each id's live row was written at, for ids an `update`
    /// rewrote; every other id is at `FIRST_VERSION`.
    versions: HashMap<u64, u64>,
    /// The live rows of an ordered fixture, by (id, order value), each at
    /// the version that wrote it; empty for a key-value one.
    rows: LiveRows,
    /// What each snapshot this fixture committed by hand listed, by
    /// snapshot id, so `move_ref` can return the table to one.
    history: HashMap<i64, Listing>,
    /// Whether a snapshot was committed by hand, after which the catalog's
    /// record is behind and only `commit_by_hand` may advance the table.
    by_hand: bool,
    /// By-hand commits so far; the next writes metadata version one above.
    hand_commits: u64,
    /// The `StatedDefault::Raw` defaults `evolve` added, by field id, which
    /// every metadata file written from then on states.
    raw_defaults: Vec<(i32, serde_json::Value)>,
}

/// What one snapshot lists, as a `Fixture` follows it.
#[derive(Clone, Debug)]
struct Listing {
    ids: BTreeSet<u64>,
    versions: HashMap<u64, u64>,
    rows: LiveRows,
    files: Vec<DataFile>,
    deletes: Vec<DataFile>,
    added: HashMap<String, AddedBy>,
}

impl Fixture {
    /// A fixture over a table with no id written yet.
    fn new(table: Table, ident: TableIdent, spec: FixtureSpec) -> Result<Self> {
        let metadata = table.metadata().clone();
        let metadata_location = table.metadata_location_result()?.to_string();
        Ok(Self {
            table,
            ident,
            spec,
            ids: BTreeSet::new(),
            metadata,
            metadata_location,
            files: Vec::new(),
            deletes: Vec::new(),
            added: HashMap::new(),
            versions: HashMap::new(),
            rows: LiveRows::default(),
            history: HashMap::new(),
            by_hand: false,
            hand_commits: 0,
            raw_defaults: Vec::new(),
        })
    }

    /// The specification the fixture was written from.
    pub fn spec(&self) -> &FixtureSpec {
        &self.spec
    }

    /// The table's identifier in its catalog.
    pub fn ident(&self) -> &TableIdent {
        &self.ident
    }

    /// The field id the current schema gives the order column, `None` for a
    /// schema without one (`ORDER_FIELD_ID`).
    pub fn order_field_id(&self) -> Option<i32> {
        self.metadata
            .current_schema()
            .field_by_name(ORDER_COLUMN)
            .map(|field| field.id)
    }

    /// The table root.
    pub fn root(&self) -> &str {
        self.metadata.location()
    }

    /// The current metadata file's location.
    pub fn metadata_location(&self) -> &str {
        &self.metadata_location
    }

    /// The current metadata.
    pub fn metadata(&self) -> &TableMetadata {
        &self.metadata
    }

    /// The `i`-th key of this fixture, in its key type, padding, and shape.
    pub fn key(&self, i: u64) -> Key {
        self.spec.key(i)
    }

    /// A key just above the `i`-th that no fixture holds.
    pub fn absent_near(&self, i: u64) -> Key {
        absent_key_from(&self.key(i))
    }

    /// The value stored for `key` in a key-value fixture (an ordered
    /// fixture's rows answer through `expected_scan`): `None` for an id
    /// `Nulls` names, else the
    /// `value_bytes` bytes of `value_kind` derived from the key and the
    /// version its live row was written at (`value_of_version`).
    pub fn value(&self, key: &Key) -> Option<Vec<u8>> {
        let id = id_of(key, self.spec.key_pad, self.spec.key_shape);
        let null = id.is_some_and(|id| self.spec.is_null(id));
        let version = id.map_or(FIRST_VERSION, |id| self.version_of(id));
        (!null).then(|| value_of_version(key, self.spec.value_bytes, self.spec.value_kind, version))
    }

    /// The bytes `SECOND_VALUE_COLUMN` holds for `key`: `value` reversed.
    pub fn second_value(&self, key: &Key) -> Option<Vec<u8>> {
        self.value(key).map(reversed)
    }

    /// The version `id`'s live row was written at: `FIRST_VERSION` until
    /// an `update` rewrites it.
    pub(crate) fn version_of(&self, id: u64) -> u64 {
        self.versions.get(&id).copied().unwrap_or(FIRST_VERSION)
    }

    /// The version `row`'s live row was written at: `version_of` its id in
    /// a key-value fixture, the version of its (id, order value) pair in an
    /// ordered one.
    fn version_of_row(&self, row: RowId) -> u64 {
        match row.order {
            None => self.version_of(row.id),
            Some(order) => self.rows.version(row.id, order),
        }
    }

    /// What a lookup naming `VALUE_COLUMN` alone answers for a key this
    /// fixture holds (`expected_of`).
    pub fn expected(&self, key: &Key) -> Row {
        self.expected_of(key, &[VALUE_COLUMN])
    }

    /// What a lookup naming `columns`, each `VALUE_COLUMN` or
    /// `SECOND_VALUE_COLUMN`, answers for a key this fixture holds:
    /// `Present` with each column's value in that order, `None` for a
    /// null.
    ///
    /// Panics on a column the fixture writes no value for.
    pub fn expected_of(&self, key: &Key, columns: &[&str]) -> Row {
        Row::Present(
            columns
                .iter()
                .map(|&column| column_value(column, self.value(key)).map(Bytes::from))
                .collect(),
        )
    }

    /// What a lookup reading `reads` answers for a key this fixture holds:
    /// `expected_of` its columns, with each read that names fields trimmed
    /// to them as a projection trims the value (RFD 024), from the fields
    /// the value was written from (`protobuf::projected`), a null staying
    /// null.
    ///
    /// Panics on fields of a column other than a `VALUE_COLUMN` of
    /// `ValueKind::Protobuf`.
    pub fn expected_reads(&self, key: &Key, reads: &[ColumnRead<'_>]) -> Row {
        let names: Vec<&str> = reads.iter().map(|read| read.name).collect();
        let Row::Present(mut values) = self.expected_of(key, &names) else {
            unreachable!("expected_of answers present");
        };
        for (value, read) in values.iter_mut().zip(reads) {
            if read.fields.is_empty() {
                continue;
            }
            assert!(
                read.name == VALUE_COLUMN && self.spec.value_kind == ValueKind::Protobuf,
                "the fixture writes protobuf into a protobuf VALUE_COLUMN alone"
            );
            if value.is_some() {
                let id = id_of(key, self.spec.key_pad, self.spec.key_shape);
                let version = id.map_or(FIRST_VERSION, |id| self.version_of(id));
                *value = Some(Bytes::from(protobuf::projected(
                    key,
                    self.spec.value_bytes,
                    version,
                    read.fields,
                )));
            }
        }
        Row::Present(values)
    }

    /// What a Scan of `key` naming `columns`, each `VALUE_COLUMN` or
    /// `SECOND_VALUE_COLUMN`, answers on this ordered fixture: the naive
    /// sort of `key`'s live rows (`scan_model`), each as its raw order value
    /// and one value per column, `None` for a null, and the raw order
    /// value of the last row returned when live rows remain inside the
    /// bounds past it, which the next page's bound follows
    /// (`TestScan::next_after`). A key the fixture holds no row for answers
    /// no rows and no such value.
    ///
    /// Panics on a fixture whose specification names no order column, and
    /// on a column the fixture writes no value for.
    pub fn expected_scan(
        &self,
        key: &Key,
        columns: &[&str],
        scan: &TestScan,
    ) -> (ScanRows, Option<i64>) {
        assert!(
            self.spec.ordered.is_some(),
            "expected_scan is for a fixture with an order column"
        );
        let Some(id) = id_of(key, self.spec.key_pad, self.spec.key_shape) else {
            return (Vec::new(), None);
        };
        let rows = self.rows.of_id(id).map(|(order, version)| {
            let value = (!self.spec.is_null(id)).then(|| {
                row_value(
                    key,
                    order,
                    self.spec.value_bytes,
                    self.spec.value_kind,
                    version,
                )
            });
            let values = columns
                .iter()
                .map(|&column| column_value(column, value.clone()))
                .collect();
            (order, values)
        });
        scan_model(rows, scan)
    }

    /// `expected_scan` as the core's answer (`scanned_of`): each row's order
    /// value in the fixture's order kind, and its values as bytes.
    ///
    /// Panics as `expected_scan` does.
    pub fn expected_scanned(&self, key: &Key, columns: &[&str], scan: &TestScan) -> Scanned {
        let answer = self.expected_scan(key, columns, scan);
        let kind = self
            .spec
            .ordered
            .expect("expected_scan answered, so the fixture has an order column")
            .kind;
        scanned_of(
            answer,
            scan,
            |order| order_value(kind, order),
            |_, values| {
                values
                    .into_iter()
                    .map(|value| value.map(Bytes::from))
                    .collect()
            },
        )
    }

    /// Every key written, ascending by id.
    pub fn keys(&self) -> impl Iterator<Item = Key> + '_ {
        self.ids.iter().map(|&id| self.key(id))
    }

    /// The keys at `ids`, in that order: a sample as the plan and the
    /// lookups take it.
    pub fn keys_at(&self, ids: &[u64]) -> Vec<Key> {
        ids.iter().map(|&id| self.key(id)).collect()
    }

    /// Every id written, ascending.
    pub fn ids(&self) -> impl Iterator<Item = u64> + '_ {
        self.ids.iter().copied()
    }

    /// The smallest id written to one above the largest; empty when none.
    pub fn id_range(&self) -> Range<u64> {
        match (self.ids.first(), self.ids.last()) {
            (Some(&first), Some(&last)) => first..last + 1,
            (None, _) | (_, None) => 0..0,
        }
    }

    /// Data files the current snapshot lists.
    pub fn file_count(&self) -> usize {
        self.files.len()
    }

    /// Every data file the current snapshot lists, as its manifest records
    /// it; what a `HandSnapshot` that drops or keeps files is built from.
    pub fn files(&self) -> &[DataFile] {
        &self.files
    }

    /// The table through `catalog`, by identifier.
    pub fn catalog_source(&self, catalog: Arc<dyn Catalog>) -> TableSource {
        TableSource::Catalog {
            catalog,
            ident: self.ident.clone(),
        }
    }

    /// The table root, followed through its version hint (`write_hint`).
    pub fn root_source(&self) -> SourceConfig {
        SourceConfig::Static {
            location: self.root().to_string(),
        }
    }

    /// The current metadata file itself, which never advances.
    pub fn pinned_source(&self) -> SourceConfig {
        SourceConfig::Static {
            location: self.metadata_location.clone(),
        }
    }

    /// A configuration serving this fixture's main branch in `residency`
    /// from the pinned source, every value column the spec wrote opaque and,
    /// for an ordered fixture, `ORDER_COLUMN` as the order column at the
    /// default `max_scan_rows`; a caller that follows the root or a catalog
    /// overrides `source`. No resident-bytes budget.
    pub fn config(&self, residency: Residency, strict: bool) -> TableConfig {
        self.config_with_budget(residency, strict, None)
    }

    /// `config` with `budget` as the resident-bytes budget.
    pub fn config_with_budget(
        &self,
        residency: Residency,
        strict: bool,
        budget: Option<u64>,
    ) -> TableConfig {
        let mut value_columns = vec![ValueColumnConfig::named(VALUE_COLUMN)];
        if self.spec.second_value {
            value_columns.push(ValueColumnConfig::named(SECOND_VALUE_COLUMN));
        }
        TableConfig {
            name: self.spec.name.clone(),
            source: self.pinned_source(),
            r#ref: MAIN_BRANCH.to_string(),
            key_column: KEY_COLUMN.to_string(),
            value_columns,
            order_column: self.spec.ordered.map(|_| ORDER_COLUMN.to_string()),
            max_scan_rows: None,
            strict,
            lookups: None,
            residency,
            resident_budget_bytes: budget,
            row_cache: None,
        }
    }

    /// `config`, not strict, serving `columns` instead, each opaque: fewer
    /// than the spec wrote, or ones a later schema added.
    pub fn config_serving(&self, residency: Residency, columns: &[&str]) -> TableConfig {
        TableConfig {
            value_columns: columns
                .iter()
                .map(|&column| ValueColumnConfig::named(column))
                .collect(),
            ..self.config(residency, false)
        }
    }

    /// `config`, not strict, with the column named `column`, one the spec
    /// wrote, declared `format`.
    pub fn config_declaring(
        &self,
        residency: Residency,
        column: &str,
        format: ValueFormat,
    ) -> TableConfig {
        let mut config = self.config(residency, false);
        config
            .value_columns
            .iter_mut()
            .find(|declared| declared.name == column)
            .expect("the column declared is one the spec wrote")
            .format = format;
        config
    }

    /// Writes `ids` in the given order as new data files and commits them
    /// through `catalog` with a fast append; the table advances. An ordered
    /// fixture writes every row of each id, ascending by order value.
    /// Refused once a snapshot was committed by hand.
    pub async fn append(
        &mut self,
        catalog: &dyn Catalog,
        ids: impl IntoIterator<Item = u64>,
        prefix: &str,
    ) -> Result<()> {
        let ids: Vec<u64> = ids.into_iter().collect();
        let rows = self.spec.rows_of(&ids)?;
        self.append_rows(catalog, &rows, prefix).await
    }

    /// Writes the rows at (id, raw order value) of `rows`, in the given
    /// order, as new data files and commits them through `catalog` with a
    /// fast append. A caller that wants a valid file passes rows ascending
    /// by id and then order value; a repeated pair or a descending run
    /// writes the file a reader refuses. A fixture whose specification
    /// names no order column is `SpecFault::NotOrdered`; once a snapshot
    /// was committed by hand the append is refused.
    pub async fn append_ordered(
        &mut self,
        catalog: &dyn Catalog,
        rows: &[(u64, i64)],
        prefix: &str,
    ) -> Result<()> {
        let rows = self.ordered_rows(rows)?;
        self.append_rows(catalog, &rows, prefix).await
    }

    /// `rows` as `RowId`s, when the fixture is ordered.
    fn ordered_rows(&self, rows: &[(u64, i64)]) -> Result<Vec<RowId>> {
        if self.spec.ordered.is_none() {
            return Err(SpecFault::NotOrdered.into());
        }
        Ok(rows
            .iter()
            .map(|&(id, order)| RowId::at(id, order))
            .collect())
    }

    /// Writes `rows` in the given order as new data files and commits them
    /// with a fast append (`append`).
    async fn append_rows(
        &mut self,
        catalog: &dyn Catalog,
        rows: &[RowId],
        prefix: &str,
    ) -> Result<()> {
        if self.by_hand {
            return Err(SpecFault::CatalogBehind.into());
        }
        let ids: Vec<u64> = rows.iter().map(|row| row.id).collect();
        self.spec.check_ids(&ids)?;
        let files = write_data_files(
            &self.table,
            &self.spec,
            rows.iter().copied(),
            prefix,
            &|row| self.version_of_row(row),
        )
        .await?;
        self.commit_files(catalog, files).await?;
        self.note_written(rows);
        self.remember_current();
        Ok(())
    }

    /// Records `rows` as written: their ids as held and, in an ordered
    /// fixture, their pairs as live, at the version a live pair already
    /// has.
    fn note_written(&mut self, rows: &[RowId]) {
        for row in rows {
            self.ids.insert(row.id);
            if let Some(order) = row.order {
                let version = self.rows.version(row.id, order);
                self.rows.insert(row.id, order, version);
            }
        }
    }

    /// Records `rows` as no longer live, and their ids as no longer held
    /// once none of an ordered id's rows is.
    fn forget(&mut self, rows: &[RowId]) {
        for row in rows {
            match row.order {
                None => {
                    self.ids.remove(&row.id);
                    self.versions.remove(&row.id);
                }
                Some(order) => {
                    self.rows.remove(row.id, order);
                    if !self.rows.holds(row.id) {
                        self.ids.remove(&row.id);
                    }
                }
            }
        }
    }

    /// The rows of `ids`, which ascend, as one raw file named `name` in this
    /// fixture's key type, values, and layout (every row of an id, for an
    /// ordered fixture), with exact key bounds, recorded
    /// under `bucket` whatever buckets its keys hash to: how a test commits
    /// a file whose keys fall outside its recorded bucket, which the
    /// library's writer, grouping rows by the bucket they hash to, cannot.
    /// `bucket` is the partition tuple's integer. `append_raw` commits it;
    /// the ids are not added to the fixture's.
    pub fn raw_file_in_bucket(&self, name: &str, ids: &[u64], bucket: i32) -> Result<RawFile> {
        self.spec.check_ids(ids)?;
        let schema = self.metadata.current_schema();
        let arrow_schema = Arc::new(iceberg::arrow::schema_to_arrow_schema(schema)?);
        let rows = self.spec.rows_of(ids)?;
        let keys: Vec<Key> = rows.iter().map(|row| self.key(row.id)).collect();
        let file_rows = batch(&arrow_schema, &self.spec, &rows, &keys, &|row| {
            self.version_of_row(row)
        })?;
        let bounds = keys.first().cloned().zip(keys.last().cloned());
        let layout = &self.spec.layout;
        let properties = match self.spec.ordered {
            Some(_) => {
                layout.ordered_properties(file_rows.num_rows(), order_column_index(&arrow_schema)?)
            }
            None => layout.properties(file_rows.num_rows()),
        };
        RawFile::written(
            name,
            &file_rows,
            properties,
            Struct::from_iter([Some(Literal::int(bucket))]),
            bounds,
        )
    }

    /// Uploads `file` under the table's data directory through the table's
    /// `FileIO` and commits it through `catalog` with a fast append, which
    /// admits any data file. Refused once a snapshot was committed by hand.
    pub async fn append_raw(&mut self, catalog: &dyn Catalog, file: RawFile) -> Result<()> {
        if self.by_hand {
            return Err(SpecFault::CatalogBehind.into());
        }
        let path = format!("{}/{DATA_DIR}/{}", self.root(), file.name);
        self.table
            .file_io()
            .new_output(&path)?
            .write(file.bytes.clone())
            .await?;
        let data_file = data_file(
            &path,
            self.metadata.default_partition_spec_id(),
            file.partition,
            (file.records, count(file.bytes.len())),
            file.bounds.as_ref(),
            DataFileFormat::Parquet,
        )?;
        self.commit_files(catalog, vec![data_file]).await?;
        self.remember_current();
        Ok(())
    }

    /// Commits `files` as one fast append and takes the table as the
    /// catalog returns it.
    async fn commit_files(&mut self, catalog: &dyn Catalog, files: Vec<DataFile>) -> Result<()> {
        let tx = Transaction::new(&self.table);
        let tx = tx
            .fast_append()
            .add_data_files(files.iter().cloned())
            .apply(tx)?;
        self.table = tx.commit(catalog).await?;
        self.metadata = self.table.metadata().clone();
        self.metadata_location = self.table.metadata_location_result()?.to_string();
        let current = self
            .metadata
            .current_snapshot()
            .expect("a committed append leaves a current snapshot");
        let by = AddedBy {
            snapshot_id: current.snapshot_id(),
            sequence: current.sequence_number(),
        };
        for file in &files {
            self.added.insert(file.file_path().to_string(), by);
        }
        self.files.extend(files);
        Ok(())
    }

    /// Commits `snapshot` by hand, past the catalog: one data manifest
    /// listing `snapshot.data`, one delete manifest listing
    /// `snapshot.deletes` when there are any, the manifest list, a snapshot
    /// with the current as parent, one sequence number above the table's
    /// last and an id above every snapshot's, and the metadata with the main
    /// branch moved to it, written as `version_file(root, n)` for the `n`-th
    /// by-hand commit and named by the version hint. Manifests and the list
    /// are written at the table's format version; a v3 snapshot carries the
    /// row range its list assigns. A file the current snapshot lists is an
    /// existing entry carrying the snapshot id and data sequence number it
    /// was added at; every other file is added at the new sequence number.
    /// This is how a snapshot carrying a delete file or dropping a data file
    /// is produced, neither of which a fast append can commit. From here on
    /// the table is followed through its root: its catalog record is behind,
    /// and `append` and `append_raw` refuse with `SpecFault::CatalogBehind`.
    pub async fn commit_by_hand(&mut self, snapshot: HandSnapshot) -> Result<()> {
        let root = self.root().to_string();
        let file_io = self.table.file_io().clone();
        let parent = self
            .metadata
            .current_snapshot()
            .map(|current| current.snapshot_id());
        let snapshot_id = self
            .metadata
            .snapshots()
            .map(|listed| listed.snapshot_id())
            .max()
            .map_or(FIRST_SNAPSHOT_ID, |last| last + 1);
        let sequence = self.metadata.last_sequence_number() + 1;
        let now = AddedBy {
            snapshot_id,
            sequence,
        };
        let mut manifests = Vec::new();
        let listings = [
            (Some(snapshot.data.as_slice()), false),
            (
                (!snapshot.deletes.is_empty()).then_some(snapshot.deletes.as_slice()),
                true,
            ),
        ];
        for (index, (files, deletes)) in listings.into_iter().enumerate() {
            let Some(files) = files else {
                continue;
            };
            let path = format!("{root}/{MANIFEST_DIR}/{snapshot_id}-m{index}{MANIFEST_SUFFIX}");
            manifests.push(self.write_hand_manifest(&path, files, deletes, now).await?);
        }
        let list_path = format!("{root}/{MANIFEST_DIR}/snap-{snapshot_id}{MANIFEST_SUFFIX}");
        let output = file_io.new_output(&list_path)?.writer().await?;
        let first_row_id = self.metadata.next_row_id();
        let mut list = match self.spec.format_version {
            FormatVersion::V3 => {
                ManifestListWriter::v3(output, snapshot_id, parent, sequence, Some(first_row_id))
            }
            FormatVersion::V1 | FormatVersion::V2 => {
                ManifestListWriter::v2(output, snapshot_id, parent, sequence)
            }
        };
        list.add_manifests(manifests.into_iter())?;
        let next_row_id = list.next_row_id();
        list.close().await?;
        let builder = Snapshot::builder()
            .with_snapshot_id(snapshot_id)
            .with_parent_snapshot_id(parent)
            .with_sequence_number(sequence)
            .with_timestamp_ms(millis_after(
                self.metadata.last_updated_ms(),
                HAND_COMMIT_STEP,
            ))
            .with_manifest_list(list_path)
            .with_summary(Summary {
                operation: Operation::Overwrite,
                additional_properties: HashMap::new(),
            })
            .with_schema_id(self.metadata.current_schema_id());
        let committed = match next_row_id {
            Some(next) => builder
                .with_row_range(first_row_id, next - first_row_id)
                .build(),
            None => builder.build(),
        };
        let metadata = self
            .metadata
            .clone()
            .into_builder(Some(self.metadata_location.clone()))
            .set_branch_snapshot(committed, MAIN_BRANCH)?
            .build()?
            .metadata;
        self.write_metadata(metadata).await?;
        self.added = snapshot
            .data
            .iter()
            .chain(&snapshot.deletes)
            .map(|file| {
                let path = file.file_path().to_string();
                let by = self.added.get(&path).copied().unwrap_or(now);
                (path, by)
            })
            .collect();
        self.files = snapshot.data;
        self.deletes = snapshot.deletes;
        self.remember_current();
        self.write_hint().await
    }

    /// Moves the main branch back, or forward, to `snapshot_id`, a snapshot
    /// this fixture committed, as a rollback does, writing the metadata as
    /// the next by-hand version and naming it in the version hint; the
    /// fixture then holds what that snapshot listed. A snapshot it did not
    /// commit is `SpecFault::UnknownSnapshot`.
    pub async fn move_ref(&mut self, snapshot_id: i64) -> Result<()> {
        let listing = self
            .history
            .get(&snapshot_id)
            .cloned()
            .ok_or(SpecFault::UnknownSnapshot { snapshot_id })?;
        let branch = SnapshotReference::new(
            snapshot_id,
            SnapshotRetention::Branch {
                min_snapshots_to_keep: None,
                max_snapshot_age_ms: None,
                max_ref_age_ms: None,
            },
        );
        let metadata = self
            .metadata
            .clone()
            .into_builder(Some(self.metadata_location.clone()))
            .set_ref(MAIN_BRANCH, branch)?
            .build()?
            .metadata;
        self.write_metadata(metadata).await?;
        self.ids = listing.ids;
        self.versions = listing.versions;
        self.rows = listing.rows;
        self.files = listing.files;
        self.deletes = listing.deletes;
        self.added = listing.added;
        self.write_hint().await
    }

    /// The current snapshot's id; `None` before the first commit.
    pub fn snapshot_id(&self) -> Option<i64> {
        self.metadata
            .current_snapshot()
            .map(|current| current.snapshot_id())
    }

    /// Writes `metadata` as the next by-hand version, every raw default
    /// `evolve` added stated in it (`stated_json`), and takes it as the
    /// table's for every by-hand write after, `commit_by_hand` and
    /// `move_ref` alike: the writers of `write_rows` write under its
    /// schema. The version hint is the caller's to write.
    async fn write_metadata(&mut self, metadata: TableMetadata) -> Result<()> {
        self.hand_commits += 1;
        let location = version_file(self.root(), self.hand_commits);
        self.table
            .file_io()
            .new_output(&location)?
            .write(Bytes::from(self.stated_json(&metadata)?))
            .await?;
        self.table = Table::builder()
            .metadata(metadata.clone())
            .metadata_location(location.clone())
            .identifier(self.ident.clone())
            .file_io(self.table.file_io().clone())
            .runtime(Runtime::try_current()?)
            .build()?;
        self.metadata = metadata;
        self.metadata_location = location;
        self.by_hand = true;
        Ok(())
    }

    /// `metadata` as JSON, with every raw default of `raw_defaults` stated
    /// on its field in every schema that holds the field.
    fn stated_json(&self, metadata: &TableMetadata) -> Result<Vec<u8>> {
        let mut json = serde_json::to_value(metadata)?;
        if let Some(schemas) = json
            .get_mut(SCHEMAS_KEY)
            .and_then(serde_json::Value::as_array_mut)
        {
            for field in schemas
                .iter_mut()
                .filter_map(|schema| schema.get_mut(FIELDS_KEY))
                .filter_map(serde_json::Value::as_array_mut)
                .flatten()
            {
                let id = field.get(FIELD_ID_KEY).and_then(serde_json::Value::as_i64);
                if let Some((_, default)) = self
                    .raw_defaults
                    .iter()
                    .find(|(field_id, _)| Some(i64::from(*field_id)) == id)
                {
                    field[INITIAL_DEFAULT_KEY] = default.clone();
                }
            }
        }
        Ok(serde_json::to_vec(&json)?)
    }

    /// Changes the current schema by `changes`, in order, and commits by
    /// hand a snapshot under the new schema listing every data and delete
    /// file the current one does (`commit_by_hand`): the files stay as they
    /// were written while what the schema means by their columns changes.
    /// A rename or drop of a column the schema lacks is
    /// `SpecFault::ColumnNotInSchema`. Rows written after (`write_rows`)
    /// carry the new schema's columns and field ids.
    pub async fn evolve(&mut self, changes: &[SchemaChange]) -> Result<()> {
        let mut fields: Vec<NestedFieldRef> =
            self.metadata.current_schema().as_struct().fields().to_vec();
        let mut next_id = self.metadata.last_column_id() + 1;
        for change in changes {
            match change {
                SchemaChange::Add { name, ty, default } => {
                    let field = NestedField::optional(next_id, name, Type::Primitive(ty.clone()));
                    let field = match default {
                        StatedDefault::None => field,
                        StatedDefault::Literal(literal) => {
                            field.with_initial_default(literal.clone())
                        }
                        StatedDefault::Raw(json) => {
                            self.raw_defaults.push((next_id, json.clone()));
                            field
                        }
                    };
                    fields.push(field.into());
                    next_id += 1;
                }
                SchemaChange::Rename { from, to } => {
                    let at = column_at(&fields, from)?;
                    let mut renamed = NestedField::clone(&fields[at]);
                    renamed.name.clone_from(to);
                    fields[at] = renamed.into();
                }
                SchemaChange::Drop { name } => {
                    fields.remove(column_at(&fields, name)?);
                }
            }
        }
        let schema = Schema::builder().with_fields(fields).build()?;
        self.metadata = self
            .metadata
            .clone()
            .into_builder(Some(self.metadata_location.clone()))
            .add_current_schema(schema)?
            .build()?
            .metadata;
        self.commit_by_hand(HandSnapshot {
            data: self.files.clone(),
            deletes: self.deletes.clone(),
        })
        .await
    }

    /// Records what the current snapshot lists, for `move_ref`.
    fn remember_current(&mut self) {
        let Some(snapshot_id) = self.snapshot_id() else {
            return;
        };
        self.history.insert(
            snapshot_id,
            Listing {
                ids: self.ids.clone(),
                versions: self.versions.clone(),
                rows: self.rows.clone(),
                files: self.files.clone(),
                deletes: self.deletes.clone(),
                added: self.added.clone(),
            },
        );
    }

    /// Writes one manifest of a by-hand commit at `path`, of delete files
    /// when `deletes`, at the table's format version: each of `files` the
    /// current snapshot lists as an existing entry at the commit that added
    /// it, every other one added by `now`.
    async fn write_hand_manifest(
        &self,
        path: &str,
        files: &[DataFile],
        deletes: bool,
        now: AddedBy,
    ) -> Result<ManifestFile> {
        let builder = ManifestWriterBuilder::new(
            self.table.file_io().new_output(path)?,
            Some(now.snapshot_id),
            Arc::clone(self.metadata.current_schema()),
            PartitionSpec::clone(self.metadata.default_partition_spec()),
        );
        let mut writer = match (self.spec.format_version, deletes) {
            (FormatVersion::V3, true) => builder.build_v3_deletes(),
            (FormatVersion::V3, false) => builder.build_v3_data(),
            (FormatVersion::V1 | FormatVersion::V2, true) => builder.build_v2_deletes(),
            (FormatVersion::V1 | FormatVersion::V2, false) => builder.build_v2_data(),
        };
        for file in files {
            match self.added.get(file.file_path()) {
                Some(by) => writer.add_existing_file(
                    file.clone(),
                    by.snapshot_id,
                    by.sequence,
                    Some(by.sequence),
                )?,
                None => writer.add_file(file.clone(), now.sequence)?,
            }
        }
        Ok(writer.write_manifest_file().await?)
    }

    /// Deletes the live row of every one of `ids` and commits the deletes
    /// by hand in `form`, files named after `name`, beside every data file
    /// and every delete file the current snapshot lists except a deletion
    /// vector `form` replaces. The ids leave the fixture's (`ids`); an id
    /// with no live row is `SpecFault::IdNotLive`. An ordered fixture
    /// deletes every live row of each id.
    pub async fn delete(&mut self, ids: &[u64], form: DeleteForm, name: &str) -> Result<()> {
        let targets = self.live_rows_of(ids)?;
        self.delete_targets(&targets, form, name).await
    }

    /// Deletes the live rows at (id, raw order value) of `rows` as `delete`
    /// does, an ordered fixture's rows addressed one by one. A pair with
    /// no live row is `SpecFault::RowNotLive`, and a fixture whose
    /// specification names no order column `SpecFault::NotOrdered`.
    pub async fn delete_rows(
        &mut self,
        rows: &[(u64, i64)],
        form: DeleteForm,
        name: &str,
    ) -> Result<()> {
        let targets = self.ordered_rows(rows)?;
        self.delete_targets(&targets, form, name).await
    }

    /// Rewrites every one of `ids` at its next version into new data files
    /// named after `name`, deletes its live row in `form`, and commits both
    /// by hand in one snapshot, as a merge-on-read `UPDATE` does: the new
    /// rows and the deletes share its sequence number. An id with no live
    /// row is `SpecFault::IdNotLive`. An ordered fixture rewrites every
    /// live row of each id.
    pub async fn update(&mut self, ids: &[u64], form: DeleteForm, name: &str) -> Result<()> {
        let targets = self.live_rows_of(ids)?;
        self.update_targets(&targets, form, name).await
    }

    /// Rewrites the live rows at (id, raw order value) of `rows` as `update`
    /// does, an ordered fixture's rows addressed one by one; the errors are
    /// those of `delete_rows`.
    pub async fn update_rows(
        &mut self,
        rows: &[(u64, i64)],
        form: DeleteForm,
        name: &str,
    ) -> Result<()> {
        let targets = self.ordered_rows(rows)?;
        self.update_targets(&targets, form, name).await
    }

    /// The live rows of `ids`: each id's row in a key-value fixture, every
    /// live row of each id in an ordered one, where an id with none is
    /// `SpecFault::IdNotLive`.
    fn live_rows_of(&self, ids: &[u64]) -> Result<Vec<RowId>> {
        if self.spec.ordered.is_none() {
            return Ok(ids.iter().map(|&id| RowId::keyed(id)).collect());
        }
        let mut rows = Vec::new();
        for &id in ids {
            if !self.rows.holds(id) {
                return Err(SpecFault::IdNotLive { id }.into());
            }
            rows.extend(self.rows.of_id(id).map(|(order, _)| RowId::at(id, order)));
        }
        Ok(rows)
    }

    /// Deletes `targets` (`delete`).
    async fn delete_targets(
        &mut self,
        targets: &[RowId],
        form: DeleteForm,
        name: &str,
    ) -> Result<()> {
        let deletes = self.deletes_for(targets, form, name).await?;
        self.forget(targets);
        self.commit_by_hand(HandSnapshot {
            data: self.files.clone(),
            deletes,
        })
        .await
    }

    /// Rewrites `targets` at their next versions (`update`).
    async fn update_targets(
        &mut self,
        targets: &[RowId],
        form: DeleteForm,
        name: &str,
    ) -> Result<()> {
        let deletes = self.deletes_for(targets, form, name).await?;
        let next: HashMap<RowId, u64> = targets
            .iter()
            .map(|&row| (row, self.version_of_row(row) + 1))
            .collect();
        let written = write_data_files(
            &self.table,
            &self.spec,
            targets.iter().copied(),
            name,
            &|row| next.get(&row).copied().unwrap_or(FIRST_VERSION),
        )
        .await?;
        for (row, version) in next {
            match row.order {
                None => {
                    self.versions.insert(row.id, version);
                }
                Some(order) => self.rows.insert(row.id, order, version),
            }
        }
        let mut data = self.files.clone();
        data.extend(written);
        self.commit_by_hand(HandSnapshot { data, deletes }).await
    }

    /// The rows of `ids` at `version` as new data files named after
    /// `prefix`, grouped by bucket as `append` writes them, not committed:
    /// for a test that lists them in a `HandSnapshot` of its own. The
    /// fixture's ids and versions do not change.
    pub async fn write_rows(
        &self,
        ids: &[u64],
        version: u64,
        prefix: &str,
    ) -> Result<Vec<DataFile>> {
        write_data_files(
            &self.table,
            &self.spec,
            self.spec.rows_of(ids)?,
            prefix,
            &|_| version,
        )
        .await
    }

    /// The live copy of `row` in `oracle`, or the fault that names the row
    /// that has none.
    fn live_copy_of<'a>(&self, oracle: &'a MergeOracle, row: RowId) -> Result<&'a RowCopy> {
        let key = self.key(row.id);
        let Some(order) = row.order else {
            return oracle
                .live_copy(&key)
                .ok_or_else(|| SpecFault::IdNotLive { id: row.id }.into());
        };
        let kind = self.spec.ordered.ok_or(SpecFault::NotOrdered)?.kind;
        kind.encode(order)
            .and_then(|encoded| oracle.live_copy_at(&key, &encoded))
            .ok_or_else(|| SpecFault::RowNotLive { id: row.id, order }.into())
    }

    /// The delete files of the current snapshot with the live rows of
    /// `targets` deleted in `form`, the new ones written under `name`.
    async fn deletes_for(
        &self,
        targets: &[RowId],
        form: DeleteForm,
        name: &str,
    ) -> Result<Vec<DataFile>> {
        let oracle = MergeOracle::read(self).await?;
        let mut by_file: BTreeMap<String, RoaringTreemap> = BTreeMap::new();
        for &row in targets {
            let copy = self.live_copy_of(&oracle, row)?;
            by_file
                .entry(copy.path.clone())
                .or_default()
                .insert(copy.position);
        }
        let mut deletes = self.deletes.clone();
        if form == DeleteForm::Vectors {
            for (path, rows) in &mut by_file {
                rows.extend(oracle.deleted_positions(path));
                deletes.retain(|delete| {
                    delete.file_format() != DataFileFormat::Puffin
                        || delete.referenced_data_file().as_deref() != Some(path.as_str())
                });
            }
        }
        let targets = by_file
            .into_iter()
            .map(|(path, rows)| Ok((self.listed(&path)?.clone(), rows)))
            .collect::<Result<Vec<_>>>()?;
        deletes.extend(self.write_deletes(targets, form, name).await?);
        Ok(deletes)
    }

    /// Writes delete files in `form`, named after `name`, deleting the
    /// positions paired with each data file of `targets`, and returns their
    /// manifest entries uncommitted, for a `HandSnapshot` that may list the
    /// data files too. `Vectors` writes one Puffin file of one vector per
    /// data file, which must hold every row that file's deletes remove, as
    /// it replaces any earlier vector of the file; the position forms write
    /// the files `DeleteForm` names.
    pub async fn write_deletes(
        &self,
        targets: Vec<(DataFile, RoaringTreemap)>,
        form: DeleteForm,
        name: &str,
    ) -> Result<Vec<DataFile>> {
        let spec_id = self.metadata.default_partition_spec_id();
        let file_io = self.table.file_io();
        let scoped = match form {
            DeleteForm::Vectors => {
                let vectors: Vec<Vector> = targets
                    .into_iter()
                    .map(|(file, positions)| Vector {
                        data_file: file.file_path().to_string(),
                        positions,
                        spec_id,
                        partition: file.partition().clone(),
                    })
                    .collect();
                let path = self.delete_path(&format!("{name}.puffin"));
                return write_vectors(file_io, &path, &vectors).await;
            }
            DeleteForm::ScopedPositions => true,
            DeleteForm::Positions => false,
        };
        let mut entries = Vec::new();
        for (at, group) in position_groups(targets, scoped, spec_id).iter().enumerate() {
            let delete_path = self.delete_path(&format!("{name}-{at}.parquet"));
            entries.push(write_position_deletes(file_io, &delete_path, group).await?);
        }
        Ok(entries)
    }

    /// The data file at `path` as the current snapshot lists it; a file it
    /// does not list is `SpecFault::FileNotListed`.
    fn listed(&self, path: &str) -> Result<&DataFile> {
        self.files
            .iter()
            .find(|file| file.file_path() == path)
            .ok_or_else(|| {
                SpecFault::FileNotListed {
                    path: path.to_string(),
                }
                .into()
            })
    }

    /// The location of a delete file `name`, extension included, under the
    /// table's data directory.
    pub fn delete_path(&self, name: &str) -> String {
        format!("{}/{DATA_DIR}/{name}", self.root())
    }

    /// Every delete file the current snapshot lists, as its manifest
    /// records it.
    pub fn deletes(&self) -> &[DataFile] {
        &self.deletes
    }

    /// The table's `FileIO`, which every fixture file is written through.
    pub fn file_io(&self) -> &FileIO {
        self.table.file_io()
    }

    /// Rewrites every id written so far into new data files, ascending,
    /// grouped by bucket as a first build writes them, named after
    /// `prefix`, and commits them by hand as a snapshot that lists them in
    /// place of every data file before (`commit_by_hand`): what a compaction
    /// leaves, the same rows in as few files per bucket as the file size
    /// target allows. The current snapshot's delete files stay listed, as
    /// a rewrite of data files leaves them, now older than every data file.
    pub async fn compact(&mut self, prefix: &str) -> Result<()> {
        let rows: Vec<RowId> = match self.spec.ordered {
            None => self.ids.iter().map(|&id| RowId::keyed(id)).collect(),
            Some(_) => self
                .rows
                .pairs()
                .map(|(id, order)| RowId::at(id, order))
                .collect(),
        };
        let data = write_data_files(&self.table, &self.spec, rows, prefix, &|row| {
            self.version_of_row(row)
        })
        .await?;
        self.commit_by_hand(HandSnapshot {
            data,
            deletes: self.deletes.clone(),
        })
        .await
    }

    /// Writes the current metadata location as the whole content of the
    /// root's version hint through the table's `FileIO`, so a
    /// `SourceConfig::Static` on the root follows this fixture. Called by
    /// `commit_by_hand`, and by a test that follows the root after a catalog
    /// commit; the testkit owns hint writing, core owns the hint's location.
    pub async fn write_hint(&self) -> Result<()> {
        self.table
            .file_io()
            .new_output(hint_location(self.root()))?
            .write(Bytes::from(self.metadata_location.clone()))
            .await?;
        Ok(())
    }
}

/// The keys of a metadata file's JSON that `Fixture::stated_json` walks to
/// a field's default, as the Iceberg spec names them.
const SCHEMAS_KEY: &str = "schemas";
const FIELDS_KEY: &str = "fields";
const FIELD_ID_KEY: &str = "id";
const INITIAL_DEFAULT_KEY: &str = "initial-default";

/// The position of the top-level column `name` among `fields`, or
/// `SpecFault::ColumnNotInSchema`.
fn column_at(fields: &[NestedFieldRef], name: &str) -> Result<usize> {
    fields
        .iter()
        .position(|field| field.name == name)
        .ok_or_else(|| {
            SpecFault::ColumnNotInSchema {
                name: name.to_string(),
            }
            .into()
        })
}

/// The position delete files that delete `targets`' positions under spec
/// `spec_id`: one per data file naming it as its referenced data file when
/// `scoped`, else one per partition naming none.
fn position_groups(
    targets: Vec<(DataFile, RoaringTreemap)>,
    scoped: bool,
    spec_id: i32,
) -> Vec<PositionDeletes> {
    let mut groups: Vec<PositionDeletes> = Vec::new();
    for (file, positions) in targets {
        let path = file.file_path().to_string();
        let referenced = scoped.then(|| path.clone());
        let rows = positions.iter().map(|position| (path.clone(), position));
        match groups
            .iter_mut()
            .find(|group| group.referenced == referenced && group.partition == *file.partition())
        {
            Some(group) => group.rows.extend(rows),
            None => groups.push(PositionDeletes {
                rows: rows.collect(),
                referenced,
                spec_id,
                partition: file.partition().clone(),
            }),
        }
    }
    groups
}

/// The ids of `class` modulo `INTERLEAVE_STRIDE` inside `INTERLEAVED_IDS`:
/// what an `append` on the interleaved fixture adds, one class at a time.
pub fn residue_class(class: u64) -> Vec<u64> {
    INTERLEAVED_IDS
        .filter(|id| id % INTERLEAVE_STRIDE == class)
        .collect()
}

/// `memory_catalog_local` over a fresh temporary directory, which lives as
/// long as the returned `TempDir`: the opening of every in-process test.
pub async fn local_catalog() -> Result<(TempDir, Arc<dyn Catalog>)> {
    let dir = tempfile::tempdir()?;
    let catalog = memory_catalog_local(dir.path()).await?;
    Ok((dir, catalog))
}

/// A memory catalog whose files live under `dir` on local disk, through the
/// library's own filesystem storage, where icetray's `Storage` reads the
/// metadata file it names too: the in-process stand-in for a catalog
/// record.
pub async fn memory_catalog_local(dir: &Path) -> Result<Arc<MemoryCatalog>> {
    std::fs::create_dir_all(dir)?;
    let warehouse = format!("file://{}", dir.display());
    let catalog = MemoryCatalogBuilder::default()
        .with_storage_factory(Arc::new(LocalFsStorageFactory))
        .load(
            MEMORY_CATALOG_NAME,
            HashMap::from([(MEMORY_CATALOG_WAREHOUSE.to_string(), warehouse)]),
        )
        .await?;
    Ok(Arc::new(catalog))
}

/// A memory catalog whose files live under `prefix` in the fixture server's
/// bucket, through the library's S3 storage.
pub async fn memory_catalog_s3(s3: &S3Fixture, prefix: &str) -> Result<Arc<MemoryCatalog>> {
    let mut props = s3.file_io_props();
    props.insert(MEMORY_CATALOG_WAREHOUSE.to_string(), s3.uri(prefix));
    let factory: Arc<dyn StorageFactory> = Arc::new(OpenDalStorageFactory::S3 {
        customized_credential_load: None,
    });
    let catalog = MemoryCatalogBuilder::default()
        .with_storage_factory(factory)
        .load(MEMORY_CATALOG_NAME, props)
        .await?;
    Ok(Arc::new(catalog))
}

/// Creates the table `spec` describes in `namespace` (creating the namespace
/// when absent) and commits `spec.ids` at `spec.id_stride` in `spec.order`
/// as its first snapshot, every row of each id when `spec.ordered` names an
/// order column. A `key_pad` on a long or uuid key is
/// `SpecFault::PadOnFixedWidthKey`, a scattered key of a type other than
/// string `SpecFault::ScatteredNonStringKey`, and a scattered id at or
/// above `SCATTERED_ID_SPACE` `SpecFault::IdOutsideScatteredSpace`.
pub async fn write_fixture(
    catalog: &dyn Catalog,
    namespace: &str,
    spec: FixtureSpec,
) -> Result<Fixture> {
    if spec.key_pad > 0 && spec.key_type.is_fixed_width() {
        return Err(SpecFault::PadOnFixedWidthKey {
            key_type: spec.key_type,
        }
        .into());
    }
    if spec.key_shape == KeyShape::Scattered && spec.key_type != KeyType::String {
        return Err(SpecFault::ScatteredNonStringKey {
            key_type: spec.key_type,
        }
        .into());
    }
    let (table, ident) = create_table(
        catalog,
        namespace,
        &spec.name,
        primitive_of(spec.key_type),
        Columns {
            value: PrimitiveType::Binary,
            second: spec.second_value,
            order: spec.ordered.map(|ordered| ordered.kind.primitive()),
        },
        spec.buckets,
        spec.format_version,
    )
    .await?;
    let ids = spec.written_ids();
    let mut fixture = Fixture::new(table, ident, spec)?;
    fixture.append(catalog, ids, FIRST_BUILD_PREFIX).await?;
    Ok(fixture)
}

/// The columns `create_table` gives a table beside its key: `VALUE_COLUMN`
/// of `value`'s type, `SECOND_VALUE_COLUMN` when `second` is set, and the
/// order column when `order` names its type.
struct Columns {
    /// The Iceberg type of `VALUE_COLUMN`.
    value: PrimitiveType,
    /// Whether the table also holds `SECOND_VALUE_COLUMN`.
    second: bool,
    /// The type of the order column, which follows the value columns.
    order: Option<PrimitiveType>,
}

/// Creates a table whose key and value columns have the given Iceberg types
/// and commits one raw data file, for the tables whose types icetray refuses
/// from the schema alone. The fixture's spec names the key type
/// `KeyType::from_iceberg` gives the key, or `Binary` for a type icetray
/// does not admit; no id is derived for such a table.
pub async fn write_table_of_types(
    catalog: &dyn Catalog,
    namespace: &str,
    name: &str,
    key: PrimitiveType,
    value: PrimitiveType,
    buckets: u32,
    file: RawFile,
) -> Result<Fixture> {
    let columns = Columns {
        value,
        second: false,
        order: None,
    };
    write_raw_table(catalog, namespace, name, key, columns, buckets, file).await
}

/// Creates an unbucketed table of a long `KEY_COLUMN`, a binary
/// `VALUE_COLUMN`, and an order column `ORDER_COLUMN` of type `order`, and
/// commits one raw data file, for the order columns icetray refuses by
/// their type or by the Parquet type a file stores them in (RFD 025). The
/// fixture's spec is a key-value one, so a caller serves it as an ordered
/// table by setting `order_column` on its configuration.
pub async fn write_ordered_table_of_types(
    catalog: &dyn Catalog,
    namespace: &str,
    name: &str,
    order: PrimitiveType,
    file: RawFile,
) -> Result<Fixture> {
    let columns = Columns {
        value: PrimitiveType::Binary,
        second: false,
        order: Some(order),
    };
    write_raw_table(
        catalog,
        namespace,
        name,
        PrimitiveType::Long,
        columns,
        0,
        file,
    )
    .await
}

/// Creates a table of `key` and `columns` under `buckets` and commits the
/// raw data file `file`, as `write_table_of_types` documents.
async fn write_raw_table(
    catalog: &dyn Catalog,
    namespace: &str,
    name: &str,
    key: PrimitiveType,
    columns: Columns,
    buckets: u32,
    file: RawFile,
) -> Result<Fixture> {
    let key_type = KeyType::from_iceberg(&Type::Primitive(key.clone())).unwrap_or(KeyType::Binary);
    let (table, ident) = create_table(
        catalog,
        namespace,
        name,
        key,
        columns,
        buckets,
        FormatVersion::V2,
    )
    .await?;
    let spec = FixtureSpec {
        ids: 0..0,
        buckets,
        ..FixtureSpec::small(name, key_type)
    };
    let mut fixture = Fixture::new(table, ident, spec)?;
    fixture.append_raw(catalog, file).await?;
    Ok(fixture)
}

/// The Iceberg primitive a key type is written as.
fn primitive_of(key_type: KeyType) -> PrimitiveType {
    match key_type {
        KeyType::Long => PrimitiveType::Long,
        KeyType::String => PrimitiveType::String,
        KeyType::Binary => PrimitiveType::Binary,
        KeyType::Uuid => PrimitiveType::Uuid,
    }
}

/// Creates `name` in `namespace`: a required `key` of `key` type (field
/// `KEY_FIELD_ID`), an optional `value` of `values.value` type (field
/// `VALUE_FIELD_ID`, so a null value is a legal row per RFD 002), an
/// optional binary `SECOND_VALUE_COLUMN` when `values.second` is set,
/// `bucket(buckets, key)` when `buckets` is above zero, and a sort order on
/// the key, at format `format_version`. `values.order` adds an optional
/// order column of that type (`ORDER_COLUMN`) after the value columns, the
/// last the catalog numbers (`ORDER_FIELD_ID`), and a second ascending sort
/// field on it. Creates the
/// namespace when absent.
async fn create_table(
    catalog: &dyn Catalog,
    namespace: &str,
    name: &str,
    key: PrimitiveType,
    values: Columns,
    buckets: u32,
    format_version: FormatVersion,
) -> Result<(Table, TableIdent)> {
    let namespace = NamespaceIdent::new(namespace.to_string());
    if !catalog.namespace_exists(&namespace).await? {
        catalog.create_namespace(&namespace, HashMap::new()).await?;
    }
    let mut fields = vec![
        NestedField::required(KEY_FIELD_ID, KEY_COLUMN, Type::Primitive(key)).into(),
        NestedField::optional(VALUE_FIELD_ID, VALUE_COLUMN, Type::Primitive(values.value)).into(),
    ];
    if values.second {
        fields.push(
            NestedField::optional(
                SECOND_VALUE_FIELD_ID,
                SECOND_VALUE_COLUMN,
                Type::Primitive(PrimitiveType::Binary),
            )
            .into(),
        );
    }
    let order_id = if values.second {
        ORDER_FIELD_ID
    } else {
        SECOND_VALUE_FIELD_ID
    };
    let ordered = values.order.is_some();
    if let Some(order) = values.order {
        fields.push(NestedField::optional(order_id, ORDER_COLUMN, Type::Primitive(order)).into());
    }
    let schema = Schema::builder()
        .with_schema_id(SCHEMA_ID)
        .with_fields(fields)
        .build()?;
    let mut partition_spec = UnboundPartitionSpec::builder().with_spec_id(SPEC_ID);
    if buckets > 0 {
        partition_spec = partition_spec.add_partition_field(
            KEY_FIELD_ID,
            KEY_BUCKET_FIELD,
            Transform::Bucket(buckets),
        )?;
    }
    let mut sort_order = SortOrder::builder();
    sort_order.with_order_id(SORT_ORDER_ID);
    for source_id in [KEY_FIELD_ID]
        .into_iter()
        .chain(ordered.then_some(order_id))
    {
        sort_order.with_sort_field(
            SortField::builder()
                .source_id(source_id)
                .transform(Transform::Identity)
                .direction(SortDirection::Ascending)
                .null_order(NullOrder::First)
                .build(),
        );
    }
    let sort_order = sort_order.build_unbound()?;
    let creation = TableCreation::builder()
        .name(name.to_string())
        .schema(schema)
        .partition_spec(partition_spec.build())
        .sort_order(sort_order)
        .format_version(format_version)
        .build();
    let table = catalog.create_table(&namespace, creation).await?;
    Ok((table, TableIdent::new(namespace, name.to_string())))
}

/// The rows of one bucket, in the order they reach the writer.
#[derive(Default)]
struct BucketRows {
    rows: Vec<RowId>,
    keys: Vec<Key>,
}

/// Writes `rows` into `table`'s layout as new data files, not yet committed:
/// keys through `key_at_shaped`, grouped by bucket through the library's own
/// bucket transform, in the given order within a bucket, through a
/// `ClusteredWriter` over the rolling data file writer under
/// `spec.layout`'s properties and file size target, in batches of
/// `spec.layout.page_rows` rows, each row's value at `version_of(row)`. A
/// row of an ordered fixture carries its order value, which sorts the
/// file's sorting columns after the key.
pub(crate) async fn write_data_files(
    table: &Table,
    spec: &FixtureSpec,
    rows: impl IntoIterator<Item = RowId>,
    prefix: &str,
    version_of: &dyn Fn(RowId) -> u64,
) -> Result<Vec<DataFile>> {
    let schema = table.metadata().current_schema().clone();
    let arrow_schema = Arc::new(iceberg::arrow::schema_to_arrow_schema(&schema)?);
    let partition_spec = PartitionSpec::clone(table.metadata().default_partition_spec());
    let by_bucket = group_by_bucket(spec, rows)?;
    let total = by_bucket.values().map(|bucket| bucket.rows.len()).sum();
    let properties = match spec.ordered {
        Some(_) => spec
            .layout
            .ordered_properties(total, order_column_index(&arrow_schema)?),
        None => spec.layout.properties(total),
    };
    let parquet = ParquetWriterBuilder::new(properties, Arc::clone(&schema));
    let rolling = RollingFileWriterBuilder::new(
        parquet,
        spec.layout.target_file_bytes,
        table.file_io().clone(),
        DefaultLocationGenerator::new(table.metadata())?,
        DefaultFileNameGenerator::new(prefix.to_string(), None, DataFileFormat::Parquet),
    );
    let mut writer = ClusteredWriter::new(DataFileWriterBuilder::new(rolling));
    let batch_rows = spec.layout.page_rows.max(1);
    for (bucket, rows) in &by_bucket {
        let partition = if spec.buckets == 0 {
            Struct::empty()
        } else {
            Struct::from_iter([Some(Literal::int(*bucket))])
        };
        let key = PartitionKey::new(partition_spec.clone(), Arc::clone(&schema), partition);
        for (rows, keys) in rows
            .rows
            .chunks(batch_rows)
            .zip(rows.keys.chunks(batch_rows))
        {
            writer
                .write(
                    key.clone(),
                    batch(&arrow_schema, spec, rows, keys, version_of)?,
                )
                .await?;
        }
    }
    Ok(writer.close().await?)
}

/// The position of the order column among `arrow_schema`'s columns, which a
/// file's sorting column names.
fn order_column_index(arrow_schema: &ArrowSchema) -> Result<i32> {
    Ok(i32::try_from(arrow_schema.index_of(ORDER_COLUMN)?).expect("a column position fits i32"))
}

/// `rows` as keys, grouped by the bucket the library's transform gives each,
/// ascending by bucket and in the given order within one; every row under
/// `UNPARTITIONED_BUCKET` for a table without a bucket spec.
fn group_by_bucket(
    spec: &FixtureSpec,
    rows: impl IntoIterator<Item = RowId>,
) -> Result<BTreeMap<i32, BucketRows>> {
    let rows: Vec<RowId> = rows.into_iter().collect();
    let keys: Vec<Key> = rows.iter().map(|row| spec.key(row.id)).collect();
    let buckets: Vec<i32> = if spec.buckets == 0 {
        vec![UNPARTITIONED_BUCKET; rows.len()]
    } else {
        let transform = create_transform_function(&Transform::Bucket(spec.buckets))?;
        let array = transform.transform(key_array(spec.key_type, &keys))?;
        array
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("the bucket transform yields an Int32 array for every admitted key type")
            .values()
            .to_vec()
    };
    let mut by_bucket: BTreeMap<i32, BucketRows> = BTreeMap::new();
    for ((row, key), bucket) in rows.into_iter().zip(keys).zip(buckets) {
        let held = by_bucket.entry(bucket).or_default();
        held.rows.push(row);
        held.keys.push(key);
    }
    Ok(by_bucket)
}

/// One batch of `keys` with their values at `version_of` each row, a null
/// for every id `spec.nulls` names, one column per field of `arrow_schema`
/// cast to its arrow type: `KEY_COLUMN` the keys, `VALUE_COLUMN` the
/// values (`row_value` of a row that has an order value, else
/// `value_of_version`), `SECOND_VALUE_COLUMN` the values reversed,
/// `ORDER_COLUMN` the order values of an ordered fixture's rows, and a
/// column the fixture derives nothing for, which `Fixture::evolve` may add,
/// all nulls.
/// A column `arrow_schema` lacks is not written, so after `evolve` drops
/// or renames the key or value column the batch omits it rather than
/// failing: a file written then lacks that column, as a writer under that
/// schema would write it.
fn batch(
    arrow_schema: &Arc<ArrowSchema>,
    spec: &FixtureSpec,
    rows: &[RowId],
    keys: &[Key],
    version_of: &dyn Fn(RowId) -> u64,
) -> Result<RecordBatch> {
    let key = key_array(spec.key_type, keys);
    let values: BinaryArray = rows
        .iter()
        .zip(keys)
        .map(|(&row, key)| {
            (!spec.is_null(row.id)).then(|| match row.order {
                Some(order) => row_value(
                    key,
                    order,
                    spec.value_bytes,
                    spec.value_kind,
                    version_of(row),
                ),
                None => value_of_version(key, spec.value_bytes, spec.value_kind, version_of(row)),
            })
        })
        .collect();
    let order: Option<ArrayRef> = match (
        spec.ordered,
        rows.iter()
            .map(|row| row.order)
            .collect::<Option<Vec<i64>>>(),
    ) {
        (Some(shape), Some(orders)) => Some(shape.kind.array(&orders)?),
        (Some(_) | None, _) => None,
    };
    let second: ArrayRef = Arc::new(
        values
            .iter()
            .map(|value| value.map(|bytes| reversed(bytes.to_vec())))
            .collect::<BinaryArray>(),
    );
    let value: ArrayRef = Arc::new(values);
    let columns = arrow_schema
        .fields()
        .iter()
        .map(|field| {
            let column = match (field.name().as_str(), &order) {
                (KEY_COLUMN, _) => Arc::clone(&key),
                (VALUE_COLUMN, _) => Arc::clone(&value),
                (SECOND_VALUE_COLUMN, _) => Arc::clone(&second),
                (ORDER_COLUMN, Some(order)) => Arc::clone(order),
                _ => new_null_array(field.data_type(), rows.len()),
            };
            Ok(arrow::compute::cast(&column, field.data_type())?)
        })
        .collect::<Result<Vec<ArrayRef>>>()?;
    Ok(RecordBatch::try_new(Arc::clone(arrow_schema), columns)?)
}

/// `bytes` in reverse order: a row's `SECOND_VALUE_COLUMN` from its `value`.
fn reversed(mut bytes: Vec<u8>) -> Vec<u8> {
    bytes.reverse();
    bytes
}

/// What `column` holds for a row whose `VALUE_COLUMN` holds `value`: the
/// value itself, or in `SECOND_VALUE_COLUMN` the value reversed.
///
/// Panics on a column the fixture writes no value for.
fn column_value(column: &str, value: Option<Vec<u8>>) -> Option<Vec<u8>> {
    match column {
        VALUE_COLUMN => value,
        SECOND_VALUE_COLUMN => value.map(reversed),
        other => panic!("the fixture writes no column {other}"),
    }
}

/// The message of the impossible case: `key_at_shaped` builds every key
/// `key_array` sees, in the type it is given.
const MIXED_KEYS: &str = "key_at_shaped builds keys of the type the fixture names";

/// `keys`, all of `key_type`, as the key column's array: `Int64`, `Utf8`,
/// `Binary`, or `FixedSizeBinary(16)`, what the library's schema conversion
/// gives a uuid column. An empty slice is an empty array of that type.
pub(crate) fn key_array(key_type: KeyType, keys: &[Key]) -> ArrayRef {
    match key_type {
        KeyType::Long => Arc::new(Int64Array::from(
            keys.iter()
                .map(|key| match key {
                    Key::Long(value) => *value,
                    Key::Str(_) | Key::Bytes(_) | Key::Uuid(_) => unreachable!("{MIXED_KEYS}"),
                })
                .collect::<Vec<i64>>(),
        )),
        KeyType::String => Arc::new(StringArray::from(
            keys.iter()
                .map(|key| match key {
                    Key::Str(text) => text.as_str(),
                    Key::Long(_) | Key::Bytes(_) | Key::Uuid(_) => unreachable!("{MIXED_KEYS}"),
                })
                .collect::<Vec<&str>>(),
        )),
        KeyType::Binary => Arc::new(BinaryArray::from(
            keys.iter()
                .map(|key| match key {
                    Key::Bytes(bytes) => bytes.as_slice(),
                    Key::Long(_) | Key::Str(_) | Key::Uuid(_) => unreachable!("{MIXED_KEYS}"),
                })
                .collect::<Vec<&[u8]>>(),
        )),
        KeyType::Uuid => {
            let mut bytes = Vec::with_capacity(keys.len() * UUID_BYTES);
            for key in keys {
                match key {
                    Key::Uuid(uuid) => bytes.extend_from_slice(uuid),
                    Key::Long(_) | Key::Str(_) | Key::Bytes(_) => unreachable!("{MIXED_KEYS}"),
                }
            }
            Arc::new(FixedSizeBinaryArray::new(
                i32::try_from(UUID_BYTES).expect("sixteen fits i32"),
                Buffer::from(bytes),
                None,
            ))
        }
    }
}

/// A key as the datum a manifest bound carries.
fn datum(key: &Key) -> Datum {
    match key {
        Key::Long(value) => Datum::long(*value),
        Key::Str(text) => Datum::string(text),
        Key::Bytes(bytes) => Datum::binary(bytes.clone()),
        Key::Uuid(bytes) => Datum::uuid(uuid::Uuid::from_bytes(*bytes)),
    }
}

/// A data file entry at `path` in `format` under `spec_id`, with
/// `(records, size)` rows and bytes, and key bounds when given.
fn data_file(
    path: &str,
    spec_id: i32,
    partition: Struct,
    (records, size): (u64, u64),
    bounds: Option<&(Key, Key)>,
    format: DataFileFormat,
) -> Result<DataFile> {
    let mut builder = DataFileBuilder::default();
    builder
        .content(DataContentType::Data)
        .file_path(path.to_string())
        .file_format(format)
        .partition(partition)
        .partition_spec_id(spec_id)
        .record_count(records)
        .file_size_in_bytes(size);
    if let Some((min, max)) = bounds {
        builder
            .lower_bounds(HashMap::from([(KEY_FIELD_ID, datum(min))]))
            .upper_bounds(HashMap::from([(KEY_FIELD_ID, datum(max))]));
    }
    builder.build().map_err(|error| {
        iceberg::Error::new(
            ErrorKind::DataInvalid,
            format!("data file entry incomplete: {error}"),
        )
        .into()
    })
}

/// A data file entry at `path` in `format` under `spec_id` with `partition`
/// as its tuple and no key bounds, for `HandSnapshot::data`, with nothing
/// written there: for a format icetray refuses from the manifest alone,
/// ORC, Avro, or Puffin.
pub fn data_file_entry(
    path: &str,
    spec_id: i32,
    partition: Struct,
    records: u64,
    size: u64,
    format: DataFileFormat,
) -> Result<DataFile> {
    data_file(path, spec_id, partition, (records, size), None, format)
}

/// A position-delete file entry at `path` in `format` under `spec_id` with
/// `partition` as its tuple, naming `referenced` as its referenced data
/// file when given, for `HandSnapshot::deletes`, with nothing written
/// there: for a format icetray refuses from the manifest alone, Avro or
/// ORC. A Parquet one is read, and `delete_files` writes it.
pub fn position_delete_file(
    path: &str,
    spec_id: i32,
    partition: Struct,
    records: u64,
    size: u64,
    format: DataFileFormat,
    referenced: Option<String>,
) -> Result<DataFile> {
    delete_file(
        path,
        spec_id,
        partition,
        (records, size),
        (DataContentType::PositionDeletes, referenced),
        format,
    )
}

/// An equality-delete file entry on the key column at `path` under
/// `spec_id` with `partition` as its tuple, for `HandSnapshot::deletes`;
/// the file need not exist, as for `position_delete_file`.
pub fn equality_delete_file(
    path: &str,
    spec_id: i32,
    partition: Struct,
    records: u64,
    size: u64,
) -> Result<DataFile> {
    delete_file(
        path,
        spec_id,
        partition,
        (records, size),
        (DataContentType::EqualityDeletes, None),
        DataFileFormat::Parquet,
    )
}

/// A delete file entry of `content` in `format` with `(records, size)`,
/// naming `referenced` as its referenced data file when given; an
/// equality delete is on the key column.
fn delete_file(
    path: &str,
    spec_id: i32,
    partition: Struct,
    (records, size): (u64, u64),
    (content, referenced): (DataContentType, Option<String>),
    format: DataFileFormat,
) -> Result<DataFile> {
    let mut builder = DataFileBuilder::default();
    builder
        .content(content)
        .referenced_data_file(referenced)
        .file_path(path.to_string())
        .file_format(format)
        .partition(partition)
        .partition_spec_id(spec_id)
        .record_count(records)
        .file_size_in_bytes(size);
    if content == DataContentType::EqualityDeletes {
        builder.equality_ids(Some(vec![KEY_FIELD_ID]));
    }
    builder.build().map_err(|error| {
        iceberg::Error::new(
            ErrorKind::DataInvalid,
            format!("delete file entry incomplete: {error}"),
        )
        .into()
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::TestkitError;

    /// A column no fixture schema names.
    const OTHER_COLUMN: &str = "other";

    /// The name a rename would give it.
    const RENAMED: &str = "payload";

    #[tokio::test]
    async fn test_evolve_refuses_to_rename_or_drop_a_column_the_schema_lacks() -> Result<()> {
        let (_dir, catalog) = local_catalog().await?;
        let spec = FixtureSpec::small("kv_evolve_absent", KeyType::Long);
        let mut fixture = write_fixture(catalog.as_ref(), NAMESPACE, spec).await?;
        let version = fixture.metadata_location().to_string();
        for change in [
            SchemaChange::Rename {
                from: OTHER_COLUMN.to_string(),
                to: RENAMED.to_string(),
            },
            SchemaChange::Drop {
                name: OTHER_COLUMN.to_string(),
            },
        ] {
            let refused = fixture.evolve(&[change]).await;
            assert!(
                matches!(
                    &refused,
                    Err(TestkitError::Spec(SpecFault::ColumnNotInSchema { name })) if name == OTHER_COLUMN
                ),
                "{refused:?}"
            );
        }
        assert_eq!(
            fixture.metadata_location(),
            version,
            "nothing was committed"
        );
        Ok(())
    }
}
