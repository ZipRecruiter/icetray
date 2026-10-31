//! One fixture per `RefusalReason` variant (RFD 007, RFD 017): each fails
//! its first load with the named reason and leaves the table with no
//! generation, `Degraded` with nothing serving.
//! The variant set itself is bound in core: the label and message-detail
//! matches in `error.rs` name every variant, so a new one fails to compile
//! there; one fixture per variant here is by convention, as for findings.

use std::ops::Range;
use std::sync::Arc;

use arrow::array::{
    ArrayRef, BinaryArray, Float64Array, Int32Array, Int64Array, RecordBatch, StringArray,
    TimestampMillisecondArray,
};
use arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
use iceberg::Catalog;
use iceberg::spec::{DataFile, DataFileFormat, PrimitiveType, Struct};
use icetray_core::ColumnRead;
use icetray_core::approx::count;
use icetray_core::config::{
    Config, ConfigError, ConfigViolation, Residency, ServerConfig, TableConfig, ValueColumnConfig,
    ValueFormat,
};
use icetray_core::contract::MAX_KEY_BYTES;
use icetray_core::open::open_with;
use icetray_core::{
    Budget, Capping, ColumnMismatch, DEFAULT_MAX_SCAN_ROWS, Direction, Error, ErrorKind,
    FindingCode, FindingLog, Key, KeyType, Level, OrderType, OrderValue, RefusalReason, Row,
    ScanBounds, ScanRow, Storage, TableSource, TableState, UnsupportedDelete, UnsupportedFormat,
    bucket,
};
use icetray_testkit::fixture::{
    DATA_DIR, IDS_2K, IDS_4K, NAMESPACE, ONE_BUCKET, OVERLAPPING_APPEND, SAMPLE_4K, SECOND_CLASS,
    STRAY_IDS, SchemaChange, StatedDefault, TINY_BUDGET, TWO_BUCKETS, data_file_entry,
    equality_delete_file, local_catalog, position_delete_file, residue_class,
    write_ordered_table_of_types, write_table_of_types,
};
use icetray_testkit::layout::{KEY_COLUMN, VALUE_COLUMN};
use icetray_testkit::verify::{self, LOAD_METADATA_GETS, METADATA_FILE_GETS};
use icetray_testkit::{
    Bloom, DeleteForm, Dictionary, Fixture, FixtureSpec, HandSnapshot, Layout, MergeOracle,
    ORDER_COLUMN, Order, OrderKind, Ordered, RawFile, Result, RowsPerKey, TestkitError, value_for,
    write_fixture,
};

/// The long keys of a raw file, ascending, and the same values as their
/// arrow arrays for a raw file whose key column is of another type.
const RAW_KEYS: [i64; 3] = [0, 3, 6];
const RAW_STRING_KEYS: [&str; 3] = ["a", "b", "c"];
const RAW_INT_KEYS: [i32; 3] = [0, 3, 6];
/// The order values of a raw file's three rows, one per key, as longs, as
/// ints, and as doubles for an order column of a type icetray refuses.
const RAW_ORDERS: [i64; 3] = [10, 20, 30];
const RAW_INT_ORDERS: [i32; 3] = [10, 20, 30];
const RAW_DOUBLE_ORDERS: [f64; 3] = [0.5, 1.5, 2.5];

/// The position of the null in the null-key raw file.
const NULL_KEY_AT: usize = 1;

/// The name of every raw file under its table's data directory.
const RAW_FILE: &str = "raw.parquet";

/// Records and bytes a manifest records for a delete or data file entry
/// with nothing written at its path.
const UNWRITTEN_RECORDS: u64 = 1;
const UNWRITTEN_BYTES: u64 = 1;

/// A key pad that alone exceeds the cap, so every key is over it.
const OVER_CAP_PAD: usize = MAX_KEY_BYTES;

/// Rows of the over-cap fixture: few, each a few kibibytes, in one bucket.
const OVER_CAP_IDS: Range<u64> = 0..300;

/// A table whose first load was refused: the reason, and the storage left
/// behind for the GET assertions.
struct Refused {
    reason: RefusalReason,
    storage: Arc<Storage>,
}

/// Builds `config` from `source` over hedge-free storage and no local
/// directory and loads it, expecting a refusal; asserts the table has no
/// generation and is `Degraded` with nothing serving and the refusal as its
/// reason.
async fn load_refused(config: TableConfig, source: TableSource) -> Result<Refused> {
    let served = verify::build(
        Arc::new(Storage::new(verify::storage_config_for_tests())),
        config,
        source,
        None,
        verify::TEST_CACHE_BYTES,
    )
    .await?;
    let error = served
        .table
        .load()
        .await
        .expect_err("the fixture is designed to be refused");
    let Error::Refused { reason, .. } = error else {
        panic!("expected a refusal, got {error:?}");
    };
    assert!(
        served.table.current().is_none(),
        "no generation after a refused first load"
    );
    assert!(
        matches!(
            served.table.state(),
            TableState::Degraded {
                serving: None,
                reason: ErrorKind::Refused(ref degraded),
                ..
            } if *degraded == reason
        ),
        "{:?}",
        served.table.state()
    );
    Ok(Refused {
        reason,
        storage: served.storage,
    })
}

/// `load_refused` of `fixture` through its catalog under `config`.
async fn refused_through_catalog(
    fixture: &Fixture,
    catalog: &Arc<dyn Catalog>,
    config: TableConfig,
) -> Result<Refused> {
    load_refused(config, fixture.catalog_source(Arc::clone(catalog))).await
}

/// A raw file of three rows whose key column is `key` and whose value
/// column holds `values`, under the contract layout with no manifest
/// bounds.
fn raw_file(key: ArrayRef, values: &[Vec<u8>]) -> Result<RawFile> {
    let schema = Arc::new(ArrowSchema::new(vec![
        Field::new(KEY_COLUMN, key.data_type().clone(), key.is_nullable()),
        Field::new(VALUE_COLUMN, DataType::Binary, true),
    ]));
    let value: ArrayRef = Arc::new(BinaryArray::from_iter_values(values.iter()));
    let batch = RecordBatch::try_new(schema, vec![key, value])?;
    RawFile::from_batch(RAW_FILE, &batch, &Layout::contract(), Struct::empty(), None)
}

/// The values of `RAW_KEYS`.
fn raw_values() -> Vec<Vec<u8>> {
    RAW_KEYS
        .iter()
        .map(|&key| value_for(&Key::Long(key)))
        .collect()
}

#[tokio::test]
async fn test_unsorted_file_is_refused() -> Result<()> {
    let (_dir, catalog) = local_catalog().await?;
    let spec = FixtureSpec {
        ids: IDS_4K,
        order: Order::Descending,
        ..FixtureSpec::small("kv_unsorted", KeyType::Long)
    };
    let fixture = write_fixture(catalog.as_ref(), NAMESPACE, spec).await?;
    let refused =
        refused_through_catalog(&fixture, &catalog, fixture.config(Residency::Full, false)).await?;
    let RefusalReason::Unsorted { path, .. } = &refused.reason else {
        panic!("{:?}", refused.reason);
    };
    assert!(
        fixture.files().iter().any(|file| file.file_path() == path),
        "{path} is one of the fixture's files"
    );
    Ok(())
}

#[tokio::test]
async fn test_null_key_is_refused() -> Result<()> {
    let (_dir, catalog) = local_catalog().await?;
    let keys: Vec<Option<i64>> = RAW_KEYS
        .iter()
        .enumerate()
        .map(|(at, &key)| (at != NULL_KEY_AT).then_some(key))
        .collect();
    let raw = raw_file(Arc::new(Int64Array::from(keys)), &raw_values())?;
    let fixture = write_table_of_types(
        catalog.as_ref(),
        NAMESPACE,
        "kv_nullkey",
        PrimitiveType::Long,
        PrimitiveType::Binary,
        0,
        raw,
    )
    .await?;
    let refused =
        refused_through_catalog(&fixture, &catalog, fixture.config(Residency::Full, false)).await?;
    assert!(
        matches!(refused.reason, RefusalReason::NullKeys { .. }),
        "{:?}",
        refused.reason
    );
    Ok(())
}

#[tokio::test]
async fn test_duplicate_keys_across_files_are_refused() -> Result<()> {
    let (_dir, catalog) = local_catalog().await?;
    let spec = FixtureSpec {
        ids: IDS_4K,
        ..FixtureSpec::small("kv_dupfiles", KeyType::Long)
    };
    let mut fixture = write_fixture(catalog.as_ref(), NAMESPACE, spec).await?;
    fixture
        .append(catalog.as_ref(), OVERLAPPING_APPEND, "build2")
        .await?;
    let refused =
        refused_through_catalog(&fixture, &catalog, fixture.config(Residency::Full, false)).await?;
    assert!(
        matches!(refused.reason, RefusalReason::DuplicateKeys { .. }),
        "{:?}",
        refused.reason
    );
    Ok(())
}

#[tokio::test]
async fn test_key_physical_type_mismatch_is_refused() -> Result<()> {
    let (_dir, catalog) = local_catalog().await?;
    let raw = raw_file(
        Arc::new(StringArray::from(RAW_STRING_KEYS.to_vec())),
        &raw_values(),
    )?;
    let fixture = write_table_of_types(
        catalog.as_ref(),
        NAMESPACE,
        "kv_mismatch",
        PrimitiveType::Long,
        PrimitiveType::Binary,
        0,
        raw,
    )
    .await?;
    let refused =
        refused_through_catalog(&fixture, &catalog, fixture.config(Residency::Full, false)).await?;
    assert!(
        matches!(
            refused.reason,
            RefusalReason::KeyTypeMismatch {
                expected: KeyType::Long,
                got: parquet::basic::Type::BYTE_ARRAY,
                ..
            }
        ),
        "{:?}",
        refused.reason
    );
    Ok(())
}

/// The schema refuses before any file is read; the raw file exists only so
/// the branch has a snapshot.
#[tokio::test]
async fn test_unsupported_key_type_is_refused() -> Result<()> {
    let (_dir, catalog) = local_catalog().await?;
    let raw = raw_file(
        Arc::new(Int32Array::from(RAW_INT_KEYS.to_vec())),
        &raw_values(),
    )?;
    let fixture = write_table_of_types(
        catalog.as_ref(),
        NAMESPACE,
        "kv_intkey",
        PrimitiveType::Int,
        PrimitiveType::Binary,
        0,
        raw,
    )
    .await?;
    let refused =
        refused_through_catalog(&fixture, &catalog, fixture.config(Residency::Full, false)).await?;
    assert_eq!(refused.reason, RefusalReason::KeyTypeUnsupported);
    assert_eq!(
        refused.storage.stats().gets(),
        METADATA_FILE_GETS,
        "refused from the metadata file alone"
    );
    Ok(())
}

#[tokio::test]
async fn test_unsupported_value_type_is_refused() -> Result<()> {
    let (_dir, catalog) = local_catalog().await?;
    let key: ArrayRef = Arc::new(Int64Array::from(RAW_KEYS.to_vec()));
    let schema = Arc::new(ArrowSchema::new(vec![
        Field::new(KEY_COLUMN, DataType::Int64, false),
        Field::new(VALUE_COLUMN, DataType::Int64, true),
    ]));
    let value: ArrayRef = Arc::new(Int64Array::from(RAW_KEYS.to_vec()));
    let batch = RecordBatch::try_new(schema, vec![key, value])?;
    let raw = RawFile::from_batch(RAW_FILE, &batch, &Layout::contract(), Struct::empty(), None)?;
    let fixture = write_table_of_types(
        catalog.as_ref(),
        NAMESPACE,
        "kv_longvalue",
        PrimitiveType::Long,
        PrimitiveType::Long,
        0,
        raw,
    )
    .await?;
    let refused =
        refused_through_catalog(&fixture, &catalog, fixture.config(Residency::Full, false)).await?;
    assert!(
        matches!(refused.reason, RefusalReason::ValueTypeUnsupported { .. }),
        "{:?}",
        refused.reason
    );
    assert_eq!(
        refused.storage.stats().gets(),
        METADATA_FILE_GETS,
        "refused from the metadata file alone"
    );
    Ok(())
}

/// A `string` value column declared protobuf is refused from the metadata
/// file alone, naming the column and the format (RFD 024); declared opaque,
/// the same table loads, and a reload declaring it protobuf is refused
/// against the serving layout and changes nothing.
#[tokio::test]
async fn test_protobuf_on_a_string_value_column_is_refused() -> Result<()> {
    let (_dir, catalog) = local_catalog().await?;
    let key: ArrayRef = Arc::new(Int64Array::from(RAW_KEYS.to_vec()));
    let schema = Arc::new(ArrowSchema::new(vec![
        Field::new(KEY_COLUMN, DataType::Int64, false),
        Field::new(VALUE_COLUMN, DataType::Utf8, true),
    ]));
    let value: ArrayRef = Arc::new(StringArray::from(RAW_STRING_KEYS.to_vec()));
    let batch = RecordBatch::try_new(schema, vec![key, value])?;
    let raw = RawFile::from_batch(RAW_FILE, &batch, &Layout::contract(), Struct::empty(), None)?;
    let fixture = write_table_of_types(
        catalog.as_ref(),
        NAMESPACE,
        "kv_stringvalue",
        PrimitiveType::Long,
        PrimitiveType::String,
        0,
        raw,
    )
    .await?;
    let protobuf = TableConfig {
        value_columns: vec![ValueColumnConfig::of(VALUE_COLUMN, ValueFormat::Protobuf)],
        ..fixture.config(Residency::Full, false)
    };
    let refused = refused_through_catalog(&fixture, &catalog, protobuf).await?;
    assert_eq!(
        refused.reason,
        RefusalReason::ValueTypeUnsupported {
            column: VALUE_COLUMN.to_string(),
            got: PrimitiveType::String.to_string(),
            format: ValueFormat::Protobuf,
        }
    );
    assert_eq!(
        refused.storage.stats().gets(),
        METADATA_FILE_GETS,
        "refused from the metadata file alone"
    );
    let opaque = fixture.config(Residency::Full, false);
    let config = Config {
        server: ServerConfig::default(),
        storage: verify::storage_config_for_tests(),
        tables: vec![opaque],
    };
    let storage = Arc::new(Storage::new(config.storage.clone()));
    let opened = open_with(&config, None, storage, FindingLog::Events).await?;
    opened.tables()[0].load().await?;
    let serving = opened.tables()[0].current();
    let retune = TableConfig {
        value_columns: vec![ValueColumnConfig::of(VALUE_COLUMN, ValueFormat::Protobuf)],
        ..config.tables[0].clone()
    };
    let Err(refusal) = opened.apply(&[retune]).await else {
        panic!("a retune to protobuf on a string column is refused");
    };
    let [(table, error)] = &refusal.failures[..] else {
        panic!("one failure, got {:?}", refusal.failures);
    };
    assert_eq!(table, &config.tables[0].name);
    assert!(
        matches!(
            error,
            Error::Refused {
                reason: RefusalReason::ValueTypeUnsupported {
                    format: ValueFormat::Protobuf,
                    ..
                },
                ..
            }
        ),
        "{error:?}"
    );
    assert_eq!(
        opened.tables()[0].config().format_of(VALUE_COLUMN),
        Some(ValueFormat::Opaque)
    );
    assert!(
        opened.tables()[0]
            .current()
            .zip(serving)
            .is_some_and(|(now, before)| Arc::ptr_eq(&now, &before)),
        "the serving generation is untouched"
    );
    Ok(())
}

/// An unpartitioned `IDS_2K` fixture, as the delete fixtures start from.
async fn unpartitioned_fixture(catalog: &Arc<dyn Catalog>, name: &str) -> Result<Fixture> {
    let spec = FixtureSpec {
        ids: IDS_2K,
        buckets: 0,
        ..FixtureSpec::small(name, KeyType::Long)
    };
    write_fixture(catalog.as_ref(), NAMESPACE, spec).await
}

/// Commits the fixture's data files by hand beside `deletes`; each data
/// file is carried at the sequence number it was added at, and every
/// delete is added by the new commit, so it is newer than every data file.
async fn commit_deletes(
    fixture: &mut Fixture,
    deletes: Vec<iceberg::spec::DataFile>,
) -> Result<()> {
    fixture
        .commit_by_hand(HandSnapshot {
            data: fixture.files().to_vec(),
            deletes,
        })
        .await
}

/// `load_refused` of `fixture` followed through its root.
async fn refused_through_root(fixture: &Fixture) -> Result<Refused> {
    let source = fixture.root_source();
    load_refused(
        TableConfig {
            source: source.clone(),
            ..fixture.config(Residency::Full, false)
        },
        TableSource::from_config(&source).await?,
    )
    .await
}

/// The location of file `name` under `fixture`'s data directory.
fn data_dir_path(fixture: &Fixture, name: &str) -> String {
    format!("{}/{DATA_DIR}/{name}", fixture.root())
}

/// A data file whose manifest entry records ORC or Avro is refused from
/// the manifests alone: neither it nor any Parquet file's footer is read,
/// so the entry's file need not exist.
#[tokio::test]
async fn test_data_file_in_orc_or_avro_is_refused_unread() -> Result<()> {
    let (_dir, catalog) = local_catalog().await?;
    let cases = [
        (
            "kv_orc_data",
            "rows.orc",
            DataFileFormat::Orc,
            UnsupportedFormat::Orc,
        ),
        (
            "kv_avro_data",
            "rows.avro",
            DataFileFormat::Avro,
            UnsupportedFormat::Avro,
        ),
    ];
    for (name, file, recorded, format) in cases {
        let mut fixture = unpartitioned_fixture(&catalog, name).await?;
        let path = data_dir_path(&fixture, file);
        let mut data = fixture.files().to_vec();
        data.push(data_file_entry(
            &path,
            fixture.metadata().default_partition_spec_id(),
            Struct::empty(),
            UNWRITTEN_RECORDS,
            UNWRITTEN_BYTES,
            recorded,
        )?);
        fixture
            .commit_by_hand(HandSnapshot {
                data,
                deletes: Vec::new(),
            })
            .await?;
        let refused = refused_through_root(&fixture).await?;
        assert_eq!(
            refused.reason,
            RefusalReason::DataFormatUnsupported { path, format }
        );
        assert_eq!(
            refused.storage.stats().gets(),
            verify::HINT_POLL_GETS + METADATA_FILE_GETS + LOAD_METADATA_GETS,
            "{format}: refused from the manifest list and its one manifest"
        );
    }
    Ok(())
}

/// A position delete file in Avro is refused from its manifest entry, before
/// any data file's footer is read.
#[tokio::test]
async fn test_avro_position_delete_is_refused_as_an_unsupported_kind() -> Result<()> {
    let (_dir, catalog) = local_catalog().await?;
    let mut fixture = unpartitioned_fixture(&catalog, "kv_avro_deletes").await?;
    let path = data_dir_path(&fixture, "deletes.avro");
    let delete = position_delete_file(
        &path,
        fixture.metadata().default_partition_spec_id(),
        Struct::empty(),
        UNWRITTEN_RECORDS,
        UNWRITTEN_BYTES,
        DataFileFormat::Avro,
        None,
    )?;
    commit_deletes(&mut fixture, vec![delete]).await?;
    let refused = refused_through_root(&fixture).await?;
    assert_eq!(
        refused.reason,
        RefusalReason::DeleteKindUnsupported {
            path,
            kind: UnsupportedDelete::AvroPositions,
        }
    );
    assert_eq!(
        refused.storage.stats().gets(),
        verify::HINT_POLL_GETS + verify::METADATA_FILE_GETS + DELETE_SNAPSHOT_MANIFEST_GETS,
        "refused from the manifests alone"
    );
    Ok(())
}

/// A deletion vector that applies to a data file masks the file's position
/// deletes, so an Avro one naming a masked file, and one naming no file in
/// a partition whose every file is masked, apply to nothing: the table
/// serves, each key as the vectors leave it.
#[tokio::test]
async fn test_avro_position_deletes_a_vector_masks_are_ignored() -> Result<()> {
    let (_dir, catalog) = local_catalog().await?;
    let mut fixture = unpartitioned_fixture(&catalog, "kv_masked_avro").await?;
    let oracle = MergeOracle::read(&fixture).await?;
    let one_per_file: Vec<u64> = fixture
        .files()
        .iter()
        .map(|file| {
            fixture
                .ids()
                .find(|&id| oracle.copies(&fixture.key(id))[0].path == file.file_path())
                .expect("each file holds a key")
        })
        .collect();
    fixture
        .delete(&one_per_file, DeleteForm::Vectors, "vectors")
        .await?;
    let spec_id = fixture.metadata().default_partition_spec_id();
    let mut deletes = fixture.deletes().to_vec();
    assert_eq!(
        deletes.len(),
        fixture.files().len(),
        "a vector masks every file"
    );
    let named = Some(fixture.files()[0].file_path().to_string());
    for (name, referenced) in [("scoped.avro", named), ("unscoped.avro", None)] {
        deletes.push(position_delete_file(
            &data_dir_path(&fixture, name),
            spec_id,
            Struct::empty(),
            UNWRITTEN_RECORDS,
            UNWRITTEN_BYTES,
            DataFileFormat::Avro,
            referenced,
        )?);
    }
    commit_deletes(&mut fixture, deletes).await?;
    let source = fixture.root_source();
    let served = verify::serve(
        TableConfig {
            source: source.clone(),
            ..fixture.config(Residency::Full, false)
        },
        TableSource::from_config(&source).await?,
        verify::storage_config_for_tests(),
    )
    .await?;
    assert_eq!(
        Some(served.current().snapshot_id.0),
        fixture.snapshot_id(),
        "the snapshot with the Avro deletes serves"
    );
    for id in fixture.ids() {
        let key = fixture.key(id);
        assert_eq!(
            served
                .table
                .get(&key, &[ColumnRead::whole(VALUE_COLUMN)])
                .await?
                .row,
            fixture.expected(&key)
        );
    }
    for id in one_per_file {
        let key = fixture.key(id);
        assert_eq!(
            served
                .table
                .get(&key, &[ColumnRead::whole(VALUE_COLUMN)])
                .await?
                .row,
            Row::Absent,
            "id {id}"
        );
    }
    Ok(())
}

/// The GETs the manifests of a by-hand snapshot with deletes cost: the
/// manifest list, the data manifest, and the delete manifest.
const DELETE_SNAPSHOT_MANIFEST_GETS: u64 = 3;

/// An equality delete newer than the data files it may delete from is
/// refused from its manifest entry; after a compaction rewrites the data
/// at a later sequence number and leaves the delete behind, it applies to
/// nothing and the snapshot serves every key.
#[tokio::test]
async fn test_equality_delete_is_refused_while_live_and_served_once_dangling() -> Result<()> {
    let (_dir, catalog) = local_catalog().await?;
    let mut fixture = unpartitioned_fixture(&catalog, "kv_equality").await?;
    let path = data_dir_path(&fixture, "equality.parquet");
    let delete = equality_delete_file(
        &path,
        fixture.metadata().default_partition_spec_id(),
        Struct::empty(),
        UNWRITTEN_RECORDS,
        UNWRITTEN_BYTES,
    )?;
    commit_deletes(&mut fixture, vec![delete]).await?;
    let refused = refused_through_root(&fixture).await?;
    assert_eq!(
        refused.reason,
        RefusalReason::EqualityDeletes { path: path.clone() }
    );
    assert_eq!(
        refused.storage.stats().gets(),
        verify::HINT_POLL_GETS + verify::METADATA_FILE_GETS + DELETE_SNAPSHOT_MANIFEST_GETS,
        "refused from the manifests alone"
    );

    fixture.compact("compacted").await?;
    let source = fixture.root_source();
    let served = verify::serve(
        TableConfig {
            source: source.clone(),
            ..fixture.config(Residency::Full, false)
        },
        TableSource::from_config(&source).await?,
        verify::storage_config_for_tests(),
    )
    .await?;
    let generation = served.current();
    assert_eq!(
        generation.snapshot_id.0,
        fixture.metadata().current_snapshot().unwrap().snapshot_id()
    );
    let mut served_paths: Vec<&str> = generation
        .files
        .iter()
        .map(|file| file.entry.path.as_str())
        .collect();
    let mut compacted: Vec<&str> = fixture.files().iter().map(DataFile::file_path).collect();
    served_paths.sort_unstable();
    compacted.sort_unstable();
    assert_eq!(served_paths, compacted, "the compacted files alone serve");
    for id in SAMPLE_4K.into_iter().filter(|id| IDS_2K.contains(id)) {
        let key = fixture.key(id);
        assert_eq!(
            served
                .table
                .get(&key, &[ColumnRead::whole(VALUE_COLUMN)])
                .await?
                .row,
            fixture.expected(&key)
        );
    }
    Ok(())
}

#[tokio::test]
async fn test_key_over_the_cap_is_refused() -> Result<()> {
    let (_dir, catalog) = local_catalog().await?;
    let spec = FixtureSpec {
        key_pad: OVER_CAP_PAD,
        ids: OVER_CAP_IDS,
        buckets: ONE_BUCKET,
        ..FixtureSpec::small("kv_hugekeys", KeyType::String)
    };
    let fixture = write_fixture(catalog.as_ref(), NAMESPACE, spec).await?;
    let refused =
        refused_through_catalog(&fixture, &catalog, fixture.config(Residency::Full, false)).await?;
    let RefusalReason::KeyTooLong { bytes, .. } = refused.reason else {
        panic!("{:?}", refused.reason);
    };
    assert!(bytes > MAX_KEY_BYTES, "{bytes} bytes");
    Ok(())
}

/// A raw file whose keys all hash to bucket 0, recorded under bucket 1:
/// its first key is a fence, so the load refuses it rather than answer its
/// keys `Absent`.
#[tokio::test]
async fn test_file_recorded_under_another_bucket_is_refused() -> Result<()> {
    let (_dir, catalog) = local_catalog().await?;
    let spec = FixtureSpec {
        ids: IDS_2K,
        buckets: TWO_BUCKETS,
        ..FixtureSpec::small("kv_misbucketed", KeyType::Long)
    };
    let mut fixture = write_fixture(catalog.as_ref(), NAMESPACE, spec).await?;
    let (hashed, recorded) = (0, 1);
    let ids: Vec<u64> = STRAY_IDS
        .filter(|&id| bucket(&fixture.key(id), TWO_BUCKETS) == hashed)
        .collect();
    let raw = fixture.raw_file_in_bucket(RAW_FILE, &ids, i32::try_from(recorded).unwrap())?;
    fixture.append_raw(catalog.as_ref(), raw).await?;
    let refused =
        refused_through_catalog(&fixture, &catalog, fixture.config(Residency::Full, false)).await?;
    let RefusalReason::KeyOutsideBucket {
        path,
        key,
        recorded: recorded_at,
        computed,
    } = &refused.reason
    else {
        panic!("{:?}", refused.reason);
    };
    assert_eq!(path, &format!("{}/{DATA_DIR}/{RAW_FILE}", fixture.root()));
    assert_eq!(key, &fixture.key(ids[0]), "the first key, a fence");
    assert_eq!((*recorded_at, *computed), (recorded, hashed));
    Ok(())
}

/// The budget is checked after every footer and before any chunk.
#[tokio::test]
async fn test_full_tier_over_budget_is_refused_at_the_footer_stage() -> Result<()> {
    let (_dir, catalog) = local_catalog().await?;
    let fixture = write_fixture(
        catalog.as_ref(),
        NAMESPACE,
        FixtureSpec::small("kv_budget", KeyType::Long),
    )
    .await?;
    let refused = refused_through_catalog(
        &fixture,
        &catalog,
        fixture.config_with_budget(Residency::Full, true, Some(TINY_BUDGET)),
    )
    .await?;
    let RefusalReason::OverBudget {
        of: Budget::Resident,
        needed,
        budget,
    } = refused.reason
    else {
        panic!("{:?}", refused.reason);
    };
    assert_eq!(budget, TINY_BUDGET);
    assert!(needed > budget);
    assert_eq!(
        refused.storage.stats().gets(),
        METADATA_FILE_GETS + LOAD_METADATA_GETS + count(fixture.file_count()),
        "the metadata, every footer, and no chunk"
    );
    Ok(())
}

/// The digits the fence budget fixture pads its string order values to: a
/// prefix every value shares and a continued page's fence keeps.
const FENCE_BUDGET_DIGITS: u32 = 2_000;
/// Rows per page of the fence budget fixture: few, so it has many fences.
const FENCE_BUDGET_PAGE_ROWS: usize = 3;
/// Keys of the fence budget fixture, in one bucket.
const FENCE_BUDGET_IDS: Range<u64> = 0..300;
/// The bytes the fence budget fixture's writer keeps of each column index
/// entry, far short of its order values.
const FENCE_BUDGET_TRUNCATE: usize = 16;

/// Rows per key of the fence budget fixture; every key holds the same
/// order values, so the order column's dictionary is small.
const FENCE_BUDGET_ROWS: u64 = 30;

/// An ordered table whose order column is dictionary-encoded, its values
/// long strings repeated across keys, under a truncated column index: no
/// footer number bounds its fences, which are many times its chunk's bytes
/// per value and its index's page minimums.
async fn fence_budget_fixture(catalog: &Arc<dyn Catalog>, name: &str) -> Result<Fixture> {
    let spec = FixtureSpec {
        ids: FENCE_BUDGET_IDS,
        buckets: ONE_BUCKET,
        layout: Layout {
            page_rows: FENCE_BUDGET_PAGE_ROWS,
            order_dictionary: true,
            column_index_truncate: Some(FENCE_BUDGET_TRUNCATE),
            ..Layout::contract()
        },
        ..FixtureSpec::ordered(
            name,
            KeyType::Long,
            Ordered {
                kind: OrderKind::PaddedStr(FENCE_BUDGET_DIGITS),
                rows_per_key: RowsPerKey::Each(FENCE_BUDGET_ROWS),
                start: 0,
                step: 1,
            },
        )
    };
    write_fixture(catalog.as_ref(), NAMESPACE, spec).await
}

/// A Full-tier ordered table is charged its exact resident bytes once
/// built: a budget the footer's estimate fits but the built fences do not
/// is `OverBudget` naming the exact bytes, after the chunks were fetched,
/// and that exact budget loads.
#[tokio::test]
async fn test_full_tier_charges_the_exact_order_fences_once_built() -> Result<()> {
    let (_dir, catalog) = local_catalog().await?;
    let fixture = fence_budget_fixture(&catalog, "fence_budget_full").await?;
    let source = || fixture.catalog_source(Arc::clone(&catalog));
    let config = |budget| fixture.config_with_budget(Residency::Full, false, budget);
    let unbounded =
        verify::serve(config(None), source(), verify::storage_config_for_tests()).await?;
    let exact = count(unbounded.current().index_bytes);
    drop(unbounded);
    let refused = load_refused(config(Some(exact - 1)), source()).await?;
    assert_eq!(
        refused.reason,
        RefusalReason::OverBudget {
            of: Budget::Resident,
            needed: exact,
            budget: exact - 1,
        }
    );
    assert!(
        refused.storage.stats().gets()
            > METADATA_FILE_GETS + LOAD_METADATA_GETS + count(fixture.file_count()),
        "refused after the chunks were fetched, not at the footer stage"
    );
    let fits = verify::serve(
        config(Some(exact)),
        source(),
        verify::storage_config_for_tests(),
    )
    .await?;
    assert_eq!(count(fits.current().index_bytes), exact);
    Ok(())
}

/// A Disk-tier ordered table is charged its chunk file's exact length
/// before it is written: an index budget the footer's estimate fits but
/// the chunk file does not is `OverBudget` with nothing written, and that
/// exact budget loads.
#[tokio::test]
async fn test_disk_tier_charges_the_exact_ordered_chunk_file_once_built() -> Result<()> {
    let (_dir, catalog) = local_catalog().await?;
    let fixture = fence_budget_fixture(&catalog, "fence_budget_disk").await?;
    assert_eq!(fixture.file_count(), 1, "one file, one reservation");
    let serve = |budget: Option<u64>| {
        let fixture = &fixture;
        let catalog = &catalog;
        async move {
            let scratch = tempfile::tempdir()?;
            let config = fixture.config(Residency::Disk, false);
            let (_storage, local, served) =
                verify::serve_disk(fixture, catalog, config, scratch.path(), budget).await?;
            Ok::<_, TestkitError>((served, local.index_bytes_in_use()?, scratch))
        }
    };
    let (served, exact, _scratch) = serve(None).await?;
    served?;
    assert!(exact > 0);
    let (refused, written, _scratch) = serve(Some(exact - 1)).await?;
    let Err(TestkitError::Core(Error::Refused { reason, .. })) = refused else {
        panic!("expected a refusal");
    };
    assert_eq!(
        reason,
        RefusalReason::OverBudget {
            of: Budget::Index,
            needed: exact,
            budget: exact - 1,
        }
    );
    assert_eq!(written, 0, "no chunk file was written");
    let (fits, written, _scratch) = serve(Some(exact)).await?;
    fits?;
    assert_eq!(written, exact);
    Ok(())
}

#[tokio::test]
async fn test_bloom_tier_without_a_filter_is_refused() -> Result<()> {
    let (_dir, catalog) = local_catalog().await?;
    let spec = FixtureSpec {
        ids: IDS_2K,
        buckets: TWO_BUCKETS,
        layout: Layout {
            bloom: Bloom::Off,
            ..Layout::contract()
        },
        ..FixtureSpec::small("kv_nobloom", KeyType::Long)
    };
    let fixture = write_fixture(catalog.as_ref(), NAMESPACE, spec).await?;
    let refused =
        refused_through_catalog(&fixture, &catalog, fixture.config(Residency::Bloom, false))
            .await?;
    assert!(
        matches!(refused.reason, RefusalReason::NoBloomFilter { .. }),
        "{:?}",
        refused.reason
    );
    Ok(())
}

/// Disk residency with no local directory is not a refusal but the
/// configuration violation `Config::parse` refuses; a configuration built
/// by hand, as this one is, meets it at the loader instead, before any
/// chunk is fetched, and the table is left as a refused first load leaves
/// it: no generation, `Degraded` with the failure's kind.
#[tokio::test]
async fn test_disk_tier_without_a_local_directory_is_a_config_violation() -> Result<()> {
    let (_dir, catalog) = local_catalog().await?;
    let spec = FixtureSpec {
        ids: IDS_2K,
        buckets: TWO_BUCKETS,
        ..FixtureSpec::small("kv_nolocal", KeyType::Long)
    };
    let fixture = write_fixture(catalog.as_ref(), NAMESPACE, spec).await?;
    let served = verify::build(
        Arc::new(Storage::new(verify::storage_config_for_tests())),
        fixture.config(Residency::Disk, false),
        fixture.catalog_source(Arc::clone(&catalog)),
        None,
        verify::TEST_CACHE_BYTES,
    )
    .await?;
    let error = served
        .table
        .load()
        .await
        .expect_err("the fixture is designed to be refused");
    assert!(
        matches!(
            error,
            Error::Config(ConfigError::Invalid(
                ConfigViolation::DiskResidencyWithoutLocalDir { .. }
            ))
        ),
        "{error:?}"
    );
    assert!(served.table.current().is_none());
    assert!(
        matches!(
            served.table.state(),
            TableState::Degraded {
                serving: None,
                reason: ErrorKind::Config,
                ..
            }
        ),
        "{:?}",
        served.table.state()
    );
    Ok(())
}

/// A strict table whose snapshot's layout is below its level is refused at
/// the footer stage (RFD 020): a dictionary-encoded value column caps the
/// level at balanced, the Full tier's default declares optimal, and the
/// load reads every footer and no key chunk.
#[tokio::test]
async fn test_strict_table_below_its_level_is_refused_before_any_chunk() -> Result<()> {
    let (_dir, catalog) = local_catalog().await?;
    let spec = FixtureSpec {
        layout: Layout {
            dictionary: Dictionary::Value,
            ..Layout::contract()
        },
        ..FixtureSpec::small("kv_below_level", KeyType::Long)
    };
    let fixture = write_fixture(catalog.as_ref(), NAMESPACE, spec).await?;
    let refused =
        refused_through_catalog(&fixture, &catalog, fixture.config(Residency::Full, true)).await?;
    assert_eq!(
        refused.reason,
        RefusalReason::BelowLevel {
            declared: Level::Optimal,
            achieved: Level::Balanced,
            columns: vec![icetray_core::ColumnLevel {
                column: VALUE_COLUMN.to_string(),
                level: Level::Balanced,
            }],
            findings: vec![Capping {
                code: FindingCode::DictionaryEncodedValue,
                column: Some(VALUE_COLUMN.to_string()),
            }],
        }
    );
    assert_eq!(
        refused.storage.stats().gets(),
        METADATA_FILE_GETS + LOAD_METADATA_GETS + count(fixture.file_count()),
        "the metadata, every footer, and no chunk"
    );
    Ok(())
}

/// A strict Disk table whose files overlap is refused from the files' edge
/// keys before any chunk file is written: `index/` stays empty, so the
/// refusal evicts nothing from the page cache.
#[tokio::test]
async fn test_strict_disk_table_with_overlapping_files_writes_no_chunk_file() -> Result<()> {
    let (_dir, catalog) = local_catalog().await?;
    let scratch = tempfile::tempdir()?;
    let mut fixture = write_fixture(
        catalog.as_ref(),
        NAMESPACE,
        FixtureSpec::interleaved("kv_overlap_disk"),
    )
    .await?;
    fixture
        .append(catalog.as_ref(), residue_class(SECOND_CLASS), "half")
        .await?;
    let config = fixture.config(Residency::Disk, true);
    let (_storage, local, served) =
        verify::serve_disk(&fixture, &catalog, config, scratch.path(), None).await?;
    let Err(TestkitError::Core(Error::Refused {
        reason:
            RefusalReason::BelowLevel {
                declared,
                achieved,
                findings,
                ..
            },
        ..
    })) = served
    else {
        panic!("expected a below_level refusal");
    };
    assert_eq!((declared, achieved), (Level::Optimal, Level::Permissive));
    assert_eq!(
        findings,
        [Capping {
            code: FindingCode::OverlappingFiles,
            column: None,
        }]
    );
    assert_eq!(local.index_bytes_in_use()?, 0, "no chunk file was written");
    Ok(())
}

/// A `lookups` level the residency cannot reach is the configuration
/// violation `Config::parse` refuses; a configuration built by hand meets
/// it at the loader, before the snapshot's manifests are read.
#[tokio::test]
async fn test_lookups_its_residency_cannot_reach_is_a_config_violation() -> Result<()> {
    let (_dir, catalog) = local_catalog().await?;
    let fixture = write_fixture(
        catalog.as_ref(),
        NAMESPACE,
        FixtureSpec::small("kv_unreachable", KeyType::Long),
    )
    .await?;
    let served = verify::build(
        Arc::new(Storage::new(verify::storage_config_for_tests())),
        TableConfig {
            lookups: Some(Level::Optimal),
            ..fixture.config(Residency::Bloom, false)
        },
        fixture.catalog_source(Arc::clone(&catalog)),
        None,
        verify::TEST_CACHE_BYTES,
    )
    .await?;
    let error = served
        .table
        .load()
        .await
        .expect_err("the configuration is designed to be refused");
    assert!(
        matches!(
            error,
            Error::Config(ConfigError::Invalid(ConfigViolation::LookupsUnreachable {
                index: None,
                lookups: Level::Optimal,
                residency: Residency::Bloom,
                ..
            }))
        ),
        "{error:?}"
    );
    assert_eq!(
        served.storage.stats().gets(),
        METADATA_FILE_GETS,
        "the metadata file, and no manifest and no footer"
    );
    assert!(served.table.current().is_none());
    assert!(
        matches!(
            served.table.state(),
            TableState::Degraded {
                serving: None,
                reason: ErrorKind::Config,
                ..
            }
        ),
        "{:?}",
        served.table.state()
    );
    Ok(())
}

/// A raw file of `RAW_KEYS`, their values, and `orders` as the order
/// column `ORDER_COLUMN`, one row per key, under the contract layout with
/// no manifest bounds.
fn ordered_raw_file(orders: ArrayRef) -> Result<RawFile> {
    let schema = Arc::new(ArrowSchema::new(vec![
        Field::new(KEY_COLUMN, DataType::Int64, false),
        Field::new(VALUE_COLUMN, DataType::Binary, true),
        Field::new(ORDER_COLUMN, orders.data_type().clone(), true),
    ]));
    let key: ArrayRef = Arc::new(Int64Array::from(RAW_KEYS.to_vec()));
    let value: ArrayRef = Arc::new(BinaryArray::from_iter_values(raw_values()));
    let batch = RecordBatch::try_new(schema, vec![key, value, orders])?;
    RawFile::from_batch(RAW_FILE, &batch, &Layout::contract(), Struct::empty(), None)
}

/// `fixture`'s configuration in the Full tier as an ordered table over
/// `ORDER_COLUMN`.
fn ordered_config(fixture: &Fixture) -> TableConfig {
    TableConfig {
        order_column: Some(ORDER_COLUMN.to_string()),
        ..fixture.config(Residency::Full, false)
    }
}

/// The schema refuses before any file is read: a `double` is no order
/// type (RFD 025).
#[tokio::test]
async fn test_order_type_unsupported_is_refused() -> Result<()> {
    let (_dir, catalog) = local_catalog().await?;
    let raw = ordered_raw_file(Arc::new(Float64Array::from(RAW_DOUBLE_ORDERS.to_vec())))?;
    let fixture = write_ordered_table_of_types(
        catalog.as_ref(),
        NAMESPACE,
        "ord_double",
        PrimitiveType::Double,
        raw,
    )
    .await?;
    let refused = refused_through_catalog(&fixture, &catalog, ordered_config(&fixture)).await?;
    assert_eq!(
        refused.reason,
        RefusalReason::OrderTypeUnsupported {
            column: ORDER_COLUMN.to_string(),
            got: PrimitiveType::Double.to_string(),
        }
    );
    assert_eq!(
        refused.storage.stats().gets(),
        METADATA_FILE_GETS,
        "refused from the metadata file alone"
    );
    Ok(())
}

/// A `timestamp` order column a file stores in milliseconds is in another
/// unit than Iceberg's microseconds, so its values would compare wrong
/// against every other file's (RFD 025).
#[tokio::test]
async fn test_order_type_mismatch_is_refused_for_millisecond_timestamps() -> Result<()> {
    let (_dir, catalog) = local_catalog().await?;
    let raw = ordered_raw_file(Arc::new(TimestampMillisecondArray::from(
        RAW_ORDERS.to_vec(),
    )))?;
    let fixture = write_ordered_table_of_types(
        catalog.as_ref(),
        NAMESPACE,
        "ord_millis",
        PrimitiveType::Timestamp,
        raw,
    )
    .await?;
    let refused = refused_through_catalog(&fixture, &catalog, ordered_config(&fixture)).await?;
    let RefusalReason::OrderTypeMismatch {
        path,
        expected: OrderType::Timestamp,
        ..
    } = &refused.reason
    else {
        panic!("{:?}", refused.reason);
    };
    assert!(
        fixture.files().iter().any(|file| file.file_path() == path),
        "{path} is the raw file"
    );
    Ok(())
}

/// A file written before Iceberg promoted the order column from `int` to
/// `long` stores it as `INT32`; it is served, its values read as longs
/// (RFD 025).
#[tokio::test]
async fn test_int_order_promoted_to_long_is_served() -> Result<()> {
    let (_dir, catalog) = local_catalog().await?;
    let raw = ordered_raw_file(Arc::new(Int32Array::from(RAW_INT_ORDERS.to_vec())))?;
    let fixture = write_ordered_table_of_types(
        catalog.as_ref(),
        NAMESPACE,
        "ord_promoted",
        PrimitiveType::Long,
        raw,
    )
    .await?;
    let served = verify::serve(
        ordered_config(&fixture),
        fixture.catalog_source(Arc::clone(&catalog)),
        verify::storage_config_for_tests(),
    )
    .await?;
    let every_row = ScanBounds {
        direction: Direction::OldestFirst,
        limit: DEFAULT_MAX_SCAN_ROWS,
        start: None,
        before: None,
    };
    for ((&key, value), &order) in RAW_KEYS.iter().zip(raw_values()).zip(&RAW_INT_ORDERS) {
        let scanned = served
            .table
            .scan(&Key::Long(key), &verify::whole(&[VALUE_COLUMN]), &every_row)
            .await?
            .scanned;
        assert_eq!(
            scanned.rows,
            [ScanRow {
                order: OrderValue::Long(i64::from(order)),
                values: vec![Some(value.into())],
            }],
            "{key}"
        );
        assert_eq!(scanned.next, None);
    }
    Ok(())
}

/// A file written before the order column was added has no order values:
/// its default would give every row one value, so the file is refused
/// naming the order column (RFD 025, RFD 023).
#[tokio::test]
async fn test_file_written_before_the_order_column_is_refused_as_column_mismatch() -> Result<()> {
    let (_dir, catalog) = local_catalog().await?;
    let spec = FixtureSpec {
        ids: IDS_2K,
        ..FixtureSpec::small("ord_added", KeyType::Long)
    };
    let mut fixture = write_fixture(catalog.as_ref(), NAMESPACE, spec).await?;
    fixture
        .evolve(&[SchemaChange::Add {
            name: ORDER_COLUMN.to_string(),
            ty: PrimitiveType::Long,
            default: StatedDefault::None,
        }])
        .await?;
    let source = fixture.root_source();
    let refused = load_refused(
        TableConfig {
            source: source.clone(),
            ..ordered_config(&fixture)
        },
        TableSource::from_config(&source).await?,
    )
    .await?;
    let RefusalReason::ColumnMismatch {
        column,
        path: Some(path),
        mismatch: ColumnMismatch::OrderAbsent,
    } = &refused.reason
    else {
        panic!("{:?}", refused.reason);
    };
    assert_eq!(column, ORDER_COLUMN);
    assert!(
        fixture.files().iter().any(|file| file.file_path() == path),
        "{path} is one of the files written before the column"
    );
    Ok(())
}

/// Rows per id: two, so every key of an ordered fixture of this shape
/// repeats.
const REPEATED_ROWS: u64 = 2;

/// A one-bucket ordered fixture of `IDS_2K` named `name`, two `long` order
/// values per key `step` apart from zero.
async fn two_row_fixture(catalog: &Arc<dyn Catalog>, name: &str, step: i64) -> Result<Fixture> {
    let ordered = Ordered {
        kind: OrderKind::Long,
        rows_per_key: RowsPerKey::Each(REPEATED_ROWS),
        start: 0,
        step,
    };
    let spec = FixtureSpec {
        ids: IDS_2K,
        buckets: ONE_BUCKET,
        ..FixtureSpec::ordered(name, KeyType::Long, ordered)
    };
    write_fixture(catalog.as_ref(), NAMESPACE, spec).await
}

/// The load verifies an ordered table's rows by key and then order value
/// (RFD 025, Load): rows of one key rising by order value are served,
/// where a key-value entry over the same table refuses the repeated key.
#[tokio::test]
async fn test_ordered_rows_rising_per_key_are_served_and_repeat_a_key_value_key() -> Result<()> {
    let (_dir, catalog) = local_catalog().await?;
    let fixture = two_row_fixture(&catalog, "ord_rising", 1).await?;
    verify::serve(
        ordered_config(&fixture),
        fixture.catalog_source(Arc::clone(&catalog)),
        verify::storage_config_for_tests(),
    )
    .await?;
    let key_value = TableConfig {
        order_column: None,
        ..fixture.config(Residency::Full, false)
    };
    let refused = refused_through_catalog(&fixture, &catalog, key_value).await?;
    assert!(
        matches!(refused.reason, RefusalReason::DuplicateKeys { .. }),
        "{:?}",
        refused.reason
    );
    Ok(())
}

/// A key and order value together twice is `duplicate_rows`, naming both
/// (RFD 025, Uniqueness).
#[tokio::test]
async fn test_duplicate_rows_are_refused() -> Result<()> {
    let (_dir, catalog) = local_catalog().await?;
    let fixture = two_row_fixture(&catalog, "ord_duprows", 0).await?;
    let refused = refused_through_catalog(&fixture, &catalog, ordered_config(&fixture)).await?;
    let (path, key, order) = verify::duplicate_rows(&refused.reason);
    assert!(
        fixture.files().iter().any(|file| file.file_path() == path),
        "{path} is one of the fixture's files"
    );
    assert_eq!(
        *key,
        fixture.key(IDS_2K.start),
        "the file's first key repeats first"
    );
    assert_eq!(*order, OrderValue::Long(0));
    Ok(())
}

/// A key's order value falling is `unsorted`.
#[tokio::test]
async fn test_ordered_rows_falling_per_key_are_refused_as_unsorted() -> Result<()> {
    let (_dir, catalog) = local_catalog().await?;
    let fixture = two_row_fixture(&catalog, "ord_falling", -1).await?;
    let refused = refused_through_catalog(&fixture, &catalog, ordered_config(&fixture)).await?;
    assert!(
        matches!(refused.reason, RefusalReason::Unsorted { .. }),
        "{:?}",
        refused.reason
    );
    Ok(())
}

/// A null order value is `null_order`.
#[tokio::test]
async fn test_null_order_is_refused() -> Result<()> {
    let (_dir, catalog) = local_catalog().await?;
    let orders: Vec<Option<i64>> = RAW_ORDERS
        .iter()
        .enumerate()
        .map(|(at, &order)| (at != NULL_KEY_AT).then_some(order))
        .collect();
    let raw = ordered_raw_file(Arc::new(Int64Array::from(orders)))?;
    let fixture = write_ordered_table_of_types(
        catalog.as_ref(),
        NAMESPACE,
        "ord_null",
        PrimitiveType::Long,
        raw,
    )
    .await?;
    let refused = refused_through_catalog(&fixture, &catalog, ordered_config(&fixture)).await?;
    let RefusalReason::NullOrder { path, row_group } = &refused.reason else {
        panic!("{:?}", refused.reason);
    };
    assert!(
        fixture.files().iter().any(|file| file.file_path() == path),
        "{path} is the raw file"
    );
    assert_eq!(*row_group, 0);
    Ok(())
}

/// The id the cross-file tests repeat a pair of: not the first of the
/// one-bucket fixture, so a file of its rows alone is later in file order
/// than the fixture's.
const REPEATED_ID: u64 = 17;

/// The order value of `two_row_fixture`'s second row of every key, at a
/// step of one.
const SECOND_ORDER: i64 = 1;

/// A pair a later file repeats is `duplicate_rows` across files (RFD 025,
/// Uniqueness), naming the later file in file order: the appended file,
/// whose first key is past the fixture's.
#[tokio::test]
async fn test_duplicate_rows_across_files_are_refused_naming_the_later_file() -> Result<()> {
    let (_dir, catalog) = local_catalog().await?;
    let mut fixture = two_row_fixture(&catalog, "ord_dup_files", 1).await?;
    assert_eq!(fixture.file_count(), 1, "one bucket, one file");
    let written = fixture.files()[0].file_path().to_string();
    fixture
        .append_ordered(catalog.as_ref(), &[(REPEATED_ID, SECOND_ORDER)], "again")
        .await?;
    let appended = fixture
        .files()
        .iter()
        .map(|file| file.file_path().to_string())
        .find(|path| *path != written)
        .expect("the append adds a file");
    let refused = refused_through_catalog(&fixture, &catalog, ordered_config(&fixture)).await?;
    let (path, key, order) = verify::duplicate_rows(&refused.reason);
    assert_eq!(path, appended);
    assert_eq!(*key, fixture.key(REPEATED_ID));
    assert_eq!(*order, OrderValue::Long(SECOND_ORDER));
    Ok(())
}

/// A ref moved back to a snapshot that lists both copies of an updated pair
/// but not the deletion vector that hid the older one revives it (RFD 021):
/// the reused file whose deletes shrank is checked again, and the refresh is
/// `duplicate_rows`, naming the later file in file order, while the last
/// generation keeps serving, in every residency.
#[tokio::test]
async fn test_refresh_refuses_a_pair_revived_by_dropped_deletes() -> Result<()> {
    for (at, residency) in Residency::ALL.into_iter().enumerate() {
        let (_dir, catalog) = local_catalog().await?;
        let mut fixture = two_row_fixture(&catalog, &format!("ord_revived_{at}"), 1).await?;
        fixture
            .update_rows(
                &[(REPEATED_ID, SECOND_ORDER)],
                DeleteForm::Vectors,
                "update",
            )
            .await?;
        let vector = fixture.deletes().to_vec();
        assert_eq!(vector.len(), 1, "one deletion vector hides the older copy");
        fixture
            .commit_by_hand(HandSnapshot {
                data: fixture.files().to_vec(),
                deletes: Vec::new(),
            })
            .await?;
        let without_vector = fixture.snapshot_id().expect("a snapshot was committed");
        fixture
            .commit_by_hand(HandSnapshot {
                data: fixture.files().to_vec(),
                deletes: vector,
            })
            .await?;
        let source = fixture.root_source();
        let storage = Arc::new(Storage::new(verify::storage_config_for_tests()));
        let scratch = tempfile::tempdir()?;
        let local = verify::local_for(residency, scratch.path(), &storage)?;
        let served = verify::serve_over(
            storage,
            TableConfig {
                source: source.clone(),
                residency,
                ..ordered_config(&fixture)
            },
            TableSource::from_config(&source).await?,
            local,
        )
        .await?;
        let generation = served.current();
        assert_eq!(generation.residency, residency);
        assert_eq!(generation.files.len(), 2, "both copies' files serve");
        let later = generation.files[1].entry.path.clone();

        fixture.move_ref(without_vector).await?;
        let error = served.table.refresh().await.unwrap_err();
        let (path, key, order) = verify::refused_duplicate_rows(&error);
        assert_eq!(path, later, "{residency:?}");
        assert_eq!(*key, fixture.key(REPEATED_ID));
        assert_eq!(*order, OrderValue::Long(SECOND_ORDER));
        assert_eq!(served.current().snapshot_id, generation.snapshot_id);
    }
    Ok(())
}
