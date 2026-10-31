//! The ref moves and the table follows (RFD 005): an unchanged poll reads
//! the catalog record or the hint and no object, a second snapshot appends
//! files, the table advances on refresh reusing every unchanged file, a
//! duplicate key and an Avro data file are each refused while the last
//! generation keeps serving, a strict table below its level is held until a compaction, and
//! overlapping files are verified once per group that gained a file. An
//! ordered table checks a repeated pair only where one can occur, and a
//! refresh costs its delta (RFD 025).

use std::collections::HashSet;
use std::ops::Range;
use std::sync::Arc;

use bytes::Bytes;
use iceberg::spec::{DataFileFormat, Struct};
use icetray_core::ColumnRead;
use icetray_core::approx;
use icetray_core::config::{Residency, TableConfig};
use icetray_core::metric;
use icetray_core::{
    Capping, Error, ErrorKind, FILE_PREFIX, FindingCode, Generation, KeyType, Level, LoadedFile,
    OrderValue, RefusalReason, Row, Storage, TableSource, TableState, UnsupportedFormat,
};
use icetray_testkit::delete_files::{Vector, write_vectors};
use icetray_testkit::fixture::{
    APPENDED_4K_6K, DATA_DIR, IDS_4K, INTERLEAVED_BUCKETS, INTERLEAVED_SAMPLE, NAMESPACE, NEW_ID,
    NEWEST_SAMPLE, OLD_ID, OVERLAPPING_APPEND, SAMPLE_4K, SECOND_CLASS, TAIL, THIRD_CLASS,
    THREE_FILES, TWO_BUCKETS, TWO_FILES, data_file_entry, local_catalog, residue_class,
};
use icetray_testkit::keys::{FIRST_VERSION, value_of_version};
use icetray_testkit::layout::VALUE_COLUMN;
use icetray_testkit::verify::{
    self, HINT_POLL_GETS, LOAD_GETS_PER_FILE, LOAD_METADATA_GETS, METADATA_FILE_GETS, Served,
};
use icetray_testkit::{
    CountingStore, DeleteForm, Fixture, FixtureSpec, HandSnapshot, Layout, MergeOracle, OrderKind,
    Ordered, Result, Statistics, TestkitError, write_fixture,
};
use object_store::ObjectStore;
use roaring::RoaringTreemap;

/// The first build of the reuse fixture and the range its second appends.
const IDS_8K: Range<u64> = 0..8_000;
const APPENDED_8K_12K: Range<u64> = 8_000..12_000;

/// An id the second build adds, absent before it.
const FUTURE_ID: u64 = 9_000;

/// Refresh GETs beyond the per-file ones after one append through the
/// catalog: the new metadata file, the manifest list, and two manifests.
const REFRESH_METADATA_GETS: u64 = 4;

/// Records and bytes the manifest records for the Avro data file entry,
/// with nothing written at its path.
const UNWRITTEN_RECORDS: u64 = 1;
const UNWRITTEN_BYTES: u64 = 1;

/// Files of the interleaved fixture after the second class: two per bucket.
const INTERLEAVED_FILES: usize = 8;

/// Ids written by the first two classes.
const INTERLEAVED_WRITTEN: usize = 4_000;

/// `fixture` served in the Full tier from its root, through the hint,
/// under `strict`.
async fn serve_root(fixture: &Fixture, strict: bool) -> Result<Served> {
    let source = fixture.root_source();
    verify::serve(
        TableConfig {
            source: source.clone(),
            ..fixture.config(Residency::Full, strict)
        },
        TableSource::from_config(&source).await?,
        verify::storage_config_for_tests(),
    )
    .await
}

#[tokio::test]
async fn test_refresh_follows_the_catalog_and_reuses_files() -> Result<()> {
    let (_dir, catalog) = local_catalog().await?;
    let spec = FixtureSpec {
        ids: IDS_8K,
        ..FixtureSpec::small("kv_adv", KeyType::Long)
    };
    let mut fixture = write_fixture(catalog.as_ref(), NAMESPACE, spec).await?;
    let served = verify::serve_catalog(&fixture, &catalog, Residency::Full, false).await?;
    let v1 = served.current();
    let new_key = fixture.key(FUTURE_ID);
    assert_eq!(
        served
            .table
            .get(&new_key, &[ColumnRead::whole(VALUE_COLUMN)])
            .await?
            .row,
        Row::Absent
    );
    let stats = served.storage.stats();
    let before = stats.gets();
    assert!(!served.table.refresh().await?, "nothing moved yet");
    assert_eq!(
        stats.gets(),
        before,
        "an unchanged poll reads the catalog record and no object"
    );

    fixture
        .append(catalog.as_ref(), APPENDED_8K_12K, "build2")
        .await?;
    let before = stats.gets();
    assert!(served.table.refresh().await?, "the ref moved");
    let v2 = served.current();
    assert_ne!(v2.snapshot_id, v1.snapshot_id);
    assert_eq!(v2.files.len(), 2 * v1.files.len());
    let reused = v2
        .files
        .iter()
        .filter(|file| v1.files.iter().any(|old| Arc::ptr_eq(old, file)))
        .count();
    assert_eq!(reused, v1.files.len(), "v1's files are shared by pointer");
    let new_files = approx::count(v2.files.len() - reused);
    assert_eq!(
        stats.gets() - before,
        REFRESH_METADATA_GETS + LOAD_GETS_PER_FILE * new_files,
        "metadata file, manifest list, two manifests, then a footer and a chunk per new file"
    );

    let lookup = served
        .table
        .get(&new_key, &[ColumnRead::whole(VALUE_COLUMN)])
        .await?;
    assert_eq!(lookup.snapshot_id, v2.snapshot_id);
    assert_eq!(lookup.row, fixture.expected(&new_key));
    let old_key = fixture.key(OLD_ID);
    assert_eq!(
        served
            .table
            .get(&old_key, &[ColumnRead::whole(VALUE_COLUMN)])
            .await?
            .row,
        fixture.expected(&old_key)
    );
    Ok(())
}

/// `fixture` served in the Full tier from its root, through the hint,
/// reading through a row cache (`verify::serve_reading_rows`).
async fn serve_root_reading_rows(fixture: &Fixture) -> Result<Served> {
    let source = fixture.root_source();
    verify::serve_reading_rows(
        TableConfig {
            source: source.clone(),
            ..fixture.config(Residency::Full, false)
        },
        TableSource::from_config(&source).await?,
    )
    .await
}

/// A row cached from one file and rewritten into another by a
/// copy-on-write commit with a new value is read from the new file after
/// the refresh: the cache is keyed by file, so the new file's row is a
/// miss, though it sits at the same position (RFD 026).
#[tokio::test]
async fn test_a_rewritten_row_is_read_from_its_new_file() -> Result<()> {
    let (_dir, catalog) = local_catalog().await?;
    let spec = FixtureSpec {
        ids: IDS_4K,
        ..FixtureSpec::small("kv_row_cache_rewrite", KeyType::Long)
    };
    let mut fixture = write_fixture(catalog.as_ref(), NAMESPACE, spec).await?;
    fixture.write_hint().await?;
    let served = serve_root_reading_rows(&fixture).await?;
    let key = fixture.key(OLD_ID);
    let read = [ColumnRead::whole(VALUE_COLUMN)];
    let old = served.table.get(&key, &read).await?.row;
    assert_eq!(old, fixture.expected(&key));
    let rows = served.row_cache.as_ref().expect("served reading rows");
    assert!(rows.usage() > 0, "the row is cached");

    let ids: Vec<u64> = fixture.ids().collect();
    let next = FIRST_VERSION + 1;
    let rewritten = fixture.write_rows(&ids, next, "rewritten").await?;
    fixture
        .commit_by_hand(HandSnapshot {
            data: rewritten,
            deletes: Vec::new(),
        })
        .await?;
    assert!(served.table.refresh().await?);
    let generation = served.current();
    let (file, _) = generation.plan(std::slice::from_ref(&key))[0]
        .clone()
        .expect("the plan names the key's file");
    assert!(
        fixture
            .files()
            .iter()
            .any(|data| data.file_path() == file.entry.path),
        "the key is read from a rewritten file"
    );
    let spec = fixture.spec();
    let new = Row::Present(vec![Some(Bytes::from(value_of_version(
        &key,
        spec.value_bytes,
        spec.value_kind,
        next,
    )))]);
    assert_ne!(new, old, "the rewrite changed the value");
    assert_eq!(served.table.get(&key, &read).await?.row, new);
    Ok(())
}

/// A refresh that appends a file keeps the entries of the files it reuses:
/// a row cached before it is a row-cache hit after it, which reads no
/// value page with the page cache emptied (RFD 026).
#[tokio::test]
async fn test_a_reused_file_keeps_its_entries_across_refresh() -> Result<()> {
    let (_dir, catalog) = local_catalog().await?;
    let spec = FixtureSpec {
        ids: IDS_4K,
        ..FixtureSpec::small("kv_row_cache_reuse", KeyType::Long)
    };
    let mut fixture = write_fixture(catalog.as_ref(), NAMESPACE, spec).await?;
    let served = verify::serve_reading_rows(
        fixture.config(Residency::Full, false),
        fixture.catalog_source(Arc::clone(&catalog)),
    )
    .await?;
    let key = fixture.key(OLD_ID);
    let read = [ColumnRead::whole(VALUE_COLUMN)];
    assert_eq!(
        served.table.get(&key, &read).await?.row,
        fixture.expected(&key)
    );
    let v1 = served.current();

    fixture
        .append(catalog.as_ref(), APPENDED_4K_6K, "build2")
        .await?;
    assert!(served.table.refresh().await?);
    let v2 = served.current();
    assert!(
        v1.files
            .iter()
            .all(|old| v2.files.iter().any(|now| Arc::ptr_eq(old, now))),
        "every file of the first generation is reused"
    );
    served.cache.clear();
    let stats = served.storage.stats();
    let before = stats.gets();
    assert_eq!(
        served.table.get(&key, &read).await?.row,
        fixture.expected(&key)
    );
    assert_eq!(stats.gets() - before, 0, "no value page is read");
    Ok(())
}

#[tokio::test]
async fn test_refresh_follows_a_static_root_through_its_hint() -> Result<()> {
    let (_dir, catalog) = local_catalog().await?;
    let spec = FixtureSpec {
        ids: IDS_4K,
        ..FixtureSpec::small("kv_hint", KeyType::Long)
    };
    let mut fixture = write_fixture(catalog.as_ref(), NAMESPACE, spec).await?;
    fixture.write_hint().await?;
    let served = serve_root(&fixture, false).await?;
    let v1 = served.current();
    assert_eq!(v1.metadata_location, fixture.metadata_location());

    fixture
        .append(catalog.as_ref(), APPENDED_4K_6K, "build2")
        .await?;
    let stats = served.storage.stats();
    let before = stats.gets();
    assert!(!served.table.refresh().await?, "the hint still names v1");
    assert_eq!(stats.gets() - before, HINT_POLL_GETS);

    fixture.write_hint().await?;
    assert!(served.table.refresh().await?);
    assert_ne!(served.current().snapshot_id, v1.snapshot_id);
    let new_key = fixture.key(NEW_ID);
    assert_eq!(
        served
            .table
            .get(&new_key, &[ColumnRead::whole(VALUE_COLUMN)])
            .await?
            .row,
        fixture.expected(&new_key)
    );
    Ok(())
}

#[tokio::test]
async fn test_refresh_refuses_duplicate_keys_and_keeps_serving() -> Result<()> {
    let (_dir, catalog) = local_catalog().await?;
    let spec = FixtureSpec {
        ids: IDS_4K,
        ..FixtureSpec::small("kv_dup", KeyType::Long)
    };
    let mut fixture = write_fixture(catalog.as_ref(), NAMESPACE, spec).await?;
    let served = verify::serve_catalog(&fixture, &catalog, Residency::Full, false).await?;
    let v1 = served.current().snapshot_id;

    fixture
        .append(catalog.as_ref(), OVERLAPPING_APPEND, "build2")
        .await?;
    let error = served.table.refresh().await.unwrap_err();
    assert!(
        matches!(
            error,
            Error::Refused {
                reason: RefusalReason::DuplicateKeys { .. },
                ..
            }
        ),
        "{error:?}"
    );
    assert_eq!(served.current().snapshot_id, v1);
    let old_key = fixture.key(OLD_ID);
    assert_eq!(
        served
            .table
            .get(&old_key, &[ColumnRead::whole(VALUE_COLUMN)])
            .await?
            .row,
        fixture.expected(&old_key)
    );
    assert!(
        matches!(
            served.table.state(),
            TableState::Degraded {
                serving: Some(serving),
                reason: ErrorKind::Refused(RefusalReason::DuplicateKeys { .. }),
                ..
            } if serving == v1
        ),
        "{:?}",
        served.table.state()
    );
    Ok(())
}

/// A refresh to a snapshot that adds a data file recorded as Avro is
/// refused from its manifests, the file never read; the last generation
/// keeps serving, and the refusal is remembered.
#[tokio::test]
async fn test_refresh_refuses_an_avro_data_file_and_keeps_serving() -> Result<()> {
    let (_dir, catalog) = local_catalog().await?;
    let spec = FixtureSpec {
        ids: IDS_4K,
        buckets: 0,
        ..FixtureSpec::small("kv_avro_refresh", KeyType::Long)
    };
    let mut fixture = write_fixture(catalog.as_ref(), NAMESPACE, spec).await?;
    fixture.write_hint().await?;
    let served = serve_root(&fixture, false).await?;
    let v1 = served.current().snapshot_id;

    let path = format!("{}/{DATA_DIR}/rows.avro", fixture.root());
    let mut data = fixture.files().to_vec();
    data.push(data_file_entry(
        &path,
        fixture.metadata().default_partition_spec_id(),
        Struct::empty(),
        UNWRITTEN_RECORDS,
        UNWRITTEN_BYTES,
        DataFileFormat::Avro,
    )?);
    fixture
        .commit_by_hand(HandSnapshot {
            data,
            deletes: Vec::new(),
        })
        .await?;
    let stats = served.storage.stats();
    let before = stats.gets();
    let error = served.table.refresh().await.unwrap_err();
    let refused = RefusalReason::DataFormatUnsupported {
        path,
        format: UnsupportedFormat::Avro,
    };
    assert!(
        matches!(&error, Error::Refused { reason, .. } if *reason == refused),
        "{error:?}"
    );
    assert_eq!(
        stats.gets() - before,
        HINT_POLL_GETS + METADATA_FILE_GETS + LOAD_METADATA_GETS,
        "the hint, the metadata, the manifest list and its manifest, and no data file"
    );
    assert_eq!(
        served.current().snapshot_id,
        v1,
        "the last generation serves"
    );
    let old_key = fixture.key(OLD_ID);
    assert_eq!(
        served
            .table
            .get(&old_key, &[ColumnRead::whole(VALUE_COLUMN)])
            .await?
            .row,
        fixture.expected(&old_key)
    );
    assert!(
        matches!(
            served.table.state(),
            TableState::Degraded {
                serving: Some(serving),
                reason: ErrorKind::Refused(ref reason),
                ..
            } if serving == v1 && *reason == refused
        ),
        "{:?}",
        served.table.state()
    );
    let before = stats.gets();
    assert!(!served.table.refresh().await?, "the refusal is remembered");
    assert_eq!(
        stats.gets() - before,
        HINT_POLL_GETS,
        "a poll of the remembered location reads the hint alone"
    );
    Ok(())
}

#[tokio::test]
async fn test_refresh_recovers_when_the_offending_file_is_dropped() -> Result<()> {
    let (_dir, catalog) = local_catalog().await?;
    let spec = FixtureSpec {
        ids: IDS_4K,
        ..FixtureSpec::small("kv_recover", KeyType::Long)
    };
    let mut fixture = write_fixture(catalog.as_ref(), NAMESPACE, spec).await?;
    fixture.write_hint().await?;
    let served = serve_root(&fixture, false).await?;
    let v1 = served.current();
    let v1_paths: HashSet<String> = v1
        .files
        .iter()
        .map(|file| file.entry.path.clone())
        .collect();

    fixture.append(catalog.as_ref(), [OLD_ID], "dup").await?;
    fixture.write_hint().await?;
    let error = served.table.refresh().await.unwrap_err();
    assert!(
        matches!(
            error,
            Error::Refused {
                reason: RefusalReason::DuplicateKeys { .. },
                ..
            }
        ),
        "{error:?}"
    );
    assert_eq!(fixture.file_count(), v1.files.len() + 1);

    let data: Vec<_> = fixture
        .files()
        .iter()
        .filter(|file| v1_paths.contains(file.file_path()))
        .cloned()
        .collect();
    assert_eq!(
        data.len(),
        v1.files.len(),
        "the one duplicating file is dropped"
    );
    fixture
        .commit_by_hand(HandSnapshot {
            data,
            deletes: Vec::new(),
        })
        .await?;
    assert!(served.table.refresh().await?);
    assert!(matches!(served.table.state(), TableState::Serving { .. }));
    assert_ne!(served.current().snapshot_id, v1.snapshot_id);
    for id in SAMPLE_4K {
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
    let appended_error = fixture
        .append(catalog.as_ref(), [OLD_ID], "again")
        .await
        .unwrap_err();
    assert!(matches!(
        appended_error,
        TestkitError::Spec(icetray_testkit::error::SpecFault::CatalogBehind)
    ));
    Ok(())
}

/// Files in one bucket whose key ranges interleave but share no key are
/// served at the cost of probing each. Uniqueness is proved once per
/// group: a range-disjoint append verifies nothing again, an interleaving
/// one verifies its groups.
#[tokio::test]
async fn test_overlapping_files_serve_and_reverify_only_grown_groups() -> Result<()> {
    let (_dir, catalog) = local_catalog().await?;
    let mut fixture = write_fixture(
        catalog.as_ref(),
        NAMESPACE,
        FixtureSpec::interleaved("kv_interleaved"),
    )
    .await?;
    fixture
        .append(catalog.as_ref(), residue_class(SECOND_CLASS), "half")
        .await?;
    let served = verify::serve_catalog(&fixture, &catalog, Residency::Full, false).await?;
    let generation = served.current();
    assert_eq!(
        generation.files.len(),
        INTERLEAVED_FILES,
        "two files per bucket"
    );
    assert_eq!(
        generation.overlapping_buckets().len(),
        approx::length(INTERLEAVED_BUCKETS)
    );
    assert!(
        generation
            .overlapping_buckets()
            .values()
            .all(|&files| files == TWO_FILES)
    );
    assert_eq!(
        generation.overlap_groups_verified(),
        approx::length(INTERLEAVED_BUCKETS)
    );
    let served = verify::verify_hit_cost(served, &fixture, &INTERLEAVED_SAMPLE).await?;
    verify::verify_misses(&served, &fixture, &INTERLEAVED_SAMPLE).await?;
    assert_eq!(fixture.ids().count(), INTERLEAVED_WRITTEN);

    fixture.append(catalog.as_ref(), TAIL, "tail").await?;
    assert!(served.table.refresh().await?);
    let generation = served.current();
    assert_eq!(
        generation.overlap_groups_verified(),
        0,
        "disjoint files join no group"
    );
    assert!(
        generation
            .overlapping_buckets()
            .values()
            .all(|&files| files == TWO_FILES)
    );

    fixture
        .append(catalog.as_ref(), residue_class(THIRD_CLASS), "quarter")
        .await?;
    assert!(served.table.refresh().await?);
    let generation = served.current();
    assert_eq!(
        generation.overlap_groups_verified(),
        approx::length(INTERLEAVED_BUCKETS)
    );
    assert!(
        generation
            .overlapping_buckets()
            .values()
            .all(|&files| files == THREE_FILES)
    );
    let fresh = verify::serve_catalog(&fixture, &catalog, Residency::Full, false).await?;
    verify::verify_hit_cost(fresh, &fixture, &NEWEST_SAMPLE).await?;
    assert_eq!(generation.records, approx::count(fixture.ids().count()));
    Ok(())
}

/// The level gauges of `table` in `scrape`: below its level, the level
/// achieved, and the level declared.
fn level_gauges(scrape: &str, table: &str) -> [Option<f64>; 3] {
    let labels = [(metric::LABEL_TABLE, table)];
    [
        metric::TABLE_BELOW_LEVEL,
        metric::TABLE_LEVEL,
        metric::TABLE_DECLARED_LEVEL,
    ]
    .map(|name| verify::series_value(scrape, name, &labels))
}

/// A strict table whose newest snapshot falls below its level is held
/// (RFD 020): an append that interleaves with every bucket's file overlaps,
/// so the last snapshot that met the level keeps serving, the table is
/// `Held`, and the level gauges judge the held snapshot; once a compaction
/// rewrites the rows into one file per bucket, the next refresh swaps the
/// compacted snapshot in and the gauges judge it.
#[tokio::test]
async fn test_refresh_below_level_holds_then_swaps_once_compacted() -> Result<()> {
    let recorder = icetray::telemetry::install().expect("the recorder installs");
    // The gauges' values for the levels, as `metric::TABLE_LEVEL` says.
    let (optimal, permissive) = (0.0, 2.0);
    let (below, at_level) = (1.0, 0.0);
    let (_dir, catalog) = local_catalog().await?;
    let mut fixture = write_fixture(
        catalog.as_ref(),
        NAMESPACE,
        FixtureSpec::interleaved("kv_held"),
    )
    .await?;
    fixture.write_hint().await?;
    let served = serve_root(&fixture, true).await?;
    let v1 = served.current();
    assert_eq!(v1.level, Level::Optimal);

    let appended = residue_class(SECOND_CLASS);
    fixture
        .append(catalog.as_ref(), appended.iter().copied(), "half")
        .await?;
    fixture.write_hint().await?;
    let error = served.table.refresh().await.unwrap_err();
    let Error::Refused {
        reason:
            RefusalReason::BelowLevel {
                declared,
                achieved,
                findings,
                ..
            },
        ..
    } = error
    else {
        panic!("expected a below_level refusal, got {error:?}");
    };
    assert_eq!((declared, achieved), (Level::Optimal, Level::Permissive));
    assert_eq!(
        findings,
        [Capping {
            code: FindingCode::OverlappingFiles,
            column: None,
        }]
    );
    let TableState::Held { serving, level, .. } = served.table.state() else {
        panic!("expected Held, got {:?}", served.table.state());
    };
    assert_eq!((serving, level), (v1.snapshot_id, Level::Permissive));
    let scrape = || {
        served.table.publish_gauges();
        recorder.render()
    };
    assert_eq!(
        level_gauges(&scrape(), served.table.name()),
        [Some(below), Some(permissive), Some(optimal)]
    );
    let held_key = fixture.key(appended[0]);
    assert_eq!(
        served
            .table
            .get(&held_key, &[ColumnRead::whole(VALUE_COLUMN)])
            .await?
            .row,
        Row::Absent
    );
    assert!(
        !served.table.refresh().await?,
        "the refused snapshot is not built again"
    );

    fixture.compact("compacted").await?;
    assert!(served.table.refresh().await?);
    assert!(matches!(served.table.state(), TableState::Serving { .. }));
    let compacted = served.current();
    assert_eq!(compacted.level, Level::Optimal);
    assert_eq!(compacted.files.len(), v1.files.len(), "one file per bucket");
    assert!(compacted.overlapping_buckets().is_empty());
    assert_eq!(
        level_gauges(&scrape(), served.table.name()),
        [Some(at_level), Some(optimal), Some(optimal)]
    );
    assert_eq!(
        served
            .table
            .get(&held_key, &[ColumnRead::whole(VALUE_COLUMN)])
            .await?
            .row,
        fixture.expected(&held_key)
    );
    Ok(())
}

/// Rows per key of the ordered refresh fixtures.
const ORDERED_ROWS_PER_KEY: u64 = 3;

/// The first raw order value of every key, and the distance between a
/// key's rows.
const ORDER_START: i64 = 1_000;
const ORDER_STEP: i64 = 10;

/// Ids of the ordered refresh fixtures: forty keys over two buckets.
const ORDERED_IDS: Range<u64> = 0..40;

/// Rows per page of the ordered fixtures: far fewer than a file holds, so
/// every file's order column has many pages.
const ORDERED_PAGE_ROWS: usize = 4;

/// The id the ordered tests append beside, update, and repeat.
const AN_ID: u64 = 17;

/// Rows per key of the late-row fixture: a run of many pages, each page's
/// order values a narrow slice of it, so the order fences rule out every
/// page of the run but those a late row can fall among.
const LONG_RUN_ROWS: u64 = 40;

/// The raw order value of a key's `index`-th row, of whatever fixture
/// `ordered_spec` writes.
fn order_of(index: u64) -> i64 {
    ordered(ORDERED_ROWS_PER_KEY)
        .order_at(index)
        .expect("a row index's order value fits a long")
}

/// `rows_per_key` long order values per key, `ORDER_STEP` apart from
/// `ORDER_START`.
fn ordered(rows_per_key: u64) -> Ordered {
    Ordered::each(OrderKind::Long, rows_per_key, ORDER_START, ORDER_STEP)
}

/// `ORDERED_IDS` over two buckets, `rows_per_key` long order values per
/// key, in pages of `ORDERED_PAGE_ROWS` rows.
fn ordered_spec(name: &str, rows_per_key: u64) -> FixtureSpec {
    let ordered = ordered(rows_per_key);
    FixtureSpec {
        ids: ORDERED_IDS,
        buckets: TWO_BUCKETS,
        layout: Layout {
            page_rows: ORDERED_PAGE_ROWS,
            ..Layout::contract()
        },
        ..FixtureSpec::ordered(name, KeyType::Long, ordered)
    }
}

/// An ordered fixture written through `catalog` with its hint, served in
/// `residency` from its root over storage whose local filesystem store
/// counts GETs per object, with a local directory under `dir` in the Disk
/// tier.
async fn serve_counted(
    fixture: &Fixture,
    residency: Residency,
    dir: &std::path::Path,
) -> Result<(Served, Arc<CountingStore>)> {
    let counting = Arc::new(CountingStore::local());
    let storage = Arc::new(Storage::new(verify::storage_config_for_tests()));
    let store: Arc<dyn ObjectStore> = Arc::<CountingStore>::clone(&counting);
    storage.register(FILE_PREFIX, store);
    let local = verify::local_for(residency, dir, &storage)?;
    let source = fixture.root_source();
    let config = TableConfig {
        source: source.clone(),
        ..fixture.config(residency, false)
    };
    let served = verify::serve_over(
        storage,
        config,
        TableSource::from_config(&source).await?,
        local,
    )
    .await?;
    Ok((served, counting))
}

/// GETs so far of every file of `generation`, by path.
fn gets_by_path(generation: &Generation, counting: &CountingStore) -> Vec<(String, u64)> {
    generation
        .files
        .iter()
        .map(|file| (file.entry.path.clone(), counting.gets_to(&file.entry.path)))
        .collect()
}

/// A refresh that appends rows later than every row the table holds reads
/// only its new files (RFD 025, Refresh): no new file's order range meets a
/// reused one's, so no merge runs and no reused file is read again.
#[tokio::test]
async fn test_refresh_reads_only_new_files_for_an_append_of_later_rows() -> Result<()> {
    for (at, residency) in Residency::ALL.into_iter().enumerate() {
        let (_dir, catalog) = local_catalog().await?;
        let spec = ordered_spec(&format!("ord_later_{at}"), ORDERED_ROWS_PER_KEY);
        let mut fixture = write_fixture(catalog.as_ref(), NAMESPACE, spec).await?;
        fixture.write_hint().await?;
        let local = tempfile::tempdir()?;
        let (served, counting) = serve_counted(&fixture, residency, local.path()).await?;
        let before = gets_by_path(&served.current(), &counting);

        let later = order_of(ORDERED_ROWS_PER_KEY);
        let rows: Vec<(u64, i64)> = ORDERED_IDS.map(|id| (id, later)).collect();
        fixture
            .append_ordered(catalog.as_ref(), &rows, "later")
            .await?;
        fixture.write_hint().await?;
        assert!(served.table.refresh().await?, "{residency:?}");
        let generation = served.current();
        for (path, gets) in &before {
            assert_eq!(
                counting.gets_to(path),
                *gets,
                "{residency:?}: {path} is not read again"
            );
        }
        let new: Vec<&Arc<LoadedFile>> = generation
            .files
            .iter()
            .filter(|file| !before.iter().any(|(path, _)| *path == file.entry.path))
            .collect();
        assert_eq!(new.len(), approx::length(TWO_BUCKETS), "{residency:?}");
        for file in new {
            assert!(
                counting.gets_to(&file.entry.path) > 0,
                "{residency:?}: the new file {} is read",
                file.entry.path
            );
        }
        assert_eq!(generation.overlap_groups_verified(), 0, "{residency:?}");
    }
    Ok(())
}

/// A row appended inside an existing file's order range costs that file's
/// order pages the row falls among (RFD 025, Refresh): under `full` a GET
/// of each, fewer than the pages its key's run spans, which the order
/// fences rule out but for those; under `bloom` those and the key pages
/// they span; under `disk` none, the pages read from the chunk file. The
/// file of the other bucket, which cannot share the key, is not read.
#[tokio::test]
async fn test_refresh_reads_only_the_order_pages_late_rows_fall_among() -> Result<()> {
    for (at, residency) in Residency::ALL.into_iter().enumerate() {
        let spec = ordered_spec(&format!("ord_late_{at}"), LONG_RUN_ROWS);
        let late = refresh_with_a_late_row(spec, residency).await?;
        assert_eq!(
            late.other_gets, 0,
            "{residency:?}: no other bucket shares the key"
        );
        let run_pages = long_run_pages();
        match residency {
            Residency::Full => assert!(
                late.holding_gets > 0 && late.holding_gets < run_pages,
                "{residency:?}: {} order pages of a run of {run_pages}",
                late.holding_gets
            ),
            Residency::Bloom => assert!(
                late.holding_gets > 0 && late.holding_gets < run_pages,
                "{residency:?}: {} order and key pages of a run of {run_pages}",
                late.holding_gets
            ),
            Residency::Disk => {
                assert_eq!(
                    late.holding_gets, 0,
                    "{residency:?}: the chunk file serves the pages"
                );
                assert!(late.local_reads > 0);
            }
        }
    }
    Ok(())
}

/// Without a column index on the order column, a row appended inside an
/// existing file's order range costs the order pages it falls among, as
/// with one (RFD 025, Refresh): the order fences choose the pages, and the
/// writer's column index is not read.
#[tokio::test]
async fn test_refresh_reads_the_same_order_pages_without_a_column_index() -> Result<()> {
    let indexed =
        refresh_with_a_late_row(ordered_spec("ord_indexed", LONG_RUN_ROWS), Residency::Full)
            .await?;
    let spec = ordered_spec("ord_unindexed", LONG_RUN_ROWS);
    let spec = FixtureSpec {
        layout: Layout {
            order_statistics: Statistics::None,
            ..spec.layout
        },
        ..spec
    };
    let late = refresh_with_a_late_row(spec, Residency::Full).await?;
    assert_eq!(late.other_gets, 0);
    assert_eq!(late.holding_gets, indexed.holding_gets);
    assert!(
        late.holding_gets > 0 && late.holding_gets < long_run_pages(),
        "{} order pages of a run of {}",
        late.holding_gets,
        long_run_pages()
    );
    Ok(())
}

/// A dictionary-encoded order column's pages decode with the chunk's
/// dictionary page, which the refresh fetches beside them in every tier
/// that fetches pages: under `full` at least one order page and the
/// dictionary page, still fewer than the key's run fills.
#[tokio::test]
async fn test_refresh_reads_the_order_dictionary_with_the_pages_late_rows_fall_among() -> Result<()>
{
    for (at, residency) in Residency::ALL.into_iter().enumerate() {
        let spec = ordered_spec(&format!("ord_dictionary_{at}"), LONG_RUN_ROWS);
        let spec = FixtureSpec {
            layout: Layout {
                order_dictionary: true,
                ..spec.layout
            },
            ..spec
        };
        let late = refresh_with_a_late_row(spec, residency).await?;
        assert_eq!(late.other_gets, 0);
        match residency {
            Residency::Full | Residency::Bloom => assert!(
                late.holding_gets > 1 && late.holding_gets < long_run_pages(),
                "{residency:?}: {} pages",
                late.holding_gets
            ),
            Residency::Disk => assert_eq!(late.holding_gets, 0),
        }
    }
    Ok(())
}

/// Late rows of `AN_ID` that fall on one order page of the file holding
/// it: both between its second row and its third, which share a page.
fn one_page_lates() -> [i64; 2] {
    [order_of(1) + 1, order_of(1) + 2]
}

/// The GETs of the one order page late rows fall on under `full`, which
/// holds the key chunks and writes the order column without a dictionary.
const ONE_ORDER_PAGE: u64 = 1;

/// Two new files of one refresh whose rows fall on the same order page of
/// a file the table held read that page once (RFD 025, Refresh): under
/// `full` one GET; under `bloom` the page and the key pages it spans, as
/// many as one late row costs; under `disk` none.
#[tokio::test]
async fn test_late_rows_read_each_touched_old_page_once() -> Result<()> {
    for (at, residency) in Residency::ALL.into_iter().enumerate() {
        let spec = ordered_spec(&format!("ord_late_once_{at}"), LONG_RUN_ROWS);
        let lates = one_page_lates();
        let one = refresh_with_late_rows(spec, residency, &lates[..1]).await?;
        let spec = ordered_spec(&format!("ord_late_twice_{at}"), LONG_RUN_ROWS);
        let two = refresh_with_late_rows(spec, residency, &lates).await?;
        assert_eq!(two.other_gets, 0, "{residency:?}");
        match residency {
            Residency::Full => assert_eq!(two.holding_gets, ONE_ORDER_PAGE),
            Residency::Bloom => assert!(
                two.holding_gets > ONE_ORDER_PAGE && two.holding_gets == one.holding_gets,
                "{residency:?}: {} pages for two late files, {} for one",
                two.holding_gets,
                one.holding_gets
            ),
            Residency::Disk => assert_eq!(two.holding_gets, 0),
        }
    }
    Ok(())
}

/// The order pages a run of `LONG_RUN_ROWS` rows fills, at least.
fn long_run_pages() -> u64 {
    approx::count(
        usize::try_from(LONG_RUN_ROWS).expect("a run's rows fit usize") / ORDERED_PAGE_ROWS,
    )
}

/// What a refresh that appends one row inside `AN_ID`'s run read.
struct LateRead {
    /// GETs of the file holding the key.
    holding_gets: u64,
    /// GETs of every other file the table held before.
    other_gets: u64,
    /// Local reads across the refresh.
    local_reads: u64,
}

/// `spec` written and served in `residency`, then a row of `AN_ID` between
/// its second and third appended and the table refreshed; what the refresh
/// read of the files it held before.
async fn refresh_with_a_late_row(spec: FixtureSpec, residency: Residency) -> Result<LateRead> {
    refresh_with_late_rows(spec, residency, &[order_of(1) + 1]).await
}

/// `refresh_with_a_late_row` with a row of `AN_ID` at each raw order value
/// of `lates`, each appended as a file of its own before one refresh.
async fn refresh_with_late_rows(
    spec: FixtureSpec,
    residency: Residency,
    lates: &[i64],
) -> Result<LateRead> {
    let (_dir, catalog) = local_catalog().await?;
    let mut fixture = write_fixture(catalog.as_ref(), NAMESPACE, spec).await?;
    fixture.write_hint().await?;
    let local = tempfile::tempdir()?;
    let (served, counting) = serve_counted(&fixture, residency, local.path()).await?;
    let before = gets_by_path(&served.current(), &counting);
    let holding = MergeOracle::read(&fixture)
        .await?
        .copies(&fixture.key(AN_ID))[0]
        .path
        .clone();
    assert!(before.iter().any(|(path, _)| *path == holding));
    let local_reads = served.storage.stats().local_reads();
    for (at, &late) in lates.iter().enumerate() {
        fixture
            .append_ordered(catalog.as_ref(), &[(AN_ID, late)], &format!("late{at}"))
            .await?;
    }
    fixture.write_hint().await?;
    assert!(served.table.refresh().await?, "{residency:?}");
    assert_eq!(served.current().overlap_groups_verified(), 1);
    let read = |(path, gets): &(String, u64)| counting.gets_to(path) - gets;
    Ok(LateRead {
        holding_gets: before
            .iter()
            .filter(|(path, _)| *path == holding)
            .map(read)
            .sum(),
        other_gets: before
            .iter()
            .filter(|(path, _)| *path != holding)
            .map(read)
            .sum(),
        local_reads: served.storage.stats().local_reads() - local_reads,
    })
}

/// Two files a refresh adds that repeat a pair the table did not hold are
/// checked against each other (RFD 025, Refresh), and the refusal names the
/// later of them in file order; the last generation keeps serving.
#[tokio::test]
async fn test_new_files_of_one_refresh_are_checked_against_each_other() -> Result<()> {
    for (at, residency) in Residency::ALL.into_iter().enumerate() {
        let (_dir, catalog) = local_catalog().await?;
        let spec = ordered_spec(&format!("ord_twice_{at}"), ORDERED_ROWS_PER_KEY);
        let mut fixture = write_fixture(catalog.as_ref(), NAMESPACE, spec).await?;
        fixture.write_hint().await?;
        let local = tempfile::tempdir()?;
        let (served, _counting) = serve_counted(&fixture, residency, local.path()).await?;
        let v1 = served.current().snapshot_id;

        let id = ORDERED_IDS.end;
        let first = fixture.write_rows(&[id], FIRST_VERSION, "first").await?;
        let second = fixture.write_rows(&[id], FIRST_VERSION, "second").await?;
        let later = [&first, &second]
            .map(|files| files[0].file_path().to_string())
            .into_iter()
            .max()
            .expect("two files");
        let mut data = fixture.files().to_vec();
        data.extend(first);
        data.extend(second);
        fixture
            .commit_by_hand(HandSnapshot {
                data,
                deletes: fixture.deletes().to_vec(),
            })
            .await?;
        let error = served.table.refresh().await.unwrap_err();
        let (path, key, order) = verify::refused_duplicate_rows(&error);
        assert_eq!(
            path, later,
            "{residency:?}: both start at the key, so by path"
        );
        assert_eq!(*key, fixture.key(id));
        assert_eq!(*order, OrderValue::Long(order_of(0)));
        assert_eq!(
            served.current().snapshot_id,
            v1,
            "the last generation serves"
        );
    }
    Ok(())
}

/// An update's new copy of a pair, beside its older copy deleted by a
/// deletion vector, is no repeat (RFD 021, RFD 025): the refresh serves.
#[tokio::test]
async fn test_a_pair_whose_older_copy_is_deleted_is_served() -> Result<()> {
    for (at, residency) in Residency::ALL.into_iter().enumerate() {
        let (_dir, catalog) = local_catalog().await?;
        let spec = ordered_spec(&format!("ord_updated_{at}"), ORDERED_ROWS_PER_KEY);
        let mut fixture = write_fixture(catalog.as_ref(), NAMESPACE, spec).await?;
        fixture.write_hint().await?;
        let local = tempfile::tempdir()?;
        let (served, _counting) = serve_counted(&fixture, residency, local.path()).await?;
        let v1 = served.current();

        fixture
            .update_rows(&[(AN_ID, order_of(1))], DeleteForm::Vectors, "update")
            .await?;
        assert!(served.table.refresh().await?, "{residency:?}");
        assert!(matches!(served.table.state(), TableState::Serving { .. }));
        let generation = served.current();
        assert_eq!(generation.files.len(), v1.files.len() + 1, "{residency:?}");
        assert_eq!(
            generation.overlap_groups_verified(),
            1,
            "{residency:?}: the new file is merged against the file it shares a pair with"
        );
    }
    Ok(())
}

/// A new file holding a pair the table holds live, its own copy deleted
/// by a deletion vector committed with it, is no repeat (RFD 021, RFD
/// 025): its deleted rows are not live, the refresh merges it against the
/// file holding the live copy, and the table serves. The same file
/// committed without the vector repeats every pair it holds, and that
/// refresh is `duplicate_rows`, naming the new file, later in file order,
/// at the key's first pair, while the last generation keeps serving.
#[tokio::test]
async fn test_a_pair_whose_new_copy_is_deleted_is_served() -> Result<()> {
    for (at, residency) in Residency::ALL.into_iter().enumerate() {
        let (_dir, catalog) = local_catalog().await?;
        let spec = ordered_spec(&format!("ord_new_deleted_{at}"), ORDERED_ROWS_PER_KEY);
        let mut fixture = write_fixture(catalog.as_ref(), NAMESPACE, spec).await?;
        fixture.write_hint().await?;
        let local = tempfile::tempdir()?;
        let (served, _counting) = serve_counted(&fixture, residency, local.path()).await?;
        let v1 = served.current();
        let deletes = fixture.deletes().to_vec();

        let copy = fixture.write_rows(&[AN_ID], FIRST_VERSION, "copy").await?;
        assert_eq!(copy.len(), 1, "one key, one file");
        let new_file = copy[0].file_path().to_string();
        let vector = write_vectors(
            fixture.file_io(),
            &fixture.delete_path("copy.puffin"),
            &[Vector {
                data_file: new_file.clone(),
                positions: (0..ORDERED_ROWS_PER_KEY).collect::<RoaringTreemap>(),
                spec_id: fixture.metadata().default_partition_spec_id(),
                partition: copy[0].partition().clone(),
            }],
        )
        .await?;
        let mut data = fixture.files().to_vec();
        data.extend(copy);
        fixture
            .commit_by_hand(HandSnapshot {
                data: data.clone(),
                deletes: deletes.iter().cloned().chain(vector).collect(),
            })
            .await?;
        assert!(served.table.refresh().await?, "{residency:?}");
        assert!(matches!(served.table.state(), TableState::Serving { .. }));
        let deleted_copy = served.current();
        assert_eq!(
            deleted_copy.files.len(),
            v1.files.len() + 1,
            "{residency:?}"
        );
        assert_eq!(
            deleted_copy.overlap_groups_verified(),
            1,
            "{residency:?}: the new file is merged against the file holding the live copy"
        );

        fixture
            .commit_by_hand(HandSnapshot { data, deletes })
            .await?;
        let error = served.table.refresh().await.unwrap_err();
        let (path, key, order) = verify::refused_duplicate_rows(&error);
        assert_eq!(path, new_file, "{residency:?}");
        assert_eq!(*key, fixture.key(AN_ID));
        assert_eq!(*order, OrderValue::Long(order_of(0)));
        assert_eq!(
            served.current().snapshot_id,
            deleted_copy.snapshot_id,
            "the last generation serves"
        );
    }
    Ok(())
}
