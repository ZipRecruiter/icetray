//! Merge-on-read (RFD 021) over fixtures written with real delete files:
//! deletion vectors in Puffin files and Parquet position deletes, scoped
//! and not, served in the Full, Disk, and Bloom tiers, each over its own
//! storage. Every answer is checked against the brute-force oracle
//! (`MergeOracle`), every lookup's GETs against the cost the generation's
//! plan and probe order predict, and the loads' delete reads are counted.

use std::collections::HashSet;
use std::path::Path;
use std::sync::Arc;

use iceberg::puffin::DELETION_VECTOR_V1;
use iceberg::spec::{DataFile, FormatVersion, Struct};
use icetray_core::ColumnRead;
use icetray_core::approx::count;
use icetray_core::config::{Residency, TableConfig};
use icetray_core::{
    BlobFraming, Budget, DeleteFault, Error, ErrorKind, FileFault, Generation, Key, KeyType,
    RefusalReason, Row, TableSource, TableState,
};
use icetray_testkit::delete_files::{
    PositionDeletes, RawBlob, VECTOR_FRAME_BYTES, Vector, encode_vector, vector_entry,
    vector_properties, write_position_deletes, write_puffin, write_vectors,
};
use icetray_testkit::fixture::{
    IDS_2K, NAMESPACE, STRAY_IDS, TWO_BUCKETS, UNPARTITIONED_FILE_BYTES, local_catalog,
};
use icetray_testkit::keys::FIRST_VERSION;
use icetray_testkit::layout::VALUE_COLUMN;
use icetray_testkit::verify::{self, HINT_POLL_GETS, METADATA_FILE_GETS, SeenPages, Served};
use icetray_testkit::{
    DeleteForm, Fixture, FixtureSpec, HandSnapshot, Layout, MergeOracle, Result, TestkitError,
    write_fixture,
};
use roaring::RoaringTreemap;

/// Ids the cases delete or update: two in the first thousand-row file, one
/// in the second.
const FIRST_FILE_IDS: [u64; 2] = [5, 640];
const SECOND_FILE_ID: u64 = 1_500;

/// Every id the cases touch.
const TOUCHED: [u64; 3] = [FIRST_FILE_IDS[0], FIRST_FILE_IDS[1], SECOND_FILE_ID];

/// The GETs of a by-hand snapshot's manifests when it lists deletes: the
/// manifest list, the data manifest, and the delete manifest.
const MANIFEST_GETS: u64 = 3;

/// The GETs a new deletion vector costs: its Puffin footer, one tail read,
/// and its blob.
const VECTOR_GETS: u64 = 2;

/// The GETs a new position delete file costs: the whole file.
const POSITION_FILE_GETS: u64 = 1;

/// The GETs of a single-row-group data file's footer.
const FOOTER_GETS: u64 = 1;

/// Rows per row group of the many-row-group fixture, so a thousand-row
/// file holds four.
const SMALL_ROW_GROUP_ROWS: usize = 300;

/// An unpartitioned `IDS_2K` fixture of thousand-row files at
/// `format_version` under `layout`'s pages and row groups.
async fn fixture_with(
    catalog: &dyn iceberg::Catalog,
    name: &str,
    format_version: FormatVersion,
    row_group_rows: Option<usize>,
) -> Result<Fixture> {
    let spec = FixtureSpec {
        ids: IDS_2K,
        buckets: 0,
        format_version,
        layout: Layout {
            target_file_bytes: UNPARTITIONED_FILE_BYTES,
            row_group_rows,
            ..Layout::contract()
        },
        ..FixtureSpec::small(name, KeyType::Long)
    };
    let fixture = write_fixture(catalog, NAMESPACE, spec).await?;
    assert_eq!(fixture.file_count(), 2, "two thousand-row files");
    Ok(fixture)
}

/// `fixture_with` at format v2 with one row group per file.
async fn fixture(catalog: &dyn iceberg::Catalog, name: &str) -> Result<Fixture> {
    fixture_with(catalog, name, FormatVersion::V2, None).await
}

/// One fixture served in each tier through its root, each over its own
/// storage and cache, with the pages each tier's lookups have fetched; the
/// Disk tier's local directory lives in `scratch`.
struct Tiers {
    served: Vec<(Residency, Served)>,
    seen: Vec<SeenPages>,
    scratch: tempfile::TempDir,
}

impl Tiers {
    /// `fixture` served in every tier.
    async fn serve(fixture: &Fixture) -> Result<Self> {
        let scratch = tempfile::tempdir()?;
        let mut tiers = Vec::new();
        for residency in Residency::ALL {
            tiers.push((
                residency,
                serve_in(fixture, residency, scratch.path(), None).await?,
            ));
        }
        Ok(Self {
            seen: tiers.iter().map(|_| SeenPages::default()).collect(),
            served: tiers,
            scratch,
        })
    }

    /// The Full tier's table, whose plan names value pages.
    fn full(&self) -> &Served {
        &self.served[0].1
    }

    /// Refreshes every tier; whether each swapped.
    async fn refresh(&self) -> Result<Vec<bool>> {
        let mut swapped = Vec::new();
        for (_, served) in &self.served {
            swapped.push(served.table.refresh().await?);
        }
        Ok(swapped)
    }
}

/// `fixture` built in `residency` through its root over a fresh storage,
/// with a local directory under `scratch` for the Disk tier, and `budget`
/// as its resident budget; loaded.
async fn serve_in(
    fixture: &Fixture,
    residency: Residency,
    scratch: &Path,
    budget: Option<u64>,
) -> Result<Served> {
    built_in(fixture, residency, scratch, budget)
        .await?
        .load()
        .await
}

/// `serve_in` before its first load.
async fn built_in(
    fixture: &Fixture,
    residency: Residency,
    scratch: &Path,
    budget: Option<u64>,
) -> Result<Served> {
    let source = fixture.root_source();
    let config = TableConfig {
        source: source.clone(),
        ..fixture.config_with_budget(residency, false, budget)
    };
    verify::build_in(
        config,
        TableSource::from_config(&source).await?,
        &scratch.join(fixture.spec().name.as_str()),
        verify::TEST_CACHE_BYTES,
    )
    .await
}

/// Every key the oracle holds, live or deleted, and the absent key after
/// each touched id.
fn keys_to_check(fixture: &Fixture, oracle: &MergeOracle) -> Vec<Key> {
    let mut keys: Vec<Key> = oracle.keys().cloned().collect();
    keys.extend(TOUCHED.map(|id| fixture.absent_near(id)));
    keys
}

/// Every tier answers every key of `keys_to_check` as the oracle does, from
/// the fixture's current snapshot, at the GETs its probe order predicts
/// (`expected_gets`) past the pages its cache already holds.
async fn check(tiers: &mut Tiers, fixture: &Fixture) -> Result<MergeOracle> {
    let oracle = MergeOracle::read(fixture).await?;
    let keys = keys_to_check(fixture, &oracle);
    let full = tiers.full().current();
    for ((residency, served), seen) in tiers.served.iter().zip(&mut tiers.seen) {
        let generation = served.current();
        assert_eq!(
            Some(generation.snapshot_id.0),
            fixture.snapshot_id(),
            "{residency} serves the current snapshot"
        );
        let stats = served.storage.stats();
        for key in &keys {
            let expected = expected_gets(&full, &generation, &oracle, key, seen);
            let before = stats.gets();
            let answer = served
                .table
                .get(key, &[ColumnRead::whole(VALUE_COLUMN)])
                .await?
                .row;
            assert_eq!(answer, oracle.expected(key), "{residency} key {key}");
            assert_eq!(
                stats.gets() - before,
                expected,
                "{residency} GETs for key {key}"
            );
        }
    }
    Ok(oracle)
}

/// The GETs a lookup of `key` costs (`verify::expected_lookup_gets`),
/// once the Full tier's plan is shown to name the oracle's live copy of it,
/// skipping its deleted copies.
fn expected_gets(
    full: &Generation,
    generation: &Generation,
    oracle: &MergeOracle,
    key: &Key,
    seen: &mut SeenPages,
) -> u64 {
    let live = oracle.live_copy(key);
    if let Some(live) = live {
        let (file, _) = full.plan(std::slice::from_ref(key))[0]
            .clone()
            .expect("the plan names a live key's file");
        assert_eq!(file.entry.path, live.path, "the plan skips deleted copies");
    }
    verify::expected_lookup_gets(full, generation, VALUE_COLUMN, key, live.is_some(), seen)
}

/// The GETs `served` issues while `work` runs.
async fn gets_during<T>(served: &Served, work: impl Future<Output = T>) -> (T, u64) {
    let stats = served.storage.stats();
    let before = stats.gets();
    let out = work.await;
    (out, stats.gets() - before)
}

/// A row cached before a position delete removes it is not served after
/// the refresh that applies the delete: a lookup checks the row against
/// its file's deletes before it asks the row cache (RFD 026), and the
/// entry itself is never invalidated.
#[tokio::test]
async fn test_a_deleted_row_is_not_served_from_the_row_cache_after_refresh() -> Result<()> {
    let (_dir, catalog) = local_catalog().await?;
    let mut fixture = fixture(catalog.as_ref(), "kv_row_cache_delete").await?;
    fixture.write_hint().await?;
    let served =
        verify::serve_root_reading_rows(&fixture, fixture.config(Residency::Full, false)).await?;
    let id = FIRST_FILE_IDS[0];
    verify::verify_hits(&served, &fixture, &[id]).await?;
    let cached = served.rows().usage();
    assert!(cached > 0, "the row is cached");
    fixture
        .delete(&[id], DeleteForm::Positions, "deleted")
        .await?;
    assert!(served.table.refresh().await?);
    assert_eq!(
        served
            .table
            .get(&fixture.key(id), &[ColumnRead::whole(VALUE_COLUMN)])
            .await?
            .row,
        Row::Absent
    );
    assert_eq!(
        served.rows().usage(),
        cached,
        "the entry stays, never asked for"
    );
    Ok(())
}

#[tokio::test]
async fn test_each_delete_kind_is_served_in_every_tier() -> Result<()> {
    let forms = [
        (DeleteForm::Vectors, 2 * VECTOR_GETS - 1),
        (DeleteForm::ScopedPositions, 2 * POSITION_FILE_GETS),
        (DeleteForm::Positions, POSITION_FILE_GETS),
    ];
    for (at, (form, delete_gets)) in forms.into_iter().enumerate() {
        let (_dir, catalog) = local_catalog().await?;
        let mut fixture = fixture(catalog.as_ref(), &format!("kv_kind_{at}")).await?;
        fixture.delete(&TOUCHED, form, "deletes").await?;
        let mut tiers = Tiers::serve(&fixture).await?;
        let oracle = check(&mut tiers, &fixture).await?;
        assert_eq!(oracle.deleted_rows(), count(TOUCHED.len()), "{form:?}");
        for id in TOUCHED {
            assert_eq!(oracle.expected(&fixture.key(id)), Row::Absent);
        }
        // One Puffin file holds both vectors: one footer, two blobs.
        let fresh = built_in(&fixture, Residency::Full, tiers.scratch.path(), None).await?;
        let (loaded, gets) = gets_during(&fresh, fresh.table.load()).await;
        loaded?;
        assert_eq!(
            gets,
            HINT_POLL_GETS
                + METADATA_FILE_GETS
                + MANIFEST_GETS
                + 2 * verify::LOAD_GETS_PER_FILE
                + delete_gets,
            "{form:?}: the data files and the deletes once each"
        );
    }
    Ok(())
}

#[tokio::test]
async fn test_a_delete_committed_with_its_data_file_applies_to_it() -> Result<()> {
    let (_dir, catalog) = local_catalog().await?;
    let mut fixture = fixture(catalog.as_ref(), "kv_same_sequence").await?;
    let stray: Vec<u64> = STRAY_IDS.collect();
    let written = fixture.write_rows(&stray, FIRST_VERSION, "stray").await?;
    assert_eq!(written.len(), 1);
    let new_file = written[0].file_path().to_string();
    let deleted: RoaringTreemap = [0, 7].into_iter().collect();
    let vectors = write_vectors(
        fixture.file_io(),
        &fixture.delete_path("same.puffin"),
        &[Vector {
            data_file: new_file,
            positions: deleted,
            spec_id: fixture.metadata().default_partition_spec_id(),
            partition: Struct::empty(),
        }],
    )
    .await?;
    let mut data = fixture.files().to_vec();
    data.extend(written);
    fixture
        .commit_by_hand(HandSnapshot {
            data,
            deletes: vectors,
        })
        .await?;
    let mut tiers = Tiers::serve(&fixture).await?;
    let oracle = check(&mut tiers, &fixture).await?;
    for (at, id) in stray.iter().enumerate() {
        let expected_live = at != 0 && at != 7;
        assert_eq!(
            oracle.live_copy(&fixture.key(*id)).is_some(),
            expected_live,
            "id {id}"
        );
    }
    Ok(())
}

#[tokio::test]
async fn test_a_vector_beside_position_deletes_masks_them() -> Result<()> {
    let (_dir, catalog) = local_catalog().await?;
    let mut fixture = fixture(catalog.as_ref(), "kv_masked").await?;
    let [masked, kept] = FIRST_FILE_IDS;
    fixture
        .delete(&[masked], DeleteForm::ScopedPositions, "positions")
        .await?;
    let before = MergeOracle::read(&fixture).await?;
    let copy = before.copies(&fixture.key(kept))[0].clone();
    // A vector for the same file naming only `kept`: a writer that broke
    // the rule to fold the position deletes in. The spec ignores them.
    let mut deletes = fixture.deletes().to_vec();
    deletes.extend(
        write_vectors(
            fixture.file_io(),
            &fixture.delete_path("vector.puffin"),
            &[Vector {
                data_file: copy.path.clone(),
                positions: [copy.position].into_iter().collect(),
                spec_id: fixture.metadata().default_partition_spec_id(),
                partition: Struct::empty(),
            }],
        )
        .await?,
    );
    fixture
        .commit_by_hand(HandSnapshot {
            data: fixture.files().to_vec(),
            deletes,
        })
        .await?;
    let mut tiers = Tiers::serve(&fixture).await?;
    let oracle = check(&mut tiers, &fixture).await?;
    assert_eq!(oracle.expected(&fixture.key(kept)), Row::Absent);
    assert_ne!(
        oracle.expected(&fixture.key(masked)),
        Row::Absent,
        "the vector masks the position delete"
    );
    Ok(())
}

#[tokio::test]
async fn test_a_puffin_file_with_a_replaced_vector_reads_its_live_blob_alone() -> Result<()> {
    let (_dir, catalog) = local_catalog().await?;
    let mut fixture =
        fixture_with(catalog.as_ref(), "kv_replaced", FormatVersion::V3, None).await?;
    fixture
        .delete(
            &[FIRST_FILE_IDS[0], SECOND_FILE_ID],
            DeleteForm::Vectors,
            "first",
        )
        .await?;
    fixture
        .delete(&[FIRST_FILE_IDS[1]], DeleteForm::Vectors, "second")
        .await?;
    let first = fixture.delete_path("first.puffin");
    assert_eq!(
        fixture
            .deletes()
            .iter()
            .filter(|delete| delete.file_path() == first)
            .count(),
        1,
        "the first Puffin file holds a replaced vector and a live one"
    );
    let mut tiers = Tiers::serve(&fixture).await?;
    let oracle = check(&mut tiers, &fixture).await?;
    assert_eq!(oracle.deleted_rows(), count(TOUCHED.len()));
    let fresh = built_in(&fixture, Residency::Full, tiers.scratch.path(), None).await?;
    let (loaded, gets) = gets_during(&fresh, fresh.table.load()).await;
    loaded?;
    assert_eq!(
        gets,
        HINT_POLL_GETS
            + METADATA_FILE_GETS
            + MANIFEST_GETS
            + 2 * verify::LOAD_GETS_PER_FILE
            + 2 * VECTOR_GETS,
        "each Puffin file's footer and its one live blob"
    );
    Ok(())
}

#[tokio::test]
async fn test_dangling_deletes_are_read_at_most_once() -> Result<()> {
    let (_dir, catalog) = local_catalog().await?;
    let mut fixture = fixture(catalog.as_ref(), "kv_dangling").await?;
    fixture
        .delete(&[SECOND_FILE_ID], DeleteForm::Vectors, "vector")
        .await?;
    fixture
        .delete(&[SECOND_FILE_ID + 1], DeleteForm::Positions, "positions")
        .await?;
    // The second file leaves the snapshot and both deletes stay listed,
    // naming a file it no longer holds; the first file is older than both,
    // so only what they name tells whether they apply to it.
    let kept = fixture.files()[0].clone();
    assert_eq!(
        MergeOracle::read(&fixture).await?.copies(&fixture.key(0))[0].path,
        kept.file_path()
    );
    fixture
        .commit_by_hand(HandSnapshot {
            data: vec![kept],
            deletes: fixture.deletes().to_vec(),
        })
        .await?;
    assert_eq!(fixture.deletes().len(), 2);
    let mut tiers = Tiers::serve(&fixture).await?;
    let oracle = check(&mut tiers, &fixture).await?;
    assert_eq!(oracle.deleted_rows(), 0);
    let fresh = built_in(&fixture, Residency::Full, tiers.scratch.path(), None).await?;
    let (loaded, gets) = gets_during(&fresh, fresh.table.load()).await;
    loaded?;
    assert_eq!(
        gets,
        HINT_POLL_GETS
            + METADATA_FILE_GETS
            + MANIFEST_GETS
            + verify::LOAD_GETS_PER_FILE
            + POSITION_FILE_GETS,
        "the unscoped position file, to learn it names nothing held; never the vector"
    );
    // A commit that changes nothing: the reload reads no data file and no
    // delete.
    fixture
        .commit_by_hand(HandSnapshot {
            data: fixture.files().to_vec(),
            deletes: fixture.deletes().to_vec(),
        })
        .await?;
    let (swapped, gets) = gets_during(&fresh, fresh.table.refresh()).await;
    assert!(swapped?);
    assert_eq!(
        gets,
        HINT_POLL_GETS + METADATA_FILE_GETS + MANIFEST_GETS,
        "the unscoped file is remembered by its path"
    );
    assert!(tiers.refresh().await?.into_iter().all(|swapped| swapped));
    check(&mut tiers, &fixture).await?;
    Ok(())
}

#[tokio::test]
async fn test_deletes_older_than_every_data_file_are_never_read() -> Result<()> {
    let (_dir, catalog) = local_catalog().await?;
    let mut fixture = fixture(catalog.as_ref(), "kv_compacted").await?;
    fixture
        .delete(&[FIRST_FILE_IDS[0]], DeleteForm::Vectors, "vector")
        .await?;
    fixture
        .delete(&[SECOND_FILE_ID], DeleteForm::Positions, "positions")
        .await?;
    // A rewrite drops the deleted rows and keeps both deletes listed, now
    // older than every data file.
    fixture.compact("compacted").await?;
    assert_eq!(fixture.deletes().len(), 2);
    let mut tiers = Tiers::serve(&fixture).await?;
    let oracle = check(&mut tiers, &fixture).await?;
    assert_eq!(oracle.deleted_rows(), 0);
    let fresh = built_in(&fixture, Residency::Full, tiers.scratch.path(), None).await?;
    let (loaded, gets) = gets_during(&fresh, fresh.table.load()).await;
    loaded?;
    assert_eq!(
        gets,
        HINT_POLL_GETS
            + METADATA_FILE_GETS
            + MANIFEST_GETS
            + count(fixture.file_count()) * verify::LOAD_GETS_PER_FILE,
        "the manifests decide that neither applies"
    );
    Ok(())
}

#[tokio::test]
async fn test_a_deleted_row_in_a_later_row_group_counts_from_the_files_first_row() -> Result<()> {
    let (_dir, catalog) = local_catalog().await?;
    let mut fixture = fixture_with(
        catalog.as_ref(),
        "kv_row_groups",
        FormatVersion::V2,
        Some(SMALL_ROW_GROUP_ROWS),
    )
    .await?;
    // Ids of the first file's last row group, and its last row.
    let late = [3 * 300 + 5, 999];
    fixture.delete(&late, DeleteForm::Vectors, "late").await?;
    let mut tiers = Tiers::serve(&fixture).await?;
    let generation = tiers.full().current();
    assert!(
        generation
            .files
            .iter()
            .all(|file| file.index.metadata().num_row_groups() > 1),
        "every file has several row groups"
    );
    let oracle = check(&mut tiers, &fixture).await?;
    for id in late {
        assert_eq!(oracle.expected(&fixture.key(id)), Row::Absent);
        assert_ne!(oracle.expected(&fixture.key(id - 1)), Row::Absent);
    }
    Ok(())
}

#[tokio::test]
async fn test_an_update_answers_from_the_newest_copy_on_the_first_probe() -> Result<()> {
    let (_dir, catalog) = local_catalog().await?;
    let mut fixture = fixture(catalog.as_ref(), "kv_update").await?;
    fixture
        .update(&TOUCHED, DeleteForm::Vectors, "updated")
        .await?;
    let mut tiers = Tiers::serve(&fixture).await?;
    let oracle = check(&mut tiers, &fixture).await?;
    for id in TOUCHED {
        let key = fixture.key(id);
        assert_eq!(
            oracle.copies(&key).len(),
            2,
            "the deleted old row and the new one"
        );
        assert_eq!(oracle.expected(&key), fixture.expected(&key));
        let live = oracle.live_copy(&key).unwrap();
        for (residency, served) in &tiers.served {
            let generation = served.current();
            let first = generation.candidates(&key).next().unwrap();
            assert_eq!(first.entry.path, live.path, "{residency}: newest first");
        }
    }
    Ok(())
}

#[tokio::test]
async fn test_a_deleted_keys_bloom_miss_costs_a_key_page_per_deleted_copy() -> Result<()> {
    let (_dir, catalog) = local_catalog().await?;
    let mut fixture = fixture(catalog.as_ref(), "kv_bloom_miss").await?;
    let [id, _] = FIRST_FILE_IDS;
    fixture
        .update(&[id], DeleteForm::Vectors, "updated")
        .await?;
    fixture
        .delete(&[id], DeleteForm::Vectors, "deleted")
        .await?;
    let scratch = tempfile::tempdir()?;
    let bloom = serve_in(&fixture, Residency::Bloom, scratch.path(), None).await?;
    let oracle = MergeOracle::read(&fixture).await?;
    let key = fixture.key(id);
    let copies: HashSet<&str> = oracle
        .copies(&key)
        .iter()
        .map(|copy| copy.path.as_str())
        .collect();
    assert_eq!(copies.len(), 2, "two deleted copies");
    let generation = bloom.current();
    let positives: HashSet<&str> = generation
        .candidates(&key)
        .filter(|candidate| candidate.index.page_of(&key).is_some())
        .map(|candidate| candidate.entry.path.as_str())
        .collect();
    assert!(
        copies.is_subset(&positives),
        "a deleted row stays in its filter"
    );
    let (answer, gets) = gets_during(
        &bloom,
        bloom.table.get(&key, &[ColumnRead::whole(VALUE_COLUMN)]),
    )
    .await;
    assert_eq!(answer?.row, Row::Absent);
    assert_eq!(
        gets,
        count(positives.len()),
        "one key page per filter positive"
    );
    for (residency, extra) in [(Residency::Full, 0), (Residency::Disk, 0)] {
        let served = serve_in(&fixture, residency, scratch.path(), None).await?;
        let (answer, gets) = gets_during(
            &served,
            served.table.get(&key, &[ColumnRead::whole(VALUE_COLUMN)]),
        )
        .await;
        assert_eq!(answer?.row, Row::Absent);
        assert_eq!(gets, extra, "{residency}: a deleted key's miss");
    }
    Ok(())
}

#[tokio::test]
async fn test_a_delete_only_commit_reuses_every_file_and_reads_the_new_delete_alone() -> Result<()>
{
    let forms = [
        (DeleteForm::Vectors, VECTOR_GETS),
        (DeleteForm::ScopedPositions, POSITION_FILE_GETS),
        (DeleteForm::Positions, POSITION_FILE_GETS),
    ];
    for (at, (form, delete_gets)) in forms.into_iter().enumerate() {
        let (_dir, catalog) = local_catalog().await?;
        let mut fixture = fixture(catalog.as_ref(), &format!("kv_delete_only_{at}")).await?;
        fixture.delete(&[FIRST_FILE_IDS[0]], form, "first").await?;
        let mut tiers = Tiers::serve(&fixture).await?;
        let before: Vec<_> = tiers
            .served
            .iter()
            .map(|(_, served)| served.current())
            .collect();
        fixture.delete(&[FIRST_FILE_IDS[1]], form, "second").await?;
        for ((residency, served), previous) in tiers.served.iter().zip(&before) {
            let (swapped, gets) = gets_during(served, served.table.refresh()).await;
            assert!(swapped?);
            assert_eq!(
                gets,
                HINT_POLL_GETS + METADATA_FILE_GETS + MANIFEST_GETS + delete_gets,
                "{residency} {form:?}: the new delete alone"
            );
            let generation = served.current();
            assert!(
                generation
                    .files
                    .iter()
                    .zip(&previous.files)
                    .all(|(now, then)| Arc::ptr_eq(now, then)),
                "every data file reused"
            );
            assert_eq!(generation.overlap_groups_verified(), 0);
        }
        check(&mut tiers, &fixture).await?;
    }
    Ok(())
}

#[tokio::test]
async fn test_a_ref_moved_back_past_a_vector_refuses_two_live_copies() -> Result<()> {
    let (_dir, catalog) = local_catalog().await?;
    let mut fixture = fixture(catalog.as_ref(), "kv_moved_duplicate").await?;
    let id = FIRST_FILE_IDS[0];
    let older = MergeOracle::read(&fixture).await?.copies(&fixture.key(id))[0].clone();
    let mut data = fixture.files().to_vec();
    data.extend(fixture.write_rows(&[id], FIRST_VERSION + 1, "copy").await?);
    fixture
        .commit_by_hand(HandSnapshot {
            data,
            deletes: Vec::new(),
        })
        .await?;
    let duplicated = fixture.snapshot_id().unwrap();
    assert!(matches!(
        MergeOracle::read(&fixture).await,
        Err(TestkitError::Oracle(_))
    ));
    let vectors = write_vectors(
        fixture.file_io(),
        &fixture.delete_path("older.puffin"),
        &[Vector {
            data_file: older.path.clone(),
            positions: [older.position].into_iter().collect(),
            spec_id: fixture.metadata().default_partition_spec_id(),
            partition: Struct::empty(),
        }],
    )
    .await?;
    fixture
        .commit_by_hand(HandSnapshot {
            data: fixture.files().to_vec(),
            deletes: vectors,
        })
        .await?;
    let mut tiers = Tiers::serve(&fixture).await?;
    let oracle = check(&mut tiers, &fixture).await?;
    let served = oracle.expected(&fixture.key(id));
    fixture.move_ref(duplicated).await?;
    for (residency, table) in &tiers.served {
        let error = table.table.refresh().await.unwrap_err();
        let Error::Refused {
            reason: RefusalReason::DuplicateKeys { key, .. },
            ..
        } = error
        else {
            panic!("{residency}: expected duplicate keys, got {error:?}");
        };
        assert_eq!(key, fixture.key(id));
        assert_eq!(
            table
                .table
                .get(&fixture.key(id), &[ColumnRead::whole(VALUE_COLUMN)])
                .await?
                .row,
            served,
            "{residency} keeps serving the snapshot with the vector"
        );
        assert!(matches!(
            table.table.state(),
            TableState::Degraded {
                serving: Some(_),
                ..
            }
        ));
    }
    Ok(())
}

/// A deletion vector of consecutive rows is written as a run container, as
/// Java Iceberg writes one, and every tier serves the rows it deletes.
#[tokio::test]
async fn test_a_vector_of_consecutive_rows_serves_through_a_run_container() -> Result<()> {
    let (_dir, catalog) = local_catalog().await?;
    let mut fixture = fixture(catalog.as_ref(), "kv_run").await?;
    let run_rows: u64 = 200;
    let run: Vec<u64> = (FIRST_FILE_IDS[0]..FIRST_FILE_IDS[0] + run_rows).collect();
    let before = MergeOracle::read(&fixture).await?;
    let positions: Vec<u64> = run
        .iter()
        .map(|&id| before.copies(&fixture.key(id))[0].position)
        .collect();
    assert!(
        positions.windows(2).all(|pair| pair[1] == pair[0] + 1),
        "the run's rows are consecutive in one file"
    );
    fixture.delete(&run, DeleteForm::Vectors, "run").await?;
    // An array container takes two bytes a row; the run takes a few in all.
    let array_bytes = 2 * run_rows;
    let [vector] = fixture.deletes() else {
        panic!("one deletion vector, got {:?}", fixture.deletes());
    };
    let blob_bytes = u64::try_from(vector.content_size_in_bytes().unwrap()).unwrap();
    assert!(blob_bytes < array_bytes, "{blob_bytes} bytes");
    let mut tiers = Tiers::serve(&fixture).await?;
    let oracle = check(&mut tiers, &fixture).await?;
    for id in run {
        assert_eq!(oracle.expected(&fixture.key(id)), Row::Absent);
    }
    Ok(())
}

#[tokio::test]
async fn test_a_ref_moved_back_past_a_vector_serves_a_revived_only_copy() -> Result<()> {
    let (_dir, catalog) = local_catalog().await?;
    let mut fixture = fixture(catalog.as_ref(), "kv_moved_revived").await?;
    let before = fixture.snapshot_id().unwrap();
    fixture
        .delete(&TOUCHED, DeleteForm::Vectors, "deletes")
        .await?;
    let mut tiers = Tiers::serve(&fixture).await?;
    check(&mut tiers, &fixture).await?;
    fixture.move_ref(before).await?;
    assert!(tiers.refresh().await?.into_iter().all(|swapped| swapped));
    let oracle = check(&mut tiers, &fixture).await?;
    for id in TOUCHED {
        assert_eq!(
            oracle.expected(&fixture.key(id)),
            fixture.expected(&fixture.key(id))
        );
        assert_ne!(oracle.expected(&fixture.key(id)), Row::Absent);
    }
    Ok(())
}

#[tokio::test]
async fn test_a_full_budget_under_the_delete_bound_is_refused_before_any_delete_read() -> Result<()>
{
    let (_dir, catalog) = local_catalog().await?;
    let mut fixture = fixture(catalog.as_ref(), "kv_budget").await?;
    fixture.write_hint().await?;
    let scratch = tempfile::tempdir()?;
    let need = |reason: &RefusalReason| {
        let RefusalReason::OverBudget {
            of: Budget::Resident,
            needed,
            ..
        } = reason
        else {
            panic!("expected over budget, got {reason:?}");
        };
        *needed
    };
    let refused = async |served: &Served| {
        let (loaded, gets) = gets_during(served, served.table.load()).await;
        let Err(Error::Refused { reason, .. }) = loaded else {
            panic!("expected a refusal, got {loaded:?}");
        };
        (reason, gets)
    };
    let tiny = Some(1);
    let without = built_in(&fixture, Residency::Full, scratch.path(), tiny).await?;
    let data_bytes = need(&refused(&without).await.0);
    let even_ids: Vec<u64> = fixture.ids().filter(|id| id % 2 == 0).collect();
    fixture
        .delete(&even_ids, DeleteForm::Vectors, "many")
        .await?;
    let probe = built_in(&fixture, Residency::Full, scratch.path(), tiny).await?;
    let with_deletes = need(&refused(&probe).await.0);
    assert!(with_deletes > data_bytes, "the deletes' bound is counted");
    let held = built_in(&fixture, Residency::Full, scratch.path(), Some(data_bytes)).await?;
    let (reason, gets) = refused(&held).await;
    assert_eq!(
        reason,
        RefusalReason::OverBudget {
            of: Budget::Resident,
            needed: with_deletes,
            budget: data_bytes,
        }
    );
    assert_eq!(
        gets,
        HINT_POLL_GETS + METADATA_FILE_GETS + MANIFEST_GETS + 2 * FOOTER_GETS,
        "the footers alone: no key chunk and no delete"
    );
    let fits = serve_in(
        &fixture,
        Residency::Full,
        scratch.path(),
        Some(with_deletes),
    )
    .await?;
    assert!(fits.current().index_bytes > 0);
    Ok(())
}

/// A fixture whose second snapshot carries the delete `deletes` builds
/// from the fixture's files, served in the Full tier from its first: what
/// the fault cases refresh into.
async fn serving_then(
    name: &str,
    deletes: impl AsyncFnOnce(&Fixture) -> Result<Vec<DataFile>>,
) -> Result<(Fixture, Served, tempfile::TempDir)> {
    let (dir, catalog) = local_catalog().await?;
    let mut fixture = fixture(catalog.as_ref(), name).await?;
    fixture
        .commit_by_hand(HandSnapshot {
            data: fixture.files().to_vec(),
            deletes: Vec::new(),
        })
        .await?;
    let served = serve_in(&fixture, Residency::Full, dir.path(), None).await?;
    let entries = deletes(&fixture).await?;
    fixture
        .commit_by_hand(HandSnapshot {
            data: fixture.files().to_vec(),
            deletes: entries,
        })
        .await?;
    Ok((fixture, served, dir))
}

/// The delete fault a refresh of `served` ends in, and whether the next
/// poll reads its snapshot again (`true`) or remembers it (`false`).
async fn refresh_fault(served: &Served) -> Result<(String, DeleteFault, bool)> {
    let error = served
        .table
        .refresh()
        .await
        .expect_err("the snapshot's delete is a fault");
    assert_eq!(error.kind(), ErrorKind::File, "{error:?}");
    let Error::File {
        path,
        fault: FileFault::Delete(fault),
    } = error
    else {
        panic!("expected a delete fault, got {error:?}");
    };
    let (again, gets) = gets_during(served, served.table.refresh()).await;
    let retried = gets > HINT_POLL_GETS;
    if retried {
        assert!(again.is_err());
    } else {
        assert!(!again?, "a remembered snapshot is not built");
    }
    Ok((path, fault, retried))
}

/// The first data file of `fixture` and the vector of `positions` in it.
fn vector_of(fixture: &Fixture, positions: &[u64]) -> Vector {
    Vector {
        data_file: fixture.files()[0].file_path().to_string(),
        positions: positions.iter().copied().collect(),
        spec_id: fixture.metadata().default_partition_spec_id(),
        partition: Struct::empty(),
    }
}

/// One vector blob of `bytes` with `properties` written as the Puffin file
/// `name`, its manifest entry recording `cardinality` rows for `vector`.
async fn raw_vector(
    fixture: &Fixture,
    name: &str,
    bytes: Vec<u8>,
    properties: std::collections::HashMap<String, String>,
    vector: &Vector,
    cardinality: u64,
) -> Result<Vec<DataFile>> {
    let path = fixture.delete_path(name);
    let written = write_puffin(
        fixture.file_io(),
        &path,
        vec![RawBlob {
            blob_type: DELETION_VECTOR_V1.to_string(),
            bytes,
            properties,
        }],
    )
    .await?;
    Ok(vec![vector_entry(
        &path,
        written.size,
        &written.ranges[0],
        vector,
        cardinality,
    )?])
}

#[tokio::test]
async fn test_a_vector_blobs_framing_faults_are_transient() -> Result<()> {
    // `usize::MAX` stands for the blob's last byte, a byte of its CRC-32.
    let last_byte = usize::MAX;
    for (at, (flip, which)) in [
        (0, BlobFraming::Length),
        (VECTOR_FRAME_BYTES, BlobFraming::Magic),
        (last_byte, BlobFraming::Checksum),
    ]
    .into_iter()
    .enumerate()
    {
        let (fixture, served, _dir) =
            serving_then(&format!("kv_framing_{at}"), async |fixture: &Fixture| {
                let vector = vector_of(fixture, &[1, 2]);
                let mut bytes = encode_vector(&vector.positions)?;
                let flip = flip.min(bytes.len() - 1);
                bytes[flip] ^= 1;
                let properties = vector_properties(&vector.data_file, vector.positions.len());
                raw_vector(fixture, "v.puffin", bytes, properties, &vector, 2).await
            })
            .await?;
        let (path, fault, retried) = refresh_fault(&served).await?;
        assert_eq!(path, fixture.delete_path("v.puffin"));
        assert_eq!(fault, DeleteFault::BlobFraming(which));
        assert!(retried, "a transient fault is read again");
    }
    Ok(())
}

#[tokio::test]
async fn test_a_vector_contradicting_its_entry_is_deterministic() -> Result<()> {
    let (fixture, served, _dir) = serving_then("kv_referenced", async |fixture: &Fixture| {
        let vector = vector_of(fixture, &[1]);
        let bytes = encode_vector(&vector.positions)?;
        let properties = vector_properties("file:///elsewhere.parquet", 1);
        raw_vector(fixture, "v.puffin", bytes, properties, &vector, 1).await
    })
    .await?;
    let (_, fault, retried) = refresh_fault(&served).await?;
    assert_eq!(
        fault,
        DeleteFault::ReferencedFileMismatch {
            referenced: fixture.files()[0].file_path().to_string(),
            found: Some("file:///elsewhere.parquet".to_string()),
        }
    );
    assert!(!retried);

    let (_, served, _dir) = serving_then("kv_cardinality", async |fixture: &Fixture| {
        let vector = vector_of(fixture, &[1, 2]);
        let bytes = encode_vector(&vector.positions)?;
        let properties = vector_properties(&vector.data_file, 2);
        raw_vector(fixture, "v.puffin", bytes, properties, &vector, 3).await
    })
    .await?;
    let (_, fault, retried) = refresh_fault(&served).await?;
    assert_eq!(
        fault,
        DeleteFault::CardinalityProperty {
            recorded: 3,
            found: Some("2".to_string()),
        }
    );
    assert!(!retried);

    let (_, served, _dir) = serving_then("kv_decoded", async |fixture: &Fixture| {
        let vector = vector_of(fixture, &[1, 2]);
        let bytes = encode_vector(&vector.positions)?;
        let properties = vector_properties(&vector.data_file, 3);
        raw_vector(fixture, "v.puffin", bytes, properties, &vector, 3).await
    })
    .await?;
    let (_, fault, retried) = refresh_fault(&served).await?;
    assert_eq!(
        fault,
        DeleteFault::RecordsDecoded {
            recorded: 3,
            decoded: 2,
        }
    );
    assert!(!retried);

    Ok(())
}

#[tokio::test]
async fn test_a_position_past_the_end_and_two_applying_vectors_are_deterministic() -> Result<()> {
    let (fixture, served, _dir) = serving_then("kv_past_end", async |fixture: &Fixture| {
        let data_file = fixture.files()[0].file_path().to_string();
        let records = fixture.files()[0].record_count();
        let deletes = PositionDeletes {
            rows: vec![(data_file.clone(), records)],
            referenced: Some(data_file),
            spec_id: fixture.metadata().default_partition_spec_id(),
            partition: Struct::empty(),
        };
        Ok(vec![
            write_position_deletes(
                fixture.file_io(),
                &fixture.delete_path("past.parquet"),
                &deletes,
            )
            .await?,
        ])
    })
    .await?;
    let records = fixture.files()[0].record_count();
    let (path, fault, retried) = refresh_fault(&served).await?;
    assert_eq!(path, fixture.delete_path("past.parquet"));
    assert_eq!(
        fault,
        DeleteFault::PositionPastEnd {
            data_file: fixture.files()[0].file_path().to_string(),
            position: records,
            records,
        }
    );
    assert!(!retried);

    let (fixture, served, _dir) = serving_then("kv_two_vectors", async |fixture: &Fixture| {
        let mut entries = write_vectors(
            fixture.file_io(),
            &fixture.delete_path("a.puffin"),
            &[vector_of(fixture, &[1])],
        )
        .await?;
        entries.extend(
            write_vectors(
                fixture.file_io(),
                &fixture.delete_path("b.puffin"),
                &[vector_of(fixture, &[2])],
            )
            .await?,
        );
        Ok(entries)
    })
    .await?;
    let (path, fault, retried) = refresh_fault(&served).await?;
    assert_eq!(path, fixture.delete_path("a.puffin"));
    assert_eq!(
        fault,
        DeleteFault::TwoVectors {
            data_file: fixture.files()[0].file_path().to_string(),
            other: fixture.delete_path("b.puffin"),
        }
    );
    assert!(!retried);
    Ok(())
}

#[tokio::test]
async fn test_a_refresh_moves_a_file_from_its_position_deletes_to_a_vector_holding_them()
-> Result<()> {
    let (_dir, catalog) = local_catalog().await?;
    let mut fixture = fixture(catalog.as_ref(), "kv_to_vector").await?;
    let [first, second] = FIRST_FILE_IDS;
    fixture
        .delete(&[first], DeleteForm::ScopedPositions, "positions")
        .await?;
    let mut tiers = Tiers::serve(&fixture).await?;
    check(&mut tiers, &fixture).await?;
    // The file's next delete is a vector holding its old row and the new
    // one, as a v3 writer folds them in; the position delete stays listed,
    // masked.
    fixture
        .delete(&[second], DeleteForm::Vectors, "vector")
        .await?;
    assert_eq!(fixture.deletes().len(), 2);
    for (residency, served) in &tiers.served {
        let (swapped, gets) = gets_during(served, served.table.refresh()).await;
        assert!(swapped?);
        assert_eq!(
            gets,
            HINT_POLL_GETS + METADATA_FILE_GETS + MANIFEST_GETS + VECTOR_GETS,
            "{residency}: the vector alone; the masked position file is not read"
        );
        assert_eq!(
            served.current().overlap_groups_verified(),
            0,
            "{residency}: no row came back to life"
        );
    }
    let oracle = check(&mut tiers, &fixture).await?;
    for id in [first, second] {
        assert_eq!(oracle.expected(&fixture.key(id)), Row::Absent);
    }
    Ok(())
}

#[tokio::test]
async fn test_a_refresh_reads_a_vector_replaced_at_another_offset_of_its_puffin_file() -> Result<()>
{
    let (_dir, catalog) = local_catalog().await?;
    let mut fixture = fixture(catalog.as_ref(), "kv_offset_refresh").await?;
    let before = MergeOracle::read(&fixture).await?;
    let [first, second] = FIRST_FILE_IDS.map(|id| before.copies(&fixture.key(id))[0].clone());
    assert_eq!(first.path, second.path, "both in the first file");
    let older = Vector {
        data_file: first.path.clone(),
        positions: [first.position].into_iter().collect(),
        spec_id: fixture.metadata().default_partition_spec_id(),
        partition: Struct::empty(),
    };
    let newer = Vector {
        positions: [first.position, second.position].into_iter().collect(),
        ..older.clone()
    };
    let entries = write_vectors(
        fixture.file_io(),
        &fixture.delete_path("both.puffin"),
        &[older, newer],
    )
    .await?;
    assert_ne!(entries[0].content_offset(), entries[1].content_offset());
    fixture
        .commit_by_hand(HandSnapshot {
            data: fixture.files().to_vec(),
            deletes: vec![entries[0].clone()],
        })
        .await?;
    let mut tiers = Tiers::serve(&fixture).await?;
    check(&mut tiers, &fixture).await?;
    fixture
        .commit_by_hand(HandSnapshot {
            data: fixture.files().to_vec(),
            deletes: vec![entries[1].clone()],
        })
        .await?;
    for (residency, served) in &tiers.served {
        let (swapped, gets) = gets_during(served, served.table.refresh()).await;
        assert!(swapped?);
        assert_eq!(
            gets,
            HINT_POLL_GETS + METADATA_FILE_GETS + MANIFEST_GETS + VECTOR_GETS,
            "{residency}: the footer, dropped once its one listed blob applied, and the new blob"
        );
        assert_eq!(served.current().overlap_groups_verified(), 0);
    }
    let oracle = check(&mut tiers, &fixture).await?;
    assert_eq!(oracle.deleted_rows(), 2);
    Ok(())
}

#[tokio::test]
async fn test_an_unscoped_position_delete_applies_in_its_partition_alone() -> Result<()> {
    let (_dir, catalog) = local_catalog().await?;
    let spec = FixtureSpec {
        ids: IDS_2K,
        buckets: TWO_BUCKETS,
        ..FixtureSpec::small("kv_partitions", KeyType::Long)
    };
    let mut fixture = write_fixture(catalog.as_ref(), NAMESPACE, spec).await?;
    let before = MergeOracle::read(&fixture).await?;
    let [own, other] = [0, 1].map(|at| fixture.files()[at].clone());
    assert_ne!(own.partition(), other.partition(), "one file per bucket");
    let first_copy = |file: &DataFile| {
        fixture
            .ids()
            .map(|id| before.copies(&fixture.key(id))[0].clone())
            .find(|copy| copy.path == file.file_path())
            .expect("each file holds a key")
    };
    let (in_own, in_other) = (first_copy(&own), first_copy(&other));
    let deletes = PositionDeletes {
        rows: vec![
            (in_own.path.clone(), in_own.position),
            (in_other.path.clone(), in_other.position),
        ],
        referenced: None,
        spec_id: fixture.metadata().default_partition_spec_id(),
        partition: own.partition().clone(),
    };
    let entry = write_position_deletes(
        fixture.file_io(),
        &fixture.delete_path("own.parquet"),
        &deletes,
    )
    .await?;
    fixture
        .commit_by_hand(HandSnapshot {
            data: fixture.files().to_vec(),
            deletes: vec![entry],
        })
        .await?;
    let mut tiers = Tiers::serve(&fixture).await?;
    let oracle = check(&mut tiers, &fixture).await?;
    assert_eq!(
        oracle.deleted_rows(),
        1,
        "the row of its own partition alone"
    );
    assert!(oracle.deleted_positions(&in_other.path).is_empty());
    Ok(())
}
