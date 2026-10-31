//! The Disk residency tier (RFD 017) in a temporary directory per test,
//! counting GETs and local reads on one `IoStats`: a cold hit is one GET and
//! one local read, a miss one local read; a restart reuses the chunk files
//! and fetches footers only; an over-budget load is refused before a chunk
//! is written; `retain` frees a dropped file's chunk file once no table
//! serves it; the page cache's disk level serves evicted pages and survives
//! a restart.

use std::collections::{BTreeMap, HashSet};
use std::path::Path;
use std::sync::Arc;

use icetray_core::ColumnRead;
use icetray_core::approx::count;
use icetray_core::config::{
    Config, LocalConfig, Residency, ServerConfig, StorageConfig, TableConfig,
};
use icetray_core::contract::{PAGE_CACHE_DISK_BLOCK_BYTES, PAGE_ROWS};
use icetray_core::open::open_with;
use icetray_core::{
    Budget, DataFileIdentity, Direction, Error, FindingLog, KeyType, OrderFences, RefusalReason,
    Row, ScanBounds, Storage,
};
use icetray_testkit::fixture::{
    FOUR_BUCKETS, IDS_4K, NAMESPACE, ONE_BUCKET, SAMPLE, TINY_BUDGET, local_catalog,
};
use icetray_testkit::layout::VALUE_COLUMN;
use icetray_testkit::verify::{self, LOAD_METADATA_GETS, METADATA_FILE_GETS, Served};
use icetray_testkit::{
    Fixture, FixtureSpec, HandSnapshot, OrderKind, Ordered, Result, RowsPerKey, TestkitError,
    write_fixture,
};

/// The two tables sharing one local directory in the retain test.
const TABLE_A: &str = "a";
const TABLE_B: &str = "b";

/// The page cache's memory capacity in the disk-level test: a few contract
/// pages of hundred-byte values, so the dozen distinct pages `SAMPLE`
/// fetches overflow it and spill to the disk level.
const SMALL_CACHE_BYTES: u64 = 384 * 1024;

#[tokio::test]
async fn test_disk_cold_hit_is_one_get_and_one_local_read_miss_one_local_read() -> Result<()> {
    let (_dir, catalog) = local_catalog().await?;
    let scratch = tempfile::tempdir()?;
    let fixture = write_fixture(
        catalog.as_ref(),
        NAMESPACE,
        FixtureSpec::small("kv_disk", KeyType::Long),
    )
    .await?;
    let oracle = verify::serve_catalog(&fixture, &catalog, Residency::Full, false).await?;
    let expected = verify::expected_cold_gets(
        &oracle.current(),
        &fixture.keys_at(&SAMPLE),
        &[VALUE_COLUMN],
    )?;
    let config = fixture.config(Residency::Disk, false);
    let (storage, _local, served) =
        verify::serve_disk(&fixture, &catalog, config, scratch.path(), None).await?;
    let served = served?;
    assert_eq!(served.current().residency, Residency::Disk);
    let stats = storage.stats();
    let counters = || (stats.gets(), stats.local_reads());
    let sample = count(SAMPLE.len());

    let (gets, reads) = counters();
    let cold = verify::verify_hits(&served, &fixture, &SAMPLE).await?;
    assert_eq!(cold, expected, "one value page GET per cold hit");
    assert_eq!(
        counters(),
        (gets + expected, reads + sample),
        "one key page read per find"
    );

    let (gets, reads) = counters();
    let warm = verify::verify_hits(&served, &fixture, &SAMPLE).await?;
    assert_eq!(warm, 0);
    assert_eq!(
        counters(),
        (gets, reads + sample),
        "a warm hit still finds its row on disk"
    );

    // An absent key hashes to its own bucket, whose file may not cover it:
    // only a probe the plan routes to a key page reads that page.
    let generation = served.current();
    let (gets, reads) = counters();
    let mut routed = 0;
    for id in SAMPLE {
        let key = fixture.absent_near(id);
        if verify::predicted_probe(&generation, &key) {
            routed += 1;
        }
        assert_eq!(
            served
                .table
                .get(&key, &[ColumnRead::whole(VALUE_COLUMN)])
                .await?
                .row,
            Row::Absent,
            "key {key}"
        );
    }
    assert!(routed > 0, "some absent key falls inside a file's range");
    assert_eq!(
        counters(),
        (gets, reads + routed),
        "a miss inside a file's range costs one local read and no GET, one outside every range nothing"
    );

    let (gets, reads) = counters();
    let far = icetray_testkit::far_key(KeyType::Long);
    assert_eq!(
        served
            .table
            .get(&far, &[ColumnRead::whole(VALUE_COLUMN)])
            .await?
            .row,
        Row::Absent
    );
    assert_eq!(counters(), (gets, reads), "a far miss touches nothing");
    Ok(())
}

#[tokio::test]
async fn test_disk_restart_reuses_chunk_files_and_fetches_footers_only() -> Result<()> {
    let (_dir, catalog) = local_catalog().await?;
    let scratch = tempfile::tempdir()?;
    let fixture = write_fixture(
        catalog.as_ref(),
        NAMESPACE,
        FixtureSpec::small("kv_disk_restart", KeyType::Long),
    )
    .await?;
    let files = count(fixture.file_count());
    let config = fixture.config(Residency::Disk, false);
    let (_storage, local, served) =
        verify::serve_disk(&fixture, &catalog, config, scratch.path(), None).await?;
    let served = served?;
    let identities = served.current().identities();
    assert_eq!(count(identities.len()), files);
    for identity in &identities {
        assert!(local.chunks_for(identity)?.is_some(), "{}", identity.path);
    }
    assert!(local.index_bytes_in_use()? > 0);
    drop(served);
    drop(local);

    let config = fixture.config(Residency::Disk, false);
    let (storage, _local, served) =
        verify::serve_disk(&fixture, &catalog, config, scratch.path(), None).await?;
    let served = served?;
    assert_eq!(
        storage.stats().gets(),
        METADATA_FILE_GETS + LOAD_METADATA_GETS + files,
        "metadata and footers only: every chunk file is reused"
    );
    let stats = storage.stats();
    for id in SAMPLE {
        let key = fixture.key(id);
        let (gets, reads) = (stats.gets(), stats.local_reads());
        assert_eq!(
            served
                .table
                .get(&key, &[ColumnRead::whole(VALUE_COLUMN)])
                .await?
                .row,
            fixture.expected(&key)
        );
        assert_eq!(
            (stats.gets() - gets, stats.local_reads() - reads),
            (1, 1),
            "key {key}"
        );
    }
    Ok(())
}

/// The digits the ordered restart fixture pads its string order values to:
/// a long prefix every value shares, which the fences cut past.
const PADDED_ORDER_DIGITS: u32 = 64;

/// Rows per key of the ordered restart fixture: a few, varying, so a
/// file's runs start and continue across its order pages.
fn a_few_rows(id: u64) -> u64 {
    2 + id % 3
}

/// Every served file's order fences, by data file path.
fn fences_of(served: &Served) -> BTreeMap<String, OrderFences> {
    served
        .current()
        .files
        .iter()
        .map(|file| {
            let fences = file
                .index
                .order_fences()
                .expect("an ordered file has fences");
            (file.entry.path.clone(), fences.clone())
        })
        .collect()
}

/// A Disk restart over an ordered table's trusted chunk files restores
/// every file's order fences as the first load recorded them (RFD 025,
/// Load), fetching metadata and footers alone and reading nothing back from
/// a chunk file, so no order value is decoded.
#[tokio::test]
async fn test_disk_restart_restores_identical_order_fences_without_a_chunk_read() -> Result<()> {
    let (_dir, catalog) = local_catalog().await?;
    let scratch = tempfile::tempdir()?;
    let ordered = Ordered {
        kind: OrderKind::PaddedStr(PADDED_ORDER_DIGITS),
        rows_per_key: RowsPerKey::By(a_few_rows),
        start: 0,
        step: 1,
    };
    let spec = FixtureSpec::ordered("ordered_disk_restart", KeyType::String, ordered);
    let fixture = write_fixture(catalog.as_ref(), NAMESPACE, spec).await?;
    let files = count(fixture.file_count());
    let config = fixture.config(Residency::Disk, false);
    let (_storage, local, served) =
        verify::serve_disk(&fixture, &catalog, config, scratch.path(), None).await?;
    let first = fences_of(&served?);
    assert_eq!(count(first.len()), files);
    assert!(
        first
            .values()
            .any(|fences| (0..fences.row_groups()).any(|row_group| fences.pages(row_group) > 1)),
        "a file has several order pages"
    );
    drop(local);

    let config = fixture.config(Residency::Disk, false);
    let (storage, _local, served) =
        verify::serve_disk(&fixture, &catalog, config, scratch.path(), None).await?;
    let restarted = fences_of(&served?);
    assert_eq!(
        storage.stats().gets(),
        METADATA_FILE_GETS + LOAD_METADATA_GETS + files,
        "metadata and footers only: every chunk file is trusted"
    );
    assert_eq!(storage.stats().local_reads(), 0, "no chunk is read back");
    assert_eq!(restarted, first);
    Ok(())
}

/// Ids of the rolled ordered fixture, all in one bucket, and its rows per
/// key: fewer than a page holds, so a key a file boundary cuts lies on one
/// order page on each side of it.
const ROLLED_IDS: std::ops::Range<u64> = 0..200;
const ROLLED_ROWS_PER_KEY: u64 = 3;

/// Rows per page of the rolled fixture, which its writer writes a batch
/// at a time, and the file size it rolls at: about a dozen batches, so
/// each file holds many pages and its runs cross into the next file.
const ROLLED_PAGE_ROWS: usize = 4;
const ROLLED_FILE_BYTES: usize = 4 * 1024;

/// The local reads one edge page of a rolled file costs a Disk merge:
/// its order page and the key page of the same rows, each read from the
/// chunk file.
const EDGE_PAGE_READS: u64 = 2;

/// The files a shared edge key lies in: the one ending at it and the one
/// starting at it.
const SIDES_OF_AN_EDGE: u64 = 2;

/// The local bytes a Disk merge reads of the last order page of `file`,
/// when `last`, or its first: the page and the key page holding the same
/// rows, which the rolled fixture writes in pages of the same rows.
fn edge_page_bytes(file: &icetray_core::LoadedFile, last: bool) -> u64 {
    let metadata = file.index.metadata();
    let offsets = metadata.offset_index().expect("an offset index");
    let row_group = if last { offsets.len() - 1 } else { 0 };
    let at = |leaf: usize| {
        let pages = offsets[row_group][leaf].page_locations();
        let page = if last { pages.len() - 1 } else { 0 };
        pages[page].clone()
    };
    let order = at(file.index.order_leaf().expect("an ordered file"));
    let key = at(file.index.key_leaf());
    assert_eq!(order.first_row_index, key.first_row_index, "aligned pages");
    count(
        usize::try_from(order.compressed_page_size + key.compressed_page_size)
            .expect("page sizes are not negative"),
    )
}

/// A Disk restart of an ordered table whose files one compaction rolled
/// by size, meeting at edge keys, trusts every chunk file and reads back
/// only the edge pages for the check across files (RFD 025, Refresh): per
/// key two neighbours share, the first file's last order page and the
/// second's first, each with the key page of its rows, and nothing else.
#[tokio::test]
async fn test_disk_restart_of_a_rolled_ordered_table_reads_only_edge_pages() -> Result<()> {
    let (_dir, catalog) = local_catalog().await?;
    let scratch = tempfile::tempdir()?;
    let ordered = Ordered {
        kind: OrderKind::Long,
        rows_per_key: RowsPerKey::Each(ROLLED_ROWS_PER_KEY),
        start: 0,
        step: 1,
    };
    let spec = FixtureSpec {
        ids: ROLLED_IDS,
        buckets: ONE_BUCKET,
        layout: icetray_testkit::Layout {
            page_rows: ROLLED_PAGE_ROWS,
            target_file_bytes: ROLLED_FILE_BYTES,
            ..icetray_testkit::Layout::contract()
        },
        ..FixtureSpec::ordered("ordered_disk_rolled", KeyType::Long, ordered)
    };
    let fixture = write_fixture(catalog.as_ref(), NAMESPACE, spec).await?;
    let config = fixture.config(Residency::Disk, false);
    let (_storage, local, served) =
        verify::serve_disk(&fixture, &catalog, config, scratch.path(), None).await?;
    drop(served?);
    drop(local);

    let config = fixture.config(Residency::Disk, false);
    let (storage, _local, served) =
        verify::serve_disk(&fixture, &catalog, config, scratch.path(), None).await?;
    let served = served?;
    let generation = served.current();
    let files = &generation.files;
    let mut shared = 0;
    let mut chains = 0;
    let mut bytes = 0;
    let mut chained = false;
    for pair in files.windows(2) {
        let meets_next = pair[0].index.key_range().1 == pair[1].index.key_range().0;
        if meets_next {
            shared += 1;
            bytes += edge_page_bytes(&pair[0], true) + edge_page_bytes(&pair[1], false);
            if !chained {
                chains += 1;
            }
        }
        chained = meets_next;
    }
    let stats = storage.stats();
    assert!(
        files.len() >= 3 && shared >= 2 && chains >= 1,
        "a rolled table"
    );
    assert_eq!(
        stats.gets(),
        METADATA_FILE_GETS + LOAD_METADATA_GETS + count(files.len()),
        "metadata and footers only: every chunk file is trusted"
    );
    assert_eq!(
        (stats.local_reads(), stats.local_bytes()),
        (SIDES_OF_AN_EDGE * EDGE_PAGE_READS * shared, bytes),
        "the edge pages alone"
    );
    assert_eq!(generation.overlap_groups_verified(), chains);
    Ok(())
}

#[tokio::test]
async fn test_disk_over_budget_is_refused_before_any_chunk_is_written() -> Result<()> {
    let (_dir, catalog) = local_catalog().await?;
    let scratch = tempfile::tempdir()?;
    let fixture = write_fixture(
        catalog.as_ref(),
        NAMESPACE,
        FixtureSpec::small("kv_disk_budget", KeyType::Long),
    )
    .await?;
    let config = fixture.config(Residency::Disk, false);
    let (storage, local, served) = verify::serve_disk(
        &fixture,
        &catalog,
        config,
        scratch.path(),
        Some(TINY_BUDGET),
    )
    .await?;
    let error = served.err().unwrap();
    assert!(
        matches!(
            error,
            TestkitError::Core(Error::Refused {
                reason: RefusalReason::OverBudget {
                    of: Budget::Index,
                    ..
                },
                ..
            })
        ),
        "{error:?}"
    );
    assert_eq!(local.index_bytes_in_use()?, 0, "no chunk file was written");
    assert_eq!(
        storage.stats().gets(),
        METADATA_FILE_GETS + LOAD_METADATA_GETS + count(fixture.file_count()),
        "the metadata, every footer, and no chunk"
    );
    Ok(())
}

#[tokio::test]
async fn test_retain_frees_only_what_no_table_serves() -> Result<()> {
    let (_dir, catalog) = local_catalog().await?;
    let scratch = tempfile::tempdir()?;
    let spec = FixtureSpec {
        ids: IDS_4K,
        buckets: FOUR_BUCKETS,
        ..FixtureSpec::small("kv_retain", KeyType::Long)
    };
    let mut fixture = write_fixture(catalog.as_ref(), NAMESPACE, spec).await?;
    fixture.write_hint().await?;
    let table = |name: &str| TableConfig {
        name: name.to_string(),
        source: fixture.root_source(),
        ..fixture.config(Residency::Disk, false)
    };
    let config = Config {
        server: ServerConfig::default(),
        storage: StorageConfig {
            local: Some(LocalConfig {
                dir: scratch.path().to_path_buf(),
                index_budget_bytes: None,
                page_cache_disk_bytes: 0,
            }),
            ..verify::storage_config_for_tests()
        },
        tables: vec![table(TABLE_A), table(TABLE_B)],
    };
    let storage = Arc::new(Storage::new(config.storage.clone()));
    let opened = open_with(&config, None, storage, FindingLog::Events).await?;
    let local = opened.local.clone().unwrap();
    let tables = opened.tables();
    let (a, b) = (&tables[0], &tables[1]);
    a.load().await?;
    b.load().await?;
    assert_eq!(opened.retain().await?, 0);
    let identities: HashSet<DataFileIdentity> =
        a.current().expect("the table was loaded").identities();
    assert_eq!(identities.len(), fixture.file_count());

    let dropped_file = fixture.files().last().unwrap().clone();
    let dropped = identities
        .iter()
        .find(|identity| identity.path == dropped_file.file_path())
        .unwrap()
        .clone();
    let kept = identities
        .iter()
        .find(|identity| identity.path != dropped_file.file_path())
        .unwrap()
        .clone();
    let data: Vec<_> = fixture
        .files()
        .iter()
        .filter(|file| file.file_path() != dropped_file.file_path())
        .cloned()
        .collect();
    fixture
        .commit_by_hand(HandSnapshot {
            data,
            deletes: Vec::new(),
        })
        .await?;
    assert!(a.refresh().await?);
    assert_eq!(opened.retain().await?, 0, "b still serves the dropped file");
    assert!(local.chunks_for(&dropped)?.is_some());
    assert!(b.refresh().await?);
    assert!(
        opened.retain().await? > 0,
        "nothing serves the dropped file"
    );
    assert!(local.chunks_for(&dropped)?.is_none());
    assert!(local.chunks_for(&kept)?.is_some());
    Ok(())
}

#[tokio::test]
async fn test_page_cache_disk_level_serves_evicted_pages_and_survives_restart() -> Result<()> {
    let (_dir, catalog) = local_catalog().await?;
    let scratch = tempfile::tempdir()?;
    let fixture = write_fixture(
        catalog.as_ref(),
        NAMESPACE,
        FixtureSpec::small("kv_disk_pages", KeyType::Long),
    )
    .await?;
    let storage = Arc::new(Storage::new(verify::storage_config_for_tests()));
    let local = verify::local_dir(scratch.path(), &storage, None, PAGE_CACHE_DISK_BLOCK_BYTES)?;
    let config = fixture.config(Residency::Full, false);
    let source = || fixture.catalog_source(Arc::clone(&catalog));

    let served = verify::build(
        Arc::clone(&storage),
        config.clone(),
        source(),
        Some(Arc::clone(&local)),
        SMALL_CACHE_BYTES,
    )
    .await?
    .load()
    .await?;
    let fetched_page_bytes =
        SAMPLE.len() * usize::try_from(PAGE_ROWS).unwrap() * fixture.spec().value_bytes;
    assert!(
        count(fetched_page_bytes) > SMALL_CACHE_BYTES,
        "the sample's pages overflow memory, so the surplus reaches the disk level"
    );
    for id in SAMPLE {
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
    assert_eq!(
        served.cache.fetches(),
        count(SAMPLE.len()),
        "one distinct page per sample id"
    );
    served.cache.close().await?;
    drop(served);

    let served = verify::build(
        Arc::clone(&storage),
        config,
        source(),
        Some(local),
        SMALL_CACHE_BYTES,
    )
    .await?
    .load()
    .await?;
    let stats = storage.stats();
    let key = fixture.key(SAMPLE[0]);
    let (gets, reads) = (stats.gets(), stats.local_reads());
    assert_eq!(
        served
            .table
            .get(&key, &[ColumnRead::whole(VALUE_COLUMN)])
            .await?
            .row,
        fixture.expected(&key)
    );
    assert_eq!(
        (stats.gets() - gets, stats.local_reads() - reads),
        (0, 1),
        "the page comes back from the disk level"
    );
    Ok(())
}

/// One row per key: an ordered fixture of this shape also loads under a
/// key-value entry, which refuses a repeated key.
const ONE_ROW: RowsPerKey = RowsPerKey::Each(1);

/// The two entries of `fixture`, an ordered fixture whose root is
/// followed, on one local directory under `dir`: `TABLE_A` a key-value
/// entry over its key column, `TABLE_B` an ordered entry over its order
/// column, both in the Disk tier.
fn both_kinds(fixture: &Fixture, dir: &Path) -> Config {
    let ordered = TableConfig {
        name: TABLE_B.to_string(),
        source: fixture.root_source(),
        ..fixture.config(Residency::Disk, false)
    };
    let key_value = TableConfig {
        name: TABLE_A.to_string(),
        order_column: None,
        ..ordered.clone()
    };
    Config {
        server: ServerConfig::default(),
        storage: StorageConfig {
            local: Some(LocalConfig {
                dir: dir.to_path_buf(),
                index_budget_bytes: None,
                page_cache_disk_bytes: 0,
            }),
            ..verify::storage_config_for_tests()
        },
        tables: vec![key_value, ordered],
    }
}

/// `config` opened over a fresh storage, every table loaded, and that
/// storage.
async fn open_loaded(config: &Config) -> Result<(icetray_core::Opened, Arc<Storage>)> {
    let storage = Arc::new(Storage::new(config.storage.clone()));
    let opened = open_with(config, None, Arc::clone(&storage), FindingLog::Events).await?;
    for table in opened.tables().iter() {
        table.load().await?;
    }
    Ok((opened, storage))
}

/// A key-value entry and an ordered entry over the same files, both under
/// `disk` in one `open`, each keep their own chunk file per data file,
/// named by the rules it was verified under, and neither trusts the
/// other's (RFD 025, Load): retention keeps both, and a restart reuses
/// each without fetching a chunk.
#[tokio::test]
async fn test_disk_entries_of_both_kinds_keep_their_own_chunk_files() -> Result<()> {
    let (_dir, catalog) = local_catalog().await?;
    let scratch = tempfile::tempdir()?;
    let ordered = Ordered {
        kind: OrderKind::Long,
        rows_per_key: ONE_ROW,
        start: 0,
        step: 1,
    };
    let spec = FixtureSpec {
        ids: IDS_4K,
        buckets: FOUR_BUCKETS,
        ..FixtureSpec::ordered("kinds_disk", KeyType::Long, ordered)
    };
    let fixture = write_fixture(catalog.as_ref(), NAMESPACE, spec).await?;
    fixture.write_hint().await?;
    let config = both_kinds(&fixture, scratch.path());
    let files = fixture.file_count();

    let (opened, storage) = open_loaded(&config).await?;
    let first_gets = storage.stats().gets();
    let generations: Vec<_> = opened
        .tables()
        .iter()
        .map(|table| table.current().expect("the table was loaded"))
        .collect();
    let paths_of = |at: usize| -> HashSet<std::path::PathBuf> {
        generations[at]
            .chunk_files()
            .map(|(path, _)| path.to_path_buf())
            .collect()
    };
    let (key_value, ordered) = (paths_of(0), paths_of(1));
    assert_eq!((key_value.len(), ordered.len()), (files, files));
    assert!(key_value.is_disjoint(&ordered), "one chunk file per kind");
    let index_dir = key_value
        .iter()
        .next()
        .unwrap()
        .parent()
        .unwrap()
        .to_path_buf();
    let on_disk = || -> Result<HashSet<std::path::PathBuf>> {
        Ok(std::fs::read_dir(&index_dir)?
            .map(|entry| entry.map(|entry| entry.path()))
            .collect::<std::io::Result<_>>()?)
    };
    let both: HashSet<_> = key_value.union(&ordered).cloned().collect();
    assert_eq!(on_disk()?, both, "two chunk files per data file");
    assert_eq!(opened.retain().await?, 0, "retention keeps both kinds");
    assert_eq!(on_disk()?, both);
    // One load GET per row group of the key column, and of the order
    // column beside it for the ordered entry.
    let row_groups: u64 = generations[1]
        .files
        .iter()
        .map(|file| count(file.index.metadata().num_row_groups()))
        .sum();
    let chunk_gets = row_groups + 2 * row_groups;
    assert!(row_groups > 0);
    drop(generations);
    drop(opened);

    let (restarted, storage) = open_loaded(&config).await?;
    assert_eq!(
        storage.stats().gets(),
        first_gets - chunk_gets,
        "metadata and footers only: every chunk file of both kinds is reused"
    );
    let stats = storage.stats();
    let reads = stats.local_reads();
    let key = fixture.key(SAMPLE[0]);
    let reads_of = [ColumnRead::whole(VALUE_COLUMN)];
    let tables = restarted.tables();
    let Row::Present(values) = tables[0].get(&key, &reads_of).await?.row else {
        panic!("the key-value entry holds the key");
    };
    let every_row = ScanBounds {
        direction: Direction::NewestFirst,
        limit: tables[1]
            .config()
            .scan_rows()
            .expect("an ordered table has a max_scan_rows"),
        start: None,
        before: None,
    };
    let scanned = tables[1].scan(&key, &reads_of, &every_row).await?.scanned;
    let scanned_values: Vec<_> = scanned.rows.into_iter().map(|row| row.values).collect();
    assert_eq!(
        scanned_values,
        [values],
        "both kinds read the key's one row"
    );
    // The key-value entry's key page; the ordered entry's key page and its
    // order column's span, each one read of its own chunk file.
    let key_value_reads = 1;
    let ordered_reads = 2;
    assert_eq!(
        stats.local_reads() - reads,
        key_value_reads + ordered_reads,
        "each kind finds its key in its own chunk file"
    );
    assert_eq!(on_disk()?, both, "nothing was rewritten under another name");
    Ok(())
}
