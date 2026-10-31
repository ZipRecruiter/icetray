//! The Bloom residency tier (RFD 017) against the Full tier on the same
//! fixtures, each over its own storage: a cold hit is a key page and a value
//! page, a warm hit nothing, a miss what the filter predicts; the load reads
//! the filters and keeps exactly the predicted bytes resident, no chunk
//! among them; a refresh refetches the chunks of the groups it verifies; a
//! duplicate key is refused as in the Full tier.

use std::collections::HashSet;
use std::sync::Arc;

use iceberg::Catalog;
use icetray_core::ColumnRead;
use icetray_core::approx::{self, count};
use icetray_core::config::{Residency, ValueFormat};
use icetray_core::{Error, Generation, Key, KeyType, Level, LoadedFile, RefusalReason, diagnose};
use icetray_testkit::error::Precondition;
use icetray_testkit::fixture::{
    IDS_4K, INTERLEAVED_BUCKETS, INTERLEAVED_SAMPLE, LONG_KEY_PAD, NAMESPACE, NEWEST_SAMPLE,
    OLD_ID, OVERLAPPING_APPEND, SAMPLE, SECOND_CLASS, TAIL, THIRD_CLASS, THREE_FILES,
    local_catalog, residue_class,
};
use icetray_testkit::layout::KEY_BLOOM_FPP;
use icetray_testkit::layout::VALUE_COLUMN;
use icetray_testkit::verify::{self, Served};
use icetray_testkit::{Fixture, FixtureSpec, Result, TestkitError, write_fixture};

/// Absent keys probed per miss batch, spread evenly over the id range.
const MISS_PROBES: u64 = 200;

/// The filter positives a miss batch may pass, as a share of its probes:
/// this many times the rate the recipe's filter is written at, a bound no
/// run reaches.
const MISS_ALLOWANCE_FACTOR: f64 = 10.0;
const MISS_FALSE_POSITIVE_ALLOWANCE: f64 = MISS_ALLOWANCE_FACTOR * KEY_BLOOM_FPP;

/// The same fixture served in both tiers, each over its own storage.
struct Tiers {
    full: Served,
    bloom: Served,
}

async fn serve_tiers(fixture: &Fixture, catalog: &Arc<dyn Catalog>) -> Result<Tiers> {
    let full = verify::serve_catalog(fixture, catalog, Residency::Full, false).await?;
    let bloom = verify::serve_catalog(fixture, catalog, Residency::Bloom, false).await?;
    assert_eq!(full.current().residency, Residency::Full);
    assert_eq!(bloom.current().residency, Residency::Bloom);
    Ok(Tiers { full, bloom })
}

/// Row groups across `files`.
fn row_groups<'a>(files: impl IntoIterator<Item = &'a Arc<LoadedFile>>) -> u64 {
    files
        .into_iter()
        .map(|file| count(file.index.metadata().num_row_groups()))
        .sum()
}

/// What a cold Bloom batch over `keys` costs: the value page of each key,
/// from the Full oracle, plus every key page a probe pays for, walking the
/// Bloom generation's candidates in lookup order up to the file the Full
/// tier says holds the key, counting a page once however many keys touch
/// it. `Generation::plan` names a Bloom-tier key's first probe alone, so it
/// cannot price a cold hit across an overlap group; this walk stands in
/// for it. On a fixture without overlap it is `expected_key_pages` plus
/// `expected_cold_gets`.
fn expected_bloom_cold_gets(full: &Generation, bloom: &Generation, keys: &[Key]) -> Result<u64> {
    let value_pages = verify::expected_cold_gets(full, keys, &[VALUE_COLUMN])?;
    let mut key_pages = HashSet::new();
    for (key, holder) in keys.iter().zip(full.plan(keys)) {
        let (holder, _page) = holder.expect("every sample key is present");
        for candidate in bloom.candidates(key) {
            if let Some(page) = candidate.index.page_of(key) {
                key_pages.insert((candidate.entry.path.clone(), page.row_group, page.key_page));
            }
            if candidate.entry.path == holder.entry.path {
                break;
            }
        }
    }
    Ok(value_pages + count(key_pages.len()))
}

/// Cold Bloom hits cost exactly what `expected_bloom_cold_gets` predicts and
/// warm ones nothing; every value verifies.
async fn verify_bloom_hit_cost(tiers: &Tiers, fixture: &Fixture, ids: &[u64]) -> Result<()> {
    let keys = fixture.keys_at(ids);
    let expected = expected_bloom_cold_gets(&tiers.full.current(), &tiers.bloom.current(), &keys)?;
    let cold = verify::verify_hits(&tiers.bloom, fixture, ids).await?;
    assert_eq!(cold, expected, "cold bloom hits");
    let warm = verify::verify_hits(&tiers.bloom, fixture, ids).await?;
    assert_eq!(warm, 0, "repeat hits find key and value pages cached");
    Ok(())
}

/// The key pages and the value pages a cold pass over `keys` reads, from
/// the Full-tier oracle over `full`, which holds no overlapping bucket, so
/// a Bloom-tier pass probes one candidate per key.
fn single_probe_costs(full: &Generation, keys: &[Key]) -> Result<(u64, u64)> {
    assert!(
        full.overlapping_buckets().is_empty(),
        "no other candidate to probe"
    );
    Ok((
        verify::expected_key_pages(full, keys),
        verify::expected_cold_gets(full, keys, &[VALUE_COLUMN])?,
    ))
}

#[tokio::test]
async fn test_bloom_cold_hit_is_two_gets_warm_none() -> Result<()> {
    let (_dir, catalog) = local_catalog().await?;
    let fixture = write_fixture(
        catalog.as_ref(),
        NAMESPACE,
        FixtureSpec::small("kv_bloom", KeyType::Long),
    )
    .await?;
    let tiers = serve_tiers(&fixture, &catalog).await?;
    let full = tiers.full.current();
    let keys = fixture.keys_at(&SAMPLE);
    let (key_pages, value_pages) = single_probe_costs(&full, &keys)?;
    let expected = key_pages + value_pages;
    assert_eq!(
        expected,
        expected_bloom_cold_gets(&full, &tiers.bloom.current(), &keys)?
    );
    let cold = verify::verify_hits(&tiers.bloom, &fixture, &SAMPLE).await?;
    assert_eq!(cold, expected, "a key page and a value page per cold hit");
    let warm = verify::verify_hits(&tiers.bloom, &fixture, &SAMPLE).await?;
    assert_eq!(warm, 0);
    Ok(())
}

/// A Bloom-tier row-cache hit still resolves its key: with the page cache
/// emptied, it reads the key page, and no value page (RFD 026).
#[tokio::test]
async fn test_a_bloom_row_cache_hit_still_reads_its_key_page() -> Result<()> {
    let (_dir, catalog) = local_catalog().await?;
    let fixture = write_fixture(
        catalog.as_ref(),
        NAMESPACE,
        FixtureSpec::small("kv_bloom_row_cache", KeyType::Long),
    )
    .await?;
    let served = verify::serve_reading_rows(
        fixture.config(Residency::Bloom, false),
        fixture.catalog_source(Arc::clone(&catalog)),
    )
    .await?;
    let full = verify::serve_catalog(&fixture, &catalog, Residency::Full, false).await?;
    let (key_pages, value_pages) = single_probe_costs(&full.current(), &fixture.keys_at(&SAMPLE))?;
    let cold = verify::verify_hits(&served, &fixture, &SAMPLE).await?;
    assert_eq!(cold, key_pages + value_pages, "a key page and a value page");
    served.cache.clear();
    let hit = verify::verify_hits(&served, &fixture, &SAMPLE).await?;
    assert_eq!(hit, key_pages, "the key pages alone");
    Ok(())
}

/// The Full-tier oracle names value pages from resident key pages; a Bloom
/// generation holds none, and the oracle says so rather than guessing.
#[tokio::test]
async fn test_expected_cold_gets_needs_the_full_tier() -> Result<()> {
    let (_dir, catalog) = local_catalog().await?;
    let spec = FixtureSpec {
        ids: IDS_4K,
        ..FixtureSpec::small("kv_bloom_oracle", KeyType::Long)
    };
    let fixture = write_fixture(catalog.as_ref(), NAMESPACE, spec).await?;
    let served = verify::serve_catalog(&fixture, &catalog, Residency::Bloom, false).await?;
    let error = verify::expected_cold_gets(
        &served.current(),
        &fixture.keys_at(&[OLD_ID]),
        &[VALUE_COLUMN],
    )
    .unwrap_err();
    assert!(
        matches!(
            error,
            TestkitError::Precondition(Precondition::OracleNotFull {
                residency: Residency::Bloom
            })
        ),
        "{error:?}"
    );
    Ok(())
}

#[tokio::test]
async fn test_bloom_misses_cost_exactly_what_the_plan_predicts() -> Result<()> {
    let (_dir, catalog) = local_catalog().await?;
    let fixture = write_fixture(
        catalog.as_ref(),
        NAMESPACE,
        FixtureSpec::small("kv_bloom_miss", KeyType::Long),
    )
    .await?;
    let served = verify::serve_catalog(&fixture, &catalog, Residency::Bloom, false).await?;
    let generation = served.current();
    let stride = fixture.id_range().end / MISS_PROBES;
    let probes: Vec<Key> = (0..MISS_PROBES)
        .map(|i| fixture.absent_near(i * stride))
        .collect();
    let positives: Vec<Key> = probes
        .iter()
        .filter(|key| verify::predicted_probe(&generation, key))
        .cloned()
        .collect();
    let stats = served.storage.stats();
    let before = stats.gets();
    for key in &probes {
        assert_eq!(
            served
                .table
                .get(key, &[ColumnRead::whole(VALUE_COLUMN)])
                .await?
                .row,
            icetray_core::Row::Absent,
            "key {key}"
        );
    }
    assert_eq!(
        stats.gets() - before,
        verify::expected_key_pages(&generation, &positives),
        "a filter positive costs its key page once, a negative nothing"
    );
    assert!(
        approx::ratio(count(positives.len()), MISS_PROBES) <= MISS_FALSE_POSITIVE_ALLOWANCE,
        "{} positives over {MISS_PROBES} probes",
        positives.len()
    );
    Ok(())
}

#[tokio::test]
async fn test_bloom_load_reads_filters_and_keeps_less_resident() -> Result<()> {
    let (_dir, catalog) = local_catalog().await?;
    let spec = FixtureSpec {
        key_pad: LONG_KEY_PAD,
        ..FixtureSpec::small("kv_bloom_strkeys", KeyType::String)
    };
    let fixture = write_fixture(catalog.as_ref(), NAMESPACE, spec).await?;
    let tiers = serve_tiers(&fixture, &catalog).await?;
    let full = tiers.full.current();
    let bloom = tiers.bloom.current();
    let full_load_gets = tiers.full.storage.stats().gets();
    assert_eq!(
        tiers.bloom.storage.stats().gets(),
        full_load_gets + row_groups(&bloom.files),
        "one filter per row group beyond the footers and chunks every tier streams"
    );
    assert!(
        bloom.index_bytes < full.index_bytes,
        "bloom {} < full {}",
        bloom.index_bytes,
        full.index_bytes
    );
    let predicted = diagnose(&full, Level::Optimal, &ValueFormat::opaque, None)
        .summary
        .predicted;
    assert_eq!(
        predicted.in_tier(Residency::Full),
        Some(count(full.index_bytes)),
        "the footer stage predicts the Full tier's resident bytes"
    );
    assert_eq!(
        predicted.in_tier(Residency::Bloom),
        Some(count(bloom.index_bytes)),
        "and the Bloom tier's: footers, indexes, filters, and fences, no chunk"
    );
    let bloom_summary = diagnose(&bloom, Level::Balanced, &ValueFormat::opaque, None).summary;
    assert_eq!(bloom_summary.residency, Residency::Bloom);
    assert_eq!(
        bloom_summary.level,
        Level::Balanced,
        "bloom residency caps the level"
    );
    verify_bloom_hit_cost(&tiers, &fixture, &SAMPLE).await
}

/// The load GETs the Bloom tier spends beyond the Full tier on a refresh
/// from `previous` to `current`. Both tiers read the same manifests and
/// stream the same new footers and chunks; the Bloom tier also reads one
/// filter per row group of every new file, and, for every overlap group a
/// new file joined, one chunk per row group of every member, new and reused
/// alike: the tier keeps no chunks, so the merge fetches them again, while
/// the Full tier hands out its resident ones for nothing. The members come
/// from core's own routing: no group was merged when
/// `overlap_groups_verified` is zero; otherwise a member is a file whose
/// exact range intersects a new file's within one bucket, and two ranges
/// intersect exactly when one covers the other's first key, so a member is
/// a file `Generation::candidates` names for a new file's first key, or a
/// file whose own first key `candidates` answers with a new file. Core
/// links members transitively; on the interleaved fixture every member
/// intersects the new file itself, so the pairwise set is the group.
fn expected_refresh_gets_over_full(previous: &Generation, current: &Generation) -> u64 {
    let previous_paths: HashSet<&str> = previous
        .files
        .iter()
        .map(|file| file.entry.path.as_str())
        .collect();
    let new_files: Vec<&Arc<LoadedFile>> = current
        .files
        .iter()
        .filter(|file| !previous_paths.contains(file.entry.path.as_str()))
        .collect();
    let filters = row_groups(new_files.iter().copied());
    if current.overlap_groups_verified() == 0 {
        return filters;
    }
    let is_new = |file: &Arc<LoadedFile>| new_files.iter().any(|new| Arc::ptr_eq(new, file));
    let touches_new = |file: &Arc<LoadedFile>| {
        new_files.iter().any(|new| {
            current
                .candidates(new.index.key_range().0)
                .any(|candidate| Arc::ptr_eq(candidate, file))
        }) || current.candidates(file.index.key_range().0).any(is_new)
    };
    filters + row_groups(current.files.iter().filter(|file| touches_new(file)))
}

/// Refreshes both tiers over the same append and prices the Bloom tier's
/// extra GETs; returns the Bloom generation.
async fn refresh_both_tiers_priced(tiers: &Tiers) -> Result<Arc<Generation>> {
    let previous = tiers.bloom.current();
    let bloom_before = tiers.bloom.storage.stats().gets();
    let full_before = tiers.full.storage.stats().gets();
    assert!(tiers.bloom.table.refresh().await?);
    assert!(tiers.full.table.refresh().await?);
    let bloom_gets = tiers.bloom.storage.stats().gets() - bloom_before;
    let full_gets = tiers.full.storage.stats().gets() - full_before;
    let generation = tiers.bloom.current();
    assert_eq!(
        bloom_gets - full_gets,
        expected_refresh_gets_over_full(&previous, &generation),
        "bloom refresh {bloom_gets} GETs, full refresh {full_gets}"
    );
    Ok(generation)
}

#[tokio::test]
async fn test_bloom_reverifies_grown_groups_by_refetching_chunks() -> Result<()> {
    let (_dir, catalog) = local_catalog().await?;
    let mut fixture = write_fixture(
        catalog.as_ref(),
        NAMESPACE,
        FixtureSpec::interleaved("kv_bloom_interleaved"),
    )
    .await?;
    fixture
        .append(catalog.as_ref(), residue_class(SECOND_CLASS), "half")
        .await?;
    let tiers = serve_tiers(&fixture, &catalog).await?;
    let generation = tiers.bloom.current();
    assert_eq!(
        generation.overlap_groups_verified(),
        approx::length(INTERLEAVED_BUCKETS)
    );
    verify_bloom_hit_cost(&tiers, &fixture, &INTERLEAVED_SAMPLE).await?;

    fixture.append(catalog.as_ref(), TAIL, "tail").await?;
    let generation = refresh_both_tiers_priced(&tiers).await?;
    assert_eq!(
        generation.overlap_groups_verified(),
        0,
        "disjoint files join no group"
    );

    fixture
        .append(catalog.as_ref(), residue_class(THIRD_CLASS), "quarter")
        .await?;
    let generation = refresh_both_tiers_priced(&tiers).await?;
    assert_eq!(
        generation.overlap_groups_verified(),
        approx::length(INTERLEAVED_BUCKETS)
    );
    assert_eq!(
        generation.overlapping_buckets().len(),
        approx::length(INTERLEAVED_BUCKETS)
    );
    assert!(
        generation
            .overlapping_buckets()
            .values()
            .all(|&files| files == THREE_FILES)
    );
    let fresh = Tiers {
        full: tiers.full,
        bloom: verify::serve_catalog(&fixture, &catalog, Residency::Bloom, false).await?,
    };
    verify_bloom_hit_cost(&fresh, &fixture, &NEWEST_SAMPLE).await?;
    assert_eq!(generation.records, count(fixture.ids().count()));
    Ok(())
}

#[tokio::test]
async fn test_bloom_refuses_duplicate_keys_on_refresh() -> Result<()> {
    let (_dir, catalog) = local_catalog().await?;
    let spec = FixtureSpec {
        ids: IDS_4K,
        ..FixtureSpec::small("kv_bloom_dup", KeyType::Long)
    };
    let mut fixture = write_fixture(catalog.as_ref(), NAMESPACE, spec).await?;
    let served = verify::serve_catalog(&fixture, &catalog, Residency::Bloom, false).await?;
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
    assert_eq!(
        served.current().snapshot_id,
        v1,
        "the previous generation keeps serving"
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
    Ok(())
}
