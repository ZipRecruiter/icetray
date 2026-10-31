//! Scan and `MultiScan` of ordered tables (RFD 025) in every residency tier:
//! the answers of randomly drawn tables, changed by appends, deletes, and
//! updates, against the naive-sort model (`Fixture::expected_scan`); paging
//! by `next`; `next` over deleted rows; half-open bounds at the extremes of
//! the order types; the GETs of a cold Scan; the files a Scan
//! reads; the calls each table kind refuses; and the cache a Scan's pages
//! stay in.

use std::collections::{HashMap, HashSet};
use std::ops::Range;
use std::sync::Arc;

use bytes::Bytes;
use iceberg::Catalog;
use iceberg::spec::{FormatVersion, Literal, PrimitiveType};
use icetray_core::approx::count;
use icetray_core::config::{Residency, TableConfig};
use icetray_core::{
    ColumnRead, Direction, Error, Key, KeyType, LoadedFile, OrderValue, Pair, ScanBounds,
    TableKind, bucket,
};
use icetray_testkit::fixture::{
    HandSnapshot, ONE_BUCKET, SECOND_VALUE_COLUMN, SchemaChange, StatedDefault, TWO_BUCKETS,
    local_catalog,
};
use icetray_testkit::layout::VALUE_COLUMN;
use icetray_testkit::verify::{self, Served};
use icetray_testkit::{
    DeleteForm, Dictionary, Fixture, FixtureSpec, Layout, MergeOracle, Nulls, OrderKind, Ordered,
    Result, RowsPerKey, Statistics, TestScan, order_value, write_fixture,
};
use rand::rngs::StdRng;
use rand::{RngExt as _, SeedableRng as _};
use tempfile::TempDir;

/// The seed every random draw of this file starts from, so a failure
/// replays.
const SCAN_SEED: u64 = 0x5ca9_0025;

/// Random tables the model test draws.
const INSTANCES: usize = 40;

/// Scans the model test draws per table, each run in every tier.
const KEYS_PER_INSTANCE: usize = 12;

/// Random tables the `MultiScan` test draws.
const MULTI_SCAN_INSTANCES: usize = 6;

/// Keys one drawn `MultiScan` asks for, repeats included.
const KEYS_PER_MULTI_SCAN: Range<usize> = 1..8;

/// Ids a drawn table writes, and the ids past them an append may add.
const DRAWN_IDS: Range<u64> = 0..12;
const NEW_IDS: u64 = 2;

/// The raw order value a key's first row takes, far enough above zero that
/// a bound below it is still encodable in every kind.
const ORDER_START: i64 = 1_000;

/// The steps between a key's rows a draw takes: at least two, so an append
/// finds raw values between two rows.
const ORDER_STEPS: Range<i64> = 2..5;

/// The key types a draw takes.
const KEY_TYPES: [KeyType; 4] = [
    KeyType::Long,
    KeyType::String,
    KeyType::Binary,
    KeyType::Uuid,
];

/// A fixed width that holds the drawn raw values, narrower than a long.
const NARROW_FIXED: u32 = 4;

/// A fixed width wider than a long: its values share a prefix of zeros.
const WIDE_FIXED: u32 = 12;

/// The order kinds a draw takes. The string and byte kinds' values share
/// long prefixes, a string's zero-padded digits and the bytes' leading
/// zeros, so a truncated column index widens their pages.
const ORDER_KINDS: [OrderKind; 9] = [
    OrderKind::Long,
    OrderKind::Int,
    OrderKind::Date,
    OrderKind::Timestamp,
    OrderKind::TimestampTz,
    OrderKind::Str,
    OrderKind::Bin,
    OrderKind::Fixed(NARROW_FIXED),
    OrderKind::Fixed(WIDE_FIXED),
];

/// The column index truncation a draw may give a table of a string or
/// byte order kind: shorter than the shared prefix, so neighboring pages'
/// entries overlap.
const TRUNCATE_INDEX: usize = 6;

/// The row counts per key a draw takes.
const ROWS_PER_KEY: [RowsPerKey; 4] = [
    RowsPerKey::Each(1),
    RowsPerKey::Each(3),
    RowsPerKey::By(up_to_five),
    RowsPerKey::By(up_to_nine),
];

/// Zero to four rows, so some keys have none.
fn up_to_five(id: u64) -> u64 {
    id % 5
}

/// One to nine rows.
fn up_to_nine(id: u64) -> u64 {
    (id * 7) % 9 + 1
}

/// Rows per page and per row group a draw takes; `None` row groups leave
/// one per file.
const PAGE_ROWS: Range<usize> = 1..5;
const ROW_GROUP_ROWS: Range<usize> = 2..9;

/// Bucket counts a draw takes; zero writes an unpartitioned table.
const BUCKETS: [u32; 4] = [0, ONE_BUCKET, TWO_BUCKETS, 4];

/// The `max_scan_rows` a draw may give a table, small enough that a limit
/// of 0 cuts a run.
const SMALL_MAX_SCAN_ROWS: Range<u32> = 2..6;

/// Bytes per value of a drawn table: small, so the test stays quick.
const DRAWN_VALUE_BYTES: usize = 24;

/// Every third id's values are null where a draw asks for nulls.
const NULL_EVERY: u64 = 3;

/// The most appends, deletes, and updates a draw makes, and the most rows
/// each touches.
const MOST_APPENDS: usize = 2;
const MOST_DELETES: usize = 2;
const MOST_UPDATES: usize = 1;
const MOST_ROWS_PER_CHANGE: usize = 4;

/// The delete forms a draw takes.
const FORMS: [DeleteForm; 3] = [
    DeleteForm::Vectors,
    DeleteForm::ScopedPositions,
    DeleteForm::Positions,
];

/// A drawn table: the fixture, its catalog's directory, the order kind,
/// and the `max_scan_rows` it is served under.
struct Drawn {
    fixture: Fixture,
    _dir: TempDir,
    kind: OrderKind,
    max_scan_rows: u32,
    /// Whether a delete or an update landed on the table.
    changed: bool,
}

/// One table served in every tier, each over storage and caches of its
/// own, the Disk tier's local directory kept alive beside it.
struct Tiers {
    served: Vec<(Residency, Served)>,
    _dirs: Vec<TempDir>,
}

impl Tiers {
    /// Every tier's serving.
    fn all(&self) -> impl Iterator<Item = &(Residency, Served)> {
        self.served.iter()
    }
}

/// `config(residency)` served in every tier.
async fn serve_in_every_tier(config: impl Fn(Residency) -> TableConfig) -> Result<Tiers> {
    let mut served = Vec::new();
    let mut dirs = Vec::new();
    for residency in Residency::ALL {
        let dir = tempfile::tempdir()?;
        served.push((
            residency,
            verify::serve_in(config(residency), dir.path()).await?,
        ));
        dirs.push(dir);
    }
    Ok(Tiers {
        served,
        _dirs: dirs,
    })
}

/// `drawn` served in every tier under its `max_scan_rows`.
async fn serve_drawn(drawn: &Drawn) -> Result<Tiers> {
    serve_in_every_tier(|residency| TableConfig {
        max_scan_rows: Some(drawn.max_scan_rows),
        ..drawn.fixture.config(residency, false)
    })
    .await
}

/// An element of `items` drawn by `rng`.
fn pick<T: Copy>(rng: &mut StdRng, items: &[T]) -> T {
    items[rng.random_range(0..items.len())]
}

/// A random ordered specification named `name`: key and order kinds, rows
/// per key, page and row-group rows, buckets, a truncated column index for
/// a string or byte order kind, an order column without a column index or
/// dictionary-encoded, a dictionary-encoded value column, a second value
/// column, nulls, and a small `max_scan_rows`, each drawn by `rng`.
fn random_ordered_spec(rng: &mut StdRng, name: &str) -> (FixtureSpec, u32) {
    let kind = pick(rng, &ORDER_KINDS);
    let bytewise = matches!(
        kind,
        OrderKind::Str | OrderKind::PaddedStr(_) | OrderKind::Bin | OrderKind::Fixed(_)
    );
    let layout = Layout {
        page_rows: rng.random_range(PAGE_ROWS),
        row_group_rows: rng
            .random_bool(0.5)
            .then(|| rng.random_range(ROW_GROUP_ROWS)),
        column_index_truncate: (bytewise && rng.random_bool(0.5)).then_some(TRUNCATE_INDEX),
        order_statistics: if rng.random_bool(0.15) {
            Statistics::None
        } else {
            Statistics::Full
        },
        order_dictionary: rng.random_bool(0.15),
        dictionary: if rng.random_bool(0.15) {
            Dictionary::Value
        } else {
            Dictionary::Off
        },
        ..Layout::contract()
    };
    let spec = FixtureSpec {
        ids: DRAWN_IDS,
        buckets: pick(rng, &BUCKETS),
        value_bytes: DRAWN_VALUE_BYTES,
        nulls: if rng.random_bool(0.3) {
            Nulls::EveryNth(NULL_EVERY)
        } else {
            Nulls::None
        },
        second_value: rng.random_bool(0.5),
        layout,
        ..FixtureSpec::ordered(
            name,
            pick(rng, &KEY_TYPES),
            Ordered {
                kind,
                rows_per_key: pick(rng, &ROWS_PER_KEY),
                start: ORDER_START,
                step: rng.random_range(ORDER_STEPS),
            },
        )
    };
    let max_scan_rows = if rng.random_bool(0.5) {
        rng.random_range(SMALL_MAX_SCAN_ROWS)
    } else {
        icetray_core::DEFAULT_MAX_SCAN_ROWS
    };
    (spec, max_scan_rows)
}

/// The raw order values of `id`'s live rows, ascending.
fn live_orders(fixture: &Fixture, id: u64) -> Vec<i64> {
    let all = TestScan {
        direction: Direction::OldestFirst,
        limit: u32::MAX,
        ..TestScan::default()
    };
    fixture
        .expected_scan(&fixture.key(id), &[], &all)
        .0
        .into_iter()
        .map(|(order, _)| order)
        .collect()
}

/// Every live (id, raw order value) pair of `fixture`.
fn live_pairs(fixture: &Fixture, ids: Range<u64>) -> Vec<(u64, i64)> {
    ids.flat_map(|id| {
        live_orders(fixture, id)
            .into_iter()
            .map(move |order| (id, order))
    })
    .collect()
}

/// Up to `most` distinct live pairs of `fixture`, drawn by `rng`.
fn some_live(fixture: &Fixture, rng: &mut StdRng, most: usize) -> Vec<(u64, i64)> {
    let mut live = live_pairs(fixture, DRAWN_IDS.start..DRAWN_IDS.end + NEW_IDS);
    let mut drawn = Vec::new();
    for _ in 0..rng.random_range(1..=most) {
        if live.is_empty() {
            break;
        }
        drawn.push(live.swap_remove(rng.random_range(0..live.len())));
    }
    drawn.sort_unstable();
    drawn
}

/// Rows to append, none a live pair: per row an id, new ones included, and
/// a raw order value past its last row, before its first, or between two,
/// ascending by id and then order value.
fn new_rows(fixture: &Fixture, rng: &mut StdRng, step: i64) -> Vec<(u64, i64)> {
    let mut rows: Vec<(u64, i64)> = Vec::new();
    for _ in 0..rng.random_range(1..=MOST_ROWS_PER_CHANGE) {
        let id = rng.random_range(DRAWN_IDS.start..DRAWN_IDS.end + NEW_IDS);
        let live = live_orders(fixture, id);
        let order = match (live.first(), live.last(), rng.random_range(0..3)) {
            (Some(&first), _, 0) => first - 1,
            (_, Some(&last), 1) => last + step * rng.random_range(1..3),
            (Some(_), Some(_), _) => live[rng.random_range(0..live.len())] + 1,
            (None, _, _) | (_, None, _) => ORDER_START,
        };
        if !live.contains(&order) && !rows.contains(&(id, order)) {
            rows.push((id, order));
        }
    }
    rows.sort_unstable();
    rows
}

/// A random ordered table, named after `instance`, written and changed:
/// appends, a compaction now and then, deletes, and updates, each drawn by
/// `rng`.
async fn draw_table(rng: &mut StdRng, instance: usize) -> Result<Drawn> {
    let (spec, max_scan_rows) = random_ordered_spec(rng, &format!("drawn_{instance}"));
    let ordered = spec.ordered.expect("an ordered specification");
    let (dir, catalog) = local_catalog().await?;
    let mut fixture =
        write_fixture(catalog.as_ref(), icetray_testkit::fixture::NAMESPACE, spec).await?;
    for at in 0..rng.random_range(0..=MOST_APPENDS) {
        let rows = new_rows(&fixture, rng, ordered.step);
        fixture
            .append_ordered(catalog.as_ref(), &rows, &format!("append_{at}"))
            .await?;
    }
    if rng.random_bool(0.2) {
        fixture.compact("compacted").await?;
    }
    // One form per table: a position delete of a file that a deletion
    // vector already covers is masked by the vector (RFD 021), which no
    // writer leaves.
    let form = pick(rng, &FORMS);
    let mut changed = false;
    for at in 0..rng.random_range(0..=MOST_DELETES) {
        let rows = some_live(&fixture, rng, MOST_ROWS_PER_CHANGE);
        if !rows.is_empty() {
            changed = true;
            fixture
                .delete_rows(&rows, form, &format!("delete_{at}"))
                .await?;
        }
    }
    for at in 0..rng.random_range(0..=MOST_UPDATES) {
        let rows = some_live(&fixture, rng, MOST_ROWS_PER_CHANGE);
        if !rows.is_empty() {
            changed = true;
            fixture
                .update_rows(&rows, form, &format!("update_{at}"))
                .await?;
        }
    }
    Ok(Drawn {
        fixture,
        _dir: dir,
        kind: ordered.kind,
        max_scan_rows,
        changed,
    })
}

/// The value columns `fixture` writes.
fn written_columns(fixture: &Fixture) -> Vec<&'static str> {
    if fixture.spec().second_value {
        vec![VALUE_COLUMN, SECOND_VALUE_COLUMN]
    } else {
        vec![VALUE_COLUMN]
    }
}

/// A raw bound for a key whose live rows are `live`: at a row, beside one,
/// or past every row on either side, never negative.
fn random_bound(rng: &mut StdRng, live: &[i64]) -> i64 {
    let (first, last) = match (live.first(), live.last()) {
        (Some(&first), Some(&last)) => (first, last),
        (None, _) | (_, None) => (ORDER_START, ORDER_START),
    };
    match rng.random_range(0..5) {
        0 => first - 1,
        1 => last + 1,
        2 | 3 if !live.is_empty() => live[rng.random_range(0..live.len())],
        _ => rng.random_range(first - 1..=last + 1),
    }
}

/// A random Scan of `drawn`: a key it holds or one it does not, the
/// columns in a random order, a direction, a limit up to its
/// `max_scan_rows` drawn around the key's live rows, and bounds at, beside,
/// and past them.
fn random_scan(drawn: &Drawn, rng: &mut StdRng) -> (Key, Vec<&'static str>, TestScan) {
    let fixture = &drawn.fixture;
    let ids: Vec<u64> = fixture.ids().collect();
    let id = if ids.is_empty() || rng.random_bool(0.1) {
        DRAWN_IDS.end + NEW_IDS
    } else {
        pick(rng, &ids)
    };
    let key = if rng.random_bool(0.05) {
        fixture.absent_near(id)
    } else {
        fixture.key(id)
    };
    let mut columns = written_columns(fixture);
    if rng.random_bool(0.5) {
        columns.reverse();
    }
    columns.truncate(rng.random_range(0..=columns.len()));
    let live = live_orders(fixture, id);
    let held = u32::try_from(live.len()).expect("a key's rows fit u32");
    let limit = match rng.random_range(0..5) {
        0 => drawn.max_scan_rows,
        1 => held,
        2 => held.saturating_sub(1),
        3 => 1,
        _ => rng.random_range(1..=drawn.max_scan_rows),
    }
    .clamp(1, drawn.max_scan_rows);
    let mut start = rng.random_bool(0.4).then(|| random_bound(rng, &live));
    let before = rng.random_bool(0.4).then(|| random_bound(rng, &live));
    if let (Some(low), Some(high)) = (start, before)
        && low >= high
    {
        start = None;
    }
    let scan = TestScan {
        direction: if rng.random_bool(0.5) {
            Direction::NewestFirst
        } else {
            Direction::OldestFirst
        },
        limit,
        start,
        before,
    };
    (key, columns, scan)
}

/// `columns` as whole-value reads.
fn reads<'a>(columns: &[&'a str]) -> Vec<ColumnRead<'a>> {
    columns
        .iter()
        .map(|&name| ColumnRead::whole(name))
        .collect()
}

/// Every drawn table, served in every tier, answers every drawn Scan as
/// the naive sort of its live rows does under half-open bounds: the rows,
/// their values, and `next`, in either direction, a key's rows in one file
/// or across several.
#[tokio::test]
async fn test_scan_matches_the_model_in_every_tier() -> Result<()> {
    let mut rng = StdRng::seed_from_u64(SCAN_SEED);
    let mut rows_compared = 0;
    // Scans that left rows past the page, by direction, and of those, the
    // scans whose key's live rows lie in two or more files.
    let (mut next_newest, mut next_oldest, mut next_across_files) = (0, 0, 0);
    let mut kinds = Vec::new();
    let (mut truncated, mut unindexed) = (false, false);
    // Scans whose key's live rows lie in two or more data files, scans of
    // a table a delete or an update landed on, and scans of a key with a
    // deleted copy, which the files themselves tell (`MergeOracle`).
    let (mut across_files, mut of_changed, mut past_deleted) = (0, 0, 0);
    for instance in 0..INSTANCES {
        let drawn = draw_table(&mut rng, instance).await?;
        let oracle = MergeOracle::read(&drawn.fixture).await?;
        if !kinds.contains(&drawn.kind) {
            kinds.push(drawn.kind);
        }
        let layout = drawn.fixture.spec().layout;
        truncated |= layout.column_index_truncate.is_some();
        unindexed |= layout.order_statistics == Statistics::None;
        let tiers = serve_drawn(&drawn).await?;
        for _ in 0..KEYS_PER_INSTANCE {
            let (key, columns, scan) = random_scan(&drawn, &mut rng);
            let want = drawn.fixture.expected_scanned(&key, &columns, &scan);
            let bounds = scan.bounds(drawn.kind);
            for (residency, served) in tiers.all() {
                let got = served.table.scan(&key, &reads(&columns), &bounds).await?;
                assert_eq!(
                    got.scanned,
                    want,
                    "instance {instance} {residency} {key} {columns:?} {scan:?} of {:?}",
                    drawn.fixture.spec()
                );
            }
            rows_compared += want.rows.len();
            let copies = oracle.copies(&key);
            let files: HashSet<&str> = copies
                .iter()
                .filter(|copy| !copy.deleted)
                .map(|copy| copy.path.as_str())
                .collect();
            across_files += usize::from(files.len() >= 2);
            if want.next.is_some() {
                match scan.direction {
                    Direction::NewestFirst => next_newest += 1,
                    Direction::OldestFirst => next_oldest += 1,
                }
                next_across_files += usize::from(files.len() >= 2);
            }
            of_changed += usize::from(drawn.changed);
            past_deleted += usize::from(copies.iter().any(|copy| copy.deleted));
        }
    }
    assert!(across_files > 0, "some scans read a key from several files");
    assert!(
        of_changed > 0,
        "some scans read a table with deletes or updates"
    );
    assert!(
        past_deleted > 0,
        "some scans read a key with a deleted copy"
    );
    assert!(rows_compared > 0, "the scans compared rows");
    assert!(
        next_newest > 0 && next_oldest > 0,
        "some scans in each direction left rows past the limit"
    );
    assert!(
        next_across_files > 0,
        "some scans across files left rows past the limit"
    );
    assert_eq!(
        kinds.len(),
        ORDER_KINDS.len(),
        "every order kind: {kinds:?}"
    );
    assert!(truncated, "some table's column index is truncated");
    assert!(unindexed, "some table's order column has no column index");
    Ok(())
}

/// Random tables the fence test draws.
const FENCE_INSTANCES: usize = 12;

/// One row of a data file as the oracle reads it: its key, its order value,
/// and whether a delete names it.
struct FileRow {
    key: Key,
    order: OrderValue,
    deleted: bool,
}

/// Every row of every live data file `oracle` read, by file path, in
/// position order, deleted rows included.
fn rows_by_file(oracle: &MergeOracle) -> HashMap<String, Vec<FileRow>> {
    let mut by_position: HashMap<String, Vec<(u64, FileRow)>> = HashMap::new();
    for key in oracle.keys() {
        for copy in oracle.copies(key) {
            let row = FileRow {
                key: key.clone(),
                order: copy
                    .order
                    .clone()
                    .expect("an ordered file's row has an order value"),
                deleted: copy.deleted,
            };
            by_position
                .entry(copy.path.clone())
                .or_default()
                .push((copy.position, row));
        }
    }
    by_position
        .into_iter()
        .map(|(path, mut rows)| {
            rows.sort_unstable_by_key(|(position, _)| *position);
            let positions = rows.iter().map(|(position, _)| *position);
            assert!(
                positions.eq(0..count(rows.len())),
                "{path}: every position once"
            );
            (path, rows.into_iter().map(|(_, row)| row).collect())
        })
        .collect()
}

/// Asserts that `file`'s order fences bound its order pages over `rows`,
/// every row of the file: each fence above the pair before its page's
/// first row and at or below that row's, and every row placed on the page
/// holding it. The pages fenced, and those whose first row, or the row
/// before it, a delete names.
fn assert_fences_bound(file: &LoadedFile, rows: &[FileRow]) -> (usize, usize) {
    let index = &file.index;
    let fences = index
        .order_fences()
        .expect("an ordered file has order fences");
    let metadata = index.metadata();
    let leaf = index
        .order_leaf()
        .expect("an ordered file has an order column");
    let pair = |row: usize| Pair::of(&rows[row].key, &rows[row].order);
    let (mut pages, mut at_deleted, mut start) = (0, 0, 0);
    for row_group in 0..metadata.num_row_groups() {
        let locations = metadata
            .offset_index()
            .expect("a served file has an offset index")[row_group][leaf]
            .page_locations();
        assert_eq!(fences.pages(row_group), locations.len());
        let first_rows: Vec<usize> = locations
            .iter()
            .map(|location| {
                let first = usize::try_from(location.first_row_index);
                start + first.expect("inspect checked first rows non-negative")
            })
            .collect();
        for (page, &row) in first_rows.iter().enumerate() {
            let fence = fences.fence(row_group, page);
            assert!(fence <= pair(row), "{} page {page}", file.entry.path);
            if let Some(before) = row.checked_sub(1) {
                assert!(pair(before) < fence, "{} page {page}", file.entry.path);
                at_deleted += usize::from(rows[before].deleted || rows[row].deleted);
            }
            pages += 1;
        }
        let group_rows = usize::try_from(metadata.row_group(row_group).num_rows())
            .expect("inspect checked row counts non-negative");
        for row in start..start + group_rows {
            let holding = first_rows.partition_point(|&first| first <= row) - 1;
            assert_eq!(
                fences.page_of(row_group, pair(row)),
                Some(holding),
                "{} row {row}",
                file.entry.path
            );
        }
        start += group_rows;
    }
    assert_eq!(start, rows.len(), "{}", file.entry.path);
    (pages, at_deleted)
}

/// Every order fence of every file of a drawn table, served in every tier,
/// bounds its order page against all of the file's rows, the rows a delete
/// names included, since fences bound positions (RFD 025, Load): above the
/// pair before the page's first row and at or below that row's, and every
/// row placed on the page holding it.
#[tokio::test]
async fn test_order_fences_bound_every_page_of_every_file_deleted_rows_included() -> Result<()> {
    let mut rng = StdRng::seed_from_u64(SCAN_SEED ^ 2);
    let (mut pages, mut at_deleted, mut files_with_deleted) = (0, 0, 0);
    let mut kinds = HashSet::new();
    for instance in 0..FENCE_INSTANCES {
        let drawn = draw_table(&mut rng, instance).await?;
        kinds.insert(format!("{:?}", drawn.kind));
        let oracle = MergeOracle::read(&drawn.fixture).await?;
        let rows = rows_by_file(&oracle);
        let tiers = serve_drawn(&drawn).await?;
        for (_, served) in tiers.all() {
            for file in &served.current().files {
                let rows = &rows[&file.entry.path];
                let (fenced, deleted) = assert_fences_bound(file, rows);
                pages += fenced;
                at_deleted += deleted;
                files_with_deleted += usize::from(rows.iter().any(|row| row.deleted));
            }
        }
    }
    assert!(pages > 0, "the files have order pages");
    assert!(files_with_deleted > 0, "some file holds deleted rows");
    assert!(at_deleted > 0, "some page starts at or after a deleted row");
    assert!(kinds.len() > 1, "several order kinds: {kinds:?}");
    Ok(())
}

/// A `MultiScan` of drawn keys, repeats included, answers each position as
/// a Scan of its key does, from the one snapshot the table serves.
#[tokio::test]
async fn test_multi_scan_matches_scan_for_each_key_from_one_snapshot() -> Result<()> {
    let mut rng = StdRng::seed_from_u64(SCAN_SEED ^ 1);
    let mut keys_answered = 0;
    for instance in 0..MULTI_SCAN_INSTANCES {
        let mut drawn = draw_table(&mut rng, instance).await?;
        drawn.max_scan_rows = drawn.max_scan_rows.min(SMALL_MAX_SCAN_ROWS.end);
        let tiers = serve_drawn(&drawn).await?;
        let (first_key, columns, scan) = random_scan(&drawn, &mut rng);
        let mut keys = vec![first_key];
        for _ in 1..rng.random_range(KEYS_PER_MULTI_SCAN) {
            keys.push(random_scan(&drawn, &mut rng).0);
        }
        let repeated = keys[0].clone();
        keys.push(repeated);
        let bounds = scan.bounds(drawn.kind);
        for (residency, served) in tiers.all() {
            let got = served
                .table
                .multi_scan(&keys, &reads(&columns), &bounds)
                .await?;
            assert_eq!(got.snapshot_id, served.current().snapshot_id);
            assert_eq!(got.results.len(), keys.len());
            for (key, answer) in keys.iter().zip(&got.results) {
                let one = served.table.scan(key, &reads(&columns), &bounds).await?;
                assert_eq!(answer, &one.scanned, "{residency} {key}");
                assert_eq!(
                    answer,
                    &drawn.fixture.expected_scanned(key, &columns, &scan),
                    "{residency} {key} {scan:?}"
                );
            }
            keys_answered += got.results.len();
        }
    }
    assert!(keys_answered > 0);
    Ok(())
}

/// Rows per key of the hand-built tables below.
const HISTORY_ROWS: u64 = 12;

/// The step between a hand-built key's rows: room for an append between
/// two of them.
const HISTORY_STEP: i64 = 4;

/// Ids of the hand-built tables.
const HISTORY_IDS: Range<u64> = 0..8;

/// Rows per page of the hand-built tables: few, so a key's `HISTORY_ROWS`
/// rows span several pages.
const HISTORY_PAGE_ROWS: usize = 2;

/// Rows per row group of the hand-built tables: few, so a key's
/// `HISTORY_ROWS` rows span several row groups.
const HISTORY_GROUP_ROWS: usize = 5;

/// A hand-built ordered table of `kind`: `HISTORY_IDS`, `HISTORY_ROWS`
/// each, in small pages and row groups, two value columns, written to a
/// fresh catalog.
async fn history_table(
    name: &str,
    kind: OrderKind,
) -> Result<(TempDir, Arc<dyn Catalog>, Fixture)> {
    let (dir, catalog) = local_catalog().await?;
    let spec = FixtureSpec {
        ids: HISTORY_IDS,
        buckets: ONE_BUCKET,
        second_value: true,
        value_bytes: DRAWN_VALUE_BYTES,
        layout: Layout {
            page_rows: HISTORY_PAGE_ROWS,
            row_group_rows: Some(HISTORY_GROUP_ROWS),
            ..Layout::contract()
        },
        ..FixtureSpec::ordered(
            name,
            KeyType::Long,
            Ordered {
                kind,
                rows_per_key: RowsPerKey::Each(HISTORY_ROWS),
                start: ORDER_START,
                step: HISTORY_STEP,
            },
        )
    };
    let fixture =
        write_fixture(catalog.as_ref(), icetray_testkit::fixture::NAMESPACE, spec).await?;
    Ok((dir, catalog, fixture))
}

/// The page size the paging test reads a history in.
const PAGE_LIMIT: u32 = 3;

/// Rows of each block the paging test appends below and above a key's
/// history: more than two pages, so a file alone holds rows past a page.
const PAGING_BLOCK_ROWS: i64 = 7;

/// A fixed width that the order column stores as 16 bytes, a uuid's.
const SIXTEEN_BYTE_FIXED: u32 = 16;

/// A key's whole history, read in pages as a caller pages (RFD 025,
/// Paging), each request passing the previous response's `next` back
/// unchanged, as `before` reading newest first and as `start` reading
/// oldest first, returns every live row once, in order: across the files an
/// append and an update left, across row groups, around deleted rows, and
/// across two blocks of rows appended below and above the history, whose
/// file alone holds the rows past the pages at either end, in a long, a
/// string, and a 16-byte fixed order column. Each page is the model's for
/// the request that read it, its `next` the one that follows its last row,
/// and the last page carries no `next`.
#[tokio::test]
async fn test_paging_by_next_returns_every_live_row_once() -> Result<()> {
    for kind in [
        OrderKind::Long,
        OrderKind::Str,
        OrderKind::Fixed(SIXTEEN_BYTE_FIXED),
    ] {
        let (_dir, catalog, mut fixture) = history_table(&format!("paged_{kind:?}"), kind).await?;
        let id = 3;
        let last = ORDER_START + HISTORY_STEP * i64::try_from(HISTORY_ROWS - 1).unwrap();
        fixture
            .append_ordered(
                catalog.as_ref(),
                &[
                    (id, ORDER_START + 1),
                    (id, ORDER_START + HISTORY_STEP + 1),
                    (id, last + 1),
                ],
                "between",
            )
            .await?;
        let block = |first: i64| -> Vec<(u64, i64)> {
            (0..PAGING_BLOCK_ROWS)
                .map(|at| (id, first + at * HISTORY_STEP))
                .collect()
        };
        let below = ORDER_START - PAGING_BLOCK_ROWS * HISTORY_STEP;
        fixture
            .append_ordered(catalog.as_ref(), &block(below), "below")
            .await?;
        fixture
            .append_ordered(catalog.as_ref(), &block(last + HISTORY_STEP), "above")
            .await?;
        fixture
            .delete_rows(
                &[(id, ORDER_START), (id, ORDER_START + 2 * HISTORY_STEP)],
                DeleteForm::Vectors,
                "gone",
            )
            .await?;
        fixture
            .update_rows(&[(id, ORDER_START + 1)], DeleteForm::Vectors, "rewritten")
            .await?;
        let key = fixture.key(id);
        let columns = [VALUE_COLUMN, SECOND_VALUE_COLUMN];
        let tiers = serve_in_every_tier(|residency| fixture.config(residency, false)).await?;
        for direction in [Direction::NewestFirst, Direction::OldestFirst] {
            let all = TestScan {
                direction,
                ..TestScan::default()
            };
            let whole = fixture.expected_scanned(&key, &columns, &all);
            let page_rows = usize::try_from(PAGE_LIMIT).unwrap();
            assert!(whole.rows.len() > page_rows * 2);
            assert!(
                whole.rows.len() > HISTORY_GROUP_ROWS,
                "the history spans row groups"
            );
            for (residency, served) in tiers.all() {
                let first = TestScan {
                    direction,
                    limit: PAGE_LIMIT,
                    ..TestScan::default()
                };
                let model = |scan: &TestScan| {
                    let last = fixture.expected_scan(&key, &[], scan).1;
                    (fixture.expected_scanned(&key, &columns, scan), last)
                };
                let (read, _) = verify::page_scans(
                    &served.table,
                    &key,
                    &reads(&columns),
                    (first, first.bounds(kind)),
                    model,
                    &format!("{kind:?} {residency}"),
                )
                .await?;
                assert_eq!(read, whole.rows, "{kind:?} {residency} {direction:?}");
            }
        }
    }
    Ok(())
}

/// `next` counts live rows alone: with rows deleted at both edges of a
/// key's run and inside it, a limit of the run's live rows leaves no
/// `next` in either direction, one less sets it, and the deleted rows at
/// the edge are never returned or counted.
#[tokio::test]
async fn test_scan_next_counts_live_rows_only() -> Result<()> {
    let (_dir, _catalog, mut fixture) = history_table("edges", OrderKind::Long).await?;
    let id = 5;
    let held = live_orders(&fixture, id);
    let deleted = [held[0], held[1], held[held.len() / 2], held[held.len() - 1]];
    let pairs: Vec<(u64, i64)> = deleted.iter().map(|&order| (id, order)).collect();
    fixture
        .delete_rows(&pairs, DeleteForm::Positions, "edges")
        .await?;
    let live = u32::try_from(live_orders(&fixture, id).len()).unwrap();
    assert_eq!(usize::try_from(live).unwrap(), held.len() - deleted.len());
    let key = fixture.key(id);
    let tiers = serve_in_every_tier(|residency| fixture.config(residency, false)).await?;
    for direction in [Direction::NewestFirst, Direction::OldestFirst] {
        for (limit, next) in [(live, false), (live - 1, true)] {
            let bounds = ScanBounds {
                direction,
                limit,
                start: None,
                before: None,
            };
            for (residency, served) in tiers.all() {
                let got = served
                    .table
                    .scan(&key, &reads(&[VALUE_COLUMN]), &bounds)
                    .await?
                    .scanned;
                assert_eq!(
                    got.next.is_some(),
                    next,
                    "{residency} {direction:?} limit {limit}"
                );
                assert_eq!(got.rows.len(), usize::try_from(limit).unwrap());
                assert!(
                    got.rows.iter().all(|row| !deleted
                        .iter()
                        .any(|&order| row.order == OrderValue::Long(order))),
                    "{residency}: no deleted row"
                );
            }
        }
    }
    Ok(())
}

/// A bound beyond the 32-bit range of an `int` or `date` column.
const PAST_32_BITS: i64 = 1 << 40;

/// Bounds are half-open at the extremes: `start` at `i64::MIN` and `before`
/// at `i64::MAX` admit every row of a long, `int`, or `date` column, and
/// `start` at `i64::MAX` or `before` at `i64::MIN` none; an `int` or `date`
/// bound beyond the 32-bit range does the same; and a bound equal to
/// a stored value, long or string, includes that row as `start` and
/// excludes it as `before`, admitting the rest on its side.
#[tokio::test]
async fn test_scan_bounds_are_half_open_at_the_extremes() -> Result<()> {
    let id = 2;
    let columns = [VALUE_COLUMN];
    for kind in [
        OrderKind::Long,
        OrderKind::Int,
        OrderKind::Date,
        OrderKind::Str,
    ] {
        let (_dir, _catalog, fixture) = history_table(&format!("extremes_{kind:?}"), kind).await?;
        let key = fixture.key(id);
        let held = live_orders(&fixture, id);
        let stored = held[held.len() / 2];
        let stored_value = order_value(kind, stored);
        let oldest = |start: Option<i64>, before: Option<i64>| TestScan {
            direction: Direction::OldestFirst,
            start,
            before,
            ..TestScan::default()
        };
        // The extreme bounds a long bound of the kind takes: i64's ends for
        // every integer kind, and for an `int` or `date` bounds past its
        // 32-bit range too.
        let extremes: &[(i64, i64)] = match kind {
            OrderKind::Long => &[(i64::MIN, i64::MAX)],
            OrderKind::Int | OrderKind::Date => {
                &[(i64::MIN, i64::MAX), (-PAST_32_BITS, PAST_32_BITS)]
            }
            OrderKind::Timestamp
            | OrderKind::TimestampTz
            | OrderKind::TimestampNs
            | OrderKind::TimestampTzNs
            | OrderKind::Str
            | OrderKind::PaddedStr(_)
            | OrderKind::Bin
            | OrderKind::Fixed(_) => &[],
        };
        // Each case: the bounds as sent, the model's scan, and the rows it
        // admits.
        let mut cases: Vec<(ScanBounds, TestScan, Admits)> = Vec::new();
        for &(low, high) in extremes {
            for (start, before, admits) in [
                (Some(low), None, Admits::Every),
                (None, Some(high), Admits::Every),
                (Some(low), Some(high), Admits::Every),
                (Some(high), None, Admits::None),
                (None, Some(low), Admits::None),
            ] {
                let scan = oldest(start, before);
                let bounds = ScanBounds {
                    start: start.map(OrderValue::Long),
                    before: before.map(OrderValue::Long),
                    ..oldest(None, None).bounds(kind)
                };
                cases.push((bounds, scan, admits));
            }
        }
        for (start, before, admits) in [
            (Some(stored), None, Admits::Stored),
            (None, Some(stored), Admits::AllBelowStored),
        ] {
            let scan = oldest(start, before);
            cases.push((scan.bounds(kind), scan, admits));
        }
        let tiers = serve_in_every_tier(|residency| fixture.config(residency, false)).await?;
        for (bounds, scan, admits) in cases {
            let want = fixture.expected_scanned(&key, &columns, &scan);
            let orders: Vec<&OrderValue> = want.rows.iter().map(|row| &row.order).collect();
            match admits {
                Admits::Every => assert_eq!(orders.len(), held.len(), "{kind:?} {scan:?}"),
                Admits::None => assert!(orders.is_empty(), "{kind:?} {scan:?}"),
                Admits::Stored => assert_eq!(
                    orders.first(),
                    Some(&&stored_value),
                    "{kind:?}: start at a stored value includes its row"
                ),
                Admits::AllBelowStored => {
                    assert!(!orders.is_empty(), "{kind:?} {scan:?}");
                    assert!(
                        !orders.contains(&&stored_value),
                        "{kind:?}: before at a stored value excludes its row"
                    );
                }
            }
            for (residency, served) in tiers.all() {
                let got = served.table.scan(&key, &reads(&columns), &bounds).await?;
                assert_eq!(got.scanned, want, "{residency} {kind:?} {bounds:?}");
            }
        }
    }
    Ok(())
}

/// What an extreme or stored bound of the half-open bounds test admits.
#[derive(Clone, Copy, Debug)]
enum Admits {
    /// Every row of the key.
    Every,
    /// No row.
    None,
    /// The rows from the stored value up, its own first.
    Stored,
    /// Some rows, all below the stored value.
    AllBelowStored,
}

/// Ids of the compacted tables, `COMPACTED_ROWS` rows each, over two
/// buckets, so a middle key's run lies inside one key page of one row group.
const COMPACTED_IDS: Range<u64> = 0..40;
const COMPACTED_ROWS: u64 = 3;

/// A compacted ordered spec named `name`: `COMPACTED_IDS` in the contract
/// layout, one file per bucket, one row group, no dictionary-encoded
/// column, both value columns.
fn compacted_spec(name: &str) -> FixtureSpec {
    FixtureSpec {
        ids: COMPACTED_IDS,
        buckets: TWO_BUCKETS,
        second_value: true,
        ..FixtureSpec::ordered(
            name,
            KeyType::Long,
            Ordered {
                kind: OrderKind::Long,
                rows_per_key: RowsPerKey::Each(COMPACTED_ROWS),
                start: ORDER_START,
                step: HISTORY_STEP,
            },
        )
    }
}

/// `compacted_spec(name)` written to a fresh catalog.
async fn compacted_table(name: &str) -> Result<(TempDir, Arc<dyn Catalog>, Fixture)> {
    let (dir, catalog) = local_catalog().await?;
    let fixture = write_fixture(
        catalog.as_ref(),
        icetray_testkit::fixture::NAMESPACE,
        compacted_spec(name),
    )
    .await?;
    Ok((dir, catalog, fixture))
}

/// An id of `fixture` with another id of its bucket on each side, so its
/// run is neither the first nor the last of its file and both its edges
/// lie inside one key page.
fn middle_id(fixture: &Fixture, buckets: u32) -> u64 {
    let ids: Vec<u64> = fixture.ids().collect();
    let bucket_of = |id: u64| bucket(&fixture.key(id), buckets);
    ids.iter()
        .copied()
        .find(|&id| {
            let mates = || {
                ids.iter()
                    .filter(|&&other| bucket_of(other) == bucket_of(id))
            };
            mates().any(|&other| other < id) && mates().any(|&other| other > id)
        })
        .expect("a bucket of more than two ids")
}

/// The value columns a cost test reads, and what reading them costs on top
/// of the order column: one span GET each.
const BOTH: [&str; 2] = [VALUE_COLUMN, SECOND_VALUE_COLUMN];

/// The order column's one span over a run in one row group: a GET under
/// `full` and `bloom`, a read of the chunk file under `disk`.
const ORDER_SPAN: u64 = 1;

/// The key pages a run inside one key page decodes: both its edges lie in
/// that page, which is decoded once.
const KEY_PAGES_OF_A_MIDDLE_RUN: u64 = 1;

/// The GETs a cold Scan of a middle key reading `columns` value columns
/// costs in one compacted file, each a page the page cache fetches: one per
/// column and one for the order column, after the key page's under
/// `bloom`; under `disk` the key page and the order column are read from
/// the chunk file instead.
fn cold_scan_gets(residency: Residency, columns: u64) -> u64 {
    match residency {
        Residency::Full => columns + ORDER_SPAN,
        Residency::Disk => columns,
        Residency::Bloom => KEY_PAGES_OF_A_MIDDLE_RUN + columns + ORDER_SPAN,
    }
}

/// A cold Scan of a key held in one file, in one row group, with no
/// dictionary-encoded column and nothing cached, costs per tier (RFD 025,
/// Cost): under `full` one GET per requested column and
/// one for the order column; under `disk` one GET per requested column,
/// the order column and the key page read from the chunk file; under
/// `bloom` the key page's GET first, then the columns' and the order
/// column's.
#[tokio::test]
async fn test_cold_scan_of_a_compacted_table_costs_one_get_per_column_and_one_for_order()
-> Result<()> {
    let (_dir, _catalog, fixture) = compacted_table("cold").await?;
    let key = fixture.key(middle_id(&fixture, TWO_BUCKETS));
    let columns = count(BOTH.len());
    let tiers = serve_in_every_tier(|residency| fixture.config(residency, false)).await?;
    for (residency, served) in tiers.all() {
        let stats = served.storage.stats();
        let before = (stats.gets(), stats.local_reads());
        let got = served
            .table
            .scan(
                &key,
                &reads(&BOTH),
                &TestScan::default().bounds(OrderKind::Long),
            )
            .await?;
        assert_eq!(
            got.scanned,
            fixture.expected_scanned(&key, &BOTH, &TestScan::default())
        );
        let (gets, local_reads) = (stats.gets() - before.0, stats.local_reads() - before.1);
        let want_local = match residency {
            Residency::Full | Residency::Bloom => 0,
            Residency::Disk => KEY_PAGES_OF_A_MIDDLE_RUN + ORDER_SPAN,
        };
        assert_eq!(
            gets,
            cold_scan_gets(*residency, columns),
            "{residency} GETs"
        );
        assert_eq!(local_reads, want_local, "{residency} local reads");
    }
    Ok(())
}

/// Ids an interleaving append writes again, which the first file's range
/// covers but does not hold: every other id of the compacted table's.
const STRIDE: u64 = 2;

/// A Scan reads only the files that hold its key (RFD 025, Cost): a key
/// the first file holds, inside the range of an interleaved file that
/// does not, costs the one file's GETs; a key both hold costs both files'.
#[tokio::test]
async fn test_scan_reads_only_files_that_hold_the_key() -> Result<()> {
    let (_dir, catalog) = local_catalog().await?;
    let spec = FixtureSpec {
        id_stride: STRIDE,
        buckets: ONE_BUCKET,
        ..compacted_spec("files")
    };
    let mut fixture =
        write_fixture(catalog.as_ref(), icetray_testkit::fixture::NAMESPACE, spec).await?;
    let odd: Vec<u64> = COMPACTED_IDS.filter(|id| id % STRIDE == 1).collect();
    fixture.append(catalog.as_ref(), odd, "odd").await?;
    let even = middle_id(&fixture, ONE_BUCKET) + 1;
    let even = even + even % STRIDE;
    let shared = even + STRIDE;
    // Later rows of the shared key and its two neighbors, so its run lies
    // inside the third file's key page too, and the third file's range
    // leaves the first key out.
    let later: Vec<(u64, i64)> = [shared - 1, shared, shared + 1]
        .into_iter()
        .map(|id| {
            (
                id,
                live_orders(&fixture, id).last().copied().unwrap() + HISTORY_STEP,
            )
        })
        .collect();
    fixture
        .append_ordered(catalog.as_ref(), &later, "later")
        .await?;
    assert_eq!(fixture.file_count(), 3);
    let columns = count(BOTH.len());
    for (key, files) in [(fixture.key(even), 1), (fixture.key(shared), 2)] {
        // Served afresh for each key, so nothing the other key's Scan
        // cached, such as a page both keys' rows share, is warm.
        let tiers = serve_in_every_tier(|residency| fixture.config(residency, false)).await?;
        for (residency, served) in tiers.all() {
            let stats = served.storage.stats();
            let before = stats.gets();
            let scan = TestScan::default();
            let got = served
                .table
                .scan(&key, &reads(&BOTH), &scan.bounds(OrderKind::Long))
                .await?;
            assert_eq!(got.scanned, fixture.expected_scanned(&key, &BOTH, &scan));
            assert_eq!(
                stats.gets() - before,
                files * cold_scan_gets(*residency, columns),
                "{residency} {key}: the files holding the key alone"
            );
        }
    }
    Ok(())
}

/// A Get or `MultiGet` of an ordered table is `Error::WrongTableKind` naming
/// it ordered, in every tier, and a Scan or `MultiScan` of a key-value table
/// the same naming it key-value (RFD 025, Wire).
#[tokio::test]
async fn test_get_on_an_ordered_table_is_wrong_table_kind() -> Result<()> {
    let (_dir, _catalog, fixture) = compacted_table("kinds").await?;
    let key = fixture.key(middle_id(&fixture, TWO_BUCKETS));
    let wrong = |result: std::result::Result<(), Error>, expected: TableKind| match result {
        Err(Error::WrongTableKind { kind, .. }) => assert_eq!(kind, expected),
        other => panic!("expected the wrong kind, got {other:?}"),
    };
    let tiers = serve_in_every_tier(|residency| fixture.config(residency, false)).await?;
    for (_, served) in tiers.all() {
        wrong(
            served.table.get(&key, &reads(&BOTH)).await.map(drop),
            TableKind::Ordered,
        );
        wrong(
            served
                .table
                .multi_get(std::slice::from_ref(&key), &reads(&BOTH))
                .await
                .map(drop),
            TableKind::Ordered,
        );
    }
    let (_kv_dir, kv_catalog) = local_catalog().await?;
    let kv = write_fixture(
        kv_catalog.as_ref(),
        icetray_testkit::fixture::NAMESPACE,
        FixtureSpec {
            ids: COMPACTED_IDS,
            buckets: TWO_BUCKETS,
            ..FixtureSpec::small("plain", KeyType::Long)
        },
    )
    .await?;
    let kv_tiers = serve_in_every_tier(|residency| kv.config(residency, false)).await?;
    let kv_key = kv.key(COMPACTED_IDS.start);
    let scan = TestScan::default().bounds(OrderKind::Long);
    for (_, served) in kv_tiers.all() {
        wrong(
            served
                .table
                .scan(&kv_key, &reads(&[VALUE_COLUMN]), &scan)
                .await
                .map(drop),
            TableKind::KeyValue,
        );
        wrong(
            served
                .table
                .multi_scan(
                    std::slice::from_ref(&kv_key),
                    &reads(&[VALUE_COLUMN]),
                    &scan,
                )
                .await
                .map(drop),
            TableKind::KeyValue,
        );
    }
    Ok(())
}

/// An ordered table's pages, a Bloom-tier key page included, go through
/// the one page cache every table shares (RFD 027): a cold Scan fetches
/// each through it, and the same Scan again finds each there, a hit, with
/// no fetch and no GET.
#[tokio::test]
async fn test_scan_pages_go_through_the_one_page_cache() -> Result<()> {
    let (_dir, _catalog, fixture) = compacted_table("caches").await?;
    let key = fixture.key(middle_id(&fixture, TWO_BUCKETS));
    let columns = count(BOTH.len());
    let tiers = serve_in_every_tier(|residency| fixture.config(residency, false)).await?;
    let bounds = TestScan::default().bounds(OrderKind::Long);
    for (residency, served) in tiers.all() {
        let pages = cold_scan_gets(*residency, columns);
        served.table.scan(&key, &reads(&BOTH), &bounds).await?;
        assert_eq!(
            (served.cache.fetches(), served.cache.hits()),
            (pages, 0),
            "{residency}: every page the cold Scan fetched went through the page cache"
        );
        let gets = served.storage.stats().gets();
        served.table.scan(&key, &reads(&BOTH), &bounds).await?;
        assert_eq!(
            (served.cache.fetches(), served.cache.hits()),
            (pages, pages),
            "{residency}: the warm Scan found every page there"
        );
        assert_eq!(served.storage.stats().gets(), gets, "{residency}: no GET");
    }
    Ok(())
}

/// The default the added column states.
const ADDED_DEFAULT: &[u8] = b"added later";

/// A column added after a table's files were written answers its stated
/// default for every row of those files, read with no GET of it (RFD 023):
/// a Scan reading it beside the value column costs that column's GET and
/// the order column's alone.
#[tokio::test]
async fn test_scan_answers_a_column_added_after_a_file_with_its_default() -> Result<()> {
    let (_dir, catalog) = local_catalog().await?;
    let spec = FixtureSpec {
        format_version: FormatVersion::V3,
        second_value: false,
        ..compacted_spec("added")
    };
    let mut fixture =
        write_fixture(catalog.as_ref(), icetray_testkit::fixture::NAMESPACE, spec).await?;
    fixture
        .evolve(&[SchemaChange::Add {
            name: SECOND_VALUE_COLUMN.to_string(),
            ty: PrimitiveType::Binary,
            default: StatedDefault::Literal(Literal::binary(ADDED_DEFAULT.to_vec())),
        }])
        .await?;
    let data = fixture.files().to_vec();
    fixture
        .commit_by_hand(HandSnapshot {
            data,
            deletes: Vec::new(),
        })
        .await?;
    let key = fixture.key(middle_id(&fixture, TWO_BUCKETS));
    let scan = TestScan::default();
    let mut want = fixture.expected_scanned(&key, &[VALUE_COLUMN], &scan);
    assert!(!want.rows.is_empty());
    for row in &mut want.rows {
        row.values
            .insert(0, Some(Bytes::from_static(ADDED_DEFAULT)));
    }
    let asked = [SECOND_VALUE_COLUMN, VALUE_COLUMN];
    let tiers = serve_in_every_tier(|residency| fixture.config_serving(residency, &asked)).await?;
    for (residency, served) in tiers.all() {
        let stats = served.storage.stats();
        let before = stats.gets();
        let got = served
            .table
            .scan(&key, &reads(&asked), &scan.bounds(OrderKind::Long))
            .await?;
        assert_eq!(got.scanned, want, "{residency}");
        let read_columns = 1;
        assert_eq!(
            stats.gets() - before,
            cold_scan_gets(*residency, read_columns),
            "{residency}: no GET of the added column"
        );
    }
    Ok(())
}
