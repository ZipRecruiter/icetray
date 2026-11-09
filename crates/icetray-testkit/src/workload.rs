//! Lookup workloads the benchmark replays: a trace of requests read from
//! Parquet, sampled by key, and placed on a table's keys.
//!
//! A trace file holds one row per request, in the order the requests
//! arrived: `ts`, an int64 of microseconds, optional and read only to
//! report the span a run replayed, and `keys`, a list or large list of
//! int64, the ids of the keys the request looked up, in its order, repeats
//! kept. An id is any non-negative integer that identifies a key within
//! the trace; by convention it is the key's rank by lookups, 0 the most
//! read.
//!
//! The reader keeps a key when a seeded hash of its id falls under the
//! sample's rate of the hash space, so it keeps every lookup of a kept key
//! and none of another's, as SHARDS (Waldspurger et al., FAST 2015) samples
//! a trace to estimate a miss ratio curve: a cache scaled by the same rate
//! is then asked the reuse a full-size cache is asked. Placed on the same
//! share of a table's rows (`scatter_share`), the kept keys fill its pages
//! as densely as the whole trace's keys fill the whole table's, so a page
//! cache scaled by the rate holds the same share of the asked keys too.

use std::fmt;
use std::fs::File;
use std::path::Path;

use arrow::array::{Array, AsArray as _, GenericListArray, Int64Array, OffsetSizeTrait};
use arrow::datatypes::{DataType, Int64Type};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use rand::SeedableRng as _;
use rand::rngs::StdRng;
use rand::seq::SliceRandom as _;

use crate::error::Result;
use crate::keys::splitmix64;
use icetray_core::approx;

/// The column of a trace holding each request's arrival time.
pub const TS_COLUMN: &str = "ts";

/// The column of a trace holding each request's key ids.
pub const KEYS_COLUMN: &str = "keys";

/// Requests a trace's reader decodes at once: enough that a batch's
/// overhead is spread over many requests, few enough that a batch of the
/// largest requests stays a few megabytes.
const READ_BATCH_ROWS: usize = 8_192;

/// A column of a trace file.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TraceColumn {
    /// `TS_COLUMN`.
    Ts,
    /// `KEYS_COLUMN`.
    Keys,
}

impl fmt::Display for TraceColumn {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Ts => TS_COLUMN,
            Self::Keys => KEYS_COLUMN,
        })
    }
}

/// What makes a workload unreadable or unplaceable; closed.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum WorkloadFault {
    /// The trace has no `KEYS_COLUMN`.
    #[error("the trace has no {KEYS_COLUMN} column")]
    NoKeys,
    /// A column of the trace is not of its type: `ts` int64, `keys` a list
    /// of int64.
    #[error("the trace's {column} column is {found}")]
    ColumnType {
        /// The column.
        column: TraceColumn,
        /// The type the file gives it.
        found: DataType,
    },
    /// A request of the trace has a null where a value belongs.
    #[error("request {request} of the trace has a null in its {column}")]
    Null {
        /// The request's place in the file, from zero.
        request: u64,
        /// The column holding the null.
        column: TraceColumn,
    },
    /// A request names a negative id.
    #[error("request {request} of the trace names id {id}, below zero")]
    NegativeId {
        /// The request's place in the file, from zero.
        request: u64,
        /// The id.
        id: i64,
    },
    /// A sample rate that is not a decimal in (0, 1].
    #[error("a sample rate of {text:?} is not a decimal in (0, 1]")]
    SampleRate {
        /// The rate as written.
        text: String,
    },
    /// More distinct keys than the table has rows to place them on.
    #[error("{keys} distinct keys do not fit a table of {rows} rows")]
    TooManyKeys {
        /// The distinct keys to place.
        keys: u64,
        /// The table's rows.
        rows: u64,
    },
    /// More distinct keys than a `u32` place numbers.
    #[error("{keys} distinct keys are more than a u32 numbers")]
    TooManyDistinct {
        /// The distinct keys.
        keys: u64,
    },
}

/// Requests of key ids in order: one slice of ids per request, held in one
/// buffer, so a trace of a hundred million lookups costs its ids alone.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Requests<Id> {
    ids: Vec<Id>,
    /// Each request's end in `ids`; a request starts where the one before
    /// it ends.
    ends: Vec<usize>,
}

impl<Id> Default for Requests<Id> {
    fn default() -> Self {
        Self {
            ids: Vec::new(),
            ends: Vec::new(),
        }
    }
}

impl<Id: Copy> Requests<Id> {
    /// No requests.
    pub fn new() -> Self {
        Self::default()
    }

    /// Appends a request of `ids`, in their order.
    pub fn push(&mut self, ids: &[Id]) {
        self.ids.extend_from_slice(ids);
        self.ends.push(self.ids.len());
    }

    /// The requests held.
    pub fn len(&self) -> usize {
        self.ends.len()
    }

    /// Whether no request is held.
    pub fn is_empty(&self) -> bool {
        self.ends.is_empty()
    }

    /// Request `i`'s ids, in its order. Panics when `i` is not below `len`.
    pub fn get(&self, i: usize) -> &[Id] {
        let start = match i {
            0 => 0,
            _ => self.ends[i - 1],
        };
        &self.ids[start..self.ends[i]]
    }

    /// The lookups of every request together.
    pub fn lookups(&self) -> usize {
        self.ids.len()
    }

    /// Every request's ids, in order.
    pub fn iter(&self) -> impl Iterator<Item = &[Id]> {
        (0..self.len()).map(|i| self.get(i))
    }
}

impl Requests<u64> {
    /// The requests with each id replaced by its place among the distinct
    /// ids in ascending order, and those ids, so the most read key of a
    /// trace ranked by lookups is place 0. More distinct ids than a `u32`
    /// numbers is `WorkloadFault::TooManyDistinct`.
    pub fn densified(&self) -> Result<(Requests<u32>, Vec<u64>)> {
        let mut distinct = self.ids.clone();
        distinct.sort_unstable();
        distinct.dedup();
        if u32::try_from(distinct.len()).is_err() {
            return Err(WorkloadFault::TooManyDistinct {
                keys: approx::count(distinct.len()),
            }
            .into());
        }
        let ids = self
            .ids
            .iter()
            .map(|id| {
                let place = distinct
                    .binary_search(id)
                    .expect("every id is among the distinct ids");
                u32::try_from(place).expect("the distinct ids fit a u32")
            })
            .collect();
        Ok((
            Requests {
                ids,
                ends: self.ends.clone(),
            },
            distinct,
        ))
    }
}

/// A share of a trace's keys, from above zero to one, held as the
/// decimal it was written as, `parts` over a power of ten, so a sample and
/// the budget scaled by it are exact integer arithmetic.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SampleRate {
    parts: u64,
    scale: u64,
}

/// The most digits a `SampleRate` takes after its point: eighteen, so its
/// scale, ten to that power, fits a `u64`.
const SAMPLE_RATE_DIGITS: usize = 18;

/// The base of a `SampleRate`'s digits.
const DECIMAL: u64 = 10;

impl SampleRate {
    /// Every key.
    pub const ALL: Self = Self { parts: 1, scale: 1 };

    /// The rate as a float, for reports.
    pub fn as_f64(self) -> f64 {
        approx::ratio(self.parts, self.scale)
    }

    /// The rate as a fraction: its parts over its scale, a power of ten.
    pub fn fraction(self) -> (u64, u64) {
        (self.parts, self.scale)
    }

    /// `n` scaled by the rate, rounded down: a full-size cache budget as
    /// the sampled run holds it.
    pub fn of(self, n: u64) -> u64 {
        let scaled = u128::from(n) * u128::from(self.parts) / u128::from(self.scale);
        u64::try_from(scaled).expect("a rate of at most one scales n to at most n")
    }

    /// Whether `hash` falls under the rate's share of the `u64` hash space.
    fn admits(self, hash: u64) -> bool {
        u128::from(hash) * u128::from(self.scale) < u128::from(self.parts) << u64::BITS
    }
}

impl std::str::FromStr for SampleRate {
    type Err = WorkloadFault;

    /// A decimal in (0, 1] of at most `SAMPLE_RATE_DIGITS` digits after
    /// its point, such as `1`, `0.25`, or `.05`; anything else is
    /// `WorkloadFault::SampleRate`.
    fn from_str(text: &str) -> std::result::Result<Self, WorkloadFault> {
        let refused = || WorkloadFault::SampleRate {
            text: text.to_string(),
        };
        let (whole, fraction) = text.split_once('.').unwrap_or((text, ""));
        let digits = |part: &str| part.bytes().all(|byte| byte.is_ascii_digit());
        if (whole.is_empty() && fraction.is_empty())
            || !digits(whole)
            || !digits(fraction)
            || fraction.len() > SAMPLE_RATE_DIGITS
        {
            return Err(refused());
        }
        let scale = DECIMAL.pow(u32::try_from(fraction.len()).map_err(|_| refused())?);
        let number = |part: &str| -> std::result::Result<u64, WorkloadFault> {
            if part.is_empty() {
                Ok(0)
            } else {
                part.parse().map_err(|_| refused())
            }
        };
        let parts = number(whole)?
            .checked_mul(scale)
            .and_then(|whole| whole.checked_add(number(fraction).ok()?))
            .ok_or_else(refused)?;
        if parts == 0 || parts > scale {
            return Err(refused());
        }
        Ok(Self { parts, scale })
    }
}

impl fmt::Display for SampleRate {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_f64())
    }
}

/// Which keys of a trace a run keeps: those whose id's hash under `seed`
/// falls under `rate` of the hash space (the module doc).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct KeySample {
    rate: SampleRate,
    seed: u64,
}

impl KeySample {
    /// Every key.
    pub fn all() -> Self {
        Self {
            rate: SampleRate::ALL,
            seed: 0,
        }
    }

    /// The keys whose id's hash under `seed` falls under `rate` of the hash
    /// space.
    pub fn new(rate: SampleRate, seed: u64) -> Self {
        Self { rate, seed }
    }

    /// The share of the hash space kept.
    pub fn rate(self) -> SampleRate {
        self.rate
    }

    /// Whether the sample keeps key `id`.
    pub fn keeps(self, id: u64) -> bool {
        self.rate.admits(splitmix64(id ^ splitmix64(self.seed)))
    }
}

/// A trace as a run replays it: the requests a sample left keys in, each
/// with its kept keys in order, and what the file held before the sample.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Trace {
    /// The requests the sample left at least one key in, in the file's
    /// order, each holding every lookup of its kept keys.
    pub requests: Requests<u64>,
    /// Each of `requests`' `ts`, when the file has the column.
    pub ts: Option<Vec<i64>>,
    /// The requests in the file.
    pub read_requests: u64,
    /// The lookups in the file.
    pub read_lookups: u64,
}

impl Trace {
    /// The trace file at `path` (the module doc) under `sample`: each
    /// request's keys that `sample` keeps, in order, and a request left
    /// with none dropped. A file without `keys`, a column of another type,
    /// a null, or a negative id is a `WorkloadFault`.
    pub fn read(path: &Path, sample: KeySample) -> Result<Self> {
        let reader = ParquetRecordBatchReaderBuilder::try_new(File::open(path)?)?
            .with_batch_size(READ_BATCH_ROWS)
            .build()?;
        let mut trace = Self {
            requests: Requests::new(),
            ts: None,
            read_requests: 0,
            read_lookups: 0,
        };
        let mut kept = Vec::new();
        for batch in reader {
            let batch = batch?;
            let keys = batch
                .column_by_name(KEYS_COLUMN)
                .ok_or(WorkloadFault::NoKeys)?;
            let wrong = || WorkloadFault::ColumnType {
                column: TraceColumn::Keys,
                found: keys.data_type().clone(),
            };
            let ts = match batch.column_by_name(TS_COLUMN) {
                Some(column) if column.data_type() == &DataType::Int64 => {
                    Some(column.as_primitive::<Int64Type>())
                }
                Some(column) => {
                    return Err(WorkloadFault::ColumnType {
                        column: TraceColumn::Ts,
                        found: column.data_type().clone(),
                    }
                    .into());
                }
                None => None,
            };
            let item = |field: &arrow::datatypes::Field| field.data_type() == &DataType::Int64;
            if let DataType::List(field) = keys.data_type()
                && item(field)
            {
                trace.take(keys.as_list::<i32>(), ts, sample, &mut kept)?;
            } else if let DataType::LargeList(field) = keys.data_type()
                && item(field)
            {
                trace.take(keys.as_list::<i64>(), ts, sample, &mut kept)?;
            } else {
                return Err(wrong().into());
            }
        }
        Ok(trace)
    }

    /// Appends the requests of one batch, `lists` of key ids and `ts` of
    /// arrival times, under `sample` (`read`), `kept` the buffer a
    /// request's kept ids collect in.
    fn take<O: OffsetSizeTrait>(
        &mut self,
        lists: &GenericListArray<O>,
        ts: Option<&Int64Array>,
        sample: KeySample,
        kept: &mut Vec<u64>,
    ) -> Result<()> {
        let ids = lists.values().as_primitive::<Int64Type>();
        let offsets = lists.value_offsets();
        for row in 0..lists.len() {
            let request = self.read_requests;
            self.read_requests += 1;
            let null = |column| WorkloadFault::Null { request, column };
            if lists.is_null(row) {
                return Err(null(TraceColumn::Keys).into());
            }
            kept.clear();
            for at in offsets[row].as_usize()..offsets[row + 1].as_usize() {
                let id = present(ids, at).ok_or_else(|| null(TraceColumn::Keys))?;
                let id =
                    u64::try_from(id).map_err(|_| WorkloadFault::NegativeId { request, id })?;
                self.read_lookups += 1;
                if sample.keeps(id) {
                    kept.push(id);
                }
            }
            if kept.is_empty() {
                continue;
            }
            self.requests.push(kept);
            if let Some(ts) = ts {
                let at = present(ts, row).ok_or_else(|| null(TraceColumn::Ts))?;
                self.ts.get_or_insert_with(Vec::new).push(at);
            }
        }
        Ok(())
    }
}

/// `values[at]`, or `None` for a null.
fn present(values: &Int64Array, at: usize) -> Option<i64> {
    values.is_valid(at).then(|| values.value(at))
}

/// Where `keys` distinct keys lie among a table's `rows` ids: the first
/// `keys` ids of a shuffle of `0..rows` seeded by `seed`, so the place of
/// key `k` is the `k`-th, and keys adjacent by place, such as a trace's
/// hottest, lie scattered through the table's key order and its files.
/// More keys than rows is `WorkloadFault::TooManyKeys`.
pub fn scatter(keys: usize, rows: u64, seed: u64) -> Result<Vec<u64>> {
    if approx::count(keys) > rows {
        return Err(WorkloadFault::TooManyKeys {
            keys: approx::count(keys),
            rows,
        }
        .into());
    }
    let mut places: Vec<u64> = (0..rows).collect();
    places.shuffle(&mut StdRng::seed_from_u64(seed));
    places.truncate(keys);
    Ok(places)
}

/// Where `keys` distinct keys of a workload that stands for `part / whole`
/// of its full-size self lie among a table's `rows` ids: scattered over the
/// first `rows · part / whole` ids (`scatter`), so they fill as large a
/// share of those ids, and of their pages, as the full-size workload's
/// keys fill of all of them. More keys than that share of ids is
/// `WorkloadFault::TooManyKeys`, naming the share's ids as `rows`.
pub fn scatter_share(keys: usize, rows: u64, share: (u64, u64), seed: u64) -> Result<Vec<u64>> {
    scatter(keys, share_of_rows(rows, share), seed)
}

/// The first `rows · part / whole` of a table's `rows` ids, rounded down
/// and at most `rows`: where `scatter_share` places a workload's keys.
pub fn share_of_rows(rows: u64, (part, whole): (u64, u64)) -> u64 {
    let share = u128::from(rows) * u128::from(part) / u128::from(whole);
    u64::try_from(share).unwrap_or(rows).min(rows)
}

#[cfg(test)]
mod tests {
    use std::collections::{HashMap, HashSet};
    use std::path::PathBuf;
    use std::sync::Arc;

    use arrow::array::{ArrayRef, Int32Array, LargeListArray, ListArray, RecordBatch};
    use arrow::datatypes::Int64Type;
    use parquet::arrow::ArrowWriter;

    use super::*;
    use crate::error::TestkitError;

    /// The seed the tests' samples and placements use.
    const SEED: u64 = 7;

    fn rate(text: &str) -> SampleRate {
        text.parse().unwrap()
    }

    /// A trace file under `dir` of `requests`, with `ts` when given.
    fn write_trace(dir: &Path, requests: &[Vec<i64>], ts: Option<&[i64]>) -> PathBuf {
        let lists = ListArray::from_iter_primitive::<Int64Type, _, _>(
            requests
                .iter()
                .map(|ids| Some(ids.iter().copied().map(Some).collect::<Vec<_>>())),
        );
        let mut columns: Vec<(&str, ArrayRef)> = vec![(KEYS_COLUMN, Arc::new(lists))];
        if let Some(ts) = ts {
            columns.insert(0, (TS_COLUMN, Arc::new(Int64Array::from(ts.to_vec()))));
        }
        write_batch(dir, &RecordBatch::try_from_iter(columns).unwrap())
    }

    fn write_batch(dir: &Path, batch: &RecordBatch) -> PathBuf {
        write_groups(dir, batch, None)
    }

    /// `batch` as a trace file under `dir`, in row groups of at most
    /// `group_rows` rows when given.
    fn write_groups(dir: &Path, batch: &RecordBatch, group_rows: Option<usize>) -> PathBuf {
        let path = dir.join("trace.parquet");
        let properties = group_rows.map(|rows| {
            parquet::file::properties::WriterProperties::builder()
                .set_max_row_group_row_count(Some(rows))
                .build()
        });
        let mut writer =
            ArrowWriter::try_new(File::create(&path).unwrap(), batch.schema(), properties).unwrap();
        writer.write(batch).unwrap();
        writer.close().unwrap();
        path
    }

    fn fault<T: fmt::Debug>(result: Result<T>) -> WorkloadFault {
        match result {
            Err(TestkitError::Workload(fault)) => fault,
            Err(other) => panic!("expected a workload fault, got {other}"),
            Ok(value) => panic!("expected a workload fault, got {value:?}"),
        }
    }

    #[test]
    fn test_read_keeps_requests_in_order_with_repeats() {
        let dir = tempfile::tempdir().unwrap();
        let requests = vec![vec![3, 1, 3], vec![0], vec![2, 2]];
        let path = write_trace(dir.path(), &requests, Some(&[10, 20, 35]));
        let trace = Trace::read(&path, KeySample::all()).unwrap();
        let read: Vec<Vec<u64>> = trace.requests.iter().map(<[u64]>::to_vec).collect();
        assert_eq!(read, vec![vec![3, 1, 3], vec![0], vec![2, 2]]);
        assert_eq!(trace.ts, Some(vec![10, 20, 35]));
        assert_eq!((trace.read_requests, trace.read_lookups), (3, 6));
        assert_eq!(trace.requests.lookups(), 6);
    }

    /// More requests than one record batch, in two row groups: order, the
    /// arrival times of the kept requests, and a fault's request index all
    /// hold across the batches' boundaries.
    #[test]
    fn test_read_holds_across_record_batches_and_row_groups() {
        let dir = tempfile::tempdir().unwrap();
        let requests_in_trace = 3 * READ_BATCH_ROWS + 5;
        let requests: Vec<Vec<i64>> = (0..i64::try_from(requests_in_trace).unwrap())
            .map(|n| vec![n, n + 1])
            .collect();
        let ts: Vec<i64> = (0..i64::try_from(requests_in_trace).unwrap())
            .map(|n| n * 10)
            .collect();
        let lists = ListArray::from_iter_primitive::<Int64Type, _, _>(
            requests
                .iter()
                .map(|ids| Some(ids.iter().copied().map(Some).collect::<Vec<_>>())),
        );
        let batch = RecordBatch::try_from_iter([
            (
                TS_COLUMN,
                Arc::new(Int64Array::from(ts.clone())) as ArrayRef,
            ),
            (KEYS_COLUMN, Arc::new(lists) as ArrayRef),
        ])
        .unwrap();
        let path = write_groups(dir.path(), &batch, Some(requests_in_trace / 2));
        let sample = KeySample::new(rate("0.5"), SEED);
        let trace = Trace::read(&path, sample).unwrap();
        assert_eq!(trace.read_requests, approx::count(requests_in_trace));
        let expected: Vec<(Vec<u64>, i64)> = requests
            .iter()
            .zip(&ts)
            .filter_map(|(ids, &at)| {
                let kept: Vec<u64> = ids
                    .iter()
                    .map(|&id| u64::try_from(id).unwrap())
                    .filter(|&id| sample.keeps(id))
                    .collect();
                (!kept.is_empty()).then_some((kept, at))
            })
            .collect();
        let read: Vec<(Vec<u64>, i64)> = trace
            .requests
            .iter()
            .map(<[u64]>::to_vec)
            .zip(trace.ts.clone().unwrap())
            .collect();
        assert_eq!(read, expected);

        let mut bad = requests;
        let past = READ_BATCH_ROWS + 7;
        bad[past] = vec![-1];
        let path = write_trace(dir.path(), &bad, None);
        assert_eq!(
            fault(Trace::read(&path, KeySample::all())),
            WorkloadFault::NegativeId {
                request: approx::count(past),
                id: -1
            }
        );
    }

    #[test]
    fn test_read_takes_a_large_list_of_keys() {
        let dir = tempfile::tempdir().unwrap();
        let lists = LargeListArray::from_iter_primitive::<Int64Type, _, _>([
            Some(vec![Some(4), Some(2)]),
            Some(vec![Some(9)]),
        ]);
        let batch =
            RecordBatch::try_from_iter([(KEYS_COLUMN, Arc::new(lists) as ArrayRef)]).unwrap();
        let path = write_batch(dir.path(), &batch);
        let trace = Trace::read(&path, KeySample::all()).unwrap();
        let read: Vec<Vec<u64>> = trace.requests.iter().map(<[u64]>::to_vec).collect();
        assert_eq!(read, vec![vec![4, 2], vec![9]]);
    }

    #[test]
    fn test_scatter_share_places_keys_within_the_share_of_rows() {
        let rows = 10_000;
        let keys = 2_000;
        let quarter = (1, 4);
        let places = scatter_share(keys, rows, quarter, SEED).unwrap();
        assert_eq!(places.len(), keys);
        assert!(
            places.iter().all(|&place| place < rows / 4),
            "inside the first quarter"
        );
        assert_eq!(places.iter().collect::<HashSet<_>>().len(), keys);
        assert_eq!(
            fault(scatter_share(
                approx::length(rows / 4 + 1),
                rows,
                quarter,
                SEED
            )),
            WorkloadFault::TooManyKeys {
                keys: rows / 4 + 1,
                rows: rows / 4
            }
        );
        assert!(
            scatter_share(keys, rows, (1, 1), SEED)
                .unwrap()
                .iter()
                .any(|&place| place >= rows / 4)
        );
    }

    #[test]
    fn test_read_without_ts_reads_the_keys() {
        let dir = tempfile::tempdir().unwrap();
        let path = write_trace(dir.path(), &[vec![5, 6]], None);
        let trace = Trace::read(&path, KeySample::all()).unwrap();
        assert_eq!(trace.requests.get(0), &[5, 6]);
        assert_eq!(trace.ts, None);
    }

    #[test]
    fn test_read_keeps_every_occurrence_of_a_kept_key_and_drops_emptied_requests() {
        let dir = tempfile::tempdir().unwrap();
        let ids_in_trace: i64 = 200;
        let requests: Vec<Vec<i64>> = (0..ids_in_trace)
            .map(|n| vec![n, (n * 7) % ids_in_trace, n])
            .chain((0..ids_in_trace).map(|n| vec![n]))
            .collect();
        let ts: Vec<i64> = (0..i64::try_from(requests.len()).unwrap()).collect();
        let path = write_trace(dir.path(), &requests, Some(&ts));
        let sample = KeySample::new(rate("0.5"), SEED);
        let trace = Trace::read(&path, sample).unwrap();

        let mut whole: HashMap<u64, usize> = HashMap::new();
        for id in requests.iter().flatten() {
            *whole.entry(u64::try_from(*id).unwrap()).or_default() += 1;
        }
        let mut kept: HashMap<u64, usize> = HashMap::new();
        for id in trace.requests.iter().flatten() {
            *kept.entry(*id).or_default() += 1;
        }
        assert!(
            !kept.is_empty() && kept.len() < whole.len(),
            "a real sample"
        );
        for (id, lookups) in &whole {
            let expected = if sample.keeps(*id) { *lookups } else { 0 };
            assert_eq!(kept.get(id).copied().unwrap_or(0), expected, "id {id}");
        }
        let left = requests
            .iter()
            .filter(|ids| {
                ids.iter()
                    .any(|&id| sample.keeps(u64::try_from(id).unwrap()))
            })
            .count();
        assert_eq!(
            trace.requests.len(),
            left,
            "a request with no kept key is dropped"
        );
        assert_eq!(trace.ts.as_ref().map(Vec::len), Some(left));
        for (ids, kept) in requests
            .iter()
            .map(|ids| {
                ids.iter()
                    .map(|&id| u64::try_from(id).unwrap())
                    .filter(|&id| sample.keeps(id))
                    .collect::<Vec<_>>()
            })
            .filter(|ids| !ids.is_empty())
            .zip(trace.requests.iter())
        {
            assert_eq!(ids, kept, "a request keeps its kept keys in order");
        }
    }

    #[test]
    fn test_keeps_is_deterministic_for_a_seed() {
        let ids = 10_000;
        let sample = KeySample::new(rate("0.3"), SEED);
        let again = KeySample::new(rate("0.3"), SEED);
        let other = KeySample::new(rate("0.3"), SEED + 1);
        let kept =
            |sample: KeySample| -> Vec<u64> { (0..ids).filter(|&id| sample.keeps(id)).collect() };
        assert_eq!(kept(sample), kept(again));
        assert_ne!(kept(sample), kept(other), "another seed keeps other keys");
    }

    /// Over 100,000 ids a rate `r` keeps a binomial count whose share has
    /// a standard deviation of `sqrt(r (1 − r) / 100,000)`, 0.031 points at
    /// 0.01 and 0.16 points at 0.5, so six deviations bound the share.
    #[test]
    fn test_keeps_keeps_about_its_rate_of_distinct_keys() {
        let ids: u64 = 100_000;
        let deviations = 6.0;
        for text in ["0.01", "0.25", ".5"] {
            let sample = KeySample::new(rate(text), SEED);
            let kept = (0..ids).filter(|&id| sample.keeps(id)).count();
            let share = approx::ratio(approx::count(kept), ids);
            let wanted = sample.rate().as_f64();
            let deviation = (wanted * (1.0 - wanted) / approx::gauge(ids)).sqrt();
            assert!(
                (share - wanted).abs() < deviations * deviation,
                "rate {text} kept {share}"
            );
        }
        assert!((0..ids).all(|id| KeySample::all().keeps(id)));
        assert!((0..ids).all(|id| KeySample::new(rate("1.0"), SEED).keeps(id)));
    }

    #[test]
    fn test_sample_rate_from_str_reads_decimals_in_zero_to_one() {
        assert_eq!(rate("1").of(1_000), 1_000);
        assert_eq!(rate("0.25").of(1_000), 250);
        assert_eq!(rate(".05").of(1_000), 50);
        assert_eq!(rate("1.000").of(1_000), 1_000);
        assert_eq!(rate("0.333").of(1_000), 333);
        assert_eq!(rate("1.").of(7), 7);
        for text in [
            "0",
            "0.0",
            "1.5",
            "2",
            "-0.5",
            "",
            ".",
            "0.2.5",
            "a",
            "0.0000000000000000001",
        ] {
            assert_eq!(
                text.parse::<SampleRate>(),
                Err(WorkloadFault::SampleRate {
                    text: text.to_string()
                }),
                "{text}"
            );
        }
    }

    #[test]
    fn test_read_refuses_a_negative_id() {
        let dir = tempfile::tempdir().unwrap();
        let path = write_trace(dir.path(), &[vec![1], vec![2, -3]], None);
        assert_eq!(
            fault(Trace::read(&path, KeySample::all())),
            WorkloadFault::NegativeId { request: 1, id: -3 }
        );
    }

    #[test]
    fn test_read_refuses_a_null_id() {
        let dir = tempfile::tempdir().unwrap();
        let lists = ListArray::from_iter_primitive::<Int64Type, _, _>([Some(vec![Some(1), None])]);
        let batch =
            RecordBatch::try_from_iter([(KEYS_COLUMN, Arc::new(lists) as ArrayRef)]).unwrap();
        let path = write_batch(dir.path(), &batch);
        assert_eq!(
            fault(Trace::read(&path, KeySample::all())),
            WorkloadFault::Null {
                request: 0,
                column: TraceColumn::Keys
            }
        );
    }

    #[test]
    fn test_read_refuses_a_trace_without_keys_or_of_another_type() {
        let dir = tempfile::tempdir().unwrap();
        let ids = Arc::new(Int64Array::from(vec![1, 2])) as ArrayRef;
        let batch = RecordBatch::try_from_iter([("ids", Arc::clone(&ids))]).unwrap();
        let path = write_batch(dir.path(), &batch);
        assert_eq!(
            fault(Trace::read(&path, KeySample::all())),
            WorkloadFault::NoKeys
        );
        let batch = RecordBatch::try_from_iter([(KEYS_COLUMN, ids)]).unwrap();
        let path = write_batch(dir.path(), &batch);
        assert_eq!(
            fault(Trace::read(&path, KeySample::all())),
            WorkloadFault::ColumnType {
                column: TraceColumn::Keys,
                found: DataType::Int64
            }
        );
        let lists = ListArray::from_iter_primitive::<Int64Type, _, _>([Some(vec![Some(1)])]);
        let batch = RecordBatch::try_from_iter([
            (TS_COLUMN, Arc::new(Int32Array::from(vec![1])) as ArrayRef),
            (KEYS_COLUMN, Arc::new(lists) as ArrayRef),
        ])
        .unwrap();
        let path = write_batch(dir.path(), &batch);
        assert_eq!(
            fault(Trace::read(&path, KeySample::all())),
            WorkloadFault::ColumnType {
                column: TraceColumn::Ts,
                found: DataType::Int32
            }
        );
    }

    #[test]
    fn test_densified_numbers_distinct_ids_in_ascending_order() {
        let mut requests = Requests::new();
        requests.push(&[40, 7, 40]);
        requests.push(&[1_000_000]);
        requests.push(&[7]);
        let (dense, distinct) = requests.densified().unwrap();
        assert_eq!(distinct, vec![7, 40, 1_000_000]);
        let read: Vec<Vec<u32>> = dense.iter().map(<[u32]>::to_vec).collect();
        assert_eq!(read, vec![vec![1, 0, 1], vec![2], vec![0]]);
    }

    #[test]
    fn test_scatter_places_keys_on_distinct_rows_by_seed() {
        let rows = 1_000;
        let keys = 600;
        let places = scatter(keys, rows, SEED).unwrap();
        assert_eq!(places.len(), keys);
        assert_eq!(
            places.iter().collect::<HashSet<_>>().len(),
            keys,
            "one row per key"
        );
        assert!(places.iter().all(|&place| place < rows));
        assert_eq!(places, scatter(keys, rows, SEED).unwrap());
        assert_ne!(places, scatter(keys, rows, SEED + 1).unwrap());
        let in_order = places.windows(2).filter(|pair| pair[0] < pair[1]).count();
        assert!(
            in_order < keys * 3 / 4,
            "adjacent keys are not adjacent rows"
        );
        assert_eq!(
            scatter(usize::try_from(rows).unwrap(), rows, SEED)
                .unwrap()
                .len(),
            1_000
        );
    }

    #[test]
    fn test_scatter_refuses_more_keys_than_rows() {
        assert_eq!(
            fault(scatter(11, 10, SEED)),
            WorkloadFault::TooManyKeys { keys: 11, rows: 10 }
        );
    }
}
