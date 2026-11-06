//! The chunk stage of the index: the key column chunks, and an ordered
//! table's order column chunks beside them, fetched once, kept in a chunk
//! file, or dropped for their filters, and every key of the file decoded
//! and checked against the contract (RFD 007, RFD 017), with every order
//! value under the ordered rules (RFD 025, `ordered`), unless a chunk file
//! already holds what that check established.

use std::cmp::Ordering;
use std::fmt;
use std::ops::Range;
use std::panic::AssertUnwindSafe;
use std::sync::{Arc, Mutex, PoisonError};

use bytes::Bytes;
use parquet::bloom_filter::Sbbf;
use parquet::column::reader::ColumnReaderImpl;
use parquet::data_type::{
    ByteArray, ByteArrayType, DataType as ParquetDataType, FixedLenByteArray,
    FixedLenByteArrayType, Int32Type, Int64Type,
};
use parquet::file::metadata::ParquetMetaData;
use tokio::task::JoinError;

use super::inspect::{bloom_ranges, column_chunk_ranges, rules_of};
use super::ordered::{OrderVerified, PairCheck};
use super::page_header::{check_key_page_headers, check_page_headers, check_value_pages};
use super::pages::{ChunkPages, PageTally};
use super::plain::{self, ByteKeys};
use super::record::{self, RecordFault};
use super::{
    Column, ColumnChunk, DecodedPart, FileFault, FileIndex, GroupRange, Inspected, KeyResidency,
    KeyShape, Keys, OrderLeaf, PageStarts, RunShape, ValueShape, file_fault, group_starts,
    identity_of, row_index, unless_panicked, value_bytes,
};
use crate::approx;
use crate::blocking::on_blocking_pool;
use crate::config::{ConfigError, ConfigViolation, Residency};
use crate::contract::{MAX_KEY_BYTES, PAGE_ROWS};
use crate::error::{Error, RefusalReason, Result};
use crate::key::{Key, KeyType, UUID_BYTES};
use crate::local::{
    ChunkFileLookup, ChunkRules, DiskChunks, LocalDir, LocalFault, Rejection, Reservation,
    VerifiedUnder, chunk_file_bytes,
};
use crate::parquet_io::{FileHandle, Segments};

impl Inspected {
    /// The chunk stage of `FileIndex::build`: streams the key chunks, and
    /// an ordered table's order chunks in the same `read_ranges` call, once
    /// for verification and keeps what `residency` keeps, as `build`
    /// documents: the Full tier the key chunks alone, the Disk tier both in
    /// the chunk file named by the rules they were verified under. The Disk
    /// tier with no local directory is the one rule the loader re-checks,
    /// since a configuration built in code may skip `Config::validated`; it
    /// is raised here as the same `ConfigViolation`, before any chunk is
    /// fetched. A Disk-tier chunk file write holds `reservation`, the
    /// load's hold on `local`'s budget, until the write ends.
    pub(crate) async fn build(
        self,
        residency: Residency,
        local: Option<&Arc<LocalDir>>,
        reservation: Option<&Arc<Reservation>>,
    ) -> Result<FileIndex> {
        let to_verify = KeysToVerify::of(&self);
        let Self {
            handle,
            metadata,
            layout,
            reuse,
            predicted,
            ..
        } = self;
        let ranges = layout.verified_ranges(&metadata, handle.size());
        let (keys, verified) = match residency {
            Residency::Full => {
                let (chunks, verified) = fetch_verified(&to_verify, &ranges).await?;
                let key_ranges = column_chunk_ranges(&metadata, layout.key_leaf, handle.size());
                (
                    KeyResidency::Chunks(key_chunks_of(&chunks, &key_ranges, handle.size())),
                    verified,
                )
            }
            Residency::Disk => {
                let local = local.ok_or_else(|| {
                    Error::Config(ConfigError::Invalid(
                        ConfigViolation::DiskResidencyWithoutLocalDir {
                            table: to_verify.table.clone(),
                            index: None,
                        },
                    ))
                })?;
                let target = ChunkFileTarget {
                    local,
                    reservation,
                    estimated: predicted.disk_bytes,
                };
                let (disk, verified) = disk_chunks(&to_verify, &target, &ranges, reuse).await?;
                (KeyResidency::Disk(disk), verified)
            }
            Residency::Bloom => {
                let filter_ranges =
                    bloom_ranges(&to_verify.table, &handle, &metadata, layout.key_leaf)?;
                let (chunks, verified) = fetch_verified(&to_verify, &ranges).await?;
                drop(chunks);
                let filters = read_bloom_filters(&handle, &filter_ranges).await?;
                (KeyResidency::Filters(filters), verified)
            }
        };
        let rows = metadata.file_metadata().num_rows();
        let group_starts = group_starts(&metadata);
        Ok(FileIndex {
            handle,
            metadata,
            layout,
            keys,
            fences: verified.fences,
            groups: verified.groups,
            group_starts,
            key_range: verified.key_range,
            max_key_bytes: verified.max_key_bytes,
            order: verified.order,
            rows,
        })
    }
}

/// The parts of `chunks` that are the key column chunks `key_ranges`: what
/// the Full tier keeps of the chunks an ordered table verified, the order
/// chunks dropped (RFD 025, Load). No byte is copied.
fn key_chunks_of(chunks: &Segments, key_ranges: &[Range<u64>], file_size: u64) -> Arc<Segments> {
    let parts = chunks
        .parts()
        .iter()
        .filter(|(range, _)| key_ranges.contains(range))
        .cloned()
        .collect();
    Arc::new(Segments::new(parts, file_size))
}

impl Inspected {
    /// The file's first and last key read from its edge pages alone (RFD
    /// 020): the first key page of the first row group that has rows and
    /// the last key page of the last, with the key column chunk's
    /// dictionary page of each when it has one, in one `read_ranges` call,
    /// one load GET per range. Of a sorted file these are its exact first
    /// and last key, and `build` verifies afterwards that the file is
    /// sorted, so overlap may be judged from them before any key chunk is
    /// fetched. The pages are decoded on the blocking pool, as `verify`
    /// is; a canceled task is `Error::VerifyCanceled` for the file. `None`
    /// when an edge page does not decode to a key: its header is over its
    /// bounds, it decodes to a fault, or it holds no key, all of which
    /// `build`'s verification refuses with its own reason.
    pub(crate) async fn edge_keys(&self) -> Result<Option<(Key, Key)>> {
        let with_rows = "inspect checked that the row groups sum to the file's rows, which are above zero, so a row group holds rows";
        let groups = self.metadata.row_groups();
        let head = groups
            .iter()
            .position(|group| group.num_rows() > 0)
            .expect(with_rows);
        let tail = groups
            .iter()
            .rposition(|group| group.num_rows() > 0)
            .expect(with_rows);
        let column = Column {
            metadata: &self.metadata,
            leaf: self.layout.key_leaf,
            path: &self.handle.location,
        };
        let (head_chunk, tail_chunk) = (column.group(head), column.group(tail));
        let last_page = tail_chunk.page_locations().len() - 1;
        let mut ranges = vec![head_chunk.page_range(0)];
        if (tail, last_page) != (head, 0) {
            ranges.push(tail_chunk.page_range(last_page));
        }
        ranges.extend(head_chunk.dictionary_range());
        if tail != head {
            ranges.extend(tail_chunk.dictionary_range());
        }
        let parts = fetch_chunk_parts(&self.handle, &ranges).await?;
        let segments = Arc::new(Segments::new(parts, self.handle.size()));
        let shape = self.layout.key_shape;
        let edges = on_column(
            &self.handle,
            &self.metadata,
            self.layout.key_leaf,
            move |column| {
                let first = edge_key(column.group(head), shape, 0, &segments, Keys::first)?;
                let last = edge_key(column.group(tail), shape, last_page, &segments, Keys::last)?;
                Ok(first.zip(last))
            },
        )
        .await
        .map_err(|canceled| Error::verify_canceled(&self.handle.location, canceled))?;
        Ok(edges.unwrap_or_else(|fault: Error| {
            // intentional: verification decodes these pages again at build
            // and refuses the file with the reason it finds there.
            tracing::debug!(path = %self.handle.location, %fault, "edge key pages do not decode");
            None
        }))
    }

    /// The file's exact first and last key from the Disk-tier chunk file in
    /// `local` that `build` would trust for it (`reused`), with no GET:
    /// the header and the payload checksum are local reads. What was found
    /// is kept for `build`, which uses a trusted chunk file as it is and
    /// rewrites a stale one without checking it a second time. `None` when
    /// no chunk file there is trusted.
    pub(crate) async fn trusted_key_range(
        &mut self,
        local: &LocalDir,
    ) -> Result<Option<(Key, Key)>> {
        let to_verify = KeysToVerify::of(self);
        let ranges = self
            .layout
            .verified_ranges(&self.metadata, self.handle.size());
        let found = reused(&to_verify, local, &ranges).await?;
        let range = found
            .as_ref()
            .ok()
            .map(|(_, verified)| verified.key_range.clone());
        self.reuse = Some(found);
        Ok(range)
    }
}

/// The key `pick` takes from key page `page` of `chunk`, decoded from
/// `segments` after its header and the chunk's dictionary page header are
/// checked as `verify` checks them; `None` for a page that holds no key.
fn edge_key(
    chunk: ColumnChunk<'_>,
    shape: KeyShape,
    page: usize,
    segments: &Arc<Segments>,
    pick: fn(&Keys) -> Option<Key>,
) -> Result<Option<Key>> {
    check_key_page_headers(chunk, shape, page, segments)?;
    let keys = decode_run(
        chunk,
        shape.run_shape(),
        chunk.page_rows(page),
        Arc::clone(segments),
    )?;
    check_utf8(chunk, &keys)?;
    Ok(pick(&keys))
}

/// The key column of one file being built, and an ordered table's order
/// column, owned, so that their verification can move to the blocking
/// pool.
struct KeysToVerify {
    table: String,
    handle: Arc<FileHandle>,
    metadata: Arc<ParquetMetaData>,
    leaf: usize,
    shape: KeyShape,
    order: Option<OrderLeaf>,
}

impl KeysToVerify {
    /// The key column of `inspected`, with its order column.
    fn of(inspected: &Inspected) -> Self {
        Self {
            table: inspected.table.clone(),
            handle: Arc::clone(&inspected.handle),
            metadata: Arc::clone(&inspected.metadata),
            leaf: inspected.layout.key_leaf,
            shape: inspected.layout.key_shape,
            order: inspected.layout.order,
        }
    }

    /// The context the file's chunks are verified under, which names its
    /// chunk file (`verified_under`, `inspect::rules_of`).
    fn under(&self) -> VerifiedUnder {
        verified_under(self.shape.key_type(), rules_of(self.order))
    }

    /// `verify` of this column over `chunks`, run on the blocking pool
    /// (`on_blocking_pool`): decoding every key of a file is CPU-bound
    /// work that grows with the file, and inline it would hold the async
    /// worker that also answers lookups. The builds a load has in flight
    /// each wait on at most one verification, so a load runs at most
    /// `generation::load_concurrency()` of them at once, each on its own blocking-pool
    /// thread. A canceled task is `Error::VerifyCanceled` for the file.
    async fn verify(&self, chunks: &Arc<Segments>) -> Result<Verified> {
        let table = self.table.clone();
        let (shape, order) = (self.shape, self.order);
        let chunks = Arc::clone(chunks);
        on_column(&self.handle, &self.metadata, self.leaf, move |column| {
            verify(&table, column, shape, Rules::of(order), &chunks)
        })
        .await
        .map_err(|canceled| Error::verify_canceled(&self.handle.location, canceled))?
    }
}

/// `work` over column `leaf` of the file `handle` opens, whose footer is
/// `metadata`, on the blocking pool (`on_blocking_pool`), the column built
/// there from clones of the two. A canceled task is the `JoinError`, which
/// each caller maps to its own error.
async fn on_column<T: Send + 'static>(
    handle: &Arc<FileHandle>,
    metadata: &Arc<ParquetMetaData>,
    leaf: usize,
    work: impl FnOnce(&Column<'_>) -> T + Send + 'static,
) -> std::result::Result<T, JoinError> {
    let (handle, metadata) = (Arc::clone(handle), Arc::clone(metadata));
    on_blocking_pool(move || {
        work(&Column {
            metadata: &metadata,
            leaf,
            path: &handle.location,
        })
    })
    .await
}

/// The column chunks of `ranges`, the key chunks and an ordered table's
/// order chunks (`Layout::verified_ranges`), fetched and verified: the
/// chunks a tier keeps (Full, the key chunks), writes (Disk), or drops
/// (Bloom), and what verification recorded.
async fn fetch_verified(
    to_verify: &KeysToVerify,
    ranges: &[Range<u64>],
) -> Result<(Arc<Segments>, Verified)> {
    let handle = &to_verify.handle;
    let parts = fetch_chunk_parts(handle, ranges).await?;
    let chunks = Arc::new(Segments::new(parts, handle.size()));
    let verified = to_verify.verify(&chunks).await?;
    Ok((chunks, verified))
}

/// Every column chunk of `ranges` in one `read_ranges` call: one load GET
/// per row group per column. Each chunk is copied out of the buffer the
/// store returned: a store may serve neighboring ranges from one fetch and
/// hand back slices of it, and a slice keeps the whole fetch alive, so the
/// bytes held would exceed the chunks by whatever lay between them. The
/// copy is paid once at load.
pub(super) async fn fetch_chunk_parts(
    handle: &FileHandle,
    ranges: &[Range<u64>],
) -> Result<Vec<(Range<u64>, Bytes)>> {
    let chunks = handle.read_ranges(ranges).await?;
    Ok(ranges
        .iter()
        .cloned()
        .zip(chunks)
        .map(|(range, bytes)| (range, Bytes::copy_from_slice(&bytes)))
        .collect())
}

/// Where a Disk-tier build writes its chunk file: the local directory, the
/// load's hold on its budget, and the chunk file's length the footer
/// predicted, which the hold covers.
struct ChunkFileTarget<'a> {
    local: &'a Arc<LocalDir>,
    reservation: Option<&'a Arc<Reservation>>,
    estimated: u64,
}

/// The disk tier's key chunks. A chunk file in `local` that is reusable
/// (`reusable_disk_chunks`) and whose payload checksum matches is trusted
/// with the verification record it stores: no GET, no chunk read back, no
/// key decoded (`trusted`). Any other file, or none, is fetched, verified,
/// and written, replacing whatever is at that name; a file that was there
/// is logged at info with why it was not reused. Nothing about a stale file
/// fails the load. `found` is what `reused` already found for the file, at
/// the level gate's edge stage, so neither a trusted nor a rejected chunk
/// file is checked twice; `None` looks now. The write runs on the blocking
/// pool (`on_blocking_pool`) and holds `reservation` until it ends, so
/// bytes it lands after its caller stopped waiting stay reserved until
/// they are on disk; once they are, the `estimated` bytes the load reserved
/// for the file are released, since `index_bytes_in_use` counts them. A
/// file written over a chunk file already there releases none: the
/// load may have reserved nothing for it. An ordered file's chunk file, whose order fences no
/// footer number bounds, is charged its exact length before it is written:
/// beyond the `estimated` length the load reserved, the rest is reserved
/// too (`LocalDir::reserve`) and held with the load's hold, or the build is
/// `RefusalReason::OverBudget` (RFD 025, Load). A canceled write or
/// checksum pass is `Error::Local` on the chunk file's path with
/// `LocalFault::Join`.
async fn disk_chunks(
    to_verify: &KeysToVerify,
    target: &ChunkFileTarget<'_>,
    ranges: &[Range<u64>],
    found: Option<Reuse>,
) -> Result<(DiskChunks, Verified)> {
    let (local, reservation) = (target.local, target.reservation);
    let handle = &to_verify.handle;
    let identity = identity_of(handle);
    let under = to_verify.under();
    let path = local.chunk_path(&identity, under.rules);
    let found = match found {
        Some(found) => found,
        None => reused(to_verify, local, ranges).await?,
    };
    match found {
        Ok(reused) => return Ok(reused),
        Err(None) => {}
        Err(Some(stale)) => tracing::info!(
            path = %path.display(),
            data_file = %handle.location,
            reason = %stale,
            detail = ?stale,
            "chunk file is stale; fetching its chunks to rewrite it"
        ),
    }
    let (chunks, verified) = fetch_verified(to_verify, ranges).await?;
    let verification = record::encode(&verified);
    let exact = chunk_file_bytes(&identity, under, ranges, approx::count(verification.len()));
    let rest = match to_verify.order {
        Some(_) if exact > target.estimated => {
            Some(local.reserve(&to_verify.table, exact - target.estimated)?)
        }
        Some(_) | None => None,
    };
    let (local, reservation, written_path) =
        (Arc::clone(local), reservation.cloned(), path.clone());
    let estimated = target.estimated;
    let disk = on_blocking_pool(move || {
        let was_absent = !path.exists();
        let written = local.write_chunks(
            &identity,
            under,
            &verification,
            chunks.parts().iter().cloned(),
        );
        if let (Some(reservation), true) = (&reservation, was_absent) {
            reservation.release(estimated);
        }
        drop((reservation, rest));
        written
    })
    .await
    .map_err(|canceled| Error::Local {
        path: written_path,
        fault: LocalFault::Join(canceled),
    })??;
    Ok((disk, verified))
}

/// What `reused` finds in `local` for a data file: the chunk file and the
/// verification record it stores when a build may trust them, otherwise
/// why not, `None` for no file.
pub(super) type Reuse = std::result::Result<(DiskChunks, Verified), Option<Stale>>;

/// The chunk file in `local` for `to_verify`'s data file, whose verified
/// chunks are `ranges`, under the rules it verifies them by, with what it
/// stores of their verification, when a build may trust it: it is reusable
/// by its header (`reusable_disk_chunks`) and by its payload (`trusted`).
/// Otherwise why not, `None` for no file.
async fn reused(
    to_verify: &KeysToVerify,
    local: &LocalDir,
    ranges: &[Range<u64>],
) -> Result<Reuse> {
    let handle = &to_verify.handle;
    let under = to_verify.under();
    Ok(match reusable_disk_chunks(handle, local, ranges, under)? {
        OnDisk::Reusable(disk) => {
            let path = local.chunk_path(&identity_of(handle), under.rules);
            trusted(to_verify, disk, &path).await?.map_err(Some)
        }
        OnDisk::Stale(stale) => Err(Some(stale)),
        OnDisk::Absent => Err(None),
    })
}

/// `disk` with the verification record it stores, once the payload
/// checksum matches (`DiskChunks::checked_verification`, bounded by
/// `record::most_bytes`) and the record describes the file the footer does
/// (`record::decode`); otherwise why not. Runs on the blocking pool: the
/// checksum pass reads the whole payload, a sequential local read far
/// cheaper than decoding it.
async fn trusted(
    to_verify: &KeysToVerify,
    disk: DiskChunks,
    path: &std::path::Path,
) -> Result<std::result::Result<(DiskChunks, Verified), Stale>> {
    let (shape, order) = (to_verify.shape, to_verify.order);
    on_column(
        &to_verify.handle,
        &to_verify.metadata,
        to_verify.leaf,
        move |column| {
            let verification = match disk.checked_verification(record::most_bytes(column, order))? {
                Ok(verification) => verification,
                Err(rejection) => return Ok(Err(Stale::Rejected(rejection))),
            };
            Ok(record::decode(&verification, column, shape, order)
                .map(|verified| (disk, verified))
                .map_err(Stale::Record))
        },
    )
    .await
    .map_err(|canceled| Error::Local {
        path: path.to_path_buf(),
        fault: LocalFault::Join(canceled),
    })?
}

/// What a Disk-tier build finds in `local` for a data file
/// (`reusable_disk_chunks`).
#[derive(Debug)]
pub(super) enum OnDisk {
    /// No chunk file.
    Absent,
    /// A chunk file the build does not reuse, and why.
    Stale(Stale),
    /// A chunk file whose header says it is the one the build would write;
    /// its payload is still to be checked (`trusted`).
    Reusable(DiskChunks),
}

/// Why a Disk-tier build does not reuse the chunk file under its data
/// file's name and fetches, verifies, and rewrites it instead (RFD 017).
/// Not an error: the load goes on as for a missing file. Display gives the
/// `snake_case` name the info log carries.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Stale {
    /// The file failed a check of its envelope, header, identity, or
    /// payload checksum.
    Rejected(Rejection),
    /// The file's keys were verified as another key type.
    KeyType {
        /// The key type the header carries.
        stored: KeyType,
    },
    /// The file's keys were verified under other rules.
    VerifierVersion {
        /// The verifier version the header carries.
        stored: u32,
    },
    /// The file does not store exactly the column chunks the footer names
    /// for the rules: the key chunks, with the order chunks beside them
    /// under the ordered rules.
    Chunks,
    /// The verification record does not describe the file the footer does.
    Record(RecordFault),
}

impl fmt::Display for Stale {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Rejected(rejection) => rejection.fmt(f),
            Self::KeyType { .. } => f.write_str("key_type_mismatch"),
            Self::VerifierVersion { .. } => f.write_str("verifier_version_mismatch"),
            Self::Chunks => f.write_str("chunks_mismatch"),
            Self::Record(fault) => write!(f, "record_{fault}"),
        }
    }
}

/// The chunk file in `local` a Disk-tier build of `handle`'s file may
/// reuse, from its envelope and header alone: one whose identity (path,
/// size, footer digest) matches, whose keys were verified `under` the
/// build's key type and verifier version, and which stores exactly
/// `ranges`, the key column chunks and under the ordered rules the order
/// column chunks. `Stale` with the first check a file there fails, `Absent`
/// when there is none.
pub(super) fn reusable_disk_chunks(
    handle: &FileHandle,
    local: &LocalDir,
    ranges: &[Range<u64>],
    under: VerifiedUnder,
) -> Result<OnDisk> {
    let disk = match local.lookup(&identity_of(handle), under.rules)? {
        ChunkFileLookup::Absent => return Ok(OnDisk::Absent),
        ChunkFileLookup::Rejected(rejection) => {
            return Ok(OnDisk::Stale(Stale::Rejected(rejection)));
        }
        ChunkFileLookup::Found(disk) => disk,
    };
    let stored = disk.verified_under();
    Ok(if stored.key_type != under.key_type {
        OnDisk::Stale(Stale::KeyType {
            stored: stored.key_type,
        })
    } else if stored.verifier_version != under.verifier_version {
        OnDisk::Stale(Stale::VerifierVersion {
            stored: stored.verifier_version,
        })
    } else if !disk.stores_exactly(ranges) {
        OnDisk::Stale(Stale::Chunks)
    } else {
        OnDisk::Reusable(disk)
    })
}

/// The key column's bloom filter of every row group, one `read_ranges` call
/// (one load GET per row group), each parsed from exactly the bytes the
/// footer bounds: a header, then a bitset of exactly the header's length,
/// holding at least one block. A filter that does not decode so, or whose
/// bitset holds no block (fewer than `parquet::bloom_filter::BITSET_MIN_LENGTH`
/// bytes; a check indexes the first block), is `FileFault::CorruptBloomFilter`,
/// and one parquet panics parsing is `FileFault::DecoderPanicked`.
async fn read_bloom_filters(handle: &FileHandle, ranges: &[Range<u64>]) -> Result<Vec<Sbbf>> {
    let bytes = handle.read_ranges(ranges).await?;
    bytes
        .iter()
        .enumerate()
        .map(|(row_group, bytes)| {
            let corrupt = || {
                file_fault(
                    &handle.location,
                    FileFault::CorruptBloomFilter { row_group },
                )
            };
            // Unwind-safe: `from_bytes` reads the bytes and mutates nothing
            // it does not return.
            let parsed = unless_panicked(
                &handle.location,
                || Sbbf::from_bytes(bytes),
                || DecodedPart::BloomFilter { row_group },
            )?;
            let filter = parsed.map_err(|error| {
                tracing::debug!(%error, path = %handle.location, row_group, "bloom filter does not decode");
                corrupt()
            })?;
            if filter.num_blocks() == 0 {
                return Err(corrupt());
            }
            Ok(filter)
        })
        .collect()
}

/// The version of the rules `verify` enforces, which a Disk-tier chunk
/// file's verification record was computed under (RFD 017). Raise it with
/// any change to what `verify` refuses or records: a check added, dropped,
/// or loosened, `contract::MAX_KEY_BYTES`, how a fence is chosen. A record
/// of any other version is stale and the file is verified again from
/// object storage, so a result computed under other rules is never trusted.
pub(super) const VERIFIER_VERSION: u32 = 4;

/// The verification context of this build for keys of `key_type` under
/// `rules`.
pub(super) fn verified_under(key_type: KeyType, rules: ChunkRules) -> VerifiedUnder {
    VerifiedUnder {
        key_type,
        verifier_version: VERIFIER_VERSION,
        rules,
    }
}

/// What verifying the key column establishes, with an ordered table's
/// order facts and order fences, and what a Disk-tier chunk file stores of
/// it (`record`).
#[derive(Clone, Debug, PartialEq)]
pub(super) struct Verified {
    pub(super) fences: Vec<Vec<Key>>,
    pub(super) groups: Vec<GroupRange>,
    pub(super) key_range: (Key, Key),
    pub(super) max_key_bytes: usize,
    /// What the ordered rules recorded; `None` under the key-value rules.
    pub(super) order: Option<OrderVerified>,
}

/// The rules `verify` checks a file's rows under.
#[derive(Clone, Copy)]
enum Rules {
    /// The key-value rules (RFD 007): every key above the one before it
    /// within a row group and across row groups, so a key equal to the one
    /// before it is a duplicate and a key below it is unsorted. A string or
    /// binary key chunk of `PLAIN` pages without a dictionary is read in
    /// place where the decoder is `KeyDecoder::Plain` (`plain::checked_run`),
    /// and any other chunk, or one of those holding a page it does not read
    /// exactly, by parquet's column reader (`check_run`); either way the
    /// keys and every refusal and fault are the column reader's. The
    /// decoder is chosen here so that tests can hold the plain decoder to
    /// the column reader's answers.
    KeyValue(KeyDecoder),
    /// The ordered rules (RFD 025, Load) of this order column, decoded
    /// from the chunks beside the keys a window at a time (`PairCheck`): a
    /// key may repeat, and every row's key and order value must be above
    /// the row's before it, within a row group and across row groups,
    /// deleted rows included, so an equal pair is
    /// `RefusalReason::DuplicateRows` naming the key and the order value
    /// and a falling one `RefusalReason::Unsorted`; a null order value is
    /// `RefusalReason::NullOrder`, and a `string` order value that is not
    /// UTF-8 `FileFault::OrderNotUtf8`. Every order page header is checked
    /// as a value page's is before its row group decodes. Within a row
    /// group, a decode fault stands before a null key, a null key before a
    /// null order value, that before a key and then an order value that is
    /// not UTF-8, and those before a pair out of order. Records the order
    /// facts and the order fences besides. The keys are read by parquet's
    /// column reader alone.
    Ordered(OrderLeaf),
}

impl Rules {
    /// The rules a file with order column `order` is verified under: the
    /// ordered rules with one, and without one the key-value rules, keys
    /// read in place where the plain decoder reads them.
    fn of(order: Option<OrderLeaf>) -> Self {
        order.map_or(Self::KeyValue(KeyDecoder::Plain), Self::Ordered)
    }
}

/// Decodes every key of every row group from `chunks` and checks what the
/// contract requires of them under `rules`: no null, UTF-8 in a string
/// column, the order `rules` give, none over `contract::MAX_KEY_BYTES`.
/// Every key page header of a row group is checked against what its page
/// can hold before the row group is decoded (`check_page_headers`), and
/// every key page against the rows its offset index gives it on the way.
/// Records the exact first and last key of the file and of every row
/// group, the longest key, and the fences. Nothing here consults column
/// statistics. A row group's keys are checked a decode batch at a time
/// (`RunCheck`), so at most one batch of them is held however long the row
/// group is.
fn verify(
    table: &str,
    column: &Column<'_>,
    shape: KeyShape,
    rules: Rules,
    chunks: &Arc<Segments>,
) -> Result<Verified> {
    let path = column.path;
    let total = column.metadata.num_row_groups();
    let mut fences = Vec::with_capacity(total);
    let mut groups: Vec<GroupRange> = Vec::with_capacity(total);
    let mut longest = 0;
    let (decoder, mut pairs) = match rules {
        Rules::KeyValue(decoder) => (decoder, None),
        Rules::Ordered(order) => (
            KeyDecoder::ColumnReader,
            Some(PairCheck::new(column, order)),
        ),
    };
    for row_group in 0..total {
        let chunk = column.group(row_group);
        let rows = chunk.rows();
        check_page_headers(chunk, shape, chunks)?;
        let (run, outcome) = match &mut pairs {
            None => (check_run(chunk, shape, chunks, decoder)?.0, None),
            Some(pairs) => {
                let (run, outcome) = pairs.check_group(chunk, shape, chunks)?;
                (run, Some(outcome))
            }
        };
        if run.values < row_index(rows) {
            let reason = RefusalReason::NullKeys {
                path: path.to_string(),
                row_group,
            };
            return Err(Error::refused(table, reason));
        }
        if let Some(outcome) = &outcome {
            outcome.check_nulls(table, chunk)?;
        }
        if run.not_utf8 {
            return Err(not_utf8(chunk));
        }
        if let Some(outcome) = outcome {
            outcome.check_values(table, chunk)?;
        }
        if let Some((order, key)) = run.disorder {
            return Err(Error::refused(table, not_above_previous(chunk, order, key)));
        }
        longest = longest.max(run.longest);
        let Some((first, last)) = run.first.zip(run.last) else {
            fences.push(Vec::new());
            continue;
        };
        if pairs.is_none()
            && let Some(previous) = groups.last()
        {
            let order = first.cmp(&previous.last);
            if order != Ordering::Greater {
                return Err(Error::refused(
                    table,
                    not_above_previous(chunk, order, first),
                ));
            }
        }
        fences.push(run.fences);
        groups.push(GroupRange {
            row_group,
            first,
            last,
        });
    }
    if longest > MAX_KEY_BYTES {
        let reason = RefusalReason::KeyTooLong {
            path: path.to_string(),
            bytes: longest,
        };
        return Err(Error::refused(table, reason));
    }
    let has_keys = "inspect checked that the row groups sum to the file's rows, which are above zero, so a row group holds keys";
    let head = groups.first().expect(has_keys);
    let tail = groups.last().expect(has_keys);
    Ok(Verified {
        key_range: (head.first.clone(), tail.last.clone()),
        fences,
        groups,
        max_key_bytes: longest,
        order: pairs.map(PairCheck::verified),
    })
}

/// One decode batch of a row group's keys, in file order, as `RunCheck`
/// reads it: values parquet's column reader decoded (`Keys`), or values
/// sliced from a plain page in place (`plain::checked_run`).
pub(super) trait KeyBatch {
    /// The number of keys.
    fn len(&self) -> usize;

    /// Whether the batch holds no key.
    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Whether every key is UTF-8, in a column of strings; true in any
    /// other.
    fn is_utf8(&self) -> bool;

    /// The length in bytes of the longest key; zero for an empty batch.
    fn max_len(&self) -> usize;

    /// The key at `at`, which is below `len`. A string key is built from
    /// its bytes as UTF-8, so a batch of strings is read back only once
    /// `is_utf8` holds for it.
    fn key(&self, at: usize) -> Key;

    /// The position of the first key not above the one before it, with how
    /// it compares to that key: `Equal` for a duplicate, `Less` for a key
    /// out of order. `None` for a strictly increasing batch.
    fn first_not_increasing(&self) -> Option<(usize, Ordering)>;
}

impl KeyBatch for Keys {
    fn len(&self) -> usize {
        Keys::len(self)
    }

    fn is_utf8(&self) -> bool {
        keys_are_utf8(self)
    }

    fn max_len(&self) -> usize {
        Keys::max_len(self)
    }

    fn key(&self, at: usize) -> Key {
        self.get(at)
    }

    fn first_not_increasing(&self) -> Option<(usize, Ordering)> {
        Keys::first_not_increasing(self)
    }
}

/// What `verify` learns of one row group's keys, batch by batch as they are
/// decoded, so that no row group's keys are held at once: the keys
/// decoded, the longest, the first and last, the key at each page's first
/// row (the fences), whether every string key is UTF-8, and under the
/// key-value rules the first key not above the one before it; under the
/// ordered rules a key repeats and `PairCheck` orders the rows by key and
/// order value instead. Nothing is decided here; `verify` applies
/// what was found once the whole row group has decoded, in the order it
/// checks a row group in, so a decode failure anywhere in it stands before
/// a null, a null before a key that is not UTF-8, and that before a key
/// out of order. Once a batch holds a string that is not UTF-8, later
/// batches are only counted, since `KeyBatch::key` builds a string key
/// from UTF-8 alone. What is found does not depend on where the batches
/// begin and end, so the two decoders, whose batches differ, find the same.
pub(super) struct RunCheck<'a> {
    values: usize,
    longest: usize,
    first: Option<Key>,
    last: Option<Key>,
    fences: Vec<Key>,
    /// The pages whose first key is a fence.
    fence_pages: PageStarts<'a>,
    not_utf8: bool,
    /// Whether the keys must strictly increase, as the key-value rules
    /// require; under the ordered rules a key repeats, and `PairCheck`
    /// orders the rows by key and order value instead.
    strict: bool,
    /// The first key not above the one before it, and how it compares.
    disorder: Option<(Ordering, Key)>,
}

impl<'a> RunCheck<'a> {
    /// A check of `chunk`'s keys under the key-value rules.
    pub(super) fn new(chunk: ColumnChunk<'a>) -> Self {
        Self {
            values: 0,
            longest: 0,
            first: None,
            last: None,
            fences: Vec::with_capacity(chunk.page_locations().len()),
            fence_pages: PageStarts::of(chunk),
            not_utf8: false,
            strict: true,
            disorder: None,
        }
    }

    /// A check of `chunk`'s keys under the ordered rules, which leave
    /// their order to `PairCheck`: no disorder is recorded.
    pub(super) fn ordered(chunk: ColumnChunk<'a>) -> Self {
        Self {
            strict: false,
            ..Self::new(chunk)
        }
    }

    /// Whether a string key that is not UTF-8 has been found.
    pub(super) fn not_utf8(&self) -> bool {
        self.not_utf8
    }

    /// The next batch of the row group's keys, in file order.
    pub(super) fn visit(&mut self, keys: &impl KeyBatch) {
        let base = self.values;
        self.values += keys.len();
        if self.not_utf8 || keys.is_empty() {
            return;
        }
        if !keys.is_utf8() {
            self.not_utf8 = true;
            return;
        }
        if self.disorder.is_some() {
            return;
        }
        self.longest = self.longest.max(keys.max_len());
        let head = keys.key(0);
        if self.strict {
            if let Some(previous) = &self.last {
                let order = head.cmp(previous);
                if order != Ordering::Greater {
                    self.disorder = Some((order, head));
                    return;
                }
            }
            if let Some((at, order)) = keys.first_not_increasing() {
                self.disorder = Some((order, keys.key(at)));
                return;
            }
        }
        for at in self.fence_pages.within(base, keys.len()) {
            self.fences.push(keys.key(at));
        }
        self.last = Some(keys.key(keys.len() - 1));
        self.first.get_or_insert(head);
    }
}

/// Which decoder reads a row group's keys for `verify`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum KeyDecoder {
    /// A string or binary key chunk of plain pages sliced in place
    /// (`plain::checked_run`), where the chunk is one it reads; the column
    /// reader otherwise.
    Plain,
    /// Parquet's column reader (`RunBatches`).
    ColumnReader,
}

/// Every key of row group `chunk` decoded from `chunks` and checked
/// (`RunCheck`), with the decoder that read them: the plain reader when
/// `decoder` asks for it and it reads the chunk, which it does exactly as
/// the column reader would or not at all, and the column reader otherwise.
/// A row group of plain pages that ends in any fault is read twice, the
/// second time by the column reader, which reports the fault.
fn check_run<'c>(
    chunk: ColumnChunk<'c>,
    shape: KeyShape,
    chunks: &Arc<Segments>,
    decoder: KeyDecoder,
) -> Result<(RunCheck<'c>, KeyDecoder)> {
    let byte_keys = match shape {
        KeyShape::Str => Some(ByteKeys::Text),
        KeyShape::Bin => Some(ByteKeys::Bytes),
        KeyShape::Long | KeyShape::FixedBin | KeyShape::Uuid => None,
    };
    if decoder == KeyDecoder::Plain
        && let Some(keys) = byte_keys
        && let Some(run) = plain::checked_run(chunk, keys, chunks)
    {
        return Ok((run, KeyDecoder::Plain));
    }
    let mut run = RunCheck::new(chunk);
    read_run(chunk, shape, chunks, &mut run)?;
    Ok((run, KeyDecoder::ColumnReader))
}

/// The offset within rows `rows` of `chunk`, a key column whose values
/// decode as `shape`, of the key equal to `key`, read from `chunks`;
/// `None` where the rows do not hold it. Comes with the decoder that read
/// the rows: the plain reader (`plain::find_key`) for a string or binary
/// key column it reads, which answers exactly as the column reader would
/// or not at all, and the column reader otherwise (`decode_run`,
/// `Keys::binary_search`).
pub(super) fn find_in_run(
    chunk: ColumnChunk<'_>,
    shape: KeyShape,
    rows: Range<i64>,
    key: &Key,
    chunks: Arc<Segments>,
) -> Result<(Option<usize>, KeyDecoder)> {
    let wanted: Option<&[u8]> = match (shape, key) {
        (KeyShape::Str, Key::Str(wanted)) => Some(wanted.as_bytes()),
        (KeyShape::Bin, Key::Bytes(wanted)) => Some(wanted),
        (
            KeyShape::Long | KeyShape::Str | KeyShape::Bin | KeyShape::FixedBin | KeyShape::Uuid,
            Key::Long(_) | Key::Str(_) | Key::Bytes(_) | Key::Uuid(_),
        ) => None,
    };
    if let Some(wanted) = wanted
        && let Some(plain::Searched(found)) = plain::find_key(chunk, rows.clone(), wanted, &chunks)
    {
        return Ok((found, KeyDecoder::Plain));
    }
    let keys = decode_run(chunk, shape.run_shape(), rows, chunks)?;
    Ok((keys.binary_search(key).ok(), KeyDecoder::ColumnReader))
}

/// Every key of row group `chunk` decoded from `chunks` by the column
/// reader a batch of at most `DECODE_BATCH_RECORDS` rows at a time
/// (`RunBatches`) and handed to `run`. Each batch's buffer is handed back
/// to the decoder for the next, so a row group's keys cost one batch's
/// buffer rather than one buffer for all of them.
fn read_run(
    chunk: ColumnChunk<'_>,
    shape: KeyShape,
    chunks: &Arc<Segments>,
    run: &mut RunCheck<'_>,
) -> Result<()> {
    let rows = row_index(chunk.rows());
    let window = row_index(DECODE_BATCH_RECORDS);
    let mut batches = RunBatches::open(chunk, shape.run_shape(), chunks)?;
    let mut asked = 0;
    while asked < rows {
        let records = window.min(rows - asked);
        let Some((_, keys)) = batches.next(records)? else {
            break;
        };
        run.visit(&keys);
        batches.give_back(keys);
        asked += records;
    }
    batches.finish()
}

/// `FIXED_LEN_BYTE_ARRAY` values as the binary keys, or binary order
/// values, they are.
fn fixed_as_binary(values: Vec<FixedLenByteArray>) -> Vec<ByteArray> {
    values.into_iter().map(ByteArray::from).collect()
}

/// `FIXED_LEN_BYTE_ARRAY` values of a uuid column as uuid keys.
fn fixed_as_uuids(values: &[FixedLenByteArray]) -> Vec<[u8; UUID_BYTES]> {
    values
        .iter()
        .map(|value| {
            <[u8; UUID_BYTES]>::try_from(value.data()).expect(
                "a FIXED_LEN_BYTE_ARRAY value is its column's width, checked at build to be UUID_BYTES",
            )
        })
        .collect()
}

/// The refusal for `key`, which is not above the key before it in `chunk`:
/// `Equal` is a duplicate, anything else is out of order.
fn not_above_previous(chunk: ColumnChunk<'_>, order: Ordering, key: Key) -> RefusalReason {
    let path = chunk.column.path.to_string();
    match order {
        Ordering::Equal => RefusalReason::DuplicateKeys { path, key },
        Ordering::Less | Ordering::Greater => RefusalReason::Unsorted {
            path,
            row_group: chunk.row_group(),
        },
    }
}

/// Rows `rows` of the key or order column chunk `chunk`, whose values
/// decode as `shape`, decoded from `chunks`, which hold at least the pages
/// those rows are in. Exactly those rows come back, nulls left out, as
/// `read_column` guarantees, and an `INT32` column's values widened to
/// longs; a string column's values are not checked as UTF-8 here, the
/// caller does that before the run is read back through `Keys::get`.
pub(super) fn decode_run(
    chunk: ColumnChunk<'_>,
    shape: RunShape,
    rows: Range<i64>,
    chunks: Arc<Segments>,
) -> Result<Keys> {
    let skip = row_index(rows.start);
    let take = row_index(rows.end - rows.start);
    Ok(match shape {
        RunShape::Int32 => Keys::Long(
            read_column::<Int32Type>(chunk, chunks, skip, take)?
                .into_iter()
                .map(i64::from)
                .collect(),
        ),
        RunShape::Int64 => Keys::Long(read_column::<Int64Type>(chunk, chunks, skip, take)?),
        RunShape::Str => Keys::Str(read_column::<ByteArrayType>(chunk, chunks, skip, take)?),
        RunShape::Bin => Keys::Bin(read_column::<ByteArrayType>(chunk, chunks, skip, take)?),
        RunShape::FixedBin => Keys::Bin(fixed_as_binary(read_column::<FixedLenByteArrayType>(
            chunk, chunks, skip, take,
        )?)),
        RunShape::Uuid => Keys::Uuid(fixed_as_uuids(&read_column::<FixedLenByteArrayType>(
            chunk, chunks, skip, take,
        )?)),
    })
}

/// Every value of row group `chunk`'s key or order column decoded from
/// `chunks` a batch at a time by one column reader (`ChunkBatches`), each
/// batch as the `Keys` its shape decodes to, as `decode_run` decodes a
/// range of them.
pub(super) enum RunBatches<'a> {
    /// `INT32` values, widened to longs.
    Int32(ChunkBatches<'a, Int32Type>),
    /// `INT64` values.
    Long(ChunkBatches<'a, Int64Type>),
    /// `BYTE_ARRAY` values, strings when `text`.
    Bytes {
        batches: ChunkBatches<'a, ByteArrayType>,
        text: bool,
    },
    /// `FIXED_LEN_BYTE_ARRAY` values, uuids when `uuid`.
    Fixed {
        batches: ChunkBatches<'a, FixedLenByteArrayType>,
        uuid: bool,
    },
}

impl<'a> RunBatches<'a> {
    /// The decode of every row of `chunk`, whose values decode as `shape`.
    pub(super) fn open(
        chunk: ColumnChunk<'a>,
        shape: RunShape,
        chunks: &Arc<Segments>,
    ) -> Result<Self> {
        let rows = row_index(chunk.rows());
        let segments = Arc::clone(chunks);
        Ok(match shape {
            RunShape::Int32 => Self::Int32(ChunkBatches::open(chunk, segments, 0, rows)?),
            RunShape::Int64 => Self::Long(ChunkBatches::open(chunk, segments, 0, rows)?),
            RunShape::Str | RunShape::Bin => Self::Bytes {
                batches: ChunkBatches::open(chunk, segments, 0, rows)?,
                text: shape == RunShape::Str,
            },
            RunShape::FixedBin | RunShape::Uuid => Self::Fixed {
                batches: ChunkBatches::open(chunk, segments, 0, rows)?,
                uuid: shape == RunShape::Uuid,
            },
        })
    }

    /// The next `records` rows' values, with how many rows were decoded
    /// (`ChunkBatches::next`).
    pub(super) fn next(&mut self, records: usize) -> Result<Option<(usize, Keys)>> {
        Ok(match self {
            Self::Int32(batches) => batches.next(records)?.map(|(read, values, _)| {
                (
                    read,
                    Keys::Long(values.iter().copied().map(i64::from).collect()),
                )
            }),
            Self::Long(batches) => batches
                .next(records)?
                .map(|(read, values, _)| (read, Keys::Long(std::mem::take(values)))),
            Self::Bytes { batches, text } => batches.next(records)?.map(|(read, values, _)| {
                let values = std::mem::take(values);
                (
                    read,
                    if *text {
                        Keys::Str(values)
                    } else {
                        Keys::Bin(values)
                    },
                )
            }),
            Self::Fixed { batches, uuid } => batches.next(records)?.map(|(read, values, _)| {
                let keys = if *uuid {
                    Keys::Uuid(fixed_as_uuids(values))
                } else {
                    Keys::Bin(fixed_as_binary(std::mem::take(values)))
                };
                (read, keys)
            }),
        })
    }

    /// `keys`, a batch `next` returned, handed back as the buffer the next
    /// batch decodes into, so that every batch reuses it. A batch built
    /// from the decoded values rather than taken from them (`INT32`,
    /// `FIXED_LEN_BYTE_ARRAY`) leaves the buffer where it was.
    pub(super) fn give_back(&mut self, keys: Keys) {
        match (self, keys) {
            (Self::Long(batches), Keys::Long(values)) => batches.values = values,
            (Self::Bytes { batches, .. }, Keys::Str(values) | Keys::Bin(values)) => {
                batches.values = values;
            }
            (
                Self::Int32(_) | Self::Long(_) | Self::Bytes { .. } | Self::Fixed { .. },
                Keys::Long(_) | Keys::Str(_) | Keys::Bin(_) | Keys::Uuid(_),
            ) => {}
        }
    }

    /// `ChunkBatches::finish`.
    pub(super) fn finish(self) -> Result<()> {
        match self {
            Self::Int32(batches) => batches.finish(),
            Self::Long(batches) => batches.finish(),
            Self::Bytes { batches, .. } => batches.finish(),
            Self::Fixed { batches, .. } => batches.finish(),
        }
    }
}

/// Every value of a `Keys::Str` run is UTF-8, or the file is
/// `FileFault::KeyNotUtf8`; the other arms hold what their bytes are.
pub(super) fn check_utf8(chunk: ColumnChunk<'_>, keys: &Keys) -> Result<()> {
    if keys_are_utf8(keys) {
        Ok(())
    } else {
        Err(not_utf8(chunk))
    }
}

/// `FileFault::KeyNotUtf8` for the row group `chunk`.
fn not_utf8(chunk: ColumnChunk<'_>) -> Error {
    file_fault(
        chunk.column.path,
        FileFault::KeyNotUtf8 {
            row_group: chunk.row_group(),
        },
    )
}

/// Whether every value of a `Keys::Str` run is UTF-8; true for the other
/// arms. A value is first checked to be ASCII, which the standard library
/// does a word at a time inline, and only a value that is not goes through
/// the full validator. Joining a batch's values into one buffer and
/// validating that once costs as much in the copy as it saves in the
/// validator for keys of a few dozen bytes (measured on the benchmark's
/// string table).
pub(super) fn keys_are_utf8(keys: &Keys) -> bool {
    let Keys::Str(run) = keys else {
        return true;
    };
    run.iter().all(|value| is_utf8(value.data()))
}

/// Whether `value` is UTF-8, checked as ASCII first (`keys_are_utf8`).
pub(super) fn is_utf8(value: &[u8]) -> bool {
    value.is_ascii() || std::str::from_utf8(value).is_ok()
}

/// The most records one `read_records` call asks the column reader for:
/// sixteen pages of the served layout, so a call's overhead is spread over
/// many pages. The reader sizes the value and level buffers of a call by
/// the records asked for, capped by the values its page has left, so the
/// buffers a call grows hold at most this many entries, under a mebibyte
/// for any Parquet type, whatever a footer claims for a row group or a
/// header for a page.
pub(super) const DECODE_BATCH_RECORDS: i64 = 16 * PAGE_ROWS;

/// `take` records of `chunk` after skipping `skip`, decoded from
/// `segments`, which hold at least the pages those rows are in and the
/// chunk's dictionary page when it has one. Only those pages are read,
/// the dictionary page first, as the reader requires (`ChunkPages`), and
/// the column reader skips the rows before `skip` within the first of
/// them; no page before it is read or walked past. Definition levels are
/// consumed when the column is nullable, and a null yields no value, so
/// fewer values than `take` may come back. On `Ok` exactly `take` records
/// were decoded, from exactly the pages the offset index names for them,
/// each page holding the rows the index gives it; a page that does not is
/// `FileFault::PageRowsMismatch`, so no row position rests on an offset
/// index the pages contradict.
///
/// `take` is the footer's number and sizes no buffer by itself. The column
/// reader is asked for at most `DECODE_BATCH_RECORDS` records a call, into
/// a batch buffer and a level buffer cleared before each call, and the
/// batch is appended to the result after it. The column reader sizes the
/// values and levels of a call by the smaller of the records asked for and
/// the values its page has left, so no value or level buffer it grows
/// holds more than a batch, whatever the footer claims for the row group
/// or a header for its page, and the result grows only with values
/// decoded. A data page whose header claims more values than the offset
/// index gives it rows is refused before the column reader decodes it. A
/// page whose values run out short of its header's count ends the decode
/// in an error, which is `FileFault::PageRowsMismatch` when a header read
/// on the way disagrees with the offset index and `Error::Parquet`
/// otherwise, or, for a byte-array page, in a panic inside parquet.
///
/// Three allocations happen before this function sees what sized them.
/// A page's uncompressed size, from its header, is reserved when the page
/// is decompressed, by `ChunkPages` for a zstd page and by parquet's page
/// reader for another codec (a Snappy chunk zero-fills it), and a
/// dictionary page's value count, from its header, sizes the dictionary
/// parquet decodes into. The callers read those headers ahead of the page
/// reader and refuse one that declares more than its page can hold: `verify`
/// every key page's before the row group's first decode
/// (`check_page_headers`) and, in an ordered file, every order page's
/// (`check_order_page_headers`); a reader of a file's edge keys its key
/// page's (`check_key_page_headers`); a Scan, those of the order pages it
/// decodes (`check_order_pages`); and every value decode, a lookup's, a
/// Scan's, and the doctor's, its value pages' and their dictionary page's
/// (`checked_values`). A key page decoded by a lookup or a Scan is one
/// `verify` checked at load, read from the resident chunks, the chunk file,
/// or, in the Bloom tier, the data file again. A delta-length or delta
/// byte-array page sizes its lengths by the count in its encoded data,
/// which no check reads.
///
/// Parquet panics on some page bodies the headers do not reveal: a
/// byte-array page holding fewer values than its header counts, a
/// dictionary page among them, and others. The decode, decompression
/// included, runs under `catch_unwind`, so a panic is
/// `FileFault::DecoderPanicked` at the last page the page reader took up,
/// and ends this load, lookup, or Scan rather than the task running it.
pub(super) fn read_column<T: ParquetDataType>(
    chunk: ColumnChunk<'_>,
    segments: Arc<Segments>,
    skip: usize,
    take: usize,
) -> Result<Vec<T::T>> {
    let mut values = Vec::new();
    read_batches_with_levels::<T>(chunk, segments, skip, take, &mut |batch, _levels| {
        values.append(batch);
    })?;
    Ok(values)
}

/// What a decode hands each batch of values to, with the batch's
/// definition levels (`read_batches_with_levels`).
type LevelledSink<'a, V> = dyn FnMut(&mut Vec<V>, &[i16]) + 'a;

/// `read_column`, with each batch of at most `DECODE_BATCH_RECORDS` records
/// handed to `sink` as it is decoded instead of gathered: the values of a
/// batch, in file order, in a buffer the sink may read or take and that is
/// cleared before the next batch, and beside them the batch's definition
/// levels, one per record, empty for a column that is not nullable, a
/// record whose level is below the column's greatest one a null that
/// holds no value. Everything `read_column` checks once the decode ends is
/// checked after the last batch, so a sink sees batches of a decode that
/// may still fail.
fn read_batches_with_levels<T: ParquetDataType>(
    chunk: ColumnChunk<'_>,
    segments: Arc<Segments>,
    skip: usize,
    take: usize,
    sink: &mut LevelledSink<'_, T::T>,
) -> Result<()> {
    let mut batches = ChunkBatches::<T>::open(chunk, segments, skip, take)?;
    let batch_records = row_index(DECODE_BATCH_RECORDS);
    while batches.asked < take {
        let Some((_, values, levels)) = batches.next(batch_records.min(take - batches.asked))?
        else {
            break;
        };
        sink(values, levels);
    }
    batches.finish()
}

/// One batch a `ChunkBatches` decoded: how many records, their values, and
/// their definition levels.
pub(super) type Batch<'b, V> = (usize, &'b mut Vec<V>, &'b [i16]);

/// The rows of one column chunk decoded in order by one column reader, a
/// batch at a time as the caller asks (`next`), so that the chunk's
/// dictionary page is decoded once and each data page once however many
/// batches the rows take, and two chunks of a row group can be decoded in
/// lockstep. `read_column` documents what is read and checked; `finish`
/// checks what the decode produced against the offset index once the
/// caller stops asking. Each step runs under `catch_unwind`, a panic
/// `FileFault::DecoderPanicked` at the last page the page reader took up,
/// after which the decode is not driven again.
pub(super) struct ChunkBatches<'a, T: ParquetDataType> {
    chunk: ColumnChunk<'a>,
    /// The first row decoded.
    skip: usize,
    tally: Arc<Mutex<PageTally>>,
    reader: ColumnReaderImpl<T>,
    nullable: bool,
    /// The records asked for so far; every record of the decode once a
    /// skip to its first ends short, since none of them can be decoded.
    asked: usize,
    /// The records decoded so far.
    decoded: usize,
    /// The error the decode ended in.
    failed: Option<parquet::errors::ParquetError>,
    /// Whether the decode ended: in an error, short of the records asked
    /// for, or by skipping short of `skip`.
    ended: bool,
    values: Vec<T::T>,
    levels: Vec<i16>,
}

impl<'a, T: ParquetDataType> ChunkBatches<'a, T> {
    /// A decode of the `take` records of `chunk` after `skip`, from
    /// `segments`, which hold at least the pages those rows are in and the
    /// chunk's dictionary page when it has one; no record is decoded yet.
    pub(super) fn open(
        chunk: ColumnChunk<'a>,
        segments: Arc<Segments>,
        skip: usize,
        take: usize,
    ) -> Result<Self> {
        let tally = Arc::new(Mutex::new(PageTally::default()));
        let descr = chunk.column.descriptor();
        let nullable = descr.max_def_level() > 0;
        // Unwind-safe: the page reader and the column reader are built
        // inside the closure and dropped with a panic, the footer `chunk`
        // borrows is only read, and the tally holds each page's record
        // whole, written under its lock.
        let opened = unless_panicked(
            chunk.column.path,
            AssertUnwindSafe(|| -> Result<_> {
                let pages =
                    ChunkPages::new(chunk, skip..skip + take, segments, Arc::clone(&tally))?;
                let within = skip.checked_sub(pages.first_row()).expect(
                    "the first page read is the one holding row `skip`, and inspect checked that a chunk's first page starts at row zero",
                );
                let mut reader: ColumnReaderImpl<T> = ColumnReaderImpl::new(descr, Box::new(pages));
                let skipped = (within > 0).then(|| reader.skip_records(within));
                Ok((reader, skipped.map(|skipped| (within, skipped))))
            }),
            || Self::panicked_at(chunk, &tally),
        )??;
        let (reader, skipped) = opened;
        let mut batches = Self {
            chunk,
            skip,
            tally,
            reader,
            nullable,
            asked: 0,
            decoded: 0,
            failed: None,
            ended: false,
            values: Vec::new(),
            levels: Vec::new(),
        };
        let short = match skipped {
            None => false,
            Some((within, Ok(skipped))) => skipped != within,
            Some((_, Err(error))) => {
                batches.failed = Some(error);
                true
            }
        };
        if short {
            batches.ended = true;
            batches.asked = take;
        }
        Ok(batches)
    }

    /// The part a panic in the decode of `chunk` is reported at.
    fn panicked_at(chunk: ColumnChunk<'_>, tally: &Mutex<PageTally>) -> DecodedPart {
        DecodedPart::Pages {
            row_group: chunk.row_group(),
            page: tally.lock().unwrap_or_else(PoisonError::into_inner).taken,
        }
    }

    /// The next `records` records, at most `DECODE_BATCH_RECORDS`: how many
    /// were decoded, their values, nulls left out, in a buffer the caller
    /// may read or take and that is cleared before the next batch, and
    /// their definition levels (`read_batches_with_levels`). `None` once
    /// the decode has ended or decodes no record; a batch short of
    /// `records` ends it. What ended it is reported by `finish`.
    pub(super) fn next(&mut self, records: usize) -> Result<Option<Batch<'_, T::T>>> {
        if self.ended {
            return Ok(None);
        }
        self.asked += records;
        self.values.clear();
        self.levels.clear();
        let (reader, values, levels, nullable) = (
            &mut self.reader,
            &mut self.values,
            &mut self.levels,
            self.nullable,
        );
        // Unwind-safe: after a panic the decode is never driven again, what
        // the buffers hold is dropped with the error it becomes, the tally
        // holds each page's record whole, written under its lock, and the
        // thread's zstd context starts each page afresh.
        let read = unless_panicked(
            self.chunk.column.path,
            AssertUnwindSafe(|| {
                reader.read_records(records, nullable.then_some(levels), None, values)
            }),
            || Self::panicked_at(self.chunk, &self.tally),
        )?;
        let read = match read {
            Ok((read, _, _)) => read,
            Err(error) => {
                self.failed = Some(error);
                self.ended = true;
                return Ok(None);
            }
        };
        self.decoded += read;
        self.ended = read < records;
        Ok((read > 0).then_some((read, &mut self.values, &self.levels[..])))
    }

    /// Whether the decode produced what was asked for: the records asked
    /// for, from exactly the pages the offset index names for them, each
    /// holding the rows the index gives it (`ColumnChunk::check_pages_read`);
    /// a page refused for a header that claims more values than its rows is
    /// `FileFault::PageRowsMismatch` at that page.
    pub(super) fn finish(self) -> Result<()> {
        let tally = self.tally.lock().unwrap_or_else(PoisonError::into_inner);
        if let Some(page) = tally.refused {
            return Err(self.chunk.page_rows_mismatch(page));
        }
        let decoded = self.failed.map_or(Ok(self.decoded), Err);
        self.chunk
            .check_pages_read(self.skip..self.skip + self.asked, &tally.seen, decoded)
    }
}

/// Rows `rows` of the value column chunk `chunk` from `segments`, one
/// entry per row, `None` for a null: the values of the value pages holding
/// them, which the caller has fetched with the chunk's dictionary page and
/// whose headers it has checked (`checked_values`); `read_column` says why.
/// Exactly those rows decode, from exactly the pages the offset index names
/// for them, as `read_column` guarantees; a column whose levels and values
/// disagree is `FileFault::PageRowsMismatch` at the rows' first page.
pub(super) fn read_optional<T: ParquetDataType>(
    chunk: ColumnChunk<'_>,
    rows: Range<i64>,
    segments: Arc<Segments>,
) -> Result<Vec<Option<T::T>>> {
    let skip = row_index(rows.start);
    let take = row_index(rows.end - rows.start);
    let present = chunk.column.descriptor().max_def_level();
    let mut values = Vec::with_capacity(take);
    let mut aligned = true;
    read_batches_with_levels::<T>(chunk, segments, skip, take, &mut |batch, levels| {
        if levels.is_empty() {
            values.extend(batch.drain(..).map(Some));
            return;
        }
        let mut held = batch.drain(..);
        for &level in levels {
            if level == present {
                let value = held.next();
                aligned &= value.is_some();
                values.push(value);
            } else {
                values.push(None);
            }
        }
        aligned &= held.next().is_none();
    })?;
    if !aligned || values.len() != take {
        return Err(chunk.page_rows_mismatch(chunk.page_holding(skip)));
    }
    Ok(values)
}

/// Rows of a value column chunk a read decoded (`decode_values`), in
/// order, each value given as `value_bytes` keeps it once asked for.
pub(super) enum ValueRows {
    /// Read in place from plain pages (`plain::value_rows`), held as the
    /// pages they lie in until a value is asked for, so a row never asked
    /// for costs no copy.
    Plain {
        values: plain::PlainValues,
        segments: Arc<Segments>,
    },
    /// Decoded by parquet's column reader, each value kept already.
    Decoded(Vec<Option<Bytes>>),
}

impl ValueRows {
    /// The rows decoded.
    pub(super) fn len(&self) -> usize {
        match self {
            Self::Plain { values, .. } => values.len(),
            Self::Decoded(values) => values.len(),
        }
    }

    /// The value of the `at`th row decoded, which is below `len`, as
    /// `value_bytes` keeps it; `None` for a null.
    pub(super) fn value(&self, at: usize) -> Option<Bytes> {
        match self {
            Self::Plain { values, segments } => {
                values.value(at).map(|value| value_bytes(segments, value))
            }
            Self::Decoded(values) => values[at].clone(),
        }
    }
}

/// Rows `rows` of the value column chunk `chunk`, its data pages `pages`
/// holding them, whose values decode as `shape`, from `segments`, which
/// hold those pages and the chunk's dictionary page when it has one: the
/// one value decode, a lookup's, a Scan's, and the doctor's. The pages'
/// headers are checked before parquet reads them (`check_value_pages`),
/// then the rows decode (`decode_values`).
pub(super) fn checked_values(
    chunk: ColumnChunk<'_>,
    shape: ValueShape,
    pages: Range<usize>,
    rows: Range<i64>,
    segments: &Arc<Segments>,
) -> Result<ValueRows> {
    check_value_pages(chunk, shape, pages, segments)?;
    decode_values(chunk, shape, rows, segments)
}

/// `checked_values` once the headers are checked: one entry per row,
/// `None` for a null. A chunk of `PLAIN` byte-array pages is read in place
/// (`plain::value_rows`); any other chunk, or one holding a page that
/// reader does not read exactly, by parquet's column reader
/// (`read_optional`), which meets and reports every fault. Either way the
/// values are the column reader's.
pub(super) fn decode_values(
    chunk: ColumnChunk<'_>,
    shape: ValueShape,
    rows: Range<i64>,
    segments: &Arc<Segments>,
) -> Result<ValueRows> {
    let keep = |value: &[u8]| value_bytes(segments, value);
    Ok(match shape {
        ValueShape::Variable => {
            if let Some(values) = plain::value_rows(chunk, rows.clone(), segments) {
                return Ok(ValueRows::Plain {
                    values,
                    segments: Arc::clone(segments),
                });
            }
            ValueRows::Decoded(
                read_optional::<ByteArrayType>(chunk, rows, Arc::clone(segments))?
                    .iter()
                    .map(|value| value.as_ref().map(|value| keep(value.data())))
                    .collect(),
            )
        }
        ValueShape::Fixed => ValueRows::Decoded(
            read_optional::<FixedLenByteArrayType>(chunk, rows, Arc::clone(segments))?
                .iter()
                .map(|value| value.as_ref().map(|value| keep(value.data())))
                .collect(),
        ),
    })
}

#[cfg(test)]
mod tests {
    use parquet::basic::{Compression, Encoding};
    use parquet::file::metadata::{PageIndexPolicy, ParquetMetaDataReader};
    use parquet::file::properties::EnabledStatistics;

    use super::*;
    use crate::approx::{self, count};
    use crate::cache::Issue;
    use crate::config::{ConfigError, ConfigViolation};
    use crate::error::ErrorKind;
    use crate::generation::testing::Dictionary;
    use crate::generation::testing::{
        self, KeyColumn, TABLE, cache, column_of, page_rows, refusal_of,
    };
    use crate::index::identity_of;
    use crate::index::page_header::{
        BYTE_ARRAY_LENGTH_PREFIX_BYTES, MOST_BYTES_PER_VALUE, MOST_KEY_PAGE_BYTES,
        MOST_VALUE_PAGE_BYTES, NIBBLE_MASK, PAGE_ENCODING_ALLOWANCE_BYTES, PageBody, PageHeader,
    };
    use crate::index::testing::{
        FEW_ROWS, FIXED_KEY_BYTES, Fixture, INFLATED_GROUP_ROWS, ORDER_COLUMN, ORDER_FIELD_ID,
        Opened, Orders, PAGES_PER_GROUP, ROW_GROUPS, SMALL_BATCH, absent_after, contract_rows,
        fault_of, group_rows, header_at, key_for, location, longs, moved_by_header, ordered_layout,
        pages_of, predicted_of, row_of, stepped, store, store_as, store_file, with_column_chunk,
        with_dictionary_one_entry_short, with_group_rows, with_header_claims, with_pages,
    };
    use crate::index::{
        ChunkPage, Column, DecodedPart, FileFault, HeaderField, KeyShape, PageHeaderFault, RowRef,
        WireType,
    };
    use crate::index::{Pair, PairPart};
    use crate::key::KeyType;
    use crate::local::testing as local_testing;
    use crate::manifest::TableLayout;
    use crate::manifest::testing::KEY_COLUMN;
    use crate::order::{OrderFacts, OrderType, OrderValue};
    use parquet::basic::PageType;
    use parquet::file::page_index::column_index::ColumnIndexMetaData;
    use parquet::file::properties::WriterVersion;
    use rand::rngs::StdRng;
    use rand::{RngExt as _, SeedableRng as _};

    /// Rows per row group in the hand-written fixtures with two row groups.
    const TINY_GROUP_ROWS: usize = 2;

    /// A row count a footer may claim for one row group that no file holds:
    /// two to the fiftieth, eight pebibytes of long keys, beyond the address
    /// space of the supported targets, so a buffer reserved for that many
    /// keys aborts the process before any check runs.
    const UNALLOCATABLE_ROWS: i64 = 1 << 50;
    /// The bytes overwritten at the start of a bloom filter to corrupt it,
    /// and the byte they are overwritten with: no thrift field header.
    const GARBAGE_BYTES: usize = 32;
    const GARBAGE: u8 = 0xff;
    /// `FEW_ROWS` string keys of exactly `len` bytes: zero-padded decimals.
    fn keys_of_len(len: usize) -> Vec<Option<Key>> {
        (0..FEW_ROWS)
            .map(|i| Some(Key::Str(format!("{:0>len$}", stepped(i)))))
            .collect()
    }

    #[tokio::test]
    async fn test_build_serves_keys_of_max_key_bytes_and_refuses_one_byte_over() {
        let opened = store(&Fixture::with_keys(
            KeyColumn::Utf8,
            keys_of_len(MAX_KEY_BYTES),
        ))
        .await;
        let index = opened.index(KeyType::String).await;
        assert_eq!(index.max_key_bytes(), MAX_KEY_BYTES);
        for i in 0..FEW_ROWS {
            let key = Key::Str(format!("{:0>MAX_KEY_BYTES$}", stepped(i)));
            let row = index
                .find(&key, None, Issue::default())
                .await
                .unwrap()
                .unwrap();
            assert_eq!(
                row,
                RowRef {
                    row_group: 0,
                    row: i64::try_from(i).unwrap()
                }
            );
        }

        let over = store(&Fixture::with_keys(
            KeyColumn::Utf8,
            keys_of_len(MAX_KEY_BYTES + 1),
        ))
        .await;
        for residency in Residency::ALL {
            let dir = tempfile::tempdir().unwrap();
            assert_eq!(
                refusal_of(over.build_in(KeyType::String, residency, dir.path()).await),
                RefusalReason::KeyTooLong {
                    path: location(),
                    bytes: MAX_KEY_BYTES + 1,
                }
            );
        }
    }

    #[tokio::test]
    async fn test_build_refuses_an_unsorted_row_group_in_every_tier() {
        let opened = store(&Fixture::with_keys(KeyColumn::Int64, longs(&[5, 3, 9]))).await;
        for residency in Residency::ALL {
            let dir = tempfile::tempdir().unwrap();
            assert_eq!(
                refusal_of(opened.build_in(KeyType::Long, residency, dir.path()).await),
                RefusalReason::Unsorted {
                    path: location(),
                    row_group: 0,
                }
            );
        }
        assert_eq!(opened.stats().local_reads(), 0);
    }

    #[tokio::test]
    async fn test_build_refuses_a_row_group_starting_below_the_previous_last_key() {
        let mut fixture = Fixture::with_keys(KeyColumn::Int64, longs(&[10, 20, 5, 30]));
        fixture.layout.row_group_rows = TINY_GROUP_ROWS;
        let opened = store(&fixture).await;
        for residency in Residency::ALL {
            let dir = tempfile::tempdir().unwrap();
            let index = opened.build_in(KeyType::Long, residency, dir.path()).await;
            assert_eq!(
                refusal_of(index),
                RefusalReason::Unsorted {
                    path: location(),
                    row_group: 1,
                }
            );
        }
        let mut sorted = Fixture::with_keys(KeyColumn::Int64, longs(&[10, 20, 25, 30]));
        sorted.layout.row_group_rows = TINY_GROUP_ROWS;
        let index = store(&sorted).await.index(KeyType::Long).await;
        assert_eq!(index.metadata().num_row_groups(), 2);
        assert_eq!(
            index
                .find(&Key::Long(25), None, Issue::default())
                .await
                .unwrap(),
            Some(RowRef {
                row_group: 1,
                row: 0
            })
        );
        assert_eq!(
            index
                .find(&Key::Long(22), None, Issue::default())
                .await
                .unwrap(),
            None
        );
    }

    #[tokio::test]
    async fn test_build_refuses_a_duplicate_key_in_every_tier() {
        let within = store(&Fixture::with_keys(KeyColumn::Int64, longs(&[1, 2, 2, 3]))).await;
        let mut across = Fixture::with_keys(KeyColumn::Int64, longs(&[10, 20, 20, 30]));
        across.layout.row_group_rows = TINY_GROUP_ROWS;
        let across = store(&across).await;
        for residency in Residency::ALL {
            let dir = tempfile::tempdir().unwrap();
            assert_eq!(
                refusal_of(within.build_in(KeyType::Long, residency, dir.path()).await),
                RefusalReason::DuplicateKeys {
                    path: location(),
                    key: Key::Long(2),
                }
            );
            assert_eq!(
                refusal_of(across.build_in(KeyType::Long, residency, dir.path()).await),
                RefusalReason::DuplicateKeys {
                    path: location(),
                    key: Key::Long(20),
                }
            );
        }
        assert_eq!(
            across
                .handle
                .load_metadata()
                .await
                .unwrap()
                .num_row_groups(),
            2
        );
    }

    #[tokio::test]
    async fn test_build_refuses_a_null_key_in_every_tier() {
        let opened = store(&Fixture::with_keys(
            KeyColumn::Int64,
            vec![Some(Key::Long(1)), None, Some(Key::Long(3))],
        ))
        .await;
        for residency in Residency::ALL {
            let dir = tempfile::tempdir().unwrap();
            assert_eq!(
                refusal_of(opened.build_in(KeyType::Long, residency, dir.path()).await),
                RefusalReason::NullKeys {
                    path: location(),
                    row_group: 0,
                }
            );
        }
    }

    #[tokio::test]
    async fn test_build_refuses_a_string_key_that_is_not_utf8() {
        let opened = store(&Fixture::with_keys(
            KeyColumn::Binary,
            (0..FEW_ROWS)
                .map(|i| Some(key_for(i, KeyType::Binary)))
                .collect(),
        ))
        .await;
        assert_eq!(
            fault_of(opened.build(KeyType::String, Residency::Full, None).await),
            FileFault::KeyNotUtf8 { row_group: 0 }
        );
    }

    /// A row group longer than one decode batch (`DECODE_BATCH_RECORDS`)
    /// is checked across the batches it decodes in: a clean one records
    /// every page's first key as a fence wherever its batch begins, a
    /// duplicate whose copies end one batch and begin the next is refused,
    /// and a key that is not UTF-8 in the second batch is found.
    #[tokio::test]
    async fn test_verify_checks_a_row_group_across_its_decode_batches() {
        let batch = row_index(DECODE_BATCH_RECORDS);
        let rows = 2 * batch;
        let one_group = |mut fixture: Fixture| {
            fixture.layout.row_group_rows = rows;
            fixture
        };
        let clean = store(&one_group(Fixture::contract(KeyType::Long, rows))).await;
        let fences: Vec<Key> = (0..rows)
            .step_by(page_rows())
            .map(|i| key_for(i, KeyType::Long))
            .collect();
        assert_eq!(clean.index(KeyType::Long).await.fences, [fences]);

        let mut keys: Vec<Option<Key>> =
            (0..rows).map(|i| Some(key_for(i, KeyType::Long))).collect();
        keys[batch] = keys[batch - 1].clone();
        let straddling = store(&one_group(Fixture::with_keys(KeyColumn::Int64, keys))).await;
        assert_eq!(
            refusal_of(straddling.build(KeyType::Long, Residency::Full, None).await),
            RefusalReason::DuplicateKeys {
                path: location(),
                key: key_for(batch - 1, KeyType::Long),
            }
        );

        let not_utf8_at = batch + 1;
        let strings: Vec<Option<Key>> = (0..rows)
            .map(|i| {
                let mut bytes = key_for(i, KeyType::String).as_bytes().to_vec();
                if i == not_utf8_at {
                    // Above the key itself and below the next: order holds.
                    bytes.push(u8::MAX);
                }
                Some(Key::Bytes(bytes))
            })
            .collect();
        let late = store(&one_group(Fixture::with_keys(KeyColumn::Binary, strings))).await;
        assert_eq!(
            fault_of(late.build(KeyType::String, Residency::Full, None).await),
            FileFault::KeyNotUtf8 { row_group: 0 }
        );
    }

    /// ASCII values pass on the fast check, a value with a multi-byte
    /// character passes the full validator, and half of one character is
    /// refused, alone or after ASCII values.
    #[test]
    fn test_keys_are_utf8_takes_ascii_and_multibyte_and_refuses_a_cut_character() {
        let text = |values: &[&[u8]]| {
            Keys::Str(
                values
                    .iter()
                    .map(|value| ByteArray::from(value.to_vec()))
                    .collect(),
            )
        };
        let e_acute = "\u{e9}".as_bytes();
        assert!(keys_are_utf8(&text(&[b"a", e_acute, b"b"])));
        assert!(!keys_are_utf8(&text(&[&e_acute[..1], &e_acute[1..]])));
        assert!(!keys_are_utf8(&text(&[b"a", &e_acute[..1]])));
        assert!(keys_are_utf8(&Keys::Bin(vec![ByteArray::from(vec![
            u8::MAX
        ])])));
    }

    /// A footer that claims more rows for a row group than any page could
    /// hold is refused by the pages read, and nothing is reserved for the
    /// claimed rows on the way: the buffer grows with what the pages hold.
    #[tokio::test]
    async fn test_verify_refuses_an_unallocatable_row_count_without_reserving_for_it() {
        let opened = store(&Fixture::contract(KeyType::Long, contract_rows())).await;
        let loaded = opened.handle.load_metadata().await.unwrap();
        let key_leaf = 0;
        let chunks = key_chunks(&opened, &loaded, key_leaf).await;
        let path = location();
        let inflated = with_group_rows(&loaded, 0, UNALLOCATABLE_ROWS);
        let column = Column {
            metadata: &inflated,
            leaf: key_leaf,
            path: &path,
        };
        assert_eq!(
            fault_of(
                verify(TABLE, &column, KeyShape::Long, Rules::of(None), &chunks)
                    .map(|verified| verified.groups)
            ),
            FileFault::PageRowsMismatch {
                row_group: 0,
                page: PAGES_PER_GROUP - 1,
            }
        );
    }

    /// The bytes of the fixture's key column chunks, as `build` fetches
    /// them for verification.
    async fn key_chunks(
        opened: &Opened,
        metadata: &ParquetMetaData,
        key_leaf: usize,
    ) -> Arc<Segments> {
        let ranges = column_chunk_ranges(metadata, key_leaf, opened.handle.size());
        let parts = fetch_chunk_parts(&opened.handle, &ranges).await.unwrap();
        Arc::new(Segments::new(parts, opened.handle.size()))
    }

    /// A key page whose header counts other rows than the span its offset
    /// index gives it is refused at build, whether the row group's count
    /// shortens the last page's span or a shifted first row moves a page
    /// boundary; without the check, the second footer would read row 999 as
    /// page 1's first row and serve row 1000's value for it.
    #[tokio::test]
    async fn test_verify_refuses_a_key_page_whose_rows_differ_from_its_offset_index() {
        let opened = store(&Fixture::contract(KeyType::Long, contract_rows())).await;
        let loaded = opened.handle.load_metadata().await.unwrap();
        let key_leaf = 0;
        let chunks = key_chunks(&opened, &loaded, key_leaf).await;
        let path = location();
        let verify_with = |metadata: &ParquetMetaData| {
            let column = Column {
                metadata,
                leaf: key_leaf,
                path: &path,
            };
            verify(TABLE, &column, KeyShape::Long, Rules::of(None), &chunks)
                .map(|verified| verified.groups.len())
        };
        assert_eq!(verify_with(&loaded).unwrap(), ROW_GROUPS);
        // A row group without rows, and so without pages, reads no page and
        // holds no keys; the other row groups verify as before.
        let empty_group = with_pages(&with_group_rows(&loaded, 0, 0), key_leaf, Vec::new());
        assert_eq!(verify_with(&empty_group).unwrap(), ROW_GROUPS - 1);

        let short_group = with_group_rows(&loaded, 0, PAGE_ROWS + PAGE_ROWS / 2);
        assert_eq!(
            fault_of(verify_with(&short_group)),
            FileFault::PageRowsMismatch {
                row_group: 0,
                page: 1,
            }
        );
        let mut shifted = pages_of(&loaded, key_leaf);
        shifted[1].first_row_index -= 1;
        let shifted = with_pages(&loaded, key_leaf, shifted);
        assert_eq!(
            fault_of(verify_with(&shifted)),
            FileFault::PageRowsMismatch {
                row_group: 0,
                page: 0,
            }
        );
    }

    /// The header of `page` of row group 0's key chunk, as read ahead of
    /// parquet.
    fn header_of(
        metadata: &ParquetMetaData,
        key_leaf: usize,
        chunks: &Segments,
        page: ChunkPage,
    ) -> PageHeader {
        let (range, bytes) = &chunks.parts()[0];
        assert_eq!(
            range.start,
            metadata.row_group(0).column(key_leaf).byte_range().0
        );
        PageHeader::read(&bytes[header_at(metadata, key_leaf, page)..]).unwrap()
    }

    /// The fixture's key chunks and footer with `field` in the header of
    /// `page` of row group 0 claiming `claimed`: the varint rewritten in the
    /// chunk's bytes, and the page, the chunk, and every later page of the
    /// chunk moved in the footer by the change in the varint's width.
    async fn with_first_group_header(
        opened: &Opened,
        loaded: &ParquetMetaData,
        key_leaf: usize,
        page: ChunkPage,
        field: HeaderField,
        claimed: i32,
    ) -> (ParquetMetaData, Arc<Segments>) {
        with_first_group_headers(opened, loaded, key_leaf, page, &[(field, claimed)]).await
    }

    /// `with_first_group_header` with each of `claims`, a field and what it
    /// claims, rewritten in turn.
    async fn with_first_group_headers(
        opened: &Opened,
        loaded: &ParquetMetaData,
        key_leaf: usize,
        page: ChunkPage,
        claims: &[(HeaderField, i32)],
    ) -> (ParquetMetaData, Arc<Segments>) {
        let ranges = column_chunk_ranges(loaded, key_leaf, opened.handle.size());
        let mut parts = fetch_chunk_parts(&opened.handle, &ranges).await.unwrap();
        let (range, bytes) = &parts[0];
        let patched = with_header_claims(bytes, header_at(loaded, key_leaf, page), claims);
        let grown = i64::try_from(patched.len() - bytes.len()).unwrap();
        let start = range.start;
        parts[0] = (start..start + count(patched.len()), Bytes::from(patched));
        let metadata = moved_by_header(loaded, key_leaf, page, grown);
        (
            metadata,
            Arc::new(Segments::new(parts, opened.handle.size())),
        )
    }

    /// `verify` of the key column over `metadata` and `chunks`, down to the
    /// row groups it verified.
    fn verify_groups(
        metadata: &ParquetMetaData,
        key_leaf: usize,
        shape: KeyShape,
        chunks: &Arc<Segments>,
    ) -> Result<usize> {
        let path = location();
        let column = Column {
            metadata,
            leaf: key_leaf,
            path: &path,
        };
        verify(TABLE, &column, shape, Rules::of(None), chunks).map(|verified| verified.groups.len())
    }

    /// A key page whose header claims more values than the rows its offset
    /// index gives it is refused before the column reader decodes it, so the
    /// claim sizes no buffer: the reader would size its first decode by the
    /// smaller of the claim and the row group's rows.
    #[tokio::test]
    async fn test_verify_refuses_a_page_header_claiming_more_values_than_its_rows() {
        let opened = store(&Fixture::contract(KeyType::Long, contract_rows())).await;
        let loaded = opened.handle.load_metadata().await.unwrap();
        let key_leaf = 0;
        let path = location();
        let span = i32::try_from(PAGE_ROWS).unwrap();
        for claimed in [span, span + 1, i32::MAX] {
            let (metadata, chunks) = with_first_group_header(
                &opened,
                &loaded,
                key_leaf,
                ChunkPage::Data(0),
                HeaderField::NumValues,
                claimed,
            )
            .await;
            let column = Column {
                metadata: &metadata,
                leaf: key_leaf,
                path: &path,
            };
            let verified = verify(TABLE, &column, KeyShape::Long, Rules::of(None), &chunks)
                .map(|verified| verified.groups);
            if claimed == span {
                assert_eq!(verified.unwrap().len(), ROW_GROUPS, "the count as written");
                continue;
            }
            assert_eq!(
                fault_of(verified),
                FileFault::PageRowsMismatch {
                    row_group: 0,
                    page: 0,
                },
                "{claimed} values claimed"
            );
        }
    }

    /// A row count a footer may claim for one row group: two to the
    /// thirty-first, one more than `i32::MAX`, so a row group with a single
    /// page gives that page a span with room for a header claiming
    /// `i32::MAX` values, and the span check lets the claim through.
    const SPAN_OF_I32_MAX_ROWS: i64 = 1 << 31;

    /// A footer that gives row group 0 one key page spanning
    /// `SPAN_OF_I32_MAX_ROWS` rows, whose header claims `i32::MAX` values
    /// over the thousand the page holds, is refused as a page that does not
    /// hold the rows its header claims, and the claim sizes no buffer on the
    /// way. The column reader sizes each decode by the smaller of the
    /// records a call asks for and the values its page has left: with one
    /// call for the whole row group that is `i32::MAX` longs, 16 GiB,
    /// reserved and zeroed before the page runs out. `read_column` asks for
    /// at most `DECODE_BATCH_RECORDS` records a call, into buffers it clears
    /// before each call, so no value or level buffer the reader grows holds
    /// more than a batch, whatever the footer or the header claims; the
    /// decode then fails where the page's values end, and the header's
    /// count, checked against the offset index, is the refusal.
    #[tokio::test]
    async fn test_verify_refuses_a_page_claiming_i32_max_values_in_a_span_that_holds_them() {
        let opened = store(&Fixture::contract(KeyType::Long, contract_rows())).await;
        let loaded = opened.handle.load_metadata().await.unwrap();
        let key_leaf = 0;
        let path = location();
        let (metadata, chunks) = with_first_group_header(
            &opened,
            &loaded,
            key_leaf,
            ChunkPage::Data(0),
            HeaderField::NumValues,
            i32::MAX,
        )
        .await;
        let first_page = pages_of(&metadata, key_leaf)[0].clone();
        let one_page = with_pages(&metadata, key_leaf, vec![first_page]);
        let lying = with_group_rows(&one_page, 0, SPAN_OF_I32_MAX_ROWS);
        let column = Column {
            metadata: &lying,
            leaf: key_leaf,
            path: &path,
        };
        assert_eq!(
            fault_of(
                verify(TABLE, &column, KeyShape::Long, Rules::of(None), &chunks)
                    .map(|verified| verified.groups)
            ),
            FileFault::PageRowsMismatch {
                row_group: 0,
                page: 0,
            }
        );
    }

    /// The bound on an uncompressed page of `values` values: every value a
    /// key at the cap with its encoding, plus the page's allowance.
    fn page_bound(values: u64) -> u64 {
        values * MOST_BYTES_PER_VALUE + PAGE_ENCODING_ALLOWANCE_BYTES
    }

    /// The fault `FileFault::PageHeaderOverBound` names for row group 0.
    fn over_bound(page: ChunkPage, field: HeaderField, declared: i32, allowed: u64) -> FileFault {
        FileFault::PageHeaderOverBound {
            row_group: 0,
            page,
            field,
            declared: u64::try_from(declared).unwrap(),
            allowed,
        }
    }

    /// A key page whose header declares an uncompressed size above what its
    /// rows could encode to with every key at the cap is refused before
    /// parquet reads the page, which would reserve the declared size and
    /// decompress into it; a size at the bound is left to parquet, whose
    /// decompression then finds the page's real size.
    #[tokio::test]
    async fn test_verify_refuses_a_key_page_declaring_more_uncompressed_bytes_than_its_rows_hold() {
        let opened = store(&Fixture::contract(KeyType::Long, contract_rows())).await;
        let loaded = opened.handle.load_metadata().await.unwrap();
        let key_leaf = 0;
        let chunks = key_chunks(&opened, &loaded, key_leaf).await;
        let page = ChunkPage::Data(0);
        let written = header_of(&loaded, key_leaf, &chunks, page).uncompressed_page_size;
        let allowed = page_bound(count(page_rows()));
        let at_bound = i32::try_from(allowed).unwrap();
        for claimed in [
            i32::try_from(written).unwrap(),
            at_bound,
            at_bound + 1,
            i32::MAX,
        ] {
            let (metadata, chunks) = with_first_group_header(
                &opened,
                &loaded,
                key_leaf,
                page,
                HeaderField::UncompressedPageSize,
                claimed,
            )
            .await;
            let verified = verify_groups(&metadata, key_leaf, KeyShape::Long, &chunks);
            if claimed == at_bound {
                assert!(
                    matches!(verified, Err(Error::Parquet(_))),
                    "at the bound parquet decodes the page: {verified:?}"
                );
                continue;
            }
            if claimed < at_bound {
                assert_eq!(verified.unwrap(), ROW_GROUPS, "the size as written");
                continue;
            }
            assert_eq!(
                fault_of(verified),
                over_bound(page, HeaderField::UncompressedPageSize, claimed, allowed),
                "{claimed} bytes declared"
            );
        }
    }

    /// A dictionary page declaring more values than its row group has rows
    /// is refused before parquet sizes the dictionary by the count, and so
    /// is one whose uncompressed size is above what that count of keys at
    /// the cap could encode to.
    #[tokio::test]
    async fn test_verify_refuses_a_dictionary_declaring_more_than_its_row_group_holds() {
        let mut fixture = Fixture::contract(KeyType::Long, contract_rows());
        fixture.layout.dictionary = Dictionary::Key;
        let opened = store(&fixture).await;
        let loaded = opened.handle.load_metadata().await.unwrap();
        let key_leaf = 0;
        let chunks = key_chunks(&opened, &loaded, key_leaf).await;
        let page = ChunkPage::Dictionary;
        let written = header_of(&loaded, key_leaf, &chunks, page);
        let rows = count(group_rows());
        assert_eq!(
            written.body,
            PageBody::Dictionary {
                num_values: u32::try_from(rows).unwrap()
            },
            "every key of the row group is in its dictionary"
        );
        let rows_claim = i32::try_from(rows).unwrap();
        for claimed in [rows_claim, rows_claim + 1, i32::MAX] {
            let (metadata, chunks) = with_first_group_header(
                &opened,
                &loaded,
                key_leaf,
                page,
                HeaderField::NumValues,
                claimed,
            )
            .await;
            let verified = verify_groups(&metadata, key_leaf, KeyShape::Long, &chunks);
            if claimed == rows_claim {
                assert_eq!(verified.unwrap(), ROW_GROUPS, "the count as written");
                continue;
            }
            assert_eq!(
                fault_of(verified),
                over_bound(page, HeaderField::NumValues, claimed, rows),
                "{claimed} values declared"
            );
        }
        let (metadata, chunks) = with_first_group_header(
            &opened,
            &loaded,
            key_leaf,
            page,
            HeaderField::UncompressedPageSize,
            i32::MAX,
        )
        .await;
        assert_eq!(
            fault_of(verify_groups(&metadata, key_leaf, KeyShape::Long, &chunks)),
            over_bound(
                page,
                HeaderField::UncompressedPageSize,
                i32::MAX,
                page_bound(rows)
            )
        );
    }

    /// A key page declaring more than `MOST_KEY_PAGE_BYTES` uncompressed is
    /// refused even when the footer claims so many rows for its row group
    /// that the span bound alone would admit it.
    #[tokio::test]
    async fn test_verify_refuses_a_key_page_over_the_ceiling_whatever_its_footer_claims() {
        let opened = store(&Fixture::contract(KeyType::Long, contract_rows())).await;
        let loaded = opened.handle.load_metadata().await.unwrap();
        let key_leaf = 0;
        let last = PAGES_PER_GROUP - 1;
        let page = ChunkPage::Data(last);
        let claimed = i32::try_from(MOST_KEY_PAGE_BYTES + 1).unwrap();
        let (patched, chunks) = with_first_group_header(
            &opened,
            &loaded,
            key_leaf,
            page,
            HeaderField::UncompressedPageSize,
            claimed,
        )
        .await;
        let metadata = with_group_rows(&patched, 0, INFLATED_GROUP_ROWS);
        let span = INFLATED_GROUP_ROWS - pages_of(&metadata, key_leaf)[last].first_row_index;
        assert!(
            page_bound(u64::try_from(span).unwrap()) > u64::try_from(claimed).unwrap(),
            "the span bound alone admits the claim"
        );
        assert_eq!(
            fault_of(verify_groups(&metadata, key_leaf, KeyShape::Long, &chunks)),
            over_bound(
                page,
                HeaderField::UncompressedPageSize,
                claimed,
                MOST_KEY_PAGE_BYTES
            )
        );
    }

    /// A key dictionary declaring more entries than `MOST_KEY_PAGE_BYTES`
    /// holds at the fewest bytes an `INT64` entry takes is refused, even
    /// when the footer claims more rows for its row group than that.
    #[tokio::test]
    async fn test_verify_refuses_a_dictionary_counting_more_entries_than_the_ceiling_holds() {
        let mut fixture = Fixture::contract(KeyType::Long, contract_rows());
        fixture.layout.dictionary = Dictionary::Key;
        let opened = store(&fixture).await;
        let loaded = opened.handle.load_metadata().await.unwrap();
        let key_leaf = 0;
        let page = ChunkPage::Dictionary;
        let most = MOST_KEY_PAGE_BYTES / count(size_of::<i64>());
        let claimed = i32::try_from(most + 1).unwrap();
        let (patched, chunks) = with_first_group_header(
            &opened,
            &loaded,
            key_leaf,
            page,
            HeaderField::NumValues,
            claimed,
        )
        .await;
        let metadata = with_group_rows(&patched, 0, INFLATED_GROUP_ROWS);
        assert!(
            INFLATED_GROUP_ROWS > i64::from(claimed),
            "the row bound alone admits the claim"
        );
        assert_eq!(
            fault_of(verify_groups(&metadata, key_leaf, KeyShape::Long, &chunks)),
            over_bound(page, HeaderField::NumValues, claimed, most)
        );
    }

    /// A byte-array key dictionary counting fewer entries than
    /// `MOST_KEY_PAGE_BYTES` holds at the fewest bytes an entry encodes to,
    /// but more than it holds at the bytes parquet keeps a decoded entry
    /// in, is refused before parquet sizes the dictionary by the count.
    #[tokio::test]
    async fn test_verify_refuses_a_key_dictionary_over_the_ceiling_in_memory() {
        let mut fixture = Fixture::contract(KeyType::String, contract_rows());
        fixture.layout.dictionary = Dictionary::Key;
        let opened = store(&fixture).await;
        let loaded = opened.handle.load_metadata().await.unwrap();
        let key_leaf = 0;
        let page = ChunkPage::Dictionary;
        let held = MOST_KEY_PAGE_BYTES / count(size_of::<ByteArray>());
        let encoded = MOST_KEY_PAGE_BYTES / count(size_of::<u32>());
        let claimed = i32::try_from(held + 1).unwrap();
        assert!(
            u64::try_from(claimed).unwrap() <= encoded,
            "the encoded bound alone admits the claim"
        );
        let (patched, chunks) = with_first_group_header(
            &opened,
            &loaded,
            key_leaf,
            page,
            HeaderField::NumValues,
            claimed,
        )
        .await;
        let metadata = with_group_rows(&patched, 0, INFLATED_GROUP_ROWS);
        assert_eq!(
            fault_of(verify_groups(&metadata, key_leaf, KeyShape::Str, &chunks)),
            over_bound(page, HeaderField::NumValues, claimed, held)
        );
    }

    /// A byte-array key dictionary whose header counts one entry more than
    /// its body holds passes every header bound: the count stays below the
    /// row group's rows, which repeated keys leave above the distinct keys
    /// the dictionary holds. Parquet panics decoding the entry that is not
    /// there; the load ends in `FileFault::DecoderPanicked` naming the
    /// dictionary page, in every tier, before the repeated keys are seen.
    #[tokio::test]
    async fn test_build_a_key_dictionary_short_of_its_count_is_a_file_fault() {
        let repeats = 2;
        let keys = (0..contract_rows())
            .map(|i| Some(key_for(i / repeats, KeyType::String)))
            .collect();
        let mut fixture = Fixture::with_keys(column_of(KeyType::String), keys);
        fixture.layout.dictionary = Dictionary::Key;
        let key_leaf = 0;
        let (short, written) = with_dictionary_one_entry_short(&fixture.write(), key_leaf);
        assert_eq!(u64::from(written), count(group_rows() / repeats));
        let opened = store_file(short).await;
        for residency in Residency::ALL {
            let dir = tempfile::tempdir().unwrap();
            let fault = fault_of(
                opened
                    .build_in(KeyType::String, residency, dir.path())
                    .await,
            );
            assert!(
                matches!(
                    fault,
                    FileFault::DecoderPanicked {
                        part: DecodedPart::Pages {
                            row_group: 0,
                            page: Some(ChunkPage::Dictionary),
                        },
                        ..
                    }
                ),
                "{residency:?}: {fault:?}"
            );
        }
    }

    /// A key page whose header declares compressed bytes past its place in
    /// the offset index is refused.
    #[tokio::test]
    async fn test_verify_refuses_a_key_page_whose_compressed_size_runs_past_its_place() {
        let opened = store(&Fixture::contract(KeyType::Long, contract_rows())).await;
        let loaded = opened.handle.load_metadata().await.unwrap();
        let key_leaf = 0;
        let chunks = key_chunks(&opened, &loaded, key_leaf).await;
        let page = ChunkPage::Data(0);
        let written = header_of(&loaded, key_leaf, &chunks, page).compressed_page_size;
        let written = i32::try_from(written).unwrap();
        for claimed in [written, written + 1] {
            let (metadata, chunks) = with_first_group_header(
                &opened,
                &loaded,
                key_leaf,
                page,
                HeaderField::CompressedPageSize,
                claimed,
            )
            .await;
            let verified = verify_groups(&metadata, key_leaf, KeyShape::Long, &chunks);
            if claimed == written {
                assert_eq!(verified.unwrap(), ROW_GROUPS, "the size as written");
                continue;
            }
            assert_eq!(
                fault_of(verified),
                over_bound(
                    page,
                    HeaderField::CompressedPageSize,
                    claimed,
                    u64::try_from(written).unwrap()
                )
            );
        }
    }

    /// A v2 key page whose level lengths are negative, or add up past its
    /// uncompressed size, is refused before parquet reads the page, which
    /// adds the two as `i32`s: two near `i32::MAX` overflow the add. The
    /// file as written, a v2 file, loads.
    #[tokio::test]
    async fn test_verify_refuses_a_v2_key_page_whose_level_lengths_overflow_its_size() {
        let mut fixture = Fixture::contract(KeyType::Long, contract_rows());
        fixture.layout.writer_version = WriterVersion::PARQUET_2_0;
        let opened = store(&fixture).await;
        let loaded = opened.handle.load_metadata().await.unwrap();
        let key_leaf = 0;
        let chunks = key_chunks(&opened, &loaded, key_leaf).await;
        let page = ChunkPage::Data(0);
        let written = header_of(&loaded, key_leaf, &chunks, page);
        assert!(
            matches!(written.body, PageBody::DataV2 { .. }),
            "{:?} is not a v2 page",
            written.body
        );
        assert_eq!(
            verify_groups(&loaded, key_leaf, KeyShape::Long, &chunks).unwrap(),
            ROW_GROUPS,
            "the file as written"
        );
        let near_max = i32::MAX;
        let (metadata, chunks) = with_first_group_headers(
            &opened,
            &loaded,
            key_leaf,
            page,
            &[
                (HeaderField::DefinitionLevelsByteLength, near_max),
                (HeaderField::RepetitionLevelsByteLength, near_max),
            ],
        )
        .await;
        let near_max = u64::try_from(near_max).unwrap();
        assert_eq!(
            fault_of(verify_groups(&metadata, key_leaf, KeyShape::Long, &chunks)),
            FileFault::PageHeaderOverBound {
                row_group: 0,
                page,
                field: HeaderField::LevelsByteLength,
                declared: near_max + near_max,
                allowed: u64::from(written.uncompressed_page_size),
            }
        );
        for field in [
            HeaderField::DefinitionLevelsByteLength,
            HeaderField::RepetitionLevelsByteLength,
        ] {
            let (metadata, chunks) =
                with_first_group_header(&opened, &loaded, key_leaf, page, field, -1).await;
            assert_eq!(
                fault_of(verify_groups(&metadata, key_leaf, KeyShape::Long, &chunks)),
                FileFault::MalformedPageHeader {
                    row_group: 0,
                    page,
                    fault: PageHeaderFault::Negative(field),
                },
                "{field}"
            );
        }
    }

    /// A key page header that parquet would not read to the numbers the
    /// pre-scan reads, or that is not a data page where the offset index
    /// places one, is refused as malformed.
    #[tokio::test]
    async fn test_verify_refuses_a_malformed_key_page_header() {
        let opened = store(&Fixture::contract(KeyType::Long, contract_rows())).await;
        let loaded = opened.handle.load_metadata().await.unwrap();
        let key_leaf = 0;
        let page = ChunkPage::Data(0);
        let malformed = |fault| FileFault::MalformedPageHeader {
            row_group: 0,
            page,
            fault,
        };
        let past_known_types = PageType::DATA_PAGE_V2 as i32 + 1;
        for (field, claimed, fault) in [
            (
                HeaderField::PageType,
                PageType::INDEX_PAGE as i32,
                PageHeaderFault::UnexpectedPageType(PageType::INDEX_PAGE),
            ),
            (
                HeaderField::PageType,
                past_known_types,
                PageHeaderFault::UnknownPageType(past_known_types),
            ),
            (
                HeaderField::UncompressedPageSize,
                -1,
                PageHeaderFault::Negative(HeaderField::UncompressedPageSize),
            ),
            (
                HeaderField::NumValues,
                -1,
                PageHeaderFault::Negative(HeaderField::NumValues),
            ),
        ] {
            let (metadata, chunks) =
                with_first_group_header(&opened, &loaded, key_leaf, page, field, claimed).await;
            assert_eq!(
                fault_of(verify_groups(&metadata, key_leaf, KeyShape::Long, &chunks)),
                malformed(fault),
                "{field} {claimed}"
            );
        }
        let ranges = column_chunk_ranges(&loaded, key_leaf, opened.handle.size());
        let mut parts = fetch_chunk_parts(&opened.handle, &ranges).await.unwrap();
        let mut bytes = parts[0].1.to_vec();
        let past_known_wire_types = WireType::Struct.code() + 1;
        let at = header_at(&loaded, key_leaf, page);
        bytes[at] = (bytes[at] & !NIBBLE_MASK) | past_known_wire_types;
        parts[0].1 = Bytes::from(bytes);
        let chunks = Arc::new(Segments::new(parts, opened.handle.size()));
        assert_eq!(
            fault_of(verify_groups(&loaded, key_leaf, KeyShape::Long, &chunks)),
            malformed(PageHeaderFault::UnknownWireType(past_known_wire_types))
        );
    }

    /// `rows` binary keys that do not compress: digests of the row number,
    /// sorted. The Bloom tier saves memory on keys shaped like these (RFD 017).
    fn random_binary_keys(rows: usize) -> Vec<Option<Key>> {
        let mut digests: Vec<Vec<u8>> = (0..rows)
            .map(|i| crate::local::digest(&i.to_le_bytes()).to_vec())
            .collect();
        digests.sort();
        digests
            .into_iter()
            .map(|digest| Some(Key::Bytes(digest)))
            .collect()
    }

    #[tokio::test]
    async fn test_bloom_tier_holds_no_key_bytes_after_build() {
        let rows = contract_rows();
        let opened = store(&Fixture::with_keys(
            KeyColumn::Binary,
            random_binary_keys(rows),
        ))
        .await;
        let full = opened.index(KeyType::Binary).await;
        let bloom = opened
            .build(KeyType::Binary, Residency::Bloom, None)
            .await
            .unwrap();
        assert!(
            bloom.resident_bytes() < full.resident_bytes(),
            "{} bloom against {} full",
            bloom.resident_bytes(),
            full.resident_bytes()
        );
        let predicted = predicted_of(&full);
        assert_eq!(count(full.resident_bytes()), predicted.full);
        assert_eq!(count(bloom.resident_bytes()), predicted.bloom.unwrap());
        let stats = opened.stats();
        let before = stats.gets();
        let chunks = bloom.key_chunks_for_verification().await.unwrap();
        assert_eq!(
            stats.gets(),
            before + count(ROW_GROUPS),
            "the chunks are fetched again"
        );
        let held = |segments: &Segments| -> usize {
            segments.parts().iter().map(|(_, bytes)| bytes.len()).sum()
        };
        assert_eq!(
            held(&chunks),
            held(&full.key_chunks_for_verification().await.unwrap())
        );
        assert_eq!(
            stats.gets(),
            before + count(ROW_GROUPS),
            "full hands out its resident chunks"
        );
    }

    #[tokio::test]
    async fn test_bloom_tier_corrupt_filter_is_a_file_fault() {
        let fixture = Fixture::contract(KeyType::Long, FEW_ROWS);
        let file = fixture.write();
        let metadata = ParquetMetaDataReader::new()
            .parse_and_finish(&file)
            .unwrap();
        let key_leaf = 0;
        let offset = usize::try_from(
            metadata
                .row_group(0)
                .column(key_leaf)
                .bloom_filter_offset()
                .unwrap(),
        )
        .unwrap();
        let mut corrupted = file.to_vec();
        corrupted[offset..offset + GARBAGE_BYTES].fill(GARBAGE);
        let opened = store_file(Bytes::from(corrupted)).await;
        assert_eq!(
            fault_of(opened.build(KeyType::Long, Residency::Bloom, None).await),
            FileFault::CorruptBloomFilter { row_group: 0 }
        );
        let index = opened.index(KeyType::Long).await;
        assert_eq!(index.residency(), Residency::Full);
    }

    /// The key column's filters of the fixture behind `opened`, read as
    /// `build` reads them, from the ranges `metadata` bounds.
    async fn filters_of(opened: &Opened, metadata: &ParquetMetaData) -> Result<Vec<Sbbf>> {
        let key_leaf = 0;
        let ranges = bloom_ranges(TABLE, &opened.handle, metadata, key_leaf)?;
        read_bloom_filters(&opened.handle, &ranges).await
    }

    /// A filter whose bitset holds no block, or whose footer length cuts
    /// the bitset short, is refused at build: a filter of no block has no
    /// first block to index on a check, and a cut bitset answers for keys
    /// it never saw.
    #[tokio::test]
    async fn test_read_bloom_filters_refuses_a_filter_of_no_block_or_one_cut_short() {
        let fixture = Fixture::contract(KeyType::Long, FEW_ROWS);
        let file = fixture.write();
        let metadata = ParquetMetaDataReader::new()
            .parse_and_finish(&file)
            .unwrap();
        let key_leaf = 0;
        let column = metadata.row_group(0).column(key_leaf);
        let offset = usize::try_from(column.bloom_filter_offset().unwrap()).unwrap();
        let length = column.bloom_filter_length().unwrap();
        let intact = store_file(file.clone()).await;
        let filters = filters_of(&intact, &metadata).await.unwrap();
        assert_eq!(filters.len(), 1);
        assert!(filters[0].num_blocks() > 0);

        // A filter of no block as parquet serializes one: a header saying
        // zero bitset bytes and nothing after it, in place of the written
        // filter, with the footer bounding exactly those bytes.
        let mut no_block = Vec::new();
        Sbbf::new(&[]).write(&mut no_block).unwrap();
        let mut overwritten = file.to_vec();
        overwritten[offset..offset + no_block.len()].copy_from_slice(&no_block);
        let opened = store_file(Bytes::from(overwritten)).await;
        let no_block_footer = with_column_chunk(&metadata, 0, key_leaf, |chunk| {
            chunk.set_bloom_filter_length(Some(i32::try_from(no_block.len()).unwrap()))
        });
        assert_eq!(
            fault_of(filters_of(&opened, &no_block_footer).await),
            FileFault::CorruptBloomFilter { row_group: 0 }
        );

        // The footer bounds one byte less than the header and bitset take.
        let cut_short = with_column_chunk(&metadata, 0, key_leaf, |chunk| {
            chunk.set_bloom_filter_length(Some(length - 1))
        });
        assert_eq!(
            fault_of(filters_of(&intact, &cut_short).await),
            FileFault::CorruptBloomFilter { row_group: 0 }
        );
    }

    #[tokio::test]
    async fn test_disk_tier_reuses_a_matching_chunk_file_and_rewrites_a_stale_one() {
        let rows = contract_rows();
        let dir = tempfile::tempdir().unwrap();
        let opened = store(&Fixture::contract(KeyType::Long, rows)).await;
        let stats = opened.stats();
        let local = testing::local_dir(dir.path(), opened.stats(), None);
        let index = opened
            .build(KeyType::Long, Residency::Disk, Some(&local))
            .await
            .unwrap();
        assert_eq!(stats.gets(), 1 + count(ROW_GROUPS));
        let cache = cache().await;
        let hit = group_rows() + 1;

        // A restart: a fresh handle over the same bytes, whose footer digest
        // is computed anew, finds the chunk file by identity.
        let reads = stats.local_reads();
        let restarted = store_file(opened.file.clone()).await;
        let again = restarted
            .build(KeyType::Long, Residency::Disk, Some(&local))
            .await
            .unwrap();
        assert_eq!(restarted.stats().gets(), 1, "the footer only");
        assert_eq!(
            stats.local_reads(),
            reads,
            "no chunk is read back: the chunk file's verification record stands in for them"
        );
        assert_eq!(again.key_range(), index.key_range());
        assert_eq!(again.fences(), index.fences());
        let row = again
            .find(&key_for(hit, KeyType::Long), Some(&cache), Issue::default())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(row, row_of(hit, group_rows()));

        // Another file at the same location, whose size and footer differ:
        // the chunk file on disk is stale for it, so the chunks are fetched
        // and the file rewritten, and a build over the original file after
        // that fetches again.
        let reads = stats.local_reads();
        let other = store(&Fixture::contract(KeyType::Long, rows + page_rows())).await;
        assert_ne!(other.file.len(), opened.file.len());
        let rewritten = other
            .build(KeyType::Long, Residency::Disk, Some(&local))
            .await
            .unwrap();
        assert_eq!(
            other.stats().gets(),
            1 + count(ROW_GROUPS + 1),
            "footer, then every chunk of the other file"
        );
        assert_eq!(stats.local_reads(), reads, "nothing was read back");
        assert_eq!(rewritten.rows(), i64::try_from(rows + page_rows()).unwrap());
        let original = store_file(opened.file.clone()).await;
        original
            .build(KeyType::Long, Residency::Disk, Some(&local))
            .await
            .unwrap();
        assert_eq!(
            original.stats().gets(),
            1 + count(ROW_GROUPS),
            "the chunk file now holds the other file's chunks"
        );
        assert_eq!(stats.local_reads(), reads);
    }

    /// Every how many keys of the contract fixture the stale-file tests
    /// probe: a prime, so the sample falls at every offset within a page.
    const PROBE_STRIDE: usize = 97;

    /// Every `stride`th key of the contract fixture of `key_type`, the last
    /// key, and the absent key just after each.
    fn probes(key_type: KeyType, stride: usize) -> Vec<Key> {
        (0..contract_rows())
            .step_by(stride)
            .chain([contract_rows() - 1])
            .flat_map(|i| {
                let key = key_for(i, key_type);
                [absent_after(&key), key]
            })
            .collect()
    }

    /// Asserts that `index` holds the range, fences, and longest key
    /// `oracle` does, and answers every `stride`th probe of `key_type` as
    /// `oracle` does: the same row for a present key, none for an absent
    /// one.
    async fn assert_lookups_match(
        index: &FileIndex,
        oracle: &FileIndex,
        key_type: KeyType,
        stride: usize,
    ) {
        assert_eq!(index.key_range(), oracle.key_range());
        assert_eq!(index.fences(), oracle.fences());
        assert_eq!(index.max_key_bytes(), oracle.max_key_bytes());
        for key in probes(key_type, stride) {
            assert_eq!(
                index.find(&key, None, Issue::default()).await.unwrap(),
                oracle.find(&key, None, Issue::default()).await.unwrap(),
                "{key_type} {key}"
            );
        }
    }

    #[tokio::test]
    async fn test_disk_tier_reuse_decodes_no_key_page_and_answers_as_a_fresh_load() {
        for key_type in KeyType::ALL {
            let dir = tempfile::tempdir().unwrap();
            let opened = store(&Fixture::contract(key_type, contract_rows())).await;
            let local = testing::local_dir(dir.path(), opened.stats(), None);
            opened
                .build(key_type, Residency::Disk, Some(&local))
                .await
                .unwrap();
            let oracle = opened.index(key_type).await;

            let reads = opened.stats().local_reads();
            let restarted = store_file(opened.file.clone()).await;
            let reused = restarted
                .build(key_type, Residency::Disk, Some(&local))
                .await
                .unwrap();
            assert_eq!(restarted.stats().gets(), 1, "{key_type}: the footer only");
            assert_eq!(
                opened.stats().local_reads(),
                reads,
                "{key_type}: no chunk was read back to be decoded"
            );
            assert_lookups_match(&reused, &oracle, key_type, 1).await;
        }
    }

    /// The chunk file `local` holds for `opened`'s data file.
    fn chunk_file_of(opened: &Opened, local: &LocalDir) -> std::path::PathBuf {
        local.chunk_path(&identity_of(&opened.handle), ChunkRules::KeyValue)
    }

    /// A Disk-tier build of the contract fixture of `key_type` in a fresh
    /// local directory: the file, the directory, and its chunk file.
    struct Written {
        _dir: tempfile::TempDir,
        opened: Opened,
        local: Arc<LocalDir>,
        path: std::path::PathBuf,
    }

    /// An edit a test makes to a written chunk file.
    type ChunkFileEdit = fn(&Written);

    async fn written(key_type: KeyType) -> Written {
        let dir = tempfile::tempdir().unwrap();
        let opened = store(&Fixture::contract(key_type, contract_rows())).await;
        let local = testing::local_dir(dir.path(), opened.stats(), None);
        opened
            .build(key_type, Residency::Disk, Some(&local))
            .await
            .unwrap();
        let path = chunk_file_of(&opened, &local);
        Written {
            _dir: dir,
            opened,
            local,
            path,
        }
    }

    impl Written {
        /// A restarted Disk-tier build as `key_type`: a fresh handle over
        /// the same bytes, and its GETs.
        async fn restart(&self, key_type: KeyType) -> (FileIndex, u64) {
            let restarted = store_file(self.opened.file.clone()).await;
            let index = restarted
                .build(key_type, Residency::Disk, Some(&self.local))
                .await
                .unwrap();
            (index, restarted.stats().gets())
        }

        /// Why a restarted Disk-tier build as `key_type` would not trust
        /// the chunk file, `None` for no file; panics when it would.
        async fn stale(&self, key_type: KeyType) -> Option<Stale> {
            let restarted = store_file(self.opened.file.clone()).await;
            let inspected = FileIndex::inspect(
                TABLE,
                Arc::clone(&restarted.handle),
                &crate::manifest::testing::layout(key_type, None),
            )
            .await
            .unwrap();
            let ranges = column_chunk_ranges(
                &inspected.metadata,
                inspected.layout.key_leaf,
                inspected.handle.size(),
            );
            let to_verify = KeysToVerify::of(&inspected);
            reused(&to_verify, &self.local, &ranges)
                .await
                .unwrap()
                .expect_err("the chunk file is trusted")
        }

        /// Asserts that a restart as `key_type` does not trust the chunk
        /// file, for the reason `stale`: it fetches every chunk again,
        /// answers as a fresh load, and rewrites the file, which the
        /// restart after it trusts.
        async fn assert_rebuilt_then_reused(
            &self,
            key_type: KeyType,
            case: &str,
            stale: Option<Stale>,
        ) {
            assert_eq!(self.stale(key_type).await, stale, "{case}");
            let oracle = self.opened.index(key_type).await;
            let (rebuilt, gets) = self.restart(key_type).await;
            assert_eq!(
                gets,
                1 + count(ROW_GROUPS),
                "{case}: footer, then every chunk"
            );
            assert_lookups_match(&rebuilt, &oracle, key_type, PROBE_STRIDE).await;
            let reads = self.opened.stats().local_reads();
            let (reused, gets) = self.restart(key_type).await;
            assert_eq!(gets, 1, "{case}: the rewritten file is reused");
            assert_eq!(self.opened.stats().local_reads(), reads, "{case}");
            assert_lookups_match(&reused, &oracle, key_type, PROBE_STRIDE).await;
        }
    }

    #[tokio::test]
    async fn test_reusable_disk_chunks_never_trusts_a_file_verified_under_other_rules() {
        let key_type = KeyType::Long;
        let written = written(key_type).await;
        let inspected = FileIndex::inspect(
            TABLE,
            Arc::clone(&written.opened.handle),
            &crate::manifest::testing::layout(key_type, None),
        )
        .await
        .unwrap();
        let ranges = column_chunk_ranges(
            &inspected.metadata,
            inspected.layout.key_leaf,
            inspected.handle.size(),
        );
        let on_disk = |rules| {
            reusable_disk_chunks(
                &inspected.handle,
                &written.local,
                &ranges,
                verified_under(key_type, rules),
            )
            .unwrap()
        };
        assert!(matches!(on_disk(ChunkRules::KeyValue), OnDisk::Reusable(_)));
        let ordered = ChunkRules::Ordered {
            order_field_id: ORDER_FIELD_ID,
        };
        assert!(
            matches!(on_disk(ordered), OnDisk::Absent),
            "the key-value file is not the ordered file"
        );
        std::fs::rename(
            &written.path,
            written
                .local
                .chunk_path(&identity_of(&inspected.handle), ordered),
        )
        .unwrap();
        assert!(matches!(
            on_disk(ordered),
            OnDisk::Stale(Stale::Rejected(Rejection::NameMismatch))
        ));
    }

    /// The bytes at the end of a torn chunk file that a crash left
    /// unwritten: zeros, which a filesystem returns for blocks it allocated
    /// and never wrote.
    const UNWRITTEN_TAIL_BYTES: usize = 64;

    /// Sets the length of the file at `path`.
    fn set_len(path: &std::path::Path, len: u64) {
        std::fs::OpenOptions::new()
            .write(true)
            .open(path)
            .unwrap()
            .set_len(len)
            .unwrap();
    }

    #[tokio::test]
    async fn test_disk_tier_rebuilds_every_stale_chunk_file_rather_than_failing() {
        let key_type = KeyType::String;
        let cases: [(&str, ChunkFileEdit, Stale); 8] = [
            (
                "payload byte flipped",
                |written| {
                    let len = std::fs::metadata(&written.path).unwrap().len();
                    let last = std::fs::read(&written.path).unwrap()[approx::length(len - 1)];
                    local_testing::overwrite(&written.path, len - 1, &[last ^ GARBAGE]);
                },
                Stale::Rejected(Rejection::ChecksumMismatch),
            ),
            (
                "payload truncated",
                |written| {
                    let len = std::fs::metadata(&written.path).unwrap().len();
                    set_len(&written.path, len - 1);
                },
                Stale::Rejected(Rejection::ShortPayload),
            ),
            (
                "tail never written",
                |written| {
                    let len = std::fs::metadata(&written.path).unwrap().len();
                    let unwritten = [0; UNWRITTEN_TAIL_BYTES];
                    let at = len - count(unwritten.len());
                    local_testing::overwrite(&written.path, at, &unwritten);
                },
                Stale::Rejected(Rejection::ChecksumMismatch),
            ),
            (
                "previous format version",
                |written| {
                    local_testing::set_version_byte(&written.path, local_testing::PREVIOUS_VERSION);
                },
                Stale::Rejected(Rejection::UnsupportedVersion {
                    version: local_testing::PREVIOUS_VERSION,
                }),
            ),
            (
                "verifier version bumped",
                |written| {
                    local_testing::set_verifier_version(&written.path, VERIFIER_VERSION + 1);
                },
                Stale::VerifierVersion {
                    stored: VERIFIER_VERSION + 1,
                },
            ),
            (
                "header garbage",
                |written| local_testing::fill_header(&written.path, GARBAGE),
                Stale::Rejected(Rejection::UndecodableHeader),
            ),
            (
                "record with a key over the longest",
                |written| {
                    local_testing::edit_verification(&written.path, |bytes| {
                        *bytes = record::with_fences(bytes, |fences| {
                            fences[0][0] = vec![b'u'; MAX_KEY_BYTES + 1];
                        });
                    });
                },
                Stale::Record(RecordFault::Key { row_group: 0 }),
            ),
            (
                "record with a fence too many",
                |written| {
                    local_testing::edit_verification(&written.path, |bytes| {
                        *bytes = record::with_fences(bytes, |fences| {
                            let repeated = fences[0][0].clone();
                            fences[0].push(repeated);
                        });
                    });
                },
                Stale::Record(RecordFault::Fences {
                    row_group: 0,
                    stored: PAGES_PER_GROUP + 1,
                }),
            ),
        ];
        for (case, edit, stale) in cases {
            let written = written(key_type).await;
            edit(&written);
            written
                .assert_rebuilt_then_reused(key_type, case, Some(stale))
                .await;
        }
    }

    #[tokio::test]
    async fn test_disk_tier_rebuilds_a_chunk_file_whose_record_is_over_the_footer_bound() {
        let key_type = KeyType::Long;
        let written = written(key_type).await;
        let most = record::most_bytes(&written.opened.index(key_type).await.key_column(), None);
        local_testing::edit_verification(&written.path, |bytes| {
            bytes.resize(approx::length(most + 1), 0);
        });
        written
            .assert_rebuilt_then_reused(
                key_type,
                "record over the bound",
                Some(Stale::Rejected(Rejection::VerificationTooLarge {
                    len: most + 1,
                    most,
                })),
            )
            .await;
    }

    #[tokio::test]
    async fn test_disk_tier_rebuilds_a_chunk_file_verified_as_another_key_type() {
        let written = written(KeyType::String).await;
        written
            .assert_rebuilt_then_reused(
                KeyType::Binary,
                "string file read as binary",
                Some(Stale::KeyType {
                    stored: KeyType::String,
                }),
            )
            .await;
        written
            .assert_rebuilt_then_reused(
                KeyType::String,
                "and back",
                Some(Stale::KeyType {
                    stored: KeyType::Binary,
                }),
            )
            .await;
    }

    #[tokio::test]
    async fn test_disk_tier_serves_through_a_write_torn_before_its_rename() {
        let key_type = KeyType::String;
        let written = written(key_type).await;
        let bytes = std::fs::read(&written.path).unwrap();
        let leftover = local_testing::stale_temp(&written.path);
        std::fs::write(&leftover, &bytes[..bytes.len() / 2]).unwrap();
        std::fs::remove_file(&written.path).unwrap();
        written
            .assert_rebuilt_then_reused(key_type, "temporary file left without its rename", None)
            .await;
        let identity = identity_of(&written.opened.handle);
        assert_eq!(
            written
                .local
                .retain(&std::collections::HashSet::from([(
                    identity,
                    ChunkRules::KeyValue
                )]))
                .unwrap(),
            approx::count(bytes.len() / 2),
            "the leftover is swept, the chunk file kept"
        );
    }

    #[tokio::test]
    async fn test_disk_tier_without_a_local_dir_is_a_config_violation_at_the_footer() {
        let opened = store(&Fixture::contract(KeyType::Long, FEW_ROWS)).await;
        let error = opened
            .build(KeyType::Long, Residency::Disk, None)
            .await
            .unwrap_err();
        assert!(
            matches!(
                error,
                Error::Config(ConfigError::Invalid(
                    ConfigViolation::DiskResidencyWithoutLocalDir { .. }
                ))
            ),
            "{error:?}"
        );
        assert_eq!(opened.stats().gets(), 1, "no chunk fetched");
    }

    /// The fixture's key column.
    const KEY_LEAF: usize = 0;

    /// How a `verify` ended, as two decoders' ends are compared: what it
    /// established, a refusal, a file fault, or the kind of any other error.
    #[derive(Debug, PartialEq)]
    enum Outcome {
        Verified(Verified),
        Refused(RefusalReason),
        Fault(FileFault),
        Other(ErrorKind),
    }

    fn outcome(result: Result<Verified>) -> Outcome {
        match result {
            Ok(verified) => Outcome::Verified(verified),
            Err(Error::Refused { reason, .. }) => Outcome::Refused(reason),
            Err(Error::File { fault, .. }) => Outcome::Fault(fault),
            Err(error) => Outcome::Other(error.kind()),
        }
    }

    /// The key column of `metadata` over `chunks` verified by each decoder:
    /// the outcome, which the plain decoder reaches exactly as the column
    /// reader does, and the decoder that read each row group when the
    /// plain one is asked for. A row group whose decode fails was read by
    /// the column reader, since the plain decoder hands every page it
    /// cannot read exactly to it.
    fn verify_by_both(
        metadata: &ParquetMetaData,
        shape: KeyShape,
        chunks: &Arc<Segments>,
    ) -> (Outcome, Vec<KeyDecoder>) {
        let path = location();
        let column = Column {
            metadata,
            leaf: KEY_LEAF,
            path: &path,
        };
        let plain = outcome(verify(
            TABLE,
            &column,
            shape,
            Rules::KeyValue(KeyDecoder::Plain),
            chunks,
        ));
        let reader = outcome(verify(
            TABLE,
            &column,
            shape,
            Rules::KeyValue(KeyDecoder::ColumnReader),
            chunks,
        ));
        assert_eq!(plain, reader, "the plain decoder against the column reader");
        let taken = (0..metadata.num_row_groups())
            .map(|row_group| {
                check_run(column.group(row_group), shape, chunks, KeyDecoder::Plain)
                    .map_or(KeyDecoder::ColumnReader, |(_, decoder)| decoder)
            })
            .collect();
        (plain, taken)
    }

    /// `verify_by_both` over the fixture file behind `opened`.
    async fn verified_by_both(opened: &Opened, shape: KeyShape) -> (Outcome, Vec<KeyDecoder>) {
        let loaded = opened.handle.load_metadata().await.unwrap();
        let chunks = key_chunks(opened, &loaded, KEY_LEAF).await;
        verify_by_both(&loaded, shape, &chunks)
    }

    /// `verified_by_both` of the string keys of `file`.
    async fn file_by_both(file: Bytes) -> (Outcome, Vec<KeyDecoder>) {
        verified_by_both(&store_file(file).await, KeyShape::Str).await
    }

    /// `verified_by_both` of `fixture` read as `shape`.
    async fn fixture_by_both(fixture: &Fixture, shape: KeyShape) -> (Outcome, Vec<KeyDecoder>) {
        verified_by_both(&store(fixture).await, shape).await
    }

    /// The plain decoder for every row group of the contract layout's
    /// string and binary tables: a regression to the column reader is
    /// caught here rather than in a profile.
    #[tokio::test]
    async fn test_check_run_reads_the_contract_string_and_binary_tables_plain() {
        for (key_type, shape) in [
            (KeyType::String, KeyShape::Str),
            (KeyType::Binary, KeyShape::Bin),
        ] {
            let fixture = Fixture::contract(key_type, contract_rows());
            let (outcome, taken) = fixture_by_both(&fixture, shape).await;
            assert!(matches!(outcome, Outcome::Verified(_)), "{outcome:?}");
            assert_eq!(taken, [KeyDecoder::Plain; ROW_GROUPS], "{key_type}");
        }
    }

    /// A layout edit a fixture is written under, named for the messages,
    /// and whether it leaves a string or binary key column one the plain
    /// decoder reads: `PLAIN` pages and no dictionary.
    type Edit = (&'static str, fn(&mut Fixture), bool);

    /// The layouts the plain decoder is held to the column reader on: the
    /// contract's, and every writer knob the index tests turn, alone and in
    /// the combinations that change where the values start in a page. A v2
    /// writer encodes byte arrays by deltas unless told `PLAIN`.
    const EDITS: &[Edit] = &[
        ("contract", |_| {}, true),
        (
            "one row group",
            |fixture| fixture.layout.row_group_rows = contract_rows(),
            true,
        ),
        (
            "pages cut by bytes",
            |fixture| {
                fixture.layout.page_bytes = SMALL_PAGE_BYTES;
                fixture.layout.write_batch = SMALL_BATCH;
            },
            true,
        ),
        (
            "no statistics",
            |fixture| fixture.layout.statistics = EnabledStatistics::None,
            true,
        ),
        (
            "dictionary",
            |fixture| fixture.layout.dictionary = Dictionary::Key,
            false,
        ),
        (
            "dictionary, nullable, v2",
            |fixture| {
                fixture.layout.dictionary = Dictionary::Key;
                fixture.nullable_key = true;
                fixture.layout.writer_version = WriterVersion::PARQUET_2_0;
            },
            false,
        ),
        ("nullable", |fixture| fixture.nullable_key = true, true),
        (
            "v2",
            |fixture| fixture.layout.writer_version = WriterVersion::PARQUET_2_0,
            false,
        ),
        (
            "v2 plain",
            |fixture| {
                fixture.layout.writer_version = WriterVersion::PARQUET_2_0;
                fixture.layout.key_encoding = Some(Encoding::PLAIN);
            },
            true,
        ),
        (
            "nullable, v2 plain",
            |fixture| {
                fixture.nullable_key = true;
                fixture.layout.writer_version = WriterVersion::PARQUET_2_0;
                fixture.layout.key_encoding = Some(Encoding::PLAIN);
            },
            true,
        ),
        (
            "uncompressed",
            |fixture| fixture.layout.compression = Compression::UNCOMPRESSED,
            true,
        ),
        (
            "uncompressed, nullable",
            |fixture| {
                fixture.layout.compression = Compression::UNCOMPRESSED;
                fixture.nullable_key = true;
            },
            true,
        ),
        (
            "uncompressed, nullable, v2 plain",
            |fixture| {
                fixture.layout.compression = Compression::UNCOMPRESSED;
                fixture.nullable_key = true;
                fixture.layout.writer_version = WriterVersion::PARQUET_2_0;
                fixture.layout.key_encoding = Some(Encoding::PLAIN);
            },
            true,
        ),
        (
            "snappy",
            |fixture| fixture.layout.compression = Compression::SNAPPY,
            true,
        ),
        (
            "snappy, nullable, v2 plain",
            |fixture| {
                fixture.layout.compression = Compression::SNAPPY;
                fixture.nullable_key = true;
                fixture.layout.writer_version = WriterVersion::PARQUET_2_0;
                fixture.layout.key_encoding = Some(Encoding::PLAIN);
            },
            true,
        ),
    ];

    /// A page byte bound under a key page of the contract's rows of any
    /// key type, so pages end by bytes at uneven row counts.
    const SMALL_PAGE_BYTES: usize = 1024;

    /// Every key type and every layout of `EDITS`: the plain decoder
    /// reaches what the column reader reaches, and reads every row group of
    /// a string or binary key column written without a dictionary.
    #[tokio::test]
    async fn test_verify_plain_decoder_matches_the_column_reader_on_every_fixture_shape() {
        let fixed_keys: Vec<Option<Key>> = (0..contract_rows())
            .map(|i| Some(Key::Bytes(count(stepped(i)).to_be_bytes().to_vec())))
            .collect();
        let fixtures: [(Fixture, KeyShape); 5] = [
            (
                Fixture::contract(KeyType::String, contract_rows()),
                KeyShape::Str,
            ),
            (
                Fixture::contract(KeyType::Binary, contract_rows()),
                KeyShape::Bin,
            ),
            (
                Fixture::contract(KeyType::Long, contract_rows()),
                KeyShape::Long,
            ),
            (
                Fixture::contract(KeyType::Uuid, contract_rows()),
                KeyShape::Uuid,
            ),
            (
                Fixture::with_keys(KeyColumn::FixedBinary(FIXED_KEY_BYTES), fixed_keys),
                KeyShape::FixedBin,
            ),
        ];
        let expected_compared = fixtures.len() * EDITS.len();
        let mut compared = 0;
        for (mut fixture, shape) in fixtures {
            let as_written = fixture.layout;
            for (label, edit, plain_pages) in EDITS {
                fixture.layout = as_written;
                fixture.nullable_key = false;
                edit(&mut fixture);
                let (outcome, taken) = fixture_by_both(&fixture, shape).await;
                assert!(
                    matches!(outcome, Outcome::Verified(_)),
                    "{shape:?} {label}: {outcome:?}"
                );
                let expected = if *plain_pages && matches!(shape, KeyShape::Str | KeyShape::Bin) {
                    KeyDecoder::Plain
                } else {
                    KeyDecoder::ColumnReader
                };
                assert!(
                    taken.iter().all(|decoder| *decoder == expected),
                    "{shape:?} {label}: {taken:?}"
                );
                compared += 1;
            }
        }
        assert_eq!(compared, expected_compared);
    }

    /// Keys a search probes beside `key`: one just above it, which the
    /// fixtures hold for long keys and for no other type.
    fn just_above(key: &Key) -> Key {
        match key {
            Key::Long(n) => Key::Long(n.saturating_add(1)),
            Key::Str(s) => Key::Str(format!("{s}\0")),
            Key::Bytes(b) => Key::Bytes([b.as_slice(), &[0]].concat()),
            Key::Uuid(u) => {
                let mut above = *u;
                above[UUID_BYTES - 1] = above[UUID_BYTES - 1].wrapping_add(1);
                Key::Uuid(above)
            }
        }
    }

    /// Every `PROBE_STRIDE`th key and the first and last keys of every key
    /// page, of every key type under every layout of `EDITS`, and a key
    /// just above each, searched for in its key page, and the first and
    /// last keys of each page in its row group whole: `find_in_run` answers
    /// what the column reader's decode and binary search answer, and reads
    /// every page of a string or binary key column written without a
    /// dictionary plain.
    #[tokio::test]
    async fn test_find_in_run_matches_the_column_reader_on_every_fixture_shape() {
        let fixed_keys: Vec<Option<Key>> = (0..contract_rows())
            .map(|i| Some(Key::Bytes(count(stepped(i)).to_be_bytes().to_vec())))
            .collect();
        let fixtures: [(Fixture, KeyShape); 5] = [
            (
                Fixture::contract(KeyType::String, contract_rows()),
                KeyShape::Str,
            ),
            (
                Fixture::contract(KeyType::Binary, contract_rows()),
                KeyShape::Bin,
            ),
            (
                Fixture::contract(KeyType::Long, contract_rows()),
                KeyShape::Long,
            ),
            (
                Fixture::contract(KeyType::Uuid, contract_rows()),
                KeyShape::Uuid,
            ),
            (
                Fixture::with_keys(KeyColumn::FixedBinary(FIXED_KEY_BYTES), fixed_keys),
                KeyShape::FixedBin,
            ),
        ];
        let path = location();
        let mut found_plain = 0;
        for (mut fixture, shape) in fixtures {
            let as_written = fixture.layout;
            for (label, edit, plain_pages) in EDITS {
                fixture.layout = as_written;
                fixture.nullable_key = false;
                edit(&mut fixture);
                let opened = store(&fixture).await;
                let metadata = opened.handle.load_metadata().await.unwrap();
                let chunks = key_chunks(&opened, &metadata, KEY_LEAF).await;
                let column = Column {
                    metadata: &metadata,
                    leaf: KEY_LEAF,
                    path: &path,
                };
                let expected_decoder =
                    if *plain_pages && matches!(shape, KeyShape::Str | KeyShape::Bin) {
                        KeyDecoder::Plain
                    } else {
                        KeyDecoder::ColumnReader
                    };
                for row_group in 0..metadata.num_row_groups() {
                    let chunk = column.group(row_group);
                    let whole = 0..chunk.rows();
                    let group_keys =
                        decode_run(chunk, shape.run_shape(), whole.clone(), Arc::clone(&chunks))
                            .unwrap();
                    for page in 0..chunk.page_locations().len() {
                        let rows = chunk.page_rows(page);
                        let keys =
                            decode_run(chunk, shape.run_shape(), rows.clone(), Arc::clone(&chunks))
                                .unwrap();
                        assert!(!keys.is_empty(), "{shape:?} {label}: {rows:?}");
                        let ends = [0, keys.len() - 1];
                        let sampled = (0..keys.len()).step_by(PROBE_STRIDE).chain(ends);
                        let held = sampled.map(|at| (rows.clone(), &keys, at));
                        let in_group = ends.into_iter().map(|at| {
                            let in_group = row_index(rows.start) + at;
                            (whole.clone(), &group_keys, in_group)
                        });
                        for (searched, run, at) in held.chain(in_group) {
                            let key = run.get(at);
                            for probe in [key.clone(), just_above(&key)] {
                                let case = format!("{shape:?} {label}: {searched:?} {probe:?}");
                                let (found, decoder) = find_in_run(
                                    chunk,
                                    shape,
                                    searched.clone(),
                                    &probe,
                                    Arc::clone(&chunks),
                                )
                                .unwrap();
                                assert_eq!(found, run.binary_search(&probe).ok(), "{case}");
                                assert_eq!(decoder, expected_decoder, "{case}");
                                if probe == key {
                                    assert_eq!(found, Some(at), "{case}");
                                }
                                if decoder == KeyDecoder::Plain && found.is_some() {
                                    found_plain += 1;
                                }
                            }
                        }
                    }
                }
            }
        }
        assert!(found_plain > 0, "some key was found plain");
    }

    /// `FEW_ROWS` string keys from `key_for`, with `edit` applied.
    fn few_strings(edit: impl FnOnce(&mut Vec<Option<Key>>)) -> Fixture {
        let mut keys = (0..FEW_ROWS)
            .map(|i| Some(key_for(i, KeyType::String)))
            .collect();
        edit(&mut keys);
        Fixture::with_keys(KeyColumn::Utf8, keys)
    }

    /// The refusals a row group's keys draw are drawn through the plain
    /// decoder as through the column reader: a null, which the plain
    /// decoder leaves to the column reader since the page's levels are not
    /// all present, and, read plain, a key that is not UTF-8, a key out of
    /// order, a duplicate, and a key over the cap.
    #[tokio::test]
    async fn test_verify_plain_decoder_refuses_what_the_column_reader_refuses() {
        let path = location();
        let at = FEW_ROWS / 2;
        let null = few_strings(|keys| keys[at] = None);
        assert_eq!(
            fixture_by_both(&null, KeyShape::Str).await,
            (
                Outcome::Refused(RefusalReason::NullKeys {
                    path: path.clone(),
                    row_group: 0
                }),
                vec![KeyDecoder::ColumnReader]
            )
        );

        let not_utf8 = Fixture::with_keys(
            KeyColumn::Binary,
            (0..FEW_ROWS)
                .map(|i| Some(key_for(i, KeyType::Binary)))
                .collect(),
        );
        assert_eq!(
            fixture_by_both(&not_utf8, KeyShape::Str).await,
            (
                Outcome::Fault(FileFault::KeyNotUtf8 { row_group: 0 }),
                vec![KeyDecoder::Plain]
            )
        );

        let unsorted = few_strings(|keys| keys.swap(at, at + 1));
        assert_eq!(
            fixture_by_both(&unsorted, KeyShape::Str).await,
            (
                Outcome::Refused(RefusalReason::Unsorted {
                    path: path.clone(),
                    row_group: 0
                }),
                vec![KeyDecoder::Plain]
            )
        );

        let duplicate = few_strings(|keys| keys[at + 1] = keys[at].clone());
        assert_eq!(
            fixture_by_both(&duplicate, KeyShape::Str).await,
            (
                Outcome::Refused(RefusalReason::DuplicateKeys {
                    path: path.clone(),
                    key: key_for(at, KeyType::String)
                }),
                vec![KeyDecoder::Plain]
            )
        );

        let over = Fixture::with_keys(KeyColumn::Utf8, keys_of_len(MAX_KEY_BYTES + 1));
        assert_eq!(
            fixture_by_both(&over, KeyShape::Str).await,
            (
                Outcome::Refused(RefusalReason::KeyTooLong {
                    path,
                    bytes: MAX_KEY_BYTES + 1
                }),
                vec![KeyDecoder::Plain]
            )
        );
    }

    /// A duplicate whose copies straddle a page boundary, a decode batch
    /// (`DECODE_BATCH_RECORDS`) inside one page, or a row group boundary is
    /// refused through the plain decoder, and so is a key that is not UTF-8
    /// in a page's second batch; the same row groups without the defect
    /// verify alike through both decoders, fences and all.
    #[tokio::test]
    async fn test_verify_plain_decoder_checks_across_pages_batches_and_row_groups() {
        let batch = row_index(DECODE_BATCH_RECORDS);
        let rows = 2 * batch;
        let written_as = |column: KeyColumn, edit: &dyn Fn(&mut Vec<Option<Key>>)| {
            let mut keys: Vec<Option<Key>> = (0..rows)
                .map(|i| Some(key_for(i, KeyType::String)))
                .collect();
            edit(&mut keys);
            let mut fixture = Fixture::with_keys(column, keys);
            fixture.layout.row_group_rows = rows;
            fixture
        };
        let strings = |edit: &dyn Fn(&mut Vec<Option<Key>>)| written_as(KeyColumn::Utf8, edit);
        let one_page = |mut fixture: Fixture| {
            fixture.layout.page_rows = rows;
            fixture
        };
        let duplicate_at =
            |at: usize| move |keys: &mut Vec<Option<Key>>| keys[at] = keys[at - 1].clone();
        let refused_at = |at: usize| {
            Outcome::Refused(RefusalReason::DuplicateKeys {
                path: location(),
                key: key_for(at - 1, KeyType::String),
            })
        };

        let (clean, taken) = fixture_by_both(&strings(&|_| {}), KeyShape::Str).await;
        let Outcome::Verified(clean) = clean else {
            panic!("{clean:?}");
        };
        let fences: Vec<Key> = (0..rows)
            .step_by(page_rows())
            .map(|i| key_for(i, KeyType::String))
            .collect();
        assert_eq!(clean.fences, [fences]);
        assert_eq!(taken, [KeyDecoder::Plain]);
        let (clean, taken) = fixture_by_both(&one_page(strings(&|_| {})), KeyShape::Str).await;
        let Outcome::Verified(clean) = clean else {
            panic!("{clean:?}");
        };
        assert_eq!(clean.fences, [vec![key_for(0, KeyType::String)]]);
        assert_eq!(taken, [KeyDecoder::Plain]);

        let across_pages = strings(&duplicate_at(page_rows()));
        assert_eq!(
            fixture_by_both(&across_pages, KeyShape::Str).await,
            (refused_at(page_rows()), vec![KeyDecoder::Plain])
        );
        let across_batches = one_page(strings(&duplicate_at(batch)));
        assert_eq!(
            fixture_by_both(&across_batches, KeyShape::Str).await,
            (refused_at(batch), vec![KeyDecoder::Plain])
        );
        let not_utf8_at = batch + 1;
        let late = one_page(written_as(KeyColumn::Binary, &|keys| {
            let mut bytes = key_for(not_utf8_at, KeyType::String).as_bytes().to_vec();
            // Above the key itself and below the next: order holds.
            bytes.push(u8::MAX);
            keys[not_utf8_at] = Some(Key::Bytes(bytes));
        }));
        assert_eq!(
            fixture_by_both(&late, KeyShape::Str).await,
            (
                Outcome::Fault(FileFault::KeyNotUtf8 { row_group: 0 }),
                vec![KeyDecoder::Plain]
            )
        );

        let mut across_groups =
            few_strings(|keys| keys[TINY_GROUP_ROWS] = keys[TINY_GROUP_ROWS - 1].clone());
        across_groups.layout.row_group_rows = TINY_GROUP_ROWS;
        let (outcome, taken) = fixture_by_both(&across_groups, KeyShape::Str).await;
        assert_eq!(outcome, refused_at(TINY_GROUP_ROWS));
        assert!(
            taken.iter().all(|decoder| *decoder == KeyDecoder::Plain),
            "{taken:?}"
        );
    }

    /// A string key page whose rows contradict its offset index, however the
    /// footer contradicts them, is left by the plain decoder to the column
    /// reader, and refused as it refuses it.
    #[tokio::test]
    async fn test_verify_plain_decoder_leaves_a_page_its_offset_index_contradicts_to_the_column_reader()
     {
        let opened = store(&Fixture::contract(KeyType::String, contract_rows())).await;
        let loaded = opened.handle.load_metadata().await.unwrap();
        let chunks = key_chunks(&opened, &loaded, KEY_LEAF).await;
        let mismatch = |page| Outcome::Fault(FileFault::PageRowsMismatch { row_group: 0, page });

        let short_group = with_group_rows(&loaded, 0, PAGE_ROWS + PAGE_ROWS / 2);
        let (outcome, taken) = verify_by_both(&short_group, KeyShape::Str, &chunks);
        assert_eq!(outcome, mismatch(1));
        assert_eq!(taken[0], KeyDecoder::ColumnReader);

        let mut shifted = pages_of(&loaded, KEY_LEAF);
        shifted[1].first_row_index -= 1;
        let shifted = with_pages(&loaded, KEY_LEAF, shifted);
        let (outcome, taken) = verify_by_both(&shifted, KeyShape::Str, &chunks);
        assert_eq!(outcome, mismatch(0));
        assert_eq!(taken[0], KeyDecoder::ColumnReader);

        let inflated = with_group_rows(&loaded, 0, UNALLOCATABLE_ROWS);
        let (outcome, taken) = verify_by_both(&inflated, KeyShape::Str, &chunks);
        assert_eq!(outcome, mismatch(PAGES_PER_GROUP - 1));
        assert_eq!(taken[0], KeyDecoder::ColumnReader);
    }

    /// The offset, within `file`, of the length prefix of every value of
    /// row group 0's first key page, a page stored uncompressed whose body
    /// is its values alone, and the lengths those prefixes hold.
    fn value_prefixes(file: &[u8], metadata: &ParquetMetaData) -> Vec<(usize, u32)> {
        let page = &pages_of(metadata, KEY_LEAF)[0];
        let start = usize::try_from(page.offset).unwrap();
        let header = PageHeader::read(&file[start..]).unwrap();
        let PageBody::Data { num_values } = header.body else {
            panic!("{header:?} is not a v1 data page");
        };
        let mut at = start + header.header_len;
        (0..num_values)
            .map(|_| {
                let len = u32::from_le_bytes(
                    file[at..at + BYTE_ARRAY_LENGTH_PREFIX_BYTES]
                        .try_into()
                        .unwrap(),
                );
                let prefix = (at, len);
                at += BYTE_ARRAY_LENGTH_PREFIX_BYTES + usize::try_from(len).unwrap();
                prefix
            })
            .collect()
    }

    /// `file` with the length prefix at `at` holding `len`.
    fn with_prefix(file: &Bytes, at: usize, len: u32) -> Bytes {
        let mut patched = file.to_vec();
        patched[at..at + BYTE_ARRAY_LENGTH_PREFIX_BYTES].copy_from_slice(&len.to_le_bytes());
        Bytes::from(patched)
    }

    /// A page whose values do not fill its bytes exactly is left by the
    /// plain decoder to the column reader, which decides it: a length
    /// prefix running past the page's end is a parquet error,
    /// a length prefix the page's end cuts short a panic, and a byte left
    /// after the last value is read past, so the page verifies with the last
    /// value it read.
    #[tokio::test]
    async fn test_verify_plain_decoder_leaves_a_page_it_cannot_read_exactly_to_the_column_reader() {
        let mut fixture = few_strings(|keys| {
            let Some(Key::Str(last)) = keys.last_mut().unwrap() else {
                panic!("a string fixture holds string keys");
            };
            // Cut off by a prefix one byte shorter, the key is the one
            // `key_for` gives, still above the key before it.
            last.push('z');
        });
        fixture.layout.compression = Compression::UNCOMPRESSED;
        let file = fixture.write();
        let metadata = ParquetMetaDataReader::new()
            .with_page_index_policy(PageIndexPolicy::Required)
            .parse_and_finish(&file)
            .unwrap();
        let prefixes = value_prefixes(&file, &metadata);
        let (last_at, last_len) = prefixes[FEW_ROWS - 1];
        let (before_at, before_len) = prefixes[FEW_ROWS - 2];
        let (outcome, taken) = file_by_both(file.clone()).await;
        assert!(matches!(outcome, Outcome::Verified(_)), "{outcome:?}");
        assert_eq!(taken, [KeyDecoder::Plain], "the file as written");

        let past_end = with_prefix(&file, last_at, last_len + 1);
        assert_eq!(
            file_by_both(past_end).await,
            (
                Outcome::Other(ErrorKind::Parquet),
                vec![KeyDecoder::ColumnReader]
            )
        );

        // The value before the last grown over all but two bytes of the
        // last value's prefix, which the page's end then cuts short.
        let prefix_bytes_left = 2;
        let grown =
            u32::try_from(BYTE_ARRAY_LENGTH_PREFIX_BYTES).unwrap() + last_len - prefix_bytes_left;
        let (outcome, taken) =
            file_by_both(with_prefix(&file, before_at, before_len + grown)).await;
        assert!(
            matches!(
                outcome,
                Outcome::Fault(FileFault::DecoderPanicked {
                    part: DecodedPart::Pages {
                        row_group: 0,
                        page: Some(ChunkPage::Data(0))
                    },
                    ..
                })
            ),
            "{outcome:?}"
        );
        assert_eq!(taken, [KeyDecoder::ColumnReader]);

        let (outcome, taken) = file_by_both(with_prefix(&file, last_at, last_len - 1)).await;
        let Outcome::Verified(verified) = outcome else {
            panic!("{outcome:?}");
        };
        assert_eq!(verified.key_range.1, key_for(FEW_ROWS - 1, KeyType::String));
        assert_eq!(taken, [KeyDecoder::ColumnReader]);
    }

    /// The refusal a build of `opened` under a `long` order column ends in,
    /// the same in every tier.
    async fn ordered_refusal(opened: &Opened) -> RefusalReason {
        let mut reasons = Vec::new();
        for residency in Residency::ALL {
            let dir = tempfile::tempdir().unwrap();
            reasons.push(refusal_of(
                opened
                    .build_ordered_in(OrderType::Long, residency, dir.path())
                    .await,
            ));
        }
        assert!(
            reasons.windows(2).all(|pair| pair[0] == pair[1]),
            "{reasons:?}"
        );
        reasons.pop().unwrap()
    }

    /// Under ordered rules a key repeats, its rows rising by order value
    /// (RFD 025, Layout), in every tier; the same file under key-value
    /// rules is still `duplicate_keys`.
    #[tokio::test]
    async fn test_verify_ordered_admits_a_key_repeated_with_rising_order() {
        let opened = store(&Fixture::of_long_pairs(&[(1, 1), (1, 2), (1, 3), (2, 1)])).await;
        for residency in Residency::ALL {
            let dir = tempfile::tempdir().unwrap();
            let index = opened
                .build_ordered_in(OrderType::Long, residency, dir.path())
                .await
                .unwrap();
            assert_eq!(index.key_range(), (&Key::Long(1), &Key::Long(2)));
            assert_eq!(index.fences(), [vec![Key::Long(1)]]);
            assert!(index.order_facts().is_some(), "{residency}");
        }
        assert_eq!(
            refusal_of(opened.build(KeyType::Long, Residency::Full, None).await),
            RefusalReason::DuplicateKeys {
                path: location(),
                key: Key::Long(1),
            }
        );
    }

    /// A key and order value together twice is `duplicate_rows`, naming
    /// both (RFD 025, Uniqueness).
    #[tokio::test]
    async fn test_verify_ordered_refuses_an_equal_pair_as_duplicate_rows() {
        let opened = store(&Fixture::of_long_pairs(&[(1, 2), (1, 2)])).await;
        assert_eq!(
            ordered_refusal(&opened).await,
            RefusalReason::DuplicateRows {
                path: location(),
                key: Key::Long(1),
                order: Box::new(OrderValue::Long(2)),
            }
        );
    }

    /// A key's order value falling is `unsorted`, as a key falling is.
    #[tokio::test]
    async fn test_verify_ordered_refuses_a_falling_order_as_unsorted() {
        let falling = store(&Fixture::of_long_pairs(&[(1, 3), (1, 2)])).await;
        assert_eq!(
            ordered_refusal(&falling).await,
            RefusalReason::Unsorted {
                path: location(),
                row_group: 0,
            }
        );
        let key_falling = store(&Fixture::of_long_pairs(&[(2, 1), (1, 2)])).await;
        assert_eq!(
            ordered_refusal(&key_falling).await,
            RefusalReason::Unsorted {
                path: location(),
                row_group: 0,
            }
        );
    }

    /// Pairs are compared across a row-group boundary, where a key's run
    /// may continue: an equal pair across it is `duplicate_rows`, a fall
    /// `unsorted` in the later row group, and a rise is served.
    #[tokio::test]
    async fn test_verify_ordered_checks_pairs_across_row_groups() {
        let in_groups = |pairs: &[(i64, i64)]| {
            let mut fixture = Fixture::of_long_pairs(pairs);
            fixture.layout.row_group_rows = TINY_GROUP_ROWS;
            fixture
        };
        let equal = store(&in_groups(&[(1, 4), (1, 5), (1, 5), (2, 1)])).await;
        assert_eq!(
            equal.handle.load_metadata().await.unwrap().num_row_groups(),
            2
        );
        assert_eq!(
            ordered_refusal(&equal).await,
            RefusalReason::DuplicateRows {
                path: location(),
                key: Key::Long(1),
                order: Box::new(OrderValue::Long(5)),
            }
        );
        let falling = store(&in_groups(&[(1, 4), (1, 5), (1, 3), (2, 1)])).await;
        assert_eq!(
            ordered_refusal(&falling).await,
            RefusalReason::Unsorted {
                path: location(),
                row_group: 1,
            }
        );
        let rising = store(&in_groups(&[(1, 4), (1, 5), (1, 6), (2, 1)])).await;
        let dir = tempfile::tempdir().unwrap();
        let index = rising
            .build_ordered_in(OrderType::Long, Residency::Full, dir.path())
            .await
            .unwrap();
        assert_eq!(index.fences(), [vec![Key::Long(1)], vec![Key::Long(1)]]);
    }

    /// A null order value is `null_order`, in the row group holding it.
    #[tokio::test]
    async fn test_verify_ordered_refuses_null_order() {
        let mut fixture = Fixture::of_long_pairs(&[(1, 1), (2, 1), (3, 1), (4, 1)]);
        fixture.layout.row_group_rows = TINY_GROUP_ROWS;
        fixture.orders = Some(Orders::Long(vec![Some(1), Some(1), Some(1), None]));
        let opened = store(&fixture).await;
        assert_eq!(
            ordered_refusal(&opened).await,
            RefusalReason::NullOrder {
                path: location(),
                row_group: 1,
            }
        );
    }

    /// A `string` order value that is not UTF-8 is a file fault, as a
    /// string key that is not is.
    #[tokio::test]
    async fn test_verify_ordered_refuses_a_string_order_that_is_not_utf8() {
        let mut fixture = Fixture::of_long_pairs(&[(1, 0), (2, 0)]);
        fixture.orders = Some(Orders::Bytes(vec![
            Some(b"a".to_vec()),
            Some(vec![u8::MAX]),
        ]));
        let opened = store(&fixture).await;
        let dir = tempfile::tempdir().unwrap();
        assert_eq!(
            fault_of(
                opened
                    .build_ordered_in(OrderType::String, Residency::Full, dir.path())
                    .await
            ),
            FileFault::OrderNotUtf8 { row_group: 0 }
        );
        let binary = opened
            .build_ordered_in(OrderType::Binary, Residency::Full, dir.path())
            .await
            .unwrap();
        assert_eq!(
            binary.order_facts().unwrap().max,
            OrderValue::Bytes(vec![u8::MAX])
        );
    }

    /// `verify` under the ordered rules of `opened`'s file over `metadata`,
    /// a footer edited from its own, with the key and order chunks fetched.
    async fn verify_ordered_over(
        opened: &Opened,
        metadata: &ParquetMetaData,
        order_type: OrderType,
    ) -> Result<Verified> {
        let layout = ordered_layout(KeyType::Long, order_type);
        let inspected = FileIndex::inspect(TABLE, Arc::clone(&opened.handle), &layout)
            .await
            .unwrap();
        let order = inspected.layout.order.unwrap();
        let ranges = inspected
            .layout
            .verified_ranges(metadata, opened.handle.size());
        let parts = fetch_chunk_parts(&opened.handle, &ranges).await.unwrap();
        let chunks = Arc::new(Segments::new(parts, opened.handle.size()));
        let path = location();
        let column = Column {
            metadata,
            leaf: inspected.layout.key_leaf,
            path: &path,
        };
        verify(
            TABLE,
            &column,
            KeyShape::Long,
            Rules::Ordered(order),
            &chunks,
        )
    }

    /// Verification never reads the order column's column index (RFD 025,
    /// Load): with the key column's index stood in for it, an index that
    /// bounds no order page, a file verifies to the same order facts and
    /// fences as under its own index; and a byte-array index the writer
    /// truncated leaves the facts exact.
    #[tokio::test]
    async fn test_verify_ordered_ignores_the_order_column_index() {
        let opened = store(&Fixture::of_long_pairs(&[(1, 100), (1, 200), (2, 100)])).await;
        let loaded = opened.handle.load_metadata().await.unwrap();
        let schema = loaded.file_metadata().schema_descr();
        let leaf_of = |name: &str| {
            (0..schema.num_columns())
                .find(|&leaf| schema.column(leaf).name() == name)
                .unwrap()
        };
        let (key_leaf, order_leaf) = (leaf_of(KEY_COLUMN), leaf_of(ORDER_COLUMN));
        let own = verify_ordered_over(&opened, &loaded, OrderType::Long)
            .await
            .unwrap();
        let mut builder = ParquetMetaData::clone(&loaded).into_builder();
        let mut column_index = builder.take_column_index().unwrap();
        column_index[0][order_leaf] = column_index[0][key_leaf].clone();
        let key_index = builder.set_column_index(Some(column_index)).build();
        let stood_in = verify_ordered_over(&opened, &key_index, OrderType::Long)
            .await
            .unwrap();
        assert_eq!(stood_in.order, own.order);
        assert!(own.order.is_some());

        let long_text =
            |n: usize| Some(format!("{:a>len$}{n}", "", len = LONG_ORDER_BYTES).into_bytes());
        let mut truncated = Fixture::of_long_pairs(&[(1, 0), (1, 0), (2, 0)]);
        truncated.orders = Some(Orders::Bytes(vec![
            long_text(1),
            long_text(2),
            long_text(1),
        ]));
        truncated.layout.truncate = Some(TRUNCATED_INDEX_BYTES);
        let truncated = store(&truncated).await;
        let loaded = truncated.handle.load_metadata().await.unwrap();
        let ColumnIndexMetaData::BYTE_ARRAY(index) = &loaded.column_index().unwrap()[0][order_leaf]
        else {
            panic!("a byte-array order column has a byte-array index");
        };
        assert!(
            index.min_value(0).unwrap().len() < LONG_ORDER_BYTES,
            "the index is truncated"
        );
        let verified = verify_ordered_over(&truncated, &loaded, OrderType::String)
            .await
            .unwrap();
        assert_eq!(
            verified.order.unwrap().facts.max,
            OrderValue::Str(String::from_utf8(long_text(2).unwrap()).unwrap())
        );
    }

    /// Order pages get the value pages' header checks and their 64 MiB
    /// ceiling at load (RFD 025, Load; RFD 004): an order page whose header
    /// declares more uncompressed bytes than `MOST_VALUE_PAGE_BYTES` is
    /// refused before parquet reserves them, naming the order page.
    #[tokio::test]
    async fn test_verify_ordered_refuses_an_order_page_header_over_the_ceiling() {
        let opened = store(&Fixture::of_long_pairs(&[(1, 1), (1, 2), (2, 1)])).await;
        let loaded = opened.handle.load_metadata().await.unwrap();
        let layout = ordered_layout(KeyType::Long, OrderType::Long);
        let inspected = FileIndex::inspect(TABLE, Arc::clone(&opened.handle), &layout)
            .await
            .unwrap();
        let order = inspected.layout.order.unwrap();
        let order_leaf = inspected.layout.order_leaf().unwrap();
        let page = ChunkPage::Data(0);
        let claimed = i32::try_from(MOST_VALUE_PAGE_BYTES + 1).unwrap();
        let ranges = inspected
            .layout
            .verified_ranges(&loaded, opened.handle.size());
        let mut parts = fetch_chunk_parts(&opened.handle, &ranges).await.unwrap();
        let at = ranges
            .iter()
            .position(|range| {
                *range == column_chunk_ranges(&loaded, order_leaf, opened.handle.size())[0]
            })
            .unwrap();
        let (range, bytes) = &parts[at];
        let patched = with_header_claims(
            bytes,
            header_at(&loaded, order_leaf, page),
            &[(HeaderField::UncompressedPageSize, claimed)],
        );
        let grown = i64::try_from(patched.len() - bytes.len()).unwrap();
        let start = range.start;
        parts[at] = (start..start + count(patched.len()), Bytes::from(patched));
        let metadata = moved_by_header(&loaded, order_leaf, page, grown);
        let chunks = Arc::new(Segments::new(parts, opened.handle.size()));
        let path = location();
        let column = Column {
            metadata: &metadata,
            leaf: inspected.layout.key_leaf,
            path: &path,
        };
        assert_eq!(
            fault_of(verify(
                TABLE,
                &column,
                KeyShape::Long,
                Rules::Ordered(order),
                &chunks
            )),
            over_bound(
                page,
                HeaderField::UncompressedPageSize,
                claimed,
                MOST_VALUE_PAGE_BYTES
            )
        );
    }

    /// The bytes of the long string order values of the truncation case,
    /// past the column index's truncation, `TRUNCATED_INDEX_BYTES`.
    const LONG_ORDER_BYTES: usize = 32;
    const TRUNCATED_INDEX_BYTES: usize = 8;

    /// The load records per file the exact least and greatest order value,
    /// each row group's distinct keys, and the longest run, a run across a
    /// row-group boundary counted whole (RFD 025, Load), in every tier and
    /// from a reused Disk-tier chunk file alike.
    #[tokio::test]
    async fn test_verify_ordered_records_min_max_distinct_keys_and_longest_run() {
        let mut fixture =
            Fixture::of_long_pairs(&[(1, 5), (1, 6), (1, 7), (1, 8), (2, -3), (2, 9), (3, 0)]);
        fixture.layout.row_group_rows = 3;
        let opened = store(&fixture).await;
        let expected = OrderFacts {
            min: OrderValue::Long(-3),
            max: OrderValue::Long(9),
            distinct_keys: vec![1, 2, 1],
            longest_run: 4,
        };
        for residency in Residency::ALL {
            let dir = tempfile::tempdir().unwrap();
            let index = opened
                .build_ordered_in(OrderType::Long, residency, dir.path())
                .await
                .unwrap();
            assert_eq!(index.order_facts(), Some(&expected), "{residency}");
            assert_eq!(
                count(index.resident_bytes()),
                predicted_of(&index).in_tier(residency).unwrap(),
                "{residency}"
            );
        }
        let dir = tempfile::tempdir().unwrap();
        opened
            .build_ordered_in(OrderType::Long, Residency::Disk, dir.path())
            .await
            .unwrap();
        let restarted = store_file(opened.file.clone()).await;
        let reused = restarted
            .build_ordered_in(OrderType::Long, Residency::Disk, dir.path())
            .await
            .unwrap();
        assert_eq!(restarted.stats().gets(), 1, "the footer only");
        assert_eq!(reused.order_facts(), Some(&expected));
    }

    /// Under `disk` an ordered file's chunk file holds its key and order
    /// chunks, fetched in one round with one GET per row group per column
    /// and written under the ordered rules, beside the key-value file's
    /// chunk file and not in place of it; the residency sample covers both
    /// columns' chunks (RFD 025, Load, Consequences).
    #[tokio::test]
    async fn test_disk_tier_keeps_an_ordered_chunk_file_of_key_and_order_chunks() {
        let pairs: Vec<(i64, i64)> = (0..i64::try_from(contract_rows()).unwrap())
            .map(|i| (i / 2, i % 2))
            .collect();
        let mut fixture = Fixture::of_long_pairs(&pairs);
        fixture.layout.row_group_rows = group_rows();
        let opened = store(&fixture).await;
        let dir = tempfile::tempdir().unwrap();
        let local = testing::local_dir(dir.path(), opened.stats(), None);
        let layout = ordered_layout(KeyType::Long, OrderType::Long);
        let index = opened
            .build_under(&layout, Residency::Disk, Some(&local))
            .await
            .unwrap();
        assert_eq!(opened.stats().gets(), 1 + 2 * count(ROW_GROUPS));
        let disk = index.disk_chunks().unwrap();
        let metadata = index.metadata();
        let key_ranges = column_chunk_ranges(metadata, index.key_leaf(), opened.handle.size());
        let order_ranges =
            column_chunk_ranges(metadata, index.order_leaf().unwrap(), opened.handle.size());
        let all: Vec<Range<u64>> = key_ranges.iter().chain(&order_ranges).cloned().collect();
        assert!(disk.stores_exactly(&all));
        assert_eq!(
            disk.verified_under().rules,
            ChunkRules::Ordered {
                order_field_id: ORDER_FIELD_ID
            }
        );
        let span = |ranges: &[Range<u64>]| -> u64 { ranges.iter().map(|r| r.end - r.start).sum() };
        let chunks = disk.chunks();
        assert_eq!(
            chunks.end - chunks.start,
            span(&key_ranges) + span(&order_ranges)
        );
        assert!(disk.resident().unwrap().pages > 0);
        assert!(
            local.index_bytes_in_use().unwrap() <= predicted_of(&index).disk_bytes,
            "the budget reserves at least the chunk file written"
        );

        opened
            .build(KeyType::Long, Residency::Disk, Some(&local))
            .await
            .unwrap_err();
        assert!(
            local
                .lookup(&identity_of(&opened.handle), index.chunk_rules())
                .map(|found| matches!(found, ChunkFileLookup::Found(_)))
                .unwrap(),
            "a key-value build leaves the ordered chunk file"
        );
    }

    /// Rows per page in the fence fixtures: a few, so a file has many
    /// order pages, at which runs start and continue alike.
    const FENCE_PAGE_ROWS: usize = 5;
    /// Rows per row group in the fence fixtures.
    const FENCE_GROUP_ROWS: usize = 40;
    /// The rows a fence fixture draws: three row groups.
    const FENCE_ROWS: usize = 3 * FENCE_GROUP_ROWS;
    /// The most rows a drawn key's run takes.
    const LONGEST_DRAWN_RUN: usize = 12;
    /// The most a drawn key, or a drawn long order value, steps by.
    const MOST_DRAWN_STEP: i64 = 3;
    /// The bytes every drawn string key and string order value starts
    /// with: adjacent pages then differ only past it.
    const SHARED_PREFIX_BYTES: usize = 64;
    /// The letters a drawn string order value's suffix is spelled in, and
    /// its longest suffix: few, so one suffix is often a prefix of the next.
    const SUFFIX_LETTERS: &[u8] = b"ab";
    const LONGEST_SUFFIX: usize = 3;
    /// The seeds the fence property test draws from, one table each.
    const FENCE_SEEDS: u64 = 12;

    /// The bytes a drawn binary order value's suffix is spelled in, the
    /// least and the greatest byte, and the byte its shared prefix repeats.
    const BINARY_SUFFIX_BYTES: &[u8] = &[0x00, 0xff];
    const BINARY_PREFIX_BYTE: u8 = 0xa5;
    /// The width of a drawn fixed order value: the shared prefix and a
    /// suffix of `LONGEST_SUFFIX` letters.
    const FIXED_ORDER_BYTES: usize = SHARED_PREFIX_BYTES + LONGEST_SUFFIX;

    /// The order column a fence fixture draws.
    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    enum DrawnOrder {
        Long,
        Str,
        Bin,
        Fixed,
    }

    impl DrawnOrder {
        /// The order type a table serves the column as.
        fn order_type(self) -> OrderType {
            match self {
                Self::Long => OrderType::Long,
                Self::Str => OrderType::String,
                Self::Bin => OrderType::Binary,
                Self::Fixed => OrderType::Fixed(u32::try_from(FIXED_ORDER_BYTES).unwrap()),
            }
        }

        /// `run` distinct byte values ascending, fewer when draws repeat:
        /// the shared prefix and a short suffix, so one value is often a
        /// prefix of the next, or of one width for a fixed column.
        fn values(self, rng: &mut StdRng, run: usize) -> Vec<Vec<u8>> {
            let (prefix, letters, lengths) = match self {
                Self::Bin => (BINARY_PREFIX_BYTE, BINARY_SUFFIX_BYTES, 0..=LONGEST_SUFFIX),
                Self::Fixed => (b'p', SUFFIX_LETTERS, LONGEST_SUFFIX..=LONGEST_SUFFIX),
                Self::Long | Self::Str => (b'p', SUFFIX_LETTERS, 0..=LONGEST_SUFFIX),
            };
            let mut values: Vec<Vec<u8>> = (0..run)
                .map(|_| {
                    let len = rng.random_range(lengths.clone());
                    let mut value = vec![prefix; SHARED_PREFIX_BYTES];
                    value.extend((0..len).map(|_| letters[rng.random_range(0..letters.len())]));
                    value
                })
                .collect();
            values.sort();
            values.dedup();
            values
        }
    }

    /// A fence fixture's pairs, sorted by key and then order value: runs
    /// of random lengths, long keys or string keys sharing
    /// `SHARED_PREFIX_BYTES`, and `order` values, byte ones sharing as many.
    fn drawn_pairs(
        rng: &mut StdRng,
        string_keys: bool,
        order: DrawnOrder,
    ) -> Vec<(Key, OrderValue)> {
        let prefix = "p".repeat(SHARED_PREFIX_BYTES);
        let mut pairs = Vec::with_capacity(FENCE_ROWS);
        let mut key = 0;
        while pairs.len() < FENCE_ROWS {
            key += rng.random_range(1..=MOST_DRAWN_STEP);
            let run = rng
                .random_range(1..=LONGEST_DRAWN_RUN)
                .min(FENCE_ROWS - pairs.len());
            let orders: Vec<OrderValue> = match order {
                DrawnOrder::Long => {
                    let mut value = rng.random_range(-MOST_DRAWN_STEP..=MOST_DRAWN_STEP);
                    (0..run)
                        .map(|_| {
                            value += rng.random_range(1..=MOST_DRAWN_STEP);
                            OrderValue::Long(value)
                        })
                        .collect()
                }
                DrawnOrder::Str => order
                    .values(rng, run)
                    .into_iter()
                    .map(|value| OrderValue::Str(String::from_utf8(value).unwrap()))
                    .collect(),
                DrawnOrder::Bin | DrawnOrder::Fixed => order
                    .values(rng, run)
                    .into_iter()
                    .map(OrderValue::Bytes)
                    .collect(),
            };
            let key = if string_keys {
                Key::Str(format!("{prefix}{key:06}"))
            } else {
                Key::Long(key)
            };
            pairs.extend(orders.into_iter().map(|order| (key.clone(), order)));
        }
        pairs
    }

    /// `pairs`, of `order` values, as an ordered fixture of
    /// `FENCE_PAGE_ROWS` rows a page and `FENCE_GROUP_ROWS` a row group,
    /// with the layout to build it under.
    fn fence_fixture(pairs: &[(Key, OrderValue)], order: DrawnOrder) -> (Fixture, TableLayout) {
        let (key_type, column) = match &pairs[0].0 {
            Key::Str(_) => (KeyType::String, KeyColumn::Utf8),
            Key::Long(_) | Key::Bytes(_) | Key::Uuid(_) => (KeyType::Long, KeyColumn::Int64),
        };
        let mut fixture = Fixture::with_keys(
            column,
            pairs.iter().map(|(key, _)| Some(key.clone())).collect(),
        );
        let bytes = || {
            pairs
                .iter()
                .map(|(_, value)| match value {
                    OrderValue::Str(text) => Some(text.clone().into_bytes()),
                    OrderValue::Bytes(bytes) => Some(bytes.clone()),
                    OrderValue::Long(_) => None,
                })
                .collect()
        };
        fixture.orders = Some(match order {
            DrawnOrder::Long => Orders::Long(
                pairs
                    .iter()
                    .map(|(_, value)| match value {
                        OrderValue::Long(value) => Some(*value),
                        OrderValue::Str(_) | OrderValue::Bytes(_) => None,
                    })
                    .collect(),
            ),
            DrawnOrder::Str | DrawnOrder::Bin => Orders::Bytes(bytes()),
            DrawnOrder::Fixed => Orders::Fixed(i32::try_from(FIXED_ORDER_BYTES).unwrap(), bytes()),
        });
        (
            fixture.in_pages(FENCE_PAGE_ROWS, FENCE_GROUP_ROWS),
            ordered_layout(key_type, order.order_type()),
        )
    }

    /// What the fence property test saw across its fixtures: order pages,
    /// pages whose first row starts a key, pages whose first row continues
    /// the key before it, row groups whose first page does, and byte fences
    /// cut short of the page's first value and cut past the shared prefix.
    #[derive(Debug, Default, PartialEq, Eq)]
    struct FencesSeen {
        pages: usize,
        new_keys: usize,
        continued: usize,
        groups_continued: usize,
        cut_short: usize,
        past_prefix: usize,
    }

    /// Asserts that `index`'s order fences fence `pairs`, its rows: one per
    /// order page, each above the pair before its page's first row and at
    /// or below that row's pair, on that row's exact key, with its order
    /// value exact for a long and otherwise a prefix of the row's that one
    /// byte fewer would leave at or below the pair before (or empty, where
    /// there is none on the key); and that `page_of` places every row's
    /// pair on the page holding the row.
    fn assert_fences_fence(index: &FileIndex, pairs: &[(Key, OrderValue)], seen: &mut FencesSeen) {
        let fences = index.order_fences().unwrap();
        let metadata = index.metadata();
        let order_leaf = index.order_leaf().unwrap();
        let pair = |row: usize| Pair::of(&pairs[row].0, &pairs[row].1);
        let path = location();
        assert_eq!(fences.row_groups(), metadata.num_row_groups());
        let mut start = 0;
        for row_group in 0..metadata.num_row_groups() {
            let locations =
                metadata.offset_index().unwrap()[row_group][order_leaf].page_locations();
            assert_eq!(fences.pages(row_group), locations.len());
            for (page, location) in locations.iter().enumerate() {
                let row = start + row_index(location.first_row_index);
                let (fence, first) = (fences.fence(row_group, page), pair(row));
                let previous = row.checked_sub(1).map(pair);
                assert!(fence <= first, "row {row}: {fence:?} above {first:?}");
                assert!(
                    previous.is_none_or(|previous| previous < fence),
                    "row {row}"
                );
                assert_eq!(fence.key, first.key, "row {row}: the key is exact");
                let continues = previous.is_some_and(|previous| previous.key == first.key);
                seen.pages += 1;
                seen.new_keys += usize::from(!continues);
                seen.continued += usize::from(continues);
                seen.groups_continued += usize::from(continues && page == 0);
                match (fence.order, first.order) {
                    (PairPart::Long(kept), PairPart::Long(exact)) => assert_eq!(kept, exact),
                    (PairPart::Bytes(cut), PairPart::Bytes(whole)) => {
                        assert!(whole.starts_with(cut), "row {row}");
                        if let Some(shorter) = cut.len().checked_sub(1) {
                            let previous = previous.expect("a cut value follows a row on its key");
                            let shorter = Pair {
                                order: PairPart::Bytes(&cut[..shorter]),
                                ..fence
                            };
                            assert!(
                                shorter <= previous,
                                "row {row}: {cut:?} is not the shortest"
                            );
                        } else {
                            assert!(!continues, "row {row}: an empty value starts a key");
                        }
                        seen.cut_short += usize::from(cut.len() < whole.len());
                        seen.past_prefix += usize::from(cut.len() > SHARED_PREFIX_BYTES);
                    }
                    (kept, exact) => panic!("row {row}: {kept:?} fences {exact:?}"),
                }
            }
            let rows = row_index(metadata.row_group(row_group).num_rows());
            let order_chunk = Column {
                metadata,
                leaf: order_leaf,
                path: &path,
            }
            .group(row_group);
            for row in 0..rows {
                let page = order_chunk.page_holding(row);
                assert_eq!(
                    fences.page_of(row_group, pair(start + row)),
                    Some(page),
                    "row {row} of row group {row_group}"
                );
                assert_eq!(fences.place(pair(start + row)), Some((row_group, page)));
            }
            start += rows;
        }
    }

    /// Every order fence of random runs (RFD 025, Load),
    /// over long and string keys and long, string, binary, and fixed order
    /// values, the byte ones sharing a long prefix, sorts above the last
    /// pair of the page before its page and at or below its page's first
    /// pair, is the shortest such pair, and places every row on its page.
    #[tokio::test]
    async fn test_verify_ordered_fences_bound_every_order_page_with_the_shortest_pair() {
        // The shapes the fixed seeds draw, per order column: pages, pages
        // starting a key, pages continuing one, row groups whose first page
        // continues the last key of the row group before, byte fences cut
        // short of their row's value, and cut past the shared prefix.
        let drawn =
            |pages, new_keys, continued, groups_continued, cut_short, past_prefix| FencesSeen {
                pages,
                new_keys,
                continued,
                groups_continued,
                cut_short,
                past_prefix,
            };
        let expected = [
            (DrawnOrder::Long, drawn(576, 114, 462, 44, 0, 0)),
            (DrawnOrder::Str, drawn(576, 128, 448, 42, 256, 448)),
            (DrawnOrder::Bin, drawn(576, 128, 448, 42, 256, 448)),
            (DrawnOrder::Fixed, drawn(576, 178, 398, 30, 426, 398)),
        ];
        for (order, expected) in expected {
            let mut seen = FencesSeen::default();
            for seed in 0..FENCE_SEEDS {
                for string_keys in [false, true] {
                    let mut rng = StdRng::seed_from_u64(seed);
                    let pairs = drawn_pairs(&mut rng, string_keys, order);
                    let (fixture, layout) = fence_fixture(&pairs, order);
                    let opened = store(&fixture).await;
                    let index = opened
                        .build_under(&layout, Residency::Full, None)
                        .await
                        .unwrap();
                    assert_eq!(
                        index.metadata().num_row_groups(),
                        FENCE_ROWS / FENCE_GROUP_ROWS
                    );
                    assert_fences_fence(&index, &pairs, &mut seen);
                }
            }
            assert_eq!(seen, expected, "{order:?}");
        }
    }

    /// Verification decodes each column's dictionary once per row group,
    /// and each page once, under the ordered rules as under the key-value
    /// rules: a row group of several decode windows, over
    /// dictionary-encoded key and order columns, hands each column reader
    /// its dictionary page once.
    #[tokio::test]
    async fn test_verify_decodes_each_dictionary_once_per_row_group() {
        use std::collections::BTreeMap;

        use super::super::pages::Handed;
        const WINDOWS: usize = 3;
        let rows = row_index(DECODE_BATCH_RECORDS) * WINDOWS;
        // Distinct keys, which the key-value rules admit too.
        let pairs: Vec<(i64, i64)> = (0..i64::try_from(rows).unwrap())
            .map(|i| (i, i % 2))
            .collect();
        let mut fixture = Fixture::of_long_pairs(&pairs);
        fixture.layout.row_group_rows = rows;
        fixture.layout.dictionary = Dictionary::All;
        let opened = store(&fixture).await;
        let layout = ordered_layout(KeyType::Long, OrderType::Long);
        let inspected = FileIndex::inspect(TABLE, Arc::clone(&opened.handle), &layout)
            .await
            .unwrap();
        let metadata = Arc::clone(&inspected.metadata);
        assert_eq!(metadata.num_row_groups(), 1);
        let order = inspected.layout.order.unwrap();
        let ranges = inspected
            .layout
            .verified_ranges(&metadata, opened.handle.size());
        let parts = fetch_chunk_parts(&opened.handle, &ranges).await.unwrap();
        let chunks = Arc::new(Segments::new(parts, opened.handle.size()));
        let path = location();
        let column = Column {
            metadata: &metadata,
            leaf: inspected.layout.key_leaf,
            path: &path,
        };
        let key_chunk = column.group(0);
        let order_chunk = Column {
            leaf: order.leaf,
            ..column
        }
        .group(0);
        assert!(key_chunk.dictionary_range().is_some() && order_chunk.dictionary_range().is_some());
        let key_pages = (column.leaf, key_chunk.page_locations().len());
        let order_pages = (order.leaf, order_chunk.page_locations().len());
        let before = Handed::now();
        verify(
            TABLE,
            &column,
            KeyShape::Long,
            Rules::Ordered(order),
            &chunks,
        )
        .unwrap();
        assert_eq!(
            Handed::now().since(&before),
            Handed {
                dictionaries: 2,
                data: BTreeMap::from([key_pages, order_pages]),
            }
        );
        let before = Handed::now();
        verify(TABLE, &column, KeyShape::Long, Rules::of(None), &chunks).unwrap();
        assert_eq!(
            Handed::now().since(&before),
            Handed {
                dictionaries: 1,
                data: BTreeMap::from([key_pages]),
            },
            "the key-value rules read the key column alone, once"
        );
    }

    /// A file whose writer left out the order column's column index loads
    /// in every tier, with the fences the same rows get with one: the
    /// fences come from the rows, not from the index.
    #[tokio::test]
    async fn test_build_ordered_without_an_order_column_index_gets_its_fences_from_the_rows() {
        let mut rng = StdRng::seed_from_u64(FENCE_SEEDS);
        for string_orders in [false, true] {
            let order = if string_orders {
                DrawnOrder::Str
            } else {
                DrawnOrder::Long
            };
            let pairs = drawn_pairs(&mut rng, false, order);
            let (mut fixture, layout) = fence_fixture(&pairs, order);
            let indexed = store(&fixture).await;
            fixture.layout.order_statistics = Some(EnabledStatistics::Chunk);
            let unindexed = store(&fixture).await;
            let oracle = indexed
                .build_under(&layout, Residency::Full, None)
                .await
                .unwrap();
            for residency in Residency::ALL {
                let dir = tempfile::tempdir().unwrap();
                let local = testing::local_dir(dir.path(), unindexed.stats(), None);
                let local = (residency == Residency::Disk).then_some(&local);
                let index = unindexed
                    .build_under(&layout, residency, local)
                    .await
                    .unwrap();
                let order_leaf = index.order_leaf().unwrap();
                assert!(
                    index.metadata().column_index().is_none_or(|index| index
                        .iter()
                        .all(|group| matches!(group[order_leaf], ColumnIndexMetaData::NONE))),
                    "{residency}: no order column index"
                );
                assert_eq!(index.order_fences(), oracle.order_fences(), "{residency}");
                let mut seen = FencesSeen::default();
                assert_fences_fence(&index, &pairs, &mut seen);
                assert!(seen.pages > 0);
            }
        }
    }

    /// A Disk-tier restart over a trusted ordered chunk file restores the
    /// fences the first load recorded, with the footer
    /// the only GET and no chunk read back, so no order value decoded.
    #[tokio::test]
    async fn test_disk_tier_restart_restores_identical_order_fences_without_a_get_or_a_decode() {
        let mut rng = StdRng::seed_from_u64(FENCE_SEEDS + 1);
        for (string_keys, string_orders) in [(false, false), (true, true)] {
            let order = if string_orders {
                DrawnOrder::Str
            } else {
                DrawnOrder::Long
            };
            let pairs = drawn_pairs(&mut rng, string_keys, order);
            let (fixture, layout) = fence_fixture(&pairs, order);
            let opened = store(&fixture).await;
            let dir = tempfile::tempdir().unwrap();
            let local = testing::local_dir(dir.path(), opened.stats(), None);
            let first = opened
                .build_under(&layout, Residency::Disk, Some(&local))
                .await
                .unwrap();
            let fences = first.order_fences().unwrap();
            assert!(fences.pages(0) > 1);
            let reads = opened.stats().local_reads();
            let restarted = store_file(opened.file.clone()).await;
            let reused = restarted
                .build_under(&layout, Residency::Disk, Some(&local))
                .await
                .unwrap();
            assert_eq!(restarted.stats().gets(), 1, "the footer only");
            assert_eq!(
                opened.stats().local_reads(),
                reads,
                "no chunk was read back to be decoded"
            );
            assert_eq!(reused.order_fences(), Some(fences));
            assert_eq!(reused.order_facts(), first.order_facts());
        }
    }

    /// An ordered chunk file this build does not trust is treated as
    /// absent and rebuilt with its fences: one of the previous format
    /// version, one verified under the previous rules, and one whose record
    /// holds no fences, as a file of the previous version did. The rebuilt
    /// file is trusted by the restart after it.
    #[tokio::test]
    async fn test_disk_tier_rebuilds_an_ordered_chunk_file_of_the_previous_version() {
        type Edit = fn(&std::path::Path);
        let layout = ordered_layout(KeyType::Long, OrderType::Long);
        let cases: [(&str, Edit, Stale); 3] = [
            (
                "previous format version",
                |path| local_testing::set_version_byte(path, local_testing::PREVIOUS_VERSION),
                Stale::Rejected(Rejection::UnsupportedVersion {
                    version: local_testing::PREVIOUS_VERSION,
                }),
            ),
            (
                "previous verifier version",
                |path| local_testing::set_verifier_version(path, VERIFIER_VERSION - 1),
                Stale::VerifierVersion {
                    stored: VERIFIER_VERSION - 1,
                },
            ),
            (
                "record without fences",
                |path| {
                    local_testing::edit_verification(path, |bytes| {
                        *bytes = record::with_order_fences(bytes, Vec::clear);
                    });
                },
                Stale::Record(RecordFault::OrderFences {
                    row_group: ROW_GROUPS,
                }),
            ),
        ];
        let pairs: Vec<(i64, i64)> = (0..i64::try_from(contract_rows()).unwrap())
            .map(|i| (i / 3, i % 3))
            .collect();
        let opened = store(&Fixture::of_long_pairs(&pairs)).await;
        let oracle = opened
            .build_under(&layout, Residency::Full, None)
            .await
            .unwrap();
        for (case, edit, stale) in cases {
            let dir = tempfile::tempdir().unwrap();
            let local = testing::local_dir(dir.path(), opened.stats(), None);
            let index = opened
                .build_under(&layout, Residency::Disk, Some(&local))
                .await
                .unwrap();
            edit(&local.chunk_path(&identity_of(&opened.handle), index.chunk_rules()));
            let restarted = store_file(opened.file.clone()).await;
            let mut inspected = FileIndex::inspect(TABLE, Arc::clone(&restarted.handle), &layout)
                .await
                .unwrap();
            assert_eq!(
                inspected.trusted_key_range(&local).await.unwrap(),
                None,
                "{case}"
            );
            let Some(Err(Some(found))) = inspected.reuse.clone() else {
                panic!("{case}: the chunk file is stale");
            };
            assert_eq!(found, stale, "{case}");
            let rebuilt = inspected
                .build(Residency::Disk, Some(&local), None)
                .await
                .unwrap();
            assert_eq!(
                restarted.stats().gets(),
                1 + 2 * count(ROW_GROUPS),
                "{case}: the footer, then every key and order chunk"
            );
            assert_eq!(rebuilt.order_fences(), oracle.order_fences(), "{case}");
            let again = store_file(opened.file.clone()).await;
            let reused = again
                .build_under(&layout, Residency::Disk, Some(&local))
                .await
                .unwrap();
            assert_eq!(
                again.stats().gets(),
                1,
                "{case}: the rebuilt file is trusted"
            );
            assert_eq!(reused.order_fences(), oracle.order_fences(), "{case}");
        }
    }

    /// Rows in each budget fixture: two row groups.
    const BUDGET_ROWS: usize = 4_000;
    /// Rows per key in the budget fixtures, so most order pages continue a
    /// key and their fences keep the shared prefix.
    const BUDGET_RUN_ROWS: usize = 100;
    /// The bytes every order value of a budget fixture starts with.
    const BUDGET_PREFIX_BYTES: usize = 1_000;
    /// The bytes a truncated order column index keeps of each value.
    const BUDGET_TRUNCATE_BYTES: usize = 16;
    /// Rows per page in the plain budget fixtures.
    const PLAIN_PAGE_ROWS: usize = 2;
    /// The prefix, rows per page, rows, and rows per key of the
    /// dictionary-encoded budget fixture, whose keys share their order
    /// values, so its dictionary is small beside its fences.
    const DICTIONARY_PREFIX_BYTES: usize = 2_000;
    const DICTIONARY_PAGE_ROWS: usize = 3;
    const DICTIONARY_ROWS: usize = 3_000;
    const DICTIONARY_RUN_ROWS: usize = 30;

    /// An ordered file of `rows` rows, `run` per long key, whose string
    /// order values are `prefix` bytes and the row's place in its run,
    /// written `page_rows` to a page, `BUDGET_ROWS / 2` to a row group, with
    /// `edit` applied to its layout.
    fn budget_fixture(
        rows: usize,
        run: usize,
        prefix: usize,
        page_rows: usize,
        edit: impl FnOnce(&mut crate::index::testing::Layout),
    ) -> Fixture {
        let shared = "p".repeat(prefix);
        let keys: Vec<i64> = (0..rows).map(|i| i64::try_from(i / run).unwrap()).collect();
        let orders = (0..rows)
            .map(|i| Some(format!("{shared}{:06}", i % run).into_bytes()))
            .collect();
        let mut fixture =
            Fixture::ordered(&keys, Orders::Bytes(orders)).in_pages(page_rows, BUDGET_ROWS / 2);
        edit(&mut fixture.layout);
        fixture
    }

    /// The Full tier's resident bytes and the Disk tier's chunk file of
    /// `opened`, built under a `string` order column, beside what the
    /// load's footer stage (`FileIndex::inspect`) predicted for each.
    async fn budget_of(opened: &Opened) -> ([u64; 2], [u64; 2]) {
        let layout = ordered_layout(KeyType::Long, OrderType::String);
        let predicted = *FileIndex::inspect(TABLE, Arc::clone(&opened.handle), &layout)
            .await
            .unwrap()
            .predicted();
        let full = opened
            .build_under(&layout, Residency::Full, None)
            .await
            .unwrap();
        let dir = tempfile::tempdir().unwrap();
        let local = testing::local_dir(dir.path(), opened.stats(), None);
        opened
            .build_under(&layout, Residency::Disk, Some(&local))
            .await
            .unwrap();
        let written = local.index_bytes_in_use().unwrap();
        (
            [predicted.full, count(full.resident_bytes())],
            [predicted.disk_bytes, written],
        )
    }

    /// The load predicts a file's resident bytes from the footer as it
    /// keeps it (`FileIndex::inspect`), which holds no order column index
    /// (RFD 025, Load), so the prediction is the one the footer as loaded
    /// gives: string order values and fences are estimated from the order
    /// chunks' uncompressed bytes per value alone, whatever the writer's
    /// column index holds, truncated, full, or none.
    #[tokio::test]
    async fn test_predicted_bytes_of_string_orders_never_read_the_order_column_index() {
        type Edit = fn(&mut crate::index::testing::Layout);
        let cases: [(&str, Edit); 3] = [
            ("truncated", |layout| {
                layout.truncate = Some(BUDGET_TRUNCATE_BYTES);
            }),
            ("full", |_| {}),
            ("absent", |layout| {
                layout.order_statistics = Some(EnabledStatistics::Chunk);
            }),
        ];
        let layout = ordered_layout(KeyType::Long, OrderType::String);
        for (case, edit) in cases {
            let fixture = budget_fixture(
                BUDGET_ROWS,
                BUDGET_RUN_ROWS,
                BUDGET_PREFIX_BYTES,
                PLAIN_PAGE_ROWS,
                edit,
            );
            let opened = store(&fixture).await;
            let inspected = FileIndex::inspect(TABLE, Arc::clone(&opened.handle), &layout)
                .await
                .unwrap();
            let loaded = opened.handle.load_metadata().await.unwrap();
            let from_loaded = FileIndex::predicted_resident_bytes(
                &loaded,
                inspected.layout.key_leaf,
                inspected.layout.order,
                &inspected.layout.held_value_leaves(),
                KeyType::Long,
                &opened.handle,
            );
            assert_eq!(*inspected.predicted(), from_loaded, "{case}");
        }
    }

    /// The footer stage's estimate of string order fences, each at the
    /// order chunk's uncompressed bytes per value, covers them where the
    /// footer bounds them, a plain chunk's, whatever the writer's column
    /// index holds: truncated, whose page minimums keep 16 of each value's
    /// thousand bytes, full, or none. Each case gives the Full tier's
    /// predicted and resident bytes and the Disk tier's predicted and
    /// written chunk file, as the load's footer stage predicts them.
    #[tokio::test]
    async fn test_predicted_bytes_cover_string_order_fences_whatever_the_column_index() {
        type Edit = fn(&mut crate::index::testing::Layout);
        let cases: [(&str, Edit, [u64; 2], [u64; 2]); 3] = [
            (
                "truncated",
                |layout| layout.truncate = Some(BUDGET_TRUNCATE_BYTES),
                [2_444_086, 2_377_460],
                [2_271_705, 2_205_036],
            ),
            (
                "full",
                |_| {},
                [2_444_470, 2_377_844],
                [2_271_705, 2_205_036],
            ),
            (
                "absent",
                |layout| layout.order_statistics = Some(EnabledStatistics::Chunk),
                [2_444_470, 2_377_844],
                [2_271_705, 2_205_036],
            ),
        ];
        for (case, edit, full, disk) in cases {
            let fixture = budget_fixture(
                BUDGET_ROWS,
                BUDGET_RUN_ROWS,
                BUDGET_PREFIX_BYTES,
                PLAIN_PAGE_ROWS,
                edit,
            );
            let opened = store(&fixture).await;
            let ([predicted, resident], [predicted_disk, written]) = budget_of(&opened).await;
            assert_eq!(
                ([predicted, resident], [predicted_disk, written]),
                (full, disk),
                "{case}"
            );
            assert!(
                predicted >= resident,
                "{case}: {predicted} predicted, {resident} resident"
            );
            assert!(
                predicted_disk >= written,
                "{case}: {predicted_disk} predicted, {written} written"
            );
        }
    }

    /// No footer number bounds a dictionary-encoded order column's fences,
    /// so the Disk tier charges a chunk file's exact length before writing
    /// it: beyond the footer's estimate, the
    /// reservation grows by the rest, refused as `OverBudget` where the
    /// budget does not hold it though it held the estimate. The Full tier's
    /// prediction falls as far short, 241,466 bytes for 2,070,894 resident,
    /// which the load charges once built (`generation::check_built_budget`).
    #[tokio::test]
    async fn test_disk_tier_charges_the_exact_ordered_chunk_file_before_writing_it() {
        let fixture = budget_fixture(
            DICTIONARY_ROWS,
            DICTIONARY_RUN_ROWS,
            DICTIONARY_PREFIX_BYTES,
            DICTIONARY_PAGE_ROWS,
            |layout| layout.dictionary = Dictionary::All,
        );
        let opened = store(&fixture).await;
        let (full, [estimate, exact]) = budget_of(&opened).await;
        assert_eq!(full, [241_466, 2_070_894], "predicted and resident");
        assert_eq!(
            [estimate, exact],
            [133_011, 1_963_372],
            "predicted and written"
        );
        let layout = ordered_layout(KeyType::Long, OrderType::String);
        let build_within = |budget: u64| {
            let opened = &opened;
            let layout = &layout;
            async move {
                let dir = tempfile::tempdir().unwrap();
                let local = testing::local_dir(dir.path(), opened.stats(), Some(budget));
                let inspected = FileIndex::inspect(TABLE, Arc::clone(&opened.handle), layout)
                    .await
                    .unwrap();
                let reservation = Arc::new(
                    local
                        .reserve(TABLE, inspected.predicted().disk_bytes)
                        .unwrap(),
                );
                let built = inspected
                    .build(Residency::Disk, Some(&local), Some(&reservation))
                    .await;
                drop(reservation);
                (built.map(|_| ()), local.index_bytes_in_use().unwrap())
            }
        };
        let (refused, written) = build_within(exact - 1).await;
        assert_eq!(
            refusal_of(refused),
            RefusalReason::OverBudget {
                of: crate::error::Budget::Index,
                needed: exact,
                budget: exact - 1,
            }
        );
        assert_eq!(written, 0, "nothing was written");
        let (built, written) = build_within(exact).await;
        built.unwrap();
        assert_eq!(written, exact);
    }

    /// A chunk file that has landed counts once, as bytes on disk, not again
    /// in the load's reservation: of two ordered files whose chunk files fill
    /// the budget exactly, the second is charged its exact length beside the
    /// first's file on disk, not beside the first's file and its estimate.
    /// A budget a byte short still refuses the second.
    #[tokio::test]
    async fn test_disk_tier_counts_a_landed_chunk_file_once() {
        let layout = ordered_layout(KeyType::Long, OrderType::String);
        let mut files = Vec::new();
        for (key, rows) in [
            ("first.parquet", DICTIONARY_ROWS),
            ("second.parquet", DICTIONARY_ROWS + DICTIONARY_RUN_ROWS),
        ] {
            let fixture = budget_fixture(
                rows,
                DICTIONARY_RUN_ROWS,
                DICTIONARY_PREFIX_BYTES,
                DICTIONARY_PAGE_ROWS,
                |layout| layout.dictionary = Dictionary::All,
            );
            let opened = store_as(&fixture, key).await;
            let (_, [estimate, exact]) = budget_of(&opened).await;
            assert!(exact > estimate, "the exact charge reserves a rest");
            files.push((opened, estimate, exact));
        }
        let estimates: u64 = files.iter().map(|(_, estimate, _)| estimate).sum();
        let exacts: u64 = files.iter().map(|(_, _, exact)| exact).sum();
        let load_within = |budget: u64| {
            let (files, layout) = (&files, &layout);
            async move {
                let dir = tempfile::tempdir().unwrap();
                let local = testing::local_dir(dir.path(), files[0].0.stats(), Some(budget));
                let reservation = Arc::new(local.reserve(TABLE, estimates).unwrap());
                let mut result = Ok(());
                for (opened, _, _) in files {
                    let inspected = FileIndex::inspect(TABLE, Arc::clone(&opened.handle), layout)
                        .await
                        .unwrap();
                    let built = inspected
                        .build(Residency::Disk, Some(&local), Some(&reservation))
                        .await;
                    if let Err(error) = built {
                        result = Err(error);
                        break;
                    }
                }
                drop(reservation);
                (result, local.index_bytes_in_use().unwrap())
            }
        };
        let (loaded, written) = load_within(exacts).await;
        loaded.unwrap();
        assert_eq!(written, exacts);
        let (refused, _) = load_within(exacts - 1).await;
        assert_eq!(
            refusal_of(refused),
            RefusalReason::OverBudget {
                of: crate::error::Budget::Index,
                needed: exacts,
                budget: exacts - 1,
            }
        );
    }
}
