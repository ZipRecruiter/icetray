//! The optional local directory (RFD 017, RFD 012): chunk files for disk
//! residency under `index/`, each named by a digest of its data file's path
//! and the rules it was verified under (RFD 025) and carrying that file's
//! identity in a header inside the persisted-state
//! envelope of RFD 011, so a restarted replica reuses them and a generation
//! that drops a file deletes them; the page cache's disk level under
//! `cache/`; positioned, counted reads of the pages the chunk files hold;
//! and samples of which of their pages the kernel's page cache holds (RFD
//! 020). Everything here is rebuilt from object storage and needs no
//! backup.
//!
//! A chunk file also stores what verifying its keys established (the
//! verification record, which the index encodes) and a crc32c of its
//! payload, so a restart trusts a file whose identity, verification context,
//! and checksum match without decoding a key. The checksum is what makes an
//! unsynced write safe: a file a crash tore fails it and is rebuilt, and the
//! write-then-rename keeps a reader from seeing a partial file under its
//! name.

mod disk;
mod format;
mod residency;
#[cfg(test)]
pub(crate) mod testing;

use std::collections::HashSet;
use std::fs::{self, File, Metadata};
use std::io::{self, Write as _};
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use bytes::Bytes;
pub use disk::{DiskChunks, DiskLevel};
use format::{
    HEADER_OFFSET, MAX_HEADER_BYTES, ParseFailure, ParsedChunkFile, chunk_name, header_for,
    parse_chunk_file, validate, write_envelope,
};
pub(crate) use format::{Rejection, chunk_file_bytes};
use prost::Message as _;
pub(crate) use residency::{ResidentPages, Unobserved, page_bytes, sysconf_count};
use sha2::{Digest as _, Sha256};

use crate::approx;
use crate::config::LocalConfig;
use crate::contract::PAGE_CACHE_DISK_BLOCK_BYTES;
use crate::error::{Budget, Error, Recurrence, Recurs, RefusalReason, Result};
use crate::key::KeyType;
use crate::storage::IoStats;

/// The directory under `LocalConfig::dir` that holds chunk files.
const INDEX_DIR: &str = "index";
/// The directory under `LocalConfig::dir` that holds the page cache's disk
/// level.
const CACHE_DIR: &str = "cache";
/// The file beside `CACHE_DIR` that names the format the disk level was
/// written in (`cache_format`). It sits outside `CACHE_DIR`, which the
/// cache's device owns.
const CACHE_FORMAT_FILE: &str = "cache.format";
/// The first byte of `CACHE_FORMAT_FILE`, as `format::CHUNK_FILE_MAGIC`
/// opens a chunk file (RFD 011).
const CACHE_FORMAT_MAGIC: u8 = 0xC2;
/// The extension of a chunk file still being written. A file carrying it is
/// incomplete: `write_chunks` renames it away once it is written, and
/// `retain` deletes any that no writer in this process has open, which is
/// what a process that died mid-write leaves behind.
const TEMP_EXTENSION: &str = "tmp";
/// The width of the digest a chunk file is named by and a footer is
/// identified by: the width of SHA-256, which `digest` computes.
pub(crate) const DIGEST_BYTES: usize = 32;

/// The digest the chunk file format uses, of a data file's footer for its
/// identity and of its path for the file name: SHA-256. The one place the
/// algorithm is named, so the loader that digests a footer and this module
/// that checks it agree.
pub(crate) fn digest(bytes: &[u8]) -> [u8; DIGEST_BYTES] {
    Sha256::digest(bytes).into()
}

/// What a chunk file is named by and validated against (RFD 017). The name
/// is the hex `digest` of `path` and the file's `ChunkRules`, so no path
/// appears in a file name; the header carries all three fields and the
/// rules, and a chunk file whose header disagrees with the identity the
/// loader holds is stale and never reused.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct DataFileIdentity {
    /// The data file's location as the manifest records it.
    pub path: String,
    /// The data file's size in bytes, as the manifest records it.
    pub size: u64,
    /// `digest` of the footer bytes the loader read. This module stores and
    /// compares it and never computes it.
    pub footer_digest: [u8; DIGEST_BYTES],
}

/// The rules a data file was verified under, which a chunk file is named
/// by (RFD 025): a key-value table and an ordered table over one data file
/// verify different things, so each keeps its own chunk file and neither
/// trusts the other's. An ordered table's rules include the order column,
/// since a file verified in order of one column says nothing of another.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) enum ChunkRules {
    /// Distinct keys, one row per key.
    KeyValue,
    /// Rows strictly increasing by key and then the order column.
    Ordered {
        /// The order column's Iceberg field id.
        order_field_id: i32,
    },
}

/// What a chunk file's verification record was computed under (RFD 017):
/// the key type the table verifies its keys as, the version of the
/// rules verification enforced (`index::VERIFIER_VERSION`), and the
/// `ChunkRules` the file is named by. The header carries it, and a record
/// computed under anything but the reader's is never trusted.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct VerifiedUnder {
    /// The key type the keys were verified as.
    pub(crate) key_type: KeyType,
    /// The verifier's rules.
    pub(crate) verifier_version: u32,
    /// The table kind's rules, which also name the chunk file.
    pub(crate) rules: ChunkRules,
}

/// What `LocalDir::lookup` finds under a data file's chunk file name.
#[derive(Debug)]
pub(crate) enum ChunkFileLookup {
    /// No file.
    Absent,
    /// A file that fails a check of its envelope, header, or identity: the
    /// check it failed.
    Rejected(Rejection),
    /// A file that passes them.
    Found(DiskChunks),
}

/// Why the local directory failed; the set is closed. The path the
/// operation was on is the `path` of `Error::Local`.
#[derive(Debug, thiserror::Error)]
pub enum LocalFault {
    /// The operation's I/O error.
    #[error("{0}")]
    Io(std::io::Error),
    /// A page read for a byte range no stored chunk contains whole. The
    /// chunk file holds the spans its header names and nothing else, so a
    /// range outside them or straddling two is a caller's error, never a
    /// fetch, and never counted as a read.
    #[error("bytes {start}..{end} are in no stored chunk")]
    RangeNotResident {
        /// The first byte asked for.
        start: u64,
        /// One past the last byte asked for.
        end: u64,
    },
    /// A chunk handed to `write_chunks` whose bytes are not its range's
    /// length, an empty or inverted range included; the file would promise
    /// one and hold the other.
    #[error("chunk {start}..{end} came with {len} bytes")]
    ChunkLengthMismatch {
        /// The first byte of the data file the chunk claims to hold.
        start: u64,
        /// One past the last byte it claims to hold.
        end: u64,
        /// The bytes it came with.
        len: u64,
    },
    /// A chunk handed to `write_chunks` whose range overlaps the chunk
    /// before it in data order; a page in the overlap could come from
    /// either.
    #[error("chunk {start}..{end} overlaps the chunk before it")]
    ChunkOverlap {
        /// The first byte of the data file the chunk claims to hold.
        start: u64,
        /// One past the last byte it claims to hold.
        end: u64,
    },
    /// An encoded header longer than `MAX_HEADER_BYTES`, which no reader
    /// accepts.
    #[error("header of {len} bytes exceeds {MAX_HEADER_BYTES} bytes")]
    HeaderTooLarge {
        /// The encoded header's length.
        len: usize,
    },
    /// The blocking task that ran a page read or a chunk file's write was
    /// canceled: the runtime shut down under it. A panic in that task is
    /// resumed on the caller instead, never turned into an error.
    #[error("blocking task was canceled: {0}")]
    Join(tokio::task::JoinError),
}

/// The local directory is the process's, rebuilt from object storage, so a
/// fault of it is tried again, unless the data file alone decides it.
impl Recurs for LocalFault {
    fn recurrence(&self) -> Recurrence {
        match self {
            // The header holds the data file's identity and one span per
            // row group of its footer, so its length is the file's.
            Self::HeaderTooLarge { .. }
            // The spans are the key chunk ranges the footer bounds.
            | Self::ChunkOverlap { .. } => Recurrence::Deterministic,
            // A chunk's bytes are what the store returned for its range,
            // and the fault does not say whether the range or the bytes
            // came up short.
            Self::ChunkLengthMismatch { .. }
            // A caller's read outside the spans it was handed: the
            // process's fault, not the snapshot's.
            | Self::RangeNotResident { .. }
            | Self::Io(_)
            | Self::Join(_) => Recurrence::Transient,
        }
    }
}

/// The local directory, opened once per process and shared by every table
/// (RFD 017). `index/` holds chunk files and `cache/` the page cache's disk
/// level; both exist once `open` has returned. Everything in it is rebuilt
/// from object storage.
///
/// Every method is synchronous and does its file I/O on the caller's
/// thread; the caller decides whether to run one on a blocking thread. The
/// header reads that validate or sweep chunk files, and the payload read
/// that checks a reused file's checksum, are not local reads in the sense
/// of RFD 003; only `DiskChunks::page` counts one.
#[derive(Debug)]
pub struct LocalDir {
    index_dir: PathBuf,
    cache_dir: PathBuf,
    index_budget: Option<u64>,
    /// The page cache's disk level's size bound, when it has one
    /// (`LocalConfig::has_page_cache_level`).
    page_cache_disk_bytes: Option<u64>,
    stats: Arc<IoStats>,
    /// The temporary files `write_chunks` calls in this process have open.
    /// `retain` keeps these and deletes every other temporary file, so a
    /// sweep never removes a write in progress from under its writer.
    in_flight: Mutex<HashSet<PathBuf>>,
    /// The sequence number of the next temporary file name.
    next_temp: AtomicU64,
    /// The bytes every outstanding `Reservation` holds: disk-tier loads
    /// that passed the budget and have not finished writing.
    reserved: Mutex<u64>,
}

impl LocalDir {
    /// Opens the directory, creating `index/` and `cache/` under it when
    /// they are absent, and emptying `cache/` when `CACHE_FORMAT_FILE` does
    /// not hold this build's `cache_format` (`open_cache_dir`). A directory
    /// that cannot be created or emptied is `Error::Local` on that path.
    pub fn open(config: &LocalConfig, stats: Arc<IoStats>) -> Result<Self> {
        let index_dir = config.dir.join(INDEX_DIR);
        fs::create_dir_all(&index_dir).map_err(|error| local_error(index_dir.clone(), error))?;
        let cache_dir = open_cache_dir(&config.dir)?;
        Ok(Self {
            index_dir,
            cache_dir,
            index_budget: config.index_budget_bytes,
            page_cache_disk_bytes: config
                .has_page_cache_level()
                .then_some(config.page_cache_disk_bytes),
            stats,
            in_flight: Mutex::new(HashSet::new()),
            next_temp: AtomicU64::new(0),
            reserved: Mutex::new(0),
        })
    }

    /// The key-value chunk file for a data file, identified by path, size,
    /// and footer digest; `None` if absent or failing a check of its envelope, header,
    /// or identity (`lookup` says which). Only an I/O failure is an error.
    pub fn chunks_for(&self, identity: &DataFileIdentity) -> Result<Option<DiskChunks>> {
        Ok(match self.lookup(identity, ChunkRules::KeyValue)? {
            ChunkFileLookup::Found(disk) => Some(disk),
            ChunkFileLookup::Absent | ChunkFileLookup::Rejected(_) => None,
        })
    }

    /// What is under the chunk file name for a data file verified under
    /// `rules`: nothing, a file failing a check of its envelope, header, or
    /// identity, or whose header names other rules, with the check it
    /// failed, or a file passing them. Reads the envelope and header and
    /// no payload byte; the caller that trusts the file checks the payload
    /// (`DiskChunks::checked_verification`), and logs whatever makes it
    /// rebuild the file. A rejected file stays until `write_chunks` replaces
    /// it or `retain` deletes it. Only an I/O failure is an error.
    pub(crate) fn lookup(
        &self,
        identity: &DataFileIdentity,
        rules: ChunkRules,
    ) -> Result<ChunkFileLookup> {
        let path = self.chunk_path(identity, rules);
        Ok(
            match open_parsed(&path, |file| validate(file, identity, rules))? {
                Opened::Absent => ChunkFileLookup::Absent,
                Opened::Rejected(rejection) => ChunkFileLookup::Rejected(rejection),
                Opened::Parsed(file, parsed) => ChunkFileLookup::Found(DiskChunks::new(
                    file,
                    path,
                    parsed,
                    Arc::clone(&self.stats),
                )),
            },
        )
    }

    /// Writes verified key chunks, and an ordered table's order chunks with
    /// them, with their verification record, the bytes
    /// the index encoded from what verifying them established `under`, and
    /// returns the handle the index keeps. The header names every span and
    /// the payload's checksum before the first payload byte, so the chunks
    /// are collected first and sorted by data offset; one file's chunks is
    /// the disk tier's stated load peak (RFD 017). The file is written to a
    /// temporary name unique to this call and renamed over `index/<name>`,
    /// so a reader never sees a partial file under that name and two writers
    /// of one identity never share a temporary file; the handle returned is
    /// over the file this call wrote, whichever writer's rename lands last.
    /// Neither the file nor the directory is synced: a file a crash tore,
    /// cut short or holding blocks never written, fails its length check or
    /// its payload checksum and is rebuilt, and a lost rename costs one
    /// re-fetch, since the directory is disposable. Every chunk's bytes must
    /// be its range's length and no two chunks may overlap.
    pub(crate) fn write_chunks(
        &self,
        identity: &DataFileIdentity,
        under: VerifiedUnder,
        verification: &[u8],
        chunks: impl Iterator<Item = (Range<u64>, Bytes)>,
    ) -> Result<DiskChunks> {
        let path = self.chunk_path(identity, under.rules);
        let mut chunks: Vec<(Range<u64>, Bytes)> = chunks.collect();
        chunks.sort_by_key(|(range, _)| range.start);
        let header = header_for(identity, under, verification, &chunks)
            .map_err(|fault| local_fault(path.clone(), fault))?;
        let temp = self.begin_temp(&path);
        let file = write_envelope(&temp.path, &header, verification, &chunks)
            .map_err(|fault| local_fault(temp.path.clone(), fault))?;
        fs::rename(&temp.path, &path).map_err(|error| local_error(path.clone(), error))?;
        drop(temp);
        let payload_offset = HEADER_OFFSET + approx::count(header.encoded_len());
        let parsed = ParsedChunkFile::from_header(header, payload_offset)
            .expect("header_for writes only headers a reader accepts");
        Ok(DiskChunks::new(file, path, parsed, Arc::clone(&self.stats)))
    }

    /// Deletes every chunk file no current generation references and every
    /// temporary file no `write_chunks` in this process has open, and
    /// returns the bytes freed. A chunk file is kept only when it is
    /// complete, passes every check, is named by its own header's path and
    /// rules, and its identity and rules are in `live`; such a file is never touched. The
    /// directory is shared by every table, so `live` is the union over every
    /// table of every identity its serving generation and any load in
    /// progress reference, each with the rules its table verifies under; the
    /// caller that owns all the tables builds it. A
    /// file that is gone by the time it is reached was freed by someone else
    /// and counts nothing.
    pub(crate) fn retain(&self, live: &HashSet<(DataFileIdentity, ChunkRules)>) -> Result<u64> {
        let mut freed = 0;
        for (path, metadata) in self.index_files()? {
            if self.keeps(&path, live)? {
                continue;
            }
            freed += remove_if_present(&path, metadata.len())?;
        }
        Ok(freed)
    }

    /// The bytes every file under `index/` occupies, a write in progress
    /// included, since the budget is disk. `reserve` checks this, with the
    /// reservations outstanding, against `index_budget` for the bytes a
    /// disk-tier load needs, at the footer stage (RFD 017).
    pub fn index_bytes_in_use(&self) -> Result<u64> {
        Ok(self
            .index_files()?
            .iter()
            .map(|(_, metadata)| metadata.len())
            .sum())
    }

    /// Holds `needed` bytes of `index/` for a disk-tier load of `table`
    /// until the returned `Reservation` drops. Refused as
    /// `RefusalReason::OverBudget` when the bytes `index/` holds, plus every
    /// reservation outstanding, plus `needed` exceed `index_budget`; that
    /// sum is the refusal's `needed`, `u64::MAX` for a sum past `u64`,
    /// which is over any budget. The directory is listed, the sum
    /// checked, and the bytes held under one lock, so of two loads that each
    /// fit alone but not together the one that reaches the lock second is
    /// refused. A load holds its reservation until it has finished writing,
    /// and each chunk file write it starts holds it too, so a write that
    /// outlives a load dropped mid-build keeps its bytes reserved until it
    /// ends. A chunk file is counted by `index_bytes_in_use` from the moment
    /// its temporary file is created, so the write releases the file's share
    /// of the reservation (`Reservation::release`) once the file has landed.
    /// A file written over a chunk file already on disk releases nothing,
    /// since the load may have reserved nothing for it: those bytes can count
    /// twice until the load ends, which errs toward refusal. With no budget
    /// nothing is refused, `index/` is not listed, and nothing is held.
    pub(crate) fn reserve(self: &Arc<Self>, table: &str, needed: u64) -> Result<Reservation> {
        let Some(budget) = self.index_budget else {
            return Ok(Reservation {
                local: Arc::clone(self),
                bytes: AtomicU64::new(0),
            });
        };
        let mut reserved = self.lock_reserved();
        let total = self
            .index_bytes_in_use()?
            .checked_add(*reserved)
            .and_then(|held| held.checked_add(needed));
        match total {
            Some(total) if total <= budget => {
                *reserved += needed;
                Ok(Reservation {
                    local: Arc::clone(self),
                    bytes: AtomicU64::new(needed),
                })
            }
            Some(_) | None => Err(Error::refused(
                table,
                RefusalReason::OverBudget {
                    of: Budget::Index,
                    needed: total.unwrap_or(u64::MAX),
                    budget,
                },
            )),
        }
    }

    /// The bytes chunk files may occupy across every table, from
    /// `LocalConfig::index_budget_bytes`; `None` is unbounded.
    pub(crate) fn index_budget(&self) -> Option<u64> {
        self.index_budget
    }

    /// The page cache's disk level, or `None` when
    /// `LocalConfig::page_cache_disk_bytes` is zero and there is no level.
    /// A level is at least one block, `contract::PAGE_CACHE_DISK_BLOCK_BYTES`,
    /// which `Config::parse` checks: below that the level has no block to
    /// write into and would hold nothing.
    pub fn page_cache_level(&self) -> Option<DiskLevel> {
        self.page_cache_disk_bytes.map(|capacity_bytes| DiskLevel {
            dir: self.cache_dir.clone(),
            capacity_bytes,
            stats: Arc::clone(&self.stats),
        })
    }

    /// The final path of the chunk file for `identity` verified under
    /// `rules`.
    pub(crate) fn chunk_path(&self, identity: &DataFileIdentity, rules: ChunkRules) -> PathBuf {
        self.index_dir.join(chunk_name(&identity.path, rules))
    }

    /// Every regular file directly under `index/` with its metadata.
    /// Subdirectories and links are nothing this module wrote and are left
    /// alone, and so is an entry renamed or deleted between the listing and
    /// its stat, which a concurrent `write_chunks` or `retain` does.
    fn index_files(&self) -> Result<Vec<(PathBuf, Metadata)>> {
        let entries = fs::read_dir(&self.index_dir)
            .map_err(|error| local_error(self.index_dir.clone(), error))?;
        let mut files = Vec::new();
        for entry in entries {
            let entry = entry.map_err(|error| local_error(self.index_dir.clone(), error))?;
            let path = entry.path();
            let metadata = match entry.metadata() {
                Ok(metadata) => metadata,
                Err(error) if error.kind() == io::ErrorKind::NotFound => continue,
                Err(error) => return Err(local_error(path, error)),
            };
            if metadata.is_file() {
                files.push((path, metadata));
            }
        }
        Ok(files)
    }

    /// Whether `retain` keeps a file under `index/`: a temporary file is
    /// kept while a `write_chunks` in this process has it open; a chunk file
    /// is kept when it passes every check, its name is the digest of its
    /// header's path and rules, and its identity and rules are in `live`. A file already gone is not
    /// kept, and deleting it then frees nothing.
    fn keeps(&self, path: &Path, live: &HashSet<(DataFileIdentity, ChunkRules)>) -> Result<bool> {
        if is_temp(path) {
            return Ok(self.lock_in_flight().contains(path));
        }
        let parsed = match open_parsed(path, parse_chunk_file)? {
            Opened::Parsed(_file, parsed) => parsed,
            Opened::Absent => return Ok(false),
            Opened::Rejected(rejection) => {
                warn_rejected(path, rejection);
                return Ok(false);
            }
        };
        let rules = parsed.under.rules;
        if path != self.chunk_path(&parsed.identity, rules) {
            warn_rejected(path, Rejection::NameMismatch);
            return Ok(false);
        }
        Ok(live.contains(&(parsed.identity, rules)))
    }

    /// A temporary path beside `path` for one `write_chunks` call,
    /// registered in `in_flight` until the guard drops. The name carries the
    /// process id and a sequence number, so two writers of one identity
    /// never share a temporary file and a leftover of a dead process never
    /// collides with a live write.
    fn begin_temp(&self, path: &Path) -> InFlight<'_> {
        let sequence = self.next_temp.fetch_add(1, Ordering::Relaxed);
        let mut name = path.as_os_str().to_owned();
        name.push(format!(
            ".{}-{sequence}.{TEMP_EXTENSION}",
            std::process::id()
        ));
        let path = PathBuf::from(name);
        self.lock_in_flight().insert(path.clone());
        InFlight { dir: self, path }
    }

    /// The registry of temporary files being written. A panic while it was
    /// held leaves the set whole (an insert or remove of one path either
    /// happened or did not), so poisoning is recovered from rather than
    /// propagated.
    fn lock_in_flight(&self) -> MutexGuard<'_, HashSet<PathBuf>> {
        self.in_flight
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
    }

    /// The bytes reservations hold. A panic while it was held leaves the
    /// count whole (an addition or subtraction either happened or did not),
    /// so poisoning is recovered from rather than propagated.
    fn lock_reserved(&self) -> MutexGuard<'_, u64> {
        self.reserved.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

/// A temporary file one `write_chunks` call has open, held in
/// `LocalDir::in_flight` so `retain` skips it; unregistered when dropped,
/// whether the write succeeded or not. A file whose write failed stays on
/// disk unregistered and the next `retain` deletes it.
struct InFlight<'a> {
    dir: &'a LocalDir,
    path: PathBuf,
}

impl Drop for InFlight<'_> {
    fn drop(&mut self) {
        self.dir.lock_in_flight().remove(&self.path);
    }
}

/// Bytes of `index/` that `LocalDir::reserve` holds for one disk-tier
/// load, released when this drops, or in part by `release`.
#[derive(Debug)]
pub(crate) struct Reservation {
    local: Arc<LocalDir>,
    bytes: AtomicU64,
}

impl Reservation {
    /// Stops holding `bytes` of what this holds, or all of it when it holds
    /// less. Called once a chunk file the bytes were held for has landed,
    /// when `index_bytes_in_use` counts them as bytes on disk.
    pub(crate) fn release(&self, bytes: u64) {
        let mut reserved = self.local.lock_reserved();
        let held = self.bytes.load(Ordering::Relaxed);
        let released = held.min(bytes);
        self.bytes.store(held - released, Ordering::Relaxed);
        *reserved -= released;
    }
}

impl Drop for Reservation {
    fn drop(&mut self) {
        *self.local.lock_reserved() -= *self.bytes.get_mut();
    }
}

/// Whether a file under `index/` is a write in progress or left behind.
fn is_temp(path: &Path) -> bool {
    path.extension()
        .is_some_and(|extension| extension == TEMP_EXTENSION)
}

/// Deletes the file at `path` and returns `len`, the bytes that freed; a
/// file already gone was freed by someone else and returns zero.
fn remove_if_present(path: &Path, len: u64) -> Result<u64> {
    match fs::remove_file(path) {
        Ok(()) => Ok(len),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(0),
        Err(error) => Err(local_error(path.to_path_buf(), error)),
    }
}

/// A file under `index/` as `open_parsed` found it.
enum Opened<T> {
    /// No file.
    Absent,
    /// A file that failed a check.
    Rejected(Rejection),
    /// A file that passed, open, with what the parse took from it.
    Parsed(File, T),
}

/// Opens the file at `path` and parses it with `parse`; only an I/O failure
/// is an error.
fn open_parsed<T>(
    path: &Path,
    parse: impl FnOnce(&File) -> std::result::Result<T, ParseFailure>,
) -> Result<Opened<T>> {
    let file = match File::open(path) {
        Ok(file) => file,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(Opened::Absent),
        Err(error) => return Err(local_error(path.to_path_buf(), error)),
    };
    match parse(&file) {
        Ok(parsed) => Ok(Opened::Parsed(file, parsed)),
        Err(ParseFailure::Rejected(rejection)) => Ok(Opened::Rejected(rejection)),
        Err(ParseFailure::Io(error)) => Err(local_error(path.to_path_buf(), error)),
    }
}

/// What `CACHE_FORMAT_FILE` holds for the disk level this build writes:
/// `CACHE_FORMAT_MAGIC`, the block size as eight little-endian bytes, and
/// the crate's major and minor version. A change to what the level stores,
/// a foyer upgrade that changes its files included, raises the minor
/// version (`CONTRIBUTING.md`, Stability), so a level survives a patch
/// release and is emptied by any other.
fn cache_format() -> Vec<u8> {
    let mut format = vec![CACHE_FORMAT_MAGIC];
    format.extend_from_slice(&PAGE_CACHE_DISK_BLOCK_BYTES.to_le_bytes());
    format.extend_from_slice(cache_release().as_bytes());
    format
}

/// The release a disk level is kept within: the crate's major and minor
/// version, as `major.minor`.
fn cache_release() -> String {
    format!(
        "{}.{}",
        env!("CARGO_PKG_VERSION_MAJOR"),
        env!("CARGO_PKG_VERSION_MINOR")
    )
}

/// Creates the cache directory under `dir` and returns its path, first
/// emptying it when `CACHE_FORMAT_FILE` is missing, cannot be read, or
/// holds anything but `cache_format()`: everything in the local
/// directory that does not match this build's format is treated as absent
/// (RFD 017), so a disk level is recovered only by a release of the minor
/// version that wrote it. The marker is written after the directory is
/// emptied and that emptying is synced, and the marker itself is written
/// whole or not at all (`write_marker`), so a crash at any point either
/// leaves the old marker over the old files or no marker, which empties the
/// directory again on the next open, and never a marker over files it does
/// not describe.
fn open_cache_dir(dir: &Path) -> Result<PathBuf> {
    let cache_dir = dir.join(CACHE_DIR);
    let marker = dir.join(CACHE_FORMAT_FILE);
    let format = cache_format();
    let ours = fs::read(&marker).is_ok_and(|held| held == format);
    if !ours {
        match fs::remove_dir_all(&cache_dir) {
            Ok(()) => tracing::info!(
                path = %cache_dir.display(),
                "page cache disk level emptied: written by another minor release"
            ),
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => return Err(local_error(cache_dir.clone(), error)),
        }
    }
    fs::create_dir_all(&cache_dir).map_err(|error| local_error(cache_dir.clone(), error))?;
    if !ours {
        sync_dir(dir)?;
        write_marker(dir, &format)?;
    }
    Ok(cache_dir)
}

/// Writes `format` to `CACHE_FORMAT_FILE` in `dir` so that a crash leaves
/// either the file it had or the whole of the new one: a temporary file
/// beside it, synced, renamed over it, then `dir` synced so the rename is
/// durable. A temporary file a crash left is rewritten here and never read.
fn write_marker(dir: &Path, format: &[u8]) -> Result<()> {
    let marker = dir.join(CACHE_FORMAT_FILE);
    let temp = dir.join(format!("{CACHE_FORMAT_FILE}.{TEMP_EXTENSION}"));
    let write = |temp: &Path| -> io::Result<()> {
        let mut file = File::create(temp)?;
        file.write_all(format)?;
        file.sync_all()
    };
    write(&temp).map_err(|error| local_error(temp.clone(), error))?;
    fs::rename(&temp, &marker).map_err(|error| local_error(marker, error))?;
    sync_dir(dir)
}

/// Syncs the directory at `dir`, which makes the entries created, removed,
/// or renamed in it durable.
fn sync_dir(dir: &Path) -> Result<()> {
    File::open(dir)
        .and_then(|handle| handle.sync_all())
        .map_err(|error| local_error(dir.to_path_buf(), error))
}

/// A failure of the local directory at `path` as the crate's error; the one
/// place `Error::Local` is built.
fn local_fault(path: PathBuf, fault: LocalFault) -> Error {
    Error::Local { path, fault }
}

/// An I/O failure at `path` as the crate's error.
fn local_error(path: PathBuf, error: io::Error) -> Error {
    local_fault(path, LocalFault::Io(error))
}

/// The one warn line for a chunk file `retain` deletes because it failed a
/// check: the path, the check, and the check's fields, so a reader knows
/// which file was deleted, why, and the value that failed.
fn warn_rejected(path: &Path, rejection: Rejection) {
    tracing::warn!(
        path = %path.display(),
        check = %rejection,
        detail = ?rejection,
        "chunk file rejected"
    );
}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::generation::testing::{TABLE, local_dir, refusal_of};
    use crate::local::format::{CHUNK_FILE_MAGIC, MAGIC_OFFSET, chunk_name};
    use crate::local::testing::{
        DATA_SIZE, DISK_CACHE_BYTES, FIRST_BYTES, FIRST_RANGE, INNER_BYTES, INNER_RANGE,
        ORDER_FIELD_ID, OTHER_PATH, SECOND_BYTES, SECOND_RANGE, UNDER, VERIFICATION, chunk_file,
        chunks, config, entries, identity, open, overwrite, payload_len, stale_temp, under_rules,
        write, write_for, write_under,
    };
    use crate::local::{
        CACHE_DIR, DataFileIdentity, INDEX_DIR, LocalDir, LocalFault, is_temp, remove_if_present,
    };

    /// What a leftover temporary file holds.
    const LEFTOVER: &[u8] = b"partial";
    /// The bytes a second writer of the same identity stores at
    /// `FIRST_RANGE`, distinguishable from `FIRST_BYTES`.
    const RIVAL_BYTES: &[u8] = b"ABCDEF";

    #[test]
    fn test_open_creates_index_and_cache_dirs() {
        let dir = tempfile::tempdir().unwrap();
        let (local, _) = open(dir.path());
        assert!(dir.path().join(INDEX_DIR).is_dir());
        assert!(dir.path().join(CACHE_DIR).is_dir());
        assert_eq!(local.index_dir, dir.path().join(INDEX_DIR));
        assert_eq!(local.cache_dir, dir.path().join(CACHE_DIR));
        assert_eq!(local.index_budget(), None);
        let again = LocalDir::open(
            &config(dir.path(), DISK_CACHE_BYTES),
            Arc::new(IoStats::default()),
        );
        assert!(again.is_ok(), "existing directories are reused");
    }

    #[test]
    fn test_open_marks_a_new_cache_dir_with_this_build_s_format() {
        let dir = tempfile::tempdir().unwrap();
        open(dir.path());
        let marker = fs::read(dir.path().join(CACHE_FORMAT_FILE)).unwrap();
        assert_eq!(marker, cache_format());
    }

    #[test]
    fn test_open_keeps_a_cache_dir_this_build_s_format_marks() {
        let dir = tempfile::tempdir().unwrap();
        open(dir.path());
        let page = dir.path().join(CACHE_DIR).join("page");
        fs::write(&page, LEFTOVER).unwrap();
        open(dir.path());
        assert_eq!(fs::read(&page).unwrap(), LEFTOVER);
    }

    #[test]
    fn test_open_empties_a_cache_dir_with_no_format_marker() {
        let dir = tempfile::tempdir().unwrap();
        let cache = dir.path().join(CACHE_DIR);
        fs::create_dir_all(&cache).unwrap();
        fs::write(cache.join("page"), LEFTOVER).unwrap();
        open(dir.path());
        assert_eq!(fs::read_dir(&cache).unwrap().count(), 0);
        let marker = fs::read(dir.path().join(CACHE_FORMAT_FILE)).unwrap();
        assert_eq!(marker, cache_format());
    }

    #[test]
    fn test_open_empties_a_cache_dir_another_minor_release_marks() {
        let dir = tempfile::tempdir().unwrap();
        open(dir.path());
        let cache = dir.path().join(CACHE_DIR);
        fs::write(cache.join("page"), LEFTOVER).unwrap();
        let next_minor: u32 = env!("CARGO_PKG_VERSION_MINOR").parse::<u32>().unwrap() + 1;
        let mut other = vec![CACHE_FORMAT_MAGIC];
        other.extend_from_slice(&PAGE_CACHE_DISK_BLOCK_BYTES.to_le_bytes());
        other.extend_from_slice(
            format!("{}.{next_minor}", env!("CARGO_PKG_VERSION_MAJOR")).as_bytes(),
        );
        fs::write(dir.path().join(CACHE_FORMAT_FILE), &other).unwrap();
        open(dir.path());
        assert_eq!(fs::read_dir(&cache).unwrap().count(), 0);
        let marker = fs::read(dir.path().join(CACHE_FORMAT_FILE)).unwrap();
        assert_eq!(marker, cache_format());
    }

    /// What a temporary marker a crash left holds: a torn prefix of the
    /// format.
    const TORN_MARKER: &[u8] = &[CACHE_FORMAT_MAGIC, 0];

    /// The temporary file `write_marker` writes in `dir`.
    fn marker_temp(dir: &Path) -> PathBuf {
        dir.join(format!("{CACHE_FORMAT_FILE}.{TEMP_EXTENSION}"))
    }

    #[test]
    fn test_write_marker_leaves_the_whole_format_and_no_temporary_file() {
        let dir = tempfile::tempdir().unwrap();
        write_marker(dir.path(), &cache_format()).unwrap();
        assert_eq!(
            fs::read(dir.path().join(CACHE_FORMAT_FILE)).unwrap(),
            cache_format()
        );
        assert!(!marker_temp(dir.path()).exists());
        let other = [CACHE_FORMAT_MAGIC];
        write_marker(dir.path(), &other).unwrap();
        assert_eq!(
            fs::read(dir.path().join(CACHE_FORMAT_FILE)).unwrap(),
            other,
            "a marker replaces the one before it whole"
        );
    }

    #[test]
    fn test_write_marker_failures_are_local_errors_naming_the_path() {
        let dir = tempfile::tempdir().unwrap();
        let missing = dir.path().join("absent");
        let Err(Error::Local {
            path,
            fault: LocalFault::Io(_),
        }) = write_marker(&missing, &cache_format())
        else {
            panic!("expected a local I/O error for the temporary file");
        };
        assert_eq!(path, marker_temp(&missing));

        let blocked = dir.path().join("blocked");
        fs::create_dir_all(blocked.join(CACHE_FORMAT_FILE).join("held")).unwrap();
        let Err(Error::Local {
            path,
            fault: LocalFault::Io(_),
        }) = write_marker(&blocked, &cache_format())
        else {
            panic!("expected a local I/O error for the rename");
        };
        assert_eq!(path, blocked.join(CACHE_FORMAT_FILE));
    }

    #[test]
    fn test_open_ignores_a_temporary_marker_a_crash_left() {
        let dir = tempfile::tempdir().unwrap();
        open(dir.path());
        let page = dir.path().join(CACHE_DIR).join("page");
        fs::write(&page, LEFTOVER).unwrap();
        fs::write(marker_temp(dir.path()), TORN_MARKER).unwrap();
        open(dir.path());
        assert_eq!(
            fs::read(&page).unwrap(),
            LEFTOVER,
            "a leftover temporary file does not empty a cache dir the marker vouches for"
        );
        assert_eq!(
            fs::read(dir.path().join(CACHE_FORMAT_FILE)).unwrap(),
            cache_format()
        );
    }

    #[test]
    fn test_open_rewrites_the_marker_over_a_temporary_one_a_crash_left() {
        let dir = tempfile::tempdir().unwrap();
        let cache = dir.path().join(CACHE_DIR);
        fs::create_dir_all(&cache).unwrap();
        fs::write(cache.join("page"), LEFTOVER).unwrap();
        fs::write(marker_temp(dir.path()), cache_format()).unwrap();
        open(dir.path());
        assert_eq!(
            fs::read_dir(&cache).unwrap().count(),
            0,
            "a complete temporary marker never renamed vouches for nothing"
        );
        assert_eq!(
            fs::read(dir.path().join(CACHE_FORMAT_FILE)).unwrap(),
            cache_format()
        );
        assert!(!marker_temp(dir.path()).exists());
    }

    #[test]
    fn test_cache_format_names_the_block_size_and_the_minor_release() {
        let format = cache_format();
        assert_eq!(format[0], CACHE_FORMAT_MAGIC);
        assert_eq!(&format[1..9], &PAGE_CACHE_DISK_BLOCK_BYTES.to_le_bytes());
        assert_eq!(&format[9..], cache_release().as_bytes());
    }

    #[test]
    fn test_cache_release_leaves_out_the_patch_version() {
        let version = env!("CARGO_PKG_VERSION");
        let patch = env!("CARGO_PKG_VERSION_PATCH");
        assert_eq!(
            cache_release(),
            version.strip_suffix(&format!(".{patch}")).unwrap()
        );
    }

    #[test]
    fn test_open_fails_typed_when_a_dir_cannot_be_created() {
        let dir = tempfile::tempdir().unwrap();
        let blocker = dir.path().join("blocker");
        fs::write(&blocker, LEFTOVER).unwrap();
        let result = LocalDir::open(
            &config(&blocker, DISK_CACHE_BYTES),
            Arc::new(IoStats::default()),
        );
        let Err(Error::Local {
            path,
            fault: LocalFault::Io(_),
        }) = result
        else {
            panic!("expected a local I/O error, got {result:?}");
        };
        assert_eq!(path, blocker.join(INDEX_DIR));
    }

    #[test]
    fn test_write_chunks_then_chunks_for_returns_some() {
        let dir = tempfile::tempdir().unwrap();
        let (local, _) = open(dir.path());
        let written = write(&local);
        assert_eq!(
            local.index_bytes_in_use().unwrap(),
            chunk_file_bytes(
                &identity(),
                UNDER,
                &[FIRST_RANGE, SECOND_RANGE],
                approx::count(VERIFICATION.len())
            )
        );
        let found = local.chunks_for(&identity()).unwrap().unwrap();
        assert_eq!(found.shared.spans, written.shared.spans);
        assert_eq!(found.shared.spans.len(), 2);
        assert_eq!(entries(&local.index_dir), vec![chunk_file(&local)]);
    }

    #[test]
    fn test_chunks_for_absent_is_none() {
        let dir = tempfile::tempdir().unwrap();
        let (local, _) = open(dir.path());
        assert!(local.chunks_for(&identity()).unwrap().is_none());
    }

    #[tokio::test]
    async fn test_chunks_for_after_restart_serves_the_same_bytes_without_a_write() {
        let dir = tempfile::tempdir().unwrap();
        let (first, _) = open(dir.path());
        write(&first);
        let path = chunk_file(&first);
        let before = fs::metadata(&path).unwrap();
        drop(first);

        let (second, stats) = open(dir.path());
        let disk = second.chunks_for(&identity()).unwrap().unwrap();
        assert_eq!(&disk.page(FIRST_RANGE).await.unwrap()[..], FIRST_BYTES);
        assert_eq!(&disk.page(SECOND_RANGE).await.unwrap()[..], SECOND_BYTES);
        assert_eq!(&disk.page(INNER_RANGE).await.unwrap()[..], INNER_BYTES);
        assert_eq!(stats.local_reads(), 3);
        assert_eq!(stats.gets(), 0);

        let after = fs::metadata(&path).unwrap();
        assert_eq!(after.len(), before.len());
        assert_eq!(
            after.modified().unwrap(),
            before.modified().unwrap(),
            "nothing wrote the chunk file again"
        );
        assert_eq!(entries(&second.index_dir), vec![path]);
    }

    #[test]
    fn test_retain_with_empty_live_set_deletes_chunk_and_temp_files() {
        let dir = tempfile::tempdir().unwrap();
        let (local, _) = open(dir.path());
        write(&local);
        let path = chunk_file(&local);
        let temp = stale_temp(&path);
        fs::write(&temp, LEFTOVER).unwrap();
        let chunk_len = fs::metadata(&path).unwrap().len();
        let temp_len = approx::count(LEFTOVER.len());

        let freed = local.retain(&HashSet::new()).unwrap();
        assert_eq!(freed, chunk_len + temp_len);
        assert!(entries(&local.index_dir).is_empty());
        assert!(local.chunks_for(&identity()).unwrap().is_none());
    }

    #[test]
    fn test_retain_with_the_identity_live_deletes_nothing() {
        let dir = tempfile::tempdir().unwrap();
        let (local, _) = open(dir.path());
        write(&local);
        let path = chunk_file(&local);
        let before = fs::metadata(&path).unwrap();

        let freed = local
            .retain(&HashSet::from([(identity(), ChunkRules::KeyValue)]))
            .unwrap();
        assert_eq!(freed, 0);
        let after = fs::metadata(&path).unwrap();
        assert_eq!(after.len(), before.len());
        assert_eq!(after.modified().unwrap(), before.modified().unwrap());
        assert!(local.chunks_for(&identity()).unwrap().is_some());
    }

    #[test]
    fn test_retain_deletes_a_file_that_fails_a_check() {
        let dir = tempfile::tempdir().unwrap();
        let (local, _) = open(dir.path());
        write(&local);
        let path = chunk_file(&local);
        overwrite(&path, MAGIC_OFFSET, &[CHUNK_FILE_MAGIC.wrapping_add(1)]);
        let len = fs::metadata(&path).unwrap().len();

        let freed = local
            .retain(&HashSet::from([(identity(), ChunkRules::KeyValue)]))
            .unwrap();
        assert_eq!(freed, len);
        assert!(entries(&local.index_dir).is_empty());
    }

    #[test]
    fn test_retain_keeps_the_live_file_and_deletes_the_dropped_one() {
        let dir = tempfile::tempdir().unwrap();
        let (local, _) = open(dir.path());
        write(&local);
        let dropped = DataFileIdentity {
            path: OTHER_PATH.to_string(),
            ..identity()
        };
        write_for(&local, &dropped, chunks().into_iter().take(1).collect());
        let dropped_len = fs::metadata(local.chunk_path(&dropped, ChunkRules::KeyValue))
            .unwrap()
            .len();
        assert_eq!(entries(&local.index_dir).len(), 2);

        let freed = local
            .retain(&HashSet::from([(identity(), ChunkRules::KeyValue)]))
            .unwrap();
        assert_eq!(freed, dropped_len);
        assert_eq!(entries(&local.index_dir), vec![chunk_file(&local)]);
        assert!(local.chunks_for(&identity()).unwrap().is_some());
        assert!(local.chunks_for(&dropped).unwrap().is_none());
    }

    #[test]
    fn test_retain_keeps_a_temp_file_a_writer_has_open_and_deletes_a_leftover() {
        let dir = tempfile::tempdir().unwrap();
        let (local, _) = open(dir.path());
        let path = chunk_file(&local);
        let open_write = local.begin_temp(&path);
        assert!(is_temp(&open_write.path));
        fs::write(&open_write.path, LEFTOVER).unwrap();
        let leftover = stale_temp(&path);
        fs::write(&leftover, LEFTOVER).unwrap();
        let leftover_len = approx::count(LEFTOVER.len());

        assert_eq!(local.retain(&HashSet::new()).unwrap(), leftover_len);
        assert_eq!(entries(&local.index_dir), vec![open_write.path.clone()]);

        drop(open_write);
        assert_eq!(local.retain(&HashSet::new()).unwrap(), leftover_len);
        assert!(entries(&local.index_dir).is_empty());
    }

    #[test]
    fn test_begin_temp_names_differ_per_call() {
        let dir = tempfile::tempdir().unwrap();
        let (local, _) = open(dir.path());
        let path = chunk_file(&local);
        let first = local.begin_temp(&path);
        let second = local.begin_temp(&path);
        assert_ne!(first.path, second.path);
        assert_eq!(first.path.parent(), Some(local.index_dir.as_path()));
        assert!(is_temp(&first.path) && is_temp(&second.path));
        assert_eq!(local.lock_in_flight().len(), 2);
        drop(first);
        drop(second);
        assert!(local.lock_in_flight().is_empty());
    }

    #[test]
    fn test_remove_if_present_of_a_missing_path_frees_nothing() {
        let dir = tempfile::tempdir().unwrap();
        let (local, _) = open(dir.path());
        let path = chunk_file(&local);
        assert_eq!(remove_if_present(&path, DATA_SIZE).unwrap(), 0);
        fs::write(&path, LEFTOVER).unwrap();
        assert_eq!(remove_if_present(&path, DATA_SIZE).unwrap(), DATA_SIZE);
        assert!(!path.exists());
    }

    #[test]
    fn test_retain_deletes_a_file_not_named_by_its_header() {
        let dir = tempfile::tempdir().unwrap();
        let (local, _) = open(dir.path());
        write(&local);
        let path = chunk_file(&local);
        let misnamed = local
            .index_dir
            .join(chunk_name(OTHER_PATH, ChunkRules::KeyValue));
        fs::copy(&path, &misnamed).unwrap();
        let len = fs::metadata(&misnamed).unwrap().len();

        let freed = local
            .retain(&HashSet::from([(identity(), ChunkRules::KeyValue)]))
            .unwrap();
        assert_eq!(freed, len);
        assert_eq!(entries(&local.index_dir), vec![path]);
    }

    #[test]
    fn test_chunk_path_differs_by_rules_for_one_data_file() {
        let dir = tempfile::tempdir().unwrap();
        let (local, _) = open(dir.path());
        let id = identity();
        let kv = local.chunk_path(&id, ChunkRules::KeyValue);
        let ordered = local.chunk_path(
            &id,
            ChunkRules::Ordered {
                order_field_id: ORDER_FIELD_ID,
            },
        );
        let other = local.chunk_path(
            &id,
            ChunkRules::Ordered {
                order_field_id: ORDER_FIELD_ID + 1,
            },
        );
        assert_ne!(kv, ordered);
        assert_ne!(ordered, other);
        assert_ne!(kv, other);
    }

    #[test]
    fn test_lookup_rejects_a_file_whose_header_names_other_rules() {
        let dir = tempfile::tempdir().unwrap();
        let (local, _) = open(dir.path());
        let ordered = ChunkRules::Ordered {
            order_field_id: ORDER_FIELD_ID,
        };
        write_under(&local, &identity(), under_rules(ordered), chunks());
        fs::rename(
            local.chunk_path(&identity(), ordered),
            local.chunk_path(&identity(), ChunkRules::KeyValue),
        )
        .unwrap();

        assert!(matches!(
            local.lookup(&identity(), ChunkRules::KeyValue).unwrap(),
            ChunkFileLookup::Rejected(Rejection::NameMismatch)
        ));
    }

    #[test]
    fn test_lookup_finds_each_kind_of_one_data_file_under_its_own_name() {
        let dir = tempfile::tempdir().unwrap();
        let (local, _) = open(dir.path());
        let ordered = ChunkRules::Ordered {
            order_field_id: ORDER_FIELD_ID,
        };
        write(&local);
        write_under(&local, &identity(), under_rules(ordered), chunks());

        for rules in [ChunkRules::KeyValue, ordered] {
            let ChunkFileLookup::Found(disk) = local.lookup(&identity(), rules).unwrap() else {
                panic!("no chunk file found under {rules:?}");
            };
            assert_eq!(disk.verified_under().rules, rules);
        }
        let other = ChunkRules::Ordered {
            order_field_id: ORDER_FIELD_ID + 1,
        };
        assert!(matches!(
            local.lookup(&identity(), other).unwrap(),
            ChunkFileLookup::Absent
        ));
    }

    #[test]
    fn test_retain_keeps_both_kinds_of_one_data_file() {
        let dir = tempfile::tempdir().unwrap();
        let (local, _) = open(dir.path());
        let ordered = ChunkRules::Ordered {
            order_field_id: ORDER_FIELD_ID,
        };
        write(&local);
        write_under(&local, &identity(), under_rules(ordered), chunks());
        let ordered_path = local.chunk_path(&identity(), ordered);
        let ordered_len = fs::metadata(&ordered_path).unwrap().len();
        assert!(ordered_len > 0);
        assert_eq!(entries(&local.index_dir).len(), 2);

        let both = HashSet::from([(identity(), ChunkRules::KeyValue), (identity(), ordered)]);
        assert_eq!(local.retain(&both).unwrap(), 0);
        assert_eq!(entries(&local.index_dir).len(), 2);

        let key_value_only = HashSet::from([(identity(), ChunkRules::KeyValue)]);
        assert_eq!(local.retain(&key_value_only).unwrap(), ordered_len);
        assert_eq!(entries(&local.index_dir), vec![chunk_file(&local)]);
    }

    #[test]
    fn test_index_bytes_in_use_is_the_sum_of_the_files() {
        let dir = tempfile::tempdir().unwrap();
        let (local, _) = open(dir.path());
        assert_eq!(local.index_bytes_in_use().unwrap(), 0);
        write(&local);
        let other = DataFileIdentity {
            path: OTHER_PATH.to_string(),
            ..identity()
        };
        write_for(&local, &other, chunks().into_iter().take(1).collect());
        let expected: u64 = entries(&local.index_dir)
            .iter()
            .map(|path| fs::metadata(path).unwrap().len())
            .sum();
        assert_eq!(entries(&local.index_dir).len(), 2);
        assert_eq!(local.index_bytes_in_use().unwrap(), expected);
        // A write in progress is bytes on disk too.
        let temp = stale_temp(&chunk_file(&local));
        fs::write(&temp, LEFTOVER).unwrap();
        assert_eq!(
            local.index_bytes_in_use().unwrap(),
            expected + approx::count(LEFTOVER.len())
        );
    }

    /// Two disk-tier loads that each fit the budget beside what `index/`
    /// holds, but not together: the second is refused while the first holds
    /// its reservation, and admitted once the first drops it.
    #[test]
    fn test_reserve_refuses_what_fits_alone_but_not_beside_an_outstanding_reservation() {
        let dir = tempfile::tempdir().unwrap();
        let (unbounded, stats) = open(dir.path());
        write(&unbounded);
        let in_use = unbounded.index_bytes_in_use().unwrap();
        let one_load = payload_len();
        let budget = in_use + one_load;
        let local = local_dir(dir.path(), stats, Some(budget));

        let first = local.reserve(TABLE, one_load).unwrap();
        assert_eq!(
            refusal_of(local.reserve(TABLE, one_load)),
            RefusalReason::OverBudget {
                of: Budget::Index,
                needed: in_use + 2 * one_load,
                budget,
            }
        );
        assert!(
            local.reserve(TABLE, 0).is_ok(),
            "the budget is full, not exceeded"
        );
        drop(first);
        let second = local.reserve(TABLE, one_load).unwrap();
        assert_eq!(
            refusal_of(local.reserve(TABLE, 1)),
            RefusalReason::OverBudget {
                of: Budget::Index,
                needed: budget + 1,
                budget,
            }
        );
        drop(second);
    }

    /// A release frees part of a hold for the next reserve, never more than
    /// the hold, and the drop frees only what is left.
    #[test]
    fn test_release_frees_part_of_a_hold_and_drop_frees_the_rest() {
        let dir = tempfile::tempdir().unwrap();
        let (_, stats) = open(dir.path());
        let local = local_dir(dir.path(), stats, Some(10));
        let hold = local.reserve(TABLE, 10).unwrap();
        hold.release(4);
        let freed = local.reserve(TABLE, 4).unwrap();
        assert!(local.reserve(TABLE, 1).is_err());
        drop(freed);
        hold.release(100);
        let all = local.reserve(TABLE, 10).unwrap();
        drop((hold, all));
        assert_eq!(*local.lock_reserved(), 0);
    }

    #[test]
    fn test_page_cache_level_none_at_zero_budget() {
        let dir = tempfile::tempdir().unwrap();
        let local = LocalDir::open(&config(dir.path(), 0), Arc::new(IoStats::default())).unwrap();
        assert!(local.page_cache_level().is_none());
    }

    #[test]
    fn test_page_cache_level_some_with_the_cache_dir_otherwise() {
        let dir = tempfile::tempdir().unwrap();
        let (local, _) = open(dir.path());
        let level = local.page_cache_level().unwrap();
        assert_eq!(level.dir(), dir.path().join(CACHE_DIR));
        assert_eq!(level.capacity_bytes(), DISK_CACHE_BYTES);
        assert!(level.dir().is_dir());
    }

    #[tokio::test]
    async fn test_write_chunks_concurrently_for_one_identity_each_handle_reads_its_own_bytes() {
        let dir = tempfile::tempdir().unwrap();
        let (local, _) = open(dir.path());
        let rival = vec![(FIRST_RANGE, Bytes::from_static(RIVAL_BYTES))];
        let (first, second) = std::thread::scope(|scope| {
            let first = scope.spawn(|| {
                local.write_chunks(&identity(), UNDER, VERIFICATION, chunks().into_iter())
            });
            let second = scope
                .spawn(|| local.write_chunks(&identity(), UNDER, VERIFICATION, rival.into_iter()));
            (first.join().unwrap(), second.join().unwrap())
        });
        let first = first.unwrap();
        let second = second.unwrap();
        assert_eq!(&first.page(FIRST_RANGE).await.unwrap()[..], FIRST_BYTES);
        assert_eq!(&second.page(FIRST_RANGE).await.unwrap()[..], RIVAL_BYTES);

        assert_eq!(entries(&local.index_dir), vec![chunk_file(&local)]);
        let landed = local.chunks_for(&identity()).unwrap().unwrap();
        let bytes = landed.page(FIRST_RANGE).await.unwrap();
        assert!(&bytes[..] == FIRST_BYTES || &bytes[..] == RIVAL_BYTES);
        assert!(local.lock_in_flight().is_empty());
    }
}
