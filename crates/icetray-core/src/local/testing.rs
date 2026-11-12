//! The chunk-file fixtures the local directory's tests share: one data-file
//! identity, its chunks, and the edits that make a chunk file fail a check,
//! some of which the index's tests make on the chunk files it writes.

use std::fs::{self, OpenOptions};
use std::ops::Range;
use std::os::unix::fs::FileExt as _;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use bytes::Bytes;
use prost::Message as _;

use crate::approx;
use crate::config::LocalConfig;
use crate::key::KeyType;
use crate::local::format::{
    CHUNK_FILE_VERSION, ChunkFileHeader, HEADER_OFFSET, LENGTH_OFFSET, LENGTH_PREFIX_BYTES,
    VERSION_OFFSET,
};
use crate::local::{
    ChunkRules, DIGEST_BYTES, DataFileIdentity, DiskChunks, LocalDir, TEMP_EXTENSION, VerifiedUnder,
};
use crate::storage::IoStats;

/// A synthetic data file path; the chunk file's name is its digest.
pub(super) const DATA_PATH: &str = "s3://example-warehouse/db/kv/data/part-00000.parquet";
/// A second synthetic path, so a second chunk file has another name.
pub(super) const OTHER_PATH: &str = "s3://example-warehouse/db/kv/data/part-00001.parquet";
/// The size the fixture identity records: a contract-sized data file.
pub(super) const DATA_SIZE: u64 = 40_000_000;
/// The footer digest the fixture identity carries; any 32 bytes.
pub(super) const FOOTER_DIGEST: [u8; DIGEST_BYTES] = [7; DIGEST_BYTES];
/// The first stored chunk's range in the data file.
pub(super) const FIRST_RANGE: Range<u64> = 100..106;
/// The first stored chunk's bytes.
pub(super) const FIRST_BYTES: &[u8] = b"abcdef";
/// The second stored chunk's range, with a gap after the first.
pub(super) const SECOND_RANGE: Range<u64> = 200..204;
/// The second stored chunk's bytes.
pub(super) const SECOND_BYTES: &[u8] = b"wxyz";
/// A range strictly inside the first chunk.
pub(super) const INNER_RANGE: Range<u64> = 102..105;
/// The bytes `INNER_RANGE` covers.
pub(super) const INNER_BYTES: &[u8] = b"cde";
/// A page cache disk bound above zero, so `page_cache_level` is `Some`.
pub(super) const DISK_CACHE_BYTES: u64 = 1024;
/// The empty range at the first chunk's end.
pub(super) const EMPTY_AT_FIRST_END: Range<u64> = 106..106;
/// The verification context the fixture files are written under; any
/// context, since the local directory stores it and never interprets it.
pub(super) const UNDER: VerifiedUnder = VerifiedUnder {
    key_type: KeyType::Long,
    verifier_version: 1,
    rules: ChunkRules::KeyValue,
};
/// The order column's field id in the ordered rules the fixtures write
/// under; any id, since the local directory stores it and names a file by
/// it and never interprets it.
pub(super) const ORDER_FIELD_ID: i32 = 1;
/// The verification record the fixture files carry; any bytes, since the
/// local directory frames the record and never reads it.
pub(super) const VERIFICATION: &[u8] = b"verified";
/// The chunk file version before this build's, which a file written by an
/// earlier build carries.
pub(crate) const PREVIOUS_VERSION: u8 = CHUNK_FILE_VERSION - 1;

pub(super) fn identity() -> DataFileIdentity {
    DataFileIdentity {
        path: DATA_PATH.to_string(),
        size: DATA_SIZE,
        footer_digest: FOOTER_DIGEST,
    }
}

/// `INNER_RANGE` with its ends swapped: inside the first chunk, but
/// asking for nothing a span can hold.
pub(super) fn inverted_range() -> Range<u64> {
    Range {
        start: INNER_RANGE.end,
        end: INNER_RANGE.start,
    }
}

pub(super) fn config(dir: &Path, page_cache_disk_bytes: u64) -> LocalConfig {
    LocalConfig {
        dir: dir.to_path_buf(),
        index_budget_bytes: None,
        page_cache_disk_bytes,
    }
}

/// A local directory over `dir` with a fresh counter.
pub(super) fn open(dir: &Path) -> (LocalDir, Arc<IoStats>) {
    let stats = Arc::new(IoStats::default());
    let local = LocalDir::open(&config(dir, DISK_CACHE_BYTES), Arc::clone(&stats)).unwrap();
    (local, stats)
}

pub(super) fn chunks() -> Vec<(Range<u64>, Bytes)> {
    vec![
        (FIRST_RANGE, Bytes::from_static(FIRST_BYTES)),
        (SECOND_RANGE, Bytes::from_static(SECOND_BYTES)),
    ]
}

/// The bytes of the fixture chunks together.
pub(super) fn chunk_bytes() -> u64 {
    approx::count(FIRST_BYTES.len() + SECOND_BYTES.len())
}

/// The bytes of the fixture file's payload: the record and the chunks.
pub(super) fn payload_len() -> u64 {
    approx::count(VERIFICATION.len()) + chunk_bytes()
}

pub(super) fn range_len(range: &Range<u64>) -> u64 {
    range.end - range.start
}

/// Writes the fixture chunks for the fixture identity.
pub(super) fn write(local: &LocalDir) -> DiskChunks {
    write_for(local, &identity(), chunks())
}

/// Writes `chunks` for `identity` with the fixture record and context.
pub(super) fn write_for(
    local: &LocalDir,
    identity: &DataFileIdentity,
    chunks: Vec<(Range<u64>, Bytes)>,
) -> DiskChunks {
    write_under(local, identity, UNDER, chunks)
}

/// Writes `chunks` for `identity` with the fixture record under `under`.
pub(super) fn write_under(
    local: &LocalDir,
    identity: &DataFileIdentity,
    under: VerifiedUnder,
    chunks: Vec<(Range<u64>, Bytes)>,
) -> DiskChunks {
    local
        .write_chunks(identity, under, VERIFICATION, chunks.into_iter())
        .unwrap()
}

/// The fixture context with `rules`.
pub(super) fn under_rules(rules: ChunkRules) -> VerifiedUnder {
    VerifiedUnder { rules, ..UNDER }
}

/// The key-value chunk file of the fixture identity.
pub(super) fn chunk_file(local: &LocalDir) -> PathBuf {
    local.chunk_path(&identity(), ChunkRules::KeyValue)
}

/// Overwrites the bytes at `offset` of the file at `path` with `bytes`.
pub(crate) fn overwrite(path: &Path, offset: u64, bytes: &[u8]) {
    OpenOptions::new()
        .write(true)
        .open(path)
        .unwrap()
        .write_all_at(bytes, offset)
        .unwrap();
}

pub(super) fn entries(dir: &Path) -> Vec<PathBuf> {
    let mut entries: Vec<PathBuf> = fs::read_dir(dir)
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .collect();
    entries.sort();
    entries
}

/// The path of a temporary file no writer in this process has open beside
/// the chunk file at `path`: what a process that died mid-write leaves
/// behind.
pub(crate) fn stale_temp(path: &Path) -> PathBuf {
    path.with_extension(TEMP_EXTENSION)
}

/// Overwrites the version byte of the chunk file at `path`.
pub(crate) fn set_version_byte(path: &Path, version: u8) {
    overwrite(path, VERSION_OFFSET, &[version]);
}

/// Overwrites every byte of the encoded header of the chunk file at `path`
/// with `byte`, keeping its length prefix.
pub(crate) fn fill_header(path: &Path, byte: u8) {
    let (header, _, _) = split(path);
    let garbage = vec![byte; header.encoded_len()];
    overwrite(path, HEADER_OFFSET, &garbage);
}

/// Rewrites the chunk file at `path` with its header's verifier version
/// set to `version`, its checksum still matching.
pub(crate) fn set_verifier_version(path: &Path, version: u32) {
    rewrite(path, |header, _| header.verifier_version = version);
}

/// Rewrites the chunk file at `path` with its verification record edited
/// by `edit`, the header's record length and payload checksum recomputed,
/// so the file passes every check of the local directory and only the
/// record's reader can tell.
pub(crate) fn edit_verification(path: &Path, edit: impl FnOnce(&mut Vec<u8>)) {
    rewrite(path, |_, verification| edit(verification));
}

/// Drops the pages of the file at `path` from the operating system's page
/// cache, written back first so that none is dirty and each can be dropped.
#[cfg(target_os = "linux")]
pub(crate) fn evict(path: &Path) {
    let file = fs::File::open(path).unwrap();
    file.sync_all().unwrap();
    // Offset 0 and no length cover the whole file.
    rustix::fs::fadvise(&file, 0, None, rustix::fs::Advice::DontNeed).unwrap();
}

/// Where the key chunks of the chunk file at `path` begin: past its
/// envelope, header, and verification record.
pub(crate) fn chunks_offset(path: &Path) -> u64 {
    let (_, _, chunks) = split(path);
    fs::metadata(path).unwrap().len() - approx::count(chunks.len())
}

/// The header, the verification record, and the chunks of the chunk file
/// at `path`, which this build wrote.
fn split(path: &Path) -> (ChunkFileHeader, Vec<u8>, Vec<u8>) {
    let bytes = fs::read(path).unwrap();
    let at = |offset: u64| approx::length(offset);
    let prefix: [u8; LENGTH_PREFIX_BYTES] = bytes[at(LENGTH_OFFSET)..at(HEADER_OFFSET)]
        .try_into()
        .unwrap();
    let header_end = at(HEADER_OFFSET) + approx::length(u32::from_le_bytes(prefix));
    let header = ChunkFileHeader::decode(&bytes[at(HEADER_OFFSET)..header_end]).unwrap();
    let record_end = header_end + approx::length(header.verification_length);
    (
        header,
        bytes[header_end..record_end].to_vec(),
        bytes[record_end..].to_vec(),
    )
}

/// Rewrites the chunk file at `path` with `edit` applied to its header and
/// verification record, then the record's length and the payload checksum
/// set to match what the file then holds.
fn rewrite(path: &Path, edit: impl FnOnce(&mut ChunkFileHeader, &mut Vec<u8>)) {
    let (mut header, mut verification, chunks) = split(path);
    edit(&mut header, &mut verification);
    header.verification_length = approx::count(verification.len());
    header.payload_crc32c = Some(crc32c::crc32c_append(
        crc32c::crc32c(&verification),
        &chunks,
    ));
    let encoded = header.encode_to_vec();
    let len = u32::try_from(encoded.len()).unwrap().to_le_bytes();
    let envelope = fs::read(path).unwrap()[..approx::length(LENGTH_OFFSET)].to_vec();
    fs::write(
        path,
        [&envelope[..], &len, &encoded, &verification, &chunks].concat(),
    )
    .unwrap();
}
