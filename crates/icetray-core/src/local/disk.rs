//! The read side of the local directory: positioned, counted reads of the
//! pages a chunk file holds, and the page cache's disk level (RFD 017).

use std::fs::File;
use std::ops::Range;
use std::os::unix::fs::FileExt as _;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use bytes::Bytes;

use super::format::{ParsedChunkFile, Rejection, StoredSpan};
use super::residency::{self, ResidentPages, Unobserved};
use super::{LocalFault, VerifiedUnder, local_fault};
use crate::approx;
use crate::blocking::on_blocking_pool;
use crate::error::{Error, Result};
use crate::storage::IoStats;

/// The key chunks of one data file, with an ordered table's order chunks
/// beside them, read from its chunk file on demand.
/// Clone-cheap: one handle is shared by the index and every read in
/// flight. It is not a `ChunkReader`; the index wraps the bytes a page read
/// returns in a `Segments`.
#[derive(Clone, Debug)]
pub struct DiskChunks {
    pub(super) shared: Arc<ChunkFile>,
}

/// The read size of the checksum pass over a reused chunk file's chunks: a
/// mebibyte, large enough that the sequential read runs at the disk's
/// throughput and the checksum at the processor's, small enough that the
/// pass holds one buffer of it however large the file.
const CHECKSUM_READ_BYTES: u64 = 1024 * 1024;

/// What every clone of a `DiskChunks` shares: the open file, the span
/// table, what the header says of the payload, and the counter.
#[derive(Debug)]
pub(super) struct ChunkFile {
    /// Read only through positioned reads, so the handle has no cursor to
    /// share. It is mapped only `PROT_NONE`, for as long as a residency
    /// sample takes, to ask the kernel which pages it holds
    /// (`DiskChunks::resident`); no byte is ever read through a map.
    file: File,
    /// For errors.
    path: PathBuf,
    /// Ascending by data offset, so a page's span is found by binary search.
    pub(super) spans: Vec<StoredSpan>,
    /// What the verification record was computed under.
    under: VerifiedUnder,
    /// Where the payload, and with it the verification record, starts.
    payload_offset: u64,
    /// The verification record's length.
    verification_length: u64,
    /// The payload's checksum as the header carries it.
    payload_crc32c: u32,
    /// Where the payload ends: the file's length when it was parsed.
    end: u64,
    stats: Arc<IoStats>,
}

impl DiskChunks {
    pub(super) fn new(
        file: File,
        path: PathBuf,
        parsed: ParsedChunkFile,
        stats: Arc<IoStats>,
    ) -> Self {
        Self {
            shared: Arc::new(ChunkFile {
                file,
                path,
                spans: parsed.spans,
                under: parsed.under,
                payload_offset: parsed.payload_offset,
                verification_length: parsed.verification_length,
                payload_crc32c: parsed.payload_crc32c,
                end: parsed.end,
                stats,
            }),
        }
    }

    /// The bytes of `range` of the data file, from the one stored chunk
    /// that holds the whole range; a range no chunk holds whole is
    /// `LocalFault::RangeNotResident` and costs nothing. The read is
    /// positioned and counts as one local read of the range's length
    /// (RFD 017), counted before it is issued as GETs are, wherever it runs.
    ///
    /// No worker waits for data from the disk (RFD 017): a read the
    /// operating system's page cache can serve without blocking may run on
    /// the calling worker, and a read that would block runs on the blocking
    /// pool. On Linux the read is first tried on the worker with
    /// `RWF_NOWAIT` (`ChunkFile::read_cached`), and goes to the blocking
    /// pool when that attempt does not return the whole range; on other
    /// systems it goes to the blocking pool.
    pub(crate) async fn page(&self, range: Range<u64>) -> Result<Bytes> {
        let Range { start, end } = range;
        let (file_offset, len) = self.shared.locate(start, end).ok_or_else(|| {
            self.shared
                .error(LocalFault::RangeNotResident { start, end })
        })?;
        self.shared.stats.record_local_read(len);
        #[cfg(target_os = "linux")]
        if let Some(bytes) = self.shared.read_cached(file_offset, len) {
            return Ok(bytes);
        }
        let shared = Arc::clone(&self.shared);
        on_blocking_pool(move || shared.read_at(file_offset, len))
            .await
            .map_err(|canceled| self.shared.error(LocalFault::Join(canceled)))?
    }

    /// What the operating system's page cache holds of the file's chunks,
    /// every page from the first chunk's to the file's end (RFD 020): an
    /// ordered table's order chunks with its key chunks, since every Scan
    /// reads both (RFD 025). Synchronous system calls, for the blocking
    /// pool, which read no byte of the file.
    pub(crate) fn resident(&self) -> std::result::Result<ResidentPages, Unobserved> {
        let shared = &self.shared;
        residency::resident(&shared.file, &shared.path, shared.chunks()).map_err(|why| Unobserved {
            path: shared.path.clone(),
            why,
        })
    }

    /// The chunk file's path under `index/`.
    pub(crate) fn path(&self) -> &Path {
        &self.shared.path
    }

    /// Where the chunks, key and order alike, lie in the chunk file; the
    /// range ends at the file's length when it was parsed.
    pub(crate) fn chunks(&self) -> Range<u64> {
        self.shared.chunks()
    }

    /// What the file's verification record was computed under, as its
    /// header says.
    pub(crate) fn verified_under(&self) -> VerifiedUnder {
        self.shared.under
    }

    /// Whether the stored chunks are exactly `ranges`, in any order: what
    /// the index checks before it trusts a reused file, since the identity
    /// does not say which column's chunks the file holds, and the
    /// verification record describes those chunks and no others.
    pub(crate) fn stores_exactly(&self, ranges: &[Range<u64>]) -> bool {
        let mut wanted: Vec<(u64, u64)> = ranges
            .iter()
            .map(|range| (range.start, range.end.saturating_sub(range.start)))
            .collect();
        wanted.sort_unstable();
        let stored = self
            .shared
            .spans
            .iter()
            .map(|span| (span.data_offset, span.length));
        wanted.into_iter().eq(stored)
    }

    /// The verification record, once the crc32c of the whole payload (the
    /// record and every chunk) matches the header's: one sequential pass
    /// over the file after the header, the chunks in reads of
    /// `CHECKSUM_READ_BYTES`, and no page decoded. `Err` is the check the
    /// file failed: `Rejection::VerificationTooLarge` for a record longer
    /// than `most`, checked before anything is read or allocated, so a
    /// corrupt length cannot ask for gigabytes; `Rejection::ShortPayload`
    /// for a file that ends early, cut short since it was opened; and
    /// `Rejection::ChecksumMismatch`. The pass validates the file, as the
    /// header reads do, and is not a local read in the sense of RFD 003; it
    /// is synchronous file I/O, for the blocking pool. Only an I/O failure
    /// is an error.
    pub(crate) fn checked_verification(
        &self,
        most: u64,
    ) -> Result<std::result::Result<Vec<u8>, Rejection>> {
        let shared = &self.shared;
        let len = shared.verification_length;
        if len > most {
            return Ok(Err(Rejection::VerificationTooLarge { len, most }));
        }
        let mut record = vec![0u8; approx::length(len)];
        if !shared.read_exact_or_eof(&mut record, shared.payload_offset)? {
            return Ok(Err(Rejection::ShortPayload));
        }
        let mut crc = crc32c::crc32c(&record);
        let mut offset = shared.payload_offset + len;
        let mut buf = Vec::new();
        while offset < shared.end {
            let take = (shared.end - offset).min(CHECKSUM_READ_BYTES);
            buf.resize(approx::length(take), 0);
            if !shared.read_exact_or_eof(&mut buf, offset)? {
                return Ok(Err(Rejection::ShortPayload));
            }
            crc = crc32c::crc32c_append(crc, &buf);
            offset += take;
        }
        Ok(if crc == shared.payload_crc32c {
            Ok(record)
        } else {
            Err(Rejection::ChecksumMismatch)
        })
    }
}

impl ChunkFile {
    /// Where the chunks lie in the chunk file: past the verification
    /// record, to the file's end.
    fn chunks(&self) -> Range<u64> {
        self.payload_offset + self.verification_length..self.end
    }

    /// The chunk-file offset and the length of `start..end`, when one span
    /// holds the whole range; `None` for a range that straddles spans, falls
    /// outside them all, is empty, or is inverted.
    fn locate(&self, start: u64, end: u64) -> Option<(u64, u64)> {
        let len = end.checked_sub(start).filter(|len| *len > 0)?;
        let after = self.spans.partition_point(|span| span.data_offset <= start);
        let span = self.spans.get(after.checked_sub(1)?)?;
        let within = start - span.data_offset;
        if within.checked_add(len)? > span.length {
            return None;
        }
        Some((span.file_offset + within, len))
    }

    /// Fills `buf` from `offset` of the chunk file by positioned read;
    /// `false` when the file ends first.
    fn read_exact_or_eof(&self, buf: &mut [u8], offset: u64) -> Result<bool> {
        match self.file.read_exact_at(buf, offset) {
            Ok(()) => Ok(true),
            Err(error) if error.kind() == std::io::ErrorKind::UnexpectedEof => Ok(false),
            Err(error) => Err(self.error(LocalFault::Io(error))),
        }
    }

    /// `len` bytes at `offset` of the chunk file by positioned read: no seek
    /// on the shared handle, and never through a map.
    fn read_at(&self, offset: u64, len: u64) -> Result<Bytes> {
        // A span's length is bounded by the chunk file's size, which
        // `parse_chunk_file` checked against the file on disk.
        let len = approx::length(len);
        let mut buf = vec![0u8; len];
        self.file
            .read_exact_at(&mut buf, offset)
            .map_err(|error| self.error(LocalFault::Io(error)))?;
        Ok(Bytes::from(buf))
    }

    /// `len` bytes at `offset` of the chunk file when the operating
    /// system's page cache holds all of them, read without blocking by
    /// `preadv2` with `RWF_NOWAIT`, so the calling worker can run it.
    /// `None` when it cannot: the kernel returns `EAGAIN` when the range's
    /// first page is not cached, fewer bytes than asked when a later one is
    /// not, and `EOPNOTSUPP` when the kernel (before 4.14) or the file's
    /// filesystem (overlayfs, as of Linux 6.8) does not support the flag.
    /// A `None` is left to `read_at` on the blocking pool, which meets and
    /// reports a real I/O error itself.
    ///
    /// A cached read can still update the file's access time, which a
    /// journaling filesystem may write through its journal and wait on;
    /// under `relatime` the kernel does so on the first read after the file
    /// is written and then at most once a day, and on a directory mounted
    /// `noatime` not at all.
    #[cfg(target_os = "linux")]
    fn read_cached(&self, offset: u64, len: u64) -> Option<Bytes> {
        use rustix::io::{ReadWriteFlags, preadv2};
        let mut buf = vec![0u8; approx::length(len)];
        let read = preadv2(
            &self.file,
            &mut [std::io::IoSliceMut::new(&mut buf)],
            offset,
            ReadWriteFlags::NOWAIT,
        )
        .ok()?;
        (read == buf.len()).then(|| Bytes::from(buf))
    }

    /// `fault` at this chunk file's path.
    fn error(&self, fault: LocalFault) -> Error {
        local_fault(self.path.clone(), fault)
    }
}

/// The page cache's disk level (RFD 017): the `cache/` directory, its size
/// bound, and the counter a page served from it is charged to, handed to
/// `PageCache` by `LocalDir::page_cache_level`. Pages there are keyed by
/// file and offset and evicted by size alone, never for staleness, since a
/// data file never changes.
#[derive(Clone, Debug)]
pub struct DiskLevel {
    pub(super) dir: PathBuf,
    pub(super) capacity_bytes: u64,
    pub(super) stats: Arc<IoStats>,
}

impl DiskLevel {
    /// The directory the level lives in; it exists once `LocalDir::open`
    /// has returned.
    pub(crate) fn dir(&self) -> &Path {
        &self.dir
    }

    /// The bytes the level may occupy; above zero, or there is no level.
    pub(crate) fn capacity_bytes(&self) -> u64 {
        self.capacity_bytes
    }

    /// The counter every page served from the level is charged to as one
    /// local read (RFD 003): the same one `LocalDir` charges chunk reads to.
    pub(crate) fn stats(&self) -> &Arc<IoStats> {
        &self.stats
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::local::LocalDir;
    #[cfg(target_os = "linux")]
    use crate::local::testing::evict;
    use crate::local::testing::{
        EMPTY_AT_FIRST_END, FIRST_BYTES, FIRST_RANGE, INNER_BYTES, INNER_RANGE, SECOND_BYTES,
        SECOND_RANGE, UNDER, VERIFICATION, chunk_file, chunks_offset, identity, inverted_range,
        open, overwrite, payload_len, range_len, write,
    };

    /// A range that starts in the first chunk and ends in the second.
    const STRADDLING_RANGE: Range<u64> = 104..202;
    /// A range before every chunk.
    const OUTSIDE_RANGE: Range<u64> = 0..3;
    /// A range in the gap between the chunks.
    const GAP_RANGE: Range<u64> = 106..200;
    /// A range one byte longer than the first chunk.
    const PAST_FIRST_END: Range<u64> = 100..107;
    /// The last byte of the first chunk alone.
    const LAST_OF_FIRST: Range<u64> = 105..106;

    fn not_resident(result: &Result<Bytes>) -> (u64, u64) {
        let Err(Error::Local {
            fault: LocalFault::RangeNotResident { start, end },
            ..
        }) = result
        else {
            panic!("expected a non-resident range, got {result:?}");
        };
        (*start, *end)
    }

    #[tokio::test]
    async fn test_page_returns_each_span_and_counts_one_read_of_its_length() {
        let dir = tempfile::tempdir().unwrap();
        let (local, stats) = open(dir.path());
        let disk = write(&local);
        let first = disk.page(FIRST_RANGE).await.unwrap();
        assert_eq!(&first[..], FIRST_BYTES);
        assert_eq!(stats.local_reads(), 1);
        assert_eq!(stats.local_bytes(), range_len(&FIRST_RANGE));
        let second = disk.page(SECOND_RANGE).await.unwrap();
        assert_eq!(&second[..], SECOND_BYTES);
        assert_eq!(stats.local_reads(), 2);
        assert_eq!(
            stats.local_bytes(),
            range_len(&FIRST_RANGE) + range_len(&SECOND_RANGE)
        );
        assert_eq!(stats.gets(), 0);
    }

    #[tokio::test]
    async fn test_page_sub_range_inside_a_span() {
        let dir = tempfile::tempdir().unwrap();
        let (local, stats) = open(dir.path());
        let disk = write(&local);
        let inner = disk.page(INNER_RANGE).await.unwrap();
        assert_eq!(&inner[..], INNER_BYTES);
        assert_eq!(stats.local_reads(), 1);
        assert_eq!(stats.local_bytes(), range_len(&INNER_RANGE));
    }

    #[tokio::test]
    async fn test_page_straddling_two_spans_is_not_resident_and_counts_no_read() {
        let dir = tempfile::tempdir().unwrap();
        let (local, stats) = open(dir.path());
        let disk = write(&local);
        let (start, end) = not_resident(&disk.page(STRADDLING_RANGE).await);
        assert_eq!(start..end, STRADDLING_RANGE);
        assert_eq!(stats.local_reads(), 0);
        assert_eq!(stats.local_bytes(), 0);
    }

    #[tokio::test]
    async fn test_page_outside_every_span_is_not_resident_and_counts_no_read() {
        let dir = tempfile::tempdir().unwrap();
        let (local, stats) = open(dir.path());
        let disk = write(&local);
        for range in [OUTSIDE_RANGE, GAP_RANGE, inverted_range()] {
            let (start, end) = not_resident(&disk.page(range.clone()).await);
            assert_eq!(start..end, range);
        }
        assert_eq!(stats.local_reads(), 0);
        assert_eq!(stats.local_bytes(), 0);
    }

    #[tokio::test]
    async fn test_page_at_a_span_end_serves_the_last_byte_and_nothing_past_it() {
        let dir = tempfile::tempdir().unwrap();
        let (local, stats) = open(dir.path());
        let disk = write(&local);
        let last = disk.page(LAST_OF_FIRST).await.unwrap();
        assert_eq!(&last[..], &FIRST_BYTES[FIRST_BYTES.len() - 1..]);
        assert_eq!(stats.local_reads(), 1);
        for range in [PAST_FIRST_END, EMPTY_AT_FIRST_END] {
            let (start, end) = not_resident(&disk.page(range.clone()).await);
            assert_eq!(start..end, range);
        }
        assert_eq!(stats.local_reads(), 1);
        assert_eq!(stats.local_bytes(), range_len(&LAST_OF_FIRST));
    }

    #[test]
    fn test_stores_exactly_is_true_for_the_stored_ranges_in_any_order_alone() {
        let dir = tempfile::tempdir().unwrap();
        let (local, stats) = open(dir.path());
        let disk = write(&local);
        assert!(disk.stores_exactly(&[FIRST_RANGE, SECOND_RANGE]));
        assert!(disk.stores_exactly(&[SECOND_RANGE, FIRST_RANGE]));
        for ranges in [
            vec![FIRST_RANGE],
            vec![FIRST_RANGE, SECOND_RANGE, GAP_RANGE],
            vec![INNER_RANGE, SECOND_RANGE],
            vec![PAST_FIRST_END, SECOND_RANGE],
            vec![],
        ] {
            assert!(!disk.stores_exactly(&ranges), "{ranges:?}");
        }
        assert_eq!(disk.verified_under(), UNDER);
        assert_eq!(stats.local_reads(), 0);
    }

    /// The record a check of the whole payload returns, or the check the
    /// file failed.
    fn checked(disk: &DiskChunks) -> std::result::Result<Vec<u8>, Rejection> {
        disk.checked_verification(u64::MAX).unwrap()
    }

    #[test]
    fn test_checked_verification_returns_the_record_and_counts_no_read() {
        let dir = tempfile::tempdir().unwrap();
        let (local, stats) = open(dir.path());
        let disk = write(&local);
        assert_eq!(checked(&disk), Ok(VERIFICATION.to_vec()));
        let reopened = local.chunks_for(&identity()).unwrap().unwrap();
        assert_eq!(checked(&reopened), Ok(VERIFICATION.to_vec()));
        assert_eq!(stats.local_reads(), 0, "a check is not a page read");
        assert_eq!(stats.local_bytes(), 0);
    }

    #[test]
    fn test_checked_verification_refuses_a_record_over_the_bound_before_reading_it() {
        let dir = tempfile::tempdir().unwrap();
        let (local, _) = open(dir.path());
        let disk = write(&local);
        let len = approx::count(VERIFICATION.len());
        assert_eq!(
            disk.checked_verification(len - 1).unwrap(),
            Err(Rejection::VerificationTooLarge { len, most: len - 1 })
        );
        assert_eq!(
            disk.checked_verification(len).unwrap(),
            Ok(VERIFICATION.to_vec())
        );
    }

    #[test]
    fn test_checked_verification_fails_for_any_payload_byte_changed() {
        let dir = tempfile::tempdir().unwrap();
        let (local, _) = open(dir.path());
        write(&local);
        let path = chunk_file(&local);
        let len = std::fs::metadata(&path).unwrap().len();
        let payload_start = len - payload_len();
        let original = std::fs::read(&path).unwrap();
        for offset in payload_start..len {
            let byte = original[approx::length(offset)];
            overwrite(&path, offset, &[!byte]);
            let disk = local.chunks_for(&identity()).unwrap().unwrap();
            assert_eq!(checked(&disk), Err(Rejection::ChecksumMismatch), "{offset}");
            overwrite(&path, offset, &[byte]);
        }
        let disk = local.chunks_for(&identity()).unwrap().unwrap();
        assert_eq!(checked(&disk), Ok(VERIFICATION.to_vec()));
    }

    #[test]
    fn test_checked_verification_of_a_file_cut_short_under_its_handle_is_a_short_payload() {
        let dir = tempfile::tempdir().unwrap();
        let (local, _) = open(dir.path());
        let disk = write(&local);
        let path = chunk_file(&local);
        let len = std::fs::metadata(&path).unwrap().len();
        std::fs::OpenOptions::new()
            .write(true)
            .open(&path)
            .unwrap()
            .set_len(len - 1)
            .unwrap();
        assert_eq!(checked(&disk), Err(Rejection::ShortPayload));
    }

    /// A page the operating system's page cache holds (the chunk file was
    /// just written) is read on the worker, to the bytes the blocking-pool
    /// read returns, and `page` counts it as one read. The temporary
    /// directory must be on a filesystem that serves `RWF_NOWAIT` reads
    /// (ext4, xfs, btrfs), not tmpfs or overlayfs: set `TMPDIR` where `/tmp`
    /// is either, as in a container or on a system that mounts `/tmp` in
    /// memory.
    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn test_page_reads_a_cached_page_on_the_worker_to_the_pool_read_bytes() {
        let dir = tempfile::tempdir().unwrap();
        let (local, stats) = open(dir.path());
        let disk = write(&local);
        for range in [FIRST_RANGE, SECOND_RANGE, INNER_RANGE] {
            let (offset, len) = disk.shared.locate(range.start, range.end).unwrap();
            let inline = disk.shared.read_cached(offset, len).expect(
                "a just-written page was not served without blocking: is the temporary \
                 directory on tmpfs, overlayfs, or another filesystem without RWF_NOWAIT \
                 reads? Set TMPDIR to a directory on ext4, xfs, or btrfs.",
            );
            let pooled = disk.shared.read_at(offset, len).unwrap();
            assert_eq!(inline, pooled, "{range:?}");
        }
        let first = disk.page(FIRST_RANGE).await.unwrap();
        assert_eq!(&first[..], FIRST_BYTES);
        assert_eq!(stats.local_reads(), 1);
        assert_eq!(stats.local_bytes(), range_len(&FIRST_RANGE));
    }

    /// A page dropped from the operating system's page cache is one the
    /// inline attempt cannot serve (`EAGAIN`), and `page` reads it on the
    /// blocking pool instead, to the right bytes, counted as one read.
    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn test_page_reads_an_uncached_page_on_the_blocking_pool() {
        let dir = tempfile::tempdir().unwrap();
        let (local, stats) = open(dir.path());
        let disk = write(&local);
        evict(&chunk_file(&local));
        let (offset, len) = disk
            .shared
            .locate(FIRST_RANGE.start, FIRST_RANGE.end)
            .unwrap();
        assert!(
            disk.shared.read_cached(offset, len).is_none(),
            "an evicted page was served without blocking: is the temporary directory on tmpfs?"
        );
        let first = disk.page(FIRST_RANGE).await.unwrap();
        assert_eq!(&first[..], FIRST_BYTES);
        assert_eq!(stats.local_reads(), 1);
        assert_eq!(stats.local_bytes(), range_len(&FIRST_RANGE));
    }

    /// A chunk file just written is in the page cache whole: every page of
    /// its key chunks, from the page the first chunk starts in to its end.
    #[test]
    fn test_resident_counts_every_page_of_the_chunks_of_a_just_written_file() {
        let dir = tempfile::tempdir().unwrap();
        let (local, stats) = open(dir.path());
        let disk = write(&local);
        let path = chunk_file(&local);
        let start = chunks_offset(&path);
        let end = std::fs::metadata(&path).unwrap().len();
        assert_eq!(disk.chunks(), start..end);
        assert_eq!(disk.path(), path);
        let page = super::residency::page_bytes();
        let pages = end.div_ceil(page) - start / page;
        assert_eq!(
            disk.resident().unwrap(),
            ResidentPages {
                pages,
                resident: pages
            }
        );
        assert_eq!(stats.local_reads(), 0, "a sample reads nothing");
    }

    #[test]
    fn test_disk_chunks_is_send_and_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<DiskChunks>();
        assert_send_sync::<LocalDir>();
    }
}
