//! How much of a chunk file the operating system's page cache holds (RFD
//! 020): a `disk` table reads its key pages at memory speed only while the
//! kernel keeps its chunk files cached, and this is how icetray asks. The
//! file is mapped `PROT_NONE`, which reads no byte and lets no access fault
//! a page in, for as long as one `mincore` call takes, and unmapped as the
//! call returns. No mapping outlives a sample: one kept per chunk file
//! would count against the process's map limit beside the allocator's, and
//! would keep a deleted chunk file's pages.

use std::fs::File;
use std::io;
use std::ops::Range;
use std::os::fd::AsRawFd as _;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::approx;

/// Pages of chunk files a sample covered, and how many of them the page
/// cache held. A page is the platform's (`sysconf(_SC_PAGESIZE)`), so a
/// sum over files is weighted by bytes.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ResidentPages {
    /// Pages sampled.
    pub pages: u64,
    /// Of those, the pages the page cache held.
    pub resident: u64,
}

impl ResidentPages {
    /// The counts of `self` and `other` together.
    pub(crate) fn plus(self, other: Self) -> Self {
        Self {
            pages: self.pages.saturating_add(other.pages),
            resident: self.resident.saturating_add(other.resident),
        }
    }

    /// The resident pages over the pages sampled; zero when none was.
    pub fn fraction(self) -> f64 {
        approx::ratio(self.resident, self.pages)
    }
}

/// Why a file's residency could not be observed; the set is closed. Clone,
/// so one sample of a file can stand for every table that reads it.
#[derive(Clone, Debug, thiserror::Error)]
pub enum Unobservable {
    /// The process neither owns the file, nor is root, nor may write it.
    /// Linux's `mincore` then reports every page of the file resident
    /// whatever the page cache holds, which would hide a shortfall.
    #[cfg(target_os = "linux")]
    #[error("owned by uid {owner}, and neither owned nor writable by this process's uid {euid}")]
    Foreign {
        /// The file's owner.
        owner: u32,
        /// The process's effective user.
        euid: u32,
    },
    /// Checking the file's owner or permissions, mapping the file, or the
    /// `mincore` call failed.
    #[error("{0}")]
    Io(Arc<io::Error>),
}

impl From<io::Error> for Unobservable {
    fn from(error: io::Error) -> Self {
        Self::Io(Arc::new(error))
    }
}

/// A chunk file whose residency could not be observed, and why.
#[derive(Clone, Debug, thiserror::Error)]
#[error("residency of chunk file {} cannot be observed: {why}", path.display())]
pub struct Unobserved {
    /// The chunk file.
    pub path: PathBuf,
    /// Why.
    pub why: Unobservable,
}

/// The bit of a `mincore` byte that is set for a resident page: the least
/// significant, on Linux and as macOS's `MINCORE_INCORE`.
const MINCORE_INCORE: u8 = 1;

/// The residency of `range` of `file`, open at `path`: every page that
/// holds a byte of it, empty for an empty range. Synchronous system calls,
/// for the blocking pool.
pub(crate) fn resident(
    file: &File,
    path: &Path,
    range: Range<u64>,
) -> Result<ResidentPages, Unobservable> {
    #[cfg(target_os = "linux")]
    {
        observable_as(file, path, rustix::process::geteuid().as_raw())?;
    }
    // macOS's `mincore` answers for any mapping, so only Linux checks.
    #[cfg(not(target_os = "linux"))]
    let _ = path;
    let page = page_bytes();
    match Span::covering(range, page) {
        Some(span) => sample(file, &span, page),
        None => Ok(ResidentPages::default()),
    }
}

/// Whether Linux's `mincore` reports `file`'s page cache to a process whose
/// effective user is `euid`: the kernel answers for a file the caller owns,
/// may write, or holds `CAP_FOWNER` over, here taken as being root.
#[cfg(target_os = "linux")]
fn observable_as(file: &File, path: &Path, euid: u32) -> Result<(), Unobservable> {
    use std::os::unix::fs::MetadataExt as _;
    let owner = file.metadata()?.uid();
    if owner == euid || euid == 0 || writable(path)? {
        Ok(())
    } else {
        Err(Unobservable::Foreign { owner, euid })
    }
}

/// Whether this process's effective user may write the file at `path`.
/// By path, since asking of a descriptor needs `AT_EMPTY_PATH`. rustix
/// asks `faccessat2` (Linux 5.8) for `AT_EACCESS`, and before 5.8 falls
/// back to `faccessat` only for a process whose real and effective ids
/// agree; a seccomp profile that refuses `faccessat2` with `EPERM` makes
/// every such file report as not writable.
#[cfg(target_os = "linux")]
fn writable(path: &Path) -> io::Result<bool> {
    use rustix::fs::{Access, AtFlags, CWD, accessat};
    use rustix::io::Errno;
    match accessat(CWD, path, Access::WRITE_OK, AtFlags::EACCESS) {
        Ok(()) => Ok(true),
        Err(Errno::ACCESS | Errno::PERM | Errno::ROFS) => Ok(false),
        Err(error) => Err(error.into()),
    }
}

/// The platform's page size in bytes.
pub(crate) fn page_bytes() -> u64 {
    approx::count(rustix::param::page_size())
}

/// The stretch of a file a mapping covers to sample a byte range: from the
/// page the range starts in to the range's end, since a mapping starts on
/// a page boundary.
#[derive(Debug, PartialEq, Eq)]
struct Span {
    /// Where the mapping starts in the file, a multiple of the page size.
    offset: u64,
    /// The mapping's length in bytes.
    len: u64,
}

impl Span {
    /// The span covering `range` in pages of `page` bytes; `None` for an
    /// empty or inverted range, which covers no page.
    fn covering(range: Range<u64>, page: u64) -> Option<Self> {
        if range.is_empty() {
            return None;
        }
        let offset = range.start - range.start % page;
        Some(Self {
            offset,
            len: range.end - offset,
        })
    }
}

/// A mapping of a file, unmapped when dropped.
struct Mapping {
    addr: *mut libc::c_void,
    len: usize,
}

impl Drop for Mapping {
    fn drop(&mut self) {
        // SAFETY: `addr` and `len` are a mapping `mmap` returned, which
        // nothing else references and which is unmapped only here.
        let status = unsafe { libc::munmap(self.addr, self.len) };
        if status != 0 {
            tracing::warn!(
                error = %io::Error::last_os_error(),
                "unmapping a residency sample failed"
            );
        }
    }
}

/// Maps `span` of `file` `PROT_NONE` and asks `mincore` which of its pages
/// of `page` bytes the page cache holds.
fn sample(file: &File, span: &Span, page: u64) -> Result<ResidentPages, Unobservable> {
    let offset = libc::off_t::try_from(span.offset)
        .map_err(|_| io::Error::from(io::ErrorKind::InvalidInput))?;
    let len = approx::length(span.len);
    // SAFETY: a fresh mapping at an address the kernel chooses, of a
    // descriptor `file` keeps open for the call; `offset` is a multiple of
    // the page size. `PROT_NONE` lets nothing read or write through it, so
    // no access to it is ever made.
    let addr = unsafe {
        libc::mmap(
            std::ptr::null_mut(),
            len,
            libc::PROT_NONE,
            libc::MAP_SHARED,
            file.as_raw_fd(),
            offset,
        )
    };
    if addr == libc::MAP_FAILED {
        return Err(io::Error::last_os_error().into());
    }
    let mapping = Mapping { addr, len };
    let mut pages = vec![0u8; approx::length(span.len.div_ceil(page))];
    // SAFETY: `mapping` is live for the call and `len` bytes long, and
    // `pages` holds one byte for each of its pages, which is what
    // `mincore` writes, and nothing else borrows it until the call returns.
    let status = unsafe { libc::mincore(mapping.addr, mapping.len, pages.as_mut_ptr().cast()) };
    if status != 0 {
        return Err(io::Error::last_os_error().into());
    }
    drop(mapping);
    let resident = pages
        .iter()
        .filter(|state| **state & MINCORE_INCORE != 0)
        .count();
    Ok(ResidentPages {
        pages: approx::count(pages.len()),
        resident: approx::count(resident),
    })
}

#[cfg(test)]
mod tests {
    use std::io::Write as _;

    use super::*;

    /// Pages in the files the tests write: enough that half of them is
    /// many pages and not all of them.
    const FILE_PAGES: u64 = 64;

    /// The page size the span tests assume: any, here 4 KiB.
    const PAGE: u64 = 4096;

    /// A file of `FILE_PAGES` pages just written under `dir`, open, its
    /// path, and its length.
    fn written(dir: &Path) -> (File, PathBuf, u64) {
        let page = page_bytes();
        let len = FILE_PAGES * page;
        let path = dir.join("pages");
        let mut file = File::create(&path).unwrap();
        file.write_all(&vec![7u8; approx::length(len)]).unwrap();
        drop(file);
        (File::open(&path).unwrap(), path, len)
    }

    /// A file just written is in the page cache whole, and a `PROT_NONE`
    /// mapping of it reports that: on macOS as on Linux, `mincore` answers
    /// for a mapping no access can touch.
    #[test]
    fn test_resident_counts_every_page_of_a_just_written_file() {
        let dir = tempfile::tempdir().unwrap();
        let (file, path, len) = written(dir.path());
        let residency = resident(&file, &path, 0..len).unwrap();
        assert_eq!(
            residency,
            ResidentPages {
                pages: FILE_PAGES,
                resident: FILE_PAGES
            }
        );
        assert_eq!(residency.fraction().to_bits(), 1.0_f64.to_bits());
    }

    /// A range that starts inside a page counts that page and every page
    /// to its end.
    #[test]
    fn test_resident_counts_the_pages_a_range_touches() {
        let dir = tempfile::tempdir().unwrap();
        let (file, path, len) = written(dir.path());
        let page = page_bytes();
        let inside_the_second_page = page + 1;
        let residency = resident(&file, &path, inside_the_second_page..len).unwrap();
        assert_eq!(residency.pages, FILE_PAGES - 1);
        assert_eq!(residency.resident, FILE_PAGES - 1);
    }

    #[test]
    fn test_resident_of_an_empty_range_counts_no_page() {
        let dir = tempfile::tempdir().unwrap();
        let (file, path, len) = written(dir.path());
        assert_eq!(
            resident(&file, &path, len..len).unwrap(),
            ResidentPages::default()
        );
        assert_eq!(
            ResidentPages::default().fraction().to_bits(),
            0.0_f64.to_bits()
        );
    }

    /// A file another user owns that this process may not write is not
    /// sampled, since Linux would report it whole whatever the page cache
    /// holds; a root process, which Linux answers for any file, samples it.
    #[cfg(target_os = "linux")]
    #[test]
    fn test_observable_as_another_user_without_write_is_foreign_unless_root() {
        use std::os::unix::fs::{MetadataExt as _, PermissionsExt as _};
        let dir = tempfile::tempdir().unwrap();
        let (file, path, _) = written(dir.path());
        let read_only = 0o444;
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(read_only)).unwrap();
        let owner = file.metadata().unwrap().uid();
        let someone_else = owner.wrapping_add(1);
        let root = rustix::process::geteuid().is_root();
        match observable_as(&file, &path, someone_else) {
            Ok(()) => assert!(root, "only root may write a read-only file"),
            Err(Unobservable::Foreign { owner: found, euid }) => {
                assert!(!root, "root writes any file");
                assert_eq!((found, euid), (owner, someone_else));
            }
            Err(other) => panic!("expected a foreign file, got {other}"),
        }
        assert!(observable_as(&file, &path, 0).is_ok(), "root is answered");
        assert!(observable_as(&file, &path, owner).is_ok(), "the owner is");
    }

    #[test]
    fn test_covering_starts_at_the_page_the_range_starts_in() {
        assert_eq!(
            Span::covering(0..10, PAGE),
            Some(Span { offset: 0, len: 10 })
        );
        assert_eq!(
            Span::covering(PAGE + 1..2 * PAGE + 1, PAGE),
            Some(Span {
                offset: PAGE,
                len: PAGE + 1
            })
        );
        assert_eq!(
            Span::covering(2 * PAGE..2 * PAGE + 1, PAGE),
            Some(Span {
                offset: 2 * PAGE,
                len: 1
            })
        );
        assert_eq!(Span::covering(5..5, PAGE), None);
        assert_eq!(Span::covering(Range { start: 9, end: 5 }, PAGE), None);
    }

    #[test]
    fn test_plus_adds_both_counts() {
        let one = ResidentPages {
            pages: 3,
            resident: 2,
        };
        let other = ResidentPages {
            pages: 5,
            resident: 1,
        };
        assert_eq!(
            one.plus(other),
            ResidentPages {
                pages: 8,
                resident: 3
            }
        );
    }

    /// A file dropped from the page cache reads as no page resident.
    #[cfg(target_os = "linux")]
    #[test]
    fn test_resident_after_eviction_counts_none() {
        let dir = tempfile::tempdir().unwrap();
        let (file, path, len) = written(dir.path());
        crate::local::testing::evict(&path);
        assert_eq!(
            resident(&file, &path, 0..len).unwrap(),
            ResidentPages {
                pages: FILE_PAGES,
                resident: 0
            }
        );
    }

    /// After an eviction, reading the first half back without readahead
    /// makes at least that half resident and leaves the rest out.
    #[cfg(target_os = "linux")]
    #[test]
    fn test_resident_after_reading_half_back_counts_that_half() {
        use std::os::unix::fs::FileExt as _;
        let dir = tempfile::tempdir().unwrap();
        let (file, path, len) = written(dir.path());
        crate::local::testing::evict(&path);
        // Offset 0 and no length cover the whole file.
        rustix::fs::fadvise(&file, 0, None, rustix::fs::Advice::Random).unwrap();
        let half = len / 2;
        file.read_exact_at(&mut vec![0u8; approx::length(half)], 0)
            .unwrap();
        let residency = resident(&file, &path, 0..len).unwrap();
        assert_eq!(residency.pages, FILE_PAGES);
        assert!(
            residency.resident >= FILE_PAGES / 2 && residency.resident < FILE_PAGES,
            "{residency:?}"
        );
    }
}
