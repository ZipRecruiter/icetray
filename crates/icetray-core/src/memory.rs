//! The memory a process may use (RFD 020): its cgroup's limit where one is
//! set, else the host's memory, so the daemon can say at start when its
//! `disk` tables' chunk files and the page cache's disk level will not
//! fit the kernel's page cache beside it. On Linux a container's page
//! cache counts against its cgroup's limit, so the cgroup's limit is the
//! room that cache has. The readers take the root the filesystem is read
//! under, so a test reads a tree of its own.

use std::fmt;
use std::fs;
use std::path::{Component, Path};

use crate::local::{page_bytes, sysconf_count};

/// Where the cgroup filesystem is mounted, under the root.
const CGROUP_MOUNT: &str = "sys/fs/cgroup";
/// The process's cgroup membership, under the root.
const PROC_SELF_CGROUP: &str = "proc/self/cgroup";
/// The cgroup v2 file holding a group's memory limit.
const V2_MEMORY_MAX: &str = "memory.max";
/// What `memory.max` holds for a group with no limit.
const V2_UNLIMITED: &str = "max";
/// The cgroup v1 memory controller: its name in `/proc/self/cgroup`, and
/// its directory under the mount, which a hierarchy that mounts it with
/// other controllers links to that hierarchy's own directory.
const V1_MEMORY_CONTROLLER: &str = "memory";
/// The fields of a v1 line of `/proc/self/cgroup`: the hierarchy id, its
/// controllers, and the group's path, which may itself hold a colon.
const V1_LINE_FIELDS: usize = 3;
/// The cgroup v1 file holding a group's memory limit. A group with no
/// limit holds a value near `i64::MAX`, above any host's memory, which the
/// limit's minimum with the host's memory discards.
const V1_LIMIT_IN_BYTES: &str = "memory.limit_in_bytes";
/// What the cgroup v2 line of `/proc/self/cgroup` starts with: hierarchy
/// id 0 and no controllers, before the group's path.
const V2_LINE_PREFIX: &str = "0::";
/// The process's own memory beyond the indexes and the cache budget, as
/// the divisor of their sum: a third of it, for requests in flight, a
/// load's transient chunks, and the allocator's fragmentation and
/// bookkeeping (RFD 012).
const PROCESS_SHARE_DENOMINATOR: u64 = 3;

/// Where a memory limit comes from. Display gives the log field's value.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LimitSource {
    /// The process's cgroup, or one of its ancestors.
    Cgroup,
    /// The host's physical memory: no cgroup limit is set, or it is above
    /// the host's memory.
    Host,
}

impl fmt::Display for LimitSource {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Cgroup => "cgroup",
            Self::Host => "host",
        })
    }
}

/// The memory a process may use, and where the bound comes from.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct MemoryLimit {
    /// The bound in bytes.
    pub bytes: u64,
    /// Where it comes from.
    pub of: LimitSource,
}

/// The process's memory limit: the smaller of its cgroup's limit and the
/// host's memory. `None` when neither can be read.
pub fn limit() -> Option<MemoryLimit> {
    limit_under(Path::new("/"), host_bytes())
}

/// `limit`, reading the cgroup filesystem under `root`, against a host of
/// `host` bytes.
fn limit_under(root: &Path, host: Option<u64>) -> Option<MemoryLimit> {
    let cgroup = cgroup_limit(root).map(|bytes| MemoryLimit {
        bytes,
        of: LimitSource::Cgroup,
    });
    let host = host.map(|bytes| MemoryLimit {
        bytes,
        of: LimitSource::Host,
    });
    match (cgroup, host) {
        (Some(cgroup), Some(host)) if cgroup.bytes < host.bytes => Some(cgroup),
        (_, Some(host)) => Some(host),
        (cgroup, None) => cgroup,
    }
}

/// The host's physical memory: its page count times the page size.
fn host_bytes() -> Option<u64> {
    // intentional: a host whose memory cannot be read leaves the cgroup's
    // limit, or none, which the caller logs.
    let pages = sysconf_count(libc::_SC_PHYS_PAGES).ok()?;
    let page = page_bytes().ok()?;
    pages.checked_mul(page)
}

/// The memory limit the process's cgroup sets under `root`: the cgroup v2
/// limit when the process is in a v2 group with one, else the v1 memory
/// controller's; `None` with neither.
fn cgroup_limit(root: &Path) -> Option<u64> {
    // intentional: no membership file is a host without cgroups, whose
    // limit is the host's memory.
    let membership = fs::read_to_string(root.join(PROC_SELF_CGROUP)).ok()?;
    let mount = root.join(CGROUP_MOUNT);
    let v2 = membership
        .lines()
        .find_map(|line| line.strip_prefix(V2_LINE_PREFIX))
        .and_then(|group| smallest_limit(&mount, group, V2_MEMORY_MAX));
    v2.or_else(|| {
        let (controllers, group) = membership.lines().find_map(v1_memory_group)?;
        [controllers, V1_MEMORY_CONTROLLER]
            .into_iter()
            .find_map(|dir| smallest_limit(&mount.join(dir), group, V1_LIMIT_IN_BYTES))
    })
}

/// The controllers and group path of a v1 line of `/proc/self/cgroup`
/// whose controllers include memory: `<id>:<controllers>:<path>`. The
/// hierarchy's directory under the mount is its controllers, such as
/// `cpu,memory`, when memory is mounted with others.
fn v1_memory_group(line: &str) -> Option<(&str, &str)> {
    let mut fields = line.splitn(V1_LINE_FIELDS, ':');
    let _id = fields.next()?;
    let controllers = fields.next()?;
    let group = fields.next()?;
    controllers
        .split(',')
        .any(|controller| controller == V1_MEMORY_CONTROLLER)
        .then_some((controllers, group))
}

/// The smallest limit `file` sets in the hierarchy mounted at `mount`, over
/// the group at `group` and each of its ancestors to the mount: a group
/// cannot use more than any ancestor allows. The mount's own file is read
/// too, since in a cgroup namespace the process's group is the mount's
/// root, and `/proc/self/cgroup` may name a path the mount does not show;
/// a path with `..` in it is read at the mount alone. A file absent or
/// holding no limit sets none.
fn smallest_limit(mount: &Path, group: &str, file: &str) -> Option<u64> {
    let group = Path::new(group);
    let inside = group
        .components()
        .all(|component| matches!(component, Component::RootDir | Component::Normal(_)));
    let mut dir = if inside {
        mount.join(group.strip_prefix("/").unwrap_or(group))
    } else {
        mount.to_path_buf()
    };
    let mut smallest = None;
    loop {
        if let Some(limit) = read_limit(&dir.join(file)) {
            smallest = Some(smallest.map_or(limit, |smallest: u64| smallest.min(limit)));
        }
        if dir == mount || !dir.pop() {
            return smallest;
        }
    }
}

/// The limit a cgroup memory file holds: its number, or `None` for
/// `max`, an absent file, or anything else.
fn read_limit(path: &Path) -> Option<u64> {
    // intentional: a group without the file, the root group's case, sets
    // no limit of its own.
    let text = fs::read_to_string(path).ok()?;
    let text = text.trim();
    if text == V2_UNLIMITED {
        return None;
    }
    // intentional: a value that is not a byte count sets no limit icetray
    // can compare with.
    text.parse().ok()
}

/// The bytes short when `needed` bytes must fit under `limit` beside a
/// resident set of `rss` bytes; `None` when they fit.
pub fn shortfall(needed: u64, limit: u64, rss: u64) -> Option<u64> {
    let room = limit.saturating_sub(rss);
    (needed > room).then(|| needed - room)
}

/// The bytes by which the cache `budget` and the resident indexes of the
/// tables served, `indexes` one entry per table, overrun `limit` (RFD
/// 028), RFD 012's sizing rule in reverse: the indexes, the largest once
/// more since a refresh that rewrites a table wholesale loads its next
/// generation beside it, and the budget, plus a third of that sum for the
/// process, requests in flight, and the allocator. `None` when they fit.
pub fn budget_shortfall(indexes: &[u64], budget: u64, limit: u64) -> Option<u64> {
    let largest = indexes.iter().copied().max().unwrap_or(0);
    let held = indexes
        .iter()
        .fold(budget.saturating_add(largest), |sum, &bytes| {
            sum.saturating_add(bytes)
        });
    let needed = held.saturating_add(held / PROCESS_SHARE_DENOMINATOR);
    needed.checked_sub(limit).filter(|&missing| missing > 0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::approx::units::GIB;

    /// A host large enough that every cgroup limit the tests write is
    /// below it.
    const HOST: u64 = 1 << 40;
    /// The larger of the limits the tests write.
    const TWO_GIB: u64 = 2 * GIB;
    /// What cgroup v1 holds for a group with no limit: `i64::MAX` rounded
    /// down to a 4 KiB page.
    const V1_UNLIMITED: u64 = 0x7FFF_FFFF_FFFF_F000;

    /// A root under `dir` whose `/proc/self/cgroup` holds `membership`.
    fn root_with(dir: &Path, membership: &str) -> std::path::PathBuf {
        let file = dir.join(PROC_SELF_CGROUP);
        fs::create_dir_all(file.parent().unwrap()).unwrap();
        fs::write(file, membership).unwrap();
        dir.to_path_buf()
    }

    /// Writes `value` to `file` in the group at `group` under `root`'s
    /// mount, `under` being the controller directory, empty for v2.
    fn write_group(root: &Path, under: &str, group: &str, file: &str, value: &str) {
        let dir = root.join(CGROUP_MOUNT).join(under).join(group);
        fs::create_dir_all(&dir).unwrap();
        fs::write(dir.join(file), format!("{value}\n")).unwrap();
    }

    fn cgroup(bytes: u64) -> MemoryLimit {
        MemoryLimit {
            bytes,
            of: LimitSource::Cgroup,
        }
    }

    fn host() -> MemoryLimit {
        MemoryLimit {
            bytes: HOST,
            of: LimitSource::Host,
        }
    }

    #[test]
    fn test_limit_under_v2_reads_the_group_s_number() {
        let dir = tempfile::tempdir().unwrap();
        let root = root_with(dir.path(), "0::/kubepods/pod\n");
        write_group(&root, "", "kubepods/pod", V2_MEMORY_MAX, &GIB.to_string());
        assert_eq!(limit_under(&root, Some(HOST)), Some(cgroup(GIB)));
    }

    #[test]
    fn test_limit_under_v2_max_everywhere_is_the_host() {
        let dir = tempfile::tempdir().unwrap();
        let root = root_with(dir.path(), "0::/kubepods/pod\n");
        write_group(&root, "", "kubepods/pod", V2_MEMORY_MAX, V2_UNLIMITED);
        write_group(&root, "", "kubepods", V2_MEMORY_MAX, V2_UNLIMITED);
        assert_eq!(limit_under(&root, Some(HOST)), Some(host()));
    }

    /// A group cannot use more than an ancestor allows, so the smallest
    /// limit on the way to the mount wins, whichever level sets it.
    #[test]
    fn test_limit_under_v2_takes_the_smallest_ancestor() {
        let dir = tempfile::tempdir().unwrap();
        let root = root_with(dir.path(), "0::/kubepods/pod/container\n");
        write_group(
            &root,
            "",
            "kubepods/pod/container",
            V2_MEMORY_MAX,
            V2_UNLIMITED,
        );
        write_group(&root, "", "kubepods/pod", V2_MEMORY_MAX, &GIB.to_string());
        write_group(&root, "", "kubepods", V2_MEMORY_MAX, &TWO_GIB.to_string());
        assert_eq!(limit_under(&root, Some(HOST)), Some(cgroup(GIB)));
    }

    /// Under a cgroup namespace the process's group is the mount's root,
    /// whose own file holds the limit, and the path the membership names
    /// may not exist under the mount.
    #[test]
    fn test_limit_under_v2_in_a_namespace_reads_the_mount_s_own_file() {
        let dir = tempfile::tempdir().unwrap();
        let root = root_with(dir.path(), "0::/\n");
        write_group(&root, "", "", V2_MEMORY_MAX, &GIB.to_string());
        assert_eq!(limit_under(&root, Some(HOST)), Some(cgroup(GIB)));
        let moved = tempfile::tempdir().unwrap();
        let root = root_with(moved.path(), "0::/../../elsewhere\n");
        write_group(&root, "", "", V2_MEMORY_MAX, &GIB.to_string());
        assert_eq!(limit_under(&root, Some(HOST)), Some(cgroup(GIB)));
    }

    #[test]
    fn test_limit_under_v1_reads_the_memory_controller_s_number() {
        let dir = tempfile::tempdir().unwrap();
        let root = root_with(dir.path(), "5:cpu,cpuacct:/docker/c\n4:memory:/docker/c\n");
        write_group(
            &root,
            V1_MEMORY_CONTROLLER,
            "docker/c",
            V1_LIMIT_IN_BYTES,
            &GIB.to_string(),
        );
        assert_eq!(limit_under(&root, Some(HOST)), Some(cgroup(GIB)));
    }

    /// A hierarchy that mounts memory with other controllers lives in
    /// their directory together.
    #[test]
    fn test_limit_under_v1_reads_a_co_mounted_hierarchy() {
        let dir = tempfile::tempdir().unwrap();
        let co_mounted = "cpu,memory";
        let root = root_with(dir.path(), &format!("3:{co_mounted}:/docker/c\n"));
        write_group(
            &root,
            co_mounted,
            "docker/c",
            V1_LIMIT_IN_BYTES,
            &GIB.to_string(),
        );
        assert_eq!(limit_under(&root, Some(HOST)), Some(cgroup(GIB)));
    }

    #[test]
    fn test_limit_under_v1_unlimited_is_the_host() {
        let dir = tempfile::tempdir().unwrap();
        let root = root_with(dir.path(), "4:memory:/docker/c\n");
        write_group(
            &root,
            V1_MEMORY_CONTROLLER,
            "docker/c",
            V1_LIMIT_IN_BYTES,
            &V1_UNLIMITED.to_string(),
        );
        assert_eq!(limit_under(&root, Some(HOST)), Some(host()));
    }

    /// A hybrid host lists a v2 line with no memory controller beside the
    /// v1 lines: the v2 group sets nothing, so the v1 limit is read.
    #[test]
    fn test_limit_under_a_hybrid_host_falls_back_to_v1() {
        let dir = tempfile::tempdir().unwrap();
        let root = root_with(dir.path(), "4:memory:/docker/c\n0::/docker/c\n");
        write_group(
            &root,
            V1_MEMORY_CONTROLLER,
            "docker/c",
            V1_LIMIT_IN_BYTES,
            &GIB.to_string(),
        );
        assert_eq!(limit_under(&root, Some(HOST)), Some(cgroup(GIB)));
    }

    #[test]
    fn test_limit_under_missing_files_is_the_host() {
        let dir = tempfile::tempdir().unwrap();
        assert_eq!(limit_under(dir.path(), Some(HOST)), Some(host()));
        let root = root_with(dir.path(), "0::/kubepods/pod\n");
        assert_eq!(limit_under(&root, Some(HOST)), Some(host()));
        assert_eq!(limit_under(&root, None), None);
    }

    /// A cgroup limit above the host's memory bounds nothing the host does
    /// not.
    #[test]
    fn test_limit_under_a_cgroup_above_the_host_is_the_host() {
        let dir = tempfile::tempdir().unwrap();
        let root = root_with(dir.path(), "0::/big\n");
        write_group(&root, "", "big", V2_MEMORY_MAX, &(HOST + 1).to_string());
        assert_eq!(limit_under(&root, Some(HOST)), Some(host()));
        assert_eq!(limit_under(&root, None), Some(cgroup(HOST + 1)));
    }

    /// The host's memory reads as some bytes on every platform the tests
    /// run on.
    #[test]
    fn test_limit_reads_the_host_at_least() {
        let limit = limit().unwrap();
        assert!(limit.bytes > 0);
        assert!(limit.bytes <= host_bytes().unwrap());
    }

    #[test]
    fn test_shortfall_is_what_does_not_fit_beside_the_resident_set() {
        assert_eq!(shortfall(GIB, TWO_GIB, GIB), None, "exactly fits");
        assert_eq!(shortfall(TWO_GIB, TWO_GIB, GIB), Some(GIB));
        assert_eq!(
            shortfall(GIB, GIB, TWO_GIB),
            Some(GIB),
            "a resident set over the limit leaves no room"
        );
        assert_eq!(shortfall(0, GIB, TWO_GIB), None, "nothing needed");
    }

    /// Indexes of one and three gibibytes and a budget of four: the one
    /// and the three, the three again, and the four, with a third more.
    #[test]
    fn test_budget_shortfall_counts_the_largest_table_twice_and_a_third() {
        assert_eq!(
            budget_shortfall(&[GIB, 3 * GIB], 4 * GIB, 10 * GIB),
            Some(44 * GIB / 3 - 10 * GIB)
        );
    }

    #[test]
    fn test_budget_shortfall_is_none_when_it_fits() {
        let needed = 4 * GIB;
        assert_eq!(budget_shortfall(&[GIB], GIB, needed), None, "exactly fits");
        assert_eq!(budget_shortfall(&[GIB], GIB, needed - 1), Some(1));
        assert_eq!(budget_shortfall(&[GIB, 3 * GIB], GIB, 11 * GIB), None);
        assert_eq!(budget_shortfall(&[], 0, 0), None, "nothing is held");
        assert_eq!(
            budget_shortfall(&[], 3 * GIB, 4 * GIB),
            None,
            "a budget alone, and a third"
        );
        assert_eq!(budget_shortfall(&[], 3 * GIB, 4 * GIB - 1), Some(1));
    }

    #[test]
    fn test_fmt_limit_source_names() {
        assert_eq!(LimitSource::Cgroup.to_string(), "cgroup");
        assert_eq!(LimitSource::Host.to_string(), "host");
    }
}
