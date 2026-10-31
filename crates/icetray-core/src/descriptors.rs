//! The process's limit on open file descriptors: raised to the hard limit
//! at start, set against what a configuration needs, and read again where
//! running out explains a failure. The page cache's disk level holds one
//! descriptor per block for the life of the process, and a load keeps one
//! read in flight per file, each a descriptor for a local table and a
//! socket for S3; every table loads at once at start, each with its own
//! concurrency. A pod has no limit setting of its own, so the process
//! takes what its hard limit allows and says when that is still short.

use std::io;

use crate::approx;
use crate::config::Config;
use crate::contract::PAGE_CACHE_DISK_BLOCK_BYTES;
use crate::generation::load_concurrency;

/// Descriptors beside the disk level's blocks and the loads' reads: the two
/// listeners and their connections, the object store's and the catalog's
/// connection pools, the standard streams, and the chunk file retention
/// opens. Headroom for an estimate, not a measured bound.
pub const DESCRIPTOR_MARGIN: u64 = 256;

/// The descriptors a process whose disk level is `page_cache_disk_bytes`
/// and that runs `loads` loads at once needs: one per block of the level
/// (`contract::PAGE_CACHE_DISK_BLOCK_BYTES`), one per file each load keeps
/// in flight on this runtime (`generation::load_concurrency`, which every
/// load runs on its own), and `DESCRIPTOR_MARGIN`. A disk-tier table's
/// chunk files, one per data file and the rules it is verified under, so
/// two for a data file a key-value entry and an ordered entry both serve,
/// come on top and are not known before its load.
pub fn needed(page_cache_disk_bytes: u64, loads: usize) -> u64 {
    page_cache_disk_bytes / PAGE_CACHE_DISK_BLOCK_BYTES
        + approx::count(loads).saturating_mul(approx::count(load_concurrency()))
        + DESCRIPTOR_MARGIN
}

/// `needed` for the disk level `config` sets, none without a local
/// directory, and one load per configured table: the start loads every
/// table at once.
pub fn needed_by(config: &Config) -> u64 {
    needed(
        config
            .storage
            .local
            .as_ref()
            .map_or(0, |local| local.page_cache_disk_bytes),
        config.tables.len(),
    )
}

/// The open-file limit in force: the soft `RLIMIT_NOFILE`, or less where
/// the kernel grants less than it states. macOS reports an unlimited soft
/// limit as unlimited and caps every process at `kern.maxfilesperproc`,
/// which `getdtablesize` returns.
pub fn limit() -> u64 {
    let soft = current().rlim_cur;
    // SAFETY: `getdtablesize` takes no argument and reads the calling
    // process's own descriptor table size.
    let table = unsafe { libc::getdtablesize() };
    u64::try_from(table).map_or(soft, |table| soft.min(table))
}

/// Raises the soft limit to the hard limit, then sets it to `limit` where
/// the kernel grants less, so the soft limit a child process inherits is
/// the one in force; returns `limit` after. A refusal leaves the limit as
/// it was and is the error.
pub fn raise() -> io::Result<u64> {
    let before = current();
    if before.rlim_cur < before.rlim_max {
        set(before.rlim_max, before.rlim_max)?;
    }
    let granted = limit();
    if current().rlim_cur > granted {
        set(granted, before.rlim_max)?;
    }
    Ok(granted)
}

/// The process's `RLIMIT_NOFILE`.
pub(crate) fn current() -> libc::rlimit {
    let mut limit = libc::rlimit {
        rlim_cur: 0,
        rlim_max: 0,
    };
    // SAFETY: `limit` is a live, writable `rlimit` that nothing else borrows
    // until the call returns, and the call writes only that struct.
    let status = unsafe { libc::getrlimit(libc::RLIMIT_NOFILE, &raw mut limit) };
    assert_eq!(
        status, 0,
        "getrlimit fails only for a bad pointer or resource, and both are fixed here"
    );
    limit
}

/// Sets the process's `RLIMIT_NOFILE` to `soft` under `hard`.
pub(crate) fn set(soft: u64, hard: u64) -> io::Result<()> {
    let limit = libc::rlimit {
        rlim_cur: soft,
        rlim_max: hard,
    };
    // SAFETY: `limit` is a live `rlimit` the call only reads.
    let status = unsafe { libc::setrlimit(libc::RLIMIT_NOFILE, &raw const limit) };
    if status == 0 {
        Ok(())
    } else {
        Err(io::Error::last_os_error())
    }
}

/// Running a test body in a child process, for a test that changes the
/// process's own limit and so must not change it under the tests running
/// beside it.
#[cfg(test)]
pub(crate) mod testing {
    use std::process::Command;

    /// Set in the child, so the test it runs takes the child's branch.
    const CHILD_ENV: &str = "ICETRAY_DESCRIPTORS_CHILD";

    /// The child's exit code once its body returned. Neither a passing run
    /// (0, which a filter matching no test also gives) nor a failing one
    /// (101), so the parent knows the body ran to its end.
    const BODY_RETURNED: i32 = 17;

    /// Runs `body` in a child process: the test binary again, filtered to
    /// the test at `test` (its path below the crate root), which reaches
    /// this call with `CHILD_ENV` set and runs `body` there. The parent
    /// passes once the child's body has returned; a panic in it fails the
    /// child, and the parent with it.
    pub(crate) fn in_child(test: &str, body: impl FnOnce()) {
        if std::env::var_os(CHILD_ENV).is_some() {
            body();
            std::process::exit(BODY_RETURNED);
        }
        let status = Command::new(std::env::current_exe().unwrap())
            .args([test, "--exact", "--test-threads=1", "--nocapture"])
            .env(CHILD_ENV, "1")
            .status()
            .unwrap();
        assert_eq!(status.code(), Some(BODY_RETURNED), "child {test}: {status}");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{
        LocalConfig, Residency, ServerConfig, SourceConfig, StorageConfig, TableConfig,
    };

    /// The disk level the pre-launch review measured at 8 GiB.
    const EIGHT_GIB: u64 = 8 * 1024 * 1024 * 1024;

    /// Blocks in `EIGHT_GIB`: the 2,048 descriptors the review counted.
    const EIGHT_GIB_BLOCKS: u64 = 2_048;

    /// Blocks in four times `EIGHT_GIB`: the review's 32 GiB count.
    const THIRTY_TWO_GIB_BLOCKS: u64 = 8_192;

    #[test]
    fn test_needed_without_a_disk_level_is_each_load_and_the_margin() {
        assert_eq!(needed(0, 0), DESCRIPTOR_MARGIN);
        assert_eq!(
            needed(0, 1),
            approx::count(load_concurrency()) + DESCRIPTOR_MARGIN
        );
        let loads = 3;
        assert_eq!(
            needed(0, loads),
            approx::count(loads) * approx::count(load_concurrency()) + DESCRIPTOR_MARGIN
        );
    }

    #[test]
    fn test_needed_counts_one_descriptor_per_block() {
        assert_eq!(needed(EIGHT_GIB, 1) - needed(0, 1), EIGHT_GIB_BLOCKS);
        assert_eq!(
            needed(4 * EIGHT_GIB, 1) - needed(0, 1),
            THIRTY_TWO_GIB_BLOCKS
        );
    }

    #[test]
    fn test_needed_does_not_count_a_partial_block() {
        let one_and_a_half = PAGE_CACHE_DISK_BLOCK_BYTES + PAGE_CACHE_DISK_BLOCK_BYTES / 2;
        assert_eq!(needed(one_and_a_half, 1) - needed(0, 1), 1);
    }

    #[test]
    fn test_needed_by_reads_the_disk_level_and_counts_a_load_per_table() {
        let table = TableConfig {
            name: "kv".to_string(),
            source: SourceConfig::Static {
                location: "s3://warehouse/db/kv".to_string(),
            },
            r#ref: "main".to_string(),
            key_column: "key".to_string(),
            value_columns: vec![crate::config::ValueColumnConfig::named("value")],
            order_column: None,
            max_scan_rows: None,
            strict: true,
            lookups: None,
            residency: Residency::Full,
            resident_budget_bytes: None,
            row_cache: None,
        };
        let mut config = Config {
            server: ServerConfig::default(),
            storage: StorageConfig::default(),
            tables: vec![table.clone(), table],
        };
        assert_eq!(needed_by(&config), needed(0, 2));
        config.storage.local = Some(LocalConfig {
            dir: "/var/lib/icetray".into(),
            index_budget_bytes: None,
            page_cache_disk_bytes: EIGHT_GIB,
        });
        assert_eq!(needed_by(&config), needed(EIGHT_GIB, 2));
    }

    /// A soft limit the child lowers itself to, below any hard limit a
    /// test machine runs under.
    const LOWERED: u64 = 64;

    #[test]
    fn test_raise_lifts_the_soft_limit_to_what_the_kernel_grants() {
        testing::in_child(
            "descriptors::tests::test_raise_lifts_the_soft_limit_to_what_the_kernel_grants",
            || {
                let hard = current().rlim_max;
                set(LOWERED, hard).unwrap();
                assert_eq!(limit(), LOWERED);
                let raised = raise().unwrap();
                assert!(raised > LOWERED, "{raised}");
                assert_eq!(raised, limit());
                assert_eq!(current().rlim_cur, raised);
                assert_eq!(current().rlim_max, hard);
            },
        );
    }

    #[test]
    fn test_limit_is_within_the_soft_limit() {
        let limit = limit();
        assert!(limit > 0);
        assert!(limit <= current().rlim_cur);
    }
}
