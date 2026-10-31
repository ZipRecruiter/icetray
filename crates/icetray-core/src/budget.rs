//! The cache budget (RFD 028): one number of bytes the row cache and the
//! page cache's memory level hold together, set by `server.cache_bytes` or
//! derived from the process's memory limit, split between the two by
//! `server.row_cache_share` with most of it going to rows.

use std::sync::atomic::{AtomicU64, Ordering};

use crate::approx;
use crate::cache::check_capacity;
use crate::config::{
    ConfigViolation, DEFAULT_CACHE_SHARE_OF_LIMIT, DEFAULT_ROW_CACHE_SHARE, FALLBACK_CACHE_BYTES,
    ServerConfig,
};
use crate::memory::MemoryLimit;
use crate::metric;

/// The bytes each cache holds in memory, and the memory limit they were
/// derived from.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Budget {
    /// The row cache's budget: zero when no table reads through it.
    pub row_bytes: u64,
    /// The page cache's memory level's budget: the rest.
    pub page_bytes: u64,
    /// The memory limit the whole was derived from, when the file sets no
    /// `cache_bytes`; `None` when it does, or when no limit could be read
    /// and the whole is `FALLBACK_CACHE_BYTES`.
    pub derived_from: Option<MemoryLimit>,
}

impl Budget {
    /// The bytes both caches hold together.
    #[must_use]
    pub fn whole(&self) -> u64 {
        self.row_bytes + self.page_bytes
    }
}

/// The bytes by which the budget and the resident indexes overrun the
/// memory limit (RFD 028), as the last check found them: the daemon
/// records it after the first loads and after each reload, and every scrape
/// publishes it, since the recorder drops a gauge nothing writes between
/// two scrapes. Reads zero before the first check.
#[derive(Debug, Default)]
pub struct Shortfall(AtomicU64);

impl Shortfall {
    /// Records `bytes` as what a check found missing; zero when nothing is.
    pub fn record(&self, bytes: u64) {
        self.0.store(bytes, Ordering::Relaxed);
    }

    /// The bytes the last check found missing.
    pub fn bytes(&self) -> u64 {
        self.0.load(Ordering::Relaxed)
    }

    /// Sets `cache_budget_shortfall_bytes` to the bytes recorded.
    pub fn publish(&self) {
        metrics::gauge!(metric::CACHE_BUDGET_SHORTFALL_BYTES).set(approx::gauge(self.bytes()));
    }
}

/// The budget `server` sets under the memory `limit`, for caches whose
/// memory levels split into `shards`. The whole is `cache_bytes`, else
/// `DEFAULT_CACHE_SHARE_OF_LIMIT` of the limit, else
/// `FALLBACK_CACHE_BYTES`. When `rows_read`, some table reads through the
/// row cache, and the row cache holds `row_cache_share` of the whole
/// (`DEFAULT_ROW_CACHE_SHARE` when unset); otherwise it holds nothing.
/// The page cache holds the rest, which below one value page per shard is
/// `ConfigViolation::PageCacheTooSmall`, naming the limit when the whole
/// was derived from it. `row_cache_share` is in `[0, 1]`, as validation
/// holds it.
pub fn resolve(
    server: &ServerConfig,
    limit: Option<MemoryLimit>,
    rows_read: bool,
    shards: usize,
) -> Result<Budget, ConfigViolation> {
    let (whole, derived_from) = match (server.cache_bytes, limit) {
        (Some(bytes), _) => (bytes, None),
        (None, Some(limit)) => (
            share_of(limit.bytes, DEFAULT_CACHE_SHARE_OF_LIMIT),
            Some(limit),
        ),
        (None, None) => (FALLBACK_CACHE_BYTES, None),
    };
    let row_bytes = if rows_read {
        share_of(
            whole,
            server.row_cache_share.unwrap_or(DEFAULT_ROW_CACHE_SHARE),
        )
    } else {
        0
    };
    let page_bytes = whole - row_bytes;
    check_capacity(page_bytes, shards, derived_from)?;
    Ok(Budget {
        row_bytes,
        page_bytes,
        derived_from,
    })
}

/// The largest count of bytes, at most `whole`, whose ratio to `whole` is
/// at most `share`, found by bisection so no float is cast to an integer.
/// `share` is in `[0, 1]`, so zero bytes always qualifies and `whole` does
/// at a share of one.
fn share_of(whole: u64, share: f64) -> u64 {
    let (mut low, mut high) = (0, whole);
    while low < high {
        let middle = low + (high - low).div_ceil(2);
        if approx::ratio(middle, whole) <= share {
            low = middle;
        } else {
            high = middle - 1;
        }
    }
    low
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cache::min_memory_bytes;
    use crate::memory::LimitSource;

    const MIB: u64 = 1024 * 1024;
    const GIB: u64 = 1024 * MIB;
    /// The shards the tests' caches split into, as on a 16-worker host.
    const SHARDS: usize = 16;
    /// The limit the tests derive from: 8 GiB, half of it the budget.
    const LIMIT_BYTES: u64 = 8 * GIB;
    /// Nine tenths of 4 GiB, rounded down: the row cache's default part of
    /// half the limit.
    const ROWS_OF_HALF_THE_LIMIT: u64 = 3_865_470_566;

    fn limit(of: LimitSource) -> MemoryLimit {
        MemoryLimit {
            bytes: LIMIT_BYTES,
            of,
        }
    }

    #[test]
    fn test_budget_defaults_to_half_the_cgroup_limit() {
        let cgroup = limit(LimitSource::Cgroup);
        let budget = resolve(&ServerConfig::default(), Some(cgroup), true, SHARDS).unwrap();
        assert_eq!(
            budget,
            Budget {
                row_bytes: ROWS_OF_HALF_THE_LIMIT,
                page_bytes: 4 * GIB - ROWS_OF_HALF_THE_LIMIT,
                derived_from: Some(cgroup),
            }
        );
    }

    #[test]
    fn test_budget_on_a_host_without_a_cgroup_limit_is_half_the_host() {
        let host = limit(LimitSource::Host);
        let budget = resolve(&ServerConfig::default(), Some(host), true, SHARDS).unwrap();
        assert_eq!(budget.row_bytes + budget.page_bytes, 4 * GIB);
        assert_eq!(budget.row_bytes, ROWS_OF_HALF_THE_LIMIT);
        assert_eq!(budget.derived_from, Some(host));
    }

    #[test]
    fn test_budget_without_a_limit_is_the_fallback() {
        let budget = resolve(&ServerConfig::default(), None, true, SHARDS).unwrap();
        assert_eq!(budget.row_bytes + budget.page_bytes, FALLBACK_CACHE_BYTES);
        assert_eq!(
            budget.row_bytes,
            share_of(FALLBACK_CACHE_BYTES, DEFAULT_ROW_CACHE_SHARE)
        );
        assert_eq!(budget.derived_from, None);
    }

    #[test]
    fn test_cache_bytes_set_wins_over_the_limit() {
        let set = 2 * GIB;
        let server = ServerConfig {
            cache_bytes: Some(set),
            row_cache_share: Some(0.5),
            ..ServerConfig::default()
        };
        let budget = resolve(&server, Some(limit(LimitSource::Cgroup)), true, SHARDS).unwrap();
        assert_eq!(
            budget,
            Budget {
                row_bytes: GIB,
                page_bytes: GIB,
                derived_from: None,
            }
        );
    }

    #[test]
    fn test_no_row_reading_table_gives_pages_the_whole_budget() {
        let cgroup = limit(LimitSource::Cgroup);
        let budget = resolve(&ServerConfig::default(), Some(cgroup), false, SHARDS).unwrap();
        assert_eq!(
            budget,
            Budget {
                row_bytes: 0,
                page_bytes: 4 * GIB,
                derived_from: Some(cgroup),
            }
        );
    }

    #[test]
    fn test_a_derived_budget_too_small_for_the_page_minimum_names_its_limit() {
        let small = MemoryLimit {
            bytes: 4 * MIB,
            of: LimitSource::Cgroup,
        };
        let refused = resolve(&ServerConfig::default(), Some(small), true, SHARDS).unwrap_err();
        let page_bytes = 2 * MIB - share_of(2 * MIB, DEFAULT_ROW_CACHE_SHARE);
        assert_eq!(
            refused,
            ConfigViolation::PageCacheTooSmall {
                bytes: page_bytes,
                minimum: min_memory_bytes(SHARDS),
                shards: SHARDS,
                limit: Some(small),
            }
        );
    }

    #[test]
    fn test_share_of_is_exact_at_the_ends_and_rounds_down() {
        assert_eq!(share_of(0, DEFAULT_ROW_CACHE_SHARE), 0);
        assert_eq!(share_of(GIB, 0.0), 0);
        assert_eq!(share_of(GIB, 1.0), GIB);
        assert_eq!(share_of(u64::MAX, 1.0), u64::MAX);
        assert_eq!(
            share_of(4 * GIB, DEFAULT_ROW_CACHE_SHARE),
            ROWS_OF_HALF_THE_LIMIT
        );
    }
}
