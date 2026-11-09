//! What a benchmark run asks of the machine's memory: how much a cache
//! budget needs available before the run starts, and how much of the
//! process's own memory the kernel may compress or swap out before the run
//! no longer measures caches held in RAM.
//!
//! A cache the kernel has compressed or swapped out still answers, slowly,
//! so a run under memory pressure reports the swap's latency as icetray's.
//! The benchmark refuses a budget that `needed_bytes` says will not fit in
//! `available_bytes`, and stops measuring once
//! `compressed_or_swapped_bytes` grows past `OUT_OF_RAM_LIMIT_BYTES` within
//! a run (`pushed_out`).

/// The resident memory a cache budget grows the benchmark process by, in
/// percent of the budget. jemalloc rounds each cached page up to its size
/// class, which for a page just past a class boundary is a quarter more
/// than the page, four classes standing to a doubling; and it keeps memory
/// freed by evictions resident a while before returning it. On the
/// scattered table of 128 files, whose pages fall just past 64 KiB, the
/// resident set grew by 1.51 to 1.54 bytes per byte of an 8 or 16 GiB
/// budget.
pub const RESIDENT_PERCENT_OF_BUDGET: u64 = 160;

/// The percent `RESIDENT_PERCENT_OF_BUDGET` is out of.
const PERCENT: u64 = 100;

/// The resident memory a run needs beside its caches' share: the fill's
/// requests in flight, the window's latency samples, and the lookups'
/// transient buffers, which at budgets of 512 MiB and 1 GiB grew the
/// resident set by up to 150 MiB more than the budget's share, with room
/// for a fitted workload's draw.
pub const RESIDENT_BESIDE_BUDGET_BYTES: u64 = 1 << 30;

/// The most bytes of the process's memory the kernel may come to hold
/// compressed or swapped out during one run, beyond what it held when the
/// run's fill began, before the benchmark stops: under 1 percent of the
/// budgets a sweep measures, so a cache whose pages it reaches have
/// stayed in RAM.
pub const OUT_OF_RAM_LIMIT_BYTES: u64 = 64 << 20;

/// Bytes in the kibibyte Linux's `/proc` files count in.
#[cfg(any(target_os = "linux", test))]
const KIB: u64 = 1 << 10;

/// The memory a run under a cache budget of `budget` bytes needs available
/// on the machine when it starts.
#[must_use]
pub fn needed_bytes(budget: u64) -> u64 {
    let share = u128::from(budget) * u128::from(RESIDENT_PERCENT_OF_BUDGET) / u128::from(PERCENT);
    u64::try_from(share)
        .unwrap_or(u64::MAX)
        .saturating_add(RESIDENT_BESIDE_BUDGET_BYTES)
}

/// How far `compressed_or_swapped_bytes` grew from `before` to `after`,
/// when it grew past `OUT_OF_RAM_LIMIT_BYTES`; `None` when it did not, or
/// either reading is unknown.
#[must_use]
pub fn pushed_out(before: Option<u64>, after: Option<u64>) -> Option<u64> {
    let grown = after?.saturating_sub(before?);
    (grown > OUT_OF_RAM_LIMIT_BYTES).then_some(grown)
}

/// The memory the kernel can hand a process now without compressing or
/// swapping anything out, or `None` where the platform does not say. On
/// Linux, `/proc/meminfo`'s `MemAvailable`. On macOS, the free pages, the
/// file-backed pages, whose contents the kernel can drop and read again,
/// the purgeable pages, and the pages processes have marked reusable.
#[must_use]
pub fn available_bytes() -> Option<u64> {
    platform::available_bytes()
}

/// The bytes of this process's memory the kernel holds compressed or
/// swapped out now, or `None` where the platform does not say. On macOS,
/// `task_vm_info`'s `compressed`, which counts compressed pages whether
/// their segment is in RAM or in swap. On Linux, `/proc/self/status`'s
/// `VmSwap`.
#[must_use]
pub fn compressed_or_swapped_bytes() -> Option<u64> {
    platform::compressed_or_swapped_bytes()
}

/// The bytes of `field` in a Linux `/proc` file of `Name:   N kB` lines,
/// such as `/proc/meminfo` and `/proc/self/status`.
#[cfg(any(target_os = "linux", test))]
fn kib_field(text: &str, field: &str) -> Option<u64> {
    text.lines().find_map(|line| {
        let (name, value) = line.split_once(':')?;
        if name != field {
            return None;
        }
        let kib: u64 = value.trim().strip_suffix("kB")?.trim().parse().ok()?;
        kib.checked_mul(KIB)
    })
}

#[cfg(target_os = "linux")]
mod platform {
    use super::kib_field;

    /// The file that says what memory Linux can hand out.
    const MEMINFO: &str = "/proc/meminfo";

    /// The file that says what of this process is swapped out.
    const STATUS: &str = "/proc/self/status";

    pub(super) fn available_bytes() -> Option<u64> {
        kib_field(&std::fs::read_to_string(MEMINFO).ok()?, "MemAvailable")
    }

    pub(super) fn compressed_or_swapped_bytes() -> Option<u64> {
        kib_field(&std::fs::read_to_string(STATUS).ok()?, "VmSwap")
    }
}

#[cfg(target_os = "macos")]
mod platform {
    use std::ffi::CStr;

    unsafe extern "C" {
        /// The calling task's own port, which `mach_task_self()` reads in
        /// C. libc deprecates its own binding in favor of the `mach2`
        /// crate, which declares this same symbol.
        static mach_task_self_: libc::mach_port_t;
    }

    /// `task_info`'s flavor for `task_vm_info`, from `<mach/task_info.h>`.
    const TASK_VM_INFO: libc::task_flavor_t = 22;

    /// The 64-bit words of `task_vm_info` through `compressed_lifetime`,
    /// its first revision (`TASK_VM_INFO_REV0_COUNT`): `virtual_size`,
    /// `region_count` and `page_size` sharing a word, then fifteen sizes.
    const TASK_VM_INFO_REV0_WORDS: usize = 18;

    /// The word of `task_vm_info` holding `compressed`.
    const COMPRESSED_WORD: usize = 15;

    /// The page counts `available_bytes` sums: free, file-backed,
    /// purgeable, and reusable.
    const AVAILABLE_PAGES: [&CStr; 4] = [
        c"vm.page_free_count",
        c"vm.page_pageable_external_count",
        c"vm.page_purgeable_count",
        c"vm.page_reusable_count",
    ];

    /// The size of the pages `AVAILABLE_PAGES` count.
    const PAGE_SIZE: &CStr = c"vm.pagesize";

    pub(super) fn available_bytes() -> Option<u64> {
        let mut pages: u64 = 0;
        for name in AVAILABLE_PAGES {
            pages = pages.checked_add(sysctl_count(name)?)?;
        }
        pages.checked_mul(sysctl_count(PAGE_SIZE)?)
    }

    pub(super) fn compressed_or_swapped_bytes() -> Option<u64> {
        let mut info = [0_u64; TASK_VM_INFO_REV0_WORDS];
        let mut count = libc::mach_msg_type_number_t::try_from(
            size_of_val(&info) / size_of::<libc::natural_t>(),
        )
        .ok()?;
        // SAFETY: `mach_task_self_` is a port the runtime sets before
        // `main` and never changes; `task_info` writes at most `count`
        // naturals into `info`, which holds exactly that many, and the
        // count it wrote into `count`.
        let status = unsafe {
            libc::task_info(
                mach_task_self_,
                TASK_VM_INFO,
                info.as_mut_ptr().cast(),
                &raw mut count,
            )
        };
        (status == libc::KERN_SUCCESS).then_some(info[COMPRESSED_WORD])
    }

    /// The unsigned integer the sysctl `name` holds, 32 or 64 bits wide.
    fn sysctl_count(name: &CStr) -> Option<u64> {
        let mut value = [0_u8; size_of::<u64>()];
        let mut len = value.len();
        // SAFETY: `sysctlbyname` writes at most `len` bytes into `value`,
        // and the count it wrote into `len`; it reads only `name`, which
        // is NUL-terminated.
        let status = unsafe {
            libc::sysctlbyname(
                name.as_ptr(),
                value.as_mut_ptr().cast(),
                &raw mut len,
                std::ptr::null_mut(),
                0,
            )
        };
        if status != 0 {
            return None;
        }
        match len {
            width if width == size_of::<u32>() => {
                let narrow: [u8; size_of::<u32>()] = value[..width].try_into().ok()?;
                Some(u64::from(u32::from_ne_bytes(narrow)))
            }
            width if width == size_of::<u64>() => Some(u64::from_ne_bytes(value)),
            _ => None,
        }
    }
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
mod platform {
    pub(super) fn available_bytes() -> Option<u64> {
        None
    }

    pub(super) fn compressed_or_swapped_bytes() -> Option<u64> {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_needed_bytes_scales_the_budget_and_adds_the_rest() {
        assert_eq!(
            needed_bytes(5 << 30),
            (8 << 30) + RESIDENT_BESIDE_BUDGET_BYTES
        );
        assert_eq!(needed_bytes(0), RESIDENT_BESIDE_BUDGET_BYTES);
        assert_eq!(needed_bytes(u64::MAX), u64::MAX);
    }

    #[test]
    fn test_pushed_out_only_past_the_limit() {
        let before = Some(1 << 20);
        let at_limit = Some((1 << 20) + OUT_OF_RAM_LIMIT_BYTES);
        let past = Some((1 << 20) + OUT_OF_RAM_LIMIT_BYTES + 1);
        assert_eq!(pushed_out(before, at_limit), None);
        assert_eq!(pushed_out(before, past), Some(OUT_OF_RAM_LIMIT_BYTES + 1));
        assert_eq!(pushed_out(past, before), None);
        assert_eq!(pushed_out(None, past), None);
        assert_eq!(pushed_out(before, None), None);
    }

    #[test]
    fn test_kib_field_reads_the_named_line() {
        let meminfo =
            "MemTotal:       65536 kB\nMemFree:         1024 kB\nMemAvailable:    4096 kB\n";
        assert_eq!(kib_field(meminfo, "MemAvailable"), Some(4096 * KIB));
        assert_eq!(kib_field(meminfo, "MemFree"), Some(1024 * KIB));
        assert_eq!(kib_field(meminfo, "Mem"), None);
        assert_eq!(kib_field(meminfo, "SwapFree"), None);
        assert_eq!(kib_field("VmSwap:\t  0 kB", "VmSwap"), Some(0));
        assert_eq!(kib_field("VmSwap: lots kB", "VmSwap"), None);
        assert_eq!(kib_field("VmSwap: 12", "VmSwap"), None);
    }

    #[cfg(any(target_os = "linux", target_os = "macos"))]
    #[test]
    fn test_platform_reads_this_machine() {
        let available = available_bytes().expect("the platform says what memory is available");
        assert!(available > 0, "no memory available reads as {available}");
        assert!(
            compressed_or_swapped_bytes().is_some(),
            "the platform says what of this process is out of RAM"
        );
    }
}
