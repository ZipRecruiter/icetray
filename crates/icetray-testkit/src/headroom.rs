//! What a benchmark run asks of the machine's memory: how much a cache
//! budget needs available before the run starts, and how much of the
//! process's own memory the kernel may push out of RAM, and read back,
//! before the run no longer measures caches held in RAM.
//!
//! A cache the kernel has compressed or swapped out still answers, slowly,
//! so a run under memory pressure reports the swap's latency as icetray's.
//! The benchmark refuses a budget that `needed_bytes` says will not fit in
//! `available_bytes`, and stops measuring once a run has waited on memory
//! out of RAM (`breach`).

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

/// The most bytes of the process's memory the kernel may read back into
/// RAM during one run, for faults that waited on a decompression or a swap
/// read, before the benchmark stops: 1,024 pages of 16 KiB. The kernel
/// compresses idle memory, such as a workload's drawn requests and what
/// jemalloc keeps of freed memory, whenever its free pages run low, even
/// with file cache it could drop; only a read of it delays a lookup.
pub const READ_BACK_LIMIT_BYTES: u64 = 16 << 20;

/// The most bytes of the process's memory the kernel may come to hold
/// compressed or swapped out during one run, beyond what it held when the
/// run began, before the benchmark stops, on a platform that does not
/// count what it reads back (`OutOfRam::read_back`): 3 percent of a 2 GiB
/// budget, and under 1 percent from 8 GiB.
pub const PUSHED_OUT_LIMIT_BYTES: u64 = 64 << 20;

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

/// What the kernel had done with this process's memory at one moment
/// (`out_of_ram`), each `None` where the platform does not say.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct OutOfRam {
    /// The bytes the kernel holds compressed or swapped out now. On macOS,
    /// `task_vm_info`'s `compressed`, which counts compressed pages whether
    /// their segment is in RAM or in swap. On Linux, `/proc/self/status`'s
    /// `VmSwap`.
    pub held: Option<u64>,
    /// The bytes the kernel has read back into RAM since the process
    /// started, for the process's faults on memory it had pushed out. On
    /// macOS, `task_vm_info`'s `decompressions` in pages of its
    /// `page_size`; swap there holds only compressed memory, so every read
    /// of it is a decompression. Linux does not count it per process.
    pub read_back: Option<u64>,
}

/// How a run waited on memory out of RAM (`breach`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Breach {
    /// The kernel read this many bytes back, past `READ_BACK_LIMIT_BYTES`.
    ReadBack(u64),
    /// The kernel came to hold this many bytes more out of RAM, past
    /// `PUSHED_OUT_LIMIT_BYTES`, on a platform that does not count what it
    /// reads back.
    PushedOut(u64),
}

/// How the process waited on memory out of RAM between `before` and
/// `after`, `None` when it did not or neither reading says. A platform
/// that counts what it reads back is judged on that alone.
#[must_use]
pub fn breach(before: OutOfRam, after: OutOfRam) -> Option<Breach> {
    if let (Some(from), Some(to)) = (before.read_back, after.read_back) {
        let grown = to.saturating_sub(from);
        return (grown > READ_BACK_LIMIT_BYTES).then_some(Breach::ReadBack(grown));
    }
    let grown = after.held?.saturating_sub(before.held?);
    (grown > PUSHED_OUT_LIMIT_BYTES).then_some(Breach::PushedOut(grown))
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

/// What the kernel has done with this process's memory now.
#[must_use]
pub fn out_of_ram() -> OutOfRam {
    platform::out_of_ram()
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

    pub(super) fn out_of_ram() -> super::OutOfRam {
        super::OutOfRam {
            held: std::fs::read_to_string(STATUS)
                .ok()
                .and_then(|status| kib_field(&status, "VmSwap")),
            read_back: None,
        }
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

    /// The naturals of `task_vm_info` through `decompressions`
    /// (`TASK_VM_INFO_REV5_COUNT`), which `<mach/task_info.h>` packs to 4
    /// bytes.
    const TASK_VM_INFO_REV5_COUNT: usize = 87;

    /// The naturals of `task_vm_info` through `compressed_lifetime`
    /// (`TASK_VM_INFO_REV0_COUNT`), which a kernel too old to count
    /// decompressions still writes.
    const TASK_VM_INFO_REV0_COUNT: usize = 36;

    /// The natural of `task_vm_info` holding `page_size`.
    const PAGE_SIZE_NATURAL: usize = 3;

    /// The first of the two naturals of `task_vm_info` holding
    /// `compressed`.
    const COMPRESSED_NATURAL: usize = 30;

    /// The natural of `task_vm_info` holding `decompressions`.
    const DECOMPRESSIONS_NATURAL: usize = 86;

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

    pub(super) fn out_of_ram() -> super::OutOfRam {
        let info = task_vm_info().unwrap_or_default();
        let natural = |at: usize| info.get(at).copied();
        let held = natural(COMPRESSED_NATURAL)
            .zip(natural(COMPRESSED_NATURAL + 1))
            .map(|(first, second)| wide(first, second));
        let read_back = natural(DECOMPRESSIONS_NATURAL)
            .zip(natural(PAGE_SIZE_NATURAL))
            .and_then(|(pages, size)| u64::from(pages).checked_mul(u64::from(size)));
        super::OutOfRam { held, read_back }
    }

    /// The 64-bit field `task_vm_info` holds in the naturals `first` and
    /// `second`, in memory order.
    fn wide(first: libc::natural_t, second: libc::natural_t) -> u64 {
        let (low, high) = if cfg!(target_endian = "little") {
            (first, second)
        } else {
            (second, first)
        };
        (u64::from(high) << libc::natural_t::BITS) | u64::from(low)
    }

    /// The naturals of this task's `task_vm_info` the kernel wrote, through
    /// `decompressions` at most.
    fn task_vm_info() -> Option<Vec<libc::natural_t>> {
        let mut info: [libc::natural_t; TASK_VM_INFO_REV5_COUNT] = [0; TASK_VM_INFO_REV5_COUNT];
        let mut count = libc::mach_msg_type_number_t::try_from(info.len()).ok()?;
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
        let written = usize::try_from(count).ok()?;
        (status == libc::KERN_SUCCESS && written >= TASK_VM_INFO_REV0_COUNT)
            .then(|| info.get(..written).map(<[_]>::to_vec))
            .flatten()
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

    #[cfg(test)]
    mod tests {
        use super::wide;

        #[test]
        fn test_wide_joins_two_naturals_in_memory_order() {
            let field: u64 = 0x0123_4567_89ab_cdef;
            let bytes = field.to_ne_bytes();
            let (first, second) = bytes.split_at(size_of::<libc::natural_t>());
            let natural =
                |half: &[u8]| libc::natural_t::from_ne_bytes(half.try_into().expect("four bytes"));
            assert_eq!(wide(natural(first), natural(second)), field);
        }
    }
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
mod platform {
    pub(super) fn available_bytes() -> Option<u64> {
        None
    }

    pub(super) fn out_of_ram() -> super::OutOfRam {
        super::OutOfRam::default()
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

    fn reading(held: Option<u64>, read_back: Option<u64>) -> OutOfRam {
        OutOfRam { held, read_back }
    }

    #[test]
    fn test_breach_judges_reads_back_where_the_platform_counts_them() {
        let start = 1 << 20;
        let before = reading(Some(0), Some(start));
        let held_lots = Some(PUSHED_OUT_LIMIT_BYTES * 4);
        assert_eq!(
            breach(
                before,
                reading(held_lots, Some(start + READ_BACK_LIMIT_BYTES))
            ),
            None
        );
        assert_eq!(
            breach(
                before,
                reading(Some(0), Some(start + READ_BACK_LIMIT_BYTES + 1))
            ),
            Some(Breach::ReadBack(READ_BACK_LIMIT_BYTES + 1))
        );
        assert_eq!(breach(before, reading(Some(0), Some(0))), None);
    }

    #[test]
    fn test_breach_judges_growth_held_out_elsewhere() {
        let before = reading(Some(1 << 20), None);
        let at_limit = reading(Some((1 << 20) + PUSHED_OUT_LIMIT_BYTES), None);
        let past = reading(Some((1 << 20) + PUSHED_OUT_LIMIT_BYTES + 1), None);
        assert_eq!(breach(before, at_limit), None);
        assert_eq!(
            breach(before, past),
            Some(Breach::PushedOut(PUSHED_OUT_LIMIT_BYTES + 1))
        );
        assert_eq!(breach(past, before), None);
        assert_eq!(breach(reading(None, None), past), None);
        assert_eq!(breach(before, reading(None, Some(1 << 40))), None);
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
        let now = out_of_ram();
        assert!(
            now.held.is_some(),
            "the platform says what of this process is out of RAM"
        );
        #[cfg(target_os = "macos")]
        assert!(
            now.read_back.is_some(),
            "macOS says what of this process it read back"
        );
    }
}
