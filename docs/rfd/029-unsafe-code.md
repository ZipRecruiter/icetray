# Unsafe code

> Updated 2026-11-14: the `unsafe` that predates this RFD is assessed:
> `local/residency` is named in RFD 017, the benchmark's CPU time moved
> to rustix, and the rest is recorded here as exempt. The cache slabs
> that would have managed shared memory were set aside (RFD 030).

## Problem

icetray has `unsafe` blocks in `icetray-core` and the testkit and no rule
for when one is acceptable. Most call the operating system: the size of
the descriptor table, the physical page count, `mincore` over a mapping,
macOS's memory queries. Others wrap the allocator to count allocations
in tests and benchmarks. A mistake in a system call often
fails loudly, with an error code or a crash, though a wrong length passed
to one can corrupt memory as silently as any other mistake. A mistake in
code that manages memory can corrupt a value another lookup returns and
fail nowhere. Caches that handed out values without copying them would
need `unsafe` that manages shared memory on the lookup path, which
icetray has not had. A reviewer needs a rule saying where `unsafe` may
go and what has to come with it.

## Decision

icetray will avoid `unsafe`, allowing it only in modules an RFD names,
where the RFD states the invariants the module's `unsafe` relies on and
the tests that check them, and every `unsafe` block will carry a
`SAFETY` comment naming the invariant it relies on.

- **Where.** An RFD that needs `unsafe` names the module that holds it
  and keeps it there, behind an API that safe code cannot misuse. Code
  outside that module does not gain `unsafe` by calling it.
- **What the RFD states.** The invariants, each as a sentence that a
  reviewer can check against the code, and the tests that would fail if
  one broke.
- **Tests for code that manages memory.** Rust code that manages memory
  itself, handing out pointers into buffers it owns, freeing or reusing
  them, or declaring a type `Send` or `Sync`, has tests that Miri runs,
  which catch use after free and reads out of bounds that ordinary tests
  pass over, and a test of many threads at once where the module is
  shared between threads. Miri runs only on a nightly toolchain, so the
  first module that needs it brings a pinned nightly used for that step
  alone, a script under `dev/`, and a CI job, together, as CONTRIBUTING
  has every second toolchain arrive.
- **Crates.** A crate whose purpose is a safe interface over `unsafe`
  operations, as rustix's is over system calls, is preferred to
  icetray's own `unsafe` for the operations it covers. A crate that only
  happens to hold the `unsafe` a feature needs is no way around this
  RFD: its `unsafe` runs in icetray's process, and a reviewer reads it
  as icetray's own.
- **Checked.** The workspace denies clippy's
  `undocumented_unsafe_blocks`, so the lint gate fails on a block without
  a `SAFETY` comment. Clippy checks that the comment is there; a reviewer
  checks that it names an invariant, and that the module is one an RFD
  names.

## Alternatives Considered

Forbidding `unsafe` outright. Rejected because icetray needs system
calls that the standard library does not cover, and because a cache
that handed out values without copying them would need a slice of a
buffer that other threads write elsewhere, which safe Rust cannot express.

`unsafe` allowed anywhere with a `SAFETY` comment. Rejected because a
comment states an invariant without anyone checking it, and nothing then
keeps memory management from spreading through modules one block at a
time.

Denying `unsafe_code` across the workspace, with an allow on each module
an RFD names. Not chosen: every named module would add a carve-out to
RFD 015 (Lint policy)'s rule against inner allows, and review against
this RFD catches the same thing while there are few such modules.

Moving `unsafe` into whatever third-party crate holds it. Rejected,
because a crate's `unsafe` runs in icetray's process and its bugs are
icetray's. A crate built to make a class of `unsafe` operations safe is
different: making them safe is what it is chosen for, and many projects
exercise its `unsafe`, where icetray's own would be exercised by icetray
alone.

## Consequences

The `unsafe` already in icetray predates this decision, and each module
that holds it has been assessed. Where rustix has a safe form of a
system call, icetray calls that instead, the benchmark's CPU time among
them. `local/residency`'s mapping, whose `mmap`, `mincore`, and `munmap`
rustix has no safe form of, is named in RFD 017 (Residency tiers) with
its invariants and tests. The rest is exempt, since none of it manages
memory:

- `descriptors`' `getdtablesize`, which takes no argument and reports
  macOS's per-process cap on open files, which neither rustix nor the
  soft limit gives.
- `memory`'s `sysconf` of the physical page count, a call by a constant
  name that reads no memory of icetray's.
- The counting allocators in `deletes/bound`'s tests and in the
  benchmark, which forward every call unchanged to the allocator beneath
  and count it, and are `unsafe` only because `GlobalAlloc` is.
- The testkit's `headroom`, whose macOS memory accounting only
  `task_info` and `sysctlbyname` give, each writing into a fixed-size
  buffer whose length goes with the call, in a crate that does not ship.

Every block among them carries a `SAFETY` comment.

Nothing mechanical stops `unsafe` from appearing in a module no RFD
names; review does. A feature that needs `unsafe` needs an RFD first,
which slows the first use of a technique and leaves later uses within
the same module free.
