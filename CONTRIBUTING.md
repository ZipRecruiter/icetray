<!-- source: github.com/bfs/rust-template CONTRIBUTING.md @ v0.1.0 -->

# Contributing

These are the project's Rust conventions. Humans and coding agents read the
same file; `AGENTS.md` adds only what agents need beyond it. Anything a lint
can enforce is enforced by `Cargo.toml` and `clippy.toml`, and the sections
below say so rather than restating the rule. The rest is here because no
tool can check it.

## Toolchain

- `rust-toolchain.toml` pins the exact stable version and the `rustfmt` and
  `clippy` components. CI installs it explicitly with `rustup toolchain
  install`. Locally, rustup installs it the first time you run `cargo` in
  the repository, through auto-install. Every rustup release except 1.28.0
  auto-installs by default. Since 1.28.1, `rustup set auto-install
  disable` or `RUSTUP_AUTO_INSTALL=0` turns it off. Without auto-install,
  run `rustup toolchain install` in the repository yourself. Bumping the
  pin is its own commit.
- `.flox/env/manifest.toml` declares the development environment: `flox
  activate` puts rustup and the container runtime on PATH. It is a
  convenience, not a requirement. Nothing in CI, the hooks, or the `dev/`
  scripts assumes it.

## Formatting

rustfmt defaults, no `.rustfmt.toml`, enforced by `cargo fmt --all --check`.
This is a decision, not an omission. Every stable toolchain and editor
produces the same output with no setup. The options worth having (import
grouping, comment wrapping) are nightly-only and would pin a second
toolchain for one tool. The stable options that remain either match the
defaults or duplicate clippy lints. If import grouping ever becomes worth
that cost, it arrives together with the nightly pin, a CI step, and a
paragraph here, never halfway.

## Lints

`[workspace.lints]` in the root `Cargo.toml` is the whole policy: clippy
pedantic denied, four pedantic lints allowed, `unwrap_used` and
`wildcard_enum_match_arm` denied, `missing_docs` warned. Every crate opts in with `[lints] workspace = true`.
CI runs clippy with `-D warnings` on all targets, so a warning is a failure.

- Do not add `#[allow(...)]` to get past a lint. Fix the code. If a lint is
  wrong for the whole project, change the workspace table in one commit that
  says why.
- The one exception is generated code: output a build script writes to
  `OUT_DIR` and an inline module `include!`s. That module may carry inner
  `#![allow(...)]` attributes, one lint per line, each added only after the
  generator's output tripped it. Nothing hand-written goes inside that module,
  the crate stays under the workspace lint table, and the attribute never
  widens to the crate or the file.
- The other exception is `clippy::cast_precision_loss`, allowed only inside
  `icetray_core::approx`. Integers become floats through its named helpers
  (`ratio`, `gauge`), so a call site says what the float is for, and the loss
  above 2^53 is accepted there by design. Identifiers never become floats;
  they go in metric labels and log fields.
- `#[allow(dead_code)]` never. Unused code is deleted, not kept for later,
  with one exception: code whose first reader lands later in the same
  series may carry `#[expect(dead_code, reason = "read by <what>")]`.
  When the reader arrives the expectation goes unmet and clippy fails, so
  that commit removes the marker. Code that only a test reads yet carries
  the same marker inside `cfg_attr(not(test), expect(dead_code, reason =
  "..."))`, and the reader's commit removes that one too.
  `dev/ci-checks.sh` refuses any tree that still carries either form, so a
  marker never reaches a push or a merge.
- `unwrap()` in production code never. `expect("reason")` marks a case that
  cannot happen and says why. `?` handles everything else. Tests may
  `unwrap()`; `clippy.toml` exempts them.

## Errors

- Every error is a variant of a `thiserror` enum with typed fields: an enum
  for the reason, a newtype for the id, a `Duration` for the timeout. A
  caller that needs to know what went wrong matches on the variant, and the
  compiler lists the cases it forgot. Nothing, tests included, decides
  anything by looking at an error's message: no `to_string().contains(..)`,
  no substring assertions. Messages exist for people reading a log.
- `thiserror` in library crates. `anyhow` only in a binary's `main`, to turn
  the typed error into an exit code or a gRPC status through one exhaustive
  match; it never wraps an error a caller might still inspect.
- Messages are lowercase, carry no trailing punctuation, and include the
  context a reader needs to act: the value, the path, the id.
- No swallowed errors. A `catch`-shaped block (a `let _ =`, an `if let Err`
  that does nothing, a `.ok()` on a `Result`) either logs and handles, logs
  and propagates, or carries an `// intentional: <reason>` comment.

## Types over strings

A closed set of values is an enum, never a string or a `&'static str` code:
finding codes, refusal reasons, tiers, key types, table states, metric label
values, writer property names. The string form is derived (`Display`, serde
`rename_all`) for logs, JSON output, and metric labels, and the only place a
string becomes the enum again is a boundary parser (configuration, the
command line) through one `FromStr`. Identifiers that must not be confused
with one another are newtypes: a snapshot id is not an `i64`, a byte offset
is not a row index. An index into a slice is the exception: it stays the
slice's `usize` and travels inside a view that captures it once (as
`ColumnChunk` does for a row group); a newtype is for a value that leaves
the slice it indexes. `clippy::wildcard_enum_match_arm` is denied, so a match
names every variant and a new variant fails to compile everywhere it matters
instead of falling into a `_` arm nobody reads. The test is always the same:
if something can or cannot happen, the compiler says so, not the presence or
absence of a substring.

## Stability

icetray follows semantic versioning and has not reached 1.0.0. Until it
does, any release may change what icetray itself defines: configuration
keys, metric and log field names, the doctor's output contract, protobuf
fields and their numbers, and the formats it writes to disk. Such a
change is made in place, with no alias for an old name, no reader for an
older layout, and no field number held open, since no deployment depends
on the old shape yet. Releases before 1.0.0 are cut to publish binaries
and images, and they promise nothing to one another.

A release raises the patch version for a change that leaves every
interface and every format icetray writes as it was. It raises the minor
version for a breaking internal change, such as a format icetray writes to
its local directory or a dependency upgrade that changes one, and, before
1.0.0, for a change to an interface. From 1.0.0, a change to the API or
another interface the contracts below protect raises the major version.
The page cache's disk level is kept across releases of one minor version
and emptied by any other (RFD 017 (Residency tiers)), so a patch release
starts warm.

From 1.0.0 these are contracts, and the rules here and in the RFDs that
protect them hold within a major version: a renamed key or metric, a
reused field number, or a changed output line waits for the next major
release. Formats icetray reads but does not define, such as Iceberg's
table metadata, manifests, and delete files, are outside this section:
icetray reads what writers produce, in every format version it supports.

## Protobuf

The `.proto` files are the client contract and follow the same rule. Every
closed set is an `enum` with a zero `_UNSPECIFIED` value; alternatives are a
`oneof`; opaque payloads are `bytes`; counts and ids use the narrowest
integer type that fits. No `map<string, string>`, no JSON or other
serialized text inside a string field, no `google.protobuf.Struct`, `Value`,
or `Any`. A field comment states what a caller may rely on, never how the
server produces it. From 1.0.0, field numbers are never reused
([Stability](#stability)).

## Constants

No magic numbers. Every numeric literal is a named `const` with a doc comment
saying what it bounds and why that value. The exceptions are `0` and `1` used
as booleans or initial values, and a `Vec::with_capacity` argument whose
meaning is obvious from the line it is on. A constant has one home: when two
crates need the same value, it is declared once in the lowest crate both
depend on and imported from there, never redeclared. A test fixture that
needs the page row limit imports `contract::PAGE_ROWS`; it does not write
`1_000` again. Beyond that, a test may use a literal when the test itself
says what it is: an expected outcome the assertion names ("no more than 7
GETs"), or input that is plainly made up (`[3, 4, 7, 3]`). A literal that
restates a production setting imports the production constant, and a
literal whose meaning the test leaves unsaid gets a name, if only a local
one (`let flux_capacitors_to_test = 4;`), so the reader is not left to
guess what the 4 is. Time is typed: elapsed time
is a `Duration`, a point in monotonic time an `Instant`, a wall-clock
timestamp a `jiff` type, and arithmetic on them uses their own methods. The
only place a time value becomes a bare number (a histogram in seconds, an
interval read from configuration, a log field) is one module of named
conversion helpers, so no `* 1000`, no `/ 86400`, no `as_millis()` at a call
site, and no hand-rolled epoch or DST math anywhere. The first occurrence is
where the helper goes.

## Dependencies

- Never write a version from memory. `cargo add <crate>` resolves it from
  the registry; the output it writes is the version to keep.
- Commit `Cargo.lock`. CI passes `--locked`, so a stale lock fails fast.
- Renovate batches Rust patch and minor bumps into one weekly PR and pins
  GitHub Actions to commit SHAs. Majors are manual.

## Layout and docs

- `lib.rs` re-exports the public API. Internals are `pub(crate)`. One
  concept per file; a concept's closed set of small types (a fault enum, a
  label value) lives with the type they serve, so a file holds the whole of
  one mechanism.
- Every crate root starts with a `//!` header saying what the crate is.
- Every public item has a `///` comment. `missing_docs` warns, and CI turns
  the warning into a failure.

## User documentation

A change a user can see is documented in the same change, in three places:

- **The guide for its reader.** What the feature does, when to use it, and
  what to watch for, in the document for the person it serves:
  [Calling icetray](docs/calling.md) for a service that sends requests,
  [The optimal layout](docs/table-layout.md) for whoever writes the table,
  and [Operating icetray](docs/operating.md) for whoever runs the server.
- **The reference.** Every configuration key and its default in
  [Configuration and the command line](docs/configuration.md), which is
  the one document that states a setting's default. The default itself
  is a named constant beside the setting in `config.rs`, and the guides
  name the setting and link the reference. Every metric
  and log field in the operating guide, and every protobuf field in its
  comment in `icetray.proto`. The TOML examples in the README and `docs/`
  are parsed by a test, so an example that no longer matches the schema
  fails the build.
- **The decision.** The RFD that records why, or an update note on the RFD
  it changes. An RFD names the settings it relies on and links the
  reference for their values; it states a value only where the value is
  the decision, with its reason (RFD 001, House rules).

The README describes a feature only when it changes how a reader thinks
about icetray, such as value columns or projection, in a short paragraph
with an example and a link to the guide.

The docs are written for engineers who know data lakes, object storage,
and gRPC. Use the term such an engineer uses, and name the actor that does
the thing: the owner configures a table, a caller requests columns, the
server responds, the page cache serves a page.

## Comments

A comment states a constraint the code has to satisfy, or it goes. Evergreen
voice: what is true now, not what used to be true, when it changed, or who
decided. If history matters, restate it as the live constraint ("hide, never
delete: deleting releases the id") and put the story in the commit message.
No comment sells the code's virtues. A constraint that takes a paragraph
gets a paragraph; a sentence carrying no load gets deleted.

## Testing

- Unit tests live in `#[cfg(test)] mod tests` at the bottom of the file they
  test. Integration tests in `crates/<name>/tests/` use the public API only.
- Names are `test_<fn>_<scenario>`.
- Assert on typed outcomes: a variant, a field, a count. Never on message
  text or log output.
- Put a test in the lowest tier that has what it needs. A property of one
  function is a unit test. An interaction between components that can run
  in-process is an in-process test. Only what genuinely needs sockets,
  processes, or containers goes to an end-to-end tier, and those run per
  batch, not per change.
- A test never sleeps a fixed time to let something happen: it waits for
  the condition, polling within a bound that fails with a message, or holds
  a fixture so the order is forced.
- A test never bets on how long an operation takes. If it needs work still
  in flight when it looks, or already finished, it makes that so: it holds
  the work behind a gate it opens itself, or waits for the work to finish.
  A read that is usually slower than one poll will, on some machine, finish
  within it.
- A test proves what it claims. Run it and read the output. If the number
  you assert on is zero when it should not be, the test passed without
  proving anything.
- A failing test is never "pre-existing, not my problem". Fix it now, or
  write down the symptom, the file and line, and the suspected cause where
  the fix will be scheduled. Ignoring it is not one of the choices.
- A change checked on only the axis you were thinking about is unchecked.
  Look at the other one before calling it done.

## Gates

- Per change: `cargo test` for the crate you touched.
- Per batch, before a push: `dev/ci-checks.sh` and `cargo test --workspace`.
  The pre-push hook runs the lint gate for you.
- The container-gated tests are separate and need a runtime:
  `dev/runtime.sh up`, `dev/rest-fixture.sh up`, then `dev/test-int.sh`.
- CI runs the same script and the same test command. There is one gate,
  called from three places.

## Review checklist

Check each of these against your own diff before asking for review.

- **DRY.** No duplicated logic blocks. When extracting, take the whole unit
  that repeats at the call sites, not the smallest shared fragment.
- **Consolidation.** DRY catches duplicated text; this catches duplicated
  mechanism. A change that adds a mechanism names what it supersedes and
  deletes it. Watch for two ways to do one thing, two guards for one hazard
  under different names, state derivable from other state, fallbacks nothing
  reaches, helpers that differ from an existing one by a parameter, and
  comments describing a mechanism that no longer exists. A diff that only
  adds needs a stated reason.
- **Small functions.** Over about 100 lines, split it.
- **No over-engineering.** No abstraction for a one-time operation. No
  hypothetical future requirement. Three similar lines beat a premature
  abstraction.
- **`f64` ordering** uses `f64::total_cmp`, never `partial_cmp().unwrap()`.
- **Boundaries are validated.** User input, files, and external APIs are
  checked where they enter. No injection paths.

## Commits

Conventional Commits: `<type>(<scope>): <description>`. Types are `feat`,
`fix`, `refactor`, `test`, `docs`, `chore`, `ci`, `perf`, `build`. Scopes are
crate names or the top-level area (`ci`, `docs`, `deps`). The message says
what changed and why, in enough detail that the reader does not need the diff
for the why.
No attribution trailers for tools or agents.
