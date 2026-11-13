#!/usr/bin/env bash
# Miri's gate: every test of each crate whose `unsafe` manages memory (RFD
# 029 (Unsafe code)) runs under Miri, which fails on undefined behavior
# that the ordinary tests pass over. One script, two callers: CI's miri job
# and a developer.
#
# Miri runs only on a nightly, so this script pins one, used for this step
# alone: rust-toolchain.toml's stable pin builds and tests everything else.
# The pin is the newest nightly that ships Miri for both aarch64-apple-darwin
# and x86_64-unknown-linux-gnu, and a bump is a commit of its own
# (CONTRIBUTING.md, Toolchain).
#
# Miri keeps its isolation from the host: Instant, which the slabs time
# their lock waits with, reads Miri's own clock under isolation, and no
# test reads a file or the environment.
set -euo pipefail

NIGHTLY=nightly-2026-11-12

log() { echo "==> $*"; }

# The nightly and its two components, from `rustup toolchain list` and
# `rustup component list`, which never install anything: `cargo +$NIGHTLY`
# on a missing toolchain can auto-install it without Miri.
has_nightly() {
  local toolchains components
  toolchains=$(rustup toolchain list)
  grep -q "^$NIGHTLY-" <<<"$toolchains" || return 1
  components=$(rustup component list --toolchain "$NIGHTLY" --installed)
  grep -q '^miri-' <<<"$components" && grep -qx 'rust-src' <<<"$components"
}

# `rustup toolchain install` adds the nightly beside the stable pin, and
# `+$NIGHTLY` selects it for one command. The minimal profile skips
# rustfmt, clippy, and the docs, which nothing here runs. Where rustup has
# no default toolchain, installing one can make it the default, so the
# default is put back as it was: the nightly is never the toolchain a bare
# `cargo` outside the repository runs.
if ! has_nightly; then
  # `rustup default` prints "<toolchain> (default)", and fails with none.
  if default=$(rustup default 2>/dev/null); then
    default=${default%% *}
  else
    default=none
  fi
  log "rustup toolchain install $NIGHTLY (miri, rust-src)"
  rustup toolchain install "$NIGHTLY" --profile minimal --component miri,rust-src
  rustup default "$default"
fi

log "cargo +$NIGHTLY miri test -p icetray-slab --locked"
cargo "+$NIGHTLY" miri test -p icetray-slab --locked
