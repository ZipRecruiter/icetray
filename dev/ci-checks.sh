#!/usr/bin/env bash
# The lint gate. One script, three callers: CI's lint job, the pre-push hook,
# and a developer. Change it here and every caller changes with it.
# Tests are not here on purpose: this stays fast enough to run on every push.
set -euo pipefail

log() { echo "==> $*"; }

log "cargo fmt --check"
cargo fmt --all --check

log "cargo clippy --workspace --all-targets --locked -- -D warnings"
cargo clippy --workspace --all-targets --locked -- -D warnings

# Code may land a commit ahead of its first reader under
# #[expect(dead_code)], or under the same inside cfg_attr(not(test), ..) when
# only a test reads it yet (rustfmt may leave the expect alone on its line),
# but never in a tree that is pushed or merged (CONTRIBUTING.md, Lints). Once
# the code is read the expectation goes unmet and clippy fails, so the
# reader's commit removes the marker; this catches a reader that never came.
log "no code staged under #[expect(dead_code)]"
if grep -rnE --include='*.rs' '(#!?\[|cfg_attr\([^]]*|^[[:space:]]*)expect\(([^]]*[ ,(])?(dead_code|unused)[ ,)]' crates; then
  echo "staged code must gain its reader before it is pushed: see CONTRIBUTING.md, Lints" >&2
  exit 1
fi
