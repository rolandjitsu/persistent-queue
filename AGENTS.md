# AGENTS.md

Guidance for AI coding agents in this repo. Human contributors: see [CONTRIBUTING.md](./CONTRIBUTING.md).

## Workflow

- Clarify the design before implementing. For anything non-trivial, agree on the approach first;
  prefer a short design note over jumping to code. `DESIGN.md` is the current working spec.
- One unit of change per commit. Never mix unrelated changes. Present the change for review
  before committing.
- Every change ships with tests. Run local CI before calling it done, and do not claim it passes
  without running it.
- Verify against the code and the tools: read before you answer, run before you assert.

Local CI:

```shell
cargo fmt --all --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test --all-features
```

## Writing: code, comments, docs, commits

- Concise and to the point. No fluff. Explain the non-obvious; do not narrate the obvious.
- ASCII only. No em-dash and no `--`; write `-`. Write `->` for the right arrow, `!=` for
  not-equal, straight quotes for curly ones, and the same for every other non-ASCII glyph.
  Applies everywhere, including this file.
- Comments justify *why*, not *what*. Delete any comment that restates the code.
- Do not use the word "seam"; say boundary, interface, or extension point. Do not use "bespoke";
  say "custom".
- Use "etc.", not "...", when a list trails off.
- Describe mechanics literally, not figuratively.

## Commits

- Conventional Commits (see CONTRIBUTING.md). Write the subject in the present tense, imperative
  voice: `feat: add group-commit`, not `added` or `adds`.
- Keep the body minimal. The subject alone is often enough; add body lines only for the
  non-obvious *why*. Do not restate the diff or enumerate every file changed.
- Subject <= 72 characters. Wrap body lines at 72 columns and keep the body to a few lines. The
  `committed` hook and CI enforce the line length.
- Disclose AI with an `Assisted-by: Claude:claude-opus-4-8` trailer. Never `Co-Authored-By`, and
  never add a human's `Signed-off-by`.

## Tests

- Unit tests inline (`#[cfg(test)] mod tests`); public-surface tests in `tests/`.
- Put helpers *after* the tests that use them.
- Coverage must not drop. Keep line coverage at or above its current level, and never below 80%
  (aim for 90%+). CI fails the build under 80%.
- Concurrency tests run on real threads. A short real sleep to observe that a push is *waiting*
  for room is the one accepted exception; keep it small.
- Backend tests that need on-disk state use `tempfile` and are gated behind their feature
  (`#[cfg(feature = "sled")]`, `#[cfg(feature = "redb")]`).

## Terminology

- **entry**: one queued item, stored under key `[0x01] ++ seq` (`u64` big-endian).
- **seq**: an entry's monotonic `u64` sequence number.
- **head / tail**: in-memory cursors derived from the min/max entry key on open. `head` is the
  oldest unacked seq; `tail` is the next seq to write.
- **reserve / ack / nack**: the consumer reserves an entry (marks it in-flight, in memory), then
  acks it (delete and advance) or nacks it (return for redelivery). Dropping a `Reserved` is a nack.
- **Reserved**: the guard the consumer gets from `reserve`; derefs to the item bytes.
- **durability**: how often a write is fsync'd - `Sync` (every op), `Group` (batch
  concurrent pushes behind one fsync), or `None` (no fsync; not crash-durable).
- **capacity**: the maximum number of unacked entries before `push` backpressures.

## Code conventions

- No `unsafe` in our code.
- The store is pure key/value bytes. Keep serialization out of the `Store` layer; it belongs in
  the caller or a codec feature.
- Backpressure is the point. Never introduce an unbounded path; any bound must be explicit and
  documented.
- Backends are optional dependencies behind features (`sled`, `redb`); the `mem` backend and the
  core are dependency-free. Do not add a dependency without a strong, stated reason.
- Document every public item with rustdoc; `#![warn(missing_docs)]` is on.

## CI workflows

- GitHub Actions live in `.github/workflows`. Write the workflow `name:`, every job name, and
  every named step in Sentence case, matching `ci.yml`.
- Keep workflows minimal and scoped to one purpose; prefer the built-in `GITHUB_TOKEN`.
