# Contributing to ChronosTrace

Thank you for contributing to ChronosTrace.

## Before opening a pull request

Run the full verification suite locally from the repository root, and
make sure every command exits with code `0`:

```bash
# Rust: formatting, compilation, linting, and tests
cargo fmt --all -- --check
cargo check --workspace --all-targets --all-features
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace --all-features

# Build the release CLI binary
cargo build --release --package chronostrace-cli

# Python SDK: syntax check for the pytest plugin
python3 -m py_compile sdks/python/pytest_chronostrace/plugin.py
```

From `sdks/typescript`, with the release binary available:

```bash
export CHRONOSTRACE_BIN_PATH="$(pwd)/../../target/release/chronostrace"
npm install
npm run typecheck
npm test
```

From `sdks/python`, the plugin test suite can be run with:

```bash
pytest -v
```

## Engineering rules

- Never introduce `todo!()`, `unimplemented!()`, empty handlers, or
  fake-success stubs. Every new behavior must be implemented for real.
- Never report a test as passing unless the test command was actually run
  and exited with code `0`. Record the command and its exit code.
- All changes to matching, diffing, chaos profiles, or secret scrubbing
  must include unit or integration tests covering the change.
- Never commit real API keys, tokens, or unscrubbed production
  credentials in fixtures, cassettes, or documentation. Use test-only
  credentials when recording.
- Keep public documentation honest: only document features, commands,
  and options that exist in the current code and have been verified.
  Do not add benchmark or latency claims without a reproducible
  benchmark behind them.
- Keep generated build output out of commits. `target/`,
  `node_modules/`, `.pytest_cache/`, and similar build artifacts are
  ignored by `.gitignore` and must not be staged.

## Pull request scope

- Keep each pull request focused on one concern.
- Update `CHANGELOG.md` for user-visible behavior changes.
- Update the relevant documentation (`README.md`, `docs/`, or the SDK
  notes) when a command, option, or matching rule changes.
- If a change affects the cassette format, update
  `schemas/ctrace-v1.schema.json` and `docs/cassette-format.md` together.
