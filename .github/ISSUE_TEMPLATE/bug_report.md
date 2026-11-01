---
name: Bug report
about: Report a reproducible problem in ChronosTrace
title: "[bug] "
labels: ["bug"]
---

## Environment

- ChronosTrace version (`chronostrace --version`):
- Operating system and architecture:
- Rust version (`rustc --version`):
- Python version (if using the pytest plugin):
- Node.js version (if using the TypeScript SDK):
- Relevant crate or SDK (`chronostrace-core`, `chronostrace-scrub`,
  `chronostrace-chaos`, `chronostrace-proxy`, `chronostrace-cli`,
  Python plugin, or TypeScript SDK):

## Steps to Reproduce

1.
2.
3.

Include the smallest cassette, command, or test case that reproduces
the problem. If the problem involves matching or diffing, include the
recorded prompt text and the tool arguments involved.

## Expected vs Actual Behavior

**Expected:**

**Actual:**

Paste the exact error message or unexpected output, if any.

## Additional Context

Anything else that helps reproduce or diagnose the problem (logs,
cassette size, similarity threshold used, chaos settings).

## Security Reminder

Do **not** attach real API keys, tokens, credentials, or unscrubbed
production cassettes to this issue. Replace secrets with clearly marked
placeholders (for example `<redacted>`) and use test-only credentials
in any reproduction.
