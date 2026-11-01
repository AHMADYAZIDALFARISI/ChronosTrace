# Changelog

## [0.1.0] - 2026-11-01
### Added
- Mesin pencocokan DAG 3-Tier deterministik dengan proteksi `safety_critical_hash` di `chronostrace-core`.
- Penulisan file kaset atomik dan validasi DAG bebas siklus.
- Mesin *chaos fault injection* lengkap (HTTP 429, HTTP 503, Latency Spike, Truncated JSON, Drop Schema Field).
- Pipeline *secret scrubbing* dengan filter entropi Shannon dan pelacakan metadata redaksi.
- Pembungkus dua arah MCP `stdio` JSON-RPC 2.0 (`chronostrace mcp-wrap`) dan CLI pembanding lintasan (`chronostrace diff`).
- Plugin `pytest-chronostrace` dan SDK `@chronostrace/test` dengan *readiness polling* dan *request timeout*.