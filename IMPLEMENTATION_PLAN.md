# IMPLEMENTATION_PLAN.md — Rencana Eksekusi Penyempurnaan ChronosTrace

1. **Phase 1 (Foundation & Build Stabilization):** Menambahkan `.gitignore`, memperbaiki nama binary di `crates/chronostrace-cli/Cargo.toml`, memperbaiki typo di `schemas/ctrace-v1.schema.json`, memperbaiki *syntax error* pada SDK Python dan TypeScript, serta menyelaraskan branch CI ke `master`.
2. **Phase 2 (Cassette Model & Deterministic Core):** Mengimplementasikan `Cassette::match_request` 3-Tier dengan proteksi `safety_critical_hash`, validasi DAG bebas siklus, penulisan file atomik, dan timestamp UTC runtime.
3. **Phase 3 (Graph-Aware Trajectory Diff):** Memperbarui `diff_cassettes` agar menyelaraskan node berdasarkan identitas target dan jenis interaksi, menegakkan ambang `similarity_threshold`, serta memisahkan mutasi skema vs mutasi argumen.
4. **Phase 4 & 5 (HTTP Proxy & MCP Stdio Wrapper):** Mengimplementasikan server replay HTTP Axum dan pembungkus proses dua arah MCP JSON-RPC 2.0 (`Record`, `Replay`, `PassthroughOnMiss`) dengan isolasi penuh pada `stdout`.
5. **Phase 6 & 7 (Chaos Engineering & Secret Scrubbing):** Mengaktifkan seluruh 5 profil *chaos fault* dan pemindaian entropi Shannon beserta penghitung *redaction* terverifikasi.
6. **Phase 8, 9 & 10 (SDKs, CI/CD & Documentation):** Menjalankan pengujian unit/integrasi lintas Rust, Python, dan TypeScript serta melengkapi dokumentasi teknis.