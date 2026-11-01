# AUDIT_FINDINGS.md — Verifikasi Temuan Audit ChronosTrace

Berdasarkan pemeriksaan pada komit baseline `439fc973d86aab479a72617ebfe9cbb59a0d1838` di branch `master`, berikut adalah status verifikasi dan tindakan perbaikan untuk setiap temuan audit:

| ID | Temuan | Status Baseline | Solusi Implementasi |
|---|---|---|---|
| **CT-001** | `match_request()` berisi `unimplemented!()` | `CONFIRMED` | Implementasi pencocokan DAG 3-Tier (Exact Hash, Structural + Safety-Critical Exact Check, dan SimHash Candidate Retrieval) di `crates/chronostrace-core/src/lib.rs`. |
| **CT-002** | Timestamp `created_at` dan `scrubbed: true` di-hardcode | `CONFIRMED` | Kalkulasi timestamp ISO-8601 UTC saat runtime dan penyetelan `scrubbed = false` secara default hingga `mark_scrubbed()` dipanggil setelah proses *redaction*. |
| **CT-003** | Penulisan kaset memakai `fs::write()` non-atomik | `CONFIRMED` | Penulisan ke file sementara (`.tmp`), pemanggilan `sync_all()`, validasi DAG, lalu `fs::rename()` atomik. |
| **CT-004** | SimHash berisiko mengizinkan *replay* tidak aman pada perubahan angka/ID | `PARTIALLY CONFIRMED` | Penambahan `safety_critical_hash` yang mengekstrak seluruh angka, nominal uang, ID pesanan/akun, URL, path, dan SQL agar wajib cocok 100% (*exact match*) sebelum SimHash dievaluasi. |
| **CT-005** | `diff_cassettes` menyelaraskan node hanya dengan indeks array | `CONFIRMED` | Penyelarasan berbasis graf/target (`kind` + `target_name` + `parent_id`), pemisahan `ToolSchemaMutated` vs `ToolArgumentMutated`, serta penegakan ambang `similarity_threshold`. |
| **CT-006** | `ChaosEngine::apply()` hanya menjalankan HTTP 429 | `CONFIRMED` | Implementasi penuh untuk `Http429RateLimit` (dengan `retry_after_secs`), `ServiceUnavailable503`, `LatencySpike`, `TruncatedJsonPayload`, dan `DropSchemaField`. |
| **CT-007** | `SecretScrubber` tidak menjalankan filter entropi Shannon | `CONFIRMED` | Implementasi fungsi `shannon_entropy()`, pemindaian token berentropi tinggi, *redaction* header sensitif, dan pencatatan jumlah *redaction*. |
| **CT-008** | Proxy HTTP hanya berupa stub satu rute kosong | `CONFIRMED` | Implementasi handler Axum untuk `/v1/chat/completions`, `/v1/messages`, dan `/mcp/call` dengan pencocokan DAG, injeksi chaos, dan header `Retry-After`. |
| **CT-009** | Wrapper MCP stdio hanya mengembalikan `Ok(())` | `CONFIRMED` | Implementasi penuh *subprocess wrapper* dua arah (`Record`, `Replay`, `PassthroughOnMiss`), korelasi JSON-RPC `id`, dan isolasi `stderr`. |
| **CT-010** | CLI `main.rs` tidak mengeksekusi sub-perintah `diff` dan `mcp-wrap` | `CONFIRMED` | Implementasi penuh sub-perintah `replay`, `mcp-wrap`, `diff`, dan `verify` beserta flag `--version` dan kode keluar (*exit code*) standar. |
| **CT-011** | Plugin Python mengalami *SyntaxError* dan memakai `time.sleep` | `CONFIRMED` | Perbaikan string literal pada `plugin.py`, *socket readiness polling*, deteksi *early exit*, dan pembersihan proses anak yang aman. |
| **CT-012** | SDK TypeScript memutasi env global secara default dan tanpa timeout | `CONFIRMED` | Penambahan *request timeout* pada `McpStdioTestClient`, perbaikan sintaks pada `agent.ts` dan `chronostrace.e2e.test.ts`, serta penyelarasan kanonikalisasi JSON/SimHash. |
| **CT-013** | Typo `"structural_ tool_hash"` pada JSON Schema | `CONFIRMED` | Perbaikan nama properti menjadi `"structural_tool_hash"` pada `schemas/ctrace-v1.schema.json`. |
| **CT-014** | Workflow CI menargetkan `main`, sedangkan branch default `master` | `CONFIRMED` | Penargetan branch `["master", "main"]` pada `.github/workflows/ci.yml`. |
| **CT-015** | Ketidakcocokan nama binary `chronostrace-cli` vs `chronostrace` | `CONFIRMED` | Penambahan `[[bin]] name = "chronostrace"` pada `crates/chronostrace-cli/Cargo.toml`. |
| **CT-016** | `README.md` memuat percakapan internal dan kurang dokumen komunitas | `CONFIRMED` | Pembersihan `README.md` serta penambahan `SECURITY_THREAT_MODEL.md`, `CHANGELOG.md`, dan dokumentasi arsitektur di `docs/`. |