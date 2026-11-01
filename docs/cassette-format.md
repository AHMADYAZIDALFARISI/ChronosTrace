# Spesifikasi Format Kaset `.ctrace` (v1.0)

Setiap file `.ctrace` adalah dokumen YAML/JSON yang divalidasi oleh `schemas/ctrace-v1.schema.json`:
- `version`: Konstanta `"1.0"`.
- `cassette_id`: Identitas unik kaset.
- `metadata`: Mencakup `created_at` (ISO-8601 UTC runtime), `similarity_threshold` (`0.5`–`1.0`), `scrubbed` (boolean), `scrubber_policy_version`, dan `redaction_count`.
- `nodes`: Daftar simpul DAG yang masing-masing memiliki `node_id` unik, `parent_id`, `kind` (`LlmCompletion`, `McpToolCall`, `HttpToolCall`), `exact_request_hash`, `prompt_simhash`, `structural_tool_hash`, dan `safety_critical_hash`.