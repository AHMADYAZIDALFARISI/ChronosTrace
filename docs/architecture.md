# Arsitektur ChronosTrace

ChronosTrace memisahkan mesin inti deterministik dari lapisan transport protokol:
1. **`chronostrace-core`:** Model DAG kaset, kanonikalisasi JSON, pencocokan 3-Tier (`Exact`, `Structural + Safety-Critical`, `SimHash`), dan penganalisis *trajectory diff*.
2. **`chronostrace-scrub`:** Pipeline redaksi kredensial berbasis struktur JSON, regex, dan entropi Shannon.
3. **`chronostrace-chaos`:** Mesin injeksi gangguan deterministik berbasis PRNG Xorshift64*.
4. **`chronostrace-proxy`:** Adaptor protokol HTTP (Axum) dan pembungkus proses `stdio` MCP JSON-RPC 2.0.
5. **`chronostrace-cli` & SDKs:** Antarmuka baris perintah tunggal (`chronostrace`) serta pembungkus pengujian untuk `pytest` dan `Vitest`.