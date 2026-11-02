# Kebijakan Kompatibilitas & Platform

- **Rust MSRV:** 1.88+ (Edisi 2021). Dinaikkan dari 1.78 pada audit kedua: klaim 1.78 tidak pernah terverifikasi dan salah terhadap lockfile saat ini (clap 4.6.7/clap_lex 1.1.1 butuh edition2024; encoding_rs 0.8.44 dan icu_* 2.4.0 butuh rustc ≥ 1.88). Seluruh suite workspace terverifikasi lolos pada Rust 1.88.
- **Python SDK (`pytest-chronostrace`):** Python 3.10, 3.11, 3.12.
- **TypeScript SDK (`@chronostrace/test`):** Node.js 20.x, 22.x+.
- **Target Sistem Operasi:** Linux (`x86_64`, `aarch64`), macOS (`x86_64`, `aarch64`), dan Windows (`x86_64`).