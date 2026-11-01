# SECURITY_THREAT_MODEL.md — Model Ancaman & Kebijakan Keamanan ChronosTrace

## 1. Batasan Kepercayaan (Trust Boundaries)
- **Boundary 1 (Developer Test Process <-> ChronosTrace Proxy):** Lalu lintas lokal melalui loopback HTTP (`127.0.0.1`) atau pipa `stdio` JSON-RPC 2.0.
- **Boundary 2 (ChronosTrace <-> File Kaset `.ctrace` di Git):** File kaset disimpan di kontrol versi publik/privat sehingga berisiko membocorkan kredensial apabila tidak dibersihkan sebelum ditulis ke disk.
- **Boundary 3 (Fuzzy Matching <-> Safety-Critical Operations):** Pencocokan kemiripan teks (SimHash) berisiko menyetujui *replay* untuk operasi yang berbeda secara material (misalnya perubahan nominal `$50` menjadi `$500`) apabila tidak dibatasi oleh pemeriksaan *exact match* pada parameter kritikal.

## 2. Mitigasi yang Diimplementasikan
1. **Safety-Critical Exact Enforcement:** Seluruh angka, nominal uang, ID pesanan/pengguna, URL, path file, dan perintah SQL diekstrak ke dalam `safety_critical_hash` yang wajib cocok 100% sebelum kandidat SimHash dapat dipilih.
2. **Two-Stage Secret Scrubbing Before Persistence:** `SecretScrubber` memindai nama field sensitif (`api_key`, `authorization`, `password`, `secret`, `cookie`), pola regex kunci populer (`sk-*`, `ghp_*`, `AKIA*`, `Bearer`, Private Key), serta token berentropi Shannon `>= 4.25` bit/karakter sebelum kaset disimpan.
3. **Verified Scrubbing Metadata:** Atribut `metadata.scrubbed` bernilai `false` secara default dan hanya diubah menjadi `true` setelah pipeline `SecretScrubber` dijalankan, disertai pencatatan `scrubber_policy_version` dan `redaction_count`.

## 3. Risiko Residual & Keterbatasan
- Pemindaian berbasis regex dan entropi Shannon tidak menjamin penemuan 100% rahasia pendek berentropi rendah (seperti kata sandi pendek di dalam teks bebas). Pengguna disarankan menggunakan kredensial khusus pengujian saat merekam kaset.