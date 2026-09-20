# OT Monitoring — Tractor and Device

Dashboard pemantauan armada traktor dan perangkat presisi: status unit,
kerusakan, lisensi, operasional tim, dan gudang. Aplikasi web statis
(HTML + CSS + JavaScript tanpa build) dengan Firebase Authentication dan
Cloud Firestore.

---

## Menjalankan di komputer sendiri

```bash
npm run serve          # menyajikan folder ini di http://localhost:8765
```

Tidak ada langkah build. Berkas yang diedit langsung tampil setelah reload.

---

## Deploy

Aplikasi ini dilayani **GitHub Pages dari branch kerja**, bukan dari `main`.
Cukup push ke branch tersebut; Pages membangun ulang dalam satu-dua menit.

Setelah deploy, **lihat pojok kanan bawah halaman**. Di situ tertulis versi yang
sedang berjalan, misalnya `v96`. Kalau angkanya belum berubah setelah Anda
deploy, yang terbuka masih versi lama — bukan kode Anda yang salah.

Kalau versinya tetap tidak berubah padahal deploy sudah jalan, service worker
lama masih memegang berkas: buka DevTools → Application → Service Workers →
**Unregister**, lalu reload sekali. Ini hanya perlu sekali.

### Setiap deploy wajib menaikkan dua angka

`APP_VERSION` di `script.js` dan `CACHE_NAME` di `service-worker.js` harus
dinaikkan bersamaan. Keduanya menandai build yang sama; kalau `CACHE_NAME`
tidak naik, service worker tidak akan mengambil berkas baru.

---

## ⚠️ Firestore rules — langkah yang paling sering terlewat

`firestore.rules` **tidak ikut ter-deploy** bersama aplikasi. Berkas itu harus
di-paste manual:

> Firebase Console → Firestore Database → Rules → tempel seluruh isi
> `firestore.rules` → **Publish**

**Publish ulang setiap kali:**

- ada koleksi Firestore baru,
- ada area akses baru di `ACCESS_AREAS` (`script.js`),
- aturan tulis sebuah koleksi berubah.

Kalau terlewat, aplikasinya tetap terbuka dan tombolnya tetap ada, tetapi
penyimpanan ditolak diam-diam oleh server. Beberapa halaman menampilkan spanduk
merah "Firestore rules memblokir …" saat ini terjadi — itu petunjuknya.

Daftar owner di `firestore.rules` harus sama persis dengan `OWNER_EMAILS` di
`firebase-init.js`.

---

## Model akses

Setiap akun punya satu **role** dan sebuah peta **akses per area**.

| Role | Arti |
|---|---|
| `owner` | Super Admin — akses penuh, tidak bisa dikunci keluar |
| `staff`, `pbt`, `khl` | Label saja; **tidak memberi akses apa pun** sampai diatur per area |
| `team`, `viewer` | Role lama, masih dihormati apa adanya |

Area yang bisa diatur (`ACCESS_AREAS` di `script.js`) — tiap area bernilai
`none` / `view` / `edit`:

`editUnits`, `implements`, `damage`, `licenseStock`, `teamShift`, `teamLog`,
`teamMembers`, `teamLogApprove`, `warehouse`, `history`, plus `csv`
(`none` / `export` / `full`).

`teamLogApprove` sengaja terpisah dari `teamLog`: seorang pemeriksa bisa
menyetujui laporan tanpa bisa mengubah isinya — dan pemisahan itu ditegakkan
oleh Firestore rules, bukan hanya oleh tampilan.

**Satu akun hanya boleh aktif di satu perangkat.** Login terbaru menang;
perangkat lain keluar sendiri.

---

## Menjalankan uji

```bash
npm install            # sekali saja
npm test               # menyalakan server sendiri, lalu menjalankan semua suite
```

606 pemeriksaan di tujuh belas suite, menggerakkan Chromium sungguhan terhadap
aplikasi yang disajikan. Kalau mesin Anda sudah punya Chromium dan tidak ingin
Playwright mengunduh miliknya:

```bash
CHROME_PATH=/jalur/ke/chrome npm test
```

Suite-nya: `team_logic`, `roles_test`, `company_test`, `adjust_test`,
`session_test`, `warehouse_test`, `approval_test`, `leader_test`, `recap_test`,
`photos_test`, `migration_test`, `history_scan_test`, `validation_test`,
`datacheck_test`, `damagephotos_test`, `mobile_test`, `regress`.

---

## Susunan berkas

| Berkas | Isi |
|---|---|
| `index.html` | Seluruh tampilan: sembilan view dan semua modal |
| `script.js` | Seluruh logika aplikasi |
| `style.css` | Gaya, termasuk mode kartu untuk layar ponsel |
| `firebase-init.js` | Jembatan Firebase → `window.cloud` |
| `firestore.rules` | Aturan keamanan — **di-publish manual** |
| `service-worker.js` | Cache PWA; `CACHE_NAME` dinaikkan tiap deploy |
| `tests/` | Uji browser |

---

## Catatan untuk yang melanjutkan

- **Foto disimpan sebagai data URL, bukan di Cloud Storage** — tetapi di
  koleksinya sendiri, bukan di dalam dokumen yang dilanggan. Semuanya
  dikecilkan dulu (1024px / 200 KB) dan dibatasi jumlahnya. Kalau suatu hari
  pindah ke Cloud Storage, dua koleksi di bawah ini yang diganti.
- **Foto laporan harian ada di koleksi terpisah `workLogPhotos/{idLaporan}`,
  dan koleksi itu tidak pernah di-`onSnapshot`.** Dokumen laporannya hanya
  menyimpan `photoCount`. Ini disengaja: `subscribeWorkLogs` berlangganan
  seluruh koleksi tanpa `limit()`, jadi selama foto ada di dalam dokumen
  laporan, setiap perangkat mengunduh ulang semua foto setiap kali aplikasi
  dibuka. Jangan tambahkan langganan ke `workLogPhotos` — ambil per dokumen
  lewat `loadWorkLogPhotos(id)`.
- **Foto kerusakan ada di `damagePhotos/{idCatatan}`**, dengan pengaturan yang
  sama persis dan alasan yang sama: `damageRecords` juga dilanggan utuh tanpa
  `limit()`. Catatannya hanya menyimpan `hasPhoto`; ambil lewat
  `loadDamagePhoto(id)`. Jangan pernah melanggan koleksi ini.
  **Tidak ada lagi data URL yang disimpan di dalam dokumen Firestore** —
  kalau Anda hendak menambahkan satu, pisahkan sejak awal.
- **Tulisan cloud lewat `cloudWrite()`.** Jangan panggil `logEvent()` di dalam
  `.then()` sebuah tulisan Firestore: offline, promise-nya tidak pernah selesai
  dan jejak auditnya hilang.
- **⚠️ Jangan pernah menulis migrasi data yang dijaga hanya penanda
  `localStorage`.** Penanda itu per-browser, bukan per-database: ganti
  perangkat, hapus data situs, atau install ulang PWA, dan migrasinya jalan
  lagi. Empat migrasi seperti itu pernah ada di sini (nickname, lisensi ×2,
  site) dan menimpa 366 field dengan data Excel Mei 2026 setiap kali seseorang
  membukanya di browser baru — termasuk mengembalikan unit ke PT yang salah.
  Semuanya sudah dihapus. Kalau memang perlu migrasi, tiru
  `applyDefaultLicensesIfNeeded()` (`script.js`): `isOwner()` **dan** hanya
  mengisi yang kosong (`if (!unit.gpsLicense)`), bukan menimpa yang berbeda.
- **Catatan riwayat bisa berbohong, dan ada alat untuk itu.** `logEvent()`
  menulis sebelum server menjawab, jadi tulisan yang ditolak tetap
  meninggalkan barisnya. `/history` diizinkan oleh `canEditAnything()` — edit
  di area **mana pun** — sementara tiap koleksi dijaga areanya sendiri, jadi
  catatan bisa lolos walau datanya ditolak. Tombol **Periksa** di modal History
  (khusus owner) memindai seluruh koleksi dan menawarkan menghapus baris yang
  nilainya terbukti tidak pernah bergerak. Sengaja dibuat sempit: hanya update
  field unit, hanya baris terbaru per unit+field, dan hanya kalau nilainya
  masih persis seperti sebelum perubahan.
- **Semua penulis `units` mengunci dirinya sendiri** lewat `canWriteUnits()`,
  bukan hanya di pemanggil. Pemanggil baru tidak boleh mewarisi nol
  perlindungan seperti dulu.
- **Ukuran untuk ponsel ada di satu blok di ujung `style.css`**
  (`@media (max-width: 700px)`), dan dijaga `tests/mobile_test.js`. Aturan yang
  tidak boleh dilanggar: **setiap field form minimal `16px`.** Safari iPhone
  nge-zoom halaman tiap kali field di bawah 16px difokuskan dan tidak pernah
  kembali — satu field 13px membuat seluruh aplikasi membesar sampai pengguna
  reload. Sisanya soal ukuran sentuh: kontrol ≥40px, tombol ≥44px.
  Desktop sengaja tidak ikut berubah, dan itu juga diuji.
- **`.btn-sm` didefinisikan dua kali di `style.css`** dan yang kedua menang di
  semua lebar. Sunting yang kedua; yang pertama tidak berpengaruh apa-apa.
- **Validasi ada di fungsi simpan, bukan hanya di atribut HTML.**
  `checkWorkLogHours`, `checkUnitFields`, `checkStockBalance`. Polanya: yang
  pasti salah **ditolak**, yang mungkin benar **dikonfirmasi**. Shift malam 23
  jam itu mencurigakan tetapi sah, jadi ia ditanya — bukan dilarang.
- **Halaman Leader → Periksa Data** mencari data yang sudah terlanjur kotor:
  SN kembar, karakter tak terlihat, ejaan PT yang beda tipis, jam laporan tidak
  wajar, tanggal masa depan, lisensi yang mulai dan habis di hari yang sama
  (tanda tangan migrasi Excel yang dihapus di v100), kerusakan tanpa unit, stok
  minus. Tiap pemeriksaan satu fungsi murni `dc*()` — tambah yang baru dengan
  mendaftarkannya di `DATA_CHECKS`. **Hanya perapian spasi yang boleh otomatis**;
  itu satu-satunya perbaikan yang tidak bisa mengubah arti sebuah nilai.
- **Jangan mencetak blok `firestore.rules` ke dalam UI.** Lima spanduk dulu
  melakukannya dengan aturan yang sudah usang, dan menyuruh owner
  menempelkannya — yang akan memutus semua akun non-owner. Pakai
  `renderRulesBanner()`: ia menunjuk ke berkas di repo, menyesuaikan pesannya
  untuk owner vs bukan, dan dibersihkan `clearRulesBanner()` begitu datanya
  mengalir lagi.
- **Data yang disalin** (nama anggota, nama unit, perusahaan) selalu punya
  jalur baca yang mengutamakan data hidup dan jatuh ke salinan hanya kalau
  aslinya sudah terhapus.
