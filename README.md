# Operation Technology Dashboard

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
`teamMembers`, `teamLogApprove`, `warehouse`, `inspection`,
`inspectionApprove`, `history`, plus `csv` (`none` / `export` / `full`).

`teamLogApprove` sengaja terpisah dari `teamLog`: seorang pemeriksa bisa
menyetujui laporan tanpa bisa mengubah isinya — dan pemisahan itu ditegakkan
oleh Firestore rules, bukan hanya oleh tampilan.

`inspection` (teknisi: mengisi laporan cek alat berat, membuat jadwal) dan
`inspectionApprove` (atasan: menyetujui / minta revisi) dipisah dengan alasan
yang sama.

**Satu akun hanya boleh aktif di satu perangkat.** Login terbaru menang;
perangkat lain keluar sendiri.

---

## Menjalankan uji

```bash
npm install            # sekali saja
npm test               # menyalakan server sendiri, lalu menjalankan semua suite
```

1.552 pemeriksaan di tiga puluh delapan suite, menggerakkan Chromium sungguhan terhadap
aplikasi yang disajikan. Kalau mesin Anda sudah punya Chromium dan tidak ingin
Playwright mengunduh miliknya:

```bash
CHROME_PATH=/jalur/ke/chrome npm test
```

Suite-nya: `team_logic`, `roles_test`, `company_test`, `adjust_test`,
`session_test`, `warehouse_test`, `approval_test`, `leader_test`, `recap_test`,
`photos_test`, `migration_test`, `history_scan_test`, `validation_test`,
`datacheck_test`, `damagephotos_test`, `mobile_test`, `window_test`,
`backup_test`, `keyboard_test`, `leave_test`, `unitgroups_test`,
`hardening_test`, `followup_test`, `licsync_test`, `xss_test`, `auditfix_test`, `regress`.

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
  Semuanya sudah dihapus — begitu juga `applyDefaultLicensesIfNeeded()`, yang
  dulu jadi contoh di sini: bentuknya sama (penanda per-browser) dan begitu ada
  alat berat ia akan menempelkan lisensi SF-RTK ke excavator. Kalau memang perlu
  migrasi: `isOwner()`, **hanya mengisi yang kosong**, dan — ini yang dulu
  terlewat — **hanya untuk field milik kelompok unit itu**
  (`fieldAllowedForGroup(field, unitGroupOf(u))`). Untuk alat berat, kolom
  lisensi yang kosong berarti *tidak berlaku*, bukan *belum diisi*.
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
- **Cache offline menentukan mahal-murahnya membuka aplikasi.** Dengan cache
  hangat, Firestore melanjutkan tiap langganan dari posisi terakhir dan hanya
  menarik yang berubah. `firebase-init.js` memakai `persistentLocalCache` +
  `persistentMultipleTabManager`; API lama `enableIndexedDbPersistence`
  **menyerah total begitu ada tab kedua**, jadi admin yang membuka dua tab dulu
  tidak punya cache sama sekali. Cek `window.cloud.cacheMode` di console —
  `persistent-multitab` berarti sehat, `memory` berarti tiap reload menarik
  ulang semuanya.
- **Langganan `shifts` dibatasi jendela tanggal** (`SHIFT_WINDOW_DAYS`,
  ±4 bulan), karena koleksi itu bertambah satu dokumen per orang per hari
  selamanya. Mundur melewati jendela **melebarkan** langganannya lewat
  `ensureShiftWindowCovers()` — jangan pernah menggantinya dengan sekadar
  menyembunyikan data. Dijaga `tests/window_test.js`.
  `workLogs` sengaja **tidak** dibatasi: enam tempat membacanya di luar jendela
  mana pun (Kotak Keputusan untuk laporan tertunda umur berapa pun, profil
  unit, Periksa Data, rekap bulan lama, ekspor CSV, daftar paddock), jadi
  penghematannya kecil sementara peluang data hilang diam-diam besar.
- **Ukuran untuk ponsel ada di satu blok di ujung `style.css`**
  (`@media (max-width: 700px)`), dan dijaga `tests/mobile_test.js`. Aturan yang
  tidak boleh dilanggar: **setiap field form minimal `16px`.** Safari iPhone
  nge-zoom halaman tiap kali field di bawah 16px difokuskan dan tidak pernah
  kembali — satu field 13px membuat seluruh aplikasi membesar sampai pengguna
  reload. Sisanya soal ukuran sentuh: kontrol ≥40px, tombol ≥44px.
  Desktop sengaja tidak ikut berubah, dan itu juga diuji.
- **Modal yang isinya dibungkus `<form>` langsung di bawah `.modal-card`**
  bergantung pada `.modal-card > form { display:flex; … min-height:0 }`.
  Tanpa itu form tidak bisa menyusut, kartu (`overflow:hidden`) memotongnya,
  dan di ponsel tidak ada yang bisa di-scroll — tombol Simpan tak terjangkau.
  `mobile_test` membuka setiap modal di 360×600 dan memeriksanya.
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
- **Cadangan itu satu tabel, bukan daftar yang ditulis dua kali.** `BACKUP_PARTS`
  (`script.js`) menggerakkan ekspor **dan** restore sekaligus. Koleksi baru =
  satu baris di sana. Jangan pernah menambah koleksi ke `exportBackup` saja:
  selama tiga versi berkasnya berisi empat koleksi dari lima belas sementara
  tombolnya berjudul *"full JSON backup"*, dan seluruh modul Tim dan Gudang
  tidak punya cadangan sama sekali.
  - `fullRead` ada untuk koleksi yang salinan di memorinya **sengaja tidak
    lengkap**. `shifts` hanya memuat jendela 120 hari, jadi mencadangkannya
    dari `teamShifts` menghasilkan berkas yang terlihat utuh dan tidak — dan
    restore mode GANTI yang dihitung dari larik itu akan menghapus setiap shift
    di luar jendela.
  - `users` dan `history` **sengaja** tidak ikut. Lihat `BACKUP_EXCLUDED`;
    alasannya ikut ditampilkan di laporan restore supaya tidak jadi misteri.
  - Laporan restore wajib menyebut koleksi yang **tidak ada di berkas**. Itu
    baris yang paling penting: restore yang diam-diam mencakup lebih sedikit
    dari yang disangka orang adalah kegagalan yang modal itu ada untuk dicegah.
- **Alur pengiriman laporan harian & izin/sakit (dijaga `tests/workflow_test.js`).**
  Draf → Kirim → Menunggu → Disetujui, atau kembali sebagai Perlu Revisi.
  Hanya draf dan yang dikembalikan yang bisa diedit/dihapus; yang menunggu
  bisa *ditarik kembali* ke draf oleh pengirimnya; yang disetujui hanya bisa
  dibuka lagi oleh atasan lewat Minta Revisi (wajib catatan). Owner bebas.
  Draf tidak dihitung di KPI lapor hari ini, rekap, ringkasan mingguan,
  profil unit, maupun penanda izin di jadwal, dan tidak terlihat orang lain.
  Pengirim diberi tahu di aplikasi: badge Tim + badge tab, kotak "Perlu
  direvisi" di atas tabel, dan toast saat dibuka / saat ada revisi atau
  persetujuan baru (penanda per akun di localStorage
  `teamApprovalSeen:<uid>:<jenis>`). Foto/surat ditulis SEBELUM laporannya,
  karena rules hanya mengizinkan foto berubah selama laporannya terbuka.
- **Bahasa antarmuka.** Seluruh teks yang dilihat pengguna berbahasa Inggris:
  menu, judul, kolom dan `data-label`, tombol, form, label status, toast,
  confirm/prompt, paragraf penjelasan, header CSV ekspor, label riwayat baru
  (`[Damage]`, `[Inspection]`, `FAILED (code) — change not saved`) dan format
  tanggal (`en-GB`). **Nilai tersimpan tidak pernah diterjemahkan** — kunci
  seperti `izin`, `pagi`, `pending`, `Mekanis`, `breakdownReason` 'Diset via
  impor CSV' tetap; yang tampil lewat peta label (mis. `DAMAGE_TYPE_LABEL`).
  Impor CSV tetap menerima header lama berbahasa Indonesia. Riwayat lama
  tetap tampil apa adanya. Selector `data-label` di style.css harus ikut nama
  kolom yang sama.
- **Kualitas data (dijaga `tests/dataquality_test.js`).** Tiga alat:
  (1) **Clean Up Values** (`valueMergeModal`, tombol di kartu kelengkapan
  dan "Merge…" di Data Check): pilih ejaan yang maksudnya sama → semua
  record diubah ke satu ejaan lewat `updateUnitsBatch` (satu simpan lokal,
  satu batch cloud, baris riwayat per field seperti `updateUnit`), bisa
  Undo. Field: jenis/brand/teks penuh Implement, Work Tool, Machine Type,
  Model, Site, dan Company anggota tim (`saveTeamMembers`). (2) **Data
  Check** baru: ejaan mirip (`similarSpelling`: sama setelah huruf/tanda
  baca diabaikan atau 1–2 salah ketik; nama ≤4 huruf harus sama persis;
  angka harus sama), field kunci kosong per kelompok, implement yang tidak
  ada di daftar Implements. (3) **Data Completeness** di atas Unit Database:
  % field kunci terisi (`REQUIRED_UNIT_FIELDS`, dibulatkan ke bawah), chip
  per kekurangan memfilter tabel (`editMissingFilter`, direset saat pindah
  kelompok). Implement terbalik "Brand — Jenis" dinilai per pasangan dari
  ejaan LAIN (`_knownImplementBrands().pairs`).
- **Master Lists: Site, Brand, Company (dijaga `tests/masterlist_test.js`
  dan rules_test).** Koleksi `masterLists` (dokumen `site`, `brand`,
  `company`; `{id, values[]}` ≤ 500), dibuka dari Admin → Master Lists atau
  kartu Data Completeness. Hak ubah: Site = editUnits, Brand = implements
  atau editUnits, Company = teamMembers (sama di `firestore.rules`
  `canEditMasterList`). Daftar kosong = bebas; daftar berisi = satu-satunya
  pilihan (`checkMasterValue`, cocok tanpa peduli huruf/spasi/tanda baca,
  menyimpan ejaan daftar) di form unit, Bulk Edit, edit langsung, impor CSV
  unit (Site), form & impor Implements (Brand), anggota tim (Company) dan
  target Clean Up Values. Rename ikut mengubah record (unit, Implements +
  teks implement unit, anggota). "Add the values in use" mengisi daftar dari
  data, ejaan mirip dilipat ke yang terbanyak. Data Check "Not in a master
  list". Belum termasuk Backup JSON (daftarnya kecil dan bisa diisi ulang
  dari data).
- **Brand pada unit Heavy Equipment (dijaga `tests/heavybrand_test.js`).**
  Field `brand` (heavy saja — ada di `HEAVY_ONLY_FIELDS`, jadi firewall
  kelompok menolaknya pada traktor): kolom sebelum Model di Unit Database
  dan dashboard, form, Bulk Edit, profil, ekspor/impor CSV (`Brand`/`Merek`; header
  bersama, jadi kolom Brand di sheet traktor diabaikan tanpa peringatan),
  field kunci di kartu kelengkapan, Clean Up Values. Memakai daftar Brand
  yang sama dengan Implements di Master Lists (pemakaian & rename mencakup
  keduanya).
  Sejak v146 brand alat berat punya daftar sendiri: **Heavy Equipment
  Brands** (`masterLists/heavyBrand`, hak ubah editUnits), terpisah dari
  **Implement Brands** (`masterLists/brand`) untuk Agricultural.
- **Implement wajib dari daftar Implements (dijaga `tests/implpick_test.js`).**
  Begitu daftar Implements berisi, `checkImplementValue` menjaga form unit
  (lewat `checkUnitFields`, dengan hint di bawah kolom), Bulk Edit, edit
  langsung di tabel, impor CSV (unit baru → kosong, update → tetap; dicatat
  di laporan impor) dan target Clean Up Values. Nilai dikenali lewat label,
  nama profil, atau jenis+brand dalam huruf/urutan/pemisah apa pun
  (`findImplementRecord`), lalu disimpan dengan ejaan daftar
  (`implementOptionLabel`). Nilai lama di luar daftar tidak dipaksa hilang —
  boleh dipertahankan, tidak boleh diganti ke nilai asing lain. Data Check
  membedakan "tidak dikenal" (Merge…) dan "beda ejaan" (tombol Match
  implements to the Implements list → `matchImplementsToList`). Daftar
  kosong = bebas seperti dulu.
- **Isi jadwal shift cepat (dijaga `tests/shiftfill_test.js`).** Tiap
  anggota punya pilihan "Fill week…" (Morning/Afternoon/Night Senin–Sabtu +
  Minggu Off, Morning/Night setiap hari, Same as last week, Clear this
  week), dan toolbar punya **Copy Last Week** untuk seluruh tim (hanya sel
  yang minggu lalu terisi; konfirmasi menyebut berapa sel yang ditimpa).
  Semuanya lewat `applyShiftChanges` → `saveShiftChanges` (satu batch, satu
  baris riwayat), dengan rollback bila ditolak server. Rules shifts tidak
  berubah.
- **Laporan cek tanpa sinyal (dijaga `tests/offline_test.js`).** Firestore
  menyimpan tulisan offline di antrean perangkat (cache persisten, bertahan
  saat aplikasi ditutup) dan mengirimnya sendiri. `subscribeInspections`
  memakai `includeMetadataChanges` dan memberi daftar id yang
  `hasPendingWrites` → `insQueuedIds`: baris laporan bertanda "Waiting to
  send", kotak `#insSyncNotice` menyebut jumlahnya, form memberi tahu saat
  offline, toast saat antrean habis, dan Sign out meminta konfirmasi bila
  masih ada antrean (Firestore menahannya sampai akun yang sama masuk lagi).
- **Ringkasan unit per Implement / Alat Kerja (dijaga `tests/toolsummary_test.js`).**
  Kartu di atas tabel Unit Database: tab Agricultural menghitung per
  `implement`, tab Heavy per `workTool`. Ikut filter status & site (bukan
  kotak cari); ejaan yang hanya beda huruf besar/spasi digabung
  (`toolKeyOf`); "Tanpa …" selalu tampil. Agricultural dihitung per JENIS
  implement dengan rincian brand di bawahnya (`toolPartsOf`: record
  Implements bila cocok, jika tidak dipecah dari "Jenis — Brand"; urutan
  terbalik "Brand — Jenis" dikenali dari skor `_knownImplementBrands`). Klik
  jenis = semua brand-nya, klik brand = hanya brand itu (`TOOL_SEP`). Klik baris = filter tabel
  (`editToolFilter`, ikut ke Export CSV, direset saat pindah tab).
- **Menu samping per kelompok alat (dijaga `tests/navgroups_test.js`).**
  Ringkasan · Agricultural Equipment · Heavy Equipment · Tim · Inventaris ·
  Admin. Dashboard, Unit Database dan Kerusakan tetap SATU halaman; kelompok
  dipilih HANYA dari menu samping (tab kelompok di Dashboard dan Unit
  Database sudah dihapus — judul tabel menyebut kelompoknya); tautannya (`data-group`, `goNav`) membuka halaman itu
  pada kelompoknya dan `markActiveNav` menyalakan tautan kelompok yang sedang
  tampil. Kerusakan punya filter kelompok (`damageGroup`; catatan mengikuti
  unit hidupnya, unit yang sudah dihapus memakai `unitGroup` di catatan).
  `updateNavGroups` menyembunyikan Dashboard per kelompok bagi akun yang
  tidak bekerja dengan kelompok itu (`NAV_GROUP_AREAS`), menyembunyikan
  Dashboard Heavy/Semua bila armada hanya satu kelompok, lalu menyembunyikan
  judul grup yang semua tautannya tersembunyi.
- **Pengecekan Alat Berat (dijaga `tests/inspection_test.js`).** Menu
  sendiri dengan tiga tab: Status Unit, Jadwal, Laporan Cek. Laporan cek
  berisi 4 komponen alat berat (Camera AI, Telematic Box, Switch Limiter,
  Rotary Lamp), masing-masing Baik/Rusak + **1 foto wajib** + catatan (wajib
  kalau Rusak), lewat alur Draf → Kirim → Disetujui / Perlu Revisi yang sama
  dengan laporan harian (`TEAM_AREAS` / `INS_AREAS` memilih area aksesnya).
  **Cek berikutnya = tanggal cek AKTUAL laporan terkirim terakhir + 14 hari**
  (`INSPECTION_INTERVAL_DAYS`); draf tidak dihitung, laporan yang dikembalikan
  tetap dihitung (pengecekannya sudah terjadi). Jadwal (`inspectionPlans`)
  hanya daftar unit per tanggal; cek di luar jadwal tetap sah dan ikut
  mereset 14 hari. Komponen Rusak masuk ke Kerusakan **setelah disetujui**,
  oleh akun yang punya akses edit Kerusakan (`applyInspectionFindings`,
  stempel `appliedAt` supaya hanya sekali); tanpa akses itu muncul di Kotak
  Keputusan. Foto di `inspectionPhotos/{id}` (tidak di-subscribe), ditulis
  SEBELUM laporannya.
- **`firestore.rules` diuji di emulator: `tests/rules_test.js`** (105 skenario;
  bukan bagian `npm test` karena butuh Java + firebase-tools — perintahnya di
  kepala berkas). Jalankan SEBELUM mem-publish rules. Yang dijaga rules:
  alur persetujuan laporan/izin (transisi status di atas, kunci isi dan foto
  selama menunggu/disetujui, tarik kembali hanya oleh pengirim, approver
  hanya menyentuh field persetujuan dengan emailnya sendiri dan tidak untuk
  catatannya sendiri), baca per area untuk laporan, izin, surat
  sakit, foto kerusakan dan riwayat, riwayat terikat ke uid+email pelaku,
  bentuk dokumen pendaftar dan `activeSession`, `id` = ID dokumen, dan nilai
  shift yang dikenal. Langganan di `initCloudSync` mengikuti aturan baca yang
  sama — ubah keduanya bersamaan.
- **Aturan anti-XSS (dijaga `tests/xss_test.js`).** Nilai yang ditulis satu
  pengguna tampil di layar pengguna lain, termasuk Super Admin:
  - `showToast` menampilkan TEKS. Kirim nilai mentah; jangan `escapeHtml`.
  - Argumen di atribut `onclick`/`onchange` selalu lewat `jsArg(v)`, bukan
    `'${escapeHtml(v)}'` — browser mengembalikan `&#39;` menjadi `'` sebelum
    handler berjalan.
  - Nilai yang menjadi nama kelas harus dari daftar yang dikenal (lihat
    `shiftFor`); `src` foto lewat `safeImageSrc`.
  - `id` setiap record diambil dari ID dokumen Firestore (`withDocId` di
    `firebase-init.js`), bukan dari field `id` yang bisa diisi siapa saja.
- **Jangan memanggil `window.cloud.X()` telanjang.** Service worker menyajikan
  `firebase-init.js` network-first tapi jatuh ke cache saat fetch gagal —
  yaitu saat sinyal buruk, yaitu saat orang menyimpan. Pakai `cloudFn('X')`
  (mengembalikan `null` + toast) atau, di posisi argumen `cloudWrite(...)`,
  `cloudCall('X', ...)` yang menolak dengan `code: 'stale-client'` alih-alih
  melempar `TypeError` keluar dari handler klik.
- **Sel tabel tanpa `data-label` itu `display:none` di ponsel.** Di tabel
  `table--cardable`, `style.css` menyembunyikan setiap `<td>` yang tidak
  membawanya. Kolom yang lupa dilabeli tidak terlihat rusak — ia hanya **hilang**
  di perangkat yang justru dipakai tim lapangan. Dua sel di Edit Units sengaja
  tanpa label (nomor baris dan kotak centang); sisanya wajib. Ada ujinya di
  `mobile_test`.
- **Modal baru wajib didaftarkan di `MODAL_CLOSERS`.** Escape menutup modal
  paling atas lewat peta itu; `keyboard_test` gagal kalau ada modal yang lupa.
  Header tabel yang bisa diurut butuh `tabindex="0" role="button"` — penangan
  Enter/Spasi-nya sudah terdelegasi, jadi tidak perlu `onkeydown` per header.
- **Riwayat audit dilanggan malas dan digerbang.** `startHistorySubscription()`
  baru jalan saat History dibuka, dan hanya untuk `hasAccess('history','view')`.
  Dulu ia jalan untuk semua orang saat login: 500 dokumen berisi nama dan email
  pelaku, termasuk ke akun yang menu History-nya memang disembunyikan.
- **Id dokumen shift itu deterministik** (`tanggal_anggota`), jadi dua orang
  bisa saling menimpa. Tidak ada merge — yang ada adalah `updatedBy` di
  dokumennya dan `_shiftPendingWrites`, supaya yang kalah **diberi tahu**
  alih-alih melihat gridnya berubah sendiri.
- **Cadangan otomatis (`BACKUP_RING_KEY`) sekarang ada yang membacanya.**
  Isinya **hanya unit** dan **hanya perangkat ini** — bukan pengganti tombol
  Backup. Pemulihannya lewat jalur yang sama dengan restore berkas, termasuk
  `cloudPushUnits`, karena rollback lokal saja akan dibatalkan snapshot
  berikutnya.
- **Izin / Sakit menumpang area `teamLog`, bukan punya areanya sendiri.**
  Yang boleh mengisi laporan harian boleh mengisi izin; yang boleh menyetujui
  laporan boleh menyetujui izin. Area baru akan menyentuh delapan tempat
  (`ACCESS_AREAS`, `VIEW_AREAS`, `RO_FLAGS`, CSS, `canEditAnything()` di rules,
  komentar header rules, `roles_test`) **dan** mengharuskan owner memberi izin
  itu satu per satu ke tiap akun sebelum fiturnya bisa dipakai siapa pun.
- **Surat sakit / surat izin disimpan di koleksi `workLogPhotos`.** Namanya
  historis, bukan pembatas isinya: bentuk dokumennya sama persis dan rules-nya
  sudah `canEditArea('teamLog')`. Pakai alias `getTeamDocs` / `saveTeamDocs` /
  `deleteTeamDocs` di `firebase-init.js`, jangan panggil nama aslinya. Id tidak
  mungkin bentrok — laporan `wl_`, izin `lv_`.
- **Jenis izin disimpan sebagai KUNCI** (`'izin'`, `'sakit'`, `'alpa'`), dengan
  label terpisah di `LEAVE_TYPES`. Menambah jenis keempat = satu baris di sana.
  Jangan sekali-kali menyamakan nilai simpan dengan teks yang tampil.
- **Rentang tanggal izin inklusif dua ujungnya.** `leaveDays('5','5') === 1`.
  Dan tanggalnya **boleh di masa depan** — izin memang direncanakan, jadi form
  ini sengaja TIDAK memakai `data-nofuture` seperti form lain.
- **Hanya izin yang DISETUJUI menandai jadwal shift**, dan penandanya murni
  tampilan: `renderShiftGrid` tidak pernah menulis ke koleksi `shifts`.
  Menulis ke sana akan menimpa jadwal yang sudah diisi, dan koleksi itu
  berjendela 120 hari sehingga izin yang lebih lama tidak punya sel untuk
  ditulisi sama sekali. Dua hitungan ikut menyesuaikan: `onDuty` di ringkasan
  mingguan, dan penyebut "Lapor Hari Ini".
- **`leaveRequests` dilanggan utuh tanpa jendela, dan itu disengaja.**
  Pengajuan yang menunggu persetujuan bisa berumur berapa pun, jadi jendela
  tanggal akan menyembunyikan persis baris yang Kotak Keputusan ada untuk
  memunculkannya. Volumenya ±200 dokumen setahun. **Tinjau ulang di ±2.000
  dokumen** dan jendelakan seperti `subscribeShifts`.
- **Foto dan surat lama dimuat SESUDAH form terbuka**, dan menyimpan set yang
  diubah MENGGANTI set yang tersimpan. Karena itu tombol Tambah Foto / Tambah
  Surat menunggu sampai yang lama tiba, dan tetap mati kalau gagal tiba
  (`setPhotoAddState`). Hasil muat dan kompresi yang terlambat dibuang lewat
  penghitung generasi (`_lvDocsGen`, `_wlPhotosGen`) — tanpa itu, surat
  pengajuan A bisa mendarat di form pengajuan B.
- **Foto kerusakan, foto laporan harian, dan surat izin ikut cadangan kalau
  diminta** (`BACKUP_PHOTO_KINDS`). Ketiganya satu dokumen per catatan dan harus
  diunduh satu per satu, jadi Export menanyakannya dulu; kalau ditolak, berkas
  dan toast-nya mencatatnya di `omitted`. Restore menulisnya kembali hanya untuk
  catatan yang ikut dipulihkan, dan restore GANTI yang menghapus catatan ikut
  menghapus dokumen fotonya (`_dropPhotoDoc`) — dokumen itu tidak pernah
  didaftar, jadi yang tertinggal tidak akan pernah ditemukan lagi.
- **Distribusi lisensi → lisensi unit berbasis selisih.** `licenseSyncPlan()`
  mengambil distribusi TERBARU per unit + jenis (GPS / Display), unitnya
  dicari lewat id lalu SN (`liveUnitFor`), dan menandai tiap baris `sync`
  (sudah sesuai — tidak pernah ditulis), `update`, atau `newer` (unit berlaku
  lebih lama dari distribusi terbarunya — diperpanjang manual, atau
  distribusinya dihapus). "Sync ke Unit" menampilkan pratinjau dan hanya
  menulis baris yang dicentang; `newer` tidak dicentang. Distribusi premium
  yang sudah habis ditulis langsung dalam keadaan akhir turun-otomatisnya
  (`distributionTarget`), jadi unit yang sudah turun ke SF-1 terbaca sesuai
  dan tidak dipantulkan kembali ke SF-RTK. Distribusi tanpa tanggal dilewati —
  targetnya akan jadi "hari ini + 1 tahun", berubah setiap hari.
  - **Yang masuk langsung tersinkron.** Form Distribusi dan Import CSV
    menerapkan distribusinya ke unit saat itu juga, dengan aturan yang sama
    (`update` ditulis, `newer` tidak). Impor hanya menulis unit yang
    diputuskan oleh baris impornya — baris lama yang sudah tidak sinkron
    sebelumnya dibiarkan untuk pratinjau Sync. Membetulkan tanggal distribusi
    yang MENENTUKAN lisensi unit boleh memundurkannya (`force`); distribusi
    lain tidak. Tidak ada sinkron otomatis di latar belakang: itu akan jalan
    di setiap perangkat, termasuk yang datanya basi.
  - **Tanggal distribusi harus YYYY-MM-DD** (`isoDistributionDate`); yang lain
    ditandai "Tanggal tidak valid", tidak ditebak. Impor CSV membaca
    D/M/YYYY hari-dulu (`csvLicenseDate`) dan menyamakan ejaan jenis
    (`canonicalLicenseType`). Jenis ditulis ke unit dengan ejaan kanonik dan
    dibandingkan persis, jadi unit berisi 'sf-rtk' ikut dibetulkan.
  - **Pratinjau Sync mengingat apa yang ditampilkannya.** Baris yang status
    atau isinya berubah sebelum Terapkan (unit diperpanjang di perangkat lain)
    dilewati dan dilaporkan, tidak ditulis.
  - **Distribusi yang menentukan lisensi unit lalu dihapus, dipindah ke unit
    lain, diganti jenisnya, atau diubah jadi stok masuk** (`_drivenBy`,
    `_releaseDistributions`): unitnya ditawari kembali ke distribusi
    sebelumnya, atau dikosongkan kalau tidak ada lagi. Satu konfirmasi per
    tindakan; Batal membiarkannya dan mengatakannya.
  - **Tombol Sync ke Unit hanya muncul kalau ada yang "Belum"** — alat
    perbaikan, bukan langkah rutin. Baris "Unit lebih baru" tidak
    memunculkannya: itu keadaan yang disengaja (perpanjangan manual).
  - **Kolom Status Unit** di Riwayat Stok Lisensi: Tersinkron, Belum, Unit
    lebih baru, Digantikan (distribusi lama yang sudah diganti yang lebih
    baru), Unit tidak ada, Bukan Pertanian, Tanggal tidak valid
    (`licenseSyncPlan().recStatus`).
- **Unit terbagi dua kelompok: Agricultural Equipment dan Heavy Equipment.**
  Satu koleksi `units`, satu kolom `unitGroup` (`'tractor'` | `'heavy'`).
  Registrinya `UNIT_GROUPS` (`script.js`): label, komponen yang dipantau, dan
  kolom milik tiap kelompok. Aturannya:
  - **190 dokumen lama tidak pernah ditulis ulang.** `unitGroup` yang kosong
    dibaca `'tractor'` (`unitGroupOf`). Jangan pernah menormalkannya di objek
    `globalData` — `updateUnit` mengirim seluruh objek, jadi normalisasi di
    tempat ikut tertulis oleh suntingan berikutnya.
  - **Hanya penciptaan yang menulis `unitGroup`**, dan kelompok yang terbaca
    **tidak bisa diubah** sesudahnya. Unit yang salah kelompok dihapus lalu
    ditambah ulang. Restore GANTI yang akan mengubah kelompok unit hidup ditolak.
    Satu-satunya pengecualian: kelompok yang **tidak terbaca** (lihat di bawah).
  - **Firewall di `updateUnit` dan `addUnits`** membuang `undefined`, membuang
    `unitGroup`, dan membuang kolom milik kelompok lain. Satu-satunya
    pengecualian: Periksa Data mengosongkan kolom silang (`clearStray`).
  - **Kelompok yang tidak terbaca** (`hasUnreadableGroup`: terisi tapi bukan
    nilai yang dikenal, mis. `"Heavy Equip"` dari cadangan yang disunting
    tangan, atau kelompok dari versi yang lebih baru) dibaca sebagai traktor,
    tapi **tidak ada yang boleh menulis kolom milik kelompok mana pun** ke unit
    itu. Form Edit dan edit sebaris menolak, impor CSV menandainya gagal, dan
    restore GANTI dari berkas yang membawanya ditolak sebelum menulis apa
    pun. Cadangan otomatis tidak ditolak: isinya salinan data perangkat ini
    sendiri, jadi menolaknya membuat rollback mustahil selama unit seperti itu
    ada — dan unit yang kembali dipagari persis seperti yang hidup. Kerusakan
    "set Breakdown" pada komponen unit seperti itu juga ditolak. Periksa Data tidak melaporkannya sebagai "field kelompok
    lain" — kolom mana yang silang bergantung pada kelompok yang belum
    diketahui, dan membersihkannya akan menghapus data asli. Jalan keluarnya
    tombol **Tetapkan kelompok** (`setUnreadableGroups`), satu-satunya penulis
    `unitGroup` sesudah penciptaan (`updateUnit(…, { setGroup: true })`), dan
    hanya untuk kelompok yang tidak terbaca.
  - **CSV yang kelompoknya disimpulkan dari kolomnya** sama mengikatnya dengan
    kolom `Unit Group`: berkas berkolom Camera AI tidak memperbarui traktor
    yang kebetulan ber-SN sama, dan sebaliknya (`csvInferredGroup`).
  - **Apa pun yang dulu meng-hardcode Display/GPS/Steering/JDLink sekarang
    bertanya ke kelompok unitnya** (`detectIssues`, `countIssues`, ring
    komponen, tabel Repair, filter). Komponen alat berat yang kosong dihitung
    bermasalah, sama seperti traktor.
  - **Kerusakan**: `damageTargetField(type, component, unit)` punya keadaan
    ketiga, `DAMAGE_DRIVES_NOTHING`, untuk catatan lama yang menyebut komponen
    John Deere pada alat berat — tidak ditulis ke unit, tidak dijadikan status.
  - **Lisensi SF/G5 hanya untuk kelompok pertanian.** Batas tulisnya
    `applyDistributedLicenseToUnit`.
  - **CSV**: kolom kelompok **hanya** `Unit Group`. "Kelompok" dan "Group" nama
    kolom yang wajar di CSV traktor untuk hal lain. Tanpa kolom itu, kelompok
    disimpulkan hanya dari empat header komponen alat berat vs header JD.
  - **Label "Alat Kerja", bukan "Attachment"** — unit sudah punya kolom berkas
    "Attachments", dan keduanya bersebelahan.
  - **Tanpa satu pun alat berat, tampilan traktor identik byte demi byte** dengan
    sebelum perubahan ini. Itu diuji terhadap `tests/fixtures/tractor_golden.json`,
    yang direkam dari kode lama. Jangan merekam ulang golden kecuali memang
    sengaja mengubah tampilan traktor.
  - Preferensi tab/cakupan disimpan per perangkat, tapi selama belum ada alat
    berat, keduanya selalu jatuh ke Pertanian.
