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

308 pemeriksaan di delapan suite, menggerakkan Chromium sungguhan terhadap
aplikasi yang disajikan. Kalau mesin Anda sudah punya Chromium dan tidak ingin
Playwright mengunduh miliknya:

```bash
CHROME_PATH=/jalur/ke/chrome npm test
```

Suite-nya: `team_logic`, `roles_test`, `company_test`, `adjust_test`,
`session_test`, `warehouse_test`, `approval_test`, `regress`.

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

- **Foto disimpan sebagai data URL di dalam dokumen Firestore**, bukan di
  Cloud Storage. Batas dokumen Firestore 1 MB, jadi foto dikecilkan dan
  dibatasi jumlahnya sebelum disimpan. Kalau data sudah banyak, ini yang
  pertama perlu dipindahkan ke Cloud Storage.
- **Tulisan cloud lewat `cloudWrite()`.** Jangan panggil `logEvent()` di dalam
  `.then()` sebuah tulisan Firestore: offline, promise-nya tidak pernah selesai
  dan jejak auditnya hilang.
- **Data yang disalin** (nama anggota, nama unit, perusahaan) selalu punya
  jalur baca yang mengutamakan data hidup dan jatuh ke salinan hanya kalau
  aslinya sudah terhapus.
