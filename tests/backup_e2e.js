// 2.35.0 — functions/backup.js and functions/thumbs.js against the Firestore +
// Storage emulators (firebase-admin and sharp from functions/node_modules).
// Not part of npm test:
//   npx firebase emulators:exec --only firestore,storage --project demo-ot "node tests/backup_e2e.js"
process.env.FIRESTORE_EMULATOR_HOST = process.env.FIRESTORE_EMULATOR_HOST || '127.0.0.1:8181';
process.env.FIREBASE_STORAGE_EMULATOR_HOST = process.env.FIREBASE_STORAGE_EMULATOR_HOST || '127.0.0.1:9199';
const path = require('path'), zlib = require('zlib'), fs = require('fs');
const admin = require(path.join(__dirname, '..', 'functions', 'node_modules', 'firebase-admin'));
const sharp = require(path.join(__dirname, '..', 'functions', 'node_modules', 'sharp'));
const { backupRun, backupList, backupManifest, restoreMissing, expiredDates } = require('../functions/backup');
const { thumbsBackfill } = require('../functions/thumbs');
admin.initializeApp({ projectId: 'demo-ot', storageBucket: 'demo-ot.appspot.com' });
const db = admin.firestore(), bucket = admin.storage().bucket();
const T = []; const t = (n, g, w) => T.push({ n, g, w, pass: JSON.stringify(g) === JSON.stringify(w) });
(async () => {
    const now = new Date(2026, 10, 10, 2, 0);
    await Promise.all([
        db.doc('units/u1').set({ id: 'u1', name: 'GGTR101G', status: 'Good' }),
        db.doc('units/u2').set({ id: 'u2', name: 'GGTR102G', status: 'Breakdown' }),
        db.doc('tickets/t1').set({ id: 't1', title: 'GPS', events: [{ type: 'created', at: 1 }], createdAt: admin.firestore.Timestamp.fromMillis(1700000000000) }),
        db.doc('users/o').set({ role: 'owner', status: 'active' }),
        db.doc('pushTokens/x').set({ uid: 'o', token: 'secret' }),
        db.doc('mailState/daily').set({ date: '2026-11-09' })
    ]);
    // Old folders: one past 30 days, one a 1st of the month (kept), one over a year.
    for (const d of ['2026-09-20', '2026-09-01', '2025-10-01']) await bucket.file(`backups/${d}/manifest.json`).save('{}');
    const r = await backupRun(db, bucket, now);
    t('ringkasan: 3 koleksi, 4 dokumen', [r.date, r.collections, r.docs, r.removed], ['2026-11-10', 3, 4, 2]);
    const m = await backupManifest(bucket, '2026-11-10');
    t('manifest: token push & catatan server tidak ikut', m.collections.map(c => [c.name, c.docs]), [['tickets', 1], ['units', 2], ['users', 1]]);
    const list = await backupList(db, bucket);
    t('yang tersimpan: hari ini + tanggal 1 dalam setahun', [list.dates, list.last.docs], [['2026-11-10', '2026-09-01'], 4]);
    const raw = JSON.parse(zlib.gunzipSync((await bucket.file('backups/2026-11-10/tickets.json.gz').download())[0]).toString('utf8'));
    t('isi: Timestamp disimpan sebagai __ts', raw[0].data.createdAt, { __ts: 1700000000000 });
    // A wrong delete, and an edit made after the backup.
    await db.doc('units/u2').delete();
    await db.doc('units/u1').update({ status: 'Breakdown' });
    await db.doc('tickets/t1').delete();
    const ru = await restoreMissing(db, bucket, '2026-11-10', 'units', admin.firestore.Timestamp);
    t('restore: hanya yang hilang ditambah', ru, { added: 1, kept: 1, total: 2 });
    t('yang masih ada tidak ditimpa', [(await db.doc('units/u1').get()).data().status, (await db.doc('units/u2').get()).data().name], ['Breakdown', 'GGTR102G']);
    await restoreMissing(db, bucket, '2026-11-10', 'tickets', admin.firestore.Timestamp);
    const tk = (await db.doc('tickets/t1').get()).data();
    t('Timestamp kembali jadi Timestamp', [typeof tk.createdAt.toMillis, tk.createdAt.toMillis(), tk.events[0].type], ['function', 1700000000000, 'created']);
    let err = ''; try { await restoreMissing(db, bucket, '2026-11-10', 'users', admin.firestore.Timestamp); } catch (e) { err = e.message; }
    t('akun & akses tidak bisa direstore dari file', err, 'not-restorable');
    err = ''; try { await restoreMissing(db, bucket, '2026-11-10', '../x', admin.firestore.Timestamp); } catch (e) { err = e.message; }
    t('nama koleksi aneh ditolak', err, 'bad-request');
    t('aturan simpan', expiredDates(['2026-11-10', '2026-10-11', '2026-10-10', '2026-10-01', '2025-12-01', '2025-10-01'], '2026-11-10'), ['2026-10-10', '2025-10-01']);

    // ---- thumbnails ----
    const jpg = fs.readFileSync(path.join(__dirname, 'fixtures', 'wr', 'p1.jpg'));
    await bucket.file('photos/workLogPhotos/w1/abc').save(jpg, { contentType: 'image/jpeg' });
    await bucket.file('photos/workLogPhotos/w1/def').save(jpg, { contentType: 'image/jpeg' });
    await bucket.file('photos/workLogPhotos/w1/def_t').save(Buffer.from('x'), { contentType: 'image/jpeg' });
    await bucket.file('photos/ticketPhotos/t1/gone_t').save(Buffer.from('x'), { contentType: 'image/jpeg' });
    await bucket.file('photos/projectPhotos/p1/doc').save(Buffer.from('%PDF'), { contentType: 'application/pdf' });
    const th = await thumbsBackfill(bucket, sharp, {});
    t('thumbnail: 1 dibuat, yang sudah ada dilewati, yatim dihapus, bukan gambar dilewati', th, { made: 1, failed: 0, orphans: 1, left: 0 });
    const meta = await sharp((await bucket.file('photos/workLogPhotos/w1/abc_t').download())[0]).metadata();
    t('ukuran thumbnail ≤ 360 px, JPEG', [meta.width <= 360 && meta.height <= 360, meta.format], [true, 'jpeg']);
    t('jalan kedua: tidak ada yang dibuat lagi', (await thumbsBackfill(bucket, sharp, {})).made, 0);

    let pass = 0;
    T.forEach(x => { if (x.pass) pass++; console.log(`${x.pass ? 'PASS' : 'FAIL'}  ${x.n}${x.pass ? '' : `\n        dapat=${JSON.stringify(x.g)} harap=${JSON.stringify(x.w)}`}`); });
    console.log(`\n${pass}/${T.length} lulus`);
    process.exit(pass === T.length ? 0 : 1);
})().catch(e => { console.error(e); process.exit(1); });
