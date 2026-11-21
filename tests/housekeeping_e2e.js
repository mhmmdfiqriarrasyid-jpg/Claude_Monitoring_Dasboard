// 2.19.0 — functions/housekeeping.js against the Firestore + Storage emulators
// (firebase-admin from functions/node_modules). Not part of npm test:
//   npx firebase emulators:exec --only firestore,storage --project demo-ot "node tests/housekeeping_e2e.js"
process.env.FIRESTORE_EMULATOR_HOST = process.env.FIRESTORE_EMULATOR_HOST || '127.0.0.1:8181';
process.env.FIREBASE_STORAGE_EMULATOR_HOST = process.env.FIREBASE_STORAGE_EMULATOR_HOST || '127.0.0.1:9199';
const path = require('path');
const admin = require(path.join(__dirname, '..', 'functions', 'node_modules', 'firebase-admin'));
const { housekeepingRun } = require('../functions/housekeeping');
admin.initializeApp({ projectId: 'demo-ot', storageBucket: 'demo-ot.appspot.com' });
const db = admin.firestore(), bucket = admin.storage().bucket();
const T = []; const t = (n, g, w) => T.push({ n, g, w, pass: JSON.stringify(g) === JSON.stringify(w) });
(async () => {
    const now = new Date(2026, 10, 3, 3, 0);
    const day = 86400000;
    const old1 = new Date(2024, 0, 10).getTime(), old2 = new Date(2024, 0, 20).getTime(), old3 = new Date(2024, 1, 5).getTime();
    const recent = now.getTime() - 100 * day;
    await Promise.all([
        db.doc('history/h1').set({ id: 'h1', timestamp: old1, action: 'update' }),
        db.doc('history/h2').set({ id: 'h2', timestamp: old2, action: 'create' }),
        db.doc('history/h3').set({ id: 'h3', timestamp: old3, action: 'delete' }),
        db.doc('history/h4').set({ id: 'h4', timestamp: recent, action: 'update' }),
        db.doc('clientErrors/e1').set({ message: 'old', at: now.getTime() - 40 * day }),
        db.doc('clientErrors/e2').set({ message: 'new', at: now.getTime() - 2 * day })
    ]);
    // a month file that already exists keeps its rows
    await bucket.file('archives/history/2024-01.json').save(JSON.stringify([{ id: 'h0', timestamp: new Date(2024, 0, 2).getTime() }]), { contentType: 'application/json' });
    const r = await housekeepingRun(db, bucket, now);
    t('ringkasan', r.reason, 'errors -1, history archived 3');
    const left = (await db.collection('history').get()).docs.map(d => d.id).sort();
    t('history baru tetap di Firestore, lama dipindah', left, ['h4']);
    const jan = JSON.parse((await bucket.file('archives/history/2024-01.json').download())[0].toString());
    t('file Januari: isi lama + baru, urut waktu', jan.map(x => x.id), ['h0', 'h1', 'h2']);
    const feb = JSON.parse((await bucket.file('archives/history/2024-02.json').download())[0].toString());
    t('file Februari', feb.map(x => x.id), ['h3']);
    t('error log > 30 hari dihapus', (await db.collection('clientErrors').get()).docs.map(d => d.id), ['e2']);
    const again = await housekeepingRun(db, bucket, now);
    t('dijalankan lagi: tidak ada yang dobel', [again.reason, JSON.parse((await bucket.file('archives/history/2024-01.json').download())[0].toString()).length], ['errors -0, history archived 0', 3]);
    let pass = 0;
    T.forEach(x => { if (x.pass) pass++; console.log(`${x.pass ? 'PASS' : 'FAIL'}  ${x.n}${x.pass ? '' : `\n        dapat=${JSON.stringify(x.g)} harap=${JSON.stringify(x.w)}`}`); });
    console.log(`\n${pass}/${T.length} lulus`);
    process.exit(pass === T.length ? 0 : 1);
})().catch(e => { console.error(e); process.exit(1); });
