// firestore.rules diuji di Firestore Emulator — BUKAN bagian dari `npm test`
// (butuh Java dan firebase-tools). Jalankan sebelum mem-publish rules:
//
//   npm i --no-save --legacy-peer-deps firebase-tools@13 @firebase/rules-unit-testing@3 firebase@10
//   npx firebase emulators:exec --only firestore --project demo-ot "node tests/rules_test.js"
//
// Setiap serangan yang ditemukan audit harus DITOLAK, dan setiap tulisan yang
// dilakukan aplikasi sendiri harus tetap DITERIMA — rules yang terlalu ketat
// mengunci tim dari pekerjaannya sendiri.
const fs = require('fs');
const path = require('path');
const { initializeTestEnvironment, assertSucceeds, assertFails } = require('@firebase/rules-unit-testing');
const { doc, setDoc, getDoc, updateDoc, deleteDoc } = require('firebase/firestore');

const T = [];
async function check(name, p, expectOk) {
    try {
        if (expectOk) await assertSucceeds(p); else await assertFails(p);
        T.push({ name, pass: true });
    } catch (e) {
        T.push({ name, pass: false, err: String(e && e.message || e).slice(0, 160) });
    }
}

(async () => {
    const env = await initializeTestEnvironment({
        projectId: 'demo-ot',
        firestore: { rules: fs.readFileSync(path.join(__dirname, '..', 'firestore.rules'), 'utf8'),
                     host: '127.0.0.1', port: Number(process.env.FIRESTORE_PORT || 8181) }
    });

    // Seed users and records without rules.
    await env.withSecurityRulesDisabled(async ctx => {
        const db = ctx.firestore();
        const u = (uid, role, access) => setDoc(doc(db, 'users', uid), { uid, email: uid + '@x.id', role, status: 'active', access: access || {} });
        await u('owner', 'owner');
        await u('writer', 'staff', { teamLog: 'edit' });
        await u('boss', 'staff', { teamLogApprove: 'edit', leader: 'view' });
        await u('both', 'staff', { teamLog: 'edit', teamLogApprove: 'edit' });
        await u('shiftOnly', 'staff', { teamShift: 'view', editUnits: 'edit' });
        await u('nobody', 'khl', {});
        await setDoc(doc(db, 'workLogs', 'wl_1'), { id: 'wl_1', task: 'x', approval: 'pending', createdByUid: 'writer' });
        await setDoc(doc(db, 'workLogs', 'wl_ok'), { id: 'wl_ok', task: 'x', approval: 'approved', approvedByEmail: 'boss@x.id', createdByUid: 'writer' });
        await setDoc(doc(db, 'workLogs', 'wl_both'), { id: 'wl_both', task: 'x', approval: 'pending', createdByUid: 'both' });
        await setDoc(doc(db, 'leaveRequests', 'lv_1'), { id: 'lv_1', reason: 'demam', approval: 'pending', createdByUid: 'writer' });
        await setDoc(doc(db, 'workLogPhotos', 'lv_1'), { id: 'lv_1', photos: ['data:image/png;base64,AA'] });
        await setDoc(doc(db, 'history', 'h1'), { id: 'h1', timestamp: 1, action: 'x' });
        await setDoc(doc(db, 'units', 'u1'), { id: 'u1', name: 'T' });
    });
    const as = (uid, email) => env.authenticatedContext(uid, { email: (email || uid + '@x.id') }).firestore();
    const W = as('writer'), B = as('boss'), BOTH = as('both'), O = as('owner'), S = as('shiftOnly'), N = as('nobody');

    // ---- persetujuan: laporan harian ----
    await check('editor: buat laporan baru (pending, dirinya penulis)', setDoc(doc(W, 'workLogs', 'wl_new'), { id: 'wl_new', task: 'y', approval: 'pending', createdByUid: 'writer' }), true);
    await check('editor: buat laporan yang langsung "approved" DITOLAK', setDoc(doc(W, 'workLogs', 'wl_bad'), { id: 'wl_bad', task: 'y', approval: 'approved', createdByUid: 'writer' }), false);
    await check('editor: buat laporan atas nama penulis lain DITOLAK', setDoc(doc(W, 'workLogs', 'wl_bad2'), { id: 'wl_bad2', task: 'y', approval: 'pending', createdByUid: 'boss' }), false);
    await check('editor: menyetujui lewat console DITOLAK', setDoc(doc(W, 'workLogs', 'wl_1'), { approval: 'approved', approvedBy: 'Boss', approvedByEmail: 'boss@x.id', approvedAt: 1 }, { merge: true }), false);
    await check('editor: edit laporan mengembalikan ke pending (alur aplikasi)', setDoc(doc(W, 'workLogs', 'wl_ok'), { id: 'wl_ok', task: 'z', approval: 'pending', approvedBy: '', approvedByEmail: '', approvedAt: 0, revisionNote: '', createdByUid: 'writer' }), true);
    await env.withSecurityRulesDisabled(ctx => setDoc(doc(ctx.firestore(), 'workLogs', 'wl_ok'), { id: 'wl_ok', task: 'x', approval: 'approved', approvedByEmail: 'boss@x.id', createdByUid: 'writer' }));
    await check('editor: migrasi foto pada laporan disetujui (field persetujuan tak disentuh)', setDoc(doc(W, 'workLogs', 'wl_ok'), { photos: [], photoCount: 2 }, { merge: true }), true);
    await check('editor: mengganti penulis laporan DITOLAK', setDoc(doc(W, 'workLogs', 'wl_1'), { createdByUid: 'someone' }, { merge: true }), false);
    await check('approver: menyetujui laporan orang lain dengan emailnya sendiri', setDoc(doc(B, 'workLogs', 'wl_1'), { approval: 'approved', approvedBy: 'Boss', approvedByEmail: 'boss@x.id', approvedAt: 2, revisionNote: '', updatedAt: 2 }, { merge: true }), true);
    await check('approver: menyetujui atas nama orang lain DITOLAK', setDoc(doc(B, 'workLogs', 'wl_1'), { approval: 'approved', approvedByEmail: 'owner@x.id', approvedAt: 3 }, { merge: true }), false);
    await check('approver: mengubah isi laporan DITOLAK', setDoc(doc(B, 'workLogs', 'wl_1'), { task: 'diubah' }, { merge: true }), false);
    await check('approver: minta revisi', setDoc(doc(B, 'workLogs', 'wl_1'), { approval: 'revision', revisionNote: 'jam salah', approvedBy: '', approvedByEmail: '', approvedAt: 0, reviewedBy: 'Boss', reviewedAt: 4, updatedAt: 4 }, { merge: true }), true);
    await check('approver+editor: menyetujui laporannya SENDIRI DITOLAK', setDoc(doc(BOTH, 'workLogs', 'wl_both'), { approval: 'approved', approvedByEmail: 'both@x.id', approvedAt: 5 }, { merge: true }), false);
    await check('owner: tetap boleh apa saja', setDoc(doc(O, 'workLogs', 'wl_both'), { approval: 'approved', approvedByEmail: 'owner@x.id' }, { merge: true }), true);

    // ---- persetujuan: izin / sakit ----
    await check('izin: editor menyetujui lewat console DITOLAK', setDoc(doc(W, 'leaveRequests', 'lv_1'), { approval: 'approved', approvedByEmail: 'boss@x.id' }, { merge: true }), false);
    await check('izin: approver menyetujui', setDoc(doc(B, 'leaveRequests', 'lv_1'), { approval: 'approved', approvedBy: 'Boss', approvedByEmail: 'boss@x.id', approvedAt: 6, revisionNote: '', updatedAt: 6 }, { merge: true }), true);

    // ---- baca per area ----
    await check('surat sakit: tanpa akses laporan TIDAK bisa dibaca', getDoc(doc(N, 'workLogPhotos', 'lv_1')), false);
    await check('surat sakit: jadwal-saja TIDAK bisa dibaca', getDoc(doc(S, 'workLogPhotos', 'lv_1')), false);
    await check('surat sakit: pengisi laporan bisa membaca', getDoc(doc(W, 'workLogPhotos', 'lv_1')), true);
    await check('surat sakit: atasan penyetuju bisa membaca', getDoc(doc(B, 'workLogPhotos', 'lv_1')), true);
    await check('izin: tanpa akses TIDAK bisa dibaca', getDoc(doc(N, 'leaveRequests', 'lv_1')), false);
    await check('izin: penyusun jadwal bisa membaca (penanda di grid)', getDoc(doc(S, 'leaveRequests', 'lv_1')), true);
    await check('laporan: tanpa akses TIDAK bisa dibaca', getDoc(doc(N, 'workLogs', 'wl_1')), false);
    await check('riwayat: tanpa akses history TIDAK bisa dibaca', getDoc(doc(W, 'history', 'h1')), false);
    await check('riwayat: owner bisa membaca', getDoc(doc(O, 'history', 'h1')), true);
    await check('unit: tetap bisa dibaca semua user aktif', getDoc(doc(N, 'units', 'u1')), true);

    // ---- riwayat audit ----
    const ev = (x) => Object.assign({ id: 'e1', timestamp: Date.now(), action: 'update', unitId: '', unitName: 'x', field: '', before: '', after: '',
        actorUid: 'writer', actorEmail: 'writer@x.id', actorName: 'W', actorRole: 'staff' }, x || {});
    await check('riwayat: catatan sah dari aplikasi diterima', setDoc(doc(W, 'history', 'e1'), ev()), true);
    await check('riwayat: email pelaku palsu DITOLAK', setDoc(doc(W, 'history', 'e2'), ev({ id: 'e2', actorEmail: 'owner@x.id' })), false);
    await check('riwayat: tanggal jauh di masa depan DITOLAK', setDoc(doc(W, 'history', 'e3'), ev({ id: 'e3', timestamp: 9e15 })), false);
    await check('riwayat: field tambahan DITOLAK', setDoc(doc(W, 'history', 'e4'), ev({ id: 'e4', blob: 'x' })), false);
    await check('riwayat: id tidak sama dengan dokumen DITOLAK', setDoc(doc(W, 'history', 'e5'), ev({ id: 'lain' })), false);

    // ---- users ----
    const newbie = env.authenticatedContext('newbie', { email: 'newbie@x.id' }).firestore();
    const signup = x => Object.assign({ uid: 'newbie', email: 'newbie@x.id', displayName: 'Baru', role: 'viewer', status: 'pending', createdAt: 1, updatedAt: 1, updatedBy: null }, x || {});
    await check('daftar: memakai email orang lain DITOLAK', setDoc(doc(newbie, 'users', 'newbie'), signup({ email: 'boss@x.id' })), false);
    await check('daftar: field tambahan DITOLAK', setDoc(doc(newbie, 'users', 'newbie'), signup({ activeSession: { device: '<img>' } })), false);
    await check('daftar: nama sangat panjang DITOLAK', setDoc(doc(newbie, 'users', 'newbie'), signup({ displayName: 'x'.repeat(500) })), false);
    await check('daftar: pendaftaran sah (alur aplikasi) diterima', setDoc(doc(newbie, 'users', 'newbie'), signup(), { merge: true }), true);
    await check('sesi: klaim sesi sah diterima', setDoc(doc(W, 'users', 'writer'), { activeSession: { id: 's_1_abc', startedAt: 1, device: 'Chrome · Android' } }, { merge: true }), true);
    await check('sesi: nama perangkat panjang/berisi field lain DITOLAK', setDoc(doc(W, 'users', 'writer'), { activeSession: { id: 's', device: 'x'.repeat(200), extra: 1 } }, { merge: true }), false);
    await check('sesi: tidak bisa menaikkan role sendiri', setDoc(doc(W, 'users', 'writer'), { role: 'owner' }, { merge: true }), false);

    // ---- id dan nilai shift ----
    await check('unit: id di data berbeda dengan dokumen DITOLAK', setDoc(doc(O, 'units', 'u2'), { id: 'u1', name: 'X' }), false);
    await check('unit: id cocok diterima', setDoc(doc(O, 'units', 'u2'), { id: 'u2', name: 'X' }), true);
    await check('shift: nilai di luar daftar DITOLAK', setDoc(doc(O, 'shifts', '2026-09-30_m1'), { id: '2026-09-30_m1', shift: 'pagi"><img>' }), false);
    await check('shift: nilai sah diterima', setDoc(doc(O, 'shifts', '2026-09-30_m1'), { id: '2026-09-30_m1', shift: 'pagi' }), true);

    await env.cleanup();
    let pass = 0;
    T.forEach(x => { if (x.pass) pass++; else console.log('FAIL', x.name, '\n   ', x.err || ''); });
    console.log(`${pass}/${T.length} lulus`);
    process.exit(pass === T.length ? 0 : 1);
})().catch(e => { console.error(e); process.exit(1); });
