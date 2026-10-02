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
        await u('tech', 'khl', { inspection: 'edit' });
        await u('sup', 'staff', { inspection: 'view', inspectionApprove: 'edit', damage: 'edit' });
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
    const reseed = (id, data) => env.withSecurityRulesDisabled(ctx => setDoc(doc(ctx.firestore(), 'workLogs', id), { id, task: 'x', createdByUid: 'writer', ...data }));
    await check('editor: buat laporan baru langsung dikirim (pending)', setDoc(doc(W, 'workLogs', 'wl_new'), { id: 'wl_new', task: 'y', approval: 'pending', createdByUid: 'writer' }), true);
    await check('editor: buat laporan sebagai draf', setDoc(doc(W, 'workLogs', 'wl_draft'), { id: 'wl_draft', task: 'y', approval: 'draft', createdByUid: 'writer' }), true);
    await check('editor: buat laporan yang langsung "approved" DITOLAK', setDoc(doc(W, 'workLogs', 'wl_bad'), { id: 'wl_bad', task: 'y', approval: 'approved', createdByUid: 'writer' }), false);
    await check('editor: buat laporan "revision" DITOLAK', setDoc(doc(W, 'workLogs', 'wl_bad3'), { id: 'wl_bad3', task: 'y', approval: 'revision', createdByUid: 'writer' }), false);
    await check('editor: buat laporan atas nama penulis lain DITOLAK', setDoc(doc(W, 'workLogs', 'wl_bad2'), { id: 'wl_bad2', task: 'y', approval: 'pending', createdByUid: 'boss' }), false);
    await check('editor: menyetujui lewat console DITOLAK', setDoc(doc(W, 'workLogs', 'wl_1'), { approval: 'approved', approvedBy: 'Boss', approvedByEmail: 'boss@x.id', approvedAt: 1 }, { merge: true }), false);
    // kunci
    await check('kunci: mengubah isi laporan yang MENUNGGU DITOLAK', setDoc(doc(W, 'workLogs', 'wl_1'), { task: 'diubah', approval: 'pending' }, { merge: true }), false);
    await check('kunci: mengubah isi laporan yang DISETUJUI DITOLAK', setDoc(doc(W, 'workLogs', 'wl_ok'), { id: 'wl_ok', task: 'z', approval: 'pending', approvedBy: '', approvedByEmail: '', approvedAt: 0, revisionNote: '', createdByUid: 'writer' }), false);
    await check('kunci: menghapus laporan yang DISETUJUI DITOLAK', deleteDoc(doc(W, 'workLogs', 'wl_ok')), false);
    await check('kunci: menghapus laporan yang MENUNGGU DITOLAK', deleteDoc(doc(W, 'workLogs', 'wl_1')), false);
    await check('kunci: laporan lama tanpa field approval terkunci', (async () => { await reseed('wl_old', {}); })().then(() => setDoc(doc(W, 'workLogs', 'wl_old'), { task: 'ubah' }, { merge: true })), false);
    // draf
    await check('draf: diubah dan tetap draf', setDoc(doc(W, 'workLogs', 'wl_draft'), { task: 'lebih lengkap', approval: 'draft' }, { merge: true }), true);
    await check('draf: dikirim (draft → pending)', setDoc(doc(W, 'workLogs', 'wl_draft'), { approval: 'pending', submittedAt: 7 }, { merge: true }), true);
    // tarik kembali
    await reseed('wl_wd', { approval: 'pending' });
    await check('tarik kembali: orang lain DITOLAK', setDoc(doc(BOTH, 'workLogs', 'wl_wd'), { approval: 'draft', updatedAt: 8 }, { merge: true }), false);
    await check('tarik kembali: sambil mengubah isi DITOLAK', setDoc(doc(W, 'workLogs', 'wl_wd'), { approval: 'draft', task: 'diam-diam', updatedAt: 8 }, { merge: true }), false);
    await check('tarik kembali: pengirimnya sendiri (pending → draft)', setDoc(doc(W, 'workLogs', 'wl_wd'), { approval: 'draft', updatedAt: 8 }, { merge: true }), true);
    await check('tarik kembali: setelah itu bisa diubah', setDoc(doc(W, 'workLogs', 'wl_wd'), { task: 'dibetulkan', approval: 'pending' }, { merge: true }), true);
    await reseed('wl_wd2', { approval: 'approved', approvedByEmail: 'boss@x.id' });
    await check('tarik kembali: yang sudah DISETUJUI DITOLAK', setDoc(doc(W, 'workLogs', 'wl_wd2'), { approval: 'draft', updatedAt: 9 }, { merge: true }), false);
    // atasan
    await check('approver: menyetujui draf (belum dikirim) DITOLAK', (async () => { await reseed('wl_d2', { approval: 'draft' }); })().then(() => setDoc(doc(B, 'workLogs', 'wl_d2'), { approval: 'approved', approvedByEmail: 'boss@x.id', approvedAt: 1 }, { merge: true })), false);
    await check('approver: menyetujui laporan orang lain dengan emailnya sendiri', setDoc(doc(B, 'workLogs', 'wl_1'), { approval: 'approved', approvedBy: 'Boss', approvedByEmail: 'boss@x.id', approvedAt: 2, revisionNote: '', updatedAt: 2 }, { merge: true }), true);
    await reseed('wl_1', { approval: 'pending' });
    await check('approver: menyetujui atas nama orang lain DITOLAK', setDoc(doc(B, 'workLogs', 'wl_1'), { approval: 'approved', approvedByEmail: 'owner@x.id', approvedAt: 3 }, { merge: true }), false);
    await check('approver: mengubah isi laporan DITOLAK', setDoc(doc(B, 'workLogs', 'wl_1'), { task: 'diubah' }, { merge: true }), false);
    await check('approver: minta revisi TANPA catatan DITOLAK', setDoc(doc(B, 'workLogs', 'wl_1'), { approval: 'revision', revisionNote: '', updatedAt: 4 }, { merge: true }), false);
    await check('approver: minta revisi', setDoc(doc(B, 'workLogs', 'wl_1'), { approval: 'revision', revisionNote: 'jam salah', approvedBy: '', approvedByEmail: '', approvedAt: 0, reviewedBy: 'Boss', reviewedAt: 4, updatedAt: 4 }, { merge: true }), true);
    await check('approver: menyetujui yang sedang direvisi DITOLAK (harus dikirim ulang)', setDoc(doc(B, 'workLogs', 'wl_1'), { approval: 'approved', approvedByEmail: 'boss@x.id', approvedAt: 5 }, { merge: true }), false);
    await check('revisi: penulis memperbaiki lalu mengirim ulang', setDoc(doc(W, 'workLogs', 'wl_1'), { task: 'jam dibetulkan', approval: 'pending', revisionNote: '' }, { merge: true }), true);
    await check('approver: membuka kembali yang sudah disetujui (approved → revision)', setDoc(doc(B, 'workLogs', 'wl_ok'), { approval: 'revision', revisionNote: 'paddock salah', approvedBy: '', approvedByEmail: '', approvedAt: 0, updatedAt: 6 }, { merge: true }), true);
    await check('revisi: laporan yang dibuka kembali bisa dihapus penulis', deleteDoc(doc(W, 'workLogs', 'wl_ok')), true);
    await check('approver+editor: menyetujui laporannya SENDIRI DITOLAK', setDoc(doc(BOTH, 'workLogs', 'wl_both'), { approval: 'approved', approvedByEmail: 'both@x.id', approvedAt: 5 }, { merge: true }), false);
    await check('owner: tetap boleh apa saja', setDoc(doc(O, 'workLogs', 'wl_both'), { approval: 'approved', approvedByEmail: 'owner@x.id' }, { merge: true }), true);
    await check('owner: migrasi foto pada laporan disetujui', setDoc(doc(O, 'workLogs', 'wl_both'), { photos: [], photoCount: 2 }, { merge: true }), true);
    await check('editor: mengganti penulis laporan draf DITOLAK', setDoc(doc(W, 'workLogs', 'wl_draft'), { createdByUid: 'someone', approval: 'draft' }, { merge: true }), false);
    // foto ikut terkunci
    await reseed('wl_ph', { approval: 'pending' });
    await check('foto: mengganti foto laporan yang MENUNGGU DITOLAK', setDoc(doc(W, 'workLogPhotos', 'wl_ph'), { id: 'wl_ph', photos: [] }), false);
    await check('foto: laporan baru (belum ada) boleh — foto ditulis duluan', setDoc(doc(W, 'workLogPhotos', 'wl_brandnew'), { id: 'wl_brandnew', photos: ['data:image/png;base64,AA'] }), true);
    await reseed('wl_ph2', { approval: 'draft' });
    await check('foto: laporan draf boleh', setDoc(doc(W, 'workLogPhotos', 'wl_ph2'), { id: 'wl_ph2', photos: [] }), true);
    await check('surat: pengajuan yang MENUNGGU DITOLAK', setDoc(doc(W, 'workLogPhotos', 'lv_1'), { id: 'lv_1', photos: [] }), false);

    // ---- persetujuan: izin / sakit ----
    await check('izin: editor menyetujui lewat console DITOLAK', setDoc(doc(W, 'leaveRequests', 'lv_1'), { approval: 'approved', approvedByEmail: 'boss@x.id' }, { merge: true }), false);
    await check('izin: approver menyetujui', setDoc(doc(B, 'leaveRequests', 'lv_1'), { approval: 'approved', approvedBy: 'Boss', approvedByEmail: 'boss@x.id', approvedAt: 6, revisionNote: '', updatedAt: 6 }, { merge: true }), true);
    await check('izin: mengubah yang sudah disetujui DITOLAK', setDoc(doc(W, 'leaveRequests', 'lv_1'), { dateTo: '2026-12-31', approval: 'pending' }, { merge: true }), false);
    await check('izin: buat draf', setDoc(doc(W, 'leaveRequests', 'lv_d'), { id: 'lv_d', approval: 'draft', createdByUid: 'writer' }), true);
    await check('izin: tarik kembali yang menunggu', (async () => { await env.withSecurityRulesDisabled(ctx => setDoc(doc(ctx.firestore(), 'leaveRequests', 'lv_p'), { id: 'lv_p', approval: 'pending', createdByUid: 'writer' })); })().then(() => setDoc(doc(W, 'leaveRequests', 'lv_p'), { approval: 'draft', updatedAt: 1 }, { merge: true })), true);

    // ---- pengecekan alat berat ----
    const TE = as('tech'), SU = as('sup');
    const seedIns = (id, data) => env.withSecurityRulesDisabled(ctx => setDoc(doc(ctx.firestore(), 'inspections', id), { id, unitId: 'h1', createdByUid: 'tech', ...data }));
    await check('cek: teknisi membuat jadwal', setDoc(doc(TE, 'inspectionPlans', 'p1'), { id: 'p1', date: '2026-10-02', unitIds: ['h1'] }), true);
    await check('cek: atasan membuat jadwal', setDoc(doc(SU, 'inspectionPlans', 'p2'), { id: 'p2', date: '2026-10-03', unitIds: ['h1'] }), true);
    await check('cek: tanpa akses TIDAK bisa membuat jadwal', setDoc(doc(W, 'inspectionPlans', 'p3'), { id: 'p3', date: '2026-10-03', unitIds: [] }), false);
    await check('cek: tanpa akses TIDAK bisa membaca laporan', (async () => { await seedIns('r0', { approval: 'pending' }); })().then(() => getDoc(doc(W, 'inspections', 'r0'))), false);
    await check('cek: foto ditulis duluan (laporan belum ada)', setDoc(doc(TE, 'inspectionPhotos', 'r1'), { id: 'r1', photos: { cameraAi: 'data:image/jpeg;base64,AA' } }), true);
    await check('cek: teknisi mengirim laporan', setDoc(doc(TE, 'inspections', 'r1'), { id: 'r1', unitId: 'h1', approval: 'pending', createdByUid: 'tech' }), true);
    await check('cek: teknisi membuat laporan "approved" DITOLAK', setDoc(doc(TE, 'inspections', 'r2'), { id: 'r2', unitId: 'h1', approval: 'approved', createdByUid: 'tech' }), false);
    await check('cek: laporan menunggu TIDAK bisa diubah teknisi', setDoc(doc(TE, 'inspections', 'r1'), { date: '2026-01-01' }, { merge: true }), false);
    await check('cek: foto laporan menunggu TIDAK bisa diganti', setDoc(doc(TE, 'inspectionPhotos', 'r1'), { id: 'r1', photos: {} }), false);
    await check('cek: teknisi tidak bisa menyetujui', setDoc(doc(TE, 'inspections', 'r1'), { approval: 'approved', approvedByEmail: 'tech@x.id' }, { merge: true }), false);
    await check('cek: pengguna laporan harian (teamLogApprove) tidak bisa menyetujui laporan cek', setDoc(doc(B, 'inspections', 'r1'), { approval: 'approved', approvedByEmail: 'boss@x.id', approvedAt: 1 }, { merge: true }), false);
    await check('cek: atasan menyetujui', setDoc(doc(SU, 'inspections', 'r1'), { approval: 'approved', approvedBy: 'Sup', approvedByEmail: 'sup@x.id', approvedAt: 1, revisionNote: '', updatedAt: 1 }, { merge: true }), true);
    await check('cek: atasan mencatat temuan sudah diterapkan', setDoc(doc(SU, 'inspections', 'r1'), { appliedAt: 2, appliedBy: 'Sup', appliedDamageIds: ['d1'], updatedAt: 2 }, { merge: true }), true);
    await check('cek: stempel diterapkan sambil mengubah hasil DITOLAK', setDoc(doc(SU, 'inspections', 'r1'), { appliedAt: 3, results: { cameraAi: 'good' } }, { merge: true }), false);
    await check('cek: laporan disetujui TIDAK bisa dihapus teknisi', deleteDoc(doc(TE, 'inspections', 'r1')), false);
    await check('cek: atasan membuka kembali (minta revisi)', setDoc(doc(SU, 'inspections', 'r1'), { approval: 'revision', revisionNote: 'foto buram', approvedBy: '', approvedByEmail: '', approvedAt: 0, updatedAt: 4 }, { merge: true }), true);
    await check('cek: setelah revisi teknisi boleh mengganti foto', setDoc(doc(TE, 'inspectionPhotos', 'r1'), { id: 'r1', photos: { cameraAi: 'data:image/jpeg;base64,BB' } }), true);
    await check('cek: dan mengirim ulang', setDoc(doc(TE, 'inspections', 'r1'), { approval: 'pending', revisionNote: '' }, { merge: true }), true);
    await check('cek: atasan tidak bisa menyetujui laporannya sendiri', (async () => { await seedIns('r3', { approval: 'pending', createdByUid: 'sup' }); })().then(() => setDoc(doc(SU, 'inspections', 'r3'), { approval: 'approved', approvedByEmail: 'sup@x.id', approvedAt: 1 }, { merge: true })), false);
    await check('cek: foto laporan bisa dibaca atasan', getDoc(doc(SU, 'inspectionPhotos', 'r1')), true);
    await check('cek: foto laporan TIDAK bisa dibaca tanpa akses', getDoc(doc(N, 'inspectionPhotos', 'r1')), false);

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

    // ---- owner dikunci lewat UID ----
    const OWNER_UID = 'vWZZ3jz1LTdzrU5c5Q1Gzco8nh72';
    const fake = env.authenticatedContext('penyusup', { email: 'mhmmdfiqriarrasyid@gmail.com' }).firestore();
    await check('owner: akun lain dengan email owner TIDAK bisa menjadikan dirinya owner',
        setDoc(doc(fake, 'users', 'penyusup'), { uid: 'penyusup', email: 'mhmmdfiqriarrasyid@gmail.com', role: 'owner', status: 'active' }), false);
    const real = env.authenticatedContext(OWNER_UID, { email: 'mhmmdfiqriarrasyid@gmail.com' }).firestore();
    await check('owner: UID owner bisa membuat/memulihkan dokumennya sendiri',
        setDoc(doc(real, 'users', OWNER_UID), { uid: OWNER_UID, email: 'mhmmdfiqriarrasyid@gmail.com', role: 'owner', status: 'active' }), true);

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
