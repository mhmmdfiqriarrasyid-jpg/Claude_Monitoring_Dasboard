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
const { doc, setDoc, getDoc, getDocs, collection, query, where, orderBy, updateDoc, deleteDoc } = require('firebase/firestore');

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
        await u('wh', 'staff', { warehouse: 'edit' });
        await u('whv', 'staff', { warehouse: 'view' });
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
    await check('editor: buat laporan baru langsung dikirim (pending)', setDoc(doc(W, 'workLogs', 'wl_new'), { id: 'wl_new', task: 'y', approval: 'pending', createdByUid: 'writer', submittedByUid: 'writer' }), true);
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
    await check('draf: dikirim (draft → pending)', setDoc(doc(W, 'workLogs', 'wl_draft'), { approval: 'pending', submittedAt: 7, submittedByUid: 'writer' }, { merge: true }), true);
    // tarik kembali
    await reseed('wl_wd', { approval: 'pending' });
    await check('tarik kembali: orang lain DITOLAK', setDoc(doc(BOTH, 'workLogs', 'wl_wd'), { approval: 'draft', updatedAt: 8 }, { merge: true }), false);
    await check('tarik kembali: sambil mengubah isi DITOLAK', setDoc(doc(W, 'workLogs', 'wl_wd'), { approval: 'draft', task: 'diam-diam', updatedAt: 8 }, { merge: true }), false);
    await check('tarik kembali: pengirimnya sendiri (pending → draft)', setDoc(doc(W, 'workLogs', 'wl_wd'), { approval: 'draft', updatedAt: 8 }, { merge: true }), true);
    await check('tarik kembali: setelah itu bisa diubah', setDoc(doc(W, 'workLogs', 'wl_wd'), { task: 'dibetulkan', approval: 'pending', submittedByUid: 'writer' }, { merge: true }), true);
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
    await check('revisi: penulis memperbaiki lalu mengirim ulang', setDoc(doc(W, 'workLogs', 'wl_1'), { task: 'jam dibetulkan', approval: 'pending', revisionNote: '', submittedByUid: 'writer' }, { merge: true }), true);
    await check('approver: membuka kembali yang sudah disetujui (approved → revision)', setDoc(doc(B, 'workLogs', 'wl_ok'), { approval: 'revision', revisionNote: 'paddock salah', approvedBy: '', approvedByEmail: '', approvedAt: 0, updatedAt: 6 }, { merge: true }), true);
    await check('revisi: laporan yang dibuka kembali bisa dihapus penulis', deleteDoc(doc(W, 'workLogs', 'wl_ok')), true);
    await check('approver+editor: menyetujui laporannya SENDIRI DITOLAK', setDoc(doc(BOTH, 'workLogs', 'wl_both'), { approval: 'approved', approvedByEmail: 'both@x.id', approvedAt: 5 }, { merge: true }), false);
    // ---- pemisahan penulis / pengirim / pemeriksa (v149) ----
    const BI = as('bothIns');
    await env.withSecurityRulesDisabled(ctx => setDoc(doc(ctx.firestore(), 'users', 'bothIns'), { uid: 'bothIns', email: 'bothIns@x.id', role: 'staff', status: 'active', access: { inspection: 'edit', inspectionApprove: 'edit' } }));
    await check('pemisahan: buat laporan TANPA createdByUid DITOLAK', setDoc(doc(BOTH, 'workLogs', 'wl_noauth'), { id: 'wl_noauth', task: 'y', approval: 'pending', submittedByUid: 'both' }), false);
    await check('pemisahan: buat draf TANPA createdByUid DITOLAK', setDoc(doc(W, 'workLogs', 'wl_noauth2'), { id: 'wl_noauth2', task: 'y', approval: 'draft' }), false);
    await check('pemisahan: kirim (pending) tanpa submittedByUid DITOLAK', setDoc(doc(W, 'workLogs', 'wl_nosub'), { id: 'wl_nosub', task: 'y', approval: 'pending', createdByUid: 'writer' }), false);
    await check('pemisahan: kirim atas nama pengirim lain DITOLAK', setDoc(doc(W, 'workLogs', 'wl_fakesub'), { id: 'wl_fakesub', task: 'y', approval: 'pending', createdByUid: 'writer', submittedByUid: 'boss' }), false);
    await reseed('wl_rv', { approval: 'revision', revisionNote: 'ulang', createdByUid: 'writer', submittedByUid: 'writer' });
    await check('pemisahan: approver+editor menulis ulang laporan rekan yang direvisi lalu mengirimnya', setDoc(doc(BOTH, 'workLogs', 'wl_rv'), { task: 'ditulis ulang', approval: 'pending', revisionNote: '', submittedByUid: 'both' }, { merge: true }), true);
    await check('pemisahan: lalu menyetujui laporan yang ia kirim sendiri DITOLAK', setDoc(doc(BOTH, 'workLogs', 'wl_rv'), { approval: 'approved', approvedBy: 'Both', approvedByEmail: 'both@x.id', approvedAt: 9, updatedAt: 9 }, { merge: true }), false);
    await check('pemisahan: approver lain tetap bisa menyetujuinya', setDoc(doc(B, 'workLogs', 'wl_rv'), { approval: 'approved', approvedBy: 'Boss', approvedByEmail: 'boss@x.id', approvedAt: 9, updatedAt: 9 }, { merge: true }), true);
    await reseed('wl_legacy', { approval: 'pending', createdByUid: '' });
    await check('pemisahan: laporan lama tanpa penulis tetap bisa disetujui approver', setDoc(doc(B, 'workLogs', 'wl_legacy'), { approval: 'approved', approvedByEmail: 'boss@x.id', approvedAt: 9 }, { merge: true }), true);
    await check('pemisahan: cek alat berat — buat & kirim sendiri', setDoc(doc(BI, 'inspections', 'r_bi'), { id: 'r_bi', unitId: 'h1', approval: 'pending', createdByUid: 'bothIns', submittedByUid: 'bothIns' }), true);
    await check('pemisahan: cek alat berat — menyetujui sendiri DITOLAK', setDoc(doc(BI, 'inspections', 'r_bi'), { approval: 'approved', approvedByEmail: 'bothIns@x.id', approvedAt: 1 }, { merge: true }), false);
    await env.withSecurityRulesDisabled(ctx => setDoc(doc(ctx.firestore(), 'inspections', 'r_rv'), { id: 'r_rv', unitId: 'h1', approval: 'revision', revisionNote: 'x', createdByUid: 'tech', submittedByUid: 'tech' }));
    await check('pemisahan: cek alat berat — kirim ulang laporan rekan lalu setujui DITOLAK', setDoc(doc(BI, 'inspections', 'r_rv'), { approval: 'pending', revisionNote: '', submittedByUid: 'bothIns' }, { merge: true })
        .then(() => setDoc(doc(BI, 'inspections', 'r_rv'), { approval: 'approved', approvedByEmail: 'bothIns@x.id', approvedAt: 2 }, { merge: true })), false);
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
    await check('cek: teknisi mengirim laporan', setDoc(doc(TE, 'inspections', 'r1'), { id: 'r1', unitId: 'h1', approval: 'pending', createdByUid: 'tech', submittedByUid: 'tech' }), true);
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
    await check('cek: dan mengirim ulang', setDoc(doc(TE, 'inspections', 'r1'), { approval: 'pending', revisionNote: '', submittedByUid: 'tech' }, { merge: true }), true);
    await check('cek: atasan tidak bisa menyetujui laporannya sendiri', (async () => { await seedIns('r3', { approval: 'pending', createdByUid: 'sup' }); })().then(() => setDoc(doc(SU, 'inspections', 'r3'), { approval: 'approved', approvedByEmail: 'sup@x.id', approvedAt: 1 }, { merge: true })), false);
    await check('cek: foto laporan bisa dibaca atasan', getDoc(doc(SU, 'inspectionPhotos', 'r1')), true);
    await check('cek: foto laporan TIDAK bisa dibaca tanpa akses', getDoc(doc(N, 'inspectionPhotos', 'r1')), false);

    // ---- baca per area ----
    await check('surat sakit: tanpa akses laporan TIDAK bisa dibaca', getDoc(doc(N, 'workLogPhotos', 'lv_1')), false);
    await check('surat sakit: jadwal-saja TIDAK bisa dibaca', getDoc(doc(S, 'workLogPhotos', 'lv_1')), false);
    await check('surat sakit: pengisi laporan bisa membaca', getDoc(doc(W, 'workLogPhotos', 'lv_1')), true);
    await check('surat sakit: atasan penyetuju bisa membaca', getDoc(doc(B, 'workLogPhotos', 'lv_1')), true);
    // Kendaraan operasional dan catatan BBM (v158): area Warehouse.
    const WH = as('wh'), WHV = as('whv');
    await check('kendaraan: editor gudang boleh menambah', setDoc(doc(WH, 'vehicles', 'veh_1'), { id: 'veh_1', plate: 'PA 1234 XY', startKm: 8880, handoverDate: '2026-10-16' }), true);
    await check('kendaraan: id di dokumen harus sama', setDoc(doc(WH, 'vehicles', 'veh_2'), { id: 'lain', plate: 'X' }), false);
    await check('kendaraan: tanpa akses gudang TIDAK boleh menulis', setDoc(doc(N, 'vehicles', 'veh_3'), { id: 'veh_3', plate: 'X' }), false);
    await check('kendaraan: hanya-lihat gudang TIDAK boleh menulis', setDoc(doc(WHV, 'vehicles', 'veh_4'), { id: 'veh_4', plate: 'X' }), false);
    await check('catatan BBM: editor gudang boleh menulis', setDoc(doc(WH, 'fuelLogs', 'fuel_1'), { id: 'fuel_1', vehicleId: 'veh_1', date: '2026-10-16', liters: 3.2, km: 9012 }), true);
    await check('catatan BBM: pengguna aktif bisa membaca', getDoc(doc(W, 'fuelLogs', 'fuel_1')), true);
    await check('catatan BBM: tanpa akses TIDAK boleh menghapus', deleteDoc(doc(N, 'fuelLogs', 'fuel_1')), false);
    await check('foto struk: editor gudang boleh menulis', setDoc(doc(WH, 'fuelPhotos', 'fuel_1'), { id: 'fuel_1', photo: 'data:image/jpeg;base64,AA' }), true);
    await check('foto struk: hanya-lihat gudang bisa membaca', getDoc(doc(WHV, 'fuelPhotos', 'fuel_1')), true);
    await check('foto struk: tanpa akses gudang TIDAK bisa membaca', getDoc(doc(N, 'fuelPhotos', 'fuel_1')), false);

    // Foto kerusakan Before = damagePhotos/{id}, After = damagePhotos/{id}__after.
    await check('foto After kerusakan: penyunting kerusakan boleh menulis', setDoc(doc(SU, 'damagePhotos', 'dmg_1__after'), { id: 'dmg_1', side: 'after', photo: 'data:image/jpeg;base64,AA', updatedAt: 1 }), true);
    await check('foto After kerusakan: penyunting kerusakan bisa membaca', getDoc(doc(SU, 'damagePhotos', 'dmg_1__after')), true);
    await check('foto After kerusakan: tanpa akses TIDAK bisa membaca', getDoc(doc(N, 'damagePhotos', 'dmg_1__after')), false);
    await check('foto After kerusakan: tanpa akses TIDAK bisa menulis', setDoc(doc(N, 'damagePhotos', 'dmg_2__after'), { id: 'dmg_2', photo: '' }), false);
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
    // ---- sesi: dikeluarkan owner lalu masuk lagi (v149) ----
    // Dokumen yang macet oleh versi lama: revokedAt/revokedBy tertinggal di map.
    await env.withSecurityRulesDisabled(ctx => setDoc(doc(ctx.firestore(), 'users', 'writer'),
        { activeSession: { id: 'revoked_1', revokedAt: 1, revokedBy: 'owner@x.id', startedAt: 1, device: 'Chrome · Android' } }, { merge: true }));
    await check('sesi: klaim gaya lama (merge) pada map macet DITOLAK — inilah bug-nya', setDoc(doc(W, 'users', 'writer'), { activeSession: { id: 's_2', startedAt: 2, device: 'HP' } }, { merge: true }), false);
    await check('sesi: klaim yang MENGGANTI seluruh map memulihkan akun macet', updateDoc(doc(W, 'users', 'writer'), { activeSession: { id: 's_3', startedAt: 3, device: 'HP' } }), true);
    await check('sesi: owner mengeluarkan (bentuk 3 kunci)', updateDoc(doc(O, 'users', 'writer'), { activeSession: { id: 'revoked_4', startedAt: 4, device: 'Signed out by owner@x.id' } }), true);
    await check('sesi: setelah dikeluarkan, pengguna bisa masuk lagi', updateDoc(doc(W, 'users', 'writer'), { activeSession: { id: 's_5', startedAt: 5, device: 'HP' } }), true);
    await check('sesi: map pengganti dengan kunci lain tetap DITOLAK', updateDoc(doc(W, 'users', 'writer'), { activeSession: { id: 's_6', startedAt: 6, device: 'HP', role: 'owner' } }), false);
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

    // ---- daftar master (Site / Brand / Company) ----
    await env.withSecurityRulesDisabled(async ctx => {
        const db = ctx.firestore();
        await setDoc(doc(db, 'users', 'impl'), { uid: 'impl', email: 'impl@x.id', role: 'staff', status: 'active', access: { implements: 'edit' } });
        await setDoc(doc(db, 'users', 'team'), { uid: 'team', email: 'team@x.id', role: 'staff', status: 'active', access: { teamMembers: 'edit' } });
    });
    const IM = as('impl'), TM = as('team');
    const ml = (kind, values) => ({ id: kind, values: values || ['A'], updatedAt: 1, updatedBy: 'x' });
    await check('master: semua akun aktif bisa membaca', getDoc(doc(N, 'masterLists', 'site')), true);
    await check('master: editor unit menulis daftar Site', setDoc(doc(S, 'masterLists', 'site'), ml('site', ['PT. GPA'])), true);
    await check('master: editor unit menulis daftar Brand', setDoc(doc(S, 'masterLists', 'brand'), ml('brand')), true);
    await check('master: editor unit TIDAK bisa menulis Company', setDoc(doc(S, 'masterLists', 'company'), ml('company')), false);
    await check('master: editor Implements menulis Brand', setDoc(doc(IM, 'masterLists', 'brand'), ml('brand', ['Gessner'])), true);
    await check('master: editor Implements TIDAK bisa menulis Site', setDoc(doc(IM, 'masterLists', 'site'), ml('site')), false);
    await check('master: editor unit menulis Heavy Equipment Brand', setDoc(doc(S, 'masterLists', 'heavyBrand'), ml('heavyBrand', ['LiuGong'])), true);
    await check('master: editor Implements TIDAK bisa menulis Heavy Equipment Brand', setDoc(doc(IM, 'masterLists', 'heavyBrand'), ml('heavyBrand')), false);
    await check('master: editor anggota tim menulis Company', setDoc(doc(TM, 'masterLists', 'company'), ml('company', ['PT. GPA'])), true);
    await check('master: tanpa akses TIDAK bisa menulis', setDoc(doc(N, 'masterLists', 'site'), ml('site')), false);
    await check('master: daftar lain di luar site/brand/company DITOLAK', setDoc(doc(O, 'masterLists', 'role'), ml('role')), false);
    await check('master: id beda dengan dokumen DITOLAK', setDoc(doc(O, 'masterLists', 'site'), ml('brand')), false);
    await check('master: values bukan list DITOLAK', setDoc(doc(O, 'masterLists', 'site'), { id: 'site', values: 'PT. GPA' }), false);
    await check('master: lebih dari 500 nilai DITOLAK', setDoc(doc(O, 'masterLists', 'site'), ml('site', Array.from({ length: 501 }, (_, i) => 'S' + i))), false);
    await check('master: owner menulis apa pun dari tiga daftar', setDoc(doc(O, 'masterLists', 'company'), ml('company')), true);

    // ---- syarat lisensi per jenis implement ----
    await env.withSecurityRulesDisabled(async ctx => {
        await setDoc(doc(ctx.firestore(), 'users', 'lic'), { uid: 'lic', email: 'lic@x.id', role: 'staff', status: 'active', access: { licenseStock: 'edit' } });
    });
    const LI = as('lic');
    const lr = (id, gps, display) => ({ id, type: 'Bed Ripper', gps, display, updatedAt: 1 });
    await check('lisensi: semua akun aktif bisa membaca syarat', getDoc(doc(N, 'licenseRequirements', 'bed-ripper')), true);
    await check('lisensi: editor License Stock menulis syarat', setDoc(doc(LI, 'licenseRequirements', 'bed-ripper'), lr('bed-ripper', 'SF-RTK', 'G5 Advance')), true);
    await check('lisensi: editor unit TIDAK bisa menulis syarat', setDoc(doc(S, 'licenseRequirements', 'bed-ripper'), lr('bed-ripper', 'SF-RTK', 'G5 Basic')), false);
    await check('lisensi: nilai lisensi asing DITOLAK', setDoc(doc(LI, 'licenseRequirements', 'bed-ripper'), lr('bed-ripper', 'SF-RTK<x>', 'G5 Advance')), false);
    await check('lisensi: id beda dengan dokumen DITOLAK', setDoc(doc(LI, 'licenseRequirements', 'bed-ripper'), lr('other', 'SF-RTK', '')), false);
    await check('lisensi: editor License Stock menghapus syarat', deleteDoc(doc(LI, 'licenseRequirements', 'bed-ripper')), true);

    // ---- v162: kerusakan menahan status unit (editor Damage tanpa edit unit) ----
    await env.withSecurityRulesDisabled(async ctx => {
        const db = ctx.firestore();
        await setDoc(doc(db, 'users', 'dmg'), { uid: 'dmg', email: 'dmg@x.id', role: 'staff', status: 'active', access: { damage: 'edit', editUnits: 'view' } });
        await setDoc(doc(db, 'users', 'dmgv'), { uid: 'dmgv', email: 'dmgv@x.id', role: 'staff', status: 'active', access: { damage: 'view' } });
        await setDoc(doc(db, 'units', 'hx'), { id: 'hx', name: 'EX-01', sn: 'SNX', unitGroup: 'heavy', status: 'Good', cameraAi: 'Good',
            site: 'PT. GPA', breakdownReason: '', downtimeHistory: [{ start: 1, end: 2 }, { start: 3, end: 4 }] });
    });
    const DM = as('dmg'), DV = as('dmgv');
    const T0 = Date.now();
    await check('damage→unit: editor Damage set Breakdown + alasan + id kerusakan', updateDoc(doc(DM, 'units', 'hx'),
        { status: 'Breakdown', breakdownReason: 'Mekanis: bocor', breakdownStartedAt: T0, breakdownDamageId: 'dmg_1' }), true);
    await check('damage→unit: jam breakdown diubah tanpa ganti status DITOLAK', updateDoc(doc(DM, 'units', 'hx'), { breakdownStartedAt: T0 - 3600000 }), false);
    await check('damage→unit: id kerusakan sangat panjang DITOLAK', updateDoc(doc(DM, 'units', 'hx'), { breakdownDamageId: 'x'.repeat(200) }), false);
    await check('damage→unit: alasan + id pindah ke kerusakan lain', updateDoc(doc(DM, 'units', 'hx'), { breakdownReason: 'Software: ECU', breakdownDamageId: 'dmg_2' }), true);
    await check('damage→unit: editor Damage set komponen Breakdown', updateDoc(doc(DM, 'units', 'hx'), { cameraAi: 'Breakdown' }), true);
    await check('damage→unit: interval palsu (mulai bukan jam breakdown) DITOLAK', updateDoc(doc(DM, 'units', 'hx'),
        { status: 'Good', breakdownReason: '', breakdownStartedAt: null, breakdownDamageId: '',
          downtimeHistory: [{ start: 1, end: 2 }, { start: 3, end: 4 }, { start: T0 - 99999, end: T0 + 10, durationMs: 99999 + 10 }] }), false);
    await check('damage→unit: riwayat lama diubah (jumlah sama) DITOLAK', updateDoc(doc(DM, 'units', 'hx'),
        { downtimeHistory: [{ start: 0, end: 0 }, { start: 0, end: 0 }] }), false);
    await check('damage→unit: kembali Good dengan satu interval downtime baru', updateDoc(doc(DM, 'units', 'hx'),
        { status: 'Good', breakdownReason: '', breakdownStartedAt: null, breakdownDamageId: '',
          downtimeHistory: [{ start: 1, end: 2 }, { start: 3, end: 4 }, { start: T0, end: T0 + 10, durationMs: 10 }] }), true);
    await check('damage→unit: jam breakdown tahun 1970 DITOLAK', updateDoc(doc(DM, 'units', 'hx'),
        { status: 'Breakdown', breakdownReason: 'x', breakdownStartedAt: 0, breakdownDamageId: 'dmg_3' }), false);
    await check('damage→unit: menghapus riwayat downtime DITOLAK', updateDoc(doc(DM, 'units', 'hx'), { downtimeHistory: [] }), false);
    await check('damage→unit: menambah dua interval sekaligus DITOLAK', updateDoc(doc(DM, 'units', 'hx'),
        { downtimeHistory: [{ start: 1, end: 2 }, { start: 3, end: 4 }, { start: 10, end: 20 }, { start: 30, end: 40 }, { start: 50, end: 60 }] }), false);
    await check('damage→unit: mengubah nama unit DITOLAK', updateDoc(doc(DM, 'units', 'hx'), { name: 'EX-99' }), false);
    await check('damage→unit: mengubah kelompok unit DITOLAK', updateDoc(doc(DM, 'units', 'hx'), { unitGroup: 'tractor' }), false);
    await check('damage→unit: status + site sekaligus DITOLAK', updateDoc(doc(DM, 'units', 'hx'), { status: 'Breakdown', site: 'PT. MNM' }), false);
    await check('damage→unit: nilai status selain Good/Breakdown DITOLAK', updateDoc(doc(DM, 'units', 'hx'), { status: 'Rusak' }), false);
    await check('damage→unit: alasan bukan teks DITOLAK', updateDoc(doc(DM, 'units', 'hx'), { breakdownReason: 5 }), false);
    await check('damage→unit: alasan sangat panjang DITOLAK', updateDoc(doc(DM, 'units', 'hx'), { breakdownReason: 'x'.repeat(1200) }), false);
    await check('damage→unit: firebase-init lama (set merge parsial) diterima', setDoc(doc(DM, 'units', 'hx'), { id: 'hx', status: 'Good' }, { merge: true }), true);
    await check('damage→unit: editor Damage TIDAK boleh membuat unit', setDoc(doc(DM, 'units', 'hy'), { id: 'hy', name: 'Baru', status: 'Good' }), false);
    await check('damage→unit: editor Damage TIDAK boleh menghapus unit', deleteDoc(doc(DM, 'units', 'hx')), false);
    await check('damage→unit: hanya-lihat Damage TIDAK boleh mengubah status', updateDoc(doc(DV, 'units', 'hx'), { status: 'Breakdown' }), false);
    await check('damage→unit: tanpa akses TIDAK boleh mengubah status', updateDoc(doc(N, 'units', 'hx'), { status: 'Breakdown' }), false);
    await check('damage→unit: editor unit tetap boleh mengubah apa pun', updateDoc(doc(S, 'units', 'hx'), { name: 'EX-01b', site: 'PT. MNM', downtimeHistory: [] }), true);

    // ---- v165: akun yang dibatasi ke satu kelompok unit ----
    await env.withSecurityRulesDisabled(async ctx => {
        const db = ctx.firestore();
        const u = (uid, access) => setDoc(doc(db, 'users', uid), { uid, email: uid + '@x.id', role: 'staff', status: 'active', access });
        await u('hv', { editUnits: 'edit', damage: 'edit', implements: 'edit', inspection: 'edit', unitScope: 'heavy' });
        await u('ag', { editUnits: 'view', implements: 'edit', licenseStock: 'edit', leader: 'view', unitScope: 'tractor' });
        await u('badsc', { editUnits: 'edit', unitScope: '' });
        await setDoc(doc(db, 'users', 'own2'), { uid: 'own2', email: 'own2@x.id', role: 'owner', status: 'active', access: { unitScope: 'heavy' } });
        await setDoc(doc(db, 'implements', 'im1'), { id: 'im1', profileName: 'Rotary' });
        await setDoc(doc(db, 'licenseStock', 'ls1'), { id: 'ls1', txnType: 'IN' });
        await setDoc(doc(db, 'inspections', 'ins1'), { id: 'ins1', approval: 'approved', createdByUid: 'tech' });
    });
    const HV = as('hv'), AG = as('ag'), BAD = as('badsc'), OW2 = as('own2');
    await check('kelompok: alat berat saja TIDAK boleh mengubah unit (hanya lihat)', updateDoc(doc(HV, 'units', 'u1'), { name: 'X' }), false);
    await check('kelompok: alat berat saja TIDAK boleh menulis kerusakan', setDoc(doc(HV, 'damageRecords', 'dmg_hv'), { id: 'dmg_hv', unitId: 'hx' }), false);
    await check('kelompok: alat berat saja TIDAK boleh mengubah status lewat kerusakan', updateDoc(doc(HV, 'units', 'hx'), { status: 'Breakdown' }), false);
    await check('kelompok: alat berat saja membaca unit alat berat', getDoc(doc(HV, 'units', 'hx')), true);
    await check('kelompok: alat berat saja TIDAK bisa membaca daftar implement', getDocs(collection(HV, 'implements')), false);
    await check('kelompok: alat berat saja TIDAK bisa membaca stok lisensi', getDocs(collection(HV, 'licenseStock')), false);
    await check('kelompok: alat berat saja TIDAK bisa membaca syarat lisensi', getDocs(collection(HV, 'licenseRequirements')), false);
    await check('kelompok: alat berat saja TIDAK boleh menulis implement', setDoc(doc(HV, 'implements', 'im2'), { id: 'im2' }), false);
    await check('kelompok: alat berat saja tetap bisa membuat jadwal pengecekan', setDoc(doc(HV, 'inspectionPlans', 'pl_hv'), { id: 'pl_hv', unitId: 'hx' }), true);
    await check('kelompok: alat berat saja tetap bisa membaca laporan pengecekan', getDocs(collection(HV, 'inspections')), true);
    await check('kelompok: alat berat saja tetap boleh menulis riwayat (punya edit Inspection)', setDoc(doc(HV, 'history', 'h_hv'),
        { id: 'h_hv', timestamp: Date.now(), action: 'add', unitId: 'hx', unitName: 'EX', field: 'x', before: '', after: '', actorUid: 'hv', actorEmail: 'hv@x.id', actorName: 'hv', actorRole: 'staff' }), true);
    await check('kelompok: pertanian saja menulis implement', setDoc(doc(AG, 'implements', 'im3'), { id: 'im3', profileName: 'Bajak' }), true);
    await check('kelompok: pertanian saja membaca stok lisensi', getDocs(collection(AG, 'licenseStock')), true);
    await check('kelompok: pertanian saja TIDAK bisa membaca laporan pengecekan (walau Leader)', getDocs(collection(AG, 'inspections')), false);
    await check('kelompok: pertanian saja TIDAK bisa membaca jadwal pengecekan', getDocs(collection(AG, 'inspectionPlans')), false);
    await check('kelompok: nilai kelompok kosong ditutup (tidak boleh mengubah unit)', updateDoc(doc(BAD, 'units', 'u1'), { name: 'Y' }), false);
    await check('kelompok: owner tidak pernah dibatasi', updateDoc(doc(OW2, 'units', 'u1'), { name: 'T' }), true);
    await check('kelompok: owner memberi batas Heavy Equipment', updateDoc(doc(O, 'users', 'writer'), { access: { teamLog: 'edit', unitScope: 'heavy' } }), true);
    await check('kelompok: owner memberi batas yang tidak dikenal DITOLAK', updateDoc(doc(O, 'users', 'writer'), { access: { teamLog: 'edit', unitScope: 'semua' } }), false);
    await check('kelompok: ganti role akun bernilai kelompok rusak tetap bisa', updateDoc(doc(O, 'users', 'badsc'), { role: 'pbt' }), true);
    await check('kelompok: keluarkan paksa akun bernilai kelompok rusak tetap bisa', updateDoc(doc(O, 'users', 'badsc'),
        { activeSession: { id: 'revoked_x', startedAt: 1, device: 'x' } }), true);
    await env.withSecurityRulesDisabled(ctx => setDoc(doc(ctx.firestore(), 'users', 'oddacc'), { uid: 'oddacc', email: 'oddacc@x.id', role: 'staff', status: 'active', access: 'rusak' }));
    await check('kelompok: field access bukan map tidak membuat bacaan gagal', getDocs(collection(as('oddacc'), 'implements')), true);
    await check('kelompok: akun terbatas TIDAK boleh mengubah batasnya sendiri', updateDoc(doc(HV, 'users', 'hv'), { access: { unitScope: 'all', editUnits: 'edit' } }), false);

    // ---- v167: bacaan unit, kerusakan, foto dan riwayat dikunci per kelompok ----
    await env.withSecurityRulesDisabled(async ctx => {
        const db = ctx.firestore();
        await setDoc(doc(db, 'users', 'hvv'), { uid: 'hvv', email: 'hvv@x.id', role: 'staff', status: 'active',
            access: { editUnits: 'view', damage: 'view', history: 'view', unitScope: 'heavy' } });
        await setDoc(doc(db, 'units', 'tx'), { id: 'tx', name: 'TR', unitGroup: 'tractor' });
        await setDoc(doc(db, 'units', 'ux'), { id: 'ux', name: 'TR lama' });
        await setDoc(doc(db, 'damageRecords', 'dh'), { id: 'dh', unitId: 'hx', unitGroup: 'heavy' });
        await setDoc(doc(db, 'damageRecords', 'dt'), { id: 'dt', unitId: 'tx', unitGroup: 'tractor' });
        await setDoc(doc(db, 'damagePhotos', 'dh'), { id: 'dh', photo: 'x' });
        await setDoc(doc(db, 'damagePhotos', 'dh__after'), { id: 'dh', side: 'after', photo: 'x' });
        await setDoc(doc(db, 'damagePhotos', 'dt'), { id: 'dt', photo: 'x' });
        for (const [id, g] of [['hh', 'heavy'], ['ht', 'tractor'], ['hs', 'shared']]) {
            await setDoc(doc(db, 'history', id), { id, timestamp: 1, action: 'x', unitGroup: g });
        }
    });
    const HVV = as('hvv');
    await check('rahasia: alat berat saja membaca unit kelompoknya (query per kelompok)', getDocs(query(collection(HVV, 'units'), where('unitGroup', '==', 'heavy'))), true);
    await check('rahasia: alat berat saja TIDAK bisa membaca seluruh koleksi unit', getDocs(collection(HVV, 'units')), false);
    await check('rahasia: alat berat saja TIDAK bisa meminta unit pertanian', getDocs(query(collection(HVV, 'units'), where('unitGroup', '==', 'tractor'))), false);
    await check('rahasia: alat berat saja TIDAK bisa membuka satu unit pertanian', getDoc(doc(HVV, 'units', 'tx')), false);
    await check('rahasia: alat berat saja TIDAK bisa membuka unit tanpa kelompok', getDoc(doc(HVV, 'units', 'ux')), false);
    await check('rahasia: alat berat saja membuka satu unit alat berat', getDoc(doc(HVV, 'units', 'hx')), true);
    await check('rahasia: kerusakan per kelompok terbaca', getDocs(query(collection(HVV, 'damageRecords'), where('unitGroup', '==', 'heavy'))), true);
    await check('rahasia: seluruh koleksi kerusakan DITOLAK', getDocs(collection(HVV, 'damageRecords')), false);
    await check('rahasia: kerusakan pertanian DITOLAK', getDoc(doc(HVV, 'damageRecords', 'dt')), false);
    await check('rahasia: foto kerusakan alat berat terbaca', getDoc(doc(HVV, 'damagePhotos', 'dh')), true);
    await check('rahasia: foto After kerusakan alat berat terbaca', getDoc(doc(HVV, 'damagePhotos', 'dh__after')), true);
    await check('rahasia: foto kerusakan pertanian DITOLAK', getDoc(doc(HVV, 'damagePhotos', 'dt')), false);
    await check('rahasia: riwayat kelompoknya + bersama terbaca (query per kelompok)', getDocs(query(collection(HVV, 'history'),
        where('unitGroup', 'in', ['heavy', 'shared']), orderBy('timestamp', 'desc'))), true);
    await check('rahasia: seluruh riwayat DITOLAK', getDocs(query(collection(HVV, 'history'), orderBy('timestamp', 'desc'))), false);
    await check('rahasia: baris riwayat pertanian DITOLAK', getDoc(doc(HVV, 'history', 'ht')), false);
    await check('rahasia: akun tanpa batas tetap membaca semua unit', getDocs(collection(S, 'units')), true);
    await check('rahasia: owner tetap membaca semua kerusakan', getDocs(collection(O, 'damageRecords')), true);
    await check('rahasia: baris riwayat baru membawa kelompoknya', setDoc(doc(W, 'history', 'h_g1'),
        { id: 'h_g1', timestamp: Date.now(), action: 'x', unitId: '', unitName: '[Team] x', field: '', before: '', after: '', actorUid: 'writer', actorEmail: 'writer@x.id', actorName: 'w', actorRole: 'staff', unitGroup: 'shared' }), true);
    await check('rahasia: kelompok riwayat yang tidak dikenal DITOLAK', setDoc(doc(W, 'history', 'h_g2'),
        { id: 'h_g2', timestamp: Date.now(), action: 'x', unitId: '', unitName: 'x', field: '', before: '', after: '', actorUid: 'writer', actorEmail: 'writer@x.id', actorName: 'w', actorRole: 'staff', unitGroup: 'semua' }), false);

    // ---- v168: perbaikan review ----
    await env.withSecurityRulesDisabled(async ctx => {
        const db = ctx.firestore();
        await setDoc(doc(db, 'units', 'unone'), { id: 'unone', unitGroup: 'none' });
        await setDoc(doc(db, 'history', 'hold'), { id: 'hold', timestamp: 1, action: 'x' });
    });
    await check('rahasia: kelompok tak dikenal ("none") tidak membaca dokumen bernilai "none"', getDoc(doc(BAD, 'units', 'unone')), false);
    await check('rahasia: editor kerusakan TIDAK boleh mengosongkan kelompok catatan', updateDoc(doc(DM, 'damageRecords', 'dt'), { unitGroup: '' }), false);
    await check('rahasia: editor kerusakan tetap boleh mengedit catatan (kelompok tetap)', updateDoc(doc(DM, 'damageRecords', 'dt'), { description: 'x' }), true);
    await check('riwayat lama: owner menambahkan kelompok sekali', updateDoc(doc(O, 'history', 'hold'), { unitGroup: 'shared' }), true);
    await check('riwayat lama: kelompok yang sudah ada TIDAK bisa diubah', updateDoc(doc(O, 'history', 'hold'), { unitGroup: 'heavy' }), false);
    await check('riwayat lama: bukan owner TIDAK boleh menambahkan kelompok', updateDoc(doc(W, 'history', 'h1'), { unitGroup: 'shared' }), false);
    await check('riwayat lama: owner TIDAK boleh mengubah kolom lain', updateDoc(doc(O, 'history', 'h1'), { unitGroup: 'shared', after: 'diubah' }), false);

    // ---- v175: Daily Report "own" dan peran kustom ----
    await env.withSecurityRulesDisabled(async ctx => {
        const db = ctx.firestore();
        await setDoc(doc(db, 'users', 'mine'), { uid: 'mine', email: 'mine@x.id', role: 'staff', status: 'active', access: { teamLog: 'own', teamShift: 'view' } });
        await setDoc(doc(db, 'users', 'mine2'), { uid: 'mine2', email: 'mine2@x.id', role: 'staff', status: 'active', access: { teamLog: 'own' } });
        await setDoc(doc(db, 'workLogs', 'own_open'), { id: 'own_open', task: 'x', approval: 'draft', createdByUid: 'mine' });
        await setDoc(doc(db, 'workLogs', 'own_ok'), { id: 'own_ok', task: 'x', approval: 'approved', approvedByEmail: 'boss@x.id', createdByUid: 'mine' });
        await setDoc(doc(db, 'workLogs', 'other_open'), { id: 'other_open', task: 'x', approval: 'draft', createdByUid: 'mine2' });
        await setDoc(doc(db, 'leaveRequests', 'own_lv'), { id: 'own_lv', reason: 'x', approval: 'draft', createdByUid: 'mine' });
        await setDoc(doc(db, 'leaveRequests', 'other_lv'), { id: 'other_lv', reason: 'x', approval: 'draft', createdByUid: 'mine2' });
        await setDoc(doc(db, 'workLogPhotos', 'own_open'), { id: 'own_open', photos: [] });
        await setDoc(doc(db, 'workLogPhotos', 'other_open'), { id: 'other_open', photos: [] });
        await setDoc(doc(db, 'roles', 'role_x'), { id: 'role_x', name: 'Field', access: { teamLog: 'own' }, csv: 'none' });
    });
    const MY = as('mine'), MY2 = as('mine2');
    await check('own: membaca laporannya sendiri', getDoc(doc(MY, 'workLogs', 'own_open')), true);
    await check('own: membaca laporan orang lain DITOLAK', getDoc(doc(MY, 'workLogs', 'other_open')), false);
    await check('own: query laporannya sendiri (where createdByUid)', getDocs(query(collection(MY, 'workLogs'), where('createdByUid', '==', 'mine'))), true);
    await check('own: query semua laporan DITOLAK', getDocs(collection(MY, 'workLogs')), false);
    await check('own: membuat laporan atas namanya sendiri', setDoc(doc(MY, 'workLogs', 'own_new'), { id: 'own_new', task: 'y', approval: 'pending', createdByUid: 'mine', submittedByUid: 'mine' }), true);
    await check('own: membuat laporan atas nama orang lain DITOLAK', setDoc(doc(MY, 'workLogs', 'own_bad'), { id: 'own_bad', task: 'y', approval: 'draft', createdByUid: 'mine2' }), false);
    await check('own: mengubah draf sendiri', setDoc(doc(MY, 'workLogs', 'own_open'), { task: 'lebih lengkap', approval: 'draft' }, { merge: true }), true);
    await check('own: mengubah draf orang lain DITOLAK', setDoc(doc(MY, 'workLogs', 'other_open'), { task: 'diubah', approval: 'draft' }, { merge: true }), false);
    await check('own: menghapus draf orang lain DITOLAK', deleteDoc(doc(MY, 'workLogs', 'other_open')), false);
    await check('own: laporan sendiri yang DISETUJUI hanya bisa dibaca', setDoc(doc(MY, 'workLogs', 'own_ok'), { task: 'diubah', approval: 'pending', submittedByUid: 'mine' }, { merge: true }), false);
    await check('own: laporan sendiri yang DISETUJUI tetap bisa dibaca', getDoc(doc(MY, 'workLogs', 'own_ok')), true);
    await check('own: menghapus laporan sendiri yang disetujui DITOLAK', deleteDoc(doc(MY, 'workLogs', 'own_ok')), false);
    await check('own: menyetujui laporan sendiri DITOLAK', setDoc(doc(MY, 'workLogs', 'own_new'), { approval: 'approved', approvedByEmail: 'mine@x.id', approvedAt: 1 }, { merge: true }), false);
    await check('own: menarik kembali kirimannya sendiri', setDoc(doc(MY, 'workLogs', 'own_new'), { approval: 'draft', updatedAt: 9 }, { merge: true }), true);
    await check('own: menghapus draf sendiri', deleteDoc(doc(MY, 'workLogs', 'own_new')), true);
    await check('own: izin sendiri bisa dibuat', setDoc(doc(MY2, 'leaveRequests', 'lv_new2'), { id: 'lv_new2', reason: 'y', approval: 'draft', createdByUid: 'mine2' }), true);
    await check('own (tanpa jadwal): membaca izin orang lain DITOLAK', getDoc(doc(MY2, 'leaveRequests', 'own_lv')), false);
    await check('own (dengan jadwal): membaca izin orang lain untuk jadwal', getDoc(doc(MY, 'leaveRequests', 'other_lv')), true);
    await check('own: mengubah izin orang lain DITOLAK', setDoc(doc(MY, 'leaveRequests', 'other_lv'), { reason: 'ubah', approval: 'draft' }, { merge: true }), false);
    await check('own: foto laporannya sendiri bisa dibaca', getDoc(doc(MY, 'workLogPhotos', 'own_open')), true);
    await check('own: foto laporan orang lain DITOLAK', getDoc(doc(MY, 'workLogPhotos', 'other_open')), false);
    await check('own: menulis foto laporan orang lain DITOLAK', setDoc(doc(MY, 'workLogPhotos', 'other_open'), { id: 'other_open', photos: ['x'] }), false);
    await check('own: menulis foto laporan baru (belum ada laporan)', setDoc(doc(MY, 'workLogPhotos', 'own_fresh'), { id: 'own_fresh', photos: ['x'] }), true);
    await check('own: boleh menulis riwayat (laporannya sendiri)', setDoc(doc(MY, 'history', 'h_own'),
        { id: 'h_own', timestamp: Date.now(), action: 'create', unitId: '', unitName: '[Team] x', field: '', before: '', after: '', actorUid: 'mine', actorEmail: 'mine@x.id', actorName: 'm', actorRole: 'staff', unitGroup: 'shared' }), true);
    await check('peran: owner membaca dan membuat peran', getDocs(collection(O, 'roles')), true);
    await check('peran: owner menyimpan peran', setDoc(doc(O, 'roles', 'role_y'), { id: 'role_y', name: 'Tech', access: { damage: 'edit' }, csv: 'none' }), true);
    await check('peran: nama kosong DITOLAK', setDoc(doc(O, 'roles', 'role_z'), { id: 'role_z', name: '', access: {} }), false);
    await check('peran: bukan owner TIDAK bisa membaca', getDocs(collection(W, 'roles')), false);
    await check('peran: bukan owner TIDAK bisa menulis', setDoc(doc(W, 'roles', 'role_w'), { id: 'role_w', name: 'X', access: {} }), false);
    await check('peran: owner menghapus peran', deleteDoc(doc(O, 'roles', 'role_y')), true);
    await check('peran: owner memberi peran (roleId + access)', updateDoc(doc(O, 'users', 'mine2'), { roleId: 'role_x', roleName: 'Field', access: { teamLog: 'own', unitScope: 'all' } }), true);
    await check('peran: orang itu TIDAK bisa memberi dirinya peran', updateDoc(doc(MY2, 'users', 'mine2'), { roleId: 'role_y', access: { teamLog: 'edit' } }), false);

    // ---- v185: foto warehouse ----
    const PH_ED = as('wh'), PH_RD = as('whv');
    await check('foto gudang: editor warehouse menyimpan foto', setDoc(doc(PH_ED, 'warehousePhotos', 'd1__p1'), { id: 'd1__p1', ownerId: 'd1', photo: 'data:image/jpeg;base64,AA' }), true);
    await check('foto gudang: pembaca warehouse bisa membuka', getDoc(doc(PH_RD, 'warehousePhotos', 'd1__p1')), true);
    await check('foto gudang: pembaca TIDAK bisa menulis', setDoc(doc(PH_RD, 'warehousePhotos', 'd1__p2'), { id: 'd1__p2', ownerId: 'd1', photo: 'x' }), false);
    await check('foto gudang: tanpa akses warehouse TIDAK bisa membaca', getDoc(doc(N, 'warehousePhotos', 'd1__p1')), false);
    await check('foto gudang: foto terlalu besar DITOLAK', setDoc(doc(PH_ED, 'warehousePhotos', 'd1__p3'), { id: 'd1__p3', ownerId: 'd1', photo: 'x'.repeat(400001) }), false);
    await check('foto gudang: editor menghapus foto', deleteDoc(doc(PH_ED, 'warehousePhotos', 'd1__p1')), true);

    // ---- v186: daftar Damage Types ----
    await check('Damage Types: editor Damage menyimpan daftar', setDoc(doc(as('sup'), 'masterLists', 'damageType'), { id: 'damageType', values: ['Electrical'] }), true);
    await check('Damage Types: tanpa edit Damage DITOLAK', setDoc(doc(as('whv'), 'masterLists', 'damageType'), { id: 'damageType', values: ['X'] }), false);

    // ---- 2.9.0: Purchase Request ----
    await env.withSecurityRulesDisabled(async ctx => {
        const db = ctx.firestore();
        await setDoc(doc(db, 'users', 'prE'), { uid: 'prE', email: 'pre@x.id', role: 'staff', status: 'active', access: { purchase: 'edit' } });
        await setDoc(doc(db, 'users', 'prV'), { uid: 'prV', email: 'prv@x.id', role: 'staff', status: 'active', access: { purchase: 'view' } });
        await setDoc(doc(db, 'users', 'prO'), { uid: 'prO', email: 'pro@x.id', role: 'staff', status: 'active', access: { purchase: 'own' } });
        await setDoc(doc(db, 'purchaseRequests', 'pr_mine'), { id: 'pr_mine', title: 'x', status: 'draft', createdByUid: 'prO' });
        await setDoc(doc(db, 'purchaseRequests', 'pr_sub'), { id: 'pr_sub', title: 'x', status: 'submitted', createdByUid: 'prO' });
        await setDoc(doc(db, 'purchaseRequests', 'pr_other'), { id: 'pr_other', title: 'x', status: 'draft', createdByUid: 'prE' });
    });
    const PE = as('prE'), PV = as('prV'), PO_ = as('prO');
    await check('PR: editor membaca semua', getDocs(collection(PE, 'purchaseRequests')), true);
    await check('PR: editor membuat permintaan', setDoc(doc(PE, 'purchaseRequests', 'pr_new'), { id: 'pr_new', title: 'y', status: 'submitted', createdByUid: 'prE' }), true);
    await check('PR: editor menyetujui / mencatat PO', setDoc(doc(PE, 'purchaseRequests', 'pr_sub'), { status: 'po', poNo: 'PO-1' }, { merge: true }), true);
    await check('PR: editor atas nama orang lain DITOLAK', setDoc(doc(PE, 'purchaseRequests', 'pr_fake'), { id: 'pr_fake', title: 'y', status: 'draft', createdByUid: 'prO' }), false);
    await check('PR: pembaca membaca semua', getDocs(collection(PV, 'purchaseRequests')), true);
    await check('PR: pembaca TIDAK bisa menulis', setDoc(doc(PV, 'purchaseRequests', 'pr_v'), { id: 'pr_v', title: 'y', status: 'draft', createdByUid: 'prV' }), false);
    await check('PR: tanpa akses TIDAK bisa membaca', getDocs(collection(N, 'purchaseRequests')), false);
    await check('PR own: query miliknya sendiri', getDocs(query(collection(PO_, 'purchaseRequests'), where('createdByUid', '==', 'prO'))), true);
    await check('PR own: query semua DITOLAK', getDocs(collection(PO_, 'purchaseRequests')), false);
    await check('PR own: membaca milik orang lain DITOLAK', getDoc(doc(PO_, 'purchaseRequests', 'pr_other')), false);
    await check('PR own: membuat draf sendiri', setDoc(doc(PO_, 'purchaseRequests', 'pr_o1'), { id: 'pr_o1', title: 'y', status: 'draft', createdByUid: 'prO' }), true);
    await check('PR own: membuat langsung Approved DITOLAK', setDoc(doc(PO_, 'purchaseRequests', 'pr_o2'), { id: 'pr_o2', title: 'y', status: 'approved', createdByUid: 'prO' }), false);
    await check('PR own: mengajukan drafnya', setDoc(doc(PO_, 'purchaseRequests', 'pr_mine'), { status: 'submitted' }, { merge: true }), true);
    await check('PR own: mengubah yang sudah diajukan DITOLAK', setDoc(doc(PO_, 'purchaseRequests', 'pr_mine'), { title: 'ubah' }, { merge: true }), false);
    await check('PR own: menghapus yang sudah diajukan DITOLAK', deleteDoc(doc(PO_, 'purchaseRequests', 'pr_mine')), false);
    await check('PR own: menghapus drafnya sendiri', deleteDoc(doc(PO_, 'purchaseRequests', 'pr_o1')), true);
    await check('PR own: mengubah draf orang lain DITOLAK', setDoc(doc(PO_, 'purchaseRequests', 'pr_other'), { title: 'ubah' }, { merge: true }), false);
    await check('PR foto: editor menyimpan', setDoc(doc(PE, 'purchasePhotos', 'pr_new__p1'), { id: 'pr_new__p1', ownerId: 'pr_new', photo: 'data:image/jpeg;base64,AA' }), true);
    await check('PR foto: own menyimpan lampiran', setDoc(doc(PO_, 'purchasePhotos', 'pr_mine__p1'), { id: 'pr_mine__p1', ownerId: 'pr_mine', photo: 'data:image/jpeg;base64,AA' }), true);
    await check('PR foto: pembaca membuka', getDoc(doc(PV, 'purchasePhotos', 'pr_new__p1')), true);
    await check('PR foto: pembaca TIDAK bisa menulis', setDoc(doc(PV, 'purchasePhotos', 'pr_v__p1'), { id: 'pr_v__p1', ownerId: 'pr_v', photo: 'x' }), false);
    await check('PR foto: tanpa akses TIDAK bisa membaca', getDoc(doc(N, 'purchasePhotos', 'pr_new__p1')), false);
    await check('PR foto: terlalu besar DITOLAK', setDoc(doc(PE, 'purchasePhotos', 'pr_new__p2'), { id: 'pr_new__p2', ownerId: 'pr_new', photo: 'x'.repeat(400001) }), false);
    await check('PR: pemegang own boleh menulis riwayat', setDoc(doc(PO_, 'history', 'h_pr'),
        { id: 'h_pr', timestamp: Date.now(), action: 'create', unitId: '', unitName: '[Purchase] x', field: '', before: '', after: '', actorUid: 'prO', actorEmail: 'prO@x.id', actorName: 'p', actorRole: 'staff', unitGroup: 'shared' }), true);

    await env.cleanup();
    let pass = 0;
    T.forEach(x => { if (x.pass) pass++; else console.log('FAIL', x.name, '\n   ', x.err || ''); });
    console.log(`${pass}/${T.length} lulus`);
    process.exit(pass === T.length ? 0 : 1);
})().catch(e => { console.error(e); process.exit(1); });
