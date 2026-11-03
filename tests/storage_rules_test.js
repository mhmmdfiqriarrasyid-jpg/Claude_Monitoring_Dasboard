// 2.13.0 — storage.rules (Learning Center di Cloud Storage), dijalankan di
// emulator Storage + Firestore:
//   npx firebase emulators:exec --only firestore,storage --project demo-ot "node tests/storage_rules_test.js"
const fs = require('fs');
const path = require('path');
const { initializeTestEnvironment, assertSucceeds, assertFails } = require('@firebase/rules-unit-testing');
const { ref, uploadBytes, getBytes, deleteObject } = require('firebase/storage');
const { doc, setDoc } = require('firebase/firestore');

(async () => {
    const env = await initializeTestEnvironment({
        projectId: 'demo-ot',
        firestore: { rules: fs.readFileSync(path.join(__dirname, '..', 'firestore.rules'), 'utf8'), host: '127.0.0.1', port: 8181 },
        storage: { rules: fs.readFileSync(path.join(__dirname, '..', 'storage.rules'), 'utf8'), host: '127.0.0.1', port: 9199 }
    });
    await env.withSecurityRulesDisabled(async c => {
        const db = c.firestore();
        const u = (uid, role, access, status) => setDoc(doc(db, 'users', uid), { uid, role, status: status || 'active', access: access || {} });
        await u('own', 'owner');
        await u('ed', 'staff', { learning: 'edit' });
        await u('rd', 'staff', {});
        await u('team', 'team');
        await u('teamNo', 'team', { learning: 'view' });
        await u('off', 'staff', { learning: 'edit' }, 'pending');
        await u('whE', 'staff', { warehouse: 'edit' });
        await u('whV', 'staff', { warehouse: 'view' });
        await u('dmgE', 'staff', { damage: 'edit' });
        await u('dmgScoped', 'staff', { damage: 'edit', unitScope: 'heavy' });
        await u('tlOwn', 'staff', { teamLog: 'own' });
        await u('tlE', 'staff', { teamLog: 'edit' });
        await setDoc(doc(db, 'workLogs', 'wl_mine'), { id: 'wl_mine', createdByUid: 'tlOwn' });
        await setDoc(doc(db, 'workLogs', 'wl_other'), { id: 'wl_other', createdByUid: 'tlE' });
    });
    const st = uid => env.authenticatedContext(uid).storage();
    const anon = env.unauthenticatedContext().storage();
    const pdf = new Uint8Array([37, 80, 68, 70, 45, 49]);
    const T = [];
    const check = async (name, p, ok) => {
        try { await (ok ? assertSucceeds(p) : assertFails(p)); T.push({ name, pass: true }); }
        catch (e) { T.push({ name, pass: false, err: e.message }); }
    };
    const put = (s, p, type, bytes) => uploadBytes(ref(s, p), bytes || pdf, { contentType: type });

    await check('owner mengunggah PDF', put(st('own'), 'learning/f1/a.pdf', 'application/pdf'), true);
    await check('editor Learning mengunggah video', put(st('ed'), 'learning/f2/v.mp4', 'video/mp4'), true);
    await check('editor Learning mengunggah EPUB', put(st('ed'), 'learning/f3/b.epub', 'application/epub+zip'), true);
    await check('role team (bawaan edit) mengunggah', put(st('team'), 'learning/f4/c.pdf', 'application/pdf'), true);
    await check('role team dengan learning=view DITOLAK', put(st('teamNo'), 'learning/f5/c.pdf', 'application/pdf'), false);
    await check('tanpa akses edit DITOLAK', put(st('rd'), 'learning/f6/c.pdf', 'application/pdf'), false);
    await check('akun belum aktif DITOLAK', put(st('off'), 'learning/f7/c.pdf', 'application/pdf'), false);
    await check('tanpa login DITOLAK', put(anon, 'learning/f8/c.pdf', 'application/pdf'), false);
    await check('tipe file lain (HTML) DITOLAK', put(st('own'), 'learning/f9/x.html', 'text/html'), false);
    await check('di luar folder learning DITOLAK', put(st('own'), 'other/x.pdf', 'application/pdf'), false);
    await check('semua akun aktif bisa membaca', getBytes(ref(st('rd'), 'learning/f1/a.pdf')), true);
    await check('akun belum aktif TIDAK bisa membaca', getBytes(ref(st('off'), 'learning/f1/a.pdf')), false);
    await check('tanpa login TIDAK bisa membaca', getBytes(ref(anon, 'learning/f1/a.pdf')), false);
    await check('pembaca TIDAK bisa menghapus', deleteObject(ref(st('rd'), 'learning/f1/a.pdf')), false);
    await check('editor menghapus', deleteObject(ref(st('ed'), 'learning/f1/a.pdf')), true);

    // ---- 2.17.0: foto di photos/<koleksi>/<record>/<file> ----
    const jpg = new Uint8Array([255, 216, 255, 224]);
    const img = (s, p) => uploadBytes(ref(s, p), jpg, { contentType: 'image/jpeg' });
    await check('foto gudang: editor warehouse mengunggah', img(st('whE'), 'photos/warehousePhotos/d1__p1/a'), true);
    await check('foto gudang: pembaca warehouse membuka', getBytes(ref(st('whV'), 'photos/warehousePhotos/d1__p1/a')), true);
    await check('foto gudang: pembaca TIDAK bisa mengunggah', img(st('whV'), 'photos/warehousePhotos/d1__p1/b'), false);
    await check('foto gudang: tanpa akses TIDAK bisa membuka', getBytes(ref(st('rd'), 'photos/warehousePhotos/d1__p1/a')), false);
    await check('foto: bukan gambar DITOLAK', uploadBytes(ref(st('whE'), 'photos/warehousePhotos/d1__p1/x'), pdf, { contentType: 'application/pdf' }), false);
    await check('foto kerusakan: editor damage mengunggah', img(st('dmgE'), 'photos/damagePhotos/dm1/a'), true);
    await check('foto kerusakan: akun terbatas satu kelompok TIDAK bisa mengunggah', img(st('dmgScoped'), 'photos/damagePhotos/dm1/b'), false);
    await check('foto laporan: editor tim mengunggah', img(st('tlE'), 'photos/workLogPhotos/wl_other/a'), true);
    await check('foto laporan: akun own mengunggah ke laporannya sendiri', img(st('tlOwn'), 'photos/workLogPhotos/wl_mine/a'), true);
    await check('foto laporan: akun own mengunggah ke laporan baru', img(st('tlOwn'), 'photos/workLogPhotos/wl_new/a'), true);
    await check('foto laporan: akun own TIDAK bisa mengunggah ke laporan orang lain', img(st('tlOwn'), 'photos/workLogPhotos/wl_other/b'), false);
    await check('foto laporan: akun own membaca laporannya sendiri', getBytes(ref(st('tlOwn'), 'photos/workLogPhotos/wl_mine/a')), true);
    await check('foto laporan: akun own TIDAK bisa membaca laporan orang lain', getBytes(ref(st('tlOwn'), 'photos/workLogPhotos/wl_other/a')), false);
    await check('arsip history: owner bisa membaca', getBytes(ref(st('own'), 'archives/history/2024-01.json')).catch(e => { if (/not.?found/i.test(e.code || e.message)) return; throw e; }), true);
    await check('arsip history: bukan owner TIDAK bisa membaca', getBytes(ref(st('ed'), 'archives/history/2024-01.json')), false);
    await check('arsip history: owner pun TIDAK bisa menulis (server saja)', uploadBytes(ref(st('own'), 'archives/history/x.json'), pdf, { contentType: 'application/json' }), false);
    await check('foto: folder koleksi lain tertutup', img(st('own'), 'photos/lainnya/x/a'), false);

    await env.cleanup();
    let pass = 0;
    T.forEach(x => { if (x.pass) pass++; else console.log('FAIL', x.name, '\n   ', x.err || ''); });
    console.log(`${pass}/${T.length} lulus`);
    process.exit(pass === T.length ? 0 : 1);
})();
