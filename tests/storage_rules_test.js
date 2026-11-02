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

    await env.cleanup();
    let pass = 0;
    T.forEach(x => { if (x.pass) pass++; else console.log('FAIL', x.name, '\n   ', x.err || ''); });
    console.log(`${pass}/${T.length} lulus`);
    process.exit(pass === T.length ? 0 : 1);
})();
