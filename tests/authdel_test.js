// 2.11.5 — Sesudah Remove user / Reject: dialog langkah menghapus akun
// login di Firebase Authentication (email + UID untuk disalin, tautan konsol).
const { launch, BASE_URL } = require('./_env');

(async () => {
    const b = await launch();
    const page = await b.newPage({ viewport: { width: 1280, height: 900 } });
    const errors = [];
    page.on('pageerror', e => { if (!/Chart is not defined/.test(e.message)) errors.push(e.message); });
    await page.goto(BASE_URL + '/index.html', { waitUntil: 'load' });
    await page.waitForTimeout(1000);
    await page.addScriptTag({ content: `(async () => { try {
        const T = []; const t = (n, g, w) => T.push({ n, g, w, pass: JSON.stringify(g) === JSON.stringify(w) });
        const tick = ms => new Promise(r => setTimeout(r, ms || 30));
        const asked = []; window.confirm = m => { asked.push(String(m)); return true; };
        const del = [];
        window.cloud = { isReady: true, projectId: 'ot-monitoring-tractor', addHistoryEvents: () => Promise.resolve(), subscribeUsers: () => () => {},
            deleteUserDoc: uid => { del.push(uid); return Promise.resolve(); } };
        currentUser = { uid: 'o', email: 'o@x.id' };
        currentUserDoc = { uid: 'o', role: 'owner', status: 'active', email: 'o@x.id' };
        hideAuthGates(); applyRoleGating();
        customRoles = []; _rolesLoaded = true;
        allUsers = [{ uid: 'o', email: 'o@x.id', role: 'owner', status: 'active' },
                    { uid: 'uidA', email: 'a@x.id', displayName: 'Andi', role: 'staff', status: 'active', access: {} },
                    { uid: 'uidP', email: 'p@x.id', displayName: 'Putri', role: 'viewer', status: 'pending' }];
        navigateTo('users'); renderUsersView();
        openAccessModal('uidA'); await tick();
        await removeUserFromAccess('uidA'); await tick();
        const open = () => document.getElementById('authDeleteModal').classList.contains('open');
        t('remove: dokumen user dihapus', del, ['uidA']);
        t('konfirmasi menyebut akun login ikut dihapus', /sign-in account is deleted too/.test(asked.pop() || ''), true);
        t('fungsi server belum ada: dialog langkah manual sebagai cadangan', open(), true);
        t('dialog hapus akun login terbuka, dialog Access tertutup', [open(), document.getElementById('accessModal').classList.contains('open')], [true, false]);
        t('email dan UID untuk disalin', [document.getElementById('authDeleteEmail').textContent, document.getElementById('authDeleteUid').textContent], ['a@x.id', 'uidA']);
        t('tautan ke Firebase Authentication project ini', document.getElementById('authDeleteOpen').getAttribute('href'),
          'https://console.firebase.google.com/project/ot-monitoring-tractor/authentication/users');
        t('tautan dibuka di tab baru, aman', [document.getElementById('authDeleteOpen').target, document.getElementById('authDeleteOpen').rel], ['_blank', 'noopener']);
        closeAuthDeleteModal();
        t('Done menutup dialog', open(), false);
        await rejectUser('uidP'); await tick();
        t('reject: dialog yang sama untuk pendaftar', [del.pop(), open(), document.getElementById('authDeleteEmail').textContent], ['uidP', true, 'p@x.id']);
        closeAuthDeleteModal();

        // ---- 2.15.0: server (Cloud Functions) menghapus akun login ----
        const calls = []; const toasts = []; window.showToast = m => toasts.push(String(m));
        window.cloud.callFunction = (n, d) => { calls.push([n, d && d.uid]); return Promise.resolve({ deleted: true }); };
        allUsers.push({ uid: 'uidB', email: 'b@x.id', displayName: 'Budi', role: 'staff', status: 'active', access: {} });
        openAccessModal('uidB'); await tick();
        await removeUserFromAccess('uidB'); await tick(80);
        t('server: fungsi deleteSignInAccount dipanggil untuk uid itu', calls, [['deleteSignInAccount', 'uidB']]);
        t('server: tanpa dialog manual, pesan sukses', [open(), toasts.pop()], [false, 'b@x.id removed — sign-in account deleted too']);
        window.cloud.callFunction = () => Promise.reject(Object.assign(new Error('x'), { code: 'functions/internal' }));
        allUsers.push({ uid: 'uidC', email: 'c@x.id', role: 'viewer', status: 'pending' });
        await rejectUser('uidC'); await tick(80);
        t('server gagal: dialog manual tetap muncul', [open(), document.getElementById('authDeleteEmail').textContent], [true, 'c@x.id']);
        closeAuthDeleteModal();
        window.__T = T;
    } catch (e) { window.__T = [{ n: 'CRASH ' + e.message + ' ' + (e.stack || '').split('\\n')[1], pass: false }]; } })()` });
    await page.waitForFunction(() => window.__T, null, { timeout: 30000 });
    const T = await page.evaluate(() => window.__T);
    let pass = 0;
    for (const x of T) {
        if (x.pass) pass++;
        console.log(`${x.pass ? 'PASS' : 'FAIL'}  ${x.n}`);
        if (!x.pass && 'g' in x) console.log(`        dapat=${JSON.stringify(x.g)} harap=${JSON.stringify(x.w)}`);
    }
    console.log(`\n${pass}/${T.length} lulus`);
    console.log('PAGE ERRORS:', errors.length ? errors : 'none');
    await b.close();
    process.exit(pass === T.length && !errors.length ? 0 : 1);
})();
