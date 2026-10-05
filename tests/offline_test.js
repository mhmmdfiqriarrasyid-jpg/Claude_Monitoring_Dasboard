// Laporan cek yang diisi tanpa sinyal: ditandai "Waiting to send" sampai
// server menerimanya, ada pemberitahuan berapa yang tertunda, form memberi
// tahu bahwa laporan akan disimpan di perangkat, dan keluar akun meminta
// konfirmasi bila masih ada yang tertunda. Juga: Unit Database tanpa tab
// kelompok, dan Root Plough / Root Rake sebagai alat kerja bawaan.
const { launch, BASE_URL } = require('./_env');

(async () => {
    const b = await launch();
    const page = await b.newPage({ viewport: { width: 1440, height: 900 } });
    await page.clock.setFixedTime('2026-10-04T09:00:00');
    const errors = [];
    page.on('pageerror', e => { if (!/Chart is not defined/.test(e.message)) errors.push(e.message); });
    await page.goto(BASE_URL + '/index.html', { waitUntil: 'load' });
    await page.waitForTimeout(1000);

    await page.addScriptTag({ content: `(async () => { try {
        const T = []; const t = (n, g, w) => T.push({ n, g, w, pass: JSON.stringify(g) === JSON.stringify(w) });
        const tick = ms => new Promise(r => setTimeout(r, ms || 20));
        const toasts = [];
        window.showToast = m => { toasts.push(String(m)); };
        let signedOut = 0, confirmAnswer = false, confirmText = '';
        window.confirm = m => { confirmText = String(m); return confirmAnswer; };
        window.cloud = { isReady: false, signOutUser: () => { signedOut++; return Promise.resolve(); } };
        let online = true;
        Object.defineProperty(navigator, 'onLine', { configurable: true, get: () => online });

        currentUser = { uid: 'tech', email: 'tech@x.id' };
        currentUserDoc = { role: 'khl', status: 'active', displayName: 'tech', access: { inspection: 'edit' } };
        hideAuthGates(); applyRoleGating();
        const H = id => ({ id, name: 'EX-' + id, sn: 'SN' + id, unitGroup: 'heavy', status: 'Good' });
        globalData = [H('h1'), H('h2')];
        saveToStorage(globalData);
        const R = (id, unitId) => ({ id, unitId, unitName: 'EX-' + unitId, date: '2026-10-03', approval: 'pending',
            results: { cameraAi: 'good' }, createdByUid: 'tech', createdBy: 'tech', submittedAt: 1 });

        navigateTo('inspection');
        switchInspectionTab('reports');
        const notice = () => { const el = document.getElementById('insSyncNotice'); return el.style.display === 'none' ? '' : el.textContent.trim(); };
        const chips = () => [...document.querySelectorAll('#insReportBody .ins-tag--queued')].length;

        // =============== online, semua terkirim ===============
        applyCloudInspectionsSnapshot([R('a', 'h1'), R('b', 'h2')], []);
        t('online tanpa antrean: tidak ada pemberitahuan', notice(), '');
        t('online tanpa antrean: tidak ada tanda tertunda', chips(), 0);

        // =============== offline ===============
        online = false; window.dispatchEvent(new Event('offline'));
        t('offline tanpa antrean: pemberitahuan masih bisa mengisi laporan', /No signal — you can still fill in check reports/.test(notice()), true);
        applyCloudInspectionsSnapshot([R('a', 'h1'), R('b', 'h2')], ['b']);
        t('laporan tertunda ditandai Waiting to send', chips(), 1);
        t('tanda ada di baris laporan yang benar', document.querySelector('#insReportBody .ins-tag--queued').closest('tr').textContent.includes('EX-h2'), true);
        t('pemberitahuan menyebut jumlahnya', /^1 report is waiting to be sent\\./.test(notice()), true);
        t('dan bahwa akan terkirim otomatis', /sent automatically when the signal returns/.test(notice()), true);
        applyCloudInspectionsSnapshot([R('a', 'h1'), R('b', 'h2')], ['a', 'b']);
        t('jamak', /^2 reports are waiting to be sent\\./.test(notice()), true);

        // form
        showInspectionForm('h1');
        t('form offline memberi tahu laporan disimpan di perangkat', document.getElementById('insModalOffline').style.display, '');
        closeInspectionModal(true);

        // keluar akun
        confirmAnswer = false;
        await handleSignOut();
        t('keluar akun dengan antrean: minta konfirmasi', [/2 inspection report\\(s\\) have not been sent yet/.test(confirmText), signedOut], [true, 0]);
        confirmAnswer = true; confirmText = '';
        await handleSignOut();
        t('dikonfirmasi: keluar', signedOut, 1);

        // =============== sinyal kembali ===============
        online = true; window.dispatchEvent(new Event('online'));
        t('online, masih mengirim', /Sending now/.test(notice()), true);
        t('form online: pemberitahuan offline tersembunyi', (showInspectionForm('h1'), document.getElementById('insModalOffline').style.display), 'none');
        closeInspectionModal(true);
        toasts.length = 0;
        applyCloudInspectionsSnapshot([R('a', 'h1'), R('b', 'h2')], []);
        t('antrean habis: toast semua terkirim', toasts.includes('All inspection reports have been sent'), true);
        t('antrean habis: pemberitahuan & tanda hilang', [notice(), chips()], ['', 0]);
        confirmText = '';
        await handleSignOut();
        t('tanpa antrean: keluar tanpa konfirmasi', [confirmText, signedOut], ['', 2]);
        toasts.length = 0;
        applyCloudInspectionsSnapshot([R('a', 'h1')], []);
        t('snapshot berikutnya tanpa antrean tidak mengulang toast', toasts.length, 0);

        // =============== Unit Database tanpa tab kelompok ===============
        currentUserDoc = { role: 'owner', status: 'active' }; currentUser = { uid: 'o', email: 'o@x.id' }; applyRoleGating();
        goNav('editUnits', 'heavy');
        t('tab kelompok Unit Database sudah tidak ada', document.querySelectorAll('#editGroupBar, .eu-tab').length, 0);
        t('judul menyebut Heavy Equipment', document.getElementById('editTableTitle').textContent, 'Unit Database — Heavy Equipment');
        goNav('editUnits', 'tractor');
        t('judul menyebut Agricultural Equipment', document.getElementById('editTableTitle').textContent, 'Unit Database — Agricultural Equipment');

        // =============== alat kerja bawaan ===============
        goNav('editUnits', 'heavy');
        showAddForm();
        const tools = [...document.querySelectorAll('#heavyWorkToolList option')].map(o => o.value);
        t('Root Plough & Root Rake jadi saran alat kerja', ['Root Plough', 'Root Rake'].every(x => tools.includes(x)), true);
        t('alat kerja lama tetap ada', ['Bucket', 'Ripper', 'Blade', 'Breaker'].every(x => tools.includes(x)), true);

        window.__T = T;
    } catch (e) { window.__T = [{ n: 'EXCEPTION ' + e.message + ' ' + e.stack, pass: false }]; } })();` });

    await page.waitForFunction(() => window.__T, null, { timeout: 60000 });
    const T = await page.evaluate(() => window.__T);
    let pass = 0;
    T.forEach(x => { if (x.pass) pass++; else console.log('FAIL', x.n, '\n  dapat :', JSON.stringify(x.g), '\n  harap :', JSON.stringify(x.w)); });
    if (errors.length) console.log('PAGE ERRORS:', errors);
    console.log(`${pass}/${T.length} lulus`);
    await b.close();
    process.exit(pass === T.length && !errors.length ? 0 : 1);
})();
