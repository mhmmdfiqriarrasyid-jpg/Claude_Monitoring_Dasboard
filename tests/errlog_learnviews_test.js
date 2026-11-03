// 2.19.0 — Catatan error otomatis, jejak belajar Learning Center, dan arsip
// History (tampilan owner).
const { launch, BASE_URL } = require('./_env');
(async () => {
    const b = await launch();
    const page = await b.newPage({ viewport: { width: 1280, height: 900 } });
    const errors = [];
    page.on('pageerror', e => { if (!/Chart is not defined|sengaja/.test(e.message)) errors.push(e.message); });
    await page.goto(BASE_URL + '/index.html', { waitUntil: 'load' });
    await page.waitForTimeout(800);
    const R = await page.evaluate(async () => {
        const out = {}; const toasts = []; window.showToast = m => toasts.push(String(m));
        window.confirm = () => true;
        const logged = [], views = [];
        window.cloud = { isReady: true, addHistoryEvents: () => Promise.resolve(), subscribeUsers: () => () => {}, subscribeRoles: () => () => {},
            logClientError: r => { logged.push(r); return Promise.resolve(); },
            listClientErrors: () => Promise.resolve([{ message: 'TypeError: x is undefined', stack: 'at renderX (script.js:10)', at: Date.now(), email: 'a@x.id', version: 'v2.19.0', view: 'damage', count: 3 }]),
            clearClientErrors: () => { logged.push('cleared'); return Promise.resolve(); },
            recordLearningView: (id, who) => { views.push([id, who.email]); return Promise.resolve(); },
            listLearningViews: () => Promise.resolve([{ uid: 'uA', name: 'Andi', email: 'andi@x.id', firstAt: 1, lastAt: Date.now(), count: 2 }]),
            listHistoryArchives: () => Promise.resolve([{ name: '2024-02.json', path: 'archives/history/2024-02.json' }, { name: '2024-01.json', path: 'archives/history/2024-01.json' }]),
            historyArchiveUrl: () => Promise.resolve('https://example/x') };
        currentUser = { uid: 'o', email: 'o@x.id' };
        currentUserDoc = { uid: 'o', role: 'owner', status: 'active', email: 'o@x.id' };
        hideAuthGates(); applyRoleGating();

        // ---------- catatan error ----------
        reportClientError('error', 'boom in app', 'Error: boom\\n    at f (https://ot.techorchestrator.cloud/script.js:1:1)', 'https://ot.techorchestrator.cloud/script.js');
        reportClientError('error', 'boom in app', 'Error: boom\\n    at f (https://ot.techorchestrator.cloud/script.js:1:1)', 'https://ot.techorchestrator.cloud/script.js');
        reportClientError('error', 'from an extension', 'at chrome-extension://abc/x.js', 'chrome-extension://abc/x.js');
        reportClientError('error', 'third party', '', 'https://cdn.example.com/lib.js');
        reportClientError('promise', 'permission-denied: Missing or insufficient permissions.', '', '');
        out.logged = logged.map(r => [r.kind, r.message, r.uid, r.version]);
        await openErrorLog();
        out.errView = [document.querySelectorAll('#errorLogList .errlog-item').length, /×3/.test(document.getElementById('errorLogList').textContent), document.getElementById('errorLogClearBtn').hidden];
        await clearErrorLog();
        out.cleared = logged.includes('cleared');
        closeErrorLog();
        applyAccessVisibility();
        out.navOwner = document.getElementById('navErrorLog').style.display;

        // ---------- arsip history ----------
        await showHistoryArchives();
        out.archive = [[...document.querySelectorAll('#errorLogList .errlog-item')].map(li => li.textContent.trim()), document.getElementById('errorLogClearBtn').hidden];
        logged.length = 0; await clearErrorLog();
        out.noClearInArchive = logged.includes('cleared');
        closeErrorLog();

        // ---------- jejak belajar ----------
        allUsers = [{ uid: 'uA', email: 'andi@x.id', displayName: 'Andi', status: 'active' }, { uid: 'uB', email: 'budi@x.id', status: 'active' }, { uid: 'uP', email: 'p@x.id', status: 'pending' }];
        learningItems = [{ id: 'lc_1', title: 'SOP K3', category: 'General', source: 'link', url: 'https://example.com/sop' }];
        navigateTo('learning');
        out.seenBtn = !!document.querySelector('.lc-item__acts button[onclick*="openLearningViews"]');
        const _open = window.open; window.open = () => null;
        await openLearningItem('lc_1');
        window.open = _open;
        out.recorded = views;
        await openLearningViews('lc_1');
        const txt = document.getElementById('lcViewsList').textContent.replace(/\\s+/g, ' ');
        out.viewsModal = [/Opened · 1/.test(txt), /Andi/.test(txt), /2×/.test(txt), /Not opened yet · 1/.test(txt), /budi@x.id/.test(txt), /p@x.id/.test(txt)];
        closeLearningViews();
        currentUserDoc = { uid: 'v', role: 'staff', status: 'active', access: { learning: 'view' } };
        renderLearningView();
        out.viewerNoSeenBtn = !document.querySelector('.lc-item__acts button[onclick*="openLearningViews"]');
        applyRoleGating();
        out.navViewer = document.getElementById('navErrorLog').style.display;
        return out;
    });
    const T = [];
    const t = (n, g, w) => T.push({ n, g, w, pass: JSON.stringify(g) === JSON.stringify(w) });
    t('error app tercatat sekali; ekstensi & skrip luar diabaikan; promise ditolak ikut tercatat', R.logged,
      [['error', 'boom in app', 'o', 'v2.19.0'], ['promise', 'permission-denied: Missing or insufficient permissions.', 'o', 'v2.19.0']]);
    t('owner: Error log tampil dengan hitungan', R.errView, [1, true, false]);
    t('owner: Clear log', R.cleared, true);
    t('menu Admin → Error log untuk owner', R.navOwner, '');
    t('arsip history: daftar file per bulan, tanpa tombol Clear', R.archive, [['2024-02.json', '2024-01.json'], true]);
    t('Clear log tidak jalan di mode arsip', R.noClearInArchive, false);
    t('editor Learning: tombol Seen by', R.seenBtn, true);
    t('membuka materi mencatat jejak', R.recorded, [['lc_1', 'o@x.id']]);
    t('Seen by: yang sudah membuka + belum (akun aktif saja)', R.viewsModal, [true, true, true, true, true, false]);
    t('pembaca Learning: tanpa tombol Seen by', R.viewerNoSeenBtn, true);
    t('bukan owner: menu Error log tersembunyi', R.navViewer, 'none');
    let pass = 0;
    T.forEach(x => { if (x.pass) pass++; console.log(`${x.pass ? 'PASS' : 'FAIL'}  ${x.n}${x.pass ? '' : `\n        dapat=${JSON.stringify(x.g)} harap=${JSON.stringify(x.w)}`}`); });
    console.log(`\n${pass}/${T.length} lulus`);
    console.log('PAGE ERRORS:', errors.length ? errors : 'none');
    await b.close();
    process.exit(pass === T.length && !errors.length ? 0 : 1);
})();
