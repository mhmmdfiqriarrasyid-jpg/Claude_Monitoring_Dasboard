// 2.12.0 — Learning Center (PDF): upload dipecah ke learningFiles, penampil
// PDF.js, link Google Drive, kategori, akses edit, dan tanpa pelanggaran CSP.
const path = require('path');
const { launch, BASE_URL } = require('./_env');

(async () => {
    const b = await launch();
    const ctx = await b.newContext({ viewport: { width: 1280, height: 900 } });
    const page = await ctx.newPage();
    const errors = [];
    page.on('pageerror', e => { if (!/Chart is not defined/.test(e.message)) errors.push(e.message); });
    page.on('dialog', d => d.accept());
    await page.addInitScript(() => { window.__csp = []; document.addEventListener('securitypolicyviolation', e => window.__csp.push(e.violatedDirective + ' ' + e.blockedURI)); });
    await page.goto(BASE_URL + '/index.html', { waitUntil: 'load' });
    await page.waitForTimeout(1000);
    await page.evaluate(() => {
        window.__T = null; window.__chunks = {}; window.__items = []; window.__toasts = [];
        window.showToast = m => __toasts.push(String(m));
        window.cloud = { isReady: true, addHistoryEvents: () => Promise.resolve(),
            saveLearningChunk: (f, i, d) => { __chunks[f + '__' + i] = d; return Promise.resolve(); },
            getLearningChunk: (f, i) => Promise.resolve(__chunks[f + '__' + i] ?? null),
            deleteLearningChunks: (f, c) => { for (let i = 0; i < c; i++) delete __chunks[f + '__' + i]; return Promise.resolve(); },
            saveLearningItem: r => { __items.push(JSON.parse(JSON.stringify(r))); return Promise.resolve(); },
            deleteLearningItem: id => { __items.push({ deleted: id }); return Promise.resolve(); } };
        currentUser = { uid: 'o', email: 'o@x.id' };
        currentUserDoc = { uid: 'o', role: 'owner', status: 'active', email: 'o@x.id' };
        hideAuthGates(); applyRoleGating(); navigateTo('learning');
    });
    const T = [];
    const t = (n, g, w) => T.push({ n, g, w, pass: JSON.stringify(g) === JSON.stringify(w) });
    t('halaman Learning Center tampil', await page.evaluate(() => document.getElementById('viewLearning').style.display), 'block');
    t('kategori bawaan', await page.evaluate(() => [...document.querySelectorAll('#lcCats .lc-cat')].map(x => x.textContent.replace(/\s*\d+$/, '').trim())),
      ['All', 'Precision Agriculture', 'Heavy Equipment', 'Safety (K3)', 'Dashboard How-to', 'General']);

    // upload
    await page.evaluate(() => { showAddLearningForm(); saveLearningItem({ preventDefault() {} }); });
    t('judul wajib', await page.evaluate(() => __toasts.pop()), 'Give the material a title');
    await page.evaluate(() => { document.getElementById('lcTitle').value = 'StarFire 7000 — calibration'; saveLearningItem({ preventDefault() {} }); });
    t('file wajib untuk upload', await page.evaluate(() => __toasts.pop()), 'Choose a PDF file');
    await page.setInputFiles('#lcFile', path.join(__dirname, 'fixtures', 'learning_sample.pdf'));
    await page.evaluate(() => saveLearningItem({ preventDefault() {} }));
    await page.waitForTimeout(300);
    const up = await page.evaluate(() => ({ item: __items[0], chunks: Object.keys(__chunks).length, cards: document.querySelectorAll('.lc-item').length }));
    t('tersimpan: sumber upload, file di firestore, potongan tertulis', [up.item.source, up.item.file.storage, up.item.file.chunks, up.chunks], ['upload', 'firestore', 1, 1]);
    t('kartu tampil', up.cards, 1);

    // buka: PDF.js menggambar halaman (dari potongan, bukan cache)
    await page.evaluate(() => { _lcBytesCache.clear(); openLearningItem(learningItems[0].id); });
    await page.waitForFunction(() => document.querySelectorAll('#lcViewerBody canvas.lc-page').length >= 3, null, { timeout: 20000 }).catch(() => {});
    const pages = await page.evaluate(() => document.querySelectorAll('#lcViewerBody canvas.lc-page').length);
    t('PDF dibuka dari potongan dan digambar per halaman', pages >= 3, true);
    await page.evaluate(() => lcZoom(1)); await page.waitForTimeout(300);
    t('zoom', await page.evaluate(() => /%$/.test(document.getElementById('lcZoomLabel').textContent)), true);
    await page.evaluate(() => closeLearningViewer());

    // link Google Drive dibuka di dalam halaman
    await page.evaluate(() => {
        showAddLearningForm();
        document.getElementById('lcTitle').value = 'G5 manual';
        document.querySelector('input[name="lcSource"][value="link"]').checked = true; syncLearningSource();
        document.getElementById('lcUrl').value = 'http://bukan-https';
    });
    await page.evaluate(() => saveLearningItem({ preventDefault() {} }));
    t('link harus https', await page.evaluate(() => __toasts.pop()), 'Paste a link that starts with https://');
    await page.evaluate(() => { document.getElementById('lcUrl').value = 'https://drive.google.com/file/d/1AbCdEfGhIjKlMnOpQr/view?usp=sharing'; return saveLearningItem({ preventDefault() {} }); });
    await page.evaluate(() => openLearningItem(learningItems.find(i => i.source === 'link').id));
    t('Google Drive: pratinjau di dalam halaman', await page.evaluate(() => document.querySelector('#lcViewerBody iframe')?.getAttribute('src')),
      'https://drive.google.com/file/d/1AbCdEfGhIjKlMnOpQr/preview');
    await page.evaluate(() => closeLearningViewer());

    // filter & cari
    t('filter kategori', await page.evaluate(() => { _lcCategory = 'Safety (K3)'; renderLearningView(); const n = document.querySelectorAll('.lc-item').length; _lcCategory = ''; renderLearningView(); return n; }), 0);
    t('cari judul', await page.evaluate(() => { document.getElementById('lcSearch').value = 'starfire'; renderLearningView(); const n = document.querySelectorAll('.lc-item').length; document.getElementById('lcSearch').value = ''; renderLearningView(); return n; }), 1);

    // hapus: potongan ikut terhapus
    await page.evaluate(() => deleteLearningItem(learningItems.find(i => i.source === 'upload').id));
    await page.waitForTimeout(200);
    t('hapus: potongan file ikut terhapus', await page.evaluate(() => Object.keys(__chunks).length), 0);

    // akses: pembaca tanpa tombol tambah/edit
    await page.evaluate(() => { currentUserDoc = { uid: 'v', role: 'staff', status: 'active', access: {} }; applyRoleGating(); renderLearningView(); });
    t('tanpa akses edit: tombol Add disembunyikan, tanpa Edit/Delete', await page.evaluate(() => [
        getComputedStyle(document.querySelector('#viewLearning .page-head__actions .btn')).display, document.querySelectorAll('.lc-item__acts').length]), ['none', 0]);
    t('tetap bisa membaca (menu tampil untuk semua akun aktif)', await page.evaluate(() => getComputedStyle(document.querySelector('.nav__link[data-view="learning"]')).display !== 'none'), true);
    t('tanpa pelanggaran CSP (PDF.js lokal)', await page.evaluate(() => window.__csp), []);
    await b.close();

    let pass = 0;
    for (const x of T) {
        if (x.pass) pass++;
        console.log(`${x.pass ? 'PASS' : 'FAIL'}  ${x.n}`);
        if (!x.pass) console.log(`        dapat=${JSON.stringify(x.g)} harap=${JSON.stringify(x.w)}`);
    }
    console.log(`\n${pass}/${T.length} lulus`);
    console.log('PAGE ERRORS:', errors.length ? errors : 'none');
    process.exit(pass === T.length && !errors.length ? 0 : 1);
})();
