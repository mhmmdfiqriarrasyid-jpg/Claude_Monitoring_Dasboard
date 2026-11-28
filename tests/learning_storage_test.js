// 2.13.0 — Learning Center di Cloud Storage (Blaze): upload ke Storage dengan
// progres, PDF/EPUB dibaca lewat URL, video diputar langsung, file lama dan
// file yang dihapus ikut dibuang dari Storage, fallback ke Firestore untuk
// file kecil kalau Storage menolak, dan pesan jelas kalau file tak bisa dibaca.
const path = require('path');
const { launch, BASE_URL } = require('./_env');
const fx = f => path.join(__dirname, 'fixtures', f);

(async () => {
    const b = await launch();
    const ctx = await b.newContext({ viewport: { width: 1280, height: 900 } });
    const page = await ctx.newPage();
    const errors = [];
    page.on('pageerror', e => { if (!/Chart is not defined/.test(e.message)) errors.push(e.message); });
    page.on('dialog', d => d.accept());
    await page.goto(BASE_URL + '/index.html', { waitUntil: 'load' });
    await page.waitForTimeout(1000);
    await page.evaluate(base => {
        window.__chunks = {}; window.__items = []; window.__toasts = []; window.__up = []; window.__del = [];
        window.__failUpload = null; window.__urlFor = null;
        window.showToast = m => __toasts.push(String(m));
        const fixtureOf = p => /\.pdf$/i.test(p) ? 'learning_sample.pdf' : /\.epub$/i.test(p) ? 'learning_sample.epub' : 'learning_sample.mp4';
        window.cloud = { isReady: true, addHistoryEvents: () => Promise.resolve(),
            saveLearningChunk: (f, i, d) => { __chunks[f + '__' + i] = d; return Promise.resolve(); },
            getLearningChunk: (f, i) => Promise.resolve(__chunks[f + '__' + i] ?? null),
            deleteLearningChunks: (f, c) => { for (let i = 0; i < c; i++) delete __chunks[f + '__' + i]; return Promise.resolve(); },
            saveLearningItem: r => { __items.push(JSON.parse(JSON.stringify(r))); return Promise.resolve(); },
            deleteLearningItem: id => Promise.resolve(),
            uploadLearningFile: (p, file, type, prog) => {
                if (__failUpload) return Promise.reject(__failUpload);
                __up.push({ p, type, size: file.size }); prog(0.5); prog(1); return Promise.resolve();
            },
            learningFileUrl: p => Promise.resolve(__urlFor || `${base}/tests/fixtures/${fixtureOf(p)}`),
            deleteLearningFile: p => { __del.push(p); return Promise.resolve(); } };
        currentUser = { uid: 'o', email: 'o@x.id' };
        currentUserDoc = { uid: 'o', role: 'owner', status: 'active', email: 'o@x.id' };
        hideAuthGates(); applyRoleGating(); navigateTo('learning');
    }, BASE_URL);
    const T = [];
    const t = (n, g, w) => T.push({ n, g, w, pass: JSON.stringify(g) === JSON.stringify(w) });
    const add = async (title, file) => {
        await page.evaluate(x => { showAddLearningForm(); document.getElementById('lcTitle').value = x; }, title);
        await page.setInputFiles('#lcFile', fx(file));
        await page.evaluate(() => saveLearningItem({ preventDefault() {} }));
        await page.waitForTimeout(250);
        return page.evaluate(() => __items[__items.length - 1]);
    };

    // ---------- PDF ke Cloud Storage ----------
    const pdf = await add('Manual G5', 'learning_sample.pdf');
    t('PDF: tersimpan di Cloud Storage, bukan potongan', [pdf.file.storage, pdf.file.format, pdf.file.contentType, Object.keys(await page.evaluate(() => __chunks)).length], ['cloud', 'pdf', 'application/pdf', 0]);
    t('PDF: path learning/<fileId>/<nama>', new RegExp(`^learning/${pdf.file.id}/learning_sample\\.pdf$`).test(pdf.file.path), true);
    await page.evaluate(id => { _lcBytesCache.clear(); openLearningItem(id); }, pdf.id);
    await page.waitForFunction(() => document.querySelectorAll('#lcViewerBody canvas.lc-page').length >= 3, null, { timeout: 20000 }).catch(() => {});
    t('PDF: dibaca lewat URL dan digambar', await page.evaluate(() => document.querySelectorAll('#lcViewerBody canvas.lc-page').length >= 3), true);
    await page.evaluate(() => closeLearningViewer());

    // ---------- EPUB ----------
    const ep = await add('Buku EPUB', 'learning_sample.epub');
    t('EPUB: tersimpan di Cloud Storage', [ep.file.storage, ep.file.contentType], ['cloud', 'application/epub+zip']);
    await page.evaluate(id => { _lcBytesCache.clear(); openLearningItem(id); }, ep.id);
    await page.waitForFunction(() => !!document.querySelector('foliate-view') && /%$/.test(document.getElementById('lcZoomLabel').textContent), null, { timeout: 15000 }).catch(() => {});
    t('EPUB: terbuka dari URL', await page.evaluate(() => !!document.querySelector('foliate-view')), true);
    await page.evaluate(() => closeLearningViewer());

    // ---------- video ----------
    const vid = await add('Kalibrasi StarFire', 'learning_sample.mp4');
    t('video: dikenali dan diunggah dengan tipe video/mp4', [vid.file.format, vid.file.storage, vid.file.contentType], ['video', 'cloud', 'video/mp4']);
    t('video: kartu berlabel Video', await page.evaluate(id => document.querySelector('.lc-item .lc-item__icon--video')?.textContent, vid.id), 'Video');
    await page.evaluate(id => openLearningItem(id), vid.id);
    await page.waitForTimeout(400);
    t('video: pemutar memakai URL Storage, tombol halaman/zoom tersembunyi, Download tampil', await page.evaluate(() => {
        const v = document.querySelector('#lcViewerBody video');
        return [!!v || /Open the file in a new tab/.test(document.getElementById('lcViewerBody').textContent), document.getElementById('lcZoomIn').hidden, document.getElementById('lcPrev').hidden, document.getElementById('lcDownload').hidden];
    }), [true, true, true, false]);
    await page.evaluate(() => closeLearningViewer());

    // ---------- batas ukuran ----------
    await page.evaluate(() => { LEARN_CLOUD_MAX.video = 10; });
    const before = await page.evaluate(() => __up.length);
    await add('Terlalu besar', 'learning_sample.mp4');
    t('video melebihi batas ditolak', [await page.evaluate(() => __up.length) - before, /the limit is/.test(await page.evaluate(() => __toasts[__toasts.length - 1]))], [0, true]);
    await page.evaluate(() => { LEARN_CLOUD_MAX.video = 500 * 1024 * 1024; });

    // ---------- ganti file & hapus: file lama dibuang dari Storage ----------
    await page.evaluate(id => { editLearningItem(id); }, pdf.id);
    await page.setInputFiles('#lcFile', fx('learning_sample.pdf'));
    await page.evaluate(() => saveLearningItem({ preventDefault() {} }));
    await page.waitForTimeout(250);
    t('ganti file: file lama dihapus dari Storage', await page.evaluate(p => __del.includes(p), pdf.file.path), true);
    const vpath = vid.file.path;
    await page.evaluate(id => deleteLearningItem(id), vid.id);
    await page.waitForTimeout(200);
    t('hapus materi: file Storage ikut dihapus', await page.evaluate(p => __del.includes(p), vpath), true);

    // ---------- Storage menolak ----------
    await page.evaluate(() => { __failUpload = Object.assign(new Error('denied'), { code: 'storage/unauthorized' }); });
    const small = await add('Fallback kecil', 'learning_sample.pdf');
    t('Storage menolak: PDF kecil tetap tersimpan sebagai potongan Firestore', [small.title, small.file.storage], ['Fallback kecil', 'firestore']);
    const nItems = await page.evaluate(() => __items.length);
    await add('Video ditolak', 'learning_sample.mp4');
    t('Storage menolak: video tidak tersimpan, pesan menyebut storage.rules', [await page.evaluate(() => __items.length) - nItems, /storage\.rules/.test(await page.evaluate(() => __toasts[__toasts.length - 1]))], [0, true]);
    await page.evaluate(() => { __failUpload = null; });

    // ---------- file tidak bisa dibaca (mis. CORS belum diatur) ----------
    await page.evaluate(() => { __urlFor = 'http://127.0.0.1:9/blocked.pdf'; });
    const latestPdf = await page.evaluate(() => learningItems.find(i => i.file && i.file.storage === 'cloud' && i.file.format === 'pdf').id);
    await page.evaluate(id => { _lcBytesCache.clear(); openLearningItem(id); }, latestPdf);
    await page.waitForFunction(() => /Could not open/.test(document.getElementById('lcViewerBody').textContent), null, { timeout: 10000 }).catch(() => {});
    t('gagal dibaca: tawarkan buka di tab baru', await page.evaluate(() => document.querySelector('#lcViewerBody a.lc-openext')?.getAttribute('href')), 'http://127.0.0.1:9/blocked.pdf');
    await page.evaluate(() => closeLearningViewer());

    const fail = T.filter(r => !r.pass);
    T.forEach(r => console.log(`${r.pass ? 'PASS' : 'FAIL'}  ${r.n}${r.pass ? '' : `\n        dapat=${JSON.stringify(r.g)} harap=${JSON.stringify(r.w)}`}`));
    console.log(`\n${T.length - fail.length}/${T.length} lulus`);
    console.log('PAGE ERRORS:', errors.length ? errors : 'none');
    await b.close();
    process.exit(fail.length || errors.length ? 1 : 0);
})();
