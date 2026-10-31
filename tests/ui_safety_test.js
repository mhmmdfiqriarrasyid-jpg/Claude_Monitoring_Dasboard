// v149: (1) versi baru tidak memuat ulang halaman di tengah form — muncul
// banner, dan halaman dimuat ulang sendiri setelah form ditutup; (2) data yang
// masuk saat sel Unit Database sedang diketik tidak menyimpan setengah ketikan;
// (3) hapus unit minta konfirmasi (lebih dari 10 unit: ketik jumlahnya),
// History menyimpan ringkasan unit, dan jadwal cek tidak macet "overdue" oleh
// unit yang sudah dihapus.
const { launch, BASE_URL } = require('./_env');

(async () => {
    const b = await launch();
    const page = await b.newPage({ viewport: { width: 1440, height: 900 } });
    const errors = [];
    page.on('pageerror', e => { if (!/Chart is not defined/.test(e.message)) errors.push(e.message); });
    await page.goto(BASE_URL + '/index.html', { waitUntil: 'load' });
    await page.waitForTimeout(1000);

    await page.addScriptTag({ content: `(async () => { try {
        const T = []; const t = (n, g, w) => T.push({ n, g, w, pass: JSON.stringify(g) === JSON.stringify(w) });
        const tick = ms => new Promise(r => setTimeout(r, ms || 20));
        const toasts = [], history = [], saved = [];
        window.showToast = m => toasts.push(String(m));
        let reloads = 0; window.appReload = () => { reloads++; };
        let confirmAnswer = true, promptAnswer = null;
        window.confirm = () => confirmAnswer;
        window.prompt = () => promptAnswer;
        const realLog = logEvent; window.logEvent = e => { history.push(e); };
        window.cloud = { isReady: true, cacheMode: 'persistent-multitab', addHistoryEvents: () => Promise.resolve(),
            saveUnits: l => { saved.push(l.map(u => u.id + ':' + u.model)); return Promise.resolve(); },
            updateUnitsFields: l => { saved.push(l.map(u => u.id + ':' + u.model)); return Promise.resolve(); },
            deleteUnits: () => Promise.resolve() };
        currentUser = { uid: 'o', email: 'o@x.id' }; currentUserDoc = { role: 'owner', status: 'active' };
        hideAuthGates(); applyRoleGating();
        const TR = (i, x) => ({ id: 'u' + i, name: 'TR' + i, sn: 'S' + i, status: 'Good', model: '6110B', site: 'PT. GPA', ...x });
        globalData = Array.from({ length: 14 }, (_, i) => TR(i + 1));
        saveToStorage(globalData);

        // =============== (1) versi baru ===============
        handleControllerChange();
        t('tanpa form terbuka: langsung dimuat ulang', reloads, 1);
        _swRefreshing = false; reloads = 0;
        showAddForm();
        handleControllerChange();
        t('form terbuka: TIDAK dimuat ulang, banner muncul', [reloads, !!document.getElementById('updateBanner')], [0, true]);
        await tick(2300);
        t('selama form masih terbuka tetap tidak dimuat ulang', reloads, 0);
        closeModal();
        await tick(2300);
        t('setelah form ditutup: dimuat ulang sendiri', reloads, 1);
        _swRefreshing = false; reloads = 0;
        document.getElementById('updateBanner').remove();
        if (_updateWatch) { clearInterval(_updateWatch); _updateWatch = null; }
        window.cloud.cacheMode = 'memory';
        showAddForm();
        handleControllerChange();
        closeModal();
        await tick(2300);
        t('tanpa cache permanen: menunggu tombol Reload (antrean offline tidak hilang)', reloads, 0);
        showAddForm();
        confirmAnswer = false; reloadForUpdate();
        t('tombol Reload saat form terbuka: minta konfirmasi, batal = tidak dimuat ulang', reloads, 0);
        confirmAnswer = true; reloadForUpdate();
        t('dikonfirmasi: dimuat ulang', reloads, 1);
        closeModal();
        if (_updateWatch) { clearInterval(_updateWatch); _updateWatch = null; }
        window.cloud.cacheMode = 'persistent-multitab';
        document.getElementById('updateBanner')?.remove();

        // =============== (2) sel Unit Database vs snapshot ===============
        goNav('editUnits', 'tractor');
        const cell = () => document.querySelector('#editBody [data-id="u1"][data-field="model"]');
        cell().focus();
        cell().textContent = '61';
        saved.length = 0; toasts.length = 0;
        applyCloudUnitsSnapshot(globalData.map(u => ({ ...u })));
        t('snapshot saat mengetik: tabel tidak digambar ulang, sel tetap fokus', [document.activeElement === cell(), cell().textContent], [true, '61']);
        t('dan setengah ketikan tidak disimpan', [globalData[0].model, saved.length, toasts.filter(x => /updated/.test(x)).length], ['6110B', 0, 0]);
        applyCloudInspectionsSnapshot([], []);
        t('snapshot inspeksi saat mengetik: juga tidak menyimpan setengah ketikan', [globalData[0].model, saved.length], ['6110B', 0]);
        cell().textContent = '6120B';
        cell().blur();
        await tick(50);
        t('selesai mengetik: nilai lengkap yang disimpan', [globalData[0].model, saved.flat().includes('u1:6120B')], ['6120B', true]);
        t('tabel digambar ulang setelah sel ditinggalkan', document.querySelector('#editBody [data-id="u1"][data-field="model"]').textContent, '6120B');

        // =============== (3) hapus unit ===============
        confirmAnswer = false;
        deleteUnit('u1');
        t('hapus satu unit: minta konfirmasi, batal = tetap ada', globalData.some(u => u.id === 'u1'), true);
        confirmAnswer = true; history.length = 0;
        globalData[0] = { ...globalData[0], implement: 'Bed Ripper — Gessner', gpsLicense: 'SF-RTK', gpsLicenseEndDate: '2027-01-01' };
        deleteUnit('u1');
        t('dikonfirmasi: dihapus', globalData.some(u => u.id === 'u1'), false);
        t('History menyimpan ringkasan unit, bukan hanya SN', history.find(h => h.action === 'delete').before,
          'SN S1 · 6120B · site PT. GPA · implement Bed Ripper — Gessner · status Good · SF-RTK until 2027-01-01');
        selectedUnitIds = new Set(['u2', 'u3']);
        confirmAnswer = false;
        deleteSelected();
        t('hapus terpilih: batal = tetap ada', globalData.filter(u => ['u2', 'u3'].includes(u.id)).length, 2);
        selectedUnitIds = new Set(globalData.slice(0, 12).map(u => u.id));
        confirmAnswer = true; promptAnswer = '11';
        deleteSelected();
        t('lebih dari 10 unit: jumlah yang diketik salah = tidak dihapus', globalData.length, 13);
        promptAnswer = '12';
        deleteSelected();
        t('jumlah yang diketik benar: dihapus', globalData.length, 1);

        // jadwal cek dengan unit yang sudah dihapus
        globalData = [{ id: 'h1', name: 'EX1', sn: 'H1', unitGroup: 'heavy', status: 'Good' }];
        inspections = [{ id: 'r1', unitId: 'h1', date: '2026-10-02', approval: 'pending' }];
        const plan = { id: 'p1', date: '2026-10-01', unitIds: ['h1', 'gone'] };
        t('unit yang sudah dihapus tidak menahan progres jadwal', planProgress(plan), { done: 1, total: 1 });

        window.logEvent = realLog;
        window.__T = T;
    } catch (e) { window.__T = [{ n: 'EXCEPTION ' + e.message + ' ' + e.stack, pass: false }]; } })();` });

    await page.waitForFunction(() => window.__T, null, { timeout: 90000 });
    const T = await page.evaluate(() => window.__T);
    let pass = 0;
    T.forEach(x => { if (x.pass) pass++; else console.log('FAIL', x.n, '\n  dapat :', JSON.stringify(x.g), '\n  harap :', JSON.stringify(x.w)); });
    if (errors.length) console.log('PAGE ERRORS:', errors);
    console.log(`${pass}/${T.length} lulus`);
    await b.close();
    process.exit(pass === T.length && !errors.length ? 0 : 1);
})();
