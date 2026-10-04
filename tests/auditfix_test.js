// Bug dari audit menyeluruh sesudah v119 (Kritis, Tinggi, Sedang).
// Setiap blok di bawah memakai skenario reproduksi pemeriksa audit.
const { launch, BASE_URL } = require('./_env');

(async () => {
    const b = await launch();
    const page = await b.newPage({ viewport: { width: 1440, height: 900 } });
    await page.clock.setFixedTime('2026-09-30T08:00:00');
    const errors = [];
    page.on('pageerror', e => { if (!/Chart is not defined/.test(e.message)) errors.push(e.message); });
    await page.goto(BASE_URL + '/index.html', { waitUntil: 'load' });
    await page.waitForTimeout(1000);

    await page.addScriptTag({ content: `(async () => { try {
        const T = []; const t = (n, g, w) => T.push({ n, g, w, pass: JSON.stringify(g) === JSON.stringify(w) });
        const tick = ms => new Promise(r => setTimeout(r, ms || 20));
        const toasts = [];
        window.showToast = m => { toasts.push(String(m)); };
        let answer = true;
        window.confirm = () => answer;
        const writes = [];
        const ok = () => Promise.resolve();
        const baseCloud = () => ({ isReady: true,
            saveUnits: u => { writes.push(...JSON.parse(JSON.stringify(u))); return ok(); },
            deleteUnits: ok, getAllUnits: () => Promise.resolve([]), addHistoryEvents: ok, subscribeUsers: () => () => {},
            saveDamage: ok, saveDamages: ok, deleteDamage: ok, saveLicense: ok, saveLicenses: ok });
        window.cloud = baseCloud();
        currentUser = { uid: 'uO', email: 'o@x.id' };
        currentUserDoc = { role: 'owner', status: 'active', email: 'o@x.id' };
        try { localStorage.setItem('tractorLicenseDatesApplied_v2', '1'); } catch (_) {}
        hideAuthGates(); applyRoleGating();
        const U = (id, x) => Object.assign({ id, name: 'GGTR' + id, model: 'JD 6110B', sn: 'SN' + id, site: 'PT. GPA', status: 'Good',
            display: 'Good', gps: 'Good', steering: 'Good', jdlink: 'Good', userCategory: '', remarks: '', downtimeHistory: [] }, x || {});

        // ===== K5: tanggal lisensi CSV unit =====
        const pd = processData([{ 'Nickname': 'A', 'Serial Number': 'S1', 'GPS License': 'sf-rtk',
            'GPS License Start Date': '01/10/2025', 'GPS License Expiration Date': '01/10/2026' }], { fallbackGroup: 'tractor' });
        t('K5: tanggal D/M/YYYY dibaca hari-dulu, jenis disamakan',
          [pd.valid[0].gpsLicense, pd.valid[0].gpsLicenseStartDate, pd.valid[0].gpsLicenseEndDate], ['SF-RTK', '2025-10-01', '2026-10-01']);
        globalData = [U('k5', { gpsLicense: 'SF-RTK', gpsLicenseEndDate: '01/10/2026' })];
        writes.length = 0;
        applyExpiredLicenseDowngrades();
        t('K5: tanggal habis non-ISO yang tersimpan tidak memicu penurunan otomatis', [writes.length, globalData[0].gpsLicense], [0, 'SF-RTK']);

        // ===== K7: seleksi tersembunyi =====
        navigateTo('editUnits');
        globalData = [U('a', { name: 'ALPHA' }), U('b', { name: 'BRAVO' }), U('c', { name: 'CHARLIE' })]; saveToStorage(globalData);
        renderEditTable();
        document.querySelectorAll('.unit-check').forEach(cb => { if (cb.dataset.id !== 'c') cb.checked = true; });
        updateSelectedCount();
        t('K7: dua unit tercentang', selectedUnitIds.size, 2);
        document.getElementById('editSearch').value = 'charlie';
        renderEditTable();
        t('K7: sesudah mencari, seleksi yang tak terlihat dibuang', [selectedUnitIds.size, document.getElementById('btnDeleteSelected').style.display], [0, 'none']);
        deleteSelected();
        t('K7: Delete selected tidak menghapus apa pun', globalData.length, 3);
        document.getElementById('editSearch').value = '';

        // ===== T6: form Edit Unit hanya menulis yang diubah =====
        globalData = [U('f', { userCategory: 'Kategori Lama', display: '', model: undefined })]; saveToStorage(globalData);
        editUnit('f');
        t('T6: field yang tidak ada tampil kosong, bukan "undefined"', document.getElementById('formModel').value, '');
        document.getElementById('formRemarks').value = 'ganti oli';
        writes.length = 0;
        const hist = []; const realLog = window.logEvent; window.logEvent = e => { hist.push(e.field); };
        saveUnit(new Event('submit'));
        window.logEvent = realLog;
        const fu = globalData[0];
        t('T6: hanya Remarks yang berubah', hist, ['remarks']);
        t('T6: kategori di luar daftar dan komponen kosong tidak disentuh', [fu.userCategory, fu.display, 'model' in fu && fu.model !== undefined], ['Kategori Lama', '', false]);
        // Perubahan dari perangkat lain selama form terbuka tidak ditimpa.
        globalData = [U('g')]; saveToStorage(globalData);
        editUnit('g');
        globalData[0] = { ...globalData[0], status: 'Breakdown', breakdownReason: 'Hidrolik bocor' };
        document.getElementById('formRemarks').value = 'catatan';
        saveUnit(new Event('submit'));
        t('T6: status yang diubah perangkat lain saat form terbuka tetap', [globalData[0].status, globalData[0].breakdownReason, globalData[0].remarks],
          ['Breakdown', 'Hidrolik bocor', 'catatan']);
        editUnit('g'); saveUnit(new Event('submit'));
        t('T6: simpan tanpa perubahan tidak menulis', toasts[toasts.length - 1], 'No changes');

        // ===== T7: tandai selesai kerusakan =====
        navigateTo('damage');
        globalData = [U('d', { status: 'Breakdown', breakdownReason: 'Mesin macet' })]; saveToStorage(globalData);
        globalDamages = [];
        const fillDamage = (setBd) => {
            showAddDamageForm();
            document.getElementById('dmgUnit').value = damageUnitLabel(globalData[0]);
            document.getElementById('dmgType').value = 'Software';
            renderDamageComponentOptions();
            document.getElementById('dmgDescription').value = 'uji';
            document.getElementById('dmgSetBreakdown').checked = setBd;
            saveDamage({ preventDefault() {} });
        };
        fillDamage(false);
        t('T7: catatan tanpa "set Breakdown" mencatat drove kosong', globalDamages[0].drove, '');
        resolveDamage(globalDamages[0].id);
        t('T7: menyelesaikannya tidak memulihkan breakdown milik orang lain', [globalData[0].status, globalData[0].breakdownReason], ['Breakdown', 'Mesin macet']);
        globalData = [U('d2')]; saveToStorage(globalData); globalDamages = [];
        fillDamage(true);
        t('T7: catatan dengan "set Breakdown" mencatat apa yang ia ubah', [globalDamages[0].drove, globalData[0].status], ['status', 'Breakdown']);
        resolveDamage(globalDamages[0].id);
        t('T7: dan menyelesaikannya memulihkan itu', globalData[0].status, 'Good');

        // ===== S6: urut kolom berisi angka =====
        navigateTo('editUnits');
        globalData = [U('y1', { yearReceived: 2021 }), U('y2', { yearReceived: '2019' })]; saveToStorage(globalData);
        let threw = false;
        try { sortEditTable('yearReceived'); renderEditTable(); } catch (e) { threw = e.message; }
        t('S6: mengurutkan kolom angka tidak error', threw, false);
        editSortState = { key: null, asc: true };

        // ===== S11: pembersih riwayat sesudah restore =====
        globalData = [U('h', { site: 'Estate A' })];
        const log = [
            { id: 'e1', action: 'update', unitId: 'h', unitName: 'GGTRh', field: 'site', before: 'Estate A', after: 'Estate B', timestamp: 100 },
            { id: 'e2', action: 'restore', unitName: '-', after: '1 unit dipulihkan', timestamp: 200 }];
        t('S11: perubahan sah sebelum restore tidak dianggap palsu', findPhantomUnitHistory(log).length, 0);
        t('S11: tanpa restore, kasus yang sama tetap terdeteksi', findPhantomUnitHistory(log.slice(0, 1)).length, 1);

        // ===== S12: cadangan sebelum data termuat =====
        URL.createObjectURL = bl => { window.__blob = bl; return 'blob:x'; };
        URL.revokeObjectURL = () => {};
        HTMLAnchorElement.prototype.click = function () {};
        const prevInit = cloudInitialized;
        cloudInitialized = true; _loadedParts.clear(); _markLoaded('units'); _markLoaded('damages');
        answer = false;
        await exportBackup();
        const ex = JSON.parse(await window.__blob.text());
        cloudInitialized = prevInit;
        t('S12: koleksi yang belum termuat tidak ditulis sebagai kosong', ['workLogs' in ex, 'damages' in ex], [false, true]);
        t('S12: dan berkasnya mengaku', ex.omitted.some(o => o.key === 'workLogs' && /not loaded yet/.test(o.why)), true);

        // ===== S13: nama perusahaan beda huruf =====
        teamMembers = [{ id: 'm1', name: 'A', company: 'PT. Global Papua Abadi', active: true },
                       { id: 'm2', name: 'B', company: 'pt. global papua abadi', active: true }];
        t('S13: satu kelompok per perusahaan', membersByCompany().length, 1);

        // ===== S5: pindah halaman tidak memuat ulang cache saat cloud hidup =====
        globalData = [U('live', { remarks: 'dari cloud' })];
        localStorage.setItem(STORAGE_KEY, JSON.stringify([U('live', { remarks: 'cache lama' })]));
        cloudInitialized = true;
        navigateTo('editUnits');
        t('S5: data di memori tetap, cache lama tidak dipakai', globalData[0].remarks, 'dari cloud');
        cloudInitialized = false;

        // ===== K6 / T8: login yang dipulihkan, sesi, offline =====
        const runAuth = async (profile, opts) => {
            const calls = { claim: 0, sync: 0 };
            let authCb = null;
            window.cloud = Object.assign(baseCloud(), {
                onAuthChange: cb => { authCb = cb; },
                getUserDoc: () => Promise.resolve(profile),
                isOwnerEmail: () => false,
                claimSession: () => { calls.claim++; return new Promise(() => {}); },   // tidak pernah dijawab (offline)
                subscribeUserDoc: (uid, cb) => { calls.userDocCb = cb; return () => {}; },
                subscribeUnits: () => { calls.sync++; return () => {}; },
                subscribeImplements: () => () => {},
                signOutUser: ok
            });
            _sessionTakenOver = false; _userDocUnsub = null; authInitialized = false; cloudInitialized = false;
            _cloudReadyFired = true; _localDataLoaded = true;
            _explicitSignIn = !!(opts && opts.explicit);
            try { localStorage.setItem(SESSION_START_KEY, String(Date.now())); } catch (_) {}
            setupAuth();
            try { await authCb({ uid: 'u9', email: 'x@x.id' }); } catch (e) { calls.err = e.message; }
            if (calls.err) T.push({ n: 'DBG auth threw ' + calls.err, pass: false });
            await tick(50);
            return calls;
        };
        const mine = mySessionId();
        let c = await runAuth({ uid: 'u9', role: 'staff', status: 'active', access: {}, activeSession: { id: mine } });
        t('K6: klaim sesi yang tak pernah dijawab tidak menahan sinkron', c.sync, 1);
        c = await runAuth({ uid: 'u9', role: 'staff', status: 'active', access: {}, activeSession: { id: 'perangkat_lain' } });
        const signedOut = () => currentUser === null && document.getElementById('authGate').style.display !== 'none';
        t('T8: login yang dipulihkan tidak merebut sesi perangkat lain', [c.claim, c.sync, signedOut()], [0, 0, true]);
        c = await runAuth({ uid: 'u9', role: 'staff', status: 'active', access: {}, activeSession: { id: 'revoked_1' } });
        t('T8: perangkat yang sudah dikeluarkan tidak masuk lagi', [c.claim, signedOut()], [0, true]);
        c = await runAuth({ uid: 'u9', role: 'staff', status: 'active', access: {}, activeSession: { id: 'perangkat_lain' } }, { explicit: true });
        t('T8: login yang baru diketik mengambil sesinya', [c.claim, c.sync], [1, 1]);
        c.userDocCb(null);
        await tick(50);
        t('akun dihapus saat terbuka: sesi diakhiri', signedOut(), true);

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
