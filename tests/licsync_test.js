// "Sync ke Unit" — distribusi lisensi → lisensi unit.
//
// Sampai v114 tombol ini menulis ulang SETIAP unit yang pernah menerima
// distribusi (55 tulisan untuk membetulkan 3), di balik satu konfirmasi, dan
// tidak punya satu uji pun. Tiga akibatnya yang diuji di sini:
//   1. unit yang sudah sesuai ikut ditulis (dan tercatat di riwayat);
//   2. unit yang diperpanjang manual (masa berlaku lebih lama) DITIMPA mundur;
//   3. unit yang sudah turun otomatis ke SF-1 dikembalikan ke SF-RTK yang
//      sudah habis, lalu diturunkan lagi — dua tulisan sia-sia per unit.
// Sekarang: rencana berbasis selisih, pratinjau, dan hanya baris yang
// dicentang yang ditulis.
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
        const toasts = [];
        window.showToast = (m, k) => { toasts.push(String(m)); };
        window.confirm = () => true;
        const writes = [];
        window.cloud = { isReady: true,
            saveUnits: u => { writes.push(...JSON.parse(JSON.stringify(u))); return Promise.resolve(); },
            deleteUnits: () => Promise.resolve(), getAllUnits: () => Promise.resolve([]),
            addHistoryEvents: () => Promise.resolve(), subscribeUsers: () => () => {},
            saveLicense: () => Promise.resolve(), saveLicenses: () => Promise.resolve(), deleteLicense: () => Promise.resolve() };
        currentUser = { uid: 'uO', email: 'o@x.id' };
        currentUserDoc = { role: 'owner', status: 'active', email: 'o@x.id' };
        try { localStorage.setItem('tractorLicenseDatesApplied_v2', '1'); } catch (_) {}
        hideAuthGates(); applyRoleGating();
        navigateTo('licenseStock');

        const U = (id, x) => Object.assign({ id, name: 'GGTR' + id, sn: 'SN' + id, status: 'Good',
            display: 'Good', gps: 'Good', steering: 'Good', jdlink: 'Good', gpsLicense: '', licenseDisplay: '',
            gpsLicenseStartDate: '', gpsLicenseEndDate: '', displayLicenseStartDate: '', displayLicenseEndDate: '' }, x || {});
        let seq = 0;
        const OUT = (unitId, type, date, x) => Object.assign({ id: 'L' + (++seq), txnType: 'OUT', licenseType: type,
            qty: 1, unitId, unitName: 'GGTR' + unitId, sn: 'SN' + unitId, date, createdAt: seq, note: '' }, x || {});
        const seed = (units, recs) => {
            globalData = units; saveToStorage(globalData);
            globalLicenseStock = recs;
            writes.length = 0; toasts.length = 0;
        };
        const plan = () => licenseSyncPlan().rows.map(r => r.unit.id + ':' + r.kind + ':' + r.status);
        const openPreview = () => { syncDistributionsToUnits(); return document.getElementById('licSyncModal').classList.contains('open'); };
        const previewRows = () => [...document.querySelectorAll('#licSyncBody tr')].map(tr => ({
            checked: tr.querySelector('.lic-sync-check').checked, text: tr.textContent.replace(/\\s+/g, ' ').trim() }));

        // =============== 1. YANG SUDAH SESUAI TIDAK DISENTUH ===============
        seed([U('a', { gpsLicense: 'SF-RTK', gpsLicenseStartDate: '2026-09-01', gpsLicenseEndDate: '2027-09-01' }),
              U('b', { licenseDisplay: 'G5 Advance', displayLicenseStartDate: '2026-08-01', displayLicenseEndDate: '2027-08-01' }),
              U('c')],
             [OUT('a', 'SF-RTK', '2026-09-01'), OUT('b', 'G5 Advance', '2026-08-01'), OUT('c', 'SF-RTK', '2026-09-15')]);
        t('rencana: dua sesuai, satu perlu diperbarui', plan(), ['a:gps:sync', 'b:display:sync', 'c:gps:update']);
        renderLicenseStockTable();
        t('tombol Sync menunjukkan jumlah yang belum sesuai', [document.getElementById('licSyncCount').textContent,
            document.getElementById('licSyncCount').hidden], ['1', false]);
        t('pratinjau terbuka', openPreview(), true);
        t('pratinjau hanya memuat yang berbeda', previewRows().length, 1);
        t('ringkasannya menyebut yang tidak disentuh', /2 sudah sesuai dan tidak disentuh/.test(document.getElementById('licSyncSummary').textContent), true);
        applyLicSyncSelection();
        t('hanya SATU unit yang ditulis', writes.map(u => u.id), ['c']);
        t('unit itu mendapat lisensinya', [globalData[2].gpsLicense, globalData[2].gpsLicenseStartDate, globalData[2].gpsLicenseEndDate],
          ['SF-RTK', '2026-09-15', '2027-09-15']);
        t('sesudahnya tombol tanpa angka', document.getElementById('licSyncCount').hidden, true);
        writes.length = 0; toasts.length = 0;
        t('sync kedua kali: tidak ada pratinjau', openPreview(), false);
        t('dan tidak ada tulisan', writes.length, 0);
        t('dan pesannya bilang semua sudah sesuai', /Semua 3 lisensi unit sudah sesuai/.test(toasts.join('|')), true);

        // =============== 2. UNIT YANG DIPERPANJANG MANUAL ===============
        seed([U('a', { gpsLicense: 'SF-RTK', gpsLicenseStartDate: '2026-09-20', gpsLicenseEndDate: '2027-09-20' })],
             [OUT('a', 'SF-RTK', '2026-03-01')]);
        t('unit berlaku lebih lama dari distribusi terbarunya: "newer"', plan(), ['a:gps:newer']);
        openPreview();
        t('ditampilkan TIDAK dicentang', previewRows().map(r => r.checked), [false]);
        t('tombol Terapkan nonaktif', document.getElementById('licSyncApplyBtn').disabled, true);
        applyLicSyncSelection();
        t('tidak ditimpa mundur', [writes.length, globalData[0].gpsLicenseEndDate], [0, '2027-09-20']);
        openPreview();
        document.querySelector('#licSyncBody .lic-sync-check').click();
        t('kalau dicentang, tombolnya aktif', document.getElementById('licSyncApplyBtn').disabled, false);
        applyLicSyncSelection();
        t('dan baru ditulis kalau memang dicentang', globalData[0].gpsLicenseEndDate, '2027-03-01');

        // =============== 3. TIER YANG SUDAH TURUN OTOMATIS ===============
        // Distribusi SF-RTK 2025-06-01 → habis 2026-06-01 (sebelum "hari ini").
        seed([U('a', { gpsLicense: 'SF-1', gpsLicenseStartDate: '2025-06-01', gpsLicenseEndDate: '', gpsLicenseExpiredAt: '2026-06-01' })],
             [OUT('a', 'SF-RTK', '2025-06-01')]);
        t('unit yang sudah turun ke SF-1 dari distribusi itu: sesuai', plan(), ['a:gps:sync']);
        openPreview();
        t('tidak dikembalikan ke SF-RTK', writes.length, 0);
        // Unit yang BELUM pernah diisi, dari distribusi yang sudah habis:
        // langsung ditulis dalam keadaan akhirnya, bukan SF-RTK yang mati.
        seed([U('b')], [OUT('b', 'SF-RTK', '2025-06-01')]);
        openPreview(); applyLicSyncSelection();
        t('distribusi habis ditulis langsung sebagai SF-1 dengan tanggal habis tersimpan',
          [globalData[0].gpsLicense, globalData[0].gpsLicenseEndDate, globalData[0].gpsLicenseExpiredAt],
          ['SF-1', '', '2026-06-01']);
        t('badge tipe menampilkan "turun otomatis"', effectiveLicense(globalData[0], 'gps').downgraded, true);
        writes.length = 0;
        applyExpiredLicenseDowngrades();
        t('penurunan otomatis tidak menulis apa-apa lagi', writes.length, 0);
        t('dan sync sesudahnya: sesuai', plan(), ['b:gps:sync']);

        // =============== 4. PEMILIHAN "TERBARU" DAN RESOLUSI UNIT ===============
        seed([U('a')], [OUT('a', 'SF-1', '2026-01-01'), OUT('a', 'SF-RTK', '2026-09-01'), OUT('a', 'G5 Basic', '2026-02-02')]);
        t('per unit + jenis dipilih yang TERBARU', licenseSyncPlan().rows.map(r => r.kind + ':' + r.rec.licenseType), ['display:G5 Basic', 'gps:SF-RTK']);
        openPreview(); applyLicSyncSelection();
        t('GPS dan Display unit yang sama = SATU tulisan', writes.length, 1);
        // Tanggal sama: createdAt lalu id yang memutus, supaya hasilnya stabil.
        seed([U('a')], [OUT('a', 'SF-1', '2026-09-01', { id: 'Z9', createdAt: 5 }), OUT('a', 'SF-RTK', '2026-09-01', { id: 'A1', createdAt: 5 })]);
        t('tanggal & createdAt sama: id yang memutus', licenseSyncPlan().rows[0].rec.id, 'Z9');
        // Unit dihapus lalu diimpor ulang: id baru, SN sama.
        seed([U('baru', { sn: 'SNlama' })], [OUT('lama', 'SF-RTK', '2026-09-01', { sn: 'SNlama' })]);
        t('distribusi ke unit yang diimpor ulang tetap ditemukan lewat SN', plan(), ['baru:gps:update']);
        // Yang dilewati, dengan alasannya.
        seed([U('a'), U('h', { unitGroup: 'heavy' }), U('x', { unitGroup: 'Heavy Equip' })],
             [OUT('hilang', 'SF-RTK', '2026-09-01', { sn: 'SNtidakada' }), OUT('h', 'SF-RTK', '2026-09-01'),
              OUT('x', 'SF-RTK', '2026-09-01'), OUT('a', 'SF-RTK', '')]);
        const pl = licenseSyncPlan();
        t('unit hilang / alat berat / kelompok tak terbaca / tanpa tanggal: dilewati',
          [pl.rows.length, pl.skipped], [0, { noUnit: 1, notTractor: 2, noDate: 1 }]);
        openPreview();
        t('dan pesannya menyebut alasannya', /1 unitnya tidak ditemukan, 1 tanpa tanggal, 2 ke unit non-Pertanian/.test(toasts.join('|')), true);

        // =============== 5. FORM DISTRIBUSI (satu catatan) ===============
        seed([U('a', { gpsLicense: 'SF-RTK', gpsLicenseStartDate: '2026-09-01', gpsLicenseEndDate: '2027-09-01' })], []);
        t('distribusi yang sama persis: "unchanged", tanpa tulisan',
          [applyDistributedLicenseToUnit(OUT('a', 'SF-RTK', '2026-09-01')), writes.length], ['unchanged', 0]);
        t('distribusi lebih lama: tidak mundur', applyDistributedLicenseToUnit(OUT('a', 'SF-RTK', '2026-01-01')), 'skipped-older');
        t('distribusi lebih baru: diterapkan', applyDistributedLicenseToUnit(OUT('a', 'SF-RTK', '2026-09-29')), 'applied');
        t('tanpa tanggal: ditolak, bukan "hari ini + 1 tahun"', applyDistributedLicenseToUnit(OUT('a', 'SF-RTK', '')), false);

        // =============== 6. HAK AKSES ===============
        seed([U('c')], [OUT('c', 'SF-RTK', '2026-09-15')]);
        currentUserDoc = { role: 'staff', status: 'active', email: 's@x.id', access: { licenseStock: 'edit', editUnits: 'view' } };
        applyRoleGating();
        t('tanpa hak edit unit: pratinjau tidak dibuka', openPreview(), false);
        t('dan alasannya disebut', /butuh hak edit pada Edit Units/.test(toasts.join('|')), true);
        currentUserDoc = { role: 'owner', status: 'active', email: 'o@x.id' };
        applyRoleGating();

        // =============== 7. RIWAYAT HANYA MENCATAT PERUBAHAN NYATA ===============
        seed([U('a', { gpsLicense: 'SF-RTK', gpsLicenseStartDate: '2026-09-01', gpsLicenseEndDate: '2027-09-01' }), U('c')],
             [OUT('a', 'SF-RTK', '2026-09-01'), OUT('c', 'SF-RTK', '2026-09-15')]);
        const hist = [];
        const realLog = window.logEvent;
        window.logEvent = e => { hist.push(e.unitId + ':' + e.field); };
        openPreview(); applyLicSyncSelection();
        window.logEvent = realLog;
        t('riwayat hanya berisi unit yang benar-benar berubah',
          hist.sort(), ['c:gpsLicense', 'c:gpsLicenseEndDate', 'c:gpsLicenseStartDate']);

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
