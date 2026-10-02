// Temuan audit berprioritas rendah (v125). Masing-masing kecil, tetapi
// semuanya terlihat oleh pengguna: data yang tidak bisa diedit, angka yang
// salah, pesan yang menyesatkan, atau file yang tertinggal di perangkat.
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
        const tick = ms => new Promise(r => setTimeout(r, ms || 30));
        const toasts = [];
        window.showToast = (m) => { toasts.push(String(m)); };
        let answers = [];
        window.confirm = () => answers.length ? answers.shift() : true;
        const ok = () => Promise.resolve();
        const W = { dmgPhoto: [], wlPhoto: [], hist: 0, units: [] };
        let failLicenses = false, histFails = 0;
        window.cloud = { isReady: true,
            saveUnits: u => { W.units.push(...u); return ok(); }, deleteUnits: ok, getAllUnits: () => Promise.resolve([]),
            addHistoryEvents: () => { W.hist++; if (histFails > 0) { histFails--; const e = new Error('x'); e.code = 'unavailable'; return Promise.reject(e); } return ok(); },
            subscribeUsers: () => () => {},
            saveDamages: ok, saveDamage: ok, saveDamagePhoto: (id, p) => { W.dmgPhoto.push(id); return ok(); }, deleteDamagePhoto: ok,
            saveWorkLogs: ok, saveWorkLog: ok, saveWorkLogPhotos: (id) => { W.wlPhoto.push(id); return ok(); }, deleteWorkLogPhotos: ok,
            saveLeaveRequest: ok, saveLeaveRequests: ok, saveTeamDocs: ok,
            saveImplements: ok, saveLicense: ok, deleteLicense: ok,
            saveLicenses: () => failLicenses ? Promise.reject(Object.assign(new Error('no'), { code: 'permission-denied' })) : ok() };
        currentUser = { uid: 'uO', email: 'o@x.id' };
        currentUserDoc = { role: 'owner', status: 'active', email: 'o@x.id', displayName: 'Owner' };
        try { localStorage.setItem('tractorLicenseDatesApplied_v2', '1'); } catch (_) {}
        hideAuthGates(); applyRoleGating();

        // =============== 1. anggota nonaktif ===============
        navigateTo('team');
        teamMembers = [{ id: 'm1', name: 'Ani', active: true }, { id: 'm2', name: 'Budi', active: false }];
        leaveRequests = [{ id: 'l1', memberId: 'm2', memberName: 'Budi', type: 'izin', dateFrom: '2026-09-01', dateTo: '2026-09-01', approval: 'draft', createdByUid: 'uO' }];
        workLogs = [{ id: 'w1', memberId: 'm2', memberName: 'Budi', date: '2026-09-01', task: 'x', approval: 'draft', createdByUid: 'uO' }];
        editLeave('l1');
        t('izin anggota nonaktif: anggotanya tetap terpilih', document.getElementById('lvMember').value, 'm2');
        t('dan ditandai nonaktif', document.getElementById('lvMember').selectedOptions[0].textContent, 'Budi (inactive)');
        closeLeaveModal(true);
        editWorkLog('w1');
        t('laporan anggota nonaktif: anggotanya tetap terpilih', document.getElementById('wlMember').value, 'm2');
        closeWorkLogModal(true);
        showAddWorkLogForm();
        t('form baru tetap hanya anggota aktif', [...document.getElementById('wlMember').options].map(o => o.value), ['', 'm1']);
        closeWorkLogModal(true);

        // =============== 2. foto di cadangan lama ===============
        const old = { version: 3, units: [{ id: 'u1', name: 'T1', sn: 'S1' }],
            damages: [{ id: 'd1', unitId: 'u1', photo: 'data:image/jpeg;base64,QUJD' }, { id: 'd2', unitId: 'u1' }],
            workLogs: [{ id: 'wOld', memberId: 'm1', date: '2026-09-01', task: 'a', photos: ['data:image/jpeg;base64,QQ'] }] };
        const split = splitInlineBackupPhotos(JSON.parse(JSON.stringify(old)));
        t('foto kerusakan dipindah ke peta foto', [split.damages[0].photo, split.damages[0].hasPhoto, split.damagePhotos.d1], ['', true, 'data:image/jpeg;base64,QUJD']);
        t('catatan tanpa foto tidak disentuh', split.damages[1], { id: 'd2', unitId: 'u1' });
        t('foto laporan dipindah', [split.workLogs[0].photos, split.workLogs[0].photoCount, split.workLogPhotos.wOld.length], [[], 1, 1]);
        globalData = [{ id: 'u1', name: 'T1', sn: 'S1' }]; globalDamages = []; workLogs = [];
        answers = [true];
        importBackup(new File([JSON.stringify(old)], 'old.json', { type: 'application/json' }));
        await tick(400);
        t('restore cadangan lama: tidak ada foto di dalam catatan', globalDamages.some(d => d.photo), false);
        t('restore cadangan lama: foto dikirim ke koleksinya', [W.dmgPhoto, W.wlPhoto], [['d1'], ['wOld']]);
        t('dan tidak ada foto yang masuk localStorage', (localStorage.getItem('tractorDamages') || '').includes('base64'), false);
        if (typeof closeRestoreReport === 'function') closeRestoreReport();

        // =============== 3 + 4. impor CSV unit ===============
        const pd = processData([{ 'Nickname': 'H1', 'Serial Number': 'SH1', 'Unit Group': 'excavator', 'Camera AI': 'Good' }],
                               { fallbackGroup: 'tractor', fileGroup: 'heavy' });
        t('Unit Group tak dikenal: dicatat kelompok yang benar-benar dipakai', [pd.valid[0].unitGroup, pd.groupWarnings[0].group], ['heavy', 'heavy']);
        navigateTo('editUnits');
        globalData = [{ id: 'u1', name: 'T1', sn: 'S1', unitGroup: 'tractor', status: 'Good' }]; saveToStorage(globalData);
        let report = null;
        const realReport = window.showImportReport;
        window.showImportReport = r => { report = r; };
        const realPapa = window.Papa;
        window.Papa = { parse: (f, o) => o.complete({ data: [
            { 'Nickname': 'T2', 'Serial Number': 'S2', 'Status': 'Good', 'Display': 'Good', 'GPS': 'Good', 'Nomor Lambung': 'LB-07' } ],
            meta: { fields: ['Nickname', 'Serial Number', 'Status', 'Display', 'GPS', 'Nomor Lambung'] } }) };
        answers = [true, true, true];
        handleEditCSVImport(new File(['x'], 'u.csv'));
        await tick(100);
        window.Papa = realPapa; window.showImportReport = realReport;
        const notes = (report && report.notes) || [];
        t('CSV traktor dengan Nomor Lambung: tidak ada peringatan palsu', notes.filter(n => /assetCode/.test(n.reason)).length, 0);
        t('tetapi nilainya tetap tidak ditulis ke traktor', (globalData.find(u => u.sn === 'S2') || {}).assetCode, undefined);
        document.getElementById('importReportModal').classList.remove('open');

        // =============== 5. filter dashboard bertahan ===============
        navigateTo('dashboard');
        globalData = [
            { id: 'a', name: 'A1', sn: 'SA', status: 'Good', site: 'X', display: 'Good', gps: 'Good', steering: 'Good', jdlink: 'Good' },
            { id: 'b', name: 'B1', sn: 'SB', status: 'Breakdown', site: 'X', display: 'Good', gps: 'Good', steering: 'Good', jdlink: 'Good' }];
        saveToStorage(globalData);
        onDataLoaded();
        document.getElementById('statusFilter').value = 'Breakdown';
        applyFilter();
        t('filter aktif: 1 unit', filteredData.length, 1);
        globalData = globalData.concat([{ id: 'c', name: 'C1', sn: 'SC', status: 'Good', site: 'X', display: 'Good', gps: 'Good', steering: 'Good', jdlink: 'Good' }]);
        onDataLoaded();
        t('setelah snapshot: filter status tetap', [document.getElementById('statusFilter').value, filteredData.length], ['Breakdown', 1]);
        clearFilter();

        // =============== 6 + 7. armada alat berat saja ===============
        globalData = [{ id: 'h1', name: 'EX1', sn: 'SH', unitGroup: 'heavy', status: 'Good' }];
        editUnitsGroup = 'tractor'; _editGroupPicked = false;
        t('armada alat berat saja: Edit Units membuka tab alat berat', effectiveEditGroup(), 'heavy');
        _editGroupPicked = true;
        t('kecuali tab pertanian dipilih sendiri', effectiveEditGroup(), 'tractor');
        _editGroupPicked = false;

        // =============== 8. role pendaftar tidak kembali ke KHL ===============
        navigateTo('users');
        const users = [{ uid: 'p1', email: 'p@x.id', displayName: 'P', role: 'viewer', status: 'pending', createdAt: 1 },
                       { uid: 'uO', email: 'o@x.id', displayName: 'Owner', role: 'owner', status: 'active', createdAt: 1 }];
        allUsers = users; renderUsersView();
        document.getElementById('approveRole_p1').value = 'staff';
        allUsers = users.map(u => u.uid === 'uO' ? { ...u, activeSession: { id: 's', startedAt: 2, device: 'x' } } : u);
        renderUsersView();
        t('pilihan role pendaftar bertahan saat data user berubah', document.getElementById('approveRole_p1').value, 'staff');

        // =============== 9. waktu CSV riwayat ===============
        const ts = new Date(2026, 8, 30, 7, 5, 9).getTime();
        t('waktu riwayat ditulis dalam waktu lokal', toLocalDateTime(ts), '2026-09-30 07:05:09');

        // =============== 10. cincin cadangan otomatis ===============
        localStorage.removeItem('tractorUnits_autobackup');
        localStorage.removeItem('tractorUnits');
        const v = n => [{ id: 'u', name: 'v' + n }];
        saveToStorage(v(1)); saveToStorage(v(2)); saveToStorage(v(3)); saveToStorage(v(4));
        const ring = readAutoBackups().map(r => r.units[0].name);
        t('cadangan otomatis: tiga keadaan SEBELUMNYA, tidak satu pun = data sekarang', ring, ['v3', 'v2', 'v1']);
        saveToStorage(v(4));
        t('menyimpan data yang sama tidak menambah titik', readAutoBackups().length, 3);

        // =============== 11. riwayat dicoba ulang ===============
        W.hist = 0; histFails = 1;
        logEvent({ action: 'update', unitName: 'x', field: 'f', before: 'a', after: 'b' });
        await tick(200);
        t('kiriman riwayat gagal sekali', W.hist, 1);
        t('dan dijadwalkan ulang sendiri', !!_historyFlushTimer, true);
        clearTimeout(_historyFlushTimer); _historyFlushTimer = null;
        _flushHistoryQueue();
        await tick();
        t('percobaan ulang terkirim tanpa menunggu logEvent baru', [W.hist, _historyPushQueue.length], [2, 0]);

        // =============== 12. tanda petik CSV ===============
        t("tanda ' dari ekspor dibuang saat impor", [getVal({ 'Remarks': "'- ganti oli" }, 'Remarks'), getVal({ 'X': "'=1+1" }, 'X'), getVal({ 'X': "'biasa" }, 'X')],
          ['- ganti oli', '=1+1', "'biasa"]);

        // =============== 14. kalimat ringkasan ===============
        navigateTo('dashboard');
        const iso = d => toISODate(new Date(Date.now() + d * 86400000));
        globalData = [
            { id: 'e1', name: 'E1', sn: 'E1', status: 'Good', display: 'Good', gps: 'Good', steering: 'Good', jdlink: 'Good', gpsLicense: 'SF-1', gpsLicenseEndDate: iso(-5) },
            { id: 'e2', name: 'E2', sn: 'E2', status: 'Good', display: 'Good', gps: 'Good', steering: 'Good', jdlink: 'Good', gpsLicense: 'SF-1', gpsLicenseEndDate: iso(10) }];
        saveToStorage(globalData);
        renderNarrative(globalData);
        const nar = document.getElementById('dashNarrative').textContent;
        t('ringkasan memisahkan yang sudah dan akan expire', [/1 lisensi sudah expire/.test(nar), /1 lisensi akan expire/.test(nar)], [true, true]);

        // =============== 15. impor implement ganda ===============
        navigateTo('implements');
        globalImplements = [{ id: 'i1', profileName: 'Bajak', code: 'B1' }];
        window.Papa = { parse: (f, o) => o.complete({ data: [
            { 'Profile Name': 'bajak', 'Code': 'b1' }, { 'Profile Name': 'Garu', 'Code': 'G1' }, { 'Profile Name': 'Garu', 'Code': 'G1' }] }) };
        toasts.length = 0;
        handleImplementCSVImport(new File(['x'], 'i.csv'));
        window.Papa = realPapa;
        t('impor implement melewati yang sudah ada dan yang ganda', [globalImplements.length, toasts.some(m => /2 dilewati \\(Profile Name \\+ Code sudah ada\\)/.test(m))], [2, true]);

        // =============== 16. lampiran dua penghapusan ===============
        const purged = [];
        const realPurge = window.attachDbDelete;
        window.attachDbDelete = ids => { purged.push(...ids); return Promise.resolve(); };
        globalData = [{ id: 'x1', name: 'X1', sn: 'X1', attachments: [{ id: 'a1' }] }, { id: 'x2', name: 'X2', sn: 'X2', attachments: [{ id: 'a2' }] }];
        _pendingAttachPurge = [];
        deleteUnits(['x1']);
        deleteUnits(['x2']);
        t('hapus kedua dalam jendela undo: lampiran yang pertama dibersihkan', purged, ['a1']);
        t('yang kedua masih menunggu undo', _pendingAttachPurge, ['a2']);
        window.attachDbDelete = realPurge; _pendingAttachPurge = [];

        // =============== 17. impor lisensi ditolak ===============
        navigateTo('licenseStock');
        globalData = [{ id: 'c1', name: 'C1', sn: 'SNc1', unitGroup: 'tractor', status: 'Good', gpsLicense: 'SF-1', gpsLicenseEndDate: '' }];
        saveToStorage(globalData);
        globalLicenseStock = [];
        failLicenses = true;
        window.Papa = { parse: (f, o) => o.complete({ data: [
            { 'Tanggal': toISODate(), 'Jenis': 'Distribusi', 'Jenis Lisensi': 'SF-RTK', 'Jumlah': '1', 'Serial Number': 'SNc1' }] }) };
        handleLicenseCSVImport(new File(['x'], 'l.csv'));
        window.Papa = realPapa;
        t('impor lisensi: unit sempat diperbarui', globalData[0].gpsLicense, 'SF-RTK');
        await tick(100);
        t('ditolak server: lisensi unit dikembalikan', [globalData[0].gpsLicense, globalLicenseStock.length], ['SF-1', 0]);
        failLicenses = false;

        // =============== 18. kolom kosong kerusakan ===============
        navigateTo('damage');
        globalDamages = [];
        renderDamageTable();
        const td = document.querySelector('#damageBody td');
        t('baris kosong kerusakan selebar tabelnya', td && td.colSpan, document.querySelectorAll('#damageTable thead th').length);

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
