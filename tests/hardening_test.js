// Empat bug "harus segera diperbaiki" dari audit sesudah v110.
//
//   1. Alasan breakdown ditaruh di argumen onclick. escapeHtml mengubah ' jadi
//      &#39;, dan browser mengembalikannya menjadi ' SEBELUM JavaScript
//      berjalan — jadi alasan berisi kutip bisa menjalankan kode, dan alasan
//      berisi baris baru membuat popover gagal dibuka.
//   2. Surat izin (dan foto laporan harian) dimuat SESUDAH form terbuka.
//      Menambah lembar sebelum yang lama tiba — atau sesudah gagal tiba —
//      membuat Simpan MENGGANTI surat lama dengan yang baru saja. Dan hasil
//      muat yang terlambat bisa mendarat di form pengajuan lain.
//   3. Unit dengan kelompok yang tidak terbaca ('Heavy Equip') dibaca sebagai
//      traktor: Periksa Data menghapus data alat beratnya, Simpan menulis
//      'Breakdown' ke komponen John Deere, dan tidak ada jalan untuk
//      membetulkan kelompoknya.
//   4. CSV berkolom alat berat tanpa kolom Unit Group bisa menimpa traktor
//      yang kebetulan ber-SN sama, dan laporannya bilang "1 updated".
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
        const tick = (ms) => new Promise(r => setTimeout(r, ms || 10));
        // Janji yang diselesaikan oleh uji, supaya urutan balapan bisa diatur.
        const defer = () => { let res, rej; const p = new Promise((a, b) => { res = a; rej = b; }); return { p, res, rej }; };

        const toasts = [];
        const realToast = window.showToast;
        window.showToast = (m, k) => { toasts.push(String(m)); };
        let answer = true;
        window.confirm = () => answer;

        const unitWrites = [], docWrites = [], leaveWrites = [], photoWrites = [], logWrites = [];
        const docFetch = {}, photoFetch = {};
        window.cloud = { isReady: true,
            saveUnits: u => { unitWrites.push(JSON.parse(JSON.stringify(u))); return Promise.resolve(); },
            deleteUnits: () => Promise.resolve(), getAllUnits: () => Promise.resolve([]),
            addHistoryEvents: () => Promise.resolve(), subscribeUsers: () => () => {},
            saveDamage: () => Promise.resolve(), saveDamages: () => Promise.resolve(),
            saveLeaveRequest: r => { leaveWrites.push(JSON.parse(JSON.stringify(r))); return Promise.resolve(); },
            deleteLeaveRequest: () => Promise.resolve(),
            getTeamDocs: id => (docFetch[id] = defer()).p,
            saveTeamDocs: (id, pages) => { docWrites.push(['save', id, pages.slice()]); return Promise.resolve(); },
            deleteTeamDocs: id => { docWrites.push(['delete', id]); return Promise.resolve(); },
            saveWorkLog: r => { logWrites.push(JSON.parse(JSON.stringify(r))); return Promise.resolve(); },
            deleteWorkLog: () => Promise.resolve(),
            getWorkLogPhotos: id => (photoFetch[id] = defer()).p,
            saveWorkLogPhotos: (id, ph) => { photoWrites.push(['save', id, ph.slice()]); return Promise.resolve(); },
            deleteWorkLogPhotos: id => { photoWrites.push(['delete', id]); return Promise.resolve(); } };
        currentUser = { uid: 'uO', email: 'o@x.id' };
        currentUserDoc = { role: 'owner', status: 'active', email: 'o@x.id', displayName: 'Owner' };
        try { localStorage.setItem('tractorLicenseDatesApplied_v2', '1');
              localStorage.setItem('tractorWorkLogPhotosSplit', '1'); } catch (_) {}
        hideAuthGates(); applyRoleGating();

        const TR = (id, x) => Object.assign({ id, name: 'GGTR' + id, model: 'JOHN DEERE 6110B', sn: 'SN' + id,
            implement: 'Plough', status: 'Good', display: 'Good', gps: 'Good', steering: 'Good', jdlink: 'Good',
            site: 'PT. GPA', yearReceived: '2021', userCategory: '', gpsLicense: 'SF-1', licenseDisplay: 'G5 Basic',
            remarks: '', breakdownReason: '', downtimeHistory: [] }, x || {});
        const HV = (id, x) => Object.assign({ id, name: 'EXC' + id, model: 'KOMATSU PC200', sn: 'KMT' + id,
            unitGroup: 'heavy', machineType: 'Excavator', assetCode: 'LB-' + id, workTool: 'Bucket',
            status: 'Good', cameraAi: 'Good', telematicBox: 'Good', switchLimiter: 'Good', rotaryLamp: 'Good',
            site: 'PT. GPA', yearReceived: '2025', userCategory: '', remarks: '', breakdownReason: '', downtimeHistory: [] }, x || {});

        // =============== 1. ALASAN BREAKDOWN ===============
        const EVIL = "x');window.__xss=1;('";
        const NL = 'Hidrolik bocor\\nselang pecah';
        const BS = 'Oli \\\\ bocor';
        navigateTo('dashboard');
        globalData = [TR('u_t1', { status: 'Breakdown', breakdownReason: EVIL }),
                      TR('u_t2', { status: 'Breakdown', breakdownReason: NL }),
                      TR('u_t3', { status: 'Breakdown', breakdownReason: BS }),
                      HV('u_h1', { status: 'Breakdown', breakdownReason: EVIL })];
        const clickReasons = () => [...document.querySelectorAll('#detailBody .bd-clickable')].map(el => {
            document.getElementById('breakdownPopover').textContent = '';
            el.click();
            return document.getElementById('breakdownPopover').textContent;
        });
        setDashGroup('tractor');
        filteredData = scopeDashUnits(); updateDashboard(filteredData);
        const trac = clickReasons();
        t('tabel traktor: alasan berkutip tidak menjalankan kode', window.__xss, undefined);
        t('tabel traktor: setiap popover menampilkan alasannya persis', trac, [EVIL, NL, BS]);
        setDashGroup('heavy');
        filteredData = scopeDashUnits(); updateDashboard(filteredData);
        t('tabel alat berat: alasan berkutip tidak menjalankan kode', [clickReasons(), window.__xss], [[EVIL], undefined]);
        setDashGroup('all');
        filteredData = scopeDashUnits(); updateDashboard(filteredData);
        t('cakupan Semua: keempat popover utuh', clickReasons().sort(), [EVIL, EVIL, NL, BS].sort());
        t('cakupan Semua: tetap tidak ada kode yang berjalan', window.__xss, undefined);
        document.getElementById('breakdownPopover').style.display = 'none';
        setDashGroup('tractor');

        // =============== 2a. SURAT IZIN ===============
        navigateTo('team');
        teamMembers = [{ id: 'm1', name: 'Andi', company: 'PT. GPA', active: true },
                       { id: 'm2', name: 'Budi', company: 'PT. MNM', active: true }];
        const LV = (id, memberId, docCount) => ({ id, memberId, memberName: memberId === 'm1' ? 'Andi' : 'Budi',
            company: 'PT. GPA', type: 'sakit', dateFrom: '2026-09-21', dateTo: '2026-09-22', days: 2,
            reason: 'Demam', docCount, approval: 'pending', createdAt: 1, updatedAt: 1 });
        leaveRequests = [LV('lvA', 'm1', 2), LV('lvB', 'm2', 1), LV('lvC', 'm1', 0)];
        const DOC = s => 'data:image/jpeg;base64,' + s;
        let compressGate = null;
        window.compressImageToDataURL = async () => { if (compressGate) await compressGate.p; return DOC('BARU'); };
        const addFile = (fn) => fn({ target: { files: [new File(['x'], 'surat.jpg', { type: 'image/jpeg' })], value: 'x' } });
        const lvAdd = () => document.getElementById('lvDocAddBtn');

        // (a) menambah surat SEBELUM yang lama tiba
        editLeave('lvA');
        t('selama surat lama dimuat, tombol Tambah Surat nonaktif', lvAdd().disabled, true);
        t('dan input berkasnya juga (labelnya tetap bisa membuka pemilih berkas)',
          document.getElementById('lvDocInput').disabled, true);
        await addFile(handleLeaveDocChange);
        t('dan menambah lewat input tidak masuk', [_lvDocs.length, _lvDocsDirty], [0, false]);
        saveLeave({ preventDefault() {} });
        t('menyimpan sebelum surat lama tiba tidak menyentuh surat', docWrites.length, 0);
        t('jumlah surat pada pengajuan dipertahankan', leaveWrites[leaveWrites.length - 1].docCount, 2);
        docFetch.lvA.res([DOC('A1'), DOC('A2')]); await tick();
        _leaveDocCache.clear();

        // (b) hasil muat yang terlambat tidak mendarat di pengajuan lain
        editLeave('lvA');
        closeLeaveModal();
        editLeave('lvB');
        docFetch.lvA.res([DOC('A1'), DOC('A2')]); await tick();
        t('surat A yang terlambat TIDAK muncul di form B', [_lvDocs.length, _lvDocsLoading], [0, true]);
        docFetch.lvB.res([DOC('B1')]); await tick();
        t('surat B sendiri tetap terisi', _lvDocs, [DOC('B1')]);
        t('sesudah termuat, tombol Tambah Surat aktif lagi', [lvAdd().disabled, document.getElementById('lvDocInput').disabled], [false, false]);
        await addFile(handleLeaveDocChange);
        saveLeave({ preventDefault() {} });
        t('menambah sesudah termuat MENAMBAH, bukan mengganti',
          docWrites[docWrites.length - 1], ['save', 'lvB', [DOC('B1'), DOC('BARU')]]);
        t('cache sesi tidak ikut tercemar oleh form', _leaveDocCache.get('lvA'), [DOC('A1'), DOC('A2')]);
        _leaveDocCache.clear();

        // (c) gagal memuat: surat lama tetap, dan tidak bisa ditimpa
        editLeave('lvA');
        docFetch.lvA.rej(new Error('offline')); await tick();
        t('gagal memuat: tombol Tambah Surat tetap nonaktif', lvAdd().disabled, true);
        t('dan form menjelaskan sebabnya', /gagal dimuat/.test(document.getElementById('lvDocPreviews').textContent), true);
        await addFile(handleLeaveDocChange);
        const before = docWrites.length;
        saveLeave({ preventDefault() {} });
        t('gagal memuat lalu simpan: surat lama tidak disentuh',
          [docWrites.length - before, leaveWrites[leaveWrites.length - 1].docCount], [0, 2]);
        _leaveDocCache.clear();

        // (d) form ditutup/berganti SELAMA foto dikompresi
        editLeave('lvC');            // tanpa surat: tidak ada yang dimuat
        compressGate = defer();
        const pending = addFile(handleLeaveDocChange);
        closeLeaveModal();
        editLeave('lvC');
        compressGate.res(); await pending; compressGate = null;
        t('kompresi yang selesai sesudah form berganti tidak masuk ke form baru', [_lvDocs.length, _lvDocsDirty], [0, false]);
        closeLeaveModal();

        // (e) pengajuan baru tetap bisa diberi surat
        showAddLeaveForm();
        t('pengajuan baru: tombol Tambah Surat aktif', lvAdd().disabled, false);
        await addFile(handleLeaveDocChange);
        t('dan suratnya masuk', _lvDocs, [DOC('BARU')]);
        closeLeaveModal(true);

        // =============== 2b. FOTO LAPORAN HARIAN ===============
        globalData = [TR('u_t1')];
        workLogs = [{ id: 'wA', date: '2026-09-15', memberId: 'm1', memberName: 'Andi', start: '08:00', end: '16:00',
                      unitId: 'u_t1', unitName: 'GGTRu_t1', sn: 'SNu_t1', task: 'Servis', issue: '', photoCount: 2 }];
        switchTeamTab('worklog');
        const wlAdd = () => document.getElementById('wlPhotoAddBtn');
        editWorkLog('wA');
        t('laporan: selama foto lama dimuat, Tambah Foto nonaktif', wlAdd().disabled, true);
        await addFile(handleWorkLogPhotoChange);
        t('laporan: menambah foto sebelum yang lama tiba tidak masuk', [_wlPhotos.length, _wlPhotosDirty], [0, false]);
        photoFetch.wA.rej(new Error('offline')); await tick();
        t('laporan: gagal memuat, Tambah Foto tetap nonaktif', wlAdd().disabled, true);
        await addFile(handleWorkLogPhotoChange);
        saveWorkLog({ preventDefault() {} });
        t('laporan: gagal memuat lalu simpan, foto lama tidak disentuh',
          [photoWrites.length, logWrites[logWrites.length - 1].photoCount], [0, 2]);

        editWorkLog('wA');
        photoFetch.wA.res([DOC('W1'), DOC('W2')]); await tick();
        t('laporan: sesudah termuat, Tambah Foto aktif', wlAdd().disabled, false);
        await addFile(handleWorkLogPhotoChange);
        t('laporan: cache sesi tidak ikut bertambah', (_wlPhotoCache.get('wA') || []).length, 2);
        saveWorkLog({ preventDefault() {} });
        t('laporan: menambah sesudah termuat menambah, bukan mengganti',
          photoWrites[photoWrites.length - 1], ['save', 'wA', [DOC('W1'), DOC('W2'), DOC('BARU')]]);
        window.compressImageToDataURL = undefined;

        // =============== 3. KELOMPOK TIDAK TERBACA ===============
        navigateTo('editUnits');
        const ODD = () => HV('u_x1', { unitGroup: 'Heavy Equip', name: 'EXC-ANEH' });
        globalData = [TR('u_t1'), ODD()];
        saveToStorage(globalData);
        t('kelompok tak terbaca dikenali', [hasUnreadableGroup(globalData[1]), hasUnreadableGroup(globalData[0]),
            hasUnreadableGroup(HV('h')), hasUnreadableGroup({ unitGroup: 'Alat Berat' })], [true, false, false, false]);

        // Firewall
        unitWrites.length = 0;
        t('firewall: komponen John Deere tidak bisa ditulis ke unit tak terbaca',
          updateUnit('u_x1', { gps: 'Breakdown', display: 'Breakdown' }), false);
        t('firewall: komponen alat berat juga tidak', updateUnit('u_x1', { cameraAi: 'Breakdown' }), false);
        t('firewall: field bersama tetap bisa', updateUnit('u_x1', { site: 'PT. MNM' }), true);
        t('firewall: kelompok tidak bisa diubah tanpa jalur Periksa Data', updateUnit('u_x1', { unitGroup: 'heavy' }), false);
        t('firewall: kelompok yang terbaca tetap tidak bisa diubah, bahkan lewat jalur itu',
          [updateUnit('u_t1', { unitGroup: 'heavy' }, { setGroup: true }), globalData[0].unitGroup], [false, undefined]);
        t('firewall: nilai kelompok ngawur ditolak', updateUnit('u_x1', { unitGroup: 'mining' }, { setGroup: true }), false);
        t('tidak ada tulisan komponen yang lolos ke cloud',
          unitWrites.flat().some(u => u.id === 'u_x1' && ('gps' in u || 'display' in u || u.cameraAi !== 'Good')), false);

        // Form edit dan edit sebaris
        toasts.length = 0;
        editUnit('u_x1');
        t('Edit pada unit tak terbaca tidak membuka form', document.getElementById('unitModal').classList.contains('open'), false);
        t('dan menjelaskan jalan keluarnya', /Periksa Data/.test(toasts[toasts.length - 1] || ''), true);
        const cell = document.createElement('td');
        cell.dataset.id = 'u_x1'; cell.dataset.field = 'gps'; cell.textContent = 'Breakdown';
        unitWrites.length = 0;
        saveInlineEdit(cell);
        // Unit itu tampil di tabel traktor, dan GPS-nya memang kosong.
        t('edit sebaris komponen: tidak menulis, dan sel dikembalikan', [unitWrites.length, cell.textContent], [0, '']);

        // Periksa Data
        const found = dcUnitGroupFields();
        t('Periksa Data: satu temuan "tak dikenal", BUKAN "field kelompok lain"',
          found.map(f => f.kind), ['kelompok-tak-dikenal']);
        t('dan menebak kelompoknya dari isinya', /cocok dengan Heavy Equipment/.test(found[0].detail), true);
        unitWrites.length = 0;
        answer = true;
        fixStrayGroupFields();
        t('tombol Bersihkan tidak menghapus data alat beratnya', [unitWrites.length, globalData[1].cameraAi, globalData[1].machineType],
          [0, 'Good', 'Excavator']);
        navigateTo('leader'); switchLeaderTab('check');
        t('tombol Tetapkan kelompok muncul', document.getElementById('dataCheckGroupSetBtn').style.display, '');
        let promptText = '';
        window.prompt = (m, d) => { promptText = m; return d; };   // terima tebakan
        setUnreadableGroups();
        t('prompt menawarkan tebakan Heavy Equipment', /2 untuk Heavy Equipment/.test(promptText), true);
        t('kelompoknya ditetapkan, isinya utuh', [globalData[1].unitGroup, globalData[1].cameraAi, globalData[1].machineType],
          ['heavy', 'Good', 'Excavator']);
        t('tulisan ke cloud membawa kelompok barunya', unitWrites.flat().filter(u => u.id === 'u_x1').pop().unitGroup, 'heavy');
        t('sesudahnya Periksa Data bersih', dcUnitGroupFields().length, 0);
        t('dan tombolnya hilang', document.getElementById('dataCheckGroupSetBtn').style.display, 'none');
        globalData[1] = ODD();
        window.prompt = () => null;
        setUnreadableGroups();
        t('Batal pada prompt tidak mengubah apa pun', globalData[1].unitGroup, 'Heavy Equip');

        // Impor CSV (perbarui) pada unit tak terbaca
        const csvRes = bulkUpdateUnitsFromCSV([{ sn: 'KMTu_x1', name: 'BARU', status: 'Good' }]);
        t('impor CSV tidak memperbarui unit tak terbaca', [csvRes.updated, csvRes.failed.length, globalData[1].name], [0, 1, 'EXC-ANEH']);

        // Restore GANTI dan cadangan otomatis
        navigateTo('editUnits');
        globalData = [TR('u_t1')]; saveToStorage(globalData);
        unitWrites.length = 0; toasts.length = 0;
        const file = new File([JSON.stringify({ version: 4, units: [TR('u_t9'), ODD()] })], 'b.json', { type: 'application/json' });
        answer = false;                                  // Batal = GANTI
        importBackup(file);
        await tick(300);
        answer = true;
        t('restore GANTI dengan kelompok tak terbaca ditolak sebelum menulis', [unitWrites.length, globalData.map(u => u.id)], [0, ['u_t1']]);
        t('dan pesannya menyebut nilainya', /"Heavy Equip"/.test(toasts.join('|')), true);
        // Cadangan otomatis adalah salinan data perangkat ini sendiri. Menolaknya
        // membuat rollback mustahil selama satu unit seperti itu ada (setiap
        // entri membawanya) — jadi ia dipulihkan, dan tetap dipagari.
        const realRead = window.readAutoBackups;
        window.readAutoBackups = () => [{ at: 1, units: [TR('u_t9'), ODD()] }];
        toasts.length = 0;
        restoreAutoBackup(0);
        t('cadangan otomatis yang membawa unit tak terbaca TETAP bisa dipulihkan', globalData.map(u => u.id), ['u_t9', 'u_x1']);
        t('dan unit yang kembali tetap dipagari', [hasUnreadableGroup(globalData[1]), updateUnit('u_x1', { gps: 'Breakdown' })], [true, false]);
        // Rollback melewati "Tetapkan kelompok" juga tidak terkunci.
        globalData[1] = { ...globalData[1] };
        updateUnit('u_x1', { unitGroup: 'heavy' }, { setGroup: true });
        restoreAutoBackup(0);
        t('rollback ke sebelum kelompoknya ditetapkan tidak ditolak', globalData.find(u => u.id === 'u_x1').unitGroup, 'Heavy Equip');
        // Tetapi dua kelompok yang TERBACA dan berbeda tetap ditolak, dengan
        // saran yang berlaku untuk cadangan otomatis.
        globalData = [HV('u_x1')]; saveToStorage(globalData);
        window.readAutoBackups = () => [{ at: 1, units: [TR('u_x1')] }];
        toasts.length = 0;
        restoreAutoBackup(0);
        t('konflik kelompok terbaca tetap ditolak', globalData[0].unitGroup, 'heavy');
        t('dan sarannya tidak menyebut GABUNG', [/GABUNG/.test(toasts.join('|')), /berkas cadangan/.test(toasts.join('|'))], [false, true]);
        window.readAutoBackups = realRead;
        const merged = addUnits([ODD()]);
        t('GABUNG tetap melewati unit itu dengan alasannya', [merged.added, (merged.skippedDetails[0] || {}).reason],
          [0, 'Kelompok "Heavy Equip" tidak dikenal']);

        // Nilai kelompok yang tak terbaca bisa berisi apa saja, dan showToast
        // merender HTML. Diuji dengan showToast ASLI.
        const EVILG = '<img src=x onerror="window.__xssG=(window.__xssG||0)+1">';
        globalData = [TR('u_t1'), HV('u_x2', { unitGroup: EVILG })]; saveToStorage(globalData);
        window.showToast = realToast;
        editUnit('u_x2');
        const evilCell = document.createElement('td');
        evilCell.dataset.id = 'u_x2'; evilCell.dataset.field = 'gps'; evilCell.textContent = 'Breakdown';
        saveInlineEdit(evilCell);
        answer = false;
        importBackup(new File([JSON.stringify({ version: 4, units: [HV('u_x3', { unitGroup: EVILG })] })], 'e.json'));
        await tick(300);
        answer = true;
        await tick(50);
        t('toast kelompok tak terbaca tidak menjalankan kode (edit, sebaris, restore)',
          [window.__xssG, document.querySelectorAll('#toastContainer img').length], [undefined, 0]);
        t('dan nilainya tetap terbaca sebagai teks',
          [...document.querySelectorAll('#toastContainer .toast')].filter(el => el.textContent.includes('<img src=x')).length, 3);
        document.getElementById('toastContainer').innerHTML = '';
        window.showToast = (m, k) => { toasts.push(String(m)); };

        // Kerusakan "set Breakdown" pada komponen unit tak terbaca.
        navigateTo('damage');
        globalData = [TR('u_t1'), ODD()]; saveToStorage(globalData);
        globalDamages = [];
        const fillDamage = (u, comp, setBd) => {
            showAddDamageForm();
            document.getElementById('dmgUnit').value = damageUnitLabel(u);
            document.getElementById('dmgType').value = 'Device Precision';
            renderDamageComponentOptions();
            document.getElementById('dmgComponent').value = comp;
            document.getElementById('dmgDescription').value = 'uji';
            document.getElementById('dmgSetBreakdown').checked = setBd;
        };
        toasts.length = 0; unitWrites.length = 0;
        fillDamage(globalData[1], 'GPS', true);
        saveDamage({ preventDefault() {} });
        t('kerusakan komponen + set Breakdown pada unit tak terbaca ditolak sebelum disimpan',
          [globalDamages.length, unitWrites.length], [0, 0]);
        t('tanpa mengaku "di-set Breakdown"', toasts.some(x => /di-set Breakdown/.test(x)), false);
        fillDamage(globalData[1], 'GPS', false);
        saveDamage({ preventDefault() {} });
        t('tanpa centang set Breakdown, catatannya tetap bisa disimpan', globalDamages.length, 1);
        document.getElementById('damageModal')?.classList.remove('open');
        t('traktor biasa tidak terpengaruh', _applyDamageBreakdown('u_t1', 'Device Precision', 'GPS', 'x'), 'Komponen GPS unit');

        // Migrasi lisensi tidak menyentuh unit tak terbaca.
        globalData = [HV('u_x1', { unitGroup: 'Heavy Equip', gpsLicense: 'SF-RTK', gpsLicenseEndDate: '2020-01-01' })];
        unitWrites.length = 0;
        applyExpiredLicenseDowngrades();
        t('penurunan lisensi otomatis melewati unit tak terbaca', [unitWrites.length, globalData[0].gpsLicense], [0, 'SF-RTK']);

        // =============== 4. CSV BERKOLOM ALAT BERAT MENIMPA TRAKTOR ===============
        globalData = [TR('u_t1'), HV('u_h1')]; saveToStorage(globalData);
        unitWrites.length = 0;
        const heavyFile = processData([{ 'Nickname': 'EXC-99', 'Serial Number': 'SNu_t1', 'Status': 'Breakdown',
                                         'Camera AI': 'Good', 'Jenis Alat': 'Excavator' }],
                                      { fallbackGroup: 'heavy', fileGroup: 'heavy' });
        const r4 = bulkUpdateUnitsFromCSV(heavyFile.valid);
        t('berkas alat berat tidak memperbarui traktor ber-SN sama', [r4.updated, r4.failed.length], [0, 1]);
        t('traktornya utuh', [globalData[0].name, globalData[0].status], ['GGTRu_t1', 'Good']);
        t('alasan gagalnya menyebut kedua kelompok', /Heavy Equipment.*Agricultural Equipment/.test(r4.failed[0].reason), true);
        const tracFile = processData([{ 'Nickname': 'X', 'Serial Number': 'KMTu_h1', 'Display': 'Good' }],
                                     { fallbackGroup: 'tractor', fileGroup: 'tractor' });
        t('arah sebaliknya juga: berkas traktor tidak memperbarui alat berat', bulkUpdateUnitsFromCSV(tracFile.valid).updated, 0);
        const okFile = processData([{ 'Nickname': 'EXC-BARU', 'Serial Number': 'KMTu_h1', 'Camera AI': 'Breakdown' }],
                                   { fallbackGroup: 'heavy', fileGroup: 'heavy' });
        const r5 = bulkUpdateUnitsFromCSV(okFile.valid);
        t('berkas alat berat tetap memperbarui alat berat', [r5.updated, globalData[1].name, globalData[1].cameraAi],
          [1, 'EXC-BARU', 'Breakdown']);
        const plain = processData([{ 'Nickname': 'GGTR-UBAH', 'Serial Number': 'SNu_t1', 'Site': 'PT. MNM' }],
                                  { fallbackGroup: 'heavy', fileGroup: null });
        t('berkas tanpa kolom khas kelompok (hanya field bersama) tetap boleh', bulkUpdateUnitsFromCSV(plain.valid).updated, 1);

        window.__T = T;
    } catch (e) { window.__T = [{ n: 'EXCEPTION ' + e.message + ' ' + e.stack, pass: false }]; } })();` });

    await page.waitForFunction(() => window.__T, null, { timeout: 30000 });
    const T = await page.evaluate(() => window.__T);
    let pass = 0;
    T.forEach(x => { if (x.pass) pass++; else console.log('FAIL', x.n, '\n  dapat :', JSON.stringify(x.g), '\n  harap :', JSON.stringify(x.w)); });
    if (errors.length) console.log('PAGE ERRORS:', errors);
    console.log(`${pass}/${T.length} lulus`);
    await b.close();
    process.exit(pass === T.length && !errors.length ? 0 : 1);
})();
