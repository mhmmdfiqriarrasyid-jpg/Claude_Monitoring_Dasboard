// v165 — akses yang dibatasi ke satu kelompok unit (access.unitScope).
// Akun "Agricultural only" / "Heavy Equipment only" hanya melihat unit dan
// kerusakan kelompoknya di semua tempat, hanya bisa melihat (tidak mengedit)
// Unit Database dan Damage, dan menu kelompok lain tertutup. Diuji: nilai
// bawaan, pembatasan area, penyaringan data saat masuk, perubahan live,
// pencarian/profil/kotak keputusan/CSV/riwayat/cadangan, pemilih kelompok,
// cache perangkat, langganan, dialog Access, Warehouse, dan akun tanpa batas
// yang tidak berubah sama sekali.
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
        const toasts = []; window.showToast = m => toasts.push(String(m));
        let confirmAnswer = true; window.confirm = () => confirmAnswer;
        const blobs = []; URL.createObjectURL = b => { blobs.push(b); return 'blob:x'; }; URL.revokeObjectURL = () => {};
        HTMLAnchorElement.prototype.click = function () {};
        const ok = () => Promise.resolve();
        const subs = [];
        const accessWrites = [];
        window.cloud = { isReady: true, addHistoryEvents: ok, saveUnits: ok, updateUnitsFields: ok, saveDamage: ok, saveDamages: ok,
            deleteUnits: ok, getAllUnits: () => Promise.resolve([]), subscribeUsers: () => () => {},
            updateUserAccess: (uid, access) => { accessWrites.push([uid, access]); return ok(); },
            getAllHistory: () => Promise.resolve(window.__hist || []),
            saveDevice: ok };
        ['subscribeUnits', 'subscribeImplements', 'subscribeDamages', 'subscribeLicenses', 'subscribeInspections', 'subscribeInspectionPlans',
         'subscribeLicenseRequirements', 'subscribeMasterLists', 'subscribeDevices', 'subscribeStockItems', 'subscribeTeamMembers']
            .forEach(k => { window.cloud[k] = () => { subs.push(k); return () => {}; }; });
        const as = doc => { currentUser = { uid: doc.uid || 'u', email: 'x@x.id' }; currentUserDoc = Object.assign({ status: 'active' }, doc); applyRoleGating(); };
        localStorage.setItem('tractorDamagePhotosSplit', '1');
        hideAuthGates();

        // =============== nilai bawaan dan pembatasan area ===============
        t('tanpa unitScope = semua kelompok', effectiveUnitScope({ role: 'staff', access: { editUnits: 'edit' } }), 'all');
        t('owner tidak pernah dibatasi', effectiveUnitScope({ role: 'owner', access: { unitScope: 'heavy' } }), 'all');
        t('role lama "team" = semua', effectiveUnitScope({ role: 'team' }), 'all');
        t('nilai kosong / asing = tertutup', [effectiveUnitScope({ role: 'staff', access: { unitScope: '' } }), effectiveUnitScope({ role: 'staff', access: { unitScope: 'semua' } })], ['none', 'none']);
        const hvDoc = { uid: 'hv', role: 'staff', access: { editUnits: 'edit', damage: 'edit', implements: 'edit', licenseStock: 'view', inspection: 'edit', warehouse: 'edit', history: 'view', leader: 'view', csv: 'export', unitScope: 'heavy' } };
        const agDoc = { uid: 'ag', role: 'staff', access: { editUnits: 'view', damage: 'view', implements: 'edit', inspection: 'edit', leader: 'view', csv: 'export', unitScope: 'tractor' } };
        t('alat berat saja: unit & damage jadi lihat-saja, implement/lisensi tertutup, inspection tetap',
          ['editUnits', 'damage', 'implements', 'licenseStock', 'inspection', 'warehouse'].map(a => effectiveAccess(a, hvDoc)),
          ['view', 'view', 'none', 'none', 'edit', 'edit']);
        t('pertanian saja: inspection tertutup, implement tetap', ['inspection', 'implements'].map(a => effectiveAccess(a, agDoc)), ['none', 'edit']);
        t('grantedAccess menyimpan nilai aslinya', grantedAccess('editUnits', hvDoc), 'edit');

        // =============== data: owner melihat semua, persis seperti sebelumnya ===============
        const H = (id, x) => Object.assign({ id, name: 'EX-' + id, sn: 'SNH' + id, model: 'PC200', unitGroup: 'heavy', status: 'Good',
            cameraAi: 'Good', telematicBox: 'Good', switchLimiter: 'Good', rotaryLamp: 'Good', site: 'PT. GPA' }, x || {});
        const A = (id, x) => Object.assign({ id, name: 'TR-' + id, sn: 'SNT' + id, model: '6110B', status: 'Good', display: 'Good', gps: 'Good', steering: 'Good', jdlink: 'Good', site: 'PT. GPA' }, x || {});
        const units = [A('t1'), A('t2', { unitGroup: 'tractor', status: 'Breakdown', breakdownReason: 'x', breakdownStartedAt: Date.now() - 20 * 86400000 }),
                       A('t3', { unitGroup: 'Heavy Equip' }), H('h1'), H('h2', { status: 'Breakdown', breakdownReason: 'y', breakdownStartedAt: Date.now() - 20 * 86400000 })];
        const damages = [
            { id: 'd1', unitId: 't1', unitName: 'TR-t1', sn: 'SNTt1', date: '2026-10-01', damageType: 'Mekanis', resolved: false },
            { id: 'd2', unitId: 'h1', unitName: 'EX-h1', sn: 'SNHh1', date: '2026-10-02', damageType: 'Mekanis', unitGroup: 'heavy', resolved: false },
            { id: 'd3', unitId: 'h2', unitName: 'EX-h2', sn: 'SNHh2', date: '2026-10-03', damageType: 'Software', resolved: false },
            { id: 'd4', unitId: 'gone', unitName: 'X', sn: 'NOPE', date: '2026-10-04', damageType: 'Mekanis', unitGroup: 'heavy', resolved: true }
        ];
        as({ uid: 'o', role: 'owner' });
        navigateTo('dashboard');
        const snapUnits = JSON.parse(JSON.stringify(units));
        applyCloudUnitsSnapshot(snapUnits);
        applyCloudDamagesSnapshot(JSON.parse(JSON.stringify(damages)));
        t('owner: daftar unit adalah array snapshot itu sendiri (tidak diubah)', [globalData === snapUnits, globalData.length, globalDamages.length], [true, 5, 4]);
        const lsUnits = localStorage.getItem('tractorUnits');
        const ring = localStorage.getItem('tractorUnits_autobackup');
        const prefDash = localStorage.getItem('dashUnitGroup');

        // =============== alat berat saja ===============
        subs.length = 0;
        as(hvDoc);
        t('alat berat saja: hanya unit alat berat (kelompok tak dikenal disembunyikan)', globalData.map(u => u.id), ['h1', 'h2']);
        t('alat berat saja: hanya kerusakan alat berat (unit hilang → cap kelompoknya)', globalDamages.map(d => d.id), ['d2', 'd3', 'd4']);
        t('alat berat saja: tanda di body + label akun', [document.body.dataset.unitScope, /Heavy Equipment only/.test(document.getElementById('userMenuRoleLabel').textContent)], ['heavy', true]);
        t('alat berat saja: dashboard dan Unit Database pada kelompoknya', [effectiveDashGroup(), effectiveEditGroup()], ['heavy', 'heavy']);
        applyCloudUnitsSnapshot(JSON.parse(JSON.stringify(units)));
        t('snapshot berikutnya juga tersaring', globalData.map(u => u.id), ['h1', 'h2']);
        t('perangkat: daftar terbatas tidak pernah disimpan (cache & cincin cadangan tetap)',
          [localStorage.getItem('tractorUnits') === lsUnits, localStorage.getItem('tractorUnits_autobackup') === ring], [true, true]);
        // permukaan
        document.getElementById('globalSearch').value = 'TR-'; renderGlobalSearchResults();
        t('pencarian tidak menemukan unit pertanian', /No matching units/.test(document.getElementById('globalSearchResults').textContent), true);
        toasts.length = 0; showUnitProfile('t1');
        t('profil unit kelompok lain: "di luar akses Anda"', toasts.pop(), 'This unit is outside your access');
        const bd = decisionGroups().find(g => g.key === 'breakdown');
        t('Kotak Keputusan: hanya breakdown alat berat', bd ? bd.items.map(i => i.text) : null, ['EX-h2']);
        blobs.length = 0; exportEditCSV(); const csvU = blobs[0] ? await blobs[0].text() : '';
        t('CSV Unit Database hanya alat berat', [/SNHh1/.test(csvU), /SNTt1/.test(csvU)], [true, false]);
        blobs.length = 0; navigateTo('damage'); exportDamageCSV(); const csvD = blobs[0] ? await blobs[0].text() : '';
        t('CSV Damage hanya alat berat', [/EX-h1/.test(csvD), /TR-t1/.test(csvD)], [true, false]);
        t('pilihan kelompok di Damage disembunyikan, kelompok terkunci', [getComputedStyle(document.getElementById('damageGroupFilter')).display, damageGroup], ['none', 'heavy']);
        cloudHistory = [
            { id: 'a', timestamp: 5, unitId: 'h1', unitName: 'EX-h1', field: 'status' },
            { id: 'b', timestamp: 4, unitId: 't1', unitName: 'TR-t1', field: 'status' },
            { id: 'c', timestamp: 3, unitId: 'im1', unitName: '[Implement] Rotary', field: 'x' },
            { id: 'e', timestamp: 2, unitId: 'x', unitName: '[Inventory] GPS', field: 'x' },
            { id: 'f', timestamp: 1, unitId: 'gone', unitName: 'Hapus', field: 'x' }
        ];
        t('riwayat: hanya baris alat berat dan baris di luar kelompok unit', getAuditLog().map(e => e.id), ['a', 'e']);
        cloudHistory = [];
        toasts.length = 0; exportBackup();
        t('cadangan ditolak untuk akun terbatas', toasts.pop(), 'Backup is for accounts that see all unit groups');
        t('tidak bisa mengedit unit maupun kerusakan', [updateUnit('h1', { site: 'X' }), hasAccess('damage', 'edit'), hasAccess('editUnits', 'view')], [false, false, true]);
        // pemilih kelompok
        goNav('dashboard', 'tractor'); await tick(10);
        t('tautan dashboard pertanian tetap mendarat di alat berat, preferensi perangkat tidak berubah', [effectiveDashGroup(), dashGroupPref, localStorage.getItem('dashUnitGroup')], ['heavy', 'heavy', prefDash]);
        const navShown = g => [...document.querySelectorAll('.nav__link[data-group="' + g + '"]')].some(el => el.style.display !== 'none');
        t('sidebar: tautan pertanian dan "All" disembunyikan', [navShown('tractor'), navShown('all'), navShown('heavy')], [false, false, true]);
        t('menu Implements dan License Stock tertutup', [canViewView('implements'), canViewView('licenseStock')], [false, false]);
        // langganan
        cloudInitialized = false; initCloudSync(); await tick(20);
        t('langganan: tidak membaca implement, lisensi, syarat lisensi; tetap membaca pengecekan',
          [subs.includes('subscribeImplements'), subs.includes('subscribeLicenses'), subs.includes('subscribeLicenseRequirements'), subs.includes('subscribeInspections')],
          [false, false, false, true]);
        tearDownCloudSync();
        // Warehouse: perangkat terpasang di unit pertanian, diedit oleh editor gudang alat berat
        warehouseDevices = [{ id: 'dev1', sn: 'GPS-1', type: 'GPS', status: 'installed', unitId: 't1', unitName: 'TR-t1', siteName: '', createdAt: 1, updatedAt: 1 }];
        editDevice('dev1'); await tick(10);
        t('form perangkat menampilkan nama unit yang tidak terlihat', document.getElementById('devUnit').value, 'TR-t1');
        document.getElementById('devNote').value = 'dicek';
        let sentDev = null; window.cloud.saveDevice = r => { sentDev = r; return ok(); };
        toasts.length = 0;
        saveDevice({ preventDefault() {} }); await tick(10);
        t('menyimpan catatan perangkat tidak memutus tautan ke unit kelompok lain',
          [sentDev && sentDev.unitId, sentDev && sentDev.unitName, sentDev && sentDev.note, toasts.some(m => /not found/.test(m))], ['t1', 'TR-t1', 'dicek', false]);

        // =============== pertanian saja ===============
        subs.length = 0;
        as(agDoc);
        t('pertanian saja: unit pertanian (kelompok tak dikenal tetap disembunyikan)', globalData.map(u => u.id), ['t1', 't2']);
        t('pertanian saja: kerusakan pertanian', globalDamages.map(d => d.id), ['d1']);
        t('pertanian saja: menu Inspection tertutup, Implements terbuka', [canViewView('inspection'), canViewView('implements')], [false, true]);
        cloudInitialized = false; initCloudSync(); await tick(20);
        t('pertanian saja: tidak membaca pengecekan (walau Leader)', [subs.includes('subscribeInspections'), subs.includes('subscribeImplements')], [false, true]);
        tearDownCloudSync();

        // =============== kembali ke semua kelompok ===============
        as({ uid: 'o', role: 'owner' });
        t('owner lagi: semua unit kembali (dari salinan perangkat)', [globalData.length, globalDamages.length, document.body.dataset.unitScope], [5, 4, undefined]);

        // =============== temuan review v165 ===============
        // riwayat: unit bernama "[BU] …" tetap diputuskan oleh kelompok unitnya; daftar brand kelompok lain disembunyikan
        applyCloudUnitsSnapshot(JSON.parse(JSON.stringify(units)).concat([H('h9', { name: '[BU] EX-9' })]));
        as(agDoc);
        cloudHistory = [
            { id: 'r1', timestamp: 3, unitId: 'h9', unitName: '[BU] EX-9', field: 'status' },
            { id: 'r2', timestamp: 2, unitId: 'x', unitName: '[Master] Heavy Equipment Brands', field: 'values', after: 'Added "KOMATSUX"' },
            { id: 'r3', timestamp: 1, unitId: 'x', unitName: '[Master] Sites', field: 'values' }
        ];
        t('riwayat pertanian saja: unit "[BU] EX-9" dan daftar brand alat berat disembunyikan', getAuditLog().map(e => e.id).filter(id => /^r\\d$/.test(id)), ['r3']);
        cloudHistory = [];
        // keluar lalu "Back" di layar masuk, lalu akun terbatas yang sama masuk lagi: tetap tersaring
        currentUserDoc = null; applyScopeChange(); loadFromStorage(); loadDamages();
        as(agDoc);
        t('masuk lagi setelah keluar: tetap hanya pertanian', [globalData.every(u => !isHeavy(u)), globalDamages.map(d => d.id)], [true, ['d1']]);
        // pembatasan live menutup profil unit kelompok lain yang sedang terbuka
        as({ uid: 'o', role: 'owner' });
        showUnitProfile('h1'); await tick(10);
        as(agDoc);
        t('dibatasi saat profil unit alat berat terbuka: profil ditutup', document.getElementById('unitProfileModal').classList.contains('open'), false);
        // terbatas → semua: pilihan kelompok perangkat kembali, filter Damage tidak macet
        localStorage.setItem('dashUnitGroup', 'all');
        as(hvDoc); navigateTo('damage');
        as({ uid: 'o', role: 'owner' });
        t('kembali ke semua: Damage "All Groups" dan semua kerusakan tampil, dashboard ikut preferensi',
          [damageGroup, document.getElementById('damageGroupFilter').value, getFilteredDamages().length, effectiveDashGroup()], ['', '', globalDamages.length, 'all']);
        // akun tanpa batas: perangkat tertaut ke unit yang sudah dihapus tetap ditolak seperti dulu
        warehouseDevices = [{ id: 'dev2', sn: 'GPS-2', type: 'GPS', status: 'installed', unitId: 'deleted-unit', unitName: 'TR-old', siteName: '', createdAt: 1, updatedAt: 1 }];
        editDevice('dev2'); await tick(10);
        toasts.length = 0; let sent2 = null; window.cloud.saveDevice = r => { sent2 = r; return ok(); };
        saveDevice({ preventDefault() {} });
        t('owner: perangkat tertaut ke unit yang dihapus tetap ditolak', [sent2, /not found/.test(toasts.pop() || '')], [null, true]);
        closeDeviceModal();

        // =============== dialog Access ===============
        allUsers = [Object.assign({ email: 'hv@x.id', displayName: 'Hv', status: 'active' }, hvDoc), { uid: 'w', email: 'w@x.id', role: 'staff', status: 'active', access: { teamLog: 'edit' } }];
        openAccessModal('w'); await tick(10);
        const grid = document.getElementById('accessGrid');
        t('dialog: baris kelompok unit di atas, nilai sekarang "All groups"', [grid.querySelector('.access-select').dataset.area, grid.querySelector('[data-area="unitScope"]').value], ['unitScope', 'all']);
        applyAccessPreset('heavy');
        t('preset "View only — Heavy Equipment": kelompok + View pada unit, damage, inspection',
          ['unitScope', 'editUnits', 'damage', 'inspection', 'teamLog'].map(a => grid.querySelector('[data-area="' + a + '"]').value), ['heavy', 'view', 'view', 'view', 'edit']);
        grid.querySelector('[data-area="implements"]').value = 'edit'; refreshAccessScopeNote();
        const tagImpl = grid.querySelector('[data-row-area="implements"] .access-row__applies');
        t('dialog menyebut efek kelompok pada baris lain, tanpa mengubah nilainya',
          [tagImpl.hidden, tagImpl.textContent.trim(), grid.querySelector('[data-area="implements"]').value, document.getElementById('accessScopeNote').hidden], [false, '→ None with this group', 'edit', false]);
        await saveAccess();
        t('simpan: unitScope ikut tersimpan', accessWrites.length && accessWrites[0][1].unitScope, 'heavy');
        openAccessModal('hv'); await tick(10);
        t('dialog akun terbatas: nilai tersimpan yang tampil (Edit), bukan yang berlaku', [grid.querySelector('[data-area="unitScope"]').value, grid.querySelector('[data-area="editUnits"]').value], ['heavy', 'edit']);
        closeAccessModal();
        // nilai kelompok yang tidak dikenal: tampil apa adanya, tidak bisa disimpan sebelum dipilih
        allUsers.push({ uid: 'bad', email: 'bad@x.id', role: 'staff', status: 'active', access: { editUnits: 'view', unitScope: 'Heavy' } });
        openAccessModal('bad'); await tick(10);
        const scSel = grid.querySelector('[data-area="unitScope"]');
        t('dialog: nilai tersimpan tak dikenal tampil sebagai "No unit group"', [scSel.value, /No unit group \\(stored: Heavy\\)/.test(scSel.textContent), document.getElementById('accessScopeNote').hidden], ['', true, false]);
        accessWrites.length = 0; toasts.length = 0;
        await saveAccess();
        t('... dan tidak tersimpan sebelum kelompok dipilih', [accessWrites.length, toasts.pop()], [0, 'Pick a unit group first']);
        closeAccessModal();
        navigateTo('users'); renderUsersView(); await tick(10);
        t('tabel pengguna menyebut batas kelompok', /Heavy Equipment only/.test(document.querySelector('#viewUsers') ? document.querySelector('#viewUsers').textContent : document.body.textContent), true);

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
