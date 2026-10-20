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
        t('profil unit kelompok lain: "di luar akses Anda"', toasts.pop(), 'This unit is outside your access (or no longer exists)');
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

        // =============== v167: kelompok tersimpan di setiap catatan, bacaan per kelompok ===============
        as({ uid: 'o', role: 'owner' });
        applyCloudUnitsSnapshot(JSON.parse(JSON.stringify(units)));
        applyCloudDamagesSnapshot(JSON.parse(JSON.stringify(damages)));
        const added = addUnits([{ name: 'TR-new', sn: 'SNNEW1', model: '6110B', status: 'Good' }]);
        t('unit baru tanpa kelompok disimpan sebagai pertanian', [added.added, (globalData.find(u => u.sn === 'SNNEW1') || {}).unitGroup], [1, 'tractor']);
        // catatan kerusakan pertanian sekarang membawa kelompoknya
        let sentRec = null; window.cloud.saveDamage = r => { sentRec = r; return ok(); };
        showAddDamageForm(); document.getElementById('dmgUnit').value = damageUnitLabel(globalData.find(u => u.id === 't1'));
        onDamageUnitChanged(); document.getElementById('dmgType').value = 'Mekanis'; document.getElementById('dmgDescription').value = 'x';
        document.getElementById('dmgSetBreakdown').checked = false;
        saveDamage({ preventDefault() {} });
        t('kerusakan unit pertanian disimpan dengan kelompoknya', sentRec && sentRec.unitGroup, 'tractor');
        // baris riwayat membawa kelompoknya
        const pushed = [];
        let refuseGroupOnce = true;
        window.cloud.addHistoryEvents = ev => {
            if (refuseGroupOnce && ev.some(e => 'unitGroup' in e)) { refuseGroupOnce = false; return Promise.reject(Object.assign(new Error('denied'), { code: 'permission-denied' })); }
            pushed.push(...ev); return ok();
        };
        logEvent({ action: 'update', unitId: 'h1', unitName: 'EX-h1', field: 'site' });
        logEvent({ action: 'update', unitId: 'im1', unitName: '[Implement] Rotary', field: 'x' });
        logEvent({ action: 'update', unitId: 'w1', unitName: '[Team] Andi', field: 'x' });
        const lastLocal = JSON.parse(localStorage.getItem('tractorAuditLog') || '[]').slice(0, 3).map(e => e.unitGroup);
        t('riwayat: kelompok tiap baris (alat berat / pertanian / bersama)', lastLocal.reverse(), ['heavy', 'tractor', 'shared']);
        await tick(300);
        t('rules lama menolak kolom kelompok: baris dikirim lagi tanpa kolom itu, tidak hilang',
          [['EX-h1', '[Implement] Rotary', '[Team] Andi'].every(n => pushed.some(e => e.unitName === n)), pushed.every(e => !('unitGroup' in e))], [true, true]);
        // owner: catatan lama tanpa kelompok, disimpan sekali lewat halaman Users
        globalData.forEach(u => { if (u.id === 't1') delete u.unitGroup; });
        allUsers = [{ uid: 'hv', role: 'staff', status: 'active', access: { unitScope: 'heavy' } }];
        navigateTo('users'); renderUsersView(); await tick(10);
        const notice = document.getElementById('groupStampNotice');
        t('halaman Users: pemberitahuan catatan tanpa kelompok', [notice.hidden, /do not store their unit group yet/.test(notice.textContent), /such accounts exist now/.test(notice.textContent)], [false, true, true]);
        const unitParts = [], dmgParts = [];
        window.cloud.updateUnitsFields = items => { unitParts.push(...items); return ok(); };
        window.cloud.updateDamagesFields = items => { dmgParts.push(...items); return ok(); };
        window.cloud.updateHistoryFields = () => ok();
        await storeUnitGroups(); await tick(20);
        t('Store unit groups: hanya kolom kelompok yang dikirim',
          [unitParts.some(p => p.id === 't1' && p.unitGroup === 'tractor'), unitParts.every(p => Object.keys(p).sort().join() === 'id,unitGroup'),
           dmgParts.some(p => p.id === 'd1' && p.unitGroup === 'tractor'), dmgParts.every(p => Object.keys(p).sort().join() === 'id,unitGroup')],
          [true, true, true, true]);
        t('setelah disimpan: tinggal unit berkelompok tak dikenal, tanpa tombol simpan',
          [/do not store their unit group/.test(notice.textContent), /1 unit\\(s\\) have a group that is not recognised/.test(notice.textContent), !!notice.querySelector('button')], [false, true, false]);
        // akun terbatas meminta server hanya kelompoknya
        const subArgs = {};
        window.cloud.subscribeUnits = (cb, err, g) => { subArgs.units = g; return () => {}; };
        window.cloud.subscribeDamages = (cb, err, g) => { subArgs.damages = g; return () => {}; };
        window.cloud.subscribeHistory = (cb, err, max, groups) => { subArgs.history = groups; return () => {}; };
        let resyncArg; window.cloud.getAllUnits = g => { resyncArg = g; return Promise.resolve([]); };
        as(hvDoc);
        cloudInitialized = false; initCloudSync(); await tick(20);
        stopHistorySubscription(); startHistorySubscription();
        await resyncUnits().catch(() => {});
        t('akun terbatas: langganan unit, kerusakan, riwayat dan sinkron ulang hanya kelompoknya',
          [subArgs.units, subArgs.damages, subArgs.history, resyncArg], ['heavy', 'heavy', ['heavy', 'shared'], 'heavy']);
        tearDownCloudSync(); stopHistorySubscription();
        as({ uid: 'o', role: 'owner' });
        cloudInitialized = false; initCloudSync(); await tick(20);
        t('owner: langganan tanpa batas kelompok', [subArgs.units, subArgs.damages], [null, null]);
        tearDownCloudSync();
        // keluar & hapus data perangkat
        localStorage.setItem('tractorUnits', '[1]'); localStorage.setItem('tractorDamageRecords', '[1]'); localStorage.setItem('tractorUnits_autobackup', '[1]');
        let erased = false, reloaded = false, signedOut = false;
        window.cloud.signOutUser = () => { signedOut = true; return ok(); };
        window.cloud.eraseLocalCache = () => { erased = true; return Promise.resolve(true); };
        window.appReload = () => { reloaded = true; };
        await handleSignOutErase();
        t('Sign out & erase: keluar, cache unit/kerusakan/cadangan dihapus, database offline dihapus, halaman dimuat ulang',
          [signedOut, localStorage.getItem('tractorUnits'), localStorage.getItem('tractorDamageRecords'), localStorage.getItem('tractorUnits_autobackup'), erased, reloaded],
          [true, null, null, null, true, true]);

        // =============== v168: perbaikan review ===============
        as({ uid: 'o', role: 'owner' });
        // riwayat: dua batch sekaligus ditolak rules lama → keduanya dikirim ulang tanpa kolom kelompok
        _historyWithoutGroup = false;
        const got = [];
        window.cloud.addHistoryEvents = ev => new Promise((res, rej) => setTimeout(() => {
            if (ev.some(e => 'unitGroup' in e)) rej(Object.assign(new Error('denied'), { code: 'permission-denied' }));
            else { got.push(...ev); res(); }
        }, 120));
        logEvent({ action: 'x', unitName: '[Team] A', field: 'x' });
        await tick(100);
        logEvent({ action: 'x', unitName: '[Team] B', field: 'x' });
        await tick(700);
        t('riwayat: dua batch yang ditolak bersamaan sama-sama sampai', ['[Team] A', '[Team] B'].every(n => got.some(e => e.unitName === n)), true);
        // ditolak juga tanpa kolom: bukan karena kolomnya → baris berikutnya membawa kolom lagi
        let refuseAll = true; const got2 = [];
        window.cloud.addHistoryEvents = ev => refuseAll ? Promise.reject(Object.assign(new Error('denied'), { code: 'permission-denied' })) : (got2.push(...ev), ok());
        logEvent({ action: 'x', unitName: '[Team] C', field: 'x' }); await tick(200);
        refuseAll = false;
        logEvent({ action: 'x', unitName: '[Team] D', field: 'x' }); await tick(200);
        t('ditolak bukan karena kolom kelompok: baris berikutnya tetap membawanya', [_historyWithoutGroup, (got2.find(e => e.unitName === '[Team] D') || {}).unitGroup], [false, 'shared']);
        window.cloud.addHistoryEvents = ok;
        // riwayat lama: owner mengisi kelompok sekali (baris unit yang sudah dihapus dilewati)
        localStorage.removeItem('tractorHistoryGroupsStored_v1');
        window.cloud.getAllHistory = () => Promise.resolve([
            { id: 'o1', unitId: 'h1', unitName: 'EX-h1' }, { id: 'o2', unitId: 'x', unitName: '[Team] A' },
            { id: 'o3', unitId: 'gone', unitName: 'Lama' }, { id: 'o4', unitId: 't1', unitName: 'TR-t1', unitGroup: 'tractor' }]);
        const hparts = []; window.cloud.updateHistoryFields = items => { hparts.push(...items); return ok(); };
        await storeUnitGroups(); await tick(20);
        t('Store unit groups juga mengisi kelompok riwayat lama', hparts.map(p => p.id + ':' + p.unitGroup), ['o1:heavy', 'o2:shared']);
        t('... sekali per perangkat', localStorage.getItem('tractorHistoryGroupsStored_v1'), '1');
        // unit yang dihapus: baris riwayatnya tetap membawa kelompok unit itu
        const before = JSON.parse(localStorage.getItem('tractorAuditLog') || '[]').length;
        deleteUnits(['h2']); await tick(10);
        const delRow = JSON.parse(localStorage.getItem('tractorAuditLog') || '[]').find(e => e.action === 'delete' && e.unitId === 'h2');
        t('baris "unit dihapus" membawa kelompok unitnya', delRow && delRow.unitGroup, 'heavy');
        // akun terbatas: tautan ke unit yang tidak terlihat (kelompok lain) tetap dianggap di luar akses
        as(hvDoc);
        t('unitOutsideScope: akun terbatas → unit tak terlihat; tanpa batas → tidak pernah', [unitOutsideScope('t1'), unitOutsideScope('h1')], [true, false]);
        // perangkat: akun terbatas pertama kali → data perangkat dihapus dan halaman dimuat ulang, sekali
        localStorage.setItem('tractorDeviceScope', 'all'); localStorage.removeItem('tractorDeviceScopeTried');
        localStorage.setItem('tractorUnits', '[{"id":"t1"}]');
        let erasedP = 0, reloadedP = 0;
        window.cloud.eraseLocalCache = () => { erasedP++; return Promise.resolve(true); };
        window.appReload = () => { reloadedP++; };
        const first = await protectDeviceForScope('hv');
        const second = await protectDeviceForScope('hv');
        t('akun terbatas: data perangkat dibersihkan sekali, lalu tidak lagi',
          [first, second, erasedP, reloadedP, localStorage.getItem('tractorUnits'), localStorage.getItem('tractorDeviceScope')], [true, false, 1, 1, null, 'hv:heavy']);
        // gagal dibersihkan (tab lain terbuka): tidak berulang-ulang memuat ulang
        localStorage.setItem('tractorDeviceScope', 'all'); localStorage.removeItem('tractorDeviceScopeTried');
        window.cloud.eraseLocalCache = () => Promise.reject(Object.assign(new Error('other tab'), { code: 'failed-precondition' }));
        reloadedP = 0; toasts.length = 0;
        await protectDeviceForScope('hv');
        const again = await protectDeviceForScope('hv');
        t('tidak bisa dibersihkan: dimuat ulang sekali, lalu hanya diberi tahu', [reloadedP, again, toasts.some(m => /close the other tabs/.test(m))], [1, false, true]);

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
