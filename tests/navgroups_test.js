// Menu samping per kelompok alat: Agricultural Equipment dan Heavy Equipment
// masing-masing punya Dashboard, Unit Database dan Kerusakan sendiri, yang
// membuka halaman bersama pada kelompok itu. Grup yang isinya tersembunyi
// semua ikut disembunyikan.
const { launch, BASE_URL } = require('./_env');

(async () => {
    const b = await launch();
    const page = await b.newPage({ viewport: { width: 1440, height: 900 } });
    const errors = [];
    page.on('pageerror', e => { if (!/Chart is not defined/.test(e.message)) errors.push(e.message); });
    await page.goto(BASE_URL + '/index.html', { waitUntil: 'load' });
    await page.waitForTimeout(1000);

    await page.addScriptTag({ content: `(() => { try {
        const T = []; const t = (n, g, w) => T.push({ n, g, w, pass: JSON.stringify(g) === JSON.stringify(w) });
        window.showToast = () => {};
        window.cloud = { isReady: false };
        const as = doc => { currentUser = { uid: 'u', email: 'u@x.id' }; currentUserDoc = { status: 'active', ...doc }; applyRoleGating(); };
        const groups = () => [...document.querySelectorAll('.sidebar__nav .nav-group')]
            .filter(g => g.style.display !== 'none').map(g => g.querySelector('.nav-group__label').textContent);
        const links = label => {
            const g = [...document.querySelectorAll('.sidebar__nav .nav-group')].find(x => x.querySelector('.nav-group__label').textContent === label);
            return g ? [...g.querySelectorAll('.nav__link')].filter(l => l.style.display !== 'none').map(l => [...l.childNodes].filter(n => n.nodeType === 3).map(n => n.textContent).join('').trim()) : [];
        };
        const active = () => { const a = document.querySelector('.nav__link.active'); return a ? a.closest('.nav-group').querySelector('.nav-group__label').textContent + ' / ' + a.textContent.trim() : null; };

        as({ role: 'owner' });
        hideAuthGates();
        globalData = [
            { id: 't1', name: 'TR1', sn: 'S1', status: 'Good', display: 'Good', gps: 'Good', steering: 'Good', jdlink: 'Good' },
            { id: 'h1', name: 'EX1', sn: 'S2', unitGroup: 'heavy', status: 'Good' }];
        globalDamages = [{ id: 'd1', unitId: 't1', unitName: 'TR1', date: '2026-10-01', damageType: 'Mekanis' },
                         { id: 'd2', unitId: 'h1', unitName: 'EX1', date: '2026-10-01', damageType: 'Mekanis', unitGroup: 'heavy' },
                         { id: 'd3', unitId: 'gone', unitName: 'EX-OLD', date: '2026-09-01', damageType: 'Mekanis', unitGroup: 'heavy' }];
        saveToStorage(globalData); saveDamages();
        onDataLoaded();

        // =============== susunan ===============
        t('owner: semua grup', groups(), ['Ringkasan', 'Agricultural Equipment', 'Heavy Equipment', 'Tim', 'Inventaris', 'Admin']);
        t('Agricultural: isi menu', links('Agricultural Equipment'), ['Dashboard', 'Unit Database', 'Implements', 'Kerusakan', 'Stok Lisensi']);
        t('Heavy: isi menu', links('Heavy Equipment'), ['Dashboard', 'Unit Database', 'Kerusakan', 'Pengecekan']);
        t('Admin: Users dan History', links('Admin'), ['Users', 'History']);

        // =============== membuka halaman pada kelompoknya ===============
        goNav('editUnits', 'heavy');
        t('Unit Database Heavy membuka tab Heavy', [currentView, effectiveEditGroup(), active()], ['editUnits', 'heavy', 'Heavy Equipment / Unit Database']);
        goNav('editUnits', 'tractor');
        t('Unit Database Agricultural membuka tab Agricultural', [effectiveEditGroup(), active()], ['tractor', 'Agricultural Equipment / Unit Database']);
        switchEditUnitsGroup('heavy');
        t('pindah tab di halaman ikut memindahkan penanda menu', active(), 'Heavy Equipment / Unit Database');
        goNav('dashboard', 'heavy');
        t('Dashboard Heavy', [effectiveDashGroup(), active()], ['heavy', 'Heavy Equipment / Dashboard']);
        goNav('dashboard', 'all');
        t('Dashboard Semua', [dashGroupPref, active()], ['all', 'Ringkasan / Dashboard Semua']);

        goNav('damage', 'heavy');
        const body = () => document.getElementById('damageBody').textContent;
        t('Kerusakan Heavy: hanya alat berat (termasuk unit yang sudah dihapus)', [body().includes('EX1'), body().includes('EX-OLD'), body().includes('TR1'), active()],
          [true, true, false, 'Heavy Equipment / Kerusakan']);
        t('saran unit di form = alat berat', [...document.querySelectorAll('#dmgUnitList option')].map(o => o.value.split(' ')[0]), ['EX1']);
        goNav('damage', 'tractor');
        t('Kerusakan Agricultural: hanya pertanian', [body().includes('TR1'), body().includes('EX1'), active()], [true, false, 'Agricultural Equipment / Kerusakan']);
        setDamageGroup('');
        t('Semua Kelompok: keduanya, tanpa penanda grup', [body().includes('TR1'), body().includes('EX1'), active()], [true, true, null]);

        // =============== per akun ===============
        as({ role: 'khl', access: { inspection: 'edit' } });
        t('teknisi alat berat: hanya grup Heavy', groups(), ['Heavy Equipment']);
        t('dan isinya Dashboard + Pengecekan', links('Heavy Equipment'), ['Dashboard', 'Pengecekan']);
        t('dashboard-nya alat berat', effectiveDashGroup(), 'heavy');
        as({ role: 'staff', access: { implements: 'edit', licenseStock: 'view' } });
        t('staf pertanian: tanpa grup Heavy', groups().includes('Heavy Equipment'), false);
        as({ role: 'khl', access: { teamLog: 'edit' } });
        t('akun Tim saja: tetap melihat kedua dashboard seperti sebelumnya', [links('Agricultural Equipment'), links('Heavy Equipment')], [['Dashboard'], ['Dashboard']]);
        t('Admin tersembunyi untuk bukan owner tanpa History', groups().includes('Admin'), false);
        as({ role: 'staff', access: { history: 'view' } });
        t('History tetap terlihat walau bukan owner', links('Admin'), ['History']);

        // =============== armada satu kelompok ===============
        as({ role: 'owner' });
        globalData = [globalData[0]]; saveToStorage(globalData); onDataLoaded();
        t('tanpa alat berat: tidak ada Dashboard Heavy / Semua', [links('Heavy Equipment').includes('Dashboard'), links('Ringkasan').includes('Dashboard Semua')], [false, false]);
        t('Unit Database Heavy tetap ada (untuk menambah unit pertama)', links('Heavy Equipment').includes('Unit Database'), true);

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
