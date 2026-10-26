// Menu samping desain F: satu tautan per halaman. Kelompok alat (Agricultural
// / Heavy) dipilih DI halaman; menu membuka halaman pada kelompok terakhir.
// Implements dan Licenses hanya untuk yang boleh melihat alat pertanian,
// Inspection hanya untuk alat berat; judul grup yang isinya tersembunyi semua
// ikut disembunyikan. Di HP: bar bawah (Home, Units, Damage, Inbox, More) dan
// tombol "Report damage" mengikuti hak akses yang sama.
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
        const label = g => { const l = g.querySelector('.nav-group__label'); return l ? l.textContent : '(top)'; };
        const shownGroups = () => [...document.querySelectorAll('.sidebar__nav .nav-group')]
            .filter(g => g.style.display !== 'none').map(label);
        const links = name => {
            const g = [...document.querySelectorAll('.sidebar__nav .nav-group')].find(x => label(x) === name);
            return g ? [...g.querySelectorAll('.nav__link')].filter(l => l.style.display !== 'none').map(l => l.querySelector('.nav__text').textContent) : [];
        };
        const active = () => [...document.querySelectorAll('.nav__link.active')].map(a => a.querySelector('.nav__text').textContent);
        const bar = () => [...document.querySelectorAll('.bottom-nav__item')].filter(el => !el.hidden).map(el => el.querySelector('span').textContent);
        const bell = () => document.getElementById('topbarBell');

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
        t('owner: semua grup', shownGroups(), ['(top)', 'Fleet', 'Operations', 'Admin']);
        t('atas: Dashboard dan Decision inbox', links('(top)'), ['Dashboard', 'Decision inbox']);
        t('Fleet: satu tautan per halaman', links('Fleet'), ['Units', 'Implements', 'Damage', 'Inspection', 'Licenses']);
        t('Operations: Team dan Warehouse', links('Operations'), ['Team', 'Warehouse']);
        t('Admin: Users, Master lists dan History', links('Admin'), ['Users', 'Master lists', 'History']);
        t('tidak ada lagi tautan per kelompok', document.querySelectorAll('.sidebar [data-group]').length, 0);
        t('badge tetap di tautannya', ['decisionBadge', 'inspectionBadge', 'teamBadge'].map(id => document.getElementById(id).closest('.nav__link').dataset.view), ['leader', 'inspection', 'team']);
        t('HP: bar bawah lengkap untuk owner', bar(), ['Home', 'Units', 'Damage', 'Inbox', 'More']);
        t('lonceng ke Decision inbox tampil untuk owner', bell().hidden, false);

        // =============== membuka halaman pada kelompoknya ===============
        goNav('editUnits', 'heavy');
        t('Units pada Heavy: halaman & penanda menu', [currentView, effectiveEditGroup(), active()], ['editUnits', 'heavy', ['Units']]);
        t('penanda: aria-current pada tautan aktif saja', [...document.querySelectorAll('.nav__link[aria-current="page"]')].map(a => a.dataset.view), ['editUnits']);
        goNav('editUnits', 'tractor');
        t('Units pada Agricultural', [effectiveEditGroup(), active()], ['tractor', ['Units']]);
        switchEditUnitsGroup('heavy');
        t('pindah kelompok di halaman: penanda tetap di Units', active(), ['Units']);
        goNav('dashboard', 'heavy');
        t('Dashboard pada Heavy', [effectiveDashGroup(), active()], ['heavy', ['Dashboard']]);
        navigateTo('warehouse');
        t('halaman lain: hanya tautannya yang menyala, judul HP ikut', [active(), document.getElementById('topbarTitle').textContent], [['Warehouse'], 'Warehouse']);
        document.querySelector('.nav__link[data-view="dashboard"]').click();
        t('tautan Dashboard membuka kelompok terakhir', [currentView, effectiveDashGroup()], ['dashboard', 'heavy']);
        goNav('dashboard', 'all');
        t('Dashboard Semua', [dashGroupPref, active()], ['all', ['Dashboard']]);

        goNav('damage', 'heavy');
        const body = () => document.getElementById('damageBody').textContent;
        t('Kerusakan Heavy: hanya alat berat (termasuk unit yang sudah dihapus)', [body().includes('EX1'), body().includes('EX-OLD'), body().includes('TR1'), active()],
          [true, true, false, ['Damage']]);
        t('saran unit di form = alat berat', [...document.querySelectorAll('#dmgUnitList option')].map(o => o.value.split(' ')[0]), ['EX1']);
        goNav('damage', 'tractor');
        t('Kerusakan Agricultural: hanya pertanian', [body().includes('TR1'), body().includes('EX1'), active()], [true, false, ['Damage']]);
        setDamageGroup('');
        t('Semua Kelompok: keduanya, penanda tetap di Damage', [body().includes('TR1'), body().includes('EX1'), active()], [true, true, ['Damage']]);
        document.querySelector('.bottom-nav__item[data-bnav="editUnits"]').click();
        t('bar bawah: Units membuka halaman dan menyala; More padam', [currentView, document.querySelector('.bottom-nav__item[data-bnav="editUnits"]').getAttribute('aria-current'), document.querySelector('.bottom-nav__more').classList.contains('active')], ['editUnits', 'page', false]);
        navigateTo('team');
        t('bar bawah: halaman tanpa tombol sendiri = More menyala', [document.querySelectorAll('.bottom-nav__item.active').length, document.querySelector('.bottom-nav__more').classList.contains('active')], [1, true]);

        // =============== per akun ===============
        as({ role: 'khl', access: { inspection: 'edit' } });
        t('teknisi alat berat: Fleet hanya Inspection', [shownGroups(), links('Fleet')], [['(top)', 'Fleet'], ['Inspection']]);
        t('dan dashboard-nya alat berat', effectiveDashGroup(), 'heavy');
        t('bar bawah: hanya Home dan More', bar(), ['Home', 'More']);
        t('tanpa akses Decision inbox: lonceng tersembunyi', bell().hidden, true);
        as({ role: 'staff', access: { implements: 'edit', licenseStock: 'view' } });
        t('staf pertanian: Implements dan Licenses, tanpa Inspection', links('Fleet'), ['Implements', 'Licenses']);
        t('dan dashboard-nya pertanian', effectiveDashGroup(), 'tractor');
        as({ role: 'khl', access: { teamLog: 'edit' } });
        t('akun Tim saja: Dashboard + Team, tanpa Fleet/Admin', [shownGroups(), links('(top)'), links('Operations')], [['(top)', 'Operations'], ['Dashboard'], ['Team']]);
        as({ role: 'staff', access: { history: 'view' } });
        t('History tetap terlihat walau bukan owner', links('Admin'), ['History']);
        as({ role: 'staff', access: { editUnits: 'view', damage: 'view', leader: 'view' } });
        t('lihat unit & kerusakan + kotak keputusan: bar bawah lengkap', bar(), ['Home', 'Units', 'Damage', 'Inbox', 'More']);
        t('lonceng tampil', bell().hidden, false);
        t('kerusakan lihat-saja: tombol Report damage tersembunyi', document.body.dataset.roDamage, '1');

        // =============== akun satu kelompok (unitScope) ===============
        as({ role: 'staff', access: { editUnits: 'view', damage: 'view', implements: 'edit', licenseStock: 'view', inspection: 'edit', unitScope: 'heavy' } });
        t('alat berat saja: Implements & Licenses tertutup, Inspection terbuka', links('Fleet'), ['Units', 'Damage', 'Inspection']);
        as({ role: 'staff', access: { editUnits: 'view', implements: 'edit', inspection: 'edit', unitScope: 'tractor' } });
        t('pertanian saja: Inspection tertutup', links('Fleet'), ['Units', 'Implements']);
        t('pertanian saja: tidak ada pilihan kelompok di menu', document.querySelectorAll('.sidebar [data-group], .bottom-nav [data-group]').length, 0);

        // =============== armada satu kelompok ===============
        as({ role: 'owner' });
        globalData = [globalData[0]]; saveToStorage(globalData); onDataLoaded();
        t('tanpa alat berat: menu tetap satu Dashboard', [...document.querySelectorAll('.nav__link[data-view="dashboard"]')].length, 1);
        t('Units dan Inspection tetap ada untuk owner (menambah unit pertama)', links('Fleet'), ['Units', 'Implements', 'Damage', 'Inspection', 'Licenses']);

        window.__T = T;
    } catch (e) { window.__T = [{ n: 'EXCEPTION ' + e.message + ' ' + e.stack, pass: false }]; } })();` });

    await page.waitForFunction(() => window.__T, null, { timeout: 60000 });
    const T = await page.evaluate(() => window.__T);

    // =============== HP 390px: bar bawah, tombol Report damage, dialog di atas bar ===============
    await page.setViewportSize({ width: 390, height: 844 });
    const phone = await page.evaluate(async () => {
        const tick = ms => new Promise(r => setTimeout(r, ms || 30));
        window.confirm = () => true;
        const out = {};
        const as = doc => { currentUser = { uid: 'u', email: 'u@x.id' }; currentUserDoc = { status: 'active', ...doc }; applyRoleGating(); };
        const shown = el => getComputedStyle(el).display !== 'none';
        const fab = document.getElementById('fabReportDamage');
        const barEl = document.getElementById('bottomNav');
        as({ role: 'owner' });
        navigateTo('dashboard'); await tick();
        out.owner = [shown(barEl), shown(fab)];
        const r = barEl.getBoundingClientRect();
        out.barAtBottom = Math.round(r.bottom) === innerHeight && r.height >= 64;
        // Nothing hides under the bar: the end of the page clears it.
        window.scrollTo(0, document.documentElement.scrollHeight); await tick();
        const main = document.querySelector('.main').getBoundingClientRect();
        out.mainClear = main.bottom <= r.top;
        out.noSideScroll = document.documentElement.scrollWidth <= innerWidth;
        fab.click(); await tick();
        out.fabOpens = document.getElementById('damageModal').classList.contains('open');
        out.fabHiddenInDialog = !shown(fab);
        const ov = document.getElementById('damageModal');
        out.dialogAboveBar = +getComputedStyle(ov).zIndex > +getComputedStyle(barEl).zIndex;
        closeDamageModal(); await tick();
        out.fabBack = shown(fab);
        as({ role: 'staff', access: { damage: 'view', editUnits: 'view' } }); await tick();
        out.viewOnly = shown(fab);
        as({ role: 'owner' }); openNav(); await tick();
        out.moreExpanded = [document.querySelector('.bottom-nav__more').getAttribute('aria-expanded'), document.querySelector('.topbar__burger').getAttribute('aria-expanded')];
        closeNav(); await tick(300);
        out.moreCollapsed = document.querySelector('.bottom-nav__more').getAttribute('aria-expanded');
        return out;
    });
    const t = (n, g, w) => T.push({ n, g, w, pass: JSON.stringify(g) === JSON.stringify(w) });
    t('HP: bar bawah dan tombol Report damage tampil untuk editor kerusakan', phone.owner, [true, true]);
    t('HP: bar menempel di bawah layar', phone.barAtBottom, true);
    t('HP: akhir halaman tidak tertutup bar', phone.mainClear, true);
    t('HP: tidak menggeser ke samping', phone.noSideScroll, true);
    t('HP: Report damage membuka form kerusakan', phone.fabOpens, true);
    t('HP: selama dialog terbuka tombolnya hilang, dialog di atas bar', [phone.fabHiddenInDialog, phone.dialogAboveBar], [true, true]);
    t('HP: dialog ditutup, tombol kembali', phone.fabBack, true);
    t('HP: kerusakan lihat-saja: tanpa tombol Report damage', phone.viewOnly, false);
    t('HP: More dan tombol menu menyatakan menu terbuka/tertutup', [phone.moreExpanded, phone.moreCollapsed], [['true', 'true'], 'false']);

    let pass = 0;
    T.forEach(x => { if (x.pass) pass++; else console.log('FAIL', x.n, '\n  dapat :', JSON.stringify(x.g), '\n  harap :', JSON.stringify(x.w)); });
    if (errors.length) console.log('PAGE ERRORS:', errors);
    console.log(`${pass}/${T.length} lulus`);
    await b.close();
    process.exit(pass === T.length && !errors.length ? 0 : 1);
})();
