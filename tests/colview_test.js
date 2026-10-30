// 2.10.4 — Units: tab kolom (Overview / Components / Licenses / All) supaya
// tabel tidak perlu digeser ke samping. Di ponsel (kartu) semua field tetap.
const { launch, BASE_URL } = require('./_env');

(async () => {
    const b = await launch();
    const page = await b.newPage({ viewport: { width: 1280, height: 800 } });
    const errors = [];
    page.on('pageerror', e => { if (!/Chart is not defined/.test(e.message)) errors.push(e.message); });
    await page.goto(BASE_URL + '/index.html', { waitUntil: 'load' });
    await page.waitForTimeout(1000);

    await page.addScriptTag({ content: `(async () => { try {
        const T = []; const t = (n, g, w) => T.push({ n, g, w, pass: JSON.stringify(g) === JSON.stringify(w) });
        const tick = ms => new Promise(r => setTimeout(r, ms || 30));
        try { localStorage.removeItem('unitsColView'); } catch (_) {}
        window.cloud = { isReady: true };
        currentUser = { uid: 'o', email: 'o@x.id' };
        currentUserDoc = { uid: 'o', role: 'owner', status: 'active', email: 'o@x.id' };
        hideAuthGates(); applyRoleGating();
        const seed = Array.from({ length: 6 }, (_, i) => ({ id: 'u' + i, name: 'GGTR1' + (10 + i) + 'G_HDG', model: '6110B', sn: 'SN' + i,
            implement: 'Bed Ripper — Gessner', status: 'Good', display: 'Good', gps: 'Good', steering: 'Good', jdlink: 'Good',
            site: 'PT. Global Papua Abadi', yearReceived: '2025', userCategory: 'Land Development',
            gpsLicense: 'SF-RTK', gpsExpiry: '2027-02-01', licenseDisplay: 'G5 Basic' }));
        globalData = seed.slice();
        navigateTo('editUnits'); globalData = seed.slice(); renderEditTable(); await tick();
        const heads = () => [...document.querySelectorAll('#editTable thead th')].filter(th => th.offsetWidth).map(th => th.textContent.trim()).filter(Boolean);
        const wrap = () => document.getElementById('editTable').closest('.table-wrapper');
        const fits = () => wrap().scrollWidth <= wrap().clientWidth + 2;
        t('tab kolom tampil', [...document.querySelectorAll('#unitsColView button')].map(x => x.textContent.trim()), ['Overview', 'Components', 'Licenses', 'All']);
        t('bawaan: Overview, muat tanpa geser', [document.querySelector('#unitsColView [aria-pressed="true"]').textContent.trim(), fits()], ['Overview', true]);
        t('Overview: kolom ringkas', heads(), ['No', 'Nickname', 'Model', 'Serial Number', 'Status', 'Site', 'User Category', 'Actions']);
        setUnitColView('components'); await tick();
        t('Components: komponen + muat', [heads(), fits()], [['No', 'Nickname', 'Status', 'Display', 'GPS', 'Steering', 'JDLink', 'Site', 'Actions'], true]);
        setUnitColView('licenses'); await tick();
        t('Licenses: lisensi + muat', [heads(), fits()], [['No', 'Nickname', 'Status', 'Site', 'GPS License', 'GPS Expiry', 'Display License', 'Display Expiry', 'Actions'], true]);
        setUnitColView('all'); await tick();
        t('All: semua kolom seperti dulu', heads().length, 20);
        t('pilihan diingat', localStorage.getItem('unitsColView'), 'all');
        setUnitColView('licenses');
        renderEditTable(); await tick();
        t('render ulang tetap di tab yang dipilih', heads().includes('GPS Expiry') && !heads().includes('Model'), true);
        t('baris tetap lengkap (data tidak dibuang)', document.querySelectorAll('#editBody tr')[0].children.length, 21);
        window.__T = T;
    } catch (e) { window.__T = [{ n: 'CRASH ' + e.message + ' ' + (e.stack || '').split('\\n')[1], pass: false }]; } })()` });

    await page.waitForFunction(() => window.__T, null, { timeout: 30000 });
    const T = await page.evaluate(() => window.__T);
    let pass = 0;
    for (const x of T) {
        if (x.pass) pass++;
        console.log(`${x.pass ? 'PASS' : 'FAIL'}  ${x.n}`);
        if (!x.pass && 'g' in x) console.log(`        dapat=${JSON.stringify(x.g)} harap=${JSON.stringify(x.w)}`);
    }
    console.log(`\n${pass}/${T.length} lulus`);
    console.log('PAGE ERRORS:', errors.length ? errors : 'none');
    await b.close();
    process.exit(pass === T.length && !errors.length ? 0 : 1);
})();
