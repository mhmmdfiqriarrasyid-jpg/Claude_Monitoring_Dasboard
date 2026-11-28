// 2.34.0 — a component can be "Not installed": not a fault (issues, health
// rates, damage links, weekly report) and shown grey.
const { launch, BASE_URL } = require('./_env');
(async () => {
    const b = await launch();
    const page = await b.newPage({ viewport: { width: 1366, height: 900 } });
    const errors = [];
    page.on('pageerror', e => { if (!/Chart is not defined/.test(e.message)) errors.push(e.message); });
    await page.goto(BASE_URL + '/index.html', { waitUntil: 'load' });
    await page.waitForTimeout(800);
    const T = await page.evaluate(() => {
        const T = []; const t = (n, g, w) => T.push({ n, g, w, pass: JSON.stringify(g) === JSON.stringify(w) });
        window.cloud = { isReady: true, addHistoryEvents: () => Promise.resolve() };
        currentUser = { uid: 'o', email: 'o@x.id' }; currentUserDoc = { uid: 'o', role: 'owner', status: 'active' }; hideAuthGates(); applyRoleGating();
        const H = (id, x) => Object.assign({ id, name: id, sn: 'S' + id, unitGroup: 'heavy', status: 'Good', cameraAi: 'Good', telematicBox: 'Good', switchLimiter: 'Good', rotaryLamp: 'Good', site: 'GPA' }, x);
        globalData = [H('BD018G', { cameraAi: 'Not installed' }), H('EX2', { cameraAi: 'Breakdown' }), H('EX3')];
        t('Not installed bukan masalah', [detectIssues(globalData[0]), detectIssues(globalData[1])], [[], ['Camera AI']]);
        t('kelas sel abu-abu', [compCellClass('Not installed'), compCellClass('Good'), compCellClass('Breakdown')], ['cell-na', 'cell-good', 'cell-bad']);
        t('Damage Camera AI pada unit tanpa Camera AI: tidak mengubah apa-apa', damageTargetField('Device Precision', 'Camera AI', globalData[0]), DAMAGE_DRIVES_NOTHING);
        // Unit form keeps the value.
        const sel = document.getElementById('formCameraAi');
        t('pilihan Not installed di form', [...sel.options].map(o => o.value), ['Good', 'Breakdown', 'Not installed']);
        setHealthSelect(sel, 'Not installed');
        t('form menampilkan Not installed', sel.value, 'Not installed');
        // Weekly report: rate over fitted units only.
        const D = buildWeeklyReport(wrWeekStart(), {});
        t('weekly report: Camera AI 1 dari 2 terpasang = 50%, unit tanpa Camera AI tetap all good', [D.precision.heavyComponents[0], D.precision.heavyPct], [['Camera AI', 50], 66.7]);
        return T;
    });
    let pass = 0;
    for (const x of T) { if (x.pass) pass++; console.log(`${x.pass ? 'PASS' : 'FAIL'}  ${x.n}`); if (!x.pass) console.log(`        dapat=${JSON.stringify(x.g)} harap=${JSON.stringify(x.w)}`); }
    console.log(`\n${pass}/${T.length} lulus`);
    console.log('PAGE ERRORS:', errors.length ? errors : 'none');
    await b.close();
    process.exit(pass === T.length && !errors.length ? 0 : 1);
})();
