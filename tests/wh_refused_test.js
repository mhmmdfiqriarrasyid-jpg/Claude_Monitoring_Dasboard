// 2.33.1 — Warehouse: the "rules are blocking" banner names what was refused,
// and goes away when that read later succeeds.
const { launch, BASE_URL } = require('./_env');
(async () => {
    const b = await launch();
    const page = await b.newPage({ viewport: { width: 1280, height: 900 } });
    const errors = [];
    page.on('pageerror', e => { if (!/Chart is not defined/.test(e.message)) errors.push(e.message); });
    await page.goto(BASE_URL + '/index.html', { waitUntil: 'load' });
    await page.waitForTimeout(800);
    const T = await page.evaluate(() => {
        const T = []; const t = (n, g, w) => T.push({ n, g, w, pass: JSON.stringify(g) === JSON.stringify(w) });
        window.cloud = { isReady: true, addHistoryEvents: () => Promise.resolve() };
        currentUser = { uid: 'o', email: 'o@x.id' }; currentUserDoc = { uid: 'o', role: 'owner', status: 'active' }; hideAuthGates(); applyRoleGating();
        navigateTo('warehouse');
        const banner = () => document.querySelector('.warehouse-rules-slot .category-rules-banner');
        showWarehouseRulesBanner('saving a fuel receipt photo');
        t('banner menyebut yang ditolak (bukan devices/stockItems)', [/saving a fuel receipt photo/.test(banner().textContent), /devices<\/code> and/.test(banner().innerHTML)], [true, false]);
        showWarehouseRulesBanner('reading stockItems');
        t('dua penolakan, satu banner', [document.querySelectorAll('.warehouse-rules-slot .category-rules-banner').length, /saving a fuel receipt photo · reading stockItems/.test(banner().textContent)], [1, true]);
        applyCloudStockSnapshot([]);
        t('stockItems terbaca lagi: hilang dari banner', [!!banner(), /reading stockItems/.test(banner().textContent)], [true, false]);
        clearWarehouseRefused('saving a fuel receipt photo');
        t('semua beres: banner hilang', !!banner(), false);
        return T;
    });
    let pass = 0;
    for (const x of T) { if (x.pass) pass++; console.log(`${x.pass ? 'PASS' : 'FAIL'}  ${x.n}`); if (!x.pass) console.log(`        dapat=${JSON.stringify(x.g)} harap=${JSON.stringify(x.w)}`); }
    console.log(`\n${pass}/${T.length} lulus`);
    console.log('PAGE ERRORS:', errors.length ? errors : 'none');
    await b.close();
    process.exit(pass === T.length && !errors.length ? 0 : 1);
})();
