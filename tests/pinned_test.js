// 2.50.0 — Pinned bars (tables.js twPin*, style.css "Pinned bars"): the search / filter bar of a
// list stays at the top while the page scrolls (tablet and desktop; not on a phone, where it
// would cover half the screen), with a shadow once stuck; a wide table scrolled sideways keeps
// the column that names its rows at the left; a table that pins its own columns is left alone.
const { launch, BASE_URL } = require('./_env');

(async () => {
    const b = await launch();
    const T = [];
    const t = (n, g, w) => T.push({ n, g, w, pass: JSON.stringify(g) === JSON.stringify(w) });
    const errors = [];
    const run = async (w, h) => {
        const page = await b.newPage({ viewport: { width: w, height: h } });
        page.on('pageerror', e => { if (!/Chart is not defined/.test(e.message)) errors.push(e.message); });
        await page.goto(BASE_URL + '/index.html', { waitUntil: 'load' });
        await page.waitForTimeout(700);
        const r = await page.evaluate(async () => {
            const wait = ms => new Promise(r => setTimeout(r, ms));
            window.showToast = () => {};
            currentUser = { uid: 'o', email: 'o@x.id' };
            currentUserDoc = { uid: 'o', role: 'owner', status: 'active', email: 'o@x.id' };
            document.getElementById('authGate').style.display = 'none';
            warehouseDevices = Array.from({ length: 80 }, (_, i) => ({ id: 'd' + i, sn: 'HT-' + i, type: 'HT', status: 'allocated', division: 'Agri', recipientName: 'P' + i, allocatedAt: '2026-11-01' }));
            navigateTo('warehouse'); renderDeviceTable(); await wait(300);
            const bar = document.querySelector('#viewWarehouse .adm-filters');
            const main = document.querySelector('.main');
            const own = getComputedStyle(main).overflowY !== 'visible' && main.scrollHeight > main.clientHeight;
            const edge = own ? main.getBoundingClientRect().top : document.querySelector('.topbar').getBoundingClientRect().bottom;
            if (own) main.scrollTop = 700; else window.scrollTo(0, 700);
            await wait(250);
            const out = { sticky: getComputedStyle(bar).position, gap: Math.round(bar.getBoundingClientRect().top - edge), stuck: bar.classList.contains('pin-stuck') };
            // a wide table: the naming column ("Unit") stays at the left
            const tb = document.getElementById('licenseTable'), wr = tb.closest('.table-wrapper');
            navigateTo('licenseStock'); await wait(200);
            tb.tBodies[0].innerHTML = Array.from({ length: 5 }, (_, i) => '<tr>' + Array.from({ length: tb.tHead.rows[0].cells.length }, (_, c) => `<td>row ${i} cell ${c} with long text</td>`).join('') + '</tr>').join('');
            wr.style.width = '480px'; wr.style.overflowX = 'auto';
            twPinAll(); await wait(50);
            wr.scrollLeft = 5000; await wait(50);
            const pin = tb.tBodies[0].querySelector('.tw-pin-col');
            out.pinHead = (tb.tHead.querySelector('.tw-pin-col') || {}).textContent || '';
            out.pinLeft = pin ? Math.round(pin.getBoundingClientRect().left - wr.getBoundingClientRect().left) : null;
            // the Units table pins its own columns: not touched
            out.unitsPins = document.querySelectorAll('#editTable .tw-pin-col').length;
            return out;
        });
        await page.close();
        return r;
    };
    const desk = await run(1366, 800);
    t('desktop: the filter bar stays flush at the top of the page', [desk.sticky, Math.abs(desk.gap) <= 2, desk.stuck], ['sticky', true, true]);
    t('desktop: no pinned naming column (tables fit)', desk.pinHead, '');
    const tab = await run(900, 1000);
    t('tablet: the filter bar stays under the top bar', [tab.sticky, Math.abs(tab.gap) <= 2, tab.stuck], ['sticky', true, true]);
    t('tablet: a wide table keeps "Unit" at the left', [tab.pinHead.trim(), tab.pinLeft], ['Unit', 0]);
    t('tablet: the Units table keeps its own pinned columns', tab.unitsPins, 0);
    const phone = await run(390, 800);
    t('phone: the filters scroll with the page (they would cover half the screen)', phone.sticky, 'static');

    let pass = 0;
    T.forEach(x => { if (x.pass) pass++; console.log(`${x.pass ? 'PASS' : 'FAIL'}  ${x.n}${x.pass ? '' : `\n        dapat=${JSON.stringify(x.g)} harap=${JSON.stringify(x.w)}`}`); });
    console.log(`\n${pass}/${T.length} lulus`);
    console.log(errors.length ? 'PAGE ERRORS: ' + errors.join(' | ') : 'PAGE ERRORS: none');
    await b.close();
    process.exit(pass === T.length && !errors.length ? 0 : 1);
})().catch(e => { console.error(e); process.exit(1); });
