// 2.27.1 — Purchase Request: tombol di jendela detail (Approve, Reject, PO,
// Receive, Cancel, Edit) benar-benar DIKLIK, bukan fungsinya dipanggil
// langsung — ID yang ter-escape dua kali membuat semuanya diam saja.
const { launch, BASE_URL } = require('./_env');
(async () => {
    const b = await launch();
    const page = await b.newPage({ viewport: { width: 1280, height: 900 } });
    const errors = [];
    page.on('pageerror', e => { if (!/Chart is not defined/.test(e.message)) errors.push(e.message); });
    await page.goto(BASE_URL + '/index.html', { waitUntil: 'load' });
    await page.waitForTimeout(800);
    await page.evaluate(() => {
        window.__saved = [];
        window.showToast = () => {}; window.confirm = () => true;
        window.prompt = (q, d) => /PO number/.test(q) ? 'PO-1' : /PO date/.test(q) ? '2026-11-01' : 'alasan';
        window.cloud = { isReady: true, addHistoryEvents: () => Promise.resolve(), savePurchaseRequest: r => { window.__saved.push(r.status); return Promise.resolve(); } };
        currentUser = { uid: 'o', email: 'o@x.id' }; currentUserDoc = { uid: 'o', role: 'owner', status: 'active' };
        hideAuthGates(); applyRoleGating();
        navigateTo('purchase');
        const pr = (id, status) => ({ id, prNo: 'PR/' + id, title: 'Radio "7/8" (x)', company: 'PT. GPA', status, createdByUid: 'z',
            items: [{ id: 'i1', spec: '7/8" RF Feeder', qty: 2, uom: 'pcs', receivedQty: 0 }] });
        purchaseRequests = [pr('pr_a', 'submitted'), pr("pr_b'x", 'submitted'), pr('pr_c', 'submitted')];
    });
    const T = [];
    const t = (n, g, w) => T.push({ n, g, w, pass: JSON.stringify(g) === JSON.stringify(w) });
    const click = async (id, label) => {
        await page.evaluate(id => showPurchaseDetail(id), id);
        await page.click(`#prDetailActions button:has-text("${label}")`);
        await page.waitForTimeout(100);
        return page.evaluate(id => purchaseRequests.find(r => r.id === id).status, id);
    };
    t('Approve diklik → approved', await click('pr_a', 'Approve'), 'approved');
    t('Record PO diklik → po', await click('pr_a', 'Record PO'), 'po');
    await page.evaluate(() => showPurchaseDetail('pr_a'));
    await page.click('#prDetailActions button:has-text("Receive goods")');
    t('Receive goods membuka form terima', await page.evaluate(() => document.getElementById('prReceiveModal').classList.contains('open')), true);
    await page.evaluate(() => closePurchaseReceive());
    t("Reject diklik (ID dengan tanda kutip) → rejected", await click("pr_b'x", 'Reject'), 'rejected');
    t('Cancel request diklik → cancelled', await click('pr_c', 'Cancel request'), 'cancelled');
    t('tersimpan ke cloud tiap langkah', await page.evaluate(() => window.__saved), ['approved', 'po', 'rejected', 'cancelled']);
    await page.evaluate(() => { purchaseRequests.push({ id: 'pr_d', prNo: 'PR/d', title: 'x', company: 'PT. GPA', status: 'draft', createdByUid: 'o', items: [{ id: 'i1', spec: 'a', qty: 1, uom: 'pcs' }] }); });
    await page.evaluate(() => showPurchaseDetail('pr_d'));
    await page.click('#prDetailActions button:has-text("Edit")');
    t('Edit diklik membuka form', await page.evaluate(() => document.getElementById('prModal').classList.contains('open')), true);
    // 2.28.0: Print checklist
    await page.evaluate(() => {
        window.__printed = 0; window.print = () => { window.__printed++; window.__during = document.body.classList.contains('printing-sheet'); };
        purchaseRequests.push({ id: 'pr_e', prNo: 'PR/e', title: 'Radio', company: 'PT. GPA', status: 'partial', poNo: 'PO-9', createdBy: 'admin.tech', createdByUid: 'z',
            items: [{ id: 'a', spec: 'Repeater <b>x</b>', qty: 3, uom: 'pcs', receivedQty: 1 }, { id: 'b', spec: 'ISR', qty: 1, uom: 'pcs', receivedQty: 1 }],
            receipts: [{ date: '2026-11-04', by: 'admin.tech@x.id', items: [{ itemId: 'a', qty: 1 }, { itemId: 'b', qty: 1 }], note: 'Delivery 1' }] });
        showPurchaseDetail('pr_e');
    });
    await page.click('#prDetailActions button:has-text("Print checklist")');
    await page.waitForTimeout(200);
    const P = await page.evaluate(() => {
        const rows = [...document.querySelectorAll('#printSheet .ps-items:not(.ps-hist) tbody tr')].map(tr => [...tr.cells].slice(3, 6).map(c => c.textContent.trim()));
        return { printed: window.__printed, during: window.__during, rows, hist: document.querySelectorAll('#printSheet .ps-hist tbody tr').length,
                 escaped: !document.querySelector('#printSheet .ps-items b:not(:only-child)') && /Repeater <b>x<\/b>/.test(document.getElementById('printSheet').textContent),
                 signs: document.querySelectorAll('#printSheet .ps-sign__box').length,
                 meta: Object.fromEntries([...document.querySelectorAll('#printSheet .ps-meta tr')].map(tr => [tr.cells[0].textContent, tr.cells[1].textContent.trim()])) };
    });
    await page.evaluate(() => window.dispatchEvent(new Event('afterprint')));
    t('Print checklist: dicetak sekali, hanya lembar ini', [P.printed, P.during], [1, true]);
    t('jumlah dipesan / sudah / sisa per item', P.rows, [['3', '1', '2'], ['1', '1', '0']]);
    t('riwayat penerimaan + 3 kolom tanda tangan', [P.hist, P.signs], [1, 3]);
    t('spesifikasi di-escape', P.escaped, true);
    t('status + progres + pengiriman', [P.meta.Status, P.meta.Progress, P.meta['Last delivery'], P.meta['This check']],
      ['Partially received', '1 of 2 items complete · 2 of 4 received (50%)', '2026-11-04 (delivery 1)', 'delivery 2']);
    t('setelah cetak kembali normal', await page.evaluate(() => document.body.classList.contains('printing-sheet')), false);
    t('tombol tidak ada untuk Submitted', await page.evaluate(() => { purchaseRequests.push({ id: 'pr_f', status: 'submitted', items: [] }); showPurchaseDetail('pr_f');
        return [...document.querySelectorAll('#prDetailActions button')].some(b => /Print checklist/.test(b.textContent)); }), false);
    t('tidak ada error halaman', errors, []);
    let pass = 0;
    for (const x of T) {
        if (x.pass) pass++;
        console.log(`${x.pass ? 'PASS' : 'FAIL'}  ${x.n}`);
        if (!x.pass) console.log(`        dapat=${JSON.stringify(x.g)} harap=${JSON.stringify(x.w)}`);
    }
    console.log(`\n${pass}/${T.length} lulus`);
    console.log('PAGE ERRORS:', errors.length ? errors : 'none');
    await b.close();
    process.exit(pass === T.length && !errors.length ? 0 : 1);
})();
