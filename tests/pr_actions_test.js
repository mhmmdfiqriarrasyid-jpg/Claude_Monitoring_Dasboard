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
