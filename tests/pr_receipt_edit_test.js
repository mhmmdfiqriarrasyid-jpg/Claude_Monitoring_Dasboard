// 2.32.1 — Purchase Request: a saved receipt can be corrected (quantities,
// date, note) or deleted; the items' received totals, the status and the
// Warehouse stock-in rows follow.
const { launch, BASE_URL } = require('./_env');
(async () => {
    const b = await launch();
    const page = await b.newPage({ viewport: { width: 1280, height: 900 } });
    const errors = [];
    page.on('pageerror', e => { if (!/Chart is not defined/.test(e.message)) errors.push(e.message); });
    await page.goto(BASE_URL + '/index.html', { waitUntil: 'load' });
    await page.waitForTimeout(800);
    const T = await page.evaluate(async () => {
        const T = []; const t = (n, g, w) => T.push({ n, g, w, pass: JSON.stringify(g) === JSON.stringify(w) });
        const tick = () => new Promise(r => setTimeout(r, 30));
        const toasts = []; window.showToast = m => toasts.push(String(m)); window.confirm = () => true;
        const ok = () => Promise.resolve();
        const stockSaved = [], stockDeleted = [];
        window.cloud = { isReady: true, addHistoryEvents: ok, savePurchaseRequest: ok, savePurchasePhoto: ok, deletePurchasePhoto: ok,
            saveStockItem: r => { stockSaved.push(JSON.parse(JSON.stringify(r))); const i = stockLedger.findIndex(x => x.id === r.id); if (i >= 0) stockLedger[i] = r; else stockLedger.push(r); return ok(); },
            deleteStockItem: id => { stockDeleted.push(id); stockLedger = stockLedger.filter(x => x.id !== id); return ok(); } };
        currentUser = { uid: 'o', email: 'o@x.id' }; currentUserDoc = { uid: 'o', role: 'owner', status: 'active' };
        hideAuthGates(); applyRoleGating();
        navigateTo('purchase');
        stockLedger = [];
        purchaseRequests = [{ id: 'pr1', prNo: 'PR/L/GPA1', title: 'Radio repeater', company: 'PT. GPA', status: 'po', poNo: 'PO-9', createdByUid: 'z',
            items: [{ id: 'a', spec: 'Hytera HR1068', qty: 2, uom: 'Unit', receivedQty: 0 }, { id: 'b', spec: 'Dipole Array Antenna', qty: 2, uom: 'Unit', receivedQty: 0 },
                    { id: 'c', spec: 'Feeder Clamp', qty: 40, uom: 'pcs', receivedQty: 0 }] }];
        // First delivery, recorded with a mistake: 40 clamps typed as 4, antenna 2 that never came.
        showPurchaseReceive('pr1');
        const set = (id, v) => { document.querySelector(`#prReceiveItems input[data-item="${id}"]`).value = v; };
        set('a', 2); set('b', 2); set('c', 4);
        document.getElementById('prReceiveLocation').value = 'Gudang GPA';
        savePurchaseReceipt(); await tick();
        let r = purchaseRequests[0];
        t('penerimaan pertama: partial, 3 baris stok', [r.status, r.items.map(i => i.receivedQty), stockLedger.length, stockLedger.every(s => s.purchaseItemId)], ['partial', [2, 2, 4], 3, true]);
        const rcId = r.receipts[0].id;
        // Detail shows an Edit receipt link.
        showPurchaseDetail('pr1');
        t('detail: tombol Edit receipt', !!document.querySelector('.pr-receipt__edit'), true);
        document.querySelector('.pr-receipt__edit').click(); await tick();
        t('form edit: isi dari penerimaan, maks = sisa + isi ini', [document.getElementById('prReceiveTitle').textContent.startsWith('Edit receipt'), document.getElementById('prReceiveSubmitLabel').textContent,
            [...document.querySelectorAll('#prReceiveItems input')].map(i => [i.value, i.max]), document.getElementById('prReceiveDeleteBtn').hidden, document.getElementById('prReceiveLocation').value],
            [true, 'Save changes', [['2', '2'], ['2', '2'], ['4', '40']], false, 'Gudang GPA']);
        set('b', 0); set('c', 41);
        savePurchaseReceipt(); await tick();
        t('lebih dari pesanan ditolak', [toasts.pop(), purchaseRequests[0].items.map(i => i.receivedQty)], ['"Feeder Clamp": at most 40 in this receipt (ordered 40)', [2, 2, 4]]);
        set('c', 40);
        document.getElementById('prReceiveNote').value = 'Koreksi: clamp 40, antena belum datang';
        savePurchaseReceipt(); await tick();
        r = purchaseRequests[0];
        t('dikoreksi: jumlah, status, catatan', [r.items.map(i => i.receivedQty), r.status, r.receipts.length, r.receipts[0].note, r.receipts[0].items.length], [[2, 0, 40], 'partial', 1, 'Koreksi: clamp 40, antena belum datang', 2]);
        t('stok ikut: clamp jadi 40, baris antena dihapus', [stockLedger.map(s => [s.itemName, s.qty]).sort(), stockDeleted.length, r.receipts[0].stockIds.length],
            [[['Feeder Clamp', 40], ['Hytera HR1068', 2]], 1, 2]);
        // Antenna arrives later: second receipt closes the request.
        showPurchaseReceive('pr1');
        t('penerimaan baru: sisa yang benar', [...document.querySelectorAll('#prReceiveItems input')].map(i => i.value), ['0', '2', '0']);
        savePurchaseReceipt(); await tick();
        t('semua diterima', purchaseRequests[0].status, 'received');
        // The second receipt was wrong after all: delete it.
        const rc2 = purchaseRequests[0].receipts[1].id;
        showPurchaseReceive('pr1', rc2);
        deletePurchaseReceipt(); await tick();
        r = purchaseRequests[0];
        t('hapus penerimaan: kembali partial, antena 0, stok antena dihapus', [r.status, r.receipts.length, r.items.map(i => i.receivedQty), stockLedger.length], ['partial', 1, [2, 0, 40], 2]);
        t('riwayat status mencatat koreksi', r.statusHistory.slice(-2).map(h => [h.status, h.note]), [['received', ''], ['partial', 'receipt corrected']]);
        // All quantities set to 0 in an edit = delete the receipt.
        showPurchaseReceive('pr1', rcId);
        set('a', 0); set('c', 0);
        savePurchaseReceipt(); await tick();
        r = purchaseRequests[0];
        t('semua 0 = hapus: kembali ke PO', [r.status, r.receipts.length, stockLedger.length], ['po', 0, 0]);
        return T;
    });
    let pass = 0;
    for (const x of T) { if (x.pass) pass++; console.log(`${x.pass ? 'PASS' : 'FAIL'}  ${x.n}`); if (!x.pass) console.log(`        dapat=${JSON.stringify(x.g)} harap=${JSON.stringify(x.w)}`); }
    console.log(`\n${pass}/${T.length} lulus`);
    console.log('PAGE ERRORS:', errors.length ? errors : 'none');
    await b.close();
    process.exit(pass === T.length && !errors.length ? 0 : 1);
})();
