// 2.9.0 — Purchase Request: form seperti sistem kantor (judul, PT, PR no.,
// tanggal, Normal/Urgent, CAPEX/Expense, item), status → PO → penerimaan
// (foto + Stock IN ke Warehouse), inbox keputusan, dan akses "Own only".
const { launch, BASE_URL } = require('./_env');

(async () => {
    const b = await launch();
    const page = await b.newPage({ viewport: { width: 1440, height: 1000 } });
    const errors = [];
    page.on('pageerror', e => { if (!/Chart is not defined/.test(e.message)) errors.push(e.message); });
    await page.goto(BASE_URL + '/index.html', { waitUntil: 'load' });
    await page.waitForTimeout(1000);

    await page.addScriptTag({ content: `(async () => { try {
        const T = []; const t = (n, g, w) => T.push({ n, g, w, pass: JSON.stringify(g) === JSON.stringify(w) });
        const tick = ms => new Promise(r => setTimeout(r, ms || 20));
        const toasts = []; window.showToast = m => toasts.push(String(m));
        window.confirm = () => true;
        const answers = []; window.prompt = () => answers.shift();
        const saved = [];
        const ok = () => Promise.resolve();
        window.cloud = { isReady: true, addHistoryEvents: ok,
            savePurchaseRequest: r => { saved.push(['pr', JSON.parse(JSON.stringify(r))]); return ok(); },
            deletePurchaseRequest: id => { saved.push(['delPr', id]); return ok(); },
            savePurchasePhoto: (id, owner) => { saved.push(['photo', id, owner]); return ok(); },
            deletePurchasePhoto: id => { saved.push(['delPhoto', id]); return ok(); },
            getPurchasePhoto: () => Promise.resolve('data:image/png;base64,AA'),
            saveStockItem: r => { saved.push(['stock', r]); return ok(); } };
        const lastPr = () => (saved.filter(x => x[0] === 'pr').pop() || [])[1];
        currentUser = { uid: 'o', email: 'o@x.id' };
        currentUserDoc = { uid: 'o', role: 'owner', status: 'active', email: 'o@x.id', displayName: 'Owner' };
        hideAuthGates(); applyRoleGating();
        masterLists.company = ['PT Mandiri Nusa Makmur', 'PT Agro Lestari'];
        purchaseRequests = [];

        navigateTo('purchase'); await tick();
        t('halaman Purchase tampil', document.getElementById('viewPurchase').style.display, 'block');
        t('daftar kosong menawarkan New request', /New request/.test(document.getElementById('prBody').textContent), true);

        // ---------- form ----------
        showAddPurchaseForm();
        t('pilihan PT dari daftar perusahaan', [...document.querySelectorAll('#prCompany option')].map(o => o.value).slice(1), ['PT Agro Lestari', 'PT Mandiri Nusa Makmur']);
        t('tanggal PR terisi hari ini', document.getElementById('prDate').value, toISODate());
        savePurchaseRequest({ preventDefault() {} });
        t('judul wajib', toasts.pop(), 'Fill in the title of the purchase request');
        document.getElementById('prTitle').value = 'Maintenance, Toolkit and Operational Kit OT';
        savePurchaseRequest({ preventDefault() {} });
        t('PT wajib', toasts.pop(), 'Pick the company');
        document.getElementById('prCompany').value = 'PT Mandiri Nusa Makmur';
        savePurchaseRequest({ preventDefault() {} });
        t('minimal satu item', toasts.pop(), 'Add at least one item with its specification');
        _prItems[0].spec = 'Kunci pas set 8-24mm'; _prItems[0].qty = '2'; _prItems[0].price = '150000';
        addPurchaseItemRow();
        t('baris item kedua', document.querySelectorAll('#prItems .pr-item:not(.pr-item--head)').length, 2);
        _prItems[1].spec = 'Kabel CAN 5m'; _prItems[1].qty = '4'; _prItems[1].uom = 'roll';
        updatePurchaseTotal();
        t('total perkiraan', document.getElementById('prTotal').textContent, 'Estimated total Rp 300.000');
        document.getElementById('prNo').value = 'PR/L/MNM2026100003';
        document.getElementById('prRequestType').value = 'Urgent';
        document.getElementById('prBudget').value = 'CAPEX';
        _whPh.prDoc.items.push({ id: '', src: 'data:image/png;base64,AA', isNew: true });
        savePurchaseRequest(null, 'draft');
        let r = lastPr();
        t('draf tersimpan dengan field kantor', [r.status, r.company, r.prNo, r.requestType, r.budgetType, r.items.length, r.items[1].uom, r.createdByUid],
          ['draft', 'PT Mandiri Nusa Makmur', 'PR/L/MNM2026100003', 'Urgent', 'CAPEX', 2, 'roll', 'o']);
        t('lampiran dokumen tersimpan ke purchasePhotos', [r.photoIds.length, saved.filter(x => x[0] === 'photo').length], [1, 1]);
        t('modal tertutup', document.getElementById('prModal').classList.contains('open'), false);
        t('baris tabel', /PR\\/L\\/MNM2026100003/.test(document.getElementById('prBody').textContent), true);

        // nomor PR ganda ditolak
        showAddPurchaseForm();
        document.getElementById('prTitle').value = 'X'; document.getElementById('prCompany').value = 'PT Agro Lestari';
        document.getElementById('prNo').value = 'pr/l/mnm2026100003'; _prItems[0].spec = 'Y';
        savePurchaseRequest({ preventDefault() {} });
        t('PR No. ganda ditolak', toasts.pop(), 'PR No. "pr/l/mnm2026100003" is already used');
        closePurchaseModal();

        // ---------- alur status ----------
        const id = r.id;
        showPurchaseDetail(id);
        const acts = () => [...document.querySelectorAll('#prDetailActions button')].map(x => x.textContent.trim());
        t('detail draf: Edit + Submit', acts().slice(0, 2), ['Edit', 'Submit']);
        t('detail memuat field kantor', ['Title purchase request', 'Company', 'PR No.', 'Request type', 'CAPEX / Expense'].every(k => document.getElementById('prDetailBody').textContent.includes(k)), true);
        setPurchaseStatus(id, 'submitted');
        t('diajukan', lastPr().status, 'submitted');
        t('badge sidebar: 1 menunggu', document.getElementById('purchaseBadge').textContent, '1');
        const g = decisionGroups().find(x => x.key === 'purchase');
        t('inbox: PR menunggu persetujuan', [g && g.total, g && g.tone, g && /waiting for approval/.test(g.items[0].sub)], [1, 'danger', true]);
        t('detail diajukan: Reject / Approve', acts().includes('Approve') && acts().includes('Reject'), true);
        answers.push('');
        setPurchaseStatus(id, 'rejected');
        t('tolak tanpa alasan ditahan', [toasts.pop(), lastPr().status], ['Write the reason first', 'submitted']);
        setPurchaseStatus(id, 'approved');
        answers.push('PO-7781', '2026/10/30');
        recordPurchasePO(id);
        t('tanggal PO salah format ditolak', toasts.pop(), 'Write the date as YYYY-MM-DD');
        answers.push('PO-7781', '2026-10-20');
        recordPurchasePO(id);
        r = lastPr();
        t('PO tercatat → status PO issued', [r.status, r.poNo, r.poDate], ['po', 'PO-7781', '2026-10-20']);
        t('riwayat status lengkap', r.statusHistory.map(h => h.status), ['draft', 'submitted', 'approved', 'po']);

        // ---------- penerimaan ----------
        showPurchaseReceive(id);
        const qty = () => [...document.querySelectorAll('#prReceiveItems input[data-item]')];
        t('form terima: sisa per item', qty().map(i => i.value), ['2', '4']);
        qty()[0].value = '3';
        savePurchaseReceipt({ preventDefault() {} });
        t('melebihi sisa ditolak', toasts.pop(), '"Kunci pas set 8-24mm": at most 2 left to receive');
        qty()[0].value = '2'; qty()[1].value = '1';
        document.getElementById('prReceiveLocation').value = 'Gudang Pusat';
        _whPh.prReceipt.items.push({ id: '', src: 'data:image/png;base64,AA', isNew: true }, { id: '', src: 'data:image/png;base64,AA', isNew: true });
        savePurchaseReceipt({ preventDefault() {} });
        r = lastPr();
        const stock = saved.filter(x => x[0] === 'stock').map(x => x[1]);
        t('terima sebagian', [r.status, r.items.map(i => i.receivedQty), r.receipts.length, r.receipts[0].photoIds.length], ['partial', [2, 1], 1, 2]);
        t('Stock IN ke Warehouse per item', stock.map(s => [s.txnType, s.itemName, s.qty, s.location, s.note, s.purchaseId]),
          [['IN', 'Kunci pas set 8-24mm', 2, 'Gudang Pusat', 'PR PR/L/MNM2026100003 · PO PO-7781', id], ['IN', 'Kabel CAN 5m', 1, 'Gudang Pusat', 'PR PR/L/MNM2026100003 · PO PO-7781', id]]);
        showPurchaseReceive(id);
        t('penerimaan kedua: item lunas terkunci', qty().map(i => [i.value, i.disabled]), [['0', true], ['3', false]]);
        document.getElementById('prReceiveToStock').checked = false;
        savePurchaseReceipt({ preventDefault() {} });
        r = lastPr();
        t('semua diterima → Received; tanpa stock tambahan', [r.status, r.receipts.length, saved.filter(x => x[0] === 'stock').length], ['received', 2, 2]);
        t('detail: bagian Goods received', /Goods received/.test(document.getElementById('prDetailBody').textContent), true);
        t('tidak lagi di inbox', (decisionGroups().find(x => x.key === 'purchase') || { total: 0 }).total, 0);

        // tertahan > 7 hari masuk inbox
        purchaseRequests.push({ id: 'pr_old', prNo: 'PR-OLD', title: 'Ban', company: 'PT Agro Lestari', status: 'approved', requestType: 'Normal', items: [],
            statusHistory: [{ status: 'approved', at: Date.now() - 9 * 86400000 }] });
        const g2 = decisionGroups().find(x => x.key === 'purchase');
        t('inbox: tertahan 9 hari', g2 && g2.items[0].sub, 'PT Agro Lestari · approved for 9 days');

        // ---------- filter ----------
        renderPurchaseView();
        document.getElementById('prStatusFilter').value = 'open'; renderPurchaseView();
        t('filter terbuka', document.getElementById('prCount').textContent, '1 request');
        document.getElementById('prStatusFilter').value = ''; document.getElementById('prTypeFilter').value = 'CAPEX'; renderPurchaseView();
        t('filter CAPEX', document.getElementById('prCount').textContent, '1 request');
        document.getElementById('prTypeFilter').value = '';
        document.getElementById('prSearch').value = 'kabel can'; renderPurchaseView();
        t('cari spesifikasi item', document.getElementById('prCount').textContent, '1 request');
        document.getElementById('prSearch').value = ''; renderPurchaseView();

        // hapus: foto dokumen + penerimaan ikut terhapus
        const nS = saved.length;
        deletePurchase(id);
        t('hapus: semua foto ikut', saved.slice(nS).filter(x => x[0] === 'delPhoto').length, 3);

        // ---------- akses Own only ----------
        currentUser = { uid: 'a', email: 'a@x.id' };
        currentUserDoc = { uid: 'a', role: 'staff', status: 'active', email: 'a@x.id', access: { purchase: 'own' } };
        applyRoleGating();
        const mine = { id: 'm1', createdByUid: 'a', status: 'draft', items: [] };
        const mineSub = { id: 'm2', createdByUid: 'a', status: 'submitted', items: [] };
        const other = { id: 'm3', createdByUid: 'z', status: 'draft', items: [] };
        t('own: boleh buat, ubah draf sendiri saja', [canEditPurchaseRecord(null), canEditPurchaseRecord(mine), canEditPurchaseRecord(mineSub), canEditPurchaseRecord(other)], [true, true, false, false]);
        t('own: tidak bisa approve/PO', canManagePurchase(), false);
        purchaseRequests = [mine, mineSub];
        showPurchaseDetail('m2');
        t('own: detail diajukan tanpa tombol aksi', acts(), ['Close']);
        t('own: tidak ada grup inbox', decisionGroups().some(x => x.key === 'purchase'), false);
        const subArgs = {};
        ['subscribeUnits', 'subscribeImplements', 'subscribeDamages', 'subscribeLicenses', 'subscribeMasterLists', 'subscribeDevices',
         'subscribeStockItems', 'subscribeTeamMembers', 'subscribeShifts', 'subscribeInspections', 'subscribeInspectionPlans',
         'subscribeLicenseRequirements', 'subscribeWorkLogs', 'subscribeLeaveRequests'].forEach(k => { window.cloud[k] = () => () => {}; });
        window.cloud.subscribePurchaseRequests = (cb, err, author) => { subArgs.pr = author; return () => {}; };
        cloudInitialized = false; initCloudSync(); await tick(20);
        t('own: langganan hanya PR sendiri', subArgs.pr, 'a');
        currentUserDoc = { uid: 'a', role: 'staff', status: 'active', email: 'a@x.id', access: { purchase: 'view' } };
        applyRoleGating(); cloudInitialized = false; initCloudSync(); await tick(20);
        t('view: langganan semua, tidak bisa membuat', [subArgs.pr, canEditPurchaseRecord(null)], ['', false]);
        currentUserDoc = { uid: 'a', role: 'staff', status: 'active', email: 'a@x.id', access: {} };
        applyRoleGating();
        t('tanpa akses: menu Purchase tersembunyi', canViewView('purchase'), false);

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
