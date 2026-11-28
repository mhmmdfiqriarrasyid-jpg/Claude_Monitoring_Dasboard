// 2.27.0 — Purchase Request: satuan barang (pcs, set, …) dari Master lists →
// Item Units. Kosong = sembilan satuan bawaan; satuan lain tetap bisa diketik.
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
        const saved = [];
        window.showToast = () => {}; window.confirm = () => true;
        window.cloud = { isReady: true, addHistoryEvents: () => Promise.resolve(), saveMasterList: (k, v) => { saved.push([k, v]); return Promise.resolve(); } };
        currentUser = { uid: 'p', email: 'p@x.id' };
        currentUserDoc = { uid: 'p', role: 'staff', status: 'active', access: { purchase: 'edit' } };
        hideAuthGates(); applyRoleGating();
        const opts = () => [...document.querySelectorAll('#prUomList option')].map(o => o.value);
        showAddPurchaseForm();
        t('daftar kosong: sembilan satuan bawaan', opts(), ['pcs', 'set', 'unit', 'box', 'pack', 'roll', 'm', 'kg', 'liter']);
        closePurchaseModal();
        applyMasterListsSnapshot([{ id: 'uom', values: ['pcs', 'drum', 'jerrycan'] }]);
        showAddPurchaseForm();
        t('daftar diisi: pilihan dari Master lists', opts(), ['pcs', 'drum', 'jerrycan']);
        const inp = document.querySelector('input[list="prUomList"]');
        t('isian satuan tetap teks bebas', [inp.tagName, inp.getAttribute('list')], ['INPUT', 'prUomList']);
        closePurchaseModal();
        t('tab Item Units tampil untuk editor purchase', masterTabs().includes('uom'), true);
        openMasterLists('uom');
        t('Master lists terbuka di Item Units, grup purchase', [_mlKind, !!document.querySelector('.ml-group[aria-label="Damage, warehouse & purchase"]')], ['uom', true]);
        closeMasterLists();
        // Satuan pertama yang ditambahkan tidak menghapus yang bawaan.
        applyMasterListsSnapshot([]);
        openMasterLists('uom');
        addMasterValue('drum');
        t('tambah pertama: bawaan ikut tersimpan', masterLists.uom, ['box', 'drum', 'kg', 'liter', 'm', 'pack', 'pcs', 'roll', 'set', 'unit']);
        closeMasterLists();
        currentUserDoc = { uid: 'p', role: 'staff', status: 'active', access: { warehouse: 'edit' } };
        applyRoleGating();
        t('tanpa akses purchase: tab tidak ada', masterTabs().includes('uom'), false);
        return T;
    });
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
