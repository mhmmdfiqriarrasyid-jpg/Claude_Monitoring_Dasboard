// 2.26.0 — Implements: tab Profiles / Actual per PT (seperti Inspection), dan
// jumlah Owned diisi langsung di tabel Actual (Fill in counts → Save counts).
const { launch, BASE_URL } = require('./_env');
(async () => {
    const b = await launch();
    const page = await b.newPage({ viewport: { width: 1366, height: 1000 } });
    const errors = [];
    page.on('pageerror', e => { if (!/Chart is not defined/.test(e.message)) errors.push(e.message); });
    await page.goto(BASE_URL + '/index.html', { waitUntil: 'load' });
    await page.waitForTimeout(800);
    const T = await page.evaluate(async () => {
        const T = []; const t = (n, g, w) => T.push({ n, g, w, pass: JSON.stringify(g) === JSON.stringify(w) });
        const pushed = [], hist = [], toasts = [];
        window.showToast = m => toasts.push(String(m));
        window.confirm = () => true;
        window.cloud = { isReady: true, addHistoryEvents: e => { hist.push(...(Array.isArray(e) ? e : [e])); return Promise.resolve(); },
            saveImplement: r => { pushed.push(JSON.parse(JSON.stringify(r))); return Promise.resolve(); } };
        currentUser = { uid: 'o', email: 'o@x.id' }; currentUserDoc = { uid: 'o', role: 'owner', status: 'active' };
        hideAuthGates(); applyRoleGating();
        try { sessionStorage.removeItem('ot.implTab'); } catch (_) {}
        navigateTo('implements');   // loads the cached list first; the test data goes in after
        masterLists.site = ['PT. Global Papua Abadi', 'PT. Murni Nusantara Mandiri'];
        globalImplements = [
            { id: 'i1', profileName: 'Offset Harrow', brand: 'Grizzly', equipmentType: 'Offset Harrow', owned: { 'PT. Global Papua Abadi': 3 } },
            { id: 'i1b', profileName: 'Offset Harrow (2)', brand: 'Grizzly', equipmentType: 'Offset Harrow', owned: { 'PT. Global Papua Abadi': 2 } },
            { id: 'i2', profileName: 'Bed Ripper', brand: 'ISJ', equipmentType: 'Bed Ripper' }];
        globalData = [
            { id: 'u1', name: 'T1', site: 'PT. Global Papua Abadi', implement: 'Offset Harrow — Grizzly', status: 'Good' },
            { id: 'u2', name: 'T2', site: 'PT. Murni Nusantara Mandiri', implement: 'Subsoiler — Nobody', status: 'Good' }];
        renderImplementsTable();
        const vis = id => document.getElementById(id).style.display !== 'none';
        t('awal: tab Profiles', [vis('implProfilesPanel'), vis('implActualPanel'), document.querySelector('.impl-tab.active').dataset.tab], [true, false, 'profiles']);
        switchImplementTab('actual');
        t('tab Actual per PT', [vis('implProfilesPanel'), vis('implActualPanel'), document.querySelector('.impl-tab.active').dataset.tab], [false, true, 'actual']);
        t('tombol Fill in counts tampil untuk editor', document.getElementById('implOwnEditBtn').style.display, '');
        t('sebelum isi: Bed Ripper (0/0) tidak tampil', [...document.querySelectorAll('#implOwnTable tbody tr')].map(r => r.cells[0].textContent.trim()), ['Offset Harrow', 'Subsoiler']);

        startImplementOwnedEdit();
        const inputs = () => [...document.querySelectorAll('#implOwnTable input[data-site]')];
        t('mode isi: semua profil tampil, Subsoiler tanpa profil tanpa input',
          [[...document.querySelectorAll('#implOwnTable tbody tr')].map(r => r.cells[0].textContent.trim().replace(/\s+/g, ' ')), inputs().length],
          [['Bed Ripper', 'Offset Harrow', 'Subsoiler · no profile'], 4]);
        t('nilai awal = jumlah tersimpan (3 + 2)', inputs().map(i => i.value), ['', '', '5', '']);
        // Data yang masuk saat mengetik tidak menghapus isian.
        const off = inputs().find(i => i.dataset.row.startsWith('offset') && /Global/.test(i.dataset.site));
        off.value = '7'; _implOwnLive(off);
        const freeCell = off.closest('tr').querySelector('td[data-free="PT. Global Papua Abadi"]');
        t('Free ikut berubah saat mengetik (7 − 1)', freeCell.textContent, '6');
        renderImplementsTable();
        document.dispatchEvent(new Event('ot:data'));
        t('redraw saat mode isi: isian tetap', inputs().find(i => i === off || (i.dataset.row === off.dataset.row && i.dataset.site === off.dataset.site)).value, '7');
        const bed = inputs().find(i => i.dataset.row.startsWith('bed') && /Murni/.test(i.dataset.site));
        bed.value = '4';
        saveImplementOwnedEdit();
        const get = id => globalImplements.find(d => d.id === id).owned;
        t('tersimpan: profil pertama memegang jumlah, profil kembar dikosongkan', [get('i1'), get('i1b'), get('i2')],
          [{ 'PT. Global Papua Abadi': 7 }, {}, { 'PT. Murni Nusantara Mandiri': 4 }]);
        t('dikirim ke cloud 3 profil', pushed.map(p => p.id).sort(), ['i1', 'i1b', 'i2']);
        t('keluar mode isi + pesan', [inputs().length, toasts.pop()], [0, 'Counts saved (3 implement profiles)']);
        t('kartu GPA: 7 owned', document.querySelector('#implOwnCards .impl-own__n').textContent, '7');
        startImplementOwnedEdit();
        saveImplementOwnedEdit();
        t('simpan tanpa perubahan: tidak ada yang dikirim', [pushed.length, toasts.pop()], [3, 'No count changed']);

        // Viewer: tidak bisa mengisi.
        currentUserDoc = { uid: 'v', role: 'viewer', status: 'active', access: { implements: 'view' } };
        currentUser = { uid: 'v', email: 'v@x.id' };
        applyRoleGating(); renderImplementOwned();
        t('viewer: tombol Fill in counts disembunyikan', document.getElementById('implOwnEditBtn').style.display, 'none');
        currentUser = { uid: 'o', email: 'o@x.id' }; currentUserDoc = { uid: 'o', role: 'owner', status: 'active' }; applyRoleGating();

        // Tab diingat saat kembali ke halaman.
        navigateTo('dashboard'); navigateTo('implements');
        t('tab diingat', document.querySelector('.impl-tab.active').dataset.tab, 'actual');
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
