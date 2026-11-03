// 2.21.0 — Implement aktual per PT: jumlah unit dimiliki tiap PT, dibanding
// traktor yang memakainya (on tractor) dan sisanya (free).
const { launch, BASE_URL } = require('./_env');
(async () => {
    const b = await launch();
    const page = await b.newPage({ viewport: { width: 1366, height: 1000 } });
    const errors = [];
    page.on('pageerror', e => { if (!/Chart is not defined/.test(e.message)) errors.push(e.message); });
    await page.goto(BASE_URL + '/index.html', { waitUntil: 'load' });
    await page.waitForTimeout(800);
    const R = await page.evaluate(async () => {
        const out = {}; const pushed = []; const logs = [];
        window.showToast = () => {};
        window.cloud = { isReady: true, addHistoryEvents: () => Promise.resolve(), saveImplement: r => { pushed.push(JSON.parse(JSON.stringify(r))); return Promise.resolve(); } };
        currentUser = { uid: 'o', email: 'o@x.id' }; currentUserDoc = { uid: 'o', role: 'owner', status: 'active' };
        hideAuthGates(); applyRoleGating();
        navigateTo('implements');
        masterLists.site = ['PT. Global Papua Abadi', 'PT. Murni Nusantara Mandiri'];
        globalImplements = [
            { id: 'i1', profileName: 'Offset Harrow', brand: 'Grizzly', equipmentType: 'Offset Harrow', owned: { 'PT. Global Papua Abadi': 10, 'pt. murni nusantara mandiri': 4 } },
            { id: 'i2', profileName: 'Offset Harrow 2', brand: 'Baldan', equipmentType: 'offset harrow', owned: { 'PT. Global Papua Abadi': 2 } },
            { id: 'i3', profileName: 'Bed Ripper', brand: 'ISJ', equipmentType: 'Bed Ripper' }];
        globalData = [
            { id: 'u1', name: 'T1', site: 'PT. Global Papua Abadi', implement: 'Offset Harrow — Grizzly', status: 'Good' },
            { id: 'u2', name: 'T2', site: 'PT. Global Papua Abadi', implement: 'Offset Harrow — Baldan', status: 'Good' },
            { id: 'u3', name: 'T3', site: 'PT. Murni Nusantara Mandiri', implement: 'Bed Ripper — ISJ', status: 'Good' },
            { id: 'u4', name: 'T4', site: 'PT. Murni Nusantara Mandiri', status: 'Good' },
            { id: 'h1', name: 'EXC', site: 'PT. Global Papua Abadi', unitGroup: 'heavy', status: 'Good' }];
        renderImplementsTable();
        const rows = implementOwnedRows();
        out.sites = rows.sites;
        out.rows = rows.rows.map(r => [r.label, ...rows.sites.map(s => [r.per[s].owned, r.per[s].attached])]);
        out.short = [siteShort('PT. Global Papua Abadi'), siteShort('PT. Murni Nusantara Mandiri'), siteShort('Timika')];
        out.cards = [...document.querySelectorAll('.impl-own__card')].map(c => c.querySelector('.impl-own__n').textContent + ' | ' + c.querySelector('.impl-own__split').textContent.replace(/\s+/g, ' ').trim());
        out.negative = !!document.querySelector('#implOwnTable .impl-own__bad');
        out.unitsCell = document.querySelector('#implementBody td[data-label="Units"]').textContent.replace(/\s+/g, ' ').trim();
        // form
        editImplement('i3');
        out.inputs = [...document.querySelectorAll('#implOwnedBox input')].map(i => [i.dataset.site, i.value]);
        document.getElementById('implOwned_1').value = '3';
        document.getElementById('implOwned_0').value = '-2';
        saveImplement({ preventDefault() {} });
        const saved = globalImplements.find(d => d.id === 'i3');
        out.savedOwned = saved.owned;
        out.afterNegative = !!document.querySelector('#implOwnTable .impl-own__bad');
        return out;
    });
    const T = [];
    const t = (n, g, w) => T.push({ n, g, w, pass: JSON.stringify(g) === JSON.stringify(w) });
    t('PT dari Master lists → Sites', R.sites, ['PT. Global Papua Abadi', 'PT. Murni Nusantara Mandiri']);
    t('per jenis: dimiliki (gabungan profil sejenis, ejaan PT longgar) dan terpasang di traktor PT itu',
      R.rows, [['Offset Harrow', [12, 2], [4, 0]], ['Bed Ripper', [0, 0], [0, 1]]]);
    t('singkatan PT', R.short, ['GPA', 'MNM', 'Timika']);
    t('kartu per PT: dimiliki · terpasang · free', R.cards, ['12 | 2 on tractor · 10 free', '4 | 1 on tractor · 3 free']);
    t('lebih banyak terpasang daripada dimiliki → ditandai', R.negative, true);
    t('kolom Units di tabel', R.unitsCell, 'GPA 10 MNM 4');
    t('form: satu isian per PT', R.inputs, [['PT. Global Papua Abadi', ''], ['PT. Murni Nusantara Mandiri', '']]);
    t('simpan: angka negatif / kosong tidak disimpan', R.savedOwned, { 'PT. Murni Nusantara Mandiri': 3 });
    t('setelah diisi, tanda minus hilang', R.afterNegative, false);
    let pass = 0;
    T.forEach(x => { if (x.pass) pass++; console.log(`${x.pass ? 'PASS' : 'FAIL'}  ${x.n}${x.pass ? '' : `\n        dapat=${JSON.stringify(x.g)} harap=${JSON.stringify(x.w)}`}`); });
    console.log(`\n${pass}/${T.length} lulus`);
    console.log('PAGE ERRORS:', errors.length ? errors : 'none');
    await b.close();
    process.exit(pass === T.length && !errors.length ? 0 : 1);
})();
