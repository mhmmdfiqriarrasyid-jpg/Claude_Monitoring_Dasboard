// Unit Database: ringkasan jumlah unit per Implement (Agricultural) dan per
// Alat Kerja (Heavy), yang juga menjadi filter tabel.
const { launch, BASE_URL } = require('./_env');

(async () => {
    const b = await launch();
    const page = await b.newPage({ viewport: { width: 1440, height: 900 } });
    const errors = [];
    page.on('pageerror', e => { if (!/Chart is not defined/.test(e.message)) errors.push(e.message); });
    await page.goto(BASE_URL + '/index.html', { waitUntil: 'load' });
    await page.waitForTimeout(1000);

    await page.addScriptTag({ content: `(() => { try {
        const T = []; const t = (n, g, w) => T.push({ n, g, w, pass: JSON.stringify(g) === JSON.stringify(w) });
        window.showToast = () => {};
        window.cloud = { isReady: false };
        currentUser = { uid: 'o', email: 'o@x.id' }; currentUserDoc = { role: 'owner', status: 'active' };
        hideAuthGates(); applyRoleGating();
        const TR = (i, implement, extra) => ({ id: 't' + i, name: 'TR' + i, sn: 'ST' + i, status: 'Good', site: 'A',
            display: 'Good', gps: 'Good', steering: 'Good', jdlink: 'Good', implement, ...extra });
        const HV = (i, workTool) => ({ id: 'h' + i, name: 'EX' + i, sn: 'SH' + i, unitGroup: 'heavy', status: 'Good', workTool });
        const names = Array.from({ length: 10 }, (_, i) => 'Alat ' + i);
        globalData = [
            TR(1, 'Offset Harrow — Grizzly'), TR(2, 'offset harrow —  GRIZZLY'), TR(3, 'Offset Harrow — Grizzly', { site: 'B' }),
            TR(4, 'HDR Ripper — Gessner', { status: 'Breakdown' }), TR(5, ''), TR(6, '  '),
            ...names.map((n, i) => TR(10 + i, n)),
            HV(1, 'Root Plough'), HV(2, 'Root Plough'), HV(3, 'Root Rake'), HV(4, '')];
        saveToStorage(globalData);
        goNav('editUnits', 'tractor');

        const rows = () => [...document.querySelectorAll('#toolSummary .tool-row')].map(r => [
            r.querySelector('.tool-row__name').textContent.trim(), Number(r.querySelector('.tool-row__count').firstChild.textContent)]);
        let r = rows();
        t('judul Agricultural', document.querySelector('.tool-summary__title').textContent.trim(), 'Units per Implement');
        t('terbanyak di atas, ejaan beda huruf/spasi digabung', r[0], ['Offset Harrow — Grizzly', 3]);
        t('tanpa implement selalu tampil, paling bawah', r[r.length - 1], ['No implement', 2]);
        t('daftar diringkas 8 jenis + tombol lainnya', [r.length, document.querySelector('.tool-summary__more').textContent.trim()], [9, '+4 more types']);
        toggleToolSummary();
        t('tampilkan semua jenis', rows().length, 13);
        t('ringkasan angka', document.querySelector('.tool-summary__meta').textContent.trim().startsWith('14 of 16 units have an implement · 12 types'), true);

        // filter
        document.querySelector('#toolSummary .tool-row').click();
        const shown = () => [...document.querySelectorAll('#editBody tr')].map(tr => tr.querySelector('[data-field="name"]')?.textContent);
        t('klik baris memfilter tabel', shown(), ['TR1', 'TR2', 'TR3']);
        t('baris aktif ditandai', document.querySelector('#toolSummary .tool-row.is-on .tool-row__name').textContent.trim(), 'Offset Harrow — Grizzly');
        document.querySelector('#toolSummary .tool-row.is-on').click();
        t('klik lagi menampilkan semua', shown().length, 16);
        [...document.querySelectorAll('#toolSummary .tool-row')].find(b => /No implement/.test(b.textContent)).click();
        t('filter tanpa implement', shown(), ['TR5', 'TR6']);
        setEditToolFilter('');

        // status & site
        document.getElementById('editSiteFilter').value = 'B';
        renderEditTable();
        t('mengikuti filter site', rows(), [['Offset Harrow — Grizzly', 1]]);
        document.getElementById('editSiteFilter').value = '';
        document.getElementById('editStatusFilter').value = 'Breakdown';
        renderEditTable();
        t('mengikuti filter status', rows(), [['HDR Ripper — Gessner', 1]]);
        document.getElementById('editStatusFilter').value = '';

        // heavy
        setEditToolFilter(globalData[0] && toolKeyOf(globalData[0], 'tractor'));
        goNav('editUnits', 'heavy');
        t('pindah tab mereset filter', editToolFilter, '');
        t('judul Heavy', document.querySelector('.tool-summary__title').textContent.trim(), 'Units per Work Tool');
        t('Heavy: per alat kerja', rows(), [['Root Plough', 2], ['Root Rake', 1], ['No work tool', 1]]);
        document.querySelector('#toolSummary .tool-row').click();
        t('Heavy: filter alat kerja', shown(), ['EX1', 'EX2']);

        // XSS
        globalData.push(HV(9, '<img src=x onerror=window.__pwn=1>'));
        setEditToolFilter('');
        t('nama alat tidak menjadi markup', [document.querySelectorAll('#toolSummary img').length, window.__pwn || 0], [0, 0]);

        window.__T = T;
    } catch (e) { window.__T = [{ n: 'EXCEPTION ' + e.message + ' ' + e.stack, pass: false }]; } })();` });

    await page.waitForFunction(() => window.__T, null, { timeout: 60000 });
    const T = await page.evaluate(() => window.__T);
    let pass = 0;
    T.forEach(x => { if (x.pass) pass++; else console.log('FAIL', x.n, '\n  dapat :', JSON.stringify(x.g), '\n  harap :', JSON.stringify(x.w)); });
    if (errors.length) console.log('PAGE ERRORS:', errors);
    console.log(`${pass}/${T.length} lulus`);
    await b.close();
    process.exit(pass === T.length && !errors.length ? 0 : 1);
})();
