// Implement wajib dari daftar Implements (bila daftarnya berisi): form unit,
// Bulk Edit, edit langsung di tabel, impor CSV dan Clean Up Values menolak
// nilai di luar daftar dan menyimpan ejaan daftar. Nilai lama di luar daftar
// tidak dipaksa hilang. Data Check bisa mencocokkan ejaan lama ke daftar.
const { launch, BASE_URL } = require('./_env');

(async () => {
    const b = await launch();
    const page = await b.newPage({ viewport: { width: 1440, height: 900 } });
    const errors = [];
    page.on('pageerror', e => { if (!/Chart is not defined/.test(e.message)) errors.push(e.message); });
    page.on('dialog', d => d.accept());
    await page.goto(BASE_URL + '/index.html', { waitUntil: 'load' });
    await page.waitForTimeout(1000);

    await page.addScriptTag({ content: `(async () => { try {
        const T = []; const t = (n, g, w) => T.push({ n, g, w, pass: JSON.stringify(g) === JSON.stringify(w) });
        const tick = ms => new Promise(r => setTimeout(r, ms || 20));
        const toasts = [];
        window.showToast = m => toasts.push(String(m));
        window.confirm = () => true;
        const lastToast = () => toasts[toasts.length - 1];
        window.cloud = { isReady: false };
        currentUser = { uid: 'o', email: 'o@x.id' }; currentUserDoc = { role: 'owner', status: 'active' };
        hideAuthGates(); applyRoleGating();
        const TR = (i, implement) => ({ id: 't' + i, name: 'TR' + i, sn: 'ST' + i, status: 'Good', site: 'A', model: '6110B',
            yearReceived: '2024', display: 'Good', gps: 'Good', steering: 'Good', jdlink: 'Good', implement });
        globalData = [TR(1, 'Bed Ripper — Gessner'), TR(2, 'Old Plough — Nobody'), TR(3, 'ISS — Final Cultivator'), TR(4, 'bed ripper - GESSNER')];
        saveToStorage(globalData);

        // =============== daftar kosong: bebas ===============
        globalImplements = [];
        t('daftar kosong: teks apa pun diterima', checkImplementValue('Apa Saja', null), { ok: true, value: 'Apa Saja' });

        globalImplements = [
            { id: 'i1', profileName: 'GS-BR', code: 'BR01', equipmentType: 'Bed Ripper', brand: 'Gessner' },
            { id: 'i2', profileName: 'ISS-FC', equipmentType: 'Final Cultivator', brand: 'ISS' },
            { id: 'i3', profileName: 'Tiller', equipmentType: 'Speed Tiller', brand: '' }];

        // =============== inti ===============
        t('ejaan daftar diterima apa adanya', checkImplementValue('Bed Ripper — Gessner', null), { ok: true, value: 'Bed Ripper — Gessner' });
        t('beda huruf & pemisah "-" → ejaan daftar', checkImplementValue('bed ripper - GESSNER', null), { ok: true, value: 'Bed Ripper — Gessner' });
        t('urutan terbalik → ejaan daftar', checkImplementValue('ISS — Final Cultivator', null), { ok: true, value: 'Final Cultivator — ISS' });
        t('jenis tanpa brand', checkImplementValue('speed tiller', null), { ok: true, value: 'Speed Tiller' });
        t('nama profil juga dikenali', checkImplementValue('GS-BR', null), { ok: true, value: 'Bed Ripper — Gessner' });
        t('di luar daftar ditolak', checkImplementValue('Zonal Ripper — Gessner', null).ok, false);
        t('nilai lama di luar daftar boleh dipertahankan', checkImplementValue('Old Plough — Nobody', 'Old Plough — Nobody'), { ok: true, value: 'Old Plough — Nobody' });
        t('kosong selalu boleh', checkImplementValue('', 'X'), { ok: true, value: '' });

        // =============== form unit ===============
        goNav('editUnits', 'tractor');
        showAddForm();
        const hint = () => document.getElementById('formImplementHint');
        const setImpl = v => { const el = document.getElementById('formImplement'); el.value = v; el.dispatchEvent(new Event('input')); };
        setImpl('Zonal Ripper — Gessner');
        t('hint memperingatkan nilai di luar daftar', [hint().classList.contains('form-hint--bad'), /not in the Implements list/.test(hint().textContent)], [true, true]);
        setImpl('bed ripper - gessner');
        t('hint: ada di daftar + disimpan sebagai', [hint().classList.contains('form-hint--ok'), hint().textContent], [true, 'In the Implements list · code BR01 · saved as "Bed Ripper — Gessner"']);
        document.getElementById('formName').value = 'NEW1'; document.getElementById('formSN').value = 'SN-NEW1';
        document.getElementById('formModel').value = '6110B';
        setImpl('Zonal Ripper — Gessner');
        saveUnit(new Event('submit'));
        t('form: di luar daftar ditolak, unit tidak dibuat', [globalData.some(u => u.sn === 'SN-NEW1'), /not in the Implements list/.test(lastToast())], [false, true]);
        setImpl('bed ripper - gessner');
        saveUnit(new Event('submit'));
        t('form: disimpan dengan ejaan daftar', (globalData.find(u => u.sn === 'SN-NEW1') || {}).implement, 'Bed Ripper — Gessner');

        // edit unit lama dengan nilai di luar daftar: field lain tetap bisa diubah
        editUnit('t2');
        t('hint untuk nilai lama', /Current value — not in the Implements list/.test(hint().textContent), true);
        document.getElementById('formSite').value = 'B';
        saveUnit(new Event('submit'));
        t('unit lama tetap bisa disimpan tanpa mengganti implement', [globalData.find(u => u.id === 't2').site, globalData.find(u => u.id === 't2').implement], ['B', 'Old Plough — Nobody']);
        editUnit('t2');
        setImpl('Another Unknown');
        saveUnit(new Event('submit'));
        t('tapi tidak bisa diganti ke nilai baru di luar daftar', globalData.find(u => u.id === 't2').implement, 'Old Plough — Nobody');
        closeModal();

        // =============== edit langsung di tabel ===============
        renderEditTable();
        const cell = id => document.querySelector('#editBody [data-id="' + id + '"][data-field="implement"]');
        cell('t1').textContent = 'Nope — Nobody';
        saveInlineEdit(cell('t1'));
        t('inline: di luar daftar dikembalikan', [globalData.find(u => u.id === 't1').implement, cell('t1').textContent], ['Bed Ripper — Gessner', 'Bed Ripper — Gessner']);
        cell('t1').textContent = 'speed tiller';
        saveInlineEdit(cell('t1'));
        t('inline: disimpan dengan ejaan daftar', globalData.find(u => u.id === 't1').implement, 'Speed Tiller');

        // =============== Bulk Edit ===============
        selectedUnitIds = new Set(['t1']);
        openBulkEdit();
        document.getElementById('bulkChkImplement').checked = true;
        document.getElementById('bulkImplement').value = 'Not Listed';
        applyBulkEdit();
        t('bulk: di luar daftar ditolak', globalData.find(u => u.id === 't1').implement, 'Speed Tiller');
        document.getElementById('bulkImplement').value = 'iss - final cultivator';
        applyBulkEdit();
        t('bulk: disimpan dengan ejaan daftar', globalData.find(u => u.id === 't1').implement, 'Final Cultivator — ISS');
        selectedUnitIds = new Set();

        // =============== Data Check: cocokkan ejaan ===============
        const items = dcImplementNotInList();
        t('Data Check: tak dikenal vs beda ejaan', items.map(i => [i.kind, i.label]),
          [['impl-tak-dikenal', 'Implement: Old Plough — Nobody'], ['impl-ejaan', 'Implement: ISS — Final Cultivator'], ['impl-ejaan', 'Implement: bed ripper - GESSNER']]);
        navigateTo('leader'); switchLeaderTab('check');
        t('tombol cocokkan tampil', document.getElementById('dataCheckImplFixBtn').style.display, '');
        matchImplementsToList();
        t('ejaan lama dicocokkan ke daftar', [globalData.find(u => u.id === 't3').implement, globalData.find(u => u.id === 't4').implement],
          ['Final Cultivator — ISS', 'Bed Ripper — Gessner']);
        t('yang tidak dikenal tidak disentuh', globalData.find(u => u.id === 't2').implement, 'Old Plough — Nobody');
        t('tombol hilang setelah beres', document.getElementById('dataCheckImplFixBtn').style.display, 'none');

        // =============== Clean Up Values ===============
        openValueMerge('implement', ['Old Plough — Nobody']);
        document.getElementById('vmTarget').value = 'Still Unknown';
        renderValueMerge();
        applyValueMerge();
        t('Clean Up: target harus ada di daftar', globalData.find(u => u.id === 't2').implement, 'Old Plough — Nobody');
        t('saran target = isi daftar', [...document.querySelectorAll('#vmTargetList option')].map(o => o.value),
          ['Bed Ripper — Gessner', 'Final Cultivator — ISS', 'Speed Tiller']);
        document.getElementById('vmTarget').value = 'speed tiller';
        renderValueMerge();
        applyValueMerge();
        t('Clean Up: disimpan dengan ejaan daftar', globalData.find(u => u.id === 't2').implement, 'Speed Tiller');
        closeValueMerge();

        // =============== impor CSV ===============
        navigateTo('editUnits');
        let report = null;
        const realReport = showImportReport, realPapa = window.Papa;
        window.showImportReport = r => { report = r; };
        window.Papa = { parse: (f, o) => o.complete({ data: [
            { 'Nickname': 'CSV1', 'Serial Number': 'SN-C1', 'Model': '6110B', 'Implement': 'iss — final cultivator' },
            { 'Nickname': 'CSV2', 'Serial Number': 'SN-C2', 'Model': '6110B', 'Implement': 'Unknown Thing' },
            { 'Nickname': 'TR1', 'Serial Number': 'ST1', 'Model': '6110B', 'Implement': 'Mystery' }],
            meta: { fields: ['Nickname', 'Serial Number', 'Model', 'Implement'] } }) };
        handleEditCSVImport(new File(['x'], 'units.csv'));
        window.showImportReport = realReport; window.Papa = realPapa;
        t('CSV: dikenal → ejaan daftar', (globalData.find(u => u.sn === 'SN-C1') || {}).implement, 'Final Cultivator — ISS');
        t('CSV: unit baru dengan nilai asing → tanpa implement', (globalData.find(u => u.sn === 'SN-C2') || {}).implement, '');
        t('CSV: update dengan nilai asing → implement lama tetap', globalData.find(u => u.sn === 'ST1').implement, 'Final Cultivator — ISS');
        t('CSV: dilaporkan', report && report.notes.filter(n => /not in the Implements list/.test(n.reason)).map(n => n.reason),
          ['Implement "Unknown Thing" is not in the Implements list — left empty', 'Implement "Mystery" is not in the Implements list — left unchanged']);

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
