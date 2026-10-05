// Kualitas data: (1) Clean Up Values — gabungkan ejaan yang maksudnya sama,
// semua record ikut berubah, bisa Undo; (2) Data Check — ejaan mirip, field
// kunci kosong, implement yang tidak ada di daftar Implements; (3) kartu
// Data Completeness di Unit Database dengan chip yang memfilter tabel.
const { launch, BASE_URL } = require('./_env');

(async () => {
    const b = await launch();
    const page = await b.newPage({ viewport: { width: 1440, height: 900 } });
    const errors = [];
    page.on('pageerror', e => { if (!/Chart is not defined/.test(e.message)) errors.push(e.message); });
    await page.goto(BASE_URL + '/index.html', { waitUntil: 'load' });
    await page.waitForTimeout(1000);

    await page.addScriptTag({ content: `(async () => { try {
        const T = []; const t = (n, g, w) => T.push({ n, g, w, pass: JSON.stringify(g) === JSON.stringify(w) });
        const tick = ms => new Promise(r => setTimeout(r, ms || 20));
        const unitBatches = [], memberBatches = [], toasts = [], history = [];
        let confirmAnswer = true, confirmText = '';
        window.cloud = { isReady: true, addHistoryEvents: () => Promise.resolve(),
            saveUnits: list => { unitBatches.push(list.map(u => u.id)); return Promise.resolve(); },
            saveTeamMembers: list => { memberBatches.push(list.map(m => m.id + '=' + m.company)); return Promise.resolve(); } };
        window.showToast = m => toasts.push(String(m));
        window.confirm = m => { confirmText = String(m); return confirmAnswer; };
        const realLog = logEvent; window.logEvent = e => { history.push(e); };
        const as = doc => { currentUser = { uid: 'u', email: 'u@x.id' }; currentUserDoc = { status: 'active', ...doc }; applyRoleGating(); };
        as({ role: 'owner' });
        hideAuthGates();
        const TR = (i, implement, extra) => ({ id: 't' + i, name: 'TR' + i, sn: 'ST' + i, status: 'Good', site: 'PT. GPA',
            model: '6110B', yearReceived: '2024', display: 'Good', gps: 'Good', steering: 'Good', jdlink: 'Good', implement, ...extra });
        const HV = (i, extra) => ({ id: 'h' + i, name: 'EX' + i, sn: 'SH' + i, unitGroup: 'heavy', status: 'Good', site: 'PT. GPA',
            brand: 'Komatsu', model: 'PC200', machineType: 'Excavator', assetCode: 'LB-' + i, workTool: 'Bucket', yearReceived: '2025', ...extra });
        globalData = [
            TR(1, 'Zonal Ripper — Gessner'), TR(2, 'Zonal Ripper — Gessner'), TR(3, 'Zonal Ripper — Geesner'),
            TR(4, 'Bed Ripper — Gessner'), TR(5, 'ISS — Final Cultivator'), TR(6, 'Final Cultivator — ISS'),
            TR(7, '', { yearReceived: '' }), TR(8, 'Speed Tiller', { model: '6120B' }), TR(9, 'Bed Ripper — ISJ'),
            TR(10, 'Spring Grubber — ISS'),
            HV(1), HV(2, { workTool: 'Buket' }), HV(3, { workTool: '' })];
        globalImplements = [];
        teamMembers = [{ id: 'm1', name: 'A', company: 'PT. Global Papua Abadi', active: true },
                       { id: 'm2', name: 'B', company: 'PT Global Papua Abadi', active: true },
                       { id: 'm3', name: 'C', company: 'PT. Murni Nusantara Mandiri', active: true }];
        saveToStorage(globalData);

        // =============== kemiripan ejaan ===============
        t('Gessner ~ Geesner', similarSpelling('Gessner', 'Geesner'), true);
        t('PT. GPA ~ PT GPA (hanya tanda baca)', similarSpelling('PT. GPA', 'PT GPA'), true);
        t('ISJ ≠ ISS (nama pendek harus sama persis)', similarSpelling('ISJ', 'ISS'), false);
        t('6110B ≠ 6120B (angka beda = model beda)', similarSpelling('6110B', '6120B'), false);
        t('Offset Harrow ≠ Offset Harrow 32', similarSpelling('Offset Harrow', 'Offset Harrow 32'), false);
        t('Bucket ~ Buket', similarSpelling('Bucket', 'Buket'), true);

        // =============== (3) kartu kelengkapan ===============
        goNav('editUnits', 'tractor');
        const dq = () => document.getElementById('dataQuality');
        const chips = () => [...dq().querySelectorAll('.dq-chip')].map(c => c.textContent.trim().replace(/\\s+/g, ' '));
        t('persen kelengkapan Agricultural (38 dari 40 field)', dq().querySelector('.dq__pct').textContent, '95%');
        t('unit lengkap', /9 of 10 unit\\(s\\) have every key field/.test(dq().textContent.replace(/\\s+/g, ' ')), true);
        t('chip per field yang kosong', chips(), ['1 no implement', '1 no year received']);
        const shown = () => [...document.querySelectorAll('#editBody tr')].map(tr => tr.querySelector('[data-field="name"]')?.textContent).filter(Boolean);
        dq().querySelector('.dq-chip').click();
        t('klik chip memfilter tabel ke unit yang kosong', shown(), ['TR7']);
        t('chip aktif ditandai', dq().querySelector('.dq-chip.is-on').textContent.trim().replace(/\\s+/g, ' '), '1 no implement');
        dq().querySelector('.dq-chip.is-on').click();
        t('klik lagi menampilkan semua', shown().length, 10);
        setEditMissingFilter('implement');
        goNav('editUnits', 'heavy');
        t('pindah kelompok mereset filter', editMissingFilter, '');
        t('Heavy: chip work tool', chips(), ['1 no work tool']);
        t('Heavy: persen (20 dari 21 field)', dq().querySelector('.dq__pct').textContent, '95%');
        t('tombol Clean Up Values ada untuk editor', !!dq().querySelector('.dq__merge'), true);

        // =============== (2) Data Check ===============
        const sim = dcSimilarSpellings();
        const brandItem = sim.find(i => i.merge && i.merge.field === 'implBrand');
        t('ejaan mirip: brand Gessner/Geesner', brandItem && brandItem.detail, '"Gessner" (3) · "Geesner" (1)');
        t('ejaan mirip: work tool Bucket/Buket', sim.some(i => i.merge.field === 'workTool' && /Bucket.*Buket/.test(i.detail)), true);
        t('ejaan mirip: company', sim.some(i => i.merge.field === 'company' && /Global Papua Abadi/.test(i.detail)), true);
        t('ISJ/ISS tidak dianggap mirip', sim.some(i => /ISJ/.test(i.detail)), false);
        t('model 6110B/6120B tidak dianggap mirip', sim.some(i => i.merge.field === 'model'), false);
        const miss = dcMissingKeyFields().map(i => i.label);
        t('field kunci kosong per kelompok', miss, ['Agricultural: no implement', 'Agricultural: no year received', 'Heavy: no work tool']);
        t('tanpa daftar Implements: cek implement dilewati', dcImplementNotInList().length, 0);
        globalImplements = [{ id: 'i1', profileName: 'GS', equipmentType: 'Zonal Ripper', brand: 'Gessner' }];
        const notIn = dcImplementNotInList().map(i => i.label);
        t('implement di luar daftar Implements dilaporkan per nilai',
          [notIn.includes('Implement: Zonal Ripper — Geesner'), notIn.includes('Implement: Bed Ripper — ISJ'), notIn.includes('Implement: Zonal Ripper — Gessner')], [true, true, false]);
        globalImplements = [];
        renderDataCheck();
        const mergeBtn = [...document.querySelectorAll('#dataCheckBody button')].find(x => /Merge/.test(x.textContent));
        t('Data Check punya tombol Merge…', !!mergeBtn, true);
        navigateTo('dashboard');
        goDecision('missing:tractor:yearReceived');
        t('Open pada "no year received" membuka Unit Database terfilter', [currentView, effectiveEditGroup(), shown()], ['editUnits', 'tractor', ['TR7']]);
        setEditMissingFilter('', true);

        // =============== (1) Clean Up Values ===============
        openValueMerge('implBrand', ['Gessner', 'Geesner']);
        t('modal terbuka dengan field & pilihan dari Data Check', [document.getElementById('vmField').value,
            [...document.querySelectorAll('#vmList .vm-row.is-on .vm-row__name')].map(x => x.textContent)], ['implBrand', ['Gessner', 'Geesner']]);
        t('ejaan yang dipertahankan = yang terbanyak', document.getElementById('vmTarget').value, 'Gessner');
        t('pratinjau menyebut jumlah unit yang berubah', document.getElementById('vmPreview').textContent, '2 spelling(s) → "Gessner" · 1 unit(s) will change');
        t('saran ejaan mirip tampil', /Looks like the same thing/.test(document.getElementById('vmSuggest').textContent), true);
        history.length = 0; unitBatches.length = 0;
        applyValueMerge();
        t('implement unit ikut diperbaiki', globalData.find(u => u.id === 't3').implement, 'Zonal Ripper — Gessner');
        t('satu kiriman cloud untuk semua unit', unitBatches, [['t3']]);
        t('tercatat di History', history.map(h => [h.unitName, h.field, h.before, h.after]), [['TR3', 'implement', 'Zonal Ripper — Geesner', 'Zonal Ripper — Gessner']]);
        t('ada tombol Undo', !!document.querySelector('#vmResult button'), true);
        undoValueMerge();
        t('Undo mengembalikan nilai lama', globalData.find(u => u.id === 't3').implement, 'Zonal Ripper — Geesner');
        selectMergeValues(['Geesner']);
        applyValueMerge();

        // jenis: format terbalik dirapikan
        switchMergeField('implType');
        selectMergeValues(['Final Cultivator']);
        document.getElementById('vmTarget').value = 'Final Cultivator (ISS)';
        renderValueMerge();
        applyValueMerge();
        t('ganti nama jenis: semua penulisan, termasuk format terbalik', [globalData.find(u => u.id === 't5').implement, globalData.find(u => u.id === 't6').implement],
          ['Final Cultivator (ISS) — ISS', 'Final Cultivator (ISS) — ISS']);
        t('unit lain tidak tersentuh', globalData.find(u => u.id === 't9').implement, 'Bed Ripper — ISJ');

        // batal di konfirmasi
        switchMergeField('workTool');
        selectMergeValues(['Bucket', 'Buket']);
        confirmAnswer = false;
        applyValueMerge();
        t('batal di konfirmasi: tidak berubah', globalData.find(u => u.id === 'h2').workTool, 'Buket');
        t('konfirmasi menyebut perubahan', /"Buket"\\n→ "Bucket"/.test(confirmText), true);
        confirmAnswer = true;
        applyValueMerge();
        t('work tool digabung', globalData.find(u => u.id === 'h2').workTool, 'Bucket');

        // company anggota tim
        memberBatches.length = 0;
        openValueMerge('company', ['PT. Global Papua Abadi', 'PT Global Papua Abadi']);
        document.getElementById('vmTarget').value = 'PT. Global Papua Abadi';
        renderValueMerge();
        applyValueMerge();
        t('company anggota digabung lewat saveTeamMembers', memberBatches, [['m2=PT. Global Papua Abadi']]);
        t('daftar anggota lokal ikut berubah', teamMembers.find(m => m.id === 'm2').company, 'PT. Global Papua Abadi');

        // XSS
        globalData.push(TR(20, '<img src=x onerror=window.__pwn=1> — Brand'));
        switchMergeField('implType');
        await tick();
        t('nilai berisi markup tidak dirender', [document.querySelectorAll('#valueMergeModal img').length, window.__pwn || 0], [0, 0]);
        globalData.pop();
        closeValueMerge();

        // =============== hanya lihat ===============
        as({ role: 'staff', access: { editUnits: 'view' } });
        goNav('editUnits', 'tractor');
        t('akses lihat: tanpa tombol Clean Up Values', !!dq().querySelector('.dq__merge'), false);
        renderDataCheck();
        t('akses lihat: tanpa tombol Merge di Data Check', [...document.querySelectorAll('#dataCheckBody button')].some(x => /Merge/.test(x.textContent)), false);
        const before = JSON.stringify(globalData);
        _vmField = 'implBrand'; _vmSelected = new Set(['Gessner']);
        document.getElementById('vmTarget').value = 'X';
        applyValueMerge();
        t('akses lihat: penggabungan ditolak', JSON.stringify(globalData) === before, true);

        window.logEvent = realLog;
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
