// 2.46.0 — BAST: menu and access, the Excel format, import with its checks (never twice:
// number, S/N, Radio ID, battery S/N), automatic numbers, the link to Warehouse devices
// (allocated / returned) and the A4 print.
const { launch, BASE_URL } = require('./_env');

(async () => {
    const b = await launch();
    const page = await b.newPage({ viewport: { width: 1366, height: 860 } });
    const errors = [];
    page.on('pageerror', e => { if (!/Chart is not defined/.test(e.message)) errors.push(e.message); });
    page.on('dialog', d => d.accept(d.type() === 'prompt' ? d.defaultValue() : undefined));
    await page.goto(BASE_URL + '/index.html', { waitUntil: 'load' });
    await page.waitForTimeout(900);

    const T = await page.evaluate(async () => {
        const T = []; const t = (n, g, w) => T.push({ n, g, w, pass: JSON.stringify(g) === JSON.stringify(w) });
        const tick = ms => new Promise(r => setTimeout(r, ms || 40));
        const ok = () => Promise.resolve();
        // ---- an in-memory server with the same rules as firebase-init.js createBast / endBast ----
        const key = (f, v) => `${f}_${String(v == null ? '' : v).trim().toUpperCase().replace(/[^A-Z0-9-]/g, '_')}`;
        const S = { bast: {}, keys: {}, seq: {}, devWrites: [], ends: [] };
        const pushSnap = () => applyBastSnapshot(Object.values(S.bast).map(x => ({ ...x })));
        const applyDev = (id, patch) => {
            S.devWrites.push([id, JSON.parse(JSON.stringify(patch))]);
            const i = warehouseDevices.findIndex(d => d.id === id);
            if (i >= 0) warehouseDevices[i] = { ...warehouseDevices[i], ...patch }; else warehouseDevices.push({ id, ...patch });
        };
        window.cloud = { isReady: true, addHistoryEvents: ok, subscribeUsers: () => () => {}, saveDevice: ok,
            bastKey: key,
            createBast: async (rec, o) => {
                const out = { ...rec };
                let next = 0;
                if (!out.no) {
                    next = (S.seq[o.seqKey] || 0) + 1;
                    while (S.keys[key('no', `${o.prefix}/${String(next).padStart(3, '0')}`)]) next++;
                    out.no = `${o.prefix}/${String(next).padStart(3, '0')}`;
                } else if (S.keys[key('no', out.no)]) throw Object.assign(new Error('dup'), { code: 'bast/dup', field: 'no', other: S.keys[key('no', out.no)] });
                const uniq = [['sn', out.sn], ['rid', out.radioId], ['bat', out.batterySn]].filter(([, v]) => v);
                for (const [f, v] of uniq) if (S.keys[key(f, v)]) throw Object.assign(new Error('dup'), { code: 'bast/dup', field: f, other: S.keys[key(f, v)] });
                if (next) S.seq[o.seqKey] = next;
                out.noKey = key('no', out.no); out.snKey = key('sn', out.sn);
                S.keys[out.noKey] = out.id; uniq.forEach(([f, v]) => { S.keys[key(f, v)] = out.id; });
                S.bast[out.id] = out;
                const cur = warehouseDevices.find(d => d.id === o.device.id) || null;
                applyDev(o.device.id, o.device.make(cur ? { ...cur } : null, out));
                return out;
            },
            endBast: async (id, info) => {
                const x = S.bast[id];
                S.ends.push([id, info.status, info.date, !!info.device]);
                if (x.status !== 'active') return x;
                Object.assign(x, { status: info.status, endedAt: info.date, endReason: info.reason }, info.extra || {});
                [['sn', x.sn], ['rid', x.radioId], ['bat', x.batterySn]].forEach(([f, v]) => { if (v && S.keys[key(f, v)] === id) delete S.keys[key(f, v)]; });
                if (info.device && x.deviceId) { const cur = warehouseDevices.find(d => d.id === x.deviceId); const p = cur && info.device({ ...cur }, x); if (p) applyDev(x.deviceId, p); }
                return x;
            } };
        const as = doc => { currentUser = { uid: doc.uid, email: doc.uid + '@x.id' }; currentUserDoc = doc; applyRoleGating(); };
        const OWNER = { uid: 'o', role: 'owner', status: 'active', displayName: 'Owner' };
        as(OWNER); cloudInitialized = true; hideAuthGates();
        try { localStorage.setItem('tractorLicenseDatesApplied_v2', '1'); } catch (_) {}
        window.print = () => { window.__printed = (window.__printed || 0) + 1; };

        // ---- menu and access ----
        const navShown = () => { applyRoleGating(); const n = document.querySelector('.nav__link[data-view="bast"]'); return !!n && !n.hidden && n.style.display !== 'none'; };
        t('menu: owner sees BAST', navShown(), true);
        as({ uid: 'v', role: 'staff', status: 'active', access: { warehouse: 'view' } });
        t('menu: a Warehouse viewer does not', [navShown(), canViewView('bast')], [false, false]);
        as({ uid: 'w', role: 'staff', status: 'active', access: { warehouse: 'edit' } });
        t('menu: a Warehouse editor does', [navShown(), canUseBast()], [true, true]);
        as(OWNER);
        const tpl = await fetch('templates/Format_Import_BAST.xlsx');
        t('format: the Excel format is served', tpl.status, 200);
        const XLSX = await loadSheetJs();
        const twb = XLSX.read(await tpl.arrayBuffer(), { type: 'array' });
        const thead = XLSX.utils.sheet_to_json(twb.Sheets.BAST, { header: 1 })[0];
        t('format: its headers are the ones the import reads', BAST_COLUMNS.every(([h]) => thead.some(x => String(x).replace(/\s*\*$/, '') === h)), true);

        // ---- dates ----
        t('tanggal: hari dan bulan Indonesia', bastDateText('2026-06-30'), 'Selasa tanggal 30 Juni 2026');
        t('tanggal: serial Excel, Date, DD/MM/YYYY, salah', [bastDate(46203), bastDate(new Date(2026, 5, 30)), bastDate('30/06/2026'), bastDate('2026-02-30'), bastDate('besok')],
            ['2026-06-30', '2026-06-30', '2026-06-30', '', '']);

        // ---- import ----
        warehouseDevices = [{ id: 'dev_ht1', sn: '25D05C0567', type: 'HT', brand: 'Hytera', model: 'HP508', status: 'warehouse', location: 'Gudang', allocationHistory: [] },
            { id: 'dev_rig', sn: 'RIG-77', type: 'RIG Mobile', status: 'allocated', division: 'Agri', recipientName: 'Budi', recipientTitle: 'Mandor', allocatedAt: '2026-05-01', allocationHistory: [] }];
        applyBastSnapshot([{ id: 'bast_old', no: 'GPA/BAST-HT/05/2026/007', status: 'active', sn: 'OLD-1', radioId: '2001', date: '2026-05-02', p2Name: 'X', itemName: 'HT' }]);
        S.keys[key('no', 'GPA/BAST-HT/05/2026/007')] = 'bast_old'; S.keys[key('sn', 'OLD-1')] = 'bast_old'; S.keys[key('rid', '2001')] = 'bast_old';
        S.bast.bast_old = { id: 'bast_old', no: 'GPA/BAST-HT/05/2026/007', status: 'active', sn: 'OLD-1', radioId: '2001', date: '2026-05-02', p2Name: 'X', itemName: 'HT' };
        const H = BAST_COLUMNS.map(([h]) => h + (BAST_REQUIRED.includes(BAST_COLUMNS.find(c => c[0] === h)[1]) ? ' *' : ''));
        const R = o => BAST_COLUMNS.map(([, f]) => o[f] !== undefined ? o[f] : ({ date: '2026-06-30', companyCode: 'GPA', place: 'PT. Global Papua Abadi', itemCode: 'HT',
            p1Name: 'Herbaruna D. Rexadona', p1Title: 'Regional Head GPA', p1Rep: 'PT. Global Papua Abadi', p2Name: 'R A Herbaruna Dahana Rexadona', p2Title: 'Regional Head', p2Dept: 'Agri',
            itemName: 'Handy Talky (HT Hytera)', brandType: 'Radio HT HP508 GPS Support', qty: 1, unit: 'Set', condition: 'Baru/Baik', witness1: 'R A Herbaruna Dahana Rexadona', witness2: 'Monti Griana' })[f] || '');
        const rows = [H,
            R({ sn: '25D05C0567', radioId: '1009', batterySn: '25D0505220' }),                              // 2: in Warehouse → allocated, auto number
            R({ sn: 'NEW-HT-2', radioId: '1010', p2Name: 'Andi', deviceType: 'HT' }),                         // 3: new device, auto number
            R({ no: 'GPA/BAST-RIG/06/2026/015', itemCode: 'RIG', itemName: 'RIG Mobile', sn: 'RIG-77', p2Name: 'Citra' }),   // 4: manual number, re-allocated
            R({ sn: 'new-ht-2 ', p2Name: 'Dobel' }),                                                          // 5: same S/N as row 3
            R({ sn: 'NEW-HT-9', p2Name: '' }),                                                                // 6: no recipient
            R({ sn: 'NEW-HT-10', radioId: '2001' }),                                                          // 7: Radio ID in an active letter
            R({ no: 'GPA/BAST-HT/05/2026/007', sn: 'NEW-HT-11' }),                                            // 8: number already used
            R({ sn: 'NEW-HT-12', date: '2099-01-01', unit: 'Box' }),                                          // 9: future date, bad unit
            ['', '', '']];                                                                                    // blank row: skipped
        const ws = XLSX.utils.aoa_to_sheet(rows);
        const wb = XLSX.utils.book_new(); XLSX.utils.book_append_sheet(wb, XLSX.utils.aoa_to_sheet([['Petunjuk']]), 'Petunjuk'); XLSX.utils.book_append_sheet(wb, ws, 'BAST');
        const file = () => new File([XLSX.write(wb, { type: 'array', bookType: 'xlsx' })], 'bast.xlsx');
        navigateTo('bast'); await tick();
        await importBastFile({ target: { files: [file()] } }); await tick();
        const imp = () => _bastImport.rows.map(r => [r.row, r.errors.length ? r.errors : 'ok']);
        t('import: the dialog opens with every row checked', document.getElementById('bastImportModal').classList.contains('open'), true);
        const got = imp();
        t('import: valid rows ready', got.filter(([, e]) => e === 'ok').map(([r]) => r), [2, 3, 4]);
        t('import: dobel di file ditolak', got.find(([r]) => r === 5)[1], ['S/N Unit new-ht-2 is also on row 3 of this file']);
        t('import: kolom wajib kosong ditolak', got.find(([r]) => r === 6)[1], ['Pihak Kedua - Nama is empty']);
        t('import: Radio ID di BAST aktif ditolak', got.find(([r]) => r === 7)[1], ['Radio ID DMR 2001 is already in active letter GPA/BAST-HT/05/2026/007']);
        t('import: nomor surat yang sudah dipakai ditolak', got.find(([r]) => r === 8)[1], ['No Surat GPA/BAST-HT/05/2026/007 is already in letter GPA/BAST-HT/05/2026/007']);
        t('import: tanggal masa depan, satuan salah', got.find(([r]) => r === 9)[1], ['Tanggal is in the future', 'Satuan must be one of: Set, Unit, Pcs']);
        t('import: the Warehouse column says what happens', [...document.querySelectorAll('#bastImportBody tr')].slice(0, 3).map(tr => tr.children[4].textContent),
            ['HT — In Warehouse → Allocated', 'New device (Allocated)', 'RIG Mobile — Allocated → Allocated']);
        await runBastImport(); await tick();
        const made = _bastImport.rows.filter(r => r.done).map(r => r.done);
        t('import: numbers — automatic per PT + kode + year, manual kept', made, ['GPA/BAST-HT/06/2026/001', 'GPA/BAST-HT/06/2026/002', 'GPA/BAST-RIG/06/2026/015']);
        const ht1 = warehouseDevices.find(d => d.id === 'dev_ht1');
        t('warehouse: device found by S/N is now allocated to Pihak Kedua', [ht1.status, ht1.recipientName, ht1.recipientTitle, ht1.division, ht1.allocatedAt, ht1.radioId, ht1.batterySn, ht1.bastNo, ht1.location],
            ['allocated', 'R A Herbaruna Dahana Rexadona', 'Regional Head', 'Agri', '2026-06-30', '1009', '25D0505220', 'GPA/BAST-HT/06/2026/001', '']);
        const nd = warehouseDevices.find(d => d.sn === 'NEW-HT-2');
        t('warehouse: an S/N not in Warehouse becomes a new allocated device', nd && [nd.type, nd.status, nd.recipientName, nd.model], ['HT', 'allocated', 'Andi', 'Radio HT HP508 GPS Support']);
        const rig = warehouseDevices.find(d => d.id === 'dev_rig');
        t('warehouse: re-allocated device keeps the earlier allocation in its history', [rig.recipientName, rig.allocationHistory.map(h => [h.recipientName, h.returnedAt])], ['Citra', [['Budi', '2026-06-30']]]);
        pushSnap(); await tick();
        t('page: the letters are listed (active)', [...document.querySelectorAll('#bastBody tr td:nth-child(2)')].map(td => td.textContent).sort(),
            ['GPA/BAST-HT/05/2026/007', 'GPA/BAST-HT/06/2026/001', 'GPA/BAST-HT/06/2026/002', 'GPA/BAST-RIG/06/2026/015']);
        // The same file again: nothing is made twice.
        await importBastFile({ target: { files: [file()] } }); await tick();
        t('import again: every row refused, nothing ready', [_bastImport.rows.filter(r => !r.errors.length).length, document.getElementById('bastImportGo').disabled], [0, true]);
        t('import again: refused because the S/N already has an active letter', _bastImport.rows[0].errors, ['S/N Unit 25D05C0567 is already in active letter GPA/BAST-HT/06/2026/001',
            'Radio ID DMR 1009 is already in active letter GPA/BAST-HT/06/2026/001', 'S/N Baterai 25D0505220 is already in active letter GPA/BAST-HT/06/2026/001']);
        closeBastImport();
        // A race (another device made it meanwhile) is refused by the server and said so.
        S.keys[key('sn', 'RACE-1')] = 'bast_old';
        _bastImport = { rows: [{ row: 2, rec: bastRecFromRow(R({ sn: 'RACE-1' }), Object.fromEntries(BAST_COLUMNS.map(([, f], i) => [f, i]))).rec, errors: [] }] };
        document.getElementById('bastImportModal').classList.add('open');
        await runBastImport();
        t('import: a clash found by the server is listed, not lost', _bastImport.rows[0].errors, ['S/N Unit is already in letter GPA/BAST-HT/05/2026/007']);
        closeBastImport();

        // ---- print ----
        const id1 = Object.values(S.bast).find(x => x.no === 'GPA/BAST-HT/06/2026/001').id;
        window.__printed = 0;
        printBast([id1]);
        const pg = document.querySelector('#bastPrint .bast-page');
        const txt = pg.textContent.replace(/\s+/g, ' ');
        t('pdf: one A4 page with the letter', [document.querySelectorAll('#bastPrint .bast-page').length, window.__printed], [1, 1]);
        t('pdf: header and opening line', [pg.querySelector('.bast-title').textContent, /Nomor : GPA\/BAST-HT\/06\/2026\/001/.test(txt),
            txt.includes('Pada hari ini, Selasa tanggal 30 Juni 2026, bertempat di PT. Global Papua Abadi, telah dilakukan serah terima aset perusahaan berupa Handy Talky (HT Hytera) dari:')],
            ['BERITA ACARA SERAH TERIMA HANDY TALKY (HT HYTERA)', true, true]);
        t('pdf: details and signatures', ['Radio HT HP508 GPS Support', '1009', '25D05C0567', '25D0505220', 'Baru/Baik', 'MENGETAHUI', 'Monti Griana'].every(s => txt.includes(s)), true);
        t('pdf: one MENGETAHUI, centred above the witnesses', [pg.querySelectorAll('.bast-sign__mid').length, pg.querySelector('.bast-sign__mid').textContent], [1, 'MENGETAHUI']);
        const wb3 = bastWitnessRow({ witness1: 'A', witness1Title: 'Regional Head', witness2: 'B', witness2Title: 'HR', witness3: 'C', witness3Title: 'OT' });
        const wb1 = bastWitnessRow({ witness1: 'A', witness1Title: 'Regional Head' });
        const cellsOf = h => { const d = document.createElement('div'); d.innerHTML = h; return [...d.querySelectorAll('.bast-sign__row > div')].map(x => x.textContent.trim()); };
        t('pdf: three witnesses with their positions, one in the middle when alone', [cellsOf(wb3), cellsOf(wb1), bastWitnessRow({})], [['ARegional Head', 'BHR', 'COT'], ['', 'ARegional Head', ''], '']);
        printBast(Object.keys(S.bast));
        t('pdf: several letters, one page each', document.querySelectorAll('#bastPrint .bast-page').length, 4);

        // ---- returned / cancelled ----
        await endBastPrompt(id1, 'returned'); pushSnap(); await tick();
        t('returned: the letter ends and the device is back in the warehouse', [S.bast[id1].status, ht1.status === undefined ? warehouseDevices.find(d => d.id === 'dev_ht1').status : warehouseDevices.find(d => d.id === 'dev_ht1').status,
            warehouseDevices.find(d => d.id === 'dev_ht1').allocationHistory.slice(-1)[0].recipientName], ['returned', 'warehouse', 'R A Herbaruna Dahana Rexadona']);
        t('returned: the S/N may get a new letter, the number never again', [!!S.keys[key('sn', '25D05C0567')], !!S.keys[key('no', 'GPA/BAST-HT/06/2026/001')]], [false, true]);

        // ---- from an allocated device ----
        warehouseDevices.push({ id: 'dev_rig2', sn: 'RIG-88', type: 'RIG Mobile', brand: 'Motorola', model: 'XiR M8668', status: 'allocated', division: 'Transport', recipientName: 'Dedi', recipientTitle: 'Driver', allocatedAt: '2026-11-01', allocationHistory: [] });
        showDeviceProfile('dev_rig2'); await tick();
        const mk = [...document.querySelectorAll('#deviceProfileBody button')].find(x => /Make BAST/.test(x.textContent));
        t('device: an allocated device without a letter offers "Make BAST"', !!mk, true);
        mk.click(); await tick();
        const v = id => document.getElementById('bast_' + id).value;
        t('device: the form is filled from the device', [v('sn'), v('itemCode'), v('p2Name'), v('p2Title'), v('p2Dept'), v('date'), v('brandType')],
            ['RIG-88', 'RIG', 'Dedi', 'Driver', 'Transport', '2026-11-01', 'Motorola XiR M8668']);
        ['companyCode', 'place', 'p1Name', 'p1Title', 'p1Rep', 'itemName'].forEach(f => { document.getElementById('bast_' + f).value = { companyCode: 'GPA', place: 'PT. Global Papua Abadi', p1Name: 'A', p1Title: 'B', p1Rep: 'C', itemName: 'RIG Mobile' }[f]; });
        window.__printed = 0;
        await saveBastForm(); await tick();
        const rigB = Object.values(S.bast).find(x => x.sn === 'RIG-88');
        t('device: the letter is made and numbered', rigB && rigB.no, 'GPA/BAST-RIG/11/2026/001');
        pushSnap(); await tick();
        showDeviceProfile('dev_rig2'); await tick();
        t('device: its profile now names the letter', /GPA\/BAST-RIG\/11\/2026\/001/.test(document.getElementById('deviceProfileBody').textContent), true);
        closeDeviceProfile();
        // Warehouse takes it off its allocation: the letter is returned (the device saved by Warehouse itself).
        editDevice('dev_rig2'); await tick();
        document.getElementById('devStatus').value = 'warehouse'; onDeviceStatusChange();
        document.getElementById('devReturnedAt').value = '2026-11-20';
        const ends0 = S.ends.length;
        saveDevice({ preventDefault() {} }); await tick(80);
        // 2.48.0: a device on a BAST leaves its holder through Return — the form opens it instead of saving.
        t('warehouse: the form sends a device on a BAST to Return', [S.ends.length - ends0, document.getElementById('devReturnModal').classList.contains('open'), /RIG-88/.test(document.getElementById('retList').textContent)], [0, true, true]);
        document.getElementById('retDate').value = '2026-11-20';
        document.getElementById('retBy').value = 'Dedi';
        document.getElementById('retCondition').value = 'Baik';
        await saveDeviceReturn(); await tick(80);
        showDeviceProfile('dev_ht1'); await tick();
        const histTxt = document.getElementById('deviceProfileBody').textContent;
        t('history: the device profile lists its BAST letters, returned ones too', [/History\s*1/.test(histTxt), /GPA\/BAST-HT\/06\/2026\/001/.test(histTxt), /Returned/.test(histTxt)], [true, true, true]);
        t('history: the allocation row names its BAST', warehouseDevices.find(d => d.id === 'dev_ht1').allocationHistory.slice(-1)[0].bastNo, 'GPA/BAST-HT/06/2026/001');
        closeDeviceProfile();
        t('warehouse: device back from allocation returns its letter', [S.ends.slice(ends0).map(e => [e[1], e[2]]), S.bast[rigB.id].status, warehouseDevices.find(d => d.id === 'dev_rig2').status], [[['returned', '2026-11-20']], 'returned', 'warehouse']);

        // ---- cancelled letters: the owner deletes them (picked or all) ----
        const delCalls = [];
        window.cloud.deleteBasts = ids => { delCalls.push(ids.slice()); ids.forEach(id => { delete S.bast[id]; }); return ok(); };
        const vid = Object.values(S.bast).find(x => x.status === 'active' && x.no === 'GPA/BAST-HT/06/2026/002').id;
        await endBastPrompt(vid, 'void'); pushSnap(); setBastStatus('void'); await tick();
        t('cancelled: delete buttons for the owner on the Cancelled tab', ['bastDelPicked', 'bastDelAll'].map(id => document.getElementById(id).style.display), ['', '']);
        await deleteBasts([vid, id1]); pushSnap(); await tick();
        t('cancelled: only cancelled letters are deleted (a returned one is kept)', [delCalls, !!S.bast[id1], !!S.keys[key('no', 'GPA/BAST-HT/06/2026/002')]], [[[vid]], true, true]);
        as({ uid: 'w', role: 'staff', status: 'active', access: { warehouse: 'edit' } }); renderBastView();
        t('cancelled: a Warehouse editor gets no delete buttons', document.getElementById('bastDelAll').style.display, 'none');
        as(OWNER); setBastStatus('active');
        // ---- reset numbering (owner) ----
        let resets = 0;
        window.cloud.resetBastNumbers = () => { resets++; return Promise.resolve({ cancelled: 0, released: 3, counters: 2 }); };
        t('reset: the owner sees "Reset numbering"', document.getElementById('bastResetNo').style.display, '');
        await resetBastNumbering(); await tick();
        t('reset: runs after confirming', resets, 1);
        // 2.47.5: deleted numbers in between are given out first, then after the top.
        const gp = bastResetPlan([3, 1, 12, 2, 4, 6, 8, 9, 11].map(n => ({ no: `GPA/BAST-HT/11/2026/${String(n).padStart(3, '0')}`, companyCode: 'GPA', itemCode: 'HT', date: '2026-11-26' })));
        t('reset plan: the gaps 005, 007, 010 first, then 013', gp, 'GPA/BAST-HT/2026: next numbers 005, 007, 010, then 013');
        t('reset plan: no letters → 001', bastResetPlan([]), 'Every series starts again at 001.');
        // ---- 2.48.0 logic fixes ----
        const wh = { status: 'warehouse', division: '', recipientName: '', allocationHistory: [] };
        const L = { id: 'bast_x', status: 'void', p2Name: 'Salah Orang', p2Dept: 'IT', p2Title: 'Staff', date: '2026-11-20', no: 'GPA/BAST-HT/11/2026/009' };
        const afterLetter = { ...wh, ...bastDevicePatch(wh, L) };
        t('cancel: a device from the warehouse goes back to the warehouse',
            (({ status, recipientName, bastId }) => [status, recipientName, bastId])({ ...afterLetter, ...bastDeviceReturn(afterLetter, { ...L, prevDevice: bastDeviceBefore(wh) }) }), ['warehouse', '', '']);
        const held = { status: 'allocated', division: 'Agri', recipientName: 'Eko', recipientTitle: 'Asisten', allocatedAt: '2026-03-10', allocationHistory: [] };
        const after2 = { ...held, ...bastDevicePatch(held, L) };
        const back2 = { ...after2, ...bastDeviceReturn(after2, { ...L, prevDevice: bastDeviceBefore(held) }) };
        t('cancel: a device someone held goes back to them, the extra history row removed',
            [back2.status, back2.recipientName, back2.allocatedAt, back2.allocationHistory.length, after2.allocationHistory.length], ['allocated', 'Eko', '2026-03-10', 0, 1]);
        t('cancel: a letter from before 2.48.0 only drops the link', bastDeviceReturn(after2, L).status, undefined);
        const nOld = warehouseDevices.length;
        warehouseDevices.push({ id: 'dev_dmg', sn: 'HT-DMG', type: 'HT', status: 'damaged' }, { id: 'dev_inst', sn: 'GPS-9', type: 'GPS', status: 'installed', unitName: 'JD-07' });
        const chk = bastDeviceChecks([{ rec: { sn: 'HT-DMG' }, errors: [] }, { rec: { sn: 'gps-9' }, errors: [] }, { rec: { sn: 'HT-NEW' }, errors: [] }]).map(r => r.errors.join());
        t('status: damaged or installed devices are refused, a new S/N is fine', [/Damaged/.test(chk[0]), /Installed on JD-07/.test(chk[1]), chk[2]], [true, true, '']);
        warehouseDevices.splice(nOld);
        t('number: the series of a typed number (month not part of it)', bastSeqOf('gpa/bast-ht/03/2026/007'), { seqKey: 'GPA_HT_2026', series: 'GPA/BAST-HT/2026', n: 7 });
        t('number: another format has no series', bastSeqOf('123/BAST/2026'), null);
        const anyNo = Object.values(S.bast)[0].no, q0 = bastSeqOf(anyNo);
        const other = anyNo.replace(/\/(\d{2})\/(\d{4})\//, (m, mm, yy) => `/${mm === '01' ? '02' : '01'}/${yy}/`);
        const cl = bastClashes([{ row: 2, rec: { no: other }, errors: [] }])[0].errors.join();
        t('number: the same sequence in the same year under another month is refused', cl.includes(`Sequence ${String(q0.n).padStart(3, '0')}`), true);
        const sl0 = stockLedger.length;
        stockLedger.push({ id: 's_in', txnType: 'IN', itemName: 'Kabel', qty: 10 }, { id: 's_out', txnType: 'OUT', itemName: 'Kabel', qty: 8 });
        let asked = 0; const conf0 = window.confirm; window.confirm = () => { asked++; return false; };
        t('stock: an IN edited below what went out is questioned', [checkStockEdit(stockLedger[sl0], 'Kabel', 2, 'IN'), checkStockEdit(stockLedger[sl0], 'Kabel', 9, 'IN'), asked], [false, true, 1]);
        window.confirm = conf0; stockLedger.splice(sl0);
        as({ uid: 'w', role: 'staff', status: 'active', access: { warehouse: 'edit' } }); renderBastView();
        t('reset: a Warehouse editor does not see it', document.getElementById('bastResetNo').style.display, 'none');
        as(OWNER); renderBastView();
        // (the Warehouse snapshot would bring the saved device back; the stub does not)
        Object.assign(warehouseDevices.find(d => d.id === 'dev_rig2'), { status: 'warehouse', bastId: '', bastNo: '' });
        // ---- devices allocated before BAST: export, complete, import ----
        warehouseDevices.push({ id: 'dev_old1', sn: 'HT-OLD-1', type: 'HT', brand: 'Hytera', model: 'HP508', radioId: '3001', batterySn: 'BAT-OLD-1', status: 'allocated',
            division: 'Agri', recipientName: 'Eko', recipientTitle: 'Asisten', allocatedAt: '2026-03-10', allocationHistory: [] });
        renderBastView();
        // (the cancelled letter's device is still allocated, so it counts too)
        t('allocated without BAST: counted on the page', document.getElementById('bastAllocCount').textContent, '2');
        let written = null;
        const realWrite = XLSX.writeFile;
        XLSX.writeFile = (wbk, name) => { written = { wbk, name }; };
        localStorage.setItem(BAST_PREF_KEY, JSON.stringify({ companyCode: 'GPA', place: 'PT. Global Papua Abadi', p1Name: 'Herbaruna D. Rexadona', p1Title: 'Regional Head GPA', p1Rep: 'PT. Global Papua Abadi' }));
        await exportBastAllocated();
        XLSX.writeFile = realWrite;
        const exRows = XLSX.utils.sheet_to_json(written.wbk.Sheets.BAST, { header: 1, defval: '' });
        const col = h => exRows[0].findIndex(x => String(x).replace(/\s*\*$/, '') === h);
        t('allocated without BAST: one row per device, filled from Warehouse and the last letter',
            [exRows.length - 1, exRows[1][col('S/N Unit')], exRows[1][col('Pihak Kedua - Nama')], exRows[1][col('Pihak Kedua - Jabatan')], exRows[1][col('Pihak Kedua - Departemen')],
             exRows[1][col('Tanggal')], exRows[1][col('Radio ID DMR')], exRows[1][col('S/N Baterai')], exRows[1][col('Kode Barang')], exRows[1][col('Kode PT')], exRows[1][col('Merk / Type')]],
            [2, 'HT-OLD-1', 'Eko', 'Asisten', 'Agri', '2026-03-10', '3001', 'BAT-OLD-1', 'HT', 'GPA', 'Hytera HP508']);
        const xfile = new File([XLSX.write(written.wbk, { type: 'array', bookType: 'xlsx' })], 'x.xlsx');
        await importBastFile({ target: { files: [xfile] } }); await tick();
        t('allocated without BAST: the exported file imports as is', _bastImport.rows.map(r => r.errors), [[], []]);
        await runBastImport(); closeBastImport(); pushSnap(); await tick();
        const old1 = warehouseDevices.find(d => d.id === 'dev_old1');
        t('allocated without BAST: same recipient, same date, linked, no extra history', [old1.status, old1.recipientName, old1.allocatedAt, !!old1.bastNo, old1.allocationHistory.length],
            ['allocated', 'Eko', '2026-03-10', true, 0]);
        renderBastView();
        t('allocated without BAST: none left', [document.getElementById('bastAllocCount').textContent, bastAllocatedWithout().map(d => d.sn)], ['0', []]);

        // ---- return (2.47.0): one dialog, condition + note, optional return letter, one timeline ----
        const oldB = Object.values(S.bast).find(x => x.sn === 'HT-OLD-1' && x.status === 'active');
        renderDeviceTable && renderDeviceTable();
        returnDevicePrompt('dev_old1'); await tick();
        t('return: dialog opens for the allocated device, letter possible (it has a BAST)',
            [document.getElementById('devReturnModal').classList.contains('open'), document.getElementById('retLetter').disabled, /HT-OLD-1/.test(document.getElementById('retList').textContent)], [true, false, true]);
        t('return: the date starts empty (it is chosen, often days later)', document.getElementById('retDate').value, '');
        t('return: "Returned by" starts with the holder', document.getElementById('retBy').value, 'Eko');
        const endsD = S.ends.length;
        document.getElementById('retCondition').value = 'Baik';
        await saveDeviceReturn();
        t('return: without a date nothing happens', [S.ends.length - endsD, document.getElementById('devReturnModal').classList.contains('open')], [0, true]);
        document.getElementById('retDate').value = '2026-11-20';
        document.getElementById('retCondition').value = 'Rusak Ringan';
        document.getElementById('retNote').value = '';
        const endsR = S.ends.length;
        await saveDeviceReturn();
        t('return: damaged without a note is refused', [S.ends.length - endsR, document.getElementById('devReturnModal').classList.contains('open')], [0, true]);
        document.getElementById('retNote').value = 'Antena patah';
        document.getElementById('retBy').value = '';
        const endsB = S.ends.length;
        await saveDeviceReturn();
        t('return: without who returned it nothing happens', S.ends.length - endsB, 0);
        document.getElementById('retBy').value = 'Budi (kurir)';
        document.getElementById('retLetter').checked = true;
        window.__printed = 0;
        await saveDeviceReturn(); await tick();
        const o1 = warehouseDevices.find(d => d.id === 'dev_old1');
        const lastH = o1.allocationHistory.slice(-1)[0];
        t('return: device to Damaged, allocation in history with condition, note and return letter number',
            [o1.status, o1.recipientName, lastH.recipientName, lastH.returnCondition, lastH.returnNote, lastH.returnNo, lastH.bastNo],
            ['damaged', '', 'Eko', 'Rusak Ringan', 'Antena patah', oldB.no.replace('/BAST-', '/BAPB-'), oldB.no]);
        t('return: who returned it is kept (history and letter)', [lastH.returnerName, S.bast[oldB.id].returnerName], ['Budi (kurir)', 'Budi (kurir)']);
        t('return: its BAST is Returned with the return details', [S.bast[oldB.id].status, S.bast[oldB.id].returnCondition, S.bast[oldB.id].returnNo], ['returned', 'Rusak Ringan', oldB.no.replace('/BAST-', '/BAPB-')]);
        const rl = document.querySelector('#bastPrint .bast-page').textContent.replace(/\s+/g, ' ');
        t('return letter: printed, numbered, names both letters and the condition',
            [window.__printed, rl.includes('BERITA ACARA PENGEMBALIAN'), rl.includes(oldB.no.replace('/BAST-', '/BAPB-')), rl.includes(oldB.no), rl.includes('Antena patah'), rl.includes('Rusak Ringan') && rl.includes('Budi (kurir)') && rl.includes('Atas nama')],
            [1, true, true, true, true, true]);
        pushSnap(); await tick();
        showDeviceProfile('dev_old1'); await tick();
        const tl = [...document.querySelectorAll('#deviceProfileBody .dev-tl__item')].map(li => li.textContent.replace(/\s+/g, ' ').trim());
        t('timeline: Returned (with condition and return letter) above Allocated (with its BAST)',
            [tl.length, /^Returned/.test(tl[0]), tl[0].includes('Rusak Ringan'), tl[0].includes('Return letter'), /^Allocated/.test(tl[1]), tl[1].includes(oldB.no)], [2, true, true, true, true, true]);
        closeDeviceProfile();
        // A device without a BAST: returned on its own (no letter possible).
        const devSaves = [];
        window.cloud.saveDevice = r => { devSaves.push(JSON.parse(JSON.stringify(r))); return ok(); };
        warehouseDevices.push({ id: 'dev_nob', sn: 'HT-NOB', type: 'HT', status: 'allocated', division: 'EHS', recipientName: 'Pos Damkar', allocatedAt: '2026-10-29', allocationHistory: [] });
        returnDevicePrompt('dev_nob'); await tick();
        t('return without BAST: no letter offered', document.getElementById('retLetter').disabled, true);
        document.getElementById('retCondition').value = 'Baik';
        document.getElementById('retDate').value = '2026-11-18';
        await saveDeviceReturn(); await tick();
        t('return without BAST: device back in the warehouse, history kept', [devSaves.length, devSaves[0].status, devSaves[0].allocationHistory[0].recipientName, devSaves[0].allocationHistory[0].returnCondition],
            [1, 'warehouse', 'Pos Damkar', 'Baik']);
        t('return: the chosen date is the one kept', [lastH.returnedAt, devSaves[0].allocationHistory[0].returnedAt], ['2026-11-20', '2026-11-18']);
        return T;
    });

    let pass = 0;
    T.forEach(x => { if (x.pass) pass++; else console.log('FAIL', x.n, '\n  got   :', JSON.stringify(x.g), '\n  wanted:', JSON.stringify(x.w)); });
    if (errors.length) console.log('PAGE ERRORS:', errors);
    console.log(`${pass}/${T.length} lulus`);
    await b.close();
    process.exit(pass === T.length && !errors.length ? 0 : 1);
})();
