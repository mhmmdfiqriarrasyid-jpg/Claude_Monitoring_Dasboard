// v149: restore yang menepati janjinya.
//   - GABUNG: catatan yang ada sekarang dipertahankan apa adanya; hanya yang
//     belum ada yang ditambahkan (dulu salinan cadangan menang: laporan yang
//     sudah disetujui kembali "pending", anggota nonaktif aktif lagi).
//   - GANTI: ditulis UTUH (replaceDocs), bukan digabung ke dokumen lama.
//   - Setiap koleksi yang dipulihkan tercatat di History.
//   - Master Lists dan License Requirements ikut cadangan dan restore.
//   - Impor CSV lisensi yang sama dua kali tidak menggandakan transaksi.
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
        const realCreate = document.createElement.bind(document);
        URL.createObjectURL = blob => { window.__blob = blob; return 'blob:test'; };
        URL.revokeObjectURL = () => {};
        document.createElement = tag => { const el = realCreate(tag); if (tag === 'a') el.click = () => {}; return el; };
        const exportNow = async () => { window.__blob = null; await exportBackup(); return JSON.parse(await window.__blob.text()); };
        let answers = [];
        window.confirm = () => answers.length ? answers.shift() : false;
        const toasts = [], history = [];
        window.showToast = m => toasts.push(String(m));
        const realLog = logEvent; window.logEvent = e => { history.push(e); };

        const written = {}, replaced = {}, deleted = [], masterSaved = [];
        const mk = name => items => { written[name] = (written[name] || []).concat(items.map(r => r.id)); return Promise.resolve(); };
        const ok = () => Promise.resolve();
        window.cloud = { isReady: true, addHistoryEvents: ok, subscribeUsers: () => () => {},
            saveUnits: mk('units'), deleteUnits: ids => { ids.forEach(i => deleted.push('unit:' + i)); return ok(); },
            saveImplements: mk('implements'), saveDamages: mk('damages'), saveLicenses: mk('licenses'),
            saveUserCategories: mk('userCategories'), saveDamageComponents: mk('damageComponents'),
            saveTeamMembers: mk('teamMembers'), saveShifts: mk('shifts'), getAllShifts: () => Promise.resolve([]),
            saveLeaveRequests: mk('leaveRequests'), saveWorkLogs: mk('workLogs'), saveDevices: mk('devices'),
            saveStockItems: mk('stockItems'), saveInspectionPlans: mk('inspectionPlans'), saveInspections: mk('inspections'),
            saveLicenseRequirements: mk('licenseRequirements'),
            saveMasterList: (kind, values) => { masterSaved.push([kind, values.slice()]); return ok(); },
            replaceDocs: (key, items) => { replaced[key] = (replaced[key] || []).concat(items.map(r => r.id)); return ok(); },
            deleteImplement: ok, deleteShift: ok, deleteTeamMember: id => { deleted.push('tm:' + id); return ok(); },
            deleteWorkLog: id => { deleted.push('wl:' + id); return ok(); }, deleteLeaveRequest: ok, deleteDevice: ok,
            deleteStockItem: ok, deleteUserCategory: ok, deleteDamageComponent: ok, deleteDamage: ok, deleteLicense: ok,
            deleteInspectionPlan: ok, deleteInspection: ok, deleteLicenseRequirement: id => { deleted.push('lr:' + id); return ok(); } };
        currentUser = { uid: 'uO', email: 'o@x.id' };
        currentUserDoc = { role: 'owner', status: 'active', email: 'o@x.id' };
        hideAuthGates(); applyRoleGating();

        const seedOld = () => {
            globalData = [{ id: 'u1', name: 'A', sn: 'S1-OLD', status: 'Good' }];
            globalImplements = []; globalDamages = []; globalLicenseStock = []; userCategories = []; damageComponents = [];
            teamMembers = [{ id: 'm1', name: 'Andi', company: 'PT. GPA', active: true }];
            teamShifts = []; warehouseDevices = []; stockLedger = []; leaveRequests = []; inspectionPlans = []; inspections = [];
            workLogs = [{ id: 'w1', date: '2026-09-14', memberId: 'm1', task: 'x', approval: 'pending' }];
            licenseRequirements = [{ id: 'bed-ripper', type: 'Bed Ripper', gps: 'SF-RTK', display: 'G5 Advance' }];
            masterLists = { site: ['PT. GPA'], brand: ['Gessner'], heavyBrand: [], company: ['PT. GPA'] };
        };

        // =============== cadangan lama ===============
        seedOld();
        const oldFile = await exportNow();
        t('cadangan membawa License Requirements', oldFile.licenseRequirements, [{ id: 'bed-ripper', type: 'Bed Ripper', gps: 'SF-RTK', display: 'G5 Advance' }]);
        t('cadangan membawa Master Lists', oldFile.masterLists.map(r => [r.id, r.values]),
          [['site', ['PT. GPA']], ['brand', ['Gessner']], ['heavyBrand', []], ['company', ['PT. GPA']]]);
        t('keduanya tidak lagi dilaporkan hilang', oldFile.omitted.filter(o => /Master|License Req/.test(o.label)), []);

        // =============== keadaan sekarang (lebih baru) ===============
        globalData = [{ id: 'u1', name: 'A', sn: 'S1-FIXED', status: 'Breakdown' }];
        teamMembers = [{ id: 'm1', name: 'Andi', company: 'PT. GPA', active: false }];
        workLogs = [{ id: 'w1', date: '2026-09-14', memberId: 'm1', task: 'x', approval: 'approved', approvedByEmail: 'b@x.id' },
                    { id: 'w2', date: '2026-10-01', memberId: 'm1', task: 'baru', approval: 'pending' }];
        licenseRequirements = [{ id: 'bed-ripper', type: 'Bed Ripper', gps: 'SF-RTK', display: 'G5 Basic' }];
        masterLists = { site: ['PT. GPA', 'PT. MNM'], brand: [], heavyBrand: ['LiuGong'], company: ['PT. GPA'] };
        saveToStorage(globalData);
        // satu laporan hilang → GABUNG harus mengembalikannya
        const file = JSON.parse(JSON.stringify(oldFile));
        file.workLogs.push({ id: 'w0', date: '2026-09-01', memberId: 'm1', task: 'hilang', approval: 'approved' });

        // =============== GABUNG ===============
        history.length = 0; Object.keys(written).forEach(k => delete written[k]);
        answers = [true];
        importBackup(new File([JSON.stringify(file)], 'b.json', { type: 'application/json' }));
        await tick(500);
        t('GABUNG: unit dengan id sama TIDAK ditambah lagi walau SN-nya beda', [globalData.length, globalData[0].sn, globalData[0].status], [1, 'S1-FIXED', 'Breakdown']);
        t('GABUNG: laporan yang sudah disetujui TETAP disetujui', workLogs.find(w => w.id === 'w1').approval, 'approved');
        t('GABUNG: laporan yang hilang dikembalikan', workLogs.some(w => w.id === 'w0'), true);
        t('GABUNG: laporan baru (tidak ada di berkas) tidak dihapus', workLogs.some(w => w.id === 'w2'), true);
        t('GABUNG: hanya yang ditambahkan dikirim ke cloud', written.workLogs, ['w0']);
        t('GABUNG: anggota nonaktif tetap nonaktif, tidak ditulis ulang', [teamMembers[0].active, written.teamMembers || []], [false, []]);
        t('GABUNG: syarat lisensi yang ada dipertahankan', licenseRequirements[0].display, 'G5 Basic');
        t('GABUNG: Master Lists ditambah entri dari berkas, tidak dikurangi',
          [masterLists.site, masterLists.brand, masterLists.heavyBrand], [['PT. GPA', 'PT. MNM'], ['Gessner'], ['LiuGong']]);
        t('GABUNG: tercatat di History per koleksi', history.filter(h => h.action === 'restore').map(h => h.unitName + ': ' + h.after),
          ['[Restore] Units: 0 added, 1 kept as they are', '[Restore] License Requirements: 0 added, 1 kept as they are',
           '[Restore] Implement Brands: 1 entry', '[Restore] Team Members: 0 added, 1 kept as they are',
           '[Restore] Daily Reports: 1 added, 1 kept as they are']);
        const rows = [...document.querySelectorAll('#restoreReportBody tr')].map(tr => tr.textContent.replace(/\\s+/g, ' ').trim());
        t('laporan restore menyebut yang ditambahkan dan dipertahankan', rows.some(r => /Daily Reports.*merged — 1 added, 1 kept as they are/.test(r)), true);
        closeRestoreReport();

        // =============== GANTI ===============
        history.length = 0; Object.keys(replaced).forEach(k => delete replaced[k]); masterSaved.length = 0;
        answers = [false, true];
        importBackup(new File([JSON.stringify(file)], 'b.json', { type: 'application/json' }));
        await tick(500);
        t('GANTI: unit ditulis utuh lewat replaceDocs', replaced.units, ['u1']);
        t('GANTI: laporan ditulis utuh lewat replaceDocs', (replaced.workLogs || []).sort(), ['w0', 'w1']);
        t('GANTI: laporan yang tidak ada di berkas dihapus', deleted.includes('wl:w2'), true);
        t('GANTI: syarat lisensi dari berkas', [licenseRequirements[0].display, replaced.licenseRequirements], ['G5 Advance', ['bed-ripper']]);
        t('GANTI: Master Lists dari berkas; daftar kosong di berkas tidak menghapus', [masterLists.site, masterLists.heavyBrand], [['PT. GPA'], ['LiuGong']]);
        t('GANTI: tercatat di History', history.some(h => h.unitName === '[Restore] Daily Reports' && h.after === '2 written from the file, 1 deleted'), true);
        closeRestoreReport();

        // =============== berkas lama tanpa kedua koleksi ===============
        const v4old = JSON.parse(JSON.stringify(file)); delete v4old.masterLists; delete v4old.licenseRequirements;
        const before = JSON.stringify([masterLists, licenseRequirements]);
        answers = [true];
        importBackup(new File([JSON.stringify(v4old)], 'b.json', { type: 'application/json' }));
        await tick(500);
        const rows2 = [...document.querySelectorAll('#restoreReportBody tr')].map(tr => tr.textContent.replace(/\\s+/g, ' ').trim());
        t('berkas tanpa keduanya: dilaporkan "not in this file", tidak diubah',
          [rows2.filter(r => /^(Master Lists|License Requirements).*not in this file/.test(r)).length, JSON.stringify([masterLists, licenseRequirements]) === before], [2, true]);
        closeRestoreReport();

        // =============== impor CSV lisensi dua kali ===============
        globalLicenseStock = [];
        const csvRows = [
            { 'Date': '2026-09-01', 'Transaction': 'In', 'License Type': 'SF-RTK', 'Quantity': '5', 'Notes': 'PO 1' },
            { 'Date': '2026-09-01', 'Transaction': 'In', 'License Type': 'SF-RTK', 'Quantity': '5', 'Notes': 'PO 1' },
            { 'Date': '2026-09-02', 'Transaction': 'In', 'License Type': 'G5 Advance', 'Quantity': '3', 'Notes': '' }];
        const realPapa = window.Papa;
        window.Papa = { parse: (f, o) => o.complete({ data: csvRows.map(r => ({ ...r })), meta: { fields: Object.keys(csvRows[0]) } }) };
        window.cloud.saveLicenses = () => Promise.resolve();
        handleLicenseCSVImport(new File(['x'], 'l.csv'));
        t('impor pertama: dua pengiriman identik pada hari yang sama tetap dua baris', globalLicenseStock.length, 3);
        handleLicenseCSVImport(new File(['x'], 'l.csv'));
        t('impor ulang berkas yang sama: tidak ada yang digandakan', globalLicenseStock.length, 3);
        t('dan toast-nya menyebut yang dilewati', /3 skipped \\(already in the ledger\\)/.test(toasts[toasts.length - 1]), true);
        csvRows.push({ 'Date': '2026-09-03', 'Transaction': 'In', 'License Type': 'SF-RTK', 'Quantity': '1', 'Notes': '' });
        handleLicenseCSVImport(new File(['x'], 'l.csv'));
        t('berkas yang ditambah satu baris: hanya baris baru yang masuk', globalLicenseStock.length, 4);
        window.Papa = realPapa;

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
