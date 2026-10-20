// Master Lists: Site, Brand, Company. Daftar kosong = bebas seperti dulu.
// Daftar berisi = satu-satunya pilihan di form unit, Bulk Edit, edit
// langsung, impor CSV, form & impor Implements, anggota tim, Clean Up
// Values; ejaan daftar yang disimpan. Rename ikut mengubah record. Data
// Check melaporkan nilai di luar daftar.
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
        const toasts = [], saved = [], memberSaves = [];
        let fail = false, confirmAnswer = true, promptAnswer = null;
        window.showToast = m => toasts.push(String(m));
        window.confirm = () => confirmAnswer;
        window.prompt = () => promptAnswer;
        const last = () => toasts[toasts.length - 1];
        window.cloud = { isReady: true, addHistoryEvents: () => Promise.resolve(),
            saveUnits: () => Promise.resolve(), saveImplement: () => Promise.resolve(), saveImplements: () => Promise.resolve(),
            saveTeamMember: r => { memberSaves.push(r.company); return Promise.resolve(); },
            saveTeamMembers: l => { l.forEach(r => memberSaves.push(r.company)); return Promise.resolve(); },
            saveMasterList: (kind, values) => { saved.push([kind, values.slice()]);
                return fail ? Promise.reject(Object.assign(new Error('x'), { code: 'permission-denied' })) : Promise.resolve(); } };
        const as = doc => { currentUser = { uid: 'u', email: 'u@x.id' }; currentUserDoc = { status: 'active', ...doc }; applyRoleGating(); };
        as({ role: 'owner' });
        hideAuthGates();
        const TR = (i, site, extra) => ({ id: 't' + i, name: 'TR' + i, sn: 'ST' + i, status: 'Good', site, model: '6110B', implement: '',
            yearReceived: '2024', display: 'Good', gps: 'Good', steering: 'Good', jdlink: 'Good', ...extra });
        globalData = [TR(1, 'PT. GPA'), TR(2, 'PT GPA'), TR(3, 'PT. MNM'), TR(4, 'Gudang Lama')];
        globalImplements = [{ id: 'i1', profileName: 'GS', equipmentType: 'Bed Ripper', brand: 'Gessner' },
                            { id: 'i2', profileName: 'BJ', equipmentType: 'Tipping Trailer', brand: 'BJAM' }];
        teamMembers = [{ id: 'm1', name: 'A', company: 'PT. Global Papua Abadi', active: true },
                       { id: 'm2', name: 'B', company: 'PT Global Papua Abadi', active: true }];
        saveToStorage(globalData); saveImplements();
        masterLists = { site: [], brand: [], heavyBrand: [], company: [] };

        // =============== daftar kosong ===============
        t('daftar kosong: bebas', checkMasterValue('site', 'Apa Saja', null), { ok: true, value: 'Apa Saja' });
        t('menu Master Lists tampil untuk owner', document.getElementById('navMasterLists').style.display, '');

        // =============== modal: isi dari data ===============
        openMasterLists('site');
        t('modal terbuka di tab Sites', document.querySelector('#mlTabs .team-tab.active').textContent.trim().replace(/\\s+/g, ' '), 'Sites 0');
        t('tombol isi dari data menyebut jumlahnya', document.getElementById('mlFillBtn').textContent, 'Add the 4 value(s) in use');
        fillMasterFromUsage();
        t('isi dari data: ejaan mirip digabung ke yang terbanyak', saved[saved.length - 1], ['site', ['Gudang Lama', 'PT GPA', 'PT. MNM']]);
        t('daftar lokal ikut', masterLists.site, ['Gudang Lama', 'PT GPA', 'PT. MNM']);
        // yang terbanyak seri → urutan abjad; rename ke ejaan baku
        promptAnswer = 'PT. GPA';
        renameMasterValue('PT GPA');
        t('rename: daftar berubah', masterLists.site, ['Gudang Lama', 'PT. GPA', 'PT. MNM']);
        t('rename: unit ikut berubah (dua ejaan lama)', [globalData[0].site, globalData[1].site], ['PT. GPA', 'PT. GPA']);
        document.getElementById('mlNewValue').value = 'pt. gpa';
        addMasterValue();
        t('tambah duplikat (beda huruf) ditolak', last(), 'Already in the list as "PT. GPA"');
        document.getElementById('mlNewValue').value = 'PT. Baru Jaya';
        addMasterValue();
        t('tambah entri baru', masterLists.site.includes('PT. Baru Jaya'), true);
        confirmAnswer = true;
        removeMasterValue('Gudang Lama');
        t('hapus entri: unit tetap memakai nilainya', [masterLists.site.includes('Gudang Lama'), globalData[3].site], [false, 'Gudang Lama']);
        t('nilai yang dipakai tapi di luar daftar ditampilkan', /In use but not in the list \\(1\\)/.test(document.getElementById('mlOutside').textContent), true);

        // gagal simpan → dikembalikan
        fail = true;
        document.getElementById('mlNewValue').value = 'PT. Gagal';
        addMasterValue();
        await tick(); await tick();
        t('ditolak server: daftar dikembalikan', masterLists.site.includes('PT. Gagal'), false);
        fail = false;
        closeMasterLists();

        // =============== Site wajib dari daftar ===============
        t('ejaan daftar disimpan', checkMasterValue('site', 'pt gpa', null), { ok: true, value: 'PT. GPA' });
        t('di luar daftar ditolak', checkMasterValue('site', 'Kebun X', null).ok, false);
        t('nilai lama boleh dipertahankan', checkMasterValue('site', 'Gudang Lama', 'Gudang Lama').ok, true);
        goNav('editUnits', 'tractor');
        showAddForm();
        t('datalist site = isi daftar', [...document.querySelectorAll('#siteList option')].map(o => o.value), ['PT. Baru Jaya', 'PT. GPA', 'PT. MNM']);
        document.getElementById('formName').value = 'N1'; document.getElementById('formSN').value = 'SN-N1';
        document.getElementById('formModel').value = '6110B';
        document.getElementById('formSite').value = 'Kebun X';
        saveUnit(new Event('submit'));
        t('form: site di luar daftar ditolak', [globalData.some(u => u.sn === 'SN-N1'), /not in the Site list/.test(last())], [false, true]);
        document.getElementById('formSite').value = 'pt. mnm';
        saveUnit(new Event('submit'));
        t('form: ejaan daftar disimpan', (globalData.find(u => u.sn === 'SN-N1') || {}).site, 'PT. MNM');
        renderEditTable();
        const cell = document.querySelector('#editBody [data-id="t3"][data-field="site"]');
        cell.textContent = 'Somewhere'; saveInlineEdit(cell);
        t('inline: di luar daftar dikembalikan', globalData.find(u => u.id === 't3').site, 'PT. MNM');
        selectedUnitIds = new Set(['t3']);
        openBulkEdit();
        document.getElementById('bulkChkSite').checked = true;
        document.getElementById('bulkSite').value = 'pt. baru jaya';
        applyBulkEdit();
        t('bulk: ejaan daftar disimpan', globalData.find(u => u.id === 't3').site, 'PT. Baru Jaya');
        selectedUnitIds = new Set();

        // CSV unit
        let report = null;
        const realReport = showImportReport, realPapa = window.Papa;
        window.showImportReport = r => { report = r; };
        window.Papa = { parse: (f, o) => o.complete({ data: [
            { 'Nickname': 'C1', 'Serial Number': 'SN-C1', 'Model': 'x', 'Site': 'pt gpa' },
            { 'Nickname': 'C2', 'Serial Number': 'SN-C2', 'Model': 'x', 'Site': 'Nowhere' }],
            meta: { fields: ['Nickname', 'Serial Number', 'Model', 'Site'] } }) };
        handleEditCSVImport(new File(['x'], 'u.csv'));
        window.showImportReport = realReport; window.Papa = realPapa;
        t('CSV: dikenal → ejaan daftar; asing → kosong + dicatat',
          [(globalData.find(u => u.sn === 'SN-C1') || {}).site, (globalData.find(u => u.sn === 'SN-C2') || {}).site,
           report.notes.some(n => /Site "Nowhere" is not in the Site list — left empty/.test(n.reason))], ['PT. GPA', '', true]);

        // =============== Brand ===============
        masterLists.brand = ['BJAM', 'Gessner'];
        navigateTo('implements');
        showAddImplementForm();
        document.getElementById('implProfileName').value = 'New';
        document.getElementById('implEquipmentType').value = 'Zonal Ripper';
        document.getElementById('implBrand').value = 'Unknown Brand';
        const nImpl = globalImplements.length;
        saveImplement(new Event('submit'));
        t('Implements: brand di luar daftar ditolak', [globalImplements.length, /not in the Implement Brand list/.test(last())], [nImpl, true]);
        document.getElementById('implBrand').value = 'gessner';
        saveImplement(new Event('submit'));
        t('Implements: ejaan daftar disimpan', (globalImplements.find(i => i.profileName === 'New') || {}).brand, 'Gessner');
        closeImplementModal();
        // rename brand ikut mengubah implement dan unit yang memakainya
        globalData.push(TR(9, 'PT. GPA', { implement: 'Bed Ripper — Gessner' }));
        openMasterLists('brand');
        promptAnswer = 'Gessner AU';
        renameMasterValue('Gessner');
        t('rename brand: record Implements & unit ikut', [globalImplements.find(i => i.id === 'i1').brand, globalData.find(u => u.id === 't9').implement],
          ['Gessner AU', 'Bed Ripper — Gessner AU']);
        closeMasterLists();

        // =============== Company ===============
        masterLists.company = ['PT. Global Papua Abadi'];
        openTeamMembersModal();
        t('datalist company = isi daftar', [...document.querySelectorAll('#companyList option')].map(o => o.value), ['PT. Global Papua Abadi']);
        setMemberCompany('m1', 'PT. Lain');
        t('anggota: company di luar daftar ditolak', [teamMembers.find(m => m.id === 'm1').company, /not in the Company list/.test(last())], ['PT. Global Papua Abadi', true]);
        setMemberCompany('m2', 'pt global papua abadi');
        t('anggota: ejaan daftar disimpan', teamMembers.find(m => m.id === 'm2').company, 'PT. Global Papua Abadi');
        closeTeamMembersModal();

        // =============== Data Check & Clean Up ===============
        const items = dcNotInMasterLists().map(i => i.label);
        t('Data Check: nilai di luar daftar', items, ['Site: Gudang Lama']);
        openValueMerge('site', ['Gudang Lama']);
        t('Clean Up: saran target = isi daftar', [...document.querySelectorAll('#vmTargetList option')].map(o => o.value), ['PT. Baru Jaya', 'PT. GPA', 'PT. MNM']);
        document.getElementById('vmTarget').value = 'Kebun Baru'; renderValueMerge(); applyValueMerge();
        t('Clean Up: target di luar daftar ditolak', globalData[3].site, 'Gudang Lama');
        document.getElementById('vmTarget').value = 'pt. gpa'; renderValueMerge(); applyValueMerge();
        t('Clean Up: ejaan daftar disimpan', globalData[3].site, 'PT. GPA');
        closeValueMerge();

        // =============== hak akses ===============
        as({ role: 'staff', access: { teamMembers: 'edit' } });
        openMasterLists();
        t('editor anggota: hanya tab Companies', [...document.querySelectorAll('#mlTabs .team-tab')].map(x => x.textContent.trim().split(' ')[0]), ['Companies']);
        t('editor anggota: Companies bisa diubah', document.getElementById('mlAddForm').style.display, '');
        closeMasterLists();
        as({ role: 'staff', access: { editUnits: 'view' } });
        openMasterLists('site');
        t('akses lihat: tanpa form tambah', document.getElementById('mlAddForm').style.display, 'none');
        const n = saved.length;
        document.getElementById('mlNewValue').value = 'X';
        addMasterValue();
        t('akses lihat: tidak bisa menambah', saved.length, n);
        closeMasterLists();
        as({ role: 'khl', access: {} });
        t('tanpa akses: menu tersembunyi', document.getElementById('navMasterLists').style.display, 'none');

        // XSS
        as({ role: 'owner' });
        masterLists.site = ['<img src=x onerror=window.__pwn=1>'];
        openMasterLists('site');
        await tick();
        t('nama berisi markup tidak dirender', [document.querySelectorAll('#masterListsModal img').length, window.__pwn || 0], [0, 0]);

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
