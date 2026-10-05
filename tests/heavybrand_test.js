// Brand pada unit Heavy Equipment: kolom di Unit Database & dashboard, form,
// impor/ekspor CSV, profil, kartu kelengkapan, dan daftar Brand di Master
// Lists (dipakai bersama Implements). Unit Agricultural tidak menyimpan brand.
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
        const toasts = [];
        window.showToast = m => toasts.push(String(m));
        window.confirm = () => true;
        let promptAnswer = null; window.prompt = () => promptAnswer;
        const last = () => toasts[toasts.length - 1];
        window.cloud = { isReady: false };
        currentUser = { uid: 'o', email: 'o@x.id' }; currentUserDoc = { role: 'owner', status: 'active' };
        hideAuthGates(); applyRoleGating();
        const HV = (i, extra) => ({ id: 'h' + i, name: 'BD0' + i, sn: 'LGCB' + i, unitGroup: 'heavy', status: 'Good', model: 'CLGB230',
            machineType: 'Bulldozer', assetCode: 'LC GPA', workTool: 'Root Plough', site: 'PT. GPA', yearReceived: '2025',
            cameraAi: 'Good', telematicBox: 'Good', switchLimiter: 'Good', rotaryLamp: 'Good', ...extra });
        globalData = [HV(1, { brand: 'LiuGong' }), HV(2), { id: 't1', name: 'TR1', sn: 'ST1', status: 'Good', site: 'PT. GPA' }];
        globalImplements = [{ id: 'i1', profileName: 'GS', equipmentType: 'Bed Ripper', brand: 'Gessner' }];
        saveToStorage(globalData); saveImplements();
        masterLists = { site: [], brand: [], company: [] };

        // =============== tabel ===============
        goNav('editUnits', 'heavy');
        const heads = [...document.querySelectorAll('#editTable thead th')].map(th => th.textContent.trim());
        t('kolom Brand sebelum Model', heads.slice(3, 5), ['Brand', 'Model']);
        const cell = id => document.querySelector('#editBody [data-id="' + id + '"][data-field="brand"]');
        t('isi brand tampil dan bisa diedit langsung', [cell('h1').textContent, cell('h1').getAttribute('contenteditable')], ['LiuGong', 'true']);
        t('baris kosong memakai colspan baru', EDIT_COLSPAN.heavy, document.querySelectorAll('#editTable thead th').length);
        cell('h2').textContent = 'LiuGong';
        saveInlineEdit(cell('h2'));
        t('edit langsung tanpa daftar Brand: tersimpan', globalData.find(u => u.id === 'h2').brand, 'LiuGong');

        // kelengkapan
        globalData.find(u => u.id === 'h2').brand = '';
        renderEditTable();
        t('kartu kelengkapan: chip "no brand"', [...document.querySelectorAll('#dataQuality .dq-chip')].map(c => c.textContent.trim().replace(/\\s+/g, ' ')), ['1 no brand']);
        t('Data Check: Heavy tanpa brand', dcMissingKeyFields().map(i => i.label).includes('Heavy: no brand'), true);

        // =============== form ===============
        editUnit('h2');
        t('form alat berat punya Brand', document.getElementById('formHeavyBrand').offsetParent !== null, true);
        document.getElementById('formHeavyBrand').value = 'Komatsu';
        saveUnit(new Event('submit'));
        t('form: brand tersimpan', globalData.find(u => u.id === 'h2').brand, 'Komatsu');

        // dashboard + profil
        goNav('dashboard', 'heavy');
        t('dashboard alat berat menampilkan Brand', [...document.querySelectorAll('#detailTable thead th')].map(th => th.textContent.trim()).slice(2, 4), ['Brand', 'Model']);
        showUnitProfile('h1');
        t('profil menampilkan Brand', /Brand\\s*LiuGong/.test(document.getElementById('unitProfileModal').textContent), true);
        closeUnitProfile();

        // unit Agricultural tidak menyimpan brand
        t('Agricultural: brand ditolak oleh firewall kelompok', updateUnit('t1', { brand: 'John Deere' }), false);

        // =============== daftar Brand (Master Lists) ===============
        masterLists.brand = ['Gessner', 'LiuGong'];
        t('pemakaian Brand menghitung Implements + alat berat', [...masterUsage('brand').entries()], [['Gessner', 1], ['LiuGong', 1], ['Komatsu', 1]]);
        goNav('editUnits', 'heavy');
        editUnit('h2');
        document.getElementById('formHeavyBrand').value = 'Sany';
        document.getElementById('formSite').value = 'PT. MNM';
        saveUnit(new Event('submit'));
        t('daftar Brand berisi: brand baru di luar daftar ditolak', [globalData.find(u => u.id === 'h2').site, /not in the Brand list/.test(last())], ['PT. GPA', true]);
        document.getElementById('formHeavyBrand').value = 'Komatsu';
        saveUnit(new Event('submit'));
        t('nilai lama di luar daftar boleh dipertahankan', globalData.find(u => u.id === 'h2').site, 'PT. MNM');
        closeModal();
        cell('h1').textContent = 'liugong';
        saveInlineEdit(cell('h1'));
        t('edit langsung: ejaan daftar', globalData.find(u => u.id === 'h1').brand, 'LiuGong');
        openMasterLists('brand');
        promptAnswer = 'Liugong';
        // rename yang hanya beda huruf tetap dianggap entri yang sama
        promptAnswer = 'LiuGong Machinery';
        renameMasterValue('LiuGong');
        t('rename brand mengubah unit alat berat', globalData.find(u => u.id === 'h1').brand, 'LiuGong Machinery');
        t('Implements tidak tersentuh', globalImplements[0].brand, 'Gessner');
        closeMasterLists();
        t('Data Check: Komatsu di luar daftar', dcNotInMasterLists().map(i => i.label), ['Brand: Komatsu']);
        openValueMerge('brand', ['Komatsu']);
        t('Clean Up Values: field Brand (Heavy)', document.getElementById('vmField').value, 'brand');
        closeValueMerge();

        // =============== CSV ===============
        let csv = null;
        const RealBlob = window.Blob;
        window.Blob = function (parts, o) { csv = parts.join(''); return new RealBlob(parts, o); };
        exportUnitsCSVGrouped(unitsOfGroup(globalData, 'heavy'), true);
        window.Blob = RealBlob;
        t('ekspor: header Brand', csv ? csv.split(/\\r?\\n/)[0].split(',').slice(2, 5).map(x => x.replace(/"/g, '')) : null, ['Nickname', 'Brand', 'Model']);
        let report = null;
        const realReport = showImportReport, realPapa = window.Papa;
        window.showImportReport = r => { report = r; };
        window.Papa = { parse: (f, o) => o.complete({ data: [
            { 'Unit Group': 'heavy', 'Nickname': 'BD09', 'Serial Number': 'LGCB9', 'Model': 'CLGB230', 'Brand': 'gessner', 'Camera AI': 'Good' },
            { 'Unit Group': 'heavy', 'Nickname': 'BD10', 'Serial Number': 'LGCB10', 'Model': 'CLGB230', 'Brand': 'Unknown', 'Camera AI': 'Good' },
            { 'Unit Group': 'tractor', 'Nickname': 'TR9', 'Serial Number': 'ST9', 'Model': '6110B', 'Brand': 'John Deere', 'GPS': 'Good' }],
            meta: { fields: ['Unit Group', 'Nickname', 'Serial Number', 'Model', 'Brand', 'Camera AI', 'GPS'] } }) };
        handleEditCSVImport(new File(['x'], 'u.csv'));
        window.showImportReport = realReport; window.Papa = realPapa;
        t('impor: brand dikenali → ejaan daftar', (globalData.find(u => u.sn === 'LGCB9') || {}).brand, 'Gessner');
        t('impor: brand asing → kosong + dicatat', [(globalData.find(u => u.sn === 'LGCB10') || {}).brand || '',
            report.notes.some(n => /Brand "Unknown" is not in the Brand list — left empty/.test(n.reason))], ['', true]);
        t('impor: kolom Brand pada traktor diabaikan tanpa peringatan', [(globalData.find(u => u.sn === 'ST9') || {}).brand,
            report.notes.filter(n => n.sn === 'ST9').map(n => n.reason)], [undefined, []]);

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
