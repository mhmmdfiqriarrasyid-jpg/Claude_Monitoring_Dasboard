// v149: (1) "ejaan mirip" tidak lagi menyamakan nama yang memang berbeda
// (Afdeling I/II, Blok A/B, 6110B/6110M); (2) "Add the values in use" hanya
// melipat beda huruf/spasi/tanda baca; (3) merge jenis/brand implement saat
// daftar Implements dipakai mengganti nama rekamannya, unit tetap di daftar,
// syarat lisensi ikut pindah, Undo mengembalikan semuanya; (4) mengubah jenis
// di form Implements ikut menulis ulang unit; (5) alat massal hanya mengirim
// field yang berubah.
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
        let confirmText = '';
        window.confirm = m => { confirmText = String(m); return true; };
        const fieldWrites = [], fullWrites = [], memberWrites = [], implWrites = [], reqSaved = [], reqDeleted = [], masterSaved = [];
        window.cloud = { isReady: true, addHistoryEvents: () => Promise.resolve(),
            saveUnits: l => { fullWrites.push(l); return Promise.resolve(); },
            updateUnitsFields: l => { fieldWrites.push(l.map(x => ({ ...x }))); return Promise.resolve(); },
            updateTeamMembersFields: l => { memberWrites.push(l.map(x => ({ ...x }))); return Promise.resolve(); },
            saveImplement: r => { implWrites.push([r.id, r.equipmentType, r.brand]); return Promise.resolve(); },
            saveLicenseRequirement: r => { reqSaved.push([r.id, r.gps, r.display]); return Promise.resolve(); },
            deleteLicenseRequirement: id => { reqDeleted.push(id); return Promise.resolve(); },
            saveMasterList: (k, v) => { masterSaved.push([k, v.slice()]); return Promise.resolve(); } };
        currentUser = { uid: 'o', email: 'o@x.id' }; currentUserDoc = { role: 'owner', status: 'active' };
        hideAuthGates(); applyRoleGating();

        // =============== (1) kemiripan ===============
        const pairs = [['Afdeling I', 'Afdeling II'], ['Divisi I', 'Divisi II'], ['Blok A', 'Blok B'], ['Estate A', 'Estate B'],
                       ['6110B', '6110M'], ['PT. GPA', 'PT. GPB'], ['Blok A1', 'Blok A2'], ['ISJ', 'ISS']];
        t('nama yang memang berbeda TIDAK dianggap salah ketik', pairs.filter(([a, b]) => similarSpelling(a, b)), []);
        const same = [['Gessner', 'Geesner'], ['Bucket', 'Buket'], ['PT. GPA', 'PT GPA'], ['PT. Global Papua Abadi', 'PT Global Papua Abadi'],
                      ['Zonal Ripper — Gessner', 'Zonal Ripper — Geesner'], ['Lime Spreader', 'Lime Spreeder']];
        t('salah ketik sungguhan tetap dikenali', same.filter(([a, b]) => !similarSpelling(a, b)), []);
        const afd = new Map([['Afdeling I', 30], ['Afdeling II', 25], ['Afdeling III', 20], ['Afdeling IV', 10], ['Afdeling V', 5]]);
        t('Afdeling I..V tidak dirangkai jadi satu kelompok', similarValueClusters(afd), []);

        // =============== (2) isi Master Lists dari data ===============
        const TR = (i, x) => ({ id: 't' + i, name: 'TR' + i, sn: 'S' + i, status: 'Good', model: '6110B', yearReceived: '2024', ...x });
        globalData = [TR(1, { site: 'Afdeling I' }), TR(2, { site: 'Afdeling II' }), TR(3, { site: 'Afdeling III' }),
                      TR(4, { site: 'PT. GPA' }), TR(5, { site: 'PT GPA' }), TR(6, { site: 'PT. GPA' }), TR(7, { site: 'Gesner Camp' }), TR(8, { site: 'Gessner Camp' })];
        saveToStorage(globalData);
        masterLists = { site: [], brand: [], heavyBrand: [], company: [] };
        openMasterLists('site');
        fillMasterFromUsage();
        t('semua afdeling masuk; PT GPA/PT. GPA sekali (yang terbanyak); salah ketik tetap masuk',
          masterSaved[masterSaved.length - 1], ['site', ['Afdeling I', 'Afdeling II', 'Afdeling III', 'Gesner Camp', 'Gessner Camp', 'PT. GPA']]);
        t('konfirmasi menunjuk yang mirip agar dicek', /look alike[\\s\\S]*Gessner Camp \\/ Gesner Camp|look alike[\\s\\S]*Gesner Camp \\/ Gessner Camp/.test(confirmText), true);
        closeMasterLists();

        // =============== (3) merge jenis implement di daftar Implements ===============
        masterLists = { site: [], brand: [], heavyBrand: [], company: [] };
        globalImplements = [{ id: 'i1', profileName: 'BR', equipmentType: 'Bed Riper', brand: 'Gessner' },
                            { id: 'i2', profileName: 'BR2', equipmentType: 'Bed Ripper', brand: 'ISJ' }];
        globalData = [TR(1, { site: 'A', implement: 'Bed Riper — Gessner' }), TR(2, { site: 'A', implement: 'BR' }),
                      TR(3, { site: 'A', implement: 'Bed Ripper — ISJ', status: 'Breakdown' })];
        licenseRequirements = [{ id: 'bed-riper', type: 'Bed Riper', gps: 'SF-RTK', display: 'G5 Advance' }];
        saveToStorage(globalData); saveImplements();
        goNav('editUnits', 'tractor');
        fieldWrites.length = 0; fullWrites.length = 0;
        openValueMerge('implType', ['Bed Riper', 'Bed Ripper']);
        document.getElementById('vmTarget').value = 'Bed Ripper'; renderValueMerge();
        applyValueMerge();
        t('rekaman Implements ikut diganti namanya', globalImplements.map(i => i.equipmentType), ['Bed Ripper', 'Bed Ripper']);
        t('unit memakai ejaan daftar dan tetap ada di daftar', globalData.map(u => [u.implement, checkImplementValue(u.implement, null).ok]),
          [['Bed Ripper — Gessner', true], ['Bed Ripper — Gessner', true], ['Bed Ripper — ISJ', true]]);
        t('syarat lisensi pindah ke nama baru', [licenseRequirements.map(r => [r.id, r.display]), reqDeleted], [[['bed-ripper', 'G5 Advance']], ['bed-riper']]);
        t('unit tetap masuk Purchase Plan', licensePlanCompute('2026-12-31').rows.length, 3);
        t('hanya field implement yang dikirim untuk unit', [fieldWrites.flat().map(x => Object.keys(x).sort().join(',')), fullWrites.length],
          [['id,implement', 'id,implement'], 0]);
        undoValueMerge();
        t('Undo mengembalikan rekaman, unit, dan syarat', [globalImplements[0].equipmentType, globalData[0].implement, globalData[1].implement, licenseRequirements.map(r => r.id)],
          ['Bed Riper', 'Bed Riper — Gessner', 'BR', ['bed-riper']]);
        closeValueMerge();

        // =============== (4) form Implements: ganti jenis ===============
        navigateTo('implements');
        reqDeleted.length = 0;
        editImplement('i1');
        document.getElementById('implEquipmentType').value = 'Bed Ripper Pro';
        saveImplement(new Event('submit'));
        t('unit yang memakai rekaman itu ikut nama baru', [globalData[0].implement, globalData[1].implement], ['Bed Ripper Pro — Gessner', 'Bed Ripper Pro — Gessner']);
        t('syarat lisensi ikut pindah', licenseRequirements.map(r => r.id), ['bed-ripper-pro']);
        t('unit lain tidak tersentuh', globalData[2].implement, 'Bed Ripper — ISJ');

        // =============== (5) anggota tim: hanya field yang berubah ===============
        teamMembers = [{ id: 'm1', name: 'A', jobTitle: 'Operator', company: 'PT Global Papua Abadi', active: true },
                       { id: 'm2', name: 'B', jobTitle: 'Helper', company: 'PT. Global Papua Abadi', active: true }];
        openValueMerge('company', ['PT Global Papua Abadi', 'PT. Global Papua Abadi']);
        document.getElementById('vmTarget').value = 'PT. Global Papua Abadi'; renderValueMerge();
        applyValueMerge();
        t('merge company: hanya company + updatedAt yang dikirim', memberWrites[memberWrites.length - 1].map(x => Object.keys(x).sort().join(',')), ['company,id,updatedAt']);
        closeValueMerge();

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
