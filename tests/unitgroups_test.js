// Unit dibagi dua kelompok: Agricultural Equipment dan Heavy Equipment.
//
// Pemetaan sebelum perubahan ini menemukan 59 kerusakan terverifikasi yang akan
// terjadi begitu satu alat berat masuk — 21 di antaranya korupsi data. Yang
// paling berbahaya: membuka Edit pada excavator lalu Simpan menulis
// 'Breakdown' ke empat komponen John Deere; migrasi lisensi lama menempelkan
// SF-RTK ke excavator; detectIssues membuat setiap alat berat terbaca rusak.
//
// Berkas ini membuktikan dua hal:
//   1. SELAMA BELUM ADA ALAT BERAT, tidak ada satu tampilan traktor pun yang
//      berubah — dibandingkan byte demi byte dengan rekaman dari kode lama
//      (tests/fixtures/tractor_golden.json, direkam di commit sebelumnya).
//   2. Setiap akar penyebab kerusakan itu benar-benar tertutup.
const { launch, BASE_URL } = require('./_env');
const { CAPTURE, FIXED_TIME } = require('./_golden');
const golden = require('./fixtures/tractor_golden.json');

const T = [];
const t = (n, g, w) => T.push({ n, g, w, pass: JSON.stringify(g) === JSON.stringify(w) });
const errors = [];

async function freshPage(b) {
    const ctx = await b.newContext({ viewport: { width: 1440, height: 900 } });
    const page = await ctx.newPage();
    await page.clock.setFixedTime(FIXED_TIME);
    page.on('pageerror', e => { if (!/Chart is not defined/.test(e.message)) errors.push(e.message); });
    await page.goto(BASE_URL + '/index.html', { waitUntil: 'load' });
    await page.waitForTimeout(900);
    return { ctx, page };
}

// Setelan standar di dalam halaman: owner, cloud palsu yang merekam tulisan.
const SETUP = `(() => {
    window.__w = { units: [], damages: [], history: [], toasts: [], asked: [] };
    window.cloud = { isReady: true,
        saveUnits: u => { __w.units.push(JSON.parse(JSON.stringify(u))); return Promise.resolve(); },
        deleteUnits: () => Promise.resolve(), getAllUnits: () => Promise.resolve([]),
        addHistoryEvents: e => { __w.history.push(...e); return Promise.resolve(); },
        subscribeUsers: () => () => {},
        saveDamage: d => { __w.damages.push(JSON.parse(JSON.stringify(d))); return Promise.resolve(); },
        saveDamages: () => Promise.resolve(), saveLicense: () => Promise.resolve(), saveLicenses: () => Promise.resolve() };
    window.showToast = (m, k) => { __w.toasts.push(String(m)); };
    window.__answer = true;
    window.confirm = m => { __w.asked.push(String(m)); return window.__answer; };
    currentUser = { uid: 'uO', email: 'o@x.id' };
    currentUserDoc = { role: 'owner', status: 'active', displayName: 'Owner' };
    try { localStorage.setItem('tractorLicenseDatesApplied_v2', '1'); } catch (_) {}
    hideAuthGates(); applyRoleGating();
    window.__T = (id, x) => Object.assign({ id, name: 'GGTR' + id, model: 'JOHN DEERE 6110B', sn: 'SN' + id,
        implement: 'Plough', status: 'Good', display: 'Good', gps: 'Good', steering: 'Good', jdlink: 'Good',
        site: 'PT. GPA', yearReceived: '2021', userCategory: '', gpsLicense: 'SF-1', licenseDisplay: 'G5 Basic',
        remarks: '', breakdownReason: '', downtimeHistory: [] }, x || {});
    window.__H = (id, x) => Object.assign({ id, name: 'EXC' + id, model: 'KOMATSU PC200', sn: 'KMT' + id,
        unitGroup: 'heavy', machineType: 'Excavator', assetCode: 'LB-' + id, workTool: 'Bucket',
        status: 'Good', cameraAi: 'Good', telematicBox: 'Good', switchLimiter: 'Good', rotaryLamp: 'Good',
        site: 'PT. GPA', yearReceived: '2025', userCategory: '', remarks: '', breakdownReason: '', downtimeHistory: [] }, x || {});
})()`;

(async () => {
    const b = await launch();

    // ============ 1. INVARIAN (a): tanpa alat berat = golden ============
    {
        const { ctx, page } = await freshPage(b);
        const out = await page.evaluate(CAPTURE);
        // Perbedaan yang DIRENCANAKAN, dan hanya itu:
        //  - unit baru dari CSV kini membawa unitGroup:'tractor' (hanya
        //    penciptaan yang menulis kelompok) dan processData mengembalikan
        //    groupWarnings kosong;
        //  - Periksa Data punya dua pemeriksaan baru, keduanya 0 temuan.
        const expCsv = JSON.parse(JSON.stringify(golden.csvImport));
        expCsv.valid.forEach(u => { u.unitGroup = 'tractor'; });
        expCsv.groupWarnings = [];
        const expChecks = golden.dataChecks.concat([['kelompok', 0], ['kelompok-ref', 0]]);
        Object.keys(golden).forEach(k => {
            const want = k === 'csvImport' ? expCsv : k === 'dataChecks' ? expChecks : golden[k];
            t(`tanpa alat berat identik dengan kode lama: ${k}`, out[k], want);
        });
        const ui = await page.evaluate(() => ({
            bar: document.getElementById('dashGroupBar').style.display,
            cap: document.getElementById('dashScopeCaption').hidden
        }));
        t('pemilih grup dashboard tersembunyi tanpa alat berat', ui.bar, 'none');
        t('keterangan cakupan tersembunyi tanpa alat berat', ui.cap, true);
        await ctx.close();
    }

    // ============ 2. INVARIAN (b): + 2 alat berat bersih, cakupan Pertanian ============
    {
        const { ctx, page } = await freshPage(b);
        await page.evaluate(`window.__extraUnits = [
            { id:'u_h1', unitGroup:'heavy', name:'EXC01', model:'KOMATSU PC200', sn:'KMT1', machineType:'Excavator',
              assetCode:'LB-1', workTool:'Bucket', status:'Good', cameraAi:'Good', telematicBox:'Good',
              switchLimiter:'Good', rotaryLamp:'Good', site:'PT. GPA', yearReceived:'2025', userCategory:'Planting',
              remarks:'', breakdownReason:'' },
            { id:'u_h2', unitGroup:'heavy', name:'DZR01', model:'CAT D6', sn:'CAT1', machineType:'Bulldozer',
              assetCode:'LB-2', workTool:'Blade', status:'Good', cameraAi:'Good', telematicBox:'Good',
              switchLimiter:'Good', rotaryLamp:'Good', site:'PT. MNM', yearReceived:'2025', userCategory:'Planting',
              remarks:'', breakdownReason:'' }];`);
        const out = await page.evaluate(CAPTURE);
        // Dashboard dicakup ke Pertanian: setiap angka traktor harus sama.
        ['text:kpiTotal','text:kpiGood','text:kpiBreakdown','text:kpiIssue','text:kpiIssuePct','text:kpiHealth',
         'text:dashNarrative','html:componentGrid','html:issueSummary','html:repairBody','html:licenseAlertsCards',
         'html:detailBody','html:detailHead','html:componentFilter','html:issueFilter','alertList','decisionLicense']
            .forEach(k => t(`dengan 2 alat berat, cakupan Pertanian tetap sama: ${k}`, out[k], golden[k]));
        const extra = await page.evaluate(() => ({
            bar: document.getElementById('dashGroupBar').style.display,
            mtbf: JSON.stringify(computeDowntimeStats(scopeDashUnits()))
        }));
        t('pemilih grup muncul begitu dua kelompok ada', extra.bar, '');
        t('MTBF cakupan Pertanian sama dengan sebelum ada alat berat', extra.mtbf, JSON.stringify(golden.downtime));
        await ctx.close();
    }

    // ============ 3..17 dalam satu halaman ============
    const { ctx, page } = await freshPage(b);
    await page.evaluate(SETUP);
    const R = await page.evaluate(`(async () => { try {
        const r = {};
        const flush = () => new Promise(res => setTimeout(res, 20));

        // ---------- 3. preferensi tersimpan tanpa alat berat ----------
        globalData = [__T('u_t1')];
        editUnitsGroup = 'heavy'; _editGroupPicked = false; dashGroupPref = 'all';
        r.prefEdit = effectiveEditGroup();
        r.prefDash = effectiveDashGroup();

        // ---------- 5. tab: halaman lain tidak menyembunyikannya ----------
        globalData = [__T('u_t1'), __H('u_h1')];
        navigateTo('team');
        navigateTo('editUnits');
        r.euTabsHidden = [...document.querySelectorAll('.eu-tab')].filter(x => x.style.display === 'none').length;
        // I8: sortTable tidak boleh mengurutkan globalData itu sendiri.
        navigateTo('dashboard');
        globalData = [__T('u_t2', {name:'ZZZ'}), __H('u_h1'), __T('u_t1', {name:'AAA'})];
        setDashGroup('all');
        const before = globalData.map(u => u.id).join();
        sortTable('name');
        r.aliasOk = globalData.map(u => u.id).join() === before;

        // ---------- 6. firewall ----------
        globalData = [__T('u_t1'), __H('u_h1')];
        __w.units.length = 0;
        r.fwGpsOnHeavy = updateUnit('u_h1', { gps: 'Breakdown' });
        r.fwCamOnTractor = updateUnit('u_t1', { cameraAi: 'Good' });
        r.fwGroupMove = updateUnit('u_t1', { unitGroup: 'heavy' });
        r.fwWrites = __w.units.length;
        r.fwNormal = updateUnit('u_t1', { name: 'X' });
        r.fwNoGroupKey = !('unitGroup' in globalData.find(u => u.id === 'u_t1'));
        const addRes = addUnits([{ id: 'u_bad', name: 'B', sn: 'SNB', unitGroup: 'Heavyy' }]);
        r.addUnknown = addRes.skippedDetails.map(x => x.reason);
        const addCross = addUnits([{ id: 'u_t1', name: 'C', sn: 'SNC', unitGroup: 'heavy' }]);
        r.addCross = addCross.skippedDetails.map(x => x.reason);
        r.clearStrayBlocked = updateUnit('u_h1', { gps: 'X' }, { clearStray: true });

        // ---------- 7. DC1 / DC2 / DC6: form ----------
        // navigateTo memuat ulang global dari localStorage, jadi semai SESUDAHNYA.
        navigateTo('editUnits');
        globalData = [__T('u_t1'), __H('u_h1', { cameraAi: '', telematicBox: '' })];
        switchEditUnitsGroup('heavy');
        __w.units.length = 0; __w.history.length = 0;
        editUnit('u_h1');
        r.blankShown = document.getElementById('formCameraAi').value;
        r.jdDisabled = document.getElementById('formGPS').disabled;
        saveUnit(new Event('submit'));
        await flush();
        const pushed = (__w.units[0] || [])[0] || {};
        r.dc1NoJd = ['display','gps','steering','jdlink','gpsLicense','licenseDisplay'].filter(k => k in pushed);
        // updateUnit mengirim seluruh dokumen, jadi '' yang memang sudah
        // tersimpan ikut terkirim. Yang dijaga: nilainya tidak dipalsukan.
        r.dc1BlankKept = [pushed.cameraAi, pushed.telematicBox];
        r.dc1NoCompHistory = __w.history.filter(h => ['cameraAi','telematicBox'].includes(h.field)).length;
        r.dc1History = __w.history.filter(h => ['display','gps','steering','jdlink'].includes(h.field)).length;

        // Tambah alat berat baru berstatus Breakdown → lewat stash alasan.
        __w.units.length = 0;
        showAddForm();
        document.getElementById('formName').value = 'EXC-NEW';
        document.getElementById('formSN').value = 'KMTNEW';
        document.getElementById('formStatus').value = 'Breakdown';
        saveUnit(new Event('submit'));
        document.getElementById('breakdownReasonText').value = 'Hidrolik';
        confirmBreakdownReason();
        await flush();
        const nu = globalData.find(u => u.sn === 'KMTNEW') || {};
        r.dc6Group = nu.unitGroup;
        r.dc2Components = HEAVY_COMPONENT_KEYS.map(k => nu[k]);
        r.dc2NoJd = ['display','gps','steering','jdlink','implement'].filter(k => k in nu);

        // Tambah traktor: kunci hari ini + unitGroup:'tractor'.
        switchEditUnitsGroup('tractor');
        showAddForm();
        document.getElementById('formName').value = 'GGTR-NEW';
        document.getElementById('formSN').value = 'SNNEW';
        saveUnit(new Event('submit'));
        await flush();
        const nt = globalData.find(u => u.sn === 'SNNEW') || {};
        r.tractorAdd = [nt.unitGroup, nt.display, nt.gps, 'cameraAi' in nt];

        // ---------- 8. DC3: penulis otomatis ----------
        r.defaultsGone = typeof window.applyDefaultLicensesIfNeeded;
        globalData = [__H('u_h9', { gpsLicense: 'SF-RTK', gpsLicenseEndDate: '2020-01-01' })];
        __w.units.length = 0;
        const downs = applyExpiredLicenseDowngrades();
        r.downgradeHeavy = [downs, __w.units.length];

        // ---------- 9. DC4 / DC5 / UI3: kerusakan ----------
        globalData = [__T('u_t1'), __H('u_h1')];
        globalDamages = [];
        r.targetGpsHeavy = damageTargetField('Device Precision', 'GPS', globalData[1]);
        r.targetCamHeavy = damageTargetField('Device Precision', 'Camera AI', globalData[1]);
        r.targetGpsTractor = damageTargetField('Device Precision', 'GPS', globalData[0]);
        r.targetMechHeavy = damageTargetField('Mekanis', 'Hidrolik', globalData[1]);
        r.conflictGpsHeavy = componentGroupConflict('GPS', globalData[1]);
        r.conflictCamTractor = componentGroupConflict('Camera AI', globalData[0]);
        __w.units.length = 0;
        _applyDamageBreakdown('u_h1', 'Device Precision', 'Camera AI', 'x');
        r.camBreak = globalData.find(u => u.id === 'u_h1').cameraAi;
        const legacy = { id: 'dL', unitId: 'u_h1', unitName: 'EXCu_h1', sn: 'KMTu_h1', damageType: 'Device Precision',
                         component: 'GPS', resolved: false, date: '2026-09-01' };
        globalDamages = [legacy];
        __w.units.length = 0;
        resolveDamage('dL');
        r.legacyResolveWrites = __w.units.length;
        r.legacyResolved = globalDamages[0].resolved;
        r.addCamComponent = (() => { const inp = document.getElementById('newComponentName');
            if (!inp) return 'no-input'; inp.value = 'Camera AI'; __w.toasts.length = 0;
            addDamageComponent(new Event('submit')); return __w.toasts.join('|'); })();

        // Pilihan komponen mengikuti unit.
        document.getElementById('dmgUnit').value = damageUnitLabel(globalData[1]);
        renderDamageComponentOptions();
        r.heavyOpts = [...document.getElementById('dmgComponent').options].map(o => o.value).filter(Boolean);
        document.getElementById('dmgComponent').value = 'Camera AI';
        document.getElementById('dmgUnit').value = damageUnitLabel(globalData[0]);
        r.droppedOnSwitch = renderDamageComponentOptions();

        // ---------- 11. DC10: lisensi ----------
        globalData = [__T('u_t1'), __H('u_h1')];
        const licInput = document.getElementById('licUnit');
        if (licInput) {
            populateLicenseUnitList();
            r.licList = [...document.getElementById('licUnitList').options].map(o => o.value).join('|');
        }
        r.licApply = applyDistributedLicenseToUnit({ txnType: 'OUT', unitId: 'u_h1', licenseType: 'SF-RTK', date: '2026-09-01' });
        r.alertHeavy = _buildAlertList([__H('u_hx', { gpsLicense: 'SF-RTK', gpsLicenseEndDate: '2026-09-30' })]).total;

        // ---------- 10. DC7 / DC8: CSV ----------
        globalData = [__T('u_t1'), __H('u_h1')];
        switchEditUnitsGroup('heavy');
        let blobText = '', fname = '';
        URL.createObjectURL = bl => { window.__bl = bl; return 'blob:x'; };
        URL.revokeObjectURL = () => {};
        HTMLAnchorElement.prototype.click = function () { fname = this.download; };
        exportEditCSV();
        blobText = await window.__bl.text();
        r.heavyCsvName = fname.replace(/_\\d{4}-\\d{2}-\\d{2}/, '');
        r.heavyCsvHasGroup = /Unit Group/.test(blobText.split('\\n')[0]) && /,"heavy",/.test(blobText);
        r.heavyCsvNoAttachment = !/"?Attachment"?,/.test(blobText.split('\\n')[0]);
        // Impor ulang berkas itu dari tab Pertanian: tetap alat berat.
        const rows = blobText.replace(/^\\uFEFF/, '').split('\\n');
        const hdr = rows[0].split(',').map(x => x.replace(/"/g, ''));
        const data = rows.slice(1).map(l => { const c = l.split(','); const o = {}; hdr.forEach((h, i) => o[h] = (c[i] || '').replace(/"/g, '')); return o; });
        const parsed = processData(data, { fallbackGroup: 'tractor', fileGroup: inferCsvGroup(hdr) });
        r.roundTripGroup = parsed.valid.map(u => u.unitGroup);
        r.inferHeavy = inferCsvGroup(['Nickname', 'Camera AI', 'Rotary Lamp']);
        r.inferTractorKelompok = inferCsvGroup(['Nickname', 'GPS', 'Kelompok', 'Nomor Lambung']);
        const pk = processData([{ 'Nickname': 'A', 'Serial Number': 'S', 'Kelompok': 'Regu 3' }], { fallbackGroup: 'tractor' });
        r.kelompokIgnored = [pk.valid[0].unitGroup, pk.groupWarnings.length];
        const pu = processData([{ 'Nickname': 'A', 'Serial Number': 'S', 'Unit Group': 'Heavyy' }], { fallbackGroup: 'heavy' });
        r.unknownGroup = [pu.valid[0].unitGroup, pu.groupWarnings.length];
        const mismatch = processData([{ 'Nickname': 'X', 'Serial Number': 'SNu_t1', 'Unit Group': 'heavy', 'Site': 'Z' }], { fallbackGroup: 'tractor' });
        r.mismatch = bulkUpdateUnitsFromCSV(mismatch.valid).failed.map(f => f.reason);

        // ---------- 12. WN1-WN5 / UI4: dashboard ----------
        navigateTo('dashboard');
        globalData = [__T('u_t1'), __T('u_t2'), __H('u_h1', { status: 'Breakdown', cameraAi: 'Breakdown', breakdownReason: 'x' })];
        setDashGroup('tractor');
        r.tractorScope = [document.getElementById('kpiTotal').textContent, document.getElementById('kpiIssue').textContent];
        r.ringsTractor = [...document.querySelectorAll('#componentGrid .component-stat__ring-text')].map(e => e.textContent);
        setDashGroup('heavy');
        r.heavyScope = [document.getElementById('kpiTotal').textContent,
                        document.getElementById('licenseAlertsSection').style.display,
                        document.getElementById('stockAlertsSection').style.display];
        r.heavyRingNames = [...document.querySelectorAll('#componentGrid .component-stat__name')].map(e => e.textContent);
        r.heavyFilterOpts = [...document.getElementById('componentFilter').options].map(o => o.value).filter(Boolean);
        // Cakupan bertahan melewati snapshot dan reset.
        applyCloudUnitsSnapshot(globalData.map(u => ({ ...u })));
        r.scopeAfterSnapshot = filteredData.every(isHeavy);
        clearFilter();
        r.scopeAfterClear = filteredData.every(isHeavy);
        setDashGroup('all');
        r.captionAll = document.getElementById('dashScopeCaption').textContent;
        r.allHead = document.querySelectorAll('#detailTable thead th').length;
        setDashGroup('tractor');

        // ---------- 13. UI1: inline edit ----------
        navigateTo('editUnits');
        globalData = [__H('u_h1', { assetCode: undefined })];
        delete globalData[0].assetCode;
        switchEditUnitsGroup('heavy');
        __w.units.length = 0; __w.toasts.length = 0;
        const cell = document.querySelector('#editBody [data-id="u_h1"][data-field="assetCode"]');
        if (cell) saveInlineEdit(cell);
        r.inlineNoWrite = [__w.units.length, __w.toasts.length, !!cell];
        r.heavyUnlabelled = [...document.querySelectorAll('#editBody tr td')]
            .filter(td => !td.hasAttribute('data-label') && !td.classList.contains('col-actions')).length;

        // ---------- 14. FA1: profil ----------
        showUnitProfile('u_h1');
        const body = document.getElementById('unitProfileBody').innerHTML;
        r.profile = [/Lisensi/.test(body), /Distribusi Lisensi/.test(body), (body.match(/profile-comp /g) || []).length, /Camera AI/.test(body)];
        closeUnitProfile();

        // ---------- 16. Periksa Data ----------
        globalData = [__T('u_t1'), __H('u_h1', { gps: 'Breakdown' }), __H('u_h2', { unitGroup: 'Heavyy' })];
        const f = dcUnitGroupFields();
        r.periksa = f.map(x => x.kind).sort();
        r.periksaTarget = f.filter(x => x.kind === 'kelompok-silang').map(x => x.goTo);
        __w.units.length = 0;
        __answer = true;
        fixStrayGroupFields();
        r.afterFix = globalData.find(u => u.id === 'u_h1').gps;
        globalData = [__T('u_t1'), __H('u_h1')];
        globalDamages = [];
        r.periksaClean = dcUnitGroupFields().length + dcGroupReferences().length;

        // goDecision tidak mengubah preferensi tersimpan.
        localStorage.removeItem('editUnitsGroup');
        goDecision('editUnits:heavy');
        r.goEdit = [currentView, effectiveEditGroup(), localStorage.getItem('editUnitsGroup')];

        // ---------- 17. restore GANTI yang memindah kelompok ditolak ----------
        globalData = [__H('u_h1')];
        __w.toasts.length = 0;
        r.restoreRefused = _refuseCrossGroupRestore([{ id: 'u_h1', name: 'EXC', sn: 'KMTu_h1' }]);
        r.restoreSame = _refuseCrossGroupRestore([__H('u_h1')]);

        return r;
    } catch (e) { return { THREW: e.message + ' | ' + (e.stack || '').split('\\n')[1] }; } })()`);

    if (R.THREW) { t('THREW: ' + R.THREW, 1, 0); }
    else {
        t('tanpa alat berat, preferensi tab tersimpan tidak berlaku', R.prefEdit, 'tractor');
        t('tanpa alat berat, preferensi dashboard tersimpan tidak berlaku', R.prefDash, 'tractor');
        t('membuka Tim tidak menyembunyikan tab Edit Units', R.euTabsHidden, 0);
        t('mengurutkan dashboard tidak mengubah urutan globalData', R.aliasOk, true);

        t('firewall: GPS ke alat berat ditolak', R.fwGpsOnHeavy, false);
        t('firewall: Camera AI ke traktor ditolak', R.fwCamOnTractor, false);
        t('firewall: kelompok tidak bisa dipindah lewat update', R.fwGroupMove, false);
        t('firewall: yang ditolak tidak menulis apa pun', R.fwWrites, 0);
        t('firewall: suntingan biasa tetap jalan', R.fwNormal, true);
        t('firewall: suntingan biasa tidak menambah kunci kelompok', R.fwNoGroupKey, true);
        t('addUnits menolak kelompok tak dikenal', R.addUnknown, ['Kelompok "Heavyy" tidak dikenal']);
        t('addUnits menolak id milik kelompok lain', R.addCross, ['ID dipakai unit kelompok lain']);
        t('clearStray hanya boleh mengosongkan', R.clearStrayBlocked, false);

        t('DC1: komponen alat berat kosong tampil "belum diisi"', R.blankShown, '');
        t('DC1: select John Deere dinonaktifkan untuk alat berat', R.jdDisabled, true);
        t('DC1: simpan alat berat tidak mengirim satu pun kolom John Deere', R.dc1NoJd, []);
        t('DC1: komponen yang belum diisi tidak dipalsukan jadi Good/Breakdown', R.dc1BlankKept, ['', '']);
        t('DC1: komponen yang belum diisi tidak menimbulkan riwayat', R.dc1NoCompHistory, 0);
        t('DC1: tidak ada riwayat perubahan komponen John Deere', R.dc1History, 0);
        t('DC6: alat berat baru berstatus Breakdown tetap masuk kelompoknya', R.dc6Group, 'heavy');
        t('DC2: alat berat baru membawa komponennya sendiri', R.dc2Components, ['Good', 'Good', 'Good', 'Good']);
        t('DC2: alat berat baru tanpa kolom John Deere', R.dc2NoJd, []);
        t('traktor baru: kolom hari ini + unitGroup tractor', R.tractorAdd, ['tractor', 'Good', 'Good', false]);

        t('DC3: migrasi lisensi default sudah dihapus', R.defaultsGone, 'undefined');
        t('DC3: penurunan lisensi kedaluwarsa melewati alat berat', R.downgradeHeavy, [0, 0]);

        t('DC4: GPS pada alat berat tidak menggerakkan apa pun', R.targetGpsHeavy, '__nothing__');
        t('Camera AI pada alat berat menggerakkan cameraAi', R.targetCamHeavy, 'cameraAi');
        t('GPS pada traktor tetap pemetaan lama', R.targetGpsTractor, 'gps');
        t('kerusakan mekanis alat berat menggerakkan status', R.targetMechHeavy, null);
        t('GPS pada alat berat terdeteksi sebagai konflik', R.conflictGpsHeavy, true);
        t('Camera AI pada traktor terdeteksi sebagai konflik', R.conflictCamTractor, true);
        t('breakdown Camera AI menulis ke komponennya', R.camBreak, 'Breakdown');
        t('DC5: menyelesaikan catatan lama GPS pada alat berat tidak menulis ke unit', R.legacyResolveWrites, 0);
        t('DC5: catatan itu tetap tertandai selesai', R.legacyResolved, true);
        t('komponen kustom bernama Camera AI ditolak', /bawaan Alat Berat/.test(R.addCamComponent), true);
        t('pilihan komponen alat berat = komponennya sendiri',
          R.heavyOpts.slice(0, 4), ['Camera AI', 'Telematic Box', 'Switch Limiter', 'Rotary Lamp']);
        t('pilihan komponen alat berat tidak memuat GPS', R.heavyOpts.includes('GPS'), false);
        t('ganti unit ke traktor menjatuhkan Camera AI', R.droppedOnSwitch, 'Camera AI');

        if (R.licList !== undefined) t('DC10: pemilih lisensi tidak memuat alat berat', /EXC/.test(R.licList), false);
        t('DC10: lisensi tidak pernah ditulis ke alat berat', R.licApply, false);
        t('DC10: alat berat tidak pernah memicu peringatan lisensi', R.alertHeavy, 0);

        t('DC8: ekspor tab alat berat memakai nama berkasnya sendiri', R.heavyCsvName, 'alat_berat_monitoring.csv');
        t('DC8: ekspor alat berat membawa kolom Unit Group', R.heavyCsvHasGroup, true);
        t('ekspor alat berat tidak memakai label "Attachment"', R.heavyCsvNoAttachment, true);
        t('DC7/DC8: impor ulang dari tab Pertanian tetap alat berat', R.roundTripGroup, ['heavy']);
        t('header komponen alat berat menyimpulkan kelompok', R.inferHeavy, 'heavy');
        t('kolom "Kelompok" di CSV traktor tidak dibaca sebagai kelompok', R.inferTractorKelompok, 'tractor');
        t('"Kelompok" bukan kolom kelompok unit', R.kelompokIgnored, ['tractor', 0]);
        t('Unit Group tak dikenal jatuh ke tab dan diperingatkan', R.unknownGroup, ['heavy', 1]);
        t('CSV tidak bisa memindah unit antar kelompok', R.mismatch, ['Kelompok di CSV berbeda dengan unit — tidak dipindah']);

        t('WN1: cakupan Pertanian tidak menghitung alat berat yang rusak', R.tractorScope, ['2', '0']);
        t('WN2: ring traktor tetap 100% meski ada alat berat', R.ringsTractor, ['100%', '100%', '100%', '100%']);
        t('cakupan Alat Berat: jumlah, lisensi & stok lisensi disembunyikan', R.heavyScope, ['1', 'none', 'none']);
        t('cakupan Alat Berat: ring komponennya sendiri',
          R.heavyRingNames, ['Camera AI', 'Telematic Box', 'Switch Limiter', 'Rotary Lamp']);
        t('UI4: filter komponen mengikuti cakupan',
          R.heavyFilterOpts, ['Camera AI', 'Telematic Box', 'Switch Limiter', 'Rotary Lamp']);
        t('WN4: cakupan bertahan melewati snapshot', R.scopeAfterSnapshot, true);
        t('WN4: cakupan bertahan melewati reset filter', R.scopeAfterClear, true);
        t('cakupan Semua menyebut isinya', R.captionAll, 'Cakupan: semua unit (2 Pertanian, 1 Alat Berat)');
        t('tabel cakupan Semua: 10 kolom', R.allHead, 10);

        t('UI1: menyentuh sel kosong alat berat tidak menulis apa pun', R.inlineNoWrite, [0, 0, true]);
        t('tabel alat berat: hanya nomor & centang tanpa label', R.heavyUnlabelled, 2);

        t('FA1: profil alat berat tanpa Lisensi, tanpa Distribusi, 4 komponennya sendiri',
          R.profile, [false, false, 4, true]);

        // u_h2 berkelompok "Heavyy" — tak dikenal, jadi dibaca Pertanian, dan
        // kolom alat beratnya pun tercatat sebagai kolom silang. Dua temuan
        // untuk satu unit itu memang benar.
        t('Periksa Data menemukan field silang dan kelompok tak dikenal', R.periksa,
          ['kelompok-silang', 'kelompok-silang', 'kelompok-tak-dikenal']);
        t('temuan membuka tab kelompok unitnya', R.periksaTarget, ['editUnits:heavy', 'editUnits']);
        t('perbaikan otomatis mengosongkan field silang', R.afterFix, '');
        t('data bersih: nol temuan kelompok', R.periksaClean, 0);
        t('tautan membuka tab alat berat tanpa mengubah preferensi', R.goEdit, ['editUnits', 'heavy', null]);

        t('restore GANTI yang memindah kelompok ditolak', R.restoreRefused, true);
        t('restore dengan kelompok sama tidak ditolak', R.restoreSame, false);
    }
    await ctx.close();

    const fail = T.filter(r => !r.pass);
    T.forEach(r => console.log(`${r.pass ? 'PASS' : 'FAIL'}  ${r.n}${r.pass ? '' : `\n        dapat=${JSON.stringify(r.g)} harap=${JSON.stringify(r.w)}`}`));
    console.log(`\n${T.length - fail.length}/${T.length} lulus`);
    console.log('PAGE ERRORS:', errors.length ? errors : 'none');
    await b.close();
    process.exit(fail.length || errors.length ? 1 : 0);
})();
