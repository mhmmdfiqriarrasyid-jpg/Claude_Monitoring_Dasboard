// Rekaman "golden" tampilan traktor.
//
// Pembagian unit menjadi dua grup (pertanian / alat berat) menyentuh hampir
// setiap fungsi yang membaca unit. Janji terpenting perubahan itu: SELAMA
// BELUM ADA ALAT BERAT, tidak ada satu angka, tabel, CSV, atau peringatan
// traktor pun yang berubah. Janji seperti itu tidak bisa dipercaya dari
// membaca kode — jadi tampilan hari ini direkam di HEAD sebelum perubahan
// (golden_capture.js), lalu unitgroups_test.js membandingkannya byte demi
// byte dengan kode baru.
//
// CAPTURE dijalankan di dalam halaman. Ia menyemai delapan unit traktor yang
// sengaja mencakup setiap cabang yang menarik, lalu mengembalikan objek.
// Parameter window.__extraUnits memungkinkan uji menambah alat berat tanpa
// mengubah semaian traktornya.
const FIXED_TIME = '2026-09-25T08:00:00';

const CAPTURE = `(async () => {
    const out = {};
    const charts = {};
    const realMake = window.makeChart;
    window.makeChart = (id, cfg) => { charts[id] = JSON.parse(JSON.stringify((cfg && cfg.data) || null)); return null; };
    const blobs = [];
    URL.createObjectURL = b => { blobs.push(b); return 'blob:x'; };
    URL.revokeObjectURL = () => {};
    const names = [];
    HTMLAnchorElement.prototype.click = function () { names.push(this.download || ''); };
    const toasts = [];
    window.showToast = (m, k) => { toasts.push(String(m) + '|' + (k || '')); };
    const unitWrites = [];
    window.cloud = { isReady: true,
        saveUnits: u => { unitWrites.push(JSON.parse(JSON.stringify(u))); return Promise.resolve(); },
        deleteUnits: () => Promise.resolve(), getAllUnits: () => Promise.resolve([]),
        addHistoryEvents: () => Promise.resolve(), subscribeUsers: () => () => {},
        saveDamage: () => Promise.resolve(), saveDamages: () => Promise.resolve() };
    currentUser = { uid: 'uO', email: 'o@x.id' };
    currentUserDoc = { role: 'owner', status: 'active', displayName: 'Owner' };
    try { localStorage.setItem('tractorLicenseDatesApplied_v2', '1');
          localStorage.setItem('tractorLicenseDefaultsApplied', '1'); } catch (_) {}
    hideAuthGates(); applyRoleGating();
    navigateTo('dashboard');

    const day = 86400000, now = Date.now();
    const iso = ms => new Date(ms).toISOString().slice(0, 10);
    const T = (id, x) => Object.assign({ id, name: 'GGTR' + id + 'G_ABC', model: 'JOHN DEERE 6110B',
        sn: '1BM7230C' + id, implement: 'Rotary Tiller', status: 'Good', display: 'Good', gps: 'Good',
        steering: 'Good', jdlink: 'Good', site: 'PT. GPA', yearReceived: '2021', userCategory: 'Planting',
        gpsLicense: 'SF-1', licenseDisplay: 'G5 Basic', gpsLicenseStartDate: '', gpsLicenseEndDate: '',
        displayLicenseStartDate: '', displayLicenseEndDate: '', remarks: '', breakdownReason: '' }, x || {});
    const units = [
        T('101'),
        T('102', { status: 'Breakdown', breakdownReason: 'Hidrolik bocor', breakdownStartedAt: now - 5 * day,
                   downtimeHistory: [{ start: now - 40 * day, end: now - 38 * day, reason: 'Ban' }] }),
        T('103', { gps: '' , site: 'PT. MNM' }),
        T('104', { gpsLicense: 'SF-RTK', gpsLicenseStartDate: iso(now - 355 * day), gpsLicenseEndDate: iso(now + 10 * day) }),
        T('105', { gpsLicense: 'SF-RTK', gpsLicenseStartDate: iso(now - 400 * day), gpsLicenseEndDate: iso(now - 20 * day) }),
        T('106', { display: 'Breakdown', jdlink: 'Breakdown', site: 'PT. MNM', yearReceived: 2020 }),
        T('107', { licenseDisplay: 'G5 Advance', displayLicenseStartDate: iso(now - 300 * day),
                   displayLicenseEndDate: iso(now + 25 * day), remarks: 'X ' }),
        T('108', { name: 'GGCH001G', model: 'JOHN DEERE CH570', sn: '1T8C570HKST250056', implement: '' })
    ].concat(window.__extraUnits || []);
    globalData = units.map(u => JSON.parse(JSON.stringify(u)));
    globalDamages = [
        { id: 'd1', date: iso(now - 3 * day), unitId: 'u_102', unitName: 'GGTR102G_ABC', sn: '1BM7230C102',
          site: 'PT. GPA', damageType: 'Mekanis', component: 'Hidrolik', description: 'Bocor',
          resolved: false, resolvedAt: '', createdAt: 1, updatedAt: 1 },
        { id: 'd2', date: iso(now - 9 * day), unitId: 'u_gone', unitName: 'HILANG', sn: 'NOPE',
          site: 'PT. GPA', damageType: 'Device Precision', component: 'GPS', description: 'Sinyal',
          resolved: true, resolvedAt: iso(now - 7 * day), createdAt: 2, updatedAt: 2 }
    ];
    globalData.forEach(u => { if (!u.id.startsWith('u_')) u.id = 'u_' + u.id; });
    globalLicenseStock = [];
    filteredData = globalData.slice();
    try { onDataLoaded(); } catch (e) { out.onDataLoadedError = String(e && e.message); }
    try { updateDashboard(filteredData); } catch (e) { out.updateError = String(e && e.message); }
    await new Promise(r => setTimeout(r, 50));

    const html = id => { const el = document.getElementById(id); return el ? el.innerHTML.replace(/\\s+/g, ' ').trim() : null; };
    const text = id => { const el = document.getElementById(id); return el ? el.textContent.replace(/\\s+/g, ' ').trim() : null; };
    ['kpiTotal','kpiGood','kpiGoodPct','kpiBreakdown','kpiBreakdownPct','kpiIssue','kpiIssuePct','kpiHealth',
     'kpiMTBF','kpiMTBFSub','kpiMTTR','kpiFailures','kpiMonthDowntime','dashNarrative','filterCount']
        .forEach(id => { out['text:' + id] = text(id); });
    ['componentGrid','issueSummary','repairBody','licenseAlertsCards','licenseAlertsSummary','detailBody']
        .forEach(id => { out['html:' + id] = html(id); });
    out['html:detailHead'] = document.querySelector('#detailTable thead').innerHTML.replace(/\\s+/g, ' ').trim();
    out['html:componentFilter'] = document.getElementById('componentFilter').outerHTML.replace(/\\s+/g, ' ');
    out['html:issueFilter'] = document.getElementById('issueFilter').outerHTML.replace(/\\s+/g, ' ');
    out.charts = charts;
    out.alertList = _buildAlertList();
    out.downtime = computeDowntimeStats();
    out.decisionLicense = decisionGroups().filter(g => g.key === 'license');
    out.dataChecks = runDataChecks().map(c => [c.key, c.items.length]);
    out.issues = globalData.map(u => [u.id, detectIssues(u)]);
    out.countIssues = countIssues(globalData);

    blobs.length = 0; names.length = 0;
    exportCSV(filteredData);
    out.csvDashboard = blobs[0] ? await blobs[0].text() : null;
    out.csvDashboardName = names[0] || null;

    navigateTo('editUnits');
    globalData = units.map(u => { const c = JSON.parse(JSON.stringify(u)); if (!c.id.startsWith('u_')) c.id = 'u_' + c.id; return c; });
    renderEditTable();
    out['html:editHead'] = document.querySelector('#editTable thead').innerHTML.replace(/\\s+/g, ' ').trim();
    out['html:editBody'] = html('editBody');
    out['text:editUnitCount'] = text('editUnitCount');
    blobs.length = 0; names.length = 0;
    exportEditCSV();
    out.csvEdit = blobs[0] ? await blobs[0].text() : null;
    out.csvEditName = names[0] || null;

    // Impor CSV traktor: payload yang akan dibuat untuk baris baru.
    const parsed = processData([{ 'Nickname': 'GGTR201G_NEW', 'Model': 'JOHN DEERE 6110B',
        'Serial Number': 'SN201', 'Implement': 'Plough', 'Status': 'Good', 'Display': 'Good', 'GPS': 'Good',
        'Steering': 'Good', 'JDLink': 'Good', 'Site': 'PT. GPA', 'Tahun Penerimaan': '2024',
        'User Category': 'Planting', 'GPS License': 'SF-1', 'Display License': 'G5 Basic', 'Remarks': '' }]);
    // id memakai Math.random(), jadi dibuang supaya rekaman deterministik.
    out.csvImport = JSON.parse(JSON.stringify(parsed, (k, v) => k === 'id' ? undefined : v));

    // Inline edit: nilai angka 2020 dan 'X ' ditinggal tanpa diubah.
    unitWrites.length = 0; toasts.length = 0;
    const cell = f => document.querySelector('#editBody [data-id="u_106"][data-field="' + f + '"]');
    const yc = cell('yearReceived');
    if (yc) { yc.textContent = '2020'; saveInlineEdit(yc); }
    const r7 = document.querySelector('#editBody [data-id="u_107"][data-field="name"]');
    if (r7) { r7.textContent = r7.textContent; saveInlineEdit(r7); }
    await new Promise(r => setTimeout(r, 30));
    out.inlineWrites = unitWrites.map(b => b.map(u => ({ id: u.id, yearReceived: u.yearReceived, name: u.name })));
    out.inlineToasts = toasts.slice();

    window.makeChart = realMake;
    return out;
})()`;

module.exports = { CAPTURE, FIXED_TIME };
