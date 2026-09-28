// Lima bug "penting" dari audit sesudah v110.
//
//   5. Cadangan tidak berisi foto kerusakan, foto laporan harian, dan surat
//      izin — padahal berkasnya dan toast-nya mengaku lengkap.
//   6. Restore GANTI menghapus catatan tapi meninggalkan dokumen fotonya:
//      tidak pernah dibaca, tidak pernah dihapus.
//   7. Akun yang hanya boleh MENYETUJUI tidak bisa membuka tab Laporan Harian
//      dan Izin, padahal Kotak Keputusan menyuruhnya menyetujui di sana.
//   8. Tautan Kotak Keputusan dan Periksa Data membuka tab / cakupan terakhir,
//      yang bisa tidak memuat unit yang ditunjuk.
//   9. Baris "Total bertugas" di grid shift ikut menghitung orang yang izin /
//      sakitnya sudah disetujui.
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
        const tick = ms => new Promise(r => setTimeout(r, ms || 10));

        URL.createObjectURL = blob => { window.__blob = blob; return 'blob:test'; };
        URL.revokeObjectURL = () => {};
        HTMLAnchorElement.prototype.click = function () {};
        let answers = [];
        const asked = [];
        window.confirm = m => { asked.push(String(m)); return answers.length ? answers.shift() : false; };
        const toasts = [];
        window.showToast = (m, k) => { toasts.push(String(m)); };

        const PH = s => 'data:image/jpeg;base64,' + s;
        const store = { dmg: { d1: PH('D1') }, wl: { w1: [PH('W1'), PH('W2')], l1: [PH('L1')] } };
        const writes = { dmg: [], wl: [], delDmg: [], delWl: [] };
        const ok = () => Promise.resolve();
        window.cloud = { isReady: true,
            getAllShifts: () => Promise.resolve(teamShifts.slice()),
            saveUnits: ok, deleteUnits: ok, getAllUnits: () => Promise.resolve([]), addHistoryEvents: ok,
            saveImplements: ok, saveDamages: ok, saveLicenses: ok, saveUserCategories: ok, saveDamageComponents: ok,
            saveTeamMembers: ok, saveShifts: ok, saveLeaveRequests: ok, saveWorkLogs: ok, saveDevices: ok, saveStockItems: ok,
            deleteImplement: ok, deleteShift: ok, deleteTeamMember: ok, deleteWorkLog: ok, deleteDevice: ok,
            deleteStockItem: ok, deleteUserCategory: ok, deleteDamageComponent: ok, deleteDamage: ok,
            deleteLicense: ok, deleteLeaveRequest: ok, subscribeUsers: () => () => {},
            getDamagePhoto: id => Promise.resolve(store.dmg[id] || ''),
            saveDamagePhoto: (id, p) => { writes.dmg.push([id, p]); return ok(); },
            deleteDamagePhoto: id => { writes.delDmg.push(id); return ok(); },
            getWorkLogPhotos: id => Promise.resolve((store.wl[id] || []).slice()),
            saveWorkLogPhotos: (id, p) => { writes.wl.push([id, p.slice()]); return ok(); },
            deleteWorkLogPhotos: id => { writes.delWl.push(id); return ok(); } };
        window.cloud.getTeamDocs = window.cloud.getWorkLogPhotos;
        window.cloud.saveTeamDocs = window.cloud.saveWorkLogPhotos;
        window.cloud.deleteTeamDocs = window.cloud.deleteWorkLogPhotos;

        currentUser = { uid: 'uO', email: 'o@x.id' };
        currentUserDoc = { role: 'owner', status: 'active', email: 'o@x.id' };
        hideAuthGates(); applyRoleGating();

        const seed = () => {
            globalData = [{ id: 'u1', name: 'A', sn: 'S1', status: 'Good' }];
            globalImplements = []; globalLicenseStock = []; userCategories = []; damageComponents = [];
            teamMembers = [{ id: 'm1', name: 'Andi', company: 'PT. GPA', active: true }];
            teamShifts = []; warehouseDevices = []; stockLedger = [];
            globalDamages = [{ id: 'd1', unitId: 'u1', hasPhoto: true }, { id: 'd2', unitId: 'u1' }];
            workLogs = [{ id: 'w1', date: '2026-09-14', memberId: 'm1', task: 'x', photoCount: 2 },
                        { id: 'w2', date: '2026-09-14', memberId: 'm1', task: 'y' }];
            leaveRequests = [{ id: 'l1', memberId: 'm1', type: 'sakit', dateFrom: '2026-09-15', dateTo: '2026-09-15',
                               days: 1, approval: 'pending', docCount: 1 }];
        };
        const exportNow = async () => {
            window.__blob = null;
            await exportBackup();
            return window.__blob ? JSON.parse(await window.__blob.text()) : null;
        };

        // =============== 5. FOTO DI CADANGAN ===============
        seed();
        answers = [true];                              // sertakan foto
        let ex = await exportNow();
        t('ditanya dulu, dengan jumlah dokumennya', /Sertakan 3 dokumen foto/.test(asked[asked.length - 1] || ''), true);
        t('foto kerusakan ikut', ex.damagePhotos, { d1: PH('D1') });
        t('foto laporan harian ikut', ex.workLogPhotos, { w1: [PH('W1'), PH('W2')] });
        t('surat izin ikut', ex.leaveDocs, { l1: [PH('L1')] });
        t('dan tidak ada yang dilaporkan hilang', ex.omitted.map(o => o.key), []);

        answers = [false];                             // tanpa foto
        ex = await exportNow();
        t('tanpa foto: berkasnya MENGAKU tidak membawanya', ex.omitted.map(o => o.key + ':' + o.why),
          ['damagePhotos:tidak disertakan', 'workLogPhotos:tidak disertakan', 'leaveDocs:tidak disertakan']);
        t('dan toast-nya menyebutnya', /TIDAK termasuk: .*Foto kerusakan/.test(toasts[toasts.length - 1]), true);

        globalDamages = [{ id: 'd2', unitId: 'u1' }]; workLogs = []; leaveRequests = [];
        asked.length = 0;
        await exportNow();
        t('tanpa catatan berfoto: tidak ada pertanyaan tambahan', asked.length, 0);

        // Restore: foto kembali ke koleksinya, untuk catatan yang dipulihkan.
        seed();
        answers = [true];
        const withPhotos = await exportNow();
        writes.dmg.length = 0; writes.wl.length = 0;
        answers = [true];                              // GABUNG
        importBackup(new File([JSON.stringify(withPhotos)], 'b.json', { type: 'application/json' }));
        await tick(400);
        t('restore mengembalikan foto kerusakan', writes.dmg, [['d1', PH('D1')]]);
        t('restore mengembalikan foto laporan dan surat',
          writes.wl.map(w => w[0]).sort(), ['l1', 'w1']);
        const rows = [...document.querySelectorAll('#restoreReportBody tr')].map(tr => tr.textContent);
        t('laporan restore menyebut ketiganya',
          ['Foto kerusakan', 'Foto laporan harian', 'Surat izin'].every(l => rows.some(r => r.includes(l))), true);
        closeRestoreReport();

        // Berkas yang catatannya berfoto tapi fotonya tidak ikut: dikatakan.
        answers = [false]; seed();
        const noPhotos = await exportNow();
        answers = [true];
        importBackup(new File([JSON.stringify(noPhotos)], 'b.json', { type: 'application/json' }));
        await tick(400);
        const rows2 = [...document.querySelectorAll('#restoreReportBody tr')].map(tr => tr.textContent);
        t('restore tanpa foto di berkas: laporannya berkata begitu',
          rows2.some(r => /Foto kerusakan.*fotonya tidak/.test(r)), true);
        closeRestoreReport();

        // =============== 6. RESTORE GANTI TIDAK MENINGGALKAN DOKUMEN FOTO ===============
        seed();
        writes.delDmg.length = 0; writes.delWl.length = 0;
        const empty = { version: 4, units: globalData.slice(), damages: [], workLogs: [], leaveRequests: [] };
        answers = [false, true];                       // GANTI, lalu lanjutkan
        importBackup(new File([JSON.stringify(empty)], 'e.json', { type: 'application/json' }));
        await tick(400);
        t('catatan kerusakan berfoto yang terhapus: dokumen fotonya ikut dihapus', writes.delDmg, ['d1']);
        t('laporan dan izin: dokumen foto/suratnya ikut dihapus', writes.delWl.sort(), ['l1', 'w1']);
        closeRestoreReport();

        // =============== 7. AKUN KHUSUS MENYETUJUI ===============
        seed();
        currentUserDoc = { role: 'staff', status: 'active', email: 'a@x.id',
                           access: { teamLogApprove: 'edit', leader: 'view' } };
        applyRoleGating();
        goDecision('leave');
        t('tab Izin terbuka untuk akun khusus menyetujui',
          [currentView, teamTab, document.getElementById('teamLeavePanel').style.display], ['team', 'leave', '']);
        t('dan tombol setujuinya ada di sana', document.querySelectorAll('#teamLeavePanel .appr-btn').length > 0, true);
        goDecision('approval');
        t('tab Laporan Harian juga', [teamTab, document.getElementById('teamWorkLogPanel').style.display], ['worklog', '']);
        t('tanpa tombol edit laporan (itu hak teamLog)',
          document.querySelectorAll('#teamWorkLogPanel [onclick^="editWorkLog"]').length, 0);
        currentUserDoc = { role: 'staff', status: 'active', email: 'b@x.id', access: { teamShift: 'view' } };
        applyRoleGating();
        navigateTo('team');
        t('tanpa hak apa pun di laporan, tab itu tetap tertutup',
          document.getElementById('teamLeavePanel').style.display, 'none');
        currentUserDoc = { role: 'owner', status: 'active', email: 'o@x.id' };
        applyRoleGating();

        // =============== 8. TAUTAN SADAR KELOMPOK ===============
        const TR = { id: 'u_t1', name: 'T', sn: 'S1', status: 'Good' };
        const HV = { id: 'u_h1', name: 'H', sn: 'S2', status: 'Good', unitGroup: 'heavy' };
        globalData = [TR];
        t('tanpa alat berat, tautan tetap seperti dulu', [unitEditTarget(TR), dashTargetFor([TR])], ['editUnits', 'dashboard']);
        // navigateTo memuat ulang unit dari localStorage — simpan dulu.
        globalData = [TR, HV]; saveToStorage(globalData);
        t('dengan dua kelompok, tautan traktor menyebut tabnya', unitEditTarget(TR), 'editUnits:tractor');
        t('tautan dashboard mengikuti unit yang ditunjuk',
          [dashTargetFor([TR]), dashTargetFor([HV]), dashTargetFor([TR, HV])],
          ['dashboard:tractor', 'dashboard:heavy', 'dashboard:all']);
        navigateTo('editUnits');
        switchEditUnitsGroup('heavy');
        goDecision(unitEditTarget(TR));
        t('temuan traktor membuka tab traktor walau tab terakhir alat berat', effectiveEditGroup(), 'tractor');
        navigateTo('dashboard');
        setDashGroup('heavy');
        goDecision(dashTargetFor([TR]));
        t('breakdown traktor membuka cakupan Pertanian walau cakupan terakhir Alat Berat', effectiveDashGroup(), 'tractor');

        // =============== 9. TOTAL BERTUGAS ===============
        const today = toISODate();
        teamMembers = [{ id: 'm1', name: 'Andi', company: 'PT. GPA', active: true },
                       { id: 'm2', name: 'Budi', company: 'PT. GPA', active: true }];
        teamShifts = [{ id: today + '_m1', date: today, memberId: 'm1', shift: 'pagi' },
                      { id: today + '_m2', date: today, memberId: 'm2', shift: 'pagi' }];
        leaveRequests = [{ id: 'l9', memberId: 'm1', type: 'sakit', dateFrom: today, dateTo: today, days: 1, approval: 'approved' }];
        teamWeekStart = startOfWeekISO(today);
        navigateTo('team'); switchTeamTab('shift');
        const footToday = () => (document.querySelector('#shiftFoot td.is-today strong') || {}).textContent;
        const groupToday = () => (document.querySelector('.shift-group__cell.is-today strong') || {}).textContent;
        t('Total bertugas tidak menghitung yang sakitnya disetujui', footToday(), '1');
        t('baris per perusahaan juga', groupToday(), '1');
        leaveRequests[0].approval = 'pending';
        renderShiftGrid();
        t('izin yang belum disetujui tidak mengurangi', footToday(), '2');

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
