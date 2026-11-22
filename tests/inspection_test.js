// Pengecekan alat berat: jadwal → laporan cek (4 komponen, 1 foto masing-
// masing) → persetujuan / revisi → cek berikutnya 14 hari dari tanggal cek
// AKTUAL → komponen rusak masuk ke Kerusakan setelah disetujui.
const { launch, BASE_URL } = require('./_env');

(async () => {
    const b = await launch();
    const page = await b.newPage({ viewport: { width: 1440, height: 900 } });
    await page.clock.setFixedTime('2026-10-02T09:00:00');
    const errors = [];
    page.on('pageerror', e => { if (!/Chart is not defined/.test(e.message)) errors.push(e.message); });
    page.on('dialog', d => d.accept());
    await page.goto(BASE_URL + '/index.html', { waitUntil: 'load' });
    await page.waitForTimeout(1000);

    await page.addScriptTag({ content: `(async () => { try {
        const T = []; const t = (n, g, w) => T.push({ n, g, w, pass: JSON.stringify(g) === JSON.stringify(w) });
        const tick = ms => new Promise(r => setTimeout(r, ms || 20));
        const calls = [];
        const ok = () => Promise.resolve();
        const clone = v => JSON.parse(JSON.stringify(v));
        window.cloud = { isReady: true,
            saveUnits: ok, deleteUnits: ok, getAllUnits: () => Promise.resolve([]), addHistoryEvents: ok, subscribeUsers: () => () => {},
            saveDamage: r => { calls.push(['damage', clone(r)]); return ok(); }, saveDamages: ok,
            saveDamagePhoto: (id, p) => { calls.push(['damagePhoto', id, p]); return ok(); },
            saveInspectionPlan: r => { calls.push(['plan', clone(r)]); return ok(); },
            deleteInspectionPlan: id => { calls.push(['delPlan', id]); return ok(); },
            saveInspection: r => { calls.push(['ins', clone(r)]); return ok(); },
            deleteInspection: id => { calls.push(['delIns', id]); return ok(); },
            saveInspectionPhotos: (id, p) => { calls.push(['photos', id, Object.keys(p).sort()]); return ok(); },
            deleteInspectionPhotos: id => { calls.push(['delPhotos', id]); return ok(); },
            // 2.45.0: read for display a photo is a download URL (of the inspection's own object); inline, the picture itself.
            getInspectionPhotos: (id, o) => { calls.push(['getInsPhotos', id, !!(o && o.inline)]);
                const v = o && o.inline ? PH : SURL;
                return Promise.resolve({ cameraAi: v, telematicBox: v, switchLimiter: v, rotaryLamp: v }); } };
        const PH = 'data:image/jpeg;base64,QUJD';
        const SURL = 'https://firebasestorage.googleapis.com/v0/b/ot-monitoring-tractor.firebasestorage.app/o/photos%2FinspectionPhotos%2Fx%2Fa?alt=media&token=abc-1';
        const toasts = [];
        window.showToast = m => { toasts.push(String(m)); };
        window.confirm = () => true;
        const last = k => { const c = calls.filter(x => x[0] === k); return c.length ? c[c.length - 1][1] : null; };
        const as = (uid, doc) => { currentUser = { uid, email: uid + '@x.id' }; currentUserDoc = { status: 'active', email: uid + '@x.id', displayName: uid, ...doc }; applyRoleGating(); };
        const TECH = { role: 'khl', access: { inspection: 'edit' } };
        const BOSS = { role: 'staff', access: { inspection: 'view', inspectionApprove: 'edit', damage: 'edit', editUnits: 'edit' } };
        try { localStorage.clear(); localStorage.setItem('tractorLicenseDatesApplied_v2', '1'); } catch (_) {}

        as('owner', { role: 'owner' });
        hideAuthGates();
        const H = (id, extra) => ({ id, name: 'EX-' + id, sn: 'SN' + id, unitGroup: 'heavy', status: 'Good', machineType: 'Excavator', assetCode: 'LB-' + id, site: 'PT. GPA',
                                    cameraAi: 'Good', telematicBox: 'Good', switchLimiter: 'Good', rotaryLamp: 'Good', ...extra });
        globalData = [H('h1'), H('h2'), H('h3'), { id: 't1', name: 'TR1', sn: 'SNT', status: 'Good' }];
        saveToStorage(globalData);
        inspections = []; inspectionPlans = []; globalDamages = [];

        // =============== akses ===============
        t('area akses baru terdaftar', ['inspection', 'inspectionApprove'].every(k => ACCESS_AREAS.some(a => a.key === k)), true);
        as('nobody', { role: 'khl', access: {} });
        t('tanpa akses: menu Pengecekan tersembunyi', document.querySelector('.nav__link[data-view="inspection"]').style.display, 'none');
        as('tech', TECH);
        t('teknisi: menu Pengecekan terlihat', document.querySelector('.nav__link[data-view="inspection"]').style.display, '');
        navigateTo('inspection');
        t('teknisi: halaman terbuka', currentView, 'inspection');

        // =============== status & 14 hari ===============
        t('hanya alat berat yang dicek', inspectableUnits().map(u => u.id), ['h1', 'h2', 'h3']);
        t('belum pernah dicek', inspectionStatusFor(globalData[0]).key, 'never');
        inspections = [{ id: 'a', unitId: 'h1', date: '2026-09-10', approval: 'approved', createdByUid: 'x', results: {} },
                       { id: 'b', unitId: 'h2', date: '2026-09-20', approval: 'pending', createdByUid: 'x', results: {} },
                       { id: 'c', unitId: 'h3', date: '2026-09-30', approval: 'draft', createdByUid: 'x', results: {} }];
        const s1 = inspectionStatusFor(globalData[0], '2026-10-02');
        t('14 hari dari tanggal cek aktual: 10 Sep → 24 Sep', [s1.next, s1.key, s1.label], ['2026-09-24', 'overdue', '8 days overdue']);
        const s2 = inspectionStatusFor(globalData[1], '2026-10-02');
        t('laporan menunggu juga dihitung sudah dicek: 20 Sep → 4 Okt', [s2.next, s2.key, s2.label], ['2026-10-04', 'soon', 'in 2 days']);
        t('draf tidak dihitung', inspectionStatusFor(globalData[2], '2026-10-02').key, 'never');
        renderInspectionView();
        t('KPI terlambat / jatuh tempo / belum pernah', ['insKpiOverdue', 'insKpiSoon', 'insKpiNever'].map(i => document.getElementById(i).textContent), ['1', '1', '1']);
        const firstRow = document.querySelector('#insStatusBody tr td[data-label="Unit"]').textContent;
        t('tabel status: terlambat paling atas', firstRow.includes('EX-h1'), true);
        updateInspectionBadge();
        t('badge menu: terlambat + jatuh tempo', document.getElementById('inspectionBadge').textContent, '2');
        navigateTo('editUnits'); switchEditUnitsGroup('heavy', { persist: false });
        t('Unit Database alat berat: chip status cek di bawah nama', !!document.querySelector('#editBody .ins-st--chip.ins-st--overdue'), true);
        navigateTo('inspection');

        // =============== jadwal ===============
        inspections = [];
        switchInspectionTab('plans');
        showInspectionPlanForm();
        document.getElementById('insPlanDate').value = '2026-10-02';
        selectDueInspectionUnits();
        t('pilih yang jatuh tempo: semua yang belum pernah dicek', [..._insPlanPick].sort(), ['h1', 'h2', 'h3']);
        _insPlanPick.delete('h3');
        saveInspectionPlan(null);
        const plan = last('plan');
        t('jadwal tersimpan', [plan.date, plan.unitIds], ['2026-10-02', ['h1', 'h2']]);
        inspectionPlans = [plan];
        renderInspectionPlans();
        t('progres jadwal 0/2', document.querySelector('#insPlanBody td[data-label="Progress"]').textContent.trim().startsWith('0/2'), true);

        // =============== laporan cek ===============
        document.querySelector('#insPlanBody .ins-chip__go').click();
        t('cek dari jadwal: form terbuka untuk unit itu', [document.getElementById('inspectionModal').classList.contains('open'), document.getElementById('insUnit').value], [true, 'h1']);
        t('dan tercatat bagian dari jadwal', [document.getElementById('insPlanId').value, document.getElementById('insPlanTag').textContent], [plan.id, 'Part of schedule 2026-10-02']);
        t('empat komponen alat berat', [...document.querySelectorAll('#insComponents .ins-comp')].map(e => e.dataset.key), ['cameraAi', 'telematicBox', 'switchLimiter', 'rotaryLamp']);
        let n0 = calls.length;
        saveInspection(null);
        t('kirim tanpa hasil ditolak', [calls.length, toasts.some(m => /Choose Good\\/Damaged/.test(m))], [n0, true]);
        ['cameraAi', 'telematicBox', 'switchLimiter', 'rotaryLamp'].forEach(k => setInspectionResult(k, 'good'));
        setInspectionResult('rotaryLamp', 'bad');
        _insPhotos = { cameraAi: PH, telematicBox: PH, switchLimiter: PH }; _insPhotosDirty = true;
        saveInspection(null);
        t('kirim tanpa foto lengkap ditolak (1 foto per komponen)', [calls.length, toasts.some(m => /Photo required for: Rotary Lamp/.test(m))], [n0, true]);
        _insPhotos.rotaryLamp = PH;
        saveInspection(null);
        t('komponen rusak wajib dijelaskan', [calls.length, toasts.some(m => /Describe the damage: Rotary Lamp/.test(m))], [n0, true]);
        document.getElementById('insNote_rotaryLamp').value = 'Lampu mati <img src=x onerror=window.__pwn=1>';
        saveInspection(null);
        const seq = calls.slice(n0).map(c => c[0]);
        t('foto dikirim sebelum laporannya', seq, ['photos', 'ins']);
        const rep = last('ins');
        t('laporan: menunggu, unit, tanggal, jadwal', [rep.approval, rep.unitId, rep.date, rep.planId], ['pending', 'h1', '2026-10-02', plan.id]);
        t('hasil per komponen', rep.results, { cameraAi: 'good', telematicBox: 'good', switchLimiter: 'good', rotaryLamp: 'bad' });
        t('4 foto', [rep.photoCount, calls.find(c => c[0] === 'photos')[2]], [4, ['cameraAi', 'rotaryLamp', 'switchLimiter', 'telematicBox']]);
        await tick();
        t('pesan menyebut cek berikutnya', toasts.some(m => /next check 2026-10-16/.test(m)), true);
        inspections = [rep];
        renderInspectionPlans();
        t('jadwal: 1/2 selesai', document.querySelector('#insPlanBody td[data-label="Progress"]').textContent.trim().startsWith('1/2'), true);

        // Di luar jadwal
        showInspectionForm('h3');
        t('cek di luar jadwal ditandai', [document.getElementById('insPlanId').value, /Unscheduled/.test(document.getElementById('insPlanTag').textContent)], ['', true]);
        document.getElementById('insDate').value = '2026-09-25';
        ['cameraAi', 'telematicBox', 'switchLimiter', 'rotaryLamp'].forEach(k => setInspectionResult(k, 'good'));
        _insPhotos = { cameraAi: PH, telematicBox: PH, switchLimiter: PH, rotaryLamp: PH }; _insPhotosDirty = true;
        saveInspection(null);
        const adhoc = last('ins');
        t('di luar jadwal: tersimpan tanpa jadwal', [adhoc.planId, adhoc.unitId, adhoc.approval], ['', 'h3', 'pending']);
        inspections = [rep, adhoc];
        t('di luar jadwal tetap menghitung ulang: 25 Sep → 9 Okt', inspectionStatusFor(globalData[2], '2026-10-02').next, '2026-10-09');

        // Kunci
        switchInspectionTab('reports');
        const row = [...document.querySelectorAll('#insReportBody tr')].find(tr => tr.textContent.includes('EX-h1'));
        t('menunggu: gembok + tarik kembali', [!!row.querySelector('button[title="Edit"]'), !!row.querySelector('.row-lock'), !!row.querySelector('button[title="Withdraw to draft"]')], [false, true, true]);
        t('catatan berbahaya tidak menjadi markup', [document.querySelectorAll('#insReportBody img').length, window.__pwn || 0], [0, 0]);
        const n1 = calls.length;
        editInspection(rep.id);
        t('menunggu: tidak bisa diedit', [calls.length, document.getElementById('inspectionModal').classList.contains('open')], [n1, false]);

        // =============== persetujuan ===============
        as('boss', BOSS);
        renderInspectionReports();
        const brow = [...document.querySelectorAll('#insReportBody tr')].find(tr => tr.textContent.includes('EX-h1'));
        t('atasan: tombol setujui + revisi', brow.querySelectorAll('.appr-btn').length, 2);
        window.prompt = () => 'Foto Camera AI buram';
        reviseInspection(adhoc.id);
        t('minta revisi', [last('ins').approval, last('ins').revisionNote], ['revision', 'Foto Camera AI buram']);
        inspections = [rep, last('ins')];
        approveInspection(rep.id);
        await tick(100);
        const appr = calls.filter(c => c[0] === 'ins').map(c => c[1]).find(r => r.id === rep.id && r.approval === 'approved');
        t('disetujui', !!appr, true);
        const dmg = last('damage');
        t('komponen rusak masuk ke Kerusakan', dmg && [dmg.unitId, dmg.damageType, dmg.component, dmg.fromInspection, dmg.hasPhoto], ['h1', 'Device Precision', 'Rotary Lamp', rep.id, true]);
        t('dengan fotonya', calls.some(c => c[0] === 'damagePhoto' && c[1] === dmg.id), true);
        // B6: a copy of the picture (uploaded into the damage record's own folder), never the inspection's download URL.
        t('B6 foto dibaca inline', calls.filter(c => c[0] === 'getInsPhotos' && c[1] === rep.id).map(c => c[2]), [true]);
        t('B6 foto kerusakan = salinan gambar, bukan URL objek inspeksi', calls.find(c => c[0] === 'damagePhoto' && c[1] === dmg.id)[2], PH);
        t('komponen unit di-set Breakdown', globalData.find(u => u.id === 'h1').rotaryLamp, 'Breakdown');
        const stamp = calls.filter(c => c[0] === 'ins').map(c => c[1]).reverse().find(r => r.id === rep.id);
        t('laporan ditandai sudah diterapkan', [!!stamp.appliedAt, stamp.appliedDamageIds, stamp.appliedKeys], [true, [dmg.id], ['rotaryLamp']]);
        const nd = calls.filter(c => c[0] === 'damage').length;
        applyInspectionFindings(rep.id);
        await tick(50);
        t('tidak diterapkan dua kali', calls.filter(c => c[0] === 'damage').length, nd);

        // Teknisi melihat revisi
        as('tech', TECH);
        _markLoaded('inspections');
        toasts.length = 0;
        renderInspectionView();
        t('teknisi: kotak perlu direvisi', document.getElementById('insMyNotice').textContent.includes('Foto Camera AI buram'), true);
        t('teknisi: badge tab laporan', document.getElementById('insTabBadge').textContent, '1');
        updateInspectionBadge();
        t('badge Tim tidak ikut menghitung revisi cek', document.getElementById('teamBadge').style.display, 'none');
        editInspection(adhoc.id);
        t('revisi bisa dibuka, catatannya tampil', [document.getElementById('inspectionModal').classList.contains('open'), document.getElementById('insRevisionNote').textContent.includes('buram')], [true, true]);
        t('unit laporan tidak bisa diganti saat edit', document.getElementById('insUnit').disabled, true);
        await tick(50);
        saveInspection(null);
        t('kirim ulang → menunggu', last('ins').approval, 'pending');

        // Teknisi tidak boleh menyetujui
        const n2 = calls.length;
        approveInspection(last('ins').id);
        t('teknisi tidak bisa menyetujui', calls.length, n2);

        // =============== B9: dibuka lagi setelah dicatat ke Kerusakan ===============
        // The server merges each write (setDoc merge), so a field a write leaves out stays.
        const merge = r => { const i = inspections.findIndex(x => x.id === r.id); inspections[i] = { ...inspections[i], ...r }; };
        as('boss', BOSS);
        window.prompt = () => 'Yang rusak Camera AI, bukan Rotary Lamp';
        reviseInspection(rep.id);
        merge(last('ins'));
        t('B9 dibuka lagi: stempel lama masih ada (merge)', [inspections.find(x => x.id === rep.id).approval, !!inspections.find(x => x.id === rep.id).appliedAt], ['revision', true]);
        as('tech', TECH);
        editInspection(rep.id);
        await tick(50);
        setInspectionResult('rotaryLamp', 'good'); setInspectionResult('cameraAi', 'bad');
        document.getElementById('insNote_cameraAi').value = 'Kamera retak';
        saveInspection(null);
        const fixed = last('ins');
        t('B9 teknisi mengirim ulang tanpa menyentuh stempel', [fixed.id, fixed.approval, 'appliedAt' in fixed, 'appliedKeys' in fixed], [rep.id, 'pending', false, false]);
        merge(fixed);
        as('boss', BOSS);
        toasts.length = 0;
        const nd2 = calls.filter(c => c[0] === 'damage').length;
        approveInspection(rep.id);
        await tick(100);
        const newDmg = calls.filter(c => c[0] === 'damage').slice(nd2).map(c => c[1]);
        t('B9 persetujuan ulang: temuan baru masuk ke Kerusakan, yang lama tidak dobel',
          [...new Set(newDmg.map(d => d.component))], ['Camera AI']);
        const dmg2 = newDmg[newDmg.length - 1];
        t('B9 komponen baru di-set Breakdown', globalData.find(u => u.id === 'h1').cameraAi, 'Breakdown');
        const stamp2 = calls.filter(c => c[0] === 'ins').map(c => c[1]).reverse().find(r => r.id === rep.id);
        t('B9 stempel ditambah, bukan diganti', [stamp2.appliedKeys, stamp2.appliedDamageIds], [['rotaryLamp', 'cameraAi'], [dmg.id, dmg2.id]]);
        t('B9 catatan lama untuk komponen yang kini Good disebut', toasts.some(m => /Still open in Damage.*Rotary Lamp/.test(m)), true);
        renderInspectionReports();
        const b9row = [...document.querySelectorAll('#insReportBody tr')].find(tr => tr.textContent.includes('EX-h1'));
        t('B9 tidak ada lagi tombol To Damage, tertulis Logged in Damage', [!!b9row.querySelector('.ins-apply'), b9row.textContent.includes('Logged in Damage')], [false, true]);
        // Approved yet again with the same findings: nothing new is logged.
        merge({ ...stamp2, approval: 'pending', approvedAt: 0, submittedByUid: 'tech' });
        const nd3 = calls.filter(c => c[0] === 'damage').length;
        approveInspection(rep.id);
        await tick(100);
        t('B9 disetujui lagi tanpa perubahan: tidak ada catatan dobel', calls.filter(c => c[0] === 'damage').length, nd3);
        // Camera AI repaired (its record resolved), then the same report sent back for an unrelated
        // correction and approved again: the same check, so nothing is logged again and the unit is
        // not put back into Breakdown. Only a check date after the repair is a new finding.
        const camRec = globalDamages.find(d => d.id === dmg2.id);
        const repDay = inspections.find(x => x.id === rep.id).date;
        camRec.resolved = true; camRec.resolvedAt = repDay;
        globalData.find(u => u.id === 'h1').cameraAi = 'Good';
        merge({ ...inspections.find(x => x.id === rep.id), approval: 'pending', approvedAt: 0, submittedByUid: 'tech' });
        const nd4 = calls.filter(c => c[0] === 'damage').length;
        await tick(5);
        approveInspection(rep.id);
        await tick(100);
        t('B9 diperbaiki lalu disetujui ulang: tidak dicatat lagi, tidak Breakdown lagi',
          [calls.filter(c => c[0] === 'damage').length - nd4, globalData.find(u => u.id === 'h1').cameraAi], [0, 'Good']);
        const curRep = inspections.find(x => x.id === rep.id);
        // The flow above runs on a frozen clock (approvedAt never passes appliedAt), so the
        // 'approved again' case is checked on its own: a same-day repair is never re-logged.
        t('B9 perbaikan hari yang sama, benar-benar disetujui ulang: tidak dicatat lagi',
          inspectionUnappliedBits({ ...curRep, approvedAt: Number(curRep.appliedAt || 0) + 10 }).map(c => c.key), []);
        t('B9 tanggal cek sesudah perbaikan (dicek ulang): dicatat lagi',
          inspectionUnappliedBits({ ...curRep, date: '2099-01-01', approvedAt: Number(curRep.appliedAt || 0) + 10 }).map(c => c.key), ['cameraAi']);
        t('B9 perbaikan tanpa tanggal: tidak pernah dicatat ulang otomatis',
          (() => { camRec.resolvedAt = ''; const k = inspectionUnappliedBits({ ...curRep, date: '2099-01-01', approvedAt: Number(curRep.appliedAt || 0) + 10 }).map(c => c.key); camRec.resolvedAt = repDay; return k; })(), []);
        // A stamp from before 2.45.0 names no components.
        const legacy = { id: 'lg', unitId: 'h2', date: '2026-09-01', approval: 'approved', createdByUid: 'x', results: { cameraAi: 'bad', rotaryLamp: 'bad' }, appliedDamageIds: ['dl1'] };
        globalDamages.push({ id: 'dl1', component: 'Rotary Lamp', fromInspection: 'lg', unitId: 'h2', resolved: false });
        t('B9 stempel lama, belum disetujui ulang: dianggap lengkap', inspectionNeedsApply({ ...legacy, approvedAt: 100, appliedAt: 200 }), false);
        t('B9 stempel lama, disetujui ulang: hanya yang belum ada catatannya',
          inspectionUnappliedBits({ ...legacy, approvedAt: 300, appliedAt: 200 }).map(c => c.key), ['cameraAi']);
        t('B9 stempel yang tidak sampai ke server: catatan dari laporan ini tetap dihitung',
          inspectionUnappliedBits({ ...legacy, appliedDamageIds: [], approvedAt: 300 }).map(c => c.key), ['cameraAi']);
        globalDamages = globalDamages.filter(d => d.id !== 'dl1');
        // 2.45.2: a photo the report holds that could not be downloaded (both reads give nothing) is
        // named in a warning — the Damage record is made without it.
        const realGet = window.cloud.getInspectionPhotos;
        window.cloud.getInspectionPhotos = (id, o) => Promise.resolve({ telematicBox: '' });
        inspections.push({ id: 'rx', unitId: 'h2', date: '2026-09-02', approval: 'approved', approvedAt: 100, createdByUid: 'x',
            results: { telematicBox: 'bad' }, notes: {}, photoKeys: ['telematicBox'] });
        toasts.length = 0;
        await applyInspectionFindings('rx');
        window.cloud.getInspectionPhotos = realGet;
        const rxDmg = calls.filter(c => c[0] === 'damage').map(c => c[1]).reverse().find(d => d.fromInspection === 'rx');
        t('B6 foto gagal diunduh: catatan tanpa foto + peringatan', [rxDmg && rxDmg.hasPhoto, toasts.some(m => m.includes('1 inspection photo(s) could not be loaded'))], [false, true]);
        inspections = inspections.filter(x => x.id !== 'rx');

        // =============== Kotak Keputusan ===============
        as('owner', { role: 'owner' });
        inspections = [{ id: 'z', unitId: 'h2', date: '2026-09-01', approval: 'approved', createdByUid: 'x', results: { cameraAi: 'bad' } }];
        const g = decisionGroups().map(x => x.key);
        t('kotak keputusan: terlambat + temuan belum dicatat', ['insOverdue', 'insApply'].every(k => g.includes(k)), true);

        // =============== CSV ===============
        let csv = '';
        const realBlob = window.Blob;
        window.Blob = function (parts, opts) { csv = parts.join(''); return new realBlob(parts, opts); };
        navigateTo('inspection'); switchInspectionTab('reports');
        exportInspectionCSV();
        window.Blob = realBlob;
        t('CSV memuat kolom komponen dan cek berikutnya', ['Camera AI', 'Rotary Lamp Notes', 'Next Check', '2026-09-15'].every(x => csv.includes(x)), true);

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
