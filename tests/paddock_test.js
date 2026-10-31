// 2.10.0 — Paddock: daftar per PT (Master lists), impor dari Excel, dan
// Daily Report berupa baris Paddock → Type Job → Unit (paddock wajib).
const { launch, BASE_URL } = require('./_env');

(async () => {
    const b = await launch();
    const page = await b.newPage({ viewport: { width: 1440, height: 1000 } });
    const errors = [];
    page.on('pageerror', e => { if (!/Chart is not defined/.test(e.message)) errors.push(e.message); });
    await page.goto(BASE_URL + '/index.html', { waitUntil: 'load' });
    await page.waitForTimeout(1000);

    await page.addScriptTag({ content: `(async () => { try {
        const T = []; const t = (n, g, w) => T.push({ n, g, w, pass: JSON.stringify(g) === JSON.stringify(w) });
        const tick = ms => new Promise(r => setTimeout(r, ms || 20));
        const toasts = []; window.showToast = m => toasts.push(String(m));
        window.confirm = () => true;
        const saved = [], logs = [];
        const ok = () => Promise.resolve();
        window.cloud = { isReady: true, addHistoryEvents: ok,
            savePaddockList: r => { saved.push(['pd', JSON.parse(JSON.stringify(r))]); return ok(); },
            deletePaddockList: id => { saved.push(['delPd', id]); return ok(); },
            saveMasterList: (k, v) => { saved.push(['ml', k, v]); return ok(); },
            saveWorkLog: r => { logs.push(JSON.parse(JSON.stringify(r))); return ok(); },
            saveWorkLogPhotos: ok, deleteWorkLogPhotos: ok };
        currentUser = { uid: 'o', email: 'o@x.id' };
        currentUserDoc = { uid: 'o', role: 'owner', status: 'active', email: 'o@x.id' };
        hideAuthGates(); applyRoleGating();
        masterLists.company = ['PT. Global Papua Abadi', 'PT. Murni Nusantara Mandiri'];
        globalData = [{ id: 'u1', name: 'GGTR101G', sn: 'SN1', status: 'Good' }, { id: 'u2', name: 'GGTR102G', sn: 'SN2', status: 'Good' },
                      { id: 'u3', name: 'MMTR040M', sn: 'SN3', status: 'Good' }];
        teamMembers = [{ id: 'm1', name: 'Andi', company: 'PT. Global Papua Abadi', active: true },
                       { id: 'm2', name: 'Budi', company: 'PT. Murni Nusantara Mandiri', active: true },
                       { id: 'm3', name: 'Citra', company: 'PT. Lain', active: true }];
        workLogs = []; leaveRequests = []; teamShifts = [];

        // ---------- impor: baris Excel → paddock ----------
        const rows = [['No', 'Region', 'Farm', 'Block', 'Paddock', 'Paddock ID (PID)', 'Landuse Type', 'Luas (Ha)'],
            [1, 'JAGF', 1, 'A', '000', 'JAGF-1-A-000', 'RD', 7.43],
            [2, 'JAGF', 1, 'A', '001', 'JAGF-1-A-001', 'CU', 3.64],
            [3, 'JAGF', 1, 'A', '008', 'JAGF-1-A-008', 'CP', 13.6],
            [4, 'JAGF', 1, 'A', '008', 'JAGF-1-A-008', 'CU', 6.8],
            ['TOTAL KESELURUHAN GPA', '', '', '', '', '', '', 34394]];
        const items = paddocksFromRows(rows);
        t('impor: PID unik, landuse digabung, luas dijumlah, baris total dibuang',
          items.map(i => [i.pid, i.landuse, Math.round(i.ha * 10) / 10]), [['JAGF-1-A-000', 'RD', 7.4], ['JAGF-1-A-001', 'CU', 3.6], ['JAGF-1-A-008', 'CP, CU', 20.4]]);
        t('impor: tanpa kolom PID → null', paddocksFromRows([['a', 'b'], [1, 2]]), null);
        t('nama sheet → PT (singkatan GPA / MNM)', [companyForSheet('GPA'), companyForSheet('MNM'), companyForSheet('XYZ')],
          ['PT. Global Papua Abadi', 'PT. Murni Nusantara Mandiri', '']);

        openMasterLists('paddock'); await tick();
        t('tab Paddocks ada di Master lists', [...document.querySelectorAll('#mlTabs .team-tab')].some(x => /Paddocks/.test(x.textContent)), true);
        t('daftar biasa disembunyikan di tab Paddocks', [document.getElementById('mlPaddocks').hidden, document.getElementById('mlAddForm').style.display], [false, 'none']);
        _pdImport = [{ sheet: 'GPA', company: companyForSheet('GPA'), items, mode: 'replace' },
                     { sheet: 'MNM', company: companyForSheet('MNM'), items: paddocksFromRows([['Paddock ID (PID)', 'Landuse Type'], ['BLAF-1-A-001', 'CU'], ['BLAF-1-A-002', 'CU']]), mode: 'replace' }];
        renderMasterLists();
        t('pratinjau impor: satu baris per sheet', document.querySelectorAll('.pd-import__row').length, 2);
        savePaddockImport(); await tick();
        const pds = saved.filter(x => x[0] === 'pd').map(x => x[1]);
        t('tersimpan satu dokumen per PT', pds.map(r => [r.id, r.company, r.items.length]),
          [['ptglobalpapuaabadi', 'PT. Global Papua Abadi', 3], ['ptmurninusantaramandiri', 'PT. Murni Nusantara Mandiri', 2]]);
        applyPaddocksSnapshot(pds);
        t('jumlah paddock di tab', paddockCount(), 5);
        _pdCompany = 'PT. Global Papua Abadi'; renderMasterLists();
        document.getElementById('pdNewPid').value = 'jagf-1-b-014';
        addPaddock();
        let last = saved.filter(x => x[0] === 'pd').pop()[1];
        t('tambah paddock: huruf besar, urut', last.items.map(i => i.pid), ['JAGF-1-A-000', 'JAGF-1-A-001', 'JAGF-1-A-008', 'JAGF-1-B-014']);
        document.getElementById('pdNewPid').value = 'JAGF-1-B-014'; addPaddock();
        t('paddock ganda ditolak', toasts.pop(), 'JAGF-1-B-014 is already in the list');
        removePaddock('JAGF-1-A-000');
        last = saved.filter(x => x[0] === 'pd').pop()[1];
        t('hapus paddock', last.items.length, 3);
        applyPaddocksSnapshot([last, pds[1]]);
        closeMasterLists();

        // ---------- Job Types ----------
        t('Job Type bawaan', jobTypeChoices(), ['Application', 'Tillage', 'Harvesting', 'Planting']);

        // ---------- Daily Report ----------
        navigateTo('team'); switchTeamTab('worklog');
        showAddWorkLogForm(); await tick();
        document.getElementById('wlMember').value = 'm1';
        document.getElementById('wlMember').dispatchEvent(new Event('change'));
        t('paddock yang ditawarkan: milik PT anggota',
          [...document.querySelectorAll('#paddockList option')].map(o => o.value), ['JAGF-1-A-001', 'JAGF-1-A-008', 'JAGF-1-B-014']);
        document.getElementById('wlTask').value = 'Olah tanah';
        saveWorkLog({ preventDefault() {} });
        t('paddock wajib', toasts.pop(), 'Fill in the paddock area');
        _wlJobs[0].paddock = 'BLAF-1-A-001';
        saveWorkLog({ preventDefault() {} });
        t('paddock PT lain ditolak', toasts.pop(), 'Paddock "BLAF-1-A-001" is not in the paddock list of PT. Global Papua Abadi — pick one from the list');
        const pIn = document.getElementById('wlJobPaddock_0');
        pIn.value = 'jagf-1-a-001'; pIn.dispatchEvent(new Event('input')); pIn.dispatchEvent(new Event('change'));
        t('paddock dirapikan ke ejaan daftar', [pIn.value, _wlJobs[0].paddock], ['JAGF-1-A-001', 'JAGF-1-A-001']);
        saveWorkLog({ preventDefault() {} });
        t('Type Job wajib', toasts.pop(), 'Select the type of job');
        const ty = document.getElementById('wlJobType_0'); ty.value = 'Tillage'; ty.dispatchEvent(new Event('change'));
        saveWorkLog({ preventDefault() {} });
        t('unit wajib', toasts.pop(), 'Add the unit worked on');
        document.getElementById('wlJobUnit_0').value = 'GGTR101G'; addWorkLogJobUnit(0);
        document.getElementById('wlJobUnit_0').value = 'SN2'; addWorkLogJobUnit(0);
        // baris kedua
        addWorkLogJob();
        t('baris paddock kedua', document.querySelectorAll('#wlJobs .wl-job').length, 2);
        _wlJobs[1].paddock = 'JAGF-1-B-014'; _wlJobs[1].jobType = 'Application';
        saveWorkLog({ preventDefault() {} });
        t('baris kedua juga butuh unit', toasts.pop(), 'Add the unit worked on (paddock 2)');
        document.getElementById('wlJobUnit_1').value = 'GGTR101G';
        saveWorkLog({ preventDefault() {} });   // unit yang diketik tapi belum Add ikut masuk
        const rec = logs[logs.length - 1];
        t('tersimpan: baris paddock lengkap', rec.jobs.map(j => [j.paddock, j.jobType, j.units.map(u => u.id)]),
          [['JAGF-1-A-001', 'Tillage', ['u1', 'u2']], ['JAGF-1-B-014', 'Application', ['u1']]]);
        t('field lama: paddock digabung, unit tanpa ganda', [rec.paddock, rec.units.map(u => u.id), rec.unitId], ['JAGF-1-A-001, JAGF-1-B-014', ['u1', 'u2'], 'u1']);
        t('terkirim untuk dicek', rec.approval, 'pending');
        workLogs = [rec]; renderWorkLogTable();
        const cell = document.querySelector('#workLogBody td[data-label="Paddock"]');
        t('tabel: paddock dengan jenis pekerjaannya', [...cell.querySelectorAll('.wl-job-cell')].map(x => x.textContent.replace(/\\s+/g, ' ').trim()),
          ['JAGF-1-A-001 Tillage', 'JAGF-1-B-014 Application']);

        // draf boleh belum lengkap
        showAddWorkLogForm(); await tick();
        document.getElementById('wlMember').value = 'm2';
        saveWorkLog(null, 'draft');
        t('draf boleh tanpa paddock', logs[logs.length - 1].approval, 'draft');

        // anggota tanpa daftar PT-nya: semua paddock
        showAddWorkLogForm(); await tick();
        document.getElementById('wlMember').value = 'm3';
        document.getElementById('wlMember').dispatchEvent(new Event('change'));
        t('PT tanpa daftar: semua paddock ditawarkan', document.querySelectorAll('#paddockList option').length, 5);
        closeWorkLogModal(true);

        // laporan lama: satu baris dari paddock + unit lama
        workLogs = [{ id: 'old', date: '2026-10-01', memberId: 'm1', memberName: 'Andi', task: 'x', approval: 'draft', createdByUid: 'o',
                      paddock: 'Blok 7', unitId: 'u3', unitName: 'MMTR040M', sn: 'SN3' }];
        editWorkLog('old'); await tick();
        t('laporan lama terbuka sebagai satu baris', _wlJobs.map(j => [j.paddock, j.jobType, j.units.map(u => u.id)]), [['Blok 7', '', ['u3']]]);
        closeWorkLogModal(true);

        // Job Types dari Master lists menggantikan bawaan
        masterLists.jobType = ['Spraying', 'Tillage'];
        showAddWorkLogForm(); await tick();
        t('Job Type dari Master lists', [...document.querySelectorAll('#wlJobType_0 option')].map(o => o.value).slice(1), ['Spraying', 'Tillage']);
        closeWorkLogModal(true);

        // ---------- akses ----------
        currentUserDoc = { uid: 'v', role: 'staff', status: 'active', access: { teamMembers: 'view' } };
        applyRoleGating();
        openMasterLists('paddock'); await tick();
        t('pembaca: tanpa tombol impor dan tambah', [!!document.querySelector('#mlPaddocks .pd-actions'), !!document.getElementById('pdNewPid')], [false, false]);
        closeMasterLists();

        window.__T = T;
    } catch (e) { window.__T = [{ n: 'CRASH ' + e.message + ' ' + (e.stack || '').split('\\n')[1], pass: false }]; } })()` });

    await page.waitForFunction(() => window.__T, null, { timeout: 30000 });
    const T = await page.evaluate(() => window.__T);
    let pass = 0;
    for (const x of T) {
        if (x.pass) pass++;
        console.log(`${x.pass ? 'PASS' : 'FAIL'}  ${x.n}`);
        if (!x.pass && 'g' in x) console.log(`        dapat=${JSON.stringify(x.g)} harap=${JSON.stringify(x.w)}`);
    }
    console.log(`\n${pass}/${T.length} lulus`);
    console.log('PAGE ERRORS:', errors.length ? errors : 'none');
    await b.close();
    process.exit(pass === T.length && !errors.length ? 0 : 1);
})();
