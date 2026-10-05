const { launch, BASE_URL } = require('./_env');

(async () => {
    const b = await launch();
    const page = await b.newPage({ viewport: { width: 1440, height: 900 } });
    const errors = [];
    page.on('pageerror', e => { if (!/Chart is not defined/.test(e.message)) errors.push(e.message); });
    await page.goto(BASE_URL + '/index.html', { waitUntil: 'load' });
    await page.waitForTimeout(1000);

    await page.addScriptTag({ content: `(() => { try {
        const T = []; const t = (n, g, w) => T.push({ n, g, w, pass: JSON.stringify(g) === JSON.stringify(w) });
        const as = doc => { currentUserDoc = doc; };

        window.cloud = { isReady: true,
            saveShift:()=>Promise.resolve(), deleteShift:()=>Promise.resolve(),
            saveTeamMember:()=>Promise.resolve(), deleteTeamMember:()=>Promise.resolve(),
            saveWorkLog:()=>Promise.resolve(), deleteWorkLog:()=>Promise.resolve() };
        currentUser = { uid:'u1', email:'o@x.id' };
        hideAuthGates();
        globalData = [];
        teamMembers = [{id:'m1',name:'Andi',jobTitle:'Mekanik',active:true}];
        teamShifts  = [{id:'2026-09-14_m1',date:'2026-09-14',memberId:'m1',shift:'pagi'}];
        workLogs    = [{id:'w1',date:'2026-09-14',memberId:'m1',memberName:'Andi',start:'08:00',end:'16:00',task:'Servis',issue:''}];
        teamWeekStart = '2026-09-14';

        // ---------- role definitions ----------
        t('empat role', ROLES.map(r=>r.key), ['owner','staff','pbt','khl']);
        t('label Super Admin', roleLabel('owner'), 'Super Admin');
        t('label Staff/PBT/KHL', [roleLabel('staff'),roleLabel('pbt'),roleLabel('khl')], ['Staff','PBT','KHL']);
        t('role lama dikenali', [isLegacyRole('team'), isLegacyRole('viewer')], [true,true]);
        t('role baru bukan role lama', ROLES.map(r=>isLegacyRole(r.key)), [false,false,false,false]);

        // ---------- new roles grant nothing ----------
        ['staff','pbt','khl'].forEach(role => {
            as({ role, status:'active' });
            const anything = ACCESS_AREAS.some(a => effectiveAccess(a.key) !== 'none');
            t(role + ': tanpa akses bawaan', anything, false);
            t(role + ': tidak bisa edit apa pun', canEditAnyArea(), false);
            t(role + ': CSV tertutup', effectiveCsv(), 'none');
        });

        // ---------- legacy roles unchanged ----------
        as({ role:'team', status:'active' });
        t('role lama team: edit armada', effectiveAccess('editUnits'), 'edit');
        t('role lama team: history hanya lihat', effectiveAccess('history'), 'view');
        t('role lama team: edit ketiga area Tim',
          ['teamShift','teamLog','teamMembers'].map(a=>effectiveAccess(a)), ['edit','edit','edit']);
        t('role lama team: CSV penuh', effectiveCsv(), 'full');
        as({ role:'viewer', status:'active' });
        t('role lama viewer: tertutup', ACCESS_AREAS.some(a=>effectiveAccess(a.key)!=='none'), false);

        // ---------- owner ----------
        as({ role:'owner', status:'active' });
        t('Super Admin: semua edit', ACCESS_AREAS.every(a=>effectiveAccess(a.key)==='edit'), true);
        t('Super Admin: isOwner', isOwner(), true);

        // ---------- three separate team areas ----------
        t('area Tim terpisah empat (termasuk persetujuan)',
          ACCESS_AREAS.filter(a=>a.key.startsWith('team')).map(a=>a.key),
          ['teamShift','teamLog','teamMembers','teamLogApprove']);
        t('area "team" lama sudah tidak ada', ACCESS_AREAS.some(a=>a.key==='team'), false);

        // KHL persis seperti yang diminta: jadwal lihat saja, laporan boleh isi.
        const KHL = { role:'khl', status:'active',
                      access:{ teamShift:'view', teamLog:'edit', teamMembers:'view' } };
        as(KHL);
        t('KHL: menu Tim terbuka', canViewView('team'), true);
        t('KHL: jadwal hanya lihat', hasAccess('teamShift','edit'), false);
        t('KHL: laporan boleh edit', hasAccess('teamLog','edit'), true);
        t('KHL: anggota hanya lihat', hasAccess('teamMembers','edit'), false);
        t('KHL: dihitung sebagai editor', canEditAnyArea(), true);
        t('KHL: menu armada tertutup', canViewView('editUnits'), false);

        navigateTo('team'); switchTeamTab('shift');
        t('KHL: grid jadwal tanpa dropdown', document.querySelectorAll('#shiftBody .shift-select').length, 0);
        t('KHL: grid jadwal pakai badge', document.querySelectorAll('#shiftBody .shift-badge').length, 1);
        switchTeamTab('worklog');
        t('KHL: laporan punya tombol aksi', document.querySelectorAll('#workLogBody .row-actions').length, 1);
        applyAccessVisibility();
        t('KHL: jadwal ditandai read-only', document.body.dataset.roTeamshift, '1');
        t('KHL: laporan tidak read-only', document.body.dataset.roTeamlog, undefined);
        t('KHL: anggota ditandai read-only', document.body.dataset.roTeammembers, '1');

        // Kebalikannya: boleh atur jadwal, tidak boleh sentuh laporan.
        as({ role:'staff', status:'active', access:{ teamShift:'edit', teamLog:'view', teamMembers:'edit' } });
        renderTeamView(); switchTeamTab('shift');
        t('Staff: tiap hari bisa diubah (1 anggota x 7 hari)',
          document.querySelectorAll('#shiftBody .shift-select').length, 7);
        t('Staff: nilai shift terbaca',
          document.querySelector('#shiftBody .shift-select').value, 'pagi');
        switchTeamTab('worklog');
        t('Staff: laporan tanpa tombol aksi', document.querySelectorAll('#workLogBody .row-actions').length, 0);

        // ---------- tab visibility ----------
        as({ role:'pbt', status:'active', access:{ teamShift:'none', teamLog:'edit', teamMembers:'none' } });
        teamTab = 'shift';           // tab tersimpan yang tidak boleh dibuka
        renderTeamView();
        t('hanya laporan: pindah otomatis ke tab laporan', teamTab, 'worklog');
        t('hanya laporan: tab jadwal disembunyikan',
          document.querySelector('.team-tab[data-tab="shift"]').style.display, 'none');
        t('hanya laporan: tombol Kelola Anggota disembunyikan',
          document.getElementById('btnTeamMembers').style.display, 'none');

        as({ role:'pbt', status:'active', access:{ teamShift:'none', teamLog:'none', teamMembers:'edit' } });
        renderTeamView();
        t('hanya anggota: menu Tim tetap terbuka', canViewView('team'), true);
        t('hanya anggota: panel penjelasan muncul',
          document.getElementById('teamNoPanel').style.display, '');
        t('hanya anggota: tombol Kelola Anggota tampil',
          document.getElementById('btnTeamMembers').style.display, '');

        as({ role:'khl', status:'active', access:{ teamShift:'none', teamLog:'none', teamMembers:'none' } });
        t('tanpa akses Tim: menu tertutup', canViewView('team'), false);
        applyAccessVisibility();
        t('tanpa akses Tim: link nav disembunyikan',
          document.querySelector('.nav__link[data-view="team"]').style.display, 'none');

        // ---------- write guards actually refuse ----------
        as({ role:'khl', status:'active', access:{ teamShift:'view', teamLog:'edit', teamMembers:'view' } });
        const calls = [];
        window.cloud.saveShift = r => { calls.push('shift'); return Promise.resolve(); };
        window.cloud.saveTeamMember = r => { calls.push('member'); return Promise.resolve(); };
        setShift('m1','2026-09-16','pagi');
        t('KHL: tulis jadwal ditolak', calls.includes('shift'), false);
        toggleTeamMember('m1');
        t('KHL: ubah anggota ditolak', calls.includes('member'), false);

        as({ role:'owner', status:'active' });
        window.__T = T;
    } catch (e) { window.__T = [{n:'THREW: '+e.message+' | '+(e.stack||'').split('\\n')[1], g:1, w:0, pass:false}]; }
    })();` });

    const res = await page.evaluate(() => window.__T);
    const fail = res.filter(r => !r.pass);
    res.forEach(r => console.log(`${r.pass ? 'PASS' : 'FAIL'}  ${r.n}${r.pass ? '' : `\n        dapat=${JSON.stringify(r.g)} harap=${JSON.stringify(r.w)}`}`));
    console.log(`\n${res.length - fail.length}/${res.length} lulus`);
    console.log('PAGE ERRORS:', errors.length ? errors : 'none');
    await b.close();
    process.exit(fail.length ? 1 : 0);
})();
