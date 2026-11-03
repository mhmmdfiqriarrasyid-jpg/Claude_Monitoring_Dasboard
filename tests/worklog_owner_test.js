// 2.16.0 — Daily Report atas nama yang login: member ditautkan ke akun login
// (owner, di Manage Members), akun 'own' terkunci ke member-nya, deskripsi per
// paddock, dan mode Office (deskripsi + minimal 1 foto).
const { launch, BASE_URL } = require('./_env');

(async () => {
    const b = await launch();
    const page = await b.newPage({ viewport: { width: 1280, height: 950 } });
    const errors = [];
    page.on('pageerror', e => { if (!/Chart is not defined/.test(e.message)) errors.push(e.message); });
    await page.goto(BASE_URL + '/index.html', { waitUntil: 'load' });
    await page.waitForTimeout(1000);
    await page.addScriptTag({ content: `(async () => { try {
        const T = []; const t = (n, g, w) => T.push({ n, g, w, pass: JSON.stringify(g) === JSON.stringify(w) });
        const tick = ms => new Promise(r => setTimeout(r, ms || 20));
        const toasts = []; window.showToast = m => toasts.push(String(m));
        window.confirm = () => true;
        const members = [], logs = [];
        const ok = () => Promise.resolve();
        window.cloud = { isReady: true, addHistoryEvents: ok, subscribeUsers: () => () => {}, subscribeRoles: () => () => {},
            saveTeamMember: r => { members.push(JSON.parse(JSON.stringify(r))); return ok(); },
            saveWorkLog: r => { logs.push(JSON.parse(JSON.stringify(r))); return ok(); },
            saveWorkLogPhotos: ok, deleteWorkLogPhotos: ok };
        const asOwner = () => { currentUser = { uid: 'o', email: 'o@x.id' }; currentUserDoc = { uid: 'o', role: 'owner', status: 'active', email: 'o@x.id' }; };
        asOwner(); hideAuthGates(); applyRoleGating();
        allUsers = [{ uid: 'o', email: 'o@x.id', role: 'owner', status: 'active' },
                    { uid: 'uA', email: 'andi@x.id', displayName: 'Andi', role: 'staff', status: 'active' },
                    { uid: 'uB', email: 'budi@x.id', role: 'staff', status: 'active' },
                    { uid: 'uP', email: 'pending@x.id', role: 'viewer', status: 'pending' }];
        globalData = [{ id: 'u1', name: 'GGTR101G', sn: 'SN1', status: 'Good' }];
        teamMembers = [{ id: 'm1', name: 'Andi', company: '', active: true }, { id: 'm2', name: 'Budi', company: '', active: true }];
        workLogs = []; leaveRequests = []; teamShifts = [];

        // ---------- owner menautkan akun ----------
        navigateTo('team');
        openTeamMembersModal(); await tick();
        const sel = document.querySelector('.member-item__account');
        t('owner: pilihan akun login per member (hanya akun aktif)', [...sel.options].map(o => o.value), ['', 'o', 'uA', 'uB']);
        setMemberAccount('m1', 'uA'); await tick();
        t('tautan tersimpan', [members[0].id, members[0].accountUid, members[0].accountEmail], ['m1', 'uA', 'andi@x.id']);
        members.length = 0;
        setMemberAccount('m2', 'uA'); await tick();
        t('akun dipindah: member lama dilepas', members.map(m => [m.id, m.accountUid]).sort(), [['m1', ''], ['m2', 'uA']]);
        setMemberAccount('m1', 'uA'); await tick();   // kembalikan
        closeTeamMembersModal();

        // ---------- akun 'own' tanpa tautan ----------
        currentUser = { uid: 'uB', email: 'budi@x.id' };
        currentUserDoc = { uid: 'uB', role: 'staff', status: 'active', access: { teamLog: 'own' } };
        showAddWorkLogForm();
        t('own tanpa tautan: form tidak dibuka, diberi tahu', [document.getElementById('workLogModal').classList.contains('open'), /not linked/.test(toasts.pop())], [false, true]);

        // ---------- akun 'own' tertaut: terkunci ----------
        currentUser = { uid: 'uA', email: 'andi@x.id' };
        currentUserDoc = { uid: 'uA', role: 'staff', status: 'active', access: { teamLog: 'own' } };
        showAddWorkLogForm();
        const mSel = document.getElementById('wlMember');
        t('own tertaut: member = dirinya dan terkunci', [mSel.value, mSel.disabled], ['m1', true]);
        t('mode Field: paddock tampil, deskripsi umum tersembunyi', [document.getElementById('wlFieldGroup').style.display, document.getElementById('wlOfficeGroup').style.display], ['', 'none']);
        t('kartu paddock punya deskripsi sendiri', !!document.getElementById('wlJobDesc_0'), true);
        mSel.value = 'm2';
        saveWorkLog({ preventDefault() {} });
        t('own: memilih member lain ditolak', /only file Daily Reports as yourself/.test(toasts.pop()), true);
        mSel.value = 'm1';

        // ---------- Office ----------
        document.querySelector('input[name="wlPlace"][value="office"]').checked = true; syncWorkLogPlace();
        t('mode Office: paddock tersembunyi, deskripsi tampil, foto wajib', [document.getElementById('wlFieldGroup').style.display, document.getElementById('wlOfficeGroup').style.display, document.getElementById('wlPhotoNeed').textContent], ['none', '', 'at least 1']);
        document.getElementById('wlDate').value = toISODate();
        saveWorkLog({ preventDefault() {} });
        t('Office: deskripsi wajib', toasts.pop(), 'Work description cannot be empty');
        document.getElementById('wlTask').value = 'Rekap data lisensi';
        saveWorkLog({ preventDefault() {} });
        t('Office: foto wajib', toasts.pop(), 'Add at least one photo of the office work');
        _wlPhotos = ['data:image/jpeg;base64,AA']; _wlPhotosDirty = true;
        saveWorkLog({ preventDefault() {} }); await tick(40);
        const off = logs[logs.length - 1];
        t('Office tersimpan: tanpa paddock, deskripsi = task', off && [off.place, off.jobs, off.task, off.memberId, off.paddock], ['office', [], 'Rekap data lisensi', 'm1', '']);
        closeWorkLogModal(true);

        // ---------- staff (edit) tertaut: terisi, tidak terkunci ----------
        currentUserDoc = { uid: 'uA', role: 'staff', status: 'active', access: { teamLog: 'edit' } };
        showAddWorkLogForm();
        t('edit-level: member terisi dirinya tapi bisa diganti', [mSel.value, mSel.disabled], ['m1', false]);
        closeWorkLogModal(true);

        // ---------- laporan lama: deskripsi umum pindah ke paddock pertama ----------
        asOwner();
        workLogs = [{ id: 'old', date: '2026-10-01', memberId: 'm2', memberName: 'Budi', task: 'Olah tanah blok 3', approval: 'draft',
                      jobs: [{ paddock: 'P-1', jobType: 'Tillage', units: [{ id: 'u1', name: 'GGTR101G', sn: 'SN1' }] }] }];
        editWorkLog('old'); await tick();
        t('laporan lama: deskripsi masuk ke paddock pertama', [_wlJobs[0].desc, document.getElementById('wlJobDesc_0').value, _wlPlace()], ['Olah tanah blok 3', 'Olah tanah blok 3', 'field']);
        closeWorkLogModal(true);
        workLogs = [{ ...off, id: 'offold', approval: 'draft', photoCount: 1 }];
        editWorkLog('offold'); await tick();
        t('laporan Office dibuka lagi sebagai Office', [_wlPlace(), document.getElementById('wlTask').value], ['office', 'Rekap data lisensi']);
        closeWorkLogModal(true);
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
