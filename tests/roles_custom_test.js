// v175 — peran kustom dan Daily Report "Own only".
// Owner membuat peran (nama + level per halaman), memberi satu peran per
// orang; mengubah peran mengubah semua pemegangnya. Daily Report "own":
// mengisi laporan sendiri, hanya melihat miliknya, terkunci setelah disetujui.
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
        const toasts = []; window.showToast = m => toasts.push(String(m));
        const asked = []; let answer = true; window.confirm = m => { asked.push(String(m)); return answer; };
        const ok = () => Promise.resolve();
        const savedRoles = [], accessWrites = [], roleWrites = [], subArgs = {};
        window.cloud = { isReady: true, addHistoryEvents: ok, subscribeUsers: () => () => {},
            saveRole: r => { savedRoles.push(JSON.parse(JSON.stringify(r))); return ok(); },
            deleteRole: ok,
            updateUserAccess: (uid, access, by, extra) => { accessWrites.push([uid, access, extra]); return ok(); },
            updateUserRole: (uid, role) => { roleWrites.push([uid, role]); return ok(); } };
        currentUser = { uid: 'o', email: 'o@x.id' };
        currentUserDoc = { uid: 'o', role: 'owner', status: 'active', displayName: 'Owner', email: 'o@x.id' };
        hideAuthGates(); applyRoleGating();
        allUsers = [
            { uid: 'o', email: 'o@x.id', role: 'owner', status: 'active' },
            { uid: 'a', email: 'a@x.id', displayName: 'Andi', role: 'khl', status: 'active', access: { teamLog: 'edit' } },
            { uid: 'b', email: 'b@x.id', displayName: 'Budi', role: 'staff', status: 'active', access: {} },
            { uid: 'p', email: 'p@x.id', displayName: 'Putri', role: 'viewer', status: 'pending' }
        ];
        customRoles = []; _rolesLoaded = true;
        navigateTo('users'); renderUsersView(); await tick(10);
        const panel = () => document.getElementById('usersSummary').textContent.replace(/\\s+/g, ' ');
        t('panel peran kosong menawarkan contoh peran', /Add the example roles/.test(panel()), true);
        t('pengguna tanpa peran ditandai', [...document.querySelectorAll('#activeUsersBody .usr-role--none')].length, 2);

        // ---------- contoh peran ----------
        await addStarterRoles();
        t('empat contoh peran disimpan', savedRoles.map(r => r.name), ['Field Team', 'Technician', 'Supervisor', 'Viewer']);
        t('Field Team: Daily Report Own only, Shift View, sisanya None',
          [savedRoles[0].access.teamLog, savedRoles[0].access.teamShift, savedRoles[0].access.damage, savedRoles[0].csv], ['own', 'view', 'none', 'none']);
        customRoles = savedRoles.slice();

        // ---------- membuat peran sendiri ----------
        openRoleModal(); await tick(5);
        t('editor peran: Daily Report punya pilihan "Own only"',
          [...document.querySelectorAll('#roleLvl_teamLog option')].map(o => o.value), ['none', 'own', 'view', 'edit']);
        saveRole(); await tick(5);
        t('nama kosong ditolak', toasts.pop(), 'Give the role a name');
        document.getElementById('roleName').value = 'field team';
        await saveRole();
        t('nama ganda ditolak (tanpa beda huruf besar)', toasts.pop(), 'A role named "field team" already exists');
        document.getElementById('roleName').value = '  Mekanik   Lapangan ';
        document.getElementById('roleLvl_damage').value = 'edit';
        document.getElementById('roleLvl_teamLog').value = 'own';
        document.getElementById('roleCsv').value = 'export';
        const nRoles = savedRoles.length;
        await saveRole();
        const mek = savedRoles[savedRoles.length - 1];
        t('peran baru tersimpan, nama dirapikan', [savedRoles.length - nRoles, mek.name, mek.access.damage, mek.access.teamLog, mek.access.editUnits, mek.csv],
          [1, 'Mekanik Lapangan', 'edit', 'own', 'none', 'export']);
        t('editor tertutup setelah simpan', document.getElementById('roleModal').classList.contains('open'), false);

        // ---------- memberi peran ke orang ----------
        openAccessModal('a'); await tick(5);
        t('dialog Access: pilihan peran, tanpa 14 baris area',
          [document.querySelectorAll('#accessGrid input[name="accessRole"]').length, document.querySelectorAll('#accessGrid [data-row-area]').length], [5, 0]);
        await saveAccess();
        t('tanpa memilih peran ditolak', toasts.pop(), 'Pick a role');
        document.querySelector('#accessGrid input[name="accessRole"][value="' + mek.id + '"]').checked = true;
        await saveAccess();
        const wA = accessWrites[accessWrites.length - 1];
        t('simpan: level peran + kelompok ditulis, roleId/roleName ikut',
          [wA[0], wA[1].damage, wA[1].teamLog, wA[1].warehouse, wA[1].csv, wA[1].unitScope, wA[2].roleId, wA[2].roleName],
          ['a', 'edit', 'own', 'none', 'export', 'all', mek.id, 'Mekanik Lapangan']);
        t('jabatan lama (KHL) menjadi akun biasa', roleWrites.pop(), ['a', 'staff']);
        Object.assign(allUsers[1], { role: 'staff', access: wA[1], roleId: mek.id, roleName: mek.name });
        renderUsersView(); await tick(5);
        t('tabel: nama peran tampil', /Mekanik Lapangan/.test(document.getElementById('activeUsersBody').textContent), true);

        // ---------- mengubah peran = berlaku untuk semua pemegangnya ----------
        openRoleModal(mek.id); await tick(5);
        t('editor menyebut pemegang peran', document.getElementById('roleHolders').textContent, 'Held by 1 person: Andi');
        document.getElementById('roleLvl_warehouse').value = 'view';
        accessWrites.length = 0; asked.length = 0;
        answer = false; await saveRole();
        t('perubahan ditanyakan dulu dengan daftar dan jumlah orang', [/Warehouse: None → View/.test(asked[0] || ''), /1 person/.test(asked[0] || ''), accessWrites.length], [true, true, 0]);
        answer = true; await saveRole();
        t('setelah ya: pemegang ikut berubah', accessWrites.map(w => [w[0], w[1].warehouse, w[1].damage]), [['a', 'view', 'edit']]);
        t('tidak ada pengecualian per orang: level persis sama dengan peran', userRoleInSync(allUsers[1], customRoles.find(r => r.id === mek.id)), true);

        // ---------- tidak sinkron & hapus ----------
        allUsers[1].access = Object.assign({}, allUsers[1].access, { damage: 'view' });
        renderUsersView(); await tick(5);
        t('akses yang berbeda dari perannya ditandai "Not in sync"', /Not in sync/.test(document.getElementById('activeUsersBody').textContent), true);
        openRoleModal(mek.id); await tick(5);
        await deleteRole();
        t('peran yang masih dipakai tidak bisa dihapus', toasts.pop(), '1 person holds this role — give them another role first');
        closeRoleModal();

        // ---------- persetujuan pendaftar langsung dengan peran ----------
        renderUsersView(); await tick(5);
        t('pendaftar: pilihan peran dari daftar buatan owner', [...document.querySelectorAll('#approveRole_p option')].map(o => o.textContent), ['Role later'].concat(customRoles.map(r => r.name)));
        accessWrites.length = 0;
        await approveUser('p', customRoles[0].id);
        t('disetujui dengan peran: akun biasa + level peran', [roleWrites.pop(), accessWrites[0] && accessWrites[0][2].roleName], [['p', 'staff'], customRoles[0].name]);

        // ---------- Daily Report "own" untuk pemegang peran ----------
        currentUser = { uid: 'a', email: 'a@x.id' };
        currentUserDoc = Object.assign({ status: 'active' }, allUsers[1], { access: Object.assign({}, allUsers[1].access, { damage: 'edit' }) });
        applyRoleGating();
        const mine = { id: 'w1', createdByUid: 'a', approval: 'draft' };
        const minePending = { id: 'w2', createdByUid: 'a', approval: 'pending', submittedByUid: 'a' };
        const mineApproved = { id: 'w3', createdByUid: 'a', approval: 'approved' };
        const others = { id: 'w4', createdByUid: 'z', approval: 'draft' };
        t('own: boleh membuat laporan (tombol Add tetap ada)', [ownTeamOnly(), hasAccess('teamLog', 'edit'), canViewView('team')], [true, true, true]);
        t('own: ubah drafnya sendiri, bukan milik orang lain, bukan yang sudah disetujui',
          [canEditTeamRecord(mine), canEditTeamRecord(others), canEditTeamRecord(mineApproved)], [true, false, false]);
        t('own: menarik kembali kirimannya sendiri saja', [canWithdrawTeamRecord(minePending), canWithdrawTeamRecord({ id: 'w5', createdByUid: '', approval: 'pending' })], [true, false]);
        t('own: alasan tolak yang jelas', lockedReason(others, 'Report'), 'This report belongs to someone else');
        t('own: tidak bisa menyetujui', canApproveWorkLogs(), false);
        // langganan: hanya laporan sendiri
        ['subscribeUnits', 'subscribeImplements', 'subscribeDamages', 'subscribeLicenses', 'subscribeMasterLists', 'subscribeDevices',
         'subscribeStockItems', 'subscribeTeamMembers', 'subscribeShifts', 'subscribeInspections', 'subscribeInspectionPlans', 'subscribeLicenseRequirements']
            .forEach(k => { window.cloud[k] = () => () => {}; });
        window.cloud.subscribeWorkLogs = (cb, err, author) => { subArgs.workLogs = author; return () => {}; };
        window.cloud.subscribeLeaveRequests = (cb, err, author) => { subArgs.leave = author; return () => {}; };
        cloudInitialized = false; initCloudSync(); await tick(20);
        t('own: langganan laporan & izin hanya milik sendiri (tanpa jadwal)', [subArgs.workLogs, subArgs.leave], ['a', 'a']);
        // pemegang Edit biasa tetap membaca semua
        currentUserDoc = { uid: 'b', role: 'staff', status: 'active', access: { teamLog: 'edit', teamShift: 'view' } };
        currentUser = { uid: 'b', email: 'b@x.id' }; applyRoleGating();
        cloudInitialized = false; initCloudSync(); await tick(20);
        t('Edit biasa: langganan semua laporan', [subArgs.workLogs, subArgs.leave], ['', '']);
        t('Edit biasa: boleh mengubah draf orang lain (seperti sebelumnya)', canEditTeamRecord(others), true);

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
