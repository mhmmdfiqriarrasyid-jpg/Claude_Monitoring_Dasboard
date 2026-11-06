// Stored XSS — nilai yang ditulis satu pengguna tidak boleh menjalankan kode
// di sesi pengguna lain (terutama Super Admin).
//
// Audit v119 membuktikan tujuh jalan masuk: nama perangkat sesi di halaman
// Users, data pendaftar baru, nilai shift di kelas HTML, metadata lampiran,
// id di ~50 atribut onclick (escapeHtml dikembalikan browser jadi ' sebelum
// handler berjalan), src foto laporan / surat izin, dan ±17 toast yang
// memakai innerHTML. Setiap muatan di bawah menaikkan window.__pwn kalau
// berhasil berjalan.
const { launch, BASE_URL } = require('./_env');

(async () => {
    const b = await launch();
    const page = await b.newPage({ viewport: { width: 1440, height: 900 } });
    const errors = [];
    page.on('pageerror', e => { if (!/Chart is not defined/.test(e.message)) errors.push(e.message); });
    page.on('dialog', d => d.accept());
    await page.goto(BASE_URL + '/index.html', { waitUntil: 'load' });
    await page.waitForTimeout(1000);

    await page.addScriptTag({ content: `(async () => { try {
        const T = []; const t = (n, g, w) => T.push({ n, g, w, pass: JSON.stringify(g) === JSON.stringify(w) });
        const tick = ms => new Promise(r => setTimeout(r, ms || 30));
        window.__pwn = 0;
        const PWN = '<img src=x onerror="window.__pwn++">';
        const QID = "x');window.__pwn++;//";
        const ok = () => Promise.resolve();
        window.cloud = { isReady: true, saveUnits: ok, deleteUnits: ok, getAllUnits: () => Promise.resolve([]),
            addHistoryEvents: ok, subscribeUsers: () => () => {}, saveShift: ok, deleteShift: ok,
            saveImplement: ok, saveImplements: ok, saveDamage: ok, saveDamages: ok, saveLicense: ok, saveLicenses: ok,
            getWorkLogPhotos: () => Promise.resolve(['data:image/png;base64,AAAA" onerror="window.__pwn++']),
            getTeamDocs: () => Promise.resolve(['x" onerror="window.__pwn++']) };
        window.confirm = () => false;
        window.prompt = () => null;
        currentUser = { uid: 'uO', email: 'o@x.id' };
        currentUserDoc = { role: 'owner', status: 'active', email: 'o@x.id', displayName: 'Owner' };
        try { localStorage.setItem('tractorLicenseDatesApplied_v2', '1'); } catch (_) {}
        hideAuthGates(); applyRoleGating();

        // =============== toast = teks biasa ===============
        showToast('Unit "' + PWN + '" diperbarui', 'success');
        await tick();
        t('toast: tidak ada elemen yang tercipta dari isi pesan', document.querySelectorAll('#toastContainer img').length, 0);
        t('toast: pesannya tampil apa adanya', [...document.querySelectorAll('#toastContainer .toast')].some(e => e.textContent.includes('<img src=x')), true);
        t('toast: ikonnya tetap ada', !!document.querySelector('#toastContainer .toast i.fas'), true);

        // =============== id di onclick ===============
        navigateTo('editUnits');
        const U = { id: QID, name: 'EVIL', sn: 'SN-E', status: 'Good', display: 'Good', gps: 'Good', steering: 'Good', jdlink: 'Good',
                    attachments: [{ id: "a1');window.__pwn++;//", name: 'x.pdf', size: 1 }, { id: 'a2" onmouseover="window.__pwn++', name: 'y.pdf', size: 1 }] };
        globalData = [U]; saveToStorage(globalData);
        renderEditTable();
        await tick();
        let got = null;
        const realProfile = window.showUnitProfile;
        window.showUnitProfile = id => { got = id; };
        document.querySelector('#editBody button[title="Profile"]').click();
        window.showUnitProfile = realProfile;
        t('onclick: id berkutip tidak menjalankan kode', window.__pwn, 0);
        t('onclick: fungsi menerima id-nya persis', got, QID);
        let gotAtt = null;
        window.downloadAttachment = id => { gotAtt = id; };
        document.querySelector('.attach-chip__dl').click();
        t('lampiran: id berkutip diteruskan persis, tidak berjalan', [gotAtt, window.__pwn], ["a1');window.__pwn++;//", 0]);
        const chip2 = document.querySelectorAll('.attach-chip__dl')[1];
        chip2.dispatchEvent(new MouseEvent('mouseover', { bubbles: true }));
        t('lampiran: id berkutip-ganda tidak menambah atribut', [chip2.hasAttribute('onmouseover'), window.__pwn], [false, 0]);

        // =============== nilai shift di kelas ===============
        navigateTo('team');
        const today = toISODate();
        teamMembers = [{ id: 'm1', name: 'Andi', company: 'PT. GPA', active: true }];
        teamShifts = [{ id: today + '_m1', date: today, memberId: 'm1', shift: 'pagi"><img src=x onerror="window.__pwn++">' }];
        teamWeekStart = startOfWeekISO(today);
        switchTeamTab('shift');
        await tick(80);
        t('shift: nilai tak dikenal tidak menjadi markup', [document.querySelectorAll('#shiftBody img, .shift-grid img').length, window.__pwn], [0, 0]);
        t('shift: dianggap kosong', shiftFor('m1', today), '');

        // =============== src foto ===============
        workLogs = [{ id: 'w1', date: today, memberId: 'm1', memberName: 'Andi', start: '08:00', end: '16:00', task: 'x', photoCount: 1 }];
        editWorkLog('w1');
        await tick(80);
        t('foto laporan: src berbahaya tidak menjadi atribut', [document.querySelectorAll('#wlPhotoPreviews img[onerror]').length, window.__pwn], [0, 0]);
        closeWorkLogModal(true);
        leaveRequests = [{ id: 'l1', memberId: 'm1', type: 'sakit', dateFrom: today, dateTo: today, days: 1, docCount: 1, approval: 'pending' }];
        editLeave('l1');
        await tick(80);
        t('surat izin: src berbahaya tidak menjadi atribut', [document.querySelectorAll('#lvDocPreviews img[onerror]').length, window.__pwn], [0, 0]);
        closeLeaveModal(true);
        t('safeImageSrc hanya menerima data URL gambar', [safeImageSrc('data:image/jpeg;base64,QUJD'), safeImageSrc('javascript:alert(1)'), safeImageSrc('data:image/png;base64,A" x="')],
          ['data:image/jpeg;base64,QUJD', '', '']);

        // =============== halaman Users ===============
        navigateTo('users');
        const users = [
            { uid: "p1');window.__pwn++;//", email: 'a' + PWN + '@x.id', displayName: 'Nama ' + PWN, role: 'viewer', status: 'pending', createdAt: 1 },
            { uid: 'u2', email: 'b@x.id', displayName: 'Budi', role: 'staff', status: 'active', createdAt: 1,
              activeSession: { id: 's1', startedAt: 1, device: 'Chrome"><img src=x onerror="window.__pwn++">' } }
        ];
        if (typeof applyCloudUsersSnapshot === 'function') applyCloudUsersSnapshot(users);
        else { allUsers = users; renderUsersView(); }
        await tick(80);
        t('Users: nama perangkat sesi tidak menjadi markup', [document.querySelectorAll('#viewUsers img').length, window.__pwn], [0, 0]);
        let gotUid = null;
        window.approveUser = uid => { gotUid = uid; };
        const apBtn = document.querySelector('button[title="Approve with the selected role"]');
        if (apBtn) apBtn.click();
        t('Users: uid berkutip diteruskan persis, tidak berjalan', [gotUid, window.__pwn], [apBtn ? "p1');window.__pwn++;//" : null, 0]);

        await tick(100);
        t('TOTAL: tidak ada satu muatan pun yang berjalan', window.__pwn, 0);
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
