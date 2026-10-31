// v150 — pemakaian di HP / lapangan:
//   1. Tombol Back menutup form (dengan peringatan foto) atau kembali ke
//      halaman sebelumnya, bukan menutup aplikasi.
//   2. Simpan yang ditolak server: isian disimpan, pemberitahuan tetap tampil,
//      "Open it again" membuka form berisi data & foto yang sama.
//   3. "Waiting to send" untuk laporan harian, izin, kerusakan; hitungan di
//      bar atas; sign out menyebut semuanya.
//   4. Sesi 24 jam: peringatan 30 menit sebelumnya; tidak keluar di tengah
//      form atau tanpa sinyal.
//   5. Semua kolom isian di HP minimal 16px (iPhone tidak zoom).
const { launch, BASE_URL } = require('./_env');

(async () => {
    const b = await launch();
    const page = await b.newPage({ viewport: { width: 390, height: 844 } });
    const errors = [];
    page.on('pageerror', e => { if (!/Chart is not defined/.test(e.message)) errors.push(e.message); });
    await page.goto(BASE_URL + '/index.html', { waitUntil: 'load' });
    await page.waitForTimeout(1000);

    await page.addScriptTag({ content: `(async () => { try {
        const T = []; const t = (n, g, w) => T.push({ n, g, w, pass: JSON.stringify(g) === JSON.stringify(w) });
        const tick = ms => new Promise(r => setTimeout(r, ms || 30));
        const back = async () => { history.back(); await tick(120); };
        const toasts = [];
        window.showToast = m => toasts.push(String(m));
        let confirmAnswer = true, confirmText = '';
        window.confirm = m => { confirmText = String(m); return confirmAnswer; };
        let online = true;
        Object.defineProperty(navigator, 'onLine', { configurable: true, get: () => online });
        let signedOut = 0, failNext = null;
        const reject = code => Promise.reject(Object.assign(new Error('x'), { code }));
        window.cloud = { isReady: true, addHistoryEvents: () => Promise.resolve(), cacheMode: 'persistent-multitab',
            signOutUser: async () => { signedOut++; },
            saveWorkLog: () => failNext ? reject(failNext) : Promise.resolve(),
            saveWorkLogPhotos: () => Promise.resolve(), deleteWorkLogPhotos: () => Promise.resolve(),
            saveLeaveRequest: () => failNext ? reject(failNext) : Promise.resolve(),
            saveInspection: () => failNext ? reject(failNext) : Promise.resolve(),
            saveInspectionPhotos: () => Promise.resolve(), deleteInspectionPhotos: () => Promise.resolve(),
            getInspectionPhotos: () => Promise.resolve({}),
            saveDamage: () => failNext ? reject(failNext) : Promise.resolve(), saveDamagePhoto: () => Promise.resolve(),
            getAllDamages: () => Promise.resolve([]), saveUnits: () => Promise.resolve() };
        const as = (uid, doc) => { currentUser = { uid, email: uid + '@x.id' }; currentUserDoc = { status: 'active', email: uid + '@x.id', displayName: uid, ...doc }; applyRoleGating(); };
        as('lead', { role: 'staff', access: { teamLog: 'edit', damage: 'edit', inspection: 'edit', editUnits: 'edit' } });
        hideAuthGates();
        teamMembers = [{ id: 'm1', name: 'Andi', company: 'PT. GPA', active: true }];
        globalData = [{ id: 'u1', name: 'TR1', sn: 'S1', status: 'Good', site: 'A', display: 'Good', gps: 'Good', steering: 'Good', jdlink: 'Good' },
                      { id: 'h1', name: 'EX1', sn: 'H1', unitGroup: 'heavy', status: 'Good', cameraAi: 'Good', telematicBox: 'Good', switchLimiter: 'Good', rotaryLamp: 'Good' }];
        saveToStorage(globalData);
        workLogs = []; leaveRequests = []; inspections = []; inspectionPlans = []; globalDamages = []; saveDamages();
        const isOpen = id => document.getElementById(id).classList.contains('open');

        // =============== 1. tombol Back ===============
        navigateTo('team');
        await tick();
        navigateTo('damage');
        await tick();
        await back();
        t('Back di halaman: kembali ke halaman sebelumnya, aplikasi tidak tertutup', currentView, 'team');
        showAddWorkLogForm();
        await tick(80);
        t('form terbuka mendapat entri riwayat', history.state && history.state.ot, 'modal');
        await back();
        t('Back menutup form', isOpen('workLogModal'), false);
        t('dan halaman tetap sama', currentView, 'team');
        showAddWorkLogForm();
        await tick(80);
        _wlPhotos = ['data:image/jpeg;base64,QUJD']; _wlPhotosDirty = true;
        confirmAnswer = false;
        await back();
        t('Back dengan foto belum disimpan: bertanya; batal = form tetap terbuka', [isOpen('workLogModal'), /photo\\(s\\) not saved yet/.test(confirmText)], [true, true]);
        t('entri riwayat form dikembalikan (Back berikutnya bertanya lagi)', history.state && history.state.ot, 'modal');
        confirmAnswer = true;
        await back();
        t('dikonfirmasi: form tertutup', isOpen('workLogModal'), false);
        showAddWorkLogForm();
        await tick(80);
        closeWorkLogModal(true);
        await tick(150);
        t('ditutup lewat tombol: entri riwayatnya ikut hilang', history.state && history.state.ot, 'view');
        // buka-tutup cepat berturut-turut: riwayat tidak boleh mundur keluar aplikasi
        const here = location.href;
        for (let i = 0; i < 4; i++) {
            showAddWorkLogForm(); await tick(5); closeWorkLogModal(true); await tick(5);
            showAddLeaveForm(); await tick(5); closeLeaveModal(true);
        }
        await tick(400);
        t('buka-tutup cepat: tetap di aplikasi', [location.href === here, typeof currentView], [true, 'string']);
        navigateTo('damage');
        showAddDamageForm();
        _dmgPhotoData = 'data:image/jpeg;base64,QUJD'; _dmgPhotoDirty = true;
        confirmAnswer = false; confirmText = '';
        closeDamageModal();
        t('form kerusakan: foto baru tidak dibuang diam-diam', [isOpen('damageModal'), /photo is not saved yet/.test(confirmText)], [true, true]);
        confirmAnswer = true;
        closeDamageModal();
        await tick(150);

        // =============== 2. simpan ditolak server ===============
        navigateTo('team');
        showAddWorkLogForm();
        document.getElementById('wlMember').value = 'm1';
        document.getElementById('wlStart').value = '07:00'; document.getElementById('wlEnd').value = '15:00';
        document.getElementById('wlTask').value = 'Kalibrasi GPS di blok 7';
        _wlJobs = [{ paddock: 'P-7', jobType: 'Tillage', units: [{ id: 'u_x', name: 'TR1', sn: '' }] }];
        _wlPhotos = ['data:image/jpeg;base64,UEhPVE8='];
        _wlPhotosDirty = true;
        failNext = 'permission-denied';
        saveWorkLog(new Event('submit'));
        await tick(80);
        const notice = () => (document.getElementById('unsentNotice') || {}).textContent || '';
        t('ditolak: form tertutup tapi pemberitahuan tetap tampil', [isOpen('workLogModal'), /Not saved: daily report — Andi/.test(notice())], [false, true]);
        t('pesannya bahasa biasa (bukan bukan-owner disuruh publish rules)', /firestore\\.rules/.test(notice()), false);
        failNext = null;
        document.querySelector('#unsentNotice .btn-primary').click();
        await tick(80);
        t('Open it again: form berisi isian dan foto yang sama',
          [isOpen('workLogModal'), document.getElementById('wlTask').value, _wlPhotos.length, _wlPhotosDirty, document.getElementById('wlMember').value, _wlJobs[0].paddock],
          [true, 'Kalibrasi GPS di blok 7', 1, true, 'm1', 'P-7']);
        t('pemberitahuan hilang setelah dibuka', !!document.getElementById('unsentNotice'), false);
        saveWorkLog(new Event('submit'));
        await tick(80);
        t('dikirim ulang: berhasil, tanpa pemberitahuan', [isOpen('workLogModal'), !!document.getElementById('unsentNotice')], [false, false]);

        // kerusakan
        navigateTo('damage');
        showAddDamageForm();
        document.getElementById('dmgUnit').value = 'TR1';
        document.getElementById('dmgDate').value = '2026-10-12';
        document.getElementById('dmgType').value = 'Mekanis';
        document.getElementById('dmgDescription').value = 'Hidrolik bocor';
        failNext = 'permission-denied';
        saveDamage(new Event('submit'));
        await tick(120);
        t('kerusakan ditolak: pemberitahuan', /Not saved: damage record — TR1/.test(notice()), true);
        failNext = null;
        document.querySelector('#unsentNotice .btn-primary').click();
        await tick(80);
        t('kerusakan dibuka lagi dengan deskripsinya', [isOpen('damageModal'), document.getElementById('dmgDescription').value], [true, 'Hidrolik bocor']);
        closeDamageModal(true);
        // inspeksi
        navigateTo('inspection');
        showInspectionForm('h1');
        document.getElementById('insDate').value = '2026-10-12';
        inspectionComponents().forEach(c => { _insResults[c.key] = 'good'; _insPhotos[c.key] = 'data:image/jpeg;base64,SU5T'; });
        _insPhotosDirty = true;
        document.getElementById('insNote').value = 'Semua normal';
        failNext = 'permission-denied';
        saveInspection(new Event('submit'));
        await tick(120);
        t('inspeksi ditolak: pemberitahuan', /Not saved: inspection report — EX1/.test(notice()), true);
        failNext = null;
        document.querySelector('#unsentNotice .btn-primary').click();
        await tick(80);
        t('inspeksi dibuka lagi: hasil, foto dan catatan utuh',
          [isOpen('inspectionModal'), Object.keys(_insPhotos).length, Object.values(_insResults).every(v => v === 'good'), document.getElementById('insNote').value],
          [true, 4, true, 'Semua normal']);
        closeInspectionModal(true);
        await tick(80);
        // buang
        rememberUnsent('test entry', { code: 'permission-denied' }, () => {});
        confirmAnswer = false; dismissUnsent([..._unsent.keys()][0]);
        t('Discard minta konfirmasi; batal = tetap ada', _unsent.size, 1);
        confirmAnswer = true; dismissUnsent([..._unsent.keys()][0]);
        t('Discard dikonfirmasi: hilang', [_unsent.size, !!document.getElementById('unsentNotice')], [0, false]);
        as('own', { role: 'owner' });
        t('owner mendapat petunjuk rules', /re-publish firestore\\.rules/.test(plainSaveError({ code: 'permission-denied' })), true);
        as('lead', { role: 'staff', access: { teamLog: 'edit', damage: 'edit', inspection: 'edit', editUnits: 'edit' } });

        // =============== 3. waiting to send ===============
        workLogs = [{ id: 'w1', date: '2026-10-12', memberId: 'm1', memberName: 'Andi', task: 'x', approval: 'pending', createdByUid: 'lead' }];
        navigateTo('team'); switchTeamTab('worklog');
        setQueued('workLogs', ['w1']);
        setQueued('damages', ['d9']);
        renderWorkLogTable();
        const pill = () => { const p = document.getElementById('queuedPill'); return p && p.style.display !== 'none' ? p.textContent.trim() : ''; };
        t('bar atas: jumlah yang menunggu', pill(), '2 waiting to send');
        t('baris laporan bertanda Waiting to send', !!document.querySelector('#workLogBody .q-tag, tbody .q-tag'), true);
        confirmAnswer = false; confirmText = '';
        await handleSignOut();
        t('sign out menyebut semua yang belum terkirim', [/Not sent yet: 1 daily report, 1 damage record/.test(confirmText), signedOut], [true, 0]);
        toasts.length = 0;
        setQueued('workLogs', []); setQueued('damages', []);
        t('semua terkirim: hitungan hilang dan diberi tahu', [pill(), toasts.includes('Everything saved offline has now been sent')], ['', true]);

        // =============== 4. sesi 24 jam ===============
        confirmAnswer = true; signedOut = 0;
        localStorage.setItem('tractorSessionStart', String(Date.now() - SESSION_MAX_MS + 10 * 60 * 1000));
        checkDailySession();
        const banner = () => (document.getElementById('sessionBanner') || {}).textContent || '';
        t('30 menit terakhir: peringatan dengan jamnya', /Your daily session ends at \\d\\d:\\d\\d/.test(banner()), true);
        snoozeSessionBanner();
        checkDailySession();
        t('Later: peringatan tidak muncul lagi untuk sesi ini', banner(), '');
        localStorage.setItem('tractorSessionStart', String(Date.now() - SESSION_MAX_MS - 1000));
        showAddWorkLogForm();
        await tick(50);
        t('habis saat form terbuka: TIDAK keluar', [checkDailySession(), signedOut, /has ended/.test(banner())], [true, 0, true]);
        closeWorkLogModal(true);
        await tick(5300);
        t('form ditutup: lalu keluar', signedOut, 1);
        clearSessionClock();
        localStorage.setItem('tractorSessionStart', String(Date.now() - SESSION_MAX_MS - 1000));
        online = false; signedOut = 0;
        t('habis tanpa sinyal: tetap bisa bekerja', [checkDailySession(), signedOut, /once you have signal/.test(banner())], [true, 0, true]);
        online = true;
        await tick(5300);
        t('sinyal kembali: baru keluar', signedOut, 1);
        clearSessionClock();
        localStorage.setItem('tractorSessionStart', String(Date.now()));
        checkDailySession();
        t('sesi baru: tanpa spanduk', banner(), '');

        // =============== 5. iPhone: semua kolom >= 16px ===============
        const small = [];
        const scan = where => document.querySelectorAll('input:not([type=hidden]):not([type=checkbox]):not([type=radio]):not([type=file]), select, textarea, [contenteditable="true"]').forEach(el => {
            const r = el.getBoundingClientRect(); if (!r.width || !r.height) return;
            const px = parseFloat(getComputedStyle(el).fontSize); if (px < 16) small.push(where + ':' + (el.id || el.className) + '@' + px);
        });
        as('own', { role: 'owner' });
        for (const [v, tab] of [['dashboard'], ['editUnits'], ['damage'], ['licenseStock'], ['team', 'shift'], ['team', 'worklog'], ['team', 'leave'], ['warehouse'], ['inspection']]) {
            navigateTo(v); if (tab) switchTeamTab(tab); await tick(80); scan(v + (tab ? '/' + tab : ''));
        }
        t('tidak ada kolom isian di bawah 16px di halaman mana pun', [...new Set(small)], []);

        window.__T = T;
    } catch (e) { window.__T = [{ n: 'EXCEPTION ' + e.message + ' ' + e.stack, pass: false }]; } })();` });

    await page.waitForFunction(() => window.__T, null, { timeout: 120000 });
    const T = await page.evaluate(() => window.__T);
    let pass = 0;
    T.forEach(x => { if (x.pass) pass++; else console.log('FAIL', x.n, '\n  dapat :', JSON.stringify(x.g), '\n  harap :', JSON.stringify(x.w)); });
    if (errors.length) console.log('PAGE ERRORS:', errors);
    console.log(`${pass}/${T.length} lulus`);
    await b.close();
    process.exit(pass === T.length && !errors.length ? 0 : 1);
})();
