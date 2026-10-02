// Alur pengiriman laporan harian dan izin/sakit:
//   Draf → Kirim → Menunggu → Disetujui, atau kembali sebagai Perlu Revisi.
// Menunggu dan Disetujui terkunci bagi pengirim; pengirim boleh menarik
// kembali yang masih menunggu; atasan bisa membuka kembali yang sudah
// disetujui lewat Minta Revisi. Pengirim diberi tahu di dalam aplikasi.
const { launch, BASE_URL } = require('./_env');

(async () => {
    const b = await launch();
    const page = await b.newPage({ viewport: { width: 1440, height: 1000 } });
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
        window.cloud = { isReady: true,
            saveShift: ok, deleteShift: ok, saveTeamMember: ok, deleteTeamMember: ok, addHistoryEvents: ok,
            saveWorkLog: r => { calls.push(['log', JSON.parse(JSON.stringify(r))]); return ok(); },
            deleteWorkLog: id => { calls.push(['delLog', id]); return ok(); },
            saveWorkLogPhotos: (id, p) => { calls.push(['photos', id]); return ok(); },
            deleteWorkLogPhotos: id => { calls.push(['delPhotos', id]); return ok(); },
            getWorkLogPhotos: () => Promise.resolve([]),
            saveLeaveRequest: r => { calls.push(['leave', JSON.parse(JSON.stringify(r))]); return ok(); },
            deleteLeaveRequest: id => { calls.push(['delLeave', id]); return ok(); },
            saveTeamDocs: (id) => { calls.push(['docs', id]); return ok(); },
            deleteTeamDocs: (id) => { calls.push(['delDocs', id]); return ok(); },
            getTeamDocs: () => Promise.resolve([]) };
        const toasts = [];
        const realToast = window.showToast;
        window.showToast = (m, k) => { toasts.push(String(m)); };
        window.confirm = () => true;
        const last = kind => { const c = calls.filter(x => x[0] === kind); return c.length ? c[c.length - 1][1] : null; };
        const as = (uid, doc) => { currentUser = { uid, email: uid + '@x.id' }; currentUserDoc = { status: 'active', email: uid + '@x.id', displayName: uid, ...doc }; applyRoleGating(); };
        const PEN = { role: 'khl', access: { teamLog: 'edit' } };
        const BOSS = { role: 'staff', access: { teamLog: 'view', teamLogApprove: 'edit' } };
        try { localStorage.clear(); } catch (_) {}

        as('ani', PEN);
        hideAuthGates();
        globalData = [{ id: 'u1', name: 'JD-01', sn: 'SN1' }];
        teamMembers = [{ id: 'm1', name: 'Ani', company: 'PT. Global Papua Abadi', active: true }];
        workLogs = []; leaveRequests = []; teamShifts = [];
        navigateTo('team'); switchTeamTab('worklog');
        workLogs = []; leaveRequests = [];

        // =============== Draf ===============
        showAddWorkLogForm();
        t('form punya tombol Simpan Draf dan Kirim', [!!document.getElementById('wlDraftBtn'), !!document.getElementById('wlSubmitBtn')], [true, true]);
        document.getElementById('wlMember').value = 'm1';
        document.getElementById('wlDate').value = toISODate();
        document.getElementById('wlTask').value = '';
        saveWorkLog(null, 'draft');
        let d = last('log');
        t('draf boleh tanpa uraian', !!d, true);
        t('draf tersimpan sebagai draft', d && d.approval, 'draft');
        t('draf mencatat pembuatnya', d && d.createdByUid, 'ani');
        t('draf belum punya waktu kirim', d && d.submittedAt, 0);
        workLogs = [d];

        showAddWorkLogForm();
        document.getElementById('wlMember').value = 'm1';
        document.getElementById('wlTask').value = '';
        const n0 = calls.length;
        saveWorkLog({ preventDefault() {} });
        t('Kirim tanpa uraian ditolak', calls.length, n0);

        // Draf orang lain tidak terlihat; draf sendiri terlihat.
        renderWorkLogTable();
        t('draf sendiri terlihat di tabel', document.querySelectorAll('#workLogBody .appr--draft').length, 1);
        t('draf tidak dihitung sudah melapor hari ini', document.getElementById('wlKpiToday').textContent.startsWith('0'), true);
        t('draf tidak masuk rekap perusahaan', companyRecap(toISODate().slice(0, 7)).length, 0);
        as('boss', BOSS);
        renderWorkLogTable();
        t('draf orang lain tidak terlihat oleh atasan', document.querySelectorAll('#workLogBody .appr--draft').length, 0);
        approveWorkLog(d.id);
        t('atasan tidak bisa menyetujui draf', last('log').approval, 'draft');
        as('ani', PEN);

        // Kirim draf.
        editWorkLog(d.id);
        t('draf bisa dibuka untuk diedit', document.getElementById('workLogModal').classList.contains('open'), true);
        document.getElementById('wlTask').value = 'Servis JD-01';
        saveWorkLog({ preventDefault() {} });
        let p = last('log');
        t('Kirim → pending', p.approval, 'pending');
        t('Kirim mencatat waktu kirim', p.submittedAt > 0, true);
        await tick();
        t('Kirim: pesan menunggu persetujuan', toasts.some(m => /menunggu persetujuan/.test(m)), true);
        workLogs = [p];

        // =============== Terkunci ===============
        renderWorkLogTable();
        let row = document.querySelector('#workLogBody tr');
        t('menunggu: tidak ada tombol edit', !!row.querySelector('button[title="Edit"]'), false);
        t('menunggu: ada gembok', !!row.querySelector('.row-lock'), true);
        t('menunggu: pengirim bisa tarik kembali', !!row.querySelector('button[title="Withdraw to draft"]'), true);
        toasts.length = 0;
        editWorkLog(p.id);
        t('menunggu: edit ditolak dengan alasan', [document.getElementById('workLogModal').classList.contains('open'), toasts.some(m => /tarik kembali/.test(m))], [false, true]);
        const n1 = calls.length;
        deleteWorkLog(p.id);
        t('menunggu: hapus ditolak', calls.length, n1);

        // Anggota lain tidak bisa menarik kembali.
        as('budi', PEN);
        renderWorkLogTable();
        row = document.querySelector('#workLogBody tr');
        t('orang lain: tidak ada tombol tarik kembali', !!row.querySelector('button[title="Withdraw to draft"]'), false);
        withdrawWorkLog(p.id);
        t('orang lain: tarik kembali ditolak', calls.length, n1);
        as('ani', PEN);

        // =============== Tarik kembali ===============
        withdrawWorkLog(p.id);
        let w = last('log');
        t('tarik kembali → draft', w.approval, 'draft');
        t('tarik kembali tidak mengubah isi', [w.task, w.createdByUid, w.submittedAt], [p.task, p.createdByUid, p.submittedAt]);
        workLogs = [{ ...p }];

        // =============== Atasan ===============
        as('boss', BOSS);
        renderWorkLogTable();
        row = document.querySelector('#workLogBody tr');
        t('atasan: tombol setujui + revisi pada yang menunggu', row.querySelectorAll('.appr-btn').length, 2);
        approveWorkLog(p.id);
        let a = last('log');
        t('atasan: disetujui', a.approval, 'approved');
        workLogs = [a];
        renderWorkLogTable();
        row = document.querySelector('#workLogBody tr');
        t('disetujui: hanya tombol buka kembali', [row.querySelectorAll('.appr-btn').length, row.querySelector('.appr-btn').title], [1, 'Reopen — request revision']);
        approveWorkLog(p.id);
        t('disetujui: tidak disetujui dua kali', last('log').approvedAt, a.approvedAt);

        // Pengirim: yang disetujui terkunci, tanpa tarik kembali.
        as('ani', PEN);
        renderWorkLogTable();
        row = document.querySelector('#workLogBody tr');
        t('disetujui: pengirim hanya melihat gembok', [!!row.querySelector('button[title="Edit"]'), !!row.querySelector('button[title="Withdraw to draft"]'), !!row.querySelector('.row-lock')], [false, false, true]);
        toasts.length = 0;
        editWorkLog(p.id);
        t('disetujui: edit ditolak, disuruh minta atasan', toasts.some(m => /Minta Revisi/.test(m)), true);

        // Atasan membuka kembali.
        as('boss', BOSS);
        window.prompt = () => 'Paddock salah <img src=x onerror="window.__pwn=1">';
        reviseWorkLog(p.id);
        let r = last('log');
        t('buka kembali: approved → revision', r.approval, 'revision');
        t('buka kembali: stempel persetujuan dihapus', [r.approvedBy, r.approvedAt], ['', 0]);
        workLogs = [r];

        // =============== Notifikasi untuk pengirim ===============
        as('ani', PEN);
        _markLoaded('workLogs'); _markLoaded('leaveRequests');
        toasts.length = 0;
        updateMyTeamNotices();
        const badge = document.getElementById('teamBadge');
        t('badge Tim menghitung yang perlu direvisi', [badge.style.display, badge.textContent], ['', '1']);
        t('badge tab Laporan Harian', document.getElementById('wlTabBadge').textContent, '1');
        t('saat dibuka: toast perlu direvisi', toasts.some(m => /perlu direvisi/.test(m)), true);
        renderTeamView();
        const box = document.getElementById('wlMyNotice');
        t('kotak Perlu direvisi tampil', box.style.display, '');
        t('kotak memuat catatan atasan apa adanya', box.textContent.includes('Paddock salah <img'), true);
        t('catatan tidak menjadi markup', [box.querySelectorAll('img').length, window.__pwn || 0], [0, 0]);
        t('kotak punya tombol Perbaiki', !!box.querySelector('button.btn-primary'), true);
        toasts.length = 0;
        updateMyTeamNotices();
        t('toast tidak diulang pada pembaruan berikutnya', toasts.length, 0);

        // Revisi di form: catatan tampil, draf menyimpan catatannya.
        box.querySelector('button.btn-primary').click();
        t('Perbaiki membuka form', document.getElementById('workLogModal').classList.contains('open'), true);
        t('judul form: Revisi', document.getElementById('workLogModalTitle').textContent, 'Revise Daily Report');
        const rn = document.getElementById('wlRevisionNote');
        t('catatan revisi tampil di form', [rn.style.display, rn.textContent.includes('Paddock salah'), rn.querySelectorAll('img').length], ['', true, 0]);
        saveWorkLog(null, 'draft');
        t('draf dari revisi menyimpan catatannya', [last('log').approval, last('log').revisionNote.startsWith('Paddock salah')], ['draft', true]);
        workLogs = [last('log')];
        editWorkLog(p.id);
        saveWorkLog({ preventDefault() {} });
        t('kirim ulang membersihkan catatan', [last('log').approval, last('log').revisionNote], ['pending', '']);
        workLogs = [last('log')];
        updateMyTeamNotices();
        t('badge hilang setelah dikirim ulang', badge.style.display, 'none');

        // Persetujuan baru diberitahukan sekali.
        toasts.length = 0;
        workLogs = [{ ...workLogs[0], approval: 'approved', approvedBy: 'Boss', approvedAt: Date.now() + 5000 }];
        updateMyTeamNotices();
        t('toast disetujui', toasts.some(m => /disetujui oleh Boss/.test(m)), true);
        toasts.length = 0;
        updateMyTeamNotices();
        t('toast disetujui tidak diulang', toasts.length, 0);

        // Revisi yang datang saat aplikasi terbuka.
        toasts.length = 0;
        workLogs = [{ ...workLogs[0], approval: 'revision', revisionNote: 'Foto kurang', approvedAt: 0 }];
        updateMyTeamNotices();
        t('revisi baru: toast dengan catatannya', toasts.some(m => /diminta revisi: Foto kurang/.test(m)), true);

        // Filter Milik saya.
        workLogs.push({ id: 'wl_lain', date: toISODate(), memberId: 'm1', task: 'punya budi', approval: 'pending', createdByUid: 'budi' });
        renderWorkLogTable();
        t('tanpa filter: dua baris', document.querySelectorAll('#workLogBody tr').length, 2);
        document.getElementById('wlMineFilter').checked = true;
        renderWorkLogTable();
        t('Milik saya: satu baris', document.querySelectorAll('#workLogBody tr').length, 1);
        clearWorkLogFilter();
        t('Reset mematikan Milik saya', document.getElementById('wlMineFilter').checked, false);

        // =============== Foto ditulis sebelum laporannya ===============
        workLogs = [];
        showAddWorkLogForm();
        document.getElementById('wlMember').value = 'm1';
        document.getElementById('wlTask').value = 'Dengan foto';
        _wlPhotos = ['data:image/jpeg;base64,QUJD']; _wlPhotosDirty = true;
        const start = calls.length;
        saveWorkLog({ preventDefault() {} });
        const seq = calls.slice(start).map(c => c[0]);
        t('foto dikirim sebelum laporan (rules mengunci foto laporan terkirim)', seq, ['photos', 'log']);

        // =============== Izin / sakit ===============
        switchTeamTab('leave');
        showAddLeaveForm();
        document.getElementById('lvMember').value = 'm1';
        document.getElementById('lvDateFrom').value = '2026-12-01';
        saveLeave(null, 'draft');
        let lv = last('leave');
        t('izin: draf', lv.approval, 'draft');
        leaveRequests = [lv];
        t('izin draf tidak menandai jadwal', membersOnLeave('2026-12-01').size, 0);
        editLeave(lv.id);
        saveLeave(null);
        lv = last('leave');
        t('izin: kirim → pending', lv.approval, 'pending');
        leaveRequests = [lv];
        renderLeaveTable();
        row = document.querySelector('#leaveBody tr');
        t('izin menunggu: gembok + tarik kembali', [!!row.querySelector('button[title="Edit"]'), !!row.querySelector('.row-lock'), !!row.querySelector('button[title="Withdraw to draft"]')], [false, true, true]);
        withdrawLeave(lv.id);
        t('izin: tarik kembali → draft', last('leave').approval, 'draft');
        as('boss', BOSS);
        approveLeave(lv.id);
        let la = last('leave');
        t('izin: disetujui', la.approval, 'approved');
        leaveRequests = [la];
        as('ani', PEN);
        const n2 = calls.length;
        editLeave(lv.id); deleteLeave(lv.id);
        t('izin disetujui: edit dan hapus ditolak', calls.length, n2);
        as('boss', BOSS);
        window.prompt = () => 'Surat dokter belum ada';
        reviseLeave(lv.id);
        leaveRequests = [last('leave')];
        as('ani', PEN);
        updateMyTeamNotices();
        t('izin: badge tab Izin', document.getElementById('lvTabBadge').textContent, '1');
        t('izin: kotak Perlu direvisi', document.getElementById('lvMyNotice').textContent.includes('Surat dokter belum ada'), true);

        // =============== Owner tetap bebas ===============
        as('owner', { role: 'owner' });
        leaveRequests = [la];
        t('owner boleh mengedit yang disetujui', canEditTeamRecord(la), true);

        window.showToast = realToast;
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
