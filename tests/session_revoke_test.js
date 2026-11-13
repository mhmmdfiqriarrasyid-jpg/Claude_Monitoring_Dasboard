// v149: (1) dikeluarkan owner lalu masuk lagi — klaim sesi yang ditolak tidak
// lagi dibaca sebagai "diambil alih", dan pesannya tidak menyuruh publish
// rules bila rules jelas sudah berlaku; (2) pemeriksa tidak boleh memeriksa
// laporan yang ia tulis ATAU kirim sendiri; laporan lama tanpa penulis tetap
// bisa diperiksa.
const { launch, BASE_URL } = require('./_env');

(async () => {
    const b = await launch();
    const page = await b.newPage({ viewport: { width: 1280, height: 900 } });
    const errors = [];
    page.on('pageerror', e => { if (!/Chart is not defined/.test(e.message)) errors.push(e.message); });
    await page.goto(BASE_URL + '/index.html', { waitUntil: 'load' });
    await page.waitForTimeout(1000);

    await page.addScriptTag({ content: `(async () => { try {
        const T = []; const t = (n, g, w) => T.push({ n, g, w, pass: JSON.stringify(g) === JSON.stringify(w) });
        const tick = ms => new Promise(r => setTimeout(r, ms || 20));
        const toasts = [];
        window.showToast = m => toasts.push(String(m));
        let docCb = null, takeovers = 0, claimFail = null;
        window.handleSessionTakenOver = () => { takeovers++; };
        window.cloud = { isReady: true,
            claimSession: async () => { if (claimFail) throw Object.assign(new Error('x'), { code: claimFail }); },
            subscribeUserDoc: (uid, cb) => { docCb = cb; return () => { docCb = null; }; } };
        const as = (uid, doc) => { currentUser = { uid, email: uid + '@x.id' }; currentUserDoc = { status: 'active', email: uid + '@x.id', ...doc }; applyRoleGating(); };

        // =============== klaim sesi ditolak ===============
        as('u1', { role: 'staff', access: { teamLog: 'edit' } });
        claimFail = 'permission-denied';
        await claimActiveSession('u1', { id: 'revoked_77', startedAt: 1, device: 'Signed out by owner@x.id' });
        t('klaim ditolak setelah dikeluarkan: bukan owner disuruh minta Super Admin publish', /ask the Super Admin to re-publish/.test(toasts[toasts.length - 1]), true);
        watchOwnUserDoc('u1');
        docCb({ status: 'active', role: 'staff', activeSession: { id: 'revoked_77', startedAt: 1, device: 'x' } });
        t('id lama yang tersisa setelah klaim ditolak TIDAK dianggap pengambilalihan', takeovers, 0);
        docCb({ status: 'active', role: 'staff', activeSession: { id: 's_other_device', startedAt: 9, device: 'Laptop' } });
        t('perangkat lain yang benar-benar masuk tetap dianggap pengambilalihan', takeovers, 1);
        stopWatchingOwnUserDoc();

        toasts.length = 0;
        await claimActiveSession('u1', { id: 's_live_1', startedAt: 1, device: 'Laptop' });
        t('rules terbukti berlaku (id hidup): pesan tidak menyalahkan rules', [/re-publish/.test(toasts[0]), /could not take over/.test(toasts[0])], [false, true]);
        as('own', { role: 'owner' });
        toasts.length = 0;
        await claimActiveSession('own', null);
        t('owner tanpa bukti klaim: disuruh publish rules', /re-publish firestore\\.rules/.test(toasts[0]), true);
        claimFail = null; toasts.length = 0;
        await claimActiveSession('own', null);
        t('klaim berhasil: tanpa pesan', toasts.length, 0);

        // =============== pemeriksa vs penulis / pengirim ===============
        as('boss', { role: 'staff', access: { teamLog: 'edit', teamLogApprove: 'edit' } });
        const W = x => ({ id: 'w', approval: 'pending', ...x });
        t('laporan rekan: boleh diperiksa', canApproveThisLog(W({ createdByUid: 'u1', submittedByUid: 'u1' })), true);
        t('laporan yang ia tulis: DITOLAK', canApproveThisLog(W({ createdByUid: 'boss', submittedByUid: 'boss' })), false);
        t('laporan rekan yang ia kirim ulang: DITOLAK', canApproveThisLog(W({ createdByUid: 'u1', submittedByUid: 'boss' })), false);
        t('laporan lama tanpa penulis: tetap boleh', canApproveThisLog(W({})), true);
        t('pesan penolakan menyebut sebabnya', [selfCheckMessage(W({ createdByUid: 'u1', submittedByUid: 'boss' }), 'report', 'approve'),
            selfCheckMessage(W({ createdByUid: 'boss' }), 'report', 'review')],
          ['You submitted this report yourself — someone else must approve it', 'You filed this report yourself — someone else must review it']);
        as('own', { role: 'owner' });
        t('owner: boleh memeriksa laporannya sendiri', canApproveThisLog(W({ createdByUid: 'own', submittedByUid: 'own' })), true);

        // pengirim distempel saat dikirim, draf mempertahankan nilai lama
        as('boss', { role: 'staff', access: { teamLog: 'edit', teamLogApprove: 'edit' } });
        t('dikirim: pengirim = akun ini', submittedByFor({ submittedByUid: 'u1' }, false), 'boss');
        t('draf: pengirim lama dipertahankan', submittedByFor({ submittedByUid: 'u1' }, true), 'u1');
        t('draf baru: kosong', submittedByFor(null, true), '');

        // tombol Approve di halaman: approveWorkLog menolak laporan yang ia kirim
        workLogs = [{ id: 'wl1', date: '2026-10-01', memberId: 'm1', memberName: 'A', approval: 'pending', createdByUid: 'u1', submittedByUid: 'boss', task: 'x' }];
        toasts.length = 0;
        const sent = [];
        window.cloud.saveWorkLog = r => { sent.push(r); return Promise.resolve(); };
        approveWorkLog('wl1');
        t('approveWorkLog: laporan yang ia kirim sendiri ditolak, tidak dikirim', [sent.length, toasts[toasts.length - 1]],
          [0, 'You submitted this report yourself — someone else must approve it']);

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
