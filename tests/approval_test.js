const { launch, BASE_URL } = require('./_env');

(async () => {
    const b = await launch();
    const page = await b.newPage({ viewport: { width: 1440, height: 1000 } });
    const errors = [];
    page.on('pageerror', e => { if (!/Chart is not defined/.test(e.message)) errors.push(e.message); });
    await page.goto(BASE_URL + '/index.html', { waitUntil: 'load' });
    await page.waitForTimeout(1000);

    await page.addScriptTag({ content: `(() => { try {
        const T = []; const t = (n, g, w) => T.push({ n, g, w, pass: JSON.stringify(g) === JSON.stringify(w) });
        const saved = [];
        window.cloud = { isReady: true,
            saveShift:()=>Promise.resolve(), deleteShift:()=>Promise.resolve(),
            saveTeamMember:()=>Promise.resolve(), deleteTeamMember:()=>Promise.resolve(),
            saveWorkLog: r => { saved.push(r); return Promise.resolve(); },
            deleteWorkLog:()=>Promise.resolve() };
        currentUser = { uid:'staff1', email:'staff@x.id' };
        currentUserDoc = { role:'staff', status:'active', email:'staff@x.id',
                           access:{ teamLog:'edit', teamLogApprove:'edit', teamShift:'view', teamMembers:'view' } };
        hideAuthGates(); applyRoleGating();
        globalData = [{ id:'u_1', name:'JD-01', sn:'SN1' }];
        teamMembers = [{ id:'m1', name:'Andi', jobTitle:'Mekanik', active:true }];
        teamShifts = [];
        workLogs = [
            // Belum pernah ada kolom approval — harus terbaca sebagai Menunggu.
            { id:'wLama', date:'2026-09-10', memberId:'m1', memberName:'Andi', start:'08:00', end:'16:00', task:'Laporan lama' },
            { id:'wPunyaOrangLain', date:'2026-09-14', memberId:'m1', memberName:'Andi', start:'08:00', end:'16:00',
              task:'Dibuat KHL', createdByUid:'khl9', createdByEmail:'khl@x.id', approval:'pending' },
            { id:'wPunyaSendiri', date:'2026-09-15', memberId:'m1', memberName:'Andi', start:'08:00', end:'16:00',
              task:'Dibuat sendiri', createdByUid:'staff1', createdByEmail:'staff@x.id', approval:'pending' },
            { id:'wSudah', date:'2026-09-13', memberId:'m1', memberName:'Andi', start:'08:00', end:'12:00',
              task:'Sudah disetujui', createdByUid:'khl9', approval:'approved', approvedBy:'Boss', approvedAt: 1757800000000 }
        ];
        teamWeekStart = '2026-09-14';
        navigateTo('team'); switchTeamTab('worklog');

        // ---------- area akses ----------
        t("area 'teamLogApprove' terdaftar", ACCESS_AREAS.some(a=>a.key==='teamLogApprove'), true);
        t('hanya dua level (tidak ada "lihat")',
          ACCESS_AREAS.find(a=>a.key==='teamLogApprove').levels, ['none','edit']);

        // ---------- status bawaan ----------
        t('laporan lama tanpa field = Menunggu', workLogApproval(workLogs[0]), 'pending');
        t('nilai ngaco jatuh ke Menunggu', workLogApproval({ approval:'ngaco' }), 'pending');
        t('disetujui terbaca', workLogApproval(workLogs[3]), 'approved');

        // ---------- tidak boleh menyetujui laporan buatan sendiri ----------
        t('boleh setujui laporan orang lain', canApproveThisLog(workLogs[1]), true);
        t('TIDAK boleh setujui laporan sendiri', canApproveThisLog(workLogs[2]), false);
        t('laporan lama tanpa pembuat: boleh', canApproveThisLog(workLogs[0]), true);

        let before = saved.length;
        approveWorkLog('wPunyaSendiri');
        t('setujui laporan sendiri ditolak', saved.length, before);

        // ---------- menyetujui ----------
        approveWorkLog('wPunyaOrangLain');
        t('persetujuan tersimpan', saved.length, before + 1);
        const ap = saved[saved.length-1];
        t('status jadi approved', ap.approval, 'approved');
        t('pencatat persetujuan terisi', ap.approvedByEmail, 'staff@x.id');
        t('waktu persetujuan terisi', ap.approvedAt > 0, true);
        t('catatan revisi dibersihkan', ap.revisionNote, '');

        // ---------- minta revisi ----------
        before = saved.length;
        window.prompt = () => '';                  // catatan kosong
        reviseWorkLog('wPunyaOrangLain');
        t('revisi tanpa catatan ditolak', saved.length, before);
        window.prompt = () => null;                // dibatalkan
        reviseWorkLog('wPunyaOrangLain');
        t('revisi dibatalkan tidak menulis', saved.length, before);
        window.prompt = () => 'Jam selesai belum diisi';
        reviseWorkLog('wPunyaOrangLain');
        t('revisi tersimpan', saved.length, before + 1);
        const rv = saved[saved.length-1];
        t('status jadi revision', rv.approval, 'revision');
        t('catatan revisi tersimpan', rv.revisionNote, 'Jam selesai belum diisi');
        t('persetujuan lama dihapus', [rv.approvedBy, rv.approvedAt], ['', 0]);

        // ---------- edit membatalkan persetujuan ----------
        before = saved.length;
        editWorkLog('wSudah');
        document.getElementById('wlTask').value = 'Diubah setelah disetujui';
        saveWorkLog({ preventDefault(){} });
        const ed = saved[saved.length-1];
        t('edit mengembalikan ke Menunggu', ed.approval, 'pending');
        t('edit menghapus stempel persetujuan', [ed.approvedBy, ed.approvedAt], ['', 0]);
        t('pembuat asli dipertahankan', ed.createdByUid, 'khl9');

        // Laporan baru mencatat pembuatnya.
        showAddWorkLogForm();
        document.getElementById('wlMember').value = 'm1';
        document.getElementById('wlDate').value = '2026-09-16';
        document.getElementById('wlTask').value = 'Baru';
        saveWorkLog({ preventDefault(){} });
        const nw = saved[saved.length-1];
        t('laporan baru mencatat pembuat', nw.createdByUid, 'staff1');
        t('laporan baru mulai dari Menunggu', nw.approval, 'pending');

        // ---------- tampilan tabel ----------
        renderWorkLogTable();
        t('kolom Persetujuan ada',
          [...document.querySelectorAll('#workLogTable thead th')].map(th=>th.textContent.trim()).includes('Persetujuan'), true);
        t('KPI menunggu menghitung seluruh log',
          document.getElementById('wlKpiPending').textContent,
          String(workLogs.filter(w=>workLogApproval(w)==='pending').length));
        const rowSendiri = [...document.querySelectorAll('#workLogBody tr')].find(tr=>tr.textContent.includes('Dibuat sendiri'));
        t('laporan sendiri: tanpa tombol setujui',
          rowSendiri.querySelectorAll('td[data-label="Persetujuan"] .appr-btn').length, 0);
        const rowLain = [...document.querySelectorAll('#workLogBody tr')].find(tr=>tr.textContent.includes('Dibuat KHL'));
        t('laporan orang lain: ada tombol', rowLain.querySelectorAll('td[data-label="Persetujuan"] .appr-btn').length > 0, true);

        // ---------- filter ----------
        document.getElementById('wlApprovalFilter').value = 'approved';
        renderWorkLogTable();
        const shown = [...document.querySelectorAll('#workLogBody tr')];
        t('filter disetujui', shown.every(tr=>tr.textContent.includes('Disetujui')), true);
        document.getElementById('wlApprovalFilter').value = 'pending';
        renderWorkLogTable();
        t('filter menunggu menyaring', document.querySelectorAll('#workLogBody tr').length < workLogs.length, true);
        clearWorkLogFilter();
        t('reset mengosongkan filter persetujuan',
          document.getElementById('wlApprovalFilter').value, '');

        // ---------- tanpa hak persetujuan ----------
        currentUserDoc = { role:'khl', status:'active', access:{ teamLog:'edit', teamLogApprove:'none' } };
        t('tanpa hak: canApproveWorkLogs false', canApproveWorkLogs(), false);
        renderWorkLogTable();
        t('tanpa hak: tidak ada tombol persetujuan sama sekali',
          document.querySelectorAll('#workLogBody .appr-btn').length, 0);
        t('tanpa hak: badge tetap terlihat',
          document.querySelectorAll('#workLogBody .appr').length > 0, true);
        before = saved.length;
        approveWorkLog('wPunyaOrangLain');
        t('tanpa hak: setujui ditolak', saved.length, before);

        // Pemeriksa yang TIDAK boleh mengedit laporan tetap bisa menyetujui.
        currentUserDoc = { role:'staff', status:'active', access:{ teamLog:'view', teamLogApprove:'edit' } };
        t('pemeriksa murni: boleh setujui', canApproveWorkLogs(), true);
        t('pemeriksa murni: tidak boleh edit', hasAccess('teamLog','edit'), false);
        renderWorkLogTable();
        t('pemeriksa murni: tanpa tombol edit/hapus',
          document.querySelectorAll('#workLogBody .row-actions').length, 0);
        t('pemeriksa murni: tetap ada tombol persetujuan',
          document.querySelectorAll('#workLogBody .appr-btn').length > 0, true);

        currentUserDoc = { role:'owner', status:'active' };
        t('owner boleh setujui laporannya sendiri (agar tidak buntu)',
          canApproveThisLog({ createdByUid:'owner-x' }), true);

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
