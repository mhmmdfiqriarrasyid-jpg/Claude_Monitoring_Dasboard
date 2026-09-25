// Izin / Sakit — pengajuan berentang tanggal dengan persetujuan atasan.
//
// Berkas ini ada terutama untuk menjaga empat hal yang kalau salah merusak
// data atau menyesatkan orang:
//
//   1. Rentang tanggal INKLUSIF dua ujungnya. Salah satu hari saja membuat
//      jumlah hari meleset di setiap rekap.
//   2. Hanya pengajuan DISETUJUI yang menandai jadwal. Kalau pengajuan yang
//      belum diperiksa ikut menandai, siapa pun bisa mengosongkan jadwal
//      dengan mengirim pengajuan yang belum dilihat siapa-siapa.
//   3. Penanda di grid shift TIDAK menulis apa pun ke koleksi shifts.
//      Menulis ke sana akan menimpa jadwal yang sudah diisi.
//   4. Jenis disimpan sebagai KUNCI ('izin'), bukan label tampilannya.
//      Nilai simpan yang sama dengan teks tampilan adalah ranjau yang sudah
//      ada di tempat lain di aplikasi ini; jangan tambah satu lagi.
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

        let asked = [], answer = true;
        window.confirm = m => { asked.push(String(m)); return answer; };
        const freshAsk = () => { asked = []; };
        const toasts = [];
        const toastAsli = window.showToast;
        window.showToast = m => { toasts.push(String(m)); };

        const written = [];
        const deleted = [];
        const docWrites = [];
        const shiftWrites = [];
        window.cloud = { isReady: true,
            saveLeaveRequest: r => { written.push(r); return Promise.resolve(); },
            deleteLeaveRequest: id => { deleted.push(id); return Promise.resolve(); },
            saveTeamDocs: (id, pages) => { docWrites.push(['save', id, pages.length]); return Promise.resolve(); },
            deleteTeamDocs: id => { docWrites.push(['delete', id]); return Promise.resolve(); },
            getTeamDocs: () => Promise.resolve([]),
            saveShift: r => { shiftWrites.push(r); return Promise.resolve(); },
            deleteShift: id => { shiftWrites.push(id); return Promise.resolve(); },
            saveUnits: () => Promise.resolve(), getAllUnits: () => Promise.resolve([]),
            addHistoryEvents: () => Promise.resolve(), subscribeUsers: () => () => {} };

        currentUser = { uid: 'uO', email: 'o@x.id' };
        currentUserDoc = { role: 'owner', status: 'active', email: 'o@x.id', displayName: 'Owner' };
        hideAuthGates(); applyRoleGating();
        navigateTo('team');

        teamMembers = [
            { id: 'm1', name: 'Andi', company: 'PT. GPA', active: true },
            { id: 'm2', name: 'Budi', company: 'PT. MNM', active: true }
        ];
        workLogs = []; teamShifts = []; leaveRequests = [];
        populateLeaveFilters();

        // ---------- JUMLAH HARI ----------
        // Inklusif dua ujungnya: tanggal 5 sampai 5 itu satu hari, bukan nol.
        t('satu tanggal sama = satu hari', leaveDays('2026-09-21', '2026-09-21'), 1);
        t('tanggal selesai kosong = satu hari', leaveDays('2026-09-21', ''), 1);
        t('rentang tiga hari', leaveDays('2026-09-21', '2026-09-23'), 3);
        t('rentang melintasi bulan', leaveDays('2026-09-29', '2026-10-02'), 4);
        t('tanggal terbalik tidak menghasilkan hari negatif',
          leaveDays('2026-09-23', '2026-09-21'), 0);

        // ---------- VALIDASI ----------
        freshAsk();
        t('anggota kosong ditolak tanpa dialog',
          [checkLeaveRange('', 'izin', '2026-09-21', '', null, 0), asked.length], [false, 0]);
        freshAsk();
        t('tanggal mulai kosong ditolak tanpa dialog',
          [checkLeaveRange('m1', 'izin', '', '', null, 0), asked.length], [false, 0]);
        freshAsk();
        t('tanggal terbalik DITOLAK, bukan ditanya',
          [checkLeaveRange('m1', 'izin', '2026-09-23', '2026-09-21', null, 0), asked.length], [false, 0]);
        t('penolakannya menjelaskan sebabnya',
          /lebih awal/.test(toasts[toasts.length - 1] || ''), true);

        freshAsk(); answer = false;
        t('rentang lebih dari sebulan minta konfirmasi, bukan ditolak diam-diam',
          checkLeaveRange('m1', 'sakit', '2026-01-01', '2026-03-01', null, 0), false);
        t('dan dialognya menyebut jumlah harinya', /60 hari/.test(asked[0] || ''), true);
        freshAsk(); answer = true;
        t('kalau dikonfirmasi, rentang panjang tetap boleh',
          checkLeaveRange('m1', 'sakit', '2026-01-01', '2026-03-01', null, 0), true);

        // Alpa berarti tanpa keterangan — surat terlampir itu ganjil, tapi
        // bukan hal yang boleh ditolak mentah-mentah.
        freshAsk(); answer = false;
        t('alpa dengan surat minta konfirmasi',
          checkLeaveRange('m1', 'alpa', '2026-09-21', '', null, 2), false);
        freshAsk(); answer = true;
        t('alpa TANPA surat tidak ditanya apa-apa',
          [checkLeaveRange('m1', 'alpa', '2026-09-21', '', null, 0), asked.length], [true, 0]);

        // Tanggal di masa depan itu normal di sini — izin memang direncanakan.
        freshAsk();
        const besok = addDaysISO(toISODate(), 30);
        t('tanggal di masa depan diterima tanpa keberatan',
          [checkLeaveRange('m1', 'izin', besok, '', null, 0), asked.length], [true, 0]);

        // ---------- TUMPANG-TINDIH ----------
        leaveRequests = [
            { id: 'lv_a', memberId: 'm1', memberName: 'Andi', type: 'sakit',
              dateFrom: '2026-09-21', dateTo: '2026-09-23', days: 3, docCount: 1,
              approval: 'approved', createdByUid: 'uX' },
            { id: 'lv_b', memberId: 'm1', memberName: 'Andi', type: 'izin',
              dateFrom: '2026-11-01', dateTo: '2026-11-01', days: 1, docCount: 0,
              approval: 'revision', createdByUid: 'uX' }
        ];
        t('beririsan terdeteksi', overlappingLeave('m1', '2026-09-22', '2026-09-25').map(r => r.id), ['lv_a']);
        t('orang lain di tanggal yang sama tidak dihitung bertabrakan',
          overlappingLeave('m2', '2026-09-22', '2026-09-25'), []);
        t('pengajuan yang sedang direvisi tidak menghalangi',
          overlappingLeave('m1', '2026-11-01', '2026-11-01'), []);
        t('menyunting dirinya sendiri bukan tabrakan',
          overlappingLeave('m1', '2026-09-21', '2026-09-23', 'lv_a'), []);
        freshAsk(); answer = false;
        t('tabrakan minta konfirmasi, bukan menolak',
          checkLeaveRange('m1', 'sakit', '2026-09-22', '2026-09-25', null, 1), false);
        t('dan menyebut pengajuan yang bertabrakan', /Sakit/.test(asked[0] || ''), true);
        answer = true;

        // ---------- HANYA YANG DISETUJUI MENANDAI JADWAL ----------
        t('hari pertama rentang tercakup', !!leaveOn('m1', '2026-09-21'), true);
        t('hari terakhir rentang tercakup', !!leaveOn('m1', '2026-09-23'), true);
        t('sehari sebelumnya tidak', leaveOn('m1', '2026-09-20'), null);
        t('sehari sesudahnya tidak', leaveOn('m1', '2026-09-24'), null);
        leaveRequests.push({ id: 'lv_c', memberId: 'm2', memberName: 'Budi', type: 'izin',
            dateFrom: '2026-09-22', dateTo: '2026-09-22', days: 1, docCount: 0,
            approval: 'pending', createdByUid: 'uX' });
        t('pengajuan yang BELUM disetujui tidak menandai jadwal',
          leaveOn('m2', '2026-09-22'), null);
        t('membersOnLeave hanya berisi yang disetujui',
          [...membersOnLeave('2026-09-22')], ['m1']);

        // ---------- GRID SHIFT: MENANDAI, TIDAK MENULIS ----------
        teamShifts = [{ id: '2026-09-22_m1', date: '2026-09-22', memberId: 'm1', shift: 'pagi' }];
        shiftWrites.length = 0;
        teamWeekStart = startOfWeekISO('2026-09-22');
        switchTeamTab('shift');
        renderShiftGrid();
        const penanda = document.querySelector('.shift-leave');
        t('penanda izin tergambar di grid', !!penanda, true);
        t('penandanya menyebut jenisnya', penanda ? penanda.textContent.trim() : '', 'Sakit');
        t('pilihan shift TIDAK ikut hilang',
          !!document.querySelector('#shiftBody .shift-select'), true);
        t('dan shift yang sudah diisi tidak berubah',
          teamShifts[0].shift, 'pagi');
        t('tidak ada satu pun tulisan ke koleksi shifts', shiftWrites, []);

        // ---------- HITUNGAN YANG IKUT TERPENGARUH ----------
        // Orang yang sakit dan sudah disetujui tidak sedang bertugas.
        weekAnchor = startOfWeekISO('2026-09-22');
        const ring = weekStats(weekDates(startOfWeekISO('2026-09-22')));
        t('orang yang izin disetujui tidak terhitung bertugas', ring.onDuty, 0);
        t('dan dihitung terpisah sebagai sedang izin', ring.onLeave, 1);

        // ---------- TABEL ----------
        switchTeamTab('leave');
        renderLeaveTable();
        const tds = [...document.querySelectorAll('#leaveBody td')];
        // Satu sel tanpa label per baris, yaitu nomor urutnya; sel aksi punya
        // kelasnya sendiri dan memang dikecualikan CSS-nya.
        t('setiap sel punya data-label kecuali nomor dan aksi',
          tds.filter(td => !td.hasAttribute('data-label') && !td.classList.contains('col-actions')).length,
          leaveRequests.length);
        t('menunggu persetujuan dihitung dari SELURUH pengajuan, bukan yang terfilter',
          document.getElementById('lvKpiPending').textContent, '1');
        t('kolom kosong tidak memakai colspan yang salah',
          (() => { const save = leaveRequests; leaveRequests = []; renderLeaveTable();
                   const c = document.querySelector('#leaveBody td[colspan]').getAttribute('colspan');
                   leaveRequests = save; renderLeaveTable(); return c; })(),
          String(document.querySelectorAll('#leaveTable thead th').length));

        // ---------- SIMPAN ----------
        leaveRequests = [];
        written.length = 0;
        document.getElementById('editLeaveId').value = '';
        document.getElementById('lvMember').value = 'm1';
        document.getElementById('lvType').value = 'izin';
        document.getElementById('lvDateFrom').value = '2026-12-01';
        document.getElementById('lvDateTo').value = '2026-12-03';
        document.getElementById('lvReason').value = 'Acara keluarga';
        saveLeave();
        t('tersimpan satu pengajuan', written.length, 1);
        t('jenisnya disimpan sebagai KUNCI, bukan label', written[0].type, 'izin');
        t('jumlah harinya ikut disimpan', written[0].days, 3);
        t('nama dan perusahaan anggota ikut disalin',
          [written[0].memberName, written[0].company], ['Andi', 'PT. GPA']);
        t('pengajuan baru selalu menunggu persetujuan', written[0].approval, 'pending');

        // ---------- PERSETUJUAN ----------
        const rec = written[0];
        leaveRequests = [{ ...rec }];
        written.length = 0;
        // Pembuatnya sendiri tidak boleh menyetujui — kecuali owner.
        currentUserDoc = { role: 'staff', status: 'active', email: 's@x.id',
                           access: { teamLog: 'edit', teamLogApprove: 'edit' } };
        currentUser = { uid: rec.createdByUid, email: 's@x.id' };
        applyRoleGating();
        approveLeave(rec.id);
        t('pembuatnya sendiri tidak bisa menyetujui', written.length, 0);

        currentUser = { uid: 'uLain', email: 'lain@x.id' };
        approveLeave(rec.id);
        t('orang lain bisa menyetujui', written.length, 1);
        t('statusnya jadi disetujui', written[0].approval, 'approved');
        t('dan tercatat siapa yang menyetujui', !!written[0].approvedAt, true);

        // Menyunting mengembalikannya ke menunggu.
        leaveRequests = [{ ...written[0] }];
        written.length = 0;
        currentUserDoc = { role: 'owner', status: 'active', email: 'o@x.id' };
        currentUser = { uid: 'uO', email: 'o@x.id' };
        applyRoleGating();
        document.getElementById('editLeaveId').value = rec.id;
        document.getElementById('lvMember').value = 'm1';
        document.getElementById('lvType').value = 'izin';
        document.getElementById('lvDateFrom').value = '2026-12-01';
        document.getElementById('lvDateTo').value = '2026-12-05';
        document.getElementById('lvReason').value = 'Acara keluarga';
        saveLeave();
        t('menyunting mengembalikan ke menunggu persetujuan', written[0].approval, 'pending');
        t('dan membersihkan jejak persetujuan lama', written[0].approvedAt, 0);
        // Toast suksesnya baru muncul setelah tulisannya selesai, jadi beri
        // satu putaran microtask sebelum memeriksanya.
        await Promise.resolve(); await Promise.resolve();
        t('penggunanya diberi tahu', toasts.some(m => /persetujuan dibatalkan/i.test(m)), true);

        // ---------- HAK AKSES ----------
        currentUserDoc = { role: 'khl', status: 'active', access: { teamLog: 'view' } };
        applyRoleGating();
        written.length = 0; deleted.length = 0;
        saveLeave();
        t('tanpa hak edit, simpan ditolak', written.length, 0);
        leaveRequests = [{ ...rec }];
        window.confirm = () => true;
        deleteLeave(rec.id);
        t('tanpa hak edit, hapus ditolak', deleted.length, 0);
        window.confirm = m => { asked.push(String(m)); return answer; };

        currentUserDoc = { role: 'owner', status: 'active' };
        applyRoleGating();

        // ---------- HAPUS IKUT MENGHAPUS SURAT ----------
        docWrites.length = 0; deleted.length = 0;
        leaveRequests = [{ ...rec, docCount: 2 }];
        answer = true;
        deleteLeave(rec.id);
        t('menghapus pengajuan ikut menghapus suratnya',
          docWrites.filter(d => d[0] === 'delete' && d[1] === rec.id).length, 1);
        t('dan pengajuannya sendiri terhapus', deleted, [rec.id]);

        // ---------- PERIKSA DATA ----------
        leaveRequests = [
            { id: 'lv_x', memberId: 'm1', memberName: 'Andi', type: 'sakit',
              dateFrom: '2026-09-01', dateTo: '2026-09-02', days: 2, docCount: 0,
              approval: 'approved' },
            { id: 'lv_y', memberId: 'm1', memberName: 'Andi', type: 'alpa',
              dateFrom: '2026-09-05', dateTo: '2026-09-05', days: 1, docCount: 0,
              approval: 'approved' },
            { id: 'lv_z', memberId: 'm2', memberName: 'Budi', type: 'izin',
              dateFrom: '2026-09-10', dateTo: '2026-09-08', days: 0, docCount: 1,
              approval: 'approved' }
        ];
        t('izin/sakit disetujui tanpa surat terdeteksi',
          dcLeaveWithoutDoc().map(x => x.label), ['Sakit: Andi']);
        t('alpa TIDAK pernah dianggap kurang surat',
          dcLeaveWithoutDoc().some(x => /Alpa/.test(x.label)), false);
        t('rentang terbalik terdeteksi', dcLeaveReversed().length, 1);

        leaveRequests = [{ id: 'lv_ok', memberId: 'm1', memberName: 'Andi', type: 'izin',
            dateFrom: '2026-09-01', dateTo: '2026-09-02', days: 2, docCount: 1, approval: 'approved' }];
        t('data bersih tidak menghasilkan temuan apa pun',
          [dcLeaveWithoutDoc().length, dcLeaveReversed().length], [0, 0]);

        // ---------- KOTAK KEPUTUSAN ----------
        leaveRequests = [{ id: 'lv_p', memberId: 'm1', memberName: 'Andi', type: 'izin',
            dateFrom: '2026-09-01', dateTo: '2026-09-02', days: 2, docCount: 0, approval: 'pending' }];
        t('kelompok izin muncul untuk yang berhak',
          decisionGroups().filter(g => g.key === 'leave').map(g => g.total), [1]);
        leaveRequests = [];
        t('kelompok kosong tidak ditampilkan',
          decisionGroups().filter(g => g.key === 'leave').length, 0);
        currentUserDoc = { role: 'khl', status: 'active', access: { teamLog: 'none', teamLogApprove: 'none' } };
        applyRoleGating();
        leaveRequests = [{ id: 'lv_p', memberId: 'm1', type: 'izin',
            dateFrom: '2026-09-01', dateTo: '2026-09-02', days: 2, approval: 'pending' }];
        t('tanpa akses, kelompoknya tidak muncul',
          decisionGroups().filter(g => g.key === 'leave').length, 0);

        // ---------- TAB ----------
        currentUserDoc = { role: 'khl', status: 'active', access: { teamShift: 'view', teamLog: 'none' } };
        applyRoleGating();
        switchTeamTab('leave');
        t('tanpa akses laporan, tab izin tidak bisa dibuka', teamTab, 'shift');
        const tombolIzin = document.querySelector('.team-tab[data-tab="leave"]');
        t('dan tombolnya disembunyikan', tombolIzin.style.display, 'none');

        currentUserDoc = { role: 'owner', status: 'active' };
        applyRoleGating();
        switchTeamTab('leave');
        t('dengan akses, tab izin terbuka', teamTab, 'leave');
        t('dan panelnya tampil',
          document.getElementById('teamLeavePanel').style.display, '');

        // ---------- REGRESI: tab halaman lain tidak ikut disembunyikan ----------
        // renderTeamView dulu menyapu SEMUA .team-tab di dokumen. Kotak
        // Keputusan dan Gudang memakai kelas yang sama, jadi begitu Tim dibuka,
        // tab kedua halaman itu hilang sampai halaman dimuat ulang.
        currentUserDoc = { role: 'owner', status: 'active' };
        applyRoleGating();
        navigateTo('team');
        navigateTo('leader');
        t('tab Kotak Keputusan tetap terlihat setelah membuka Tim',
          [...document.querySelectorAll('.leader-tab')].filter(b => b.style.display === 'none').length, 0);
        navigateTo('team');
        navigateTo('warehouse');
        t('tab Gudang tetap terlihat setelah membuka Tim',
          [...document.querySelectorAll('.wh-tab')].filter(b => b.style.display === 'none').length, 0);

        window.showToast = toastAsli;
        window.__T = T;
    } catch (e) { window.__T = [{n:'THREW: '+e.message+' | '+(e.stack||'').split('\\n')[1], g:1, w:0, pass:false}]; }
    })();` });

    await page.waitForFunction(() => window.__T, null, { timeout: 30000 });
    const res = await page.evaluate(() => window.__T);
    const fail = res.filter(r => !r.pass);
    res.forEach(r => console.log(`${r.pass ? 'PASS' : 'FAIL'}  ${r.n}${r.pass ? '' : `\n        dapat=${JSON.stringify(r.g)} harap=${JSON.stringify(r.w)}`}`));
    console.log(`\n${res.length - fail.length}/${res.length} lulus`);
    console.log('PAGE ERRORS:', errors.length ? errors : 'none');
    await b.close();
    process.exit(fail.length ? 1 : 0);
})();
