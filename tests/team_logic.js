const { launch, BASE_URL } = require('./_env');

(async () => {
    const b = await launch();
    const page = await b.newPage({ viewport: { width: 1440, height: 900 } });
    const errors = [];
    page.on('pageerror', e => { if (!/Chart is not defined/.test(e.message)) errors.push(e.message); });
    await page.goto(BASE_URL + '/index.html', { waitUntil: 'load' });
    await page.waitForTimeout(1000);

    await page.addScriptTag({ content: `(() => {
  try {

        const T = []; const t = (name, got, want) => T.push({ name, got, want, pass: JSON.stringify(got) === JSON.stringify(want) });

        // ---------- pure logic ----------
        t('parseHHMM 07:30', parseHHMM('07:30'), 450);
        t('parseHHMM 00:00', parseHHMM('00:00'), 0);
        t('parseHHMM invalid 25:00', parseHHMM('25:00'), null);
        t('parseHHMM empty', parseHHMM(''), null);

        t('durasi siang 08:00-16:00', workLogMinutes({ start: '08:00', end: '16:00' }), 480);
        t('durasi malam 23:00-07:00 (lewat tengah malam)', workLogMinutes({ start: '23:00', end: '07:00' }), 480);
        t('durasi 22:30-06:15 (lewat tengah malam)', workLogMinutes({ start: '22:30', end: '06:15' }), 465);
        t('durasi jam kosong', workLogMinutes({ start: '', end: '' }), 0);

        t('formatMinutes 480', formatMinutes(480), '8j');
        t('formatMinutes 465', formatMinutes(465), '7j 45m');
        t('formatMinutes 45', formatMinutes(45), '45m');
        t('formatMinutes 0', formatMinutes(0), '0j');

        // Week starts on Monday. 2026-09-15 is a Tuesday.
        t('startOfWeek Selasa 2026-09-15', startOfWeekISO('2026-09-15'), '2026-09-14');
        t('startOfWeek Senin 2026-09-14', startOfWeekISO('2026-09-14'), '2026-09-14');
        t('startOfWeek Minggu 2026-09-20', startOfWeekISO('2026-09-20'), '2026-09-14');
        t('weekDates panjang 7', weekDates('2026-09-14').length, 7);
        t('weekDates akhir', weekDates('2026-09-14')[6], '2026-09-20');

        // Crossing a month and a DST-free boundary must not slip a day.
        t('addDays lintas bulan', addDaysISO('2026-09-30', 1), '2026-10-01');
        t('addDays lintas tahun', addDaysISO('2026-12-31', 1), '2027-01-01');
        t('addDays mundur', addDaysISO('2026-01-01', -1), '2025-12-31');
        t('weekRange satu bulan', weekRangeLabel('2026-09-14'), '14 – 20 Sep 2026');
        t('weekRange lintas bulan', weekRangeLabel('2026-09-28'), '28 Sep – 4 Okt 2026');

        // ---------- seed data + stub cloud ----------
        const writes = [];
        window.cloud = {
            isReady: true,
            saveShift: r => { writes.push(['saveShift', r.id, r.shift]); return Promise.resolve(); },
            deleteShift: id => { writes.push(['deleteShift', id]); return Promise.resolve(); },
            saveTeamMember: r => { writes.push(['saveTeamMember', r.name]); return Promise.resolve(); },
            deleteTeamMember: id => { writes.push(['deleteTeamMember', id]); return Promise.resolve(); },
            saveWorkLog: r => { writes.push(['saveWorkLog', r.id]); return Promise.resolve(); },
            deleteWorkLog: id => { writes.push(['deleteWorkLog', id]); return Promise.resolve(); }
        };
        currentUserDoc = { role: 'owner', status: 'active', email: 'o@x.id' };
        currentUser = { uid: 'u1', email: 'o@x.id' };
        globalData = [
            { id: 'u_1', name: 'TRAC-01', sn: 'SN111', site: 'Site A', status: 'Good' },
            { id: 'u_2', name: 'TRAC-02', sn: 'SN222', site: 'Site B', status: 'Breakdown' }
        ];
        teamMembers = [
            { id: 'm1', name: 'Andi', jobTitle: 'Mekanik', active: true },
            { id: 'm2', name: 'Budi', jobTitle: 'Operator', active: true },
            { id: 'm3', name: 'Citra', jobTitle: 'Admin', active: false }
        ];
        teamShifts = [
            { id: '2026-09-14_m1', date: '2026-09-14', memberId: 'm1', shift: 'pagi' },
            { id: '2026-09-14_m2', date: '2026-09-14', memberId: 'm2', shift: 'malam' },
            { id: '2026-09-15_m1', date: '2026-09-15', memberId: 'm1', shift: 'libur' }
        ];
        workLogs = [
            { id: 'w1', date: '2026-09-15', memberId: 'm1', memberName: 'Andi', start: '08:00', end: '16:00', unitId: 'u_1', unitName: 'TRAC-01', sn: 'SN111', task: 'Ganti filter oli', issue: '' },
            { id: 'w2', date: '2026-09-14', memberId: 'm2', memberName: 'Budi', start: '23:00', end: '07:00', unitId: 'u_2', unitName: 'TRAC-02', sn: 'SN222', task: 'Operasi malam', issue: 'Lampu mati' },
            { id: 'w3', date: '2026-09-10', memberId: 'm9', memberName: 'Dedi', start: '09:00', end: '12:00', unitId: '', unitName: '', task: 'Rapat', issue: '' }
        ];

        // ---------- shift grid ----------
        teamWeekStart = '2026-09-14';
        navigateTo('team');
        t('view Tim terlihat', document.getElementById('viewTeam').style.display, 'block');
        t('baris anggota = anggota aktif (Citra nonaktif dikecualikan)',
            document.querySelectorAll('#shiftBody tr:not(.shift-group)').length, 2);
        t('satu judul grup (keduanya tanpa perusahaan)',
            document.querySelectorAll('#shiftBody tr.shift-group').length, 1);
        t('kolom hari', document.querySelectorAll('#shiftHead th').length, 8);
        t('label minggu', document.getElementById('shiftWeekLabel').textContent, '14 – 20 Sep 2026');

        const sels = document.querySelectorAll('#shiftBody tr:not(.shift-group) .shift-select');
        t('sel Senin Andi = pagi', sels[0].value, 'pagi');
        t('sel Selasa Andi = libur', sels[1].value, 'libur');
        t('sel Rabu Andi kosong', sels[2].value, '');

        // Footer counts only real duty, so 'libur' must not be counted.
        const foot = document.querySelectorAll('#shiftFoot td');
        t('Senin bertugas 2 (pagi+malam)', foot[0].querySelector('strong').textContent, '2');
        t('Selasa bertugas 0 (hanya libur)', foot[1].querySelector('strong').textContent, '0');

        // Member deleted from the list still reads sensibly on old rows.
        t('anggota terhapus: pakai nama tersimpan', memberNameOf({ memberId: 'm9', memberName: 'Dedi' }), 'Dedi');
        t('anggota terhapus tanpa nama tersimpan', memberNameOf({ memberId: 'm9' }), '(anggota dihapus)');
        t('nama anggota hidup', memberNameOf({ memberId: 'm1', memberName: 'basi' }), 'Andi');

        // ---------- setShift ----------
        setShift('m1', '2026-09-16', 'siang');
        t('setShift menulis ke cloud', writes[writes.length - 1][0], 'saveShift');
        t('setShift id dokumen', writes[writes.length - 1][1], '2026-09-16_m1');
        t('setShift tersimpan lokal', shiftFor('m1', '2026-09-16'), 'siang');
        setShift('m1', '2026-09-16', '');
        t('setShift kosong = hapus', writes[writes.length - 1][0], 'deleteShift');
        t('setShift kosong hilang lokal', shiftFor('m1', '2026-09-16'), '');

        // ---------- work logs ----------
        switchTeamTab('worklog');
        t('tab laporan aktif', document.getElementById('teamWorkLogPanel').style.display, '');
        t('jumlah baris laporan', document.querySelectorAll('#workLogBody tr').length, 3);
        t('KPI jumlah', document.getElementById('wlKpiCount').textContent, '3');
        t('KPI total jam (8j + 8j + 3j)', document.getElementById('wlKpiHours').textContent, '19j');

        // filter by member
        document.getElementById('wlMemberFilter').value = 'm1';
        renderWorkLogTable();
        t('filter anggota', document.querySelectorAll('#workLogBody tr').length, 1);
        t('filter anggota KPI jam', document.getElementById('wlKpiHours').textContent, '8j');
        document.getElementById('wlMemberFilter').value = '';

        // filter by date range
        document.getElementById('wlFrom').value = '2026-09-14';
        document.getElementById('wlTo').value = '2026-09-15';
        renderWorkLogTable();
        t('filter rentang tanggal', document.querySelectorAll('#workLogBody tr').length, 2);

        // search
        clearWorkLogFilter();
        document.getElementById('wlSearch').value = 'lampu';
        renderWorkLogTable();
        t('cari di kolom kendala', document.querySelectorAll('#workLogBody tr').length, 1);
        document.getElementById('wlSearch').value = 'dedi';
        renderWorkLogTable();
        t('cari nama anggota terhapus', document.querySelectorAll('#workLogBody tr').length, 1);
        clearWorkLogFilter();
        t('reset filter kembalikan semua', document.querySelectorAll('#workLogBody tr').length, 3);

        // ---------- unit profile cross-link ----------
        t('workLogsForUnit id', workLogsForUnit('u_1', 'SN111').length, 1);
        t('workLogsForUnit sn saja', workLogsForUnit('', 'SN222').length, 1);
        t('workLogsForUnit tak terkait', workLogsForUnit('u_9', 'SN999').length, 0);

        // ---------- CSV safety ----------
        t('csvCell menetralkan formula', csvCell('=CMD()'), '"\\'=CMD()"');

        // ---------- read-only gating ----------
        currentUserDoc = { role: 'viewer', status: 'active', access: { teamShift: 'view', teamLog: 'view', teamMembers: 'view' } };
        switchTeamTab('shift');
        t('viewer: tanpa select shift', document.querySelectorAll('#shiftBody .shift-select').length, 0);
        t('viewer: pakai badge', document.querySelectorAll('#shiftBody .shift-badge').length > 0, true);
        switchTeamTab('worklog');
        t('viewer: tanpa tombol aksi', document.querySelectorAll('#workLogBody .row-actions').length, 0);
        const before = writes.length;
        setShift('m1', '2026-09-17', 'pagi');
        t('viewer: setShift ditolak', writes.length, before);
        currentUserDoc = { role: 'owner', status: 'active' };


        // ---------- tabrakan jadwal shift tidak lagi senyap ----------
        // Id dokumennya tanggal + id anggota, jadi dua supervisor yang
        // mengisi orang dan hari yang sama menulis dokumen yang sama dan yang
        // belakangan menang. Masing-masing melihat nilainya sendiri
        // dikonfirmasi, lalu grid yang kalah berubah sendiri tanpa penjelasan.
        const pesanShift = [];
        const toastLama = window.showToast;
        window.showToast = m => { pesanShift.push(String(m)); };
        currentUser = { uid: 'uAku', email: 'aku@x.id' };

        _shiftPendingWrites.clear();
        setShift('m1', '2026-09-17', 'pagi');
        t('tulisan sendiri dicatat sebagai klaim', _shiftPendingWrites.has('2026-09-17_m1'), true);
        t('dan dokumennya membawa siapa yang menulis',
          !!teamShifts.find(s => s.id === '2026-09-17_m1').updatedByUid, true);

        // Snapshot mengembalikan nilai yang sama: itu tulisan kita sendiri yang
        // mendarat, tidak perlu memberi tahu apa pun.
        pesanShift.length = 0;
        applyCloudShiftsSnapshot([{ id:'2026-09-17_m1', date:'2026-09-17', memberId:'m1',
            shift:'pagi', updatedBy:'Aku', updatedByUid:'uAku' }]);
        t('tulisan sendiri yang mendarat tidak memicu peringatan', pesanShift.length, 0);
        t('dan klaimnya dilepas', _shiftPendingWrites.has('2026-09-17_m1'), false);

        // Sekarang orang lain menimpanya.
        _shiftPendingWrites.clear();
        setShift('m1', '2026-09-18', 'pagi');
        pesanShift.length = 0;
        applyCloudShiftsSnapshot([{ id:'2026-09-18_m1', date:'2026-09-18', memberId:'m1',
            shift:'malam', updatedBy:'Budi', updatedByUid:'uBudi' }]);
        t('ditimpa orang lain memunculkan peringatan', pesanShift.length, 1);
        t('peringatannya menyebut siapa', /Budi/.test(pesanShift[0]), true);
        t('dan menyebut jadi apa', /Malam|malam/.test(pesanShift[0]), true);

        // Nilai berbeda TANPA pemilik lain berarti tulisan kita belum mendarat.
        // Memperingatkan di situ akan jadi alarm palsu tiap sinyal lambat.
        _shiftPendingWrites.clear();
        setShift('m1', '2026-09-19', 'pagi');
        pesanShift.length = 0;
        applyCloudShiftsSnapshot([]);
        t('tulisan yang belum mendarat bukan alasan memperingatkan', pesanShift.length, 0);
        t('klaimnya ditahan sampai jelas', _shiftPendingWrites.has('2026-09-19_m1'), true);
        window.showToast = toastLama;

        window.__T = T;
  } catch (e) { window.__T = [{name:'THREW: '+e.message+' | '+(e.stack||'').split('\\n')[1], got:1, want:0, pass:false}]; }
    })();` });
    const res = await page.evaluate(() => window.__T);

    const fail = res.filter(r => !r.pass);
    res.forEach(r => console.log(`${r.pass ? 'PASS' : 'FAIL'}  ${r.name}${r.pass ? '' : `\n        dapat=${JSON.stringify(r.got)} harap=${JSON.stringify(r.want)}`}`));
    console.log(`\n${res.length - fail.length}/${res.length} lulus`);
    console.log('PAGE ERRORS:', errors.length ? errors : 'none');
    await b.close();
    process.exit(fail.length ? 1 : 0);
})();
