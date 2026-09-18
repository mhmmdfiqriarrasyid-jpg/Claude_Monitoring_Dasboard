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
        const GPA = 'PT. Global Papua Abadi', MNM = 'PT. Murni Nusantara Mandiri';

        const writes = [];
        window.cloud = { isReady: true,
            saveShift: r => { writes.push(['shift', r.id]); return Promise.resolve(); },
            deleteShift: id => { writes.push(['delShift', id]); return Promise.resolve(); },
            saveTeamMember: r => { writes.push(['member', r.name, r.company]); return Promise.resolve(); },
            deleteTeamMember: () => Promise.resolve(),
            saveWorkLog: r => { writes.push(['log', r.id, r.company]); return Promise.resolve(); },
            deleteWorkLog: () => Promise.resolve() };
        currentUser = { uid:'u1', email:'o@x.id' };
        currentUserDoc = { role:'owner', status:'active', email:'o@x.id' };
        hideAuthGates(); applyRoleGating();
        globalData = [{ id:'u_1', name:'JD-01', sn:'SN1', site:'Timika', status:'Good' }];

        teamMembers = [
            { id:'m1', name:'Andreas Kayembob',  jobTitle:'Tenaga Operational Technology', company:GPA, active:true },
            { id:'m2', name:'Farel Mohammad',    jobTitle:'Tenaga Operational Technology', company:GPA, active:true },
            { id:'m3', name:'Ibrahim H.Z',       jobTitle:'Tenaga Operational Technology', company:MNM, active:true },
            { id:'m4', name:'Johanes Valdes',    jobTitle:'Tenaga Operational Technology', company:MNM, active:true },
            { id:'m5', name:'Philipus Bone',     jobTitle:'Tenaga Operational Technology', active:true },      // belum diisi
            { id:'m6', name:'Eka (nonaktif)',    jobTitle:'Helper', company:GPA, active:false }
        ];
        const wk = '2026-09-14';
        teamShifts = [
            { id: wk+'_m1', date:wk, memberId:'m1', shift:'pagi' },
            { id: wk+'_m2', date:wk, memberId:'m2', shift:'malam' },
            { id: wk+'_m3', date:wk, memberId:'m3', shift:'pagi' },
            { id: wk+'_m4', date:wk, memberId:'m4', shift:'libur' },
            { id: wk+'_m5', date:wk, memberId:'m5', shift:'siang' }
        ];
        workLogs = [
            { id:'w1', date:wk, memberId:'m1', memberName:'Andreas Kayembob', company:GPA, start:'08:00', end:'16:00', task:'Servis', issue:'' },
            { id:'w2', date:wk, memberId:'m3', memberName:'Ibrahim H.Z',      company:MNM, start:'08:00', end:'12:00', task:'Cek unit', issue:'' },
            { id:'w3', date:wk, memberId:'m5', memberName:'Philipus Bone',                  start:'09:00', end:'17:00', task:'Rapat', issue:'' },
            { id:'w4', date:wk, memberId:'mX', memberName:'Orang lama', company:'PT. Lama', start:'08:00', end:'10:00', task:'Arsip', issue:'' }
        ];
        teamWeekStart = wk;

        // ---------- helper perusahaan ----------
        t('dua perusahaan bawaan ada', DEFAULT_COMPANIES, [GPA, MNM]);
        t('allCompanies gabung bawaan + yang dipakai', allCompanies(), [GPA, MNM]);
        teamMembers.push({ id:'m7', name:'Baru', company:'PT. Ketiga Jaya', active:true });
        t('perusahaan baru otomatis masuk saran', allCompanies().includes('PT. Ketiga Jaya'), true);
        teamMembers.pop();
        t('duplikat beda huruf besar-kecil tidak menggandakan',
          (() => { teamMembers.push({ id:'mz', name:'Z', company:'pt. global papua abadi', active:true });
                   const n = allCompanies().length; teamMembers.pop(); return n; })(), 2);

        // ---------- pengelompokan ----------
        const groups = membersByCompany();
        t('jumlah grup (2 perusahaan + tanpa perusahaan)', groups.length, 3);
        t('urutan grup, tanpa-perusahaan paling akhir', groups.map(g => g[0]), [GPA, MNM, NO_COMPANY]);
        t('anggota nonaktif tidak masuk grup', groups[0][1].map(m => m.id), ['m1','m2']);
        t('grup tanpa perusahaan berisi yang kosong', groups[2][1].map(m => m.id), ['m5']);

        // ---------- render grid ----------
        navigateTo('team'); switchTeamTab('shift');
        const groupRows = [...document.querySelectorAll('#shiftBody tr.shift-group')];
        t('baris judul grup muncul', groupRows.length, 3);
        t('judul grup pertama', groupRows[0].querySelector('.shift-group__name').textContent.trim().split('\\n')[0].trim(), GPA);
        t('jumlah orang per grup', groupRows.map(r => r.querySelector('.shift-group__count').textContent), ['2 orang','2 orang','1 orang']);
        t('total baris = 3 judul + 5 anggota aktif', document.querySelectorAll('#shiftBody tr').length, 8);

        // subtotal per perusahaan pada hari Senin (kolom pertama)
        const cellOf = (row, i) => row.querySelectorAll('td')[i].querySelector('strong').textContent;
        t('GPA Senin bertugas 2 (pagi+malam)', cellOf(groupRows[0], 0), '2');
        t('MNM Senin bertugas 1 (libur tidak dihitung)', cellOf(groupRows[1], 0), '1');
        t('tanpa perusahaan Senin bertugas 1', cellOf(groupRows[2], 0), '1');
        const footCells = document.querySelectorAll('#shiftFoot td');
        t('total keseluruhan Senin = 4', footCells[0].querySelector('strong').textContent, '4');
        t('label total', document.querySelector('#shiftFoot th').textContent.trim(), 'Total bertugas');

        // ---------- laporan harian ----------
        switchTeamTab('worklog');
        t('kolom Perusahaan ada di kepala tabel',
          [...document.querySelectorAll('#workLogTable thead th')].map(th=>th.textContent.trim()).includes('Perusahaan'), true);
        t('semua laporan tampil', document.querySelectorAll('#workLogBody tr').length, 4);

        document.getElementById('wlCompanyFilter').value = GPA;
        renderWorkLogTable();
        t('filter perusahaan GPA', document.querySelectorAll('#workLogBody tr').length, 1);
        t('KPI jam ikut tersaring', document.getElementById('wlKpiHours').textContent, '8j');

        document.getElementById('wlCompanyFilter').value = '__none__';
        renderWorkLogTable();
        t('filter tanpa perusahaan', document.querySelectorAll('#workLogBody tr').length, 1);

        clearWorkLogFilter();
        t('reset mengosongkan filter perusahaan', document.getElementById('wlCompanyFilter').value, '');
        t('reset menampilkan semua lagi', document.querySelectorAll('#workLogBody tr').length, 4);

        document.getElementById('wlSearch').value = 'murni';
        renderWorkLogTable();
        t('pencarian ikut menjangkau nama perusahaan', document.querySelectorAll('#workLogBody tr').length, 1);
        clearWorkLogFilter();

        // Perusahaan diambil dari data anggota terkini, bukan yang tersimpan basi.
        t('perusahaan mengikuti anggota hidup',
          companyOfRecord({ memberId:'m1', company:'PT. Basi' }), GPA);
        t('anggota terhapus: pakai yang tersimpan',
          companyOfRecord({ memberId:'mX', company:'PT. Lama' }), 'PT. Lama');

        // ---------- simpan ----------
        setMemberCompany('m5', MNM);
        t('isi perusahaan tersimpan', writes[writes.length-1], ['member','Philipus Bone',MNM]);
        setMemberCompany('m5', MNM);
        t('nilai sama tidak menulis ulang', writes.length, 1);

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
