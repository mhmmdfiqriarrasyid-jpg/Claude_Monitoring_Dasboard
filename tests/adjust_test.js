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
        const saved = []; const photoWrites = [];
        window.cloud = { isReady: true,
            saveShift:()=>Promise.resolve(), deleteShift:()=>Promise.resolve(),
            saveTeamMember:()=>Promise.resolve(), deleteTeamMember:()=>Promise.resolve(),
            saveWorkLog: r => { saved.push(r); return Promise.resolve(); },
            deleteWorkLog:()=>Promise.resolve(),
            getWorkLogPhotos:()=>Promise.resolve([]),
            saveWorkLogPhotos:(id,ph)=>{ photoWrites.push({id,ph}); return Promise.resolve(); },
            deleteWorkLogPhotos:id=>{ photoWrites.push({id,ph:null}); return Promise.resolve(); } };
        currentUser = { uid:'u1', email:'o@x.id' };
        currentUserDoc = { role:'owner', status:'active', email:'o@x.id' };
        hideAuthGates(); applyRoleGating();
        globalData = [
            { id:'u_1', name:'JD-6110B-01', sn:'SN111', site:'Timika', status:'Good' },
            { id:'u_2', name:'JD-5075E-04', sn:'SN222', site:'Timika', status:'Good' },
            { id:'u_3', name:'JD-5045D-09', sn:'SN333', site:'Timika', status:'Good' }
        ];
        teamMembers = [{ id:'m1', name:'Andi', jobTitle:'Mekanik', company:'PT. Global Papua Abadi', active:true }];
        teamShifts = [];
        // Satu laporan format LAMA (unitId tunggal), satu format BARU (units[]).
        workLogs = [
            { id:'wOld', date:'2026-09-14', memberId:'m1', memberName:'Andi', start:'08:00', end:'16:00',
              unitId:'u_1', unitName:'JD-6110B-01', sn:'SN111', task:'Servis lama', issue:'' },
            { id:'wNew', date:'2026-09-15', memberId:'m1', memberName:'Andi', start:'08:00', end:'12:00',
              units:[{id:'u_2',name:'JD-5075E-04',sn:'SN222'},{id:'u_3',name:'JD-5045D-09',sn:'SN333'}],
              unitId:'u_2', unitName:'JD-5075E-04', sn:'SN222',
              paddock:'Paddock B-12', photos:['data:image/jpeg;base64,AAA','data:image/jpeg;base64,BBB'],
              task:'Servis baru', issue:'' }
        ];
        teamWeekStart = '2026-09-14';
        navigateTo('team'); switchTeamTab('worklog');

        // ---------- kompatibilitas format lama ----------
        t('laporan lama terbaca sebagai 1 unit', workLogUnits(workLogs[0]).map(u=>u.id), ['u_1']);
        t('laporan baru terbaca sebagai 2 unit', workLogUnits(workLogs[1]).map(u=>u.id), ['u_2','u_3']);
        t('laporan tanpa unit = daftar kosong', workLogUnits({}), []);
        t('nama unit diambil dari data unit hidup',
          workLogUnitNames({ units:[{id:'u_1',name:'NAMA BASI',sn:'SN111'}] }), ['JD-6110B-01']);
        t('unit terhapus: pakai nama tersimpan',
          workLogUnitNames({ units:[{id:'u_9',name:'UNIT LAMA',sn:'SN999'}] }), ['UNIT LAMA']);

        // ---------- profil unit mencocokkan unit ke-2 ----------
        t('profil unit menemukan laporan lewat unit pertama', workLogsForUnit('u_2','SN222').map(w=>w.id), ['wNew']);
        t('profil unit menemukan laporan lewat unit KEDUA', workLogsForUnit('u_3','SN333').map(w=>w.id), ['wNew']);
        t('profil unit format lama tetap jalan', workLogsForUnit('u_1','SN111').map(w=>w.id), ['wOld']);
        t('unit tak terkait tidak muncul', workLogsForUnit('u_9','SN999').length, 0);

        // ---------- paddock ----------
        t('saran paddock dari data yang ada', allPaddocks(), ['Paddock B-12']);
        document.getElementById('wlSearch').value = 'b-12';
        renderWorkLogTable();
        t('pencarian menjangkau paddock', document.querySelectorAll('#workLogBody tr').length, 1);
        document.getElementById('wlSearch').value = '5045';
        renderWorkLogTable();
        t('pencarian menjangkau unit kedua', document.querySelectorAll('#workLogBody tr').length, 1);
        clearWorkLogFilter();

        // ---------- kolom tabel ----------
        const heads = [...document.querySelectorAll('#workLogTable thead th')].map(th=>th.textContent.trim());
        t('kepala tabel lengkap', heads,
          ['No','Tanggal','Anggota','Perusahaan','Jam Kerja','Unit','Paddock','Uraian Pekerjaan','Kendala','Persetujuan','Dokumentasi','Actions']);
        const rowNew = [...document.querySelectorAll('#workLogBody tr')].find(tr => tr.textContent.includes('Servis baru'));
        t('dua badge unit di satu baris', rowNew.querySelectorAll('td[data-label="Unit"] .badge').length, 2);
        t('paddock tampil', rowNew.querySelector('td[data-label="Paddock"]').textContent.trim(), 'Paddock B-12');
        // Foto tidak lagi dirender inline di tabel — hanya tombol berisi jumlahnya,
        // supaya membuka halaman ini tidak menarik satu pun gambar.
        t('tidak ada gambar inline di tabel', rowNew.querySelectorAll('td[data-label="Dokumentasi"] img').length, 0);
        t('tombol foto menampilkan jumlah',
          rowNew.querySelector('td[data-label="Dokumentasi"] .wl-photo-btn').textContent.trim(), '2');
        const jamCell = rowNew.querySelector('td[data-label="Jam Kerja"]');
        t('rentang jam di kolom Jam Kerja',
          jamCell.firstChild.textContent.trim(), '08:00–12:00');
        t('durasi di baris kedua kolom yang sama',
          jamCell.querySelector('.wl-duration').textContent.trim(), '4j');
        t('durasi memang ditampilkan sebagai blok terpisah',
          getComputedStyle(jamCell.querySelector('.wl-duration')).display, 'block');

        // ---------- form: chip unit ----------
        showAddWorkLogForm();
        t('form baru: chip kosong', _wlUnits.length, 0);
        t('placeholder chip muncul',
          !!document.querySelector('#wlUnitChips .unit-chip__empty'), true);
        document.getElementById('wlUnit').value = 'JD-6110B-01 — SN111';
        addWorkLogUnit();
        t('unit pertama masuk', _wlUnits.map(u=>u.id), ['u_1']);
        t('input dikosongkan setelah tambah', document.getElementById('wlUnit').value, '');
        document.getElementById('wlUnit').value = 'SN222';
        addWorkLogUnit();
        t('unit kedua masuk lewat SN', _wlUnits.map(u=>u.id), ['u_1','u_2']);
        document.getElementById('wlUnit').value = 'SN222';
        addWorkLogUnit();
        t('duplikat ditolak', _wlUnits.length, 2);
        document.getElementById('wlUnit').value = 'UNIT TIDAK ADA';
        addWorkLogUnit();
        t('unit tak dikenal ditolak', _wlUnits.length, 2);
        t('input tetap terisi agar bisa diperbaiki', document.getElementById('wlUnit').value, 'UNIT TIDAK ADA');
        document.getElementById('wlUnit').value = '';
        t('dua chip terender', document.querySelectorAll('#wlUnitChips .unit-chip').length, 2);
        removeWorkLogUnit(0);
        t('chip bisa dihapus', _wlUnits.map(u=>u.id), ['u_2']);

        // ---------- form: foto ----------
        _wlPhotos = ['data:image/jpeg;base64,X'];
        renderWorkLogPhotos();
        t('penghitung foto', document.getElementById('wlPhotoCount').textContent, '1/4');
        t('pratinjau foto terender', document.querySelectorAll('#wlPhotoPreviews .wl-photo').length, 1);
        removeWorkLogPhoto(0);
        t('foto bisa dihapus', _wlPhotos.length, 0);

        // ---------- simpan ----------
        _wlPhotos = ['data:image/jpeg;base64,Y'];
        document.getElementById('wlMember').value = 'm1';
        document.getElementById('wlDate').value = '2026-09-16';
        document.getElementById('wlStart').value = '07:00';
        document.getElementById('wlEnd').value = '15:00';
        document.getElementById('wlPaddock').value = 'Paddock C-3';
        document.getElementById('wlTask').value = 'Perawatan berkala';
        // Unit diketik tapi lupa ditekan Tambah — harus tetap ikut tersimpan.
        document.getElementById('wlUnit').value = 'JD-5045D-09 — SN333';
        saveWorkLog({ preventDefault(){} });
        const rec = saved[saved.length-1];
        t('unit yang lupa ditambahkan ikut tersimpan', rec.units.map(u=>u.id), ['u_2','u_3']);
        t('paddock tersimpan', rec.paddock, 'Paddock C-3');
        t('jumlah foto tersimpan di laporan', rec.photoCount, 1);
        t('foto tidak lagi ikut di dokumen laporan', rec.photos, []);
        t('foto ditulis ke koleksi terpisah',
          photoWrites[photoWrites.length-1].ph, ['data:image/jpeg;base64,Y']);
        t('id dokumen foto sama dengan id laporan',
          photoWrites[photoWrites.length-1].id, rec.id);
        t('unit pertama dicerminkan ke field lama', [rec.unitId, rec.sn], ['u_2','SN222']);

        // Unit yang tidak dikenal harus MEMBATALKAN simpan, bukan diam-diam hilang.
        const before = saved.length;
        showAddWorkLogForm();
        document.getElementById('wlMember').value = 'm1';
        document.getElementById('wlDate').value = '2026-09-16';
        document.getElementById('wlTask').value = 'Uji';
        document.getElementById('wlUnit').value = 'NGACO';
        saveWorkLog({ preventDefault(){} });
        t('unit ngaco membatalkan simpan', saved.length, before);

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
