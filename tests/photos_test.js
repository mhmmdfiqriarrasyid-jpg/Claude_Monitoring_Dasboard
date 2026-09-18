// Foto laporan harian dipisah ke koleksi sendiri (workLogPhotos) dan hanya
// dimuat saat dibuka. Yang diuji di sini terutama BUKAN tampilannya, melainkan
// apa yang sampai ke cloud — karena inti perubahannya adalah berapa banyak data
// yang ditarik dan ditulis, bukan bagaimana fotonya terlihat.
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
        const saved = [], photoWrites = [], fetched = [];
        let failNextFetch = false;

        const PH = i => 'data:image/jpeg;base64,' + i;
        const store = { wOld: null, wSplit: [PH('S1'), PH('S2'), PH('S3')] };

        window.cloud = { isReady: true,
            saveShift:()=>Promise.resolve(), deleteShift:()=>Promise.resolve(),
            saveTeamMember:()=>Promise.resolve(), deleteTeamMember:()=>Promise.resolve(),
            saveWorkLog: r => { saved.push(JSON.parse(JSON.stringify(r))); return Promise.resolve(); },
            deleteWorkLog:()=>Promise.resolve(),
            getWorkLogPhotos: id => {
                fetched.push(id);
                if (failNextFetch) { failNextFetch = false; return Promise.reject(new Error('offline')); }
                return Promise.resolve((store[id] || []).slice());
            },
            saveWorkLogPhotos: (id, ph) => { photoWrites.push({ id, ph: ph.slice() }); store[id] = ph.slice(); return Promise.resolve(); },
            deleteWorkLogPhotos: id => { photoWrites.push({ id, ph: null }); delete store[id]; return Promise.resolve(); } };

        currentUser = { uid:'u1', email:'o@x.id' };
        currentUserDoc = { role:'owner', status:'active', email:'o@x.id' };
        hideAuthGates(); applyRoleGating();
        globalData = [{ id:'u_1', name:'JD-6110B-01', sn:'SN111', site:'Timika', status:'Good' }];
        teamMembers = [{ id:'m1', name:'Andi', jobTitle:'Mekanik', company:'PT. Global Papua Abadi', active:true }];
        teamShifts = [];
        // wOld  : bentuk LAMA — foto masih di dalam dokumen laporan.
        // wSplit: bentuk BARU — hanya photoCount, foto di koleksi terpisah.
        // wNone : tanpa foto sama sekali.
        const mk = (id, extra) => Object.assign({
            id, date:'2026-09-15', memberId:'m1', memberName:'Andi',
            start:'08:00', end:'16:00', unitId:'u_1', unitName:'JD-6110B-01', sn:'SN111',
            task:'Servis ' + id, issue:'', createdByUid:'zz' }, extra);
        workLogs = [
            mk('wOld',   { photos:[PH('O1'), PH('O2')] }),
            mk('wSplit', { photoCount:3 }),
            mk('wNone',  {})
        ];
        teamWeekStart = '2026-09-14';
        localStorage.setItem('tractorWorkLogPhotosSplit', '1');  // migrasi diuji terpisah di bawah
        navigateTo('team'); switchTeamTab('worklog');

        // ---------- menghitung foto dari dua bentuk ----------
        t('bentuk lama: jumlah dari foto inline', workLogPhotoCount(workLogs[0]), 2);
        t('bentuk baru: jumlah dari photoCount', workLogPhotoCount(workLogs[1]), 3);
        t('tanpa foto', workLogPhotoCount(workLogs[2]), 0);
        t('laporan kosong tidak bikin error', workLogPhotoCount(null), 0);
        // Laporan yang sudah dimigrasi punya photos:[] DAN photoCount — array
        // kosong tidak boleh menutupi hitungan yang sebenarnya.
        t('photos kosong jatuh ke photoCount',
          workLogPhotoCount({ photos:[], photoCount:4 }), 4);

        // ---------- tabel tidak menarik satu gambar pun ----------
        const cellOf = id => [...document.querySelectorAll('#workLogBody tr')]
            .find(tr => tr.textContent.includes('Servis ' + id))
            .querySelector('td[data-label="Dokumentasi"]');
        t('tidak ada <img> di seluruh kolom dokumentasi',
          document.querySelectorAll('#workLogBody td[data-label="Dokumentasi"] img').length, 0);
        t('tombol bentuk lama menunjukkan 2', cellOf('wOld').querySelector('.wl-photo-btn').textContent.trim(), '2');
        t('tombol bentuk baru menunjukkan 3', cellOf('wSplit').querySelector('.wl-photo-btn').textContent.trim(), '3');
        t('tanpa foto: tidak ada tombol', !!cellOf('wNone').querySelector('.wl-photo-btn'), false);
        t('membuka halaman tidak mengambil foto apa pun', fetched.length, 0);

        // ---------- membuka foto: satu ambil, lalu dari cache ----------
        await openWorkLogPhotos('wSplit', cellOf('wSplit').querySelector('.wl-photo-btn'));
        t('sekali klik = sekali ambil', fetched, ['wSplit']);
        t('lightbox terbuka', document.getElementById('photoLightbox').classList.contains('open'), true);
        t('foto pertama tampil', document.getElementById('photoLightboxImg').src, PH('S1'));
        t('penghitung lightbox', document.getElementById('photoLightboxCount').textContent, '1 / 3');
        stepPhotoLightbox(1);
        t('maju satu foto', document.getElementById('photoLightboxImg').src, PH('S2'));
        stepPhotoLightbox(-1); stepPhotoLightbox(-1);
        t('mundur dari foto pertama memutar ke terakhir', document.getElementById('photoLightboxImg').src, PH('S3'));
        closePhotoLightbox();
        t('lightbox tertutup', document.getElementById('photoLightbox').classList.contains('open'), false);

        await openWorkLogPhotos('wSplit', cellOf('wSplit').querySelector('.wl-photo-btn'));
        t('klik kedua dilayani cache, tidak ambil ulang', fetched, ['wSplit']);
        closePhotoLightbox();

        // Bentuk lama sudah punya fotonya di memori — tidak perlu diambil sama sekali.
        await openWorkLogPhotos('wOld', cellOf('wOld').querySelector('.wl-photo-btn'));
        t('bentuk lama tidak perlu diambil dari cloud', fetched, ['wSplit']);
        t('satu foto: tombol navigasi disembunyikan',
          document.getElementById('photoLightboxPrev').style.display, '');
        closePhotoLightbox();

        // ---------- gagal memuat: dikatakan, bukan didiamkan ----------
        _wlPhotoCache.delete('wSplit');
        failNextFetch = true;
        const btn = cellOf('wSplit').querySelector('.wl-photo-btn');
        await openWorkLogPhotos('wSplit', btn);
        t('gagal memuat tidak membuka lightbox',
          document.getElementById('photoLightbox').classList.contains('open'), false);
        t('gagal memuat memberi tahu pengguna',
          [...document.querySelectorAll('#toastContainer .toast')].some(el => /foto/i.test(el.textContent)), true);
        t('tombol dipulihkan setelah gagal', btn.disabled, false);

        // ---------- menyunting teks TIDAK menyentuh foto ----------
        editWorkLog('wSplit');
        await new Promise(r => setTimeout(r, 50));
        t('foto lama ikut termuat ke form', _wlPhotos.length, 3);
        document.getElementById('wlTask').value = 'Uraian dibetulkan';
        const pwBefore = photoWrites.length;
        saveWorkLog({ preventDefault(){} });
        t('menyunting uraian tidak menulis ulang foto', photoWrites.length, pwBefore);
        t('jumlah foto dipertahankan', saved[saved.length-1].photoCount, 3);
        t('dokumen laporan tetap tanpa foto inline',
          'photos' in saved[saved.length-1], false);

        // ---------- menyimpan sebelum foto selesai dimuat ----------
        // Balapan yang paling berbahaya: kalau simpan menulis _wlPhotos apa
        // adanya, foto lama terhapus karena belum sempat termuat.
        _wlPhotoCache.delete('wSplit');
        editWorkLog('wSplit');           // sengaja TIDAK ditunggu
        document.getElementById('wlTask').value = 'Simpan buru-buru';
        const pwBefore2 = photoWrites.length;
        saveWorkLog({ preventDefault(){} });
        t('simpan sebelum foto termuat tidak menulis foto', photoWrites.length, pwBefore2);
        t('simpan buru-buru tetap mempertahankan jumlah foto', saved[saved.length-1].photoCount, 3);
        await new Promise(r => setTimeout(r, 50));

        // ---------- menghapus semua foto ----------
        editWorkLog('wSplit');
        await new Promise(r => setTimeout(r, 50));
        removeWorkLogPhoto(0); removeWorkLogPhoto(0); removeWorkLogPhoto(0);
        saveWorkLog({ preventDefault(){} });
        t('foto habis: dokumen fotonya dihapus, bukan ditulis kosong',
          photoWrites[photoWrites.length-1], { id:'wSplit', ph:null });
        t('photoCount ikut nol', saved[saved.length-1].photoCount, 0);

        // ---------- menghapus laporan ikut menghapus fotonya ----------
        const pwBefore3 = photoWrites.length;
        window.confirm = () => true;
        deleteWorkLog('wOld');
        t('hapus laporan ikut menghapus dokumen fotonya',
          photoWrites[photoWrites.length-1].id, 'wOld');
        t('tepat satu tulisan tambahan', photoWrites.length, pwBefore3 + 1);

        // ---------- CSV memakai jumlah, bukan isi foto ----------
        t('CSV bentuk baru menghitung dari photoCount',
          workLogPhotoCount({ photoCount:3 }), 3);

        // ---------- migrasi sekali jalan ----------
        localStorage.removeItem('tractorWorkLogPhotosSplit');
        photoWrites.length = 0; saved.length = 0;
        workLogs = [ mk('mA', { photos:[PH('A1')] }), mk('mB', { photoCount:2 }), mk('mC', { photos:[PH('C1'), PH('C2')] }) ];
        migrateWorkLogPhotosIfNeeded();
        await new Promise(r => setTimeout(r, 100));
        t('hanya laporan berfoto inline yang dipindah',
          photoWrites.map(w => w.id), ['mA', 'mC']);
        t('foto ditulis lebih dulu, laporannya menyusul',
          saved.map(r => r.id), ['mA', 'mC']);
        t('laporan yang dipindah kehilangan foto inline-nya',
          saved.map(r => r.photos), [[], []]);
        t('jumlahnya dipindah ke photoCount',
          saved.map(r => r.photoCount), [1, 2]);
        t('migrasi ditandai selesai',
          localStorage.getItem('tractorWorkLogPhotosSplit'), '1');

        photoWrites.length = 0;
        migrateWorkLogPhotosIfNeeded();
        await new Promise(r => setTimeout(r, 50));
        t('migrasi tidak jalan dua kali', photoWrites.length, 0);

        window.__T = T;
    } catch (e) { window.__T = [{n:'THREW: '+e.message+' | '+(e.stack||'').split('\\n')[1], g:1, w:0, pass:false}]; }
    })();` });

    await page.waitForFunction(() => window.__T, null, { timeout: 20000 });
    const res = await page.evaluate(() => window.__T);
    const fail = res.filter(r => !r.pass);
    res.forEach(r => console.log(`${r.pass ? 'PASS' : 'FAIL'}  ${r.n}${r.pass ? '' : `\n        dapat=${JSON.stringify(r.g)} harap=${JSON.stringify(r.w)}`}`));
    console.log(`\n${res.length - fail.length}/${res.length} lulus`);
    console.log('PAGE ERRORS:', errors.length ? errors : 'none');
    await b.close();
    process.exit(fail.length ? 1 : 0);
})();
