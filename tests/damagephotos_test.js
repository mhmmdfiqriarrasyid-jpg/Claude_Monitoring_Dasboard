// Foto kerusakan dipisah ke koleksi sendiri (damagePhotos) dan hanya dimuat
// saat dibuka — pengobatan yang sama seperti foto laporan harian.
//
// Yang diuji terutama bukan tampilannya, melainkan apa yang SAMPAI ke cloud dan
// berapa banyak yang ditarik: inti perubahannya adalah berhenti mengunduh
// setiap foto di setiap perangkat setiap kali aplikasi dibuka, dan berhenti
// menimbun foto di localStorage sampai kuotanya jebol.
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
        window.confirm = () => true;

        const PH = i => 'data:image/jpeg;base64,' + i;
        const store = { dOld: null, dSplit: PH('SPLIT') };

        window.cloud = { isReady: true,
            saveDamage: r => { saved.push(JSON.parse(JSON.stringify(r))); return Promise.resolve(); },
            saveDamages: () => Promise.resolve(), deleteDamage: () => Promise.resolve(),
            getAllDamages: () => Promise.resolve([]),
            saveUnits: () => Promise.resolve(), getAllUnits: () => Promise.resolve([]),
            addHistoryEvents: () => Promise.resolve(), subscribeUsers: () => () => {},
            getDamagePhoto: id => {
                fetched.push(id);
                if (failNextFetch) { failNextFetch = false; return Promise.reject(new Error('offline')); }
                return Promise.resolve(store[id] || '');
            },
            saveDamagePhoto: (id, p) => { photoWrites.push({ id, p }); store[id] = p; return Promise.resolve(); },
            deleteDamagePhoto: id => { photoWrites.push({ id, p: null }); delete store[id]; return Promise.resolve(); } };

        currentUser = { uid:'uO', email:'o@x.id' };
        currentUserDoc = { role:'owner', status:'active', email:'o@x.id' };
        hideAuthGates(); applyRoleGating();
        globalData = [{ id:'u_1', name:'JD-6110B-01', sn:'SN111', site:'PT. GPA', status:'Good' }];

        const mk = (id, extra) => Object.assign({
            id, date:'2026-09-01', unitId:'u_1', unitName:'JD-6110B-01', sn:'SN111',
            site:'PT. GPA', damageType:'Mekanis', component:'', description:'Rusak ' + id,
            resolved:false, resolvedAt:'', createdAt:1, updatedAt:1 }, extra);
        // dOld  : bentuk LAMA — foto masih di dalam dokumennya.
        // dSplit: bentuk BARU — hanya hasPhoto, fotonya di koleksi terpisah.
        // dNone : tanpa foto.
        globalDamages = [
            mk('dOld',   { photo: PH('OLD') }),
            mk('dSplit', { hasPhoto: true }),
            mk('dNone',  {})
        ];
        localStorage.setItem('tractorDamagePhotosSplit', '1');  // migrasi diuji terpisah
        // navigateTo('damage') memanggil loadDamages(), yang menimpa
        // globalDamages dari localStorage — jadi pasang datanya SESUDAH pindah
        // halaman, bukan sebelumnya.
        const seed = globalDamages.map(d => ({ ...d }));
        navigateTo('damage');
        globalDamages = seed.map(d => ({ ...d }));
        populateDamageUnitSelect();
        renderDamageTable();

        // ---------- mengenali dua bentuk ----------
        t('bentuk lama: foto inline terbaca', damageHasPhoto(globalDamages[0]), true);
        t('bentuk baru: dari hasPhoto', damageHasPhoto(globalDamages[1]), true);
        t('tanpa foto', damageHasPhoto(globalDamages[2]), false);
        t('catatan kosong tidak bikin error', damageHasPhoto(null), false);
        // Catatan yang sudah dimigrasi punya photo:'' DAN hasPhoto — string
        // kosong tidak boleh menutupi kenyataan bahwa fotonya ada.
        t('photo kosong jatuh ke hasPhoto',
          damageHasPhoto({ photo:'', hasPhoto:true }), true);

        // ---------- tabel tidak menarik satu gambar pun ----------
        // Ini inti perubahannya: dulu setiap baris menanam base64 ke DOM.
        t('tidak ada gambar inline di tabel kerusakan',
          document.querySelectorAll('#damageTable img').length, 0);
        t('membuka halaman tidak mengambil foto apa pun', fetched.length, 0);

        // ---------- diambil hanya saat dibuka, lalu di-cache ----------
        await openDamagePhoto('dSplit', null);
        t('sekali klik = sekali ambil', fetched, ['dSplit']);
        t('lightbox terbuka',
          document.getElementById('photoLightbox').classList.contains('open'), true);
        t('gambarnya yang benar',
          document.getElementById('photoLightboxImg').src, PH('SPLIT'));
        closePhotoLightbox();
        await openDamagePhoto('dSplit', null);
        t('klik kedua dilayani cache, tidak menarik ulang', fetched.length, 1);
        closePhotoLightbox();

        // Catatan lama tidak perlu jaringan sama sekali — fotonya sudah ada.
        await openDamagePhoto('dOld', null);
        t('foto inline lama tidak perlu diambil dari cloud', fetched.length, 1);
        t('dan tetap tampil', document.getElementById('photoLightboxImg').src, PH('OLD'));
        closePhotoLightbox();

        // ---------- gagal memuat dikatakan, bukan didiamkan ----------
        _dmgPhotoCache.delete('dSplit');
        failNextFetch = true;
        const btn = document.createElement('button');
        btn.innerHTML = 'X';
        await openDamagePhoto('dSplit', btn);
        t('gagal memuat tidak membuka lightbox',
          document.getElementById('photoLightbox').classList.contains('open'), false);
        t('tombolnya dipulihkan', [btn.disabled, btn.innerHTML], [false, 'X']);

        // ---------- menyunting uraian tidak menyentuh fotonya ----------
        // Tanpa aturan ini satu koreksi teks bisa menghapus foto yang belum
        // selesai dimuat.
        photoWrites.length = 0; saved.length = 0;
        editDamage('dSplit');
        await new Promise(r => setTimeout(r, 100));
        document.getElementById('dmgDescription').value = 'Uraian dibetulkan';
        saveDamage({ preventDefault(){} });
        await new Promise(r => setTimeout(r, 100));
        t('tidak ada tulisan foto sama sekali', photoWrites.length, 0);
        const edited = globalDamages.find(d => d.id === 'dSplit');
        t('hasPhoto dipertahankan', edited.hasPhoto, true);
        t('uraiannya memang berubah', edited.description, 'Uraian dibetulkan');

        // ---------- menyimpan sebelum foto selesai dimuat ----------
        // Balapan yang paling mudah merusak: modal terbuka, fetch masih jalan,
        // pengguna menekan Simpan.
        _dmgPhotoCache.delete('dSplit');
        photoWrites.length = 0;
        editDamage('dSplit');
        saveDamage({ preventDefault(){} });   // tanpa menunggu
        await new Promise(r => setTimeout(r, 150));
        t('simpan terburu-buru tidak menulis foto', photoWrites.length, 0);
        t('dan fotonya tetap tercatat ada',
          globalDamages.find(d => d.id === 'dSplit').hasPhoto, true);

        // ---------- mengganti foto ----------
        photoWrites.length = 0;
        editDamage('dNone');
        await new Promise(r => setTimeout(r, 50));
        _dmgPhotoData = PH('BARU'); _dmgPhotoDirty = true;
        saveDamage({ preventDefault(){} });
        await new Promise(r => setTimeout(r, 100));
        t('foto baru ditulis ke koleksi terpisah',
          photoWrites.map(w => [w.id, w.p]), [['dNone', PH('BARU')]]);
        const withNew = globalDamages.find(d => d.id === 'dNone');
        t('catatannya hanya menyimpan penanda', withNew.hasPhoto, true);
        t('dan tidak menyimpan gambarnya', withNew.photo, '');

        // ---------- membuang foto ----------
        photoWrites.length = 0;
        editDamage('dNone');
        await new Promise(r => setTimeout(r, 50));
        removeDamagePhoto();
        saveDamage({ preventDefault(){} });
        await new Promise(r => setTimeout(r, 100));
        t('membuang foto menghapus dokumennya',
          photoWrites.map(w => [w.id, w.p]), [['dNone', null]]);
        t('penandanya ikut dimatikan',
          globalDamages.find(d => d.id === 'dNone').hasPhoto, false);

        // ---------- menghapus catatan ikut menghapus fotonya ----------
        photoWrites.length = 0;
        deleteDamage('dSplit');
        await new Promise(r => setTimeout(r, 100));
        t('hapus catatan = hapus fotonya juga',
          photoWrites.map(w => w.id), ['dSplit']);

        // ---------- jatah kompresi disamakan dengan laporan ----------
        t('jatah foto kerusakan = jatah foto laporan',
          [DAMAGE_PHOTO_MAX_DIM, DAMAGE_PHOTO_MAX_BYTES],
          [WORKLOG_PHOTO_OPTS.maxDim, WORKLOG_PHOTO_OPTS.maxBytes]);

        // ---------- cadangan otomatis yang gagal harus bersuara ----------
        // Dulu ditelan diam-diam: cadangan berhenti diperbarui sementara owner
        // mengira punya tiga titik pulih.
        const toasts = [];
        const realToast = window.showToast;
        window.showToast = (m, k) => { toasts.push(k + ':' + m); };
        const realSet = localStorage.setItem.bind(localStorage);
        localStorage.setItem = (k, v) => {
            if (k === 'tractorUnits_autobackup') { const e = new Error('quota'); e.name = 'QuotaExceededError'; throw e; }
            return realSet(k, v);
        };
        _autoBackupFailed = false;
        writeAutoBackup([{ id:'u_1' }]);
        t('kegagalan cadangan diberitahukan',
          toasts.some(x => /Cadangan otomatis berhenti/.test(x)), true);
        const n1 = toasts.length;
        writeAutoBackup([{ id:'u_1' }]);
        t('tapi tidak diulang-ulang jadi dinding toast', toasts.length, n1);
        localStorage.setItem = realSet;
        window.showToast = realToast;

        // ---------- migrasi foto lama ----------
        localStorage.removeItem('tractorDamagePhotosSplit');
        globalDamages = [mk('mA', { photo: PH('A') }), mk('mB'), mk('mC', { photo: PH('C') })];
        photoWrites.length = 0; saved.length = 0;
        migrateDamagePhotosIfNeeded();
        await new Promise(r => setTimeout(r, 150));
        t('hanya catatan berfoto inline yang dipindah',
          photoWrites.map(w => w.id), ['mA', 'mC']);
        // Urutannya penting: foto ditulis dulu, salinan inline dibersihkan
        // sesudahnya. Terputus di tengah, fotonya ada di dua tempat.
        t('foto ditulis lebih dulu, catatannya menyusul',
          saved.map(r => r.id), ['mA', 'mC']);
        t('catatan yang dipindah kehilangan foto inline-nya',
          saved.map(r => r.photo), ['', '']);
        t('penandanya dipasang', saved.map(r => r.hasPhoto), [true, true]);
        t('migrasi ditandai selesai',
          localStorage.getItem('tractorDamagePhotosSplit'), '1');
        photoWrites.length = 0;
        migrateDamagePhotosIfNeeded();
        await new Promise(r => setTimeout(r, 50));
        t('migrasi tidak jalan dua kali', photoWrites.length, 0);

        window.__T = T;
    } catch (e) { window.__T = [{n:'THREW: '+e.message+' | '+(e.stack||'').split('\\n')[1], g:1, w:0, pass:false}]; }
    })();` });

    await page.waitForFunction(() => window.__T, null, { timeout: 25000 });
    const res = await page.evaluate(() => window.__T);
    const fail = res.filter(r => !r.pass);
    res.forEach(r => console.log(`${r.pass ? 'PASS' : 'FAIL'}  ${r.n}${r.pass ? '' : `\n        dapat=${JSON.stringify(r.g)} harap=${JSON.stringify(r.w)}`}`));
    console.log(`\n${res.length - fail.length}/${res.length} lulus`);
    console.log('PAGE ERRORS:', errors.length ? errors : 'none');
    await b.close();
    process.exit(fail.length ? 1 : 0);
})();
