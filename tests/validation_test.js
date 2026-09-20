// Validasi saat input — menghentikan data kotor di pintu masuknya.
//
// Yang diuji terutama bukan pesan yang muncul, melainkan apakah data yang salah
// benar-benar TIDAK jadi tersimpan, dan apakah data yang benar tetap bisa lewat.
// Menolak terlalu banyak sama merugikannya dengan menerima terlalu banyak:
// shift malam yang sah harus tetap bisa dicatat.
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

        // confirm() dikendalikan supaya bisa menguji "diminta konfirmasi" vs
        // "ditolak langsung" — dua hal yang berbeda.
        let asked = [], answer = true;
        window.confirm = msg => { asked.push(String(msg)); return answer; };
        const freshAsk = () => { asked = []; };

        window.cloud = { isReady: true,
            saveUnits: () => Promise.resolve(), deleteUnits: () => Promise.resolve(),
            getAllUnits: () => Promise.resolve([]), addHistoryEvents: () => Promise.resolve(),
            saveWorkLog: () => Promise.resolve(), deleteWorkLog: () => Promise.resolve(),
            saveStockItem: () => Promise.resolve(), saveShift: () => Promise.resolve(),
            saveTeamMember: () => Promise.resolve() };

        currentUser = { uid: 'uO', email: 'o@x.id' };
        currentUserDoc = { role: 'owner', status: 'active', email: 'o@x.id' };
        hideAuthGates(); applyRoleGating();

        // ---------- jam laporan harian ----------
        // Pembungkusan ke +24 jam itu disengaja supaya shift malam benar. Yang
        // berbahaya adalah salah ketik yang memakai jalur yang sama.
        t('jam wajar lolos tanpa bertanya', [checkWorkLogHours('07:00','16:00'), asked.length], [true, 0]);
        t('dua-duanya kosong boleh', [checkWorkLogHours('',''), asked.length], [true, 0]);

        freshAsk();
        t('terisi sebelah ditolak, bukan ditanya', checkWorkLogHours('07:00',''), false);
        t('penolakan itu tidak memakai dialog', asked.length, 0);
        t('terisi sebelah (kebalikannya) juga ditolak', checkWorkLogHours('','16:00'), false);

        freshAsk(); answer = false;
        t('jam terbalik minta konfirmasi dulu', checkWorkLogHours('08:00','07:00'), false);
        t('dialognya menyebut berapa jam yang terbaca', /23j/.test(asked[0] || ''), true);
        freshAsk(); answer = true;
        // Shift malam yang sah harus tetap bisa disimpan — ini bukan larangan.
        t('kalau dikonfirmasi, tetap boleh', checkWorkLogHours('08:00','07:00'), true);
        t('dan memang ditanya sekali', asked.length, 1);

        freshAsk(); answer = false;
        t('mulai == selesai (0 jam) ditanya', checkWorkLogHours('08:00','08:00'), false);

        freshAsk(); answer = true;
        // Shift malam pendek tidak boleh ikut ditanyai — hanya yang tidak wajar.
        t('shift malam 8 jam lewat tanpa ditanya',
          [checkWorkLogHours('22:00','06:00'), asked.length], [true, 0]);
        t('dihitung 8 jam, bukan 16', workLogMinutes({ start:'22:00', end:'06:00' }), 480);

        // ---------- nomor seri duplikat ----------
        globalData = [
            { id:'u_1', name:'GGTR147G', sn:'SN-AAA', site:'PT. GPA', status:'Good' },
            { id:'u_2', name:'MGTR074M', sn:'SN-BBB', site:'PT. MNM', status:'Good' }
        ];
        t('SN bentrok ditolak', checkUnitFields('u_1', { sn:'SN-BBB' }), false);
        t('beda huruf besar-kecil tetap bentrok', checkUnitFields('u_1', { sn:'sn-bbb' }), false);
        t('SN milik unit itu sendiri boleh', checkUnitFields('u_1', { sn:'SN-AAA' }), true);
        t('SN baru boleh', checkUnitFields('u_1', { sn:'SN-CCC' }), true);
        t('SN kosong tidak dipersoalkan', checkUnitFields('u_1', { sn:'' }), true);
        t('unit baru dengan SN bentrok ditolak', checkUnitFields('', { sn:'SN-AAA' }), false);

        // ---------- urutan tanggal lisensi ----------
        t('habis sebelum mulai ditolak (GPS)',
          checkUnitFields('u_1', { gpsLicenseStartDate:'2026-06-01', gpsLicenseEndDate:'2026-01-01' }), false);
        t('habis sebelum mulai ditolak (Display)',
          checkUnitFields('u_1', { displayLicenseStartDate:'2026-06-01', displayLicenseEndDate:'2026-01-01' }), false);
        t('urutan benar lolos',
          checkUnitFields('u_1', { gpsLicenseStartDate:'2026-01-01', gpsLicenseEndDate:'2026-06-01' }), true);
        t('hanya salah satu terisi lolos',
          checkUnitFields('u_1', { gpsLicenseEndDate:'2026-06-01' }), true);

        // ---------- tanggal kejadian tidak boleh ke depan ----------
        capEventDatesToToday();
        const today = toISODate();
        t('empat input tanggal kejadian dibatasi hari ini',
          ['dmgDate','wlDate','stkDate','licDate'].map(i => document.getElementById(i).max),
          [today, today, today, today]);
        // Tanggal lisensi memang harus boleh ke depan — jangan ikut dibatasi.
        t('tanggal lisensi tidak ikut dibatasi',
          document.getElementById('formGpsLicenseEnd').getAttribute('max'), null);

        // ---------- saldo stok gudang ----------
        stockLedger = [
          { id:'s1', date:'2026-01-01', txnType:'IN',  itemName:'Filter Oli', qty:10, location:'Gudang A' },
          { id:'s2', date:'2026-01-02', txnType:'OUT', itemName:'Filter Oli', qty:4,  location:'Gudang A' }
        ];
        freshAsk(); answer = true;
        t('keluar dalam batas saldo tidak ditanya',
          [checkStockBalance('Filter Oli', 6, null), asked.length], [true, 0]);
        freshAsk(); answer = false;
        t('keluar melebihi saldo ditanya dulu', checkStockBalance('Filter Oli', 7, null), false);
        t('dialognya menyebut sisa saldonya', /tinggal 6/.test(asked[0] || ''), true);
        freshAsk(); answer = true;
        t('kalau dikonfirmasi tetap boleh', checkStockBalance('Filter Oli', 7, null), true);
        // Menyunting transaksi keluar yang sudah ada harus mengembalikan
        // jumlah lamanya, atau ia memperingatkan kekurangan yang ia buat sendiri.
        freshAsk();
        t('menyunting transaksi keluar mengembalikan jumlah lamanya',
          [checkStockBalance('Filter Oli', 10, stockLedger[1]), asked.length], [true, 0]);
        freshAsk(); answer = true;
        t('barang yang belum pernah masuk saldonya nol',
          [checkStockBalance('Barang Hantu', 1, null), /tinggal 0/.test(asked[0] || '')], [true, true]);

        // ---------- Escape tidak membuang isi form ----------
        document.getElementById('unitModal').classList.add('open');
        const input = document.getElementById('formName');
        input.value = 'SETENGAH DIKETIK';
        input.focus();
        input.dispatchEvent(new KeyboardEvent('keydown', { key:'Escape', bubbles:true }));
        t('Escape saat mengetik tidak menutup modal',
          document.getElementById('unitModal').classList.contains('open'), true);
        t('isinya masih ada', document.getElementById('formName').value, 'SETENGAH DIKETIK');
        // Di luar field, Escape tetap menutup seperti biasa.
        document.body.focus();
        document.body.dispatchEvent(new KeyboardEvent('keydown', { key:'Escape', bubbles:true }));
        t('Escape di luar field tetap menutup',
          document.getElementById('unitModal').classList.contains('open'), false);

        // ---------- foto laporan yang belum tersimpan ----------
        _wlPhotos = ['data:image/jpeg;base64,AAA', 'data:image/jpeg;base64,BBB'];
        _wlPhotosDirty = true;
        document.getElementById('workLogModal').classList.add('open');
        freshAsk(); answer = false;
        closeWorkLogModal();
        t('menutup dengan foto belum tersimpan minta konfirmasi',
          document.getElementById('workLogModal').classList.contains('open'), true);
        t('dialognya menyebut jumlah fotonya', /2 foto/.test(asked[0] || ''), true);
        answer = true;
        closeWorkLogModal();
        t('kalau dikonfirmasi, tertutup',
          document.getElementById('workLogModal').classList.contains('open'), false);
        freshAsk();
        _wlPhotos = ['data:image/jpeg;base64,CCC']; _wlPhotosDirty = true;
        document.getElementById('workLogModal').classList.add('open');
        closeWorkLogModal(true);
        t('jalur simpan menutup tanpa bertanya', asked.length, 0);

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
