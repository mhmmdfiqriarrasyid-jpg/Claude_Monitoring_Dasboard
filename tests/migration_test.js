// Empat migrasi Excel yang berjalan otomatis di perangkat siapa pun sudah
// dihapus, dan jalur tulis unit sekarang terkunci sendiri.
//
// Yang diuji di sini bukan tampilan melainkan apa yang SAMPAI ke cloud dan apa
// yang MASUK ke riwayat — karena itulah dua hal yang dulu berbohong: unitnya
// ditolak server, tetapi catatannya tetap tertulis seolah berhasil.
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
        const unitWrites = [], histWrites = [];
        let rejectUnits = null;
        // Salinan "milik server", terpisah dari globalData. Tanpa pemisahan ini
        // tidak mungkin menguji penarikan ulang: globalData sudah berubah
        // duluan secara optimistis sebelum server sempat menolak.
        let serverUnits = [];

        const mkUnit = (id, extra) => Object.assign({
            id, name: 'GGTR' + id, sn: 'SN' + id, model: 'JD-6110B', site: 'PT. GPA',
            status: 'Good', gps: 'Good', steering: 'Good', jdlink: 'Good', display: 'Good'
        }, extra);

        window.cloud = { isReady: true,
            saveUnits: u => { unitWrites.push(u.map(x => x.id));
                              if (rejectUnits) return Promise.reject(rejectUnits);
                              u.forEach(x => { const i = serverUnits.findIndex(s => s.id === x.id);
                                               if (i < 0) serverUnits.push({ ...x }); else serverUnits[i] = { ...x }; });
                              return Promise.resolve(); },
            deleteUnits: ids => { unitWrites.push(['del:' + ids.join(',')]);
                                  if (rejectUnits) return Promise.reject(rejectUnits);
                                  serverUnits = serverUnits.filter(s => !ids.includes(s.id));
                                  return Promise.resolve(); },
            getAllUnits: () => Promise.resolve(serverUnits.map(u => ({ ...u }))),
            addHistoryEvents: ev => { ev.forEach(e => histWrites.push(e)); return Promise.resolve(); },
            getAllHistory: () => Promise.resolve(histWrites.slice()) };

        currentUser = { uid: 'u1', email: 'o@x.id' };
        currentUserDoc = { role: 'owner', status: 'active', email: 'o@x.id' };
        hideAuthGates(); applyRoleGating();

        // ---------- migrasi benar-benar hilang ----------
        // Ini yang mencabut bomnya: selama fungsinya masih ada dan masih
        // dipanggil dari applyCloudUnitsSnapshot, satu browser baru cukup
        // untuk menimpa 366 field dengan data Excel Mei 2026.
        t('migrasi nickname tidak ada lagi', typeof window.migrateNicknamesFromExcel, 'undefined');
        t('migrasi lisensi batch 1 tidak ada lagi', typeof window.migrateLicenseDataBatch1, 'undefined');
        t('migrasi lisensi batch 2 tidak ada lagi', typeof window.migrateLicenseDataBatch2, 'undefined');
        t('migrasi site tidak ada lagi', typeof window.migrateSiteData20260525, 'undefined');
        t('dua migrasi yang aman tetap ada',
          [typeof applyDefaultLicensesIfNeeded, typeof applyLicenseDatesIfNeeded],
          ['function', 'function']);

        // Snapshot unit tidak boleh lagi memicu tulisan apa pun dari migrasi.
        globalData = [mkUnit('A', { name: 'GGTR147G_OHZ' }), mkUnit('B', { name: 'MGTR074M_HBA' })];
        localStorage.setItem('tractorLicenseDefaultsApplied', '1');
        localStorage.setItem('tractorLicenseDatesApplied_v2', '1');
        unitWrites.length = 0; histWrites.length = 0;
        applyCloudUnitsSnapshot(globalData.map(u => ({ ...u })));
        await new Promise(r => setTimeout(r, 200));
        t('snapshot unit tidak menulis apa pun', unitWrites.length, 0);
        t('snapshot unit tidak mencatat riwayat', histWrites.length, 0);
        t('nama unit tidak tersentuh',
          globalData.map(u => u.name), ['GGTR147G_OHZ', 'MGTR074M_HBA']);

        // ---------- penulis unit terkunci sendiri ----------
        // Dulu semua penguncian ada di pemanggil, jadi pemanggil baru (seperti
        // migrasi) mewarisi nol perlindungan.
        currentUserDoc = { role: 'pbt', status: 'active', email: 'y@x.id', access: { editUnits: 'view', teamLog: 'edit' } };
        applyRoleGating();
        unitWrites.length = 0; histWrites.length = 0;

        t('updateUnit ditolak untuk akses lihat-saja', updateUnit('A', { name: 'DIUBAH' }), false);
        t('nama unit tidak berubah', globalData[0].name, 'GGTR147G_OHZ');
        await new Promise(r => setTimeout(r, 200));
        // Inti keluhan owner: baris riwayat muncul padahal tulisannya ditolak.
        t('tidak ada catatan riwayat dari tulisan yang ditolak', histWrites.length, 0);
        t('tidak ada tulisan ke cloud', unitWrites.length, 0);

        t('addUnits ditolak', addUnits([mkUnit('C')]).added, 0);
        t('deleteUnits ditolak', deleteUnits(['A']).count, 0);
        t('unit tidak jadi terhapus', globalData.length, 2);
        t('bulkUpdateUnitsFromCSV ditolak',
          bulkUpdateUnitsFromCSV([{ sn: 'SNA', name: 'X' }]).updated, 0);
        await new Promise(r => setTimeout(r, 200));
        t('semua jalur unit senyap ke cloud', unitWrites.length, 0);
        t('semua jalur unit senyap ke riwayat', histWrites.length, 0);

        // ---------- tulisan yang ditolak server dicatat sebagai gagal ----------
        currentUserDoc = { role: 'owner', status: 'active', email: 'o@x.id' };
        applyRoleGating();
        rejectUnits = Object.assign(new Error('denied'), { code: 'permission-denied' });
        serverUnits = globalData.map(u => ({ ...u }));   // server masih memegang nama lama
        unitWrites.length = 0; histWrites.length = 0;
        updateUnit('A', { name: 'GGTR147G_SCL' });
        await new Promise(r => setTimeout(r, 400));

        const upd = histWrites.filter(e => e.action === 'update');
        const errs = histWrites.filter(e => e.action === 'error');
        t('perubahan tetap dicatat lebih dulu (tahan offline)', upd.length, 1);
        t('penolakan meninggalkan catatan gagal', errs.length, 1);
        t('catatan gagal menyebut kodenya',
          /GAGAL \\(permission-denied\\)/.test(errs[0] ? errs[0].after : ''), true);
        // Tanpa ini layar terus menampilkan nilai yang tidak ada di mana pun.
        t('data lokal ditarik ulang dari server', globalData[0].name, 'GGTR147G_OHZ');

        rejectUnits = null;

        // ---------- perubahan semu tidak dicatat ----------
        // Nilai dari backup JSON bertipe angka, dari form bertipe teks. Strict
        // !== menganggapnya berubah, lalu logEvent memaksa keduanya ke String
        // sehingga barisnya terbaca "2020 → 2020" di layar.
        globalData = [mkUnit('A', { yearReceived: 2020 })];
        histWrites.length = 0;
        updateUnit('A', { yearReceived: '2020' });
        await new Promise(r => setTimeout(r, 200));
        t('angka vs teks yang sama tidak dicatat sebagai perubahan',
          histWrites.filter(e => e.field === 'yearReceived').length, 0);

        histWrites.length = 0;
        updateUnit('A', { yearReceived: '2021' });
        await new Promise(r => setTimeout(r, 200));
        t('perubahan sungguhan tetap dicatat',
          histWrites.filter(e => e.field === 'yearReceived').length, 1);

        // ---------- penurunan lisensi tidak lagi menghapus tanggal ----------
        // Dulu tanggalnya dikosongkan, jadi tidak ada lagi jejak kapan tier
        // premium habis — dan tidak bisa dibedakan dari unit yang memang tidak
        // pernah punya lisensi.
        globalData = [mkUnit('L', { gpsLicense: 'SF-RTK', gpsLicenseStartDate: '2020-01-01',
                                    gpsLicenseEndDate: '2020-06-01',
                                    licenseDisplay: 'G5 Advance', displayLicenseEndDate: '2020-06-01' })];
        applyExpiredLicenseDowngrades();
        const L = globalData[0];
        t('lisensi GPS turun ke fallback', L.gpsLicense, 'SF-1');
        t('tanggal GPS lama diarsipkan', L.gpsLicenseExpiredAt, '2020-06-01');
        t('tanggal GPS aktif dikosongkan', L.gpsLicenseEndDate, '');
        t('lisensi display turun ke fallback', L.licenseDisplay, 'G5 Basic');
        t('tanggal display lama diarsipkan', L.displayLicenseExpiredAt, '2020-06-01');
        // Badge-nya jadi jujur lagi: sebelum ini, unit yang sudah ditulis-turun
        // tidak lagi terbaca sebagai hasil penurunan otomatis.
        const eff = effectiveLicense(L, 'gps');
        t('badge tetap menandai unit ini hasil penurunan', eff.downgraded, true);
        t('badge menyebut tanggal habisnya', eff.end, '2020-06-01');
        // Unit yang memang lahir di tier fallback tidak boleh ikut ditandai.
        t('unit fallback biasa tidak ditandai turun',
          effectiveLicense(mkUnit('N', { gpsLicense: 'SF-1' }), 'gps').downgraded, false);

        // ---------- impor CSV berhenti kehilangan data diam-diam ----------
        // Dulu penolakan cloud hanya jadi baris console dan toast
        // "data tersimpan lokal" — tidak benar: barisnya ada di perangkat itu
        // saja, tampil seperti implement biasa, lalu lenyap saat snapshot
        // berikutnya tanpa penjelasan.
        const pesanImpor = [];
        const toastLama2 = window.showToast;
        window.showToast = m => { pesanImpor.push(String(m)); };

        let tolakImpor = true;
        window.cloud = { isReady: true,
            saveImplements: () => tolakImpor
                ? Promise.reject(Object.assign(new Error('nope'), { code: 'permission-denied' }))
                : Promise.resolve(),
            getAllImplements: () => Promise.resolve([{ id: 'srv1', profileName: 'DariServer' }]),
            addHistoryEvents: () => Promise.resolve(), subscribeUsers: () => () => {} };
        currentUser = { uid: 'uO', email: 'o@x.id' };
        currentUserDoc = { role: 'owner', status: 'active' };
        applyRoleGating();

        // Papa Parse datang dari CDN yang diblokir di sandbox ini, jadi yang
        // diuji adalah jalur SESUDAH parsing — tempat cacatnya berada.
        window.Papa = { parse: (file, opts) => opts.complete({
            data: [{ 'Profile Name': 'Plough Baru', 'Brand': 'Deere' }] }) };

        globalImplements = [{ id: 'lama1', profileName: 'Sudah Ada' }];
        const csv = 'x';
        await new Promise(res => {
            handleImplementCSVImport(new File([csv], 'i.csv', { type: 'text/csv' }));
            setTimeout(res, 600);
        });
        t('baris yang ditolak server tidak tertinggal di layar',
          globalImplements.filter(o => o.profileName === 'Plough Baru').length, 0);
        t('yang sudah ada sebelumnya tidak ikut hilang',
          globalImplements.some(o => o.id === 'lama1' || o.id === 'srv1'), true);
        t('penolakan meninggalkan baris audit GAGAL',
          getAuditLog().some(e => e.action === 'error' && /GAGAL/.test(e.after || '')), true);
        t('dan toastnya tidak lagi berbunyi "tersimpan lokal"',
          pesanImpor.some(m => /tersimpan lokal/i.test(m)), false);
        t('melainkan mengatakan perubahannya dikembalikan',
          pesanImpor.some(m => /dikembalikan/i.test(m)), true);

        // Dan kalau cloudnya menerima, barisnya memang harus tinggal.
        tolakImpor = false;
        globalImplements = [];
        await new Promise(res => {
            handleImplementCSVImport(new File([csv], 'i.csv', { type: 'text/csv' }));
            setTimeout(res, 600);
        });
        t('impor yang diterima tetap tersimpan',
          globalImplements.filter(o => o.profileName === 'Plough Baru').length, 1);
        window.showToast = toastLama2;

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
