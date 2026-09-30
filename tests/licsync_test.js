// "Sync ke Unit" — distribusi lisensi → lisensi unit.
//
// Sampai v114 tombol ini menulis ulang SETIAP unit yang pernah menerima
// distribusi (55 tulisan untuk membetulkan 3), di balik satu konfirmasi, dan
// tidak punya satu uji pun. Tiga akibatnya yang diuji di sini:
//   1. unit yang sudah sesuai ikut ditulis (dan tercatat di riwayat);
//   2. unit yang diperpanjang manual (masa berlaku lebih lama) DITIMPA mundur;
//   3. unit yang sudah turun otomatis ke SF-1 dikembalikan ke SF-RTK yang
//      sudah habis, lalu diturunkan lagi — dua tulisan sia-sia per unit.
// Sekarang: rencana berbasis selisih, pratinjau, dan hanya baris yang
// dicentang yang ditulis.
const { launch, BASE_URL } = require('./_env');

(async () => {
    const b = await launch();
    const page = await b.newPage({ viewport: { width: 1440, height: 900 } });
    await page.clock.setFixedTime('2026-09-30T08:00:00');
    const errors = [];
    page.on('pageerror', e => { if (!/Chart is not defined/.test(e.message)) errors.push(e.message); });
    await page.goto(BASE_URL + '/index.html', { waitUntil: 'load' });
    await page.waitForTimeout(1000);

    await page.addScriptTag({ content: `(async () => { try {
        const T = []; const t = (n, g, w) => T.push({ n, g, w, pass: JSON.stringify(g) === JSON.stringify(w) });
        const toasts = [];
        window.showToast = (m, k) => { toasts.push(String(m)); };
        let confirmAnswer = true;
        const asked = [];
        window.confirm = m => { asked.push(String(m)); return confirmAnswer; };
        const writes = [];
        window.cloud = { isReady: true,
            saveUnits: u => { writes.push(...JSON.parse(JSON.stringify(u))); return Promise.resolve(); },
            deleteUnits: () => Promise.resolve(), getAllUnits: () => Promise.resolve([]),
            addHistoryEvents: () => Promise.resolve(), subscribeUsers: () => () => {},
            saveLicense: () => Promise.resolve(), saveLicenses: () => Promise.resolve(), deleteLicense: () => Promise.resolve() };
        currentUser = { uid: 'uO', email: 'o@x.id' };
        currentUserDoc = { role: 'owner', status: 'active', email: 'o@x.id' };
        try { localStorage.setItem('tractorLicenseDatesApplied_v2', '1'); } catch (_) {}
        hideAuthGates(); applyRoleGating();
        navigateTo('licenseStock');

        const U = (id, x) => Object.assign({ id, name: 'GGTR' + id, sn: 'SN' + id, status: 'Good',
            display: 'Good', gps: 'Good', steering: 'Good', jdlink: 'Good', gpsLicense: '', licenseDisplay: '',
            gpsLicenseStartDate: '', gpsLicenseEndDate: '', displayLicenseStartDate: '', displayLicenseEndDate: '' }, x || {});
        let seq = 0;
        const OUT = (unitId, type, date, x) => Object.assign({ id: 'L' + (++seq), txnType: 'OUT', licenseType: type,
            qty: 1, unitId, unitName: 'GGTR' + unitId, sn: 'SN' + unitId, date, createdAt: seq, note: '' }, x || {});
        const seed = (units, recs) => {
            globalData = units; saveToStorage(globalData);
            globalLicenseStock = recs;
            writes.length = 0; toasts.length = 0;
        };
        const plan = () => licenseSyncPlan().rows.map(r => r.unit.id + ':' + r.kind + ':' + r.status);
        const openPreview = () => { syncDistributionsToUnits(); return document.getElementById('licSyncModal').classList.contains('open'); };
        const previewRows = () => [...document.querySelectorAll('#licSyncBody tr')].map(tr => ({
            checked: tr.querySelector('.lic-sync-check').checked, text: tr.textContent.replace(/\\s+/g, ' ').trim() }));

        // =============== 1. YANG SUDAH SESUAI TIDAK DISENTUH ===============
        seed([U('a', { gpsLicense: 'SF-RTK', gpsLicenseStartDate: '2026-09-01', gpsLicenseEndDate: '2027-09-01' }),
              U('b', { licenseDisplay: 'G5 Advance', displayLicenseStartDate: '2026-08-01', displayLicenseEndDate: '2027-08-01' }),
              U('c')],
             [OUT('a', 'SF-RTK', '2026-09-01'), OUT('b', 'G5 Advance', '2026-08-01'), OUT('c', 'SF-RTK', '2026-09-15')]);
        t('rencana: dua sesuai, satu perlu diperbarui', plan(), ['a:gps:sync', 'b:display:sync', 'c:gps:update']);
        renderLicenseStockTable();
        t('tombol Sync menunjukkan jumlah yang belum sesuai', [document.getElementById('licSyncCount').textContent,
            document.getElementById('licSyncCount').hidden], ['1', false]);
        t('tombol Sync muncul saat ada yang belum sesuai', document.getElementById('licSyncBtn').style.display, '');
        t('pratinjau terbuka', openPreview(), true);
        t('pratinjau hanya memuat yang berbeda', previewRows().length, 1);
        t('ringkasannya menyebut yang tidak disentuh', /2 sudah sesuai dan tidak disentuh/.test(document.getElementById('licSyncSummary').textContent), true);
        applyLicSyncSelection();
        t('hanya SATU unit yang ditulis', writes.map(u => u.id), ['c']);
        t('unit itu mendapat lisensinya', [globalData[2].gpsLicense, globalData[2].gpsLicenseStartDate, globalData[2].gpsLicenseEndDate],
          ['SF-RTK', '2026-09-15', '2027-09-15']);
        t('sesudahnya tombol tanpa angka', document.getElementById('licSyncCount').hidden, true);
        t('dan tombol Sync tersembunyi selama tidak ada yang perlu disinkron', document.getElementById('licSyncBtn').style.display, 'none');
        writes.length = 0; toasts.length = 0;
        t('sync kedua kali: tidak ada pratinjau', openPreview(), false);
        t('dan tidak ada tulisan', writes.length, 0);
        t('dan pesannya bilang semua sudah sesuai', /Semua 3 lisensi unit sudah sesuai/.test(toasts.join('|')), true);

        // =============== 2. UNIT YANG DIPERPANJANG MANUAL ===============
        seed([U('a', { gpsLicense: 'SF-RTK', gpsLicenseStartDate: '2026-09-20', gpsLicenseEndDate: '2027-09-20' })],
             [OUT('a', 'SF-RTK', '2026-03-01')]);
        t('unit berlaku lebih lama dari distribusi terbarunya: "newer"', plan(), ['a:gps:newer']);
        openPreview();
        t('ditampilkan TIDAK dicentang', previewRows().map(r => r.checked), [false]);
        t('tombol Terapkan nonaktif', document.getElementById('licSyncApplyBtn').disabled, true);
        applyLicSyncSelection();
        t('tidak ditimpa mundur', [writes.length, globalData[0].gpsLicenseEndDate], [0, '2027-09-20']);
        openPreview();
        document.querySelector('#licSyncBody .lic-sync-check').click();
        t('kalau dicentang, tombolnya aktif', document.getElementById('licSyncApplyBtn').disabled, false);
        applyLicSyncSelection();
        t('dan baru ditulis kalau memang dicentang', globalData[0].gpsLicenseEndDate, '2027-03-01');

        // =============== 3. TIER YANG SUDAH TURUN OTOMATIS ===============
        // Distribusi SF-RTK 2025-06-01 → habis 2026-06-01 (sebelum "hari ini").
        seed([U('a', { gpsLicense: 'SF-1', gpsLicenseStartDate: '2025-06-01', gpsLicenseEndDate: '', gpsLicenseExpiredAt: '2026-06-01' })],
             [OUT('a', 'SF-RTK', '2025-06-01')]);
        t('unit yang sudah turun ke SF-1 dari distribusi itu: sesuai', plan(), ['a:gps:sync']);
        openPreview();
        t('tidak dikembalikan ke SF-RTK', writes.length, 0);
        // Unit yang BELUM pernah diisi, dari distribusi yang sudah habis:
        // langsung ditulis dalam keadaan akhirnya, bukan SF-RTK yang mati.
        seed([U('b')], [OUT('b', 'SF-RTK', '2025-06-01')]);
        openPreview(); applyLicSyncSelection();
        t('distribusi habis ditulis langsung sebagai SF-1 dengan tanggal habis tersimpan',
          [globalData[0].gpsLicense, globalData[0].gpsLicenseEndDate, globalData[0].gpsLicenseExpiredAt],
          ['SF-1', '', '2026-06-01']);
        t('badge tipe menampilkan "turun otomatis"', effectiveLicense(globalData[0], 'gps').downgraded, true);
        writes.length = 0;
        applyExpiredLicenseDowngrades();
        t('penurunan otomatis tidak menulis apa-apa lagi', writes.length, 0);
        t('dan sync sesudahnya: sesuai', plan(), ['b:gps:sync']);

        // =============== 4. PEMILIHAN "TERBARU" DAN RESOLUSI UNIT ===============
        seed([U('a')], [OUT('a', 'SF-1', '2026-01-01'), OUT('a', 'SF-RTK', '2026-09-01'), OUT('a', 'G5 Basic', '2026-02-02')]);
        t('per unit + jenis dipilih yang TERBARU', licenseSyncPlan().rows.map(r => r.kind + ':' + r.rec.licenseType), ['display:G5 Basic', 'gps:SF-RTK']);
        openPreview(); applyLicSyncSelection();
        t('GPS dan Display unit yang sama = SATU tulisan', writes.length, 1);
        // Tanggal sama: createdAt lalu id yang memutus, supaya hasilnya stabil.
        seed([U('a')], [OUT('a', 'SF-1', '2026-09-01', { id: 'Z9', createdAt: 5 }), OUT('a', 'SF-RTK', '2026-09-01', { id: 'A1', createdAt: 5 })]);
        t('tanggal & createdAt sama: id yang memutus', licenseSyncPlan().rows[0].rec.id, 'Z9');
        // Unit dihapus lalu diimpor ulang: id baru, SN sama.
        seed([U('baru', { sn: 'SNlama' })], [OUT('lama', 'SF-RTK', '2026-09-01', { sn: 'SNlama' })]);
        t('distribusi ke unit yang diimpor ulang tetap ditemukan lewat SN', plan(), ['baru:gps:update']);
        // Yang dilewati, dengan alasannya.
        seed([U('a'), U('h', { unitGroup: 'heavy' }), U('x', { unitGroup: 'Heavy Equip' })],
             [OUT('hilang', 'SF-RTK', '2026-09-01', { sn: 'SNtidakada' }), OUT('h', 'SF-RTK', '2026-09-01'),
              OUT('x', 'SF-RTK', '2026-09-01'), OUT('a', 'SF-RTK', '')]);
        const pl = licenseSyncPlan();
        t('unit hilang / alat berat / kelompok tak terbaca / tanpa tanggal: dilewati',
          [pl.rows.length, pl.skipped], [0, { noUnit: 1, notTractor: 2, noDate: 1 }]);
        openPreview();
        t('dan pesannya menyebut alasannya', /1 unitnya tidak ditemukan, 1 tanggalnya tidak valid, 2 ke unit non-Pertanian/.test(toasts.join('|')), true);

        // =============== 5. FORM DISTRIBUSI (satu catatan) ===============
        seed([U('a', { gpsLicense: 'SF-RTK', gpsLicenseStartDate: '2026-09-01', gpsLicenseEndDate: '2027-09-01' })], []);
        t('distribusi yang sama persis: "unchanged", tanpa tulisan',
          [applyDistributedLicenseToUnit(OUT('a', 'SF-RTK', '2026-09-01')), writes.length], ['unchanged', 0]);
        t('distribusi lebih lama: tidak mundur', applyDistributedLicenseToUnit(OUT('a', 'SF-RTK', '2026-01-01')), 'skipped-older');
        t('distribusi lebih baru: diterapkan', applyDistributedLicenseToUnit(OUT('a', 'SF-RTK', '2026-09-29')), 'applied');
        t('tanpa tanggal: ditolak, bukan "hari ini + 1 tahun"', applyDistributedLicenseToUnit(OUT('a', 'SF-RTK', '')), false);

        // =============== 6. HAK AKSES ===============
        seed([U('c')], [OUT('c', 'SF-RTK', '2026-09-15')]);
        currentUserDoc = { role: 'staff', status: 'active', email: 's@x.id', access: { licenseStock: 'edit', editUnits: 'view' } };
        applyRoleGating();
        t('tanpa hak edit unit: pratinjau tidak dibuka', openPreview(), false);
        t('dan alasannya disebut', /butuh hak edit pada Edit Units/.test(toasts.join('|')), true);
        currentUserDoc = { role: 'owner', status: 'active', email: 'o@x.id' };
        applyRoleGating();

        // =============== 7. RIWAYAT HANYA MENCATAT PERUBAHAN NYATA ===============
        seed([U('a', { gpsLicense: 'SF-RTK', gpsLicenseStartDate: '2026-09-01', gpsLicenseEndDate: '2027-09-01' }), U('c')],
             [OUT('a', 'SF-RTK', '2026-09-01'), OUT('c', 'SF-RTK', '2026-09-15')]);
        const hist = [];
        const realLog = window.logEvent;
        window.logEvent = e => { hist.push(e.unitId + ':' + e.field); };
        openPreview(); applyLicSyncSelection();
        window.logEvent = realLog;
        t('riwayat hanya berisi unit yang benar-benar berubah',
          hist.sort(), ['c:gpsLicense', 'c:gpsLicenseEndDate', 'c:gpsLicenseStartDate']);

        // =============== 8. KOLOM STATUS UNIT ===============
        seed([U('ok', { gpsLicense: 'SF-RTK', gpsLicenseStartDate: '2026-09-01', gpsLicenseEndDate: '2027-09-01' }),
              U('todo'), U('renew', { gpsLicense: 'SF-RTK', gpsLicenseStartDate: '2026-09-20', gpsLicenseEndDate: '2027-09-20' }),
              U('h', { unitGroup: 'heavy' })],
             [OUT('ok', 'SF-1', '2026-01-01', { id: 'OLD' }), OUT('ok', 'SF-RTK', '2026-09-01', { id: 'OK' }),
              OUT('todo', 'SF-RTK', '2026-09-15', { id: 'TODO' }), OUT('renew', 'SF-RTK', '2026-03-01', { id: 'NEWER' }),
              OUT('x', 'SF-RTK', '2026-09-01', { id: 'GONE', sn: 'SNnope' }), OUT('h', 'SF-RTK', '2026-09-01', { id: 'HV' }),
              OUT('todo', 'G5 Advance', '15/09/2026', { id: 'BADDATE' }),
              { id: 'IN1', txnType: 'IN', licenseType: 'SF-RTK', qty: 5, date: '2026-01-01' }]);
        renderLicenseStockTable();
        const statusOf = id => {
            const tr = [...document.querySelectorAll('#licenseBody tr')].find(x => x.querySelector('.license-check')?.dataset.id === id);
            const cell = tr && tr.querySelector('td[data-label="Status Unit"]');
            return cell ? cell.textContent.trim() : '(tidak ada)';
        };
        t('kolom Status Unit ada di tabel', document.querySelectorAll('#licenseTable thead th').length, 11);
        t('status tiap jenis baris',
          ['OK', 'OLD', 'TODO', 'NEWER', 'GONE', 'HV', 'BADDATE', 'IN1'].map(statusOf),
          ['Tersinkron', 'Digantikan', 'Belum', 'Unit lebih baru', 'Unit tidak ada', 'Bukan Pertanian', 'Tanggal tidak valid', '—']);
        t('tooltip "Digantikan" menyebut distribusi penggantinya',
          /2026-09-01/.test(([...document.querySelectorAll('#licenseBody .lic-status--muted')].find(x => /Digantikan/.test(x.textContent)) || {}).title), true);

        // =============== 9. MASUK LEWAT FORM = LANGSUNG TERSINKRON ===============
        const fillForm = (unit, type, date, editId) => {
            if (editId) editLicenseStock(editId); else showAddLicenseForm('OUT');
            document.getElementById('licTxnType').value = 'OUT'; onLicenseTxnChange();
            document.getElementById('licType').value = type;
            document.getElementById('licDate').value = date;
            document.getElementById('licQty').value = '1';
            populateLicenseUnitList(damageUnitLabel(unit));
            saveLicenseStock({ preventDefault() {} });
        };
        seed([U('n')], [{ id: 'IN1', txnType: 'IN', licenseType: 'SF-RTK', qty: 5, date: '2026-01-01' }]);
        fillForm(globalData[0], 'SF-RTK', '2026-09-29');
        const newId = globalLicenseStock.find(r => r.txnType === 'OUT').id;
        t('distribusi baru lewat form: unit langsung diperbarui', [globalData[0].gpsLicense, globalData[0].gpsLicenseEndDate], ['SF-RTK', '2027-09-29']);
        t('dan kolomnya langsung "Tersinkron"', statusOf(newId), 'Tersinkron');
        t('tombol Sync tanpa angka', document.getElementById('licSyncCount').hidden, true);

        // Membetulkan tanggal distribusi yang MENENTUKAN lisensi unit: boleh mundur.
        fillForm(globalData[0], 'SF-RTK', '2025-09-29', newId);
        t('salah ketik tahun dibetulkan: lisensi unit ikut mundur', globalData[0].gpsLicenseStartDate, '2025-09-29');
        t('dan tetap "Tersinkron"', statusOf(newId), 'Tersinkron');
        // Tapi distribusi yang TIDAK menentukan lisensi unit tetap tidak boleh
        // memundurkannya.
        seed([U('r', { gpsLicense: 'SF-RTK', gpsLicenseStartDate: '2026-09-20', gpsLicenseEndDate: '2027-09-20' })],
             [OUT('r', 'SF-RTK', '2026-03-01', { id: 'OLDER' })]);
        fillForm(globalData[0], 'SF-RTK', '2026-02-01', 'OLDER');
        t('edit distribusi lama tidak memundurkan unit yang diperpanjang', globalData[0].gpsLicenseEndDate, '2027-09-20');
        // Dipindah ke unit lain: unit lama diberi tahu, tidak ditebak.
        seed([U('p'), U('q')], [{ id: 'IN1', txnType: 'IN', licenseType: 'SF-RTK', qty: 5, date: '2026-01-01' }]);
        fillForm(globalData[0], 'SF-RTK', '2026-09-10');
        const movId = globalLicenseStock.find(r => r.txnType === 'OUT').id;
        toasts.length = 0; asked.length = 0;
        confirmAnswer = false;
        fillForm(globalData[1], 'SF-RTK', '2026-09-10', movId);
        t('dipindah ke unit lain: unit baru mendapat lisensinya', globalData[1].gpsLicense, 'SF-RTK');
        t('unit lama DITANYAKAN, tidak diubah diam-diam', asked.some(m => m.includes('GGTRp (GPS): dikosongkan')), true);
        t('Batal: unit lama dibiarkan, dan itu dikatakan', [globalData[0].gpsLicense, toasts.some(m => /dibiarkan/.test(m))], ['SF-RTK', true]);
        confirmAnswer = true;

        // =============== 10. IMPOR CSV = LANGSUNG TERSINKRON ===============
        seed([U('c1'), U('c2', { gpsLicense: 'SF-RTK', gpsLicenseStartDate: '2026-09-20', gpsLicenseEndDate: '2027-09-20' }), U('old')],
             [OUT('old', 'SF-RTK', '2026-09-01', { id: 'PREEXISTING' })]);
        const realPapa = window.Papa;
        window.Papa = { parse: (file, opts) => opts.complete({ data: [
            { 'Tanggal': '2026-09-28', 'Jenis': 'Distribusi', 'Jenis Lisensi': 'SF-RTK', 'Jumlah': '1', 'Serial Number': 'SNc1' },
            { 'Tanggal': '2026-03-01', 'Jenis': 'Distribusi', 'Jenis Lisensi': 'SF-RTK', 'Jumlah': '1', 'Serial Number': 'SNc2' }
        ] }) };
        handleLicenseCSVImport(new File(['x'], 'l.csv'));
        window.Papa = realPapa;
        t('impor CSV: unit dari baris impor langsung diperbarui', [globalData[0].gpsLicense, globalData[0].gpsLicenseEndDate], ['SF-RTK', '2027-09-28']);
        t('impor CSV: unit yang berlaku lebih lama tidak dimundurkan', globalData[1].gpsLicenseEndDate, '2027-09-20');
        t('impor CSV: baris lama yang belum sinkron TIDAK ikut ditulis (belum dilihat siapa pun)', globalData[2].gpsLicense, '');
        const importToast = toasts.find(m => /^Import lisensi/.test(m)) || '';
        t('dan pesannya menyebut apa yang terjadi', importToast,
          'Import lisensi: 2 ditambahkan · lisensi 1 unit ikut diperbarui · 1 unit tidak diubah (berlaku lebih lama)');

        // =============== 11. TEMUAN REVIEW v115 ===============
        // A. Pratinjau basi: unit diperpanjang di perangkat lain selama
        //    pratinjau terbuka — baris itu TIDAK boleh ditulis.
        seed([U('s')], [OUT('s', 'SF-RTK', '2026-09-15')]);
        openPreview();
        globalData[0] = { ...globalData[0], gpsLicense: 'SF-RTK', gpsLicenseStartDate: '2026-09-28', gpsLicenseEndDate: '2027-09-28' };
        toasts.length = 0; writes.length = 0;
        applyLicSyncSelection();
        t('A: baris yang berubah sejak pratinjau tidak ditulis', [writes.length, globalData[0].gpsLicenseEndDate], [0, '2027-09-28']);
        t('A: dan pesannya bilang begitu', /1 baris dilewati karena datanya berubah sejak pratinjau/.test(toasts.join('|')), true);
        // Baris "newer" yang dicentang dengan sengaja tetap bisa ditulis.
        seed([U('n2', { gpsLicense: 'SF-RTK', gpsLicenseStartDate: '2026-09-20', gpsLicenseEndDate: '2027-09-20' })], [OUT('n2', 'SF-RTK', '2026-03-01')]);
        openPreview(); document.querySelector('#licSyncBody .lic-sync-check').click(); applyLicSyncSelection();
        t('A: "newer" yang dicentang sengaja tetap ditulis', globalData[0].gpsLicenseEndDate, '2027-03-01');

        // B. Tanggal distribusi non-ISO: tidak dipakai untuk menebak urutan.
        seed([U('b1')], [OUT('b1', 'SF-RTK', '9/15/2024', { id: 'US' }), OUT('b1', 'SF-RTK', '2025-10-01', { id: 'ISO' })]);
        t('B: tanggal non-ISO ditandai, bukan diurutkan sebagai teks',
          [licenseSyncPlan().rows[0].rec.id, licenseSyncPlan().recStatus.get('US').code], ['ISO', 'noDate']);
        t('B: CSV D/M/YYYY dibaca hari-dulu', [csvLicenseDate('01/10/2025'), csvLicenseDate('1-2-2026'), csvLicenseDate('2026-09-30'), csvLicenseDate('31/02/2026')],
          ['2025-10-01', '2026-02-01', '2026-09-30', '31/02/2026']);

        // C. Tanggal habis unit non-ISO yang lebih lama: tidak dimundurkan.
        seed([U('c1', { gpsLicense: 'SF-RTK', gpsLicenseStartDate: '2026-12-31', gpsLicenseEndDate: '12/31/2027' })], [OUT('c1', 'SF-RTK', '2026-03-01')]);
        t('C: expiry unit 12/31/2027 dibaca sebagai tanggal, jadi "newer"', plan(), ['c1:gps:newer']);
        seed([U('c2', { gpsLicense: 'SF-RTK', gpsLicenseStartDate: '2026-03-01', gpsLicenseEndDate: 'besok' })], [OUT('c2', 'SF-RTK', '2026-03-01')]);
        t('C: expiry unit yang tak terbaca tidak ditimpa tanpa dicek', plan(), ['c2:gps:newer']);

        // D. Ejaan jenis kanonik.
        seed([U('d1'), U('d2', { gpsLicense: 'sf-rtk', gpsLicenseStartDate: '2026-09-01', gpsLicenseEndDate: '2027-09-01' })],
             [OUT('d1', 'sf-rtk', '2026-09-01'), OUT('d2', 'SF-RTK', '2026-09-01')]);
        openPreview(); applyLicSyncSelection();
        t('D: distribusi "sf-rtk" ditulis sebagai SF-RTK, dan unit berejaan salah dibetulkan',
          [globalData[0].gpsLicense, globalData[1].gpsLicense], ['SF-RTK', 'SF-RTK']);
        t('D: unit berejaan salah hanya ditulis jenisnya', writes.filter(u => u.id === 'd2').length, 1);

        // E. Unit yang hanya punya field GPS lama dan sudah cocok: sesuai.
        seed([U('e1', { gpsLicense: 'SF-RTK', licenseStartDate: '2026-03-01', licenseEndDate: '2027-03-01' })], [OUT('e1', 'SF-RTK', '2026-03-01')]);
        t('E: field lama yang cocok terbaca sesuai', plan(), ['e1:gps:sync']);

        // F. Hapus distribusi yang menentukan lisensi unit.
        seed([U('f1')], [OUT('f1', 'SF-1', '2026-01-01', { id: 'F_OLD' }), OUT('f1', 'SF-RTK', '2026-09-01', { id: 'F_NEW' })]);
        applyDistributedLicenseToUnit(globalLicenseStock[0]); applyDistributedLicenseToUnit(globalLicenseStock[1]);
        t('F: sebelum hapus unit memegang SF-RTK', globalData[0].gpsLicense, 'SF-RTK');
        asked.length = 0;
        deleteLicenseStock('F_NEW');
        t('F: hapus distribusi terbaru → ditawari kembali ke distribusi sebelumnya', asked.some(m => /kembali ke SF-1 dari distribusi 2026-01-01/.test(m)), true);
        t('F: OK → unit kembali ke SF-1', [globalData[0].gpsLicense, globalData[0].gpsLicenseStartDate], ['SF-1', '2026-01-01']);
        deleteLicenseStock('F_OLD');
        t('F: hapus distribusi terakhir → lisensi unit dikosongkan', [globalData[0].gpsLicense, globalData[0].gpsLicenseEndDate], ['', '']);
        // Distribusi yang TIDAK menentukan lisensi unit: menghapusnya tidak menanyakan apa pun.
        seed([U('f2', { gpsLicense: 'SF-RTK', gpsLicenseStartDate: '2026-09-20', gpsLicenseEndDate: '2027-09-20' })], [OUT('f2', 'SF-RTK', '2026-03-01', { id: 'F_X' })]);
        asked.length = 0;
        deleteLicenseStock('F_X');
        t('F: hapus distribusi yang tidak menentukan unit: hanya konfirmasi hapus', [asked.length, globalData[0].gpsLicenseEndDate], [1, '2027-09-20']);

        // G. Tanpa hak edit unit: form Distribusi memberi tahu.
        seed([U('g1')], [{ id: 'IN1', txnType: 'IN', licenseType: 'SF-RTK', qty: 5, date: '2026-01-01' }]);
        currentUserDoc = { role: 'staff', status: 'active', email: 's@x.id', access: { licenseStock: 'edit', editUnits: 'view' } };
        applyRoleGating();
        fillForm(globalData[0], 'SF-RTK', '2026-09-29');
        t('G: distribusi dicatat tapi unit tidak diubah — dan itu dikatakan', toasts.some(m => /butuh hak edit pada Edit Units/.test(m)), true);
        currentUserDoc = { role: 'owner', status: 'active', email: 'o@x.id' };
        applyRoleGating();

        // I. Kolom "Sekarang" untuk tier yang sudah turun.
        seed([U('i1', { gpsLicense: 'SF-1', gpsLicenseStartDate: '2025-06-01', gpsLicenseEndDate: '', gpsLicenseExpiredAt: '2026-06-01' })],
             [OUT('i1', 'SF-RTK', '2026-09-01')]);
        openPreview();
        t('I: "Sekarang" menyebut premium yang habis, bukan SF-1 "s/d" tanggal mati',
          document.querySelector('#licSyncBody td[data-label="Sekarang"]').textContent.split(String.fromCharCode(10)).join(' ').replace(/  +/g, ' ').trim(), 'SF-1 (SF-RTK habis 2026-06-01)');
        closeLicSyncModal();

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
