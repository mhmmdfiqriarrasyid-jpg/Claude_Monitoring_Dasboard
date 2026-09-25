// Pemindai catatan riwayat yang menggambarkan perubahan yang tidak pernah
// terjadi — tulisannya ditolak server, catatannya terlanjur tertulis.
//
// Alat ini MENGHAPUS data audit, jadi yang paling penting diuji bukan apa yang
// ia temukan melainkan apa yang ia BIARKAN: baris lama dalam satu rantai,
// modul yang entrinya tidak menunjuk record apa pun, dan unit yang sudah
// terhapus. Salah di sisi itu berarti menghapus jejak yang sah.
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

        let serverHistory = [], deleted = [], getAllCalls = 0;
        const H = (id, o) => Object.assign({
            id, timestamp: 1000, action: 'update', unitId: '', unitName: '',
            field: '', before: '', after: '',
            actorUid: 'uA', actorEmail: 'a@x.id', actorName: 'Andi', actorRole: 'pbt'
        }, o);

        window.cloud = { isReady: true,
            saveUnits: () => Promise.resolve(), deleteUnits: () => Promise.resolve(),
            getAllUnits: () => Promise.resolve([]),
            addHistoryEvents: () => Promise.resolve(),
            getAllHistory: () => { getAllCalls++; return Promise.resolve(serverHistory.slice()); },
            deleteHistoryEvents: ids => { deleted.push(...ids);
                serverHistory = serverHistory.filter(e => !ids.includes(e.id));
                return Promise.resolve(); } };

        currentUser = { uid: 'uO', email: 'o@x.id' };
        currentUserDoc = { role: 'owner', status: 'active', email: 'o@x.id' };
        hideAuthGates(); applyRoleGating();

        globalData = [
            { id: 'u_1', name: 'GGTR147G_OHZ', sn: 'SN1', site: 'PT. GPA', status: 'Good', yearReceived: 2020 },
            { id: 'u_2', name: 'MGTR074M_HBA', sn: 'SN2', site: 'PT. MNM', status: 'Breakdown' }
        ];
        teamMembers = []; teamShifts = []; workLogs = [];

        // ---------- apa yang dianggap palsu ----------
        serverHistory = [
            // Palsu: migrasi mencoba menimpa nama, server menolak.
            H('p1', { timestamp: 9000, unitId: 'u_1', unitName: 'GGTR147G_SCL', field: 'name',
                      before: 'GGTR147G_OHZ', after: 'GGTR147G_SCL' }),
            // Palsu juga, pelaku LAIN — pemindai tidak boleh terpaku satu orang.
            H('p2', { timestamp: 9000, unitId: 'u_2', unitName: 'MGTR074M_', field: 'name',
                      before: 'MGTR074M_HBA', after: 'MGTR074M_',
                      actorUid: 'uB', actorName: 'Budi' }),
            // Asli: nilainya cocok dengan data sekarang.
            H('ok1', { timestamp: 9000, unitId: 'u_2', unitName: 'MGTR074M_HBA', field: 'status',
                       before: 'Good', after: 'Breakdown' }),
            // Baris LAMA dalam rantai yang sama — tidak bisa dibedakan antara
            // "pernah berlaku lalu diubah" dan "gagal", jadi harus dibiarkan.
            H('old1', { timestamp: 100, unitId: 'u_1', unitName: 'lama', field: 'name',
                        before: 'X', after: 'Y' }),
            // Unit sudah terhapus — tidak ada pembanding.
            H('gone', { timestamp: 9000, unitId: 'u_99', unitName: 'sudah dihapus', field: 'name',
                        before: 'A', after: 'B' }),
            // Modul lain: unitId dipakai untuk hal yang bukan dokumen unit.
            H('tim', { timestamp: 9000, unitId: '', unitName: '[Tim] Andi', field: 'Perusahaan',
                       before: '—', after: 'PT. GPA' }),
            H('gdg', { timestamp: 9000, unitId: 'u_1', unitName: '[Gudang] GPS SN9', field: 'Perangkat',
                       before: '', after: 'Terpasang' }),
            H('lis', { timestamp: 9000, unitId: 'u_1', unitName: '[Lisensi] SF-RTK', field: 'Stok masuk',
                       before: '', after: '5 (2026-01-01)' }),
            H('rsk', { timestamp: 9000, unitId: 'u_1', unitName: '[Kerusakan] GGTR147G_OHZ',
                       field: 'Mekanis / GPS', before: '', after: '2026-01-01' }),
            // Aksi selain update tidak punya dasar penilaian yang aman.
            H('add', { timestamp: 9000, action: 'add', unitId: 'u_1', unitName: 'GGTR147G_OHZ', after: 'SN1' }),
            H('del', { timestamp: 9000, action: 'delete', unitId: 'u_1', unitName: 'GGTR147G_OHZ', before: 'SN1' }),
            H('mig', { timestamp: 9000, action: 'migrate', unitName: '-', field: 'license defaults', after: '10' }),
            // Field internal.
            H('int', { timestamp: 9000, unitId: 'u_1', unitName: 'GGTR147G_OHZ',
                       field: 'downtimeHistory', after: '[]' }),
            // Angka vs teks: sama bagi mata dan bagi logEvent, jadi bukan palsu.
            H('num', { timestamp: 9000, unitId: 'u_1', unitName: 'GGTR147G_OHZ',
                       field: 'yearReceived', before: '2019', after: '2020' })
        ];

        const found = findPhantomUnitHistory(serverHistory);
        const ids = found.map(f => f.entry.id).sort();
        t('menemukan tepat baris yang tidak cocok dengan data sekarang', ids, ['p1', 'p2']);
        t('mencakup lebih dari satu pelaku',
          [...new Set(found.map(f => f.entry.actorName))].sort(), ['Andi', 'Budi']);
        t('membawa nilai sekarang untuk dinilai owner',
          found.map(f => f.current).sort(), ['GGTR147G_OHZ', 'MGTR074M_HBA']);
        t('baris lama dalam satu rantai dibiarkan', ids.includes('old1'), false);
        t('unit yang sudah terhapus dibiarkan', ids.includes('gone'), false);
        t('modul Tim dibiarkan', ids.includes('tim'), false);
        t('modul Gudang dibiarkan', ids.includes('gdg'), false);
        t('modul Lisensi dibiarkan', ids.includes('lis'), false);
        t('modul Kerusakan dibiarkan', ids.includes('rsk'), false);
        t('baris tambah/hapus/migrasi dibiarkan',
          ['add', 'del', 'mig'].some(x => ids.includes(x)), false);
        t('field internal dibiarkan', ids.includes('int'), false);
        t('angka vs teks tidak dianggap palsu', ids.includes('num'), false);
        t('urut dari yang terbaru',
          found[0].entry.timestamp >= found[found.length - 1].entry.timestamp, true);
        // Nilai yang sudah bergerak dari "sebelum"-nya berarti ada perubahan
        // lain yang berhasil sesudahnya — bukan bukti bahwa baris ini gagal.
        t('baris yang nilainya sudah bergerak dibiarkan',
          findPhantomUnitHistory([H('moved', { timestamp: 9000, unitId: 'u_1',
              unitName: 'GGTR147G_OHZ', field: 'name', before: 'LAIN', after: 'JUGA LAIN' })]).length, 0);

        // ---------- memindai seluruh koleksi, bukan 500 terbaru ----------
        // getAuditLog() terpotong dua kali ke 500, dan baris palsu justru
        // kebanyakan lebih tua dari itu.
        const bulk = [];
        for (let i = 0; i < 600; i++) bulk.push(H('bulk' + i, { timestamp: 20000 + i, unitId: 'u_2',
            unitName: 'MGTR074M_HBA', field: 'site', before: 'PT. MNM', after: 'PT. MNM' }));
        serverHistory = bulk.concat(serverHistory);
        cloudHistory = bulk.slice(0, 500);          // seperti langganan yang dibatasi
        localStorage.setItem('tractorAuditLog', JSON.stringify([]));
        getAllCalls = 0;
        await scanPhantomHistory();
        await new Promise(r => setTimeout(r, 300));
        t('memakai getAllHistory, bukan yang tampil di layar', getAllCalls, 1);
        t('tetap menemukan baris tua di balik batas 500',
          _phantomHistoryFound.map(f => f.entry.id).sort(), ['p1', 'p2']);
        t('modal pratinjau terbuka',
          document.getElementById('phantomHistoryModal').classList.contains('open'), true);
        t('pratinjau menampilkan tiap barisnya',
          document.querySelectorAll('#phantomHistoryBody tr').length, 2);
        t('pratinjau menyebut jumlah pelaku',
          /2 pelaku/.test(document.getElementById('phantomHistorySummary').textContent), true);

        // ---------- menghapus dari cloud DAN cache lokal ----------
        // Kalau cache lokal tidak ikut dipangkas, getAuditLog() memunculkannya
        // kembali pada render berikutnya dan pekerjaannya sia-sia.
        localStorage.setItem('tractorAuditLog', JSON.stringify(
            [H('p1', { unitId: 'u_1', field: 'name', after: 'GGTR147G_SCL' }),
             H('ok1', { unitId: 'u_2', field: 'status', after: 'Breakdown' })]));
        window.confirm = () => true;
        deleted = [];
        await deletePhantomHistory();
        await new Promise(r => setTimeout(r, 300));
        t('menghapus tepat baris yang dipilih', deleted.sort(), ['p1', 'p2']);
        t('hilang juga dari cache lokal',
          JSON.parse(localStorage.getItem('tractorAuditLog')).map(e => e.id), ['ok1']);
        t('hilang dari daftar riwayat di memori',
          cloudHistory.some(e => e.id === 'p1' || e.id === 'p2'), false);
        t('modal tertutup setelah menghapus',
          document.getElementById('phantomHistoryModal').classList.contains('open'), false);
        // Pemindaian berulang tidak boleh makin agresif: setelah baris palsu
        // hilang, baris lama di bawahnya tidak ikut tertuduh.
        t('memindai ulang tidak menemukan apa-apa lagi',
          findPhantomUnitHistory(serverHistory).length, 0);

        // ---------- pengaman ----------
        // Service worker bisa menyajikan firebase-init.js lama; tanpa deteksi
        // ini penghapusannya diam-diam tidak terjadi tapi toast bilang sukses.
        _phantomHistoryFound = [{ entry: H('z', { unitId: 'u_1', field: 'name' }), current: 'x' }];
        const realDelete = window.cloud.deleteHistoryEvents;
        delete window.cloud.deleteHistoryEvents;
        deleted = [];
        await deletePhantomHistory();
        t('versi lama terdeteksi, tidak ada yang dihapus', deleted.length, 0);
        window.cloud.deleteHistoryEvents = realDelete;

        currentUserDoc = { role: 'pbt', status: 'active', access: { teamLog: 'edit' } };
        applyRoleGating();
        deleted = [];
        await deletePhantomHistory();
        t('bukan owner tidak bisa menghapus', deleted.length, 0);
        t('tombol Periksa tersembunyi untuk bukan owner',
          [...document.querySelectorAll('[data-owner-only]')]
              .every(el => el.style.display === 'none'), true);
        getAllCalls = 0;
        await scanPhantomHistory();
        t('bukan owner tidak bisa memindai', getAllCalls, 0);

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
