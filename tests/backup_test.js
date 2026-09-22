// Cadangan yang benar-benar mencadangkan.
//
// Selama tiga versi tombolnya berjudul "Download full JSON backup" sementara
// isinya empat koleksi dari lima belas. Seluruh modul Tim dan Gudang tidak
// punya cadangan sama sekali.
//
// Yang diuji di sini bukan "apakah fungsinya jalan", melainkan tiga hal yang
// kalau salah membuat cadangan berbohong lagi:
//   1. apa yang IKUT dan apa yang SENGAJA tidak ikut,
//   2. koleksi yang tidak ada di berkas dilaporkan, bukan didiamkan,
//   3. jendela langganan shift tidak memotong cadangan, dan REPLACE tidak
//      menghapus shift di luar jendela itu.
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

        // Unduhan ditangkap, bukan dijalankan: yang menarik isinya.
        let exported = null;
        const realCreate = document.createElement.bind(document);
        URL.createObjectURL = blob => { window.__blob = blob; return 'blob:test'; };
        URL.revokeObjectURL = () => {};
        document.createElement = tag => {
            const el = realCreate(tag);
            if (tag === 'a') el.click = () => {};   // jangan benar-benar mengunduh
            return el;
        };
        // Blobnya dibaca setelah exportBackup selesai, bukan di dalam click():
        // click() dipanggil tanpa await, jadi membaca di sana bisa terlambat.
        const ambilHasil = async () => {
            window.__blob = null;
            await exportBackup();
            exported = window.__blob ? JSON.parse(await window.__blob.text()) : null;
        };

        window.confirm = () => false;      // jangan sertakan lampiran
        const toasts = [];
        const realToast = window.showToast;
        window.showToast = (m, k) => { toasts.push(String(m)); };

        // Shift di server LEBIH BANYAK dari yang ada di memori — persis
        // keadaan sebenarnya, karena langganannya berjendela 120 hari.
        const serverShifts = [
            { id: '2020-01-06_m1', date: '2020-01-06', memberId: 'm1', shift: 'pagi' },
            { id: '2026-09-14_m1', date: '2026-09-14', memberId: 'm1', shift: 'malam' }
        ];
        const written = {};
        const deleted = [];
        const mk = name => items => { written[name] = (written[name] || []).concat(items); return Promise.resolve(); };
        window.cloud = { isReady: true,
            getAllShifts: () => Promise.resolve(serverShifts.slice()),
            saveUnits: mk('units'), deleteUnits: () => Promise.resolve(),
            getAllUnits: () => Promise.resolve([]), addHistoryEvents: () => Promise.resolve(),
            saveImplements: mk('implements'), saveDamages: mk('damages'), saveLicenses: mk('licenses'),
            saveUserCategories: mk('userCategories'), saveDamageComponents: mk('damageComponents'),
            saveTeamMembers: mk('teamMembers'), saveShifts: mk('shifts'),
            saveLeaveRequests: mk('leaveRequests'),
            deleteLeaveRequest: id => { deleted.push('lv:' + id); return Promise.resolve(); },
            saveWorkLogs: mk('workLogs'), saveDevices: mk('devices'), saveStockItems: mk('stockItems'),
            deleteImplement: id => { deleted.push('impl:' + id); return Promise.resolve(); },
            deleteShift: id => { deleted.push('shift:' + id); return Promise.resolve(); },
            deleteTeamMember: id => { deleted.push('tm:' + id); return Promise.resolve(); },
            deleteWorkLog: () => Promise.resolve(), deleteDevice: () => Promise.resolve(),
            deleteStockItem: () => Promise.resolve(), deleteUserCategory: () => Promise.resolve(),
            deleteDamageComponent: () => Promise.resolve(), deleteDamage: () => Promise.resolve(),
            deleteLicense: () => Promise.resolve(), subscribeUsers: () => () => {} };

        currentUser = { uid: 'uO', email: 'o@x.id' };
        currentUserDoc = { role: 'owner', status: 'active', email: 'o@x.id' };
        hideAuthGates(); applyRoleGating();

        globalData        = [{ id: 'u1', name: 'A', sn: 'S1', status: 'Good' }];
        globalImplements  = [{ id: 'i1', profileName: 'Plough' }];
        globalDamages     = [{ id: 'd1', unitId: 'u1' }];
        globalLicenseStock= [{ id: 'l1', txnType: 'IN', qty: 2 }];
        userCategories    = [{ id: 'c1', name: 'Harvest' }];
        damageComponents  = [{ id: 'dc1', name: 'GPS', unitField: 'gps' }];
        teamMembers       = [{ id: 'm1', name: 'Andi', company: 'PT. GPA', active: true }];
        teamShifts        = [serverShifts[1]];          // hanya yang di dalam jendela
        workLogs          = [{ id: 'w1', date: '2026-09-14', memberId: 'm1', task: 'x' }];
        leaveRequests     = [{ id: 'lv1', memberId: 'm1', type: 'izin',
                               dateFrom: '2026-09-15', dateTo: '2026-09-15', days: 1, approval: 'pending' }];
        warehouseDevices  = [{ id: 'dev1', sn: 'D1', status: 'warehouse' }];
        stockLedger       = [{ id: 'st1', itemName: 'Oli', txnType: 'IN', qty: 5 }];

        // ---------- EKSPOR ----------
        await ambilHasil();
        t('cadangan tertulis', !!exported, true);
        t('versinya naik ke 4', exported.version, 4);

        const isi = k => Array.isArray(exported[k]) ? exported[k].length : 'TIDAK ADA';
        t('unit ikut', isi('units'), 1);
        t('implement ikut', isi('implements'), 1);
        t('kerusakan ikut', isi('damages'), 1);
        t('stok lisensi ikut', isi('licenseStock'), 1);
        t('kategori user ikut', isi('userCategories'), 1);
        t('komponen kerusakan ikut', isi('damageComponents'), 1);
        t('anggota tim ikut', isi('teamMembers'), 1);
        t('laporan harian ikut', isi('workLogs'), 1);
        t('izin / sakit ikut', isi('leaveRequests'), 1);
        t('perangkat gudang ikut', isi('devices'), 1);
        t('stok barang ikut', isi('stockItems'), 1);

        // Inti dari seluruh berkas uji ini: kalau shift diambil dari larik di
        // memori, cadangannya kehilangan semua shift di luar jendela dan
        // TERLIHAT lengkap.
        t('shift diambil dari server, bukan dari jendela langganan', isi('shifts'), 2);
        t('shift lama di luar jendela ikut tercadang',
          exported.shifts.some(s => s.date === '2020-01-06'), true);

        // Dua yang sengaja tidak ikut harus tetap tidak ikut.
        t('akun tidak ikut dicadangkan', 'users' in exported, false);
        t('riwayat audit tidak ikut dicadangkan', 'history' in exported, false);
        t('daftar yang dikecualikan tersedia untuk UI', BACKUP_EXCLUDED.length, 2);

        // ---------- EKSPOR TANPA AKSES PENUH ----------
        // Cadangan tidak boleh jadi jalan memutar area 'none', tapi juga tidak
        // boleh diam-diam pendek: berkasnya harus mengaku.
        currentUserDoc = { role: 'staff', status: 'active', email: 's@x.id',
                           access: { csv: 'full',
                                     editUnits:'edit', implements:'edit', damage:'edit',
                                     licenseStock:'edit', teamMembers:'none', teamShift:'none',
                                     teamLog:'none', warehouse:'none', leader:'none', history:'none' } };
        applyRoleGating();
        await ambilHasil();
        t('tanpa akses tim, koleksi tim tidak ikut', 'teamMembers' in exported, false);
        t('dan berkasnya mengakui apa yang tidak ada di dalamnya',
          exported.omitted.map(o => o.key).sort(),
          ['devices', 'leaveRequests', 'shifts', 'stockItems', 'teamMembers', 'workLogs']);
        t('toastnya menyebut yang tidak termasuk',
          toasts.some(m => /TIDAK termasuk/.test(m)), true);

        currentUserDoc = { role: 'owner', status: 'active', email: 'o@x.id' };
        applyRoleGating();

        // ---------- RESTORE CADANGAN v3 LAMA ----------
        // Berkas lama tidak punya data Tim/Gudang sama sekali. Yang berbahaya
        // bukan kehilangan data — jalur restore memang tidak menyentuhnya —
        // melainkan orang mengira restore-nya lengkap.
        const v3 = { version: 3, units: [{ id: 'u1', name: 'A', sn: 'S1' }],
                     implements: [], damages: [], licenseStock: [] };
        window.confirm = () => true;        // GABUNG
        await new Promise(res => {
            importBackup(new File([JSON.stringify(v3)], 'b.json', { type: 'application/json' }));
            setTimeout(res, 400);
        });
        const barisLaporan = [...document.querySelectorAll('#restoreReportBody tr')]
            .map(tr => tr.textContent.replace(/\\s+/g, ' ').trim());
        t('laporan restore muncul', barisLaporan.length > 0, true);
        // v3 hanya membawa implement, kerusakan, dan stok lisensi — tujuh
        // koleksi lainnya tidak ada di dalamnya, dan ketujuhnya harus tertulis.
        t('koleksi yang tidak ada di berkas DILAPORKAN, bukan didiamkan',
          barisLaporan.filter(x => /tidak ada di berkas ini/.test(x)).length, 8);
        t('anggota tim termasuk yang dilaporkan hilang',
          barisLaporan.some(x => /Anggota Tim.*tidak ada di berkas ini/.test(x)), true);
        t('peringatan cadangan lama tampil',
          /Cadangan lama/.test(document.getElementById('restoreReportNote').innerHTML), true);
        t('akun dan riwayat disebut tidak pernah dicadangkan',
          barisLaporan.filter(x => /tidak pernah dicadangkan/.test(x)).length, 2);
        t('restore v3 tidak menghapus anggota tim yang ada', teamMembers.length, 1);
        closeRestoreReport();

        // ---------- RESTORE PENUH, MODE GANTI ----------
        // REPLACE harus dihitung terhadap isi SERVER. Kalau dihitung terhadap
        // teamShifts (yang berjendela), shift 2020 akan ikut terhapus padahal
        // ada di dalam berkas.
        deleted.length = 0;
        Object.keys(written).forEach(k => delete written[k]);
        const v4 = JSON.parse(JSON.stringify(exported));
        v4.version = 4;
        v4.units = [{ id: 'u1', name: 'A', sn: 'S1' }];
        v4.teamMembers = [{ id: 'm1', name: 'Andi', company: 'PT. GPA', active: true }];
        v4.shifts = serverShifts.slice();
        v4.workLogs = [{ id: 'w1', date: '2026-09-14', memberId: 'm1', task: 'x' }];
        v4.leaveRequests = [{ id: 'lv1', memberId: 'm1', type: 'izin',
                              dateFrom: '2026-09-15', dateTo: '2026-09-15', days: 1, approval: 'pending' }];
        v4.devices = [{ id: 'dev1', sn: 'D1', status: 'warehouse' }];
        v4.stockItems = [{ id: 'st1', itemName: 'Oli', txnType: 'IN', qty: 5 }];
        v4.userCategories = [{ id: 'c1', name: 'Harvest' }];
        v4.damageComponents = [{ id: 'dc1', name: 'GPS', unitField: 'gps' }];
        // Mode GANTI bertanya DUA kali: pertama memilih gabung/ganti (Batal =
        // ganti), lalu meminta penegasan sebelum menghapus. Menjawab "tidak"
        // pada yang kedua membatalkan seluruh restore.
        let tanya = 0;
        window.confirm = () => { tanya++; return tanya === 1 ? false : true; };
        await new Promise(res => {
            importBackup(new File([JSON.stringify(v4)], 'b4.json', { type: 'application/json' }));
            setTimeout(res, 500);
        });
        t('shift lama di luar jendela TIDAK terhapus saat mode ganti',
          deleted.filter(x => x === 'shift:2020-01-06_m1'), []);
        t('anggota tim benar-benar ditulis ke cloud',
          (written.teamMembers || []).length, 1);
        t('jadwal shift benar-benar ditulis ke cloud',
          (written.shifts || []).length, 2);
        t('gudang benar-benar ditulis ke cloud',
          [(written.devices || []).length, (written.stockItems || []).length], [1, 1]);
        t('izin / sakit benar-benar ditulis ke cloud',
          (written.leaveRequests || []).length, 1);
        const barisV4 = [...document.querySelectorAll('#restoreReportBody tr')]
            .map(tr => tr.textContent.replace(/\\s+/g, ' ').trim());
        t('tidak ada lagi koleksi yang hilang dari berkas v4',
          barisV4.filter(x => /tidak ada di berkas ini/.test(x)).length, 0);
        t('peringatan cadangan lama tidak muncul untuk v4',
          document.getElementById('restoreReportNote').style.display, 'none');
        closeRestoreReport();

        // ---------- CADANGAN BERGULIR ----------
        // Tiga titik pulih yang selama ini ditulis tapi tidak pernah dibaca.
        localStorage.setItem(BACKUP_RING_KEY, JSON.stringify([
            { at: 1700000000000, count: 2, units: [{ id: 'u1', name: 'A' }, { id: 'u2', name: 'B' }] },
            { at: 1700000900000, count: 1, units: [{ id: 'u1', name: 'A' }] }
        ]));
        const ring = readAutoBackups();
        t('cadangan otomatis terbaca', ring.length, 2);
        t('yang terbaru di atas', ring[0].at, 1700000900000);

        showAutoBackups();
        t('daftarnya tergambar', document.querySelectorAll('#autoBackupList li').length, 2);

        globalData = [{ id: 'u9', name: 'Z', sn: 'S9' }];
        Object.keys(written).forEach(k => delete written[k]);
        window.confirm = () => true;
        restoreAutoBackup(0);
        t('memulihkan mengembalikan unitnya', globalData.map(u => u.id), ['u1']);
        t('dan mengirimnya ke cloud, bukan lokal saja',
          (written.units || []).length > 0, true);

        // Tanpa hak edit unit, jalur ini harus tertutup.
        currentUserDoc = { role: 'staff', status: 'active', email: 's@x.id',
                           access: { editUnits: 'view' } };
        applyRoleGating();
        const sebelum = globalData.map(u => u.id);
        restoreAutoBackup(1);
        t('tanpa hak edit unit, pemulihan ditolak', globalData.map(u => u.id), sebelum);

        window.showToast = realToast;
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
