const { launch, BASE_URL } = require('./_env');

(async () => {
    const b = await launch();
    const page = await b.newPage({ viewport: { width: 1440, height: 1100 } });
    const errors = [];
    page.on('pageerror', e => { if (!/Chart is not defined/.test(e.message)) errors.push(e.message); });
    await page.goto(BASE_URL + '/index.html', { waitUntil: 'load' });
    await page.waitForTimeout(1000);

    await page.addScriptTag({ content: `(() => { try {
        const T = []; const t = (n, g, w) => T.push({ n, g, w, pass: JSON.stringify(g) === JSON.stringify(w) });
        const GPA = 'PT. Global Papua Abadi', MNM = 'PT. Murni Nusantara Mandiri';
        window.cloud = { isReady: true, subscribeUsers: cb => { cb([]); return () => {}; } };
        currentUser = { uid:'boss', email:'boss@x.id' };
        currentUserDoc = { role:'owner', status:'active', email:'boss@x.id' };
        hideAuthGates();
        globalData = [
            { id:'u_1', name:'JD-01', sn:'SN1', status:'Good' },
            { id:'u_2', name:'JD-02', sn:'SN2', status:'Breakdown' }
        ];
        teamMembers = [
            { id:'m1', name:'Andi', company:GPA, active:true },
            { id:'m2', name:'Budi', company:GPA, active:true },
            { id:'m3', name:'Citra', company:MNM, active:true },
            { id:'m4', name:'Dedi', active:true }                    // tanpa perusahaan
        ];
        teamShifts = [
            { id:'2026-09-14_m1', date:'2026-09-14', memberId:'m1', shift:'pagi' },
            { id:'2026-09-15_m1', date:'2026-09-15', memberId:'m1', shift:'libur' },  // libur tidak dihitung
            { id:'2026-09-15_m2', date:'2026-09-15', memberId:'m2', shift:'malam' },
            { id:'2026-09-07_m1', date:'2026-09-07', memberId:'m1', shift:'pagi' }    // minggu lalu
        ];
        workLogs = [
            // September — GPA: 8j + 4j = 12j, dua anggota, dua unit
            { id:'a1', date:'2026-09-14', memberId:'m1', memberName:'Andi', company:GPA,
              start:'08:00', end:'16:00', approval:'approved',
              units:[{id:'u_1',name:'JD-01',sn:'SN1'}] },
            { id:'a2', date:'2026-09-15', memberId:'m2', memberName:'Budi', company:GPA,
              start:'08:00', end:'12:00', approval:'pending',
              units:[{id:'u_2',name:'JD-02',sn:'SN2'}] },
            // September — MNM: shift malam lewat tengah malam = 8j
            { id:'b1', date:'2026-09-15', memberId:'m3', memberName:'Citra', company:MNM,
              start:'23:00', end:'07:00', approval:'revision', revisionNote:'x' },
            // September — tanpa perusahaan
            { id:'c1', date:'2026-09-16', memberId:'m4', memberName:'Dedi',
              start:'09:00', end:'12:00' },
            // Agustus — tidak boleh ikut terhitung di September
            { id:'z1', date:'2026-08-20', memberId:'m1', memberName:'Andi', company:GPA,
              start:'08:00', end:'18:00', approval:'approved' }
        ];
        globalDamages = [
            { id:'d1', date:'2026-09-15', unitId:'u_2', resolved:false },
            { id:'d2', date:'2026-09-16', unitId:'u_1', resolved:true, resolvedAt:'2026-09-16' },
            { id:'d3', date:'2026-09-08', unitId:'u_1', resolved:false }   // minggu lalu
        ];
        stockLedger = [
            { id:'s1', date:'2026-09-15', txnType:'OUT', itemName:'Filter', qty:4 },
            { id:'s2', date:'2026-09-15', txnType:'IN',  itemName:'Filter', qty:10 },
            { id:'s3', date:'2026-09-08', txnType:'OUT', itemName:'Filter', qty:1 }
        ];
        warehouseDevices = []; globalLicenseStock = []; allUsers = [];
        navigateTo('leader');

        // ================= REKAP PER PERUSAHAAN =================
        recapMonth = '2026-09';
        const sep = companyRecap('2026-09');
        const gpa = sep.find(r => r.company === GPA);
        const mnm = sep.find(r => r.company === MNM);
        const none = sep.find(r => r.company === NO_COMPANY);

        t('tiga baris: dua PT + tanpa perusahaan', sep.length, 3);
        t('urut dari jam terbanyak', sep[0].company, GPA);
        t('GPA total jam = 12j (8 + 4)', gpa.minutes, 720);
        t('GPA jumlah laporan', gpa.reports, 2);
        t('GPA jumlah anggota', gpa.members, 2);
        t('GPA unit ditangani', gpa.units, 2);
        t('GPA disetujui / menunggu', [gpa.approved, gpa.pending], [1, 1]);
        t('MNM shift malam lewat tengah malam = 8j', mnm.minutes, 480);
        t('MNM perlu revisi', mnm.revision, 1);
        t('tanpa perusahaan tetap dihitung, bukan dibuang', none.reports, 1);
        t('bulan lain tidak ikut (Agustus 10j tidak masuk)',
          sep.reduce((n,r)=>n+r.minutes,0), 720 + 480 + 180);
        t('Agustus punya rekapnya sendiri', companyRecap('2026-08')[0].minutes, 600);
        t('bulan kosong = tidak ada baris', companyRecap('2026-07').length, 0);

        switchLeaderTab('company');
        t('tabel rekap terender', document.querySelectorAll('#recapBody tr').length, 3);
        const footCells = document.querySelectorAll('#recapFoot td');
        t('baris total menjumlah jam', footCells[2].textContent.trim(), formatMinutes(720+480+180));
        t('baris total menjumlah laporan', footCells[1].textContent.trim(), '4');
        shiftRecapMonth(-1);
        t('mundur satu bulan', recapMonth, '2026-08');
        shiftRecapMonth(1);
        t('maju lagi', recapMonth, '2026-09');

        // ================= RINGKASAN MINGGUAN =================
        weekAnchor = '2026-09-15';                 // minggu 14-20 Sep
        const now = weekStats('2026-09-14');
        const prev = weekStats('2026-09-07');
        t('minggu ini: 4 laporan', now.reports, 4);
        t('minggu ini: jam = 8+4+8+3', now.minutes, 480+240+480+180);
        t('minggu ini: 4 orang melapor', now.people, 4);
        t('minggu ini: hari-orang bertugas = 2 (libur tidak dihitung)', now.onDuty, 2);
        t('minggu ini: kerusakan baru = 2', now.damages, 2);
        t('minggu ini: kerusakan selesai = 1', now.resolved, 1);
        t('minggu ini: barang keluar = 4 (yang masuk tidak dihitung)', now.stockOut, 4);
        t('minggu lalu: 1 laporan', prev.reports, 0);
        t('minggu lalu: 1 kerusakan', prev.damages, 1);
        t('minggu lalu: bertugas 1', prev.onDuty, 1);
        t('minggu lalu: barang keluar 1', prev.stockOut, 1);

        switchLeaderTab('week');
        const metrics = document.querySelectorAll('#weekSummaryGrid .week-metric');
        t('tujuh metrik terender', metrics.length, 7);
        const txt = document.getElementById('weekSummaryGrid').textContent;
        t('label minggu tampil',
          document.getElementById('weekSummaryLabel').textContent, '14 – 20 Sep 2026');
        // Kerusakan naik dari 1 ke 2 -> itu kabar BURUK, harus merah
        const dmg = [...metrics].find(m => m.textContent.includes('Kerusakan baru'));
        t('kerusakan naik ditandai buruk',
          !!dmg.querySelector('.week-metric__delta--bad'), true);
        // Laporan naik dari 0 ke 4 -> kabar BAIK
        const rep = [...metrics].find(m => m.textContent.includes('Laporan harian'));
        t('laporan naik ditandai baik',
          !!rep.querySelector('.week-metric__delta--good'), true);
        t('kesehatan armada disebut sebagai angka sekarang',
          /angka sekarang/.test(document.getElementById('weekHealth').textContent), true);
        shiftSummaryWeek(-1);
        t('mundur satu minggu',
          document.getElementById('weekSummaryLabel').textContent, '7 – 13 Sep 2026');

        // ================= TAB =================
        switchLeaderTab('inbox');
        t('tab kotak aktif', document.getElementById('leaderInboxPanel').style.display, '');
        t('tab rekap tersembunyi', document.getElementById('leaderCompanyPanel').style.display, 'none');
        switchLeaderTab('ngaco');
        t('tab tak dikenal jatuh ke kotak', leaderTab, 'inbox');

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
