const { launch, BASE_URL } = require('./_env');

(async () => {
    const b = await launch();
    const page = await b.newPage({ viewport: { width: 1440, height: 1000 } });
    const errors = [];
    page.on('pageerror', e => { if (!/Chart is not defined/.test(e.message)) errors.push(e.message); });
    await page.goto(BASE_URL + '/index.html', { waitUntil: 'load' });
    await page.waitForTimeout(1000);

    await page.addScriptTag({ content: `(() => { try {
        const T = []; const t = (n, g, w) => T.push({ n, g, w, pass: JSON.stringify(g) === JSON.stringify(w) });
        const DAY = 86400000;
        window.cloud = { isReady: true, subscribeUsers: cb => { cb(allUsers); return () => {}; } };
        currentUser = { uid:'boss', email:'boss@x.id' };
        currentUserDoc = { role:'owner', status:'active', email:'boss@x.id' };
        hideAuthGates();

        globalData = [
            // Rusak 5 hari -> lewat ambang 3 hari
            { id:'u_1', name:'JD-01', sn:'SN1', site:'Timika', status:'Breakdown',
              breakdownStartedAt: Date.now() - 5*DAY, breakdownReason:'Sensor mati' },
            // Rusak baru 1 hari -> BELUM masuk kotak
            { id:'u_2', name:'JD-02', sn:'SN2', site:'Timika', status:'Breakdown',
              breakdownStartedAt: Date.now() - 1*DAY },
            { id:'u_3', name:'JD-03', sn:'SN3', site:'Timika', status:'Good' }
        ];
        teamMembers = [{ id:'m1', name:'Andi', active:true }];
        teamShifts = [];
        workLogs = [
            { id:'w1', date:'2026-09-15', memberId:'m1', memberName:'Andi', task:'A', approval:'pending' },
            { id:'w2', date:'2026-09-15', memberId:'m1', memberName:'Andi', task:'B' },              // tanpa field = pending
            { id:'w3', date:'2026-09-14', memberId:'m1', memberName:'Andi', task:'C', approval:'approved' },
            { id:'w4', date:'2026-09-14', memberId:'m1', memberName:'Andi', task:'D',
              approval:'revision', revisionNote:'Jam kosong' }
        ];
        warehouseDevices = [
            { id:'d1', sn:'S1', type:'GPS', status:'damaged' },
            { id:'d2', sn:'S2', type:'Display', status:'repair' },
            { id:'d3', sn:'S3', type:'JDLink', status:'warehouse' }
        ];
        stockLedger = [
            { id:'s1', date:'2026-09-10', txnType:'IN',  itemName:'Filter Oli', qty:20 },
            { id:'s2', date:'2026-09-12', txnType:'OUT', itemName:'Filter Oli', qty:18 },   // sisa 2 -> menipis
            { id:'s3', date:'2026-09-10', txnType:'IN',  itemName:'Selang', qty:3 },
            { id:'s4', date:'2026-09-12', txnType:'OUT', itemName:'Selang', qty:3 },        // sisa 0 -> habis
            { id:'s5', date:'2026-09-10', txnType:'IN',  itemName:'Oli', qty:50 }           // aman
        ];
        allUsers = [
            { uid:'p1', email:'baru@x.id', displayName:'Pendaftar', role:'viewer', status:'pending' },
            { uid:'p2', email:'kosong@x.id', displayName:'Tanpa Akses', role:'khl', status:'active' },
            { uid:'boss', email:'boss@x.id', role:'owner', status:'active' }
        ];
        globalLicenseStock = [];
        navigateTo('leader');

        const byKey = k => decisionGroups().find(g => g.key === k);

        // ---------- hitungan tiap kelompok ----------
        t('laporan menunggu = 2 (termasuk yang tanpa field)', byKey('approval').total, 2);
        t('laporan perlu revisi = 1', byKey('revision').total, 1);
        t('unit rusak >3 hari = 1 (yang 1 hari tidak dihitung)', byKey('breakdown').total, 1);
        t('detail unit rusak menyebut lama dan alasan',
          byKey('breakdown').items[0].sub.includes('5 hari') && byKey('breakdown').items[0].sub.includes('Sensor mati'), true);
        t('perangkat rusak/perbaikan = 2', byKey('devices').total, 2);
        t('stok habis/menipis = 2 (Oli aman)', byKey('stock').total, 2);
        t('stok habis ditandai', byKey('stock').items.some(i => i.sub === 'habis'), true);
        t('pendaftar menunggu = 1', byKey('users').total, 1);
        t('akun aktif tanpa akses = 1', byKey('noaccess').total, 1);
        t('owner sendiri tidak dihitung tanpa-akses',
          byKey('noaccess').items.every(i => !String(i.text).includes('boss')), true);

        // ---------- warna keparahan ----------
        t('stok habis -> nada bahaya', byKey('stock').tone, 'danger');
        t('perangkat -> nada peringatan', byKey('devices').tone, 'warning');

        // ---------- total dan tampilan ----------
        const total = decisionTotal();
        t('total = jumlah semua kelompok',
          total, decisionGroups().reduce((n,g)=>n+g.total,0));
        t('total tampil di KPI', document.getElementById('decisionTotal').textContent, String(total));
        t('kartu terender', document.querySelectorAll('#decisionGroups .decision-card').length, decisionGroups().length);
        t('lencana menu terisi', document.getElementById('decisionBadge').textContent, String(total));

        // ---------- kelompok kosong tidak muncul ----------
        workLogs = workLogs.filter(w => workLogApproval(w) !== 'revision');
        t('kelompok revisi hilang saat kosong', !!byKey('revision'), false);

        // ---------- kotak bersih ----------
        const keep = { g: globalData, w: workLogs, d: warehouseDevices, s: stockLedger, u: allUsers };
        globalData = []; workLogs = []; warehouseDevices = []; stockLedger = []; allUsers = [];
        renderDecisionInbox();
        t('kotak kosong -> total 0', decisionTotal(), 0);
        t('kotak kosong -> pesan lega muncul',
          !!document.querySelector('#decisionGroups .decision-clear'), true);
        updateDecisionBadge();
        t('kotak kosong -> lencana disembunyikan',
          document.getElementById('decisionBadge').style.display, 'none');
        globalData = keep.g; workLogs = keep.w; warehouseDevices = keep.d;
        stockLedger = keep.s; allUsers = keep.u;

        // ---------- hak akses dihormati ----------
        currentUserDoc = { role:'staff', status:'active',
                           access:{ leader:'view', teamLog:'view' } };   // hanya laporan
        const keys = decisionGroups().map(g => g.key);
        t('tanpa akses gudang: kelompok stok tidak muncul', keys.includes('stock'), false);
        t('tanpa akses gudang: kelompok perangkat tidak muncul', keys.includes('devices'), false);
        t('tanpa akses armada: lisensi & breakdown tidak muncul',
          keys.includes('license') || keys.includes('breakdown'), false);
        t('bukan owner: pendaftar tidak muncul', keys.includes('users'), false);
        t('yang boleh dilihat tetap muncul', keys.includes('approval'), true);

        currentUserDoc = { role:'khl', status:'active', access:{ leader:'none' } };
        t('tanpa akses leader: menu tertutup', canViewView('leader'), false);
        applyAccessVisibility();
        t('tanpa akses leader: link nav disembunyikan',
          document.querySelector('.nav__link[data-view="leader"]').style.display, 'none');
        t('tanpa akses leader: lencana disembunyikan',
          document.getElementById('decisionBadge').style.display, 'none');

        // leader hanya-baca: tidak boleh dianggap izin edit
        currentUserDoc = { role:'khl', status:'active', access:{ leader:'view' } };
        t('leader bukan area edit', canEditAnyArea(), false);
        t('leader tidak masuk DATA_AREAS', DATA_AREAS.includes('leader'), false);

        currentUserDoc = { role:'owner', status:'active' };
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
