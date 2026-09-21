// Langganan jadwal shift dibatasi jendela tanggal.
//
// Koleksi `shifts` bertambah satu dokumen per orang per hari dan tidak pernah
// berhenti — delapan orang menghasilkan ±2.900 dokumen setahun, ±8.800 di tahun
// ketiga, dan semuanya ditarik hanya untuk menggambar satu minggu.
//
// Bahaya dari pembatasan seperti ini adalah data yang hilang diam-diam: minggu
// lama terlihat kosong padahal jadwalnya ada. Jadi yang paling penting diuji di
// sini bukan penghematannya, melainkan bahwa **mundur ke minggu lama tetap
// memuat datanya** — jendelanya melebar, bukan menyembunyikan.
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

        // Tiap langganan mencatat batas tanggal yang diminta, supaya bisa
        // dibuktikan kapan ia melebar dan kapan tidak.
        const subs = [];
        let unsubCount = 0;
        window.cloud = { isReady: true,
            subscribeShifts: (cb, err, since) => { subs.push(since || null);
                return () => { unsubCount++; }; },
            saveShift: () => Promise.resolve(), deleteShift: () => Promise.resolve(),
            saveTeamMember: () => Promise.resolve(), deleteTeamMember: () => Promise.resolve(),
            saveUnits: () => Promise.resolve(), getAllUnits: () => Promise.resolve([]),
            addHistoryEvents: () => Promise.resolve(), subscribeUsers: () => () => {} };

        currentUser = { uid:'uO', email:'o@x.id' };
        currentUserDoc = { role:'owner', status:'active' };
        hideAuthGates(); applyRoleGating();
        teamMembers = [{ id:'m1', name:'Andi', company:'PT. GPA', active:true }];
        teamShifts = []; workLogs = []; globalData = [];

        const iso = d => toISODate(d);
        const daysBetween = (a, b) => Math.round(
            (new Date(b + 'T00:00:00') - new Date(a + 'T00:00:00')) / 86400000);

        // ---------- langganan pertama dibatasi ----------
        teamWeekStart = startOfWeekISO(toISODate());
        startShiftsSubscription();
        t('langganan membawa batas tanggal, bukan seluruh koleksi',
          typeof subs[0], 'string');
        t('batasnya berbentuk tanggal ISO', /^\\d{4}-\\d{2}-\\d{2}$/.test(subs[0]), true);
        const back = daysBetween(subs[0], iso(new Date()));
        t('jendelanya mundur kira-kira empat bulan',
          back >= SHIFT_WINDOW_DAYS && back <= SHIFT_WINDOW_DAYS + 35, true);

        // ---------- mundur di dalam jendela: tidak perlu langganan baru ----------
        // Menganti langganan itu mahal; kalau datanya sudah ada, jangan.
        const sebelum = subs.length;
        shiftWeekShift(-1);
        shiftWeekShift(-1);
        t('mundur dua minggu tidak bikin langganan baru', subs.length, sebelum);

        // ---------- mundur melewati jendela: harus melebar ----------
        // Ini bagian yang menentukan. Kalau ia TIDAK melebar, minggu lama akan
        // tampil kosong padahal jadwalnya ada di server — persis kegagalan
        // senyap yang harus dihindari.
        const lamaSekali = startOfWeekISO('2020-03-02');
        teamWeekStart = lamaSekali;
        ensureShiftWindowCovers(lamaSekali);
        t('mundur jauh memicu langganan baru', subs.length, sebelum + 1);
        const batasBaru = subs[subs.length - 1];
        t('langganan lama dilepas dulu', unsubCount >= 1, true);
        t('jendela baru mencakup minggu yang diminta', batasBaru <= lamaSekali, true);
        t('dan masih memberi ruang di belakangnya',
          daysBetween(batasBaru, lamaSekali) >= 14, true);

        // Sekali melebar, mundur lagi di dalamnya tidak memicu apa-apa.
        const sesudahLebar = subs.length;
        ensureShiftWindowCovers(addDaysISO(lamaSekali, 7));
        t('tidak melebar dua kali untuk minggu yang sudah tercakup',
          subs.length, sesudahLebar);

        // ---------- ringkasan mingguan mundur satu minggu ekstra ----------
        // Ia membandingkan dengan minggu SEBELUM yang ditampilkan, jadi
        // jendelanya harus menjangkau lebih jauh dari labelnya.
        // Kembalikan ke minggu ini dulu, atau jendelanya masih selebar
        // pelebaran sebelumnya dan tidak akan melebar lagi.
        teamWeekStart = startOfWeekISO(toISODate());
        startShiftsSubscription();               // reset ke jendela normal
        const patokan = subs.length;
        weekAnchor = startOfWeekISO(addDaysISO(toISODate(), -(SHIFT_WINDOW_DAYS - 3)));
        shiftSummaryWeek(0);
        t('ringkasan ikut melebarkan jendela untuk minggu pembanding',
          subs.length, patokan + 1);

        // ---------- dialog hapus anggota tidak mengaku jumlah shift ----------
        // teamShifts sekarang cuma berisi jendela, jadi menghitungnya akan
        // memberi angka yang lebih kecil dari kenyataan.
        let ditanya = '';
        window.confirm = m => { ditanya = String(m); return false; };
        teamShifts = [{ id:'2026-09-14_m1', date:'2026-09-14', memberId:'m1', shift:'pagi' }];
        workLogs = [{ id:'w1', date:'2026-09-14', memberId:'m1', memberName:'Andi',
                      start:'07:00', end:'16:00', task:'x' }];
        deleteTeamMember('m1');
        t('tidak menyebut angka jadwal shift', /\\d+ jadwal shift/.test(ditanya), false);
        t('tetap menyebut jumlah laporan yang memang lengkap',
          /1 laporan harian/.test(ditanya), true);
        t('dan tetap menjelaskan akibatnya',
          /anggota dihapus/.test(ditanya), true);

        // ---------- keluar sesi membersihkan penanda jendela ----------
        tearDownCloudSync();
        t('penanda jendela dibersihkan saat sesi dibongkar', _shiftWindowStart, '');
        // Setelah dibersihkan, pemeriksaan jendela tidak boleh menebak-nebak.
        const setelahBongkar = subs.length;
        ensureShiftWindowCovers('2019-01-07');
        t('tanpa langganan aktif, tidak ada langganan liar', subs.length, setelahBongkar);

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
