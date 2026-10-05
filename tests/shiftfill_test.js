// Jadwal shift tanpa klik satu per satu: "Fill week…" per anggota
// (pola Senin–Sabtu + Minggu Off, setiap hari, sama dengan minggu lalu,
// kosongkan) dan tombol Copy Last Week untuk seluruh tim. Setiap aksi =
// SATU kiriman batch dan SATU baris riwayat, bukan satu per sel.
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
        const tick = ms => new Promise(r => setTimeout(r, ms || 20));
        const batches = [], singles = [], toasts = [], history = [];
        let fail = false, confirmAnswer = true, confirmText = '';
        window.cloud = { isReady: true, addHistoryEvents: ok => Promise.resolve(),
            saveShift: r => { singles.push(r.id); return Promise.resolve(); },
            deleteShift: id => { singles.push('del:' + id); return Promise.resolve(); },
            saveShiftChanges: (sets, dels) => { batches.push({ sets: sets.map(r => r.id + '=' + r.shift), dels: dels.slice() });
                return fail ? Promise.reject(Object.assign(new Error('x'), { code: 'permission-denied' })) : Promise.resolve(); } };
        window.showToast = m => toasts.push(String(m));
        window.confirm = m => { confirmText = String(m); return confirmAnswer; };
        const realLog = logEvent; window.logEvent = e => { history.push(e); };
        currentUser = { uid: 'u1', email: 'o@x.id' };
        currentUserDoc = { role: 'owner', status: 'active', email: 'o@x.id', displayName: 'Boss' };
        hideAuthGates(); applyRoleGating();
        teamMembers = [
            { id: 'm1', name: 'Farel', company: 'PT. A', active: true },
            { id: 'm2', name: 'Yuda', company: 'PT. A', active: true },
            { id: 'm3', name: 'Wahyudi', company: 'PT. B', active: true }];
        const wk = '2026-10-05';   // Senin
        const prev = '2026-09-28';
        const D = i => addDaysISO(wk, i), P = i => addDaysISO(prev, i);
        teamShifts = [];
        for (let i = 0; i < 7; i++) {
            teamShifts.push({ id: P(i) + '_m1', date: P(i), memberId: 'm1', shift: i === 6 ? 'libur' : 'pagi' });
            teamShifts.push({ id: P(i) + '_m2', date: P(i), memberId: 'm2', shift: i === 6 ? 'libur' : 'malam' });
        }
        teamWeekStart = wk;
        navigateTo('team');
        renderShiftGrid();
        const week = id => [0,1,2,3,4,5,6].map(i => shiftFor(id, D(i)) || '');

        // =============== tampilan ===============
        t('setiap anggota punya pilihan Fill week', document.querySelectorAll('#shiftBody .shift-fill').length, 3);
        t('tombol Copy Last Week ada', !!document.querySelector('[onclick="copyLastWeekShifts()"]'), true);
        t('pilihan pola', [...document.querySelector('#shiftBody .shift-fill').options].map(o => o.textContent.trim()),
          ['Fill week…', 'Morning Mon–Sat · Sun Off', 'Afternoon Mon–Sat · Sun Off', 'Night Mon–Sat · Sun Off',
           'Morning every day', 'Night every day', 'Same as last week', 'Clear this week']);

        // =============== isi satu baris ===============
        const sel = document.querySelectorAll('#shiftBody .shift-fill')[2];   // Wahyudi
        sel.value = 'pagi'; sel.dispatchEvent(new Event('change'));
        await tick();
        t('Morning Mon–Sat, Minggu Off', week('m3'), ['pagi','pagi','pagi','pagi','pagi','pagi','libur']);
        t('satu kiriman batch, bukan 7', [batches.length, singles.length, batches[0].sets.length], [1, 0, 7]);
        t('satu baris riwayat', history.length, 1);
        t('riwayat menyebut pola dan jumlah sel', [history[0].unitName, /Morning Mon–Sat · Sun Off — 7 cell\\(s\\)/.test(history[0].after)], ['[Team] Wahyudi', true]);
        t('pilihan kembali ke "Fill week…"', document.querySelectorAll('#shiftBody .shift-fill')[2].value, '');
        t('toast tersimpan', toasts.includes('7 shift cell(s) saved'), true);

        quickFillShiftRow('m3', 'pagi');
        t('pola yang sama lagi: tidak mengirim apa-apa', [batches.length, toasts[toasts.length - 1]], [1, 'Nothing to change — the week already looks like that']);

        quickFillShiftRow('m3', 'malam7');
        t('Night every day: hanya sel yang berubah dikirim', [week('m3'), batches[1].sets.length], [['malam','malam','malam','malam','malam','malam','malam'], 7]);

        // kosongkan
        confirmAnswer = false;
        quickFillShiftRow('m3', 'clear');
        t('kosongkan minta konfirmasi; batal = tidak berubah', [/Clear every shift of Wahyudi/.test(confirmText), batches.length], [true, 2]);
        confirmAnswer = true;
        quickFillShiftRow('m3', 'clear');
        t('kosongkan menghapus 7 sel dalam satu batch', [week('m3').join(''), batches[2].dels.length, batches[2].sets.length], ['', 7, 0]);

        // sama dengan minggu lalu (per anggota)
        quickFillShiftRow('m1', 'copy');
        t('Same as last week per anggota', week('m1'), ['pagi','pagi','pagi','pagi','pagi','pagi','libur']);
        quickFillShiftRow('m3', 'copy');
        t('minggu lalu kosong: diberi tahu, tidak mengirim', [toasts[toasts.length - 1], batches.length], ['Wahyudi has no shifts last week to copy', 4]);

        // =============== Copy Last Week (seluruh tim) ===============
        teamShifts.push({ id: D(0) + '_m2', date: D(0), memberId: 'm2', shift: 'pagi' });   // sudah diisi, beda
        confirmAnswer = false;
        copyLastWeekShifts();
        t('konfirmasi menyebut jumlah sel dan yang ditimpa', [/7 cell\\(s\\) will be filled, 1 of them replacing/.test(confirmText), batches.length], [true, 4]);
        confirmAnswer = true;
        copyLastWeekShifts();
        t('Copy Last Week mengisi anggota lain', week('m2'), ['malam','malam','malam','malam','malam','malam','libur']);
        t('yang sudah sama tidak dikirim ulang', batches[4].sets.length, 7);
        t('anggota yang minggu lalu kosong tidak disentuh', week('m3').join(''), '');
        copyLastWeekShifts();
        t('sudah sama dengan minggu lalu: diberi tahu', toasts[toasts.length - 1], 'This week already matches last week');

        // =============== gagal → dikembalikan ===============
        fail = true;
        const before = week('m3').join(',');
        quickFillShiftRow('m3', 'siang');
        await tick(); await tick();
        t('ditolak server: semua sel dikembalikan', week('m3').join(','), before);
        t('dan diberi tahu', toasts.includes('Failed to save the shifts — changes reverted'), true);
        fail = false;

        // =============== hanya lihat ===============
        currentUserDoc = { role: 'staff', status: 'active', access: { teamShift: 'view' } }; applyRoleGating();
        renderShiftGrid();
        t('akses lihat: tidak ada Fill week', document.querySelectorAll('#shiftBody .shift-fill').length, 0);
        const n = batches.length;
        quickFillShiftRow('m3', 'pagi');
        t('akses lihat: tidak bisa mengisi lewat fungsi', batches.length, n);

        window.logEvent = realLog;
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
