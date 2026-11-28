// 2.11.0 — Kalender laporan: per hari berapa yang melapor dari yang
// diharapkan, siapa yang belum, dan yang izin / libur tidak dihitung.
const { launch, BASE_URL } = require('./_env');

(async () => {
    const b = await launch();
    const page = await b.newPage({ viewport: { width: 1440, height: 1000 } });
    const errors = [];
    page.on('pageerror', e => { if (!/Chart is not defined/.test(e.message)) errors.push(e.message); });
    await page.goto(BASE_URL + '/index.html', { waitUntil: 'load' });
    await page.waitForTimeout(1000);

    await page.addScriptTag({ content: `(async () => { try {
        const T = []; const t = (n, g, w) => T.push({ n, g, w, pass: JSON.stringify(g) === JSON.stringify(w) });
        const tick = ms => new Promise(r => setTimeout(r, ms || 30));
        try { localStorage.removeItem('rcCollapsed'); } catch (_) {}
        window.cloud = { isReady: true };
        currentUser = { uid: 'o', email: 'o@x.id' };
        currentUserDoc = { uid: 'o', role: 'owner', status: 'active', email: 'o@x.id' };
        hideAuthGates(); applyRoleGating();
        const M = [{ id: 'a', name: 'Andi Pratama', company: 'PT. A', active: true },
                   { id: 'b', name: 'Budi', company: 'PT. A', active: true },
                   { id: 'c', name: 'Citra Dewi', company: 'PT. B', active: true },
                   { id: 'd', name: 'Dodi', company: 'PT. B', active: true }];
        const d1 = new Date(); d1.setDate(d1.getDate() - 1);
        const day = toISODate(d1);            // kemarin: selalu sudah lewat
        const L = [{ id: 'w1', date: day, memberId: 'a', start: '07:00', end: '15:00', task: 'x', approval: 'approved', createdByUid: 'o' },
                   { id: 'w2', date: day, memberId: 'c', start: '07:00', end: '11:00', task: 'x', approval: 'pending', createdByUid: 'o' },
                   { id: 'w3', date: day, memberId: 'b', start: '07:00', end: '11:00', task: 'x', approval: 'draft', createdByUid: 'o' }];
        const LV = [{ id: 'l1', memberId: 'd', dateFrom: day, dateTo: day, approval: 'approved' }];
        teamMembers = M.slice(); workLogs = L.slice(); leaveRequests = LV.slice(); teamShifts = [];
        navigateTo('team'); switchTeamTab('worklog');
        teamMembers = M.slice(); workLogs = L.slice(); leaveRequests = LV.slice(); renderWorkLogTable(); await tick();
        const st = reportDayStatus(day);
        t('dilaporkan: hanya yang terkirim (draf tidak)', st.reported.map(r => r.m.id), ['a', 'c']);
        t('yang cuti disetujui tidak diharapkan', [st.expected.map(m => m.id), st.excused.map(x => [x.m.id, x.why])], [['a', 'b', 'c'], [['d', 'Leave / sick']]]);
        t('belum melapor', st.missing.map(m => m.id), ['b']);
        _rcMonth = day.slice(0, 7); renderReportCalendar();
        const cell = [...document.querySelectorAll('#reportCalendar .rc-day')].find(x => (x.getAttribute('aria-label') || '').startsWith(day));
        t('sel hari: 2 dari 3, sebagian', [cell.querySelector('.rc-day__count').textContent, cell.classList.contains('rc-day--part')], ['2/3', true]);
        t('inisial yang melapor', [...cell.querySelectorAll('.rc-face')].map(x => x.textContent), ['AP', 'CD']);
        cell.click(); await tick();
        const det = document.querySelector('#reportCalendar .rc-detail');
        t('klik hari membuka rincian', [!!det, det.querySelector('.rc-detail__title--ok').textContent, det.querySelector('.rc-detail__title--bad').textContent], [true, 'Reported (2)', 'Not reported (1)']);
        t('status persetujuan per laporan', [...det.querySelectorAll('.appr')].map(x => x.textContent), ['Approved', 'Pending']);
        t('yang cuti tampil sebagai tidak diharapkan', /Not expected \\(1\\)/.test(det.textContent) && /Leave \\/ sick/.test(det.textContent), true);
        rcShowInTable(day); await tick();
        t('tampilkan di tabel: filter tanggal diisi', [document.getElementById('wlFrom').value, document.getElementById('wlTo').value], [day, day]);
        // shift libur
        teamShifts = [{ id: day + '_a', shift: 'pagi' }, { id: day + '_b', shift: 'libur' }, { id: day + '_c', shift: 'pagi' }];
        const st2 = reportDayStatus(day);
        t('dengan jadwal: shift libur tidak diharapkan', [st2.expected.map(m => m.id), st2.missing.length], [['a', 'c'], 0]);
        teamShifts = [];
        // filter perusahaan
        document.getElementById('wlCompanyFilter').innerHTML += '<option value="PT. B">PT. B</option>';
        document.getElementById('wlCompanyFilter').value = 'PT. B';
        t('ikut filter perusahaan', reportDayStatus(day).reported.map(r => r.m.id), ['c']);
        document.getElementById('wlCompanyFilter').value = '';
        // sembunyikan
        rcToggle(); await tick();
        t('bisa disembunyikan', document.querySelectorAll('#reportCalendar .rc-day').length, 0);
        rcToggle(); await tick();
        t('dan ditampilkan lagi', document.querySelectorAll('#reportCalendar .rc-day').length > 0, true);
        // pemegang Own only tidak melihat kalender tim
        currentUserDoc = { uid: 'x', role: 'staff', status: 'active', access: { teamLog: 'own' } }; currentUser = { uid: 'x', email: 'x@x.id' };
        applyRoleGating(); renderReportCalendar();
        t('Own only: kalender tim disembunyikan', document.getElementById('reportCalendar').hidden, true);
        window.__T = T;
    } catch (e) { window.__T = [{ n: 'CRASH ' + e.message + ' ' + (e.stack || '').split('\\n')[1], pass: false }]; } })()` });

    await page.waitForFunction(() => window.__T, null, { timeout: 30000 });
    const T = await page.evaluate(() => window.__T);
    let pass = 0;
    for (const x of T) {
        if (x.pass) pass++;
        console.log(`${x.pass ? 'PASS' : 'FAIL'}  ${x.n}`);
        if (!x.pass && 'g' in x) console.log(`        dapat=${JSON.stringify(x.g)} harap=${JSON.stringify(x.w)}`);
    }
    console.log(`\n${pass}/${T.length} lulus`);
    console.log('PAGE ERRORS:', errors.length ? errors : 'none');
    await b.close();
    process.exit(pass === T.length && !errors.length ? 0 : 1);
})();
