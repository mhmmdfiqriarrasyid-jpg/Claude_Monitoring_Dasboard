// 2.41.0 — Calendar (owner only): shifts & leave, Daily Report, tickets, inspections,
// licenses, projects (with milestone target dates) and damage, from the records already
// loaded. Sample data: tests/_weekly_seed.js (week Sat 31 Oct – Fri 6 Nov 2026).
const { launch, BASE_URL } = require('./_env');
const path = require('path');
(async () => {
    const b = await launch();
    const page = await b.newPage({ viewport: { width: 1366, height: 900 } });
    const errors = [];
    page.on('pageerror', e => { if (!/Chart is not defined/.test(e.message)) errors.push(e.message); });
    await page.addInitScript({ path: path.join(__dirname, '_weekly_seed.js') });
    await page.goto(BASE_URL + '/index.html', { waitUntil: 'load' });
    await page.waitForTimeout(900);
    const T = await page.evaluate(async () => {
        const T = []; const t = (n, g, w) => T.push({ n, g, w, pass: JSON.stringify(g) === JSON.stringify(w) });
        const toasts = []; window.showToast = m => toasts.push(String(m));
        window.__seed();
        projects[1].target.milestones[2].due = '2026-11-20';
        const E = calendarEntries('2026-10-31', '2026-11-30', '2026-11-12');
        const txt = d => (E[d] || []).map(e => `${e.cat}:${e.text}`);
        t('shift & cuti', [txt('2026-11-01').includes('team:Morning 3 · Off 3'), txt('2026-11-04').includes('team:Leave: Paulus Baru')], [true, true]);
        t('Daily Report: siapa yang tidak lapor (cuti & libur tidak dihitung)', [txt('2026-11-03').includes('report:Daily Report 5/6 · missing Febri'), txt('2026-11-04').includes('report:Daily Report 5/5')], [true, true]);
        t('ticket: dibuka, selesai, batas SLA, lewat SLA muncul di hari ini', [txt('2026-11-02').includes('ticket:Opened TK-261102-0800AB'), txt('2026-11-04').includes('ticket:Resolved TK-261103-1000CD'),
            txt('2026-11-02').includes('ticket:Due TK-261102-0800AB'), txt('2026-11-12').includes('ticket:Overdue TK-261102-0800AB')], [true, true, true, true]);
        t('inspeksi: jatuh tempo per unit, yang overdue di hari ini', [txt('2026-11-12').some(x => /^inspection:\d+ inspections? overdue$/.test(x)), Object.values(E).flat().some(e => e.cat === 'inspection' && /^Inspection due: /.test(e.text))], [true, true]);
        t('lisensi berakhir dikelompokkan per hari', txt('2026-11-20').includes('license:GPS license ends: GGTR101G, GGTR102G, GGTR103G'), true);
        t('project: deadline terlambat, target milestone', [(E['2026-10-31'] || []).find(e => e.text === 'Project due: RTK radio repeater stage 2').tone, txt('2026-11-20').includes('project:Target: Install')], ['bad', true]);
        t('kerusakan baru & selesai diperbaiki', [txt('2026-11-02').includes('damage:Damage: GGTR107G'), txt('2026-11-02').includes('damage:Repaired: GGTR109G')], [true, true]);
        // Owner only.
        currentUser = { uid: 'v', email: 'v@x.id' }; currentUserDoc = { uid: 'v', role: 'staff', status: 'active', access: { weeklyReport: 'view', tickets: 'edit' } }; applyRoleGating();
        const link = () => document.querySelector('.nav__link[data-view="calendar"]');
        navigateTo('calendar');
        t('bukan owner: menu tersembunyi, halaman ditolak', [link().style.display, currentView, toasts.pop()], ['none', 'dashboard', 'Owner only']);
        currentUser = { uid: 'o', email: 'o@x.id' }; currentUserDoc = { uid: 'o', role: 'owner', status: 'active' }; applyRoleGating();
        navigateTo('calendar'); calSetMode('month'); calSelect('2026-11-03');
        t('owner: menu tampil, tampilan Bulan + detail hari', [link().style.display !== 'none', currentView, document.querySelectorAll('#calHost .cal-cell').length % 7, /Tuesday, 3 November 2026/.test(document.querySelector('.cal-detail h3').textContent)], [true, 'calendar', 0, true]);
        calToggleCat('team');
        t('filter: kategori dimatikan hilang dari tampilan', [...document.querySelectorAll('.cal-detail .cal-e__k')].some(e => e.textContent === 'Shifts & leave'), false);
        calToggleCat('team');
        calSetMode('agenda');
        t('tampilan Agenda: daftar per hari', document.querySelectorAll('#calHost .cal-aday').length > 5, true);
        const btn = [...document.querySelectorAll('.cal-e__b')].find(x => /Due TK-261104-1300GH/.test(x.textContent)); btn.click(); await new Promise(r => setTimeout(r, 200));
        t('klik item ticket membuka ticket-nya', currentView, 'tickets');
        // Milestone target date in the project form.
        _pjMilestones = [{ id: 'm1', label: 'Survey', weight: 50, done: false, due: '2026-12-01' }];
        renderProjectFormLists();
        t('form milestone: kolom target tanggal', document.querySelector('#pjMilestones .pj-due').value, '2026-12-01');
        return T;
    });
    let pass = 0;
    for (const x of T) { if (x.pass) pass++; console.log(`${x.pass ? 'PASS' : 'FAIL'}  ${x.n}`); if (!x.pass) console.log(`        dapat=${JSON.stringify(x.g)} harap=${JSON.stringify(x.w)}`); }
    console.log(`\n${pass}/${T.length} lulus`);
    console.log('PAGE ERRORS:', errors.length ? errors : 'none');
    await b.close();
    process.exit(pass === T.length && !errors.length ? 0 : 1);
})();
