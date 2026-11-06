// 2.31.0 — Weekly Report: Sat–Fri week, team and manager versions, from the
// Daily Report, tickets, jobs, units, damage, licenses and projects.
// Sample data: tests/_weekly_seed.js (week 45, Sat 31 Oct – Fri 6 Nov 2026).
const { launch, BASE_URL } = require('./_env');
const path = require('path');
(async () => {
    const b = await launch();
    const ctx = await b.newContext({ viewport: { width: 1366, height: 900 }, acceptDownloads: true });
    const page = await ctx.newPage();
    const errors = [];
    page.on('pageerror', e => { if (!/Chart is not defined/.test(e.message)) errors.push(e.message); });
    await page.addInitScript({ path: path.join(__dirname, '_weekly_seed.js') });
    await page.goto(BASE_URL + '/index.html', { waitUntil: 'load' });
    await page.waitForTimeout(1000);
    const T = await page.evaluate(() => {
        const T = []; const t = (n, g, w) => T.push({ n, g, w, pass: JSON.stringify(g) === JSON.stringify(w) });
        window.__seed();
        t('minggu Sabtu–Jumat', [wrWeekStart('2026-11-06'), wrWeekStart('2026-10-31'), wrWeekStart('2026-11-07'), wrWeekInfo('2026-10-31').no, wrWeekInfo('2026-10-31').to], ['2026-10-31', '2026-10-31', '2026-11-07', 45, '2026-11-06']);
        const D = buildWeeklyReport('2026-10-31', { today: '2026-11-06' });
        t('Daily Report: 34 dari 37, yang bolos tercatat', [D.team.filed, D.team.expected, D.team.pct, D.team.missed], [34, 37, 91.9, ['Febri (Tue)', 'Yohanis (Thu)', 'Agus (Fri)']]);
        t('cuti & libur tidak dihitung bolos', [D.team.members.find(m => m.name === 'Paulus Baru').days, D.team.members.find(m => m.name === 'Andreas Kayembob').days[1]], [['ok', 'off', 'ok', 'ok', 'leave', 'leave', 'ok'], 'off']);
        t('minggu lalu tanpa laporan: tidak dibandingkan', D.team.prevPct, null);
        t('ticket: masuk, selesai, terbuka, lewat due, tanpa PIC', [D.tickets.newIn, D.tickets.resolved, D.tickets.open, D.tickets.noPic, D.tickets.openList[0].no], [4, 1, 3, 1, 'TK-261102-0800AB']);
        t('jobs: 5 minggu ini, 3 selesai, 2 backlog, tautan ticket', [D.jobs.total, D.jobs.done, D.jobs.backlog.length, D.jobs.list[0].ticket], [5, 3, 2, 'TK-261102-0800AB']);
        t('precision: 23/26 traktor semua Good, heavy 7/8', [D.precision.pct, D.precision.heavyPct, D.precision.bySite.map(s => s.pt)], [88.5, 87.5, ['GPA', 'MNM']]);
        t('kerusakan: 3 baru, 1 selesai, 4 terbuka, 1 > 14 hari', [D.damage.newIn, D.damage.fixedIn, D.damage.open, D.damage.stale, D.damage.faults[0].unit], [3, 1, 4, 1, 'GGTR103G']);
        t('lisensi: 3 habis ≤ 30 hari, stok SF-RTK 1', [D.licenses.soon, D.licenses.stock.find(s => s[0] === 'SF-RTK')[1]], [3, 1]);
        t('project merah di urutan pertama', [D.projects[0].name, D.projects[0].rag], ['RTK radio repeater stage 2', 'red']);
        t('next week otomatis', D.nextWeek[0].startsWith('Close 2 urgent or overdue tickets'), true);
        const team = buildWeeklyPages(D, 'team', 'Barang radio datang Selasa');
        const mgmt = buildWeeklyPages(D, 'mgmt', '');
        t('versi tim: halaman', team.map(p => p.section || 'Cover'), ['Cover', 'Summary', 'Daily Report', 'Daily Report', 'Daily Report', 'Field tickets', 'Jobs & activities', 'Fleet & support', 'Projects', 'Next week']);
        t('versi manajer: halaman', mgmt.map(p => p.section || 'Cover'), ['Cover', 'Summary', 'Scorecard', 'Precision health', 'Licenses', 'Issues', 'Field tickets & jobs', 'Projects', 'Next week']);
        t('judul halaman berupa kesimpulan', [team[1].title, mgmt[1].title], ['34 of 37 daily reports came in; the team logged 291 hours on 5 paddocks', 'Precision health is 88.5%, short of the 95% target']);
        t('catatan sendiri masuk paling atas di Next week', team[team.length - 1].cols[0][0].items[0], 'Barang radio datang Selasa');
        // The view
        _wrWeek = '2026-10-31';
        navigateTo('weeklyReport');
        t('menu & halaman tampil untuk owner', [currentView, document.querySelectorAll('#wrPages .wr-page').length, document.getElementById('wrWeek').value], ['weeklyReport', 10, '2026-10-31']);
        setWeeklyReportAudience('mgmt');
        t('ganti ke versi manajer', [document.querySelectorAll('#wrPages .wr-page').length, document.querySelector('.wr-aud.active').dataset.aud], [9, 'mgmt']);
        // Access: a viewer without the area does not see it.
        currentUser = { uid: 'v', email: 'v@x.id' }; currentUserDoc = { uid: 'v', role: 'staff', status: 'active', access: { tickets: 'view' } }; applyRoleGating();
        t('tanpa akses Weekly Report: menu disembunyikan', document.querySelector('.nav__link[data-view="weeklyReport"]').style.display, 'none');
        currentUserDoc = { uid: 'v', role: 'staff', status: 'active', access: { weeklyReport: 'view' } }; applyRoleGating();
        t('dengan akses view: menu tampil', document.querySelector('.nav__link[data-view="weeklyReport"]').style.display, '');
        currentUser = { uid: 'o', email: 'o@x.id' }; currentUserDoc = { uid: 'o', role: 'owner', status: 'active', displayName: 'Rasyid' }; applyRoleGating();
        navigateTo('weeklyReport'); setWeeklyReportAudience('team');
        return T;
    });
    // PowerPoint download
    const [dl] = await Promise.all([page.waitForEvent('download', { timeout: 30000 }), page.evaluate(() => downloadWeeklyPptx())]);
    const f = await dl.path();
    const size = require('fs').statSync(f).size;
    T.push({ n: 'PowerPoint terunduh (nama file & isi)', g: [dl.suggestedFilename(), size > 20000], w: ['Weekly_Operation_Report_W45_2026-10-31.pptx', true], pass: dl.suggestedFilename() === 'Weekly_Operation_Report_W45_2026-10-31.pptx' && size > 20000 });
    // PDF: one sheet per page
    await page.evaluate(() => { const box = document.createElement('div'); box.id = 'wrPrint'; document.body.appendChild(box); box.innerHTML = document.getElementById('wrPages').innerHTML; document.body.classList.add('wr-printing'); });
    await page.emulateMedia({ media: 'print' });
    const pdf = await page.pdf({ preferCSSPageSize: true, printBackground: true });
    const pages = (pdf.toString('latin1').match(/\/Type\s*\/Page[^s]/g) || []).length;
    T.push({ n: 'PDF: 10 halaman (satu per lembar)', g: pages, w: 10, pass: pages === 10 });
    let pass = 0;
    for (const x of T) { if (x.pass) pass++; console.log(`${x.pass ? 'PASS' : 'FAIL'}  ${x.n}`); if (!x.pass) console.log(`        dapat=${JSON.stringify(x.g)} harap=${JSON.stringify(x.w)}`); }
    console.log(`\n${pass}/${T.length} lulus`);
    console.log('PAGE ERRORS:', errors.length ? errors : 'none');
    await b.close();
    process.exit(pass === T.length && !errors.length ? 0 : 1);
})();
