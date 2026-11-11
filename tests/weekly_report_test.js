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
    const T = await page.evaluate(async () => {
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
        t('next week otomatis (ticket mendesak dulu; jumlahnya ikut waktu karena SLA)', /^Close \d+ urgent or overdue tickets?: TK-261102-0800AB/.test(D.nextWeek[0]), true);
        const team = buildWeeklyPages(D, 'team', 'Barang radio datang Selasa');
        const mgmt = buildWeeklyPages(D, 'mgmt', '');
        t('tanpa galeri (media kosong): versi tim', team.map(p => p.section || 'Cover'), ['Cover', 'Summary', 'Daily Report', 'Daily Report', 'Daily Report', 'Field tickets', 'Jobs & activities', 'Jobs & activities', 'Fleet & support', 'Projects', 'Projects', 'Projects', 'Next week']);
        t('tanpa galeri: versi manajer', mgmt.map(p => p.section || 'Cover'), ['Cover', 'Precision health', 'Issue in field', 'Jobs & activities', 'Projects', 'Projects', 'Projects', 'Cover']);
        // 2.37.1: the management deck as the owner shaped it — cover, no eyebrow, 3 next-week points.
        // 2.38.0: the management deck on the company template.
        t('versi manajer: template perusahaan — judul, penutup Terima Kasih, tanpa Next week',
            [mgmt.every(p => p.theme === 'msg'), mgmt[0].title, mgmt[0].title2, mgmt.slice(1, -1).map(p => p.title), mgmt[mgmt.length - 1].kind, mgmt[mgmt.length - 1].title],
            [true, 'Weekly Update', 'Division Operation Technology', ['Component Health Precision and Sensor', 'Issue in Field', 'Job Log', 'Project', 'Project RTK radio repeater stage 2', 'Project Depth Sensor - Cartrack (Phase 2)'], 'closing', 'Terima Kasih']);
        t('Issue in Field: 3 kartu di atas, tabel ticket di bawah', mgmt[2].rows.map(r => r.cards.map(b => b.type)), [['stats', 'kv', 'bars'], ['table']]);
        t('halaman project versi manajer: subjudul hanya departemen, tanpa eyebrow', [mgmt[5].sub, mgmt[5].eyebrow], ['', '']);
        // 2.40.0: the job log — every job of the week in plain words, with its day, status and problem.
        const log = team[7].cols.flat().flatMap(b => b.items);
        t('job log: 5 job, hari + kategori + ticket, status, masalah', [team[7].title, log.length, log[0].text, log[0].tag.text, /^Mon 2 · Calibration · Ticket TK-261102-0800AB/.test(log[0].sub), log[0].note],
            ['Job log: what the OT team did this week', 5, 'Calibration lime spreader LS006', 'In progress', true, 'Problem: Reading still wrong after calibration']);
        t('job log versi manajer', [mgmt[3].title, mgmt[3].cols.flat().flatMap(b => b.items).length], ['Job Log', 5]);
        t('manajer: unit dengan komponen rusak + ticket lewat SLA di atas', [mgmt[1].cols[1][1].title, mgmt[1].cols[1][1].rows.map(r => r[0].text), mgmt[2].rows[1].cards[0].head.map(h => h.t)],
            ['Units with a faulty component', ['GGTR103G', 'HEX02', 'GGTR107G', 'GGTR118G'], ['Open ticket', 'Unit', 'Prio', 'PIC', 'Days']]);
        // Ticks undone the same week cancel out; twin project names get their PT; empty cards become one line.
        projects.push({ id: 'tw1', name: 'Twin', status: 'active', company: 'PT. Global Papua Abadi', target: { mode: 'milestones', milestones: [{ label: 'A', weight: 50, done: false }, { label: 'B', weight: 50, done: true }] },
            updates: [{ date: '2026-11-02', at: 1, auto: true, text: 'Marked done: A', progress: 'progress 50% → 100%' }, { date: '2026-11-02', at: 2, auto: true, text: 'Marked not done: A', progress: 'progress 100% → 50%' },
                { date: '2026-11-03', at: 3, auto: true, text: 'Marked done: B', progress: 'progress 0% → 50%' }] },
            { id: 'tw2', name: 'Twin', status: 'active', company: 'PT. Murni Nusantara Mandiri', target: { mode: 'count', count: 2, done: 0 }, updates: [{ date: '2026-11-03', at: 4, auto: true, text: 'Marked done: X', progress: 'progress 0% → 50%' }, { date: '2026-11-04', at: 5, auto: true, text: 'Marked not done: X', progress: 'progress 50% → 0%' }] });
        const D2 = buildWeeklyReport('2026-10-31', { today: '2026-11-06' });
        const tw = D2.projectPages.filter(x => /^Twin/.test(x.name));
        t('nama kembar + PT; centang lalu batal hilang; progress dari awal ke akhir minggu', [tw.map(x => x.name), tw.map(x => [x.sum.done, x.sum.undone, x.sum.start, x.sum.end, x.sum.empty])],
            [['Twin · GPA', 'Twin · MNM'], [[['B'], [], 50, 50, false], [[], [], 0, 0, true]]]);
        t('ringkasan Projects: baris minggu dari ringkasan, tipe tanpa "Other /"', [D2.projects.find(x => x.name === 'Twin · GPA').week, D2.projects.find(x => x.name === 'Twin · MNM').week, D2.projects.find(x => x.name === 'Twin · GPA').type],
            ['Done: B', '', 'Infrastructure']);
        const fit = DailyCore.fitPage({ kind: 'content', title: 'x', cols: [[{ type: 'list', items: [], empty: 'No job logged' }], [{ type: 'table', head: [], rows: [['a']] }, { type: 'bars', items: [], empty: 'No bars' }]], widths: [1, 1] });
        t('kartu kosong → satu baris strip; kolom kosong hilang', [fit.strip, fit.cols.length, fit.cols[0].map(b => b.type), fit.widths], ['No job logged · No bars', 1, ['table'], [1]]);
        projects = projects.filter(p => !/^tw/.test(p.id));
        t('judul halaman berupa kesimpulan', [/^34 of 37 daily reports came in; 3 tickets still open/.test(team[1].title), mgmt[1].title], [true, 'Component Health Precision and Sensor']);
        // 2.37.0: bento cards + a page per running project
        t('Summary: kartu utama (hero), angka, kesehatan, ticket, jam, perlu perhatian', team[1].cols.map(c => c.map(b => b.type + (b.card ? ':' + b.card : ''))), [['hero:hero', 'bars'], ['stats', 'table'], ['bars', 'list:warn']]);
        const pp = D.projectPages;
        t('halaman project: semua yang berjalan, yang merah dulu', pp.map(x => [x.name, x.rag, x.pct]), [['RTK radio repeater stage 2', 'red', 50], ['Depth Sensor - Cartrack (Phase 2)', 'green', 55]]);
        t('project milestone: langkah + bobot; project jumlah: target & selesai', [pp[0].steps.map(m => [m.label, m.weight, m.done]), [pp[1].mode, pp[1].done, pp[1].total, pp[1].steps.length]],
            [[['Survey', 20, true], ['PO', 30, true], ['Install', 50, false]], ['count', 22, 40, 0]]);
        t('update minggu ini masuk ke halaman project', [pp[0].updates.map(u => u.text), pp[1].updates.map(u => u.text)], [['Goods still at Merauke port'], ['6 more sensors installed at MNM']]);
        const pPage = team.find(p => p.projectId === 'pj2');
        t('halaman project: eyebrow, judul = nama project, 3 kolom kartu', [pPage.title, /^Project in progress · /.test(pPage.eyebrow), pPage.cols.map(c => c.map(b => b.type))], ['RTK radio repeater stage 2', true, [['progress'], ['steps'], ['list']]]);
        t('catatan sendiri masuk paling atas di Next week', team[team.length - 1].cols[0][0].items[0], 'Barang radio datang Selasa');
        // 2.32.0: pictures
        const refs = wrMediaRefs(D);
        t('foto Daily Report tersebar: paddock & hari berbeda dulu', [refs.daily.slice(0, 5).map(r => r.paddock).length === new Set(refs.daily.slice(0, 5).map(r => r.paddock)).size,
            new Set(refs.daily.slice(0, 6).map(r => r.date)).size >= 5], [true, true]);
        t('foto ticket (prioritas tinggi dulu), project & job, video di depan', [refs.tickets.map(r => r.key), refs.work[0].kind, refs.work.filter(r => r.kind === 'photo').map(r => r.key)],
            [['tk:k1_p_3', 'tk:k1_p_4', 'tk:k4_p_5'], 'video', ['pj:u1_p_7', 'pj:u1_p_8', 'pj:j1_p_6']]);
        // The view
        _wrWeek = '2026-10-31';
        navigateTo('weeklyReport');
        const wait = async () => { for (let i = 0; i < 40 && wrMediaPending(); i++) await new Promise(r => setTimeout(r, 150)); await new Promise(r => setTimeout(r, 400)); };
        await wait();
        const secs = () => [...document.querySelectorAll('#wrPages .wr-foot span:last-child')].map(x => x.textContent.split(' · ')[0]);
        t('menu & halaman tampil untuk owner (dengan 3 galeri)', [currentView, document.querySelectorAll('#wrPages .wr-page').length, document.getElementById('wrWeek').value, document.querySelectorAll('#wrPages .wr-gal').length], ['weeklyReport', 16, '2026-10-31', 4]);
        t('gaya bento: kartu putih, satu kartu teal, satu kartu perhatian', [document.querySelectorAll('#wrPages .wr-page:nth-child(2) .wr-card').length, document.querySelectorAll('#wrPages .wr-page:nth-child(2) .wr-card--hero').length, document.querySelectorAll('#wrPages .wr-page:nth-child(2) .wr-card--warn').length], [6, 1, 1]);
        t('halaman project punya galeri kecil di dalam kartu', document.querySelectorAll('#wrPages .wr-gal--mini').length, 1);
        t('galeri: 6 foto Daily Report termuat', document.querySelectorAll('#wrPages .wr-gal')[0].querySelectorAll('img').length, 6);
        t('video: gambar diam + tanda play', [!!document.querySelector('#wrPages .wr-gal__play'), /^data:image\/jpeg/.test(document.querySelector('.wr-gal__play').parentElement.querySelector('img').getAttribute('src'))], [true, true]);
        const firstKey = refs.daily[0].key;
        hideWeeklyMedia(firstKey); await wait();
        t('foto dikeluarkan (×): diganti foto berikutnya, tombol kembalikan muncul', [document.querySelectorAll('#wrPages .wr-gal')[0].querySelectorAll('img').length, !document.getElementById('wrHiddenBtn').hidden],
            [6, true]);
        resetWeeklyMedia();
        setWeeklyMediaShow('daily', false);
        t('matikan foto Daily Report: galerinya hilang', document.querySelectorAll('#wrPages .wr-gal').length, 3);
        setWeeklyMediaShow('daily', true);
        setWeeklyReportAudience('mgmt'); await wait();
        t('versi manajer di layar: 8 halaman bertemplate (cover, isi, penutup)', [document.querySelectorAll('#wrPages .wr-page').length, document.querySelector('.wr-aud.active').dataset.aud,
            document.querySelectorAll('#wrPages .wr-msg--cover').length, document.querySelectorAll('#wrPages .wr-msg--content').length, document.querySelectorAll('#wrPages .wr-msg--closing').length], [8, 'mgmt', 1, 6, 1]);
        // Access: a viewer without the area does not see it.
        currentUser = { uid: 'v', email: 'v@x.id' }; currentUserDoc = { uid: 'v', role: 'staff', status: 'active', access: { tickets: 'view' } }; applyRoleGating();
        t('tanpa akses Weekly Report: menu disembunyikan', document.querySelector('.nav__link[data-view="weeklyReport"]').style.display, 'none');
        currentUserDoc = { uid: 'v', role: 'staff', status: 'active', access: { weeklyReport: 'view' } }; applyRoleGating();
        t('dengan akses view: menu tampil', document.querySelector('.nav__link[data-view="weeklyReport"]').style.display, '');
        currentUser = { uid: 'o', email: 'o@x.id' }; currentUserDoc = { uid: 'o', role: 'owner', status: 'active', displayName: 'Rasyid' }; applyRoleGating();
        navigateTo('weeklyReport'); setWeeklyReportAudience('team'); await wait();
        document.getElementById('wrEmbedVideo').checked = true; setWeeklyEmbedVideo(true);
        return T;
    });
    // PowerPoint download
    const [dl] = await Promise.all([page.waitForEvent('download', { timeout: 30000 }), page.evaluate(() => downloadWeeklyPptx())]);
    const f = await dl.path();
    const size = require('fs').statSync(f).size;
    const zip = require('fs').readFileSync(f).toString('latin1');
    T.push({ n: 'PowerPoint: video ikut di dalam file', g: /ppt\/media\/media-[\w-]+\.mp4/.test(zip), w: true, pass: /ppt\/media\/media-[\w-]+\.mp4/.test(zip) });
    T.push({ n: 'PowerPoint terunduh (nama file & isi)', g: [dl.suggestedFilename(), size > 20000], w: ['Weekly_Operation_Report_W45_2026-10-31.pptx', true], pass: dl.suggestedFilename() === 'Weekly_Operation_Report_W45_2026-10-31.pptx' && size > 20000 });
    // PDF: one sheet per page
    await page.evaluate(() => { const box = document.createElement('div'); box.id = 'wrPrint'; document.body.appendChild(box); box.innerHTML = document.getElementById('wrPages').innerHTML; document.body.classList.add('wr-printing'); });
    await page.emulateMedia({ media: 'print' });
    const pdf = await page.pdf({ preferCSSPageSize: true, printBackground: true });
    const pages = (pdf.toString('latin1').match(/\/Type\s*\/Page[^s]/g) || []).length;
    T.push({ n: 'PDF: 16 halaman (satu per lembar)', g: pages, w: 16, pass: pages === 16 });
    let pass = 0;
    for (const x of T) { if (x.pass) pass++; console.log(`${x.pass ? 'PASS' : 'FAIL'}  ${x.n}`); if (!x.pass) console.log(`        dapat=${JSON.stringify(x.g)} harap=${JSON.stringify(x.w)}`); }
    console.log(`\n${pass}/${T.length} lulus`);
    console.log('PAGE ERRORS:', errors.length ? errors : 'none');
    await b.close();
    process.exit(pass === T.length && !errors.length ? 0 : 1);
})();
