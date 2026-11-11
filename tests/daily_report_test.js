// 2.39.0 — Daily Report for the manager: one day on the company template,
// built by functions/dailyCore.js (the same code the 07:00 WIT email runs).
// Sample data: tests/_weekly_seed.js — the day tested is Tue 3 Nov 2026.
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
        const wait = async () => { for (let i = 0; i < 40 && wrMediaPending(); i++) await new Promise(r => setTimeout(r, 100)); await new Promise(r => setTimeout(r, 400)); };
        window.__seed();
        t('modul bersama dimuat di browser', typeof window.DailyCore && typeof DailyCore.buildDaily, 'function');
        const D = DailyCore.buildDaily(wrDailyInput('2026-11-03'));
        t('Daily Report: 5 dari 6, Febri bolos, 45 jam', [D.team.filed, D.team.expected, D.team.missed, D.team.hours], [5, 6, ['Febri Manuel'], 45]);
        t('kendala hari itu: issue DR, kerusakan baru, ticket masuk', [D.team.issues.length, D.damage.map(d => [d.unit, d.part, d.resolved]), D.tickets.opened.map(x => x.no)],
            [0, [['GGTR118G', 'GPS', false]], ['TK-261103-1000CD']]);
        t('jobs & update project hari itu', [D.jobs.map(j => [j.category, j.status]), D.updates.map(u => [u.project, u.s.texts[0]])],
            [[['Implement install', 'In progress']], [['RTK radio repeater stage 2', 'Goods still at Merauke port']]]);
        t('foto: paddock berbeda didahulukan', D.photos.length, 5);
        currentView = 'weeklyReport';
        navigateTo('weeklyReport'); setWeeklyReportAudience('daily'); setDailyReportDay('2026-11-03'); await wait();
        const titles = [...document.querySelectorAll('#wrPages .wr-page')].map(p => (p.querySelector('h1,h2') || {}).textContent);
        t('halaman: sampul, ringkasan, kendala, foto, jobs, penutup', titles,
            ['Daily ReportDivision Operation Technology', 'Daily Report Summary', 'Issue & Damage', 'Field Photos', 'OT Jobs & Project Progress', 'Terima Kasih']);
        t('template perusahaan di setiap halaman', [...document.querySelectorAll('#wrPages .wr-page')].every(p => p.classList.contains('wr-msg')), true);
        t('kontrol mingguan disembunyikan, pemilih tanggal tampil', [document.getElementById('wrWeekBox').hidden, document.getElementById('wrDayBox').hidden, document.getElementById('wrNotesBox').hidden, document.getElementById('wrDay').value],
            [true, false, true, '2026-11-03']);
        t('foto lapangan dimuat', document.querySelectorAll('#wrPages .wr-gal__item img').length, 5);
        t('ringkasan: Febri "Missed"', [...document.querySelectorAll('#wrPages .wr-table tr')].some(r => /Febri Manuel/.test(r.textContent) && /Missed/.test(r.textContent)), true);
        t('nama file', [_wrFileName('pptx'), _wrFileName('pdf')], ['Daily_Report_2026-11-03.pptx', 'Daily_Report_2026-11-03.pdf']);
        setWeeklyReportAudience('mgmt'); await wait();
        t('kembali ke mingguan', [document.getElementById('wrWeekBox').hidden, document.getElementById('wrDayBox').hidden, document.querySelectorAll('#wrPages .wr-page')[1].querySelector('h2').textContent], [false, true, 'Component Health Precision and Sensor']);
        setWeeklyReportAudience('daily'); await wait();
        // Admin → Notifications: the manager's daily email.
        const saved = [];
        window.cloud.getEmailSettings = () => Promise.resolve({ mgrEnabled: true, mgrHour: 7, mgrRecipients: ['boss@x.id'] });
        window.cloud.saveEmailSettings = rec => { saved.push(rec); return Promise.resolve(); };
        window.cloud.lastMails = () => Promise.resolve({ mgrDaily: { at: Date.now(), trigger: 'auto', items: 5, recipients: ['boss@x.id'] } });
        await openEmailSettingsModal(); await new Promise(r => setTimeout(r, 200));
        t('pengaturan email manajer terbaca', [document.getElementById('mgrEnabled').checked, document.getElementById('mgrHour').value, document.getElementById('mgrRecipients').value, /daily report/.test(document.getElementById('mgrLast').textContent)],
            [true, '7', 'boss@x.id', true]);
        document.getElementById('mgrRecipients').value = 'boss@x.id\nhead@x.id';
        await saveEmailSettings();
        t('tersimpan: penerima, aktif, jam', saved.length ? [saved[0].mgrRecipients, saved[0].mgrEnabled, saved[0].mgrHour] : null, [['boss@x.id', 'head@x.id'], true, 7]);
        const called = [];
        window.cloud.callFunction = (name, data) => { called.push([name, data.kind]); return Promise.resolve({ sent: true, recipients: 2 }); };
        await sendNotificationNow('mgrDaily', document.getElementById('mgrSendBtn'));
        t('Send now: lewat server', called, [['sendNotificationNow', 'mgrDaily']]);
        closeEmailSettingsModal();
        return T;
    });
    const [dl] = await Promise.all([page.waitForEvent('download', { timeout: 30000 }), page.evaluate(() => downloadWeeklyPptx())]);
    const f = await dl.path();
    const zip = require('fs').readFileSync(f).toString('latin1');
    const slides = new Set(zip.match(/ppt\/slides\/slide\d+\.xml/g) || []).size;
    T.push({ n: 'PowerPoint: nama file, 6 slide', g: [dl.suggestedFilename(), slides], w: ['Daily_Report_2026-11-03.pptx', 6], pass: dl.suggestedFilename() === 'Daily_Report_2026-11-03.pptx' && slides === 6 });
    await page.evaluate(() => { const box = document.createElement('div'); box.id = 'wrPrint'; document.body.appendChild(box); box.innerHTML = document.getElementById('wrPages').innerHTML; document.body.classList.add('wr-printing'); });
    await page.emulateMedia({ media: 'print' });
    const pdf = await page.pdf({ preferCSSPageSize: true, printBackground: true });
    const pages = (pdf.toString('latin1').match(/\/Type\s*\/Page[^s]/g) || []).length;
    T.push({ n: 'PDF: 6 halaman', g: pages, w: 6, pass: pages === 6 });
    let pass = 0;
    for (const x of T) { if (x.pass) pass++; console.log(`${x.pass ? 'PASS' : 'FAIL'}  ${x.n}`); if (!x.pass) console.log(`        dapat=${JSON.stringify(x.g)} harap=${JSON.stringify(x.w)}`); }
    console.log(`\n${pass}/${T.length} lulus`);
    console.log('PAGE ERRORS:', errors.length ? errors : 'none');
    await b.close();
    process.exit(pass === T.length && !errors.length ? 0 : 1);
})();
