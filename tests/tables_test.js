// 2.42.0 — long lists without a scroll inside the page (tables.js), and Jobs &
// Activities in four columns that never run off the side — at 100% and at the
// owner's 75–80% browser zoom (a wider page), on a laptop and on a phone.
const { launch, BASE_URL } = require('./_env');
const path = require('path');
(async () => {
    const b = await launch();
    const T = []; const errors = [];
    for (const [name, w, h] of [['zoom 80% (2008 px)', 2008, 1100], ['100% (1606 px)', 1606, 880], ['laptop 1280', 1280, 720], ['HP 390', 390, 844]]) {
        const page = await b.newPage({ viewport: { width: w, height: h } });
        page.on('pageerror', e => { if (!/Chart is not defined/.test(e.message)) errors.push(e.message); });
        await page.addInitScript({ path: path.join(__dirname, '_weekly_seed.js') });
        await page.goto(BASE_URL + '/index.html', { waitUntil: 'load' });
        await page.waitForTimeout(900);
        const r = await page.evaluate(async () => {
            const tick = () => new Promise(res => setTimeout(res, 120));
            window.__seed(); cloudInitialized = true;
            for (let i = 0; i < 60; i++) otJobs.push({ id: 'jx' + i, date: '2026-11-' + String(1 + i % 12).padStart(2, '0'), category: 'Other',
                description: 'Pemasangan 2 Bracket Antenna dan pemasangan 2 Dipole Array Antenna 16W dengan ketinggian 45M dan 38M serta penarikan kabel coaxial 7/8 sepanjang 70 meter',
                status: 'done', units: Array.from({ length: 6 }, (_, k) => ({ id: 't' + (k + 1), name: 'MNMT-BD0' + (60 + k) })), requestedBy: 'Land Preparation', doneBy: 'Muh Yunus dan Fikri Suleman', hours: 8 });
            for (let i = 0; i < 80; i++) globalDamages.push({ id: 'dx' + i, date: '2026-10-' + String(1 + i % 28).padStart(2, '0'), unitId: 't1', unitName: 'GGTR101G', damageType: 'Device Precision', component: 'Display', description: 'x', resolved: false });
            navigateTo('projects'); switchProjectsTab('jobs'); document.getElementById('jobPeriod').value = ''; renderJobsList(); await tick();
            const jw = document.querySelector('#jobTable').closest('.table-wrapper');
            const jobs = { side: jw.scrollWidth > jw.clientWidth + 1, cols: document.querySelectorAll('#jobTable thead th').length, more: (document.querySelector('#jobBody').closest('.table-wrapper').nextElementSibling || {}).className,
                shown: [...document.querySelectorAll('#jobBody tr')].filter(tr => !tr.classList.contains('tw-hide')).length, plus: (document.querySelector('#jobBody .jb-more') || {}).textContent };
            navigateTo('damage'); await tick();
            const dw = document.querySelector('#damageBody').closest('.table-wrapper');
            const capped = getComputedStyle(dw).maxHeight;
            document.getElementById('selectAllDamage').checked = true; toggleSelectAllDamages();
            const picked = selectedDamageIds.size;
            const bar = dw.nextElementSibling; bar.querySelectorAll('button')[1].click(); await tick();
            const all = [...document.querySelectorAll('#damageBody tr')].filter(tr => !tr.classList.contains('tw-hide')).length;
            return { jobs, capped, picked, all, barGone: !(dw.nextElementSibling && dw.nextElementSibling.classList.contains('tw-more')), fit: jw.classList.contains('tw-fit') };
        });
        const t = (n, g, wnt) => T.push({ n: `${name}: ${n}`, g, w: wnt, pass: JSON.stringify(g) === JSON.stringify(wnt) });
        t('Jobs tidak perlu digeser ke samping', r.jobs.side, false);
        t('Jobs 4 kolom; unit lebih dari 4 jadi "+2"', [r.jobs.cols, r.jobs.plus], [4, '+2']);
        t('Jobs: 50 baris + bar "Show more"', [r.jobs.shown, r.jobs.more], [50, 'tw-more']);
        t('Damage: kotak tabel tidak dibatasi tinggi (satu scroll per halaman)', r.capped, 'none');
        t('Damage: "pilih semua" hanya baris yang tampil', r.picked, 50);
        t('Damage: "Show all" menampilkan semua, bar hilang', [r.all, r.barGone], [85, true]);
        if (w > 1024) t('judul kolom menempel di atas (tabel muat lebar)', r.fit, true);
        await page.close();
    }
    let pass = 0;
    for (const x of T) { if (x.pass) pass++; console.log(`${x.pass ? 'PASS' : 'FAIL'}  ${x.n}`); if (!x.pass) console.log(`        dapat=${JSON.stringify(x.g)} harap=${JSON.stringify(x.w)}`); }
    console.log(`\n${pass}/${T.length} lulus`);
    console.log('PAGE ERRORS:', errors.length ? errors : 'none');
    await b.close();
    process.exit(pass === T.length && !errors.length ? 0 : 1);
})();
