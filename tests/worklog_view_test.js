// 2.29.4 — Daily Report bisa dilihat tanpa Edit: klik baris / tombol mata membuka
// jendela baca-saja (jam, tiap paddock dengan unit, deskripsi, foto per paddock,
// issue, status), dengan tombol Approve / Send back untuk pemeriksa.
const { launch, BASE_URL } = require('./_env');
(async () => {
    const b = await launch();
    const page = await b.newPage({ viewport: { width: 1366, height: 900 } });
    const errors = [];
    page.on('pageerror', e => { if (!/Chart is not defined/.test(e.message)) errors.push(e.message); });
    await page.goto(BASE_URL + '/index.html', { waitUntil: 'load' });
    await page.waitForTimeout(1000);
    await page.evaluate(() => {
        const ok = () => Promise.resolve();
        window.__saved = [];
        window.showToast = () => {}; window.confirm = () => true; window.prompt = () => 'Foto kurang jelas';
        const px = c => { const cv = document.createElement('canvas'); cv.width = cv.height = 4; const x = cv.getContext('2d'); x.fillStyle = c; x.fillRect(0, 0, 4, 4); return cv.toDataURL('image/png'); };
        window.cloud = { isReady: true, addHistoryEvents: ok, saveWorkLog: r => { window.__saved.push(JSON.parse(JSON.stringify(r))); return ok(); },
            getWorkLogPhotos: () => Promise.resolve([px('#f00'), px('#0f0'), px('#00f')]) };
        currentUser = { uid: 'o', email: 'o@x.id' }; currentUserDoc = { uid: 'o', role: 'owner', status: 'active' };
        hideAuthGates(); applyRoleGating();
        teamMembers = [{ id: 'm1', name: 'Philipus Bone', company: 'PT. Global Papua Abadi', active: true }];
        workLogs = [{ id: 'w1', date: '2026-11-04', memberId: 'm1', memberName: 'Philipus Bone', company: 'PT. Global Papua Abadi', start: '08:20', end: '16:30',
            approval: 'pending', submittedAt: Date.now(), createdByUid: 'k', createdByEmail: 'philipus@x.id', issue: 'Hujan sore', photoCount: 3, place: 'field',
            jobs: [{ paddock: 'JAGF-1-E-003', jobType: 'Tillage', desc: 'Bajak 1', units: [{ id: 'u1', name: 'GGTR130G_OHZ' }], photoCount: 2 },
                   { paddock: 'JAGF-4-P-006', jobType: 'Tillage', desc: 'Bajak 2', units: [{ id: 'u2', name: 'GGTR113G_SGI' }], photoCount: 1 }] }];
        leaveRequests = []; teamShifts = [];
        navigateTo('team');
        switchTeamTab('worklog');
        renderWorkLogTable();
    });
    const T = [];
    const t = (n, g, w) => T.push({ n, g, w, pass: JSON.stringify(g) === JSON.stringify(w) });
    await page.click('#workLogBody tr.wl-row td:nth-child(3)');
    await page.waitForTimeout(300);
    const R = await page.evaluate(() => ({
        open: document.getElementById('wlDetailModal').classList.contains('open'),
        title: document.getElementById('wlDetailTitle').textContent,
        jobs: [...document.querySelectorAll('#wlDetailBody .wl-detail__job')].map(j => j.querySelector('.wl-detail__jobhead b').textContent + ' | ' + j.querySelector('.wl-detail__desc').textContent + ' | ' + j.querySelectorAll('.wl-detail__thumb img').length),
        issue: document.querySelector('#wlDetailBody .wl-detail__issue')?.textContent,
        hours: /08:20–16:30/.test(document.getElementById('wlDetailBody').textContent),
        btns: [...document.querySelectorAll('#wlDetailActions button')].map(b => b.textContent.trim())
    }));
    t('klik baris membuka jendela View', [R.open, R.title], [true, 'Philipus Bone — 2026-11-04']);
    t('tiap paddock: nama, deskripsi, fotonya sendiri', R.jobs, ['JAGF-1-E-003 | Bajak 1 | 2', 'JAGF-4-P-006 | Bajak 2 | 1']);
    t('jam kerja & issue tampil', [R.hours, R.issue], [true, 'Hujan sore']);
    t('tombol pemeriksa', R.btns, ['Approve', 'Send back', 'Edit', 'Close']);
    await page.click('#wlDetailBody .wl-detail__thumb');
    t('foto dibuka di lightbox dengan keterangan paddock', await page.evaluate(() => [document.getElementById('photoLightbox').classList.contains('open'), _lightboxCaptions[0]]), [true, 'JAGF-1-E-003 · Tillage — photo 1 of 2']);
    await page.evaluate(() => closePhotoLightbox());
    await page.click('#wlDetailActions button:has-text("Approve")');
    await page.waitForTimeout(200);
    // The server's answer arrives as a snapshot; the open view follows it.
    await page.evaluate(() => applyCloudWorkLogsSnapshot(window.__saved.slice(-1)));
    await page.waitForTimeout(100);
    t('Approve dari jendela View (tersimpan, tampilan ikut)', await page.evaluate(() => [window.__saved.slice(-1)[0].approval, document.querySelector('#wlDetailBody .appr').textContent.trim(),
        [...document.querySelectorAll('#wlDetailActions button')].map(b => b.textContent.trim())]), ['approved', 'Approved', ['Reopen', 'Edit', 'Close']]);
    await page.evaluate(() => closeWorkLogDetail());
    await page.click('#workLogBody tr.wl-row button.wl-view-btn');
    t('tombol mata juga membuka View', await page.evaluate(() => document.getElementById('wlDetailModal').classList.contains('open')), true);
    await page.keyboard.press('Escape');
    t('Esc menutup', await page.evaluate(() => document.getElementById('wlDetailModal').classList.contains('open')), false);
    t('tidak ada error halaman', errors, []);
    let pass = 0;
    for (const x of T) { if (x.pass) pass++; console.log(`${x.pass ? 'PASS' : 'FAIL'}  ${x.n}`); if (!x.pass) console.log(`        dapat=${JSON.stringify(x.g)} harap=${JSON.stringify(x.w)}`); }
    console.log(`\n${pass}/${T.length} lulus`);
    console.log('PAGE ERRORS:', errors.length ? errors : 'none');
    if (process.env.SHOT) { await page.evaluate(() => showWorkLogDetail('w1')); await page.waitForTimeout(400); await (await page.$('#wlDetailModal .modal-card')).screenshot({ path: process.env.SHOT }); }
    await b.close();
    process.exit(pass === T.length && !errors.length ? 0 : 1);
})();
