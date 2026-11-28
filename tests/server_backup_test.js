// 2.35.0 — Server Backups window (owner) and photo thumbnails in lists.
// The server side is tested in tests/backup_e2e.js (emulators).
const { launch, BASE_URL } = require('./_env');
(async () => {
    const b = await launch();
    const page = await b.newPage({ viewport: { width: 1280, height: 900 } });
    const errors = [];
    page.on('pageerror', e => { if (!/Chart is not defined/.test(e.message)) errors.push(e.message); });
    await page.goto(BASE_URL + '/index.html', { waitUntil: 'load' });
    await page.waitForTimeout(800);
    const T = await page.evaluate(async () => {
        const T = []; const t = (n, g, w) => T.push({ n, g, w, pass: JSON.stringify(g) === JSON.stringify(w) });
        const tick = ms => new Promise(r => setTimeout(r, ms || 30));
        const toasts = []; window.showToast = m => toasts.push(String(m)); window.confirm = () => true;
        const calls = [];
        const today = toISODate(), yesterday = addDaysISO(today, -1);
        let list = { last: { date: yesterday, at: Date.now() - 86400000, docs: 5120, collections: 21, bytes: 2400000, result: 'ok' }, dates: [yesterday, '2026-10-01'] };
        window.cloud = { isReady: true, addHistoryEvents: () => Promise.resolve(),
            callFunction: (name, data, ms) => { calls.push([name, data.action, data.collection || '', ms]);
                if (data.action === 'list') return Promise.resolve(list);
                if (data.action === 'detail') return Promise.resolve({ manifest: { date: data.date, collections: [{ name: 'tickets', docs: 12, bytes: 4000 }, { name: 'users', docs: 9, bytes: 2000 }, { name: 'paddocks', docs: 300, bytes: 9000 }] } });
                if (data.action === 'now') { list = { last: { date: today, at: Date.now(), docs: 5200, collections: 21, bytes: 2500000, result: 'ok' }, dates: [today, yesterday, '2026-10-01'] }; return Promise.resolve({ date: today, docs: 5200, collections: 21 }); }
                if (data.action === 'restore') return Promise.resolve({ added: 3, kept: 9, total: 12 });
                return Promise.reject({ code: 'functions/invalid-argument' }); },
            photoThumbUrl: url => Promise.resolve(/abc/.test(url) ? url.replace('abc', 'abc_t') : '') };
        // Not the owner: no button, no window.
        currentUser = { uid: 'v', email: 'v@x.id' }; currentUserDoc = { uid: 'v', role: 'staff', status: 'active', access: { editUnits: 'edit' } }; hideAuthGates(); applyRoleGating();
        showServerBackups();
        t('bukan owner: ditolak', [document.getElementById('serverBackupModal').classList.contains('open'), toasts.pop()], [false, 'Only the owner can open server backups']);
        currentUser = { uid: 'o', email: 'o@x.id' }; currentUserDoc = { uid: 'o', role: 'owner', status: 'active' }; applyRoleGating();
        await showServerBackups(); await tick();
        t('status: backup kemarin, sehat', [/Last backup/.test(document.getElementById('sbStatus').textContent), !!document.querySelector('#sbStatus .sb-ok'), /5,120 records in 21 collections · 2.3 MB/.test(document.getElementById('sbStatus').textContent)], [true, true, true]);
        t('daftar tanggal + salinan bulanan', [...document.querySelectorAll('.sb-day')].map(li => [li.dataset.date, li.querySelector('.adm-sub').textContent]), [[yesterday, 'yesterday'], ['2026-10-01', 'monthly copy']]);
        await toggleServerBackup(yesterday); await tick();
        const rows = [...document.querySelectorAll('.sb-day.is-open .sb-table tbody tr')];
        t('isi: koleksi, jumlah; akun tidak bisa direstore', rows.map(r => [r.cells[0].querySelector('b').textContent, r.cells[1].textContent, /Restore missing/.test(r.cells[3].textContent)]),
            [['Tickets', '12', true], ['Accounts & access', '9', false], ['paddocks', '300', true]]);
        await restoreServerBackup(yesterday, 'tickets'); await tick();
        t('restore: hanya yang hilang, lewat server', [calls.filter(c => c[1] === 'restore')[0], toasts.pop()], [['serverBackups', 'restore', 'tickets', 540000], '3 Tickets record(s) restored — 9 were already there']);
        await serverBackupNow(); await tick();
        t('Back up now: daftar diperbarui', [document.querySelector('.sb-day').dataset.date, toasts.pop()], [today, 'Backup made: 5,200 records']);
        // A backup two days old is flagged.
        renderServerBackups({ last: { date: addDaysISO(today, -3), docs: 1, collections: 1, bytes: 10 }, dates: [] });
        t('backup lebih dari sehari: diberi peringatan', /older than a day/.test(document.getElementById('sbStatus').textContent), true);
        renderServerBackups({ last: null, dates: [] });
        t('belum pernah backup', /No backup has been made yet/.test(document.getElementById('sbStatus').textContent), true);
        closeServerBackups();

        // ---- thumbnails ----
        const full = 'https://firebasestorage.googleapis.com/v0/b/ot-monitoring-tractor.firebasestorage.app/o/photos%2FworkLogPhotos%2Fw1%2Fabc?alt=media&token=1-2';
        const none = full.replace('abc', 'old');
        const box = document.createElement('div'); document.body.appendChild(box);
        box.innerHTML = `<img id="th1" ${thumbAttrs(full)}><img id="th2" ${thumbAttrs(none)}><img id="th3" ${thumbAttrs('data:image/png;base64,AAAA')}>`;
        t('foto di Storage: mulai dari placeholder, URL penuh disimpan', [/^data:image\/gif/.test(box.querySelector('#th1').getAttribute('src')), box.querySelector('#th1').dataset.full === full], [true, true]);
        thumbLoad(box.querySelector('#th1')); thumbLoad(box.querySelector('#th2')); await tick();
        t('ada thumbnail: dipakai; belum ada: foto penuh', [/abc_t\?alt=media/.test(box.querySelector('#th1').getAttribute('src')), box.querySelector('#th2').getAttribute('src') === none], [true, true]);
        t('foto inline (data:) dipakai langsung', [box.querySelector('#th3').getAttribute('src'), box.querySelector('#th3').hasAttribute('data-full')], ['data:image/png;base64,AAAA', false]);
        box.remove();
        return T;
    });
    let pass = 0;
    for (const x of T) { if (x.pass) pass++; console.log(`${x.pass ? 'PASS' : 'FAIL'}  ${x.n}`); if (!x.pass) console.log(`        dapat=${JSON.stringify(x.g)} harap=${JSON.stringify(x.w)}`); }
    console.log(`\n${pass}/${T.length} lulus`);
    console.log('PAGE ERRORS:', errors.length ? errors : 'none');
    await b.close();
    process.exit(pass === T.length && !errors.length ? 0 : 1);
})();
