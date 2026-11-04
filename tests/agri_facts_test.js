// 2.22.0 — Kartu Sugarcane knowledge: berganti otomatis, berhenti saat
// ditahan, setiap fakta bersumber; editor Learning menambah fakta sendiri.
const { launch, BASE_URL } = require('./_env');
(async () => {
    const b = await launch();
    const page = await b.newPage({ viewport: { width: 1366, height: 900 } });
    const errors = [];
    page.on('pageerror', e => { if (!/Chart is not defined/.test(e.message)) errors.push(e.message); });
    await page.goto(BASE_URL + '/index.html', { waitUntil: 'load' });
    await page.waitForTimeout(800);
    const R = await page.evaluate(async () => {
        const out = {}; const toasts = []; window.showToast = m => toasts.push(String(m)); window.confirm = () => true;
        const saved = [], deleted = [], cfg = [];
        window.cloud = { isReady: true, addHistoryEvents: () => Promise.resolve(),
            listAgriFacts: () => Promise.resolve({ facts: [], builtIns: true }),
            saveAgriFact: r => { saved.push(r); return Promise.resolve(); },
            deleteAgriFact: id => { deleted.push(id); return Promise.resolve(); },
            saveAgriSettings: r => { cfg.push(r); return Promise.resolve(); } };
        currentUser = { uid: 'o', email: 'o@x.id' }; currentUserDoc = { uid: 'o', role: 'owner', status: 'active' };
        hideAuthGates(); applyRoleGating();
        globalData = [{ id: 'u1', name: 'T1', site: 'S', status: 'Good' }];
        navigateTo('dashboard'); filteredData = globalData.slice(); onDataLoaded();
        await new Promise(r => setTimeout(r, 100));
        const txt = () => document.getElementById('dashKnowText').textContent;
        out.healthHidden = document.querySelector('.dash-health').hidden;
        out.first = [!!txt(), /^Source: /.test(document.getElementById('dashKnowSrc').textContent), /^https:\/\//.test(document.getElementById('dashKnowSrc').href)];
        out.everyBuiltInSourced = [agriAuto.length >= 10, agriAuto.every(f => f.term && f.text && f.source && /^https:\/\/[^\s]+$/.test(f.url) && !/wikipedia\.org/i.test(f.url))];
        // 2.23.0: materi hari ini tampil paling depan dengan tanda New today; Hide
        agriAuto = agriAuto.concat({ id: 'today', term: 'Hari ini', text: 'materi baru', source: 'X', url: 'https://example.org/x', addedAt: toISODate() });
        _agriOrder = []; renderAgriFact();
        out.newToday = [/^New today/.test(txt()), /Hari ini/.test(txt())];
        window.cloud.saveAgriSettings = r => { cfg.push(r); return Promise.resolve(); };
        await toggleAgriHidden('today');
        out.hidden = [cfg.pop(), agriPool().some(f => f.id === 'today')];
        await toggleAgriHidden('today');
        out.shownAgain = agriPool().some(f => f.id === 'today'); cfg.length = 0;
        const a = txt(); agriStep(1); const b2 = txt(); agriStep(-1);
        out.step = [a !== b2, txt() === a];
        // berganti otomatis
        _agriLeft = 900; await new Promise(r => setTimeout(r, 1300));
        out.auto = txt() !== a;
        // ditahan: tidak berganti
        const held = txt(); agriHold(true); _agriLeft = 900; await new Promise(r => setTimeout(r, 1300));
        out.held = [txt() === held, document.getElementById('dashKnow').classList.contains('is-held')];
        agriHold(false); await new Promise(r => setTimeout(r, 1300));
        out.released = txt() !== held;
        // kelola
        openAgriFacts();
        out.modalOpen = document.getElementById('agriFactsModal').classList.contains('open');
        document.getElementById('agriTerm').value = 'Pol'; document.getElementById('agriText').value = 'adalah kadar sukrosa nira.';
        await saveAgriFact(); out.noSource = toasts.pop();
        document.getElementById('agriSource').value = 'P3GI'; document.getElementById('agriUrl').value = 'http://bukan-https.id';
        await saveAgriFact(); out.badUrl = toasts.pop();
        document.getElementById('agriUrl').value = 'https://example.org/pol';
        await saveAgriFact();
        out.saved = saved.length && [saved[0].term, saved[0].source, saved[0].url, agriFacts.length];
        out.inPool = agriPool().some(f => f.term === 'Pol');
        document.getElementById('agriBuiltIns').checked = false; await saveAgriBuiltIns();
        out.builtInsOff = [cfg.pop(), agriPool().length];
        await deleteAgriFact(saved[0].id);
        out.deleted = [deleted.length, deleted[0] === saved[0].id, agriFacts.length];
        document.getElementById('agriBuiltIns').checked = false; await saveAgriBuiltIns();
        out.needOne = toasts.pop();
        closeAgriFacts();
        // xss
        agriFacts = [{ id: 'x', term: '<img src=x onerror=alert(1)>', text: 'x', source: 's', url: 'javascript:alert(1)' }]; agriBuiltIns = false; _agriOrder = []; renderAgriFact();
        out.xss = [!document.querySelector('#dashKnowText img'), document.getElementById('dashKnowSrc').hasAttribute('href')];
        currentUserDoc = { uid: 'v', role: 'staff', status: 'active', access: { learning: 'view' } };
        applyAccessVisibility(); openAgriFacts();
        out.viewerBlocked = [document.getElementById('agriFactsModal').classList.contains('open'), toasts.pop()];
        return out;
    });
    const T = [];
    const t = (n, g, w) => T.push({ n, g, w, pass: JSON.stringify(g) === JSON.stringify(w) });
    t('Fleet health diganti kartu pengetahuan', R.healthHidden, true);
    t('fakta tampil dengan tautan sumber https', R.first, [true, true, true]);
    t('materi harian dari data/agri-facts.json, semua bersumber, tanpa Wikipedia', R.everyBuiltInSourced, [true, true]);
    t('materi hari ini paling depan + tanda New today', R.newToday, [true, true]);
    t('Hide: materi harian tidak tampil lagi', R.hidden, [{ builtIns: true, hidden: ['today'] }, false]);
    t('Show: tampil lagi', R.shownAgain, true);
    t('‹ › berpindah fakta', R.step, [true, true]);
    t('berganti otomatis setelah waktunya', R.auto, true);
    t('ditahan: tidak berganti, kartu ditandai', R.held, [true, true]);
    t('dilepas: lanjut berganti', R.released, true);
    t('owner membuka Manage', R.modalOpen, true);
    t('sumber wajib', R.noSource, 'Name the source — every fact needs one');
    t('tautan sumber harus https', R.badUrl, 'Add the source link (starting with https://)');
    t('fakta sendiri tersimpan', R.saved, ['Pol', 'P3GI', 'https://example.org/pol', 1]);
    t('fakta sendiri ikut tampil', R.inPool, true);
    t('materi harian bisa dimatikan', R.builtInsOff, [{ builtIns: false, hidden: [] }, 1]);
    t('hapus fakta', R.deleted, [1, true, 0]);
    t('tanpa fakta sendiri, bawaan tidak boleh dimatikan', R.needOne, 'Add a fact of your own first — the card needs at least one');
    t('isi aneh tidak menjadi markup, tautan non-https tidak dipasang', R.xss, [true, false]);
    t('pembaca Learning tidak bisa mengelola', R.viewerBlocked, [false, 'You do not have edit access to the Learning Center']);
    let pass = 0;
    T.forEach(x => { if (x.pass) pass++; console.log(`${x.pass ? 'PASS' : 'FAIL'}  ${x.n}${x.pass ? '' : `\n        dapat=${JSON.stringify(x.g)} harap=${JSON.stringify(x.w)}`}`); });
    console.log(`\n${pass}/${T.length} lulus`);
    console.log('PAGE ERRORS:', errors.length ? errors : 'none');
    await b.close();
    process.exit(pass === T.length && !errors.length ? 0 : 1);
})();
