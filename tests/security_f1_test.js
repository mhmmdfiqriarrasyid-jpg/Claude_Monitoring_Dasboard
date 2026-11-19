// 2.43.0 — security audit, phase 1 (client side). Values another account can
// write must never run code in the viewer's session (the owner's above all):
//   · a project's manual RAG, drawn into class names;
//   · a receipt's quantity in the purchase request detail and the receipt form;
//   · a Learning item's file format, drawn into a class name;
//   · a link inside an e-book (javascript: used to run in this page).
// Each payload bumps window.__pwn if it runs or turns into markup.
const { launch, BASE_URL } = require('./_env');

(async () => {
    const b = await launch();
    const page = await b.newPage({ viewport: { width: 1366, height: 900 } });
    const errors = [];
    page.on('pageerror', e => { if (!/Chart is not defined/.test(e.message)) errors.push(e.message); });
    page.on('dialog', d => d.dismiss());
    await page.goto(BASE_URL + '/index.html', { waitUntil: 'load' });
    await page.waitForTimeout(900);

    const T = await page.evaluate(async () => {
        const T = []; const t = (n, g, w) => T.push({ n, g, w, pass: JSON.stringify(g) === JSON.stringify(w) });
        const tick = ms => new Promise(r => setTimeout(r, ms || 40));
        window.__pwn = 0;
        const ATTR = 'x" onmouseover="window.__pwn++" data-pwn="1';
        const TAG = '<img src=x data-pwn="1" onerror="window.__pwn++">';
        const ok = () => Promise.resolve();
        window.cloud = { isReady: true, addHistoryEvents: ok, saveProject: ok, saveOtJob: ok, savePurchaseRequest: ok, savePurchasePhoto: ok,
            saveStockItem: ok, subscribeUsers: () => () => {} };
        window.confirm = () => false;
        currentUser = { uid: 'o', email: 'o@x.id' };
        currentUserDoc = { uid: 'o', role: 'owner', status: 'active', email: 'o@x.id', displayName: 'Owner' };
        cloudInitialized = true;
        try { localStorage.setItem('tractorLicenseDatesApplied_v2', '1'); } catch (_) {}
        hideAuthGates(); applyRoleGating();
        const injected = () => document.querySelectorAll('[data-pwn]').length;

        // ---- (a) project RAG ----
        applyProjectsSnapshot([{ id: 'pj_x', name: 'Radio tower', status: 'active', start: '2026-11-01', end: '2026-12-01', ragOverride: ATTR,
            createdByUid: 'z', target: { type: 'milestones' }, milestones: [], updates: [] }]);
        navigateTo('projects'); switchProjectsTab('projects'); renderProjectsView();
        await tick();
        t('RAG: a planted value adds no attribute to the list', [injected(), window.__pwn], [0, 0]);
        t('RAG: an unknown value is ignored — the card shows the automatic RAG (amber: behind schedule)',
            [...document.querySelector('.pj-card').classList].filter(c => c.startsWith('pj-card--')), ['pj-card--amber']);
        showProjectDetail('pj_x'); await tick();
        t('RAG: nor to the project detail', [injected(), window.__pwn], [0, 0]);
        if (typeof closeProjectDetail === 'function') closeProjectDetail();
        applyProjectsSnapshot([{ id: 'pj_y', name: 'RTK', status: 'active', ragOverride: 'red', createdByUid: 'z', target: { type: 'milestones' }, milestones: [], updates: [] }]);
        renderProjectsView(); await tick();
        t('RAG: a real manual RAG still shows', !!document.querySelector('.pj-card--red'), true);

        // ---- (b) purchase receipt quantity ----
        navigateTo('purchase');
        purchaseRequests = [{ id: 'pr_x', prNo: 'PR/1', title: 'Antenna', company: 'PT. GPA', status: 'partial', createdByUid: 'z',
            items: [{ id: 'a', spec: 'Dipole', qty: 2, uom: 'Unit', receivedQty: 1 }],
            receipts: [{ id: 'rc1', date: '2026-11-10', by: 'x', items: [{ itemId: 'a', qty: TAG }], photoIds: [] },
                       { id: 'rc2', date: '2026-11-11', by: 'y', items: [{ itemId: 'a', qty: ATTR }], photoIds: [] }] }];
        showPurchaseDetail('pr_x'); await tick();
        t('receipt qty: no markup in the request detail', [injected(), window.__pwn, (document.getElementById('prDetailBody').textContent.match(/Dipole × 0/g) || []).length], [0, 0, 2]);
        if (typeof showPurchaseReceive === 'function') {
            showPurchaseReceive('pr_x', 'rc2'); await tick();
            t('receipt qty: no attribute in the receipt form', [injected(), window.__pwn], [0, 0]);
            document.getElementById('prReceiveModal').classList.remove('open');
        }
        if (typeof closePurchaseDetail === 'function') closePurchaseDetail();

        // ---- (c) Learning file format ----
        navigateTo('learning');
        applyLearningSnapshot([
            { id: 'lc_x', title: 'Evil book', category: 'General', source: 'upload', file: { id: 'f1', name: 'a.epub', format: ATTR }, createdAt: 1 },
            { id: 'lc_y', title: 'Real book', category: 'General', source: 'upload', file: { id: 'f2', name: 'b.epub', format: 'epub' }, createdAt: 2 }]);
        renderLearningView(); await tick();
        t('Learning: a planted format adds no attribute', [injected(), window.__pwn], [0, 0]);
        t('Learning: it is read as a PDF; a real format is kept',
            [learnFormatOf(learningItems.find(i => i.id === 'lc_x')), learnFormatOf(learningItems.find(i => i.id === 'lc_y')), !!document.querySelector('.lc-item__icon--epub')], ['pdf', 'epub', true]);

        // ---- (d) links inside an e-book ----
        t('e-book links: only http(s) leave the book',
            ['javascript:window.__pwn++', ' JavaScript:alert(1)', 'data:text/html,<script>1</script>', 'vbscript:x', 'https://example.org/a', 'http://example.org']
                .map(lcSafeExternal), [false, false, false, false, true, true]);
        const viewSrc = await (await fetch('vendor/foliate/view.js')).text();
        t('e-book links: the reader itself also refuses other schemes', /OT Dashboard 2\.43\.0[\s\S]{0,400}\^https\?:/.test(viewSrc), true);

        // ---- (e) weekly / daily report cells and items built from stored records ----
        const box = document.createElement('div'); document.body.appendChild(box);
        const evilCell = { text: 'Budi', chip: 'good"><img src=x data-pwn="1" onerror="window.__pwn++">', tone: 'bad" data-pwn="1', a: 'left" data-pwn="1' };
        box.innerHTML = `<table><tbody><tr>${_wrCell(evilCell, { a: 'right" data-pwn="1' })}</tr></tbody></table>`
            + _wrBlockHtml({ type: 'list', items: [{ text: 'x', tone: 'bad" data-pwn="1', tag: { text: 'y', tone: 'good" data-pwn="1' } }] })
            + _wrBlockHtml({ type: 'stats', items: [{ value: 1, label: 'x', tone: 'bad" data-pwn="1', delta: { text: '+1', tone: 'good" data-pwn="1' } }] })
            + _wrBlockHtml({ type: 'rag', items: [['x', 'red" data-pwn="1', 'y']] });
        await tick();
        t('report: an object written into a record field adds no attribute or markup', [injected(), window.__pwn, box.querySelector('td').textContent], [0, 0, 'Budi']);
        t('report: a normal cell keeps its style', (() => { box.innerHTML = `<table><tbody><tr>${_wrCell({ text: 'Open', chip: 'warn', tone: 'bad', a: 'right' })}</tr></tbody></table>`;
            const td = box.querySelector('td'); return [td.className, td.querySelector('span').className]; })(), ['wr-a-right wr-t-bad', 'wr-chip wr-chip--warn']);
        box.remove();

        // ---- (f) deleting a record deletes only the photos named after it ----
        const gone = [];
        window.cloud.deleteProjectPhoto = id => { gone.push(id); return Promise.resolve(); };
        window.cloud.deletePurchasePhoto = id => { gone.push(id); return Promise.resolve(); };
        deleteWhPhotos(['job_a__p1', 'job_victim__p9', 'pj_victim__p2', 'job_a__p2'], 'pj', 'job_a');
        deleteWhPhotos(['pr_1__p1', 'pr_1__rc_1__p1', 'pr_2__p1'], 'pr', 'pr_1');
        t('photos: another record’s photo ids on a record are never deleted with it', gone, ['job_a__p1', 'job_a__p2', 'pr_1__p1', 'pr_1__rc_1__p1']);

        // ---- (g) 2.43.2: a "waiting to upload" placeholder is never cached or put in a report ----
        const WAIT = 'data:image/png;base64,V0FJVA==';
        window.cloud.isWaitingPhoto = s => s === WAIT;
        window.cloud.getProjectPhoto = () => Promise.resolve(WAIT);
        _whPhotoCache.delete('job_w__p1');
        const got1 = await loadWhPhoto('job_w__p1', 'pj');
        t('waiting photo: shown, but not cached for the session', [got1, _whPhotoCache.has('job_w__p1')], [WAIT, false]);
        _wrMedia.delete('pj:job_w__p1');
        await wrLoadMedia([{ key: 'pj:job_w__p1', kind: 'photo', load: () => loadWhPhoto('job_w__p1', 'pj') }]);
        t('waiting photo: left out of the report', (_wrMedia.get('pj:job_w__p1') || {}).state, 'fail');

        await tick(100);
        t('TOTAL: nothing ran', window.__pwn, 0);
        return T;
    });

    let pass = 0;
    T.forEach(x => { if (x.pass) pass++; else console.log('FAIL', x.n, '\n  got   :', JSON.stringify(x.g), '\n  wanted:', JSON.stringify(x.w)); });
    if (errors.length) console.log('PAGE ERRORS:', errors);
    console.log(`${pass}/${T.length} lulus`);
    await b.close();
    process.exit(pass === T.length && !errors.length ? 0 : 1);
})();
