// 2.44.0 — 3D Models: the library, the add / edit form, links to implements and
// units, the sandboxed studio page and the built-in GLB studio.
const fs = require('fs');
const path = require('path');
const { launch, BASE_URL } = require('./_env');

(async () => {
    const b = await launch({ args: ['--use-gl=swiftshader', '--enable-webgl', '--ignore-gpu-blocklist'] });
    const page = await b.newPage({ viewport: { width: 1366, height: 860 } });
    const errors = [];
    page.on('pageerror', e => { if (!/Chart is not defined/.test(e.message)) errors.push(e.message); });
    page.on('dialog', d => d.accept());
    await page.goto(BASE_URL + '/index.html', { waitUntil: 'load' });
    await page.waitForTimeout(900);
    const glb = fs.existsSync(path.join(__dirname, 'fixtures', 'm3_sample.glb'));

    const T = await page.evaluate(async (hasGlb) => {
        const T = []; const t = (n, g, w) => T.push({ n, g, w, pass: JSON.stringify(g) === JSON.stringify(w) });
        const tick = ms => new Promise(r => setTimeout(r, ms || 40));
        const ok = () => Promise.resolve();
        const saved = [], uploads = [], deleted = [], updates = [], partUpd = [];
        const files = {};   // Storage path -> blob URL
        let holdUpload = null;   // a test can keep an upload running
        window.cloud = { isReady: true, addHistoryEvents: ok, subscribeUsers: () => () => {},
            saveModel3d: rec => { saved.push(JSON.parse(JSON.stringify(rec))); return ok(); },
            updateModel3d: rec => { if (!models3d.some(m => m.id === rec.id)) return Promise.reject(Object.assign(new Error('nf'), { code: 'not-found' })); updates.push(JSON.parse(JSON.stringify(rec))); return ok(); },
            updateModel3dPart: (id, key, info, level) => { partUpd.push([id, key, info, level]); return ok(); },
            deleteModel3d: id => { deleted.push(id); return ok(); },
            deleteModel3dFile: p => { deleted.push(p); return ok(); },
            uploadModel3dFile: (p, data, type, prog) => { uploads.push([p, type]); files[p] = URL.createObjectURL(data); if (prog) prog(1); return holdUpload || ok(); },
            model3dFileUrl: p => files[p] ? Promise.resolve(files[p]) : Promise.reject(new Error('missing')) };
        const asOwner = () => { currentUser = { uid: 'o', email: 'o@x.id' }; currentUserDoc = { uid: 'o', role: 'owner', status: 'active', displayName: 'Owner' }; applyRoleGating(); };
        asOwner(); cloudInitialized = true; hideAuthGates();
        try { localStorage.setItem('tractorLicenseDatesApplied_v2', '1'); } catch (_) {}
        globalImplements = [{ id: 'i1', profileName: 'Harrell Series 50', equipmentType: 'Rock Rake', brand: 'Harrell' }, { id: 'i2', profileName: 'Disc 3', equipmentType: 'Disc Harrow' }];
        globalData = [{ id: 'u1', name: 'GGTR101G', sn: 'SN1', model: '6155J', implement: 'Rock Rake — Harrell', status: 'Good' }];

        // ---- library ----
        applyModels3dSnapshot([
            { id: 'm3_a', name: 'Harrell Series 50', kind: 'studio', category: 'Implement', description: 'Bongkar-pasang', file: { path: 'models3d/m3_a/a.html', name: 'a.html', size: 1000 }, implements: ['Rock Rake'], unitModels: [], parts: {} },
            { id: 'm3_b', name: 'Tractor 6155J', kind: 'glb', category: 'Tractor', file: { path: 'models3d/m3_b/b.glb', name: 'b.glb', size: 4000 }, implements: [], unitModels: ['6155J'], parts: {} },
            { id: 'x', name: 'invalid', kind: 'exe', file: {} }]);
        navigateTo('models3d'); await tick();
        t('library: the menu is there for every active account', !!document.querySelector('.nav__link[data-view="models3d"]'), true);
        t('library: a card per valid model', [...document.querySelectorAll('#m3Grid .m3-card h3')].map(h => h.textContent), ['Harrell Series 50', 'Tractor 6155J']);
        setModels3dKind('studio');
        t('library: kind filter', document.querySelectorAll('#m3Grid .m3-card').length, 1);
        setModels3dKind('');
        document.getElementById('m3Search').value = '6155'; renderModels3dView();
        t('library: search finds a unit model', [...document.querySelectorAll('#m3Grid .m3-card h3')].map(h => h.textContent), ['Tractor 6155J']);
        document.getElementById('m3Search').value = ''; renderModels3dView();

        // ---- links ----
        t('links: an implement named by its type has its model', models3dForImplement(globalImplements[0]).map(m => m.id), ['m3_a']);
        t('links: another implement has none', models3dForImplement(globalImplements[1]).length, 0);
        t('links: a unit by its implement and its model', models3dForUnit(globalData[0]).map(m => m.id).sort(), ['m3_a', 'm3_b']);
        setModels3dKind('glb');   // a filter set earlier must not hide a linked model
        _m3OpenFor(models3dForUnit(globalData[0]));
        t('links: two models for a unit — the library shows exactly those', [currentView, [...document.querySelectorAll('#m3Grid .m3-card h3')].map(h => h.textContent).sort(), /Show all/.test(document.getElementById('m3Count').textContent)],
            ['models3d', ['Harrell Series 50', 'Tractor 6155J'], true]);
        m3ShowAll();
        navigateTo('implements'); await tick(100);
        t('links: the implement row carries a 3D button', document.querySelectorAll('.m3-chip').length, 1);
        showUnitProfile('u1'); await tick();
        t('links: the unit profile shows "3D model"', document.getElementById('unitProfile3dBtn').hidden, false);
        closeUnitProfile();

        // ---- gating ----
        currentUserDoc = { uid: 'v', role: 'staff', status: 'active', access: {} }; applyRoleGating();
        navigateTo('models3d'); await tick();
        const addBtn = document.querySelector('#viewModels3d .editonly--models3d');
        t('gating: without edit no "Add model" and no Edit/Delete on cards', [getComputedStyle(addBtn).display, document.querySelectorAll('#m3Grid .m3-card .btn-icon').length], ['none', 0]);
        showAddModel3dForm();
        t('gating: the form does not open without edit', document.getElementById('m3Modal').classList.contains('open'), false);
        currentUserDoc = { uid: 'e', role: 'staff', status: 'active', access: { models3d: 'edit' } }; applyRoleGating(); renderModels3dView();
        t('gating: with 3D Models edit the button shows', getComputedStyle(addBtn).display !== 'none', true);
        asOwner();

        // ---- add: a studio page ----
        const studio = `<!doctype html><html><body><h1>Studio</h1><script>
            const r = { type: 'probe' };
            try { r.parent = parent.document.title; } catch (e) { r.parent = 'blocked'; }
            try { r.ls = localStorage.getItem('x'); } catch (e) { r.ls = 'blocked'; }
            parent.postMessage(r, '*');<\/script></body></html>`;
        const setFile = (id, file) => { const dt = new DataTransfer(); dt.items.add(file); const el = document.getElementById(id); el.files = dt.files; return m3PickFile({ target: el }); };
        showAddModel3dForm();
        await setFile('m3FileInput', new File(['not a model'], 'notes.txt', { type: 'text/plain' }));
        t('form: a file of another kind is refused', document.getElementById('m3FileInput').value, '');
        await setFile('m3FileInput', new File([new Uint8Array([1, 2, 3, 4, 5])], 'fake.glb'));
        t('form: a .glb that is not GLB is refused', document.getElementById('m3FileInput').files.length === 0 || document.getElementById('m3FileInput').value === '', true);
        await setFile('m3FileInput', new File([studio], 'Harrell Rake Studio.html', { type: 'text/html' }));
        t('form: the name follows the file name', document.getElementById('m3Name').value, 'Harrell Rake Studio');
        document.getElementById('m3ImplAdd').value = 'Disc Harrow';
        await saveModel3d(); await tick();
        const rec = saved[saved.length - 1];
        t('save: one document, studio kind, file in its own folder, kept as text', [rec.kind, /^models3d\/m3_[a-z0-9]+\/[a-z0-9]+-Harrell_Rake_Studio\.html$/.test(rec.file.path), uploads[0][1]], ['studio', true, 'text/plain;charset=utf-8']);
        t('save: a link typed but not added still counts', rec.implements, ['Disc Harrow']);
        t('save: the form closes and the card shows', [document.getElementById('m3Modal').classList.contains('open'), models3d.some(m => m.id === rec.id)], [false, true]);

        // ---- open the studio: sandboxed, it reaches nothing of the dashboard ----
        const probe = new Promise(res => window.addEventListener('message', e => { if (e.data && e.data.type === 'probe') res(e.data); }));
        openModel3d(rec.id);
        const got = await Promise.race([probe, new Promise(r => setTimeout(() => r({ timeout: true }), 8000))]);
        const fr = document.querySelector('#m3ViewerBody iframe');
        t('studio: runs in a sandbox with scripts only', fr && fr.getAttribute('sandbox'), 'allow-scripts');
        t('studio: no access to the dashboard page or its storage', [got.parent, got.ls], ['blocked', 'blocked']);
        closeModel3dViewer();
        t('studio: closing removes the frame', document.querySelectorAll('#m3ViewerBody iframe').length, 0);

        // ---- the frame page refuses to run outside the dashboard's sandbox ----
        const loose = document.createElement('iframe');
        let looseReady = false;
        const onLoose = e => { if (e.source === loose.contentWindow && e.data && e.data.type === 'ot-studio-ready') looseReady = true; };
        window.addEventListener('message', onLoose);
        loose.src = 'models3d-frame.html'; document.body.appendChild(loose);
        await tick(1200);
        window.removeEventListener('message', onLoose); loose.remove();
        t('frame: framed without a sandbox it does nothing (no "ready", nothing written)', looseReady, false);

        // ---- edit and delete ----
        editModel3d(rec.id);
        document.getElementById('m3Desc').value = 'From the parts manual';
        await saveModel3d(); await tick();
        const rec2 = updates[updates.length - 1];
        t('edit: an update of the same id and file, new description', [rec2.id, rec2.file.path, rec2.description], [rec.id, rec.file.path, 'From the parts manual']);
        // Replacing the file with one of the same name never touches the file in use.
        editModel3d(rec.id);
        await setFile('m3FileInput', new File([studio], 'Harrell Rake Studio.html', { type: 'text/html' }));
        const delBefore = deleted.length;
        await saveModel3d(); await tick();
        const rec3 = updates[updates.length - 1];
        t('edit: a new file of the same name gets its own path; the old one goes only after the save',
            [rec3.file.path !== rec.file.path, uploads[uploads.length - 1][0] === rec3.file.path, deleted.slice(delBefore).includes(rec.file.path), deleted.includes(rec3.file.path)], [true, true, true, false]);
        // "Close anyway" while uploading: nothing saved, the upload removed again.
        let release; holdUpload = new Promise(r => { release = r; });
        showAddModel3dForm();
        await setFile('m3FileInput', new File([studio], 'second.html', { type: 'text/html' }));
        const nSaved = saved.length;
        const pending = saveModel3d();
        await tick(30);
        closeModel3dForm();   // the confirm is accepted
        release(); holdUpload = null;
        await pending; await tick();
        const abandoned = uploads[uploads.length - 1][0];
        t('form: closed during the upload — nothing saved, the upload removed', [saved.length === nSaved, deleted.includes(abandoned), document.getElementById('m3Modal').classList.contains('open')], [true, true, false]);
        await deleteModel3d(rec.id); await tick();
        t('delete: the document and its file go', [deleted.includes(rec.id), deleted.includes(rec3.file.path), models3d.some(m => m.id === rec.id)], [true, true, false]);

        // ---- the GLB studio ----
        if (hasGlb) {
            const res = await fetch('tests/fixtures/m3_sample.glb');
            const blob = await res.blob();
            files['models3d/m3_g/sample.glb'] = URL.createObjectURL(blob);
            applyModels3dSnapshot([...models3d, { id: 'm3_g', name: 'Sample rake', kind: 'glb', category: 'Implement', file: { path: 'models3d/m3_g/sample.glb', name: 'sample.glb', size: blob.size }, implements: [], unitModels: [], parts: {}, thumb: '' }]);
            openModel3d('m3_g');
            for (let i = 0; i < 100 && !document.querySelector('#m3PartList .m3s-row'); i++) await tick(100);
            const rows = document.querySelectorAll('#m3PartList .m3s-row');
            t('glb: the parts list fills', rows.length > 1, true);
            if (!rows.length) { t('glb: viewer message', (document.getElementById('m3ViewerBody') || {}).textContent.trim().slice(0, 120), ''); return T; }
            t('glb: a canvas is drawn', !!document.querySelector('#m3Canvas canvas'), true);
            const key = rows[0].dataset.key;
            m3Pick(key); await tick(120);
            t('glb: picking a part opens its card', document.getElementById('m3Insp').hidden, false);
            m3Do('toggle'); await tick(600);
            t('glb: taking a part off is counted', /^\d+ \/ \d+ in place$/.test(document.getElementById('m3sProgressText').textContent) && document.querySelector('#m3PartList .m3s-row.is-off') !== null, true);
            m3EditPart(true); await tick();
            document.getElementById('m3pItem').value = 'F1'; document.getElementById('m3pNum').value = '85-566-50'; document.getElementById('m3pQty').value = '2';
            await m3SavePart(); await tick();
            const pu = partUpd[partUpd.length - 1] || [];
            t('glb: only that part\'s details are written, under a safe key', [pu[0], /^[A-Za-z0-9_-]+$/.test(pu[1] || ''), pu[2] && pu[2].partNumber, pu[2] && pu[2].qty], ['m3_g', true, '85-566-50', 2]);
            m3EditPart(true); await tick();
            document.getElementById('m3pQty').value = '';
            t('glb: a blank quantity reads as 1', (await (async () => { const n = partUpd.length; await m3SavePart(); await tick(); return (partUpd[n] || [])[2]; })()).qty, 1);
            t('glb: the row shows the item # and part number', document.querySelector(`#m3PartList .m3s-row[data-key="${CSS.escape(key)}"]`).textContent.includes('85-566-50'), true);
            document.getElementById('m3PartSearch').value = '85-566'; m3RenderParts();
            t('glb: search by part number', document.querySelectorAll('#m3PartList .m3s-row').length, 1);
            document.getElementById('m3PartSearch').value = ''; m3RenderParts();
            m3Do('attachAll'); await tick(600);
            t('glb: put all back', document.querySelectorAll('#m3PartList .m3s-row.is-off').length, 0);
            // An owner opening a GLB without a picture gives it one.
            for (let i = 0; i < 30 && !updates.some(s => s.id === 'm3_g' && s.thumb); i++) await tick(100);
            t('glb: the first editor to open it saves a card picture', updates.some(s => s.id === 'm3_g' && s.thumb === 'models3d/m3_g/thumb.jpg') && uploads.some(u => u[0] === 'models3d/m3_g/thumb.jpg' && u[1] === 'image/jpeg'), true);
            // Deleted by someone else while open: a part note does not bring it back.
            const keep = models3d.slice();
            applyModels3dSnapshot(models3d.filter(m => m.id !== 'm3_g'));
            m3EditPart(true); await tick();
            const nPart = partUpd.length;
            await m3SavePart(); await tick();
            t('glb: a model deleted meanwhile is not written again', partUpd.length, nPart);
            applyModels3dSnapshot(keep);
            closeModel3dViewer();
            t('glb: closing frees the viewer', document.querySelectorAll('#m3ViewerBody canvas').length, 0);
            // Opened twice in a row (a double click): one viewer, not two.
            openModel3d('m3_g'); openModel3d('m3_g');
            for (let i = 0; i < 100 && !document.querySelector('#m3PartList .m3s-row'); i++) await tick(100);
            await tick(300);
            t('glb: opened twice quickly — one canvas', document.querySelectorAll('#m3ViewerBody canvas').length, 1);
            closeModel3dViewer();
        }
        return T;
    }, glb);

    // Escape pressed inside a studio page closes the viewer.
    await page.evaluate(() => {
        const studio = '<!doctype html><html><body><h1>Studio</h1></body></html>';
        const url = URL.createObjectURL(new Blob([studio], { type: 'text/plain' }));
        window.cloud.model3dFileUrl = () => Promise.resolve(url);
        applyModels3dSnapshot([{ id: 'm3_e', name: 'Esc', kind: 'studio', category: 'Other', file: { path: 'models3d/m3_e/e.html', name: 'e.html', size: studio.length }, implements: [], unitModels: [], parts: {} }]);
        openModel3d('m3_e');
    });
    await page.waitForTimeout(1500);
    const sf = page.frames().find(f => f !== page.mainFrame() && /models3d-frame/.test(f.url()));
    if (sf) await sf.evaluate(() => document.dispatchEvent(new KeyboardEvent('keydown', { key: 'Escape', bubbles: true })));
    await page.waitForTimeout(300);
    const escClosed = await page.evaluate(() => !document.getElementById('m3Viewer').classList.contains('open'));
    T.push({ n: 'studio: Escape inside the studio closes the viewer', g: [!!sf, escClosed], w: [true, true], pass: !!sf && escClosed });

    let pass = 0;
    T.forEach(x => { if (x.pass) pass++; else console.log('FAIL', x.n, '\n  got   :', JSON.stringify(x.g), '\n  wanted:', JSON.stringify(x.w)); });
    if (!glb) console.log('(GLB fixture missing — GLB studio checks skipped)');
    if (errors.length) console.log('PAGE ERRORS:', errors);
    console.log(`${pass}/${T.length} lulus`);
    await b.close();
    process.exit(pass === T.length && !errors.length && glb ? 0 : 1);
})();
