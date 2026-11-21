// 2.45.0 — security audit, phase 2 (cluster C, client data safety):
//   B2  tickets, projects, jobs and purchase requests written offline count as
//       "waiting to send" (pill, toast, sign-out & erase, automatic erase), and
//       an erase waits for anything else Firestore still holds;
//   B4  an admin changing this account's role or access keeps what the server
//       refused (the "Not saved" list, with its photos) and its audit queue;
//   S14 a ';' or line break inside an exported value cannot start a live cell
//       in semicolon-locale Excel, and every export still imports back as it was;
//   B10 "Export photos (ZIP)" holds the photos kept in Cloud Storage.
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
        const until = async (fn, ms) => { const end = Date.now() + (ms || 3000); while (!fn() && Date.now() < end) await tick(30); };
        const toasts = [];
        window.showToast = m => { toasts.push(String(m)); };
        let confirmAnswer = false, confirmText = '';
        window.confirm = m => { confirmText = String(m); return confirmAnswer; };
        const blobs = [];
        URL.createObjectURL = bl => { blobs.push(bl); return 'blob:test'; };
        URL.revokeObjectURL = () => {};
        HTMLAnchorElement.prototype.click = function () {};
        const ok = () => Promise.resolve();
        window.cloud = { isReady: true, addHistoryEvents: ok, subscribeUsers: () => () => {},
            saveUnits: ok, updateUnitsFields: ok, deleteUnits: ok, saveImplements: ok, saveLicenses: ok, saveLicense: ok,
            saveDamages: ok, saveDamage: ok, signOutUser: ok };
        const as = doc => { currentUser = { uid: doc.uid, email: 'x@x.id' }; currentUserDoc = Object.assign({ status: 'active', email: 'x@x.id' }, doc); applyRoleGating(); };
        as({ uid: 'o', role: 'owner', displayName: 'Owner' });
        try { localStorage.setItem('tractorLicenseDatesApplied_v2', '1'); localStorage.setItem('tractorDamagePhotosSplit', '1'); } catch (_) {}
        hideAuthGates();
        inspections = []; insQueuedIds = new Set();

        // =============== B2: offline tickets / projects / jobs / purchase requests ===============
        const pill = () => { const p = document.getElementById('queuedPill'); return p && p.style.display !== 'none' ? p.textContent.trim() : ''; };
        const queueAll = () => { setQueued('tickets', ['t1']); setQueued('projects', ['p1', 'p2']); setQueued('otJobs', ['j1']); setQueued('purchaseRequests', ['r1']); };
        const unqueueAll = () => ['tickets', 'projects', 'otJobs', 'purchaseRequests'].forEach(k => setQueued(k, []));
        queueAll();
        t('B2: the top bar counts them', pill(), '5 waiting to send');
        t('B2: and names them', queuedSummary(0), ['1 ticket', '2 projects', '1 job', '1 purchase request']);
        confirmAnswer = false; confirmText = '';
        await handleSignOutErase();
        t('B2: "Sign out & erase" warns they will be lost',
            /these will be LOST: 1 ticket, 2 projects, 1 job, 1 purchase request\./.test(confirmText), true);
        confirmText = '';
        await handleSignOut();
        t('B2: plain sign-out names them too', /^Not sent yet: 1 ticket, 2 projects, 1 job, 1 purchase request\./.test(confirmText), true);
        toasts.length = 0;
        unqueueAll();
        t('B2: once they land — count gone, and said', [pill(), toasts.includes('Everything saved offline has now been sent')], ['', true]);

        // The automatic erase of a newly limited account leaves them alone.
        const scoped = { uid: 'hv', role: 'staff', access: { editUnits: 'edit', damage: 'edit', unitScope: 'heavy' } };
        as(scoped);
        let erased = 0, reloaded = 0;
        window.cloud.eraseLocalCache = () => { erased++; return Promise.resolve(true); };
        window.appReload = () => { reloaded++; };
        const freshScope = () => { localStorage.setItem('tractorDeviceScope', 'all'); localStorage.removeItem('tractorDeviceScopeTried'); };
        freshScope();
        setQueued('tickets', ['t1']);
        const r1 = await protectDeviceForScope('hv');
        t('B2: automatic erase skipped while a ticket waits (and tried again later)',
            [r1, erased, reloaded, localStorage.getItem('tractorDeviceScopeTried')], [false, 0, 0, null]);
        setQueued('tickets', []);
        // Nothing any list marks, but Firestore still holds a write (a unit, a
        // fuel entry, a delete — or a list that has not reported yet).
        window.cloud.hasPendingWrites = () => Promise.resolve(true);
        const r2 = await protectDeviceForScope('hv');
        t('B2: automatic erase skipped while Firestore holds any unsent write',
            [r2, erased, reloaded, localStorage.getItem('tractorDeviceScopeTried')], [false, 0, 0, null]);
        confirmText = '';
        await handleSignOutErase();
        t('B2: "Sign out & erase" says so as well', /these will be LOST: other changes made offline\./.test(confirmText), true);
        window.cloud.hasPendingWrites = () => Promise.reject(new Error('terminated'));
        t('B2: a check that fails counts as "something is waiting"', typeof cloudHasPendingWrites === 'function' && await cloudHasPendingWrites(), true);
        window.cloud.hasPendingWrites = () => Promise.resolve(false);
        confirmText = '';
        await handleSignOutErase();
        t('B2: nothing waiting — no warning', /Not sent yet/.test(confirmText), false);
        const r3 = await protectDeviceForScope('hv');
        t('B2: nothing waiting — the device is cleared once, as before', [r3, erased, reloaded], [true, 1, 1]);
        delete window.cloud.hasPendingWrites;
        as({ uid: 'o', role: 'owner', displayName: 'Owner' });

        // Signing out empties the counts without anything being sent.
        queueAll(); toasts.length = 0;
        tearDownCloudSync();
        t('B2: sign-out clears the count, without "everything has been sent"',
            [pill(), toasts.includes('Everything saved offline has now been sent')], ['', false]);

        // =============== B4: an admin's role/access change keeps the unsent list ===============
        const field = { uid: 'f1', role: 'staff', status: 'active', email: 'f@x.id', access: { teamLog: 'edit' } };
        as(field);
        let userDocCb = null;
        window.cloud.subscribeUserDoc = (uid, cb) => { userDocCb = cb; return () => {}; };
        stopWatchingOwnUserDoc(); watchOwnUserDoc('f1');
        clearUnsent();
        let reopened = 0;
        const key = rememberUnsent('Daily report 2026-11-20 (4 photos)', { code: 'permission-denied' }, () => { reopened++; });
        const addHist = window.cloud.addHistoryEvents;
        delete window.cloud.addHistoryEvents;   // keep the queued row where the test can see it
        _historyPushQueue.push({ id: 'h_f1', actorUid: 'f1', action: 'x' });
        window.cloud.isReady = false;            // the re-subscribe itself is not under test
        inspections = [{ id: 'marker' }];
        cloudInitialized = true;
        userDocCb(Object.assign({}, field, { access: { teamLog: 'edit', damage: 'edit' } }));
        t('B4: the lists were torn down and subscribed again', [inspections.length, toasts.includes('Your access rights were updated by an admin')], [0, true]);
        t('B4: access change — the refused report stays, with its notice and audit row',
            [_unsent.size, !!document.getElementById('unsentNotice'), _historyPushQueue.length], [1, true, 1]);
        cloudInitialized = true;
        userDocCb(Object.assign({}, field, { role: 'supervisor', access: { teamLog: 'edit', damage: 'edit' } }));
        t('B4: role change — still there', [_unsent.size, _historyPushQueue.length], [1, 1]);
        reopenUnsent(key);
        t('B4: "Open it again" still opens it', [reopened, _unsent.size], [1, 0]);
        rememberUnsent('Leave request', { code: 'permission-denied' }, () => {});
        tearDownCloudSync();
        t('B4: sign-out still clears both (the next person must not get them)',
            [_unsent.size, !!document.getElementById('unsentNotice'), _historyPushQueue.length], [0, false, 0]);
        stopWatchingOwnUserDoc();
        window.cloud.isReady = true;
        window.cloud.addHistoryEvents = addHist;
        as({ uid: 'o', role: 'owner', displayName: 'Owner' });

        // =============== S14: CSV cells in semicolon-locale Excel ===============
        // Excel with ';' as list separator: a cell may begin after any ';' or
        // line break, whatever the quotes say. A cell is live when, past spaces
        // and quote marks, it begins with = + - @.
        const liveCells = text => text.split(/[;\r\n]/).map(c => c.replace(/^[ \t"]+/, '')).filter(c => /^[=+\-@]/.test(c));
        const PAY = ["Cek kabel;=cmd|' /C calc'!A0;", 'Ganti;=HYPERLINK(CONCAT(CHAR(104),CHAR(116)),A2);', 'a; +62 812 000',
            'a;\t-1+2', 'x;"=HYPERLINK(1)', 'baris 1\n=1+1', 'baris 1\r\n@SUM(1)', 'a;;  -cmd', '=start', '-5 m', '@x',
            'oli; 5 liter', 'a;b;c', 'tanpa titik koma', ''];
        const csv = toCSV(['Note'], PAY.map(p => [p]));
        const oldCell = v => { v = v == null ? '' : String(v); if (/^[=+\-@\t\r]/.test(v)) v = "'" + v; return '"' + v.replace(/"/g, '""') + '"'; };
        t('S14: control — the old export had live cells once split on ";"', liveCells(PAY.map(p => oldCell(p)).join('\n')).length > 0, true);
        t('S14: no cell can start a formula, split on ";" or on lines', liveCells(csv), []);
        t('S14: the reported payload', csvCell("Cek kabel;=cmd|' /C calc'!A0;"), `"Cek kabel;'=cmd|' /C calc'!A0;"`);
        t('S14: plain text is left as it was', ['oli; 5 liter', 'a;b;c', 'tanpa titik koma', ''].map(csvCell), ['"oli; 5 liter"', '"a;b;c"', '"tanpa titik koma"', '""']);
        // Google Sheets / LibreOffice (and Excel with ','): ordinary CSV, first
        // row the header, one cell per value — and every value reads back.
        const parsed = Papa.parse(csv, { delimiter: ',' }).data;
        t('S14: comma CSV — header first, one cell per row', [parsed[0], parsed.length, parsed.every(r => r.length === 1)], [['Note'], PAY.length + 1, true]);
        t('S14: every value reads back exactly (uncsvCell)', parsed.slice(1).map(r => uncsvCell(r[0])), PAY);

        // Real round trips through the app's own exporters and importers.
        const exported = async fn => { blobs.length = 0; fn(); await tick(10); const bl = blobs[blobs.length - 1]; return bl ? (await bl.text()).replace(/^﻿/, '') : ''; };
        const NOTE1 = "Kirim;=HYPERLINK(CHAR(104));", NOTE2 = '- ganti; -2 unit\n+1 cadangan';
        // License stock
        globalLicenseStock = [{ id: 'L1', txnType: 'IN', licenseType: 'SF-RTK', qty: 2, date: '2026-11-01', note: NOTE1 },
                              { id: 'L2', txnType: 'IN', licenseType: 'SF-RTK', qty: 1, date: '2026-11-02', note: NOTE2 }];
        const licCsv = await exported(exportLicenseStockCSV);
        t('S14: license stock export is safe in ";" Excel', [licCsv.length > 0, liveCells(licCsv)], [true, []]);
        globalLicenseStock = [];
        handleLicenseCSVImport(new File([licCsv], 'license_stock.csv', { type: 'text/csv' }));
        await until(() => globalLicenseStock.length >= 2);
        t('S14: license stock export → import gives the same notes', globalLicenseStock.map(r => r.note).sort(), [NOTE1, NOTE2].sort());
        // Implements
        globalImplements = [{ id: 'i1', profileName: 'Bajak;=1+1', brand: 'Deere', code: 'C-1', operation: 'a\n-b', equipmentType: '@Tillage' }];
        const impCsv = await exported(exportImplementsCSV);
        t('S14: implements export is safe in ";" Excel', [impCsv.length > 0, liveCells(impCsv)], [true, []]);
        globalImplements = [];
        handleImplementCSVImport(new File([impCsv], 'implements.csv', { type: 'text/csv' }));
        await until(() => globalImplements.length >= 1);
        const imp = globalImplements[0] || {};
        t('S14: implements export → import gives the same values',
            [imp.profileName, imp.code, imp.operation, imp.equipmentType], ['Bajak;=1+1', 'C-1', 'a\n-b', '@Tillage']);
        // Units
        const unit = { id: 'u_rt', name: 'TR;=1', model: '6110B', sn: 'SNRT1', status: 'Good', display: 'Good', gps: 'Good', steering: 'Good', jdlink: 'Good',
            site: 'PT. GPA', remarks: 'Cek;=WEBSERVICE(A1);\n-ok' };
        const unitCsv = await exported(() => exportCSV([unit]));
        t('S14: units export is safe in ";" Excel', [unitCsv.length > 0, liveCells(unitCsv)], [true, []]);
        globalData = [];
        confirmAnswer = true;
        handleEditCSVImport(new File([unitCsv], 'tractor_monitoring.csv', { type: 'text/csv' }));
        await until(() => globalData.length >= 1);
        confirmAnswer = false;
        const u = globalData.find(x => x.sn === 'SNRT1') || {};
        t('S14: units export → import gives the same values', [u.name, u.remarks], ['TR;=1', 'Cek;=WEBSERVICE(A1);\n-ok']);

        // =============== B10: damage photo ZIP holds the Cloud Storage photos ===============
        const URL_OF = id => `https://firebasestorage.googleapis.com/v0/b/x/o/photos%2F${id}.jpg?alt=media`;
        const DATA = s => 'data:image/jpeg;base64,' + s;
        const asked = [];
        window.cloud.getDamagePhoto = (id, opts) => { asked.push(['B', id, opts || null]); return Promise.resolve(opts && opts.inline ? DATA('/9j/' + id) : URL_OF(id)); };
        window.cloud.getDamageAfterPhoto = (id, opts) => { asked.push(['A', id, opts || null]); return Promise.resolve(opts && opts.inline ? DATA('/9j/A' + id) : URL_OF(id + '_a')); };
        const mk = (id, x) => Object.assign({ id, date: '2026-11-10', unitId: 'u1', unitName: 'BD100T', sn: 'LGCB1', site: 'PT. GPA',
            damageType: 'Mekanis', component: 'Camera AI', description: 'd ' + id, resolved: false, resolvedAt: '', createdAt: 1, updatedAt: 1 }, x);
        globalData = [{ id: 'u1', name: 'BD100T', sn: 'LGCB1', site: 'PT. GPA', status: 'Good' }];
        globalDamages = [mk('sA', { hasPhoto: true }), mk('sB', { hasPhoto: true, hasAfterPhoto: true, resolved: true, resolvedAt: '2026-11-12', component: 'GPS' })];
        _dmgPhotoCache.clear(); _dmgAfterCache.clear();
        _dmgPhotoCache.set('sA', URL_OF('sA'));   // viewed earlier this session: a link, not data
        saveToStorage(globalData); saveDamages();
        navigateTo('damage'); setDamageGroup(''); damageUnitFilter = '';
        ['damageSearch', 'damageTypeFilter', 'damageRepairFilter'].forEach(id => { const el = document.getElementById(id); if (el) el.value = ''; });
        renderDamageTable();
        confirmAnswer = true; blobs.length = 0; toasts.length = 0;
        await exportDamagePhotosZip();
        confirmAnswer = false;
        const zb = blobs.find(x => x.type === 'application/zip');
        const zbytes = zb ? new Uint8Array(await zb.arrayBuffer()) : new Uint8Array(0);
        const zdv = new DataView(zbytes.buffer), zdec = new TextDecoder(), zfiles = [];
        for (let p = 0; p + 4 <= zbytes.length && zdv.getUint32(p, true) === 0x04034b50;) {
            const size = zdv.getUint32(p + 18, true), nl = zdv.getUint16(p + 26, true);
            zfiles.push({ name: zdec.decode(zbytes.slice(p + 30, p + 30 + nl)), size });
            p += 30 + nl + size;
        }
        t('B10: the ZIP holds every Storage photo (Before and After)', zfiles.map(f => f.name).sort(),
            ['BD100T/2026-11-10_Mechanical_Camera AI_before.jpg', 'BD100T/2026-11-10_Mechanical_GPS_after.jpg',
             'BD100T/2026-11-10_Mechanical_GPS_before.jpg', 'photo_list.csv']);
        t('B10: nothing reported missing', toasts.pop(), '3 photo(s) in 1 unit folder(s)');
        t('B10: asked for the bytes, without moving anything', asked.every(a => a[2] && a[2].inline === true && a[2].noMigrate === true) && asked.length === 3, true);
        // A photo held inline (old record, or still in this tab as data) is used as it is.
        asked.length = 0; _dmgPhotoCache.set('sA', DATA('/9j/HELD'));
        globalDamages = [mk('sA', { hasPhoto: true })]; saveDamages();
        renderDamageTable(); confirmAnswer = true; toasts.length = 0;
        await exportDamagePhotosZip();
        confirmAnswer = false;
        t('B10: a photo already here as data is not fetched again', [asked.length, toasts.pop()], [0, '1 photo(s) in 1 unit folder(s)']);
        return T;
    });

    let pass = 0;
    T.forEach(x => { if (x.pass) pass++; else console.log('FAIL', x.n, '\n  got   :', JSON.stringify(x.g), '\n  wanted:', JSON.stringify(x.w)); });
    if (errors.length) console.log('PAGE ERRORS:', errors);
    console.log(`${pass}/${T.length} lulus`);
    await b.close();
    process.exit(pass === T.length && !errors.length ? 0 : 1);
})();
