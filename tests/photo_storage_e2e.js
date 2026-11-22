// 2.17.0 — the real firebase-init.js photo layer (photos in Cloud Storage)
// against the Firestore + Storage emulators. Not part of npm test: it needs
// the firebase npm package (for the SDK files) and the emulators, e.g. from a
// folder with firebase + firebase-tools installed and open rules:
//   BASE_URL=http://127.0.0.1:8811 npx firebase emulators:exec --only firestore,storage \
//     --project ot-monitoring-tractor "node <repo>/tests/photo_storage_e2e.js"
// (Firestore emulator on 8182, Storage on 9198; serve the repo on BASE_URL.)
const { chromium } = require(require.resolve('playwright', { paths: [process.cwd(), __dirname] }));
const fs = require('fs'); const path = require('path');
const FB = path.dirname(require.resolve('firebase/package.json', { paths: [process.cwd()] }));
const ROOT = path.join(__dirname, '..');
const BASE = process.env.BASE_URL || 'http://127.0.0.1:8811';
(async () => {
  const b = await chromium.launch({ args: ['--disable-features=LocalNetworkAccessChecks,PrivateNetworkAccessChecks,BlockInsecurePrivateNetworkRequests,PrivateNetworkAccessSendPreflights'] });
  const ctx = await b.newContext({ serviceWorkers: 'block' });
  const page = await ctx.newPage();
  const logs = []; page.on('console', m => { logs.push(m.type()+': '+m.text()); }); page.on('requestfailed', q => logs.push('FAILED ' + q.url()));
  page.on('pageerror', e => logs.push('PAGEERR ' + e.message));
  await page.route(/^https:\/\/www\.gstatic\.com\/firebasejs\/[\d.]+\/(firebase-[a-z-]+\.js)$/, r => {
    const u = r.request().url(), name = u.split('/').pop();
    r.fulfill({ status: 200, contentType: 'text/javascript', headers: { 'access-control-allow-origin': '*' },
      body: fs.readFileSync(path.join(FB, name), 'utf8').split('/firebasejs/10.14.1/').join('/firebasejs/12.12.0/') });
  });
  await page.route(/^https?:\/\/(?!localhost|127\.0\.0\.1|www\.gstatic\.com)/, r => r.abort());
  await page.route('**/firebase-init.js', r => {
    let s = fs.readFileSync(path.join(ROOT, 'firebase-init.js'), 'utf8');
    s = s.replace("const APP_CHECK_SITE_KEY = '6LdyDwsuAAAAABX7mliizaCC7z99eHVXRFzfxFpx';", "const APP_CHECK_SITE_KEY = '';");
    s = s.replace("console.log(`[cloud] offline cache: ${cacheMode}`);", "FS.connectFirestoreEmulator(db, '127.0.0.1', 8182); window.__fs = FS; window.__db = db;");
    s = s.replace(".then(mod => ({ mod, s: mod.getStorage(app) }))", ".then(mod => { const s = mod.getStorage(app); mod.connectStorageEmulator(s, '127.0.0.1', 9198); window.__st = { mod, s }; return { mod, s }; })");
    r.fulfill({ status: 200, contentType: 'text/javascript', body: s });
  });
  await page.route('**/index.html', r => r.fulfill({ status: 200, contentType: 'text/html',
    body: fs.readFileSync(path.join(ROOT, 'index.html'), 'utf8').replace(/<meta http-equiv="Content-Security-Policy"[^>]*>/, '') }));
  setTimeout(() => { console.log('TIMEOUT\n' + logs.slice(-30).join('\n')); process.exit(2); }, 90000);
  await page.goto(BASE + '/index.html'); await page.waitForTimeout(2500);
  const ready = await page.evaluate(() => !!window.cloud); if (!ready) { console.log('NOT READY\n' + logs.join('\n')); await b.close(); process.exit(1); }
  const R = await page.evaluate(async () => {
    const out = {}; const FS = window.__fs, db = window.__db, C = window.cloud;
    const raw = async (col, id) => (await FS.getDoc(FS.doc(db, col, id))).data();
    // tiny valid jpeg-ish data url
    const c = document.createElement('canvas'); c.width = 4; c.height = 4; const img1 = c.toDataURL('image/jpeg'), img2 = c.toDataURL('image/png');
    await C.saveDamagePhoto('d1', img1);
    const d1 = await raw('damagePhotos', 'd1');
    out.stored = String(d1.photo).slice(0, 26);
    const st = await (async () => { const { mod, s } = window.__st; return { mod, s }; })();
    out.objExists = !!(await st.mod.getMetadata(st.mod.ref(st.s, d1.photo.slice(3))).catch(() => null));
    const url = await C.getDamagePhoto('d1');
    out.url = /^http:\/\/127\.0\.0\.1:9198\/v0\/b\//.test(url);
    out.inline = (await C.getDamagePhoto('d1', { inline: true })).slice(0, 23);
    // work log list: keep one by URL, add one new
    await C.saveWorkLogPhotos('wl1', [img1, img2]);
    const w1 = await raw('workLogPhotos', 'wl1');
    const urls = await C.getWorkLogPhotos('wl1');
    await C.saveWorkLogPhotos('wl1', [urls[1], img1]);
    const w2 = await raw('workLogPhotos', 'wl1');
    out.kept = w2.photos[0] === w1.photos[1];
    out.newOne = w2.photos[1].startsWith('sp:') && w2.photos[1] !== w1.photos[0];
    await new Promise(r => setTimeout(r, 500));
    out.removedGone = !(await st.mod.getMetadata(st.mod.ref(st.s, w1.photos[0].slice(3))).catch(() => null));
    // 2.26.1: a report whose photo document never landed — images only in Storage
    for (const n of ['k1', 'k2', 'k3']) await st.mod.uploadString(st.mod.ref(st.s, `photos/workLogPhotos/wl_lost/${n}`), img1, 'data_url');
    out.lostPlain = (await C.getWorkLogPhotos('wl_lost')).length;
    const rec = await C.getWorkLogPhotos('wl_lost', { recover: true, expect: 2 });
    out.recovered = [rec.length, rec.every(u => /^http:\/\/127\.0\.0\.1:9198\//.test(u))];
    await new Promise(r => setTimeout(r, 800));
    const lost = await raw('workLogPhotos', 'wl_lost');
    out.writtenBack = lost && lost.photos;
    // 2.29.5: photo outbox — no signal: photos wait on the device as pending
    // tokens (positions kept); signal back: they upload and replace the tokens.
    Object.defineProperty(navigator, 'onLine', { configurable: true, get: () => false });
    await C.saveWorkLogPhotos('wl_out', [img1, img2, img1]);
    const o1 = await raw('workLogPhotos', 'wl_out');
    out.outboxPending = [o1.photos.length, o1.photos.every(x => x.startsWith('pending:')), await C.pendingPhotoUploads()];
    Object.defineProperty(navigator, 'onLine', { configurable: true, get: () => true });
    const left = await C.flushPhotoOutbox();
    const o2 = await raw('workLogPhotos', 'wl_out');
    out.outboxFlushed = [left, o2.photos.length, o2.photos.every(x => x.startsWith('sp:')), await C.pendingPhotoUploads()];
    // 2.43.0 (B1): a report reopened while its photos still wait keeps them — one
    // removed and one added used to lose every photo that was waiting.
    const offline = on => Object.defineProperty(navigator, 'onLine', { configurable: true, get: () => !on });
    const exists = async p => !!(await st.mod.getMetadata(st.mod.ref(st.s, p)).catch(() => null));
    offline(true);
    await C.saveWorkLogPhotos('wl_b1', [img1, img2, img1]);
    const b0 = (await raw('workLogPhotos', 'wl_b1')).photos;
    const shown = await C.getWorkLogPhotos('wl_b1');                    // the form opens: the photos themselves
    out.b1Shown = [shown.length, shown.every(x => /^data:image\/(jpeg|png);base64,/.test(x))];
    const c3 = document.createElement('canvas'); c3.width = 6; c3.height = 6; c3.getContext('2d').fillRect(0, 0, 3, 3);
    await C.saveWorkLogPhotos('wl_b1', [shown[0], shown[2], c3.toDataURL('image/jpeg')]);     // the 2nd removed, a new one added
    const b1 = (await raw('workLogPhotos', 'wl_b1')).photos;
    out.b1Kept = [b1.length, b1[0] === b0[0], b1[1] === b0[2], b1[2].startsWith('pending:') && !b0.includes(b1[2]), await C.pendingPhotoUploads()];
    offline(false);
    const left1 = await C.flushPhotoOutbox();
    const b2 = (await raw('workLogPhotos', 'wl_b1')).photos;
    out.b1Flushed = [left1, b2.length, b2.every(x => x.startsWith('sp:photos/workLogPhotos/wl_b1/')), await C.pendingPhotoUploads()];
    // A photo waiting on another phone: a placeholder here, and saved back it is still that photo.
    await FS.setDoc(FS.doc(db, 'workLogPhotos', 'wl_b2'), { id: 'wl_b2', photos: ['pending:elsewhere1'], updatedAt: 1 });
    const sh2 = await C.getWorkLogPhotos('wl_b2');
    out.b2Placeholder = /^data:image\/png;base64,/.test(sh2[0]);
    await C.saveWorkLogPhotos('wl_b2', [sh2[0], img1]);
    const b2d = (await raw('workLogPhotos', 'wl_b2')).photos;
    out.b2Kept = [b2d[0], b2d[1].startsWith('sp:')];
    // Same browser: the form was opened before the upload, saved after it — the upload is kept.
    offline(true);
    await C.saveWorkLogPhotos('wl_b3', [img1]);
    const sh3 = await C.getWorkLogPhotos('wl_b3');
    offline(false);
    await C.flushPhotoOutbox();
    const up3 = (await raw('workLogPhotos', 'wl_b3')).photos[0];
    await C.saveWorkLogPhotos('wl_b3', [sh3[0], img2]);
    await new Promise(r => setTimeout(r, 500));
    const b3 = (await raw('workLogPhotos', 'wl_b3')).photos;
    out.b3 = [up3.startsWith('sp:'), b3[0] === up3, await exists(up3.slice(3))];
    // Another phone sent it while the form was open here: its upload is kept, nothing deleted.
    await FS.setDoc(FS.doc(db, 'workLogPhotos', 'wl_b4'), { id: 'wl_b4', photos: ['pending:elsewhere2'], updatedAt: 1 });
    const sh4 = await C.getWorkLogPhotos('wl_b4');
    await st.mod.uploadString(st.mod.ref(st.s, 'photos/workLogPhotos/wl_b4/x1'), img1, 'data_url');
    await FS.setDoc(FS.doc(db, 'workLogPhotos', 'wl_b4'), { id: 'wl_b4', photos: ['sp:photos/workLogPhotos/wl_b4/x1'], updatedAt: 2 });
    await C.saveWorkLogPhotos('wl_b4', [sh4[0], img2]);
    await new Promise(r => setTimeout(r, 500));
    const b4 = (await raw('workLogPhotos', 'wl_b4')).photos;
    out.b4 = [b4[0], b4[1].startsWith('sp:'), await exists('photos/workLogPhotos/wl_b4/x1')];
    // ...and when a photo before it was removed here, the upload still takes its place (not the removed one).
    await st.mod.uploadString(st.mod.ref(st.s, 'photos/workLogPhotos/wl_b5/a1'), img1, 'data_url');
    await FS.setDoc(FS.doc(db, 'workLogPhotos', 'wl_b5'), { id: 'wl_b5', photos: ['sp:photos/workLogPhotos/wl_b5/a1', 'pending:elsewhere3'], updatedAt: 1 });
    const sh5 = await C.getWorkLogPhotos('wl_b5');
    await st.mod.uploadString(st.mod.ref(st.s, 'photos/workLogPhotos/wl_b5/t1'), img2, 'data_url');
    await FS.setDoc(FS.doc(db, 'workLogPhotos', 'wl_b5'), { id: 'wl_b5', photos: ['sp:photos/workLogPhotos/wl_b5/a1', 'sp:photos/workLogPhotos/wl_b5/t1'], updatedAt: 2 });
    await C.saveWorkLogPhotos('wl_b5', [sh5[1]]);                       // the first photo removed here
    out.b5 = (await raw('workLogPhotos', 'wl_b5')).photos;
    // The photo document still queued on this device when the upload goes: the upload
    // replaces the token behind that write (it used to be thrown away as an orphan).
    await FS.disableNetwork(db);
    offline(true);
    const pSave = C.saveWorkLogPhotos('wl_b6', [img1]);
    await new Promise(r => setTimeout(r, 400));
    offline(false);
    const pFlush = C.flushPhotoOutbox();
    await new Promise(r => setTimeout(r, 1500));
    await FS.enableNetwork(db);
    await pSave; await pFlush;
    const b6 = (await raw('workLogPhotos', 'wl_b6')).photos;
    out.b6 = [b6.length, b6[0].startsWith('sp:photos/workLogPhotos/wl_b6/'), b6[0].startsWith('sp:') && await exists(b6[0].slice(3)), await C.pendingPhotoUploads()];
    // 2.43.2: a placeholder is recognisable, and saved into another document it is no photo.
    out.b7 = [C.isWaitingPhoto(sh2[0]), C.isWaitingPhoto(img1)];
    await C.saveDamagePhoto('d7', sh2[0]);
    out.b7.push((await raw('damagePhotos', 'd7')).photo);
    // A waiting photo removed in the form stays removed when the other phone sends it meanwhile.
    for (const n of ['a', 'u1', 'u2']) await st.mod.uploadString(st.mod.ref(st.s, `photos/workLogPhotos/wl_b8/${n}`), img1, 'data_url');
    await FS.setDoc(FS.doc(db, 'workLogPhotos', 'wl_b8'), { id: 'wl_b8', photos: ['sp:photos/workLogPhotos/wl_b8/a', 'pending:other81', 'pending:other82'], updatedAt: 1 });
    const sh8 = await C.getWorkLogPhotos('wl_b8');
    await FS.setDoc(FS.doc(db, 'workLogPhotos', 'wl_b8'), { id: 'wl_b8', photos: ['sp:photos/workLogPhotos/wl_b8/a', 'sp:photos/workLogPhotos/wl_b8/u1', 'sp:photos/workLogPhotos/wl_b8/u2'], updatedAt: 2 });
    await C.saveWorkLogPhotos('wl_b8', [sh8[0], sh8[2]]);              // the first waiting photo removed here
    await new Promise(r => setTimeout(r, 500));
    out.b8 = [(await raw('workLogPhotos', 'wl_b8')).photos, await exists('photos/workLogPhotos/wl_b8/u1'), await exists('photos/workLogPhotos/wl_b8/a')];
    // A photo another device added while the form was open stays, and is not deleted.
    await st.mod.uploadString(st.mod.ref(st.s, 'photos/workLogPhotos/wl_b9/a'), img1, 'data_url');
    await st.mod.uploadString(st.mod.ref(st.s, 'photos/workLogPhotos/wl_b9/b'), img1, 'data_url');
    await FS.setDoc(FS.doc(db, 'workLogPhotos', 'wl_b9'), { id: 'wl_b9', photos: ['sp:photos/workLogPhotos/wl_b9/a'], updatedAt: 1 });
    const sh9 = await C.getWorkLogPhotos('wl_b9');
    await FS.setDoc(FS.doc(db, 'workLogPhotos', 'wl_b9'), { id: 'wl_b9', photos: ['sp:photos/workLogPhotos/wl_b9/a', 'sp:photos/workLogPhotos/wl_b9/b'], updatedAt: 2 });
    await C.saveWorkLogPhotos('wl_b9', [sh9[0], img2]);
    await new Promise(r => setTimeout(r, 500));
    const b9 = (await raw('workLogPhotos', 'wl_b9')).photos;
    out.b9 = [b9.length, b9[0], b9[1].startsWith('sp:photos/workLogPhotos/wl_b9/'), b9[2], await exists('photos/workLogPhotos/wl_b9/b')];
    // Firestore not reached and no write of this device queued: the outbox waits instead of writing from the cached copy.
    offline(true);
    await C.saveWorkLogPhotos('wl_b10', [img1]);
    offline(false);
    await FS.disableNetwork(db);
    const leftOff = await C.flushPhotoOutbox();
    const b10a = (await raw('workLogPhotos', 'wl_b10')).photos[0];
    await FS.enableNetwork(db);
    await new Promise(r => setTimeout(r, 300));
    const leftOn = await C.flushPhotoOutbox();
    const b10b = (await raw('workLogPhotos', 'wl_b10')).photos[0];
    out.b10 = [leftOff >= 1, b10a.startsWith('pending:'), leftOn, b10b.startsWith('sp:photos/workLogPhotos/wl_b10/')];
    // 2.43.0 (S4): a path planted in a photo document is never deleted or loaded.
    await st.mod.uploadString(st.mod.ref(st.s, 'learning/f9/book'), img1, 'data_url');
    await st.mod.uploadString(st.mod.ref(st.s, 'photos/inspectionPhotos/in9/i1'), img1, 'data_url');
    await FS.setDoc(FS.doc(db, 'damagePhotos', 'd9'), { id: 'd9', photo: 'sp:learning/f9/book', updatedAt: 1 });
    out.plantedLoad = await C.getDamagePhoto('d9');
    await C.deleteDamagePhoto('d9');
    await FS.setDoc(FS.doc(db, 'damagePhotos', 'd8'), { id: 'd8', photo: 'sp:photos/inspectionPhotos/in9/i1', updatedAt: 1 });
    out.copiedLoads = /^http:\/\/127\.0\.0\.1:9198\//.test(await C.getDamagePhoto('d8'));
    await C.saveDamagePhoto('d8', img2);                                 // replaced: the inspection's photo stays
    await C.deleteProjectFile('learning/f9/book', 'job_1');
    await new Promise(r => setTimeout(r, 500));
    out.plantedKept = [await exists('learning/f9/book'), await exists('photos/inspectionPhotos/in9/i1')];
    out.jobFileGuard = [await C.projectFileUrl('backups/2026-11-10/users.json.gz', 'job_1').then(() => 'opened', () => 'refused'),
                        await C.projectFileUrl('projectFiles/job_2/a.pdf', 'job_1').then(() => 'opened', () => 'refused'),
                        await C.learningFileUrl('backups/x.json').then(() => 'opened', () => 'refused')];
    // inspection map
    await C.saveInspectionPhotos('in1', { gps: img1, cam: '' });
    const i1 = await raw('inspectionPhotos', 'in1');
    out.map = [i1.photos.gps.startsWith('sp:'), i1.photos.cam];
    // legacy inline doc migrates when read
    await FS.setDoc(FS.doc(db, 'warehousePhotos', 'wh_old'), { id: 'wh_old', ownerId: 'dev1', photo: img2, updatedAt: 1 });
    const whUrl = await C.getWhPhoto('wh_old');
    out.legacyServed = whUrl.startsWith('data:image/png');
    await new Promise(r => setTimeout(r, 1500));
    const wh = await raw('warehousePhotos', 'wh_old');
    out.migrated = [String(wh.photo).startsWith('sp:'), wh.ownerId];
    // delete removes the object
    const ref1 = (await raw('damagePhotos', 'd1')).photo;
    await C.deleteDamagePhoto('d1');
    await new Promise(r => setTimeout(r, 500));
    out.deleted = [!(await raw('damagePhotos', 'd1')), !(await st.mod.getMetadata(st.mod.ref(st.s, ref1.slice(3))).catch(() => null))];
    // after photo
    await C.saveDamageAfterPhoto('d2', img1);
    const a = await raw('damagePhotos', 'd2__after');
    out.after = [a.side, a.photo.startsWith('sp:')];
    // 2.45.1 (S6): a report's or a check's photo is shown only from its own folder. One naming
    // another record's folder (writable after approval) or a link shows nothing.
    await st.mod.uploadString(st.mod.ref(st.s, 'photos/workLogPhotos/wl_decoy/p1'), img1, 'data_url');
    await st.mod.uploadString(st.mod.ref(st.s, 'photos/workLogPhotos/wl_s6/own'), img1, 'data_url');
    await FS.setDoc(FS.doc(db, 'workLogPhotos', 'wl_s6'), { id: 'wl_s6', photos: ['sp:photos/workLogPhotos/wl_decoy/p1', 'sp:photos/workLogPhotos/wl_s6/own', 'https://example.invalid/x.jpg'], updatedAt: 1 });
    const s6 = await C.getWorkLogPhotos('wl_s6');
    await st.mod.uploadString(st.mod.ref(st.s, 'photos/inspectionPhotos/in_s6/g'), img1, 'data_url');
    await FS.setDoc(FS.doc(db, 'inspectionPhotos', 'in_s6'), { id: 'in_s6', photos: { cam: 'sp:photos/inspectionPhotos/in9/i1', gps: 'sp:photos/inspectionPhotos/in_s6/g' }, updatedAt: 1 });
    const s6i = await C.getInspectionPhotos('in_s6');
    out.ownFolder = [s6[0], /^http:\/\/127\.0\.0\.1:9198\//.test(s6[1]), s6[2], s6i.cam, /^http:\/\/127\.0\.0\.1:9198\//.test(s6i.gps)];
    // 2.45.1: another document's photo saved into this one is copied into this one's folder
    // (the shared object was blanked when that document's photo changed).
    await C.saveDamagePhoto('d_s6', s6[1]);
    const ds6 = (await raw('damagePhotos', 'd_s6')).photo;
    out.foreignCopied = [String(ds6).startsWith('sp:photos/damagePhotos/d_s6/'), await exists(String(ds6).slice(3)), await exists('photos/workLogPhotos/wl_s6/own')];
    // Inline, only the keys asked for come down.
    const part = await C.getInspectionPhotos('in_s6', { inline: true, keys: ['gps'] });
    out.keysOnly = [Object.keys(part), String(part.gps).startsWith('data:')];
    return out;
  });
  const want = { stored: 'sp:photos/damagePhotos/d1/', objExists: true, url: true, inline: 'data:image/jpeg;base64,', kept: true, newOne: true,
    removedGone: true, lostPlain: 0, recovered: [2, true], writtenBack: ['sp:photos/workLogPhotos/wl_lost/k2', 'sp:photos/workLogPhotos/wl_lost/k3'], outboxPending: [3, true, 3], outboxFlushed: [0, 3, true, 0],
    b1Shown: [3, true], b1Kept: [3, true, true, true, 3], b1Flushed: [0, 3, true, 0], b2Placeholder: true, b2Kept: ['pending:elsewhere1', true],
    b3: [true, true, true], b4: ['sp:photos/workLogPhotos/wl_b4/x1', true, true], b5: ['sp:photos/workLogPhotos/wl_b5/t1'], b6: [1, true, true, 0],
    b7: [true, false, ''], b8: [['sp:photos/workLogPhotos/wl_b8/a', 'sp:photos/workLogPhotos/wl_b8/u2'], false, true],
    b9: [3, 'sp:photos/workLogPhotos/wl_b9/a', true, 'sp:photos/workLogPhotos/wl_b9/b', true], b10: [true, true, 0, true],
    plantedLoad: '', copiedLoads: true, plantedKept: [true, true], jobFileGuard: ['refused', 'refused', 'refused'], map: [true, ''], legacyServed: true, migrated: [true, 'dev1'], deleted: [true, true], after: ['after', true],
    ownFolder: ['', true, '', '', true], foreignCopied: [true, true, true], keysOnly: [['gps'], true] };
  let fail = 0;
  Object.keys(want).forEach(k => { const ok = JSON.stringify(R[k]) === JSON.stringify(want[k]); if (!ok) fail++;
    console.log(`${ok ? 'PASS' : 'FAIL'}  ${k}${ok ? '' : ' dapat=' + JSON.stringify(R[k])}`); });
  console.log(`\n${Object.keys(want).length - fail}/${Object.keys(want).length} lulus`);
  await b.close(); process.exit(fail ? 1 : 0);
  await b.close(); process.exit(0);
})();
