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
    return out;
  });
  const want = { stored: 'sp:photos/damagePhotos/d1/', objExists: true, url: true, inline: 'data:image/jpeg;base64,', kept: true, newOne: true,
    removedGone: true, map: [true, ''], legacyServed: true, migrated: [true, 'dev1'], deleted: [true, true], after: ['after', true] };
  let fail = 0;
  Object.keys(want).forEach(k => { const ok = JSON.stringify(R[k]) === JSON.stringify(want[k]); if (!ok) fail++;
    console.log(`${ok ? 'PASS' : 'FAIL'}  ${k}${ok ? '' : ' dapat=' + JSON.stringify(R[k])}`); });
  console.log(`\n${Object.keys(want).length - fail}/${Object.keys(want).length} lulus`);
  await b.close(); process.exit(fail ? 1 : 0);
  await b.close(); process.exit(0);
})();
