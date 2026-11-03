// 2.18.0 — service-worker.js: pesan push ditampilkan sebagai notifikasi, dan
// mengetuknya membuka / memfokuskan dashboard di tautannya.
const fs = require('fs'); const path = require('path'); const vm = require('vm');
const src = fs.readFileSync(path.join(__dirname, '..', 'service-worker.js'), 'utf8');
const handlers = {}; const shown = []; const opened = []; const navigated = [];
let windows = [];
const self = {
    addEventListener: (t, fn) => { handlers[t] = fn; },
    registration: { scope: 'https://ot.techorchestrator.cloud/', showNotification: (title, opt) => { shown.push({ title, ...opt }); return Promise.resolve(); } },
    clients: { matchAll: () => Promise.resolve(windows), openWindow: u => { opened.push(u); return Promise.resolve(); }, claim: () => Promise.resolve() },
    skipWaiting: () => Promise.resolve(), location: { origin: 'https://ot.techorchestrator.cloud' }
};
vm.runInNewContext(src, { self, caches: {}, fetch: () => Promise.reject(), URL, console, Promise, Response: function () {} });
const T = []; const t = (n, g, w) => T.push({ n, g, w, pass: JSON.stringify(g) === JSON.stringify(w) });
(async () => {
    let waited;
    handlers.push({ data: { json: () => ({ data: { title: 'Breakdown: BD025G', body: 'Camera AI', url: './?action=damage', tag: 'dmg-1' } }) }, waitUntil: p => { waited = p; } });
    await waited;
    t('push → notifikasi', [shown[0].title, shown[0].body, shown[0].tag, shown[0].data.url], ['Breakdown: BD025G', 'Camera AI', 'dmg-1', './?action=damage']);
    handlers.push({ data: { json: () => { throw new Error('bad'); } }, waitUntil: p => { waited = p; } });
    await waited;
    t('isi rusak tetap ditampilkan dengan judul bawaan', shown[1].title, 'OT Dashboard');
    let closed = 0;
    handlers.notificationclick({ notification: { close: () => closed++, data: { url: './?action=inbox' } }, waitUntil: p => { waited = p; } });
    await waited;
    t('ketuk, belum ada jendela: buka tautan', [closed, opened], [1, ['https://ot.techorchestrator.cloud/?action=inbox']]);
    let focused = 0;
    windows = [{ url: 'https://ot.techorchestrator.cloud/', navigate: u => { navigated.push(u); return Promise.resolve(null); }, focus: () => { focused++; return Promise.resolve(); } }];
    handlers.notificationclick({ notification: { close: () => {}, data: { url: './?action=damage' } }, waitUntil: p => { waited = p; } });
    await waited;
    t('ketuk, dashboard sudah terbuka: pindah ke tautan dan fokus', [navigated, focused], [['https://ot.techorchestrator.cloud/?action=damage'], 1]);
    let pass = 0;
    T.forEach(x => { if (x.pass) pass++; console.log(`${x.pass ? 'PASS' : 'FAIL'}  ${x.n}${x.pass ? '' : `\n        dapat=${JSON.stringify(x.g)} harap=${JSON.stringify(x.w)}`}`); });
    console.log(`\n${pass}/${T.length} lulus`);
    process.exit(pass === T.length ? 0 : 1);
})();
