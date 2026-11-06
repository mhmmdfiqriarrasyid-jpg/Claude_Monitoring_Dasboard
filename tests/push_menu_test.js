// 2.18.0 — Menu akun: "Turn on phone notifications" (push per perangkat).
const { launch, BASE_URL } = require('./_env');
(async () => {
    const b = await launch();
    const page = await b.newPage({ viewport: { width: 1280, height: 900 } });
    const errors = [];
    page.on('pageerror', e => { if (!/Chart is not defined/.test(e.message)) errors.push(e.message); });
    await page.goto(BASE_URL + '/index.html', { waitUntil: 'load' });
    await page.waitForTimeout(800);
    const R = await page.evaluate(async () => {
        const out = {}; const toasts = []; window.showToast = m => toasts.push(String(m));
        let available = false, tokenHere = '', perm = 'default', enableFails = null;
        const calls = [];
        window.cloud = { isReady: true,
            pushAvailable: () => Promise.resolve(available), pushPermission: () => perm,
            pushTokenHere: () => Promise.resolve(tokenHere),
            enablePush: () => { calls.push('on'); if (enableFails) return Promise.reject(enableFails); perm = 'granted'; tokenHere = 't1'; return Promise.resolve('t1'); },
            disablePush: () => { calls.push('off'); tokenHere = ''; return Promise.resolve(); } };
        const item = document.getElementById('pushMenuItem'), label = () => document.getElementById('pushMenuLabel').textContent;
        await syncPushMenu();
        out.hiddenWhenUnavailable = item.hidden;
        available = true; await syncPushMenu();
        out.offState = [item.hidden, label()];
        await togglePush();
        out.turnedOn = [calls.slice(), label(), toasts.pop()];
        await togglePush();
        out.turnedOff = [calls.slice(), label(), toasts.pop()];
        enableFails = Object.assign(new Error('permission denied'), { code: 'push/denied' });
        await togglePush();
        out.blocked = toasts.pop();
        out.actionDamage = typeof LAUNCH_ACTIONS.damage;
        return out;
    });
    const T = [];
    const t = (n, g, w) => T.push({ n, g, w, pass: JSON.stringify(g) === JSON.stringify(w) });
    t('tidak didukung / belum dikonfigurasi: menu tersembunyi', R.hiddenWhenUnavailable, true);
    t('didukung: menu tampil, keadaan mati', R.offState, [false, 'Turn on phone notifications']);
    t('menyalakan', R.turnedOn, [['on'], 'Phone notifications: On', 'Notifications are on for this device']);
    t('mematikan', R.turnedOff, [['on', 'off'], 'Turn on phone notifications', 'Notifications turned off on this device']);
    t('izin diblokir browser: diberi tahu caranya', R.blocked, 'Notifications are blocked — allow them for this site in the browser settings');
    t('tautan notifikasi ?action=damage dikenal', R.actionDamage, 'function');
    let pass = 0;
    T.forEach(x => { if (x.pass) pass++; console.log(`${x.pass ? 'PASS' : 'FAIL'}  ${x.n}${x.pass ? '' : `\n        dapat=${JSON.stringify(x.g)} harap=${JSON.stringify(x.w)}`}`); });
    console.log(`\n${pass}/${T.length} lulus`);
    console.log('PAGE ERRORS:', errors.length ? errors : 'none');
    await b.close();
    process.exit(pass === T.length && !errors.length ? 0 : 1);
})();
