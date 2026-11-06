// 2.11.6 — Pasang sebagai aplikasi: tombol/banner Install, panduan iPhone,
// shortcut peluncur (?action=…), dan manifest.
const fs = require('fs');
const path = require('path');
const { launch, BASE_URL } = require('./_env');

(async () => {
    const T = []; const t = (n, g, w) => T.push({ n, g, w, pass: JSON.stringify(g) === JSON.stringify(w) });
    const man = JSON.parse(fs.readFileSync(path.join(__dirname, '..', 'manifest.webmanifest'), 'utf8'));
    t('manifest: id, start_url, standalone', [man.id, man.start_url, man.display], ['./', './?source=pwa', 'standalone']);
    t('manifest: 4 shortcut', man.shortcuts.map(s => s.url), ['./?action=add-report', './?action=report-damage', './?action=inbox', './?action=units']);
    t('ikon shortcut ada', fs.existsSync(path.join(__dirname, '..', 'icons', 'icon-96.png')), true);

    const b = await launch();
    const errors = [];
    // Android: prompt install milik browser ditangkap dan dipicu dari tombol kita
    let ctx = await b.newContext({ viewport: { width: 390, height: 844 }, isMobile: true, hasTouch: true });
    let page = await ctx.newPage();
    page.on('pageerror', e => { if (!/Chart is not defined/.test(e.message)) errors.push(e.message); });
    await page.goto(BASE_URL + '/index.html?action=inbox', { waitUntil: 'load' });
    await page.waitForTimeout(900);
    const a = await page.evaluate(async () => {
        try { localStorage.removeItem('installBannerDismissedAt'); } catch (_) {}
        window.cloud = { isReady: true };
        currentUser = { uid: 'o', email: 'o@x.id' }; currentUserDoc = { uid: 'o', role: 'owner', status: 'active', email: 'o@x.id' };
        hideAuthGates(); applyRoleGating();
        const ev = new Event('beforeinstallprompt'); let prompted = 0;
        ev.prompt = () => { prompted++; return Promise.resolve(); }; ev.userChoice = Promise.resolve({ outcome: 'accepted' });
        window.dispatchEvent(ev);
        syncInstallUi();
        const before = [!document.getElementById('installBanner').hidden, !document.getElementById('installMenuItem').hidden];
        _cloudReadyFired = true; runLaunchAction();
        await new Promise(r => setTimeout(r, 700));
        const launched = [currentView, location.search];
        await installApp();
        dismissInstallBanner();
        return { before, launched, prompted, after: document.getElementById('installBanner').hidden };
    });
    t('Android: banner + menu Install muncul', a.before, [true, true]);
    t('shortcut ?action=inbox membuka Decision inbox, alamat dirapikan', a.launched, ['leader', '']);
    t('Install memicu prompt browser', a.prompted, 1);
    t('Not now menyembunyikan banner', a.after, true);
    await ctx.close();

    // iPhone: tanpa prompt browser → panduan langkah
    ctx = await b.newContext({ viewport: { width: 390, height: 844 }, isMobile: true, hasTouch: true,
        userAgent: 'Mozilla/5.0 (iPhone; CPU iPhone OS 17_5 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.5 Mobile/15E148 Safari/604.1' });
    page = await ctx.newPage();
    page.on('pageerror', e => { if (!/Chart is not defined/.test(e.message)) errors.push(e.message); });
    await page.goto(BASE_URL + '/index.html', { waitUntil: 'load' });
    await page.waitForTimeout(900);
    const i = await page.evaluate(async () => {
        try { localStorage.removeItem('installBannerDismissedAt'); } catch (_) {}
        window.cloud = { isReady: true };
        currentUser = { uid: 'o', email: 'o@x.id' }; currentUserDoc = { uid: 'o', role: 'owner', status: 'active', email: 'o@x.id' };
        hideAuthGates(); applyRoleGating(); syncInstallUi();
        const banner = !document.getElementById('installBanner').hidden;
        await installApp();
        const modal = document.getElementById('iosInstallModal').classList.contains('open');
        closeIosInstall();
        return { banner, modal, closed: !document.getElementById('iosInstallModal').classList.contains('open') };
    });
    t('iPhone: banner muncul, Install membuka panduan', [i.banner, i.modal, i.closed], [true, true, true]);
    await ctx.close();

    // Desktop: tidak ada banner
    ctx = await b.newContext({ viewport: { width: 1366, height: 800 } });
    page = await ctx.newPage();
    await page.goto(BASE_URL + '/index.html', { waitUntil: 'load' });
    await page.waitForTimeout(700);
    t('desktop tanpa prompt: tidak ada banner/menu', await page.evaluate(() => {
        window.cloud = { isReady: true }; currentUser = { uid: 'o' }; currentUserDoc = { role: 'owner', status: 'active' };
        hideAuthGates(); syncInstallUi();
        return [document.getElementById('installBanner').hidden, document.getElementById('installMenuItem').hidden];
    }), [true, true]);
    await b.close();

    let pass = 0;
    for (const x of T) {
        if (x.pass) pass++;
        console.log(`${x.pass ? 'PASS' : 'FAIL'}  ${x.n}`);
        if (!x.pass) console.log(`        dapat=${JSON.stringify(x.g)} harap=${JSON.stringify(x.w)}`);
    }
    console.log(`\n${pass}/${T.length} lulus`);
    console.log('PAGE ERRORS:', errors.length ? errors : 'none');
    process.exit(pass === T.length && !errors.length ? 0 : 1);
})();
