// 2.11.1 — Keamanan: library dan font dari situs sendiri (bukan CDN pihak
// ketiga), Content-Security-Policy terpasang, dan halaman berjalan tanpa
// satu pun pelanggaran CSP.
const fs = require('fs');
const path = require('path');
const { launch, BASE_URL } = require('./_env');

(async () => {
    const T = []; const t = (n, g, w) => T.push({ n, g, w, pass: JSON.stringify(g) === JSON.stringify(w) });
    const root = path.join(__dirname, '..');
    const html = fs.readFileSync(path.join(root, 'index.html'), 'utf8');
    const js = fs.readFileSync(path.join(root, 'script.js'), 'utf8');
    const ext = [...html.matchAll(/<(?:script|link)[^>]+(?:src|href)="(https?:[^"]+)"/g)].map(m => m[1]);
    t('index.html: tidak ada script/stylesheet dari luar', ext, []);
    t('script.js: tidak memuat dari CDN', /cdnjs\.cloudflare\.com|cdn\.jsdelivr\.net|unpkg\.com|fonts\.googleapis\.com/.test(js), false);
    const csp = (html.match(/http-equiv="Content-Security-Policy" content="([^"]+)"/) || [])[1] || '';
    t('CSP terpasang', !!csp, true);
    t('CSP: object-src none, base-uri self', /object-src 'none'/.test(csp) && /base-uri 'self'/.test(csp), true);
    t('CSP: script hanya dari situs ini + Firebase/reCAPTCHA', (csp.match(/script-src ([^;]+)/) || [])[1], "'self' 'unsafe-inline' https://www.gstatic.com https://www.google.com");
    ['vendor/chart.umd.js', 'vendor/papaparse.min.js', 'vendor/xlsx.full.min.js', 'vendor/fonts.css',
     'vendor/fontawesome/css/all.min.css', 'vendor/fontawesome/webfonts/fa-solid-900.woff2']
        .forEach(f => t(`ada ${f}`, fs.existsSync(path.join(root, f)), true));

    const b = await launch();
    const page = await b.newPage({ viewport: { width: 1280, height: 900 } });
    await page.addInitScript(() => { window.__csp = []; document.addEventListener('securitypolicyviolation', e => window.__csp.push(e.violatedDirective + ' ' + e.blockedURI)); });
    const errors = [];
    page.on('pageerror', e => errors.push(e.message));
    await page.goto(BASE_URL + '/index.html', { waitUntil: 'load' });
    await page.waitForTimeout(1200);
    const r = await page.evaluate(async () => {
        const X = await loadSheetJs().catch(e => null);
        return { chart: typeof Chart, papa: typeof Papa, emailjs: typeof emailjs, xlsx: !!(X && X.read), csp: window.__csp };
    });
    t('library lokal termuat (Chart, Papa, SheetJS); EmailJS tidak lagi dimuat (2.14.0)', [r.chart, r.papa, r.emailjs, r.xlsx], ['function', 'object', 'undefined', true]);
    t('tanpa pelanggaran CSP', r.csp, []);
    await b.close();

    let pass = 0;
    for (const x of T) {
        if (x.pass) pass++;
        console.log(`${x.pass ? 'PASS' : 'FAIL'}  ${x.n}`);
        if (!x.pass) console.log(`        dapat=${JSON.stringify(x.g)} harap=${JSON.stringify(x.w)}`);
    }
    console.log(`\n${pass}/${T.length} lulus`);
    console.log('PAGE ERRORS:', errors.length ? errors : 'none');
    process.exit(pass === T.length ? 0 : 1);
})();
