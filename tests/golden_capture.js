// Menulis tests/fixtures/tractor_golden.json. Dijalankan SEKALI, di commit
// sebelum pembagian grup unit, supaya pembandingnya adalah perilaku lama —
// bukan perilaku baru yang direkam oleh dirinya sendiri. Jangan jalankan ulang
// kecuali memang sengaja mengubah tampilan traktor.
const fs = require('fs');
const path = require('path');
const { launch, BASE_URL } = require('./_env');
const { CAPTURE, FIXED_TIME } = require('./_golden');

(async () => {
    const b = await launch();
    const ctx = await b.newContext({ viewport: { width: 1440, height: 900 } });
    const page = await ctx.newPage();
    await page.clock.setFixedTime(FIXED_TIME);
    const errors = [];
    page.on('pageerror', e => { if (!/Chart is not defined/.test(e.message)) errors.push(e.message); });
    await page.goto(BASE_URL + '/index.html', { waitUntil: 'load' });
    await page.waitForTimeout(900);
    const out = await page.evaluate(CAPTURE);
    const file = path.join(__dirname, 'fixtures', 'tractor_golden.json');
    fs.writeFileSync(file, JSON.stringify(out, null, 1));
    console.log('golden:', Object.keys(out).length, 'kunci →', file);
    console.log('PAGE ERRORS:', errors.length ? errors : 'none');
    await b.close();
})();
