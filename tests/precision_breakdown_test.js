// 2.33.0 — kerusakan alat precision (Device Precision: GPS, Display, Camera AI,
// Switch Limiter, Processor, antena …) tidak membuat STATUS unit Breakdown;
// hanya komponennya (bila ada kolomnya). Mekanis tetap Breakdown. Data lama
// diperbaiki lewat banner di halaman Damage.
const { launch, BASE_URL } = require('./_env');

(async () => {
    const b = await launch();
    const page = await b.newPage({ viewport: { width: 1440, height: 900 } });
    const errors = [];
    page.on('pageerror', e => { if (!/Chart is not defined/.test(e.message)) errors.push(e.message); });
    await page.goto(BASE_URL + '/index.html', { waitUntil: 'load' });
    await page.waitForTimeout(1000);

    await page.addScriptTag({ content: `(async () => { try {
        const T = []; const t = (n, g, w) => T.push({ n, g, w, pass: JSON.stringify(g) === JSON.stringify(w) });
        const tick = ms => new Promise(r => setTimeout(r, ms || 20));
        const toasts = []; window.showToast = m => toasts.push(String(m));
        let confirmAnswer = true, asked = [];
        window.confirm = m => { asked.push(String(m)); return confirmAnswer; };
        const ok = () => Promise.resolve();
        const partials = [], whole = [], recs = [];
        let refuseUnits = null;
        window.cloud = { isReady: true,
            saveDamage: r => { recs.push(JSON.parse(JSON.stringify(r))); return ok(); },
            saveDamages: ok, deleteDamage: ok,
            updateUnitsFields: items => { partials.push(JSON.parse(JSON.stringify(items))); return refuseUnits ? Promise.reject(refuseUnits) : ok(); },
            saveUnits: units => { whole.push(JSON.parse(JSON.stringify(units))); return refuseUnits ? Promise.reject(refuseUnits) : ok(); },
            deleteUnits: ok, getAllUnits: () => Promise.resolve(JSON.parse(JSON.stringify(globalData))),
            addHistoryEvents: ok, subscribeUsers: () => () => {},
            saveDamagePhoto: ok, deleteDamagePhoto: ok, saveDamageAfterPhoto: ok, deleteDamageAfterPhoto: ok,
            getDamagePhoto: () => Promise.resolve(''), getDamageAfterPhoto: () => Promise.resolve('') };
        const asOwner = () => { currentUser = { uid: 'uO', email: 'o@x.id' }; currentUserDoc = { role: 'owner', status: 'active' }; applyRoleGating(); };
        asOwner(); hideAuthGates();
        const today = toISODate();
        const daysAgo = n => toISODate(new Date(Date.now() - n * 86400000));
        const H = (id, x) => Object.assign({ id, name: 'EX-' + id, sn: 'SN' + id, model: 'PC200', unitGroup: 'heavy', brand: 'Komatsu',
            status: 'Good', cameraAi: 'Good', telematicBox: 'Good', switchLimiter: 'Good', rotaryLamp: 'Good',
            site: 'PT. GPA', breakdownReason: '', downtimeHistory: [] }, x || {});
        globalData = [
            H('01'), H('02', { status: 'Breakdown', breakdownReason: 'Engine overhaul', breakdownStartedAt: Date.now() - 86400000 }),
            H('03'), H('04'), H('05'), H('06'),
            { id: 'T1', name: 'TR-01', sn: 'SNT1', model: '6110B', status: 'Good', display: 'Good', gps: 'Good', steering: 'Good', jdlink: 'Good', site: 'PT. GPA' }
        ];
        globalDamages = [];
        const U = id => globalData.find(u => u.id === id);
        const D = id => globalDamages.find(d => d.id === id);
        const last = () => globalDamages[globalDamages.length - 1];
        const add = (unitId, type, comp, desc, opts) => {
            opts = opts || {};
            showAddDamageForm();
            document.getElementById('dmgUnit').value = damageUnitLabel(U(unitId));
            onDamageUnitChanged();
            document.getElementById('dmgType').value = type;
            const sel = document.getElementById('dmgComponent');
            if (comp && ![...sel.options].some(o => o.value === comp)) sel.insertAdjacentHTML('beforeend', '<option value="' + comp + '">' + comp + '</option>');
            sel.value = comp || '';
            document.getElementById('dmgDescription').value = desc || '';
            document.getElementById('dmgDate').value = opts.date || today;
            document.getElementById('dmgSetBreakdown').checked = opts.untick ? false : true;
            if (opts.repaired) {
                document.getElementById('dmgAlreadyRepaired').checked = true; onDamageAlreadyRepairedChange();
                document.getElementById('dmgRepairedDate').value = opts.repaired;
            }
            saveDamage({ preventDefault() {} });
            return last();
        };
        const keysOk = list => list.every(p => Object.keys(p).every(k => k === 'id' || DAMAGE_LINK_FIELDS.includes(k)));

        // =============== 2.33.0: alat precision rusak tidak membuat unit Breakdown ===============
        globalData.push({ id: 'T2', name: 'GMTR118G', sn: 'SNT2', model: '6110B', status: 'Good', display: 'Good', gps: 'Good', steering: 'Good', jdlink: 'Good', site: 'PT. MNM' });
        const g1 = add('T1', 'Device Precision', 'GPS', 'No RTK fix');
        t('traktor: Device Precision / GPS → komponen GPS Breakdown, unit tetap Good', [U('T1').status, U('T1').gps, g1.drove], ['Good', 'Breakdown', 'gps']);
        const p1 = add('T2', 'Device Precision', 'Processor', 'Processor di swap ke TR040');
        t('traktor: Device Precision / Processor (tanpa kolom) → unit tetap Good, tidak ada yang berubah', [U('T2').status, U('T2').gps, U('T2').display, p1.drove], ['Good', 'Good', 'Good', '']);
        const c1 = add('03', 'Device Precision', 'Camera AI', 'Camera AI offline');
        t('alat berat: Camera AI → komponen Camera AI Breakdown, unit tetap Good', [U('03').status, U('03').cameraAi, c1.drove], ['Good', 'Breakdown', 'cameraAi']);
        const s1 = add('04', 'Device Precision', 'switch limiter', 'Switch limiter putus');
        t('alat berat: switch limiter (huruf kecil) → komponennya, unit tetap Good', [U('04').status, U('04').switchLimiter, s1.drove], ['Good', 'Breakdown', 'switchLimiter']);
        const x1 = add('05', 'Device Precision', 'Antenna', 'Antena patah');
        t('alat berat: bagian device tanpa kolom → unit tetap Good', [U('05').status, x1.drove], ['Good', '']);
        globalData.push({ id: 'T9', name: 'TR-09', sn: 'SNT9', model: '6110B', status: 'Good', display: 'Good', gps: 'Good', steering: 'Good', jdlink: 'Good', site: 'PT. GPA' });
        const sw = add('T9', 'Software', 'Display', 'Display perlu update');
        t('2.35.0: Software seperti Device Precision — hanya komponennya', [U('T9').status, U('T9').display, sw.drove], ['Good', 'Breakdown', 'display']);
        resolveDamage(sw.id, { confirmed: true, quiet: true });
        const m1 = add('06', 'Mekanis', 'Transmission', 'Transmisi rusak');
        t('Mechanical tetap membuat unit Breakdown', [U('06').status, m1.drove], ['Breakdown', 'status']);
        // Needs attention lists the device faults (unit running).
        renderDashAttention(globalData, Date.now());
        const rows = [...document.querySelectorAll('#dashAttention .dash-attn__row, .dash-attn__row')].map(r => [r.querySelector('.dash-unit').textContent, r.querySelector('.dash-attn__status').textContent.trim()]);
        t('Needs attention: Breakdown dulu, lalu Device (unit jalan)', [rows.filter(r => r[1] === 'Device').map(r => r[0]).sort(), rows.find(r => r[0] === 'EX-06')[1]],
          [['EX-03', 'EX-04', 'EX-05', 'GMTR118G', 'TR-01'], 'Breakdown']);
        // Resolving the GPS fault brings the GPS back.
        resolveDamage(g1.id, { confirmed: true, quiet: true });
        t('GPS selesai diperbaiki → GPS Good', U('T1').gps, 'Good');

        // =============== data lama: unit Breakdown karena device ===============
        navigateTo('damage');
        globalData.push({ id: 'T3', name: 'MMTR040M', sn: 'SNT3', model: '6110B', status: 'Breakdown', display: 'Good', gps: 'Good', steering: 'Good', jdlink: 'Good', site: 'PT. MNM',
            breakdownReason: 'Device Precision / Processor: kelistrikan', breakdownDamageId: 'old1', breakdownStartedAt: Date.now() - 5 * 86400000, downtimeHistory: [] });
        globalDamages.push({ id: 'old1', date: daysAgo(5), unitId: 'T3', unitName: 'MMTR040M', sn: 'SNT3', damageType: 'Device Precision', component: 'Processor', description: 'kelistrikan', resolved: false, drove: 'status' });
        globalData.push(H('07', { status: 'Breakdown', breakdownReason: 'Device Precision / Display: layar mati', breakdownStartedAt: Date.now() - 3 * 86400000 }));
        globalDamages.push({ id: 'old2', date: daysAgo(3), unitId: '07', unitName: 'EX-07', sn: 'SN07', damageType: 'Device Precision', component: 'Display', description: 'layar mati', resolved: false });
        globalDamages.push({ id: 'old3', date: daysAgo(2), unitId: '07', unitName: 'EX-07', sn: 'SN07', damageType: 'Mekanis', component: 'Engine', description: 'mesin', resolved: false, drove: 'status' });
        renderDamageTable();
        const banner = document.getElementById('dmgPrecisionFix');
        t('banner: 2 unit Breakdown hanya karena device', [banner.hidden, /2 units are in Breakdown/.test(banner.textContent)], [false, true]);
        fixPrecisionBreakdowns(); await tick();
        t('diperbaiki: MMTR040M jalan lagi, catatan device tetap terbuka tanpa menahan', [U('T3').status, D('old1').resolved, D('old1').drove, (U('T3').downtimeHistory || []).length], ['Good', false, '', 1]);
        t('EX-07 tetap Breakdown karena kerusakan mesin, alasannya pindah ke mesin', [U('07').status, U('07').breakdownDamageId, U('07').breakdownReason, D('old2').drove],
          ['Breakdown', 'old3', 'Mekanis / Engine: mesin', '']);
        renderDamageTable();
        t('banner hilang setelah diperbaiki', banner.hidden, true);
        // A legacy device record closed later: gives the status back.
        globalData.push({ id: 'T4', name: 'GGTR117G', sn: 'SNT4', model: '6110B', status: 'Breakdown', display: 'Good', gps: 'Good', steering: 'Good', jdlink: 'Good', site: 'PT. GPA',
            breakdownReason: 'Device Precision / Processor: x', breakdownStartedAt: Date.now() - 86400000, downtimeHistory: [] });
        globalDamages.push({ id: 'old4', date: daysAgo(1), unitId: 'T4', unitName: 'GGTR117G', sn: 'SNT4', damageType: 'Device Precision', component: 'Processor', description: 'x', resolved: false });
        resolveDamage('old4', { confirmed: true, quiet: true });
        t('catatan device lama ditutup → unit kembali Good', U('T4').status, 'Good');
        window.__T = T;
    } catch (e) { window.__T = [{ n: 'EXCEPTION ' + e.message + ' ' + e.stack, pass: false }]; } })();` });

    await page.waitForFunction(() => window.__T, null, { timeout: 90000 });
    const T = await page.evaluate(() => window.__T);
    let pass = 0;
    T.forEach(x => { if (x.pass) pass++; else console.log('FAIL', x.n, '\n  dapat :', JSON.stringify(x.g), '\n  harap :', JSON.stringify(x.w)); });
    if (errors.length) console.log('PAGE ERRORS:', errors);
    console.log(`${pass}/${T.length} lulus`);
    await b.close();
    process.exit(pass === T.length && !errors.length ? 0 : 1);
})();
