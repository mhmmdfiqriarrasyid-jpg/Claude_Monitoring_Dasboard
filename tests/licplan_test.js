// License Purchase Plan: syarat lisensi per JENIS implement (tanpa brand),
// dicek ke setiap unit Agricultural pada tanggal rencana. SF-RTK & G5 Advance
// dibeli; SF-1 & G5 Basic gratis; G5 Advance mencakup G5 Basic. Yang harus
// dibeli = kebutuhan − sisa stok.
const { launch, BASE_URL } = require('./_env');

(async () => {
    const b = await launch();
    const page = await b.newPage({ viewport: { width: 1440, height: 900 } });
    await page.clock.setFixedTime('2026-10-06T09:00:00');
    const errors = [];
    page.on('pageerror', e => { if (!/Chart is not defined/.test(e.message)) errors.push(e.message); });
    await page.goto(BASE_URL + '/index.html', { waitUntil: 'load' });
    await page.waitForTimeout(1000);

    await page.addScriptTag({ content: `(async () => { try {
        const T = []; const t = (n, g, w) => T.push({ n, g, w, pass: JSON.stringify(g) === JSON.stringify(w) });
        const tick = ms => new Promise(r => setTimeout(r, ms || 20));
        const saved = [], deleted = [], toasts = [];
        let fail = false;
        window.showToast = m => toasts.push(String(m));
        window.cloud = { isReady: true, addHistoryEvents: () => Promise.resolve(),
            saveLicenseRequirement: r => { saved.push([r.id, r.gps, r.display]); return fail ? Promise.reject(Object.assign(new Error('x'), { code: 'permission-denied' })) : Promise.resolve(); },
            deleteLicenseRequirement: id => { deleted.push(id); return Promise.resolve(); } };
        const as = doc => { currentUser = { uid: 'u', email: 'u@x.id' }; currentUserDoc = { status: 'active', ...doc }; applyRoleGating(); };
        as({ role: 'owner' });
        hideAuthGates();
        const TR = (i, implement, extra) => ({ id: 't' + i, name: 'TR' + i, sn: 'ST' + i, status: 'Good', site: 'PT. GPA', implement, ...extra });
        globalData = [
            TR(1, 'Bed Ripper — Gessner', { gpsLicense: 'SF-RTK', gpsLicenseEndDate: '2027-06-01', licenseDisplay: 'G5 Advance', displayLicenseEndDate: '2027-06-01' }),
            TR(2, 'Bed Ripper — ISJ', { gpsLicense: 'SF-1', licenseDisplay: 'G5 Basic' }),
            TR(3, 'Bed Ripper — Gessner', { gpsLicense: 'SF-RTK', gpsLicenseEndDate: '2026-11-15', licenseDisplay: '' }),
            TR(4, 'Lime Spreader — Agri Spreader', { gpsLicense: '', licenseDisplay: 'G5 Advance' }),
            TR(5, 'Lime Spreader — Agri Spreader', { gpsLicense: 'SF-RTK', licenseDisplay: '' }),
            TR(6, 'Haulout Bin — Antoniosi', {}),
            TR(7, '', {}),
            { id: 'h1', name: 'EX1', sn: 'SH1', unitGroup: 'heavy', status: 'Good', workTool: 'Bucket' }];
        globalImplements = [{ id: 'i1', profileName: 'X', equipmentType: 'Zonal Ripper', brand: 'Gessner' }];
        globalLicenseStock = [
            { id: 'l1', txnType: 'IN', licenseType: 'SF-RTK', qty: 2, date: '2026-09-01' },
            { id: 'l2', txnType: 'IN', licenseType: 'G5 Advance', qty: 5, date: '2026-09-01' },
            { id: 'l3', txnType: 'OUT', licenseType: 'G5 Advance', qty: 1, date: '2026-09-02' }];
        saveToStorage(globalData); saveImplements(); saveLicenseStockLocal();
        licenseRequirements = [];

        navigateTo('licenseStock');
        switchLicenseTab('plan');
        const body = () => document.getElementById('lpBody').textContent.replace(/\\s+/g, ' ');
        t('tab Purchase Plan terbuka', [document.getElementById('licPlanPanel').style.display, document.getElementById('licStockPanel').style.display], ['', 'none']);
        t('tanpa syarat: petunjuk mengisi syarat', /No requirements yet/.test(body()), true);
        t('tanggal rencana default akhir tahun', document.getElementById('lpUntil').value, '2026-12-31');

        // =============== modal syarat ===============
        openLicenseRequirements();
        const types = [...document.querySelectorAll('#lrList .lr-row:not(.lr-row--head) .lr-row__type strong')].map(x => x.textContent);
        t('jenis implement saja (tanpa brand), terbanyak dulu, termasuk dari daftar Implements', types, ['Bed Ripper', 'Lime Spreader', 'Haulout Bin', 'Zonal Ripper']);
        setLicenseRequirement('Bed Ripper', 'gps', 'SF-RTK');
        setLicenseRequirement('Bed Ripper', 'display', 'G5 Advance');
        setLicenseRequirement('Lime Spreader', 'gps', 'SF-RTK');
        setLicenseRequirement('Lime Spreader', 'display', 'G5 Basic');
        t('tersimpan per jenis', saved.slice(-1)[0], ['lime-spreader', 'SF-RTK', 'G5 Basic']);
        t('pilihan di modal mengikuti', [...document.querySelectorAll('#lrList select')].slice(0, 2).map(s => s.value), ['SF-RTK', 'G5 Advance']);
        closeLicenseRequirements();

        // =============== perhitungan ===============
        const plan = licensePlanCompute('2026-12-31');
        const st = id => { const r = plan.rows.find(x => x.u.id === id); return r ? [r.gps.state, r.display.state] : null; };
        t('lengkap & berlaku: sesuai', st('t1'), ['ok', 'ok']);
        t('SF-1/G5 Basic untuk kebutuhan SF-RTK/G5 Advance: upgrade', st('t2'), ['upgrade', 'upgrade']);
        t('SF-RTK habis sebelum tanggal rencana: perpanjang', st('t3'), ['renew', 'missing']);
        t('G5 Advance mencakup G5 Basic', st('t4'), ['missing', 'ok']);
        t('G5 Basic kosong: gratis, bukan pembelian', st('t5'), ['ok', 'free']);
        t('jenis tanpa syarat & unit tanpa implement & alat berat tidak masuk', plan.rows.map(r => r.u.id).sort(), ['t1', 't2', 't3', 't4', 't5']);
        t('jenis tanpa syarat dicatat', [...plan.noReq.entries()], [['Haulout Bin', 1]]);
        t('kebutuhan vs stok vs beli', plan.buy, [{ type: 'SF-RTK', need: 3, stock: 2, toBuy: 1 }, { type: 'G5 Advance', need: 2, stock: 4, toBuy: 0 }]);
        t('tanggal rencana lebih awal: SF-RTK TR3 belum perlu diperpanjang', licensePlanCompute('2026-11-01').buy[0].need, 2);

        // =============== tampilan ===============
        renderLicensePlan();
        t('kartu SF-RTK: 1 dibeli', /SF-RTK 1 to buy 3 needed · 2 in stock/.test(body()), true);
        t('kartu kombinasi', [/SF-RTK \\+ G5 Advance 3 unit\\(s\\) 2 with a gap/.test(body()), /SF-RTK \\+ G5 Basic 2 unit\\(s\\) 2 with a gap/.test(body())], [true, true]);
        t('catatan jenis tanpa syarat', /1 implement type\\(s\\) in use have no requirement yet \\(1 unit\\(s\\)\\): Haulout Bin/.test(body()), true);
        t('daftar unit: hanya yang ada celah', document.querySelectorAll('#lpUnitTable tbody tr').length, 4);
        t('tindakan per unit', [...document.querySelectorAll('#lpUnitTable tbody tr')].map(tr => tr.querySelector('[data-label="Unit"]').textContent + ': ' + tr.querySelector('[data-label="To Do"]').textContent.trim().replace(/\\s+/g, ' ')),
          ['TR2: Buy SF-RTK (has SF-1) Buy G5 Advance (has G5 Basic)', 'TR3: Renew SF-RTK (ends 2026-11-15) Buy G5 Advance', 'TR4: Buy SF-RTK', 'TR5: Set G5 Basic (free)']);
        document.getElementById('lpGapsOnly').checked = false; renderLicensePlan();
        t('semua unit bila filter dimatikan', document.querySelectorAll('#lpUnitTable tbody tr').length, 5);
        t('badge tab = jumlah yang harus dibeli', document.getElementById('licPlanBadge').textContent, '1');

        // distribusi stok mengurangi rencana
        globalData.find(u => u.id === 't4').gpsLicense = 'SF-RTK';
        refreshLicensePlan();
        t('lisensi dipasang ke unit → rencana berkurang', licensePlanCompute('2026-12-31').buy[0].need, 2);

        // hapus syarat bila keduanya kosong
        setLicenseRequirement('Lime Spreader', 'gps', '');
        setLicenseRequirement('Lime Spreader', 'display', '');
        t('syarat kosong dihapus', [deleted.includes('lime-spreader'), licenseRequirements.some(r => r.id === 'lime-spreader')], [true, false]);

        // gagal simpan
        fail = true;
        setLicenseRequirement('Zonal Ripper', 'gps', 'SF-RTK');
        await tick(); await tick();
        t('ditolak server: dikembalikan', licenseRequirements.some(r => r.id === 'zonal-ripper'), false);
        fail = false;

        // =============== hak akses ===============
        as({ role: 'staff', access: { licenseStock: 'view' } });
        openLicenseRequirements();
        t('akses lihat: pilihan dikunci', [...document.querySelectorAll('#lrList select')].every(s => s.disabled), true);
        const n = saved.length;
        setLicenseRequirement('Bed Ripper', 'gps', 'SF-1');
        t('akses lihat: tidak bisa mengubah', saved.length, n);
        closeLicenseRequirements();

        // XSS
        as({ role: 'owner' });
        globalData.push(TR(9, '<img src=x onerror=window.__pwn=1> — B'));
        setLicenseRequirement('<img src=x onerror=window.__pwn=1>', 'gps', 'SF-RTK');
        renderLicensePlan(); openLicenseRequirements();
        await tick();
        t('nama jenis berisi markup tidak dirender', [document.querySelectorAll('#lpBody img, #lrList img').length, window.__pwn || 0], [0, 0]);

        window.__T = T;
    } catch (e) { window.__T = [{ n: 'EXCEPTION ' + e.message + ' ' + e.stack, pass: false }]; } })();` });

    await page.waitForFunction(() => window.__T, null, { timeout: 60000 });
    const T = await page.evaluate(() => window.__T);
    let pass = 0;
    T.forEach(x => { if (x.pass) pass++; else console.log('FAIL', x.n, '\n  dapat :', JSON.stringify(x.g), '\n  harap :', JSON.stringify(x.w)); });
    if (errors.length) console.log('PAGE ERRORS:', errors);
    console.log(`${pass}/${T.length} lulus`);
    await b.close();
    process.exit(pass === T.length && !errors.length ? 0 : 1);
})();
