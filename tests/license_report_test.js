// 2.14.0 — Email lisensi dari server (Cloud Functions): laporan di server
// (functions/licenseReport.js) harus sama dengan laporan dashboard
// (_buildAlertList / _lowStockList) untuk data yang sama; layar pengaturan
// email (penerima + harian, hanya owner) dan tombol Email Report memanggil
// fungsi server tanpa EmailJS.
const { launch, BASE_URL } = require('./_env');
const server = require('../functions/licenseReport');

(async () => {
    const day = n => { const d = new Date(); d.setDate(d.getDate() + n); const p = x => String(x).padStart(2, '0'); return `${d.getFullYear()}-${p(d.getMonth() + 1)}-${p(d.getDate())}`; };
    const units = [
        { id: 'u1', name: 'JD-01', model: '6110B', sn: 'S1', site: 'Timika', gpsLicense: 'SF-RTK', gpsLicenseEndDate: day(5), licenseDisplay: 'G5 Advance', displayLicenseEndDate: day(20) },
        { id: 'u2', name: 'JD-02', model: '5075E', sn: 'S2', site: 'Timika', gpsLicense: 'sf-rtk', gpsLicenseEndDate: day(-3) },
        { id: 'u3', name: 'JD-03', gpsLicense: 'SF-1', gpsLicenseEndDate: day(2) },
        { id: 'u4', name: 'JD-04', gpsLicense: 'SF-RTK', gpsLicenseEndDate: day(29), licenseDisplay: 'G5 Basic', displayLicenseExpiredAt: day(-40) },
        { id: 'u5', name: 'EXC-01', unitGroup: 'heavy', gpsLicense: 'SF-RTK', gpsLicenseEndDate: day(3) },
        { id: 'u6', name: 'JD-06', gpsLicense: 'StarFire Custom', licenseEndDate: day(0) },
        { id: 'u7', name: 'JD-07', gpsLicense: 'SF-RTK', gpsLicenseEndDate: day(31) }
    ];
    const stock = [
        { id: 'l1', licenseType: 'SF-RTK', qty: 6, txnType: 'IN' }, { id: 'l2', licenseType: 'sf-rtk', qty: 3, txnType: 'OUT' },
        { id: 'l3', licenseType: 'G5 Advance', qty: 10, txnType: 'IN' },
        { id: 'l4', licenseType: 'StarFire Custom', qty: 1, txnType: 'IN' }
    ];

    const b = await launch();
    const page = await b.newPage({ viewport: { width: 1280, height: 900 } });
    const errors = [];
    page.on('pageerror', e => { if (!/Chart is not defined/.test(e.message)) errors.push(e.message); });
    await page.goto(BASE_URL + '/index.html', { waitUntil: 'load' });
    await page.waitForTimeout(800);
    const web = await page.evaluate(([u, s]) => {
        globalData = u; globalLicenseStock = s;
        return { alert: _buildAlertList(undefined, { soonOnly: true }), all: _buildAlertList(), low: _lowStockList() };
    }, [units, stock]);
    const T = [];
    const t = (n, g, w) => T.push({ n, g, w, pass: JSON.stringify(g) === JSON.stringify(w) });

    const srv = server.buildAlertList(units, stock, { soonOnly: true });
    t('server = dashboard: baris laporan email', srv.lines, web.alert.lines);
    t('server = dashboard: jumlah', [srv.total, srv.soonCount, srv.expiredCount], [web.alert.total, web.alert.soonCount, web.alert.expiredCount]);
    const srvAll = server.buildAlertList(units, stock);
    t('server = dashboard: termasuk yang sudah habis', srvAll.lines, web.all.lines);
    t('server = dashboard: stok lisensi menipis', server.lowStockList(stock), web.low);
    const mail = server.composeEmail(units, stock);
    t('email: subjek', mail.subject, `License Alert: ${web.alert.soonCount} expiring soon`);
    t('email: HTML di-escape', server.composeEmail([{ name: '<b>x</b>', gpsLicense: 'SF-RTK', gpsLicenseEndDate: day(3) }], []).html.includes('&lt;b&gt;x&lt;/b&gt;'), true);
    t('email: tidak ada yang habis = tidak ada email', server.composeEmail([{ name: 'A', gpsLicense: 'SF-RTK', gpsLicenseEndDate: day(90) }], []), null);

    // ---------- layar pengaturan + tombol ----------
    const ui = await page.evaluate(async () => {
        const R = {};
        window.__saved = null; window.__calls = []; window.__toasts = [];
        window.showToast = m => __toasts.push(String(m));
        window.cloud = { isReady: true, addHistoryEvents: () => Promise.resolve(),
            getEmailSettings: () => Promise.resolve({ recipients: ['a@x.id'], dailyEnabled: true }),
            saveEmailSettings: r => { __saved = r; return Promise.resolve(); },
            lastMail: () => Promise.resolve({ at: Date.now(), trigger: 'daily', items: 3, recipients: ['a@x.id'] }),
            callFunction: (n, d) => { __calls.push(n); return Promise.resolve(window.__fnResult); } };
        currentUser = { uid: 'o', email: 'o@x.id' };
        currentUserDoc = { uid: 'o', role: 'owner', status: 'active', email: 'o@x.id' };
        await openEmailSettingsModal();
        R.loaded = [document.getElementById('emailRecipients').value, document.getElementById('emailDaily').checked, /Last sent/.test(document.getElementById('emailLast').textContent)];
        document.getElementById('emailRecipients').value = 'B@x.id, c@x.id\nb@x.id';
        await saveEmailSettings();
        R.saved = __saved && [__saved.recipients, __saved.dailyEnabled];
        document.getElementById('emailRecipients').value = 'bukan-email';
        __saved = null; await saveEmailSettings();
        R.bad = [__saved, __toasts[__toasts.length - 1]];
        closeEmailSettingsModal();
        window.__fnResult = { sent: true, recipients: 2, items: 3 };
        await sendLicenseAlertEmail();
        R.sent = [__calls.slice(), __toasts[__toasts.length - 1]];
        window.__fnResult = { sent: false, reason: 'no-recipients' };
        await sendLicenseAlertEmail();
        R.noRec = __toasts[__toasts.length - 1];
        window.cloud.callFunction = () => Promise.reject(Object.assign(new Error('x'), { code: 'functions/not-found' }));
        await sendLicenseAlertEmail();
        R.notDeployed = __toasts[__toasts.length - 1];
        R.btn = document.getElementById('btnEmailAlert').disabled;
        closeEmailSettingsModal();
        currentUserDoc = { uid: 'v', role: 'staff', status: 'active', access: { licenseStock: 'view' } };
        await openEmailSettingsModal();
        R.viewer = [document.getElementById('emailRecipients').disabled, document.getElementById('emailSaveBtn').hidden, document.getElementById('emailOwnerNote').hidden];
        closeEmailSettingsModal();
        R.noEmailjs = typeof emailjs;
        return R;
    });
    t('pengaturan dimuat dari server', ui.loaded, ['a@x.id', true, true]);
    t('owner menyimpan: huruf kecil, tanpa duplikat', ui.saved, [['b@x.id', 'c@x.id'], true]);
    t('alamat salah ditolak', ui.bad, [null, 'Not an email address: bukan-email']);
    t('Email Report memanggil fungsi server', ui.sent, [['sendLicenseAlert'], 'Alert report sent to 2 recipients']);
    t('tanpa penerima: diberi tahu', ui.noRec, 'No recipients yet — set them with the gear icon');
    t('fungsi belum di-deploy: pesan jelas', ui.notDeployed, 'The email service is not set up on the server yet');
    t('tombol kembali aktif', ui.btn, false);
    t('bukan owner: hanya lihat', ui.viewer, [true, true, false]);
    t('EmailJS tidak dimuat lagi', ui.noEmailjs, 'undefined');

    const fail = T.filter(r => !r.pass);
    T.forEach(r => console.log(`${r.pass ? 'PASS' : 'FAIL'}  ${r.n}${r.pass ? '' : `\n        dapat=${JSON.stringify(r.g)} harap=${JSON.stringify(r.w)}`}`));
    console.log(`\n${T.length - fail.length}/${T.length} lulus`);
    console.log('PAGE ERRORS:', errors.length ? errors : 'none');
    await b.close();
    process.exit(fail.length || errors.length ? 1 : 0);
})();
