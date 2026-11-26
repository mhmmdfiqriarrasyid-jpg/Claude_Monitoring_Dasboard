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
            lastMails: () => Promise.resolve({ licenseAlert: { at: Date.now(), trigger: 'daily', items: 3, recipients: ['a@x.id'] } }),
            callFunction: (n, d) => { __calls.push(n); return Promise.resolve(window.__fnResult); } };
        currentUser = { uid: 'o', email: 'o@x.id' };
        currentUserDoc = { uid: 'o', role: 'owner', status: 'active', email: 'o@x.id' };
        await openEmailSettingsModal();
        R.loaded = [document.getElementById('emailRecipients').value, document.getElementById('emailDaily').checked, /Last report/.test(document.getElementById('emailLast').textContent)];
        R.otherLast = [document.getElementById('damageLast').textContent, document.getElementById('reminderLast').textContent];
        document.getElementById('emailRecipients').value = 'B@x.id, c@x.id\nb@x.id';
        await saveEmailSettings();
        R.saved = __saved && [__saved.recipients, __saved.dailyEnabled, __saved.sendHour];
        R.savedDefaults = __saved && [__saved.damageEnabled, __saved.damageRecipients, __saved.reminderEnabled, __saved.reminderHour, __saved.reminderRecipients];
        // 2.15.0: breakdown + reminder sections
        document.getElementById('damageEnabled').checked = true; await saveEmailSettings();
        R.dmgNeedsRec = __toasts[__toasts.length - 1];
        document.getElementById('damageRecipients').value = 'Sup@x.id';
        document.getElementById('reminderEnabled').checked = true; syncEmailDaily();
        R.remHourShown = document.getElementById('reminderHourGroup').style.display;
        document.getElementById('reminderRecipients').value = 'lead@x.id, x@';
        __saved = null; await saveEmailSettings();
        R.remBad = [__saved, __toasts[__toasts.length - 1]];
        document.getElementById('reminderRecipients').value = 'lead@x.id';
        document.getElementById('reminderHour').value = '18';
        await saveEmailSettings();
        R.full = __saved && [__saved.damageEnabled, __saved.damageRecipients, __saved.reminderEnabled, __saved.reminderHour, __saved.reminderRecipients];
        // 2.18.0: laporan bulanan
        document.getElementById('monthlyEnabled').checked = true; syncEmailDaily();
        await saveEmailSettings();
        R.monNeedsRec = __toasts[__toasts.length - 1];
        document.getElementById('monthlyRecipients').value = 'Boss@x.id';
        document.getElementById('monthlyHour').value = '8';
        await saveEmailSettings();
        R.monthly = __saved && [__saved.monthlyEnabled, __saved.monthlyRecipients, __saved.monthlyHour, document.getElementById('monthlyHourGroup').style.display];
        R.pushDefaults = __saved && [__saved.pushDamage, __saved.pushApprovals];
        document.getElementById('pushApprovals').checked = false;
        await saveEmailSettings();
        R.pushOff = __saved && __saved.pushApprovals;
        // 2.20.0: jadwal mingguan / bulanan
        document.getElementById('emailFreq').value = 'weekly'; syncEmailDaily();
        R.weeklyShows = [document.getElementById('emailWeekdayGroup').style.display, document.getElementById('emailMonthdayGroup').style.display];
        document.getElementById('emailWeekday').value = '5'; document.getElementById('emailHour').value = '9';
        document.getElementById('monthlyFreq').value = 'monthly'; document.getElementById('monthlyMonthday').value = '15'; syncEmailDaily();
        R.monthlyShows = [document.getElementById('monthlyWeekdayGroup').style.display, document.getElementById('monthlyMonthdayGroup').style.display];
        await saveEmailSettings();
        R.sched = __saved && [__saved.sendFreq, __saved.sendWeekday, __saved.sendHour, __saved.monthlyFreq, __saved.monthlyMonthday, __saved.reminderFreq];
        document.getElementById('monthlyFreq').value = 'weekly'; syncEmailDaily();
        R.note = document.getElementById('monthlySendNote').textContent;
        document.getElementById('monthlyFreq').value = 'monthly'; document.getElementById('emailFreq').value = 'daily';
        document.getElementById('monthlyEnabled').checked = false; syncEmailDaily();
        document.getElementById('damageEnabled').checked = false; document.getElementById('reminderEnabled').checked = false; syncEmailDaily();
        R.hourShown = document.getElementById('emailHourGroup').style.display;
        document.getElementById('emailHour').value = '16'; document.getElementById('emailDaily').checked = false; syncEmailDaily();
        R.hourHidden = document.getElementById('emailHourGroup').style.display;
        await saveEmailSettings();
        R.off = __saved && [__saved.dailyEnabled, __saved.sendHour];
        document.getElementById('emailDaily').checked = true;
        document.getElementById('emailRecipients').value = 'B@x.id, c@x.id\nb@x.id';
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
        window.cloud.callFunction = () => Promise.reject(Object.assign(new Error('gmail-auth'), { code: 'functions/failed-precondition' }));
        await sendLicenseAlertEmail();
        R.gmailAuth = __toasts[__toasts.length - 1];
        // 2.45.0: the server's limit — its message is shown as it is, the button comes back.
        window.showToast = (m, type) => __toasts.push(String(m) + '|' + type);
        window.cloud.callFunction = () => Promise.reject(Object.assign(new Error('You have sent the License Alert 3 times today — try again tomorrow'), { code: 'functions/resource-exhausted' }));
        await sendLicenseAlertEmail();
        R.limited = [__toasts[__toasts.length - 1], document.getElementById('btnEmailAlert').disabled];
        window.cloud.callFunction = () => Promise.reject(Object.assign(new Error(''), { code: 'functions/resource-exhausted' }));
        await sendLicenseAlertEmail();
        R.limitedNoMsg = __toasts[__toasts.length - 1];
        window.showToast = m => __toasts.push(String(m));
        R.btn = document.getElementById('btnEmailAlert').disabled;
        closeEmailSettingsModal();
        currentUserDoc = { uid: 'v', role: 'staff', status: 'active', access: { licenseStock: 'view' } };
        await openEmailSettingsModal();
        R.viewerBtns = [document.getElementById('damageSendBtn').hidden, document.getElementById('reminderSendBtn').hidden];
        R.viewer = [document.getElementById('emailRecipients').disabled, document.getElementById('emailSaveBtn').hidden, document.getElementById('emailOwnerNote').hidden];
        closeEmailSettingsModal();
        // 2.45.0 (S13): the mail log is only asked for by an account that sees every unit group.
        R.viewerLast = /Last report/.test(document.getElementById('emailLast').textContent);
        let asked = 0; const lm = window.cloud.lastMails; window.cloud.lastMails = () => { asked++; return lm(); };
        currentUserDoc = { uid: 'v', role: 'staff', status: 'active', access: { licenseStock: 'view', unitScope: 'tractor' } };
        await openEmailSettingsModal();
        R.scopedLast = [asked, document.getElementById('emailLast').textContent, document.getElementById('emailRecipients').value];
        closeEmailSettingsModal();
        window.cloud.lastMails = lm;
        R.noEmailjs = typeof emailjs;
        // 2.15.1: Send test / Send now per bagian
        currentUserDoc = { uid: 'o', role: 'owner', status: 'active' };
        const calls2 = []; let res2 = { sent: true, recipients: 1 };
        window.cloud.callFunction = (n, d) => { calls2.push([n, d && d.kind]); return Promise.resolve(res2); };
        await openEmailSettingsModal();
        await sendNotificationNow('damage', document.getElementById('damageSendBtn'));
        R.dmgSend = [calls2.slice(), __toasts[__toasts.length - 1]];
        res2 = { sent: false, reason: 'all-sent' };
        await sendNotificationNow('reminder', document.getElementById('reminderSendBtn'));
        R.remSend = [calls2[1], __toasts[__toasts.length - 1]];
        res2 = { sent: true, recipients: 2 };
        await sendNotificationNow('monthly', document.getElementById('monthlySendBtn'));
        R.monSend = [calls2[2], __toasts[__toasts.length - 1]];
        document.getElementById('damageRecipients').value = 'baru@x.id';
        calls2.length = 0;
        await sendNotificationNow('damage', null);
        R.unsaved = [calls2.length, __toasts[__toasts.length - 1]];
        closeEmailSettingsModal();
        currentUserDoc = { uid: 'o', role: 'owner', status: 'active' }; applyAccessVisibility();
        R.navOwner = document.getElementById('navNotifications').style.display;
        currentUserDoc = { uid: 'n', role: 'staff', status: 'active', access: {} }; applyAccessVisibility();
        R.navNone = document.getElementById('navNotifications').style.display;
        currentUserDoc = { uid: 'v', role: 'staff', status: 'active', access: { licenseStock: 'view' } }; applyAccessVisibility();
        R.navViewer = document.getElementById('navNotifications').style.display;
        return R;
    });
    t('pengaturan dimuat dari server', ui.loaded, ['a@x.id', true, true]);
    t('riwayat per jenis notifikasi', ui.otherLast, ['No alert sent yet', 'No reminder sent yet']);
    t('bawaan: breakdown & pengingat mati, jam pengingat 17', ui.savedDefaults, [false, [], false, 17, []]);
    t('breakdown aktif tanpa penerima ditolak', ui.dmgNeedsRec, 'Breakdown alerts: add at least one recipient');
    t('jam pengingat tampil saat aktif', ui.remHourShown, '');
    t('alamat salah di pengingat ditolak', ui.remBad, [null, 'Daily Report reminder: not an email address — x@']);
    t('2.18.0: laporan ringkasan aktif tanpa penerima ditolak', ui.monNeedsRec, 'Summary Report: add at least one recipient');
    t('2.18.0: laporan bulanan tersimpan', ui.monthly, [true, ['boss@x.id'], 8, '']);
    t('2.18.0: push bawaan aktif', ui.pushDefaults, [true, true]);
    t('2.20.0: mingguan → pilih hari', ui.weeklyShows, ['', 'none']);
    t('2.20.0: bulanan → pilih tanggal', ui.monthlyShows, ['none', '']);
    t('2.20.0: jadwal tersimpan', ui.sched, ['weekly', 5, 9, 'monthly', 15, 'daily']);
    t('2.20.0: catatan tombol mengikuti periode', ui.note, 'The last full week (Saturday–Friday)');
    t('2.18.0: push persetujuan bisa dimatikan', ui.pushOff, false);
    t('breakdown + pengingat tersimpan', ui.full, [true, ['sup@x.id'], true, 18, ['lead@x.id']]);
    t('owner menyimpan: huruf kecil, tanpa duplikat, jam bawaan 07', ui.saved, [['b@x.id', 'c@x.id'], true, 7]);
    t('jam kirim tampil saat harian aktif, tersembunyi saat mati', [ui.hourShown, ui.hourHidden], ['', 'none']);
    t('harian dimatikan, jam tersimpan', ui.off, [false, 16]);
    t('alamat salah ditolak', ui.bad, [null, 'License Alert: not an email address — bukan-email']);
    t('Email Report memanggil fungsi server', ui.sent, [['sendLicenseAlert'], 'Alert report sent to 2 recipients']);
    t('tanpa penerima: diberi tahu', ui.noRec, 'No recipients yet — set them with the gear icon');
    t('fungsi belum di-deploy: pesan jelas', ui.notDeployed, 'The email service is not set up on the server yet');
    t('App Password salah: pesan jelas', ui.gmailAuth, 'Gmail refused the sign-in — the App Password on the server is wrong (see functions/README.md)');
    t('tombol kembali aktif', ui.btn, false);
    t('2.45.0: dibatasi server: pesannya ditampilkan apa adanya, tombol aktif lagi', ui.limited, ['You have sent the License Alert 3 times today — try again tomorrow|warning', false]);
    t('2.45.0: dibatasi tanpa pesan: pesan bawaan', ui.limitedNoMsg, 'The License Alert was sent recently — try again later|warning');
    // 2.45.0: the limit itself (functions/licenseReport.js manualSendCheck), pure.
    {
        const ck = server.manualSendCheck, at = (d, h, m) => new Date(`${d}T${String(h).padStart(2, '0')}:${String(m || 0).padStart(2, '0')}:00`).getTime();
        let st = null; const run = (uid, ms, owner) => { const r = ck(st, uid, ms, owner); if (r.ok) st = r.next; return r.ok ? 'ok' : r.message; };
        const d1 = '2026-11-10';
        t('batas: kirim pertama boleh', run('v1', at(d1, 8)), 'ok');
        t('batas: 10 menit untuk semua akun', [run('v2', at(d1, 8, 5)), run('v1', at(d1, 8, 9))], ['The License Alert was sent less than 10 minutes ago — try again in 5 min', 'The License Alert was sent less than 10 minutes ago — try again in 1 min']);
        t('batas: owner tidak ditahan tapi dihitung', [run('own', at(d1, 8, 9), true), st.total, st.lastAt === at(d1, 8, 9)], ['ok', 2, true]);
        run('v1', at(d1, 9)); run('v1', at(d1, 10));
        t('batas: 3 kali sehari per akun', run('v1', at(d1, 11)), 'You have sent the License Alert 3 times today — try again tomorrow');
        ['a', 'b', 'c', 'd', 'e', 'f'].forEach((u, i) => run(u, at(d1, 12 + i)));
        t('batas: 10 kali sehari untuk semua', [st.total, run('g', at(d1, 18))], [10, 'The License Alert has been sent 10 times today — try again tomorrow']);
        t('batas: owner tetap boleh setelah 10', run('own', at(d1, 18, 1), true), 'ok');
        t('batas: jeda 10 menit tetap berlaku lewat tengah malam', [run('own', at(d1, 23, 58), true), run('v1', at('2026-11-11', 0, 3))], ['ok', 'The License Alert was sent less than 10 minutes ago — try again in 5 min']);
        t('batas: hari berikutnya hitungan mulai lagi', [run('v1', at('2026-11-11', 7)), st.total, st.byUid], ['ok', 1, { v1: 1 }]);
        t('batas: data aneh di state tidak membuat gagal', ck({ day: '2026-11-11', total: 'x', byUid: 5, lastAt: 'y' }, 'z', at('2026-11-11', 9)).ok, true);
    }
    t('bukan owner: hanya lihat', ui.viewer, [true, true, false]);
    t('EmailJS tidak dimuat lagi', ui.noEmailjs, 'undefined');
    t('Send test breakdown memanggil server', ui.dmgSend, [[['sendNotificationNow', 'damage']], 'Test alert sent to 1 recipient']);
    t('Send now pengingat: semua sudah lapor', ui.remSend, [['sendNotificationNow', 'reminder'], 'Everyone has sent their Daily Report — nothing to send']);
    t('2.18.0: Send now laporan ringkasan', ui.monSend, [['sendNotificationNow', 'monthly'], 'Summary report sent to 2 recipients']);
    t('perubahan belum disimpan: tidak dikirim', ui.unsaved, [0, 'Save the settings first — sending uses the saved recipients']);
    t('bukan owner: tombol kirim tersembunyi', ui.viewerBtns, [true, true]);
    t('S13 pembaca License Stock (semua kelompok): baris terakhir dikirim tampil', ui.viewerLast, true);
    t('S13 pembaca License Stock satu kelompok: mailLog tidak diminta, pengaturan tetap dimuat', ui.scopedLast, [0, '', 'a@x.id']);
    t('menu Admin → Notifications: owner & pembaca License Stock saja', [ui.navOwner, ui.navNone, ui.navViewer], ['', 'none', '']);

    const fail = T.filter(r => !r.pass);
    T.forEach(r => console.log(`${r.pass ? 'PASS' : 'FAIL'}  ${r.n}${r.pass ? '' : `\n        dapat=${JSON.stringify(r.g)} harap=${JSON.stringify(r.w)}`}`));
    console.log(`\n${T.length - fail.length}/${T.length} lulus`);
    console.log('PAGE ERRORS:', errors.length ? errors : 'none');
    await b.close();
    process.exit(fail.length || errors.length ? 1 : 0);
})();
