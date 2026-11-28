// 2.18.0 — Laporan bulanan (functions/monthlyReport.js): angka bulan lalu
// dan isi emailnya.
const r = require('../functions/monthlyReport');
const T = [];
const t = (name, g, w) => T.push({ name, g, w, pass: JSON.stringify(g) === JSON.stringify(w) });

const now = new Date(2026, 10, 1, 7, 0).getTime();          // 1 Nov 2026 07:00 → laporan Oktober
const at = (d, h) => new Date(2026, 9, d, h || 0).getTime();
const m = r.previousMonth(now);
t('bulan lalu: Oktober 2026', [m.from, m.to, m.label], ['2026-10-01', '2026-11-01', 'October 2026']);

const units = [
    { id: 'u1', name: 'GGTR101G', status: 'Good', downtimeHistory: [{ start: at(2, 8), end: at(3, 8), durationMs: 86400000 }, { start: new Date(2026, 8, 30, 12).getTime(), end: at(1, 12), durationMs: 0 }] },
    { id: 'u2', name: 'BD025G', unitGroup: 'heavy', status: 'Breakdown', breakdownStartedAt: at(31, 0), gpsLicense: 'SF-RTK', gpsLicenseEndDate: '2026-11-10' },
    { id: 'u3', name: 'GGTR102G', status: 'Good', gpsLicense: 'SF-RTK', gpsLicenseEndDate: '2026-11-20' }
];
const damages = [
    { unitId: 'u1', date: '2026-10-02', damageType: 'Mekanis', resolved: true },
    { unitId: 'u1', date: '2026-10-15', damageType: 'Device Precision' },
    { unitId: 'u2', date: '2026-10-31', damageType: 'Mekanis' },
    { unitId: 'u2', date: '2026-09-30', damageType: 'Mekanis' }       // bulan lain
];
const workLogs = [
    { date: '2026-10-05', company: 'PT. GPA', start: '07:00', end: '15:30', approval: 'approved' },
    { date: '2026-10-06', company: 'PT. GPA', start: '23:00', end: '07:00', approval: 'pending', place: 'field' },
    { date: '2026-10-07', company: 'PT. MNM', start: '08:00', end: '12:00', approval: 'approved', place: 'office' },
    { date: '2026-10-08', company: 'PT. MNM', start: '08:00', end: '12:00', approval: 'draft' },   // draft tidak dihitung
    { date: '2026-11-01', company: 'PT. MNM', start: '08:00', end: '12:00', approval: 'approved' }
];
const rep = r.buildMonthly({ units, damages, workLogs, licenseStock: [], now });
t('armada', rep.fleet, { total: 3, heavy: 1, breakdownNow: 1 });
t('kerusakan bulan itu saja', [rep.damage.total, rep.damage.resolved], [3, 1]);
t('kerusakan per tipe', rep.damage.byType, [['Mekanis', 2], ['Device Precision', 1]]);
t('unit paling sering rusak', rep.damage.topUnits, [['GGTR101G', 2], ['BD025G', 1]]);
t('downtime: interval dipotong ke bulan, breakdown berjalan dihitung sampai akhir bulan',
  rep.downtime.topUnits.map(x => [x.name, Math.round(x.hours * 10) / 10]), [['GGTR101G', 36], ['BD025G', 24]]);
t('daily report: tanpa draft, jam shift malam benar', [rep.reports.total, rep.reports.approved, rep.reports.office, rep.reports.hours], [3, 2, 1, 20.5]);
t('jam per perusahaan', rep.reports.byCompany, [['PT. GPA', 16.5], ['PT. MNM', 4]]);
t('lisensi habis dalam 30 hari (heavy tidak dihitung)', rep.licenses.expiringSoon, 1);
const mail = r.composeMonthly(rep);
t('email: subjek', mail.subject, 'Monthly Report — October 2026');
t('email: teks memuat angka utama', ['DAMAGE: 3 recorded', 'DOWNTIME: 60 hours', 'DAILY REPORTS: 3 sent'].every(s => mail.text.includes(s)), true);
t('email: HTML di-escape', r.composeMonthly({ ...rep, month: '<b>x</b>' }).html.includes('&lt;b&gt;x&lt;/b&gt;'), true);

let pass = 0;
T.forEach(x => { if (x.pass) pass++; console.log(`${x.pass ? 'PASS' : 'FAIL'}  ${x.name}${x.pass ? '' : `\n        dapat=${JSON.stringify(x.g)} harap=${JSON.stringify(x.w)}`}`); });
console.log(`\n${pass}/${T.length} lulus`);
process.exit(pass === T.length ? 0 : 1);
