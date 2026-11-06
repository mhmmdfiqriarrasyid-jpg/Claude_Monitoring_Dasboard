// 2.20.0 — Jadwal notifikasi (functions/schedule.js) dan periode laporan ringkasan.
const s = require('../functions/schedule');
const r = require('../functions/monthlyReport');
const T = [];
const t = (name, g, w) => T.push({ name, g, w, pass: JSON.stringify(g) === JSON.stringify(w) });
const at = (y, m, d, h) => new Date(y, m - 1, d, h, 5).getTime();   // menit 5 = tick pertama jam itu
// 2 Nov 2026 = Senin
t('bawaan lama: License harian jam 7, tanpa field baru', s.readSchedule({ sendHour: 7 }, 'send', { freq: 'daily', hour: 7 }), { freq: 'daily', hour: 7, weekday: 1, monthday: 1 });
t('nilai tersimpan dipakai', s.readSchedule({ reminderFreq: 'weekly', reminderHour: 16, reminderWeekday: 5 }, 'reminder', { freq: 'daily', hour: 17 }), { freq: 'weekly', hour: 16, weekday: 5, monthday: 1 });
t('nilai rusak → bawaan', s.readSchedule({ monthlyFreq: 'yearly', monthlyHour: 99, monthlyMonthday: 31 }, 'monthly', { freq: 'monthly', hour: 7, monthday: 1 }), { freq: 'monthly', hour: 7, weekday: 1, monthday: 1 });
t('harian: tiap hari di jamnya saja', [s.isDue({ freq: 'daily', hour: 7 }, at(2026, 11, 2, 7)), s.isDue({ freq: 'daily', hour: 7 }, at(2026, 11, 2, 8))], [true, false]);
t('mingguan: hanya hari yang dipilih', [s.isDue({ freq: 'weekly', hour: 7, weekday: 1 }, at(2026, 11, 2, 7)), s.isDue({ freq: 'weekly', hour: 7, weekday: 1 }, at(2026, 11, 3, 7))], [true, false]);
t('bulanan: hanya tanggal yang dipilih', [s.isDue({ freq: 'monthly', hour: 7, monthday: 15 }, at(2026, 11, 15, 7)), s.isDue({ freq: 'monthly', hour: 7, monthday: 15 }, at(2026, 11, 16, 7))], [true, false]);
const d = s.periodRange('daily', at(2026, 11, 3, 7)), w = s.periodRange('weekly', at(2026, 11, 9, 7)), m = s.periodRange('monthly', at(2026, 11, 1, 7));
t('periode harian = kemarin', [d.from, d.to, d.name, d.label], ['2026-11-02', '2026-11-03', 'Daily Summary', '2 November 2026']);
t('periode mingguan = 7 hari sebelum hari ini', [w.from, w.to, w.name, w.label], ['2026-11-02', '2026-11-09', 'Weekly Summary', '2 Nov – 8 Nov 2026']);
t('periode bulanan = bulan lalu', [m.from, m.to, m.name, m.label], ['2026-10-01', '2026-11-01', 'Monthly Report', 'October 2026']);
const data = { units: [], damages: [{ date: '2026-11-02', damageType: 'Mekanis' }, { date: '2026-11-05', damageType: 'Mekanis' }, { date: '2026-10-30', damageType: 'Mekanis' }], workLogs: [] };
t('ringkasan mingguan menghitung minggu itu saja', r.buildMonthly({ ...data, now: at(2026, 11, 9, 7), freq: 'weekly' }).damage.total, 2);
t('ringkasan harian: kemarin saja', r.buildMonthly({ ...data, now: at(2026, 11, 3, 7), freq: 'daily' }).damage.total, 1);
t('subjek mengikuti periode', r.composeMonthly(r.buildMonthly({ ...data, now: at(2026, 11, 9, 7), freq: 'weekly' })).subject, 'Weekly Summary — 2 Nov – 8 Nov 2026');
let pass = 0;
T.forEach(x => { if (x.pass) pass++; console.log(`${x.pass ? 'PASS' : 'FAIL'}  ${x.name}${x.pass ? '' : `\n        dapat=${JSON.stringify(x.g)} harap=${JSON.stringify(x.w)}`}`); });
console.log(`\n${pass}/${T.length} lulus`);
process.exit(pass === T.length ? 0 : 1);
