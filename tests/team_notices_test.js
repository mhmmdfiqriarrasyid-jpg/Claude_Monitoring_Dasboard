// 2.15.0 — Pengingat Daily Report dan email breakdown (functions/teamNotices.js):
// siapa yang belum kirim laporan shift yang sudah selesai, dan isi emailnya.
const n = require('../functions/teamNotices');
const T = [];
const t = (name, g, w) => T.push({ name, g, w, pass: JSON.stringify(g) === JSON.stringify(w) });

const members = [{ id: 'a', name: 'Andi', company: 'PT. GPA' }, { id: 'b', name: 'Budi' }, { id: 'c', name: 'Citra' },
    { id: 'd', name: 'Dewi' }, { id: 'e', name: 'Eko', active: false }, { id: 'f', name: 'Fajar' }, { id: 'g', name: 'Gita' }];
const shifts = [
    { date: '2026-11-03', memberId: 'a', shift: 'pagi' },     // selesai 15:00, belum kirim
    { date: '2026-11-03', memberId: 'b', shift: 'siang' },    // selesai 23:00
    { date: '2026-11-02', memberId: 'c', shift: 'malam' },    // selesai 07:00 hari ini, belum kirim
    { date: '2026-11-03', memberId: 'd', shift: 'pagi' },     // sudah kirim
    { date: '2026-11-03', memberId: 'e', shift: 'pagi' },     // tidak aktif
    { date: '2026-11-03', memberId: 'f', shift: 'pagi' },     // cuti disetujui
    { date: '2026-11-03', memberId: 'g', shift: 'pagi' },     // masih draft = belum kirim
    { date: '2026-11-03', memberId: 'a', shift: 'libur' },
    { date: '2026-11-03', memberId: 'zz', shift: 'pagi' }     // anggota sudah dihapus
];
const workLogs = [{ memberId: 'd', date: '2026-11-03', approval: 'pending' }, { memberId: 'g', date: '2026-11-03', approval: 'draft' }];
const leaves = [{ memberId: 'f', dateFrom: '2026-11-01', dateTo: '2026-11-04', approval: 'approved' }, { memberId: 'a', dateFrom: '2026-11-03', approval: 'pending' }];

const at = h => new Date(2026, 10, 3, h, 0, 0);
const names = h => n.missingReports({ shifts, workLogs, members, leaves, now: at(h) }).map(r => `${r.name}/${r.shift}/${r.date}`);
t('jam 17: pagi hari ini + malam kemarin; draft dihitung belum kirim', names(17), ['Citra/Night/2026-11-02', 'Andi/Morning/2026-11-03', 'Gita/Morning/2026-11-03']);
t('jam 06: belum ada shift yang selesai', names(6), []);
t('jam 23: shift siang ikut', names(23).includes('Budi/Afternoon/2026-11-03'), true);
t('tidak aktif, cuti disetujui, sudah kirim, anggota terhapus: tidak masuk', ['Eko', 'Fajar', 'Dewi'].some(x => names(23).join().includes(x)), false);

const rem = n.composeReminder(n.missingReports({ shifts, workLogs, members, leaves, now: at(17) }));
t('email pengingat: subjek', rem.subject, 'Daily Report reminder: 3 not sent yet');
t('email pengingat: perusahaan tampil', rem.text.includes('Andi | PT. GPA'), true);
t('tidak ada yang kurang = tidak ada email', n.composeReminder([]), null);

const bd = n.composeDamage({ unitName: 'JD-01 <x>', sn: 'S1', site: 'Timika', date: '2026-11-03', damageType: 'Device Precision', component: 'GPS', description: 'Sinyal hilang', drove: 'gps' });
t('email breakdown: subjek', bd.subject, 'Breakdown: JD-01 <x> — Device Precision / GPS');
t('email breakdown: HTML di-escape', bd.html.includes('JD-01 &lt;x&gt;') && !bd.html.includes('JD-01 <x>'), true);
t('kerusakan tanpa breakdown', n.composeDamage({ unitName: 'JD-02', damageType: 'Mechanical' }).subject, 'Damage reported: JD-02 — Mechanical');
t('catatan yang disimpan sudah selesai diperbaiki: tidak dikirim', n.composeDamage({ unitName: 'JD-03', resolved: true }), null);

let pass = 0;
T.forEach(x => { if (x.pass) pass++; console.log(`${x.pass ? 'PASS' : 'FAIL'}  ${x.name}${x.pass ? '' : `\n        dapat=${JSON.stringify(x.g)} harap=${JSON.stringify(x.w)}`}`); });
console.log(`\n${pass}/${T.length} lulus`);
process.exit(pass === T.length ? 0 : 1);
