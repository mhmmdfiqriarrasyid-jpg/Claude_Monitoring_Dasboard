// 2.18.0 — Push notification: siapa yang menerima dan isinya (functions/pushNotices.js).
const p = require('../functions/pushNotices');
const T = [];
const t = (name, g, w) => T.push({ name, g, w, pass: JSON.stringify(g) === JSON.stringify(w) });
const users = [
    { uid: 'vWZZ3jz1LTdzrU5c5Q1Gzco8nh72', role: 'owner', status: 'active' },
    { uid: 'dmgV', role: 'staff', status: 'active', access: { damage: 'view' } },
    { uid: 'dmgHeavy', role: 'staff', status: 'active', access: { damage: 'edit', unitScope: 'heavy' } },
    { uid: 'appr', role: 'staff', status: 'active', access: { teamLogApprove: 'edit' } },
    { uid: 'apprView', role: 'staff', status: 'active', access: { teamLogApprove: 'view' } },
    { uid: 'insAppr', role: 'staff', status: 'active', access: { inspectionApprove: 'edit', unitScope: 'heavy' } },
    { uid: 'insTractor', role: 'staff', status: 'active', access: { inspectionApprove: 'edit', unitScope: 'tractor' } },
    { uid: 'off', role: 'staff', status: 'pending', access: { damage: 'edit' } },
    { uid: 'legacy', role: 'team', status: 'active' }
];
t('kerusakan alat berat: owner, pembaca damage, akun alat berat, role team; bukan yang belum aktif',
  p.recipientsFor('damage', { unitGroup: 'heavy' }, users), ['vWZZ3jz1LTdzrU5c5Q1Gzco8nh72', 'dmgV', 'dmgHeavy', 'legacy']);
t('kerusakan traktor: akun khusus alat berat tidak menerima', p.recipientsFor('damage', { unitGroup: 'tractor' }, users).includes('dmgHeavy'), false);
t('daily report menunggu: penyetuju saja, bukan pembuatnya', p.recipientsFor('workLog', { createdByUid: 'appr' }, users), ['vWZZ3jz1LTdzrU5c5Q1Gzco8nh72', 'legacy']);
t('izin menunggu: penyetuju', p.recipientsFor('leave', { createdByUid: 'x' }, users), ['vWZZ3jz1LTdzrU5c5Q1Gzco8nh72', 'appr', 'legacy']);
t('inspeksi menunggu: penyetuju inspeksi alat berat (role team lama = edit semua)', p.recipientsFor('inspection', {}, users), ['vWZZ3jz1LTdzrU5c5Q1Gzco8nh72', 'insAppr', 'legacy']);
t('isi: breakdown', p.composePush('damage', 'd1', { unitName: 'BD025G', damageType: 'Device', component: 'Camera AI', description: 'Bracket patah', drove: 'cameraAi' }),
  { title: 'Breakdown: BD025G', body: 'Device / Camera AI — Bracket patah', url: './?action=damage', tag: 'dmg-d1' });
t('isi: kerusakan tanpa breakdown', p.composePush('damage', 'd2', { unitName: 'X', damageType: 'Mekanis' }).title, 'Damage reported: X');
t('isi: daily report', p.composePush('workLog', 'w1', { memberName: 'Andi', date: '2026-11-03' }),
  { title: 'Daily Report to check', body: 'Andi · 2026-11-03', url: './?action=inbox', tag: 'wl-w1' });
t('isi: teks panjang dipotong', p.composePush('damage', 'd3', { unitName: 'X', damageType: 'M', description: 'a'.repeat(400) }).body.length, 160);
// 2.45.0: tokens — an account's newest 5, the rest handed back to be removed; each token once; batches of 500.
const tk = p.tokensByAccount([{ id: 'a1', uid: 'a', createdAt: 1 }, ...Array.from({ length: 7 }, (_, i) => ({ id: `b${i}`, uid: 'b', createdAt: i })), { id: 'x', uid: '' }, null, { id: 5, uid: 'c' }, { id: 'c1', uid: 'c', createdAt: 'x' }]);
t('token: 5 terbaru per akun, sisanya dikembalikan untuk dihapus, data aneh dilewati', [[...tk.byUid.entries()], tk.extra], [[['a', ['a1']], ['b', ['b6', 'b5', 'b4', 'b3', 'b2']], ['c', ['c1']]], ['b1', 'b0']]);
// 2.45.1: only extras older than staleBefore are handed back; a newer one is just not used this time.
const st = p.tokensByAccount(Array.from({ length: 7 }, (_, i) => ({ id: `s${i}`, uid: 's', createdAt: i * 10 })), 5);
t('token: hanya token lama (di bawah batas) yang dihapus', [st.byUid.get('s'), st.extra], [['s6', 's5', 's4', 's3', 's2'], ['s0']]);
// 2.45.2: a date more than a day ahead counts as the oldest; past 20 an account's tokens go whatever their date.
const fut = p.tokensByAccount([{ id: 'f1', uid: 'f', createdAt: 9e15 }, ...Array.from({ length: 5 }, (_, i) => ({ id: `g${i}`, uid: 'f', createdAt: 1000 + i }))], 500, 2000);
t('token: tanggal masa depan = paling lama, dihapus', [fut.byUid.get('f'), fut.extra], [['g4', 'g3', 'g2', 'g1', 'g0'], ['f1']]);
const big = p.tokensByAccount(Array.from({ length: 25 }, (_, i) => ({ id: `k${i}`, uid: 'k', createdAt: 1000 + i })), 0, 2000);
t('token: lebih dari 20 per akun → kelebihannya dihapus walau baru', big.extra, ['k4', 'k3', 'k2', 'k1', 'k0']);
const many = new Map(Array.from({ length: 130 }, (_, i) => [`u${i}`, [`t${i}a`, `t${i}b`, `t${i}c`, `t${i}d`, `t${i}e`]]));
many.set('dup', ['t0a']);
const bs = p.pushBatches([...many.keys(), 'u0', 'none'], many);
t('token: batch paling banyak 500, tiap token sekali', [bs.map(b => b.length), new Set(bs.flat()).size], [[500, 150], 650]);
t('token: tanpa penerima = tanpa batch', p.pushBatches([], many), []);
t('isi ticket: unit bukan daftar / berisi null tidak membuat gagal', [p.composePush('ticketNew', 'k', { title: 'X', category: 'C', units: 5 }).body, p.composePush('ticketOverdue', 'k', { title: 'X', units: [null, { name: 'GGTR' }] }).body],
  ['C — ', ' · Medium priority · no PIC yet · GGTR']);
let pass = 0;
T.forEach(x => { if (x.pass) pass++; console.log(`${x.pass ? 'PASS' : 'FAIL'}  ${x.name}${x.pass ? '' : `\n        dapat=${JSON.stringify(x.g)} harap=${JSON.stringify(x.w)}`}`); });
console.log(`\n${pass}/${T.length} lulus`);
process.exit(pass === T.length ? 0 : 1);
