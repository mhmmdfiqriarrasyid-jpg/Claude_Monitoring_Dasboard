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
let pass = 0;
T.forEach(x => { if (x.pass) pass++; console.log(`${x.pass ? 'PASS' : 'FAIL'}  ${x.name}${x.pass ? '' : `\n        dapat=${JSON.stringify(x.g)} harap=${JSON.stringify(x.w)}`}`); });
console.log(`\n${pass}/${T.length} lulus`);
process.exit(pass === T.length ? 0 : 1);
