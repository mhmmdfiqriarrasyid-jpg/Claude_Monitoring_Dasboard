// 2.30.0 — push ticket: siapa menerima apa (functions/ticketNotices.js + pushNotices.js).
const { ticketPushes } = require('../functions/ticketNotices');
const p = require('../functions/pushNotices');
const T = [];
const t = (name, g, w) => T.push({ name, g, w, pass: JSON.stringify(g) === JSON.stringify(w) });
const now = Date.parse('2026-11-05T08:00:00Z');
const tk = (o) => Object.assign({ id: 'a', status: 'open', assigneeUid: '', reportedByUid: 'khl', lastActorUid: 'khl', updatedAt: now, title: 'GPS hilang', priority: 'high', no: 'TK-1' }, o);
const r0 = ticketPushes(null, [tk({})], now);
t('jalan pertama: hanya mencatat', [r0.out.length, r0.next.a[0]], [0, 'open']);
const r1 = ticketPushes(r0.next, [tk({}), tk({ id: 'b' })], now);
t('ticket baru → ticketNew', r1.out.map(x => [x.kind, x.id]), [['ticketNew', 'b']]);
const r2 = ticketPushes(r1.next, [tk({ status: 'assigned', assigneeUid: 'ot1', lastActorUid: 'ot2' }), tk({ id: 'b' })], now);
t('ditugaskan → PIC + pelapor (status berubah oleh OT)', r2.out.map(x => x.kind), ['ticketAssign', 'ticketStatus']);
const r3 = ticketPushes(r2.next, [tk({ status: 'closed', assigneeUid: 'ot1', lastActorUid: 'khl' }), tk({ id: 'b' })], now);
t('pelapor sendiri menutup → tidak ada push ke dirinya', r3.out.length, 0);
const old = ticketPushes({ z: ['closed', '', now - 40 * 86400000] }, [], now);
t('catatan lebih dari 30 hari dibuang', Object.keys(old.next), []);
const users = [
    { uid: 'vWZZ3jz1LTdzrU5c5Q1Gzco8nh72', role: 'owner', status: 'active' },
    { uid: 'ot1', role: 'staff', status: 'active', access: { tickets: 'edit' } },
    { uid: 'v', role: 'staff', status: 'active', access: { tickets: 'view' } },
    { uid: 'khl', role: 'khl', status: 'active', access: { tickets: 'edit' } }];
t('ticket baru: tim OT (edit) + owner, bukan pelapor', p.recipientsFor('ticketNew', tk({}), users), ['vWZZ3jz1LTdzrU5c5Q1Gzco8nh72', 'ot1']);
t('ditugaskan: hanya PIC', p.recipientsFor('ticketAssign', tk({ assigneeUid: 'ot1', lastActorUid: 'v' }), users), ['ot1']);
t('menugaskan diri sendiri: tanpa push', p.recipientsFor('ticketAssign', tk({ assigneeUid: 'ot1', lastActorUid: 'ot1' }), users), []);
t('status berubah: pelapor', p.recipientsFor('ticketStatus', tk({ status: 'resolved' }), users), ['khl']);
const m = p.composePush('ticketStatus', 'a', tk({ status: 'resolved', resolution: 'Antena diganti' }));
t('isi: resolved minta konfirmasi + tautan ke ticket', [m.title, /Antena diganti/.test(m.body), m.url], ['Your ticket is resolved — please confirm', true, './?action=tickets&ticket=a']);
let pass = 0;
T.forEach(x => { if (x.pass) pass++; console.log(`${x.pass ? 'PASS' : 'FAIL'}  ${x.name}${x.pass ? '' : `\n        dapat=${JSON.stringify(x.g)} harap=${JSON.stringify(x.w)}`}`); });
console.log(`\n${pass}/${T.length} lulus`);
process.exit(pass === T.length ? 0 : 1);
