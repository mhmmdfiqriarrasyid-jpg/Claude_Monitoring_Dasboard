// 2.30.0 — push ticket: siapa menerima apa (functions/ticketNotices.js + pushNotices.js).
const { ticketPushes, ticketSlaPushes } = require('../functions/ticketNotices');
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
// ---- 2.35.0: SLA escalation ----
const H = 3600000;
const sla = o => tk(Object.assign({ status: 'assigned', assigneeUid: 'ot1', assigneeName: 'Kautsar', reportedAt: now }, o));
const s0 = ticketSlaPushes({}, [sla({})], now + 1 * H);
t('High, 1 jam berjalan: belum ada push', s0.out.length, 0);
const s1 = ticketSlaPushes(s0.next, [sla({})], now + 3 * H + 60000);
t('seperempat waktu terakhir (High: 1 jam): due soon', s1.out.map(x => x.kind), ['ticketDueSoon']);
const s1b = ticketSlaPushes(s1.next, [sla({})], now + 3.5 * H);
t('due soon hanya sekali', s1b.out.length, 0);
const s2 = ticketSlaPushes(s1b.next, [sla({})], now + 4 * H + 60000);
t('lewat SLA: overdue', s2.out.map(x => x.kind), ['ticketOverdue']);
const s3 = ticketSlaPushes(s2.next, [sla({})], now + 10 * H);
t('belum 24 jam sejak pengingat terakhir: diam', s3.out.length, 0);
const s4 = ticketSlaPushes(s3.next, [sla({})], now + 4 * H + 60000 + 24 * H);
t('masih terbuka 24 jam kemudian: overdue lagi', s4.out.map(x => x.kind), ['ticketOverdue']);
const s5 = ticketSlaPushes(s4.next, [sla({ status: 'resolved' })], now + 60 * H);
t('sudah resolved: berhenti & dilupakan', [s5.out.length, Object.keys(s5.next)], [0, []]);
const sFirst = ticketSlaPushes(null, [sla({ reportedAt: now - 100 * H })], now);
t('jalan pertama: ticket lama yang sudah telat hanya dicatat', [sFirst.out.length, sFirst.next.a[0]], [0, 2]);
const sOld = ticketSlaPushes({ a: [2, now - 30 * 86400000] }, [sla({ reportedAt: now - 30 * 86400000 })], now);
t('telat lebih dari 2 minggu: tidak diingatkan terus', sOld.out.length, 0);
t('Medium: due soon mulai jam ke-18', [ticketSlaPushes({}, [sla({ priority: 'medium' })], now + 17 * H).out.length, ticketSlaPushes({}, [sla({ priority: 'medium' })], now + 18.5 * H).out.map(x => x.kind)], [0, ['ticketDueSoon']]);
t('due soon: ke PIC saja', p.recipientsFor('ticketDueSoon', sla({}), users), ['ot1']);
t('due soon tanpa PIC: ke tim OT + owner', p.recipientsFor('ticketDueSoon', sla({ assigneeUid: '' }), users), ['vWZZ3jz1LTdzrU5c5Q1Gzco8nh72', 'ot1', 'khl']);
t('overdue: PIC + tim OT + owner (bukan yang hanya view)', p.recipientsFor('ticketOverdue', sla({}), users), ['vWZZ3jz1LTdzrU5c5Q1Gzco8nh72', 'ot1', 'khl']);
const mo = p.composePush('ticketOverdue', 'a', sla({ lateMs: 5 * H }));
t('isi overdue', [mo.title, /PIC Kautsar/.test(mo.body), mo.tag], ['Ticket overdue by 5 h: GPS hilang', true, 'tk-sla-a']);
t('isi due soon', p.composePush('ticketDueSoon', 'a', sla({ leftMs: 45 * 60000 })).title, 'Ticket due in 45 min: GPS hilang');
let pass = 0;
T.forEach(x => { if (x.pass) pass++; console.log(`${x.pass ? 'PASS' : 'FAIL'}  ${x.name}${x.pass ? '' : `\n        dapat=${JSON.stringify(x.g)} harap=${JSON.stringify(x.w)}`}`); });
console.log(`\n${pass}/${T.length} lulus`);
process.exit(pass === T.length ? 0 : 1);
