// 2.45.0 — functions/index.js against an in-memory Firestore (firebase-* and nodemailer stubbed):
//   · the hand-sent License Alert is limited (10 min for everybody, 3 a day per account, 10 a day;
//     the owner is not held) and refuses with 'resource-exhausted' and a message the app shows
//   · push: an account's newest 5 tokens only (older ones removed), each token once, batches of
//     at most 500, and one batch that fails does not stop the others
//   · the manager's Daily Report still goes with a malformed ticket / project, and without the
//     PowerPoint / PDF when those cannot be made.
// Node only (needs `npm install` in functions/ for the daily deck).
const Module = require('module');
const path = require('path');
const T = []; const t = (name, g, w) => T.push({ name, g, w, pass: JSON.stringify(g) === JSON.stringify(w) });

// ---- In-memory Firestore ----
const store = new Map();
let autoId = 0;
const clone = v => v === undefined ? undefined : JSON.parse(JSON.stringify(v));
function docRef(p) {
    return { path: p, id: p.split('/').pop(),
        get: async () => ({ exists: store.has(p), id: p.split('/').pop(), data: () => clone(store.get(p)) }),
        set: async (d, o) => { store.set(p, o && o.merge ? Object.assign({}, store.get(p), clone(d)) : clone(d)); },
        delete: async () => { store.delete(p); } };
}
function query(col, filters) {
    const test = (x, op, v) => op === '==' ? x === v : op === '>' ? x > v : op === '>=' ? x >= v : op === 'in' ? v.includes(x) : true;
    return { where: (f, op, v) => query(col, filters.concat([[f, op, v]])), orderBy: () => query(col, filters), limit: () => query(col, filters),
        get: async () => { const docs = [...store.keys()].filter(k => k.startsWith(col + '/') && k.split('/').length === 2)
            .map(k => ({ id: k.split('/')[1], data: () => clone(store.get(k)) })).filter(d => filters.every(([f, op, v]) => test(d.data()[f], op, v)));
        return { docs, empty: !docs.length }; },
        add: async d => { const r = docRef(`${col}/auto${++autoId}`); await r.set(d); return r; },
        doc: id => docRef(`${col}/${id}`) };
}
let txLock = Promise.resolve();
const db = { collection: c => query(c, []), doc: p => docRef(p),
    batch: () => { const ops = []; return { delete: r => ops.push(r), commit: async () => { for (const r of ops) await r.delete(); } }; },
    // Transactions run one after the other, as Firestore makes them behave.
    runTransaction: fn => { const run = txLock.then(() => fn({ get: r => r.get(), set: (r, d) => r.set(d) })); txLock = run.catch(() => {}); return run; } };

// ---- Stubs ----
class HttpsError extends Error { constructor(code, message) { super(message); this.code = code; } }
const logs = [], mails = [];
let mailFail = false;   // the next sendMail throws (Gmail down)
const fcm = { calls: [], failNext: false };
const admin = {
    initializeApp() {}, firestore: Object.assign(() => db, { FieldValue: { serverTimestamp: () => 'ts' }, Timestamp: {} }),
    storage: () => ({ bucket: () => ({ file: () => ({ download: async () => { throw new Error('no file'); } }), getFiles: async () => [[]] }) }),
    auth: () => ({}),
    messaging: () => ({ sendEachForMulticast: async ({ tokens }) => {
        if (tokens.length > 500) throw Object.assign(new Error('tokens list must not contain more than 500 items'), { code: 'messaging/invalid-argument' });
        fcm.calls.push(tokens.slice());
        if (fcm.failNext) { fcm.failNext = false; throw new Error('FCM unavailable'); }
        const responses = tokens.map(x => /^bad/.test(x) ? { success: false, error: { code: 'messaging/registration-token-not-registered' } } : { success: true });
        return { responses, successCount: responses.filter(r => r.success).length };
    } })
};
const STUBS = {
    'firebase-functions/v2/scheduler': { onSchedule: (o, fn) => fn },
    'firebase-functions/v2/https': { onCall: (o, fn) => fn, HttpsError },
    'firebase-functions/params': { defineSecret: n => ({ value: () => n === 'GMAIL_USER' ? 'sender@example.com' : 'app pass' }) },
    'firebase-functions/logger': { info: () => {}, warn: (...a) => logs.push(['warn', ...a]), error: (...a) => logs.push(['error', ...a]) },
    'firebase-admin': admin,
    nodemailer: { createTransport: () => ({ sendMail: async m => { if (mailFail) { mailFail = false; throw new Error('SMTP unavailable'); } mails.push(m); } }) }
};
const realLoad = Module._load;
Module._load = function (req) { return Object.prototype.hasOwnProperty.call(STUBS, req) ? STUBS[req] : realLoad.apply(this, arguments); };
// The deck writer, made to fail on demand (dailySend takes it when it is first loaded).
const deck = require(path.join(__dirname, '..', 'functions', 'dailyDeck.js'));
const realPptx = deck.renderPptx; let deckFails = false;
deck.renderPptx = async (...a) => { if (deckFails) throw new Error('deck broke'); return realPptx(...a); };
const fx = require(path.join(__dirname, '..', 'functions', 'index.js'));

const reset = () => { store.clear(); logs.length = 0; mails.length = 0; fcm.calls.length = 0; };
const put = (p, d) => store.set(p, clone(d));
const caught = async p => { try { return ['ok', await p]; } catch (e) { return [e.code || 'error', e.message]; } };
const realNow = Date.now;
// A fixed mid-day WIT start: the sends below move the clock on by minutes, and a start just
// before midnight made the day's count restart halfway (2.45.1).
let clock = Date.parse('2026-11-10T09:00:00+09:00');
const dayAhead = n => { const d = new Date(clock + n * 86400000), p = x => String(x).padStart(2, '0'); return `${d.getFullYear()}-${p(d.getMonth() + 1)}-${p(d.getDate())}`; };
const call = uid => fx.sendLicenseAlert({ auth: { uid, token: { email: `${uid}@example.com` } }, data: {} });

(async () => {
    // ---------- S11: License Alert sent by hand ----------
    reset();
    put('settings/email', { recipients: ['boss@example.com', 'lead@example.com'] });
    put('units/u1', { name: 'JD-01', gpsLicense: 'SF-RTK', gpsLicenseEndDate: dayAhead(5) });
    put('users/boss', { status: 'active', role: 'owner' });
    for (let i = 1; i <= 12; i++) put(`users/v${i}`, { status: 'active', role: 'staff', access: { licenseStock: 'view' } });
    put('users/nobody', { status: 'active', role: 'staff', access: {} });
    Date.now = () => clock;
    t('lisensi: pembaca License Stock mengirim', (await caught(call('v1')))[1].sent, true);
    const again = await caught(call('v2'));
    t('lisensi: langsung lagi (akun lain) ditolak resource-exhausted + pesan', again, ['resource-exhausted', 'The License Alert was sent less than 10 minutes ago — try again in 10 min']);
    clock += 11 * 60000;
    const burst = await Promise.all(['v3', 'v4', 'v5', 'v6', 'v7', 'v8', 'v9', 'v10'].map(u => caught(call(u))));
    t('lisensi: 8 tekan bersamaan → hanya 1 email', [burst.filter(r => r[0] === 'ok').length, burst.filter(r => r[0] === 'resource-exhausted').length, mails.length], [1, 7, 2]);
    t('lisensi: owner tidak ditahan jeda', [(await caught(call('boss')))[0], mails.length], ['ok', 3]);
    t('lisensi: tercatat di mailState/licenseManual', [store.get('mailState/licenseManual').total, Object.keys(store.get('mailState/licenseManual').byUid).sort()], [3, ['boss', 'v1', 'v3']]);
    put('settings/email', { recipients: [] });
    clock += 11 * 60000;
    t('lisensi: tanpa penerima → tidak memakai giliran', [(await caught(call('v11')))[1], store.get('mailState/licenseManual').total], [{ sent: false, reason: 'no-recipients' }, 3]);
    put('settings/email', { recipients: ['boss@example.com'] });
    t('lisensi: akun tanpa akses tetap ditolak', (await caught(call('nobody')))[0], 'permission-denied');
    t('lisensi: tiap kirim tercatat siapa', [...store.entries()].filter(([k, v]) => k.startsWith('mailLog/') && v.kind === 'licenseAlert').map(([, v]) => v.by).sort(), ['boss@example.com', 'v1@example.com', 'v3@example.com']);
    // Gmail fails: nothing went out, so the turn goes back and the next press is not refused.
    clock += 11 * 60000;
    mailFail = true;
    const failed = await caught(call('v11'));
    const lm1 = store.get('mailState/licenseManual');
    const retried = await caught(call('v12'));
    const lm2 = store.get('mailState/licenseManual');
    t('lisensi: email gagal terkirim → giliran dikembalikan, kiriman berikutnya diterima',
        [failed[0] !== 'ok', lm1.total, (lm1.byUid || {}).v11 || 0, retried[0], lm2.total, mails.length], [true, 3, 0, 'ok', 4, 4]);
    Date.now = realNow;

    // ---------- S12: push tokens ----------
    reset();
    const now = realNow();
    put('settings/email', {});
    put('mailState/damage', { ids: [] });
    put('mailState/approvals', { ids: [] });
    put('users/boss', { status: 'active', role: 'owner' });
    put('users/flood', { status: 'active', role: 'staff', access: { damage: 'view' } });
    for (let i = 0; i < 120; i++) put(`users/w${i}`, { status: 'active', role: 'staff', access: { damage: 'view' } });
    ['own1', 'own2', 'bad1'].forEach((x, i) => put(`pushTokens/${x}`, { token: x, uid: 'boss', createdAt: now - i }));
    for (let i = 0; i < 600; i++) put(`pushTokens/junk${i}`, { token: `junk${i}`, uid: 'flood', createdAt: i });
    for (let i = 0; i < 120; i++) for (let k = 0; k < 6; k++) put(`pushTokens/w${i}t${k}`, { token: `w${i}t${k}`, uid: `w${i}`, createdAt: 1000 + k });
    put('damageRecords/d1', { unitName: 'BD025G', damageType: 'Mekanis', createdAt: now, updatedAt: now });
    await fx.notificationTick();
    const sent1 = fcm.calls.flat();
    t('push: per panggilan paling banyak 500 token', fcm.calls.map(c => c.length), [500, 108]);
    t('push: tiap token sekali', sent1.length === new Set(sent1).size, true);
    t('push: akun banjir token hanya 5 terbaru', sent1.filter(x => /^junk/.test(x)).sort(), ['junk595', 'junk596', 'junk597', 'junk598', 'junk599']);
    t('push: perangkat owner tetap menerima', ['own1', 'own2'].every(x => sent1.includes(x)), true);
    t('push: token yang ditolak FCM dihapus; token lama dihapus (450 per putaran)', [store.has('pushTokens/bad1'), [...store.keys()].filter(k => /^pushTokens\/junk/.test(k)).length], [false, 150]);
    // Next damage: the first batch fails at FCM — the second still goes out.
    put('damageRecords/d2', { unitName: 'BD026G', damageType: 'Mekanis', createdAt: realNow(), updatedAt: realNow() });
    fcm.calls.length = 0; fcm.failNext = true;
    await fx.notificationTick();
    t('push: satu batch gagal, batch lain tetap terkirim', [fcm.calls.map(c => c.length), logs.some(l => l[1] === 'push batch failed')], [[500, 107], true]);
    t('push: sisa token lama dibersihkan', [...store.keys()].filter(k => /^pushTokens\//.test(k)).length, 2 + 5 + 120 * 5);

    // A token whose createdAt is a map with a toString key (Number() of it throws): every push stopped.
    reset();
    put('settings/email', {});
    put('mailState/damage', { ids: [] });
    put('mailState/approvals', { ids: [] });
    put('users/boss', { status: 'active', role: 'owner' });
    put('users/v1', { status: 'active', role: 'staff', access: { damage: 'view' } });
    put('pushTokens/own1', { token: 'own1', uid: 'boss', createdAt: realNow() });
    put('pushTokens/j1', { token: 'j1', uid: 'v1', ua: '', createdAt: { toString: 'x' } });
    put('pushTokens/j2', { token: 'j2', uid: 'v1', ua: '', createdAt: 1 });
    put('damageRecords/d3', { unitName: 'BD027G', damageType: 'Mekanis', createdAt: realNow(), updatedAt: realNow() });
    await fx.notificationTick();
    t('push: createdAt {toString} di satu token → push tetap terkirim ke semua',
        [fcm.calls.flat().sort(), (store.get('mailState/damage').ids || []).includes('d3')], [['j1', 'j2', 'own1'], true]);
    // Approvals with a submittedAt map: the scan used to throw on the comparison.
    put('workLogs/wl9', { approval: 'pending', memberName: 'Budi', date: '2026-01-01', submittedAt: { toString: 'x' } });
    const r9 = await caught(fx.notificationTick());
    t('push: submittedAt {toString} tidak menghentikan tick', r9[0], 'ok');

    // ---------- S10: manager's Daily Report email ----------
    reset();
    const D = new Date(realNow() - 86400000), p2 = x => String(x).padStart(2, '0');
    const day = `${D.getFullYear()}-${p2(D.getMonth() + 1)}-${p2(D.getDate())}`, at9 = new Date(`${day}T09:00:00`).getTime();
    put('users/boss', { status: 'active', role: 'owner' });
    put('settings/email', { mgrRecipients: ['mgr@example.com'] });
    put('teamMembers/m1', { name: 'Budi', company: 'PT. Global Papua Abadi' });
    put('workLogs/wl1', { date: day, memberId: 'm1', memberName: 'Budi', start: '07:00', end: '15:00', jobs: [null, { paddock: 'P-1', jobType: 'Tillage', units: 'x' }] });
    put('tickets/k1', { no: 'TK-1', title: 'Display mati', priority: 'high', status: 'open', reportedAt: at9, events: [null], units: 5 });
    put('projects/p1', { name: 'RTK Tower', status: 'progress', target: { mode: 'units', units: 'x' } });
    put('projects/p2', { name: 'Depth', status: 'progress', updates: [null, { date: day, text: 'Sensor dipasang', progress: 'progress 0% → 10%' }] });
    // Times and numbers written as maps with a toString key (they throw in any sort or sum).
    put('tickets/k2', { no: 'TK-2', title: 'Waktu rusak', priority: 'low', status: 'open', reportedAt: { toString: 'x' },
        events: [{ type: 'resolved', at: { toString: 'x' } }, { type: 'resolved', at: 1 }] });
    put('projects/p3', { name: 'Bad time', status: 'progress', updatedAt: { toString: 'x' }, updates: [{ at: { toString: 'x' }, date: day, text: 'Tower naik' }],
        target: { mode: 'milestones', milestones: [{ label: 'A', weight: { toString: 'x' }, done: true }] } });
    put('otJobs/j1', { date: day, category: 'Install', description: 'Pasang GPS', status: 'done', hours: { toString: 'x' }, updatedAt: { toString: 'x' } });
    const owner = { auth: { uid: 'boss', token: { email: 'boss@example.com' } } };
    const r1 = await caught(fx.sendNotificationNow(Object.assign({ data: { kind: 'mgrDaily' } }, owner)));
    t('daily report: ticket events [null], units 5, jobs [null], target.units bukan daftar → email tetap terkirim lengkap',
        [r1[0], mails.length, (mails[0].attachments || []).length, /Display mati/.test(mails[0].html), /Sensor dipasang/.test(mails[0].html), /Left out/.test(mails[0].html)], ['ok', 1, 2, true, true, false]);
    deckFails = true;
    const r2 = await caught(fx.sendNotificationNow(Object.assign({ data: { kind: 'mgrDaily' } }, owner)));
    deckFails = false;
    t('daily report: PowerPoint/PDF gagal dibuat → email tetap terkirim tanpa lampiran, disebutkan',
        [r2[0], mails.length, mails[1].attachments, /Left out of this report \(could not be built\): PowerPoint \/ PDF/.test(mails[1].html), logs.some(l => l[1] === 'daily report: parts left out' && l[2].fileError === 'deck broke')],
        ['ok', 2, undefined, true, true]);

    let pass = 0;
    T.forEach(x => { if (x.pass) pass++; console.log(`${x.pass ? 'PASS' : 'FAIL'}  ${x.name}${x.pass ? '' : `\n        dapat=${JSON.stringify(x.g)} harap=${JSON.stringify(x.w)}`}`); });
    console.log(`\n${pass}/${T.length} lulus`);
    process.exit(pass === T.length ? 0 : 1);
})().catch(e => { console.error(e); process.exit(1); });
