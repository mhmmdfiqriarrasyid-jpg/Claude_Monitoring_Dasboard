// Push notifications (2.18.0) — the pure parts: who may receive a notice, and
// what it says. Same access rules as firestore.rules. tests/push_notices_test.js.
'use strict';

const OWNER_UIDS = ['vWZZ3jz1LTdzrU5c5Q1Gzco8nh72'];

function lvlOf(d, area) {
    if (!d || d.status !== 'active') return 'none';
    const role = d.role || '';
    if (role === 'owner') return 'edit';
    const a = d.access && typeof d.access === 'object' ? d.access : {};
    const given = a[area] != null ? a[area] : null;
    const raw = given != null ? given : (role === 'team' ? 'edit' : 'none');
    const sc0 = a.unitScope == null ? null : a.unitScope;
    const sc = sc0 == null ? 'all' : (['all', 'tractor', 'heavy'].includes(sc0) ? sc0 : 'none');
    if (sc === 'all' || raw === 'none') return raw;
    if (['implements', 'licenseStock'].includes(area) && sc !== 'tractor') return 'none';
    if (['inspection', 'inspectionApprove'].includes(area) && sc !== 'heavy') return 'none';
    if (['editUnits', 'damage'].includes(area) && raw === 'edit') return 'view';
    return raw;
}
const canView = (d, area) => ['view', 'edit'].includes(lvlOf(d, area));
const groupOk = (d, g) => {
    const a = d && d.access && typeof d.access === 'object' ? d.access : {};
    const sc = d && d.role === 'owner' || a.unitScope == null ? 'all' : a.unitScope;
    return sc === 'all' || !g || sc === g;
};

// users: [{ uid, ...userDoc }]. Returns the uids a notice goes to.
function recipientsFor(kind, rec, users) {
    return (users || []).filter(u => {
        const owner = OWNER_UIDS.includes(u.uid) || (u.status === 'active' && u.role === 'owner');
        if (!owner && u.status !== 'active') return false;
        if (kind === 'damage') return owner || (canView(u, 'damage') && groupOk(u, rec.unitGroup));
        // 2.30.0: tickets — not to whoever caused the notice.
        if (kind === 'ticketNew') return rec.reportedByUid !== u.uid && (owner || lvlOf(u, 'tickets') === 'edit');
        if (kind === 'ticketAssign') return rec.assigneeUid === u.uid && rec.lastActorUid !== u.uid;
        if (kind === 'ticketStatus') return rec.reportedByUid === u.uid;
        // 2.35.0: SLA — due soon to the PIC (the OT team when nobody holds it); overdue to the PIC and the OT team.
        if (kind === 'ticketDueSoon') return rec.assigneeUid ? rec.assigneeUid === u.uid : (owner || lvlOf(u, 'tickets') === 'edit');
        if (kind === 'ticketOverdue') return rec.assigneeUid === u.uid || owner || lvlOf(u, 'tickets') === 'edit';
        // Nobody is asked to approve their own work.
        if (rec.createdByUid === u.uid || rec.submittedByUid === u.uid) return false;
        if (kind === 'workLog' || kind === 'leave') return owner || lvlOf(u, 'teamLogApprove') === 'edit';
        if (kind === 'inspection') return owner || lvlOf(u, 'inspectionApprove') === 'edit';
        return false;
    }).map(u => u.uid);
}

const cut = (s, n) => { s = String(s || ''); return s.length > n ? s.slice(0, n - 1) + '…' : s; };
// 2.45.0: a ticket's units as users wrote them (not always a list of records).
const unitList = us => (Array.isArray(us) ? us : []).filter(u => u && typeof u === 'object').map(u => u.name).filter(Boolean).join(', ');
function composePush(kind, id, r) {
    if (kind === 'damage') {
        const what = [r.damageType, r.component].filter(Boolean).join(' / ') || 'Damage';
        return { title: `${r.drove ? 'Breakdown' : 'Damage reported'}: ${r.unitName || r.sn || 'unit'}`,
                 body: cut(`${what}${r.description ? ' — ' + r.description : ''}`, 160), url: './?action=damage', tag: `dmg-${id}` };
    }
    const TK = { high: 'High', medium: 'Medium', low: 'Low' };
    const ST = { open: 'reopened', assigned: 'assigned', progress: 'in progress', resolved: 'resolved — please confirm', closed: 'closed' };
    const tkUrl = `./?action=tickets&ticket=${encodeURIComponent(id)}`;
    if (kind === 'ticketNew') return { title: `New ticket · ${TK[r.priority] || 'Medium'}: ${cut(r.title, 60)}`, body: cut(`${r.category || ''}${unitList(r.units) ? ' · ' + unitList(r.units) : ''}${r.location ? ' · ' + r.location : ''} — ${r.reportedBy || ''}`, 160), url: tkUrl, tag: `tk-${id}` };
    if (kind === 'ticketAssign') return { title: `Ticket for you: ${cut(r.title, 70)}`, body: cut(`${r.no || ''} · ${TK[r.priority] || ''} priority${r.location ? ' · ' + r.location : ''}`, 160), url: tkUrl, tag: `tk-${id}` };
    if (kind === 'ticketStatus') return { title: `Your ticket is ${ST[r.status] || r.status}`, body: cut(`${r.no || ''} · ${r.title}${r.status === 'resolved' && r.resolution ? ' — ' + r.resolution : ''}`, 160), url: tkUrl, tag: `tk-${id}` };
    const span = ms => { const h = Math.max(0, ms || 0) / 3600000; return h >= 48 ? `${Math.floor(h / 24)} days` : h >= 1 ? `${Math.floor(h)} h` : `${Math.max(1, Math.round(h * 60))} min`; };
    if (kind === 'ticketDueSoon') return { title: `Ticket due in ${span(r.leftMs)}: ${cut(r.title, 60)}`, body: cut(`${r.no || ''} · ${TK[r.priority] || 'Medium'} priority · ${r.assigneeName ? 'PIC ' + r.assigneeName : 'no PIC yet'}`, 160), url: tkUrl, tag: `tk-sla-${id}` };
    if (kind === 'ticketOverdue') return { title: `Ticket overdue by ${span(r.lateMs)}: ${cut(r.title, 55)}`, body: cut(`${r.no || ''} · ${TK[r.priority] || 'Medium'} priority · ${r.assigneeName ? 'PIC ' + r.assigneeName : 'no PIC yet'}${unitList(r.units) ? ' · ' + unitList(r.units) : ''}`, 160), url: tkUrl, tag: `tk-sla-${id}` };
    if (kind === 'workLog') return { title: 'Daily Report to check', body: cut(`${r.memberName || 'Someone'} · ${r.date || ''}`, 160), url: './?action=inbox', tag: `wl-${id}` };
    if (kind === 'leave') return { title: 'Leave request to check', body: cut(`${r.memberName || 'Someone'} · ${r.dateFrom || ''}${r.dateTo && r.dateTo !== r.dateFrom ? ' – ' + r.dateTo : ''}`, 160), url: './?action=inbox', tag: `lv-${id}` };
    if (kind === 'inspection') return { title: 'Inspection to check', body: cut(`${r.unitName || 'Unit'} · ${r.date || ''}`, 160), url: './?action=inbox', tag: `ins-${id}` };
    return null;
}

// ---- Device tokens (2.45.0) ----
// The rules let an account write any number of pushTokens docs, and FCM refuses a call with more
// than 500 tokens — one account with 501 junk tokens used to stop a notice for everybody. So an
// account's newest PUSH_PER_ACCOUNT tokens are used and the older ones are handed back (extra) for
// the caller to remove; a notice goes out in batches of at most FCM_BATCH tokens.
const PUSH_PER_ACCOUNT = 5, FCM_BATCH = 500;
// docs: [{ id, uid, createdAt }] (id = the token). Returns { byUid: Map uid → [token], extra: [token] }.
function tokensByAccount(docs) {
    const all = new Map();
    (docs || []).forEach(d => {
        if (!d || typeof d.id !== 'string' || !d.id || typeof d.uid !== 'string' || !d.uid) return;
        if (!all.has(d.uid)) all.set(d.uid, []);
        all.get(d.uid).push(d);
    });
    // createdAt comes from the client: anything but a finite number counts as oldest (a map with a
    // toString key would make Number() throw and stop every notice).
    const ts = v => (typeof v === 'number' && isFinite(v) ? v : 0);
    const byUid = new Map(), extra = [];
    all.forEach((list, uid) => {
        list.sort((a, b) => ts(b.createdAt) - ts(a.createdAt));
        byUid.set(uid, list.slice(0, PUSH_PER_ACCOUNT).map(d => d.id));
        list.slice(PUSH_PER_ACCOUNT).forEach(d => extra.push(d.id));
    });
    return { byUid, extra };
}
// The tokens of these accounts, each once, in batches FCM accepts.
function pushBatches(uids, byUid, size) {
    const seen = new Set(), all = [];
    (uids || []).forEach(uid => ((byUid && byUid.get(uid)) || []).forEach(t => { if (!seen.has(t)) { seen.add(t); all.push(t); } }));
    const n = size || FCM_BATCH, out = [];
    for (let i = 0; i < all.length; i += n) out.push(all.slice(i, i + n));
    return out;
}

module.exports = { lvlOf, recipientsFor, composePush, tokensByAccount, pushBatches, PUSH_PER_ACCOUNT, FCM_BATCH };
