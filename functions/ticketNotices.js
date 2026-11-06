// Tickets (2.30.0) — which pushes a tick sends, worked out from what it saw
// last time. Pure: tests/ticket_notices_test.js.
//   new ticket            → the OT team (tickets 'edit'), not its reporter
//   PIC set or changed    → that PIC
//   status changed        → the reporter, unless the reporter changed it
'use strict';

const KEEP_DAYS = 30;

// prev: { id: [status, assigneeUid, updatedAt] } from the last tick, or null
// on the very first run (which only takes note of what exists).
function ticketPushes(prev, list, now) {
    const first = !prev;
    const next = {};
    Object.entries(prev || {}).forEach(([id, e]) => { if ((e[2] || 0) >= now - KEEP_DAYS * 86400000) next[id] = e; });
    const out = [];
    (list || []).forEach(t => {
        const p = (prev || {})[t.id];
        next[t.id] = [t.status || '', t.assigneeUid || '', t.updatedAt || 0];
        if (first) return;
        if (!p) {
            out.push({ kind: 'ticketNew', id: t.id, rec: t });
            if (t.assigneeUid) out.push({ kind: 'ticketAssign', id: t.id, rec: t });
            return;
        }
        if (t.assigneeUid && t.assigneeUid !== p[1]) out.push({ kind: 'ticketAssign', id: t.id, rec: t });
        if ((t.status || '') !== p[0] && t.lastActorUid !== t.reportedByUid) out.push({ kind: 'ticketStatus', id: t.id, rec: t });
    });
    return { out, next };
}

module.exports = { ticketPushes, KEEP_DAYS };
