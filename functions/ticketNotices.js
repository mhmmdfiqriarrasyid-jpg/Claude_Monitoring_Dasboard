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

// ---- SLA escalation (2.35.0) ----
// High 4 h, Medium 24 h, Low 72 h from when the ticket was reported (the same
// hours as the dashboard). While a ticket is not resolved:
//   due soon  — the last quarter of its time: its PIC (the OT team if it has none), once
//   overdue   — past its time: its PIC and the OT team, then again every 24 h
// prev: { id: [stage, lastAt] } with stage 1 = due soon sent, 2 = overdue sent.
const SLA_HOURS = { high: 4, medium: 24, low: 72 };
const SLA_REPEAT_MS = 24 * 3600000;
const SLA_MAX_REPEATS_MS = 14 * 86400000;   // a ticket two weeks late stops nagging
const OPEN = ['open', 'assigned', 'progress'];
function ticketDueAt(t) { return (t.reportedAt || 0) + (SLA_HOURS[t.priority] || SLA_HOURS.medium) * 3600000; }
function ticketSlaPushes(prev, list, now) {
    const first = !prev;
    const next = {}, out = [];
    (list || []).forEach(t => {
        if (!OPEN.includes(t.status) || !t.reportedAt) return;
        const due = ticketDueAt(t), span = due - t.reportedAt;
        const p = (prev || {})[t.id] || [0, 0];
        let stage = p[0], lastAt = p[1];
        if (now > due) {
            const late = now - due;
            if (first) { stage = 2; lastAt = now; }
            else if (stage < 2 || (now - lastAt >= SLA_REPEAT_MS && late <= SLA_MAX_REPEATS_MS)) {
                out.push({ kind: 'ticketOverdue', id: t.id, rec: { ...t, lateMs: late } });
                stage = 2; lastAt = now;
            }
        } else if (now >= due - span / 4) {
            if (first) { stage = 1; lastAt = now; }
            else if (stage < 1) { out.push({ kind: 'ticketDueSoon', id: t.id, rec: { ...t, leftMs: due - now } }); stage = 1; lastAt = now; }
        }
        if (stage) next[t.id] = [stage, lastAt];
    });
    return { out, next };
}

module.exports = { ticketPushes, ticketSlaPushes, ticketDueAt, SLA_HOURS, KEEP_DAYS };
