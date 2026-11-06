// Daily Report reminder and damage alert — the pure parts, testable without
// Firebase (tests/team_notices_test.js).
'use strict';

// A shift's report is due once the shift has ended (WIT):
//   Morning 07–15 → due from 15:00 the same day
//   Afternoon 15–23 → due from 23:00 the same day
//   Night 23–07 → due from 07:00 the next day
const SHIFT_END = { pagi: { label: 'Morning', end: 15, nextDay: false },
                    siang: { label: 'Afternoon', end: 23, nextDay: false },
                    malam: { label: 'Night', end: 7, nextDay: true } };

const iso = d => { const p = n => String(n).padStart(2, '0'); return `${d.getFullYear()}-${p(d.getMonth() + 1)}-${p(d.getDate())}`; };
function shiftDates(now) {
    const today = new Date(now); const yest = new Date(now); yest.setDate(yest.getDate() - 1);
    return { today: iso(today), yesterday: iso(yest) };
}
const onLeave = (leaves, memberId, date) => (leaves || []).some(r => r.memberId === memberId && r.approval === 'approved'
    && r.dateFrom && date >= r.dateFrom && date <= (r.dateTo || r.dateFrom));

// Who has not sent the report of a shift that has ended by `now`. A draft
// does not count as sent. Members marked inactive, or on approved leave,
// are left out.
function missingReports({ shifts, workLogs, members, leaves, now }) {
    const t = new Date(now);
    const hour = t.getHours();
    const { today, yesterday } = shiftDates(t);
    const memberById = new Map((members || []).map(m => [m.id, m]));
    const sent = new Set((workLogs || []).filter(w => (w.approval || 'pending') !== 'draft').map(w => `${w.memberId}|${w.date}`));
    const out = [];
    (shifts || []).forEach(s => {
        const def = SHIFT_END[s.shift];
        if (!def) return;
        const due = def.nextDay ? (s.date === yesterday && hour >= def.end) : (s.date === today && hour >= def.end);
        if (!due) return;
        const m = memberById.get(s.memberId);
        if (!m || m.active === false) return;
        if (onLeave(leaves, s.memberId, s.date)) return;
        if (sent.has(`${s.memberId}|${s.date}`)) return;
        out.push({ date: s.date, shift: def.label, name: m.name || s.memberName || '-', company: m.company || '' });
    });
    return out.sort((a, b) => a.date.localeCompare(b.date) || a.shift.localeCompare(b.shift) || a.name.localeCompare(b.name));
}

const esc = s => String(s == null ? '' : s).replace(/[&<>"']/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));
const FOOT_TEXT = 'Sent by the Operation Technology Dashboard — https://ot.techorchestrator.cloud';
const FOOT_HTML = '<p style="margin-top:18px;font-size:12px;color:#667085">Sent by the <a href="https://ot.techorchestrator.cloud">Operation Technology Dashboard</a>.</p>';

function composeReminder(list) {
    if (!list.length) return null;
    const head = ['Date', 'Shift', 'Name', 'Company'];
    const rows = list.map(r => [r.date, r.shift, r.name, r.company || '-']);
    const text = [`Daily Reports not sent yet: ${list.length}`, '', head.join(' | '), '—'.repeat(40),
        ...rows.map(r => r.join(' | ')), '', FOOT_TEXT].join('\n');
    const td = 'style="padding:6px 10px;border-bottom:1px solid #EAECF0;font-size:13px"';
    const html = `<div style="font-family:Arial,Helvetica,sans-serif;color:#101828">
<h2 style="margin:0 0 4px;font-size:18px">Daily Reports not sent yet</h2>
<p style="margin:0 0 14px;color:#475467"><strong>${list.length}</strong> shift report(s) are still missing.</p>
<table style="border-collapse:collapse;width:100%"><thead><tr>${head.map(h => `<th align="left" style="padding:6px 10px;background:#F2F4F7;font-size:12px">${esc(h)}</th>`).join('')}</tr></thead>
<tbody>${rows.map(r => `<tr>${r.map(c => `<td ${td}>${esc(c)}</td>`).join('')}</tr>`).join('')}</tbody></table>${FOOT_HTML}</div>`;
    return { subject: `Daily Report reminder: ${list.length} not sent yet`, text, html };
}

// A new damage record, as an email. Records that were saved already repaired
// are not news.
function composeDamage(d) {
    if (!d || d.resolved) return null;
    const bd = !!d.drove;
    const what = [d.damageType, d.component].filter(Boolean).join(' / ') || 'Damage';
    const rows = [['Unit', d.unitName || '-'], ['Serial number', d.sn || '-'], ['Site', d.site || '-'], ['Date', d.date || '-'],
        ['Type', what], ['Description', d.description || '-'], ['Unit set to Breakdown', bd ? 'Yes' : 'No']];
    const subject = `${bd ? 'Breakdown' : 'Damage reported'}: ${d.unitName || d.sn || 'unit'} — ${what}`;
    const text = [subject, '', ...rows.map(([k, v]) => `${k}: ${v}`), '', FOOT_TEXT].join('\n');
    const html = `<div style="font-family:Arial,Helvetica,sans-serif;color:#101828">
<h2 style="margin:0 0 12px;font-size:18px;color:${bd ? '#B42318' : '#101828'}">${esc(bd ? 'Breakdown' : 'Damage reported')}: ${esc(d.unitName || d.sn || 'unit')}</h2>
<table style="border-collapse:collapse">${rows.map(([k, v]) => `<tr><td style="padding:4px 12px 4px 0;color:#475467;font-size:13px;vertical-align:top">${esc(k)}</td><td style="padding:4px 0;font-size:13px">${esc(v)}</td></tr>`).join('')}</table>${FOOT_HTML}</div>`;
    return { subject, text, html };
}

module.exports = { missingReports, composeReminder, composeDamage, shiftDates };
