// Monthly summary for management (2.18.0) — pure: give it the data and the
// month, it returns the numbers and the email. tests/monthly_report_test.js.
'use strict';
const { buildAlertList } = require('./licenseReport');

const pad = n => String(n).padStart(2, '0');
// The calendar month before `now` (local time = WIT on the server).
function previousMonth(now) {
    const d = new Date(now);
    const start = new Date(d.getFullYear(), d.getMonth() - 1, 1);
    const end = new Date(d.getFullYear(), d.getMonth(), 1);
    return { start: start.getTime(), end: end.getTime(), from: `${start.getFullYear()}-${pad(start.getMonth() + 1)}-01`,
             to: `${end.getFullYear()}-${pad(end.getMonth() + 1)}-01`,
             label: start.toLocaleDateString('en-GB', { month: 'long', year: 'numeric' }) };
}
const inMonth = (date, m) => typeof date === 'string' && date >= m.from && date < m.to;
const parseHHMM = v => { const x = /^(\d{1,2}):(\d{2})/.exec(String(v || '')); return x ? (+x[1]) * 60 + (+x[2]) : null; };
function workMinutes(w) {
    const s = parseHHMM(w && w.start), e = parseHHMM(w && w.end);
    if (s == null || e == null) return 0;
    const d = e - s; return d < 0 ? d + 1440 : d;
}
const isHeavy = u => ['heavy', 'heavy equipment', 'alat berat'].includes(String(u && u.unitGroup || '').trim().toLowerCase());
const isBroken = v => String(v || '').trim().toLowerCase() === 'breakdown';

// Hours a unit was down inside the month: closed intervals, plus a breakdown
// still open, cut to the month.
function downtimeMs(u, m, now) {
    let ms = 0;
    const clip = (a, b) => { const s = Math.max(a, m.start), e = Math.min(b, m.end); return e > s ? e - s : 0; };
    (Array.isArray(u.downtimeHistory) ? u.downtimeHistory : []).forEach(iv => {
        if (iv && typeof iv.start === 'number' && typeof iv.end === 'number') ms += clip(iv.start, iv.end);
    });
    if (isBroken(u.status) && typeof u.breakdownStartedAt === 'number') ms += clip(u.breakdownStartedAt, Math.min(now, m.end));
    return ms;
}

function buildMonthly({ units, damages, workLogs, licenseStock, now }) {
    const m = previousMonth(now);
    units = units || []; damages = damages || []; workLogs = workLogs || [];
    const nameOf = id => { const u = units.find(x => x.id === id); return u ? (u.name || u.sn || id) : ''; };

    const dmg = damages.filter(d => inMonth(d.date, m));
    const byType = {};
    dmg.forEach(d => { const t = d.damageType || 'Other'; byType[t] = (byType[t] || 0) + 1; });
    const byUnit = {};
    dmg.forEach(d => { const k = nameOf(d.unitId) || d.unitName || d.sn || '-'; byUnit[k] = (byUnit[k] || 0) + 1; });

    const down = units.map(u => ({ name: u.name || u.sn || u.id, hours: downtimeMs(u, m, now) / 3600000 }))
        .filter(x => x.hours > 0).sort((a, b) => b.hours - a.hours);

    const logs = workLogs.filter(w => inMonth(w.date, m) && (w.approval || 'pending') !== 'draft');
    const byCompany = {};
    logs.forEach(w => { const c = w.company || '(no company)'; byCompany[c] = (byCompany[c] || 0) + workMinutes(w); });

    const lic = buildAlertList(units, licenseStock || [], { soonOnly: true, now });
    return {
        month: m.label,
        fleet: { total: units.length, heavy: units.filter(isHeavy).length,
                 breakdownNow: units.filter(u => isBroken(u.status)).length },
        damage: { total: dmg.length, resolved: dmg.filter(d => d.resolved).length,
                  byType: Object.entries(byType).sort((a, b) => b[1] - a[1]),
                  topUnits: Object.entries(byUnit).sort((a, b) => b[1] - a[1] || a[0].localeCompare(b[0])).slice(0, 5) },
        downtime: { totalHours: down.reduce((n, x) => n + x.hours, 0), topUnits: down.slice(0, 5) },
        reports: { total: logs.length, approved: logs.filter(w => w.approval === 'approved').length,
                   office: logs.filter(w => w.place === 'office').length,
                   hours: logs.reduce((n, w) => n + workMinutes(w), 0) / 60,
                   byCompany: Object.entries(byCompany).map(([c, min]) => [c, min / 60]).sort((a, b) => b[1] - a[1]) },
        licenses: { expiringSoon: lic.soonCount }
    };
}

const esc = s => String(s == null ? '' : s).replace(/[&<>"']/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));
const h1 = n => (Math.round(n * 10) / 10).toLocaleString('en-US');

function composeMonthly(r) {
    const lines = [
        `Monthly Report — ${r.month}`, '',
        `FLEET: ${r.fleet.total} units (${r.fleet.heavy} heavy equipment) · ${r.fleet.breakdownNow} in Breakdown now`,
        `DAMAGE: ${r.damage.total} recorded, ${r.damage.resolved} of them repaired`,
        ...r.damage.byType.map(([t, n]) => `  ${t}: ${n}`),
        ...(r.damage.topUnits.length ? ['  Most damage: ' + r.damage.topUnits.map(([u, n]) => `${u} (${n})`).join(', ')] : []),
        `DOWNTIME: ${h1(r.downtime.totalHours)} hours in total`,
        ...r.downtime.topUnits.map(x => `  ${x.name}: ${h1(x.hours)} h`),
        `DAILY REPORTS: ${r.reports.total} sent (${r.reports.approved} approved, ${r.reports.office} office) · ${h1(r.reports.hours)} work hours`,
        ...r.reports.byCompany.map(([c, h]) => `  ${c}: ${h1(h)} h`),
        `LICENSES: ${r.licenses.expiringSoon} expiring in the next 30 days`,
        '', 'Sent by the Operation Technology Dashboard — https://ot.techorchestrator.cloud'
    ];
    const tile = (label, value, sub) => `<td style="padding:12px 14px;border:1px solid #EAECF0;border-radius:10px;vertical-align:top">
<div style="font-size:12px;color:#475467">${esc(label)}</div><div style="font-size:22px;font-weight:700;margin:2px 0">${esc(value)}</div><div style="font-size:12px;color:#667085">${esc(sub)}</div></td>`;
    const list = (title, rows) => rows.length ? `<h3 style="margin:18px 0 6px;font-size:15px">${esc(title)}</h3>
<table style="border-collapse:collapse;width:100%">${rows.map(([a, b]) => `<tr><td style="padding:5px 10px;border-bottom:1px solid #EAECF0;font-size:13px">${esc(a)}</td><td align="right" style="padding:5px 10px;border-bottom:1px solid #EAECF0;font-size:13px">${esc(b)}</td></tr>`).join('')}</table>` : '';
    const html = `<div style="font-family:Arial,Helvetica,sans-serif;color:#101828">
<h2 style="margin:0 0 12px;font-size:20px">Monthly Report — ${esc(r.month)}</h2>
<table style="border-collapse:separate;border-spacing:8px;width:100%"><tr>
${tile('Fleet', r.fleet.total, `${r.fleet.breakdownNow} in Breakdown now`)}
${tile('Damage recorded', r.damage.total, `${r.damage.resolved} repaired`)}
${tile('Downtime', `${h1(r.downtime.totalHours)} h`, 'all units')}
${tile('Daily Reports', r.reports.total, `${h1(r.reports.hours)} work hours`)}
${tile('Licenses', r.licenses.expiringSoon, 'expiring in 30 days')}</tr></table>
${list('Damage by type', r.damage.byType)}
${list('Units with the most damage', r.damage.topUnits)}
${list('Longest downtime (hours)', r.downtime.topUnits.map(x => [x.name, h1(x.hours)]))}
${list('Work hours by company', r.reports.byCompany.map(([c, h]) => [c, h1(h)]))}
<p style="margin-top:18px;font-size:12px;color:#667085">Sent by the <a href="https://ot.techorchestrator.cloud">Operation Technology Dashboard</a>.</p></div>`;
    return { subject: `Monthly Report — ${r.month}`, text: lines.join('\n'), html };
}

module.exports = { previousMonth, buildMonthly, composeMonthly, downtimeMs, workMinutes };
