// License alert report — the same rules as the dashboard's _buildAlertList
// and _lowStockList (script.js), for the server. Pure: give it the units and
// the licence-stock ledger, it returns the report. tests/license_report_test.js
// checks that both sides agree on the same data.
'use strict';

const LICENSE_TYPE_DEFAULTS = ['SF-RTK', 'SF-1', 'G5 Basic', 'G5 Advance'];
const LICENSE_LOW_STOCK_THRESHOLD = 5;
const SOON_DAYS = 30;

const licenseTypeKey = raw => (raw || '').trim().replace(/\s+/g, ' ').toLowerCase();

function canonicalLicenseType(raw, stock) {
    const key = licenseTypeKey(raw);
    if (!key) return '';
    const known = LICENSE_TYPE_DEFAULTS.find(t => licenseTypeKey(t) === key);
    if (known) return known;
    const seen = (stock || []).find(r => licenseTypeKey(r.licenseType) === key);
    return ((seen ? seen.licenseType : raw) || '').trim().replace(/\s+/g, ' ');
}

function isHeavy(u) {
    const v = String(u && u.unitGroup != null ? u.unitGroup : '').trim().toLowerCase().replace(/\s+/g, ' ');
    return ['heavy', 'heavy equipment', 'alat berat'].includes(v);
}

function parseLocalDate(value) {
    const m = /^(\d{4})-(\d{2})-(\d{2})/.exec(String(value || '').trim());
    if (m) return new Date(+m[1], +m[2] - 1, +m[3]);
    const d = new Date(value);
    return isNaN(d.getTime()) ? null : d;
}

function getExpiryStatus(endDate, now) {
    if (!endDate) return { kind: 'none', label: '—' };
    const end = parseLocalDate(endDate);
    if (!end) return { kind: 'none', label: '—' };
    const today = new Date(now || Date.now());
    today.setHours(0, 0, 0, 0);
    end.setHours(0, 0, 0, 0);
    const days = Math.round((end - today) / 86400000);
    if (days < 0) return { kind: 'expired', label: `Expired ${-days}d ago`, daysLeft: days };
    if (days <= SOON_DAYS) return { kind: 'soon', label: `${days}d left`, daysLeft: days };
    return { kind: 'ok', label: `${days}d left`, daysLeft: days };
}

function getLicenseEndDate(unit, kind) {
    if (!unit) return '';
    if (kind === 'display') return unit.displayLicenseEndDate || '';
    return unit.gpsLicenseEndDate || unit.licenseEndDate || '';
}

function licenseExpiryEnd(unit, kind, stock) {
    const type = canonicalLicenseType(kind === 'display' ? unit.licenseDisplay : unit.gpsLicense, stock);
    if (type === (kind === 'display' ? 'G5 Basic' : 'SF-1')) return '';
    return getLicenseEndDate(unit, kind);
}

function isDowngraded(unit, kind, now) {
    const rawType = kind === 'display' ? (unit.licenseDisplay || '') : (unit.gpsLicense || '');
    const end = getLicenseEndDate(unit, kind);
    const premium = kind === 'display' ? 'G5 Advance' : 'SF-RTK';
    const fallback = kind === 'display' ? 'G5 Basic' : 'SF-1';
    if (rawType === premium && getExpiryStatus(end, now).kind === 'expired') return true;
    const archived = kind === 'display' ? unit.displayLicenseExpiredAt : unit.gpsLicenseExpiredAt;
    return rawType === fallback && !!archived && !end;
}

// opts.soonOnly leaves expired licences out (what the email has always sent).
function buildAlertList(units, stock, opts) {
    const soonOnly = !!(opts && opts.soonOnly);
    const now = opts && opts.now;
    const lines = [];
    let expiredCount = 0, soonCount = 0;
    (units || []).filter(u => !isHeavy(u)).forEach(unit => {
        ['gps', 'display'].forEach(kind => {
            const end = licenseExpiryEnd(unit, kind, stock);
            if (!end) return;
            const s = getExpiryStatus(end, now);
            if (isDowngraded(unit, kind, now)) return;
            if (soonOnly && s.kind === 'expired') return;
            if (s.kind === 'expired' || s.kind === 'soon' || (s.kind === 'ok' && s.daysLeft <= SOON_DAYS)) {
                const licName = kind === 'display' ? (unit.licenseDisplay || 'Display') : (unit.gpsLicense || 'GPS');
                const tag = s.kind === 'expired' ? 'EXPIRED' : 'EXPIRING';
                if (s.kind === 'expired') expiredCount++; else soonCount++;
                lines.push({
                    daysLeft: s.daysLeft,
                    cells: [tag, unit.name || '-', unit.model || '-', unit.sn || '-', licName, unit.site || '-', end, s.label]
                });
            }
        });
    });
    lines.sort((a, b) => a.daysLeft - b.daysLeft);
    return { expiredCount, soonCount, total: lines.length, rows: lines.map(l => l.cells), lines: lines.map(l => l.cells.join(' | ')) };
}

function lowStockList(stock) {
    const known = new Map(LICENSE_TYPE_DEFAULTS.map(t => [licenseTypeKey(t), t]));
    const acc = {};
    (stock || []).forEach(r => {
        const key = licenseTypeKey(r.licenseType) || '(tanpa jenis)';
        if (!acc[key]) acc[key] = { label: known.get(key) || (r.licenseType || '').trim().replace(/\s+/g, ' ') || '(no type)', in: 0, out: 0 };
        const q = Number(r.qty) || 0;
        if (r.txnType === 'OUT') acc[key].out += q; else acc[key].in += q;
    });
    const map = {};
    Object.values(acc).forEach(v => { map[v.label] = { in: v.in, out: v.out, sisa: v.in - v.out }; });
    return Object.keys(map)
        .filter(t => (map[t].in > 0 || map[t].out > 0) && map[t].sisa <= LICENSE_LOW_STOCK_THRESHOLD)
        .sort((a, b) => map[a].sisa - map[b].sisa)
        .map(t => ({ type: t, sisa: map[t].sisa }));
}

const esc = s => String(s == null ? '' : s).replace(/[&<>"']/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));

// Subject, plain text and HTML for one email. null when there is nothing to say.
function composeEmail(units, stock, opts) {
    const report = buildAlertList(units, stock, { soonOnly: true, now: opts && opts.now });
    if (!report.total) return null;
    const low = lowStockList(stock);
    const today = new Date((opts && opts.now) || Date.now()).toLocaleDateString('en-GB',
        { weekday: 'long', year: 'numeric', month: 'long', day: 'numeric', timeZone: (opts && opts.timeZone) || undefined });
    const head = ['Status', 'Unit', 'Model', 'Serial Number', 'License', 'Site', 'Expiry Date', 'Remaining'];
    const text = [
        `License Alert Report — ${today}`,
        `Expiring in the next ${SOON_DAYS} days: ${report.soonCount}`,
        '',
        head.join(' | '),
        '—'.repeat(60),
        ...report.lines,
        ...(low.length ? ['', 'LOW LICENSE STOCK', ...low.map(l => `${l.type} | ${l.sisa} left`)] : []),
        '',
        'Sent by the Operation Technology Dashboard — https://ot.techorchestrator.cloud'
    ].join('\n');
    const td = 'style="padding:6px 10px;border-bottom:1px solid #EAECF0;font-size:13px"';
    const html = `<div style="font-family:Arial,Helvetica,sans-serif;color:#101828">
<h2 style="margin:0 0 4px;font-size:18px">License Alert Report</h2>
<p style="margin:0 0 14px;color:#475467">${esc(today)} · <strong>${report.soonCount}</strong> expiring in the next ${SOON_DAYS} days</p>
<table style="border-collapse:collapse;width:100%"><thead><tr>${head.map(h => `<th align="left" style="padding:6px 10px;background:#F2F4F7;font-size:12px">${esc(h)}</th>`).join('')}</tr></thead>
<tbody>${report.rows.map(r => `<tr>${r.map(c => `<td ${td}>${esc(c)}</td>`).join('')}</tr>`).join('')}</tbody></table>
${low.length ? `<h3 style="margin:18px 0 6px;font-size:15px">Low license stock</h3><ul style="margin:0;padding-left:18px">${low.map(l => `<li>${esc(l.type)} — ${l.sisa} left</li>`).join('')}</ul>` : ''}
<p style="margin-top:18px;font-size:12px;color:#667085">Sent by the <a href="https://ot.techorchestrator.cloud">Operation Technology Dashboard</a>.</p></div>`;
    return { subject: `License Alert: ${report.soonCount} expiring soon`, text, html, report, low };
}

module.exports = { buildAlertList, lowStockList, composeEmail, getExpiryStatus };
