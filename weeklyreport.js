// ---- Weekly Report (2.31.0) ----
// One week, Saturday to Friday, read from what the dashboard already holds.
// Two versions from the same numbers:
//   team  — "Weekly Operation Report": Daily Report, field tickets, jobs,
//           fleet support, projects, next week (for the OT team itself);
//   mgmt  — "Precision Health Weekly Update": precision health against the
//           95% target, scorecard, licenses, open faults, projects (for the
//           manager / head).
// Both are built as a list of pages (buildWeeklyPages), drawn on screen as
// landscape sheets (Save as PDF prints them one per page) and written to a
// PowerPoint with pptxgenjs (vendor/pptxgen.bundle.js, loaded on demand).

const WR_TARGET = 95;          // precision health target, %
const WR_STALE_DAYS = 14;      // a fault open longer than this is flagged
const WR_COL = { ink: '1C2620', green: '24503A', cane: 'B88A2E', sage: '8C9A90', rust: 'A8432C', slate: '5D6B63', hair: 'DDE3DE', tint: 'F1F3EF', amber: 'C27C0E' };
const WR_TONE = { good: WR_COL.green, warn: WR_COL.amber, bad: WR_COL.rust, muted: WR_COL.slate, ink: WR_COL.ink };
const WR_RAG = { good: 'On track', warn: 'Watch', bad: 'Action' };

let _wrWeek = '', _wrAud = 'team', _wrNotes = '', _wrPages = [];

function canViewWeeklyReport() { return hasAccess('weeklyReport', 'view'); }

// ---- Weeks ----
const _wrIso = d => `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, '0')}-${String(d.getDate()).padStart(2, '0')}`;
const _wrDate = iso => { const d = new Date(iso + 'T00:00:00'); return isNaN(d) ? null : d; };
const _wrAdd = (iso, n) => { const d = _wrDate(iso); d.setDate(d.getDate() + n); return _wrIso(d); };
function wrWeekStart(iso) {   // the Saturday on or before the date
    const d = _wrDate(iso || toISODate());
    d.setDate(d.getDate() - ((d.getDay() + 1) % 7));
    return _wrIso(d);
}
function wrIsoWeek(iso) {     // ISO week number of the week's Friday
    const d = _wrDate(iso);
    const t = new Date(Date.UTC(d.getFullYear(), d.getMonth(), d.getDate()));
    const day = t.getUTCDay() || 7;
    t.setUTCDate(t.getUTCDate() + 4 - day);
    const y0 = new Date(Date.UTC(t.getUTCFullYear(), 0, 1));
    return Math.ceil(((t - y0) / 86400000 + 1) / 7);
}
function wrWeekInfo(from) {
    const days = Array.from({ length: 7 }, (_, i) => _wrAdd(from, i));
    const to = days[6];
    const f = (iso, o) => _wrDate(iso).toLocaleDateString('en-GB', o);
    return {
        from, to, days, no: wrIsoWeek(to),
        dayLabels: days.map(d => f(d, { weekday: 'short' }) + ' ' + _wrDate(d).getDate()),
        label: `${f(from, { weekday: 'short', day: 'numeric', month: 'short' })} – ${f(to, { weekday: 'short', day: 'numeric', month: 'short', year: 'numeric' })}`,
        start: _wrDate(from).getTime(), end: _wrDate(_wrAdd(to, 1)).getTime()
    };
}
// The last finished week is the default; this week (still running) is offered too.
function wrWeekChoices(n) {
    const cur = wrWeekStart();
    return Array.from({ length: n || 12 }, (_, i) => _wrAdd(cur, -7 * i)).map(from => {
        const w = wrWeekInfo(from);
        return { from, text: `W${w.no} · ${w.label}${from === cur ? ' (this week, so far)' : ''}` };
    });
}

// ---- Small helpers ----
const _wrPt = s => { const t = String(s || '').replace(/^PT\.?\s*/i, '').trim(); if (!t) return '—'; const w = t.split(/\s+/); return w.length > 1 ? w.map(x => x[0]).join('').toUpperCase() : t; };
const _wrPct = (a, b) => b ? Math.round(1000 * a / b) / 10 : null;
const _wrH = n => (Math.round(n * 10) / 10).toLocaleString('en-US');
const _wrIn = (iso, w) => typeof iso === 'string' && iso >= w.from && iso <= w.to;
const _wrInMs = (ms, w) => typeof ms === 'number' && ms >= w.start && ms < w.end;
const _wrPlural = (n, one, many) => `${n} ${n === 1 ? one : (many || one + 's')}`;
const _wrNum = v => v == null ? '—' : (typeof v === 'number' ? v.toLocaleString('en-US') : String(v));
const _wrFmtPct = v => v == null ? '—' : `${v}%`;
const _wrToneOf = (v, target, warnGap) => v == null ? 'muted' : v >= target ? 'good' : v >= target - (warnGap || 5) ? 'warn' : 'bad';

// ---- The numbers ----
function buildWeeklyReport(from, opts) {
    opts = opts || {};
    const w = wrWeekInfo(from || wrWeekStart(_wrAdd(toISODate(), -7)));
    const prev = wrWeekInfo(_wrAdd(w.from, -7));
    const today = opts.today || toISODate();
    const D = { week: w, prevWeek: prev, today, preparedBy: opts.preparedBy || '', generatedAt: new Date().toLocaleString('en-GB', { weekday: 'short', day: 'numeric', month: 'short', year: 'numeric', hour: '2-digit', minute: '2-digit' }) };
    const units = globalData || [];
    const nameOf = id => { const u = units.find(x => x.id === id); return u ? (u.name || u.sn || id) : ''; };

    // Daily Report: who was expected each day, who filed, hours and paddocks.
    const teamOn = (typeof workLogs !== 'undefined') && ['teamLog', 'teamLogApprove', 'leader'].some(a => hasAccess(a, 'view'));
    D.team = { on: teamOn };
    if (teamOn) {
        const members = activeMembers();
        const sub = l => l.memberId && isSubmitted(l);
        const compliance = wk => {
            let expected = 0, filed = 0;
            const rows = members.map(m => {
                const days = wk.days.map(date => {
                    if (date > today) return 'future';
                    const has = workLogs.some(l => l.date === date && l.memberId === m.id && sub(l));
                    if (has) { expected++; filed++; return 'ok'; }
                    if (leaveOn(m.id, date)) return 'leave';
                    const anyShift = members.some(x => shiftFor(x.id, date));
                    const sh = shiftFor(m.id, date);
                    if (anyShift && (!sh || sh === 'libur')) return 'off';
                    expected++; return 'miss';
                });
                const mins = workLogs.filter(l => l.memberId === m.id && sub(l) && _wrIn(l.date, wk)).reduce((n, l) => n + workLogMinutes(l), 0);
                return { name: m.name || '', pt: _wrPt(typeof companyOf === 'function' ? companyOf(m) : m.company), days, hours: mins / 60 };
            });
            return { rows, expected, filed, pct: _wrPct(filed, expected) };
        };
        const c = compliance(w), cp = compliance(prev);
        if (!cp.filed) cp.pct = null;   // nothing filed that week: no comparison
        const logs = workLogs.filter(l => _wrIn(l.date, w) && isSubmitted(l));
        const hoursPerDay = w.days.map(d => logs.filter(l => l.date === d).reduce((n, l) => n + workLogMinutes(l), 0) / 60);
        const byType = new Map(), byPaddock = new Map(), byUnit = new Map();
        logs.forEach(l => {
            const jobs = workLogJobs(l).filter(j => j.paddock || j.jobType);
            const share = jobs.length ? workLogMinutes(l) / 60 / jobs.length : 0;
            jobs.forEach(j => {
                const t = j.jobType || 'Other';
                byType.set(t, (byType.get(t) || 0) + share);
                if (j.paddock) { const p = byPaddock.get(j.paddock) || { job: t, days: new Set(), hours: 0 }; p.days.add(l.date); p.hours += share; byPaddock.set(j.paddock, p); }
                (j.units || []).forEach(u => { const k = u.name || nameOf(u.id) || u.sn; if (!k) return; const x = byUnit.get(k) || { days: new Set(), hours: 0 }; x.days.add(l.date); x.hours += share; byUnit.set(k, x); });
            });
        });
        const approval = { approved: 0, pending: 0, revision: 0 };
        logs.forEach(l => { const a = workLogApproval(l); if (a in approval) approval[a]++; });
        D.team = Object.assign(D.team, {
            members: c.rows, expected: c.expected, filed: c.filed, pct: c.pct, prevPct: cp.pct,
            missed: c.rows.flatMap(r => r.days.map((s, i) => s === 'miss' ? `${r.name.split(' ')[0]} (${w.dayLabels[i].split(' ')[0]})` : null).filter(Boolean)),
            hours: hoursPerDay.reduce((a, b) => a + b, 0), hoursPerDay,
            jobTypes: [...byType.entries()].sort((a, b) => b[1] - a[1]),
            paddocks: byPaddock.size, unitsUsed: byUnit.size,
            topPaddocks: [...byPaddock.entries()].map(([p, x]) => [p, x.job, x.days.size, x.hours]).sort((a, b) => b[3] - a[3]).slice(0, 6),
            topUnits: [...byUnit.entries()].map(([u, x]) => [u, x.days.size, x.hours]).sort((a, b) => b[2] - a[2]).slice(0, 8),
            issues: logs.filter(l => (l.issue || '').trim()).map(l => ({ date: l.date, who: l.memberName || '', text: l.issue.trim() })).slice(0, 6),
            approval, office: logs.filter(l => l.place === 'office').length, reports: logs.length
        });
    }

    // Field tickets.
    const tk = typeof tickets !== 'undefined' ? tickets : [];
    const evAt = (t, type) => (t.events || []).filter(e => e.type === type).map(e => e.at).sort((a, b) => b - a)[0];
    const resolvedIn = tk.filter(t => _wrInMs(t.resolvedAt || evAt(t, 'resolved'), w));
    const openNow = tk.filter(t => !TK_DONE.includes(t.status));
    const prio = { high: 0, medium: 1, low: 2 };
    D.tickets = {
        on: tk.length > 0 || typeof canHandleTickets === 'function',
        newIn: tk.filter(t => _wrInMs(t.reportedAt, w)).length,
        newPrev: tk.filter(t => _wrInMs(t.reportedAt, prev)).length,
        resolved: resolvedIn.length,
        slaMet: resolvedIn.filter(t => (t.resolvedAt || evAt(t, 'resolved')) <= ticketDueAt(t)).length,
        open: openNow.length,
        overdue: openNow.filter(t => ticketOverdue(t)).length,
        noPic: openNow.filter(t => t.status === 'open').length,
        byCategory: [...tk.filter(t => _wrInMs(t.reportedAt, w)).reduce((m, t) => m.set(t.category || 'Other', (m.get(t.category || 'Other') || 0) + 1), new Map()).entries()].sort((a, b) => b[1] - a[1]),
        openList: openNow.slice().sort((a, b) => (prio[a.priority] ?? 1) - (prio[b.priority] ?? 1) || (a.reportedAt || 0) - (b.reportedAt || 0)).map(t => ({
            no: t.no, title: t.title, priority: t.priority, status: t.status, pic: t.assigneeName || '', units: (t.units || []).map(u => u.name).join(', '),
            age: Math.max(0, Math.floor((Date.now() - (t.reportedAt || Date.now())) / 86400000)), late: ticketOverdue(t), jobs: ticketJobs(t).length
        }))
    };

    // Jobs & activities outside the Daily Report.
    const jobs = (typeof otJobs !== 'undefined' ? otJobs : []).filter(j => _wrIn(j.date, w)).sort((a, b) => (a.date || '').localeCompare(b.date || ''));
    D.jobs = {
        on: typeof otJobs !== 'undefined' && canViewView('projects'),
        total: jobs.length, done: jobs.filter(j => j.status === 'done').length,
        prevTotal: (otJobs || []).filter(j => _wrIn(j.date, prev)).length,
        hours: jobs.reduce((n, j) => n + (Number(j.hours) || 0), 0),
        byCategory: [...jobs.reduce((m, j) => m.set(j.category || 'Other', (m.get(j.category || 'Other') || 0) + 1), new Map()).entries()].sort((a, b) => b[1] - a[1]),
        list: jobs.map(j => { const t = ticketOfJob(j); return { date: j.date, category: j.category, what: j.description || '', units: (j.units || []).map(u => u.name).join(', '), status: j.status || 'done', by: j.requestedBy || '', ticket: t ? t.no : '' }; }),
        backlog: (otJobs || []).filter(j => (j.issue || '').trim() && j.status !== 'done').map(j => ({ date: j.date, category: j.category, issue: j.issue.trim(), units: (j.units || []).map(u => u.name).join(', '), ticket: (ticketOfJob(j) || {}).no || '' })),
        unfinished: (otJobs || []).filter(j => j.status !== 'done').length
    };

    // Precision health: a tractor counts when every precision component is Good.
    const tractors = units.filter(u => !isHeavy(u)), heavy = units.filter(u => isHeavy(u));
    const allGood = (u, keys) => keys.every(k => compOk(u[k]));
    // Per component: only the units that have it fitted.
    const compPct = (list, k) => { const fit = list.filter(u => !isNotInstalled(u[k])); return _wrPct(fit.filter(u => isGood(u[k])).length, fit.length); };
    const sites = [...new Set(tractors.map(u => u.site || '—'))].sort();
    D.precision = {
        tractors: tractors.length, heavy: heavy.length,
        pct: _wrPct(tractors.filter(u => allGood(u, COMPONENT_KEYS)).length, tractors.length),
        heavyPct: _wrPct(heavy.filter(u => allGood(u, HEAVY_COMPONENT_KEYS)).length, heavy.length),
        components: COMPONENT_KEYS.map(k => [COMPONENT_LABELS[k] || k, compPct(tractors, k)]),
        heavyComponents: HEAVY_COMPONENT_KEYS.map(k => [((UNIT_GROUPS.heavy && UNIT_GROUPS.heavy.components || []).find(c => c.key === k) || {}).label || k, compPct(heavy, k)]),
        bySite: sites.map(s => { const us = tractors.filter(u => (u.site || '—') === s); return { site: s, pt: _wrPt(s), units: us.length, pct: _wrPct(us.filter(u => allGood(u, COMPONENT_KEYS)).length, us.length),
            components: COMPONENT_KEYS.map(k => compPct(us, k)) }; }),
        breakdownNow: units.filter(u => isBrokenValue(u.status)).length
    };
    D.precision.target = WR_TARGET;
    D.precision.lastWeek = wrSnapshot(prev.from);

    // Damage and faults.
    const dmg = typeof globalDamages !== 'undefined' ? globalDamages : [];
    const openD = dmg.filter(d => !d.resolved);
    D.damage = {
        newIn: dmg.filter(d => _wrIn(d.date, w)).length, fixedIn: dmg.filter(d => d.resolved && _wrIn(d.resolvedAt, w)).length,
        open: openD.length,
        faults: openD.map(d => ({ unit: d.unitName || nameOf(d.unitId) || d.sn || '', pt: _wrPt(d.site), part: d.component || d.damageType || '', type: d.damageType || '',
            what: d.description || '', days: Math.max(0, daysFromTo(d.date, today)), date: d.date, heavy: d.unitGroup === 'heavy' }))
            .sort((a, b) => b.days - a.days),
    };
    D.damage.stale = D.damage.faults.filter(f => f.days > WR_STALE_DAYS).length;
    D.damage.precisionFaults = D.damage.faults.filter(f => /precision|software/i.test(f.type) || /gps|display|steer|jdlink|camera|telematic|switch|lamp/i.test(f.part));

    // Inspections (heavy equipment).
    const insOn = typeof inspections !== 'undefined' && ['inspection', 'inspectionApprove', 'leader'].some(a => hasAccess(a, 'view'));
    D.inspection = { on: insOn };
    if (insOn) {
        const st = inspectableUnits().map(u => inspectionStatusFor(u, today).key);
        D.inspection = { on: true, units: st.length, ok: st.filter(k => k === 'ok').length, soon: st.filter(k => k === 'soon').length,
            overdue: st.filter(k => k === 'overdue').length, never: st.filter(k => k === 'never').length,
            doneIn: inspections.filter(r => isSubmitted(r) && _wrIn(r.date, w)).length };
    }

    // Licenses: per type, active / expiring within 30 days / expired, and stock.
    const lic = new Map();
    tractors.forEach(u => ['gps', 'display'].forEach(kind => {
        const type = kind === 'display' ? (u.licenseDisplay || '') : (u.gpsLicense || '');
        if (!type) return;
        const end = licenseExpiryEnd(u, kind);
        const s = getExpiryStatus(end);
        const eff = effectiveLicense(u, kind);
        const key = `${eff.downgraded ? eff.fallback : type}|${_wrPt(u.site)}`;
        const r = lic.get(key) || { type: eff.downgraded ? eff.fallback : type, pt: _wrPt(u.site), active: 0, soon: 0, expired: 0 };
        if (!eff.downgraded && s.kind === 'expired') r.expired++; else if (!eff.downgraded && s.kind === 'soon') r.soon++; else r.active++;
        lic.set(key, r);
    }));
    const stock = typeof computeLicenseSummary === 'function' && typeof globalLicenseStock !== 'undefined' ? computeLicenseSummary() : {};
    D.licenses = { rows: [...lic.values()].sort((a, b) => a.type.localeCompare(b.type) || a.pt.localeCompare(b.pt)),
        stock: Object.entries(stock).map(([k, v]) => [k, v.sisa]).filter(x => x[0] && x[0] !== '(no type)') };
    D.licenses.soon = D.licenses.rows.reduce((n, r) => n + r.soon, 0);
    D.licenses.expired = D.licenses.rows.reduce((n, r) => n + r.expired, 0);

    // Purchase requests that moved this week.
    const prs = typeof purchaseRequests !== 'undefined' ? purchaseRequests : [];
    const reached = (p, s) => (p.statusHistory || []).some(h => h.status === s && _wrInMs(h.at, w));
    D.purchase = { on: canViewView('purchase'), submitted: prs.filter(p => reached(p, 'submitted')).length, approved: prs.filter(p => reached(p, 'approved')).length,
        po: prs.filter(p => reached(p, 'po')).length, received: prs.filter(p => reached(p, 'received') || reached(p, 'partial')).length,
        open: prs.filter(p => !PR_CLOSED.includes(p.status) && p.status !== 'draft').length };

    // Projects.
    const pj = (typeof projects !== 'undefined' ? projects : []).filter(p => !PJ_CLOSED.includes(p.status) || (p.doneAt && _wrInMs(p.doneAt, w)));
    D.projects = pj.map(p => { const pr = projectProgress(p), rag = projectRag(p, today); const ups = (p.updates || []).filter(u => _wrIn(u.date, w));
        return { name: p.name || '', type: pjTypeLabel(p.type), pt: _wrPt(p.company), pic: p.pic || '', pct: pr.pct, label: pr.label, rag, due: pjDueText(p, today),
            week: ups.length ? ups[ups.length - 1].text : '', status: p.status, start: p.start || '', end: p.end || '' }; })
        .sort((a, b) => ({ red: 0, amber: 1, green: 2, done: 3 }[a.rag] ?? 2) - ({ red: 0, amber: 1, green: 2, done: 3 }[b.rag] ?? 2));

    // Next week: what carries over, in the order it needs doing.
    const next = [];
    const hi = D.tickets.openList.filter(t => t.priority === 'high' || t.late);
    if (hi.length) next.push(`Close ${_wrPlural(hi.length, 'urgent or overdue ticket')}: ${hi.slice(0, 3).map(t => t.no).join(', ')}${hi.length > 3 ? '…' : ''}`);
    if (D.damage.stale) next.push(`Clear ${_wrPlural(D.damage.stale, 'fault')} open more than ${WR_STALE_DAYS} days`);
    if (D.jobs.backlog.length) next.push(`Follow up ${_wrPlural(D.jobs.backlog.length, 'field issue')} from jobs (backlog)`);
    if (D.licenses.expired + D.licenses.soon) next.push(`Renew ${_wrPlural(D.licenses.expired + D.licenses.soon, 'license')} (${D.licenses.expired} expired, ${D.licenses.soon} within 30 days)`);
    if (D.inspection.on && D.inspection.overdue + D.inspection.never) next.push(`Inspect ${_wrPlural(D.inspection.overdue + D.inspection.never, 'heavy unit')} that are overdue`);
    D.projects.filter(p => p.rag === 'red').slice(0, 2).forEach(p => next.push(`Project ${p.name}: ${p.due}`));
    if (D.team.on && D.team.pct != null && D.team.pct < 100) next.push(`Daily Report at 100% (this week ${D.team.pct}%)`);
    D.nextWeek = next.slice(0, 6);
    return D;
}

// The precision numbers are kept per week on this device, so next week's
// report can say how they moved (the units only hold today's state).
function wrSnapshot(from) {
    try { return JSON.parse(localStorage.getItem('ot.wrSnap.' + from) || 'null'); } catch (_) { return null; }
}
function wrSaveSnapshot(D) {
    if (D.week.from > wrWeekStart() || D.precision.pct == null) return;
    try { localStorage.setItem('ot.wrSnap.' + D.week.from, JSON.stringify({ pct: D.precision.pct, heavyPct: D.precision.heavyPct, reports: D.team.pct })); } catch (_) {}
}

// ---- The pages (one model, drawn as HTML and as PowerPoint) ----
// A page: { kind: 'cover' | 'content', section, title, cols: [[block…], [block…]], widths }
// Blocks: hero, stats, table, bars, list, panel, numbered, projects, note.
function buildWeeklyPages(D, aud, notes, media) {
    media = media || { daily: [], tickets: [], work: [] };
    const w = D.week, P = [];
    const page = (section, title, cols, widths) => P.push({ kind: 'content', section, title, cols, widths: widths || cols.map(() => 1) });
    const mgmt = aud === 'mgmt';
    const name = mgmt ? 'Precision Health Weekly Update' : 'Weekly Operation Report';
    P.push({ kind: 'cover', title: name, week: w.no,
        lines: [`Week ${w.no} · ${w.label}`, mgmt ? `All PTs · target ${WR_TARGET}% · faults flagged after ${WR_STALE_DAYS} days` : 'All PTs · Daily Report, field tickets, jobs and projects',
                `Prepared by ${D.preparedBy || 'Operation Technology'} · ${D.generatedAt}`] });
    const t = D.team, tk = D.tickets, jb = D.jobs, pr = D.precision, dm = D.damage;
    const delta = (a, b, unit) => a == null || b == null ? '' : (a === b ? 'same as last week' : `${a > b ? '▲' : '▼'} ${_wrH(Math.abs(a - b))}${unit || ''} vs last week`);

    if (!mgmt) {
        // Summary
        const stats = [];
        if (t.on) stats.push({ value: _wrH(t.hours), label: 'work hours' }, { value: t.paddocks, label: 'paddocks worked' });
        stats.push({ value: tk.newIn, label: 'tickets raised' }, { value: tk.open, label: 'tickets still open', tone: tk.overdue ? 'bad' : '' });
        if (jb.on) stats.push({ value: `${jb.done} / ${jb.total}`, label: 'jobs done / logged' });
        stats.push({ value: `${dm.newIn} / ${dm.fixedIn}`, label: 'new / fixed faults', tone: dm.newIn > dm.fixedIn ? 'bad' : '' });
        const hero = t.on ? { type: 'hero', value: _wrFmtPct(t.pct), tone: _wrToneOf(t.pct, 100, 10), label: `of expected daily reports filed (${t.filed} of ${t.expected})`,
            sub: t.missed.length ? `Missed: ${t.missed.slice(0, 5).join(', ')}${t.missed.length > 5 ? '…' : ''}` : 'Nobody missed a report', subTone: t.missed.length ? 'bad' : 'good', foot: delta(t.pct, t.prevPct, ' pts') }
            : { type: 'hero', value: tk.newIn, label: 'field tickets raised this week', sub: `${tk.resolved} resolved · ${tk.open} still open` };
        page('Summary', t.on ? `${t.filed} of ${t.expected} daily reports came in; the team logged ${_wrH(t.hours)} hours on ${_wrPlural(t.paddocks, 'paddock')}`
            : `${_wrPlural(tk.newIn, 'ticket')} raised, ${jb.total} jobs logged this week`, [[hero], [{ type: 'stats', items: stats }]], [1, 1.5]);
        // Daily Report
        if (t.on && t.members.length) {
            const M = { ok: { text: '✓', tone: 'good', bold: true }, miss: { text: '✗', tone: 'bad', bold: true }, off: { text: '–', tone: 'muted' }, leave: { text: 'L', tone: 'warn', bold: true }, future: { text: '·', tone: 'muted' } };
            page('Daily Report', t.missed.length ? `${_wrPlural(t.missed.length, 'report was', 'reports were')} missed — ${t.missed.slice(0, 3).join(', ')}${t.missed.length > 3 ? '…' : ''}` : 'Every expected daily report came in',
                [[{ type: 'table', compact: true, head: [{ t: 'Member' }, { t: 'PT' }, ...w.dayLabels.map(d => ({ t: d, a: 'center' })), { t: 'Hours', a: 'right' }],
                    rows: t.members.slice(0, 14).map(m => [m.name, m.pt, ...m.days.map(d => ({ ...M[d], a: 'center' })), _wrH(m.hours)]), more: Math.max(0, t.members.length - 14) },
                  { type: 'note', text: `✓ reported   ✗ missed on shift   L leave   – day off   ·   Approval: ${t.approval.approved} approved · ${t.approval.pending} pending · ${t.approval.revision} sent back` }]]);
            const maxI = t.hoursPerDay.indexOf(Math.max(...t.hoursPerDay));
            const top = t.jobTypes[0];
            page('Daily Report', top ? `${top[0]} took ${Math.round(100 * top[1] / (t.hours || 1))}% of the hours; ${w.dayLabels[maxI].split(' ')[0]} was the busiest day` : 'Work hours this week',
                [[{ type: 'bars', title: 'Work hours per day', items: w.dayLabels.map((d, i) => [d, t.hoursPerDay[i]]), highlight: maxI, vertical: true }],
                 [{ type: 'bars', title: 'Hours by job type', items: t.jobTypes.slice(0, 5) },
                  { type: 'table', head: [{ t: 'Top paddocks' }, { t: 'Job' }, { t: 'Hours', a: 'right' }], rows: t.topPaddocks.slice(0, 4).map(p => [p[0], p[1], _wrH(p[3])]) }]]);
            page('Daily Report', t.issues.length ? `${_wrPlural(t.issues.length, 'field issue was', 'field issues were')} raised in the Daily Report` : `${t.unitsUsed} units worked this week`,
                [[{ type: 'table', head: [{ t: 'Unit' }, { t: 'Days', a: 'right' }, { t: 'Hours', a: 'right' }], rows: t.topUnits.map(u => [{ text: u[0], bold: true }, u[1], _wrH(u[2])]) }],
                 [{ type: 'list', title: 'Issues raised', items: t.issues.map(i => ({ text: i.text, sub: `${i.date} · ${i.who}`, tone: 'bad' })), empty: 'No issue written in the reports' }]], [1, 1.25]);
        }
        if (media.daily.length) { const g = _wrGallery(media.daily), pads = new Set(g.items.map(i => i.cap.split(' · ')[0]));
            page('Daily Report', `From the field: ${_wrPlural(g.items.length, 'photo')} from ${_wrPlural(pads.size, 'paddock')}`, [[g]]); }
        // Field tickets
        page('Field tickets', tk.open ? `${_wrPlural(tk.open, 'ticket')} still open${tk.overdue ? `, ${tk.overdue} past the due time` : ''}; ${tk.resolved} resolved this week` : `No open tickets; ${tk.resolved} resolved this week`,
            [[{ type: 'stats', small: true, per: 3, items: [{ value: tk.newIn, label: `raised (last week ${tk.newPrev})` }, { value: tk.resolved, label: `resolved · ${tk.resolved ? Math.round(100 * tk.slaMet / tk.resolved) + '% on time' : 'none'}` },
                { value: tk.open, label: 'open now' }, { value: tk.overdue, label: 'past due time', tone: tk.overdue ? 'bad' : '' }, { value: tk.noPic, label: 'without a PIC', tone: tk.noPic ? 'warn' : '' }] },
               { type: 'bars', title: 'Raised by category', items: tk.byCategory.slice(0, 4) }],
             [{ type: 'table', head: [{ t: 'Ticket' }, { t: 'Problem' }, { t: 'Prio' }, { t: 'PIC' }, { t: 'Days', a: 'right' }],
                rows: tk.openList.slice(0, 8).map(x => [{ text: x.no, bold: true }, x.title + (x.units ? ` (${x.units})` : ''), { text: (TK_PRIORITY[x.priority] || {}).label || '', tone: x.priority === 'high' ? 'bad' : x.priority === 'medium' ? 'warn' : 'muted', bold: true },
                    x.pic || { text: 'no PIC', tone: 'warn' }, { text: String(x.age), tone: x.late ? 'bad' : '', bold: x.late }]), more: Math.max(0, tk.openList.length - 8), empty: 'No open tickets' }]], [1, 2]);
        if (media.tickets.length) { const g = _wrGallery(media.tickets), n = new Set(g.items.map(i => i.cap.split(' · ')[0])).size;
            page('Field tickets', `Tickets in pictures: ${_wrPlural(n, 'ticket')} from the field`, [[g]]); }
        // Jobs
        if (jb.on) page('Jobs & activities', jb.total ? `${_wrPlural(jb.total, 'job')} outside the Daily Report${jb.byCategory[0] ? `, mostly ${jb.byCategory[0][0].toLowerCase()}` : ''}; ${jb.backlog.length} field issues waiting` : 'No jobs logged outside the Daily Report this week',
            [[{ type: 'table', head: [{ t: 'Day' }, { t: 'Category' }, { t: 'What was done' }, { t: 'Units' }, { t: 'Status' }],
                rows: jb.list.slice(0, 8).map(j => [_wrDate(j.date).toLocaleDateString('en-GB', { weekday: 'short', day: 'numeric' }), { text: j.category, tone: 'good', bold: true }, j.what + (j.ticket ? ` · ${j.ticket}` : ''), { text: j.units || '—', tone: 'muted' },
                    { text: JOB_STATUSES[j.status] || j.status, tone: j.status === 'done' ? 'good' : 'warn' }]), more: Math.max(0, jb.list.length - 8), empty: 'No jobs this week' },
              ...(jb.backlog.length ? [{ type: 'list', title: 'Field issues / backlog', items: jb.backlog.slice(0, 3).map(b => ({ text: b.issue.split('\n')[0], sub: `${b.category} · ${b.units || '—'}${b.ticket ? ' · ' + b.ticket : ''}`, tone: 'warn' })) }] : [])]]);
        if (media.work.length) { const g = _wrGallery(media.work), v = g.items.filter(i => i.kind === 'video').length;
            page('Jobs & activities', `Projects and jobs in pictures${v ? ` — ${_wrPlural(v, 'video')} included` : ''}`, [[g]]); }
        // Fleet and support
        const blocks = [['Damage & repairs', [['New faults', dm.newIn], ['Fixed', dm.fixedIn], ['Open now', dm.open], [`Open > ${WR_STALE_DAYS} days`, dm.stale]]],
            ['Precision health', [['Tractors all Good', _wrFmtPct(pr.pct)], ['Heavy devices Good', _wrFmtPct(pr.heavyPct)], ['In breakdown now', pr.breakdownNow]]]];
        if (D.inspection.on) blocks.push(['Inspections', [['Done this week', D.inspection.doneIn], ['Overdue', D.inspection.overdue], ['Never checked', D.inspection.never], ['Due soon', D.inspection.soon]]]);
        blocks.push(['Licenses', [['Expiring ≤ 30 days', D.licenses.soon], ['Expired', D.licenses.expired], ...D.licenses.stock.slice(0, 2).map(s => [`Stock ${s[0]}`, s[1]])]]);
        if (D.purchase.on) blocks.push(['Purchase', [['Submitted', D.purchase.submitted], ['Approved', D.purchase.approved], ['PO issued', D.purchase.po], ['Goods received', D.purchase.received]]]);
        page('Fleet & support', `${_wrPlural(dm.fixedIn, 'repair')} closed and ${_wrPlural(dm.newIn, 'new fault')}; ${dm.open} still open`, [blocks.map(b => ({ type: 'kv', title: b[0], items: b[1] }))], [1]);
    } else {
        // Management: precision first.
        const last = pr.lastWeek;
        const rag = [['Tractor precision', _wrToneOf(pr.pct, WR_TARGET), `${_wrFmtPct(pr.pct)} vs ${WR_TARGET}%`], ['Heavy equipment devices', _wrToneOf(pr.heavyPct, WR_TARGET), _wrFmtPct(pr.heavyPct)],
            ['Licenses', D.licenses.expired ? 'bad' : D.licenses.soon ? 'warn' : 'good', D.licenses.expired ? `${D.licenses.expired} expired` : `${D.licenses.soon} expiring ≤ 30 days`],
            ['Open faults', dm.stale ? 'bad' : dm.open ? 'warn' : 'good', `${dm.open} open · ${dm.stale} > ${WR_STALE_DAYS} days`],
            ['Field tickets', tk.overdue ? 'bad' : tk.open ? 'warn' : 'good', `${tk.open} open · ${tk.overdue} overdue`]];
        if (t.on) rag.push(['Team reports', _wrToneOf(t.pct, 100, 10), `${_wrFmtPct(t.pct)} filed`]);
        page('Summary', pr.pct == null ? 'Precision health' : `Precision health is ${pr.pct}%${last && last.pct != null ? ` — ${pr.pct >= last.pct ? 'up' : 'down'} ${_wrH(Math.abs(pr.pct - last.pct))} points` : ''}, ${pr.pct >= WR_TARGET ? 'at or above' : 'short of'} the ${WR_TARGET}% target`,
            [[{ type: 'hero', value: _wrFmtPct(pr.pct), tone: _wrToneOf(pr.pct, WR_TARGET), label: `of ${pr.tractors} tractors have every precision component Good`,
                sub: `${last && last.pct != null ? `Last week ${last.pct}% · ` : ''}Target ${WR_TARGET}%`, foot: pr.heavy ? `Heavy equipment devices ${_wrFmtPct(pr.heavyPct)}` : '' }],
             [{ type: 'rag', items: rag }]], [1, 1.3]);
        const score = [
            ['Tractor precision health (all components Good)', _wrFmtPct(pr.pct), last ? _wrFmtPct(last.pct) : '—', `≥ ${WR_TARGET}%`, _wrToneOf(pr.pct, WR_TARGET)],
            ...pr.components.map(c => [c[0], _wrFmtPct(c[1]), '—', `≥ ${WR_TARGET}%`, _wrToneOf(c[1], WR_TARGET)]),
            ...(pr.heavy ? [['Heavy equipment devices (all Good)', _wrFmtPct(pr.heavyPct), last ? _wrFmtPct(last.heavyPct) : '—', `≥ ${WR_TARGET}%`, _wrToneOf(pr.heavyPct, WR_TARGET)]] : []),
            [`Faults open > ${WR_STALE_DAYS} days`, String(dm.stale), '—', '0', dm.stale ? 'bad' : 'good'],
            ['Licenses expired', String(D.licenses.expired), '—', '0', D.licenses.expired ? 'bad' : 'good'],
            ['Licenses expiring ≤ 30 days', String(D.licenses.soon), '—', '—', D.licenses.soon ? 'warn' : 'good'],
            ['Field tickets past due time', String(tk.overdue), '—', '0', tk.overdue ? 'bad' : 'good'],
            ...(D.inspection.on ? [['Heavy inspections overdue', String(D.inspection.overdue + D.inspection.never), '—', '0', D.inspection.overdue + D.inspection.never ? 'warn' : 'good']] : []),
            ...(t.on ? [['Daily Report filed', _wrFmtPct(t.pct), _wrFmtPct(t.prevPct), '100%', _wrToneOf(t.pct, 100, 10)]] : [])];
        const need = score.filter(r => r[4] === 'bad').length;
        page('Scorecard', need ? `${need} of ${score.length} measures need action` : `All ${score.length} measures are on track or close`,
            [[{ type: 'table', compact: true, head: [{ t: '' }, { t: 'Measure' }, { t: 'This week', a: 'right' }, { t: 'Last week', a: 'right' }, { t: 'Target', a: 'right' }, { t: 'Status' }],
                rows: score.map(r => [{ text: '●', tone: r[4], a: 'center' }, r[0], { text: r[1], bold: true }, { text: r[2], tone: 'muted' }, { text: r[3], tone: 'muted' }, { text: WR_RAG[r[4]], tone: r[4], bold: r[4] !== 'good' }]) }]]);
        const worst = pr.components.filter(c => c[1] != null).sort((a, b) => a[1] - b[1])[0];
        page('Precision health', worst ? `${worst[0]} is the weakest component at ${worst[1]}%${pr.bySite.length > 1 ? '; ' + pr.bySite.map(s => `${s.pt} ${_wrFmtPct(s.pct)}`).join(', ') : ''}` : 'Precision health by component',
            [[{ type: 'bars', title: 'Tractor components Good, %', items: pr.components, max: 100, target: WR_TARGET, unit: '%' },
              ...(pr.heavy ? [{ type: 'bars', title: 'Heavy equipment devices Good, %', items: pr.heavyComponents, max: 100, target: WR_TARGET, unit: '%' }] : [])],
             [{ type: 'table', head: [{ t: 'PT' }, { t: 'Units', a: 'right' }, { t: 'All Good', a: 'right' }, ...COMPONENT_KEYS.map(k => ({ t: COMPONENT_LABELS[k] || k, a: 'right' }))],
                rows: pr.bySite.map(s => [{ text: s.pt, bold: true }, s.units, { text: _wrFmtPct(s.pct), tone: _wrToneOf(s.pct, WR_TARGET), bold: true }, ...s.components.map(v => ({ text: _wrFmtPct(v), tone: _wrToneOf(v, WR_TARGET) }))]) }]], [1, 1.2]);
        page('Licenses', D.licenses.expired + D.licenses.soon ? `${_wrPlural(D.licenses.expired + D.licenses.soon, 'license')} need renewing — ${D.licenses.expired} expired, ${D.licenses.soon} within 30 days` : 'No license runs out in the next 30 days',
            [[{ type: 'table', head: [{ t: 'License' }, { t: 'PT' }, { t: 'Active', a: 'right' }, { t: '≤ 30 days', a: 'right' }, { t: 'Expired', a: 'right' }],
                rows: D.licenses.rows.map(r => [r.type, r.pt, r.active, { text: String(r.soon), tone: r.soon ? 'warn' : '', bold: !!r.soon }, { text: String(r.expired), tone: r.expired ? 'bad' : '', bold: !!r.expired }]), empty: 'No licenses recorded' }],
             [{ type: 'stats', small: true, title: 'Ready to assign (stock)', items: D.licenses.stock.map(s => ({ value: s[1], label: s[0], tone: s[1] > 0 ? 'good' : 'bad' })), empty: 'No stock recorded' }]], [1.6, 1]);
        const faults = dm.precisionFaults.length ? dm.precisionFaults : dm.faults;
        const support = [];
        if (dm.stale) support.push(`${_wrPlural(dm.stale, 'fault')} open more than ${WR_STALE_DAYS} days — parts or vendor follow-up needed`);
        if (D.licenses.expired) support.push(`${_wrPlural(D.licenses.expired, 'expired license')} — renewal approval`);
        D.licenses.stock.filter(s => s[1] <= 0).forEach(s => support.push(`No ${s[0]} license in stock`));
        tk.openList.filter(x => x.priority === 'high').slice(0, 2).forEach(x => support.push(`${x.no}: ${x.title}`));
        page('Issues', dm.stale ? `${_wrPlural(dm.stale, 'fault is', 'faults are')} older than ${WR_STALE_DAYS} days` : faults.length ? `${_wrPlural(faults.length, 'fault')} open, none older than ${WR_STALE_DAYS} days` : 'No open faults',
            [[{ type: 'table', compact: true, head: [{ t: 'Unit' }, { t: 'PT' }, { t: 'Part' }, { t: 'Days', a: 'right' }, { t: 'Problem' }],
                rows: faults.slice(0, 8).map(f => [{ text: f.unit, bold: true }, f.pt, f.part, { text: String(f.days), tone: f.days > WR_STALE_DAYS ? 'bad' : '', bold: f.days > WR_STALE_DAYS }, f.what]), more: Math.max(0, faults.length - 8), empty: 'No open faults' },
              ...(support.length ? [{ type: 'panel', title: 'Support needed', items: support.slice(0, 4) }] : [])]]);
        // Field overview for the manager: tickets and jobs, short.
        page('Field tickets & jobs', `${_wrPlural(tk.newIn, 'ticket')} raised and ${tk.resolved} resolved; ${_wrPlural(jb.total, 'job')} done outside the Daily Report`,
            [[{ type: 'stats', small: true, items: [{ value: tk.newIn, label: 'tickets raised' }, { value: tk.resolved, label: 'resolved' }, { value: tk.open, label: 'open now' }, { value: tk.overdue, label: 'past due time', tone: tk.overdue ? 'bad' : '' }] },
              { type: 'table', head: [{ t: 'Open ticket' }, { t: 'Prio' }, { t: 'PIC' }, { t: 'Days', a: 'right' }], rows: tk.openList.slice(0, 5).map(x => [`${x.no} · ${x.title}`, (TK_PRIORITY[x.priority] || {}).label || '', x.pic || '—', { text: String(x.age), tone: x.late ? 'bad' : '' }]), empty: 'No open tickets' }],
             [{ type: 'bars', title: 'Jobs by category', items: jb.byCategory.slice(0, 6) }, { type: 'kv', title: 'Jobs', items: [['Logged', jb.total], ['Done', jb.done], ['Field issues waiting', jb.backlog.length]] }]], [1.5, 1]);
    }
    if (mgmt) {
        const mix = [], a = media.work.slice(), b = media.tickets.slice();
        while (a.length || b.length) { if (a.length) mix.push(a.shift()); if (b.length) mix.push(b.shift()); }
        if (mix.length) { const g = _wrGallery(mix); page('From the field', `From the field: ${_wrPlural(g.items.length, 'picture')} of this week's work and tickets`, [[g]]); }
    }
    // Both: projects and next week.
    if (D.projects.length) {
        const need = D.projects.filter(p => p.rag === 'red' || p.rag === 'amber');
        page('Projects', need.length ? `${need.length} of ${_wrPlural(D.projects.length, 'project')} need attention: ${need.slice(0, 2).map(p => p.name).join(', ')}` : `All ${_wrPlural(D.projects.length, 'project')} on track`,
            [[{ type: 'projects', items: D.projects.slice(0, 5), more: Math.max(0, D.projects.length - 5) }]]);
    }
    const items = (notes || '').split('\n').map(s => s.trim()).filter(Boolean).concat(D.nextWeek).slice(0, 5);
    page('Next week', items.length ? `Next week: ${items.slice(0, 2).map(s => s.charAt(0).toLowerCase() + s.slice(1)).join('; ')}`.slice(0, 120) : 'Next week', [[{ type: 'numbered', items: items.length ? items : ['Keep the Daily Report at 100%'] }]]);
    return P;
}

// ---- HTML ----
const _wrE = s => escapeHtml(String(s == null ? '' : s));
function _wrCell(c, head) {
    const o = c && typeof c === 'object' ? c : { text: c };
    const a = o.a || (head && head.a) || 'left';
    return `<td class="wr-a-${a}${o.tone ? ' wr-t-' + o.tone : ''}${o.bold ? ' wr-b' : ''}">${_wrE(o.text)}</td>`;
}
function _wrBlockHtml(b) {
    const title = b.title ? `<div class="wr-btitle">${_wrE(b.title)}</div>` : '';
    switch (b.type) {
    case 'hero': return `<div class="wr-hero"><div class="wr-hero__v wr-t-${b.tone || 'good'}">${_wrE(b.value)}</div><div class="wr-hero__l">${_wrE(b.label)}</div>
        ${b.sub ? `<div class="wr-hero__s wr-t-${b.subTone || 'muted'}">${_wrE(b.sub)}</div>` : ''}${b.foot ? `<div class="wr-hero__f">${_wrE(b.foot)}</div>` : ''}</div>`;
    case 'stats': return `${title}<div class="wr-stats${b.small ? ' wr-stats--sm' : ''}">${b.items.length ? b.items.map(s => `<div class="wr-stat"><div class="wr-stat__v${s.tone ? ' wr-t-' + s.tone : ''}">${_wrE(_wrNum(s.value))}</div><div class="wr-stat__l">${_wrE(s.label)}</div></div>`).join('') : `<div class="wr-empty">${_wrE(b.empty || '—')}</div>`}</div>`;
    case 'table': return `${title}${b.rows.length ? `<table class="wr-table${b.compact ? ' wr-table--c' : ''}"><thead><tr>${b.head.map(h => `<th class="wr-a-${h.a || 'left'}">${_wrE(h.t)}</th>`).join('')}</tr></thead>
        <tbody>${b.rows.map(r => `<tr>${r.map((c, i) => _wrCell(c, b.head[i])).join('')}</tr>`).join('')}</tbody></table>${b.more ? `<div class="wr-more">+ ${b.more} more in the dashboard</div>` : ''}` : `<div class="wr-empty">${_wrE(b.empty || 'Nothing this week')}</div>`}`;
    case 'bars': {
        const max = b.max || Math.max(1, ...b.items.map(i => Number(i[1]) || 0));
        if (!b.items.length) return `${title}<div class="wr-empty">Nothing this week</div>`;
        if (b.vertical) return `${title}<div class="wr-vbars">${b.items.map((it, i) => `<div class="wr-vbar"><span class="wr-vbar__v">${_wrH(it[1])}</span><i style="height:${Math.round(100 * (it[1] || 0) / max)}%" class="${i === b.highlight ? 'is-hi' : ''}"></i><span class="wr-vbar__l">${_wrE(it[0])}</span></div>`).join('')}</div>`;
        return `${title}<div class="wr-bars">${b.items.map(it => { const v = it[1]; return `<div class="wr-bar"><span class="wr-bar__l">${_wrE(it[0])}</span><span class="wr-bar__t"><i style="width:${v == null ? 0 : Math.round(100 * v / max)}%" class="${b.target && v != null ? (v >= b.target ? 'is-good' : v >= b.target - 5 ? 'is-warn' : 'is-bad') : ''}"></i>${b.target ? `<b style="left:${b.target}%"></b>` : ''}</span><span class="wr-bar__v">${v == null ? '—' : _wrH(v) + (b.unit || '')}</span></div>`; }).join('')}</div>`;
    }
    case 'list': return `${title}${b.items.length ? `<ul class="wr-list">${b.items.map(i => `<li><i class="wr-dot wr-bg-${i.tone || 'muted'}"></i><div><div>${_wrE(i.text)}</div>${i.sub ? `<div class="wr-sub">${_wrE(i.sub)}</div>` : ''}</div></li>`).join('')}</ul>` : `<div class="wr-empty">${_wrE(b.empty || '—')}</div>`}`;
    case 'rag': return `<ul class="wr-rag">${b.items.map(r => `<li><i class="wr-dot wr-bg-${r[1]}"></i><b>${_wrE(r[0])}</b><span>${_wrE(r[2])}</span></li>`).join('')}</ul>`;
    case 'kv': return `<div class="wr-kv"><div class="wr-kv__t">${_wrE(b.title)}</div>${b.items.map(i => `<div class="wr-kv__r"><span>${_wrE(i[0])}</span><b>${_wrE(_wrNum(i[1]))}</b></div>`).join('')}</div>`;
    case 'panel': return `<div class="wr-panel"><div class="wr-panel__t">${_wrE(b.title)}</div><ul>${b.items.map(i => `<li>${_wrE(i)}</li>`).join('')}</ul></div>`;
    case 'note': return `<div class="wr-note">${_wrE(b.text)}</div>`;
    case 'gallery': return _wrGalleryHtml(b);
    case 'numbered': return `<ol class="wr-num">${b.items.map((s, i) => `<li><span>${String(i + 1).padStart(2, '0')}</span>${_wrE(s)}</li>`).join('')}</ol>`;
    case 'projects': return `<div class="wr-pj">${b.items.map(p => { const tone = p.rag === 'red' ? 'bad' : p.rag === 'amber' ? 'warn' : 'good'; return `<div class="wr-pj__row">
        <div class="wr-pj__name"><i class="wr-dot wr-bg-${tone}"></i><div><b>${_wrE(p.name)}</b><div class="wr-sub">${_wrE([p.type, p.pt, p.pic].filter(Boolean).join(' · '))}</div></div></div>
        <div class="wr-pj__pct wr-t-${tone}">${p.pct}%</div>
        <div class="wr-pj__bar"><span><i class="wr-bg-${tone}" style="width:${p.pct}%"></i></span><div class="wr-sub">${_wrE(p.label)} · ${_wrE(p.due)}</div></div>
        <div class="wr-pj__wk">${_wrE(p.week || '—')}</div></div>`; }).join('')}${b.more ? `<div class="wr-more">+ ${b.more} more in the dashboard</div>` : ''}</div>`;
    }
    return '';
}
function weeklyPagesHtml(pages, D, aud) {
    const foot = `${aud === 'mgmt' ? 'Precision Health Weekly Update' : 'Weekly Operation Report'} · Week ${D.week.no} · ${D.week.label}`;
    return pages.map((p, i) => p.kind === 'cover'
        ? `<section class="wr-page wr-cover"><div class="wr-cover__brand"><img src="icons/icon-192.png" alt=""><span>Operation Technology</span></div>
            <div class="wr-cover__main"><div><h1>${_wrE(p.title)}</h1>${p.lines.map((l, k) => `<p${k === 0 ? ' class="wr-b"' : ''}>${_wrE(l)}</p>`).join('')}</div><div class="wr-cover__wk">W${p.week}</div></div></section>`
        : `<section class="wr-page"><header class="wr-head"><h2>${_wrE(p.title)}</h2><img src="icons/icon-192.png" alt=""></header>
            <div class="wr-body">${p.cols.map((c, k) => `<div class="wr-col" style="flex:${p.widths[k]}">${c.map(_wrBlockHtml).join('')}</div>`).join('')}</div>
            <footer class="wr-foot"><span>${_wrE(foot)}</span><span>${_wrE(p.section)} · ${i + 1}</span></footer></section>`).join('');
}

// ---- The view ----
function renderWeeklyReportView() {
    const sel = document.getElementById('wrWeek');
    if (!sel) return;
    if (!canViewWeeklyReport()) { document.getElementById('wrPages').innerHTML = '<div class="adm-empty">You do not have access to the weekly report.</div>'; return; }
    const choices = wrWeekChoices(12);
    // The last full week; on Friday (the week's last day) this week.
    if (!_wrWeek) _wrWeek = _wrDate(toISODate()).getDay() === 5 ? choices[0].from : choices[1].from;
    sel.innerHTML = choices.map(c => `<option value="${c.from}">${_wrE(c.text)}</option>`).join('');
    sel.value = _wrWeek;
    document.querySelectorAll('.wr-aud').forEach(b => b.classList.toggle('active', b.dataset.aud === _wrAud));
    const notes = document.getElementById('wrNotes');
    if (notes && document.activeElement !== notes) notes.value = _wrNotes;
    const D = buildWeeklyReport(_wrWeek, { preparedBy: (currentUserDoc && currentUserDoc.displayName) || (currentUser && currentUser.email ? shortActor(currentUser.email) : '') });
    wrSaveSnapshot(D);
    const refs = wrMediaRefs(D);
    const media = { daily: _wrShow.daily ? refs.daily : [], tickets: _wrShow.tickets ? refs.tickets : [], work: _wrShow.work ? refs.work : [] };
    // Load what a gallery can show, plus a few spares for pictures taken out or failed.
    [media.daily, media.tickets, media.work].forEach(list => wrLoadMedia(list.filter(r => !_wrHidden.has(r.key)).slice(0, WR_GALLERY_MAX + 3)));
    _wrPages = buildWeeklyPages(D, _wrAud, _wrNotes, media);
    const hid = document.getElementById('wrHiddenBtn');
    if (hid) { hid.hidden = !_wrHidden.size; hid.textContent = `Show ${_wrHidden.size} left-out picture${_wrHidden.size === 1 ? '' : 's'} again`; }
    ['daily', 'tickets', 'work'].forEach(k => { const el = document.getElementById('wrShow_' + k); if (el) el.checked = _wrShow[k]; });
    window._wrLast = D;
    document.getElementById('wrPages').innerHTML = weeklyPagesHtml(_wrPages, D, _wrAud);
    const c = document.getElementById('wrCount');
    if (c) c.textContent = `${_wrPages.length} pages`;
}
function setWeeklyReportWeek(v) { _wrWeek = v; renderWeeklyReportView(); }
function setWeeklyReportAudience(a) { _wrAud = a === 'mgmt' ? 'mgmt' : 'team'; renderWeeklyReportView(); }
let _wrNotesT = null;
function setWeeklyReportNotes(v) { _wrNotes = v; clearTimeout(_wrNotesT); _wrNotesT = setTimeout(renderWeeklyReportView, 500); }
function _wrFileName(ext) {
    const D = window._wrLast;
    return `${_wrAud === 'mgmt' ? 'Precision_Health_Update' : 'Weekly_Operation_Report'}_W${D ? D.week.no : ''}_${D ? D.week.from : ''}.${ext}`;
}
async function _wrWaitMedia() {
    for (let i = 0; i < 60 && wrMediaPending(); i++) await new Promise(r => setTimeout(r, 500));
    clearTimeout(_wrMediaT);
    renderWeeklyReportView();
}
async function printWeeklyReport() {
    if (wrMediaPending()) { showToast('Loading the pictures first…', 'info'); await _wrWaitMedia(); }
    if (!_wrPages.length) renderWeeklyReportView();
    const old = document.title;
    document.title = _wrFileName('pdf').replace(/\.pdf$/, '');
    let box = document.getElementById('wrPrint');
    if (!box) { box = document.createElement('div'); box.id = 'wrPrint'; document.body.appendChild(box); }
    box.innerHTML = document.getElementById('wrPages').innerHTML;
    await Promise.all([...box.querySelectorAll('img')].map(im => im.decode ? im.decode().catch(() => {}) : null));
    document.body.classList.add('wr-printing');
    const done = () => { document.body.classList.remove('wr-printing'); document.title = old; window.removeEventListener('afterprint', done); };
    window.addEventListener('afterprint', done);
    window.print();
    setTimeout(done, 1500);
}

// ---- PowerPoint ----
let _pptxLoading = null;
function loadPptxGen() {
    if (window.PptxGenJS) return Promise.resolve(window.PptxGenJS);
    if (!_pptxLoading) _pptxLoading = new Promise((resolve, reject) => {
        const s = document.createElement('script');
        s.src = 'vendor/pptxgen.bundle.js';   // pptxgenjs@3.12.0, self-hosted (vendor/README.md)
        s.onload = () => window.PptxGenJS ? resolve(window.PptxGenJS) : reject(new Error('PptxGenJS missing'));
        s.onerror = () => { _pptxLoading = null; reject(new Error('PptxGenJS failed to load')); };
        document.head.appendChild(s);
    });
    return _pptxLoading;
}
async function _wrLogoData() {
    try {
        const r = await fetch('icons/icon-192.png');
        const b = await r.blob();
        return await new Promise(res => { const fr = new FileReader(); fr.onload = () => res(String(fr.result).replace(/^data:/, '')); fr.readAsDataURL(b); });
    } catch (_) { return ''; }
}
// Column widths from the text in each column, and each row's height from how
// many lines its longest cell wraps to (pptxgenjs does not measure text).
function _wrTableLayout(b, wdt) {
    const fs = b.compact ? 9 : 10, cw = fs * 0.0072, line = fs * 0.0185, base = b.compact ? 0.25 : 0.3;
    const txt = c => String(c && typeof c === 'object' ? (c.text == null ? '' : c.text) : (c == null ? '' : c));
    const n = b.head.length;
    const weight = Array.from({ length: n }, (_, i) => Math.max(b.head[i].t ? 6 : 3, Math.min(42, Math.max(...[b.head[i].t, ...b.rows.map(r => txt(r[i]))].map(t => t.length)))));
    const sum = weight.reduce((a, x) => a + x, 0);
    const colW = weight.map(x => wdt * x / sum);
    const rowH = r => Math.max(base, ...r.map((c, i) => Math.ceil(Math.max(1, txt(c).length) * cw / Math.max(0.2, colW[i] - 0.1)) * line + 0.08));
    const heights = [base, ...b.rows.map(rowH)];
    return { colW, heights, total: heights.reduce((a, x) => a + x, 0), fs };
}
// Rough height of a block on a slide, to stack blocks in a column.
function _wrBlockH(b, wdt) {
    const t = b.title ? 0.32 : 0;
    switch (b.type) {
    case 'hero': return 2.4;
    case 'stats': return t + Math.ceil(Math.max(1, b.items.length) / (b.per || (b.small ? 2 : 3))) * (b.small ? 0.72 : 1.25);
    case 'table': return t + (b.rows.length ? _wrTableLayout(b, wdt || 4).total + (b.more ? 0.25 : 0) : 0.3);
    case 'bars': return t + (b.vertical ? 3.2 : Math.max(1, b.items.length) * 0.3 + 0.1);
    case 'list': return t + Math.max(1, b.items.length) * 0.62;
    case 'rag': return b.items.length * 0.55;
    case 'kv': return 0.4 + b.items.length * 0.28;
    case 'panel': return 0.5 + b.items.length * 0.28;
    case 'note': return 0.35;
    case 'numbered': return b.items.length * 0.72;
    case 'gallery': return 3.75;
    case 'projects': return b.items.length * 0.72 + (b.more ? 0.25 : 0);
    }
    return 0.5;
}
// Where each picture of a gallery sits on a slide (inches).
function _wrGalleryGeom(n, x, y, wdt, hgt) {
    const cols = n <= 3 ? Math.max(1, n) : 3, rows = n <= 3 ? 1 : 2, gap = 0.25, cap = 0.48;
    const w = (wdt - gap * (cols - 1)) / cols, rowH = (hgt - gap * (rows - 1)) / rows;
    const ph = Math.min(rowH - cap, w * 0.75);
    return Array.from({ length: n }, (_, i) => ({ x: x + (i % cols) * (w + gap), y: y + Math.floor(i / cols) * (ph + cap + gap), w, h: ph, cap }));
}
// A picture cut to the box's shape (centre crop), as a JPEG data URL.
async function _wrCrop(src, aspect, fmt) {
    if (!src) return '';
    try {
        const blob = await (await fetch(src)).blob();
        const bmp = await createImageBitmap(blob);
        let sw = bmp.width, sh = bmp.height;
        if (sw / sh > aspect) sw = Math.round(sh * aspect); else sh = Math.round(sw / aspect);
        const outW = Math.min(1200, sw), outH = Math.round(outW / aspect);
        const c = document.createElement('canvas'); c.width = outW; c.height = outH;
        c.getContext('2d').drawImage(bmp, (bmp.width - sw) / 2, (bmp.height - sh) / 2, sw, sh, 0, 0, outW, outH);
        return fmt === 'png' ? c.toDataURL('image/png') : c.toDataURL('image/jpeg', 0.82);
    } catch (_) { return ''; }
}
async function _wrVideoData(url, size) {
    if (!url || (size && size > WR_VIDEO_EMBED_MAX)) return '';
    try {
        const blob = await (await fetch(url)).blob();
        if (blob.size > WR_VIDEO_EMBED_MAX) return '';
        return await new Promise((res, rej) => { const fr = new FileReader(); fr.onload = () => res(String(fr.result)); fr.onerror = rej; fr.readAsDataURL(blob); });
    } catch (_) { return ''; }
}
async function downloadWeeklyPptx() {
    if (wrMediaPending()) { showToast('Loading the pictures first…', 'info'); await _wrWaitMedia(); }
    if (!_wrPages.length) renderWeeklyReportView();
    const D = window._wrLast;
    let PptxGenJS;
    try { PptxGenJS = await loadPptxGen(); } catch (e) { showToast(navigator.onLine ? 'PowerPoint could not be prepared' : 'PowerPoint needs a signal the first time', 'error'); return; }
    showToast('Preparing the PowerPoint…', 'info');
    const logo = await _wrLogoData();
    // Pictures: cut to their boxes first (and the videos read, when asked for).
    const prep = new Map();
    let videos = 0, videosLeft = 0;
    for (const pg of _wrPages) for (const col of (pg.cols || [])) for (const b of col) if (b.type === 'gallery') {
        const geom = _wrGalleryGeom(b.items.length, 0.5, 1.35, 9.0, 3.75);
        for (let i = 0; i < b.items.length; i++) {
            const it = b.items[i], g = geom[i], asp = g.w / g.h;
            if (it.kind === 'video') {
                const cover = await _wrCrop(it.poster, asp, 'png');
                let data = '';
                if (_wrEmbedVideo) { data = await _wrVideoData(it.src, it.size); if (data) videos++; else videosLeft++; }
                prep.set(it.key, { img: cover, video: data });
            } else prep.set(it.key, { img: await _wrCrop(it.src, asp) });
        }
    }
    const C = WR_COL, pres = new PptxGenJS();
    pres.layout = 'LAYOUT_16x9';
    pres.title = `${_wrAud === 'mgmt' ? 'Precision Health Weekly Update' : 'Weekly Operation Report'} — Week ${D.week.no}`;
    pres.company = 'Operation Technology';
    const foot = `${pres.title.replace(' — ', ' · ')} · ${D.week.label}`;
    const head = 'Cambria', body = 'Calibri';
    const tx = (s, t, o) => s.addText(t, Object.assign({ margin: 0, fontFace: body, color: C.ink, valign: 'top' }, o));
    const tone = k => WR_TONE[k] || C.ink;
    const cellOpt = (c, h) => { const o = c && typeof c === 'object' ? c : { text: c };
        return { text: String(o.text == null ? '' : o.text), options: { align: o.a || (h && h.a) || 'left', color: o.tone ? tone(o.tone) : C.ink, bold: !!o.bold, border: [{ type: 'none' }, { type: 'none' }, { pt: 0.5, color: C.hair }, { type: 'none' }] } }; };
    const drawBlock = (s, b, x, y, wdt) => {
        let yy = y;
        if (b.title && b.type !== 'kv' && b.type !== 'panel') { tx(s, b.title, { x, y: yy, w: wdt, h: 0.28, fontFace: head, fontSize: 12, bold: true }); yy += 0.32; }
        switch (b.type) {
        case 'hero':
            tx(s, String(b.value), { x, y: yy, w: wdt, h: 1.05, fontFace: head, fontSize: 60, bold: true, color: tone(b.tone || 'good'), valign: 'bottom' });
            tx(s, b.label, { x, y: yy + 1.1, w: wdt, h: 0.5, fontSize: 13 });
            if (b.sub) tx(s, b.sub, { x, y: yy + 1.62, w: wdt, h: 0.45, fontSize: 11, color: tone(b.subTone || 'muted') });
            if (b.foot) tx(s, b.foot, { x, y: yy + 2.08, w: wdt, h: 0.3, fontSize: 10.5, color: C.slate });
            break;
        case 'stats': { const per = b.per || (b.small ? 2 : 3), cw = wdt / per, rh = b.small ? 0.72 : 1.25;
            b.items.forEach((st, i) => { const cx = x + (i % per) * cw, cy = yy + Math.floor(i / per) * rh;
                tx(s, _wrNum(st.value), { x: cx, y: cy, w: cw - 0.1, h: rh * 0.55, fontFace: head, fontSize: b.small ? 22 : 28, bold: true, color: st.tone ? tone(st.tone) : C.ink, valign: 'bottom' });
                tx(s, st.label, { x: cx, y: cy + rh * 0.57, w: cw - 0.1, h: rh * 0.4, fontSize: b.small ? 9.5 : 11, color: C.slate }); });
            if (!b.items.length) tx(s, b.empty || '—', { x, y: yy, w: wdt, h: 0.3, fontSize: 11, color: C.slate });
            break; }
        case 'table': {
            if (!b.rows.length) { tx(s, b.empty || 'Nothing this week', { x, y: yy, w: wdt, h: 0.3, fontSize: 11, color: C.slate }); break; }
            const H = b.head.map(h => ({ text: h.t, options: { bold: true, color: C.slate, fontSize: 9, align: h.a || 'left', border: [{ type: 'none' }, { type: 'none' }, { pt: 1, color: C.ink }, { type: 'none' }] } }));
            const R = b.rows.map(r => r.map((c, i) => cellOpt(c, b.head[i])));
            const L = _wrTableLayout(b, wdt);
            s.addTable([H, ...R], { x, y: yy, w: wdt, colW: L.colW, rowH: L.heights, fontFace: body, fontSize: L.fs, color: C.ink, valign: 'top', margin: [0.03, 0.05, 0.03, 0.05], autoPage: false });
            if (b.more) tx(s, `+ ${b.more} more in the dashboard`, { x, y: yy + L.total + 0.03, w: wdt, h: 0.22, fontSize: 9, color: C.slate });
            break; }
        case 'bars': {
            const max = b.max || Math.max(1, ...b.items.map(i => Number(i[1]) || 0));
            if (b.vertical) { const n = b.items.length, bw = wdt / n, ch = 2.5;
                b.items.forEach((it, i) => { const h = Math.max(0.02, ch * (it[1] || 0) / max), bx = x + i * bw + bw * 0.18;
                    s.addShape(pres.ShapeType.rect, { x: bx, y: yy + 0.25 + ch - h, w: bw * 0.64, h, fill: { color: i === b.highlight ? C.green : C.sage }, line: { color: i === b.highlight ? C.green : C.sage, width: 0 } });
                    tx(s, _wrH(it[1]), { x: x + i * bw, y: yy + ch - h, w: bw, h: 0.22, fontSize: 9, align: 'center' });
                    tx(s, it[0], { x: x + i * bw, y: yy + 0.3 + ch, w: bw, h: 0.25, fontSize: 9, color: C.slate, align: 'center' }); });
                break; }
            const lw = Math.min(1.9, wdt * 0.38), vw = 0.6, tw = wdt - lw - vw - 0.1;
            b.items.forEach((it, i) => { const cy = yy + i * 0.3, v = it[1];
                tx(s, it[0], { x, y: cy, w: lw, h: 0.24, fontSize: 10, valign: 'middle' });
                s.addShape(pres.ShapeType.rect, { x: x + lw, y: cy + 0.06, w: tw, h: 0.13, fill: { color: C.tint }, line: { color: C.tint, width: 0 } });
                const col = b.target && v != null ? (v >= b.target ? C.green : v >= b.target - 5 ? C.amber : C.rust) : C.green;
                if (v) s.addShape(pres.ShapeType.rect, { x: x + lw, y: cy + 0.06, w: Math.max(0.02, tw * v / max), h: 0.13, fill: { color: col }, line: { color: col, width: 0 } });
                if (b.target) s.addShape(pres.ShapeType.line, { x: x + lw + tw * b.target / max, y: cy + 0.02, w: 0, h: 0.21, line: { color: C.ink, width: 1, dashType: 'dash' } });
                tx(s, v == null ? '—' : _wrH(v) + (b.unit || ''), { x: x + lw + tw + 0.08, y: cy, w: vw, h: 0.24, fontSize: 10, bold: true, align: 'right', valign: 'middle' }); });
            break; }
        case 'list':
            if (!b.items.length) { tx(s, b.empty || '—', { x, y: yy, w: wdt, h: 0.3, fontSize: 11, color: C.slate }); break; }
            b.items.forEach((it, i) => { const cy = yy + i * 0.62;
                s.addShape(pres.ShapeType.ellipse, { x, y: cy + 0.06, w: 0.12, h: 0.12, fill: { color: tone(it.tone || 'muted') }, line: { color: tone(it.tone || 'muted'), width: 0 } });
                tx(s, it.text.slice(0, 140), { x: x + 0.22, y: cy, w: wdt - 0.22, h: 0.36, fontSize: 11, fit: 'shrink' });
                if (it.sub) tx(s, it.sub, { x: x + 0.22, y: cy + 0.36, w: wdt - 0.22, h: 0.22, fontSize: 9, color: C.slate }); });
            break;
        case 'rag':
            b.items.forEach((r, i) => { const cy = yy + i * 0.55;
                s.addShape(pres.ShapeType.ellipse, { x, y: cy + 0.08, w: 0.16, h: 0.16, fill: { color: tone(r[1]) }, line: { color: tone(r[1]), width: 0 } });
                tx(s, r[0], { x: x + 0.3, y: cy, w: wdt * 0.5, h: 0.32, fontSize: 13, bold: true });
                tx(s, r[2], { x: x + wdt * 0.5, y: cy, w: wdt * 0.5, h: 0.32, fontSize: 12, color: C.slate, align: 'right' });
                if (i < b.items.length - 1) s.addShape(pres.ShapeType.line, { x, y: cy + 0.46, w: wdt, h: 0, line: { color: C.hair, width: 0.75 } }); });
            break;
        case 'kv':
            tx(s, b.title, { x, y: yy, w: wdt, h: 0.3, fontFace: head, fontSize: 13, bold: true, color: C.green });
            b.items.forEach((it, i) => { tx(s, it[0], { x, y: yy + 0.38 + i * 0.28, w: wdt - 0.7, h: 0.25, fontSize: 11, color: C.slate });
                tx(s, _wrNum(it[1]), { x: x + wdt - 0.7, y: yy + 0.38 + i * 0.28, w: 0.7, h: 0.25, fontSize: 11, bold: true, align: 'right' }); });
            break;
        case 'panel': {
            const h = 0.5 + b.items.length * 0.28;
            s.addShape(pres.ShapeType.roundRect, { x, y: yy, w: wdt, h, fill: { color: C.tint }, line: { color: C.tint, width: 0 }, rectRadius: 0.06 });
            tx(s, b.title, { x: x + 0.2, y: yy + 0.1, w: wdt - 0.4, h: 0.28, fontFace: head, fontSize: 12, bold: true, color: C.green });
            tx(s, b.items.map((t, k) => ({ text: t, options: { bullet: true, breakLine: k < b.items.length - 1 } })), { x: x + 0.2, y: yy + 0.42, w: wdt - 0.4, h: h - 0.5, fontSize: 10.5 });
            break; }
        case 'note': tx(s, b.text, { x, y: yy, w: wdt, h: 0.3, fontSize: 9.5, color: C.slate }); break;
        case 'gallery':
            _wrGalleryGeom(b.items.length, x, yy, wdt, 3.75).forEach((g, i) => {
                const it = b.items[i], pr = prep.get(it.key) || {};
                if (it.kind === 'video' && pr.video) {
                    const data = pr.video.replace(/^data:/, ''), extn = ((it.name || '').split('.').pop() || 'mp4').toLowerCase();
                    const media = { type: 'video', data, extn: extn === 'mov' ? 'mov' : extn, x: g.x, y: g.y, w: g.w, h: g.h };
                    if (pr.img) media.cover = pr.img;
                    s.addMedia(media);
                } else if (pr.img) s.addImage({ data: pr.img, x: g.x, y: g.y, w: g.w, h: g.h });
                else {
                    s.addShape(pres.ShapeType.rect, { x: g.x, y: g.y, w: g.w, h: g.h, fill: { color: C.tint }, line: { color: C.tint, width: 0 } });
                    tx(s, it.kind === 'video' ? 'Video — open it in the dashboard' : 'Photo in the dashboard', { x: g.x, y: g.y, w: g.w, h: g.h, fontSize: 10, color: C.slate, align: 'center', valign: 'middle' });
                }
                if (it.kind === 'video' && !pr.video) {
                    const d = Math.min(g.w, g.h) * 0.28;
                    s.addShape(pres.ShapeType.ellipse, { x: g.x + (g.w - d) / 2, y: g.y + (g.h - d) / 2, w: d, h: d, fill: { color: '000000', transparency: 45 }, line: { color: 'FFFFFF', width: 1.5 } });
                    s.addShape(pres.ShapeType.triangle, { x: g.x + (g.w - d * 0.38) / 2 + d * 0.05, y: g.y + (g.h - d * 0.42) / 2, w: d * 0.38, h: d * 0.42, rotate: 90, fill: { color: 'FFFFFF' }, line: { color: 'FFFFFF', width: 0 } });
                }
                tx(s, it.cap, { x: g.x, y: g.y + g.h + 0.05, w: g.w, h: 0.22, fontSize: 9.5, bold: true, fit: 'shrink' });
                tx(s, it.sub, { x: g.x, y: g.y + g.h + 0.26, w: g.w, h: 0.2, fontSize: 8.5, color: C.slate, fit: 'shrink' });
            });
            break;
        case 'numbered':
            b.items.forEach((it, i) => { const cy = yy + i * 0.72;
                tx(s, String(i + 1).padStart(2, '0'), { x, y: cy, w: 0.8, h: 0.6, fontFace: head, fontSize: 28, bold: true, color: C.cane });
                tx(s, it, { x: x + 0.95, y: cy + 0.12, w: wdt - 0.95, h: 0.45, fontSize: 15, fit: 'shrink' }); });
            break;
        case 'projects':
            b.items.forEach((p, i) => { const cy = yy + i * 0.72, t2 = p.rag === 'red' ? 'bad' : p.rag === 'amber' ? 'warn' : 'good';
                s.addShape(pres.ShapeType.ellipse, { x, y: cy + 0.07, w: 0.14, h: 0.14, fill: { color: tone(t2) }, line: { color: tone(t2), width: 0 } });
                tx(s, p.name, { x: x + 0.25, y: cy, w: 3.2, h: 0.28, fontSize: 12, bold: true });
                tx(s, [p.type, p.pt, p.pic].filter(Boolean).join(' · '), { x: x + 0.25, y: cy + 0.29, w: 3.2, h: 0.22, fontSize: 9, color: C.slate });
                tx(s, `${p.pct}%`, { x: x + 3.5, y: cy, w: 0.8, h: 0.4, fontFace: head, fontSize: 18, bold: true, color: tone(t2) });
                s.addShape(pres.ShapeType.rect, { x: x + 4.35, y: cy + 0.1, w: 2.0, h: 0.12, fill: { color: C.hair }, line: { color: C.hair, width: 0 } });
                if (p.pct) s.addShape(pres.ShapeType.rect, { x: x + 4.35, y: cy + 0.1, w: 2.0 * p.pct / 100, h: 0.12, fill: { color: tone(t2) }, line: { color: tone(t2), width: 0 } });
                tx(s, p.due, { x: x + 4.35, y: cy + 0.29, w: 2.2, h: 0.22, fontSize: 9, color: C.slate });
                tx(s, p.week || '—', { x: x + 6.7, y: cy, w: wdt - 6.7, h: 0.55, fontSize: 9.5, color: C.slate, fit: 'shrink' }); });
            if (b.more) tx(s, `+ ${b.more} more in the dashboard`, { x, y: yy + b.items.length * 0.72, w: wdt, h: 0.22, fontSize: 9, color: C.slate });
            break;
        }
    };
    _wrPages.forEach((p, i) => {
        const s = pres.addSlide();
        s.background = { color: 'FFFFFF' };
        if (p.kind === 'cover') {
            if (logo) s.addImage({ data: logo, x: 0.6, y: 0.55, w: 0.7, h: 0.7 });
            tx(s, 'Operation Technology', { x: 1.45, y: 0.72, w: 5, h: 0.4, fontSize: 14, bold: true, color: C.green });
            tx(s, p.title, { x: 0.6, y: 1.9, w: 6.6, h: 1.3, fontFace: head, fontSize: 34, bold: true, color: C.green, valign: 'bottom' });
            tx(s, p.lines.map((l, k) => ({ text: l, options: { breakLine: k < p.lines.length - 1, bold: k === 0, color: k === 0 ? C.ink : C.slate } })), { x: 0.6, y: 3.35, w: 6.6, h: 1.3, fontSize: 14 });
            tx(s, `W${p.week}`, { x: 6.9, y: 2.4, w: 2.6, h: 1.6, fontFace: head, fontSize: 80, bold: true, color: C.cane, align: 'right', valign: 'middle' });
            return;
        }
        tx(s, p.title, { x: 0.5, y: 0.3, w: 8.4, h: 0.85, fontFace: head, fontSize: 20, bold: true, fit: 'shrink' });
        if (logo) s.addImage({ data: logo, x: 9.15, y: 0.32, w: 0.38, h: 0.38 });
        tx(s, foot, { x: 0.5, y: 5.22, w: 7.5, h: 0.25, fontSize: 8, color: C.slate });
        tx(s, `${p.section} · ${i + 1}`, { x: 8.0, y: 5.22, w: 1.5, h: 0.25, fontSize: 8, color: C.slate, align: 'right' });
        const total = p.widths.reduce((a, b) => a + b, 0), gap = 0.35, avail = 9.0 - gap * (p.cols.length - 1);
        let cx = 0.5;
        p.cols.forEach((col, k) => {
            const cw = avail * p.widths[k] / total;
            // A column of several equal cards (kv) is laid out as a grid.
            if (col.length > 1 && col.every(b => b.type === 'kv')) {
                const per = 3, kw = (cw - gap * (per - 1)) / per;
                col.forEach((b, j) => drawBlock(s, b, cx + (j % per) * (kw + gap), 1.35 + Math.floor(j / per) * 1.85, kw));
            } else {
                let cy = 1.35;
                col.forEach(b => { drawBlock(s, b, cx, cy, cw); cy += _wrBlockH(b, cw) + 0.25; });
            }
            cx += cw + gap;
        });
    });
    try {
        await pres.writeFile({ fileName: _wrFileName('pptx') });
        showToast(videosLeft ? `PowerPoint downloaded — ${videosLeft} video(s) too large or not reachable, shown as a still` : 'PowerPoint downloaded', videosLeft ? 'warning' : 'success');
    } catch (e) { showToast('PowerPoint could not be saved', 'error'); }
}

// ---- Photos and videos (2.32.0) ----
// The week's pictures, picked automatically: Daily Report paddock photos (one
// per paddock line, different paddocks and days first), ticket photos, project
// update photos, job photos and job videos. A picture can be taken out of the
// report (× on screen). Videos show a still with a play mark; the PowerPoint
// can carry the video itself.
const WR_GALLERY_MAX = 6;
const WR_VIDEO_EMBED_MAX = 60 * 1024 * 1024;   // per video, for the PowerPoint
const _wrMedia = new Map();     // key → { state: 'loading'|'ok'|'fail', src, poster }
const _wrHidden = new Set();
let _wrShow = { daily: true, tickets: true, work: true }, _wrEmbedVideo = false, _wrMediaT = null;

function wrMediaRefs(D) {
    const w = D.week, out = { daily: [], tickets: [], work: [] };
    const dayName = iso => _wrDate(iso).toLocaleDateString('en-GB', { weekday: 'short', day: 'numeric' });
    // Daily Report: each paddock line with photos; spread over paddocks and days.
    if (D.team.on && typeof loadWorkLogPhotos === 'function') {
        const cand = [];
        workLogs.filter(l => _wrIn(l.date, w) && isSubmitted(l) && workLogPhotoCount(l) > 0).forEach(l => {
            const jobs = l.place === 'office' ? [] : workLogJobs(l);
            const counted = jobs.some(j => Number(j.photoCount) > 0);
            const who = (l.memberName || '').split(' ')[0];
            if (counted) jobs.forEach((j, k) => { if (Number(j.photoCount) > 0) cand.push({ key: `wl:${l.id}:${k}`, kind: 'photo', paddock: j.paddock || '', date: l.date,
                cap: [j.paddock, j.jobType].filter(Boolean).join(' · ') || 'Daily Report', sub: `${who} · ${dayName(l.date)}${Number(j.photoCount) > 1 ? ` · ${j.photoCount} photos` : ''}`,
                load: async () => { const ph = await loadWorkLogPhotos(l.id); return workLogPhotoSplit(l, ph).jobs[k][0] || ''; } }); });
            else cand.push({ key: `wl:${l.id}:g`, kind: 'photo', paddock: (jobs[0] || {}).paddock || l.place || '', date: l.date,
                cap: l.place === 'office' ? `Office · ${(l.task || '').slice(0, 40)}` : ([(jobs[0] || {}).paddock, (jobs[0] || {}).jobType].filter(Boolean).join(' · ') || 'Daily Report'),
                sub: `${who} · ${dayName(l.date)}`, load: async () => (await loadWorkLogPhotos(l.id))[0] || '' });
        });
        // Spread out: each pick prefers a paddock and a day not shown yet.
        const seenP = new Set(), seenD = new Set(), left = cand.sort((a, b) => a.date.localeCompare(b.date) || a.key.localeCompare(b.key));
        while (left.length && out.daily.length < 18) {
            let best = 0, score = -1;
            left.forEach((c, i) => { const sc = (seenP.has(c.paddock) ? 0 : 2) + (seenD.has(c.date) ? 0 : 1); if (sc > score) { score = sc; best = i; } });
            const c = left.splice(best, 1)[0];
            seenP.add(c.paddock); seenD.add(c.date); out.daily.push(c);
        }
    }
    // Tickets raised or handled this week, with their photos.
    (typeof tickets !== 'undefined' ? tickets : []).filter(t => (t.photoIds || []).length && (_wrInMs(t.reportedAt, w) || (t.events || []).some(e => _wrInMs(e.at, w))))
        .sort((a, b) => ({ high: 0, medium: 1, low: 2 }[a.priority] ?? 1) - ({ high: 0, medium: 1, low: 2 }[b.priority] ?? 1))
        .forEach(t => t.photoIds.slice(0, 2).forEach((id, k) => out.tickets.push({ key: `tk:${id}`, kind: 'photo', cap: `${t.no} · ${t.title}`,
            sub: `${(TK_PRIORITY[t.priority] || {}).label || ''} · ${(TK_STATUSES[t.status] || {}).label || t.status}${t.photoIds.length > 1 ? ` · photo ${k + 1} of ${t.photoIds.length}` : ''}`,
            load: () => loadWhPhoto(id, 'tk') })));
    // Projects (updates this week) and jobs (this week): photos and videos.
    if (canViewView('projects')) {
        (typeof projects !== 'undefined' ? projects : []).forEach(p => (p.updates || []).filter(u => _wrIn(u.date, w) && (u.photoIds || []).length).forEach(u =>
            u.photoIds.slice(0, 2).forEach(id => out.work.push({ key: `pj:${id}`, kind: 'photo', cap: p.name || 'Project', sub: `${dayName(u.date)} · ${(u.text || '').slice(0, 60)}`, load: () => loadWhPhoto(id, 'pj') }))));
        (typeof otJobs !== 'undefined' ? otJobs : []).filter(j => _wrIn(j.date, w)).forEach(j => {
            const sub = `${dayName(j.date)}${(j.units || []).length ? ' · ' + j.units.map(u => u.name).join(', ') : ''}`;
            (j.files || []).filter(f => /^video\//.test(f.type || '')).slice(0, 2).forEach(f => out.work.push({ key: `vid:${f.path}`, kind: 'video', cap: `${j.category} · ${j.description || ''}`.slice(0, 80), sub: `${sub} · video`,
                name: f.name, size: f.size || 0, type: f.type, load: async () => window.cloud && window.cloud.projectFileUrl ? window.cloud.projectFileUrl(f.path) : '' }));
            (j.photoIds || []).slice(0, 2).forEach(id => out.work.push({ key: `pj:${id}`, kind: 'photo', cap: `${j.category} · ${j.description || ''}`.slice(0, 80), sub, load: () => loadWhPhoto(id, 'pj') }));
        });
        out.work.sort((a, b) => (a.kind === 'video' ? 0 : 1) - (b.kind === 'video' ? 0 : 1));
    }
    return out;
}
// What a gallery shows now: not taken out, not failed; at most six.
function _wrVisible(refs) { return refs.filter(r => !_wrHidden.has(r.key) && (_wrMedia.get(r.key) || {}).state !== 'fail').slice(0, WR_GALLERY_MAX); }
function wrLoadMedia(refs) {
    const todo = refs.filter(r => !_wrMedia.has(r.key));
    if (!todo.length) return Promise.resolve();
    todo.forEach(r => _wrMedia.set(r.key, { state: 'loading' }));
    let i = 0;
    const one = async () => {
        while (i < todo.length) {
            const r = todo[i++];
            try {
                const src = await r.load();
                const m = { state: src ? 'ok' : 'fail', src: src || '' };
                if (src && r.kind === 'video') m.poster = await _wrVideoPoster(src);
                _wrMedia.set(r.key, m);
            } catch (_) { _wrMedia.set(r.key, { state: 'fail' }); }
            clearTimeout(_wrMediaT);
            _wrMediaT = setTimeout(() => { if (currentView === 'weeklyReport') renderWeeklyReportView(); }, 250);
        }
    };
    return Promise.all([one(), one(), one()]);
}
function wrMediaPending() { return [..._wrMedia.values()].some(m => m.state === 'loading'); }
// A still from the video, a second in (or its first frame).
function _wrVideoPoster(url) {
    return new Promise(resolve => {
        const v = document.createElement('video');
        let done = false;
        const end = s => { if (done) return; done = true; try { v.removeAttribute('src'); v.load(); } catch (_) {} resolve(s || ''); };
        v.crossOrigin = 'anonymous'; v.muted = true; v.preload = 'auto'; v.playsInline = true;
        v.onloadedmetadata = () => { try { v.currentTime = Math.min(1, (v.duration || 2) / 2); } catch (_) { end(''); } };
        v.onseeked = () => {
            try {
                const c = document.createElement('canvas'), k = Math.min(1, 960 / (v.videoWidth || 960));
                c.width = Math.round((v.videoWidth || 640) * k); c.height = Math.round((v.videoHeight || 360) * k);
                c.getContext('2d').drawImage(v, 0, 0, c.width, c.height);
                end(c.toDataURL('image/jpeg', 0.8));
            } catch (_) { end(''); }
        };
        v.onerror = () => end('');
        setTimeout(() => end(''), 15000);
        v.src = url;
    });
}
function hideWeeklyMedia(key) { _wrHidden.add(key); renderWeeklyReportView(); }
function resetWeeklyMedia() { _wrHidden.clear(); renderWeeklyReportView(); }
function setWeeklyMediaShow(part, on) { _wrShow[part] = !!on; renderWeeklyReportView(); }
function setWeeklyEmbedVideo(on) { _wrEmbedVideo = !!on; }

// One gallery block from a list of references.
function _wrGallery(refs) {
    return { type: 'gallery', items: _wrVisible(refs).map(r => { const m = _wrMedia.get(r.key) || { state: 'loading' };
        return { key: r.key, kind: r.kind, cap: r.cap, sub: r.sub, state: m.state, src: m.src || '', poster: m.poster || '', name: r.name || '', size: r.size || 0, type: r.type || '' }; }) };
}
function _wrGalleryHtml(b) {
    if (!b.items.length) return '<div class="wr-empty">No pictures this week</div>';
    return `<div class="wr-gal wr-gal--${Math.min(b.items.length, WR_GALLERY_MAX)}">${b.items.map(it => {
        const img = it.kind === 'video' ? it.poster : it.src;
        const pic = it.state === 'loading' ? '<div class="wr-gal__ph">Loading…</div>'
            : img ? `<img src="${_wrE(img)}" alt="" loading="eager">` : `<div class="wr-gal__ph"><i class="fas fa-film" aria-hidden="true"></i></div>`;
        return `<figure class="wr-gal__item"><div class="wr-gal__pic">${pic}${it.kind === 'video' ? '<span class="wr-gal__play" aria-hidden="true"></span>' : ''}
            <button type="button" class="wr-gal__x wr-noprint" title="Leave this out of the report" aria-label="Leave this picture out" onclick="hideWeeklyMedia(${jsArg(it.key)})">&times;</button></div>
            <figcaption><b>${_wrE(it.cap)}</b><span>${_wrE(it.sub)}</span></figcaption></figure>`; }).join('')}</div>`;
}
