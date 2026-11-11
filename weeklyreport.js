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
// 2.37.0: the "bento" look — a soft grey sheet of white cards, one teal card
// for the number that matters, an amber one for what needs attention. After
// the bento brief of github.com/cheahhl814/design-pack (MIT), in the
// dashboard's own colours.
const WR_COL = { ink: '101828', green: '0F766E', deep: '115E59', cane: '0F766E', sage: 'B9D8D3', rust: 'B42318', slate: '667085', hair: 'E4E7EC', tint: 'EEF2F1', amber: 'B54708',
    bg: 'F3F5F4', card: 'FFFFFF', good: '067647', warnBg: 'FFF7ED', warnLine: 'FED7AA', heroSub: 'B7E4DC' };
const WR_TONE = { good: WR_COL.good, warn: WR_COL.amber, bad: WR_COL.rust, muted: WR_COL.slate, ink: WR_COL.ink };
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

    // 2.37.0: one page per project still running — where it stands, what moved
    // this week, what blocks it and which goods it still waits for.
    D.projectPages = (typeof projects !== 'undefined' ? projects : []).filter(p => !PJ_CLOSED.includes(p.status)).map(p => {
        const pr = projectProgress(p), rag = projectRag(p, today), t = p.target || {};
        const ups = (p.updates || []).filter(u => _wrIn(u.date, w)).sort((a, b) => (a.at || 0) - (b.at || 0));
        // Progress at the start of the week, read from the first update of the week ("progress 3% → 21%").
        const m0 = ups.map(u => /(\d+)%\s*→/.exec(u.progress || '')).find(Boolean);
        const before = m0 ? Number(m0[1]) : (ups.length ? null : pr.pct);
        const mats = p.materials || [];
        const state = x => _pjMatState(x);
        const short = mats.filter(x => state(x) !== 'ready').map(x => ({ name: x.name || '', need: Number(x.qty) || 0, have: _pjMatHave(x), uom: x.uom || '' }));
        const goodsOf = id => { const l = _pjMatsOf(p, id); return l.length ? `${l.filter(x => state(x) === 'ready').length}/${l.length}` : ''; };
        const steps = t.mode === 'milestones' ? (t.milestones || []).map(m => ({ label: m.label || '', weight: Number(m.weight) || 0, done: !!m.done, goods: m.id ? goodsOf(m.id) : '' }))
            : (t.units || []).map(u => ({ label: u.name || u.sn || '', weight: null, done: !!u.done, goods: '' }));
        const daysLeft = p.end ? Math.round((_pjDay(p.end) - _pjDay(today)) / 86400000) : null;
        const s0 = p.start ? _pjDay(p.start) : NaN, e0 = p.end ? _pjDay(p.end) : NaN, n0 = _pjDay(today);
        return { id: p.id, name: p.name || '', type: pjTypeLabel(p.type), company: p.company || '', pic: p.pic || '', status: pjStatusLabel(p.status),
            departments: (p.departments || []).map(d => d.name + (d.pic ? ` (${d.pic})` : '')), pct: pr.pct, label: pr.label, done: pr.done, total: pr.total, what: t.what || '', rag, due: pjDueText(p, today), start: p.start || '', end: p.end || '', daysLeft,
            timePct: isFinite(s0) && isFinite(e0) && e0 > s0 ? Math.max(0, Math.min(100, Math.round(100 * (n0 - s0) / (e0 - s0)))) : null,
            delta: before == null ? null : pr.pct - before, mode: t.mode || 'units', steps, stepsDone: steps.filter(x => x.done).length,
            updates: ups.filter(u => !u.auto || ups.every(x => x.auto)).slice(-4).map(u => ({ date: u.date, text: u.text || '', blocker: u.blocker || '' })),
            blockers: ups.map(u => u.blocker).filter(Boolean).slice(-2),
            jobs: (typeof otJobs !== 'undefined' ? otJobs : []).filter(j => j.projectId === p.id && _wrIn(j.date, w)).length,
            goods: { total: mats.length, ready: mats.length - short.length, short: short.slice(0, 6), more: Math.max(0, short.length - 6) } };
    }).sort((a, b) => ({ red: 0, amber: 1, green: 2 }[a.rag] ?? 2) - ({ red: 0, amber: 1, green: 2 }[b.rag] ?? 2));

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
    const page = (section, title, cols, widths, extra) => P.push(Object.assign({ kind: 'content', section, title, cols, widths: widths || cols.map(() => 1) }, extra || {}));
    media.projects = media.projects || {};
    const mgmt = aud === 'mgmt';
    const name = mgmt ? 'Precision Health Weekly Update' : 'Weekly Operation Report';
    // 2.37.1: the cover of the owner's own management deck — the week, a
    // two-line title, the logo on its own; nothing else.
    P.push({ kind: 'cover', title: mgmt ? 'Weekly Update' : 'Weekly Operation Report', title2: 'Division Operation Technology', week: w.no,
        lines: [`Week ${w.no} · ${w.label}`] });
    const t = D.team, tk = D.tickets, jb = D.jobs, pr = D.precision, dm = D.damage;
    const delta = (a, b, unit) => a == null || b == null ? '' : (a === b ? 'same as last week' : `${a > b ? '▲' : '▼'} ${_wrH(Math.abs(a - b))}${unit || ''} vs last week`);

    if (!mgmt) {
        // Summary: the one number, the week in six figures, and what needs attention.
        const dl = (cur, prv, goodUp, unit) => (cur == null || prv == null || cur === prv) ? null
            : { text: `${cur > prv ? '▲' : '▼'} ${_wrH(Math.abs(cur - prv))}${unit || ''}`, tone: (cur > prv) === goodUp ? 'good' : 'bad' };
        const stats = [];
        if (t.on) stats.push({ value: _wrH(t.hours), label: 'work hours' }, { value: t.paddocks, label: 'paddocks worked' }, { value: t.unitsUsed, label: 'units used' });
        stats.push({ value: tk.newIn, label: 'tickets raised', delta: dl(tk.newIn, tk.newPrev, false) }, { value: tk.open, label: 'tickets still open', tone: tk.overdue ? 'bad' : '' });
        if (jb.on) stats.push({ value: `${jb.done} / ${jb.total}`, label: 'jobs done / logged', delta: dl(jb.total, jb.prevTotal, true) });
        stats.push({ value: `${dm.newIn} / ${dm.fixedIn}`, label: 'new / fixed faults', tone: dm.newIn > dm.fixedIn ? 'bad' : '' });
        const hero = t.on ? { type: 'hero', card: 'hero', title: 'Daily Report filed', value: _wrFmtPct(t.pct), label: `${t.filed} of ${t.expected} expected${t.prevPct != null && t.prevPct !== t.pct ? ` · ${t.pct > t.prevPct ? '▲' : '▼'} ${_wrH(Math.abs(t.pct - t.prevPct))} pts vs last week` : ''}`,
            foot: t.missed.length ? `Missed: ${t.missed.slice(0, 5).join(', ')}${t.missed.length > 5 ? '…' : ''}` : 'Nobody missed a report' }
            : { type: 'hero', card: 'hero', title: 'Field tickets', value: tk.newIn, label: 'raised this week', foot: `${tk.resolved} resolved · ${tk.open} still open` };
        const attention = [];
        if (tk.overdue) attention.push({ text: `${_wrPlural(tk.overdue, 'ticket')} past the due time`, tone: 'bad' });
        if (dm.stale) attention.push({ text: `${_wrPlural(dm.stale, 'fault')} open more than ${WR_STALE_DAYS} days${dm.faults[0] ? ` (${dm.faults[0].unit})` : ''}`, tone: 'bad' });
        if (D.licenses.expired) attention.push({ text: `${_wrPlural(D.licenses.expired, 'license')} expired`, tone: 'bad' });
        if (D.licenses.soon) attention.push({ text: `${_wrPlural(D.licenses.soon, 'license expires', 'licenses expire')} within 30 days`, tone: 'warn' });
        if (D.inspection.on && D.inspection.overdue + D.inspection.never) attention.push({ text: `${_wrPlural(D.inspection.overdue + D.inspection.never, 'heavy unit')} overdue for inspection`, tone: 'warn' });
        if (jb.backlog.length) attention.push({ text: `${_wrPlural(jb.backlog.length, 'field issue')} waiting from jobs`, tone: 'warn' });
        D.projectPages.filter(x => x.rag === 'red').slice(0, 2).forEach(x => attention.push({ text: `Project ${x.name}: ${x.due}`, tone: 'bad' }));
        const health = { type: 'bars', title: 'Precision health', items: [['Tractors all Good', pr.pct], ...(pr.heavy ? [['Heavy devices', pr.heavyPct]] : []), ...pr.components.slice().sort((x, y) => (x[1] ?? 101) - (y[1] ?? 101)).slice(0, 2)], max: 100, target: WR_TARGET, unit: '%' };
        const openTk = { type: 'table', title: 'Open tickets', head: [{ t: 'Ticket' }, { t: 'Prio' }, { t: 'PIC' }, { t: 'Days', a: 'right' }],
            rows: tk.openList.slice(0, 4).map(x => [x.title, { text: (TK_PRIORITY[x.priority] || {}).label || '', chip: x.priority === 'high' ? 'bad' : x.priority === 'medium' ? 'warn' : 'good' }, x.pic || '—', { text: String(x.age), tone: x.late ? 'bad' : '', bold: x.late }]),
            more: Math.max(0, tk.openList.length - 4), empty: 'No open tickets' };
        const need = { type: 'list', card: attention.length ? 'warn' : '', title: 'Needs attention', items: attention.slice(0, 5), empty: 'Nothing urgent this week' };
        const hoursCard = t.on ? { type: 'bars', title: 'Work hours per day', items: w.dayLabels.map((d, i) => [d.split(' ')[0], t.hoursPerDay[i]]), highlight: t.hoursPerDay.indexOf(Math.max(...t.hoursPerDay)), vertical: true }
            : { type: 'bars', title: 'Tickets by category', items: tk.byCategory.slice(0, 5) };
        page('Summary', t.on ? `${t.filed} of ${t.expected} daily reports came in; ${tk.open ? `${_wrPlural(tk.open, 'ticket')} still open${tk.overdue ? `, ${tk.overdue} past due` : ''}` : 'no ticket is open'}`
            : `${_wrPlural(tk.newIn, 'ticket')} raised, ${jb.total} jobs logged this week`,
            [[hero, health], [{ type: 'stats', title: 'Headline numbers', items: stats.slice(0, 6) }, openTk], [hoursCard, need]], [1.1, 1.6, 1.2]);
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
        // Management (2.37.1, as the owner's own deck): precision health,
        // field tickets & jobs, the projects and one page each, next week.
        const worst = pr.components.filter(c => c[1] != null).sort((a, b) => a[1] - b[1])[0];
        page('Precision health', 'Component Health Precision and Sensor',
            [[{ type: 'bars', title: 'Tractor components Good, %', items: pr.components, max: 100, target: WR_TARGET, unit: '%' },
              ...(pr.heavy ? [{ type: 'bars', title: 'Heavy equipment devices Good, %', items: pr.heavyComponents, max: 100, target: WR_TARGET, unit: '%' }] : [])],
             [{ type: 'table', head: [{ t: 'PT' }, { t: 'Units', a: 'right' }, { t: 'All Good', a: 'right' }, ...COMPONENT_KEYS.map(k => ({ t: COMPONENT_LABELS[k] || k, a: 'right' }))],
                rows: pr.bySite.map(s => [{ text: s.pt, bold: true }, s.units, { text: _wrFmtPct(s.pct), tone: _wrToneOf(s.pct, WR_TARGET), bold: true }, ...s.components.map(v => ({ text: _wrFmtPct(v), tone: _wrToneOf(v, WR_TARGET) }))]) }]], [1, 1.2]);
        // Issue in Field (2.38.0): three cards across, the open tickets below.
        page('Issue in field', 'Issue in Field', [], [], { rows: [
            { h: 1, widths: [1.3, 0.9, 1.15], cards: [
                { type: 'stats', small: true, items: [{ value: tk.newIn, label: 'tickets raised' }, { value: tk.resolved, label: 'resolved' }, { value: tk.open, label: 'open now' }, { value: tk.overdue, label: 'past due time', tone: tk.overdue ? 'bad' : '' }] },
                { type: 'kv', title: 'Jobs', items: [['Logged', jb.total], ['Done', jb.done], ['Field issues waiting', jb.backlog.length]] },
                { type: 'bars', title: 'Jobs by category', items: jb.byCategory.slice(0, 4), empty: 'No jobs this week' }] },
            { h: 2, widths: [1], cards: [
                { type: 'table', head: [{ t: 'Open ticket' }, { t: 'Prio' }, { t: 'PIC' }, { t: 'Days', a: 'right' }], rows: tk.openList.slice(0, 6).map(x => [`${x.no} · ${x.title}`, (TK_PRIORITY[x.priority] || {}).label || '', x.pic || '—', { text: String(x.age), tone: x.late ? 'bad' : '' }]), more: Math.max(0, tk.openList.length - 6), empty: 'No open tickets' }] }] });
    }
    // Both: projects and next week.
    if (D.projects.length) {
        const need = D.projects.filter(p => p.rag === 'red' || p.rag === 'amber');
        page('Projects', mgmt ? 'Project' : need.length ? `${need.length} of ${_wrPlural(D.projects.length, 'project')} need attention: ${need.slice(0, 2).map(p => p.name).join(', ')}` : `All ${_wrPlural(D.projects.length, 'project')} on track`,
            [[{ type: 'projects', items: D.projects.slice(0, 5), more: Math.max(0, D.projects.length - 5) }]]);
    }
    // 2.37.0: a page for each project still running.
    (D.projectPages || []).forEach(x => {
        const tone = x.rag === 'red' ? 'bad' : x.rag === 'amber' ? 'warn' : 'good';
        const week = x.updates.map(u => ({ text: u.text, sub: _wrDate(u.date).toLocaleDateString('en-GB', { weekday: 'short', day: 'numeric', month: 'short' }), tone: 'good' }))
            .concat(x.blockers.map(bk => ({ text: `Blocker: ${bk}`, tone: 'warn' })));
        const left = [{ type: 'progress', card: 'hero', title: 'Progress', pct: x.pct, label: x.label, delta: x.delta, timePct: x.timePct, start: x.start, end: x.end, daysLeft: x.daysLeft, rag: PJ_RAG[x.rag] || '', tone }];
        if (x.goods.total) left.push(x.goods.short.length
            ? { type: 'table', card: 'warn', title: `Goods still short · ${x.goods.ready} of ${x.goods.total} complete`, head: [{ t: 'Item' }, { t: 'Need', a: 'right' }, { t: 'Have', a: 'right' }],
                rows: x.goods.short.slice(0, 3).map(g => [g.name, `${_wrH(g.need)} ${g.uom}`.trim(), { text: _wrH(g.have), tone: g.have ? 'warn' : 'bad', bold: true }]), more: x.goods.more + Math.max(0, x.goods.short.length - 3) }
            : { type: 'list', title: 'Goods', items: [{ text: `All ${x.goods.total} items have arrived`, tone: 'good' }] });
        const right = [{ type: 'list', title: 'This week', items: week.slice(0, 5), empty: 'No update written this week' }];
        const refs = media.projects[x.id] || [];
        if (refs.length) right.push(Object.assign(_wrGallery(refs.slice(0, 4), 2), { title: 'From the field', inCard: true }));
        else if (x.jobs) right.push({ type: 'kv', title: 'Jobs this week', items: [['Logged for this project', x.jobs]] });
        const steps = x.steps.length
            ? { type: 'steps', title: x.mode === 'milestones' ? 'Milestones & goods' : x.mode === 'units' ? 'Units' : 'Units involved', items: x.steps.slice(0, 11), more: Math.max(0, x.steps.length - 11) }
            : { type: 'kv', title: `Target${x.what ? ' — ' + x.what : ''}`, grow: 1, items: [['Target', x.total], ['Done so far', x.done], ['Still to do', Math.max(0, x.total - x.done)], ['Jobs logged this week', x.jobs]] };
        // Management: "Project <name>", with its PT when two running projects share a name.
        const twin = (D.projectPages || []).filter(y => y.name === x.name).length > 1;
        page('Projects', mgmt ? `Project ${x.name}${twin && x.company ? ' – ' + x.company : ''}` : x.name, [left, [steps], right], [1, 1.45, 1.15],
            mgmt ? { sub: x.departments.join(', '), projectId: x.id }
                 : { eyebrow: `Project in progress · ${x.type}${x.company ? ' · ' + x.company : ''}`, sub: [x.pic ? `PIC ${x.pic}` : '', x.departments.length ? `with ${x.departments.join(', ')}` : ''].filter(Boolean).join(' · '), projectId: x.id });
    });
    // 2.38.0: the management deck closes with the company's thank-you page instead.
    if (mgmt) {
        P.push({ kind: 'closing', title: 'Terima Kasih' });
        P.forEach(p => { p.theme = 'msg'; if (p.kind === 'content') p.eyebrow = ''; });
        return P;
    }
    const items = (notes || '').split('\n').map(s => s.trim()).filter(Boolean).concat(D.nextWeek).slice(0, 5);
    const first = items[0] ? items[0].charAt(0).toLowerCase() + items[0].slice(1) : '';
    page('Next week', first ? `Next week: ${first.length > 90 ? first.slice(0, 88).replace(/[\s,;:]+\S*$/, '') + '…' : first}${items.length > 1 ? `, and ${items.length - 1} more` : ''}` : 'Next week', [[{ type: 'numbered', items: items.length ? items : ['Keep the Daily Report at 100%'] }]]);
    return P;
}

// ---- HTML ----
const _wrE = s => escapeHtml(String(s == null ? '' : s));
function _wrCell(c, head) {
    const o = c && typeof c === 'object' ? c : { text: c };
    const a = o.a || (head && head.a) || 'left';
    const inner = o.chip ? `<span class="wr-chip wr-chip--${o.chip}">${_wrE(o.text)}</span>` : _wrE(o.text);
    return `<td class="wr-a-${a}${o.tone ? ' wr-t-' + o.tone : ''}${o.bold ? ' wr-b' : ''}">${inner}</td>`;
}
// What sits inside a card (its title is drawn by the card).
function _wrBlockHtml(b) {
    switch (b.type) {
    case 'hero': return `<div class="wr-hero"><div class="wr-hero__v">${_wrE(b.value)}</div><div class="wr-hero__l">${_wrE(b.label)}</div>
        ${b.sub ? `<div class="wr-hero__s">${_wrE(b.sub)}</div>` : ''}${b.foot ? `<div class="wr-hero__f">${_wrE(b.foot)}</div>` : ''}</div>`;
    case 'progress': return `<div class="wr-hero"><div class="wr-hero__v">${b.pct}<small>%</small></div><div class="wr-hero__l">${_wrE(b.label)}${b.delta ? ` · ${b.delta > 0 ? '▲' : '▼'} ${Math.abs(b.delta)} pts this week` : b.delta === 0 ? ' · no change this week' : ''}</div>
        <div class="wr-tl"><u style="width:${b.pct}%"></u>${b.timePct != null ? `<s style="left:${b.timePct}%" title="today"></s>` : ''}</div>
        <div class="wr-hero__s">${_wrE([b.start && b.end ? `${b.start} → ${b.end}` : (b.end || ''), b.daysLeft == null ? '' : b.daysLeft >= 0 ? `${b.daysLeft} days left` : `${-b.daysLeft} days late`].filter(Boolean).join(' · '))}${b.rag ? ` · <b>${_wrE(b.rag)}</b>` : ''}</div></div>`;
    case 'steps': return b.items.length ? `<div class="wr-steps">${b.items.map(m => `<div class="wr-step${m.done ? ' is-done' : ''}"><span class="wr-step__ck">${m.done ? '✓' : ''}</span><span class="wr-step__l">${_wrE(m.label)}</span>
        ${m.weight != null ? `<span class="wr-step__w">${m.weight}%</span>` : ''}${m.goods ? `<span class="wr-step__g ${m.goods.split('/')[0] === m.goods.split('/')[1] ? 'wr-t-good' : 'wr-t-bad'}">▣ ${_wrE(m.goods)}</span>` : (m.weight != null ? '<span class="wr-step__g"></span>' : '')}</div>`).join('')}
        ${b.more ? `<div class="wr-more">+ ${b.more} more in the dashboard</div>` : ''}</div>` : `<div class="wr-empty">${_wrE(b.empty || '—')}</div>`;
    case 'stats': return `<div class="wr-stats${b.small ? ' wr-stats--sm' : ''}"${b.per ? ` style="grid-template-columns:repeat(${b.per},1fr)"` : ''}>${b.items.length ? b.items.map(s => `<div class="wr-stat"><div class="wr-stat__v${s.tone ? ' wr-t-' + s.tone : ''}">${_wrE(_wrNum(s.value))}</div><div class="wr-stat__l">${_wrE(s.label)}</div>${s.delta ? `<div class="wr-stat__d wr-t-${s.delta.tone}">${_wrE(s.delta.text)}</div>` : ''}</div>`).join('') : `<div class="wr-empty">${_wrE(b.empty || '—')}</div>`}</div>`;
    case 'table': return b.rows.length ? `<table class="wr-table${b.compact ? ' wr-table--c' : ''}"><thead><tr>${b.head.map(h => `<th class="wr-a-${h.a || 'left'}">${_wrE(h.t)}</th>`).join('')}</tr></thead>
        <tbody>${b.rows.map(r => `<tr>${r.map((c, i) => _wrCell(c, b.head[i])).join('')}</tr>`).join('')}</tbody></table>${b.more ? `<div class="wr-more">+ ${b.more} more in the dashboard</div>` : ''}` : `<div class="wr-empty">${_wrE(b.empty || 'Nothing this week')}</div>`;
    case 'bars': {
        const max = b.max || Math.max(1, ...b.items.map(i => Number(i[1]) || 0));
        if (!b.items.length) return `<div class="wr-empty">${_wrE(b.empty || 'Nothing this week')}</div>`;
        if (b.vertical) return `<div class="wr-vbars">${b.items.map((it, i) => `<div class="wr-vbar${i === b.highlight ? ' is-hi' : ''}"><span class="wr-vbar__v">${_wrH(it[1])}</span><i style="height:${Math.round(100 * (it[1] || 0) / max)}%"></i><span class="wr-vbar__l">${_wrE(it[0])}</span></div>`).join('')}</div>`;
        return `<div class="wr-bars">${b.items.map(it => { const v = it[1]; return `<div class="wr-bar"><span class="wr-bar__l">${_wrE(it[0])}</span><span class="wr-bar__t"><i style="width:${v == null ? 0 : Math.round(100 * v / max)}%" class="${b.target && v != null ? (v >= b.target ? 'is-good' : v >= b.target - 5 ? 'is-warn' : 'is-bad') : ''}"></i>${b.target ? `<b style="left:${b.target}%"></b>` : ''}</span><span class="wr-bar__v">${v == null ? '—' : _wrH(v) + (b.unit || '')}</span></div>`; }).join('')}</div>`;
    }
    // 2.39.2: an entry may carry a heading (head), a status chip (tag) and a problem line (note).
    case 'list': return b.items.length ? `<ul class="wr-list">${b.items.map(i => `<li><i class="wr-dot wr-bg-${i.tone || 'muted'}"></i><div class="wr-li">${i.head ? `<div class="wr-li__h">${_wrE(i.head)}</div>` : ''}<div class="wr-li__top"><div>${_wrE(i.text)}</div>${i.tag ? `<span class="wr-chip wr-chip--${i.tag.tone || 'good'}">${_wrE(i.tag.text)}</span>` : ''}</div>${i.sub ? `<div class="wr-sub">${_wrE(i.sub)}</div>` : ''}${i.note ? `<div class="wr-li__note">${_wrE(i.note)}</div>` : ''}</div></li>`).join('')}</ul>${b.more ? `<div class="wr-more">+ ${b.more} more in the dashboard</div>` : ''}` : `<div class="wr-empty">${_wrE(b.empty || '—')}</div>`;
    case 'rag': return `<ul class="wr-rag">${b.items.map(r => `<li><i class="wr-dot wr-bg-${r[1]}"></i><b>${_wrE(r[0])}</b><span>${_wrE(r[2])}</span></li>`).join('')}</ul>`;
    case 'kv': return `<div class="wr-kv">${b.items.map(i => `<div class="wr-kv__r"><span>${_wrE(i[0])}</span><b>${_wrE(_wrNum(i[1]))}</b></div>`).join('')}</div>`;
    case 'panel': return `<ul class="wr-panel">${b.items.map(i => `<li>${_wrE(i)}</li>`).join('')}</ul>`;
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
// A block as a card: white by default, teal for the number that matters
// ('hero'), amber for what needs attention ('warn'). A note and a full-page
// gallery stand without one.
const _wrBare = b => b.type === 'note' || (b.type === 'gallery' && !b.inCard);
const _wrGrow = b => b.grow != null ? b.grow : (['steps', 'gallery', 'table', 'list', 'numbered', 'projects', 'rag'].includes(b.type) || (b.type === 'bars' && b.vertical) ? 1 : 0);
function _wrCardHtml(b) {
    if (_wrBare(b)) return `<div class="wr-bare${b.type === 'gallery' ? ' wr-bare--fill' : ''}">${_wrBlockHtml(b)}</div>`;
    return `<div class="wr-card${b.card ? ' wr-card--' + b.card : ''} wr-card--${b.type}" style="flex-grow:${_wrGrow(b)}">${b.title ? `<div class="wr-card__t">${_wrE(b.title)}</div>` : ''}${_wrBlockHtml(b)}</div>`;
}
// The body of a content page: columns of stacked cards, or (rows) bands of cards side by side.
function _wrBodyHtml(p) {
    if (p.rows) return `<div class="wr-body wr-body--rows">${p.rows.map(r => `<div class="wr-row" style="flex:${r.h}">${r.cards.map((b, k) =>
        _wrCardHtml(b).replace('class="wr-card', `style="flex:${r.widths[k] || 1} 1 0" class="wr-card`)).join('')}</div>`).join('')}</div>`;
    return `<div class="wr-body${p.cols.length === 1 && p.cols[0].length > 1 && p.cols[0].every(b => b.type === 'kv') ? ' wr-body--grid' : ''}">${p.cols.map((c, k) => `<div class="wr-col" style="flex:${p.widths[k]}">${c.map(_wrCardHtml).join('')}</div>`).join('')}</div>`;
}
function weeklyPagesHtml(pages, D, aud) {
    const name = aud === 'mgmt' ? 'Weekly Update' : 'Weekly Operation Report';
    const eyebrow = `${name} · Week ${D.week.no} · ${D.week.label}`;
    return pages.map((p, i) => {
        // 2.38.0: the management deck on the company template (report-assets/).
        if (p.theme === 'msg') {
            if (p.kind === 'cover') return `<section class="wr-page wr-msg wr-msg--cover"><img class="wr-msg__ot" src="icons/icon-192.png" alt="">
                <div class="wr-msg__title"><h1>${_wrE(p.title)}${p.title2 ? `<br>${_wrE(p.title2)}` : ''}</h1><div class="wr-msg__week">${_wrE(p.lines[0] || '')}</div></div></section>`;
            if (p.kind === 'closing') return `<section class="wr-page wr-msg wr-msg--closing"><h1>${_wrE(p.title)}</h1></section>`;
            return `<section class="wr-page wr-msg wr-msg--content${p.sub ? ' has-sub' : ''}"><header class="wr-msg__head"><h2>${_wrE(p.title)}</h2>${p.sub ? `<div class="wr-msg__sub">${_wrE(p.sub)}</div>` : ''}</header>${_wrBodyHtml(p)}</section>`;
        }
        return p.kind === 'cover'
        ? `<section class="wr-page wr-cover"><img class="wr-cover__logo" src="icons/icon-192.png" alt="">
            <div class="wr-cover__main"><div><div class="wr-cover__eyebrow">${_wrE(p.lines[0] || '')}</div><h1>${_wrE(p.title)}${p.title2 ? `<br>${_wrE(p.title2)}` : ''}</h1>${p.lines.slice(1).map(l => `<p>${_wrE(l)}</p>`).join('')}</div></div></section>`
        : `<section class="wr-page"><header class="wr-head"><div class="wr-head__text">${p.eyebrow === '' ? '' : `<div class="wr-eyebrow">${_wrE(p.eyebrow || eyebrow)}</div>`}<h2>${_wrE(p.title)}</h2>${p.sub ? `<div class="wr-head__sub">${_wrE(p.sub)}</div>` : ''}</div><img src="icons/icon-192.png" alt=""></header>
            ${_wrBodyHtml(p)}
            <footer class="wr-foot"><span>${_wrE(name)} · W${D.week.no}</span><span>${_wrE(p.section)} · ${i}</span></footer></section>`;
    }).join('');
}

// ---- The view ----
function renderWeeklyReportView() {
    const sel = document.getElementById('wrWeek');
    if (!sel) return;
    if (!canViewWeeklyReport()) { document.getElementById('wrPages').innerHTML = '<div class="adm-empty">You do not have access to the weekly report.</div>'; return; }
    const daily = _wrAud === 'daily';
    [['wrWeekBox', daily], ['wrDayBox', !daily], ['wrNotesBox', daily], ['wrMediaBox', daily], ['wrDailyHint', !daily]].forEach(([id, hide]) => { const el = document.getElementById(id); if (el) el.hidden = hide; });
    document.querySelectorAll('.wr-aud').forEach(b => b.classList.toggle('active', b.dataset.aud === _wrAud));
    const sub = document.getElementById('wrSub');
    if (sub) sub.textContent = daily ? 'One day for the manager — the Daily Report, issues, field photos, jobs and projects' : 'Weekly (Saturday to Friday) and daily reports, from the Daily Report, tickets, jobs, units and projects';
    if (daily) return renderDailyReportView();
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
    const media = { daily: _wrShow.daily ? refs.daily : [], tickets: _wrShow.tickets ? refs.tickets : [], work: _wrShow.work ? refs.work : [], projects: _wrShow.work ? (refs.projects || {}) : {} };
    Object.values(media.projects).forEach(list => wrLoadMedia(list.filter(r => !_wrHidden.has(r.key)).slice(0, 6)));
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
function setWeeklyReportAudience(a) { _wrAud = ['mgmt', 'daily'].includes(a) ? a : 'team'; renderWeeklyReportView(); }
let _wrNotesT = null;
function setWeeklyReportNotes(v) { _wrNotes = v; clearTimeout(_wrNotesT); _wrNotesT = setTimeout(renderWeeklyReportView, 500); }
function _wrFileName(ext) {
    const D = window._wrLast;
    if (_wrAud === 'daily') return `Daily_Report_${D && D.day ? D.day.date : ''}.${ext}`;
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
    // On paper the full photos, not the small copies the screen shows.
    box.querySelectorAll('img[data-full]').forEach(im => { im.removeAttribute('onload'); im.src = im.dataset.full; });
    await Promise.all([...box.querySelectorAll('img')].map(im => im.decode ? im.decode().catch(() => {}) : null));
    document.body.classList.add('wr-printing');
    const done = () => { document.body.classList.remove('wr-printing'); document.title = old; window.removeEventListener('afterprint', done); };
    window.addEventListener('afterprint', done);
    window.print();
    setTimeout(done, 1500);
}

// ---- Daily Report for the manager (2.39.0) ----
// One day, on the company template: built by functions/dailyCore.js — the
// same code the 07:00 WIT email runs, so the download and the email agree.
let _wrDay = '';
function wrDailyInput(date) {
    const arr = v => Array.isArray(v) ? v : [];
    return { date, now: Date.now(),
        members: typeof teamMembers !== 'undefined' ? arr(teamMembers) : [], shifts: typeof teamShifts !== 'undefined' ? arr(teamShifts) : [],
        leaves: typeof leaveRequests !== 'undefined' ? arr(leaveRequests) : [], workLogs: typeof workLogs !== 'undefined' ? arr(workLogs) : [],
        tickets: typeof tickets !== 'undefined' ? arr(tickets) : [], damages: typeof globalDamages !== 'undefined' ? arr(globalDamages) : [],
        jobs: typeof otJobs !== 'undefined' && canViewView('projects') ? arr(otJobs) : [], projects: typeof projects !== 'undefined' && canViewView('projects') ? arr(projects) : [],
        units: arr(globalData) };
}
// The day's photo references, with how each is loaded on this device.
function wrDailyRefs(D) {
    return D.photos.map(p => Object.assign({ kind: 'photo' }, p, { load: p.src === 'wl'
        ? async () => ((typeof loadWorkLogPhotos === 'function' ? await loadWorkLogPhotos(p.id) : [])[p.index] || '')
        : () => loadWhPhoto(p.id, p.src) }));
}
function renderDailyReportView() {
    if (!window.DailyCore) { document.getElementById('wrPages').innerHTML = '<div class="adm-empty">The daily report could not be loaded — reload the page.</div>'; return; }
    if (!_wrDay) _wrDay = DailyCore.yesterday();
    const inp = document.getElementById('wrDay');
    if (inp) { inp.max = toISODate(); if (document.activeElement !== inp) inp.value = _wrDay; }
    const D = DailyCore.buildDaily(wrDailyInput(_wrDay));
    D.week = { no: '', label: D.day.label, from: D.day.date };   // what the shared renderers read
    const refs = wrDailyRefs(D);
    wrLoadMedia(refs.filter(r => !_wrHidden.has(r.key)));
    _wrPages = DailyCore.dailyPages(D, { gallery: _wrGallery(refs) });
    window._wrLast = D;
    document.getElementById('wrPages').innerHTML = weeklyPagesHtml(_wrPages, D, 'mgmt');
    const c = document.getElementById('wrCount');
    if (c) c.textContent = `${_wrPages.length} pages`;
}
function setDailyReportDay(v) { if (/^\d{4}-\d{2}-\d{2}$/.test(v || '')) { _wrDay = v; renderWeeklyReportView(); } }

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
async function _wrImgData(url) {
    try {
        const b = await (await fetch(url)).blob();
        return await new Promise(res => { const fr = new FileReader(); fr.onload = () => res(String(fr.result).replace(/^data:/, '')); fr.readAsDataURL(b); });
    } catch (_) { return ''; }
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
    const fs = b.compact ? 9 : 9.5, cw = fs * 0.0072, line = fs * 0.0185, base = b.compact ? 0.25 : 0.29;
    const txt = c => String(c && typeof c === 'object' ? (c.text == null ? '' : c.text) : (c == null ? '' : c));
    const n = b.head.length;
    const len = Array.from({ length: n }, (_, i) => Math.max(...[b.head[i].t, ...b.rows.map(r => txt(r[i]))].map(t => t.length)));
    // Short columns (a number, a chip, a name) get exactly the room their text
    // needs, so they never wrap; the long ones share what is left.
    const narrow = wdt < 3.2;
    const fixed = len.map(l => l <= 18 ? Math.max(narrow ? 0.42 : 0.55, l * cw * (narrow ? 1.15 : 1.5) + (narrow ? 0.12 : 0.2)) : 0);
    let rest = wdt - fixed.reduce((a, x) => a + x, 0);
    const longSum = len.reduce((a, l, i) => a + (fixed[i] ? 0 : Math.min(60, l)), 0);
    let colW;
    if (!longSum) { const k = wdt / fixed.reduce((a, x) => a + x, 0); colW = fixed.map(x => x * k); }
    else if (rest < wdt * 0.25) { const weight = len.map(l => Math.max(4, Math.min(42, l))), sum = weight.reduce((a, x) => a + x, 0); colW = weight.map(x => wdt * x / sum); }
    else colW = len.map((l, i) => fixed[i] || rest * Math.min(60, l) / longSum);
    const rowH = r => Math.max(base, ...r.map((c, i) => Math.ceil(Math.max(1, txt(c).length) * cw / Math.max(0.2, colW[i] - 0.1)) * line + 0.1));
    const heights = [base * 0.85, ...b.rows.map(rowH)];
    return { colW, heights, total: heights.reduce((a, x) => a + x, 0), fs };
}
// Rough height of what a card holds on a slide (inches), to stack cards in a column.
function _wrBlockH(b, wdt) {
    switch (b.type) {
    case 'hero': return 1.05 + (b.sub ? 0.24 : 0) + (b.foot ? 0.3 : 0);
    case 'progress': return 1.8;
    case 'steps': return b.items.length ? b.items.length * 0.27 + (b.more ? 0.22 : 0) : 0.3;
    case 'stats': { const per = b.per || (b.small ? 2 : 3), rows = Math.ceil(Math.max(1, b.items.length) / per); return rows * (b.small ? 0.6 : 0.72) + (b.items.some(i => i.delta) ? 0.12 : 0); }
    case 'table': return b.rows.length ? _wrTableLayout(b, wdt || 4).total + (b.more ? 0.22 : 0) : 0.3;
    case 'bars': return b.vertical ? 1.15 : Math.max(1, b.items.length) * 0.27;
    case 'list': return b.items.length ? b.items.reduce((n, i) => n + (String(i.text).length * 0.064 > (wdt || 3) - 0.2 ? 0.36 : 0.2) + (i.sub ? 0.16 : 0) + (i.head ? 0.19 : 0) + (i.note ? 0.17 : 0) + 0.07, 0) : 0.3;
    case 'rag': return b.items.length * 0.42;
    case 'kv': return b.items.length * 0.27;
    case 'panel': return b.items.length * 0.27;
    case 'note': return 0.22;
    case 'numbered': return b.items.length * 0.6;
    case 'projects': return b.items.length * 0.68 + (b.more ? 0.22 : 0);
    case 'gallery': return b.inCard ? 1.2 : 3.75;
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
    // 2.38.0: the company template's pictures, for the management deck.
    const msg = _wrPages.some(p => p.theme === 'msg');
    const bgCover = msg ? await _wrImgData('report-assets/msg-cover.jpg') : '', bgContent = msg ? await _wrImgData('report-assets/msg-content.jpg') : '';
    const C = WR_COL, pres = new PptxGenJS();
    pres.layout = 'LAYOUT_16x9';
    const repName = _wrAud === 'daily' ? 'Daily Report' : _wrAud === 'mgmt' ? 'Precision Health Weekly Update' : 'Weekly Operation Report';
    pres.title = _wrAud === 'daily' ? `Daily Report — ${D.day.label}` : `${repName} — Week ${D.week.no}`;
    pres.company = 'Operation Technology';
    const eyebrow = `${repName} · Week ${D.week.no} · ${D.week.label}`.toUpperCase();
    const face = 'Calibri';
    const tx = (s, t, o) => s.addText(t, Object.assign({ margin: 0, fontFace: face, color: C.ink, valign: 'top' }, o));
    const tone = k => WR_TONE[k] || C.ink;
    const X0 = 0.4, W0 = 9.2, GAP = 0.14, PAD = 0.16;
    // ---- layout: every card of every page gets its box (galleries their picture boxes) ----
    const layout = pg => {
        const isMsg = pg.theme === 'msg';
        const y0 = isMsg ? (pg.sub ? 1.12 : 0.92) : (pg.sub ? 1.42 : 1.32), avail = (isMsg ? 5.4 : 5.22) - y0, cards = [];
        if (pg.rows) {
            const hsum = pg.rows.reduce((a, r) => a + r.h, 0), room = avail - GAP * (pg.rows.length - 1);
            let cy = y0;
            pg.rows.forEach(r => {
                const rh = room * r.h / hsum, wsum = r.widths.reduce((a, x) => a + x, 0), space = W0 - GAP * (r.cards.length - 1);
                let cx = X0;
                r.cards.forEach((b, k) => { const cw = space * (r.widths[k] || 1) / wsum; cards.push({ b, x: cx, y: cy, w: cw, h: rh }); cx += cw + GAP; });
                cy += rh + GAP;
            });
            return cards;
        }
        if (pg.cols.length === 1 && pg.cols[0].length > 1 && pg.cols[0].every(b => b.type === 'kv')) {
            const per = 3, list = pg.cols[0], rows = Math.ceil(list.length / per), cw = (W0 - GAP * (per - 1)) / per, ch = (avail - GAP * (rows - 1)) / rows;
            list.forEach((b, j) => cards.push({ b, x: X0 + (j % per) * (cw + GAP), y: y0 + Math.floor(j / per) * (ch + GAP), w: cw, h: ch }));
            return cards;
        }
        const total = pg.widths.reduce((a, x) => a + x, 0), space = W0 - GAP * (pg.cols.length - 1);
        let cx = X0;
        pg.cols.forEach((col, k) => {
            const cw = space * pg.widths[k] / total;
            const nat = col.map(b => _wrBare(b) ? _wrBlockH(b, cw) : PAD * 2 + (b.title ? 0.26 : 0) + _wrBlockH(b, cw - PAD * 2));
            const room = avail - GAP * (col.length - 1), sum = nat.reduce((a, x) => a + x, 0);
            const grow = col.map(_wrGrow), gsum = grow.reduce((a, x) => a + x, 0);
            // Too tall: the cards that can give (tables, lists) shrink; a hero card keeps its size.
            const keep = col.map(b => b.type === 'hero' || b.type === 'progress');
            const keptH = nat.reduce((a, h, i) => a + (keep[i] ? h : 0), 0), flexH = sum - keptH;
            const hs = nat.map((h, i) => sum > room ? (keep[i] || !flexH ? h : h * Math.max(0.2, room - keptH) / flexH) : h + (gsum ? (room - sum) * grow[i] / gsum : 0));
            let cy = y0;
            col.forEach((b, i) => { cards.push({ b, x: cx, y: cy, w: cw, h: hs[i] }); cy += hs[i] + GAP; });
            cx += cw + GAP;
        });
        return cards;
    };
    const pages = _wrPages.map(pg => ({ pg, cards: pg.kind === 'content' ? layout(pg) : [] }));
    // Pictures: cut to their boxes first (and the videos read, when asked for).
    const prep = new Map();
    let videosLeft = 0;
    for (const { cards } of pages) for (const c of cards) if (c.b.type === 'gallery') {
        const b = c.b;
        if (b.inCard) { const ix = c.x + PAD, iy = c.y + PAD + (b.title ? 0.26 : 0), iw = c.w - PAD * 2, ih = c.y + c.h - PAD - iy, n = b.items.length, cols = n > 1 ? 2 : 1, rows = Math.ceil(n / cols), g = 0.08;
            c.tiles = b.items.map((_, i) => ({ x: ix + (i % cols) * ((iw - g * (cols - 1)) / cols + g), y: iy + Math.floor(i / cols) * ((ih - g * (rows - 1)) / rows + g), w: (iw - g * (cols - 1)) / cols, h: Math.max(0.3, (ih - g * (rows - 1)) / rows), cap: 0 }));
        } else c.tiles = _wrGalleryGeom(b.items.length, c.x, c.y, c.w, c.h);
        for (let i = 0; i < b.items.length; i++) {
            const it = b.items[i], g = c.tiles[i], asp = g.w / g.h;
            if (it.kind === 'video') {
                const cover = await _wrCrop(it.poster, asp, 'png');
                let data = '';
                if (_wrEmbedVideo) { data = await _wrVideoData(it.src, it.size); if (!data) videosLeft++; }
                prep.set(it.key, { img: cover, video: data });
            } else prep.set(it.key, { img: await _wrCrop(it.src, asp) });
        }
    }
    const rect = (s, x, y, w, h, fill, line, radius) => s.addShape(radius === 0 ? pres.ShapeType.rect : pres.ShapeType.roundRect,
        Object.assign({ x, y, w, h, fill: { color: fill }, line: line ? { color: line, width: 0.75 } : { color: fill, width: 0 } }, radius === 0 ? {} : { rectRadius: radius == null ? 0.1 : radius }));
    const cellOpt = (c, h, sepColor) => { const o = c && typeof c === 'object' ? c : { text: c };
        return { text: String(o.text == null ? '' : o.text), options: { align: o.a || (h && h.a) || 'left', color: o.chip ? tone(o.chip) : o.tone ? tone(o.tone) : C.ink, bold: !!o.bold || !!o.chip,
            border: [{ pt: 0.5, color: sepColor }, { type: 'none' }, { type: 'none' }, { type: 'none' }] } }; };
    // What a card holds, drawn inside (x, y, w, h). hero: on the teal card (white text).
    const drawBlock = (s, c, x, y, w, h, hero) => {
        const b = c.b, ink = hero ? 'FFFFFF' : C.ink, soft = hero ? C.heroSub : C.slate, sep = c.b.card === 'warn' ? 'FDE3C3' : 'EEF0F2';
        switch (b.type) {
        case 'hero':
            tx(s, String(b.value), { x, y, w, h: 0.82, fontSize: 46, bold: true, color: hero ? 'FFFFFF' : C.green, valign: 'middle', fit: 'shrink' });
            tx(s, b.label, { x, y: y + 0.86, w, h: 0.22, fontSize: 10, color: ink, fit: 'shrink' });
            if (b.sub) tx(s, b.sub, { x, y: y + 1.1, w, h: 0.22, fontSize: 9.5, color: soft });
            if (b.foot) tx(s, b.foot, { x, y: y + h - 0.3, w, h: 0.3, fontSize: 9.5, color: soft, valign: 'bottom', fit: 'shrink' });
            break;
        case 'progress': {
            tx(s, [{ text: String(b.pct), options: { fontSize: 46 } }, { text: '%', options: { fontSize: 15 } }], { x, y, w, h: 0.82, bold: true, color: 'FFFFFF', valign: 'middle' });
            tx(s, `${b.label}${b.delta ? ` · ${b.delta > 0 ? '▲' : '▼'} ${Math.abs(b.delta)} pts this week` : b.delta === 0 ? ' · no change this week' : ''}`, { x, y: y + 0.86, w, h: 0.22, fontSize: 10, color: 'FFFFFF', fit: 'shrink' });
            rect(s, x, y + 1.2, w, 0.09, '3F9189', null, 0.045);
            if (b.pct) rect(s, x, y + 1.2, Math.max(0.09, w * b.pct / 100), 0.09, 'FFFFFF', null, 0.045);
            if (b.timePct != null) s.addShape(pres.ShapeType.line, { x: x + w * b.timePct / 100, y: y + 1.14, w: 0, h: 0.21, line: { color: 'FDBA74', width: 1.5 } });
            tx(s, [{ text: [b.start && b.end ? `${b.start} → ${b.end}` : (b.end || ''), b.daysLeft == null ? '' : b.daysLeft >= 0 ? `${b.daysLeft} days left` : `${-b.daysLeft} days late`].filter(Boolean).join(' · ') + (b.rag ? ' · ' : '') }, { text: b.rag || '', options: { bold: true } }],
                { x, y: y + 1.36, w, h: 0.36, fontSize: 9, color: C.heroSub });
            break; }
        case 'steps':
            if (!b.items.length) { tx(s, b.empty || '—', { x, y, w, h: 0.3, fontSize: 10, color: soft }); break; }
            { const rh = Math.min(0.3, (h - (b.more ? 0.22 : 0)) / b.items.length);
            b.items.forEach((m, i) => { const cy = y + i * rh;
                if (i) s.addShape(pres.ShapeType.line, { x, y: cy, w, h: 0, line: { color: sep, width: 0.5 } });
                rect(s, x, cy + rh / 2 - 0.075, 0.15, 0.15, m.done ? C.green : C.tint, null, 0.035);
                if (m.done) tx(s, '✓', { x, y: cy + rh / 2 - 0.075, w: 0.15, h: 0.15, fontSize: 7.5, bold: true, color: 'FFFFFF', align: 'center', valign: 'middle' });
                const gw = m.goods ? 0.5 : 0, ww = m.weight != null ? 0.4 : 0;
                const lw = w - 0.24 - gw - ww - 0.05, maxc = Math.max(8, Math.floor(lw / 0.066));
                tx(s, m.label.length > maxc ? m.label.slice(0, maxc - 1) + '…' : m.label, { x: x + 0.24, y: cy, w: lw, h: rh, fontSize: 9.5, color: m.done ? C.slate : C.ink, valign: 'middle' });
                if (m.weight != null) tx(s, `${m.weight}%`, { x: x + w - gw - ww, y: cy, w: ww, h: rh, fontSize: 8.5, color: C.slate, align: 'right', valign: 'middle' });
                if (m.goods) { const ok = m.goods.split('/')[0] === m.goods.split('/')[1]; tx(s, `▣ ${m.goods}`, { x: x + w - gw, y: cy, w: gw, h: rh, fontSize: 8.5, bold: true, color: ok ? C.good : C.rust, align: 'right', valign: 'middle' }); } });
            if (b.more) tx(s, `+ ${b.more} more in the dashboard`, { x, y: y + b.items.length * rh + 0.03, w, h: 0.2, fontSize: 8.5, color: C.slate }); }
            break;
        case 'stats': { const per = b.per || (b.small ? 2 : 3), cw = w / per, rows = Math.ceil(Math.max(1, b.items.length) / per), rh = Math.min(b.small ? 0.7 : 0.95, h / rows);
            b.items.forEach((st, i) => { const cx = x + (i % per) * cw, cy = y + Math.floor(i / per) * rh;
                tx(s, _wrNum(st.value), { x: cx, y: cy, w: cw - 0.08, h: rh * 0.5, fontSize: b.small ? 18 : 22, bold: true, color: st.tone ? tone(st.tone) : ink, valign: 'bottom', fit: 'shrink' });
                tx(s, st.label, { x: cx, y: cy + rh * 0.52, w: cw - 0.08, h: 0.2, fontSize: 8.5, color: soft, fit: 'shrink' });
                if (st.delta) tx(s, st.delta.text, { x: cx, y: cy + rh * 0.52 + 0.19, w: cw - 0.08, h: 0.17, fontSize: 8, bold: true, color: tone(st.delta.tone) }); });
            if (!b.items.length) tx(s, b.empty || '—', { x, y, w, h: 0.3, fontSize: 10, color: soft });
            break; }
        case 'table': {
            if (!b.rows.length) { tx(s, b.empty || 'Nothing this week', { x, y, w, h: 0.3, fontSize: 10, color: soft }); break; }
            const H = b.head.map(hd => ({ text: String(hd.t).toUpperCase(), options: { bold: true, color: C.slate, fontSize: 7.5, align: hd.a || 'left', border: [{ type: 'none' }, { type: 'none' }, { type: 'none' }, { type: 'none' }] } }));
            const L = _wrTableLayout(b, w);
            // Rows that do not fit the card are left to the dashboard ("+ n more"), never drawn over its edge.
            let fit = b.rows.length;
            while (fit > 1 && L.heights.slice(0, fit + 1).reduce((a, x) => a + x, 0) + ((b.more || fit < b.rows.length) ? 0.22 : 0) > h) fit--;
            const more = (b.more || 0) + (b.rows.length - fit), heights = L.heights.slice(0, fit + 1), tot = heights.reduce((a, x) => a + x, 0);
            const R = b.rows.slice(0, fit).map(r => r.map((cc, i) => cellOpt(cc, b.head[i], sep)));
            s.addTable([H, ...R], { x, y, w, colW: L.colW, rowH: heights, fontFace: face, fontSize: L.fs, color: C.ink, valign: 'middle', margin: [0.03, 0.05, 0.03, 0.05], autoPage: false });
            if (more) tx(s, `+ ${more} more in the dashboard`, { x, y: y + tot + 0.03, w, h: 0.2, fontSize: 8.5, color: C.slate });
            break; }
        case 'bars': {
            if (!b.items.length) { tx(s, b.empty || 'Nothing this week', { x, y, w, h: 0.3, fontSize: 10, color: soft }); break; }
            const max = b.max || Math.max(1, ...b.items.map(i => Number(i[1]) || 0));
            if (b.vertical) { const n = b.items.length, g = 0.07, bw = (w - g * (n - 1)) / n, ch = Math.max(0.4, h - 0.42);
                b.items.forEach((it, i) => { const bh = Math.max(0.02, ch * (it[1] || 0) / max), bx = x + i * (bw + g), col = i === b.highlight ? C.green : C.sage;
                    rect(s, bx, y + 0.2 + ch - bh, bw, bh, col, null, 0);
                    tx(s, _wrH(it[1]), { x: bx - 0.05, y: y + 0.2 + ch - bh - 0.19, w: bw + 0.1, h: 0.18, fontSize: 8, align: 'center', valign: 'bottom' });
                    tx(s, it[0], { x: bx - 0.05, y: y + 0.24 + ch, w: bw + 0.1, h: 0.18, fontSize: 8, color: C.slate, align: 'center' }); });
                break; }
            const lw = Math.min(1.7, w * 0.42), vw = 0.52, tw = w - lw - vw - 0.1, rh = Math.min(0.3, h / b.items.length);
            b.items.forEach((it, i) => { const cy = y + i * rh, v = it[1];
                tx(s, it[0], { x, y: cy, w: lw, h: rh, fontSize: 9.5, valign: 'middle', fit: 'shrink' });
                rect(s, x + lw, cy + rh / 2 - 0.04, tw, 0.08, C.tint, null, 0.04);
                const col = b.target && v != null ? (v >= b.target ? C.green : v >= b.target - 5 ? 'F79009' : C.rust) : C.green;
                if (v) rect(s, x + lw, cy + rh / 2 - 0.04, Math.max(0.08, tw * Math.min(v, max) / max), 0.08, col, null, 0.04);
                if (b.target) s.addShape(pres.ShapeType.line, { x: x + lw + tw * b.target / max, y: cy + rh / 2 - 0.09, w: 0, h: 0.18, line: { color: '344054', width: 0.75, dashType: 'dash' } });
                tx(s, v == null ? '—' : _wrH(v) + (b.unit || ''), { x: x + lw + tw + 0.06, y: cy, w: vw + 0.04, h: rh, fontSize: 9.5, bold: true, align: 'right', valign: 'middle' }); });
            break; }
        case 'list': {
            if (!b.items.length) { tx(s, b.empty || '—', { x, y, w, h: 0.3, fontSize: 10, color: soft }); break; }
            let cy = y, shown = 0;
            const tagW = b.items.some(it => it.tag) ? 0.78 : 0;
            for (const it of b.items) {
                // Lines the text takes in its width (pptxgenjs does not measure), at most three.
                const tw = w - 0.18 - tagW, lines = Math.min(3, Math.max(1, Math.ceil(String(it.text).length * 9.5 * 0.0072 / tw)));
                const hh = it.head ? 0.19 : 0, th = 0.17 * lines + 0.03, subH = it.sub ? Math.min(2, Math.ceil(String(it.sub).length * 8 * 0.0072 / (w - 0.18))) * 0.15 : 0, nh = it.note ? 0.17 : 0;
                const need = hh + th + subH + nh;
                if (shown && cy + need > y + h - (b.more ? 0.2 : 0)) break;
                s.addShape(pres.ShapeType.ellipse, { x, y: cy + 0.07, w: 0.085, h: 0.085, fill: { color: it.tone === 'warn' ? 'F79009' : it.tone === 'bad' ? C.rust : it.tone === 'good' ? C.green : '98A2B3' }, line: { color: 'FFFFFF', width: 0 } });
                if (it.head) tx(s, it.head, { x: x + 0.18, y: cy, w: tw, h: 0.18, fontSize: 9.5, bold: true, fit: 'shrink' });
                tx(s, String(it.text), { x: x + 0.18, y: cy + hh, w: tw, h: th, fontSize: 9.5, fit: 'shrink' });
                if (it.tag) { const tc = it.tag.tone === 'warn' ? 'B54708' : it.tag.tone === 'bad' ? C.rust : C.good, bg = it.tag.tone === 'warn' ? 'FFFAEB' : it.tag.tone === 'bad' ? 'FEF3F2' : 'ECFDF3';
                    rect(s, x + w - tagW + 0.06, cy + hh + 0.01, tagW - 0.06, 0.19, bg, null, 0.09);
                    tx(s, it.tag.text, { x: x + w - tagW + 0.06, y: cy + hh + 0.01, w: tagW - 0.06, h: 0.19, fontSize: 7.5, bold: true, color: tc, align: 'center', valign: 'middle' }); }
                if (it.sub) tx(s, it.sub, { x: x + 0.18, y: cy + hh + th, w: w - 0.18, h: subH, fontSize: 8, color: C.slate, fit: 'shrink' });
                if (it.note) tx(s, it.note, { x: x + 0.18, y: cy + hh + th + subH, w: w - 0.18, h: 0.17, fontSize: 8, color: C.amber, bold: true, fit: 'shrink' });
                cy += need + 0.09; shown++;
            }
            const left = (b.more || 0) + b.items.length - shown;
            if (left) tx(s, `+ ${left} more in the dashboard`, { x, y: cy, w, h: 0.2, fontSize: 8.5, color: C.slate });
            break; }
        case 'rag': { const rh = Math.min(0.5, h / b.items.length);
            b.items.forEach((r, i) => { const cy = y + i * rh;
                if (i) s.addShape(pres.ShapeType.line, { x, y: cy, w, h: 0, line: { color: sep, width: 0.5 } });
                s.addShape(pres.ShapeType.ellipse, { x, y: cy + rh / 2 - 0.055, w: 0.11, h: 0.11, fill: { color: r[1] === 'warn' ? 'F79009' : r[1] === 'bad' ? C.rust : r[1] === 'good' ? C.green : '98A2B3' }, line: { color: 'FFFFFF', width: 0 } });
                tx(s, r[0], { x: x + 0.22, y: cy, w: w * 0.52, h: rh, fontSize: 11, bold: true, valign: 'middle' });
                tx(s, r[2], { x: x + w * 0.5, y: cy, w: w * 0.5, h: rh, fontSize: 10, color: C.slate, align: 'right', valign: 'middle' }); });
            break; }
        case 'kv': { const rh = Math.min(0.32, h / Math.max(1, b.items.length));
            b.items.forEach((it, i) => { const cy = y + i * rh;
                if (i) s.addShape(pres.ShapeType.line, { x, y: cy, w, h: 0, line: { color: sep, width: 0.5 } });
                tx(s, it[0], { x, y: cy, w: w - 0.8, h: rh, fontSize: 10, color: C.slate, valign: 'middle' });
                tx(s, _wrNum(it[1]), { x: x + w - 0.8, y: cy, w: 0.8, h: rh, fontSize: 11, bold: true, align: 'right', valign: 'middle' }); });
            break; }
        case 'panel': tx(s, b.items.map((t, k) => ({ text: t, options: { bullet: true, breakLine: k < b.items.length - 1 } })), { x, y, w, h, fontSize: 10 }); break;
        case 'note': tx(s, b.text, { x, y, w, h: 0.22, fontSize: 8.5, color: C.slate }); break;
        case 'numbered': { const rh = Math.min(0.62, h / b.items.length);
            b.items.forEach((it, i) => { const cy = y + i * rh;
                rect(s, x, cy + rh / 2 - 0.17, 0.34, 0.34, C.green, null, 0.07);
                tx(s, String(i + 1).padStart(2, '0'), { x, y: cy + rh / 2 - 0.17, w: 0.34, h: 0.34, fontSize: 11, bold: true, color: 'FFFFFF', align: 'center', valign: 'middle' });
                tx(s, it, { x: x + 0.5, y: cy, w: w - 0.5, h: rh, fontSize: 13, valign: 'middle', fit: 'shrink' }); });
            break; }
        case 'projects':
            b.items.forEach((p, i) => { const cy = y + i * 0.68, t2 = p.rag === 'red' ? 'bad' : p.rag === 'amber' ? 'warn' : 'good', col = t2 === 'good' ? C.green : tone(t2);
                s.addShape(pres.ShapeType.ellipse, { x, y: cy + 0.07, w: 0.1, h: 0.1, fill: { color: col }, line: { color: 'FFFFFF', width: 0 } });
                tx(s, p.name, { x: x + 0.2, y: cy, w: w * 0.33, h: 0.24, fontSize: 10.5, bold: true, fit: 'shrink' });
                tx(s, [p.type, p.pt, p.pic].filter(Boolean).join(' · '), { x: x + 0.2, y: cy + 0.25, w: w * 0.33, h: 0.2, fontSize: 8.5, color: C.slate });
                tx(s, `${p.pct}%`, { x: x + w * 0.37, y: cy, w: 0.7, h: 0.34, fontSize: 16, bold: true, color: col });
                rect(s, x + w * 0.46, cy + 0.08, w * 0.22, 0.08, C.tint, null, 0.04);
                if (p.pct) rect(s, x + w * 0.46, cy + 0.08, Math.max(0.08, w * 0.22 * p.pct / 100), 0.08, col, null, 0.04);
                tx(s, `${p.label} · ${p.due}`, { x: x + w * 0.46, y: cy + 0.22, w: w * 0.24, h: 0.34, fontSize: 8, color: C.slate, fit: 'shrink' });
                tx(s, p.week || '—', { x: x + w * 0.72, y: cy, w: w * 0.28, h: 0.55, fontSize: 9, color: C.slate, fit: 'shrink' }); });
            if (b.more) tx(s, `+ ${b.more} more in the dashboard`, { x, y: y + b.items.length * 0.68, w, h: 0.2, fontSize: 8.5, color: C.slate });
            break;
        case 'gallery':
            (c.tiles || []).forEach((g, i) => {
                const it = b.items[i], pr = prep.get(it.key) || {};
                if (it.kind === 'video' && pr.video) {
                    const data = pr.video.replace(/^data:/, ''), extn = ((it.name || '').split('.').pop() || 'mp4').toLowerCase();
                    const media = { type: 'video', data, extn, x: g.x, y: g.y, w: g.w, h: g.h };
                    if (pr.img) media.cover = pr.img;
                    s.addMedia(media);
                } else if (pr.img) s.addImage({ data: pr.img, x: g.x, y: g.y, w: g.w, h: g.h, rounding: false });
                else {
                    rect(s, g.x, g.y, g.w, g.h, 'E7ECEA', null, 0.08);
                    tx(s, it.kind === 'video' ? 'Video — open it in the dashboard' : 'Photo in the dashboard', { x: g.x, y: g.y, w: g.w, h: g.h, fontSize: 9, color: C.slate, align: 'center', valign: 'middle' });
                }
                if (it.kind === 'video' && !pr.video) {
                    const d = Math.min(g.w, g.h) * 0.28;
                    s.addShape(pres.ShapeType.ellipse, { x: g.x + (g.w - d) / 2, y: g.y + (g.h - d) / 2, w: d, h: d, fill: { color: '000000', transparency: 45 }, line: { color: 'FFFFFF', width: 1.5 } });
                    s.addShape(pres.ShapeType.triangle, { x: g.x + (g.w - d * 0.38) / 2 + d * 0.05, y: g.y + (g.h - d * 0.42) / 2, w: d * 0.38, h: d * 0.42, rotate: 90, fill: { color: 'FFFFFF' }, line: { color: 'FFFFFF', width: 0 } });
                }
                if (g.cap) { tx(s, it.cap, { x: g.x, y: g.y + g.h + 0.05, w: g.w, h: 0.2, fontSize: 9, bold: true, fit: 'shrink' });
                    tx(s, it.sub, { x: g.x, y: g.y + g.h + 0.25, w: g.w, h: 0.18, fontSize: 8, color: C.slate, fit: 'shrink' }); }
            });
            break;
        }
    };
    const MSG_GREEN = '385723';
    pages.forEach(({ pg: p, cards }, i) => {
        const s = pres.addSlide();
        if (p.theme === 'msg' && p.kind !== 'content') {
            s.background = bgCover ? { data: bgCover } : { color: 'FFFFFF' };
            if (p.kind === 'cover') {
                if (logo) s.addImage({ data: logo, x: 5.74, y: 0.24, w: 0.65, h: 0.65 });
                tx(s, [{ text: p.title, options: { breakLine: !!p.title2 } }, ...(p.title2 ? [{ text: p.title2 }] : [])], { x: 3.7, y: 1.83, w: 5.75, h: 1.27, fontSize: 33, bold: true, color: MSG_GREEN, valign: 'middle', fit: 'shrink' });
                tx(s, String(p.lines[0] || '').toUpperCase(), { x: 3.7, y: 3.1, w: 5.6, h: 0.24, fontSize: 8.5, bold: true, color: MSG_GREEN, charSpacing: 1.5 });
            } else tx(s, p.title, { x: 1.83, y: 1.3, w: 6.67, h: 1.43, fontSize: 37.5, bold: true, color: '000000', align: 'center', valign: 'middle' });
            return;
        }
        if (p.kind === 'cover') {
            s.background = { color: C.green };
            if (logo) { s.addShape(pres.ShapeType.ellipse, { x: 8.55, y: 0.38, w: 0.95, h: 0.95, fill: { color: 'FFFFFF' }, line: { color: 'FFFFFF', width: 0 } }); s.addImage({ data: logo, x: 8.6, y: 0.43, w: 0.85, h: 0.85 }); }
            tx(s, String(p.lines[0] || '').toUpperCase(), { x: 0.6, y: 2.3, w: 7.5, h: 0.3, fontSize: 11, bold: true, color: C.heroSub, charSpacing: 1.5 });
            tx(s, [{ text: p.title, options: { breakLine: !!p.title2 } }, ...(p.title2 ? [{ text: p.title2 }] : [])], { x: 0.6, y: 2.68, w: 8.2, h: 1.2, fontSize: 30, bold: true, color: 'FFFFFF', valign: 'top', fit: 'shrink' });
            if (p.lines.length > 1) tx(s, p.lines.slice(1).join('\n'), { x: 0.6, y: 3.95, w: 7.5, h: 0.6, fontSize: 12, color: 'D5EFEA' });
            return;
        }
        if (p.theme === 'msg') {
            s.background = bgContent ? { data: bgContent } : { color: 'FFFFFF' };
            tx(s, p.title, { x: 0.39, y: 0.12, w: 8.55, h: 0.58, fontSize: 24, bold: true, color: MSG_GREEN, valign: 'middle', fit: 'shrink' });
            if (p.sub) tx(s, p.sub, { x: 0.39, y: 0.82, w: 9.1, h: 0.22, fontSize: 10.5, color: C.slate, fit: 'shrink' });
        } else {
        s.background = { color: C.bg };
        const noEyebrow = p.eyebrow === '';
        if (!noEyebrow) tx(s, (p.eyebrow ? String(p.eyebrow).toUpperCase() : eyebrow), { x: X0, y: 0.26, w: 8.4, h: 0.2, fontSize: 8, bold: true, color: C.green, charSpacing: 1.2 });
        tx(s, p.title, { x: X0, y: noEyebrow ? 0.34 : 0.47, w: 8.5, h: p.sub ? 0.5 : 0.7, fontSize: 19, bold: true, valign: noEyebrow ? 'middle' : (p.sub ? 'middle' : 'top'), fit: 'shrink' });
        if (p.sub) tx(s, p.sub, { x: X0, y: 1.0, w: 8.5, h: 0.24, fontSize: 9.5, color: C.slate, fit: 'shrink' });
        if (logo) s.addImage({ data: logo, x: 9.22, y: 0.28, w: 0.38, h: 0.38 });
        tx(s, `${repName} · W${D.week.no}`, { x: X0, y: 5.33, w: 6, h: 0.2, fontSize: 7.5, color: C.slate });
        tx(s, `${p.section} · ${i}`, { x: 7.6, y: 5.33, w: 2.0, h: 0.2, fontSize: 7.5, color: C.slate, align: 'right' });
        }
        cards.forEach(c => {
            const b = c.b;
            if (_wrBare(b)) { drawBlock(s, c, c.x, c.y, c.w, c.h, false); return; }
            const hero = b.card === 'hero', warn = b.card === 'warn';
            rect(s, c.x, c.y, c.w, c.h, hero ? C.green : warn ? C.warnBg : C.card, hero ? null : warn ? C.warnLine : C.hair, 0.1);
            let iy = c.y + PAD;
            if (b.title) { tx(s, String(b.title).toUpperCase(), { x: c.x + PAD, y: iy, w: c.w - PAD * 2, h: 0.2, fontSize: 7.5, bold: true, color: hero ? C.heroSub : warn ? C.amber : '475467', charSpacing: 0.8, fit: 'shrink' }); iy += 0.26; }
            drawBlock(s, c, c.x + PAD, iy, c.w - PAD * 2, Math.max(0.2, c.y + c.h - PAD - iy), hero);
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
    const w = D.week, out = { daily: [], tickets: [], work: [], projects: {} };
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
        // 2.37.0: the pictures of each running project — this week's first, else its latest.
        out.projects = {};
        (typeof projects !== 'undefined' ? projects : []).filter(p => !PJ_CLOSED.includes(p.status)).forEach(p => {
            const refs = [];
            const ups = (p.updates || []).filter(u => (u.photoIds || []).length).sort((a, b) => (_wrIn(b.date, w) ? 1 : 0) - (_wrIn(a.date, w) ? 1 : 0) || String(b.date || '').localeCompare(String(a.date || '')));
            ups.slice(0, 3).forEach(u => u.photoIds.slice(0, 2).forEach(id => refs.push({ key: `pj:${id}`, kind: 'photo', cap: dayName(u.date), sub: (u.text || '').slice(0, 50), load: () => loadWhPhoto(id, 'pj') })));
            (typeof otJobs !== 'undefined' ? otJobs : []).filter(j => j.projectId === p.id && _wrIn(j.date, w)).forEach(j => (j.photoIds || []).slice(0, 2).forEach(id =>
                refs.push({ key: `pj:${id}`, kind: 'photo', cap: dayName(j.date), sub: `${j.category}`.slice(0, 50), load: () => loadWhPhoto(id, 'pj') })));
            if (refs.length) out.projects[p.id] = refs.slice(0, 6);
        });
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
function _wrGallery(refs, max) {
    return { type: 'gallery', items: _wrVisible(refs).slice(0, max || WR_GALLERY_MAX).map(r => { const m = _wrMedia.get(r.key) || { state: 'loading' };
        return { key: r.key, kind: r.kind, cap: r.cap, sub: r.sub, state: m.state, src: m.src || '', poster: m.poster || '', name: r.name || '', size: r.size || 0, type: r.type || '' }; }) };
}
function _wrGalleryHtml(b) {
    if (!b.items.length) return '<div class="wr-empty">No pictures this week</div>';
    return `<div class="wr-gal wr-gal--${Math.min(b.items.length, WR_GALLERY_MAX)}${b.inCard ? ' wr-gal--mini' : ''}">${b.items.map(it => {
        const img = it.kind === 'video' ? it.poster : it.src;
        const pic = it.state === 'loading' ? '<div class="wr-gal__ph">Loading…</div>'
            : img ? (it.kind === 'video' ? `<img src="${_wrE(img)}" alt="">` : `<img ${thumbAttrs(img, true)} alt="">`) : `<div class="wr-gal__ph"><i class="fas fa-film" aria-hidden="true"></i></div>`;
        return `<figure class="wr-gal__item"><div class="wr-gal__pic">${pic}${it.kind === 'video' ? '<span class="wr-gal__play" aria-hidden="true"></span>' : ''}
            <button type="button" class="wr-gal__x wr-noprint" title="Leave this out of the report" aria-label="Leave this picture out" onclick="hideWeeklyMedia(${jsArg(it.key)})">&times;</button></div>
            <figcaption><b>${_wrE(it.cap)}</b><span>${_wrE(it.sub)}</span></figcaption></figure>`; }).join('')}</div>`;
}
