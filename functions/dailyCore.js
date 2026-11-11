// Daily Report for the manager (2.39.0) — one day of field work in one deck:
// the Daily Report summary, issues & damage, field photos, OT jobs and project
// progress. Pure: give it the records, it returns the numbers (buildDaily),
// the pages (dailyPages, the same page model as weeklyreport.js) and the email
// (dailyEmail). The SAME file runs in the browser (window.DailyCore, loaded by
// index.html) and in the Cloud Function that mails it at 07:00 WIT, so the
// download and the email always say the same. tests/daily_core_test.js.
(function (root, factory) {
    if (typeof module === 'object' && module.exports) module.exports = factory();
    else root.DailyCore = factory();
}(typeof self !== 'undefined' ? self : this, function () {
    'use strict';
    const PRIO_HOURS = { high: 4, medium: 24, low: 72 };
    const PRIO_LABEL = { high: 'High', medium: 'Medium', low: 'Low' };
    const TK_STATUS = { open: 'Open', assigned: 'Assigned', progress: 'In progress', waiting: 'Waiting', resolved: 'Resolved', closed: 'Closed' };
    const TK_DONE = ['resolved', 'closed'];
    const JOB_STATUS = { open: 'Open', progress: 'In progress', done: 'Done' };
    const PJ_CLOSED = ['done', 'cancelled'];
    const PHOTO_MAX = 6;

    const pad = n => String(n).padStart(2, '0');
    const isoOf = d => `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())}`;
    const dayOf = iso => new Date(iso + 'T00:00:00');
    function dayInfo(date) {
        const d = dayOf(date), e = new Date(d); e.setDate(e.getDate() + 1);
        const p = new Date(d); p.setDate(p.getDate() - 1);
        // Spelled out here, not by the locale: the browser and the server write it the same.
        const WD = ['Sunday', 'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday'];
        const MO = ['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October', 'November', 'December'];
        return { date, prev: isoOf(p), start: d.getTime(), end: e.getTime(),
            label: `${WD[d.getDay()]}, ${d.getDate()} ${MO[d.getMonth()]} ${d.getFullYear()}`,
            short: `${WD[d.getDay()].slice(0, 3)} ${d.getDate()} ${MO[d.getMonth()].slice(0, 3)} ${d.getFullYear()}` };
    }
    // Yesterday, seen from `now` (local time — WIT on the server).
    function yesterday(now) { const d = new Date(now == null ? Date.now() : now); d.setDate(d.getDate() - 1); return isoOf(d); }

    const hhmm = v => { const x = /^(\d{1,2}):(\d{2})/.exec(String(v || '')); return x ? (+x[1]) * 60 + (+x[2]) : null; };
    function minutes(l) { const s = hhmm(l && l.start), e = hhmm(l && l.end); if (s == null || e == null) return 0; const d = e - s; return d < 0 ? d + 1440 : d; }
    const submitted = r => (r && r.approval) !== 'draft';
    function logUnits(l) { if (Array.isArray(l.units) && l.units.length) return l.units; return l.unitId ? [{ id: l.unitId, name: l.unitName || '', sn: l.sn || '' }] : []; }
    function logJobs(l) {
        if (Array.isArray(l.jobs) && l.jobs.length) return l.jobs;
        const u = logUnits(l);
        return l.paddock || u.length ? [{ paddock: l.paddock || '', jobType: '', units: u }] : [];
    }
    const photoCount = l => (Array.isArray(l.photos) && l.photos.length) ? l.photos.length : (Number(l.photoCount) || 0);
    const pt = s => { const t = String(s || '').replace(/^PT\.?\s*/i, '').trim(); if (!t) return '—'; const w = t.split(/\s+/); return w.length > 1 ? w.map(x => x[0]).join('').toUpperCase() : t; };
    const h1 = n => (Math.round((n || 0) * 10) / 10).toLocaleString('en-US');
    const plural = (n, one, many) => `${n} ${n === 1 ? one : (many || one + 's')}`;
    const first = s => String(s || '').trim().split(/\s+/)[0] || '';
    const clip = (s, n) => { s = String(s || '').replace(/\s+/g, ' ').trim(); return s.length > n ? s.slice(0, n - 1).trimEnd() + '…' : s; };
    function progressPct(p) {
        const t = (p && p.target) || {};
        if (t.mode === 'count') { const tot = Math.max(0, Number(t.count) || 0), d = Math.min(tot, Math.max(0, Number(t.done) || 0)); return tot ? Math.round(100 * d / tot) : 0; }
        if (t.mode === 'milestones') { const ms = t.milestones || [], all = ms.reduce((n, m) => n + Math.max(0, Number(m.weight) || 0), 0);
            return all ? Math.round(100 * ms.filter(m => m.done).reduce((n, m) => n + Math.max(0, Number(m.weight) || 0), 0) / all) : 0; }
        const us = t.units || []; return us.length ? Math.round(100 * us.filter(u => u.done).length / us.length) : 0;
    }

    // in: { date, members, shifts, leaves, workLogs, tickets, damages, jobs, projects, units, now }
    function buildDaily(input) {
        const I = input || {}, day = dayInfo(I.date || yesterday(I.now));
        const now = I.now == null ? Date.now() : I.now;
        const date = day.date, inDay = ms => typeof ms === 'number' && ms >= day.start && ms < day.end;
        const units = I.units || [], nameOf = id => { const u = units.find(x => x.id === id); return u ? (u.name || u.sn || '') : ''; };
        const unitName = u => (u && (nameOf(u.id) || u.name || u.sn)) || '';
        const members = (I.members || []).filter(m => m.active !== false).slice().sort((a, b) => String(a.name || '').localeCompare(String(b.name || '')));
        const shifts = I.shifts || [], leaves = I.leaves || [];
        const shiftOf = id => { const s = shifts.find(x => x.memberId === id && x.date === date); return s ? s.shift || '' : ''; };
        const onLeave = id => leaves.some(r => r.memberId === id && r.approval === 'approved' && r.dateFrom && date >= r.dateFrom && date <= (r.dateTo || r.dateFrom));
        const logs = (I.workLogs || []).filter(l => l.date === date && submitted(l));
        const anyShift = members.some(m => shiftOf(m.id));
        const SHIFT = { pagi: 'Morning', siang: 'Afternoon', malam: 'Night', libur: 'Off' };

        // ---- Daily Report ----
        let expected = 0, filed = 0;
        const rows = members.map(m => {
            const mine = logs.filter(l => l.memberId === m.id);
            const sh = shiftOf(m.id);
            let state;
            if (mine.length) { state = 'ok'; expected++; filed++; }
            else if (onLeave(m.id)) state = 'leave';
            else if (anyShift && (!sh || sh === 'libur')) state = 'off';
            else { state = 'miss'; expected++; }
            const jobs = mine.flatMap(l => l.place === 'office' ? [{ paddock: 'Office', jobType: clip(l.task, 40), units: [] }] : logJobs(l));
            return { id: m.id, name: m.name || '', pt: pt(m.company), shift: SHIFT[sh] || '', state,
                hours: mine.reduce((n, l) => n + minutes(l), 0) / 60,
                work: [...new Set(jobs.map(j => [j.paddock, j.jobType].filter(Boolean).join(' · ')).filter(Boolean))].join('; '),
                units: [...new Set(jobs.flatMap(j => (j.units || []).map(unitName)).filter(Boolean))].join(', ') };
        });
        // A report from someone not (or no longer) on the member list still counts as work done.
        const byType = new Map(), paddocks = new Set(), unitSet = new Set();
        logs.forEach(l => {
            const jobs = l.place === 'office' ? [] : logJobs(l).filter(j => j.paddock || j.jobType);
            const share = jobs.length ? minutes(l) / 60 / jobs.length : 0;
            jobs.forEach(j => { const t = j.jobType || 'Other'; byType.set(t, (byType.get(t) || 0) + share);
                if (j.paddock) paddocks.add(j.paddock); (j.units || []).forEach(u => { const k = unitName(u); if (k) unitSet.add(k); }); });
        });
        const team = { members: rows, expected, filed, pct: expected ? Math.round(1000 * filed / expected) / 10 : null,
            missed: rows.filter(r => r.state === 'miss').map(r => r.name), leave: rows.filter(r => r.state === 'leave').map(r => r.name),
            hours: logs.reduce((n, l) => n + minutes(l), 0) / 60, reports: logs.length, office: logs.filter(l => l.place === 'office').length,
            paddocks: paddocks.size, units: unitSet.size, jobTypes: [...byType.entries()].sort((a, b) => b[1] - a[1]),
            issues: logs.filter(l => String(l.issue || '').trim()).map(l => ({ who: l.memberName || '', text: String(l.issue).trim() })) };

        // ---- Issues & damage ----
        const broken = u => !!u && String(u.status || '').trim().toLowerCase() === 'breakdown';
        const dmg = I.damages || [];
        const damage = dmg.filter(d => d.date === date).map(d => ({ unit: d.unitName || nameOf(d.unitId) || d.sn || '', pt: pt(d.site), part: d.component || '', type: d.damageType || '',
            what: d.description || '', resolved: !!d.resolved, breakdown: !d.resolved && broken(units.find(u => u.id === d.unitId)) }));
        const fixed = dmg.filter(d => d.resolved && d.resolvedAt === date).length;
        const tk = I.tickets || [];
        const evAt = (t, type) => (t.events || []).filter(e => e.type === type).map(e => e.at).sort((a, b) => b - a)[0];
        const tkRow = t => ({ no: t.no || '', title: t.title || '', prio: PRIO_LABEL[t.priority] || '', priority: t.priority || 'medium', status: TK_STATUS[t.status] || t.status || '',
            pic: t.assigneeName || '', units: (t.units || []).map(unitName).filter(Boolean).join(', '), category: t.category || '' });
        const openNow = tk.filter(t => !TK_DONE.includes(t.status));
        const tickets = { opened: tk.filter(t => inDay(t.reportedAt)).map(tkRow), resolved: tk.filter(t => inDay(t.resolvedAt || evAt(t, 'resolved'))).map(tkRow),
            open: openNow.length, overdue: openNow.filter(t => now > (t.reportedAt || 0) + (PRIO_HOURS[t.priority] || 24) * 3600000).length };
        const breakdownNow = units.filter(broken).length;

        // ---- OT jobs & project progress ----
        const tkOf = j => (j.ticketId && tk.find(t => t.id === j.ticketId)) || tk.find(t => (t.jobIds || []).includes(j.id));
        const pjs = I.projects || [];
        const jobs = (I.jobs || []).filter(j => j.date === date).map(j => { const t = tkOf(j), p = j.projectId && pjs.find(x => x.id === j.projectId);
            return { category: j.category || 'Other', what: j.description || '', units: (j.units || []).map(unitName).filter(Boolean).join(', '), status: JOB_STATUS[j.status] || 'Done',
                done: (j.status || 'done') === 'done', hours: Number(j.hours) || 0, ticket: t ? t.no : '', project: p ? p.name : '', issue: String(j.issue || '').trim() }; });
        const updates = [];
        pjs.forEach(p => (p.updates || []).filter(u => u.date === date).forEach(u => updates.push({ project: p.name || '', pct: progressPct(p), text: u.text || '', progress: u.progress || '',
            blocker: u.blocker || '', by: u.by || '', auto: !!u.auto })));
        const running = pjs.filter(p => !PJ_CLOSED.includes(p.status)).map(p => ({ name: p.name || '', pct: progressPct(p), end: p.end || '',
            late: !!p.end && p.end < date, today: updates.some(u => u.project === p.name) })).sort((a, b) => (b.today - a.today) || a.name.localeCompare(b.name));

        // ---- Photos: Daily Report paddock lines (different paddocks first), tickets, jobs, projects ----
        const cand = [];
        logs.filter(l => photoCount(l) > 0).forEach(l => {
            const js = l.place === 'office' ? [] : logJobs(l), counts = js.map(j => Math.max(0, Math.floor(Number(j.photoCount) || 0))), sum = counts.reduce((a, b) => a + b, 0);
            const who = first(l.memberName);
            if (sum && sum <= photoCount(l)) { let k = 0; js.forEach((j, i) => { if (counts[i]) cand.push({ key: `wl:${l.id}:${i}`, src: 'wl', id: l.id, index: k, group: j.paddock || '',
                cap: [j.paddock, j.jobType].filter(Boolean).join(' · ') || 'Daily Report', sub: `${who}${(j.units || []).length ? ' · ' + j.units.map(unitName).filter(Boolean).join(', ') : ''}` }); k += counts[i]; }); }
            else cand.push({ key: `wl:${l.id}:g`, src: 'wl', id: l.id, index: 0, group: (js[0] || {}).paddock || l.place || '',
                cap: l.place === 'office' ? `Office · ${clip(l.task, 40)}` : ([(js[0] || {}).paddock, (js[0] || {}).jobType].filter(Boolean).join(' · ') || 'Daily Report'), sub: who });
        });
        const seen = new Set(), daily = [];
        while (cand.length) { let b = cand.findIndex(c => !seen.has(c.group)); if (b < 0) b = 0; const c = cand.splice(b, 1)[0]; seen.add(c.group); daily.push(c); }
        const other = [];
        tk.filter(t => (t.photoIds || []).length && (inDay(t.reportedAt) || (t.events || []).some(e => inDay(e.at)))).forEach(t =>
            other.push({ key: `tk:${t.photoIds[0]}`, src: 'tk', id: t.photoIds[0], cap: clip(`${t.no || ''} · ${t.title || ''}`, 60), sub: `Ticket · ${TK_STATUS[t.status] || t.status || ''}` }));
        (I.jobs || []).filter(j => j.date === date && (j.photoIds || []).length).forEach(j =>
            other.push({ key: `pj:${j.photoIds[0]}`, src: 'pj', id: j.photoIds[0], cap: clip(`${j.category || 'Job'} · ${j.description || ''}`, 60), sub: (j.units || []).map(unitName).filter(Boolean).join(', ') || 'Job' }));
        pjs.forEach(p => (p.updates || []).filter(u => u.date === date && (u.photoIds || []).length).forEach(u =>
            other.push({ key: `pj:${u.photoIds[0]}`, src: 'pj', id: u.photoIds[0], cap: clip(p.name || 'Project', 60), sub: clip(u.text, 60) })));
        // Four from the Daily Report and the rest from the others when both have them; spares after.
        const photos = [...daily.slice(0, 4), ...other.slice(0, PHOTO_MAX - Math.min(4, daily.length)), ...daily.slice(4), ...other.slice(PHOTO_MAX - Math.min(4, daily.length))]
            .filter((p, i, a) => a.findIndex(x => x.key === p.key) === i).slice(0, PHOTO_MAX + 4);

        return { day, team, damage, fixed, tickets, breakdownNow, jobs, updates, running, photos,
            generatedAt: new Date(now).toLocaleString('en-GB', { day: 'numeric', month: 'short', year: 'numeric', hour: '2-digit', minute: '2-digit' }) };
    }

    // The deck, on the company template (theme 'msg', as the management
    // Weekly Update). gallery: the photo block the caller filled (or null).
    function dailyPages(D, opts) {
        opts = opts || {};
        const t = D.team, tk = D.tickets, P = [];
        const page = (title, extra) => P.push(Object.assign({ kind: 'content', theme: 'msg', eyebrow: '', section: title, title, cols: [], widths: [] }, extra));
        P.push({ kind: 'cover', theme: 'msg', title: 'Daily Report', title2: 'Division Operation Technology', lines: [D.day.label] });
        const ST = { ok: { text: 'Filed', tone: 'good', bold: true }, miss: { text: 'Missed', tone: 'bad', bold: true }, leave: { text: 'Leave', tone: 'warn' }, off: { text: 'Day off', tone: 'muted' } };
        const rows = t.members.filter(m => m.state !== 'off').concat(t.members.filter(m => m.state === 'off'));
        page('Daily Report Summary', { sub: `${D.day.label} · ${t.filed} of ${t.expected} expected reports filed${t.missed.length ? ' · missed: ' + t.missed.map(first).join(', ') : ''}`, rows: [
            { h: 1, widths: [1.5, 1.2, 1], cards: [
                { type: 'stats', small: true, per: 3, items: [{ value: t.pct == null ? '—' : `${t.pct}%`, label: `filed (${t.filed}/${t.expected})`, tone: t.pct == null ? '' : t.pct >= 100 ? 'good' : t.pct >= 80 ? 'warn' : 'bad' },
                    { value: h1(t.hours), label: 'work hours' }, { value: t.reports, label: 'reports' }, { value: t.paddocks, label: 'paddocks' }, { value: t.units, label: 'units used' }, { value: t.missed.length, label: 'missed', tone: t.missed.length ? 'bad' : '' }] },
                { type: 'bars', title: 'Hours by job type', items: t.jobTypes.slice(0, 4).map(x => [x[0], Math.round(x[1] * 10) / 10]), empty: 'No field work' },
                { type: 'list', title: 'Missed / leave', card: t.missed.length ? 'warn' : '', items: t.missed.map(n => ({ text: n, tone: 'bad' })).concat(t.leave.map(n => ({ text: `${n} (leave)`, tone: 'warn' }))).slice(0, 5), empty: 'Nobody missed a report' }] },
            { h: 2.1, widths: [1], cards: [
                { type: 'table', head: [{ t: 'Member' }, { t: 'PT' }, { t: 'Status' }, { t: 'Paddock · job' }, { t: 'Units' }, { t: 'Hours', a: 'right' }],
                    rows: rows.map(m => [{ text: m.name, bold: true }, m.pt, ST[m.state], m.work || '—', { text: m.units || '—', tone: 'muted' }, m.hours ? h1(m.hours) : '—']), empty: 'No team members' }] }] });
        const dmgTone = d => d.resolved ? 'good' : d.breakdown ? 'bad' : 'warn';
        page('Issue & Damage', { sub: `${plural(t.issues.length, 'issue')} in the Daily Report · ${plural(D.damage.length, 'new damage record')} · ${plural(tk.opened.length, 'ticket')} opened, ${tk.resolved.length} resolved`, rows: [
            { h: 0.8, widths: [1], cards: [{ type: 'stats', small: true, per: 6, items: [{ value: t.issues.length, label: 'field issues', tone: t.issues.length ? 'warn' : '' }, { value: D.damage.length, label: 'new damage', tone: D.damage.length ? 'bad' : '' },
                { value: D.fixed, label: 'repairs closed', tone: D.fixed ? 'good' : '' }, { value: tk.opened.length, label: 'tickets opened' }, { value: tk.resolved.length, label: 'tickets resolved', tone: tk.resolved.length ? 'good' : '' },
                { value: tk.open, label: `open now${tk.overdue ? ` · ${tk.overdue} past due` : ''}`, tone: tk.overdue ? 'bad' : '' }] }] },
            { h: 2.3, widths: [1, 1.15, 1.25], cards: [
                { type: 'list', title: 'Issues from the Daily Report', items: t.issues.slice(0, 5).map(i => ({ text: clip(i.text, 150), sub: i.who, tone: 'warn' })), empty: 'No issue written' },
                { type: 'table', title: 'New damage / breakdown', head: [{ t: 'Unit' }, { t: 'Part' }, { t: 'Status' }],
                    rows: D.damage.map(d => [{ text: d.unit, bold: true }, clip([d.part, d.what].filter(Boolean).join(' — '), 60) || d.type || '—', { text: d.resolved ? 'Fixed' : d.breakdown ? 'Breakdown' : 'Open', tone: dmgTone(d), bold: true }]), empty: 'No new damage' },
                { type: 'table', title: 'Tickets opened & resolved', head: [{ t: 'Ticket' }, { t: 'Prio' }, { t: 'Status' }],
                    rows: tk.opened.map(x => [{ text: clip(`${x.no} · ${x.title}`, 60), bold: true }, { text: x.prio, tone: x.priority === 'high' ? 'bad' : x.priority === 'medium' ? 'warn' : 'muted' }, x.status])
                        .concat(tk.resolved.filter(x => !tk.opened.some(o => o.no === x.no)).map(x => [clip(`${x.no} · ${x.title}`, 60), { text: x.prio, tone: 'muted' }, { text: 'Resolved', tone: 'good', bold: true }])), empty: 'No ticket opened or resolved' }] }] });
        if (opts.gallery && opts.gallery.items && opts.gallery.items.length) {
            const g = opts.gallery, groups = new Set(g.items.map(i => i.cap.split(' · ')[0]));
            page('Field Photos', { sub: `${plural(g.items.length, 'photo')} from ${plural(groups.size, 'place')} · ${D.day.label}`, cols: [[g]], widths: [1] });
        }
        const done = D.jobs.filter(j => j.done).length;
        page('OT Jobs & Project Progress', { sub: `${plural(D.jobs.length, 'job')} logged (${done} done) · ${plural(D.updates.length, 'project update')}`, rows: [
            { h: 1, widths: [1], cards: [
                { type: 'table', title: 'Jobs & activities', head: [{ t: 'Category' }, { t: 'What was done' }, { t: 'Units' }, { t: 'Status' }],
                    rows: D.jobs.map(j => [{ text: j.category, bold: true, tone: 'good' }, clip(j.what + (j.ticket ? ` · ${j.ticket}` : '') + (j.project ? ` · ${j.project}` : ''), 110), { text: j.units || '—', tone: 'muted' },
                        { text: j.status, tone: j.done ? 'good' : 'warn' }]), empty: 'No job logged this day' }] },
            { h: 1, widths: [1.45, 1], cards: [
                { type: 'list', title: 'Project updates', items: D.updates.filter(u => !u.auto || D.updates.every(x => x.auto)).slice(0, 5).map(u => ({ text: clip(`${u.project}: ${u.text || u.progress}`, 150),
                    sub: [u.progress && u.text ? clip(u.progress, 70) : '', u.blocker ? 'Blocker: ' + clip(u.blocker, 60) : '', u.by].filter(Boolean).join(' · '), tone: u.blocker ? 'warn' : 'good' })), empty: 'No project update this day' },
                { type: 'bars', title: 'Running projects, %', items: D.running.slice(0, 5).map(p => [p.name, p.pct]), max: 100, unit: '%', empty: 'No running project' }] }] });
        P.push({ kind: 'closing', theme: 'msg', title: 'Terima Kasih' });
        return P;
    }

    // The email body: the same four parts, readable without opening a file.
    const esc = s => String(s == null ? '' : s).replace(/[&<>"']/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));
    function dailyEmail(D, opts) {
        opts = opts || {};
        const t = D.team, tk = D.tickets, G = '#385723';
        const th = 'style="padding:6px 8px;background:#F2F4F7;font-size:12px;text-align:left"', td = 'style="padding:6px 8px;border-bottom:1px solid #EAECF0;font-size:13px;vertical-align:top"';
        const table = (head, rows, empty) => rows.length ? `<table style="border-collapse:collapse;width:100%;margin:6px 0 14px"><thead><tr>${head.map(h => `<th ${th}>${esc(h)}</th>`).join('')}</tr></thead><tbody>${rows.map(r => `<tr>${r.map(c => `<td ${td}>${esc(c)}</td>`).join('')}</tr>`).join('')}</tbody></table>`
            : `<p style="color:#667085;margin:4px 0 14px">${esc(empty)}</p>`;
        const h = s => `<h3 style="margin:18px 0 4px;font-size:15px;color:${G}">${esc(s)}</h3>`;
        const ST = { ok: 'Filed', miss: 'Missed', leave: 'Leave', off: 'Day off' };
        const html = `<div style="font-family:Arial,Helvetica,sans-serif;color:#101828;max-width:760px">
<h2 style="margin:0 0 2px;font-size:19px;color:${G}">Daily Report — Division Operation Technology</h2>
<p style="margin:0 0 10px;color:#475467">${esc(D.day.label)}${opts.attached ? ' · PowerPoint and PDF attached' : ''}</p>
<p style="margin:0 0 6px;font-size:14px"><b>${t.filed} of ${t.expected}</b> expected reports filed${t.pct == null ? '' : ` (${t.pct}%)`} · <b>${h1(t.hours)}</b> work hours · ${t.paddocks} paddocks · ${t.units} units${t.missed.length ? `<br><span style="color:#B42318">Missed: ${esc(t.missed.join(', '))}</span>` : ''}</p>
${h('1. Daily Report summary')}${table(['Member', 'PT', 'Status', 'Paddock · job', 'Hours'], t.members.map(m => [m.name, m.pt, ST[m.state], m.work || '-', m.hours ? h1(m.hours) : '-']), 'No team members')}
${h('2. Issue & damage')}${table(['From', 'Issue'], t.issues.map(i => [i.who, i.text]), 'No issue written in the Daily Report')}
${table(['Unit', 'Part / damage', 'Status'], D.damage.map(d => [d.unit, [d.part, d.what].filter(Boolean).join(' — ') || d.type, d.resolved ? 'Fixed' : d.breakdown ? 'Breakdown' : 'Open']), 'No new damage record')}
${table(['Ticket', 'Priority', 'Status', 'PIC'], tk.opened.concat(tk.resolved.filter(x => !tk.opened.some(o => o.no === x.no))).map(x => [`${x.no} · ${x.title}`, x.prio, x.status, x.pic || '-']), 'No ticket opened or resolved')}
<p style="margin:0 0 6px;color:#475467;font-size:13px">Tickets open now: <b>${tk.open}</b>${tk.overdue ? ` · <span style="color:#B42318">${tk.overdue} past the due time</span>` : ''} · Units in breakdown now: <b>${D.breakdownNow}</b></p>
${h('3. Field photos')}<p style="margin:0 0 6px;color:#475467;font-size:13px">${opts.photos ? `${plural(opts.photos, 'photo')} in the attached PowerPoint / PDF.` : 'No photo this day.'}</p>
${h('4. OT jobs & project progress')}${table(['Category', 'What was done', 'Units', 'Status'], D.jobs.map(j => [j.category, j.what + (j.ticket ? ` · ${j.ticket}` : ''), j.units || '-', j.status]), 'No job logged this day')}
${table(['Project', 'Update', 'Progress'], D.updates.map(u => [u.project, u.text + (u.blocker ? ` (Blocker: ${u.blocker})` : ''), u.progress || `${u.pct}%`]), 'No project update this day')}
<p style="margin-top:18px;font-size:12px;color:#667085">Sent by the <a href="https://ot.techorchestrator.cloud">Operation Technology Dashboard</a> · Reports → Daily Report for the full deck.</p></div>`;
        const text = [`Daily Report — Division Operation Technology`, D.day.label, '',
            `${t.filed} of ${t.expected} expected reports filed · ${h1(t.hours)} work hours · ${t.paddocks} paddocks · ${t.units} units`,
            t.missed.length ? `Missed: ${t.missed.join(', ')}` : 'Nobody missed a report',
            `Issues: ${t.issues.length} · New damage: ${D.damage.length} · Tickets opened ${tk.opened.length}, resolved ${tk.resolved.length}, open now ${tk.open}`,
            `Jobs: ${D.jobs.length} · Project updates: ${D.updates.length}`, '', 'Sent by the Operation Technology Dashboard — https://ot.techorchestrator.cloud'].join('\n');
        return { subject: `Daily Report — ${D.day.short}`, html, text };
    }

    return { buildDaily, dailyPages, dailyEmail, dayInfo, yesterday, PHOTO_MAX };
}));
