// Calendar (2.41.0) — owner only for now. One place for what happened and what is
// coming: shifts & leave, the Daily Report, ticket SLA, heavy-equipment inspections,
// license expiry, projects (start, deadline, milestone targets and ticks) and damage.
// Nothing is stored: every entry is read from the records the dashboard already has.
// A month grid on a wide screen, an agenda list on a phone; tap a day for its list.
'use strict';

const CAL_CATS = [
    { key: 'team', label: 'Shifts & leave', color: '#2E90FA' },
    { key: 'report', label: 'Daily Report', color: '#12B76A' },
    { key: 'ticket', label: 'Tickets', color: '#F04438' },
    { key: 'inspection', label: 'Inspections', color: '#F79009' },
    { key: 'license', label: 'Licenses', color: '#7A5AF8' },
    { key: 'project', label: 'Projects', color: '#9E6A3C' },
    { key: 'damage', label: 'Damage', color: '#667085' }
];
let _calMonth = '', _calSel = '', _calMode = '';
const _calOff = new Set((() => { try { return JSON.parse(localStorage.getItem('ot.calOff') || '[]'); } catch (_) { return []; } })());

function canViewCalendar() { return typeof isOwner === 'function' && isOwner(); }
const _calIso = d => `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, '0')}-${String(d.getDate()).padStart(2, '0')}`;
const _calDay = iso => new Date(iso + 'T00:00:00');
const _calAdd = (iso, n) => { const d = _calDay(iso); d.setDate(d.getDate() + n); return _calIso(d); };
const _calMsDay = ms => typeof ms === 'number' && ms > 0 ? _calIso(new Date(ms)) : '';
const _calList = (names, max) => names.length <= (max || 3) ? names.join(', ') : `${names.slice(0, max || 3).join(', ')} +${names.length - (max || 3)}`;

// Every entry between two dates (inclusive), as { date: [ { cat, text, sub, tone, go } ] }.
// tone: '' | 'good' | 'warn' | 'bad' | 'muted'; go: what a tap opens.
function calendarEntries(from, to, today) {
    today = today || toISODate();
    const out = {}, add = (date, e) => { if (date && date >= from && date <= to) (out[date] = out[date] || []).push(e); };
    const inRange = d => d && d >= from && d <= to;
    const members = typeof activeMembers === 'function' ? activeMembers() : [];
    const nameOf = id => (members.find(m => m.id === id) || {}).name || '';
    const days = []; for (let d = from; d <= to; d = _calAdd(d, 1)) days.push(d);

    // Shifts & leave, and (for days gone by) who filed the Daily Report.
    const shifts = typeof teamShifts !== 'undefined' ? teamShifts : [], leaves = typeof leaveRequests !== 'undefined' ? leaveRequests : [];
    const logs = typeof workLogs !== 'undefined' ? workLogs : [];
    const SH = { pagi: 'Morning', siang: 'Afternoon', malam: 'Night', libur: 'Off' };
    days.forEach(d => {
        const sh = shifts.filter(s => s.date === d && SH[s.shift]);
        if (sh.length) {
            const n = k => sh.filter(s => s.shift === k).length;
            add(d, { cat: 'team', text: ['pagi', 'siang', 'malam', 'libur'].filter(k => n(k)).map(k => `${SH[k]} ${n(k)}`).join(' · '), sub: 'Shift schedule', go: { view: 'team' } });
        }
        const on = leaves.filter(r => r.dateFrom && d >= r.dateFrom && d <= (r.dateTo || r.dateFrom));
        const ok = on.filter(r => r.approval === 'approved').map(r => nameOf(r.memberId) || r.memberName || '?');
        const wait = on.filter(r => r.approval === 'pending').map(r => nameOf(r.memberId) || r.memberName || '?');
        if (ok.length) add(d, { cat: 'team', text: `Leave: ${_calList(ok)}`, sub: ok.join(', '), go: { view: 'team' } });
        if (wait.length) add(d, { cat: 'team', text: `Leave waiting: ${_calList(wait)}`, sub: wait.join(', '), tone: 'muted', go: { view: 'team' } });
        if (d > today) return;
        // Expected: on a shift (not off) and not on approved leave; without a schedule, nobody is expected.
        const away = new Set(on.filter(r => r.approval === 'approved').map(r => r.memberId));
        const expected = members.filter(m => sh.some(s => s.memberId === m.id && s.shift !== 'libur') && !away.has(m.id));
        const filed = new Set(logs.filter(l => l.date === d && (l.approval || 'pending') !== 'draft').map(l => l.memberId));
        if (expected.length || filed.size) {
            const miss = expected.filter(m => !filed.has(m.id)).map(m => (m.name || '').split(' ')[0]);
            const got = expected.filter(m => filed.has(m.id)).length;
            add(d, { cat: 'report', text: expected.length ? `Daily Report ${got}/${expected.length}${miss.length ? ' · missing ' + _calList(miss) : ''}` : `Daily Report: ${filed.size} filed`,
                sub: miss.length ? `Not filed: ${miss.join(', ')}` : 'Everyone expected filed', tone: miss.length ? (d === today ? 'muted' : 'warn') : 'good', go: { view: 'team' } });
        }
    });

    // Tickets: opened, resolved, and the SLA due date of those still open.
    const tks = typeof tickets !== 'undefined' ? tickets : [], done = typeof TK_DONE !== 'undefined' ? TK_DONE : ['resolved', 'closed'];
    tks.forEach(t => {
        const go = { ticket: t.id }, nm = `${t.no || ''} · ${t.title || ''}`;
        add(_calMsDay(t.reportedAt), { cat: 'ticket', text: `Opened ${t.no || ''}`, sub: nm, tone: 'muted', go });
        if (done.includes(t.status)) { add(_calMsDay(t.resolvedAt || t.closedAt), { cat: 'ticket', text: `Resolved ${t.no || ''}`, sub: nm, tone: 'good', go }); return; }
        const due = typeof ticketDueAt === 'function' ? ticketDueAt(t) : 0, dd = _calMsDay(due);
        const late = due && Date.now() > due;
        add(dd, { cat: 'ticket', text: `Due ${t.no || ''}`, sub: `${nm}${t.assigneeName ? ' · PIC ' + t.assigneeName : ' · no PIC'}`, tone: late ? 'bad' : 'warn', go });
        // Past its time: also on today, so it is not missed.
        if (late && dd < today) add(today, { cat: 'ticket', text: `Overdue ${t.no || ''}`, sub: `${nm} · due ${dd}`, tone: 'bad', go });
    });

    // Heavy-equipment inspections: the next due date of each unit; overdue or never checked on today.
    if (typeof inspectableUnits === 'function' && typeof inspectionStatusFor === 'function') {
        const due = new Map(), late = [];
        inspectableUnits().forEach(u => { const st = inspectionStatusFor(u, today); const n = u.name || u.sn || '';
            if (st.key === 'overdue' || st.key === 'never') late.push(`${n}${st.key === 'never' ? ' (never)' : ''}`);
            else if (st.next) due.set(st.next, (due.get(st.next) || []).concat(n)); });
        due.forEach((names, d) => add(d, { cat: 'inspection', text: `Inspection due: ${_calList(names)}`, sub: names.join(', '), tone: 'warn', go: { view: 'inspection' } }));
        if (late.length) add(today, { cat: 'inspection', text: `${late.length} inspection${late.length === 1 ? '' : 's'} overdue`, sub: late.join(', '), tone: 'bad', go: { view: 'inspection' } });
    }

    // Licenses: the day each GPS / display license ends (tractors), grouped per day and kind.
    if (typeof licenseExpiryEnd === 'function') {
        const ends = new Map();
        (typeof globalData !== 'undefined' ? globalData : []).filter(u => !(typeof isHeavy === 'function' && isHeavy(u))).forEach(u => ['gps', 'display'].forEach(kind => {
            const end = licenseExpiryEnd(u, kind);
            if (!inRange(end)) return;
            const k = `${end}|${kind}`; ends.set(k, (ends.get(k) || []).concat({ id: u.id, name: u.name || u.sn || '' }));
        }));
        ends.forEach((list, k) => { const [d, kind] = k.split('|');
            add(d, { cat: 'license', text: `${kind === 'gps' ? 'GPS' : 'Display'} license ends: ${_calList(list.map(x => x.name))}`, sub: list.map(x => x.name).join(', '), tone: d < today ? 'bad' : 'warn',
                go: list.length === 1 ? { unit: list[0].id } : { view: 'licenseStock' } }); });
    }

    // Projects: start, deadline, milestone targets, milestones and units ticked done.
    (typeof projects !== 'undefined' ? projects : []).forEach(p => {
        const go = { project: p.id }, closed = ['done', 'cancelled'].includes(p.status), t = p.target || {};
        add(p.start, { cat: 'project', text: `Project starts: ${p.name || ''}`, sub: p.pic ? `PIC ${p.pic}` : '', tone: 'muted', go });
        if (p.end) add(p.end, { cat: 'project', text: `Project due: ${p.name || ''}`, sub: closed ? 'Done' : (p.end < today ? 'Late' : ''), tone: closed ? 'good' : p.end < today ? 'bad' : 'warn', go });
        if (p.doneAt && p.status === 'done') add(_calMsDay(p.doneAt), { cat: 'project', text: `Project done: ${p.name || ''}`, tone: 'good', go });
        (t.mode === 'milestones' ? t.milestones || [] : []).forEach(m => {
            if (m.due && !m.done) add(m.due, { cat: 'project', text: `Target: ${m.label}`, sub: `${p.name || ''}${m.due < today ? ' · late' : ''}`, tone: m.due < today ? 'bad' : 'warn', go });
            if (m.done && m.doneAt) add(m.doneAt, { cat: 'project', text: `Done: ${m.label}`, sub: p.name || '', tone: 'good', go });
        });
    });

    // Damage: new records and repairs closed.
    (typeof globalDamages !== 'undefined' ? globalDamages : []).forEach(d => {
        const u = d.unitName || d.sn || '', what = [d.component, d.description].filter(Boolean).join(' — ');
        add(d.date, { cat: 'damage', text: `Damage: ${u}`, sub: what, tone: d.resolved ? 'muted' : 'bad', go: { view: 'damage' } });
        if (d.resolved && d.resolvedAt) add(d.resolvedAt, { cat: 'damage', text: `Repaired: ${u}`, sub: what, tone: 'good', go: { view: 'damage' } });
    });
    return out;
}

// ---- The view ----
function renderCalendarView() {
    const host = document.getElementById('calHost');
    if (!host) return;
    if (!canViewCalendar()) { host.innerHTML = '<div class="adm-empty">The calendar is for the owner.</div>'; return; }
    const today = toISODate();
    if (!_calMonth) _calMonth = today.slice(0, 7);
    if (!_calSel) _calSel = today;
    const mode = _calMode || (window.innerWidth < 760 ? 'agenda' : 'month');
    const first = _calMonth + '-01', d0 = _calDay(first);
    const last = _calIso(new Date(d0.getFullYear(), d0.getMonth() + 1, 0));
    // The grid runs Monday to Sunday, whole weeks.
    const gridFrom = _calAdd(first, -((d0.getDay() + 6) % 7)), lastD = _calDay(last), gridTo = _calAdd(last, (7 - lastD.getDay()) % 7);
    const all = calendarEntries(gridFrom, gridTo, today);
    const shown = d => (all[d] || []).filter(e => !_calOff.has(e.cat));
    const cat = k => CAL_CATS.find(c => c.key === k) || CAL_CATS[0];
    const monthName = d0.toLocaleDateString('en-GB', { month: 'long', year: 'numeric' });
    const chips = CAL_CATS.map(c => `<button type="button" class="cal-chip${_calOff.has(c.key) ? ' is-off' : ''}" style="--c:${c.color}" aria-pressed="${!_calOff.has(c.key)}" onclick="calToggleCat('${c.key}')"><i></i>${escapeHtml(c.label)}</button>`).join('');
    const entry = (e, i) => `<li class="cal-e cal-e--${e.tone || 'none'}" style="--c:${cat(e.cat).color}"><button type="button" class="cal-e__b" onclick="calOpen(${jsArg(JSON.stringify(e.go || {}))})">
        <span class="cal-e__t">${escapeHtml(e.text)}</span>${e.sub ? `<span class="cal-e__s">${escapeHtml(e.sub)}</span>` : ''}<span class="cal-e__k">${escapeHtml(cat(e.cat).label)}</span></button></li>`;
    const dayTitle = d => _calDay(d).toLocaleDateString('en-GB', { weekday: 'long', day: 'numeric', month: 'long', year: 'numeric' });
    const detail = d => { const list = shown(d); return `<div class="cal-detail"><h3>${escapeHtml(dayTitle(d))}${d === today ? ' <span class="cal-today-tag">Today</span>' : ''}</h3>
        ${list.length ? `<ul class="cal-elist">${list.map(entry).join('')}</ul>` : '<p class="adm-sub">Nothing on this day.</p>'}</div>`; };
    let body;
    if (mode === 'month') {
        const cells = []; for (let d = gridFrom; d <= gridTo; d = _calAdd(d, 1)) cells.push(d);
        body = `<div class="cal-grid" role="grid" aria-label="${escapeHtml(monthName)}">
            ${['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun'].map(n => `<div class="cal-wd" role="columnheader">${n}</div>`).join('')}
            ${cells.map(d => { const list = shown(d), out = d.slice(0, 7) !== _calMonth;
                return `<button type="button" role="gridcell" class="cal-cell${out ? ' is-out' : ''}${d === today ? ' is-today' : ''}${d === _calSel ? ' is-sel' : ''}${d < today ? ' is-past' : ''}" onclick="calSelect('${d}')" aria-label="${escapeHtml(dayTitle(d))}: ${list.length} item${list.length === 1 ? '' : 's'}">
                    <span class="cal-cell__n">${_calDay(d).getDate()}</span>
                    ${list.slice(0, 3).map(e => `<span class="cal-pill cal-pill--${e.tone || 'none'}" style="--c:${cat(e.cat).color}">${escapeHtml(e.text)}</span>`).join('')}
                    ${list.length > 3 ? `<span class="cal-more">+${list.length - 3} more</span>` : ''}</button>`; }).join('')}
        </div>${detail(_calSel)}`;
    } else {
        const ds = []; for (let d = first; d <= last; d = _calAdd(d, 1)) if (shown(d).length) ds.push(d);
        body = ds.length ? `<div class="cal-agenda">${ds.map(d => `<section class="cal-aday${d === today ? ' is-today' : ''}" id="cal-${d}"><h3>${escapeHtml(dayTitle(d))}${d === today ? ' <span class="cal-today-tag">Today</span>' : ''}</h3>
            <ul class="cal-elist">${shown(d).map(entry).join('')}</ul></section>`).join('')}</div>` : '<div class="adm-empty">Nothing this month.</div>';
    }
    const n = Object.keys(all).filter(d => d >= first && d <= last).reduce((a, d) => a + shown(d).length, 0);
    host.innerHTML = `<div class="cal-bar">
            <div class="cal-nav"><button type="button" class="btn btn-secondary btn-sm" onclick="calShift(-1)" aria-label="Previous month">‹</button>
                <h2 class="cal-month">${escapeHtml(monthName)}</h2>
                <button type="button" class="btn btn-secondary btn-sm" onclick="calShift(1)" aria-label="Next month">›</button>
                <button type="button" class="btn btn-secondary btn-sm" onclick="calToday()">Today</button></div>
            <div class="cal-modes" role="tablist" aria-label="View"><button type="button" role="tab" class="cal-mode${mode === 'month' ? ' active' : ''}" aria-selected="${mode === 'month'}" onclick="calSetMode('month')">Month</button><button type="button" role="tab" class="cal-mode${mode === 'agenda' ? ' active' : ''}" aria-selected="${mode === 'agenda'}" onclick="calSetMode('agenda')">Agenda</button></div>
        </div>
        <div class="cal-chips" aria-label="Show">${chips}<span class="adm-count">${n} item${n === 1 ? '' : 's'} this month</span></div>
        ${body}`;
    if (mode === 'agenda' && _calMonth === today.slice(0, 7)) { const el = document.getElementById('cal-' + today); if (el && el.scrollIntoView && host.dataset.scrolled !== today) { host.dataset.scrolled = today; el.scrollIntoView({ block: 'start' }); } }
}
function calShift(n) { const d = _calDay(_calMonth + '-01'); d.setMonth(d.getMonth() + n); _calMonth = _calIso(d).slice(0, 7); _calSel = _calMonth + '-01'; renderCalendarView(); }
function calToday() { _calMonth = toISODate().slice(0, 7); _calSel = toISODate(); renderCalendarView(); }
function calSelect(d) { _calSel = d; if (d.slice(0, 7) !== _calMonth) _calMonth = d.slice(0, 7); renderCalendarView(); }
function calSetMode(m) { _calMode = m === 'agenda' ? 'agenda' : 'month'; renderCalendarView(); }
function calToggleCat(k) { if (_calOff.has(k)) _calOff.delete(k); else _calOff.add(k); try { localStorage.setItem('ot.calOff', JSON.stringify([..._calOff])); } catch (_) {} renderCalendarView(); }
// A tap opens the record behind the entry.
function calOpen(json) {
    let go = {}; try { go = JSON.parse(json) || {}; } catch (_) {}
    if (go.ticket && typeof showTicketDetail === 'function') { navigateTo('tickets'); showTicketDetail(go.ticket); return; }
    if (go.project && typeof showProjectDetail === 'function') { navigateTo('projects'); showProjectDetail(go.project); return; }
    if (go.unit && typeof showUnitProfile === 'function') { showUnitProfile(go.unit); return; }
    if (go.view) navigateTo(go.view);
}
