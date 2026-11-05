/* Tickets (2.30.0)
   ------------------------------------------------------------
   The front door for problems in the field. Anyone signed in reports one —
   what, how urgent, which unit, where, photos — and follows it; the OT team
   (access 'tickets' edit) assigns a PIC, works it and resolves it, and can
   turn it into a Job (Projects & Jobs) or a Damage record. The reporter
   confirms the fix (closed) or reopens it.
   Status: open → assigned → progress → resolved → closed.
   Due time (SLA) from the priority: high 4 h, medium 1 day, low 3 days.
   Loaded after script.js and projects.js and uses their helpers. */

const TK_STATUSES = {
    open:     { label: 'Open',        tone: 'bad' },
    assigned: { label: 'Assigned',    tone: 'info' },
    progress: { label: 'In progress', tone: 'warn' },
    resolved: { label: 'Resolved',    tone: 'good' },
    closed:   { label: 'Closed',      tone: 'neutral' }
};
const TK_PRIORITY = { high: { label: 'High', hours: 4 }, medium: { label: 'Medium', hours: 24 }, low: { label: 'Low', hours: 72 } };
const TK_CATEGORIES = ['GPS / RTK signal', 'Display', 'Steering / auto-guidance', 'JDLink / telematics', 'Camera AI', 'Implement',
    'Radio / HT', 'Network / internet', 'Dashboard / account', 'Other'];
const TK_EVENTS_MAX = 300;
const TK_DONE = ['resolved', 'closed'];

// ---- Access ----
function canHandleTickets() { return hasAccess('tickets', 'edit'); }
function canSeeAllTickets() { return hasAccess('tickets', 'view'); }
function ticketMine(t) { return !!t && t.reportedByUid === myUid(); }
function ticketCategoryChoices() { return masterListActive('ticketCategory') ? masterLists.ticketCategory.slice() : TK_CATEGORIES.slice(); }
function _tkMe() { return (currentUserDoc && currentUserDoc.displayName) || ((currentUser && currentUser.email) ? shortActor(currentUser.email) : ''); }

// ---- Time ----
function ticketDueAt(t) { return (t.reportedAt || 0) + ((TK_PRIORITY[t.priority] || TK_PRIORITY.medium).hours * 3600000); }
function ticketOverdue(t, now) { return !TK_DONE.includes(t.status) && (now || Date.now()) > ticketDueAt(t); }
function _tkSpan(ms) {
    const m = Math.round(Math.abs(ms) / 60000);
    if (m < 60) return `${m} min`;
    const h = Math.round(m / 60);
    return h < 48 ? `${h} h` : `${Math.round(h / 24)} days`;
}
function ticketDueText(t, now) {
    now = now || Date.now();
    if (t.status === 'closed') return `Closed ${formatUserTime(t.closedAt || t.updatedAt)}`;
    if (t.status === 'resolved') return `Resolved in ${_tkSpan((t.resolvedAt || now) - (t.reportedAt || now))}`;
    const left = ticketDueAt(t) - now;
    return left < 0 ? `${_tkSpan(left)} over due` : `Due in ${_tkSpan(left)}`;
}
function tkStatusChip(s) { const d = TK_STATUSES[s] || TK_STATUSES.open; return `<span class="pr-status pr-status--${d.tone}"><span class="pr-dot" aria-hidden="true"></span>${escapeHtml(d.label)}</span>`; }
function tkPrioChip(p) { return `<span class="tk-pchip tk-pchip--${escapeHtml(p || 'medium')}">${escapeHtml((TK_PRIORITY[p] || TK_PRIORITY.medium).label)}</span>`; }

// ---- Snapshots ----
function applyTicketsSnapshot(list) {
    tickets = (list || []).slice().sort((a, b) => (b.reportedAt || 0) - (a.reportedAt || 0));
    updateTicketBadge();
    if (currentView === 'tickets') renderTicketsView();
    const modal = document.getElementById('tkDetailModal');
    if (modal && modal.classList.contains('open')) {
        const t = tickets.find(x => x.id === modal.dataset.id);
        if (!t) closeTicketDetail();
        else if ((t.updatedAt || 0) !== Number(modal.dataset.seen || 0)) showTicketDetail(t.id, true);
    }
    if (typeof scheduleDecisionRefresh === 'function') scheduleDecisionRefresh();
}
function applyTicketStaffSnapshot(list) { ticketStaff = (list || []).slice().sort((a, b) => (a.name || '').localeCompare(b.name || '')); }
// An OT account puts its own card where the others can pick it as PIC.
function registerTicketStaff() {
    if (!currentUser || !window.cloud || !window.cloud.saveTicketStaff) return;
    const card = { uid: myUid(), name: _tkMe() || currentUser.email || '', email: currentUser.email || '', updatedAt: Date.now() };
    window.cloud.saveTicketStaff(card).catch(err => console.warn('[tickets] staff card not saved:', err && err.code));
}
function ticketAssignees() {
    const map = new Map();
    (ticketStaff || []).forEach(s => map.set(s.uid, s.name || s.email || s.uid));
    if (typeof allUsers !== 'undefined' && isOwner()) (allUsers || []).filter(u => u.status === 'active' && (u.role === 'owner' || (u.access && u.access.tickets === 'edit')))
        .forEach(u => { if (!map.has(u.uid)) map.set(u.uid, u.displayName || u.email || u.uid); });
    if (canHandleTickets() && !map.has(myUid())) map.set(myUid(), _tkMe() || 'Me');
    return [...map.entries()].map(([uid, name]) => ({ uid, name })).sort((a, b) => a.name.localeCompare(b.name));
}
function updateTicketBadge() {
    const b = document.getElementById('ticketBadge');
    if (!b) return;
    const n = canHandleTickets() ? tickets.filter(t => t.status === 'open' || ticketOverdue(t)).length
        : tickets.filter(t => ticketMine(t) && t.status === 'resolved').length;
    b.style.display = n ? '' : 'none';
    b.textContent = n;
}

// Saving: the OT team's changes go in History; a reporter's stay on the ticket's own timeline.
function _tkPush(rec, msg, existing) {
    const i = tickets.findIndex(x => x.id === rec.id);
    if (i >= 0) tickets[i] = rec; else tickets.unshift(rec);
    const onErr = err => {
        console.error('[tickets] save failed:', err);
        showToast(err && err.code === 'permission-denied' ? 'Tickets need the new firestore.rules — publish them first' : 'Failed to save the ticket', 'error');
    };
    if (canHandleTickets()) {
        cloudWrite({ action: existing ? 'update' : 'create', unitId: (rec.units || [])[0]?.id || '', unitName: `[Ticket] ${rec.no}`, field: rec.title,
            before: existing ? TK_STATUSES[existing.status].label : '', after: TK_STATUSES[rec.status].label }, cloudCall('saveTicket', rec), msg, onErr);
    } else {
        cloudCall('saveTicket', rec).then(() => msg && showToast(msg, 'success')).catch(onErr);
    }
    updateTicketBadge();
    if (currentView === 'tickets') renderTicketsView();
}
function _tkEvent(t, type, extra) {
    const ev = Object.assign({ at: Date.now(), by: _tkMe(), byUid: myUid(), type }, extra || {});
    return { ...t, events: (t.events || []).concat([ev]).slice(-TK_EVENTS_MAX), updatedAt: Date.now(), lastActorUid: myUid() };
}

// ---- Page ----
function getFilteredTickets() {
    const q = (document.getElementById('tkSearch')?.value || '').trim().toLowerCase();
    const st = document.getElementById('tkStatusFilter')?.value ?? 'active';
    const pr = document.getElementById('tkPriorityFilter')?.value || '';
    const mine = document.getElementById('tkMineFilter')?.value || '';
    const now = Date.now();
    return tickets.filter(t => {
        if (st === 'active' ? t.status === 'closed' : st === 'overdue' ? !ticketOverdue(t, now) : (st && t.status !== st)) return false;
        if (pr && t.priority !== pr) return false;
        if (mine === 'mine' && t.assigneeUid !== myUid()) return false;
        if (mine === 'reported' && !ticketMine(t)) return false;
        if (q) {
            const hay = [t.no, t.title, t.category, t.description, t.location, t.reportedBy, t.assigneeName, ...(t.units || []).map(u => `${u.name} ${u.sn || ''}`)].join(' ').toLowerCase();
            if (!hay.includes(q)) return false;
        }
        return true;
    }).sort((a, b) => {
        // Overdue and high first among the active ones, newest after that.
        const rank = t => (TK_DONE.includes(t.status) ? 3 : ticketOverdue(t, now) ? 0 : t.priority === 'high' ? 1 : 2);
        return rank(a) - rank(b) || (b.reportedAt || 0) - (a.reportedAt || 0);
    });
}
function renderTicketsView() {
    if (!document.getElementById('viewTickets')) return;
    const all = canSeeAllTickets(), handle = canHandleTickets(), now = Date.now();
    const sub = document.getElementById('tkSub');
    if (sub) sub.textContent = all ? 'Problems in the field, reported by anyone and handled by the OT team' : 'Report a problem in the field and follow what happens with it';
    const mineSel = document.getElementById('tkMineFilter');
    if (mineSel) mineSel.closest('label').style.display = all ? '' : 'none';
    const tile = (n, label, sub2, tone) => `<div class="wh-tile pr-tile--${tone}"><span class="wh-tile__label">${label}</span><span class="wh-tile__value">${n}</span><span class="wh-tile__sub">${sub2}</span></div>`;
    const active = tickets.filter(t => t.status !== 'closed');
    const week = Date.now() - 7 * 86400000;
    const solved = tickets.filter(t => t.resolvedAt && t.resolvedAt >= now - 30 * 86400000);
    const avg = solved.length ? solved.reduce((n, t) => n + (t.resolvedAt - (t.reportedAt || t.resolvedAt)), 0) / solved.length : 0;
    const tiles = document.getElementById('tkTiles');
    if (tiles) tiles.innerHTML = all
        ? tile(tickets.filter(t => t.status === 'open').length, 'New', 'Not assigned yet', 'bad')
          + tile(tickets.filter(t => t.status === 'assigned' || t.status === 'progress').length, 'Being handled', 'Assigned or in progress', 'info')
          + tile(active.filter(t => ticketOverdue(t, now)).length, 'Overdue', 'Past the due time for its priority', 'warn')
          + tile(tickets.filter(t => t.status === 'resolved').length, 'Resolved', 'Waiting for the reporter to confirm', 'good')
          + tile(solved.length ? _tkSpan(avg) : '—', 'Average time to fix', 'Resolved in the last 30 days', 'teal')
        : tile(tickets.filter(t => ticketMine(t) && !TK_DONE.includes(t.status)).length, 'My open tickets', 'Still being handled', 'info')
          + tile(tickets.filter(t => ticketMine(t) && t.status === 'resolved').length, 'Please confirm', 'Resolved — fixed for you?', 'good')
          + tile(tickets.filter(t => ticketMine(t) && t.status === 'closed' && (t.closedAt || 0) >= week).length, 'Closed this week', 'Confirmed fixed', 'teal');
    renderTicketsList();
    updateTicketBadge();
    void handle;
}
function renderTicketsList() {
    const rows = getFilteredTickets();
    const cnt = document.getElementById('tkCount');
    if (cnt) cnt.textContent = `${rows.length} ticket${rows.length === 1 ? '' : 's'}`;
    const box = document.getElementById('tkList');
    if (!box) return;
    if (!rows.length) {
        box.innerHTML = `<div class="adm-empty">${tickets.length ? 'No tickets match the filter' : 'No tickets yet. Use <strong>Report a problem</strong> when something in the field needs the OT team.'}</div>`;
        return;
    }
    const now = Date.now();
    box.innerHTML = rows.map(t => {
        const late = ticketOverdue(t, now);
        return `<button type="button" class="tk-row${late ? ' is-late' : ''}" onclick="showTicketDetail(${jsArg(t.id)})">
            <span class="tk-row__main">
                <span class="tk-row__top">${tkPrioChip(t.priority)}<span class="tk-no">${escapeHtml(t.no || '')}</span><span class="tk-cat">${escapeHtml(t.category || '')}</span></span>
                <span class="tk-row__title">${escapeHtml(t.title || '')}</span>
                <span class="tk-row__meta">${escapeHtml([(t.units || []).map(u => u.name).join(', '), t.location].filter(Boolean).join(' · ') || '—')}</span>
            </span>
            <span class="tk-row__who"><span>${escapeHtml(t.reportedBy || '')}</span><span class="adm-sub">${t.assigneeName ? 'PIC ' + escapeHtml(t.assigneeName) : 'No PIC yet'}</span></span>
            <span class="tk-row__state">${tkStatusChip(t.status)}<span class="tk-due${late ? ' is-late' : ''}">${escapeHtml(ticketDueText(t, now))}</span></span>
        </button>`;
    }).join('');
}

// ---- New / edit ----
let _tkUnits = [];
function _tkFillForm(t) {
    const cats = ticketCategoryChoices();
    if (t && t.category && !cats.includes(t.category)) cats.push(t.category);
    document.getElementById('tkCategory').innerHTML = cats.map(c => `<option value="${escapeHtml(c)}">${escapeHtml(c)}</option>`).join('');
    const dl = document.getElementById('tkUnitList');
    if (dl) dl.innerHTML = [...globalData].sort((a, b) => (a.name || '').localeCompare(b.name || '')).map(u => `<option value="${escapeHtml(damageUnitLabel(u))}"></option>`).join('');
    if (typeof populatePaddockList === 'function') { try { populatePaddockList(''); } catch (_) {} }
}
function showAddTicketForm(prefill) {
    if (!currentUser) return;
    document.getElementById('tkForm').reset();
    document.getElementById('tkId').value = '';
    document.getElementById('tkModalTitle').textContent = 'Report a problem';
    document.getElementById('tkSubmitLabel').textContent = 'Send ticket';
    _tkFillForm(null);
    _tkUnits = [];
    if (prefill) {
        if (prefill.unit) _tkUnits.push({ id: prefill.unit.id, name: prefill.unit.name || '', sn: prefill.unit.sn || '' });
        if (prefill.title) document.getElementById('tkTitle').value = prefill.title;
    }
    renderTicketUnits();
    whPhotoReset('ticket', []);
    document.getElementById('tkModal').classList.add('open');
    document.getElementById('tkTitle').focus();
}
function editTicket(id) {
    const t = tickets.find(x => x.id === id);
    if (!t || !canHandleTickets()) return;
    closeTicketDetail();
    document.getElementById('tkForm').reset();
    document.getElementById('tkId').value = t.id;
    document.getElementById('tkModalTitle').textContent = `Edit ${t.no}`;
    document.getElementById('tkSubmitLabel').textContent = 'Save';
    _tkFillForm(t);
    document.getElementById('tkTitle').value = t.title || '';
    document.getElementById('tkCategory').value = t.category || '';
    const r = document.querySelector(`input[name="tkPriority"][value="${t.priority || 'medium'}"]`); if (r) r.checked = true;
    document.getElementById('tkLocation').value = t.location || '';
    document.getElementById('tkDesc').value = t.description || '';
    _tkUnits = (t.units || []).map(u => ({ ...u }));
    renderTicketUnits();
    whPhotoReset('ticket', t.photoIds || []);
    document.getElementById('tkModal').classList.add('open');
}
function closeTicketModal() { document.getElementById('tkModal').classList.remove('open'); }
function renderTicketUnits() {
    document.getElementById('tkUnitChips').innerHTML = _tkUnits.length ? _tkUnits.map((u, k) => `<span class="unit-chip">${escapeHtml(u.name || u.sn || '(unit)')}
        <button type="button" class="unit-chip__x" aria-label="Remove ${escapeHtml(u.name || '')}" onclick="_tkUnits.splice(${k}, 1); renderTicketUnits()">&times;</button></span>`).join('')
        : '<span class="unit-chip__empty">No unit (a radio, the network or an account needs none)</span>';
}
function addTicketUnit() {
    const input = document.getElementById('tkUnitAdd');
    const raw = (input.value || '').trim();
    if (!raw) return;
    const u = resolveDamageUnit(raw);
    if (!u) { showToast(`Unit "${raw}" not found — pick one from the list`, 'warning'); return; }
    if (!_tkUnits.some(x => x.id === u.id)) _tkUnits.push({ id: u.id, name: u.name || '', sn: u.sn || '' });
    input.value = '';
    renderTicketUnits();
    input.focus();
}
function _tkNumber() {
    const d = new Date(), p = n => String(n).padStart(2, '0');
    return `TK-${String(d.getFullYear()).slice(2)}${p(d.getMonth() + 1)}${p(d.getDate())}-${p(d.getHours())}${p(d.getMinutes())}${Math.random().toString(36).slice(2, 4).toUpperCase()}`;
}
function saveTicket(event) {
    if (event) event.preventDefault();
    const id = document.getElementById('tkId').value;
    const existing = id ? tickets.find(x => x.id === id) : null;
    if (existing && !canHandleTickets()) return;
    if ((document.getElementById('tkUnitAdd').value || '').trim()) { addTicketUnit(); if ((document.getElementById('tkUnitAdd').value || '').trim()) return; }
    const title = (document.getElementById('tkTitle').value || '').trim();
    if (!title) { showToast('Say in one line what the problem is', 'warning'); document.getElementById('tkTitle').focus(); return; }
    const category = document.getElementById('tkCategory').value || 'Other';
    const priority = document.querySelector('input[name="tkPriority"]:checked')?.value || 'medium';
    const first = _tkUnits.length ? globalData.find(u => u.id === _tkUnits[0].id) : null;
    const recId = existing ? existing.id : 'tk_' + Date.now().toString(36) + Math.random().toString(36).slice(2, 7);
    const now = Date.now();
    let rec = {
        ...(existing || {}),
        id: recId, no: existing ? existing.no : _tkNumber(),
        title, category, priority,
        description: (document.getElementById('tkDesc').value || '').trim(),
        location: (document.getElementById('tkLocation').value || '').trim(),
        units: _tkUnits.map(u => ({ id: u.id, name: u.name || '', sn: u.sn || '' })),
        company: (first && first.site) || (existing && existing.company) || '',
        photoIds: whPhotoCommit('ticket', recId),
        status: existing ? existing.status : 'open',
        reportedByUid: existing ? existing.reportedByUid : myUid(),
        reportedBy: existing ? existing.reportedBy : _tkMe(),
        reportedByEmail: existing ? (existing.reportedByEmail || '') : ((currentUser && currentUser.email) || ''),
        reportedAt: existing ? existing.reportedAt : now,
        assigneeUid: existing ? (existing.assigneeUid || '') : '', assigneeName: existing ? (existing.assigneeName || '') : '',
        jobIds: existing ? (existing.jobIds || []) : [],
        events: existing ? (existing.events || []) : [],
        updatedAt: now, lastActorUid: myUid()
    };
    if (!existing) rec = _tkEvent(rec, 'created', { text: `${TK_PRIORITY[priority].label} priority` });
    else if (existing.priority !== priority) rec = _tkEvent(rec, 'priority', { from: existing.priority, to: priority });
    _tkPush(rec, existing ? 'Ticket saved' : `Ticket ${rec.no} sent — the OT team is notified`, existing);
    closeTicketModal();
    if (existing) showTicketDetail(rec.id);
}

// ---- Detail: timeline and actions ----
function closeTicketDetail() { document.getElementById('tkDetailModal').classList.remove('open'); }
const _TK_EVENT_TEXT = {
    created: 'reported the problem', assign: 'assigned it to', progress: 'started working on it', resolved: 'marked it resolved',
    closed: 'confirmed it is fixed — closed', reopen: 'reopened it', comment: 'commented', priority: 'changed the priority', job: 'logged a job'
};
function showTicketDetail(id, keepTyping) {
    const t = tickets.find(x => x.id === id);
    if (!t) return;
    const modal = document.getElementById('tkDetailModal');
    const typed = keepTyping && modal.classList.contains('open') ? (document.getElementById('tkComment')?.value || '') : '';
    modal.dataset.id = id;
    modal.dataset.seen = String(t.updatedAt || 0);
    const handle = canHandleTickets(), mine = ticketMine(t), now = Date.now(), late = ticketOverdue(t, now);
    document.getElementById('tkDetailTitle').textContent = `${t.no} — ${t.title}`;
    const row = (label, html) => `<div class="profile-row"><span class="profile-row__label">${label}</span><span>${html || '<span class="adm-muted">—</span>'}</span></div>`;
    const jobs = (t.jobIds || []).map(j => otJobs.find(x => x.id === j)).filter(Boolean);
    const evs = (t.events || []).slice().reverse();
    const staff = handle ? ticketAssignees() : [];
    document.getElementById('tkDetailBody').innerHTML = `
        <div class="tk-head">${tkPrioChip(t.priority)} ${tkStatusChip(t.status)} <span class="tk-due${late ? ' is-late' : ''}">${escapeHtml(ticketDueText(t, now))}</span></div>
        <div class="profile-section">
            ${row('Category', escapeHtml(t.category || ''))}
            ${row('Units', (t.units || []).map(u => `<button type="button" class="badge badge-cat pj-unitlink" onclick="showUnitProfile(${jsArg(u.id)})">${escapeHtml(u.name || u.sn || '')}</button>`).join(' '))}
            ${row('Where', escapeHtml(t.location || ''))}
            ${row('Reported by', `${escapeHtml(t.reportedBy || '')} · ${escapeHtml(formatUserTime(t.reportedAt))}`)}
            ${row('PIC', escapeHtml(t.assigneeName || ''))}
            ${row('Due', escapeHtml(formatUserTime(ticketDueAt(t))))}
        </div>
        ${t.description ? `<div class="pr-section-title">Details</div><div class="wl-detail__desc">${escapeHtml(t.description)}</div>` : ''}
        ${(t.photoIds || []).length ? `<div class="pr-section-title">Photos</div>${whPhotoChip(t.photoIds, t.no, 'tk')}` : ''}
        ${t.resolution ? `<div class="pr-section-title">Resolution</div><div class="tk-resolution">${escapeHtml(t.resolution)}</div>` : ''}
        ${jobs.length ? `<div class="pr-section-title">Jobs</div><div class="pj-links">${jobs.map(j => `<div class="pj-jobline"><b>${escapeHtml(j.date || '')}</b> ${escapeHtml(j.category)} — ${escapeHtml((j.description || '').slice(0, 90))} <span class="adm-sub" style="display:inline">(${escapeHtml(JOB_STATUSES[j.status] || '')})</span></div>`).join('')}</div>` : ''}
        ${handle && !TK_DONE.includes(t.status) ? `<div class="pr-section-title">Assign</div>
            <div class="tk-assign"><select class="form-select" id="tkAssignee" aria-label="PIC"><option value="">— Choose PIC —</option>${staff.map(s => `<option value="${escapeHtml(s.uid)}"${s.uid === t.assigneeUid ? ' selected' : ''}>${escapeHtml(s.name)}</option>`).join('')}</select>
            <button type="button" class="btn btn-secondary btn-sm" onclick="assignTicket(${jsArg(t.id)})">Assign</button></div>` : ''}
        <div class="pr-section-title">Timeline</div>
        ${(mine || handle) && t.status !== 'closed' ? `<div class="tk-comment"><textarea class="form-input form-textarea" id="tkComment" rows="2" maxlength="2000" placeholder="Add a note or an update…">${escapeHtml(typed)}</textarea>
            <button type="button" class="btn btn-secondary btn-sm" onclick="commentTicket(${jsArg(t.id)})"><i class="fas fa-comment" aria-hidden="true"></i> Add note</button></div>` : ''}
        <ol class="pj-updates tk-timeline">${evs.map(ev => `<li class="tk-ev tk-ev--${escapeHtml(ev.type)}">
            <div class="pj-updates__head"><b>${escapeHtml(ev.by || '')}</b> ${escapeHtml(_TK_EVENT_TEXT[ev.type] || ev.type)}${ev.type === 'assign' && ev.to ? ' <b>' + escapeHtml(ev.toName || '') + '</b>' : ''}${ev.type === 'priority' ? ` (${escapeHtml((TK_PRIORITY[ev.from] || {}).label || '')} → ${escapeHtml((TK_PRIORITY[ev.to] || {}).label || '')})` : ''} · ${escapeHtml(formatUserTime(ev.at))}</div>
            ${ev.text ? `<div>${escapeHtml(ev.text)}</div>` : ''}</li>`).join('')}</ol>`;
    const acts = [], idArg = jsArg(t.id), b = (label, fn, primary) => `<button type="button" class="btn ${primary ? 'btn-primary' : 'btn-secondary'}" onclick="${fn}">${label}</button>`;
    if (handle) {
        if (t.status === 'open' || t.status === 'assigned') acts.push(b('<i class="fas fa-play" aria-hidden="true"></i> Start work', `setTicketStatus(${idArg}, 'progress')`));
        if (!TK_DONE.includes(t.status)) acts.push(b('<i class="fas fa-check" aria-hidden="true"></i> Resolve', `setTicketStatus(${idArg}, 'resolved')`, true));
        if (!TK_DONE.includes(t.status) && typeof canAddProjects === 'function' && canAddProjects()) acts.push(b('<i class="fas fa-wrench" aria-hidden="true"></i> Log job', `jobFromTicket(${idArg})`));
        if (!TK_DONE.includes(t.status) && hasAccess('damage', 'edit') && (t.units || []).length) acts.push(b('Report damage', `damageFromTicket(${idArg})`));
        acts.push(b('<i class="fas fa-pen" aria-hidden="true"></i> Edit', `editTicket(${idArg})`));
    }
    if (t.status === 'resolved' && (mine || handle)) {
        acts.push(b('Not fixed — reopen', `setTicketStatus(${idArg}, 'open')`));
        acts.push(b('<i class="fas fa-check-double" aria-hidden="true"></i> Fixed — close', `setTicketStatus(${idArg}, 'closed')`, true));
    }
    if (t.status === 'closed' && handle) acts.push(b('Reopen', `setTicketStatus(${idArg}, 'open')`));
    acts.push(b('Close window', 'closeTicketDetail()'));
    document.getElementById('tkDetailActions').innerHTML = acts.join('');
    modal.classList.add('open');
}
function assignTicket(id) {
    const t = tickets.find(x => x.id === id);
    if (!t || !canHandleTickets()) return;
    const uid = document.getElementById('tkAssignee')?.value || '';
    if (!uid) { showToast('Choose the PIC first', 'warning'); return; }
    const who = ticketAssignees().find(s => s.uid === uid);
    let next = { ...t, assigneeUid: uid, assigneeName: who ? who.name : '', assignedAt: Date.now(), status: t.status === 'open' ? 'assigned' : t.status };
    next = _tkEvent(next, 'assign', { to: uid, toName: next.assigneeName });
    _tkPush(next, `Assigned to ${next.assigneeName}`, t);
    showTicketDetail(id);
}
function commentTicket(id) {
    const t = tickets.find(x => x.id === id);
    if (!t || !(canHandleTickets() || ticketMine(t))) return;
    const el = document.getElementById('tkComment');
    const text = (el?.value || '').trim();
    if (!text) { showToast('Write the note first', 'warning'); return; }
    _tkPush(_tkEvent(t, 'comment', { text }), 'Note added', t);
    showTicketDetail(id);
}
function setTicketStatus(id, status) {
    const t = tickets.find(x => x.id === id);
    if (!t) return;
    const handle = canHandleTickets(), mine = ticketMine(t);
    // The reporter only answers a resolved ticket: fixed (closed) or not (open).
    if (!handle && !(mine && t.status === 'resolved' && (status === 'closed' || status === 'open'))) { showToast('Only the OT team can change this ticket', 'warning'); return; }
    let text = '';
    if (status === 'resolved') {
        const r = prompt('What was done to fix it?', t.resolution || '');
        if (r == null) return;
        text = r.trim();
        if (!text) { showToast('Write what fixed it', 'warning'); return; }
    }
    if (status === 'open' && TK_DONE.includes(t.status)) {
        const r = prompt('Why is it reopened?', '');
        if (r == null) return;
        text = r.trim();
    }
    const now = Date.now();
    let next = { ...t, status };
    if (status === 'progress') next.startedAt = t.startedAt || now;
    if (status === 'resolved') { next.resolvedAt = now; next.resolution = text; }
    if (status === 'closed') next.closedAt = now;
    if (status === 'open' && TK_DONE.includes(t.status)) { next.resolvedAt = null; next.closedAt = null; if (handle) next.reportedAt = t.reportedAt; }
    // A reporter's write may touch only events, status, closedAt and updatedAt (firestore.rules).
    if (!handle) next = { ...t, status, closedAt: status === 'closed' ? now : (t.closedAt || null) };
    const type = status === 'open' ? 'reopen' : status;
    next = _tkEvent(next, type, text ? { text } : {});
    _tkPush(next, `${TK_STATUSES[status].label}`, t);
    showTicketDetail(id);
}
// From a ticket to a Job in Projects & Jobs, linked both ways.
let _jobTicketLink = '';
function jobFromTicket(id) {
    const t = tickets.find(x => x.id === id);
    if (!t || typeof showAddJobForm !== 'function') return;
    closeTicketDetail();
    showAddJobForm();
    _jobTicketLink = t.id;
    document.getElementById('otJobDesc').value = `${t.title}${t.location ? ' — ' + t.location : ''}`;
    _jobUnits = (t.units || []).map(u => ({ ...u }));
    renderJobUnits();
    document.getElementById('otJobIssue').value = '';
}
// Called by saveJob (projects.js) after a job is saved.
function linkJobToTicket(job) {
    if (!_jobTicketLink) return;
    const t = tickets.find(x => x.id === _jobTicketLink);
    _jobTicketLink = '';
    if (!t || !canHandleTickets()) return;
    let next = { ...t, jobIds: [...new Set((t.jobIds || []).concat([job.id]))] };
    if (t.status === 'open' || t.status === 'assigned') { next.status = 'progress'; next.startedAt = t.startedAt || Date.now(); }
    next = _tkEvent(next, 'job', { text: `${job.category}: ${job.description}` });
    _tkPush(next, '', t);
}
function damageFromTicket(id) {
    const t = tickets.find(x => x.id === id);
    if (!t || !t.units || !t.units.length) return;
    closeTicketDetail();
    navigateTo('damage');
    showAddDamageForm();
    const u = globalData.find(x => x.id === t.units[0].id);
    const unitEl = document.getElementById('dmgUnit');
    if (unitEl && u) { unitEl.value = damageUnitLabel(u); unitEl.dispatchEvent(new Event('change')); }
    const d = document.getElementById('dmgDescription');
    if (d) d.value = `${t.title}${t.description ? '\n' + t.description : ''}\n(${t.no})`;
}

// ---- Decision inbox: new and overdue tickets for the OT team ----
function ticketDecisionGroup() {
    if (!canHandleTickets()) return null;
    const now = Date.now();
    const list = tickets.filter(t => t.status === 'open' || ticketOverdue(t, now));
    if (!list.length) return null;
    return {
        key: 'tickets', icon: 'ticket', tone: list.some(t => ticketOverdue(t, now) || t.priority === 'high') ? 'danger' : 'warning',
        title: 'Tickets to handle',
        total: list.length,
        items: list.map(t => ({ open: `showTicketDetail(${jsArg(t.id)})`, text: `${t.no} · ${t.title}`,
            sub: `${TK_PRIORITY[t.priority].label} · ${t.status === 'open' ? 'no PIC yet' : TK_STATUSES[t.status].label.toLowerCase()} · ${ticketDueText(t, now)}` })),
        goto: 'tickets',
        since: Math.min(...list.map(t => t.reportedAt || now))
    };
}
