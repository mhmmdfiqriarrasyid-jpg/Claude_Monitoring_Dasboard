/* Projects & Jobs (2.29.0)
   ------------------------------------------------------------
   Two kinds of work that the Daily Report (KHL / PBT field work) does not
   hold:
   - a PROJECT: shared with other departments, with a target and a finish
     date — one unit or many, a number to reach, or weighted milestones —
     and a running log of what was done;
   - a JOB / ACTIVITY: one-off work (calibration, an implement fitted, a
     device swapped, a meeting, a training), optionally on units and
     optionally part of a project.
   Access area 'projects': 'edit' keeps everything, 'own' adds its own and
   changes only those, 'view' reads. Loaded after script.js and uses its
   helpers (escapeHtml, jsArg, cloudWrite, whPhoto*, …). */

const OT_JOB_CATEGORIES = ['Calibration', 'Implement install', 'Implement removal', 'Device install', 'Device swap',
    'Troubleshooting', 'Firmware update', 'Survey', 'Meeting', 'Coordination', 'Training', 'Admin', 'Other'];
const OT_DEPARTMENTS = ['Workshop', 'Agronomy', 'Plantation', 'Harvesting', 'Procurement', 'IT', 'HSE', 'Finance', 'Estate Management'];
const PJ_TYPES = [
    { key: 'agri',  label: 'Precision – Agriculture' },
    { key: 'heavy', label: 'Precision – Heavy Equipment' },
    { key: 'other', label: 'Other / Infrastructure' }
];
const PJ_STATUSES = [
    { key: 'plan',      label: 'Planned' },
    { key: 'active',    label: 'In progress' },
    { key: 'hold',      label: 'On hold' },
    { key: 'done',      label: 'Done' },
    { key: 'cancelled', label: 'Cancelled' }
];
const PJ_CLOSED = ['done', 'cancelled'];
const PJ_DEFAULT_MILESTONES = [['Survey & design', 10], ['PR / PO', 15], ['Goods received', 20], ['Installation', 35], ['Testing', 15], ['Handover', 5]];
const PJ_RAG = { green: 'On track', amber: 'Watch', red: 'Action needed', done: 'Done', cancelled: 'Cancelled' };
const PJ_UPDATES_MAX = 300;
const JOB_STATUSES = { open: 'Open', progress: 'In progress', done: 'Done' };

let projectsTab = 'projects';

// ---- Access ----
function projectsOwnOnly() { return !isOwner() && effectiveAccess('projects') === 'own'; }
function canAddProjects() { return hasAccess('projects', 'edit'); }   // 'own' ranks as edit
function canEditProjectRecord(rec) {
    if (!hasAccess('projects', 'edit')) return false;
    if (!rec || !projectsOwnOnly()) return true;
    return rec.createdByUid === myUid();
}
function _pjMe() {
    return (currentUserDoc && currentUserDoc.displayName) || ((currentUser && currentUser.email) ? shortActor(currentUser.email) : '');
}
function _pjId(prefix) { return `${prefix}_${Date.now().toString(36)}${Math.random().toString(36).slice(2, 7)}`; }
function jobCategoryChoices() { return masterListActive('jobCategory') ? masterLists.jobCategory.slice() : OT_JOB_CATEGORIES.slice(); }
function departmentChoices() { return masterListActive('department') ? masterLists.department.slice() : OT_DEPARTMENTS.slice(); }
function isImplementJob(cat) { return /implement/i.test(cat || '') && !/remov/i.test(cat || ''); }
function pjTypeLabel(k) { return (PJ_TYPES.find(t => t.key === k) || PJ_TYPES[2]).label; }
function pjStatusLabel(k) { return (PJ_STATUSES.find(t => t.key === k) || PJ_STATUSES[0]).label; }

// ---- Progress and health ----
function projectProgress(p) {
    const t = (p && p.target) || {};
    if (t.mode === 'count') {
        const total = Math.max(0, Number(t.count) || 0), done = Math.min(total, Math.max(0, Number(t.done) || 0));
        return { done, total, pct: total ? Math.round(100 * done / total) : 0, label: `${done}/${total} ${t.what || 'done'}` };
    }
    if (t.mode === 'milestones') {
        const ms = t.milestones || [];
        const all = ms.reduce((n, m) => n + Math.max(0, Number(m.weight) || 0), 0);
        const got = ms.filter(m => m.done).reduce((n, m) => n + Math.max(0, Number(m.weight) || 0), 0);
        return { done: ms.filter(m => m.done).length, total: ms.length, pct: all ? Math.round(100 * got / all) : 0, label: `${ms.filter(m => m.done).length}/${ms.length} milestones` };
    }
    const us = t.units || [];
    const done = us.filter(u => u.done).length;
    return { done, total: us.length, pct: us.length ? Math.round(100 * done / us.length) : 0, label: `${done}/${us.length} unit${us.length === 1 ? '' : 's'}${t.what ? ' · ' + t.what : ''}` };
}
function _pjDay(s) { const t = Date.parse(s + 'T00:00:00'); return isFinite(t) ? t : NaN; }
// green / amber / red — or done / cancelled. Automatic unless set by hand:
// late = red; progress more than 15 points behind the time used = amber.
function projectRag(p, today) {
    if (!p) return 'green';
    if (p.status === 'done') return 'done';
    if (p.status === 'cancelled') return 'cancelled';
    if (p.ragOverride) return p.ragOverride;
    const now = _pjDay(today || toISODate());
    const s = _pjDay(p.start), e = _pjDay(p.end);
    const pct = projectProgress(p).pct;
    if (isFinite(e) && now > e && pct < 100) return 'red';
    if (p.status === 'hold') return 'amber';
    if (isFinite(s) && isFinite(e) && e > s && now > s) {
        const used = Math.min(1, (now - s) / (e - s));
        if (pct + 15 < used * 100) return 'amber';
    }
    return 'green';
}
function pjRagChip(rag) { return `<span class="pj-rag pj-rag--${rag}"><span class="pj-rag__dot" aria-hidden="true"></span>${escapeHtml(PJ_RAG[rag] || rag)}</span>`; }
function pjDueText(p, today) {
    if (!p.end) return 'No finish date';
    const d = Math.round((_pjDay(p.end) - _pjDay(today || toISODate())) / 86400000);
    if (PJ_CLOSED.includes(p.status)) return `Finish ${p.end}`;
    return d < 0 ? `${-d} day${d === -1 ? '' : 's'} late (due ${p.end})` : d === 0 ? 'Due today' : `Due ${p.end} · ${d} day${d === 1 ? '' : 's'} left`;
}
function pjProgressBar(p, rag) {
    const pr = projectProgress(p);
    return `<div class="pj-bar" role="progressbar" aria-valuemin="0" aria-valuemax="100" aria-valuenow="${pr.pct}"><i class="pj-bar__fill pj-bar__fill--${rag}" style="width:${pr.pct}%"></i></div>
        <span class="pj-bar__label"><b>${pr.pct}%</b> · ${escapeHtml(pr.label)}</span>`;
}

// ---- Weeks (Saturday to Friday, as the weekly report) ----
function otWeekStart(iso) {
    const d = new Date((iso || toISODate()) + 'T00:00:00');
    d.setDate(d.getDate() - ((d.getDay() + 1) % 7));
    return d;
}
function _isoOf(d) { return `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, '0')}-${String(d.getDate()).padStart(2, '0')}`; }
function jobPeriodRange(period, today) {
    const t = today || toISODate();
    if (period === 'week' || period === 'lastweek') {
        const s = otWeekStart(t);
        if (period === 'lastweek') s.setDate(s.getDate() - 7);
        const e = new Date(s); e.setDate(e.getDate() + 6);
        return [_isoOf(s), _isoOf(e)];
    }
    if (period === 'month') return [t.slice(0, 7) + '-01', t.slice(0, 7) + '-31'];
    if (period === '30') { const s = new Date(t + 'T00:00:00'); s.setDate(s.getDate() - 29); return [_isoOf(s), t]; }
    return ['', ''];
}

// ---- Snapshots ----
function applyProjectsSnapshot(list) {
    projects = (list || []).slice().sort((a, b) => (b.updatedAt || 0) - (a.updatedAt || 0));
    if (currentView === 'projects') renderProjectsView();
    if (document.getElementById('pjDetailModal')?.classList.contains('open')) {
        const id = document.getElementById('pjDetailModal').dataset.id;
        if (projects.some(p => p.id === id)) showProjectDetail(id, true);
    }
}
function applyOtJobsSnapshot(list) {
    otJobs = (list || []).slice().sort((a, b) => String(b.date || '').localeCompare(String(a.date || '')) || (b.updatedAt || 0) - (a.updatedAt || 0));
    if (currentView === 'projects') renderProjectsView();
}
function _pjPush(rec, msg, existing, field) {
    const i = projects.findIndex(x => x.id === rec.id);
    if (i >= 0) projects[i] = rec; else projects.unshift(rec);
    cloudWrite(
        { action: existing ? 'update' : 'create', unitName: `[Project] ${rec.name}`, field: field || 'Project',
          before: existing ? `${pjStatusLabel(existing.status)} · ${projectProgress(existing).pct}%` : '',
          after: `${pjStatusLabel(rec.status)} · ${projectProgress(rec).pct}%` },
        cloudCall('saveProject', rec), msg,
        err => {
            console.error('[projects] save failed:', err);
            showToast(err && err.code === 'permission-denied' ? 'Projects need the new firestore.rules — publish them first' : 'Failed to save the project', 'error');
        });
    if (currentView === 'projects') renderProjectsView();
}
function _jobPush(rec, msg, existing) {
    const i = otJobs.findIndex(x => x.id === rec.id);
    if (i >= 0) otJobs[i] = rec; else otJobs.unshift(rec);
    cloudWrite(
        { action: existing ? 'update' : 'create', unitId: (rec.units || [])[0]?.id || '', unitName: `[Job] ${rec.category}`, field: rec.date,
          before: existing ? (existing.description || '') : '', after: rec.description || '' },
        cloudCall('saveOtJob', rec), msg,
        err => {
            console.error('[jobs] save failed:', err);
            showToast(err && err.code === 'permission-denied' ? 'Jobs need the new firestore.rules — publish them first' : 'Failed to save the job', 'error');
        });
    if (currentView === 'projects') renderProjectsView();
}

// ---- Page ----
function switchProjectsTab(tab) {
    projectsTab = tab === 'jobs' ? 'jobs' : 'projects';
    try { sessionStorage.setItem('ot.pjTab', projectsTab); } catch (_) {}
    renderProjectsView();
}
function _pjFillDatalists() {
    const fill = (id, vals) => { const dl = document.getElementById(id); if (dl) dl.innerHTML = vals.map(v => `<option value="${escapeHtml(v)}"></option>`).join(''); };
    fill('pjUnitList', [...globalData].sort((a, b) => (a.name || '').localeCompare(b.name || '')).map(damageUnitLabel));
    fill('pjDeptList', departmentChoices());
    fill('pjImplList', [...new Set(globalImplements.map(implementOptionLabel).filter(Boolean))].sort((a, b) => a.localeCompare(b)));
    fill('pjPrList', (purchaseRequests || []).map(r => r.prNo).filter(Boolean));
}
function renderProjectsView() {
    if (!document.getElementById('viewProjects')) return;
    try { if (!renderProjectsView._tab) { renderProjectsView._tab = true; projectsTab = sessionStorage.getItem('ot.pjTab') === 'jobs' ? 'jobs' : 'projects'; } } catch (_) {}
    document.querySelectorAll('#viewProjects .pj-tab').forEach(b => {
        const on = b.dataset.tab === projectsTab;
        b.classList.toggle('active', on);
        b.setAttribute('aria-selected', on ? 'true' : 'false');
    });
    document.getElementById('pjProjectsPanel').style.display = projectsTab === 'projects' ? '' : 'none';
    document.getElementById('pjJobsPanel').style.display = projectsTab === 'jobs' ? '' : 'none';
    const add = canAddProjects();
    ['btnNewProject', 'btnNewJob'].forEach(id => { const b = document.getElementById(id); if (b) b.style.display = add ? '' : 'none'; });
    const badge = document.getElementById('pjTabBadge');
    const red = projects.filter(p => projectRag(p) === 'red').length;
    if (badge) { badge.style.display = red ? '' : 'none'; badge.textContent = red; }
    _pjFillDatalists();
    renderProjectsList();
    renderJobsList();
}

function getFilteredProjects() {
    const q = (document.getElementById('pjSearch')?.value || '').trim().toLowerCase();
    const st = document.getElementById('pjStatusFilter')?.value ?? 'open';
    const ty = document.getElementById('pjTypeFilter')?.value || '';
    const co = document.getElementById('pjCompanyFilter')?.value || '';
    return projects.filter(p => {
        if (st === 'open' ? PJ_CLOSED.includes(p.status) : (st && p.status !== st)) return false;
        if (ty && p.type !== ty) return false;
        if (co && p.company !== co) return false;
        if (q) {
            const hay = [p.name, p.pic, p.company, p.description, ...(p.departments || []).flatMap(d => [d.name, d.pic])].join(' ').toLowerCase();
            if (!hay.includes(q)) return false;
        }
        return true;
    });
}
function renderProjectsList() {
    const tSel = document.getElementById('pjTypeFilter');
    if (tSel && tSel.options.length < 2) tSel.insertAdjacentHTML('beforeend', PJ_TYPES.map(t => `<option value="${t.key}">${escapeHtml(t.label)}</option>`).join(''));
    const sSel = document.getElementById('pjStatusFilter');
    if (sSel && sSel.options.length < 3) sSel.insertAdjacentHTML('beforeend', PJ_STATUSES.map(t => `<option value="${t.key}">${escapeHtml(t.label)}</option>`).join(''));
    const cSel = document.getElementById('pjCompanyFilter');
    if (cSel) {
        const cur = cSel.value;
        const cos = [...new Set(projects.map(p => p.company).filter(Boolean))].sort((a, b) => a.localeCompare(b));
        cSel.innerHTML = '<option value="">All companies</option>' + cos.map(c => `<option value="${escapeHtml(c)}">${escapeHtml(c)}</option>`).join('');
        cSel.value = cos.includes(cur) ? cur : '';
    }
    const open = projects.filter(p => !PJ_CLOSED.includes(p.status));
    const rags = open.map(p => projectRag(p));
    const year = toISODate().slice(0, 4);
    const tile = (n, label, sub, tone) => `<div class="wh-tile pr-tile--${tone}"><span class="wh-tile__label">${label}</span><span class="wh-tile__value">${n}</span><span class="wh-tile__sub">${sub}</span></div>`;
    const tiles = document.getElementById('pjTiles');
    if (tiles) tiles.innerHTML =
        tile(open.length, 'Open projects', 'Planned, in progress or on hold', 'teal') +
        tile(rags.filter(r => r === 'green').length, 'On track', 'Progress keeps up with time', 'good') +
        tile(rags.filter(r => r === 'amber').length, 'Watch', 'Behind or on hold', 'warn') +
        tile(rags.filter(r => r === 'red').length, 'Action needed', 'Past the finish date', 'bad') +
        tile(projects.filter(p => p.status === 'done' && String(p.doneAt || p.updatedAt ? new Date(p.doneAt || p.updatedAt).getFullYear() : '') === year).length, `Done in ${year}`, 'Finished projects', 'info');
    const rows = getFilteredProjects();
    const cnt = document.getElementById('pjCount');
    if (cnt) cnt.textContent = `${rows.length} project${rows.length === 1 ? '' : 's'}`;
    const list = document.getElementById('pjList');
    if (!list) return;
    if (!rows.length) {
        list.innerHTML = `<div class="adm-empty pj-empty">${projects.length ? 'No projects match the filter'
            : `No projects yet.${canAddProjects() ? ' Use <strong>New project</strong> to add one — one unit or many, with the departments involved.' : ''}`}</div>`;
        return;
    }
    list.innerHTML = rows.map(p => {
        const rag = projectRag(p);
        const last = (p.updates || [])[p.updates.length - 1];
        return `<button type="button" class="pj-card pj-card--${rag}" onclick="showProjectDetail(${jsArg(p.id)})">
            <span class="pj-card__top">${pjRagChip(rag)}<span class="pj-type">${escapeHtml(pjTypeLabel(p.type))}</span></span>
            <span class="pj-card__name">${escapeHtml(p.name || '')}</span>
            <span class="pj-card__meta">${escapeHtml([p.company, p.pic ? 'PIC ' + p.pic : ''].filter(Boolean).join(' · ') || '—')}</span>
            ${(p.departments || []).length ? `<span class="pj-depts">${p.departments.map(d => `<span class="pj-dept">${escapeHtml(d.name)}</span>`).join('')}</span>` : ''}
            <span class="pj-card__progress">${pjProgressBar(p, rag)}</span>
            <span class="pj-card__due">${escapeHtml(pjDueText(p))}</span>
            ${last ? `<span class="pj-card__last"><b>${escapeHtml(last.date || '')}</b> ${escapeHtml((last.text || '').slice(0, 110))}</span>` : ''}
        </button>`;
    }).join('');
}

// ---- Project form ----
let _pjDepts = [], _pjUnits = [], _pjMilestones = [], _pjPrIds = [];
function _pjFillSelects(rec) {
    document.getElementById('pjType').innerHTML = PJ_TYPES.map(t => `<option value="${t.key}">${escapeHtml(t.label)}</option>`).join('');
    document.getElementById('pjStatus').innerHTML = PJ_STATUSES.map(t => `<option value="${t.key}">${escapeHtml(t.label)}</option>`).join('');
    const cos = companySuggestions().slice();
    if (rec && rec.company && !cos.includes(rec.company)) cos.push(rec.company);
    document.getElementById('pjCompany').innerHTML = '<option value="">Select company…</option>' + cos.sort((a, b) => a.localeCompare(b)).map(c => `<option value="${escapeHtml(c)}">${escapeHtml(c)}</option>`).join('');
    document.getElementById('pjPrGroup').style.display = canViewView('purchase') ? '' : 'none';
    _pjFillDatalists();
}
function showAddProjectForm() {
    if (!canAddProjects()) { showToast('You cannot add projects', 'warning'); return; }
    document.getElementById('pjForm').reset();
    document.getElementById('pjId').value = '';
    document.getElementById('pjModalTitle').textContent = 'New project';
    _pjFillSelects(null);
    document.getElementById('pjStatus').value = 'active';
    document.getElementById('pjStart').value = toISODate();
    document.getElementById('pjPic').value = _pjMe();
    _pjDepts = [{ name: '', pic: '' }];
    _pjUnits = []; _pjMilestones = []; _pjPrIds = [];
    _setPjMode('units');
    renderProjectFormLists();
    document.getElementById('pjModal').classList.add('open');
    document.getElementById('pjName').focus();
}
function editProject(id) {
    const p = projects.find(x => x.id === id);
    if (!p) return;
    if (!canEditProjectRecord(p)) { showToast('Only the person who added this project can change it', 'warning'); return; }
    closeProjectDetail();
    document.getElementById('pjForm').reset();
    document.getElementById('pjId').value = p.id;
    document.getElementById('pjModalTitle').textContent = `Edit ${p.name}`;
    _pjFillSelects(p);
    document.getElementById('pjName').value = p.name || '';
    document.getElementById('pjType').value = p.type || 'other';
    document.getElementById('pjCompany').value = p.company || '';
    document.getElementById('pjStart').value = p.start || '';
    document.getElementById('pjEnd').value = p.end || '';
    document.getElementById('pjPic').value = p.pic || '';
    document.getElementById('pjStatus').value = p.status || 'active';
    document.getElementById('pjDesc').value = p.description || '';
    document.getElementById('pjRag').value = p.ragOverride || '';
    const t = p.target || {};
    _pjDepts = (p.departments || []).map(d => ({ ...d }));
    if (!_pjDepts.length) _pjDepts = [{ name: '', pic: '' }];
    _pjUnits = (t.units || []).map(u => ({ ...u }));
    _pjMilestones = (t.milestones || []).map(m => ({ ...m }));
    _pjPrIds = (p.prIds || []).slice();
    document.getElementById('pjUnitWhat').value = t.mode === 'units' ? (t.what || '') : '';
    document.getElementById('pjCountTarget').value = t.mode === 'count' ? (t.count || '') : '';
    document.getElementById('pjCountWhat').value = t.mode === 'count' ? (t.what || '') : '';
    document.getElementById('pjCountDone').value = t.mode === 'count' ? (t.done || 0) : '';
    _setPjMode(t.mode || 'units');
    renderProjectFormLists();
    document.getElementById('pjModal').classList.add('open');
}
function closeProjectModal() { document.getElementById('pjModal').classList.remove('open'); }
function _setPjMode(mode) {
    const r = document.querySelector(`input[name="pjMode"][value="${mode}"]`);
    if (r) r.checked = true;
    syncProjectMode();
}
function _pjMode() { return document.querySelector('input[name="pjMode"]:checked')?.value || 'units'; }
function syncProjectMode() {
    const m = _pjMode();
    document.getElementById('pjModeUnits').style.display = m === 'units' ? '' : 'none';
    document.getElementById('pjModeCount').style.display = m === 'count' ? '' : 'none';
    document.getElementById('pjModeMilestones').style.display = m === 'milestones' ? '' : 'none';
    document.getElementById('pjUnitsLabel').innerHTML = m === 'units' ? 'Units' : 'Units involved <span class="form-optional">(optional, one or more)</span>';
    document.getElementById('pjUnitsHint').style.display = m === 'units' ? 'none' : '';
    if (m === 'milestones' && !_pjMilestones.length) useDefaultMilestones();
}
function renderProjectFormLists() {
    const deps = departmentChoices();
    document.getElementById('pjDepts').innerHTML = _pjDepts.map((d, i) => `<div class="pj-row">
        <input class="form-input" list="pjDeptList" maxlength="80" placeholder="Department, e.g. ${escapeHtml(deps[0] || 'Workshop')}" aria-label="Department ${i + 1}"
            value="${escapeHtml(d.name || '')}" oninput="_pjDepts[${i}].name = this.value">
        <input class="form-input" maxlength="80" placeholder="PIC (name)" aria-label="PIC of department ${i + 1}" value="${escapeHtml(d.pic || '')}" oninput="_pjDepts[${i}].pic = this.value">
        <button type="button" class="btn-icon" aria-label="Remove department ${i + 1}" title="Remove" onclick="_pjDepts.splice(${i}, 1); renderProjectFormLists()"><i class="fas fa-xmark"></i></button>
    </div>`).join('') || '<p class="form-hint">No department yet.</p>';
    document.getElementById('pjUnitChips').innerHTML = _pjUnits.length ? _pjUnits.map((u, k) => `<span class="unit-chip${u.done ? ' unit-chip--done' : ''}">${u.done ? '✓ ' : ''}${escapeHtml(u.name || u.sn || '(unit)')}
        <button type="button" class="unit-chip__x" aria-label="Remove ${escapeHtml(u.name || '')}" title="Remove" onclick="_pjUnits.splice(${k}, 1); renderProjectFormLists()">&times;</button></span>`).join('')
        : '<span class="unit-chip__empty">No units yet</span>';
    document.getElementById('pjMilestones').innerHTML = _pjMilestones.map((m, i) => `<div class="pj-row">
        <input class="form-input" maxlength="120" placeholder="Milestone" aria-label="Milestone ${i + 1}" value="${escapeHtml(m.label || '')}" oninput="_pjMilestones[${i}].label = this.value">
        <label class="pj-weight"><input class="form-input" type="number" min="0" max="100" step="1" aria-label="Weight of milestone ${i + 1}" value="${Number(m.weight) || 0}" oninput="_pjMilestones[${i}].weight = Number(this.value) || 0"> %</label>
        <label class="notif-check"><input type="checkbox" ${m.done ? 'checked' : ''} onchange="_pjMilestones[${i}].done = this.checked; _pjMilestones[${i}].doneAt = this.checked ? toISODate() : ''"> done</label>
        <button type="button" class="btn-icon" aria-label="Remove milestone ${i + 1}" title="Remove" onclick="_pjMilestones.splice(${i}, 1); renderProjectFormLists()"><i class="fas fa-xmark"></i></button>
    </div>`).join('');
    document.getElementById('pjPrChips').innerHTML = _pjPrIds.map((id, k) => {
        const r = (purchaseRequests || []).find(x => x.id === id);
        return `<span class="unit-chip">${escapeHtml(r ? (r.prNo || r.title) : id)}<button type="button" class="unit-chip__x" aria-label="Remove" onclick="_pjPrIds.splice(${k}, 1); renderProjectFormLists()">&times;</button></span>`;
    }).join('');
}
function addProjectDept() { _pjDepts.push({ name: '', pic: '' }); renderProjectFormLists(); }
function addProjectMilestone() { _pjMilestones.push({ id: _pjId('m'), label: '', weight: 10, done: false }); renderProjectFormLists(); }
function useDefaultMilestones() {
    if (_pjMilestones.some(m => (m.label || '').trim()) && !confirm('Replace the milestones with the standard steps?')) return;
    _pjMilestones = PJ_DEFAULT_MILESTONES.map(([label, weight]) => ({ id: _pjId('m'), label, weight, done: false }));
    renderProjectFormLists();
}
function addProjectUnit() {
    const input = document.getElementById('pjUnitAdd');
    const raw = (input.value || '').trim();
    if (!raw) return;
    const u = resolveDamageUnit(raw);
    if (!u) { showToast(`Unit "${raw}" not found — pick one from the list`, 'warning'); return; }
    if (_pjUnits.some(x => x.id === u.id)) { showToast(`${u.name || u.sn} is already in the project`, 'info'); input.value = ''; return; }
    _pjUnits.push({ id: u.id, name: u.name || '', sn: u.sn || '', done: false, doneAt: '' });
    input.value = '';
    renderProjectFormLists();
    input.focus();
}
function addProjectPr() {
    const input = document.getElementById('pjPrAdd');
    const raw = (input.value || '').trim().toLowerCase();
    if (!raw) return;
    const r = (purchaseRequests || []).find(x => (x.prNo || '').toLowerCase() === raw) || (purchaseRequests || []).find(x => (x.prNo || '').toLowerCase().includes(raw));
    if (!r) { showToast('Purchase request not found', 'warning'); return; }
    if (!_pjPrIds.includes(r.id)) _pjPrIds.push(r.id);
    input.value = '';
    renderProjectFormLists();
}
function saveProject(event) {
    if (event) event.preventDefault();
    const id = document.getElementById('pjId').value;
    const existing = id ? projects.find(x => x.id === id) : null;
    if (!canEditProjectRecord(existing)) { showToast('You cannot change this project', 'warning'); return; }
    const name = (document.getElementById('pjName').value || '').trim();
    if (!name) { showToast('Fill in the project name', 'warning'); document.getElementById('pjName').focus(); return; }
    const start = document.getElementById('pjStart').value, end = document.getElementById('pjEnd').value;
    if (!end) { showToast('Set the target finish date', 'warning'); return; }
    if (start && end < start) { showToast('The finish date is before the start', 'warning'); return; }
    const mode = _pjMode();
    const target = { mode };
    if (mode === 'units') {
        if (!_pjUnits.length) { showToast('Add at least one unit — or measure the project by a number or milestones', 'warning'); return; }
        target.units = _pjUnits.map(u => ({ id: u.id, name: u.name || '', sn: u.sn || '', done: !!u.done, doneAt: u.doneAt || '' }));
        target.what = (document.getElementById('pjUnitWhat').value || '').trim();
    } else if (mode === 'count') {
        target.units = _pjUnits.map(u => ({ id: u.id, name: u.name || '', sn: u.sn || '', done: !!u.done, doneAt: u.doneAt || '' }));
        const count = Math.floor(Number(document.getElementById('pjCountTarget').value) || 0);
        if (count < 1) { showToast('Set the target number', 'warning'); return; }
        target.count = count;
        target.done = Math.max(0, Math.min(count, Math.floor(Number(document.getElementById('pjCountDone').value) || 0)));
        target.what = (document.getElementById('pjCountWhat').value || '').trim();
    } else {
        const ms = _pjMilestones.map(m => ({ id: m.id || _pjId('m'), label: (m.label || '').trim(), weight: Math.max(0, Number(m.weight) || 0), done: !!m.done, doneAt: m.doneAt || '' })).filter(m => m.label);
        if (!ms.length || !ms.some(m => m.weight > 0)) { showToast('Add milestones with a weight', 'warning'); return; }
        target.milestones = ms;
        target.units = _pjUnits.map(u => ({ id: u.id, name: u.name || '', sn: u.sn || '', done: !!u.done, doneAt: u.doneAt || '' }));
    }
    const status = document.getElementById('pjStatus').value || 'active';
    const rec = {
        ...(existing || {}),
        id: existing ? existing.id : _pjId('pj'),
        name, type: document.getElementById('pjType').value || 'other',
        company: document.getElementById('pjCompany').value
            || (mode === 'units' && target.units.length ? ((globalData.find(u => u.id === target.units[0].id) || {}).site || '') : ''),
        start, end, status,
        pic: (document.getElementById('pjPic').value || '').trim(),
        description: (document.getElementById('pjDesc').value || '').trim(),
        departments: _pjDepts.map(d => ({ name: _toolNorm(d.name), pic: (d.pic || '').trim() })).filter(d => d.name),
        target,
        ragOverride: document.getElementById('pjRag').value || '',
        prIds: _pjPrIds.slice(),
        updates: existing ? (existing.updates || []) : [],
        createdByUid: existing ? (existing.createdByUid || '') : myUid(),
        createdBy: existing ? (existing.createdBy || '') : _pjMe(),
        createdAt: existing ? (existing.createdAt || Date.now()) : Date.now(),
        updatedAt: Date.now()
    };
    if (status === 'done' && (!existing || existing.status !== 'done')) rec.doneAt = Date.now();
    _pjPush(rec, existing ? 'Project saved' : 'Project added', existing);
    closeProjectModal();
    showProjectDetail(rec.id);
}
function deleteProject(id) {
    const p = projects.find(x => x.id === id);
    if (!p || !canEditProjectRecord(p)) return;
    if (!confirm(`Delete the project "${p.name}" and its updates?\n\nJobs linked to it stay, without the link.`)) return;
    deleteWhPhotos((p.updates || []).flatMap(u => u.photoIds || []), 'pj');
    projects = projects.filter(x => x.id !== id);
    cloudWrite({ action: 'delete', unitName: `[Project] ${p.name}`, field: 'Project', before: pjStatusLabel(p.status), after: '' },
        cloudCall('deleteProject', id), 'Project deleted', err => showToast('Failed to delete the project', 'error'));
    closeProjectDetail();
    if (currentView === 'projects') renderProjectsView();
}

// ---- Project detail and updates ----
function closeProjectDetail() { document.getElementById('pjDetailModal').classList.remove('open'); }
function showProjectDetail(id, keepForm) {
    const p = projects.find(x => x.id === id);
    if (!p) return;
    const modal = document.getElementById('pjDetailModal');
    const typed = keepForm && modal.classList.contains('open') ? _pjUpdateDraft() : null;
    modal.dataset.id = id;
    const rag = projectRag(p), pr = projectProgress(p), t = p.target || {};
    const canEdit = canEditProjectRecord(p);
    const today = toISODate();
    const s = _pjDay(p.start), e = _pjDay(p.end), n = _pjDay(today);
    const pos = v => (isFinite(s) && isFinite(e) && e > s) ? Math.max(0, Math.min(100, 100 * (v - s) / (e - s))) : 0;
    document.getElementById('pjDetailTitle').textContent = p.name || 'Project';
    const row = (label, html) => `<div class="profile-row"><span class="profile-row__label">${label}</span><span>${html || '<span class="adm-muted">—</span>'}</span></div>`;
    let targetHtml = '';
    if (t.mode === 'units') {
        targetHtml = `<div class="pj-targets">${(t.units || []).map((u, i) => `<label class="pj-target${u.done ? ' is-done' : ''}">
            <input type="checkbox" ${u.done ? 'checked' : ''} ${canEdit ? '' : 'disabled'} onchange="toggleProjectTarget(${jsArg(p.id)}, 'unit', ${i}, this.checked)">
            <span><b>${escapeHtml(u.name || u.sn || '(unit)')}</b>${u.done && u.doneAt ? `<span class="adm-sub">done ${escapeHtml(u.doneAt)}</span>` : ''}</span></label>`).join('')}</div>`;
    } else if (t.mode === 'milestones') {
        targetHtml = `<div class="pj-targets">${(t.milestones || []).map((m, i) => `<label class="pj-target${m.done ? ' is-done' : ''}">
            <input type="checkbox" ${m.done ? 'checked' : ''} ${canEdit ? '' : 'disabled'} onchange="toggleProjectTarget(${jsArg(p.id)}, 'milestone', ${i}, this.checked)">
            <span><b>${escapeHtml(m.label)}</b> <span class="adm-sub">${Number(m.weight) || 0}%${m.done && m.doneAt ? ' · done ' + escapeHtml(m.doneAt) : ''}</span></span></label>`).join('')}</div>`;
    } else {
        targetHtml = `<p>${pr.done} of ${pr.total} ${escapeHtml(t.what || '')}</p>`;
    }
    // 2.29.1: units involved in a Number or Milestones project (optional).
    if (t.mode !== 'units' && (t.units || []).length) {
        const ud = t.units.filter(u => u.done).length;
        targetHtml += `<div class="pj-subtitle">Units involved — ${ud} of ${t.units.length} done</div>
        <div class="pj-targets">${t.units.map((u, i) => `<label class="pj-target${u.done ? ' is-done' : ''}">
            <input type="checkbox" ${u.done ? 'checked' : ''} ${canEdit ? '' : 'disabled'} onchange="toggleProjectTarget(${jsArg(p.id)}, 'unit', ${i}, this.checked)">
            <span><b>${escapeHtml(u.name || u.sn || '(unit)')}</b>${u.done && u.doneAt ? `<span class="adm-sub">done ${escapeHtml(u.doneAt)}</span>` : ''}</span></label>`).join('')}</div>`;
    }
    const jobs = otJobs.filter(j => j.projectId === p.id);
    const prs = (p.prIds || []).map(pid => (purchaseRequests || []).find(r => r.id === pid)).filter(Boolean);
    const ups = (p.updates || []).slice().reverse();
    let addForm = '';
    if (canEdit && !PJ_CLOSED.includes(p.status)) {
        const pending = t.mode === 'units' ? (t.units || []).map((u, i) => [i, u.name || u.sn, u.done]).filter(x => !x[2])
            : t.mode === 'milestones' ? (t.milestones || []).map((m, i) => [i, m.label, m.done]).filter(x => !x[2]) : [];
        addForm = `<div class="pr-section-title">Add an update</div>
        <div class="pj-upd">
            <div class="form-grid">
                <div class="form-group"><label class="form-label" for="pjUpdDate">Date</label><input class="form-input" id="pjUpdDate" type="date" value="${escapeHtml(typed ? typed.date : today)}"></div>
                ${t.mode === 'count' ? `<div class="form-group"><label class="form-label" for="pjUpdCount">Done so far (of ${pr.total})</label>
                    <input class="form-input" id="pjUpdCount" type="number" min="0" max="${pr.total}" step="1" value="${typed && typed.count != null ? typed.count : pr.done}"></div>` : ''}
                <div class="form-group full-width"><label class="form-label" for="pjUpdText">What was done</label>
                    <textarea class="form-input form-textarea" id="pjUpdText" rows="2" maxlength="2000" placeholder="e.g. Installed on 3 dump trucks, tested with the workshop">${escapeHtml(typed ? typed.text : '')}</textarea></div>
                ${pending.length ? `<div class="form-group full-width"><span class="form-label">${t.mode === 'units' ? 'Units finished in this update' : 'Milestones finished in this update'}</span>
                    <div class="pj-checks">${pending.map(([i, label]) => `<label class="notif-check"><input type="checkbox" data-pj-done="${i}" ${typed && typed.done.includes(i) ? 'checked' : ''}> ${escapeHtml(label || '')}</label>`).join('')}</div></div>` : ''}
                ${t.mode !== 'units' && (t.units || []).some(u => !u.done) ? `<div class="form-group full-width"><span class="form-label">Units finished in this update <span class="form-optional">(optional${t.mode === 'count' ? ' — each adds 1 to Done so far' : ''})</span></span>
                    <div class="pj-checks">${t.units.map((u, i) => u.done ? '' : `<label class="notif-check"><input type="checkbox" data-pj-unit="${i}" ${typed && typed.units.includes(i) ? 'checked' : ''}${t.mode === 'count' ? ' onchange="_pjCountFromUnits(this)"' : ''}> ${escapeHtml(u.name || u.sn || '')}</label>`).join('')}</div></div>` : ''}
                <div class="form-group full-width"><label class="form-label" for="pjUpdBlocker">Blocker / support needed <span class="form-optional">(optional)</span></label>
                    <input class="form-input" id="pjUpdBlocker" maxlength="300" value="${escapeHtml(typed ? typed.blocker : '')}" placeholder="e.g. Waiting for steering kit from vendor"></div>
                <div class="form-group full-width"><span class="form-label">Photos <span class="form-optional">(up to 10)</span> <span class="wh-photo-count" id="pjUpdPhotoCount"></span></span>
                    <div class="wh-photos" id="pjUpdPhotoGrid"></div>
                    <input type="file" id="pjUpdPhotoInput" accept="image/*" multiple hidden onchange="whPhotoAdd('pjUpdate', event)"></div>
            </div>
            <button type="button" class="btn btn-primary btn-sm" onclick="saveProjectUpdate(${jsArg(p.id)})"><i class="fas fa-plus" aria-hidden="true"></i> Save update</button>
        </div>`;
    }
    document.getElementById('pjDetailBody').innerHTML = `
        <div class="pj-head">${pjRagChip(rag)} <span class="pj-type">${escapeHtml(pjTypeLabel(p.type))}</span> <span class="pj-status">${escapeHtml(pjStatusLabel(p.status))}</span></div>
        <div class="pj-progress-big">${pjProgressBar(p, rag)}</div>
        <div class="pj-timeline" title="Start to finish; the red line is today">
            <span class="pj-timeline__plan"><i class="pj-timeline__fill pj-bar__fill--${rag}" style="width:${pr.pct}%"></i></span>
            ${isFinite(s) && isFinite(e) && n >= s && n <= e ? `<span class="pj-timeline__today" style="left:${pos(n)}%"></span>` : ''}
            <span class="pj-timeline__lbl"><span>${escapeHtml(p.start || '—')}</span><span>${escapeHtml(pjDueText(p))}</span></span>
        </div>
        <div class="profile-section">
            ${row('Company', escapeHtml(p.company || ''))}
            ${row('OT PIC', escapeHtml(p.pic || ''))}
            ${row('Departments', (p.departments || []).map(d => `${escapeHtml(d.name)}${d.pic ? ` <span class="adm-sub" style="display:inline">(${escapeHtml(d.pic)})</span>` : ''}`).join(', '))}
            ${p.description ? row('Description', escapeHtml(p.description)) : ''}
            ${row('Added by', escapeHtml(p.createdBy || ''))}
        </div>
        <div class="pr-section-title">Target — ${escapeHtml(t.mode === 'units' ? (t.what || 'work on each unit') : t.mode === 'milestones' ? 'milestones' : (t.what || 'number'))}</div>
        ${targetHtml}
        ${prs.length ? `<div class="pr-section-title">Purchase requests</div><div class="pj-links">${prs.map(r => `<button type="button" class="pj-link" onclick="closeProjectDetail(); navigateTo('purchase'); showPurchaseDetail(${jsArg(r.id)})">${escapeHtml(r.prNo || r.title)} ${prStatusChip(r.status)}</button>`).join('')}</div>` : ''}
        ${canEdit && _pjUncountedJobUnits(p) ? `<div class="pj-notice">${_pjUncountedJobUnits(p)} unit(s) from finished jobs are not counted yet.
            <button type="button" class="btn btn-primary btn-sm" onclick="countProjectJobUnits(${jsArg(p.id)})">Count units from jobs (+${_pjUncountedJobUnits(p)})</button></div>` : ''}
        ${jobs.length ? `<div class="pr-section-title">Jobs (${jobs.length})</div><div class="pj-links">${jobs.slice(0, 20).map(j => `<div class="pj-jobline"><b>${escapeHtml(j.date || '')}</b> ${escapeHtml(j.category)} — ${escapeHtml((j.description || '').slice(0, 90))}${(j.units || []).length ? ` <span class="adm-sub" style="display:inline">(${escapeHtml(j.units.map(u => u.name).join(', '))})</span>` : ''}</div>`).join('')}</div>` : ''}
        ${addForm}
        <div class="pr-section-title">Updates (${ups.length})</div>
        ${ups.length ? `<ol class="pj-updates">${ups.map(u => `<li>
            <div class="pj-updates__head"><b>${escapeHtml(u.date || '')}</b> · ${escapeHtml(u.by || '')}${u.auto ? ' <span class="pj-auto">auto</span>' : ''}${whPhotoChip(u.photoIds, 'update ' + (u.date || ''), 'pj')}</div>
            <div>${escapeHtml(u.text || '')}</div>
            ${u.progress ? `<div class="adm-sub">${escapeHtml(u.progress)}</div>` : ''}
            ${u.blocker ? `<div class="pj-blocker">Blocker: ${escapeHtml(u.blocker)}</div>` : ''}</li>`).join('')}</ol>` : '<p class="adm-muted">No updates yet.</p>'}`;
    // Redrawn while an update is being written (a snapshot came in): keep its photos.
    if (addForm) { if (typed) whPhotoRender('pjUpdate'); else whPhotoReset('pjUpdate', []); }
    const acts = [];
    const idArg = jsArg(p.id);
    if (canEdit) {
        acts.push(`<button type="button" class="btn btn-secondary" onclick="editProject(${idArg})"><i class="fas fa-pen" aria-hidden="true"></i> Edit</button>`);
        acts.push(`<button type="button" class="btn btn-secondary" onclick="deleteProject(${idArg})"><i class="fas fa-trash" aria-hidden="true"></i> Delete</button>`);
        if (!PJ_CLOSED.includes(p.status) && canAddProjects()) acts.push(`<button type="button" class="btn btn-secondary" onclick="closeProjectDetail(); showAddJobForm(${idArg})"><i class="fas fa-plus" aria-hidden="true"></i> Log job</button>`);
    }
    acts.push('<button type="button" class="btn btn-primary" onclick="closeProjectDetail()">Close</button>');
    document.getElementById('pjDetailActions').innerHTML = acts.join('');
    modal.classList.add('open');
}
function _pjUpdateDraft() {
    const v = id => document.getElementById(id)?.value || '';
    return { date: v('pjUpdDate'), text: v('pjUpdText'), blocker: v('pjUpdBlocker'),
             count: document.getElementById('pjUpdCount') ? v('pjUpdCount') : null,
             done: [...document.querySelectorAll('#pjDetailBody input[data-pj-done]:checked')].map(el => Number(el.dataset.pjDone)),
             units: [...document.querySelectorAll('#pjDetailBody input[data-pj-unit]:checked')].map(el => Number(el.dataset.pjUnit)) };
}
// Number target: ticking a unit adds 1 to "Done so far" (unticking takes it back).
function _pjCountFromUnits(box) {
    const el = document.getElementById('pjUpdCount');
    if (!el) return;
    const max = Number(el.max) || Infinity;
    el.value = Math.max(0, Math.min(max, (Number(el.value) || 0) + (box.checked ? 1 : -1)));
}
function _pjWithUpdate(p, upd) {
    const updates = (p.updates || []).concat([upd]).slice(-PJ_UPDATES_MAX);
    return { ...p, updates, updatedAt: Date.now() };
}
function saveProjectUpdate(id) {
    const p = projects.find(x => x.id === id);
    if (!p || !canEditProjectRecord(p)) return;
    const d = _pjUpdateDraft();
    const text = d.text.trim();
    const t = JSON.parse(JSON.stringify(p.target || {}));
    const date = d.date || toISODate();
    const notes = [];
    if (t.mode === 'units') d.done.forEach(i => { if (t.units[i] && !t.units[i].done) { t.units[i].done = true; t.units[i].doneAt = date; notes.push(t.units[i].name || t.units[i].sn); } });
    if (t.mode === 'milestones') d.done.forEach(i => { if (t.milestones[i] && !t.milestones[i].done) { t.milestones[i].done = true; t.milestones[i].doneAt = date; notes.push(t.milestones[i].label); } });
    if (t.mode !== 'units') (d.units || []).forEach(i => { const u = (t.units || [])[i]; if (u && !u.done) { u.done = true; u.doneAt = date; notes.push(u.name || u.sn); } });
    if (t.mode === 'count' && d.count !== null && d.count !== '') {
        const c = Math.max(0, Math.min(Number(t.count) || 0, Math.floor(Number(d.count) || 0)));
        if (c !== (Number(t.done) || 0)) notes.push(`${c}/${t.count} ${t.what || 'done'}`);
        t.done = c;
    }
    if (!text && !notes.length) { showToast('Write what was done', 'warning'); document.getElementById('pjUpdText')?.focus(); return; }
    const before = projectProgress(p).pct;
    let next = { ...p, target: t };
    const after = projectProgress(next).pct;
    const upd = { id: _pjId('u'), date, at: Date.now(), by: _pjMe(), byUid: myUid(), text, blocker: d.blocker.trim(),
                  progress: notes.length ? `Finished: ${notes.join(', ')} · progress ${before}% → ${after}%` : '',
                  photoIds: whPhotoCommit('pjUpdate', p.id) };
    next = _pjWithUpdate(next, upd);
    if (after >= 100 && next.status !== 'done' && confirm('Everything in the target is done. Mark the project as Done?')) { next.status = 'done'; next.doneAt = Date.now(); }
    _pjPush(next, 'Update saved', p, 'Update');
    showProjectDetail(id);
}
// Ticking a unit or milestone in the detail: one change, logged as an update.
function toggleProjectTarget(id, kind, i, on) {
    const p = projects.find(x => x.id === id);
    if (!p || !canEditProjectRecord(p)) return;
    const t = JSON.parse(JSON.stringify(p.target || {}));
    const item = kind === 'unit' ? (t.units || [])[i] : (t.milestones || [])[i];
    if (!item) return;
    const was = !!item.done;
    item.done = !!on; item.doneAt = on ? toISODate() : '';
    // A unit of a Number project counts in "Done so far" too.
    if (kind === 'unit' && t.mode === 'count' && was !== !!on) t.done = Math.max(0, Math.min(Number(t.count) || 0, (Number(t.done) || 0) + (on ? 1 : -1)));
    const before = projectProgress(p).pct;
    let next = { ...p, target: t };
    const after = projectProgress(next).pct;
    next = _pjWithUpdate(next, { id: _pjId('u'), date: toISODate(), at: Date.now(), by: _pjMe(), byUid: myUid(), auto: true,
        text: `${on ? 'Marked done' : 'Marked not done'}: ${item.name || item.label || item.sn || ''}`, progress: `progress ${before}% → ${after}%`, photoIds: [] });
    _pjPush(next, on ? 'Marked done' : 'Marked not done', p, 'Progress');
    showProjectDetail(id, true);
}

// ---- Jobs & Activities ----
let _jobUnits = [];
function getFilteredJobs() {
    const q = (document.getElementById('jobSearch')?.value || '').trim().toLowerCase();
    const per = document.getElementById('jobPeriod')?.value ?? 'week';
    const cat = document.getElementById('jobCategoryFilter')?.value || '';
    const st = document.getElementById('jobStatusFilter')?.value || '';
    const [from, to] = jobPeriodRange(per);
    return otJobs.filter(j => {
        if (from && (j.date || '') < from) return false;
        if (to && (j.date || '') > to) return false;
        if (cat && j.category !== cat) return false;
        if (st && j.status !== st) return false;
        if (q) {
            const hay = [j.category, j.description, j.requestedBy, j.doneBy, j.company, j.implement, ...(j.units || []).map(u => u.name + ' ' + (u.sn || ''))].join(' ').toLowerCase();
            if (!hay.includes(q)) return false;
        }
        return true;
    });
}
function renderJobsList() {
    const cSel = document.getElementById('jobCategoryFilter');
    if (cSel) {
        const cur = cSel.value;
        const cats = [...new Set(jobCategoryChoices().concat(otJobs.map(j => j.category).filter(Boolean)))];
        cSel.innerHTML = '<option value="">All categories</option>' + cats.map(c => `<option value="${escapeHtml(c)}">${escapeHtml(c)}</option>`).join('');
        cSel.value = cats.includes(cur) ? cur : '';
    }
    const rows = getFilteredJobs();
    const per = document.getElementById('jobPeriod');
    const perLabel = per ? per.options[per.selectedIndex].text : '';
    const byCat = new Map();
    rows.forEach(j => byCat.set(j.category, (byCat.get(j.category) || 0) + 1));
    const top = [...byCat.entries()].sort((a, b) => b[1] - a[1]).slice(0, 3);
    const tile = (n, label, sub, tone) => `<div class="wh-tile pr-tile--${tone}"><span class="wh-tile__label">${escapeHtml(label)}</span><span class="wh-tile__value">${n}</span><span class="wh-tile__sub">${escapeHtml(sub)}</span></div>`;
    const tiles = document.getElementById('jobTiles');
    const hours = rows.reduce((n, j) => n + (Number(j.hours) || 0), 0);
    if (tiles) tiles.innerHTML = tile(rows.length, 'Jobs', perLabel, 'teal')
        + top.map(([c, n], i) => tile(n, c, perLabel, ['info', 'good', 'warn'][i])).join('')
        + tile(otJobs.filter(j => j.status !== 'done').length, 'Not finished', 'Open or in progress, any date', 'bad')
        + (hours ? tile(Math.round(hours * 10) / 10, 'Hours', perLabel, 'neutral') : '');
    const cnt = document.getElementById('jobCount');
    if (cnt) cnt.textContent = `${rows.length} job${rows.length === 1 ? '' : 's'}`;
    const tbody = document.getElementById('jobBody');
    if (!tbody) return;
    if (!rows.length) {
        tbody.innerHTML = `<tr><td colspan="7" class="adm-empty">${otJobs.length ? 'No jobs in this period or filter'
            : `No jobs yet.${canAddProjects() ? ' Use <strong>Log job</strong> for calibrations, implement installs, meetings and other work outside the Daily Report.' : ''}`}</td></tr>`;
        return;
    }
    tbody.innerHTML = rows.map(j => {
        const p = j.projectId ? projects.find(x => x.id === j.projectId) : null;
        const can = canEditProjectRecord(j);
        return `<tr>
            <td data-label="Date" class="adm-nowrap">${escapeHtml(j.date || '')}</td>
            <td data-label="Job"><span class="adm-stack"><span class="pj-cat">${escapeHtml(j.category || '')}</span>
                <span>${escapeHtml(j.description || '')}</span>
                ${j.implement ? `<span class="adm-sub">Implement: ${escapeHtml(j.implement)}${j.appliedImplement ? ' · set on the unit' : ''}</span>` : ''}
                ${p ? `<button type="button" class="adm-link" onclick="showProjectDetail(${jsArg(p.id)})">Project: ${escapeHtml(p.name)}</button>` : ''}</span></td>
            <td data-label="Units">${(j.units || []).length ? j.units.map(u => `<button type="button" class="badge badge-cat pj-unitlink" onclick="showUnitProfile(${jsArg(u.id)})">${escapeHtml(u.name || u.sn || '')}</button>`).join(' ') : '<span class="adm-muted">—</span>'}${j.company ? `<span class="adm-sub">${escapeHtml(j.company)}</span>` : ''}</td>
            <td data-label="Requested / done by"><span class="adm-stack"><span>${escapeHtml(j.requestedBy || '—')}</span><span class="adm-sub">by ${escapeHtml(j.doneBy || '—')}</span></span></td>
            <td data-label="Hours" class="num">${j.hours ? escapeHtml(String(j.hours)) : '<span class="adm-muted">—</span>'}</td>
            <td data-label="Status"><span class="pj-jstatus pj-jstatus--${escapeHtml(j.status || 'done')}">${escapeHtml(JOB_STATUSES[j.status] || 'Done')}</span>${whPhotoChip(j.photoIds, j.category || 'job', 'pj')}</td>
            <td class="col-actions">${can ? `<div class="row-actions">
                <button class="btn btn-secondary" title="Edit" aria-label="Edit job" onclick="editJob(${jsArg(j.id)})"><i class="fas fa-pen" aria-hidden="true"></i></button>
                <button class="btn btn-secondary" title="Delete" aria-label="Delete job" onclick="deleteJob(${jsArg(j.id)})"><i class="fas fa-trash" aria-hidden="true"></i></button></div>` : ''}</td>
        </tr>`;
    }).join('');
}
function _jobFillSelects(rec, projectId) {
    const cats = jobCategoryChoices();
    if (rec && rec.category && !cats.includes(rec.category)) cats.push(rec.category);
    document.getElementById('otJobCategory').innerHTML = cats.map(c => `<option value="${escapeHtml(c)}">${escapeHtml(c)}</option>`).join('');
    const open = projects.filter(p => !PJ_CLOSED.includes(p.status) || p.id === (rec && rec.projectId) || p.id === projectId);
    document.getElementById('otJobProject').innerHTML = '<option value="">— Not part of a project —</option>'
        + open.map(p => `<option value="${escapeHtml(p.id)}">${escapeHtml(p.name)}</option>`).join('');
    _pjFillDatalists();
}
function showAddJobForm(projectId) {
    if (!canAddProjects()) { showToast('You cannot log jobs', 'warning'); return; }
    document.getElementById('otJobForm').reset();
    document.getElementById('otJobId').value = '';
    document.getElementById('otJobModalTitle').textContent = 'Log job';
    _jobFillSelects(null, projectId);
    document.getElementById('otJobDate').value = toISODate();
    document.getElementById('otJobDoneBy').value = _pjMe();
    document.getElementById('otJobStatus').value = 'done';
    document.getElementById('otJobProject').value = projectId || '';
    _jobUnits = [];
    const p = projectId && projects.find(x => x.id === projectId);
    if (p && p.target && p.target.what) document.getElementById('otJobDesc').value = p.target.what;
    syncJobCategory();
    renderJobUnits();
    whPhotoReset('otJob', []);
    document.getElementById('otJobModal').classList.add('open');
}
function editJob(id) {
    const j = otJobs.find(x => x.id === id);
    if (!j) return;
    if (!canEditProjectRecord(j)) { showToast('Only the person who logged this job can change it', 'warning'); return; }
    document.getElementById('otJobForm').reset();
    document.getElementById('otJobId').value = j.id;
    document.getElementById('otJobModalTitle').textContent = 'Edit job';
    _jobFillSelects(j);
    document.getElementById('otJobDate').value = j.date || '';
    document.getElementById('otJobCategory').value = j.category || '';
    document.getElementById('otJobDesc').value = j.description || '';
    document.getElementById('otJobImpl').value = j.implement || '';
    document.getElementById('otJobRequestedBy').value = j.requestedBy || '';
    document.getElementById('otJobDoneBy').value = j.doneBy || '';
    document.getElementById('otJobHours').value = j.hours || '';
    document.getElementById('otJobStatus').value = j.status || 'done';
    document.getElementById('otJobProject').value = j.projectId || '';
    _jobUnits = (j.units || []).map(u => ({ ...u }));
    syncJobCategory();
    renderJobUnits();
    whPhotoReset('otJob', j.photoIds || []);
    document.getElementById('otJobModal').classList.add('open');
}
function closeJobModal() { document.getElementById('otJobModal').classList.remove('open'); }
function syncJobCategory() {
    const cat = document.getElementById('otJobCategory').value;
    document.getElementById('otJobImplGroup').style.display = isImplementJob(cat) ? '' : 'none';
}
function renderJobUnits() {
    document.getElementById('otJobUnitChips').innerHTML = _jobUnits.length ? _jobUnits.map((u, k) => `<span class="unit-chip">${escapeHtml(u.name || u.sn || '(unit)')}
        <button type="button" class="unit-chip__x" aria-label="Remove ${escapeHtml(u.name || '')}" onclick="_jobUnits.splice(${k}, 1); renderJobUnits()">&times;</button></span>`).join('')
        : '<span class="unit-chip__empty">No units (a meeting or training needs none)</span>';
}
function addJobUnit() {
    const input = document.getElementById('otJobUnitAdd');
    const raw = (input.value || '').trim();
    if (!raw) return;
    const u = resolveDamageUnit(raw);
    if (!u) { showToast(`Unit "${raw}" not found — pick one from the list`, 'warning'); return; }
    if (!_jobUnits.some(x => x.id === u.id)) _jobUnits.push({ id: u.id, name: u.name || '', sn: u.sn || '' });
    input.value = '';
    renderJobUnits();
    input.focus();
}
// The units of finished jobs counted in their project. A Units target counts
// only its own units; a Number or Milestones target takes the job's units in
// (2.29.2), and on a Number target each new unit adds 1 to Done so far.
// Returns null when nothing changes.
function _pjCountJobUnits(p, jobs) {
    const t = JSON.parse(JSON.stringify(p.target || {}));
    if (!Array.isArray(t.units)) t.units = [];
    const hit = [];
    jobs.forEach(j => (j.units || []).forEach(u => {
        let tu = t.units.find(x => x.id === u.id);
        if (!tu) {
            if (t.mode === 'units') return;
            tu = { id: u.id, name: u.name || '', sn: u.sn || '', done: false, doneAt: '' };
            t.units.push(tu);
        }
        if (tu.done) return;
        tu.done = true; tu.doneAt = j.date || toISODate();
        hit.push(tu.name || tu.sn);
    }));
    if (!hit.length) return null;
    if (t.mode === 'count') t.done = Math.min(Number(t.count) || 0, (Number(t.done) || 0) + hit.length);
    const before = projectProgress(p).pct;
    const next = { ...p, target: t };
    const after = projectProgress(next).pct;
    return { next, hit, after, progress: `Finished: ${hit.join(', ')} · progress ${before}% → ${after}%` };
}
// Jobs finished before their units were counted (logged on 2.29.0 or 2.29.1).
function _pjUncountedJobUnits(p) {
    const t = p.target || {};
    if (t.mode === 'units') return 0;
    const seen = new Set((t.units || []).filter(u => u.done).map(u => u.id));
    const add = new Set();
    otJobs.filter(j => j.projectId === p.id && j.status === 'done').forEach(j => (j.units || []).forEach(u => { if (!seen.has(u.id)) add.add(u.id); }));
    return add.size;
}
function countProjectJobUnits(id) {
    const p = projects.find(x => x.id === id);
    if (!p || !canEditProjectRecord(p)) return;
    const r = _pjCountJobUnits(p, otJobs.filter(j => j.projectId === p.id && j.status === 'done'));
    if (!r) { showToast('Every job unit is already counted', 'info'); return; }
    _pjPush(_pjWithUpdate(r.next, { id: _pjId('u'), date: toISODate(), at: Date.now(), by: _pjMe(), byUid: myUid(), auto: true,
        text: `Counted the units of finished jobs`, progress: r.progress, photoIds: [] }), `Project progress ${r.after}%`, p, 'Progress');
    showProjectDetail(id);
}
function saveJob(event) {
    if (event) event.preventDefault();
    const id = document.getElementById('otJobId').value;
    const existing = id ? otJobs.find(x => x.id === id) : null;
    if (!canEditProjectRecord(existing)) { showToast('You cannot change this job', 'warning'); return; }
    // A unit left typed but not added is folded in.
    if ((document.getElementById('otJobUnitAdd').value || '').trim()) { addJobUnit(); if ((document.getElementById('otJobUnitAdd').value || '').trim()) return; }
    const date = document.getElementById('otJobDate').value || toISODate();
    if (date > toISODate()) { showToast('The date cannot be in the future', 'warning'); return; }
    const category = document.getElementById('otJobCategory').value;
    const description = (document.getElementById('otJobDesc').value || '').trim();
    if (!category) { showToast('Pick a category', 'warning'); return; }
    if (!description) { showToast('Write what was done', 'warning'); document.getElementById('otJobDesc').focus(); return; }
    const implement = isImplementJob(category) ? _toolNorm(document.getElementById('otJobImpl').value) : '';
    const status = document.getElementById('otJobStatus').value || 'done';
    if (isImplementJob(category) && status === 'done' && !implement) { showToast('Fill in the implement that was installed', 'warning'); return; }
    if (isImplementJob(category) && status === 'done' && !_jobUnits.length) { showToast('Add the unit the implement was fitted to', 'warning'); return; }
    const first = _jobUnits.length ? globalData.find(u => u.id === _jobUnits[0].id) : null;
    const recId = existing ? existing.id : _pjId('job');
    const rec = {
        ...(existing || {}),
        id: recId, date, category, description, status,
        units: _jobUnits.map(u => ({ id: u.id, name: u.name || '', sn: u.sn || '' })),
        implement,
        company: (first && first.site) || (existing && existing.company) || '',
        requestedBy: _toolNorm(document.getElementById('otJobRequestedBy').value),
        doneBy: (document.getElementById('otJobDoneBy').value || '').trim(),
        hours: Math.max(0, Math.round((Number(document.getElementById('otJobHours').value) || 0) * 10) / 10) || 0,
        projectId: document.getElementById('otJobProject').value || '',
        photoIds: whPhotoCommit('otJob', recId),
        createdByUid: existing ? (existing.createdByUid || '') : myUid(),
        createdBy: existing ? (existing.createdBy || '') : _pjMe(),
        createdAt: existing ? (existing.createdAt || Date.now()) : Date.now(),
        updatedAt: Date.now()
    };
    // An implement fitted: the units take it in the Unit Database (once).
    if (implement && status === 'done' && rec.units.length) {
        const already = rec.appliedImplement && rec.appliedImplement.implement === implement
            && rec.units.every(u => (rec.appliedImplement.units || []).some(a => a.id === u.id));
        if (!already) {
            if (hasAccess('editUnits', 'edit')) {
                const changes = new Map(), applied = [];
                rec.units.forEach(u => {
                    const live = globalData.find(x => x.id === u.id);
                    if (!live) return;
                    applied.push({ id: u.id, before: live.implement || '' });
                    if ((live.implement || '') !== implement) changes.set(u.id, { implement });
                });
                if (changes.size) updateUnitsBatch(changes);
                rec.appliedImplement = { implement, at: Date.now(), units: applied };
                if (changes.size) showToast(`${changes.size} unit(s) now carry ${implement}`, 'info');
            } else {
                showToast('Job saved — the unit’s implement was not changed (needs Unit Database edit access)', 'warning');
            }
        }
    }
    _jobPush(rec, existing ? 'Job saved' : 'Job logged', existing);
    // Part of a project measured by units: those units count as done.
    const p = rec.projectId && projects.find(x => x.id === rec.projectId);
    if (p && status === 'done' && canEditProjectRecord(p) && p.target) {
        const r = _pjCountJobUnits(p, [rec]);
        if (r) _pjPush(_pjWithUpdate(r.next, { id: _pjId('u'), date, at: Date.now(), by: rec.doneBy || _pjMe(), byUid: myUid(), auto: true,
            text: `${category}: ${description}`, progress: r.progress, photoIds: [] }), `Project progress ${r.after}%`, p, 'Progress');
    }
    closeJobModal();
}
function deleteJob(id) {
    const j = otJobs.find(x => x.id === id);
    if (!j || !canEditProjectRecord(j)) return;
    if (!confirm(`Delete this job (${j.category}, ${j.date})?${j.appliedImplement ? '\n\nThe implement set on the unit stays as it is.' : ''}`)) return;
    deleteWhPhotos(j.photoIds || [], 'pj');
    otJobs = otJobs.filter(x => x.id !== id);
    cloudWrite({ action: 'delete', unitName: `[Job] ${j.category}`, field: j.date, before: j.description || '', after: '' },
        cloudCall('deleteOtJob', id), 'Job deleted', () => showToast('Failed to delete the job', 'error'));
    if (currentView === 'projects') renderProjectsView();
}

// ---- Unit profile: jobs and projects of this unit ----
function unitJobsProfileHtml(u) {
    if (!canViewView('projects')) return '';
    const jobs = otJobs.filter(j => (j.units || []).some(x => x.id === u.id));
    const pjs = projects.filter(p => p.target && (p.target.units || []).some(x => x.id === u.id));
    const items = jobs.slice(0, 8).map(j => `<div class="profile-item">
            <span class="profile-item__date">${escapeHtml(j.date || '')}</span>
            <span class="badge badge-cat" style="font-size:10px">${escapeHtml(j.category || '')}</span>
            <span class="profile-item__text" title="${escapeHtml(j.description || '')}">${escapeHtml((j.description || '').slice(0, 50))}</span>
        </div>`).join('');
    const pjItems = pjs.map(p => { const tu = p.target.units.find(x => x.id === u.id);
        return `<div class="profile-item"><span class="badge badge-cat" style="font-size:10px">Project</span>
            <span class="profile-item__text">${escapeHtml(p.name)} — ${tu && tu.done ? 'done' : 'to do'}</span></div>`; }).join('');
    return `<div class="profile-section">
        <div class="profile-section__title">Jobs &amp; Projects (${jobs.length + pjs.length})</div>
        <div class="profile-list">${pjItems + items || '<div class="profile-empty">No jobs or projects for this unit yet.</div>'}</div>
    </div>`;
}
