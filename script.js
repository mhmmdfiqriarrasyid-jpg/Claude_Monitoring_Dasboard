/* ============================================================
   Tractor Monitoring Dashboard - Core Application
   ============================================================ */

// ---- State ----
let globalData = [];
let filteredData = [];
let charts = {};
let sortState = { key: null, asc: true };
let currentView = 'dashboard';
let selectedUnitIds = new Set();
let lastDeletedUnits = null;
let undoTimer = null;
let globalImplements = [];
let selectedImplementIds = new Set();

// ---- Breakdown reason modal state ----
let _pendingBreakdown = null;  // { unitId, fields, isInline, el }

// ---- Edit table sort state ----
let editSortState = { key: null, asc: true };

// ---- Cloud sync state ----
let cloudInitialized = false;
let cloudUnitsUnsub = null;
let cloudImplUnsub = null;
let cloudUsersUnsub = null;
let cloudHistoryUnsub = null;
let cloudHistory = [];               // newest-first, mirrors Firestore `history`
let _historyFlushTimer = null;
const _historyPushQueue = [];
let cloudUserCategoriesUnsub = null;
let userCategories = [];             // [{ id, name, createdAt }] from Firestore
let _firstUserCategoriesSnapshot = true;
let suppressCloudWrites = false; // true while applying a cloud snapshot — prevents loops
let _cloudReadyFired = false;
let _localDataLoaded = false;
let _firstUnitsSnapshot = true;
let _firstImplSnapshot = true;

// ---- Auth state ----
let currentUser = null;        // Firebase Auth user object
let currentUserDoc = null;     // Firestore profile doc { email, role, status, ... }
let allUsers = [];             // Mirror of users collection (owner only)
let authInitialized = false;

// ---- Constants ----
const STORAGE_KEY = 'tractorUnits';
const IMPLEMENTS_STORAGE_KEY = 'tractorImplements';
const PENDING_CHANGES_KEY = 'tractorPendingChanges';
const AUDIT_LOG_KEY = 'tractorAuditLog';
const AUDIT_LOG_MAX = 500;
const BACKUP_RING_KEY = 'tractorUnits_autobackup';
const BACKUP_RING_SIZE = 3;
const DARK_MODE_KEY = 'tractorDarkMode';
const LICENSE_DEFAULTS_KEY = 'tractorLicenseDefaultsApplied';
const LICENSE_DATES_KEY = 'tractorLicenseDatesApplied_v2';
const USER_CATEGORIES_SEED_KEY = 'tractorUserCategoriesSeeded_v1';
const DEFAULT_USER_CATEGORIES = [
    'Land Development',
    'Maintenance and Fertilization',
    'Planting'
];

// One-shot import: license start dates supplied by the owner (serial number → start date).
// Expiration is auto-computed as +1 year. Only applied to units that currently
// have no licenseStartDate — manual edits are preserved.
const LICENSE_DATES_MAP = {
    'IT8C570HKST250056': '2025-08-04',
    '1YR6I50BASU540056': '2025-10-30',
    '1YR6I50BCSU540035': '2026-02-14',
    '1YR6I50BCSU540083': '2025-09-12',
    '1YR6I50BCSU540068': '2025-11-08',
    'IBM7230CVS3001122': '2025-09-14',
    'IBM7230CJS3001134': '2025-09-16',
    'IBM7230CCS3001132': '2025-09-23',
    'IBM7230CCS3001026': '2026-01-03',
    'IBM7230CCS3001047': '2026-01-31',
    'IBM7230CLS3001141': '2026-01-31',
    'IBM7230CHS3001125': '2025-09-19',
    'IBM7230CJS3001139': '2025-09-16',
    'IBM7230CLS3001150': '2026-01-01',
    'IBM7230CLS3001117': '2025-09-12',
    'IBM7230CAS3001137': '2025-09-12',
    'IBM7230CLS3001118': '2025-09-12',
    'IBM7230CCS3001143': '2025-09-13',
    'IBM7230CLS3001149': '2025-09-12',
    'IBM7230CLS3001136': '2025-09-12',
    'IBM7230CTS3001128': '2025-09-24',
    'IBM7230CCS3001080': '2025-09-13',
    'IBM7230CLS3001050': '2026-01-31',
    'IBM7230CCS3001045': '2025-09-17',
    'IBM7230CCS3001065': '2025-09-17',
    'IBM7230CCS3001077': '2025-09-13',
    'IBM7230CCS3001083': '2025-09-24',
    'IBM7230CCS3001063': '2026-01-05',
    'IBM7230CJS3001098': '2025-09-23',
    'IBM7230CCS3001094': '2025-09-11',
    'IBM7230CAS3001090': '2025-09-13',
    'IBM7230CPS3001101': '2025-09-13',
    'IBM7230CLS3001088': '2025-09-16',
    'IBM7230CCS3001102': '2025-09-13',
    'IBM7230CCS3001035': '2025-09-16',
    'IBM7230CCS3001108': '2025-09-13',
    'IBM7230CCS3001112': '2025-09-14',
    'IBM7230CLS3001110': '2025-09-16',
    'IBM7230CJS3001036': '2025-09-24',
    'IBM7230CCS3001057': '2025-09-18',
    'IBM7230CCS3001028': '2025-11-17',
    'IBM7230CCS3001071': '2025-09-14',
    'IBM7230CCS3001068': '2025-07-30',
    'IBM7230CHS3001110': '2025-09-14',
    'IBM7230CCS3001066': '2025-09-11',
    'IBM7230CCS3001075': '2025-09-15',
    'IBM7230CJS3001073': '2025-09-30',
    'IBM7230CCS3001073': '2025-09-23',
    'IBM7230CJS3001053': '2025-09-25',
    'IBM7230CCS3001051': '2025-09-08',
    'IBM7230CCS3001034': '2025-09-12',
    'IBM7230CVS3001069': '2025-09-13',
    'IBM7230CVS3001082': '2025-09-30',
    'IBM7230CVS3001072': '2025-09-12',
    'IBM7230CVS3001074': '2025-09-16',
    'IBM7230CCS3001060': '2025-09-16',
    'IFW8310DLSA260677': '2025-09-23',
    'IFW8310DCSA260876': '2025-09-23',
    'IFW8310DLSA260881': '2026-02-01',
    'IFW8310DLSA260853': '2025-07-26',
    'IFW8310DESA260910': '2025-07-26',
    'IFW8310DESA260912': '2025-07-26',
    'IFW8310DPSA261028': '2025-07-26',
    'IFW8310DPSA261036': '2025-07-26',
    'IFW8310DESB261152': '2025-10-03',
    'IFW8310DPSB260929': '2026-02-01',
    'IFW8310DESB261222': '2026-02-01',
    'IFW8310DPSB260963': '2025-09-18',
    'IFW8310DASB260937': '2025-09-26',
    'IFW8310DPSB261126': '2026-01-31',
    'IFW8310DPSB261205': '2025-09-26',
    'IBM7230CTS3001114': '2025-09-12',
    'IFW8310DHSB261010': '2025-10-03',
    'IFW8310DPSB260946': '2025-09-23',
    'IFW8310DPSB260905': '2025-09-17',
    'IFW8310DCSB261105': '2025-09-17',
    'IFW8310DVSB261096': '2025-09-24',
    'IBM7230CJS3001095': '2025-09-14',
    'INV4025MJS0250247': '2025-09-19',
    'INV4025MPS0250245': '2025-10-10',
    'INV4025MKS0250246': '2025-09-05',
    'INV4025MKS0250249': '2025-10-30',
    'INV4025MVS0250233': '2025-09-20'
};
const COMPONENT_KEYS = ['display', 'gps', 'steering', 'jdlink'];
const COMPONENT_LABELS = { display: 'Display', gps: 'GPS', steering: 'Steering', jdlink: 'JDLink' };
const COMPONENT_COLORS = {
    display: '#dd6b20',
    gps: '#805ad5',
    steering: '#319795',
    jdlink: '#2d3748'
};

// ---- Chart.js Global Config (HD rendering on all screens) ----
Chart.defaults.devicePixelRatio = Math.max(window.devicePixelRatio || 1, 2);

// ---- Initialization ----
document.addEventListener('DOMContentLoaded', () => {
    // Dark mode: apply saved preference before render
    if (localStorage.getItem(DARK_MODE_KEY) === '1') {
        document.body.classList.add('dark');
    }

    setupEventListeners();
    setupKeyboardShortcuts();
    registerServiceWorker();

    loadImplements();

    if (loadFromStorage()) {
        onDataLoaded();
    }

    // Local data is now in globalData — safe to start cloud sync if cloud is ready.
    _localDataLoaded = true;
    maybeInitCloudSync();
});

function setupEventListeners() {
    // Dashboard filters
    document.getElementById('searchInput').addEventListener('input', applyFilter);
    document.getElementById('statusFilter').addEventListener('change', applyFilter);
    document.getElementById('siteFilter').addEventListener('change', applyFilter);
    document.getElementById('componentFilter').addEventListener('change', applyFilter);

    // Edit page search box
    const editSearch = document.getElementById('editSearch');
    if (editSearch) editSearch.addEventListener('input', renderEditTable);

    // Implements page search box
    const implementSearch = document.getElementById('implementSearch');
    if (implementSearch) implementSearch.addEventListener('input', renderImplementsTable);

    // Edit page CSV upload
    const editInput = document.getElementById('editCsvInput');
    editInput.addEventListener('change', e => {
        const file = e.target.files[0];
        if (file) handleEditCSVImport(file);
        editInput.value = '';
    });

    // Edit page drag & drop
    const dropZone = document.getElementById('editDropZone');
    dropZone.addEventListener('dragover', e => { e.preventDefault(); dropZone.style.borderColor = '#3182ce'; });
    dropZone.addEventListener('dragleave', () => { dropZone.style.borderColor = ''; });
    dropZone.addEventListener('drop', e => {
        e.preventDefault();
        dropZone.style.borderColor = '';
        const file = e.dataTransfer.files[0];
        if (file && file.name.endsWith('.csv')) {
            handleEditCSVImport(file);
        } else {
            showToast('Please upload a .csv file', 'error');
        }
    });

    // Restore backup file input
    const restoreInput = document.getElementById('restoreFileInput');
    if (restoreInput) {
        restoreInput.addEventListener('change', e => {
            const file = e.target.files[0];
            if (file) importBackup(file);
            restoreInput.value = '';
        });
    }
}

function setupKeyboardShortcuts() {
    document.addEventListener('keydown', e => {
        const tag = e.target.tagName;
        const isTyping = tag === 'INPUT' || tag === 'TEXTAREA' || e.target.isContentEditable;

        if (e.key === 'Escape') {
            closeModal();
            closeHistory();
            closeImportReport();
            if (isTyping) e.target.blur();
            return;
        }

        if (isTyping) return;

        if (e.key === '/') {
            e.preventDefault();
            const id = currentView === 'editUnits' ? 'editSearch' : 'searchInput';
            document.getElementById(id)?.focus();
        } else if (e.key === 'n' && currentView === 'editUnits') {
            e.preventDefault();
            showAddForm();
        } else if ((e.ctrlKey || e.metaKey) && e.key === 's') {
            e.preventDefault();
            exportBackup();
        } else if (e.key === 'd' && e.shiftKey) {
            e.preventDefault();
            toggleDarkMode();
        }
    });
}

function toggleDarkMode() {
    const dark = document.body.classList.toggle('dark');
    localStorage.setItem(DARK_MODE_KEY, dark ? '1' : '0');
    // Re-render charts so colors pick up (Chart.js doesn't reactively recolor)
    if (currentView === 'dashboard' && globalData.length > 0) {
        updateDashboard(filteredData);
    }
}

function registerServiceWorker() {
    if ('serviceWorker' in navigator) {
        window.addEventListener('load', () => {
            navigator.serviceWorker.register('./service-worker.js').catch(() => { /* offline support is best-effort */ });
        });
    }
}

// ---- Utilities ----
function clean(v) { return (v || '').toString().trim(); }
function isGood(v) { return clean(v).toLowerCase() === 'good'; }
function pct(part, total) { return total > 0 ? Math.round((part / total) * 1000) / 10 : 0; }

function getVal(row, key) {
    const k = Object.keys(row).find(h => h.toLowerCase().trim() === key.toLowerCase());
    return k ? row[k] : '';
}

function generateId() {
    return 'u_' + Date.now() + '_' + Math.random().toString(36).substr(2, 5);
}

function showToast(message, type = 'info') {
    const container = document.getElementById('toastContainer');
    const toast = document.createElement('div');
    toast.className = `toast ${type}`;
    const icons = { success: 'check-circle', error: 'times-circle', warning: 'exclamation-circle', info: 'info-circle' };
    toast.innerHTML = `<i class="fas fa-${icons[type] || icons.info}"></i> ${message}`;
    container.appendChild(toast);
    setTimeout(() => { toast.style.opacity = '0'; setTimeout(() => toast.remove(), 300); }, 4000);
}

function showLoading(show) {
    document.getElementById('loadingOverlay').classList.toggle('active', show);
}

function escapeHtml(v) {
    return String(v == null ? '' : v)
        .replace(/&/g, '&amp;')
        .replace(/</g, '&lt;')
        .replace(/>/g, '&gt;')
        .replace(/"/g, '&quot;')
        .replace(/'/g, '&#39;');
}

function formatDuration(ms) {
    if (!ms || ms < 0) return '0h';
    const h = ms / 3600000;
    if (h < 1) return Math.round(ms / 60000) + 'm';
    if (h < 24) return h.toFixed(1) + 'h';
    const d = h / 24;
    return d.toFixed(1) + 'd';
}

// ============================================================
// NAVIGATION
// ============================================================

function navigateTo(view) {
    // Role gating: viewers can only see the dashboard; only owners see Users.
    if ((view === 'editUnits' || view === 'implements') && !canEdit()) {
        showToast('Read-only access — viewers can only see the dashboard', 'warning');
        view = 'dashboard';
    }
    if (view === 'users' && !isOwner()) {
        showToast('Owner only', 'warning');
        view = 'dashboard';
    }

    document.getElementById('viewDashboard').style.display = (view === 'dashboard') ? 'block' : 'none';
    document.getElementById('viewEditUnits').style.display = (view === 'editUnits') ? 'block' : 'none';
    const implView = document.getElementById('viewImplements');
    if (implView) implView.style.display = (view === 'implements') ? 'block' : 'none';
    const usersView = document.getElementById('viewUsers');
    if (usersView) usersView.style.display = (view === 'users') ? 'block' : 'none';

    document.querySelectorAll('.nav__link').forEach(el => el.classList.remove('active'));
    const activeLink = document.querySelector(`[data-view="${view}"]`);
    if (activeLink) activeLink.classList.add('active');

    currentView = view;

    if (view === 'dashboard') {
        loadFromStorage();
        if (globalData.length > 0) {
            document.getElementById('emptyState').style.display = 'none';
            document.getElementById('dashboardContent').style.display = 'block';
            filteredData = [...globalData];
            clearFilter();
            checkPendingAlerts();
        } else {
            document.getElementById('emptyState').style.display = '';
            document.getElementById('dashboardContent').style.display = 'none';
        }
    }

    if (view === 'editUnits') {
        loadFromStorage();
        renderEditTable();
    }

    if (view === 'implements') {
        loadImplements();
        renderImplementsTable();
    }

    if (view === 'users') {
        ensureUsersSubscription();
        renderUsersView();
    }
}

// ============================================================
// LOCAL STORAGE
// ============================================================

function saveToStorage(data) {
    try {
        localStorage.setItem(STORAGE_KEY, JSON.stringify(data));
        writeAutoBackup(data);
    } catch (e) {
        showToast('Storage full. Could not save data.', 'error');
    }
}

function writeAutoBackup(data) {
    try {
        const ring = JSON.parse(localStorage.getItem(BACKUP_RING_KEY) || '[]');
        ring.push({ at: Date.now(), count: data.length, units: data });
        while (ring.length > BACKUP_RING_SIZE) ring.shift();
        localStorage.setItem(BACKUP_RING_KEY, JSON.stringify(ring));
    } catch (e) { /* ignore quota issues on backup */ }
}

function loadFromStorage() {
    try {
        const raw = localStorage.getItem(STORAGE_KEY);
        if (raw) {
            globalData = JSON.parse(raw);
            return globalData.length > 0;
        }
    } catch (e) { /* ignore */ }
    globalData = [];
    return false;
}

function addUnits(newUnits) {
    const existingSNs = new Set(globalData.map(d => (d.sn || '').toLowerCase()));
    const toAdd = [];
    const skippedDetails = [];
    let skipped = 0;

    newUnits.forEach(u => {
        if (!u.id) u.id = generateId();
        const snLower = (u.sn || '').toLowerCase();
        if (snLower && existingSNs.has(snLower)) {
            skipped++;
            skippedDetails.push({ name: u.name, sn: u.sn, reason: 'Duplicate serial number' });
        } else {
            if (!u.downtimeHistory) u.downtimeHistory = [];
            if (!isGood(u.status)) u.breakdownStartedAt = Date.now();
            toAdd.push(u);
            if (snLower) existingSNs.add(snLower);
        }
    });

    if (toAdd.length > 0) {
        globalData = [...globalData, ...toAdd];
        saveToStorage(globalData);
        recordChange({ type: 'added', detail: `${toAdd.length} unit(s) added` });
        toAdd.forEach(u => logEvent({ action: 'add', unitId: u.id, unitName: u.name, after: u.sn }));
        cloudPushUnits(toAdd);
    }

    return { added: toAdd.length, skipped, skippedDetails };
}

function updateUnit(id, fields) {
    const idx = globalData.findIndex(d => d.id === id);
    if (idx === -1) return false;

    const before = { ...globalData[idx] };
    const unit = { ...before, ...fields };

    // Downtime tracking when status changes
    if (fields.status !== undefined && fields.status !== before.status) {
        trackStatusChange(unit, before.status, fields.status);
    }

    globalData[idx] = unit;
    saveToStorage(globalData);
    cloudPushUnits([unit]);
    recordChange({ type: 'updated', detail: `Unit "${unit.name}" updated` });

    // Log each field change
    Object.keys(fields).forEach(field => {
        if (field === 'id' || field === 'downtimeHistory' || field === 'breakdownStartedAt') return;
        if (before[field] !== fields[field]) {
            logEvent({
                action: 'update',
                unitId: id,
                unitName: unit.name,
                field,
                before: before[field],
                after: fields[field]
            });
        }
    });
    return true;
}

function deleteUnits(ids) {
    const idSet = new Set(ids);
    const removed = globalData.filter(d => idSet.has(d.id));
    const count = removed.length;
    globalData = globalData.filter(d => !idSet.has(d.id));
    saveToStorage(globalData);
    if (count > 0) {
        recordChange({ type: 'deleted', detail: `${count} unit(s) deleted` });
        removed.forEach(u => logEvent({ action: 'delete', unitId: u.id, unitName: u.name, before: u.sn }));
        cloudDeleteUnits(ids);
    }
    return { count, removed };
}

// ============================================================
// AUDIT LOG
// ============================================================

function logEvent(entry) {
    const ts = Date.now();
    const id = `${ts}_${Math.random().toString(36).slice(2, 10)}`;
    const actor = currentUser ? {
        actorUid:   currentUser.uid,
        actorEmail: currentUser.email || '',
        actorName:  (currentUserDoc && currentUserDoc.displayName)
                    || currentUser.displayName
                    || (currentUser.email || '').split('@')[0],
        actorRole:  (currentUserDoc && currentUserDoc.role) || 'unknown'
    } : { actorUid: '', actorEmail: '', actorName: 'system', actorRole: 'system' };

    const full = {
        id,
        timestamp: ts,
        action:    entry.action || 'edit',
        unitId:    entry.unitId || '',
        unitName:  entry.unitName || '',
        field:     entry.field || '',
        before:    entry.before != null ? String(entry.before) : '',
        after:     entry.after  != null ? String(entry.after)  : '',
        ...actor
    };

    // Local cache — instant render + offline support.
    try {
        const log = JSON.parse(localStorage.getItem(AUDIT_LOG_KEY) || '[]');
        log.unshift(full);
        while (log.length > AUDIT_LOG_MAX) log.pop();
        localStorage.setItem(AUDIT_LOG_KEY, JSON.stringify(log));
    } catch (e) { /* ignore */ }

    // Cloud push — coalesce many calls in the same tick into one batch.
    // Skip if not signed in (auto-migrations), or while a snapshot is being
    // applied (those mutations aren't user-initiated and shouldn't be logged).
    if (!currentUser || suppressCloudWrites || !window.cloud?.addHistoryEvents) return;
    _historyPushQueue.push(full);
    if (_historyFlushTimer) return;
    _historyFlushTimer = setTimeout(() => {
        const batch = _historyPushQueue.splice(0);
        _historyFlushTimer = null;
        if (batch.length === 0) return;
        window.cloud.addHistoryEvents(batch).catch(err => {
            console.error('[cloud] history push failed:', err);
            if (err && err.code === 'permission-denied') {
                showHistoryRulesBanner();
            }
        });
    }, 80);
}

function getAuditLog() {
    let local = [];
    try { local = JSON.parse(localStorage.getItem(AUDIT_LOG_KEY) || '[]'); }
    catch (e) {}
    // Cloud is the source of truth; fall back to local when offline or for
    // any entry that hasn't synced yet.
    if (!cloudHistory.length) return local;
    const seen = new Set(cloudHistory.map(e => e.id));
    const merged = [...cloudHistory, ...local.filter(e => e.id && !seen.has(e.id))];
    merged.sort((a, b) => (b.timestamp || 0) - (a.timestamp || 0));
    return merged.slice(0, AUDIT_LOG_MAX);
}

function showHistory(unitId) {
    const log = getAuditLog();
    const filtered = unitId ? log.filter(e => e.unitId === unitId) : log;
    const title = unitId
        ? `History: ${escapeHtml((globalData.find(u => u.id === unitId) || {}).name || 'Unit')}`
        : 'Change History';
    document.getElementById('historyTitle').innerHTML = title;

    const modal = document.getElementById('historyModal');
    // Stash filter so live snapshots can re-render with the same scope.
    modal.dataset.unitId = unitId || '';

    const tbody = document.getElementById('historyBody');
    if (filtered.length === 0) {
        tbody.innerHTML = '<tr><td colspan="7" style="text-align:center;padding:24px;color:#718096">No history recorded</td></tr>';
    } else {
        tbody.innerHTML = filtered.map(e => {
            const who = e.actorName
                ? `<span class="audit-actor" title="${escapeHtml(e.actorEmail || '')}">${escapeHtml(e.actorName)} <em>(${escapeHtml(e.actorRole || '?')})</em></span>`
                : '<span style="color:#a0aec0">—</span>';
            return `
            <tr>
                <td style="white-space:nowrap">${new Date(e.timestamp).toLocaleString()}</td>
                <td><span class="audit-badge audit-${escapeHtml(e.action)}">${escapeHtml(e.action)}</span></td>
                <td>${who}</td>
                <td>${escapeHtml(e.unitName || '-')}</td>
                <td>${escapeHtml(e.field || '-')}</td>
                <td>${escapeHtml(e.before != null ? e.before : '-')}</td>
                <td>${escapeHtml(e.after  != null ? e.after  : '-')}</td>
            </tr>`;
        }).join('');
    }
    modal.classList.add('open');
}

function closeHistory() {
    document.getElementById('historyModal').classList.remove('open');
}

function clearHistory() {
    if (!isOwner || !isOwner()) {
        showToast('Only the owner can clear shared history', 'warning');
        return;
    }
    if (!confirm('Clear ALL change history for the entire team? This cannot be undone.')) return;
    localStorage.removeItem(AUDIT_LOG_KEY);
    if (window.cloud?.clearHistoryCloud) {
        window.cloud.clearHistoryCloud().then(() => {
            cloudHistory = [];
            showHistory();
            showToast('Team history cleared', 'success');
        }).catch(err => {
            console.error('[cloud] clear history failed:', err);
            showToast('Cloud clear failed — check console', 'error');
        });
    } else {
        cloudHistory = [];
        showHistory();
        showToast('History cleared', 'success');
    }
}

function exportHistory() {
    const log = getAuditLog();
    if (log.length === 0) { showToast('No history to export', 'warning'); return; }
    const headers = ['Timestamp', 'Action', 'Unit', 'Field', 'Before', 'After'];
    const rows = log.map(e => [
        new Date(e.timestamp).toISOString(),
        e.action, e.unitName || '', e.field || '',
        e.before != null ? e.before : '', e.after != null ? e.after : ''
    ]);
    const csv = [headers, ...rows].map(row => row.map(cell => `"${String(cell).replace(/"/g, '""')}"`).join(',')).join('\n');
    const blob = new Blob(['\uFEFF' + csv], { type: 'text/csv;charset=utf-8;' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = `tractor_history_${new Date().toISOString().slice(0, 10)}.csv`;
    a.click();
    URL.revokeObjectURL(url);
    showToast('History exported', 'success');
}

// ============================================================
// BACKUP & RESTORE
// ============================================================

function exportBackup() {
    const payload = {
        version: 1,
        exportedAt: new Date().toISOString(),
        count: globalData.length,
        units: globalData
    };
    const blob = new Blob([JSON.stringify(payload, null, 2)], { type: 'application/json' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = `tractor_backup_${new Date().toISOString().slice(0, 10)}.json`;
    a.click();
    URL.revokeObjectURL(url);
    showToast(`Backup exported (${globalData.length} units)`, 'success');
}

function triggerRestore() {
    document.getElementById('restoreFileInput').click();
}

function importBackup(file) {
    const reader = new FileReader();
    reader.onload = e => {
        try {
            const data = JSON.parse(e.target.result);
            if (!data || !Array.isArray(data.units)) {
                showToast('Invalid backup file', 'error');
                return;
            }
            const merge = confirm(
                `Backup contains ${data.units.length} units.\n\n` +
                `OK  = MERGE (add new, keep existing)\n` +
                `Cancel = REPLACE (wipe current, load backup)`
            );
            if (merge) {
                const result = addUnits(data.units);
                showToast(`Merged backup: ${result.added} added, ${result.skipped} duplicate(s) skipped`, 'success');
            } else {
                if (!confirm(`This will DELETE all ${globalData.length} current units and replace them with the backup. Continue?`)) return;
                globalData = data.units.map(u => ({ ...u, id: u.id || generateId() }));
                saveToStorage(globalData);
                logEvent({ action: 'restore', unitName: '-', after: `Restored ${data.units.length} units from backup` });
                recordChange({ type: 'restored', detail: `${data.units.length} units restored from backup` });
                showToast(`Restored ${data.units.length} units from backup`, 'success');
            }
            renderEditTable();
        } catch (err) {
            showToast('Failed to read backup: ' + err.message, 'error');
        }
    };
    reader.readAsText(file);
}

// ============================================================
// CHANGE ALERT SYSTEM
// ============================================================

function recordChange(change) {
    const changes = JSON.parse(sessionStorage.getItem(PENDING_CHANGES_KEY) || '[]');
    changes.push({ ...change, timestamp: Date.now() });
    sessionStorage.setItem(PENDING_CHANGES_KEY, JSON.stringify(changes));
}

function checkPendingAlerts() {
    const changes = JSON.parse(sessionStorage.getItem(PENDING_CHANGES_KEY) || '[]');
    if (changes.length === 0) return;

    const summary = changes.map(c => c.detail).join('; ');
    document.getElementById('changeBannerText').textContent = `Data updated: ${summary}`;
    document.getElementById('changeBanner').classList.add('show');
    sessionStorage.removeItem(PENDING_CHANGES_KEY);
}

function dismissBanner() {
    document.getElementById('changeBanner').classList.remove('show');
}

// ============================================================
// DOWNTIME TRACKING
// ============================================================

function trackStatusChange(unit, oldStatus, newStatus) {
    const wasGood = isGood(oldStatus);
    const willBeGood = isGood(newStatus);
    if (wasGood && !willBeGood) {
        unit.breakdownStartedAt = Date.now();
    } else if (!wasGood && willBeGood && unit.breakdownStartedAt) {
        const start = unit.breakdownStartedAt;
        const end = Date.now();
        if (!unit.downtimeHistory) unit.downtimeHistory = [];
        unit.downtimeHistory.push({ start, end, durationMs: end - start });
        unit.breakdownStartedAt = null;
        unit.breakdownReason = '';
    }
}

function computeDowntimeStats() {
    const now = Date.now();
    const monthStart = new Date();
    monthStart.setDate(1); monthStart.setHours(0, 0, 0, 0);
    const monthStartMs = monthStart.getTime();

    let totalDowntimeMs = 0;
    let totalFailures = 0;
    let totalMonthDowntime = 0;
    const perUnit = [];

    globalData.forEach(u => {
        const history = u.downtimeHistory || [];
        let unitDowntime = 0;
        history.forEach(iv => {
            totalDowntimeMs += iv.durationMs;
            unitDowntime += iv.durationMs;
            totalFailures++;
            if (iv.end >= monthStartMs) {
                totalMonthDowntime += Math.min(iv.durationMs, iv.end - Math.max(iv.start, monthStartMs));
            }
        });
        if (u.breakdownStartedAt) {
            const ongoing = now - u.breakdownStartedAt;
            unitDowntime += ongoing;
            totalDowntimeMs += ongoing;
            totalFailures++;
            const effStart = Math.max(u.breakdownStartedAt, monthStartMs);
            if (now > effStart) totalMonthDowntime += (now - effStart);
        }
        if (unitDowntime > 0) perUnit.push({ id: u.id, name: u.name, downtime: unitDowntime });
    });

    const mttr = totalFailures > 0 ? totalDowntimeMs / totalFailures : 0;
    const fleetOperatingMs = Math.max(1, globalData.length) * 30 * 24 * 3600 * 1000;
    const uptimeMs = Math.max(0, fleetOperatingMs - totalDowntimeMs);
    const mtbf = totalFailures > 0 ? uptimeMs / totalFailures : 0;

    perUnit.sort((a, b) => b.downtime - a.downtime);
    return { mtbf, mttr, totalMonthDowntime, totalFailures, topOffenders: perUnit.slice(0, 5), topTen: perUnit.slice(0, 10) };
}

function renderDowntimeKPIs() {
    const s = computeDowntimeStats();
    document.getElementById('kpiMTBF').textContent = formatDuration(s.mtbf);
    document.getElementById('kpiMTTR').textContent = formatDuration(s.mttr);
    document.getElementById('kpiMonthDowntime').textContent = formatDuration(s.totalMonthDowntime);
    document.getElementById('kpiFailures').textContent = s.totalFailures;

    const listEl = document.getElementById('topOffendersList');
    if (!listEl) return;
    if (s.topOffenders.length === 0) {
        listEl.innerHTML = '<div class="top-offender top-offender--empty">No downtime recorded yet</div>';
    } else {
        listEl.innerHTML = s.topOffenders.map((u, i) => `
            <div class="top-offender">
                <span class="top-offender__rank">#${i + 1}</span>
                <span class="top-offender__name">${escapeHtml(u.name || 'Unnamed')}</span>
                <span class="top-offender__time">${formatDuration(u.downtime)}</span>
            </div>`).join('');
    }

    destroyChart('downtimeChart');
    if (s.topTen.length > 0) {
        charts.downtimeChart = new Chart(document.getElementById('downtimeChart'), {
            type: 'bar',
            data: {
                labels: s.topTen.map(u => u.name || 'Unnamed'),
                datasets: [{
                    data: s.topTen.map(u => +(u.downtime / 3600000).toFixed(2)),
                    backgroundColor: '#e53e3e', borderRadius: 4, barPercentage: 0.6
                }]
            },
            options: {
                responsive: true, maintainAspectRatio: false, indexAxis: 'y',
                scales: {
                    x: { beginAtZero: true, title: { display: true, text: 'Downtime (hours)', font: { size: 11, family: 'Inter' } }, ticks: { font: { size: 11 } }, grid: { color: '#edf2f7' } },
                    y: { ticks: { font: { size: 11, family: 'Inter' } }, grid: { display: false } }
                },
                plugins: { legend: { display: false }, tooltip: { callbacks: { label: ctx => ` ${ctx.parsed.x}h` } } }
            }
        });
    }
}

// ============================================================
// DATA PROCESSING
// ============================================================

function processData(rows) {
    const valid = [];
    const rejected = [];
    rows.forEach((r, idx) => {
        const unit = {
            id: generateId(),
            name: clean(getVal(r, 'Nickname')),
            model: clean(getVal(r, 'Model')),
            sn: clean(getVal(r, 'Serial Number')),
            status: clean(getVal(r, 'Status Unit')),
            display: clean(getVal(r, 'Status Unit Display')),
            gps: clean(getVal(r, 'Status Unit GPS')),
            steering: clean(getVal(r, 'Status Unit Steering')),
            jdlink: clean(getVal(r, 'Status Unit JDLink')),
            site: clean(getVal(r, 'Site')),
            gpsLicense: clean(getVal(r, 'GPS License')),
            licenseDisplay: clean(getVal(r, 'License Display')),
            // New dual columns. Fall back to the legacy single-pair columns so
            // importing an old export still works — legacy dates map to GPS.
            gpsLicenseStartDate: clean(getVal(r, 'GPS License Start Date')) || clean(getVal(r, 'License Start Date')),
            gpsLicenseEndDate:   clean(getVal(r, 'GPS License Expiration Date')) || clean(getVal(r, 'License Expiration Date')),
            displayLicenseStartDate: clean(getVal(r, 'Display License Start Date')),
            displayLicenseEndDate:   clean(getVal(r, 'Display License Expiration Date')),
            remarks: clean(getVal(r, 'Remarks')),
            downtimeHistory: [],
            breakdownStartedAt: null
        };
        if (!unit.sn && !unit.name) {
            rejected.push({ row: idx + 2, reason: 'Missing both nickname and serial number' });
        } else if (!unit.sn) {
            rejected.push({ row: idx + 2, reason: 'Missing serial number', name: unit.name });
        } else {
            valid.push(unit);
        }
    });
    return { valid, rejected };
}

function detectIssues(d) {
    const issues = [];
    if (!isGood(d.status)) issues.push('Unit');
    if (!isGood(d.display)) issues.push('Display');
    if (!isGood(d.gps)) issues.push('GPS');
    if (!isGood(d.steering)) issues.push('Steering');
    if (!isGood(d.jdlink)) issues.push('JDLink');
    return issues;
}

function countIssues(data) {
    const counts = { Unit: 0, Display: 0, GPS: 0, Steering: 0, JDLink: 0 };
    let totalWithIssues = 0;
    data.forEach(d => {
        const issues = detectIssues(d);
        if (issues.length > 0) totalWithIssues++;
        issues.forEach(i => counts[i]++);
    });
    return { total: totalWithIssues, counts };
}

// ============================================================
// DASHBOARD RENDERING
// ============================================================

function onDataLoaded() {
    document.getElementById('emptyState').style.display = 'none';
    document.getElementById('dashboardContent').style.display = 'block';

    populateFilters();
    populateEditFilters();
    filteredData = [...globalData];
    updateDashboard(filteredData);

    const now = new Date().toLocaleString();
    document.getElementById('lastUpdated').textContent = `Updated: ${now}`;

    document.getElementById('connectionDot').classList.add('connected');
    document.getElementById('connectionLabel').textContent = `${globalData.length} Units`;

    updateEditCount();
}

function updateDashboard(data) {
    renderKPI(data);
    renderStatusChart(data);
    renderSiteChart(data);
    renderComponentHealth(data);
    renderDowntimeKPIs();
    renderTable(data);
    renderRepair();
    updateFilterCount(data);
}

// ---- KPI Cards ----
function renderKPI(data) {
    const total = data.length;
    const good = data.filter(d => isGood(d.status)).length;
    const breakdown = total - good;
    const withIssues = data.filter(d => detectIssues(d).length > 0).length;
    const healthRate = pct(good, total);

    document.getElementById('kpiTotal').textContent = total;
    document.getElementById('kpiGood').textContent = good;
    document.getElementById('kpiBreakdown').textContent = breakdown;
    document.getElementById('kpiIssue').textContent = withIssues;
    document.getElementById('kpiHealth').textContent = healthRate + '%';

    document.getElementById('kpiGoodPct').textContent = pct(good, total) + '% of total';
    document.getElementById('kpiBreakdownPct').textContent = pct(breakdown, total) + '% of total';
    document.getElementById('kpiIssuePct').textContent = pct(withIssues, total) + '% of total';

    document.getElementById('kpiGoodBar').style.width = pct(good, total) + '%';
    document.getElementById('kpiBreakdownBar').style.width = pct(breakdown, total) + '%';
    document.getElementById('kpiIssueBar').style.width = pct(withIssues, total) + '%';
    document.getElementById('kpiHealthBar').style.width = healthRate + '%';
}

// ---- Status Chart (Donut) ----
function renderStatusChart(data) {
    const good = data.filter(d => isGood(d.status)).length;
    const breakdown = data.length - good;

    destroyChart('statusChart');
    charts.statusChart = new Chart(document.getElementById('statusChart'), {
        type: 'doughnut',
        data: {
            labels: ['Good', 'Breakdown'],
            datasets: [{ data: [good, breakdown], backgroundColor: ['#38a169', '#e53e3e'], borderWidth: 0, hoverOffset: 6 }]
        },
        options: {
            responsive: true, maintainAspectRatio: false, cutout: '65%',
            plugins: {
                legend: { position: 'bottom', labels: { padding: 16, usePointStyle: true, pointStyle: 'circle', font: { size: 12, family: 'Inter' } } },
                tooltip: { callbacks: { label: ctx => { const t = ctx.dataset.data.reduce((a, b) => a + b, 0); return ` ${ctx.label}: ${ctx.parsed} (${pct(ctx.parsed, t)}%)`; } } }
            }
        }
    });
}

// ---- Site Chart (Horizontal Bar) ----
function renderSiteChart(data) {
    const siteMap = {};
    data.forEach(d => {
        const s = d.site || 'Unknown';
        if (!siteMap[s]) siteMap[s] = { good: 0, breakdown: 0 };
        if (isGood(d.status)) siteMap[s].good++; else siteMap[s].breakdown++;
    });

    const labels = Object.keys(siteMap).sort();

    destroyChart('siteChart');
    charts.siteChart = new Chart(document.getElementById('siteChart'), {
        type: 'bar',
        data: {
            labels,
            datasets: [
                { label: 'Good', data: labels.map(s => siteMap[s].good), backgroundColor: '#38a169', borderRadius: 4, barPercentage: 0.6 },
                { label: 'Breakdown', data: labels.map(s => siteMap[s].breakdown), backgroundColor: '#e53e3e', borderRadius: 4, barPercentage: 0.6 }
            ]
        },
        options: {
            responsive: true, maintainAspectRatio: false, indexAxis: 'y',
            scales: {
                x: { stacked: true, beginAtZero: true, ticks: { stepSize: 1, font: { size: 11, family: 'Inter' } }, grid: { color: '#edf2f7' } },
                y: { stacked: true, ticks: { font: { size: 11, family: 'Inter' } }, grid: { display: false } }
            },
            plugins: {
                legend: { position: 'top', align: 'end', labels: { usePointStyle: true, pointStyle: 'circle', padding: 12, font: { size: 11, family: 'Inter' } } },
                tooltip: { callbacks: { label: ctx => ` ${ctx.dataset.label}: ${ctx.parsed.x} units` } }
            }
        }
    });
}

// ---- Component Health ----
function renderComponentHealth(data) {
    const grid = document.getElementById('componentGrid');
    const total = data.length;

    grid.innerHTML = COMPONENT_KEYS.map(key => {
        const goodCount = data.filter(d => isGood(d[key])).length;
        const rate = pct(goodCount, total);
        const color = COMPONENT_COLORS[key];
        const circumference = 2 * Math.PI * 28;
        const offset = circumference - (rate / 100) * circumference;
        return `
        <div class="component-stat">
            <div class="component-stat__name">${COMPONENT_LABELS[key]}</div>
            <div class="component-stat__ring">
                <svg width="72" height="72" viewBox="0 0 72 72">
                    <circle cx="36" cy="36" r="28" fill="none" stroke="#edf2f7" stroke-width="6"/>
                    <circle cx="36" cy="36" r="28" fill="none" stroke="${color}" stroke-width="6"
                        stroke-dasharray="${circumference}" stroke-dashoffset="${offset}" stroke-linecap="round"/>
                </svg>
                <div class="component-stat__ring-text" style="color:${color}">${rate}%</div>
            </div>
            <div class="component-stat__detail">${goodCount} / ${total} Good</div>
        </div>`;
    }).join('');
}

// ---- Detail Table ----
function renderTable(data) {
    const tbody = document.getElementById('detailBody');
    tbody.innerHTML = data.map((d, i) => {
        const isBD = !isGood(d.status);
        return `
        <tr class="${isBD ? 'row-breakdown' : ''}">
            <td>${i + 1}</td>
            <td><strong>${escapeHtml(d.name)}</strong></td>
            <td>${escapeHtml(d.model)}</td>
            <td style="font-family:monospace;font-size:12px">${escapeHtml(d.sn)}</td>
            <td>${!isGood(d.status) && d.breakdownReason
                ? `<span class="badge badge-breakdown bd-clickable" onclick="showBreakdownPopover(event, '${escapeHtml(d.breakdownReason).replace(/'/g, "\\'")}')"><i class="fas fa-xmark"></i> ${escapeHtml(d.status)}</span>`
                : `<span class="badge ${isGood(d.status) ? 'badge-good' : 'badge-breakdown'}"><i class="fas fa-${isGood(d.status) ? 'check' : 'xmark'}"></i> ${escapeHtml(d.status)}</span>`
            }</td>
            <td class="${isGood(d.display) ? 'cell-good' : 'cell-bad'}">${escapeHtml(d.display)}</td>
            <td class="${isGood(d.gps) ? 'cell-good' : 'cell-bad'}">${escapeHtml(d.gps)}</td>
            <td class="${isGood(d.steering) ? 'cell-good' : 'cell-bad'}">${escapeHtml(d.steering)}</td>
            <td class="${isGood(d.jdlink) ? 'cell-good' : 'cell-bad'}">${escapeHtml(d.jdlink)}</td>
            <td>${escapeHtml(d.site)}</td>
            <td>${d.userCategory ? `<span class="badge badge-cat" style="font-size:10px">${escapeHtml(d.userCategory)}</span>` : '<span style="color:#a0aec0;font-size:11px">—</span>'}</td>
            <td>${d.gpsLicense ? `<span class="badge badge-good" style="font-size:10px">${escapeHtml(d.gpsLicense)}</span>` : '<span style="color:#a0aec0;font-size:11px">—</span>'}</td>
            <td>${licenseBadgeFor(d, 'gps')}</td>
            <td>${d.licenseDisplay ? `<span class="badge badge-good" style="font-size:10px">${escapeHtml(d.licenseDisplay)}</span>` : '<span style="color:#a0aec0;font-size:11px">—</span>'}</td>
            <td>${licenseBadgeFor(d, 'display')}</td>
        </tr>`;
    }).join('');
}

// ---- Sorting ----
function sortTable(key) {
    if (sortState.key === key) { sortState.asc = !sortState.asc; } else { sortState.key = key; sortState.asc = true; }
    if (key === 'no') { sortState.key = null; filteredData = [...applyFilterLogic()]; }
    else {
        filteredData.sort((a, b) => {
            const va = (a[key] || '').toLowerCase(), vb = (b[key] || '').toLowerCase();
            if (va < vb) return sortState.asc ? -1 : 1;
            if (va > vb) return sortState.asc ? 1 : -1;
            return 0;
        });
    }
    renderTable(filteredData);
    updateFilterCount(filteredData);
}

// ---- Repair & Maintenance ----
function renderRepair() {
    const issueFilterVal = document.getElementById('issueFilter').value;
    const issueData = countIssues(globalData);
    const chipColors = { Unit: '#e53e3e', Display: '#dd6b20', GPS: '#805ad5', Steering: '#319795', JDLink: '#2d3748' };

    document.getElementById('issueSummary').innerHTML = Object.entries(issueData.counts).map(([key, count]) => `
        <div class="issue-chip">
            <span class="issue-chip__dot" style="background:${chipColors[key]}"></span>
            <span class="issue-chip__label">${key}</span>
            <span class="issue-chip__count">${count}</span>
        </div>`).join('');

    // Top Issue Chart
    const sorted = Object.entries(issueData.counts).filter(([, v]) => v > 0).sort((a, b) => b[1] - a[1]);
    destroyChart('topIssueChart');
    charts.topIssueChart = new Chart(document.getElementById('topIssueChart'), {
        type: 'bar',
        data: { labels: sorted.map(x => x[0]), datasets: [{ data: sorted.map(x => x[1]), backgroundColor: sorted.map(x => chipColors[x[0]]), borderRadius: 4, barPercentage: 0.5 }] },
        options: { responsive: true, maintainAspectRatio: false, indexAxis: 'y',
            scales: { x: { beginAtZero: true, ticks: { stepSize: 1, font: { size: 11 } }, grid: { color: '#edf2f7' } }, y: { ticks: { font: { size: 11, family: 'Inter', weight: 600 } }, grid: { display: false } } },
            plugins: { legend: { display: false } } }
    });

    // Issues by Site Chart
    const siteCounts = {};
    globalData.forEach(d => { if (detectIssues(d).length > 0) { const s = d.site || 'Unknown'; siteCounts[s] = (siteCounts[s] || 0) + 1; } });
    const siteLabels = Object.keys(siteCounts).sort();
    destroyChart('issueBySiteChart');
    charts.issueBySiteChart = new Chart(document.getElementById('issueBySiteChart'), {
        type: 'bar',
        data: { labels: siteLabels, datasets: [{ data: siteLabels.map(s => siteCounts[s]), backgroundColor: '#d69e2e', borderRadius: 4, barPercentage: 0.5 }] },
        options: { responsive: true, maintainAspectRatio: false,
            scales: { y: { beginAtZero: true, ticks: { stepSize: 1, font: { size: 11 } }, grid: { color: '#edf2f7' } }, x: { ticks: { font: { size: 11, family: 'Inter' } }, grid: { display: false } } },
            plugins: { legend: { display: false } } }
    });

    // Repair Table
    let repairRows = globalData.filter(d => detectIssues(d).length > 0);
    if (issueFilterVal) repairRows = repairRows.filter(d => detectIssues(d).includes(issueFilterVal));
    document.getElementById('repairBody').innerHTML = repairRows.map((d, i) => `
        <tr>
            <td>${i + 1}</td>
            <td><strong>${escapeHtml(d.name)}</strong></td>
            <td>${escapeHtml(d.model)}</td>
            <td style="font-family:monospace;font-size:12px">${escapeHtml(d.sn)}</td>
            <td>${detectIssues(d).map(x => `<span class="badge-component badge-${x.toLowerCase()}">${escapeHtml(x)}</span>`).join(' ')}</td>
            <td>${escapeHtml(d.site)}</td>
        </tr>`).join('');
}

// ============================================================
// FILTERS
// ============================================================

function populateFilters() {
    const statuses = [...new Set(globalData.map(d => d.status))].filter(Boolean).sort();
    const sites = [...new Set(globalData.map(d => d.site))].filter(Boolean).sort();
    document.getElementById('statusFilter').innerHTML = `<option value="">All Status</option>` + statuses.map(s => `<option value="${escapeHtml(s)}">${escapeHtml(s)}</option>`).join('');
    document.getElementById('siteFilter').innerHTML = `<option value="">All Sites</option>` + sites.map(s => `<option value="${escapeHtml(s)}">${escapeHtml(s)}</option>`).join('');
}

function applyFilterLogic() {
    const keyword = document.getElementById('searchInput').value.toLowerCase();
    const statusVal = document.getElementById('statusFilter').value;
    const siteVal = document.getElementById('siteFilter').value;
    const compVal = document.getElementById('componentFilter').value;

    return globalData.filter(d => {
        if (statusVal && d.status !== statusVal) return false;
        if (siteVal && d.site !== siteVal) return false;
        if (keyword && !`${d.name} ${d.model} ${d.sn}`.toLowerCase().includes(keyword)) return false;
        if (compVal && !detectIssues(d).includes(compVal)) return false;
        return true;
    });
}

function applyFilter() {
    filteredData = applyFilterLogic();
    sortState.key = null;
    updateDashboard(filteredData);
}

function clearFilter() {
    document.getElementById('searchInput').value = '';
    document.getElementById('statusFilter').value = '';
    document.getElementById('siteFilter').value = '';
    document.getElementById('componentFilter').value = '';
    filteredData = [...globalData];
    sortState.key = null;
    updateDashboard(filteredData);
}

function updateFilterCount(data) {
    const el = document.getElementById('filterCount');
    const total = globalData.length;
    el.textContent = data.length === total ? `${total} units` : `${data.length} of ${total} units`;
}

// ============================================================
// EXPORT
// ============================================================

function exportCSV() {
    if (filteredData.length === 0) { showToast('No data to export', 'warning'); return; }
    const headers = ['No', 'Nickname', 'Model', 'Serial Number', 'Status', 'Display', 'GPS', 'Steering', 'JDLink', 'Site',
                     'User Category', 'GPS License', 'Display License',
                     'GPS License Start Date', 'GPS License Expiration Date',
                     'Display License Start Date', 'Display License Expiration Date', 'Remarks'];
    const rows = filteredData.map((d, i) => [i + 1, d.name, d.model, d.sn, d.status, d.display, d.gps, d.steering, d.jdlink, d.site,
                     d.userCategory || '', d.gpsLicense || '', d.licenseDisplay || '',
                     d.gpsLicenseStartDate || d.licenseStartDate || '',
                     d.gpsLicenseEndDate   || d.licenseEndDate   || '',
                     d.displayLicenseStartDate || '',
                     d.displayLicenseEndDate   || '',
                     d.remarks || '']);
    const csv = [headers, ...rows].map(row => row.map(cell => `"${String(cell).replace(/"/g, '""')}"`).join(',')).join('\n');
    const blob = new Blob(['\uFEFF' + csv], { type: 'text/csv;charset=utf-8;' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = `tractor_monitoring_${new Date().toISOString().slice(0, 10)}.csv`;
    a.click();
    URL.revokeObjectURL(url);
    showToast(`Exported ${filteredData.length} units to CSV`, 'success');
}

// ============================================================
// CHART UTILITIES
// ============================================================

function destroyChart(id) {
    if (charts[id]) { charts[id].destroy(); delete charts[id]; }
    const oldCanvas = document.getElementById(id);
    if (oldCanvas) {
        const newCanvas = document.createElement('canvas');
        newCanvas.id = id;
        oldCanvas.parentNode.replaceChild(newCanvas, oldCanvas);
    }
}

// ============================================================
// EDIT UNITS PAGE
// ============================================================

function updateEditCount() {
    const el = document.getElementById('editUnitCount');
    if (el) el.textContent = `${globalData.length} unit(s) in database`;
}

function toggleImportPanel() {
    document.getElementById('importPanel').classList.toggle('open');
}

function handleEditCSVImport(file) {
    showLoading(true);
    Papa.parse(file, {
        header: true,
        skipEmptyLines: true,
        complete: result => {
            const { valid, rejected } = processData(result.data);
            const { added, skipped, skippedDetails } = addUnits(valid);
            showImportReport({ total: result.data.length, added, skipped, skippedDetails, rejected });
            renderEditTable();
            showLoading(false);
            document.getElementById('importPanel').classList.remove('open');
        },
        error: err => {
            showToast('Failed to parse CSV: ' + err.message, 'error');
            showLoading(false);
        }
    });
}

function showImportReport({ total, added, skipped, skippedDetails, rejected }) {
    const hasIssues = skipped > 0 || rejected.length > 0;
    const type = added > 0 ? (hasIssues ? 'warning' : 'success') : 'warning';
    const shortMsg = `Imported ${added} of ${total}. ${skipped} duplicate(s), ${rejected.length} rejected.`;
    showToast(shortMsg, type);

    if (!hasIssues) return;

    const rows = [
        ...skippedDetails.map(d => `<tr><td>${escapeHtml(d.name || '-')}</td><td style="font-family:monospace">${escapeHtml(d.sn || '-')}</td><td>${escapeHtml(d.reason)}</td></tr>`),
        ...rejected.map(r => `<tr><td>${escapeHtml(r.name || '-')}</td><td>Row ${r.row}</td><td>${escapeHtml(r.reason)}</td></tr>`)
    ].join('');

    document.getElementById('importReportBody').innerHTML = rows;
    document.getElementById('importReportSummary').textContent =
        `${added} added · ${skipped} duplicate(s) · ${rejected.length} rejected`;
    document.getElementById('importReportModal').classList.add('open');
}

function closeImportReport() {
    document.getElementById('importReportModal').classList.remove('open');
}

// ---- Edit Table ----
function renderEditTable() {
    updateEditCount();
    selectedUnitIds.clear();
    updateSelectedCount();

    const selectAllBox = document.getElementById('selectAll');
    if (selectAllBox) selectAllBox.checked = false;

    const query = (document.getElementById('editSearch')?.value || '').toLowerCase().trim();
    const statusVal = (document.getElementById('editStatusFilter')?.value || '');
    const siteVal = (document.getElementById('editSiteFilter')?.value || '');

    let rows = [...globalData];
    if (query) rows = rows.filter(d => `${d.name} ${d.model} ${d.sn} ${d.site}`.toLowerCase().includes(query));
    if (statusVal) rows = rows.filter(d => d.status === statusVal);
    if (siteVal) rows = rows.filter(d => d.site === siteVal);

    // Apply sort
    if (editSortState.key && editSortState.key !== 'no') {
        const k = editSortState.key;
        rows.sort((a, b) => {
            const va = (a[k] || '').toLowerCase(), vb = (b[k] || '').toLowerCase();
            if (va < vb) return editSortState.asc ? -1 : 1;
            if (va > vb) return editSortState.asc ? 1 : -1;
            return 0;
        });
    }

    const tbody = document.getElementById('editBody');
    if (rows.length === 0) {
        tbody.innerHTML = `<tr><td colspan="18" style="text-align:center;padding:24px;color:#718096">${(query || statusVal || siteVal) ? 'No units match your filters' : 'No units yet. Click <strong>Add Unit</strong> or <strong>Import CSV</strong> to get started.'}</td></tr>`;
        return;
    }

    tbody.innerHTML = rows.map((d, i) => {
        const remarks = d.remarks || '';
        const remarksShort = remarks.length > 40 ? remarks.slice(0, 40) + '…' : remarks;
        return `
        <tr>
            <td class="col-check"><input type="checkbox" class="unit-check" data-id="${escapeHtml(d.id)}" onchange="updateSelectedCount()"></td>
            <td>${i + 1}</td>
            <td><span class="inline-edit" contenteditable="true" data-id="${escapeHtml(d.id)}" data-field="name" onblur="saveInlineEdit(this)">${escapeHtml(d.name)}</span></td>
            <td><span class="inline-edit" contenteditable="true" data-id="${escapeHtml(d.id)}" data-field="model" onblur="saveInlineEdit(this)">${escapeHtml(d.model)}</span></td>
            <td style="font-family:monospace;font-size:12px">${escapeHtml(d.sn)}</td>
            <td><span class="inline-edit" contenteditable="true" data-id="${escapeHtml(d.id)}" data-field="status" onblur="saveInlineEdit(this)">${escapeHtml(d.status)}</span></td>
            <td><span class="inline-edit" contenteditable="true" data-id="${escapeHtml(d.id)}" data-field="display" onblur="saveInlineEdit(this)">${escapeHtml(d.display)}</span></td>
            <td><span class="inline-edit" contenteditable="true" data-id="${escapeHtml(d.id)}" data-field="gps" onblur="saveInlineEdit(this)">${escapeHtml(d.gps)}</span></td>
            <td><span class="inline-edit" contenteditable="true" data-id="${escapeHtml(d.id)}" data-field="steering" onblur="saveInlineEdit(this)">${escapeHtml(d.steering)}</span></td>
            <td><span class="inline-edit" contenteditable="true" data-id="${escapeHtml(d.id)}" data-field="jdlink" onblur="saveInlineEdit(this)">${escapeHtml(d.jdlink)}</span></td>
            <td><span class="inline-edit" contenteditable="true" data-id="${escapeHtml(d.id)}" data-field="site" onblur="saveInlineEdit(this)">${escapeHtml(d.site)}</span></td>
            <td>${d.userCategory ? `<span class="badge badge-cat" style="font-size:10px">${escapeHtml(d.userCategory)}</span>` : '<span style="color:#a0aec0;font-size:11px">—</span>'}</td>
            <td>${d.gpsLicense ? `<span class="badge badge-good" style="font-size:10px">${escapeHtml(d.gpsLicense)}</span>` : '<span style="color:#a0aec0;font-size:11px">—</span>'}</td>
            <td>${licenseBadgeFor(d, 'gps')}</td>
            <td>${d.licenseDisplay ? `<span class="badge badge-good" style="font-size:10px">${escapeHtml(d.licenseDisplay)}</span>` : '<span style="color:#a0aec0;font-size:11px">—</span>'}</td>
            <td>${licenseBadgeFor(d, 'display')}</td>
            <td style="max-width:180px;font-size:12px;color:#4a5568" title="${escapeHtml(remarks)}">${escapeHtml(remarksShort) || '<span style="color:#a0aec0">—</span>'}</td>
            <td class="col-actions">
                <div class="row-actions">
                    <button class="btn btn-secondary" title="History" onclick="showHistory('${escapeHtml(d.id)}')"><i class="fas fa-clock-rotate-left"></i></button>
                    <button class="btn btn-secondary" title="Edit" onclick="editUnit('${escapeHtml(d.id)}')"><i class="fas fa-pen"></i></button>
                    <button class="btn btn-secondary" title="Delete" onclick="deleteUnit('${escapeHtml(d.id)}')"><i class="fas fa-trash" style="color:var(--danger)"></i></button>
                </div>
            </td>
        </tr>`; }).join('');
}

function sortEditTable(key) {
    if (key === 'no') { editSortState.key = null; }
    else if (editSortState.key === key) { editSortState.asc = !editSortState.asc; }
    else { editSortState.key = key; editSortState.asc = true; }
    renderEditTable();
}

function populateEditFilters() {
    const statuses = [...new Set(globalData.map(d => d.status))].filter(Boolean).sort();
    const sites = [...new Set(globalData.map(d => d.site))].filter(Boolean).sort();
    const sf = document.getElementById('editStatusFilter');
    const sif = document.getElementById('editSiteFilter');
    if (sf) {
        const cur = sf.value;
        sf.innerHTML = '<option value="">All Status</option>' + statuses.map(s => `<option value="${escapeHtml(s)}">${escapeHtml(s)}</option>`).join('');
        sf.value = cur;
    }
    if (sif) {
        const cur = sif.value;
        sif.innerHTML = '<option value="">All Sites</option>' + sites.map(s => `<option value="${escapeHtml(s)}">${escapeHtml(s)}</option>`).join('');
        sif.value = cur;
    }
}

// ---- Inline Edit ----
function saveInlineEdit(el) {
    if (!canEdit()) {
        // Revert the DOM if a viewer somehow triggered this
        const id = el.dataset.id;
        const field = el.dataset.field;
        const unit = globalData.find(d => d.id === id);
        if (unit) el.textContent = unit[field] || '';
        return;
    }
    const id = el.dataset.id;
    const field = el.dataset.field;
    const newValue = clean(el.textContent);
    const unit = globalData.find(d => d.id === id);

    if (unit && unit[field] !== newValue) {
        // Intercept status changing TO Breakdown → prompt for reason
        if (field === 'status' && !isGood(newValue) && isGood(unit.status)) {
            _pendingBreakdown = { unitId: id, fields: { status: newValue }, isInline: true, el };
            document.getElementById('breakdownReasonText').value = '';
            document.getElementById('breakdownReasonModal').classList.add('open');
            return;
        }
        updateUnit(id, { [field]: newValue });
        showToast(`${COMPONENT_LABELS[field] || field.charAt(0).toUpperCase() + field.slice(1)} updated`, 'success');
    }
}

// ---- Breakdown reason modal ----
function confirmBreakdownReason() {
    const reason = (document.getElementById('breakdownReasonText').value || '').trim();
    if (!reason) {
        showToast('Please enter a breakdown reason', 'warning');
        document.getElementById('breakdownReasonText').focus();
        return;
    }
    const p = _pendingBreakdown;
    if (!p) return;
    _pendingBreakdown = null;
    document.getElementById('breakdownReasonModal').classList.remove('open');

    p.fields.breakdownReason = reason;
    if (p.isInline) {
        updateUnit(p.unitId, p.fields);
        showToast('Status updated — breakdown reason recorded', 'success');
    } else {
        _commitSaveUnit(p.unitId, p.fields);
    }
}

function cancelBreakdownReason() {
    const p = _pendingBreakdown;
    _pendingBreakdown = null;
    document.getElementById('breakdownReasonModal').classList.remove('open');
    // Revert inline edit cell if it was an inline change
    if (p && p.isInline && p.el) {
        const unit = globalData.find(d => d.id === p.unitId);
        if (unit) p.el.textContent = unit.status || 'Good';
    }
}

// ---- Breakdown popover (dashboard) ----
function showBreakdownPopover(event, reason) {
    event.stopPropagation();
    const pop = document.getElementById('breakdownPopover');
    pop.textContent = reason;
    pop.style.display = 'block';
    const rect = event.currentTarget.getBoundingClientRect();
    pop.style.top = (rect.bottom + window.scrollY + 8) + 'px';
    pop.style.left = (rect.left + window.scrollX + rect.width / 2) + 'px';

    const dismiss = (e) => {
        if (!pop.contains(e.target)) {
            pop.style.display = 'none';
            document.removeEventListener('click', dismiss);
        }
    };
    setTimeout(() => document.addEventListener('click', dismiss), 0);
}

// ---- Select / Delete ----
function toggleSelectAll() {
    const checked = document.getElementById('selectAll').checked;
    document.querySelectorAll('.unit-check').forEach(cb => { cb.checked = checked; });
    updateSelectedCount();
}

function updateSelectedCount() {
    selectedUnitIds.clear();
    document.querySelectorAll('.unit-check:checked').forEach(cb => selectedUnitIds.add(cb.dataset.id));
    const count = selectedUnitIds.size;
    document.getElementById('selectedCount').textContent = count;
    document.getElementById('btnDeleteSelected').style.display = count > 0 ? '' : 'none';
}

function deleteUnit(id) {
    if (!requireEdit()) return;
    const unit = globalData.find(d => d.id === id);
    if (!unit) return;
    const { removed } = deleteUnits([id]);
    renderEditTable();
    showUndoToast(`Unit "${unit.name || unit.sn}" deleted`, removed);
}

function deleteSelected() {
    if (!requireEdit()) return;
    const count = selectedUnitIds.size;
    if (count === 0) return;
    const { removed } = deleteUnits([...selectedUnitIds]);
    renderEditTable();
    showUndoToast(`${count} unit(s) deleted`, removed);
}

// ---- Undo Toast ----
function showUndoToast(message, units) {
    if (!units || units.length === 0) return;
    lastDeletedUnits = units;
    if (undoTimer) clearTimeout(undoTimer);
    document.querySelectorAll('.undo-toast').forEach(t => t.remove());

    const container = document.getElementById('toastContainer');
    const toast = document.createElement('div');
    toast.className = 'toast info undo-toast';
    toast.innerHTML = `<i class="fas fa-trash"></i> <span>${escapeHtml(message)}</span> <button class="toast-undo-btn" onclick="undoDelete()">UNDO</button>`;
    container.appendChild(toast);

    undoTimer = setTimeout(() => {
        toast.style.opacity = '0';
        setTimeout(() => toast.remove(), 300);
        lastDeletedUnits = null;
        undoTimer = null;
    }, 10000);
}

function undoDelete() {
    if (!lastDeletedUnits || lastDeletedUnits.length === 0) return;
    const restored = lastDeletedUnits;
    globalData = [...globalData, ...restored];
    saveToStorage(globalData);
    cloudPushUnits(restored);
    restored.forEach(u => logEvent({ action: 'restore', unitId: u.id, unitName: u.name, after: 'undelete' }));
    recordChange({ type: 'restored', detail: `${restored.length} unit(s) restored` });
    renderEditTable();
    showToast(`Restored ${restored.length} unit(s)`, 'success');
    lastDeletedUnits = null;
    if (undoTimer) { clearTimeout(undoTimer); undoTimer = null; }
    document.querySelectorAll('.undo-toast').forEach(t => t.remove());
}

// ---- Modal: Add / Edit ----
function showAddForm() {
    if (!requireEdit()) return;
    document.getElementById('modalTitle').textContent = 'Add Unit';
    document.getElementById('editUnitId').value = '';
    document.getElementById('unitForm').reset();
    renderUserCategoryOptions();
    document.getElementById('unitModal').classList.add('open');
}

function editUnit(id) {
    if (!requireEdit()) return;
    const unit = globalData.find(d => d.id === id);
    if (!unit) return;

    document.getElementById('modalTitle').textContent = 'Edit Unit';
    document.getElementById('editUnitId').value = id;
    document.getElementById('formName').value = unit.name;
    document.getElementById('formModel').value = unit.model;
    document.getElementById('formSN').value = unit.sn;
    document.getElementById('formSite').value = unit.site;
    document.getElementById('formStatus').value = isGood(unit.status) ? 'Good' : 'Breakdown';
    document.getElementById('formDisplay').value = isGood(unit.display) ? 'Good' : 'Breakdown';
    document.getElementById('formGPS').value = isGood(unit.gps) ? 'Good' : 'Breakdown';
    document.getElementById('formSteering').value = isGood(unit.steering) ? 'Good' : 'Breakdown';
    document.getElementById('formJDLink').value = isGood(unit.jdlink) ? 'Good' : 'Breakdown';

    // License & notes
    renderUserCategoryOptions();
    document.getElementById('formUserCategory').value   = unit.userCategory || '';
    document.getElementById('formGpsLicense').value     = unit.gpsLicense || '';
    document.getElementById('formLicenseDisplay').value = unit.licenseDisplay || '';
    // New dual-license date pairs; fall back to legacy licenseStartDate /
    // licenseEndDate (which were GPS-license dates historically) if the new
    // GPS-specific fields are empty.
    document.getElementById('formGpsLicenseStart').value =
        unit.gpsLicenseStartDate || unit.licenseStartDate || '';
    document.getElementById('formGpsLicenseEnd').value =
        unit.gpsLicenseEndDate || unit.licenseEndDate || '';
    document.getElementById('formDisplayLicenseStart').value = unit.displayLicenseStartDate || '';
    document.getElementById('formDisplayLicenseEnd').value   = unit.displayLicenseEndDate || '';
    document.getElementById('formRemarks').value        = unit.remarks || '';

    // Show breakdown reason if this unit is currently in Breakdown
    const bdBox = document.getElementById('breakdownReasonDisplay');
    const bdInfo = document.getElementById('breakdownReasonInfo');
    if (!isGood(unit.status) && unit.breakdownReason) {
        bdInfo.textContent = unit.breakdownReason;
        bdBox.style.display = '';
    } else {
        bdInfo.textContent = '';
        bdBox.style.display = 'none';
    }

    document.getElementById('unitModal').classList.add('open');
}

function saveUnit(event) {
    event.preventDefault();
    if (!requireEdit()) return;

    const id = document.getElementById('editUnitId').value;
    const fields = {
        name: document.getElementById('formName').value.trim(),
        model: document.getElementById('formModel').value.trim(),
        sn: document.getElementById('formSN').value.trim(),
        site: document.getElementById('formSite').value.trim(),
        status: document.getElementById('formStatus').value,
        display: document.getElementById('formDisplay').value,
        gps: document.getElementById('formGPS').value,
        steering: document.getElementById('formSteering').value,
        jdlink: document.getElementById('formJDLink').value,
        userCategory: document.getElementById('formUserCategory').value,
        gpsLicense: document.getElementById('formGpsLicense').value,
        licenseDisplay: document.getElementById('formLicenseDisplay').value,
        gpsLicenseStartDate: document.getElementById('formGpsLicenseStart').value || '',
        gpsLicenseEndDate:   document.getElementById('formGpsLicenseEnd').value   || '',
        displayLicenseStartDate: document.getElementById('formDisplayLicenseStart').value || '',
        displayLicenseEndDate:   document.getElementById('formDisplayLicenseEnd').value   || '',
        remarks: document.getElementById('formRemarks').value.trim()
    };

    // If status is changing TO Breakdown, prompt for a reason first.
    if (!isGood(fields.status)) {
        const existingUnit = id ? globalData.find(d => d.id === id) : null;
        const wasGood = existingUnit ? isGood(existingUnit.status) : true;
        if (wasGood) {
            _pendingBreakdown = { unitId: id, fields, isInline: false };
            document.getElementById('breakdownReasonText').value = '';
            document.getElementById('breakdownReasonModal').classList.add('open');
            return;
        }
    }

    _commitSaveUnit(id, fields);
}

function _commitSaveUnit(id, fields) {
    if (id) {
        updateUnit(id, fields);
        showToast(`Unit "${fields.name}" updated`, 'success');
    } else {
        const newUnit = { id: generateId(), ...fields, downtimeHistory: [], breakdownStartedAt: null };
        const { added } = addUnits([newUnit]);
        if (added > 0) {
            showToast(`Unit "${fields.name}" added`, 'success');
        } else {
            showToast(`Duplicate serial number "${fields.sn}" — unit not added`, 'warning');
        }
    }

    closeModal();
    renderEditTable();
}

// Auto-fill expiration to start + 1 year (still editable). One helper per
// license kind so the onchange on each start-date input targets the right
// expiration field.
function autoFillGpsLicenseEnd()     { _autoFillEnd('formGpsLicenseStart',     'formGpsLicenseEnd'); }
function autoFillDisplayLicenseEnd() { _autoFillEnd('formDisplayLicenseStart', 'formDisplayLicenseEnd'); }
function _autoFillEnd(startId, endId) {
    const startEl = document.getElementById(startId);
    const endEl = document.getElementById(endId);
    if (!startEl || !endEl) return;
    if (!startEl.value) return;
    if (endEl.value) return;
    const d = new Date(startEl.value);
    if (isNaN(d.getTime())) return;
    d.setFullYear(d.getFullYear() + 1);
    endEl.value = d.toISOString().slice(0, 10);
}

// Compute expiry status for a single end-date string.
// Returns one of: { kind: 'none'|'expired'|'soon'|'ok', label, daysLeft }
function getExpiryStatus(endDate) {
    if (!endDate) return { kind: 'none', label: '—' };
    const end = new Date(endDate);
    if (isNaN(end.getTime())) return { kind: 'none', label: '—' };
    const today = new Date();
    today.setHours(0, 0, 0, 0);
    end.setHours(0, 0, 0, 0);
    const days = Math.round((end - today) / 86400000);
    if (days < 0)  return { kind: 'expired', label: `Expired ${-days}d ago`, daysLeft: days };
    if (days <= 30) return { kind: 'soon',    label: `${days}d left`,        daysLeft: days };
    return { kind: 'ok', label: `${days}d left`, daysLeft: days };
}

// Pick the effective end date for a license kind. Falls back to the legacy
// `licenseEndDate` for `gps` only, since historically that single field
// stored the GPS-license expiry. Display kind has no legacy fallback.
function getLicenseEndDate(unit, kind) {
    if (!unit) return '';
    if (kind === 'display') return unit.displayLicenseEndDate || '';
    return unit.gpsLicenseEndDate || unit.licenseEndDate || '';
}

// Legacy helper kept for any old callers — returns status for whichever
// expiry is soonest (across GPS + Display + legacy).
function getLicenseStatus(unit) {
    const dates = [
        getLicenseEndDate(unit, 'gps'),
        getLicenseEndDate(unit, 'display')
    ].filter(Boolean);
    if (!dates.length) return { kind: 'none', label: '—' };
    let worst = null;
    dates.forEach(d => {
        const s = getExpiryStatus(d);
        if (s.kind === 'none') return;
        if (!worst || (s.daysLeft ?? 0) < (worst.daysLeft ?? 0)) worst = s;
    });
    return worst || { kind: 'none', label: '—' };
}

// Render an expiry badge for either 'gps' or 'display' license.
function licenseBadgeFor(unit, kind) {
    const end = getLicenseEndDate(unit, kind);
    const s = getExpiryStatus(end);
    if (s.kind === 'none') return '<span style="color:#a0aec0;font-size:11px">—</span>';
    const cls = `license-badge license-badge--${s.kind}`;
    const icon = s.kind === 'expired' ? 'circle-xmark'
               : s.kind === 'soon'    ? 'triangle-exclamation'
               : 'circle-check';
    const labelName = kind === 'display' ? (unit.licenseDisplay || 'Display') : (unit.gpsLicense || 'GPS');
    const tt = `${labelName} · Expires: ${end}`;
    return `<span class="${cls}" title="${escapeHtml(tt)}"><i class="fas fa-${icon}"></i> ${escapeHtml(s.label)}</span>`;
}

// Back-compat shim — callers that used the single-badge version now get the
// earliest-of-both rendered with generic tooltip.
function licenseBadge(unit) {
    const s = getLicenseStatus(unit);
    if (s.kind === 'none') return '<span style="color:#a0aec0;font-size:11px">—</span>';
    const cls = `license-badge license-badge--${s.kind}`;
    const icon = s.kind === 'expired' ? 'circle-xmark'
               : s.kind === 'soon'    ? 'triangle-exclamation'
               : 'circle-check';
    return `<span class="${cls}"><i class="fas fa-${icon}"></i> ${escapeHtml(s.label)}</span>`;
}

function closeModal() {
    document.getElementById('unitModal').classList.remove('open');
}

// ============================================================
// IMPLEMENTS — CRUD, storage, render, modal
// ============================================================

const IMPLEMENT_FIELDS = [
    { key: 'profileName',        inputId: 'implProfileName',        label: 'Profile Name' },
    { key: 'equipmentType',      inputId: 'implEquipmentType',      label: 'Type of Equipment' },
    { key: 'lateralOffset',      inputId: 'implLateralOffset',      label: 'Lateral Offset' },
    { key: 'centerOfRotation',   inputId: 'implCenterOfRotation',   label: 'Center of Rotation' },
    { key: 'rearConnection',     inputId: 'implRearConnection',     label: 'Rear Connection' },
    { key: 'operation',          inputId: 'implOperation',          label: 'Operation' },
    { key: 'workingWidth',       inputId: 'implWorkingWidth',       label: 'Working Width' },
    { key: 'workPoint',          inputId: 'implWorkPoint',          label: 'Work Point' },
    { key: 'workRecording',      inputId: 'implWorkRecording',      label: 'Work Recording' },
    { key: 'connectingType',     inputId: 'implConnectingType',     label: 'Connecting Type' },
    { key: 'implementReceiver',  inputId: 'implImplementReceiver',  label: 'Implement Receiver' }
];

function generateImplementId() {
    return 'imp_' + Date.now() + '_' + Math.random().toString(36).substr(2, 5);
}

// ---- Chart of Account dynamic list ----
function renderChartOfAccountsInputs(values) {
    const container = document.getElementById('implChartOfAccountsList');
    if (!container) return;
    const list = (Array.isArray(values) && values.length) ? values : [''];
    container.innerHTML = list.map(v => `
        <div class="coa-row">
            <input type="text" class="form-input coa-input" value="${escapeHtml(v)}" placeholder="e.g. 5100-001 Spare Parts">
            <button type="button" class="btn btn-secondary coa-remove" onclick="removeChartOfAccountRow(this)" title="Remove">
                <i class="fas fa-xmark"></i>
            </button>
        </div>
    `).join('');
}

function addChartOfAccountRow() {
    const container = document.getElementById('implChartOfAccountsList');
    if (!container) return;
    const row = document.createElement('div');
    row.className = 'coa-row';
    row.innerHTML = `
        <input type="text" class="form-input coa-input" placeholder="e.g. 5100-001 Spare Parts">
        <button type="button" class="btn btn-secondary coa-remove" onclick="removeChartOfAccountRow(this)" title="Remove">
            <i class="fas fa-xmark"></i>
        </button>
    `;
    container.appendChild(row);
    row.querySelector('input').focus();
}

function removeChartOfAccountRow(btn) {
    const row = btn.closest('.coa-row');
    if (row) row.remove();
    // Always keep at least one empty row so the UI never looks empty.
    const container = document.getElementById('implChartOfAccountsList');
    if (container && container.children.length === 0) {
        renderChartOfAccountsInputs(['']);
    }
}

function collectChartOfAccounts() {
    return Array.from(document.querySelectorAll('#implChartOfAccountsList .coa-input'))
        .map(i => i.value.trim())
        .filter(Boolean);
}

// ---- Storage ----
function loadImplements() {
    try {
        const raw = localStorage.getItem(IMPLEMENTS_STORAGE_KEY);
        globalImplements = raw ? JSON.parse(raw) : [];
    } catch (e) {
        globalImplements = [];
    }
    updateImplementCount();
    return globalImplements.length > 0;
}

function saveImplements() {
    try {
        localStorage.setItem(IMPLEMENTS_STORAGE_KEY, JSON.stringify(globalImplements));
    } catch (e) {
        showToast('Storage full. Could not save implements.', 'error');
    }
}

function updateImplementCount() {
    const el = document.getElementById('implementCount');
    if (el) el.textContent = `${globalImplements.length} implement(s) in database`;
}

// ---- Render ----
function renderImplementsTable() {
    updateImplementCount();
    selectedImplementIds.clear();
    updateSelectedImplementCount();

    const selectAllBox = document.getElementById('selectAllImpl');
    if (selectAllBox) selectAllBox.checked = false;

    const query = (document.getElementById('implementSearch')?.value || '').toLowerCase().trim();
    const rows = query
        ? globalImplements.filter(d =>
            `${d.profileName} ${d.equipmentType} ${d.operation} ${d.connectingType} ${d.workingWidth}`
                .toLowerCase().includes(query))
        : globalImplements;

    const tbody = document.getElementById('implementBody');
    if (!tbody) return;

    if (rows.length === 0) {
        tbody.innerHTML = `<tr><td colspan="9" style="text-align:center;padding:24px;color:#718096">${
            query ? 'No implements match your search'
                  : 'No implements yet. Click <strong>Add Implement</strong> to get started.'
        }</td></tr>`;
        return;
    }

    tbody.innerHTML = rows.map((d, i) => {
        const coaList = Array.isArray(d.chartOfAccounts) ? d.chartOfAccounts.filter(Boolean) : [];
        const coaCell = coaList.length
            ? coaList.map(c => `<span class="badge badge-cat" style="font-size:10px;margin:1px">${escapeHtml(c)}</span>`).join(' ')
            : '<span style="color:#a0aec0;font-size:11px">—</span>';
        return `
        <tr>
            <td class="col-check"><input type="checkbox" class="impl-check" data-id="${escapeHtml(d.id)}" onchange="updateSelectedImplementCount()"></td>
            <td>${i + 1}</td>
            <td>${escapeHtml(d.profileName)}</td>
            <td>${escapeHtml(d.equipmentType)}</td>
            <td>${escapeHtml(d.workingWidth)}</td>
            <td>${escapeHtml(d.operation)}</td>
            <td>${escapeHtml(d.connectingType)}</td>
            <td style="max-width:220px">${coaCell}</td>
            <td class="col-actions">
                <div class="row-actions">
                    <button class="btn btn-secondary" title="Edit" onclick="editImplement('${escapeHtml(d.id)}')"><i class="fas fa-pen"></i></button>
                    <button class="btn btn-secondary" title="Delete" onclick="deleteImplement('${escapeHtml(d.id)}')"><i class="fas fa-trash" style="color:var(--danger)"></i></button>
                </div>
            </td>
        </tr>`;
    }).join('');
}

// ---- Selection ----
function toggleSelectAllImplements() {
    const checked = document.getElementById('selectAllImpl').checked;
    document.querySelectorAll('.impl-check').forEach(cb => { cb.checked = checked; });
    updateSelectedImplementCount();
}

function updateSelectedImplementCount() {
    selectedImplementIds.clear();
    document.querySelectorAll('.impl-check:checked').forEach(cb => selectedImplementIds.add(cb.dataset.id));
    const count = selectedImplementIds.size;
    const countEl = document.getElementById('selectedImplCount');
    const btn = document.getElementById('btnDeleteSelectedImpl');
    if (countEl) countEl.textContent = count;
    if (btn) btn.style.display = count > 0 ? '' : 'none';
}

// ---- Modal: Add / Edit ----
function showAddImplementForm() {
    if (!requireEdit()) return;
    document.getElementById('implementModalTitle').textContent = 'Add Implement';
    document.getElementById('editImplementId').value = '';
    document.getElementById('implementForm').reset();
    renderChartOfAccountsInputs(['']);
    document.getElementById('implementModal').classList.add('open');
}

function editImplement(id) {
    if (!requireEdit()) return;
    const imp = globalImplements.find(d => d.id === id);
    if (!imp) return;

    document.getElementById('implementModalTitle').textContent = 'Edit Implement';
    document.getElementById('editImplementId').value = id;
    IMPLEMENT_FIELDS.forEach(f => {
        const el = document.getElementById(f.inputId);
        if (el) el.value = imp[f.key] || '';
    });
    renderChartOfAccountsInputs(imp.chartOfAccounts || ['']);
    document.getElementById('implementModal').classList.add('open');
}

function saveImplement(event) {
    event.preventDefault();

    const id = document.getElementById('editImplementId').value;
    const data = {};
    IMPLEMENT_FIELDS.forEach(f => {
        const el = document.getElementById(f.inputId);
        data[f.key] = el ? el.value.trim() : '';
    });
    data.chartOfAccounts = collectChartOfAccounts();

    if (id) {
        // Update existing
        const idx = globalImplements.findIndex(d => d.id === id);
        if (idx !== -1) {
            const before = { ...globalImplements[idx] };
            globalImplements[idx] = { ...before, ...data, updatedAt: Date.now() };
            saveImplements();
            cloudPushImplement(globalImplements[idx]);
            // Audit log per changed field
            IMPLEMENT_FIELDS.forEach(f => {
                if (before[f.key] !== data[f.key]) {
                    logEvent({
                        action: 'update',
                        unitId: id,
                        unitName: `[Implement] ${data.profileName}`,
                        field: f.label,
                        before: before[f.key],
                        after: data[f.key]
                    });
                }
            });
            const beforeCoa = JSON.stringify(before.chartOfAccounts || []);
            const afterCoa = JSON.stringify(data.chartOfAccounts);
            if (beforeCoa !== afterCoa) {
                logEvent({
                    action: 'update',
                    unitId: id,
                    unitName: `[Implement] ${data.profileName}`,
                    field: 'Chart of Account',
                    before: (before.chartOfAccounts || []).join(', '),
                    after: data.chartOfAccounts.join(', ')
                });
            }
            showToast(`Implement "${data.profileName}" updated`, 'success');
        }
    } else {
        // Create new
        const newImp = {
            id: generateImplementId(),
            ...data,
            createdAt: Date.now(),
            updatedAt: Date.now()
        };
        globalImplements.push(newImp);
        saveImplements();
        cloudPushImplement(newImp);
        logEvent({
            action: 'add',
            unitId: newImp.id,
            unitName: `[Implement] ${newImp.profileName}`,
            after: newImp.equipmentType || newImp.profileName
        });
        showToast(`Implement "${newImp.profileName}" added`, 'success');
    }

    closeImplementModal();
    renderImplementsTable();
}

function closeImplementModal() {
    document.getElementById('implementModal').classList.remove('open');
}

// ---- Delete ----
function deleteImplement(id) {
    if (!requireEdit()) return;
    const imp = globalImplements.find(d => d.id === id);
    if (!imp) return;
    if (!confirm(`Delete implement "${imp.profileName}"?`)) return;

    globalImplements = globalImplements.filter(d => d.id !== id);
    saveImplements();
    cloudDeleteImplement(id);
    logEvent({
        action: 'delete',
        unitId: imp.id,
        unitName: `[Implement] ${imp.profileName}`,
        before: imp.equipmentType || imp.profileName
    });
    renderImplementsTable();
    showToast(`Implement "${imp.profileName}" deleted`, 'success');
}

function deleteSelectedImplements() {
    if (!requireEdit()) return;
    const count = selectedImplementIds.size;
    if (count === 0) return;
    if (!confirm(`Delete ${count} selected implement(s)?`)) return;

    const idSet = new Set(selectedImplementIds);
    const removed = globalImplements.filter(d => idSet.has(d.id));
    globalImplements = globalImplements.filter(d => !idSet.has(d.id));
    saveImplements();
    removed.forEach(imp => cloudDeleteImplement(imp.id));
    removed.forEach(imp => logEvent({
        action: 'delete',
        unitId: imp.id,
        unitName: `[Implement] ${imp.profileName}`,
        before: imp.equipmentType || imp.profileName
    }));
    renderImplementsTable();
    showToast(`${count} implement(s) deleted`, 'success');
}

// ============================================================
// CLOUD SYNC (Firestore via window.cloud from firebase-init.js)
// ============================================================

function cloudPushUnits(units) {
    if (suppressCloudWrites || !window.cloud?.isReady || !units?.length) return;
    window.cloud.saveUnits(units).catch(err => {
        console.error('[cloud] push units failed:', err);
        showToast('Cloud sync failed — changes saved locally', 'warning');
    });
}

function cloudDeleteUnits(ids) {
    if (suppressCloudWrites || !window.cloud?.isReady || !ids?.length) return;
    window.cloud.deleteUnits(ids).catch(err => {
        console.error('[cloud] delete units failed:', err);
        showToast('Cloud delete failed — changes saved locally', 'warning');
    });
}

function cloudPushImplement(imp) {
    if (suppressCloudWrites || !window.cloud?.isReady || !imp) return;
    window.cloud.saveImplement(imp).catch(err => {
        console.error('[cloud] push implement failed:', err);
        showToast('Cloud sync failed — changes saved locally', 'warning');
    });
}

function cloudDeleteImplement(id) {
    if (suppressCloudWrites || !window.cloud?.isReady || !id) return;
    window.cloud.deleteImplement(id).catch(err => {
        console.error('[cloud] delete implement failed:', err);
    });
}

async function migrateLocalToCloudIfNeeded() {
    try {
        // Units
        const cloudUnits = await window.cloud.getAllUnits();
        console.log(`[cloud] check: cloud has ${cloudUnits.length} units, local has ${globalData.length} units`);
        if (cloudUnits.length === 0 && globalData.length > 0) {
            console.log(`[cloud] migrating ${globalData.length} local units to Firestore...`);
            await window.cloud.saveUnits(globalData);
            showToast(`Uploaded ${globalData.length} units to cloud`, 'success');
        }
        // Implements
        const cloudImpls = await window.cloud.getAllImplements();
        console.log(`[cloud] check: cloud has ${cloudImpls.length} implements, local has ${globalImplements.length} implements`);
        if (cloudImpls.length === 0 && globalImplements.length > 0) {
            console.log(`[cloud] migrating ${globalImplements.length} local implements to Firestore...`);
            await window.cloud.saveImplements(globalImplements);
            showToast(`Uploaded ${globalImplements.length} implements to cloud`, 'success');
        }
    } catch (e) {
        console.error('[cloud] migration failed:', e);
        showToast('Cloud migration failed — check console', 'error');
    }
}

function applyCloudUnitsSnapshot(units) {
    console.log(`[cloud] units snapshot received — ${units.length} docs`);

    // First-snapshot guard: if cloud is empty but we have local data, do NOT
    // wipe — migration may still be in-flight, or this client beat the rest of
    // the team to upload. Re-push our local data and bail out for this round.
    if (_firstUnitsSnapshot && units.length === 0 && globalData.length > 0) {
        console.warn(`[cloud] first units snapshot is empty but local has ${globalData.length} — keeping local, re-uploading`);
        _firstUnitsSnapshot = false;
        window.cloud.saveUnits(globalData).catch(err => {
            console.error('[cloud] re-upload after empty snapshot failed:', err);
        });
        return;
    }
    _firstUnitsSnapshot = false;

    suppressCloudWrites = true;
    try {
        globalData = units;
        // Persist to local cache so offline / next visit sees latest snapshot
        try { localStorage.setItem(STORAGE_KEY, JSON.stringify(units)); } catch (e) {}

        // Update connection indicator
        const dot = document.getElementById('connectionDot');
        const lbl = document.getElementById('connectionLabel');
        if (dot) dot.classList.add('connected');
        if (lbl) lbl.textContent = `Cloud · ${units.length} units`;

        // Re-render whichever view is visible
        if (currentView === 'dashboard') {
            const empty = document.getElementById('emptyState');
            const content = document.getElementById('dashboardContent');
            if (units.length > 0) {
                if (empty) empty.style.display = 'none';
                if (content) content.style.display = 'block';
                filteredData = [...units];
                onDataLoaded();
            } else {
                if (empty) empty.style.display = '';
                if (content) content.style.display = 'none';
            }
        } else if (currentView === 'editUnits') {
            renderEditTable();
        }
    } finally {
        suppressCloudWrites = false;
    }

    // One-shot license defaults fill — runs only for owner on first load
    // that has units, gated by a localStorage flag so it never repeats.
    applyDefaultLicensesIfNeeded();
    applyLicenseDatesIfNeeded();
}

function applyCloudImplementsSnapshot(items) {
    console.log(`[cloud] implements snapshot received — ${items.length} docs`);

    // First-snapshot guard: same idea as units — don't wipe local data on the
    // very first empty snapshot; re-upload instead.
    if (_firstImplSnapshot && items.length === 0 && globalImplements.length > 0) {
        console.warn(`[cloud] first implements snapshot is empty but local has ${globalImplements.length} — keeping local, re-uploading`);
        _firstImplSnapshot = false;
        window.cloud.saveImplements(globalImplements).catch(err => {
            console.error('[cloud] re-upload implements after empty snapshot failed:', err);
        });
        return;
    }
    _firstImplSnapshot = false;

    suppressCloudWrites = true;
    try {
        globalImplements = items;
        try { localStorage.setItem(IMPLEMENTS_STORAGE_KEY, JSON.stringify(items)); } catch (e) {}
        if (currentView === 'implements') {
            renderImplementsTable();
        } else {
            updateImplementCount();
        }
    } finally {
        suppressCloudWrites = false;
    }
}

// One-shot migration: seed every unit that has blank license fields with
// sensible defaults (GPS License = SF-RTK, Display License = G5 Basic).
// Only the owner runs it, and the localStorage flag guarantees it never
// re-runs after the initial fill. Manually-set values are preserved.
function applyDefaultLicensesIfNeeded() {
    if (!isOwner || !isOwner()) return;
    if (localStorage.getItem(LICENSE_DEFAULTS_KEY) === '1') return;
    if (!Array.isArray(globalData) || globalData.length === 0) return;

    const updates = [];
    globalData.forEach(unit => {
        const patch = {};
        if (!unit.gpsLicense)     patch.gpsLicense = 'SF-RTK';
        if (!unit.licenseDisplay) patch.licenseDisplay = 'G5 Basic';
        if (Object.keys(patch).length > 0) {
            Object.assign(unit, patch);
            updates.push(unit);
        }
    });

    if (updates.length === 0) {
        localStorage.setItem(LICENSE_DEFAULTS_KEY, '1');
        return;
    }

    console.log(`[license-defaults] seeding defaults on ${updates.length} units...`);
    try { saveToStorage(globalData); } catch (e) {}
    window.cloud.saveUnits(updates).then(() => {
        localStorage.setItem(LICENSE_DEFAULTS_KEY, '1');
        try {
            logEvent({
                action: 'migrate',
                unitName: '-',
                field: 'license defaults',
                after: `GPS=SF-RTK + Display=G5 Basic on ${updates.length} units`
            });
        } catch (e) {}
        showToast(`Applied default licenses to ${updates.length} units`, 'success');
        if (currentView === 'dashboard') updateDashboard(filteredData);
        if (currentView === 'editUnits') renderEditTable();
    }).catch(err => {
        console.error('[license-defaults] bulk save failed:', err);
        showToast('License defaults migration failed — check console', 'error');
    });
}

// One-shot migration: import license start dates from LICENSE_DATES_MAP
// (serial number → YYYY-MM-DD). Expiration is auto-set to +1 year. Only
// patches units with no existing licenseStartDate; manual values are kept.
// Owner-only, guarded by a localStorage flag so it never repeats.
function applyLicenseDatesIfNeeded() {
    if (!isOwner || !isOwner()) return;
    if (localStorage.getItem(LICENSE_DATES_KEY) === '1') return;
    if (!Array.isArray(globalData) || globalData.length === 0) return;

    const addOneYear = (isoDate) => {
        const d = new Date(isoDate);
        if (isNaN(d.getTime())) return '';
        d.setFullYear(d.getFullYear() + 1);
        return d.toISOString().slice(0, 10);
    };

    // Normalize serial numbers so OCR-style confusables match:
    // uppercase, strip whitespace, and collapse I↔1 and O↔0.
    const normSn = (s) => (s || '')
        .toString()
        .trim()
        .toUpperCase()
        .replace(/\s+/g, '')
        .replace(/I/g, '1')
        .replace(/O/g, '0');

    // Build a normalized lookup table once, keeping a reverse index so we
    // can report which map keys went unmatched.
    const normalizedMap = {};
    Object.keys(LICENSE_DATES_MAP).forEach(rawKey => {
        normalizedMap[normSn(rawKey)] = { start: LICENSE_DATES_MAP[rawKey], rawKey };
    });

    const updates = [];
    const unmatched = [];
    const matchedKeys = new Set();

    globalData.forEach(unit => {
        const key = normSn(unit.sn);
        if (!key) return;
        const hit = normalizedMap[key];
        if (!hit) return;
        matchedKeys.add(hit.rawKey);
        // Preserve any existing license dates the owner entered manually
        // (either in the new GPS pair or the legacy single pair).
        if (unit.gpsLicenseStartDate || unit.gpsLicenseEndDate
            || unit.licenseStartDate || unit.licenseEndDate) return;
        const start = hit.start;
        const end = addOneYear(start);
        unit.gpsLicenseStartDate = start;
        unit.gpsLicenseEndDate = end;
        updates.push(unit);
    });

    Object.keys(LICENSE_DATES_MAP).forEach(rawKey => {
        if (!matchedKeys.has(rawKey)) unmatched.push(rawKey);
    });
    if (unmatched.length) {
        console.warn(`[license-dates] ${unmatched.length} serial numbers in the map were not found in cloud data:`, unmatched);
    }

    if (updates.length === 0) {
        localStorage.setItem(LICENSE_DATES_KEY, '1');
        return;
    }

    console.log(`[license-dates] applying start/end dates to ${updates.length} units...`);
    try { saveToStorage(globalData); } catch (e) {}
    window.cloud.saveUnits(updates).then(() => {
        localStorage.setItem(LICENSE_DATES_KEY, '1');
        try {
            logEvent({
                action: 'migrate',
                unitName: '-',
                field: 'license dates',
                after: `Imported start+expiry dates on ${updates.length} units`
            });
        } catch (e) {}
        showToast(`Imported license dates for ${updates.length} units`, 'success');
        if (currentView === 'dashboard') updateDashboard(filteredData);
        if (currentView === 'editUnits') renderEditTable();
    }).catch(err => {
        console.error('[license-dates] bulk save failed:', err);
        showToast('License dates import failed — check console', 'error');
    });
}

// ============================================================
// USER CATEGORIES (dynamic dropdown source)
// ============================================================

function applyCloudUserCategoriesSnapshot(cats) {
    // Sort alphabetically for a stable UI
    userCategories = (cats || []).slice().sort((a, b) =>
        (a.name || '').localeCompare(b.name || '')
    );
    // First-snapshot seed: if an owner lands on an empty collection, populate
    // the three defaults so the dropdown is never blank.
    if (_firstUserCategoriesSnapshot) {
        _firstUserCategoriesSnapshot = false;
        if (userCategories.length === 0) {
            seedDefaultUserCategoriesIfOwner();
        }
    }
    renderUserCategoryOptions();
    // Re-render the management modal if it's open
    const mgr = document.getElementById('categoriesModal');
    if (mgr && mgr.classList.contains('open')) renderCategoriesList();
}

function seedDefaultUserCategoriesIfOwner() {
    if (!isOwner || !isOwner()) return;
    if (localStorage.getItem(USER_CATEGORIES_SEED_KEY) === '1') return;
    const now = Date.now();
    const defaults = DEFAULT_USER_CATEGORIES.map((name, idx) => ({
        id: `cat_${now}_${idx}`,
        name,
        createdAt: now
    }));
    console.log('[user-categories] seeding 3 default categories...');
    window.cloud.saveUserCategories(defaults).then(() => {
        localStorage.setItem(USER_CATEGORIES_SEED_KEY, '1');
        showToast('Seeded default user categories', 'success');
    }).catch(err => {
        console.error('[user-categories] seed failed:', err);
        if (err && err.code === 'permission-denied') {
            showCategoryRulesBanner();
        }
    });
}

// Surfaces a clear, actionable banner inside the Manage Categories modal
// when Firestore rejects writes to userCategories. The most common cause
// is that the owner hasn't added rules for the new collection yet.
function showCategoryRulesBanner() {
    const modal = document.getElementById('categoriesModal');
    if (!modal) return;
    // Only inject once per open
    if (modal.querySelector('.category-rules-banner')) return;
    const body = modal.querySelector('.modal-body');
    if (!body) return;
    const banner = document.createElement('div');
    banner.className = 'category-rules-banner';
    banner.innerHTML = `
        <strong><i class="fas fa-triangle-exclamation"></i> Firestore rules are blocking this write.</strong>
        <p>Your project's security rules don't allow anyone to write to the <code>userCategories</code> collection yet.
        Paste the block below into <em>Firebase Console → Firestore → Rules</em>, then try again:</p>
        <pre>match /userCategories/{catId} {
  allow read:  if request.auth != null
               &amp;&amp; get(/databases/$(database)/documents/users/$(request.auth.uid)).data.status == 'active';
  allow write: if request.auth != null
               &amp;&amp; get(/databases/$(database)/documents/users/$(request.auth.uid)).data.role in ['owner', 'team']
               &amp;&amp; get(/databases/$(database)/documents/users/$(request.auth.uid)).data.status == 'active';
}</pre>
    `;
    body.insertBefore(banner, body.firstChild);
}

// Surfaces the exact Firestore rules that need to be pasted into the Firebase
// Console when the `history` collection rejects reads or writes. Without this
// the team-visibility failure is invisible to the user — they just see an empty
// History modal with no clue why.
function showHistoryRulesBanner() {
    const modal = document.getElementById('historyModal');
    if (!modal) return;
    const slot = modal.querySelector('.history-rules-slot');
    if (!slot || slot.querySelector('.category-rules-banner')) return;
    const banner = document.createElement('div');
    banner.className = 'category-rules-banner';
    banner.innerHTML = `
        <strong><i class="fas fa-triangle-exclamation"></i> Firestore rules are blocking change history.</strong>
        <p>Your project's security rules don't allow this account to read or write the shared <code>history</code> collection yet — that's why you can't see edits from other team members. Paste the block below into <em>Firebase Console → Firestore → Rules</em>, then hard-refresh:</p>
        <pre>match /history/{eventId} {
  allow read:   if request.auth != null
                &amp;&amp; get(/databases/$(database)/documents/users/$(request.auth.uid)).data.status == 'active';
  allow create: if request.auth != null
                &amp;&amp; get(/databases/$(database)/documents/users/$(request.auth.uid)).data.role in ['owner', 'team']
                &amp;&amp; request.resource.data.actorUid == request.auth.uid;
  allow delete: if request.auth != null
                &amp;&amp; get(/databases/$(database)/documents/users/$(request.auth.uid)).data.role == 'owner';
  allow update: if false;
}</pre>
    `;
    slot.appendChild(banner);
}

function renderUserCategoryOptions() {
    const select = document.getElementById('formUserCategory');
    if (!select) return;
    const current = select.value;
    const opts = ['<option value="">Select category…</option>'];
    userCategories.forEach(c => {
        opts.push(`<option value="${escapeHtml(c.name)}">${escapeHtml(c.name)}</option>`);
    });
    select.innerHTML = opts.join('');
    // Preserve whatever the user had selected across live updates
    if (current) select.value = current;
}

function openCategoriesModal() {
    if (!requireEdit()) return;
    renderCategoriesList();
    document.getElementById('categoriesModal').classList.add('open');
    setTimeout(() => {
        const input = document.getElementById('newCategoryName');
        if (input) input.focus();
    }, 50);
}

function closeCategoriesModal() {
    document.getElementById('categoriesModal').classList.remove('open');
}

function renderCategoriesList() {
    const list = document.getElementById('categoriesList');
    if (!list) return;
    if (userCategories.length === 0) {
        list.innerHTML = '<li class="category-empty">No categories yet — add one below.</li>';
        return;
    }
    list.innerHTML = userCategories.map(c => `
        <li class="category-item">
            <span class="category-item__name">${escapeHtml(c.name)}</span>
            <button class="btn-icon category-item__del" title="Delete category" onclick="deleteCategory('${escapeHtml(c.id)}')">
                <i class="fas fa-trash" style="color:var(--danger)"></i>
            </button>
        </li>
    `).join('');
}

function addCategory(event) {
    if (event) event.preventDefault();
    if (!requireEdit()) return;
    const input = document.getElementById('newCategoryName');
    const name = (input.value || '').trim();
    if (!name) {
        showToast('Enter a category name', 'warning');
        return;
    }
    // Prevent duplicates (case-insensitive)
    const exists = userCategories.some(c => (c.name || '').toLowerCase() === name.toLowerCase());
    if (exists) {
        showToast(`Category "${name}" already exists`, 'warning');
        return;
    }
    const cat = {
        id: `cat_${Date.now()}_${Math.random().toString(36).slice(2, 6)}`,
        name,
        createdAt: Date.now()
    };
    window.cloud.saveUserCategory(cat).then(() => {
        input.value = '';
        showToast(`Category "${name}" added`, 'success');
        logEvent({ action: 'add', unitName: '-', field: 'user category', after: name });
    }).catch(err => {
        console.error('[user-categories] save failed:', err);
        const code = (err && err.code) || 'unknown';
        if (code === 'permission-denied') {
            showToast('Firestore rules block writes to userCategories — see banner', 'error');
            showCategoryRulesBanner();
        } else {
            showToast(`Failed to save category (${code})`, 'error');
        }
    });
}

function deleteCategory(id) {
    if (!requireEdit()) return;
    const cat = userCategories.find(c => c.id === id);
    if (!cat) return;
    // Warn if this category is in use by any unit
    const inUse = globalData.filter(u => u.userCategory === cat.name).length;
    const prompt = inUse > 0
        ? `Delete category "${cat.name}"?\n${inUse} unit(s) still reference it — their value will be cleared.`
        : `Delete category "${cat.name}"?`;
    if (!confirm(prompt)) return;
    window.cloud.deleteUserCategory(id).then(() => {
        showToast(`Category "${cat.name}" deleted`, 'success');
        logEvent({ action: 'delete', unitName: '-', field: 'user category', before: cat.name });
    }).catch(err => {
        console.error('[user-categories] delete failed:', err);
        const code = (err && err.code) || 'unknown';
        if (code === 'permission-denied') {
            showToast('Firestore rules block writes to userCategories — see banner', 'error');
            showCategoryRulesBanner();
        } else {
            showToast(`Failed to delete category (${code})`, 'error');
        }
    });
}

function initCloudSync() {
    if (cloudInitialized) return;
    if (!window.cloud?.isReady) return;
    cloudInitialized = true;

    console.log('[cloud] initializing sync...');

    // Only the owner should bulk-migrate local→cloud. Viewers and pending
    // users must never push their (possibly stale) local data up.
    const canMigrate = currentUserDoc && currentUserDoc.role === 'owner';

    const startSubscriptions = () => {
        cloudUnitsUnsub = window.cloud.subscribeUnits(applyCloudUnitsSnapshot, err => {
            const lbl = document.getElementById('connectionLabel');
            if (lbl) lbl.textContent = 'Cloud offline';
        });
        cloudImplUnsub = window.cloud.subscribeImplements(applyCloudImplementsSnapshot, err => {
            console.warn('[cloud] implements offline');
        });
        if (window.cloud.subscribeHistory) {
            cloudHistoryUnsub = window.cloud.subscribeHistory(events => {
                cloudHistory = events || [];
                // Re-render the history modal live if it's currently open
                const modal = document.getElementById('historyModal');
                if (modal && modal.classList.contains('open')) {
                    showHistory(modal.dataset.unitId || undefined);
                }
            }, err => {
                console.warn('[cloud] history offline:', err && err.code);
                if (err && err.code === 'permission-denied') {
                    showHistoryRulesBanner();
                    showToast('History blocked by Firestore rules — open History for fix', 'warning');
                }
            });
        }
        if (window.cloud.subscribeUserCategories) {
            cloudUserCategoriesUnsub = window.cloud.subscribeUserCategories(
                applyCloudUserCategoriesSnapshot,
                err => {
                    console.warn('[cloud] userCategories offline:', err && err.code);
                    // Permission-denied here means the Firestore rules are
                    // missing — the Manage Categories modal (if open) should
                    // show the banner so the owner knows what to paste.
                    if (err && err.code === 'permission-denied') {
                        showCategoryRulesBanner();
                    }
                }
            );
        }
    };

    if (canMigrate) {
        migrateLocalToCloudIfNeeded().finally(startSubscriptions);
    } else {
        // Non-owners: never write, only read. Disable the local-first guard
        // so the cloud snapshot is the source of truth.
        _firstUnitsSnapshot = false;
        _firstImplSnapshot = false;
        startSubscriptions();
    }
}

// ============================================================
// AUTHENTICATION & ROLE GATING
// ============================================================

function setupAuth() {
    if (authInitialized) return;
    authInitialized = true;
    if (!window.cloud?.onAuthChange) return;

    window.cloud.onAuthChange(async user => {
        if (!user) {
            // Signed out — show login, tear down sync, clear in-memory data
            currentUser = null;
            currentUserDoc = null;
            tearDownCloudSync();
            showAuthGate('signin');
            return;
        }

        currentUser = user;

        // Look up (or create) the Firestore profile document for this user.
        let profile;
        try {
            profile = await window.cloud.getUserDoc(user.uid);
            if (!profile) {
                profile = await window.cloud.createUserDoc(user);
            } else if (window.cloud.isOwnerEmail(user.email) &&
                       (profile.role !== 'owner' || profile.status !== 'active')) {
                // Owner allowlist takes precedence — repair the doc.
                profile = await window.cloud.ensureOwnerDoc(user);
            }
        } catch (e) {
            console.error('[auth] could not load/create user doc:', e);
            showAuthError('signInError', 'Could not load your account profile. Please try again.');
            try { await window.cloud.signOutUser(); } catch (_) {}
            return;
        }

        currentUserDoc = profile;

        // Pending users: park them on the waiting screen.
        if (profile.status !== 'active') {
            showPendingGate(user.email);
            return;
        }

        // Active user — show app, gate UI by role, start cloud sync.
        hideAuthGates();
        applyRoleGating();
        renderUserPill();
        maybeInitCloudSync();
    });
}

function tearDownCloudSync() {
    if (cloudUnitsUnsub) { try { cloudUnitsUnsub(); } catch (_) {} cloudUnitsUnsub = null; }
    if (cloudImplUnsub) { try { cloudImplUnsub(); } catch (_) {} cloudImplUnsub = null; }
    if (cloudUsersUnsub) { try { cloudUsersUnsub(); } catch (_) {} cloudUsersUnsub = null; }
    if (cloudHistoryUnsub) { try { cloudHistoryUnsub(); } catch (_) {} cloudHistoryUnsub = null; }
    if (cloudUserCategoriesUnsub) { try { cloudUserCategoriesUnsub(); } catch (_) {} cloudUserCategoriesUnsub = null; }
    cloudHistory = [];
    userCategories = [];
    _firstUserCategoriesSnapshot = true;
    cloudInitialized = false;
}

function showAuthGate(tab) {
    document.getElementById('authGate').style.display = 'flex';
    document.getElementById('pendingGate').style.display = 'none';
    document.body.classList.add('auth-blocked');
    if (tab) switchAuthTab(tab);
}

function showPendingGate(email) {
    document.getElementById('authGate').style.display = 'none';
    const el = document.getElementById('pendingGate');
    el.style.display = 'flex';
    document.body.classList.add('auth-blocked');
    const emailEl = document.getElementById('pendingEmail');
    if (emailEl) emailEl.textContent = email || '';
}

function hideAuthGates() {
    document.getElementById('authGate').style.display = 'none';
    document.getElementById('pendingGate').style.display = 'none';
    document.body.classList.remove('auth-blocked');
}

function switchAuthTab(tab) {
    const isSignIn = tab === 'signin';
    document.getElementById('authTabSignIn').classList.toggle('active', isSignIn);
    document.getElementById('authTabSignUp').classList.toggle('active', !isSignIn);
    document.getElementById('signInForm').style.display = isSignIn ? '' : 'none';
    document.getElementById('signUpForm').style.display = isSignIn ? 'none' : '';
    showAuthError('signInError', '');
    showAuthError('signUpError', '');
}

function showAuthError(id, msg) {
    const el = document.getElementById(id);
    if (!el) return;
    el.textContent = msg || '';
    el.style.display = msg ? 'block' : 'none';
}

function friendlyAuthError(err) {
    const code = (err && err.code) || '';
    const map = {
        'auth/invalid-email': 'That email address is not valid.',
        'auth/user-not-found': 'No account found for that email.',
        'auth/wrong-password': 'Incorrect password.',
        'auth/invalid-credential': 'Email or password is incorrect.',
        'auth/email-already-in-use': 'An account with that email already exists.',
        'auth/weak-password': 'Password is too weak (min 6 characters).',
        'auth/network-request-failed': 'Network error — check your connection.',
        'auth/too-many-requests': 'Too many failed attempts. Try again later.'
    };
    return map[code] || (err && err.message) || 'Authentication failed.';
}

async function handleSignIn(event) {
    event.preventDefault();
    showAuthError('signInError', '');
    const email = document.getElementById('signInEmail').value.trim();
    const password = document.getElementById('signInPassword').value;
    try {
        showLoading(true);
        await window.cloud.signIn(email, password);
        // onAuthChange will take over from here.
    } catch (err) {
        showAuthError('signInError', friendlyAuthError(err));
    } finally {
        showLoading(false);
    }
}

async function handleSignUp(event) {
    event.preventDefault();
    showAuthError('signUpError', '');
    const name = document.getElementById('signUpName').value.trim();
    const email = document.getElementById('signUpEmail').value.trim();
    const password = document.getElementById('signUpPassword').value;
    try {
        showLoading(true);
        const user = await window.cloud.signUp(email, password, name);
        // Eagerly create the user doc so the owner sees them in the pending list.
        await window.cloud.createUserDoc(user, name);
        // onAuthChange will pick up the new user and route to pending/app.
    } catch (err) {
        showAuthError('signUpError', friendlyAuthError(err));
    } finally {
        showLoading(false);
    }
}

async function handleSignOut() {
    try {
        await window.cloud.signOutUser();
    } catch (e) { /* ignore */ }
}

function renderUserPill() {
    if (!currentUserDoc) return;
    const pill = document.getElementById('userPill');
    if (!pill) return;
    pill.style.display = '';
    document.getElementById('userPillName').textContent =
        currentUserDoc.displayName || currentUserDoc.email || 'User';
    const roleLabel = currentUserDoc.role === 'owner' ? 'Owner'
        : currentUserDoc.role === 'team' ? 'Team' : 'Viewer';
    document.getElementById('userPillRole').textContent = roleLabel;
    pill.dataset.role = currentUserDoc.role;
    document.getElementById('userMenuEmail').textContent = currentUserDoc.email || '';
    document.getElementById('userMenuRoleLabel').textContent = roleLabel + ' account';
}

function toggleUserMenu() {
    const menu = document.getElementById('userMenu');
    if (!menu) return;
    menu.classList.toggle('open');
    // Close on next outside click
    if (menu.classList.contains('open')) {
        setTimeout(() => {
            const close = (e) => {
                if (!document.getElementById('userPill').contains(e.target)) {
                    menu.classList.remove('open');
                    document.removeEventListener('click', close);
                }
            };
            document.addEventListener('click', close);
        }, 0);
    }
}

function canEdit() {
    return currentUserDoc && (currentUserDoc.role === 'owner' || currentUserDoc.role === 'team');
}
function isOwner() {
    return currentUserDoc && currentUserDoc.role === 'owner';
}

function applyRoleGating() {
    const editor = canEdit();
    const owner = isOwner();
    document.body.classList.toggle('role-viewer', !editor);
    document.body.classList.toggle('role-owner', !!owner);

    // Owner-only navigation links
    document.querySelectorAll('[data-owner-only]').forEach(el => {
        el.style.display = owner ? '' : 'none';
    });

    // If a non-owner is currently viewing the Users page, kick them back.
    if (!owner && currentView === 'users') {
        navigateTo('dashboard');
    }
    // If a viewer is on the Edit Units page, send them back to the dashboard.
    if (!editor && (currentView === 'editUnits' || currentView === 'implements')) {
        navigateTo('dashboard');
    }

    // Re-render any visible table to refresh its action buttons
    if (currentView === 'editUnits') renderEditTable();
    if (currentView === 'implements') renderImplementsTable();
}

function requireEdit() {
    if (!canEdit()) {
        showToast('Read-only access — ask the owner to grant edit rights', 'warning');
        return false;
    }
    return true;
}

// ============================================================
// USER MANAGEMENT (Owner only)
// ============================================================

function ensureUsersSubscription() {
    if (!isOwner()) return;
    if (cloudUsersUnsub) return;
    cloudUsersUnsub = window.cloud.subscribeUsers(users => {
        allUsers = users;
        if (currentView === 'users') renderUsersView();
    }, err => {
        console.warn('[cloud] users subscription error:', err);
    });
}

function renderUsersView() {
    if (!isOwner()) return;
    const pending = allUsers.filter(u => u.status !== 'active');
    const active  = allUsers.filter(u => u.status === 'active');

    document.getElementById('usersCount').textContent =
        `${allUsers.length} user(s) · ${pending.length} pending`;

    // Summary chips
    const ownerCount = active.filter(u => u.role === 'owner').length;
    const teamCount  = active.filter(u => u.role === 'team').length;
    const viewerCount = active.filter(u => u.role === 'viewer').length;
    document.getElementById('usersSummary').innerHTML = `
        <div class="user-chip user-chip--owner"><i class="fas fa-crown"></i> ${ownerCount} Owner</div>
        <div class="user-chip user-chip--team"><i class="fas fa-user-pen"></i> ${teamCount} Team</div>
        <div class="user-chip user-chip--viewer"><i class="fas fa-eye"></i> ${viewerCount} Viewer</div>
        <div class="user-chip user-chip--pending"><i class="fas fa-hourglass-half"></i> ${pending.length} Pending</div>
    `;

    // Pending table
    const pendingBody = document.getElementById('pendingUsersBody');
    if (pending.length === 0) {
        pendingBody.innerHTML = `<tr><td colspan="5" style="text-align:center;padding:20px;color:#718096">No pending sign-ups</td></tr>`;
    } else {
        pendingBody.innerHTML = pending.map((u, i) => `
            <tr>
                <td>${i + 1}</td>
                <td><strong>${escapeHtml(u.displayName || '—')}</strong></td>
                <td style="font-family:monospace;font-size:12px">${escapeHtml(u.email || '')}</td>
                <td style="white-space:nowrap;font-size:12px;color:#718096">${u.createdAt ? new Date(u.createdAt).toLocaleString() : '—'}</td>
                <td class="col-actions">
                    <div class="row-actions">
                        <button class="btn btn-success btn-sm" title="Approve as Viewer" onclick="approveUser('${escapeHtml(u.uid)}','viewer')">
                            <i class="fas fa-eye"></i> Approve as Viewer
                        </button>
                        <button class="btn btn-primary btn-sm" title="Approve as Team (with edit rights)" onclick="approveUser('${escapeHtml(u.uid)}','team')">
                            <i class="fas fa-user-pen"></i> Approve as Team
                        </button>
                        <button class="btn btn-secondary btn-sm" title="Reject and remove" onclick="rejectUser('${escapeHtml(u.uid)}')">
                            <i class="fas fa-xmark" style="color:var(--danger)"></i>
                        </button>
                    </div>
                </td>
            </tr>`).join('');
    }

    // Active table
    const activeBody = document.getElementById('activeUsersBody');
    if (active.length === 0) {
        activeBody.innerHTML = `<tr><td colspan="7" style="text-align:center;padding:20px;color:#718096">No active users yet</td></tr>`;
    } else {
        activeBody.innerHTML = active.map((u, i) => {
            const isMe = currentUser && u.uid === currentUser.uid;
            const isOwnerRow = u.role === 'owner';
            // Owner can't be demoted from this UI (and can't demote themselves).
            const roleSelect = isOwnerRow
                ? `<span class="badge badge-good"><i class="fas fa-crown"></i> Owner</span>`
                : `<select class="form-select user-role-select" onchange="changeUserRole('${escapeHtml(u.uid)}', this.value)">
                       <option value="viewer" ${u.role === 'viewer' ? 'selected' : ''}>Viewer (read-only)</option>
                       <option value="team"   ${u.role === 'team'   ? 'selected' : ''}>Team (can edit)</option>
                   </select>`;
            return `
            <tr>
                <td>${i + 1}</td>
                <td><strong>${escapeHtml(u.displayName || '—')}</strong>${isMe ? ' <span style="font-size:11px;color:#718096">(you)</span>' : ''}</td>
                <td style="font-family:monospace;font-size:12px">${escapeHtml(u.email || '')}</td>
                <td>${roleSelect}</td>
                <td style="white-space:nowrap;font-size:12px;color:#718096">${u.updatedAt ? new Date(u.updatedAt).toLocaleString() : '—'}</td>
                <td style="font-size:12px;color:#718096">${escapeHtml(u.updatedBy || '—')}</td>
                <td class="col-actions">
                    ${isOwnerRow
                        ? '<span style="font-size:11px;color:#a0aec0">protected</span>'
                        : `<button class="btn btn-secondary btn-sm" title="Remove user" onclick="removeUser('${escapeHtml(u.uid)}')"><i class="fas fa-user-minus" style="color:var(--danger)"></i></button>`}
                </td>
            </tr>`;
        }).join('');
    }
}

async function approveUser(uid, role) {
    if (!isOwner()) return;
    const user = allUsers.find(u => u.uid === uid);
    if (!user) return;
    try {
        await window.cloud.updateUserRole(uid, role, 'active', currentUserDoc.email);
        const roleLabel = role === 'team' ? 'Team' : 'Viewer';
        logEvent({
            action: 'approve',
            unitId: uid,
            unitName: `[User] ${user.displayName || user.email}`,
            field: 'role',
            before: 'pending',
            after: roleLabel
        });
        showToast(`Approved ${user.email} as ${roleLabel}`, 'success');
    } catch (e) {
        console.error('[users] approve failed:', e);
        showToast('Could not approve user — ' + e.message, 'error');
    }
}

async function rejectUser(uid) {
    if (!isOwner()) return;
    const user = allUsers.find(u => u.uid === uid);
    if (!user) return;
    if (!confirm(`Reject and remove ${user.email}? Their auth account will remain in Firebase but lose dashboard access.`)) return;
    try {
        await window.cloud.deleteUserDoc(uid);
        logEvent({
            action: 'reject',
            unitId: uid,
            unitName: `[User] ${user.displayName || user.email}`,
            before: 'pending',
            after: 'rejected'
        });
        showToast(`Rejected ${user.email}`, 'success');
    } catch (e) {
        showToast('Could not reject user — ' + e.message, 'error');
    }
}

async function changeUserRole(uid, newRole) {
    if (!isOwner()) return;
    const user = allUsers.find(u => u.uid === uid);
    if (!user) return;
    if (user.uid === currentUser.uid && user.role === 'owner') {
        showToast("You can't change your own owner role.", 'warning');
        renderUsersView();
        return;
    }
    const oldRole = user.role;
    if (oldRole === newRole) return;
    try {
        await window.cloud.updateUserRole(uid, newRole, 'active', currentUserDoc.email);
        const before = oldRole === 'team' ? 'Team' : oldRole === 'viewer' ? 'Viewer' : oldRole;
        const after  = newRole === 'team' ? 'Team' : 'Viewer';
        logEvent({
            action: 'role-change',
            unitId: uid,
            unitName: `[User] ${user.displayName || user.email}`,
            field: 'role',
            before,
            after
        });
        showToast(`${user.email} is now ${after}`, 'success');
    } catch (e) {
        showToast('Could not change role — ' + e.message, 'error');
    }
}

async function removeUser(uid) {
    if (!isOwner()) return;
    const user = allUsers.find(u => u.uid === uid);
    if (!user) return;
    if (user.role === 'owner') { showToast('Cannot remove an owner', 'warning'); return; }
    if (!confirm(`Remove ${user.email} from the dashboard? Their auth account stays in Firebase but they lose all access.`)) return;
    try {
        await window.cloud.deleteUserDoc(uid);
        logEvent({
            action: 'remove',
            unitId: uid,
            unitName: `[User] ${user.displayName || user.email}`,
            before: user.role,
            after: 'removed'
        });
        showToast(`Removed ${user.email}`, 'success');
    } catch (e) {
        showToast('Could not remove user — ' + e.message, 'error');
    }
}

// ============================================================
// CLOUD-READY HOOK
// ============================================================
// Cloud sync used to start as soon as the SDK was ready. Now it waits for
// the auth state to be known, so Firestore reads happen with a logged-in user.

function maybeInitCloudSync() {
    if (_cloudReadyFired && _localDataLoaded && currentUser && currentUserDoc?.status === 'active') {
        initCloudSync();
        ensureUsersSubscription();
    }
}

if (window.cloudReady) {
    _cloudReadyFired = true;
    setupAuth();
} else {
    document.addEventListener('cloud-ready', () => {
        _cloudReadyFired = true;
        setupAuth();
    });
}
