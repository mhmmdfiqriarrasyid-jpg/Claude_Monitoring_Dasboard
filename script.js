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

// ---- Cloud sync state ----
let cloudInitialized = false;
let cloudUnitsUnsub = null;
let cloudImplUnsub = null;
let suppressCloudWrites = false; // true while applying a cloud snapshot — prevents loops

// ---- Constants ----
const STORAGE_KEY = 'tractorUnits';
const IMPLEMENTS_STORAGE_KEY = 'tractorImplements';
const PENDING_CHANGES_KEY = 'tractorPendingChanges';
const AUDIT_LOG_KEY = 'tractorAuditLog';
const AUDIT_LOG_MAX = 500;
const BACKUP_RING_KEY = 'tractorUnits_autobackup';
const BACKUP_RING_SIZE = 3;
const DARK_MODE_KEY = 'tractorDarkMode';
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
    document.getElementById('viewDashboard').style.display = (view === 'dashboard') ? 'block' : 'none';
    document.getElementById('viewEditUnits').style.display = (view === 'editUnits') ? 'block' : 'none';
    const implView = document.getElementById('viewImplements');
    if (implView) implView.style.display = (view === 'implements') ? 'block' : 'none';

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
    try {
        const log = JSON.parse(localStorage.getItem(AUDIT_LOG_KEY) || '[]');
        log.unshift({ ...entry, timestamp: Date.now() });
        while (log.length > AUDIT_LOG_MAX) log.pop();
        localStorage.setItem(AUDIT_LOG_KEY, JSON.stringify(log));
    } catch (e) { /* ignore */ }
}

function getAuditLog() {
    try { return JSON.parse(localStorage.getItem(AUDIT_LOG_KEY) || '[]'); }
    catch (e) { return []; }
}

function showHistory(unitId) {
    const log = getAuditLog();
    const filtered = unitId ? log.filter(e => e.unitId === unitId) : log;
    const title = unitId
        ? `History: ${escapeHtml((globalData.find(u => u.id === unitId) || {}).name || 'Unit')}`
        : 'Change History';
    document.getElementById('historyTitle').innerHTML = title;

    const tbody = document.getElementById('historyBody');
    if (filtered.length === 0) {
        tbody.innerHTML = '<tr><td colspan="6" style="text-align:center;padding:24px;color:#718096">No history recorded</td></tr>';
    } else {
        tbody.innerHTML = filtered.map(e => `
            <tr>
                <td style="white-space:nowrap">${new Date(e.timestamp).toLocaleString()}</td>
                <td><span class="audit-badge audit-${escapeHtml(e.action)}">${escapeHtml(e.action)}</span></td>
                <td>${escapeHtml(e.unitName || '-')}</td>
                <td>${escapeHtml(e.field || '-')}</td>
                <td>${escapeHtml(e.before != null ? e.before : '-')}</td>
                <td>${escapeHtml(e.after != null ? e.after : '-')}</td>
            </tr>`).join('');
    }
    document.getElementById('historyModal').classList.add('open');
}

function closeHistory() {
    document.getElementById('historyModal').classList.remove('open');
}

function clearHistory() {
    if (!confirm('Clear ALL change history? This cannot be undone.')) return;
    localStorage.removeItem(AUDIT_LOG_KEY);
    showHistory();
    showToast('History cleared', 'success');
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
            <td><span class="badge ${isGood(d.status) ? 'badge-good' : 'badge-breakdown'}">
                <i class="fas fa-${isGood(d.status) ? 'check' : 'xmark'}"></i> ${escapeHtml(d.status)}
            </span></td>
            <td class="${isGood(d.display) ? 'cell-good' : 'cell-bad'}">${escapeHtml(d.display)}</td>
            <td class="${isGood(d.gps) ? 'cell-good' : 'cell-bad'}">${escapeHtml(d.gps)}</td>
            <td class="${isGood(d.steering) ? 'cell-good' : 'cell-bad'}">${escapeHtml(d.steering)}</td>
            <td class="${isGood(d.jdlink) ? 'cell-good' : 'cell-bad'}">${escapeHtml(d.jdlink)}</td>
            <td>${escapeHtml(d.site)}</td>
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
    const headers = ['No', 'Nickname', 'Model', 'Serial Number', 'Status', 'Display', 'GPS', 'Steering', 'JDLink', 'Site'];
    const rows = filteredData.map((d, i) => [i + 1, d.name, d.model, d.sn, d.status, d.display, d.gps, d.steering, d.jdlink, d.site]);
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
    const rows = query
        ? globalData.filter(d => `${d.name} ${d.model} ${d.sn} ${d.site}`.toLowerCase().includes(query))
        : globalData;

    const tbody = document.getElementById('editBody');
    if (rows.length === 0) {
        tbody.innerHTML = `<tr><td colspan="12" style="text-align:center;padding:24px;color:#718096">${query ? 'No units match your search' : 'No units yet. Click <strong>Add Unit</strong> or <strong>Import CSV</strong> to get started.'}</td></tr>`;
        return;
    }

    tbody.innerHTML = rows.map((d, i) => `
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
            <td class="col-actions">
                <div class="row-actions">
                    <button class="btn btn-secondary" title="History" onclick="showHistory('${escapeHtml(d.id)}')"><i class="fas fa-clock-rotate-left"></i></button>
                    <button class="btn btn-secondary" title="Edit" onclick="editUnit('${escapeHtml(d.id)}')"><i class="fas fa-pen"></i></button>
                    <button class="btn btn-secondary" title="Delete" onclick="deleteUnit('${escapeHtml(d.id)}')"><i class="fas fa-trash" style="color:var(--danger)"></i></button>
                </div>
            </td>
        </tr>`).join('');
}

// ---- Inline Edit ----
function saveInlineEdit(el) {
    const id = el.dataset.id;
    const field = el.dataset.field;
    const newValue = clean(el.textContent);
    const unit = globalData.find(d => d.id === id);

    if (unit && unit[field] !== newValue) {
        updateUnit(id, { [field]: newValue });
        showToast(`${COMPONENT_LABELS[field] || field.charAt(0).toUpperCase() + field.slice(1)} updated`, 'success');
    }
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
    const unit = globalData.find(d => d.id === id);
    if (!unit) return;
    const { removed } = deleteUnits([id]);
    renderEditTable();
    showUndoToast(`Unit "${unit.name || unit.sn}" deleted`, removed);
}

function deleteSelected() {
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
    document.getElementById('modalTitle').textContent = 'Add Unit';
    document.getElementById('editUnitId').value = '';
    document.getElementById('unitForm').reset();
    document.getElementById('unitModal').classList.add('open');
}

function editUnit(id) {
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

    document.getElementById('unitModal').classList.add('open');
}

function saveUnit(event) {
    event.preventDefault();

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
        jdlink: document.getElementById('formJDLink').value
    };

    if (id) {
        // Edit existing
        updateUnit(id, fields);
        showToast(`Unit "${fields.name}" updated`, 'success');
    } else {
        // Add new
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
        tbody.innerHTML = `<tr><td colspan="8" style="text-align:center;padding:24px;color:#718096">${
            query ? 'No implements match your search'
                  : 'No implements yet. Click <strong>Add Implement</strong> to get started.'
        }</td></tr>`;
        return;
    }

    tbody.innerHTML = rows.map((d, i) => `
        <tr>
            <td class="col-check"><input type="checkbox" class="impl-check" data-id="${escapeHtml(d.id)}" onchange="updateSelectedImplementCount()"></td>
            <td>${i + 1}</td>
            <td>${escapeHtml(d.profileName)}</td>
            <td>${escapeHtml(d.equipmentType)}</td>
            <td>${escapeHtml(d.workingWidth)}</td>
            <td>${escapeHtml(d.operation)}</td>
            <td>${escapeHtml(d.connectingType)}</td>
            <td class="col-actions">
                <div class="row-actions">
                    <button class="btn btn-secondary" title="Edit" onclick="editImplement('${escapeHtml(d.id)}')"><i class="fas fa-pen"></i></button>
                    <button class="btn btn-secondary" title="Delete" onclick="deleteImplement('${escapeHtml(d.id)}')"><i class="fas fa-trash" style="color:var(--danger)"></i></button>
                </div>
            </td>
        </tr>`).join('');
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
    document.getElementById('implementModalTitle').textContent = 'Add Implement';
    document.getElementById('editImplementId').value = '';
    document.getElementById('implementForm').reset();
    document.getElementById('implementModal').classList.add('open');
}

function editImplement(id) {
    const imp = globalImplements.find(d => d.id === id);
    if (!imp) return;

    document.getElementById('implementModalTitle').textContent = 'Edit Implement';
    document.getElementById('editImplementId').value = id;
    IMPLEMENT_FIELDS.forEach(f => {
        const el = document.getElementById(f.inputId);
        if (el) el.value = imp[f.key] || '';
    });
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
        if (cloudUnits.length === 0 && globalData.length > 0) {
            console.log(`[cloud] migrating ${globalData.length} local units to Firestore...`);
            await window.cloud.saveUnits(globalData);
            showToast(`Uploaded ${globalData.length} units to cloud`, 'success');
        }
        // Implements
        const cloudImpls = await window.cloud.getAllImplements();
        if (cloudImpls.length === 0 && globalImplements.length > 0) {
            console.log(`[cloud] migrating ${globalImplements.length} local implements to Firestore...`);
            await window.cloud.saveImplements(globalImplements);
            showToast(`Uploaded ${globalImplements.length} implements to cloud`, 'success');
        }
    } catch (e) {
        console.error('[cloud] migration failed:', e);
    }
}

function applyCloudUnitsSnapshot(units) {
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
}

function applyCloudImplementsSnapshot(items) {
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

function initCloudSync() {
    if (cloudInitialized) return;
    if (!window.cloud?.isReady) return;
    cloudInitialized = true;

    console.log('[cloud] initializing sync...');

    // Step 1: push any local-only data up before subscribing
    migrateLocalToCloudIfNeeded().finally(() => {
        // Step 2: subscribe to live updates from Firestore
        cloudUnitsUnsub = window.cloud.subscribeUnits(applyCloudUnitsSnapshot, err => {
            const lbl = document.getElementById('connectionLabel');
            if (lbl) lbl.textContent = 'Cloud offline';
        });
        cloudImplUnsub = window.cloud.subscribeImplements(applyCloudImplementsSnapshot, err => {
            console.warn('[cloud] implements offline');
        });
    });
}

// Wait for firebase-init.js to dispatch 'cloud-ready'. It may already
// have run by the time this listener registers, so check the flag too.
if (window.cloudReady) {
    initCloudSync();
} else {
    document.addEventListener('cloud-ready', initCloudSync);
}
