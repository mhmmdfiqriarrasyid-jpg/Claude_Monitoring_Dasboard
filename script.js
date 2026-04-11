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

// ---- Constants ----
const STORAGE_KEY = 'tractorUnits';
const PENDING_CHANGES_KEY = 'tractorPendingChanges';
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
    setupEventListeners();
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

// ============================================================
// NAVIGATION
// ============================================================

function navigateTo(view) {
    document.getElementById('viewDashboard').style.display = (view === 'dashboard') ? 'block' : 'none';
    document.getElementById('viewEditUnits').style.display = (view === 'editUnits') ? 'block' : 'none';

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
}

// ============================================================
// LOCAL STORAGE
// ============================================================

function saveToStorage(data) {
    try {
        localStorage.setItem(STORAGE_KEY, JSON.stringify(data));
    } catch (e) {
        showToast('Storage full. Could not save data.', 'error');
    }
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
    const existingSNs = new Set(globalData.map(d => d.sn.toLowerCase()));
    const toAdd = [];
    let skipped = 0;

    newUnits.forEach(u => {
        if (!u.id) u.id = generateId();
        const snLower = u.sn.toLowerCase();
        if (snLower && existingSNs.has(snLower)) {
            skipped++;
        } else {
            toAdd.push(u);
            if (snLower) existingSNs.add(snLower);
        }
    });

    if (toAdd.length > 0) {
        globalData = [...globalData, ...toAdd];
        saveToStorage(globalData);
        recordChange({ type: 'added', detail: `${toAdd.length} unit(s) added` });
    }

    return { added: toAdd.length, skipped };
}

function updateUnit(id, fields) {
    const idx = globalData.findIndex(d => d.id === id);
    if (idx === -1) return false;

    globalData[idx] = { ...globalData[idx], ...fields };
    saveToStorage(globalData);
    recordChange({ type: 'updated', detail: `Unit "${globalData[idx].name}" updated` });
    return true;
}

function deleteUnits(ids) {
    const idSet = new Set(ids);
    const count = globalData.filter(d => idSet.has(d.id)).length;
    globalData = globalData.filter(d => !idSet.has(d.id));
    saveToStorage(globalData);
    if (count > 0) {
        recordChange({ type: 'deleted', detail: `${count} unit(s) deleted` });
    }
    return count;
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
// DATA PROCESSING
// ============================================================

function processData(rows) {
    return rows
        .map(r => ({
            id: generateId(),
            name: clean(getVal(r, 'Nickname')),
            model: clean(getVal(r, 'Model')),
            sn: clean(getVal(r, 'Serial Number')),
            status: clean(getVal(r, 'Status Unit')),
            display: clean(getVal(r, 'Status Unit Display')),
            gps: clean(getVal(r, 'Status Unit GPS')),
            steering: clean(getVal(r, 'Status Unit Steering')),
            jdlink: clean(getVal(r, 'Status Unit JDLink')),
            site: clean(getVal(r, 'Site'))
        }))
        .filter(r => r.name || r.sn);
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
            <td><strong>${d.name}</strong></td>
            <td>${d.model}</td>
            <td style="font-family:monospace;font-size:12px">${d.sn}</td>
            <td><span class="badge ${isGood(d.status) ? 'badge-good' : 'badge-breakdown'}">
                <i class="fas fa-${isGood(d.status) ? 'check' : 'xmark'}"></i> ${d.status}
            </span></td>
            <td class="${isGood(d.display) ? 'cell-good' : 'cell-bad'}">${d.display}</td>
            <td class="${isGood(d.gps) ? 'cell-good' : 'cell-bad'}">${d.gps}</td>
            <td class="${isGood(d.steering) ? 'cell-good' : 'cell-bad'}">${d.steering}</td>
            <td class="${isGood(d.jdlink) ? 'cell-good' : 'cell-bad'}">${d.jdlink}</td>
            <td>${d.site}</td>
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
            <td><strong>${d.name}</strong></td>
            <td>${d.model}</td>
            <td style="font-family:monospace;font-size:12px">${d.sn}</td>
            <td>${detectIssues(d).map(x => `<span class="badge-component badge-${x.toLowerCase()}">${x}</span>`).join(' ')}</td>
            <td>${d.site}</td>
        </tr>`).join('');
}

// ============================================================
// FILTERS
// ============================================================

function populateFilters() {
    const statuses = [...new Set(globalData.map(d => d.status))].filter(Boolean).sort();
    const sites = [...new Set(globalData.map(d => d.site))].filter(Boolean).sort();
    document.getElementById('statusFilter').innerHTML = `<option value="">All Status</option>` + statuses.map(s => `<option value="${s}">${s}</option>`).join('');
    document.getElementById('siteFilter').innerHTML = `<option value="">All Sites</option>` + sites.map(s => `<option value="${s}">${s}</option>`).join('');
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
            const parsed = processData(result.data);
            const { added, skipped } = addUnits(parsed);

            let msg = `${added} unit(s) added`;
            if (skipped > 0) msg += `, ${skipped} duplicate(s) skipped`;
            showToast(msg, added > 0 ? 'success' : 'warning');

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

// ---- Edit Table ----
function renderEditTable() {
    updateEditCount();
    selectedUnitIds.clear();
    updateSelectedCount();

    const selectAllBox = document.getElementById('selectAll');
    if (selectAllBox) selectAllBox.checked = false;

    const tbody = document.getElementById('editBody');
    tbody.innerHTML = globalData.map((d, i) => `
        <tr>
            <td class="col-check"><input type="checkbox" class="unit-check" data-id="${d.id}" onchange="updateSelectedCount()"></td>
            <td>${i + 1}</td>
            <td><span class="inline-edit" contenteditable="true" data-id="${d.id}" data-field="name" onblur="saveInlineEdit(this)">${d.name}</span></td>
            <td><span class="inline-edit" contenteditable="true" data-id="${d.id}" data-field="model" onblur="saveInlineEdit(this)">${d.model}</span></td>
            <td style="font-family:monospace;font-size:12px">${d.sn}</td>
            <td><span class="inline-edit" contenteditable="true" data-id="${d.id}" data-field="status" onblur="saveInlineEdit(this)">${d.status}</span></td>
            <td><span class="inline-edit" contenteditable="true" data-id="${d.id}" data-field="display" onblur="saveInlineEdit(this)">${d.display}</span></td>
            <td><span class="inline-edit" contenteditable="true" data-id="${d.id}" data-field="gps" onblur="saveInlineEdit(this)">${d.gps}</span></td>
            <td><span class="inline-edit" contenteditable="true" data-id="${d.id}" data-field="steering" onblur="saveInlineEdit(this)">${d.steering}</span></td>
            <td><span class="inline-edit" contenteditable="true" data-id="${d.id}" data-field="jdlink" onblur="saveInlineEdit(this)">${d.jdlink}</span></td>
            <td><span class="inline-edit" contenteditable="true" data-id="${d.id}" data-field="site" onblur="saveInlineEdit(this)">${d.site}</span></td>
            <td class="col-actions">
                <div class="row-actions">
                    <button class="btn btn-secondary" title="Edit" onclick="editUnit('${d.id}')"><i class="fas fa-pen"></i></button>
                    <button class="btn btn-secondary" title="Delete" onclick="deleteUnit('${d.id}')"><i class="fas fa-trash" style="color:var(--danger)"></i></button>
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
    if (!confirm(`Delete unit "${unit.name || unit.sn}"?`)) return;
    deleteUnits([id]);
    renderEditTable();
    showToast(`Unit "${unit.name || unit.sn}" deleted`, 'success');
}

function deleteSelected() {
    const count = selectedUnitIds.size;
    if (count === 0) return;
    if (!confirm(`Delete ${count} selected unit(s)?`)) return;
    deleteUnits([...selectedUnitIds]);
    renderEditTable();
    showToast(`${count} unit(s) deleted`, 'success');
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
        const newUnit = { id: generateId(), ...fields };
        const { added, skipped } = addUnits([newUnit]);
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
