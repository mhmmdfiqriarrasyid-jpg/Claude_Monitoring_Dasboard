/* ============================================================
   Tractor Monitoring Dashboard - Core Application
   ============================================================ */

// ---- State ----
let globalData = [];
let filteredData = [];
let charts = {};
let sortState = { key: null, asc: true };

// ---- Constants ----
const COMPONENT_KEYS = ['display', 'gps', 'steering', 'jdlink'];
const COMPONENT_LABELS = { display: 'Display', gps: 'GPS', steering: 'Steering', jdlink: 'JDLink' };
const COMPONENT_COLORS = {
    display: '#dd6b20',
    gps: '#805ad5',
    steering: '#319795',
    jdlink: '#2d3748'
};
const CHART_COLORS = [
    '#3182ce', '#38a169', '#e53e3e', '#d69e2e', '#805ad5',
    '#319795', '#dd6b20', '#e53e3e', '#2d3748', '#ed64a6'
];

// ---- Initialization ----
document.addEventListener('DOMContentLoaded', () => {
    setupEventListeners();
});

function setupEventListeners() {
    // CSV Upload
    const csvInput = document.getElementById('csvInput');
    csvInput.addEventListener('change', handleCSVUpload);

    // Drag & Drop
    const dropZone = document.getElementById('dropZone');
    dropZone.addEventListener('dragover', e => { e.preventDefault(); dropZone.style.borderColor = '#3182ce'; });
    dropZone.addEventListener('dragleave', () => { dropZone.style.borderColor = ''; });
    dropZone.addEventListener('drop', e => {
        e.preventDefault();
        dropZone.style.borderColor = '';
        const file = e.dataTransfer.files[0];
        if (file && file.name.endsWith('.csv')) {
            parseCSVFile(file);
        } else {
            showToast('Please upload a .csv file', 'error');
        }
    });

    // Filters
    document.getElementById('searchInput').addEventListener('input', applyFilter);
    document.getElementById('statusFilter').addEventListener('change', applyFilter);
    document.getElementById('siteFilter').addEventListener('change', applyFilter);
    document.getElementById('componentFilter').addEventListener('change', applyFilter);
}

// ---- Utilities ----
function clean(v) { return (v || '').toString().trim(); }
function isGood(v) { return clean(v).toLowerCase() === 'good'; }
function pct(part, total) { return total > 0 ? Math.round((part / total) * 1000) / 10 : 0; }

function getVal(row, key) {
    const k = Object.keys(row).find(h => h.toLowerCase().trim() === key.toLowerCase());
    return k ? row[k] : '';
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

// ---- Data Source Panel ----
function toggleDatasource(forceOpen) {
    const body = document.getElementById('dsBody');
    const chevron = document.getElementById('dsChevron');
    const isOpen = body.classList.contains('open');

    if (forceOpen && isOpen) return;

    body.classList.toggle('open', forceOpen || !isOpen);
    chevron.classList.toggle('open', forceOpen || !isOpen);
}

// ---- CSV Upload ----
function handleCSVUpload(e) {
    const file = e.target.files[0];
    if (file) parseCSVFile(file);
}

function parseCSVFile(file) {
    showLoading(true);
    Papa.parse(file, {
        header: true,
        skipEmptyLines: true,
        complete: result => {
            globalData = processData(result.data);
            onDataLoaded('CSV');
            showLoading(false);
        },
        error: err => {
            showToast('Failed to parse CSV: ' + err.message, 'error');
            showLoading(false);
        }
    });
}

// ---- Data Processing ----
function processData(rows) {
    return rows
        .map(r => ({
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
        .filter(r => r.name || r.sn); // Remove empty rows
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

// ---- Dashboard Rendering ----
function onDataLoaded(source) {
    document.getElementById('emptyState').style.display = 'none';
    document.getElementById('dashboardContent').style.display = 'block';

    populateFilters();
    filteredData = [...globalData];
    updateDashboard(filteredData);

    const now = new Date().toLocaleString();
    document.getElementById('lastUpdated').textContent = `Updated: ${now}`;

    // Update header status indicator
    document.getElementById('connectionDot').classList.add('connected');
    document.getElementById('connectionLabel').textContent = 'CSV Loaded';

    showToast(`${globalData.length} units loaded successfully`, 'success');
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
            datasets: [{
                data: [good, breakdown],
                backgroundColor: ['#38a169', '#e53e3e'],
                borderWidth: 0,
                hoverOffset: 6
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            cutout: '65%',
            plugins: {
                legend: { position: 'bottom', labels: { padding: 16, usePointStyle: true, pointStyle: 'circle', font: { size: 12, family: 'Inter' } } },
                tooltip: {
                    callbacks: {
                        label: ctx => {
                            const total = ctx.dataset.data.reduce((a, b) => a + b, 0);
                            const val = ctx.parsed;
                            return ` ${ctx.label}: ${val} (${pct(val, total)}%)`;
                        }
                    }
                }
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
        if (isGood(d.status)) siteMap[s].good++;
        else siteMap[s].breakdown++;
    });

    const labels = Object.keys(siteMap).sort();
    const goodArr = labels.map(s => siteMap[s].good);
    const breakdownArr = labels.map(s => siteMap[s].breakdown);

    destroyChart('siteChart');
    charts.siteChart = new Chart(document.getElementById('siteChart'), {
        type: 'bar',
        data: {
            labels,
            datasets: [
                { label: 'Good', data: goodArr, backgroundColor: '#38a169', borderRadius: 4, barPercentage: 0.6 },
                { label: 'Breakdown', data: breakdownArr, backgroundColor: '#e53e3e', borderRadius: 4, barPercentage: 0.6 }
            ]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            indexAxis: 'y',
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
                        stroke-dasharray="${circumference}" stroke-dashoffset="${offset}"
                        stroke-linecap="round"/>
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
        const isBreakdown = !isGood(d.status);
        return `
        <tr class="${isBreakdown ? 'row-breakdown' : ''}">
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
    if (sortState.key === key) {
        sortState.asc = !sortState.asc;
    } else {
        sortState.key = key;
        sortState.asc = true;
    }

    if (key === 'no') {
        // Reset to default order
        sortState.key = null;
        filteredData = [...applyFilterLogic()];
    } else {
        filteredData.sort((a, b) => {
            const va = (a[key] || '').toLowerCase();
            const vb = (b[key] || '').toLowerCase();
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

    // Summary chips
    const chipColors = { Unit: '#e53e3e', Display: '#dd6b20', GPS: '#805ad5', Steering: '#319795', JDLink: '#2d3748' };
    document.getElementById('issueSummary').innerHTML = Object.entries(issueData.counts).map(([key, count]) => `
        <div class="issue-chip">
            <span class="issue-chip__dot" style="background:${chipColors[key]}"></span>
            <span class="issue-chip__label">${key}</span>
            <span class="issue-chip__count">${count}</span>
        </div>
    `).join('');

    // Top Issue Chart (horizontal bar)
    const sorted = Object.entries(issueData.counts)
        .filter(([, v]) => v > 0)
        .sort((a, b) => b[1] - a[1]);

    destroyChart('topIssueChart');
    charts.topIssueChart = new Chart(document.getElementById('topIssueChart'), {
        type: 'bar',
        data: {
            labels: sorted.map(x => x[0]),
            datasets: [{
                data: sorted.map(x => x[1]),
                backgroundColor: sorted.map(x => chipColors[x[0]]),
                borderRadius: 4,
                barPercentage: 0.5
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            indexAxis: 'y',
            scales: {
                x: { beginAtZero: true, ticks: { stepSize: 1, font: { size: 11 } }, grid: { color: '#edf2f7' } },
                y: { ticks: { font: { size: 11, family: 'Inter', weight: 600 } }, grid: { display: false } }
            },
            plugins: { legend: { display: false } }
        }
    });

    // Issues by Site Chart
    const siteCounts = {};
    globalData.forEach(d => {
        const issues = detectIssues(d);
        if (issues.length > 0) {
            const s = d.site || 'Unknown';
            siteCounts[s] = (siteCounts[s] || 0) + 1;
        }
    });

    const siteLabels = Object.keys(siteCounts).sort();
    const siteValues = siteLabels.map(s => siteCounts[s]);

    destroyChart('issueBySiteChart');
    charts.issueBySiteChart = new Chart(document.getElementById('issueBySiteChart'), {
        type: 'bar',
        data: {
            labels: siteLabels,
            datasets: [{
                data: siteValues,
                backgroundColor: '#d69e2e',
                borderRadius: 4,
                barPercentage: 0.5
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            scales: {
                y: { beginAtZero: true, ticks: { stepSize: 1, font: { size: 11 } }, grid: { color: '#edf2f7' } },
                x: { ticks: { font: { size: 11, family: 'Inter' } }, grid: { display: false } }
            },
            plugins: { legend: { display: false } }
        }
    });

    // Repair Table
    let repairRows = globalData.filter(d => detectIssues(d).length > 0);
    if (issueFilterVal) {
        repairRows = repairRows.filter(d => detectIssues(d).includes(issueFilterVal));
    }

    document.getElementById('repairBody').innerHTML = repairRows.map((d, i) => `
        <tr>
            <td>${i + 1}</td>
            <td><strong>${d.name}</strong></td>
            <td>${d.model}</td>
            <td style="font-family:monospace;font-size:12px">${d.sn}</td>
            <td>${detectIssues(d).map(x =>
                `<span class="badge-component badge-${x.toLowerCase()}">${x}</span>`
            ).join(' ')}</td>
            <td>${d.site}</td>
        </tr>
    `).join('');
}

// ---- Filters ----
function populateFilters() {
    const statuses = [...new Set(globalData.map(d => d.status))].filter(Boolean).sort();
    const sites = [...new Set(globalData.map(d => d.site))].filter(Boolean).sort();

    document.getElementById('statusFilter').innerHTML =
        `<option value="">All Status</option>` + statuses.map(s => `<option value="${s}">${s}</option>`).join('');

    document.getElementById('siteFilter').innerHTML =
        `<option value="">All Sites</option>` + sites.map(s => `<option value="${s}">${s}</option>`).join('');
}

function applyFilterLogic() {
    const keyword = document.getElementById('searchInput').value.toLowerCase();
    const statusVal = document.getElementById('statusFilter').value;
    const siteVal = document.getElementById('siteFilter').value;
    const compVal = document.getElementById('componentFilter').value;

    return globalData.filter(d => {
        if (statusVal && d.status !== statusVal) return false;
        if (siteVal && d.site !== siteVal) return false;
        if (keyword) {
            const haystack = `${d.name} ${d.model} ${d.sn}`.toLowerCase();
            if (!haystack.includes(keyword)) return false;
        }
        if (compVal) {
            const issues = detectIssues(d);
            if (!issues.includes(compVal)) return false;
        }
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
    const shown = data.length;
    el.textContent = shown === total ? `${total} units` : `${shown} of ${total} units`;
}

// ---- Export ----
function exportCSV() {
    if (filteredData.length === 0) {
        showToast('No data to export', 'warning');
        return;
    }

    const headers = ['No', 'Nickname', 'Model', 'Serial Number', 'Status', 'Display', 'GPS', 'Steering', 'JDLink', 'Site'];
    const rows = filteredData.map((d, i) => [
        i + 1, d.name, d.model, d.sn, d.status, d.display, d.gps, d.steering, d.jdlink, d.site
    ]);

    const csv = [headers, ...rows].map(row =>
        row.map(cell => `"${String(cell).replace(/"/g, '""')}"`).join(',')
    ).join('\n');

    const blob = new Blob(['\uFEFF' + csv], { type: 'text/csv;charset=utf-8;' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = `tractor_monitoring_${new Date().toISOString().slice(0, 10)}.csv`;
    a.click();
    URL.revokeObjectURL(url);

    showToast(`Exported ${filteredData.length} units to CSV`, 'success');
}

// ---- Chart Utilities ----
function destroyChart(id) {
    if (charts[id]) {
        charts[id].destroy();
        delete charts[id];
    }
}
