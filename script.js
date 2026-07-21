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
let globalDamages = [];
let selectedDamageIds = new Set();
let _dmgPhotoData = '';   // data URL of the photo for the damage modal currently open
let globalLicenseStock = [];
let selectedLicenseIds = new Set();

// ---- Breakdown reason modal state ----
let _pendingBreakdown = null;  // { unitId, fields, isInline, el }

// ---- Edit table sort state ----
let editSortState = { key: null, asc: true };

// ---- Cloud sync state ----
let cloudInitialized = false;
let cloudUnitsUnsub = null;
let cloudImplUnsub = null;
let cloudDamageUnsub = null;
let cloudLicenseUnsub = null;
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
let _firstDamageSnapshot = true;
let _firstLicenseSnapshot = true;

// ---- Auth state ----
let currentUser = null;        // Firebase Auth user object
let currentUserDoc = null;     // Firestore profile doc { email, role, status, ... }
let allUsers = [];             // Mirror of users collection (owner only)
let authInitialized = false;

// ---- Constants ----
const STORAGE_KEY = 'tractorUnits';
const IMPLEMENTS_STORAGE_KEY = 'tractorImplements';
const DAMAGE_STORAGE_KEY = 'tractorDamageRecords';
const DAMAGE_TYPES = ['Mekanis', 'Software', 'Device Precision'];
const DAMAGE_COMPONENTS = ['GPS', 'Display', 'JDLink', 'Steering Sensor'];
// Map a Device Precision component to the unit field it controls.
const DAMAGE_COMPONENT_FIELD = { 'GPS': 'gps', 'Display': 'display', 'Steering Sensor': 'steering', 'JDLink': 'jdlink' };
const DAMAGE_PHOTO_MAX_DIM = 1280;     // longest-side px after resize
const DAMAGE_PHOTO_QUALITY = 0.7;      // initial JPEG quality
const DAMAGE_PHOTO_MAX_BYTES = 900 * 1024; // keep data URL under Firestore 1MB doc limit
const LICENSE_STORAGE_KEY = 'tractorLicenseStock';
const LICENSE_TYPE_DEFAULTS = ['SF-RTK', 'SF-1', 'G5 Basic', 'G5 Advance'];
const LICENSE_LOW_STOCK_THRESHOLD = 5; // "sisa" at or below this → dashboard warning
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
    // Light-only theme (warm editorial). Clear any stale dark preference so the
    // app never renders the retired dark mode.
    document.body.classList.remove('dark');

    setupEventListeners();
    setupKeyboardShortcuts();
    registerServiceWorker();

    loadImplements();
    loadDamages();
    loadLicenseStock();
    checkStorageUsage();

    if (loadFromStorage()) {
        onDataLoaded();
    }

    // Local data is now in globalData — safe to start cloud sync if cloud is ready.
    _localDataLoaded = true;
    maybeInitCloudSync();
});

function setupEventListeners() {
    // Grouped nav dropdowns (Setup / Log Report): click toggle opens one group at
    // a time; any other click (incl. a dropdown item) closes all groups.
    document.addEventListener('click', e => {
        const toggle = e.target.closest('.nav__group-toggle');
        if (toggle) {
            const grp = toggle.closest('.nav__group');
            const wasOpen = grp.classList.contains('open');
            document.querySelectorAll('.nav__group.open').forEach(g => g.classList.remove('open'));
            if (!wasOpen) grp.classList.add('open');
            return;
        }
        document.querySelectorAll('.nav__group.open').forEach(g => g.classList.remove('open'));
    });

    // Global unit search (topbar)
    const gSearch = document.getElementById('globalSearch');
    if (gSearch) {
        let gTimer = null;
        gSearch.addEventListener('input', () => {
            clearTimeout(gTimer);
            gTimer = setTimeout(renderGlobalSearchResults, 150);
        });
        gSearch.addEventListener('keydown', e => {
            if (e.key === 'Enter') {
                const first = document.querySelector('#globalSearchResults .global-search__item');
                if (first) first.click();
            } else if (e.key === 'Escape') {
                closeGlobalSearch();
                gSearch.blur();
            }
        });
        document.addEventListener('click', e => {
            if (!e.target.closest('#globalSearchWrap')) closeGlobalSearch();
        });
    }

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
    const implementCsvInput = document.getElementById('implementCsvInput');
    if (implementCsvInput) implementCsvInput.addEventListener('change', e => {
        const file = e.target.files[0];
        if (file) handleImplementCSVImport(file);
        implementCsvInput.value = '';
    });

    // Damage (Kerusakan) page search + type filter
    const damageSearch = document.getElementById('damageSearch');
    if (damageSearch) damageSearch.addEventListener('input', renderDamageTable);
    const damageTypeFilter = document.getElementById('damageTypeFilter');
    if (damageTypeFilter) damageTypeFilter.addEventListener('change', renderDamageTable);

    // License stock page search + filters
    const licenseSearch = document.getElementById('licenseSearch');
    if (licenseSearch) licenseSearch.addEventListener('input', renderLicenseStockTable);
    const licenseTxnFilter = document.getElementById('licenseTxnFilter');
    if (licenseTxnFilter) licenseTxnFilter.addEventListener('change', renderLicenseStockTable);
    const licenseTypeFilter = document.getElementById('licenseTypeFilter');
    if (licenseTypeFilter) licenseTypeFilter.addEventListener('change', renderLicenseStockTable);
    const licenseCsvInput = document.getElementById('licenseCsvInput');
    if (licenseCsvInput) licenseCsvInput.addEventListener('change', e => {
        const file = e.target.files[0];
        if (file) handleLicenseCSVImport(file);
        licenseCsvInput.value = '';
    });

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

    // Attachment file input
    const attachInput = document.getElementById('attachFileInput');
    if (attachInput) {
        attachInput.addEventListener('change', handleAttachFileChange);
    }

    // Restore compact mode preference
    if (localStorage.getItem('editTableCompact') === '1') {
        const table = document.getElementById('editTable');
        if (table) table.classList.add('compact');
        const icon = document.querySelector('#compactToggle i');
        if (icon) icon.className = 'fas fa-expand';
    }

    // Close COA dropdowns on outside click
    document.addEventListener('click', () => {
        document.querySelectorAll('.coa-cell.open').forEach(el => el.classList.remove('open'));
    });
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
        }
    });
}

// Dark mode retired — the app is light-only (warm editorial theme). Kept as a
// no-op so any lingering reference stays safe.
function toggleDarkMode() {}

function registerServiceWorker() {
    if (!('serviceWorker' in navigator)) return;

    // When a new service worker takes control, reload once so the fresh app
    // shell is shown. Guard against reload loops, and skip the very first
    // install (no previous controller = first visit, nothing to refresh).
    let refreshing = false;
    const hadController = !!navigator.serviceWorker.controller;
    navigator.serviceWorker.addEventListener('controllerchange', () => {
        if (refreshing || !hadController) return;
        refreshing = true;
        window.location.reload();
    });

    window.addEventListener('load', () => {
        navigator.serviceWorker.register('./service-worker.js').then(reg => {
            // Check for a newer version on every load.
            reg.update().catch(() => {});
            // If an updated worker is already waiting, activate it now.
            if (reg.waiting) reg.waiting.postMessage('SKIP_WAITING');
            reg.addEventListener('updatefound', () => {
                const nw = reg.installing;
                if (!nw) return;
                nw.addEventListener('statechange', () => {
                    if (nw.state === 'installed' && navigator.serviceWorker.controller) {
                        nw.postMessage('SKIP_WAITING');
                    }
                });
            });
        }).catch(() => { /* offline support is best-effort */ });
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

// Try multiple header aliases (e.g. "Status Unit" or short "Status") and
// return the first non-empty match. Lets CSVs use either the legacy long
// headers or the shorter ones produced by exportCSV.
function getValAny(row, keys) {
    for (const key of keys) {
        const v = getVal(row, key);
        if (v !== '' && v != null) return v;
    }
    return '';
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
    if ((view === 'editUnits' || view === 'implements' || view === 'damage' || view === 'licenseStock') && !canEdit()) {
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
    const damageView = document.getElementById('viewDamage');
    if (damageView) damageView.style.display = (view === 'damage') ? 'block' : 'none';
    const licenseView = document.getElementById('viewLicenseStock');
    if (licenseView) licenseView.style.display = (view === 'licenseStock') ? 'block' : 'none';
    const usersView = document.getElementById('viewUsers');
    if (usersView) usersView.style.display = (view === 'users') ? 'block' : 'none';

    document.querySelectorAll('.nav__link').forEach(el => el.classList.remove('active'));
    const activeLink = document.querySelector(`[data-view="${view}"]`);
    if (activeLink) {
        activeLink.classList.add('active');
        // Highlight the parent group toggle when an item inside a dropdown is active.
        activeLink.closest('.nav__group')?.querySelector('.nav__group-toggle')?.classList.add('active');
    }

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

    if (view === 'damage') {
        loadDamages();
        populateDamageUnitSelect();
        renderDamageTable();
    }

    if (view === 'licenseStock') {
        loadLicenseStock();
        populateLicenseTypeList();
        renderLicenseSummary();
        renderLicenseStockTable();
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

// ============================================================
// INDEXEDDB — ATTACHMENT STORAGE
// ============================================================

const ATTACH_DB_NAME = 'tractorAttachments';
const ATTACH_DB_VERSION = 1;
const ATTACH_STORE = 'files';
const ATTACH_MAX_SIZE = 5 * 1024 * 1024;
const ATTACH_MAX_PER_UNIT = 10;
const ATTACH_ALLOWED_EXT = ['.pdf', '.csv', '.doc', '.docx', '.xls', '.xlsx'];

let _attachDb = null;

function attachDbOpen() {
    if (_attachDb) return Promise.resolve(_attachDb);
    return new Promise((resolve, reject) => {
        if (!window.indexedDB) { reject(new Error('IndexedDB not available')); return; }
        const req = indexedDB.open(ATTACH_DB_NAME, ATTACH_DB_VERSION);
        req.onupgradeneeded = e => {
            const db = e.target.result;
            if (!db.objectStoreNames.contains(ATTACH_STORE)) {
                const store = db.createObjectStore(ATTACH_STORE, { keyPath: 'id' });
                store.createIndex('unitId', 'unitId', { unique: false });
            }
        };
        req.onsuccess = () => { _attachDb = req.result; resolve(_attachDb); };
        req.onerror = () => reject(req.error);
    });
}

function attachDbPut(record) {
    return attachDbOpen().then(db => new Promise((resolve, reject) => {
        const tx = db.transaction(ATTACH_STORE, 'readwrite');
        tx.objectStore(ATTACH_STORE).put(record);
        tx.oncomplete = () => resolve();
        tx.onerror = () => reject(tx.error);
    }));
}

function attachDbGet(id) {
    return attachDbOpen().then(db => new Promise((resolve, reject) => {
        const tx = db.transaction(ATTACH_STORE, 'readonly');
        const req = tx.objectStore(ATTACH_STORE).get(id);
        req.onsuccess = () => resolve(req.result || null);
        req.onerror = () => reject(req.error);
    }));
}

function attachDbDelete(ids) {
    if (!ids || ids.length === 0) return Promise.resolve();
    return attachDbOpen().then(db => new Promise((resolve, reject) => {
        const tx = db.transaction(ATTACH_STORE, 'readwrite');
        const store = tx.objectStore(ATTACH_STORE);
        ids.forEach(id => store.delete(id));
        tx.oncomplete = () => resolve();
        tx.onerror = () => reject(tx.error);
    }));
}

function attachDbGetByUnit(unitId) {
    return attachDbOpen().then(db => new Promise((resolve, reject) => {
        const tx = db.transaction(ATTACH_STORE, 'readonly');
        const idx = tx.objectStore(ATTACH_STORE).index('unitId');
        const req = idx.getAll(unitId);
        req.onsuccess = () => resolve(req.result || []);
        req.onerror = () => reject(req.error);
    }));
}

function generateAttachId() {
    return 'att_' + Date.now() + '_' + Math.random().toString(36).slice(2, 7);
}

function formatFileSize(bytes) {
    if (bytes < 1024) return bytes + ' B';
    if (bytes < 1024 * 1024) return (bytes / 1024).toFixed(1) + ' KB';
    return (bytes / (1024 * 1024)).toFixed(1) + ' MB';
}

function attachFileIcon(name) {
    const ext = (name || '').split('.').pop().toLowerCase();
    if (ext === 'pdf') return 'fa-file-pdf';
    if (ext === 'csv') return 'fa-file-csv';
    if (ext === 'doc' || ext === 'docx') return 'fa-file-word';
    if (ext === 'xls' || ext === 'xlsx') return 'fa-file-excel';
    return 'fa-file';
}

// ---- Attachment UI in Edit Table ----

let _currentAttachUnitId = null;
let _pendingAttachPurge = [];

function triggerAttachUpload(unitId) {
    _currentAttachUnitId = unitId;
    document.getElementById('attachFileInput').click();
}

async function handleAttachFileChange(e) {
    const files = e.target.files;
    if (!files || files.length === 0) return;
    const unitId = _currentAttachUnitId;
    if (!unitId) return;

    const unit = globalData.find(d => d.id === unitId);
    if (!unit) return;
    if (!unit.attachments) unit.attachments = [];

    const currentCount = unit.attachments.length;
    let added = 0;

    for (const file of files) {
        if (currentCount + added >= ATTACH_MAX_PER_UNIT) {
            showToast(`Maks ${ATTACH_MAX_PER_UNIT} file per unit`, 'warning');
            break;
        }
        const ext = '.' + file.name.split('.').pop().toLowerCase();
        if (!ATTACH_ALLOWED_EXT.includes(ext)) {
            showToast(`"${file.name}" — tipe tidak didukung (hanya PDF, CSV, Word, Excel)`, 'warning');
            continue;
        }
        if (file.size > ATTACH_MAX_SIZE) {
            showToast(`"${file.name}" — melebihi batas 5MB`, 'warning');
            continue;
        }

        const attId = generateAttachId();
        const meta = { id: attId, name: file.name, type: file.type, size: file.size, addedAt: new Date().toISOString() };
        try {
            await attachDbPut({ id: attId, unitId, name: file.name, type: file.type, size: file.size, addedAt: meta.addedAt, blob: file });
            unit.attachments.push(meta);
            added++;
        } catch (err) {
            showToast(`Gagal menyimpan "${file.name}": ${err.message}`, 'error');
        }
    }

    if (added > 0) {
        saveToStorage(globalData);
        cloudPushUnits([unit]);
        renderEditTable();
        showToast(`${added} file dilampirkan`, 'success');
    }

    e.target.value = '';
    _currentAttachUnitId = null;
}

async function downloadAttachment(attId) {
    try {
        const record = await attachDbGet(attId);
        if (!record || !record.blob) {
            showToast('File tidak tersedia di perangkat ini — diunggah dari perangkat lain. Gunakan Export/Import Backup.', 'warning');
            return;
        }
        const blob = record.blob instanceof Blob ? record.blob : new Blob([record.blob], { type: record.type });
        const url = URL.createObjectURL(blob);
        const a = document.createElement('a');
        a.href = url;
        a.download = record.name;
        a.click();
        URL.revokeObjectURL(url);
    } catch (err) {
        showToast('Gagal mengunduh file: ' + err.message, 'error');
    }
}

async function removeAttachment(unitId, attId) {
    if (!confirm('Hapus lampiran ini?')) return;
    const unit = globalData.find(d => d.id === unitId);
    if (!unit) return;

    try {
        await attachDbDelete([attId]);
    } catch (err) { /* best effort */ }

    unit.attachments = (unit.attachments || []).filter(a => a.id !== attId);
    saveToStorage(globalData);
    cloudPushUnits([unit]);
    renderEditTable();
    showToast('Lampiran dihapus', 'success');
}

function renderAttachCell(d) {
    const atts = d.attachments || [];
    let html = '<div class="attach-cell">';
    if (atts.length === 0) {
        html += '<span class="attach-empty">No files</span>';
    }
    atts.forEach(a => {
        const ext = a.name.split('.').pop().toLowerCase();
        const shortName = a.name.length > 18 ? a.name.slice(0, 15) + '…' + a.name.slice(a.name.lastIndexOf('.')) : a.name;
        html += `<div class="attach-chip attach-chip--${escapeHtml(ext)}" title="${escapeHtml(a.name)} (${formatFileSize(a.size)})">
            <i class="fas ${attachFileIcon(a.name)}"></i>
            <span class="attach-chip__name">${escapeHtml(shortName)}</span>
            <button class="btn-icon attach-chip__dl" title="Download" onclick="downloadAttachment('${a.id}')"><i class="fas fa-download"></i></button>
            <button class="btn-icon attach-chip__rm" title="Hapus" onclick="removeAttachment('${escapeHtml(d.id)}','${a.id}')"><i class="fas fa-xmark"></i></button>
        </div>`;
    });
    html += `<button class="btn-icon attach-upload-btn" title="Upload file" onclick="triggerAttachUpload('${escapeHtml(d.id)}')"><i class="fas fa-paperclip"></i></button>`;
    html += '</div>';
    return html;
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

// Bulk-update existing units from parsed CSV rows, matched by serial number.
// Only non-empty CSV fields are applied so a partial CSV (e.g. just
// "Serial Number, Tahun Penerimaan") never blanks out other data.
// Storage/cloud/changelog are written once for the whole batch.
const CSV_UPDATABLE_FIELDS = [
    'name', 'model', 'implement', 'status', 'display', 'gps', 'steering', 'jdlink', 'site',
    'yearReceived', 'userCategory', 'gpsLicense', 'licenseDisplay',
    'gpsLicenseStartDate', 'gpsLicenseEndDate',
    'displayLicenseStartDate', 'displayLicenseEndDate', 'remarks'
];

function bulkUpdateUnitsFromCSV(parsedUnits) {
    const bySN = new Map();
    globalData.forEach(d => { const k = (d.sn || '').toLowerCase(); if (k) bySN.set(k, d); });

    let updated = 0, unchanged = 0;
    const failed = [];
    const changedUnits = [];

    parsedUnits.forEach(p => {
        const existing = bySN.get((p.sn || '').toLowerCase());
        if (!existing) {
            failed.push({ sn: p.sn, reason: 'Serial number not found' });
            return;
        }

        const idx = globalData.findIndex(d => d.id === existing.id);
        if (idx === -1) {
            failed.push({ sn: p.sn, reason: 'Unit not found' });
            return;
        }

        const before = { ...globalData[idx] };
        const fields = {};
        CSV_UPDATABLE_FIELDS.forEach(f => {
            const val = p[f];
            if (val !== undefined && val !== null && String(val).trim() !== '' && val !== before[f]) {
                fields[f] = val;
            }
        });

        if (Object.keys(fields).length === 0) {
            unchanged++;
            return;
        }

        const unit = { ...before, ...fields };
        if (fields.status !== undefined && fields.status !== before.status) {
            trackStatusChange(unit, before.status, fields.status);
        }

        globalData[idx] = unit;
        Object.keys(fields).forEach(field => {
            logEvent({
                action: 'update',
                unitId: unit.id,
                unitName: unit.name,
                field,
                before: before[field],
                after: fields[field]
            });
        });
        changedUnits.push(unit);
        updated++;
    });

    if (changedUnits.length > 0) {
        saveToStorage(globalData);
        cloudPushUnits(changedUnits);
        recordChange({ type: 'updated', detail: `${changedUnits.length} unit(s) updated via CSV import` });
    }

    return { updated, unchanged, failed };
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
        _pendingAttachPurge = removed.flatMap(u => (u.attachments || []).map(a => a.id));
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

async function exportBackup() {
    const includeFiles = globalData.some(u => u.attachments && u.attachments.length > 0)
        && confirm('Sertakan file lampiran dalam backup? (ukuran file bisa besar)');

    const payload = {
        version: 3,
        exportedAt: new Date().toISOString(),
        count: globalData.length,
        units: globalData,
        // v3: the full dataset, not just units.
        implements: globalImplements,
        damages: globalDamages,
        licenseStock: globalLicenseStock
    };

    if (includeFiles) {
        const attachArr = [];
        for (const unit of globalData) {
            if (!unit.attachments || unit.attachments.length === 0) continue;
            for (const meta of unit.attachments) {
                try {
                    const rec = await attachDbGet(meta.id);
                    if (rec && rec.blob) {
                        const b64 = await new Promise((resolve, reject) => {
                            const reader = new FileReader();
                            reader.onload = () => resolve(reader.result);
                            reader.onerror = () => reject(reader.error);
                            reader.readAsDataURL(rec.blob instanceof Blob ? rec.blob : new Blob([rec.blob], { type: rec.type }));
                        });
                        attachArr.push({ id: meta.id, unitId: unit.id, name: meta.name, type: meta.type, size: meta.size, dataB64: b64 });
                    }
                } catch (e) { /* skip unavailable */ }
            }
        }
        if (attachArr.length > 0) payload.attachments = attachArr;
    }

    const blob = new Blob([JSON.stringify(payload, null, 2)], { type: 'application/json' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = `tractor_backup_${new Date().toISOString().slice(0, 10)}.json`;
    a.click();
    URL.revokeObjectURL(url);
    showToast(`Backup exported: ${globalData.length} units, ${globalImplements.length} implements, ${globalDamages.length} kerusakan, ${globalLicenseStock.length} transaksi lisensi`, 'success');
}

// ---- localStorage usage guard ----
// Damage photos (base64) are the main storage driver; warn before the ~5MB
// quota is hit so the user can export a backup / prune old photos in time.
let _storageWarned = false;
const STORAGE_WARN_BYTES = 4.5 * 1024 * 1024;

function estimateLocalStorageBytes() {
    let total = 0;
    try {
        for (let i = 0; i < localStorage.length; i++) {
            const k = localStorage.key(i);
            total += (k.length + (localStorage.getItem(k) || '').length) * 2; // UTF-16
        }
    } catch (e) { /* ignore */ }
    return total;
}

function checkStorageUsage() {
    if (_storageWarned) return;
    const used = estimateLocalStorageBytes();
    if (used > STORAGE_WARN_BYTES) {
        _storageWarned = true;
        showToast(`Penyimpanan browser hampir penuh (${(used / 1048576).toFixed(1)} MB terpakai) — export Backup sekarang & pertimbangkan menghapus foto kerusakan lama`, 'warning');
    }
}

function triggerRestore() {
    document.getElementById('restoreFileInput').click();
}

// Merge (union by id, backup wins) or replace one auxiliary collection from a
// backup file, mirroring the mode chosen for units.
function _restoreCollection(items, current, merge) {
    const valid = items.filter(r => r && r.id);
    if (!merge) return valid;
    const map = new Map(current.map(r => [r.id, r]));
    valid.forEach(r => map.set(r.id, r));
    return [...map.values()];
}

function importBackup(file) {
    const reader = new FileReader();
    reader.onload = async e => {
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

            // v3 backups carry the other collections too — restore them with
            // the same mode (merge / replace) the user chose for units.
            const extras = [];
            if (Array.isArray(data.implements)) {
                globalImplements = _restoreCollection(data.implements, globalImplements, merge);
                saveImplements();
                if (window.cloud?.isReady) window.cloud.saveImplements(globalImplements).catch(() => {});
                extras.push(`${data.implements.length} implements`);
            }
            if (Array.isArray(data.damages)) {
                globalDamages = _restoreCollection(data.damages, globalDamages, merge);
                saveDamages();
                if (window.cloud?.isReady) window.cloud.saveDamages(globalDamages).catch(() => {});
                extras.push(`${data.damages.length} kerusakan`);
            }
            if (Array.isArray(data.licenseStock)) {
                globalLicenseStock = _restoreCollection(data.licenseStock, globalLicenseStock, merge);
                saveLicenseStockLocal();
                if (window.cloud?.isReady) window.cloud.saveLicenses(globalLicenseStock).catch(() => {});
                extras.push(`${data.licenseStock.length} transaksi lisensi`);
            }
            if (extras.length) showToast(`Ikut direstore: ${extras.join(', ')}`, 'success');

            if (Array.isArray(data.attachments) && data.attachments.length > 0) {
                let restored = 0;
                for (const att of data.attachments) {
                    try {
                        const parts = att.dataB64.split(',');
                        const byteStr = atob(parts.length > 1 ? parts[1] : parts[0]);
                        const bytes = new Uint8Array(byteStr.length);
                        for (let i = 0; i < byteStr.length; i++) bytes[i] = byteStr.charCodeAt(i);
                        const blob = new Blob([bytes], { type: att.type });
                        await attachDbPut({ id: att.id, unitId: att.unitId, name: att.name, type: att.type, size: att.size, addedAt: new Date().toISOString(), blob });
                        restored++;
                    } catch (err) { /* skip invalid */ }
                }
                if (restored > 0) showToast(`${restored} lampiran berhasil di-restore`, 'success');
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
                    backgroundColor: '#D97757', borderRadius: 4, barPercentage: 0.6
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
            implement: clean(getVal(r, 'Implement')),
            status: clean(getValAny(r, ['Status Unit', 'Status'])),
            display: clean(getValAny(r, ['Status Unit Display', 'Display'])),
            gps: clean(getValAny(r, ['Status Unit GPS', 'GPS'])),
            steering: clean(getValAny(r, ['Status Unit Steering', 'Steering'])),
            jdlink: clean(getValAny(r, ['Status Unit JDLink', 'JDLink'])),
            site: clean(getVal(r, 'Site')),
            yearReceived: clean(getVal(r, 'Tahun Penerimaan')) || clean(getVal(r, 'Year Received')),
            userCategory: clean(getVal(r, 'User Category')),
            gpsLicense: clean(getVal(r, 'GPS License')),
            licenseDisplay: clean(getValAny(r, ['License Display', 'Display License'])),
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

    _tryAutoDailyEmail();
}

function updateDashboard(data) {
    renderNarrative(data);
    renderKPI(data);
    renderStatusChart(data);
    renderSiteChart(data);
    renderLicenseAlerts(data);
    renderStockAlerts();
    renderComponentHealth(data);
    renderDowntimeKPIs();
    renderTable(data);
    renderRepair();
    renderDamageStats();
    updateFilterCount(data);
}

// ---- Narrative summary sentence (editorial style) ----
function renderNarrative(data) {
    const el = document.getElementById('dashNarrative');
    if (!el) return;
    if (!data.length) { el.style.display = 'none'; return; }

    const total = data.length;
    const sites = [...new Set(data.map(d => d.site).filter(Boolean))].length || 1;
    const breakdown = data.filter(d => !isGood(d.status)).length;
    const issues = data.filter(d => detectIssues(d).length > 0).length;
    const alerts = _buildAlertList().total;

    const clauses = [
        breakdown === 0 ? 'semua unit beroperasi hari ini' : `${breakdown} unit sedang breakdown`
    ];
    if (issues > 0) clauses.push(`${issues} berjalan dengan gangguan komponen`);
    if (alerts > 0) clauses.push(`${alerts} lisensi akan expire dalam 30 hari ke depan`);

    let tail;
    if (clauses.length === 1) tail = clauses[0];
    else tail = clauses.slice(0, -1).join(', ') + ', dan ' + clauses[clauses.length - 1];

    el.textContent = `${total} unit di ${sites} site — ${tail}.`;
    el.style.display = '';
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
            datasets: [{ data: [good, breakdown], backgroundColor: ['#4F7B58', '#BF4D43'], borderWidth: 0, hoverOffset: 6 }]
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
                { label: 'Good', data: labels.map(s => siteMap[s].good), backgroundColor: '#4F7B58', borderRadius: 4, barPercentage: 0.6 },
                { label: 'Breakdown', data: labels.map(s => siteMap[s].breakdown), backgroundColor: '#BF4D43', borderRadius: 4, barPercentage: 0.6 }
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

// ---- License Alerts ----
function renderLicenseAlerts(data) {
    const section = document.getElementById('licenseAlertsSection');
    const container = document.getElementById('licenseAlertsCards');
    const summary = document.getElementById('licenseAlertsSummary');

    const SOON_DAYS = 30;
    const alerts = [];

    data.forEach(unit => {
        ['gps', 'display'].forEach(kind => {
            const end = getLicenseEndDate(unit, kind);
            if (!end) return;
            const s = getExpiryStatus(end);
            // Auto-downgraded premium (expired SF-RTK/G5 Advance) is now on its
            // stable fallback tier — no action needed, so drop it from alerts.
            if (effectiveLicense(unit, kind).downgraded) return;
            if (s.kind === 'expired' || (s.kind === 'soon' || (s.kind === 'ok' && s.daysLeft <= SOON_DAYS))) {
                const licName = kind === 'display'
                    ? (unit.licenseDisplay || 'Display')
                    : (unit.gpsLicense || 'GPS');
                alerts.push({
                    unit,
                    kind,
                    licName,
                    endDate: end,
                    status: s.kind === 'expired' ? 'expired' : 'soon',
                    daysLeft: s.daysLeft,
                    label: s.label
                });
            }
        });
    });

    if (alerts.length === 0) {
        section.style.display = 'none';
        return;
    }

    alerts.sort((a, b) => a.daysLeft - b.daysLeft);

    const expiredCount = alerts.filter(a => a.status === 'expired').length;
    const soonCount = alerts.filter(a => a.status === 'soon').length;

    section.style.display = '';
    summary.innerHTML = [
        expiredCount ? `<span class="la-chip expired"><i class="fas fa-circle-xmark"></i> ${expiredCount} Expired</span>` : '',
        soonCount ? `<span class="la-chip soon"><i class="fas fa-triangle-exclamation"></i> ${soonCount} Expiring ≤${SOON_DAYS}d</span>` : ''
    ].filter(Boolean).join('');

    container.innerHTML = alerts.map(a => {
        const meta = [a.licName, a.unit.model, a.unit.site, `Exp: ${a.endDate}`]
            .filter(Boolean).map(escapeHtml).join(' · ');
        return `
        <div class="la-card ${a.status}">
            <div class="la-card__icon">
                <i class="fas fa-${a.status === 'expired' ? 'circle-xmark' : 'triangle-exclamation'}"></i>
            </div>
            <div class="la-card__body">
                <div class="la-card__name">${escapeHtml(a.unit.name || a.unit.sn)}</div>
                <div class="la-card__meta">${meta}</div>
            </div>
            <div class="la-card__badge">${escapeHtml(a.label)}</div>
        </div>
    `;
    }).join('');
}

// ---- Low license stock (dashboard) ----
function _lowStockList() {
    const sum = computeLicenseSummary();
    return Object.keys(sum)
        .filter(t => (sum[t].in > 0 || sum[t].out > 0) && sum[t].sisa <= LICENSE_LOW_STOCK_THRESHOLD)
        .sort((a, b) => sum[a].sisa - sum[b].sisa)
        .map(t => ({ type: t, sisa: sum[t].sisa }));
}

function renderStockAlerts() {
    const section = document.getElementById('stockAlertsSection');
    const cards = document.getElementById('stockAlertsCards');
    if (!section || !cards) return;
    const low = _lowStockList();
    if (!low.length) { section.style.display = 'none'; return; }
    section.style.display = '';
    cards.innerHTML = low.map(l => `
        <div class="stock-alert-card ${l.sisa <= 0 ? 'empty' : ''}" onclick="navigateTo('licenseStock')" title="Buka Stok Lisensi">
            <i class="fas fa-${l.sisa <= 0 ? 'circle-xmark' : 'triangle-exclamation'}"></i>
            <span class="stock-alert-card__type">${escapeHtml(l.type)}</span>
            <span class="stock-alert-card__n">sisa ${l.sisa}</span>
        </div>`).join('');
}

// ---- Email Alert (EmailJS) ----
function _getEmailSettings() {
    try { return JSON.parse(localStorage.getItem('emailjs_settings') || '{}'); }
    catch { return {}; }
}

function openEmailSettingsModal() {
    const s = _getEmailSettings();
    document.getElementById('emailjsPublicKey').value  = s.publicKey  || '';
    document.getElementById('emailjsServiceId').value  = s.serviceId  || '';
    document.getElementById('emailjsTemplateId').value = s.templateId || '';
    document.getElementById('emailjsRecipient').value  = s.recipient  || '';
    document.getElementById('emailjsAutoDaily').checked = !!s.autoDaily;
    document.getElementById('emailSettingsModal').classList.add('open');
}
function closeEmailSettingsModal() {
    document.getElementById('emailSettingsModal').classList.remove('open');
}
function saveEmailSettings() {
    const s = {
        publicKey:  document.getElementById('emailjsPublicKey').value.trim(),
        serviceId:  document.getElementById('emailjsServiceId').value.trim(),
        templateId: document.getElementById('emailjsTemplateId').value.trim(),
        recipient:  document.getElementById('emailjsRecipient').value.trim(),
        autoDaily:  document.getElementById('emailjsAutoDaily').checked
    };
    if (!s.publicKey || !s.serviceId || !s.templateId || !s.recipient) {
        showToast('Please fill in all EmailJS fields', 'warning'); return;
    }
    localStorage.setItem('emailjs_settings', JSON.stringify(s));
    closeEmailSettingsModal();
    showToast('Email settings saved', 'success');
}

function _buildAlertList() {
    const SOON_DAYS = 30;
    const lines = [];
    let expiredCount = 0, soonCount = 0;

    globalData.forEach(unit => {
        ['gps', 'display'].forEach(kind => {
            const end = getLicenseEndDate(unit, kind);
            if (!end) return;
            const s = getExpiryStatus(end);
            if (effectiveLicense(unit, kind).downgraded) return; // on fallback tier — not an alert
            if (s.kind === 'expired' || s.kind === 'soon' || (s.kind === 'ok' && s.daysLeft <= SOON_DAYS)) {
                const licName = kind === 'display' ? (unit.licenseDisplay || 'Display') : (unit.gpsLicense || 'GPS');
                const tag = s.kind === 'expired' ? 'EXPIRED' : 'EXPIRING';
                if (s.kind === 'expired') expiredCount++; else soonCount++;
                lines.push({
                    daysLeft: s.daysLeft,
                    text: `${tag} | ${unit.name || '-'} | ${unit.model || '-'} | ${unit.sn || '-'} | ${licName} | ${unit.site || '-'} | ${end} | ${s.label}`
                });
            }
        });
    });
    lines.sort((a, b) => a.daysLeft - b.daysLeft);
    return { expiredCount, soonCount, total: lines.length, lines: lines.map(l => l.text) };
}

function sendLicenseAlertEmail() {
    const s = _getEmailSettings();
    if (!s.publicKey || !s.serviceId || !s.templateId || !s.recipient) {
        showToast('Setup EmailJS first — click the gear icon', 'warning');
        openEmailSettingsModal();
        return;
    }

    const report = _buildAlertList();
    if (report.total === 0) {
        showToast('No license alerts to send', 'info'); return;
    }

    const today = new Date().toLocaleDateString('id-ID', { weekday:'long', year:'numeric', month:'long', day:'numeric' });
    const lowStock = _lowStockList();
    const body = [
        `License Alert Report — ${today}`,
        `Expired: ${report.expiredCount} | Expiring soon: ${report.soonCount}`,
        '',
        'Status | Unit | Model | Serial Number | License | Site | Expiry Date | Remaining',
        '—'.repeat(60),
        ...report.lines,
        ...(lowStock.length ? ['', 'STOK LISENSI MENIPIS', ...lowStock.map(l => `${l.type} | sisa ${l.sisa}`)] : [])
    ].join('\n');

    const btn = document.getElementById('btnEmailAlert');
    btn.disabled = true;
    btn.innerHTML = '<i class="fas fa-spinner fa-spin"></i> Sending…';

    emailjs.init(s.publicKey);
    emailjs.send(s.serviceId, s.templateId, {
        to_email: s.recipient,
        subject: `License Alert: ${report.expiredCount} expired, ${report.soonCount} expiring soon`,
        message: body
    }).then(() => {
        showToast(`Alert report sent to ${s.recipient}`, 'success');
        localStorage.setItem('emailjs_last_sent', new Date().toDateString());
    }).catch(err => {
        console.error('[emailjs]', err);
        showToast('Failed to send email — check EmailJS settings', 'error');
    }).finally(() => {
        btn.disabled = false;
        btn.innerHTML = '<i class="fas fa-envelope"></i> Email Report';
    });
}

function _tryAutoDailyEmail() {
    const s = _getEmailSettings();
    if (!s.autoDaily || !s.publicKey) return;
    const lastSent = localStorage.getItem('emailjs_last_sent');
    if (lastSent === new Date().toDateString()) return;
    const report = _buildAlertList();
    if (report.total === 0) return;
    sendLicenseAlertEmail();
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
            <td><strong class="unit-link" title="Lihat profil unit" onclick="showUnitProfile('${escapeHtml(d.id)}')">${escapeHtml(d.name)}</strong></td>
            <td>${escapeHtml(d.model)}</td>
            <td style="font-family:monospace;font-size:12px">${escapeHtml(d.sn)}</td>
            <td>${escapeHtml(d.implement || '')}</td>
            <td>${!isGood(d.status) && d.breakdownReason
                ? `<span class="badge badge-breakdown bd-clickable" onclick="showBreakdownPopover(event, '${escapeHtml(d.breakdownReason).replace(/'/g, "\\'")}')"><i class="fas fa-xmark"></i> ${escapeHtml(d.status)}</span>`
                : `<span class="badge ${isGood(d.status) ? 'badge-good' : 'badge-breakdown'}"><i class="fas fa-${isGood(d.status) ? 'check' : 'xmark'}"></i> ${escapeHtml(d.status)}</span>`
            }</td>
            <td class="${isGood(d.display) ? 'cell-good' : 'cell-bad'}">${escapeHtml(d.display)}</td>
            <td class="${isGood(d.gps) ? 'cell-good' : 'cell-bad'}">${escapeHtml(d.gps)}</td>
            <td class="${isGood(d.steering) ? 'cell-good' : 'cell-bad'}">${escapeHtml(d.steering)}</td>
            <td class="${isGood(d.jdlink) ? 'cell-good' : 'cell-bad'}">${escapeHtml(d.jdlink)}</td>
            <td>${escapeHtml(d.site)}</td>
            <td>${escapeHtml(d.yearReceived || '') || '<span style="color:#a0aec0;font-size:11px">—</span>'}</td>
            <td>${d.userCategory ? `<span class="badge badge-cat" style="font-size:10px">${escapeHtml(d.userCategory)}</span>` : '<span style="color:#a0aec0;font-size:11px">—</span>'}</td>
            <td>${licenseTypeBadge(d, 'gps')}</td>
            <td>${licenseBadgeFor(d, 'gps')}</td>
            <td>${licenseTypeBadge(d, 'display')}</td>
            <td>${licenseBadgeFor(d, 'display')}</td>
        </tr>`;
    }).join('');
}

// ---- Sorting ----
function _resolveSortValue(d, key) {
    if (key === 'gpsExpiry') return getLicenseEndDate(d, 'gps');
    if (key === 'displayExpiry') return getLicenseEndDate(d, 'display');
    return d[key] || '';
}

function sortTable(key) {
    if (sortState.key === key) { sortState.asc = !sortState.asc; } else { sortState.key = key; sortState.asc = true; }
    if (key === 'no') { sortState.key = null; filteredData = [...applyFilterLogic()]; }
    else {
        filteredData.sort((a, b) => {
            const va = _resolveSortValue(a, key).toLowerCase(), vb = _resolveSortValue(b, key).toLowerCase();
            if (va < vb) return sortState.asc ? -1 : 1;
            if (va > vb) return sortState.asc ? 1 : -1;
            return 0;
        });
    }
    renderTable(filteredData);
    updateFilterCount(filteredData);
}

// ---- Repair & Maintenance ----
// ---- Damage statistics (dashboard) ----
function renderDamageStats() {
    const section = document.getElementById('damageStatsSection');
    if (!section) return;
    if (!globalDamages.length) {
        section.style.display = 'none';
        destroyChart('damageTrendChart');
        return;
    }
    section.style.display = '';

    const open = globalDamages.filter(r => !r.resolved).length;
    const totalEl = document.getElementById('damageStatsTotal');
    if (totalEl) totalEl.textContent = `${globalDamages.length} catatan · ${open} belum selesai`;

    // Counts per damage type
    const typeColors = { 'Mekanis': 'var(--danger)', 'Software': 'var(--info)', 'Device Precision': 'var(--warning)' };
    const counts = {};
    globalDamages.forEach(r => { const t = r.damageType || 'Lainnya'; counts[t] = (counts[t] || 0) + 1; });
    const typeOrder = DAMAGE_TYPES.concat(Object.keys(counts).filter(t => !DAMAGE_TYPES.includes(t)));
    document.getElementById('damageTypeChips').innerHTML = typeOrder
        .filter(t => counts[t])
        .map(t => `<div class="damage-type-chip"><span class="dot" style="background:${typeColors[t] || 'var(--text-light)'}"></span>${escapeHtml(t)}<strong>${counts[t]}</strong></div>`)
        .join('');

    // Monthly trend, last 6 months
    const months = [];
    const now = new Date();
    for (let i = 5; i >= 0; i--) {
        const d = new Date(now.getFullYear(), now.getMonth() - i, 1);
        months.push({
            key: `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, '0')}`,
            label: d.toLocaleDateString('id-ID', { month: 'short' })
        });
    }
    const byMonth = months.map(m => globalDamages.filter(r => (r.date || '').startsWith(m.key)).length);
    destroyChart('damageTrendChart');
    charts.damageTrendChart = new Chart(document.getElementById('damageTrendChart'), {
        type: 'bar',
        data: { labels: months.map(m => m.label), datasets: [{ data: byMonth, backgroundColor: '#D97757', borderRadius: 4, barPercentage: 0.55 }] },
        options: {
            plugins: { legend: { display: false } },
            scales: { y: { beginAtZero: true, ticks: { precision: 0 } } },
            maintainAspectRatio: false
        }
    });

    // Top-5 most frequently damaged units (live name via liveUnitFor)
    const perUnit = {};
    globalDamages.forEach(r => {
        const lu = liveUnitFor(r);
        const name = (lu ? lu.name : r.unitName) || '(tanpa nama)';
        perUnit[name] = (perUnit[name] || 0) + 1;
    });
    const top = Object.entries(perUnit).sort((a, b) => b[1] - a[1]).slice(0, 5);
    const max = top.length ? top[0][1] : 1;
    document.getElementById('damageTopUnits').innerHTML = top.map(([name, n]) => `
        <div class="damage-top-row">
            <span class="damage-top-row__name" title="${escapeHtml(name)}">${escapeHtml(name)}</span>
            <span class="damage-top-row__bar"><span style="width:${Math.round(n / max * 100)}%"></span></span>
            <span class="damage-top-row__n">${n}</span>
        </div>`).join('');
}

function renderRepair() {
    const issueFilterVal = document.getElementById('issueFilter').value;
    const issueData = countIssues(globalData);
    const chipColors = { Unit: '#BF4D43', Display: '#BC8A2E', GPS: '#5A7DA0', Steering: '#4F7B58', JDLink: '#403E3A' };

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
        data: { labels: siteLabels, datasets: [{ data: siteLabels.map(s => siteCounts[s]), backgroundColor: '#BC8A2E', borderRadius: 4, barPercentage: 0.5 }] },
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

function exportCSV(data) {
    const exportData = Array.isArray(data) ? data : filteredData;
    if (exportData.length === 0) { showToast('No data to export', 'warning'); return; }
    const headers = ['No', 'Nickname', 'Model', 'Serial Number', 'Implement', 'Status', 'Display', 'GPS', 'Steering', 'JDLink', 'Site',
                     'Tahun Penerimaan', 'User Category', 'GPS License', 'Display License',
                     'GPS License Start Date', 'GPS License Expiration Date',
                     'Display License Start Date', 'Display License Expiration Date', 'Remarks'];
    const rows = exportData.map((d, i) => [i + 1, d.name, d.model, d.sn, d.implement || '', d.status, d.display, d.gps, d.steering, d.jdlink, d.site,
                     d.yearReceived || '', d.userCategory || '',
                     effectiveLicense(d, 'gps').type || '', effectiveLicense(d, 'display').type || '',
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
    showToast(`Exported ${exportData.length} units to CSV`, 'success');
}

// Export the units currently visible in the Edit Units table (honors its
// search + status/site filters and sort). Falls back to all units when no
// filter is active.
function exportEditCSV() {
    exportCSV(getEditTableRows());
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

            // Split rows into brand-new units vs updates to existing SNs
            const existingSNs = new Set(globalData.map(d => (d.sn || '').toLowerCase()).filter(Boolean));
            const newUnits = [];
            const updateCandidates = [];
            valid.forEach(u => {
                if (existingSNs.has((u.sn || '').toLowerCase())) updateCandidates.push(u);
                else newUnits.push(u);
            });

            let updateResult = { updated: 0, unchanged: 0, failed: [] };
            let updatesAsSkipped = [];
            if (updateCandidates.length > 0) {
                const doUpdate = confirm(
                    `${updateCandidates.length} unit dengan Serial Number yang sama sudah ada di database.\n\n` +
                    `OK = UPDATE field yang terisi di CSV\n` +
                    `Cancel = SKIP (hanya tambah unit baru)`
                );
                if (doUpdate) {
                    updateResult = bulkUpdateUnitsFromCSV(updateCandidates);
                } else {
                    updatesAsSkipped = updateCandidates.map(u => ({ name: u.name, sn: u.sn, reason: 'Duplicate serial number' }));
                }
            }

            const { added, skipped, skippedDetails } = addUnits(newUnits);
            showImportReport({
                total: result.data.length, added,
                skipped: skipped + updatesAsSkipped.length,
                skippedDetails: [...skippedDetails, ...updatesAsSkipped],
                rejected,
                updated: updateResult.updated,
                unchanged: updateResult.unchanged,
                updateFailed: updateResult.failed
            });
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

function showImportReport({ total, added, skipped, skippedDetails, rejected, updated = 0, unchanged = 0, updateFailed = [] }) {
    const hasIssues = skipped > 0 || rejected.length > 0 || updateFailed.length > 0;
    const type = (added > 0 || updated > 0) ? (hasIssues ? 'warning' : 'success') : 'warning';

    const parts = [`${added} added`];
    if (updated > 0) parts.push(`${updated} updated`);
    if (unchanged > 0) parts.push(`${unchanged} unchanged`);
    if (skipped > 0) parts.push(`${skipped} duplicate(s) skipped`);
    if (rejected.length > 0) parts.push(`${rejected.length} rejected`);
    if (updateFailed.length > 0) parts.push(`${updateFailed.length} update failed`);
    const summary = parts.join(' · ');
    showToast(`Import: ${summary} (of ${total} rows)`, type);

    if (!hasIssues) return;

    const rows = [
        ...skippedDetails.map(d => `<tr><td>${escapeHtml(d.name || '-')}</td><td style="font-family:monospace">${escapeHtml(d.sn || '-')}</td><td>${escapeHtml(d.reason)}</td></tr>`),
        ...updateFailed.map(f => `<tr><td>-</td><td style="font-family:monospace">${escapeHtml(f.sn || '-')}</td><td>${escapeHtml(f.reason)}</td></tr>`),
        ...rejected.map(r => `<tr><td>${escapeHtml(r.name || '-')}</td><td>Row ${r.row}</td><td>${escapeHtml(r.reason)}</td></tr>`)
    ].join('');

    document.getElementById('importReportBody').innerHTML = rows;
    document.getElementById('importReportSummary').textContent = summary;
    document.getElementById('importReportModal').classList.add('open');
}

function closeImportReport() {
    document.getElementById('importReportModal').classList.remove('open');
}

// ---- Edit Table ----
// Compute the rows shown in the Edit Units table — honors the search box,
// status/site filters and the active sort. Shared by renderEditTable() and
// the Export CSV button so both stay in sync.
function getEditTableRows() {
    const query = (document.getElementById('editSearch')?.value || '').toLowerCase().trim();
    const statusVal = (document.getElementById('editStatusFilter')?.value || '');
    const siteVal = (document.getElementById('editSiteFilter')?.value || '');

    let rows = [...globalData];
    if (query) rows = rows.filter(d => `${d.name} ${d.model} ${d.sn} ${d.implement || ''} ${d.site}`.toLowerCase().includes(query));
    if (statusVal) rows = rows.filter(d => d.status === statusVal);
    if (siteVal) rows = rows.filter(d => d.site === siteVal);

    if (editSortState.key && editSortState.key !== 'no') {
        const k = editSortState.key;
        rows.sort((a, b) => {
            const va = _resolveSortValue(a, k).toLowerCase(), vb = _resolveSortValue(b, k).toLowerCase();
            if (va < vb) return editSortState.asc ? -1 : 1;
            if (va > vb) return editSortState.asc ? 1 : -1;
            return 0;
        });
    }
    return rows;
}

function toggleCompactMode() {
    const table = document.getElementById('editTable');
    if (!table) return;
    const isCompact = table.classList.toggle('compact');
    localStorage.setItem('editTableCompact', isCompact ? '1' : '');
    const icon = document.querySelector('#compactToggle i');
    if (icon) {
        icon.className = isCompact ? 'fas fa-expand' : 'fas fa-compress';
    }
}

function renderEditTable() {
    updateEditCount();
    selectedUnitIds.clear();
    updateSelectedCount();

    const selectAllBox = document.getElementById('selectAll');
    if (selectAllBox) selectAllBox.checked = false;

    const query = (document.getElementById('editSearch')?.value || '').toLowerCase().trim();
    const statusVal = (document.getElementById('editStatusFilter')?.value || '');
    const siteVal = (document.getElementById('editSiteFilter')?.value || '');

    const rows = getEditTableRows();

    const tbody = document.getElementById('editBody');
    if (rows.length === 0) {
        tbody.innerHTML = `<tr><td colspan="21" style="text-align:center;padding:24px;color:#718096">${(query || statusVal || siteVal) ? 'No units match your filters' : 'No units yet. Click <strong>Add Unit</strong> or <strong>Import CSV</strong> to get started.'}</td></tr>`;
        return;
    }

    tbody.innerHTML = rows.map((d, i) => {
        const remarks = d.remarks || '';
        const remarksShort = remarks.length > 40 ? remarks.slice(0, 40) + '…' : remarks;
        return `
        <tr>
            <td class="col-check"><input type="checkbox" class="unit-check" data-id="${escapeHtml(d.id)}" onchange="updateSelectedCount()"></td>
            <td>${i + 1}</td>
            <td data-label="Nickname"><span class="inline-edit" contenteditable="true" data-id="${escapeHtml(d.id)}" data-field="name" onblur="saveInlineEdit(this)">${escapeHtml(d.name)}</span></td>
            <td data-label="Model"><span class="inline-edit" contenteditable="true" data-id="${escapeHtml(d.id)}" data-field="model" onblur="saveInlineEdit(this)">${escapeHtml(d.model)}</span></td>
            <td data-label="SN" style="font-family:monospace;font-size:12px">${escapeHtml(d.sn)}</td>
            <td><span class="inline-edit" contenteditable="true" data-id="${escapeHtml(d.id)}" data-field="implement" onblur="saveInlineEdit(this)">${escapeHtml(d.implement || '')}</span></td>
            <td data-label="Status"><span class="inline-edit" contenteditable="true" data-id="${escapeHtml(d.id)}" data-field="status" onblur="saveInlineEdit(this)">${escapeHtml(d.status)}</span></td>
            <td><span class="inline-edit" contenteditable="true" data-id="${escapeHtml(d.id)}" data-field="display" onblur="saveInlineEdit(this)">${escapeHtml(d.display)}</span></td>
            <td><span class="inline-edit" contenteditable="true" data-id="${escapeHtml(d.id)}" data-field="gps" onblur="saveInlineEdit(this)">${escapeHtml(d.gps)}</span></td>
            <td><span class="inline-edit" contenteditable="true" data-id="${escapeHtml(d.id)}" data-field="steering" onblur="saveInlineEdit(this)">${escapeHtml(d.steering)}</span></td>
            <td><span class="inline-edit" contenteditable="true" data-id="${escapeHtml(d.id)}" data-field="jdlink" onblur="saveInlineEdit(this)">${escapeHtml(d.jdlink)}</span></td>
            <td data-label="Site"><span class="inline-edit" contenteditable="true" data-id="${escapeHtml(d.id)}" data-field="site" onblur="saveInlineEdit(this)">${escapeHtml(d.site)}</span></td>
            <td><span class="inline-edit" contenteditable="true" data-id="${escapeHtml(d.id)}" data-field="yearReceived" onblur="saveInlineEdit(this)">${escapeHtml(d.yearReceived || '')}</span></td>
            <td>${d.userCategory ? `<span class="badge badge-cat" style="font-size:10px">${escapeHtml(d.userCategory)}</span>` : '<span style="color:#a0aec0;font-size:11px">—</span>'}</td>
            <td>${licenseTypeBadge(d, 'gps')}</td>
            <td>${licenseBadgeFor(d, 'gps')}</td>
            <td>${licenseTypeBadge(d, 'display')}</td>
            <td>${licenseBadgeFor(d, 'display')}</td>
            <td style="max-width:180px;font-size:12px;color:#4a5568" title="${escapeHtml(remarks)}">${escapeHtml(remarksShort) || '<span style="color:#a0aec0">—</span>'}</td>
            <td class="col-attach">${renderAttachCell(d)}</td>
            <td class="col-actions">
                <div class="row-actions">
                    <button class="btn btn-secondary" title="Profil" onclick="showUnitProfile('${escapeHtml(d.id)}')"><i class="fas fa-eye"></i></button>
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
        if (_pendingAttachPurge.length > 0) {
            attachDbDelete(_pendingAttachPurge).catch(() => {});
            _pendingAttachPurge = [];
        }
    }, 10000);
}

function undoDelete() {
    if (!lastDeletedUnits || lastDeletedUnits.length === 0) return;
    _pendingAttachPurge = [];
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
// ---- Implement picker (unit form ↔ Implements database) ----
// Options read "equipmentType — brand" (e.g. "HDR Ripper — Gessner"). Free
// text is still allowed, so legacy values, inline edits and CSV imports keep
// working unchanged.
function implementOptionLabel(imp) {
    const parts = [imp.equipmentType, imp.brand].map(v => (v || '').trim()).filter(Boolean);
    return parts.length ? parts.join(' — ') : (imp.profileName || '').trim();
}

// The previous "profileName — equipmentType" label, kept so units saved with
// the old format still resolve back to their implement.
function _implementLegacyLabel(imp) {
    return `${imp.profileName || ''}${imp.equipmentType ? ' — ' + imp.equipmentType : ''}`.trim();
}

function populateImplementUnitList() {
    const list = document.getElementById('implementUnitList');
    if (!list) return;
    const labels = [...new Set(globalImplements
        .map(implementOptionLabel)
        .filter(Boolean))]
        .sort((a, b) => a.localeCompare(b));
    list.innerHTML = labels.map(l => `<option value="${escapeHtml(l)}"></option>`).join('');
}

// Find the implement record a unit's free-text implement value refers to.
function matchImplementForUnit(text) {
    const t = (text || '').toLowerCase().trim();
    if (!t) return null;
    return globalImplements.find(imp =>
        implementOptionLabel(imp).toLowerCase() === t ||
        _implementLegacyLabel(imp).toLowerCase() === t ||
        (imp.profileName || '').toLowerCase() === t) || null;
}

function showAddForm() {
    if (!requireEdit()) return;
    document.getElementById('modalTitle').textContent = 'Add Unit';
    document.getElementById('editUnitId').value = '';
    document.getElementById('unitForm').reset();
    renderUserCategoryOptions();
    populateImplementUnitList();
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
    populateImplementUnitList();
    document.getElementById('formImplement').value = unit.implement || '';
    document.getElementById('formSite').value = unit.site;
    document.getElementById('formYearReceived').value = unit.yearReceived || '';
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
        implement: document.getElementById('formImplement').value.trim(),
        site: document.getElementById('formSite').value.trim(),
        yearReceived: document.getElementById('formYearReceived').value.trim(),
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

// Auto-downgrade: a premium license that has EXPIRED falls back to the lower
// tier (GPS SF-RTK → SF-1, Display G5 Advance → G5 Basic). Display-only —
// the stored value stays as-is; renewing the date brings the premium tier back.
function effectiveLicense(unit, kind) {
    const rawType = kind === 'display' ? (unit.licenseDisplay || '') : (unit.gpsLicense || '');
    const end = getLicenseEndDate(unit, kind);
    const status = getExpiryStatus(end);
    const premium  = kind === 'display' ? 'G5 Advance' : 'SF-RTK';
    const fallback = kind === 'display' ? 'G5 Basic'   : 'SF-1';
    const downgraded = rawType === premium && status.kind === 'expired';
    return { type: downgraded ? fallback : rawType, rawType, premium, fallback, downgraded, status, end };
}

// Render the license-type badge for a table cell, showing the effective tier
// (with a small marker + tooltip when auto-downgraded).
function licenseTypeBadge(unit, kind) {
    const eff = effectiveLicense(unit, kind);
    if (!eff.type) return '<span style="color:#a0aec0;font-size:11px">—</span>';
    if (eff.downgraded) {
        const tt = `Otomatis turun dari ${eff.premium} (expired ${eff.end})`;
        return `<span class="badge badge-cat" style="font-size:10px" title="${escapeHtml(tt)}"><i class="fas fa-arrow-turn-down" style="font-size:9px;opacity:.7"></i> ${escapeHtml(eff.type)}</span>`;
    }
    return `<span class="badge badge-good" style="font-size:10px">${escapeHtml(eff.type)}</span>`;
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
    // Auto-downgraded premium → show a neutral "on fallback tier" badge instead
    // of a red expired one (the receiver still works on SF-1 / G5 Basic).
    const eff = effectiveLicense(unit, kind);
    if (eff.downgraded) {
        const tt = `Auto-fallback dari ${eff.premium} (expired ${end})`;
        return `<span class="license-badge license-badge--ok" title="${escapeHtml(tt)}"><i class="fas fa-circle-check"></i> ${escapeHtml(eff.fallback)}</span>`;
    }
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
    { key: 'brand',              inputId: 'implBrand',              label: 'Brand' },
    { key: 'equipmentType',      inputId: 'implEquipmentType',      label: 'Type of Equipment' },
    { key: 'code',               inputId: 'implCode',               label: 'Code' },
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
            `${d.profileName} ${d.brand || ''} ${d.equipmentType} ${d.code || ''} ${d.operation} ${d.connectingType} ${d.workingWidth}`
                .toLowerCase().includes(query))
        : globalImplements;

    const tbody = document.getElementById('implementBody');
    if (!tbody) return;

    if (rows.length === 0) {
        tbody.innerHTML = `<tr><td colspan="11" style="text-align:center;padding:24px;color:#718096">${
            query ? 'No implements match your search'
                  : 'No implements yet. Click <strong>Add Implement</strong> to get started.'
        }</td></tr>`;
        return;
    }

    tbody.innerHTML = rows.map((d, i) => {
        const coaList = Array.isArray(d.chartOfAccounts) ? d.chartOfAccounts.filter(Boolean) : [];
        let coaCell;
        if (!coaList.length) {
            coaCell = '<span style="color:#a0aec0;font-size:11px">—</span>';
        } else if (coaList.length === 1) {
            coaCell = `<span class="badge badge-cat" style="font-size:10px">${escapeHtml(coaList[0])}</span>`;
        } else {
            const first = `<span class="badge badge-cat" style="font-size:10px">${escapeHtml(coaList[0])}</span>`;
            const rest = coaList.slice(1).map(c => `<span class="badge badge-cat" style="font-size:10px;margin:2px 0">${escapeHtml(c)}</span>`).join('');
            coaCell = `<div class="coa-cell">${first}<span class="coa-more" onclick="this.parentElement.classList.toggle('open');event.stopPropagation()">+${coaList.length - 1} more</span><div class="coa-dropdown">${rest}</div></div>`;
        }
        return `
        <tr>
            <td class="col-check"><input type="checkbox" class="impl-check" data-id="${escapeHtml(d.id)}" onchange="updateSelectedImplementCount()"></td>
            <td>${i + 1}</td>
            <td>${escapeHtml(d.profileName)}</td>
            <td>${escapeHtml(d.brand || '')}</td>
            <td>${escapeHtml(d.equipmentType)}</td>
            <td>${escapeHtml(d.code || '')}</td>
            <td>${escapeHtml(d.workingWidth)}</td>
            <td>${escapeHtml(d.operation)}</td>
            <td>${escapeHtml(d.connectingType)}</td>
            <td style="max-width:240px;white-space:nowrap">${coaCell}</td>
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

// ---- Implements CSV: export / template / import ----
// Header aliases accepted on import (besides the canonical label).
function _implementColAliases(field) {
    const map = {
        equipmentType: ['Type of Equipment', 'Equipment Type'],
        profileName:   ['Profile Name', 'Profile'],
        workingWidth:  ['Working Width'],
        connectingType:['Connecting Type']
    };
    return map[field.key] || [field.label];
}

function exportImplementsCSV() {
    if (globalImplements.length === 0) { showToast('No implements to export', 'warning'); return; }
    const headers = ['No', ...IMPLEMENT_FIELDS.map(f => f.label), 'Chart of Account'];
    const rows = globalImplements.map((d, i) => [
        i + 1,
        ...IMPLEMENT_FIELDS.map(f => d[f.key] || ''),
        (Array.isArray(d.chartOfAccounts) ? d.chartOfAccounts.filter(Boolean) : []).join('; ')
    ]);
    const csv = [headers, ...rows].map(row =>
        row.map(cell => `"${String(cell).replace(/"/g, '""')}"`).join(',')).join('\n');
    const blob = new Blob(['﻿' + csv], { type: 'text/csv;charset=utf-8;' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = `implements_${new Date().toISOString().slice(0, 10)}.csv`;
    a.click();
    URL.revokeObjectURL(url);
    showToast(`Exported ${globalImplements.length} implements to CSV`, 'success');
}

function downloadImplementTemplate() {
    const headers = ['No', ...IMPLEMENT_FIELDS.map(f => f.label), 'Chart of Account'];
    // One example row (No is ignored on import).
    const sample = ['1', 'JNR Leopard E 10.0', 'John Deere', 'Scooping', 'IMP-001', '0 m', '1.2 m', '0.8 m',
        'Tillage', '3', 'Center', 'Manual', 'Drawbar', 'iGrade',
        '159361_Scooping; 159201_Offset Harrow'];
    const csv = [headers, sample].map(row =>
        row.map(cell => `"${String(cell).replace(/"/g, '""')}"`).join(',')).join('\n');
    const blob = new Blob(['﻿' + csv], { type: 'text/csv;charset=utf-8;' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = 'template_implements.csv';
    a.click();
    URL.revokeObjectURL(url);
}

function handleImplementCSVImport(file) {
    if (!requireEdit()) return;
    showLoading(true);
    Papa.parse(file, {
        header: true,
        skipEmptyLines: true,
        complete: result => {
            const added = [];
            let rejected = 0;
            result.data.forEach(row => {
                const obj = { id: generateImplementId() };
                IMPLEMENT_FIELDS.forEach(f => {
                    obj[f.key] = (getValAny(row, _implementColAliases(f)) || '').toString().trim();
                });
                if (!obj.profileName) { rejected++; return; }
                const coaRaw = (getValAny(row, ['Chart of Account', 'Chart of Accounts', 'COA']) || '').toString();
                obj.chartOfAccounts = coaRaw.split(/[;\n]/).map(s => s.trim()).filter(Boolean);
                obj.createdAt = Date.now();
                obj.updatedAt = Date.now();
                added.push(obj);
            });

            if (added.length > 0) {
                globalImplements.push(...added);
                saveImplements();
                if (!suppressCloudWrites && window.cloud?.isReady) {
                    window.cloud.saveImplements(added).catch(err => {
                        console.error('[cloud] import implements failed:', err);
                        showToast('Cloud sync gagal — data tersimpan lokal', 'warning');
                    });
                }
                logEvent({ action: 'add', unitName: '[Implement] Import CSV', after: `${added.length} implement` });
                renderImplementsTable();
            }

            showLoading(false);
            const msg = `Import implement: ${added.length} ditambahkan` + (rejected ? `, ${rejected} dilewati (Profile Name kosong)` : '');
            showToast(msg, added.length ? 'success' : 'warning');
        },
        error: err => {
            showToast('Gagal membaca CSV: ' + err.message, 'error');
            showLoading(false);
        }
    });
}

// ============================================================
// DAMAGE LOG (KERUSAKAN)
// ============================================================

function generateDamageId() {
    return 'dmg_' + Date.now() + '_' + Math.random().toString(36).substr(2, 5);
}

// ---- Photo: compress/resize an image File to a JPEG data URL ----
// Resizes to DAMAGE_PHOTO_MAX_DIM on the longest side, then lowers quality
// until the data URL fits under DAMAGE_PHOTO_MAX_BYTES (Firestore 1MB doc cap).
function compressImageToDataURL(file) {
    return new Promise((resolve, reject) => {
        const reader = new FileReader();
        reader.onerror = () => reject(new Error('Gagal membaca file'));
        reader.onload = () => {
            const img = new Image();
            img.onerror = () => reject(new Error('File bukan gambar yang valid'));
            img.onload = () => {
                let { width, height } = img;
                const max = DAMAGE_PHOTO_MAX_DIM;
                if (width > height && width > max) { height = Math.round(height * max / width); width = max; }
                else if (height > max) { width = Math.round(width * max / height); height = max; }
                const canvas = document.createElement('canvas');
                canvas.width = width; canvas.height = height;
                canvas.getContext('2d').drawImage(img, 0, 0, width, height);
                let q = DAMAGE_PHOTO_QUALITY;
                let out = canvas.toDataURL('image/jpeg', q);
                while (out.length > DAMAGE_PHOTO_MAX_BYTES && q > 0.3) {
                    q -= 0.1;
                    out = canvas.toDataURL('image/jpeg', q);
                }
                resolve(out);
            };
            img.src = reader.result;
        };
        reader.readAsDataURL(file);
    });
}

async function handleDamagePhotoChange(event) {
    const file = event.target.files && event.target.files[0];
    event.target.value = '';
    if (!file) return;
    if (!file.type.startsWith('image/')) {
        showToast('File harus berupa gambar (foto)', 'warning');
        return;
    }
    try {
        _dmgPhotoData = await compressImageToDataURL(file);
        setDamagePhotoPreview();
    } catch (err) {
        showToast(err.message || 'Gagal memproses foto', 'error');
    }
}

function setDamagePhotoPreview() {
    const wrap = document.getElementById('dmgPhotoPreviewWrap');
    const img = document.getElementById('dmgPhotoPreview');
    if (!wrap || !img) return;
    if (_dmgPhotoData) {
        img.src = _dmgPhotoData;
        wrap.style.display = '';
    } else {
        img.removeAttribute('src');
        wrap.style.display = 'none';
    }
}

function removeDamagePhoto() {
    _dmgPhotoData = '';
    const input = document.getElementById('dmgPhotoInput');
    if (input) input.value = '';
    setDamagePhotoPreview();
}

// ---- Lightbox (view full-size photo) ----
function openPhotoLightbox(src) {
    if (!src) return;
    const box = document.getElementById('photoLightbox');
    const img = document.getElementById('photoLightboxImg');
    if (!box || !img) return;
    img.src = src;
    box.classList.add('open');
}

function closePhotoLightbox() {
    const box = document.getElementById('photoLightbox');
    if (box) box.classList.remove('open');
}

// ============================================================
// UNIT PROFILE (Profil Unit) — one panel with everything about a unit
// ============================================================

function closeUnitProfile() {
    document.getElementById('unitProfileModal').classList.remove('open');
}

// ---- Global unit search (topbar) — jumps straight to a unit's profile ----
function closeGlobalSearch() {
    const box = document.getElementById('globalSearchResults');
    if (box) { box.innerHTML = ''; box.style.display = 'none'; }
}

function renderGlobalSearchResults() {
    const input = document.getElementById('globalSearch');
    const box = document.getElementById('globalSearchResults');
    if (!input || !box) return;
    const q = input.value.toLowerCase().trim();
    if (!q) { closeGlobalSearch(); return; }
    const hits = globalData.filter(u =>
        `${u.name} ${u.sn} ${u.model} ${u.site}`.toLowerCase().includes(q)).slice(0, 8);
    if (!hits.length) {
        box.innerHTML = '<div class="global-search__empty">Tidak ada unit yang cocok</div>';
        box.style.display = '';
        return;
    }
    box.innerHTML = hits.map(u => `
        <div class="global-search__item" onclick="openUnitFromSearch('${escapeHtml(u.id)}')">
            <span class="global-search__name">${escapeHtml(u.name || '(tanpa nama)')}</span>
            <span class="global-search__meta"><span class="mono">${escapeHtml(u.sn || '')}</span>${u.site ? ' · ' + escapeHtml(u.site) : ''}</span>
        </div>`).join('');
    box.style.display = '';
}

function openUnitFromSearch(id) {
    closeGlobalSearch();
    const input = document.getElementById('globalSearch');
    if (input) input.value = '';
    showUnitProfile(id);
}

function showUnitProfile(id) {
    const u = globalData.find(d => d.id === id);
    if (!u) { showToast('Unit tidak ditemukan', 'warning'); return; }

    document.getElementById('unitProfileTitle').textContent = u.name || u.sn || 'Profil Unit';
    document.getElementById('unitProfileEditBtn').onclick = () => { closeUnitProfile(); editUnit(id); };
    document.getElementById('unitProfileHistoryBtn').onclick = () => showHistory(id);

    const snLc = (u.sn || '').toLowerCase();
    const dash = '<span style="color:#a0aec0">—</span>';
    const val = v => v ? escapeHtml(v) : dash;

    // When the unit's implement text matches a record in the Implements
    // database, show its key specs under the value.
    const impMatch = matchImplementForUnit(u.implement);
    const impDetail = impMatch
        ? [impMatch.brand, impMatch.code, impMatch.workingWidth ? `WW ${impMatch.workingWidth}` : '']
            .filter(Boolean).map(escapeHtml).join(' · ')
        : '';
    const impVal = u.implement
        ? `<span style="text-align:right">${escapeHtml(u.implement)}${impDetail ? `<div style="font-size:11px;color:var(--text-light);margin-top:2px">${impDetail}</div>` : ''}</span>`
        : dash;

    const identity = [
        ['Model', val(u.model)],
        ['Serial Number', u.sn ? `<span style="font-family:var(--font-mono);font-size:12px">${escapeHtml(u.sn)}</span>` : dash],
        ['Implement', impVal],
        ['Site', val(u.site)],
        ['Tahun Penerimaan', val(u.yearReceived)],
        ['User Category', u.userCategory ? `<span class="badge badge-cat" style="font-size:10px">${escapeHtml(u.userCategory)}</span>` : dash]
    ].map(([l, v]) => `<div class="profile-row"><span class="profile-row__label">${l}</span><span>${v}</span></div>`).join('');

    const statusBadge = isGood(u.status)
        ? '<span class="badge badge-good"><i class="fas fa-check"></i> Good</span>'
        : `<span class="badge badge-breakdown"><i class="fas fa-xmark"></i> ${escapeHtml(u.status || 'Breakdown')}</span>`;
    const compRow = ['display', 'gps', 'steering', 'jdlink'].map(k => {
        const label = { display: 'Display', gps: 'GPS', steering: 'Steering', jdlink: 'JDLink' }[k];
        const good = isGood(u[k]);
        return `<div class="profile-comp ${good ? 'ok' : 'bad'}"><i class="fas fa-${good ? 'circle-check' : 'circle-xmark'}"></i> ${label}</div>`;
    }).join('');
    const bdReason = (!isGood(u.status) && u.breakdownReason)
        ? `<div class="profile-note"><i class="fas fa-triangle-exclamation"></i> ${escapeHtml(u.breakdownReason)}</div>` : '';

    const licenses = ['gps', 'display'].map(kind => {
        const label = kind === 'gps' ? 'GPS License' : 'Display License';
        const end = getLicenseEndDate(u, kind);
        return `<div class="profile-row"><span class="profile-row__label">${label}</span>
            <span style="display:flex;gap:8px;align-items:center;flex-wrap:wrap;justify-content:flex-end">${licenseTypeBadge(u, kind)} ${licenseBadgeFor(u, kind)}${end ? ` <span style="font-size:11.5px;color:var(--text-secondary)">exp ${escapeHtml(end)}</span>` : ''}</span></div>`;
    }).join('');

    const dmg = globalDamages
        .filter(r => r.unitId === id || (snLc && (r.sn || '').toLowerCase() === snLc))
        .sort((a, b) => (b.date || '').localeCompare(a.date || ''));
    const dmgHtml = dmg.length ? dmg.map(r => `
        <div class="profile-item">
            <span class="profile-item__date">${escapeHtml(r.date || '')}</span>
            <span class="badge badge-breakdown" style="font-size:10px">${escapeHtml(r.damageType || '')}</span>
            ${r.component ? `<span class="badge badge-cat" style="font-size:10px">${escapeHtml(r.component)}</span>` : ''}
            <span class="profile-item__text" title="${escapeHtml(r.description || '')}">${escapeHtml((r.description || '').slice(0, 60))}</span>
            ${r.photo ? `<img class="dmg-thumb" src="${r.photo}" alt="foto" onclick="openPhotoLightbox(this.src)">` : ''}
        </div>`).join('')
        : '<div class="profile-empty">Belum ada catatan kerusakan.</div>';

    const dist = globalLicenseStock
        .filter(r => r.txnType === 'OUT' && (r.unitId === id || (snLc && (r.sn || '').toLowerCase() === snLc)))
        .sort((a, b) => (b.date || '').localeCompare(a.date || ''));
    const distHtml = dist.length ? dist.map(r => `
        <div class="profile-item">
            <span class="profile-item__date">${escapeHtml(r.date || '')}</span>
            <span class="badge badge-good" style="font-size:10px">${escapeHtml(r.licenseType || '')}</span>
            <span style="font-size:12px">× ${Number(r.qty) || 0}</span>
            ${r.note ? `<span class="profile-item__text" title="${escapeHtml(r.note)}">${escapeHtml(r.note.slice(0, 40))}</span>` : ''}
        </div>`).join('')
        : '<div class="profile-empty">Belum ada distribusi lisensi.</div>';

    const hist = u.downtimeHistory || [];
    const downParts = hist.slice(-5).reverse().map(iv =>
        `<div class="profile-item"><span class="profile-item__date">${new Date(iv.start).toLocaleDateString()}</span><span style="font-size:12px">${formatDuration(iv.durationMs)}</span></div>`);
    if (u.breakdownStartedAt) {
        downParts.unshift(`<div class="profile-item"><span class="badge badge-breakdown" style="font-size:10px">Sedang breakdown</span><span style="font-size:12px">${formatDuration(Date.now() - u.breakdownStartedAt)}</span></div>`);
    }
    const downHtml = downParts.length ? downParts.join('') : '<div class="profile-empty">Tidak ada riwayat downtime.</div>';

    document.getElementById('unitProfileBody').innerHTML = `
        <div class="profile-grid">
            <div class="profile-section">
                <div class="profile-section__title">Identitas</div>
                ${identity}
                ${u.remarks ? `<div class="profile-note">${escapeHtml(u.remarks)}</div>` : ''}
            </div>
            <div class="profile-section">
                <div class="profile-section__title">Status</div>
                <div style="margin-bottom:10px">${statusBadge}</div>
                <div class="profile-comps">${compRow}</div>
                ${bdReason}
                <div class="profile-section__title" style="margin-top:16px">Lisensi</div>
                ${licenses}
            </div>
            <div class="profile-section">
                <div class="profile-section__title">Riwayat Kerusakan (${dmg.length})</div>
                <div class="profile-list">${dmgHtml}</div>
            </div>
            <div class="profile-section">
                <div class="profile-section__title">Distribusi Lisensi (${dist.length})</div>
                <div class="profile-list">${distHtml}</div>
            </div>
            <div class="profile-section">
                <div class="profile-section__title">Lampiran</div>
                <div class="profile-attach">${renderAttachCell(u)}</div>
            </div>
            <div class="profile-section">
                <div class="profile-section__title">Downtime</div>
                <div class="profile-list">${downHtml}</div>
            </div>
        </div>`;

    document.getElementById('unitProfileModal').classList.add('open');
}

// ---- Storage ----
function loadDamages() {
    try {
        const raw = localStorage.getItem(DAMAGE_STORAGE_KEY);
        globalDamages = raw ? JSON.parse(raw) : [];
    } catch (e) {
        globalDamages = [];
    }
    updateDamageCount();
    return globalDamages.length > 0;
}

function saveDamages() {
    try {
        localStorage.setItem(DAMAGE_STORAGE_KEY, JSON.stringify(globalDamages));
        checkStorageUsage(); // photos are the main storage driver
    } catch (e) {
        showToast('Storage full. Could not save damage records.', 'error');
    }
}

function updateDamageCount() {
    const el = document.getElementById('damageCount');
    if (el) el.textContent = `${globalDamages.length} catatan kerusakan`;
}

// Apply the current search + type filter and sort newest-first. Shared by the
// table renderer and the CSV export so both stay in sync.
function getFilteredDamages() {
    const query = (document.getElementById('damageSearch')?.value || '').toLowerCase().trim();
    const typeVal = (document.getElementById('damageTypeFilter')?.value || '');

    let rows = [...globalDamages];
    if (typeVal) rows = rows.filter(d => d.damageType === typeVal);
    if (query) rows = rows.filter(d =>
        `${d.unitName} ${d.sn} ${d.site} ${d.damageType} ${d.component} ${d.description}`
            .toLowerCase().includes(query));

    rows.sort((a, b) => {
        const da = a.date || '', db = b.date || '';
        if (da !== db) return da < db ? 1 : -1;          // date desc
        return (b.createdAt || 0) - (a.createdAt || 0);   // tie-break newest-first
    });
    return rows;
}

// ---- Unit picker ----
// Single source of truth for how a unit is shown/typed in the damage picker.
function damageUnitLabel(u) {
    return `${u.name || '(tanpa nama)'}${u.sn ? ' — ' + u.sn : ''}`;
}

// Fill the searchable datalist behind the #dmgUnit input. With selectedId, also
// pre-fill the input with that unit's label (used when editing).
function populateDamageUnitSelect(selectedId) {
    const input = document.getElementById('dmgUnit');
    const list = document.getElementById('dmgUnitList');
    if (!input || !list) return;
    const units = [...globalData].sort((a, b) =>
        (a.name || '').localeCompare(b.name || ''));
    list.innerHTML = units.map(u =>
        `<option value="${escapeHtml(damageUnitLabel(u))}"></option>`
    ).join('');
    if (selectedId) {
        const u = globalData.find(x => x.id === selectedId);
        input.value = u ? damageUnitLabel(u) : '';
    } else {
        input.value = '';
    }
}

// Resolve the free-typed picker text back to a unit: exact label match first,
// then fall back to matching the serial number after the "—" separator.
function resolveDamageUnit(val) {
    val = (val || '').trim();
    if (!val) return null;
    let u = globalData.find(x => damageUnitLabel(x) === val);
    if (!u && val.includes('—')) {
        const sn = val.split('—').pop().trim();
        if (sn) u = globalData.find(x => (x.sn || '') === sn);
    }
    return u || null;
}

// Find the CURRENT unit for a saved record (damage / license distribution) so
// renames in the unit database reflect everywhere. Matches by unitId first,
// then by serial number; returns null if the unit no longer exists (caller
// falls back to the stored snapshot).
function liveUnitFor(rec) {
    if (!rec) return null;
    if (rec.unitId) {
        const u = globalData.find(x => x.id === rec.unitId);
        if (u) return u;
    }
    if (rec.sn) {
        const sn = (rec.sn || '').toLowerCase();
        const u = globalData.find(x => (x.sn || '').toLowerCase() === sn);
        if (u) return u;
    }
    return null;
}

// ---- Render ----
function renderDamageTable() {
    updateDamageCount();
    selectedDamageIds.clear();
    updateSelectedDamageCount();

    const selectAllBox = document.getElementById('selectAllDamage');
    if (selectAllBox) selectAllBox.checked = false;

    const rows = getFilteredDamages();
    const tbody = document.getElementById('damageBody');
    if (!tbody) return;

    const hasFilter = (document.getElementById('damageSearch')?.value || '') ||
                      (document.getElementById('damageTypeFilter')?.value || '');

    if (rows.length === 0) {
        tbody.innerHTML = `<tr><td colspan="12" style="text-align:center;padding:24px;color:#718096">${
            hasFilter ? 'Tidak ada kerusakan yang cocok dengan filter'
                      : 'Belum ada catatan kerusakan. Klik <strong>Tambah Kerusakan</strong> untuk mulai.'
        }</td></tr>`;
        return;
    }

    tbody.innerHTML = rows.map((d, i) => {
        const comp = d.component
            ? `<span class="badge badge-cat" style="font-size:10px">${escapeHtml(d.component)}</span>`
            : '<span style="color:#a0aec0;font-size:11px">—</span>';
        const desc = d.description || '';
        const descShort = desc.length > 50 ? desc.slice(0, 50) + '…' : desc;
        const lu = liveUnitFor(d);
        const uName = lu ? (lu.name || '') : (d.unitName || '');
        const uSn   = lu ? (lu.sn || '')   : (d.sn || '');
        const uSite = lu ? (lu.site || '') : (d.site || '');
        return `
        <tr>
            <td class="col-check"><input type="checkbox" class="damage-check" data-id="${escapeHtml(d.id)}" onchange="updateSelectedDamageCount()"></td>
            <td>${i + 1}</td>
            <td data-label="Tanggal" style="white-space:nowrap">${escapeHtml(d.date || '')}</td>
            <td data-label="Unit"><strong>${escapeHtml(uName)}</strong></td>
            <td data-label="SN" style="font-family:monospace;font-size:12px">${escapeHtml(uSn)}</td>
            <td data-label="Site">${escapeHtml(uSite)}</td>
            <td data-label="Tipe"><span class="badge badge-breakdown" style="font-size:10px">${escapeHtml(d.damageType || '')}</span></td>
            <td data-label="Komponen">${comp}</td>
            <td data-label="Deskripsi" style="max-width:240px;font-size:12px;color:#4a5568" title="${escapeHtml(desc)}">${escapeHtml(descShort) || '<span style="color:#a0aec0">—</span>'}</td>
            <td data-label="Foto">${d.photo
                ? `<img class="dmg-thumb" src="${d.photo}" alt="foto" onclick="openPhotoLightbox(this.src)">`
                : '<span style="color:#a0aec0;font-size:11px">—</span>'}</td>
            <td data-label="Perbaikan" style="white-space:nowrap">${d.resolved
                ? `<span class="badge badge-good" style="font-size:10px" title="Selesai diperbaiki"><i class="fas fa-check"></i> Selesai${d.resolvedAt ? ' ' + escapeHtml(d.resolvedAt) : ''}</span>`
                : `<button class="btn btn-secondary btn-sm" style="font-size:11px" title="Tandai selesai & pulihkan status unit" onclick="resolveDamage('${escapeHtml(d.id)}')"><i class="fas fa-wrench"></i> Tandai selesai</button>`}</td>
            <td class="col-actions">
                <div class="row-actions">
                    <button class="btn btn-secondary" title="Edit" onclick="editDamage('${escapeHtml(d.id)}')"><i class="fas fa-pen"></i></button>
                    <button class="btn btn-secondary" title="Delete" onclick="deleteDamage('${escapeHtml(d.id)}')"><i class="fas fa-trash" style="color:var(--danger)"></i></button>
                </div>
            </td>
        </tr>`;
    }).join('');
}

// ---- Selection ----
function toggleSelectAllDamages() {
    const checked = document.getElementById('selectAllDamage').checked;
    document.querySelectorAll('.damage-check').forEach(cb => { cb.checked = checked; });
    updateSelectedDamageCount();
}

function updateSelectedDamageCount() {
    selectedDamageIds.clear();
    document.querySelectorAll('.damage-check:checked').forEach(cb => selectedDamageIds.add(cb.dataset.id));
    const count = selectedDamageIds.size;
    const countEl = document.getElementById('selectedDamageCount');
    const btn = document.getElementById('btnDeleteSelectedDamage');
    if (countEl) countEl.textContent = count;
    if (btn) btn.style.display = count > 0 ? '' : 'none';
}

// ---- Component sub-field visibility ----
function onDamageTypeChange() {
    const type = document.getElementById('dmgType').value;
    const group = document.getElementById('dmgComponentGroup');
    const compSel = document.getElementById('dmgComponent');
    if (!group) return;
    if (type === 'Device Precision') {
        group.style.display = '';
    } else {
        group.style.display = 'none';
        if (compSel) compSel.value = '';
    }
}

// ---- Modal: Add / Edit ----
function showAddDamageForm() {
    if (!requireEdit()) return;
    document.getElementById('damageModalTitle').textContent = 'Tambah Kerusakan';
    document.getElementById('editDamageId').value = '';
    document.getElementById('damageForm').reset();
    document.getElementById('dmgDate').value = new Date().toISOString().slice(0, 10);
    populateDamageUnitSelect();
    onDamageTypeChange();
    _dmgPhotoData = '';
    setDamagePhotoPreview();
    const bdGroup = document.getElementById('dmgSetBreakdownGroup');
    if (bdGroup) bdGroup.style.display = '';
    document.getElementById('damageModal').classList.add('open');
}

function editDamage(id) {
    if (!requireEdit()) return;
    const rec = globalDamages.find(d => d.id === id);
    if (!rec) return;

    document.getElementById('damageModalTitle').textContent = 'Edit Kerusakan';
    document.getElementById('editDamageId').value = id;
    document.getElementById('dmgDate').value = rec.date || '';
    // Prefer the current unit (by id or serial) so a renamed unit shows its new
    // name; fall back to the stored snapshot if the unit was deleted.
    const liveDmg = liveUnitFor(rec);
    populateDamageUnitSelect(liveDmg ? liveDmg.id : rec.unitId);
    const dmgUnitInput = document.getElementById('dmgUnit');
    if (dmgUnitInput && !dmgUnitInput.value) {
        dmgUnitInput.value = `${rec.unitName || ''}${rec.sn ? ' — ' + rec.sn : ''}`;
    }
    document.getElementById('dmgType').value = rec.damageType || '';
    document.getElementById('dmgComponent').value = rec.component || '';
    document.getElementById('dmgDescription').value = rec.description || '';
    onDamageTypeChange();
    _dmgPhotoData = rec.photo || '';
    setDamagePhotoPreview();
    // Status linkage only applies when recording a NEW damage, not when editing.
    const bdGroup = document.getElementById('dmgSetBreakdownGroup');
    if (bdGroup) bdGroup.style.display = 'none';
    document.getElementById('damageModal').classList.add('open');
}

function saveDamage(event) {
    event.preventDefault();
    if (!requireEdit()) return;

    const id = document.getElementById('editDamageId').value;
    const unit = resolveDamageUnit(document.getElementById('dmgUnit').value);
    if (!unit) { showToast('Pilih unit dari daftar (ketik nama atau SN)', 'warning'); return; }

    const type = document.getElementById('dmgType').value;
    const data = {
        date: document.getElementById('dmgDate').value,
        unitId: unit.id,
        unitName: unit.name || '',
        sn: unit.sn || '',
        site: unit.site || '',
        damageType: type,
        component: type === 'Device Precision' ? document.getElementById('dmgComponent').value : '',
        description: document.getElementById('dmgDescription').value.trim(),
        photo: _dmgPhotoData || ''
    };

    if (id) {
        const idx = globalDamages.findIndex(d => d.id === id);
        if (idx !== -1) {
            globalDamages[idx] = { ...globalDamages[idx], ...data, updatedAt: Date.now() };
            saveDamages();
            cloudPushDamage(globalDamages[idx]);
            logEvent({
                action: 'update',
                unitId: unit.id,
                unitName: `[Kerusakan] ${data.unitName}`,
                field: data.damageType + (data.component ? ` / ${data.component}` : ''),
                after: data.date
            });
            showToast('Catatan kerusakan diperbarui', 'success');
        }
    } else {
        const newRec = { id: generateDamageId(), ...data, resolved: false, resolvedAt: '', createdAt: Date.now(), updatedAt: Date.now() };
        globalDamages.push(newRec);
        saveDamages();
        cloudPushDamage(newRec);
        logEvent({
            action: 'add',
            unitId: unit.id,
            unitName: `[Kerusakan] ${data.unitName}`,
            field: data.damageType + (data.component ? ` / ${data.component}` : ''),
            after: data.date
        });
        showToast('Catatan kerusakan ditambahkan', 'success');

        // Link to unit status: put the unit (or the affected component) into
        // Breakdown so the dashboard/downtime tracking reflect this damage.
        if (document.getElementById('dmgSetBreakdown')?.checked) {
            const target = _applyDamageBreakdown(unit.id, data.damageType, data.component, data.description);
            if (target) showToast(`${target} "${unit.name}" di-set Breakdown`, 'info');
        }
    }

    closeDamageModal();
    renderDamageTable();
}

// Put the unit (or the damaged Device Precision component) into Breakdown.
// Returns a label describing what was changed, or '' when nothing changed.
function _applyDamageBreakdown(unitId, damageType, component, description) {
    const compField = DAMAGE_COMPONENT_FIELD[component];
    if (damageType === 'Device Precision' && compField) {
        updateUnit(unitId, { [compField]: 'Breakdown' });
        return `Komponen ${component} unit`;
    }
    const reason = `${damageType}${component ? ' / ' + component : ''}: ${description || '-'}`;
    updateUnit(unitId, { status: 'Breakdown', breakdownReason: reason });
    return 'Status unit';
}

// Mark a damage record repaired: flag it resolved and restore the unit /
// component this damage put into Breakdown (downtime duration is recorded
// automatically by trackStatusChange inside updateUnit).
function resolveDamage(id) {
    if (!requireEdit()) return;
    const rec = globalDamages.find(d => d.id === id);
    if (!rec || rec.resolved) return;
    if (!confirm(`Tandai kerusakan unit "${rec.unitName}" (${rec.date}) selesai diperbaiki?`)) return;

    rec.resolved = true;
    rec.resolvedAt = new Date().toISOString().slice(0, 10);
    rec.updatedAt = Date.now();
    saveDamages();
    cloudPushDamage(rec);
    logEvent({
        action: 'update',
        unitId: rec.unitId,
        unitName: `[Kerusakan] ${rec.unitName}`,
        field: 'Perbaikan',
        before: 'Open',
        after: `Selesai (${rec.resolvedAt})`
    });

    const unit = liveUnitFor(rec);
    if (unit) {
        const compField = DAMAGE_COMPONENT_FIELD[rec.component];
        if (rec.damageType === 'Device Precision' && compField) {
            if (!isGood(unit[compField])) updateUnit(unit.id, { [compField]: 'Good' });
        } else if (!isGood(unit.status)) {
            updateUnit(unit.id, { status: 'Good' });
        }
    }

    renderDamageTable();
    showToast('Kerusakan ditandai selesai — status unit dipulihkan', 'success');
}

function closeDamageModal() {
    document.getElementById('damageModal').classList.remove('open');
}

// ---- Delete ----
function deleteDamage(id) {
    if (!requireEdit()) return;
    const rec = globalDamages.find(d => d.id === id);
    if (!rec) return;
    if (!confirm(`Hapus catatan kerusakan unit "${rec.unitName}" (${rec.date})?`)) return;

    globalDamages = globalDamages.filter(d => d.id !== id);
    saveDamages();
    cloudDeleteDamage(id);
    logEvent({
        action: 'delete',
        unitId: rec.unitId,
        unitName: `[Kerusakan] ${rec.unitName}`,
        before: rec.damageType + (rec.component ? ` / ${rec.component}` : '')
    });
    renderDamageTable();
    showToast('Catatan kerusakan dihapus', 'success');
}

function deleteSelectedDamages() {
    if (!requireEdit()) return;
    const count = selectedDamageIds.size;
    if (count === 0) return;
    if (!confirm(`Hapus ${count} catatan kerusakan terpilih?`)) return;

    const idSet = new Set(selectedDamageIds);
    const removed = globalDamages.filter(d => idSet.has(d.id));
    globalDamages = globalDamages.filter(d => !idSet.has(d.id));
    saveDamages();
    removed.forEach(rec => cloudDeleteDamage(rec.id));
    removed.forEach(rec => logEvent({
        action: 'delete',
        unitId: rec.unitId,
        unitName: `[Kerusakan] ${rec.unitName}`,
        before: rec.damageType + (rec.component ? ` / ${rec.component}` : '')
    }));
    renderDamageTable();
    showToast(`${count} catatan kerusakan dihapus`, 'success');
}

// ---- Export report (CSV, opens in Excel via UTF-8 BOM) ----
function exportDamageCSV() {
    const rows = getFilteredDamages();
    if (rows.length === 0) { showToast('Tidak ada data kerusakan untuk diexport', 'warning'); return; }
    const headers = ['No', 'Tanggal', 'Unit', 'Serial Number', 'Site', 'Tipe Kerusakan', 'Komponen', 'Deskripsi', 'Foto', 'Perbaikan'];
    const dataRows = rows.map((d, i) => {
        const lu = liveUnitFor(d);
        return [
            i + 1, d.date || '',
            lu ? (lu.name || '') : (d.unitName || ''),
            lu ? (lu.sn || '') : (d.sn || ''),
            lu ? (lu.site || '') : (d.site || ''),
            d.damageType || '', d.component || '', d.description || '', d.photo ? 'Ada' : '',
            d.resolved ? `Selesai ${d.resolvedAt || ''}`.trim() : 'Open'
        ];
    });
    const csv = [headers, ...dataRows].map(row =>
        row.map(cell => `"${String(cell).replace(/"/g, '""')}"`).join(',')).join('\n');
    const blob = new Blob(['﻿' + csv], { type: 'text/csv;charset=utf-8;' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = `kerusakan_report_${new Date().toISOString().slice(0, 10)}.csv`;
    a.click();
    URL.revokeObjectURL(url);
    showToast(`Export ${rows.length} catatan kerusakan ke CSV`, 'success');
}

// ---- Cloud ----
function cloudPushDamage(rec) {
    if (suppressCloudWrites || !window.cloud?.isReady || !rec) return;
    window.cloud.saveDamage(rec).catch(err => {
        console.error('[cloud] push damage failed:', err);
        showToast('Cloud sync failed — changes saved locally', 'warning');
    });
}

function cloudDeleteDamage(id) {
    if (suppressCloudWrites || !window.cloud?.isReady || !id) return;
    window.cloud.deleteDamage(id).catch(err => {
        console.error('[cloud] delete damage failed:', err);
    });
}

function applyCloudDamagesSnapshot(items) {
    console.log(`[cloud] damage snapshot received — ${items.length} docs`);

    if (_firstDamageSnapshot && items.length === 0 && globalDamages.length > 0) {
        console.warn(`[cloud] first damage snapshot is empty but local has ${globalDamages.length} — keeping local, re-uploading`);
        _firstDamageSnapshot = false;
        window.cloud.saveDamages(globalDamages).catch(err => {
            console.error('[cloud] re-upload damages after empty snapshot failed:', err);
        });
        return;
    }
    _firstDamageSnapshot = false;

    suppressCloudWrites = true;
    try {
        globalDamages = items;
        try { localStorage.setItem(DAMAGE_STORAGE_KEY, JSON.stringify(items)); } catch (e) {}
        if (currentView === 'damage') {
            renderDamageTable();
        } else {
            updateDamageCount();
        }
    } finally {
        suppressCloudWrites = false;
    }
}

// ============================================================
// LICENSE STOCK (STOK LISENSI)
// ============================================================

function generateLicenseId() {
    return 'lic_' + Date.now() + '_' + Math.random().toString(36).substr(2, 5);
}

// ---- Storage ----
function loadLicenseStock() {
    try {
        const raw = localStorage.getItem(LICENSE_STORAGE_KEY);
        globalLicenseStock = raw ? JSON.parse(raw) : [];
    } catch (e) {
        globalLicenseStock = [];
    }
    updateLicenseCount();
    return globalLicenseStock.length > 0;
}

function saveLicenseStockLocal() {
    try {
        localStorage.setItem(LICENSE_STORAGE_KEY, JSON.stringify(globalLicenseStock));
    } catch (e) {
        showToast('Storage full. Could not save license stock.', 'error');
    }
}

function updateLicenseCount() {
    const el = document.getElementById('licenseCount');
    if (el) el.textContent = `${globalLicenseStock.length} transaksi`;
}

// All distinct license types: defaults plus any already used in the data.
function allLicenseTypes() {
    const set = new Set(LICENSE_TYPE_DEFAULTS);
    globalLicenseStock.forEach(r => { if (r.licenseType) set.add(r.licenseType); });
    return [...set].sort((a, b) => a.localeCompare(b));
}

// Per-type stock summary: { type: { in, out, sisa } }
function computeLicenseSummary() {
    const map = {};
    globalLicenseStock.forEach(r => {
        const t = r.licenseType || '(tanpa jenis)';
        if (!map[t]) map[t] = { in: 0, out: 0, sisa: 0 };
        const q = Number(r.qty) || 0;
        if (r.txnType === 'OUT') map[t].out += q;
        else map[t].in += q;
    });
    Object.values(map).forEach(v => { v.sisa = v.in - v.out; });
    return map;
}

function renderLicenseSummary() {
    const el = document.getElementById('licenseSummary');
    if (!el) return;
    const map = computeLicenseSummary();
    const types = Object.keys(map).sort((a, b) => a.localeCompare(b));
    if (types.length === 0) {
        el.innerHTML = '<div class="license-summary__empty">Belum ada data stok lisensi.</div>';
        return;
    }
    el.innerHTML = types.map(t => {
        const s = map[t];
        const low = s.sisa <= 0;
        return `
        <div class="license-sum-card${low ? ' low' : ''}">
            <div class="license-sum-card__type">${escapeHtml(t)}</div>
            <div class="license-sum-card__nums">
                <span title="Masuk"><i class="fas fa-arrow-down" style="color:var(--success)"></i> ${s.in}</span>
                <span title="Terdistribusi"><i class="fas fa-arrow-up" style="color:var(--primary)"></i> ${s.out}</span>
                <span class="license-sum-card__sisa" title="Sisa">Sisa: <strong>${s.sisa}</strong></span>
            </div>
        </div>`;
    }).join('');
}

// ---- Filtering / sort (newest date first) ----
function getFilteredLicenseStock() {
    const query = (document.getElementById('licenseSearch')?.value || '').toLowerCase().trim();
    const txnVal = (document.getElementById('licenseTxnFilter')?.value || '');
    const typeVal = (document.getElementById('licenseTypeFilter')?.value || '');

    let rows = [...globalLicenseStock];
    if (txnVal) rows = rows.filter(r => r.txnType === txnVal);
    if (typeVal) rows = rows.filter(r => r.licenseType === typeVal);
    if (query) rows = rows.filter(r =>
        `${r.licenseType} ${r.unitName} ${r.sn} ${r.note}`.toLowerCase().includes(query));

    rows.sort((a, b) => {
        const da = a.date || '', db = b.date || '';
        if (da !== db) return da < db ? 1 : -1;
        return (b.createdAt || 0) - (a.createdAt || 0);
    });
    return rows;
}

// ---- Render table ----
function renderLicenseStockTable() {
    updateLicenseCount();
    selectedLicenseIds.clear();
    updateSelectedLicenseCount();

    const selectAllBox = document.getElementById('selectAllLicense');
    if (selectAllBox) selectAllBox.checked = false;

    const rows = getFilteredLicenseStock();
    const tbody = document.getElementById('licenseBody');
    if (!tbody) return;

    const hasFilter = (document.getElementById('licenseSearch')?.value || '') ||
                      (document.getElementById('licenseTxnFilter')?.value || '') ||
                      (document.getElementById('licenseTypeFilter')?.value || '');

    if (rows.length === 0) {
        tbody.innerHTML = `<tr><td colspan="10" style="text-align:center;padding:24px;color:#718096">${
            hasFilter ? 'Tidak ada transaksi yang cocok dengan filter'
                      : 'Belum ada transaksi stok lisensi. Klik <strong>Tambah Stok</strong> atau <strong>Distribusi</strong>.'
        }</td></tr>`;
        return;
    }

    tbody.innerHTML = rows.map((r, i) => {
        const isOut = r.txnType === 'OUT';
        const badge = isOut
            ? '<span class="badge badge-cat" style="font-size:10px"><i class="fas fa-share-from-square"></i> Distribusi</span>'
            : '<span class="badge badge-good" style="font-size:10px"><i class="fas fa-arrow-down"></i> Masuk</span>';
        const note = r.note || '';
        const noteShort = note.length > 40 ? note.slice(0, 40) + '…' : note;
        const lu = isOut ? liveUnitFor(r) : null;
        const uName = isOut ? (lu ? (lu.name || '') : (r.unitName || '')) : '';
        const uSn   = isOut ? (lu ? (lu.sn || '')   : (r.sn || '')) : '';
        return `
        <tr>
            <td class="col-check"><input type="checkbox" class="license-check" data-id="${escapeHtml(r.id)}" onchange="updateSelectedLicenseCount()"></td>
            <td>${i + 1}</td>
            <td data-label="Tanggal" style="white-space:nowrap">${escapeHtml(r.date || '')}</td>
            <td data-label="Transaksi">${badge}</td>
            <td data-label="Jenis"><strong>${escapeHtml(r.licenseType || '')}</strong></td>
            <td data-label="Jumlah">${Number(r.qty) || 0}</td>
            <td data-label="Unit">${isOut ? escapeHtml(uName) : '<span style="color:#a0aec0;font-size:11px">—</span>'}</td>
            <td data-label="SN" style="font-family:monospace;font-size:12px">${isOut ? escapeHtml(uSn) : '<span style="color:#a0aec0;font-size:11px">—</span>'}</td>
            <td data-label="Catatan" style="max-width:200px;font-size:12px;color:#4a5568" title="${escapeHtml(note)}">${escapeHtml(noteShort) || '<span style="color:#a0aec0">—</span>'}</td>
            <td class="col-actions">
                <div class="row-actions">
                    <button class="btn btn-secondary" title="Edit" onclick="editLicenseStock('${escapeHtml(r.id)}')"><i class="fas fa-pen"></i></button>
                    <button class="btn btn-secondary" title="Delete" onclick="deleteLicenseStock('${escapeHtml(r.id)}')"><i class="fas fa-trash" style="color:var(--danger)"></i></button>
                </div>
            </td>
        </tr>`;
    }).join('');
}

// ---- Selection ----
function toggleSelectAllLicense() {
    const checked = document.getElementById('selectAllLicense').checked;
    document.querySelectorAll('.license-check').forEach(cb => { cb.checked = checked; });
    updateSelectedLicenseCount();
}

function updateSelectedLicenseCount() {
    selectedLicenseIds.clear();
    document.querySelectorAll('.license-check:checked').forEach(cb => selectedLicenseIds.add(cb.dataset.id));
    const count = selectedLicenseIds.size;
    const countEl = document.getElementById('selectedLicenseCount');
    const btn = document.getElementById('btnDeleteSelectedLicense');
    if (countEl) countEl.textContent = count;
    if (btn) btn.style.display = count > 0 ? '' : 'none';
}

// ---- Pickers ----
function populateLicenseTypeList() {
    const types = allLicenseTypes();
    const list = document.getElementById('licTypeList');
    if (list) list.innerHTML = types.map(t => `<option value="${escapeHtml(t)}"></option>`).join('');
    // Also keep the toolbar type filter in sync.
    const filter = document.getElementById('licenseTypeFilter');
    if (filter) {
        const cur = filter.value;
        filter.innerHTML = '<option value="">Semua Jenis</option>' +
            types.map(t => `<option value="${escapeHtml(t)}">${escapeHtml(t)}</option>`).join('');
        filter.value = cur;
    }
}

function populateLicenseUnitList(selectedLabel) {
    const input = document.getElementById('licUnit');
    const list = document.getElementById('licUnitList');
    if (!input || !list) return;
    const units = [...globalData].sort((a, b) => (a.name || '').localeCompare(b.name || ''));
    list.innerHTML = units.map(u => `<option value="${escapeHtml(damageUnitLabel(u))}"></option>`).join('');
    input.value = selectedLabel || '';
}

function onLicenseTxnChange() {
    const type = document.getElementById('licTxnType').value;
    const group = document.getElementById('licUnitGroup');
    if (group) group.style.display = (type === 'OUT') ? '' : 'none';
}

// ---- Modal: Add / Edit ----
function showAddLicenseForm(txnType) {
    if (!requireEdit()) return;
    document.getElementById('licenseModalTitle').textContent =
        txnType === 'OUT' ? 'Distribusi Lisensi' : 'Tambah Stok Lisensi';
    document.getElementById('editLicenseId').value = '';
    document.getElementById('licenseForm').reset();
    document.getElementById('licTxnType').value = txnType || 'IN';
    document.getElementById('licDate').value = new Date().toISOString().slice(0, 10);
    document.getElementById('licQty').value = '1';
    populateLicenseTypeList();
    populateLicenseUnitList('');
    onLicenseTxnChange();
    document.getElementById('licenseModal').classList.add('open');
}

function editLicenseStock(id) {
    if (!requireEdit()) return;
    const rec = globalLicenseStock.find(r => r.id === id);
    if (!rec) return;

    document.getElementById('licenseModalTitle').textContent = 'Edit Transaksi Lisensi';
    document.getElementById('editLicenseId').value = id;
    populateLicenseTypeList();
    document.getElementById('licTxnType').value = rec.txnType || 'IN';
    document.getElementById('licDate').value = rec.date || '';
    document.getElementById('licType').value = rec.licenseType || '';
    document.getElementById('licQty').value = rec.qty || 1;
    // Prefer the current unit (by id or serial) so renames show; fall back to
    // the stored snapshot if the unit was deleted.
    const liveLic = rec.txnType === 'OUT' ? liveUnitFor(rec) : null;
    const label = liveLic
        ? damageUnitLabel(liveLic)
        : (rec.txnType === 'OUT' ? `${rec.unitName || ''}${rec.sn ? ' — ' + rec.sn : ''}` : '');
    populateLicenseUnitList(label);
    document.getElementById('licNote').value = rec.note || '';
    onLicenseTxnChange();
    document.getElementById('licenseModal').classList.add('open');
}

// Which unit license field-group a stock license type belongs to.
function _licenseKindForType(type) {
    if (type === 'SF-RTK' || type === 'SF-1') return 'gps';
    if (type === 'G5 Advance' || type === 'G5 Basic') return 'display';
    return null; // custom type — not a standard unit license
}

// Connect a distribution (OUT) to its target unit: set the unit's license type,
// start date (= distribution date) and expiry (= start + 1 year). This flows
// straight into expiry status, License Alerts and the auto-downgrade. Returns
// true when a unit license was updated.
function applyDistributedLicenseToUnit(rec) {
    if (!rec || rec.txnType !== 'OUT' || !rec.unitId) return false;
    const kind = _licenseKindForType(rec.licenseType);
    if (!kind) return false;
    const unit = globalData.find(u => u.id === rec.unitId);
    if (!unit) return false;
    const start = rec.date || new Date().toISOString().slice(0, 10);
    let end = '';
    const d = new Date(start);
    if (!isNaN(d.getTime())) { d.setFullYear(d.getFullYear() + 1); end = d.toISOString().slice(0, 10); }
    const fields = kind === 'gps'
        ? { gpsLicense: rec.licenseType, gpsLicenseStartDate: start, gpsLicenseEndDate: end }
        : { licenseDisplay: rec.licenseType, displayLicenseStartDate: start, displayLicenseEndDate: end };
    return updateUnit(rec.unitId, fields);
}

// Backfill: apply EXISTING distributions to their units in one go. For each
// unit + license kind we take the latest-dated OUT record, so a unit ends up
// with its most recent distributed license. Overwrites current unit license
// data (confirmed first). Use after recording distributions that predate the
// auto-link, or after a CSV import of the stock ledger.
function syncDistributionsToUnits() {
    if (!requireEdit()) return;
    const latest = {}; // `${unitId}|${kind}` -> record
    globalLicenseStock.forEach(r => {
        if (r.txnType !== 'OUT' || !r.unitId) return;
        const kind = _licenseKindForType(r.licenseType);
        if (!kind) return;
        if (!globalData.some(u => u.id === r.unitId)) return;
        const key = r.unitId + '|' + kind;
        const cur = latest[key];
        const newer = !cur || (r.date || '') > (cur.date || '')
            || ((r.date || '') === (cur.date || '') && (r.createdAt || 0) > (cur.createdAt || 0));
        if (newer) latest[key] = r;
    });
    const recs = Object.values(latest);
    if (recs.length === 0) {
        showToast('Tidak ada distribusi standar (SF-RTK/SF-1/G5) yang bisa disinkron ke unit', 'info');
        return;
    }
    if (!confirm(`Terapkan ${recs.length} distribusi terbaru ke lisensi unit terkait?\n\n` +
                 `Tanggal habis = tanggal distribusi + 1 tahun. Data lisensi unit yang ada akan ditimpa.`)) return;
    let n = 0;
    recs.forEach(r => { if (applyDistributedLicenseToUnit(r)) n++; });
    showToast(`${n} lisensi unit disinkron dari daftar distribusi`, 'success');
    if (currentView === 'dashboard') updateDashboard(filteredData);
    else if (currentView === 'editUnits') renderEditTable();
}

function saveLicenseStock(event) {
    event.preventDefault();
    if (!requireEdit()) return;

    const id = document.getElementById('editLicenseId').value;
    const txnType = document.getElementById('licTxnType').value;
    const licenseType = document.getElementById('licType').value.trim();
    const qty = Math.max(1, parseInt(document.getElementById('licQty').value, 10) || 1);
    if (!licenseType) { showToast('Isi jenis lisensi', 'warning'); return; }

    const data = {
        date: document.getElementById('licDate').value,
        txnType,
        licenseType,
        qty,
        unitId: '', unitName: '', sn: '',
        note: document.getElementById('licNote').value.trim()
    };

    if (txnType === 'OUT') {
        const unit = resolveDamageUnit(document.getElementById('licUnit').value);
        if (!unit) { showToast('Pilih unit tujuan dari daftar (ketik nama atau SN)', 'warning'); return; }
        data.unitId = unit.id;
        data.unitName = unit.name || '';
        data.sn = unit.sn || '';

        // Warn (but allow) if distributing more than current remaining stock.
        const sum = computeLicenseSummary()[licenseType];
        let sisa = sum ? sum.sisa : 0;
        if (id) { // editing an existing OUT — add its old qty back to available
            const old = globalLicenseStock.find(r => r.id === id);
            if (old && old.txnType === 'OUT' && old.licenseType === licenseType) sisa += (Number(old.qty) || 0);
        }
        if (qty > sisa) {
            if (!confirm(`Stok "${licenseType}" tidak cukup (sisa ${sisa}). Tetap simpan?`)) return;
        }
    }

    if (id) {
        const idx = globalLicenseStock.findIndex(r => r.id === id);
        if (idx !== -1) {
            globalLicenseStock[idx] = { ...globalLicenseStock[idx], ...data, updatedAt: Date.now() };
            saveLicenseStockLocal();
            cloudPushLicense(globalLicenseStock[idx]);
            logEvent({
                action: 'update',
                unitId: data.unitId || '',
                unitName: `[Lisensi] ${data.licenseType}`,
                field: txnType === 'OUT' ? `Distribusi → ${data.unitName}` : 'Stok masuk',
                after: `${qty} (${data.date})`
            });
            showToast('Transaksi lisensi diperbarui', 'success');
        }
    } else {
        const newRec = { id: generateLicenseId(), ...data, createdAt: Date.now(), updatedAt: Date.now() };
        globalLicenseStock.push(newRec);
        saveLicenseStockLocal();
        cloudPushLicense(newRec);
        logEvent({
            action: 'add',
            unitId: data.unitId || '',
            unitName: `[Lisensi] ${data.licenseType}`,
            field: txnType === 'OUT' ? `Distribusi → ${data.unitName}` : 'Stok masuk',
            after: `${qty} (${data.date})`
        });
        showToast(txnType === 'OUT' ? 'Distribusi lisensi dicatat' : 'Stok lisensi ditambahkan', 'success');
    }

    // Connect distribution → the unit's own license (type + dates).
    if (txnType === 'OUT') {
        const applied = applyDistributedLicenseToUnit({ txnType, unitId: data.unitId, licenseType, date: data.date });
        if (applied) {
            const kind = _licenseKindForType(licenseType) === 'display' ? 'Display' : 'GPS';
            showToast(`Lisensi ${kind} unit "${data.unitName}" di-set ${licenseType} (berlaku 1 tahun)`, 'info');
        } else if (!_licenseKindForType(licenseType)) {
            showToast(`"${licenseType}" bukan lisensi unit standar — hanya dicatat di stok`, 'warning');
        }
    }

    closeLicenseModal();
    populateLicenseTypeList();
    renderLicenseSummary();
    renderLicenseStockTable();
}

function closeLicenseModal() {
    document.getElementById('licenseModal').classList.remove('open');
}

// ---- Delete ----
function deleteLicenseStock(id) {
    if (!requireEdit()) return;
    const rec = globalLicenseStock.find(r => r.id === id);
    if (!rec) return;
    if (!confirm(`Hapus transaksi lisensi "${rec.licenseType}" (${rec.date})?`)) return;

    globalLicenseStock = globalLicenseStock.filter(r => r.id !== id);
    saveLicenseStockLocal();
    cloudDeleteLicense(id);
    logEvent({
        action: 'delete',
        unitId: rec.unitId || '',
        unitName: `[Lisensi] ${rec.licenseType}`,
        before: rec.txnType === 'OUT' ? `Distribusi → ${rec.unitName}` : 'Stok masuk'
    });
    renderLicenseSummary();
    renderLicenseStockTable();
    showToast('Transaksi lisensi dihapus', 'success');
}

function deleteSelectedLicenseStock() {
    if (!requireEdit()) return;
    const count = selectedLicenseIds.size;
    if (count === 0) return;
    if (!confirm(`Hapus ${count} transaksi lisensi terpilih?`)) return;

    const idSet = new Set(selectedLicenseIds);
    const removed = globalLicenseStock.filter(r => idSet.has(r.id));
    globalLicenseStock = globalLicenseStock.filter(r => !idSet.has(r.id));
    saveLicenseStockLocal();
    removed.forEach(rec => cloudDeleteLicense(rec.id));
    removed.forEach(rec => logEvent({
        action: 'delete',
        unitId: rec.unitId || '',
        unitName: `[Lisensi] ${rec.licenseType}`,
        before: rec.txnType === 'OUT' ? `Distribusi → ${rec.unitName}` : 'Stok masuk'
    }));
    renderLicenseSummary();
    renderLicenseStockTable();
    showToast(`${count} transaksi lisensi dihapus`, 'success');
}

// ---- Export report (CSV, opens in Excel via UTF-8 BOM) ----
function exportLicenseStockCSV() {
    const rows = getFilteredLicenseStock();
    if (rows.length === 0) { showToast('Tidak ada data lisensi untuk diexport', 'warning'); return; }
    const headers = ['No', 'Tanggal', 'Jenis', 'Jenis Lisensi', 'Jumlah', 'Unit', 'Serial Number', 'Catatan'];
    const dataRows = rows.map((r, i) => {
        const lu = r.txnType === 'OUT' ? liveUnitFor(r) : null;
        return [
            i + 1, r.date || '', r.txnType === 'OUT' ? 'Distribusi' : 'Masuk',
            r.licenseType || '', Number(r.qty) || 0,
            r.txnType === 'OUT' ? (lu ? (lu.name || '') : (r.unitName || '')) : '',
            r.txnType === 'OUT' ? (lu ? (lu.sn || '') : (r.sn || '')) : '',
            r.note || ''
        ];
    });
    const csv = [headers, ...dataRows].map(row =>
        row.map(cell => `"${String(cell).replace(/"/g, '""')}"`).join(',')).join('\n');
    const blob = new Blob(['﻿' + csv], { type: 'text/csv;charset=utf-8;' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = `stok_lisensi_${new Date().toISOString().slice(0, 10)}.csv`;
    a.click();
    URL.revokeObjectURL(url);
    showToast(`Export ${rows.length} transaksi lisensi ke CSV`, 'success');
}

// ---- Import (CSV, append-only ledger) ----
function parseLicenseTxnType(s) {
    const v = (s || '').toString().trim().toLowerCase();
    if (['out', 'distribusi', 'keluar', 'distribution'].includes(v)) return 'OUT';
    return 'IN'; // masuk / in / empty default
}

function downloadLicenseTemplate() {
    const headers = ['Tanggal', 'Jenis', 'Jenis Lisensi', 'Jumlah', 'Unit', 'Serial Number', 'Catatan'];
    const sample = [
        ['2026-04-06', 'Masuk', 'SF-RTK', '50', '', '', 'PO.GPA.2026.04.01343'],
        ['2026-05-11', 'Distribusi', 'G5 Advance', '1', 'GGCH001G', '', 'Dipasang di unit']
    ];
    const csv = [headers, ...sample].map(row =>
        row.map(cell => `"${String(cell).replace(/"/g, '""')}"`).join(',')).join('\n');
    const blob = new Blob(['﻿' + csv], { type: 'text/csv;charset=utf-8;' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = 'template_stok_lisensi.csv';
    a.click();
    URL.revokeObjectURL(url);
}

function handleLicenseCSVImport(file) {
    if (!requireEdit()) return;
    showLoading(true);
    Papa.parse(file, {
        header: true,
        skipEmptyLines: true,
        complete: result => {
            const today = new Date().toISOString().slice(0, 10);
            const added = [];
            let rejected = 0;

            result.data.forEach(row => {
                const licenseType = (getValAny(row, ['Jenis Lisensi', 'License', 'License Type']) || '').toString().trim();
                if (!licenseType) { rejected++; return; }

                const txnType = parseLicenseTxnType(getValAny(row, ['Jenis', 'Type', 'Transaksi', 'Transaction']));
                const qty = Math.max(1, parseInt(getValAny(row, ['Jumlah', 'Qty', 'Quantity']), 10) || 1);
                const date = (getValAny(row, ['Tanggal', 'Date']) || '').toString().trim() || today;
                const note = (getValAny(row, ['Catatan', 'Note', 'Notes', 'Remarks']) || '').toString().trim();

                const rec = {
                    id: generateLicenseId(),
                    date, txnType, licenseType, qty,
                    unitId: '', unitName: '', sn: '',
                    note,
                    createdAt: Date.now(), updatedAt: Date.now()
                };

                if (txnType === 'OUT') {
                    const snCsv = (getValAny(row, ['Serial Number', 'SN']) || '').toString().trim();
                    const nameCsv = (getValAny(row, ['Unit', 'Nickname']) || '').toString().trim();
                    let unit = null;
                    if (snCsv) unit = globalData.find(u => (u.sn || '').toLowerCase() === snCsv.toLowerCase());
                    if (!unit && nameCsv) unit = globalData.find(u => (u.name || '').toLowerCase() === nameCsv.toLowerCase());
                    if (unit) {
                        rec.unitId = unit.id; rec.unitName = unit.name || ''; rec.sn = unit.sn || '';
                    } else {
                        rec.unitName = nameCsv; rec.sn = snCsv;
                    }
                }
                added.push(rec);
            });

            if (added.length > 0) {
                globalLicenseStock.push(...added);
                saveLicenseStockLocal();
                if (!suppressCloudWrites && window.cloud?.isReady) {
                    window.cloud.saveLicenses(added).catch(err => {
                        console.error('[cloud] import licenses failed:', err);
                        showToast('Cloud sync gagal — data tersimpan lokal', 'warning');
                    });
                }
                logEvent({ action: 'add', unitName: '[Lisensi] Import CSV', after: `${added.length} transaksi` });
                populateLicenseTypeList();
                renderLicenseSummary();
                renderLicenseStockTable();
            }

            showLoading(false);
            const msg = `Import lisensi: ${added.length} ditambahkan` + (rejected ? `, ${rejected} dilewati (jenis lisensi kosong)` : '');
            showToast(msg, added.length ? 'success' : 'warning');
        },
        error: err => {
            showToast('Gagal membaca CSV: ' + err.message, 'error');
            showLoading(false);
        }
    });
}

// ---- Cloud ----
function cloudPushLicense(rec) {
    if (suppressCloudWrites || !window.cloud?.isReady || !rec) return;
    window.cloud.saveLicense(rec).catch(err => {
        console.error('[cloud] push license failed:', err);
        showToast('Cloud sync failed — changes saved locally', 'warning');
    });
}

function cloudDeleteLicense(id) {
    if (suppressCloudWrites || !window.cloud?.isReady || !id) return;
    window.cloud.deleteLicense(id).catch(err => {
        console.error('[cloud] delete license failed:', err);
    });
}

function applyCloudLicenseSnapshot(items) {
    console.log(`[cloud] license snapshot received — ${items.length} docs`);

    if (_firstLicenseSnapshot && items.length === 0 && globalLicenseStock.length > 0) {
        console.warn(`[cloud] first license snapshot is empty but local has ${globalLicenseStock.length} — keeping local, re-uploading`);
        _firstLicenseSnapshot = false;
        window.cloud.saveLicenses(globalLicenseStock).catch(err => {
            console.error('[cloud] re-upload licenses after empty snapshot failed:', err);
        });
        return;
    }
    _firstLicenseSnapshot = false;

    suppressCloudWrites = true;
    try {
        globalLicenseStock = items;
        try { localStorage.setItem(LICENSE_STORAGE_KEY, JSON.stringify(items)); } catch (e) {}
        if (currentView === 'licenseStock') {
            populateLicenseTypeList();
            renderLicenseSummary();
            renderLicenseStockTable();
        } else {
            updateLicenseCount();
        }
    } finally {
        suppressCloudWrites = false;
    }
}

// Firestore rules banner for the licenseStock collection (mirrors damage/history).
function showLicenseRulesBanner() {
    const slot = document.querySelector('#viewLicenseStock .license-rules-slot');
    if (!slot || slot.querySelector('.category-rules-banner')) return;
    const banner = document.createElement('div');
    banner.className = 'category-rules-banner';
    banner.innerHTML = `
        <strong><i class="fas fa-triangle-exclamation"></i> Firestore rules are blocking license stock.</strong>
        <p>Your project's security rules don't allow this account to read or write the <code>licenseStock</code> collection yet — that's why license stock won't sync across devices. Paste the block below into <em>Firebase Console → Firestore → Rules</em>, then hard-refresh:</p>
        <pre>match /licenseStock/{id} {
  allow read:  if request.auth != null
               &amp;&amp; get(/databases/$(database)/documents/users/$(request.auth.uid)).data.status == 'active';
  allow write: if request.auth != null
               &amp;&amp; get(/databases/$(database)/documents/users/$(request.auth.uid)).data.role in ['owner', 'team']
               &amp;&amp; get(/databases/$(database)/documents/users/$(request.auth.uid)).data.status == 'active';
}</pre>
    `;
    slot.appendChild(banner);
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
        // Damage records
        if (window.cloud.getAllDamages) {
            const cloudDamages = await window.cloud.getAllDamages();
            console.log(`[cloud] check: cloud has ${cloudDamages.length} damage records, local has ${globalDamages.length}`);
            if (cloudDamages.length === 0 && globalDamages.length > 0) {
                console.log(`[cloud] migrating ${globalDamages.length} local damage records to Firestore...`);
                await window.cloud.saveDamages(globalDamages);
                showToast(`Uploaded ${globalDamages.length} damage records to cloud`, 'success');
            }
        }
        // License stock
        if (window.cloud.getAllLicenses) {
            const cloudLicenses = await window.cloud.getAllLicenses();
            console.log(`[cloud] check: cloud has ${cloudLicenses.length} license records, local has ${globalLicenseStock.length}`);
            if (cloudLicenses.length === 0 && globalLicenseStock.length > 0) {
                console.log(`[cloud] migrating ${globalLicenseStock.length} local license records to Firestore...`);
                await window.cloud.saveLicenses(globalLicenseStock);
                showToast(`Uploaded ${globalLicenseStock.length} license records to cloud`, 'success');
            }
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
    migrateNicknamesFromExcel();
    migrateLicenseDataBatch1();
    migrateLicenseDataBatch2();
    migrateSiteData20260525();
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

// Surfaces the exact Firestore rules for the `damageRecords` collection when it
// rejects reads/writes. Shown inside the Kerusakan view so the user knows why
// damage records aren't syncing across devices.
function showDamageRulesBanner() {
    const slot = document.querySelector('#viewDamage .damage-rules-slot');
    if (!slot || slot.querySelector('.category-rules-banner')) return;
    const banner = document.createElement('div');
    banner.className = 'category-rules-banner';
    banner.innerHTML = `
        <strong><i class="fas fa-triangle-exclamation"></i> Firestore rules are blocking damage records.</strong>
        <p>Your project's security rules don't allow this account to read or write the <code>damageRecords</code> collection yet — that's why damage records won't sync across devices. Paste the block below into <em>Firebase Console → Firestore → Rules</em>, then hard-refresh:</p>
        <pre>match /damageRecords/{docId} {
  allow read:  if request.auth != null
               &amp;&amp; get(/databases/$(database)/documents/users/$(request.auth.uid)).data.status == 'active';
  allow write: if request.auth != null
               &amp;&amp; get(/databases/$(database)/documents/users/$(request.auth.uid)).data.role in ['owner', 'team']
               &amp;&amp; get(/databases/$(database)/documents/users/$(request.auth.uid)).data.status == 'active';
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
        if (window.cloud.subscribeDamages) {
            cloudDamageUnsub = window.cloud.subscribeDamages(applyCloudDamagesSnapshot, err => {
                console.warn('[cloud] damage records offline:', err && err.code);
                if (err && err.code === 'permission-denied') {
                    showDamageRulesBanner();
                    showToast('Kerusakan diblokir Firestore rules — lihat panel Kerusakan', 'warning');
                }
            });
        }
        if (window.cloud.subscribeLicenses) {
            cloudLicenseUnsub = window.cloud.subscribeLicenses(applyCloudLicenseSnapshot, err => {
                console.warn('[cloud] license stock offline:', err && err.code);
                if (err && err.code === 'permission-denied') {
                    showLicenseRulesBanner();
                    showToast('Stok Lisensi diblokir Firestore rules — lihat panel Stok Lisensi', 'warning');
                }
            });
        }
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
        _firstDamageSnapshot = false;
        _firstLicenseSnapshot = false;
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
    if (cloudDamageUnsub) { try { cloudDamageUnsub(); } catch (_) {} cloudDamageUnsub = null; }
    if (cloudLicenseUnsub) { try { cloudLicenseUnsub(); } catch (_) {} cloudLicenseUnsub = null; }
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

// One-shot nickname migration from Excel data (May 2026)
function migrateNicknamesFromExcel() {
    const FLAG = 'nickname_migration_v1_done';
    if (localStorage.getItem(FLAG)) return;
    if (!globalData.length) return;

    const map = {"1T8C570HKST250056":"GGCH001G","1BM7230CVS3001122":"GGTR063G_LSL","1BM7230CKS3001133":"GGTR064G_BRG","1BM7230CJS3001134":"GGTR065G_TTW","1BM7230CPS3001115":"GGTR068G","1BM7230CKS3001147":"GGTR069G_HBT","1BM7230CLS3001141":"GGTR070G_FBG","1BM7230CCS3001113":"GGTR071G_FBG","1BM7230CHS3001125":"GGTR072G_","1BM7230CHS3001139":"GGTR073G_FBG","1BM7230CKS3001150":"GGTR074G_SGD","1BM7230CLS3001107":"GGTR077G","1BM7230CCS3001118":"GGTR079G_BRG","1BM7230CES3001143":"GGTR080G_BRI","1BM7230CCS3001149":"GGTR081G_TTW","1BM7230CCS3001127":"GGTR083G_BRI","1BM7230CTS3001128":"GGTR084G_LSL","1BM7230CES3001045":"GGTR087G_BRI","1BM7230CKS3001049":"GGTR089G_HBT","1BM7230CCS3001077":"GGTR090G","1BM7230CKS3001083":"GGTR091G_BRI","1BM7230CES3001076":"GGTR092G_SGD","1BM7230CJS3001084":"GGTR094G_TTW","1BM7230CCS3001104":"GGTR095G_BRG","1BM7230CAS3001090":"GGTR096G_HBT","1BM7230CAS3001087":"GGTR097G_BRI","1BM7230CES3001093":"GGTR099G_TTW","1BM7230CLS3001088":"GGTR100G_SGD","1BM7230CKS3001102":"GGTR101G_HBT","1BM7230CHS3001108":"GGTR103G_SGG","1BM7230CES3001112":"GGTR105G_FCI","1BM7230CJS3001036":"GGTR107G_SGG","1BM7230CLS3001057":"GGTR108G_LSS","1BM7230CLS3001026":"GGTR110G_FCI","1BM7230CCS3001071":"GGTR111G_HN","1BM7230CCS3001068":"GGTR113G_SGD","1BM7230CKS3001066":"GGTR115G_FCI","1BM7230CHS3001075":"GGTR116G_BPH","1BM7230CAS3001073":"GGTR117G_BPH","1BM7230CJS3001053":"GGTR119G_BPH","1BM7230CPS3001051":"GGTR120G_HBT","1BM7230CCS3001054":"GGTR121G_HBT","1BM7230CKS3001052":"GGTR122G_BRI","1BM7230CVS3001069":"GGTR123G_LS","1BM7230CVS3001072":"GGTR125G_SGD","1BM7230CJS3001067":"GGTR128G_BSS","1RW8310DCSA260870":"GGTR130G_PLJ","1RW8310DLSA260881":"GGTR131G_OHZ","1RW8310DESB260912":"GGTR134G_OHB","1RW8310DPSB261028":"GGTR135G_HD","1RW8310DCSB261180":"GGTR136G_OHB","1RW8310DASB261036":"GGTR137G_OHB","1RW8310DLSB261152":"GGTR138G_SCL","1RW8310DPSB260929":"GGTR139G_SCL","1RW8310DCSB261222":"GGTR140G_SCL","1RW8310DPSB260963":"GGTR141G_OHB","1RW8310DASB260937":"GGTR142G_OHB","1RW8310DHSB260973":"GGTR143G_PL","1RW8310DPSB261126":"GGTR144G_SCL","1RW8310DCSB261205":"GGTR145G_SCL","1RW8310DHSB261010":"GGTR147G_SCL","1RW8310DPSB260946":"GGTR148G_OHB","1RW8310DPSB261000":"GGTR149G_PLJ","1RW8310DVSB261200":"GGTR152G_OHB","1RW8310DCSB261096":"GGTR153G_PLJ","1BM7230CTS3001095":"GGTR154G_SGD","1NW4025MKS0250246":"GGTS003G","1NW4025MCS0250248":"GGTS004G","1NW4025MVS0250249":"GGTS005G","1BM7230CPS3001132":"GMTR066G_TTW","1BM7230CES3001126":"GMTR067G_ZRW","1BM7230CJS3001117":"GMTR075G_SGD","1BM7230CLS3001124":"GMTR076G_PLJ","1BM7230CAS3001137":"GMTR078G_LSL","1BM7230CPS3001129":"GMTR082G_BSS","1BM7230CCS3001080":"GMTR085G_BPH","1BM7230CTS3001050":"GMTR086G_OHB","1BM7230CPS3001065":"GMTR088G_ZRP","1BM7230CJS3001098":"GMTR093G_FBG","1BM7230CPS3001101":"GMTR098G_BRI","1BM7230CCS3001135":"GMTR102G_ZRP","1BM7230CCS3001121":"GMTR104G_OHB","1BM7230CLS3001110":"GMTR106G_FBG","1BM7230CES3001028":"GMTR109G_OHB","1BM7230CCS3001063":"GMTR112G_ZR","1BM7230CTS3001047":"GMTR118G_","1BM7230CPS3001082":"GMTR124G_LSL","1BM7230CTS3001078":"GMTR126G_BRI","1BM7230CLS3001060":"GMTR127G_ZR","1RW8310DKSA260873":"GMTR129G_SCL","1RW8310DCSA260853":"GMTR132G_SCL","1RW8310DLSB260910":"GMTR133G_OHB","1BM7230CTS3001114":"GMTR146G_SGD","1RW8310DPSA260905":"GMTR150G_OHZ","1RW8310DHSB261105":"GMTR151G_TTW","1NW4025MJS0250247":"GMTS001G","1NW4025MPS0250245":"GMTS002G","1T8C570HHST260045":"MGCH001M","1T8C570HEST260046":"MGCH002M","1T8C570HTST260048":"MGCH004M","1T8C570HPST260049":"MGCH005M","1BM7230CKS3002332":"MGTR040M_BPT","1BM7230CPS3002331":"MGTR042M_HBM","1BM7230CCS3002320":"MGTR043M_HBA","1BM7230CHS3002324":"MGTR048M_HBM","1BM7230CCS3002334":"MGTR051M_HBA","1BM7230CHS3002355":"MGTR052M_HBA","1BM7230CKS3002346":"MGTR060M_HBM","1BM7230CVS3002349":"MGTR061M_HBA","1BM7230CJS3002350":"MGTR062_HBM","1BM7230CCS3002348":"MGTR063M_HBM","1BM7230CCS3002351":"MGTR070M_BPA","1BM7230CPS3002345":"MGTR071M_BPA","1BM7230CAS3002353":"MGTR072M_BPH","1BM7230CTS3002344":"MGTR073M_","1BM7230CLS3002340":"MGTR074M_","1BM7230CAT3002368":"MGTR075M_HBA","1BM7230CTT3002376":"MGTR076M_HBA","1BM7230CET3002374":"MGTR077M_HBA","1BM7230CKT3002378":"MGTR078M_HBA","1BM7230CTT3002359":"MGTR079M_HBA","1BM7230CET3002360":"MGTR080M_HBA","1BM7230CHT3002390":"MGTR081M_HBA","1BM7230CET3002388":"MGTR082M_HBA","1T8C570HCST260047":"MMCH003M","1T8C570HETT260050":"MMCH006M","1BM7230CKS3002329":"MMTR039M_BPT","1BM7230CCS3002326":"MMTR041M_HBM","1BM7230CES3002325":"MMTR044M_HBM","1BM7230CTS3002330":"MMTR045M_HBM","1BM7230CAS3002322":"MMTR046M_HBM","1BM7230CHS3002338":"MMTR047M_HBM","1BM7230CLS3002323":"MMTR049M_HBM","1BM7230CLS3002337":"MMTR050M_HBM","1BM7230CVS3002352":"MMTR053M_HBA","1BM7230CHS3002341":"MMTR054M_HBM","1BM7230CES3002339":"MMTR055M_HBA","1BM7230CJS3002347":"MMTR056M_HBM","1BM7230CES3002342":"MMTR057M_BPT","1BM7230CCS3002343":"MMTR058M_BPT","1BM7230CES3002356":"MMTR059M_HBA","1BM7230CKT3002381":"MMTR083M_BPA","1BM7230CCT3002389":"MMTR084M_BPA","1BM7230CCT3002361":"MMTR085M_BPA","1BM7230CJT3002379":"MMTR086M_BPA","1BM7230CAT3002371":"MMTR087M_HBT","1BM7230CCT3002383":"MMTR088M_PLJ","1BM7230CVT3002370":"MMTR089M_HBT","1BM7230CHT3002387":"MMTR090M_HBT","1BM7230CVT3002384":"MMTR091M_HBT","1BM7230CAT3002385":"MMTR092M_HBT","1BM7230CCT3002375":"MMTR093M_HBT","1BM7230CPT3002363":"MMTR094M_HBT","1BM7230CCT3002366":"MMTR095M_HBT","1BM7230CLT3002386":"MMTR096M_HBT","1BM7230CHS3001061":"TR114M_PL"};

    let updated = 0, skipped = 0;
    globalData.forEach(unit => {
        const sn = (unit.sn || '').trim();
        if (!sn || !map[sn]) return;
        const newName = map[sn];
        if (unit.name === newName) { skipped++; return; }
        updateUnit(unit.id, { name: newName });
        updated++;
    });

    localStorage.setItem(FLAG, '1');
    if (updated > 0) {
        showToast(`Nickname migration: ${updated} unit(s) updated, ${skipped} already correct`, 'success');
        if (currentView === 'dashboard') { filteredData = [...globalData]; onDataLoaded(); }
        else if (currentView === 'editUnits') renderEditTable();
    }
    console.log(`[migration] nicknames: ${updated} updated, ${skipped} skipped`);
}

// One-shot license data migration — batch 1 (Autotrac/JDLink install monitoring)
function migrateLicenseDataBatch1() {
    const FLAG = 'license_batch1_migration_done';
    if (localStorage.getItem(FLAG)) return;
    if (!globalData.length) return;

    const data = [
        {sn:"1BM7230CHS3002338",gpsLicense:"SF-RTK",licenseDisplay:"G5 Advance",gpsLicenseStartDate:"2027-04-29",gpsLicenseEndDate:"2027-04-29"},
        {sn:"1BM7230CTS3002330",gpsLicense:"SF-RTK",licenseDisplay:"G5 Advance",gpsLicenseStartDate:"2027-04-29",gpsLicenseEndDate:"2027-04-29"},
        {sn:"1BM7230CAS3002322",gpsLicense:"SF-RTK",licenseDisplay:"G5 Advance",gpsLicenseStartDate:"2027-04-29",gpsLicenseEndDate:"2027-04-29"},
        {sn:"1BM7230CES3002325",gpsLicense:"SF-RTK",licenseDisplay:"G5 Advance",gpsLicenseStartDate:"2027-04-29",gpsLicenseEndDate:"2027-04-29"},
        {sn:"1BM7230CKS3002329",gpsLicense:"SF-RTK",licenseDisplay:"G5 Advance",gpsLicenseStartDate:"2027-05-05",gpsLicenseEndDate:"2027-05-05"},
        {sn:"1BM7230CKS3002332",gpsLicense:"SF-RTK",licenseDisplay:"G5 Advance",gpsLicenseStartDate:"2027-05-04",gpsLicenseEndDate:"2027-05-04"},
        {sn:"1BM7230CCS3002320",gpsLicense:"SF-RTK",licenseDisplay:"G5 Advance",gpsLicenseStartDate:"2027-05-05",gpsLicenseEndDate:"2027-05-05"},
        {sn:"1BM7230CHS3002324",gpsLicense:"SF-RTK",licenseDisplay:"G5 Advance",gpsLicenseStartDate:"2027-05-05",gpsLicenseEndDate:"2027-05-05"},
        {sn:"1BM7230CLS3002337",gpsLicense:"SF-RTK",licenseDisplay:"G5 Advance",gpsLicenseStartDate:"2027-05-04",gpsLicenseEndDate:"2027-05-04"},
        {sn:"1BM7230CCS3002334",gpsLicense:"SF-RTK",licenseDisplay:"G5 Advance",gpsLicenseStartDate:"2027-05-05",gpsLicenseEndDate:"2027-05-05"},
        {sn:"1BM7230CHS3002355",gpsLicense:"SF-RTK",licenseDisplay:"G5 Advance",gpsLicenseStartDate:"2027-05-05",gpsLicenseEndDate:"2027-05-05"},
        {sn:"1BM7230CVS3002352",gpsLicense:"SF-RTK",licenseDisplay:"G5 Advance",gpsLicenseStartDate:"2027-05-01",gpsLicenseEndDate:"2027-05-01"},
        {sn:"1BM7230CHS3002341",gpsLicense:"SF-RTK",licenseDisplay:"G5 Advance",gpsLicenseStartDate:"2027-05-04",gpsLicenseEndDate:"2027-05-04"},
        {sn:"1BM7230CES3002339",gpsLicense:"SF-RTK",licenseDisplay:"G5 Advance",gpsLicenseStartDate:"2027-05-04",gpsLicenseEndDate:"2027-05-04"},
        {sn:"1BM7230CES3002342",gpsLicense:"SF-RTK",licenseDisplay:"G5 Advance",gpsLicenseStartDate:"2027-05-03",gpsLicenseEndDate:"2027-05-03"},
        {sn:"1BM7230CES3002356",gpsLicense:"SF-RTK",licenseDisplay:"G5 Advance",gpsLicenseStartDate:"2027-05-04",gpsLicenseEndDate:"2027-05-04"},
        {sn:"1BM7230CKS3002346",gpsLicense:"SF-RTK",licenseDisplay:"G5 Advance",gpsLicenseStartDate:"2027-05-05",gpsLicenseEndDate:"2027-05-05"},
        {sn:"1BM7230CJS3002350",gpsLicense:"SF-RTK",licenseDisplay:"G5 Advance",gpsLicenseStartDate:"2027-05-05",gpsLicenseEndDate:"2027-05-05"},
        {sn:"1BM7230CCS3002348",gpsLicense:"SF-RTK",licenseDisplay:"G5 Advance",gpsLicenseStartDate:"2027-05-01",gpsLicenseEndDate:"2027-05-01"},
        {sn:"1T8C570HHST260045",gpsLicense:"SF-RTK",licenseDisplay:"G5 Advance",gpsLicenseStartDate:"2027-05-08",gpsLicenseEndDate:"2027-05-08"},
        {sn:"1T8C570HEST260046",gpsLicense:"SF-RTK",licenseDisplay:"G5 Advance",gpsLicenseStartDate:"2027-05-04",gpsLicenseEndDate:"2027-05-04"},
        {sn:"1T8C570HCST260047",gpsLicense:"SF-RTK",licenseDisplay:"G5 Advance",gpsLicenseStartDate:"2027-05-04",gpsLicenseEndDate:"2027-05-04"},
        {sn:"1T8C570HTST260048",gpsLicense:"SF-RTK",licenseDisplay:"G5 Advance",gpsLicenseStartDate:"2027-05-08",gpsLicenseEndDate:"2027-05-08"},
        {sn:"1T8C570HPST260049",gpsLicense:"SF-RTK",licenseDisplay:"G5 Advance",gpsLicenseStartDate:"2027-05-04",gpsLicenseEndDate:"2027-05-04"},
        {sn:"1T8C570HETT260050",gpsLicense:"SF-RTK",licenseDisplay:"G5 Advance",gpsLicenseStartDate:"2027-05-04",gpsLicenseEndDate:"2027-05-04"}
    ];

    let updated = 0;
    data.forEach(entry => {
        const unit = globalData.find(u => (u.sn || '').trim() === entry.sn);
        if (!unit) return;
        const fields = {};
        if (unit.gpsLicense !== entry.gpsLicense) fields.gpsLicense = entry.gpsLicense;
        if (unit.licenseDisplay !== entry.licenseDisplay) fields.licenseDisplay = entry.licenseDisplay;
        if (unit.gpsLicenseStartDate !== entry.gpsLicenseStartDate) fields.gpsLicenseStartDate = entry.gpsLicenseStartDate;
        if (unit.gpsLicenseEndDate !== entry.gpsLicenseEndDate) fields.gpsLicenseEndDate = entry.gpsLicenseEndDate;
        if (Object.keys(fields).length > 0) {
            updateUnit(unit.id, fields);
            updated++;
        }
    });

    localStorage.setItem(FLAG, '1');
    if (updated > 0) {
        showToast(`License migration (batch 1): ${updated} unit(s) updated`, 'success');
        if (currentView === 'dashboard') { filteredData = [...globalData]; onDataLoaded(); }
        else if (currentView === 'editUnits') renderEditTable();
    }
    console.log(`[migration] license batch 1: ${updated} updated out of ${data.length}`);
}

// One-shot license data migration — batch 2
function migrateLicenseDataBatch2() {
    const FLAG = 'license_batch2_migration_done';
    if (localStorage.getItem(FLAG)) return;
    if (!globalData.length) return;

    const data = [
        {sn:"1BM7230CCS3002351",gpsLicense:"SF-RTK",licenseDisplay:"G5 Advance",gpsLicenseStartDate:"2027-05-10",gpsLicenseEndDate:"2027-05-10"},
        {sn:"1BM7230CPS3002345",gpsLicense:"SF-RTK",licenseDisplay:"G5 Advance",gpsLicenseStartDate:"2027-05-12",gpsLicenseEndDate:"2027-05-12"},
        {sn:"1BM7230CAS3002353",gpsLicense:"SF-RTK",licenseDisplay:"G5 Advance",gpsLicenseStartDate:"2027-05-14",gpsLicenseEndDate:"2027-05-14"},
        {sn:"1BM7230CTS3002344",gpsLicense:"SF-RTK",licenseDisplay:"G5 Advance",gpsLicenseStartDate:"2027-05-10",gpsLicenseEndDate:"2027-05-10"},
        {sn:"1BM7230CLS3002340",gpsLicense:"SF-RTK",licenseDisplay:"G5 Advance",gpsLicenseStartDate:"2027-05-12",gpsLicenseEndDate:"2027-05-12"},
        {sn:"1BM7230CAT3002368",gpsLicense:"SF-RTK",licenseDisplay:"G5 Advance",gpsLicenseStartDate:"2027-05-12",gpsLicenseEndDate:"2027-05-12"},
        {sn:"1BM7230CTT3002376",gpsLicense:"SF-RTK",licenseDisplay:"G5 Advance",gpsLicenseStartDate:"2027-05-12",gpsLicenseEndDate:"2027-05-12"},
        {sn:"1BM7230CET3002374",gpsLicense:"SF-RTK",licenseDisplay:"G5 Advance",gpsLicenseStartDate:"2027-05-12",gpsLicenseEndDate:"2027-05-12"},
        {sn:"1BM7230CKT3002378",gpsLicense:"SF-RTK",licenseDisplay:"G5 Advance",gpsLicenseStartDate:"2027-05-12",gpsLicenseEndDate:"2027-05-12"},
        {sn:"1BM7230CTT3002359",gpsLicense:"SF-RTK",licenseDisplay:"G5 Advance",gpsLicenseStartDate:"2027-05-12",gpsLicenseEndDate:"2027-05-12"},
        {sn:"1BM7230CET3002360",gpsLicense:"SF-RTK",licenseDisplay:"G5 Advance",gpsLicenseStartDate:"2027-05-12",gpsLicenseEndDate:"2027-05-12"},
        {sn:"1BM7230CHT3002390",gpsLicense:"SF-RTK",licenseDisplay:"G5 Advance",gpsLicenseStartDate:"2027-05-12",gpsLicenseEndDate:"2027-05-12"},
        {sn:"1BM7230CET3002388",gpsLicense:"SF-RTK",licenseDisplay:"G5 Advance",gpsLicenseStartDate:"2027-05-14",gpsLicenseEndDate:"2027-05-14"},
        {sn:"1BM7230CKT3002381",gpsLicense:"SF-RTK",licenseDisplay:"G5 Advance",gpsLicenseStartDate:"2027-05-12",gpsLicenseEndDate:"2027-05-12"},
        {sn:"1BM7230CCT3002389",gpsLicense:"SF-RTK",licenseDisplay:"G5 Advance",gpsLicenseStartDate:"2027-05-12",gpsLicenseEndDate:"2027-05-12"},
        {sn:"1BM7230CCT3002361",gpsLicense:"SF-RTK",licenseDisplay:"G5 Advance",gpsLicenseStartDate:"2027-05-14",gpsLicenseEndDate:"2027-05-14"},
        {sn:"1BM7230CJT3002379",gpsLicense:"SF-RTK",licenseDisplay:"G5 Advance",gpsLicenseStartDate:"2027-05-12",gpsLicenseEndDate:"2027-05-12"},
        {sn:"1BM7230CAT3002371",gpsLicense:"SF-RTK",licenseDisplay:"G5 Advance",gpsLicenseStartDate:"2027-05-12",gpsLicenseEndDate:"2027-05-12"},
        {sn:"1BM7230CCT3002383",gpsLicense:"SF-RTK",licenseDisplay:"G5 Advance",gpsLicenseStartDate:"2027-05-12",gpsLicenseEndDate:"2027-05-12"},
        {sn:"1BM7230CVT3002370",gpsLicense:"SF-RTK",licenseDisplay:"G5 Advance",gpsLicenseStartDate:"2027-05-12",gpsLicenseEndDate:"2027-05-12"},
        {sn:"1BM7230CHT3002387",gpsLicense:"SF-RTK",licenseDisplay:"G5 Advance",gpsLicenseStartDate:"2027-05-10",gpsLicenseEndDate:"2027-05-10"},
        {sn:"1BM7230CVT3002384",gpsLicense:"SF-RTK",licenseDisplay:"G5 Advance",gpsLicenseStartDate:"2027-05-12",gpsLicenseEndDate:"2027-05-12"},
        {sn:"1BM7230CAT3002385",gpsLicense:"SF-RTK",licenseDisplay:"G5 Advance",gpsLicenseStartDate:"2027-05-12",gpsLicenseEndDate:"2027-05-12"},
        {sn:"1BM7230CCT3002375",gpsLicense:"SF-RTK",licenseDisplay:"G5 Advance",gpsLicenseStartDate:"2027-05-12",gpsLicenseEndDate:"2027-05-12"},
        {sn:"1BM7230CPT3002363",gpsLicense:"SF-RTK",licenseDisplay:"G5 Advance",gpsLicenseStartDate:"2027-05-12",gpsLicenseEndDate:"2027-05-12"},
        {sn:"1BM7230CCT3002366",gpsLicense:"SF-RTK",licenseDisplay:"G5 Advance",gpsLicenseStartDate:"2027-05-10",gpsLicenseEndDate:"2027-05-10"},
        {sn:"1BM7230CLT3002386",gpsLicense:"SF-RTK",licenseDisplay:"G5 Advance",gpsLicenseStartDate:"2027-05-10",gpsLicenseEndDate:"2027-05-10"}
    ];

    let updated = 0;
    data.forEach(entry => {
        const unit = globalData.find(u => (u.sn || '').trim() === entry.sn);
        if (!unit) return;
        const fields = {};
        if (unit.gpsLicense !== entry.gpsLicense) fields.gpsLicense = entry.gpsLicense;
        if (unit.licenseDisplay !== entry.licenseDisplay) fields.licenseDisplay = entry.licenseDisplay;
        if (unit.gpsLicenseStartDate !== entry.gpsLicenseStartDate) fields.gpsLicenseStartDate = entry.gpsLicenseStartDate;
        if (unit.gpsLicenseEndDate !== entry.gpsLicenseEndDate) fields.gpsLicenseEndDate = entry.gpsLicenseEndDate;
        if (Object.keys(fields).length > 0) {
            updateUnit(unit.id, fields);
            updated++;
        }
    });

    localStorage.setItem(FLAG, '1');
    if (updated > 0) {
        showToast(`License migration (batch 2): ${updated} unit(s) updated`, 'success');
        if (currentView === 'dashboard') { filteredData = [...globalData]; onDataLoaded(); }
        else if (currentView === 'editUnits') renderEditTable();
    }
    console.log(`[migration] license batch 2: ${updated} updated out of ${data.length}`);
}

// One-shot site assignment migration — 2025-05-25
function migrateSiteData20260525() {
    const FLAG = 'site_migration_20260525_done';
    if (localStorage.getItem(FLAG)) return;
    if (!globalData.length) return;

    const data = [
        {sn:"1T8C570HKST250056",site:"PT. GPA"},
        {sn:"1YR6150BASU540056",site:"PT. GPA"},
        {sn:"1YR6150BCSU540068",site:"PT. GPA"},
        {sn:"1YR6150BVSU540055",site:"PT. GPA"},
        {sn:"1BM7230CVS3001122",site:"PT. GPA"},
        {sn:"1BM7230CKS3001133",site:"PT. GPA"},
        {sn:"1BM7230CJS3001134",site:"PT. GPA"},
        {sn:"1BM7230CPS3001115",site:"PT. GPA"},
        {sn:"1BM7230CKS3001147",site:"PT. GPA"},
        {sn:"1BM7230CLS3001141",site:"PT. GPA"},
        {sn:"1BM7230CCS3001113",site:"PT. GPA"},
        {sn:"1BM7230CHS3001125",site:"PT. GPA"},
        {sn:"1BM7230CHS3001139",site:"PT. GPA"},
        {sn:"1BM7230CKS3001150",site:"PT. GPA"},
        {sn:"1BM7230CLS3001107",site:"PT. GPA"},
        {sn:"1BM7230CCS3001118",site:"PT. GPA"},
        {sn:"1BM7230CES3001143",site:"PT. GPA"},
        {sn:"1BM7230CCS3001149",site:"PT. GPA"},
        {sn:"1BM7230CCS3001127",site:"PT. GPA"},
        {sn:"1BM7230CTS3001128",site:"PT. GPA"},
        {sn:"1BM7230CES3001045",site:"PT. GPA"},
        {sn:"1BM7230CKS3001049",site:"PT. GPA"},
        {sn:"1BM7230CCS3001077",site:"PT. GPA"},
        {sn:"1BM7230CKS3001083",site:"PT. GPA"},
        {sn:"1BM7230CES3001076",site:"PT. GPA"},
        {sn:"1BM7230CJS3001084",site:"PT. GPA"},
        {sn:"1BM7230CCS3001104",site:"PT. GPA"},
        {sn:"1BM7230CAS3001090",site:"PT. GPA"},
        {sn:"1BM7230CAS3001087",site:"PT. GPA"},
        {sn:"1BM7230CES3001093",site:"PT. GPA"},
        {sn:"1BM7230CLS3001088",site:"PT. GPA"},
        {sn:"1BM7230CKS3001102",site:"PT. GPA"},
        {sn:"1BM7230CHS3001108",site:"PT. GPA"},
        {sn:"1BM7230CES3001112",site:"PT. GPA"},
        {sn:"1BM7230CJS3001036",site:"PT. GPA"},
        {sn:"1BM7230CLS3001057",site:"PT. GPA"},
        {sn:"1BM7230CLS3001026",site:"PT. GPA"},
        {sn:"1BM7230CCS3001071",site:"PT. GPA"},
        {sn:"1BM7230CCS3001068",site:"PT. GPA"},
        {sn:"1BM7230CKS3001066",site:"PT. GPA"},
        {sn:"1BM7230CHS3001075",site:"PT. GPA"},
        {sn:"1BM7230CAS3001073",site:"PT. GPA"},
        {sn:"1BM7230CJS3001053",site:"PT. GPA"},
        {sn:"1BM7230CPS3001051",site:"PT. GPA"},
        {sn:"1BM7230CCS3001054",site:"PT. GPA"},
        {sn:"1BM7230CKS3001052",site:"PT. GPA"},
        {sn:"1BM7230CVS3001069",site:"PT. GPA"},
        {sn:"1BM7230CVS3001072",site:"PT. GPA"},
        {sn:"1BM7230CJS3001067",site:"PT. GPA"},
        {sn:"1RW8310DCSA260870",site:"PT. GPA"},
        {sn:"1RW8310DLSA260881",site:"PT. GPA"},
        {sn:"1RW8310DESB260912",site:"PT. GPA"},
        {sn:"1RW8310DPSB261028",site:"PT. GPA"},
        {sn:"1RW8310DCSB261180",site:"PT. GPA"},
        {sn:"1RW8310DASB261036",site:"PT. GPA"},
        {sn:"1RW8310DLSB261152",site:"PT. GPA"},
        {sn:"1RW8310DPSB260929",site:"PT. GPA"},
        {sn:"1RW8310DCSB261222",site:"PT. GPA"},
        {sn:"1RW8310DPSB260963",site:"PT. GPA"},
        {sn:"1RW8310DASB260937",site:"PT. GPA"},
        {sn:"1RW8310DHSB260973",site:"PT. GPA"},
        {sn:"1RW8310DPSB261126",site:"PT. GPA"},
        {sn:"1RW8310DCSB261205",site:"PT. GPA"},
        {sn:"1RW8310DHSB261010",site:"PT. GPA"},
        {sn:"1RW8310DPSB260946",site:"PT. GPA"},
        {sn:"1RW8310DPSB261000",site:"PT. GPA"},
        {sn:"1RW8310DVSB261200",site:"PT. GPA"},
        {sn:"1RW8310DCSB261096",site:"PT. GPA"},
        {sn:"1BM7230CTS3001095",site:"PT. GPA"},
        {sn:"1NW4025MKS0250246",site:"PT. GPA"},
        {sn:"1NW4025MCS0250248",site:"PT. GPA"},
        {sn:"1NW4025MVS0250249",site:"PT. GPA"},
        {sn:"1T8C570HHST260045",site:"PT. GPA"},
        {sn:"1T8C570HEST260046",site:"PT. GPA"},
        {sn:"1T8C570HTST260048",site:"PT. GPA"},
        {sn:"1T8C570HPST260049",site:"PT. GPA"},
        {sn:"1BM7230CKS3002332",site:"PT. GPA"},
        {sn:"1BM7230CPS3002331",site:"PT. GPA"},
        {sn:"1BM7230CCS3002320",site:"PT. GPA"},
        {sn:"1BM7230CHS3002324",site:"PT. GPA"},
        {sn:"1BM7230CCS3002334",site:"PT. GPA"},
        {sn:"1BM7230CHS3002355",site:"PT. GPA"},
        {sn:"1BM7230CKS3002346",site:"PT. GPA"},
        {sn:"1BM7230CVS3002349",site:"PT. GPA"},
        {sn:"1BM7230CJS3002350",site:"PT. GPA"},
        {sn:"1BM7230CCS3002348",site:"PT. GPA"},
        {sn:"1BM7230CCS3002351",site:"PT. GPA"},
        {sn:"1BM7230CPS3002345",site:"PT. GPA"},
        {sn:"1BM7230CAS3002353",site:"PT. GPA"},
        {sn:"1BM7230CTS3002344",site:"PT. GPA"},
        {sn:"1BM7230CLS3002340",site:"PT. GPA"},
        {sn:"1BM7230CAT3002368",site:"PT. GPA"},
        {sn:"1BM7230CTT3002376",site:"PT. GPA"},
        {sn:"1BM7230CET3002374",site:"PT. GPA"},
        {sn:"1BM7230CKT3002378",site:"PT. GPA"},
        {sn:"1BM7230CTT3002359",site:"PT. GPA"},
        {sn:"1BM7230CET3002360",site:"PT. GPA"},
        {sn:"1BM7230CHT3002390",site:"PT. GPA"},
        {sn:"1BM7230CET3002388",site:"PT. GPA"},
        {sn:"1BM7230CPS3001132",site:"PT. MNM"},
        {sn:"1BM7230CES3001126",site:"PT. MNM"},
        {sn:"1BM7230CJS3001117",site:"PT. MNM"},
        {sn:"1BM7230CLS3001124",site:"PT. MNM"},
        {sn:"1BM7230CAS3001137",site:"PT. MNM"},
        {sn:"1BM7230CPS3001129",site:"PT. MNM"},
        {sn:"1BM7230CCS3001080",site:"PT. MNM"},
        {sn:"1BM7230CTS3001050",site:"PT. MNM"},
        {sn:"1BM7230CPS3001065",site:"PT. MNM"},
        {sn:"1BM7230CJS3001098",site:"PT. MNM"},
        {sn:"1BM7230CPS3001101",site:"PT. MNM"},
        {sn:"1BM7230CCS3001135",site:"PT. MNM"},
        {sn:"1BM7230CCS3001121",site:"PT. MNM"},
        {sn:"1BM7230CLS3001110",site:"PT. MNM"},
        {sn:"1BM7230CES3001028",site:"PT. MNM"},
        {sn:"1BM7230CCS3001063",site:"PT. MNM"},
        {sn:"1BM7230CTS3001047",site:"PT. MNM"},
        {sn:"1BM7230CPS3001082",site:"PT. MNM"},
        {sn:"1BM7230CTS3001078",site:"PT. MNM"},
        {sn:"1BM7230CLS3001060",site:"PT. MNM"},
        {sn:"1RW8310DKSA260873",site:"PT. MNM"},
        {sn:"1RW8310DCSA260853",site:"PT. MNM"},
        {sn:"1RW8310DLSB260910",site:"PT. MNM"},
        {sn:"1BM7230CTS3001114",site:"PT. MNM"},
        {sn:"1RW8310DPSA260905",site:"PT. MNM"},
        {sn:"1RW8310DHSB261105",site:"PT. MNM"},
        {sn:"1NW4025MJS0250247",site:"PT. MNM"},
        {sn:"1NW4025MPS0250245",site:"PT. MNM"},
        {sn:"1T8C570HCST260047",site:"PT. MNM"},
        {sn:"1T8C570HETT260050",site:"PT. MNM"},
        {sn:"1BM7230CKS3002329",site:"PT. MNM"},
        {sn:"1BM7230CCS3002326",site:"PT. MNM"},
        {sn:"1BM7230CES3002325",site:"PT. MNM"},
        {sn:"1BM7230CTS3002330",site:"PT. MNM"},
        {sn:"1BM7230CAS3002322",site:"PT. MNM"},
        {sn:"1BM7230CHS3002338",site:"PT. MNM"},
        {sn:"1BM7230CLS3002323",site:"PT. MNM"},
        {sn:"1BM7230CLS3002337",site:"PT. MNM"},
        {sn:"1BM7230CVS3002352",site:"PT. MNM"},
        {sn:"1BM7230CHS3002341",site:"PT. MNM"},
        {sn:"1BM7230CES3002339",site:"PT. MNM"},
        {sn:"1BM7230CJS3002347",site:"PT. MNM"},
        {sn:"1BM7230CES3002342",site:"PT. MNM"},
        {sn:"1BM7230CCS3002343",site:"PT. MNM"},
        {sn:"1BM7230CES3002356",site:"PT. MNM"},
        {sn:"1BM7230CKT3002381",site:"PT. MNM"},
        {sn:"1BM7230CCT3002389",site:"PT. MNM"},
        {sn:"1BM7230CCT3002361",site:"PT. MNM"},
        {sn:"1BM7230CJT3002379",site:"PT. MNM"},
        {sn:"1BM7230CAT3002371",site:"PT. MNM"},
        {sn:"1BM7230CCT3002383",site:"PT. MNM"},
        {sn:"1BM7230CVT3002370",site:"PT. MNM"},
        {sn:"1BM7230CHT3002387",site:"PT. MNM"},
        {sn:"1BM7230CVT3002384",site:"PT. MNM"},
        {sn:"1BM7230CAT3002385",site:"PT. MNM"},
        {sn:"1BM7230CCT3002375",site:"PT. MNM"},
        {sn:"1BM7230CPT3002363",site:"PT. MNM"},
        {sn:"1BM7230CCT3002366",site:"PT. MNM"},
        {sn:"1BM7230CLT3002386",site:"PT. MNM"}
    ];

    let updated = 0;
    data.forEach(entry => {
        const unit = globalData.find(u => (u.sn || '').trim() === entry.sn);
        if (!unit) return;
        if (unit.site !== entry.site) {
            updateUnit(unit.id, { site: entry.site });
            updated++;
        }
    });

    localStorage.setItem(FLAG, '1');
    if (updated > 0) {
        showToast(`Site migration: ${updated} unit(s) updated`, 'success');
        if (currentView === 'dashboard') { filteredData = [...globalData]; onDataLoaded(); }
        else if (currentView === 'editUnits') renderEditTable();
    }
    console.log(`[migration] site assignment: ${updated} updated out of ${data.length}`);
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
