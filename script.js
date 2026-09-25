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
// Set only when a photo is actually added or removed. saveDamage writes the
// photo document only then — so correcting a description never rewrites (or,
// worse, wipes) a photo that may not even have finished loading yet.
let _dmgPhotoDirty = false;
let _dmgPhotoLoading = false;
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
let cloudDamageComponentsUnsub = null;
let damageComponents = [];           // [{ id, name, unitField, createdAt }] from Firestore
let _firstDamageComponentsSnapshot = true;

// ---- Team operations (cloud-only, like userCategories/damageComponents) ----
// These three have no legacy local data to migrate, so Firestore is the single
// source of truth; its IndexedDB persistence covers offline reads.
let cloudTeamMembersUnsub = null;
let teamMembers = [];                // [{ id, name, jobTitle, active, createdAt }]
let cloudShiftsUnsub = null;
let teamShifts = [];                 // [{ id:`${date}_${memberId}`, date, memberId, shift }]
let cloudWorkLogsUnsub = null;
let workLogs = [];                   // [{ id, date, memberId, start, end, unitId, task, issue }]
let cloudLeaveUnsub = null;
let leaveRequests = [];              // [{ id, memberId, type, dateFrom, dateTo, days, reason, approval }]

// ---- Warehouse (cloud-only, same pattern as the team collections) ----
let cloudDevicesUnsub = null;
let warehouseDevices = [];           // serial-tracked devices
let cloudStockUnsub = null;
let stockLedger = [];                // IN/OUT rows for quantity-counted items
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
// Build marker, shown in the footer and the account menu. Bumped with the
// service worker's CACHE_NAME on every deploy, so "is this the new version?"
// is answerable by looking at the page instead of guessing at caches.
const APP_VERSION = 'v110';

const STORAGE_KEY = 'tractorUnits';
const IMPLEMENTS_STORAGE_KEY = 'tractorImplements';
const DAMAGE_STORAGE_KEY = 'tractorDamageRecords';
const DAMAGE_TYPES = ['Mekanis', 'Software', 'Device Precision'];
// Built-in components seeded into the manageable list. `unitField` links a
// component to the unit status column it controls; custom components added
// later have no field, so they flip the whole unit's status instead.
const DAMAGE_COMPONENTS_SEED_KEY = 'tractorDamageComponentsSeeded_v1';
const DEFAULT_DAMAGE_COMPONENTS = [
    { name: 'GPS', unitField: 'gps' },
    { name: 'Display', unitField: 'display' },
    { name: 'JDLink', unitField: 'jdlink' },
    { name: 'Steering Sensor', unitField: 'steering' }
];
// Fallback map for the built-ins (used when the cloud list hasn't loaded yet).
const DAMAGE_COMPONENT_FIELD = { 'GPS': 'gps', 'Display': 'display', 'Steering Sensor': 'steering', 'JDLink': 'jdlink' };

// Which unit status field (if any) a component name controls.
function componentUnitField(name) {
    if (!name) return null;
    const c = damageComponents.find(x => (x.name || '').toLowerCase() === name.toLowerCase());
    if (c) return c.unitField || null;
    return DAMAGE_COMPONENT_FIELD[name] || null;
}
// Matched to WORKLOG_PHOTO_OPTS. The old budget was 1280px / 900KB — 1.5x the
// pixels and 4.5x the bytes of a work-log photo, for no reason anyone recorded,
// and with no cap on how many records could carry one. A field photo of a
// broken part is legible at 1024px, and every byte here used to be re-read by
// every device on every app open.
const DAMAGE_PHOTO_MAX_DIM = 1024;     // longest-side px after resize
const DAMAGE_PHOTO_QUALITY = 0.7;      // initial JPEG quality
const DAMAGE_PHOTO_MAX_BYTES = 200 * 1024;
const DAMAGE_PHOTOS_SPLIT_KEY = 'tractorDamagePhotosSplit';
const LICENSE_STORAGE_KEY = 'tractorLicenseStock';
const LICENSE_TYPE_DEFAULTS = ['SF-RTK', 'SF-1', 'G5 Basic', 'G5 Advance'];
const LICENSE_LOW_STOCK_THRESHOLD = 5; // "sisa" at or below this → dashboard warning
const PENDING_CHANGES_KEY = 'tractorPendingChanges';
const AUDIT_LOG_KEY = 'tractorAuditLog';
const AUDIT_LOG_MAX = 500;
const BACKUP_RING_KEY = 'tractorUnits_autobackup';
const BACKUP_RING_SIZE = 3;
const WL_PHOTOS_SPLIT_KEY = 'tractorWorkLogPhotosSplit';
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
// Chart.js and SVG stroke attributes need a resolved colour string, not a
// var() reference, so read the palette token at draw time. This keeps charts
// in step with style.css instead of drifting into a second, stale palette.
function themeColor(name, fallback) {
    try {
        const v = getComputedStyle(document.documentElement).getPropertyValue(name).trim();
        if (v) return v;
    } catch (e) { /* pre-render / no document */ }
    return fallback;
}
// Keyed to --color-* in style.css; the fallbacks mirror those token values.
const COMPONENT_COLORS = {
    get display()  { return themeColor('--color-display', '#BC8A2E'); },
    get gps()      { return themeColor('--color-gps', '#5A7DA0'); },
    get steering() { return themeColor('--color-steering', '#4F7B58'); },
    get jdlink()   { return themeColor('--color-jdlink', '#403E3A'); },
    get cameraAi()      { return themeColor('--color-camera-ai', '#6E5A8A'); },
    get telematicBox()  { return themeColor('--color-telematic-box', '#3E6F73'); },
    get switchLimiter() { return themeColor('--color-switch-limiter', '#8A5A3C'); },
    get rotaryLamp()    { return themeColor('--color-rotary-lamp', '#A07A2A'); }
};
// Nothing iterates COMPONENT_LABELS; it is a lookup (saveInlineEdit's toast).
// COMPONENT_KEYS stays tractor-only on purpose: renderComponentHealth reads it
// to draw the John Deere rings, and must not grow heavy rings by accident.
Object.assign(COMPONENT_LABELS, {
    cameraAi: 'Camera AI', telematicBox: 'Telematic Box',
    switchLimiter: 'Switch Limiter', rotaryLamp: 'Rotary Lamp'
});

// ============================================================
// UNIT GROUPS — agricultural equipment and heavy equipment
// ------------------------------------------------------------
// Every unit belongs to one group. The original 190 documents have no
// `unitGroup` field and are NEVER back-filled: a missing value reads as
// 'tractor', so no existing document changes meaning and nothing is written to
// them. Only creating a unit writes the field, and it cannot change afterwards.
//
// Each group owns its own monitored components and its own extra fields. The
// app used to assume every unit had Display/GPS/Steering/JDLink and SF/G5
// licences; a heavy unit read through that assumption looked like four broken
// components, and editing one wrote 'Breakdown' into all four. Anything that
// used to hard-code those four now asks the unit's group instead.
//
// The internal key stays 'tractor' (nothing stores it yet, and it matches the
// existing storage names); the LABEL is what people see. The group actually
// holds tractors, CH570 cane harvesters and sprayers.
// ============================================================
const TRACTOR_ONLY_FIELDS = ['implement', 'display', 'gps', 'steering', 'jdlink',
    'gpsLicense', 'licenseDisplay', 'gpsLicenseStartDate', 'gpsLicenseEndDate',
    'displayLicenseStartDate', 'displayLicenseEndDate', 'licenseStartDate', 'licenseEndDate',
    'gpsLicenseExpiredAt', 'displayLicenseExpiredAt'];
const HEAVY_COMPONENT_KEYS = ['cameraAi', 'telematicBox', 'switchLimiter', 'rotaryLamp'];
const HEAVY_ONLY_FIELDS = ['machineType', 'assetCode', 'workTool', ...HEAVY_COMPONENT_KEYS];
const UNIT_GROUPS = {
    tractor: {
        key: 'tractor', label: 'Agricultural Equipment', shortLabel: 'Pertanian', icon: 'fa-tractor',
        hasLicences: true, csvPrefix: 'tractor_monitoring', onlyFields: TRACTOR_ONLY_FIELDS,
        components: COMPONENT_KEYS.map(k => ({ key: k, label: COMPONENT_LABELS[k],
                                               badge: 'badge-' + COMPONENT_LABELS[k].toLowerCase() }))
    },
    heavy: {
        key: 'heavy', label: 'Heavy Equipment', shortLabel: 'Alat Berat', icon: 'fa-person-digging',
        hasLicences: false, csvPrefix: 'alat_berat_monitoring', onlyFields: HEAVY_ONLY_FIELDS,
        components: [
            { key: 'cameraAi',      label: 'Camera AI',      badge: 'badge-camera-ai' },
            { key: 'telematicBox',  label: 'Telematic Box',  badge: 'badge-telematic-box' },
            { key: 'switchLimiter', label: 'Switch Limiter', badge: 'badge-switch-limiter' },
            { key: 'rotaryLamp',    label: 'Rotary Lamp',    badge: 'badge-rotary-lamp' }
        ]
    }
};
const UNIT_GROUP_KEYS = ['tractor', 'heavy'];
const DASH_GROUP_KEYS = ['tractor', 'heavy', 'all'];
const HEAVY_MACHINE_TYPES = ['Excavator', 'Bulldozer', 'Motor Grader', 'Wheel Loader'];
const HEAVY_WORK_TOOLS = ['Bucket', 'Ripper', 'Blade', 'Breaker'];
const HEAVY_CSV_UPDATABLE_FIELDS = ['name', 'model', 'machineType', 'assetCode', 'workTool', 'status',
    ...HEAVY_COMPONENT_KEYS, 'site', 'yearReceived', 'userCategory', 'remarks', 'breakdownReason'];
// "Alat Kerja", never "Attachment": units already have an "Attachments" file
// column, and the two side by side read as the same thing.
const UNIT_FIELD_LABELS = { machineType: 'Jenis Alat', assetCode: 'Nomor Lambung', workTool: 'Alat Kerja' };

function normalizeGroupKey(raw) {
    const v = String(raw == null ? '' : raw).trim().toLowerCase().replace(/\s+/g, ' ');
    if (!v) return '';
    for (const k of UNIT_GROUP_KEYS) {
        const g = UNIT_GROUPS[k];
        if (v === k || v === g.label.toLowerCase() || v === g.shortLabel.toLowerCase()) return k;
    }
    if (['traktor', 'agricultural', 'agricultural tractor'].includes(v)) return 'tractor';
    if (['alat berat', 'heavy equipment'].includes(v)) return 'heavy';
    return null;
}
function unitGroupOf(u)  { return normalizeGroupKey(u && u.unitGroup) || 'tractor'; }
function isHeavy(u)      { return !!u && unitGroupOf(u) === 'heavy'; }
function groupDef(k)     { return UNIT_GROUPS[k] || UNIT_GROUPS.tractor; }
function otherGroup(k)   { return UNIT_GROUPS[k === 'heavy' ? 'tractor' : 'heavy']; }
function hasHeavyUnits(list)   { return (list || globalData).some(isHeavy); }
function hasTractorUnits(list) { return (list || globalData).some(u => !isHeavy(u)); }
// Always a NEW array. sortTable sorts filteredData in place, so filteredData
// must never be the same array as globalData.
function unitsOfGroup(list, sel) {
    return sel === 'all' ? list.slice() : list.filter(u => unitGroupOf(u) === sel);
}
function groupsInScope(sel) { return sel === 'all' ? ['tractor', 'heavy'] : [sel]; }
function fieldAllowedForGroup(field, g) { return !otherGroup(g).onlyFields.includes(field); }
// Fields of the OTHER group that carry a value — e.g. an excavator saved from
// a stale v108 tab with gps:'Breakdown'. Periksa Data reports and clears them.
function strayGroupFields(u) {
    const g = unitGroupOf(u);
    return otherGroup(g).onlyFields
        .filter(f => !sameStoredValue(u[f], ''))
        .map(f => ({ field: f, value: u[f] }));
}
function heavyComponentField(name) {
    if (!name) return null;
    const n = String(name).trim().toLowerCase();
    const c = UNIT_GROUPS.heavy.components.find(x => x.label.toLowerCase() === n);
    return c ? c.key : null;
}
function tractorComponentOffered(name) {
    const list = (typeof damageComponents !== 'undefined' && damageComponents.length)
        ? damageComponents : DEFAULT_DAMAGE_COMPONENTS;
    const n = String(name || '').toLowerCase();
    return list.some(c => (c.name || '').toLowerCase() === n);
}
// A damage component that belongs to the other group than the unit's.
function componentGroupConflict(name, unit) {
    if (!name || !unit) return false;
    if (isHeavy(unit)) return !heavyComponentField(name) && !!componentUnitField(name);
    return !!heavyComponentField(name) && !tractorComponentOffered(name);
}
function issueBadgeClass(label) {
    if (label === 'Unit') return 'badge-unit';
    for (const k of UNIT_GROUP_KEYS) {
        const c = UNIT_GROUPS[k].components.find(x => x.label === label);
        if (c) return c.badge;
    }
    return 'badge-' + String(label).toLowerCase();
}
function readPref(key, allowed, dflt) {
    try { const v = localStorage.getItem(key); return allowed.includes(v) ? v : dflt; }
    catch (_) { return dflt; }
}
function writePref(key, v) { try { localStorage.setItem(key, v); } catch (_) {} }

// Declared after the consts above on purpose: placing these in the state block
// at the top of the file would read UNIT_GROUP_KEYS in its temporal dead zone
// and stop the whole script. Preferences are loaded by loadUnitGroupPrefs(),
// the first thing setupEventListeners() does.
let editUnitsGroup = 'tractor';   // Edit Units tab asked for (saved or clicked)
let _editGroupPicked = false;     // a click this session beats the zero-heavy fallback
let dashGroupPref = 'tractor';    // dashboard scope asked for
let _editHeadGroup = 'tractor';
let _detailHeadGroup = 'tractor';
const EDIT_HEADS = { tractor: '' };
const DETAIL_HEADS = { tractor: '' };
const csvExplicitGroup = new WeakSet();

function loadUnitGroupPrefs() {
    editUnitsGroup = readPref('editUnitsGroup', UNIT_GROUP_KEYS, 'tractor');
    dashGroupPref = readPref('dashUnitGroup', DASH_GROUP_KEYS, 'tractor');
    // Captured from the static markup before anything renders, so the tractor
    // header is byte-for-byte today's.
    const eh = document.querySelector('#editTable thead');
    const dh = document.querySelector('#detailTable thead');
    if (eh && !EDIT_HEADS.tractor) EDIT_HEADS.tractor = eh.innerHTML;
    if (dh && !DETAIL_HEADS.tractor) DETAIL_HEADS.tractor = dh.innerHTML;
}
// While no heavy unit exists, everything opens exactly as it does today,
// whatever preference a device saved.
function effectiveEditGroup() {
    if (editUnitsGroup === 'heavy' && !_editGroupPicked && !hasHeavyUnits()) return 'tractor';
    return editUnitsGroup;
}
function effectiveDashGroup() {
    if (!hasHeavyUnits()) return 'tractor';
    if (!hasTractorUnits()) return 'heavy';
    return dashGroupPref;
}
function scopeDashUnits(list) { return unitsOfGroup(list || globalData, effectiveDashGroup()); }
function unitEditTarget(u) { return isHeavy(u) ? 'editUnits:heavy' : 'editUnits'; }

// ---- Chart.js Global Config (HD rendering on all screens) ----
// Chart.js is loaded from a CDN, which can be blocked or simply unreachable in
// the field. Touching it unguarded here would throw at top level and abort the
// rest of this file: every `const` below would stay uninitialised while the
// hoisted function declarations still look alive, so the app would fail in a
// deeply confusing way instead of just losing its charts.
if (window.Chart) {
    Chart.defaults.devicePixelRatio = Math.max(window.devicePixelRatio || 1, 2);
} else {
    console.warn('[chart] Chart.js tidak termuat — grafik dinonaktifkan, sisa aplikasi tetap berjalan.');
}

// Every chart goes through here. Without Chart.js the canvas is replaced by a
// short note and the caller simply gets null back, so the rest of the render
// (KPIs, repair table, alerts) still runs instead of dying on a missing CDN.
function makeChart(canvasId, config) {
    const canvas = document.getElementById(canvasId);
    if (!canvas) return null;
    if (!window.Chart) {
        const host = canvas.parentNode;
        if (host && !host.querySelector('.chart-unavailable')) {
            const note = document.createElement('div');
            note.className = 'chart-unavailable';
            note.textContent = 'Grafik tidak tersedia — pustaka grafik gagal dimuat.';
            host.appendChild(note);
        }
        canvas.style.display = 'none';
        return null;
    }
    canvas.style.display = '';
    return new Chart(canvas, config);
}

// ---- Initialization ----
document.addEventListener('DOMContentLoaded', () => {
    // Light-only theme (warm editorial). Clear any stale dark preference so the
    // app never renders the retired dark mode.
    document.body.classList.remove('dark');

    setupEventListeners();
    setupKeyboardShortcuts();
    showAppVersion();
    watchConnection();
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
    loadUnitGroupPrefs();
    // The damage component list follows the chosen unit's group.
    const dmgUnitEl = document.getElementById('dmgUnit');
    if (dmgUnitEl) {
        let t = null;
        dmgUnitEl.addEventListener('change', onDamageUnitChanged);
        dmgUnitEl.addEventListener('input', () => { clearTimeout(t); t = setTimeout(onDamageUnitChanged, 150); });
    }
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
    dropZone.addEventListener('dragover', e => { e.preventDefault(); dropZone.style.borderColor = themeColor('--info', '#5A7DA0'); });
    dropZone.addEventListener('dragleave', () => { dropZone.style.borderColor = ''; });
    dropZone.addEventListener('drop', e => {
        e.preventDefault();
        dropZone.style.borderColor = '';
        const file = e.dataTransfer.files[0];
        if (file && file.name.endsWith('.csv')) {
            handleEditCSVImport(file);
        } else {
            showToast('Silakan unggah berkas .csv', 'error');
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

// Escape used to reach exactly three of the eighteen modals, even though every
// one of them already has a close function. The rest trapped the keyboard: the
// only way out was the mouse. One map, so a new modal is one line here rather
// than an edit to the key handler.
//
// The value is a function NAME, not the function: most of these are defined
// further down this file and would be undefined at this point.
const MODAL_CLOSERS = {
    unitModal:            'closeModal',
    historyModal:         'closeHistory',
    importReportModal:    'closeImportReport',
    phantomHistoryModal:  'closePhantomHistory',
    emailSettingsModal:   'closeEmailSettingsModal',
    bulkEditModal:        'closeBulkEdit',
    implementModal:       'closeImplementModal',
    unitProfileModal:     'closeUnitProfile',
    damageModal:          'closeDamageModal',
    licenseModal:         'closeLicenseModal',
    categoriesModal:      'closeCategoriesModal',
    damageComponentsModal:'closeDamageComponentsModal',
    accessModal:          'closeAccessModal',
    teamMembersModal:     'closeTeamMembersModal',
    workLogModal:         'closeWorkLogModal',
    leaveModal:           'closeLeaveModal',
    deviceModal:          'closeDeviceModal',
    stockModal:           'closeStockModal',
    // Not a plain dismissal: an abandoned breakdown reason has to put the
    // inline cell back, or the grid shows a status that was never saved.
    restoreReportModal:   'closeRestoreReport',
    autoBackupModal:      'closeAutoBackups',
    breakdownReasonModal: 'cancelBreakdownReason'
};

// Closes the topmost open modal only. Closing all of them at once would
// dismiss the page behind a confirmation dialog the user is still reading.
// The lightbox has its own handler (with arrow-key navigation) and sits above
// everything, so it is checked first.
function closeTopModal() {
    const lightbox = document.getElementById('photoLightbox');
    if (lightbox && lightbox.classList.contains('open')) { closePhotoLightbox(); return true; }

    const open = Array.from(document.querySelectorAll('.modal-overlay.open'));
    if (open.length === 0) return false;
    const top = open[open.length - 1];
    const fn = window[MODAL_CLOSERS[top.id]];
    if (typeof fn === 'function') {
        // closeWorkLogModal(force) must be called WITHOUT force, so the
        // unsaved-photo guard still gets to ask.
        fn();
        return true;
    }
    // A modal with no registered closer is a bug, but stranding the keyboard
    // is worse than closing it bluntly.
    console.warn(`[ui] ${top.id} belum terdaftar di MODAL_CLOSERS`);
    top.classList.remove('open');
    return true;
}

// Focus goes back where it came from when a modal closes. Without this, Tab
// after closing restarts at the top of the document and the place the person
// was working is lost — the reason keyboard users avoid modals.
let _focusBeforeModal = null;

function rememberFocus() {
    const el = document.activeElement;
    _focusBeforeModal = (el && el !== document.body) ? el : null;
}

function restoreFocus() {
    const el = _focusBeforeModal;
    _focusBeforeModal = null;
    if (el && document.contains(el) && typeof el.focus === 'function') {
        try { el.focus(); } catch (_) {}
    }
}

// Wiring remember/restore into all eighteen open and close functions would be
// thirty-six edits that the next modal would forget to repeat. Watch the class
// instead: every modal in this app opens and closes by toggling .open.
function watchModalFocus() {
    const overlays = document.querySelectorAll('.modal-overlay');
    if (!overlays.length || typeof MutationObserver !== 'function') return;
    const anyOpen = () => !!document.querySelector('.modal-overlay.open');
    let wasOpen = anyOpen();

    const obs = new MutationObserver(() => {
        const isOpen = anyOpen();
        if (isOpen === wasOpen) return;
        wasOpen = isOpen;
        if (isOpen) return;      // remembered on the way in, below
        restoreFocus();
    });

    overlays.forEach(el => {
        obs.observe(el, { attributes: true, attributeFilter: ['class'] });
    });

    // Remember before the modal opens, while the trigger still has focus —
    // by the time the class lands, focus may already have moved.
    document.addEventListener('mousedown', () => { if (!anyOpen()) rememberFocus(); }, true);
    document.addEventListener('keydown', e => {
        if ((e.key === 'Enter' || e.key === ' ') && !anyOpen()) rememberFocus();
    }, true);
}

// The 34 sortable headers are styled as controls and carry an onclick, but had
// no tabindex, no role and no aria-sort: unreachable by keyboard and silent to
// a screen reader. index.html now gives them tabindex/role; this makes the keys
// work, and marks which column is sorted.
//
// Delegated rather than 34 inline onkeydown attributes, so a new sortable
// column needs nothing but the two attributes.
function setupSortableHeaders() {
    document.addEventListener('keydown', e => {
        if (e.key !== 'Enter' && e.key !== ' ') return;
        const th = e.target.closest && e.target.closest('th[role="button"]');
        if (!th) return;
        e.preventDefault();          // Space would scroll the page
        th.click();
    });
}

// Mark the sorted column. This also lights the .sort-icon: style.css:678 has
// styled th.sorted since the beginning, but nothing ever added the class, so
// the arrow never turned on.
function markSortedHeader(theadSelector, key, asc) {
    const head = document.querySelector(theadSelector);
    if (!head) return;
    head.querySelectorAll('th[role="button"]').forEach(th => {
        const onclick = th.getAttribute('onclick') || '';
        const isThis = key && onclick.includes(`'${key}'`);
        th.classList.toggle('sorted', !!isThis);
        if (isThis) th.setAttribute('aria-sort', asc ? 'ascending' : 'descending');
        else th.removeAttribute('aria-sort');
    });
}

function setupKeyboardShortcuts() {
    watchModalFocus();
    setupSortableHeaders();
    document.addEventListener('keydown', e => {
        const tag = e.target.tagName;
        const isTyping = tag === 'INPUT' || tag === 'TEXTAREA' || e.target.isContentEditable;

        if (e.key === 'Escape') {
            // Leave the field first, and leave the modal alone. Escape is the
            // reflex for dismissing a datalist suggestion, and closing the unit
            // form on that keystroke threw away everything typed into it — no
            // confirmation, no undo. Closing stays on the X and Batal buttons.
            if (isTyping) { e.target.blur(); return; }
            closeTopModal();
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

// Stamp the running build into the footer. Runs regardless of sign-in state,
// so the version is readable even from the login screen.
function showAppVersion() {
    const el = document.getElementById('appVersion');
    if (el) el.textContent = APP_VERSION;
}

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

// ---- CSV output ----
// Quoting alone does NOT stop a spreadsheet executing a cell: Excel and Google
// Sheets evaluate any cell whose text begins with = + - @ (or a leading tab /
// carriage return) when the file is opened. Every free-text field here —
// Nickname, Remarks, Deskripsi, Catatan — is user supplied and lands in an
// export other people open, so prefix those with an apostrophe, which
// spreadsheets strip on display but never evaluate.
function csvCell(value) {
    let v = value == null ? '' : String(value);
    if (/^[=+\-@\t\r]/.test(v)) v = "'" + v;
    return '"' + v.replace(/"/g, '""') + '"';
}

function toCSV(headers, rows) {
    return [headers, ...rows].map(row => row.map(csvCell).join(',')).join('\n');
}

// ---- Calendar dates (YYYY-MM-DD), always in the viewer's LOCAL time ----
// Every date in this app is a calendar day, not an instant, but the built-ins
// treat them as UTC: `new Date('2026-09-01')` is UTC midnight, and
// `toISOString().slice(0,10)` reports the UTC day. That skews a whole day
// wherever the local date differs from the UTC one — one day EARLY west of
// UTC, and in WIB/WITA any time before 07:00/08:00 local still reports
// yesterday. It stopped being cosmetic once applyExpiredLicenseDowngrades
// began WRITING a tier downgrade off getExpiryStatus.
function parseLocalDate(value) {
    const m = /^(\d{4})-(\d{2})-(\d{2})/.exec(String(value || '').trim());
    if (m) return new Date(+m[1], +m[2] - 1, +m[3]);   // local midnight
    const d = new Date(value);
    return isNaN(d.getTime()) ? null : d;
}

function toISODate(d = new Date()) {
    if (!d || isNaN(d.getTime())) return '';
    const p = n => String(n).padStart(2, '0');
    return `${d.getFullYear()}-${p(d.getMonth() + 1)}-${p(d.getDate())}`;
}

// Calendar date exactly one year on, or '' when the input isn't a date.
function addOneYearISO(value) {
    const d = parseLocalDate(value);
    if (!d) return '';
    d.setFullYear(d.getFullYear() + 1);
    return toISODate(d);
}

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
    // Per-user access gating: the user needs at least 'view' on the area.
    if (GATED_VIEWS.includes(view) && !canViewView(view)) {
        showToast('Anda tidak punya akses ke menu ini', 'warning');
        view = 'dashboard';
    }
    if (view === 'users' && !isOwner()) {
        showToast('Khusus owner', 'warning');
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
    const teamView = document.getElementById('viewTeam');
    if (teamView) teamView.style.display = (view === 'team') ? 'block' : 'none';
    const whView = document.getElementById('viewWarehouse');
    if (whView) whView.style.display = (view === 'warehouse') ? 'block' : 'none';
    const leaderView = document.getElementById('viewLeader');
    if (leaderView) leaderView.style.display = (view === 'leader') ? 'block' : 'none';
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
            filteredData = scopeDashUnits();
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

    if (view === 'team') {
        populateWorkLogFilters();
        renderTeamView();
    }

    if (view === 'warehouse') {
        populateWarehouseFilters();
        renderWarehouseView();
    }

    if (view === 'leader') {
        // Pending sign-ups only load once the owner's user subscription runs.
        if (isOwner()) ensureUsersSubscription();
        renderLeaderView();
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
        showToast('Penyimpanan penuh. Data tidak tersimpan.', 'error');
    }
}

function writeAutoBackup(data) {
    try {
        const ring = JSON.parse(localStorage.getItem(BACKUP_RING_KEY) || '[]');
        ring.push({ at: Date.now(), count: data.length, units: data });
        while (ring.length > BACKUP_RING_SIZE) ring.shift();
        localStorage.setItem(BACKUP_RING_KEY, JSON.stringify(ring));
        _autoBackupFailed = false;
    } catch (e) {
        // This used to be swallowed in silence, which is the worst possible
        // place for silence: the ring quietly stopped updating while the owner
        // went on believing they had three rollback points. Said once per
        // failure run so a full disk does not become a wall of toasts.
        console.error('[backup] auto backup failed:', e);
        if (!_autoBackupFailed) {
            _autoBackupFailed = true;
            showToast('Cadangan otomatis berhenti — penyimpanan browser penuh. Export Backup sekarang.', 'error');
        }
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
    if (!requireEdit('editUnits')) return;
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
    if (!requireEdit('editUnits')) return;
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

// Compare two field values the way the audit log will store them. logEvent
// coerces both sides through String(), so 2020 and '2020' land as the same
// entry — a strict !== here records a "change" whose Sebelum and Sesudah
// columns read identically on screen. Values arrive as strings from forms and
// CSV but as numbers from a JSON backup, so this is not hypothetical.
function sameStoredValue(a, b) {
    return String(a == null ? '' : a).trim() === String(b == null ? '' : b).trim();
}

// Last-resort client-side gate on every write to the units collection.
//
// Every other guard in this file sits at the call site, so a new caller
// inherits no protection at all. That is exactly how four automatic
// migrations came to rewrite unit names, licences and site assignments under
// accounts that had no edit rights: the rules rejected the writes, but
// logEvent had already recorded them, so the history filled with changes that
// never happened. Guarding the writers themselves means a future caller has to
// opt out deliberately instead of merely forgetting.
//
// Silent by design. Legitimate callers already went through
// requireEdit('editUnits'), which shows the message; a loop over hundreds of
// units must not raise hundreds of toasts.
function canWriteUnits(what) {
    if (hasAccess('editUnits', 'edit')) return true;
    console.warn(`[access] ${what} blocked — no edit rights on units`);
    return false;
}

function addUnits(newUnits) {
    if (!canWriteUnits('addUnits')) {
        return { added: 0, skipped: (newUnits || []).length, skippedDetails: [] };
    }
    const existingSNs = new Set(globalData.map(d => (d.sn || '').toLowerCase()));
    const toAdd = [];
    const skippedDetails = [];
    let skipped = 0;

    const existingById = new Map(globalData.map(d => [d.id, d]));
    newUnits.forEach(u => {
        // Group first. addUnits never invents a group — callers that create a
        // unit stamp it — but it refuses one it cannot read, and refuses an id
        // that already belongs to a unit of the other group.
        if ('unitGroup' in u) {
            const k = normalizeGroupKey(u.unitGroup);
            if (k === null) {
                skipped++;
                skippedDetails.push({ name: u.name, sn: u.sn, reason: `Kelompok "${u.unitGroup}" tidak dikenal` });
                return;
            }
            if (k === '') delete u.unitGroup; else u.unitGroup = k;
        }
        if (u.id && existingById.has(u.id) && unitGroupOf(existingById.get(u.id)) !== unitGroupOf(u)) {
            skipped++;
            skippedDetails.push({ name: u.name, sn: u.sn, reason: 'ID dipakai unit kelompok lain' });
            return;
        }
        // A unit never carries the other group's fields. Tractor inputs today
        // carry none, so for them this deletes nothing.
        otherGroup(unitGroupOf(u)).onlyFields.forEach(f => { delete u[f]; });

        if (!u.id) u.id = generateId();
        const snLower = (u.sn || '').toLowerCase();
        if (snLower && existingSNs.has(snLower)) {
            skipped++;
            skippedDetails.push({ name: u.name, sn: u.sn, reason: 'Duplicate serial number' });
        } else {
            if (!u.downtimeHistory) u.downtimeHistory = [];
            if (!isGood(u.status)) {
                u.breakdownStartedAt = Date.now();
                if (!u.breakdownReason) u.breakdownReason = 'Diset via impor CSV';
            }
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

// The write firewall for unit groups. Drops undefined (Firestore here has no
// ignoreUndefinedProperties), always drops unitGroup (a group is set once, at
// creation), and drops fields owned by the other group — so no path, however
// it was reached, can write GPS onto an excavator or Camera AI onto a tractor.
// The single exception is Periksa Data clearing such a stray field to ''.
function guardUnitFields(before, fields, opts) {
    const g = unitGroupOf(before);
    const out = {};
    const dropped = [];
    Object.keys(fields || {}).forEach(k => {
        const v = fields[k];
        if (v === undefined) { dropped.push(k); return; }
        if (k === 'unitGroup') { dropped.push(k); return; }
        if (!fieldAllowedForGroup(k, g) && !(opts && opts.clearStray && v === '')) { dropped.push(k); return; }
        out[k] = v;
    });
    if (dropped.length) console.warn(`[group] ${dropped.join(', ')} diabaikan untuk unit ${groupDef(g).shortLabel}`);
    return out;
}

function updateUnit(id, fields, opts) {
    if (!canWriteUnits('updateUnit')) return false;
    const idx = globalData.findIndex(d => d.id === id);
    if (idx === -1) return false;

    const before = { ...globalData[idx] };
    const guarded = guardUnitFields(before, fields, opts);
    // Only a payload the guard emptied is refused; updateUnit(id, {}) keeps
    // today's behaviour.
    if (Object.keys(fields || {}).length && !Object.keys(guarded).length) return false;
    fields = guarded;
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
        if (!sameStoredValue(before[field], fields[field])) {
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
    'displayLicenseStartDate', 'displayLicenseEndDate', 'remarks', 'breakdownReason'
];

function bulkUpdateUnitsFromCSV(parsedUnits) {
    if (!canWriteUnits('bulkUpdateUnitsFromCSV')) {
        return { updated: 0, unchanged: 0, failed: [] };
    }
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
        const g = unitGroupOf(before);
        // A row that names a group can only update a unit of that group: a
        // CSV never moves a unit between groups.
        if (csvExplicitGroup.has(p) && normalizeGroupKey(p.unitGroup) !== g) {
            failed.push({ sn: p.sn, reason: 'Kelompok di CSV berbeda dengan unit — tidak dipindah' });
            return;
        }
        const fields = {};
        (g === 'heavy' ? HEAVY_CSV_UPDATABLE_FIELDS : CSV_UPDATABLE_FIELDS).forEach(f => {
            const val = p[f];
            if (val !== undefined && val !== null && String(val).trim() !== '' &&
                !sameStoredValue(val, before[f])) {
                fields[f] = val;
            }
        });

        if (Object.keys(fields).length === 0) {
            unchanged++;
            return;
        }

        // A CSV can push a unit into Breakdown without naming a reason, which
        // leaves the Alasan Breakdown column blank — the same gap Bulk Edit
        // already guards against. Keep a real reason if one is already on the
        // unit, otherwise record where it came from.
        if (fields.status !== undefined && !isGood(fields.status) && !fields.breakdownReason) {
            if (!(!isGood(before.status) && before.breakdownReason) &&
                !sameStoredValue('Diset via impor CSV', before.breakdownReason)) {
                fields.breakdownReason = 'Diset via impor CSV';
            }
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
// ============================================================
// OFFLINE-SAFE WRITES
// ------------------------------------------------------------
// Firestore queues writes made offline and replays them later, but the promise
// setDoc() returns only settles once the SERVER confirms. Offline it stays
// pending forever, so anything inside .then() never runs. That is why these two
// helpers exist: the audit entry and the user's confirmation must not depend on
// a promise that will not settle until the signal comes back.
// ============================================================

// Records the failure of a change that was already logged as done. Called from
// .catch(), so the trail says "attempted, then rejected" rather than implying
// the change stuck.
function logEventFailed(entry, err) {
    logEvent({
        ...entry,
        action: 'error',
        before: entry.after != null ? String(entry.after) : '',
        after: `GAGAL (${(err && err.code) || 'error'}) — perubahan tidak tersimpan`
    });
}

// Wraps a cloud write so the user always hears something. Online, the success
// toast waits for the server as before. Offline — or if the server is simply
// slow — it says the change is held on the device, which is the truth.
function saveWithFeedback(promise, successMsg, onError) {
    let settled = false;
    // Offline we know immediately; online we give the server a fair chance
    // before claiming anything.
    const delay = navigator.onLine ? 8000 : 0;
    const timer = setTimeout(() => {
        if (settled) return;
        showToast('Tersimpan di perangkat — akan terkirim saat sinyal kembali', 'info');
    }, delay);
    return promise.then(value => {
        settled = true;
        clearTimeout(timer);
        if (successMsg) showToast(successMsg, 'success');
        return value;
    }).catch(err => {
        settled = true;
        clearTimeout(timer);
        // A stale service-worker bundle is not a server refusal, and the
        // caller's own "gagal menyimpan" toast does not tell anyone what to
        // do about it. Say the one thing that fixes it, then let the caller
        // roll back exactly as it would for any other rejection.
        if (err && err.code === 'stale-client') showToast(STALE_CLIENT_MSG, 'warning');
        if (onError) return onError(err);
        throw err;
    });
}

// One place where a cloud write, its audit entry and its user feedback are put
// in the right order for offline. The audit entry is recorded FIRST — logEvent
// writes to the local cache and queues its own cloud push, so it survives a
// signal drop — and only a genuine rejection adds the cancelling entry.
function cloudWrite(auditEntry, promise, successMsg, onError) {
    // An array is allowed because one action can change several fields, and
    // each field gets its own row — updateUnit is the usual case.
    const entries = Array.isArray(auditEntry) ? auditEntry.filter(Boolean)
                  : auditEntry ? [auditEntry] : [];
    entries.forEach(logEvent);
    return saveWithFeedback(promise, successMsg, err => {
        entries.forEach(e => logEventFailed(e, err));
        if (onError) onError(err);
    });
}

const STALE_CLIENT_MSG = 'Muat ulang halaman — versi lama masih aktif, perubahan belum terkirim';

// The service worker serves firebase-init.js network-first but falls back to
// the cache when the fetch fails — which on a bad signal is exactly when
// people are saving. A device can therefore be running today's script.js
// against a firebase-init.js from before a function existed, and a bare
// window.cloud.saveDevice(...) then throws a TypeError out of the click
// handler: no toast, no audit row, and the modal stays open over a save that
// never happened. saveWorkLog already guarded this by hand (see the
// saveWorkLogPhotos check); this is that guard, once, for everyone.
//
// Returns null when the method is missing, so callers read:
//     const fn = cloudFn('saveDevice'); if (!fn) return;
function cloudFn(name) {
    const fn = window.cloud && window.cloud[name];
    if (typeof fn === 'function') return fn.bind(window.cloud);
    console.warn(`[cloud] ${name} tidak ada — firebase-init.js lama masih disajikan`);
    showToast(STALE_CLIENT_MSG, 'warning');
    return null;
}

// The same guard for the cloudWrite(...) call sites. There the call is an
// ARGUMENT — cloudWrite(entry, window.cloud.saveDevice(rec), …) — so a missing
// method throws while the arguments are being evaluated, before cloudWrite can
// do anything about it. Returning a rejected promise instead keeps the whole
// existing failure path intact: the cancelling audit row, the rollback, the
// banner. saveWithFeedback recognises the code and explains what to do.
function cloudCall(name, ...args) {
    const fn = window.cloud && window.cloud[name];
    if (typeof fn === 'function') return fn.apply(window.cloud, args);
    console.warn(`[cloud] ${name} tidak ada — firebase-init.js lama masih disajikan`);
    const err = new Error(`window.cloud.${name} tidak tersedia`);
    err.code = 'stale-client';
    return Promise.reject(err);
}

// The four oldest modules — units, implements, damage, licenseStock — write
// through the cloudPush*/cloudDelete* helpers rather than cloudWrite. Those
// helpers used to swallow a rejection into a console line and a toast reading
// "perubahan tersimpan lokal", which was not true: the change existed only on
// that device and the audit entry had already been written as if it stuck.
// That is how the history filled with changes the server had refused.
//
// Three things happen on a rejection now:
//   1. a cancelling audit entry, so the trail says the change did not stick;
//   2. a re-read from the server, because a rejected write produces no
//      snapshot of its own — without this the screen keeps showing a value
//      that exists nowhere else, until some unrelated change triggers a sync;
//   3. a toast that names a rules rejection as a rules rejection.
//
// Offline is deliberately NOT this path: Firestore never settles the promise
// while offline, so .catch does not run and the queued write still lands.
function cloudWriteFailed(err, opts) {
    const code = (err && err.code) || 'error';
    console.error(`[cloud] ${opts.what} write failed:`, err);

    logEvent({
        action: 'error',
        unitId: opts.unitId || '',
        unitName: opts.label || '-',
        field: opts.what,
        after: `GAGAL (${code}) — perubahan tidak tersimpan`
    });

    if (opts.resync && navigator.onLine) {
        Promise.resolve().then(opts.resync)
            .catch(e => console.error('[cloud] resync failed:', e));
    }

    showToast(code === 'permission-denied'
        ? `Ditolak server — ${opts.what} dikembalikan. Akses Anda tidak mengizinkan perubahan ini.`
        : `Gagal menyimpan ${opts.what} ke cloud — perubahan dikembalikan`, 'error');
}

// Pull the server's copy back over the local one after a rejected write. Each
// reuses the same snapshot applier the live subscription uses, so there is no
// second code path that could drift from it.
function resyncUnits()      { return window.cloud.getAllUnits().then(applyCloudUnitsSnapshot); }
function resyncImplements() { return window.cloud.getAllImplements().then(applyCloudImplementsSnapshot); }
function resyncDamages()    { return window.cloud.getAllDamages().then(applyCloudDamagesSnapshot); }
function resyncLicenses()   { return window.cloud.getAllLicenses().then(applyCloudLicenseSnapshot); }

// The status pill in the topbar doubles as the offline indicator: without it a
// field operator has no way to tell a queued save from a finished one.
function updateConnectionLabel() {
    const el = document.getElementById('connectionLabel');
    if (!el) return;
    if (navigator.onLine) {
        delete document.body.dataset.offline;
        el.textContent = `${globalData.length} Units`;
    } else {
        document.body.dataset.offline = '1';
        el.textContent = 'Offline — perubahan tersimpan di perangkat';
    }
}

function watchConnection() {
    window.addEventListener('online', () => {
        updateConnectionLabel();
        showToast('Kembali online — perubahan yang tertunda sedang dikirim', 'success');
    });
    window.addEventListener('offline', () => {
        updateConnectionLabel();
        showToast('Sinyal hilang — perubahan tetap bisa disimpan di perangkat', 'warning');
    });
    updateConnectionLabel();
}

function deleteUnits(ids) {
    if (!canWriteUnits('deleteUnits')) return { count: 0, removed: [] };
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
                // The rules will keep refusing this batch, so putting it back
                // would only cost the next one too. The local cache still has
                // these rows; they just never become shared.
                showHistoryRulesBanner();
                return;
            }
            // Transient failure. splice(0) above already emptied the queue, so
            // without this the batch is simply gone — the next flush carries
            // it instead. Capped, because a queue that never drains is a leak
            // and the local cache is the durable copy either way.
            if (_historyPushQueue.length + batch.length <= AUDIT_LOG_MAX) {
                _historyPushQueue.unshift(...batch);
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

// The audit log used to be subscribed for everyone the moment they signed in:
// 500 documents, roughly 150 KB, on every app open — for a modal most people
// never open. Worse, the History page is gated on hasAccess('history','view')
// (see below) while the subscription was not, so an account whose History menu
// is hidden still pulled down 500 audit rows complete with every actor's name
// and e-mail. That is not merely wasteful.
//
// So it starts on the first open instead, and only for someone allowed to read
// it. Idempotent: reopening the modal does not resubscribe. tearDownCloudSync
// clears cloudHistoryUnsub, so a new session starts clean.
function startHistorySubscription() {
    if (cloudHistoryUnsub) return;
    if (!window.cloud?.isReady || !window.cloud.subscribeHistory) return;
    if (!hasAccess('history', 'view')) return;
    cloudHistoryUnsub = window.cloud.subscribeHistory(events => {
        cloudHistory = events || [];
        clearRulesBanner('history');
        // Re-render the history modal live if it's currently open
        const modal = document.getElementById('historyModal');
        if (modal && modal.classList.contains('open')) {
            showHistory(modal.dataset.unitId || undefined);
        }
    }, err => {
        console.warn('[cloud] history offline:', err && err.code);
        if (err && err.code === 'permission-denied') {
            showHistoryRulesBanner();
            showToast('History diblokir Firestore rules — lihat pesan di panel History', 'warning');
        }
    });
}

// Access can be revoked mid-session. Drop the stream and the cached rows with
// it, or a demoted account keeps the audit trail it was just cut off from.
function stopHistorySubscription() {
    if (!cloudHistoryUnsub) return;
    try { cloudHistoryUnsub(); } catch (_) {}
    cloudHistoryUnsub = null;
    cloudHistory = [];
}

function showHistory(unitId) {
    if (!hasAccess('history', 'view')) { showToast('Anda tidak punya akses ke History', 'warning'); return; }
    startHistorySubscription();
    const log = getAuditLog();
    const filtered = unitId ? log.filter(e => e.unitId === unitId) : log;
    const title = unitId
        ? `History: ${escapeHtml((globalData.find(u => u.id === unitId) || {}).name || 'Unit')}`
        : 'Change History';
    document.getElementById('historyTitle').innerHTML = title;

    const modal = document.getElementById('historyModal');
    // Stash filter so live snapshots can re-render with the same scope.
    modal.dataset.unitId = unitId || '';

    // This view is fed by a subscription capped at the newest 500 and then
    // capped again on merge, with nothing on screen saying so. Someone looking
    // for an old change scrolled to the bottom of an ordinary-looking list and
    // concluded it was not there. Say it plainly, and point at Export, which
    // is the only path that reads the whole collection.
    const truncated = filtered.length >= AUDIT_LOG_MAX;
    const countEl = document.getElementById('historyCount');
    if (countEl) {
        countEl.textContent = truncated
            ? `Menampilkan ${filtered.length} terbaru — riwayat lengkap ada di tombol Export`
            : `${filtered.length} kejadian`;
    }

    const tbody = document.getElementById('historyBody');
    if (filtered.length === 0) {
        // 'Belum ada riwayat' would be a lie for a unit whose events exist but
        // fall outside the capped window, so separate the two cases.
        tbody.innerHTML = `<tr><td colspan="7" style="text-align:center;padding:24px;color:var(--text-secondary)">${
            unitId
                ? 'Tidak ada riwayat unit ini di dalam 500 kejadian terbaru — coba Export untuk riwayat lengkap'
                : 'Belum ada riwayat'
        }</td></tr>`;
    } else {
        tbody.innerHTML = filtered.map(e => {
            const who = e.actorName
                ? `<span class="audit-actor" title="${escapeHtml(e.actorEmail || '')}">${escapeHtml(e.actorName)} <em>(${escapeHtml(e.actorRole || '?')})</em></span>`
                : '<span style="color:var(--text-light)">—</span>';
            return `
            <tr>
                <td data-label="Waktu" style="white-space:nowrap">${formatUserTime(e.timestamp)}</td>
                <td data-label="Aksi"><span class="audit-badge audit-${escapeHtml(e.action)}">${escapeHtml(e.action)}</span></td>
                <td data-label="Oleh">${who}</td>
                <td data-label="Unit">${escapeHtml(e.unitName || '-')}</td>
                <td data-label="Field">${escapeHtml(e.field || '-')}</td>
                <td data-label="Sebelum">${escapeHtml(e.before != null ? e.before : '-')}</td>
                <td data-label="Sesudah">${escapeHtml(e.after  != null ? e.after  : '-')}</td>
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
        showToast('Hanya owner yang bisa menghapus riwayat bersama', 'warning');
        return;
    }
    if (!confirm('Hapus SELURUH riwayat perubahan untuk seluruh tim? Tindakan ini tidak bisa dibatalkan.')) return;
    localStorage.removeItem(AUDIT_LOG_KEY);
    if (window.cloud?.clearHistoryCloud) {
        window.cloud.clearHistoryCloud().then(() => {
            cloudHistory = [];
            showHistory();
            showToast('Riwayat tim dihapus', 'success');
        }).catch(err => {
            console.error('[cloud] clear history failed:', err);
            showToast('Gagal menghapus di cloud — periksa console', 'error');
        });
    } else {
        cloudHistory = [];
        showHistory();
        showToast('Riwayat dihapus', 'success');
    }
}

// ============================================================
// PHANTOM HISTORY — entries for changes the server refused
// ------------------------------------------------------------
// A history entry is written the moment an action is taken, before the server
// answers. When the server then refuses the data write — wrong access area,
// rules not published — the entry stays behind describing a change that never
// happened. It lands because /history is gated on canEditAnything(), edit on
// ANY area, while each data collection is gated on its own area.
//
// Deliberately narrow. It judges only unit field updates, because units are
// the one module whose entries carry a real document id in unitId; every other
// module reuses that field for something else, or has no pointer at all. And
// it judges only the NEWEST entry per unit and field, because an older entry
// in a chain cannot be told apart from one that applied and was later changed
// again. Everything it cannot be sure about it leaves alone — this deletes
// audit data, so under-reaching is the safe direction.
// ============================================================

const PHANTOM_SKIP_FIELDS = new Set(['id', 'downtimeHistory', 'breakdownStartedAt']);
let _phantomHistoryFound = [];

function findPhantomUnitHistory(log) {
    const byUnit = new Map(globalData.map(u => [u.id, u]));
    const newest = new Map();

    log.forEach(e => {
        if (!e || e.action !== 'update' || !e.unitId || !e.field) return;
        if (PHANTOM_SKIP_FIELDS.has(e.field)) return;
        // Other modules prefix unitName with [Tim], [Gudang], [Lisensi]… and
        // put something that is not a unit document id into unitId.
        if (String(e.unitName || '').startsWith('[')) return;
        // Unit is gone: a deleted unit leaves nothing to compare against, and
        // the entry may well be a true record of a change made before deletion.
        if (!byUnit.has(e.unitId)) return;

        const key = `${e.unitId}::${e.field}`;
        const prev = newest.get(key);
        if (!prev || (e.timestamp || 0) > (prev.timestamp || 0)) newest.set(key, e);
    });

    const found = [];
    newest.forEach(e => {
        const unit = byUnit.get(e.unitId);
        // Field not part of the record any more — nothing to compare.
        if (!(e.field in unit)) return;
        const now = unit[e.field];
        // Two conditions, and the second is what keeps this safe. The value
        // must not be what the entry claims it became, AND it must still be
        // exactly what the entry was changing away FROM — the value never
        // moved at all. Requiring only the first would mean that deleting a
        // phantom row exposes the older row beneath it to the next scan, and
        // repeated scans would eat backwards through a legitimate chain.
        if (sameStoredValue(now, e.after)) return;
        if (!sameStoredValue(now, e.before)) return;
        found.push({ entry: e, current: now });
    });
    found.sort((a, b) => (b.entry.timestamp || 0) - (a.entry.timestamp || 0));
    return found;
}

async function scanPhantomHistory() {
    if (!isOwner || !isOwner()) {
        showToast('Hanya owner yang bisa memeriksa riwayat bersama', 'warning');
        return;
    }
    if (!window.cloud?.getAllHistory) {
        showToast('Perlu koneksi ke cloud untuk memeriksa riwayat', 'warning');
        return;
    }
    showLoading(true);
    try {
        // The whole collection, not getAuditLog() — that is capped at 500 twice
        // over, and the rows we are hunting are mostly older than that.
        const log = await window.cloud.getAllHistory();
        _phantomHistoryFound = findPhantomUnitHistory(log);

        if (_phantomHistoryFound.length === 0) {
            showToast(`Diperiksa ${log.length} catatan — tidak ada yang mencurigakan`, 'success');
            return;
        }
        renderPhantomHistory(log.length);
    } catch (err) {
        console.error('[audit] phantom scan failed:', err);
        showToast('Gagal memeriksa riwayat — periksa console', 'error');
    } finally {
        showLoading(false);
    }
}

function renderPhantomHistory(scanned) {
    const actors = new Set(_phantomHistoryFound.map(f => f.entry.actorName || '-'));
    document.getElementById('phantomHistorySummary').textContent =
        `${_phantomHistoryFound.length} dari ${scanned} catatan tidak cocok dengan data unit sekarang · ${actors.size} pelaku`;

    document.getElementById('phantomHistoryBody').innerHTML = _phantomHistoryFound.map(f => {
        const e = f.entry;
        return `<tr>
            <td data-label="Waktu" style="white-space:nowrap">${formatUserTime(e.timestamp)}</td>
            <td data-label="Oleh">${escapeHtml(e.actorName || '-')}</td>
            <td data-label="Unit">${escapeHtml(e.unitName || '-')}</td>
            <td data-label="Field">${escapeHtml(e.field)}</td>
            <td data-label="Sebelum">${escapeHtml(e.before != null && e.before !== '' ? String(e.before) : '(kosong)')}</td>
            <td data-label="Tercatat jadi">${escapeHtml(e.after != null ? String(e.after) : '-')}</td>
            <td data-label="Nilai sekarang"><strong>${escapeHtml(f.current != null && f.current !== '' ? String(f.current) : '(kosong)')}</strong></td>
        </tr>`;
    }).join('');

    document.getElementById('phantomHistoryDeleteBtn').textContent =
        `Hapus ${_phantomHistoryFound.length} baris ini`;
    document.getElementById('phantomHistoryModal').classList.add('open');
}

function closePhantomHistory() {
    document.getElementById('phantomHistoryModal').classList.remove('open');
    _phantomHistoryFound = [];
}

async function deletePhantomHistory() {
    if (!isOwner || !isOwner()) return;
    if (_phantomHistoryFound.length === 0) return;
    // The service worker can still be serving a firebase-init.js from before
    // this function existed; without the check the delete silently does
    // nothing while the toast says it worked.
    if (!window.cloud?.deleteHistoryEvents) {
        showToast('Muat ulang halaman — versi lama masih aktif', 'warning');
        return;
    }
    const n = _phantomHistoryFound.length;
    if (!confirm(`Hapus ${n} catatan riwayat ini untuk seluruh tim? Tindakan ini tidak bisa dibatalkan.`)) return;

    const ids = _phantomHistoryFound.map(f => f.entry.id).filter(Boolean);
    showLoading(true);
    try {
        await window.cloud.deleteHistoryEvents(ids);
        // getAuditLog() merges the local cache back over the cloud list, so
        // rows deleted only in Firestore reappear on the next render. Prune
        // both or the work is undone the moment the modal closes.
        pruneLocalAuditByIds(ids);
        const gone = new Set(ids);
        cloudHistory = cloudHistory.filter(e => !gone.has(e.id));
        closePhantomHistory();
        showHistory();
        showToast(`${n} catatan palsu dihapus`, 'success');
    } catch (err) {
        console.error('[audit] phantom delete failed:', err);
        showToast('Gagal menghapus — periksa console', 'error');
    } finally {
        showLoading(false);
    }
}

function pruneLocalAuditByIds(ids) {
    const gone = new Set(ids);
    try {
        const log = JSON.parse(localStorage.getItem(AUDIT_LOG_KEY) || '[]');
        localStorage.setItem(AUDIT_LOG_KEY, JSON.stringify(log.filter(e => e && !gone.has(e.id))));
    } catch (e) { /* ignore */ }
}

// The History view is fed by a capped live subscription (newest 500), so
// exporting what's on screen silently truncated the audit trail. Pull the full
// collection from Firestore instead, falling back to the cached window when
// offline — and say which one the file actually contains.
async function exportHistory() {
    if (!canCsv('export')) return;

    let log = getAuditLog();
    let complete = false;
    if (window.cloud?.isReady && window.cloud.getAllHistory) {
        try {
            showLoading(true);
            const all = await window.cloud.getAllHistory();
            if (Array.isArray(all) && all.length >= log.length) {
                log = all.slice().sort((a, b) => (b.timestamp || 0) - (a.timestamp || 0));
                complete = true;
            }
        } catch (err) {
            console.warn('[cloud] full history fetch failed, exporting cached window:', err);
        } finally {
            showLoading(false);
        }
    }

    if (log.length === 0) { showToast('Tidak ada riwayat untuk diekspor', 'warning'); return; }
    // The actor columns matter more here than anywhere else in the app: this
    // export is the only path that sees the whole collection rather than the
    // newest 500, so it is the tool you reach for to answer "who changed this"
    // — and it used to be the one place that dropped the answer.
    const headers = ['Waktu', 'Aksi', 'Objek', 'Field', 'Sebelum', 'Sesudah',
                     'Oleh', 'Email', 'Peran', 'ID'];
    const rows = log.map(e => [
        new Date(e.timestamp).toISOString(),
        e.action, e.unitName || '', e.field || '',
        e.before != null ? e.before : '', e.after != null ? e.after : '',
        e.actorName || '', e.actorEmail || '', e.actorRole || '', e.id || ''
    ]);
    const csv = [headers, ...rows].map(row => row.map(csvCell).join(',')).join('\n');
    const blob = new Blob(['\uFEFF' + csv], { type: 'text/csv;charset=utf-8;' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = `tractor_history_${toISODate()}.csv`;
    a.click();
    URL.revokeObjectURL(url);
    showToast(complete
        ? `${log.length} kejadian riwayat diekspor (lengkap)`
        : `${log.length} kejadian riwayat diekspor — hanya yang tersimpan di perangkat ini, bukan seluruh riwayat`,
        complete ? 'success' : 'warning');
}

// ============================================================
// BACKUP & RESTORE
// ============================================================

// ============================================================
// BACKUP — what a backup actually contains
// ============================================================
// For three versions this file exported four collections out of fifteen while
// the button read "Download full JSON backup". Everything the Team and
// Warehouse modules hold — people, shift schedules, daily reports, devices,
// stock movements — had no backup at all. One table now drives both the export
// and the restore, so a collection cannot be added to the app and forgotten
// here: adding it is one row.
//
// `read` is the in-memory array. `fullRead` exists for collections whose
// in-memory copy is deliberately incomplete — shifts hold only the subscribed
// 120-day window, so backing up that array would produce a file that looks
// whole and is not, and a REPLACE restore computed against it would delete
// every shift outside the window.
const BACKUP_PARTS = [
    { key: 'implements', label: 'Implement', area: 'implements',
      read: () => globalImplements, write: l => { globalImplements = l; },
      saveLocal: () => saveImplements(), bulk: 'saveImplements',
      deleteOne: id => cloudDeleteImplement(id), resync: () => resyncImplements() },

    { key: 'damages', label: 'Kerusakan', area: 'damage',
      read: () => globalDamages, write: l => { globalDamages = l; },
      saveLocal: () => saveDamages(), bulk: 'saveDamages',
      deleteOne: id => cloudDeleteDamage(id), resync: () => resyncDamages() },

    { key: 'licenseStock', label: 'Stok Lisensi', area: 'licenseStock',
      read: () => globalLicenseStock, write: l => { globalLicenseStock = l; },
      saveLocal: () => saveLicenseStockLocal(), bulk: 'saveLicenses',
      deleteOne: id => cloudDeleteLicense(id), resync: () => resyncLicenses() },

    { key: 'userCategories', label: 'Kategori User', area: 'editUnits',
      read: () => userCategories, write: l => { userCategories = l; },
      bulk: 'saveUserCategories', deleteOne: id => window.cloud.deleteUserCategory(id) },

    { key: 'damageComponents', label: 'Komponen Kerusakan', area: 'damage',
      read: () => damageComponents, write: l => { damageComponents = l; },
      bulk: 'saveDamageComponents', deleteOne: id => window.cloud.deleteDamageComponent(id) },

    { key: 'teamMembers', label: 'Anggota Tim', area: 'teamMembers',
      read: () => teamMembers, write: l => { teamMembers = l; },
      bulk: 'saveTeamMembers', deleteOne: id => window.cloud.deleteTeamMember(id) },

    { key: 'shifts', label: 'Jadwal Shift', area: 'teamShift',
      read: () => teamShifts, write: l => { teamShifts = l; },
      // The subscription is windowed; the backup must not be.
      fullRead: () => window.cloud.getAllShifts(),
      bulk: 'saveShifts', deleteOne: id => window.cloud.deleteShift(id) },

    { key: 'workLogs', label: 'Laporan Harian', area: 'teamLog',
      read: () => workLogs, write: l => { workLogs = l; },
      bulk: 'saveWorkLogs', deleteOne: id => window.cloud.deleteWorkLog(id) },

    { key: 'leaveRequests', label: 'Izin / Sakit', area: 'teamLog',
      read: () => leaveRequests, write: l => { leaveRequests = l; },
      bulk: 'saveLeaveRequests', deleteOne: id => window.cloud.deleteLeaveRequest(id) },

    { key: 'devices', label: 'Perangkat Gudang', area: 'warehouse',
      read: () => warehouseDevices, write: l => { warehouseDevices = l; },
      bulk: 'saveDevices', deleteOne: id => window.cloud.deleteDevice(id) },

    { key: 'stockItems', label: 'Stok Barang', area: 'warehouse',
      read: () => stockLedger, write: l => { stockLedger = l; },
      bulk: 'saveStockItems', deleteOne: id => window.cloud.deleteStockItem(id) }
];

// Two collections are left out on purpose, and the UI says so rather than
// leaving it a mystery:
//   users   — role and per-area access. Restoring it would rewrite people's
//             permissions from a file. Accounts belong on the Users page.
//   history — the audit log has its own Export button, reads the whole
//             collection, and has no size limit worth putting in every backup.
const BACKUP_EXCLUDED = [
    { label: 'Akun & Akses', why: 'diatur di halaman Users, bukan lewat berkas' },
    { label: 'Riwayat Audit', why: 'punya tombol Export sendiri di halaman History' }
];

async function exportBackup() {
    // A backup is a full-dataset export, so it needs the export privilege AND
    // read access to every area it contains — otherwise it is a way around
    // both the CSV privilege and per-area 'none'.
    if (!canCsv('export')) return;
    const blocked = ['editUnits', 'implements', 'damage', 'licenseStock']
        .filter(a => !hasAccess(a, 'view'));
    if (blocked.length) {
        showToast('Backup butuh akses ke semua data — Anda tidak punya akses penuh', 'warning');
        return;
    }

    const includeFiles = globalData.some(u => u.attachments && u.attachments.length > 0)
        && confirm('Sertakan file lampiran dalam backup? (ukuran file bisa besar)');

    const payload = {
        version: 4,
        exportedAt: new Date().toISOString(),
        count: globalData.length,
        units: globalData,
        // Everything else comes from BACKUP_PARTS, so a new collection is one
        // row there instead of an edit in two places that drift apart.
        omitted: []
    };
    const included = [];

    showLoading(true);
    try {
        for (const part of BACKUP_PARTS) {
            // A backup must not become a way around per-area 'none'. Skip what
            // this account may not read — and record it, so the file says what
            // it is missing instead of looking complete.
            if (!hasAccess(part.area, 'view')) {
                payload.omitted.push({ key: part.key, label: part.label, why: 'tanpa akses' });
                continue;
            }
            let rows = part.read() || [];
            if (part.fullRead && window.cloud?.isReady) {
                try {
                    const all = await part.fullRead();
                    if (Array.isArray(all)) rows = all;
                } catch (err) {
                    // Better a short backup that admits it than a short backup
                    // that does not.
                    console.warn(`[backup] ${part.key} full read failed:`, err);
                    payload.omitted.push({ key: part.key, label: part.label, why: 'gagal dibaca lengkap' });
                }
            }
            payload[part.key] = rows;
            included.push(`${rows.length} ${part.label.toLowerCase()}`);
        }
    } finally {
        showLoading(false);
    }

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
    a.download = `tractor_backup_${toISODate()}.json`;
    a.click();
    URL.revokeObjectURL(url);
    // Say what it holds AND what it does not. The old toast named the four
    // collections it saved, which read as a complete list when eleven others
    // were missing.
    const missing = payload.omitted.map(o => o.label);
    showToast(`Backup tersimpan: ${globalData.length} unit, ${included.join(', ')}`
        + (missing.length ? ` · TIDAK termasuk: ${missing.join(', ')}` : ''),
        missing.length ? 'warning' : 'success');
}

// ---- localStorage usage guard ----
// Damage photos (base64) are the main storage driver; warn before the ~5MB
// quota is hit so the user can export a backup / prune old photos in time.
let _storageWarnedAt = 0;
let _autoBackupFailed = false;
const STORAGE_WARN_BYTES = 4.5 * 1024 * 1024;
const STORAGE_WARN_REPEAT_MS = 10 * 60 * 1000;

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

// Warns again every STORAGE_WARN_REPEAT_MS while the problem persists. It used
// to latch on a module-level flag, so a user who missed or dismissed the single
// toast got no further signal as writes began failing — and the toast is the
// only indication that anything is wrong until data stops saving.
function checkStorageUsage() {
    const used = estimateLocalStorageBytes();
    if (used <= STORAGE_WARN_BYTES) { _storageWarnedAt = 0; return; }
    const now = Date.now();
    if (_storageWarnedAt && now - _storageWarnedAt < STORAGE_WARN_REPEAT_MS) return;
    _storageWarnedAt = now;
    showToast(`Penyimpanan browser hampir penuh (${(used / 1048576).toFixed(1)} MB terpakai) — export Backup sekarang`, 'warning');
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

// The bulk cloud savers only upsert, so a REPLACE restore would leave the
// dropped documents alive in Firestore and the next live snapshot would put
// them straight back — silently undoing the restore. Delete what the restore
// removed, one id at a time (these collections have no bulk delete).
function _cloudDeleteRemoved(previous, kept, deleteOne) {
    if (suppressCloudWrites || !window.cloud?.isReady) return;
    const keptIds = new Set(kept.map(r => r.id));
    previous.forEach(r => { if (r.id && !keptIds.has(r.id)) deleteOne(r.id); });
}

// A restore replaces FOUR collections at once, so it needs edit rights on all
// of them — gating on editUnits alone let a units-only editor wipe implements,
// damage records and licence stock.
function importBackup(file) {
    if (!canCsv('full')) return;
    // Units are non-negotiable: every backup carries them and the units path
    // below writes unconditionally. The rest are checked per collection as
    // they are restored, so someone with partial rights restores what they may
    // and is told plainly what was left alone.
    if (!hasAccess('editUnits', 'edit')) {
        showToast('Restore butuh hak edit pada data Unit', 'warning');
        return;
    }
    const reader = new FileReader();
    reader.onload = async e => {
        try {
            const data = JSON.parse(e.target.result);
            if (!data || !Array.isArray(data.units)) {
                showToast('Berkas backup tidak valid', 'error');
                return;
            }
            const merge = confirm(
                `Backup berisi ${data.units.length} unit.\n\n` +
                `OK    = GABUNG (tambahkan yang baru, pertahankan yang ada)\n` +
                `Batal = GANTI (hapus data sekarang, muat isi backup)\n\n` +
                `Pilihan ini berlaku untuk SEMUA koleksi di dalam berkas, bukan unit saja.`
            );
            if (merge) {
                const result = addUnits(data.units);
                showToast(`Backup digabung: ${result.added} ditambahkan, ${result.skipped} duplikat dilewati`, 'success');
            } else {
                if (_refuseCrossGroupRestore(data.units)) return;
                const nHeavy = data.units.filter(isHeavy).length;
                if (!confirm(`Ini akan MENGHAPUS seluruh ${globalData.length} unit saat ini dan menggantinya dengan isi backup`
                    + (nHeavy ? ` (termasuk ${nHeavy} unit ${UNIT_GROUPS.heavy.shortLabel})` : '') + `. Lanjutkan?`)) return;
                // Mirror the replace to the cloud, otherwise the next units
                // snapshot overwrites localStorage and silently undoes the
                // whole restore (and deleted units come back).
                const previousIds = globalData.map(u => u.id);
                globalData = data.units.map(u => ({ ...u, id: u.id || generateId() }));
                const keptIds = new Set(globalData.map(u => u.id));
                saveToStorage(globalData);
                cloudDeleteUnits(previousIds.filter(id => !keptIds.has(id)));
                cloudPushUnits(globalData);
                logEvent({ action: 'restore', unitName: '-', after: `${data.units.length} unit dipulihkan dari backup` });
                recordChange({ type: 'restored', detail: `${data.units.length} units restored from backup` });
                showToast(`${data.units.length} unit dipulihkan dari backup`, 'success');
            }

            // Every other collection goes through BACKUP_PARTS, in the same
            // mode (merge / replace) the user chose for units. The report
            // afterwards names three different outcomes, and the third is the
            // one that used to be silent: a collection that is simply NOT IN
            // THE FILE. A v3 backup has no Team or Warehouse data at all, and
            // the person restoring it has to know that rather than assume the
            // restore covered everything.
            const report = [];
            for (const part of BACKUP_PARTS) {
                const rows = data[part.key];
                if (!Array.isArray(rows)) {
                    report.push({ label: part.label, count: '—', note: 'tidak ada di berkas ini' });
                    continue;
                }
                if (!hasAccess(part.area, 'edit')) {
                    report.push({ label: part.label, count: rows.length, note: 'dilewati — Anda tidak punya hak edit' });
                    continue;
                }
                const bulk = cloudFn(part.bulk);
                if (window.cloud?.isReady && !bulk) {
                    report.push({ label: part.label, count: rows.length, note: 'dilewati — versi lama masih aktif, muat ulang halaman' });
                    continue;
                }

                // REPLACE has to be computed against what the SERVER holds, not
                // against the in-memory array: shifts only keep the subscribed
                // window, so diffing the array would delete every shift outside
                // it. fullRead gives the real "before".
                let before = part.read() || [];
                if (!merge && part.fullRead && window.cloud?.isReady) {
                    try {
                        const all = await part.fullRead();
                        if (Array.isArray(all)) before = all;
                    } catch (err) {
                        // Without a trustworthy "before" a REPLACE could delete
                        // rows it cannot see. Upsert only, and say so.
                        console.warn(`[restore] ${part.key} full read failed:`, err);
                        report.push({ label: part.label, count: rows.length,
                                      note: 'hanya ditambahkan — daftar lama gagal dibaca, tidak ada yang dihapus' });
                        part.write(_restoreCollection(rows, part.read() || [], true));
                        if (part.saveLocal) part.saveLocal();
                        if (bulk) bulk(part.read()).catch(e => cloudWriteFailed(e, {
                            what: `${part.label.toLowerCase()} (restore)`,
                            label: `[${part.label}] restore`, resync: part.resync }));
                        continue;
                    }
                }

                const next = _restoreCollection(rows, before, merge);
                part.write(next);
                if (part.saveLocal) part.saveLocal();
                _cloudDeleteRemoved(before, next, part.deleteOne);
                if (bulk) {
                    bulk(next).catch(err => cloudWriteFailed(err, {
                        what: `${part.label.toLowerCase()} (restore)`,
                        label: `[${part.label}] restore`,
                        resync: part.resync
                    }));
                }
                report.push({ label: part.label, count: rows.length, note: merge ? 'digabung' : 'diganti' });
            }
            showRestoreReport(report, data.version || 0, merge);

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

            // Refresh every surface, not just the units table — a restore can
            // be triggered from any view and replaces four collections.
            // Every surface, not just units: a restore can be triggered from
            // any view and now replaces up to eleven collections.
            renderEditTable();
            renderImplementsTable();
            populateLicenseTypeList();
            renderLicenseSummary();
            renderLicenseStockTable();
            renderDamageTable();
            renderUserCategoryOptions();
            renderDamageComponentsList();
            if (typeof renderTeamView === 'function') renderTeamView();
            if (typeof renderDeviceTable === 'function') renderDeviceTable();
            if (typeof renderStockView === 'function') renderStockView();
            scheduleDecisionRefresh();
            filteredData = scopeDashUnits();
            updateDashboard(filteredData);
        } catch (err) {
            showToast('Gagal membaca backup: ' + err.message, 'error');
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

// Takes the units to measure; the default (every unit) keeps the old call
// sites — the email report and the decision inbox — fleet-wide.
function computeDowntimeStats(units) {
    const list = units || globalData;
    const now = Date.now();
    const monthStart = new Date();
    monthStart.setDate(1); monthStart.setHours(0, 0, 0, 0);
    const monthStartMs = monthStart.getTime();

    let totalDowntimeMs = 0;
    let totalFailures = 0;
    let totalMonthDowntime = 0;
    const perUnit = [];

    // How far back the downtime record actually goes. MTBF used to divide
    // all-time failures by a FIXED 30 days per unit, so the denominator stood
    // still while the numerator kept growing: an unchanged fleet's MTBF fell
    // from ~30 days to ~2 days over a year, and hit 0 once total downtime
    // passed 30 days per unit. Measure the real observation window instead.
    let earliestEventMs = Infinity;
    list.forEach(u => {
        (u.downtimeHistory || []).forEach(iv => {
            if (typeof iv.start === 'number' && iv.start < earliestEventMs) earliestEventMs = iv.start;
        });
        if (typeof u.breakdownStartedAt === 'number' && u.breakdownStartedAt < earliestEventMs) {
            earliestEventMs = u.breakdownStartedAt;
        }
    });

    list.forEach(u => {
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

    // Observed window per unit, floored at a day so a fleet whose first
    // breakdown happened minutes ago can't produce an absurd MTBF.
    const DAY_MS = 24 * 3600 * 1000;
    const windowMs = earliestEventMs === Infinity
        ? 30 * DAY_MS                       // nothing recorded yet
        : Math.max(DAY_MS, now - earliestEventMs);
    // When both groups are in the list, each group is credited with its OWN
    // observation window; one old tractor breakdown must not stretch the
    // window of excavators registered last week. A single group uses the
    // original expression verbatim.
    const fleetOperatingMs = (hasHeavyUnits(list) && hasTractorUnits(list))
        ? _groupOperatingMs(list, now)
        : Math.max(1, list.length) * windowMs;
    const uptimeMs = Math.max(0, fleetOperatingMs - totalDowntimeMs);
    const mtbf = totalFailures > 0 ? uptimeMs / totalFailures : 0;

    perUnit.sort((a, b) => b.downtime - a.downtime);
    return {
        mtbf, mttr, totalMonthDowntime, totalFailures,
        observedMs: earliestEventMs === Infinity ? 0 : now - earliestEventMs,
        topOffenders: perUnit.slice(0, 5), topTen: perUnit.slice(0, 10)
    };
}

function _groupOperatingMs(list, now) {
    const DAY_MS = 24 * 3600 * 1000;
    return UNIT_GROUP_KEYS.reduce((sum, g) => {
        const members = list.filter(u => unitGroupOf(u) === g);
        if (!members.length) return sum;
        let earliest = Infinity;
        members.forEach(u => {
            (u.downtimeHistory || []).forEach(iv => { if (typeof iv.start === 'number' && iv.start < earliest) earliest = iv.start; });
            if (typeof u.breakdownStartedAt === 'number' && u.breakdownStartedAt < earliest) earliest = u.breakdownStartedAt;
        });
        const win = earliest === Infinity ? 30 * DAY_MS : Math.max(DAY_MS, now - earliest);
        return sum + members.length * win;
    }, 0);
}

function renderDowntimeKPIs(units) {
    const s = computeDowntimeStats(units || scopeDashUnits());
    document.getElementById('kpiMTBF').textContent = formatDuration(s.mtbf);
    document.getElementById('kpiMTTR').textContent = formatDuration(s.mttr);
    // MTBF only means something against the period it was measured over.
    const mtbfSub = document.getElementById('kpiMTBFSub');
    if (mtbfSub) {
        mtbfSub.textContent = s.observedMs > 0
            ? `Rata-rata jarak antar kerusakan · diamati ${formatDuration(s.observedMs)}`
            : 'Rata-rata jarak antar kerusakan';
    }
    document.getElementById('kpiMonthDowntime').textContent = formatDuration(s.totalMonthDowntime);
    document.getElementById('kpiFailures').textContent = s.totalFailures;

    const listEl = document.getElementById('topOffendersList');
    if (!listEl) return;
    if (s.topOffenders.length === 0) {
        listEl.innerHTML = '<div class="top-offender top-offender--empty">Belum ada downtime tercatat</div>';
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
        charts.downtimeChart = makeChart('downtimeChart', {
            type: 'bar',
            data: {
                labels: s.topTen.map(u => u.name || 'Unnamed'),
                datasets: [{
                    data: s.topTen.map(u => +(u.downtime / 3600000).toFixed(2)),
                    backgroundColor: themeColor('--primary', '#D97757'), borderRadius: 4, barPercentage: 0.6
                }]
            },
            options: {
                responsive: true, maintainAspectRatio: false, indexAxis: 'y',
                scales: {
                    x: { beginAtZero: true, title: { display: true, text: 'Downtime (hours)', font: { size: 11, family: 'Inter' } }, ticks: { font: { size: 11 } }, grid: { color: themeColor('--border-light', '#DEDACE') } },
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

// The group column is ONLY "Unit Group". "Kelompok", "Group" and the like are
// ordinary column names in a plantation tractor sheet (a crew, a block), and
// must never be read as a unit group.
const CSV_HEAVY_COLUMNS = {
    machineType: ['Jenis Alat', 'Machine Type'],
    assetCode: ['Nomor Lambung', 'Kode Aset', 'Asset Code'],
    workTool: ['Alat Kerja', 'Work Tool'],
    cameraAi: ['Camera AI'], telematicBox: ['Telematic Box'],
    switchLimiter: ['Switch Limiter'], rotaryLamp: ['Rotary Lamp']
};
// Infer the file's group from its headers, using only unambiguous markers: the
// four heavy component headers on one side, the John Deere headers on the
// other. 'Nomor Lambung', 'Jenis Alat' and 'Kelompok' are deliberately NOT
// markers — tractor sheets use them too.
function inferCsvGroup(fields) {
    const f = (fields || []).map(x => String(x || '').trim().toLowerCase());
    const has = list => list.some(h => f.includes(h.toLowerCase()));
    const heavy = has(['Camera AI', 'Telematic Box', 'Switch Limiter', 'Rotary Lamp']);
    const tractor = has(['Implement', 'Display', 'GPS', 'Steering', 'JDLink', 'Status Unit Display',
        'Status Unit GPS', 'Status Unit Steering', 'Status Unit JDLink', 'GPS License', 'License Display', 'Display License']);
    if (heavy && !tractor) return 'heavy';
    if (tractor && !heavy) return 'tractor';
    return null;
}

function processData(rows, ctx) {
    const valid = [];
    const rejected = [];
    const groupWarnings = [];
    const fallback = ctx && ctx.fallbackGroup === 'heavy' ? 'heavy' : 'tractor';
    const fileGroup = (ctx && ctx.fileGroup) || null;
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
            breakdownReason: clean(getValAny(r, ['Breakdown Reason', 'Alasan Breakdown'])),
            downtimeHistory: [],
            breakdownStartedAt: null
        };
        // Heavy columns are attached only when they carry a value, so a
        // tractor row is exactly today's object.
        Object.keys(CSV_HEAVY_COLUMNS).forEach(k => {
            const v = clean(getValAny(r, CSV_HEAVY_COLUMNS[k]));
            if (v) unit[k] = v;
        });
        const raw = clean(getVal(r, 'Unit Group'));
        const explicit = normalizeGroupKey(raw);
        if (explicit === null) groupWarnings.push({ row: idx + 2, name: unit.name, sn: unit.sn, value: raw });
        unit.unitGroup = explicit || fileGroup || fallback;
        if (explicit) csvExplicitGroup.add(unit);
        if (!unit.sn && !unit.name) {
            rejected.push({ row: idx + 2, reason: 'Missing both nickname and serial number' });
        } else if (!unit.sn) {
            rejected.push({ row: idx + 2, reason: 'Missing serial number', name: unit.name });
        } else {
            valid.push(unit);
        }
    });
    return { valid, rejected, groupWarnings };
}

// 'Unit' when the status is not Good, then the components OF THE UNIT'S GROUP.
// For a tractor the result is identical, in content and order, to the old
// hard-coded Display/GPS/Steering/JDLink list; for a heavy unit it reads
// Camera AI / Telematic Box / Switch Limiter / Rotary Lamp instead of four
// phantom John Deere faults.
function detectIssues(d) {
    const issues = [];
    if (!isGood(d.status)) issues.push('Unit');
    groupDef(unitGroupOf(d)).components.forEach(c => { if (!isGood(d[c.key])) issues.push(c.label); });
    return issues;
}

// With the default groups (['tractor']) this returns today's object in today's
// key order.
function countIssues(data, groups) {
    const counts = { Unit: 0 };
    (groups || ['tractor']).forEach(g => groupDef(g).components.forEach(c => { counts[c.label] = 0; }));
    let totalWithIssues = 0;
    data.forEach(d => {
        const issues = detectIssues(d);
        if (issues.length > 0) totalWithIssues++;
        issues.forEach(i => { if (i in counts) counts[i]++; });
    });
    return { total: totalWithIssues, counts };
}

// ============================================================
// DASHBOARD RENDERING
// ============================================================

// A date that records when something happened cannot be in the future, and a
// mistyped year (2026 → 2062) is the common way it ends up there. Such a row
// then sorts to the top of every table and falls outside every date-range
// filter. Licence dates are deliberately NOT capped — those are meant to be
// in the future. Refreshed rather than hardcoded so a session running past
// midnight still gets the right ceiling.
function capEventDatesToToday() {
    const today = toISODate();
    document.querySelectorAll('input[data-nofuture]').forEach(el => { el.max = today; });
}

function onDataLoaded() {
    document.getElementById('emptyState').style.display = 'none';
    document.getElementById('dashboardContent').style.display = 'block';
    capEventDatesToToday();

    populateFilters();
    populateEditFilters();
    filteredData = scopeDashUnits();
    updateDashboard(filteredData);

    const now = new Date().toLocaleString();
    document.getElementById('lastUpdated').textContent = `Updated: ${now}`;

    document.getElementById('connectionDot').classList.add('connected');
    updateConnectionLabel();

    updateEditCount();
    scheduleDecisionRefresh();

    // Persist any premium licence that has expired into its fallback tier.
    scheduleExpiredLicenseDowngrades();

    _tryAutoDailyEmail();
}

// ---- Dashboard group scope: Pertanian / Alat Berat / Semua ----
function _issueOptions(scope, withUnit) {
    const opts = [];
    const add = (g, grp) => groupDef(g).components.forEach(c => opts.push({ v: c.label, t: c.label, grp }));
    if (scope === 'all') { add('tractor', UNIT_GROUPS.tractor.shortLabel); add('heavy', UNIT_GROUPS.heavy.shortLabel); }
    else add(scope, '');
    return withUnit ? [{ v: 'Unit', t: 'Unit', grp: '' }].concat(opts) : opts;
}
// Rebuild a filter <select> only when its option list actually differs, so a
// database with no heavy equipment never has its static markup rewritten.
function _syncSelect(sel, allLabel, opts, suffix) {
    if (!sel) return;
    const want = [['', allLabel]].concat(opts.map(o => [o.v, o.t + suffix, o.grp]));
    const have = [...sel.options].map(o => [o.value, o.textContent, o.parentElement.tagName === 'OPTGROUP' ? o.parentElement.label : '']);
    const same = want.length === have.length && want.every((w, i) => w[0] === have[i][0] && w[1] === have[i][1] && (w[2] || '') === have[i][2]);
    if (same) return;
    const cur = sel.value;
    let html = `<option value="">${escapeHtml(allLabel)}</option>`;
    let open = '';
    opts.forEach(o => {
        if ((o.grp || '') !== open) {
            if (open) html += '</optgroup>';
            if (o.grp) html += `<optgroup label="${escapeHtml(o.grp)}">`;
            open = o.grp || '';
        }
        html += `<option value="${escapeHtml(o.v)}">${escapeHtml(o.t + suffix)}</option>`;
    });
    if (open) html += '</optgroup>';
    sel.innerHTML = html;
    sel.value = opts.some(o => o.v === cur) ? cur : '';
}

function syncDashGroupUI() {
    const both = hasHeavyUnits() && hasTractorUnits();
    const scope = effectiveDashGroup();
    const bar = document.getElementById('dashGroupBar');
    if (bar) bar.style.display = both ? '' : 'none';
    document.querySelectorAll('.dash-group-tab').forEach(btn => {
        const g = btn.dataset.group;
        const n = g === 'all' ? globalData.length : unitsOfGroup(globalData, g).length;
        const label = g === 'all' ? 'Semua' : groupDef(g).shortLabel;
        btn.style.display = '';
        btn.classList.toggle('active', g === scope);
        btn.setAttribute('aria-selected', g === scope ? 'true' : 'false');
        btn.innerHTML = `${escapeHtml(label)} <span class="team-tab__count">${n}</span>`;
    });
    const cap = document.getElementById('dashScopeCaption');
    if (cap) {
        cap.hidden = !both;
        if (both) {
            const nH = unitsOfGroup(globalData, 'heavy').length;
            cap.textContent = scope === 'all'
                ? `Cakupan: semua unit (${globalData.length - nH} ${UNIT_GROUPS.tractor.shortLabel}, ${nH} ${UNIT_GROUPS.heavy.shortLabel})`
                : `Cakupan: ${groupDef(scope).label}`;
        }
    }
    _syncSelect(document.getElementById('componentFilter'), 'All Components', _issueOptions(scope, false), ' Issues');
    _syncSelect(document.getElementById('issueFilter'), 'All Issues', _issueOptions(scope, true), '');
}

function setDashGroup(g, opts) {
    if (!DASH_GROUP_KEYS.includes(g)) return;
    dashGroupPref = g;
    if (!opts || opts.persist !== false) writePref('dashUnitGroup', g);
    sortState.key = null;
    ['componentFilter', 'issueFilter'].forEach(id => { const el = document.getElementById(id); if (el) el.value = ''; });
    populateFilters();
    filteredData = applyFilterLogic();
    updateDashboard(filteredData);
}

function updateDashboard(data) {
    syncDashGroupUI();
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
    const alerts = _buildAlertList(scopeDashUnits()).total;

    const clauses = [
        breakdown === 0 ? 'semua unit beroperasi hari ini' : `${breakdown} unit sedang breakdown`
    ];
    if (issues > 0) clauses.push(`${issues} berjalan dengan gangguan komponen`);
    if (alerts > 0) clauses.push(`${alerts} lisensi akan expire dalam 30 hari ke depan`);

    let tail;
    if (clauses.length === 1) tail = clauses[0];
    else tail = clauses.slice(0, -1).join(', ') + ', dan ' + clauses[clauses.length - 1];

    // The noun names the group only when the dashboard is not the plain
    // agricultural view, so that sentence reads exactly as it always has.
    const scope = effectiveDashGroup();
    let noun = 'unit';
    if (scope === 'heavy') noun = `unit ${UNIT_GROUPS.heavy.shortLabel}`;
    else if (scope === 'all') {
        const nH = data.filter(isHeavy).length;
        noun = `unit (${total - nH} ${UNIT_GROUPS.tractor.shortLabel}, ${nH} ${UNIT_GROUPS.heavy.shortLabel})`;
    }
    el.textContent = `${total} ${noun} di ${sites} site — ${tail}.`;
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
    charts.statusChart = makeChart('statusChart', {
        type: 'doughnut',
        data: {
            labels: ['Good', 'Breakdown'],
            datasets: [{ data: [good, breakdown], backgroundColor: [themeColor('--success', '#4F7B58'), themeColor('--danger', '#BF4D43')], borderWidth: 0, hoverOffset: 6 }]
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
    charts.siteChart = makeChart('siteChart', {
        type: 'bar',
        data: {
            labels,
            datasets: [
                { label: 'Good', data: labels.map(s => siteMap[s].good), backgroundColor: themeColor('--success', '#4F7B58'), borderRadius: 4, barPercentage: 0.6 },
                { label: 'Breakdown', data: labels.map(s => siteMap[s].breakdown), backgroundColor: themeColor('--danger', '#BF4D43'), borderRadius: 4, barPercentage: 0.6 }
            ]
        },
        options: {
            responsive: true, maintainAspectRatio: false, indexAxis: 'y',
            scales: {
                x: { stacked: true, beginAtZero: true, ticks: { stepSize: 1, font: { size: 11, family: 'Inter' } }, grid: { color: themeColor('--border-light', '#DEDACE') } },
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

    data.filter(u => !isHeavy(u)).forEach(unit => {
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
    // Licence stock is about John Deere licences; nothing to say about it on
    // a heavy-equipment dashboard.
    if (effectiveDashGroup() === 'heavy') { section.style.display = 'none'; return; }
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
        showToast('Lengkapi semua kolom EmailJS', 'warning'); return;
    }
    localStorage.setItem('emailjs_settings', JSON.stringify(s));
    closeEmailSettingsModal();
    showToast('Pengaturan email disimpan', 'success');
}

// SF/G5 licences are John Deere only, so heavy units never produce an alert.
// Called with no argument (the email report, the decision inbox) it stays
// fleet-wide.
function _buildAlertList(units) {
    const SOON_DAYS = 30;
    const lines = [];
    let expiredCount = 0, soonCount = 0;

    (units || globalData).filter(u => !isHeavy(u)).forEach(unit => {
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
        showToast('Atur EmailJS dulu — klik ikon gerigi', 'warning');
        openEmailSettingsModal();
        return;
    }

    const report = _buildAlertList();
    if (report.total === 0) {
        showToast('Tidak ada peringatan lisensi untuk dikirim', 'info'); return;
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
        showToast('Gagal mengirim email — periksa pengaturan EmailJS', 'error');
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
// Each group's rings are measured against that group's own units. The old
// denominator was every unit, so a single excavator — which has no Display or
// GPS at all — pulled a fully healthy tractor fleet below 100%.
function renderComponentHealth(data) {
    const grid = document.getElementById('componentGrid');
    const scope = effectiveDashGroup();
    let groups = scope === 'all'
        ? UNIT_GROUP_KEYS.filter(g => data.some(u => unitGroupOf(u) === g))
        : [scope];
    if (!groups.length) groups = scope === 'all' ? UNIT_GROUP_KEYS.slice() : [scope];
    const captions = groups.length > 1;

    grid.innerHTML = groups.map(g => {
        const members = data.filter(u => unitGroupOf(u) === g);
        const total = members.length;
        const rings = groupDef(g).components.map(c => {
            const goodCount = members.filter(d => isGood(d[c.key])).length;
            const rate = pct(goodCount, total);
            const color = COMPONENT_COLORS[c.key];
            const circumference = 2 * Math.PI * 28;
            const offset = circumference - (rate / 100) * circumference;
            return `
        <div class="component-stat">
            <div class="component-stat__name">${c.label}</div>
            <div class="component-stat__ring">
                <svg width="72" height="72" viewBox="0 0 72 72">
                    <circle cx="36" cy="36" r="28" fill="none" style="stroke:var(--border-light)" stroke-width="6"/>
                    <circle cx="36" cy="36" r="28" fill="none" stroke="${color}" stroke-width="6"
                        stroke-dasharray="${circumference}" stroke-dashoffset="${offset}" stroke-linecap="round"/>
                </svg>
                <div class="component-stat__ring-text" style="color:${color}">${rate}%</div>
            </div>
            <div class="component-stat__detail">${goodCount} / ${total} Good</div>
        </div>`;
        }).join('');
        return (captions ? `<div class="component-grid__group">${escapeHtml(groupDef(g).label)}</div>` : '') + rings;
    }).join('');
}

// ---- Detail Table ----
// True when anything is narrowing the dashboard list. Used to tell "there is
// no data" apart from "your filter matched nothing" — the second needs a way
// back out, the first does not.
function dashboardFilterActive() {
    const val = id => (document.getElementById(id) || {}).value || '';
    return !!(val('searchInput') || val('statusFilter') || val('siteFilter') || val('componentFilter'));
}

// ---- Dashboard table per scope ----
// Agricultural: today's 17 columns, header captured from the static markup.
// Heavy: its own 15. "Semua": 10 columns with a Kelompok column and one
// "Komponen" cell summarising whatever each unit's group monitors.
const DETAIL_COLSPAN = { tractor: 17, heavy: 15, all: 10 };
function renderDetailHead(scope) {
    const head = document.querySelector('#detailTable thead');
    if (!head || _detailHeadGroup === scope) return;
    const s = (k, l) => sortTh(k, l, 'sortTable');
    if (scope === 'tractor') head.innerHTML = DETAIL_HEADS.tractor;
    else if (scope === 'heavy') head.innerHTML = `<tr>${s('no', 'No')}${s('name', 'Nickname')}${s('model', 'Model')}${s('sn', 'Serial Number')}
        ${s('machineType', 'Jenis Alat')}${s('assetCode', 'Nomor Lambung')}${s('workTool', 'Alat Kerja')}${s('status', 'Status')}
        ${UNIT_GROUPS.heavy.components.map(c => s(c.key, c.label)).join('')}
        ${s('site', 'Site')}${s('yearReceived', 'Tahun Penerimaan')}${s('userCategory', 'User Category')}</tr>`;
    else head.innerHTML = `<tr>${s('no', 'No')}${s('name', 'Nickname')}${s('unitGroup', 'Kelompok')}${s('model', 'Model')}
        ${s('sn', 'Serial Number')}${s('status', 'Status')}<th>Komponen</th>
        ${s('site', 'Site')}${s('yearReceived', 'Tahun Penerimaan')}${s('userCategory', 'User Category')}</tr>`;
    _detailHeadGroup = scope;
    markSortedHeader('#detailTable thead', sortState.key, sortState.asc);
}
function _statusCell(d) {
    return !isGood(d.status) && d.breakdownReason
        ? `<span class="badge badge-breakdown bd-clickable" onclick="showBreakdownPopover(event, '${escapeHtml(d.breakdownReason).replace(/'/g, "\\'")}')"><i class="fas fa-xmark"></i> ${escapeHtml(d.status)}</span>`
        : `<span class="badge ${isGood(d.status) ? 'badge-good' : 'badge-breakdown'}"><i class="fas fa-${isGood(d.status) ? 'check' : 'xmark'}"></i> ${escapeHtml(d.status)}</span>`;
}
function _dash(v) { return escapeHtml(v || '') || '<span style="color:var(--text-light);font-size:11px">—</span>'; }
function _catCell(d) {
    return d.userCategory ? `<span class="badge badge-cat" style="font-size:10px">${escapeHtml(d.userCategory)}</span>` : '<span style="color:var(--text-light);font-size:11px">—</span>';
}
function _detailRowHeavy(d, i) {
    return `
        <tr class="${!isGood(d.status) ? 'row-breakdown' : ''}">
            <td>${i + 1}</td>
            <td><strong class="unit-link" title="Lihat profil unit" onclick="showUnitProfile('${escapeHtml(d.id)}')">${escapeHtml(d.name)}</strong></td>
            <td>${escapeHtml(d.model)}</td>
            <td style="font-family:monospace;font-size:12px">${escapeHtml(d.sn)}</td>
            <td>${_dash(d.machineType)}</td><td>${_dash(d.assetCode)}</td><td>${_dash(d.workTool)}</td>
            <td>${_statusCell(d)}</td>
            ${UNIT_GROUPS.heavy.components.map(c => `<td class="${isGood(d[c.key]) ? 'cell-good' : 'cell-bad'}">${escapeHtml(d[c.key] || '') || '—'}</td>`).join('')}
            <td>${escapeHtml(d.site)}</td><td>${_dash(d.yearReceived)}</td><td>${_catCell(d)}</td>
        </tr>`;
}
function _detailRowAll(d, i) {
    const comp = detectIssues(d).filter(x => x !== 'Unit');
    return `
        <tr class="${!isGood(d.status) ? 'row-breakdown' : ''}">
            <td>${i + 1}</td>
            <td><strong class="unit-link" title="Lihat profil unit" onclick="showUnitProfile('${escapeHtml(d.id)}')">${escapeHtml(d.name)}</strong></td>
            <td>${escapeHtml(groupDef(unitGroupOf(d)).shortLabel)}</td>
            <td>${escapeHtml(d.model)}</td>
            <td style="font-family:monospace;font-size:12px">${escapeHtml(d.sn)}</td>
            <td>${_statusCell(d)}</td>
            <td>${comp.length ? comp.map(x => `<span class="badge-component ${issueBadgeClass(x)}">${escapeHtml(x)}</span>`).join(' ') : '<span class="cell-good">Semua baik</span>'}</td>
            <td>${escapeHtml(d.site)}</td><td>${_dash(d.yearReceived)}</td><td>${_catCell(d)}</td>
        </tr>`;
}

function renderTable(data) {
    const scope = effectiveDashGroup();
    renderDetailHead(scope);
    const tbody = document.getElementById('detailBody');
    // An empty tbody under a 17-column header used to be the whole answer when
    // a filter matched nothing: no row, no message, and #emptyState does not
    // help because it is gated on globalData.length, not on the filtered list.
    if (!data || data.length === 0) {
        const span = DETAIL_COLSPAN[scope];
        tbody.innerHTML = dashboardFilterActive()
            ? `<tr><td colspan="${span}" style="text-align:center;padding:24px;color:var(--text-secondary)">
                   Tidak ada unit yang cocok dengan filter.
                   <button class="btn btn-secondary btn-sm" style="margin-left:8px" onclick="clearFilter()">
                       <i class="fas fa-filter-circle-xmark"></i> Hapus filter</button>
               </td></tr>`
            : `<tr><td colspan="${span}" style="text-align:center;padding:24px;color:var(--text-secondary)">Belum ada unit.</td></tr>`;
        return;
    }
    if (scope === 'heavy') { tbody.innerHTML = data.map(_detailRowHeavy).join(''); return; }
    if (scope === 'all') { tbody.innerHTML = data.map(_detailRowAll).join(''); return; }
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
            <td>${escapeHtml(d.yearReceived || '') || '<span style="color:var(--text-light);font-size:11px">—</span>'}</td>
            <td>${d.userCategory ? `<span class="badge badge-cat" style="font-size:10px">${escapeHtml(d.userCategory)}</span>` : '<span style="color:var(--text-light);font-size:11px">—</span>'}</td>
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
    if (key === 'unitGroup') return groupDef(unitGroupOf(d)).shortLabel;
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
    markSortedHeader('#detailTable thead', sortState.key, sortState.asc);
    renderTable(filteredData);
    updateFilterCount(filteredData);
}

// ---- Repair & Maintenance ----
// ---- Damage statistics (dashboard) ----
// Which group a damage record belongs to: its live unit's group, or — for a
// record whose unit is gone — the group stamped on the record itself.
function damageInScope(r) {
    const scope = effectiveDashGroup();
    if (scope === 'all') return true;
    const u = liveUnitFor(r);
    const g = u ? unitGroupOf(u) : (normalizeGroupKey(r.unitGroup) === 'heavy' ? 'heavy' : 'tractor');
    return g === scope;
}

function renderDamageStats() {
    const recs = globalDamages.filter(damageInScope);
    const section = document.getElementById('damageStatsSection');
    if (!section) return;
    if (!recs.length) {
        section.style.display = 'none';
        destroyChart('damageTrendChart');
        return;
    }
    section.style.display = '';

    const open = recs.filter(r => !r.resolved).length;
    const totalEl = document.getElementById('damageStatsTotal');
    if (totalEl) totalEl.textContent = `${recs.length} catatan · ${open} belum selesai`;

    // Counts per damage type
    const typeColors = { 'Mekanis': 'var(--danger)', 'Software': 'var(--info)', 'Device Precision': 'var(--warning)' };
    const counts = {};
    recs.forEach(r => { const t = r.damageType || 'Lainnya'; counts[t] = (counts[t] || 0) + 1; });
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
    const byMonth = months.map(m => recs.filter(r => (r.date || '').startsWith(m.key)).length);
    destroyChart('damageTrendChart');
    charts.damageTrendChart = makeChart('damageTrendChart', {
        type: 'bar',
        data: { labels: months.map(m => m.label), datasets: [{ data: byMonth, backgroundColor: themeColor('--primary', '#D97757'), borderRadius: 4, barPercentage: 0.55 }] },
        options: {
            plugins: { legend: { display: false } },
            scales: { y: { beginAtZero: true, ticks: { precision: 0 } } },
            maintainAspectRatio: false
        }
    });

    // Top-5 most frequently damaged units (live name via liveUnitFor)
    const perUnit = {};
    recs.forEach(r => {
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

// Reads the dashboard's scope, not every unit: with a group selected, the
// repair chips, charts and table describe that group only.
function renderRepair(scope) {
    const list = scope || scopeDashUnits();
    const issueFilterVal = document.getElementById('issueFilter').value;
    const issueData = countIssues(list, groupsInScope(effectiveDashGroup()));
    const chipColors = {
        Unit: themeColor('--danger', '#BF4D43'), Display: COMPONENT_COLORS.display,
        GPS: COMPONENT_COLORS.gps, Steering: COMPONENT_COLORS.steering,
        JDLink: COMPONENT_COLORS.jdlink
    };
    UNIT_GROUPS.heavy.components.forEach(c => { chipColors[c.label] = COMPONENT_COLORS[c.key]; });

    document.getElementById('issueSummary').innerHTML = Object.entries(issueData.counts).map(([key, count]) => `
        <div class="issue-chip">
            <span class="issue-chip__dot" style="background:${chipColors[key]}"></span>
            <span class="issue-chip__label">${key}</span>
            <span class="issue-chip__count">${count}</span>
        </div>`).join('');

    // Top Issue Chart
    const sorted = Object.entries(issueData.counts).filter(([, v]) => v > 0).sort((a, b) => b[1] - a[1]);
    destroyChart('topIssueChart');
    charts.topIssueChart = makeChart('topIssueChart', {
        type: 'bar',
        data: { labels: sorted.map(x => x[0]), datasets: [{ data: sorted.map(x => x[1]), backgroundColor: sorted.map(x => chipColors[x[0]]), borderRadius: 4, barPercentage: 0.5 }] },
        options: { responsive: true, maintainAspectRatio: false, indexAxis: 'y',
            scales: { x: { beginAtZero: true, ticks: { stepSize: 1, font: { size: 11 } }, grid: { color: themeColor('--border-light', '#DEDACE') } }, y: { ticks: { font: { size: 11, family: 'Inter', weight: 600 } }, grid: { display: false } } },
            plugins: { legend: { display: false } } }
    });

    // Issues by Site Chart
    const siteCounts = {};
    list.forEach(d => { if (detectIssues(d).length > 0) { const s = d.site || 'Unknown'; siteCounts[s] = (siteCounts[s] || 0) + 1; } });
    const siteLabels = Object.keys(siteCounts).sort();
    destroyChart('issueBySiteChart');
    charts.issueBySiteChart = makeChart('issueBySiteChart', {
        type: 'bar',
        data: { labels: siteLabels, datasets: [{ data: siteLabels.map(s => siteCounts[s]), backgroundColor: themeColor('--warning', '#BC8A2E'), borderRadius: 4, barPercentage: 0.5 }] },
        options: { responsive: true, maintainAspectRatio: false,
            scales: { y: { beginAtZero: true, ticks: { stepSize: 1, font: { size: 11 } }, grid: { color: themeColor('--border-light', '#DEDACE') } }, x: { ticks: { font: { size: 11, family: 'Inter' } }, grid: { display: false } } },
            plugins: { legend: { display: false } } }
    });

    // Repair Table
    let repairRows = list.filter(d => detectIssues(d).length > 0);
    if (issueFilterVal) repairRows = repairRows.filter(d => detectIssues(d).includes(issueFilterVal));
    const repairBody = document.getElementById('repairBody');
    if (repairRows.length === 0) {
        repairBody.innerHTML = issueFilterVal
            ? `<tr><td colspan="6" style="text-align:center;padding:24px;color:var(--text-secondary)">
                   Tidak ada unit dengan masalah <strong>${escapeHtml(issueFilterVal)}</strong>.
                   <button class="btn btn-secondary btn-sm" style="margin-left:8px"
                           onclick="document.getElementById('issueFilter').value='';renderRepair()">
                       <i class="fas fa-filter-circle-xmark"></i> Tampilkan semua</button>
               </td></tr>`
            : `<tr><td colspan="6" style="text-align:center;padding:24px;color:var(--text-secondary)">
                   Tidak ada unit bermasalah. Semua komponen terbaca normal.</td></tr>`;
        return;
    }
    repairBody.innerHTML = repairRows.map((d, i) => `
        <tr>
            <td>${i + 1}</td>
            <td><strong>${escapeHtml(d.name)}</strong></td>
            <td>${escapeHtml(d.model)}</td>
            <td style="font-family:monospace;font-size:12px">${escapeHtml(d.sn)}</td>
            <td>${detectIssues(d).map(x => `<span class="badge-component ${issueBadgeClass(x)}">${escapeHtml(x)}</span>`).join(' ')}</td>
            <td>${escapeHtml(d.site)}</td>
        </tr>`).join('');
}

// ============================================================
// FILTERS
// ============================================================

function populateFilters(units) {
    const pool = units || scopeDashUnits();
    const statuses = [...new Set(pool.map(d => d.status))].filter(Boolean).sort();
    const sites = [...new Set(pool.map(d => d.site))].filter(Boolean).sort();
    document.getElementById('statusFilter').innerHTML = `<option value="">All Status</option>` + statuses.map(s => `<option value="${escapeHtml(s)}">${escapeHtml(s)}</option>`).join('');
    document.getElementById('siteFilter').innerHTML = `<option value="">All Sites</option>` + sites.map(s => `<option value="${escapeHtml(s)}">${escapeHtml(s)}</option>`).join('');
}

function applyFilterLogic() {
    const keyword = document.getElementById('searchInput').value.toLowerCase();
    const statusVal = document.getElementById('statusFilter').value;
    const siteVal = document.getElementById('siteFilter').value;
    const compVal = document.getElementById('componentFilter').value;

    // The group scope is applied here, inside the filter, so every re-render —
    // a snapshot, a reset, a restore — keeps the group the user chose.
    return scopeDashUnits().filter(d => {
        if (statusVal && d.status !== statusVal) return false;
        if (siteVal && d.site !== siteVal) return false;
        const hay = isHeavy(d) ? `${d.name} ${d.model} ${d.sn} ${d.machineType || ''} ${d.assetCode || ''}` : `${d.name} ${d.model} ${d.sn}`;
        if (keyword && !hay.toLowerCase().includes(keyword)) return false;
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
    filteredData = scopeDashUnits();
    sortState.key = null;
    updateDashboard(filteredData);
}

function updateFilterCount(data) {
    const el = document.getElementById('filterCount');
    const total = scopeDashUnits().length;
    el.textContent = data.length === total ? `${total} units` : `${data.length} of ${total} units`;
}

// ============================================================
// EXPORT
// ============================================================

function exportCSV(data) {
    if (!canCsv('export')) return;
    const exportData = Array.isArray(data) ? data : filteredData;
    if (exportData.length === 0) { showToast('No data to export', 'warning'); return; }
    // Three shapes. No heavy rows: today's file, verbatim. Heavy only: its own
    // columns. Mixed: today's 21 columns plus the heavy ones. Every shape that
    // holds heavy rows carries "Unit Group", so exporting and re-importing as
    // new units keeps each unit in its group.
    const heavyN = exportData.filter(isHeavy).length;
    if (heavyN) { exportUnitsCSVGrouped(exportData, heavyN === exportData.length); return; }
    const headers = ['No', 'Nickname', 'Model', 'Serial Number', 'Implement', 'Status', 'Display', 'GPS', 'Steering', 'JDLink', 'Site',
                     'Tahun Penerimaan', 'User Category', 'GPS License', 'Display License',
                     'GPS License Start Date', 'GPS License Expiration Date',
                     'Display License Start Date', 'Display License Expiration Date', 'Remarks', 'Breakdown Reason'];
    const rows = exportData.map((d, i) => [i + 1, d.name, d.model, d.sn, d.implement || '', d.status, d.display, d.gps, d.steering, d.jdlink, d.site,
                     d.yearReceived || '', d.userCategory || '',
                     effectiveLicense(d, 'gps').type || '', effectiveLicense(d, 'display').type || '',
                     d.gpsLicenseStartDate || d.licenseStartDate || '',
                     d.gpsLicenseEndDate   || d.licenseEndDate   || '',
                     d.displayLicenseStartDate || '',
                     d.displayLicenseEndDate   || '',
                     d.remarks || '',
                     (!isGood(d.status) && d.breakdownReason) ? d.breakdownReason : '']);
    const csv = [headers, ...rows].map(row => row.map(csvCell).join(',')).join('\n');
    const blob = new Blob(['\uFEFF' + csv], { type: 'text/csv;charset=utf-8;' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = `tractor_monitoring_${toISODate()}.csv`;
    a.click();
    URL.revokeObjectURL(url);
    showToast(`${exportData.length} unit diekspor ke CSV`, 'success');
}

function exportUnitsCSVGrouped(exportData, heavyOnly) {
    const HEAVY_COLS = ['Jenis Alat', 'Nomor Lambung', 'Alat Kerja', ...UNIT_GROUPS.heavy.components.map(c => c.label)];
    const heavyCells = d => [d.machineType || '', d.assetCode || '', d.workTool || '',
                             ...UNIT_GROUPS.heavy.components.map(c => d[c.key] || '')];
    let headers, rows, prefix;
    if (heavyOnly) {
        headers = ['No', 'Unit Group', 'Nickname', 'Model', 'Serial Number', 'Jenis Alat', 'Nomor Lambung', 'Alat Kerja', 'Status',
                   ...UNIT_GROUPS.heavy.components.map(c => c.label),
                   'Site', 'Tahun Penerimaan', 'User Category', 'Remarks', 'Breakdown Reason'];
        rows = exportData.map((d, i) => [i + 1, 'heavy', d.name, d.model, d.sn, d.machineType || '', d.assetCode || '', d.workTool || '',
                   d.status, ...UNIT_GROUPS.heavy.components.map(c => d[c.key] || ''),
                   d.site, d.yearReceived || '', d.userCategory || '', d.remarks || '',
                   (!isGood(d.status) && d.breakdownReason) ? d.breakdownReason : '']);
        prefix = UNIT_GROUPS.heavy.csvPrefix;
    } else {
        headers = ['No', 'Nickname', 'Model', 'Serial Number', 'Implement', 'Status', 'Display', 'GPS', 'Steering', 'JDLink', 'Site',
                   'Tahun Penerimaan', 'User Category', 'GPS License', 'Display License',
                   'GPS License Start Date', 'GPS License Expiration Date',
                   'Display License Start Date', 'Display License Expiration Date', 'Remarks', 'Breakdown Reason',
                   'Unit Group', ...HEAVY_COLS];
        rows = exportData.map((d, i) => {
            const h = isHeavy(d);
            // A row never reads the other group's fields.
            return [i + 1, d.name, d.model, d.sn, h ? '' : (d.implement || ''), d.status,
                h ? '' : d.display, h ? '' : d.gps, h ? '' : d.steering, h ? '' : d.jdlink, d.site,
                d.yearReceived || '', d.userCategory || '',
                h ? '' : (effectiveLicense(d, 'gps').type || ''), h ? '' : (effectiveLicense(d, 'display').type || ''),
                h ? '' : (d.gpsLicenseStartDate || d.licenseStartDate || ''),
                h ? '' : (d.gpsLicenseEndDate || d.licenseEndDate || ''),
                h ? '' : (d.displayLicenseStartDate || ''), h ? '' : (d.displayLicenseEndDate || ''),
                d.remarks || '', (!isGood(d.status) && d.breakdownReason) ? d.breakdownReason : '',
                unitGroupOf(d), ...(h ? heavyCells(d) : HEAVY_COLS.map(() => ''))];
        });
        prefix = 'unit_monitoring';
    }
    const csv = [headers, ...rows].map(row => row.map(csvCell).join(',')).join('\n');
    const blob = new Blob(['\uFEFF' + csv], { type: 'text/csv;charset=utf-8;' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = `${prefix}_${toISODate()}.csv`;
    a.click();
    URL.revokeObjectURL(url);
    showToast(`${exportData.length} unit diekspor ke CSV`, 'success');
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
    if (!el) return;
    // Unchanged until heavy equipment exists.
    if (!hasHeavyUnits()) { el.textContent = `${globalData.length} unit(s) in database`; return; }
    const g = effectiveEditGroup();
    el.textContent = `${unitsOfGroup(globalData, g).length} ${groupDef(g).label} · ${globalData.length} unit(s) in database`;
}

function toggleImportPanel() {
    document.getElementById('importPanel').classList.toggle('open');
}

function handleEditCSVImport(file) {
    if (!canCsv('full')) return;
    if (!requireEdit('editUnits')) return;
    showLoading(true);
    Papa.parse(file, {
        header: true,
        skipEmptyLines: true,
        complete: result => {
            const tab = effectiveEditGroup();
            const { valid, rejected, groupWarnings } = processData(result.data,
                { fallbackGroup: tab, fileGroup: inferCsvGroup(result.meta && result.meta.fields) });

            // Split rows into brand-new units vs updates to existing SNs
            const existingSNs = new Set(globalData.map(d => (d.sn || '').toLowerCase()).filter(Boolean));
            const newUnits = [];
            const updateCandidates = [];
            valid.forEach(u => {
                if (existingSNs.has((u.sn || '').toLowerCase())) updateCandidates.push(u);
                else newUnits.push(u);
            });

            // Asked BEFORE anything is written: new units about to land in the
            // group other than the open tab. Cancel aborts the whole import.
            const offTab = newUnits.filter(u => u.unitGroup !== tab);
            if (offTab.length && !confirm(
                `${offTab.length} unit baru akan ditambahkan sebagai ${groupDef(otherGroup(tab).key).label}, bukan ${groupDef(tab).label}. Lanjutkan?\n\n` +
                `Batal = batalkan seluruh impor (tidak ada yang ditulis).`)) {
                showLoading(false);
                document.getElementById('importPanel').classList.remove('open');
                showToast('Impor dibatalkan — tidak ada yang ditulis', 'info');
                return;
            }
            const notes = [];
            groupWarnings.forEach(w => notes.push({ name: w.name, sn: w.sn,
                reason: `Unit Group "${w.value}" tidak dikenal — dibaca sebagai ${groupDef(tab).label}` }));
            valid.forEach(u => {
                const g = unitGroupOf(u);
                const ignored = otherGroup(g).onlyFields.filter(f => !sameStoredValue(u[f], ''));
                if (ignored.length) notes.push({ name: u.name, sn: u.sn,
                    reason: `kolom ${ignored.join(', ')} diabaikan: bukan milik ${groupDef(g).shortLabel}` });
            });
            const blank = newUnits.filter(u => u.unitGroup === 'heavy' && HEAVY_COMPONENT_KEYS.every(k => sameStoredValue(u[k], ''))).length;
            if (blank) notes.push({ name: `${blank} unit`, sn: '-',
                reason: `${UNIT_GROUPS.heavy.shortLabel} tanpa status komponen — dihitung bermasalah sampai diisi` });

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
                updateFailed: updateResult.failed,
                notes
            });
            renderEditTable();
            showLoading(false);
            document.getElementById('importPanel').classList.remove('open');
        },
        error: err => {
            showToast('Gagal membaca CSV: ' + err.message, 'error');
            showLoading(false);
        }
    });
}

// `notes` carries the unit-group warnings (unknown Unit Group values, columns
// of the other group that were ignored, heavy units with no component status).
// A tractor CSV produces none, so its toast and report are unchanged.
function showImportReport({ total, added, skipped, skippedDetails, rejected, updated = 0, unchanged = 0, updateFailed = [], notes = [] }) {
    const hasIssues = skipped > 0 || rejected.length > 0 || updateFailed.length > 0 || notes.length > 0;
    const type = (added > 0 || updated > 0) ? (hasIssues ? 'warning' : 'success') : 'warning';

    const parts = [`${added} added`];
    if (updated > 0) parts.push(`${updated} updated`);
    if (unchanged > 0) parts.push(`${unchanged} unchanged`);
    if (skipped > 0) parts.push(`${skipped} duplicate(s) skipped`);
    if (rejected.length > 0) parts.push(`${rejected.length} rejected`);
    if (updateFailed.length > 0) parts.push(`${updateFailed.length} update failed`);
    const summary = parts.join(' · ');
    showToast(`Impor: ${summary} (dari ${total} baris)`, type);

    if (!hasIssues) return;

    const rows = [
        ...skippedDetails.map(d => `<tr><td>${escapeHtml(d.name || '-')}</td><td style="font-family:monospace">${escapeHtml(d.sn || '-')}</td><td>${escapeHtml(d.reason)}</td></tr>`),
        ...updateFailed.map(f => `<tr><td>-</td><td style="font-family:monospace">${escapeHtml(f.sn || '-')}</td><td>${escapeHtml(f.reason)}</td></tr>`),
        ...rejected.map(r => `<tr><td>${escapeHtml(r.name || '-')}</td><td>Row ${r.row}</td><td>${escapeHtml(r.reason)}</td></tr>`),
        ...notes.map(n => `<tr><td>${escapeHtml(n.name || '-')}</td><td style="font-family:monospace">${escapeHtml(n.sn || '-')}</td><td>${escapeHtml(n.reason)}</td></tr>`)
    ].join('');

    document.getElementById('importReportBody').innerHTML = rows;
    document.getElementById('importReportSummary').textContent = summary;
    document.getElementById('importReportModal').classList.add('open');
}

// A restore used to end with one toast naming the collections it had written.
// Anything absent from the file produced no line at all, so a v3 backup looked
// like a complete restore. This lists every collection the app knows about,
// including the ones that were not in the file and the ones skipped for lack
// of rights — the two outcomes people most need to see.
function showRestoreReport(rows, version, merge) {
    const missing = rows.filter(r => r.note === 'tidak ada di berkas ini').length;
    const done = rows.filter(r => r.note === 'digabung' || r.note === 'diganti').length;
    const summary = `${done} koleksi ${merge ? 'digabung' : 'diganti'}`
        + (missing ? ` · ${missing} koleksi tidak ada di berkas ini` : '');

    const excluded = BACKUP_EXCLUDED.map(e =>
        `<tr><td>${escapeHtml(e.label)}</td><td>—</td><td>tidak pernah dicadangkan — ${escapeHtml(e.why)}</td></tr>`).join('');
    const body = rows.map(r =>
        `<tr><td data-label="Koleksi">${escapeHtml(r.label)}</td>`
        + `<td data-label="Jumlah">${escapeHtml(String(r.count))}</td>`
        + `<td data-label="Keterangan">${escapeHtml(r.note)}</td></tr>`).join('');

    const note = document.getElementById('restoreReportNote');
    if (note) {
        note.innerHTML = version < 4
            ? `<strong>Cadangan lama (v${version || '?'}).</strong> Berkas ini dibuat sebelum data Tim dan Gudang ikut dicadangkan, jadi data itu tidak ada di dalamnya dan tidak diubah sama sekali.`
            : '';
        note.style.display = version < 4 ? '' : 'none';
    }
    const sum = document.getElementById('restoreReportSummary');
    if (sum) sum.textContent = summary;
    const tbody = document.getElementById('restoreReportBody');
    if (tbody) tbody.innerHTML = body + excluded;
    const modal = document.getElementById('restoreReportModal');
    if (modal) modal.classList.add('open');
    else showToast(summary, missing ? 'warning' : 'success');
}

// ---- Auto backup ring ----
// writeAutoBackup() has pushed a full copy of the units array into
// localStorage on every save since the first version, keeping three. Nothing
// in the repo ever read it: the quota — the same quota the damage photos were
// moved out of — was paying for three rollback points nobody could reach.
// These three functions are that missing half.
function readAutoBackups() {
    try {
        const ring = JSON.parse(localStorage.getItem(BACKUP_RING_KEY) || '[]');
        return Array.isArray(ring) ? ring.slice().reverse() : [];   // newest first
    } catch (e) { return []; }
}

function showAutoBackups() {
    if (!isOwner || !isOwner()) { showToast('Hanya owner yang bisa memulihkan cadangan otomatis', 'warning'); return; }
    const ring = readAutoBackups();
    const list = document.getElementById('autoBackupList');
    if (list) {
        list.innerHTML = ring.length === 0
            ? '<li style="color:var(--text-secondary);padding:8px 0">Belum ada cadangan otomatis di perangkat ini.</li>'
            : ring.map((b, i) => {
                const when = b.at ? new Date(b.at).toLocaleString('id-ID',
                    { day: 'numeric', month: 'short', hour: '2-digit', minute: '2-digit' }) : '—';
                return `<li>
                    <span><strong>${escapeHtml(when)}</strong>
                        <span style="color:var(--text-secondary)"> · ${Number(b.count) || 0} unit</span></span>
                    <button class="btn btn-secondary btn-sm" onclick="restoreAutoBackup(${i})">
                        <i class="fas fa-rotate-left"></i> Pulihkan</button>
                </li>`;
            }).join('');
    }
    document.getElementById('autoBackupModal').classList.add('open');
}

function closeAutoBackups() {
    document.getElementById('autoBackupModal').classList.remove('open');
}

// Goes through the same cloud-mirrored replace as a file restore. A local-only
// rollback would be undone by the next units snapshot, and the units it
// removed would come straight back.
// A unit's group never changes, so a backup copy and the live copy of the
// same id always agree on it — unless something went wrong. A REPLACE restore
// that would change a live unit's group is refused before anything is written:
// the write is set(merge:true), so the old group would come back alongside
// fields of the new one.
function _refuseCrossGroupRestore(units) {
    const live = new Map(globalData.map(u => [u.id, u]));
    const conflicts = (units || []).filter(u => u && u.id && live.has(u.id) && unitGroupOf(live.get(u.id)) !== unitGroupOf(u));
    if (!conflicts.length) return false;
    showToast(`Pemulihan dibatalkan: ${conflicts.length} unit di cadangan punya kelompok berbeda dengan data sekarang (id sama). `
        + 'Hapus unit itu dulu, atau pilih GABUNG.', 'error');
    return true;
}

function restoreAutoBackup(index) {
    if (!isOwner || !isOwner()) return;
    if (!canWriteUnits('restore cadangan otomatis')) return;
    const ring = readAutoBackups();
    const entry = ring[index];
    if (!entry || !Array.isArray(entry.units)) { showToast('Cadangan itu tidak terbaca', 'error'); return; }

    if (_refuseCrossGroupRestore(entry.units)) return;
    const when = entry.at ? new Date(entry.at).toLocaleString('id-ID') : 'waktu tidak diketahui';
    if (!confirm(`Kembalikan ${entry.units.length} unit ke keadaan ${when}?\n\n`
        + `${globalData.length} unit yang ada sekarang akan diganti. Data lain (kerusakan, lisensi, tim, gudang) tidak ikut berubah.`)) return;

    const previousIds = globalData.map(u => u.id);
    globalData = entry.units.map(u => ({ ...u, id: u.id || generateId() }));
    const keptIds = new Set(globalData.map(u => u.id));
    saveToStorage(globalData);
    cloudDeleteUnits(previousIds.filter(id => !keptIds.has(id)));
    cloudPushUnits(globalData);
    logEvent({ action: 'restore', unitName: '-', after: `${globalData.length} unit dikembalikan dari cadangan otomatis (${when})` });
    closeAutoBackups();
    renderEditTable();
    filteredData = scopeDashUnits();
    updateDashboard(filteredData);
    showToast(`${globalData.length} unit dikembalikan ke keadaan ${when}`, 'success');
}

function closeRestoreReport() {
    const m = document.getElementById('restoreReportModal');
    if (m) m.classList.remove('open');
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

    const g = effectiveEditGroup();
    let rows = unitsOfGroup(globalData, g);
    if (query) rows = rows.filter(d => (g === 'heavy'
        ? `${d.name} ${d.model} ${d.sn} ${d.machineType || ''} ${d.assetCode || ''} ${d.workTool || ''} ${d.site}`
        : `${d.name} ${d.model} ${d.sn} ${d.implement || ''} ${d.site}`).toLowerCase().includes(query));
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

// ---- Edit Units: one table per group ----
// The tractor header is captured from the static markup at startup and put
// back verbatim; only the heavy tab gets a header built here.
const EDIT_COLSPAN = { tractor: 21, heavy: 19 };
function sortTh(key, label, fn) {
    return `<th tabindex="0" role="button" onclick="${fn}('${key}')">${label} <span class="sort-icon"><i class="fas fa-sort"></i></span></th>`;
}
function heavyEditHead() {
    const s = (k, l) => sortTh(k, l, 'sortEditTable');
    return `<tr>
        <th class="col-check"><input type="checkbox" id="selectAll" aria-label="Pilih semua unit" onchange="toggleSelectAll()"></th>
        ${s('no', 'No')}${s('name', 'Nickname')}${s('model', 'Model')}${s('sn', 'Serial Number')}
        ${s('machineType', 'Jenis Alat')}${s('assetCode', 'Nomor Lambung')}${s('workTool', 'Alat Kerja')}${s('status', 'Status')}
        ${UNIT_GROUPS.heavy.components.map(c => s(c.key, c.label)).join('')}
        ${s('site', 'Site')}${s('yearReceived', 'Tahun Penerimaan')}${s('userCategory', 'User Category')}
        <th>Remarks</th><th>Attachments</th><th class="col-actions">Actions</th>
    </tr>`;
}
function renderEditHead() {
    const g = effectiveEditGroup();
    const head = document.querySelector('#editTable thead');
    if (head && _editHeadGroup !== g) {
        head.innerHTML = g === 'heavy' ? heavyEditHead() : EDIT_HEADS.tractor;
        _editHeadGroup = g;
    }
    markSortedHeader('#editTable thead', editSortState.key, editSortState.asc);
}

function renderEditGroupTabs() {
    const g = effectiveEditGroup();
    document.querySelectorAll('.eu-tab').forEach(btn => {
        const key = btn.dataset.group;
        const def = groupDef(key);
        const on = key === g;
        btn.style.display = '';
        btn.classList.toggle('active', on);
        btn.setAttribute('aria-selected', on ? 'true' : 'false');
        btn.innerHTML = `<i class="fas ${def.icon}"></i> ${escapeHtml(def.label)} <span class="team-tab__count">${unitsOfGroup(globalData, key).length}</span>`;
    });
    const title = document.getElementById('editTableTitle');
    if (title) title.textContent = g === 'heavy' ? `Unit Database — ${UNIT_GROUPS.heavy.shortLabel}` : 'Unit Database';
    const hint = document.getElementById('importHint');
    if (hint) {
        if (!hint.dataset.tractor) hint.dataset.tractor = hint.innerHTML;
        hint.innerHTML = g === 'heavy'
            ? `Unit baru masuk ke ${escapeHtml(UNIT_GROUPS.heavy.label)} kecuali kolom "Unit Group" berkata lain. Serial number duplikat dilewati.`
            : hint.dataset.tractor;
    }
}

function switchEditUnitsGroup(g, opts) {
    if (!UNIT_GROUP_KEYS.includes(g)) return;
    editUnitsGroup = g;
    _editGroupPicked = true;
    if (!opts || opts.persist !== false) writePref('editUnitsGroup', g);
    editSortState = { key: null, asc: true };
    ['editSearch', 'editStatusFilter', 'editSiteFilter'].forEach(id => {
        const el = document.getElementById(id); if (el) el.value = '';
    });
    populateEditFilters();
    renderEditTable();
}

function _editRowHeavy(d, i, ce) {
    const remarks = d.remarks || '';
    const remarksShort = remarks.length > 40 ? remarks.slice(0, 40) + '…' : remarks;
    const id = escapeHtml(d.id);
    const cell = (label, field, style) =>
        `<td data-label="${label}"${style ? ` style="${style}"` : ''}><span class="inline-edit" contenteditable="${ce}" data-id="${id}" data-field="${field}" onblur="saveInlineEdit(this)">${escapeHtml(d[field] == null ? '' : d[field])}</span></td>`;
    return `
        <tr>
            <td class="col-check"><input type="checkbox" class="unit-check" data-id="${id}" onchange="updateSelectedCount()"></td>
            <td>${i + 1}</td>
            ${cell('Nickname', 'name')}${cell('Model', 'model')}
            <td data-label="SN" style="font-family:monospace;font-size:12px">${escapeHtml(d.sn)}</td>
            ${cell('Jenis Alat', 'machineType')}${cell('Nomor Lambung', 'assetCode')}${cell('Alat Kerja', 'workTool')}
            ${cell('Status', 'status')}
            ${UNIT_GROUPS.heavy.components.map(c => cell(c.label, c.key)).join('')}
            ${cell('Site', 'site')}${cell('Tahun Penerimaan', 'yearReceived')}
            <td data-label="User Category">${d.userCategory ? `<span class="badge badge-cat" style="font-size:10px">${escapeHtml(d.userCategory)}</span>` : '<span style="color:var(--text-light);font-size:11px">—</span>'}</td>
            <td data-label="Remarks" style="max-width:180px;font-size:12px;color:var(--text-secondary)" title="${escapeHtml(remarks)}">${escapeHtml(remarksShort) || '<span style="color:var(--text-light)">—</span>'}</td>
            <td class="col-attach" data-label="Attachments">${renderAttachCell(d)}</td>
            <td class="col-actions">
                <div class="row-actions">
                    <button class="btn btn-secondary" title="Profil" onclick="showUnitProfile('${id}')"><i class="fas fa-eye"></i></button>
                    <button class="btn btn-secondary" title="History" onclick="showHistory('${id}')"><i class="fas fa-clock-rotate-left"></i></button>
                    <button class="btn btn-secondary" title="Edit" onclick="editUnit('${id}')"><i class="fas fa-pen"></i></button>
                    <button class="btn btn-secondary" title="Delete" onclick="deleteUnit('${id}')"><i class="fas fa-trash" style="color:var(--danger)"></i></button>
                </div>
            </td>
        </tr>`;
}

function renderEditTable() {
    renderEditHead();
    renderEditGroupTabs();
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
    const group = effectiveEditGroup();
    if (rows.length === 0) {
        const empty = group === 'heavy'
            ? `Belum ada unit ${escapeHtml(UNIT_GROUPS.heavy.label)}. Klik <strong>Add Unit</strong> atau <strong>Import CSV</strong> untuk memulai.`
            : 'Belum ada unit. Klik <strong>Add Unit</strong> atau <strong>Import CSV</strong> untuk memulai.';
        tbody.innerHTML = `<tr><td colspan="${EDIT_COLSPAN[group]}" style="text-align:center;padding:24px;color:var(--text-secondary)">${(query || statusVal || siteVal) ? 'Tidak ada unit yang cocok dengan filter' : empty}</td></tr>`;
        return;
    }

    const _ceEdit = hasAccess('editUnits', 'edit') ? 'true' : 'false'; // inline editing off for view-only
    if (group === 'heavy') {
        tbody.innerHTML = rows.map((d, i) => _editRowHeavy(d, i, _ceEdit)).join('');
        return;
    }
    tbody.innerHTML = rows.map((d, i) => {
        const remarks = d.remarks || '';
        const remarksShort = remarks.length > 40 ? remarks.slice(0, 40) + '…' : remarks;
        // A cell without data-label is display:none in card mode
        // (style.css). Two here stay unlabelled on purpose: the row number
        // means nothing once rows are stacked as cards, and bulk-select is a
        // desktop action — a checkbox column on a phone mostly produces
        // accidental selections. Everything else must carry a label, or the
        // column silently vanishes on the device the field team actually uses.
        return `
        <tr>
            <td class="col-check"><input type="checkbox" class="unit-check" data-id="${escapeHtml(d.id)}" onchange="updateSelectedCount()"></td>
            <td>${i + 1}</td>
            <td data-label="Nickname"><span class="inline-edit" contenteditable="${_ceEdit}" data-id="${escapeHtml(d.id)}" data-field="name" onblur="saveInlineEdit(this)">${escapeHtml(d.name)}</span></td>
            <td data-label="Model"><span class="inline-edit" contenteditable="${_ceEdit}" data-id="${escapeHtml(d.id)}" data-field="model" onblur="saveInlineEdit(this)">${escapeHtml(d.model)}</span></td>
            <td data-label="SN" style="font-family:monospace;font-size:12px">${escapeHtml(d.sn)}</td>
            <td data-label="Implement"><span class="inline-edit" contenteditable="${_ceEdit}" data-id="${escapeHtml(d.id)}" data-field="implement" onblur="saveInlineEdit(this)">${escapeHtml(d.implement || '')}</span></td>
            <td data-label="Status"><span class="inline-edit" contenteditable="${_ceEdit}" data-id="${escapeHtml(d.id)}" data-field="status" onblur="saveInlineEdit(this)">${escapeHtml(d.status)}</span></td>
            <td data-label="Display"><span class="inline-edit" contenteditable="${_ceEdit}" data-id="${escapeHtml(d.id)}" data-field="display" onblur="saveInlineEdit(this)">${escapeHtml(d.display)}</span></td>
            <td data-label="GPS"><span class="inline-edit" contenteditable="${_ceEdit}" data-id="${escapeHtml(d.id)}" data-field="gps" onblur="saveInlineEdit(this)">${escapeHtml(d.gps)}</span></td>
            <td data-label="Steering"><span class="inline-edit" contenteditable="${_ceEdit}" data-id="${escapeHtml(d.id)}" data-field="steering" onblur="saveInlineEdit(this)">${escapeHtml(d.steering)}</span></td>
            <td data-label="JDLink"><span class="inline-edit" contenteditable="${_ceEdit}" data-id="${escapeHtml(d.id)}" data-field="jdlink" onblur="saveInlineEdit(this)">${escapeHtml(d.jdlink)}</span></td>
            <td data-label="Site"><span class="inline-edit" contenteditable="${_ceEdit}" data-id="${escapeHtml(d.id)}" data-field="site" onblur="saveInlineEdit(this)">${escapeHtml(d.site)}</span></td>
            <td data-label="Tahun Penerimaan"><span class="inline-edit" contenteditable="${_ceEdit}" data-id="${escapeHtml(d.id)}" data-field="yearReceived" onblur="saveInlineEdit(this)">${escapeHtml(d.yearReceived || '')}</span></td>
            <td data-label="User Category">${d.userCategory ? `<span class="badge badge-cat" style="font-size:10px">${escapeHtml(d.userCategory)}</span>` : '<span style="color:var(--text-light);font-size:11px">—</span>'}</td>
            <td data-label="GPS License">${licenseTypeBadge(d, 'gps')}</td>
            <td data-label="GPS Expiry">${licenseBadgeFor(d, 'gps')}</td>
            <td data-label="Display License">${licenseTypeBadge(d, 'display')}</td>
            <td data-label="Display Expiry">${licenseBadgeFor(d, 'display')}</td>
            <td data-label="Remarks" style="max-width:180px;font-size:12px;color:var(--text-secondary)" title="${escapeHtml(remarks)}">${escapeHtml(remarksShort) || '<span style="color:var(--text-light)">—</span>'}</td>
            <td class="col-attach" data-label="Attachments">${renderAttachCell(d)}</td>
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
    markSortedHeader('#editTable thead', editSortState.key, editSortState.asc);
    renderEditTable();
}

function populateEditFilters() {
    const pool = unitsOfGroup(globalData, effectiveEditGroup());
    const statuses = [...new Set(pool.map(d => d.status))].filter(Boolean).sort();
    const sites = [...new Set(pool.map(d => d.site))].filter(Boolean).sort();
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
    if (!hasAccess('editUnits', 'edit')) {
        // Revert the DOM if a view-only user somehow triggered this
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

    // Tractor rows keep the strict comparison verbatim — including the writes it
    // makes today to normalise whitespace or a numeric year. For a heavy unit
    // an absent field left empty is not a change: comparing undefined !== ''
    // used to write '' to Firestore and toast "diperbarui" on a mere click.
    const changed = unit && (isHeavy(unit) ? !sameStoredValue(unit[field], newValue) : unit[field] !== newValue);
    if (changed) {
        // Intercept status changing TO Breakdown → prompt for reason
        if (field === 'status' && !isGood(newValue) && isGood(unit.status)) {
            _pendingBreakdown = { unitId: id, fields: { status: newValue }, isInline: true, el };
            document.getElementById('breakdownReasonText').value = '';
            document.getElementById('breakdownReasonModal').classList.add('open');
            return;
        }
        if (updateUnit(id, { [field]: newValue })) {
            showToast(`${COMPONENT_LABELS[field] || UNIT_FIELD_LABELS[field] || field.charAt(0).toUpperCase() + field.slice(1)} diperbarui`, 'success');
        }
    }
}

// ---- Breakdown reason modal ----
function confirmBreakdownReason() {
    const reason = (document.getElementById('breakdownReasonText').value || '').trim();
    if (!reason) {
        showToast('Isi alasan breakdown', 'warning');
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
        showToast('Status diperbarui — alasan breakdown dicatat', 'success');
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
    const bulk = document.getElementById('btnBulkEdit');
    const bulkCount = document.getElementById('bulkEditCount');
    if (bulkCount) bulkCount.textContent = count;
    if (bulk) bulk.style.display = count > 0 ? '' : 'none';
}

// ---- Bulk edit selected units ----
function openBulkEdit() {
    if (!requireEdit('editUnits')) return;
    if (selectedUnitIds.size === 0) return;
    document.getElementById('bulkEditTitle').textContent = `${selectedUnitIds.size} unit`;
    ['bulkChkSite', 'bulkChkStatus', 'bulkChkCategory', 'bulkChkYear', 'bulkChkImplement', 'bulkChkWorkTool'].forEach(id => {
        const el = document.getElementById(id); if (el) el.checked = false;
    });
    document.getElementById('bulkSite').value = '';
    document.getElementById('bulkYear').value = '';
    document.getElementById('bulkImplement').value = '';
    const wt = document.getElementById('bulkWorkTool'); if (wt) wt.value = '';
    // The selection is always one tab's rows (renderEditTable clears it), so
    // only that group's own field is offered: Implement for agricultural units,
    // Alat Kerja for heavy equipment.
    const g = effectiveEditGroup();
    document.querySelectorAll('#bulkEditModal [data-group-only]').forEach(el => {
        el.style.display = el.dataset.groupOnly === g ? '' : 'none';
    });
    if (g === 'heavy') populateHeavySuggestionLists();
    // Category options
    const cat = document.getElementById('bulkCategory');
    cat.innerHTML = '<option value="">—</option>' +
        (userCategories || []).map(c => `<option value="${escapeHtml(c.name)}">${escapeHtml(c.name)}</option>`).join('');
    populateImplementUnitList();
    document.getElementById('bulkEditModal').classList.add('open');
}

function closeBulkEdit() {
    document.getElementById('bulkEditModal').classList.remove('open');
}

function applyBulkEdit() {
    if (!requireEdit('editUnits')) return;
    const ids = [...selectedUnitIds];
    if (ids.length === 0) { closeBulkEdit(); return; }

    const fields = {};
    if (document.getElementById('bulkChkSite').checked)     fields.site = document.getElementById('bulkSite').value.trim();
    if (document.getElementById('bulkChkStatus').checked)   fields.status = document.getElementById('bulkStatus').value;
    if (document.getElementById('bulkChkCategory').checked) fields.userCategory = document.getElementById('bulkCategory').value;
    if (document.getElementById('bulkChkYear').checked)     fields.yearReceived = document.getElementById('bulkYear').value.trim();
    const bg = effectiveEditGroup();
    if (bg !== 'heavy' && document.getElementById('bulkChkImplement').checked) fields.implement = document.getElementById('bulkImplement').value.trim();
    if (bg === 'heavy' && document.getElementById('bulkChkWorkTool')?.checked) fields.workTool = document.getElementById('bulkWorkTool').value.trim();

    if (Object.keys(fields).length === 0) { showToast('Centang minimal satu field untuk diubah', 'warning'); return; }

    // A ticked box with an empty input clears that field on every selected unit
    // and there is no undo, so make the destructive part explicit rather than
    // hiding it behind the generic confirm.
    const labels = { site: 'Site', status: 'Status', userCategory: 'User Category',
                     yearReceived: 'Tahun Penerimaan', implement: 'Implement', workTool: 'Alat Kerja' };
    const cleared = Object.keys(fields).filter(k => fields[k] === '');
    if (cleared.length) {
        const names = cleared.map(k => labels[k] || k).join(', ');
        if (!confirm(`Field berikut akan DIKOSONGKAN pada ${ids.length} unit: ${names}.\n\n` +
                     `Tindakan ini tidak bisa dibatalkan. Lanjutkan?`)) return;
    }
    if (!confirm(`Terapkan perubahan ke ${ids.length} unit?`)) return;

    let n = 0;
    ids.forEach(id => {
        const unit = globalData.find(u => u.id === id);
        const perUnit = { ...fields };
        // Only stamp a placeholder reason on units that were actually running;
        // never overwrite an existing diagnosis on a unit already broken down.
        if (perUnit.status === 'Breakdown') {
            if (unit && !isGood(unit.status) && unit.breakdownReason) {
                // keep the real reason
            } else {
                perUnit.breakdownReason = 'Diset massal (bulk edit)';
            }
        }
        if (updateUnit(id, perUnit)) n++;
    });
    closeBulkEdit();
    renderEditTable();
    showToast(`${n} unit diperbarui`, 'success');
}

function deleteUnit(id) {
    if (!requireEdit('editUnits')) return;
    const unit = globalData.find(d => d.id === id);
    if (!unit) return;
    const { removed } = deleteUnits([id]);
    renderEditTable();
    showUndoToast(`Unit "${unit.name || unit.sn}" deleted`, removed);
}

function deleteSelected() {
    if (!requireEdit('editUnits')) return;
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
    if (!requireEdit('editUnits')) return;
    _pendingAttachPurge = [];
    const restored = lastDeletedUnits;
    globalData = [...globalData, ...restored];
    saveToStorage(globalData);
    cloudPushUnits(restored);
    restored.forEach(u => logEvent({ action: 'restore', unitId: u.id, unitName: u.name, after: 'undelete' }));
    recordChange({ type: 'restored', detail: `${restored.length} unit(s) restored` });
    renderEditTable();
    showToast(`${restored.length} unit dipulihkan`, 'success');
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

// One form, two groups. The other group's sections are hidden AND disabled,
// so they are neither validated nor read. A heavy unit therefore never reaches
// the John Deere component selects — whose "isGood(x) ? 'Good' : 'Breakdown'"
// preselect turned every blank field into 'Breakdown' and wrote it on Save.
function setUnitFormGroup(g) {
    document.getElementById('formUnitGroup').value = g;
    document.querySelectorAll('#unitForm [data-group-only]').forEach(el => {
        const match = el.dataset.groupOnly === g;
        el.style.display = match ? '' : 'none';
        el.querySelectorAll('input, select, textarea').forEach(i => { i.disabled = !match; });
    });
    document.querySelectorAll('#unitForm option[data-temp]').forEach(o => o.remove());
    const legend = document.getElementById('unitFormLegend');
    if (legend) legend.textContent = g === 'heavy' ? 'Kategori & Catatan' : 'Category, License & Notes';
    if (g === 'heavy') populateHeavySuggestionLists();
}

function populateHeavySuggestionLists() {
    const heavy = globalData.filter(isHeavy);
    const fill = (id, items) => {
        const el = document.getElementById(id);
        if (el) el.innerHTML = items.map(v => `<option value="${escapeHtml(v)}"></option>`).join('');
    };
    fill('heavyMachineTypeList', _mergeSuggestions(HEAVY_MACHINE_TYPES, heavy.map(u => u.machineType)));
    fill('heavyWorkToolList', _mergeSuggestions(HEAVY_WORK_TOOLS, heavy.map(u => u.workTool)));
}

// A heavy component with no value shows "— belum diisi —" and is not sent,
// instead of being silently turned into 'Breakdown' (or written as '').
function setHealthSelect(el, v) {
    if (isGood(v)) { el.value = 'Good'; return; }
    if (!sameStoredValue(v, '')) { el.value = 'Breakdown'; return; }
    el.insertAdjacentHTML('afterbegin', '<option value="" data-temp>— belum diisi —</option>');
    el.value = '';
}

function showAddForm() {
    if (!requireEdit('editUnits')) return;
    const g = effectiveEditGroup();
    document.getElementById('modalTitle').textContent = g === 'heavy' ? `Tambah ${UNIT_GROUPS.heavy.shortLabel}` : 'Add Unit';
    document.getElementById('editUnitId').value = '';
    document.getElementById('unitForm').reset();
    setUnitFormGroup(g);
    renderUserCategoryOptions();
    populateImplementUnitList();
    document.getElementById('unitModal').classList.add('open');
}

function editUnit(id) {
    if (!requireEdit('editUnits')) return;
    const unit = globalData.find(d => d.id === id);
    if (!unit) return;

    const g = unitGroupOf(unit);
    setUnitFormGroup(g);
    if (g === 'heavy') { editHeavyUnit(unit); return; }

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

function editHeavyUnit(unit) {
    const set = (id, v) => { document.getElementById(id).value = v == null ? '' : v; };
    document.getElementById('modalTitle').textContent = `Edit ${UNIT_GROUPS.heavy.shortLabel}`;
    set('editUnitId', unit.id);
    set('formName', unit.name); set('formModel', unit.model); set('formSN', unit.sn);
    set('formMachineType', unit.machineType); set('formAssetCode', unit.assetCode); set('formWorkTool', unit.workTool);
    set('formSite', unit.site); set('formYearReceived', unit.yearReceived);
    document.getElementById('formStatus').value = isGood(unit.status) ? 'Good' : 'Breakdown';
    HEAVY_FORM_SELECTS.forEach(([key, elId]) => setHealthSelect(document.getElementById(elId), unit[key]));
    renderUserCategoryOptions();
    set('formUserCategory', unit.userCategory);
    set('formRemarks', unit.remarks);
    const bdBox = document.getElementById('breakdownReasonDisplay');
    const bdInfo = document.getElementById('breakdownReasonInfo');
    const show = !isGood(unit.status) && unit.breakdownReason;
    bdInfo.textContent = show ? unit.breakdownReason : '';
    bdBox.style.display = show ? '' : 'none';
    document.getElementById('unitModal').classList.add('open');
}
const HEAVY_FORM_SELECTS = [['cameraAi', 'formCameraAi'], ['telematicBox', 'formTelematicBox'],
                            ['switchLimiter', 'formSwitchLimiter'], ['rotaryLamp', 'formRotaryLamp']];

// Checked on the form, not inside updateUnit, so the message appears once and
// a bulk CSV import is not stopped row by row.
//
// addUnits has always rejected a duplicate serial number; updateUnit never
// looked at it, so the edit form could quietly create a second unit with the
// same SN. That matters more than it sounds: SN is the key a CSV import
// matches on, so a duplicate makes every later import ambiguous about which
// unit it is updating. saveDevice already does this correctly.
function checkUnitFields(id, fields) {
    const sn = (fields.sn || '').trim();
    if (sn) {
        const clash = globalData.find(u => u.id !== id && (u.sn || '').toLowerCase() === sn.toLowerCase());
        if (clash) {
            // Serial numbers are unique across BOTH groups. Name the group only
            // when the clash is in the other one, so the tractor message is
            // unchanged.
            const self = id ? globalData.find(u => u.id === id) : null;
            const mine = self ? unitGroupOf(self) : normalizeGroupKey(fields.unitGroup) || 'tractor';
            const where = unitGroupOf(clash) !== mine ? ` (${groupDef(unitGroupOf(clash)).shortLabel})` : '';
            showToast(`SN "${sn}" sudah dipakai unit "${clash.name || '-'}"${where}`, 'warning');
            return false;
        }
    }
    // A duplicate asset code is unusual but not impossible (a re-painted hull),
    // so it asks rather than refuses.
    const code = (fields.assetCode || '').trim();
    if (code) {
        const dup = globalData.find(u => u.id !== id && isHeavy(u) && (u.assetCode || '').trim().toLowerCase() === code.toLowerCase());
        if (dup && !confirm(`Nomor lambung "${code}" sudah dipakai "${dup.name || '-'}". Tetap simpan?`)) return false;
    }
    // An end date before its start date leaves the unit reading "expired" while
    // its licence has not begun — and applyExpiredLicenseDowngrades acts on it.
    const ranges = [
        ['GPS', fields.gpsLicenseStartDate, fields.gpsLicenseEndDate],
        ['Display', fields.displayLicenseStartDate, fields.displayLicenseEndDate]
    ];
    for (const [label, from, to] of ranges) {
        if (from && to && to < from) {
            showToast(`Tanggal habis lisensi ${label} lebih awal dari tanggal mulainya`, 'warning');
            return false;
        }
    }
    return true;
}

function heavyFormFields() {
    const val = id => document.getElementById(id).value.trim();
    const f = {
        name: val('formName'), model: val('formModel'), sn: val('formSN'),
        machineType: val('formMachineType'), assetCode: val('formAssetCode'), workTool: val('formWorkTool'),
        site: val('formSite'), yearReceived: val('formYearReceived'),
        status: document.getElementById('formStatus').value,
        userCategory: document.getElementById('formUserCategory').value,
        remarks: val('formRemarks')
    };
    // A component left on "— belum diisi —" is simply not sent.
    HEAVY_FORM_SELECTS.forEach(([key, elId]) => {
        const v = document.getElementById(elId).value;
        if (v !== '') f[key] = v;
    });
    return f;
}

function saveUnit(event) {
    event.preventDefault();
    if (!requireEdit('editUnits')) return;

    const id = document.getElementById('editUnitId').value;
    // The group of an existing unit comes from the unit — it cannot change.
    const existing = id ? globalData.find(d => d.id === id) : null;
    const g = existing ? unitGroupOf(existing)
                       : (document.getElementById('formUnitGroup').value === 'heavy' ? 'heavy' : 'tractor');
    const fields = g === 'heavy' ? heavyFormFields() : {
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
    // Stamped here — BEFORE the breakdown-reason stash below. A new unit saved
    // as Breakdown goes through that stash and is committed later from it, so
    // stamping afterwards would be lost and the unit would land as a tractor.
    if (!id) fields.unitGroup = g;

    if (!checkUnitFields(id, fields)) return;

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
    if (!requireEdit('editUnits')) return;
    if (id) {
        if (updateUnit(id, fields)) showToast(`Unit "${fields.name}" updated`, 'success');
        else showToast(`Unit "${fields.name}" tidak disimpan — tidak ada perubahan yang boleh ditulis`, 'warning');
    } else {
        const firstHeavy = fields.unitGroup === 'heavy' && !hasHeavyUnits();
        const newUnit = { id: generateId(), ...fields, downtimeHistory: [], breakdownStartedAt: null };
        const { added, skippedDetails } = addUnits([newUnit]);
        const reason = skippedDetails && skippedDetails[0] && skippedDetails[0].reason;
        if (added > 0) {
            showToast(`Unit "${fields.name}" added`, 'success');
            if (firstHeavy) showToast(`Alat berat tampil di Dashboard lewat pilihan ${UNIT_GROUPS.heavy.shortLabel} / Semua`, 'info');
        } else if (!reason || reason === 'Duplicate serial number') {
            showToast(`Duplicate serial number "${fields.sn}" — unit not added`, 'warning');
        } else {
            showToast(`${reason} — unit tidak ditambahkan`, 'warning');
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
    const end = addOneYearISO(startEl.value);
    if (!end) return;
    endEl.value = end;
}

// Compute expiry status for a single end-date string.
// Returns one of: { kind: 'none'|'expired'|'soon'|'ok', label, daysLeft }
function getExpiryStatus(endDate) {
    if (!endDate) return { kind: 'none', label: '—' };
    const end = parseLocalDate(endDate);
    if (!end) return { kind: 'none', label: '—' };
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
    // Already written down to the fallback tier by applyExpiredLicenseDowngrades:
    // rawType and end carry no trace of that any more, so the archived expiry is
    // what says it happened. Requiring an empty end date keeps a unit that was
    // simply issued a fresh fallback licence from being mislabelled.
    const archived = kind === 'display' ? unit.displayLicenseExpiredAt : unit.gpsLicenseExpiredAt;
    if (!downgraded && rawType === fallback && archived && !end) {
        return { type: fallback, rawType, premium, fallback, downgraded: true, status, end: archived };
    }
    return { type: downgraded ? fallback : rawType, rawType, premium, fallback, downgraded, status, end };
}

// Once a premium licence's expiry has passed the receiver really is running on
// the lower tier, so write that into the data instead of only painting it:
// SF-RTK → SF-1, G5 Advance → G5 Basic, clearing the expiry date because the
// fallback tier has none. The premium tier is not lost — it stays in the
// licence-stock ledger, and recording a new Distribusi restores it with fresh
// dates. Idempotent: once a unit reads SF-1 it no longer matches.
let _downgradeScanQueued = false;
function scheduleExpiredLicenseDowngrades() {
    if (_downgradeScanQueued) return;
    _downgradeScanQueued = true;
    // Deferred so it runs outside applyCloudUnitsSnapshot's suppressCloudWrites
    // window — otherwise the change would never reach Firestore.
    setTimeout(() => { _downgradeScanQueued = false; applyExpiredLicenseDowngrades(); }, 0);
}

function applyExpiredLicenseDowngrades() {
    // Viewers must not write; their session would just fail against the rules.
    if (!hasAccess('editUnits', 'edit')) return 0;
    let n = 0;
    globalData.slice().forEach(u => {
        if (isHeavy(u)) return;   // SF/G5 licences are John Deere only
        const fields = {};
        if (u.gpsLicense === 'SF-RTK' &&
            getExpiryStatus(getLicenseEndDate(u, 'gps')).kind === 'expired') {
            fields.gpsLicense = 'SF-1';
            // Keep the expiry that caused the downgrade. Blanking it outright
            // destroyed the only record of when the premium tier ran out, and
            // the only way to tell a unit that was downgraded from one that
            // never held a premium licence at all.
            fields.gpsLicenseExpiredAt = getLicenseEndDate(u, 'gps');
            fields.gpsLicenseEndDate = '';
            // getLicenseEndDate falls back to the legacy field, so clear it too.
            if (u.licenseEndDate) fields.licenseEndDate = '';
        }
        if (u.licenseDisplay === 'G5 Advance' &&
            getExpiryStatus(getLicenseEndDate(u, 'display')).kind === 'expired') {
            fields.licenseDisplay = 'G5 Basic';
            fields.displayLicenseExpiredAt = getLicenseEndDate(u, 'display');
            fields.displayLicenseEndDate = '';
        }
        if (Object.keys(fields).length && updateUnit(u.id, fields)) n++;
    });
    if (n > 0) {
        showToast(`${n} unit turun otomatis ke tier fallback (lisensi sudah habis)`, 'info');
        if (currentView === 'editUnits') renderEditTable();
        else if (currentView === 'dashboard') updateDashboard(filteredData);
    }
    return n;
}

// Render the license-type badge for a table cell, showing the effective tier
// (with a small marker + tooltip when auto-downgraded).
function licenseTypeBadge(unit, kind) {
    const eff = effectiveLicense(unit, kind);
    if (!eff.type) return '<span style="color:var(--text-light);font-size:11px">—</span>';
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

// Render an expiry badge for either 'gps' or 'display' license.
function licenseBadgeFor(unit, kind) {
    const end = getLicenseEndDate(unit, kind);
    const s = getExpiryStatus(end);
    if (s.kind === 'none') return '<span style="color:var(--text-light);font-size:11px">—</span>';
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
        showToast('Penyimpanan penuh. Implement tidak tersimpan.', 'error');
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
        tbody.innerHTML = `<tr><td colspan="11" style="text-align:center;padding:24px;color:var(--text-secondary)">${
            query ? 'No implements match your search'
                  : 'No implements yet. Click <strong>Add Implement</strong> to get started.'
        }</td></tr>`;
        return;
    }

    tbody.innerHTML = rows.map((d, i) => {
        const coaList = Array.isArray(d.chartOfAccounts) ? d.chartOfAccounts.filter(Boolean) : [];
        let coaCell;
        if (!coaList.length) {
            coaCell = '<span style="color:var(--text-light);font-size:11px">—</span>';
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
            <td data-label="Profile Name">${escapeHtml(d.profileName)}</td>
            <td data-label="Brand">${escapeHtml(d.brand || '')}</td>
            <td data-label="Equipment Type">${escapeHtml(d.equipmentType)}</td>
            <td data-label="Code">${escapeHtml(d.code || '')}</td>
            <td data-label="Working Width">${escapeHtml(d.workingWidth)}</td>
            <td data-label="Operation">${escapeHtml(d.operation)}</td>
            <td data-label="Connecting Type">${escapeHtml(d.connectingType)}</td>
            <td data-label="Chart of Account" style="max-width:240px;white-space:nowrap">${coaCell}</td>
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
    if (!requireEdit('implements')) return;
    document.getElementById('implementModalTitle').textContent = 'Add Implement';
    document.getElementById('editImplementId').value = '';
    document.getElementById('implementForm').reset();
    renderChartOfAccountsInputs(['']);
    document.getElementById('implementModal').classList.add('open');
}

function editImplement(id) {
    if (!requireEdit('implements')) return;
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
    if (!requireEdit('implements')) return;

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
        showToast(`Implement "${newImp.profileName}" ditambahkan`, 'success');
    }

    closeImplementModal();
    renderImplementsTable();
}

function closeImplementModal() {
    document.getElementById('implementModal').classList.remove('open');
}

// ---- Delete ----
function deleteImplement(id) {
    if (!requireEdit('implements')) return;
    const imp = globalImplements.find(d => d.id === id);
    if (!imp) return;
    if (!confirm(`Hapus implement "${imp.profileName}"?`)) return;

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
    if (!requireEdit('implements')) return;
    const count = selectedImplementIds.size;
    if (count === 0) return;
    if (!confirm(`Hapus ${count} implement terpilih?`)) return;

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
    if (!canCsv('export')) return;
    if (globalImplements.length === 0) { showToast('Tidak ada implement untuk diekspor', 'warning'); return; }
    const headers = ['No', ...IMPLEMENT_FIELDS.map(f => f.label), 'Chart of Account'];
    const rows = globalImplements.map((d, i) => [
        i + 1,
        ...IMPLEMENT_FIELDS.map(f => d[f.key] || ''),
        (Array.isArray(d.chartOfAccounts) ? d.chartOfAccounts.filter(Boolean) : []).join('; ')
    ]);
    const csv = [headers, ...rows].map(row =>
        row.map(csvCell).join(',')).join('\n');
    const blob = new Blob(['﻿' + csv], { type: 'text/csv;charset=utf-8;' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = `implements_${toISODate()}.csv`;
    a.click();
    URL.revokeObjectURL(url);
    showToast(`${globalImplements.length} implement diekspor ke CSV`, 'success');
}

function downloadImplementTemplate() {
    if (!canCsv('export')) return;
    const headers = ['No', ...IMPLEMENT_FIELDS.map(f => f.label), 'Chart of Account'];
    // One example row (No is ignored on import).
    const sample = ['1', 'JNR Leopard E 10.0', 'John Deere', 'Scooping', 'IMP-001', '0 m', '1.2 m', '0.8 m',
        'Tillage', '3', 'Center', 'Manual', 'Drawbar', 'iGrade',
        '159361_Scooping; 159201_Offset Harrow'];
    const csv = [headers, sample].map(row =>
        row.map(csvCell).join(',')).join('\n');
    const blob = new Blob(['﻿' + csv], { type: 'text/csv;charset=utf-8;' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = 'template_implements.csv';
    a.click();
    URL.revokeObjectURL(url);
}

function handleImplementCSVImport(file) {
    if (!canCsv('full')) return;
    if (!requireEdit('implements')) return;
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
                    // A rejected import used to survive only on the importer's
                    // device: the rows rendered as normal implements, existed
                    // nowhere else, and vanished on the next snapshot with no
                    // explanation. Roll the rows back out and say so instead.
                    const addedIds = new Set(added.map(o => o.id));
                    window.cloud.saveImplements(added).catch(err => {
                        globalImplements = globalImplements.filter(o => !addedIds.has(o.id));
                        saveImplements();
                        renderImplementsTable();
                        cloudWriteFailed(err, {
                            what: `impor ${added.length} implement`,
                            label: '[Implement] Import CSV',
                            resync: resyncImplements
                        });
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
// opts lets a caller ask for a tighter budget than the damage-photo defaults —
// a work log holds several photos in one document, so each gets less room.
function compressImageToDataURL(file, opts = {}) {
    const maxDim = opts.maxDim || DAMAGE_PHOTO_MAX_DIM;
    const maxBytes = opts.maxBytes || DAMAGE_PHOTO_MAX_BYTES;
    const startQuality = opts.quality || DAMAGE_PHOTO_QUALITY;
    return new Promise((resolve, reject) => {
        const reader = new FileReader();
        reader.onerror = () => reject(new Error('Gagal membaca file'));
        reader.onload = () => {
            const img = new Image();
            img.onerror = () => reject(new Error('File bukan gambar yang valid'));
            img.onload = () => {
                let { width, height } = img;
                const max = maxDim;
                if (width > height && width > max) { height = Math.round(height * max / width); width = max; }
                else if (height > max) { width = Math.round(width * max / height); height = max; }
                const canvas = document.createElement('canvas');
                canvas.width = width; canvas.height = height;
                canvas.getContext('2d').drawImage(img, 0, 0, width, height);
                let q = startQuality;
                let out = canvas.toDataURL('image/jpeg', q);
                while (out.length > maxBytes && q > 0.3) {
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
        _dmgPhotoDirty = true;
        setDamagePhotoPreview();
    } catch (err) {
        showToast(err.message || 'Gagal memproses foto', 'error');
    }
}

function setDamagePhotoPreview() {
    const wrap = document.getElementById('dmgPhotoPreviewWrap');
    const img = document.getElementById('dmgPhotoPreview');
    if (!wrap || !img) return;
    const loading = document.getElementById('dmgPhotoLoading');
    if (loading) loading.style.display = (_dmgPhotoLoading && !_dmgPhotoData) ? '' : 'none';
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
    _dmgPhotoDirty = true;
    const input = document.getElementById('dmgPhotoInput');
    if (input) input.value = '';
    setDamagePhotoPreview();
}

// ============================================================
// DAMAGE PHOTOS — stored apart from the record, fetched on demand
// ------------------------------------------------------------
// Same arrangement as work-log photos, for the same reason and with the same
// three rules. damageRecords is subscribed whole with no limit(), so an inline
// data URL was pulled down by every device on every app open — roughly 150 KB
// per photo, never expiring. It also rode along into localStorage through
// saveDamages(), where a few dozen photos exhausted the ~5 MB origin quota and
// took unit saves and the automatic backup ring down with it.
//
// The record keeps only hasPhoto. The image lives in damagePhotos/{recordId}
// and is read one document at a time, when somebody opens it. This collection
// must never be subscribed.
// ============================================================

const _dmgPhotoCache = new Map();   // damage record id -> data URL

function damageHasPhoto(rec) {
    if (!rec) return false;
    // Records written before the split still carry the image inline. An empty
    // string means the inline copy was cleared on migration, so fall through to
    // hasPhoto rather than reporting none.
    if (rec.photo) return true;
    return !!rec.hasPhoto;
}

async function loadDamagePhoto(id) {
    const rec = globalDamages.find(d => d.id === id);
    if (rec && rec.photo) return rec.photo;
    if (_dmgPhotoCache.has(id)) return _dmgPhotoCache.get(id);
    if (!window.cloud || !window.cloud.getDamagePhoto) return '';
    const photo = await window.cloud.getDamagePhoto(id);
    _dmgPhotoCache.set(id, photo);
    return photo;
}

async function openDamagePhoto(id, btn) {
    const original = btn ? btn.innerHTML : '';
    if (btn) { btn.disabled = true; btn.innerHTML = '<i class="fas fa-spinner fa-spin"></i>'; }
    try {
        const photo = await loadDamagePhoto(id);
        if (!photo) { showToast('Foto tidak ditemukan', 'warning'); return; }
        openPhotoLightbox(photo);
    } catch (err) {
        console.error('[damage] photo load failed:', err);
        showToast(navigator.onLine
            ? 'Gagal memuat foto kerusakan'
            : 'Foto perlu sinyal untuk dimuat', 'error');
    } finally {
        if (btn) { btn.disabled = false; btn.innerHTML = original; }
    }
}

// One-shot move of inline photos into damagePhotos, mirroring the work-log
// migration that has already run in production.
//
// Order matters and is deliberate: the photo document is written FIRST and the
// inline copy cleared only after that write is confirmed. Interrupted halfway,
// the photo exists in two places — never in none. The flag is only set once the
// whole pass succeeds, so a failure simply retries on the next snapshot instead
// of leaving records stranded.
let _dmgPhotoMigrationRunning = false;
function migrateDamagePhotosIfNeeded() {
    if (_dmgPhotoMigrationRunning) return;
    if (!isOwner || !isOwner()) return;
    if (localStorage.getItem(DAMAGE_PHOTOS_SPLIT_KEY) === '1') return;
    if (!window.cloud || !window.cloud.saveDamagePhoto) return;
    if (!Array.isArray(globalDamages) || globalDamages.length === 0) return;

    const pending = globalDamages.filter(d => d.photo);
    if (pending.length === 0) {
        localStorage.setItem(DAMAGE_PHOTOS_SPLIT_KEY, '1');
        return;
    }
    if (!navigator.onLine) return;   // retried on the next snapshot

    _dmgPhotoMigrationRunning = true;
    console.log(`[dmg-photos] moving photos out of ${pending.length} damage records...`);
    (async () => {
        let moved = 0;
        for (const d of pending) {
            const photo = d.photo;
            await window.cloud.saveDamagePhoto(d.id, photo);
            _dmgPhotoCache.set(d.id, photo);
            await window.cloud.saveDamage({ ...d, photo: '', hasPhoto: true });
            moved++;
        }
        return moved;
    })().then(moved => {
        localStorage.setItem(DAMAGE_PHOTOS_SPLIT_KEY, '1');
        try {
            logEvent({
                action: 'migrate',
                unitName: '-',
                field: 'foto kerusakan',
                after: `${moved} catatan dipindah ke koleksi damagePhotos`
            });
        } catch (e) {}
        console.log(`[dmg-photos] done — ${moved} damage records migrated`);
    }).catch(err => {
        console.error('[dmg-photos] migration failed:', err);
        if (err && err.code === 'permission-denied') showDamageRulesBanner();
    }).finally(() => {
        _dmgPhotoMigrationRunning = false;
    });
}

// Renders the cell for a damage record's photo: a button carrying the fetch,
// never an <img> holding the image itself.
function damagePhotoButton(rec) {
    if (!damageHasPhoto(rec)) return '<span style="color:var(--text-light);font-size:11px">—</span>';
    // Carries a word, not just the icon: a one-glyph button is a poor tap
    // target on a phone, and it disappears entirely if the icon font does not
    // load — which is exactly when a field device is on a bad connection.
    return `<button type="button" class="wl-photo-btn" title="Lihat foto kerusakan"
            aria-label="Lihat foto kerusakan"
            onclick="event.stopPropagation();openDamagePhoto('${escapeHtml(rec.id)}', this)"><i class="fas fa-image"></i> Foto</button>`;
}

// ---- Lightbox (view full-size photo) ----
// Takes one photo or a whole set. A work log can hold four, and they are now
// fetched together, so paging through them beats opening each separately.
let _lightboxPhotos = [];
let _lightboxIndex = 0;

function openPhotoLightbox(src, index) {
    const list = Array.isArray(src) ? src.filter(Boolean) : (src ? [src] : []);
    if (!list.length) return;
    const box = document.getElementById('photoLightbox');
    if (!box) return;
    _lightboxPhotos = list;
    _lightboxIndex = Math.min(Math.max(Number(index) || 0, 0), list.length - 1);
    renderPhotoLightbox();
    box.classList.add('open');
}

function renderPhotoLightbox() {
    const img = document.getElementById('photoLightboxImg');
    if (img) img.src = _lightboxPhotos[_lightboxIndex] || '';
    const many = _lightboxPhotos.length > 1;
    ['photoLightboxPrev', 'photoLightboxNext'].forEach(id => {
        const el = document.getElementById(id);
        if (el) el.style.display = many ? '' : 'none';
    });
    const count = document.getElementById('photoLightboxCount');
    if (count) {
        count.style.display = many ? '' : 'none';
        count.textContent = `${_lightboxIndex + 1} / ${_lightboxPhotos.length}`;
    }
}

// Wraps around, so holding one arrow never dead-ends.
function stepPhotoLightbox(delta) {
    if (_lightboxPhotos.length < 2) return;
    const n = _lightboxPhotos.length;
    _lightboxIndex = (_lightboxIndex + delta + n) % n;
    renderPhotoLightbox();
}

function closePhotoLightbox() {
    const box = document.getElementById('photoLightbox');
    if (box) box.classList.remove('open');
    _lightboxPhotos = [];
}

document.addEventListener('keydown', e => {
    const box = document.getElementById('photoLightbox');
    if (!box || !box.classList.contains('open')) return;
    if (e.key === 'Escape')     { closePhotoLightbox(); }
    if (e.key === 'ArrowLeft')  { stepPhotoLightbox(-1); }
    if (e.key === 'ArrowRight') { stepPhotoLightbox(1); }
});

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
    const hits = globalData.filter(u => (isHeavy(u)
        ? `${u.name} ${u.sn} ${u.model} ${u.site} ${u.machineType || ''} ${u.assetCode || ''}`
        : `${u.name} ${u.sn} ${u.model} ${u.site}`).toLowerCase().includes(q)).slice(0, 8);
    if (!hits.length) {
        box.innerHTML = '<div class="global-search__empty">Tidak ada unit yang cocok</div>';
        box.style.display = '';
        return;
    }
    box.innerHTML = hits.map(u => `
        <div class="global-search__item" onclick="openUnitFromSearch('${escapeHtml(u.id)}')">
            <span class="global-search__name">${escapeHtml(u.name || '(tanpa nama)')}</span>
            <span class="global-search__meta"><span class="mono">${escapeHtml(u.sn || '')}</span>${u.site ? ' · ' + escapeHtml(u.site) : ''}${isHeavy(u) ? ' · ' + escapeHtml(UNIT_GROUPS.heavy.shortLabel) : ''}</span>
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
    const dash = '<span style="color:var(--text-light)">—</span>';
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

    const heavyUnit = isHeavy(u);
    const snCell = u.sn ? `<span style="font-family:var(--font-mono);font-size:12px">${escapeHtml(u.sn)}</span>` : dash;
    const catCell = u.userCategory ? `<span class="badge badge-cat" style="font-size:10px">${escapeHtml(u.userCategory)}</span>` : dash;
    const identity = (heavyUnit ? [
        ['Kelompok', `<span class="badge badge-cat" style="font-size:10px">${escapeHtml(UNIT_GROUPS.heavy.label)}</span>`],
        ['Model', val(u.model)],
        ['Serial Number', snCell],
        ['Jenis Alat', val(u.machineType)],
        ['Nomor Lambung', val(u.assetCode)],
        ['Alat Kerja', val(u.workTool)],
        ['Site', val(u.site)],
        ['Tahun Penerimaan', val(u.yearReceived)],
        ['User Category', catCell]
    ] : [
        ['Model', val(u.model)],
        ['Serial Number', u.sn ? `<span style="font-family:var(--font-mono);font-size:12px">${escapeHtml(u.sn)}</span>` : dash],
        ['Implement', impVal],
        ['Site', val(u.site)],
        ['Tahun Penerimaan', val(u.yearReceived)],
        ['User Category', u.userCategory ? `<span class="badge badge-cat" style="font-size:10px">${escapeHtml(u.userCategory)}</span>` : dash]
    ]).map(([l, v]) => `<div class="profile-row"><span class="profile-row__label">${l}</span><span>${v}</span></div>`).join('');

    const statusBadge = isGood(u.status)
        ? '<span class="badge badge-good"><i class="fas fa-check"></i> Good</span>'
        : `<span class="badge badge-breakdown"><i class="fas fa-xmark"></i> ${escapeHtml(u.status || 'Breakdown')}</span>`;
    // The unit's own group's components: an excavator used to show four red
    // John Deere chips it never had.
    const compRow = groupDef(unitGroupOf(u)).components.map(c => {
        const label = c.label;
        const good = isGood(u[c.key]);
        return `<div class="profile-comp ${good ? 'ok' : 'bad'}"><i class="fas fa-${good ? 'circle-check' : 'circle-xmark'}"></i> ${label}</div>`;
    }).join('');
    const bdReason = (!isGood(u.status) && u.breakdownReason)
        ? `<div class="profile-note"><i class="fas fa-triangle-exclamation"></i> ${escapeHtml(u.breakdownReason)}</div>` : '';
    const stray = strayGroupFields(u);
    const strayNote = stray.length
        ? `<div class="profile-note"><i class="fas fa-triangle-exclamation"></i> Unit ini membawa ${stray.length} field milik kelompok lain — lihat Kotak Keputusan → Periksa Data.</div>` : '';

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
            ${damageHasPhoto(r) ? damagePhotoButton(r) : ''}
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

    // Who has worked on this machine — newest first, capped like the other lists.
    const work = workLogsForUnit(u.id, u.sn);
    const workHtml = work.length ? work.slice(0, 8).map(w => `
        <div class="profile-item">
            <span class="profile-item__date">${escapeHtml(w.date || '')}</span>
            <span style="font-size:12px"><strong>${escapeHtml(memberNameOf(w))}</strong></span>
            <span style="font-size:12px;color:var(--text-light)">${escapeHtml(formatMinutes(workLogMinutes(w)))}</span>
            ${w.task ? `<span class="profile-item__text" title="${escapeHtml(w.task)}">${escapeHtml(w.task.slice(0, 40))}</span>` : ''}
        </div>`).join('')
        : '<div class="profile-empty">Belum ada laporan kerja untuk unit ini.</div>';

    // Devices from the warehouse currently fitted to this machine.
    const fitted = devicesForUnit(u.id);
    const fittedHtml = fitted.length ? fitted.map(d => `
        <div class="profile-item">
            <span class="badge badge-cat" style="font-size:10px">${escapeHtml(d.type || 'Perangkat')}</span>
            <span style="font-family:monospace;font-size:12px">${escapeHtml(d.sn || '')}</span>
            ${d.note ? `<span class="profile-item__text" title="${escapeHtml(d.note)}">${escapeHtml(d.note.slice(0, 30))}</span>` : ''}
        </div>`).join('')
        : '<div class="profile-empty">Tidak ada perangkat gudang terdaftar di unit ini.</div>';

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
                ${bdReason}${strayNote}
                ${heavyUnit ? '' : `<div class="profile-section__title" style="margin-top:16px">Lisensi</div>
                ${licenses}`}
            </div>
            <div class="profile-section">
                <div class="profile-section__title">Riwayat Kerusakan (${dmg.length})</div>
                <div class="profile-list">${dmgHtml}</div>
            </div>
            ${heavyUnit ? '' : `<div class="profile-section">
                <div class="profile-section__title">Distribusi Lisensi (${dist.length})</div>
                <div class="profile-list">${distHtml}</div>
            </div>`}
            <div class="profile-section">
                <div class="profile-section__title">Lampiran</div>
                <div class="profile-attach">${renderAttachCell(u)}</div>
            </div>
            <div class="profile-section">
                <div class="profile-section__title">Downtime</div>
                <div class="profile-list">${downHtml}</div>
            </div>
            <div class="profile-section">
                <div class="profile-section__title">Pekerjaan Tim (${work.length})</div>
                <div class="profile-list">${workHtml}</div>
            </div>
            <div class="profile-section">
                <div class="profile-section__title">Perangkat Terpasang (${fitted.length})</div>
                <div class="profile-list">${fittedHtml}</div>
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
        showToast('Penyimpanan penuh. Catatan kerusakan tidak tersimpan.', 'error');
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
    const lc = val.toLowerCase();

    // 1. Full "Nama — SN" label, as offered by the datalist.
    let u = globalData.find(x => damageUnitLabel(x).toLowerCase() === lc);
    // 2. Serial number after the "—" separator (partially typed label).
    if (!u && val.includes('—')) {
        const sn = val.split('—').pop().trim().toLowerCase();
        if (sn) u = globalData.find(x => (x.sn || '').toLowerCase() === sn);
    }
    // 3/4. Plain nickname or plain serial number — the field's own hint says
    // "ketik nama atau SN", so accept either on its own.
    if (!u) u = globalData.find(x => (x.name || '').toLowerCase() === lc);
    if (!u) u = globalData.find(x => (x.sn || '').toLowerCase() === lc);
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
        tbody.innerHTML = `<tr><td colspan="13" style="text-align:center;padding:24px;color:var(--text-secondary)">${
            hasFilter ? 'Tidak ada kerusakan yang cocok dengan filter'
                      : 'Belum ada catatan kerusakan. Klik <strong>Tambah Kerusakan</strong> untuk mulai.'
        }</td></tr>`;
        return;
    }

    tbody.innerHTML = rows.map((d, i) => {
        const comp = d.component
            ? `<span class="badge badge-cat" style="font-size:10px">${escapeHtml(d.component)}</span>`
            : '<span style="color:var(--text-light);font-size:11px">—</span>';
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
            <td data-label="Deskripsi" style="max-width:240px;font-size:12px;color:var(--text-secondary)" title="${escapeHtml(desc)}">${escapeHtml(descShort) || '<span style="color:var(--text-light)">—</span>'}</td>
            <td data-label="Foto">${damageHasPhoto(d)
                ? damagePhotoButton(d)
                : '<span style="color:var(--text-light);font-size:11px">—</span>'}</td>
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

// ---- Component sub-field ----
// The component list applies to every damage type (mechanical, software,
// device precision), so the field stays visible; it is always optional.
function onDamageTypeChange() {
    const group = document.getElementById('dmgComponentGroup');
    if (group) group.style.display = '';
}

// ---- Modal: Add / Edit ----
function showAddDamageForm() {
    if (!requireEdit('damage')) return;
    document.getElementById('damageModalTitle').textContent = 'Tambah Kerusakan';
    document.getElementById('editDamageId').value = '';
    document.getElementById('damageForm').reset();
    document.getElementById('dmgDate').value = toISODate();
    populateDamageUnitSelect();
    renderDamageComponentOptions();
    onDamageTypeChange();
    _dmgPhotoData = '';
    _dmgPhotoDirty = false;
    _dmgPhotoLoading = false;
    setDamagePhotoPreview();
    const bdGroup = document.getElementById('dmgSetBreakdownGroup');
    if (bdGroup) bdGroup.style.display = '';
    document.getElementById('damageModal').classList.add('open');
}

function editDamage(id) {
    if (!requireEdit('damage')) return;
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
    renderDamageComponentOptions();
    // Keep a component that was deleted from the list still selectable here.
    const compSel = document.getElementById('dmgComponent');
    if (rec.component && !Array.from(compSel.options).some(o => o.value === rec.component)) {
        compSel.insertAdjacentHTML('beforeend', `<option value="${escapeHtml(rec.component)}">${escapeHtml(rec.component)}</option>`);
    }
    compSel.value = rec.component || '';
    document.getElementById('dmgDescription').value = rec.description || '';
    onDamageTypeChange();
    _dmgPhotoData = rec.photo || '';
    _dmgPhotoDirty = false;
    _dmgPhotoLoading = !_dmgPhotoData && damageHasPhoto(rec);
    setDamagePhotoPreview();
    // Status linkage only applies when recording a NEW damage, not when editing.
    const bdGroup = document.getElementById('dmgSetBreakdownGroup');
    if (bdGroup) bdGroup.style.display = 'none';
    document.getElementById('damageModal').classList.add('open');

    // The photo lives in its own document now, so it arrives after the form
    // does. Bail out if the user has moved on to another record or has already
    // changed the photo themselves — their choice must not be overwritten by a
    // fetch they started before making it.
    if (_dmgPhotoLoading) {
        loadDamagePhoto(id).then(photo => {
            if (document.getElementById('editDamageId').value !== id) return;
            if (_dmgPhotoDirty) return;
            _dmgPhotoData = photo;
            _dmgPhotoLoading = false;
            setDamagePhotoPreview();
        }).catch(err => {
            console.error('[damage] photo load failed:', err);
            _dmgPhotoLoading = false;
            setDamagePhotoPreview();
            showToast('Foto lama gagal dimuat — foto lama tetap tersimpan', 'warning');
        });
    }
}

function saveDamage(event) {
    event.preventDefault();
    if (!requireEdit('damage')) return;

    const id = document.getElementById('editDamageId').value;
    const unit = resolveDamageUnit(document.getElementById('dmgUnit').value);
    if (!unit) { showToast('Pilih unit dari daftar (ketik nama atau SN)', 'warning'); return; }

    // A component of the other group is refused in both directions: GPS on an
    // excavator would flip a field it does not own, Camera AI on a tractor
    // would do the same the other way. Editing only the text of a legacy record
    // that is already like that stays allowed.
    const comp = document.getElementById('dmgComponent').value;
    const old = id ? globalDamages.find(d => d.id === id) : null;
    const moved = !old || comp !== (old.component || '') || ((liveUnitFor(old) || {}).id !== unit.id);
    if (comp && moved && componentGroupConflict(comp, unit)) {
        const own = groupDef(unitGroupOf(unit)).shortLabel;
        showToast(`Komponen "${comp}" bukan milik ${own} — pilih komponen ${own}`, 'warning');
        return;
    }

    const type = document.getElementById('dmgType').value;
    const data = {
        date: document.getElementById('dmgDate').value,
        unitId: unit.id,
        unitName: unit.name || '',
        sn: unit.sn || '',
        site: unit.site || '',
        damageType: type,
        component: document.getElementById('dmgComponent').value,
        description: document.getElementById('dmgDescription').value.trim(),
        // Only the flag lives on the record; the image goes to damagePhotos.
        hasPhoto: _dmgPhotoDirty ? !!_dmgPhotoData
                                 : damageHasPhoto(globalDamages.find(d => d.id === id))
    };
    // Untouched photos are left exactly as they are — not re-read, not
    // rewritten, not cleared. Writing photo:'' unconditionally would wipe a
    // legacy inline photo that has not been migrated yet, on a save that only
    // meant to fix a typo.
    if (_dmgPhotoDirty) data.photo = '';
    // Marked only on heavy-equipment records, so tractor records keep exactly
    // today's shape; cleared when a record is moved off a heavy unit, because
    // the merges below would otherwise keep the old value.
    if (isHeavy(unit) || (old && old.unitGroup)) data.unitGroup = isHeavy(unit) ? 'heavy' : '';

    let savedId = id;
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
        savedId = newRec.id;
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

    if (_dmgPhotoDirty && savedId) {
        const recId = savedId;
        const photo = _dmgPhotoData || '';
        _dmgPhotoCache.set(recId, photo);
        // Offline, the service worker can still be serving a firebase-init.js
        // from before this collection existed. Say so rather than failing
        // silently on a save that otherwise looks like it worked.
        if (!window.cloud.saveDamagePhoto) {
            showToast('Muat ulang halaman — versi lama masih aktif, foto belum terkirim', 'warning');
        } else {
            const write = photo
                ? window.cloud.saveDamagePhoto(recId, photo)
                : window.cloud.deleteDamagePhoto(recId);
            write.catch(err => {
                console.error('[damage] photo save failed:', err);
                if (err && err.code === 'permission-denied') showDamageRulesBanner();
                showToast('Catatan tersimpan, tetapi foto gagal dikirim', 'error');
            });
        }
    }

    closeDamageModal();
    renderDamageTable();
}

// Which place on the unit does a damage record drive? This is the contract the
// damage form states: "Device Precision → komponen terkait; tipe lain → status
// unit". A Mekanis/Software failure hits the unit itself even when a component
// is named, so its downtime is tracked and its description is kept as the
// breakdown reason. Returns a unit field name, or null meaning `status`.
// What a damage record drives on its unit: a component field, null (the unit's
// status), or DAMAGE_DRIVES_NOTHING. The last one exists for a legacy record
// that names a John Deere component on a heavy unit — flipping gps on an
// excavator is exactly the corruption this guards against, and flipping its
// status instead would claim a breakdown nobody reported.
// For a tractor the result is today's mapping, verbatim.
const DAMAGE_DRIVES_NOTHING = '__nothing__';
function damageTargetField(damageType, component, unit) {
    if (unit && isHeavy(unit)) {
        if (damageType !== 'Device Precision') return null;
        const h = heavyComponentField(component);
        if (h) return h;
        return componentUnitField(component) ? DAMAGE_DRIVES_NOTHING : null;
    }
    const compField = componentUnitField(component);
    return (damageType === 'Device Precision' && compField) ? compField : null;
}

// Put the unit (or the damaged component) into Breakdown.
// Returns a label describing what was changed, or '' when nothing changed.
function _applyDamageBreakdown(unitId, damageType, component, description) {
    const compField = damageTargetField(damageType, component, globalData.find(u => u.id === unitId));
    if (compField === DAMAGE_DRIVES_NOTHING) return '';
    if (compField) {
        updateUnit(unitId, { [compField]: 'Breakdown' });
        return `Komponen ${component} unit`;
    }
    const reason = `${damageType}${component ? ' / ' + component : ''}: ${description || '-'}`;
    updateUnit(unitId, { status: 'Breakdown', breakdownReason: reason });
    return 'Status unit';
}

// Is there ANOTHER still-open damage record on the same unit that drives the
// same place (same component field, or both the unit's status)? If so,
// resolving this one must not flip that place back to Good.
function hasOtherOpenDamage(rec, unit, compField) {
    if (!unit || compField === DAMAGE_DRIVES_NOTHING) return false;
    return globalDamages.some(d => {
        if (d.id === rec.id || d.resolved) return false;
        const u = liveUnitFor(d);
        if (!u || u.id !== unit.id) return false;
        return damageTargetField(d.damageType, d.component, u) === compField;
    });
}

// Mark a damage record repaired: flag it resolved and restore the unit /
// component this damage put into Breakdown (downtime duration is recorded
// automatically by trackStatusChange inside updateUnit).

function resolveDamage(id) {
    if (!requireEdit('damage')) return;
    const rec = globalDamages.find(d => d.id === id);
    if (!rec || rec.resolved) return;
    if (!confirm(`Tandai kerusakan unit "${rec.unitName}" (${rec.date}) selesai diperbaiki?`)) return;

    rec.resolved = true;
    rec.resolvedAt = toISODate();
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
    let restored = false;
    let blocked = false;
    if (unit) {
        const compField = damageTargetField(rec.damageType, rec.component, unit);
        if (compField === DAMAGE_DRIVES_NOTHING) {
            // Nothing on the unit to restore — see damageTargetField.
        } else if (hasOtherOpenDamage(rec, unit, compField)) {
            blocked = true;
        } else if (compField) {
            if (!isGood(unit[compField])) restored = updateUnit(unit.id, { [compField]: 'Good' });
        } else if (!isGood(unit.status)) {
            restored = updateUnit(unit.id, { status: 'Good' });
        }
    }

    renderDamageTable();
    if (blocked) {
        showToast('Kerusakan ditandai selesai — status unit belum dipulihkan karena masih ada kerusakan lain yang terbuka', 'warning');
    } else if (restored) {
        showToast('Kerusakan ditandai selesai — status unit dipulihkan', 'success');
    } else {
        showToast('Kerusakan ditandai selesai', 'success');
    }
}

function closeDamageModal() {
    document.getElementById('damageModal').classList.remove('open');
}

// ---- Delete ----
function deleteDamage(id) {
    if (!requireEdit('damage')) return;
    const rec = globalDamages.find(d => d.id === id);
    if (!rec) return;
    if (!confirm(`Hapus catatan kerusakan unit "${rec.unitName}" (${rec.date})?`)) return;

    globalDamages = globalDamages.filter(d => d.id !== id);
    saveDamages();
    cloudDeleteDamage(id);
    if (damageHasPhoto(rec) && window.cloud?.deleteDamagePhoto) {
        _dmgPhotoCache.delete(id);
        window.cloud.deleteDamagePhoto(id).catch(err =>
            console.error('[damage] photo delete failed:', err));
    }
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
    if (!requireEdit('damage')) return;
    const count = selectedDamageIds.size;
    if (count === 0) return;
    if (!confirm(`Hapus ${count} catatan kerusakan terpilih?`)) return;

    const idSet = new Set(selectedDamageIds);
    const removed = globalDamages.filter(d => idSet.has(d.id));
    globalDamages = globalDamages.filter(d => !idSet.has(d.id));
    saveDamages();
    removed.forEach(rec => cloudDeleteDamage(rec.id));
    // Leaving these behind would orphan them: nothing else ever reads or
    // removes a photo whose record is gone.
    removed.forEach(rec => {
        if (!damageHasPhoto(rec) || !window.cloud?.deleteDamagePhoto) return;
        _dmgPhotoCache.delete(rec.id);
        window.cloud.deleteDamagePhoto(rec.id).catch(err =>
            console.error('[damage] photo delete failed:', err));
    });
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
    if (!canCsv('export')) return;
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
            d.damageType || '', d.component || '', d.description || '', damageHasPhoto(d) ? 'Ada' : '',
            d.resolved ? `Selesai ${d.resolvedAt || ''}`.trim() : 'Open'
        ];
    });
    const csv = [headers, ...dataRows].map(row =>
        row.map(csvCell).join(',')).join('\n');
    const blob = new Blob(['﻿' + csv], { type: 'text/csv;charset=utf-8;' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = `kerusakan_report_${toISODate()}.csv`;
    a.click();
    URL.revokeObjectURL(url);
    showToast(`Export ${rows.length} catatan kerusakan ke CSV`, 'success');
}

// ---- Cloud ----
function cloudPushDamage(rec) {
    if (suppressCloudWrites || !window.cloud?.isReady || !rec) return;
    window.cloud.saveDamage(rec).catch(err => cloudWriteFailed(err, {
        what: 'kerusakan',
        label: `[Kerusakan] ${rec.unitName || '-'}`,
        unitId: rec.unitId || '',
        resync: resyncDamages
    }));
}

function cloudDeleteDamage(id) {
    if (suppressCloudWrites || !window.cloud?.isReady || !id) return;
    window.cloud.deleteDamage(id).catch(err => cloudWriteFailed(err, {
        what: 'hapus kerusakan',
        label: '[Kerusakan] -',
        resync: resyncDamages
    }));
}

function applyCloudDamagesSnapshot(items) {
    clearRulesBanner('damageRecords');
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

    // Outside the suppress window on purpose: the migration's writes must
    // actually reach Firestore.
    migrateDamagePhotosIfNeeded();
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
        showToast('Penyimpanan penuh. Stok lisensi tidak tersimpan.', 'error');
    }
}

function updateLicenseCount() {
    const el = document.getElementById('licenseCount');
    if (el) el.textContent = `${globalLicenseStock.length} transaksi`;
}

// All distinct license types: defaults plus any already used in the data.
// "Jenis Lisensi" is a free-text field with a datalist, so nothing stops
// someone typing "sf-rtk" instead of picking "SF-RTK". Without folding, that
// becomes a SEPARATE stock bucket: the real type keeps counting the units it
// already handed out (looks over-stocked) while the phantom one goes negative
// and fires a false "stok habis" alert. Fold on this key everywhere.
function licenseTypeKey(raw) {
    return (raw || '').trim().replace(/\s+/g, ' ').toLowerCase();
}

// Preferred spelling for a type: a known default wins, otherwise the spelling
// already stored for that type, otherwise the text as typed (tidied).
function canonicalLicenseType(raw) {
    const key = licenseTypeKey(raw);
    if (!key) return '';
    const known = LICENSE_TYPE_DEFAULTS.find(t => licenseTypeKey(t) === key);
    if (known) return known;
    const seen = globalLicenseStock.find(r => licenseTypeKey(r.licenseType) === key);
    const src = seen ? seen.licenseType : raw;
    return (src || '').trim().replace(/\s+/g, ' ');
}

function allLicenseTypes() {
    const byKey = new Map();
    LICENSE_TYPE_DEFAULTS.forEach(t => byKey.set(licenseTypeKey(t), t));
    globalLicenseStock.forEach(r => {
        const key = licenseTypeKey(r.licenseType);
        if (key && !byKey.has(key)) byKey.set(key, (r.licenseType || '').trim().replace(/\s+/g, ' '));
    });
    return [...byKey.values()].sort((a, b) => a.localeCompare(b));
}

// Per-type stock summary: { type: { in, out, sisa } }.
// Keyed case-insensitively so records already saved with a different casing
// merge into one bucket without needing the stored data to be rewritten.
function computeLicenseSummary() {
    const known = new Map(LICENSE_TYPE_DEFAULTS.map(t => [licenseTypeKey(t), t]));
    const acc = {};   // key -> { label, in, out }
    globalLicenseStock.forEach(r => {
        const key = licenseTypeKey(r.licenseType) || '(tanpa jenis)';
        if (!acc[key]) {
            acc[key] = {
                label: known.get(key) || (r.licenseType || '').trim().replace(/\s+/g, ' ') || '(tanpa jenis)',
                in: 0, out: 0
            };
        }
        const q = Number(r.qty) || 0;
        if (r.txnType === 'OUT') acc[key].out += q;
        else acc[key].in += q;
    });
    const map = {};
    Object.values(acc).forEach(v => {
        map[v.label] = { in: v.in, out: v.out, sisa: v.in - v.out };
    });
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
    // Folded so filtering by "SF-RTK" also catches rows stored as "sf-rtk".
    if (typeVal) rows = rows.filter(r => licenseTypeKey(r.licenseType) === licenseTypeKey(typeVal));
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
        tbody.innerHTML = `<tr><td colspan="12" style="text-align:center;padding:24px;color:var(--text-secondary)">${
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
            <td data-label="Unit">${isOut ? escapeHtml(uName) : '<span style="color:var(--text-light);font-size:11px">—</span>'}</td>
            <td data-label="SN" style="font-family:monospace;font-size:12px">${isOut ? escapeHtml(uSn) : '<span style="color:var(--text-light);font-size:11px">—</span>'}</td>
            <td data-label="Catatan" style="max-width:200px;font-size:12px;color:var(--text-secondary)" title="${escapeHtml(note)}">${escapeHtml(noteShort) || '<span style="color:var(--text-light)">—</span>'}</td>
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
    // SF/G5 licences are John Deere only: heavy equipment is never offered.
    const units = globalData.filter(u => !isHeavy(u)).sort((a, b) => (a.name || '').localeCompare(b.name || ''));
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
    if (!requireEdit('licenseStock')) return;
    document.getElementById('licenseModalTitle').textContent =
        txnType === 'OUT' ? 'Distribusi Lisensi' : 'Tambah Stok Lisensi';
    document.getElementById('editLicenseId').value = '';
    document.getElementById('licenseForm').reset();
    document.getElementById('licTxnType').value = txnType || 'IN';
    document.getElementById('licDate').value = toISODate();
    document.getElementById('licQty').value = '1';
    populateLicenseTypeList();
    populateLicenseUnitList('');
    onLicenseTxnChange();
    document.getElementById('licenseModal').classList.add('open');
}

function editLicenseStock(id) {
    if (!requireEdit('licenseStock')) return;
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
// Folded, so a record saved as "sf-rtk" still links to the unit's GPS licence
// instead of being written off as a non-standard type.
function _licenseKindForType(type) {
    const k = licenseTypeKey(type);
    if (k === 'sf-rtk' || k === 'sf-1') return 'gps';
    if (k === 'g5 advance' || k === 'g5 basic') return 'display';
    return null; // custom type — not a standard unit license
}

// Connect a distribution (OUT) to its target unit: set the unit's license type,
// start date (= distribution date) and expiry (= start + 1 year). This flows
// straight into expiry status, License Alerts and the auto-downgrade. Returns
// true when a unit license was updated.
// Apply an OUT (distribution) record to the unit it was handed to.
// `force` is for the explicit "Sync ke Unit" action, which warns first; the
// normal path refuses to move a unit's licence BACKWARDS, so back-filling an
// old handover can't expire a unit that has since been renewed.
// Returns 'applied' | 'skipped-older' | false.
function applyDistributedLicenseToUnit(rec, force = false) {
    if (!rec || rec.txnType !== 'OUT' || !rec.unitId) return false;
    const kind = _licenseKindForType(rec.licenseType);
    if (!kind) return false;
    const unit = globalData.find(u => u.id === rec.unitId);
    // The write boundary: whatever path reached here, no licence lands on a
    // heavy unit.
    if (!unit || isHeavy(unit)) return false;
    const start = rec.date || toISODate();
    const end = addOneYearISO(start);

    const currentEnd = getLicenseEndDate(unit, kind);
    if (!force && currentEnd && end && currentEnd > end) return 'skipped-older';

    const fields = kind === 'gps'
        ? { gpsLicense: rec.licenseType, gpsLicenseStartDate: start, gpsLicenseEndDate: end }
        : { licenseDisplay: rec.licenseType, displayLicenseStartDate: start, displayLicenseEndDate: end };
    return updateUnit(rec.unitId, fields) ? 'applied' : false;
}

// Backfill: apply EXISTING distributions to their units in one go. For each
// unit + license kind we take the latest-dated OUT record, so a unit ends up
// with its most recent distributed license. Overwrites current unit license
// data (confirmed first). Use after recording distributions that predate the
// auto-link, or after a CSV import of the stock ledger.
function syncDistributionsToUnits() {
    if (!requireEdit('licenseStock')) return;
    const latest = {}; // `${unitId}|${kind}` -> record
    globalLicenseStock.forEach(r => {
        if (r.txnType !== 'OUT' || !r.unitId) return;
        const kind = _licenseKindForType(r.licenseType);
        if (!kind) return;
        if (!globalData.some(u => u.id === r.unitId && !isHeavy(u))) return;
        const key = r.unitId + '|' + kind;
        const cur = latest[key];
        // date → createdAt → id. The id tie-break keeps the result stable:
        // without it two same-day records with the same createdAt resolve by
        // whatever order Firestore happened to return, so the same data could
        // sync different licences on different runs.
        const newer = !cur
            || (r.date || '') > (cur.date || '')
            || ((r.date || '') === (cur.date || '') && (
                   (r.createdAt || 0) > (cur.createdAt || 0)
                || ((r.createdAt || 0) === (cur.createdAt || 0) && String(r.id) > String(cur.id))
               ));
        if (newer) latest[key] = r;
    });
    const recs = Object.values(latest);
    if (recs.length === 0) {
        showToast('Tidak ada distribusi standar (SF-RTK/SF-1/G5) yang bisa disinkron ke unit', 'info');
        return;
    }
    if (!confirm(`Terapkan ${recs.length} distribusi terbaru ke lisensi unit terkait?\n\n` +
                 `Tanggal habis = tanggal distribusi + 1 tahun. Data lisensi unit yang ada akan ditimpa.`)) return;
    // Explicit action with an "akan ditimpa" confirm above → force overwrite.
    let n = 0;
    recs.forEach(r => { if (applyDistributedLicenseToUnit(r, true) === 'applied') n++; });
    showToast(`${n} lisensi unit disinkron dari daftar distribusi`, 'success');
    if (currentView === 'dashboard') updateDashboard(filteredData);
    else if (currentView === 'editUnits') renderEditTable();
}

function saveLicenseStock(event) {
    event.preventDefault();
    if (!requireEdit('licenseStock')) return;

    const id = document.getElementById('editLicenseId').value;
    // Snapshot BEFORE any mutation — used to decide whether this save should
    // touch the unit's own licence at all (editing only the note must not).
    const prevRec = id ? { ...(globalLicenseStock.find(r => r.id === id) || {}) } : null;
    const txnType = document.getElementById('licTxnType').value;
    const typedType = document.getElementById('licType').value.trim();
    // Snap to the existing spelling so a stray "sf-rtk" doesn't open a second
    // stock bucket for a type that already exists.
    const licenseType = canonicalLicenseType(typedType);
    const qty = Math.max(1, parseInt(document.getElementById('licQty').value, 10) || 1);
    if (!licenseType) { showToast('Isi jenis lisensi', 'warning'); return; }
    if (typedType && licenseType !== typedType) {
        showToast(`Jenis lisensi disamakan menjadi "${licenseType}"`, 'info');
    }

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
        // A legacy row that already points at this heavy unit stays editable;
        // applyDistributedLicenseToUnit still refuses to write to the unit.
        const legacy = prevRec && prevRec.unitId === unit.id;
        if (isHeavy(unit) && !legacy) {
            showToast(`Unit "${unit.name}" adalah Alat Berat — lisensi SF/G5 hanya untuk ${UNIT_GROUPS.tractor.label}`, 'warning');
            return;
        }
        data.unitId = unit.id;
        data.unitName = unit.name || '';
        data.sn = unit.sn || '';

        // Warn (but allow) if distributing more than current remaining stock.
        const sum = computeLicenseSummary()[licenseType];
        let sisa = sum ? sum.sisa : 0;
        if (id) { // editing an existing OUT — add its old qty back to available
            const old = globalLicenseStock.find(r => r.id === id);
            // Folded: the stored row may carry a different casing than the
            // canonical type, and missing that would understate the stock the
            // edit frees up and warn about a shortage that isn't real.
            if (old && old.txnType === 'OUT' &&
                licenseTypeKey(old.licenseType) === licenseTypeKey(licenseType)) {
                sisa += (Number(old.qty) || 0);
            }
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
    // Only on create, or when the edit actually changed unit / type / date —
    // fixing a typo in the note must not re-stamp the unit's licence.
    const licenseRelevantChange = !prevRec
        || prevRec.txnType !== txnType
        || (prevRec.unitId || '') !== (data.unitId || '')
        || (prevRec.licenseType || '') !== licenseType
        || (prevRec.date || '') !== (data.date || '');
    if (txnType === 'OUT' && licenseRelevantChange) {
        const applied = applyDistributedLicenseToUnit({ txnType, unitId: data.unitId, licenseType, date: data.date });
        const kind = _licenseKindForType(licenseType) === 'display' ? 'Display' : 'GPS';
        if (applied === 'applied') {
            showToast(`Lisensi ${kind} unit "${data.unitName}" di-set ${licenseType} (berlaku 1 tahun)`, 'info');
        } else if (applied === 'skipped-older') {
            showToast(`Lisensi ${kind} unit "${data.unitName}" tidak diubah — unit sudah punya masa berlaku yang lebih panjang`, 'warning');
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
    if (!requireEdit('licenseStock')) return;
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
    if (!requireEdit('licenseStock')) return;
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
    if (!canCsv('export')) return;
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
        row.map(csvCell).join(',')).join('\n');
    const blob = new Blob(['﻿' + csv], { type: 'text/csv;charset=utf-8;' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = `stok_lisensi_${toISODate()}.csv`;
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
    if (!canCsv('export')) return;
    const headers = ['Tanggal', 'Jenis', 'Jenis Lisensi', 'Jumlah', 'Unit', 'Serial Number', 'Catatan'];
    const sample = [
        ['2026-04-06', 'Masuk', 'SF-RTK', '50', '', '', 'PO.GPA.2026.04.01343'],
        ['2026-05-11', 'Distribusi', 'G5 Advance', '1', 'GGCH001G', '', 'Dipasang di unit']
    ];
    const csv = [headers, ...sample].map(row =>
        row.map(csvCell).join(',')).join('\n');
    const blob = new Blob(['﻿' + csv], { type: 'text/csv;charset=utf-8;' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = 'template_stok_lisensi.csv';
    a.click();
    URL.revokeObjectURL(url);
}

function handleLicenseCSVImport(file) {
    if (!canCsv('full')) return;
    if (!requireEdit('licenseStock')) return;
    showLoading(true);
    Papa.parse(file, {
        header: true,
        skipEmptyLines: true,
        complete: result => {
            const today = toISODate();
            const added = [];
            let rejected = 0;
            let heavyRejected = 0;

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
                    // Matched against the agricultural units; a row whose SN or
                    // name belongs to a heavy unit is rejected outright rather
                    // than stored unlinked — an unlinked row still carries the
                    // SN, and liveUnitFor would resolve it back to that unit.
                    const bySn = u => (u.sn || '').toLowerCase() === snCsv.toLowerCase();
                    const byName = u => (u.name || '').toLowerCase() === nameCsv.toLowerCase();
                    let unit = null;
                    if (snCsv) {
                        unit = globalData.find(u => !isHeavy(u) && bySn(u));
                        if (!unit && globalData.some(u => isHeavy(u) && bySn(u))) { heavyRejected++; return; }
                    }
                    if (!unit && nameCsv) {
                        unit = globalData.find(u => !isHeavy(u) && byName(u));
                        if (!unit && globalData.some(u => isHeavy(u) && byName(u))) { heavyRejected++; return; }
                    }
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
                    // Same rollback as the implements import above: a refused
                    // batch must not keep rendering as if it had landed.
                    const addedIds = new Set(added.map(o => o.id));
                    window.cloud.saveLicenses(added).catch(err => {
                        globalLicenseStock = globalLicenseStock.filter(o => !addedIds.has(o.id));
                        saveLicenseStockLocal();
                        renderLicenseSummary();
                        renderLicenseStockTable();
                        cloudWriteFailed(err, {
                            what: `impor ${added.length} transaksi lisensi`,
                            label: '[Lisensi] Import CSV',
                            resync: resyncLicenses
                        });
                    });
                }
                logEvent({ action: 'add', unitName: '[Lisensi] Import CSV', after: `${added.length} transaksi` });
                populateLicenseTypeList();
                renderLicenseSummary();
                renderLicenseStockTable();
            }

            showLoading(false);
            const msg = `Import lisensi: ${added.length} ditambahkan` + (rejected ? `, ${rejected} dilewati (jenis lisensi kosong)` : '')
                + (heavyRejected ? `, ${heavyRejected} dilewati (unit Alat Berat — lisensi SF/G5 hanya untuk ${UNIT_GROUPS.tractor.shortLabel})` : '');
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
    window.cloud.saveLicense(rec).catch(err => cloudWriteFailed(err, {
        what: 'stok lisensi',
        label: `[Lisensi] ${rec.licenseType || '-'}`,
        unitId: rec.unitId || '',
        resync: resyncLicenses
    }));
}

function cloudDeleteLicense(id) {
    if (suppressCloudWrites || !window.cloud?.isReady || !id) return;
    window.cloud.deleteLicense(id).catch(err => cloudWriteFailed(err, {
        what: 'hapus stok lisensi',
        label: '[Lisensi] -',
        resync: resyncLicenses
    }));
}

function applyCloudLicenseSnapshot(items) {
    clearRulesBanner('licenseStock');
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
// One shape for every "the server refused" banner, replacing five that were
// each wrong in the same three ways.
//
// They printed a rules block keyed on `role in ['owner','team']` and told the
// reader to paste it. That is the model from before per-area access existed,
// so following the instruction would have cut off every staff/pbt/khl account
// holding an area grant — destructive advice, shown most often to the people
// most likely to act on it.
//
// They also fired from READ failures, but every one of these collections is
// `allow read: if isActive()`, so a read refusal means the account is not
// active — not that rules are missing. An ordinary member whose account was
// just disabled was being told to open the Firebase Console.
//
// And they never cleared, so the warning outlived the problem.
function renderRulesBanner(host, key, label, collections, opts) {
    if (!host || host.querySelector('.category-rules-banner')) return;
    const banner = document.createElement('div');
    banner.className = 'category-rules-banner';
    banner.dataset.rulesFor = key;
    const cols = collections.map(c => `<code>${escapeHtml(c)}</code>`).join(', ');
    banner.innerHTML = (isOwner && isOwner())
        ? `<strong><i class="fas fa-triangle-exclamation"></i> Firestore rules memblokir ${escapeHtml(label)}.</strong>
           <p>Rules proyek Anda belum mengizinkan akses ke koleksi ${cols}. Publish ulang file
           <code>firestore.rules</code> dari repo ini di
           <em>Firebase Console → Firestore → Rules</em>, lalu muat ulang halaman.</p>`
        : `<strong><i class="fas fa-triangle-exclamation"></i> Tidak bisa membuka ${escapeHtml(label)}.</strong>
           <p>Biasanya ini berarti akun Anda belum diaktifkan, atau belum diberi akses ke
           bagian ini. Minta owner memeriksanya, lalu muat ulang halaman.</p>`;
    if (opts && opts.prepend) host.insertBefore(banner, host.firstChild);
    else host.appendChild(banner);
}

// Called from the snapshot handlers: once data flows again the warning is
// stale, and a banner that outlives its cause teaches people to ignore banners.
function clearRulesBanner(key) {
    document.querySelectorAll(`.category-rules-banner[data-rules-for="${key}"]`)
        .forEach(b => b.remove());
}

function showLicenseRulesBanner() {
    renderRulesBanner(document.querySelector('#viewLicenseStock .license-rules-slot'),
        'licenseStock', 'stok lisensi', ['licenseStock']);
}

// ============================================================
// CLOUD SYNC (Firestore via window.cloud from firebase-init.js)
// ============================================================

function cloudPushUnits(units) {
    if (suppressCloudWrites || !window.cloud?.isReady || !units?.length) return;
    if (!canWriteUnits('cloudPushUnits')) return;
    window.cloud.saveUnits(units).catch(err => cloudWriteFailed(err, {
        what: 'unit',
        label: units.length === 1 ? (units[0].name || '-') : `${units.length} unit`,
        unitId: units.length === 1 ? units[0].id : '',
        resync: resyncUnits
    }));
}

function cloudDeleteUnits(ids) {
    if (suppressCloudWrites || !window.cloud?.isReady || !ids?.length) return;
    if (!canWriteUnits('cloudDeleteUnits')) return;
    window.cloud.deleteUnits(ids).catch(err => cloudWriteFailed(err, {
        what: 'hapus unit',
        label: `${ids.length} unit`,
        unitId: ids.length === 1 ? ids[0] : '',
        resync: resyncUnits
    }));
}

function cloudPushImplement(imp) {
    if (suppressCloudWrites || !window.cloud?.isReady || !imp) return;
    window.cloud.saveImplement(imp).catch(err => cloudWriteFailed(err, {
        what: 'implement',
        label: `[Implement] ${imp.profileName || imp.equipmentType || '-'}`,
        unitId: imp.id || '',
        resync: resyncImplements
    }));
}

function cloudDeleteImplement(id) {
    if (suppressCloudWrites || !window.cloud?.isReady || !id) return;
    window.cloud.deleteImplement(id).catch(err => cloudWriteFailed(err, {
        what: 'hapus implement',
        label: '[Implement] -',
        unitId: id,
        resync: resyncImplements
    }));
}

async function migrateLocalToCloudIfNeeded() {
    // Owner-only, checked here rather than only at the call site (script.js
    // ~5862): this uploads the whole local cache into four collections, so a
    // caller that forgets the check would refill them from stale data.
    if (!isOwner || !isOwner()) return;
    // Each of these used to answer "is the cloud copy empty?" by downloading
    // the entire collection and reading .length — 288 documents on every owner
    // sign-in, for four yes/no answers. isCollectionEmpty asks with limit(1).
    // Older cached firebase-init.js may not have it, so fall back rather than
    // skipping the bootstrap silently.
    const isEmpty = async (which, getAll) => {
        if (window.cloud.isCollectionEmpty) return window.cloud.isCollectionEmpty(which);
        const all = getAll ? await getAll() : [];
        return all.length === 0;
    };

    try {
        // Units
        if (await isEmpty('units', window.cloud.getAllUnits) && globalData.length > 0) {
            console.log(`[cloud] migrating ${globalData.length} local units to Firestore...`);
            await window.cloud.saveUnits(globalData);
            showToast(`${globalData.length} unit diunggah ke cloud`, 'success');
        }
        // Implements
        if (await isEmpty('implements', window.cloud.getAllImplements) && globalImplements.length > 0) {
            console.log(`[cloud] migrating ${globalImplements.length} local implements to Firestore...`);
            await window.cloud.saveImplements(globalImplements);
            showToast(`${globalImplements.length} implement diunggah ke cloud`, 'success');
        }
        // Damage records
        if (window.cloud.getAllDamages) {
            if (await isEmpty('damages', window.cloud.getAllDamages) && globalDamages.length > 0) {
                console.log(`[cloud] migrating ${globalDamages.length} local damage records to Firestore...`);
                await window.cloud.saveDamages(globalDamages);
                showToast(`${globalDamages.length} catatan kerusakan diunggah ke cloud`, 'success');
            }
        }
        // License stock
        if (window.cloud.getAllLicenses) {
            if (await isEmpty('licenses', window.cloud.getAllLicenses) && globalLicenseStock.length > 0) {
                console.log(`[cloud] migrating ${globalLicenseStock.length} local license records to Firestore...`);
                await window.cloud.saveLicenses(globalLicenseStock);
                showToast(`${globalLicenseStock.length} catatan lisensi diunggah ke cloud`, 'success');
            }
        }
    } catch (e) {
        console.error('[cloud] migration failed:', e);
        showToast('Migrasi ke cloud gagal — periksa console', 'error');
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
                filteredData = scopeDashUnits();
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

// applyDefaultLicensesIfNeeded() used to live here: it filled every unit with a
// blank licence with SF-RTK / G5 Basic, guarded only by a per-browser flag.
// All 190 units went through it long ago, and the flag meant any new owner
// browser would run it again — re-filling licences someone had cleared on
// purpose, and, once heavy equipment exists, stamping John Deere licences onto
// excavators. Removed for the same reason as the Excel migrations before it.

// One-shot migration: import license start dates from LICENSE_DATES_MAP
// (serial number → YYYY-MM-DD). Expiration is auto-set to +1 year. Only
// patches units with no existing licenseStartDate; manual values are kept.
// Owner-only, guarded by a localStorage flag so it never repeats.
function applyLicenseDatesIfNeeded() {
    if (!isOwner || !isOwner()) return;
    if (localStorage.getItem(LICENSE_DATES_KEY) === '1') return;
    if (!Array.isArray(globalData) || globalData.length === 0) return;

    const addOneYear = addOneYearISO;

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
        // For heavy equipment a missing licence means "not applicable", not
        // "not yet filled in". Fill-if-empty migrations must never reach it.
        if (isHeavy(unit)) return;
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
        showToast(`Tanggal lisensi diimpor untuk ${updates.length} unit`, 'success');
        if (currentView === 'dashboard') updateDashboard(filteredData);
        if (currentView === 'editUnits') renderEditTable();
    }).catch(err => {
        console.error('[license-dates] bulk save failed:', err);
        showToast('Impor tanggal lisensi gagal — periksa console', 'error');
    });
}

// ============================================================
// USER CATEGORIES (dynamic dropdown source)
// ============================================================

function applyCloudUserCategoriesSnapshot(cats) {
    clearRulesBanner('userCategories');
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
    // Automatic background seed: guard without a toast, unlike the
    // user-triggered paths. Nobody asked for this write, so nobody should be
    // told it was skipped.
    if (!window.cloud || !window.cloud.saveUserCategories) return;
    console.log('[user-categories] seeding 3 default categories...');
    window.cloud.saveUserCategories(defaults).then(() => {
        localStorage.setItem(USER_CATEGORIES_SEED_KEY, '1');
        showToast('Kategori user bawaan ditambahkan', 'success');
    }).catch(err => {
        console.error('[user-categories] seed failed:', err);
        if (err && err.code === 'permission-denied') {
            showCategoryRulesBanner();
        }
    });
}

// Surfaces a clear, actionable banner inside the Manage Categories modal
function showCategoryRulesBanner() {
    const modal = document.getElementById('categoriesModal');
    renderRulesBanner(modal && modal.querySelector('.modal-body'),
        'userCategories', 'kategori pengguna', ['userCategories'], { prepend: true });
}

function showHistoryRulesBanner() {
    const modal = document.getElementById('historyModal');
    renderRulesBanner(modal && modal.querySelector('.history-rules-slot'),
        'history', 'riwayat perubahan', ['history']);
}

function showDamageRulesBanner() {
    renderRulesBanner(document.querySelector('#viewDamage .damage-rules-slot'),
        'damageRecords', 'catatan kerusakan', ['damageRecords']);
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
    if (!requireEdit('editUnits')) return;
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
    if (!requireEdit('editUnits')) return;
    const input = document.getElementById('newCategoryName');
    const name = (input.value || '').trim();
    if (!name) {
        showToast('Isi nama kategori', 'warning');
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
    // Input dikosongkan langsung: tulisannya sudah terantre, jadi menunggu
    // konfirmasi server hanya akan membuat form terasa macet saat sinyal buruk.
    input.value = '';
    cloudWrite(
        { action: 'add', unitName: '-', field: 'user category', after: name },
        cloudCall('saveUserCategory', cat),
        `Kategori "${name}" ditambahkan`,
        err => {
        console.error('[user-categories] save failed:', err);
        const code = (err && err.code) || 'unknown';
        if (code === 'permission-denied') {
            showToast('Firestore rules memblokir userCategories — lihat banner', 'error');
            showCategoryRulesBanner();
        } else {
            showToast(`Gagal menyimpan kategori (${code})`, 'error');
        }
        }
    );
}

function deleteCategory(id) {
    if (!requireEdit('editUnits')) return;
    const cat = userCategories.find(c => c.id === id);
    if (!cat) return;
    // Warn if this category is in use by any unit
    const inUse = globalData.filter(u => u.userCategory === cat.name).length;
    const prompt = inUse > 0
        ? `Delete category "${cat.name}"?\n${inUse} unit(s) still reference it — their value will be cleared.`
        : `Delete category "${cat.name}"?`;
    if (!confirm(prompt)) return;
    cloudWrite(
        { action: 'delete', unitName: '-', field: 'user category', before: cat.name },
        cloudCall('deleteUserCategory', id),
        `Kategori "${cat.name}" dihapus`,
        err => {
        console.error('[user-categories] delete failed:', err);
        const code = (err && err.code) || 'unknown';
        if (code === 'permission-denied') {
            showToast('Firestore rules memblokir userCategories — lihat banner', 'error');
            showCategoryRulesBanner();
        } else {
            showToast(`Gagal menghapus kategori (${code})`, 'error');
        }
        }
    );
}

// ============================================================
// DAMAGE COMPONENTS (dynamic dropdown source)
// ============================================================

function applyCloudDamageComponentsSnapshot(comps) {
    clearRulesBanner('damageComponents');
    damageComponents = (comps || []).slice().sort((a, b) =>
        (a.name || '').localeCompare(b.name || '')
    );
    // First-snapshot seed: populate the four built-ins so the dropdown is
    // never blank on a fresh project.
    if (_firstDamageComponentsSnapshot) {
        _firstDamageComponentsSnapshot = false;
        if (damageComponents.length === 0) {
            seedDefaultDamageComponentsIfOwner();
        }
    }
    renderDamageComponentOptions();
    const mgr = document.getElementById('damageComponentsModal');
    if (mgr && mgr.classList.contains('open')) renderDamageComponentsList();
}

function seedDefaultDamageComponentsIfOwner() {
    if (!isOwner || !isOwner()) return;
    if (localStorage.getItem(DAMAGE_COMPONENTS_SEED_KEY) === '1') return;
    const now = Date.now();
    const defaults = DEFAULT_DAMAGE_COMPONENTS.map((c, idx) => ({
        id: `dcmp_${now}_${idx}`,
        name: c.name,
        unitField: c.unitField,
        createdAt: now
    }));
    console.log('[damage-components] seeding 4 default components...');
    if (!window.cloud || !window.cloud.saveDamageComponents) return;
    window.cloud.saveDamageComponents(defaults).then(() => {
        localStorage.setItem(DAMAGE_COMPONENTS_SEED_KEY, '1');
    }).catch(err => {
        console.error('[damage-components] seed failed:', err);
        if (err && err.code === 'permission-denied') showDamageComponentRulesBanner();
    });
}

function showDamageComponentRulesBanner() {
    const modal = document.getElementById('damageComponentsModal');
    renderRulesBanner(modal && modal.querySelector('.modal-body'),
        'damageComponents', 'komponen kerusakan', ['damageComponents'], { prepend: true });
}

// Fill the damage modal's component <select> from the managed list.
function renderDamageComponentOptions() {
    const select = document.getElementById('dmgComponent');
    if (!select) return;
    const current = select.value;
    const unitEl = document.getElementById('dmgUnit');
    const unit = unitEl && unitEl.value ? resolveDamageUnit(unitEl.value) : null;
    const tractorList = damageComponents.length
        ? damageComponents
        : DEFAULT_DAMAGE_COMPONENTS; // pre-sync fallback
    // A heavy unit is offered its own components, plus the owner's custom
    // parts that drive the unit's status (Hidrolik, Mesin…). Everything else —
    // no unit yet, or a tractor — gets today's list exactly.
    const list = (unit && isHeavy(unit))
        ? UNIT_GROUPS.heavy.components.map(c => ({ name: c.label }))
            .concat(tractorList.filter(c => !c.unitField && !heavyComponentField(c.name)))
        : tractorList;
    const opts = ['<option value="">Pilih komponen… (opsional)</option>'];
    list.forEach(c => {
        opts.push(`<option value="${escapeHtml(c.name)}">${escapeHtml(c.name)}</option>`);
    });
    select.innerHTML = opts.join('');
    // A live snapshot can land while the damage modal is open. If the selected
    // component is missing from the new list (someone deleted it, or it's a
    // legacy value), keep it as an option — otherwise the field silently goes
    // blank mid-edit and the next save writes an empty component.
    // ...except a value that belongs to the other group than the chosen unit,
    // which is dropped — unless it is the saved value of the very record being
    // edited, on the same unit (a legacy record must stay editable).
    let dropped = '';
    if (current && unit && componentGroupConflict(current, unit)) {
        const editId = (document.getElementById('editDamageId') || {}).value;
        const rec = editId ? globalDamages.find(d => d.id === editId) : null;
        const legacyKeep = rec && rec.component === current && (liveUnitFor(rec) || {}).id === unit.id;
        if (!legacyKeep) dropped = current;
    }
    if (current && !dropped && !list.some(c => c.name === current)) {
        select.insertAdjacentHTML('beforeend',
            `<option value="${escapeHtml(current)}">${escapeHtml(current)}</option>`);
    }
    if (current && !dropped) select.value = current;
    renderDamageHint(unit);
    return dropped;
}

// The hint under "Set status Breakdown" names the components of the chosen
// unit's group. Tractor (or no unit): today's wording.
const DAMAGE_HINT_TRACTOR = 'Device Precision → komponen terkait (GPS/Display/Steering/JDLink); tipe lain → status unit.';
function renderDamageHint(unit) {
    const el = document.getElementById('dmgBreakdownHint');
    if (!el) return;
    el.textContent = (unit && isHeavy(unit))
        ? 'Device Precision → komponen terkait (Camera AI/Telematic Box/Switch Limiter/Rotary Lamp); tipe lain → status unit.'
        : DAMAGE_HINT_TRACTOR;
}

function onDamageUnitChanged() {
    const dropped = renderDamageComponentOptions();
    if (dropped) {
        const unit = resolveDamageUnit(document.getElementById('dmgUnit').value);
        showToast(`Komponen "${dropped}" tidak berlaku untuk ${groupDef(unitGroupOf(unit)).shortLabel} — pilih ulang`, 'warning');
    }
}

function openDamageComponentsModal() {
    if (!requireEdit('damage')) return;
    renderDamageComponentsList();
    document.getElementById('damageComponentsModal').classList.add('open');
    setTimeout(() => {
        const input = document.getElementById('newComponentName');
        if (input) input.focus();
    }, 50);
}

function closeDamageComponentsModal() {
    document.getElementById('damageComponentsModal').classList.remove('open');
}

function renderDamageComponentsList() {
    const list = document.getElementById('damageComponentsList');
    if (!list) return;
    if (damageComponents.length === 0) {
        list.innerHTML = '<li class="category-empty">Belum ada komponen — tambahkan di bawah.</li>';
        return;
    }
    list.innerHTML = damageComponents.map(c => `
        <li class="category-item">
            <span class="category-item__name">${escapeHtml(c.name)}${c.unitField
                ? ` <span style="font-size:11px;color:var(--text-light)">· status unit: ${escapeHtml(c.unitField)}</span>`
                : ''}</span>
            <button class="btn-icon category-item__del" title="Hapus komponen" onclick="deleteDamageComponent('${escapeHtml(c.id)}')">
                <i class="fas fa-trash" style="color:var(--danger)"></i>
            </button>
        </li>
    `).join('');
}

function addDamageComponent(event) {
    if (event) event.preventDefault();
    if (!requireEdit('damage')) return;
    const input = document.getElementById('newComponentName');
    const name = (input.value || '').trim();
    if (!name) { showToast('Isi nama komponen', 'warning'); return; }
    const exists = damageComponents.some(c => (c.name || '').toLowerCase() === name.toLowerCase());
    if (exists) { showToast(`Komponen "${name}" sudah ada`, 'warning'); return; }
    // Heavy components are built in and keyed to heavy fields; a custom one of
    // the same name would put 'Camera AI' in front of every tractor.
    if (heavyComponentField(name)) {
        showToast(`"${name}" sudah bawaan Alat Berat — tidak perlu ditambahkan`, 'warning');
        return;
    }

    const comp = {
        id: `dcmp_${Date.now()}_${Math.random().toString(36).slice(2, 6)}`,
        name,
        // Custom components aren't tied to a unit status column; a damage on
        // them flips the unit's overall status instead.
        unitField: DAMAGE_COMPONENT_FIELD[name] || '',
        createdAt: Date.now()
    };
    input.value = '';
    cloudWrite(
        { action: 'add', unitName: '-', field: 'komponen kerusakan', after: name },
        cloudCall('saveDamageComponent', comp),
        `Komponen "${name}" ditambahkan`,
        err => {
        console.error('[damage-components] save failed:', err);
        const code = (err && err.code) || 'unknown';
        if (code === 'permission-denied') {
            showToast('Firestore rules memblokir damageComponents — lihat panduan', 'error');
            showDamageComponentRulesBanner();
        } else {
            showToast(`Gagal menyimpan komponen (${code})`, 'error');
        }
        }
    );
}

function deleteDamageComponent(id) {
    if (!requireEdit('damage')) return;
    const comp = damageComponents.find(c => c.id === id);
    if (!comp) return;
    const inUse = globalDamages.filter(d => d.component === comp.name).length;
    const prompt = inUse > 0
        ? `Hapus komponen "${comp.name}"?\n${inUse} catatan kerusakan memakainya (data lama tetap tersimpan).`
        : `Hapus komponen "${comp.name}"?`;
    if (!confirm(prompt)) return;
    cloudWrite(
        { action: 'delete', unitName: '-', field: 'komponen kerusakan', before: comp.name },
        cloudCall('deleteDamageComponent', id),
        `Komponen "${comp.name}" dihapus`,
        err => {
        console.error('[damage-components] delete failed:', err);
        const code = (err && err.code) || 'unknown';
        if (code === 'permission-denied') {
            showToast('Firestore rules memblokir damageComponents — lihat panduan', 'error');
            showDamageComponentRulesBanner();
        } else {
            showToast(`Gagal menghapus komponen (${code})`, 'error');
        }
        }
    );
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
        // History is deliberately NOT subscribed here. See
        // startHistorySubscription() for why.
        if (window.cloud.subscribeDamageComponents) {
            cloudDamageComponentsUnsub = window.cloud.subscribeDamageComponents(
                applyCloudDamageComponentsSnapshot,
                err => {
                    console.warn('[cloud] damageComponents offline:', err && err.code);
                    if (err && err.code === 'permission-denied') {
                        showDamageComponentRulesBanner();
                    }
                }
            );
        }
        if (window.cloud.subscribeDevices) {
            cloudDevicesUnsub = window.cloud.subscribeDevices(
                applyCloudDevicesSnapshot,
                err => {
                    console.warn('[cloud] devices offline:', err && err.code);
                    if (err && err.code === 'permission-denied') showWarehouseRulesBanner();
                }
            );
        }
        if (window.cloud.subscribeStockItems) {
            cloudStockUnsub = window.cloud.subscribeStockItems(
                applyCloudStockSnapshot,
                err => {
                    console.warn('[cloud] stockItems offline:', err && err.code);
                    if (err && err.code === 'permission-denied') showWarehouseRulesBanner();
                }
            );
        }
        if (window.cloud.subscribeTeamMembers) {
            cloudTeamMembersUnsub = window.cloud.subscribeTeamMembers(
                applyCloudTeamMembersSnapshot,
                err => {
                    console.warn('[cloud] teamMembers offline:', err && err.code);
                    if (err && err.code === 'permission-denied') showTeamRulesBanner();
                }
            );
        }
        if (window.cloud.subscribeShifts) startShiftsSubscription();
        if (window.cloud.subscribeWorkLogs) {
            cloudWorkLogsUnsub = window.cloud.subscribeWorkLogs(
                applyCloudWorkLogsSnapshot,
                err => {
                    console.warn('[cloud] workLogs offline:', err && err.code);
                    if (err && err.code === 'permission-denied') showTeamRulesBanner();
                }
            );
        }
        if (window.cloud.subscribeLeaveRequests) {
            cloudLeaveUnsub = window.cloud.subscribeLeaveRequests(
                applyCloudLeaveSnapshot,
                err => {
                    console.warn('[cloud] leaveRequests offline:', err && err.code);
                    if (err && err.code === 'permission-denied') showTeamRulesBanner();
                }
            );
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
            clearSessionClock();
            stopWatchingOwnUserDoc();
            tearDownCloudSync();
            showAuthGate('signin');
            return;
        }

        // Enforce the daily session window on the persisted login.
        if (!checkDailySession()) return;

        currentUser = user;
        // Before anything in this session logs or renders: a shared browser may
        // still hold unsynced audit rows belonging to whoever signed in last.
        pruneForeignLocalAudit(user.uid);

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

        // Pending users: park them on the waiting screen. Waiting for approval
        // is not a session, so stop the 24h clock — otherwise approval at hour
        // 23 would hand the user a single hour instead of a full day.
        if (profile.status !== 'active') {
            clearSessionClock();
            showPendingGate(user.email);
            return;
        }

        // Active user — show app, gate UI by role, start cloud sync.
        if (!localStorage.getItem(SESSION_START_KEY)) startSessionClock();
        hideAuthGates();
        applyRoleGating();
        renderUserPill();
        // Take the account's single session slot, then watch for anyone else
        // taking it from us.
        await claimActiveSession(user.uid);
        watchOwnUserDoc(user.uid);
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
    // Claims belong to the session that made them; a new sign-in must not
    // inherit them and warn about its own writes.
    _shiftPendingWrites.clear();
    if (cloudUserCategoriesUnsub) { try { cloudUserCategoriesUnsub(); } catch (_) {} cloudUserCategoriesUnsub = null; }
    if (cloudDamageComponentsUnsub) { try { cloudDamageComponentsUnsub(); } catch (_) {} cloudDamageComponentsUnsub = null; }
    if (cloudDevicesUnsub) { try { cloudDevicesUnsub(); } catch (_) {} cloudDevicesUnsub = null; }
    if (cloudStockUnsub) { try { cloudStockUnsub(); } catch (_) {} cloudStockUnsub = null; }
    warehouseDevices = [];
    stockLedger = [];
    if (cloudTeamMembersUnsub) { try { cloudTeamMembersUnsub(); } catch (_) {} cloudTeamMembersUnsub = null; }
    if (cloudShiftsUnsub) { try { cloudShiftsUnsub(); } catch (_) {} cloudShiftsUnsub = null; }
    _shiftWindowStart = '';
    if (cloudWorkLogsUnsub) { try { cloudWorkLogsUnsub(); } catch (_) {} cloudWorkLogsUnsub = null; }
    if (cloudLeaveUnsub) { try { cloudLeaveUnsub(); } catch (_) {} cloudLeaveUnsub = null; }
    teamMembers = [];
    teamShifts = [];
    workLogs = [];
    leaveRequests = [];
    _leaveDocCache.clear();
    cloudHistory = [];
    userCategories = [];
    _firstUserCategoriesSnapshot = true;
    damageComponents = [];
    _firstDamageComponentsSnapshot = true;
    // Entries queued but not yet pushed still carry the outgoing user's
    // actorUid. Firestore requires actorUid == request.auth.uid, so sending
    // them after the next person signs in gets the whole batch refused — and
    // the outgoing user's real actions vanish along with it. Drop them here;
    // logEvent has already written them to the local cache.
    if (_historyFlushTimer) { clearTimeout(_historyFlushTimer); _historyFlushTimer = null; }
    _historyPushQueue.length = 0;
    cloudInitialized = false;
}

// The local audit cache lives in the browser, and getAuditLog() merges it into
// whatever the signed-in user sees. On a shared device that meant the next
// person read the previous person's unsynced rows, rendered under the previous
// person's name and indistinguishable from real ones. Anything belonging to
// somebody else is dropped as soon as we know who is signing in; the user's
// own rows survive a sign-out and come back.
function pruneForeignLocalAudit(uid) {
    try {
        const log = JSON.parse(localStorage.getItem(AUDIT_LOG_KEY) || '[]');
        const mine = log.filter(e => e && e.actorUid === uid);
        if (mine.length !== log.length) {
            localStorage.setItem(AUDIT_LOG_KEY, JSON.stringify(mine));
            console.log(`[audit] dropped ${log.length - mine.length} cached entries from another account`);
        }
    } catch (e) { /* ignore */ }
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

// ---- Daily session expiry (re-login required after 24h) ----
const SESSION_MAX_MS = 24 * 60 * 60 * 1000; // 24 hours
const SESSION_START_KEY = 'tractorSessionStart';
let _sessionTimer = null;

function startSessionClock() {
    localStorage.setItem(SESSION_START_KEY, String(Date.now()));
}

// ============================================================
// ONE ACTIVE SESSION PER ACCOUNT
// ------------------------------------------------------------
// The newest sign-in wins: it writes its own id into the account document,
// and every other device — which is watching that document — sees an id that
// is not its own and signs itself out. Newest-wins rather than first-wins on
// purpose: a tab closed without signing out, or a lost phone, would otherwise
// hold the account hostage until the session aged out.
//
// The id lives in localStorage, which is shared between tabs of the same
// browser, so several tabs on one device count as one session and do not
// fight each other.
//
// This is enforced by the app, not by the database: it stops an account being
// shared across two phones, but it is not a defence against someone editing
// the client. Hard enforcement would need a server.
// ============================================================

const SESSION_ID_KEY = 'tractorSessionId';
let _mySessionId = '';
let _userDocUnsub = null;
let _sessionTakenOver = false;

function mySessionId() {
    if (_mySessionId) return _mySessionId;
    let id = '';
    try { id = localStorage.getItem(SESSION_ID_KEY) || ''; } catch (e) { /* private mode */ }
    if (!id) {
        id = 's_' + Date.now() + '_' + Math.random().toString(36).slice(2, 10);
        try { localStorage.setItem(SESSION_ID_KEY, id); } catch (e) { /* ignore */ }
    }
    _mySessionId = id;
    return id;
}

// Coarse, human-readable label so the audit trail says something useful.
// Deliberately not a fingerprint.
function deviceLabel() {
    const ua = navigator.userAgent || '';
    const os = /Android/i.test(ua) ? 'Android'
        : /iPhone|iPad|iPod/i.test(ua) ? 'iOS'
        : /Windows/i.test(ua) ? 'Windows'
        : /Mac OS X/i.test(ua) ? 'macOS'
        : /Linux/i.test(ua) ? 'Linux' : 'Perangkat lain';
    const browser = /Edg\//.test(ua) ? 'Edge'
        : /OPR\//.test(ua) ? 'Opera'
        : /Chrome\//.test(ua) ? 'Chrome'
        : /Safari\//.test(ua) ? 'Safari'
        : /Firefox\//.test(ua) ? 'Firefox' : 'Browser';
    return `${browser} · ${os}`;
}

async function claimActiveSession(uid) {
    if (!window.cloud?.claimSession) return;
    try {
        await window.cloud.claimSession(uid, {
            id: mySessionId(),
            startedAt: Date.now(),
            device: deviceLabel()
        });
    } catch (e) {
        // Old rules that do not allow a self-update of activeSession land here.
        // Sign-in still works; the single-session rule just is not enforced.
        console.warn('[session] could not claim session:', e && e.code);
        if (e && e.code === 'permission-denied') {
            showToast('Aturan sesi tunggal belum aktif — publish ulang firestore.rules', 'warning');
        }
    }
}

// Watch this user's own document: session takeovers, plus role and access
// changes, now reach them without a reload.
function watchOwnUserDoc(uid) {
    if (_userDocUnsub || !window.cloud?.subscribeUserDoc) return;
    _userDocUnsub = window.cloud.subscribeUserDoc(uid, doc => {
        if (!doc || _sessionTakenOver) return;

        const active = doc.activeSession;
        if (active && active.id && active.id !== mySessionId()) {
            handleSessionTakenOver(active);
            return;
        }

        // Role/status/access changed under us — apply it live.
        const before = currentUserDoc || {};
        currentUserDoc = doc;
        if (doc.status !== 'active') {
            showPendingGate(doc.email || '');
            return;
        }
        if (JSON.stringify(before.access) !== JSON.stringify(doc.access) || before.role !== doc.role) {
            applyRoleGating();
            renderUserPill();
            showToast('Hak akses Anda diperbarui oleh admin', 'info');
        }
    }, err => console.warn('[session] user doc watch failed:', err && err.code));
}

function stopWatchingOwnUserDoc() {
    if (_userDocUnsub) { try { _userDocUnsub(); } catch (_) {} _userDocUnsub = null; }
}

async function handleSessionTakenOver(active) {
    if (_sessionTakenOver) return;
    _sessionTakenOver = true;
    stopWatchingOwnUserDoc();
    tearDownCloudSync();
    clearSessionClock();

    const revoked = String(active.id || '').startsWith('revoked_');
    const where = active.device ? ` (${active.device})` : '';
    const msg = revoked
        ? 'Sesi Anda diakhiri oleh admin. Silakan masuk kembali.'
        : `Anda dikeluarkan karena akun ini dibuka di perangkat lain${where}.`;

    try { await window.cloud.signOutUser(); } catch (e) { /* gate still shows below */ }
    currentUser = null;
    currentUserDoc = null;
    showAuthGate('signin');
    showAuthError('signInError', msg);
    // Cleared so the next sign-in on this device is not treated as a takeover.
    _sessionTakenOver = false;
}

function clearSessionClock() {
    localStorage.removeItem(SESSION_START_KEY);
    if (_sessionTimer) { clearTimeout(_sessionTimer); _sessionTimer = null; }
}

let _expiring = false;

async function expireSession() {
    if (_expiring) return;
    _expiring = true;
    if (_sessionTimer) { clearTimeout(_sessionTimer); _sessionTimer = null; }
    showToast('Sesi harian berakhir — silakan login kembali', 'warning');
    try {
        await window.cloud.signOutUser();
        // onAuthChange(null) clears the clock; do it here too in case it
        // doesn't fire (e.g. no listener yet).
        clearSessionClock();
    } catch (_) {
        // Offline: the sign-out call failed. Do NOT clear the start timestamp
        // — that would grandfather a brand-new 24h window on the next check
        // and let an expired session run indefinitely. Lock the UI now and
        // leave the expired stamp in place so the next check expires again.
        currentUser = null;
        currentUserDoc = null;
        tearDownCloudSync();
        showAuthGate('signin');
    }
    _expiring = false;
}

// Returns true while the session is valid; logs out and returns false if the
// 24-hour window has passed. Also arms a timer to auto-logout when it does.
function checkDailySession() {
    let start = parseInt(localStorage.getItem(SESSION_START_KEY), 10);
    if (!start || isNaN(start)) {
        // Grandfather an already-persisted login: start counting from now.
        start = Date.now();
        localStorage.setItem(SESSION_START_KEY, String(start));
    }
    const elapsed = Date.now() - start;
    if (elapsed >= SESSION_MAX_MS) { expireSession(); return false; }
    if (_sessionTimer) clearTimeout(_sessionTimer);
    _sessionTimer = setTimeout(expireSession, SESSION_MAX_MS - elapsed);
    return true;
}

// A sleeping device or a discarded background tab delays setTimeout by however
// long it was out, so the timer alone can leave an expired session running.
// Re-check the wall clock whenever the tab comes back to the foreground.
function watchSessionOnResume() {
    const recheck = () => { if (currentUser) checkDailySession(); };
    document.addEventListener('visibilitychange', () => { if (!document.hidden) recheck(); });
    window.addEventListener('focus', recheck);
}
watchSessionOnResume();

async function handleSignIn(event) {
    event.preventDefault();
    showAuthError('signInError', '');
    const email = document.getElementById('signInEmail').value.trim();
    const password = document.getElementById('signInPassword').value;
    try {
        showLoading(true);
        await window.cloud.signIn(email, password);
        startSessionClock(); // fresh 24h window
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
        startSessionClock(); // fresh 24h window
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
    const label = roleLabel(currentUserDoc.role);
    document.getElementById('userPillRole').textContent = label;
    pill.dataset.role = currentUserDoc.role;
    document.getElementById('userMenuEmail').textContent = currentUserDoc.email || '';
    document.getElementById('userMenuRoleLabel').textContent = `Akun ${label} · ${APP_VERSION}`;
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

function isOwner() {
    return currentUserDoc && currentUserDoc.role === 'owner';
}

// ============================================================
// PER-USER ACCESS (privilege per area)
// ============================================================
// Configurable areas the owner can restrict per user. Users management stays
// owner-only; the Dashboard is always available to any active user.
const ACCESS_AREAS = [
    { key: 'editUnits',    label: 'Edit Units',     levels: ['none', 'view', 'edit'] },
    { key: 'implements',   label: 'Implements',     levels: ['none', 'view', 'edit'] },
    { key: 'damage',       label: 'Kerusakan',      levels: ['none', 'view', 'edit'] },
    { key: 'licenseStock', label: 'Stok Lisensi',   levels: ['none', 'view', 'edit'] },
    // The Tim page is three separate permissions, not one. A KHL may need to
    // read the roster and file their own daily report while having no say over
    // the shift schedule, which a single team-wide level cannot express.
    { key: 'teamShift',    label: 'Jadwal Shift',   levels: ['none', 'view', 'edit'] },
    { key: 'teamLog',      label: 'Laporan Harian', levels: ['none', 'view', 'edit'] },
    { key: 'teamMembers',  label: 'Daftar Anggota', levels: ['none', 'view', 'edit'] },
    // Separate from teamLog on purpose: a checker should be able to approve a
    // report without being able to rewrite the thing they are checking.
    { key: 'teamLogApprove', label: 'Persetujuan Laporan', levels: ['none', 'edit'] },
    { key: 'warehouse',    label: 'Gudang',         levels: ['none', 'view', 'edit'] },
    // Read-only by nature: it shows what other areas already allow, nothing more.
    { key: 'leader',       label: 'Kotak Keputusan', levels: ['none', 'view'] },
    { key: 'history',      label: 'History',        levels: ['none', 'view'] }
];

// Which access areas back each navigable view. A view opens when the user can
// view at least one of them — the Tim page is reachable through any of its
// three areas. Several places used to hardcode this list; keeping the mapping
// in one place stops them drifting apart when an area is added.
const VIEW_AREAS = {
    editUnits:    ['editUnits'],
    implements:   ['implements'],
    damage:       ['damage'],
    licenseStock: ['licenseStock'],
    team:         ['teamShift', 'teamLog', 'teamMembers', 'teamLogApprove'],
    warehouse:    ['warehouse'],
    leader:       ['leader']
};
const GATED_VIEWS = Object.keys(VIEW_AREAS);
// Every area that holds data (i.e. everything except the read-only audit log).
// 'leader' is view-only and 'history' is the audit log: neither is a place
// where data is edited, so neither counts towards "can this person edit".
const DATA_AREAS = ACCESS_AREAS
    .filter(a => a.key !== 'history' && a.key !== 'leader')
    .map(a => a.key);
// area key → <body> dataset flag used by the editonly--* CSS rules.
const RO_FLAGS = {
    editUnits: 'roEditunits', implements: 'roImplements', damage: 'roDamage',
    licenseStock: 'roLicense', teamShift: 'roTeamshift', teamLog: 'roTeamlog',
    teamMembers: 'roTeammembers', teamLogApprove: 'roTeamapprove',
    warehouse: 'roWarehouse'
};
const _LVL_RANK = { none: 0, view: 1, edit: 2 };

// ---- Roles ----
// 'owner' keeps its internal key: the owner e-mail allowlist and the Firestore
// rules are written against it, so only its label changed to Super Admin.
// Staff / PBT / KHL grant nothing on their own — they are labels, and the owner
// sets every area explicitly in the Akses dialog.
const ROLES = [
    { key: 'owner', label: 'Super Admin' },
    { key: 'staff', label: 'Staff' },
    { key: 'pbt',   label: 'PBT' },
    { key: 'khl',   label: 'KHL' }
];
// Roles from before the four-level model. Still honoured exactly as they were,
// so no existing account silently gains or loses access; the Users table flags
// them so an owner can move each person over deliberately.
const LEGACY_ROLES = [
    { key: 'team',   label: 'Team' },
    { key: 'viewer', label: 'Viewer' }
];
function roleLabel(key) {
    const r = ROLES.concat(LEGACY_ROLES).find(x => x.key === key);
    return r ? r.label : (key || '—');
}
function isLegacyRole(key) {
    return LEGACY_ROLES.some(r => r.key === key);
}

// Level a role grants for an area when the user has no explicit override.
function roleDefaultAccess(role, area) {
    if (role === 'owner') return 'edit';
    // Legacy role: keep the old behaviour untouched.
    if (role === 'team')  return area === 'history' ? 'view' : 'edit';
    // staff / pbt / khl / viewer / pending — nothing until the owner grants it.
    return 'none';
}

// True when the user can open a given view — any one of its areas is enough.
function canViewView(view) {
    return (VIEW_AREAS[view] || []).some(a => hasAccess(a, 'view'));
}

// Effective level of a user for an area (explicit access override → role default).
function effectiveAccess(area, user = currentUserDoc) {
    if (!user) return 'none';
    if (user.role === 'owner') return 'edit';
    const explicit = user.access && user.access[area];
    return explicit || roleDefaultAccess(user.role, area);
}

// Does the user meet at least `min` access ('view' or 'edit') for an area?
function hasAccess(area, min, user = currentUserDoc) {
    if (user && user.role === 'owner') return true;
    return _LVL_RANK[effectiveAccess(area, user)] >= _LVL_RANK[min];
}

// ---- CSV export/import capability (separate none/export/full privilege) ----
const CSV_LEVELS = ['none', 'export', 'full'];
const _CSV_RANK = { none: 0, export: 1, full: 2 };
// Same shape as roleDefaultAccess: only the legacy 'team' role carries a
// default, the new roles start at none and are granted explicitly.
function roleDefaultCsv(role) { return (role === 'owner' || role === 'team') ? 'full' : 'none'; }
function effectiveCsv(user = currentUserDoc) {
    if (!user) return 'none';
    if (user.role === 'owner') return 'full';
    return (user.access && user.access.csv) || roleDefaultCsv(user.role);
}
// min = 'export' (can export) or 'full' (can import). Shows a toast when denied.
function canCsv(min, notify = true) {
    if (currentUserDoc && currentUserDoc.role === 'owner') return true;
    const ok = _CSV_RANK[effectiveCsv()] >= _CSV_RANK[min];
    if (!ok && notify) {
        showToast(min === 'full'
            ? 'Anda tidak punya izin Import CSV'
            : 'Anda tidak punya izin Export CSV', 'warning');
    }
    return ok;
}

// True when the user can edit at least one data area — by role OR by an
// explicit per-area grant. A viewer the owner gave 'edit' on one area is an
// editor, so gating must not go by role alone.
function canEditAnyArea() {
    return DATA_AREAS.some(a => hasAccess(a, 'edit'));
}

function applyRoleGating() {
    const owner = isOwner();

    // Owner-only navigation links
    document.querySelectorAll('[data-owner-only]').forEach(el => {
        el.style.display = owner ? '' : 'none';
    });

    applyAccessVisibility();

    // If the user lost access to the current view, send them to the dashboard.
    if ((!owner && currentView === 'users') ||
        (GATED_VIEWS.includes(currentView) && !canViewView(currentView))) {
        navigateTo('dashboard');
    }

    // Re-render any visible table to refresh its action buttons / editability
    if (currentView === 'editUnits') renderEditTable();
    if (currentView === 'implements') renderImplementsTable();
}

// Hide sidebar links the user can't view, and flag view-only areas on <body>
// so edit-only controls (Add buttons, inline editing) can be CSS-hidden.
function applyAccessVisibility() {
    document.querySelectorAll('.nav__link[data-view]').forEach(el => {
        const v = el.getAttribute('data-view');
        if (GATED_VIEWS.includes(v)) {
            el.style.display = canViewView(v) ? '' : 'none';
        }
    });
    updateDecisionBadge();
    const navHistory = document.getElementById('navHistory');
    if (navHistory) navHistory.style.display = hasAccess('history', 'view') ? '' : 'none';
    // The owner can change someone's access while they are signed in. Hiding
    // the menu is not enough — the stream has to stop too.
    if (!hasAccess('history', 'view')) stopHistorySubscription();

    // data-ro-<area>="1" whenever the user may NOT edit that area — including
    // no access at all, so the edit controls stay hidden even if the section
    // is somehow reachable.
    Object.entries(RO_FLAGS).forEach(([area, flag]) => {
        if (!hasAccess(area, 'edit')) document.body.dataset[flag] = '1';
        else delete document.body.dataset[flag];
    });

    // CSV capability flags → hide export/import buttons.
    if (canCsv('export', false)) delete document.body.dataset.nocsvexport; else document.body.dataset.nocsvexport = '1';
    if (canCsv('full', false))   delete document.body.dataset.nocsvimport; else document.body.dataset.nocsvimport = '1';
}

// Gate an edit action. With an `area` it checks per-area edit access; without,
// it falls back to canEditAnyArea() — an explicit per-area grant counts, so
// this must not go by role alone.
function requireEdit(area) {
    const ok = area ? hasAccess(area, 'edit') : canEditAnyArea();
    if (!ok) {
        showToast('Akses hanya-lihat — minta owner untuk memberi hak edit', 'warning');
        return false;
    }
    return true;
}

// ============================================================
// USER MANAGEMENT (Khusus owner)
// ============================================================

function ensureUsersSubscription() {
    if (!isOwner()) return;
    if (cloudUsersUnsub) return;
    if (!window.cloud || !window.cloud.subscribeUsers) {
        console.warn('[users] subscribeUsers tidak ada — firebase-init.js lama masih disajikan');
        showToast(STALE_CLIENT_MSG, 'warning');
        return;
    }
    cloudUsersUnsub = window.cloud.subscribeUsers(users => {
        allUsers = users;
        if (currentView === 'users') renderUsersView();
    }, err => {
        console.warn('[cloud] users subscription error:', err);
    });
}

// Compact, Indonesian, no seconds — the default toLocaleString() renders
// "8/19/2026, 2:16:34 PM", which is US-formatted and wide enough to crowd the
// action column out of the row.
function formatUserTime(ms) {
    if (!ms) return '—';
    const d = new Date(ms);
    if (isNaN(d.getTime())) return '—';
    return d.toLocaleString('id-ID', {
        day: '2-digit', month: 'short', year: 'numeric',
        hour: '2-digit', minute: '2-digit'
    });
}

// "Diubah oleh" repeats the same address on every row, and the domain is the
// noisy half. Show the local part; the full address stays in the tooltip.
function shortActor(value) {
    const v = (value || '').trim();
    if (!v) return '—';
    const at = v.indexOf('@');
    return at > 0 ? v.slice(0, at) : v;
}

function renderUsersView() {
    if (!isOwner()) return;
    const pending = allUsers.filter(u => u.status !== 'active');
    const active  = allUsers.filter(u => u.status === 'active');

    document.getElementById('usersCount').textContent =
        `${allUsers.length} user(s) · ${pending.length} pending`;

    // Summary chips — one per current role, plus legacy roles only while any
    // account still holds one, so the row disappears once everyone is moved.
    const countOf = key => active.filter(u => u.role === key).length;
    const icons = { owner: 'crown', staff: 'user-tie', pbt: 'user-gear', khl: 'user' };
    let chips = ROLES.map(r =>
        `<div class="user-chip user-chip--${r.key}"><i class="fas fa-${icons[r.key] || 'user'}"></i> ${countOf(r.key)} ${escapeHtml(r.label)}</div>`
    ).join('');
    const legacyTotal = LEGACY_ROLES.reduce((n, r) => n + countOf(r.key), 0);
    if (legacyTotal > 0) {
        chips += `<div class="user-chip user-chip--legacy" title="Role lama — pilihkan role baru untuk mereka"><i class="fas fa-clock-rotate-left"></i> ${legacyTotal} role lama</div>`;
    }
    chips += `<div class="user-chip user-chip--pending"><i class="fas fa-hourglass-half"></i> ${pending.length} Pending</div>`;
    document.getElementById('usersSummary').innerHTML = chips;

    // Pending table
    const pendingBody = document.getElementById('pendingUsersBody');
    if (pending.length === 0) {
        pendingBody.innerHTML = `<tr><td colspan="5" style="text-align:center;padding:20px;color:var(--text-secondary)">Tidak ada pendaftaran menunggu</td></tr>`;
    } else {
        pendingBody.innerHTML = pending.map((u, i) => `
            <tr>
                <td>${i + 1}</td>
                <td data-label="Nama"><strong>${escapeHtml(u.displayName || '—')}</strong></td>
                <td data-label="Email"><span class="user-cell-email" title="${escapeHtml(u.email || '')}">${escapeHtml(u.email || '')}</span></td>
                <td data-label="Waktu daftar"><span class="user-cell-time">${formatUserTime(u.createdAt)}</span></td>
                <td class="col-actions">
                    <div class="row-actions row-actions--labeled">
                        <select class="form-select user-role-select" id="approveRole_${escapeHtml(u.uid)}"
                                aria-label="Role untuk ${escapeHtml(u.email || '')}">
                            ${ROLES.filter(r => r.key !== 'owner').map(r =>
                                `<option value="${r.key}"${r.key === 'khl' ? ' selected' : ''}>${escapeHtml(r.label)}</option>`).join('')}
                        </select>
                        <button class="btn btn-success btn-sm" title="Setujui dengan role terpilih" onclick="approveUser('${escapeHtml(u.uid)}')">
                            <i class="fas fa-check"></i> Setujui
                        </button>
                        <button class="btn btn-secondary btn-sm row-actions__icon" title="Tolak dan hapus pendaftaran" onclick="rejectUser('${escapeHtml(u.uid)}')">
                            <i class="fas fa-xmark" style="color:var(--danger)"></i>
                        </button>
                    </div>
                </td>
            </tr>`).join('');
    }

    // Active table
    const activeBody = document.getElementById('activeUsersBody');
    if (active.length === 0) {
        activeBody.innerHTML = `<tr><td colspan="7" style="text-align:center;padding:20px;color:var(--text-secondary)">Belum ada user aktif</td></tr>`;
    } else {
        activeBody.innerHTML = active.map((u, i) => {
            const isMe = currentUser && u.uid === currentUser.uid;
            const isOwnerRow = u.role === 'owner';
            const legacy = isLegacyRole(u.role);

            // Your own Super Admin row stays locked so you cannot lock yourself
            // out; every other row is changeable, including demoting another
            // Super Admin — a promotion by mistake has to be undoable.
            let roleSelect;
            if (isOwnerRow && isMe) {
                roleSelect = `<span class="badge badge-good"><i class="fas fa-crown"></i> ${escapeHtml(roleLabel('owner'))}</span>`;
            } else {
                // A legacy role is offered only to the account that still holds
                // it, so it is never handed out again but also never silently
                // swapped underneath someone.
                const opts = ROLES.map(r =>
                    `<option value="${r.key}" ${u.role === r.key ? 'selected' : ''}>${escapeHtml(r.label)}</option>`)
                    .concat(legacy ? [`<option value="${u.role}" selected>${escapeHtml(roleLabel(u.role))} (role lama)</option>`] : [])
                    .join('');
                roleSelect = `<select class="form-select user-role-select" title="Role hanya label — atur hak aksesnya lewat tombol Akses"
                           onchange="changeUserRole('${escapeHtml(u.uid)}', this.value)">${opts}</select>`;
            }

            // A new role grants nothing by itself, so an account can sit active
            // with no way in. Say so where the owner will see it.
            const noAccess = !isOwnerRow &&
                ACCESS_AREAS.every(a => effectiveAccess(a.key, u) === 'none');
            const roleNote = legacy
                ? '<div class="user-role-note user-role-note--legacy">Role lama — pilihkan role baru</div>'
                : (noAccess ? '<div class="user-role-note user-role-note--warn">Belum diberi akses apa pun</div>' : '');

            // Which device currently holds this account's single session slot.
            const sess = u.activeSession;
            const live = sess && sess.id && !String(sess.id).startsWith('revoked_');
            const sessionTitle = live
                ? `Keluarkan dari perangkat aktif (${sess.device || 'tidak diketahui'}${sess.startedAt ? ' · masuk ' + formatUserTime(sess.startedAt) : ''})`
                : 'Keluarkan dari semua perangkat';

            return `
            <tr>
                <td>${i + 1}</td>
                <td data-label="Nama"><strong>${escapeHtml(u.displayName || '—')}</strong>${isMe ? ' <span style="font-size:11px;color:var(--text-secondary)">(Anda)</span>' : ''}</td>
                <td data-label="Email"><span class="user-cell-email" title="${escapeHtml(u.email || '')}">${escapeHtml(u.email || '')}</span></td>
                <td data-label="Role">${roleSelect}${roleNote}</td>
                <td data-label="Terakhir diubah"><span class="user-cell-time">${formatUserTime(u.updatedAt)}</span></td>
                <td data-label="Diubah oleh"><span class="user-cell-actor" title="${escapeHtml(u.updatedBy || '')}">${escapeHtml(shortActor(u.updatedBy))}</span></td>
                <td class="col-actions">
                    ${isOwnerRow
                        ? '<span class="user-cell-protected">dilindungi</span>'
                        : `<div class="row-actions row-actions--labeled">
                            <button class="btn btn-secondary btn-sm" title="Atur akses per menu" onclick="openAccessModal('${escapeHtml(u.uid)}')"><i class="fas fa-sliders"></i> Akses</button>
                            <button class="btn btn-secondary btn-sm row-actions__icon" title="${sessionTitle}" onclick="forceSignOutUser('${escapeHtml(u.uid)}')"><i class="fas fa-right-from-bracket"></i></button>
                            <button class="btn btn-secondary btn-sm row-actions__icon" title="Hapus user" onclick="removeUser('${escapeHtml(u.uid)}')"><i class="fas fa-user-minus" style="color:var(--danger)"></i></button>
                           </div>`}
                </td>
            </tr>`;
        }).join('');
    }
}

// ---- Per-user access editor (owner) ----
function openAccessModal(uid) {
    if (!isOwner()) return;
    const user = allUsers.find(u => u.uid === uid);
    if (!user) return;
    document.getElementById('accessUserUid').value = uid;
    document.getElementById('accessUserName').textContent = user.displayName || user.email || uid;
    const labels = { none: 'Tidak ada', view: 'Lihat', edit: 'Edit' };
    let rows = ACCESS_AREAS.map(area => {
        const cur = effectiveAccess(area.key, user);
        const opts = area.levels.map(l => `<option value="${l}" ${l === cur ? 'selected' : ''}>${labels[l]}</option>`).join('');
        return `<div class="access-row">
            <span class="access-row__label">${escapeHtml(area.label)}</span>
            <select class="form-select access-select" data-area="${area.key}">${opts}</select>
        </div>`;
    }).join('');
    // CSV export/import capability (its own level set)
    const csvLabels = { none: 'Tidak ada', export: 'Export saja', full: 'Export + Import' };
    const curCsv = effectiveCsv(user);
    rows += `<div class="access-row">
        <span class="access-row__label">CSV Export/Import</span>
        <select class="form-select access-select" data-area="csv">${CSV_LEVELS.map(l => `<option value="${l}" ${l === curCsv ? 'selected' : ''}>${csvLabels[l]}</option>`).join('')}</select>
    </div>`;
    document.getElementById('accessGrid').innerHTML = rows;
    document.getElementById('accessModal').classList.add('open');
}

function closeAccessModal() {
    document.getElementById('accessModal').classList.remove('open');
}

async function saveAccess() {
    if (!isOwner()) return;
    const uid = document.getElementById('accessUserUid').value;
    const user = allUsers.find(u => u.uid === uid);
    if (!user) return;
    const access = {};
    document.querySelectorAll('#accessGrid .access-select').forEach(sel => {
        access[sel.dataset.area] = sel.value;
    });
    try {
        const fn = cloudFn('updateUserAccess');
        if (!fn) return;
        await fn(uid, access, currentUserDoc.email);
        logEvent({
            action: 'update',
            unitId: uid,
            unitName: `[User] ${user.displayName || user.email}`,
            field: 'access',
            after: ACCESS_AREAS.map(a => `${a.label}:${access[a.key]}`).join(', ')
        });
        showToast(`Akses "${user.displayName || user.email}" diperbarui`, 'success');
        closeAccessModal();
    } catch (e) {
        showToast('Gagal menyimpan akses: ' + e.message, 'error');
    }
}

async function approveUser(uid, role) {
    if (!isOwner()) return;
    const user = allUsers.find(u => u.uid === uid);
    if (!user) return;
    // Role comes from the picker in that user's own pending row.
    if (!role) {
        const sel = document.getElementById('approveRole_' + uid);
        role = sel ? sel.value : 'khl';
    }
    if (!ROLES.some(r => r.key === role) || role === 'owner') {
        showToast('Role tidak dikenal', 'warning');
        return;
    }
    const label = roleLabel(role);
    try {
        await window.cloud.updateUserRole(uid, role, 'active', currentUserDoc.email);
        logEvent({
            action: 'approve',
            unitId: uid,
            unitName: `[User] ${user.displayName || user.email}`,
            field: 'role',
            before: 'pending',
            after: label
        });
        // The new roles carry no access, so approving alone leaves them with an
        // empty app — point the owner straight at the next step.
        showToast(`${user.email} disetujui sebagai ${label} — atur hak aksesnya lewat tombol Akses`, 'success');
    } catch (e) {
        console.error('[users] approve failed:', e);
        showToast('Gagal menyetujui user — ' + e.message, 'error');
    }
}

async function rejectUser(uid) {
    if (!isOwner()) return;
    const user = allUsers.find(u => u.uid === uid);
    if (!user) return;
    if (!confirm(`Tolak dan hapus ${user.email}?\n\nAkun Firebase-nya tetap ada, tetapi ia kehilangan akses ke dashboard.`)) return;
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
        showToast('Gagal menolak user — ' + e.message, 'error');
    }
}

async function changeUserRole(uid, newRole) {
    if (!isOwner()) return;
    const user = allUsers.find(u => u.uid === uid);
    if (!user) return;
    if (user.uid === currentUser.uid && user.role === 'owner') {
        showToast("Anda tidak bisa mengubah role owner milik sendiri.", 'warning');
        renderUsersView();
        return;
    }
    const oldRole = user.role;
    if (oldRole === newRole) return;

    const before = roleLabel(oldRole);
    const after  = roleLabel(newRole);

    // Super Admin is unrestricted by design and can manage every other account,
    // so granting it deserves a deliberate yes.
    if (newRole === 'owner' &&
        !confirm(`Jadikan ${user.email} Super Admin?\n\nSuper Admin punya akses penuh ke seluruh data dan bisa mengubah atau menghapus akun lain, termasuk mencabut akses Anda. Hanya berikan ke orang yang Anda percaya sepenuhnya.`)) {
        renderUsersView();
        return;
    }
    if (oldRole === 'owner' &&
        !confirm(`Turunkan ${user.email} dari Super Admin menjadi ${after}?\n\nSetelah ini akses mereka mengikuti pengaturan per-menu, dan role baru tidak memberi akses apa pun sampai Anda mengaturnya.`)) {
        renderUsersView();
        return;
    }

    try {
        await window.cloud.updateUserRole(uid, newRole, 'active', currentUserDoc.email);
        logEvent({
            action: 'role-change',
            unitId: uid,
            unitName: `[User] ${user.displayName || user.email}`,
            field: 'role',
            before,
            after
        });
        const needsAccess = newRole !== 'owner' &&
            ACCESS_AREAS.every(a => effectiveAccess(a.key, { ...user, role: newRole }) === 'none');
        showToast(needsAccess
            ? `${user.email} sekarang ${after} — belum punya akses, atur lewat tombol Akses`
            : `${user.email} sekarang ${after}`, 'success');
    } catch (e) {
        showToast('Gagal mengubah role — ' + e.message, 'error');
    }
}

// Owner-initiated remote sign-out. Writes a session id no device can match,
// so whoever is signed in is dropped on their next snapshot.
async function forceSignOutUser(uid) {
    if (!isOwner()) return;
    const user = allUsers.find(u => u.uid === uid);
    if (!user) return;
    if (currentUser && uid === currentUser.uid) {
        showToast('Gunakan menu akun untuk keluar dari perangkat ini', 'warning');
        return;
    }
    const sess = user.activeSession;
    const where = (sess && sess.device) ? `\n\nPerangkat aktif: ${sess.device}` : '';
    if (!confirm(`Keluarkan ${user.email} dari semua perangkat?${where}\n\nMereka harus masuk lagi. Data mereka tidak terhapus.`)) return;
    try {
        const fn = cloudFn('revokeUserSession');
        if (!fn) return;
        await fn(uid, currentUserDoc.email);
        logEvent({
            action: 'update',
            unitId: uid,
            unitName: `[User] ${user.displayName || user.email}`,
            field: 'Sesi',
            before: (sess && sess.device) || 'aktif',
            after: 'dikeluarkan'
        });
        showToast(`${user.email} dikeluarkan dari semua perangkat`, 'success');
    } catch (e) {
        showToast('Gagal mengakhiri sesi — ' + e.message, 'error');
    }
}

async function removeUser(uid) {
    if (!isOwner()) return;
    const user = allUsers.find(u => u.uid === uid);
    if (!user) return;
    if (user.role === 'owner') { showToast('Owner tidak bisa dihapus', 'warning'); return; }
    if (!confirm(`Hapus ${user.email} dari dashboard?\n\nAkun Firebase-nya tetap ada, tetapi ia kehilangan seluruh akses.`)) return;
    try {
        await window.cloud.deleteUserDoc(uid);
        logEvent({
            action: 'remove',
            unitId: uid,
            unitName: `[User] ${user.displayName || user.email}`,
            before: user.role,
            after: 'removed'
        });
        showToast(`${user.email} dihapus`, 'success');
    } catch (e) {
        showToast('Gagal menghapus user — ' + e.message, 'error');
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

// ============================================================
// TEAM OPERATIONS — members, shift schedule, daily work logs
// ------------------------------------------------------------
// All three collections are cloud-only (like userCategories and
// damageComponents): there is no legacy local data to migrate, so Firestore
// stays the single source of truth and its IndexedDB persistence covers
// offline reads. Each part is gated by its own access area: teamShift for the
// schedule, teamLog for the daily reports, teamMembers for the roster.
// ============================================================

const SHIFT_TYPES = [
    { key: 'pagi',  label: 'Pagi',  hours: '07:00–15:00' },
    { key: 'siang', label: 'Siang', hours: '15:00–23:00' },
    { key: 'malam', label: 'Malam', hours: '23:00–07:00' },
    { key: 'libur', label: 'Libur', hours: '—' }
];
const SHIFT_LABEL = SHIFT_TYPES.reduce((m, s) => { m[s.key] = s.label; return m; }, {});
const DAY_NAMES = ['Min', 'Sen', 'Sel', 'Rab', 'Kam', 'Jum', 'Sab'];
const MONTH_NAMES = ['Jan', 'Feb', 'Mar', 'Apr', 'Mei', 'Jun', 'Jul', 'Agu', 'Sep', 'Okt', 'Nov', 'Des'];

let teamTab = 'shift';      // 'shift' | 'worklog'
let teamWeekStart = null;   // ISO date of the Monday of the visible week

// ---- Week helpers (all local-date, via parseLocalDate/toISODate) ----
function startOfWeekISO(dateLike) {
    const d = parseLocalDate(dateLike) || new Date();
    d.setDate(d.getDate() - ((d.getDay() + 6) % 7)); // Monday = 0
    return toISODate(d);
}

function addDaysISO(iso, n) {
    const d = parseLocalDate(iso);
    if (!d) return iso;
    d.setDate(d.getDate() + n);
    return toISODate(d);
}

function weekDates(startIso) {
    return Array.from({ length: 7 }, (_, i) => addDaysISO(startIso, i));
}

function dayLabel(iso) {
    const d = parseLocalDate(iso);
    return d ? `${DAY_NAMES[d.getDay()]} ${d.getDate()}` : iso;
}

function weekRangeLabel(startIso) {
    const a = parseLocalDate(startIso);
    const b = parseLocalDate(addDaysISO(startIso, 6));
    if (!a || !b) return '';
    const sameMonth = a.getMonth() === b.getMonth() && a.getFullYear() === b.getFullYear();
    return sameMonth
        ? `${a.getDate()} – ${b.getDate()} ${MONTH_NAMES[b.getMonth()]} ${b.getFullYear()}`
        : `${a.getDate()} ${MONTH_NAMES[a.getMonth()]} – ${b.getDate()} ${MONTH_NAMES[b.getMonth()]} ${b.getFullYear()}`;
}

// ---- Member helpers ----
function generateMemberId() {
    return 'tm_' + Date.now() + '_' + Math.random().toString(36).substr(2, 5);
}

function generateWorkLogId() {
    return 'wl_' + Date.now() + '_' + Math.random().toString(36).substr(2, 5);
}

// ---- Field documentation photos ----
// A work log carries several photos in one Firestore document, so each gets a
// far tighter budget than the single damage photo: 4 × 200KB leaves headroom
// under the 1MB document limit even with a long description alongside.
const WORKLOG_PHOTO_MAX = 4;
const WORKLOG_PHOTO_OPTS = { maxDim: 1024, maxBytes: 200 * 1024, quality: 0.7 };

// Above this, a shift that wraps past midnight is more likely a typo than a
// real night shift, so we ask. Not a hard limit — a genuine long shift must
// still be recordable.
const WORKLOG_LONG_SHIFT_MIN = 16 * 60;
const WORKLOG_PHOTOS_TOTAL_BYTES = 800 * 1024;

// Photos live in their own collection, one document per report, fetched only
// when somebody opens them. They used to sit inside the work log itself — and
// since subscribeWorkLogs streams the whole collection with no limit(), that
// meant every device re-downloaded every photo ever taken, on every app open.
// Measured: one field photo compresses to ~96KB, so an eight-person team was
// pulling roughly 33MB a month back down the wire for nothing.
//
// The report keeps only photoCount, so the table can still show how many there
// are without loading a single byte of image.
const _wlPhotoCache = new Map();   // logId -> data URLs

function workLogPhotoCount(rec) {
    if (!rec) return 0;
    // Reports written before the split still carry their photos inline.
    // An empty array means the inline copy was cleared on migration, so fall
    // through to photoCount rather than reporting zero.
    if (Array.isArray(rec.photos) && rec.photos.length) return rec.photos.length;
    return Number(rec.photoCount) || 0;
}

// Inline (old shape) → cache → one getDoc. Rejects when offline and the
// document was never cached; callers surface that rather than spinning.
async function loadWorkLogPhotos(id) {
    const rec = workLogs.find(w => w.id === id);
    if (rec && Array.isArray(rec.photos) && rec.photos.length) return rec.photos.slice();
    if (_wlPhotoCache.has(id)) return _wlPhotoCache.get(id).slice();
    if (!window.cloud || !window.cloud.getWorkLogPhotos) return [];
    const photos = await window.cloud.getWorkLogPhotos(id);
    _wlPhotoCache.set(id, photos);
    return photos.slice();
}

// ---- Multiple units per report ----
// Reports used to carry one unit (unitId/unitName/sn). They now carry a list,
// and this reads either shape so existing rows keep working.
function workLogUnits(rec) {
    if (rec && Array.isArray(rec.units) && rec.units.length) return rec.units;
    if (rec && rec.unitId) return [{ id: rec.unitId, name: rec.unitName || '', sn: rec.sn || '' }];
    return [];
}

// Display names, preferring the live unit record over the stored copy so a
// renamed unit reads correctly on old reports.
function workLogUnitNames(rec) {
    return workLogUnits(rec).map(u => {
        const live = liveUnitFor({ unitId: u.id, sn: u.sn });
        return live ? (live.name || '') : (u.name || '');
    }).filter(Boolean);
}

// ---- Report approval ----
// Three states, because two would leave a rejected report with nowhere to go:
// the person who filed it would never learn what to fix.
const APPROVAL_STATES = {
    pending:  { label: 'Menunggu',     tone: 'warning' },
    approved: { label: 'Disetujui',    tone: 'success' },
    revision: { label: 'Perlu Revisi', tone: 'danger'  }
};

// Reports written before approval existed carry no field; they have genuinely
// never been checked, so they read as pending rather than as approved.
function workLogApproval(w) {
    const a = w && w.approval;
    return APPROVAL_STATES[a] ? a : 'pending';
}

function canApproveWorkLogs() {
    return hasAccess('teamLogApprove', 'edit');
}

// Checking your own work is not a check. Blocked on the account that filed the
// report, not on the team member it is about — an admin may legitimately file
// on someone else's behalf. Owners are exempt so a one-person setup is not
// deadlocked, and older reports have no author recorded, so they pass.
function canApproveThisLog(w) {
    if (!canApproveWorkLogs()) return false;
    if (isOwner()) return true;
    return !(w && w.createdByUid && currentUser && w.createdByUid === currentUser.uid);
}

// ---- Paddock area ----
// Free text with suggestions gathered from what has already been entered, the
// same approach as company names.
function allPaddocks() {
    const seen = new Map();
    workLogs.forEach(w => {
        const p = ((w && w.paddock) || '').trim();
        if (p && !seen.has(p.toLowerCase())) seen.set(p.toLowerCase(), p);
    });
    return [...seen.values()].sort((a, b) => a.localeCompare(b));
}

function activeMembers() {
    return teamMembers.filter(m => m.active !== false);
}

// ---- Companies ----
// Free text on the member record with suggestions, rather than a fixed list:
// a third company can be added by typing it, without a code change.
const DEFAULT_COMPANIES = ['PT. Global Papua Abadi', 'PT. Murni Nusantara Mandiri'];
const NO_COMPANY = '(Tanpa perusahaan)';

function companyOf(m) {
    return ((m && m.company) || '').trim();
}

// The two seeded names plus anything already typed, de-duplicated
// case-insensitively so "PT. Global" and "pt. global" do not split a group.
function allCompanies() {
    const seen = new Map();
    DEFAULT_COMPANIES.forEach(c => seen.set(c.toLowerCase(), c));
    teamMembers.forEach(m => {
        const c = companyOf(m);
        if (c && !seen.has(c.toLowerCase())) seen.set(c.toLowerCase(), c);
    });
    return [...seen.values()].sort((a, b) => a.localeCompare(b));
}

// Active members grouped by company, named companies first (alphabetical) and
// anyone without one collected at the end rather than silently hidden.
function membersByCompany() {
    const groups = new Map();
    activeMembers().forEach(m => {
        const key = companyOf(m) || NO_COMPANY;
        if (!groups.has(key)) groups.set(key, []);
        groups.get(key).push(m);
    });
    return [...groups.entries()].sort((a, b) => {
        if (a[0] === NO_COMPANY) return 1;
        if (b[0] === NO_COMPANY) return -1;
        return a[0].localeCompare(b[0]);
    });
}

// Company for a work log: the member's current one, falling back to the value
// stored on the record so rows survive a member being deleted or moved.
function companyOfRecord(rec) {
    const m = rec && rec.memberId ? memberById(rec.memberId) : null;
    return m ? companyOf(m) : ((rec && rec.company) || '').trim();
}

function memberById(id) {
    return teamMembers.find(m => m.id === id) || null;
}

// Name for display: live member record first, then the name denormalized onto
// the record, so a deleted member still reads correctly in old rows.
function memberNameOf(rec) {
    const m = rec && rec.memberId ? memberById(rec.memberId) : null;
    return m ? m.name : ((rec && rec.memberName) || '(anggota dihapus)');
}

// ---- Cloud snapshots ----
function applyCloudTeamMembersSnapshot(list) {
    teamMembers = (list || []).slice().sort((a, b) =>
        (a.name || '').localeCompare(b.name || ''));
    // Company suggestions and the report filter are derived from the roster,
    // so they have to be rebuilt whenever it changes.
    if (currentView === 'team') { populateWorkLogFilters(); renderTeamView(); }
    const modal = document.getElementById('teamMembersModal');
    if (modal && modal.classList.contains('open')) renderTeamMembersList();
}

function applyCloudShiftsSnapshot(list) {
    reportShiftOverwrites(list);
    teamShifts = list || [];
    if (currentView === 'team' && teamTab === 'shift') renderShiftGrid();
}

function applyCloudWorkLogsSnapshot(list) {
    workLogs = (list || []).slice().sort((a, b) =>
        String(b.date || '').localeCompare(String(a.date || '')) ||
        ((b.createdAt || 0) - (a.createdAt || 0)));
    if (currentView === 'team') {
        populateWorkLogFilters();
        if (teamTab === 'worklog') renderWorkLogTable();
    }
    scheduleDecisionRefresh();
    migrateWorkLogPhotosIfNeeded();
}

// One-shot migration: move photos that still sit inside a work log out to
// workLogPhotos/{id}. Same shape as applyLicenseDatesIfNeeded() above —
// owner only, guarded by a localStorage flag so it never runs twice.
//
// Order matters: the photo document is written FIRST and the inline copy is
// cleared only after that write is acknowledged. Interrupted halfway, the
// worst case is a report that has its photos in both places — which reads
// correctly either way — never one that has them in neither.
//
// Done now, while the feature is days old and the data is small. Every month
// this waits, the migration gets more expensive and the daily waste gets
// bigger.
let _wlPhotoMigrationRunning = false;
function migrateWorkLogPhotosIfNeeded() {
    if (_wlPhotoMigrationRunning) return;
    if (!isOwner || !isOwner()) return;
    if (localStorage.getItem(WL_PHOTOS_SPLIT_KEY) === '1') return;
    if (!window.cloud || !window.cloud.saveWorkLogPhotos) return;
    if (!Array.isArray(workLogs) || workLogs.length === 0) return;

    const pending = workLogs.filter(w => Array.isArray(w.photos) && w.photos.length);
    if (pending.length === 0) {
        localStorage.setItem(WL_PHOTOS_SPLIT_KEY, '1');
        return;
    }
    if (!navigator.onLine) return;   // retried on the next snapshot

    _wlPhotoMigrationRunning = true;
    console.log(`[wl-photos] moving photos out of ${pending.length} work logs...`);
    (async () => {
        let moved = 0;
        for (const w of pending) {
            const photos = w.photos.slice();
            await window.cloud.saveWorkLogPhotos(w.id, photos);
            _wlPhotoCache.set(w.id, photos);
            await window.cloud.saveWorkLog({ ...w, photos: [], photoCount: photos.length });
            moved++;
        }
        return moved;
    })().then(moved => {
        localStorage.setItem(WL_PHOTOS_SPLIT_KEY, '1');
        try {
            logEvent({
                action: 'migrate',
                unitName: '-',
                field: 'foto laporan',
                after: `${moved} laporan dipindah ke koleksi workLogPhotos`
            });
        } catch (e) {}
        console.log(`[wl-photos] done — ${moved} work logs migrated`);
    }).catch(err => {
        // No flag set, so the next snapshot tries again from where it stopped.
        console.error('[wl-photos] migration failed:', err);
        if (err && err.code === 'permission-denied') showTeamRulesBanner();
    }).finally(() => {
        _wlPhotoMigrationRunning = false;
    });
}

function showTeamRulesBanner() {
    const slot = document.querySelector('.team-rules-slot');
    if (!slot || slot.querySelector('.category-rules-banner')) return;
    const banner = document.createElement('div');
    banner.className = 'category-rules-banner';
    banner.innerHTML = `
        <strong><i class="fas fa-triangle-exclamation"></i> Firestore rules memblokir data Tim.</strong>
        <p>Rules proyek Anda belum mengizinkan akses ke koleksi <code>teamMembers</code>,
        <code>shifts</code>, dan <code>workLogs</code>. Publish ulang file
        <code>firestore.rules</code> dari repo ini di
        <em>Firebase Console → Firestore → Rules</em>, lalu muat ulang halaman.</p>`;
    slot.appendChild(banner);
}

// ---- View shell ----
// One map instead of the ternary that used to be here. That ternary read
// `tab === 'shift' ? canShift : canLog`, so anything that was not 'shift' fell
// through to canLog — correct by accident for two tabs, and silently wrong for
// the next one.
const TEAM_TABS = {
    shift:   { area: 'teamShift', panel: 'teamShiftPanel',   render: () => renderShiftGrid() },
    worklog: { area: 'teamLog',   panel: 'teamWorkLogPanel', render: () => renderWorkLogTable() },
    leave:   { area: 'teamLog',   panel: 'teamLeavePanel',   render: () => renderLeaveTable() }
};

function switchTeamTab(tab) {
    teamTab = TEAM_TABS[tab] ? tab : 'shift';
    renderTeamView();
}

function renderTeamView() {
    const canTab = key => {
        const t = TEAM_TABS[key];
        return !!t && hasAccess(t.area, 'view');
    };
    const canMembers = hasAccess('teamMembers', 'view');
    const anyTab = Object.keys(TEAM_TABS).some(canTab);

    // Land on a tab the user can actually open — the page is reachable through
    // any one of the areas, so the stored tab may not be permitted.
    if (!canTab(teamTab)) {
        teamTab = Object.keys(TEAM_TABS).find(canTab) || teamTab;
    }

    // Scoped to this page. Kotak Keputusan and Gudang reuse the .team-tab
    // class for styling, and an unscoped sweep hid every one of their tabs the
    // moment anyone opened Tim — canTab() knows nothing about 'inbox' or
    // 'devices', so it answered false for all of them.
    document.querySelectorAll('#viewTeam .team-tab').forEach(btn => {
        const key = btn.dataset.tab;
        const allowed = canTab(key);
        btn.style.display = allowed ? '' : 'none';
        const on = allowed && key === teamTab;
        btn.classList.toggle('active', on);
        btn.setAttribute('aria-selected', on ? 'true' : 'false');
    });

    const membersBtn = document.getElementById('btnTeamMembers');
    if (membersBtn) membersBtn.style.display = canMembers ? '' : 'none';

    Object.entries(TEAM_TABS).forEach(([key, t]) => {
        const panel = document.getElementById(t.panel);
        if (panel) panel.style.display = (canTab(key) && key === teamTab) ? '' : 'none';
    });

    // Reachable via Daftar Anggota alone — no tab is permitted, so say so
    // instead of showing blank panels.
    const emptyPanel = document.getElementById('teamNoPanel');
    if (emptyPanel) emptyPanel.style.display = anyTab ? 'none' : '';

    const active = TEAM_TABS[teamTab];
    if (active && canTab(teamTab)) active.render();
}

// ============================================================
// SHIFT SCHEDULE
// ============================================================

function shiftFor(memberId, date) {
    const rec = teamShifts.find(s => s.id === `${date}_${memberId}`);
    return rec ? rec.shift : '';
}

// "Diisi Budi · 14 Sep 09:12" for the cell tooltip. Two people can edit the
// same cell and the last one wins, so who set it is worth being able to see
// without opening History.
function shiftSetByLabel(memberId, date) {
    const rec = teamShifts.find(s => s.id === `${date}_${memberId}`);
    if (!rec || !rec.updatedBy) return '';
    const when = rec.updatedAt ? new Date(rec.updatedAt).toLocaleString('id-ID',
        { day: 'numeric', month: 'short', hour: '2-digit', minute: '2-digit' }) : '';
    return `Diisi ${rec.updatedBy}${when ? ' · ' + when : ''}`;
}

// ---- Shift subscription window ----
// One document per person per day, forever: eight people make ~2,900 a year,
// ~8,800 by year three, and every one of them was pulled down to draw a single
// week. The listener now covers a rolling window instead, and widens itself if
// somebody pages back past the edge — so the schedule still works however far
// back they go, it just fetches that range when they ask for it.
const SHIFT_WINDOW_DAYS = 120;   // roughly four months back
let _shiftWindowStart = '';

function shiftWindowFor(weekStart) {
    const byToday = addDaysISO(toISODate(), -SHIFT_WINDOW_DAYS);
    const byWeek = addDaysISO(weekStart || toISODate(), -28);
    return byWeek < byToday ? byWeek : byToday;
}

function startShiftsSubscription(weekStart) {
    if (!window.cloud || !window.cloud.subscribeShifts) return;
    _shiftWindowStart = shiftWindowFor(weekStart || teamWeekStart);
    if (cloudShiftsUnsub) { try { cloudShiftsUnsub(); } catch (_) {} cloudShiftsUnsub = null; }
    cloudShiftsUnsub = window.cloud.subscribeShifts(
        applyCloudShiftsSnapshot,
        err => {
            console.warn('[cloud] shifts offline:', err && err.code);
            if (err && err.code === 'permission-denied') showTeamRulesBanner();
        },
        _shiftWindowStart
    );
}

// Called before rendering a week: if it falls outside what the listener
// covers, widen and resubscribe. Without this the grid would quietly render
// an empty week that actually has shifts in it.
function ensureShiftWindowCovers(weekStart) {
    if (!_shiftWindowStart || !weekStart) return;
    if (weekStart >= _shiftWindowStart) return;
    console.log(`[shifts] widening window back to cover ${weekStart}`);
    startShiftsSubscription(weekStart);
}

function shiftWeekShift(delta) {
    if (!teamWeekStart) teamWeekStart = startOfWeekISO(toISODate());
    teamWeekStart = addDaysISO(teamWeekStart, delta * 7);
    ensureShiftWindowCovers(teamWeekStart);
    renderShiftGrid();
}

function shiftWeekToday() {
    teamWeekStart = startOfWeekISO(toISODate());
    renderShiftGrid();
}

function renderShiftGrid() {
    if (!teamWeekStart) teamWeekStart = startOfWeekISO(toISODate());
    const dates = weekDates(teamWeekStart);
    const today = toISODate();
    const canEdit = hasAccess('teamShift', 'edit');

    const label = document.getElementById('shiftWeekLabel');
    if (label) label.textContent = weekRangeLabel(teamWeekStart);

    const head = document.getElementById('shiftHead');
    if (head) {
        head.innerHTML = `<tr><th class="shift-grid__member">Anggota</th>${
            dates.map(d => `<th class="${d === today ? 'is-today' : ''}">${escapeHtml(dayLabel(d))}</th>`).join('')
        }</tr>`;
    }

    const body = document.getElementById('shiftBody');
    const foot = document.getElementById('shiftFoot');
    if (!body) return;

    const members = activeMembers();
    if (members.length === 0) {
        body.innerHTML = `<tr><td colspan="8" style="text-align:center;padding:24px;color:var(--text-secondary)">
            Belum ada anggota tim aktif. Klik <strong>Kelola Anggota</strong> untuk menambahkan.
        </td></tr>`;
        if (foot) foot.innerHTML = '';
        return;
    }

    // Per-day duty counts for a set of members; 'libur' is time off, not duty.
    const dutyCells = (list, extraClass) => dates.map(d => {
        const counts = SHIFT_TYPES.filter(s => s.key !== 'libur')
            .map(s => ({ s, n: list.filter(m => shiftFor(m.id, d) === s.key).length }));
        const working = counts.reduce((a, c) => a + c.n, 0);
        const detail = counts.map(c => `${c.s.label} ${c.n}`).join(' · ');
        return `<td class="${extraClass}${d === today ? ' is-today' : ''}" title="${escapeHtml(detail)}">
            <strong>${working}</strong> <span class="shift-foot__detail">${escapeHtml(detail)}</span>
        </td>`;
    }).join('');

    const memberRow = m => {
        const cells = dates.map(d => {
            const cur = shiftFor(m.id, d);
            const cls = `shift-cell${cur ? ' shift-cell--' + cur : ''}${d === today ? ' is-today' : ''}`;
            const by = shiftSetByLabel(m.id, d);
            // Izin/sakit yang sudah disetujui ditandai DI SAMPING pilihan
            // shift, tidak menggantikannya. Menulis otomatis ke koleksi shifts
            // akan menimpa jadwal yang sudah diisi — dan koleksi itu berjendela
            // 120 hari, jadi izin yang lebih lama tidak punya sel untuk ditulisi
            // sama sekali. Jadwalnya tetap milik penyusun jadwal.
            const lv = leaveOn(m.id, d);
            const lvBadge = lv
                ? `<span class="shift-leave shift-leave--${escapeHtml(lv.type || 'izin')}">${escapeHtml(leaveTypeLabel(lv))}</span>`
                : '';
            const lvNote = lv
                ? `${leaveTypeLabel(lv)} ${leaveRangeLabel(lv)}${lv.reason ? ' — ' + lv.reason : ''}`
                : '';
            const tip = [by, lvNote].filter(Boolean).join(' · ');
            const byAttr = tip ? ` title="${escapeHtml(tip)}"` : '';
            if (!canEdit) {
                return `<td class="${cls}"${byAttr}>${lvBadge}${cur
                    ? `<span class="shift-badge shift-badge--${cur}">${escapeHtml(SHIFT_LABEL[cur] || cur)}</span>`
                    : '<span class="shift-empty">—</span>'}</td>`;
            }
            const opts = ['<option value="">—</option>'].concat(
                SHIFT_TYPES.map(s =>
                    `<option value="${s.key}"${s.key === cur ? ' selected' : ''}>${escapeHtml(s.label)}</option>`)
            ).join('');
            return `<td class="${cls}"${byAttr}>${lvBadge}
                <select class="shift-select shift-select--${cur || 'none'}"
                        aria-label="Shift ${escapeHtml(m.name)} tanggal ${escapeHtml(d)}"
                        onchange="setShift('${escapeHtml(m.id)}','${escapeHtml(d)}',this.value)">${opts}</select>
            </td>`;
        }).join('');
        return `<tr>
            <th scope="row" class="shift-grid__member">
                <strong>${escapeHtml(m.name)}</strong>
                ${m.jobTitle ? `<span class="shift-grid__job" title="${escapeHtml(m.jobTitle)}">${escapeHtml(m.jobTitle)}</span>` : ''}
            </th>${cells}
        </tr>`;
    };

    // One block per company: a heading row carrying that company's own duty
    // counts, then its members.
    body.innerHTML = membersByCompany().map(([company, list]) => `
        <tr class="shift-group">
            <th scope="row" class="shift-grid__member shift-group__name">
                ${escapeHtml(company)}
                <span class="shift-group__count">${list.length} orang</span>
            </th>${dutyCells(list, 'shift-group__cell')}
        </tr>
        ${list.map(memberRow).join('')}`).join('');

    if (foot) {
        foot.innerHTML = `<tr><th scope="row" class="shift-grid__member">Total bertugas</th>${
            dutyCells(members, '')
        }</tr>`;
    }
}

// Cells this device has written and not yet seen confirmed. See setShift.
const _shiftPendingWrites = new Map();
const SHIFT_CLAIM_MS = 60000;

function shiftActorName() {
    if (!currentUser) return '';
    return (currentUserDoc && currentUserDoc.displayName)
        || currentUser.displayName
        || (currentUser.email || '').split('@')[0]
        || '';
}

// Compare the snapshot against what we just wrote. Anything that came back
// different, from somebody else, is an overwrite the person needs to be told
// about — silently flipping the grid under them is how a supervisor ends up
// believing a shift is set when it is not.
function reportShiftOverwrites(list) {
    if (_shiftPendingWrites.size === 0) return;
    const now = Date.now();
    const byId = new Map((list || []).map(s => [s.id, s]));
    const myUid = (currentUser && currentUser.uid) || '';
    _shiftPendingWrites.forEach((mine, id) => {
        if (now - mine.at > SHIFT_CLAIM_MS) { _shiftPendingWrites.delete(id); return; }
        if (!byId.has(id) && mine.shift === '') { _shiftPendingWrites.delete(id); return; }
        const server = byId.get(id);
        const serverShift = server ? (server.shift || '') : '';
        if (serverShift === mine.shift) { _shiftPendingWrites.delete(id); return; }
        // Different value — but only shout if somebody else put it there.
        // Our own pending write simply has not landed yet.
        if (server && server.updatedByUid && server.updatedByUid !== myUid) {
            const who = server.updatedBy || 'orang lain';
            const label = serverShift ? (SHIFT_LABEL[serverShift] || serverShift) : 'kosong';
            showToast(`Jadwal ${mine.memberName} ${mine.date} baru diubah ${who} menjadi ${label}`, 'warning');
            _shiftPendingWrites.delete(id);
        }
    });
}

function setShift(memberId, date, shiftKey) {
    if (!requireEdit('teamShift')) { renderShiftGrid(); return; }
    const m = memberById(memberId);
    if (!m) return;

    const id = `${date}_${memberId}`;
    const prev = teamShifts.find(s => s.id === id) || null;
    if ((prev ? prev.shift : '') === (shiftKey || '')) return;

    const before = prev ? (SHIFT_LABEL[prev.shift] || prev.shift) : '—';
    const after = shiftKey ? (SHIFT_LABEL[shiftKey] || shiftKey) : '—';

    // Optimistic update so the grid responds instantly. Replace rather than
    // mutate, so the rollback snapshot stays a valid earlier state.
    const snapshot = teamShifts.slice();
    teamShifts = teamShifts.filter(s => s.id !== id);
    const rec = shiftKey ? {
        id, date, memberId,
        memberName: m.name,
        shift: shiftKey,
        createdAt: prev ? (prev.createdAt || Date.now()) : Date.now(),
        updatedAt: Date.now(),
        // Who last touched this cell. The shift document id is deterministic
        // (`${date}_${memberId}`), so two supervisors editing the same person
        // on the same day write the same document and the later one wins with
        // no warning to either. A real merge needs transactions and is bigger
        // than the problem; knowing you were overwritten is what was missing.
        updatedBy: shiftActorName(),
        updatedByUid: (currentUser && currentUser.uid) || ''
    } : null;
    if (rec) teamShifts.push(rec);
    // Remember what we just sent, so the snapshot coming back can be compared
    // against it. Cleared as soon as it is checked, or after SHIFT_CLAIM_MS.
    _shiftPendingWrites.set(id, {
        shift: shiftKey || '', at: Date.now(),
        memberName: m.name, date
    });
    renderShiftGrid();

    if (!window.cloud || !window.cloud.saveShift) {
        showToast('Cloud belum siap — perubahan shift belum tersimpan', 'warning');
        teamShifts = snapshot;
        renderShiftGrid();
        return;
    }

    cloudWrite(
        { action: 'update', unitName: `[Tim] ${m.name}`, field: `Shift ${date}`, before, after },
        rec ? cloudCall('saveShift', rec) : cloudCall('deleteShift', id),
        null,
        err => {
            console.error('[team] shift save failed:', err);
            _shiftPendingWrites.delete(id);
            teamShifts = snapshot;
            renderShiftGrid();
            if (err && err.code === 'permission-denied') showTeamRulesBanner();
            showToast('Gagal menyimpan shift — perubahan dikembalikan', 'error');
        }
    );
}

function exportShiftCSV() {
    if (!canCsv('export')) return;
    const members = activeMembers();
    if (members.length === 0) { showToast('Belum ada anggota tim untuk diexport', 'warning'); return; }
    const dates = weekDates(teamWeekStart || startOfWeekISO(toISODate()));
    const headers = ['Perusahaan', 'Anggota', 'Jabatan', ...dates.map(d => `${dayLabel(d)} (${d})`)];
    // Exported in the same company order the grid shows, so the file reads the
    // same way as the screen.
    const rows = membersByCompany().flatMap(([company, list]) => list.map(m => [
        company === NO_COMPANY ? '' : company,
        m.name || '', m.jobTitle || '',
        ...dates.map(d => {
            const s = shiftFor(m.id, d);
            return s ? (SHIFT_LABEL[s] || s) : '';
        })
    ]));
    const csv = toCSV(headers, rows);
    const blob = new Blob(['﻿' + csv], { type: 'text/csv;charset=utf-8;' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = `jadwal_shift_${dates[0]}_sd_${dates[6]}.csv`;
    a.click();
    URL.revokeObjectURL(url);
    showToast(`Export jadwal shift ${members.length} anggota ke CSV`, 'success');
}

// ============================================================
// TEAM MEMBERS (master list — may include people without an account)
// ============================================================

function openTeamMembersModal() {
    renderTeamMembersList();
    document.getElementById('teamMembersModal').classList.add('open');
}

function closeTeamMembersModal() {
    document.getElementById('teamMembersModal').classList.remove('open');
}

function renderTeamMembersList() {
    const list = document.getElementById('teamMembersList');
    if (!list) return;
    const canEdit = hasAccess('teamMembers', 'edit');
    // Keep the shared company suggestions current — a company typed on one row
    // should be offered on the next.
    const dl = document.getElementById('companyList');
    if (dl) dl.innerHTML = allCompanies().map(c => `<option value="${escapeHtml(c)}"></option>`).join('');
    if (teamMembers.length === 0) {
        list.innerHTML = `<li class="category-empty">Belum ada anggota tim.</li>`;
        return;
    }
    list.innerHTML = teamMembers.map(m => `
        <li class="category-item member-item${m.active === false ? ' is-inactive' : ''}">
            <span class="category-item__name member-item__info">
                <strong>${escapeHtml(m.name)}</strong>
                ${(m.jobTitle || m.active === false) ? `<span class="member-item__meta">
                    ${m.jobTitle ? `<span class="member-item__job" title="${escapeHtml(m.jobTitle)}">${escapeHtml(m.jobTitle)}</span>` : ''}
                    ${m.active === false ? '<span class="member-item__off">Nonaktif</span>' : ''}
                </span>` : ''}
                ${canEdit
                    ? `<input class="form-input member-item__company" list="companyList"
                              value="${escapeHtml(companyOf(m))}" placeholder="Perusahaan…" maxlength="80"
                              aria-label="Perusahaan ${escapeHtml(m.name)}"
                              onchange="setMemberCompany('${escapeHtml(m.id)}', this.value)">`
                    : `<span class="member-item__job">${escapeHtml(companyOf(m) || NO_COMPANY)}</span>`}
            </span>
            ${canEdit ? `<span class="row-actions row-actions--labeled">
                <button class="btn btn-secondary btn-sm" onclick="toggleTeamMember('${escapeHtml(m.id)}')">
                    ${m.active === false ? 'Aktifkan' : 'Nonaktifkan'}
                </button>
                <button class="btn btn-secondary btn-sm row-actions__icon" title="Hapus anggota" aria-label="Hapus ${escapeHtml(m.name)}"
                        onclick="deleteTeamMember('${escapeHtml(m.id)}')">
                    <i class="fas fa-trash" style="color:var(--danger)"></i>
                </button>
            </span>` : ''}
        </li>`).join('');
}

// Change one member's company from the roster list. Company is the only field
// editable in place — it is the one that has to be set for people who were
// added before companies existed, and retyping a name would orphan their
// shifts, which are keyed by member id.
function setMemberCompany(id, value) {
    if (!requireEdit('teamMembers')) { renderTeamMembersList(); return; }
    const m = memberById(id);
    if (!m) return;
    const company = (value || '').trim();
    const before = companyOf(m);
    if (before === company) return;

    // Optimistic, like setShift: replace the record rather than mutate it, so
    // the rollback snapshot stays a valid earlier state. Without this the grid
    // and the company suggestions would lag until the Firestore snapshot lands,
    // and a second edit would compare against a stale value.
    const snapshot = teamMembers.slice();
    const rec = { ...m, company, updatedAt: Date.now() };
    teamMembers = teamMembers.map(x => (x.id === id ? rec : x));
    renderTeamMembersList();
    if (currentView === 'team') renderTeamView();

    cloudWrite(
        { action: 'update', unitName: `[Tim] ${m.name}`, field: 'Perusahaan',
          before: before || '—', after: company || '—' },
        cloudCall('saveTeamMember', rec),
        null,
        err => {
            console.error('[team] company save failed:', err);
            teamMembers = snapshot;
            renderTeamMembersList();
            if (currentView === 'team') renderTeamView();
            if (err && err.code === 'permission-denied') showTeamRulesBanner();
            showToast('Gagal menyimpan perusahaan — perubahan dikembalikan', 'error');
        }
    );
}

function addTeamMember(event) {
    event.preventDefault();
    if (!requireEdit('teamMembers')) return;
    const nameEl = document.getElementById('newMemberName');
    const jobEl = document.getElementById('newMemberJob');
    const compEl = document.getElementById('newMemberCompany');
    const name = (nameEl.value || '').trim();
    const jobTitle = (jobEl.value || '').trim();
    const company = ((compEl && compEl.value) || '').trim();
    if (!name) { showToast('Nama anggota tidak boleh kosong', 'warning'); return; }
    if (teamMembers.some(m => (m.name || '').toLowerCase() === name.toLowerCase())) {
        showToast(`Anggota "${name}" sudah ada`, 'warning');
        return;
    }
    const rec = { id: generateMemberId(), name, jobTitle, company, active: true, createdAt: Date.now() };
    cloudWrite(
        { action: 'create', unitName: `[Tim] ${name}`, field: 'Anggota', before: '', after: jobTitle || name },
        cloudCall('saveTeamMember', rec),
        `Anggota "${name}" ditambahkan`,
        err => {
            console.error('[team] member save failed:', err);
            if (err && err.code === 'permission-denied') showTeamRulesBanner();
            showToast('Gagal menyimpan anggota', 'error');
        }
    );
    nameEl.value = '';
    jobEl.value = '';
    // The company is deliberately kept: several people from the same company
    // are usually added in a row.
    nameEl.focus();
}

function toggleTeamMember(id) {
    if (!requireEdit('teamMembers')) return;
    const m = memberById(id);
    if (!m) return;
    const nextActive = m.active === false;
    cloudWrite(
        { action: 'update', unitName: `[Tim] ${m.name}`, field: 'Status anggota',
          before: m.active === false ? 'Nonaktif' : 'Aktif',
          after: nextActive ? 'Aktif' : 'Nonaktif' },
        cloudCall('saveTeamMember', { ...m, active: nextActive, updatedAt: Date.now() }),
        null,
        err => {
            console.error('[team] member toggle failed:', err);
            showToast('Gagal mengubah status anggota', 'error');
        }
    );
}

function deleteTeamMember(id) {
    if (!requireEdit('teamMembers')) return;
    const m = memberById(id);
    if (!m) return;
    // teamShifts only holds the subscribed window now, so counting it would
    // understate the real number — say what happens instead of a figure that
    // would be wrong. Work logs are still loaded in full, so that count stands.
    const hasShifts = teamShifts.some(s => s.memberId === id);
    const logCount = workLogs.filter(w => w.memberId === id).length;
    const warn = (hasShifts || logCount)
        ? `\n\nJadwal shift${logCount ? ` dan ${logCount} laporan harian` : ''} miliknya akan tetap tersimpan, tetapi namanya akan tampil sebagai "(anggota dihapus)". Untuk sekadar mengeluarkannya dari jadwal, pakai Nonaktifkan.`
        : '';
    if (!confirm(`Hapus anggota "${m.name}"?${warn}`)) return;

    cloudWrite(
        { action: 'delete', unitName: `[Tim] ${m.name}`, field: 'Anggota', before: m.jobTitle || m.name, after: '' },
        cloudCall('deleteTeamMember', id),
        `Anggota "${m.name}" dihapus`,
        err => {
            console.error('[team] member delete failed:', err);
            showToast('Gagal menghapus anggota', 'error');
        }
    );
}

// ============================================================
// DAILY WORK LOGS
// ============================================================

// "HH:MM" → minutes since midnight, or null when unparseable.
// Guards the two ways the hours on a daily report go wrong in the field.
//
// workLogMinutes wraps a negative difference to +24h so a night shift counts
// correctly — which means typing 08:00–07:00 instead of 07:00–08:00 records
// TWENTY-THREE hours, silently, straight into the "Total Jam Kerja" KPI and the
// monthly per-company recap. And filling only one of the two times records zero
// minutes while the report still looks filed.
//
// A wrap is asked about rather than refused: a real night shift has to stay
// recordable. Returns false when the save should stop.
function checkWorkLogHours(start, end) {
    const s = parseHHMM(start);
    const e = parseHHMM(end);

    if ((s == null) !== (e == null)) {
        showToast('Isi jam mulai dan jam selesai dua-duanya, atau kosongkan dua-duanya', 'warning');
        return false;
    }
    if (s == null) return true;          // both blank — allowed on purpose

    if (s === e) {
        return confirm('Jam mulai dan jam selesai sama, jadi laporan ini terhitung 0 jam. Tetap simpan?');
    }
    const mins = e - s < 0 ? e - s + 24 * 60 : e - s;
    if (e - s < 0 && mins >= WORKLOG_LONG_SHIFT_MIN) {
        return confirm(`Jam kerja terbaca ${formatMinutes(mins)} karena jam selesai lebih awal dari jam mulai.\n\n`
            + 'Kalau ini shift malam yang melewati tengah malam, tekan OK.\n'
            + 'Kalau jamnya tertukar, tekan Batal lalu betulkan.');
    }
    return true;
}

function parseHHMM(v) {
    const m = /^(\d{1,2}):(\d{2})$/.exec(String(v || '').trim());
    if (!m) return null;
    const h = +m[1], mi = +m[2];
    if (h > 23 || mi > 59) return null;
    return h * 60 + mi;
}

// Duration in minutes. An end time earlier than the start is read as crossing
// midnight (the night shift runs 23:00–07:00), not as a negative duration.
function workLogMinutes(log) {
    const s = parseHHMM(log && log.start);
    const e = parseHHMM(log && log.end);
    if (s == null || e == null) return 0;
    const diff = e - s;
    return diff < 0 ? diff + 24 * 60 : diff;
}

function formatMinutes(min) {
    if (!min) return '0j';
    const h = Math.floor(min / 60), m = min % 60;
    if (!h) return `${m}m`;
    return m ? `${h}j ${m}m` : `${h}j`;
}

function populateWorkLogFilters() {
    const sel = document.getElementById('wlMemberFilter');
    if (sel) {
        const keep = sel.value;
        sel.innerHTML = '<option value="">Semua Anggota</option>' +
            teamMembers.map(m =>
                `<option value="${escapeHtml(m.id)}">${escapeHtml(m.name)}</option>`).join('');
        if (keep && sel.querySelector(`option[value="${CSS.escape(keep)}"]`)) sel.value = keep;
    }
    const formSel = document.getElementById('wlMember');
    if (formSel) {
        const keep = formSel.value;
        formSel.innerHTML = '<option value="">— Pilih anggota —</option>' +
            activeMembers().map(m =>
                `<option value="${escapeHtml(m.id)}">${escapeHtml(m.name)}</option>`).join('');
        if (keep && formSel.querySelector(`option[value="${CSS.escape(keep)}"]`)) formSel.value = keep;
    }
    const list = document.getElementById('wlUnitList');
    if (list) {
        list.innerHTML = [...globalData]
            .sort((a, b) => (a.name || '').localeCompare(b.name || ''))
            .map(u => `<option value="${escapeHtml(damageUnitLabel(u))}"></option>`).join('');
    }
    // Company suggestions, shared by the roster modal and the report filter.
    const comps = allCompanies();
    const dl = document.getElementById('companyList');
    if (dl) dl.innerHTML = comps.map(c => `<option value="${escapeHtml(c)}"></option>`).join('');
    const pdl = document.getElementById('paddockList');
    if (pdl) pdl.innerHTML = allPaddocks().map(p => `<option value="${escapeHtml(p)}"></option>`).join('');
    const compFilter = document.getElementById('wlCompanyFilter');
    if (compFilter) {
        const keep = compFilter.value;
        compFilter.innerHTML = '<option value="">Semua Perusahaan</option>' +
            comps.map(c => `<option value="${escapeHtml(c)}">${escapeHtml(c)}</option>`).join('') +
            `<option value="__none__">${escapeHtml(NO_COMPANY)}</option>`;
        if (keep && compFilter.querySelector(`option[value="${CSS.escape(keep)}"]`)) compFilter.value = keep;
    }
}

function getFilteredWorkLogs() {
    const from = (document.getElementById('wlFrom')?.value || '').trim();
    const to = (document.getElementById('wlTo')?.value || '').trim();
    const member = (document.getElementById('wlMemberFilter')?.value || '');
    const company = (document.getElementById('wlCompanyFilter')?.value || '');
    const approval = (document.getElementById('wlApprovalFilter')?.value || '');
    const q = (document.getElementById('wlSearch')?.value || '').toLowerCase().trim();

    return workLogs.filter(w => {
        if (from && String(w.date || '') < from) return false;
        if (to && String(w.date || '') > to) return false;
        if (member && w.memberId !== member) return false;
        if (company) {
            const c = companyOfRecord(w);
            if (company === '__none__' ? !!c : c !== company) return false;
        }
        if (approval && workLogApproval(w) !== approval) return false;
        if (q) {
            const hay = [memberNameOf(w), companyOfRecord(w), w.paddock,
                         ...workLogUnitNames(w), w.task, w.issue]
                .join(' ').toLowerCase();
            if (!hay.includes(q)) return false;
        }
        return true;
    });
}

function renderWorkLogTable() {
    const rows = getFilteredWorkLogs();
    const tbody = document.getElementById('workLogBody');
    if (!tbody) return;

    // KPI strip
    const totalMin = rows.reduce((a, w) => a + workLogMinutes(w), 0);
    const today = toISODate();
    const reportedToday = new Set(
        workLogs.filter(w => w.date === today && w.memberId).map(w => w.memberId)
    ).size;
    const activeCount = activeMembers().length;

    const setText = (id, v) => { const el = document.getElementById(id); if (el) el.textContent = v; };
    setText('wlKpiCount', rows.length);
    setText('wlKpiHours', formatMinutes(totalMin));
    // Orang yang izin/sakitnya sudah disetujui hari ini dikeluarkan dari
    // penyebutnya. Sebelumnya mereka terbaca sebagai orang yang lalai melapor,
    // padahal ketidakhadirannya justru sudah dicatat dan disetujui.
    const onLeaveToday = [...membersOnLeave(today)]
        .filter(id => activeMembers().some(m => m.id === id)).length;
    const expected = Math.max(0, activeCount - onLeaveToday);
    setText('wlKpiToday', `${reportedToday} / ${expected}`);
    const todaySub = document.getElementById('wlKpiTodaySub');
    if (todaySub) {
        todaySub.textContent = onLeaveToday
            ? `Anggota aktif yang sudah melapor · ${onLeaveToday} izin/sakit`
            : 'Anggota aktif yang sudah melapor';
    }
    // Counts the whole log, not the filtered slice: a backlog you have filtered
    // out of sight is exactly the backlog worth showing.
    setText('wlKpiPending', workLogs.filter(w => workLogApproval(w) === 'pending').length);
    setText('workLogCount', `${rows.length} laporan`);

    const canEdit = hasAccess('teamLog', 'edit');
    const hasFilter = (document.getElementById('wlFrom')?.value || '') ||
                      (document.getElementById('wlTo')?.value || '') ||
                      (document.getElementById('wlMemberFilter')?.value || '') ||
                      (document.getElementById('wlCompanyFilter')?.value || '') ||
                      (document.getElementById('wlApprovalFilter')?.value || '') ||
                      (document.getElementById('wlSearch')?.value || '');

    if (rows.length === 0) {
        tbody.innerHTML = `<tr><td colspan="12" style="text-align:center;padding:24px;color:var(--text-secondary)">${
            hasFilter ? 'Tidak ada laporan yang cocok dengan filter'
                      : 'Belum ada laporan harian. Klik <strong>Tambah Laporan</strong> untuk mulai.'
        }</td></tr>`;
        return;
    }

    tbody.innerHTML = rows.map((w, i) => {
        const names = workLogUnitNames(w);
        const photoCount = workLogPhotoCount(w);
        const task = w.task || '';
        const taskShort = task.length > 60 ? task.slice(0, 60) + '…' : task;
        const issue = w.issue || '';
        const issueShort = issue.length > 40 ? issue.slice(0, 40) + '…' : issue;
        const jam = (w.start || w.end) ? `${escapeHtml(w.start || '?')}–${escapeHtml(w.end || '?')}` : '—';
        return `
        <tr>
            <td>${i + 1}</td>
            <td data-label="Tanggal" style="white-space:nowrap">${escapeHtml(w.date || '')}</td>
            <td data-label="Anggota"><strong>${escapeHtml(memberNameOf(w))}</strong></td>
            <td data-label="Perusahaan" style="font-size:12px">${(() => {
                const c = companyOfRecord(w);
                return c ? escapeHtml(c) : '<span style="color:var(--text-light)">—</span>';
            })()}</td>
            <td data-label="Jam Kerja" style="white-space:nowrap;font-variant-numeric:tabular-nums">
                ${jam}<span class="wl-duration">${escapeHtml(formatMinutes(workLogMinutes(w)))}</span></td>
            <td data-label="Unit">${names.length
                ? names.map(n => `<span class="badge badge-cat" style="font-size:10px">${escapeHtml(n)}</span>`).join(' ')
                : '<span style="color:var(--text-light);font-size:11px">—</span>'}</td>
            <td data-label="Paddock" style="font-size:12px">${w.paddock
                ? escapeHtml(w.paddock)
                : '<span style="color:var(--text-light)">—</span>'}</td>
            <td data-label="Uraian" style="max-width:150px;font-size:12px" title="${escapeHtml(task)}">${escapeHtml(taskShort)}</td>
            <td data-label="Kendala" style="max-width:110px;font-size:12px;color:var(--text-secondary)" title="${escapeHtml(issue)}">${
                issueShort ? escapeHtml(issueShort) : '<span style="color:var(--text-light)">—</span>'}</td>
            <td data-label="Persetujuan">${(() => {
                const st = workLogApproval(w);
                const meta = st === 'approved'
                    ? `Disetujui ${w.approvedBy || ''}${w.approvedAt ? ' · ' + formatUserTime(w.approvedAt) : ''}`
                    : (st === 'revision' ? (w.revisionNote || 'Perlu revisi') : 'Belum diperiksa');
                const badge = `<span class="appr appr--${st}" title="${escapeHtml(meta)}">${escapeHtml(APPROVAL_STATES[st].label)}</span>`;
                if (!canApproveThisLog(w)) return badge;
                const btns = `<span class="appr-actions">
                    ${st !== 'approved' ? `<button class="btn btn-secondary appr-btn" title="Setujui laporan" aria-label="Setujui laporan" onclick="approveWorkLog('${escapeHtml(w.id)}')"><i class="fas fa-check"></i></button>` : ''}
                    ${st !== 'revision' ? `<button class="btn btn-secondary appr-btn" title="Minta revisi" aria-label="Minta revisi" onclick="reviseWorkLog('${escapeHtml(w.id)}')"><i class="fas fa-rotate-left"></i></button>` : ''}
                </span>`;
                return badge + btns;
            })()}</td>
            <td data-label="Dokumentasi">${photoCount
                ? `<button type="button" class="wl-photo-btn" title="Lihat ${photoCount} foto dokumentasi"
                        aria-label="Lihat ${photoCount} foto dokumentasi"
                        onclick="openWorkLogPhotos('${escapeHtml(w.id)}', this)"><i class="fas fa-image"></i> ${photoCount}</button>`
                : '<span style="color:var(--text-light);font-size:11px">—</span>'}</td>
            <td class="col-actions">
                ${canEdit ? `<div class="row-actions">
                    <button class="btn btn-secondary" title="Edit" aria-label="Edit laporan" onclick="editWorkLog('${escapeHtml(w.id)}')"><i class="fas fa-pen"></i></button>
                    <button class="btn btn-secondary" title="Hapus" aria-label="Hapus laporan" onclick="deleteWorkLog('${escapeHtml(w.id)}')"><i class="fas fa-trash" style="color:var(--danger)"></i></button>
                </div>` : ''}
            </td>
        </tr>`;
    }).join('');
}

function clearWorkLogFilter() {
    ['wlFrom', 'wlTo', 'wlSearch'].forEach(id => {
        const el = document.getElementById(id);
        if (el) el.value = '';
    });
    ['wlMemberFilter', 'wlCompanyFilter', 'wlApprovalFilter'].forEach(id => {
        const sel = document.getElementById(id);
        if (sel) sel.value = '';
    });
    renderWorkLogTable();
}

// ---- Work log form: unit chips + documentation photos ----
// Held outside the DOM while the modal is open, like _dmgPhotoData.
let _wlUnits = [];   // [{ id, name, sn }]
let _wlPhotos = [];  // data URLs
// Set only when a photo is actually added or removed. saveWorkLog writes the
// photo document only then — so editing just the description never rewrites
// (or, worse, wipes) photos that may not even have finished loading yet.
let _wlPhotosDirty = false;
let _wlPhotosLoading = false;

function renderWorkLogUnitChips() {
    const wrap = document.getElementById('wlUnitChips');
    if (!wrap) return;
    wrap.innerHTML = _wlUnits.length
        ? _wlUnits.map((u, i) => `
            <span class="unit-chip">
                ${escapeHtml(u.name || u.sn || '(unit)')}
                <button type="button" class="unit-chip__x" aria-label="Hapus ${escapeHtml(u.name || u.sn)}"
                        title="Hapus dari daftar" onclick="removeWorkLogUnit(${i})">&times;</button>
            </span>`).join('')
        : '<span class="unit-chip__empty">Belum ada unit dipilih</span>';
}

function addWorkLogUnit() {
    const input = document.getElementById('wlUnit');
    if (!input) return;
    const raw = (input.value || '').trim();
    if (!raw) return;
    const unit = resolveDamageUnit(raw);
    if (!unit) {
        showToast(`Unit "${raw}" tidak ditemukan — pilih dari daftar`, 'warning');
        return;
    }
    if (_wlUnits.some(u => u.id === unit.id)) {
        showToast(`${unit.name || unit.sn} sudah ada di daftar`, 'warning');
        input.value = '';
        return;
    }
    _wlUnits.push({ id: unit.id, name: unit.name || '', sn: unit.sn || '' });
    input.value = '';
    input.focus();
    renderWorkLogUnitChips();
}

function removeWorkLogUnit(index) {
    _wlUnits.splice(index, 1);
    renderWorkLogUnitChips();
}

function renderWorkLogPhotos() {
    const wrap = document.getElementById('wlPhotoPreviews');
    const count = document.getElementById('wlPhotoCount');
    if (count) count.textContent = `${_wlPhotos.length}/${WORKLOG_PHOTO_MAX}`;
    if (!wrap) return;
    if (_wlPhotosLoading && !_wlPhotos.length) {
        wrap.innerHTML = '<span class="wl-photo__loading"><i class="fas fa-spinner fa-spin"></i> Memuat foto…</span>';
        return;
    }
    wrap.innerHTML = _wlPhotos.map((src, i) => `
        <div class="wl-photo">
            <img src="${src}" alt="Dokumentasi ${i + 1}" onclick="openPhotoLightbox(_wlPhotos, ${i})">
            <button type="button" class="wl-photo__x" aria-label="Hapus dokumentasi ${i + 1}"
                    title="Hapus foto" onclick="removeWorkLogPhoto(${i})">&times;</button>
        </div>`).join('');
}

async function handleWorkLogPhotoChange(event) {
    const files = [...(event.target.files || [])];
    event.target.value = '';
    if (!files.length) return;

    for (const file of files) {
        if (_wlPhotos.length >= WORKLOG_PHOTO_MAX) {
            showToast(`Maksimal ${WORKLOG_PHOTO_MAX} foto per laporan`, 'warning');
            break;
        }
        if (!file.type.startsWith('image/')) {
            showToast(`"${file.name}" bukan gambar — dilewati`, 'warning');
            continue;
        }
        try {
            const data = await compressImageToDataURL(file, WORKLOG_PHOTO_OPTS);
            // Firestore rejects a document over 1MB outright, so stop before
            // the write fails rather than after.
            const total = _wlPhotos.reduce((n, p) => n + p.length, 0) + data.length;
            if (total > WORKLOG_PHOTOS_TOTAL_BYTES) {
                showToast('Total ukuran foto sudah maksimal — hapus satu dulu', 'warning');
                break;
            }
            _wlPhotos.push(data);
            _wlPhotosDirty = true;
        } catch (err) {
            showToast(err.message || `Gagal memproses "${file.name}"`, 'error');
        }
    }
    renderWorkLogPhotos();
}

function removeWorkLogPhoto(index) {
    _wlPhotos.splice(index, 1);
    _wlPhotosDirty = true;
    renderWorkLogPhotos();
}

// ---- Opening documentation from the table ----
// One fetch per report, then cached for the session. Offline against a report
// whose photos were never cached, this fails — and says so plainly instead of
// leaving a button spinning forever.
async function openWorkLogPhotos(id, btn) {
    const original = btn ? btn.innerHTML : '';
    if (btn) { btn.disabled = true; btn.innerHTML = '<i class="fas fa-spinner fa-spin"></i>'; }
    try {
        const photos = await loadWorkLogPhotos(id);
        if (!photos.length) { showToast('Foto tidak ditemukan', 'warning'); return; }
        openPhotoLightbox(photos);
    } catch (err) {
        console.error('[team] work log photos load failed:', err);
        showToast(navigator.onLine
            ? 'Gagal memuat foto dokumentasi'
            : 'Foto perlu sinyal untuk dimuat', 'error');
    } finally {
        if (btn) { btn.disabled = false; btn.innerHTML = original; }
    }
}

function showAddWorkLogForm() {
    if (!requireEdit('teamLog')) return;
    if (activeMembers().length === 0) {
        showToast('Tambahkan anggota tim dulu lewat Kelola Anggota', 'warning');
        return;
    }
    document.getElementById('workLogModalTitle').textContent = 'Tambah Laporan Harian';
    document.getElementById('editWorkLogId').value = '';
    document.getElementById('workLogForm').reset();
    _wlUnits = [];
    _wlPhotos = [];
    _wlPhotosDirty = false;
    _wlPhotosLoading = false;
    populateWorkLogFilters();
    renderWorkLogUnitChips();
    renderWorkLogPhotos();
    document.getElementById('wlDate').value = toISODate();
    document.getElementById('workLogModal').classList.add('open');
}

function editWorkLog(id) {
    if (!requireEdit('teamLog')) return;
    const w = workLogs.find(x => x.id === id);
    if (!w) return;
    document.getElementById('workLogModalTitle').textContent = 'Edit Laporan Harian';
    document.getElementById('editWorkLogId').value = w.id;
    populateWorkLogFilters();
    document.getElementById('wlDate').value = w.date || '';
    document.getElementById('wlMember').value = w.memberId || '';
    document.getElementById('wlStart').value = w.start || '';
    document.getElementById('wlEnd').value = w.end || '';
    // Copies, so cancelling the modal leaves the stored record untouched.
    _wlUnits = workLogUnits(w).map(u => ({ ...u }));
    _wlPhotos = [];
    _wlPhotosDirty = false;
    // Photos arrive separately; until they do the strip shows a placeholder.
    // Nothing is lost if they never arrive — see _wlPhotosDirty in saveWorkLog.
    _wlPhotosLoading = workLogPhotoCount(w) > 0;
    document.getElementById('wlUnit').value = '';
    document.getElementById('wlPaddock').value = w.paddock || '';
    renderWorkLogUnitChips();
    renderWorkLogPhotos();
    document.getElementById('wlTask').value = w.task || '';
    document.getElementById('wlIssue').value = w.issue || '';
    document.getElementById('workLogModal').classList.add('open');

    if (_wlPhotosLoading) {
        loadWorkLogPhotos(id).then(photos => {
            // The modal may have been closed or reopened on another report
            // while the fetch was in flight — only fill what still applies.
            if (document.getElementById('editWorkLogId').value !== id) return;
            if (_wlPhotosDirty) return;   // user already changed the photos
            _wlPhotos = photos;
            _wlPhotosLoading = false;
            renderWorkLogPhotos();
        }).catch(err => {
            console.error('[team] work log photos load failed:', err);
            if (document.getElementById('editWorkLogId').value !== id) return;
            _wlPhotosLoading = false;
            renderWorkLogPhotos();
            showToast(navigator.onLine
                ? 'Foto lama gagal dimuat — foto lama tetap tersimpan'
                : 'Foto lama perlu sinyal untuk dimuat — foto lama tetap tersimpan', 'warning');
        });
    }
}

function closeWorkLogModal(force) {
    // Photos are compressed on the device and exist nowhere else until the
    // report is saved, so closing after shooting four of them in the field is
    // the most expensive discard in the app. saveWorkLog passes force.
    if (!force && _wlPhotosDirty && _wlPhotos.length &&
        !confirm(`${_wlPhotos.length} foto belum tersimpan dan akan hilang. Tutup saja?`)) {
        return;
    }
    _wlPhotosDirty = false;
    document.getElementById('workLogModal').classList.remove('open');
}

function saveWorkLog(event) {
    event.preventDefault();
    if (!requireEdit('teamLog')) return;

    const id = document.getElementById('editWorkLogId').value;
    const memberId = document.getElementById('wlMember').value;
    const member = memberById(memberId);
    if (!member) { showToast('Pilih anggota tim dulu', 'warning'); return; }

    const date = document.getElementById('wlDate').value;
    const start = document.getElementById('wlStart').value;
    const end = document.getElementById('wlEnd').value;
    const task = (document.getElementById('wlTask').value || '').trim();
    if (!task) { showToast('Uraian pekerjaan tidak boleh kosong', 'warning'); return; }
    if (!checkWorkLogHours(start, end)) return;
    // The native max= is the first line of defence, but a report filed decades
    // out skews the monthly recap badly enough to be worth a second one.
    if (date && date > toISODate()) {
        showToast('Tanggal laporan tidak boleh di masa depan — periksa tahunnya', 'warning');
        return;
    }

    // A unit left typed but not added is an easy mistake to make, so fold it in
    // rather than dropping it silently.
    const pending = (document.getElementById('wlUnit').value || '').trim();
    if (pending) {
        addWorkLogUnit();
        if ((document.getElementById('wlUnit').value || '').trim()) return; // unresolved
    }
    const units = _wlUnits.map(u => ({ ...u }));

    const existing = id ? workLogs.find(w => w.id === id) : null;
    const rec = {
        id: id || generateWorkLogId(),
        date,
        memberId,
        memberName: member.name,
        company: companyOf(member),
        start, end,
        units,
        // First unit mirrored into the old single-unit fields so anything still
        // reading them — the unit profile cross-link, older exports — keeps working.
        unitId: units.length ? units[0].id : '',
        unitName: units.length ? (units[0].name || '') : '',
        sn: units.length ? (units[0].sn || '') : '',
        paddock: (document.getElementById('wlPaddock').value || '').trim(),
        // Only the count lives on the report; the images go to workLogPhotos.
        photoCount: _wlPhotosDirty ? _wlPhotos.length : workLogPhotoCount(existing),
        task,
        issue: (document.getElementById('wlIssue').value || '').trim(),
        createdAt: existing ? (existing.createdAt || Date.now()) : Date.now(),
        updatedAt: Date.now(),
        // Who filed it — needed so an approver cannot sign off their own work.
        createdByUid: existing ? (existing.createdByUid || '') : ((currentUser && currentUser.uid) || ''),
        createdByEmail: existing ? (existing.createdByEmail || '') : ((currentUser && currentUser.email) || ''),
        // Editing sends the report back for checking. Without this, someone
        // could get a report approved and then change what it says.
        approval: 'pending',
        approvedBy: '', approvedByEmail: '', approvedAt: 0,
        revisionNote: ''
    };
    // Untouched photos are left exactly as they are — not re-read, not
    // rewritten, not cleared. That is what makes it safe to save while the
    // photos are still loading, and it keeps a description fix from costing a
    // 800KB write. The inline copy on an older report is cleared only when the
    // user actually changes the photos, or by migrateWorkLogPhotos().
    if (_wlPhotosDirty) rec.photos = [];

    const wasApproved = existing && workLogApproval(existing) === 'approved';

    cloudWrite(
        {
            action: existing ? 'update' : 'create',
            unitId: rec.unitId,
            unitName: `[Laporan] ${member.name}`,
            field: `Laporan ${rec.date}`,
            before: existing ? (existing.task || '') : '',
            after: rec.task
        },
        cloudCall('saveWorkLog', rec),
        wasApproved
            ? 'Laporan diperbarui — persetujuan dibatalkan, perlu diperiksa ulang'
            : (existing ? 'Laporan diperbarui' : 'Laporan harian ditambahkan'),
        err => {
            console.error('[team] work log save failed:', err);
            if (err && err.code === 'permission-denied') showTeamRulesBanner();
            showToast('Gagal menyimpan laporan', 'error');
        }
    );

    if (_wlPhotosDirty) {
        const photos = _wlPhotos.slice();
        _wlPhotoCache.set(rec.id, photos);
        // Offline, the service worker can still be serving a firebase-init.js
        // from before this collection existed. Say so rather than throwing
        // halfway through a save that otherwise looks like it worked.
        if (!window.cloud.saveWorkLogPhotos) {
            showToast('Muat ulang halaman — versi lama masih aktif, foto belum terkirim', 'warning');
            closeWorkLogModal(true);
            return;
        }
        const write = photos.length
            ? window.cloud.saveWorkLogPhotos(rec.id, photos)
            : window.cloud.deleteWorkLogPhotos(rec.id);
        write.catch(err => {
            console.error('[team] work log photos save failed:', err);
            if (err && err.code === 'permission-denied') showTeamRulesBanner();
            showToast('Laporan tersimpan, tetapi foto gagal dikirim', 'error');
        });
    }

    closeWorkLogModal(true);
}

function deleteWorkLog(id) {
    if (!requireEdit('teamLog')) return;
    const w = workLogs.find(x => x.id === id);
    if (!w) return;
    if (!confirm(`Hapus laporan ${memberNameOf(w)} tanggal ${w.date}?`)) return;
    // Otherwise the photo document is orphaned: invisible, but still billed for
    // and still downloaded by anyone who happens to request that id.
    if (workLogPhotoCount(w) > 0 && window.cloud.deleteWorkLogPhotos) {
        _wlPhotoCache.delete(id);
        window.cloud.deleteWorkLogPhotos(id).catch(err =>
            console.error('[team] work log photos delete failed:', err));
    }
    cloudWrite(
        { action: 'delete', unitId: w.unitId || '',
          unitName: `[Laporan] ${memberNameOf(w)}`,
          field: `Laporan ${w.date}`, before: w.task || '', after: '' },
        cloudCall('deleteWorkLog', id),
        'Laporan dihapus',
        err => {
            console.error('[team] work log delete failed:', err);
            showToast('Gagal menghapus laporan', 'error');
        }
    );
}

function approveWorkLog(id) {
    const w = workLogs.find(x => x.id === id);
    if (!w) return;
    if (!canApproveWorkLogs()) {
        showToast('Anda tidak punya hak menyetujui laporan', 'warning');
        return;
    }
    if (!canApproveThisLog(w)) {
        showToast('Laporan yang Anda buat sendiri harus disetujui orang lain', 'warning');
        return;
    }
    if (workLogApproval(w) === 'approved') return;

    const rec = {
        ...w,
        approval: 'approved',
        approvedBy: (currentUserDoc && currentUserDoc.displayName) || (currentUser && currentUser.email) || '',
        approvedByEmail: (currentUser && currentUser.email) || '',
        approvedAt: Date.now(),
        revisionNote: '',
        updatedAt: Date.now()
    };
    cloudWrite(
        { action: 'update', unitId: rec.unitId || '',
          unitName: `[Laporan] ${memberNameOf(rec)}`,
          field: `Persetujuan ${rec.date}`,
          before: APPROVAL_STATES[workLogApproval(w)].label,
          after: 'Disetujui' },
        cloudCall('saveWorkLog', rec),
        'Laporan disetujui',
        err => {
            console.error('[team] approve failed:', err);
            if (err && err.code === 'permission-denied') showTeamRulesBanner();
            showToast('Gagal menyetujui laporan', 'error');
        }
    );
}

function reviseWorkLog(id) {
    const w = workLogs.find(x => x.id === id);
    if (!w) return;
    if (!canApproveWorkLogs()) {
        showToast('Anda tidak punya hak menyetujui laporan', 'warning');
        return;
    }
    if (!canApproveThisLog(w)) {
        showToast('Laporan yang Anda buat sendiri harus diperiksa orang lain', 'warning');
        return;
    }
    // The note is the whole point of this state — without it the person is
    // told "wrong" and nothing else.
    const note = prompt(`Apa yang perlu diperbaiki pada laporan ${memberNameOf(w)} (${w.date})?`,
        w.revisionNote || '');
    if (note === null) return;
    if (!note.trim()) { showToast('Tulis alasannya supaya bisa diperbaiki', 'warning'); return; }

    const rec = {
        ...w,
        approval: 'revision',
        revisionNote: note.trim(),
        approvedBy: '', approvedByEmail: '', approvedAt: 0,
        reviewedBy: (currentUserDoc && currentUserDoc.displayName) || (currentUser && currentUser.email) || '',
        reviewedAt: Date.now(),
        updatedAt: Date.now()
    };
    cloudWrite(
        { action: 'update', unitId: rec.unitId || '',
          unitName: `[Laporan] ${memberNameOf(rec)}`,
          field: `Persetujuan ${rec.date}`,
          before: APPROVAL_STATES[workLogApproval(w)].label,
          after: `Perlu Revisi — ${rec.revisionNote}` },
        cloudCall('saveWorkLog', rec),
        'Laporan ditandai perlu revisi',
        err => {
            console.error('[team] revise failed:', err);
            if (err && err.code === 'permission-denied') showTeamRulesBanner();
            showToast('Gagal menandai laporan', 'error');
        }
    );
}

function exportWorkLogCSV() {
    if (!canCsv('export')) return;
    const rows = getFilteredWorkLogs();
    if (rows.length === 0) { showToast('Tidak ada laporan untuk diexport', 'warning'); return; }
    const headers = ['No', 'Tanggal', 'Perusahaan', 'Anggota', 'Jabatan', 'Mulai', 'Selesai', 'Durasi (jam)',
                     'Paddock Area', 'Unit', 'Serial Number', 'Jumlah Foto', 'Uraian Pekerjaan', 'Kendala',
                     'Persetujuan', 'Disetujui Oleh', 'Catatan Revisi'];
    const dataRows = rows.map((w, i) => {
        const m = w.memberId ? memberById(w.memberId) : null;
        const us = workLogUnits(w);
        return [
            i + 1, w.date || '', companyOfRecord(w), memberNameOf(w), m ? (m.jobTitle || '') : '',
            w.start || '', w.end || '',
            (workLogMinutes(w) / 60).toFixed(2),
            w.paddock || '',
            // Several units share one cell, separated so the column stays readable.
            workLogUnitNames(w).join(' | '),
            us.map(u => u.sn || '').filter(Boolean).join(' | '),
            workLogPhotoCount(w),
            w.task || '', w.issue || '',
            APPROVAL_STATES[workLogApproval(w)].label,
            w.approvedBy || '', w.revisionNote || ''
        ];
    });
    const csv = toCSV(headers, dataRows);
    const blob = new Blob(['﻿' + csv], { type: 'text/csv;charset=utf-8;' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = `laporan_harian_${toISODate()}.csv`;
    a.click();
    URL.revokeObjectURL(url);
    showToast(`Export ${rows.length} laporan ke CSV`, 'success');
}

// Work logs attached to one unit, newest first — used by the unit profile so
// "who has worked on this machine" is answerable from the unit's own page.
function workLogsForUnit(unitId, sn) {
    const snLc = (sn || '').toLowerCase();
    // A report can list several units, so match any of them — not just the
    // first one mirrored into the legacy unitId field.
    return workLogs.filter(w => workLogUnits(w).some(u =>
        (unitId && u.id === unitId) || (snLc && (u.sn || '').toLowerCase() === snLc)
    ));
}

// ============================================================
// IZIN / SAKIT — leave and sick-day requests
// ------------------------------------------------------------
// A third team record, alongside the shift schedule and the daily report.
// It exists because the schedule's 'libur' cell cannot answer the questions
// that matter here: it has no date range, no scanned letter, no note and no
// approval — it is one enum value on one day.
//
// Access rides on the SAME two areas as the daily report: teamLog to file one,
// teamLogApprove to approve one. The people are the same people and the
// approver is the same approver, so there is no new permission to hand out.
//
// Scanned letters go to the workLogPhotos collection (see the getTeamDocs
// aliases in firebase-init.js) — same shape, same area, and no fourth path
// waiting on a rules publish.
// ============================================================

// Stored as KEYS, never as the label. Translating or rewording a label must
// not rewrite what is in the database — the Good/Breakdown values elsewhere in
// this app are the cautionary example.
const LEAVE_TYPES = [
    { key: 'izin',  label: 'Izin',                   tone: 'info',    needsDoc: true  },
    { key: 'sakit', label: 'Sakit',                  tone: 'warning', needsDoc: true  },
    { key: 'alpa',  label: 'Alpa (tanpa keterangan)', tone: 'danger', needsDoc: false }
];
const LEAVE_LABEL = LEAVE_TYPES.reduce((m, t) => { m[t.key] = t.label; return m; }, {});
const LEAVE_DOC_MAX = 4;                       // lembar surat per pengajuan
const LEAVE_LONG_DAYS = 30;                    // di atas ini, minta konfirmasi

function generateLeaveId() {
    return 'lv_' + Date.now() + '_' + Math.random().toString(36).substr(2, 5);
}

function leaveTypeLabel(rec) {
    return LEAVE_LABEL[rec && rec.type] || (rec && rec.type) || '-';
}

function leaveTypeTone(rec) {
    const t = LEAVE_TYPES.find(x => x.key === (rec && rec.type));
    return t ? t.tone : 'info';
}

// Both ends inclusive: a request for the 5th to the 5th is one day, not zero.
function leaveDays(from, to) {
    const a = parseLocalDate(from);
    const b = parseLocalDate(to || from);
    if (!a || !b) return 0;
    const n = Math.round((b - a) / 86400000) + 1;
    return n > 0 ? n : 0;
}

function leaveCovers(rec, date) {
    if (!rec || !rec.dateFrom || !date) return false;
    return date >= rec.dateFrom && date <= (rec.dateTo || rec.dateFrom);
}

// Only APPROVED requests count as being away. A pending one is a claim, not a
// fact, and letting it mark the schedule would let anyone empty the roster by
// filing a request nobody has looked at yet.
function leaveOn(memberId, date) {
    return leaveRequests.find(r =>
        r.memberId === memberId &&
        workLogApproval(r) === 'approved' &&
        leaveCovers(r, date)) || null;
}

function membersOnLeave(date) {
    const d = date || toISODate();
    return new Set(leaveRequests
        .filter(r => workLogApproval(r) === 'approved' && leaveCovers(r, d))
        .map(r => r.memberId));
}

// Anything not yet refused still occupies the days it claims. A request sent
// back for revision is excluded: it is not standing.
function overlappingLeave(memberId, from, to, exceptId) {
    const a = from;
    const b = to || from;
    return leaveRequests.filter(r =>
        r.memberId === memberId &&
        r.id !== exceptId &&
        workLogApproval(r) !== 'revision' &&
        a <= (r.dateTo || r.dateFrom) && b >= r.dateFrom);
}

function leaveDocCount(rec) {
    return Number(rec && rec.docCount) || 0;
}

const _leaveDocCache = new Map();              // leaveId -> data URLs

// Fetched one document at a time, only when someone opens it. The collection
// this reads is never subscribed; see firebase-init.js.
async function loadLeaveDocs(id) {
    if (_leaveDocCache.has(id)) return _leaveDocCache.get(id);
    const fn = window.cloud && (window.cloud.getTeamDocs || window.cloud.getWorkLogPhotos);
    if (!fn) return [];
    const pages = await fn.call(window.cloud, id);
    const list = Array.isArray(pages) ? pages : [];
    _leaveDocCache.set(id, list);
    return list;
}

function applyCloudLeaveSnapshot(list) {
    leaveRequests = (list || []).slice().sort((a, b) =>
        String(b.dateFrom || '').localeCompare(String(a.dateFrom || '')) ||
        ((b.createdAt || 0) - (a.createdAt || 0)));
    if (currentView === 'team') {
        populateLeaveFilters();
        if (teamTab === 'leave') renderLeaveTable();
        // An approved request marks the schedule, so the grid has to redraw.
        if (teamTab === 'shift') renderShiftGrid();
    }
    // Without this the pending count never reaches the sidebar badge.
    scheduleDecisionRefresh();
}

function populateLeaveFilters() {
    const memberSel = document.getElementById('lvMemberFilter');
    if (memberSel) {
        const keep = memberSel.value;
        memberSel.innerHTML = '<option value="">Semua Anggota</option>'
            + teamMembers.map(m => `<option value="${escapeHtml(m.id)}">${escapeHtml(m.name)}</option>`).join('');
        if (keep && memberSel.querySelector(`option[value="${CSS.escape(keep)}"]`)) memberSel.value = keep;
    }
    const formSel = document.getElementById('lvMember');
    if (formSel) {
        const keep = formSel.value;
        formSel.innerHTML = '<option value="">— Pilih anggota —</option>'
            + activeMembers().map(m => `<option value="${escapeHtml(m.id)}">${escapeHtml(m.name)}</option>`).join('');
        if (keep && formSel.querySelector(`option[value="${CSS.escape(keep)}"]`)) formSel.value = keep;
    }
    const compSel = document.getElementById('lvCompanyFilter');
    if (compSel) {
        const keep = compSel.value;
        const companies = [...new Set(teamMembers.map(m => (m.company || '').trim()).filter(Boolean))].sort();
        compSel.innerHTML = '<option value="">Semua Perusahaan</option>'
            + companies.map(c => `<option value="${escapeHtml(c)}">${escapeHtml(c)}</option>`).join('')
            + '<option value="__none__">(Tanpa perusahaan)</option>';
        if (keep && compSel.querySelector(`option[value="${CSS.escape(keep)}"]`)) compSel.value = keep;
    }
}

// A request OVERLAPPING the from/to window counts, unlike the work log where a
// record sits on a single day. Someone filtering "September" wants the sick
// leave that started in August and ran into it.
function getFilteredLeave() {
    const from = (document.getElementById('lvFrom')?.value || '').trim();
    const to = (document.getElementById('lvTo')?.value || '').trim();
    const member = (document.getElementById('lvMemberFilter')?.value || '');
    const company = (document.getElementById('lvCompanyFilter')?.value || '');
    const approval = (document.getElementById('lvApprovalFilter')?.value || '');
    const type = (document.getElementById('lvTypeFilter')?.value || '');
    const q = (document.getElementById('lvSearch')?.value || '').toLowerCase().trim();

    return leaveRequests.filter(r => {
        const a = String(r.dateFrom || '');
        const b = String(r.dateTo || r.dateFrom || '');
        if (from && b < from) return false;
        if (to && a > to) return false;
        if (member && r.memberId !== member) return false;
        if (type && r.type !== type) return false;
        if (company) {
            const c = companyOfRecord(r);
            if (company === '__none__' ? !!c : c !== company) return false;
        }
        if (approval && workLogApproval(r) !== approval) return false;
        if (q) {
            const hay = [memberNameOf(r), companyOfRecord(r), leaveTypeLabel(r), r.reason]
                .join(' ').toLowerCase();
            if (!hay.includes(q)) return false;
        }
        return true;
    });
}

function clearLeaveFilter() {
    ['lvFrom', 'lvTo', 'lvMemberFilter', 'lvCompanyFilter', 'lvApprovalFilter', 'lvTypeFilter', 'lvSearch']
        .forEach(id => { const el = document.getElementById(id); if (el) el.value = ''; });
    renderLeaveTable();
}

function leaveRangeLabel(r) {
    const a = r.dateFrom || '-';
    const b = r.dateTo || r.dateFrom || '';
    return (!b || b === a) ? a : `${a} → ${b}`;
}

function renderLeaveTable() {
    const rows = getFilteredLeave();
    const tbody = document.getElementById('leaveBody');
    if (!tbody) return;

    const setText = (id, v) => { const el = document.getElementById(id); if (el) el.textContent = v; };
    setText('lvKpiCount', rows.length);
    setText('lvKpiDays', rows.reduce((a, r) => a + (Number(r.days) || leaveDays(r.dateFrom, r.dateTo)), 0));
    // Pending is counted over EVERY request, not the filtered slice: a filter
    // must not be able to make the backlog look empty.
    setText('lvKpiPending', leaveRequests.filter(r => workLogApproval(r) === 'pending').length);
    setText('lvKpiToday', membersOnLeave().size);

    const counter = document.getElementById('leaveCount');
    if (counter) counter.textContent = `${rows.length} pengajuan`;

    const canEdit = hasAccess('teamLog', 'edit');
    const filterOn = ['lvFrom', 'lvTo', 'lvMemberFilter', 'lvCompanyFilter',
                      'lvApprovalFilter', 'lvTypeFilter', 'lvSearch']
        .some(id => (document.getElementById(id)?.value || '') !== '');

    if (rows.length === 0) {
        tbody.innerHTML = `<tr><td colspan="10" style="text-align:center;padding:24px;color:var(--text-secondary)">${
            filterOn
                ? `Tidak ada pengajuan yang cocok dengan filter.
                   <button class="btn btn-secondary btn-sm" style="margin-left:8px" onclick="clearLeaveFilter()">
                       <i class="fas fa-filter-circle-xmark"></i> Hapus filter</button>`
                : 'Belum ada pengajuan izin atau sakit.'
        }</td></tr>`;
        return;
    }

    // Every <td> carries data-label except No and col-actions: style.css hides
    // an unlabelled cell entirely on a phone, so a forgotten label does not
    // look broken, the column just disappears on the device the team uses.
    tbody.innerHTML = rows.map((r, i) => {
        const st = workLogApproval(r);
        const meta = st === 'approved'
            ? `Disetujui ${r.approvedBy || '-'}${r.approvedAt ? ' · ' + new Date(r.approvedAt).toLocaleString('id-ID') : ''}`
            : st === 'revision' ? (r.revisionNote || 'Perlu revisi') : 'Belum diperiksa';
        const docs = leaveDocCount(r);
        const days = Number(r.days) || leaveDays(r.dateFrom, r.dateTo);
        return `
        <tr>
            <td>${i + 1}</td>
            <td data-label="Jenis"><span class="lv-type lv-type--${escapeHtml(r.type || 'izin')}">${escapeHtml(leaveTypeLabel(r))}</span></td>
            <td data-label="Anggota"><strong>${escapeHtml(memberNameOf(r))}</strong></td>
            <td data-label="Perusahaan">${escapeHtml(companyOfRecord(r) || '—')}</td>
            <td data-label="Tanggal">${escapeHtml(leaveRangeLabel(r))}</td>
            <td data-label="Hari">${days}</td>
            <td data-label="Catatan" style="max-width:220px;font-size:12px;color:var(--text-secondary)"
                title="${escapeHtml(r.reason || '')}">${escapeHtml((r.reason || '').slice(0, 60)) || '—'}</td>
            <td data-label="Surat">${docs
                ? `<button type="button" class="wl-photo-btn" title="Lihat ${docs} lembar surat"
                        aria-label="Lihat ${docs} lembar surat"
                        onclick="openLeaveDocs('${escapeHtml(r.id)}', this)"><i class="fas fa-file-image"></i> ${docs}</button>`
                : '<span style="color:var(--text-light)">—</span>'}</td>
            <td data-label="Persetujuan">
                <span class="appr appr--${st}" title="${escapeHtml(meta)}">${escapeHtml(APPROVAL_STATES[st].label)}</span>
                ${canApproveThisLog(r) ? `<span class="appr-actions">
                    ${st !== 'approved' ? `<button class="btn btn-secondary appr-btn" title="Setujui pengajuan" aria-label="Setujui pengajuan" onclick="approveLeave('${escapeHtml(r.id)}')"><i class="fas fa-check"></i></button>` : ''}
                    ${st !== 'revision' ? `<button class="btn btn-secondary appr-btn" title="Minta revisi" aria-label="Minta revisi" onclick="reviseLeave('${escapeHtml(r.id)}')"><i class="fas fa-rotate-left"></i></button>` : ''}
                </span>` : ''}
            </td>
            <td class="col-actions">
                ${canEdit ? `<div class="row-actions">
                    <button class="btn btn-secondary" title="Edit" onclick="editLeave('${escapeHtml(r.id)}')"><i class="fas fa-pen"></i></button>
                    <button class="btn btn-secondary" title="Hapus" onclick="deleteLeave('${escapeHtml(r.id)}')"><i class="fas fa-trash" style="color:var(--danger)"></i></button>
                </div>` : ''}
            </td>
        </tr>`;
    }).join('');
}

// ---- Form ----
let _lvDocs = [];
let _lvDocsDirty = false;
let _lvDocsLoading = false;

function renderLeaveDocs() {
    const count = document.getElementById('lvDocCount');
    if (count) count.textContent = `${_lvDocs.length}/${LEAVE_DOC_MAX}`;
    const wrap = document.getElementById('lvDocPreviews');
    if (!wrap) return;
    if (_lvDocsLoading && !_lvDocs.length) {
        wrap.innerHTML = '<span class="wl-photo__loading"><i class="fas fa-spinner fa-spin"></i> Memuat surat…</span>';
        return;
    }
    wrap.innerHTML = _lvDocs.map((src, i) => `
        <div class="wl-photo">
            <img src="${src}" alt="Surat lembar ${i + 1}" onclick="openPhotoLightbox(_lvDocs, ${i})">
            <button type="button" class="wl-photo__x" aria-label="Hapus surat lembar ${i + 1}"
                    title="Hapus lembar ini" onclick="removeLeaveDoc(${i})">&times;</button>
        </div>`).join('');
}

async function handleLeaveDocChange(event) {
    const files = [...(event.target.files || [])];
    event.target.value = '';
    for (const file of files) {
        if (_lvDocs.length >= LEAVE_DOC_MAX) {
            showToast(`Maksimal ${LEAVE_DOC_MAX} lembar surat`, 'warning');
            break;
        }
        // Nothing in this app reads PDFs, so say that rather than failing
        // silently on a file someone scanned straight from a printer.
        if (!file.type.startsWith('image/')) {
            showToast(`"${file.name}" dilewati — surat harus berupa foto atau hasil pindai gambar, bukan PDF`, 'warning');
            continue;
        }
        try {
            const data = await compressImageToDataURL(file, WORKLOG_PHOTO_OPTS);
            const total = _lvDocs.reduce((a, d) => a + d.length, 0) + data.length;
            if (total > WORKLOG_PHOTOS_TOTAL_BYTES) {
                showToast('Ukuran surat terlalu besar — kurangi jumlah lembarnya', 'warning');
                break;
            }
            _lvDocs.push(data);
            _lvDocsDirty = true;
        } catch (err) {
            showToast(String(err && err.message ? err.message : err), 'error');
        }
    }
    renderLeaveDocs();
}

function removeLeaveDoc(index) {
    _lvDocs.splice(index, 1);
    _lvDocsDirty = true;
    renderLeaveDocs();
}

async function openLeaveDocs(id, btn) {
    const old = btn ? btn.innerHTML : '';
    if (btn) { btn.disabled = true; btn.innerHTML = '<i class="fas fa-spinner fa-spin"></i>'; }
    try {
        const pages = await loadLeaveDocs(id);
        if (!pages.length) { showToast('Surat tidak ditemukan', 'warning'); return; }
        openPhotoLightbox(pages, 0);
    } catch (err) {
        console.error('[izin] gagal memuat surat:', err);
        showToast(navigator.onLine
            ? 'Gagal memuat surat'
            : 'Surat perlu sinyal untuk dimuat', 'warning');
    } finally {
        if (btn) { btn.disabled = false; btn.innerHTML = old; }
    }
}

function showAddLeaveForm() {
    if (!requireEdit('teamLog')) return;
    populateLeaveFilters();
    document.getElementById('leaveForm').reset();
    document.getElementById('editLeaveId').value = '';
    document.getElementById('leaveModalTitle').innerHTML =
        '<i class="fas fa-user-clock"></i> Tambah Pengajuan Izin / Sakit';
    document.getElementById('lvDateFrom').value = toISODate();
    document.getElementById('lvType').value = 'izin';
    _lvDocs = []; _lvDocsDirty = false; _lvDocsLoading = false;
    renderLeaveDocs();
    rememberFocus();
    document.getElementById('leaveModal').classList.add('open');
}

function editLeave(id) {
    if (!requireEdit('teamLog')) return;
    const r = leaveRequests.find(x => x.id === id);
    if (!r) return;
    populateLeaveFilters();
    document.getElementById('editLeaveId').value = r.id;
    document.getElementById('leaveModalTitle').innerHTML =
        '<i class="fas fa-user-clock"></i> Edit Pengajuan Izin / Sakit';
    document.getElementById('lvMember').value = r.memberId || '';
    document.getElementById('lvType').value = r.type || 'izin';
    document.getElementById('lvDateFrom').value = r.dateFrom || '';
    document.getElementById('lvDateTo').value = r.dateTo || '';
    document.getElementById('lvReason').value = r.reason || '';

    _lvDocs = []; _lvDocsDirty = false;
    _lvDocsLoading = leaveDocCount(r) > 0;
    renderLeaveDocs();
    rememberFocus();
    document.getElementById('leaveModal').classList.add('open');
    if (_lvDocsLoading) {
        loadLeaveDocs(r.id)
            .then(pages => { _lvDocs = pages.slice(); })
            .catch(() => showToast('Surat gagal dimuat — menyimpan sekarang tidak akan mengubahnya', 'warning'))
            .finally(() => { _lvDocsLoading = false; renderLeaveDocs(); });
    }
}

function closeLeaveModal(force) {
    if (!force && _lvDocsDirty && _lvDocs.length &&
        !confirm(`${_lvDocs.length} lembar surat belum tersimpan dan akan hilang. Tutup saja?`)) {
        return;
    }
    _lvDocsDirty = false;
    document.getElementById('leaveModal').classList.remove('open');
}

// Rejects with a toast, warns-but-allows with confirm() — the same split the
// work log validation uses. Refusing too much costs as much as accepting too
// much: an extended illness and a corrected date really do overlap.
function checkLeaveRange(memberId, type, from, to, exceptId, docCount) {
    if (!memberId) { showToast('Pilih anggota dulu', 'warning'); return false; }
    if (!from) { showToast('Tanggal mulai wajib diisi', 'warning'); return false; }
    const end = to || from;
    if (end < from) {
        showToast('Tanggal selesai lebih awal dari tanggal mulai', 'warning');
        return false;
    }
    const days = leaveDays(from, end);
    if (days > LEAVE_LONG_DAYS) {
        if (!confirm(`Pengajuan ini ${days} hari (${from} sampai ${end}).\n\n`
            + 'Kalau memang selama itu, tekan OK. Kalau tanggalnya salah ketik, tekan Batal lalu betulkan.')) {
            return false;
        }
    }
    const clash = overlappingLeave(memberId, from, end, exceptId);
    if (clash.length) {
        const list = clash.slice(0, 3)
            .map(c => `· ${leaveTypeLabel(c)} ${leaveRangeLabel(c)} (${APPROVAL_STATES[workLogApproval(c)].label})`)
            .join('\n');
        if (!confirm(`Tanggal ini beririsan dengan pengajuan yang sudah ada:\n\n${list}\n\n`
            + 'Kalau ini perpanjangan yang memang disengaja, tekan OK.')) {
            return false;
        }
    }
    if (type === 'alpa' && docCount > 0) {
        if (!confirm('Alpa berarti tidak masuk TANPA keterangan, tetapi ada surat dilampirkan.\n\n'
            + 'Kalau suratnya memang ada, mungkin jenisnya Izin atau Sakit. Tetap simpan sebagai Alpa?')) {
            return false;
        }
    }
    return true;
}

function saveLeave(event) {
    if (event) event.preventDefault();
    if (!requireEdit('teamLog')) return;

    const id = document.getElementById('editLeaveId').value;
    const existing = id ? leaveRequests.find(x => x.id === id) : null;
    const memberId = document.getElementById('lvMember').value;
    const type = document.getElementById('lvType').value;
    const from = document.getElementById('lvDateFrom').value;
    const to = document.getElementById('lvDateTo').value || from;
    const reason = document.getElementById('lvReason').value.trim();

    const docCount = _lvDocsDirty ? _lvDocs.length : leaveDocCount(existing);
    if (!checkLeaveRange(memberId, type, from, to, id || null, docCount)) return;

    const member = memberById(memberId);
    if (!member) { showToast('Anggota tidak ditemukan', 'warning'); return; }

    const rec = {
        id: id || generateLeaveId(),
        memberId,
        memberName: member.name,
        company: member.company || '',
        type,
        dateFrom: from,
        dateTo: to,
        days: leaveDays(from, to),
        reason,
        docCount,
        // Editing sends the request back for checking. Without this, someone
        // could get three days approved and then change which three days.
        approval: 'pending',
        approvedBy: '', approvedByEmail: '', approvedAt: 0,
        revisionNote: '',
        createdBy: existing ? (existing.createdBy || '') : ((currentUserDoc && currentUserDoc.displayName) || (currentUser && currentUser.email) || ''),
        createdByUid: existing ? (existing.createdByUid || '') : ((currentUser && currentUser.uid) || ''),
        createdAt: existing ? (existing.createdAt || Date.now()) : Date.now(),
        updatedAt: Date.now()
    };

    const wasApproved = existing && workLogApproval(existing) === 'approved';

    cloudWrite(
        {
            action: existing ? 'update' : 'create',
            unitId: '',
            unitName: `[Izin] ${member.name}`,
            field: `${leaveTypeLabel(rec)} ${leaveRangeLabel(rec)}`,
            before: existing ? `${leaveTypeLabel(existing)} ${leaveRangeLabel(existing)}` : '',
            after: `${rec.days} hari${reason ? ' — ' + reason : ''}`
        },
        cloudCall('saveLeaveRequest', rec),
        wasApproved
            ? 'Pengajuan diperbarui — persetujuan dibatalkan, perlu diperiksa ulang'
            : (existing ? 'Pengajuan diperbarui' : 'Pengajuan dicatat'),
        err => {
            console.error('[izin] save failed:', err);
            if (err && err.code === 'permission-denied') showTeamRulesBanner();
            showToast('Gagal menyimpan pengajuan', 'error');
        }
    );

    if (_lvDocsDirty) {
        const pages = _lvDocs.slice();
        _leaveDocCache.set(rec.id, pages);
        const save = cloudFn('saveTeamDocs') && cloudFn('deleteTeamDocs');
        if (!save) { closeLeaveModal(true); return; }
        const write = pages.length
            ? window.cloud.saveTeamDocs(rec.id, pages)
            : window.cloud.deleteTeamDocs(rec.id);
        write.catch(err => {
            console.error('[izin] surat save failed:', err);
            if (err && err.code === 'permission-denied') showTeamRulesBanner();
            showToast('Pengajuan tersimpan, tetapi surat gagal dikirim', 'error');
        });
    }

    closeLeaveModal(true);
}

function deleteLeave(id) {
    if (!requireEdit('teamLog')) return;
    const r = leaveRequests.find(x => x.id === id);
    if (!r) return;
    if (!confirm(`Hapus pengajuan ${leaveTypeLabel(r)} ${memberNameOf(r)} (${leaveRangeLabel(r)})?`)) return;
    // Otherwise the letter document is orphaned: invisible, still billed for.
    if (leaveDocCount(r) > 0 && window.cloud.deleteTeamDocs) {
        _leaveDocCache.delete(id);
        window.cloud.deleteTeamDocs(id).catch(err =>
            console.error('[izin] surat delete failed:', err));
    }
    cloudWrite(
        { action: 'delete', unitId: '',
          unitName: `[Izin] ${memberNameOf(r)}`,
          field: `${leaveTypeLabel(r)} ${leaveRangeLabel(r)}`,
          before: `${r.days || 0} hari`, after: '' },
        cloudCall('deleteLeaveRequest', id),
        'Pengajuan dihapus',
        err => {
            console.error('[izin] delete failed:', err);
            if (err && err.code === 'permission-denied') showTeamRulesBanner();
            showToast('Gagal menghapus pengajuan', 'error');
        }
    );
}

// ---- Approval ----
// Reuses workLogApproval / APPROVAL_STATES / canApproveThisLog wholesale,
// including the rule that you cannot approve a request you filed yourself
// unless you are the owner.
function approveLeave(id) {
    const r = leaveRequests.find(x => x.id === id);
    if (!r) return;
    if (!canApproveWorkLogs()) {
        showToast('Anda tidak punya hak menyetujui pengajuan', 'warning');
        return;
    }
    if (!canApproveThisLog(r)) {
        showToast('Pengajuan yang Anda buat sendiri harus disetujui orang lain', 'warning');
        return;
    }
    if (workLogApproval(r) === 'approved') return;

    const rec = {
        ...r,
        approval: 'approved',
        approvedBy: (currentUserDoc && currentUserDoc.displayName) || (currentUser && currentUser.email) || '',
        approvedByEmail: (currentUser && currentUser.email) || '',
        approvedAt: Date.now(),
        revisionNote: '',
        updatedAt: Date.now()
    };
    cloudWrite(
        { action: 'approve', unitId: '',
          unitName: `[Izin] ${memberNameOf(rec)}`,
          field: `Persetujuan ${leaveRangeLabel(rec)}`,
          before: APPROVAL_STATES[workLogApproval(r)].label,
          after: 'Disetujui' },
        cloudCall('saveLeaveRequest', rec),
        'Pengajuan disetujui',
        err => {
            console.error('[izin] approve failed:', err);
            if (err && err.code === 'permission-denied') showTeamRulesBanner();
            showToast('Gagal menyetujui pengajuan', 'error');
        }
    );
}

function reviseLeave(id) {
    const r = leaveRequests.find(x => x.id === id);
    if (!r) return;
    if (!canApproveThisLog(r)) {
        showToast('Anda tidak punya hak memeriksa pengajuan ini', 'warning');
        return;
    }
    const note = prompt('Apa yang perlu dibetulkan?', r.revisionNote || '');
    if (note === null) return;
    if (!note.trim()) {
        showToast('Tulis dulu apa yang perlu dibetulkan', 'warning');
        return;
    }
    const rec = {
        ...r,
        approval: 'revision',
        revisionNote: note.trim(),
        approvedBy: '', approvedByEmail: '', approvedAt: 0,
        reviewedBy: (currentUserDoc && currentUserDoc.displayName) || (currentUser && currentUser.email) || '',
        reviewedAt: Date.now(),
        updatedAt: Date.now()
    };
    cloudWrite(
        { action: 'reject', unitId: '',
          unitName: `[Izin] ${memberNameOf(rec)}`,
          field: `Persetujuan ${leaveRangeLabel(rec)}`,
          before: APPROVAL_STATES[workLogApproval(r)].label,
          after: `Perlu revisi — ${note.trim()}` },
        cloudCall('saveLeaveRequest', rec),
        'Pengajuan dikembalikan untuk revisi',
        err => {
            console.error('[izin] revise failed:', err);
            if (err && err.code === 'permission-denied') showTeamRulesBanner();
            showToast('Gagal mengirim permintaan revisi', 'error');
        }
    );
}

function exportLeaveCSV() {
    if (!canCsv('export')) return;
    const rows = getFilteredLeave();
    if (rows.length === 0) { showToast('Tidak ada pengajuan untuk diexport', 'warning'); return; }
    const headers = ['No', 'Jenis', 'Anggota', 'Perusahaan', 'Tanggal Mulai', 'Tanggal Selesai',
                     'Jumlah Hari', 'Keterangan', 'Lembar Surat', 'Persetujuan', 'Disetujui Oleh',
                     'Catatan Revisi', 'Dicatat Oleh'];
    const body = rows.map((r, i) => [
        i + 1,
        leaveTypeLabel(r),
        memberNameOf(r),
        companyOfRecord(r) || '',
        r.dateFrom || '',
        r.dateTo || r.dateFrom || '',
        Number(r.days) || leaveDays(r.dateFrom, r.dateTo),
        r.reason || '',
        leaveDocCount(r),
        APPROVAL_STATES[workLogApproval(r)].label,
        r.approvedBy || '',
        r.revisionNote || '',
        r.createdBy || ''
    ]);
    const csv = toCSV(headers, body);
    const blob = new Blob(['﻿' + csv], { type: 'text/csv;charset=utf-8;' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = `izin_sakit_${toISODate()}.csv`;
    a.click();
    URL.revokeObjectURL(url);
    showToast(`Export ${rows.length} pengajuan ke CSV`, 'success');
}

// ============================================================
// WAREHOUSE — serial-tracked devices + quantity-counted stock
// ------------------------------------------------------------
// Two shapes, because the fleet needs both. A GPS receiver is worth
// following one by one: which serial number, and where it is right now.
// A box of filters is not — only how many are left matters.
//
// Both collections are cloud-only, like the team ones, and both sit behind
// the single 'warehouse' access area.
// ============================================================

const DEVICE_STATUSES = [
    { key: 'warehouse', label: 'Di Gudang',  tone: 'info' },
    { key: 'installed', label: 'Terpasang',  tone: 'success' },
    { key: 'damaged',   label: 'Rusak',      tone: 'danger' },
    { key: 'repair',    label: 'Perbaikan',  tone: 'warning' },
    { key: 'retired',   label: 'Afkir',      tone: 'muted' }
];
const DEVICE_STATUS_LABEL = DEVICE_STATUSES.reduce((m, s) => { m[s.key] = s.label; return m; }, {});
const DEFAULT_DEVICE_TYPES = ['GPS / StarFire', 'Display', 'Steering Sensor', 'JDLink', 'Weather Station'];
const STOCK_LOW_THRESHOLD = 5;   // "sisa" at or below this is flagged

let warehouseTab = 'devices';    // 'devices' | 'stock'

function generateDeviceId() {
    return 'dev_' + Date.now() + '_' + Math.random().toString(36).substr(2, 5);
}
function generateStockId() {
    return 'stk_' + Date.now() + '_' + Math.random().toString(36).substr(2, 5);
}

// ---- Suggestion lists, gathered from what is already in use ----
function _mergeSuggestions(defaults, values) {
    const seen = new Map();
    (defaults || []).forEach(v => seen.set(v.toLowerCase(), v));
    (values || []).forEach(raw => {
        const v = (raw || '').trim();
        if (v && !seen.has(v.toLowerCase())) seen.set(v.toLowerCase(), v);
    });
    return [...seen.values()].sort((a, b) => a.localeCompare(b));
}
function allDeviceTypes() {
    // Camera AI, Telematic Box and the rest are devices too, but only worth
    // suggesting once heavy equipment exists.
    const defaults = hasHeavyUnits()
        ? DEFAULT_DEVICE_TYPES.concat(UNIT_GROUPS.heavy.components.map(c => c.label))
        : DEFAULT_DEVICE_TYPES;
    return _mergeSuggestions(defaults, warehouseDevices.map(d => d.type));
}
function allWarehouseLocations() {
    return _mergeSuggestions([], [
        ...warehouseDevices.map(d => d.location),
        ...stockLedger.map(r => r.location)
    ]);
}
function allStockItemNames() {
    return _mergeSuggestions([], stockLedger.map(r => r.itemName));
}

// ---- Cloud snapshots ----
function applyCloudDevicesSnapshot(list) {
    warehouseDevices = (list || []).slice().sort((a, b) =>
        (a.type || '').localeCompare(b.type || '') || (a.sn || '').localeCompare(b.sn || ''));
    if (currentView === 'warehouse') { populateWarehouseFilters(); renderWarehouseView(); }
    scheduleDecisionRefresh();
}

function applyCloudStockSnapshot(list) {
    stockLedger = (list || []).slice().sort((a, b) =>
        String(b.date || '').localeCompare(String(a.date || '')) ||
        ((b.createdAt || 0) - (a.createdAt || 0)));
    if (currentView === 'warehouse') { populateWarehouseFilters(); renderWarehouseView(); }
    scheduleDecisionRefresh();
}

function showWarehouseRulesBanner() {
    const slot = document.querySelector('.warehouse-rules-slot');
    if (!slot || slot.querySelector('.category-rules-banner')) return;
    const banner = document.createElement('div');
    banner.className = 'category-rules-banner';
    banner.innerHTML = `
        <strong><i class="fas fa-triangle-exclamation"></i> Firestore rules memblokir data Gudang.</strong>
        <p>Rules proyek Anda belum mengizinkan akses ke koleksi <code>devices</code> dan
        <code>stockItems</code>. Publish ulang file <code>firestore.rules</code> dari repo ini di
        <em>Firebase Console → Firestore → Rules</em>, lalu muat ulang halaman.</p>`;
    slot.appendChild(banner);
}

// ---- View shell ----
function switchWarehouseTab(tab) {
    warehouseTab = (tab === 'stock') ? 'stock' : 'devices';
    renderWarehouseView();
}

function renderWarehouseView() {
    document.querySelectorAll('.wh-tab').forEach(btn => {
        // Never trust another page to have left these visible.
        btn.style.display = '';
        const on = btn.dataset.tab === warehouseTab;
        btn.classList.toggle('active', on);
        btn.setAttribute('aria-selected', on ? 'true' : 'false');
    });
    const devPanel = document.getElementById('whDevicePanel');
    const stkPanel = document.getElementById('whStockPanel');
    if (devPanel) devPanel.style.display = (warehouseTab === 'devices') ? '' : 'none';
    if (stkPanel) stkPanel.style.display = (warehouseTab === 'stock') ? '' : 'none';

    if (warehouseTab === 'devices') renderDeviceTable();
    else renderStockView();
}

function populateWarehouseFilters() {
    const fill = (id, values, allLabel) => {
        const sel = document.getElementById(id);
        if (!sel) return;
        const keep = sel.value;
        sel.innerHTML = `<option value="">${escapeHtml(allLabel)}</option>` +
            values.map(v => `<option value="${escapeHtml(v)}">${escapeHtml(v)}</option>`).join('');
        if (keep && sel.querySelector(`option[value="${CSS.escape(keep)}"]`)) sel.value = keep;
    };
    fill('devTypeFilter', allDeviceTypes(), 'Semua Jenis');
    fill('devLocationFilter', allWarehouseLocations(), 'Semua Lokasi');
    fill('stkLocationFilter', allWarehouseLocations(), 'Semua Lokasi');
    fill('stkItemFilter', allStockItemNames(), 'Semua Barang');

    const dl = (id, values) => {
        const el = document.getElementById(id);
        if (el) el.innerHTML = values.map(v => `<option value="${escapeHtml(v)}"></option>`).join('');
    };
    dl('deviceTypeList', allDeviceTypes());
    dl('warehouseLocationList', allWarehouseLocations());
    dl('stockItemList', allStockItemNames());

    const unitOpts = [...globalData]
        .sort((a, b) => (a.name || '').localeCompare(b.name || ''))
        .map(u => `<option value="${escapeHtml(damageUnitLabel(u))}"></option>`).join('');
    dl('devUnitList', []);
    const du = document.getElementById('devUnitList');
    if (du) du.innerHTML = unitOpts;
    const su = document.getElementById('stkUnitList');
    if (su) su.innerHTML = unitOpts;
}

// ============================================================
// DEVICES (serial-tracked)
// ============================================================

// Where a device physically is, in one readable phrase.
function deviceWhere(d) {
    if (!d) return '—';
    if (d.status === 'installed') {
        if (d.unitId) {
            const live = globalData.find(u => u.id === d.unitId);
            return live ? (live.name || d.unitName || '—') : (d.unitName || '—');
        }
        return d.siteName || 'Terpasang (lokasi belum diisi)';
    }
    return d.location || '—';
}

// X/Y are free text so UTM easting/northing works as well as decimal
// degrees. Only when both look like real lat/long is a map link offered —
// UTM values fall outside these ranges, so they never produce a wrong pin.
function deviceMapLink(d) {
    const x = parseFloat(d && d.posX), y = parseFloat(d && d.posY);
    if (!isFinite(x) || !isFinite(y)) return '';
    if (Math.abs(y) > 90 || Math.abs(x) > 180) return '';
    if (x === 0 && y === 0) return '';
    return `https://www.google.com/maps?q=${y},${x}`;
}

function getFilteredDevices() {
    const status = document.getElementById('devStatusFilter')?.value || '';
    const type = document.getElementById('devTypeFilter')?.value || '';
    const loc = document.getElementById('devLocationFilter')?.value || '';
    const q = (document.getElementById('devSearch')?.value || '').toLowerCase().trim();
    return warehouseDevices.filter(d => {
        if (status && d.status !== status) return false;
        if (type && d.type !== type) return false;
        if (loc && (d.location || '') !== loc) return false;
        if (q) {
            const hay = [d.sn, d.type, d.brand, d.model, d.location, d.siteName,
                         deviceWhere(d), d.note].join(' ').toLowerCase();
            if (!hay.includes(q)) return false;
        }
        return true;
    });
}

function renderDeviceTable() {
    const rows = getFilteredDevices();
    const tbody = document.getElementById('deviceBody');
    if (!tbody) return;

    // KPI strip counts the whole warehouse, not the filtered slice — it is a
    // standing summary, not a reflection of the filters.
    const setText = (id, v) => { const el = document.getElementById(id); if (el) el.textContent = v; };
    DEVICE_STATUSES.forEach(s => {
        setText('devKpi_' + s.key, warehouseDevices.filter(d => d.status === s.key).length);
    });
    setText('deviceCount', `${rows.length} perangkat`);

    const canEdit = hasAccess('warehouse', 'edit');
    const hasFilter = (document.getElementById('devStatusFilter')?.value || '') ||
                      (document.getElementById('devTypeFilter')?.value || '') ||
                      (document.getElementById('devLocationFilter')?.value || '') ||
                      (document.getElementById('devSearch')?.value || '');

    if (rows.length === 0) {
        tbody.innerHTML = `<tr><td colspan="9" style="text-align:center;padding:24px;color:var(--text-secondary)">${
            hasFilter ? 'Tidak ada perangkat yang cocok dengan filter'
                      : 'Belum ada perangkat. Klik <strong>Tambah Perangkat</strong> untuk mulai.'
        }</td></tr>`;
        return;
    }

    tbody.innerHTML = rows.map((d, i) => {
        const st = DEVICE_STATUSES.find(s => s.key === d.status) || DEVICE_STATUSES[0];
        const map = deviceMapLink(d);
        const pos = (d.posX || d.posY)
            ? `${escapeHtml(d.posX || '—')}, ${escapeHtml(d.posY || '—')}${
                map ? ` <a href="${map}" target="_blank" rel="noopener" title="Buka di peta"><i class="fas fa-map-location-dot"></i></a>` : ''}`
            : '<span style="color:var(--text-light)">—</span>';
        return `
        <tr>
            <td>${i + 1}</td>
            <td data-label="Serial Number" style="font-family:monospace;font-size:12px">${escapeHtml(d.sn || '')}</td>
            <td data-label="Jenis">${d.type ? `<span class="badge badge-cat" style="font-size:10px">${escapeHtml(d.type)}</span>` : '<span style="color:var(--text-light)">—</span>'}</td>
            <td data-label="Merek / Model" style="font-size:12px">${escapeHtml([d.brand, d.model].filter(Boolean).join(' ')) || '<span style="color:var(--text-light)">—</span>'}</td>
            <td data-label="Status"><span class="wh-status wh-status--${st.key}">${escapeHtml(st.label)}</span></td>
            <td data-label="Posisi" style="font-size:12px">${escapeHtml(deviceWhere(d))}</td>
            <td data-label="Koordinat" style="font-size:12px;white-space:nowrap">${pos}</td>
            <td data-label="Catatan" style="max-width:130px;font-size:12px;color:var(--text-secondary)" title="${escapeHtml(d.note || '')}">${
                d.note ? escapeHtml(d.note) : '<span style="color:var(--text-light)">—</span>'}</td>
            <td class="col-actions">
                ${canEdit ? `<div class="row-actions">
                    <button class="btn btn-secondary" title="Edit" aria-label="Edit perangkat" onclick="editDevice('${escapeHtml(d.id)}')"><i class="fas fa-pen"></i></button>
                    <button class="btn btn-secondary" title="Hapus" aria-label="Hapus perangkat" onclick="deleteDevice('${escapeHtml(d.id)}')"><i class="fas fa-trash" style="color:var(--danger)"></i></button>
                </div>` : ''}
            </td>
        </tr>`;
    }).join('');
}

function clearDeviceFilter() {
    ['devStatusFilter', 'devTypeFilter', 'devLocationFilter'].forEach(id => {
        const el = document.getElementById(id); if (el) el.value = '';
    });
    const s = document.getElementById('devSearch'); if (s) s.value = '';
    renderDeviceTable();
}

// Which fields the form shows depends on the status: a device in the
// warehouse needs a shelf, an installed one needs a unit or a map position.
function onDeviceStatusChange() {
    const status = document.getElementById('devStatus').value;
    const installed = status === 'installed';
    const locGroup = document.getElementById('devLocationGroup');
    const instGroup = document.getElementById('devInstallGroup');
    if (locGroup) locGroup.style.display = installed ? 'none' : '';
    if (instGroup) instGroup.style.display = installed ? '' : 'none';
}

function showAddDeviceForm() {
    if (!requireEdit('warehouse')) return;
    document.getElementById('deviceModalTitle').textContent = 'Tambah Perangkat';
    document.getElementById('editDeviceId').value = '';
    document.getElementById('deviceForm').reset();
    document.getElementById('devStatus').value = 'warehouse';
    populateWarehouseFilters();
    onDeviceStatusChange();
    document.getElementById('deviceModal').classList.add('open');
}

function editDevice(id) {
    if (!requireEdit('warehouse')) return;
    const d = warehouseDevices.find(x => x.id === id);
    if (!d) return;
    document.getElementById('deviceModalTitle').textContent = 'Edit Perangkat';
    document.getElementById('editDeviceId').value = d.id;
    populateWarehouseFilters();
    document.getElementById('devSn').value = d.sn || '';
    document.getElementById('devType').value = d.type || '';
    document.getElementById('devBrand').value = d.brand || '';
    document.getElementById('devModel').value = d.model || '';
    document.getElementById('devStatus').value = d.status || 'warehouse';
    document.getElementById('devLocation').value = d.location || '';
    const live = d.unitId ? globalData.find(u => u.id === d.unitId) : null;
    document.getElementById('devUnit').value = live ? damageUnitLabel(live) : (d.unitName || '');
    document.getElementById('devSite').value = d.siteName || '';
    document.getElementById('devPosX').value = d.posX || '';
    document.getElementById('devPosY').value = d.posY || '';
    document.getElementById('devNote').value = d.note || '';
    onDeviceStatusChange();
    document.getElementById('deviceModal').classList.add('open');
}

function closeDeviceModal() {
    document.getElementById('deviceModal').classList.remove('open');
}

function saveDevice(event) {
    event.preventDefault();
    if (!requireEdit('warehouse')) return;

    const id = document.getElementById('editDeviceId').value;
    const sn = (document.getElementById('devSn').value || '').trim();
    if (!sn) { showToast('Serial number tidak boleh kosong', 'warning'); return; }
    // The serial number is the identity here, so a duplicate would make
    // "where is this device" unanswerable.
    const clash = warehouseDevices.find(d =>
        d.id !== id && (d.sn || '').toLowerCase() === sn.toLowerCase());
    if (clash) { showToast(`SN "${sn}" sudah terdaftar`, 'warning'); return; }

    const status = document.getElementById('devStatus').value || 'warehouse';
    const installed = status === 'installed';

    let unit = null;
    const unitRaw = (document.getElementById('devUnit').value || '').trim();
    if (installed && unitRaw) {
        unit = resolveDamageUnit(unitRaw);
        if (!unit) {
            showToast(`Unit "${unitRaw}" tidak ditemukan — pilih dari daftar`, 'warning');
            return;
        }
    }
    const siteName = installed ? (document.getElementById('devSite').value || '').trim() : '';
    if (installed && !unit && !siteName) {
        showToast('Perangkat terpasang: isi unit atau nama lokasi pemasangan', 'warning');
        return;
    }

    const existing = id ? warehouseDevices.find(d => d.id === id) : null;
    const rec = {
        id: id || generateDeviceId(),
        sn,
        type: (document.getElementById('devType').value || '').trim(),
        brand: (document.getElementById('devBrand').value || '').trim(),
        model: (document.getElementById('devModel').value || '').trim(),
        status,
        location: installed ? '' : (document.getElementById('devLocation').value || '').trim(),
        unitId: unit ? unit.id : '',
        unitName: unit ? (unit.name || '') : '',
        siteName,
        posX: installed ? (document.getElementById('devPosX').value || '').trim() : '',
        posY: installed ? (document.getElementById('devPosY').value || '').trim() : '',
        note: (document.getElementById('devNote').value || '').trim(),
        createdAt: existing ? (existing.createdAt || Date.now()) : Date.now(),
        updatedAt: Date.now()
    };

    cloudWrite(
        {
            action: existing ? 'update' : 'create',
            unitId: rec.unitId,
            unitName: `[Gudang] ${rec.type || 'Perangkat'} ${rec.sn}`,
            field: 'Perangkat',
            before: existing ? `${DEVICE_STATUS_LABEL[existing.status] || existing.status} · ${deviceWhere(existing)}` : '',
            after: `${DEVICE_STATUS_LABEL[rec.status] || rec.status} · ${deviceWhere(rec)}`
        },
        cloudCall('saveDevice', rec),
        existing ? 'Perangkat diperbarui' : 'Perangkat ditambahkan',
        err => {
            console.error('[warehouse] device save failed:', err);
            if (err && err.code === 'permission-denied') showWarehouseRulesBanner();
            showToast('Gagal menyimpan perangkat', 'error');
        }
    );

    closeDeviceModal();
}

function deleteDevice(id) {
    if (!requireEdit('warehouse')) return;
    const d = warehouseDevices.find(x => x.id === id);
    if (!d) return;
    if (!confirm(`Hapus perangkat ${d.type || ''} SN ${d.sn}?\n\nRiwayatnya di audit log tetap tersimpan.`)) return;
    cloudWrite(
        { action: 'delete',
          unitName: `[Gudang] ${d.type || 'Perangkat'} ${d.sn}`,
          field: 'Perangkat',
          before: `${DEVICE_STATUS_LABEL[d.status] || d.status} · ${deviceWhere(d)}`,
          after: '' },
        cloudCall('deleteDevice', id),
        'Perangkat dihapus',
        err => {
            console.error('[warehouse] device delete failed:', err);
            showToast('Gagal menghapus perangkat', 'error');
        }
    );
}

function exportDeviceCSV() {
    if (!canCsv('export')) return;
    const rows = getFilteredDevices();
    if (rows.length === 0) { showToast('Tidak ada perangkat untuk diexport', 'warning'); return; }
    const headers = ['No', 'Serial Number', 'Jenis', 'Merek', 'Model', 'Status',
                     'Lokasi Gudang', 'Terpasang di Unit', 'Lokasi Pemasangan', 'Posisi X', 'Posisi Y', 'Catatan'];
    const dataRows = rows.map((d, i) => [
        i + 1, d.sn || '', d.type || '', d.brand || '', d.model || '',
        DEVICE_STATUS_LABEL[d.status] || d.status || '',
        d.location || '',
        d.unitId ? deviceWhere(d) : '',
        d.siteName || '', d.posX || '', d.posY || '', d.note || ''
    ]);
    const csv = toCSV(headers, dataRows);
    const blob = new Blob(['﻿' + csv], { type: 'text/csv;charset=utf-8;' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = `perangkat_gudang_${toISODate()}.csv`;
    a.click();
    URL.revokeObjectURL(url);
    showToast(`Export ${rows.length} perangkat ke CSV`, 'success');
}

// Devices fitted to one tractor — surfaced on the unit profile.
function devicesForUnit(unitId) {
    if (!unitId) return [];
    return warehouseDevices.filter(d => d.status === 'installed' && d.unitId === unitId);
}

// ============================================================
// STOCK (counted by quantity)
// ============================================================

// Remaining per item, optionally within one location. IN adds, OUT removes.
// Warns, but lets the owner through: a stocktake can legitimately find the
// ledger behind reality. Editing an existing OUT adds its old quantity back to
// what is available, or the edit would warn about a shortage it is itself
// creating.
function checkStockBalance(itemName, qty, existing) {
    const key = (itemName || '').trim().toLowerCase();
    const row = stockSummary().find(s => s.name.toLowerCase() === key);
    let available = row ? row.qty : 0;
    if (existing && existing.txnType === 'OUT' &&
        (existing.itemName || '').trim().toLowerCase() === key) {
        available += Number(existing.qty) || 0;
    }
    if (qty <= available) return true;
    return confirm(`Stok "${itemName}" tinggal ${available}, tetapi keluar ${qty}.\n\n`
        + 'Saldo akan jadi minus. Biasanya ini karena nama barangnya beda tipis '
        + 'dari yang sudah ada.\n\nTetap simpan?');
}

function stockSummary(location) {
    const totals = new Map();
    stockLedger.forEach(r => {
        if (location && (r.location || '') !== location) return;
        const name = (r.itemName || '').trim();
        if (!name) return;
        const key = name.toLowerCase();
        const qty = (Number(r.qty) || 0) * (r.txnType === 'OUT' ? -1 : 1);
        const cur = totals.get(key) || { name, qty: 0 };
        cur.qty += qty;
        totals.set(key, cur);
    });
    return [...totals.values()].sort((a, b) => a.name.localeCompare(b.name));
}

function getFilteredStock() {
    const from = (document.getElementById('stkFrom')?.value || '').trim();
    const to = (document.getElementById('stkTo')?.value || '').trim();
    const item = document.getElementById('stkItemFilter')?.value || '';
    const loc = document.getElementById('stkLocationFilter')?.value || '';
    const type = document.getElementById('stkTypeFilter')?.value || '';
    const q = (document.getElementById('stkSearch')?.value || '').toLowerCase().trim();
    return stockLedger.filter(r => {
        if (from && String(r.date || '') < from) return false;
        if (to && String(r.date || '') > to) return false;
        if (item && (r.itemName || '') !== item) return false;
        if (loc && (r.location || '') !== loc) return false;
        if (type && r.txnType !== type) return false;
        if (q) {
            const hay = [r.itemName, r.location, r.unitName, r.note].join(' ').toLowerCase();
            if (!hay.includes(q)) return false;
        }
        return true;
    });
}

function renderStockView() {
    const loc = document.getElementById('stkLocationFilter')?.value || '';
    const summary = stockSummary(loc);
    const sumEl = document.getElementById('stockSummary');
    if (sumEl) {
        sumEl.innerHTML = summary.length
            ? summary.map(s => `
                <div class="stock-chip${s.qty <= 0 ? ' stock-chip--out' : (s.qty <= STOCK_LOW_THRESHOLD ? ' stock-chip--low' : '')}">
                    <span class="stock-chip__name">${escapeHtml(s.name)}</span>
                    <span class="stock-chip__qty">${s.qty}</span>
                </div>`).join('')
            : '<div class="stock-empty">Belum ada barang tercatat.</div>';
    }

    const rows = getFilteredStock();
    const tbody = document.getElementById('stockBody');
    if (!tbody) return;
    const countEl = document.getElementById('stockCount');
    if (countEl) countEl.textContent = `${rows.length} transaksi`;

    const canEdit = hasAccess('warehouse', 'edit');
    if (rows.length === 0) {
        tbody.innerHTML = `<tr><td colspan="8" style="text-align:center;padding:24px;color:var(--text-secondary)">
            Belum ada transaksi stok. Klik <strong>Catat Masuk/Keluar</strong> untuk mulai.
        </td></tr>`;
        return;
    }

    tbody.innerHTML = rows.map((r, i) => `
        <tr>
            <td>${i + 1}</td>
            <td data-label="Tanggal" style="white-space:nowrap">${escapeHtml(r.date || '')}</td>
            <td data-label="Jenis"><span class="wh-txn wh-txn--${r.txnType === 'OUT' ? 'out' : 'in'}">${r.txnType === 'OUT' ? 'Keluar' : 'Masuk'}</span></td>
            <td data-label="Barang"><strong>${escapeHtml(r.itemName || '')}</strong></td>
            <td data-label="Jumlah" style="white-space:nowrap">${Number(r.qty) || 0}</td>
            <td data-label="Lokasi" style="font-size:12px">${escapeHtml(r.location || '') || '<span style="color:var(--text-light)">—</span>'}</td>
            <td data-label="Untuk Unit" style="font-size:12px">${escapeHtml(r.unitName || '') || '<span style="color:var(--text-light)">—</span>'}</td>
            <td class="col-actions">
                ${canEdit ? `<div class="row-actions">
                    <button class="btn btn-secondary" title="Edit" aria-label="Edit transaksi" onclick="editStockItem('${escapeHtml(r.id)}')"><i class="fas fa-pen"></i></button>
                    <button class="btn btn-secondary" title="Hapus" aria-label="Hapus transaksi" onclick="deleteStockItem('${escapeHtml(r.id)}')"><i class="fas fa-trash" style="color:var(--danger)"></i></button>
                </div>` : ''}
            </td>
        </tr>`).join('');
}

function clearStockFilter() {
    ['stkFrom', 'stkTo', 'stkSearch'].forEach(id => {
        const el = document.getElementById(id); if (el) el.value = '';
    });
    ['stkItemFilter', 'stkLocationFilter', 'stkTypeFilter'].forEach(id => {
        const el = document.getElementById(id); if (el) el.value = '';
    });
    renderStockView();
}

function onStockTxnChange() {
    const isOut = document.getElementById('stkTxnType').value === 'OUT';
    const g = document.getElementById('stkUnitGroup');
    if (g) g.style.display = isOut ? '' : 'none';
}

function showAddStockForm() {
    if (!requireEdit('warehouse')) return;
    document.getElementById('stockModalTitle').textContent = 'Catat Stok Masuk/Keluar';
    document.getElementById('editStockId').value = '';
    document.getElementById('stockForm').reset();
    populateWarehouseFilters();
    document.getElementById('stkDate').value = toISODate();
    document.getElementById('stkTxnType').value = 'IN';
    onStockTxnChange();
    document.getElementById('stockModal').classList.add('open');
}

function editStockItem(id) {
    if (!requireEdit('warehouse')) return;
    const r = stockLedger.find(x => x.id === id);
    if (!r) return;
    document.getElementById('stockModalTitle').textContent = 'Edit Transaksi Stok';
    document.getElementById('editStockId').value = r.id;
    populateWarehouseFilters();
    document.getElementById('stkDate').value = r.date || '';
    document.getElementById('stkTxnType').value = r.txnType || 'IN';
    document.getElementById('stkItem').value = r.itemName || '';
    document.getElementById('stkQty').value = Number(r.qty) || 1;
    document.getElementById('stkLocation').value = r.location || '';
    const live = r.unitId ? globalData.find(u => u.id === r.unitId) : null;
    document.getElementById('stkUnit').value = live ? damageUnitLabel(live) : (r.unitName || '');
    document.getElementById('stkNote').value = r.note || '';
    onStockTxnChange();
    document.getElementById('stockModal').classList.add('open');
}

function closeStockModal() {
    document.getElementById('stockModal').classList.remove('open');
}

function saveStockItem(event) {
    event.preventDefault();
    if (!requireEdit('warehouse')) return;

    const id = document.getElementById('editStockId').value;
    const itemName = (document.getElementById('stkItem').value || '').trim();
    if (!itemName) { showToast('Isi nama barang', 'warning'); return; }
    const qty = Math.max(1, parseInt(document.getElementById('stkQty').value, 10) || 1);
    const txnType = document.getElementById('stkTxnType').value === 'OUT' ? 'OUT' : 'IN';

    let unit = null;
    const unitRaw = (document.getElementById('stkUnit').value || '').trim();
    if (txnType === 'OUT' && unitRaw) {
        unit = resolveDamageUnit(unitRaw);
        if (!unit) {
            showToast(`Unit "${unitRaw}" tidak ditemukan — kosongkan atau pilih dari daftar`, 'warning');
            return;
        }
    }

    const existing = id ? stockLedger.find(r => r.id === id) : null;

    // Taking out more than the ledger holds means either a miscount or, more
    // often, a typo in the item name that created a phantom item whose real
    // counterpart never gets decremented. The licence ledger already warns
    // like this (saveLicenseStock); the warehouse one did not, and
    // stockSummary would happily render a negative balance.
    if (txnType === 'OUT' && !checkStockBalance(itemName, qty, existing)) return;

    const rec = {
        id: id || generateStockId(),
        date: document.getElementById('stkDate').value,
        txnType,
        itemName,
        qty,
        location: (document.getElementById('stkLocation').value || '').trim(),
        unitId: unit ? unit.id : '',
        unitName: unit ? (unit.name || '') : '',
        note: (document.getElementById('stkNote').value || '').trim(),
        createdAt: existing ? (existing.createdAt || Date.now()) : Date.now(),
        updatedAt: Date.now()
    };

    cloudWrite(
        {
            action: existing ? 'update' : 'create',
            unitId: rec.unitId,
            unitName: `[Gudang] ${rec.itemName}`,
            field: txnType === 'OUT' ? 'Stok keluar' : 'Stok masuk',
            before: existing ? `${existing.txnType} ${existing.qty}` : '',
            after: `${txnType} ${qty}${rec.location ? ' @ ' + rec.location : ''}`
        },
        cloudCall('saveStockItem', rec),
        existing ? 'Transaksi diperbarui' : 'Transaksi dicatat',
        err => {
            console.error('[warehouse] stock save failed:', err);
            if (err && err.code === 'permission-denied') showWarehouseRulesBanner();
            showToast('Gagal menyimpan transaksi', 'error');
        }
    );

    closeStockModal();
}

function deleteStockItem(id) {
    if (!requireEdit('warehouse')) return;
    const r = stockLedger.find(x => x.id === id);
    if (!r) return;
    if (!confirm(`Hapus transaksi ${r.txnType === 'OUT' ? 'keluar' : 'masuk'} ${r.qty} ${r.itemName} (${r.date})?`)) return;
    cloudWrite(
        { action: 'delete',
          unitName: `[Gudang] ${r.itemName}`,
          field: 'Transaksi stok',
          before: `${r.txnType} ${r.qty}`,
          after: '' },
        cloudCall('deleteStockItem', id),
        'Transaksi dihapus',
        err => {
            console.error('[warehouse] stock delete failed:', err);
            showToast('Gagal menghapus transaksi', 'error');
        }
    );
}

function exportStockCSV() {
    if (!canCsv('export')) return;
    const rows = getFilteredStock();
    if (rows.length === 0) { showToast('Tidak ada transaksi untuk diexport', 'warning'); return; }
    const headers = ['No', 'Tanggal', 'Jenis', 'Barang', 'Jumlah', 'Lokasi', 'Untuk Unit', 'Catatan'];
    const dataRows = rows.map((r, i) => [
        i + 1, r.date || '', r.txnType === 'OUT' ? 'Keluar' : 'Masuk',
        r.itemName || '', Number(r.qty) || 0, r.location || '', r.unitName || '', r.note || ''
    ]);
    const csv = toCSV(headers, dataRows);
    const blob = new Blob(['﻿' + csv], { type: 'text/csv;charset=utf-8;' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = `stok_gudang_${toISODate()}.csv`;
    a.click();
    URL.revokeObjectURL(url);
    showToast(`Export ${rows.length} transaksi ke CSV`, 'success');
}


// ============================================================
// KOTAK KEPUTUSAN — everything waiting on the person in charge
// ------------------------------------------------------------
// Nothing here is newly computed: the licence alerts, the low-stock lists and
// the downtime clocks already existed, each on its own page. What was missing
// was one place that answers "what needs me today", so a backlog on a page
// nobody opened stayed invisible.
//
// Every group is gated by the area it reads from, so this page never becomes a
// way around the access model.
// ============================================================

const LEADER_BREAKDOWN_DAYS = 3;   // a unit down longer than this needs a call

function _daysSince(ms) {
    if (!ms) return 0;
    return Math.floor((Date.now() - ms) / 86400000);
}

// Returns [{ key, title, icon, tone, total, items:[{text, sub}], goto }]
// Only non-empty groups come back — an empty inbox should look empty.
function decisionGroups() {
    const groups = [];
    const add = g => { if (g.total > 0) groups.push(g); };

    // ---- Laporan harian menunggu diperiksa ----
    if (hasAccess('teamLog', 'view') || hasAccess('teamLogApprove', 'edit')) {
        const pending = workLogs.filter(w => workLogApproval(w) === 'pending');
        add({
            key: 'approval', icon: 'clipboard-check', tone: 'warning',
            title: 'Laporan menunggu persetujuan',
            total: pending.length,
            items: pending.slice(0, 6).map(w => ({
                text: `${memberNameOf(w)} · ${w.date}`,
                sub: (w.task || '').slice(0, 70)
            })),
            goto: 'approval'
        });

        const revision = workLogs.filter(w => workLogApproval(w) === 'revision');
        add({
            key: 'revision', icon: 'rotate-left', tone: 'danger',
            title: 'Laporan diminta revisi, belum diperbaiki',
            total: revision.length,
            items: revision.slice(0, 6).map(w => ({
                text: `${memberNameOf(w)} · ${w.date}`,
                sub: w.revisionNote || ''
            })),
            goto: 'revision'
        });

        // Izin / sakit menunggu persetujuan. Digerbang sama dengan laporan,
        // karena areanya memang sama.
        const izin = leaveRequests.filter(r => workLogApproval(r) === 'pending');
        add({
            key: 'leave', icon: 'user-clock', tone: 'warning',
            title: 'Izin / sakit menunggu persetujuan',
            total: izin.length,
            items: izin.slice(0, 6).map(r => ({
                text: `${memberNameOf(r)} · ${leaveTypeLabel(r)}`,
                sub: `${leaveRangeLabel(r)} — ${Number(r.days) || leaveDays(r.dateFrom, r.dateTo)} hari`
            })),
            goto: 'leave'
        });
    }

    // ---- Lisensi habis / segera habis ----
    if (hasAccess('editUnits', 'view')) {
        const alerts = _buildAlertList();
        add({
            key: 'license', icon: 'key', tone: alerts.expiredCount ? 'danger' : 'warning',
            title: alerts.expiredCount
                ? `Lisensi kedaluwarsa (${alerts.expiredCount}) dan segera habis (${alerts.soonCount})`
                : 'Lisensi segera habis',
            total: alerts.total,
            // _buildAlertList already sorts soonest-first and formats each line.
            items: alerts.lines.slice(0, 6).map(line => {
                const p = line.split(' | ');
                return { text: `${p[1] || '-'} · ${p[4] || '-'}`, sub: `${p[6] || ''} — ${p[7] || ''}` };
            }),
            goto: 'editUnits'
        });

        // ---- Unit breakdown terlalu lama ----
        const stuck = globalData
            .filter(u => u.breakdownStartedAt && _daysSince(u.breakdownStartedAt) >= LEADER_BREAKDOWN_DAYS)
            .sort((a, b) => a.breakdownStartedAt - b.breakdownStartedAt);
        add({
            key: 'breakdown', icon: 'triangle-exclamation', tone: 'danger',
            title: `Unit breakdown lebih dari ${LEADER_BREAKDOWN_DAYS} hari`,
            total: stuck.length,
            items: stuck.slice(0, 6).map(u => ({
                text: u.name || u.sn || '(tanpa nama)',
                sub: `${_daysSince(u.breakdownStartedAt)} hari · ${u.site || '-'}${u.breakdownReason ? ' · ' + u.breakdownReason : ''}${isHeavy(u) ? ' · ' + UNIT_GROUPS.heavy.shortLabel : ''}`
            })),
            // With a heavy unit in the list, open the dashboard on "Semua" so
            // the unit is actually on screen when the link lands.
            goto: stuck.some(isHeavy) ? 'dashboard:all' : 'dashboard'
        });
    }

    // ---- Stok lisensi menipis ----
    if (hasAccess('licenseStock', 'view')) {
        const low = _lowStockList();
        add({
            key: 'licenseStock', icon: 'layer-group', tone: 'warning',
            title: 'Stok lisensi menipis',
            total: low.length,
            items: low.slice(0, 6).map(l => ({ text: l.type, sub: `sisa ${l.sisa}` })),
            goto: 'licenseStock'
        });
    }

    // ---- Gudang: barang habis, dan perangkat yang tidak sehat ----
    if (hasAccess('warehouse', 'view')) {
        const low = stockSummary('').filter(s => s.qty <= STOCK_LOW_THRESHOLD);
        add({
            key: 'stock', icon: 'boxes-stacked',
            tone: low.some(s => s.qty <= 0) ? 'danger' : 'warning',
            title: 'Stok barang habis atau menipis',
            total: low.length,
            items: low.slice(0, 6).map(s => ({
                text: s.name,
                sub: s.qty <= 0 ? 'habis' : `sisa ${s.qty}`
            })),
            goto: 'warehouseStock'
        });

        const unwell = warehouseDevices.filter(d => d.status === 'damaged' || d.status === 'repair');
        add({
            key: 'devices', icon: 'microchip', tone: 'warning',
            title: 'Perangkat rusak atau sedang diperbaiki',
            total: unwell.length,
            items: unwell.slice(0, 6).map(d => ({
                text: `${d.type || 'Perangkat'} · ${d.sn || ''}`,
                sub: `${DEVICE_STATUS_LABEL[d.status] || d.status}${d.note ? ' · ' + d.note.slice(0, 40) : ''}`
            })),
            goto: 'warehouseDevices'
        });
    }

    // ---- Pendaftar menunggu disetujui ----
    if (isOwner()) {
        const pendingUsers = allUsers.filter(u => u.status !== 'active');
        add({
            key: 'users', icon: 'user-plus', tone: 'info',
            title: 'Pendaftar menunggu persetujuan',
            total: pendingUsers.length,
            items: pendingUsers.slice(0, 6).map(u => ({
                text: u.displayName || u.email || '(tanpa nama)',
                sub: u.email || ''
            })),
            goto: 'users'
        });

        // An approved account with no area granted cannot do anything; that is
        // a half-finished action, so it belongs here too.
        const noAccess = allUsers.filter(u =>
            u.status === 'active' && u.role !== 'owner' &&
            ACCESS_AREAS.every(a => effectiveAccess(a.key, u) === 'none'));
        add({
            key: 'noaccess', icon: 'user-lock', tone: 'warning',
            title: 'Akun aktif tapi belum diberi akses',
            total: noAccess.length,
            items: noAccess.slice(0, 6).map(u => ({
                text: u.displayName || u.email || '(tanpa nama)',
                sub: `${roleLabel(u.role)} — belum bisa membuka apa pun`
            })),
            goto: 'users'
        });
    }

    return groups;
}

function decisionTotal() {
    return decisionGroups().reduce((n, g) => n + g.total, 0);
}

// Each group knows where its work actually lives; landing on the right tab with
// the right filter already applied is the difference between a list and a tool.
function goDecision(target) {
    // Group-aware links. They switch the group for this session only — a link
    // must not change the device's saved default — and do nothing when
    // navigateTo refused the view for lack of access.
    if (typeof target === 'string' && target.startsWith('editUnits:')) {
        navigateTo('editUnits');
        if (currentView === 'editUnits') switchEditUnitsGroup(target.slice(10), { persist: false });
        return;
    }
    if (typeof target === 'string' && target.startsWith('dashboard:')) {
        navigateTo('dashboard');
        if (currentView === 'dashboard') setDashGroup(target.slice(10), { persist: false });
        return;
    }
    switch (target) {
        case 'leave': {
            navigateTo('team');
            switchTeamTab('leave');
            const f = document.getElementById('lvApprovalFilter');
            if (f) { f.value = 'pending'; renderLeaveTable(); }
            break;
        }
        case 'approval':
        case 'revision': {
            navigateTo('team');
            switchTeamTab('worklog');
            const f = document.getElementById('wlApprovalFilter');
            if (f) { f.value = target === 'approval' ? 'pending' : 'revision'; renderWorkLogTable(); }
            break;
        }
        case 'teamLog':
            navigateTo('team');
            switchTeamTab('worklog');
            break;
        case 'warehouseStock':
            navigateTo('warehouse');
            switchWarehouseTab('stock');
            break;
        case 'warehouseDevices':
            navigateTo('warehouse');
            switchWarehouseTab('devices');
            break;
        default:
            navigateTo(target);
    }
}

function renderDecisionInbox() {
    const wrap = document.getElementById('decisionGroups');
    if (!wrap) return;
    const groups = decisionGroups();
    const total = groups.reduce((n, g) => n + g.total, 0);
    // Handling something here changes the count, so the badge has to follow —
    // otherwise it keeps advertising work that is already done.
    updateDecisionBadge();

    const stamp = document.getElementById('printStamp');
    if (stamp) stamp.textContent = `Dicetak ${formatUserTime(Date.now())}`;

    const countEl = document.getElementById('decisionTotal');
    if (countEl) countEl.textContent = total;
    const subEl = document.getElementById('decisionTotalSub');
    if (subEl) {
        subEl.textContent = total === 0
            ? 'Tidak ada yang menunggu keputusan Anda'
            : `Tersebar di ${groups.length} bagian`;
    }

    if (groups.length === 0) {
        wrap.innerHTML = `<div class="decision-clear">
            <i class="fas fa-circle-check"></i>
            <div>
                <strong>Tidak ada yang menunggu.</strong>
                <p>Semua laporan sudah diperiksa, tidak ada lisensi atau stok yang perlu ditindak,
                dan tidak ada unit yang terlalu lama berhenti.</p>
            </div>
        </div>`;
        return;
    }

    wrap.innerHTML = groups.map(g => `
        <div class="decision-card decision-card--${g.tone}">
            <div class="decision-card__head">
                <span class="decision-card__title">
                    <i class="fas fa-${g.icon}"></i> ${escapeHtml(g.title)}
                </span>
                <span class="decision-card__count">${g.total}</span>
            </div>
            <ul class="decision-list">
                ${g.items.map(it => `
                    <li>
                        <span class="decision-list__text">${escapeHtml(it.text)}</span>
                        ${it.sub ? `<span class="decision-list__sub">${escapeHtml(it.sub)}</span>` : ''}
                    </li>`).join('')}
                ${g.total > g.items.length
                    ? `<li class="decision-list__more">…dan ${g.total - g.items.length} lagi</li>`
                    : ''}
            </ul>
            <button class="btn btn-secondary btn-sm" onclick="goDecision('${g.goto}')">
                Tangani <i class="fas fa-arrow-right"></i>
            </button>
        </div>`).join('');
}

// Snapshots arrive in bursts at sign-in, and the inbox reads every collection,
// so recomputing per snapshot would be wasteful. One pass once the dust
// settles is enough.
let _decisionTimer = null;
function scheduleDecisionRefresh() {
    if (_decisionTimer) return;
    _decisionTimer = setTimeout(() => {
        _decisionTimer = null;
        updateDecisionBadge();
        if (currentView === 'leader') renderDecisionInbox();
    }, 250);
}

// The sidebar badge is what makes this page get opened at all.
function updateDecisionBadge() {
    const el = document.getElementById('decisionBadge');
    if (!el) return;
    if (!canViewView('leader')) { el.style.display = 'none'; return; }
    const n = decisionTotal();
    el.textContent = n > 99 ? '99+' : String(n);
    el.style.display = n > 0 ? '' : 'none';
}


// ============================================================
// REKAP PER PERUSAHAAN + RINGKASAN MINGGUAN
// ------------------------------------------------------------
// Both read only what the daily reports already carry. The company recap
// exists because the hours are billable between two PTs and nothing was ever
// adding them up; the weekly summary exists because "how did this week go"
// should not require opening five pages.
// ============================================================

let leaderTab = 'inbox';        // 'inbox' | 'company' | 'week'
let recapMonth = '';            // 'YYYY-MM'
let weekAnchor = '';            // ISO date inside the week being shown

function currentMonthISO() {
    return toISODate().slice(0, 7);
}

function monthLabel(iso) {
    const m = /^(\d{4})-(\d{2})$/.exec(iso || '');
    if (!m) return iso || '';
    return `${MONTH_NAMES[+m[2] - 1]} ${m[1]}`;
}

// ---- B2: per-company recap ----
// One row per company for the chosen month. Reports whose member has no
// company yet are grouped rather than dropped, so the totals always add up to
// the month's real figures.
function companyRecap(monthISO) {
    const rows = new Map();
    workLogs.forEach(w => {
        const d = String(w.date || '');
        if (monthISO && d.slice(0, 7) !== monthISO) return;
        const key = companyOfRecord(w) || NO_COMPANY;
        const cur = rows.get(key) || {
            company: key, minutes: 0, reports: 0,
            approved: 0, pending: 0, revision: 0,
            units: new Set(), members: new Set()
        };
        cur.minutes += workLogMinutes(w);
        cur.reports += 1;
        cur[workLogApproval(w)] += 1;
        workLogUnits(w).forEach(u => cur.units.add(u.id || u.sn || ''));
        if (w.memberId) cur.members.add(w.memberId);
        rows.set(key, cur);
    });
    return [...rows.values()]
        .map(r => ({ ...r, units: r.units.size, members: r.members.size }))
        .sort((a, b) => b.minutes - a.minutes);
}

function renderCompanyRecap() {
    if (!recapMonth) recapMonth = currentMonthISO();
    const picker = document.getElementById('recapMonth');
    if (picker && picker.value !== recapMonth) picker.value = recapMonth;
    const label = document.getElementById('recapMonthLabel');
    if (label) label.textContent = monthLabel(recapMonth);

    const rows = companyRecap(recapMonth);
    const tbody = document.getElementById('recapBody');
    if (!tbody) return;

    if (rows.length === 0) {
        tbody.innerHTML = `<tr><td colspan="8" style="text-align:center;padding:24px;color:var(--text-secondary)">
            Belum ada laporan harian pada ${escapeHtml(monthLabel(recapMonth))}.
        </td></tr>`;
        const f = document.getElementById('recapFoot');
        if (f) f.innerHTML = '';
        return;
    }

    tbody.innerHTML = rows.map((r, i) => `
        <tr>
            <td>${i + 1}</td>
            <td data-label="Perusahaan"><strong>${escapeHtml(r.company)}</strong></td>
            <td data-label="Anggota">${r.members}</td>
            <td data-label="Laporan">${r.reports}</td>
            <td data-label="Total Jam" style="white-space:nowrap"><strong>${escapeHtml(formatMinutes(r.minutes))}</strong></td>
            <td data-label="Unit Ditangani">${r.units}</td>
            <td data-label="Disetujui"><span class="appr appr--approved">${r.approved}</span></td>
            <td data-label="Belum Selesai">
                ${r.pending ? `<span class="appr appr--pending">${r.pending} menunggu</span> ` : ''}
                ${r.revision ? `<span class="appr appr--revision">${r.revision} revisi</span>` : ''}
                ${(!r.pending && !r.revision) ? '<span style="color:var(--text-light)">—</span>' : ''}
            </td>
        </tr>`).join('');

    // A totals row is what makes this usable for invoicing: the figure at the
    // bottom is the one that goes on the sheet.
    const tot = rows.reduce((a, r) => ({
        members: a.members + r.members, reports: a.reports + r.reports,
        minutes: a.minutes + r.minutes, units: a.units + r.units,
        approved: a.approved + r.approved
    }), { members: 0, reports: 0, minutes: 0, units: 0, approved: 0 });
    const foot = document.getElementById('recapFoot');
    if (foot) {
        foot.innerHTML = `<tr>
            <th scope="row" colspan="2">Total</th>
            <td>${tot.members}</td>
            <td>${tot.reports}</td>
            <td style="white-space:nowrap"><strong>${escapeHtml(formatMinutes(tot.minutes))}</strong></td>
            <td>${tot.units}</td>
            <td>${tot.approved}</td>
            <td></td>
        </tr>`;
    }
}

function shiftRecapMonth(delta) {
    if (!recapMonth) recapMonth = currentMonthISO();
    const [y, m] = recapMonth.split('-').map(Number);
    const d = new Date(y, m - 1 + delta, 1);
    recapMonth = `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, '0')}`;
    renderCompanyRecap();
}

function onRecapMonthChange() {
    const el = document.getElementById('recapMonth');
    if (el && el.value) recapMonth = el.value;
    renderCompanyRecap();
}

function exportRecapCSV() {
    if (!canCsv('export')) return;
    const rows = companyRecap(recapMonth);
    if (rows.length === 0) { showToast('Tidak ada data untuk diexport', 'warning'); return; }
    const headers = ['Perusahaan', 'Bulan', 'Jumlah Anggota', 'Jumlah Laporan', 'Total Jam',
                     'Total Jam (desimal)', 'Unit Ditangani', 'Disetujui', 'Menunggu', 'Perlu Revisi'];
    const dataRows = rows.map(r => [
        r.company, monthLabel(recapMonth), r.members, r.reports,
        formatMinutes(r.minutes), (r.minutes / 60).toFixed(2),
        r.units, r.approved, r.pending, r.revision
    ]);
    const csv = toCSV(headers, dataRows);
    const blob = new Blob(['﻿' + csv], { type: 'text/csv;charset=utf-8;' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = `rekap_perusahaan_${recapMonth}.csv`;
    a.click();
    URL.revokeObjectURL(url);
    showToast(`Export rekap ${monthLabel(recapMonth)}`, 'success');
}

// ---- B3: this week against last week ----
function weekStats(weekStartISO) {
    const days = weekDates(weekStartISO);
    const inWeek = v => days.includes(String(v || '').slice(0, 10));
    const logs = workLogs.filter(w => inWeek(w.date));
    return {
        reports: logs.length,
        minutes: logs.reduce((n, w) => n + workLogMinutes(w), 0),
        people: new Set(logs.map(w => w.memberId).filter(Boolean)).size,
        damages: globalDamages.filter(d => inWeek(d.date)).length,
        resolved: globalDamages.filter(d => d.resolved && inWeek(d.resolvedAt)).length,
        // Seseorang yang shiftnya terisi tetapi izin/sakitnya sudah disetujui
        // TIDAK bertugas. Tanpa pengurangan ini, orang yang sakit tetap
        // terhitung masuk — jadwalnya memang tidak dihapus, dan memang tidak
        // boleh dihapus.
        onDuty: teamShifts.filter(s =>
            inWeek(s.date) && s.shift && s.shift !== 'libur' &&
            !leaveOn(s.memberId, s.date)).length,
        onLeave: teamShifts.filter(s => inWeek(s.date) && leaveOn(s.memberId, s.date)).length,
        stockOut: stockLedger
            .filter(r => r.txnType === 'OUT' && inWeek(r.date))
            .reduce((n, r) => n + (Number(r.qty) || 0), 0)
    };
}

function renderWeekSummary() {
    if (!weekAnchor) weekAnchor = startOfWeekISO(toISODate());
    const thisStart = startOfWeekISO(weekAnchor);
    const lastStart = addDaysISO(thisStart, -7);
    const now = weekStats(thisStart);
    const prev = weekStats(lastStart);

    const label = document.getElementById('weekSummaryLabel');
    if (label) label.textContent = weekRangeLabel(thisStart);
    const prevLabel = document.getElementById('weekSummaryPrev');
    if (prevLabel) prevLabel.textContent = `dibanding ${weekRangeLabel(lastStart)}`;

    // Fewer breakdowns is good, more hours is good — direction is not the same
    // for every row, so each one says which way is better.
    const metrics = [
        { key: 'reports',  label: 'Laporan harian',    fmt: v => String(v),        better: 'up' },
        { key: 'minutes',  label: 'Jam kerja tercatat', fmt: v => formatMinutes(v), better: 'up' },
        { key: 'people',   label: 'Anggota melapor',    fmt: v => String(v),        better: 'up' },
        { key: 'onDuty',   label: 'Hari-orang bertugas', fmt: v => String(v),       better: 'up' },
        { key: 'damages',  label: 'Kerusakan baru',     fmt: v => String(v),        better: 'down' },
        { key: 'resolved', label: 'Kerusakan selesai',  fmt: v => String(v),        better: 'up' },
        { key: 'stockOut', label: 'Barang keluar gudang', fmt: v => String(v),      better: 'flat' }
    ];

    const wrap = document.getElementById('weekSummaryGrid');
    if (!wrap) return;
    wrap.innerHTML = metrics.map(m => {
        const a = now[m.key], b = prev[m.key];
        const diff = a - b;
        let tone = 'flat', arrow = '→';
        if (diff !== 0 && m.better !== 'flat') {
            const good = m.better === 'up' ? diff > 0 : diff < 0;
            tone = good ? 'good' : 'bad';
            arrow = diff > 0 ? '▲' : '▼';
        } else if (diff !== 0) {
            arrow = diff > 0 ? '▲' : '▼';
        }
        const deltaText = diff === 0
            ? 'sama'
            : `${arrow} ${m.key === 'minutes' ? formatMinutes(Math.abs(diff)) : Math.abs(diff)}`;
        return `
        <div class="week-metric">
            <div class="week-metric__label">${escapeHtml(m.label)}</div>
            <div class="week-metric__value">${escapeHtml(m.fmt(a))}</div>
            <div class="week-metric__delta week-metric__delta--${tone}">
                ${escapeHtml(deltaText)}
                <span class="week-metric__prev">minggu lalu ${escapeHtml(m.fmt(b))}</span>
            </div>
        </div>`;
    }).join('');

    // Fleet health has no weekly history to compare against — the app stores
    // current status, not snapshots — so it is shown as a plain "right now"
    // figure rather than a fake trend.
    const good = globalData.filter(u => isGood(u.status)).length;
    const health = document.getElementById('weekHealth');
    if (health) {
        health.innerHTML = globalData.length
            ? `<strong>${pct(good, globalData.length)}%</strong> unit sehat saat ini
               (${good} dari ${globalData.length}) — angka sekarang, bukan perbandingan mingguan`
            : 'Belum ada data unit.';
    }
}

function shiftSummaryWeek(delta) {
    if (!weekAnchor) weekAnchor = startOfWeekISO(toISODate());
    weekAnchor = addDaysISO(startOfWeekISO(weekAnchor), delta * 7);
    // The summary compares against the week BEFORE the one shown, so the
    // listener has to reach a week further back than the label says.
    ensureShiftWindowCovers(addDaysISO(startOfWeekISO(weekAnchor), -7));
    renderWeekSummary();
}

// ---- tabs ----
// ============================================================
// PERIKSA DATA — finds records that are already wrong
// ------------------------------------------------------------
// Validation stops new mistakes; this finds the ones already stored. Each
// check is a small pure function returning findings, so each can be tested on
// its own and a wrong one can be removed without touching the others.
//
// Nothing here writes except the whitespace tidy-up, which is the only fix
// that cannot change what a value means. Everything else points at the record
// and leaves the judgement to a person.
// ============================================================

// Characters that survive .trim() and make two identical-looking strings
// different to a computer. This is not hypothetical: a unit name carrying one
// of these is what made a migration rewrite a record that already matched.
const INVISIBLE_RE = /[ ​-‍﻿]/;

function dc(kind, label, detail, goTo, extra) {
    return { kind, label, detail, goTo, ...(extra || {}) };
}

// Two records claiming the same serial number. Worse than untidy: SN is the
// key a CSV import matches on, so a duplicate makes every later import
// ambiguous about which record it is updating.
function dcDuplicateSerials() {
    const out = [];
    const scan = (list, what, goTo) => {
        const seen = new Map();
        (list || []).forEach(r => {
            const sn = (r.sn || '').trim().toLowerCase();
            if (!sn) return;
            if (seen.has(sn)) {
                out.push(dc('sn-ganda', `${what}: ${r.sn}`,
                    `Dipakai "${seen.get(sn)}" dan "${r.name || r.type || '-'}"`,
                    goTo === 'editUnits' ? unitEditTarget(r) : goTo));
            } else {
                seen.set(sn, r.name || r.type || '-');
            }
        });
    };
    scan(globalData, 'Unit', 'editUnits');
    scan(warehouseDevices, 'Perangkat', 'warehouseDevices');
    return out;
}

function dcInvisibleCharacters() {
    const out = [];
    globalData.forEach(u => {
        ['name', 'sn', 'site'].forEach(f => {
            const v = u[f];
            if (typeof v !== 'string' || !v) return;
            if (INVISIBLE_RE.test(v) || v !== v.trim()) {
                out.push(dc('spasi', `Unit ${u.name || u.sn || u.id}`,
                    `Field "${f}" punya spasi atau karakter tak terlihat`, unitEditTarget(u),
                    { unitId: u.id, field: f, fixable: true }));
            }
        });
    });
    return out;
}

// "PT. GPA" and "PT GPA" split one company into two rows in the recap, and
// nothing on screen shows why the totals look wrong. Grouped by a key that
// ignores case, punctuation and spacing.
function dcSiteVariants() {
    const groups = new Map();
    globalData.forEach(u => {
        const raw = (u.site || '').trim();
        if (!raw) return;
        const key = raw.toLowerCase().replace(/[^a-z0-9]/g, '');
        if (!key) return;
        const g = groups.get(key) || new Map();
        g.set(raw, (g.get(raw) || 0) + 1);
        groups.set(key, g);
    });
    const out = [];
    groups.forEach(variants => {
        if (variants.size < 2) return;
        const parts = [...variants.entries()]
            .sort((a, b) => b[1] - a[1])
            .map(([v, n]) => `"${v}" (${n})`);
        out.push(dc('site-beda-tipis', 'Nama site ditulis beberapa cara',
            parts.join(' · '), 'editUnits'));
    });
    return out;
}

// A member with no company, or a unit with no site, silently drops out of the
// per-company recap — the numbers simply come up short with no row to explain it.
function dcMissingCompany() {
    const out = [];
    (teamMembers || []).forEach(m => {
        if (m.active === false) return;
        if (!(companyOf(m) || '').trim()) {
            out.push(dc('tanpa-perusahaan', `Anggota: ${m.name}`,
                'Belum punya perusahaan — jamnya tidak masuk rekap mana pun', 'team'));
        }
    });
    globalData.forEach(u => {
        if (!(u.site || '').trim()) {
            out.push(dc('tanpa-site', `Unit: ${u.name || u.sn || u.id}`,
                'Belum punya site — tidak terhitung di pembagian PT', unitEditTarget(u),
                { unitId: u.id }));
        }
    });
    return out;
}

// Leftovers from before the hours were validated on save.
function dcWorkLogHours() {
    const out = [];
    (workLogs || []).forEach(w => {
        const s = parseHHMM(w.start), e = parseHHMM(w.end);
        const who = `${memberNameOf(w)} · ${w.date}`;
        if ((s == null) !== (e == null)) {
            out.push(dc('jam-sebelah', `Laporan: ${who}`, 'Hanya satu jam yang terisi', 'teamLog'));
            return;
        }
        if (s == null) return;
        const mins = workLogMinutes(w);
        if (mins === 0) {
            out.push(dc('jam-nol', `Laporan: ${who}`, 'Terhitung 0 jam', 'teamLog'));
        } else if (mins >= WORKLOG_LONG_SHIFT_MIN) {
            out.push(dc('jam-panjang', `Laporan: ${who}`,
                `Terhitung ${formatMinutes(mins)} — kemungkinan jamnya tertukar`, 'teamLog'));
        }
    });
    return out;
}

function dcFutureDates() {
    const today = toISODate();
    const out = [];
    const scan = (list, what, goTo) => (list || []).forEach(r => {
        if (r.date && r.date > today) {
            out.push(dc('tanggal-depan', `${what}: ${r.date}`,
                'Tanggal di masa depan — kemungkinan salah ketik tahun', goTo));
        }
    });
    scan(workLogs, 'Laporan', 'teamLog');
    scan(globalDamages, 'Kerusakan', 'damage');
    scan(stockLedger, 'Stok', 'warehouseStock');
    scan(globalLicenseStock, 'Lisensi', 'licenseStock');
    return out;
}

// A licence that starts and expires on the same day is not a real record. It
// is the signature of the Excel migrations that were removed in v100 — all 52
// rows of their payload had start == end — so this is how to tell whether they
// ever managed to run somewhere before they were deleted.
function dcLicenceDates() {
    const out = [];
    globalData.forEach(u => {
        const pairs = [
            ['GPS', u.gpsLicenseStartDate, u.gpsLicenseEndDate],
            ['Display', u.displayLicenseStartDate, u.displayLicenseEndDate]
        ];
        pairs.forEach(([label, from, to]) => {
            if (!from || !to) return;
            const name = u.name || u.sn || u.id;
            if (from === to) {
                out.push(dc('lisensi-sehari', `Unit: ${name}`,
                    `Lisensi ${label} mulai dan habis di hari yang sama (${from})`, 'editUnits',
                    { unitId: u.id }));
            } else if (to < from) {
                out.push(dc('lisensi-terbalik', `Unit: ${name}`,
                    `Lisensi ${label} habis (${to}) sebelum mulai (${from})`, 'editUnits',
                    { unitId: u.id }));
            }
        });
    });
    return out;
}

function dcOrphanDamage() {
    const ids = new Set(globalData.map(u => u.id));
    const sns = new Set(globalData.map(u => (u.sn || '').toLowerCase()).filter(Boolean));
    return (globalDamages || []).filter(d => {
        if (d.unitId && ids.has(d.unitId)) return false;
        if (d.sn && sns.has(String(d.sn).toLowerCase())) return false;
        return true;
    }).map(d => dc('kerusakan-yatim', `Kerusakan: ${d.unitName || d.sn || '-'}`,
        'Unitnya sudah tidak ada — catatan ini tidak bisa ditelusuri', 'damage'));
}

function dcNegativeStock() {
    return stockSummary().filter(s => s.qty < 0).map(s =>
        dc('stok-minus', `Stok: ${s.name}`,
            `Saldo ${s.qty} — biasanya nama barangnya beda tipis dari yang sudah ada`,
            'warehouseStock'));
}

// Izin dan sakit yang sudah disetujui tetapi tidak ada suratnya. Alpa
// dikecualikan: alpa memang berarti tanpa keterangan, jadi menandainya akan
// menghasilkan temuan yang tidak pernah bisa diselesaikan.
//
// Ini membaca docCount di record induknya, bukan koleksi suratnya — koleksi itu
// tidak pernah dilanggan, dan semua pemeriksaan di sini membaca larik di memori
// secara sinkron.
function dcLeaveWithoutDoc() {
    return (leaveRequests || [])
        .filter(r => r.type !== 'alpa'
            && workLogApproval(r) === 'approved'
            && leaveDocCount(r) === 0)
        .map(r => dc('izin-tanpa-surat',
            `${leaveTypeLabel(r)}: ${memberNameOf(r)}`,
            `Disetujui untuk ${leaveRangeLabel(r)} tetapi tidak ada surat yang dilampirkan.`,
            'leave'));
}

// Sisa dari data sebelum validasi rentang ada. Tanggal selesai lebih awal dari
// tanggal mulai membuat jumlah harinya nol dan penanda di grid shift tidak
// pernah muncul.
function dcLeaveReversed() {
    return (leaveRequests || [])
        .filter(r => r.dateFrom && r.dateTo && r.dateTo < r.dateFrom)
        .map(r => dc('izin-terbalik',
            `${leaveTypeLabel(r)}: ${memberNameOf(r)}`,
            `Tanggal selesai (${r.dateTo}) lebih awal dari tanggal mulai (${r.dateFrom}).`,
            'leave'));
}

// A unit carrying the other group's fields — e.g. an excavator saved from a
// v108 tab that still thought it was a tractor, with gps:'Breakdown'. The
// values are never read for the unit's own group, so clearing them cannot
// change what the unit means; that is why this one is fixable in bulk.
function dcUnitGroupFields() {
    const out = [];
    globalData.forEach(u => {
        const stray = strayGroupFields(u);
        if (stray.length) {
            const g = groupDef(unitGroupOf(u));
            out.push(dc('kelompok-silang', `${g.shortLabel}: ${u.name || u.sn || u.id}`,
                `${g.shortLabel} membawa field kelompok lain: ${stray.map(x => `${COMPONENT_LABELS[x.field] || UNIT_FIELD_LABELS[x.field] || x.field}=${x.value}`).join(', ')}`,
                unitEditTarget(u), { unitId: u.id, fixable: true, stray: stray.map(x => x.field) }));
        }
        // Only a value that is set AND unreadable. 'Alat Berat' is not
        // canonical but reads correctly, so it is not flagged.
        if (!sameStoredValue(u.unitGroup, '') && normalizeGroupKey(u.unitGroup) === null) {
            out.push(dc('kelompok-tak-dikenal', `Unit: ${u.name || u.sn || u.id}`,
                `Kelompok "${u.unitGroup}" tidak dikenal — dibaca sebagai ${UNIT_GROUPS.tractor.label}`, unitEditTarget(u)));
        }
    });
    return out;
}

// Records elsewhere that point across groups: a licence handed to heavy
// equipment, or a damage record naming a component the unit does not have.
function dcGroupReferences() {
    const out = [];
    (globalLicenseStock || []).forEach(r => {
        if (r.txnType !== 'OUT') return;
        const u = liveUnitFor(r);
        if (u && isHeavy(u)) out.push(dc('lisensi-alat-berat', `Lisensi: ${u.name || u.sn}`,
            `${r.licenseType || 'Lisensi'} didistribusikan ke unit ${UNIT_GROUPS.heavy.shortLabel} — lisensi SF/G5 hanya untuk ${UNIT_GROUPS.tractor.shortLabel}`,
            'licenseStock'));
    });
    (globalDamages || []).forEach(r => {
        const u = liveUnitFor(r);
        if (u && r.component && componentGroupConflict(r.component, u)) out.push(dc('kerusakan-komponen-silang',
            `Kerusakan: ${u.name || u.sn}`,
            `Komponen "${r.component}" bukan milik ${groupDef(unitGroupOf(u)).shortLabel}`, 'damage'));
    });
    return out;
}

const DATA_CHECKS = [
    { key: 'sn',       label: 'Nomor seri duplikat',        run: dcDuplicateSerials },
    { key: 'spasi',    label: 'Spasi / karakter tak terlihat', run: dcInvisibleCharacters },
    { key: 'site',     label: 'Nama site ditulis beda-beda', run: dcSiteVariants },
    { key: 'kosong',   label: 'Perusahaan / site belum diisi', run: dcMissingCompany },
    { key: 'jam',      label: 'Jam laporan tidak wajar',    run: dcWorkLogHours },
    { key: 'tanggal',  label: 'Tanggal di masa depan',      run: dcFutureDates },
    { key: 'lisensi',  label: 'Tanggal lisensi janggal',    run: dcLicenceDates },
    { key: 'yatim',    label: 'Kerusakan tanpa unit',       run: dcOrphanDamage },
    { key: 'stok',     label: 'Saldo stok minus',           run: dcNegativeStock },
    { key: 'surat',    label: 'Izin / sakit tanpa surat',    run: dcLeaveWithoutDoc },
    { key: 'izin-tgl', label: 'Rentang izin terbalik',       run: dcLeaveReversed },
    { key: 'kelompok', label: 'Field kelompok unit tidak cocok', run: dcUnitGroupFields },
    { key: 'kelompok-ref', label: 'Lisensi / kerusakan salah kelompok', run: dcGroupReferences }
];

function runDataChecks() {
    return DATA_CHECKS.map(c => {
        let items = [];
        // One broken check must not take the whole page down with it.
        try { items = c.run() || []; }
        catch (err) { console.error(`[periksa] ${c.key} gagal:`, err); }
        return { ...c, items };
    });
}

function renderDataCheck() {
    const host = document.getElementById('dataCheckBody');
    if (!host) return;
    const groups = runDataChecks();
    const total = groups.reduce((n, g) => n + g.items.length, 0);
    // Two fixers, each with its own button: invisible characters, and fields
    // that belong to the other unit group.
    const fixable = groups.reduce((n, g) => n + g.items.filter(i => i.fixable && i.kind !== 'kelompok-silang').length, 0);
    const strayFixable = groups.reduce((n, g) => n + g.items.filter(i => i.kind === 'kelompok-silang').length, 0);
    const groupFixBtn = document.getElementById('dataCheckGroupFixBtn');
    if (groupFixBtn) {
        groupFixBtn.style.display = strayFixable ? '' : 'none';
        groupFixBtn.textContent = `Bersihkan field kelompok lain di ${strayFixable} unit`;
    }

    const summary = document.getElementById('dataCheckSummary');
    if (summary) {
        summary.textContent = total === 0
            ? 'Tidak ada yang mencurigakan — data Anda bersih.'
            : `${total} hal perlu dilihat, di ${groups.filter(g => g.items.length).length} kelompok.`;
    }
    const fixBtn = document.getElementById('dataCheckFixBtn');
    if (fixBtn) {
        fixBtn.style.display = fixable ? '' : 'none';
        fixBtn.textContent = `Rapikan ${fixable} spasi tersembunyi`;
    }

    if (total === 0) { host.innerHTML = ''; return; }

    host.innerHTML = groups.filter(g => g.items.length).map(g => `
        <div class="datacheck-group">
            <h4>${escapeHtml(g.label)} <span class="datacheck-count">${g.items.length}</span></h4>
            <table class="data-table table--cardable">
                <thead><tr><th>Apa</th><th>Keterangan</th><th></th></tr></thead>
                <tbody>${g.items.map(i => `<tr>
                    <td data-label="Apa">${escapeHtml(i.label)}</td>
                    <td data-label="Keterangan">${escapeHtml(i.detail)}</td>
                    <td data-label="" style="white-space:nowrap"><button type="button" class="btn btn-secondary btn-sm"
                        onclick="goDecision('${escapeHtml(i.goTo)}')">Buka</button></td>
                </tr>`).join('')}</tbody>
            </table>
        </div>`).join('');
}

// The one automatic fix. Trimming whitespace and stripping zero-width
// characters cannot change what a value means, which is why it is safe to do
// in bulk; every other finding needs a person to decide. Writes go through
// updateUnit, so they are gated by canWriteUnits() and land in the history
// like any other edit.
function fixInvisibleCharacters() {
    if (!requireEdit('editUnits')) return;
    const found = dcInvisibleCharacters().filter(i => i.fixable);
    if (!found.length) { showToast('Tidak ada yang perlu dirapikan', 'info'); return; }
    if (!confirm(`Rapikan ${found.length} nilai yang punya spasi atau karakter tak terlihat?\n\n`
        + 'Tulisannya tidak berubah — hanya karakter tersembunyinya yang dibuang.')) return;

    const byUnit = new Map();
    found.forEach(i => {
        const u = globalData.find(x => x.id === i.unitId);
        if (!u) return;
        const cleaned = String(u[i.field]).replace(new RegExp(INVISIBLE_RE.source, 'g'), '').trim();
        if (cleaned === u[i.field]) return;
        const fields = byUnit.get(i.unitId) || {};
        fields[i.field] = cleaned;
        byUnit.set(i.unitId, fields);
    });

    let n = 0;
    byUnit.forEach((fields, id) => { if (updateUnit(id, fields)) n++; });
    showToast(n ? `${n} unit dirapikan` : 'Tidak ada yang berubah', n ? 'success' : 'info');
    renderDataCheck();
}

// The second safe fix: clearing values a unit's own group never reads. Goes
// through updateUnit with clearStray, the one case the write firewall lets a
// field of the other group through — and only as ''.
function fixStrayGroupFields() {
    if (!requireEdit('editUnits')) return;
    const found = dcUnitGroupFields().filter(i => i.kind === 'kelompok-silang');
    if (!found.length) { showToast('Tidak ada yang perlu dibersihkan', 'info'); return; }
    if (!confirm(`Kosongkan field milik kelompok lain di ${found.length} unit?\n\n`
        + 'Nilai itu tidak pernah dibaca untuk kelompok unitnya sendiri, jadi arti unitnya tidak berubah.')) return;
    let n = 0;
    found.forEach(i => {
        const fields = {};
        i.stray.forEach(f => { fields[f] = ''; });
        if (updateUnit(i.unitId, fields, { clearStray: true })) n++;
    });
    showToast(n ? `${n} unit dibersihkan` : 'Tidak ada yang berubah', n ? 'success' : 'info');
    renderDataCheck();
}

function switchLeaderTab(tab) {
    leaderTab = ['company', 'week', 'check'].includes(tab) ? tab : 'inbox';
    renderLeaderView();
}

function renderLeaderView() {
    document.querySelectorAll('.leader-tab').forEach(btn => {
        // Never trust another page to have left these visible.
        btn.style.display = '';
        const on = btn.dataset.tab === leaderTab;
        btn.classList.toggle('active', on);
        btn.setAttribute('aria-selected', on ? 'true' : 'false');
    });
    const panels = { inbox: 'leaderInboxPanel', company: 'leaderCompanyPanel',
                     week: 'leaderWeekPanel', check: 'leaderCheckPanel' };
    Object.entries(panels).forEach(([key, id]) => {
        const el = document.getElementById(id);
        if (el) el.style.display = (key === leaderTab) ? '' : 'none';
    });
    if (leaderTab === 'inbox') renderDecisionInbox();
    else if (leaderTab === 'company') renderCompanyRecap();
    else if (leaderTab === 'check') renderDataCheck();
    else renderWeekSummary();
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
