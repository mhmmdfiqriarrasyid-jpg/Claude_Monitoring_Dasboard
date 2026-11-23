// ============================================================
// BAST — Berita Acara Serah Terima (2.46.0)
// ------------------------------------------------------------
// Handover letters for company assets (HT, RIG Mobile, …): imported from the
// Excel format (templates/Format_Import_BAST.xlsx, one row = one letter) or
// made from an allocated Warehouse device, numbered automatically and printed
// to PDF on the company layout.
//
// Never twice: a letter's number, its unit's S/N, and (when given) its Radio ID
// and battery S/N each have a create-once key in Firestore (firebase-init.js
// createBast, firestore.rules bastKeys). The import checks the same here first
// so a row that would clash is listed with its reason instead of failing.
//
// Linked to Warehouse: a letter's S/N is looked up among the devices. Found,
// the device becomes Allocated to the letter's recipient (PIHAK KEDUA); not
// found, it is added as an allocated device. Returning the letter brings the
// device back to the warehouse; a device taken off its allocation in Warehouse
// returns its letter.
//
// Who: the owner and Warehouse editors (canUseBast).
// ============================================================

let bastList = [];
let _bastQ = '', _bastStatus = 'active';
let _bastImport = null;          // { rows: [{ row, rec, errors, device }] } while the import dialog is open
let _bastLastIds = [];           // letters made by the last import / form, for "Print"
const BAST_STATUS = { active: 'Active', returned: 'Returned', void: 'Cancelled' };
const BAST_HARI = ['Minggu', 'Senin', 'Selasa', 'Rabu', 'Kamis', "Jum'at", 'Sabtu'];
const BAST_BULAN = ['Januari', 'Februari', 'Maret', 'April', 'Mei', 'Juni', 'Juli', 'Agustus', 'September', 'Oktober', 'November', 'Desember'];
const BAST_CONDITIONS = ['Baru/Baik', 'Baik', 'Bekas/Baik', 'Rusak Ringan', 'Rusak Berat'];
const BAST_UNITS = ['Set', 'Unit', 'Pcs'];
const BAST_TEMPLATE_URL = 'templates/Format_Import_BAST.xlsx';
// Excel column → field. Headers are matched without the " *" mark, case and spaces ignored.
const BAST_COLUMNS = [
    ['No Surat', 'no'], ['Tanggal', 'date'], ['Kode PT', 'companyCode'], ['Nama PT / Lokasi', 'place'], ['Kode Barang', 'itemCode'],
    ['Pihak Pertama - Nama', 'p1Name'], ['Pihak Pertama - Jabatan', 'p1Title'], ['Pihak Pertama - Mewakili', 'p1Rep'],
    ['Pihak Kedua - Nama', 'p2Name'], ['Pihak Kedua - Jabatan', 'p2Title'], ['Pihak Kedua - Departemen', 'p2Dept'],
    ['Nama Barang', 'itemName'], ['Merk / Type', 'brandType'], ['Radio ID DMR', 'radioId'], ['S/N Unit', 'sn'], ['S/N Baterai', 'batterySn'],
    ['Jumlah', 'qty'], ['Satuan', 'unit'], ['Kondisi', 'condition'], ['Mengetahui 1 - Nama', 'witness1'], ['Mengetahui 2 - Nama', 'witness2'],
    ['Kategori Device (Warehouse)', 'deviceType'], ['Judul Dokumen', 'title'], ['Catatan', 'note']
];
const BAST_REQUIRED = ['date', 'companyCode', 'place', 'itemCode', 'p1Name', 'p1Title', 'p1Rep', 'p2Name', 'p2Title', 'p2Dept', 'itemName', 'brandType', 'sn', 'qty', 'unit', 'condition'];
const BAST_LABEL = Object.fromEntries(BAST_COLUMNS.map(([h, f]) => [f, h]));
const BAST_MAX = { no: 80, sn: 60, radioId: 40, batterySn: 60, note: 300, title: 120 };

function canUseBast() { return typeof hasAccess === 'function' && hasAccess('warehouse', 'edit'); }
function generateBastId() { return 'bast_' + Date.now() + '_' + Math.random().toString(36).slice(2, 7); }
const _bastNorm = v => String(v == null ? '' : v).trim().toUpperCase().replace(/[^A-Z0-9-]/g, '_');
const _bastTxt = (v, max) => String(v == null ? '' : v).replace(/\s+/g, ' ').trim().slice(0, max || 160);

function applyBastSnapshot(list) {
    bastList = (Array.isArray(list) ? list : []).filter(b => b && typeof b === 'object' && b.id)
        .sort((a, b) => String(b.date || '').localeCompare(String(a.date || '')) || (b.createdAt || 0) - (a.createdAt || 0));
    if (typeof currentView !== 'undefined' && currentView === 'bast') renderBastView();
}

// ---- Dates and numbers ----
// An Excel date (serial number or Date) or text YYYY-MM-DD / DD/MM/YYYY → YYYY-MM-DD ('' when not a date).
function bastDate(v) {
    const p = n => String(n).padStart(2, '0');
    if (v instanceof Date && !isNaN(v)) return `${v.getFullYear()}-${p(v.getMonth() + 1)}-${p(v.getDate())}`;
    if (typeof v === 'number' && isFinite(v) && v > 20000 && v < 80000) {
        const d = new Date(Math.round((v - 25569) * 86400000));
        return `${d.getUTCFullYear()}-${p(d.getUTCMonth() + 1)}-${p(d.getUTCDate())}`;
    }
    const s = String(v == null ? '' : v).trim();
    let m = /^(\d{4})-(\d{1,2})-(\d{1,2})$/.exec(s);
    if (!m) { const k = /^(\d{1,2})[/.-](\d{1,2})[/.-](\d{4})$/.exec(s); if (k) m = [s, k[3], k[2], k[1]]; }
    if (!m) return '';
    const iso = `${m[1]}-${p(m[2])}-${p(m[3])}`;
    const d = new Date(`${iso}T12:00:00`);
    return !isNaN(d) && d.getDate() === Number(m[3]) ? iso : '';
}
// "Selasa tanggal 30 Juni 2026"
function bastDateText(iso) {
    const d = new Date(`${iso}T12:00:00`);
    if (isNaN(d)) return iso || '';
    return `${BAST_HARI[d.getDay()]} tanggal ${d.getDate()} ${BAST_BULAN[d.getMonth()]} ${d.getFullYear()}`;
}
// The number's prefix and counter: <PT>/BAST-<KODE>/<MM>/<YYYY>/<seq>, counted per PT + item code + year.
function bastNumbering(rec) {
    const [y, m] = String(rec.date || '').split('-');
    return { prefix: `${rec.companyCode}/BAST-${rec.itemCode}/${m}/${y}`, seqKey: `${rec.companyCode}_${rec.itemCode}_${y}` };
}

// ---- Rows → letters ----
function bastHeaderMap(header) {
    const key = h => String(h == null ? '' : h).replace(/\*/g, '').replace(/\s+/g, '').toLowerCase();
    const want = new Map(BAST_COLUMNS.map(([h, f]) => [key(h), f]));
    const map = {};
    (header || []).forEach((h, i) => { const f = want.get(key(h)); if (f && map[f] === undefined) map[f] = i; });
    return map;
}
// A row (array, by header map) → a letter record (not yet numbered) and its own errors.
function bastRecFromRow(cells, map) {
    const get = f => (map[f] === undefined ? '' : cells[map[f]]);
    const rec = {};
    BAST_COLUMNS.forEach(([, f]) => {
        if (f === 'date') rec.date = bastDate(get('date'));
        else if (f === 'qty') { const n = Number(String(get('qty')).replace(',', '.')); rec.qty = isFinite(n) && n > 0 ? Math.round(n * 100) / 100 : 0; }
        else rec[f] = _bastTxt(get(f), BAST_MAX[f]);
    });
    rec.companyCode = rec.companyCode.toUpperCase();
    rec.itemCode = rec.itemCode.toUpperCase();
    return { rec, errors: bastCheck(rec, get('date')) };
}
function bastCheck(rec, rawDate) {
    const errors = [];
    BAST_REQUIRED.forEach(f => {
        if (f === 'date') { if (!rec.date) errors.push(rawDate === '' || rawDate == null ? 'Tanggal is empty' : 'Tanggal is not a date (YYYY-MM-DD)'); return; }
        if (f === 'qty') { if (!(rec.qty > 0)) errors.push('Jumlah must be a number above 0'); return; }
        if (!rec[f]) errors.push(`${BAST_LABEL[f]} is empty`);
    });
    if (rec.companyCode && !/^[A-Z0-9]{1,10}$/.test(rec.companyCode)) errors.push('Kode PT: letters and digits only (e.g. GPA)');
    if (rec.itemCode && !/^[A-Z0-9]{1,10}$/.test(rec.itemCode)) errors.push('Kode Barang: letters and digits only (e.g. HT)');
    if (rec.date && rec.date > (typeof toISODate === 'function' ? toISODate() : rec.date)) errors.push('Tanggal is in the future');
    if (rec.unit && !BAST_UNITS.includes(rec.unit)) errors.push(`Satuan must be one of: ${BAST_UNITS.join(', ')}`);
    if (rec.condition && !BAST_CONDITIONS.includes(rec.condition)) errors.push(`Kondisi must be one of: ${BAST_CONDITIONS.join(', ')}`);
    return errors;
}
// Clashes of each row with the letters already made and with the rows above it.
function bastClashes(rows) {
    const taken = { no: new Map(), sn: new Map(), rid: new Map(), bat: new Map() };
    bastList.forEach(b => {
        if (b.no) taken.no.set(_bastNorm(b.no), `letter ${b.no}`);
        if (b.status === 'active') {
            if (b.sn) taken.sn.set(_bastNorm(b.sn), `active letter ${b.no}`);
            if (b.radioId) taken.rid.set(_bastNorm(b.radioId), `active letter ${b.no}`);
            if (b.batterySn) taken.bat.set(_bastNorm(b.batterySn), `active letter ${b.no}`);
        }
    });
    const seen = { no: new Map(), sn: new Map(), rid: new Map(), bat: new Map() };
    const what = { no: 'No Surat', sn: 'S/N Unit', rid: 'Radio ID DMR', bat: 'S/N Baterai' };
    const field = { no: 'no', sn: 'sn', rid: 'radioId', bat: 'batterySn' };
    rows.forEach(r => {
        Object.keys(field).forEach(k => {
            const v = r.rec[field[k]];
            if (!v) return;
            const n = _bastNorm(v);
            if (taken[k].has(n)) r.errors.push(`${what[k]} ${v} is already in ${taken[k].get(n)}`);
            else if (seen[k].has(n)) r.errors.push(`${what[k]} ${v} is also on row ${seen[k].get(n)} of this file`);
            else seen[k].set(n, r.row);
        });
    });
    return rows;
}
// The Warehouse device with this S/N (case and spaces ignored).
function bastDeviceFor(sn) {
    const n = _bastNorm(sn);
    return (typeof warehouseDevices !== 'undefined' ? warehouseDevices : []).find(d => d && d.sn && _bastNorm(d.sn) === n) || null;
}
// The device fields a letter sets: allocated to PIHAK KEDUA from the letter's date. A
// device allocated to someone else before keeps that allocation in its history.
function bastDevicePatch(cur, b) {
    const d = cur || {};
    const hist = Array.isArray(d.allocationHistory) ? d.allocationHistory.slice() : [];
    if (d.status === 'allocated' && (d.recipientName || d.division) && d.recipientName !== b.p2Name)
        hist.push({ division: d.division || '', recipientName: d.recipientName || '', recipientTitle: d.recipientTitle || '', allocatedAt: d.allocatedAt || '', returnedAt: b.date, bastNo: d.bastNo || '' });
    const patch = {
        status: 'allocated', division: b.p2Dept, recipientName: b.p2Name, recipientTitle: b.p2Title, allocatedAt: b.date,
        location: '', unitId: '', unitName: '', posX: '', posY: '',
        allocationHistory: hist.slice(-20), bastId: b.id, bastNo: b.no, updatedAt: Date.now()
    };
    if (b.radioId) patch.radioId = b.radioId;
    if (b.batterySn && !d.batterySn) { patch.batterySn = b.batterySn; patch.hasBattery = true; }
    if (!cur) Object.assign(patch, {
        id: b.deviceId, sn: b.sn, type: b.deviceType || (b.itemCode === 'HT' ? 'HT' : b.itemCode === 'RIG' ? 'RIG Mobile' : b.itemName),
        brand: '', model: b.brandType, photoIds: [], note: `From BAST ${b.no}`, siteName: '', hasBattery: !!b.batterySn, batterySn: b.batterySn || '',
        createdAt: Date.now()
    });
    return patch;
}
// The device back in the warehouse when its letter is returned.
function bastDeviceReturn(cur, b) {
    if (!cur || cur.bastId !== b.id) return null;
    if (b.status === 'void') return { bastId: '', bastNo: '', updatedAt: Date.now() };
    if (cur.status !== 'allocated') return { bastId: '', bastNo: '', updatedAt: Date.now() };
    const hist = (Array.isArray(cur.allocationHistory) ? cur.allocationHistory.slice() : []).concat([{ division: cur.division || '', recipientName: cur.recipientName || '',
        recipientTitle: cur.recipientTitle || '', allocatedAt: cur.allocatedAt || '', returnedAt: b.endedAt || toISODate(), bastNo: b.no || '' }]).slice(-20);
    return { status: 'warehouse', division: '', recipientName: '', recipientTitle: '', allocatedAt: '', allocationHistory: hist, bastId: '', bastNo: '', updatedAt: Date.now() };
}

// ---- Saving ----
function _bastMe() { return (typeof currentUserDoc !== 'undefined' && currentUserDoc && (currentUserDoc.displayName || currentUserDoc.email)) || ''; }
// One letter: numbered (when no number is given), keys written, device allocated — all at once.
async function bastCreate(rec) {
    const dev = bastDeviceFor(rec.sn);
    const full = Object.assign({}, rec, {
        id: rec.id || generateBastId(), status: 'active', deviceId: dev ? dev.id : (typeof generateDeviceId === 'function' ? generateDeviceId() : 'dev_' + Date.now()),
        createdAt: Date.now(), createdBy: _bastMe(), createdByUid: typeof myUid === 'function' ? myUid() : '', updatedAt: Date.now()
    });
    const out = await window.cloud.createBast(full, Object.assign(bastNumbering(full), { device: { id: full.deviceId, make: bastDevicePatch } }));
    if (typeof logEvent === 'function') logEvent({ action: 'add', unitId: '', unitName: `[BAST] ${out.no}`, field: `${out.itemName} · ${out.sn}`, after: `${out.p2Name} (${out.p2Dept})` });
    return out;
}
function bastErrorText(err) {
    if (err && err.code === 'bast/dup') {
        const what = { no: 'No Surat', sn: 'S/N Unit', rid: 'Radio ID DMR', bat: 'S/N Baterai' }[err.field] || 'A value';
        const other = bastList.find(b => b.id === err.other);
        return `${what} is already in letter ${other ? other.no : '(another letter)'}`;
    }
    if (err && err.code === 'permission-denied') return 'Refused — BAST needs the new firestore.rules published, and Warehouse edit access';
    if (err && (err.code === 'unavailable' || err.code === 'failed-precondition' || /offline/i.test(err.message || ''))) return 'No connection — a BAST is numbered by the server; try again online';
    return (err && err.message) || 'Failed to save';
}

// ---- Page ----
function renderBastView() {
    const view = document.getElementById('viewBast');
    if (!view) return;
    const q = (_bastQ || '').toLowerCase();
    const rows = bastList.filter(b => (_bastStatus === 'all' || b.status === _bastStatus)
        && (!q || [b.no, b.p2Name, b.p2Dept, b.itemName, b.brandType, b.sn, b.radioId, b.batterySn, b.place].join(' ').toLowerCase().includes(q)));
    const count = s => bastList.filter(b => s === 'all' || b.status === s).length;
    document.querySelectorAll('#bastStatusTabs [data-status]').forEach(btn => {
        btn.classList.toggle('active', btn.dataset.status === _bastStatus);
        const c = btn.querySelector('.team-tab__count'); if (c) c.textContent = count(btn.dataset.status);
    });
    const body = document.getElementById('bastBody');
    const esc = v => escapeHtml(v == null ? '' : String(v));
    body.innerHTML = rows.length ? rows.map(b => `<tr>
            <td data-label="Select"><input type="checkbox" class="bast-pick" value="${esc(b.id)}" aria-label="Select ${esc(b.no)}"></td>
            <td data-label="No Surat"><b>${esc(b.no)}</b></td>
            <td data-label="Tanggal">${esc(b.date)}</td>
            <td data-label="Pihak Kedua">${esc(b.p2Name)}<div class="adm-sub">${esc([b.p2Title, b.p2Dept].filter(Boolean).join(' · '))}</div></td>
            <td data-label="Barang">${esc(b.itemName)}<div class="adm-sub">${esc(b.brandType)}</div></td>
            <td data-label="S/N">${esc(b.sn)}${b.radioId ? `<div class="adm-sub">ID ${esc(b.radioId)}</div>` : ''}</td>
            <td data-label="Status"><span class="bast-status bast-status--${esc(b.status)}">${esc(BAST_STATUS[b.status] || b.status)}</span>${b.endedAt ? `<div class="adm-sub">${esc(b.endedAt)}</div>` : ''}</td>
            <td data-label="Actions" class="bast-actions">
                <button type="button" class="btn btn-secondary btn-sm" onclick="printBast(['${esc(b.id)}'])"><i class="fas fa-file-pdf" aria-hidden="true"></i> PDF</button>
                ${b.status === 'active' ? `<button type="button" class="btn btn-secondary btn-sm" onclick="endBastPrompt('${esc(b.id)}', 'returned')">Returned</button>
                <button type="button" class="btn btn-secondary btn-sm" onclick="endBastPrompt('${esc(b.id)}', 'void')">Cancel</button>` : ''}
            </td></tr>`).join('')
        : `<tr><td colspan="8" class="adm-muted" style="text-align:center;padding:24px">${bastList.length ? 'No letter matches.' : 'No BAST yet — import the Excel format or make one from an allocated device.'}</td></tr>`;
    const cnt = document.getElementById('bastCount');
    if (cnt) cnt.textContent = `${rows.length} letter(s)`;
    const ac = document.getElementById('bastAllocCount');
    if (ac) ac.textContent = bastAllocatedWithout().length;
}
function setBastStatus(s) { _bastStatus = s; renderBastView(); }
function bastPicked() { return [...document.querySelectorAll('#bastBody .bast-pick:checked')].map(i => i.value); }
function bastToggleAll(on) { document.querySelectorAll('#bastBody .bast-pick').forEach(i => { i.checked = on; }); }
function printBastPicked() {
    const ids = bastPicked();
    if (!ids.length) { showToast('Tick the letters to print first', 'warning'); return; }
    printBast(ids);
}

// ---- Import ----
function openBastImport() {
    if (!canUseBast()) { showToast('BAST needs Warehouse edit access', 'warning'); return; }
    const inp = document.getElementById('bastImportInput');
    if (inp) { inp.value = ''; inp.click(); }
}
async function importBastFile(event) {
    const file = event && event.target && event.target.files && event.target.files[0];
    if (!file) return;
    let rows;
    try {
        const XLSX = await loadSheetJs();
        const wb = XLSX.read(await file.arrayBuffer(), { type: 'array', cellDates: true });
        const name = wb.SheetNames.find(n => /^bast$/i.test(n.trim())) || wb.SheetNames.find(n => {
            const r = XLSX.utils.sheet_to_json(wb.Sheets[n], { header: 1, raw: true, defval: '' })[0] || [];
            return bastHeaderMap(r).sn !== undefined;
        });
        if (!name) { showToast('No "BAST" sheet with an "S/N Unit" column in this file — use the BAST format', 'error'); return; }
        rows = XLSX.utils.sheet_to_json(wb.Sheets[name], { header: 1, raw: true, defval: '' });
    } catch (err) {
        console.error('[bast] import read failed:', err);
        showToast('This file could not be read', 'error');
        return;
    }
    bastPreview(rows);
}
// rows: the sheet as arrays (first row = headers).
function bastPreview(rows) {
    const map = bastHeaderMap(rows[0] || []);
    const missing = ['sn', 'date', 'p2Name'].filter(f => map[f] === undefined);
    if (missing.length) { showToast(`Columns missing: ${missing.map(f => BAST_LABEL[f]).join(', ')} — use the BAST format`, 'error'); return; }
    const list = [];
    rows.slice(1).forEach((cells, i) => {
        if (!Array.isArray(cells) || !cells.some(c => String(c == null ? '' : c).trim() !== '')) return;
        const { rec, errors } = bastRecFromRow(cells, map);
        list.push({ row: i + 2, rec, errors });
    });
    if (!list.length) { showToast('The BAST sheet has no rows', 'warning'); return; }
    bastClashes(list);
    list.forEach(r => { r.device = bastDeviceFor(r.rec.sn); });
    _bastImport = { rows: list };
    renderBastImport();
    document.getElementById('bastImportModal').classList.add('open');
}
function renderBastImport() {
    const list = (_bastImport && _bastImport.rows) || [];
    const ok = list.filter(r => !r.errors.length && !r.done), bad = list.filter(r => r.errors.length);
    const esc = v => escapeHtml(v == null ? '' : String(v));
    document.getElementById('bastImportSummary').textContent =
        `${list.length} row(s): ${ok.length} ready${bad.length ? `, ${bad.length} refused (listed with the reason)` : ''}${list.some(r => r.done) ? `, ${list.filter(r => r.done).length} made` : ''}.`;
    document.getElementById('bastImportBody').innerHTML = list.map(r => `<tr class="${r.errors.length ? 'bast-row--bad' : r.done ? 'bast-row--done' : ''}">
        <td data-label="Row">${r.row}</td>
        <td data-label="No Surat">${r.done ? `<b>${esc(r.done)}</b>` : esc(r.rec.no) || '<span class="adm-muted">automatic</span>'}</td>
        <td data-label="Pihak Kedua">${esc(r.rec.p2Name)}</td>
        <td data-label="Barang">${esc(r.rec.itemName)} · ${esc(r.rec.sn)}</td>
        <td data-label="Warehouse">${r.device ? `${esc(r.device.type || 'Device')} — ${esc(DEVICE_STATUS_LABEL[r.device.status] || r.device.status)} → Allocated` : 'New device (Allocated)'}</td>
        <td data-label="Check">${r.errors.length ? `<span class="bast-err">${r.errors.map(esc).join('<br>')}</span>` : r.done ? 'Made' : 'Ready'}</td></tr>`).join('');
    const go = document.getElementById('bastImportGo');
    go.disabled = !ok.length;
    go.textContent = ok.length ? `Make ${ok.length} BAST` : 'Nothing to make';
    const pr = document.getElementById('bastImportPrint');
    if (pr) pr.style.display = list.some(r => r.done) ? '' : 'none';
}
function closeBastImport() { document.getElementById('bastImportModal').classList.remove('open'); _bastImport = null; }
async function runBastImport() {
    if (!_bastImport || !canUseBast()) return;
    const go = document.getElementById('bastImportGo');
    go.disabled = true;
    const made = [];
    for (const r of _bastImport.rows) {
        if (r.errors.length || r.done) continue;
        go.textContent = `Making… ${made.length + 1}`;
        try {
            const out = await bastCreate(r.rec);
            r.done = out.no; r.id = out.id; made.push(out.id);
        } catch (err) {
            console.error('[bast] import row failed:', r.row, err);
            r.errors.push(bastErrorText(err));
        }
        renderBastImport();
    }
    _bastLastIds = made;
    renderBastImport();
    showToast(made.length ? `${made.length} BAST made` : 'No BAST made', made.length ? 'success' : 'warning');
}
function printBastImported() { if (_bastLastIds.length) printBast(_bastLastIds); }

// ---- Devices allocated before BAST existed (2.46.2) ----
// The allocated Warehouse devices that have no active letter, as rows of the import format,
// filled from the device (recipient, date, S/N, battery, Radio ID, item). The person adds
// what Warehouse does not know (PIHAK PERTAMA, PT, witnesses), then imports the file.
function bastAllocatedWithout() {
    return (typeof warehouseDevices !== 'undefined' ? warehouseDevices : [])
        .filter(d => d && d.status === 'allocated' && d.sn && !bastOfDevice(d))
        .sort((a, b) => String(a.allocatedAt || '').localeCompare(String(b.allocatedAt || '')) || String(a.sn).localeCompare(String(b.sn)));
}
function bastRowOfDevice(d, last) {
    const code = /\bHT\b|handy/i.test(d.type || '') ? 'HT' : /rig/i.test(d.type || '') ? 'RIG' : '';
    const v = {
        no: '', date: d.allocatedAt || '', companyCode: last.companyCode || '', place: last.place || '', itemCode: code,
        p1Name: last.p1Name || '', p1Title: last.p1Title || '', p1Rep: last.p1Rep || '',
        p2Name: d.recipientName || '', p2Title: d.recipientTitle || '', p2Dept: d.division || '',
        itemName: d.type || '', brandType: [d.brand, d.model].filter(Boolean).join(' '), radioId: d.radioId || '', sn: d.sn || '', batterySn: d.batterySn || '',
        qty: 1, unit: 'Set', condition: 'Baik', witness1: last.witness1 || '', witness2: last.witness2 || '', deviceType: d.type || '', title: '', note: ''
    };
    return BAST_COLUMNS.map(([, f]) => v[f]);
}
async function exportBastAllocated() {
    if (!canUseBast()) return;
    const list = bastAllocatedWithout();
    if (!list.length) { showToast('Every allocated device already has an active BAST', 'info'); return; }
    let last = {};
    try { last = JSON.parse(localStorage.getItem(BAST_PREF_KEY) || '{}') || {}; } catch (_) { last = {}; }
    try {
        const XLSX = await loadSheetJs();
        const head = BAST_COLUMNS.map(([h, f]) => h + (BAST_REQUIRED.includes(f) ? ' *' : ''));
        const ws = XLSX.utils.aoa_to_sheet([head].concat(list.map(d => bastRowOfDevice(d, last))));
        ws['!cols'] = head.map(h => ({ wch: Math.max(12, Math.min(34, h.length + 4)) }));
        const info = XLSX.utils.aoa_to_sheet([
            ['Device Allocated tanpa BAST — dari Warehouse'],
            [''],
            ['1. Lengkapi kolom yang masih kosong di sheet BAST (terutama Kode PT, Nama PT / Lokasi, Kode Barang, Pihak Pertama, Mengetahui).'],
            ['2. Periksa data yang terisi dari Warehouse (penerima, jabatan, departemen, tanggal alokasi, S/N).'],
            ['3. Import file ini di menu BAST. Nomor surat dibuat otomatis; device tetap Allocated ke penerima yang sama dan tersambung ke BAST-nya.'],
            ['Baris yang tidak ingin dibuatkan BAST: hapus barisnya.']]);
        info['!cols'] = [{ wch: 120 }];
        const wb = XLSX.utils.book_new();
        XLSX.utils.book_append_sheet(wb, info, 'Petunjuk');
        XLSX.utils.book_append_sheet(wb, ws, 'BAST');
        XLSX.writeFile(wb, `BAST_Allocated_tanpa_BAST_${toISODate()}.xlsx`);
        showToast(`${list.length} allocated device(s) without BAST exported — complete the file, then import it`, 'success');
    } catch (err) {
        console.error('[bast] export failed:', err);
        showToast('The Excel file could not be made', 'error');
    }
}

// ---- One letter by hand (also from an allocated device) ----
const BAST_PREF_KEY = 'otBastLastParties';
function showBastForm(fromDeviceId) {
    if (!canUseBast()) { showToast('BAST needs Warehouse edit access', 'warning'); return; }
    const f = document.getElementById('bastForm');
    f.reset();
    let last = {};
    try { last = JSON.parse(localStorage.getItem(BAST_PREF_KEY) || '{}') || {}; } catch (_) { last = {}; }
    const set = (id, v) => { const el = document.getElementById(id); if (el) el.value = v == null ? '' : v; };
    ['companyCode', 'place', 'p1Name', 'p1Title', 'p1Rep', 'witness1', 'witness2'].forEach(k => set('bast_' + k, last[k] || ''));
    set('bast_date', toISODate()); set('bast_qty', 1); set('bast_unit', 'Set'); set('bast_condition', 'Baik');
    const d = fromDeviceId ? warehouseDevices.find(x => x.id === fromDeviceId) : null;
    if (d) {
        set('bast_sn', d.sn); set('bast_batterySn', d.batterySn); set('bast_radioId', d.radioId);
        set('bast_itemName', d.type); set('bast_brandType', [d.brand, d.model].filter(Boolean).join(' '));
        set('bast_itemCode', /\bHT\b|handy/i.test(d.type || '') ? 'HT' : /rig/i.test(d.type || '') ? 'RIG' : '');
        set('bast_deviceType', d.type);
        if (d.status === 'allocated') { set('bast_p2Name', d.recipientName); set('bast_p2Title', d.recipientTitle); set('bast_p2Dept', d.division); if (d.allocatedAt) set('bast_date', d.allocatedAt); }
    }
    document.getElementById('bastFormModal').classList.add('open');
}
function closeBastForm() { document.getElementById('bastFormModal').classList.remove('open'); }
async function saveBastForm(event) {
    if (event) event.preventDefault();
    if (!canUseBast()) return;
    const cells = [], map = {};
    BAST_COLUMNS.forEach(([, f], i) => { const el = document.getElementById('bast_' + f); cells.push(el ? el.value : ''); map[f] = i; });
    const r = { row: 1, ...bastRecFromRow(cells, map) };
    bastClashes([r]);
    if (r.errors.length) { showToast(r.errors[0], 'warning'); return; }
    const btn = document.getElementById('bastFormSave');
    btn.disabled = true;
    try {
        const out = await bastCreate(r.rec);
        try { localStorage.setItem(BAST_PREF_KEY, JSON.stringify(Object.fromEntries(['companyCode', 'place', 'p1Name', 'p1Title', 'p1Rep', 'witness1', 'witness2'].map(k => [k, r.rec[k]])))); } catch (_) {}
        _bastLastIds = [out.id];
        closeBastForm();
        showToast(`BAST ${out.no} made`, 'success');
        if (confirm(`BAST ${out.no} made. Print it to PDF now?`)) printBast([out.id], [out]);
    } catch (err) {
        console.error('[bast] save failed:', err);
        showToast(bastErrorText(err), 'error');
    } finally { btn.disabled = false; }
}

// ---- Ending a letter ----
async function endBastPrompt(id, status) {
    const b = bastList.find(x => x.id === id);
    if (!b || b.status !== 'active' || !canUseBast()) return;
    const date = prompt(status === 'returned' ? `Date the ${b.itemName} (${b.sn}) came back (YYYY-MM-DD):` : `Cancel letter ${b.no}? Its number is not used again. Date (YYYY-MM-DD):`, toISODate());
    if (date === null) return;
    const iso = bastDate(date);
    if (!iso || iso > toISODate() || (b.date && iso < b.date)) { showToast('Enter a date between the letter\'s date and today', 'warning'); return; }
    const reason = status === 'void' ? (prompt('Reason for cancelling (e.g. wrong recipient):', '') || '') : '';
    await bastEnd(b, status, iso, reason);
}
async function bastEnd(b, status, date, reason, opts) {
    try {
        await window.cloud.endBast(b.id, { status, date, reason: _bastTxt(reason, 200), by: _bastMe(), device: opts && opts.noDevice ? null : bastDeviceReturn });
        if (typeof logEvent === 'function') logEvent({ action: 'update', unitId: '', unitName: `[BAST] ${b.no}`, field: 'Status', before: 'Active', after: `${BAST_STATUS[status]} ${date}` });
        if (!(opts && opts.quiet)) showToast(status === 'returned' ? `BAST ${b.no} returned — the device is back in the warehouse` : `BAST ${b.no} cancelled`, 'success');
        return true;
    } catch (err) {
        console.error('[bast] end failed:', err);
        if (!(opts && opts.quiet)) showToast(bastErrorText(err), 'error');
        return false;
    }
}
// Called by Warehouse when a device leaves its allocation (or goes to someone else):
// its active letter is returned, the device itself already saved by Warehouse.
function bastDeviceLeft(device, date) {
    if (!device || !device.bastId) return;
    const b = bastList.find(x => x.id === device.bastId && x.status === 'active');
    if (b) bastEnd(b, 'returned', date || toISODate(), 'Device changed in Warehouse', { noDevice: true, quiet: true });
}
// Every letter of a device (any status), newest first — its BAST history (2.46.3).
function bastHistoryOfDevice(device) {
    if (!device) return [];
    const n = _bastNorm(device.sn);
    return bastList.filter(b => b.deviceId === device.id || (n && _bastNorm(b.sn) === n))
        .sort((a, b) => String(b.date || '').localeCompare(String(a.date || '')) || (b.createdAt || 0) - (a.createdAt || 0));
}
// The active letter of a device, if any.
function bastOfDevice(device) {
    return device ? bastList.find(b => b.status === 'active' && (b.deviceId === device.id || _bastNorm(b.sn) === _bastNorm(device.sn))) || null : null;
}

// ---- PDF (print) ----
function bastPageHtml(b) {
    const esc = v => escapeHtml(v == null ? '' : String(v));
    const kv = (k, v) => `<tr><td class="bast-k">${k}</td><td class="bast-c">:</td><td class="bast-v"><b>${esc(v)}</b></td></tr>`;
    const items = [['Nama Barang', b.itemName], ['Merk / Type', b.brandType], ['Radio ID DMR', b.radioId], ['S/N', b.sn], ['S/N Baterai', b.batterySn],
        ['Jumlah', b.qty], ['Satuan', b.unit], ['Kondisi', b.condition]].filter(([, v]) => v !== '' && v != null);
    const title = b.title || `BERITA ACARA SERAH TERIMA ${String(b.itemName || '').toUpperCase()}`;
    return `<section class="bast-page">
        <h1 class="bast-title">${esc(title.toUpperCase())}</h1>
        <p class="bast-no">Nomor : ${esc(b.no)}</p>
        <p>Pada hari ini, ${esc(bastDateText(b.date))}, bertempat di ${esc(b.place)}, telah dilakukan serah terima aset perusahaan berupa ${esc(b.itemName)} dari:</p>
        <p class="bast-party"><b>PIHAK PERTAMA</b></p>
        <table class="bast-kv">${kv('Nama', b.p1Name)}${kv('Jabatan', b.p1Title)}${kv('Mewakili', b.p1Rep)}</table>
        <p class="bast-party"><b>PIHAK KEDUA</b></p>
        <table class="bast-kv">${kv('Nama', b.p2Name)}${kv('Jabatan', b.p2Title)}${kv('Departemen', b.p2Dept)}</table>
        <p>Dengan ini PIHAK PERTAMA menyerahkan kepada PIHAK KEDUA dan PIHAK KEDUA menerima aset perusahaan dengan rincian sebagai berikut :</p>
        <table class="bast-kv bast-items">${items.map(([k, v]) => kv(k, v)).join('')}</table>
        <p>PIHAK KEDUA bertanggung jawab atas penggunaan, pemeliharaan, dan pengembalian ${esc(b.itemCode === 'HT' ? 'HT' : 'aset')} yang diterima, serta menanggung biaya perbaikan atau penggantian apabila terjadi kerusakan atau kehilangan akibat kelalaian, penyalahgunaan, atau penggunaan yang tidak sesuai prosedur.</p>
        <p>Demikian Berita Acara Serah Terima ini dibuat dan ditandatangani oleh kedua belah pihak setelah dipahami dan disetujui oleh kedua belah pihak untuk dipergunakan sebagaimana mestinya.</p>
        <table class="bast-sign">
            <tr><td>PIHAK PERTAMA</td><td></td><td>PIHAK KEDUA</td></tr>
            <tr class="bast-sign__space"><td></td><td></td><td></td></tr>
            <tr><td><b>${esc(b.p1Name)}</b></td><td></td><td><b>${esc(b.p2Name)}</b></td></tr>
            ${b.witness1 || b.witness2 ? `<tr class="bast-sign__gap"><td></td><td></td><td></td></tr>
            <tr><td>${b.witness1 ? 'MENGETAHUI' : ''}</td><td></td><td>${b.witness2 ? 'MENGETAHUI' : ''}</td></tr>
            <tr class="bast-sign__space"><td></td><td></td><td></td></tr>
            <tr><td><b>${esc(b.witness1)}</b></td><td></td><td><b>${esc(b.witness2)}</b></td></tr>` : ''}
        </table>
        ${b.status !== 'active' ? `<p class="bast-stamp">${esc(BAST_STATUS[b.status])}${b.endedAt ? ' ' + esc(b.endedAt) : ''}</p>` : ''}
    </section>`;
}
// Prints the letters (each on its own A4 page) — "Save as PDF" in the print dialog.
// extra: letters just made that the snapshot may not hold yet.
function printBast(ids, extra) {
    const all = bastList.concat((extra || []).filter(e => !bastList.some(b => b.id === e.id)));
    const list = (ids || []).map(id => all.find(b => b.id === id)).filter(Boolean);
    if (!list.length) { showToast('Nothing to print yet — wait a moment and try again', 'warning'); return; }
    let box = document.getElementById('bastPrint');
    if (!box) { box = document.createElement('div'); box.id = 'bastPrint'; document.body.appendChild(box); }
    box.innerHTML = list.map(bastPageHtml).join('');
    const old = document.title;
    document.title = list.length === 1 ? `BAST_${list[0].no.replace(/[^A-Za-z0-9-]+/g, '_')}` : `BAST_${list.length}_letters_${toISODate()}`;
    document.body.classList.add('bast-printing');
    const done = () => { document.body.classList.remove('bast-printing'); document.title = old; window.removeEventListener('afterprint', done); };
    window.addEventListener('afterprint', done);
    window.print();
    setTimeout(done, 1500);
}
