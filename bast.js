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
    ['Jumlah', 'qty'], ['Satuan', 'unit'], ['Kondisi', 'condition'], ['Mengetahui 1 - Nama', 'witness1'], ['Mengetahui 1 - Jabatan', 'witness1Title'],
    ['Mengetahui 2 - Nama', 'witness2'], ['Mengetahui 2 - Jabatan', 'witness2Title'], ['Mengetahui 3 - Nama', 'witness3'], ['Mengetahui 3 - Jabatan', 'witness3Title'],
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
                ${b.returnNo ? `<button type="button" class="btn btn-secondary btn-sm" onclick="printReturnLetter('${esc(b.id)}')" title="Berita Acara Pengembalian ${esc(b.returnNo)}"><i class="fas fa-file-pdf" aria-hidden="true"></i> Return</button>` : ''}
                ${b.status === 'active' ? `<button type="button" class="btn btn-secondary btn-sm" onclick="returnFromBast('${esc(b.id)}')">Returned</button>
                <button type="button" class="btn btn-secondary btn-sm" onclick="endBastPrompt('${esc(b.id)}', 'void')">Cancel</button>` : ''}
            </td></tr>`).join('')
        : `<tr><td colspan="8" class="adm-muted" style="text-align:center;padding:24px">${bastList.length ? 'No letter matches.' : 'No BAST yet — import the Excel format or make one from an allocated device.'}</td></tr>`;
    const cnt = document.getElementById('bastCount');
    if (cnt) cnt.textContent = `${rows.length} letter(s)`;
    // 2.46.5: the owner may remove cancelled letters (picked, or all of them).
    const canDel = typeof isOwner === 'function' && isOwner() && _bastStatus === 'void';
    ['bastDelPicked', 'bastDelAll'].forEach(id => { const el = document.getElementById(id); if (el) el.style.display = canDel && count('void') ? '' : 'none'; });
    const rs = document.getElementById('bastResetNo');
    if (rs) rs.style.display = typeof isOwner === 'function' && isOwner() ? '' : 'none';
    const ac = document.getElementById('bastAllocCount');
    if (ac) ac.textContent = bastAllocatedWithout().length;
}
function setBastStatus(s) { _bastStatus = s; renderBastView(); }
function bastPicked() { return [...document.querySelectorAll('#bastBody .bast-pick:checked')].map(i => i.value); }
function bastToggleAll(on) { document.querySelectorAll('#bastBody .bast-pick').forEach(i => { i.checked = on; }); }
// ---- Removing cancelled letters (owner only, 2.46.5) ----
// A cancelled letter was a mistake (wrong recipient, wrong data) and never in force. It is kept
// as a record until the owner removes it; its number is never given out again either way.
async function deleteBasts(ids) {
    if (!(typeof isOwner === 'function' && isOwner())) { showToast('Only the Super Admin can delete letters', 'warning'); return; }
    const list = ids.map(id => bastList.find(b => b.id === id)).filter(b => b && b.status === 'void');
    if (!list.length) { showToast('Only cancelled letters can be deleted — tick them in the Cancelled tab', 'warning'); return; }
    if (!confirm(`Delete ${list.length} cancelled letter(s) for good?\n\n${list.slice(0, 8).map(b => b.no).join('\n')}${list.length > 8 ? `\n… and ${list.length - 8} more` : ''}\n\nTheir numbers are not used again.`)) return;
    try {
        await window.cloud.deleteBasts(list.map(b => b.id));
        list.forEach(b => { if (typeof logEvent === 'function') logEvent({ action: 'delete', unitId: '', unitName: `[BAST] ${b.no}`, field: 'Cancelled letter', before: `${b.p2Name} · ${b.sn}` }); });
        showToast(`${list.length} cancelled letter(s) deleted`, 'success');
    } catch (err) {
        console.error('[bast] delete failed:', err);
        showToast(err && err.code === 'permission-denied' ? 'Only the Super Admin can delete letters' : 'Failed to delete', 'error');
    }
}
// ---- Reset numbering (owner only, 2.46.6) ----
// For a start after trial letters: cancelled letters go, numbers no letter holds are released,
// and each counter restarts after the highest number still on a letter (001 when none is left).
async function resetBastNumbering() {
    if (!(typeof isOwner === 'function' && isOwner())) { showToast('Only the Super Admin can reset the numbering', 'warning'); return; }
    const kept = bastList.filter(b => b.status !== 'void');
    const cancelled = bastList.filter(b => b.status === 'void').length;
    const top = {};
    kept.forEach(b => { const m = /\/(\d+)$/.exec(String(b.no || '')); if (!m) return; const k = `${b.companyCode}/BAST-${b.itemCode}/${String(b.date || '').slice(0, 4)}`; top[k] = Math.max(top[k] || 0, Number(m[1])); });
    const plan = Object.keys(top).length ? Object.entries(top).map(([k, n]) => `${k}: next number ${String(n + 1).padStart(3, '0')}`).join('\n') : 'Every series starts again at 001.';
    if (!confirm(`Reset the BAST numbering?\n\n${cancelled ? `${cancelled} cancelled letter(s) are deleted.\n` : ''}Numbers no letter holds are released.\n${kept.length ? `${kept.length} active/returned letter(s) keep their numbers.\n` : ''}\n${plan}`)) return;
    try {
        const r = await window.cloud.resetBastNumbers();
        if (typeof logEvent === 'function') logEvent({ action: 'update', unitId: '', unitName: '[BAST] Numbering', field: 'Reset', after: `${r.cancelled} cancelled deleted, ${r.released} number(s) released` });
        showToast(`Numbering reset — ${r.cancelled} cancelled letter(s) deleted, ${r.released} number(s) released`, 'success');
    } catch (err) {
        console.error('[bast] reset failed:', err);
        showToast(err && err.code === 'permission-denied' ? 'Refused — publish the new firestore.rules first (Super Admin only)' : 'Failed to reset the numbering', 'error');
    }
}
function deleteBastPicked() {
    const ids = bastPicked();
    if (!ids.length) { showToast('Tick the cancelled letters to delete first', 'warning'); return; }
    deleteBasts(ids);
}
function deleteBastAllCancelled() { deleteBasts(bastList.filter(b => b.status === 'void').map(b => b.id)); }
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
        showToast(err && err.code === 'xlsx/load' ? err.message
            : /NotReadable|could not be read|permission/i.test(String(err && (err.name || err.message)))
            ? 'This file could not be read — close it in Excel first, then import again' : 'This file could not be read', 'error');
        return;
    }
    // 2.47.3: anything that goes wrong after reading is said, not swallowed (and lands in the Error log).
    try { bastPreview(rows); }
    catch (err) {
        console.error('[bast] import check failed:', err);
        if (typeof reportClientError === 'function') reportClientError('bast-import', (err && err.message) || String(err), err && err.stack, '');
        showToast(`Import stopped: ${(err && err.message) || err} — send this message to the admin`, 'error');
    }
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
        qty: 1, unit: 'Set', condition: 'Baik', witness1: last.witness1 || '', witness1Title: last.witness1Title || '', witness2: last.witness2 || '', witness2Title: last.witness2Title || '',
        witness3: last.witness3 || '', witness3Title: last.witness3Title || '', deviceType: d.type || '', title: '', note: ''
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
const BAST_PREF_FIELDS = ['companyCode', 'place', 'p1Name', 'p1Title', 'p1Rep', 'witness1', 'witness1Title', 'witness2', 'witness2Title', 'witness3', 'witness3Title'];
function showBastForm(fromDeviceId) {
    if (!canUseBast()) { showToast('BAST needs Warehouse edit access', 'warning'); return; }
    const f = document.getElementById('bastForm');
    f.reset();
    let last = {};
    try { last = JSON.parse(localStorage.getItem(BAST_PREF_KEY) || '{}') || {}; } catch (_) { last = {}; }
    const set = (id, v) => { const el = document.getElementById(id); if (el) el.value = v == null ? '' : v; };
    BAST_PREF_FIELDS.forEach(k => set('bast_' + k, last[k] || ''));
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
        try { localStorage.setItem(BAST_PREF_KEY, JSON.stringify(Object.fromEntries(BAST_PREF_FIELDS.map(k => [k, r.rec[k]])))); } catch (_) {}
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
        <div class="bast-sign">
            <div class="bast-sign__row"><div>PIHAK PERTAMA</div><div></div><div>PIHAK KEDUA</div></div>
            <div class="bast-sign__space"></div>
            <div class="bast-sign__row">${bastSigner(b.p1Name, b.p1Title)}<div></div>${bastSigner(b.p2Name, b.p2Title)}</div>
            ${bastWitnessRow(b)}
        </div>
        ${b.status !== 'active' ? `<p class="bast-stamp">${esc(BAST_STATUS[b.status])}${b.endedAt ? ' ' + esc(b.endedAt) : ''}</p>` : ''}
    </section>`;
}
// One signer: the name (bold, underlined, one line) and the position under it.
function bastSigner(name, title) {
    const esc = v => escapeHtml(v == null ? '' : String(v));
    const n = String(name || '');
    return `<div class="bast-signer"><b class="bast-signer__name${n.length > 26 ? ' bast-signer__name--long' : ''}">${esc(n)}</b>${title ? `<span class="bast-signer__title">${esc(title)}</span>` : ''}</div>`;
}
// MENGETAHUI, centred, and up to three witnesses on the same three columns as the parties:
// one in the middle, two on the outer columns, three across.
function bastWitnessRow(b) {
    const w = [[b.witness1, b.witness1Title], [b.witness2, b.witness2Title], [b.witness3, b.witness3Title]].filter(([n]) => n);
    if (!w.length) return '';
    const cells = w.length === 1 ? ['', w[0], ''] : w.length === 2 ? [w[0], '', w[1]] : w;
    return `<div class="bast-sign__mid">MENGETAHUI</div>
            <div class="bast-sign__space"></div>
            <div class="bast-sign__row">${cells.map(c => c ? bastSigner(c[0], c[1]) : '<div></div>').join('')}</div>`;
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

// ============================================================
// Return of allocated devices (2.47.0)
// ------------------------------------------------------------
// One small dialog (a device's "Return" button, or several letters ticked on the BAST page):
// date, condition (the BAST list) and a note — required when the item comes back damaged or
// incomplete. The device goes back to the warehouse (or to Damaged), the allocation goes into
// its history with the return, and its active BAST is marked Returned. A return letter
// (Berita Acara Pengembalian) is optional: numbered after its BAST (…/BAPB-<KODE>/…).
// ============================================================
let _retIds = [];
const RET_DAMAGED = ['Rusak Ringan', 'Rusak Berat'];
function bastReturnNo(b) { return String(b.no || '').replace('/BAST-', '/BAPB-'); }
function returnDevicePrompt(ids) {
    if (!canUseBast()) { showToast('Returning needs Warehouse edit access', 'warning'); return; }
    const list = (Array.isArray(ids) ? ids : [ids]).map(id => warehouseDevices.find(d => d.id === id)).filter(d => d && d.status === 'allocated');
    if (!list.length) { showToast('Only allocated devices can be returned', 'warning'); return; }
    _retIds = list.map(d => d.id);
    const esc = v => escapeHtml(v == null ? '' : String(v));
    document.getElementById('retTitle').textContent = list.length === 1 ? `Return ${list[0].type || 'device'} · ${list[0].sn}` : `Return ${list.length} devices`;
    document.getElementById('retList').innerHTML = list.map(d => {
        const b = bastOfDevice(d);
        return `<div class="ret-item"><div class="ret-item__dev"><b>${esc(d.type || 'Device')}</b> <span class="adm-mono">${esc(d.sn)}</span></div>
            <div class="ret-item__who">Held by <b>${esc(d.recipientName || '—')}</b>${d.division ? ` · ${esc(d.division)}` : ''}${d.allocatedAt ? ` · since ${esc(fpShortDate(d.allocatedAt))}` : ''}</div>
            <div class="ret-item__doc">${b ? `BAST ${esc(b.no)}` : '<span class="adm-muted">No BAST</span>'}</div></div>`;
    }).join('');
    // 2.47.2: who brings it back — the holder by default (one device), anyone else typed in.
    const holders = [...new Set(list.map(d => d.recipientName).filter(Boolean))];
    document.getElementById('retBy').value = holders.length === 1 ? holders[0] : '';
    // 2.47.1: no default — the return is often filed days later, so the date is always chosen.
    const rd = document.getElementById('retDate');
    rd.value = ''; rd.max = toISODate();
    rd.min = list.map(d => d.allocatedAt || '').sort().pop() || '';
    document.getElementById('retCondition').value = 'Baik';
    document.getElementById('retNote').value = '';
    const withBast = list.filter(d => bastOfDevice(d)).length;
    const letter = document.getElementById('retLetter');
    letter.checked = false;
    letter.disabled = !withBast;
    document.getElementById('retLetterHint').textContent = withBast
        ? (withBast < list.length ? `For the ${withBast} device(s) with a BAST.` : 'Numbered after its BAST (…/BAPB-…).')
        : 'Only for a device with a BAST — make its BAST first.';
    document.getElementById('devReturnModal').classList.add('open');
}
function closeDeviceReturn() { document.getElementById('devReturnModal').classList.remove('open'); _retIds = []; }
// The device fields a return sets; the allocation goes into the history with how it came back.
function deviceReturnPatch(cur, r) {
    const hist = (Array.isArray(cur.allocationHistory) ? cur.allocationHistory.slice() : []).concat([Object.assign({
        division: cur.division || '', recipientName: cur.recipientName || '', recipientTitle: cur.recipientTitle || '', allocatedAt: cur.allocatedAt || '',
        returnedAt: r.date, returnCondition: r.condition, returnNote: r.note, returnedBy: r.by, returnerName: r.returner || ''
    }, cur.bastNo ? { bastNo: cur.bastNo } : {}, r.returnNo ? { returnNo: r.returnNo } : {})]).slice(-20);
    return { status: RET_DAMAGED.includes(r.condition) ? 'damaged' : 'warehouse', division: '', recipientName: '', recipientTitle: '', allocatedAt: '',
        allocationHistory: hist, bastId: '', bastNo: '', updatedAt: Date.now() };
}
async function saveDeviceReturn(event) {
    if (event) event.preventDefault();
    if (!canUseBast() || !_retIds.length) return;
    const date = bastDate(document.getElementById('retDate').value);
    const condition = document.getElementById('retCondition').value;
    const note = _bastTxt(document.getElementById('retNote').value, 300);
    const returner = _bastTxt(document.getElementById('retBy').value, 120);
    const wantLetter = document.getElementById('retLetter').checked;
    const devs = _retIds.map(id => warehouseDevices.find(d => d.id === id)).filter(Boolean);
    if (!returner) { showToast('Fill in who returned it', 'warning'); document.getElementById('retBy').focus(); return; }
    if (!date) { showToast('Choose the date the device actually came back', 'warning'); document.getElementById('retDate').focus(); return; }
    if (date > toISODate()) { showToast('The return date cannot be in the future', 'warning'); return; }
    if (devs.some(d => d.allocatedAt && date < d.allocatedAt)) { showToast('The return date is before the allocation date', 'warning'); return; }
    if (RET_DAMAGED.includes(condition) && !note) { showToast('Describe the damage or what is missing in the note', 'warning'); document.getElementById('retNote').focus(); return; }
    const btn = document.getElementById('retSave');
    btn.disabled = true;
    const letters = [];
    let done = 0;
    try {
        for (const d of devs) {
            const b = bastOfDevice(d);
            const r = { date, condition, note, returner, by: _bastMe(), returnNo: b && wantLetter ? bastReturnNo(b) : '' };
            if (b && b.deviceId === d.id) {
                const extra = { returnCondition: condition, returnNote: note, returnBy: r.by, returnerName: returner, returnNo: r.returnNo, returnLetter: !!r.returnNo };
                const out = await window.cloud.endBast(b.id, { status: 'returned', date, reason: note, by: r.by, extra,
                    device: cur => (cur && cur.status === 'allocated' ? deviceReturnPatch(cur, r) : null) });
                if (r.returnNo) letters.push(Object.assign({}, b, out));
            } else {
                await window.cloud.saveDevice(Object.assign({ id: d.id }, deviceReturnPatch(d, r)));
                if (b) await bastEnd(b, 'returned', date, note, { noDevice: true, quiet: true });
            }
            if (typeof logEvent === 'function') logEvent({ action: 'update', unitId: '', unitName: `[Inventory] ${d.type || 'Device'} ${d.sn}`, field: 'Returned',
                before: `${d.recipientName || ''}${d.division ? ' · ' + d.division : ''}`, after: `${date} · by ${returner} · ${condition}${note ? ' · ' + note : ''}` });
            done++;
        }
        closeDeviceReturn();
        showToast(`${done} device(s) returned${RET_DAMAGED.includes(condition) ? ' — marked Damaged' : ' to the warehouse'}`, 'success');
        if (letters.length) printReturnLetters(letters);
    } catch (err) {
        console.error('[return] failed:', err);
        showToast(`${done ? `${done} returned, then: ` : ''}${bastErrorText(err)}`, 'error');
    } finally { btn.disabled = false; }
}
// The BAST page's "Returned" opens the same dialog for the letter's device(s).
function returnFromBast(ids) {
    const devIds = (Array.isArray(ids) ? ids : [ids]).map(id => bastList.find(b => b.id === id)).filter(b => b && b.status === 'active')
        .map(b => (warehouseDevices.find(d => d.id === b.deviceId) || bastDeviceFor(b.sn) || {}).id).filter(Boolean);
    if (devIds.length) returnDevicePrompt(devIds);
    else if (!Array.isArray(ids)) endBastPrompt(ids, 'returned');   // no device in Warehouse: the letter alone
    else showToast('None of the ticked letters has an allocated device', 'warning');
}
function returnBastPicked() {
    const ids = bastPicked();
    if (!ids.length) { showToast('Tick the active letters to return first', 'warning'); return; }
    returnFromBast(ids);
}

// ---- Return letter (Berita Acara Pengembalian) ----
function returnLetterHtml(b) {
    const esc = v => escapeHtml(v == null ? '' : String(v));
    const rName = b.returnerName || b.p2Name, rSame = rName === b.p2Name;
    const kv = (k, v) => `<tr><td class="bast-k">${k}</td><td class="bast-c">:</td><td class="bast-v"><b>${esc(v)}</b></td></tr>`;
    const items = [['Nama Barang', b.itemName], ['Merk / Type', b.brandType], ['Radio ID DMR', b.radioId], ['S/N', b.sn], ['S/N Baterai', b.batterySn],
        ['Jumlah', b.qty], ['Satuan', b.unit], ['Kondisi saat kembali', b.returnCondition], ['Catatan', b.returnNote]].filter(([, v]) => v !== '' && v != null);
    return `<section class="bast-page">
        <h1 class="bast-title">${esc(`BERITA ACARA PENGEMBALIAN ${String(b.itemName || '').toUpperCase()}`)}</h1>
        <p class="bast-no">Nomor : ${esc(b.returnNo)}</p>
        <p>Pada hari ini, ${esc(bastDateText(b.endedAt))}, bertempat di ${esc(b.place)}, telah dilakukan pengembalian aset perusahaan berupa ${esc(b.itemName)} yang sebelumnya diserahkan dengan Berita Acara Serah Terima Nomor ${esc(b.no)}, dari:</p>
        <p class="bast-party"><b>PIHAK PERTAMA</b> (yang mengembalikan)</p>
        <table class="bast-kv">${kv('Nama', rName)}${rSame ? kv('Jabatan', b.p2Title) : ''}${kv('Departemen', b.p2Dept)}${rSame ? '' : kv('Atas nama', b.p2Name)}</table>
        <p class="bast-party"><b>PIHAK KEDUA</b> (yang menerima)</p>
        <table class="bast-kv">${kv('Nama', b.p1Name)}${kv('Jabatan', b.p1Title)}${kv('Mewakili', b.p1Rep)}</table>
        <p>Dengan ini PIHAK PERTAMA mengembalikan kepada PIHAK KEDUA dan PIHAK KEDUA menerima kembali aset perusahaan dengan rincian sebagai berikut :</p>
        <table class="bast-kv bast-items">${items.map(([k, v]) => kv(k, v)).join('')}</table>
        <p>Demikian Berita Acara Pengembalian ini dibuat dan ditandatangani oleh kedua belah pihak untuk dipergunakan sebagaimana mestinya.</p>
        <div class="bast-sign">
            <div class="bast-sign__row"><div>PIHAK PERTAMA</div><div></div><div>PIHAK KEDUA</div></div>
            <div class="bast-sign__space"></div>
            <div class="bast-sign__row">${bastSigner(rName, rSame ? b.p2Title : '')}<div></div>${bastSigner(b.p1Name, b.p1Title)}</div>
            ${bastWitnessRow(b)}
        </div>
    </section>`;
}
function printReturnLetters(list) {
    if (!list.length) return;
    let box = document.getElementById('bastPrint');
    if (!box) { box = document.createElement('div'); box.id = 'bastPrint'; document.body.appendChild(box); }
    box.innerHTML = list.map(returnLetterHtml).join('');
    const old = document.title;
    document.title = list.length === 1 ? `BAPB_${String(list[0].returnNo).replace(/[^A-Za-z0-9-]+/g, '_')}` : `BAPB_${list.length}_letters_${toISODate()}`;
    document.body.classList.add('bast-printing');
    const done = () => { document.body.classList.remove('bast-printing'); document.title = old; window.removeEventListener('afterprint', done); };
    window.addEventListener('afterprint', done);
    window.print();
    setTimeout(done, 1500);
}
function printReturnLetter(bastId) {
    const b = bastList.find(x => x.id === bastId);
    if (b && b.returnNo) printReturnLetters([b]);
}

// ---- One timeline per device: allocated / returned, with its BAST and return letter ----
function deviceTimelineHtml(d) {
    const esc = v => escapeHtml(v == null ? '' : String(v));
    const byNo = no => (no ? bastList.find(b => b.no === no) : null);
    const pdf = (label, call) => `<button type="button" class="btn btn-secondary btn-sm" onclick="${call}"><i class="fas fa-file-pdf" aria-hidden="true"></i> ${label}</button>`;
    const canBast = canUseBast();
    const periods = (Array.isArray(d.allocationHistory) ? d.allocationHistory : []).map(h => Object.assign({}, h, { current: false }));
    if (d.status === 'allocated') periods.push({ division: d.division, recipientName: d.recipientName, recipientTitle: d.recipientTitle, allocatedAt: d.allocatedAt, bastNo: d.bastNo, current: true });
    const ev = [];
    periods.forEach((p, i) => {
        const b = byNo(p.bastNo) || (p.current ? bastOfDevice(d) : null);
        const bastBit = b ? `BAST <b>${esc(b.no)}</b> ${canBast ? pdf('BAST', `printBast(['${esc(b.id)}'])`) : ''}`
            : (p.bastNo ? `BAST <b>${esc(p.bastNo)}</b>` : (p.current && canBast ? `No BAST yet <button type="button" class="btn btn-secondary btn-sm" onclick="closeDeviceProfile(); showBastForm('${esc(d.id)}')"><i class="fas fa-file-signature" aria-hidden="true"></i> Make BAST</button>` : '<span class="adm-muted">No BAST</span>'));
        ev.push({ at: p.allocatedAt || '', ord: i * 2, html: `<li class="dev-tl__item dev-tl__item--alloc${p.current ? ' is-current' : ''}">
            <span class="dev-tl__dot" aria-hidden="true"></span>
            <div><div class="dev-tl__head"><b>Allocated</b> ${p.allocatedAt ? esc(fpShortDate(p.allocatedAt)) : ''}${p.current ? ' <span class="wh-status wh-status--allocated">Still allocated</span>' : ''}</div>
            <div>→ ${esc(p.recipientName) || '—'}${p.recipientTitle ? ` · ${esc(p.recipientTitle)}` : ''}${p.division ? ` · ${esc(p.division)}` : ''}</div>
            <div class="dev-tl__docs">${bastBit}</div></div></li>` });
        if (p.returnedAt) {
            const rb = b && b.returnNo ? b : null;
            ev.push({ at: p.returnedAt, ord: i * 2 + 1, html: `<li class="dev-tl__item dev-tl__item--ret">
                <span class="dev-tl__dot" aria-hidden="true"></span>
                <div><div class="dev-tl__head"><b>Returned</b> ${esc(fpShortDate(p.returnedAt))} <span class="adm-sub">${esc(_devDays(p.allocatedAt, p.returnedAt))}</span></div>
                <div>← ${p.returnerName ? `by <b>${esc(p.returnerName)}</b> · ` : ''}${p.returnCondition ? `condition <b>${esc(p.returnCondition)}</b>` : 'back in the warehouse'}${p.returnNote ? ` · ${esc(p.returnNote)}` : ''}${p.returnedBy ? ` <span class="adm-sub">recorded by ${esc(p.returnedBy)}</span>` : ''}</div>
                ${p.returnNo ? `<div class="dev-tl__docs">Return letter <b>${esc(p.returnNo)}</b> ${rb && canBast ? pdf('Return letter', `printReturnLetter('${esc(rb.id)}')`) : ''}</div>` : ''}</div></li>` });
        }
    });
    ev.sort((a, b) => String(b.at).localeCompare(String(a.at)) || b.ord - a.ord);
    return `<h4 class="dev-alloc-title">History <span class="team-tab__count">${periods.length}</span></h4>` +
        (ev.length ? `<ol class="dev-tl">${ev.map(e => e.html).join('')}</ol>` : '<p class="adm-muted">This device has never been allocated.</p>');
}
