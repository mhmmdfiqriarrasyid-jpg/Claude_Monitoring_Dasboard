// 3D Models (2.44.0) — take implements and units apart, part by part.
// Two kinds of model, one library:
//   · 'glb'    — a GLB file (exported from CAD, Blender, SketchUp…) opened in the
//                built-in studio (models3d-viewer.js, three.js): every part can be
//                picked, taken off and put back, with its item #, part number,
//                quantity and notes kept per model in Firestore.
//   · 'studio' — a complete studio page someone built (like the Harrell Series 50
//                one). It runs as it is, inside <iframe sandbox="allow-scripts">
//                (models3d-frame.html): an opaque origin, so its scripts can reach
//                nothing of the dashboard — no session, no storage, no data.
// Every active account opens models; 'models3d' edit adds, changes and removes.
// A model can name implements and unit models: their rows and profiles get a
// 3D button that opens it.
'use strict';

let models3d = [];
let _m3Kind = '';                       // library filter: '' | 'glb' | 'studio'
const M3_CATEGORIES = ['Implement', 'Tractor', 'Heavy equipment', 'Device / GPS', 'Other'];
const M3_GLB_MAX = 200 * 1024 * 1024, M3_STUDIO_MAX = 15 * 1024 * 1024;
const M3_KIND_LABEL = { glb: 'GLB studio', studio: 'Studio page' };

function canEditModels3d() { return typeof hasAccess === 'function' && hasAccess('models3d', 'edit'); }
const _m3Esc = s => escapeHtml(String(s == null ? '' : s));
const _m3Norm = s => String(s == null ? '' : s).trim().toLowerCase();
const _m3Id = () => 'm3_' + Date.now().toString(36) + Math.random().toString(36).slice(2, 7);
// A part's key from the viewer ('n0.3.1') as a Firestore map key.
const _m3PKey = k => String(k).replace(/[^A-Za-z0-9_-]/g, '_');

// ---- Data ----
function _m3Valid(m) {
    return m && typeof m.id === 'string' && /^m3_[a-z0-9]+$/.test(m.id) && typeof m.name === 'string'
        && (m.kind === 'glb' || m.kind === 'studio') && m.file && typeof m.file.path === 'string';
}
let _m3ImplRedraw = 0, _m3ImplSig = '';
function applyModels3dSnapshot(list) {
    models3d = (list || []).filter(_m3Valid).sort((a, b) => a.name.localeCompare(b.name));
    if (!list || !list.length) { _m3Bytes.clear(); }   // signed out (or nothing left): no model kept in memory
    // The open model follows other editors' part notes and picture (same object: the
    // loaders compare their session, not the model).
    if (_m3Open) {
        const f = models3d.find(x => x.id === _m3Open.model.id);
        if (f) {
            const changed = JSON.stringify(f.parts || {}) !== JSON.stringify(_m3Open.model.parts || {});
            _m3Open.model.parts = f.parts || {}; _m3Open.model.thumb = f.thumb || ''; _m3Open.model.level = f.level;
            // Only when its notes changed — and the Part details form being typed in is left alone.
            if (_m3Open.viewer && changed) { m3RenderParts(); if (!_m3EditPart) m3RenderInsp(); }
        }
    }
    if (typeof currentView !== 'undefined' && currentView === 'models3d') renderModels3dView();
    // The 3D buttons on implement rows follow the links — redrawn only when the links
    // change (a redraw drops the rows ticked for "Delete selected").
    const sig = JSON.stringify(models3d.map(m => [m.id, m.implements || []]));
    if (sig === _m3ImplSig) return;
    _m3ImplSig = sig;
    if (typeof currentView !== 'undefined' && currentView === 'implements' && typeof renderImplementsTable === 'function' && !_m3ImplRedraw) {
        _m3ImplRedraw = setTimeout(() => { _m3ImplRedraw = 0; if (currentView === 'implements') renderImplementsTable(); }, 300);
    }
}

// ---- Links: implements and unit models ----
// An implement is named by its profile name, its "Type — Brand" label or its type.
function _m3ImplNames(imp) {
    if (!imp) return [];
    const out = [imp.profileName, imp.equipmentType];
    if (typeof implementOptionLabel === 'function') out.push(implementOptionLabel(imp));
    return out.map(_m3Norm).filter(Boolean);
}
function models3dForImplement(imp) {
    const names = new Set(_m3ImplNames(imp));
    return names.size ? models3d.filter(m => (m.implements || []).some(n => names.has(_m3Norm(n)))) : [];
}
function models3dForUnit(u) {
    if (!u) return [];
    const imp = typeof matchImplementForUnit === 'function' ? matchImplementForUnit(u.implement) : null;
    const names = new Set([_m3Norm(u.implement), ..._m3ImplNames(imp)].filter(Boolean));
    const model = _m3Norm(u.model);
    return models3d.filter(m => (m.implements || []).some(n => names.has(_m3Norm(n)))
        || (model && (m.unitModels || []).some(n => _m3Norm(n) === model)));
}
// Several models for one implement or unit: the library shows exactly those.
let _m3Only = null;   // Set of model ids from a 3D button, or null
function _m3OpenFor(list) {
    if (!list.length) return;
    if (list.length === 1) { openModel3d(list[0].id); return; }
    _m3Kind = '';
    const s = document.getElementById('m3Search');
    if (s) s.value = '';
    _m3Only = new Set(list.map(m => m.id));
    navigateTo('models3d');
}
function m3ShowAll() { _m3Only = null; renderModels3dView(); }
// The chip after an implement's name in the Implements table.
function m3ImplementChip(imp) {
    const list = models3dForImplement(imp);
    if (!list.length) return '';
    return ` <button type="button" class="m3-chip" title="Open the 3D model${list.length > 1 ? 's' : ''}" onclick="event.stopPropagation(); m3OpenForImplement(${jsArg(imp.id)})">3D</button>`;
}
function m3OpenForImplement(id) {
    const imp = (typeof globalImplements !== 'undefined' ? globalImplements : []).find(x => x.id === id);
    _m3OpenFor(models3dForImplement(imp));
}
// The 3D button in a unit's profile header.
function m3SetUnitButton(u) {
    const btn = document.getElementById('unitProfile3dBtn');
    if (!btn) return;
    const list = models3dForUnit(u);
    btn.hidden = !list.length;
    btn.onclick = list.length ? () => { if (typeof closeUnitProfile === 'function') closeUnitProfile(); _m3OpenFor(list); } : null;
    btn.title = list.length ? `Open ${list.length === 1 ? `"${list[0].name}"` : `${list.length} 3D models`}` : '';
}

// ---- Library ----
const _m3ThumbUrls = new Map();   // Storage path -> Promise of its URL
function _m3ThumbUrl(path) {
    if (!_m3ThumbUrls.has(path)) _m3ThumbUrls.set(path, window.cloud && window.cloud.model3dFileUrl ? window.cloud.model3dFileUrl(path).catch(() => '') : Promise.resolve(''));
    return _m3ThumbUrls.get(path);
}
function renderModels3dView() {
    const grid = document.getElementById('m3Grid');
    if (!grid) return;
    const q = _m3Norm((document.getElementById('m3Search') || {}).value);
    document.querySelectorAll('#m3Kinds [data-kind]').forEach(b => b.setAttribute('aria-pressed', b.dataset.kind === _m3Kind ? 'true' : 'false'));
    if (q) _m3Only = null;   // typing a search shows the whole library again
    const list = models3d.filter(m => (!_m3Only || _m3Only.has(m.id)) && (!_m3Kind || m.kind === _m3Kind)
        && (!q || [m.name, m.category, m.description, ...(m.implements || []), ...(m.unitModels || [])].some(v => _m3Norm(v).includes(q))));
    const cnt = document.getElementById('m3Count');
    if (cnt) cnt.innerHTML = _m3Only ? `${list.length} linked model${list.length === 1 ? '' : 's'} · <button type="button" class="lc-manage-link" onclick="m3ShowAll()">Show all</button>`
        : (models3d.length ? `${list.length} of ${models3d.length}` : '');
    const edit = canEditModels3d();
    if (!list.length) {
        grid.innerHTML = `<div class="m3-empty">${models3d.length ? 'No model matches the search.'
            : `No 3D model yet.${edit ? ' Add one: a GLB file (exported from CAD, Blender or SketchUp) or a studio page (.html).' : ' Ask whoever manages the 3D models to add one.'}`}</div>`;
        return;
    }
    grid.innerHTML = list.map(m => {
        const links = [...(m.implements || []), ...(m.unitModels || [])];
        const nParts = Object.keys(m.parts || {}).length;
        return `<article class="m3-card" data-id="${_m3Esc(m.id)}">
            <button type="button" class="m3-card__pic" onclick="openModel3d(${jsArg(m.id)})" aria-label="Open ${_m3Esc(m.name)}">
                ${m.thumb ? `<img alt="" data-thumb="${_m3Esc(m.thumb)}">` : `<span class="m3-card__ico" aria-hidden="true">${_M3_CUBE}</span>`}
                <span class="m3-badge m3-badge--${m.kind === 'glb' ? 'glb' : 'studio'}">${_m3Esc(M3_KIND_LABEL[m.kind])}</span>
            </button>
            <div class="m3-card__body">
                <div class="m3-card__cat">${_m3Esc(m.category || 'Other')}</div>
                <h3 class="m3-card__title">${_m3Esc(m.name)}</h3>
                ${m.description ? `<p class="m3-card__desc">${_m3Esc(m.description)}</p>` : ''}
                ${links.length ? `<div class="m3-card__links">${links.slice(0, 4).map(l => `<span class="m3-tag">${_m3Esc(l)}</span>`).join('')}${links.length > 4 ? `<span class="m3-tag m3-tag--more">+${links.length - 4}</span>` : ''}</div>` : ''}
                <div class="m3-card__meta">${_m3Esc(lcFmtSize(m.file.size))}${nParts ? ` · ${nParts} part note${nParts === 1 ? '' : 's'}` : ''}</div>
            </div>
            <div class="m3-card__acts">
                <button type="button" class="btn btn-primary btn-sm" onclick="openModel3d(${jsArg(m.id)})">Open</button>
                ${edit ? `<button type="button" class="btn btn-secondary btn-sm" onclick="editModel3d(${jsArg(m.id)})">Edit</button>
                <button type="button" class="btn-icon" onclick="deleteModel3d(${jsArg(m.id)})" aria-label="Delete ${_m3Esc(m.name)}" title="Delete"><i class="fas fa-trash" aria-hidden="true"></i></button>` : ''}
            </div>
        </article>`;
    }).join('');
    grid.querySelectorAll('img[data-thumb]').forEach(img => _m3ThumbUrl(img.dataset.thumb).then(u => { if (u) img.src = u; else img.remove(); }));
}
function setModels3dKind(kind) { _m3Kind = kind || ''; _m3Only = null; renderModels3dView(); }
const _M3_CUBE = '<svg width="44" height="44" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.4" stroke-linecap="round" stroke-linejoin="round"><path d="M12 2 3 7v10l9 5 9-5V7z"></path><path d="m3 7 9 5 9-5"></path><path d="M12 12v10"></path></svg>';

// ---- Add / edit ----
let _m3Edit = null, _m3File = null, _m3FileKind = '', _m3Pic = '', _m3Links = { implements: [], unitModels: [] }, _m3Busy = false, _m3FormSeq = 0;
function showAddModel3dForm() { if (!requireEdit('models3d')) return; _m3OpenForm(null); }
function editModel3d(id) { if (!requireEdit('models3d')) return; const m = models3d.find(x => x.id === id); if (m) _m3OpenForm(m); }
function _m3OpenForm(m) {
    _m3FormSeq++;
    _m3Edit = m; _m3File = null; _m3FileKind = ''; _m3Pic = '';
    _m3Links = { implements: [...((m && m.implements) || [])], unitModels: [...((m && m.unitModels) || [])] };
    document.getElementById('m3FormTitle').textContent = m ? 'Edit 3D model' : 'Add 3D model';
    document.getElementById('m3Name').value = m ? m.name : '';
    const cat = document.getElementById('m3Category');
    cat.innerHTML = M3_CATEGORIES.map(c => `<option>${_m3Esc(c)}</option>`).join('');
    cat.value = m && M3_CATEGORIES.includes(m.category) ? m.category : 'Implement';
    document.getElementById('m3Desc').value = m ? (m.description || '') : '';
    document.getElementById('m3FileInput').value = '';
    document.getElementById('m3PicInput').value = '';
    document.getElementById('m3FileNote').textContent = m ? `Now: ${m.file.name || m.file.path.split('/').pop()} (${M3_KIND_LABEL[m.kind]}, ${lcFmtSize(m.file.size)}) — pick a file only to replace it.${m.kind === 'glb' && Object.keys(m.parts || {}).length ? ' A new GLB keeps the part notes: they follow each part\'s place in the file.' : ''}` : 'A .glb file, or a studio page (.html).';
    document.getElementById('m3PicNote').textContent = m && m.thumb ? 'A picture is set — pick one only to replace it.' : (m && m.kind === 'glb' ? 'Optional — a GLB model gets one the first time it is opened.' : 'Optional.');
    _m3Progress(null);
    // Suggestions: the implements and the unit models already in the dashboard.
    const impl = new Set();
    (typeof globalImplements !== 'undefined' ? globalImplements : []).forEach(i => [i.profileName, i.equipmentType].forEach(v => { if (v && String(v).trim()) impl.add(String(v).trim()); }));
    document.getElementById('m3ImplList').innerHTML = [...impl].sort().map(v => `<option value="${_m3Esc(v)}">`).join('');
    const um = new Set();
    (typeof globalData !== 'undefined' ? globalData : []).forEach(u => { if (u.model && String(u.model).trim()) um.add(String(u.model).trim()); });
    document.getElementById('m3UnitModelList').innerHTML = [...um].sort().map(v => `<option value="${_m3Esc(v)}">`).join('');
    _m3RenderChips();
    document.getElementById('m3Modal').classList.add('open');
    setTimeout(() => document.getElementById('m3Name').focus(), 50);
}
function closeModel3dForm() {
    if (_m3Busy && !confirm('The upload is still running. Close anyway? The model is not saved.')) return;
    _m3FormSeq++;   // a save still running for this form stops and leaves nothing behind
    document.getElementById('m3Modal').classList.remove('open');
    _m3Edit = null; _m3File = null; _m3Pic = '';
    _m3SetFormBusy(false);
}
// While a file uploads the form cannot change under it.
function _m3SetFormBusy(on) {
    _m3Busy = !!on;
    ['m3SaveBtn', 'm3FileInput', 'm3PicInput'].forEach(id => { const el = document.getElementById(id); if (el) el.disabled = !!on; });
}
function _m3RenderChips() {
    ['implements', 'unitModels'].forEach(kind => {
        const el = document.getElementById(kind === 'implements' ? 'm3ImplChips' : 'm3UnitModelChips');
        el.innerHTML = _m3Links[kind].map((v, i) => `<span class="unit-chip">${_m3Esc(v)}<button type="button" aria-label="Remove ${_m3Esc(v)}" onclick="m3RemoveLink('${kind}', ${i})">×</button></span>`).join('');
    });
}
function m3AddLink(kind) {
    const input = document.getElementById(kind === 'implements' ? 'm3ImplAdd' : 'm3UnitModelAdd');
    const v = (input.value || '').trim().slice(0, 120);
    if (!v) return;
    if (!_m3Links[kind].some(x => _m3Norm(x) === _m3Norm(v)) && _m3Links[kind].length < 30) _m3Links[kind].push(v);
    input.value = '';
    _m3RenderChips();
}
function m3RemoveLink(kind, i) { _m3Links[kind].splice(i, 1); _m3RenderChips(); }
async function m3PickFile(event) {
    const f = event && event.target && event.target.files && event.target.files[0];
    _m3File = null; _m3FileKind = '';
    if (!f) return;
    const note = document.getElementById('m3FileNote');
    const ext = (f.name.split('.').pop() || '').toLowerCase();
    try {
        if (ext === 'glb') {
            if (f.size > M3_GLB_MAX) throw new Error(`That file is ${lcFmtSize(f.size)} — the limit is ${lcFmtSize(M3_GLB_MAX)}`);
            const head = new Uint8Array(await f.slice(0, 4).arrayBuffer());
            if (String.fromCharCode(...head) !== 'glTF') throw new Error('That is not a GLB file (a .gltf with separate files can be turned into one .glb in Blender: File → Export → glTF Binary)');
            _m3FileKind = 'glb';
        } else if (ext === 'html' || ext === 'htm') {
            if (f.size > M3_STUDIO_MAX) throw new Error(`That file is ${lcFmtSize(f.size)} — the limit is ${lcFmtSize(M3_STUDIO_MAX)}`);
            const text = await f.slice(0, 4096).text();
            if (!/<(!doctype html|html|script|body)/i.test(text)) throw new Error('That does not look like an HTML studio page');
            _m3FileKind = 'studio';
        } else if (ext === 'gltf') {
            throw new Error('A .gltf comes with separate files — export it as one .glb (glTF Binary) instead');
        } else {
            throw new Error('Pick a .glb model or an .html studio page');
        }
        _m3File = f;
        note.textContent = `${f.name} — ${M3_KIND_LABEL[_m3FileKind]}, ${lcFmtSize(f.size)}`;
        if (!document.getElementById('m3Name').value.trim()) document.getElementById('m3Name').value = f.name.replace(/\.[^.]+$/, '').replace(/[_-]+/g, ' ').slice(0, 160);
    } catch (err) {
        event.target.value = '';
        note.textContent = err.message;
        showToast(err.message, 'warning');
    }
}
async function m3PickPic(event) {
    const f = event && event.target && event.target.files && event.target.files[0];
    _m3Pic = '';
    if (!f) return;
    if (!/^image\//.test(f.type || '')) { showToast('Pick a picture (JPG or PNG)', 'warning'); event.target.value = ''; return; }
    try {
        _m3Pic = await compressImageToDataURL(f, { maxDim: 960, maxBytes: 300 * 1024 });
        document.getElementById('m3PicNote').textContent = `${f.name} — set as the card picture`;
    } catch (_) { showToast('The picture could not be read', 'warning'); }
}
function _m3Progress(p, label) {
    const box = document.getElementById('m3Progress');
    if (!box) return;
    box.hidden = p == null;
    if (p == null) return;
    document.getElementById('m3ProgressBar').style.width = `${Math.round(p * 100)}%`;
    document.getElementById('m3ProgressText').textContent = label || `Uploading ${Math.round(p * 100)}%…`;
}
const _m3SafeName = (name, kind) => {
    let n = String(name || '').replace(/[^A-Za-z0-9_.-]+/g, '_').replace(/^\.+/, '').slice(-80) || 'model';
    const want = kind === 'glb' ? '.glb' : '.html';
    if (!n.toLowerCase().endsWith(want)) n += want;
    return n;
};
const _m3DataUrlBlob = u => { const [h, b] = u.split(','); const bin = atob(b); const a = new Uint8Array(bin.length); for (let i = 0; i < bin.length; i++) a[i] = bin.charCodeAt(i); return new Blob([a], { type: (/data:([^;]+)/.exec(h) || [])[1] || 'image/jpeg' }); };
async function _m3JpegBlob(dataUrl) {
    if (/^data:image\/jpe?g/.test(dataUrl)) return _m3DataUrlBlob(dataUrl);
    // A PNG picture is drawn again as JPEG (the card picture is always thumb.jpg).
    const img = await new Promise((res, rej) => { const i = new Image(); i.onload = () => res(i); i.onerror = rej; i.src = dataUrl; });
    const c = document.createElement('canvas'); c.width = img.naturalWidth; c.height = img.naturalHeight;
    const g = c.getContext('2d'); g.fillStyle = '#fff'; g.fillRect(0, 0, c.width, c.height); g.drawImage(img, 0, 0);
    return _m3DataUrlBlob(c.toDataURL('image/jpeg', 0.82));
}
async function saveModel3d(event) {
    if (event) event.preventDefault();
    if (_m3Busy || !requireEdit('models3d')) return;
    const name = document.getElementById('m3Name').value.trim().slice(0, 160);
    if (!name) { showToast('Give the model a name', 'warning'); document.getElementById('m3Name').focus(); return; }
    if (!_m3Edit && !_m3File) { showToast('Pick the model file (.glb or .html)', 'warning'); return; }
    if (!window.cloud || !window.cloud.saveModel3d || !window.cloud.uploadModel3dFile) { showToast('Reload the page — an old version is still active', 'warning'); return; }
    // A link typed but not added with + still counts.
    ['implements', 'unitModels'].forEach(k => m3AddLink(k));
    // 2.44.2: everything the form holds is read now — the uploads take a while, and the
    // form may be closed (or another one opened) before they end.
    const seq = _m3FormSeq, f = _m3File, fKind = _m3FileKind, pic = _m3Pic, editId = _m3Edit ? _m3Edit.id : null;
    const category = document.getElementById('m3Category').value || 'Other';
    const description = document.getElementById('m3Desc').value.trim().slice(0, 2000);
    const links = { implements: _m3Links.implements.slice(0, 30), unitModels: _m3Links.unitModels.slice(0, 30) };
    const id = editId || _m3Id();
    const uploaded = [];
    let picPath = '';
    const stillOpen = () => seq === _m3FormSeq;
    _m3SetFormBusy(true);
    try {
        let file = null, newThumb = '';
        if (f) {
            // Each upload has a path of its own: the file in use is never overwritten,
            // so a failed or abandoned save can always remove what it uploaded.
            const path = `models3d/${id}/${Date.now().toString(36)}-${_m3SafeName(f.name, fKind)}`;
            const type = fKind === 'glb' ? 'model/gltf-binary' : 'text/plain;charset=utf-8';   // a studio page is kept as text: its link never runs as a page
            _m3Progress(0);
            await window.cloud.uploadModel3dFile(path, f, type, p => { if (stillOpen()) _m3Progress(p); });
            uploaded.push(path);
            file = { path, name: f.name.slice(0, 160), size: f.size, contentType: type };
            if (!stillOpen()) throw Object.assign(new Error('closed'), { code: 'm3/closed' });
        }
        if (pic) {
            if (stillOpen()) _m3Progress(1, 'Saving the picture…');
            const tp = `models3d/${id}/thumb.jpg`;
            await window.cloud.uploadModel3dFile(tp, await _m3JpegBlob(pic), 'image/jpeg');
            if (!editId) uploaded.push(tp);
            _m3ThumbUrls.delete(tp);
            newThumb = tp; picPath = tp;
            if (!stillOpen()) throw Object.assign(new Error('closed'), { code: 'm3/closed' });
        }
        // The model as it is now (another editor may have changed it since the form opened).
        const old = editId ? models3d.find(x => x.id === editId) : null;
        if (editId && !old) throw Object.assign(new Error('gone'), { code: 'm3/gone' });
        const kind = f ? fKind : old.kind;
        const me = (currentUserDoc && (currentUserDoc.displayName || currentUserDoc.email)) || '';
        // A new GLB without a picture of its own gets a fresh one the first time it is opened.
        const thumb = newThumb || (f && kind === 'glb' ? '' : ((old && old.thumb) || ''));
        const rec = {
            ...(old || {}), id, name, kind, category, description,
            file: file || { ...old.file }, thumb,
            implements: links.implements, unitModels: links.unitModels,
            // Part notes belong to a GLB: a studio page has none.
            parts: kind === 'glb' && old && old.kind === 'glb' ? (old.parts || {}) : {},
            createdByUid: old ? (old.createdByUid || '') : myUid(),
            createdBy: old ? (old.createdBy || '') : me,
            createdAt: old ? (old.createdAt || Date.now()) : Date.now(),
            updatedAt: Date.now(), updatedBy: me
        };
        if (!old || !old.level || kind !== 'glb') delete rec.level;
        // An existing model is updated, never written anew: if someone deleted it
        // meanwhile, the update fails instead of bringing it back.
        const upd = { ...rec };
        if (old && kind === 'glb' && old.kind === 'glb') delete upd.parts;   // part notes stay as other editors left them
        await (old && window.cloud.updateModel3d ? window.cloud.updateModel3d(upd) : window.cloud.saveModel3d(rec));
        if (old && file) window.cloud.deleteModel3dFile(old.file.path).catch(() => {});
        if (old && old.thumb && !rec.thumb) { window.cloud.deleteModel3dFile(old.thumb).catch(() => {}); _m3ThumbUrls.delete(old.thumb); }
        const i = models3d.findIndex(x => x.id === id);
        if (i >= 0) models3d[i] = rec; else models3d.push(rec);
        _m3Bytes.delete(old && file ? old.file.path : '');
        if (typeof logEvent === 'function') logEvent({ action: old ? 'update' : 'add', unitId: '', unitName: `[3D] ${name}`, field: '3D model', before: '', after: M3_KIND_LABEL[kind] });
        if (stillOpen()) {
            _m3SetFormBusy(false);   // another form opened since keeps its own busy state
            showToast(old ? '3D model saved' : '3D model added', 'success');
            closeModel3dForm();
        } else showToast(`"${name}" was saved after all`, 'success');
        renderModels3dView();
    } catch (err) {
        // An upload whose document never landed is removed again.
        uploaded.forEach(p => window.cloud.deleteModel3dFile(p).catch(() => {}));
        // The model was deleted meanwhile: its picture, uploaded just now, goes too.
        if (editId && picPath && err && (err.code === 'm3/gone' || err.code === 'not-found')) { window.cloud.deleteModel3dFile(picPath).catch(() => {}); _m3ThumbUrls.delete(picPath); }
        if (err && err.code === 'm3/closed') return;   // "Close anyway": nothing saved, nothing left behind
        console.error('[3d] save failed:', err);
        showToast(err && (err.code === 'm3/gone' || err.code === 'not-found') ? 'This model was deleted meanwhile — nothing was saved'
            : err && (err.code === 'permission-denied' || err.code === 'storage/unauthorized') ? '3D Models need the new firestore.rules and storage.rules — publish them first'
            : 'The model was not saved — try again', 'error');
    } finally {
        if (stillOpen()) { _m3SetFormBusy(false); _m3Progress(null); }
        else if (_m3Busy && !document.getElementById('m3Modal').classList.contains('open')) _m3SetFormBusy(false);
    }
}
async function deleteModel3d(id) {
    if (!requireEdit('models3d')) return;
    const m = models3d.find(x => x.id === id);
    if (!m || !confirm(`Delete the 3D model "${m.name}"?\n\nIts file and part notes are removed for everyone.`)) return;
    try {
        await window.cloud.deleteModel3d(id);
        window.cloud.deleteModel3dFile(m.file.path).catch(() => {});
        window.cloud.deleteModel3dFile(m.thumb || `models3d/${m.id}/thumb.jpg`).catch(() => {});
        models3d = models3d.filter(x => x.id !== id);
        if (typeof logEvent === 'function') logEvent({ action: 'delete', unitId: '', unitName: `[3D] ${m.name}`, field: '3D model', before: M3_KIND_LABEL[m.kind], after: '' });
        showToast('3D model deleted', 'success');
        renderModels3dView();
    } catch (err) {
        showToast(err && err.code === 'permission-denied' ? 'View-only access — ask the owner for edit rights' : 'The model was not deleted — try again', 'error');
    }
}

// ---- Open ----
let _m3Open = null;          // the open viewer: { model, ctl, viewer?, onMsg?, onKey? } — each open is its own object
// The last models opened, kept for a quick reopen — small files only (a 200 MB GLB is
// not held in memory after its viewer closes).
const _m3Bytes = new Map();  // Storage path -> ArrayBuffer | string
const M3_CACHE_MAX = 40 * 1024 * 1024;
async function _m3Fetch(model, asText, onProgress, signal) {
    const key = model.file.path;
    if (_m3Bytes.has(key)) return _m3Bytes.get(key);
    const url = await window.cloud.model3dFileUrl(key);
    let res;
    try { res = await fetch(url, { signal }); }
    catch (err) {
        if (err && err.name === 'AbortError') throw err;
        const e = new Error('The file could not be read (the bucket needs CORS for this site, as for the Learning Center)'); e.url = url; throw e;
    }
    if (!res.ok) throw new Error(`The file could not be read (HTTP ${res.status})`);
    const total = Number(res.headers.get('content-length')) || 0;
    let buf;
    if (res.body && res.body.getReader && total) {
        // Read straight into one buffer (no list of chunks plus a copy of them).
        const reader = res.body.getReader();
        let out = new Uint8Array(total), got = 0;
        for (;;) {
            const { done, value } = await reader.read();
            if (done) break;
            if (got + value.length > out.length) { const bigger = new Uint8Array(Math.max(out.length * 2, got + value.length)); bigger.set(out.subarray(0, got)); out = bigger; }
            out.set(value, got); got += value.length;
            if (onProgress) onProgress(Math.min(1, got / total));
        }
        buf = got === out.length ? out.buffer : out.slice(0, got).buffer;
    } else buf = await res.arrayBuffer();
    const val = asText ? new TextDecoder('utf-8').decode(buf) : buf;
    if (buf.byteLength <= M3_CACHE_MAX) {
        _m3Bytes.set(key, val);
        while (_m3Bytes.size > 2) _m3Bytes.delete(_m3Bytes.keys().next().value);
    }
    return val;
}
function openModel3d(id) {
    const m = models3d.find(x => x.id === id);
    if (!m) { showToast('That 3D model is no longer there', 'warning'); return; }
    closeModel3dViewer(true);
    document.getElementById('m3ViewerTitle').textContent = m.name;
    document.getElementById('m3ViewerSub').textContent = [M3_KIND_LABEL[m.kind], m.category].filter(Boolean).join(' · ');
    document.getElementById('m3Viewer').classList.add('open');
    document.getElementById('m3Viewer').dataset.kind = m.kind;
    // 2.44.2: every open is its own session; the loaders check it, not the model (the
    // same model opened twice used to build two viewers).
    const o = { model: m, ctl: typeof AbortController === 'function' ? new AbortController() : null };
    _m3Open = o;
    if (m.kind === 'studio') _m3OpenStudio(m, o); else _m3OpenGlb(m, o);
}
function closeModel3dViewer(quiet) {
    const o = _m3Open;
    _m3Open = null;
    if (o) {
        if (o.ctl) { try { o.ctl.abort(); } catch (_) {} }   // a download still running stops
        if (o.onMsg) window.removeEventListener('message', o.onMsg);
        if (o.viewer) { try { o.viewer.dispose(); } catch (_) {} }
        if (o.onKey) document.removeEventListener('keydown', o.onKey);
    }
    const body = document.getElementById('m3ViewerBody');
    if (body) body.innerHTML = '';
    const v = document.getElementById('m3Viewer');
    if (v && !quiet) v.classList.remove('open');
}
function _m3Fail(msg) {
    const body = document.getElementById('m3ViewerBody');
    if (body) body.innerHTML = `<p class="m3-fail">${_m3Esc(msg)}</p>`;
}
const _m3Aborted = err => err && err.name === 'AbortError';

// A studio page: fetched as text, then handed to the sandboxed frame, which turns into it.
async function _m3OpenStudio(m, o) {
    const body = document.getElementById('m3ViewerBody');
    body.innerHTML = '<p class="m3-wait" id="m3Wait">Opening…</p>';
    let html;
    try { html = await _m3Fetch(m, true, p => { const w = document.getElementById('m3Wait'); if (w && _m3Open === o) w.textContent = `Loading ${Math.round(p * 100)}%…`; }, o.ctl && o.ctl.signal); }
    catch (err) { if (_m3Open === o && !_m3Aborted(err)) _m3Fail(navigator.onLine ? err.message : 'This model needs a signal to open'); return; }
    if (_m3Open !== o) return;
    const frame = document.createElement('iframe');
    frame.className = 'm3-studio';
    frame.title = m.name;
    // allow-scripts only: no allow-same-origin (so an opaque origin), no forms, popups or top navigation.
    frame.setAttribute('sandbox', 'allow-scripts');
    frame.setAttribute('referrerpolicy', 'no-referrer');
    let sent = false;
    // Only this frame is listened to: 'ready' gets the page (once); 'escape' — Escape
    // pressed inside the studio, where the dashboard's own key handler cannot hear it —
    // closes the viewer.
    const onMsg = e => {
        if (e.source !== frame.contentWindow || !e.data) return;
        if (e.data.type === 'ot-studio-ready' && !sent) {
            sent = true;
            frame.contentWindow.postMessage({ type: 'ot-studio', html }, '*');   // the frame's origin is opaque: '*' is the only target
        } else if (e.data.type === 'ot-studio-escape' && _m3Open === o) closeModel3dViewer();
    };
    o.onMsg = onMsg;
    window.addEventListener('message', onMsg);
    frame.src = 'models3d-frame.html';
    body.innerHTML = '';
    body.appendChild(frame);
}

// ---- GLB studio ----
let _m3ThreeP = null;
function _m3LoadThree() {
    if (window.M3Viewer && window.THREE && THREE.GLTFLoader && THREE.OrbitControls) return Promise.resolve();
    if (_m3ThreeP) return _m3ThreeP;
    const add = src => new Promise((res, rej) => { const s = document.createElement('script'); s.src = src; s.onload = res; s.onerror = () => rej(new Error('The 3D viewer could not be loaded — check the signal')); document.head.appendChild(s); });
    _m3ThreeP = (window.THREE ? Promise.resolve() : add('vendor/three/three.min.js'))
        .then(() => Promise.all([THREE.GLTFLoader ? 0 : add('vendor/three/GLTFLoader.js'), THREE.OrbitControls ? 0 : add('vendor/three/OrbitControls.js')]))
        .then(() => (window.M3Viewer ? 0 : add('models3d-viewer.js')))
        .catch(err => { _m3ThreeP = null; throw err; });
    return _m3ThreeP;
}
const _M3_STUDIO_HTML = `
<div class="m3s" id="m3Studio">
    <aside class="m3s-panel" id="m3Panel">
        <div class="m3s-panel__head">
            <button type="button" class="m3s-tool m3s-tool--panel m3s-panel__close" onclick="m3TogglePanel(false)" aria-controls="m3Panel">Close parts</button>
            <label class="m3s-field"><span>Part detail</span><select id="m3Level" onchange="m3SetLevel(this.value)"></select></label>
            <input type="search" id="m3PartSearch" class="form-input" placeholder="Search name, item # or part number" oninput="m3RenderParts()">
            <div class="m3s-filters" role="group" aria-label="Show">
                <button type="button" data-f="" onclick="m3Filter('')">All</button>
                <button type="button" data-f="on" onclick="m3Filter('on')">In place</button>
                <button type="button" data-f="off" onclick="m3Filter('off')">Taken off</button>
            </div>
        </div>
        <div class="m3s-list" id="m3PartList"></div>
        <div class="m3s-panel__foot">
            <div class="m3s-progress"><span id="m3sProgressText">—</span><span class="m3s-bar"><i id="m3sProgressBar"></i></span></div>
            <div class="m3s-foot-btns">
                <button type="button" class="btn btn-secondary btn-sm" onclick="m3Do('detachAll')">Take all apart</button>
                <button type="button" class="btn btn-secondary btn-sm" onclick="m3Do('attachAll')">Put all back</button>
            </div>
        </div>
    </aside>
    <div class="m3s-stage">
        <div class="m3s-canvas" id="m3Canvas"></div>
        <div class="m3s-wait" id="m3Loading">Opening…</div>
        <div class="m3s-tools" role="toolbar" aria-label="View">
            <button type="button" class="m3s-tool m3s-tool--panel" onclick="m3TogglePanel()" aria-controls="m3Panel">Parts</button>
            <button type="button" class="m3s-tool" onclick="m3Do('view','iso')">Iso</button>
            <button type="button" class="m3s-tool" onclick="m3Do('view','front')">Front</button>
            <button type="button" class="m3s-tool" onclick="m3Do('view','side')">Side</button>
            <button type="button" class="m3s-tool" onclick="m3Do('view','top')">Top</button>
            <button type="button" class="m3s-tool" onclick="m3Do('view','fit')">Fit</button>
            <span class="m3s-sep"></span>
            <button type="button" class="m3s-tool" id="m3XrayBtn" aria-pressed="false" onclick="m3Toggle('xray')">X-ray</button>
            <button type="button" class="m3s-tool" id="m3GridBtn" aria-pressed="true" onclick="m3Toggle('grid')">Grid</button>
            <button type="button" class="m3s-tool" id="m3RotateBtn" aria-pressed="false" onclick="m3Toggle('rotate')">Rotate</button>
        </div>
        <div class="m3s-bottom">
            <button type="button" class="btn btn-secondary btn-sm" onclick="m3Do('step',-1)" title="Put back the last part taken off">◀ Put back</button>
            <label class="m3s-explode"><span>Explode</span><input type="range" id="m3Explode" min="0" max="100" value="0" oninput="m3Explode(this.value)"><b id="m3ExplodeVal">0%</b></label>
            <button type="button" class="btn btn-secondary btn-sm" onclick="m3Do('step',1)" title="Take off the next part (outside in)">Take off ▶</button>
        </div>
        <div class="m3s-insp" id="m3Insp" hidden></div>
        <div class="m3s-tip" id="m3Tip" hidden></div>
    </div>
</div>`;
let _m3PartFilter = '', _m3EditPart = false, _m3IsolateKey = null;
async function _m3OpenGlb(m, o) {
    const body = document.getElementById('m3ViewerBody');
    body.innerHTML = _M3_STUDIO_HTML;
    _m3PartFilter = ''; _m3EditPart = false; _m3IsolateKey = null;
    const wait = t => { const w = document.getElementById('m3Loading'); if (w && _m3Open === o) { w.hidden = !t; w.textContent = t || ''; } };
    let buf;
    try {
        await _m3LoadThree();
        buf = await _m3Fetch(m, false, p => wait(`Loading ${Math.round(p * 100)}%…`), o.ctl && o.ctl.signal);
    } catch (err) { if (_m3Open === o && !_m3Aborted(err)) _m3Fail(navigator.onLine ? err.message : 'This model needs a signal to open'); return; }
    if (_m3Open !== o) return;
    wait('Building the model…');
    const tip = document.getElementById('m3Tip');
    let v;
    // The constructor throws when the browser has no WebGL (or the viewer files did not load).
    try { v = new M3Viewer(document.getElementById('m3Canvas'), {
        onSelect: () => { _m3EditPart = false; m3RenderInsp(); m3RenderParts(); },
        onHover: (key, x, y) => {
            if (!tip) return;
            if (!key) { tip.hidden = true; return; }
            const p = v.part(key), info = _m3Info(key);
            const stage = tip.parentElement.getBoundingClientRect();
            tip.innerHTML = `${info.itemNo || info.partNumber ? `<b>${_m3Esc([info.itemNo, info.partNumber].filter(Boolean).join(' · '))}</b>` : ''}<span>${_m3Esc(info.name || (p && p.name) || '')}</span>`;
            tip.style.left = `${Math.min(stage.width - 200, x - stage.left + 14)}px`;
            tip.style.top = `${Math.max(0, y - stage.top + 14)}px`;
            tip.hidden = false;
        },
        onChange: () => { _m3SchedRender(); }
    }); } catch (err) { _m3Fail(err && err.message ? err.message : 'This browser cannot show 3D models'); return; }
    // Kept on the session before loading: closing meanwhile disposes it.
    o.viewer = v;
    try { await v.load(buf); }
    catch (err) { if (_m3Open === o) _m3Fail(err && err.message ? err.message : 'The model could not be read'); return; }
    buf = null;
    if (_m3Open !== o) return;
    // The level the editor chose last time, when it still exists.
    if (m.level != null && v.levels.some(l => String(l.level) === String(m.level))) v.setLevel(m.level === 'mesh' ? 'mesh' : Number(m.level));
    wait('');
    const lv = document.getElementById('m3Level');
    lv.innerHTML = v.levels.map(l => `<option value="${_m3Esc(l.level)}">${l.level === 'mesh' ? 'Every piece' : `Level ${_m3Esc(l.level)}`} (${l.count})</option>`).join('');
    lv.value = String(v.level);
    o.onKey = e => {
        if (_m3Open !== o) return;
        const t = e.target || {};
        if (/^(INPUT|TEXTAREA|SELECT)$/.test(t.tagName || '') || t.isContentEditable) return;
        if (e.key === 'ArrowRight') { e.preventDefault(); m3Do('step', 1); }
        else if (e.key === 'ArrowLeft') { e.preventDefault(); m3Do('step', -1); }
        // Space takes the picked part off / puts it back — not when a button has the
        // focus: there Space presses the button.
        else if (e.key === ' ' && v.selected && !(t.closest && t.closest('button, a[href], [role=button], summary'))) { e.preventDefault(); m3Do('toggle'); }
    };
    document.addEventListener('keydown', o.onKey);
    m3RenderParts(); m3RenderInsp();
    // A model without a card picture gets one, taken by the first editor who opens it.
    if (!m.thumb && canEditModels3d()) setTimeout(() => _m3MakeThumb(m), 900);
}
async function _m3MakeThumb(m) {
    const o = _m3Open;
    if (!o || o.model !== m || !o.viewer || m.thumb || !window.cloud.updateModel3d) return;
    // Still there, and still without a picture (someone may have deleted it or set one).
    const cur = models3d.find(x => x.id === m.id);
    if (!cur || cur.thumb) return;
    try {
        const data = o.viewer.snapshot(640);
        if (!/^data:image\/jpeg/.test(data)) return;
        const tp = `models3d/${m.id}/thumb.jpg`;
        await window.cloud.uploadModel3dFile(tp, _m3DataUrlBlob(data), 'image/jpeg');
        await window.cloud.updateModel3d({ id: m.id, thumb: tp, updatedAt: Date.now() });
        m.thumb = tp; _m3ThumbUrls.delete(tp);
    } catch (err) { console.warn('[3d] card picture not saved:', err && (err.code || err.message)); }
}
let _m3RenderQ = 0;
function _m3SchedRender() {
    if (_m3RenderQ) return;
    _m3RenderQ = requestAnimationFrame(() => { _m3RenderQ = 0; m3RenderParts(); m3RenderInsp(); });
}
// The notes kept for a part: item #, part number, name, quantity, note.
function _m3Info(key) {
    const m = _m3Open && _m3Open.model;
    const p = m && m.parts && m.parts[_m3PKey(key)];
    return p && typeof p === 'object' ? p : {};
}
const M3_LIST_MAX = 600;
function m3RenderParts() {
    const o = _m3Open, list = document.getElementById('m3PartList');
    if (!o || !o.viewer || !list) return;
    const v = o.viewer, q = _m3Norm((document.getElementById('m3PartSearch') || {}).value);
    document.querySelectorAll('.m3s-filters [data-f]').forEach(b => b.setAttribute('aria-pressed', b.dataset.f === _m3PartFilter ? 'true' : 'false'));
    const parts = v.parts.filter(p => (!_m3PartFilter || (_m3PartFilter === 'off') === !!p.detached)
        && (!q || [p.name, p.path, _m3Info(p.key).itemNo, _m3Info(p.key).partNumber, _m3Info(p.key).name].some(x => _m3Norm(x).includes(q))));
    const sel = v.selected;
    list.innerHTML = parts.slice(0, M3_LIST_MAX).map(p => {
        const info = _m3Info(p.key);
        return `<div class="m3s-row${p.detached ? ' is-off' : ''}${p.key === sel ? ' is-sel' : ''}${p.hidden ? ' is-hidden' : ''}" data-key="${_m3Esc(p.key)}">
            <button type="button" class="m3s-row__main" onclick="m3Pick(this.parentElement.dataset.key)">
                <span class="m3s-row__no">${_m3Esc(info.itemNo || p.order)}</span>
                <span class="m3s-row__txt"><b>${_m3Esc(info.name || p.name)}</b>${info.partNumber ? `<small>${_m3Esc(info.partNumber)}</small>` : ''}</span>
            </button>
            <button type="button" class="m3s-row__act" onclick="m3Do('toggle', this.parentElement.dataset.key)" title="${p.detached ? 'Put back' : 'Take off'}" aria-label="${p.detached ? 'Put back' : 'Take off'} ${_m3Esc(info.name || p.name)}">${p.detached ? '↩' : '↗'}</button>
        </div>`;
    }).join('') + (parts.length > M3_LIST_MAX ? `<div class="m3s-more">Showing ${M3_LIST_MAX} of ${parts.length} — search to narrow</div>` : '')
        + (!parts.length ? '<div class="m3s-more">No part matches</div>' : '');
    const off = v.parts.filter(p => p.detached).length, all = v.parts.length;
    const t = document.getElementById('m3sProgressText'); if (t) t.textContent = `${all - off} / ${all} in place`;
    const bar = document.getElementById('m3sProgressBar'); if (bar) bar.style.width = `${all ? Math.round(100 * (all - off) / all) : 0}%`;
    const sync = (id, on) => { const b = document.getElementById(id); if (b) b.setAttribute('aria-pressed', on ? 'true' : 'false'); };
    sync('m3XrayBtn', v.xray);
}
function m3RenderInsp() {
    const o = _m3Open, box = document.getElementById('m3Insp');
    if (!o || !o.viewer || !box) return;
    const v = o.viewer, key = v.selected, p = key ? v.part(key) : null;
    if (!p) { box.hidden = true; return; }
    const info = _m3Info(key), edit = canEditModels3d();
    box.hidden = false;
    if (_m3EditPart && edit) {
        box.innerHTML = `<form class="m3s-insp__form" onsubmit="m3SavePart(event)">
            <div class="m3s-insp__head"><b>Part details</b><button type="button" class="btn-icon" onclick="m3EditPart(false)" aria-label="Cancel">×</button></div>
            <div class="m3s-insp__grid">
                <label>Item #<input class="form-input" id="m3pItem" maxlength="20" value="${_m3Esc(info.itemNo || '')}"></label>
                <label>Part number<input class="form-input" id="m3pNum" maxlength="60" value="${_m3Esc(info.partNumber || '')}"></label>
                <label class="m3s-insp__wide">Name<input class="form-input" id="m3pName" maxlength="120" value="${_m3Esc(info.name || p.name)}"></label>
                <label>Qty<input class="form-input" id="m3pQty" type="number" min="0" max="9999" value="${_m3Esc(info.qty == null ? 1 : info.qty)}"></label>
                <label class="m3s-insp__wide">Note<textarea class="form-input" id="m3pNote" maxlength="600" rows="3">${_m3Esc(info.note || '')}</textarea></label>
            </div>
            <div class="m3s-insp__acts"><button type="submit" class="btn btn-primary btn-sm">Save</button></div>
        </form>`;
        return;
    }
    box.innerHTML = `<div class="m3s-insp__head">
            <div>${info.itemNo || info.partNumber ? `<div class="m3s-insp__nums">${info.itemNo ? `<span class="m3-tag m3-tag--dark">${_m3Esc(info.itemNo)}</span>` : ''}${info.partNumber ? `<b>${_m3Esc(info.partNumber)}</b>` : ''}</div>` : ''}
            <h4>${_m3Esc(info.name || p.name)}</h4></div>
            <button type="button" class="btn-icon" onclick="m3Pick(null)" aria-label="Close the part card">×</button></div>
        <div class="m3s-insp__meta"><span>Qty <b>${_m3Esc(info.qty == null ? 1 : info.qty)}</b></span><span>Order <b>#${_m3Esc(p.order)}</b></span><span>${p.detached ? 'Taken off' : 'In place'}</span></div>
        ${p.path && p.path !== p.name ? `<div class="m3s-insp__path">${_m3Esc(p.path)}</div>` : ''}
        ${info.note ? `<p class="m3s-insp__note">${_m3Esc(info.note)}</p>` : ''}
        <div class="m3s-insp__acts">
            <button type="button" class="btn btn-primary btn-sm" onclick="m3Do('toggle')">${p.detached ? 'Put back' : 'Take off'}</button>
            <button type="button" class="btn btn-secondary btn-sm" onclick="m3Do('focus')">Focus</button>
            <button type="button" class="btn btn-secondary btn-sm" onclick="m3Do('isolate')">${_m3IsolateKey === key ? 'Show all' : 'Only this'}</button>
            ${edit ? '<button type="button" class="btn btn-secondary btn-sm" onclick="m3EditPart(true)">Edit details</button>' : ''}
        </div>`;
}
function m3Pick(key) {
    const v = _m3Open && _m3Open.viewer;
    if (!v) return;
    v.select(key || null);
    if (key) v.focus(key);
    if (key && window.matchMedia && window.matchMedia('(max-width: 900px)').matches) m3TogglePanel(false);
}
function m3Do(action, arg) {
    const v = _m3Open && _m3Open.viewer;
    if (!v) return;
    const key = typeof arg === 'string' && action !== 'view' ? arg : v.selected;
    switch (action) {
    case 'toggle': if (key) v.toggle(key); break;
    case 'focus': if (key) v.focus(key); break;
    case 'isolate': _m3IsolateKey = _m3IsolateKey === key ? null : key; v.isolate(_m3IsolateKey); break;
    case 'detachAll': v.detachAll(); break;
    case 'attachAll': v.attachAll(); break;
    case 'step': v.step(arg); break;
    case 'view': v.view(arg); break;
    }
    _m3SchedRender();
}
function m3Toggle(what) {
    const v = _m3Open && _m3Open.viewer;
    if (!v) return;
    const btn = document.getElementById(what === 'xray' ? 'm3XrayBtn' : what === 'grid' ? 'm3GridBtn' : 'm3RotateBtn');
    const on = btn.getAttribute('aria-pressed') !== 'true';
    btn.setAttribute('aria-pressed', on ? 'true' : 'false');
    if (what === 'xray') v.setXray(on); else if (what === 'grid') v.setGrid(on); else v.setAutoRotate(on);
}
function m3Explode(val) {
    const v = _m3Open && _m3Open.viewer;
    if (!v) return;
    v.setExplode(Math.max(0, Math.min(100, Number(val) || 0)) / 100);
    const b = document.getElementById('m3ExplodeVal'); if (b) b.textContent = `${Math.round(Number(val) || 0)}%`;
}
function m3Filter(f) { _m3PartFilter = f; m3RenderParts(); }
function m3SetLevel(level) {
    const o = _m3Open;
    if (!o || !o.viewer) return;
    o.viewer.setLevel(level === 'mesh' ? 'mesh' : Number(level));
    _m3IsolateKey = null;
    const ex = document.getElementById('m3Explode'); if (ex) { ex.value = 0; m3Explode(0); }
    // An editor's choice becomes everyone's starting level — only while the model has no
    // part notes: once it has, the level is the one they were written at (m3SavePart).
    const cur = models3d.find(x => x.id === o.model.id);
    if (cur && canEditModels3d() && !Object.keys(cur.parts || {}).length && String(cur.level) !== String(level) && window.cloud.updateModel3d) {
        o.model.level = level;
        window.cloud.updateModel3d({ id: cur.id, level: level === 'mesh' ? 'mesh' : Number(level), updatedAt: Date.now() }).catch(() => {});
    }
}
function m3TogglePanel(open) {
    const s = document.getElementById('m3Studio');
    if (s) s.classList.toggle('is-panel', open == null ? !s.classList.contains('is-panel') : !!open);
}
function m3EditPart(on) { _m3EditPart = !!on; m3RenderInsp(); }
async function m3SavePart(event) {
    if (event) event.preventDefault();
    const o = _m3Open;
    if (!o || !o.viewer || !o.viewer.selected || !requireEdit('models3d')) return;
    const key = o.viewer.selected, p = o.viewer.part(key);
    const val = id => (document.getElementById(id).value || '').trim();
    const n = parseInt(val('m3pQty'), 10);
    const qty = Number.isFinite(n) ? Math.max(0, Math.min(9999, n)) : 1;   // a blank quantity is the default, 1
    const info = { itemNo: val('m3pItem').slice(0, 20), partNumber: val('m3pNum').slice(0, 60), name: val('m3pName').slice(0, 120), qty, note: val('m3pNote').slice(0, 600) };
    if (p && info.name === p.name) info.name = '';   // the model's own name is not stored twice
    const cur = models3d.find(x => x.id === o.model.id);
    if (!cur) { showToast('This model was deleted meanwhile — nothing was saved', 'warning'); return; }
    const pk = _m3PKey(key);
    const empty = !info.itemNo && !info.partNumber && !info.name && qty === 1 && !info.note;
    const count = Object.keys(cur.parts || {}).length + (cur.parts && cur.parts[pk] ? 0 : 1);
    if (!empty && count > 3000) { showToast('Up to 3,000 parts can carry notes', 'warning'); return; }
    try {
        // Only this part's notes are written: another editor's notes on other parts stay.
        await window.cloud.updateModel3dPart(cur.id, pk, empty ? null : info, o.viewer.level);
        // From the model as it is now: another editor's note may have arrived during the write.
        const latest = models3d.find(x => x.id === cur.id) || cur;
        const parts = { ...(latest.parts || {}) };
        if (empty) delete parts[pk]; else parts[pk] = info;
        latest.parts = parts; latest.level = o.viewer.level; o.model.parts = parts; o.model.level = o.viewer.level;
        _m3EditPart = false;
        showToast('Part details saved', 'success');
        m3RenderInsp(); m3RenderParts();
    } catch (err) {
        showToast(err && err.code === 'not-found' ? 'This model was deleted meanwhile — nothing was saved'
            : err && err.code === 'permission-denied' ? 'View-only access — ask the owner for edit rights'
            : err && err.code === 'invalid-argument' ? 'This model has too many part notes to save more' : 'Not saved — try again', 'error');
    }
}
