/* Operation Technology Dashboard — Firebase / Firestore / Auth initializer
   Loaded as an ES module from index.html. Bridges Firebase Auth + Firestore
   to the classic (non-module) script.js via a global window.cloud API.        */

import { initializeApp } from "https://www.gstatic.com/firebasejs/12.12.0/firebase-app.js";
import {
    getFirestore,
    collection,
    doc,
    setDoc,
    updateDoc,
    deleteDoc,
    getDoc,
    getDocs,
    onSnapshot,
    writeBatch,
    runTransaction,
    query,
    orderBy,
    limit,
    where
} from "https://www.gstatic.com/firebasejs/12.12.0/firebase-firestore.js";
// Namespace import of the SAME module (the browser already has it cached) so
// the newer cache API can be feature-detected. A named import of something the
// SDK does not export is a link error that takes the whole app down; reading a
// missing property off a namespace is just undefined.
import * as FS from "https://www.gstatic.com/firebasejs/12.12.0/firebase-firestore.js";
import {
    getAuth,
    onAuthStateChanged,
    createUserWithEmailAndPassword,
    signInWithEmailAndPassword,
    sendPasswordResetEmail,
    signOut,
    updateProfile,
    setPersistence,
    browserLocalPersistence
} from "https://www.gstatic.com/firebasejs/12.12.0/firebase-auth.js";

// Every record's id IS its document id. The stored "id" field is whatever the
// writer put there, and the app builds edit/delete targets and click handlers
// from it — so a document whose data claims someone else's id (or carries
// markup in it) steered the UI. The document id cannot be forged that way.
// The whole collection, or — for an account limited to one unit group — only
// that group's documents.
function groupQuery(col, group) {
    return group ? query(collection(db, col), where('unitGroup', '==', group)) : collection(db, col);
}
const withDocId = d => ({ ...d.data(), id: d.id });

// A listener that also reports which documents are still only on this device
// (written offline, not yet acknowledged by the server): callback(list, ids).
// includeMetadataChanges makes it fire again when they land; snapshots that
// change nothing but metadata, and leave that set as it was, are skipped so a
// list is not redrawn for nothing.
function _onSnapshotWithPending(ref, callback, onError) {
    let last = null;
    return onSnapshot(ref, { includeMetadataChanges: true }, snap => {
        const pending = snap.docs.filter(d => d.metadata.hasPendingWrites).map(d => d.id);
        const key = pending.join(',');
        if (last !== null && key === last && !snap.docChanges().length) return;
        last = key;
        callback(snap.docs.map(withDocId), pending);
    }, onError);
}

const firebaseConfig = {
    apiKey: "AIzaSyB0rZdvv44jDErvA6dGCrivyueY-2UB-Mw",
    authDomain: "ot-monitoring-tractor.firebaseapp.com",
    projectId: "ot-monitoring-tractor",
    storageBucket: "ot-monitoring-tractor.firebasestorage.app",
    messagingSenderId: "500229762814",
    appId: "1:500229762814:web:eb7a32a4c156f46ecabfaf",
    measurementId: "G-RCCFG0JGQN"
};

const app = initializeApp(firebaseConfig);

// Cloud Functions (2.14.0): callable functions in Jakarta, loaded on first use.
let _functionsP = null;
function _functions() {
    if (!_functionsP) _functionsP = import("https://www.gstatic.com/firebasejs/12.12.0/firebase-functions.js")
        .then(mod => ({ mod, f: mod.getFunctions(app, 'asia-southeast2') }))
        .catch(err => { _functionsP = null; throw err; });
    return _functionsP;
}

// Push notifications (2.18.0, Firebase Cloud Messaging). The public key of the
// project's Web Push certificate (Firebase console → Project settings → Cloud
// Messaging → Web Push certificates). Empty = push is not offered.
const PUSH_VAPID_KEY = 'BMYHau7f2Rn4PWCoWAUTLJTB1wWupKp11Ifuj7_gMyCi5LTg2s0J1sh2hEHpZpjcSZzq2Ov6jsNOd7ognmqNN5E';   // public Web Push key
let _messagingP = null;
function _messaging() {
    if (!_messagingP) _messagingP = import("https://www.gstatic.com/firebasejs/12.12.0/firebase-messaging.js")
        .then(async mod => ({ mod, ok: await mod.isSupported().catch(() => false) }))
        .then(x => ({ ...x, m: x.ok ? x.mod.getMessaging(app) : null }))
        .catch(err => { _messagingP = null; throw err; });
    return _messagingP;
}
const PUSH_TOKENS_COL = 'pushTokens';

// Cloud Storage (2.13.0, Blaze): loaded on first use only.
let _storageP = null;
function _storage() {
    if (!_storageP) _storageP = import("https://www.gstatic.com/firebasejs/12.12.0/firebase-storage.js")
        .then(mod => ({ mod, s: mod.getStorage(app) }))
        .catch(err => { _storageP = null; throw err; });
    return _storageP;
}

// ---- Photos in Cloud Storage (2.17.0) ----
// Photo documents (warehousePhotos, damagePhotos, workLogPhotos, …) keep their
// shape, but each image is uploaded to Storage under photos/<collection>/<doc>/
// and the document stores "sp:<path>" instead of the base64 picture. Reading
// gives a download URL. When Storage cannot be reached (offline, rules not yet
// published) the image is kept inline as before, so saving never fails over it.
// A document read with inline images is moved to Storage on the way (by
// whoever may write it; for others the move is simply refused).
const PHOTO_REF = 'sp:';
const _photoUrls = new Map();     // download URL -> "sp:" ref, so a photo read and saved back is not uploaded again
const _isImgData = v => typeof v === 'string' && /^data:image\//.test(v);
const _withTimeout = (p, ms) => Promise.race([p, new Promise((_, rej) => setTimeout(() => rej(new Error('timeout')), ms))]);
const PHOTO_PENDING = 'pending:';   // 2.29.5: still on the sender's device (photo outbox)
async function _photoUp(col, docId, v) {
    if (typeof v !== 'string' || !v || v.startsWith(PHOTO_REF) || v.startsWith(PHOTO_PENDING)) return v;
    if (_photoUrls.has(v)) {
        const ref = _photoUrls.get(v);
        if (_ownPhotoRef(col, docId, ref)) return ref;
        // 2.45.0: another document's photo gets its own copy here — sharing its Storage object
        // blanked this one when that document's photo was replaced or deleted, and showed
        // nothing to whoever may read this document but not that one.
        try { v = await _withTimeout(_urlToData(v), 60000); }
        catch (err) { console.warn('[photos] another document\'s photo kept as it is:', err && (err.code || err.message)); return ref; }
    }
    if (!_isImgData(v)) return v;
    if (typeof navigator !== 'undefined' && navigator.onLine === false) return v;
    try {
        const st = await _storage();
        const name = `${Date.now().toString(36)}${Math.random().toString(36).slice(2, 8)}`;
        const path = `photos/${col}/${String(docId).replace(/[^\w-]/g, '_')}/${name}`;
        await _withTimeout(st.mod.uploadString(st.mod.ref(st.s, path), v, 'data_url'), 120000);
        _uploadThumb(st, path, v);   // 2.35.0: best effort, never waited for
        return PHOTO_REF + path;
    } catch (err) {
        console.warn('[photos] kept inline — Storage upload failed:', err && (err.code || err.message));
        return v;
    }
}
// ---- Thumbnails (2.35.0) ----
// A small copy of each photo sits next to it as <file>_t (same folder, same
// storage.rules). Made here at upload; the server makes the ones still missing
// every night (functions/thumbs.js). Lists and galleries show the small copy
// and open the full photo only when it is tapped.
const THUMB_SUFFIX = '_t', THUMB_MAX = 360;
function _thumbData(dataUrl) {
    return new Promise(resolve => {
        const img = new Image();
        img.onload = () => {
            try {
                const k = Math.min(1, THUMB_MAX / Math.max(img.naturalWidth || 1, img.naturalHeight || 1));
                const c = document.createElement('canvas');
                c.width = Math.max(1, Math.round(img.naturalWidth * k)); c.height = Math.max(1, Math.round(img.naturalHeight * k));
                c.getContext('2d').drawImage(img, 0, 0, c.width, c.height);
                resolve(c.toDataURL('image/jpeg', 0.7));
            } catch (_) { resolve(''); }
        };
        img.onerror = () => resolve('');
        img.src = dataUrl;
    });
}
async function _uploadThumb(st, path, dataUrl) {
    try {
        if (typeof document === 'undefined') return;
        const t = await _thumbData(dataUrl);
        if (t) await _withTimeout(st.mod.uploadString(st.mod.ref(st.s, path + THUMB_SUFFIX), t, 'data_url'), 60000);
    } catch (err) { console.warn('[photos] thumbnail not uploaded:', err && (err.code || err.message)); }
}
const _thumbUrls = new Map();     // "sp:" ref -> Promise of its thumbnail URL ('' = none yet)
function _thumbUrlOf(url) {
    const ref = _photoUrls.get(url);
    if (!ref) return Promise.resolve('');
    if (!_thumbUrls.has(ref)) _thumbUrls.set(ref, (async () => {
        try {
            const st = await _storage();
            return await st.mod.getDownloadURL(st.mod.ref(st.s, ref.slice(PHOTO_REF.length) + THUMB_SUFFIX));
        } catch (err) {
            // Not made yet: asked again next session, not on every redraw.
            return '';
        }
    })());
    return _thumbUrls.get(ref);
}
// ---- Paths read from documents (2.43.0) ----
// A photo document is written by whoever may add photos, so the "sp:<path>" it
// holds is checked before this browser acts on it with its own rights:
//   · deleting: only a file in that document's own folder, photos/<col>/<doc>/
//     (a planted path used to make the owner's browser delete any file — a
//     Learning Center book, another record's photo);
//   · reading: only a record photo, photos/<col>/<record>/<file> (a damage record
//     shows the inspection photo it was copied from), never the rest of the bucket
//     (Learning files, backups, archives).
const _photoDir = docId => String(docId).replace(/[^\w-]/g, '_');
const _PHOTO_PATH = /^photos\/([A-Za-z]+)\/([A-Za-z0-9_-]+)\/([A-Za-z0-9_-]+)$/;
function _photoRefShape(r) { return typeof r === 'string' && r.startsWith(PHOTO_REF) && _PHOTO_PATH.test(r.slice(PHOTO_REF.length)); }
function _ownPhotoRef(col, docId, r) {
    const m = typeof r === 'string' && r.startsWith(PHOTO_REF) ? _PHOTO_PATH.exec(r.slice(PHOTO_REF.length)) : null;
    return !!m && m[1] === col && m[2] === _photoDir(docId);
}
// 2.44.0: a 3D model's file or card picture, models3d/<m3_…>/<file>.
function _m3PathOk(path) {
    const m = /^models3d\/(m3_[a-z0-9]+)\/([A-Za-z0-9_.-]+)$/.exec(typeof path === 'string' ? path : '');
    return !!m && !/^\.+$/.test(m[2]);
}
function _learnFileOk(path) {
    const m = /^learning\/([A-Za-z0-9_-]+)\/([A-Za-z0-9_.-]+)$/.exec(typeof path === 'string' ? path : '');
    return !!m && !/^\.+$/.test(m[2]);
}
// A job attachment: projectFiles/<job>/<file>, of that job when it is known.
function _jobFileOk(path, jobId) {
    const m = /^projectFiles\/([A-Za-z0-9_-]+)\/([A-Za-z0-9_.-]+)$/.exec(typeof path === 'string' ? path : '');
    return !!m && !/^\.+$/.test(m[2]) && (jobId == null || jobId === '' || m[1] === String(jobId));
}
async function _urlToData(url) {
    const blob = await (await fetch(url)).blob();
    return new Promise((res, rej) => { const r = new FileReader(); r.onload = () => res(r.result); r.onerror = rej; r.readAsDataURL(blob); });
}
async function _photoDown(v, inline) {
    if (typeof v === 'string' && v.startsWith(PHOTO_PENDING)) return '';
    if (typeof v !== 'string' || !v.startsWith(PHOTO_REF)) return v;
    if (!_photoRefShape(v)) { console.warn('[photos] not a photo path, not loaded:', v.slice(0, 80)); return ''; }
    try {
        const st = await _storage();
        // 2.45.1: the time limits are for the picture itself (reports, exports, copies). A form's
        // read keeps the SDK's own retries: '' there would look like no photo, and the next save
        // would remove the stored one.
        const urlP = st.mod.getDownloadURL(st.mod.ref(st.s, v.slice(PHOTO_REF.length)));
        const url = await (inline ? _withTimeout(urlP, 30000) : urlP);
        _photoUrls.set(url, v);
        _photoFailed.delete(v);
        if (!inline) return url;
        return await _withTimeout(_urlToData(url), 60000);   // 2.45.0: a weak signal no longer waits forever
    } catch (err) {
        console.warn('[photos] not loaded from Storage:', err && (err.code || err.message));
        if (!inline) _photoFailed.add(v);   // 2.45.2: a form read shows a stand-in for it (_failedView)
        return '';
    }
}
const _photoFailed = new Set();   // "sp:" refs a form read could not load this session
// The images inside a stored value: a string, a list, or a { key: image } map.
async function _photoMap(v, fn) {
    if (typeof v === 'string') return fn(v);
    if (Array.isArray(v)) return Promise.all(v.map(x => fn(x)));
    if (v && typeof v === 'object') {
        const out = {};
        await Promise.all(Object.keys(v).map(async k => { out[k] = await fn(v[k]); }));
        return out;
    }
    return v;
}
const _photoRefs = v => (typeof v === 'string' ? [v] : Array.isArray(v) ? v : v && typeof v === 'object' ? Object.values(v) : [])
    .filter(x => typeof x === 'string' && x.startsWith(PHOTO_REF));
const _hasInline = v => (typeof v === 'string' ? [v] : Array.isArray(v) ? v : v && typeof v === 'object' ? Object.values(v) : []).some(_isImgData);
async function _dropPhotoRefs(col, docId, list) {
    const refs = (list || []).filter(r => _ownPhotoRef(col, docId, r));
    if (refs.length < (list || []).length) console.warn(`[photos] ${(list || []).length - refs.length} path(s) outside ${col}/${docId} left in place`);
    if (!refs.length) return;
    try {
        const st = await _storage();
        await Promise.all(refs.flatMap(r => [r.slice(PHOTO_REF.length), r.slice(PHOTO_REF.length) + THUMB_SUFFIX])
            .map(path => st.mod.deleteObject(st.mod.ref(st.s, path)).catch(() => {})));
    } catch (_) { /* best effort */ }
}
// The same as _photoMap, at most `limit` at a time: eight photos racing up a
// weak field signal together all time out; two at a time get through.
async function _photoMapLimited(v, fn, limit) {
    if (typeof v === 'string') return fn(v);
    const keys = Array.isArray(v) ? v.map((_, i) => i) : (v && typeof v === 'object' ? Object.keys(v) : null);
    if (!keys) return v;
    const out = Array.isArray(v) ? new Array(v.length) : {};
    let next = 0;
    const worker = async () => { while (next < keys.length) { const k = keys[next++]; out[k] = await fn(v[k]); } };
    await Promise.all(Array.from({ length: Math.min(limit || 2, keys.length) }, worker));
    return out;
}

// ---- Photo outbox (2.29.5) ----
// A photo that could not reach Storage used to be written into its document
// inline. Several of those push a document past Firestore's 1 MB limit, the
// write is refused, and the photos are gone — a report then counts 8 photos
// and holds 1. Now such a photo stays on this device, in IndexedDB, and the
// document gets a "pending:<token>" in its place (so a paddock's photos keep
// their positions). The outbox is retried when the app opens, when the signal
// comes back and every minute; each upload then replaces its token.
const _OUTBOX_DB = 'ot-photo-outbox';
const _OUTBOX_DAYS = 30;
function _outboxDb() {
    return new Promise((res, rej) => {
        if (typeof indexedDB === 'undefined') { rej(new Error('no indexedDB')); return; }
        const r = indexedDB.open(_OUTBOX_DB, 1);
        r.onupgradeneeded = () => r.result.createObjectStore('jobs', { keyPath: 'key' });
        r.onsuccess = () => res(r.result);
        r.onerror = () => rej(r.error);
    });
}
async function _outbox(mode, fn) {
    const db = await _outboxDb();
    return new Promise((res, rej) => {
        const tx = db.transaction('jobs', mode);
        const req = fn(tx.objectStore('jobs'));
        tx.oncomplete = () => { db.close(); res(req && req.result); };
        tx.onerror = () => { db.close(); rej(tx.error); };
        // 2.43.2: a commit that fails (a full phone) aborts with no error event.
        tx.onabort = () => { db.close(); rej(tx.error || new Error('outbox transaction aborted')); };
    });
}
const _outboxDel = key => _outbox('readwrite', st => st.delete(key));
const _outboxAll = () => _outbox('readonly', st => st.getAll());
const _outboxGet = key => _outbox('readonly', st => st.get(key));
// 2.43.0: a job is changed by reading and writing it in one transaction, so a save
// and an upload running at the same time no longer overwrite each other's photos.
// edit(job or undefined) returns the job to keep, or null to remove it.
async function _outboxEdit(key, edit) {
    const db = await _outboxDb();
    return new Promise((res, rej) => {
        const tx = db.transaction('jobs', 'readwrite');
        const st = tx.objectStore('jobs');
        let out = null;
        const g = st.get(key);
        g.onsuccess = () => {
            out = edit(g.result);
            if (out && Object.keys(out.pend || {}).length) st.put(out); else { out = null; st.delete(key); }
        };
        tx.oncomplete = () => { db.close(); res(out); };
        tx.onerror = () => { db.close(); rej(tx.error); };
        tx.onabort = () => { db.close(); rej(tx.error || new Error('outbox transaction aborted')); };
    });
}
// The values held by a photo field: a string, a list, or a { key: image } map.
const _valuesOf = v => typeof v === 'string' ? [v] : Array.isArray(v) ? v : (v && typeof v === 'object' ? Object.values(v) : []);
const _isPending = x => typeof x === 'string' && x.startsWith(PHOTO_PENDING);
// What a form shows for a photo still waiting in an outbox, and the token it stands
// for: saved back, it is that same token again. It used to be read as '' — saved
// back, the waiting photo was gone (2.43.0).
// "col/doc/field" -> Map(shown value -> its tokens, in order): two identical
// pictures waiting in one report stay two photos.
const _pendingShown = new Map();
// The placeholders handed out: a reader (a report, a copy into another record, a
// cache) asks window.cloud.isWaitingPhoto and leaves them out (2.43.2).
const _placeholderSrc = new Set();
function _pendingPlaceholder(tok) {
    try {
        const c = document.createElement('canvas'); c.width = 320; c.height = 240;
        const g = c.getContext('2d');
        g.fillStyle = '#e5e7eb'; g.fillRect(0, 0, 320, 240);
        g.fillStyle = '#374151'; g.font = '16px sans-serif'; g.textAlign = 'center';
        g.fillText('Photo waiting to upload', 160, 112);
        g.fillText('from the phone that took it', 160, 134);
        g.fillStyle = '#9ca3af'; g.font = '10px sans-serif';
        g.fillText(tok.slice(PHOTO_PENDING.length), 160, 228);   // also keeps each placeholder unique
        return c.toDataURL('image/png');
    } catch (_) { return ''; }
}
// 2.45.2: a photo of a document that a form read could not load (Storage unreachable, retries
// spent) is shown as a stand-in, unique per photo, mapped back to its ref when the form saves.
// It used to be '' — the slot looked empty, and the next save removed the photo for good.
// Readers treat it like a waiting photo (isWaitingPhoto): no report, copy or cache uses it.
const _failedShown = new Map();   // "<col>/<doc>/<field>" -> Map(stand-in -> "sp:" ref)
function _failedView(key, ref) {
    let shown = '';
    try {
        const c = document.createElement('canvas'); c.width = 320; c.height = 240;
        const g = c.getContext('2d');
        g.fillStyle = '#fef3c7'; g.fillRect(0, 0, 320, 240);
        g.fillStyle = '#78350f'; g.font = '16px sans-serif'; g.textAlign = 'center';
        g.fillText('Photo could not be loaded', 160, 112);
        g.fillText('it is kept — open the report again later', 160, 134);
        g.fillStyle = '#b45309'; g.font = '10px sans-serif';
        g.fillText(ref.slice(-40), 160, 228);   // also keeps each stand-in unique
        shown = c.toDataURL('image/png');
    } catch (_) { return ''; }
    if (!shown) return '';
    _placeholderSrc.add(shown);
    let m = _failedShown.get(key);
    if (!m) _failedShown.set(key, m = new Map());
    m.set(shown, ref);
    return shown;
}
function _pendingView(key, tok, data, inline) {
    // A report (inline) uses the photo itself when this device holds it, else leaves it out.
    if (inline) return data || '';
    const shown = data || _pendingPlaceholder(tok) || tok;
    if (!data) _placeholderSrc.add(shown);
    let m = _pendingShown.get(key);
    if (!m) _pendingShown.set(key, m = new Map());
    const list = m.get(shown) || [];
    if (!list.includes(tok)) list.push(tok);
    m.set(shown, list);
    return shown;
}
const _photoToken = () => PHOTO_PENDING + Date.now().toString(36) + Math.random().toString(36).slice(2, 10);
const _mapValue = (v, fn) => typeof v === 'string' ? fn(v) : Array.isArray(v) ? v.map(x => fn(x))
    : (v && typeof v === 'object' ? Object.fromEntries(Object.entries(v).map(([k, x]) => [k, fn(x)])) : v);
// The same, with each value's place (list index or map key).
const _mapKeyed = (v, fn) => typeof v === 'string' ? fn(v, undefined) : Array.isArray(v) ? v.map((x, i) => fn(x, i))
    : (v && typeof v === 'object' ? Object.fromEntries(Object.entries(v).map(([k, x]) => [k, fn(x, k)])) : v);
// Waiting photos this browser has sent, token -> "sp:" ref: a form opened before
// the upload saves the upload, not the old token (2.43.0).
const _tokenDone = new Map();
// "col/doc/field" -> the field as last loaded for a form: where each waiting photo was.
const _loadedVals = new Map();
let _outboxBusy = false, _outboxTimer = null, _outboxAgain = false;
async function _flushPhotoOutbox() {
    // 2.43.2: asked again while busy (a new save): it runs again soon after.
    if (_outboxBusy) { _outboxAgain = true; return; }
    if (typeof navigator !== 'undefined' && navigator.onLine === false) return;
    _outboxBusy = true; _outboxAgain = false;
    let left = 0;
    // Only this device's cache answered (Firestore not reached): that copy may be older
    // than another device's save, so it is not written from — unless it holds this
    // device's own unsent write, which goes first anyway.
    const cacheOnly = s => s.metadata.fromCache && !s.metadata.hasPendingWrites;
    try {
        for (const job of (await _outboxAll()) || []) {
            if (job.uid && auth.currentUser && job.uid !== auth.currentUser.uid) { left++; continue; }   // another account's, on a shared phone
            if (Date.now() - (job.at || 0) > _OUTBOX_DAYS * 86400000) { await _outboxDel(job.key); continue; }
            try { if (cacheOnly(await getDoc(doc(db, job.col, job.docId)))) { left++; continue; } }
            catch (_) { left++; continue; }
            const refs = {};
            for (const [tok, data] of Object.entries(job.pend || {})) {
                const up = await _photoUp(job.col, job.docId, data);
                if (typeof up === 'string' && up.startsWith(PHOTO_REF)) refs[tok] = up;
            }
            if (Object.keys(refs).length) {
                const used = new Set();
                const ref = doc(db, job.col, job.docId);
                try {
                    // Read as this device sees it — with its own writes not sent yet — and
                    // updated behind them, in order. (A transaction reads the server only:
                    // a photo document still queued here looked missing, and its upload was
                    // thrown away as an orphan.) Nothing is awaited between the read and
                    // the update, so a save on this device cannot slip in between.
                    const snap = await getDoc(ref);
                    if (cacheOnly(snap)) throw Object.assign(new Error('only a cached copy'), { code: 'unavailable' });
                    if (snap.exists()) {
                        const next = _mapValue(snap.data()[job.field], x => (refs[x] ? (used.add(x), refs[x]) : x));
                        if (used.size) {
                            used.forEach(t => _tokenDone.set(t, refs[t]));
                            await updateDoc(ref, { [job.field]: next, updatedAt: Date.now() });
                        }
                    }
                } catch (err) {
                    // Locked by now (approved) or not ours: try again later, until the job expires.
                    Object.keys(refs).forEach(t => _tokenDone.delete(t));
                    console.warn('[photos] outbox: document not updated:', err && (err.code || err.message));
                    _dropPhotoRefs(job.col, job.docId, Object.values(refs));
                    left++;
                    continue;
                }
                // A token no longer in the document (photo deleted, record gone): its upload is an orphan.
                _dropPhotoRefs(job.col, job.docId, Object.entries(refs).filter(([t]) => !used.has(t)).map(([, r]) => r));
                // Only the photos sent here leave the job: a save may have added others meanwhile.
                const sent = new Set(Object.keys(refs));
                await _outboxEdit(job.key, cur => cur ? { ...cur, pend: Object.fromEntries(Object.entries(cur.pend || {}).filter(([t]) => !sent.has(t))) } : null);
            }
            const rest = await _outboxGet(job.key);
            if (rest && Object.keys(rest.pend || {}).length) left++;
        }
    } catch (err) {
        console.warn('[photos] outbox not flushed:', err && (err.message || err));
    } finally {
        _outboxBusy = false;
    }
    if ((left || _outboxAgain) && !_outboxTimer) _outboxTimer = setTimeout(() => { _outboxTimer = null; _flushPhotoOutbox(); }, _outboxAgain ? 5000 : 60000);
    return left;
}
if (typeof window !== 'undefined') window.addEventListener('online', () => setTimeout(_flushPhotoOutbox, 3000));

// Saves { ...fields, [field]: images } at col/docId, images through Storage.
// Images the document no longer holds are removed from Storage afterwards.
// opts.keepInline: a photo that cannot be uploaded stays inline (the read-time
// migration of an old document — never a reason to lose what it holds).
async function _savePhotoDoc(col, docId, field, value, fields, opts) {
    const key = `${col}/${docId}/${field}`;
    let waiting = {};   // photos of this document still on this device
    try { const job = await _outboxGet(key); waiting = (job && job.pend) || {}; } catch (_) { /* no outbox here */ }
    const pend = {};
    const shownMap = _pendingShown.get(key), usedTok = new Set();
    const failedMap = _failedShown.get(key);
    let stored = await _photoMapLimited(value, async x => {
        // 2.45.2: a photo the form could not load (shown as a stand-in) stays that same photo, in
        // its place. Removed in the form, it is removed.
        if (typeof x === 'string' && failedMap && failedMap.has(x)) return failedMap.get(x);
        // 2.43.0: a photo still waiting (as the form showed it) stays that same waiting photo.
        if (typeof x === 'string' && shownMap && shownMap.has(x)) {
            const t = shownMap.get(x).find(c => !usedTok.has(c));
            if (t) { usedTok.add(t); x = t; }
        }
        if (_isPending(x)) return x;
        if (typeof x === 'string' && _placeholderSrc.has(x)) return '';   // another document's placeholder is no photo
        const up = await _photoUp(col, docId, x);
        if (_isImgData(up) && !(opts && opts.keepInline)) { const tok = _photoToken(); pend[tok] = up; return tok; }
        return up;
    }, 2);
    // 2.43.2: the document is read only now, after the uploads (they take seconds,
    // and another device may send a waiting photo meanwhile) — just before writing.
    let latest;
    try { const cur = await getDoc(doc(db, col, docId)); if (cur.exists()) latest = cur.data()[field]; } catch (_) { /* new or unreadable */ }
    // The outbox keeps the waiting photos the form still holds, plus the new ones —
    // added to the job, never replacing it (it used to: every photo still waiting
    // from before was lost).
    const tokens = new Set(_valuesOf(stored).filter(_isPending));
    if (Object.keys(pend).length || Object.keys(waiting).length) {
        try {
            const me = auth.currentUser ? auth.currentUser.uid : '';
            await _outboxEdit(key, cur => ({ key, col, docId, field, uid: Object.keys(pend).length ? me : ((cur && cur.uid) || me), at: Date.now(),
                pend: { ...Object.fromEntries(Object.entries((cur && cur.pend) || {}).filter(([t]) => tokens.has(t))), ...pend } }));
            if (Object.keys(pend).length && !_outboxTimer) _outboxTimer = setTimeout(() => { _outboxTimer = null; _flushPhotoOutbox(); }, 30000);
        } catch (err) {
            if (Object.keys(pend).length) {
                // No outbox on this device: keep them inline, as before.
                console.warn('[photos] outbox unavailable, kept inline:', err && err.message);
                stored = _mapValue(stored, x => (pend[x] || x));
            }
        }
    }
    // ---- From here to the write nothing is awaited. ----
    // Sent by this browser meanwhile (its upload replaced the token in the document):
    // saved as the upload, not as the old token.
    stored = _mapValue(stored, x => (_isPending(x) && _tokenDone.has(x) ? _tokenDone.get(x) : x));
    const now = new Set(_valuesOf(latest)), before = _photoRefs(latest);
    // Against what the form loaded (when one did): where each waiting photo was, and
    // which photos are new since — another device's.
    const loaded = _loadedVals.get(key);
    const seen = loaded === undefined ? null : new Set(_valuesOf(loaded));
    let dropped = new Set();
    if (seen) {
        const placeOf = t => typeof loaded === 'string' ? (loaded === t ? '' : null)
            : Array.isArray(loaded) ? (loaded.indexOf(t) >= 0 ? loaded.indexOf(t) : null)
            : (loaded && typeof loaded === 'object' ? (Object.keys(loaded).find(k => loaded[k] === t) ?? null) : null);
        const there = t => { const at = placeOf(t); return at === null ? undefined : at === '' ? latest : (latest && typeof latest === 'object' ? latest[at] : undefined); };
        const fresh = r => typeof r === 'string' && r.startsWith(PHOTO_REF) && !seen.has(r);
        const kept = new Set(_valuesOf(stored));
        // A waiting photo the form had and the user removed: its upload is not brought back.
        dropped = new Set(_valuesOf(loaded).filter(t => _isPending(t) && !kept.has(t) && !kept.has(_tokenDone.get(t))).map(there).filter(fresh));
        const have = new Set(kept);
        // A waiting photo another device has sent meanwhile: its upload, in its place (where
        // the token was when the form loaded — a photo removed before it moves it).
        stored = _mapKeyed(stored, x => {
            if (!_isPending(x) || now.has(x) || waiting[x] || pend[x]) return x;
            const r = there(x);
            if (fresh(r) && !have.has(r) && !dropped.has(r)) { have.add(r); return r; }
            return x;
        });
        // A photo another device added since the form opened stays (a list keeps it at the end).
        if (Array.isArray(stored) && Array.isArray(latest)) stored = stored.concat(latest.filter(r => fresh(r) && !have.has(r) && !dropped.has(r)));
    }
    await setDoc(doc(db, col, docId), { ...fields, [field]: stored, updatedAt: Date.now() });
    _loadedVals.set(key, stored);   // what this browser now knows the document holds
    // Removed from Storage: what the document held and no longer holds — of the photos
    // the form had seen, or a waiting one it dropped. Another device's new photo never.
    const keep = new Set(_photoRefs(stored));
    _dropPhotoRefs(col, docId, before.filter(r => !keep.has(r) && (!seen || seen.has(r) || dropped.has(r))));
}
async function _getPhotoDoc(col, docId, field, empty, opts, fieldsOf) {
    const snap = await getDoc(doc(db, col, docId));
    if (!snap.exists()) return empty;
    const data = snap.data();
    const v = data[field];
    const ok = typeof empty === 'string' ? typeof v === 'string' : Array.isArray(empty) ? Array.isArray(v) : (v && typeof v === 'object' && !Array.isArray(v));
    if (!ok) return empty;
    // Moved to Storage when read, by whoever may write it (quietly refused otherwise).
    if (_hasInline(v) && !(opts && opts.noMigrate) && !(typeof navigator !== 'undefined' && navigator.onLine === false)) {
        _savePhotoDoc(col, docId, field, v, fieldsOf ? fieldsOf(data) : { id: data.id || docId }, { keepInline: true }).catch(() => {});
    }
    // 2.43.0: a photo still waiting to upload shows as itself when this device holds
    // it (else as a placeholder), and saved back it is the same waiting photo.
    let waiting = {};
    if (_valuesOf(v).some(_isPending)) {
        try { const job = await _outboxGet(`${col}/${docId}/${field}`); waiting = (job && job.pend) || {}; } catch (_) { /* no outbox here */ }
    }
    const inline = opts && opts.inline, key = `${col}/${docId}/${field}`;
    if (!inline) _loadedVals.set(key, v);   // what a form opened with (see _savePhotoDoc)
    // 2.45.0: opts.keys — only these photos of a { key: image } map (the rest are not downloaded).
    const pick = opts && Array.isArray(opts.keys) && v && typeof v === 'object' && !Array.isArray(v)
        ? Object.fromEntries(Object.entries(v).filter(([k]) => opts.keys.includes(k))) : v;
    // 2.45.1: a report's or a check's photo is shown only from its own folder. A photo document
    // naming another folder (or a link to some object) showed a picture its writer could still
    // replace after approval. Pictures kept inline and waiting photos are its own.
    const strict = _OWN_FOLDER_ONLY.has(col);
    const own = x => !strict || typeof x !== 'string' || !x || _isPending(x) || _isImgData(x)
        || (x.startsWith(PHOTO_REF) && _ownPhotoRef(col, docId, x));
    const one = async x => {
        if (!own(x)) { console.warn(`[photos] not from ${col}/${docId}, not shown:`, String(x).slice(0, 80)); return ''; }
        if (_isPending(x)) return _tokenDone.has(x) ? _photoDown(_tokenDone.get(x), inline) : _pendingView(key, x, waiting[x], inline);
        const got = await _photoDown(x, inline);
        // 2.45.2: this document's own photo that a form read could not load: a stand-in, not ''.
        if (!got && !inline && typeof x === 'string' && _photoFailed.has(x) && _ownPhotoRef(col, docId, x)) return _failedView(key, x);
        return got;
    };
    // 2.45.1: pictures fetched whole come down two at a time (a weak link shared by four at once
    // timed them all out).
    return inline ? _photoMapLimited(pick, one, 2) : _photoMap(pick, one);
}
const _OWN_FOLDER_ONLY = new Set(['workLogPhotos', 'inspectionPhotos']);
// 2.26.1: a report whose photo document never landed (refused by the rules
// before 2.26.1, see firestore.rules → recordSentByMe) still has its images in
// Storage under photos/<col>/<doc>/. They are found there, the newest `expect`
// of them, and written back into the document — quietly refused for whoever
// may not write it.
async function _recoverPhotos(col, docId, field, opts) {
    try {
        const st = await _storage();
        if (!st.mod.listAll) return [];
        const res = await _withTimeout(st.mod.listAll(st.mod.ref(st.s, `photos/${col}/${String(docId).replace(/[^\w-]/g, '_')}`)), 20000);
        let items = (res.items || []).filter(i => !i.name.endsWith(THUMB_SUFFIX)).sort((a, b) => a.name.localeCompare(b.name));
        const expect = Math.floor(Number(opts && opts.expect) || 0);
        if (expect > 0 && items.length > expect) items = items.slice(-expect);
        if (!items.length) return [];
        const refs = items.map(i => PHOTO_REF + i.fullPath);
        setDoc(doc(db, col, docId), { id: docId, [field]: refs, updatedAt: Date.now() })
            .catch(err => console.warn('[photos] recovered, not written back:', err && (err.code || err.message)));
        console.info(`[photos] ${refs.length} photo(s) of ${col}/${docId} recovered from Storage`);
        // 2.45.4: like _getPhotoDoc — the form knows what it opened with, and a photo it could not
        // load is a stand-in kept in its place (a '' slot used to be saved and its object deleted).
        const inline = opts && opts.inline, key = `${col}/${docId}/${field}`;
        if (!inline) _loadedVals.set(key, refs);
        return Promise.all(refs.map(async r => {
            const got = await _photoDown(r, inline);
            if (!got && !inline && _photoFailed.has(r) && _ownPhotoRef(col, docId, r)) return _failedView(key, r);
            return got;
        }));
    } catch (err) {
        console.warn('[photos] nothing recovered from Storage:', err && (err.code || err.message));
        return [];
    }
}
async function _deletePhotoDoc(col, docId, field) {
    let refs = [];
    try { const old = await getDoc(doc(db, col, docId)); if (old.exists()) refs = _photoRefs(old.data()[field]); } catch (_) { /* gone */ }
    await deleteDoc(doc(db, col, docId));
    _dropPhotoRefs(col, docId, refs);
}

// App Check (2.11.2): every request carries a reCAPTCHA token proving it
// comes from this site, so the public config above cannot be reused from a
// script elsewhere to create accounts or burn the quota. Off while the key
// is empty. The key is a reCAPTCHA Enterprise ("Fraud Defense") key of the
// Google Cloud project, registered under Firebase console -> App Check with
// that same site key (2.11.4: Firebase offers only the Enterprise provider).
// Press "Enforce" in the console only once the metrics show nearly all
// requests verified.
const APP_CHECK_SITE_KEY = '6LdyDwsuAAAAABX7mliizaCC7z99eHVXRFzfxFpx';   // public reCAPTCHA Enterprise site key
if (APP_CHECK_SITE_KEY) {
    try {
        const { initializeAppCheck, ReCaptchaEnterpriseProvider } =
            await import("https://www.gstatic.com/firebasejs/12.12.0/firebase-app-check.js");
        initializeAppCheck(app, { provider: new ReCaptchaEnterpriseProvider(APP_CHECK_SITE_KEY), isTokenAutoRefreshEnabled: true });
        document.documentElement.classList.add('appcheck-on');
    } catch (err) {
        // Never block the app on it: unenforced, requests still go through.
        console.warn('[appcheck] not started:', err && err.message);
    }
}

// Offline cache. This is what decides whether reopening the app costs a full
// re-read of every collection or almost nothing: with a warm cache Firestore
// resumes each listener from where it left off and fetches only what changed.
//
// It used to use enableIndexedDbPersistence, which is deprecated and — by its
// own documentation — gives up entirely as soon as a second tab is open. An
// office admin with two tabs therefore had NO cache at all, and every reload
// re-read the whole database. persistentMultipleTabManager is the fix: the
// tabs share one cache instead of fighting over it.
//
// Feature-detected rather than assumed, and the outcome is recorded instead of
// swallowed, so a silent fallback to no-cache is visible in the console and to
// anyone who asks window.cloud.cacheMode.
let db = null;
let cacheMode = 'memory';
if (FS.initializeFirestore && FS.persistentLocalCache && FS.persistentMultipleTabManager) {
    try {
        db = FS.initializeFirestore(app, {
            localCache: FS.persistentLocalCache({
                tabManager: FS.persistentMultipleTabManager()
            })
        });
        cacheMode = 'persistent-multitab';
    } catch (e) {
        console.warn('[cloud] multi-tab cache unavailable:', e && e.code);
    }
}
if (!db) {
    db = getFirestore(app);
    console.warn('[cloud] running without a persistent cache — every reload re-reads everything');
}
console.log(`[cloud] offline cache: ${cacheMode}`);

const auth = getAuth(app);
// 2.29.5: photos left on this device go up once someone is signed in.
onAuthStateChanged(auth, u => { if (u) setTimeout(() => _flushPhotoOutbox(), 5000); });

// Owner allowlist — any account that signs up with one of these emails is
// auto-promoted to role=owner with status=active. Other emails start as
// role=viewer with status=pending and must be approved by an owner.
const OWNER_EMAILS = ['mhmmdfiqriarrasyid@gmail.com'];
// The owner is pinned by Firebase Auth UID — an e-mail address can be
// registered by anyone if the original account is ever deleted or renamed; a
// UID cannot be claimed. Must match isOwnerAccount() in firestore.rules.
const OWNER_UIDS = ['vWZZ3jz1LTdzrU5c5Q1Gzco8nh72'];

// Persist login across reloads / browser restarts (best-effort).
setPersistence(auth, browserLocalPersistence).catch(err => {
    console.warn('[auth] persistence not enabled:', err.code);
});

const UNITS_COL = 'units';
const IMPL_COL = 'implements';
const DAMAGE_COL = 'damageRecords';
const LICENSE_COL = 'licenseStock';
const USERS_COL = 'users';
const ROLES_COL = 'roles';
const HISTORY_COL = 'history';
const USER_CATEGORIES_COL = 'userCategories';
const MASTER_LISTS_COL = 'masterLists';
const PADDOCKS_COL = 'paddocks';         // 2.10.0: one paddock list per company
const LICENSE_REQ_COL = 'licenseRequirements';
const DAMAGE_COMPONENTS_COL = 'damageComponents';
const DEVICES_COL = 'devices';
const BAST_COL = 'bast', BAST_KEYS_COL = 'bastKeys', BAST_SEQ_COL = 'bastSeq';
// The key doc id of a unique BAST value — firestore.rules builds the same (bastKeyOf).
const _bastKey = (field, value) => `${field}_${String(value == null ? '' : value).trim().toUpperCase().replace(/[^A-Z0-9-]/g, '_')}`;
const PURCHASE_COL = 'purchaseRequests';
const LEARNING_COL = 'learningItems';        // 2.12.0: Learning Center catalogue
const LEARNING_FILES_COL = 'learningFiles';  // 2.12.0: uploaded PDFs, in pieces
const PURCHASE_PHOTOS_COL = 'purchasePhotos';
const PROJECTS_COL = 'projects';             // 2.29.0: Projects & Jobs
const OT_JOBS_COL = 'otJobs';
const PROJECT_PHOTOS_COL = 'projectPhotos';
const TICKETS_COL = 'tickets', TICKET_PHOTOS_COL = 'ticketPhotos', TICKET_STAFF_COL = 'ticketStaff';   // 2.30.0
const STOCK_ITEMS_COL = 'stockItems';
// Operational vehicles (company motorbikes) and their fuel log. A fuel
// record's receipt photo lives in fuelPhotos/{recordId}, never subscribed.
const VEHICLES_COL = 'vehicles';
const FUEL_LOGS_COL = 'fuelLogs';
const WH_PHOTOS_COL = 'warehousePhotos';
const FUEL_PHOTOS_COL = 'fuelPhotos';
const TEAM_MEMBERS_COL = 'teamMembers';
const SHIFTS_COL = 'shifts';
const WORK_LOGS_COL = 'workLogs';
// Field documentation lives in its own collection, one document per work
// log, and is deliberately never subscribed — see getWorkLogPhotos below.
const WORK_LOG_PHOTOS_COL = 'workLogPhotos';
// Leave / sick-day requests. A separate collection from workLogs on purpose:
// a leave record has a date RANGE and an approval of its own, and folding it
// into the work log would pollute every work-log figure — total hours, "who
// reported today", the CSV, the decision inbox.
const LEAVE_COL = 'leaveRequests';
// Same again for damage records — one document per record, never subscribed.
const DAMAGE_PHOTOS_COL = 'damagePhotos';
// damagePhotos/{recordId} is the photo of the damage ("Before");
// damagePhotos/{recordId}__after the photo after the repair.
const DAMAGE_AFTER_SUFFIX = '__after';
// Heavy-equipment checks: the schedule, one report per unit check (with its
// own approval), and that report's photos — one per component, kept apart
// and never subscribed, for the same reason as the photo collections above.
const INSPECTION_PLANS_COL = 'inspectionPlans';
const INSPECTIONS_COL = 'inspections';
const INSPECTION_PHOTOS_COL = 'inspectionPhotos';

function batchInChunks(items, fn, chunkSize = 400) {
    // Firestore allows up to 500 ops per batch; 400 is a safe cap.
    const promises = [];
    for (let i = 0; i < items.length; i += chunkSize) {
        const slice = items.slice(i, i + chunkSize);
        const batch = writeBatch(db);
        slice.forEach(item => fn(batch, item));
        promises.push(batch.commit());
    }
    return Promise.all(promises);
}

window.cloud = {
    isReady: true,
    // 'persistent-multitab' when reopening the app is cheap, 'memory' when
    // every reload re-reads everything. Worth checking before blaming the
    // network for a slow start.
    cacheMode,
    OWNER_EMAILS,

    // ---- Units ----
    async saveUnit(unit) {
        await setDoc(doc(db, UNITS_COL, unit.id), unit, { merge: true });
    },
    // Only the named fields of existing units: [{ id, field: value, … }].
    // A bulk tool (Clean Up Values, a master-list rename) used to send every
    // field of every unit it touched, so a phone working from an hours-old
    // copy put back status, licences and breakdown reasons other devices had
    // changed since. update, not set: a unit deleted on another device is
    // not brought back as a stub.
    async updateUnitsFields(items) {
        if (!items || !items.length) return;
        await batchInChunks(items, (batch, u) => { const { id, ...f } = u; batch.update(doc(db, UNITS_COL, id), f); });
    },
    async saveUnits(units) {
        if (!units.length) return;
        await batchInChunks(units, (batch, u) =>
            batch.set(doc(db, UNITS_COL, u.id), u, { merge: true })
        );
    },
    async deleteUnit(id) {
        await deleteDoc(doc(db, UNITS_COL, id));
    },
    async deleteUnits(ids) {
        if (!ids.length) return;
        await batchInChunks(ids, (batch, id) =>
            batch.delete(doc(db, UNITS_COL, id))
        );
    },
    // group: for an account limited to one unit group, only that group's
    // units — the only query firestore.rules lets such an account make.
    async getAllUnits(group) {
        const snap = await getDocs(groupQuery(UNITS_COL, group));
        return snap.docs.map(withDocId);
    },
    subscribeUnits(callback, errorCallback, group) {
        return onSnapshot(
            groupQuery(UNITS_COL, group),
            snap => callback(snap.docs.map(withDocId)),
            err => {
                console.error('[cloud] units subscription error:', err);
                if (errorCallback) errorCallback(err);
            }
        );
    },

    // ---- Implements ----
    async saveImplement(imp) {
        await setDoc(doc(db, IMPL_COL, imp.id), imp, { merge: true });
    },
    async saveImplements(items) {
        if (!items.length) return;
        await batchInChunks(items, (batch, i) =>
            batch.set(doc(db, IMPL_COL, i.id), i, { merge: true })
        );
    },
    async deleteImplement(id) {
        await deleteDoc(doc(db, IMPL_COL, id));
    },
    async getAllImplements() {
        const snap = await getDocs(collection(db, IMPL_COL));
        return snap.docs.map(withDocId);
    },
    subscribeImplements(callback, errorCallback) {
        return onSnapshot(
            collection(db, IMPL_COL),
            snap => callback(snap.docs.map(withDocId)),
            err => {
                console.error('[cloud] implements subscription error:', err);
                if (errorCallback) errorCallback(err);
            }
        );
    },

    // ---- Damage Records ----
    async saveDamage(rec) {
        await setDoc(doc(db, DAMAGE_COL, rec.id), rec, { merge: true });
    },
    async saveDamages(items) {
        if (!items.length) return;
        await batchInChunks(items, (batch, r) =>
            batch.set(doc(db, DAMAGE_COL, r.id), r, { merge: true })
        );
    },
    async deleteDamage(id) {
        await deleteDoc(doc(db, DAMAGE_COL, id));
    },
    async getAllDamages(group) {
        const snap = await getDocs(groupQuery(DAMAGE_COL, group));
        return snap.docs.map(withDocId);
    },
    // Only the named fields of existing records: [{ id, field: value, … }].
    async updateDamagesFields(items) {
        if (!items || !items.length) return;
        await batchInChunks(items, (batch, r) => { const { id, ...f } = r; batch.update(doc(db, DAMAGE_COL, id), f); });
    },
    subscribeDamages(callback, errorCallback, group) {
        return _onSnapshotWithPending(
            groupQuery(DAMAGE_COL, group),
            callback,
            err => {
                console.error('[cloud] damage subscription error:', err);
                if (errorCallback) errorCallback(err);
            }
        );
    },

    // ---- License Stock ----
    async saveLicense(rec) {
        await setDoc(doc(db, LICENSE_COL, rec.id), rec, { merge: true });
    },
    async saveLicenses(items) {
        if (!items.length) return;
        await batchInChunks(items, (batch, r) =>
            batch.set(doc(db, LICENSE_COL, r.id), r, { merge: true })
        );
    },
    async deleteLicense(id) {
        await deleteDoc(doc(db, LICENSE_COL, id));
    },
    async getAllLicenses() {
        const snap = await getDocs(collection(db, LICENSE_COL));
        return snap.docs.map(withDocId);
    },
    subscribeLicenses(callback, errorCallback) {
        return onSnapshot(
            collection(db, LICENSE_COL),
            snap => callback(snap.docs.map(withDocId)),
            err => {
                console.error('[cloud] license subscription error:', err);
                if (errorCallback) errorCallback(err);
            }
        );
    },

    // ---- Auth ----
    isOwnerAccount(user) {
        return !!user && OWNER_UIDS.includes(user.uid);
    },
    isOwnerEmail(email) {
        return OWNER_EMAILS.includes((email || '').toLowerCase());
    },
    onAuthChange(callback) {
        return onAuthStateChanged(auth, callback);
    },
    getCurrentUser() {
        return auth.currentUser;
    },
    async signIn(email, password) {
        const cred = await signInWithEmailAndPassword(auth, email, password);
        return cred.user;
    },
    // Firebase sends the reset link itself; the app never sees the password.
    async sendPasswordReset(email) {
        await sendPasswordResetEmail(auth, email);
    },
    async signUp(email, password, displayName) {
        const cred = await createUserWithEmailAndPassword(auth, email, password);
        if (displayName) {
            try { await updateProfile(cred.user, { displayName }); } catch (e) { /* non-fatal */ }
        }
        return cred.user;
    },
    async signOutUser() {
        await signOut(auth);
    },

    // ---- User profile docs ----
    async getUserDoc(uid) {
        const snap = await getDoc(doc(db, USERS_COL, uid));
        return snap.exists() ? snap.data() : null;
    },
    async createUserDoc(user, displayName) {
        const isOwner = this.isOwnerAccount(user);
        const data = {
            uid: user.uid,
            email: user.email,
            displayName: displayName || user.displayName || (user.email || '').split('@')[0],
            role: isOwner ? 'owner' : 'viewer',
            status: isOwner ? 'active' : 'pending',
            createdAt: Date.now(),
            updatedAt: Date.now(),
            updatedBy: isOwner ? 'system' : null
        };
        await setDoc(doc(db, USERS_COL, user.uid), data, { merge: true });
        return data;
    },
    async ensureOwnerDoc(user) {
        // If an owner-allowlisted user signs in but their doc is missing or
        // wrong (e.g. they were created before the auth feature shipped),
        // fix it so they actually get owner privileges.
        if (!this.isOwnerAccount(user)) return null;
        const existing = await this.getUserDoc(user.uid);
        if (existing && existing.role === 'owner' && existing.status === 'active') return existing;
        const fixed = {
            uid: user.uid,
            email: user.email,
            displayName: (existing && existing.displayName) || user.displayName || (user.email || '').split('@')[0],
            role: 'owner',
            status: 'active',
            createdAt: (existing && existing.createdAt) || Date.now(),
            updatedAt: Date.now(),
            updatedBy: 'system'
        };
        await setDoc(doc(db, USERS_COL, user.uid), fixed, { merge: true });
        return fixed;
    },
    async updateUserRole(uid, role, status, updatedByEmail) {
        await setDoc(doc(db, USERS_COL, uid), {
            role,
            status,
            updatedAt: Date.now(),
            updatedBy: updatedByEmail || null
        }, { merge: true });
    },
    // ---- Single active session per account ----
    // Writes ONLY activeSession, because the security rules let a non-owner
    // touch that one field on their own document and nothing else.
    //
    // updateDoc, not setDoc with merge: the map is REPLACED, never merged
    // into. A merge kept every key already stored inside activeSession — the
    // old remote sign-out left revokedAt/revokedBy there — and the rules
    // accept only {id, startedAt, device}, so every later claim was refused
    // and the account was signed out again on every sign-in. Replacing the
    // map also heals a document already stuck that way.
    async claimSession(uid, session) {
        await updateDoc(doc(db, USERS_COL, uid), { activeSession: session });
    },
    // Owner-initiated remote sign-out: no device can ever hold this id, so
    // whoever is signed in is dropped on their next snapshot. Same three keys
    // as a claim (the Users page shows `device`), and replaced the same way.
    async revokeUserSession(uid, revokedByEmail) {
        await updateDoc(doc(db, USERS_COL, uid), {
            activeSession: {
                id: 'revoked_' + Date.now(),
                startedAt: Date.now(),
                device: ('Signed out by ' + (revokedByEmail || 'an admin')).slice(0, 80)
            }
        });
    },
    // Live view of one user's own document — session takeovers, and role or
    // access changes, reach the signed-in user without a reload.
    subscribeUserDoc(uid, callback, errorCallback) {
        return onSnapshot(
            doc(db, USERS_COL, uid),
            snap => callback(snap.exists() ? snap.data() : null),
            err => {
                console.error('[cloud] user doc subscription error:', err);
                if (errorCallback) errorCallback(err);
            }
        );
    },

    // extra: other fields written with it — the role a person holds
    // (roleId, roleName, v175). The access map is REPLACED, not merged, so a
    // level the role no longer grants does not linger.
    // The name shown for a person (2.10.3) — set by the owner.
    async updateUserName(uid, displayName, updatedByEmail) {
        await updateDoc(doc(db, USERS_COL, uid), {
            displayName,
            updatedAt: Date.now(),
            updatedBy: updatedByEmail || null
        });
    },
    async updateUserAccess(uid, access, updatedByEmail, extra) {
        await updateDoc(doc(db, USERS_COL, uid), {
            ...(extra || {}),
            access,
            updatedAt: Date.now(),
            updatedBy: updatedByEmail || null
        });
    },

    // ---- Roles (v175): named sets of access levels, owner only ----
    async saveRole(rec) {
        await setDoc(doc(db, ROLES_COL, rec.id), rec);
    },
    async deleteRole(id) {
        await deleteDoc(doc(db, ROLES_COL, id));
    },
    subscribeRoles(callback, errorCallback) {
        return onSnapshot(
            collection(db, ROLES_COL),
            snap => callback(snap.docs.map(withDocId)),
            err => {
                console.error('[cloud] roles subscription error:', err);
                if (errorCallback) errorCallback(err);
            }
        );
    },
    // For links into the Firebase console (2.11.5).
    projectId: firebaseConfig.projectId,
    async deleteUserDoc(uid) {
        await deleteDoc(doc(db, USERS_COL, uid));
    },
    subscribeUsers(callback, errorCallback) {
        return onSnapshot(
            collection(db, USERS_COL),
            snap => callback(snap.docs.map(d => ({ ...d.data(), uid: d.id }))),
            err => {
                console.error('[cloud] users subscription error:', err);
                if (errorCallback) errorCallback(err);
            }
        );
    },

    // ---- History (shared audit log) ----
    async addHistoryEvents(events) {
        if (!events || !events.length) return;
        await batchInChunks(events, (batch, e) =>
            batch.set(doc(db, HISTORY_COL, e.id), e)
        );
    },
    // groups: for a limited account, the rows of its group and the shared
    // ones (needs the composite index unitGroup + timestamp desc).
    subscribeHistory(callback, errorCallback, max = 500, groups) {
        const q = groups
            ? query(collection(db, HISTORY_COL), where('unitGroup', 'in', groups), orderBy('timestamp', 'desc'), limit(max))
            : query(collection(db, HISTORY_COL), orderBy('timestamp', 'desc'), limit(max));
        return onSnapshot(q,
            snap => callback(snap.docs.map(withDocId)),
            err => {
                console.error('[cloud] history subscription error:', err);
                if (errorCallback) errorCallback(err);
            }
        );
    },
    // The live subscription is capped so the dashboard stays light. Export
    // needs the whole audit trail, not just the newest page of it.
    async getAllHistory(groups) {
        const snap = await getDocs(groups
            ? query(collection(db, HISTORY_COL), where('unitGroup', 'in', groups), orderBy('timestamp', 'desc'))
            : query(collection(db, HISTORY_COL), orderBy('timestamp', 'desc')));
        return snap.docs.map(withDocId);
    },
    // The owner's one-time back-fill of the group on rows written before
    // v167: [{ id, unitGroup }]. The rules allow exactly that field, once.
    async updateHistoryFields(items) {
        if (!items || !items.length) return;
        await batchInChunks(items, (batch, r) => { const { id, ...f } = r; batch.update(doc(db, HISTORY_COL, id), f); });
    },
    // "Sign out & erase this device": the offline database goes too. The
    // instance is unusable afterwards, so the caller reloads the page.
    async eraseLocalCache() {
        if (typeof FS.terminate !== 'function' || typeof FS.clearIndexedDbPersistence !== 'function') return false;
        await FS.terminate(db);
        await FS.clearIndexedDbPersistence(db);
        return true;
    },
    // 2.45.0: true while this device holds writes the server has not
    // acknowledged — of ANY collection, so also those no list marks as waiting
    // (units, fuel, stock, deletes …). Not settled within `ms` counts as yes:
    // erasing on a guess would lose them.
    async hasPendingWrites(ms = 1500) {
        if (typeof FS.waitForPendingWrites !== 'function') return false;
        let timer;
        const settled = await Promise.race([
            FS.waitForPendingWrites(db).then(() => true, () => false),
            new Promise(r => { timer = setTimeout(() => r(false), ms); })]);
        clearTimeout(timer);
        return !settled;
    },
    async clearHistoryCloud() {
        const snap = await getDocs(collection(db, HISTORY_COL));
        if (snap.empty) return;
        await batchInChunks(snap.docs, (batch, d) => batch.delete(d.ref));
    },
    // Targeted removal, for entries describing a change the server refused.
    // clearHistoryCloud is all-or-nothing; this takes the ids the scanner
    // picked. Owner-only, same as clearHistoryCloud — firestore.rules gates
    // history deletes per document, so no rules change is needed for this.
    async deleteHistoryEvents(ids) {
        if (!ids || !ids.length) return;
        await batchInChunks(ids, (batch, id) => batch.delete(doc(db, HISTORY_COL, id)));
    },

    // ---- User categories (dynamic dropdown source) ----
    async saveUserCategory(cat) {
        await setDoc(doc(db, USER_CATEGORIES_COL, cat.id), cat, { merge: true });
    },
    async saveUserCategories(cats) {
        if (!cats.length) return;
        await batchInChunks(cats, (batch, c) =>
            batch.set(doc(db, USER_CATEGORIES_COL, c.id), c, { merge: true })
        );
    },
    async deleteUserCategory(id) {
        await deleteDoc(doc(db, USER_CATEGORIES_COL, id));
    },
    async getAllUserCategories() {
        const snap = await getDocs(collection(db, USER_CATEGORIES_COL));
        return snap.docs.map(withDocId);
    },
    subscribeUserCategories(callback, errorCallback) {
        return onSnapshot(
            collection(db, USER_CATEGORIES_COL),
            snap => callback(snap.docs.map(withDocId)),
            err => {
                console.error('[cloud] userCategories subscription error:', err);
                if (errorCallback) errorCallback(err);
            }
        );
    },

    // ---- Damage components (dynamic dropdown source) ----
    async saveDamageComponent(comp) {
        await setDoc(doc(db, DAMAGE_COMPONENTS_COL, comp.id), comp, { merge: true });
    },
    async saveDamageComponents(comps) {
        if (!comps.length) return;
        await batchInChunks(comps, (batch, c) =>
            batch.set(doc(db, DAMAGE_COMPONENTS_COL, c.id), c, { merge: true })
        );
    },
    async deleteDamageComponent(id) {
        await deleteDoc(doc(db, DAMAGE_COMPONENTS_COL, id));
    },
    async getAllDamageComponents() {
        const snap = await getDocs(collection(db, DAMAGE_COMPONENTS_COL));
        return snap.docs.map(withDocId);
    },
    subscribeDamageComponents(callback, errorCallback) {
        return onSnapshot(
            collection(db, DAMAGE_COMPONENTS_COL),
            snap => callback(snap.docs.map(withDocId)),
            err => {
                console.error('[cloud] damageComponents subscription error:', err);
                if (errorCallback) errorCallback(err);
            }
        );
    },

    // ---- Warehouse: devices tracked one by one, by serial number ----
    async saveDevice(rec) {
        await setDoc(doc(db, DEVICES_COL, rec.id), rec, { merge: true });
    },
    async saveDevices(items) {
        if (!items || !items.length) return;
        await batchInChunks(items, (batch, r) => batch.set(doc(db, DEVICES_COL, r.id), r, { merge: true }));
    },
    async deleteDevice(id) {
        await deleteDoc(doc(db, DEVICES_COL, id));
    },
    async getAllDevices() {
        const snap = await getDocs(collection(db, DEVICES_COL));
        return snap.docs.map(withDocId);
    },
    // ---- Learning Center (2.12.0) ----
    // A material: { id, title, category, description, source: 'upload'|'link',
    // url, file: { name, size, chunks, storage: 'firestore' } }. On the Spark
    // plan an uploaded PDF lives in Firestore as base64 pieces of <= ~700 KB,
    // one document each ("<itemId>__<n>"), fetched by id so no index is needed.
    // file.storage leaves room for 'storage' (Cloud Storage) on Blaze.
    async saveLearningItem(rec) {
        await setDoc(doc(db, LEARNING_COL, rec.id), rec);
    },
    async deleteLearningItem(id) {
        await deleteDoc(doc(db, LEARNING_COL, id));
    },
    subscribeLearningItems(callback, errorCallback) {
        return onSnapshot(
            collection(db, LEARNING_COL),
            snap => callback(snap.docs.map(withDocId)),
            err => {
                console.error('[cloud] learningItems subscription error:', err);
                if (errorCallback) errorCallback(err);
            }
        );
    },
    async saveLearningChunk(itemId, idx, data) {
        await setDoc(doc(db, LEARNING_FILES_COL, `${itemId}__${idx}`), { id: `${itemId}__${idx}`, itemId, idx, data });
    },
    async getLearningChunk(itemId, idx) {
        const snap = await getDoc(doc(db, LEARNING_FILES_COL, `${itemId}__${idx}`));
        return snap.exists() ? (snap.data().data || '') : null;
    },
    async deleteLearningChunks(itemId, count) {
        for (let i = 0; i < count; i++) await deleteDoc(doc(db, LEARNING_FILES_COL, `${itemId}__${i}`));
    },
    // ---- Sugarcane knowledge (2.22.0) ----
    async listAgriFacts() {
        const [facts, cfg] = await Promise.all([getDocs(collection(db, 'agriFacts')), getDoc(doc(db, 'settings', 'agri')).catch(() => null)]);
        const c = cfg && cfg.exists() ? cfg.data() : {};
        return { facts: facts.docs.map(d => ({ ...d.data(), id: d.id })).sort((a, b) => (a.createdAt || 0) - (b.createdAt || 0)),
                 builtIns: c.builtIns !== false, hidden: Array.isArray(c.hidden) ? c.hidden : [] };
    },
    async saveAgriFact(rec) { await setDoc(doc(db, 'agriFacts', rec.id), rec); },
    async deleteAgriFact(id) { await deleteDoc(doc(db, 'agriFacts', id)); },
    async saveAgriSettings(rec) {
        await setDoc(doc(db, 'settings', 'agri'), { builtIns: !!rec.builtIns, hidden: (rec.hidden || []).slice(0, 1000), updatedAt: Date.now() });
    },

    // ---- Learning Center: who opened what (2.19.0) ----
    // learningViews/{itemId}__{uid}: one document per person per material.
    async recordLearningView(itemId, who) {
        const uid = auth.currentUser && auth.currentUser.uid;
        if (!uid || !itemId) return;
        const ref = doc(db, 'learningViews', `${itemId}__${uid}`);
        let prev = null;
        try { const s = await getDoc(ref); prev = s.exists() ? s.data() : null; } catch (_) { /* first time */ }
        const now = Date.now();
        await setDoc(ref, { itemId, uid, name: (who && who.name) || '', email: (who && who.email) || '',
            firstAt: prev && prev.firstAt ? prev.firstAt : now, lastAt: now, count: ((prev && prev.count) || 0) + 1 });
    },
    async listLearningViews(itemId) {
        const snap = await getDocs(query(collection(db, 'learningViews'), where('itemId', '==', itemId)));
        return snap.docs.map(d => d.data()).sort((a, b) => (b.lastAt || 0) - (a.lastAt || 0));
    },

    // ---- History archive (2.19.0, owner): archives/history/YYYY-MM.json ----
    async listHistoryArchives() {
        const st = await _storage();
        const res = await st.mod.listAll(st.mod.ref(st.s, 'archives/history'));
        return res.items.map(r => ({ name: r.name, path: r.fullPath })).sort((a, b) => b.name.localeCompare(a.name));
    },
    async historyArchiveUrl(path) {
        const st = await _storage();
        return st.mod.getDownloadURL(st.mod.ref(st.s, path));
    },

    // ---- Error log (2.19.0) ----
    async logClientError(rec) {
        await setDoc(doc(collection(db, 'clientErrors')), rec);
    },
    async listClientErrors(n) {
        const snap = await getDocs(query(collection(db, 'clientErrors'), orderBy('at', 'desc'), limit(n || 200)));
        // The same message from the same version reads as one line, with a count.
        const byKey = new Map();
        snap.docs.forEach(d => {
            const r = { ...d.data(), id: d.id };
            const k = `${r.message}|${r.version}`;
            const cur = byKey.get(k);
            if (cur) cur.count++; else byKey.set(k, { ...r, count: 1 });
        });
        return [...byKey.values()];
    },
    async clearClientErrors() {
        const snap = await getDocs(query(collection(db, 'clientErrors'), limit(500)));
        await Promise.all(snap.docs.map(d => deleteDoc(d.ref)));
    },

    // ---- Push notifications on this device (2.18.0) ----
    // The device's FCM token is kept in pushTokens/{token} with the account's
    // uid; the server (notificationTick) sends to the tokens of the accounts
    // a notice is for. The service worker shows the notification.
    async pushAvailable() {
        if (!PUSH_VAPID_KEY || typeof Notification === 'undefined' || !('serviceWorker' in navigator)) return false;
        try { return (await _messaging()).ok; } catch (_) { return false; }
    },
    pushPermission() { return typeof Notification === 'undefined' ? 'unsupported' : Notification.permission; },
    async pushTokenHere() {
        const uid = auth.currentUser && auth.currentUser.uid;
        const t = localStorage.getItem('ot_push_token') || '';
        if (!uid || !t) return '';
        try {
            const s = await getDoc(doc(db, PUSH_TOKENS_COL, t));
            if (!s.exists() || s.data().uid !== uid) return '';
            // 2.45.0: the server keeps an account's 5 newest tokens (pushNotices.tokensByAccount);
            // a device in use renews its date once a day so it stays among them.
            const at = s.data().createdAt;
            if (!(typeof at === 'number' && at > Date.now() - 86400000)) updateDoc(doc(db, PUSH_TOKENS_COL, t), { createdAt: Date.now() }).catch(() => {});
            return t;
        } catch (_) { return ''; }
    },
    async enablePush() {
        const uid = auth.currentUser && auth.currentUser.uid;
        if (!uid) throw new Error('not signed in');
        const perm = await Notification.requestPermission();
        if (perm !== 'granted') { const e = new Error('permission ' + perm); e.code = 'push/' + perm; throw e; }
        const { mod, m } = await _messaging();
        const reg = await navigator.serviceWorker.ready;
        const token = await mod.getToken(m, { vapidKey: PUSH_VAPID_KEY, serviceWorkerRegistration: reg });
        if (!token) throw new Error('no token');
        await setDoc(doc(db, PUSH_TOKENS_COL, token), { token, uid, ua: (navigator.userAgent || '').slice(0, 160), createdAt: Date.now() });
        localStorage.setItem('ot_push_token', token);
        return token;
    },
    async disablePush() {
        const t = localStorage.getItem('ot_push_token') || '';
        localStorage.removeItem('ot_push_token');
        try { const { mod, m } = await _messaging(); if (m) await mod.deleteToken(m); } catch (_) { /* already gone */ }
        if (t) await deleteDoc(doc(db, PUSH_TOKENS_COL, t)).catch(() => {});
    },

    // ---- Server email (2.14.0) ----
    async callFunction(name, data, timeoutMs) {
        const f = await _functions();
        const res = await f.mod.httpsCallable(f.f, name, { timeout: timeoutMs || 60000 })(data || {});
        return res.data;
    },
    // 2.35.0: the small copy of a photo this session has a URL for ('' = none).
    photoThumbUrl(url) { return _thumbUrlOf(url); },
    // 2.35.0: one collection of a server backup, to download (owner only — storage.rules).
    async serverBackupUrl(date, name) {
        const st = await _storage();
        return st.mod.getDownloadURL(st.mod.ref(st.s, `backups/${date}/${name}.json.gz`));
    },
    async getEmailSettings() {
        const snap = await getDoc(doc(db, 'settings', 'email'));
        return snap.exists() ? snap.data() : {};
    },
    async saveEmailSettings(rec) {
        await setDoc(doc(db, 'settings', 'email'), rec);
    },
    // The newest mailLog entry of each kind (one small query, no index).
    async lastMails() {
        const snap = await getDocs(query(collection(db, 'mailLog'), orderBy('at', 'desc'), limit(40)));
        const out = {};
        snap.docs.forEach(x => {
            const d = x.data();
            if (d.kind && !out[d.kind]) out[d.kind] = { ...d, at: d.at && d.at.toMillis ? d.at.toMillis() : null };
        });
        return out;
    },
    // 2.13.0 (Blaze): files in Cloud Storage under learning/<fileId>/<name>.
    // The Storage SDK is imported on first use, so the app still starts when
    // it cannot be loaded. onProgress(fraction); resolves when stored.
    async uploadLearningFile(path, file, contentType, onProgress) {
        const st = await _storage();
        const task = st.mod.uploadBytesResumable(st.mod.ref(st.s, path), file, { contentType, cacheControl: 'private, max-age=86400' });
        return new Promise((resolve, reject) => {
            task.on('state_changed',
                snap => { if (onProgress && snap.totalBytes) onProgress(snap.bytesTransferred / snap.totalBytes); },
                reject,
                () => resolve());
        });
    },
    // 2.43.0: only a Learning Center file, learning/<fileId>/<name> — the path comes from the item.
    async learningFileUrl(path) {
        if (!_learnFileOk(path)) throw new Error('not a Learning Center file');
        const st = await _storage();
        return st.mod.getDownloadURL(st.mod.ref(st.s, path));
    },
    async deleteLearningFile(path) {
        if (!_learnFileOk(path)) { console.warn('[learning] not a Learning Center file, left in place:', String(path).slice(0, 80)); return; }
        const st = await _storage();
        try { await st.mod.deleteObject(st.mod.ref(st.s, path)); }
        catch (err) { if (!err || err.code !== 'storage/object-not-found') throw err; }
    },
    // ---- 3D Models (2.44.0) ----
    // One document per model in models3d/{id}; its file (and card picture) under
    // models3d/<id>/ in Storage. Only paths of that shape are read or deleted.
    subscribeModels3d(callback, errorCallback) {
        return onSnapshot(collection(db, 'models3d'), snap => callback(snap.docs.map(withDocId)),
            err => { console.warn('[cloud] models3d subscription error:', err && err.code); if (errorCallback) errorCallback(err); });
    },
    async saveModel3d(rec) { await setDoc(doc(db, 'models3d', rec.id), rec); },
    // 2.44.2: changes to an existing model — fails (not-found) once it was deleted,
    // instead of creating it again. Fields given are replaced whole (a map too).
    async updateModel3d(rec) { await updateDoc(doc(db, 'models3d', rec.id), rec); },
    // One part's notes only (null removes them), so two editors noting different parts
    // at the same time keep both.
    async updateModel3dPart(id, key, info, level) {
        if (!/^[A-Za-z0-9_-]{1,200}$/.test(String(key))) throw new Error('bad part key');
        const upd = { [`parts.${key}`]: info === null ? FS.deleteField() : info, updatedAt: Date.now() };
        if (level !== undefined) upd.level = level;
        await updateDoc(doc(db, 'models3d', id), upd);
    },
    async deleteModel3d(id) { await deleteDoc(doc(db, 'models3d', id)); },
    async uploadModel3dFile(path, data, contentType, onProgress) {
        if (!_m3PathOk(path)) throw new Error('not a 3D model path');
        const st = await _storage();
        const task = st.mod.uploadBytesResumable(st.mod.ref(st.s, path), data, { contentType, cacheControl: 'private, max-age=86400' });
        return new Promise((resolve, reject) => {
            task.on('state_changed',
                snap => { if (onProgress && snap.totalBytes) onProgress(snap.bytesTransferred / snap.totalBytes); },
                reject, () => resolve());
        });
    },
    async model3dFileUrl(path) {
        if (!_m3PathOk(path)) throw new Error('not a 3D model path');
        const st = await _storage();
        return st.mod.getDownloadURL(st.mod.ref(st.s, path));
    },
    async deleteModel3dFile(path) {
        if (!_m3PathOk(path)) { console.warn('[3d] not a 3D model path, left in place:', String(path).slice(0, 80)); return; }
        const st = await _storage();
        try { await st.mod.deleteObject(st.mod.ref(st.s, path)); }
        catch (err) { if (!err || err.code !== 'storage/object-not-found') throw err; }
    },
    // ---- Tickets (2.30.0) ----
    // reporterUid: only that account's own tickets (no 'view' on the area).
    async saveTicket(rec) { await setDoc(doc(db, TICKETS_COL, rec.id), rec); },
    async deleteTicket(id) { await deleteDoc(doc(db, TICKETS_COL, id)); },
    subscribeTickets(callback, errorCallback, reporterUid) {
        return _onSnapshotWithPending(
            reporterUid ? query(collection(db, TICKETS_COL), where('reportedByUid', '==', reporterUid)) : collection(db, TICKETS_COL),
            callback, err => { console.error('[cloud] tickets subscription error:', err); if (errorCallback) errorCallback(err); });
    },
    async getTicketPhoto(id, opts) {
        return _getPhotoDoc(TICKET_PHOTOS_COL, id, 'photo', '', opts, d => ({ id: d.id || id, ownerId: d.ownerId || '' }));
    },
    async saveTicketPhoto(id, ownerId, photo) { await _savePhotoDoc(TICKET_PHOTOS_COL, id, 'photo', photo, { id, ownerId }); },
    async deleteTicketPhoto(id) { await _deletePhotoDoc(TICKET_PHOTOS_COL, id, 'photo'); },
    async saveTicketStaff(card) { await setDoc(doc(db, TICKET_STAFF_COL, card.uid), card); },
    subscribeTicketStaff(callback, errorCallback) {
        return onSnapshot(collection(db, TICKET_STAFF_COL), snap => callback(snap.docs.map(withDocId)), err => { if (errorCallback) errorCallback(err); });
    },
    // 2.29.8: PDF attachments of jobs, under projectFiles/<record>/<file>.
    // 2.29.9: also videos (mp4 / mov / webm); contentType is the file's own.
    async uploadProjectFile(path, file, onProgress, contentType) {
        const st = await _storage();
        const task = st.mod.uploadBytesResumable(st.mod.ref(st.s, path), file, { contentType: contentType || 'application/pdf', cacheControl: 'private, max-age=86400' });
        return new Promise((resolve, reject) => {
            task.on('state_changed',
                snap => { if (onProgress && snap.totalBytes) onProgress(snap.bytesTransferred / snap.totalBytes); },
                reject, () => resolve());
        });
    },
    async projectFileUrl(path, jobId) {
        if (!_jobFileOk(path, jobId)) throw new Error('not an attachment of this job');   // 2.43.0
        const st = await _storage();
        return st.mod.getDownloadURL(st.mod.ref(st.s, path));
    },
    async deleteProjectFile(path, jobId) {
        if (!_jobFileOk(path, jobId)) { console.warn('[jobs] not an attachment of this job, left in place:', String(path).slice(0, 80)); return; }   // 2.43.0
        const st = await _storage();
        try { await st.mod.deleteObject(st.mod.ref(st.s, path)); }
        catch (err) { if (!err || err.code !== 'storage/object-not-found') throw err; }
    },
    // ---- Purchase requests (2.9.0) ----
    // authorUid: only that account's own requests (access level 'own').
    async savePurchaseRequest(rec) {
        await setDoc(doc(db, PURCHASE_COL, rec.id), rec);
    },
    async deletePurchaseRequest(id) {
        await deleteDoc(doc(db, PURCHASE_COL, id));
    },
    subscribePurchaseRequests(callback, errorCallback, authorUid) {
        return _onSnapshotWithPending(
            authorUid ? query(collection(db, PURCHASE_COL), where('createdByUid', '==', authorUid))
                      : collection(db, PURCHASE_COL),
            callback,
            err => {
                console.error('[cloud] purchaseRequests subscription error:', err);
                if (errorCallback) errorCallback(err);
            }
        );
    },
    // ---- Projects & Jobs (2.29.0) ----
    async saveProject(rec) { await setDoc(doc(db, PROJECTS_COL, rec.id), rec); },
    async deleteProject(id) { await deleteDoc(doc(db, PROJECTS_COL, id)); },
    subscribeProjects(callback, errorCallback) {
        return _onSnapshotWithPending(collection(db, PROJECTS_COL), callback, err => {
            console.error('[cloud] projects subscription error:', err);
            if (errorCallback) errorCallback(err);
        });
    },
    async saveOtJob(rec) { await setDoc(doc(db, OT_JOBS_COL, rec.id), rec); },
    async deleteOtJob(id) { await deleteDoc(doc(db, OT_JOBS_COL, id)); },
    subscribeOtJobs(callback, errorCallback) {
        return _onSnapshotWithPending(collection(db, OT_JOBS_COL), callback, err => {
            console.error('[cloud] otJobs subscription error:', err);
            if (errorCallback) errorCallback(err);
        });
    },
    async getProjectPhoto(id, opts) {
        return _getPhotoDoc(PROJECT_PHOTOS_COL, id, 'photo', '', opts, d => ({ id: d.id || id, ownerId: d.ownerId || '' }));
    },
    async saveProjectPhoto(id, ownerId, photo) {
        await _savePhotoDoc(PROJECT_PHOTOS_COL, id, 'photo', photo, { id, ownerId });
    },
    async deleteProjectPhoto(id) {
        await _deletePhotoDoc(PROJECT_PHOTOS_COL, id, 'photo');
    },
    async getPurchasePhoto(id, opts) {
        return _getPhotoDoc(PURCHASE_PHOTOS_COL, id, 'photo', '', opts, d => ({ id: d.id || id, ownerId: d.ownerId || '' }));
    },
    async savePurchasePhoto(id, ownerId, photo) {
        await _savePhotoDoc(PURCHASE_PHOTOS_COL, id, 'photo', photo, { id, ownerId });
    },
    async deletePurchasePhoto(id) {
        await _deletePhotoDoc(PURCHASE_PHOTOS_COL, id, 'photo');
    },

    subscribeDevices(callback, errorCallback) {
        return onSnapshot(
            collection(db, DEVICES_COL),
            snap => callback(snap.docs.map(withDocId)),
            err => {
                console.error('[cloud] devices subscription error:', err);
                if (errorCallback) errorCallback(err);
            }
        );
    },

    // ---- Warehouse: consumables counted by quantity (IN/OUT ledger) ----
    async saveStockItem(rec) {
        await setDoc(doc(db, STOCK_ITEMS_COL, rec.id), rec, { merge: true });
    },
    async saveStockItems(items) {
        if (!items || !items.length) return;
        await batchInChunks(items, (batch, r) => batch.set(doc(db, STOCK_ITEMS_COL, r.id), r, { merge: true }));
    },
    async deleteStockItem(id) {
        await deleteDoc(doc(db, STOCK_ITEMS_COL, id));
    },
    async getAllStockItems() {
        const snap = await getDocs(collection(db, STOCK_ITEMS_COL));
        return snap.docs.map(withDocId);
    },
    subscribeStockItems(callback, errorCallback) {
        return onSnapshot(
            query(collection(db, STOCK_ITEMS_COL), orderBy('date', 'desc')),
            snap => callback(snap.docs.map(withDocId)),
            err => {
                console.error('[cloud] stockItems subscription error:', err);
                if (errorCallback) errorCallback(err);
            }
        );
    },

    // ---- BAST — Berita Acara Serah Terima (2.46.0) ----
    // A letter's number and its unit's S/N (and, when given, Radio ID and battery S/N) are
    // unique: each has a key doc in bastKeys that is created once and never changed
    // (firestore.rules), written in the same transaction as the letter. The number comes
    // from a per PT + item code + year counter (bastSeq), skipping numbers already taken.
    subscribeBast(callback, errorCallback) {
        return onSnapshot(collection(db, BAST_COL), snap => callback(snap.docs.map(withDocId)), err => {
            console.error('[cloud] bast subscription error:', err);
            if (errorCallback) errorCallback(err);
        });
    },
    bastKey(field, value) { return _bastKey(field, value); },
    // 2.46.5: the owner removes cancelled letters (firestore.rules: delete is owner-only). Their
    // number keys stay in bastKeys, so a removed letter's number is still never used again.
    async deleteBasts(ids) {
        await batchInChunks(ids || [], (batch, id) => batch.delete(doc(db, BAST_COL, id)));
    },
    // 2.46.6: the owner resets the numbering — cancelled letters are removed, every key of a
    // letter that no longer exists is released, and each counter goes back to the highest number
    // a remaining letter holds (or away, so the next number is 001). A number still on a letter
    // is never released, so nothing can be numbered twice.
    async resetBastNumbers() {
        const letters = await getDocs(collection(db, BAST_COL));
        const cancelled = letters.docs.filter(d => d.data().status === 'void').map(d => d.id);
        await batchInChunks(cancelled, (batch, id) => batch.delete(doc(db, BAST_COL, id)));
        const live = letters.docs.filter(d => d.data().status !== 'void').map(d => d.data());
        const liveIds = new Set(letters.docs.filter(d => d.data().status !== 'void').map(d => d.id));
        const keys = await getDocs(collection(db, BAST_KEYS_COL));
        const drop = keys.docs.filter(k => !liveIds.has(k.data().bastId)).map(k => k.id);
        await batchInChunks(drop, (batch, id) => batch.delete(doc(db, BAST_KEYS_COL, id)));
        // 2.47.5: per series, the numbers still on a letter; the ones in between that nobody holds
        // (deleted letters) are kept as `free` and handed out first, lowest first.
        const used = new Map();
        live.forEach(b => {
            const y = String(b.date || '').slice(0, 4), m = /\/(\d+)$/.exec(String(b.no || ''));
            if (!m || !b.companyCode || !b.itemCode) return;
            const k = `${b.companyCode}_${b.itemCode}_${y}`;
            if (!used.has(k)) used.set(k, new Set());
            used.get(k).add(Number(m[1]));
        });
        const plan = new Map();
        used.forEach((nums, k) => {
            const last = Math.max(...nums), free = [];
            for (let n = 1; n < last && free.length < 500; n++) if (!nums.has(n)) free.push(n);
            plan.set(k, { last, free });
        });
        const seqs = await getDocs(collection(db, BAST_SEQ_COL));
        const ids = new Set([...seqs.docs.map(d => d.id), ...plan.keys()]);
        let gaps = 0;
        await batchInChunks([...ids], (batch, id) => {
            const p = plan.get(id), ref = doc(db, BAST_SEQ_COL, id);
            if (p) { gaps += p.free.length; batch.set(ref, { last: p.last, free: p.free, updatedAt: Date.now() }); }
            else batch.delete(ref);
        });
        return { cancelled: cancelled.length, released: drop.length, counters: ids.size, gaps };
    },
    // rec: the letter (rec.no '' = numbered here). opts: { prefix, seqKey } for the number;
    // device: { id, make(current, letter) → fields } to allocate the unit in Warehouse.
    async createBast(rec, opts) {
        const o = opts || {};
        return runTransaction(db, async tx => {
            const out = { ...rec };
            let seqRef = null, next = 0, seqLast = 0, free = [];
            if (!out.no) {
                seqRef = doc(db, BAST_SEQ_COL, o.seqKey);
                const s = await tx.get(seqRef);
                const sd = s.exists() ? s.data() : {};
                seqLast = Number(sd.last) || 0;
                // 2.47.5: numbers a reset left free (gaps) are given out first, lowest first.
                free = (Array.isArray(sd.free) ? sd.free : []).filter(n => Number.isInteger(n) && n > 0 && n <= seqLast).sort((a, b) => a - b);
                while (free.length && !out.no) {
                    const n = free.shift();
                    const cand = `${o.prefix}/${String(n).padStart(3, '0')}`;
                    if (!(await tx.get(doc(db, BAST_KEYS_COL, _bastKey('no', cand)))).exists()) out.no = cand;
                }
                const usedGap = !!out.no;
                next = usedGap ? seqLast : seqLast + 1;   // a gap was used: the counter stays where it is
                for (let i = 0; i < 60 && !out.no; i++, next++) {
                    const cand = `${o.prefix}/${String(next).padStart(3, '0')}`;
                    if (!(await tx.get(doc(db, BAST_KEYS_COL, _bastKey('no', cand)))).exists()) { out.no = cand; break; }
                }
                if (!out.no) throw Object.assign(new Error('no free number'), { code: 'bast/no-number' });
            } else {
                const k = await tx.get(doc(db, BAST_KEYS_COL, _bastKey('no', out.no)));
                if (k.exists()) throw Object.assign(new Error('number taken'), { code: 'bast/dup', field: 'no', other: k.data().bastId });
            }
            const uniq = [['sn', out.sn], ['rid', out.radioId], ['bat', out.batterySn]].filter(([, v]) => v);
            for (const [f, v] of uniq) {
                const k = await tx.get(doc(db, BAST_KEYS_COL, _bastKey(f, v)));
                if (k.exists()) throw Object.assign(new Error(f + ' taken'), { code: 'bast/dup', field: f, other: k.data().bastId });
            }
            let cur = null, devRef = null;
            if (o.device && o.device.id) { devRef = doc(db, DEVICES_COL, o.device.id); const ds = await tx.get(devRef); cur = ds.exists() ? ds.data() : null; }
            const at = Date.now();
            out.noKey = _bastKey('no', out.no);
            out.snKey = _bastKey('sn', out.sn);
            if (seqRef) tx.set(seqRef, { last: next, free, updatedAt: at });
            tx.set(doc(db, BAST_KEYS_COL, out.noKey), { bastId: out.id, field: 'no', value: out.no, at });
            uniq.forEach(([f, v]) => tx.set(doc(db, BAST_KEYS_COL, _bastKey(f, v)), { bastId: out.id, field: f, value: String(v), at }));
            tx.set(doc(db, BAST_COL, out.id), out);
            if (devRef) { const patch = o.device.make(cur, out); if (patch) tx.set(devRef, patch, { merge: true }); }
            return out;
        });
    },
    // Ends an active letter: info { status: 'returned' | 'void', date, reason, by, device(current, letter) → fields }.
    // Its S/N, Radio ID and battery keys are given back; the number's key stays.
    async endBast(id, info) {
        return runTransaction(db, async tx => {
            const ref = doc(db, BAST_COL, id);
            const s = await tx.get(ref);
            if (!s.exists()) throw Object.assign(new Error('not found'), { code: 'bast/not-found' });
            const b = s.data();
            if (b.status !== 'active') return b;
            const refs = [['sn', b.sn], ['rid', b.radioId], ['bat', b.batterySn]].filter(([, v]) => v).map(([f, v]) => doc(db, BAST_KEYS_COL, _bastKey(f, v)));
            const keys = [];
            for (const k of refs) keys.push([k, await tx.get(k)]);
            let cur = null, devRef = null;
            if (info.device && b.deviceId) { devRef = doc(db, DEVICES_COL, b.deviceId); const ds = await tx.get(devRef); cur = ds.exists() ? ds.data() : null; }
            const upd = { status: info.status, endedAt: info.date || '', endReason: info.reason || '', endedBy: info.by || '', updatedAt: Date.now(), ...(info.extra || {}) };
            tx.update(ref, upd);
            keys.forEach(([k, snap]) => { if (snap.exists() && snap.data().bastId === id) tx.delete(k); });
            if (devRef && cur) { const patch = info.device(cur, { ...b, ...upd }); if (patch) tx.set(devRef, patch, { merge: true }); }
            return { ...b, ...upd };
        });
    },

    // ---- Operational vehicles ----
    async saveVehicle(rec) {
        await setDoc(doc(db, VEHICLES_COL, rec.id), rec, { merge: true });
    },
    async saveVehicles(items) {
        if (!items || !items.length) return;
        await batchInChunks(items, (batch, r) => batch.set(doc(db, VEHICLES_COL, r.id), r, { merge: true }));
    },
    async deleteVehicle(id) {
        await deleteDoc(doc(db, VEHICLES_COL, id));
    },
    async getAllVehicles() {
        const snap = await getDocs(collection(db, VEHICLES_COL));
        return snap.docs.map(withDocId);
    },
    subscribeVehicles(callback, errorCallback) {
        return onSnapshot(
            collection(db, VEHICLES_COL),
            snap => callback(snap.docs.map(withDocId)),
            err => {
                console.error('[cloud] vehicles subscription error:', err);
                if (errorCallback) errorCallback(err);
            }
        );
    },

    // ---- Fuel log: one record per refuel (date, liters, odometer) ----
    async saveFuelLog(rec) {
        await setDoc(doc(db, FUEL_LOGS_COL, rec.id), rec, { merge: true });
    },
    async saveFuelLogs(items) {
        if (!items || !items.length) return;
        await batchInChunks(items, (batch, r) => batch.set(doc(db, FUEL_LOGS_COL, r.id), r, { merge: true }));
    },
    async deleteFuelLog(id) {
        await deleteDoc(doc(db, FUEL_LOGS_COL, id));
    },
    async getAllFuelLogs() {
        const snap = await getDocs(collection(db, FUEL_LOGS_COL));
        return snap.docs.map(withDocId);
    },
    subscribeFuelLogs(callback, errorCallback) {
        return onSnapshot(
            collection(db, FUEL_LOGS_COL),
            snap => callback(snap.docs.map(withDocId)),
            err => {
                console.error('[cloud] fuelLogs subscription error:', err);
                if (errorCallback) errorCallback(err);
            }
        );
    },
    // Receipt photo of a fuel record, one document per record, read on demand.
    // ---- Warehouse photos (v185): one document per photo, for devices and
    // stock movements, so ten photos never meet the 1 MB document limit.
    async getWhPhoto(id, opts) {
        return _getPhotoDoc(WH_PHOTOS_COL, id, 'photo', '', opts, d => ({ id: d.id || id, ownerId: d.ownerId || '' }));
    },
    async saveWhPhoto(id, ownerId, photo) {
        await _savePhotoDoc(WH_PHOTOS_COL, id, 'photo', photo, { id, ownerId });
    },
    async deleteWhPhoto(id) {
        await _deletePhotoDoc(WH_PHOTOS_COL, id, 'photo');
    },
    async getFuelPhoto(id, opts) {
        return _getPhotoDoc(FUEL_PHOTOS_COL, id, 'photo', '', opts);
    },
    async saveFuelPhoto(id, photo) {
        await _savePhotoDoc(FUEL_PHOTOS_COL, id, 'photo', photo, { id });
    },
    async deleteFuelPhoto(id) {
        await _deletePhotoDoc(FUEL_PHOTOS_COL, id, 'photo');
    },

    // ---- Team members (people, including those without a login account) ----
    async saveTeamMember(member) {
        await setDoc(doc(db, TEAM_MEMBERS_COL, member.id), member, { merge: true });
    },
    // Bulk upsert for restore. A backup can carry hundreds of rows and the
    // single-document savers would fire one request each; batchInChunks is the
    // same 400-per-batch path saveUnits already uses.
    // Only the named fields of existing members — see updateTeamMembersFields
    // in script.js (_commitMembers). update, not set: no stub is created for a
    // member deleted elsewhere meanwhile.
    async updateTeamMembersFields(items) {
        if (!items || !items.length) return;
        await batchInChunks(items, (batch, m) => { const { id, ...f } = m; batch.update(doc(db, TEAM_MEMBERS_COL, id), f); });
    },
    async saveTeamMembers(items) {
        if (!items || !items.length) return;
        await batchInChunks(items, (batch, m) => batch.set(doc(db, TEAM_MEMBERS_COL, m.id), m, { merge: true }));
    },
    async deleteTeamMember(id) {
        await deleteDoc(doc(db, TEAM_MEMBERS_COL, id));
    },
    async getAllTeamMembers() {
        const snap = await getDocs(collection(db, TEAM_MEMBERS_COL));
        return snap.docs.map(withDocId);
    },
    subscribeTeamMembers(callback, errorCallback) {
        return onSnapshot(
            collection(db, TEAM_MEMBERS_COL),
            snap => callback(snap.docs.map(withDocId)),
            err => {
                console.error('[cloud] teamMembers subscription error:', err);
                if (errorCallback) errorCallback(err);
            }
        );
    },

    // ---- Shift schedule ----
    // Document id is `${date}_${memberId}`, so one person can only ever hold
    // one shift on a given day — the id itself enforces it, no dedupe needed.
    async saveShift(shift) {
        await setDoc(doc(db, SHIFTS_COL, shift.id), shift, { merge: true });
    },
    async saveShifts(items) {
        if (!items || !items.length) return;
        await batchInChunks(items, (batch, r) => batch.set(doc(db, SHIFTS_COL, r.id), r, { merge: true }));
    },
    async deleteShift(id) {
        await deleteDoc(doc(db, SHIFTS_COL, id));
    },
    // A whole week's quick-fill in one go: shifts to write and cell ids to
    // clear, committed in batches instead of one round-trip per cell.
    async saveShiftChanges(sets, deleteIds) {
        const ops = (sets || []).map(r => ({ set: r })).concat((deleteIds || []).map(id => ({ del: id })));
        if (!ops.length) return;
        await batchInChunks(ops, (batch, op) => op.set
            ? batch.set(doc(db, SHIFTS_COL, op.set.id), op.set, { merge: true })
            : batch.delete(doc(db, SHIFTS_COL, op.del)));
    },
    async getAllShifts() {
        const snap = await getDocs(collection(db, SHIFTS_COL));
        return snap.docs.map(withDocId);
    },
    // Bounded by date, because this collection grows by one document per
    // person per day and never stops — a year of eight people is ~2,900
    // documents pulled to render one week. `date` is an ISO string, so a
    // lexicographic >= is a correct date comparison, and a single-field range
    // needs no composite index.
    //
    // sinceDate is the caller's window; script.js widens it and resubscribes
    // when someone pages back past it.
    subscribeShifts(callback, errorCallback, sinceDate) {
        const q = sinceDate
            ? query(collection(db, SHIFTS_COL), where('date', '>=', sinceDate))
            : query(collection(db, SHIFTS_COL));
        return onSnapshot(
            q,
            snap => callback(snap.docs.map(withDocId)),
            err => {
                console.error('[cloud] shifts subscription error:', err);
                if (errorCallback) errorCallback(err);
            }
        );
    },

    // ---- Daily work logs ----
    async saveWorkLog(log) {
        await setDoc(doc(db, WORK_LOGS_COL, log.id), log, { merge: true });
    },
    async saveWorkLogs(items) {
        if (!items || !items.length) return;
        await batchInChunks(items, (batch, r) => batch.set(doc(db, WORK_LOGS_COL, r.id), r, { merge: true }));
    },
    async deleteWorkLog(id) {
        await deleteDoc(doc(db, WORK_LOGS_COL, id));
    },
    async getAllWorkLogs() {
        const snap = await getDocs(collection(db, WORK_LOGS_COL));
        return snap.docs.map(withDocId);
    },
    // authorUid: only that account's own reports (Daily Report 'own'). No
    // orderBy then — date + author would need a composite index; the app sorts.
    subscribeWorkLogs(callback, errorCallback, authorUid) {
        return _onSnapshotWithPending(
            authorUid ? query(collection(db, WORK_LOGS_COL), where('createdByUid', '==', authorUid))
                      : query(collection(db, WORK_LOGS_COL), orderBy('date', 'desc')),
            callback,
            err => {
                console.error('[cloud] workLogs subscription error:', err);
                if (errorCallback) errorCallback(err);
            }
        );
    },

    // ---- Leave / sick-day requests ----
    // Subscribed whole, with no date window, and that is deliberate. A request
    // waiting for approval can be any age, so a window would hide exactly the
    // rows the decision inbox exists to surface. The volume is also a different
    // class from shifts: eight people filing a few requests a month is roughly
    // 200 documents a year, about 25x smaller than workLogs, which is already
    // subscribed whole. Revisit past ~2,000 documents and window it the way
    // subscribeShifts does.
    async saveLeaveRequest(rec) {
        await setDoc(doc(db, LEAVE_COL, rec.id), rec, { merge: true });
    },
    async saveLeaveRequests(items) {
        if (!items || !items.length) return;
        await batchInChunks(items, (batch, r) => batch.set(doc(db, LEAVE_COL, r.id), r, { merge: true }));
    },
    async deleteLeaveRequest(id) {
        await deleteDoc(doc(db, LEAVE_COL, id));
    },
    async getAllLeaveRequests() {
        const snap = await getDocs(collection(db, LEAVE_COL));
        return snap.docs.map(withDocId);
    },
    subscribeLeaveRequests(callback, errorCallback, authorUid) {
        return _onSnapshotWithPending(
            authorUid ? query(collection(db, LEAVE_COL), where('createdByUid', '==', authorUid))
                      : query(collection(db, LEAVE_COL), orderBy('dateFrom', 'desc')),
            callback,
            err => {
                console.error('[cloud] leaveRequests subscription error:', err);
                if (errorCallback) errorCallback(err);
            }
        );
    },

    // ---- Field documentation photos ----
    // These live apart from the work log on purpose. Photos are stored as data
    // URLs, and subscribeWorkLogs above streams the WHOLE collection with no
    // limit() — so while the photos sat inside the log document, every device
    // re-downloaded every photo ever taken on every app open. Split out, they
    // are fetched one document at a time, only when someone actually opens
    // them. There is no subscription here, and there must not be one.
    // 2.29.5: photos waiting on this device, and a manual retry.
    async pendingPhotoUploads() { try { return ((await _outboxAll()) || []).reduce((n, j) => n + Object.keys(j.pend || {}).length, 0); } catch (_) { return 0; } },
    flushPhotoOutbox() { return _flushPhotoOutbox(); },
    // 2.43.2: the "waiting to upload" placeholder a form shows for a photo still on
    // another phone — no photo for a report, a copy or a cache.
    isWaitingPhoto(src) { return typeof src === 'string' && (_placeholderSrc.has(src) || src.startsWith(PHOTO_PENDING)); },
    async getWorkLogPhotos(id, opts) {
        const got = await _getPhotoDoc(WORK_LOG_PHOTOS_COL, id, 'photos', [], opts);
        if (got.length || !(opts && opts.recover)) return got;
        return _recoverPhotos(WORK_LOG_PHOTOS_COL, id, 'photos', opts);
    },
    async saveWorkLogPhotos(id, photos) {
        await _savePhotoDoc(WORK_LOG_PHOTOS_COL, id, 'photos', photos, { id });
    },
    async deleteWorkLogPhotos(id) {
        await _deletePhotoDoc(WORK_LOG_PHOTOS_COL, id, 'photos');
    },

    // Scanned letters attached to a leave request live in the SAME collection.
    // The name is historical — it predates this feature — but the shape is
    // identical ({ id, photos[], updatedAt }), the id is the parent record's
    // id, and the rules on it are already canEditArea('teamLog'), which is
    // exactly the area a leave request uses. Giving the letters a collection of
    // their own would have added a fourth path waiting on a rules publish, all
    // failing with the same indistinguishable permission-denied.
    //
    // Ids cannot collide: work logs are wl_..., leave requests are lv_...
    async getTeamDocs(id, opts)   { return this.getWorkLogPhotos(id, opts); },
    async saveTeamDocs(id, pages) { return this.saveWorkLogPhotos(id, pages); },
    async deleteTeamDocs(id)      { return this.deleteWorkLogPhotos(id); },

    // Damage photos, same arrangement and for the same reason: damageRecords is
    // subscribed whole with no limit(), so an inline photo was re-downloaded by
    // every device on every app open. Never subscribe this collection either.
    async getDamagePhoto(id, opts) {
        return _getPhotoDoc(DAMAGE_PHOTOS_COL, id, 'photo', '', opts);
    },
    async saveDamagePhoto(id, photo) {
        await _savePhotoDoc(DAMAGE_PHOTOS_COL, id, 'photo', photo, { id });
    },
    async deleteDamagePhoto(id) {
        await _deletePhotoDoc(DAMAGE_PHOTOS_COL, id, 'photo');
    },
    // The photo taken after the repair. Same collection and the same rules as
    // the damage photo itself (now shown as "Before"), one document of its own
    // so opening one side never downloads the other.
    async getDamageAfterPhoto(id, opts) {
        return _getPhotoDoc(DAMAGE_PHOTOS_COL, id + DAMAGE_AFTER_SUFFIX, 'photo', '', opts, () => ({ id, side: 'after' }));
    },
    async saveDamageAfterPhoto(id, photo) {
        await _savePhotoDoc(DAMAGE_PHOTOS_COL, id + DAMAGE_AFTER_SUFFIX, 'photo', photo, { id, side: 'after' });
    },
    async deleteDamageAfterPhoto(id) {
        await _deletePhotoDoc(DAMAGE_PHOTOS_COL, id + DAMAGE_AFTER_SUFFIX, 'photo');
    },

    // ---- Master lists: one document per list (site, brand, company) ----
    // { id, values: [name, …], updatedAt, updatedBy }. Small and read by
    // everyone, so a whole list is one document and one write.
    async saveMasterList(kind, values, by) {
        await setDoc(doc(db, MASTER_LISTS_COL, kind), { id: kind, values, updatedAt: Date.now(), updatedBy: by || '' });
    },
    // ---- Paddocks (2.10.0): one document per company ----
    // { id (company key), company, items: [{ pid, landuse, ha }], updatedAt, updatedBy }.
    // A few hundred rows per company: one document keeps it to one read.
    async savePaddockList(rec) {
        await setDoc(doc(db, PADDOCKS_COL, rec.id), rec);
    },
    async deletePaddockList(id) {
        await deleteDoc(doc(db, PADDOCKS_COL, id));
    },
    subscribePaddocks(callback, errorCallback) {
        return onSnapshot(
            collection(db, PADDOCKS_COL),
            snap => callback(snap.docs.map(withDocId)),
            err => {
                console.error('[cloud] paddocks subscription error:', err);
                if (errorCallback) errorCallback(err);
            }
        );
    },
    subscribeMasterLists(callback, errorCallback) {
        return onSnapshot(
            collection(db, MASTER_LISTS_COL),
            snap => callback(snap.docs.map(withDocId)),
            err => {
                console.error('[cloud] masterLists subscription error:', err);
                if (errorCallback) errorCallback(err);
            }
        );
    },

    // ---- License requirements: one document per implement type ----
    // { id (type key), type, gps: ''|'SF-1'|'SF-RTK', display: ''|'G5 Basic'|'G5 Advance' }
    async saveLicenseRequirement(rec) {
        await setDoc(doc(db, LICENSE_REQ_COL, rec.id), rec);
    },
    // Bulk form for a backup restore. Whole documents, like the single saver:
    // a requirement is two fields, there is nothing to merge into.
    async saveLicenseRequirements(items) {
        if (!items || !items.length) return;
        await batchInChunks(items, (batch, r) => batch.set(doc(db, LICENSE_REQ_COL, r.id), r));
    },
    async deleteLicenseRequirement(id) {
        await deleteDoc(doc(db, LICENSE_REQ_COL, id));
    },
    subscribeLicenseRequirements(callback, errorCallback) {
        return onSnapshot(
            collection(db, LICENSE_REQ_COL),
            snap => callback(snap.docs.map(withDocId)),
            err => {
                console.error('[cloud] licenseRequirements subscription error:', err);
                if (errorCallback) errorCallback(err);
            }
        );
    },

    // ---- Heavy-equipment checks ----
    async saveInspectionPlan(rec) {
        await setDoc(doc(db, INSPECTION_PLANS_COL, rec.id), rec, { merge: true });
    },
    async saveInspectionPlans(items) {
        if (!items || !items.length) return;
        await batchInChunks(items, (batch, r) => batch.set(doc(db, INSPECTION_PLANS_COL, r.id), r, { merge: true }));
    },
    async deleteInspectionPlan(id) {
        await deleteDoc(doc(db, INSPECTION_PLANS_COL, id));
    },
    async getAllInspectionPlans() {
        const snap = await getDocs(collection(db, INSPECTION_PLANS_COL));
        return snap.docs.map(withDocId);
    },
    subscribeInspectionPlans(callback, errorCallback) {
        return onSnapshot(
            collection(db, INSPECTION_PLANS_COL),
            snap => callback(snap.docs.map(withDocId)),
            err => {
                console.error('[cloud] inspectionPlans subscription error:', err);
                if (errorCallback) errorCallback(err);
            }
        );
    },
    async saveInspection(rec) {
        await setDoc(doc(db, INSPECTIONS_COL, rec.id), rec, { merge: true });
    },
    async saveInspections(items) {
        if (!items || !items.length) return;
        await batchInChunks(items, (batch, r) => batch.set(doc(db, INSPECTIONS_COL, r.id), r, { merge: true }));
    },
    async deleteInspection(id) {
        await deleteDoc(doc(db, INSPECTIONS_COL, id));
    },
    async getAllInspections() {
        const snap = await getDocs(collection(db, INSPECTIONS_COL));
        return snap.docs.map(withDocId);
    },
    // The second argument lists the reports still queued on this device — a
    // check filed with no signal. includeMetadataChanges makes the listener
    // fire again when the server acknowledges them, so the "waiting to send"
    // marks clear by themselves.
    subscribeInspections(callback, errorCallback) {
        return onSnapshot(
            collection(db, INSPECTIONS_COL),
            { includeMetadataChanges: true },
            snap => callback(snap.docs.map(withDocId),
                             snap.docs.filter(d => d.metadata.hasPendingWrites).map(d => d.id)),
            err => {
                console.error('[cloud] inspections subscription error:', err);
                if (errorCallback) errorCallback(err);
            }
        );
    },
    // { componentKey: dataURL } for one report. Never subscribed.
    async getInspectionPhotos(id, opts) {
        return _getPhotoDoc(INSPECTION_PHOTOS_COL, id, 'photos', {}, opts);
    },
    async saveInspectionPhotos(id, photos) {
        await _savePhotoDoc(INSPECTION_PHOTOS_COL, id, 'photos', photos, { id });
    },
    async deleteInspectionPhotos(id) {
        await _deletePhotoDoc(INSPECTION_PHOTOS_COL, id, 'photos');
    },

    // A REPLACE restore: each record written as the WHOLE document. The bulk
    // savers above upsert with merge:true — right for an edit, wrong here: a
    // field added after the backup was taken survived the "replace", leaving
    // a record that is half backup and half today (the backup's text next to
    // today's approval stamp). A method of its own, not a flag on those
    // savers, so a stale copy of this file is detectable: an old saver would
    // ignore the flag and merge anyway, an absent method is caught by
    // cloudFn. Names come from a fixed map, like isCollectionEmpty below.
    async replaceDocs(which, items) {
        const cols = {
            units: UNITS_COL, implements: IMPL_COL, damages: DAMAGE_COL, licenseStock: LICENSE_COL,
            userCategories: USER_CATEGORIES_COL, damageComponents: DAMAGE_COMPONENTS_COL,
            teamMembers: TEAM_MEMBERS_COL, shifts: SHIFTS_COL, workLogs: WORK_LOGS_COL,
            leaveRequests: LEAVE_COL, inspectionPlans: INSPECTION_PLANS_COL, inspections: INSPECTIONS_COL,
            devices: DEVICES_COL, stockItems: STOCK_ITEMS_COL, licenseRequirements: LICENSE_REQ_COL,
            vehicles: VEHICLES_COL, fuelLogs: FUEL_LOGS_COL
        };
        const name = cols[which];
        if (!name) throw new Error(`unknown collection: ${which}`);
        if (!items || !items.length) return;
        await batchInChunks(items, (batch, r) => batch.set(doc(db, name, r.id), r));
    },

    // "Is this collection empty?" without downloading it.
    //
    // The first-run bootstrap used to answer this with getAllUnits() and
    // friends and then read only .length — 288 documents fetched on every
    // owner sign-in to decide four yes/no questions. limit(1) answers the same
    // question in one read.
    //
    // Names come from a fixed map rather than a raw string, so this cannot
    // become a way to reach an arbitrary collection from script.js.
    async isCollectionEmpty(which) {
        const cols = {
            units: UNITS_COL, implements: IMPL_COL,
            damages: DAMAGE_COL, licenses: LICENSE_COL
        };
        const name = cols[which];
        if (!name) throw new Error(`unknown collection: ${which}`);
        const snap = await getDocs(query(collection(db, name), limit(1)));
        return snap.empty;
    }
};

// Notify script.js (which loaded earlier as a classic script) that the
// cloud API is now usable. script.js sets a one-shot listener for this
// event before this module ever runs, so the order is safe.
window.cloudReady = true;
document.dispatchEvent(new CustomEvent('cloud-ready'));
console.log('[cloud] Firestore + Auth ready — project:', firebaseConfig.projectId);
