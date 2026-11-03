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
async function _photoUp(col, docId, v) {
    if (typeof v !== 'string' || !v || v.startsWith(PHOTO_REF)) return v;
    if (_photoUrls.has(v)) return _photoUrls.get(v);
    if (!_isImgData(v)) return v;
    if (typeof navigator !== 'undefined' && navigator.onLine === false) return v;
    try {
        const st = await _storage();
        const name = `${Date.now().toString(36)}${Math.random().toString(36).slice(2, 8)}`;
        const path = `photos/${col}/${String(docId).replace(/[^\w-]/g, '_')}/${name}`;
        await _withTimeout(st.mod.uploadString(st.mod.ref(st.s, path), v, 'data_url'), 45000);
        return PHOTO_REF + path;
    } catch (err) {
        console.warn('[photos] kept inline — Storage upload failed:', err && (err.code || err.message));
        return v;
    }
}
async function _photoDown(v, inline) {
    if (typeof v !== 'string' || !v.startsWith(PHOTO_REF)) return v;
    try {
        const st = await _storage();
        const url = await st.mod.getDownloadURL(st.mod.ref(st.s, v.slice(PHOTO_REF.length)));
        _photoUrls.set(url, v);
        if (!inline) return url;
        const blob = await (await fetch(url)).blob();
        return await new Promise((res, rej) => { const r = new FileReader(); r.onload = () => res(r.result); r.onerror = rej; r.readAsDataURL(blob); });
    } catch (err) {
        console.warn('[photos] not loaded from Storage:', err && (err.code || err.message));
        return '';
    }
}
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
async function _dropPhotoRefs(refs) {
    if (!refs.length) return;
    try {
        const st = await _storage();
        await Promise.all(refs.map(r => st.mod.deleteObject(st.mod.ref(st.s, r.slice(PHOTO_REF.length))).catch(() => {})));
    } catch (_) { /* best effort */ }
}
// Saves { ...fields, [field]: images } at col/docId, images through Storage.
// Images the document no longer holds are removed from Storage afterwards.
async function _savePhotoDoc(col, docId, field, value, fields) {
    let before = [];
    try { const old = await getDoc(doc(db, col, docId)); if (old.exists()) before = _photoRefs(old.data()[field]); } catch (_) { /* new or unreadable */ }
    const stored = await _photoMap(value, x => _photoUp(col, docId, x));
    await setDoc(doc(db, col, docId), { ...fields, [field]: stored, updatedAt: Date.now() });
    const keep = new Set(_photoRefs(stored));
    _dropPhotoRefs(before.filter(r => !keep.has(r)));
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
        _savePhotoDoc(col, docId, field, v, fieldsOf ? fieldsOf(data) : { id: data.id || docId }).catch(() => {});
    }
    return _photoMap(v, x => _photoDown(x, opts && opts.inline));
}
async function _deletePhotoDoc(col, docId, field) {
    let refs = [];
    try { const old = await getDoc(doc(db, col, docId)); if (old.exists()) refs = _photoRefs(old.data()[field]); } catch (_) { /* gone */ }
    await deleteDoc(doc(db, col, docId));
    _dropPhotoRefs(refs);
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
const PURCHASE_COL = 'purchaseRequests';
const LEARNING_COL = 'learningItems';        // 2.12.0: Learning Center catalogue
const LEARNING_FILES_COL = 'learningFiles';  // 2.12.0: uploaded PDFs, in pieces
const PURCHASE_PHOTOS_COL = 'purchasePhotos';
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
        return { facts: facts.docs.map(d => ({ ...d.data(), id: d.id })).sort((a, b) => (a.createdAt || 0) - (b.createdAt || 0)),
                 builtIns: !(cfg && cfg.exists() && cfg.data().builtIns === false) };
    },
    async saveAgriFact(rec) { await setDoc(doc(db, 'agriFacts', rec.id), rec); },
    async deleteAgriFact(id) { await deleteDoc(doc(db, 'agriFacts', id)); },
    async saveAgriSettings(rec) { await setDoc(doc(db, 'settings', 'agri'), { builtIns: !!rec.builtIns, updatedAt: Date.now() }); },

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
        try { const s = await getDoc(doc(db, PUSH_TOKENS_COL, t)); return s.exists() && s.data().uid === uid ? t : ''; } catch (_) { return ''; }
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
    async callFunction(name, data) {
        const f = await _functions();
        const res = await f.mod.httpsCallable(f.f, name, { timeout: 60000 })(data || {});
        return res.data;
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
    async learningFileUrl(path) {
        const st = await _storage();
        return st.mod.getDownloadURL(st.mod.ref(st.s, path));
    },
    async deleteLearningFile(path) {
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
    async getWorkLogPhotos(id, opts) {
        return _getPhotoDoc(WORK_LOG_PHOTOS_COL, id, 'photos', [], opts);
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
