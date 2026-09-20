/* OT Monitoring Tractor and Device — Firebase / Firestore / Auth initializer
   Loaded as an ES module from index.html. Bridges Firebase Auth + Firestore
   to the classic (non-module) script.js via a global window.cloud API.        */

import { initializeApp } from "https://www.gstatic.com/firebasejs/12.12.0/firebase-app.js";
import {
    getFirestore,
    collection,
    doc,
    setDoc,
    deleteDoc,
    getDoc,
    getDocs,
    onSnapshot,
    writeBatch,
    enableIndexedDbPersistence,
    query,
    orderBy,
    limit
} from "https://www.gstatic.com/firebasejs/12.12.0/firebase-firestore.js";
import {
    getAuth,
    onAuthStateChanged,
    createUserWithEmailAndPassword,
    signInWithEmailAndPassword,
    signOut,
    updateProfile,
    setPersistence,
    browserLocalPersistence
} from "https://www.gstatic.com/firebasejs/12.12.0/firebase-auth.js";

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
const db = getFirestore(app);
const auth = getAuth(app);

// Owner allowlist — any account that signs up with one of these emails is
// auto-promoted to role=owner with status=active. Other emails start as
// role=viewer with status=pending and must be approved by an owner.
const OWNER_EMAILS = ['mhmmdfiqriarrasyid@gmail.com'];

// Persist login across reloads / browser restarts (best-effort).
setPersistence(auth, browserLocalPersistence).catch(err => {
    console.warn('[auth] persistence not enabled:', err.code);
});

// Best-effort offline persistence — works in single-tab Chrome, may fail
// in multi-tab or private mode; we just log and continue.
try {
    enableIndexedDbPersistence(db).catch(err => {
        console.warn('[cloud] offline persistence not enabled:', err.code);
    });
} catch (e) { /* ignore */ }

const UNITS_COL = 'units';
const IMPL_COL = 'implements';
const DAMAGE_COL = 'damageRecords';
const LICENSE_COL = 'licenseStock';
const USERS_COL = 'users';
const HISTORY_COL = 'history';
const USER_CATEGORIES_COL = 'userCategories';
const DAMAGE_COMPONENTS_COL = 'damageComponents';
const DEVICES_COL = 'devices';
const STOCK_ITEMS_COL = 'stockItems';
const TEAM_MEMBERS_COL = 'teamMembers';
const SHIFTS_COL = 'shifts';
const WORK_LOGS_COL = 'workLogs';
// Field documentation lives in its own collection, one document per work
// log, and is deliberately never subscribed — see getWorkLogPhotos below.
const WORK_LOG_PHOTOS_COL = 'workLogPhotos';
// Same again for damage records — one document per record, never subscribed.
const DAMAGE_PHOTOS_COL = 'damagePhotos';

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
    OWNER_EMAILS,

    // ---- Units ----
    async saveUnit(unit) {
        await setDoc(doc(db, UNITS_COL, unit.id), unit, { merge: true });
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
    async getAllUnits() {
        const snap = await getDocs(collection(db, UNITS_COL));
        return snap.docs.map(d => d.data());
    },
    subscribeUnits(callback, errorCallback) {
        return onSnapshot(
            collection(db, UNITS_COL),
            snap => callback(snap.docs.map(d => d.data())),
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
        return snap.docs.map(d => d.data());
    },
    subscribeImplements(callback, errorCallback) {
        return onSnapshot(
            collection(db, IMPL_COL),
            snap => callback(snap.docs.map(d => d.data())),
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
    async getAllDamages() {
        const snap = await getDocs(collection(db, DAMAGE_COL));
        return snap.docs.map(d => d.data());
    },
    subscribeDamages(callback, errorCallback) {
        return onSnapshot(
            collection(db, DAMAGE_COL),
            snap => callback(snap.docs.map(d => d.data())),
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
        return snap.docs.map(d => d.data());
    },
    subscribeLicenses(callback, errorCallback) {
        return onSnapshot(
            collection(db, LICENSE_COL),
            snap => callback(snap.docs.map(d => d.data())),
            err => {
                console.error('[cloud] license subscription error:', err);
                if (errorCallback) errorCallback(err);
            }
        );
    },

    // ---- Auth ----
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
        const isOwner = this.isOwnerEmail(user.email);
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
        if (!this.isOwnerEmail(user.email)) return null;
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
    async claimSession(uid, session) {
        await setDoc(doc(db, USERS_COL, uid), { activeSession: session }, { merge: true });
    },
    // Owner-initiated remote sign-out: no device can ever hold this id, so
    // whoever is signed in is dropped on their next snapshot.
    async revokeUserSession(uid, revokedByEmail) {
        await setDoc(doc(db, USERS_COL, uid), {
            activeSession: {
                id: 'revoked_' + Date.now(),
                revokedAt: Date.now(),
                revokedBy: revokedByEmail || null
            }
        }, { merge: true });
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

    async updateUserAccess(uid, access, updatedByEmail) {
        await setDoc(doc(db, USERS_COL, uid), {
            access,
            updatedAt: Date.now(),
            updatedBy: updatedByEmail || null
        }, { merge: true });
    },
    async deleteUserDoc(uid) {
        await deleteDoc(doc(db, USERS_COL, uid));
    },
    subscribeUsers(callback, errorCallback) {
        return onSnapshot(
            collection(db, USERS_COL),
            snap => callback(snap.docs.map(d => d.data())),
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
    subscribeHistory(callback, errorCallback, max = 500) {
        const q = query(
            collection(db, HISTORY_COL),
            orderBy('timestamp', 'desc'),
            limit(max)
        );
        return onSnapshot(q,
            snap => callback(snap.docs.map(d => d.data())),
            err => {
                console.error('[cloud] history subscription error:', err);
                if (errorCallback) errorCallback(err);
            }
        );
    },
    // The live subscription is capped so the dashboard stays light. Export
    // needs the whole audit trail, not just the newest page of it.
    async getAllHistory() {
        const snap = await getDocs(query(collection(db, HISTORY_COL), orderBy('timestamp', 'desc')));
        return snap.docs.map(d => d.data());
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
        return snap.docs.map(d => d.data());
    },
    subscribeUserCategories(callback, errorCallback) {
        return onSnapshot(
            collection(db, USER_CATEGORIES_COL),
            snap => callback(snap.docs.map(d => d.data())),
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
        return snap.docs.map(d => d.data());
    },
    subscribeDamageComponents(callback, errorCallback) {
        return onSnapshot(
            collection(db, DAMAGE_COMPONENTS_COL),
            snap => callback(snap.docs.map(d => d.data())),
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
    async deleteDevice(id) {
        await deleteDoc(doc(db, DEVICES_COL, id));
    },
    async getAllDevices() {
        const snap = await getDocs(collection(db, DEVICES_COL));
        return snap.docs.map(d => d.data());
    },
    subscribeDevices(callback, errorCallback) {
        return onSnapshot(
            collection(db, DEVICES_COL),
            snap => callback(snap.docs.map(d => d.data())),
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
    async deleteStockItem(id) {
        await deleteDoc(doc(db, STOCK_ITEMS_COL, id));
    },
    async getAllStockItems() {
        const snap = await getDocs(collection(db, STOCK_ITEMS_COL));
        return snap.docs.map(d => d.data());
    },
    subscribeStockItems(callback, errorCallback) {
        return onSnapshot(
            query(collection(db, STOCK_ITEMS_COL), orderBy('date', 'desc')),
            snap => callback(snap.docs.map(d => d.data())),
            err => {
                console.error('[cloud] stockItems subscription error:', err);
                if (errorCallback) errorCallback(err);
            }
        );
    },

    // ---- Team members (people, including those without a login account) ----
    async saveTeamMember(member) {
        await setDoc(doc(db, TEAM_MEMBERS_COL, member.id), member, { merge: true });
    },
    async deleteTeamMember(id) {
        await deleteDoc(doc(db, TEAM_MEMBERS_COL, id));
    },
    async getAllTeamMembers() {
        const snap = await getDocs(collection(db, TEAM_MEMBERS_COL));
        return snap.docs.map(d => d.data());
    },
    subscribeTeamMembers(callback, errorCallback) {
        return onSnapshot(
            collection(db, TEAM_MEMBERS_COL),
            snap => callback(snap.docs.map(d => d.data())),
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
    async deleteShift(id) {
        await deleteDoc(doc(db, SHIFTS_COL, id));
    },
    async getAllShifts() {
        const snap = await getDocs(collection(db, SHIFTS_COL));
        return snap.docs.map(d => d.data());
    },
    subscribeShifts(callback, errorCallback) {
        return onSnapshot(
            collection(db, SHIFTS_COL),
            snap => callback(snap.docs.map(d => d.data())),
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
    async deleteWorkLog(id) {
        await deleteDoc(doc(db, WORK_LOGS_COL, id));
    },
    async getAllWorkLogs() {
        const snap = await getDocs(collection(db, WORK_LOGS_COL));
        return snap.docs.map(d => d.data());
    },
    subscribeWorkLogs(callback, errorCallback) {
        return onSnapshot(
            query(collection(db, WORK_LOGS_COL), orderBy('date', 'desc')),
            snap => callback(snap.docs.map(d => d.data())),
            err => {
                console.error('[cloud] workLogs subscription error:', err);
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
    async getWorkLogPhotos(id) {
        const snap = await getDoc(doc(db, WORK_LOG_PHOTOS_COL, id));
        if (!snap.exists()) return [];
        const data = snap.data();
        return Array.isArray(data.photos) ? data.photos : [];
    },
    async saveWorkLogPhotos(id, photos) {
        await setDoc(doc(db, WORK_LOG_PHOTOS_COL, id),
                     { id, photos, updatedAt: Date.now() });
    },
    async deleteWorkLogPhotos(id) {
        await deleteDoc(doc(db, WORK_LOG_PHOTOS_COL, id));
    },

    // Damage photos, same arrangement and for the same reason: damageRecords is
    // subscribed whole with no limit(), so an inline photo was re-downloaded by
    // every device on every app open. Never subscribe this collection either.
    async getDamagePhoto(id) {
        const snap = await getDoc(doc(db, DAMAGE_PHOTOS_COL, id));
        if (!snap.exists()) return '';
        const data = snap.data();
        return typeof data.photo === 'string' ? data.photo : '';
    },
    async saveDamagePhoto(id, photo) {
        await setDoc(doc(db, DAMAGE_PHOTOS_COL, id),
                     { id, photo, updatedAt: Date.now() });
    },
    async deleteDamagePhoto(id) {
        await deleteDoc(doc(db, DAMAGE_PHOTOS_COL, id));
    }
};

// Notify script.js (which loaded earlier as a classic script) that the
// cloud API is now usable. script.js sets a one-shot listener for this
// event before this module ever runs, so the order is safe.
window.cloudReady = true;
document.dispatchEvent(new CustomEvent('cloud-ready'));
console.log('[cloud] Firestore + Auth ready — project:', firebaseConfig.projectId);
