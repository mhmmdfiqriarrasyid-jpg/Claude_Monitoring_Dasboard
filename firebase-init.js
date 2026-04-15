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
const USERS_COL = 'users';
const HISTORY_COL = 'history';

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
    async clearHistoryCloud() {
        const snap = await getDocs(collection(db, HISTORY_COL));
        if (snap.empty) return;
        await batchInChunks(snap.docs, (batch, d) => batch.delete(d.ref));
    }
};

// Notify script.js (which loaded earlier as a classic script) that the
// cloud API is now usable. script.js sets a one-shot listener for this
// event before this module ever runs, so the order is safe.
window.cloudReady = true;
document.dispatchEvent(new CustomEvent('cloud-ready'));
console.log('[cloud] Firestore + Auth ready — project:', firebaseConfig.projectId);
