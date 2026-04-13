/* OT Monitoring Tractor and Device — Firebase / Firestore initializer
   Loaded as an ES module from index.html. Bridges Firestore to the
   classic (non-module) script.js via a global window.cloud API.        */

import { initializeApp } from "https://www.gstatic.com/firebasejs/12.12.0/firebase-app.js";
import {
    getFirestore,
    collection,
    doc,
    setDoc,
    deleteDoc,
    getDocs,
    onSnapshot,
    writeBatch,
    enableIndexedDbPersistence
} from "https://www.gstatic.com/firebasejs/12.12.0/firebase-firestore.js";

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

// Best-effort offline persistence — works in single-tab Chrome, may fail
// in multi-tab or private mode; we just log and continue.
try {
    enableIndexedDbPersistence(db).catch(err => {
        console.warn('[cloud] offline persistence not enabled:', err.code);
    });
} catch (e) { /* ignore */ }

const UNITS_COL = 'units';
const IMPL_COL = 'implements';

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
    }
};

// Notify script.js (which loaded earlier as a classic script) that the
// cloud API is now usable. script.js sets a one-shot listener for this
// event before this module ever runs, so the order is safe.
window.cloudReady = true;
document.dispatchEvent(new CustomEvent('cloud-ready'));
console.log('[cloud] Firestore ready — project:', firebaseConfig.projectId);
