// Daily backup (2.35.0) — every Firestore collection written to Cloud Storage
// as backups/<YYYY-MM-DD>/<collection>.json.gz, with a manifest. Kept 30 days,
// and the 1st of each month for a year. Restoring only ever ADDS records that
// are missing — nothing that exists now is overwritten — so it is safe to run
// after a wrong delete. Kept apart from index.js so it runs against the
// emulators (tests/backup_e2e.js). db: firebase-admin Firestore, bucket: a
// firebase-admin Storage bucket.
'use strict';
const zlib = require('zlib');

const KEEP_DAYS = 30;
const KEEP_MONTHLY_DAYS = 365;
// Not data: device push tokens, the server's own bookkeeping and logs.
const SKIP = ['pushTokens', 'mailState', 'mailLog', 'clientErrors'];
// Never restored from a file: who may do what belongs on the Users page.
const NO_RESTORE = ['users'];
const PREFIX = 'backups/';
const DATE_RE = /^\d{4}-\d{2}-\d{2}$/;
const NAME_RE = /^[A-Za-z][A-Za-z0-9_]{0,60}$/;

const pad = n => String(n).padStart(2, '0');
const isoDate = d => `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())}`;

// Firestore Timestamps survive the trip as { __ts: millis }.
function encode(v) {
    if (v == null || typeof v !== 'object') return v;
    if (typeof v.toMillis === 'function') return { __ts: v.toMillis() };
    if (Array.isArray(v)) return v.map(encode);
    const out = {};
    Object.keys(v).forEach(k => { out[k] = encode(v[k]); });
    return out;
}
function decode(v, Timestamp) {
    if (v == null || typeof v !== 'object') return v;
    if (Array.isArray(v)) return v.map(x => decode(x, Timestamp));
    const keys = Object.keys(v);
    if (keys.length === 1 && keys[0] === '__ts' && typeof v.__ts === 'number') return Timestamp ? Timestamp.fromMillis(v.__ts) : new Date(v.__ts);
    const out = {};
    keys.forEach(k => { out[k] = decode(v[k], Timestamp); });
    return out;
}

// Which dated folders go: older than 30 days, unless the 1st of a month within a year.
function expiredDates(dates, today) {
    const t0 = Date.parse(today + 'T00:00:00Z');
    return dates.filter(d => {
        if (!DATE_RE.test(d)) return false;
        const age = Math.round((t0 - Date.parse(d + 'T00:00:00Z')) / 86400000);
        if (age <= KEEP_DAYS) return false;
        return !(d.endsWith('-01') && age <= KEEP_MONTHLY_DAYS);
    });
}

async function listDates(bucket) {
    const [files] = await bucket.getFiles({ prefix: PREFIX });
    const dates = new Map();
    files.forEach(f => { const m = /^backups\/(\d{4}-\d{2}-\d{2})\//.exec(f.name); if (m) dates.set(m[1], (dates.get(m[1]) || []).concat(f)); });
    return dates;
}

async function backupRun(db, bucket, now, opts) {
    const date = isoDate(now);
    const cols = (await db.listCollections()).map(c => c.id).filter(id => !SKIP.includes(id) && NAME_RE.test(id)).sort();
    const manifest = { v: 1, date, at: now.getTime(), trigger: (opts && opts.trigger) || 'auto', collections: [], docs: 0, bytes: 0 };
    for (const name of cols) {
        const snap = await db.collection(name).get();
        const rows = snap.docs.map(d => ({ id: d.id, data: encode(d.data()) }));
        const gz = zlib.gzipSync(Buffer.from(JSON.stringify(rows), 'utf8'));
        await bucket.file(`${PREFIX}${date}/${name}.json.gz`).save(gz, { contentType: 'application/gzip', resumable: false, metadata: { cacheControl: 'private, max-age=0' } });
        manifest.collections.push({ name, docs: rows.length, bytes: gz.length });
        manifest.docs += rows.length; manifest.bytes += gz.length;
    }
    await bucket.file(`${PREFIX}${date}/manifest.json`).save(JSON.stringify(manifest), { contentType: 'application/json', resumable: false });
    // Old days leave only after today's is safely written.
    const dates = await listDates(bucket);
    const gone = expiredDates([...dates.keys()], date);
    for (const d of gone) await Promise.all(dates.get(d).map(f => f.delete().catch(() => {})));
    const kept = [...dates.keys()].filter(d => !gone.includes(d)).sort().reverse();
    await db.doc('mailState/backup').set({ date, at: now.getTime(), docs: manifest.docs, bytes: manifest.bytes, collections: manifest.collections.length,
        dates: kept.slice(0, 60), result: 'ok' });
    return { sent: true, date, docs: manifest.docs, bytes: manifest.bytes, collections: manifest.collections.length, removed: gone.length };
}

async function backupList(db, bucket) {
    const st = await db.doc('mailState/backup').get();
    const dates = [...(await listDates(bucket)).keys()].sort().reverse();
    return { last: st.exists ? st.data() : null, dates };
}

async function backupManifest(bucket, date) {
    if (!DATE_RE.test(date)) throw new Error('bad-date');
    const f = bucket.file(`${PREFIX}${date}/manifest.json`);
    const [exists] = await f.exists();
    if (!exists) return null;
    return JSON.parse((await f.download())[0].toString('utf8'));
}

// Adds the records of one collection that are in the backup and not in the
// database now. Nothing that exists is touched. Returns { added, kept }.
async function restoreMissing(db, bucket, date, name, Timestamp) {
    if (!DATE_RE.test(date) || !NAME_RE.test(name)) throw new Error('bad-request');
    if (NO_RESTORE.includes(name) || SKIP.includes(name)) throw new Error('not-restorable');
    const f = bucket.file(`${PREFIX}${date}/${name}.json.gz`);
    const [exists] = await f.exists();
    if (!exists) throw new Error('not-found');
    const rows = JSON.parse(zlib.gunzipSync((await f.download())[0]).toString('utf8'));
    let added = 0, kept = 0;
    for (let i = 0; i < rows.length; i += 300) {
        const part = rows.slice(i, i + 300).filter(r => r && typeof r.id === 'string' && r.id && r.data && typeof r.data === 'object');
        if (!part.length) continue;
        const snaps = await db.getAll(...part.map(r => db.collection(name).doc(r.id)));
        const batch = db.batch();
        let n = 0;
        snaps.forEach((s, k) => {
            if (s.exists) { kept++; return; }
            batch.create(s.ref, decode(part[k].data, Timestamp)); n++;
        });
        if (n) { await batch.commit(); added += n; }
    }
    return { added, kept, total: rows.length };
}

module.exports = { backupRun, backupList, backupManifest, restoreMissing, expiredDates, encode, decode, KEEP_DAYS, KEEP_MONTHLY_DAYS, SKIP, NO_RESTORE };
