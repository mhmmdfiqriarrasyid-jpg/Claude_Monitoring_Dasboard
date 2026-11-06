// Housekeeping (2.19.0) — kept apart from index.js so it can be run against
// the emulators (tests/housekeeping_e2e.js). db: a firebase-admin Firestore,
// bucket: a firebase-admin Storage bucket.
'use strict';
const ERROR_KEEP_MS = 30 * 86400000;
const HISTORY_KEEP_MS = 730 * 86400000;
async function housekeepingRun(db, bucket, now) {
    const oldErr = await db.collection('clientErrors').where('at', '<', now.getTime() - ERROR_KEEP_MS).limit(500).get();
    await Promise.all(oldErr.docs.map(d => d.ref.delete()));
    const old = await db.collection('history').where('timestamp', '<', now.getTime() - HISTORY_KEEP_MS)
        .orderBy('timestamp').limit(2000).get();
    let archived = 0;
    if (!old.empty) {
        const byMonth = new Map();
        old.docs.forEach(d => {
            const r = { ...d.data(), id: d.id };
            const t = new Date(Number(r.timestamp) || 0);
            const ym = `${t.getFullYear()}-${String(t.getMonth() + 1).padStart(2, '0')}`;
            byMonth.set(ym, (byMonth.get(ym) || []).concat(r));
        });
        for (const [ym, rows] of byMonth) {
            const file = bucket.file(`archives/history/${ym}.json`);
            let have = [];
            try { const [exists] = await file.exists(); if (exists) have = JSON.parse((await file.download())[0].toString('utf8')); } catch (_) { have = []; }
            const ids = new Set(have.map(r => r.id));
            const all = have.concat(rows.filter(r => !ids.has(r.id))).sort((a, b) => (a.timestamp || 0) - (b.timestamp || 0));
            await file.save(JSON.stringify(all), { contentType: 'application/json', resumable: false });
            // Only once the month's file is written do its rows leave Firestore.
            for (let i = 0; i < rows.length; i += 400) {
                const batch = db.batch();
                rows.slice(i, i + 400).forEach(r => batch.delete(db.doc(`history/${r.id}`)));
                await batch.commit();
            }
            archived += rows.length;
        }
    }
    return { sent: false, reason: `errors -${oldErr.size}, history archived ${archived}` };
}


module.exports = { housekeepingRun, ERROR_KEEP_MS, HISTORY_KEEP_MS };
