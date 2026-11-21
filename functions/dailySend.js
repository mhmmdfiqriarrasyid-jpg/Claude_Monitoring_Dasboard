// The manager's Daily Report email (2.39.0): reads one day's records,
// builds the deck with dailyCore.js (the same code as the dashboard's
// download), loads the field photos from Storage, and returns the email with
// the PowerPoint and the PDF attached. db: firebase-admin Firestore,
// bucket: a Storage bucket, sharp: the sharp module. tests/daily_deck_test.js.
'use strict';
const { buildDaily, dailyPages, dailyEmail, PHOTO_MAX } = require('./dailyCore');
const { renderPptx, renderPdf, layout, tilesOf } = require('./dailyDeck');

const PHOTO_COL = { wl: ['workLogPhotos', 'photos'], tk: ['ticketPhotos', 'photo'], pj: ['projectPhotos', 'photo'] };
const all = async (q) => (await q.get()).docs.map(d => ({ id: d.id, ...d.data() }));

async function gatherDaily(db, date) {
    const [members, shifts, leaves, workLogs, tickets, dmgDay, dmgFixed, jobs, projects, units] = await Promise.all([
        all(db.collection('teamMembers')), all(db.collection('shifts').where('date', '==', date)), all(db.collection('leaveRequests')),
        all(db.collection('workLogs').where('date', '==', date)), all(db.collection('tickets')),
        all(db.collection('damageRecords').where('date', '==', date)), all(db.collection('damageRecords').where('resolvedAt', '==', date)),
        all(db.collection('otJobs').where('date', '==', date)), all(db.collection('projects')), all(db.collection('units'))]);
    const damages = dmgDay.concat(dmgFixed.filter(d => !dmgDay.some(x => x.id === d.id)));
    return { date, members, shifts, leaves, workLogs, tickets, damages, jobs, projects, units };
}

// photos/<col>/<doc>/<file>, the folder the app uploads a document's photos to.
function photoPathOk(col, docId, path) {
    const m = /^photos\/([A-Za-z]+)\/([A-Za-z0-9_-]+)\/([A-Za-z0-9_-]+)$/.exec(String(path || ''));
    return !!m && m[1] === col && m[2] === String(docId).replace(/[^\w-]/g, '_');
}
// One stored photo ("sp:<path>" in Storage, or an old inline data URL) as a Buffer.
async function photoBuffer(db, bucket, ref) {
    const [col, field] = PHOTO_COL[ref.src] || [];
    if (!col) return null;
    const snap = await db.collection(col).doc(String(ref.id)).get();
    if (!snap.exists) return null;
    const v = snap.data()[field];
    const one = Array.isArray(v) ? v[ref.index || 0] : v;
    if (typeof one !== 'string' || !one) return null;
    if (one.startsWith('sp:')) {
        // 2.43.0: only a photo in that document's own folder. The document is written
        // by users and this server reads with admin rights, past the storage rules.
        if (!photoPathOk(col, ref.id, one.slice(3))) return null;
        const [buf] = await bucket.file(one.slice(3)).download();
        return buf;
    }
    const m = /^data:image\/[a-z+]+;base64,(.+)$/i.exec(one);
    return m ? Buffer.from(m[1], 'base64') : null;
}

// The day's report as an email: { subject, html, text, attachments }, plus counts.
// 2.45.0: it degrades by part instead of failing whole — buildDaily leaves out (and names, D.failed)
// a part it cannot build, and when the PowerPoint / PDF cannot be made the email goes without them
// (fileError says why). Only a failed read of the records stops it.
async function composeDaily({ db, bucket, sharp, date, now }) {
    const D = buildDaily(Object.assign(await gatherDaily(db, date), { now: now == null ? Date.now() : now }));
    const items = [];
    for (const ref of D.photos) {
        if (items.length >= PHOTO_MAX) break;
        try {
            const raw = await photoBuffer(db, bucket, ref);
            if (!raw) continue;
            // 2.40.2: keep its shape (ar) so a portrait photo gets a portrait box.
            const { data, info } = await sharp(raw).rotate().resize({ width: 1400, height: 1400, fit: 'inside', withoutEnlargement: true }).jpeg({ quality: 80 }).toBuffer({ resolveWithObject: true });
            items.push({ key: ref.key, kind: 'photo', cap: ref.cap, sub: ref.sub, data, ar: info.width && info.height ? info.width / info.height : 4 / 3 });
        } catch (_) { /* a photo that cannot be read is left out */ }
    }
    let pages = [], attachments = [], fileError = '';
    try {
        pages = dailyPages(D, { gallery: items.length ? { type: 'gallery', items } : null });
        // Each photo cut to the shape of its box on the page.
        const gp = pages.find(p => p.cols && p.cols[0] && p.cols[0][0] && p.cols[0][0].type === 'gallery');
        if (gp) {
            const c = layout(gp)[0];
            const boxes = tilesOf(items, c.x, c.y, c.w, c.h);
            for (let i = 0; i < items.length; i++) {
                try { items[i].data = await sharp(items[i].data).resize(Math.round(boxes[i].w * 160), Math.round(boxes[i].h * 160), { fit: 'cover' }).jpeg({ quality: 78 }).toBuffer(); } catch (_) { /* keep it whole */ }
            }
        }
        const title = `Daily Report — ${D.day.label}`;
        const [pptx, pdf] = await Promise.all([renderPptx(pages, { title }), renderPdf(pages, { title })]);
        attachments = [
            { filename: `Daily_Report_${date}.pptx`, content: pptx, contentType: 'application/vnd.openxmlformats-officedocument.presentationml.presentation' },
            { filename: `Daily_Report_${date}.pdf`, content: pdf, contentType: 'application/pdf' }];
    } catch (err) {
        fileError = String(err && err.message || err).slice(0, 300);
        D.failed.push('PowerPoint / PDF');
    }
    const m = dailyEmail(D, { attached: attachments.length > 0, photos: items.length });
    if (attachments.length) m.attachments = attachments;
    return { m, D, photos: attachments.length ? items.length : 0, pages: attachments.length ? pages.length : 0, fileError };
}

module.exports = { gatherDaily, photoBuffer, photoPathOk, composeDaily };
