// Photo thumbnails (2.35.0). Every photo in Storage (photos/<collection>/
// <record>/<file>) gets a small copy next to it, <file>_t — the same folder,
// so the same storage.rules apply. The app makes it when it uploads a photo;
// this nightly pass makes the ones still missing (photos from before 2.35.0,
// or sent by an older tab) and removes thumbnails whose photo is gone.
// bucket: a firebase-admin Storage bucket, sharp: the sharp module.
'use strict';
const SUFFIX = '_t';
const MAX_SIDE = 360;

async function makeThumb(sharp, buf) {
    return sharp(buf).rotate().resize({ width: MAX_SIDE, height: MAX_SIDE, fit: 'inside', withoutEnlargement: true }).jpeg({ quality: 70 }).toBuffer();
}

// opts.limit: photos per run (default 500); opts.deadline: stop after this time (ms).
async function thumbsBackfill(bucket, sharp, opts) {
    opts = opts || {};
    const limit = opts.limit || 500;
    const [files] = await bucket.getFiles({ prefix: 'photos/' });
    const names = new Set(files.map(f => f.name));
    let made = 0, failed = 0, orphans = 0, left = 0;
    for (const f of files) {
        if (f.name.endsWith(SUFFIX)) {
            if (!names.has(f.name.slice(0, -SUFFIX.length))) { await f.delete().catch(() => {}); orphans++; }
            continue;
        }
        if (names.has(f.name + SUFFIX)) continue;
        const type = (f.metadata && f.metadata.contentType) || '';
        if (!/^image\//.test(type)) continue;
        if (made + failed >= limit || (opts.deadline && Date.now() > opts.deadline)) { left++; continue; }
        try {
            const [buf] = await f.download();
            const out = await makeThumb(sharp, buf);
            await bucket.file(f.name + SUFFIX).save(out, { contentType: 'image/jpeg', resumable: false, metadata: { cacheControl: 'private, max-age=604800' } });
            made++;
        } catch (err) { failed++; }
    }
    return { made, failed, orphans, left };
}

module.exports = { thumbsBackfill, makeThumb, SUFFIX, MAX_SIDE };
