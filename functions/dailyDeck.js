// The manager's Daily Report as files, on the server (2.39.0): the pages of
// dailyCore.js drawn once as a list of shapes (rectangles, text, pictures) on
// a 10 × 5.625 in slide, then written out twice — PowerPoint (pptxgenjs) and
// PDF (pdfkit) — so both look the same. Only what the daily deck uses:
// stats, bars, list, table and gallery cards, in rows or columns, on the
// company template. tests/daily_deck_test.js.
'use strict';
const fs = require('fs');
const path = require('path');

const COL = { ink: '101828', green: '0F766E', sage: 'B9D8D3', rust: 'B42318', slate: '667085', hair: 'E4E7EC', tint: 'EEF2F1', amber: 'B54708',
    card: 'FFFFFF', good: '067647', warnBg: 'FFF7ED', warnLine: 'FED7AA', msg: '385723', sep: 'EEF0F2' };
const TONE = { good: COL.good, warn: COL.amber, bad: COL.rust, muted: COL.slate };
const X0 = 0.4, W0p = 9.2, W0 = W0p, GAP = 0.14, PAD = 0.16, BOTTOM = 5.4;

// Text is cut to its box here (neither writer measures text the same way).
const CHAR = 0.0074;    // inches per point of font size, an average letter
function fitLine(s, w, size) {
    s = String(s == null ? '' : s).replace(/\s+/g, ' ').trim();
    const max = Math.max(3, Math.floor(w / (size * CHAR)));
    return s.length > max ? s.slice(0, max - 1).trimEnd() + '…' : s;
}
function fitLines(s, w, h, size) {
    const max = Math.max(3, Math.floor(w / (size * CHAR))), rows = Math.max(1, Math.floor(h / (size * 1.25 / 72)));
    const words = String(s == null ? '' : s).replace(/\s+/g, ' ').trim().split(' ');
    const out = [];
    let cur = '';
    for (const wd of words) {
        if (!cur) cur = wd; else if ((cur + ' ' + wd).length <= max) cur += ' ' + wd; else { out.push(cur); cur = wd; }
        if (out.length === rows) break;
    }
    if (out.length < rows && cur) out.push(cur);
    const all = words.join(' ').length, used = out.join(' ').length;
    if (used < all && out.length) out[out.length - 1] = fitLine(out[out.length - 1] + ' …', w, size).replace(/ …$/, '…');
    return out.map(l => fitLine(l, w, size)).join('\n');
}
const plain = c => c && typeof c === 'object' ? c : { text: c };
const str = v => String(v == null ? '' : v);

// ---- Layout: the box of every card (same geometry as weeklyreport.js) ----
// 2.40.0: the height a card needs for what it holds (estimates that match the drawing below).
const lines = (t, size, w, max) => Math.min(max, Math.max(1, Math.ceil(str(t).length * size * CHAR / Math.max(0.3, w))));
const barRowH = (it, w) => lines(it[0], 9.5, Math.min(1.7, w * 0.42) - 0.05, 2) > 1 ? 0.4 : 0.3;
const listItemH = (it, w, tagW) => (it.head ? 0.19 : 0) + 0.165 * lines(it.text, 9.5, w - 0.18 - tagW, 6) + 0.03 + (it.sub ? 0.15 * lines(it.sub, 8, w - 0.18, 2) : 0) + (it.note ? 0.17 : 0);
function blockH(b, w) {
    switch (b.type) {
    case 'stats': return Math.ceil(Math.max(1, b.items.length) / (b.per || 3)) * 0.62;
    case 'bars': return b.items.length ? b.items.reduce((a, it) => a + barRowH(it, w), 0) : 0.3;
    case 'list': { if (!b.items.length) return 0.3; const tagW = b.items.some(it => it.tag) ? 0.78 : 0; return b.items.reduce((a, it) => a + listItemH(it, w, tagW) + 0.09, 0) + (b.more ? 0.2 : 0); }
    case 'table': { if (!b.rows.length) return 0.3; const cols = tableCols(b, w, 9), lh = 9 * 1.22 / 72;
        return 0.25 + b.rows.reduce((a, r) => a + Math.max(0.25, Math.max(...r.map((c, i) => Math.min(3, Math.ceil(str(plain(c).text).length * 9 * CHAR / Math.max(0.2, cols[i] - 0.1))))) * lh + 0.07), 0) + (b.more ? 0.22 : 0); }
    case 'gallery': return 99;   // takes the room there is
    }
    return 0.5;
}
const cardH = (b, w) => b.type === 'gallery' ? 99 : PAD * 2 + (b.title ? 0.26 : 0) + blockH(b, w - PAD * 2) + 0.04;
// The box of every card: as tall as what it holds; squeezed (by the page's weights) only when it does not fit.
// 2.40.1: z — laid out on a page z times smaller, drawn z times larger (see pageOps).
function layout(pg, z) {
    z = z || 1;
    const W0 = W0p / z, y0 = (pg.sub ? 1.12 : 0.92) + (pg.strip ? 0.3 : 0), avail = (BOTTOM - y0) / z, cards = [];
    cards.y0 = y0; cards.fits = true;
    if (pg.rows) {
        const hsum = pg.rows.reduce((a, r) => a + r.h, 0), room = avail - GAP * (pg.rows.length - 1);
        const geo = pg.rows.map(r => { const wsum = r.widths.reduce((a, x) => a + x, 0), space = W0 - GAP * (r.cards.length - 1); return r.cards.map((b, k) => space * (r.widths[k] || 1) / wsum); });
        const nat = pg.rows.map((r, i) => Math.max(...r.cards.map((b, k) => cardH(b, geo[i][k]))));
        const fits = nat.reduce((a, x) => a + x, 0) <= room;
        cards.fits = fits;
        let cy = y0;
        pg.rows.forEach((r, i) => {
            const rh = fits ? nat[i] : room * r.h / hsum;
            let cx = X0;
            r.cards.forEach((b, k) => { cards.push({ b, x: cx, y: cy, w: geo[i][k], h: rh }); cx += geo[i][k] + GAP; });
            cy += rh + GAP;
        });
        return cards;
    }
    const total = pg.widths.reduce((a, x) => a + x, 0), space = W0 - GAP * (pg.cols.length - 1);
    let cx = X0;
    pg.cols.forEach((col, k) => {
        const cw = space * pg.widths[k] / total, room = avail - GAP * (col.length - 1);
        const nat = col.map(b => Math.min(room, cardH(b, cw))), sum = nat.reduce((a, x) => a + x, 0);
        if (col.reduce((a, b) => a + cardH(b, cw), 0) > room) cards.fits = false;
        const hs = sum <= room ? nat : nat.map(x => x * room / sum);
        let cy = y0;
        col.forEach((b, i) => { cards.push({ b, x: cx, y: cy, w: cw, h: hs[i] }); cy += hs[i] + GAP; });
        cx += cw + GAP;
    });
    return cards;
}
function galleryGeom(n, x, y, w0, h0) {
    const cols = n <= 3 ? Math.max(1, n) : 3, rows = n <= 3 ? 1 : 2, gap = 0.25, cap = 0.48;
    const w = (w0 - gap * (cols - 1)) / cols, rowH = (h0 - gap * (rows - 1)) / rows, ph = Math.min(rowH - cap, w * 0.75);
    return Array.from({ length: n }, (_, i) => ({ x: x + (i % cols) * (w + gap), y: y + Math.floor(i / cols) * (ph + cap + gap), w, h: ph }));
}
// Column widths: short columns get what their text needs, the rest share what is left.
function tableCols(b, w, size) {
    const n = b.head.length, len = Array.from({ length: n }, (_, i) => Math.max(str(b.head[i].t).length, ...b.rows.map(r => str(plain(r[i]).text).length)));
    const need = len.map(l => Math.min(l, 40) * size * CHAR + 0.12);
    const sum = need.reduce((a, x) => a + x, 0);
    if (sum <= w) return need.map(x => x + (w - sum) * x / sum);
    const short = need.map(x => x <= 0.9), fixed = need.reduce((a, x, i) => a + (short[i] ? x : 0), 0), flex = sum - fixed;
    return need.map((x, i) => short[i] ? x : x * Math.max(0.2, w - fixed) / flex);
}

// ---- The shapes of one page ----
// op: { t: 'rect', x, y, w, h, fill, line, r } | { t: 'text', s, x, y, w, h, size, bold, color, align, valign } |
//     { t: 'img', key | data, x, y, w, h } | { t: 'line', x, y, w, h, color, width }
function drawBlock(ops, b, x, y, w, h, warn) {
    const T = (s, o) => ops.push(Object.assign({ t: 'text', s: str(s), size: 9.5, color: COL.ink, align: 'left', valign: 'top' }, o));
    const empty = msg => T(msg || '—', { x, y, w, h: 0.3, size: 10, color: COL.slate });
    const sep = warn ? 'FDE3C3' : COL.sep;
    switch (b.type) {
    case 'stats': {
        const per = b.per || 3, cw = w / per, rows = Math.ceil(Math.max(1, b.items.length) / per), rh = Math.min(0.7, h / rows);
        if (!b.items.length) return empty(b.empty);
        b.items.forEach((s, i) => { const cx = x + (i % per) * cw, cy = y + Math.floor(i / per) * rh;
            T(fitLine(s.value == null ? '—' : s.value, cw - 0.08, 18), { x: cx, y: cy, w: cw - 0.08, h: rh * 0.5, size: 18, bold: true, color: s.tone ? TONE[s.tone] || COL.ink : COL.ink, valign: 'bottom' });
            T(fitLine(s.label, cw - 0.08, 8.5), { x: cx, y: cy + rh * 0.52, w: cw - 0.08, h: 0.2, size: 8.5, color: COL.slate }); });
        return;
    }
    case 'bars': {
        if (!b.items.length) return empty(b.empty);
        const max = b.max || Math.max(1, ...b.items.map(i => Number(i[1]) || 0));
        const lw = Math.min(1.7, w * 0.42), vw = 0.52, tw = w - lw - vw - 0.1, hs = b.items.map(it => barRowH(it, w)), k = Math.min(1, h / hs.reduce((a, v) => a + v, 0));
        let by = y;
        b.items.forEach((it, i) => { const rh = hs[i] * k, cy = by, v = Number(it[1]) || 0; by += rh;
            T(fitLines(it[0], lw - 0.05, rh, 9.5), { x, y: cy, w: lw - 0.05, h: rh, valign: 'middle' });
            ops.push({ t: 'rect', x: x + lw, y: cy + rh / 2 - 0.04, w: tw, h: 0.08, fill: COL.tint, r: 0.04 });
            if (v) ops.push({ t: 'rect', x: x + lw, y: cy + rh / 2 - 0.04, w: Math.max(0.08, tw * Math.min(v, max) / max), h: 0.08, fill: COL.green, r: 0.04 });
            T(`${(Math.round(v * 10) / 10).toLocaleString('en-US')}${b.unit || ''}`, { x: x + lw + tw + 0.06, y: cy, w: vw + 0.04, h: rh, bold: true, align: 'right', valign: 'middle' }); });
        return;
    }
    case 'list': {
        if (!b.items.length) return empty(b.empty);
        let cy = y, shown = 0;
        const tagW = b.items.some(it => it.tag) ? 0.78 : 0;
        for (const it of b.items) {
            const tw = w - 0.18 - tagW, lines = Math.min(6, Math.max(1, Math.ceil(str(it.text).length * 9.5 * CHAR / tw)));
            const hh = it.head ? 0.19 : 0, th = 0.165 * lines + 0.03, subL = it.sub ? Math.min(2, Math.ceil(str(it.sub).length * 8 * CHAR / (w - 0.18))) : 0, subH = subL * 0.15, nh = it.note ? 0.17 : 0;
            const need = hh + th + subH + nh;
            if (shown && cy + need > y + h - (b.more ? 0.2 : 0)) break;
            ops.push({ t: 'rect', x, y: cy + 0.07, w: 0.085, h: 0.085, fill: it.tone === 'warn' ? 'F79009' : it.tone === 'bad' ? COL.rust : it.tone === 'good' ? COL.green : '98A2B3', r: 0.0425 });
            if (it.head) T(fitLine(it.head, tw, 9.5), { x: x + 0.18, y: cy, w: tw, h: 0.18, bold: true });
            T(fitLines(it.text, tw, th, 9.5), { x: x + 0.18, y: cy + hh, w: tw, h: th });
            if (it.tag) { const tc = it.tag.tone === 'warn' ? COL.amber : it.tag.tone === 'bad' ? COL.rust : COL.good, bg = it.tag.tone === 'warn' ? 'FFFAEB' : it.tag.tone === 'bad' ? 'FEF3F2' : 'ECFDF3';
                ops.push({ t: 'rect', x: x + w - tagW + 0.06, y: cy + hh + 0.01, w: tagW - 0.06, h: 0.19, fill: bg, r: 0.09 });
                T(fitLine(it.tag.text, tagW - 0.1, 7.5), { x: x + w - tagW + 0.06, y: cy + hh + 0.01, w: tagW - 0.06, h: 0.19, size: 7.5, bold: true, color: tc, align: 'center', valign: 'middle' }); }
            if (it.sub) T(fitLines(it.sub, w - 0.18, subH, 8), { x: x + 0.18, y: cy + hh + th, w: w - 0.18, h: subH, size: 8, color: COL.slate });
            if (it.note) T(fitLine(it.note, w - 0.18, 8), { x: x + 0.18, y: cy + hh + th + subH, w: w - 0.18, h: 0.17, size: 8, bold: true, color: COL.amber });
            cy += need + 0.09; shown++;
        }
        const left = (b.more || 0) + b.items.length - shown;
        if (left) T(`+ ${left} more in the dashboard`, { x, y: cy, w, h: 0.2, size: 8.5, color: COL.slate });
        return;
    }
    case 'table': {
        if (!b.rows.length) return empty(b.empty);
        // 2.39.3: a long cell wraps (up to three lines) and its row grows, instead of being cut.
        // Long tables (a big team) get tighter rows before any is left out.
        const base = Math.max(0.19, Math.min(0.25, h / (b.rows.length + 1))), size = base < 0.23 ? 8 : 9, cols = tableCols(b, w, size), lh = size * 1.22 / 72;
        const linesOf = r => Math.max(1, ...r.map((c, i) => Math.min(3, Math.ceil(str(plain(c).text).length * size * CHAR / Math.max(0.2, cols[i] - 0.1)))));
        const hs = b.rows.map(r => Math.max(base, linesOf(r) * lh + 0.07));
        let fit = b.rows.length;
        const used = n => base + hs.slice(0, n).reduce((a2, v) => a2 + v, 0) + (n < b.rows.length ? 0.22 : 0);
        while (fit > 1 && used(fit) > h) fit--;
        const row = (cells, cy, rh, head) => { let cx = x;
            cells.forEach((c, i) => { const o = plain(c), cw = cols[i], a = o.a || b.head[i].a || 'left', fs = head ? 7.5 : size;
                const txt = head ? fitLine(str(o.text).toUpperCase(), cw - 0.1, fs) : fitLines(o.text, cw - 0.1, rh - 0.04, fs);
                T(txt, { x: cx + 0.05, y: cy, w: cw - 0.1, h: rh, size: fs, bold: head || !!o.bold, color: head ? COL.slate : o.tone ? TONE[o.tone] || COL.ink : COL.ink, align: a, valign: 'middle' });
                cx += cw; }); };
        row(b.head.map(hd => ({ text: hd.t, a: hd.a })), y, base, true);
        let cy = y + base;
        b.rows.slice(0, fit).forEach((r, k) => { ops.push({ t: 'line', x, y: cy, w, h: 0, color: sep, width: 0.5 }); row(r, cy, hs[k], false); cy += hs[k]; });
        const more = (b.more || 0) + b.rows.length - fit;
        if (more) T(`+ ${more} more in the dashboard`, { x, y: cy + 0.03, w, h: 0.2, size: 8.5, color: COL.slate });
        return;
    }
    case 'gallery': {
        galleryGeom(b.items.length, x, y, w, h).forEach((g, i) => {
            const it = b.items[i];
            if (it.data) ops.push({ t: 'img', data: it.data, x: g.x, y: g.y, w: g.w, h: g.h });
            else { ops.push({ t: 'rect', x: g.x, y: g.y, w: g.w, h: g.h, fill: 'E7ECEA', r: 0.08 }); T('Photo in the dashboard', { x: g.x, y: g.y, w: g.w, h: g.h, size: 9, color: COL.slate, align: 'center', valign: 'middle' }); }
            T(fitLine(it.cap, g.w, 9), { x: g.x, y: g.y + g.h + 0.05, w: g.w, h: 0.2, size: 9, bold: true });
            T(fitLine(it.sub, g.w, 8), { x: g.x, y: g.y + g.h + 0.25, w: g.w, h: 0.18, size: 8, color: COL.slate });
        });
        return;
    }
    }
}
function pageOps(pg) {
    const ops = [];
    const T = (s, o) => ops.push(Object.assign({ t: 'text', s: str(s), size: 10, color: COL.ink, align: 'left', valign: 'top' }, o));
    if (pg.kind === 'cover') {
        ops.push({ t: 'bg', key: 'cover' }, { t: 'img', key: 'logo', x: 5.74, y: 0.24, w: 0.65, h: 0.65 });
        T(pg.title + (pg.title2 ? '\n' + pg.title2 : ''), { x: 3.7, y: 1.83, w: 6.05, h: 1.27, size: 24, bold: true, color: COL.msg, valign: 'middle' });
        T(str(pg.lines && pg.lines[0]).toUpperCase(), { x: 3.7, y: 3.1, w: 5.6, h: 0.24, size: 8.5, bold: true, color: COL.msg });
        return ops;
    }
    if (pg.kind === 'closing') {
        ops.push({ t: 'bg', key: 'cover' });
        T(pg.title, { x: 1.83, y: 1.3, w: 6.67, h: 1.43, size: 37.5, bold: true, color: '000000', align: 'center', valign: 'middle' });
        return ops;
    }
    ops.push({ t: 'bg', key: 'content' });
    T(fitLine(pg.title, 8.55, 24), { x: 0.39, y: 0.12, w: 8.55, h: 0.58, size: 24, bold: true, color: COL.msg, valign: 'middle' });
    if (pg.sub) T(fitLine(pg.sub, 9.1, 10.5), { x: 0.39, y: 0.82, w: 9.1, h: 0.22, size: 10.5, color: COL.slate });
    if (pg.strip) T(fitLine('o  ' + pg.strip, W0, 9), { x: X0, y: pg.sub ? 1.12 : 0.92, w: W0, h: 0.22, size: 9, color: COL.slate });
    // The largest zoom (up to 1.6) at which everything still fits; the cards' shapes are drawn that much larger.
    let z = 1;
    for (let k = 1.6; k > 1; k -= 0.05) { if (layout(pg, k).fits) { z = k; break; } }
    const L = layout(pg, z), from = ops.length;
    L.forEach(c => {
        const b = c.b;
        if (b.type === 'gallery') return drawBlock(ops, b, c.x, c.y, c.w, c.h, false);
        const warn = b.card === 'warn';
        ops.push({ t: 'rect', x: c.x, y: c.y, w: c.w, h: c.h, fill: warn ? COL.warnBg : COL.card, line: warn ? COL.warnLine : COL.hair, r: 0.1 });
        let iy = c.y + PAD;
        if (b.title) { T(fitLine(str(b.title).toUpperCase(), c.w - PAD * 2, 7.5), { x: c.x + PAD, y: iy, w: c.w - PAD * 2, h: 0.2, size: 7.5, bold: true, color: warn ? COL.amber : '475467' }); iy += 0.26; }
        drawBlock(ops, b, c.x + PAD, iy, c.w - PAD * 2, Math.max(0.2, c.y + c.h - PAD - iy), warn);
    });
    if (z !== 1) for (let i = from; i < ops.length; i++) { const o = ops[i];
        o.x = X0 + (o.x - X0) * z; o.y = L.y0 + (o.y - L.y0) * z; o.w *= z; o.h *= z;
        if (o.size) o.size *= z; if (o.r) o.r *= z; }
    return ops;
}

function assets() {
    const rd = f => fs.readFileSync(path.join(__dirname, 'assets', f));
    return { cover: rd('msg-cover.jpg'), content: rd('msg-content.jpg'), logo: rd('ot-logo.png') };
}
const dataUrl = (buf, type) => `${type || 'image/jpeg'};base64,${buf.toString('base64')}`;

// pages → PowerPoint (Buffer)
async function renderPptx(pages, opts) {
    opts = opts || {};
    const PptxGenJS = require('pptxgenjs');
    const A = opts.assets || assets(), pres = new PptxGenJS();
    pres.layout = 'LAYOUT_16x9';
    pres.title = opts.title || 'Daily Report';
    pres.company = 'Operation Technology';
    const bg = { cover: dataUrl(A.cover), content: dataUrl(A.content) }, logo = dataUrl(A.logo, 'image/png');
    pages.forEach(pg => {
        const s = pres.addSlide();
        pageOps(pg).forEach(o => {
            if (o.t === 'bg') s.background = { data: bg[o.key] };
            else if (o.t === 'rect') s.addShape(o.r ? pres.ShapeType.roundRect : pres.ShapeType.rect, Object.assign({ x: o.x, y: o.y, w: o.w, h: o.h, fill: { color: o.fill },
                line: o.line ? { color: o.line, width: 0.75 } : { color: o.fill, width: 0 } }, o.r ? { rectRadius: o.r } : {}));
            else if (o.t === 'line') s.addShape(pres.ShapeType.line, { x: o.x, y: o.y, w: o.w, h: o.h, line: { color: o.color, width: o.width || 0.5 } });
            else if (o.t === 'img') s.addImage({ data: o.key === 'logo' ? logo : dataUrl(o.data), x: o.x, y: o.y, w: o.w, h: o.h });
            else if (o.t === 'text') s.addText(o.s, { x: o.x, y: o.y, w: o.w, h: o.h, margin: 0, fontFace: 'Calibri', fontSize: o.size, bold: !!o.bold, color: o.color, align: o.align, valign: o.valign });
        });
    });
    return pres.write({ outputType: 'nodebuffer' });
}

// pages → PDF (Buffer). Standard Helvetica: letters it cannot draw are swapped.
const PDF_SAFE = s => s.replace(/→/g, '->').replace(/[✓✔]/g, 'v').replace(/[✗✘]/g, 'x').replace(/[^\x00-\x7E -ÿ–—‘’“”•…€]/g, '');
function renderPdf(pages, opts) {
    opts = opts || {};
    const PDFDocument = require('pdfkit');
    const A = opts.assets || assets(), P = 72;
    const doc = new PDFDocument({ size: [10 * P, 5.625 * P], margin: 0, autoFirstPage: false, info: { Title: opts.title || 'Daily Report', Author: 'Operation Technology' } });
    const chunks = [];
    doc.on('data', c => chunks.push(c));
    const done = new Promise((res, rej) => { doc.on('end', () => res(Buffer.concat(chunks))); doc.on('error', rej); });
    pages.forEach(pg => {
        doc.addPage();
        pageOps(pg).forEach(o => {
            if (o.t === 'bg') doc.image(A[o.key], 0, 0, { width: 10 * P, height: 5.625 * P });
            else if (o.t === 'rect') {
                (o.r ? doc.roundedRect(o.x * P, o.y * P, o.w * P, o.h * P, o.r * P) : doc.rect(o.x * P, o.y * P, o.w * P, o.h * P));
                if (o.line) doc.lineWidth(0.75).fillAndStroke('#' + o.fill, '#' + o.line); else doc.fill('#' + o.fill);
            } else if (o.t === 'line') doc.moveTo(o.x * P, o.y * P).lineTo((o.x + o.w) * P, (o.y + o.h) * P).lineWidth(o.width || 0.5).stroke('#' + o.color);
            else if (o.t === 'img') { try { doc.image(o.key === 'logo' ? A.logo : o.data, o.x * P, o.y * P, { width: o.w * P, height: o.h * P }); } catch (_) { /* a picture pdfkit cannot read is left out */ } }
            else if (o.t === 'text') {
                const font = o.bold ? 'Helvetica-Bold' : 'Helvetica', size = o.size, text = PDF_SAFE(o.s);
                doc.font(font).fontSize(size).fillColor('#' + o.color);
                const th = doc.heightOfString(text || ' ', { width: o.w * P, lineGap: 0 });
                const y = o.valign === 'middle' ? o.y * P + Math.max(0, (o.h * P - th) / 2) : o.valign === 'bottom' ? o.y * P + Math.max(0, o.h * P - th) : o.y * P;
                doc.text(text, o.x * P, y, { width: o.w * P, height: Math.max(o.h * P, size * 1.3), align: o.align === 'right' ? 'right' : o.align === 'center' ? 'center' : 'left', lineBreak: true, ellipsis: true });
            }
        });
    });
    doc.end();
    return done;
}

module.exports = { renderPptx, renderPdf, pageOps, layout, fitLine, fitLines, galleryGeom };
