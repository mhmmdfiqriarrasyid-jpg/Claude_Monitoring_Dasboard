// 2.23.0 — data/agri-facts.json, the daily sugarcane facts. Guards what the
// scheduled task adds every morning (rules in CLAUDE.md): complete, sourced,
// https, never Wikipedia, one id each, short, dated, at most one per day.
const fs = require('fs'); const path = require('path');
const T = []; const t = (n, g, w) => T.push({ n, g, w, pass: JSON.stringify(g) === JSON.stringify(w) });
let data = null;
try { data = JSON.parse(fs.readFileSync(path.join(__dirname, '..', 'data', 'agri-facts.json'), 'utf8')); } catch (e) { data = null; }
t('file terbaca sebagai JSON dengan daftar facts', !!(data && Array.isArray(data.facts)), true);
const facts = (data && data.facts) || [];
const bad = (name, fn) => facts.filter(fn).map(f => f.id || '(no id)');
t('setiap materi lengkap (id, term, text, source, url, addedAt)', bad('', f => !f.id || !f.term || !f.text || !f.source || !f.url || !f.addedAt), []);
t('id unik', facts.map(f => f.id).filter((x, i, a) => a.indexOf(x) !== i), []);
t('id huruf kecil-angka-strip', bad('', f => !/^[a-z0-9-]{3,60}$/.test(f.id || '')), []);
t('tautan https', bad('', f => !/^https:\/\/[^\s"'<>]+\.[^\s"'<>]+$/.test(f.url || '')), []);
t('tanpa Wikipedia', bad('', f => /wikipedia\.org|wikimedia\.org|wiki\b/i.test(`${f.url} ${f.source}`)), []);
t('panjang wajar (term ≤ 80, text ≤ 400, source ≤ 120)', bad('', f => String(f.term).length > 80 || String(f.text).length > 400 || String(f.source).length > 120), []);
t('tanggal YYYY-MM-DD', bad('', f => !/^\d{4}-\d{2}-\d{2}$/.test(f.addedAt || '')), []);
const counts = {}; facts.forEach(f => { counts[f.addedAt] = (counts[f.addedAt] || 0) + 1; });
const first = facts.map(f => f.addedAt).sort()[0];
t('setelah tanggal awal, paling banyak satu materi per hari', Object.entries(counts).filter(([d, n]) => d !== first && n > 1).map(([d]) => d), []);
t('tidak ada tanggal di masa depan', bad('', f => f.addedAt > new Date(Date.now() + 86400000).toISOString().slice(0, 10)), []);
t('istilah tidak dobel', facts.map(f => String(f.term).toLowerCase()).filter((x, i, a) => a.indexOf(x) !== i), []);
let pass = 0;
T.forEach(x => { if (x.pass) pass++; console.log(`${x.pass ? 'PASS' : 'FAIL'}  ${x.n}${x.pass ? '' : `\n        dapat=${JSON.stringify(x.g)} harap=${JSON.stringify(x.w)}`}`); });
console.log(`\n${pass}/${T.length} lulus`);
process.exit(pass === T.length ? 0 : 1);
