// 2.46.0 — firestore.rules for BAST (bast, bastKeys, bastSeq) in the Firestore emulator.
// Not part of npm test (needs Java + firebase-tools):
//   npx firebase emulators:exec --only firestore --project demo-ot "node tests/bast_rules_test.js"
// A letter's number and S/N must never be used twice; only the owner and Warehouse
// editors may read or write letters; the app's own transaction must still pass.
const fs = require('fs');
const path = require('path');
const { initializeTestEnvironment, assertSucceeds, assertFails } = require('@firebase/rules-unit-testing');
const { doc, setDoc, getDoc, updateDoc, deleteDoc, writeBatch, runTransaction } = require('firebase/firestore');

const T = [];
async function check(name, p, expectOk) {
    try { if (expectOk) await assertSucceeds(p); else await assertFails(p); T.push({ name, pass: true }); }
    catch (e) { T.push({ name, pass: false, err: String(e && e.message || e).slice(0, 160) }); }
}
const key = (f, v) => `${f}_${String(v).trim().toUpperCase().replace(/[^A-Z0-9-]/g, '_')}`;
const letter = (id, no, sn, extra) => Object.assign({ id, no, sn, noKey: key('no', no), snKey: key('sn', sn), date: '2026-06-30', status: 'active',
    p2Name: 'Andi', itemName: 'HT', createdAt: 1 }, extra || {});
// The writes of firebase-init.js createBast, as one batch.
function make(db, b, extra) {
    const w = writeBatch(db);
    w.set(doc(db, 'bastKeys', b.noKey), { bastId: b.id, field: 'no', value: b.no, at: 1 });
    w.set(doc(db, 'bastKeys', b.snKey), { bastId: b.id, field: 'sn', value: b.sn, at: 1 });
    (extra || []).forEach(([f, v]) => w.set(doc(db, 'bastKeys', key(f, v)), { bastId: b.id, field: f, value: v, at: 1 }));
    w.set(doc(db, 'bast', b.id), b);
    return w.commit();
}

(async () => {
    const env = await initializeTestEnvironment({ projectId: 'demo-ot', firestore: { rules: fs.readFileSync(path.join(__dirname, '..', 'firestore.rules'), 'utf8'), host: '127.0.0.1', port: 8181 } });
    await env.withSecurityRulesDisabled(async ctx => {
        const db = ctx.firestore();
        await setDoc(doc(db, 'users', 'owner'), { role: 'owner', status: 'active' });
        await setDoc(doc(db, 'users', 'wed'), { role: 'staff', status: 'active', access: { warehouse: 'edit' } });
        await setDoc(doc(db, 'users', 'wview'), { role: 'staff', status: 'active', access: { warehouse: 'view' } });
        await setDoc(doc(db, 'users', 'other'), { role: 'staff', status: 'active', access: { damage: 'edit' } });
    });
    const as = uid => env.authenticatedContext(uid).firestore();
    const W = as('wed'), O = as('owner'), V = as('wview'), X = as('other');

    // ---- making letters ----
    const b1 = letter('bast_1', 'GPA/BAST-HT/06/2026/001', '25D05C0567');
    await check('editor: a letter with its number and S/N keys', make(W, b1, [['rid', '1009']]), true);
    await check('editor: the counter', setDoc(doc(W, 'bastSeq', 'GPA_HT_2026'), { last: 1, updatedAt: 1 }), true);
    await check('dobel: another letter with the same number', make(W, letter('bast_2', 'GPA/BAST-HT/06/2026/001', 'OTHER-1')), false);
    await check('dobel: same number, other case and spacing', make(W, letter('bast_2', ' gpa/bast-ht/06/2026/001', 'OTHER-1')), false);
    await check('dobel: another letter for the same S/N', make(W, letter('bast_3', 'GPA/BAST-HT/06/2026/002', '25d05c0567')), false);
    await check('dobel: the same Radio ID', make(W, letter('bast_4', 'GPA/BAST-HT/06/2026/003', 'SN-4'), [['rid', '1009']]), false);
    await check('a letter without its keys', setDoc(doc(W, 'bast', 'bast_5'), letter('bast_5', 'GPA/BAST-HT/06/2026/005', 'SN-5')), false);
    await check('a letter whose key names another letter', (async () => {
        await setDoc(doc(W, 'bastKeys', key('no', 'X/1')), { bastId: 'bast_zz', field: 'no', value: 'X/1', at: 1 });
        await setDoc(doc(W, 'bastKeys', key('sn', 'SN-6')), { bastId: 'bast_zz', field: 'sn', value: 'SN-6', at: 1 });
        return setDoc(doc(W, 'bast', 'bast_6'), letter('bast_6', 'X/1', 'SN-6'));
    })(), false);
    await check('a key under the wrong id', setDoc(doc(W, 'bastKeys', 'no_SOMETHING_ELSE'), { bastId: 'bast_7', field: 'no', value: 'X/7', at: 1 }), false);
    await check('a key changed later', setDoc(doc(W, 'bastKeys', b1.noKey), { bastId: 'bast_9', field: 'no', value: b1.no, at: 2 }), false);
    await check('a letter with a forged noKey', make(W, Object.assign(letter('bast_8', 'GPA/BAST-HT/06/2026/008', 'SN-8'), { noKey: key('no', 'OTHER') })), false);
    await check('a letter made already ended', make(W, letter('bast_8', 'GPA/BAST-HT/06/2026/008', 'SN-8', { status: 'returned' })), false);
    await check('the counter going back', setDoc(doc(W, 'bastSeq', 'GPA_HT_2026'), { last: 1, updatedAt: 2 }), false);
    await check('the counter going on', setDoc(doc(W, 'bastSeq', 'GPA_HT_2026'), { last: 2, updatedAt: 2 }), true);

    // ---- who ----
    await check('Warehouse editor reads letters', getDoc(doc(W, 'bast', 'bast_1')), true);
    await check('owner reads letters', getDoc(doc(O, 'bast', 'bast_1')), true);
    await check('Warehouse viewer does not', getDoc(doc(V, 'bast', 'bast_1')), false);
    await check('another area does not', getDoc(doc(X, 'bast', 'bast_1')), false);
    await check('Warehouse viewer cannot make a letter', make(V, letter('bast_10', 'GPA/BAST-HT/06/2026/010', 'SN-10')), false);
    await check('owner makes a letter', make(O, letter('bast_11', 'GPA/BAST-HT/06/2026/011', 'SN-11')), true);

    // ---- ending ----
    await check('the number cannot change', updateDoc(doc(W, 'bast', 'bast_1'), { no: 'GPA/BAST-HT/06/2026/099', noKey: key('no', 'GPA/BAST-HT/06/2026/099') }), false);
    await check('returned', updateDoc(doc(W, 'bast', 'bast_1'), { status: 'returned', endedAt: '2026-11-20' }), true);
    await check('returned letter cannot come back to active', updateDoc(doc(W, 'bast', 'bast_1'), { status: 'active' }), false);
    await check('its S/N key is given back', deleteDoc(doc(W, 'bastKeys', b1.snKey)), true);
    await check('its number key never is', deleteDoc(doc(W, 'bastKeys', b1.noKey)), false);
    await check('the S/N gets a new letter after the return', make(W, letter('bast_12', 'GPA/BAST-HT/11/2026/001', '25D05C0567')), true);
    await check('only the owner deletes a letter', deleteDoc(doc(W, 'bast', 'bast_12')), false);

    // ---- reset numbering (owner only, 2.46.6) ----
    await check('reset: a Warehouse editor cannot release a number key', deleteDoc(doc(W, 'bastKeys', key('no', 'GPA/BAST-HT/06/2026/011'))), false);
    await check('reset: nor set a counter back', setDoc(doc(W, 'bastSeq', 'GPA_HT_2026'), { last: 1, updatedAt: 9 }), false);
    await check('reset: the owner releases a number key', deleteDoc(doc(O, 'bastKeys', key('no', 'X/1'))), true);
    await check('reset: the owner sets a counter back', setDoc(doc(O, 'bastSeq', 'GPA_HT_2026'), { last: 2, updatedAt: 9 }), true);
    await check('reset: the owner removes a counter', (async () => { await setDoc(doc(O, 'bastSeq', 'TMP_HT_2026'), { last: 5 }); return deleteDoc(doc(O, 'bastSeq', 'TMP_HT_2026')); })(), true);

    // ---- the app's own transaction (firebase-init.js createBast) ----
    const tx = (db, rec, prefix, seqKey) => runTransaction(db, async t => {
        const out = { ...rec };
        const seqRef = doc(db, 'bastSeq', seqKey);
        const s = await t.get(seqRef);
        let next = (s.exists() ? s.data().last : 0) + 1;
        for (let i = 0; i < 60; i++, next++) { const c = `${prefix}/${String(next).padStart(3, '0')}`; if (!(await t.get(doc(db, 'bastKeys', key('no', c)))).exists()) { out.no = c; break; } }
        const k = await t.get(doc(db, 'bastKeys', key('sn', out.sn)));
        if (k.exists()) throw Object.assign(new Error('dup'), { code: 'bast/dup' });
        out.noKey = key('no', out.no); out.snKey = key('sn', out.sn);
        t.set(seqRef, { last: next, updatedAt: 3 });
        t.set(doc(db, 'bastKeys', out.noKey), { bastId: out.id, field: 'no', value: out.no, at: 3 });
        t.set(doc(db, 'bastKeys', out.snKey), { bastId: out.id, field: 'sn', value: out.sn, at: 3 });
        t.set(doc(db, 'bast', out.id), out);
        return out;
    });
    let n1 = '';
    await check('transaction: numbered after the taken ones', tx(W, letter('bast_13', '', 'SN-13'), 'GPA/BAST-HT/06/2026', 'GPA_HT_2026').then(o => { n1 = o.no; }), true);
    T.push({ name: 'transaction: skips 001 (taken) and 002 (counter) → 003', pass: n1 === 'GPA/BAST-HT/06/2026/003', err: n1 });
    const again = await tx(W, letter('bast_14', '', 'SN-13'), 'GPA/BAST-HT/06/2026', 'GPA_HT_2026').then(() => 'made', e => e.code || 'error');
    T.push({ name: 'transaction: the same S/N again is refused (bast/dup)', pass: again === 'bast/dup', err: again });

    let pass = 0;
    T.forEach(x => { if (x.pass) pass++; console.log(`${x.pass ? 'PASS' : 'FAIL'}  ${x.name}${x.pass ? '' : '  — ' + x.err}`); });
    console.log(`\n${pass}/${T.length} lulus`);
    await env.cleanup();
    process.exit(pass === T.length ? 0 : 1);
})().catch(e => { console.error(e); process.exit(1); });
