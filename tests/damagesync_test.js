// v162 — kerusakan ↔ status unit alat berat.
// Catatan kerusakan yang masih terbuka MENAHAN status unit (atau satu
// komponen, untuk Device Precision) di Breakdown; unit kembali Good hanya saat
// catatan TERAKHIR yang menahannya selesai atau dihapus. Breakdown yang diset
// manual tidak diambil alih. Diuji: tambah, gabung, selesai, hapus (satu dan
// massal), edit (pindah unit, ganti tipe, centang/tidak), buka lagi, "sudah
// diperbaiki", akun yang hanya boleh edit Damage (tulisan parsial), tulisan
// yang ditolak server, set Good manual, Periksa Data + perbaikannya, penanda
// di tabel, dan traktor yang tidak berubah.
const { launch, BASE_URL } = require('./_env');

(async () => {
    const b = await launch();
    const page = await b.newPage({ viewport: { width: 1440, height: 900 } });
    const errors = [];
    page.on('pageerror', e => { if (!/Chart is not defined/.test(e.message)) errors.push(e.message); });
    await page.goto(BASE_URL + '/index.html', { waitUntil: 'load' });
    await page.waitForTimeout(1000);

    await page.addScriptTag({ content: `(async () => { try {
        const T = []; const t = (n, g, w) => T.push({ n, g, w, pass: JSON.stringify(g) === JSON.stringify(w) });
        const tick = ms => new Promise(r => setTimeout(r, ms || 20));
        const toasts = []; window.showToast = m => toasts.push(String(m));
        let confirmAnswer = true, asked = [];
        window.confirm = m => { asked.push(String(m)); return confirmAnswer; };
        const ok = () => Promise.resolve();
        const partials = [], whole = [], recs = [];
        let refuseUnits = null;
        window.cloud = { isReady: true,
            saveDamage: r => { recs.push(JSON.parse(JSON.stringify(r))); return ok(); },
            saveDamages: ok, deleteDamage: ok,
            updateUnitsFields: items => { partials.push(JSON.parse(JSON.stringify(items))); return refuseUnits ? Promise.reject(refuseUnits) : ok(); },
            saveUnits: units => { whole.push(JSON.parse(JSON.stringify(units))); return refuseUnits ? Promise.reject(refuseUnits) : ok(); },
            deleteUnits: ok, getAllUnits: () => Promise.resolve(JSON.parse(JSON.stringify(globalData))),
            addHistoryEvents: ok, subscribeUsers: () => () => {},
            saveDamagePhoto: ok, deleteDamagePhoto: ok, saveDamageAfterPhoto: ok, deleteDamageAfterPhoto: ok,
            getDamagePhoto: () => Promise.resolve(''), getDamageAfterPhoto: () => Promise.resolve('') };
        const asOwner = () => { currentUser = { uid: 'uO', email: 'o@x.id' }; currentUserDoc = { role: 'owner', status: 'active' }; applyRoleGating(); };
        asOwner(); hideAuthGates();
        const today = toISODate();
        const daysAgo = n => toISODate(new Date(Date.now() - n * 86400000));
        const H = (id, x) => Object.assign({ id, name: 'EX-' + id, sn: 'SN' + id, model: 'PC200', unitGroup: 'heavy', brand: 'Komatsu',
            status: 'Good', cameraAi: 'Good', telematicBox: 'Good', switchLimiter: 'Good', rotaryLamp: 'Good',
            site: 'PT. GPA', breakdownReason: '', downtimeHistory: [] }, x || {});
        globalData = [
            H('01'), H('02', { status: 'Breakdown', breakdownReason: 'Engine overhaul', breakdownStartedAt: Date.now() - 86400000 }),
            H('03'), H('04'), H('05'), H('06'),
            { id: 'T1', name: 'TR-01', sn: 'SNT1', model: '6110B', status: 'Good', display: 'Good', gps: 'Good', steering: 'Good', jdlink: 'Good', site: 'PT. GPA' }
        ];
        globalDamages = [];
        const U = id => globalData.find(u => u.id === id);
        const D = id => globalDamages.find(d => d.id === id);
        const last = () => globalDamages[globalDamages.length - 1];
        const add = (unitId, type, comp, desc, opts) => {
            opts = opts || {};
            showAddDamageForm();
            document.getElementById('dmgUnit').value = damageUnitLabel(U(unitId));
            onDamageUnitChanged();
            document.getElementById('dmgType').value = type;
            const sel = document.getElementById('dmgComponent');
            if (comp && ![...sel.options].some(o => o.value === comp)) sel.insertAdjacentHTML('beforeend', '<option value="' + comp + '">' + comp + '</option>');
            sel.value = comp || '';
            document.getElementById('dmgDescription').value = desc || '';
            document.getElementById('dmgDate').value = opts.date || today;
            document.getElementById('dmgSetBreakdown').checked = opts.untick ? false : true;
            if (opts.repaired) {
                document.getElementById('dmgAlreadyRepaired').checked = true; onDamageAlreadyRepairedChange();
                document.getElementById('dmgRepairedDate').value = opts.repaired;
            }
            saveDamage({ preventDefault() {} });
            return last();
        };
        const keysOk = list => list.every(p => Object.keys(p).every(k => k === 'id' || DAMAGE_LINK_FIELDS.includes(k)));

        // =============== damage pertama: unit Good → Breakdown ===============
        partials.length = 0; toasts.length = 0;
        const a1 = add('01', 'Mekanis', 'Hidrolik', 'Selang bocor');
        t('Mechanical pada alat berat Good: status Breakdown + alasan + id kerusakan',
          [U('01').status, U('01').breakdownReason, U('01').breakdownDamageId, a1.drove],
          ['Breakdown', 'Mekanis / Hidrolik: Selang bocor', a1.id, 'status']);
        t('unit dikirim sebagai tulisan parsial berisi field status saja',
          [partials.length, keysOk(partials.flat()), partials.flat().some(p => 'name' in p || 'site' in p)], [1, true, false]);
        t('pesan: status unit di-set Breakdown', toasts.includes('Unit status "EX-01" set to Breakdown'), true);
        t('catatan terkirim dulu, lalu unit', recs.length >= 1 && recs[recs.length - 1].id === a1.id, true);

        // =============== Device Precision komponen: hanya komponen ===============
        const c3 = add('03', 'Device Precision', 'Camera AI', 'Lensa retak');
        t('Device Precision / Camera AI: hanya komponen Breakdown, status unit tetap Good',
          [U('03').status, U('03').cameraAi, c3.drove], ['Good', 'Breakdown', 'cameraAi']);
        const c1 = add('01', 'Device Precision', 'Rotary Lamp', 'Mati');
        t('komponen pada unit yang sudah Breakdown: komponen ikut Breakdown', [U('01').rotaryLamp, c1.drove], ['Breakdown', 'rotaryLamp']);

        // =============== damage kedua: bergabung, alasan tetap ===============
        toasts.length = 0;
        const a2 = add('01', 'Software', '', 'Error ECU');
        t('damage kedua pada unit Breakdown: ikut menahan, alasan pertama tetap',
          [a2.drove, U('01').breakdownReason, U('01').breakdownDamageId], ['status', 'Mekanis / Hidrolik: Selang bocor', a1.id]);
        t('pesan: tetap Breakdown sampai damage ini selesai', toasts.some(m => /already Breakdown — it stays Breakdown until this damage is done too/.test(m)), true);

        // =============== selesai: tetap Breakdown selama masih ada yang terbuka ===============
        toasts.length = 0;
        resolveDamage(a1.id, { confirmed: true });
        t('selesai yang pertama: tetap Breakdown, alasan pindah ke damage berikutnya',
          [U('01').status, U('01').breakdownDamageId, U('01').breakdownReason, toasts.pop()],
          ['Breakdown', a2.id, 'Software: Error ECU', 'Damage marked as done — unit status not restored because other damage is still open']);
        const dh0 = (U('01').downtimeHistory || []).length;
        partials.length = 0;
        resolveDamage(a2.id, { confirmed: true });
        t('selesai yang terakhir: Good, alasan dan id kosong, downtime tercatat',
          [U('01').status, U('01').breakdownReason, U('01').breakdownDamageId, U('01').downtimeHistory.length, toasts.pop()],
          ['Good', '', '', dh0 + 1, 'Damage marked as done — unit status restored']);
        // Interval downtime harus ikut terkirim, kalau tidak snapshot berikutnya menghapusnya.
        const sentGood = partials.flat().find(p => p.id === '01' && p.status === 'Good');
        t('interval downtime ikut dalam tulisan ke server', [!!sentGood, sentGood && Array.isArray(sentGood.downtimeHistory) ? sentGood.downtimeHistory.length : -1], [true, dh0 + 1]);
        resolveDamage(c1.id, { confirmed: true });
        t('komponen kembali Good saat damage komponennya selesai', U('01').rotaryLamp, 'Good');

        // =============== Breakdown manual tidak diambil alih ===============
        toasts.length = 0;
        const m2 = add('02', 'Mekanis', 'Mesin', 'Bunyi kasar');
        t('unit Breakdown manual: alasan tidak ditimpa, catatan tidak menahan',
          [U('02').breakdownReason, m2.drove, toasts.some(m => /set by hand/.test(m))], ['Engine overhaul', '', true]);
        resolveDamage(m2.id, { confirmed: true });
        t('selesai: unit Breakdown manual tetap Breakdown', U('02').status, 'Breakdown');

        // =============== tidak dicentang: tidak menahan, tidak menghalangi ===============
        const u4a = add('04', 'Mekanis', '', 'A');
        const u4b = add('04', 'Mekanis', '', 'B (minor)', { untick: true });
        t('tidak dicentang: drove kosong dan noHold', [u4b.drove, u4b.noHold], ['', true]);
        resolveDamage(u4a.id, { confirmed: true });
        t('catatan yang tidak dicentang tidak menghalangi kembali Good', U('04').status, 'Good');
        t('Periksa Data tidak menanyakan catatan yang sengaja tidak dicentang', dcDamageStatusSync().filter(i => i.unitId === '04').length, 0);

        // =============== edit: pindah unit, ganti tipe, centang ===============
        const e1 = add('05', 'Mekanis', '', 'Pompa');
        editDamage(e1.id); await tick(10);
        t('edit catatan terbuka: kotak "set Breakdown" tampil dan tercentang',
          [document.getElementById('dmgSetBreakdownGroup').style.display, document.getElementById('dmgSetBreakdown').checked], ['', true]);
        document.getElementById('dmgUnit').value = damageUnitLabel(U('06'));
        saveDamage({ preventDefault() {} });
        t('dipindah ke unit lain: unit lama Good, unit baru Breakdown', [U('05').status, U('06').status, D(e1.id).drove, U('06').breakdownDamageId], ['Good', 'Breakdown', 'status', e1.id]);
        editDamage(e1.id); await tick(10);
        document.getElementById('dmgType').value = 'Device Precision';
        const cs = document.getElementById('dmgComponent'); cs.value = 'Telematic Box';
        saveDamage({ preventDefault() {} });
        t('tipe diganti ke Device Precision / Telematic Box: status dilepas, komponen ditahan',
          [U('06').status, U('06').telematicBox, D(e1.id).drove], ['Good', 'Breakdown', 'telematicBox']);
        editDamage(e1.id); await tick(10);
        document.getElementById('dmgSetBreakdown').checked = false;
        saveDamage({ preventDefault() {} });
        t('centang dilepas: komponen kembali Good, catatan tidak menahan', [U('06').telematicBox, D(e1.id).drove, D(e1.id).noHold], ['Good', '', true]);
        editDamage(e1.id); await tick(10);
        document.getElementById('dmgSetBreakdown').checked = true;
        saveDamage({ preventDefault() {} });
        t('dicentang lagi: komponen Breakdown lagi', [U('06').telematicBox, D(e1.id).drove, D(e1.id).noHold], ['Breakdown', 'telematicBox', false]);
        const r1 = add('05', 'Mekanis', '', 'Lama');
        editDamage(r1.id); await tick(10);
        document.getElementById('dmgDescription').value = 'Baru';
        saveDamage({ preventDefault() {} });
        t('deskripsi diedit: alasan unit ikut berubah', U('05').breakdownReason, 'Mekanis: Baru');

        // =============== hapus: satu dan massal ===============
        asked = []; confirmAnswer = true;
        deleteDamage(r1.id);
        t('hapus catatan yang menahan: ditanya dengan efeknya, unit Good', [/goes back to Good/.test(asked[0] || ''), U('05').status], [true, 'Good']);
        const b1 = add('05', 'Mekanis', '', 'X1'), b2 = add('05', 'Software', '', 'X2');
        partials.length = 0; asked = [];
        selectedDamageIds.clear(); selectedDamageIds.add(b1.id); selectedDamageIds.add(b2.id);
        deleteSelectedDamages();
        t('hapus massal dua catatan satu unit: satu kali kembali Good',
          [U('05').status, /1 unit\\(s\\) go back to Good/.test(asked[0] || ''), partials.flat().filter(p => p.id === '05' && p.status === 'Good').length], ['Good', true, 1]);

        // =============== buka lagi ===============
        const o1 = add('05', 'Mekanis', '', 'Kopling');
        resolveDamage(o1.id, { confirmed: true });
        asked = [];
        reopenDamage(o1.id);
        t('buka lagi: unit kembali Breakdown, catatan terbuka',
          [U('05').status, D(o1.id).resolved, D(o1.id).drove, /goes back to Breakdown/.test(asked[0] || '')], ['Breakdown', false, 'status', true]);
        t('tombol Reopen ada di baris Done', (() => { resolveDamage(o1.id, { confirmed: true }); navigateTo('damage'); renderDamageTable();
            return !!document.querySelector('#damageBody button[onclick*="reopenDamage"]'); })(), true);

        // =============== sudah diperbaiki ===============
        partials.length = 0; toasts.length = 0;
        const fx = add('03', 'Mekanis', '', 'Sudah beres', { date: daysAgo(5), repaired: daysAgo(2) });
        t('"Already repaired": tercatat selesai, unit tidak disentuh',
          [fx.resolved, fx.resolvedAt, fx.drove, U('03').status, partials.length], [true, daysAgo(2), '', 'Good', 0]);
        const n0 = globalDamages.length; toasts.length = 0;
        add('03', 'Mekanis', '', 'Salah tanggal', { date: daysAgo(2), repaired: daysAgo(5) });
        t('tanggal perbaikan sebelum tanggal kerusakan: ditolak', [globalDamages.length, toasts.pop()], [n0, 'The repair date is before the damage date']);
        closeDamageModal(true);

        // =============== traktor: tetap seperti sebelumnya ===============
        const tg = add('T1', 'Device Precision', 'GPS', 'Sinyal');
        t('traktor Device Precision GPS: hanya gps', [U('T1').gps, U('T1').status, tg.drove], ['Breakdown', 'Good', 'gps']);
        resolveDamage(tg.id, { confirmed: true });
        t('traktor: gps kembali Good', U('T1').gps, 'Good');

        // =============== akun yang hanya boleh edit Damage ===============
        currentUserDoc = { role: 'staff', status: 'active', access: { damage: 'edit', editUnits: 'view' } }; applyRoleGating();
        partials.length = 0; whole.length = 0; toasts.length = 0;
        const s1 = add('04', 'Mekanis', '', 'Oleh staf damage');
        t('akun Damage-only: unit tetap Breakdown (dulu diam-diam gagal)', [U('04').status, s1.drove], ['Breakdown', 'status']);
        t('akun Damage-only: hanya field status yang dikirim, bukan dokumen unit utuh',
          [partials.length, whole.length, keysOk(partials.flat())], [1, 0, true]);
        resolveDamage(s1.id, { confirmed: true });
        t('akun Damage-only: selesai → Good', U('04').status, 'Good');
        const realPartial = window.cloud.updateUnitsFields; delete window.cloud.updateUnitsFields;
        whole.length = 0;
        const s2 = add('04', 'Mekanis', '', 'firebase-init lama');
        t('firebase-init lama (tanpa updateUnitsFields): tetap parsial lewat saveUnits',
          [whole.length, keysOk(whole.flat()), whole.flat().some(p => 'name' in p)], [1, true, false]);
        window.cloud.updateUnitsFields = realPartial;
        resolveDamage(s2.id, { confirmed: true });
        // ditolak server (rules belum dipublikasi): dicatat gagal dan ditarik ulang
        refuseUnits = Object.assign(new Error('Missing or insufficient permissions.'), { code: 'permission-denied' });
        toasts.length = 0;
        const s3 = add('04', 'Mekanis', '', 'Ditolak');
        await tick(60);
        t('status unit ditolak server: diberi tahu', toasts.some(m => /Rejected by the server — unit status \\(from damage\\) reverted/.test(m)), true);
        refuseUnits = null;
        asOwner();
        await tick(30);

        // =============== set Good manual saat damage masih terbuka ===============
        globalData.find(u => u.id === '04').status = 'Breakdown';   // seperti sebelum penolakan
        const g1 = D(s3.id) || add('04', 'Mekanis', '', 'Masih terbuka');
        navigateTo('editUnits'); switchEditUnitsGroup('heavy', { persist: false }); renderEditTable(); await tick(20);
        const cell = document.querySelector('#editBody [data-id="04"][data-field="status"]');
        asked = []; confirmAnswer = false;
        cell.textContent = 'Good'; saveInlineEdit(cell);
        t('set Good manual dengan damage terbuka: ditanya, batal = tetap Breakdown',
          [/open damage record\\(s\\)/.test(asked[0] || ''), U('04').status, cell.textContent], [true, 'Breakdown', 'Breakdown']);
        confirmAnswer = true; asked = [];
        cell.textContent = 'Good'; saveInlineEdit(cell);
        t('... ya = Good, catatannya tetap terbuka', [U('04').status, D(g1.id).resolved], ['Good', false]);

        // =============== Periksa Data ===============
        let found = dcDamageStatusSync().filter(i => i.unitId === '04');
        t('Periksa Data: unit Good dengan damage terbuka ditemukan', [found.length, found[0] && found[0].syncFix, /Status is Good, but 1 damage record\\(s\\) are still open/.test(found[0] ? found[0].detail : '')], [1, 'hold', true]);
        fixDamageSync('04', 'status', 'hold');
        t('perbaikan "Set Breakdown": unit Breakdown dengan id damage-nya', [U('04').status, U('04').breakdownDamageId, dcDamageStatusSync().filter(i => i.unitId === '04').length], ['Breakdown', g1.id, 0]);
        cell.textContent = 'Good';
        const cell2 = (renderEditTable(), document.querySelector('#editBody [data-id="04"][data-field="status"]'));
        cell2.textContent = 'Good'; saveInlineEdit(cell2);
        fixDamageSync('04', 'status', 'done');
        t('perbaikan "Mark done": catatannya selesai, unit tetap Good', [D(g1.id).resolved, U('04').status, dcDamageStatusSync().filter(i => i.unitId === '04').length], [true, 'Good', 0]);
        // Breakdown yang ditinggal damage yang sudah selesai (perangkat lama)
        const z = add('03', 'Mekanis', '', 'Ditinggal');
        D(z.id).resolved = true; D(z.id).resolvedAt = today;   // diselesaikan dari tab lama tanpa melepas unit
        found = dcDamageStatusSync().filter(i => i.unitId === '03');
        t('Periksa Data: Breakdown yang damage-nya sudah selesai → tawarkan Set Good', found.map(i => i.syncFix), ['release']);
        fixDamageSync('03', 'status', 'release');
        t('perbaikan "Set Good"', [U('03').status, dcDamageStatusSync().filter(i => i.unitId === '03').length], ['Good', 0]);
        // catatan lama yang tidak tertaut (akun tanpa hak unit sebelum v162)
        globalDamages.push({ id: 'old1', date: daysAgo(3), unitId: '06', unitName: 'EX-06', sn: 'SN06', damageType: 'Mekanis', component: '', description: 'v161', resolved: false, resolvedAt: '', drove: '', createdAt: 1, updatedAt: 1 });
        found = dcDamageStatusSync().filter(i => i.unitId === '06' && i.field === 'status');
        t('Periksa Data: catatan lama yang tidak tertaut juga ditemukan', found.length, 1);
        navigateTo('leader'); switchLeaderTab('check'); renderDataCheck(); await tick(20);
        t('tombol pilihan per unit di Periksa Data', !!document.querySelector('#dataCheckBody button[onclick*="fixDamageSync"]'), true);

        // =============== penanda ===============
        navigateTo('damage'); setDamageGroup('heavy'); renderDamageTable();
        const chipTxt = [...document.querySelectorAll('#damageBody .dmg-unit-chip')].map(x => x.textContent);
        t('tabel Damage: penanda status unit pada catatan terbuka', chipTxt.includes('Telematic Box: Breakdown'), true);
        navigateTo('editUnits'); switchEditUnitsGroup('heavy', { persist: false }); renderEditTable();
        const chip = document.querySelector('#editBody [data-id="06"][data-field="name"]').parentElement.querySelector('.dmg-open-chip');
        t('Unit Database: penanda "N open damage"', !!chip && /open damage/.test(chip.textContent), true);
        chip.click(); await tick(20);
        t('penanda membuka Damage dengan pencarian SN', [currentView, document.getElementById('damageSearch').value], ['damage', 'SN06']);
        openDamageRepair(e1.id); await tick(10);
        t('dialog Mark done menyebut efeknya pada unit', document.getElementById('dmgRepairEffect').textContent, 'After this: "EX-06" Telematic Box goes back to Good.');
        closeDamageRepair(true);

        window.__T = T;
    } catch (e) { window.__T = [{ n: 'EXCEPTION ' + e.message + ' ' + e.stack, pass: false }]; } })();` });

    await page.waitForFunction(() => window.__T, null, { timeout: 90000 });
    const T = await page.evaluate(() => window.__T);
    let pass = 0;
    T.forEach(x => { if (x.pass) pass++; else console.log('FAIL', x.n, '\n  dapat :', JSON.stringify(x.g), '\n  harap :', JSON.stringify(x.w)); });
    if (errors.length) console.log('PAGE ERRORS:', errors);
    console.log(`${pass}/${T.length} lulus`);
    await b.close();
    process.exit(pass === T.length && !errors.length ? 0 : 1);
})();
