// 2.10.7 — Implement Group: field di implement, daftar di Master lists,
// kolom + filter di tabel, dan menetapkan group ke beberapa implement sekaligus.
const { launch, BASE_URL } = require('./_env');

(async () => {
    const b = await launch();
    const page = await b.newPage({ viewport: { width: 1280, height: 900 } });
    const errors = [];
    page.on('pageerror', e => { if (!/Chart is not defined/.test(e.message)) errors.push(e.message); });
    await page.goto(BASE_URL + '/index.html', { waitUntil: 'load' });
    await page.waitForTimeout(1000);

    await page.addScriptTag({ content: `(async () => { try {
        const T = []; const t = (n, g, w) => T.push({ n, g, w, pass: JSON.stringify(g) === JSON.stringify(w) });
        const tick = ms => new Promise(r => setTimeout(r, ms || 30));
        const toasts = []; window.showToast = m => toasts.push(String(m));
        const pushed = [];
        const ok = () => Promise.resolve();
        window.cloud = { isReady: true, addHistoryEvents: ok, saveImplement: r => { pushed.push(JSON.parse(JSON.stringify(r))); return ok(); },
            saveMasterList: ok };
        currentUser = { uid: 'o', email: 'o@x.id' };
        currentUserDoc = { uid: 'o', role: 'owner', status: 'active', email: 'o@x.id' };
        hideAuthGates(); applyRoleGating();
        const seed = [
            { id: 'i1', profileName: 'Offset Harrow', brand: 'Grizzly', equipmentType: 'Offset Harrow', code: 'OHZ', operation: 'Tillage' },
            { id: 'i2', profileName: 'Bed Ripper', brand: 'ISJ', equipmentType: 'Bed Ripper', code: 'BRI', operation: 'Tillage' },
            { id: 'i3', profileName: 'Billet Planter', brand: 'Hodge', equipmentType: 'Billet Planter', code: 'BPH', operation: 'Seeding' }];
        globalImplements = seed.map(x => ({ ...x }));
        navigateTo('implements'); globalImplements = seed.map(x => ({ ...x })); renderImplementsTable(); await tick();
        t('kolom Group di tabel', [...document.querySelectorAll('#implementTable thead th')].map(th => th.textContent.trim()).includes('Group'), true);
        t('belum ada group: tanda —', document.querySelector('#implementBody td[data-label="Implement Group"]').textContent.trim(), '—');

        // form: isi group
        editImplement('i1'); await tick();
        document.getElementById('implGroup').value = 'Primary Tillage';
        saveImplement({ preventDefault() {} }); await tick();
        t('group tersimpan dari form', globalImplements.find(x => x.id === 'i1').group, 'Primary Tillage');

        // pilih beberapa → Set group
        renderImplementsTable(); await tick();
        document.querySelectorAll('.impl-check').forEach(cb => { if (cb.dataset.id !== 'i1') cb.checked = true; });
        updateSelectedImplementCount();
        t('tombol Set group muncul saat dipilih', [document.getElementById('btnGroupSelectedImpl').style.display, document.getElementById('groupImplCount').textContent], ['', '2']);
        window.prompt = () => 'Planting';
        setGroupForSelectedImplements(); await tick();
        t('Set group untuk beberapa implement', globalImplements.map(x => x.group || ''), ['Primary Tillage', 'Planting', 'Planting']);

        // filter
        populateImplementGroupFilter();
        t('filter berisi group yang dipakai', [...document.querySelectorAll('#implementGroupFilter option')].map(o => o.textContent), ['All groups', 'Planting', 'Primary Tillage']);
        document.getElementById('implementGroupFilter').value = 'planting'; renderImplementsTable(); await tick();
        t('filter Group', document.querySelectorAll('#implementBody tr').length, 2);
        document.getElementById('implementGroupFilter').value = ''; renderImplementsTable(); await tick();

        // master list membatasi pilihan
        masterLists.implGroup = ['Primary Tillage', 'Planting', 'Harvesting'];
        editImplement('i2'); await tick();
        document.getElementById('implGroup').value = 'Asal ketik';
        saveImplement({ preventDefault() {} }); await tick();
        t('group di luar Master lists ditolak', /not in the Implement Group list/.test(toasts.pop() || ''), true);
        document.getElementById('implGroup').value = 'harvesting';
        saveImplement({ preventDefault() {} }); await tick();
        t('ejaan disamakan dengan Master lists', globalImplements.find(x => x.id === 'i2').group, 'Harvesting');
        closeImplementModal && closeImplementModal(true);
        openMasterLists('implGroup'); await tick();
        t('tab Implement Groups di Master lists', [...document.querySelectorAll('#mlTabs .team-tab')].some(x => /Implement Groups/.test(x.textContent)), true);
        closeMasterLists();

        // Units: group tampil di bawah nama implement, dan bisa dicari
        const units = [{ id: 'u1', name: 'GGTR130G_OHZ', implement: 'Offset Harrow — Grizzly', status: 'Good', site: 'PT. A' },
                       { id: 'u2', name: 'GGTR131G_BPH', implement: 'Billet Planter — Hodge', status: 'Good', site: 'PT. A' }];
        globalData = units.slice(); navigateTo('editUnits'); globalData = units.slice(); renderEditTable(); await tick();
        t('Units: group di kolom Implement', [...document.querySelectorAll('#editBody td[data-label="Implement"] .unit-impl-group')].map(x => x.textContent), ['Primary Tillage', 'Planting']);
        document.getElementById('editSearch').value = 'planting'; renderEditTable(); await tick();
        t('Units: cari berdasarkan group', document.querySelectorAll('#editBody tr').length, 1);
        document.getElementById('editSearch').value = '';
        window.__T = T;
    } catch (e) { window.__T = [{ n: 'CRASH ' + e.message + ' ' + (e.stack || '').split('\\n')[1], pass: false }]; } })()` });

    await page.waitForFunction(() => window.__T, null, { timeout: 30000 });
    const T = await page.evaluate(() => window.__T);
    let pass = 0;
    for (const x of T) {
        if (x.pass) pass++;
        console.log(`${x.pass ? 'PASS' : 'FAIL'}  ${x.n}`);
        if (!x.pass && 'g' in x) console.log(`        dapat=${JSON.stringify(x.g)} harap=${JSON.stringify(x.w)}`);
    }
    console.log(`\n${pass}/${T.length} lulus`);
    console.log('PAGE ERRORS:', errors.length ? errors : 'none');
    await b.close();
    process.exit(pass === T.length && !errors.length ? 0 : 1);
})();
