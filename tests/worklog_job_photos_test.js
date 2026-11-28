// 2.25.0 — Daily Report: foto per paddock (maks 8 per paddock). Semua foto
// tetap di satu dokumen workLogPhotos sebagai satu daftar: foto tiap paddock
// berurutan, lalu foto umum (Office / laporan lama); tiap paddock menyimpan
// jumlahnya sendiri (jobs[i].photoCount).
const { launch, BASE_URL } = require('./_env');

(async () => {
    const b = await launch();
    const page = await b.newPage({ viewport: { width: 1280, height: 950 } });
    const errors = [];
    page.on('pageerror', e => { if (!/Chart is not defined/.test(e.message)) errors.push(e.message); });
    await page.goto(BASE_URL + '/index.html', { waitUntil: 'load' });
    await page.waitForTimeout(1000);
    await page.addScriptTag({ content: `(async () => { try {
        const T = []; const t = (n, g, w) => T.push({ n, g, w, pass: JSON.stringify(g) === JSON.stringify(w) });
        const tick = ms => new Promise(r => setTimeout(r, ms || 20));
        const toasts = []; window.showToast = m => toasts.push(String(m));
        window.confirm = () => true;
        const logs = [], saved = {};
        let photoGate = null;
        const ok = () => Promise.resolve();
        window.cloud = { isReady: true, addHistoryEvents: ok,
            saveWorkLog: r => { logs.push(JSON.parse(JSON.stringify(r))); return ok(); },
            saveWorkLogPhotos: (id, p) => { saved[id] = p.slice(); return ok(); }, deleteWorkLogPhotos: ok,
            getWorkLogPhotos: id => photoGate ? photoGate.then(() => saved[id] || []) : Promise.resolve(saved[id] || []) };
        let seq = 0;
        window.compressImageToDataURL = () => Promise.resolve('data:image/jpeg;base64,P' + (++seq));
        const img = n => Array.from({ length: n }, () => new File(['x'], 'a.jpg', { type: 'image/jpeg' }));
        const pick = async (i, n) => { _wlPhotoJob = i; await handleWorkLogJobPhotoChange({ target: { files: img(n), value: '' } }); };
        currentUser = { uid: 'o', email: 'o@x.id' }; currentUserDoc = { uid: 'o', role: 'owner', status: 'active', email: 'o@x.id' };
        hideAuthGates(); applyRoleGating();
        globalData = [{ id: 'u1', name: 'GGTR101G', sn: 'SN1', status: 'Good' }, { id: 'u2', name: 'GGTR102G', sn: 'SN2', status: 'Good' }];
        teamMembers = [{ id: 'm1', name: 'Andi', company: '', active: true }];
        workLogs = []; leaveRequests = []; teamShifts = [];
        navigateTo('team');

        showAddWorkLogForm();
        document.getElementById('wlMember').value = 'm1';
        t('Field: kotak foto umum disembunyikan', document.getElementById('wlPhotoGroup').style.display, 'none');
        t('kartu paddock punya kotak foto + tombol Add', [!!document.querySelector('#wlJobs .wl-job__photos'), !!document.querySelector('#wlJobs .wl-photo-add')], [true, true]);
        await pick(0, 3);
        t('3 foto masuk ke paddock 1', [_wlJobs[0].pics.length, document.querySelectorAll('#wlJobs .wl-job:nth-child(1) .wl-photo').length, _wlPhotosDirty], [3, 3, true]);
        await pick(0, 7);
        t('maksimal 8 per paddock', [_wlJobs[0].pics.length, toasts.some(x => /Maximum 8 photos per paddock/.test(x))], [8, true]);
        t('paddock penuh: tombol Add hilang', !!document.querySelector('#wlJobs .wl-job:nth-child(1) .wl-photo-add'), false);
        removeWorkLogJobPhoto(0, 7); removeWorkLogJobPhoto(0, 6); removeWorkLogJobPhoto(0, 5); removeWorkLogJobPhoto(0, 4);
        addWorkLogJob();
        await pick(1, 2);
        t('paddock 2 punya foto sendiri', [_wlJobs[0].pics.length, _wlJobs[1].pics.length], [4, 2]);
        const type = jobTypeChoices()[0];
        _wlJobs[0].paddock = 'P-1'; _wlJobs[0].jobType = type; _wlJobs[0].desc = 'Bajak'; _wlJobs[0].units = [{ id: 'u1', name: 'GGTR101G', sn: 'SN1' }];
        _wlJobs[1].paddock = 'P-2'; _wlJobs[1].jobType = type; _wlJobs[1].desc = 'Garu';  _wlJobs[1].units = [{ id: 'u2', name: 'GGTR102G', sn: 'SN2' }];
        renderWorkLogJobs();
        document.getElementById('wlDate').value = toISODate();
        const p1 = _wlJobs[0].pics.slice(), p2 = _wlJobs[1].pics.slice();
        saveWorkLog({ preventDefault() {} }); await tick(40);
        const rec = logs[logs.length - 1];
        t('tersimpan: jumlah per paddock + total', rec && [rec.jobs.map(j => j.photoCount), rec.photoCount], [[4, 2], 6]);
        t('satu daftar foto: paddock 1 lalu paddock 2', saved[rec.id], [...p1, ...p2]);

        // ---- dibuka lagi: foto kembali ke paddocknya ----
        workLogs = [{ ...rec, approval: 'draft' }];
        _wlPhotoCache.clear();
        let open; photoGate = new Promise(r => { open = r; });
        editWorkLog(rec.id); await tick();
        t('sedang dimuat: jumlah tersimpan tampil, Add dimatikan', [document.querySelector('#wlJobs .wl-job__photos .form-optional').textContent.includes('4/8'), !!document.querySelector('#wlJobs .wl-photo-add')],
          [true, false]);
        // Paddock 1 dihapus sebelum fotonya tiba: ditolak.
        removeWorkLogJob(0);
        t('hapus paddock berfoto sebelum foto tiba: ditolak', [_wlJobs.length, toasts.some(x => /finish loading/.test(x))], [2, true]);
        // Baris baru tanpa foto ditambahkan sebelum foto tiba: tidak menggeser.
        addWorkLogJob();
        open(); await tick(40);
        t('foto kembali ke paddock masing-masing', [_wlJobs[0].pics, _wlJobs[1].pics, _wlJobs[2].pics.length, _wlPhotos.length], [p1, p2, 0, 0]);
        // Simpan tanpa mengubah foto: dokumen foto tidak ditulis ulang.
        delete saved[rec.id];
        _wlJobs.pop(); renderWorkLogJobs();
        saveWorkLog({ preventDefault() {} }, 'draft'); await tick(40);
        const rec2 = logs[logs.length - 1];
        t('simpan tanpa ubah foto: jumlah tetap, foto tidak ditulis', [rec2.jobs.map(j => j.photoCount), rec2.photoCount, saved[rec.id]], [[4, 2], 6, undefined]);

        // ---- lightbox: keterangan per paddock ----
        saved[rec.id] = [...p1, ...p2]; photoGate = null; _wlPhotoCache.clear();
        workLogs = [{ ...rec2 }];
        await openWorkLogPhotos(rec.id);
        t('lightbox: keterangan paddock', [_lightboxPhotos.length, _lightboxCaptions[0], _lightboxCaptions[5]],
          [6, 'P-1 · ' + type + ' — photo 1 of 4', 'P-2 · ' + type + ' — photo 2 of 2']);
        if (typeof closePhotoLightbox === 'function') closePhotoLightbox();

        // ---- laporan lama: foto tanpa paddock tampil sebagai Other photos ----
        saved.old = ['data:image/jpeg;base64,O1', 'data:image/jpeg;base64,O2'];
        workLogs = [{ id: 'old', date: '2026-10-01', memberId: 'm1', memberName: 'Andi', approval: 'draft', photoCount: 2,
                      jobs: [{ paddock: 'P-9', jobType: type, units: [{ id: 'u1', name: 'GGTR101G', sn: 'SN1' }], desc: 'x' }] }];
        editWorkLog('old'); await tick(40);
        t('laporan lama: foto jadi Other photos (tanpa Add), paddock kosong',
          [_wlPhotos.length, _wlJobs[0].pics.length, document.getElementById('wlPhotoGroup').style.display, document.getElementById('wlPhotoAddRow').style.display, document.getElementById('wlPhotoTitle').textContent],
          [2, 0, '', 'none', 'Other photos']);
        await pick(0, 1);
        saveWorkLog({ preventDefault() {} }, 'draft'); await tick(40);
        const rec3 = logs[logs.length - 1];
        t('laporan lama + foto paddock: paddock dulu, lalu foto lama', [rec3.jobs[0].photoCount, rec3.photoCount, saved.old.length, saved.old[2]], [1, 3, 3, 'data:image/jpeg;base64,O2']);

        // ---- Field -> Office: foto paddock dibuang (dengan konfirmasi) ----
        showAddWorkLogForm();
        document.getElementById('wlMember').value = 'm1';
        await pick(0, 2);
        document.querySelector('input[name="wlPlace"][value="office"]').checked = true; syncWorkLogPlace();
        document.getElementById('wlDate').value = toISODate();
        document.getElementById('wlTask').value = 'Rekap';
        saveWorkLog({ preventDefault() {} });
        t('Office: foto paddock dibuang, foto Office tetap wajib', [toasts.pop(), _wlJobs[0].pics.length], ['Add at least one photo of the office work', 0]);
        closeWorkLogModal(true);
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
