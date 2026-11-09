// 2.29.0 — Projects & Jobs: project (unit / angka / milestone), progres dan
// kesehatan otomatis, update, job di luar Daily Report (kalibrasi, pasang
// implement → implement unit ikut berubah), job menambah progres project,
// akses own / view, minggu Sabtu–Jumat, dan riwayat di profil unit.
const { launch, BASE_URL } = require('./_env');
(async () => {
    const b = await launch();
    const page = await b.newPage({ viewport: { width: 1366, height: 900 } });
    const errors = [];
    page.on('pageerror', e => { if (!/Chart is not defined/.test(e.message)) errors.push(e.message); });
    await page.goto(BASE_URL + '/index.html', { waitUntil: 'load' });
    await page.waitForTimeout(1000);
    const T = await page.evaluate(async () => {
        const T = []; const t = (n, g, w) => T.push({ n, g, w, pass: JSON.stringify(g) === JSON.stringify(w) });
        const tick = ms => new Promise(r => setTimeout(r, ms || 20));
        const toasts = []; window.showToast = m => toasts.push(String(m));
        window.confirm = () => true;
        const savedP = [], savedJ = [];
        const ok = () => Promise.resolve();
        window.cloud = { isReady: true, addHistoryEvents: ok, saveProjectPhoto: ok, deleteProjectPhoto: ok, saveUnit: ok, saveUnits: ok, updateUnit: ok, updateUnitsBatch: ok,
            saveProject: r => { savedP.push(JSON.parse(JSON.stringify(r))); return ok(); }, deleteProject: ok,
            saveOtJob: r => { savedJ.push(JSON.parse(JSON.stringify(r))); return ok(); }, deleteOtJob: ok };
        const asOwner = () => { currentUser = { uid: 'o', email: 'o@x.id' }; currentUserDoc = { uid: 'o', role: 'owner', status: 'active', displayName: 'Rasyid' }; applyRoleGating(); };
        asOwner(); hideAuthGates();
        navigateTo('projects');
        globalData = [
            { id: 'h1', name: 'EXC-07', sn: 'S1', site: 'PT. Murni Nusantara Mandiri', status: 'Good', unitGroup: 'heavy' },
            { id: 'h2', name: 'EXC-09', sn: 'S2', site: 'PT. Murni Nusantara Mandiri', status: 'Good', unitGroup: 'heavy' },
            { id: 't1', name: 'GGTR136G', sn: 'S3', site: 'PT. Global Papua Abadi', status: 'Good', implement: 'Offset Harrow — Baldan' }];
        projects = []; otJobs = [];

        t('menu Projects & Jobs tampil untuk owner', document.querySelector('.nav__link[data-view="projects"]').style.display !== 'none', true);
        t('area akses baru dengan level own', ACCESS_AREAS.find(a => a.key === 'projects').levels, ['none', 'own', 'view', 'edit']);

        // ---- kesehatan otomatis ----
        const P = (o) => Object.assign({ status: 'active', start: '2026-10-01', end: '2026-11-30', target: { mode: 'count', count: 10, done: 5 } }, o);
        t('progres angka', projectProgress(P({})).pct, 50);
        t('tepat waktu: 50% di tengah periode', projectRag(P({}), '2026-10-31'), 'green');
        t('tertinggal >15 poin: Watch', projectRag(P({ target: { mode: 'count', count: 10, done: 2 } }), '2026-11-10'), 'amber');
        t('lewat tenggat belum selesai: Action', projectRag(P({}), '2026-12-02'), 'red');
        t('on hold: Watch', projectRag(P({ status: 'hold' }), '2026-10-05'), 'amber');
        t('diatur manual', projectRag(P({ ragOverride: 'red' }), '2026-10-05'), 'red');
        t('done', projectRag(P({ status: 'done' }), '2026-12-30'), 'done');
        t('milestone berbobot', projectProgress({ target: { mode: 'milestones', milestones: [{ label: 'a', weight: 30, done: true }, { label: 'b', weight: 70, done: false }] } }).pct, 30);
        t('minggu Sabtu–Jumat', [jobPeriodRange('week', '2026-11-04'), jobPeriodRange('lastweek', '2026-11-07')], [['2026-10-31', '2026-11-06'], ['2026-10-31', '2026-11-06']]);

        // ---- project per unit ----
        showAddProjectForm();
        t('form: PIC = yang login, mulai hari ini', [document.getElementById('pjPic').value, document.getElementById('pjStart').value], ['Rasyid', toISODate()]);
        saveProject();
        t('nama wajib', toasts.pop(), 'Fill in the project name');
        document.getElementById('pjName').value = 'Camera AI — HE MNM';
        document.getElementById('pjType').value = 'heavy';
        document.getElementById('pjEnd').value = '2026-11-30';
        saveProject();
        t('unit wajib untuk target unit', /Add at least one unit/.test(toasts.pop()), true);
        _pjDepts = [{ name: 'Workshop', pic: 'Budi' }, { name: '', pic: '' }];
        document.getElementById('pjUnitAdd').value = 'EXC-07'; addProjectUnit();
        document.getElementById('pjUnitAdd').value = 'S2'; addProjectUnit();
        document.getElementById('pjUnitAdd').value = 'EXC-07'; addProjectUnit();
        t('unit tidak dobel', _pjUnits.map(u => u.id), ['h1', 'h2']);
        document.getElementById('pjUnitWhat').value = 'Camera AI installed';
        saveProject(); await tick();
        const p0 = savedP[savedP.length - 1];
        t('tersimpan: unit, departemen, PT dari unit, pembuat', [p0.target.mode, p0.target.units.length, p0.departments, p0.company, p0.createdByUid], ['units', 2, [{ name: 'Workshop', pic: 'Budi' }], 'PT. Murni Nusantara Mandiri', 'o']);
        t('detail terbuka setelah simpan', document.getElementById('pjDetailModal').classList.contains('open'), true);
        // update dengan satu unit selesai
        document.getElementById('pjUpdText').value = 'Pasang di EXC-07 bersama workshop';
        document.getElementById('pjUpdBlocker').value = 'Bracket kurang 1';
        document.querySelector('#pjDetailBody input[data-pj-done="0"]').checked = true;
        saveProjectUpdate(p0.id); await tick();
        const p1 = projects.find(p => p.id === p0.id);
        t('update: unit selesai, progres 50%, kendala tercatat', [p1.target.units[0].done, projectProgress(p1).pct, p1.updates.length, p1.updates[0].blocker, /0% → 50%/.test(p1.updates[0].progress)], [true, 50, 1, 'Bracket kurang 1', true]);
        t('kartu project tampil dengan progres', /50%/.test(document.querySelector('#pjList .pj-card').textContent), true);
        closeProjectDetail();

        // ---- job: kalibrasi bagian dari project → progres ----
        showAddJobForm(p0.id);
        t('form job: proyek terisi, deskripsi dari target', [document.getElementById('otJobProject').value, document.getElementById('otJobDesc').value], [p0.id, 'Camera AI installed']);
        document.getElementById('otJobCategory').value = 'Calibration'; syncJobCategory();
        t('kolom implement hanya untuk pasang implement', document.getElementById('otJobImplGroup').style.display, 'none');
        document.getElementById('otJobUnitAdd').value = 'EXC-09';
        document.getElementById('otJobRequestedBy').value = 'Workshop';
        document.getElementById('otJobHours').value = '1.5';
        saveJob(); await tick();
        const j0 = savedJ[savedJ.length - 1];
        t('job tersimpan (unit yang diketik ikut)', [j0.category, j0.units.map(u => u.id), j0.hours, j0.requestedBy, j0.company], ['Calibration', ['h2'], 1.5, 'Workshop', 'PT. Murni Nusantara Mandiri']);
        const p2 = projects.find(p => p.id === p0.id);
        t('job selesai menambah progres project (auto update)', [projectProgress(p2).pct, p2.updates.length, p2.updates[1].auto], [100, 2, true]);

        // ---- job: pasang implement → unit ikut berubah ----
        showAddJobForm();
        document.getElementById('otJobCategory').value = 'Implement install'; syncJobCategory();
        t('kolom implement tampil', document.getElementById('otJobImplGroup').style.display, '');
        document.getElementById('otJobDesc').value = 'Pasang lime spreader';
        saveJob();
        t('implement wajib saat selesai', toasts.pop(), 'Fill in the implement that was installed');
        document.getElementById('otJobImpl').value = 'Lime or Dolomite Spreader — Bonel';
        saveJob();
        t('unit wajib untuk pasang implement', toasts.pop(), 'Add the unit the implement was fitted to');
        document.getElementById('otJobUnitAdd').value = 'GGTR136G'; addJobUnit();
        saveJob(); await tick();
        const j1 = savedJ[savedJ.length - 1];
        t('implement unit diganti + dicatat sebelumnya', [globalData.find(u => u.id === 't1').implement, j1.appliedImplement.units[0].before], ['Lime or Dolomite Spreader — Bonel', 'Offset Harrow — Baldan']);
        // diedit lagi: tidak diterapkan dua kali
        const before = globalData.find(u => u.id === 't1');
        globalData.find(u => u.id === 't1').implement = 'Manual change';
        editJob(j1.id); saveJob(); await tick();
        t('simpan ulang tidak menimpa lagi', globalData.find(u => u.id === 't1').implement, 'Manual change');
        void before;

        // ---- 2.29.6: kendala / backlog lapangan ----
        editJob(j0.id);
        document.getElementById('otJobIssue').value = 'Selang hidrolik bocor, tunggu sparepart';
        saveJob(); await tick();
        t('job menyimpan kendala lapangan', savedJ[savedJ.length - 1].issue, 'Selang hidrolik bocor, tunggu sparepart');
        switchProjectsTab('jobs');
        document.getElementById('jobStatusFilter').value = 'issue'; renderJobsList();
        t('filter "With field issues"', [document.querySelectorAll('#jobBody tr').length, /Selang hidrolik/.test(document.getElementById('jobBody').textContent)], [1, true]);
        document.getElementById('jobStatusFilter').value = '';

        // ---- 2.29.8: lampiran PDF ----
        const up = [], del = [];
        window.cloud.uploadProjectFile = (path, file, prog) => { up.push(path); prog(0.5); return Promise.resolve(); };
        window.cloud.deleteProjectFile = path => { del.push(path); return Promise.resolve(); };
        window.cloud.projectFileUrl = () => Promise.resolve('https://example.org/x.pdf');
        const pdf = n => new File(['%PDF-1.4'], n, { type: 'application/pdf' });
        editJob(j0.id);
        await addJobFiles({ target: { files: [pdf('Berita Acara.pdf'), new File(['x'], 'foto.png', { type: 'image/png' })], value: '' } }); await tick();
        t('PDF diunggah ke projectFiles/<job>/, PNG ditolak', [up.length, up[0].startsWith('projectFiles/' + j0.id + '/'), toasts.some(x => /not a PDF/.test(x))], [1, true, true]);
        await addJobFiles({ target: { files: [new File(['v'], 'kalibrasi.MOV', { type: '' })], value: '' } }); await tick();
        t('video MOV diterima (tipe dari nama file)', [up.length, _jobFiles[1].type, /fa-file-video/.test(document.getElementById('otJobFiles').innerHTML)], [2, 'video/quicktime', true]);
        removeJobFile(1);
        t('daftar lampiran tampil dengan nama', /Berita Acara\.pdf/.test(document.getElementById('otJobFiles').textContent), true);
        saveJob(); await tick();
        t('job menyimpan lampiran', savedJ[savedJ.length - 1].files.map(f => f.name), ['Berita Acara.pdf']);
        // dibuka lagi, hapus lampiran, simpan → file dihapus dari Storage
        editJob(j0.id);
        removeJobFile(0);
        t('hapus lampiran lama belum dihapus sebelum simpan', del.length, 1);
        saveJob(); await tick();
        t('setelah simpan: file dihapus dari Storage', [del.length, savedJ[savedJ.length - 1].files.length], [2, 0]);
        // unggah lalu batal: file baru dihapus lagi
        editJob(j0.id);
        await addJobFiles({ target: { files: [pdf('draft.pdf')], value: '' } }); await tick();
        closeJobModal();
        t('batal: PDF yang baru diunggah dihapus lagi', del.length, 3);

        // ---- tabel job & filter periode ----
        switchProjectsTab('jobs');
        t('tabel job: 2 job minggu ini', document.querySelectorAll('#jobBody tr').length, 2);
        otJobs.push({ id: 'old', date: '2026-01-05', category: 'Meeting', description: 'Lama', status: 'done', units: [], createdByUid: 'o' });
        renderJobsList();
        t('job lama tidak di minggu ini', document.querySelectorAll('#jobBody tr').length, 2);
        document.getElementById('jobPeriod').value = ''; renderJobsList();
        t('periode All menampilkan semua', document.querySelectorAll('#jobBody tr').length, 3);
        document.getElementById('jobPeriod').value = 'week';

        // ---- profil unit ----
        const html = unitJobsProfileHtml(globalData.find(u => u.id === 'h2'));
        t('profil unit: job & project', [/Calibration/.test(html), /Camera AI — HE MNM/.test(html), /done/.test(html)], [true, true, true]);

        // ---- akses own / view ----
        currentUser = { uid: 'k', email: 'k@x.id' };
        currentUserDoc = { uid: 'k', role: 'staff', status: 'active', displayName: 'Kiki', access: { projects: 'own' } };
        applyRoleGating(); renderProjectsView();
        t('own: boleh menambah', [canAddProjects(), document.getElementById('btnNewProject').style.display], [true, '']);
        t('own: tidak bisa ubah project orang lain', canEditProjectRecord(projects[0]), false);
        editProject(projects[0].id);
        t('own: edit ditolak dengan pesan', /Only the person who added/.test(toasts.pop()), true);
        showProjectDetail(projects[0].id);
        t('own: detail tanpa tombol Edit/Delete', [...document.querySelectorAll('#pjDetailActions button')].map(b => b.textContent.trim()), ['Close']);
        closeProjectDetail();
        currentUserDoc = { uid: 'k', role: 'staff', status: 'active', access: { projects: 'view' } };
        applyRoleGating(); renderProjectsView();
        t('view: tombol tambah disembunyikan', [canAddProjects(), document.getElementById('btnNewProject').style.display], [false, 'none']);
        currentUserDoc = { uid: 'k', role: 'staff', status: 'active', access: {} };
        applyRoleGating();
        t('tanpa akses: menu tersembunyi', document.querySelector('.nav__link[data-view="projects"]').style.display, 'none');
        asOwner();

        // ---- 2.29.1: target angka + unit opsional (lebih dari 1) ----
        showAddProjectForm();
        document.getElementById('pjName').value = 'Depth Sensor - Cartrack (Phase 1)';
        document.getElementById('pjEnd').value = '2026-12-31';
        document.querySelector('input[name="pjMode"][value="count"]').checked = true; syncProjectMode();
        t('mode angka: kolom unit tetap ada, opsional', [document.getElementById('pjUnitsHint').style.display, /optional/.test(document.getElementById('pjUnitsLabel').textContent)], ['', true]);
        document.getElementById('pjCountTarget').value = '54';
        document.getElementById('pjCountWhat').value = 'Installed Complete Depth Sensor';
        saveProject(); await tick();
        const c0 = savedP[savedP.length - 1];
        t('angka tanpa unit boleh', [c0.target.mode, c0.target.count, c0.target.units], ['count', 54, []]);
        editProject(c0.id);
        document.getElementById('pjUnitAdd').value = 'EXC-07'; addProjectUnit();
        document.getElementById('pjUnitAdd').value = 'GGTR136G'; addProjectUnit();
        saveProject(); await tick();
        const c1 = projects.find(x => x.id === c0.id);
        t('unit alat berat + traktor ditambahkan', c1.target.units.map(u => u.id), ['h1', 't1']);
        t('detail: daftar unit terkait', /Units involved — 0 of 2 done/.test(document.getElementById('pjDetailBody').textContent), true);
        const box = document.querySelector('#pjDetailBody input[data-pj-unit="0"]');
        box.checked = true; _pjCountFromUnits(box);
        t('centang unit menambah Done so far', document.getElementById('pjUpdCount').value, '1');
        document.getElementById('pjUpdText').value = 'Depth sensor terpasang di EXC-07';
        saveProjectUpdate(c0.id); await tick();
        const c2 = projects.find(x => x.id === c0.id);
        t('update: unit selesai + angka 1/54', [c2.target.units[0].done, c2.target.done, projectProgress(c2).pct], [true, 1, 2]);
        closeProjectDetail();
        showAddJobForm(c0.id);
        t('form job: deskripsi dari target', document.getElementById('otJobDesc').value, 'Installed Complete Depth Sensor');
        document.getElementById('otJobCategory').value = 'Device install'; syncJobCategory();
        document.getElementById('otJobUnitAdd').value = 'GGTR136G';
        saveJob(); await tick();
        const c3 = projects.find(x => x.id === c0.id);
        t('job selesai di unit terkait: unit dicentang, angka +1', [c3.target.units[1].done, c3.target.done], [true, 2]);
        toggleProjectTarget(c0.id, 'unit', 1, false); await tick();
        t('hilangkan centang: angka kembali -1', projects.find(x => x.id === c0.id).target.done, 1);
        t('profil unit traktor: project angka tampil', /Depth Sensor/.test(unitJobsProfileHtml(globalData.find(u => u.id === 't1'))), true);
        closeProjectDetail();
        projects = projects.filter(x => x.id !== c0.id);

        // ---- 2.29.2: job dengan unit di project angka TANPA daftar unit ----
        projects.push({ id: 'pd', name: 'Dept Sensor', status: 'active', start: '2026-09-01', end: '2026-12-31', createdByUid: 'o', updates: [],
            target: { mode: 'count', count: 54, done: 0, what: 'Installed Complete Depth Sensor' } });
        globalData.push({ id: 'b1', name: 'BD018G', site: 'PT. GPA', status: 'Good' }, { id: 'b2', name: 'BD100T', site: 'PT. GPA', status: 'Good' }, { id: 'b3', name: 'BD121T', site: 'PT. GPA', status: 'Good' });
        showAddJobForm('pd');
        document.getElementById('otJobCategory').value = 'Calibration';
        ['BD018G', 'BD100T'].forEach(n => { document.getElementById('otJobUnitAdd').value = n; addJobUnit(); });
        saveJob(); await tick();
        const d1 = projects.find(x => x.id === 'pd');
        t('job Done: unitnya masuk ke project, angka +2', [d1.target.units.map(u => u.name), d1.target.done], [['BD018G', 'BD100T'], 2]);
        // job lama (dicatat sebelum perbaikan) belum terhitung
        otJobs.push({ id: 'oldjob', projectId: 'pd', status: 'done', date: '2026-09-24', category: 'Calibration', units: [{ id: 'b2', name: 'BD100T' }, { id: 'b3', name: 'BD121T' }], createdByUid: 'o' });
        showProjectDetail('pd');
        t('detail: tombol hitung unit dari job (+1)', /Count units from jobs \(\+1\)/.test(document.getElementById('pjDetailBody').textContent), true);
        countProjectJobUnits('pd'); await tick();
        const d2 = projects.find(x => x.id === 'pd');
        t('setelah dihitung: 3 unit, 3/54, tombol hilang', [d2.target.units.length, d2.target.done, /Count units from jobs/.test(document.getElementById('pjDetailBody').textContent)], [3, 3, false]);
        const pdNow = projects.find(x => x.id === 'pd');
        pdNow.target.done = 4; showProjectDetail('pd');
        t('detail: asal angka (unit vs manual)', document.querySelector('#pjDetailBody .pj-count-src').textContent.trim(), '3 from units · 1 entered by hand (no unit)');
        closeProjectDetail();
        projects = projects.filter(x => x.id !== 'pd');

        // ---- hapus ----
        deleteProject(projects[0].id);
        t('project dihapus', projects.length, 0);
        // ---- 2.30.3: selesai 100% dihitung dari tanggal kerja, bukan tanggal input ----
        const fp = { id: 'f1', name: 'Depth Sensor', status: 'progress', start: '2026-09-01', end: '2026-10-06', target: { mode: 'count', count: 52, done: 52, what: 'sensors' },
            updates: [{ date: '2026-09-10', text: 'a', progress: 'x' }, { date: '2026-09-22', text: 'Complete', progress: 'y' }] };
        t('100% sebelum due: on time, bukan late', [pjDueText(fp, '2026-11-06'), projectRag(fp, '2026-11-06')], ['Finished 2026-09-22 · on time (due 2026-10-06)', 'green']);
        const fu = { id: 'f2', status: 'progress', end: '2026-10-06', target: { mode: 'units', units: [{ id: 'a', done: true, doneAt: '2026-10-01' }, { id: 'b', done: true, doneAt: '2026-10-09' }] }, updates: [] };
        t('mode unit: tanggal unit terakhir, telat 3 hari', pjDueText(fu, '2026-11-06'), 'Finished 2026-10-09 · 3 days late (due 2026-10-06)');
        t('belum 100%: tetap late', pjDueText({ ...fp, target: { mode: 'count', count: 52, done: 40 } }, '2026-11-06'), '31 days late (due 2026-10-06)');
        // ---- 2.36.0: materials / goods of a project ----
        const mp = { id: 'mp1', name: 'Radio repeater', status: 'active', start: '2026-11-01', end: '2026-12-31', createdByUid: 'o', prIds: ['prm'],
            target: { mode: 'milestones', milestones: [{ id: 'a', label: 'Instalasi Antenna', weight: 100, done: false }] }, updates: [] };
        projects.push(mp);
        purchaseRequests = [{ id: 'prm', prNo: 'PR/L/GPA1', status: 'partial', items: [{ id: 'i1', spec: 'Hytera HR1068', qty: 2, uom: 'Unit', receivedQty: 2 }, { id: 'i2', spec: 'Dipole Array Antenna', qty: 2, uom: 'Unit', receivedQty: 0 }] }];
        showProjectDetail('mp1');
        t('detail: bagian Materials kosong + tombol Add item & From PR', [/Goods that belong to no single milestone/.test(document.getElementById('pjDetailBody').textContent), [...document.querySelectorAll('.pj-mat__acts button')].map(b => b.textContent.trim())], [true, ['Add item', 'From PR/L/GPA1']]);
        showProjectMaterialForm('mp1');
        saveProjectMaterial();
        t('nama wajib', toasts[toasts.length - 1], 'Fill in the item name');
        document.getElementById('pjMatName').value = 'Kabel RF Feeder 7/8"';
        document.getElementById('pjMatQty').value = '140';
        document.getElementById('pjMatUom').value = prUomChoices()[0];
        document.getElementById('pjMatHave').value = '70';
        document.getElementById('pjMatNote').value = 'tarikan kedua menyusul';
        saveProjectMaterial(); await tick();
        let mp1 = projects.find(x => x.id === 'mp1');
        t('barang tersimpan di project', [mp1.materials.length, mp1.materials[0].name, mp1.materials[0].qty, mp1.materials[0].have], [1, 'Kabel RF Feeder 7/8"', 140, 70]);
        addProjectMaterialsFromPr('mp1', 'prm'); await tick();
        mp1 = projects.find(x => x.id === 'mp1');
        t('dari PR: 2 barang ikut, terhubung ke PR', [mp1.materials.length, mp1.materials.slice(1).map(m => [m.name, m.prItemId])], [3, [['Hytera HR1068', 'i1'], ['Dipole Array Antenna', 'i2']]]);
        addProjectMaterialsFromPr('mp1', 'prm'); await tick();
        t('dari PR lagi: tidak dobel', projects.find(x => x.id === 'mp1').materials.length, 3);
        const rowsM = () => [...document.querySelectorAll('.pj-mat__row')].map(r => [r.cells[0].querySelector('b').textContent, r.cells[2].textContent.trim(), r.cells[3].textContent.trim(), r.cells[4].textContent.trim()]);
        t('tabel: need / have / short / status', rowsM(), [['Kabel RF Feeder 7/8"', '70', '70', 'Partly'], ['Hytera HR1068', '2', '—', 'Complete'], ['Dipole Array Antenna', '0', '2', 'Not yet']]);
        t('ringkasan 1 dari 3 lengkap', [projectMaterialsSummary(mp1), /1 of 3 complete/.test(document.querySelector('.pj-mat__title').textContent)], [{ total: 3, ready: 1, partial: 1 }, true]);
        // The antenna arrives: received in the purchase request, the project follows.
        purchaseRequests[0].items[1].receivedQty = 2;
        showProjectDetail('mp1');
        t('barang dari PR mengikuti penerimaan di PR', rowsM()[2], ['Dipole Array Antenna', '2', '—', 'Complete']);
        showProjectMaterialForm('mp1', mp1.materials[2].id);
        t('barang dari PR: Have dikunci', [document.getElementById('pjMatHave').disabled, document.getElementById('pjMatHaveHint').hidden], [true, false]);
        closeProjectMaterialForm();
        removeProjectMaterial('mp1', mp1.materials[0].id); await tick();
        t('hapus barang', projects.find(x => x.id === 'mp1').materials.length, 2);
        // ---- 2.36.1: goods per milestone ----
        showProjectDetail('mp1');
        t('tiap milestone punya tombol + Item', document.querySelectorAll('.pj-ms__item .pj-ms__head button').length, 1);
        showProjectMaterialForm('mp1', '', 'a');
        t('form: milestone terpilih', [document.getElementById('pjMatMilestoneGroup').style.display, document.getElementById('pjMatMilestone').value, [...document.getElementById('pjMatMilestone').options].map(o => o.textContent)],
            ['', 'a', ['— No single milestone (other goods) —', 'Instalasi Antenna']]);
        document.getElementById('pjMatName').value = 'Mounting Antenna';
        document.getElementById('pjMatQty').value = '2';
        document.getElementById('pjMatHave').value = '1';
        saveProjectMaterial(); await tick();
        mp1 = projects.find(x => x.id === 'mp1');
        t('barang tersimpan untuk milestone itu', mp1.materials.filter(m => m.milestoneId === 'a').map(m => m.name), ['Mounting Antenna']);
        t('tampil di bawah milestone, bukan di Other goods', [[...document.querySelectorAll('.pj-ms__item .pj-mat__row td[data-label="Item"] b')].map(x => x.textContent), document.querySelector('.pj-ms__goods').textContent.trim(),
            [...document.querySelectorAll('.pj-ms ~ .pj-mat .pj-mat__row b, .pj-mat__title ~ .pj-mat .pj-mat__row b')].map(x => x.textContent).includes('Mounting Antenna')], [['Mounting Antenna'], '0/1', false]);
        // Move an item taken from the PR to the milestone.
        showProjectMaterialForm('mp1', mp1.materials.find(m => m.name === 'Dipole Array Antenna').id);
        document.getElementById('pjMatMilestone').value = 'a';
        saveProjectMaterial(); await tick();
        t('barang dari PR dipindah ke milestone', [...document.querySelectorAll('.pj-ms__item .pj-mat__row td[data-label="Item"] b')].map(x => x.textContent), ['Dipole Array Antenna', 'Mounting Antenna']);
        // Marking the milestone done while goods are short asks first.
        let askedMs = ''; window.confirm = m => { askedMs = m; return false; };
        toggleProjectTarget('mp1', 'milestone', 0, true); await tick();
        t('barang belum lengkap: ditanya dulu, dibatalkan', [/still waits for 1 item/.test(askedMs), /Mounting Antenna — have 1 of 2/.test(askedMs), projects.find(x => x.id === 'mp1').target.milestones[0].done], [true, true, false]);
        window.confirm = () => true;
        toggleProjectTarget('mp1', 'milestone', 0, true); await tick();
        t('tetap ditandai selesai bila dijawab ya', projects.find(x => x.id === 'mp1').target.milestones[0].done, true);
        switchProjectsTab('projects'); document.getElementById('pjStatusFilter') && (document.getElementById('pjStatusFilter').value = ''); renderProjectsList();
        t('kartu project: ringkasan semua barang', [...document.querySelectorAll('.pj-card__mat')].map(x => x.textContent.trim()), ['Goods 2/3 ready']);
        closeProjectDetail();
        return T;
    });
    let pass = 0;
    for (const x of T) {
        if (x.pass) pass++;
        console.log(`${x.pass ? 'PASS' : 'FAIL'}  ${x.n}`);
        if (!x.pass) console.log(`        dapat=${JSON.stringify(x.g)} harap=${JSON.stringify(x.w)}`);
    }
    console.log(`\n${pass}/${T.length} lulus`);
    console.log('PAGE ERRORS:', errors.length ? errors : 'none');
    await b.close();
    process.exit(pass === T.length && !errors.length ? 0 : 1);
})();
