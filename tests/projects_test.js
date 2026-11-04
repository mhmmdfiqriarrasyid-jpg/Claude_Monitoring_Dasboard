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

        // ---- hapus ----
        deleteProject(projects[0].id);
        t('project dihapus', projects.length, 0);
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
