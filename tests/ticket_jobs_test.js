// 2.30.1 — Ticket <-> Jobs & Activities: job menyimpan ticketId, label ticket
// di tabel Jobs, pilih "Part of ticket" di form, job Done menawarkan Resolve,
// dan field issue di job bisa dijadikan ticket.
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
        let confirmAns = true, asked = ''; window.confirm = m => { asked = m; return confirmAns; };
        const ok = () => Promise.resolve();
        window.cloud = { isReady: true, addHistoryEvents: ok, saveTicketPhoto: ok, saveTicketStaff: ok, saveProjectPhoto: ok, saveUnits: ok, saveProject: ok,
            saveTicket: ok, saveOtJob: ok };
        globalData = [{ id: 'u1', name: 'GGTR123G_LSD', sn: 'S1', site: 'PT. Global Papua Abadi', status: 'Good' }];
        const now = Date.now();
        tickets = [{ id: 'tk1', no: 'TK-261105-0800AB', title: 'Lime spreader salah baca berat', category: 'Implement', priority: 'high', status: 'assigned',
                     units: [{ id: 'u1', name: 'GGTR123G_LSD', sn: 'S1' }], reportedAt: now, reportedByUid: 'khl', events: [], jobIds: [] },
                   { id: 'tk2', no: 'TK-261105-0900CD', title: 'Ticket lain', category: 'Other', priority: 'low', status: 'open', reportedAt: now - 1000, reportedByUid: 'khl', events: [], jobIds: [] },
                   { id: 'tk3', no: 'TK-261101-0900EF', title: 'Sudah tutup', category: 'Other', priority: 'low', status: 'closed', reportedAt: now - 5000, reportedByUid: 'khl', events: [], jobIds: [] }];
        otJobs = []; projects = [];
        currentUser = { uid: 'ot', email: 'ot@x.id' }; currentUserDoc = { uid: 'ot', role: 'staff', status: 'active', displayName: 'Rasyid', access: { tickets: 'edit', projects: 'edit' } };
        hideAuthGates(); applyRoleGating();
        navigateTo('projects'); switchProjectsTab('jobs');

        // ---- Log job dari ticket ----
        jobFromTicket('tk1');
        const sel = document.getElementById('otJobTicket');
        t('form: pilihan ticket tampil & terisi, status In progress', [document.getElementById('otJobTicketGroup').style.display, sel.value, document.getElementById('otJobStatus').value], ['', 'tk1', 'progress']);
        t('pilihan: hanya ticket yang belum selesai', [...sel.options].map(o => o.value), ['', 'tk1', 'tk2']);
        document.getElementById('otJobCategory').value = 'Calibration';
        saveJob(); await tick();
        let j = otJobs[0], tk = tickets.find(x => x.id === 'tk1');
        t('job menyimpan ticketId; ticket mencatat job & jadi In progress', [j.ticketId, tk.jobIds, tk.status, tk.events.some(e => e.type === 'job')], ['tk1', [j.id], 'progress', true]);
        document.getElementById('jobPeriod').value = ''; renderJobsList();
        t('tabel Jobs: label ticket', /Ticket TK-261105-0800AB · In progress/.test(document.getElementById('jobBody').textContent), true);
        document.getElementById('jobSearch').value = 'TK-261105-0800AB'; renderJobsList();
        t('cari dengan nomor ticket', document.querySelectorAll('#jobBody tr').length, 1);
        document.getElementById('jobSearch').value = '';

        // ---- job selesai -> tawarkan Resolve ----
        editJob(j.id);
        t('edit: ticket tetap terpilih', document.getElementById('otJobTicket').value, 'tk1');
        document.getElementById('otJobStatus').value = 'done';
        saveJob(); await tick();
        tk = tickets.find(x => x.id === 'tk1');
        t('Done: ditanya resolve, ticket resolved dengan resolusi dari job', [/Resolve ticket TK-261105-0800AB/.test(asked), tk.status, tk.resolution], [true, 'resolved', 'Calibration: Lime spreader salah baca berat']);

        // ---- detail ticket menampilkan job + buka di Jobs ----
        showTicketDetail('tk1');
        t('detail ticket: daftar Jobs & activities', /Jobs & activities/.test(document.getElementById('tkDetailBody').textContent) && /Done/.test(document.querySelector('#tkDetailBody .pj-jobline').textContent), true);
        openTicketJobs('tk1');
        t('Open in Jobs & Activities: tab jobs, dicari nomor ticket', [currentView, projectsTab, document.getElementById('jobSearch').value, document.querySelectorAll('#jobBody tr').length], ['projects', 'jobs', 'TK-261105-0800AB', 1]);
        document.getElementById('jobSearch').value = '';

        // ---- job lama ditautkan ke ticket lain, lalu dipindah ----
        otJobs.push({ id: 'jobOld', date: toISODate(), category: 'Troubleshooting', description: 'Cek display', status: 'progress', units: [], createdByUid: 'ot', issue: '1. Display mati - tunggu spare part' });
        editJob('jobOld');
        document.getElementById('otJobTicket').value = 'tk2';
        saveJob(); await tick();
        t('job lama ditautkan ke tk2', [otJobs.find(x => x.id === 'jobOld').ticketId, tickets.find(x => x.id === 'tk2').jobIds, tickets.find(x => x.id === 'tk2').status], ['tk2', ['jobOld'], 'progress']);
        editJob('jobOld');
        document.getElementById('otJobTicket').value = '';
        saveJob(); await tick();
        t('dilepas: ticket tidak lagi mencatat job', [otJobs.find(x => x.id === 'jobOld').ticketId, tickets.find(x => x.id === 'tk2').jobIds], ['', []]);

        // ---- field issue -> ticket baru ----
        renderJobsList();
        const raise = [...document.querySelectorAll('#jobBody button')].find(x => /Raise a ticket/.test(x.textContent));
        t('job dengan issue tanpa ticket: tombol Raise a ticket', !!raise, true);
        ticketFromJob('jobOld');
        t('form ticket terisi dari issue', [document.getElementById('tkTitle').value, /From job/.test(document.getElementById('tkDesc').value)], ['Display mati - tunggu spare part', true]);
        saveTicket(); await tick();
        const nt = tickets.find(x => x.title === 'Display mati - tunggu spare part');
        t('ticket baru menautkan job, job menunjuk ticket baru', [nt && nt.jobIds, otJobs.find(x => x.id === 'jobOld').ticketId === (nt && nt.id)], [['jobOld'], true]);

        // ---- tanpa akses tickets: pilihan disembunyikan, tautan tetap ----
        currentUserDoc = { uid: 'ot', role: 'staff', status: 'active', access: { projects: 'edit' } }; applyRoleGating();
        editJob(j.id);
        t('tanpa Tickets edit: pilihan ticket disembunyikan', document.getElementById('otJobTicketGroup').style.display, 'none');
        saveJob(); await tick();
        t('simpan tanpa akses: ticketId tidak hilang', otJobs.find(x => x.id === j.id).ticketId, 'tk1');
        return T;
    });
    let pass = 0;
    for (const x of T) { if (x.pass) pass++; console.log(`${x.pass ? 'PASS' : 'FAIL'}  ${x.n}`); if (!x.pass) console.log(`        dapat=${JSON.stringify(x.g)} harap=${JSON.stringify(x.w)}`); }
    console.log(`\n${pass}/${T.length} lulus`);
    console.log('PAGE ERRORS:', errors.length ? errors : 'none');
    await b.close();
    process.exit(pass === T.length && !errors.length ? 0 : 1);
})();
