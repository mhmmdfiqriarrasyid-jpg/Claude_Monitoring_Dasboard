// 2.30.0 — Tickets: semua akun aktif melapor dan mengikuti ticket miliknya;
// tim OT (tickets edit) menugaskan PIC, mengerjakan, menyelesaikan; pelapor
// mengonfirmasi (closed) atau membuka lagi; SLA per prioritas; job dari ticket.
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
        window.confirm = () => true; let promptAns = 'Antena diganti'; window.prompt = () => promptAns;
        const saved = [], jobs = [];
        const ok = () => Promise.resolve();
        window.cloud = { isReady: true, addHistoryEvents: ok, saveTicketPhoto: ok, saveTicketStaff: ok, saveProjectPhoto: ok, saveUnits: ok,
            saveTicket: r => { saved.push(JSON.parse(JSON.stringify(r))); return ok(); },
            saveOtJob: r => { jobs.push(JSON.parse(JSON.stringify(r))); return ok(); }, saveProject: ok };
        globalData = [{ id: 'u1', name: 'GGTR136G', sn: 'S1', site: 'PT. Global Papua Abadi', status: 'Good' }];
        tickets = []; otJobs = []; projects = [];
        // ---- pelapor (KHL, tanpa akses tickets) ----
        currentUser = { uid: 'khl', email: 'khl@x.id' }; currentUserDoc = { uid: 'khl', role: 'khl', status: 'active', displayName: 'Andi', access: {} };
        hideAuthGates(); applyRoleGating();
        t('menu Tickets tampil untuk semua akun aktif', document.querySelector('.nav__link[data-view="tickets"]').style.display !== 'none', true);
        navigateTo('tickets');
        t('halaman terbuka untuk akun tanpa akses area', currentView, 'tickets');
        showAddTicketForm();
        saveTicket();
        t('judul wajib', toasts.pop(), 'Say in one line what the problem is');
        document.getElementById('tkTitle').value = 'GPS hilang sinyal RTK di JAGF-1';
        document.querySelector('input[name="tkPriority"][value="high"]').checked = true;
        document.getElementById('tkUnitAdd').value = 'GGTR136G';
        document.getElementById('tkLocation').value = 'JAGF-1-A-001';
        saveTicket(); await tick();
        const t0 = saved[saved.length - 1];
        t('ticket tersimpan: open, high, unit, PT dari unit, pelapor', [t0.status, t0.priority, t0.units[0].id, t0.company, t0.reportedByUid, t0.events[0].type, /^TK-[0-9]{6}-/.test(t0.no)],
          ['open', 'high', 'u1', 'PT. Global Papua Abadi', 'khl', 'created', true]);
        t('due 4 jam untuk High', ticketDueAt(t0) - t0.reportedAt, 4 * 3600000);
        t('pelapor tidak melihat tile tim OT', /My open tickets/.test(document.getElementById('tkTiles').textContent), true);
        showTicketDetail(t0.id);
        t('pelapor: hanya tombol tutup jendela (belum resolved)', [...document.querySelectorAll('#tkDetailActions button')].map(x => x.textContent.trim()), ['Close window']);
        setTicketStatus(t0.id, 'progress');
        t('pelapor tidak bisa mengubah status', /Only the OT team/.test(toasts.pop()), true);
        closeTicketDetail();
        // ---- tim OT ----
        currentUser = { uid: 'ot', email: 'ot@x.id' }; currentUserDoc = { uid: 'ot', role: 'staff', status: 'active', displayName: 'Rasyid', access: { tickets: 'edit', projects: 'edit' } };
        applyRoleGating(); renderTicketsView();
        t('OT: tile New = 1, badge = 1', [document.querySelector('#tkTiles .wh-tile__value').textContent, document.getElementById('ticketBadge').textContent], ['1', '1']);
        t('Decision inbox: ticket baru masuk', ticketDecisionGroup().total, 1);
        showTicketDetail(t0.id);
        document.getElementById('tkAssignee').value = 'ot';
        assignTicket(t0.id); await tick();
        let cur = tickets.find(x => x.id === t0.id);
        t('assign: status assigned + PIC', [cur.status, cur.assigneeUid, cur.assigneeName], ['assigned', 'ot', 'Rasyid']);
        jobFromTicket(t0.id);
        t('Log job dari ticket: unit & deskripsi terisi', [_jobUnits.map(u => u.id), /GPS hilang/.test(document.getElementById('otJobDesc').value)], [['u1'], true]);
        document.getElementById('otJobCategory').value = 'Troubleshooting';
        saveJob(); await tick();
        cur = tickets.find(x => x.id === t0.id);
        t('job tertaut, ticket jadi In progress', [cur.jobIds.length, cur.status, cur.events.some(e => e.type === 'job')], [1, 'progress', true]);
        setTicketStatus(t0.id, 'resolved'); await tick();
        cur = tickets.find(x => x.id === t0.id);
        t('resolve: catatan perbaikan wajib & tersimpan', [cur.status, cur.resolution, !!cur.resolvedAt], ['resolved', 'Antena diganti', true]);
        // ---- pelapor menutup ----
        currentUser = { uid: 'khl', email: 'khl@x.id' }; currentUserDoc = { uid: 'khl', role: 'khl', status: 'active', displayName: 'Andi', access: {} };
        applyRoleGating(); updateTicketBadge();
        t('pelapor: badge "please confirm"', document.getElementById('ticketBadge').textContent, '1');
        showTicketDetail(t0.id);
        t('pelapor: tombol konfirmasi', [...document.querySelectorAll('#tkDetailActions button')].map(x => x.textContent.trim()), ['Not fixed — reopen', 'Fixed — close', 'Close window']);
        const before = JSON.parse(JSON.stringify(cur));
        setTicketStatus(t0.id, 'closed'); await tick();
        const after = saved[saved.length - 1];
        const changed = Object.keys(after).filter(k => JSON.stringify(after[k]) !== JSON.stringify(before[k])).sort();
        t('pelapor menutup: hanya kolom yang diizinkan rules berubah', changed.every(k => ['events', 'status', 'closedAt', 'updatedAt', 'lastActorUid'].includes(k)), true);
        t('closed', after.status, 'closed');
        // ---- SLA terlewat ----
        tickets.push({ id: 'old', no: 'TK-0', title: 'Radio mati', category: 'Radio / HT', priority: 'medium', status: 'open', reportedAt: Date.now() - 30 * 3600000, reportedByUid: 'x', events: [] });
        t('lewat 1 hari (Medium): overdue', ticketOverdue(tickets.find(x => x.id === 'old')), true);
        currentUserDoc = { uid: 'ot', role: 'staff', status: 'active', access: { tickets: 'edit' } }; currentUser = { uid: 'ot', email: 'ot@x.id' };
        applyRoleGating(); renderTicketsView();
        document.getElementById('tkStatusFilter').value = 'overdue'; renderTicketsList();
        t('filter Overdue', [...document.querySelectorAll('#tkList .tk-row')].length, 1);
        t('baris overdue diberi tanda', !!document.querySelector('#tkList .tk-row.is-late'), true);
        return T;
    });
    let pass = 0;
    for (const x of T) { if (x.pass) pass++; console.log(`${x.pass ? 'PASS' : 'FAIL'}  ${x.n}`); if (!x.pass) console.log(`        dapat=${JSON.stringify(x.g)} harap=${JSON.stringify(x.w)}`); }
    console.log(`\n${pass}/${T.length} lulus`);
    console.log('PAGE ERRORS:', errors.length ? errors : 'none');
    await b.close();
    process.exit(pass === T.length && !errors.length ? 0 : 1);
})();
