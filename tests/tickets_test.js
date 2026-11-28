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
        // 2.45.0 (S9): a new ticket is what firestore.rules accepts from a non-OT account.
        const TK_NEW_KEYS = ['id', 'no', 'title', 'category', 'priority', 'description', 'location', 'units', 'company', 'photoIds', 'status', 'reportedByUid',
            'reportedBy', 'reportedByEmail', 'reportedAt', 'assigneeUid', 'assigneeName', 'jobIds', 'events', 'updatedAt', 'lastActorUid'];
        t('S9 ticket baru: hanya kolom yang diizinkan rules, tanpa PIC, satu event "created" atas nama pelapor',
          [Object.keys(t0).every(k => TK_NEW_KEYS.includes(k)), t0.assigneeUid, t0.assigneeName, t0.reportedBy, t0.reportedByEmail, t0.lastActorUid,
           t0.events.length, Object.keys(t0.events[0]).sort(), [t0.events[0].by, t0.events[0].byUid]],
          [true, '', '', 'Andi', 'khl@x.id', 'khl', 1, ['at', 'by', 'byUid', 'text', 'type'], ['Andi', 'khl']]);
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
        t('S9 pelapor menutup: tepat satu event "closed" miliknya di akhir, yang lama utuh',
          [after.events.length - before.events.length, JSON.stringify(after.events.slice(0, -1)) === JSON.stringify(before.events), after.events[after.events.length - 1].type, after.events[after.events.length - 1].byUid, after.resolvedAt === before.resolvedAt],
          [1, true, 'closed', 'khl', true]);
        // ---- 2.45.0 (S9): "Not fixed — reopen" by the reporter clears resolvedAt ----
        const solvedAt = Date.now() - 2 * 3600000;
        tickets.push({ id: 'tk_r1', no: 'TK-2', title: 'Display mati', category: 'Display', priority: 'medium', status: 'resolved', reportedByUid: 'khl', reportedBy: 'Andi',
            reportedAt: Date.now() - 5 * 3600000, resolvedAt: solvedAt, resolution: 'Kabel diganti', assigneeUid: 'ot', assigneeName: 'Rasyid',
            events: [{ at: solvedAt, by: 'Rasyid', byUid: 'ot', type: 'resolved', text: 'Kabel diganti' }], updatedAt: solvedAt });
        const before2 = JSON.parse(JSON.stringify(tickets.find(x => x.id === 'tk_r1')));
        promptAns = '  Masih mati pagi ini  ' + 'x'.repeat(2100);
        setTicketStatus('tk_r1', 'open'); await tick();
        const re = saved[saved.length - 1];
        const changed2 = Object.keys({ ...re, ...before2 }).filter(k => JSON.stringify(re[k]) !== JSON.stringify(before2[k])).sort();
        // 2.48.0: punya PIC → kembali ke Assigned (bukan "Open / belum ada PIC")
        t('S9 pelapor membuka lagi: kembali ke PIC (assigned), resolvedAt & closedAt kosong', [re.id, re.status, re.resolvedAt, re.closedAt], ['tk_r1', 'assigned', null, null]);
        t('S9 pelapor membuka lagi: hanya kolom yang diizinkan rules berubah', changed2.every(k => ['events', 'status', 'closedAt', 'resolvedAt', 'updatedAt', 'lastActorUid'].includes(k)), true);
        t('S9 pelapor membuka lagi: satu event "reopen" miliknya, alasan maks. 2000 karakter',
          [re.events.length, re.events[1].type, re.events[1].byUid, re.events[1].by, re.events[1].text.length, re.events[1].text.startsWith('Masih mati pagi ini')],
          [2, 'reopen', 'khl', 'Andi', 2000, true]);
        promptAns = 'Antena diganti';
        // The OT team's "Average time to fix" leaves out a ticket that is open again, even one with an old resolvedAt.
        currentUserDoc = { uid: 'ot', role: 'staff', status: 'active', displayName: 'Rasyid', access: { tickets: 'edit' } }; currentUser = { uid: 'ot', email: 'ot@x.id' };
        applyRoleGating(); renderTicketsView();
        const tiles = () => [...document.querySelectorAll('#tkTiles .wh-tile')].map(x => x.querySelector('.wh-tile__value').textContent);
        const avgBefore = tiles()[4];
        tickets.push({ id: 'tk_old', no: 'TK-3', title: 'HT', category: 'Radio / HT', priority: 'low', status: 'open', reportedByUid: 'khl', reportedAt: Date.now() - 3 * 86400000, resolvedAt: Date.now() - 86400000, events: [] });
        renderTicketsView();
        t('S9 tile Average time to fix: ticket yang dibuka lagi tidak dihitung', tiles()[4], avgBefore);
        tickets = tickets.filter(x => x.id !== 'tk_old');
        // ---- SLA terlewat ----
        tickets.push({ id: 'old', no: 'TK-0', title: 'Radio mati', category: 'Radio / HT', priority: 'medium', status: 'open', reportedAt: Date.now() - 30 * 3600000, reportedByUid: 'x', events: [] });
        t('lewat 1 hari (Medium): overdue', ticketOverdue(tickets.find(x => x.id === 'old')), true);
        currentUserDoc = { uid: 'ot', role: 'staff', status: 'active', access: { tickets: 'edit' } }; currentUser = { uid: 'ot', email: 'ot@x.id' };
        applyRoleGating(); renderTicketsView();
        document.getElementById('tkStatusFilter').value = 'overdue'; renderTicketsList();
        t('filter Overdue', [...document.querySelectorAll('#tkList .tk-row')].length, 1);
        t('baris overdue diberi tanda', !!document.querySelector('#tkList .tk-row.is-late'), true);
        // 2.45.0: tickets as any active account can write them (the rules check only title, category,
        // priority, status and that events is a list) — the Tickets page, the detail and the job links keep working.
        let thrown = '';
        try {
            applyTicketsSnapshot([{ id: 'x1', no: 'TK-X1', title: 'Odd one', category: 'Other', priority: 'low', status: 'open', reportedAt: Date.now(), reportedByUid: 'z', events: [null, 5], units: 5, photoIds: 'abc', jobIds: 7, location: 12 },
                { id: 'x2', no: 'TK-X2', title: 'Another', category: 'Other', priority: 'low', status: 'open', reportedAt: Date.now(), reportedByUid: 'z', events: 'oops', units: [null, { id: 'u1', name: 'GGTR136G' }] }, null, 3]);
            document.getElementById('tkStatusFilter').value = 'active'; document.getElementById('tkSearch').value = 'ggtr'; renderTicketsView();
            showTicketDetail('x1'); showTicketDetail('x2'); closeTicketDetail();
        } catch (e) { thrown = e.message; }
        t('2.45.0: ticket aneh: daftar, pencarian unit & detail tetap jalan', [thrown, tickets.length, [...document.querySelectorAll('#tkList .tk-row')].map(r => r.querySelector('.tk-no').textContent)], ['', 2, ['TK-X2']]);
        const x1 = tickets.find(x => x.id === 'x1');
        t('2.45.0: ticket aneh dibaca rapi', [x1.events, x1.units, x1.photoIds, x1.jobIds, x1.location, ticketOfJob({ id: 'j9' }), ticketJobs({ id: 'x9', jobIds: 'abc' })], [[], [], [], [], '12', null, []]);
        // Times written as maps with a toString key: every sort of them threw, and the page went stale.
        thrown = '';
        try {
            applyTicketsSnapshot([{ id: 'x3', no: 'TK-X3', title: 'Bad time', category: 'Other', priority: 'high', status: 'resolved', reportedAt: { toString: 'x' }, updatedAt: { toString: 'x' },
                resolvedAt: { toString: 'x' }, reportedByUid: 'z', events: [{ type: 'resolved', at: { toString: 'x' } }, { type: 'resolved', at: 1 }] },
                { id: 'x4', no: 'TK-X4', title: 'Fine', category: 'Other', priority: 'low', status: 'open', reportedAt: Date.now(), reportedByUid: 'z', resolvedAt: null }]);
            document.getElementById('tkStatusFilter').value = 'all'; document.getElementById('tkSearch').value = ''; renderTicketsView();
            showTicketDetail('x3'); closeTicketDetail(); updateTicketBadge();
        } catch (e) { thrown = e.message; }
        const x3 = tickets.find(x => x.id === 'x3');
        t('2.45.0: waktu ticket berupa map → halaman tetap jalan, waktunya dibuang, null tetap', [thrown, tickets.length, 'reportedAt' in x3, 'resolvedAt' in x3, x3.events.map(e => e.at), tickets.find(x => x.id === 'x4').resolvedAt],
            ['', 2, false, false, [undefined, 1], null]);
        return T;
    });
    let pass = 0;
    for (const x of T) { if (x.pass) pass++; console.log(`${x.pass ? 'PASS' : 'FAIL'}  ${x.n}`); if (!x.pass) console.log(`        dapat=${JSON.stringify(x.g)} harap=${JSON.stringify(x.w)}`); }
    console.log(`\n${pass}/${T.length} lulus`);
    console.log('PAGE ERRORS:', errors.length ? errors : 'none');
    await b.close();
    process.exit(pass === T.length && !errors.length ? 0 : 1);
})();
