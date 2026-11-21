// 2.39.0 — the manager's Daily Report on the server: functions/dailyCore.js
// (data, pages, email), dailyDeck.js (PowerPoint + PDF) and dailySend.js
// (records and photos read, email with both files attached) — with a fake
// Firestore and Storage. Node only (needs `npm install` in functions/).
const path = require('path');
const F = p => path.join(__dirname, '..', 'functions', p);
const T = []; const t = (name, g, w) => T.push({ name, g, w, pass: JSON.stringify(g) === JSON.stringify(w) });
const C = require(F('dailyCore.js'));
const { renderPptx, renderPdf, fitLine } = require(F('dailyDeck.js'));
const { composeDaily } = require(F('dailySend.js'));
const sharp = require(F('node_modules/sharp'));
process.env.TZ = 'Asia/Jayapura';
const at = (d, h) => new Date(`${d}T${String(h).padStart(2, '0')}:00:00`).getTime();
const day = '2026-11-10';
const data = {
    teamMembers: [{ id: 'm1', name: 'Budi Santoso', company: 'PT. Global Papua Abadi' }, { id: 'm2', name: 'Andi Wijaya', company: 'PT. Murni Nusantara Mandiri' },
        { id: 'm3', name: 'Rudi Hartono', company: 'PT. Global Papua Abadi' }, { id: 'm4', name: 'Old Member', active: false }],
    shifts: [{ id: `${day}_m1`, date: day, memberId: 'm1', shift: 'pagi' }, { id: `${day}_m2`, date: day, memberId: 'm2', shift: 'pagi' }, { id: `${day}_m3`, date: day, memberId: 'm3', shift: 'libur' }],
    leaveRequests: [],
    workLogs: [{ id: 'wl1', date: day, memberId: 'm1', memberName: 'Budi Santoso', start: '07:00', end: '15:30', approval: 'approved', photoCount: 2,
        jobs: [{ paddock: 'BLAF-2-E-005', jobType: 'Tillage', units: [{ id: 'u1', name: 'GGTR101G' }], photoCount: 1 }, { paddock: 'JAGF-1-A-001', jobType: 'Planting', units: [{ id: 'u2', name: 'GGTR102G' }], photoCount: 1 }],
        issue: 'Selang hidrolik GGTR101G bocor' },
        { id: 'wl2', date: day, memberId: 'm2', memberName: 'Andi Wijaya', start: '07:00', end: '12:00', approval: 'draft' }],
    tickets: [{ id: 'k1', no: 'TK-1', title: 'Display mati', priority: 'high', status: 'assigned', assigneeName: 'Rasyid', reportedAt: at(day, 9), photoIds: ['tp1'] },
        { id: 'k2', no: 'TK-0', title: 'GPS lost', priority: 'low', status: 'resolved', reportedAt: at('2026-11-08', 9), resolvedAt: at(day, 14) }],
    damageRecords: [{ id: 'd1', date: day, unitId: 'u1', unitName: 'GGTR101G', component: 'Hydraulic', damageType: 'Mekanis', description: 'Hose leak' },
        { id: 'd2', date: '2026-11-01', unitId: 'u2', unitName: 'GGTR102G', component: 'Display', resolved: true, resolvedAt: day }],
    otJobs: [{ id: 'j1', date: day, category: 'Calibration', description: 'Kalibrasi steering', status: 'done', ticketId: 'k1', photoIds: ['jp1'] }],
    projects: [{ id: 'p1', name: 'RTK Tower', status: 'progress', target: { mode: 'count', count: 4, done: 1 }, updates: [{ date: day, text: 'Pondasi selesai', progress: 'progress 0% → 25%', blocker: 'Semen kurang' }] }],
    units: [{ id: 'u1', name: 'GGTR101G', status: 'Breakdown' }, { id: 'u2', name: 'GGTR102G', status: 'Good' }]
};

(async () => {
    const jpg = await sharp({ create: { width: 640, height: 480, channels: 3, background: { r: 90, g: 140, b: 80 } } }).jpeg().toBuffer();
    const photoDocs = { workLogPhotos: { wl1: { photos: ['sp:photos/workLogPhotos/wl1/a', 'sp:photos/workLogPhotos/wl1/b'] } }, ticketPhotos: { tp1: { photo: 'data:image/jpeg;base64,' + jpg.toString('base64') } }, projectPhotos: {} };
    const asked = [];
    // A Firestore that answers the few reads dailySend.js makes.
    const coll = name => {
        const rows = (data[name] || []).map(x => ({ id: x.id, data: () => { const { id, ...r } = x; return r; } }));
        const q = filters => ({ where: (f, op, v) => q(filters.concat([[f, v]])), get: async () => { asked.push(name + filters.map(x => `:${x[0]}`).join('')); return { docs: rows.filter(r => filters.every(([f, v]) => r.data()[f] === v)) }; },
            doc: id => ({ get: async () => { const d = (photoDocs[name] || {})[id]; return { exists: !!d, data: () => d }; } }) });
        return q([]);
    };
    const db = { collection: coll };
    const bucket = { file: p => ({ download: async () => { if (/\/b$/.test(p)) throw new Error('gone'); return [jpg]; } }) };

    const D = C.buildDaily({ date: day, now: at('2026-11-11', 7), members: data.teamMembers, shifts: data.shifts, leaves: [], workLogs: data.workLogs, tickets: data.tickets,
        damages: data.damageRecords, jobs: data.otJobs, projects: data.projects, units: data.units });
    t('kemarin dari jam 07:00 WIT', C.yesterday(at('2026-11-11', 7)), day);
    t('Daily Report: Andi masih draft = tidak lapor, Rudi libur, member nonaktif diabaikan', D.team.members.map(m => [m.name, m.state]), [['Andi Wijaya', 'miss'], ['Budi Santoso', 'ok'], ['Rudi Hartono', 'off']]);
    t('angka: 1 dari 2, 8.5 jam, 2 paddock, 2 unit, jam per job', [D.team.filed, D.team.expected, D.team.pct, D.team.hours, D.team.paddocks, D.team.units, D.team.jobTypes], [1, 2, 50, 8.5, 2, 2, [['Tillage', 4.25], ['Planting', 4.25]]]);
    t('kerusakan: baru hari itu (unit breakdown), perbaikan selesai hari itu', [D.damage.map(d => [d.unit, d.breakdown]), D.fixed, D.breakdownNow], [[['GGTR101G', true]], 1, 1]);
    t('ticket: masuk & selesai hari itu, lewat due', [D.tickets.opened.map(x => x.no), D.tickets.resolved.map(x => x.no), D.tickets.open, D.tickets.overdue], [['TK-1'], ['TK-0'], 1, 1]);
    t('jobs & project', [D.jobs.map(j => [j.category, j.ticket]), D.updates.map(u => [u.project, u.pct, u.s.blockers[0]])], [[['Calibration', 'TK-1']], [['RTK Tower', 25, 'Semen kurang']]]);
    t('foto: per paddock, lalu ticket & job', D.photos.map(p => [p.key, p.index]), [['wl:wl1:0', 0], ['wl:wl1:1', 1], ['tk:tp1', undefined], ['pj:jp1', undefined]]);
    const P = C.dailyPages(D, {});
    t('halaman tanpa foto', P.map(p => p.title), ['Daily Report', 'Daily Report Summary', 'Issue & Damage', 'OT Jobs & Project Progress', 'Terima Kasih']);
    t('template perusahaan', P.every(p => p.theme === 'msg'), true);
    const jl = P[3].cols[0][0].items[0], ul = P[3].cols[1][0].items[0];
    t('job: teks lengkap, status sebagai label, keterangan informatif (2.39.2)', [jl.text, jl.tag, jl.sub, jl.note], ['Kalibrasi steering', { text: 'Done', tone: 'good' }, 'Calibration · Ticket TK-1', ''] );
    t('update project: nama, progress awal → akhir hari, blocker (2.40.0)', [ul.head, ul.text, ul.tag, ul.note], ['RTK Tower', 'Pondasi selesai', { text: '0% → 25%', tone: 'good' }, 'Blocker: Semen kurang']);
    const ts = C.summarizeUpdates({ updates: [{ date: day, at: 1, auto: true, text: 'Marked done: Antenna', progress: 'progress 0% → 19%' }, { date: day, at: 2, auto: true, text: 'Marked not done: Antenna', progress: 'progress 19% → 0%' }] }, day, day);
    t('centang lalu batal di hari yang sama: tidak ada perubahan, tidak ditampilkan', [ts.empty, C.projectEntry('X', ts, 0)], [true, null]);
    const noJobs = C.dailyPages(C.buildDaily({ date: day, projects: data.projects }), {}).find(p => p.title === 'OT Jobs & Project Progress');
    t('tanpa job: strip satu baris, update project jadi kartu utama', [noJobs.strip, noJobs.cols.map(c => c.map(b => b.title))], ['No job logged outside the Daily Report', [['Project updates · 1'], ['Running projects, % done']]]);
    const m = C.dailyEmail(D, { attached: true, photos: 2 });
    t('email: subjek & isi', [m.subject, /Missed: Andi Wijaya/.test(m.html), /Selang hidrolik/.test(m.html), /RTK Tower/.test(m.text) || /Project updates: 1/.test(m.text)], ['Daily Report — Tue 10 Nov 2026', true, true, true]);
    t('email: HTML di-escape', C.dailyEmail(C.buildDaily({ date: day, workLogs: [{ id: 'x', date: day, memberId: 'q', memberName: '<b>', issue: '<script>' }] })).html.includes('&lt;script&gt;'), true);
    t('laporan Office bertingkat: satu baris rapi, tidak terpotong (2.39.3)', C.buildDaily({ date: day, members: [{ id: 'a', name: 'Yuda' }], workLogs: [{ id: 'w', date: day, memberId: 'a', place: 'office', start: '07:00', end: '13:00',
        task: 'Lokasi: Tower KM 8\nPekerjaan dan Unit :\n- Setting radio repeater dan antena\n- Cek kabel coax' }] }).team.members[0].work,
        'Office · Lokasi: Tower KM 8 · Pekerjaan dan Unit: Setting radio repeater dan antena · Cek kabel coax');
    const { pageOps } = require(F('dailyDeck.js'));
    const sizes = pageOps(noJobs).filter(o => o.t === 'text' && o.y > 1.2).map(o => o.size);
    t('halaman yang isinya sedikit diperbesar sampai memenuhi slide (2.40.1)', Math.max(...sizes) > 12, true);
    const G = C.galleryLayout([4 / 3, 0.66, 4 / 3, 4 / 3, 0.66, 4 / 3], 9.2, 4.2);
    t('galeri: potret tetap potret, lanskap tetap lanskap, semua di dalam halaman (2.40.2)', [G.every((g, i) => (g.w < g.h) === ([1, 4].includes(i))), G.every(g => g.x >= -1e-9 && g.x + g.w <= 9.2 + 1e-9 && g.y + g.h <= 4.2 + 1e-9)], [true, true]);
    t('teks dipotong ke kotaknya', fitLine('Pasang Camera AI di seluruh unit traktor GPA', 1, 10), 'Pasang Camer…');

    // 2.45.0: records as users can write them (the rules check only a few fields). None of them may stop
    // the report: the odd parts are skipped, everything else is counted as before.
    const odd = {
        teamMembers: [{ id: 'm1', name: 'Budi Santoso', company: 'PT. Global Papua Abadi' }, { id: 'm5', name: 42, company: { x: 1 } }],
        workLogs: [{ id: 'wl1', date: day, memberId: 'm1', memberName: 'Budi', start: '07:00', end: '15:00', approval: 'approved', photoCount: 3,
            jobs: [null, { paddock: 'BLAF-2-E-005', jobType: 'Tillage', units: 'x', photoCount: 2 }, { paddock: { a: 1 }, jobType: 7, units: [null, { id: 'u1' }] }], issue: 12 },
            { id: 'wl2', date: day, memberId: 'm5', start: '07:00', end: '09:00', jobs: 'abc', units: 5, issue: { a: 1 } }],
        tickets: [{ id: 'k1', no: 'TK-1', title: 'Display mati', priority: 'high', status: 'open', reportedAt: at(day, 9), events: [null, 5, { type: 'resolved', at: 'x' }], units: 5, photoIds: 'abc', jobIds: 7 },
            { id: 'k2', no: 'TK-2', title: { a: 1 }, priority: 'constructor', status: 'resolved', reportedAt: at(day, 8), events: 'oops', units: [null, 'GGTR', { id: 'u2' }], photoIds: [null, 'tp9'] },
            { id: 'k3', no: 'TK-3', title: 'Old', priority: 'low', status: 'resolved', resolvedAt: 'yesterday', events: { 0: { type: 'resolved', at: at(day, 12) } } }],
        otJobs: [{ id: 'j1', date: day, category: { a: 1 }, description: 5, status: 'constructor', ticketId: 'k9', units: 'GGTR', photoIds: 'jp1' }, { id: 'j2', date: day, units: [null], photoIds: [3, 'jp2'] }],
        projects: [{ id: 'p1', name: 'RTK Tower', status: 'progress', target: { mode: 'units', units: 'x' } },
            { id: 'p2', name: 'No updates', status: 'progress', target: { mode: 'milestones', milestones: [null, { label: 'A', weight: 50, done: true }] }, updates: [null, 'x', { date: day, text: 'Pondasi', progress: 'progress 0% → 50%', photoIds: 'pp' }] },
            { id: 'p3', name: 7, status: 'progress', target: 'x', updates: { 0: 1 } }, { id: 'p4', status: 'progress', target: { units: [null, { done: true }] }, updates: [{ date: day, text: 'u', photoIds: [null, 'pp4'] }] }],
        damageRecords: [{ id: 'd1', date: day, unitId: 'u1', unitName: { a: 1 }, component: 5, description: ['x'] }],
        units: [{ id: 'u1', name: 'GGTR101G', status: 'Breakdown' }, { id: 'u2', name: 7, status: 5 }]
    };
    const oddIn = { date: day, now: at('2026-11-11', 7), members: odd.teamMembers, shifts: [], leaves: [], workLogs: odd.workLogs, tickets: odd.tickets, damages: odd.damageRecords, jobs: odd.otJobs, projects: odd.projects, units: odd.units };
    const O = C.buildDaily(oddIn);
    // Times and numbers written as maps with a toString key (any sort or sum of them threw).
    const bad = { toString: 'x' };
    const badIn = Object.assign({}, oddIn, {
        tickets: [{ id: 'k4', no: 'TK-4', title: 'Bad time', priority: 'low', status: 'open', reportedAt: bad, updatedAt: bad, events: [{ type: 'resolved', at: bad }, { type: 'resolved', at: 1 }] },
            { id: 'k5', no: 'TK-5', title: 'Fine', priority: 'high', status: 'open', reportedAt: at(day, 10) }],
        projects: [{ id: 'p5', name: 'Bad time', status: 'progress', updatedAt: bad, updates: [{ at: bad, date: day, text: 'Tower', progress: 'progress 0% → 5%' }],
            target: { mode: 'milestones', milestones: [{ label: 'A', weight: bad, done: true }] } }],
        jobs: [{ id: 'j3', date: day, category: 'Install', description: 'GPS', status: 'done', hours: bad, updatedAt: bad }],
        workLogs: [{ id: 'wl3', date: day, memberId: 'm1', memberName: 'Budi', start: bad, end: bad, approval: 'approved', createdAt: bad, photoCount: bad, jobs: [{ paddock: 'P-9', jobType: 'Tillage' }] }] });
    const BT = C.buildDaily(badIn);
    t('2.45.0: angka/waktu berupa map {toString} → tidak ada bagian yang gagal', [BT.failed, BT.errors, BT.tickets.opened.map(x => x.no), BT.jobs.map(j => j.what), BT.running.map(p => p.name)],
        [[], [], ['TK-5'], ['GPS'], ['Bad time']]);
    t('2.45.0: tidy: angka berupa map dibuang, null tetap', [C.tidy({ reportedAt: bad, resolvedAt: null, title: 'a' }, C.SHAPES.ticket), C.tidy({ hours: NaN }, C.SHAPES.job)], [{ resolvedAt: null, title: 'a' }, {}]);
    t('2.45.0: data aneh — tidak ada bagian yang gagal', [O.failed, O.errors], [[], []]);
    t('2.45.0: daily report: job null dilewati, unit bukan daftar = tanpa unit', [O.team.filed, O.team.members.map(m => [m.name, m.state, m.work, m.units]), O.team.paddocks, O.team.units],
        [2, [['42', 'ok', '', ''], ['Budi Santoso', 'ok', 'BLAF-2-E-005 · Tillage; 7', 'GGTR101G']], 1, 1]);
    t('2.45.0: daily report: isu angka dibaca sebagai teks, isu objek dilewati', O.team.issues, [{ who: 'Budi', text: '12' }]);
    t('2.45.0: ticket: events null / bukan daftar, units 5, label "constructor"', [O.tickets.opened.map(x => [x.no, x.title, x.prio, x.status, x.units]), O.tickets.resolved.map(x => x.no), O.tickets.open, O.tickets.overdue],
        [[['TK-1', 'Display mati', 'High', 'Open', ''], ['TK-2', '', '', 'Open → resolved later', '7']], [], 1, 1]);
    t('2.45.0: job: kategori/status aneh, ticket & unit bukan daftar', O.jobs.map(j => [j.category, j.what, j.status, j.done, j.units, j.ticket]), [['Other', '5', 'Done', false, '', ''], ['Other', '', 'Done', true, '', '']]);
    t('2.45.0: project: units bukan daftar = 0%, tanpa updates, milestone null dilewati', [O.running.map(p => [p.name, p.pct]), O.updates.map(u => [u.project, u.pct, u.s.texts])],
        [[['', 100], ['No updates', 50], ['7', 0], ['RTK Tower', 0]], [['No updates', 100, ['Pondasi']], ['', 100, ['u']]]]);
    t('2.45.0: foto: id bukan teks dilewati', O.photos.map(p => [p.key, p.index]), [['wl:wl1:0', 0], ['tk:tp9', undefined], ['pj:jp2', undefined], ['pj:pp4', undefined], ['wl:wl1:0:1', 1]]);
    t('2.45.0: kerusakan: nama/komponen aneh tidak membuat gagal', O.damage.map(d => [d.unit, d.part, d.what, d.breakdown]), [['GGTR101G', '5', '', true]]);
    const OP = C.dailyPages(O, {}), OM = C.dailyEmail(O, { attached: true });
    t('2.45.0: halaman & email dari data aneh', [OP.length, /Display mati/.test(OM.html), /Left out/.test(OM.html)], [5, true, false]);
    const op = await renderPptx(OP), od = await renderPdf(OP);
    t('2.45.0: PowerPoint & PDF dari data aneh', [op.length > 20000, od.length > 5000], [true, true]);
    // A part that still fails (here: a record that throws when read) is left out and named; the rest stays.
    const boom = { id: 'mx', get name() { throw new Error('boom'); } };
    const B = C.buildDaily(Object.assign({}, oddIn, { members: [boom] }));
    t('2.45.0: bagian yang tetap gagal ditinggal & disebut, bagian lain tetap ada', [B.failed, /boom/.test(B.errors[0]), B.team.members, B.tickets.opened.length, B.jobs.length], [['Daily Report summary'], true, [], 2, 2]);
    const BM = C.dailyEmail(B, {}), BP = C.dailyPages(B, {});
    t('2.45.0: email & halaman menyebut bagian yang ditinggal', [/Left out of this report \(could not be built\): Daily Report summary/.test(BM.html), /Left out of this report \(could not be built\): Daily Report summary/.test(BM.text), /left out \(could not be built\): Daily Report summary/.test(BP[1].sub)], [true, true, true]);
    t('2.45.0: helper bersama: tidy membersihkan daftar, objek yang benar tetap sama', [C.tidy({ events: [null, { a: 1 }], units: 3, photoIds: ['a', 2] }, C.SHAPES.ticket), (() => { const r = { events: [{ a: 1 }], units: [] }; return C.tidy(r, C.SHAPES.ticket) === r; })(),
        C.tidy({ target: 'x', updates: [{ photoIds: 'y' }] }, C.SHAPES.project), C.tidy(null, C.SHAPES.job)],
        [{ events: [{ a: 1 }], units: [], photoIds: ['a'] }, true, { target: {}, updates: [{ photoIds: [] }] }, null]);
    t('2.45.0: tidy: teks dibaca sebagai teks (angka → teks, objek/null → kosong), yang tidak ada tetap tidak ada',
        C.tidy({ title: 5, no: { a: 1 }, description: null, units: [{ name: 7, sn: 'S1' }] }, C.SHAPES.ticket), { title: '5', no: '', description: '', units: [{ name: '7', sn: 'S1' }] });
    t('2.45.0: summarizeUpdates / jobInfo / projectNamer dengan data aneh', [C.summarizeUpdates({ updates: 'x' }, day, day), C.jobInfo({ units: 5, category: 3, status: '__proto__' }, { tickets: [null, { id: 't', jobIds: 'abc' }], projects: 'x' }).status, C.projectNamer([null, { name: 5 }])({ name: 5 })],
        [null, 'Done', '5']);

    const pptx = await renderPptx(P), pdf = await renderPdf(P);
    t('PowerPoint: 5 slide', new Set(pptx.toString('latin1').match(/ppt\/slides\/slide\d+\.xml/g) || []).size, 5);
    t('PDF: 5 halaman', (pdf.toString('latin1').match(/\/Type\s*\/Page[^s]/g) || []).length, 5);

    const r = await composeDaily({ db, bucket, sharp, date: day, now: at('2026-11-11', 7) });
    t('server: catatan dibaca per tanggal', asked.sort(), ['damageRecords:date', 'damageRecords:resolvedAt', 'leaveRequests', 'otJobs:date', 'projects', 'shifts:date', 'teamMembers', 'tickets', 'units', 'workLogs:date'].sort());
    t('server: foto dari Storage & foto lama inline; yang hilang/tak terbaca dilewati', [r.photos, r.pages], [2, 6]);
    t('server: lampiran PowerPoint + PDF', r.m.attachments.map(a => [a.filename, a.content.length > 20000]), [['Daily_Report_2026-11-10.pptx', true], ['Daily_Report_2026-11-10.pdf', true]]);
    // 2.45.0: the same records the server reads, written oddly — the email still goes, whole, with both files.
    Object.assign(data, odd);
    const r2 = await composeDaily({ db, bucket, sharp, date: day, now: at('2026-11-11', 7) });
    t('2.45.0: server: data aneh — email lengkap dengan lampiran', [r2.D.failed, r2.fileError, r2.m.attachments.length, /Display mati/.test(r2.m.html), /Pondasi/.test(r2.m.html)], [[], '', 2, true, true]);

    let pass = 0;
    T.forEach(x => { if (x.pass) pass++; console.log(`${x.pass ? 'PASS' : 'FAIL'}  ${x.name}${x.pass ? '' : `\n        dapat=${JSON.stringify(x.g)} harap=${JSON.stringify(x.w)}`}`); });
    console.log(`\n${pass}/${T.length} lulus`);
    process.exit(pass === T.length ? 0 : 1);
})().catch(e => { console.error(e); process.exit(1); });
