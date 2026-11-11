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
    t('jobs & project', [D.jobs.map(j => [j.category, j.ticket]), D.updates.map(u => [u.project, u.pct, u.blocker])], [[['Calibration', 'TK-1']], [['RTK Tower', 25, 'Semen kurang']]]);
    t('foto: per paddock, lalu ticket & job', D.photos.map(p => [p.key, p.index]), [['wl:wl1:0', 0], ['wl:wl1:1', 1], ['tk:tp1', undefined], ['pj:jp1', undefined]]);
    const P = C.dailyPages(D, {});
    t('halaman tanpa foto', P.map(p => p.title), ['Daily Report', 'Daily Report Summary', 'Issue & Damage', 'OT Jobs & Project Progress', 'Terima Kasih']);
    t('template perusahaan', P.every(p => p.theme === 'msg'), true);
    const jl = P[3].cols[0][0].items[0], ul = P[3].cols[1][0].items[0];
    t('job: teks lengkap, status sebagai label, keterangan informatif (2.39.2)', [jl.text, jl.tag, jl.sub, jl.note], ['Kalibrasi steering', { text: 'Done', tone: 'good' }, 'Calibration · Ticket TK-1', ''] );
    t('update project: nama project, progress sekarang, blocker', [ul.head, ul.text, ul.sub, ul.note], ['RTK Tower', 'Pondasi selesai', 'progress 0% → 25% · progress now 25%', 'Blocker: Semen kurang']);
    const m = C.dailyEmail(D, { attached: true, photos: 2 });
    t('email: subjek & isi', [m.subject, /Missed: Andi Wijaya/.test(m.html), /Selang hidrolik/.test(m.html), /RTK Tower/.test(m.text) || /Project updates: 1/.test(m.text)], ['Daily Report — Tue 10 Nov 2026', true, true, true]);
    t('email: HTML di-escape', C.dailyEmail(C.buildDaily({ date: day, workLogs: [{ id: 'x', date: day, memberId: 'q', memberName: '<b>', issue: '<script>' }] })).html.includes('&lt;script&gt;'), true);
    t('laporan Office bertingkat: satu baris rapi, tidak terpotong (2.39.3)', C.buildDaily({ date: day, members: [{ id: 'a', name: 'Yuda' }], workLogs: [{ id: 'w', date: day, memberId: 'a', place: 'office', start: '07:00', end: '13:00',
        task: 'Lokasi: Tower KM 8\nPekerjaan dan Unit :\n- Setting radio repeater dan antena\n- Cek kabel coax' }] }).team.members[0].work,
        'Office · Lokasi: Tower KM 8 · Pekerjaan dan Unit: Setting radio repeater dan antena · Cek kabel coax');
    t('teks dipotong ke kotaknya', fitLine('Pasang Camera AI di seluruh unit traktor GPA', 1, 10), 'Pasang Camer…');

    const pptx = await renderPptx(P), pdf = await renderPdf(P);
    t('PowerPoint: 5 slide', new Set(pptx.toString('latin1').match(/ppt\/slides\/slide\d+\.xml/g) || []).size, 5);
    t('PDF: 5 halaman', (pdf.toString('latin1').match(/\/Type\s*\/Page[^s]/g) || []).length, 5);

    const r = await composeDaily({ db, bucket, sharp, date: day, now: at('2026-11-11', 7) });
    t('server: catatan dibaca per tanggal', asked.sort(), ['damageRecords:date', 'damageRecords:resolvedAt', 'leaveRequests', 'otJobs:date', 'projects', 'shifts:date', 'teamMembers', 'tickets', 'units', 'workLogs:date'].sort());
    t('server: foto dari Storage & foto lama inline; yang hilang/tak terbaca dilewati', [r.photos, r.pages], [2, 6]);
    t('server: lampiran PowerPoint + PDF', r.m.attachments.map(a => [a.filename, a.content.length > 20000]), [['Daily_Report_2026-11-10.pptx', true], ['Daily_Report_2026-11-10.pdf', true]]);

    let pass = 0;
    T.forEach(x => { if (x.pass) pass++; console.log(`${x.pass ? 'PASS' : 'FAIL'}  ${x.name}${x.pass ? '' : `\n        dapat=${JSON.stringify(x.g)} harap=${JSON.stringify(x.w)}`}`); });
    console.log(`\n${pass}/${T.length} lulus`);
    process.exit(pass === T.length ? 0 : 1);
})().catch(e => { console.error(e); process.exit(1); });
