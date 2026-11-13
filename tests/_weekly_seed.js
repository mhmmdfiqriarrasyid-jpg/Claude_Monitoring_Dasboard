// Sample data for tests/weekly_report_test.js (injected as an init script).
// Sample data for the weekly report (week Sat 31 Oct – Fri 6 Nov 2026).
window.__seed = () => {
  const ok = () => Promise.resolve();
  const F = n => `tests/fixtures/wr/p${n}.jpg`;
  window.cloud = { isReady: true, addHistoryEvents: ok,
    // 2.32.0 pictures: every report has its paddock photos; ids name the file.
    getWorkLogPhotos: id => Promise.resolve([F((Number(id.slice(1)) % 8) + 1), F(((Number(id.slice(1)) + 3) % 8) + 1)]),
    getTicketPhoto: id => Promise.resolve(F(Number(id.split('_').pop()))),
    getProjectPhoto: id => Promise.resolve(F(Number(id.split('_').pop()))),
    projectFileUrl: path => Promise.resolve('tests/fixtures/wr/v1.mp4') };
  const GPA = 'PT. Global Papua Abadi', MNM = 'PT. Murni Nusantara Mandiri';
  const comp = (bad) => ({ display: bad === 'display' ? 'Breakdown' : 'Good', gps: bad === 'gps' ? 'Breakdown' : 'Good', steering: bad === 'steering' ? 'Breakdown' : 'Good', jdlink: 'Good' });
  globalData = [];
  for (let i = 1; i <= 26; i++) globalData.push({ id: 't' + i, name: `GGTR1${String(i).padStart(2, '0')}G`, sn: 'S' + i, site: i <= 14 ? GPA : MNM, unitGroup: 'tractor', status: i === 3 ? 'Breakdown' : 'Good',
    ...comp(i === 3 ? 'steering' : i === 7 ? 'display' : i === 18 ? 'gps' : ''), gpsLicense: 'SF-RTK', gpsLicenseEndDate: i < 4 ? '2026-11-20' : i === 5 ? '2026-10-01' : '2027-05-01', licenseDisplay: 'G5 Advance', displayLicenseEndDate: '2027-03-01' });
  for (let i = 1; i <= 8; i++) globalData.push({ id: 'h' + i, name: `HEX0${i}`, sn: 'H' + i, site: MNM, unitGroup: 'heavy', status: 'Good', cameraAi: i === 2 ? 'Breakdown' : 'Good', telematicBox: 'Good', switchLimiter: 'Good', rotaryLamp: 'Good' });
  globalDamages = [
    { id: 'd1', date: '2026-10-15', unitId: 't3', unitName: 'GGTR103G', site: GPA, damageType: 'Device Precision', component: 'Steering Sensor', description: 'Steering sensor error, waiting for spare from vendor', resolved: false },
    { id: 'd2', date: '2026-11-02', unitId: 't7', unitName: 'GGTR107G', site: GPA, damageType: 'Device Precision', component: 'Display', description: 'Display black screen', resolved: false },
    { id: 'd3', date: '2026-11-03', unitId: 't18', unitName: 'GGTR118G', site: MNM, damageType: 'Device Precision', component: 'GPS', description: 'No RTK fix', resolved: false },
    { id: 'd4', date: '2026-10-28', unitId: 't9', unitName: 'GGTR109G', site: GPA, damageType: 'Mekanis', component: 'Hydraulic', description: 'Hose leak', resolved: true, resolvedAt: '2026-11-02' },
    { id: 'd5', date: '2026-11-01', unitId: 'h2', unitName: 'HEX02', site: MNM, damageType: 'Device Precision', component: 'Camera AI', description: 'Camera AI offline', resolved: false, unitGroup: 'heavy' }];
  teamMembers = [['m1', 'Ibrahim H.Z', MNM], ['m2', 'Andreas Kayembob', MNM], ['m3', 'Febri Manuel', GPA], ['m4', 'Paulus Baru', GPA], ['m5', 'Yohanis P.', GPA], ['m6', 'Agus Kalem', MNM]].map(([id, name, company]) => ({ id, name, company, active: true }));
  const days = ['2026-10-31', '2026-11-01', '2026-11-02', '2026-11-03', '2026-11-04', '2026-11-05', '2026-11-06'];
  const pads = ['BLAF-2-E-005', 'BLAF-1-A-031', 'JAGF-1-A-001', 'BLAF-2-E-001', 'JAGF-2-C-014'], types = ['Application', 'Tillage', 'Harvesting', 'Planting'];
  workLogs = []; teamShifts = []; leaveRequests = [{ id: 'l1', memberId: 'm4', type: 'izin', dateFrom: '2026-11-04', dateTo: '2026-11-05', days: 2, approval: 'approved' }];
  let n = 0;
  teamMembers.forEach((m, mi) => days.forEach((d, di) => {
    teamShifts.push({ id: d + '_' + m.id, date: d, memberId: m.id, shift: di === 1 && mi % 2 ? 'libur' : 'pagi' });
    const miss = (m.id === 'm3' && di === 3) || (m.id === 'm5' && di === 5) || (m.id === 'm6' && di === 6) || (m.id === 'm4' && (di === 4 || di === 5)) || (di === 1 && mi % 2);
    if (miss) return;
    n++;
    workLogs.push({ id: 'w' + n, date: d, memberId: m.id, memberName: m.name, company: m.company, start: '07:00', end: di === 1 ? '11:00' : '16:00', approval: n % 5 ? 'approved' : 'pending', place: 'field',
      jobs: [{ paddock: pads[(mi + di) % 5], jobType: types[(mi + di) % 4], units: [{ id: 't' + (mi + 1), name: `GGTR1${String(mi + 1).padStart(2, '0')}G` }], desc: 'x', photoCount: 2 }], photoCount: 2,
      issue: (m.id === 'm1' && di === 2) ? 'Hydraulic hose on GGTR101G leaking — workshop informed' : '' });
  }));
  inspections = [{ id: 'i1', unitId: 'h1', date: '2026-11-03', approval: 'approved', submittedAt: 1 }, { id: 'i2', unitId: 'h3', date: '2026-10-10', approval: 'approved', submittedAt: 1 }];
  globalLicenseStock = [{ txnType: 'IN', licenseType: 'SF-RTK', qty: 4 }, { txnType: 'OUT', licenseType: 'SF-RTK', qty: 3 }, { txnType: 'IN', licenseType: 'G5 Advance', qty: 2 }, { txnType: 'OUT', licenseType: 'G5 Advance', qty: 2 }];
  const at = (d, h) => new Date(d + 'T' + String(h).padStart(2, '0') + ':00:00').getTime();
  purchaseRequests = [{ id: 'p1', status: 'approved', statusHistory: [{ status: 'submitted', at: at('2026-11-02', 9) }, { status: 'approved', at: at('2026-11-04', 9) }] }];
  tickets = [
    { id: 'k1', no: 'TK-261102-0800AB', title: 'Lime spreader reads 15 t when loaded 4 t', category: 'Implement', priority: 'high', status: 'progress', units: [{ id: 't12', name: 'GGTR112G' }], assigneeName: 'Kautsar', reportedAt: at('2026-11-02', 8), events: [], jobIds: [], photoIds: ['k1_p_3', 'k1_p_4'] },
    { id: 'k2', no: 'TK-261103-1000CD', title: 'GPS loses RTK at JAGF-1 north end', category: 'GPS / RTK signal', priority: 'medium', status: 'resolved', resolvedAt: at('2026-11-04', 10), reportedAt: at('2026-11-03', 10), events: [], jobIds: [] },
    { id: 'k3', no: 'TK-261105-0700EF', title: 'Radio channel 3 noisy', category: 'Radio / HT', priority: 'low', status: 'open', reportedAt: at('2026-11-05', 7), events: [], jobIds: [] },
    { id: 'k4', no: 'TK-261104-1300GH', title: 'Display freezes after update', category: 'Display', priority: 'medium', status: 'assigned', assigneeName: 'Rasyid', units: [{ id: 't7', name: 'GGTR107G' }], reportedAt: at('2026-11-04', 13), events: [], jobIds: [], photoIds: ['k4_p_5'] }];
  otJobs = [
    { id: 'j1', date: '2026-11-02', category: 'Calibration', description: 'Calibration lime spreader LS006', status: 'progress', units: [{ id: 't12', name: 'GGTR112G' }], ticketId: 'k1', issue: 'Reading still wrong after calibration', requestedBy: 'Land Development', photoIds: ['j1_p_6'], files: [{ name: 'Calibration test.mp4', path: 'projectFiles/j1/v.mp4', type: 'video/mp4', size: 83994 }] },
    { id: 'j2', date: '2026-11-03', category: 'Implement install', description: 'Install scooping box automation', status: 'progress', units: [{ id: 't20', name: 'GGTR120G' }], issue: 'Electrical problem — waiting workshop electrical team' },
    { id: 'j3', date: '2026-11-04', category: 'Calibration', description: 'Steering calibration after wheel change', status: 'done', units: [{ id: 't5', name: 'GGTR105G' }], hours: 3 },
    { id: 'j4', date: '2026-11-05', category: 'Meeting', description: 'Weekly coordination with Agronomy', status: 'done', hours: 1.5 },
    { id: 'j5', date: '2026-11-04', category: 'Troubleshooting', description: 'RTK base station reset', status: 'done', ticketId: 'k2', hours: 2 }];
  projects = [
    { id: 'pj1', name: 'Depth Sensor - Cartrack (Phase 2)', type: 'precision_heavy', status: 'progress', company: MNM, pic: 'Rasyid', start: '2026-10-01', end: '2026-11-30', target: { mode: 'count', count: 40, done: 22, what: 'sensors' }, updates: [{ date: '2026-11-04', text: '6 more sensors installed at MNM', progress: 'x', photoIds: ['u1_p_7', 'u1_p_8'] }] },
    { id: 'pj2', name: 'RTK radio repeater stage 2', type: 'infra', status: 'progress', company: GPA, pic: 'Kautsar', start: '2026-09-15', end: '2026-10-31', target: { mode: 'milestones', milestones: [{ label: 'Survey', weight: 20, done: true }, { label: 'PO', weight: 30, done: true }, { label: 'Install', weight: 50, done: false }] }, updates: [{ date: '2026-11-03', text: 'Goods still at Merauke port', progress: '' }] }];
  currentUser = { uid: 'o', email: 'o@x.id' }; currentUserDoc = { uid: 'o', role: 'owner', status: 'active', displayName: 'Rasyid' };
  hideAuthGates(); applyRoleGating();
};
