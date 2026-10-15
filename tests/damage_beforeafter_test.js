// v151: foto kerusakan menjadi Before / After.
// Foto yang sudah ada = Before (dokumen damagePhotos/{id}); foto sesudah
// perbaikan = After (damagePhotos/{id}__after, ditandai hasAfterPhoto di
// catatannya). Diuji: tombol di tabel, penampil (label + berdampingan), dialog
// "Mark as repaired" dengan foto After, form Edit, hapus, cadangan/restore,
// CSV, foto yang ditolak server, dan tampilan di HP.
const { launch, BASE_URL } = require('./_env');

(async () => {
    const b = await launch();
    const page = await b.newPage({ viewport: { width: 1440, height: 900 } });
    const errors = [];
    page.on('pageerror', e => { if (!/Chart is not defined/.test(e.message)) errors.push(e.message); });
    await page.goto(BASE_URL + '/index.html', { waitUntil: 'load' });
    await page.waitForTimeout(1000);

    await page.addScriptTag({ content: `(async () => { try {
        const T = []; const t = (n, g, w) => T.push({ n, g, w, pass: JSON.stringify(g) === JSON.stringify(w) });
        const tick = ms => new Promise(r => setTimeout(r, ms || 20));
        const toasts = [];
        window.showToast = (m) => { toasts.push(String(m)); };
        let confirmAnswer = true, confirmText = '';
        window.confirm = m => { confirmText = String(m); return confirmAnswer; };
        URL.createObjectURL = blob => { window.__blob = blob; return 'blob:test'; };
        URL.revokeObjectURL = () => {};
        HTMLAnchorElement.prototype.click = function () {};

        const PH = s => 'data:image/jpeg;base64,' + s;
        const store = { before: { dA: PH('BA'), dB: PH('BB') }, after: { dB: PH('AB') } };
        const calls = [];          // urutan semua tulisan ke cloud
        const fetched = [];
        let failAfterFetch = false, failAfterSave = null;
        const ok = () => Promise.resolve();
        window.cloud = { isReady: true,
            saveDamage: r => { calls.push(['rec', r.id, !!r.resolved, !!r.hasAfterPhoto]); return ok(); },
            saveDamages: ok, deleteDamage: id => { calls.push(['delRec', id]); return ok(); },
            saveUnits: ok, updateUnitsFields: ok, deleteUnits: ok, getAllUnits: () => Promise.resolve([]),
            addHistoryEvents: ok, subscribeUsers: () => () => {},
            saveImplements: ok, saveLicenses: ok, saveUserCategories: ok, saveDamageComponents: ok,
            saveTeamMembers: ok, saveShifts: ok, saveLeaveRequests: ok, saveWorkLogs: ok, saveDevices: ok, saveStockItems: ok,
            getAllShifts: () => Promise.resolve([]),
            getDamagePhoto: id => { fetched.push('B:' + id); return Promise.resolve(store.before[id] || ''); },
            saveDamagePhoto: (id, p) => { calls.push(['before', id]); store.before[id] = p; return ok(); },
            deleteDamagePhoto: id => { calls.push(['delBefore', id]); delete store.before[id]; return ok(); },
            getDamageAfterPhoto: id => {
                fetched.push('A:' + id);
                if (failAfterFetch) return Promise.reject(Object.assign(new Error('offline'), { code: 'unavailable' }));
                return Promise.resolve(store.after[id] || '');
            },
            saveDamageAfterPhoto: (id, p) => {
                calls.push(['after', id]);
                if (failAfterSave) { const e = failAfterSave; failAfterSave = null; return Promise.reject(e); }
                store.after[id] = p; return ok();
            },
            deleteDamageAfterPhoto: id => { calls.push(['delAfter', id]); delete store.after[id]; return ok(); } };

        const asOwner = () => { currentUser = { uid: 'uO', email: 'o@x.id' }; currentUserDoc = { role: 'owner', status: 'active', email: 'o@x.id' }; applyRoleGating(); };
        asOwner(); hideAuthGates();
        localStorage.setItem('tractorDamagePhotosSplit', '1');

        const mk = (id, x) => Object.assign({ id, date: '2026-10-12', unitId: 'u1', unitName: 'BD100T', sn: 'LGCB1', site: 'PT. GPA',
            damageType: 'Mekanis', component: 'Camera AI', description: 'Kronologi ' + id, resolved: false, resolvedAt: '',
            createdAt: 1, updatedAt: 1 }, x);
        const seed = () => {
            globalData = [{ id: 'u1', name: 'BD100T', sn: 'LGCB1', site: 'PT. GPA', status: 'Breakdown', breakdownReason: 'x' },
                          { id: 'u2', name: 'BD021G', sn: 'LGCB2', site: 'PT. GPA', status: 'Good' }];
            saveToStorage(globalData);
            return [
                mk('dA', { hasPhoto: true, drove: 'status' }),                                        // open, Before only
                mk('dB', { hasPhoto: true, hasAfterPhoto: true, resolved: true, resolvedAt: '2026-10-14' }), // both
                mk('dC', { unitId: 'u2', unitName: 'BD021G', sn: 'LGCB2', resolved: true, resolvedAt: '2026-10-13' }), // repaired, no photo
                mk('dD', { unitId: 'u2', unitName: 'BD021G', sn: 'LGCB2' })                          // open, no photo
            ];
        };
        let recs = seed();
        navigateTo('damage');
        globalDamages = recs.map(d => ({ ...d }));
        saveDamages();
        renderDamageTable();
        const cell = id => document.querySelector('#damageBody .damage-check[data-id="' + id + '"]').closest('tr').querySelector('td[data-label="Photos"]');
        const btnText = id => [...cell(id).querySelectorAll('button')].map(x => x.textContent.trim());

        // =============== 1. TABEL ===============
        t('kolomnya bernama Photos', [...document.querySelectorAll('#damageTable thead th')].map(th => th.textContent.trim()).includes('Photos'), true);
        t('foto lama tampil sebagai Before', btnText('dA'), ['Before']);
        t('Before + After berdampingan', btnText('dB'), ['Before', 'After']);
        t('sudah diperbaiki tanpa foto After: tombol tambah After', [btnText('dC'), cell('dC').querySelector('.dmg-ph--add') !== null], [['+ After'], true]);
        t('masih terbuka tanpa foto: strip', [btnText('dD'), cell('dD').textContent.trim()], [[], '—']);
        t('tabel tidak memuat gambar apa pun', [document.querySelectorAll('#damageTable img').length, fetched.length], [0, 0]);
        currentUserDoc = { role: 'staff', status: 'active', email: 'v@x.id', access: { damage: 'view' } }; applyRoleGating();
        renderDamageTable();
        t('hanya-lihat: tidak ada tombol tambah After', cell('dC').querySelector('.dmg-ph--add'), null);
        asOwner(); renderDamageTable();

        // =============== 2. PENAMPIL ===============
        const lb = () => document.getElementById('photoLightbox');
        const vis = id => document.getElementById(id).style.display !== 'none';
        await openDamagePhotos('dB', 'after', null);
        t('klik After: dua sisi diambil, masing-masing dari dokumennya', fetched.slice().sort(), ['A:dB', 'B:dB']);
        t('yang tampil foto After, berlabel', [lb().classList.contains('open'), document.getElementById('photoLightboxImg').src,
            document.getElementById('photoLightboxCaption').textContent, document.getElementById('photoLightboxCount').textContent],
          [true, PH('AB'), 'After · repaired 2026-10-14', '2 / 2']);
        t('tombol berdampingan tersedia', [vis('photoLightboxMode'), /Side by side/.test(document.getElementById('photoLightboxMode').textContent)], [true, true]);
        stepPhotoLightbox(1);
        t('panah: pindah ke Before', [document.getElementById('photoLightboxImg').src, document.getElementById('photoLightboxCaption').textContent],
          [PH('BB'), 'Before · 2026-10-12']);
        togglePhotoLightboxCompare();
        const figs = [...document.querySelectorAll('#photoLightboxCompare figure')];
        t('berdampingan: dua foto dengan labelnya', figs.map(f => [f.querySelector('img').src, f.querySelector('figcaption').textContent]),
          [[PH('BB'), 'Before · 2026-10-12'], [PH('AB'), 'After · repaired 2026-10-14']]);
        t('berdampingan: gambar tunggal, panah dan label atas disembunyikan',
          [vis('photoLightboxImg'), vis('photoLightboxPrev'), vis('photoLightboxCaption'), lb().classList.contains('photo-lightbox--compare')], [false, false, false, true]);
        stepPhotoLightbox(1);
        t('panah tidak mengacaukan tampilan berdampingan', document.querySelectorAll('#photoLightboxCompare figure').length, 2);
        figs[1].click();
        t('ketuk satu foto: tampil sendiri', [lb().classList.contains('open'), vis('photoLightboxImg'), document.getElementById('photoLightboxImg').src],
          [true, true, PH('AB')]);
        closePhotoLightbox();
        t('ditutup: sisa tampilan berdampingan dibersihkan', [lb().classList.contains('open'), document.querySelectorAll('#photoLightboxCompare figure').length], [false, 0]);

        fetched.length = 0;
        await openDamagePhotos('dB', 'before', null);
        t('klik kedua dilayani cache', fetched, []);
        t('klik Before: mulai dari Before', document.getElementById('photoLightboxImg').src, PH('BB'));
        closePhotoLightbox();

        await openDamagePhotos('dA', 'before', null);
        t('hanya Before: tanpa tombol berdampingan, tanpa panah', [vis('photoLightboxMode'), vis('photoLightboxPrev'), document.getElementById('photoLightboxCaption').textContent],
          [false, false, 'Before · 2026-10-12']);
        closePhotoLightbox();

        // After gagal dimuat: klik Before tetap jalan; klik After mengatakan gagal.
        _dmgAfterCache.clear(); failAfterFetch = true;
        await openDamagePhotos('dB', 'before', null);
        t('After gagal dimuat, Before tetap tampil sendiri', [lb().classList.contains('open'), document.getElementById('photoLightboxImg').src, vis('photoLightboxMode')],
          [true, PH('BB'), false]);
        closePhotoLightbox();
        toasts.length = 0;
        const btn = document.createElement('button'); btn.innerHTML = 'X';
        await openDamagePhotos('dB', 'after', btn);
        t('klik After yang gagal: tidak terbuka, dikatakan, tombol dipulihkan',
          [lb().classList.contains('open'), /Failed to load the damage photo|needs a signal/.test(toasts[toasts.length - 1] || ''), btn.disabled, btn.innerHTML],
          [false, true, false, 'X']);
        failAfterFetch = false;
        _dmgAfterCache.delete('dB');
        // gagal tidak diingat: klik berikutnya mengambil lagi
        fetched.length = 0;
        await openDamagePhotos('dB', 'after', null);
        t('kegagalan tidak di-cache: klik berikutnya berhasil', [fetched.includes('A:dB'), document.getElementById('photoLightboxImg').src], [true, PH('AB')]);
        closePhotoLightbox();

        // Label dari data yang diketik orang: tidak pernah jadi markup.
        openPhotoLightbox([PH('X'), PH('Y')], 0, { captions: ['<img src=x onerror=window.__pwn=1>', 'b'], compare: true });
        togglePhotoLightboxCompare();
        t('label bukan markup', [document.querySelectorAll('#photoLightboxCompare img').length, window.__pwn === undefined], [2, true]);
        closePhotoLightbox();
        openPhotoLightbox(['', PH('Y')], 0, { captions: ['hilang', 'kedua'] });
        t('foto kosong dibuang, labelnya tetap berpasangan', document.getElementById('photoLightboxCaption').textContent, 'kedua');
        closePhotoLightbox();

        // =============== 3. MARK AS REPAIRED + FOTO AFTER ===============
        const isOpen = id => document.getElementById(id).classList.contains('open');
        const makeFile = () => new Promise(res => {
            const c = document.createElement('canvas'); c.width = 40; c.height = 30;
            const g = c.getContext('2d'); g.fillStyle = '#4B7453'; g.fillRect(0, 0, 40, 30);
            c.toBlob(bl => res(new File([bl], 'after.png', { type: 'image/png' })), 'image/png');
        });
        const pick = async (fn, side) => { const f = await makeFile(); await fn({ target: { files: [f], value: 'x' } }, side); };

        openDamageRepair('dA');
        t('Mark done membuka dialog, bukan confirm()', [isOpen('damageRepairModal'), document.getElementById('damageRepairTitle').textContent.trim()],
          [true, 'Mark as repaired']);
        t('ringkasannya: unit, tanggal, komponen, dan tombol Before',
          [/BD100T/.test(document.getElementById('damageRepairSummary').textContent), /2026-10-12/.test(document.getElementById('damageRepairSummary').textContent),
           /Camera AI/.test(document.getElementById('damageRepairSummary').textContent),
           [...document.querySelectorAll('#damageRepairSummary button')].map(x => x.textContent.trim())],
          [true, true, true, ['Before']]);
        t('foto After opsional di sini', vis('dmgRepairOptional'), true);
        await pick(handleRepairPhotoChange);
        t('foto dipilih: pratinjau tampil', [vis('dmgRepairPreviewWrap'), /^data:image\\/jpeg;base64,/.test(document.getElementById('dmgRepairPreview').src)], [true, true]);
        calls.length = 0; toasts.length = 0;
        saveDamageRepair();
        await tick(50);
        const recA = globalDamages.find(d => d.id === 'dA');
        t('catatan ditandai selesai, dengan hasAfterPhoto', [recA.resolved, recA.hasAfterPhoto, recA.resolvedAt === toISODate()], [true, true, true]);
        t('foto ditulis LEBIH DULU, lalu catatannya — satu tulisan catatan', calls.filter(c => c[1] === 'dA'), [['after', 'dA'], ['rec', 'dA', true, true]]);
        t('status unit dipulihkan seperti sebelumnya', globalData.find(u => u.id === 'u1').status, 'Good');
        t('dialog tertutup, toast menyebut fotonya', [isOpen('damageRepairModal'), toasts[toasts.length - 1]],
          [false, 'Damage marked as done with the After photo — unit status restored']);
        t('tabel: kini Before + After', btnText('dA'), ['Before', 'After']);
        fetched.length = 0;
        await openDamagePhotos('dA', 'after', null);
        t('foto yang baru diambil tampil tanpa mengunduh', [fetched.filter(f => f === 'A:dA').length, document.getElementById('photoLightboxCaption').textContent],
          [0, 'After · repaired ' + toISODate()]);
        closePhotoLightbox();

        // tanpa foto
        calls.length = 0;
        openDamageRepair('dD');
        saveDamageRepair();
        const recD = globalDamages.find(d => d.id === 'dD');
        t('tanpa foto: tetap bisa ditandai selesai', [recD.resolved, !!recD.hasAfterPhoto, calls.filter(c => c[0] === 'after').length], [true, false, 0]);
        renderDamageTable();
        t('lalu muncul tombol tambah After', btnText('dD'), ['+ After']);

        // sudah selesai: mode tambah foto saja
        openDamageRepair('dC');
        t('catatan yang sudah selesai: judul "Add the After photo", tombol "Save photo"',
          [document.getElementById('damageRepairTitle').textContent.trim(), document.getElementById('damageRepairSave').textContent.trim(), vis('dmgRepairOptional')],
          ['Add the After photo', 'Save photo', false]);
        toasts.length = 0;
        saveDamageRepair();
        t('tanpa foto tidak disimpan', [isOpen('damageRepairModal'), toasts[toasts.length - 1]], [true, 'Choose a photo first']);
        await pick(handleRepairPhotoChange);
        confirmAnswer = false;
        closeDamageRepair();
        t('menutup dengan foto belum disimpan: ditanya, batal = tetap terbuka', [isOpen('damageRepairModal'), /After photo is not saved yet/.test(confirmText)], [true, true]);
        confirmAnswer = true;
        calls.length = 0;
        const logged = [];
        const realLog = logEvent; window.logEvent = e => { logged.push(e); };
        saveDamageRepair();
        window.logEvent = realLog;
        await tick(30);
        const recC = globalDamages.find(d => d.id === 'dC');
        t('foto After ditambahkan ke catatan yang sudah selesai', [recC.hasAfterPhoto, recC.resolvedAt, calls.map(c => c[0])], [true, '2026-10-13', ['after', 'rec']]);
        t('tercatat di History', logged.map(e => [e.field, e.after]), [['After photo', 'added']]);
        openDamageRepair('dB');
        t('sudah punya After: judulnya Replace', document.getElementById('damageRepairTitle').textContent.trim(), 'Replace the After photo');
        closeDamageRepair();

        // foto After ditolak server: tidak hilang
        openDamageRepair('dC');
        await pick(handleRepairPhotoChange);
        failAfterSave = Object.assign(new Error('Missing or insufficient permissions.'), { code: 'permission-denied' });
        saveDamageRepair();
        await tick(50);
        t('ditolak: muncul pemberitahuan belum terkirim', !!document.getElementById('unsentNotice'), true);
        const key = [..._unsent.keys()].pop();
        reopenUnsent(key);
        t('dibuka lagi: dialog foto dengan fotonya', [isOpen('damageRepairModal'), vis('dmgRepairPreviewWrap'), document.getElementById('damageRepairTitle').textContent.trim()],
          [true, true, 'Replace the After photo']);
        calls.length = 0;
        saveDamageRepair();
        await tick(30);
        t('dikirim ulang', calls.map(c => c[0]), ['after', 'rec']);

        // =============== 4. FORM EDIT ===============
        showAddDamageForm();
        t('form Tambah: slot After disembunyikan', vis('dmgAfterGroup'), false);
        closeDamageModal(true);
        _dmgAfterCache.clear(); fetched.length = 0;
        editDamage('dB');
        t('form Edit: slot After tampil, sedang dimuat', [vis('dmgAfterGroup'), vis('dmgAfterLoading')], [true, true]);
        await tick(60);
        t('foto After dimuat ke pratinjaunya', [vis('dmgAfterPreviewWrap'), document.getElementById('dmgAfterPreview').src, vis('dmgPhotoPreviewWrap')],
          [true, PH('AB'), true]);
        // ubah teks saja: tidak ada tulisan foto
        calls.length = 0;
        document.getElementById('dmgDescription').value = 'Teks dibetulkan';
        saveDamage({ preventDefault() {} });
        await tick(30);
        t('ubah teks saja: foto tidak ditulis, hasAfterPhoto tetap', [calls.filter(c => c[0] !== 'rec').length, globalDamages.find(d => d.id === 'dB').hasAfterPhoto], [0, true]);
        editDamage('dB'); await tick(40);
        removeDamagePhoto('after');
        confirmAnswer = false; confirmText = '';
        closeDamageModal();
        t('hapus foto After lalu tutup: ditanya dulu', [isOpen('damageModal'), /photo change is not saved yet/.test(confirmText)], [true, true]);
        confirmAnswer = true;
        calls.length = 0;
        saveDamage({ preventDefault() {} });
        await tick(30);
        t('hapus foto After lewat Edit: dokumennya dihapus, tanda dilepas', [calls.map(c => c[0]), globalDamages.find(d => d.id === 'dB').hasAfterPhoto],
          [['rec', 'delAfter'], false]);
        editDamage('dB'); await tick(40);
        await pick(handleDamagePhotoChange, 'after');
        t('pilih foto After di form', [vis('dmgAfterPreviewWrap'), _dmgAfterDirty], [true, true]);
        calls.length = 0;
        saveDamage({ preventDefault() {} });
        await tick(30);
        // Foto dulu, baru catatannya: HP lain tidak melihat catatan berubah
        // (lalu membuang salinan fotonya) sebelum foto barunya ada di server,
        // dan tanda "waiting to send" catatan baru hilang setelah foto ikut
        // terkirim. Menghapus tetap SESUDAH catatan (lihat di atas).
        t('simpan: foto After dikirim LEBIH DULU, lalu catatannya', [calls.map(c => c[0]), globalDamages.find(d => d.id === 'dB').hasAfterPhoto], [['after', 'rec'], true]);
        // Before tidak tersentuh oleh slot After
        t('slot After tidak menyentuh Before', calls.filter(c => c[0] === 'before' || c[0] === 'delBefore').length, 0);
        // Foto Before mengikuti urutan yang sama — di Edit maupun di Tambah.
        editDamage('dB'); await tick(40);
        await pick(handleDamagePhotoChange, 'before');
        calls.length = 0;
        saveDamage({ preventDefault() {} });
        await tick(30);
        t('Edit: foto Before diganti — foto dulu, lalu catatannya', calls.map(c => c[0]), ['before', 'rec']);
        showAddDamageForm();
        document.getElementById('dmgUnit').value = 'BD021G';
        document.getElementById('dmgType').value = 'Mekanis';
        document.getElementById('dmgSetBreakdown').checked = false;
        await pick(handleDamagePhotoChange, 'before');
        calls.length = 0;
        saveDamage({ preventDefault() {} });
        await tick(30);
        const added = globalDamages[globalDamages.length - 1];
        t('Tambah dengan foto: foto dulu, lalu catatan — dengan id yang sama',
          [calls.map(c => c[0]), calls[0] && calls[0][1] === added.id, calls[1] && calls[1][1] === added.id, added.hasPhoto], [['before', 'rec'], true, true, true]);
        globalDamages = globalDamages.filter(d => d.id !== added.id); saveDamages();

        // =============== 5. PROFIL UNIT ===============
        showUnitProfile('u1');
        const profBtns = [...document.querySelectorAll('#unitProfileModal .dmg-ph')].map(x => x.textContent.trim());
        t('profil unit: Before + After untuk tiap catatan berfoto', profBtns, ['Before', 'After', 'Before', 'After']);
        closeUnitProfile();

        // =============== 6. CSV ===============
        window.__blob = null;
        exportDamageCSV();
        const csv = window.__blob ? await window.__blob.text() : '';
        const head = csv.replace(/^\\uFEFF/, '').split('\\n')[0];
        t('CSV: kolom Photo Before dan Photo After', head, head && /Photo Before"?,"?Photo After/.test(head) ? head : 'Photo Before,Photo After');

        // =============== 7. CADANGAN ===============
        let answers = [];
        window.confirm = m => { confirmText = String(m); return answers.length ? answers.shift() : true; };
        globalDamages = [mk('dB', { hasPhoto: true, hasAfterPhoto: true, resolved: true, resolvedAt: '2026-10-14' }), mk('dE', {})];
        store.after.dB = PH('AB'); store.before.dB = PH('BB');
        teamMembers = []; teamShifts = []; workLogs = []; leaveRequests = [];
        window.__blob = null;
        answers = [true];
        await exportBackup();
        const ex = JSON.parse(await window.__blob.text());
        t('cadangan membawa foto Before dan After', [ex.damagePhotos, ex.damageAfterPhotos], [{ dB: PH('BB') }, { dB: PH('AB') }]);
        // restore GABUNG ke tempat catatannya hilang: kedua foto kembali
        globalDamages = [mk('dE', {})]; saveDamages();
        calls.length = 0;
        answers = [true];
        importBackup(new File([JSON.stringify(ex)], 'b.json', { type: 'application/json' }));
        await tick(400);
        t('restore mengembalikan foto Before dan After', calls.filter(c => c[0] === 'before' || c[0] === 'after').map(c => c.join(':')).sort(), ['after:dB', 'before:dB']);
        t('laporan restore menyebut foto After', [...document.querySelectorAll('#restoreReportBody tr')].some(tr => /after repair/.test(tr.textContent)), true);
        closeRestoreReport();
        // restore GANTI tanpa catatan dB: kedua dokumen fotonya ikut dihapus
        const emptied = { ...ex, damages: [mk('dE', {})], damagePhotos: {}, damageAfterPhotos: {} };
        calls.length = 0;
        answers = [false, true];
        importBackup(new File([JSON.stringify(emptied)], 'e.json', { type: 'application/json' }));
        await tick(400);
        t('restore GANTI: dokumen Before dan After ikut dihapus', calls.filter(c => /^del(Before|After)$/.test(c[0])).map(c => c.join(':')).sort(),
          ['delAfter:dB', 'delBefore:dB']);
        closeRestoreReport();
        window.confirm = m => { confirmText = String(m); return confirmAnswer; };

        // =============== 8. HAPUS ===============
        globalDamages = [mk('dB', { hasPhoto: true, hasAfterPhoto: true, resolved: true }), mk('dF', { hasAfterPhoto: true }), mk('dG', {})];
        saveDamages(); renderDamageTable();
        calls.length = 0;
        deleteDamage('dB');
        t('hapus satu: Before dan After ikut dihapus', calls.map(c => c.join(':')), ['delRec:dB', 'delBefore:dB', 'delAfter:dB']);
        calls.length = 0;
        selectedDamageIds = new Set(['dF', 'dG']);
        deleteSelectedDamages();
        t('hapus terpilih: hanya dokumen yang ada yang dihapus', calls.map(c => c.join(':')).sort(), ['delAfter:dF', 'delRec:dF', 'delRec:dG']);

        // =============== 9. PERUBAHAN DARI PERANGKAT LAIN ===============
        globalDamages = [mk('dH', { hasAfterPhoto: true, updatedAt: 5 })];
        _dmgAfterCache.set('dH', PH('OLD')); _dmgPhotoCache.set('dH', PH('OLDB'));
        applyCloudDamagesSnapshot([mk('dH', { hasAfterPhoto: true, updatedAt: 9 })]);
        t('catatan diubah di perangkat lain: salinan fotonya dilupakan', [_dmgAfterCache.has('dH'), _dmgPhotoCache.has('dH')], [false, false]);
        _dmgAfterCache.set('dH', PH('SAME'));
        applyCloudDamagesSnapshot([mk('dH', { hasAfterPhoto: true, updatedAt: 9 })]);
        t('tanpa perubahan: salinannya dipakai terus', _dmgAfterCache.get('dH'), PH('SAME'));

        // =============== 10. TOMBOL BACK ===============
        t('dialog terdaftar untuk tombol Back / Esc', MODAL_CLOSERS.damageRepairModal, 'closeDamageRepair');
        openDamageRepair('dH');
        closeTopModal();
        t('Back menutup dialognya', isOpen('damageRepairModal'), false);

        // =============== 11. TEMUAN REVIEW ===============
        globalDamages = [mk('rA', { hasPhoto: true }), mk('rB', { hasPhoto: true, hasAfterPhoto: true, resolved: true, resolvedAt: '2026-10-14' }),
                         mk('rC', {}), mk('rD', {})];
        store.before.rA = PH('RA'); store.after.rB = PH('RB');
        saveDamages(); renderDamageTable();

        // a. Esc di penampil foto tidak ikut menutup dialog di bawahnya
        openDamageRepair('rA');
        _repairPhoto = PH('UNSAVED'); setRepairPhotoPreview();
        openPhotoLightbox(_repairPhoto);
        confirmText = '';
        document.activeElement && document.activeElement.blur && document.activeElement.blur();
        document.dispatchEvent(new KeyboardEvent('keydown', { key: 'Escape', bubbles: true }));
        t('Esc menutup penampil saja; dialog dan fotonya tetap', [lb().classList.contains('open'), isOpen('damageRepairModal'), confirmText, _repairPhoto],
          [false, true, '', PH('UNSAVED')]);

        // b. membuka dialog untuk catatan lain saat foto belum disimpan: ditanya
        confirmAnswer = false;
        t('menolak mengganti: tetap di catatan semula', [openDamageRepair('rC'), _repairId, _repairPhoto], [false, 'rA', PH('UNSAVED')]);
        confirmAnswer = true;
        t('setuju: pindah ke catatan lain', [openDamageRepair('rC'), _repairId, _repairPhoto], [true, 'rC', '']);
        closeDamageRepair(true);

        // c/d. "Open it again" untuk foto After yang ditolak
        _unsent.clear(); renderUnsentNotice();
        openDamageRepair('rB'); _repairPhoto = PH('R1'); setRepairPhotoPreview();
        failAfterSave = Object.assign(new Error('denied'), { code: 'permission-denied' });
        saveDamageRepair(); await tick(40);
        t('foto ditolak: tercatat', _unsent.size, 1);
        openDamageRepair('rC'); _repairPhoto = PH('OTHER'); setRepairPhotoPreview();
        confirmAnswer = false;
        reopenUnsent([..._unsent.keys()][0]);
        t('dibuka lagi saat foto lain belum disimpan dan ditolak: catatan tetap tersimpan', [_unsent.size, _repairId, _repairPhoto], [1, 'rC', PH('OTHER')]);
        confirmAnswer = true;
        closeDamageRepair(true);
        globalDamages = globalDamages.filter(d => d.id !== 'rB');
        toasts.length = 0;
        reopenUnsent([..._unsent.keys()][0]);
        t('catatannya sudah dihapus: dikatakan, tidak diam-diam', [/no longer exists/.test(toasts[toasts.length - 1] || ''), isOpen('damageRepairModal')], [true, false]);

        // e. foto masih diproses: tombol simpan menunggu
        // Pengecilan foto dibuat bisa ditahan, supaya jeda "sedang diproses"
        // pasti teramati (foto uji yang kecil selesai dalam sekejap).
        const realCompress = compressImageToDataURL;
        let gate = null;
        window.compressImageToDataURL = () => new Promise(r => { gate = () => r(PH('SHRUNK')); });
        const jpg = () => ({ target: { files: [new File(['x'], 'cam.jpg', { type: 'image/jpeg' })], value: 'x' } });
        openDamageRepair('rC');
        const pending = handleRepairPhotoChange(jpg());
        toasts.length = 0;
        t('sedang diproses: tombol nonaktif, ada tanda', [document.getElementById('damageRepairSave').disabled, vis('dmgRepairLoading')], [true, true]);
        saveDamageRepair();
        t('menyimpan saat diproses ditolak, catatan belum ditandai', [/still being processed/.test(toasts[toasts.length - 1] || ''), !!globalDamages.find(d => d.id === 'rC').resolved], [true, false]);
        gate(); await pending;
        t('selesai diproses: tombol aktif, foto ada', [document.getElementById('damageRepairSave').disabled, vis('dmgRepairLoading'), !!_repairPhoto], [false, false, true]);
        closeDamageRepair(true);
        // diproses lalu dialog ditutup dan dibuka untuk catatan lain: foto tidak nyasar
        openDamageRepair('rC');
        const stray = handleRepairPhotoChange(jpg());
        closeDamageRepair(true);
        openDamageRepair('rD');
        gate(); await stray;
        t('foto yang diproses untuk dialog lama tidak masuk dialog baru', [_repairId, _repairPhoto, document.getElementById('damageRepairSave').disabled], ['rD', '', false]);
        closeDamageRepair(true);

        // f. sudah ditandai selesai di perangkat lain saat dialog terbuka
        openDamageRepair('rD');
        globalDamages.find(d => d.id === 'rD').resolved = true;
        toasts.length = 0;
        saveDamageRepair();
        t('dialog "Mark as repaired" tanpa foto: ditutup, dikatakan sudah selesai', [isOpen('damageRepairModal'), toasts[toasts.length - 1]],
          [false, 'Already marked as repaired (on another device)']);

        // g. firebase-init lama (tanpa fungsi foto After): tidak ada tanda tanpa foto
        const keepFn = window.cloud.saveDamageAfterPhoto; delete window.cloud.saveDamageAfterPhoto;
        globalDamages.push(mk('rE', {})); saveDamages();
        openDamageRepair('rE'); _repairPhoto = PH('OLDVER'); setRepairPhotoPreview();
        calls.length = 0; toasts.length = 0;
        saveDamageRepair();
        const recE = globalDamages.find(d => d.id === 'rE');
        t('versi lama: tidak ditandai selesai/berfoto, dialog tetap dengan fotonya, disuruh muat ulang',
          [!!recE.resolved, !!recE.hasAfterPhoto, calls.length, isOpen('damageRepairModal'), _repairPhoto, /Reload the page/.test(toasts[toasts.length - 1] || '')],
          [false, false, 0, true, PH('OLDVER'), true]);
        closeDamageRepair(true);
        editDamage('rE'); await tick(30);
        _dmgAfterData = PH('OLDVER2'); _dmgAfterDirty = true;
        toasts.length = 0; calls.length = 0;
        saveDamage({ preventDefault() {} });
        t('form Edit versi lama: tidak disimpan, form tetap terbuka', [calls.length, isOpen('damageModal'), !!globalDamages.find(d => d.id === 'rE').hasAfterPhoto, /Reload the page/.test(toasts[toasts.length - 1] || '')],
          [0, true, false, true]);
        closeDamageModal(true);
        window.cloud.saveDamageAfterPhoto = keepFn;

        // h. foto form yang diproses untuk catatan A tidak masuk form catatan B
        globalDamages.push(mk('rF', { hasAfterPhoto: true, resolved: true })); store.after.rF = PH('RF'); saveDamages();
        editDamage('rE'); await tick(20);
        const strayForm = handleDamagePhotoChange(jpg(), 'after');
        toasts.length = 0;
        saveDamage({ preventDefault() {} });
        t('form: menyimpan saat foto diproses ditolak', [/still being processed/.test(toasts[toasts.length - 1] || ''), isOpen('damageModal')], [true, true]);
        closeDamageModal(true);
        _dmgAfterCache.delete('rF');
        editDamage('rF');
        gate(); await strayForm; await tick(40);
        window.compressImageToDataURL = realCompress;
        t('foto untuk catatan lain tidak masuk form ini', [_dmgAfterDirty, _dmgAfterData, document.getElementById('dmgAfterPreview').src], [false, PH('RF'), PH('RF')]);
        closeDamageModal(true);

        // i. tanda "Loading photo…" tidak tertinggal
        let release;
        const slowGet = window.cloud.getDamageAfterPhoto;
        window.cloud.getDamageAfterPhoto = () => new Promise(r => { release = () => r(PH('SLOW')); });
        _dmgAfterCache.delete('rF');
        editDamage('rF');
        t('sedang dimuat', vis('dmgAfterLoading'), true);
        removeDamagePhoto('after');
        release(); await tick(20);
        t('dihapus saat dimuat: tanda hilang, foto lama tidak kembali', [vis('dmgAfterLoading'), vis('dmgAfterPreviewWrap'), _dmgAfterData], [false, false, '']);
        closeDamageModal(true);
        window.cloud.getDamageAfterPhoto = slowGet;

        // j. restore GANTI dari cadangan lama: catatan tetap, fotonya tidak ada di berkas
        answers = [];
        window.confirm = m => { confirmText = String(m); return answers.length ? answers.shift() : true; };
        globalDamages = [mk('rG', { hasPhoto: true, hasAfterPhoto: true, resolved: true }), mk('rH', { hasAfterPhoto: true })];
        saveDamages();
        const oldFile = { version: 5, units: globalData.slice(), damages: [mk('rG', { resolved: true }), mk('rH', { hasAfterPhoto: true })] };
        calls.length = 0;
        answers = [false, true];
        importBackup(new File([JSON.stringify(oldFile)], 'old.json', { type: 'application/json' }));
        await tick(400);
        t('restore GANTI: dokumen foto dari catatan yang di berkas tidak berfoto ikut dihapus',
          calls.filter(c => /^del(Before|After)$/.test(c[0])).map(c => c.join(':')).sort(), ['delAfter:rG', 'delBefore:rG']);
        closeRestoreReport();
        window.confirm = m => { confirmText = String(m); return confirmAnswer; };

        // =============== 12. SHOULD-FIX ponytail-review ===============
        // 12a. foto yang diketuk tampil dulu, sisi lainnya menyusul
        globalDamages = [mk('sA', { hasPhoto: true, hasAfterPhoto: true, resolved: true, resolvedAt: '2026-10-14' })];
        saveDamages();
        _dmgAfterCache.set('sA', PH('SA_AFTER')); _dmgPhotoCache.delete('sA');
        let releaseBefore = null;
        const realGetBefore = window.cloud.getDamagePhoto;
        window.cloud.getDamagePhoto = () => new Promise(r => { releaseBefore = () => r(PH('SA_BEFORE')); });
        const opening = openDamagePhotos('sA', 'after', null);
        await tick(10);
        t('After yang diketuk langsung tampil, tidak menunggu unduhan Before',
          [lb().classList.contains('open'), document.getElementById('photoLightboxImg').src, vis('photoLightboxCount')], [true, PH('SA_AFTER'), false]);
        releaseBefore(); await opening;
        t('Before menyusul: bisa dibalik dan dibuat berdampingan, tetap di After',
          [document.getElementById('photoLightboxImg').src, document.getElementById('photoLightboxCount').textContent, vis('photoLightboxMode')], [PH('SA_AFTER'), '2 / 2', true]);
        closePhotoLightbox();
        _dmgPhotoCache.delete('sA');
        const opening2 = openDamagePhotos('sA', 'after', null);
        await tick(10);
        closePhotoLightbox();
        releaseBefore(); await opening2;
        t('ditutup sebelum Before tiba: penampil tidak muncul lagi', lb().classList.contains('open'), false);
        window.cloud.getDamagePhoto = realGetBefore;

        // 12b. hasil kosong tidak diingat (foto dari HP lain belum sampai)
        globalDamages.push(mk('sB', { hasPhoto: true, hasAfterPhoto: true, resolved: true }));
        delete store.after.sB; delete store.before.sB; _dmgAfterCache.delete('sB'); _dmgPhotoCache.delete('sB');
        toasts.length = 0;
        await openDamagePhotos('sB', 'after', null);
        await openDamagePhotos('sB', 'before', null);
        t('foto belum ada di server: "Photo not found", tidak terbuka', [toasts.slice(-2), lb().classList.contains('open')], [['Photo not found', 'Photo not found'], false]);
        store.after.sB = PH('SB_AFTER'); store.before.sB = PH('SB_BEFORE');
        await openDamagePhotos('sB', 'after', null);
        t('After tiba kemudian: ketukan berikutnya menampilkannya', [lb().classList.contains('open'), document.getElementById('photoLightboxImg').src], [true, PH('SB_AFTER')]);
        closePhotoLightbox();
        await openDamagePhotos('sB', 'before', null);
        t('Before juga', document.getElementById('photoLightboxImg').src, PH('SB_BEFORE'));
        closePhotoLightbox();

        // 12c. form Edit: hasil muat foto catatan sebelumnya tidak mengganggu
        let failC = null, loadD = null;
        window.cloud.getDamagePhoto = id => new Promise((res, rej) => {
            if (id === 'sC') failC = () => rej(new Error('offline')); else loadD = () => res(PH('SD_BEFORE'));
        });
        globalDamages.push(mk('sC', { hasPhoto: true }), mk('sD', { hasPhoto: true }));
        _dmgPhotoCache.delete('sC'); _dmgPhotoCache.delete('sD');
        editDamage('sC'); closeDamageModal(true); editDamage('sD');
        toasts.length = 0;
        failC(); await tick(10);
        t('gagal memuat foto catatan sebelumnya: form ini tetap "Loading", tanpa pesan salah',
          [vis('dmgPhotoLoading'), toasts.filter(x => /failed to load/.test(x)).length], [true, 0]);
        loadD(); await tick(10);
        t('foto catatan yang terbuka tetap dimuat', [vis('dmgPhotoLoading'), document.getElementById('dmgPhotoPreview').src], [false, PH('SD_BEFORE')]);
        closeDamageModal(true);
        window.cloud.getDamagePhoto = realGetBefore;

        // 12d. restore GANTI yang menghapus catatan juga membuang salinan fotonya
        answers = [];
        window.confirm = m => { confirmText = String(m); return answers.length ? answers.shift() : true; };
        globalDamages = [mk('sE', { hasPhoto: true, hasAfterPhoto: true }), mk('sF', {})]; saveDamages();
        _dmgPhotoCache.set('sE', PH('E1')); _dmgAfterCache.set('sE', PH('E2'));
        calls.length = 0;
        answers = [false, true];
        importBackup(new File([JSON.stringify({ version: 5, units: globalData.slice(), damages: [mk('sF', {})] })], 'f.json', { type: 'application/json' }));
        await tick(400);
        t('restore GANTI: dokumen dan salinan foto catatan yang dihapus ikut hilang',
          [calls.filter(c => /^del(Before|After)$/.test(c[0])).map(c => c.join(':')).sort(), _dmgPhotoCache.has('sE'), _dmgAfterCache.has('sE')],
          [['delAfter:sE', 'delBefore:sE'], false, false]);
        closeRestoreReport();
        window.confirm = m => { confirmText = String(m); return confirmAnswer; };

        // k. keluar / akses dicabut: salinan foto After ikut dibuang bersama
        // sesi, seperti Before — di HP bersama, akun berikutnya tanpa akses
        // Kerusakan tidak boleh masih bisa membukanya dari memori.
        _dmgAfterCache.set('zz', PH('Z')); _dmgPhotoCache.set('zz', PH('Z'));
        tearDownCloudSync();
        t('keluar: salinan foto Before dan After dibuang', [_dmgAfterCache.size, _dmgPhotoCache.has('zz')], [0, false]);

        window.__T = T;
    } catch (e) { window.__T = [{ n: 'EXCEPTION ' + e.message + ' ' + e.stack, pass: false }]; } })();` });

    await page.waitForFunction(() => window.__T, null, { timeout: 120000 });
    const T = await page.evaluate(() => window.__T);

    // HP tegak: dua foto bertumpuk, bukan berdampingan yang sempit.
    await page.setViewportSize({ width: 390, height: 800 });
    const phone = await page.evaluate(() => {
        openPhotoLightbox(['data:image/jpeg;base64,QQ==', 'data:image/jpeg;base64,Qg=='], 0, { captions: ['Before', 'After'], compare: true });
        togglePhotoLightboxCompare();
        const dir = getComputedStyle(document.getElementById('photoLightboxCompare')).flexDirection;
        closePhotoLightbox();
        return dir;
    });
    T.push({ n: 'HP: Before di atas After', g: phone, w: 'column', pass: phone === 'column' });
    await page.setViewportSize({ width: 1440, height: 900 });
    const desk = await page.evaluate(() => {
        openPhotoLightbox(['data:image/jpeg;base64,QQ==', 'data:image/jpeg;base64,Qg=='], 0, { captions: ['Before', 'After'], compare: true });
        togglePhotoLightboxCompare();
        const dir = getComputedStyle(document.getElementById('photoLightboxCompare')).flexDirection;
        closePhotoLightbox();
        return dir;
    });
    T.push({ n: 'layar lebar: berdampingan', g: desk, w: 'row', pass: desk === 'row' });
    // HP miring: tetap berdampingan, bukan dua foto kecil bertumpuk.
    await page.setViewportSize({ width: 640, height: 360 });
    const land = await page.evaluate(() => {
        openPhotoLightbox(['data:image/jpeg;base64,QQ==', 'data:image/jpeg;base64,Qg=='], 0, { captions: ['Before', 'After'], compare: true });
        togglePhotoLightboxCompare();
        const dir = getComputedStyle(document.getElementById('photoLightboxCompare')).flexDirection;
        closePhotoLightbox();
        return dir;
    });
    T.push({ n: 'HP miring: berdampingan', g: land, w: 'row', pass: land === 'row' });

    let pass = 0;
    T.forEach(x => { if (x.pass) pass++; else console.log('FAIL', x.n, '\n  dapat :', JSON.stringify(x.g), '\n  harap :', JSON.stringify(x.w)); });
    if (errors.length) console.log('PAGE ERRORS:', errors);
    console.log(`${pass}/${T.length} lulus`);
    await b.close();
    process.exit(pass === T.length && !errors.length ? 0 : 1);
})();
