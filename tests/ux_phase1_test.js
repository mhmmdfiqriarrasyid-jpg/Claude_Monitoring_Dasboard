// v156 — perbaikan tampilan fase 1 (HP di lapangan), dari review ui-ux-pro-max:
// 1.1 tombol aksi kartu berlabel dan tidak keluar layar; 1.2 form kerusakan
// muat di layar HP; 1.3 ketikan tidak dibuang tanpa ditanya; 1.4 masuk akun
// terlihat sedang diproses; 1.5 hasil pencarian unit tampil; 1.6 pindah
// halaman mulai dari atas, Back kembali ke posisi semula; 1.7 menu HP bisa
// ditutup (ketuk luar, Esc, Back, tombol ✕); 1.8 titik offline tidak hijau;
// 1.9 tombol menu menandai ada yang menunggu.
const { launch, BASE_URL } = require('./_env');

(async () => {
    const b = await launch();
    const page = await b.newPage({ viewport: { width: 390, height: 844 } });
    const errors = [];
    page.on('pageerror', e => { if (!/Chart is not defined/.test(e.message)) errors.push(e.message); });
    await page.goto(BASE_URL + '/index.html', { waitUntil: 'load' });
    await page.waitForTimeout(1000);

    // ---------- 1.4 masuk akun, sebelum data apa pun ----------
    const signin = await page.evaluate(async () => {
        const out = {};
        let rejectIt;
        window.cloud = { isReady: true, signIn: () => new Promise((res, rej) => { rejectIt = rej; }) };
        document.getElementById('signInEmail').value = 'teknisi@ot.id';
        document.getElementById('signInPassword').value = 'xxxxxxxx';
        document.getElementById('signInForm').requestSubmit();
        await new Promise(r => setTimeout(r, 50));
        const btn = document.querySelector('#signInForm button[type=submit]');
        const r = document.getElementById('loadingText').getBoundingClientRect();
        out.busy = [btn.disabled, btn.textContent.trim(), document.getElementById('loadingText').textContent,
                    document.elementFromPoint(r.left + r.width / 2, r.top + r.height / 2) === document.getElementById('loadingText')];
        rejectIt(Object.assign(new Error('x'), { code: 'auth/network-request-failed' }));
        await new Promise(r => setTimeout(r, 50));
        out.done = [btn.disabled, /Sign In/.test(btn.textContent), document.getElementById('loadingOverlay').classList.contains('active'),
                    document.getElementById('loadingText').textContent];
        // Berhasil: profil masih dibaca, jadi tetap "sibuk" sampai layar berikutnya tampil.
        let resolveIt;
        window.cloud.signIn = () => new Promise(res => { resolveIt = res; });
        document.getElementById('signInForm').requestSubmit();
        await new Promise(r => setTimeout(r, 30));
        resolveIt({ uid: 'x' });
        await new Promise(r => setTimeout(r, 50));
        out.afterResolve = [btn.disabled, document.getElementById('loadingOverlay').classList.contains('active')];
        showPendingGate('x@y.id');
        out.afterScreen = [btn.disabled, document.getElementById('loadingOverlay').classList.contains('active')];
        showAuthGate();
        return out;
    });

    await page.addScriptTag({ content: `(async () => { try {
        const T = []; const t = (n, g, w) => T.push({ n, g, w, pass: JSON.stringify(g) === JSON.stringify(w) });
        const tick = ms => new Promise(r => setTimeout(r, ms || 20));
        const toasts = []; window.showToast = m => toasts.push(String(m));
        let confirmAnswer = true, asked = [];
        window.confirm = m => { asked.push(String(m)); return confirmAnswer; };
        const ok = () => Promise.resolve();
        window.cloud = { isReady: true, saveUnits: ok, updateUnitsFields: ok, deleteUnits: ok, getAllUnits: () => Promise.resolve([]),
            addHistoryEvents: ok, subscribeUsers: () => () => {}, saveDamage: ok, saveDamages: ok, getDamagePhoto: () => Promise.resolve(''),
            getDamageAfterPhoto: () => Promise.resolve(''), saveWorkLog: ok, saveLeaveRequest: ok, saveInspection: ok };
        currentUser = { uid: 'uO', email: 'o@x.id' };
        currentUserDoc = { role: 'owner', status: 'active', displayName: 'O' };
        hideAuthGates(); applyRoleGating();
        const units = [];
        for (let i = 1; i <= 24; i++) units.push({ id: 'u' + i, name: 'GGTR' + (100 + i) + 'G', sn: 'SN' + i, model: 'JOHN DEERE 6110B',
            site: ['PT. GPA', 'PT. MNM'][i % 2], status: i === 3 ? 'Breakdown' : 'Good', display: 'Good', gps: 'Good', steering: 'Good', jdlink: 'Good',
            implement: 'Root Rake — ISJ', yearReceived: '2022', userCategory: 'Planting' });
        for (let i = 1; i <= 6; i++) units.push({ id: 'h' + i, unitGroup: 'heavy', name: 'BD10' + i + 'T', sn: 'LG' + i, brand: 'LiuGong', model: 'CLGB230',
            machineType: 'Excavator', site: 'PT. GPA', status: 'Good', cameraAi: 'Good', telematicBox: 'Good', switchLimiter: 'Good', rotaryLamp: 'Good' });
        globalData = units; saveToStorage(globalData);
        globalDamages = [{ id: 'd1', date: '2026-10-12', unitId: 'h2', unitName: 'BD102T', sn: 'LG2', site: 'PT. GPA', damageType: 'Mekanis',
            component: 'Switch Limiter', description: 'Kronologi : bracket bengkok', resolved: false, resolvedAt: '', createdAt: 1, updatedAt: 1 }];
        saveDamages();
        teamMembers = [{ id: 'm1', name: 'Andi', active: true, company: 'PT. GPA' }];
        filteredData = globalData.slice();
        try { onDataLoaded(); } catch (_) {}
        const isOpen = id => document.getElementById(id).classList.contains('open');
        const vw = innerWidth;

        // =============== 1.1 tombol aksi kartu ===============
        goNav('editUnits', 'tractor'); await tick(200);
        const row = document.querySelector('#editBody tr');
        const btns = [...row.querySelectorAll('td.col-actions .btn')];
        t('kartu unit: keempat tombol aksi ada di dalam layar', btns.map(x => x.getBoundingClientRect().right <= vw + 0.5 && x.getBoundingClientRect().left >= 0), btns.map(() => true));
        t('dan tiap tombol menulis artinya', btns.map(x => getComputedStyle(x, '::after').content), btns.map(x => JSON.stringify(x.getAttribute('title'))));
        t('tinggi tombol cukup untuk jari (≥44px)', btns.every(x => x.getBoundingClientRect().height >= 44), true);
        t('mode ringkas disembunyikan di HP', getComputedStyle(document.getElementById('compactToggle')).display, 'none');

        // =============== 1.2 form kerusakan ===============
        goNav('damage', 'heavy'); await tick(100);
        editDamage('d1'); await tick(150);
        const body = document.querySelector('#damageModal .modal-body');
        const unitIn = document.getElementById('dmgUnit');
        t('form kerusakan tidak melebar keluar layar', [body.scrollWidth <= body.clientWidth, unitIn.getBoundingClientRect().right <= body.getBoundingClientRect().right + 0.5], [true, true]);
        closeDamageModal(true); await tick(50);

        // =============== 1.3 ketikan tidak dibuang tanpa ditanya ===============
        showAddDamageForm(); await tick(30);
        asked = []; closeDamageModal(); await tick(30);
        t('tanpa ketikan: langsung tertutup, tanpa pertanyaan', [isOpen('damageModal'), asked.length], [false, 0]);
        showAddDamageForm(); await tick(30);
        document.getElementById('dmgDescription').value = 'Kronologi : hidrolik bocor di silinder kiri saat bekerja di blok 12';
        confirmAnswer = false; asked = [];
        closeDamageModal(); await tick(30);
        t('ada ketikan: ditanya, batal = tetap terbuka dengan isinya', [isOpen('damageModal'), /Discard what you typed/.test(asked[0] || ''), document.getElementById('dmgDescription').value.length > 20], [true, true, true]);
        // Tombol Back sistem: entri riwayat dikembalikan, form tetap terbuka.
        asked = [];
        history.back(); await tick(250);
        t('Back dengan ketikan: ditanya, batal = tetap terbuka', [isOpen('damageModal'), asked.length >= 1], [true, true]);
        confirmAnswer = true;
        closeDamageModal(); await tick(30);
        t('setuju dibuang: tertutup', isOpen('damageModal'), false);
        // Simpan memakai force: tidak ditanya.
        editDamage('d1'); await tick(100);
        document.getElementById('dmgDescription').value = 'Teks baru';
        asked = []; closeDamageModal(true); await tick(30);
        t('setelah simpan (force): tidak ditanya', [isOpen('damageModal'), asked.length], [false, 0]);
        // Dibuka lagi dari "Not saved": isinya belum pernah tersimpan, jadi ditanya walau tidak diubah.
        showAddDamageForm(); await tick(30);
        document.getElementById('dmgDescription').value = 'Tertahan di HP';
        const snap = _snapDamage();
        closeDamageModal(true); await tick(30);
        _reopenDamage(snap); await tick(30);
        confirmAnswer = false; asked = [];
        closeDamageModal(); await tick(30);
        t('dibuka lagi dari "Not saved": menutup tetap ditanya', [isOpen('damageModal'), asked.length], [true, 1]);
        confirmAnswer = true; closeDamageModal(); await tick(30);
        // Laporan harian, izin, unit, inspeksi: perilaku yang sama.
        navigateTo('team'); switchTeamTab('worklog'); await tick(80);
        showAddWorkLogForm(); await tick(30);
        document.getElementById('wlTask').value = 'Ganti oli hidrolik';
        confirmAnswer = false; asked = [];
        closeWorkLogModal(); await tick(30);
        t('laporan harian: ketikan ditanya', [isOpen('workLogModal'), asked.length], [true, 1]);
        confirmAnswer = true; closeWorkLogModal(); await tick(30);
        showAddLeaveForm(); await tick(30);
        const lvReason = document.querySelector('#leaveForm textarea, #leaveForm input[type=text]');
        lvReason.value = 'Demam tinggi';
        confirmAnswer = false; asked = [];
        closeLeaveModal(); await tick(30);
        t('izin / sakit: ketikan ditanya', [isOpen('leaveModal'), asked.length], [true, 1]);
        confirmAnswer = true; closeLeaveModal(); await tick(30);
        goNav('editUnits', 'tractor'); await tick(80);
        showAddForm(); await tick(30);
        document.getElementById('formName').value = 'GGTR999G';
        confirmAnswer = false; asked = [];
        closeModal(); await tick(30);
        t('form unit: ketikan ditanya', [isOpen('unitModal'), asked.length], [true, 1]);
        confirmAnswer = true; closeModal(); await tick(30);
        t('form unit: setuju dibuang = tertutup', isOpen('unitModal'), false);

        // =============== 1.5 pencarian unit ===============
        goNav('dashboard', 'tractor'); await tick(100);
        const g = document.getElementById('globalSearch');
        g.value = 'GGTR10'; renderGlobalSearchResults();
        const box = document.getElementById('globalSearchResults');
        const br = box.getBoundingClientRect();
        t('hasil pencarian tampil', [getComputedStyle(box).display, box.querySelectorAll('.global-search__item').length > 0], ['block', true]);
        t('di HP selebar layar, di bawah bar atas', [br.left >= 0, br.right <= vw, br.top >= 54], [true, true, true]);
        t('selama daftar terbuka, bar atas di atas banner sesi', document.body.classList.contains('gsearch-open'), true);
        g.value = 'tidakadaunitini'; renderGlobalSearchResults();
        t('tidak ada yang cocok: pesannya tampil', [getComputedStyle(box).display, /No matching units/.test(box.textContent)], ['block', true]);
        closeGlobalSearch(); g.value = '';
        t('daftar ditutup: kelasnya dilepas', document.body.classList.contains('gsearch-open'), false);

        // =============== 1.6 posisi gulir ===============
        goNav('dashboard', 'tractor'); await tick(150);
        window.scrollTo(0, 1500); await tick(50);
        const yBefore = Math.round(scrollY);
        goNav('damage', 'heavy'); await tick(150);
        t('halaman baru dibuka dari atas', Math.round(scrollY), 0);
        history.back(); await tick(350);
        t('Back: kembali ke dashboard di posisi semula', [currentView, Math.abs(Math.round(scrollY) - yBefore) <= 2, yBefore > 500], ['dashboard', true, true]);

        // =============== 1.7 menu HP ===============
        const shell = document.getElementById('appShell');
        openNav(); await tick(30);
        t('menu terbuka: punya entri riwayat sendiri', [shell.classList.contains('nav-open'), history.state && history.state.ot], [true, 'drawer']);
        t('ada tombol tutup di dalam menu', getComputedStyle(document.querySelector('.sidebar__close')).display !== 'none', true);
        document.dispatchEvent(new KeyboardEvent('keydown', { key: 'Escape', bubbles: true })); await tick(250);
        t('Esc menutup menu (dan entrinya)', [shell.classList.contains('nav-open'), history.state && history.state.ot], [false, 'view']);
        openNav(); await tick(30);
        shell.dispatchEvent(new MouseEvent('click', { bubbles: true }));  // ketukan pada area gelap = target #appShell
        await tick(250);
        t('ketuk di luar menu menutupnya', shell.classList.contains('nav-open'), false);
        const viewBefore = currentView;
        openNav(); await tick(30);
        history.back(); await tick(300);
        t('Back menutup menu dan tidak mengganti halaman', [shell.classList.contains('nav-open'), currentView], [false, viewBefore]);
        openNav(); await tick(30);
        document.querySelector('.sidebar__close').click(); await tick(250);
        t('tombol ✕ menutup menu', shell.classList.contains('nav-open'), false);
        // Memilih halaman dari menu: entri menu dipakai halaman baru; Back kembali ke halaman sebelumnya.
        openNav(); await tick(30);
        document.querySelector('.nav__link[data-view="warehouse"]').click(); await tick(250);
        t('pilih halaman dari menu: menu tertutup, halaman berganti', [shell.classList.contains('nav-open'), currentView, history.state && history.state.ot], [false, 'warehouse', 'view']);
        history.back(); await tick(350);
        t('lalu Back kembali ke halaman sebelumnya', currentView, viewBefore);
        // Dari menu juga: Back kembali ke posisi gulir halaman sebelumnya.
        goNav('dashboard', 'tractor'); await tick(150);
        window.scrollTo(0, 1200); await tick(50);
        const y2 = Math.round(scrollY);
        openNav(); await tick(30);
        document.querySelector('.nav__link[data-view="warehouse"]').click(); await tick(250);
        t('halaman dari menu dibuka dari atas', Math.round(scrollY), 0);
        history.back(); await tick(350);
        t('Back: dashboard di posisi semula', [currentView, Math.abs(Math.round(scrollY) - y2) <= 2], ['dashboard', true]);

        // Halaman yang sedang tampil dipilih lagi dari menu: tidak menambah entri.
        goNav('damage', 'heavy'); await tick(150);
        goNav('dashboard', 'tractor'); await tick(150);
        openNav(); await tick(30);
        document.querySelector('.nav__link.active[data-view="dashboard"]').click(); await tick(300);
        history.back(); await tick(350);
        t('memilih halaman yang sama dari menu: Back langsung ke halaman sebelumnya', currentView, 'damage');
        // History dibuka dari menu: menu tertutup, entrinya dipakai modal; Back berikutnya kembali.
        goNav('dashboard', 'tractor'); await tick(150);
        openNav(); await tick(30);
        document.getElementById('navHistory').click(); await tick(150);
        t('History dari menu: menu tertutup, entri menjadi milik modal', [isNavOpen(), isOpen('historyModal'), history.state && history.state.ot], [false, true, 'modal']);
        closeHistory(); await tick(300);
        history.back(); await tick(350);
        t('setelah History ditutup, Back kembali ke halaman sebelumnya', currentView, 'damage');

        // Dibuka lagi dari "Not saved" tetapi ditolak: form kosong berikutnya tidak bertanya.
        navigateTo('team'); switchTeamTab('worklog'); await tick(80);
        const membersBak = teamMembers;
        teamMembers = [];
        _reopenWorkLog({ vals: { editWorkLogId: '', wlTask: 'x' }, units: [], photos: null }); await tick(30);
        teamMembers = membersBak;
        const opened = isOpen('workLogModal');
        if (!opened) { showAddWorkLogForm(); await tick(30); }
        asked = []; confirmAnswer = true; closeWorkLogModal(); await tick(30);
        t('pembukaan ulang yang ditolak tidak membuat form kosong bertanya', [opened, asked.length], [false, 0]);
        // Anggota nonaktif: daftar anggota dibangun ulang saat form terbuka, pilihannya tetap.
        teamMembers = [{ id: 'm1', name: 'Andi', active: true }, { id: 'm2', name: 'Budi', active: false }];
        leaveRequests = [{ id: 'lv1', memberId: 'm2', memberName: 'Budi', type: 'izin', dateFrom: '2026-10-20', dateTo: '2026-10-20', days: 1, approval: 'draft', createdByUid: 'uO' }];
        editLeave('lv1'); await tick(40);
        populateLeaveFilters();
        asked = []; closeLeaveModal(); await tick(30);
        t('anggota nonaktif tetap terpilih, tanpa pertanyaan palsu', [asked.length, isOpen('leaveModal')], [0, false]);
        teamMembers = [{ id: 'm1', name: 'Andi', active: true, company: 'PT. GPA' }];

        // =============== 1.8 offline ===============
        Object.defineProperty(navigator, 'onLine', { configurable: true, get: () => false });
        document.getElementById('connectionDot').classList.add('connected');
        updateConnectionLabel(); await tick(400);   // the dot's colour change is animated
        const dot = getComputedStyle(document.getElementById('connectionDot'));
        const warn = getComputedStyle(document.documentElement).getPropertyValue('--warning').trim();
        const hex = c => '#' + c.match(/\\d+/g).slice(0, 3).map(n => (+n).toString(16).padStart(2, '0')).join('').toUpperCase();
        t('offline: titik kuning, tidak berdenyut', [hex(dot.backgroundColor), dot.animationName], [warn.toUpperCase(), 'none']);
        const lab = document.getElementById('connectionLabel');
        t('di HP tertulis "Offline", kalimat lengkap tetap untuk pembaca layar', [getComputedStyle(lab.querySelector('.conn-short')).display, lab.querySelector('.conn-long').getBoundingClientRect().width <= 1, /changes saved on this device/.test(lab.textContent)], ['inline', true, true]);
        t('kotak pencarian tidak terhimpit', document.getElementById('globalSearchWrap').getBoundingClientRect().width > 150, true);
        Object.defineProperty(navigator, 'onLine', { configurable: true, get: () => true });
        updateConnectionLabel();
        t('online lagi: kembali normal', [lab.textContent, lab.hasAttribute('title')], [globalData.length + ' Units', false]);

        // =============== 1.9 tombol menu ===============
        const dBadge = document.getElementById('decisionBadge');
        dBadge.textContent = '5'; dBadge.style.display = '';
        updateMenuButtonLabel();
        const burger = document.querySelector('.topbar__burger');
        t('ada keputusan menunggu: titik di tombol menu dan labelnya', [burger.classList.contains('has-pending'), getComputedStyle(burger, '::after').content !== 'none', burger.getAttribute('aria-label')],
          [true, true, 'Menu — 5 decisions waiting']);
        dBadge.style.display = 'none'; document.getElementById('inspectionBadge').style.display = 'none'; document.getElementById('teamBadge').style.display = 'none';
        updateMenuButtonLabel();
        t('tidak ada yang menunggu: tanpa titik', [burger.classList.contains('has-pending'), burger.getAttribute('aria-label')], [false, 'Menu']);

        window.__T = T;
    } catch (e) { window.__T = [{ n: 'EXCEPTION ' + e.message + ' ' + e.stack, pass: false }]; } })();` });

    await page.waitForFunction(() => window.__T, null, { timeout: 120000 });
    const T = await page.evaluate(() => window.__T);
    const t = (n, g, w) => T.unshift({ n, g, w, pass: JSON.stringify(g) === JSON.stringify(w) });
    await page.setViewportSize({ width: 1366, height: 768 });
    const desk = await page.evaluate(async () => {
        const tick = ms => new Promise(r => setTimeout(r, ms));
        const m = document.querySelector('.main');
        goNav('editUnits', 'tractor'); await tick(200);
        m.scrollTop = 300; await tick(50);
        const saved = m.scrollTop;
        goNav('damage', 'heavy'); await tick(200);
        const top0 = m.scrollTop;
        history.back(); await tick(400);
        return [currentView, m.scrollTop === saved, top0, Math.round(window.scrollY), Math.round(document.querySelector('.topbar').getBoundingClientRect().top)];
    });
    t('laptop: Back memulihkan posisi .main, jendela dan bar atas tidak bergeser', desk, ['editUnits', true, 0, 0, 0]);
    t('masuk akun: tombol nonaktif, teks proses, lapisan loading di atas kartu', signin.busy,
      [true, 'Signing in…', 'Signing in… on a weak signal this can take a while', true]);
    t('gagal masuk: tombol aktif lagi, loading hilang', signin.done, [false, true, false, '']);
    t('berhasil: tetap sibuk selama profil dimuat', signin.afterResolve, [true, true]);
    t('layar berikutnya tampil: sibuk selesai', signin.afterScreen, [false, false]);

    let pass = 0;
    T.forEach(x => { if (x.pass) pass++; else console.log('FAIL', x.n, '\n  dapat :', JSON.stringify(x.g), '\n  harap :', JSON.stringify(x.w)); });
    if (errors.length) console.log('PAGE ERRORS:', errors);
    console.log(`${pass}/${T.length} lulus`);
    await b.close();
    process.exit(pass === T.length && !errors.length ? 0 : 1);
})();
