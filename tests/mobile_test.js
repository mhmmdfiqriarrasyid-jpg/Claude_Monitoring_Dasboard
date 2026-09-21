// Ukuran sentuh dan keterbacaan di browser ponsel.
//
// Semua angka di sini diukur di Chromium sungguhan pada 360x740 — lebar
// Android yang umum, bukan simulasi. Uji ini ada karena ukuran seperti ini
// mudah pelan-pelan kembali mengecil tanpa ada yang sadar: tidak ada yang
// rusak, tidak ada yang error, aplikasinya cuma jadi susah dipakai lagi.
//
// Yang paling penting di berkas ini adalah pemeriksaan 16px. Safari iPhone
// nge-zoom halaman setiap kali field dengan font di bawah 16px difokuskan, dan
// tidak pernah zoom kembali — jadi mengisi satu laporan kerusakan meninggalkan
// seluruh aplikasi dalam keadaan membesar dan menggeser ke samping.
const { launch, BASE_URL } = require('./_env');

const SEED = `(() => {
    window.cloud = { isReady:true, saveUnits:()=>Promise.resolve(), getAllUnits:()=>Promise.resolve([]),
        saveShift:()=>Promise.resolve(), deleteShift:()=>Promise.resolve(),
        saveDamage:()=>Promise.resolve(), saveDamages:()=>Promise.resolve(),
        addHistoryEvents:()=>Promise.resolve(), subscribeUsers:()=>()=>{},
        getDamagePhoto:()=>Promise.resolve('') };
    currentUser = { uid:'uO', email:'o@x.id' };
    currentUserDoc = { role:'owner', status:'active', displayName:'Fikri' };
    hideAuthGates(); applyRoleGating();
})()`;

const DATA = `(() => {
    globalData = [{ id:'u_1', name:'GGTR141G_SCL', model:'JOHN DEERE 6110B',
        sn:'1BM7230CHS3002331', implement:'Rotary Tiller', site:'PT. Global Papua Abadi',
        yearReceived:'2021', status:'Good', display:'Good', gps:'Good', steering:'Good',
        jdlink:'Good', userCategory:'Operator', gpsLicense:'SF-RTK',
        licenseDisplay:'G5 Advance', remarks:'x' }];
    filteredData = [...globalData];
    globalDamages = [{ id:'d1', date:'2026-09-01', unitId:'u_1', unitName:'GGTR141G_SCL',
        sn:'1BM7230CHS3002331', site:'PT. Global Papua Abadi', damageType:'Mekanis',
        component:'Hidrolik', description:'Selang bocor', hasPhoto:true,
        resolved:false, resolvedAt:'', createdAt:1, updatedAt:1 }];
    teamMembers = [{ id:'m1', name:'Yulianus Mop Anggawen', jobTitle:'Mekanik Lapangan',
        company:'PT. Global Papua Abadi', active:true }];
    teamShifts = []; workLogs = [];
})()`;

async function run(page, width, body) {
    await page.goto(BASE_URL + '/index.html', { waitUntil: 'load' });
    await page.waitForTimeout(900);
    await page.addScriptTag({ content: SEED });
    await page.waitForTimeout(200);
    return page.evaluate(body);
}

(async () => {
    const b = await launch();
    const T = [];
    const t = (n, g, w) => T.push({ n, g, w, pass: JSON.stringify(g) === JSON.stringify(w) });
    const errors = [];

    // ---------------- ponsel 360px ----------------
    const phone = await b.newPage({ viewport:{ width:360, height:740 }, isMobile:true, hasTouch:true });
    phone.on('pageerror', e => { if (!/Chart is not defined/.test(e.message)) errors.push(e.message); });

    const m = await run(phone, 360, `(() => {
        const px = el => parseFloat(getComputedStyle(el).fontSize);
        const h = el => Math.round(el.getBoundingClientRect().height);
        const w = el => Math.round(el.getBoundingClientRect().width);
        const vis = el => { const r = el.getBoundingClientRect(); return r.width > 0 && r.height > 0; };

        // Semua modal dibuka sekaligus supaya setiap field ikut terukur.
        const ids = ['unitModal','damageModal','workLogModal','deviceModal','stockModal',
                     'licenseModal','implementModal','bulkEditModal'];
        ids.forEach(i => { const el = document.getElementById(i); if (el) el.classList.add('open'); });

        const fields = [...document.querySelectorAll('input:not([type=hidden]):not([type=checkbox]):not([type=radio]):not([type=file]), select, textarea')]
            .filter(vis);
        const out = {
            jumlahField: fields.length,
            fontTerkecil: Math.min(...fields.map(px)),
            tinggiTerkecil: Math.min(...fields.map(h)),
            fieldDiBawah16: fields.filter(el => px(el) < 16)
                .map(el => (el.id || el.name || el.tagName) + '@' + px(el) + 'px')
        };

        const boxes = [...document.querySelectorAll('input[type=checkbox]')].filter(vis);
        out.kotakCentang = boxes.length ? Math.min(...boxes.map(h)) : null;

        ids.forEach(i => { const el = document.getElementById(i); if (el) el.classList.remove('open'); });

        // Tombol di halaman kerusakan — tempat aksi utamanya ada.
        navigateTo('damage');
        return out;
    })()`);

    t('setiap field form minimal 16px (iPhone tidak nge-zoom)', m.fieldDiBawah16, []);
    t('ada banyak field yang diperiksa, bukan satu-dua', m.jumlahField >= 40, true);
    t('field paling pendek pun bisa disentuh', m.tinggiTerkecil >= 40, true);
    t('kotak centang cukup besar', m.kotakCentang >= 20, true);

    // navigateTo memanggil load*() yang menimpa data dari localStorage, jadi
    // pasang datanya SESUDAH pindah halaman lalu render ulang.
    const btns = await phone.evaluate(`
        navigateTo('damage');
        ${DATA};
        populateDamageUnitSelect(); renderDamageTable();
        (() => {
            const h = el => Math.round(el.getBoundingClientRect().height);
            const w = el => Math.round(el.getBoundingClientRect().width);
            const vis = el => { const r = el.getBoundingClientRect(); return r.width > 0 && r.height > 0; };
            const all = [...document.querySelectorAll('button')].filter(vis);
            const kecil = all.filter(el => h(el) < 36 || w(el) < 36)
                .map(el => (el.id || el.className || 'button') + '@' + w(el) + 'x' + h(el));
            const burger = document.querySelector('.topbar__burger');
            const badges = [...document.querySelectorAll('.badge')].filter(vis);
            return { jumlahTombol: all.length, kecil,
                     burger: w(burger) + 'x' + h(burger),
                     badgeTerkecil: badges.length
                        ? Math.min(...badges.map(el => parseFloat(getComputedStyle(el).fontSize)))
                        : null };
        })()`);

    t('tidak ada tombol di bawah 36px', btns.kecil, []);
    t('tombol menu punya area sentuh penuh', btns.burger, '44x44');
    t('badge terbaca, bukan 10px', btns.badgeTerkecil >= 12, true);

    // Tabel kosong dulu memaksa halaman menggeser ke samping, karena pesannya
    // panjang dan selnya nowrap — kena saat instalasi baru atau filter nihil.
    const kosong = await phone.evaluate(`(() => {
        globalData = []; filteredData = [];
        navigateTo('editUnits'); renderEditTable();
        return { geser: document.documentElement.scrollWidth > innerWidth,
                 bungkus: getComputedStyle(
                     document.querySelector('#editTable td[colspan]')).whiteSpace };
    })()`);
    t('tabel kosong tidak memaksa halaman menggeser', kosong.geser, false);
    t('pesan tabel kosong boleh membungkus', kosong.bungkus, 'normal');

    // Sel tanpa data-label itu display:none di mode kartu — jadi kolom yang
    // lupa dilabeli tidak terlihat rusak, ia cuma HILANG di ponsel. Selama ini
    // 15 dari 21 kolom Edit Units hilang dengan cara itu.
    const label = await phone.evaluate(`(() => {
        // navigateTo DULU: ia memanggil load*() yang menimpa global dari
        // localStorage, jadi menyemai sebelum berpindah tidak ada gunanya.
        navigateTo('editUnits');
        globalData = [{ id:'u_1', name:'A', model:'M', sn:'S1', implement:'Imp',
            status:'Good', display:'Good', gps:'Good', steering:'Good', jdlink:'Good',
            site:'PT. GPA', yearReceived:'2021', userCategory:'Operator',
            gpsLicense:'SF-RTK', licenseDisplay:'G5 Advance', remarks:'catatan' }];
        filteredData = [...globalData];
        renderEditTable();
        const tds = [...document.querySelectorAll('#editBody tr td')];
        const tanpaLabel = tds.filter(td =>
            !td.hasAttribute('data-label') && !td.classList.contains('col-actions'));
        const terlihat = n => {
            const td = document.querySelector(\`#editBody td[data-label="\${n}"]\`);
            return !!td && td.offsetParent !== null;
        };
        return {
            tanpaLabel: tanpaLabel.map(td => td.className || '(polos)'),
            gps: terlihat('GPS'),
            lisensi: terlihat('GPS Expiry'),
            remarks: terlihat('Remarks'),
            tahun: terlihat('Tahun Penerimaan')
        };
    })()`);
    // Dua yang sengaja tanpa label: nomor baris dan kotak centang.
    t('hanya dua sel yang sengaja tanpa label', label.tanpaLabel.length, 2);
    t('kolom GPS terlihat di ponsel', label.gps, true);
    t('kolom masa berlaku lisensi terlihat di ponsel', label.lisensi, true);
    t('kolom Remarks terlihat di ponsel', label.remarks, true);
    t('kolom Tahun Penerimaan terlihat di ponsel', label.tahun, true);

    // Filter yang tidak cocok dulu menghasilkan tabel kosong tanpa satu kata
    // penjelasan — di ponsel itu berarti layar kosong tanpa sebab.
    const nihil = await phone.evaluate(`(() => {
        navigateTo('dashboard');
        globalData = [{ id:'u_1', name:'A', model:'M', sn:'S1', status:'Good', site:'X' }];
        document.getElementById('searchInput').value = 'tidakada';
        applyFilter();
        const pesan = document.querySelector('#detailBody td[colspan]');
        return { ada: !!pesan, teks: pesan ? pesan.textContent.replace(/\\s+/g, ' ').trim() : '',
                 adaTombol: !!(pesan && pesan.querySelector('button')) };
    })()`);
    t('filter nihil memberi penjelasan, bukan tabel kosong', nihil.ada, true);
    t('penjelasannya menyebut filter', /filter/i.test(nihil.teks), true);
    t('dan menyediakan jalan keluar', nihil.adaTombol, true);

    // Jadwal shift: satu-satunya yang memang menggeser, jadi nama harus menempel.
    const shift = await phone.evaluate(`(async () => {
        teamMembers = [{ id:'m1', name:'Yulianus Mop Anggawen', jobTitle:'Mekanik',
                         company:'PT. GPA', active:true }];
        teamShifts = [];
        navigateTo('team'); switchTeamTab('shift');
        teamWeekStart = startOfWeekISO('2026-09-14');
        renderShiftGrid();
        const wrap = document.querySelector('.shift-grid-wrapper');
        const cell = document.querySelector('#shiftBody .shift-grid__member');
        const before = Math.round(cell.getBoundingClientRect().left);
        wrap.scrollLeft = wrap.scrollWidth;
        await new Promise(r => requestAnimationFrame(r));
        const after = Math.round(cell.getBoundingClientRect().left);
        const sel = document.querySelector('.shift-select');
        return { posisi: getComputedStyle(cell).position,
                 benarMenggeser: wrap.scrollLeft > 0,
                 namaTetapDiTempat: Math.abs(after - before) <= 1,
                 tinggiSelect: sel ? Math.round(sel.getBoundingClientRect().height) : null,
                 halamanTidakGeser: document.documentElement.scrollWidth <= innerWidth };
    })()`);
    t('kolom nama dipatok', shift.posisi, 'sticky');
    t('grid memang menggeser ke samping', shift.benarMenggeser, true);
    t('nama tetap terlihat saat digeser', shift.namaTetapDiTempat, true);
    t('pilihan shift bisa disentuh', shift.tinggiSelect >= 36, true);
    t('halamannya sendiri tidak ikut menggeser', shift.halamanTidakGeser, true);

    await phone.close();

    // ---------------- desktop 1440px tidak boleh ikut berubah ----------------
    // Aturan ponsel ada di @media (max-width: 700px); kalau bocor ke desktop,
    // tabel yang padat jadi renggang dan lebih sedikit baris yang muat.
    const desk = await b.newPage({ viewport:{ width:1440, height:900 } });
    desk.on('pageerror', e => { if (!/Chart is not defined/.test(e.message)) errors.push(e.message); });
    const d = await run(desk, 1440, `(() => {
        const el = document.getElementById('unitModal');
        el.classList.add('open');
        const inp = document.getElementById('formName');
        const fs = parseFloat(getComputedStyle(inp).fontSize);
        const hh = Math.round(inp.getBoundingClientRect().height);
        el.classList.remove('open');
        const probe = document.createElement('button');
        probe.className = 'btn btn-secondary btn-sm';
        probe.textContent = 'Uji';
        document.body.appendChild(probe);
        const bh = Math.round(probe.getBoundingClientRect().height);
        probe.remove();
        return { fontInput: fs, tinggiInput: hh, tinggiBtnSm: bh };
    })()`);

    t('desktop: font input tetap 13px', d.fontInput, 13);
    t('desktop: input tetap ramping', d.tinggiInput < 40, true);
    t('desktop: btn-sm tetap ramping', d.tinggiBtnSm < 40, true);
    await desk.close();

    const fail = T.filter(r => !r.pass);
    T.forEach(r => console.log(`${r.pass ? 'PASS' : 'FAIL'}  ${r.n}${r.pass ? '' : `\n        dapat=${JSON.stringify(r.g)} harap=${JSON.stringify(r.w)}`}`));
    console.log(`\n${T.length - fail.length}/${T.length} lulus`);
    console.log('PAGE ERRORS:', errors.length ? errors : 'none');
    await b.close();
    process.exit(fail.length ? 1 : 0);
})();
