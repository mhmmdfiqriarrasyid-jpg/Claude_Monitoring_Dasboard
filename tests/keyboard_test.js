// Keyboard: Escape, fokus, dan header tabel.
//
// Lima belas dari delapan belas modal tidak bisa ditutup dengan Escape,
// padahal fungsi close-nya sudah ada untuk hampir semuanya — jadi satu-satunya
// jalan keluar adalah tetikus. Dan 34 header yang bisa diurut punya onclick
// tanpa tabindex, jadi sama sekali tidak terjangkau keyboard.
//
// Yang diuji bukan "apakah tombolnya bereaksi", melainkan dua hal yang mudah
// salah: Escape hanya menutup modal PALING ATAS (menutup semuanya sekaligus
// akan membuang dialog di belakang yang sedang dibaca), dan fokus kembali ke
// tempat asalnya, bukan ke awal dokumen.
const { launch, BASE_URL } = require('./_env');

(async () => {
    const b = await launch();
    const page = await b.newPage({ viewport: { width: 1440, height: 900 } });
    const errors = [];
    page.on('pageerror', e => { if (!/Chart is not defined/.test(e.message)) errors.push(e.message); });
    await page.goto(BASE_URL + '/index.html', { waitUntil: 'load' });
    await page.waitForTimeout(1000);

    await page.addScriptTag({ content: `(() => { try {
        const T = []; const t = (n, g, w) => T.push({ n, g, w, pass: JSON.stringify(g) === JSON.stringify(w) });

        window.cloud = { isReady: true, saveUnits: () => Promise.resolve(),
            getAllUnits: () => Promise.resolve([]), addHistoryEvents: () => Promise.resolve(),
            subscribeUsers: () => () => {} };
        currentUser = { uid: 'uO', email: 'o@x.id' };
        currentUserDoc = { role: 'owner', status: 'active' };
        hideAuthGates(); applyRoleGating();

        // ---------- setiap modal punya penutup terdaftar ----------
        // Kalau ada modal baru yang lupa didaftarkan, ini yang menangkapnya.
        const semuaModal = [...document.querySelectorAll('.modal-overlay[id]')]
            .map(el => el.id)
            .filter(id => id !== 'photoLightbox');   // punya penangan sendiri
        const belumTerdaftar = semuaModal.filter(id => !MODAL_CLOSERS[id]);
        t('semua modal punya penutup terdaftar', belumTerdaftar, []);
        t('dan penutupnya benar-benar ada sebagai fungsi',
          semuaModal.filter(id => typeof window[MODAL_CLOSERS[id]] !== 'function'), []);
        t('jumlah modalnya memang banyak, bukan satu-dua', semuaModal.length >= 18, true);

        // ---------- Escape menutup yang paling atas saja ----------
        const a = document.getElementById('deviceModal');
        const c = document.getElementById('stockModal');
        a.classList.add('open'); c.classList.add('open');
        const ditutup = closeTopModal();
        t('Escape menutup sesuatu', ditutup, true);
        t('yang ditutup adalah yang paling atas', c.classList.contains('open'), false);
        t('yang di bawahnya dibiarkan', a.classList.contains('open'), true);
        closeTopModal();
        t('tekan lagi menutup yang berikutnya', a.classList.contains('open'), false);
        t('tanpa modal terbuka, tidak terjadi apa-apa', closeTopModal(), false);

        // ---------- mengetik tetap kebal ----------
        // Escape adalah refleks untuk menutup saran datalist. Kalau ia menutup
        // form unit, semua yang sudah diketik hilang tanpa konfirmasi.
        document.getElementById('unitModal').classList.add('open');
        const field = document.getElementById('formName') || document.querySelector('#unitModal input');
        field.focus();
        // Dikirim DARI fieldnya, bukan dari document: itulah yang terjadi saat
        // orang sungguhan menekan Escape sambil mengetik.
        field.dispatchEvent(new KeyboardEvent('keydown', { key: 'Escape', bubbles: true }));
        t('Escape saat mengetik tidak menutup form',
          document.getElementById('unitModal').classList.contains('open'), true);
        closeModal();

        // ---------- fokus kembali ke pemicunya ----------
        const pemicu = document.createElement('button');
        pemicu.id = 'pemicuUji';
        document.body.appendChild(pemicu);
        pemicu.focus();
        rememberFocus();
        document.getElementById('deviceModal').classList.add('open');
        (document.querySelector('#deviceModal input') || document.body).focus();
        closeTopModal();
        restoreFocus();
        t('fokus kembali ke tombol yang membuka modal', document.activeElement.id, 'pemicuUji');
        pemicu.remove();

        // Elemen pemicu bisa saja sudah hilang saat modal ditutup (baris tabel
        // yang dirender ulang). Itu tidak boleh melempar.
        const sementara = document.createElement('button');
        document.body.appendChild(sementara);
        sementara.focus(); rememberFocus();
        sementara.remove();
        let aman = true;
        try { restoreFocus(); } catch (e) { aman = false; }
        t('pemicu yang sudah hilang tidak bikin error', aman, true);

        // ---------- header tabel terjangkau keyboard ----------
        const headerUrut = [...document.querySelectorAll('th[onclick*="sortTable"], th[onclick*="sortEditTable"]')];
        t('semua header yang bisa diurut punya tabindex',
          headerUrut.filter(th => th.getAttribute('tabindex') !== '0').length, 0);
        t('dan punya role tombol',
          headerUrut.filter(th => th.getAttribute('role') !== 'button').length, 0);
        t('jumlahnya 34 seperti yang dihitung', headerUrut.length, 34);

        // Enter harus benar-benar mengurutkan, bukan sekadar bisa difokus.
        globalData = [
            { id: 'u1', name: 'Zeta', model: 'M', sn: 'S2', status: 'Good' },
            { id: 'u2', name: 'Alfa', model: 'M', sn: 'S1', status: 'Good' }
        ];
        filteredData = [...globalData];
        navigateTo('dashboard');
        globalData = [
            { id: 'u1', name: 'Zeta', model: 'M', sn: 'S2', status: 'Good' },
            { id: 'u2', name: 'Alfa', model: 'M', sn: 'S1', status: 'Good' }
        ];
        filteredData = [...globalData];
        renderTable(filteredData);
        const thNama = document.querySelector('#detailTable th[onclick*="\\'name\\'"]');
        thNama.focus();
        thNama.dispatchEvent(new KeyboardEvent('keydown', { key: 'Enter', bubbles: true }));
        t('Enter di header benar-benar mengurutkan',
          [...document.querySelectorAll('#detailBody tr td:nth-child(2)')].map(td => td.textContent.trim()),
          ['Alfa', 'Zeta']);
        t('kolom yang diurut ditandai untuk pembaca layar',
          thNama.getAttribute('aria-sort'), 'ascending');
        t('dan ikonnya menyala — selama ini kelas .sorted tidak pernah dipasang',
          thNama.classList.contains('sorted'), true);
        thNama.dispatchEvent(new KeyboardEvent('keydown', { key: 'Enter', bubbles: true }));
        t('menekan lagi membalik arahnya', thNama.getAttribute('aria-sort'), 'descending');
        const thLain = document.querySelector('#detailTable th[onclick*="\\'model\\'"]');
        thLain.dispatchEvent(new KeyboardEvent('keydown', { key: 'Enter', bubbles: true }));
        t('hanya satu kolom yang ditandai sekaligus',
          [...document.querySelectorAll('#detailTable th[aria-sort]')].length, 1);

        window.__T = T;
    } catch (e) { window.__T = [{n:'THREW: '+e.message+' | '+(e.stack||'').split('\\n')[1], g:1, w:0, pass:false}]; }
    })();` });

    await page.waitForFunction(() => window.__T, null, { timeout: 25000 });
    const res = await page.evaluate(() => window.__T);
    const fail = res.filter(r => !r.pass);
    res.forEach(r => console.log(`${r.pass ? 'PASS' : 'FAIL'}  ${r.n}${r.pass ? '' : `\n        dapat=${JSON.stringify(r.g)} harap=${JSON.stringify(r.w)}`}`));
    console.log(`\n${res.length - fail.length}/${res.length} lulus`);
    console.log('PAGE ERRORS:', errors.length ? errors : 'none');
    await b.close();
    process.exit(fail.length ? 1 : 0);
})();
