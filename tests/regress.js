const { launch, BASE_URL } = require('./_env');
(async () => {
    const b = await launch();
    const page = await b.newPage({ viewport: { width: 1440, height: 900 } });
    const errs = [];
    page.on('pageerror', e => errs.push(e.message.split('\n')[0]));
    await page.goto(BASE_URL + '/index.html', { waitUntil: 'load' });
    await page.waitForTimeout(900);
    await page.addScriptTag({ content: `(() => {
      const T=[]; const t=(n,g,w)=>T.push({n,g,w,pass:JSON.stringify(g)===JSON.stringify(w)});
      currentUserDoc={role:'owner',status:'active',email:'o@x.id'};
      currentUser={uid:'u1',email:'o@x.id'};
      window.cloud={isReady:true};
      hideAuthGates(); applyRoleGating();
      globalData=[
        {id:'u_1',name:'A-01',sn:'S1',site:'Cikarang',status:'Good',gps:'Good',display:'Good',steering:'Good',jdlink:'Good'},
        {id:'u_2',name:'A-02',sn:'S2',site:'Karawang',status:'Breakdown',gps:'Breakdown',display:'Good',steering:'Good',jdlink:'Good'}
      ];
      filteredData=[...globalData];
      teamMembers=[]; teamShifts=[]; workLogs=[];

      t('Chart.js memang tidak ada di sandbox', typeof window.Chart, 'undefined');

      let threw=null;
      try { onDataLoaded(); } catch(e) { threw = e.message; }
      t('onDataLoaded tidak melempar error', threw, null);

      // The repair table sits AFTER two chart calls inside renderDamageStats —
      // it is exactly what an unguarded throw used to skip.
      const repair = document.getElementById('repairBody');
      t('tabel perbaikan tetap terisi', repair && repair.children.length > 0, true);
      t('KPI total terisi', document.getElementById('kpiTotal').textContent, '2');
      t('placeholder grafik muncul', document.querySelectorAll('.chart-unavailable').length > 0, true);

      // Access modal must list the new Tim area automatically.
      t("ACCESS_AREAS memuat 'teamLog'", ACCESS_AREAS.some(a=>a.key==='teamLog'), true);
      t("GATED_VIEWS memuat view 'team'", GATED_VIEWS.includes('team'), true);
      t('DATA_AREAS tanpa history', DATA_AREAS.includes('history'), false);
      t('DATA_AREAS memuat tiga area Tim',
        ['teamShift','teamLog','teamMembers'].every(a=>DATA_AREAS.includes(a)), true);

      // Per-area gating: no team access hides the nav link.
      currentUserDoc={role:'viewer',status:'active',access:{teamShift:'none',teamLog:'none',teamMembers:'none'}};
      applyAccessVisibility();
      t('tanpa akses: menu Tim disembunyikan',
        document.querySelector('.nav__link[data-view="team"]').style.display, 'none');
      currentUserDoc={role:'viewer',status:'active',access:{teamShift:'view',teamLog:'view',teamMembers:'view'}};
      applyAccessVisibility();
      t('akses lihat: menu Tim tampil',
        document.querySelector('.nav__link[data-view="team"]').style.display, '');
      t('akses lihat: body ditandai read-only', document.body.dataset.roTeamshift, '1');
      currentUserDoc={role:'team',status:'active'};
      applyAccessVisibility();
      t('role lama team: bisa edit', document.body.dataset.roTeamshift, undefined);
      t('canEditAnyArea utk team-only grant',
        (()=>{ currentUserDoc={role:'viewer',status:'active',access:{teamLog:'edit'}}; return canEditAnyArea(); })(), true);
      window.__T=T;
    })();` });
    const res = await page.evaluate(() => window.__T);
    res.forEach(r => console.log(`${r.pass?'PASS':'FAIL'}  ${r.n}${r.pass?'':`\n        dapat=${JSON.stringify(r.g)} harap=${JSON.stringify(r.w)}`}`));
    console.log(`\n${res.filter(r=>r.pass).length}/${res.length} lulus`);
    console.log('PAGE ERRORS:', errs.length?errs:'none');
    await b.close();
})();
