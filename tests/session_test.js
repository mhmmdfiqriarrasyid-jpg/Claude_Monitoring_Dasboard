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

        // ---------- id sesi ----------
        try { localStorage.removeItem(SESSION_ID_KEY); } catch(e) {}
        _mySessionId = '';
        const first = mySessionId();
        t('id sesi dibuat', /^s_\\d+_[a-z0-9]+$/.test(first), true);
        t('id sesi disimpan di localStorage', localStorage.getItem(SESSION_ID_KEY), first);
        _mySessionId = '';
        t('tab lain di perangkat sama memakai id yang SAMA', mySessionId(), first);
        t('label perangkat terbaca', /·/.test(deviceLabel()), true);

        // ---------- klaim sesi ----------
        const writes = [];
        let snapCb = null;
        window.cloud = {
            isReady: true,
            claimSession: (uid, s) => { writes.push(['claim', uid, s.id, s.device]); return Promise.resolve(); },
            revokeUserSession: (uid, by) => { writes.push(['revoke', uid, by]); return Promise.resolve(); },
            subscribeUserDoc: (uid, cb) => { snapCb = cb; return () => { snapCb = null; }; },
            signOutUser: () => { writes.push(['signOut']); return Promise.resolve(); }
        };
        currentUser = { uid:'u1', email:'andi@x.id' };
        currentUserDoc = { uid:'u1', email:'andi@x.id', role:'staff', status:'active', access:{ teamLog:'edit' } };
        hideAuthGates();

        claimActiveSession('u1');
        t('klaim mengirim id milik sendiri', writes[0][2], first);
        t('klaim menyertakan label perangkat', writes[0][3], deviceLabel());

        // ---------- pantau dokumen sendiri ----------
        _userDocUnsub = null; _sessionTakenOver = false;
        watchOwnUserDoc('u1');
        t('berlangganan dokumen sendiri', typeof snapCb, 'function');

        // Snapshot dengan id KITA SENDIRI: bukan pengambilalihan.
        const authGate = document.getElementById('authGate');
        snapCb({ ...currentUserDoc, activeSession: { id: first, device: 'Chrome · Android' } });
        t('id sendiri tidak menendang', authGate.style.display, 'none');
        t('belum ada signOut', writes.some(w => w[0]==='signOut'), false);

        // Perubahan akses dari admin diterapkan langsung tanpa reload.
        snapCb({ ...currentUserDoc, access:{ teamLog:'view' }, activeSession:{ id: first } });
        t('akses baru langsung dipakai', effectiveAccess('teamLog'), 'view');

        // Status dicabut -> layar menunggu.
        snapCb({ ...currentUserDoc, status:'pending', activeSession:{ id: first } });
        t('status dicabut -> layar pending',
          document.getElementById('pendingGate').style.display, 'flex');
        hideAuthGates();
        currentUserDoc = { uid:'u1', email:'andi@x.id', role:'staff', status:'active', access:{ teamLog:'edit' } };

        window.__T = T; window.__snap = () => snapCb; window.__writes = writes; window.__first = first;
    } catch (e) { window.__T = [{n:'THREW: '+e.message+' | '+(e.stack||'').split('\\n')[1], g:1, w:0, pass:false}]; }
    })();` });

    // Takeover is async (signOut awaited). Driven from an injected script so it
    // can read currentUser — a top-level `let`, invisible to page.evaluate.
    await page.addScriptTag({ content: `(() => {
        window.__snap()({ uid:'u1', email:'andi@x.id', role:'staff', status:'active',
             activeSession: { id: 's_OTHER_DEVICE', device: 'Safari · iOS' } });
        setTimeout(() => {
            window.__after = {
                gate: document.getElementById('authGate').style.display,
                err: document.getElementById('signInError').textContent,
                signedOut: window.__writes.some(w => w[0] === 'signOut'),
                currentUser: currentUser === null ? null : 'masih ada',
                currentUserDoc: currentUserDoc === null ? null : 'masih ada',
                watcherStopped: window.__snap() === null,
                flagReset: _sessionTakenOver === false
            };
        }, 300);
    })();` });
    await page.waitForTimeout(700);
    const after = await page.evaluate(() => window.__after);

    // Owner remote sign-out path.
    const revoke = await page.evaluate(async () => {
        const out = {};
        currentUser = { uid:'owner1', email:'boss@x.id' };
        currentUserDoc = { uid:'owner1', email:'boss@x.id', role:'owner', status:'active' };
        allUsers = [
            { uid:'u2', email:'budi@x.id', displayName:'Budi', role:'staff', status:'active',
              activeSession:{ id:'s_abc', device:'Chrome · Android', startedAt: Date.now() } },
            { uid:'owner1', email:'boss@x.id', displayName:'Boss', role:'owner', status:'active' }
        ];
        window.confirm = () => true;
        await forceSignOutUser('u2');
        out.revoked = window.__writes.filter(w => w[0]==='revoke');
        // Revoking yourself must be refused.
        const before = window.__writes.length;
        await forceSignOutUser('owner1');
        out.selfRefused = window.__writes.length === before;
        return out;
    });

    const res = await page.evaluate(() => window.__T);
    const extra = [
        { n: 'pengambilalihan: gerbang login tampil', g: after.gate, w: 'flex' },
        { n: 'pengambilalihan: pesan menyebut perangkat lain', g: /perangkat lain/i.test(after.err), w: true },
        { n: 'pengambilalihan: pesan menyebut perangkatnya', g: /Safari · iOS/.test(after.err), w: true },
        { n: 'pengambilalihan: benar-benar signOut', g: after.signedOut, w: true },
        { n: 'pengambilalihan: currentUser dibersihkan', g: after.currentUser, w: null },
        { n: 'pengambilalihan: currentUserDoc dibersihkan', g: after.currentUserDoc, w: null },
        { n: 'pengambilalihan: pantauan dilepas', g: after.watcherStopped, w: true },
        { n: 'pengambilalihan: penanda direset agar bisa login lagi', g: after.flagReset, w: true },
        { n: 'owner bisa mengeluarkan user lain', g: revoke.revoked.length, w: 1 },
        { n: 'revoke mencatat siapa yang melakukan', g: revoke.revoked[0] && revoke.revoked[0][2], w: 'boss@x.id' },
        { n: 'owner tidak bisa mengeluarkan dirinya sendiri', g: revoke.selfRefused, w: true }
    ].map(x => ({ ...x, pass: JSON.stringify(x.g) === JSON.stringify(x.w) }));

    const all = res.concat(extra);
    const fail = all.filter(r => !r.pass);
    all.forEach(r => console.log(`${r.pass ? 'PASS' : 'FAIL'}  ${r.n}${r.pass ? '' : `\n        dapat=${JSON.stringify(r.g)} harap=${JSON.stringify(r.w)}`}`));
    console.log(`\n${all.length - fail.length}/${all.length} lulus`);
    console.log('PAGE ERRORS:', errors.length ? errors : 'none');
    await b.close();
    process.exit(fail.length ? 1 : 0);
})();
