// "Password sudah benar tapi tidak bisa masuk": jam sesi harian di-reset
// SEBELUM sign-in (cap waktu basi dari sesi kedaluwarsa dulu langsung
// mengeluarkan orang lagi), tombol mata untuk melihat password, petunjuk
// spesifik (spasi di ujung, huruf kapital otomatis, Caps Lock), lupa password,
// dan pesan untuk akun terkunci / dinonaktifkan.
const { launch, BASE_URL } = require('./_env');

(async () => {
    const b = await launch();
    const page = await b.newPage({ viewport: { width: 390, height: 844 } });
    const errors = [];
    page.on('pageerror', e => { if (!/Chart is not defined/.test(e.message)) errors.push(e.message); });
    await page.goto(BASE_URL + '/index.html', { waitUntil: 'load' });
    await page.waitForTimeout(1000);

    await page.addScriptTag({ content: `(async () => { try {
        const T = []; const t = (n, g, w) => T.push({ n, g, w, pass: JSON.stringify(g) === JSON.stringify(w) });
        const tick = ms => new Promise(r => setTimeout(r, ms || 20));
        let stampDuringSignIn = null, resetFor = null, failWith = null, resetFail = null;
        window.cloud = Object.assign(window.cloud || {}, {
            signIn: async () => {
                stampDuringSignIn = Number(localStorage.getItem('tractorSessionStart'));
                if (failWith) throw Object.assign(new Error('x'), { code: failWith });
                return { uid: 'u' };
            },
            sendPasswordReset: async email => { if (resetFail) throw Object.assign(new Error('x'), { code: resetFail }); resetFor = email; },
            signOutUser: async () => {}
        });
        showAuthGate('signin');
        const err = () => document.getElementById('signInError').textContent;
        const notice = () => { const n = document.getElementById('signInNotice'); return n.style.display === 'none' ? '' : n.textContent; };
        const submit = async (email, pw) => {
            document.getElementById('signInEmail').value = email;
            document.getElementById('signInPassword').value = pw;
            await handleSignIn(new Event('submit'));
        };

        // =============== cap waktu sesi basi ===============
        localStorage.setItem('tractorSessionStart', String(Date.now() - 3 * 24 * 3600 * 1000));
        failWith = null;
        const before = Date.now();
        await submit('tim@x.id', 'Rahasia123');
        t('jam sesi sudah baru saat Firebase memanggil listener', stampDuringSignIn >= before, true);
        failWith = 'auth/invalid-credential';
        await submit('tim@x.id', 'salah');
        t('gagal masuk: cap waktu dibersihkan', localStorage.getItem('tractorSessionStart'), null);

        // =============== petunjuk password ===============
        await submit('tim@x.id', 'Contoh456 ');
        t('spasi di ujung disebut', /starts or ends with a space/.test(err()), true);
        await submit('tim@x.id', 'Rahasia');
        t('huruf kapital pertama dari HP disebut', /only the first letter is a capital/.test(err()), true);
        await submit('tim@x.id', 'RAHASIA123');
        t('Caps Lock disebut', /Caps Lock/.test(err()), true);
        await submit('tim@x.id', 'rahasia123');
        t('tanpa pola khusus: arahkan ke mata & lupa password', /eye icon.*Forgot password/.test(err()), true);
        failWith = 'auth/too-many-requests';
        await submit('tim@x.id', 'rahasia123');
        t('akun terkunci sementara dijelaskan', /locked for a while/.test(err()), true);
        t('tanpa petunjuk password untuk akun terkunci', /eye icon/.test(err()), false);
        failWith = 'auth/user-disabled';
        await submit('tim@x.id', 'rahasia123');
        t('akun dinonaktifkan dijelaskan', /disabled/.test(err()), true);

        // =============== tombol mata ===============
        const pw = document.getElementById('signInPassword');
        const eye = document.querySelector('#signInForm .pw-toggle');
        eye.click();
        t('mata: password terlihat', [pw.type, eye.getAttribute('aria-pressed'), eye.getAttribute('aria-label')], ['text', 'true', 'Hide password']);
        eye.click();
        t('mata: disembunyikan lagi', pw.type, 'password');
        t('HP tidak mengubah huruf otomatis', [pw.getAttribute('autocapitalize'), document.getElementById('signInEmail').getAttribute('autocapitalize')], ['off', 'off']);

        // =============== lupa password ===============
        document.getElementById('signInEmail').value = '';
        await handleForgotPassword();
        t('lupa password tanpa email: minta isi email', /Type your email above first/.test(err()), true);
        document.getElementById('signInEmail').value = ' tim@x.id ';
        await handleForgotPassword();
        t('lupa password: tautan dikirim ke email itu', [resetFor, /a password-reset link has been sent/.test(notice())], ['tim@x.id', true]);
        resetFail = 'auth/user-not-found'; resetFor = null;
        await handleForgotPassword();
        t('email tak terdaftar: pesan sama (tidak membocorkan akun)', [resetFor, /If an account exists/.test(notice())], [null, true]);
        resetFail = 'auth/network-request-failed';
        await handleForgotPassword();
        t('gangguan jaringan dilaporkan', /Network error/.test(err()), true);

        window.__T = T;
    } catch (e) { window.__T = [{ n: 'EXCEPTION ' + e.message + ' ' + e.stack, pass: false }]; } })();` });

    await page.waitForFunction(() => window.__T, null, { timeout: 60000 });
    const T = await page.evaluate(() => window.__T);
    let pass = 0;
    T.forEach(x => { if (x.pass) pass++; else console.log('FAIL', x.n, '\n  dapat :', JSON.stringify(x.g), '\n  harap :', JSON.stringify(x.w)); });
    if (errors.length) console.log('PAGE ERRORS:', errors);
    console.log(`${pass}/${T.length} lulus`);
    await b.close();
    process.exit(pass === T.length && !errors.length ? 0 : 1);
})();
