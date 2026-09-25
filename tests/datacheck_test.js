// Halaman Periksa Data — menemukan data yang sudah terlanjur kotor.
//
// Tiap pemeriksaan diuji dua arah: menemukan kasus yang memang salah, DAN
// tidak menemukan apa-apa pada data yang bersih. Yang kedua sama pentingnya —
// alat pemeriksa yang berteriak pada data sehat akan diabaikan, dan sekali
// diabaikan ia tidak berguna lagi.
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
        let asked = [], answer = true;
        window.confirm = m => { asked.push(String(m)); return answer; };
        const unitWrites = [];

        window.cloud = { isReady: true,
            saveUnits: u => { unitWrites.push(...u.map(x => x.id)); return Promise.resolve(); },
            deleteUnits: () => Promise.resolve(), getAllUnits: () => Promise.resolve([]),
            addHistoryEvents: () => Promise.resolve(),
            subscribeUsers: () => () => {} };

        currentUser = { uid:'uO', email:'o@x.id' };
        currentUserDoc = { role:'owner', status:'active', email:'o@x.id' };
        hideAuthGates(); applyRoleGating();

        const U = (id, o) => Object.assign({
            id, name:'GGTR'+id, sn:'SN-'+id, site:'PT. GPA', status:'Good',
            gps:'Good', steering:'Good', jdlink:'Good', display:'Good'
        }, o);

        // ---------- data bersih tidak boleh memicu apa pun ----------
        const clean = () => {
            globalData = [U('A'), U('B')];
            globalDamages = []; globalLicenseStock = []; stockLedger = [];
            workLogs = []; teamMembers = [{ id:'m1', name:'Andi', company:'PT. GPA', active:true }];
            warehouseDevices = [];
        };
        clean();
        t('data bersih: nol temuan',
          runDataChecks().reduce((n, g) => n + g.items.length, 0), 0);

        // ---------- nomor seri duplikat ----------
        clean();
        globalData.push(U('C', { sn:'SN-A' }));
        t('SN kembar ketahuan', dcDuplicateSerials().length, 1);
        t('disebut nomor serinya', /SN-A/.test(dcDuplicateSerials()[0].label), true);
        clean();
        globalData.push(U('C', { sn:'' }), U('D', { sn:'' }));
        t('SN kosong bukan duplikat', dcDuplicateSerials().length, 0);
        clean();
        warehouseDevices = [{ id:'d1', sn:'DV-1', type:'GPS' }, { id:'d2', sn:'dv-1', type:'GPS' }];
        t('perangkat gudang ikut diperiksa, beda huruf tetap kembar',
          dcDuplicateSerials().length, 1);

        // ---------- karakter tak terlihat ----------
        clean();
        globalData = [U('A', { name:'MGTR063M_HBM\\u200B' }), U('B', { site:'PT. GPA ' }), U('C')];
        const inv = dcInvisibleCharacters();
        t('zero-width dan spasi ujung ketahuan', inv.length, 2);
        t('menyebut field mana', inv.map(i => i.field).sort(), ['name','site']);
        t('ditandai bisa diperbaiki otomatis', inv.every(i => i.fixable), true);

        // perbaikan otomatis: membersihkan tanpa mengubah tulisannya
        answer = true; asked = [];
        fixInvisibleCharacters();
        t('nama jadi bersih', globalData[0].name, 'MGTR063M_HBM');
        t('site jadi bersih', globalData[1].site, 'PT. GPA');
        t('unit yang sudah bersih tidak ikut ditulis', unitWrites.includes('C'), false);
        t('setelah dirapikan tidak ada temuan lagi', dcInvisibleCharacters().length, 0);
        // Konfirmasi wajib — ini menulis ke data.
        asked = []; answer = false;
        globalData = [U('A', { name:'X\\u200B' })];
        fixInvisibleCharacters();
        t('menolak konfirmasi berarti tidak menulis', globalData[0].name, 'X\\u200B');

        // ---------- nama site beda tipis ----------
        clean();
        globalData = [U('A', { site:'PT. GPA' }), U('B', { site:'PT GPA' }), U('C', { site:'PT. MNM' })];
        const sv = dcSiteVariants();
        t('ejaan beda tipis dikelompokkan jadi satu temuan', sv.length, 1);
        t('kedua ejaannya disebut',
          /"PT\\. GPA"/.test(sv[0].detail) && /"PT GPA"/.test(sv[0].detail), true);
        clean();
        globalData = [U('A', { site:'PT. GPA' }), U('B', { site:'PT. MNM' })];
        t('dua PT yang memang beda tidak dianggap salah ketik', dcSiteVariants().length, 0);

        // ---------- perusahaan / site kosong ----------
        clean();
        teamMembers = [
            { id:'m1', name:'Andi', company:'PT. GPA', active:true },
            { id:'m2', name:'Budi', company:'', active:true },
            { id:'m3', name:'Cito', company:'', active:false }   // nonaktif — abaikan
        ];
        globalData = [U('A'), U('B', { site:'' })];
        const mc = dcMissingCompany();
        t('anggota aktif tanpa perusahaan + unit tanpa site', mc.length, 2);
        t('anggota nonaktif tidak diributkan',
          mc.some(i => /Cito/.test(i.label)), false);

        // ---------- jam laporan ----------
        clean();
        const W = (id, o) => Object.assign({ id, date:'2026-09-01', memberId:'m1',
            memberName:'Andi', start:'07:00', end:'16:00', task:'x' }, o);
        workLogs = [
            W('w1'),                                  // wajar
            W('w2', { start:'08:00', end:'07:00' }),  // terbalik → 23 jam
            W('w3', { start:'08:00', end:'08:00' }),  // 0 jam
            W('w4', { start:'07:00', end:'' }),       // sebelah
            W('w5', { start:'22:00', end:'06:00' })   // shift malam sah
        ];
        const jm = dcWorkLogHours();
        t('tiga laporan janggal ketahuan', jm.length, 3);
        t('shift malam yang sah tidak ikut ditandai',
          jm.some(i => /w5/.test(JSON.stringify(i))), false);
        t('jenisnya dibedakan',
          jm.map(i => i.kind).sort(), ['jam-nol','jam-panjang','jam-sebelah']);

        // ---------- tanggal masa depan ----------
        clean();
        const future = '2099-01-01';
        workLogs = [W('w1', { date: future })];
        globalDamages = [{ id:'d1', date: future, unitId:'A', unitName:'GGTRA' }];
        t('tanggal masa depan ketahuan lintas modul', dcFutureDates().length, 2);
        clean();
        workLogs = [W('w1', { date: toISODate() })];
        t('tanggal hari ini bukan masa depan', dcFutureDates().length, 0);

        // ---------- tanggal lisensi janggal ----------
        // mulai == habis adalah tanda tangan migrasi Excel yang dihapus di v100.
        clean();
        globalData = [
            U('A', { gpsLicenseStartDate:'2027-04-29', gpsLicenseEndDate:'2027-04-29' }),
            U('B', { displayLicenseStartDate:'2027-06-01', displayLicenseEndDate:'2027-01-01' }),
            U('C', { gpsLicenseStartDate:'2026-01-01', gpsLicenseEndDate:'2027-01-01' })
        ];
        const ld = dcLicenceDates();
        t('sehari dan terbalik ketahuan, yang wajar tidak', ld.length, 2);
        t('jenisnya dibedakan',
          ld.map(i => i.kind).sort(), ['lisensi-sehari','lisensi-terbalik']);
        clean();
        globalData = [U('A', { gpsLicenseEndDate:'2027-01-01' })];
        t('hanya satu tanggal terisi tidak dinilai', dcLicenceDates().length, 0);

        // ---------- kerusakan tanpa unit ----------
        clean();
        globalDamages = [
            { id:'d1', unitId:'A', unitName:'GGTRA' },
            { id:'d2', unitId:'HILANG', unitName:'Unit terhapus' },
            { id:'d3', unitId:'', sn:'SN-B', unitName:'lewat SN' }   // masih ketemu via SN
        ];
        const od = dcOrphanDamage();
        t('hanya yang unitnya benar-benar hilang', od.length, 1);
        t('yang cocok lewat nomor seri tidak dianggap yatim',
          od.some(i => /lewat SN/.test(i.label)), false);

        // ---------- saldo stok minus ----------
        clean();
        stockLedger = [
            { id:'s1', date:'2026-01-01', txnType:'IN',  itemName:'Filter', qty:2 },
            { id:'s2', date:'2026-01-02', txnType:'OUT', itemName:'Filter', qty:5 },
            { id:'s3', date:'2026-01-02', txnType:'IN',  itemName:'Oli',    qty:3 }
        ];
        const ns = dcNegativeStock();
        t('hanya barang yang saldonya minus', ns.length, 1);
        t('saldonya disebut', /-3/.test(ns[0].detail), true);

        // ---------- satu pemeriksaan rusak tidak menjatuhkan halaman ----------
        const realFn = window.dcNegativeStock;
        const broken = DATA_CHECKS.find(c => c.key === 'stok');
        const realRun = broken.run;
        broken.run = () => { throw new Error('sengaja rusak'); };
        clean();
        t('pemeriksaan yang melempar galat tidak menghentikan sisanya',
          Array.isArray(runDataChecks()) && runDataChecks().length, DATA_CHECKS.length);
        broken.run = realRun;

        // ---------- tampilannya ----------
        clean();
        globalData.push(U('C', { sn:'SN-A' }));
        navigateTo('leader'); switchLeaderTab('check');
        t('panel Periksa Data terbuka',
          document.getElementById('leaderCheckPanel').style.display !== 'none', true);
        t('temuannya tampil di tabel',
          document.querySelectorAll('#dataCheckBody tbody tr').length >= 1, true);
        t('tabelnya pakai mode kartu untuk ponsel',
          [...document.querySelectorAll('#dataCheckBody table')]
              .every(el => el.classList.contains('table--cardable')), true);
        clean();
        renderDataCheck();
        t('data bersih: tabelnya kosong',
          document.getElementById('dataCheckBody').innerHTML, '');
        t('dan dikatakan bersih',
          /bersih/.test(document.getElementById('dataCheckSummary').textContent), true);
        t('tombol perbaiki disembunyikan kalau tidak ada yang bisa diperbaiki',
          document.getElementById('dataCheckFixBtn').style.display, 'none');

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
