const { launch, BASE_URL } = require('./_env');

(async () => {
    const b = await launch();
    const page = await b.newPage({ viewport: { width: 1440, height: 1000 } });
    const errors = [];
    page.on('pageerror', e => { if (!/Chart is not defined/.test(e.message)) errors.push(e.message); });
    await page.goto(BASE_URL + '/index.html', { waitUntil: 'load' });
    await page.waitForTimeout(1000);

    await page.addScriptTag({ content: `(() => { try {
        const T = []; const t = (n, g, w) => T.push({ n, g, w, pass: JSON.stringify(g) === JSON.stringify(w) });
        const saved = [];
        window.cloud = { isReady: true,
            saveDevice: r => { saved.push(['device', r]); return Promise.resolve(); },
            deleteDevice: id => { saved.push(['delDevice', id]); return Promise.resolve(); },
            saveStockItem: r => { saved.push(['stock', r]); return Promise.resolve(); },
            deleteStockItem: id => { saved.push(['delStock', id]); return Promise.resolve(); } };
        currentUser = { uid:'u1', email:'o@x.id' };
        currentUserDoc = { role:'owner', status:'active', email:'o@x.id' };
        hideAuthGates(); applyRoleGating();
        globalData = [
            { id:'u_1', name:'JD-6110B-01', sn:'SN111', site:'Timika', status:'Good' },
            { id:'u_2', name:'JD-5075E-04', sn:'SN222', site:'Timika', status:'Good' }
        ];

        warehouseDevices = [
            { id:'d1', sn:'RTK-0001', type:'GPS / StarFire', brand:'John Deere', model:'SF6000',
              status:'warehouse', location:'Gudang Pusat' },
            { id:'d2', sn:'DSP-0002', type:'Display', status:'installed', unitId:'u_1', unitName:'JD-6110B-01' },
            { id:'d3', sn:'WS-0003', type:'Weather Station', status:'installed',
              siteName:'Weather Station Blok C', posX:'136.8901', posY:'-4.5432' },
            { id:'d4', sn:'UTM-0004', type:'Weather Station', status:'installed',
              siteName:'Blok D', posX:'742110', posY:'9497880' },
            { id:'d5', sn:'STR-0005', type:'Steering Sensor', status:'damaged', location:'Gudang Site' }
        ];
        stockLedger = [
            { id:'s1', date:'2026-09-10', txnType:'IN',  itemName:'Filter Oli', qty:20, location:'Gudang Pusat' },
            { id:'s2', date:'2026-09-12', txnType:'OUT', itemName:'Filter Oli', qty:16, location:'Gudang Pusat',
              unitId:'u_1', unitName:'JD-6110B-01' },
            { id:'s3', date:'2026-09-13', txnType:'IN',  itemName:'Selang Hidrolik', qty:3, location:'Gudang Site' },
            { id:'s4', date:'2026-09-14', txnType:'OUT', itemName:'Selang Hidrolik', qty:3, location:'Gudang Site' }
        ];

        navigateTo('warehouse');

        // ---------- akses & menu ----------
        t('view gudang terlihat', document.getElementById('viewWarehouse').style.display, 'block');
        t("area 'warehouse' terdaftar", ACCESS_AREAS.some(a=>a.key==='warehouse'), true);
        t('menu gudang ada di nav', !!document.querySelector('.nav__link[data-view="warehouse"]'), true);

        // ---------- perangkat: KPI per status ----------
        t('KPI di gudang', document.getElementById('devKpi_warehouse').textContent, '1');
        t('KPI terpasang', document.getElementById('devKpi_installed').textContent, '3');
        t('KPI rusak', document.getElementById('devKpi_damaged').textContent, '1');
        t('KPI afkir', document.getElementById('devKpi_retired').textContent, '0');

        // ---------- "di mana perangkat ini" ----------
        t('di gudang -> nama gudang', deviceWhere(warehouseDevices[0]), 'Gudang Pusat');
        t('terpasang di unit -> nama unit hidup', deviceWhere(warehouseDevices[1]), 'JD-6110B-01');
        t('terpasang di lapangan -> nama titik', deviceWhere(warehouseDevices[2]), 'Weather Station Blok C');
        t('terpasang tanpa lokasi -> diberi tahu',
          deviceWhere({ status:'installed' }), 'Terpasang (lokasi belum diisi)');
        t('nama unit diambil dari data hidup, bukan salinan basi',
          deviceWhere({ status:'installed', unitId:'u_1', unitName:'NAMA BASI' }), 'JD-6110B-01');

        // ---------- tautan peta hanya untuk koordinat yang masuk akal ----------
        t('lat/long -> ada tautan peta',
          deviceMapLink(warehouseDevices[2]), 'https://www.google.com/maps?q=-4.5432,136.8901');
        t('UTM -> TIDAK ada tautan peta (agar tidak salah pin)',
          deviceMapLink(warehouseDevices[3]), '');
        t('kosong -> tidak ada tautan', deviceMapLink({}), '');
        t('0,0 -> tidak ada tautan', deviceMapLink({ posX:'0', posY:'0' }), '');

        // ---------- filter perangkat ----------
        t('semua perangkat tampil', document.querySelectorAll('#deviceBody tr').length, 5);
        document.getElementById('devStatusFilter').value = 'installed';
        renderDeviceTable();
        t('filter status', document.querySelectorAll('#deviceBody tr').length, 3);
        document.getElementById('devStatusFilter').value = '';
        document.getElementById('devLocationFilter').value = 'Gudang Site';
        renderDeviceTable();
        t('filter lokasi', document.querySelectorAll('#deviceBody tr').length, 1);
        clearDeviceFilter();
        document.getElementById('devSearch').value = 'weather';
        renderDeviceTable();
        t('pencarian jenis', document.querySelectorAll('#deviceBody tr').length, 2);
        document.getElementById('devSearch').value = 'blok c';
        renderDeviceTable();
        t('pencarian nama titik pemasangan', document.querySelectorAll('#deviceBody tr').length, 1);
        clearDeviceFilter();
        t('reset mengembalikan semua', document.querySelectorAll('#deviceBody tr').length, 5);

        // ---------- saran jenis & lokasi ----------
        t('jenis bawaan + yang dipakai ada di saran',
          allDeviceTypes().includes('Weather Station') && allDeviceTypes().includes('JDLink'), true);
        t('lokasi dikumpulkan dari perangkat dan stok',
          allWarehouseLocations(), ['Gudang Pusat','Gudang Site']);

        // ---------- form perangkat: field mengikuti status ----------
        showAddDeviceForm();
        t('status awal = di gudang', document.getElementById('devStatus').value, 'warehouse');
        t('di gudang: field lokasi tampil',
          document.getElementById('devLocationGroup').style.display, '');
        t('di gudang: field pemasangan tersembunyi',
          document.getElementById('devInstallGroup').style.display, 'none');
        document.getElementById('devStatus').value = 'installed';
        onDeviceStatusChange();
        t('terpasang: field pemasangan tampil',
          document.getElementById('devInstallGroup').style.display, '');
        t('terpasang: field lokasi gudang tersembunyi',
          document.getElementById('devLocationGroup').style.display, 'none');

        // ---------- simpan perangkat ----------
        let before = saved.length;
        document.getElementById('devSn').value = 'RTK-0001';   // SN sudah dipakai
        saveDevice({ preventDefault(){} });
        t('SN duplikat ditolak', saved.length, before);

        document.getElementById('devSn').value = 'WS-0009';
        document.getElementById('devType').value = 'Weather Station';
        saveDevice({ preventDefault(){} });
        t('terpasang tanpa unit & tanpa lokasi ditolak', saved.length, before);

        document.getElementById('devSite').value = 'Weather Station Blok E';
        document.getElementById('devPosX').value = '136.9';
        document.getElementById('devPosY').value = '-4.6';
        saveDevice({ preventDefault(){} });
        t('tersimpan setelah lokasi diisi', saved.length, before + 1);
        const dev = saved[saved.length-1][1];
        t('koordinat tersimpan', [dev.posX, dev.posY], ['136.9','-4.6']);
        t('lokasi gudang dikosongkan saat terpasang', dev.location, '');

        // Pindah ke gudang: data pemasangan harus dibersihkan.
        showAddDeviceForm();
        document.getElementById('devSn').value = 'RTK-0010';
        document.getElementById('devStatus').value = 'warehouse';
        onDeviceStatusChange();
        document.getElementById('devLocation').value = 'Gudang Pusat';
        document.getElementById('devPosX').value = '999';   // sisa isian lama
        saveDevice({ preventDefault(){} });
        const dev2 = saved[saved.length-1][1];
        t('kembali ke gudang: koordinat dibersihkan', [dev2.posX, dev2.posY, dev2.siteName], ['','','']);
        t('kembali ke gudang: lokasi tersimpan', dev2.location, 'Gudang Pusat');

        // ---------- profil unit ----------
        t('perangkat terpasang muncul di unit', devicesForUnit('u_1').map(d=>d.sn), ['DSP-0002']);
        t('unit tanpa perangkat', devicesForUnit('u_2').length, 0);

        // ---------- stok: sisa ----------
        switchWarehouseTab('stock');
        const sum = stockSummary('');
        t('sisa filter oli (20 masuk - 16 keluar)', sum.find(s=>s.name==='Filter Oli').qty, 4);
        t('sisa selang habis (3 - 3)', sum.find(s=>s.name==='Selang Hidrolik').qty, 0);
        t('sisa per lokasi', stockSummary('Gudang Site').map(s=>s.name), ['Selang Hidrolik']);
        t('chip stok menipis ditandai',
          document.querySelectorAll('#stockSummary .stock-chip--low').length, 1);
        t('chip stok habis ditandai',
          document.querySelectorAll('#stockSummary .stock-chip--out').length, 1);

        // ---------- stok: filter ----------
        t('semua transaksi tampil', document.querySelectorAll('#stockBody tr').length, 4);
        document.getElementById('stkTypeFilter').value = 'OUT';
        renderStockView();
        t('filter keluar saja', document.querySelectorAll('#stockBody tr').length, 2);
        document.getElementById('stkTypeFilter').value = '';
        document.getElementById('stkFrom').value = '2026-09-13';
        renderStockView();
        t('filter tanggal', document.querySelectorAll('#stockBody tr').length, 2);
        clearStockFilter();
        t('reset stok', document.querySelectorAll('#stockBody tr').length, 4);

        // ---------- form stok ----------
        showAddStockForm();
        t('masuk: field unit tersembunyi', document.getElementById('stkUnitGroup').style.display, 'none');
        document.getElementById('stkTxnType').value = 'OUT';
        onStockTxnChange();
        t('keluar: field unit tampil', document.getElementById('stkUnitGroup').style.display, '');
        before = saved.length;
        document.getElementById('stkItem').value = 'Filter Oli';
        document.getElementById('stkQty').value = '2';
        document.getElementById('stkUnit').value = 'UNIT NGACO';
        saveStockItem({ preventDefault(){} });
        t('unit ngaco membatalkan simpan', saved.length, before);
        document.getElementById('stkUnit').value = 'SN222';
        saveStockItem({ preventDefault(){} });
        t('tersimpan setelah unit benar', saved.length, before + 1);
        const stk = saved[saved.length-1][1];
        t('unit terhubung', [stk.unitId, stk.unitName], ['u_2','JD-5075E-04']);
        t('jenis transaksi tersimpan', stk.txnType, 'OUT');

        // ---------- akses hanya-lihat ----------
        currentUserDoc = { role:'khl', status:'active', access:{ warehouse:'view' } };
        renderWarehouseView();
        t('hanya-lihat: tabel stok tanpa tombol aksi',
          document.querySelectorAll('#stockBody .row-actions').length, 0);
        switchWarehouseTab('devices');
        t('hanya-lihat: tabel perangkat tanpa tombol aksi',
          document.querySelectorAll('#deviceBody .row-actions').length, 0);
        applyAccessVisibility();
        t('hanya-lihat: body ditandai read-only', document.body.dataset.roWarehouse, '1');
        before = saved.length;
        showAddDeviceForm();
        t('hanya-lihat: form tambah ditolak', saved.length, before);

        currentUserDoc = { role:'khl', status:'active', access:{ warehouse:'none' } };
        t('tanpa akses: menu gudang tertutup', canViewView('warehouse'), false);
        applyAccessVisibility();
        t('tanpa akses: link nav disembunyikan',
          document.querySelector('.nav__link[data-view="warehouse"]').style.display, 'none');

        currentUserDoc = { role:'owner', status:'active' };
        window.__T = T;
    } catch (e) { window.__T = [{n:'THREW: '+e.message+' | '+(e.stack||'').split('\\n')[1], g:1, w:0, pass:false}]; }
    })();` });

    const res = await page.evaluate(() => window.__T);
    const fail = res.filter(r => !r.pass);
    res.forEach(r => console.log(`${r.pass ? 'PASS' : 'FAIL'}  ${r.n}${r.pass ? '' : `\n        dapat=${JSON.stringify(r.g)} harap=${JSON.stringify(r.w)}`}`));
    console.log(`\n${res.length - fail.length}/${res.length} lulus`);
    console.log('PAGE ERRORS:', errors.length ? errors : 'none');
    await b.close();
    process.exit(fail.length ? 1 : 0);
})();
