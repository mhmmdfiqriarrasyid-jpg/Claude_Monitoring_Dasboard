// v158 — kendaraan operasional (motor perusahaan) dan catatan isi BBM di Warehouse:
// data motor dengan KM awal saat serah terima; setiap isi BBM mencatat
// tanggal, liter, KM odometer dan foto struk (opsional). Jarak dan km/liter
// dihitung dari KM berturut-turut; KM mundur dan konsumsi janggal ditandai.
const { launch, BASE_URL } = require('./_env');

(async () => {
    const b = await launch();
    const page = await b.newPage({ viewport: { width: 1280, height: 900 } });
    const errors = [];
    page.on('pageerror', e => { if (!/Chart is not defined/.test(e.message)) errors.push(e.message); });
    await page.goto(BASE_URL + '/index.html', { waitUntil: 'load' });
    await page.waitForTimeout(1000);

    await page.addScriptTag({ content: `(async () => { try {
        const T = []; const t = (n, g, w) => T.push({ n, g, w, pass: JSON.stringify(g) === JSON.stringify(w) });
        const tick = ms => new Promise(r => setTimeout(r, ms || 20));
        const toasts = []; window.showToast = m => toasts.push(String(m));
        let confirmAnswer = true, asked = [];
        window.confirm = m => { asked.push(String(m)); return confirmAnswer; };
        URL.createObjectURL = blob => { window.__blob = blob; return 'blob:test'; };
        URL.revokeObjectURL = () => {};
        HTMLAnchorElement.prototype.click = function () {};
        const calls = [];
        const ok = () => Promise.resolve();
        const PH = s => 'data:image/jpeg;base64,' + s;
        const photos = {};
        window.cloud = { isReady: true, addHistoryEvents: ok, subscribeUsers: () => () => {},
            saveVehicle: r => { calls.push(['veh', r.id]); return ok(); },
            deleteVehicle: id => { calls.push(['delVeh', id]); return ok(); },
            saveFuelLog: r => { calls.push(['fuel', r.id, r.hasPhoto]); return ok(); },
            deleteFuelLog: id => { calls.push(['delFuel', id]); return ok(); },
            saveFuelPhoto: (id, p) => { calls.push(['photo', id]); photos[id] = p; return ok(); },
            deleteFuelPhoto: id => { calls.push(['delPhoto', id]); delete photos[id]; return ok(); },
            getFuelPhoto: id => Promise.resolve(photos[id] || '') };
        currentUser = { uid: 'uO', email: 'o@x.id' };
        currentUserDoc = { role: 'owner', status: 'active' };
        hideAuthGates(); applyRoleGating();
        teamMembers = [{ id: 'm1', name: 'Andi', active: true }];
        const isOpen = id => document.getElementById(id).classList.contains('open');
        const today = toISODate();
        const daysAgo = n => toISODate(new Date(Date.now() - n * 86400000));

        // =============== tab dan data motor ===============
        navigateTo('warehouse'); switchWarehouseTab('vehicles'); await tick(60);
        t('tab Vehicles & Fuel tampil', [document.getElementById('whVehiclePanel').style.display, document.getElementById('whDevicePanel').style.display], ['', 'none']);
        t('belum ada motor: petunjuk menyebut KM awal saat serah terima', /handover date and the odometer/.test(document.getElementById('vehicleCards').textContent), true);
        toasts.length = 0;
        showFuelForm();
        t('isi BBM tanpa motor: diminta tambah motor dulu', [isOpen('fuelModal'), toasts.pop()], [false, 'Add a vehicle first']);

        showAddVehicleForm(); await tick(20);
        t('form motor: tanggal serah terima terisi hari ini', document.getElementById('vehHandover').value, today);
        document.getElementById('vehPlate').value = 'PA 1234 XY';
        document.getElementById('vehBrand').value = 'Honda';
        document.getElementById('vehHandover').value = '2026-10-16';
        document.getElementById('vehStartKm').value = '8880';
        saveVehicle({ preventDefault() {} }); await tick(30);
        const v = vehicles[0];
        t('motor tersimpan dengan KM awal 8.880', [vehicles.length, v.plate, v.brand, v.startKm, v.handoverDate, v.status, calls.pop()[0]],
          [1, 'PA 1234 XY', 'Honda', 8880, '2026-10-16', 'active', 'veh']);
        showAddVehicleForm(); document.getElementById('vehPlate').value = 'pa 1234 xy'; document.getElementById('vehStartKm').value = '1';
        toasts.length = 0; saveVehicle({ preventDefault() {} });
        t('plat ganda ditolak', [vehicles.length, /already registered/.test(toasts.pop() || '')], [1, true]);
        closeVehicleModal(true);

        // =============== isi BBM ===============
        const log = (date, liters, km, extra) => {
            showFuelForm(v.id);
            document.getElementById('fuelDate').value = date;
            document.getElementById('fuelLiters').value = liters;
            document.getElementById('fuelKm').value = km;
            if (extra) extra();
            saveFuelLog({ preventDefault() {} });
        };
        log(daysAgo(10), '3.2', '9012');
        const h1 = fuelHistory(v.id);
        t('isi pertama: jarak dihitung dari KM serah terima', [h1[0].distance, Math.round(h1[0].kmPerL * 10) / 10, h1[0].flags], [132, 41.3, []]);
        log(daysAgo(7), '3.5', '9160');
        log(daysAgo(4), '3.1', '9295');
        showFuelForm(v.id); document.getElementById('fuelKm').value = '9400'; updateFuelKmHint();
        t('petunjuk KM terakhir di form', document.getElementById('fuelKmHint').textContent, 'Last reading: 9,295 km on ' + daysAgo(4) + ' (refuel) — +105 km since then');
        closeFuelModal(true);
        // KM lebih rendah: ditanya; batal = tidak disimpan
        confirmAnswer = false; asked = [];
        log(daysAgo(1), '3.4', '9080');
        t('KM lebih rendah dari sebelumnya: ditanya, batal = tidak disimpan', [fuelLogs.length, /LOWER than the last reading 9,295 km/.test(asked[0] || '')], [3, true]);
        confirmAnswer = true; closeFuelModal(true);
        log(daysAgo(1), '3.4', '9080');
        const h = fuelHistory(v.id);
        t('KM mundur tetap tersimpan tetapi ditandai', h.map(x => x.flags.join('|')), ['', '', '', 'back']);
        t('jarak setelah KM mundur tidak dihitung', h[3].distance, null);
        log(today, '3.3', '9420');
        const h2 = fuelHistory(v.id);
        t('isi berikutnya diukur dari KM tertinggi sebelumnya (bukan yang salah ketik)', h2[4].distance, 125);
        const s = vehicleStats(v, today.slice(0, 7));
        t('ringkasan motor: KM sekarang, sejak serah terima, total liter', [s.latestKm, s.totalKm, Math.round(s.totalLiters * 100) / 100], [9420, 540, 16.5]);
        // rata-rata = jarak terhitung / liter dari isi yang terhitung
        t('rata-rata km/L dari isi yang terhitung', Math.round(s.avgKmPerL * 10) / 10, Math.round(((132 + 148 + 135 + 125) / (3.2 + 3.5 + 3.1 + 3.3)) * 10) / 10);
        // liter tidak wajar
        toasts.length = 0; log(today, '0', '9500');
        t('liter 0 ditolak', [fuelLogs.length, toasts.pop()], [5, 'Liters must be more than 0']);
        closeFuelModal(true);
        toasts.length = 0; log(toISODate(new Date(Date.now() + 2 * 86400000)), '3', '9500');
        t('tanggal di masa depan ditolak', [fuelLogs.length, toasts.pop()], [5, 'The date is in the future']);
        closeFuelModal(true);
        // konsumsi janggal: 50 km dengan 10 liter (5 km/L) jauh dari median ~40
        log(today, '10', '9470');
        t('konsumsi jauh dari biasanya ditandai', fuelHistory(v.id).pop().flags, ['unusual']);
        // lompatan besar: ditanya
        asked = []; confirmAnswer = false;
        log(today, '3', '12000');
        t('lompatan > 1.500 km: ditanya dulu', [fuelLogs.length, /since the last reading/.test(asked[0] || '')], [6, true]);
        confirmAnswer = true; closeFuelModal(true);

        // =============== tampilan ===============
        renderVehicleView(); await tick(30);
        t('kartu motor: KM sekarang dan sejak serah terima', [...document.querySelectorAll('.vehicle-card__stats dd')].slice(0, 2).map(d => d.textContent), ['9,470 km', '590 km']);
        t('kartu menyebut serah terima 8,880 km dan yang perlu dicek', [/Handover 2026-10-16 at 8,880 km/.test(document.querySelector('.vehicle-card__meta').textContent), /2 to check/.test(document.querySelector('.vehicle-card__meta').textContent)], [true, true]);
        t('tabel BBM: 6 baris, terbaru di atas, baris janggal diberi warna', [document.querySelectorAll('#fuelBody tr').length, document.querySelectorAll('#fuelBody tr.row-flagged').length], [6, 2]);
        t('KPI: yang perlu dicek', document.getElementById('vehKpi_flags').textContent, '2');
        document.getElementById('fuelMonthFilter').value = '2020-01'; renderVehicleView();
        t('filter bulan tanpa data: pesan kosong', /No fuel records match/.test(document.getElementById('fuelBody').textContent), true);
        clearFuelFilter();

        // =============== foto struk: dikirim sebelum catatannya ===============
        calls.length = 0;
        log(today, '3', '9550', () => { _fuelPhotoData = PH('STRUK'); _fuelPhotoDirty = true; });
        const withPhoto = fuelLogs[fuelLogs.length - 1];
        t('foto dulu, lalu catatan (hasPhoto)', calls.map(c => c.slice(0, 1).concat(c[0] === 'fuel' ? [c[2]] : [])), [['photo'], ['fuel', true]]);
        window.cloud.getFuelPhoto = id => Promise.resolve(photos[id] || '');
        _fuelPhotoCache.clear();
        await openFuelPhoto(withPhoto.id, null);
        t('foto struk dibuka dengan label', [document.getElementById('photoLightbox').classList.contains('open'), /^Receipt · /.test(document.getElementById('photoLightboxCaption').textContent)], [true, true]);
        closePhotoLightbox();
        calls.length = 0;
        deleteFuelLog(withPhoto.id);
        t('hapus catatan: fotonya ikut dihapus', calls.map(c => c[0]).sort(), ['delFuel', 'delPhoto']);

        // =============== ketikan tidak dibuang tanpa ditanya ===============
        showFuelForm(v.id); await tick(30);
        document.getElementById('fuelNote').value = 'Isi di SPBU Sentani';
        confirmAnswer = false; asked = [];
        closeFuelModal(); await tick(20);
        t('form BBM: ketikan ditanya sebelum dibuang', [isOpen('fuelModal'), /Discard what you typed/.test(asked[0] || '')], [true, true]);
        confirmAnswer = true; closeFuelModal(); await tick(20);

        // =============== hapus motor yang punya catatan ===============
        toasts.length = 0;
        deleteVehicle(v.id);
        t('motor dengan catatan BBM tidak bisa dihapus (pakai Retired)', [vehicles.length, /set its status to Retired/.test(toasts.pop() || '')], [1, true]);

        // =============== hanya-lihat ===============
        currentUserDoc = { role: 'staff', status: 'active', access: { warehouse: 'view' } }; applyRoleGating();
        renderVehicleView();
        t('hanya-lihat: tanpa tombol Log Fuel / Edit di kartu dan tabel', [document.querySelectorAll('.vehicle-card__actions .btn-primary').length, document.querySelectorAll('#fuelBody .row-actions').length], [0, 0]);
        toasts.length = 0; showFuelForm(v.id);
        t('hanya-lihat: form tidak terbuka', isOpen('fuelModal'), false);
        currentUserDoc = { role: 'owner', status: 'active' }; applyRoleGating();

        // =============== Kotak Keputusan ===============
        const g = decisionGroups().find(x => x.key === 'fuel');
        t('Kotak Keputusan: catatan BBM yang perlu dicek', [!!g, g && g.total, g && g.goto], [true, 2, 'warehouseVehicles']);

        // =============== CSV ===============
        window.__blob = null; exportFuelCSV();
        const csv = await window.__blob.text();
        t('CSV: kolom odometer, jarak, liter, km/L, perlu dicek', /Odometer \\(km\\).*Distance \\(km\\).*Liters.*km\\/L \\(estimate\\).*To check/.test(csv.split('\\n')[0]), true);

        // =============== cadangan ===============
        t('cadangan memuat kendaraan, catatan BBM dan foto struk',
          [BACKUP_PARTS.some(p => p.key === 'vehicles'), BACKUP_PARTS.some(p => p.key === 'fuelLogs'), BACKUP_PHOTO_KINDS.some(k => k.key === 'fuelPhotos' && k.part === 'fuelLogs')],
          [true, true, true]);
        t('dialog terdaftar untuk Back / Esc', [MODAL_CLOSERS.vehicleModal, MODAL_CLOSERS.fuelModal], ['closeVehicleModal', 'closeFuelModal']);

        window.__T = T;
    } catch (e) { window.__T = [{ n: 'EXCEPTION ' + e.message + ' ' + e.stack, pass: false }]; } })();` });

    await page.waitForFunction(() => window.__T, null, { timeout: 90000 });
    const T = await page.evaluate(() => window.__T);

    // HP: kartu motor satu kolom, tombol cukup besar, tabel jadi kartu
    await page.setViewportSize({ width: 390, height: 844 });
    const phone = await page.evaluate(async () => {
        renderVehicleView(); await new Promise(r => setTimeout(r, 50));
        const card = document.querySelector('.vehicle-card').getBoundingClientRect();
        const btn = document.querySelector('.vehicle-card__actions .btn').getBoundingClientRect();
        return [card.right <= innerWidth, btn.height >= 44, document.documentElement.scrollWidth <= innerWidth];
    });
    T.push({ n: 'HP: kartu motor muat, tombol ≥44px, tanpa geser samping', g: phone, w: [true, true, true], pass: JSON.stringify(phone) === JSON.stringify([true, true, true]) });

    let pass = 0;
    T.forEach(x => { if (x.pass) pass++; else console.log('FAIL', x.n, '\n  dapat :', JSON.stringify(x.g), '\n  harap :', JSON.stringify(x.w)); });
    if (errors.length) console.log('PAGE ERRORS:', errors);
    console.log(`${pass}/${T.length} lulus`);
    await b.close();
    process.exit(pass === T.length && !errors.length ? 0 : 1);
})();
