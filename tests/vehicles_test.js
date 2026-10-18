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
        // serah terima sebelum semua catatan uji (tanggal relatif ke hari ini)
        const HANDOVER = daysAgo(30);

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
        document.getElementById('vehHandover').value = HANDOVER;
        document.getElementById('vehStartKm').value = '8880';
        saveVehicle({ preventDefault() {} }); await tick(30);
        const v = vehicles[0];
        t('motor tersimpan dengan KM awal 8.880', [vehicles.length, v.plate, v.brand, v.startKm, v.handoverDate, v.status, calls.pop()[0]],
          [1, 'PA 1234 XY', 'Honda', 8880, HANDOVER, 'active', 'veh']);
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
        t('petunjuk KM sebelumnya di form', document.getElementById('fuelKmHint').textContent, 'Previous reading: 9,295 km on ' + daysAgo(4) + ' — +105 km since then');
        closeFuelModal(true);
        // KM lebih rendah: ditanya; batal = tidak disimpan
        confirmAnswer = false; asked = [];
        log(daysAgo(1), '3.4', '9080');
        t('KM lebih rendah dari sebelumnya: ditanya, batal = tidak disimpan', [fuelLogs.length, /LOWER than the reading before it, 9,295 km/.test(asked[0] || '')], [3, true]);
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
        // rata-rata = sejak serah terima ÷ total liter: tiga angka di kartu cocok
        t('rata-rata km/L = sejak serah terima ÷ total liter', Math.round(s.avgKmPerL * 10) / 10, Math.round((540 / 16.5) * 10) / 10);
        t('isi setelah KM mundur ikut menghitung liter isi yang tidak terukur', Math.round(h2[4].kmPerL * 10) / 10, Math.round((125 / (3.3 + 3.4)) * 10) / 10);
        // liter tidak wajar
        toasts.length = 0; log(today, '0', '9500');
        t('liter 0 ditolak', [fuelLogs.length, toasts.pop()], [5, 'Liters must be more than 0']);
        closeFuelModal(true);
        toasts.length = 0; log(today, 'tiga', '9500');
        t('liter bukan angka ditolak', [fuelLogs.length, toasts.pop()], [5, 'Liters must be a number like 3.5 or 3,5']);
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
        t('lompatan > 1.500 km: ditanya dulu', [fuelLogs.length, /since the reading before/.test(asked[0] || '')], [6, true]);
        confirmAnswer = true; closeFuelModal(true);

        // =============== HP Indonesia: koma desimal, titik ribuan ===============
        t('parse liter "3,5" = 3,5 L (bukan 35)', [parseLiters('3,5'), parseLiters('3.5'), parseLiters('12'), isNaN(parseLiters('1,234')), isNaN(parseLiters('3,5,1'))], [3.5, 3.5, 12, true, true]);
        t('parse KM "9.012" = 9012', [parseKm('9.012'), parseKm('9,012'), parseKm('9012'), parseKm('12.345.678'), isNaN(parseKm('9.01')), isNaN(parseKm('9012,5'))], [9012, 9012, 9012, 12345678, true, true]);
        asked = []; confirmAnswer = true;
        log(today, '3,5', '9.600');
        const lastRec = fuelLogs[fuelLogs.length - 1];
        t('isi "3,5" liter di KM "9.600" tersimpan sebagai 3,5 L dan 9600 km', [lastRec.liters, lastRec.km], [3.5, 9600]);

        // =============== mengedit catatan lama: tidak ada peringatan palsu ===============
        const old = fuelHistory(v.id)[0].rec;
        asked = [];
        editFuelLog(old.id); await tick(20);
        document.getElementById('fuelNote').value = 'struk menyusul';
        t('edit catatan lama: petunjuk tidak merah', document.getElementById('fuelKmHint').classList.contains('form-hint--bad'), false);
        saveFuelLog({ preventDefault() {} });
        t('edit catatan lama: tersimpan tanpa pertanyaan', [asked.length, fuelLogs.find(r => r.id === old.id).note], [0, 'struk menyusul']);
        // isi terlupa dengan tanggal lampau: dibandingkan dengan bacaan sebelumnya menurut tanggal
        asked = [];
        log(daysAgo(9), '2', '9100');
        t('isi terlupa diisi belakangan (tanggal benar): tanpa pertanyaan', asked.length, 0);
        // tanggal lampau tetapi KM melebihi bacaan yang lebih baru: ditanya
        asked = []; confirmAnswer = false;
        log(daysAgo(8), '2', '9300');
        t('KM lebih tinggi dari bacaan sesudahnya: ditanya', /HIGHER than the later reading/.test(asked[0] || ''), true);
        confirmAnswer = true; closeFuelModal(true);

        // =============== simpan ditolak: tidak tertinggal di layar, bisa dibuka lagi ===============
        const realSave = window.cloud.saveFuelLog;
        window.cloud.saveFuelLog = () => Promise.reject(Object.assign(new Error('Missing or insufficient permissions.'), { code: 'permission-denied' }));
        const n0 = fuelLogs.length;
        log(today, '3', '9700');
        await tick(60);
        t('ditolak server: baris diambil kembali, ada "Not saved"', [fuelLogs.length, !!document.getElementById('unsentNotice')], [n0, true]);
        window.cloud.saveFuelLog = realSave;
        reopenUnsent([..._unsent.keys()].pop()); await tick(30);
        t('dibuka lagi dengan isinya', [isOpen('fuelModal'), document.getElementById('fuelKm').value, document.getElementById('fuelLiters').value], [true, '9700', '3']);
        saveFuelLog({ preventDefault() {} }); await tick(30);
        t('lalu tersimpan', fuelLogs.length, n0 + 1);

        // =============== foto ditolak bersama catatannya: tidak ada "Record saved" ===============
        const denied = () => Promise.reject(Object.assign(new Error('Missing or insufficient permissions.'), { code: 'permission-denied' }));
        const realPhotoSave = window.cloud.saveFuelPhoto;
        window.cloud.saveFuelLog = denied; window.cloud.saveFuelPhoto = denied;
        toasts.length = 0;
        const u0 = _unsent.size;
        log(today, '3', '9710', () => { _fuelPhotoData = PH('DITOLAK'); _fuelPhotoDirty = true; });
        await tick(80);
        t('foto dan catatan ditolak: hanya "Not saved", tanpa "Record saved"', [toasts.some(m => /Record saved/.test(m)), _unsent.size], [false, u0 + 1]);
        window.cloud.saveFuelLog = realSave;
        toasts.length = 0;
        log(today, '3', '9705', () => { _fuelPhotoData = PH('GAGAL'); _fuelPhotoDirty = true; });
        await tick(80);
        t('catatan tersimpan tetapi fotonya gagal: diberi tahu', toasts.includes('Record saved, but the receipt photo failed to send'), true);
        window.cloud.saveFuelPhoto = realPhotoSave;
        // dibuka lagi saat form tidak bisa terbuka (motor Retired): isian tetap disimpan
        const keptKey = [..._unsent.keys()].pop();
        editVehicle(v.id); document.getElementById('vehStatus').value = 'retired'; saveVehicle({ preventDefault() {} }); await tick(20);
        reopenUnsent(keptKey);
        t('dibuka lagi saat motor Retired: form tidak terbuka, isian tetap ada', [isOpen('fuelModal'), _unsent.has(keptKey)], [false, true]);
        editVehicle(v.id); document.getElementById('vehStatus').value = 'active'; saveVehicle({ preventDefault() {} }); await tick(20);
        // dibuka lagi saat sedang mengetik isi BBM lain: ditanya dulu
        showFuelForm(v.id); await tick(20);
        document.getElementById('fuelNote').value = 'isi kedua, belum disimpan';
        asked = []; confirmAnswer = false;
        reopenUnsent(keptKey); await tick(20);
        t('dibuka lagi di atas form yang sedang diketik: ditanya, "tidak" = ketikan dan isian tetap', [/Discard what you typed/.test(asked[0] || ''), document.getElementById('fuelNote').value, _unsent.has(keptKey)], [true, 'isi kedua, belum disimpan', true]);
        confirmAnswer = true;
        reopenUnsent(keptKey); await tick(20);
        t('... "ya" = isian yang ditolak terbuka dengan fotonya', [document.getElementById('fuelKm').value, _fuelPhotoData, _unsent.has(keptKey)], ['9710', PH('DITOLAK'), false]);
        closeFuelModal(true);
        // dibuat lalu dihapus saat offline, keduanya ditolak: tidak muncul lagi
        let refuseCreate, refuseDelete;
        const realDel = window.cloud.deleteFuelLog;
        window.cloud.saveFuelLog = () => new Promise((_, rej) => { refuseCreate = rej; });
        window.cloud.deleteFuelLog = () => new Promise((_, rej) => { refuseDelete = rej; });
        log(today, '3', '9720');
        const ghost = fuelLogs[fuelLogs.length - 1];
        deleteFuelLog(ghost.id);
        const permErr = Object.assign(new Error('Missing or insufficient permissions.'), { code: 'permission-denied' });
        refuseCreate(permErr); await tick(20);
        refuseDelete(permErr); await tick(20);
        t('dibuat lalu dihapus, keduanya ditolak: catatan tidak muncul lagi', fuelLogs.some(r => r.id === ghost.id), false);
        window.cloud.saveFuelLog = realSave; window.cloud.deleteFuelLog = realDel;
        [..._unsent.keys()].forEach(k => _unsent.delete(k)); renderUnsentNotice();

        // =============== tampilan ===============
        renderVehicleView(); await tick(30);
        const st2 = vehicleStats(v, today.slice(0, 7));
        t('kartu motor: KM sekarang dan sejak serah terima', [...document.querySelectorAll('.vehicle-card__stats dd')].slice(0, 2).map(d => d.textContent), [fmtKm(st2.latestKm), fmtKm(st2.latestKm - 8880)]);
        t('kartu menyebut serah terima 8,880 km dan yang perlu dicek', [new RegExp('Handover ' + HANDOVER + ' at 8,880 km').test(document.querySelector('.vehicle-card__meta').textContent), new RegExp(st2.flagged + ' to check').test(document.querySelector('.vehicle-card__meta').textContent)], [true, true]);
        t('tabel BBM: semua baris, baris janggal diberi warna', [document.querySelectorAll('#fuelBody tr').length, document.querySelectorAll('#fuelBody tr.row-flagged').length], [fuelLogs.length, st2.flagged]);
        t('KPI: yang perlu dicek', document.getElementById('vehKpi_flags').textContent, String(st2.flagged));
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

        // =============== catatan motor tampil di kartu; semua motor Retired ===============
        editVehicle(v.id); document.getElementById('vehNote').value = 'STNK habis 2027-03'; document.getElementById('vehStatus').value = 'retired';
        saveVehicle({ preventDefault() {} }); await tick(30);
        t('catatan motor tampil di kartu', /STNK habis 2027-03/.test(document.getElementById('vehicleCards').textContent), true);
        toasts.length = 0; showFuelForm();
        t('semua motor Retired: pesan yang tepat', /All vehicles are retired/.test(toasts.pop() || ''), true);
        editVehicle(v.id); document.getElementById('vehStatus').value = 'active'; saveVehicle({ preventDefault() {} }); await tick(20);

        // =============== hanya-lihat ===============
        currentUserDoc = { role: 'staff', status: 'active', access: { warehouse: 'view' } }; applyRoleGating();
        renderVehicleView();
        t('hanya-lihat: tanpa tombol Log Fuel / Edit di kartu dan tabel', [document.querySelectorAll('.vehicle-card__actions .btn-primary').length, document.querySelectorAll('#fuelBody .row-actions').length], [0, 0]);
        toasts.length = 0; showFuelForm(v.id);
        t('hanya-lihat: form tidak terbuka', isOpen('fuelModal'), false);
        currentUserDoc = { role: 'owner', status: 'active' }; applyRoleGating();

        // =============== Kotak Keputusan ===============
        const g = decisionGroups().find(x => x.key === 'fuel');
        t('Kotak Keputusan: catatan BBM yang perlu dicek', [!!g, g && g.total > 0, g && g.goto], [true, true, 'warehouseVehicles']);
        document.getElementById('fuelMonthFilter').value = '2020-01';
        goDecision('warehouseVehicles'); await tick(30);
        t('dari Kotak Keputusan: filter lama dibersihkan, baris janggal terlihat', [document.getElementById('fuelMonthFilter').value, document.querySelectorAll('#fuelBody tr.row-flagged').length > 0], ['', true]);

        // =============== isi saat serah terima, isi ulang di KM yang sama, tanggal bacaan ===============
        showAddVehicleForm(); await tick(10);
        document.getElementById('vehPlate').value = 'PA 5678 ZZ';
        document.getElementById('vehHandover').value = HANDOVER;
        document.getElementById('vehStartKm').value = '5000';
        saveVehicle({ preventDefault() {} }); await tick(20);
        const v2 = vehicles.find(x => x.plate === 'PA 5678 ZZ');
        const log2 = (date, liters, km) => {
            showFuelForm(v2.id);
            document.getElementById('fuelDate').value = date;
            document.getElementById('fuelLiters').value = liters;
            document.getElementById('fuelKm').value = km;
            saveFuelLog({ preventDefault() {} });
        };
        asked = []; confirmAnswer = true;
        log2(HANDOVER, '3', '5000');      // tangki diisi penuh saat serah terima
        log2(daysAgo(25), '3', '5120');
        log2(daysAgo(20), '3', '5240');
        let hh = fuelHistory(v2.id);
        const s3 = vehicleStats(v2);
        t('isi saat serah terima: tanpa pertanyaan, tidak ditandai, bukan konsumsi',
          [asked.length, hh[0].flags, hh[0].used, hh[1].kmPerL, s3.avgKmPerL, s3.totalLiters, s3.usedLiters], [0, [], null, 40, 40, 9, 6]);
        renderVehicleView();
        const card2 = [...document.querySelectorAll('.vehicle-card')].find(c => /PA 5678 ZZ/.test(c.textContent));
        t('kartu: rata-rata menyebut liter yang tidak dihitung', /≈ 40\\.0 km\\/L\\s*excl\\. 3 L/.test(card2.querySelector('.vehicle-card__stats').textContent), true);
        log2(daysAgo(20), '1', '5240');   // isi ulang di KM yang sama
        hh = fuelHistory(v2.id);
        t('isi ulang di KM yang sama: ditandai, liternya menambah isi sebelumnya', [hh[3].flags, hh[2].used, hh[2].kmPerL], [['noDistance'], 4, 30]);
        // KM salah ketik rendah, lalu bacaan baru: tanggal dari bacaan tertinggi, bukan dari catatan salah ketik
        log2(daysAgo(15), '3', '5100');
        showFuelForm(v2.id); document.getElementById('fuelDate').value = daysAgo(12); document.getElementById('fuelKm').value = '5360'; updateFuelKmHint();
        t('bacaan sebelumnya: KM dan tanggal dari catatan yang sama', document.getElementById('fuelKmHint').textContent, 'Previous reading: 5,240 km on ' + daysAgo(20) + ' — +120 km since then');
        closeFuelModal(true);
        log2(daysAgo(10), '3', '5400');
        showFuelForm(v2.id); document.getElementById('fuelDate').value = daysAgo(10); updateFuelKmHint();
        t('KM belum diisi: petunjuk menyebut bacaan terakhir hari itu', document.getElementById('fuelKmHint').textContent, 'Previous reading: 5,400 km on ' + daysAgo(10));
        document.getElementById('fuelKm').value = '5300'; updateFuelKmHint();
        t('KM lebih rendah dari isi lain di hari yang sama: petunjuk merah', [/5,400 km is already logged on/.test(document.getElementById('fuelKmHint').textContent), document.getElementById('fuelKmHint').classList.contains('form-hint--bad')], [true, true]);
        closeFuelModal(true);
        const n2 = fuelLogs.length;
        asked = []; confirmAnswer = false;
        log2(daysAgo(10), '2', '5300');
        t('... dan ditanya; batal = tidak disimpan', [/already a fill on .* at 5,400 km/.test(asked[0] || ''), fuelLogs.length], [true, n2]);
        closeFuelModal(true);
        // sebelum serah terima: satu pertanyaan, tidak juga "LOWER than the reading before"
        asked = [];
        log2(daysAgo(40), '2', '4900');
        t('tanggal sebelum serah terima: satu pertanyaan saja', [asked.length, /before the handover \\(.* at 5,000 km\\)\\. It will be marked/.test(asked[0] || ''), fuelLogs.length], [1, true, n2]);
        updateFuelKmHint();
        t('... petunjuknya menyebut bacaan serah terima', document.getElementById('fuelKmHint').textContent, 'Handover reading: 5,000 km on ' + HANDOVER + ' — this date is before the handover');
        confirmAnswer = true; closeFuelModal(true);
        // edit isi yang lebih awal di hari yang sama: tanpa pertanyaan
        log2(daysAgo(10), '2', '5300');
        const early = fuelLogs[fuelLogs.length - 1];
        asked = [];
        editFuelLog(early.id); await tick(10);
        document.getElementById('fuelNote').value = 'isi pagi';
        saveFuelLog({ preventDefault() {} });
        t('edit isi pagi (hari yang sama, KM sama): tanpa pertanyaan', [asked.length, fuelLogs.find(r => r.id === early.id).note], [0, 'isi pagi']);

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
        // catatan berisi tautan panjang tanpa spasi tidak boleh melebarkan kartu
        vehicles[0].note = 'STNK: https://drive.google.com/file/d/1AbCdEfGhIjKlMnOpQrStUvWxYz0123456789/view';
        renderVehicleView(); await new Promise(r => setTimeout(r, 50));
        const card = document.querySelector('.vehicle-card').getBoundingClientRect();
        const note = document.querySelector('.vehicle-card__note').getBoundingClientRect();
        const btn = document.querySelector('.vehicle-card__actions .btn').getBoundingClientRect();
        return [card.right <= innerWidth, note.right <= card.right, btn.height >= 44, document.documentElement.scrollWidth <= innerWidth];
    });
    T.push({ n: 'HP: kartu motor muat (juga dengan tautan panjang di catatan), tombol ≥44px, tanpa geser samping', g: phone, w: [true, true, true, true], pass: JSON.stringify(phone) === JSON.stringify([true, true, true, true]) });

    let pass = 0;
    T.forEach(x => { if (x.pass) pass++; else console.log('FAIL', x.n, '\n  dapat :', JSON.stringify(x.g), '\n  harap :', JSON.stringify(x.w)); });
    if (errors.length) console.log('PAGE ERRORS:', errors);
    console.log(`${pass}/${T.length} lulus`);
    await b.close();
    process.exit(pass === T.length && !errors.length ? 0 : 1);
})();
