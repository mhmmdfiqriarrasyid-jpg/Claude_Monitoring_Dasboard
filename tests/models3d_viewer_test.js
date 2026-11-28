// 2.44.0 — the 3D viewer engine (models3d-viewer.js), on a small GLB: a rake whose parts sit four
// levels deep (Rake › Frame | Wheel L/R | Arm 1–3 › meshes; Arm n › Arm n Tines › two tines).
// It checks what the 3D Models page relies on: grouping by level, keys that stay the same from one
// load to the next, picking with the real mouse, disassembly order, explode, x-ray, isolate,
// snapshot, files it must refuse, and a clean dispose.
const { launch, BASE_URL } = require('./_env');

(async () => {
    // Headless Chromium draws WebGL in software (SwiftShader); these keep it on in builds that
    // would otherwise refuse it.
    const b = await launch({ args: ['--enable-unsafe-swiftshader', '--ignore-gpu-blocklist'] });
    const page = await b.newPage({ viewport: { width: 1000, height: 700 } });
    const errors = [];
    page.on('pageerror', e => errors.push(e.message));
    await page.goto(BASE_URL + '/tests/fixtures/m3_harness.html', { waitUntil: 'load' });
    const run = fn => page.evaluate(fn);
    const click = async (p, opts) => { await page.mouse.click(p.x, p.y, opts); await page.waitForTimeout(120); };

    // ---- load, levels, keys ----
    await run(async () => {
        const T = window.__T = [];
        window.t = (n, g, w) => T.push({ n, g, w, pass: JSON.stringify(g) === JSON.stringify(w) });
        window.wait = ms => new Promise(r => setTimeout(r, ms));
        const L = window.__log = { sel: [], hov: [], chg: 0 };
        const v = window.__v = new M3Viewer(document.getElementById('v'), {
            onSelect: k => L.sel.push(k), onHover: k => L.hov.push(k), onChange: () => L.chg++ });
        // Test helpers: where a part's meshes really are in the scene, and on screen.
        window.H = {
            meshes: k => v._byKey.get(k).entries.map(e => e.mesh),
            box: k => { const bx = new THREE.Box3(); H.meshes(k).forEach(m => { m.updateWorldMatrix(true, false); bx.expandByObject(m); }); return bx; },
            center: k => H.box(k).getCenter(new THREE.Vector3()),
            radius: k => H.box(k).getSize(new THREE.Vector3()).length() / 2,
            screen: k => {
                const c = H.center(k).project(v._camera), r = document.querySelector('#v canvas').getBoundingClientRect();
                return { x: r.left + (c.x + 1) / 2 * r.width, y: r.top + (1 - c.y) / 2 * r.height };
            },
            byName: n => { let f = null; v._world.traverse(o => { if (o.userData.name === n) f = o; }); return f; },
            near: (a, b2, eps) => Math.abs(a - b2) <= (eps || 1e-6) * Math.max(1, Math.abs(b2))
        };
        const buf = await (await fetch('m3_sample.glb')).arrayBuffer();
        window.__buf = buf;
        await v.load(buf);

        t('levels: 1, 2, 3 and mesh with their part counts (depth 4 = single meshes, so not listed)', v.levels,
            [{ level: 1, count: 1 }, { level: 2, count: 6 }, { level: 3, count: 13 }, { level: 'mesh', count: 16 }]);
        t('default level: the first with at least 2 parts (2)', v.level, 2);
        t('level 2: the six groups', v.parts.map(p => p.name).sort(), ['Arm 1', 'Arm 2', 'Arm 3', 'Frame', 'Wheel L', 'Wheel R']);
        t('keys are node index paths; names keep their spaces; path runs from the root',
            ['n0.0', 'n0.1', 'n0.5'].map(k => { const p = v.part(k); return [p.key, p.name, p.path]; }),
            [['n0.0', 'Frame', 'Rake › Frame'], ['n0.1', 'Wheel L', 'Rake › Wheel L'], ['n0.5', 'Arm 3', 'Rake › Arm 3']]);
        t('meshCount and triangles per part (box 12, cylinder 64)', ['n0.0', 'n0.1', 'n0.3'].map(k => [v.part(k).meshCount, v.part(k).triangles]),
            [[3, 88], [2, 128], [3, 36]]);
        t('part() of an unknown key is null; parts are copies', [v.part('n9'), (() => { v.parts[0].name = 'x'; return v.parts[0].name !== 'x'; })()], [null, true]);
        t('order: 1..n, parts listed in that order', v.parts.map(p => p.order), [1, 2, 3, 4, 5, 6]);
        // Distances measured in the scene itself, not taken from the viewer.
        const all = new THREE.Box3(); v.parts.forEach(p => all.union(H.box(p.key)));
        const mc = all.getCenter(new THREE.Vector3());
        window.__R = all.getSize(new THREE.Vector3()).length() / 2;
        window.__mc = mc;
        const d = v.parts.map(p => H.center(p.key).distanceTo(mc));
        t('disassembly order: farthest from the model centre first', d.every((x, i) => i === 0 || x <= d[i - 1] + 1e-6), true);
        t('mirror twins (same distance and size) go by key: Wheel L, then Wheel R', v.parts.slice(0, 2).map(p => p.name), ['Wheel L', 'Wheel R']);

        v.setLevel(3);
        t('level 3: a group below an arm is one part (its two tines)', [v.part('n0.3.1').name, v.part('n0.3.1').meshCount, v.part('n0.3.1').path],
            ['Arm 1 Tines', 2, 'Rake › Arm 1 › Arm 1 Tines']);
        v.setLevel('mesh');
        t('level mesh: every mesh is a part, keyed by its own path', [v.parts.length, v.part('n0.3.1.1').path],
            [16, 'Rake › Arm 1 › Arm 1 Tines › Tine 1b']);
        // Tyre and hub share their meshes with the other wheel; GLTFLoader finishes those in a
        // different order from the file, and the keys must still follow the file.
        t('keys follow the file order, also for nodes that share a mesh', ['n0.0.2', 'n0.1.0', 'n0.1.1', 'n0.2.0', 'n0.2.1'].map(k => v.part(k).name),
            ['Hitch', 'Tyre L', 'Hub L', 'Tyre R', 'Hub R']);
        v.setLevel(7);
        t('setLevel to a level the model does not have changes nothing', v.level, 'mesh');

        const snap = () => v.levels.map(l => { v.setLevel(l.level); return v.parts.map(p => [p.key, p.name, p.path, p.order]); });
        const first = JSON.stringify(snap());
        await v.load(new Uint8Array(buf.slice(0)));
        t('second load (from a Uint8Array) starts again at level 2', v.level, 2);
        t('same file → same keys, names, paths and order at every level', JSON.stringify(snap()) === first && first.length > 500, true);
        v.setLevel(2);
    });

    // ---- select and pick ----
    await run(async () => {
        const v = __v, L = __log;
        const c0 = L.chg;
        v.select('n0.1');
        t('select(key): onSelect, onChange, selected', [L.sel[L.sel.length - 1], L.chg - c0, v.selected], ['n0.1', 1, 'n0.1']);
        t('selected part glows; others do not', [H.meshes('n0.1')[0].material.emissive.getHex() !== 0, H.meshes('n0.0')[0].material.emissive.getHex()], [true, 0]);
        v.select('n0.1');
        t('selecting it again fires nothing', [L.sel.length, L.chg - c0], [1, 1]);
        v.select(null);
        t('select(null) clears', [L.sel[L.sel.length - 1], v.selected, H.meshes('n0.1')[0].material.emissive.getHex()], [null, null, 0]);
        v.isolate('n0.1');
        await wait(150);
        window.__p = H.screen('n0.1');
    });
    await click(await run(() => __p));
    await run(() => t('click on a part (others hidden) selects it', [__v.selected, __log.sel[__log.sel.length - 1]], ['n0.1', 'n0.1']));
    await click({ x: 8, y: 8 });
    await run(async () => {
        t('click on empty space selects nothing', [__v.selected, __log.sel[__log.sel.length - 1]], [null, null]);
        __v.isolate(null);
        __v.view('top');
        await wait(700);
        window.__p = H.screen('n0.0');
    });
    await click(await run(() => __p));
    await run(() => t('top view, everything shown: a click on the drawbar picks the Frame', __v.selected, 'n0.0'));
    await run(async () => { __v.select(null); __v.isolate('n0.0'); __v.setHidden('n0.0', true); await wait(80); });
    await click(await run(() => __p));
    await run(() => t('hidden parts are not picked', __v.selected, null));
    await run(async () => { __v.isolate(null); await wait(80); });
    // A drag that starts on a part orbits the camera and selects nothing.
    const p0 = await run(() => __p);
    await page.mouse.move(p0.x, p0.y);
    await page.mouse.down();
    await page.mouse.move(p0.x + 30, p0.y + 20, { steps: 5 });
    await page.mouse.up();
    await page.waitForTimeout(150);
    await run(() => t('a drag (≥ 6 px) is an orbit, not a pick', __v.selected, null));
    await run(async () => { __v.view('top'); await wait(800); window.__p = H.screen('n0.0'); __log.hov.length = 0; });
    const ph = await run(() => __p);
    await page.mouse.move(ph.x - 3, ph.y - 3);
    await page.mouse.move(ph.x, ph.y, { steps: 2 });
    await page.waitForTimeout(150);
    await run(() => t('mouse hover reports the part and tints it lightly',
        [__log.hov.includes('n0.0'), H.meshes('n0.0')[0].material.emissive.getHex() !== 0, __v.selected], [true, true, null]));
    await page.mouse.move(8, 8, { steps: 2 });
    await page.waitForTimeout(150);
    await run(() => t('leaving the part reports null and clears the tint', [__log.hov[__log.hov.length - 1], H.meshes('n0.0')[0].material.emissive.getHex()], [null, 0]));

    // ---- disassembly ----
    await run(async () => {
        const v = __v, ord = v.parts.map(p => p.key);
        const det = () => v.parts.filter(p => p.detached).map(p => p.order);
        v.toggle(ord[2]);
        t('toggle detaches', v.part(ord[2]).detached, true);
        v.toggle(ord[2]);
        t('toggle again re-attaches', v.part(ord[2]).detached, false);
        v.detachAll();
        t('detachAll', det(), [1, 2, 3, 4, 5, 6]);
        v.attachAll();
        t('attachAll', det(), []);
        await wait(500);
        const w0 = H.center('n0.1'), rW = H.radius('n0.1');
        const k1 = v.step(1), k2 = v.step(1);
        t('step(+1) twice: order 1, then order 2', [k1, k2, det()], [ord[0], ord[1], [1, 2]]);
        const k3 = v.step(-1);
        t('step(-1): the most recently detached goes back', [k3, det()], [ord[1], [1]]);
        await wait(550);
        // Wheel L (order 1) is out: 1.6 × (R·0.55 + its radius), away from the model centre.
        const moved = H.center('n0.1').sub(w0), dir = w0.clone().sub(__mc).normalize();
        t('a detached part sits 1.6 explode vectors out, along centre → part',
            [H.near(moved.length(), 1.6 * (__R * 0.55 + rW), 1e-4), moved.normalize().dot(dir) > 0.9999], [true, true]);
        v.setDetached(ord[0], false);
        await wait(550);
        t('and comes back to where it was', H.center('n0.1').distanceTo(w0) < 1e-9, true);
        v.setHidden('n0.3', true);
        t('step(+1) and detachAll skip hidden parts', [(v.detachAll(), v.part('n0.3').detached), det().length], [false, 5]);
        v.attachAll(); v.setHidden('n0.3', false);
        await wait(550);
    });

    // ---- explode ----
    await run(async () => {
        const v = __v, f0 = H.center('n0.0'), w0 = H.center('n0.1'), vf = __R * 0.55 + H.radius('n0.0'), vw = __R * 0.55 + H.radius('n0.1');
        v.setExplode(1);
        t('setExplode(1) moves an attached part by its explode vector', [H.near(H.center('n0.0').distanceTo(f0), vf, 1e-4), v.explode], [true, 1]);
        v.setDetached('n0.1', true);
        await wait(550);
        v.setExplode(0.5);
        t('explode 0.5: attached parts at half, a detached part stays at 1.6',
            [H.near(H.center('n0.0').distanceTo(f0), 0.5 * vf, 1e-4), H.near(H.center('n0.1').distanceTo(w0), 1.6 * vw, 1e-4), v.part('n0.1').detached], [true, true, true]);
        v.setExplode(0);
        t('setExplode(0): back to the original place', H.center('n0.0').distanceTo(f0) < 1e-9, true);
        v.attachAll();
        await wait(550);
        t('attachAll brings the detached part home', H.center('n0.1').distanceTo(w0) < 1e-9, true);
    });

    // ---- x-ray, materials, isolate, change events ----
    await run(async () => {
        const v = __v, L = __log, mat = k => H.meshes(k)[0].material;
        v.select('n0.0');
        v.setXray(true);
        t('x-ray: other parts transparent at 0.18 with no depth write', [mat('n0.3').transparent, mat('n0.3').opacity, mat('n0.3').depthWrite, v.xray], [true, 0.18, false, true]);
        t('x-ray: the selected part stays solid', [mat('n0.0').transparent, mat('n0.0').opacity, mat('n0.0').depthWrite], [false, 1, true]);
        v.setXray(false);
        t('x-ray off: materials as loaded', [mat('n0.3').transparent, mat('n0.3').opacity, mat('n0.3').depthWrite], [false, 1, true]);
        const beam = H.byName('Beam'), drawbar = H.byName('Drawbar'), bar = H.byName('Arm 1 Bar');
        t('the file shares "Steel"; every mesh got its own copy, same name',
            [beam.material !== drawbar.material, beam.material !== bar.material, beam.material.name, bar.material.name], [true, true, 'Steel', 'Steel']);
        t('tinting the selected part leaves another that shared its material untouched', [beam.material.emissive.getHex() !== 0, bar.material.emissive.getHex()], [true, 0]);
        v.setLevel('mesh');
        v.select('n0.0.0');
        t('mesh level: selecting the beam does not tint the drawbar', [beam.material.emissive.getHex() !== 0, drawbar.material.emissive.getHex()], [true, 0]);
        v.setLevel(2);
        t('setLevel clears the selection (onSelect null)', [v.selected, L.sel[L.sel.length - 1], beam.material.emissive.getHex()], [null, null, 0]);

        v.isolate('n0.2');
        t('isolate: only that part is shown', v.parts.filter(p => !p.hidden).map(p => p.key), ['n0.2']);
        t('isolate: the others are not drawn', H.meshes('n0.0').concat(H.meshes('n0.3')).every(m => m.material.visible === false), true);
        v.isolate(null);
        t('isolate(null) shows everything again', [v.parts.every(p => !p.hidden), H.meshes('n0.0').every(m => m.material.visible)], [true, true]);

        v.toggle('n0.1'); v.setExplode(0.4); v.setHidden('n0.2', true);
        v.setLevel(3); v.setLevel(2);
        t('setLevel resets detach, explode and visibility', [v.parts.some(p => p.detached || p.hidden), v.explode], [false, 0]);

        const c0 = L.chg;
        v.setXray(true); v.setXray(false); v.isolate('n0.1'); v.isolate(null); v.setExplode(0.3); v.setExplode(0);
        v.toggle('n0.1'); v.toggle('n0.1'); v.select('n0.2'); v.select(null); v.setLevel(3); v.setLevel(2);
        t('onChange fires once per state change', L.chg - c0, 12);
        await wait(550);
    });

    // ---- camera ----
    await run(async () => {
        const v = __v;
        v.view('front');
        await wait(650);
        const dir = v._camera.position.clone().sub(v._controls.target).normalize();
        t('view(front) looks from +Z', [H.near(dir.z, 1, 1e-6), Math.abs(dir.x) < 1e-6], [true, true]);
        v.focus('n0.3');
        await wait(650);
        t('focus(key) aims at the part and frames it', [v._controls.target.distanceTo(H.center('n0.3')) < 1e-6,
            v._camera.position.distanceTo(v._controls.target) < __R], [true, true]);
        v.view('iso');
        await wait(650);
    });

    // ---- render on demand, snapshot ----
    await run(async () => {
        const v = __v, info = v._renderer.info.render;
        await wait(400);
        const f0 = info.frame;
        await wait(400);
        t('idle: no frame drawn while nothing moves', [info.frame - f0, v._raf], [0, 0]);
        v.setAutoRotate(true);
        await wait(300);
        t('auto-rotate keeps drawing', [info.frame - f0 > 3, v.autoRotate], [true, true]);
        v.setAutoRotate(false);
        v.setGrid(false);
        t('grid toggles', [v.grid, v._grid.visible], [false, false]);
        v.setGrid(true);
        await wait(300);
        const s = v.snapshot();
        t('snapshot: a JPEG data URL', [s.startsWith('data:image/jpeg;base64,'), s.length > 1000], [true, true]);
        const corner = async src => {
            const img = new Image();
            img.src = src;
            await img.decode();
            const c = document.createElement('canvas'); c.width = img.naturalWidth; c.height = img.naturalHeight;
            const g = c.getContext('2d'); g.drawImage(img, 0, 0);
            return { w: img.naturalWidth, px: [...g.getImageData(2, 2, 1, 1).data.slice(0, 3)] };
        };
        const close = (a, b2) => a.every((x, i) => Math.abs(x - b2[i]) <= 6);
        const s1 = await corner(v.snapshot(320));
        t('snapshot(320) is 320 px wide, on the page background (#eef1f4)', [s1.w, close(s1.px, [238, 241, 244])], [320, true]);
        // A gradient stage (like the 3D Models page): its average colour, not the body's.
        document.getElementById('v').style.background = 'radial-gradient(rgb(20, 40, 60), rgb(40, 60, 80))';
        const s2 = await corner(v.snapshot(320));
        t('snapshot on a gradient background uses its average colour', close(s2.px, [30, 50, 70]), true);
        document.getElementById('v').style.background = '';
    });

    // ---- files it must refuse ----
    await run(async () => {
        const v = __v, before = JSON.stringify(v.parts);
        const glb = json => {
            const j = new TextEncoder().encode(JSON.stringify(json)), pad = (4 - j.length % 4) % 4, total = 20 + j.length + pad;
            const ab = new ArrayBuffer(total), dv = new DataView(ab);
            dv.setUint32(0, 0x46546C67, true); dv.setUint32(4, 2, true); dv.setUint32(8, total, true);
            dv.setUint32(12, j.length + pad, true); dv.setUint32(16, 0x4E4F534A, true);
            new Uint8Array(ab, 20).set(j);
            for (let i = 0; i < pad; i++) dv.setUint8(20 + j.length + i, 0x20);
            return ab;
        };
        const why = data => v.load(data).then(() => 'loaded', e => e.message);
        t('non-GLB bytes: "Not a GLB file"', await why(new TextEncoder().encode('solid cube\nfacet normal 0 0 1\n')), 'Not a GLB file');
        t('nothing at all: "Not a GLB file"', await why(null), 'Not a GLB file');
        t('Draco is refused in words', await why(glb({ asset: { version: '2.0' }, extensionsUsed: ['KHR_draco_mesh_compression'], extensionsRequired: ['KHR_draco_mesh_compression'] })),
            'This model uses Draco compression, which this viewer does not read yet');
        t('meshopt is refused in words', await why(glb({ asset: { version: '2.0' }, extensionsUsed: ['EXT_meshopt_compression'], extensionsRequired: ['EXT_meshopt_compression'] })),
            'This model uses meshopt compression, which this viewer does not read yet');
        t('a refused file leaves the current model as it was', JSON.stringify(v.parts), before);
    });

    // ---- loadObject: shallow meshes and a mesh nested in a mesh ----
    await run(async () => {
        const v = __v, L = __log;
        const box = (name, x, y, z, s) => { const m = new THREE.Mesh(new THREE.BoxGeometry(s, s, s), new THREE.MeshStandardMaterial()); m.name = name; m.position.set(x, y, z); return m; };
        const root = new THREE.Group();
        const a = box('A', -3, 0, 0, 1), e = box('E', 0, 1.5, 0, 0.5);   // E rides on A
        a.add(e);
        const g = new THREE.Group(); g.name = 'G';
        const h = new THREE.Group();                               // no name: named after its first mesh
        h.add(box('C', 2, 0, 1, 0.6), box('', 2, 0, -1, 0.6));
        g.add(box('B', 3, 0, 0, 0.8), h);
        root.add(a, g);
        const c0 = L.chg;
        v.loadObject(root);
        t('loadObject: levels and default level', [v.levels, v.level], [[{ level: 1, count: 2 }, { level: 2, count: 4 }, { level: 'mesh', count: 5 }], 1]);
        t('loadObject fires onChange', L.chg - c0, 1);
        v.setLevel(2);
        t('level 2: a mesh higher up is its own part; an unnamed group takes its first mesh name',
            v.parts.map(p => [p.key, p.name]).sort(), [['n0', 'A'], ['n0.0', 'E'], ['n1.0', 'B'], ['n1.1', 'C']]);
        v.setLevel('mesh');
        t('a mesh with no name anywhere is "Part <n>"', v.part('n1.1.1').name, 'Part 5');
        const e0 = H.center('n0.0');
        v.setDetached('n0', true);
        await wait(550);
        t('detaching a mesh leaves the part nested under it in place', [H.center('n0').x < -3.5, H.center('n0.0').distanceTo(e0) < 1e-9], [true, true]);
        v.setDetached('n0.0', true);
        await wait(550);
        const want = H.center('n0.0').sub(e0).length();
        t('the nested part moves by its own vector only', H.near(want, 1.6 * (v._radius * 0.55 + H.radius('n0.0')), 1e-4), true);
        v.setHidden('n0', true);
        t('hiding a mesh does not hide the part nested under it', [H.meshes('n0')[0].material.visible, H.meshes('n0.0')[0].material.visible], [false, true]);
    });

    // ---- dispose ----
    const T = await run(async () => {
        const v = __v, mem = v._renderer.info.memory;
        t('GPU memory in use before dispose', mem.geometries > 0, true);
        v.dispose();
        await wait(100);
        t('dispose: no canvas left in the container', document.querySelectorAll('#v canvas').length, 0);
        t('dispose: geometries and textures released', [mem.geometries, mem.textures], [0, 0]);
        t('dispose: no frame pending; later calls are harmless', (() => {
            try { v.dispose(); v.select('n0'); v.setExplode(1); v.view('iso'); v.step(1); return v._raf; } catch (e) { return e.message; }
        })(), 0);
        t('dispose: load afterwards rejects', await v.load(__buf).then(() => 'loaded', e => e.message), 'This viewer is closed');
        return __T;
    });

    let pass = 0;
    T.forEach(x => { if (x.pass) pass++; else console.log('FAIL', x.n, '\n  got   :', JSON.stringify(x.g), '\n  wanted:', JSON.stringify(x.w)); });
    if (errors.length) console.log('PAGE ERRORS:', errors);
    console.log(`${pass}/${T.length} lulus`);
    await b.close();
    process.exit(pass === T.length && !errors.length ? 0 : 1);
})();
