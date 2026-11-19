// 3D viewer for GLB models (2.44.0). The 3D Models page draws its lists, inspector and toolbars
// around it; inside the container there is only a <canvas>, and the viewer answers through three
// callbacks: onSelect(key), onHover(key, x, y) and onChange().
//   · Parts: meshes are grouped under their ancestor node at a chosen depth (a "level"). A part's
//     key is that node's index path ('n0.3.1'), so the same file always gives the same keys —
//     the page stores its notes under them.
//   · Disassembly: a part slides out along the line from the model centre through its own
//     centre; step(+1) takes the outermost attached part first, as a fitter would.
//   · It renders only while something moves, so a big CAD file costs nothing while it sits still.
// Needs vendor/three (three.min.js, GLTFLoader.js, OrbitControls.js — r147, no Draco) loaded first.
'use strict';

(() => {
    const PICK_SLOP = 6;            // px a press may travel and still be a click; more is an orbit drag
    const DETACH_MS = 400;
    const CAMERA_MS = 450;
    const DETACH_OUT = 1.6;         // a detached part sits this many explode vectors out
    const XRAY_OPACITY = 0.18;
    const MAX_DEFAULT_PARTS = 400;  // a longer parts list is no use as a first view
    const TINT = 0x10b981;
    const HOVER_GLOW = 0.16, SELECT_GLOW = 0.45;
    const VIEWS = { iso: [1, 0.7, 1], front: [0, 0, 1], side: [1, 0, 0], top: [0, 1, 0] };
    const GLB_MAGIC = 0x46546C67, JSON_CHUNK = 0x4E4F534A;

    const ease = k => k < 0.5 ? 4 * k * k * k : 1 - Math.pow(2 - 2 * k, 3) / 2;
    const wrapAngle = a => a - Math.PI * 2 * Math.round(a / (Math.PI * 2));
    // GLTFLoader keeps the file's own name in userData.name; .name has spaces turned into "_".
    const nameOf = o => (typeof o.userData.name === 'string' && o.userData.name) || o.name || '';

    // GLTFLoader wants an ArrayBuffer of this window. A view over a whole buffer is used as it is:
    // a big CAD file should not sit in memory twice.
    function toBuffer(d) {
        if (d instanceof ArrayBuffer) return d;
        if (ArrayBuffer.isView(d)) {
            if (d.buffer instanceof ArrayBuffer && d.byteOffset === 0 && d.byteLength === d.buffer.byteLength) return d.buffer;
            return new Uint8Array(d.buffer, d.byteOffset, d.byteLength).slice().buffer;
        }
        if (Object.prototype.toString.call(d) === '[object ArrayBuffer]') return new Uint8Array(d).slice().buffer;
        return null;
    }

    // Reads the JSON chunk before GLTFLoader does, so a file this build cannot open fails with a
    // sentence the page can show instead of a loader stack message.
    function checkGlb(buf) {
        const dv = new DataView(buf);
        if (buf.byteLength < 12 || dv.getUint32(0, true) !== GLB_MAGIC) throw new Error('Not a GLB file');
        if (dv.getUint32(4, true) !== 2) throw new Error('This is an old glTF 1.0 file; export it again as glTF 2.0 (.glb)');
        let json = null;
        try {
            const len = dv.getUint32(12, true);
            if (dv.getUint32(16, true) === JSON_CHUNK && 20 + len <= buf.byteLength) {
                json = JSON.parse(new TextDecoder().decode(new Uint8Array(buf, 20, len)));
            }
        } catch (_) { json = null; }
        if (!json || typeof json !== 'object') throw new Error('This GLB file is damaged: its model description cannot be read');
        const list = v => Array.isArray(v) ? v : [];
        const required = new Set(list(json.extensionsRequired));
        // GLTFLoader refuses Draco even when it is only "used"; meshopt and KTX2 have a fallback
        // unless the file says it cannot do without them.
        if (required.has('KHR_draco_mesh_compression') || list(json.extensionsUsed).includes('KHR_draco_mesh_compression')) {
            throw new Error('This model uses Draco compression, which this viewer does not read yet');
        }
        if (required.has('EXT_meshopt_compression')) throw new Error('This model uses meshopt compression, which this viewer does not read yet');
        if (required.has('KHR_texture_basisu')) throw new Error('This model uses KTX2 textures, which this viewer does not read yet');
        if (list(json.buffers).some(b => b && typeof b.uri === 'string' && !/^data:/i.test(b.uri))) {
            throw new Error('This model needs separate .bin files; export it as one self-contained .glb');
        }
    }

    // GLTFLoader adds a node to its parent once the node's own meshes and textures are ready, so
    // siblings come out in an order that depends on loading, not on the file. Put them back in
    // file order: part keys are index paths and must not move.
    // Node indices come from the parser's own node cache: r147's `associations` shares one record
    // between a mesh used twice and its copy, so there the second node overwrites the first.
    async function fileOrder(gltf) {
        const parser = gltf.parser, json = parser.json, nodeOf = new Map();
        await Promise.all((json.nodes || []).map((_, i) => {
            const p = parser.cache.get('node:' + i);
            return p && p.then(o => { nodeOf.set(o, i); });
        }));
        gltf.scene.traverse(o => {
            const n = nodeOf.get(o);
            const want = o === gltf.scene ? ((json.scenes || [])[json.scene || 0] || {}).nodes : n !== undefined ? json.nodes[n].children : null;
            if (!Array.isArray(want) || o.children.length < 2) return;
            const pos = new Map(want.map((id, i) => [id, i]));
            // A node's own primitives and camera were added first, synchronously: they keep their place.
            const rank = new Map(o.children.map((c, i) => {
                const id = nodeOf.get(c);
                return [c, pos.has(id) ? o.children.length + pos.get(id) : i];
            }));
            o.children.sort((a, b) => rank.get(a) - rank.get(b));
        });
    }

    // Frees what a model holds on the GPU, each geometry, material and texture once.
    function disposeTree(root) {
        const geos = new Set(), mats = new Set(), texs = new Set();
        root.traverse(o => {
            if (o.geometry) geos.add(o.geometry);
            if (o.material) [].concat(o.material).forEach(m => m && mats.add(m));
            if (o.isInstancedMesh && o.dispose) o.dispose();
        });
        mats.forEach(m => {
            for (const k in m) { const v = m[k]; if (v && v.isTexture) texs.add(v); }
            m.dispose();
        });
        geos.forEach(g => g.dispose());
        texs.forEach(t => {
            t.dispose();
            if (t.image && typeof t.image.close === 'function') t.image.close();   // ImageBitmap memory
        });
    }

    // A grey room with three soft lamps, pre-blurred into an environment map: CAD exports are
    // often fully metallic, and metal with nothing to reflect renders black.
    function makeEnvironment(renderer) {
        const T = THREE, pm = new T.PMREMGenerator(renderer), room = new T.Scene(), box = new T.BoxGeometry(1, 1, 1), mats = [];
        const add = (k, pos, size, side) => {
            const m = new T.MeshBasicMaterial({ side });
            m.color.setScalar(k);
            mats.push(m);
            const o = new T.Mesh(box, m);
            o.position.fromArray(pos);
            o.scale.fromArray(size);
            room.add(o);
        };
        add(0.45, [0, 3, 0], [16, 12, 16], T.BackSide);  // walls, floor, ceiling
        add(4, [0, 8.9, 0], [7, 0.1, 7], T.FrontSide);    // ceiling panel
        add(2.5, [-7.9, 3, 2], [0.1, 4, 6], T.FrontSide); // window, left
        add(1.5, [7.9, 3, -3], [0.1, 4, 6], T.FrontSide); // window, right
        const rt = pm.fromScene(room, 0.04);
        box.dispose();
        mats.forEach(m => m.dispose());
        pm.dispose();
        return rt;
    }

    function niceStep(x) {
        const p = Math.pow(10, Math.floor(Math.log10(x)));
        return x <= p ? p : x <= 2 * p ? 2 * p : x <= 5 * p ? 5 * p : 10 * p;
    }

    const pub = p => ({ key: p.key, name: p.name, path: p.path, meshCount: p.entries.length, triangles: p.tris,
        detached: p.detached, hidden: p.hidden, order: p.order });

    class M3Viewer {
        constructor(container, opts) {
            const T = window.THREE;
            if (!T || !T.GLTFLoader || !T.OrbitControls) throw new Error('The 3D viewer files did not load');
            if (!container || typeof container.appendChild !== 'function') throw new Error('M3Viewer needs a container element');
            this._el = container;
            this._cb = opts || {};
            this._disposed = false;
            this._setPos = false;
            let renderer;
            try {
                renderer = new T.WebGLRenderer({ antialias: true, alpha: true, powerPreference: 'high-performance' });
            } catch (_) {
                throw new Error('This browser cannot show 3D models (WebGL is off or not supported)');
            }
            this._renderer = renderer;
            renderer.setPixelRatio(Math.min(window.devicePixelRatio || 1, 2));
            renderer.setClearColor(0x000000, 0);         // the page's own background shows through
            renderer.outputEncoding = T.sRGBEncoding;    // glTF colours are linear
            const cv = this._canvas = renderer.domElement;
            // Absolute, so the canvas never sizes its container: under height:auto every resize
            // would grow it again. A static container becomes its positioning box.
            cv.style.cssText = 'position:absolute;left:0;top:0;width:100%;height:100%;display:block;outline:none';
            if (getComputedStyle(container).position === 'static') { container.style.position = 'relative'; this._setPos = true; }
            container.appendChild(cv);

            const scene = this._scene = new T.Scene();
            const cam = this._camera = new T.PerspectiveCamera(40, 1, 0.01, 100);
            scene.add(cam);
            scene.add(new T.HemisphereLight(0xffffff, 0x8a8f98, 0.35));
            // The key light rides with the camera (upper left, behind the eye), so the side the
            // user looks at is lit whichever way the model is turned.
            const sun = new T.DirectionalLight(0xffffff, 0.75);
            sun.position.set(-0.6, 1, 0.6);
            sun.target.position.set(0, 0, -1);
            cam.add(sun, sun.target);
            this._world = new T.Group();
            scene.add(this._world);

            const ctl = this._controls = new T.OrbitControls(cam, cv);
            ctl.enableDamping = true;
            ctl.dampingFactor = 0.09;
            this._onControls = () => this._invalidate();
            this._onGrab = () => this._stopCamera();    // the user takes the camera back mid-flight
            ctl.addEventListener('change', this._onControls);
            ctl.addEventListener('start', this._onGrab);

            // Scratch objects, so frames and pointer moves allocate nothing of ours.
            this._v1 = new T.Vector3(); this._v2 = new T.Vector3(); this._v3 = new T.Vector3();
            this._b1 = new T.Box3(); this._b2 = new T.Box3();
            this._sph = new T.Spherical();
            this._ndc = new T.Vector2();
            this._ray = new T.Raycaster();
            this._hits = [];
            this._tint = new T.Color(TINT);
            this._center = new T.Vector3(); this._modelBox = new T.Box3(); this._radius = 1;

            this._model = null; this._entries = []; this._keyOf = new Map();
            this._levels = []; this._level = null;
            this._parts = []; this._byKey = new Map(); this._partOf = new Map(); this._pick = [];
            this._sel = null; this._hov = null;
            this._explode = 0; this._xray = false; this._gridOn = true; this._grid = null; this._env = null;
            this._stack = []; this._anim = new Set(); this._cam = null;
            this._downs = new Map(); this._multi = false;
            this._hoverDue = false; this._hx = 0; this._hy = 0;
            this._raf = 0; this._dirty = true; this._moved = false; this._seq = 0; this._w = 0; this._h = 0;
            this._frame = t => this._tick(t);

            this._on = {
                pointerdown: e => this._down(e), pointerup: e => this._up(e), pointercancel: e => this._cancel(e),
                pointermove: e => this._move(e), pointerleave: () => this._leave(),
                webglcontextrestored: () => this._invalidate()
            };
            for (const k in this._on) cv.addEventListener(k, this._on[k]);
            this._ro = new ResizeObserver(() => this._resize());
            this._ro.observe(container);
            this._resize();
        }

        // ---------- loading ----------

        async load(data) {
            if (this._disposed) throw new Error('This viewer is closed');
            const buf = toBuffer(data);
            if (!buf) throw new Error('Not a GLB file');
            checkGlb(buf);
            const seq = ++this._seq;
            const manager = new THREE.LoadingManager();
            // Only what is inside the file: a texture pointing at a web address would make
            // everyone who opens the model request it.
            manager.setURLModifier(u => /^(data|blob):/i.test(u) ? u : 'data:,');
            let gltf;
            try {
                gltf = await new Promise((resolve, reject) => new THREE.GLTFLoader(manager).parse(buf, '', resolve, reject));
            } catch (e) {
                const why = String((e && e.message) || e || '').replace(/^THREE\.GLTFLoader:\s*/, '');
                throw new Error('This GLB file could not be read' + (why ? ': ' + why : ''));
            }
            await fileOrder(gltf);
            // A newer load (or dispose) came in while this one parsed: it wins.
            if (seq !== this._seq || this._disposed) { disposeTree(gltf.scene); return; }
            this._install(gltf.scene);
        }

        loadObject(object) {
            if (this._disposed) throw new Error('This viewer is closed');
            if (!object || !object.isObject3D) throw new Error('loadObject needs a THREE.Object3D');
            this._seq++;    // a GLB still parsing must not replace this one
            this._install(object);
        }

        _install(root) {
            const hadSel = !!this._sel, hadHov = !!this._hov;
            this._unload();
            let model = root;
            if (root.isMesh) { model = new THREE.Group(); model.add(root); }   // a lone mesh is a one-part model
            this._model = model;
            this._world.add(model);
            model.updateMatrixWorld(true);

            // One walk: index-path keys, every mesh's ancestor chain, its world box and own materials.
            const entries = [], keys = new Map();
            const walk = (o, key, chain) => o.children.forEach((c, i) => {
                const k = key ? key + '.' + i : 'n' + i, ch = chain.concat(c);
                keys.set(c, k);
                if (c.isMesh && c.material && c.geometry && c.geometry.attributes.position) entries.push(this._entry(c, ch));
                walk(c, k, ch);
            });
            walk(model, '', []);
            this._entries = entries;
            this._keyOf = keys;

            const box = this._modelBox.makeEmpty();
            entries.forEach(e => box.union(e.box));
            if (box.isEmpty()) box.setFromObject(model);
            if (box.isEmpty()) box.set(this._v1.set(-1, -1, -1), this._v2.set(1, 1, 1));
            box.getCenter(this._center);
            this._radius = Math.max(box.getSize(this._v1).length() / 2, 1e-6);

            // Levels: each depth that splits the model further, until parts are single meshes.
            const n = entries.length, depth = entries.reduce((d, e) => Math.max(d, e.chain.length), 0);
            const levels = [];
            for (let L = 1, prev = 0; L <= depth; L++) {
                const s = new Set();
                entries.forEach(e => s.add(e.chain[Math.min(L, e.chain.length) - 1]));
                if (L === 1 || (s.size > prev && s.size < n)) levels.push({ level: L, count: s.size });
                prev = s.size;
                if (s.size === n) break;
            }
            if (n) levels.push({ level: 'mesh', count: n });
            this._levels = levels;
            let def = levels.find(l => l.count >= 2) || levels[0];
            if (def && def.count > MAX_DEFAULT_PARTS) def = levels.find(l => l.count <= MAX_DEFAULT_PARTS) || levels[0];
            this._level = def ? def.level : null;

            this._makeGrid();
            if (!this._env) this._env = makeEnvironment(this._renderer);
            this._scene.environment = this._env.texture;
            this._controls.minDistance = this._radius * 1e-4;
            this._controls.maxDistance = this._radius * 40;
            this._buildParts();
            this._parts.forEach(p => this._paint(p));
            this._fly(this._center, this._fitDistance(this._radius), VIEWS.iso, 0);
            if (hadHov) this._emit('onHover', null, 0, 0);
            if (hadSel) this._emit('onSelect', null);
            this._emit('onChange');
        }

        _entry(mesh, chain) {
            const g = mesh.geometry;
            if (!g.boundingBox) g.computeBoundingBox();
            const box = new THREE.Box3();
            if (mesh.isInstancedMesh) {
                const m = new THREE.Matrix4();
                for (let i = 0; i < mesh.count; i++) { mesh.getMatrixAt(i, m); box.union(this._b1.copy(g.boundingBox).applyMatrix4(m)); }
            } else box.copy(g.boundingBox);
            box.applyMatrix4(mesh.matrixWorld);
            const tris = Math.floor((g.index ? g.index.count : g.attributes.position.count) / 3) * (mesh.isInstancedMesh ? mesh.count : 1);
            // Its own copy of every material: tinting one part must never tint another that shared it.
            const mats = [].concat(mesh.material).map(m => m.clone());
            mesh.material = Array.isArray(mesh.material) ? mats : mats[0];
            const base = mats.map(m => ({ emissive: m.emissive ? m.emissive.clone() : null, ei: m.emissiveIntensity,
                color: m.color ? m.color.clone() : null, transparent: m.transparent, opacity: m.opacity, depthWrite: m.depthWrite }));
            return { mesh, chain, box, tris, mats, base };
        }

        _unload() {
            if (this._grid) {
                this._scene.remove(this._grid);
                this._grid.geometry.dispose();
                this._grid.material.dispose();
                this._grid = null;
            }
            if (!this._model) return;
            this._world.remove(this._model);
            disposeTree(this._model);
            this._model = null; this._entries = []; this._keyOf = new Map(); this._levels = []; this._level = null;
            this._parts = []; this._byKey = new Map(); this._partOf = new Map(); this._pick = [];
            this._sel = this._hov = null; this._stack = []; this._anim.clear(); this._explode = 0;
            this._canvas.style.cursor = '';
        }

        _makeGrid() {
            const b = this._modelBox, s = b.getSize(this._v1), span = Math.max(s.x, s.z, this._radius * 0.5) * 1.5;
            const step = niceStep(span / 20), n = 2 * Math.ceil(span / step / 2);
            const g = this._grid = new THREE.GridHelper(step * n, n, 0x7c8591, 0xa3abb5);
            g.material.transparent = true;
            g.material.opacity = 0.45;
            g.material.depthWrite = false;
            // Just under the model, so the floor never cuts through its lowest face.
            g.position.set(this._center.x, b.min.y - this._radius * 0.002, this._center.z);
            g.visible = this._gridOn;
            this._scene.add(g);
        }

        // ---------- parts ----------

        get levels() { return this._levels.map(l => ({ level: l.level, count: l.count })); }
        get level() { return this._level; }
        get parts() { return this._parts.map(pub); }
        part(key) { const p = this._byKey.get(key); return p ? pub(p) : null; }
        get selected() { return this._sel ? this._sel.key : null; }
        get xray() { return this._xray; }
        get explode() { return this._explode; }
        get autoRotate() { return this._controls.autoRotate; }
        get grid() { return this._gridOn; }

        setLevel(level) {
            if (this._disposed || level === this._level || !this._levels.some(l => l.level === level)) return;
            const hadSel = !!this._sel;
            this._setHover(null, 0, 0);
            this._sel = null;
            this._level = level;
            this._buildParts();
            this._parts.forEach(p => this._paint(p));
            this._invalidate();
            if (hadSel) this._emit('onSelect', null);
            this._emit('onChange');
        }

        // Groups the meshes for the current level: under their ancestor at that depth, or alone
        // when they sit higher up. Resets detach, explode and visibility.
        _buildParts() {
            const T = THREE, L = this._level;
            this._parts.forEach(p => p.h.position.copy(p.pos0));
            this._anim.clear(); this._stack = []; this._explode = 0; this._moved = true;
            const byHandle = new Map(), list = [];
            for (const e of this._entries) {
                const h = L === 'mesh' ? e.mesh : e.chain[Math.min(L, e.chain.length) - 1];
                let p = byHandle.get(h);
                if (!p) {
                    p = { h, key: this._keyOf.get(h), entries: [], box: new T.Box3(), tris: 0, center: new T.Vector3(), radius: 0, dist: 0,
                        vec: new T.Vector3(), off: new T.Vector3(), pos0: h.position.clone(), toLocal: new T.Matrix3(),
                        anc: null, deps: [], a: 0, aFrom: 0, aTo: 0, t0: 0, detached: false, hidden: false, order: 0, name: '', path: '' };
                    byHandle.set(h, p);
                    list.push(p);
                }
                p.entries.push(e);
                p.box.union(e.box);
                p.tris += e.tris;
            }
            const R = this._radius, d = this._v1;
            list.forEach((p, i) => {
                if (!p.box.isEmpty()) { p.box.getCenter(p.center); p.radius = p.box.getSize(d).length() / 2; } else p.center.copy(this._center);
                d.subVectors(p.center, this._center);
                p.dist = d.length();
                if (p.dist < R * 1e-6) d.set(0, 1, 0); else d.divideScalar(p.dist);
                p.vec.copy(d).multiplyScalar(R * 0.55 + p.radius);
                // Offsets are world vectors; the handle moves in its parent's space.
                p.toLocal.setFromMatrix4(p.h.parent.matrixWorld).invert();
                p.name = nameOf(p.h) || nameOf(p.entries[0].mesh) || 'Part ' + (i + 1);
                const chain = p.entries[0].chain, names = chain.slice(0, chain.indexOf(p.h) + 1).map(nameOf).filter(Boolean);
                if (!nameOf(p.h)) names.push(p.name);
                p.path = names.join(' › ');
                // A mesh that is its own part can hold other parts below it: they inherit its move.
                for (let o = p.h.parent; o && o !== this._model; o = o.parent) {
                    const q = byHandle.get(o);
                    if (q) { p.anc = q; q.deps.push(p); break; }
                }
            });
            // Disassembly order: farthest from the centre first; mirror twins (same distance) by
            // size, then by key, so the order never depends on rounding.
            const q = R * 1e-4;
            list.sort((a, b) => (Math.round(b.dist / q) - Math.round(a.dist / q)) || (a.radius - b.radius) || (a.key < b.key ? -1 : 1));
            list.forEach((p, i) => { p.order = i + 1; });
            this._parts = list;
            this._byKey = new Map(list.map(p => [p.key, p]));
            this._partOf = new Map();
            list.forEach(p => p.entries.forEach(e => this._partOf.set(e.mesh, p)));
            this._repick();
        }

        _repick() {
            this._pick = [];
            this._parts.forEach(p => { if (!p.hidden) p.entries.forEach(e => this._pick.push(e.mesh)); });
        }

        // Every look a part can have comes from its saved base material plus the current state.
        _paint(p) {
            const sel = p === this._sel, hov = !sel && p === this._hov, ghost = this._xray && !sel;
            for (const e of p.entries) {
                for (let i = 0; i < e.mats.length; i++) {
                    const m = e.mats[i], b = e.base[i];
                    if (b.emissive) {
                        if (sel || hov) m.emissive.copy(this._tint).multiplyScalar(sel ? SELECT_GLOW : HOVER_GLOW);
                        else m.emissive.copy(b.emissive);
                        m.emissiveIntensity = sel || hov ? 1 : b.ei;
                    } else if (b.color) {   // unlit materials have no emissive: tint the colour instead
                        m.color.copy(b.color);
                        if (sel || hov) m.color.lerp(this._tint, sel ? 0.5 : 0.25);
                    }
                    const tr = ghost || b.transparent;
                    if (m.transparent !== tr) { m.transparent = tr; m.needsUpdate = true; }
                    m.opacity = ghost ? Math.min(b.opacity, XRAY_OPACITY) : b.opacity;
                    m.depthWrite = ghost ? false : b.depthWrite;
                    m.visible = !p.hidden;   // per material, so a hidden mesh never hides parts nested under it
                }
            }
        }

        select(key) {
            const p = key == null ? null : this._byKey.get(key) || null;
            if (p === this._sel) return;
            const old = this._sel;
            this._sel = p;
            if (old) this._paint(old);
            if (p) this._paint(p);
            this._invalidate();
            this._emit('onSelect', p ? p.key : null);
            this._emit('onChange');
        }

        // ---------- disassembly ----------

        setDetached(key, on) {
            const p = this._byKey.get(key);
            if (!p || p.detached === !!on) return;
            this._detach(p, !!on);
            this._emit('onChange');
        }

        toggle(key) {
            const p = this._byKey.get(key);
            if (p) this.setDetached(key, !p.detached);
        }

        // Hidden parts stay put: the user would see nothing move.
        detachAll() {
            let n = 0;
            this._parts.forEach(p => { if (!p.detached && !p.hidden) { this._detach(p, true); n++; } });
            if (n) this._emit('onChange');
        }

        attachAll() {
            let n = 0;
            this._parts.forEach(p => { if (p.detached) { this._detach(p, false); n++; } });
            this._stack = [];
            if (n) this._emit('onChange');
        }

        step(dir) {
            let p = null;
            if (dir > 0) p = this._parts.find(q => !q.detached && !q.hidden) || null;
            else if (dir < 0) p = this._stack[this._stack.length - 1] || null;
            if (!p) return null;
            this._detach(p, dir > 0);
            this._emit('onChange');
            return p.key;
        }

        _detach(p, on) {
            p.detached = on;
            if (on) this._stack.push(p);
            else { const i = this._stack.lastIndexOf(p); if (i >= 0) this._stack.splice(i, 1); }
            p.aFrom = p.a;
            p.aTo = on ? 1 : 0;
            p.t0 = performance.now();
            this._anim.add(p);
            this._kick();
        }

        setExplode(f) {
            f = Math.min(1, Math.max(0, Number(f) || 0));
            if (f === this._explode || !this._model) return;
            this._explode = f;
            this._parts.forEach(p => { if (!this._anim.has(p)) this._offset(p); });   // moving ones pick it up next frame
            this._invalidate();
            this._emit('onChange');
        }

        _stepParts(now) {
            for (const p of this._anim) {
                const k = Math.min(1, Math.max(0, (now - p.t0) / DETACH_MS));
                p.a = p.aFrom + (p.aTo - p.aFrom) * ease(k);
                if (k >= 1) this._anim.delete(p);
                this._offset(p);
            }
        }

        // Attached parts ride the global explode; detached ones sit at DETACH_OUT whatever it is.
        _offset(p) {
            p.off.copy(p.vec).multiplyScalar(this._explode + (DETACH_OUT - this._explode) * p.a);
            this._place(p);
        }

        _place(p) {
            const o = this._v3.copy(p.off);
            if (p.anc) o.sub(p.anc.off);   // already carried that far by the parent part
            p.h.position.copy(o.applyMatrix3(p.toLocal)).add(p.pos0);
            this._moved = true;
            for (const d of p.deps) this._place(d);
        }

        // ---------- visibility and look ----------

        setXray(on) {
            on = !!on;
            if (on === this._xray) return;
            this._xray = on;
            this._parts.forEach(p => this._paint(p));
            this._invalidate();
            this._emit('onChange');
        }

        setHidden(key, on) {
            const p = this._byKey.get(key);
            if (!p || p.hidden === !!on) return;
            p.hidden = !!on;
            this._afterVisibility([p]);
        }

        isolate(key) {
            const keep = key == null ? null : this._byKey.get(key);
            if (key != null && !keep) return;
            const changed = this._parts.filter(p => {
                const h = !!keep && p !== keep;
                if (p.hidden === h) return false;
                p.hidden = h;
                return true;
            });
            if (changed.length) this._afterVisibility(changed);
        }

        _afterVisibility(changed) {
            changed.forEach(p => this._paint(p));
            if (this._hov && this._hov.hidden) this._setHover(null, this._hx, this._hy);
            this._repick();
            this._invalidate();
            this._emit('onChange');
        }

        setAutoRotate(on) { this._controls.autoRotate = !!on; this._kick(); }

        setGrid(on) {
            this._gridOn = !!on;
            if (this._grid) this._grid.visible = this._gridOn;
            this._invalidate();
        }

        // ---------- camera ----------

        view(name) {
            const dir = name === 'fit' ? null : Object.prototype.hasOwnProperty.call(VIEWS, name) ? VIEWS[name] : undefined;
            if (!this._model || dir === undefined) return;
            const b = this._bounds(null), r = Math.max(b.getSize(this._v2).length() / 2, this._radius * 1e-4);
            this._fly(b.getCenter(this._v3), this._fitDistance(r), dir, CAMERA_MS);
        }

        focus(key) {
            const p = this._byKey.get(key);
            if (!p) return;
            const b = this._bounds(p), r = Math.max(b.getSize(this._v2).length() / 2, this._radius * 1e-3);
            this._fly(b.getCenter(this._v3), this._fitDistance(r), null, CAMERA_MS);
        }

        // Where things are now (explode included): one part, or every visible one.
        _bounds(p) {
            const b = this._b1.makeEmpty();
            const add = q => { if (!q.box.isEmpty()) b.union(this._b2.copy(q.box).translate(q.off)); };
            if (p) add(p);
            else {
                this._parts.forEach(q => { if (!q.hidden) add(q); });
                if (b.isEmpty()) this._parts.forEach(add);
            }
            if (b.isEmpty()) b.copy(this._modelBox);
            return b;
        }

        _fitDistance(r) {
            const cam = this._camera, v = cam.fov * Math.PI / 360, h = Math.atan(Math.tan(v) * cam.aspect);
            return r / Math.sin(Math.min(v, h)) * 1.05;
        }

        // Flies to look at `target` from `dist` away, from direction `dir` (or the current one).
        // Interpolates in spherical terms, so the camera swings round the model instead of
        // cutting through it, and zooms on a log scale, so a part 1000× smaller arrives smoothly.
        _fly(target, dist, dir, ms) {
            const s = this._sph.setFromVector3(this._v1.subVectors(this._camera.position, this._controls.target));
            let phi = s.phi, theta = s.theta;
            if (dir) {
                this._v1.fromArray(dir).normalize();
                phi = Math.acos(Math.min(1, Math.max(-1, this._v1.y)));
                theta = Math.atan2(this._v1.x, this._v1.z);
            }
            phi = Math.min(Math.PI - 1e-4, Math.max(1e-4, phi));   // straight down has no azimuth
            this._cam = { t0: performance.now(), ms, t1: this._controls.target.clone(), t2: target.clone(),
                r1: Math.max(s.radius, 1e-9), r2: dist, p1: s.phi, p2: phi, th1: s.theta, th2: s.theta + wrapAngle(theta - s.theta) };
            this._controls.enableDamping = false;   // drops leftover orbit drift, so the flight lands where it aims
            if (!ms) { this._stepCamera(0); this._controls.update(); }
            this._invalidate();
        }

        _stepCamera(now) {
            const c = this._cam, k = c.ms ? Math.min(1, Math.max(0, (now - c.t0) / c.ms)) : 1, e = ease(k);
            const target = this._controls.target;
            target.lerpVectors(c.t1, c.t2, e);
            this._sph.set(c.r1 * Math.pow(c.r2 / c.r1, e), c.p1 + (c.p2 - c.p1) * e, c.th1 + (c.th2 - c.th1) * e);
            this._camera.position.setFromSpherical(this._sph).add(target);
            if (k >= 1) this._stopCamera();
        }

        _stopCamera() {
            if (!this._cam) return;
            this._cam = null;
            this._controls.enableDamping = true;
        }

        // ---------- pointer ----------

        _down(e) {
            this._downs.set(e.pointerId, [e.clientX, e.clientY]);
            if (this._downs.size > 1) this._multi = true;
            this._hoverDue = false;
            if (this._hov) this._setHover(null, e.clientX, e.clientY);   // no hover tint while orbiting
        }

        _up(e) {
            const d = this._downs.get(e.pointerId), multi = this._multi;
            this._downs.delete(e.pointerId);
            if (!this._downs.size) this._multi = false;
            // A drag or a pinch is camera work, not a pick.
            if (!d || multi || e.button !== 0 || Math.hypot(e.clientX - d[0], e.clientY - d[1]) >= PICK_SLOP) return;
            const p = this._hit(e.clientX, e.clientY);
            this.select(p ? p.key : null);
            this._move(e);
        }

        _cancel(e) {
            this._downs.delete(e.pointerId);
            if (!this._downs.size) this._multi = false;
        }

        // Hover is worked out once per frame at most, and only for a mouse with no button held.
        _move(e) {
            if (e.pointerType !== 'mouse' || e.buttons) return;
            this._hx = e.clientX;
            this._hy = e.clientY;
            this._hoverDue = true;
            this._kick();
        }

        _leave() {
            this._hoverDue = false;
            if (this._hov) this._setHover(null, this._hx, this._hy);
        }

        _setHover(p, x, y) {
            if (p === this._hov) { if (p) this._emit('onHover', p.key, x, y); return; }   // same part: the tooltip follows
            const old = this._hov;
            this._hov = p;
            if (old) this._paint(old);
            if (p) this._paint(p);
            this._canvas.style.cursor = p ? 'pointer' : '';
            this._invalidate();
            this._emit('onHover', p ? p.key : null, x, y);
        }

        // Raycasts the flat list of visible meshes only, never the whole scene.
        _hit(x, y) {
            if (!this._pick.length) return null;
            const r = this._canvas.getBoundingClientRect();
            if (!r.width || !r.height) return null;
            if (this._moved) { this._model.updateMatrixWorld(); this._moved = false; }
            this._ndc.set((x - r.left) / r.width * 2 - 1, 1 - (y - r.top) / r.height * 2);
            this._ray.setFromCamera(this._ndc, this._camera);
            this._hits.length = 0;
            this._ray.intersectObjects(this._pick, false, this._hits);
            return this._hits.length ? this._partOf.get(this._hits[0].object) || null : null;
        }

        // ---------- frame loop ----------

        _invalidate() { this._dirty = true; this._kick(); }

        _kick() { if (!this._raf && !this._disposed) this._raf = requestAnimationFrame(this._frame); }

        // Runs only while something moves (a flight, a part, damping, auto-rotate) or after a change.
        _tick(now) {
            this._raf = 0;
            if (this._disposed) return;
            let busy = false;
            if (this._cam) { this._stepCamera(now); busy = true; }
            if (this._anim.size) { this._stepParts(now); busy = true; }
            if (this._controls.update()) busy = true;
            if (this._hoverDue) {
                this._hoverDue = false;
                this._setHover(this._hit(this._hx, this._hy), this._hx, this._hy);
            }
            if (busy || this._dirty) this._render();
            if (busy) this._kick();
        }

        _render() {
            this._dirty = false;
            if (!this._w) return;
            const cam = this._camera, d = cam.position.distanceTo(this._controls.target), R = this._radius;
            // Clip planes follow the zoom, so a bolt seen up close and the whole machine both
            // keep their depth precision.
            cam.near = Math.max(d * 0.01, R * 1e-5);
            cam.far = d + R * 8;
            cam.updateProjectionMatrix();
            this._renderer.render(this._scene, cam);
            this._moved = false;
        }

        _resize() {
            const w = this._el.clientWidth, h = this._el.clientHeight;
            if (!w || !h) { this._w = 0; return; }
            if (w === this._w && h === this._h) return;
            this._w = w;
            this._h = h;
            this._renderer.setSize(w, h, false);
            this._camera.aspect = w / h;
            this._camera.updateProjectionMatrix();
            this._render();   // now, before paint: resizing cleared the canvas
        }

        // Callbacks run after the viewer's own state is settled; one that throws is reported
        // without leaving the viewer half-updated.
        _emit(name, ...args) {
            const f = this._cb[name];
            if (typeof f !== 'function') return;
            try { f(...args); } catch (err) { setTimeout(() => { throw err; }); }
        }

        // ---------- output and teardown ----------

        snapshot(maxWidth = 640) {
            if (this._disposed) return '';
            this._render();   // read right after drawing: the WebGL buffer is not kept between frames
            const src = this._canvas, w = Math.max(1, Math.min(Math.round(maxWidth) || 640, src.width));
            const h = Math.max(1, Math.round(src.height * w / Math.max(1, src.width)));
            const c = document.createElement('canvas');
            c.width = w;
            c.height = h;
            const g = c.getContext('2d');
            // JPEG has no transparency: put the page's background under the model, as it is seen.
            g.fillStyle = this._background();
            g.fillRect(0, 0, w, h);
            g.drawImage(src, 0, 0, w, h);
            return c.toDataURL('image/jpeg', 0.9);
        }

        // The first background up the tree: a solid colour, or for a CSS gradient the average of
        // its colour stops (close enough for a card picture).
        _background() {
            for (let el = this._el; el && el.nodeType === 1; el = el.parentElement) {
                const cs = getComputedStyle(el), c = cs.backgroundColor;
                if (c && c !== 'transparent' && !/^rgba\(.*,\s*0\)$/.test(c)) return c;
                const stops = (cs.backgroundImage.match(/rgba?\([^)]*\)/g) || []).map(s => s.match(/[\d.]+/g).map(Number));
                if (stops.length) {
                    const avg = i => Math.round(stops.reduce((a, s) => a + s[i], 0) / stops.length);
                    return `rgb(${avg(0)}, ${avg(1)}, ${avg(2)})`;
                }
            }
            return '#ffffff';
        }

        dispose() {
            if (this._disposed) return;
            this._disposed = true;
            cancelAnimationFrame(this._raf);
            this._raf = 0;
            this._ro.disconnect();
            for (const k in this._on) this._canvas.removeEventListener(k, this._on[k]);
            this._controls.removeEventListener('change', this._onControls);
            this._controls.removeEventListener('start', this._onGrab);
            this._controls.dispose();
            this._cb = {};
            this._unload();
            this._scene.environment = null;
            if (this._env) this._env.dispose();
            this._renderer.dispose();
            this._renderer.forceContextLoss();   // a page gets only a few live WebGL contexts
            this._canvas.remove();
            if (this._setPos) this._el.style.position = '';
        }
    }

    window.M3Viewer = M3Viewer;
})();
