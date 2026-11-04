/* Tractor Monitoring Dashboard - Service Worker
   Network-first for the app shell (so deploys show up on reload), cache-first
   for static assets, network-only for Firebase live endpoints. */

const CACHE_NAME = 'tractor-monitor-2.29.1';

// Same-origin core files — always revalidated from network first so a new
// deploy is picked up on the next reload (falls back to cache when offline).
const CORE = [
    './',
    './index.html',
    './script.js',
    './projects.js',
    './firebase-init.js',
    './style.css',
    './logo.png',
    './icons/icon-32.png',
    './icons/icon-192.png',
    './icons/apple-touch-icon.png',
    './icons/icon-96.png',
    './manifest.webmanifest',
    // Libraries and fonts, self-hosted since 2.11.1 (vendor/README.md).
    // vendor/xlsx.full.min.js is cached the first time an Excel is imported.
    './vendor/chart.umd.js',
    './vendor/papaparse.min.js',
    './vendor/fonts.css',
    './vendor/fontawesome/css/all.min.css',
    './vendor/fontawesome/webfonts/fa-solid-900.woff2',
    './vendor/fontawesome/webfonts/fa-regular-400.woff2',
    './vendor/fonts/poppins-latin-500-normal.woff2',
    './vendor/fonts/poppins-latin-600-normal.woff2',
    './vendor/fonts/open-sans-latin-400-normal.woff2',
    './vendor/fonts/open-sans-latin-500-normal.woff2',
    './vendor/fonts/open-sans-latin-600-normal.woff2',
    './vendor/fonts/open-sans-latin-700-normal.woff2'
];

// Same-origin paths served network-first (kept fresh). Everything else
// same-origin (assets) is cache-first.
//
// These are resolved against the worker's own scope rather than written as
// absolute paths. Hosted in a subdirectory — a GitHub Pages project site, say,
// at /<repo>/ — absolute paths like '/style.css' never match the real request
// path, so the app shell silently fell back to cache-first and a fresh deploy
// kept serving the previous style.css and script.js.
const SCOPE_PATH = new URL('./', self.registration.scope).pathname;
const NETWORK_FIRST_PATHS = ['', 'index.html', 'script.js', 'projects.js', 'firebase-init.js', 'style.css', 'data/agri-facts.json']
    .map(file => SCOPE_PATH + file);

// Hosts whose responses must always go to the network (real-time data, auth).
const NETWORK_ONLY_HOSTS = [
    'firestore.googleapis.com',
    'firebasestorage.googleapis.com',   // 2.13.0: Learning Center files and videos
    'firebaseinstallations.googleapis.com',
    'firebaseremoteconfig.googleapis.com',
    'identitytoolkit.googleapis.com',
    'securetoken.googleapis.com',
    'www.googleapis.com',
    'firebaselogging-pa.googleapis.com',
    'fcmregistrations.googleapis.com'
];

self.addEventListener('install', event => {
    event.waitUntil((async () => {
        const cache = await caches.open(CACHE_NAME);
        // The app shell must be complete and from THIS deploy: cache:'reload'
        // skips the browser's HTTP cache (GitHub Pages sends max-age=600), so
        // a fresh index.html is never paired with the previous script.js. If
        // any core file fails, install fails and the old worker — with a
        // complete cache — stays in charge.
        await cache.addAll(CORE.map(u => new Request(u, { cache: 'reload' })));
    })());
    self.skipWaiting();
});

self.addEventListener('activate', event => {
    event.waitUntil((async () => {
        const keys = await caches.keys();
        await Promise.all(keys.filter(k => k !== CACHE_NAME).map(k => caches.delete(k)));
        await self.clients.claim();
    })());
});

self.addEventListener('message', event => {
    if (event.data === 'SKIP_WAITING') self.skipWaiting();
});

self.addEventListener('fetch', event => {
    if (event.request.method !== 'GET') return;

    let url;
    try { url = new URL(event.request.url); } catch (e) { return; }

    // Firebase/Firestore live endpoints: never cache, never intercept.
    if (NETWORK_ONLY_HOSTS.includes(url.hostname)) return;

    const sameOrigin = url.origin === self.location.origin;
    const isNavigation = event.request.mode === 'navigate';
    const isCorePath = sameOrigin && NETWORK_FIRST_PATHS.includes(url.pathname);

    // Network-first for navigations and core app-shell files → fresh on reload.
    if (isNavigation || isCorePath) {
        event.respondWith(networkFirst(event.request));
        return;
    }

    // Cache-first for everything else (static assets, CDN libs, fonts).
    event.respondWith(cacheFirst(event.request));
});

async function networkFirst(request) {
    const cache = await caches.open(CACHE_NAME);
    try {
        // 'no-cache': revalidate with the server instead of taking a copy the
        // browser's HTTP cache may hold from the previous deploy.
        // (A navigation request cannot be re-issued with options — it throws —
        // and navigations revalidate on their own.)
        const response = await fetch(request.mode === 'navigate' ? request : new Request(request, { cache: 'no-cache' }));
        if (response && response.ok) cache.put(request, response.clone());
        return response;
    } catch (e) {
        const cached = await cache.match(request);
        if (cached) return cached;
        // Navigation offline fallback → the cached shell.
        if (request.mode === 'navigate') {
            const shell = await cache.match('./index.html');
            if (shell) return shell;
        }
        throw e;
    }
}

async function cacheFirst(request) {
    const cache = await caches.open(CACHE_NAME);
    const cached = await cache.match(request);
    if (cached) return cached;
    try {
        const response = await fetch(request);
        if (response && response.ok) cache.put(request, response.clone());
        return response;
    } catch (e) {
        return cached;
    }
}

// ---- Push notifications (2.18.0) ----
// The server sends FCM data messages { title, body, url, tag }; they are
// shown here, and a tap opens (or focuses) the dashboard at that link.
self.addEventListener('push', event => {
    let p = {};
    try { p = event.data ? event.data.json() : {}; } catch (_) { p = {}; }
    const d = p.data || p.notification || {};
    event.waitUntil(self.registration.showNotification(d.title || 'OT Dashboard', {
        body: d.body || '',
        icon: './icons/icon-192.png',
        badge: './icons/icon-96.png',
        tag: d.tag || undefined,
        renotify: !!d.tag,
        data: { url: d.url || './' }
    }));
});
self.addEventListener('notificationclick', event => {
    event.notification.close();
    const url = new URL((event.notification.data && event.notification.data.url) || './', self.registration.scope).href;
    event.waitUntil(self.clients.matchAll({ type: 'window', includeUncontrolled: true }).then(list => {
        const open = list.find(c => c.url.startsWith(self.registration.scope));
        if (open) return open.navigate(url).then(c => (c || open).focus()).catch(() => open.focus());
        return self.clients.openWindow(url);
    }));
});
