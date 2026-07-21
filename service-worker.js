/* Tractor Monitoring Dashboard - Service Worker
   Network-first for the app shell (so deploys show up on reload), cache-first
   for static assets, network-only for Firebase live endpoints. */

const CACHE_NAME = 'tractor-monitor-v59';

// Same-origin core files — always revalidated from network first so a new
// deploy is picked up on the next reload (falls back to cache when offline).
const CORE = [
    './',
    './index.html',
    './script.js',
    './firebase-init.js',
    './style.css',
    './logo.png',
    './manifest.webmanifest'
];

// Optional third-party libs — best-effort precache; served cache-first.
const CDN = [
    'https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.5.1/css/all.min.css',
    'https://cdn.jsdelivr.net/npm/chart.js@4.4.1/dist/chart.umd.min.js',
    'https://cdn.jsdelivr.net/npm/papaparse@5.4.1/papaparse.min.js'
];

// Same-origin paths served network-first (kept fresh). Everything else
// same-origin (assets) is cache-first.
const NETWORK_FIRST_PATHS = ['/', '/index.html', '/script.js', '/firebase-init.js', '/style.css'];

// Hosts whose responses must always go to the network (real-time data, auth).
const NETWORK_ONLY_HOSTS = [
    'firestore.googleapis.com',
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
        // Per-item best-effort: one failing URL must not abort the whole precache.
        await Promise.allSettled([...CORE, ...CDN].map(u => cache.add(u)));
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
        const response = await fetch(request);
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
