/* Tractor Monitoring Dashboard - Service Worker
   Cache-first strategy for app shell, network fallback for everything else. */

const CACHE_NAME = 'tractor-monitor-v20';
const APP_SHELL = [
    './',
    './index.html',
    './script.js',
    './firebase-init.js',
    './style.css',
    './logo.png',
    './manifest.webmanifest',
    'https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap',
    'https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.5.1/css/all.min.css',
    'https://cdn.jsdelivr.net/npm/chart.js@4.4.1/dist/chart.umd.min.js',
    'https://cdn.jsdelivr.net/npm/papaparse@5.4.1/papaparse.min.js'
];

// Hosts whose responses must always go to the network (real-time data,
// auth, telemetry). Caching them would break Firestore live sync.
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
    event.waitUntil(
        caches.open(CACHE_NAME)
            .then(cache => cache.addAll(APP_SHELL).catch(() => { /* best-effort */ }))
    );
    self.skipWaiting();
});

self.addEventListener('activate', event => {
    event.waitUntil(
        caches.keys().then(keys =>
            Promise.all(keys.filter(k => k !== CACHE_NAME).map(k => caches.delete(k)))
        )
    );
    self.clients.claim();
});

self.addEventListener('fetch', event => {
    if (event.request.method !== 'GET') return;

    let url;
    try { url = new URL(event.request.url); } catch (e) { return; }

    // Firestore + Firebase live endpoints: never cache, never intercept.
    if (NETWORK_ONLY_HOSTS.includes(url.hostname)) return;

    event.respondWith(
        caches.match(event.request).then(cached => {
            if (cached) return cached;
            return fetch(event.request).then(response => {
                if (response.ok) {
                    const copy = response.clone();
                    caches.open(CACHE_NAME).then(cache => cache.put(event.request, copy)).catch(() => {});
                }
                return response;
            }).catch(() => cached);
        })
    );
});
