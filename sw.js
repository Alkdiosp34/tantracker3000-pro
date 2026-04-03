// TexTrack Service Worker
const CACHE = 'textrack-v1';
const OFFLINE_URLS = [
  '/tantracker3000-pro/control.html',
  '/tantracker3000-pro/account.html',
  '/tantracker3000-pro/manifest.json'
];

self.addEventListener('install', e => {
  e.waitUntil(
    caches.open(CACHE).then(cache => cache.addAll(OFFLINE_URLS)).catch(() => {})
  );
  self.skipWaiting();
});

self.addEventListener('activate', e => {
  e.waitUntil(
    caches.keys().then(keys =>
      Promise.all(keys.filter(k => k !== CACHE).map(k => caches.delete(k)))
    )
  );
  self.clients.claim();
});

self.addEventListener('fetch', e => {
  // Network first for API calls
  if (e.request.url.includes('onrender.com') || e.request.url.includes('api.')) {
    return;
  }
  // Cache first for app shell
  e.respondWith(
    caches.match(e.request).then(cached => cached || fetch(e.request).catch(() => cached))
  );
});
