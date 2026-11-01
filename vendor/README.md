# Third-party files, served from this site

Copied unchanged from the official npm packages, pinned versions. Kept here
instead of a CDN so a compromised or changed CDN file can never run in the
app (2.11.1). To update one, take the file from the new npm package (`npm
pack <name>@<version>`) and bump CACHE_NAME in service-worker.js.

| File | Package |
|---|---|
| chart.umd.js | chart.js@4.4.1 (dist/chart.umd.js) |
| papaparse.min.js | papaparse@5.4.1 |
| email.min.js | @emailjs/browser@4.4.1 (dist/email.min.js) |
| xlsx.full.min.js | xlsx@0.18.5 (dist/xlsx.full.min.js), loaded only when importing Excel |
| fontawesome/ | @fortawesome/fontawesome-free@6.5.1 (css/all.min.css, webfonts/*.woff2) |
| fonts/ + fonts.css | @fontsource/poppins@5.0.8, @fontsource/open-sans@5.0.28 (Latin) |

Licences are in `licenses/`. Firebase is still loaded from www.gstatic.com,
Google's official host for the Firebase JS SDK without a bundler.
