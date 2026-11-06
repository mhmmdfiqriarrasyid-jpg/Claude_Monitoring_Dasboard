// Shared launch + URL resolution for the browser tests.
//
// The tests drive a real Chromium against a static server, because that is the
// only way to check what this app actually does: almost everything it gets
// wrong shows up in layout, in gating, or in what reaches the cloud stub —
// none of which a DOM-less test can see.
const { chromium } = require('playwright');

// CHROME_PATH lets a machine point at a Chromium it already has instead of
// downloading one; without it Playwright uses its own.
const EXEC = process.env.CHROME_PATH || '';

// The runner starts the server and passes the port through.
const BASE_URL = process.env.BASE_URL || 'http://localhost:8765';

function launch(opts = {}) {
    return chromium.launch(EXEC ? { ...opts, executablePath: EXEC } : opts);
}

module.exports = { chromium, launch, BASE_URL };
