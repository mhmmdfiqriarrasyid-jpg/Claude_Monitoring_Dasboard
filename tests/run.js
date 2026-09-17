#!/usr/bin/env node
// Runs every browser test against a static server started here, so `npm test`
// needs nothing else running. Exits non-zero if any suite fails.
const { spawn } = require('child_process');
const http = require('http');
const fs = require('fs');
const path = require('path');

const ROOT = path.join(__dirname, '..');
const PORT = Number(process.env.PORT) || 8765;
const SUITES = [
    'team_logic', 'roles_test', 'company_test', 'adjust_test',
    'session_test', 'warehouse_test', 'approval_test', 'regress'
];

const TYPES = {
    '.html': 'text/html; charset=utf-8',
    '.js': 'text/javascript; charset=utf-8',
    '.css': 'text/css; charset=utf-8',
    '.json': 'application/json; charset=utf-8',
    '.webmanifest': 'application/manifest+json; charset=utf-8',
    '.png': 'image/png'
};

const server = http.createServer((req, res) => {
    // Path traversal would only hurt the machine running the tests, but there
    // is no reason to serve anything outside the project.
    const rel = decodeURIComponent(req.url.split('?')[0]).replace(/^\/+/, '') || 'index.html';
    const file = path.join(ROOT, rel);
    if (!file.startsWith(ROOT)) { res.writeHead(403).end(); return; }
    fs.readFile(file, (err, buf) => {
        if (err) { res.writeHead(404).end('not found'); return; }
        res.writeHead(200, { 'Content-Type': TYPES[path.extname(file)] || 'application/octet-stream' });
        res.end(buf);
    });
});

function runSuite(name) {
    return new Promise(resolve => {
        const child = spawn(process.execPath, [path.join(__dirname, name + '.js')], {
            env: { ...process.env, BASE_URL: `http://localhost:${PORT}` }
        });
        let out = '';
        child.stdout.on('data', d => { out += d; });
        child.stderr.on('data', d => { out += d; });
        child.on('close', code => resolve({ name, code, out }));
    });
}

(async () => {
    await new Promise(r => server.listen(PORT, r));
    let failed = 0;
    for (const name of SUITES) {
        const { code, out } = await runSuite(name);
        const summary = (out.match(/^\d+\/\d+ lulus$/m) || ['(tidak ada ringkasan)'])[0];
        const ok = code === 0;
        if (!ok) failed++;
        console.log(`${ok ? 'PASS' : 'FAIL'}  ${name.padEnd(16)} ${summary}`);
        // Only a failing suite is worth its full output.
        if (!ok) console.log(out.split('\n').filter(l => /FAIL|THREW|ERROR/.test(l)).join('\n'));
    }
    server.close();
    console.log(failed ? `\n${failed} suite gagal` : '\nSemua suite lulus');
    process.exit(failed ? 1 : 0);
})();
