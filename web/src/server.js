/**
 * ╔═══════════════════════════════════════════════════════╗
 * ║  LAB NUTRITION — Shopify Dev Proxy Server             ║
 * ║                                                       ║
 * ║  Sirve admin.html/portal.html localmente y proxea    ║
 * ║  TODAS las peticiones /api/* a Railway.              ║
 * ║                                                       ║
 * ║  Así shopify app dev SIEMPRE usa el backend más      ║
 * ║  reciente sin necesidad de mantener dos servidores.  ║
 * ╚═══════════════════════════════════════════════════════╝
 */

require('dotenv').config();
const express = require('express');
const path = require('path');
const app = express();

const PORT = process.env.PORT || 3000;
const RAILWAY = process.env.BACKEND_URL || 'https://pixel-suite-pro-production.up.railway.app';

app.use(express.json({ limit: '10mb' }));
app.use(express.urlencoded({ extended: true }));

/* ── Shopify iframe/CSP permissive headers ── */
app.use((req, res, next) => {
    res.setHeader('Content-Security-Policy', "frame-ancestors 'none' https://*.shopify.com https://*.myshopify.com;");
    res.setHeader('X-Frame-Options', 'ALLOWALL');
    next();
});

/* ── Static files (admin.html, portal.html, assets) ── */
app.use(express.static(path.join(__dirname, 'public')));

/* ── Health check ── */
app.get('/health', (req, res) =>
    res.json({ status: 'ok (proxy)', backend: RAILWAY, port: PORT, ts: new Date() })
);

/* ══════════════════════════════════════════════════
   🔀 PROXY: todas las rutas /api/* → Railway
══════════════════════════════════════════════════ */
app.all('/api/*', async (req, res) => {
    const target = `${RAILWAY}${req.url}`;
    try {
        const opts = {
            method: req.method,
            headers: {
                'Content-Type': 'application/json',
                'Accept': 'application/json',
            },
        };
        if (req.method !== 'GET' && req.method !== 'HEAD') {
            opts.body = JSON.stringify(req.body);
        }
        const upstream = await fetch(target, opts);
        const ct = upstream.headers.get('content-type') || '';

        // Pass status code
        res.status(upstream.status);

        if (ct.includes('text/csv')) {
            res.setHeader('Content-Type', ct);
            res.setHeader('Content-Disposition', upstream.headers.get('content-disposition') || '');
            res.send(Buffer.from(await upstream.arrayBuffer()));
        } else {
            const data = await upstream.json();
            res.json(data);
        }
    } catch (err) {
        console.error(`[PROXY] ${req.method} ${req.url} → ${err.message}`);
        res.status(502).json({ error: 'Backend unreachable', backend: RAILWAY, detail: err.message });
    }
});

/* ── Proxy webhooks too ── */
app.all('/webhooks/*', async (req, res) => {
    try {
        const upstream = await fetch(`${RAILWAY}${req.url}`, {
            method: req.method, headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(req.body),
        });
        res.status(upstream.status).json(await upstream.json().catch(() => ({})));
    } catch { res.sendStatus(200); }
});

/* ── SPA fallback: serve admin.html for all other routes ── */
app.get('*', (req, res) => {
    res.sendFile(path.join(__dirname, 'public', 'admin.html'));
});

/* ── Start ── */
app.listen(PORT, () => {
    console.log(`\n🚀 LAB NUTRITION Dev Proxy — port ${PORT}`);
    console.log(`   📡 Proxying /api/* → ${RAILWAY}`);
    console.log(`   📊 Admin: http://localhost:${PORT}/admin.html`);
    console.log(`   👤 Portal: http://localhost:${PORT}/portal.html\n`);
});
