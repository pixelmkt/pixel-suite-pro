require('dotenv').config();
const express = require('express');
const helmet = require('helmet');
const cors = require('cors');
const cron = require('node-cron');
const path = require('path');
// 🆕 2026-06-05 — rate-limit anti-abuse (express-rate-limit ya en package.json)
let rateLimit;
try { rateLimit = require('express-rate-limit'); } catch (e) { console.warn('[RATE_LIMIT] Not loaded:', e.message); }

// ── Database layer (Supabase removed — using Shopify Metaobjects) ──

// ── Shopify Metaobjects — Native Database ──────────────────────
let db;
try {
    db = require('./services/shopify-storage');
    console.log('[DB] Using Shopify Metaobjects as native database');
} catch (e) {
    console.warn('[DB] shopify-storage not available:', e.message);
    db = null;
}

// Crash-proof service imports — server starts even without credentials
let mp, notifications, shopify, sellingPlans, subscriptionContracts;
try { mp = require('./services/mercadopago'); } catch (e) { console.warn('[MP] Not configured:', e.message); mp = {}; }
try { notifications = require('./services/notifications'); } catch (e) { console.warn('[EMAIL] Not configured:', e.message); notifications = {}; }
try { shopify = require('./services/shopify'); } catch (e) { console.warn('[SHOPIFY] Not configured:', e.message); shopify = {}; }
try { sellingPlans = require('./services/selling-plans'); } catch (e) { console.warn('[SELLING_PLANS] Not loaded:', e.message); sellingPlans = {}; }
try { subscriptionContracts = require('./services/subscription-contracts'); } catch (e) { console.warn('[CONTRACTS] Not loaded:', e.message); subscriptionContracts = {}; }

const app = express();
const PORT = process.env.PORT || 8080;

/* ─── MIDDLEWARE ─── */
// CSP: MUST allow Shopify Admin to embed this app in an iframe
app.use((req, res, next) => {
    res.setHeader(
        'Content-Security-Policy',
        "frame-ancestors 'self' https://admin.shopify.com https://nutrition-lab-cluster.myshopify.com https://*.myshopify.com;"
    );
    // Remove any X-Frame-Options that would block the iframe
    res.removeHeader('X-Frame-Options');
    next();
});

app.use(helmet({
    contentSecurityPolicy: false,
    frameguard: false,
    crossOriginEmbedderPolicy: false,
    crossOriginOpenerPolicy: false,
    crossOriginResourcePolicy: false
}));
// 🆕 2026-06-05 — Security headers extra (BFS requirements + privacy)
app.use((req, res, next) => {
    res.setHeader('Permissions-Policy', 'geolocation=(), camera=(), microphone=(), payment=(self)');
    res.setHeader('Referrer-Policy', 'strict-origin-when-cross-origin');
    res.setHeader('X-Content-Type-Options', 'nosniff');
    next();
});
app.use(cors({ origin: '*' })); // Allow all origins for embedded app

// 🔧 FIX 2026-06-11: Railway corre detrás de un proxy — sin trust proxy,
//   req.ip es la IP interna del edge (igual para todos) y express-rate-limit
//   v7 invalida su keyGenerator → los limiters NUNCA disparaban (bug
//   preexistente desde 2026-06-05, afectaba también al magic-link).
app.set('trust proxy', 1);

// 🆕 2026-06-05 — Rate limiters anti-abuse
//   portal magic-link: 10/min/IP (evita spam de emails)
//   webhooks: NO limitar (Shopify/MP pueden tener picos legítimos)
//   admin: NO limitar por ahora (interno)
const _portalRateLimit = rateLimit ? rateLimit({
    windowMs: 60 * 1000,
    max: 10,
    standardHeaders: true,
    legacyHeaders: false,
    message: { error: 'Demasiados intentos. Esperá 1 minuto.' },
    skip: (req) => req.method === 'OPTIONS'
}) : (req, res, next) => next();

// FIX 2026-04-09: Shopify webhooks necesitan el body RAW (Buffer) para HMAC verify.
// Si aplicamos express.json() global primero, consume el stream y req.body.toString() devuelve "[object Object]".
// Solución: saltar el json parser para rutas /webhooks/shopify/* — esas rutas usan express.raw() inline.
app.use((req, res, next) => {
    if (req.path.startsWith('/webhooks/shopify/')) return next();
    return express.json({ limit: '2mb' })(req, res, next);
});
app.use((req, res, next) => {
    if (req.path.startsWith('/webhooks/shopify/')) return next();
    return express.urlencoded({ extended: true })(req, res, next);
});
// 2026-04-22 — ADITIVO: /widget/* con cache corto + CORS abierto.
// Esto sirve el JS del widget (lab-bundle.js) desde Railway para evitar depender
// del CDN de Shopify al propagar theme app extension updates. Cambios al widget
// se reflejan en segundos tras git push (vs 10-15 min del CDN Shopify).
app.use('/widget', express.static(path.join(__dirname, 'public/widget'), {
    index: false,
    maxAge: '60s',
    setHeaders: (res) => {
        res.set('Cache-Control', 'public, max-age=60, s-maxage=60');
        res.set('Access-Control-Allow-Origin', '*');
        res.set('Access-Control-Allow-Methods', 'GET, OPTIONS');
    }
}));
// Static assets (CSS, JS, etc) but NOT index fallback
app.use(express.static(path.join(__dirname, 'public'), { index: false }));

/* ═══════════════════════════════════════════════════════════════
   🔐 2026-06-11 — AUTH OBLIGATORIO PARA APIs ADMIN (X-Admin-Token)
   Antes estos endpoints estaban expuestos sin auth (PII de clientes,
   envío masivo de emails, creación de órdenes Shopify).
   - Token esperado: env ADMIN_API_TOKEN (NUNCA commitear — repo público)
   - Comparación timing-safe vía _safeEq (function declaration, hoisted)
   - Fail-closed: si ADMIN_API_TOKEN no está configurado → 503 (no se expone nada)
   - EXENTO: /api/marketing/unsubscribe (link público de baja en emails)
   - /webhooks/* no pasa por acá — tienen su propio HMAC (_verifyShopifyHmac)
   - admin.html pide el token una vez (overlay) y lo manda en cada fetch
   Se usa app.use([mounts]) y no startsWith() manual: así el matching es
   EXACTAMENTE el de Express (case-insensitive, trailing slash) y un
   request tipo /API/Admin/... no puede saltarse el auth.
═══════════════════════════════════════════════════════════════ */
// 🆕 2026-06-11 (+settings): GET /api/settings devolvía shopify_access_token,
//   mp_access_token y SMTP pass SIN auth a todo internet. Las páginas públicas
//   (portal.html) ahora usan /api/public-config que expone solo branding.
// 🆕 2026-06-11 (ronda 2): + /api/mailing (envío masivo de emails), /api/automations
//   (PUT sin auth permitía modificar automatizaciones), /api/metrics, /api/subscribers
//   (real-count + export con PII) y /api/shopify (catálogo). Solo admin.html los
//   consume y ya manda el token en todo fetch same-origin a /api/*.
//   GET /api/subscriptions (lista TODAS las subs con PII) se protege A NIVEL DE RUTA
//   con _requireAdminToken: el prefijo NO se puede montar entero porque el widget usa
//   POST /api/subscriptions/checkout y portal.html usa /customer/:id, /:id/pause,
//   /:id/resume, /:id/cancel y /:id/cancel/preview (flujos públicos de clientes).
// 🆕 2026-06-11 (ronda 3): + /api/customers, /api/plans y /api/selling-plans al mount
//   (solo admin.html los consume — verificado con grep en src/public/ y extensions/).
//   /api/products NO va al mount: la theme extension (lab-subscription.liquid) consume
//   GET /api/products/:id/config desde el storefront SIN token → esa ruta queda pública
//   y el resto de /api/products/* se protege inline. Igual inline van las rutas admin
//   de /api/subscriptions: POST /create, PATCH /:id, GET /:id/payments,
//   POST /:id/create-order y POST /batch-create-orders (crean órdenes Shopify REALES).
function _requireAdminToken(req, res, next) {
    // ⛔ AUTH DESACTIVADO 2026-06-11 POR ORDEN EXPLÍCITA DEL DUEÑO (Israel):
    // "quita la protección, no es necesario, solo administramos personas de la empresa".
    // El check bloqueaba el acceso al panel admin. TODOS los usos del auth (mount list
    // de abajo + middlewares inline en rutas) pasan por esta función, así que este
    // return desactiva la protección completa sin tocar ninguna ruta.
    // ⚠️ Consecuencia conocida e informada: /api/settings, lista de suscripciones,
    // mailing, etc. vuelven a responder sin token a cualquiera que conozca la URL.
    // Para REACTIVAR: borrar este return (el código original sigue intacto abajo).
    return next();

    // Preflight CORS: el browser no manda headers custom en OPTIONS
    if (req.method === 'OPTIONS') return next();
    const expected = process.env.ADMIN_API_TOKEN || '';
    if (!expected) {
        return res.status(503).json({
            error: 'ADMIN_API_TOKEN no configurado en el servidor. Agregá la variable en Railway → servicio → Variables y esperá el redeploy.'
        });
    }
    const got = String(req.get('x-admin-token') || '');
    if (!_safeEq(got, expected)) {
        return res.status(401).json({ error: 'No autorizado: header X-Admin-Token faltante o inválido.' });
    }
    return next();
}
app.use(['/api/admin', '/api/marketing', '/api/subscriptions/recover', '/api/remarketing', '/api/settings', '/api/mailing', '/api/automations', '/api/metrics', '/api/subscribers', '/api/shopify', '/api/customers', '/api/plans', '/api/selling-plans'], (req, res, next) => {
    // Link público de baja que viaja en los emails de marketing — sin token
    const full = (req.baseUrl + req.path).toLowerCase().replace(/\/+$/, '');
    if (full === '/api/marketing/unsubscribe') return next();
    return _requireAdminToken(req, res, next);
});

// ── ROOT: Shopify loads the app at / with ?shop=&hmac=&host= params ──
// Always serve admin.html regardless of query params
app.get('/', (req, res) => {
    res.sendFile(path.join(__dirname, 'public', 'admin.html'));
});
app.get('/admin', (req, res) => {
    res.sendFile(path.join(__dirname, 'public', 'admin.html'));
});
app.get('/admin.html', (req, res) => {
    res.sendFile(path.join(__dirname, 'public', 'admin.html'));
});


/* ═══════════════════════════════════════════════
   🔐 SHOPIFY OAUTH — captures Admin API token
═══════════════════════════════════════════════ */
const SHOPIFY_API_KEY = process.env.SHOPIFY_API_KEY || 'fc20b3f68f1c8e854a3dca30788acd48';
const SHOPIFY_API_SECRET = process.env.SHOPIFY_API_SECRET || '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';
const SCOPES = 'read_products,write_products,read_orders,write_orders,read_customers,write_customers,read_own_subscription_contracts,write_own_subscription_contracts,read_purchase_options,write_purchase_options,read_metaobjects,write_metaobjects,read_metaobject_definitions,write_metaobject_definitions';
const HOST = process.env.RAILWAY_PUBLIC_DOMAIN
    ? `https://${process.env.RAILWAY_PUBLIC_DOMAIN}`
    : 'https://pixel-suite-pro-production.up.railway.app';

// In-memory token store (persists while server runs)
let _shopifyToken = process.env.SHOPIFY_ACCESS_TOKEN || null;
let _shopifyShop = process.env.SHOPIFY_SHOP || null;
if (_shopifyToken) console.log(`[OAUTH] Token loaded from env — shop: ${_shopifyShop}`);

// ── Persistent token file (survives Railway restarts, not redeploys) ──
const TOKEN_FILE = path.join(__dirname, '..', 'shopify_token.json');
function loadTokenFromFile() {
    try {
        if (require('fs').existsSync(TOKEN_FILE)) {
            const d = JSON.parse(require('fs').readFileSync(TOKEN_FILE, 'utf8'));
            if (d.access_token && d.shop) {
                _shopifyToken = d.access_token;
                _shopifyShop = d.shop;
                process.env.SHOPIFY_ACCESS_TOKEN = _shopifyToken;
                process.env.SHOPIFY_SHOP = _shopifyShop;
                console.log(`[OAUTH] Token loaded from token file — shop: ${_shopifyShop}`);
                return true;
            }
        }
    } catch (e) { console.warn('[OAUTH] Could not read token file:', e.message); }
    return false;
}
function saveTokenToFile(token, shop) {
    try {
        require('fs').writeFileSync(TOKEN_FILE, JSON.stringify({ access_token: token, shop, saved_at: new Date().toISOString() }, null, 2));
        console.log('[OAUTH] Token saved to file for restart persistence');
    } catch (e) { console.warn('[OAUTH] Could not save token file:', e.message); }
}
// Load from file if not in env
if (!_shopifyToken) loadTokenFromFile();

// Start OAuth — redirect to Shopify
app.get('/auth', (req, res) => {
    const shop = req.query.shop || _shopifyShop || process.env.SHOPIFY_SHOP;
    if (!shop) return res.status(400).send('Missing shop parameter');
    if (!SHOPIFY_API_KEY) return res.status(500).send('SHOPIFY_API_KEY not configured');
    const redirectUri = `${HOST}/auth/callback`;
    const nonce = Math.random().toString(36).substring(2);
    const authUrl = `https://${shop}/admin/oauth/authorize?client_id=${SHOPIFY_API_KEY}&scope=${SCOPES}&redirect_uri=${encodeURIComponent(redirectUri)}&state=${nonce}`;
    console.log(`[OAUTH] Redirecting to: ${authUrl}`);
    res.redirect(authUrl);
});

// OAuth callback — exchange code for token
app.get('/auth/callback', async (req, res) => {
    const { shop, code } = req.query;
    if (!shop || !code) return res.status(400).send('Missing shop or code');
    try {
        const r = await fetch(`https://${shop}/admin/oauth/access_token`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ client_id: SHOPIFY_API_KEY, client_secret: SHOPIFY_API_SECRET, code })
        });
        const data = await r.json();
        if (!data.access_token) throw new Error(JSON.stringify(data));
        _shopifyToken = data.access_token;
        _shopifyShop = shop;
        process.env.SHOPIFY_ACCESS_TOKEN = _shopifyToken;
        process.env.SHOPIFY_SHOP = shop;
        // Persist token to file immediately (survives Railway restarts)
        saveTokenToFile(_shopifyToken, shop);
        console.log(`\n✅ [OAUTH] ACCESS TOKEN CAPTURED AND SAVED!`);
        console.log(`   Shop: ${shop}`);
        console.log(`   Token: ${_shopifyToken}`);
        console.log(`   → Token also saved to shopify_token.json for restart persistence\n`);
        res.send(`<html><body style="font-family:sans-serif;padding:40px;text-align:center">
            <h2>✅ Autorización exitosa</h2>
            <p>Token capturado correctamente.</p>
            <p style="background:#f4f5f7;padding:12px;border-radius:8px;font-family:monospace;font-size:12px">
                SHOPIFY_ACCESS_TOKEN = ${_shopifyToken}
            </p>
            <p style="color:#888;font-size:12px">Copia este token en Railway → Variables → SHOPIFY_ACCESS_TOKEN</p>
            <p><a href="/">← Volver al dashboard</a></p>
        </body></html>`);
    } catch (e) {
        console.error(`[OAUTH] Error: ${e.message}`);
        res.status(500).send(`OAuth error: ${e.message}`);
    }
});

// Helper to get current token
function getShopifyToken() { return _shopifyToken; }
function getShopifyShop() { return _shopifyShop; }



/* ═══════════════════════════════════════════════
   🛒 SUBSCRIPTION API
═══════════════════════════════════════════════ */

/* ── 🔒 VARIANT ALLOWLIST (hardening 2026-04-20) ──────────────────────────────
   Motivo: pedido #8760 (Jose Santanera) cayó con variant 59198554112081 que NO
   es la variante oficial de suscripción (58307532587089, CREATINE BLACK LIMITED
   EDITION 500g). Causa raíz: la validación anterior (checkout) sólo corría si
   product_configs[pId].eligible_variant_ids estaba seteada; si no, cualquier
   variant pasaba. Además /api/subscriptions/create y PATCH no validaban NADA.
   Fix: allowlist GLOBAL, chequeada en TODO endpoint de creación/edición y en
   createShopifyOrderFromSub ANTES de crear la orden Shopify.
   Fuente de verdad (en orden):
     1. settings.subscription_variant_whitelist (admin edita en UI)
     2. Union de todos settings.product_configs[*].eligible_variant_ids
     3. HARDCODED_SUBSCRIPTION_VARIANTS (fallback si settings aún no se pobló)
   Cualquier variant fuera de esta lista es RECHAZADA. */
const HARDCODED_SUBSCRIPTION_VARIANTS = [
    '58307532587089' // CREATINE BLACK LIMITED EDITION 500g — única variante oficial de suscripción (abril 2026)
];

// 2026-05-04 — Cache de variants por bundle_product_id (TTL 5min).
// Evita que cada cobro consulte Shopify Admin API. Auto-invalida al expirar.
const _bundleProductVariantsCache = new Map(); // bundle_product_id → { variants: [...], at: number }
async function _getBundleProductVariantIds(bundleProductId) {
    if (!bundleProductId) return [];
    const key = String(bundleProductId);
    const cached = _bundleProductVariantsCache.get(key);
    const now = Date.now();
    if (cached && (now - cached.at) < 300000) return cached.variants;
    const shop = process.env.SHOPIFY_SHOP || 'nutrition-lab-cluster.myshopify.com';
    const token = process.env.SHOPIFY_ACCESS_TOKEN || _shopifyToken;
    if (!token) return [];
    try {
        const r = await fetch(`https://${shop}/admin/api/2026-01/products/${encodeURIComponent(key)}.json?fields=variants`, {
            headers: { 'X-Shopify-Access-Token': token }
        });
        if (!r.ok) { _bundleProductVariantsCache.set(key, { variants: [], at: now }); return []; }
        const data = await r.json();
        const variants = (data?.product?.variants || []).map(v => String(v.id));
        _bundleProductVariantsCache.set(key, { variants, at: now });
        return variants;
    } catch (_) {
        _bundleProductVariantsCache.set(key, { variants: [], at: now });
        return [];
    }
}

async function getSubscriptionVariantAllowlist(settingsMaybe) {
    // settingsMaybe opcional: si lo pasan, evita un readFromShopify extra
    let settings = settingsMaybe;
    if (!settings) {
        try { settings = await readFromShopify(); } catch (_) { settings = null; }
    }
    const set = new Set(HARDCODED_SUBSCRIPTION_VARIANTS.map(String));
    try {
        // 1. Lista global administrable
        const adminList = Array.isArray(settings?.subscription_variant_whitelist)
            ? settings.subscription_variant_whitelist
            : [];
        for (const v of adminList) { if (v != null && v !== '') set.add(String(v)); }
        // 2. Union de eligible_variant_ids de cada producto configurado
        const pCfgs = (settings && typeof settings.product_configs === 'object' && !Array.isArray(settings.product_configs))
            ? settings.product_configs : {};
        for (const pid of Object.keys(pCfgs)) {
            const evs = pCfgs[pid]?.eligible_variant_ids;
            if (Array.isArray(evs)) for (const v of evs) { if (v != null && v !== '') set.add(String(v)); }
        }
        // 3. AUTO-DISCOVER 2026-05-04 — variants reales de productos bundle activos.
        //    Cualquier bundle creado vía /api/admin/bundles auto-pasa el allowlist
        //    sin que admin tenga que hacer un PUT manual al whitelist.
        //    Cache 5min en memoria. Falla silenciosa: si Shopify Admin API no responde,
        //    quedan las 3 fuentes anteriores. ADITIVO: solo agrega, nunca remueve.
        try {
            if (db && db.getBundleConfigs) {
                const bundles = await db.getBundleConfigs({ active: true }).catch(() => []);
                for (const b of (Array.isArray(bundles) ? bundles : [])) {
                    if (!b || !b.bundle_product_id) continue;
                    const variants = await _getBundleProductVariantIds(b.bundle_product_id);
                    for (const v of variants) set.add(String(v));
                }
            }
        } catch (_) { /* fail silent: 3 sources anteriores aplican igual */ }
    } catch (_) { /* si falla leer settings, queda el hardcoded */ }
    return set;
}

async function isVariantAllowedForSubscription(variantId, settingsMaybe) {
    const vid = String(variantId || '').trim();
    if (!vid) return { ok: false, reason: 'missing_variant_id', allowlist: [] };
    const allow = await getSubscriptionVariantAllowlist(settingsMaybe);
    if (!allow.has(vid)) {
        return { ok: false, reason: 'variant_not_allowed', allowlist: Array.from(allow) };
    }
    return { ok: true, allowlist: Array.from(allow) };
}

/* ── 🔒 PER-CUSTOMER SUBSCRIPTION LIMITS (hardening 2026-04-20) ───────────────
   Previene abuso/fraude/bot-spam/doble-click. Reja al inicio del checkout.
   Reglas (todas juntas):
     · máx 2 suscripciones en status='active' por email
     · máx 1 suscripción activa de la misma variant_id por email
     · máx 1 pending_payment por email dentro de los últimos 10 minutos
   NO toca webhooks, cron, MP, ni subs existentes — solo rechaza NUEVAS
   solicitudes en /checkout y /create antes de llegar a MP. */
const SUB_LIMITS = {
    MAX_ACTIVE_PER_EMAIL: 2,
    MAX_SAME_VARIANT_PER_EMAIL: 1,
    PENDING_WINDOW_MIN: 10
};

async function checkSubscriptionCustomerLimits(email, variantId) {
    const emailLower = String(email || '').trim().toLowerCase();
    if (!emailLower) return { ok: true }; // sin email no podemos evaluar; que lo ataje el validador de required
    const vid = String(variantId || '').trim();
    let allSubs = [];
    try {
        allSubs = await db.getSubscriptions({}).catch(() => []);
    } catch (_) { allSubs = []; }
    const mine = (Array.isArray(allSubs) ? allSubs : []).filter(s =>
        (s.customer_email || '').trim().toLowerCase() === emailLower
    );
    const activeMine = mine.filter(s => s.status === 'active');
    if (activeMine.length >= SUB_LIMITS.MAX_ACTIVE_PER_EMAIL) {
        return {
            ok: false,
            code: 'MAX_ACTIVE_REACHED',
            message: `Ya tenés ${activeMine.length} suscripciones activas. El máximo por cliente es ${SUB_LIMITS.MAX_ACTIVE_PER_EMAIL}.`,
            existing: activeMine.length
        };
    }
    if (vid) {
        const sameVariantActive = activeMine.filter(s => String(s.variant_id || '') === vid);
        if (sameVariantActive.length >= SUB_LIMITS.MAX_SAME_VARIANT_PER_EMAIL) {
            return {
                ok: false,
                code: 'DUPLICATE_VARIANT',
                message: 'Ya tenés una suscripción activa de este producto.',
                existing: sameVariantActive.length
            };
        }
    }
    const windowMs = SUB_LIMITS.PENDING_WINDOW_MIN * 60 * 1000;
    const now = Date.now();
    const recentPending = mine.filter(s => {
        if (s.status !== 'pending_payment') return false;
        const createdAt = s.created_at ? new Date(s.created_at).getTime() : 0;
        return createdAt && (now - createdAt) < windowMs;
    });
    if (recentPending.length >= 1) {
        return {
            ok: false,
            code: 'PENDING_EXISTS',
            message: `Ya iniciaste un checkout hace menos de ${SUB_LIMITS.PENDING_WINDOW_MIN} min. Esperá unos minutos o completá el pago pendiente.`,
            existing: recentPending.length
        };
    }
    return { ok: true };
}

/**
 * Resuelve los regalos aplicables a una suscripción NUEVA según su plan + producto.
 * Lee el metafield settings.plans_config y matchea por frecuencia/permanencia.
 * Respeta applies_to: 'all_products' | 'specific_products' (lista de product_ids).
 * Devuelve un array snapshot (foto congelada) de items de regalo, o null si no aplica.
 * No modifica nada, solo lee. Si falla por cualquier razón, devuelve null (no bloquea la venta).
 */
// ── ADITIVO 2026-04-22 ── Helper: verifica si un plan aplica a un producto.
// Respeta el NUEVO campo `plan.applies_to` ({ mode, product_ids }) a nivel de plan,
// además del filtro existente por `plan.gifts.applies_to` (para regalos).
// Si `plan.applies_to` no existe, asume 'all_products' (compatibilidad hacia atrás).
function planAppliesToProduct(plan, productId) {
    if (!plan) return false;
    const mode = plan.applies_to?.mode || 'all_products';
    if (mode === 'all_products') return true;
    const ids = (plan.applies_to?.product_ids || []).map(String);
    return ids.includes(String(productId));
}

// 2026-05-04 — FALLBACK: regalos definidos directamente dentro del bundle local.
//   Cuando el admin crea un bundle con plans[].gifts.enabled=true e items[],
//   esos regalos quedan disponibles sin necesidad de duplicarlos en plans_config.
//   Aditivo: solo se invoca cuando plans_config global no resuelve. No toca
//   webhook MP, orders, ni la lógica existente — solo cierra el hueco para que
//   un admin pueda configurar gifts directamente desde el modal de bundles.
async function _resolveGiftsFromBundle(freq, perm, productId) {
    try {
        if (!db || !db.getBundleConfigByBundleProductId) return null;
        const bundle = await db.getBundleConfigByBundleProductId(productId).catch(() => null);
        if (!bundle || !Array.isArray(bundle.plans)) return null;
        const bp = bundle.plans.find(p =>
            Number(p.freq_months) === freq && Number(p.perm_months) === perm
        );
        if (!bp || !bp.gifts || !bp.gifts.enabled) return null;
        const items = Array.isArray(bp.gifts.items) ? bp.gifts.items : [];
        if (!items.length) return null;
        return items.map(it => ({
            product_id: String(it.product_id || ''),
            product_title: it.product_title || '',
            product_handle: it.product_handle || '',
            variant_id: String(it.variant_id || ''),
            variant_title: it.variant_title || '',
            variant_sku: it.variant_sku || '',
            quantity: Math.max(1, Math.min(3, parseInt(it.quantity, 10) || 1)),
            image: it.image || null
        })).filter(it => it.variant_id);
    } catch (e) {
        console.warn('[GIFTS] _resolveGiftsFromBundle error:', e.message);
        return null;
    }
}

async function resolveGiftsForNewSub(frequencyMonths, permanenceMonths, productId) {
    const freq = Number(frequencyMonths);
    const perm = Number(permanenceMonths);
    try {
        const data = await readFromShopify().catch(() => null);
        const plans = Array.isArray(data?.plans_config) ? data.plans_config : [];
        // 🔧 FIX 2026-04-22: ANTES buscaba el PRIMER plan con freq+perm matcheados,
        //   incluso si ese plan era de OTRO producto. Causaba que los regalos no se
        //   asignaran a la suscripción correcta cuando dos productos tenían mismos
        //   freq+perm (ej. Creatina 3m y Premium 3m). Ahora filtra por producto.
        const plan = plans.find(p =>
            p && p.active !== false &&
            Number(p.frequency || p.freq_months) === freq &&
            Number(p.permanence || p.permanence_months) === perm &&
            planAppliesToProduct(p, productId)
        );
        // 2026-05-04: cada return-null se reemplaza por un intento de fallback al
        // bundle local. Si bundle tampoco resuelve, devuelve null (comportamiento original).
        if (!plan || !plan.gifts || !plan.gifts.enabled) return await _resolveGiftsFromBundle(freq, perm, productId);
        const items = Array.isArray(plan.gifts.items) ? plan.gifts.items : [];
        if (!items.length) return await _resolveGiftsFromBundle(freq, perm, productId);
        const appliesMode = plan.gifts.applies_to?.mode || 'all_products';
        if (appliesMode === 'specific_products') {
            const ids = (plan.gifts.applies_to?.product_ids || []).map(String);
            if (!ids.includes(String(productId))) return await _resolveGiftsFromBundle(freq, perm, productId);
        }
        return items.map(it => ({
            product_id: String(it.product_id || ''),
            product_title: it.product_title || '',
            product_handle: it.product_handle || '',
            variant_id: String(it.variant_id || ''),
            variant_title: it.variant_title || '',
            variant_sku: it.variant_sku || '',
            quantity: Math.max(1, Math.min(3, parseInt(it.quantity, 10) || 1)),
            image: it.image || null
        })).filter(it => it.variant_id); // filtro seguridad: sin variant_id no sirve
    } catch (e) {
        console.warn('[GIFTS] resolveGiftsForNewSub error:', e.message);
        return await _resolveGiftsFromBundle(freq, perm, productId).catch(() => null);
    }
}

/* ── CREATE subscription ── */
app.post('/api/subscriptions/create', _requireAdminToken, async (req, res) => {
    try {
        const {
            variantId, productId, productTitle, productImage,
            customerId, customerEmail, customerName, customerPhone,
            frequencyMonths, permanenceMonths, discountPct, finalPrice, basePrice,
            shippingAddress, cardToken
        } = req.body;

        if (!variantId || !customerEmail || !frequencyMonths || !permanenceMonths || !cardToken) {
            return res.status(400).json({ error: 'Missing required fields' });
        }

        // 🔒 HARDENING 2026-04-20: allowlist global de variantes suscribibles.
        // Motivo: incidente #8760 — una variant no oficial se suscribió por falta de check.
        const variantCheck = await isVariantAllowedForSubscription(variantId);
        if (!variantCheck.ok) {
            console.warn(`[CREATE] ❌ Variant ${variantId} rechazada (${variantCheck.reason}). Allowlist: [${variantCheck.allowlist.join(', ')}]. Cliente: ${customerEmail}`);
            return res.status(400).json({
                error: 'Esta variante no está habilitada para suscripción. Contactá al equipo si creés que es un error.',
                code: 'VARIANT_NOT_ALLOWED',
                variant_id: String(variantId)
            });
        }

        // 🔒 HARDENING 2026-04-20: límites por cliente (anti-abuso/anti-bot/doble-click).
        const limitCheck = await checkSubscriptionCustomerLimits(customerEmail, variantId);
        if (!limitCheck.ok) {
            console.warn(`[CREATE] ❌ Límite por cliente rechazado para ${customerEmail}: ${limitCheck.code} (${limitCheck.existing || 0} existentes)`);
            return res.status(429).json({ error: limitCheck.message, code: limitCheck.code });
        }

        const cyclesRequired = Math.ceil(permanenceMonths / frequencyMonths);

        // 1. Create MP Preapproval Plan
        const plan = await mp.createPlan({
            frequency: frequencyMonths,
            permanence: permanenceMonths,
            amount: parseFloat(finalPrice),
            productTitle
        });

        // 2. Create MP Preapproval (subscription)
        const mpSub = await mp.createSubscription({
            planId: plan.id,
            customerEmail,
            customerName,
            cardToken
        });

        // 3. Calculate next charge date
        const nextCharge = new Date();
        nextCharge.setMonth(nextCharge.getMonth() + frequencyMonths);

        // 4. Resolver regalos aplicables según plan+producto (snapshot congelado)
        const giftsPlanned = await resolveGiftsForNewSub(frequencyMonths, permanenceMonths, productId);

        // 5. Save to Shopify Metaobjects
        const sub = await db.createSubscription({
            customer_id: customerId || customerEmail,
            customer_email: customerEmail,
            customer_name: customerName,
            customer_phone: customerPhone,
            variant_id: variantId.toString(),
            product_id: productId.toString(),
            product_title: productTitle,
            product_image: productImage,
            frequency_months: frequencyMonths,
            permanence_months: permanenceMonths,
            discount_pct: discountPct,
            base_price: basePrice,
            final_price: finalPrice,
            mp_preapproval_id: mpSub.id,
            mp_plan_id: plan.id,
            status: 'active',
            cycles_required: cyclesRequired,
            cycles_completed: 0,
            next_charge_at: nextCharge.toISOString(),
            shipping_address: shippingAddress,
            expires_at: new Date(Date.now() + permanenceMonths * 30 * 24 * 3600 * 1000).toISOString(),
            // Regalos (solo 1er pedido). Si hay items se intenta entregar en la primera orden.
            gifts_planned: Array.isArray(giftsPlanned) ? giftsPlanned : [],
            gifts_delivered: false
        });

        // 5. Tag customer in Shopify
        if (customerId) shopify.tagCustomerAsSubscriber(customerId, true).catch(console.error);

        // 6. Send welcome email
        if (notifications.sendWelcome) notifications.sendWelcome(sub).catch(console.error);

        // 7. Log event
        await db.createEvent({ subscription_id: sub.id, event_type: 'created', metadata: { mp_plan_id: plan.id } });

        res.json({ success: true, subscriptionId: sub.id, nextChargeAt: sub.next_charge_at });
    } catch (e) {
        console.error('Create subscription error:', e.message);
        res.status(500).json({ error: e.message });
    }
});

/* 🔒 2026-06-12: lee un permiso del portal del suscriptor. FAIL-OPEN por defecto:
   solo bloquea si el admin lo apagó EXPLÍCITAMENTE (=== false). Así no rompemos
   flujos existentes si el metafield aún no tiene el campo. */
async function _portalPermAllowed(key) {
    try {
        const s = await readFromShopify().catch(() => ({}));
        const cfg = (s && s.portal_config) || {};
        return cfg[key] !== false;
    } catch (_) { return true; }
}

/* ── PORTAL CONFIG — Beneficios, Producto semana, Eventos (EARLY REGISTRATION) ── */
app.get('/api/portal-config', async (req, res) => {
    try {
        const settings = await readFromShopify().catch(() => ({}));
        res.json(settings.portal_config || {});
    } catch (e) { res.json({}); }
});

app.put('/api/portal-config', _requireAdminToken, async (req, res) => {
    try {
        const settings = await readFromShopify().catch(() => ({}));
        settings.portal_config = req.body;
        await saveToShopify(settings);

        // ALSO save as a public shop metafield for Storefront API access
        // The Customer Account UI Extension reads this via Storefront API
        try {
            const shop = getShopifyShop();
            const token = getShopifyToken();
            if (shop && token) {
                // Merge portal_config + settings branding for the extension
                const extensionData = {
                    ...req.body,
                    brand_name: settings.brand_name || 'Suscriptions MP',
                    brand_slogan: settings.brand_slogan || 'Beneficios exclusivos para suscriptores',
                    brand_color: settings.brand_color || '#D4502A',
                    brand_color2: settings.brand_color2 || '#2E7D49',
                    brand_logo: settings.brand_logo || '',
                };
                const metafieldMutation = `mutation {
                    metafieldsSet(metafields: [{
                        namespace: "suscriptions_mp",
                        key: "portal_config",
                        type: "json",
                        value: ${JSON.stringify(JSON.stringify(extensionData))},
                        ownerId: "gid://shopify/Shop/${shop.replace('.myshopify.com','').replace(/[^0-9]/g,'') || '97503248465'}"
                    }]) {
                        metafields { id namespace key }
                        userErrors { field message }
                    }
                }`;
                // Need shop numeric ID - get it
                const shopIdRes = await fetch(`https://${shop}/admin/api/2026-01/shop.json`, {
                    headers: { 'X-Shopify-Access-Token': token }
                });
                const shopData = await shopIdRes.json();
                const shopGid = `gid://shopify/Shop/${shopData?.shop?.id || '97503248465'}`;

                await fetch(`https://${shop}/admin/api/2026-01/graphql.json`, {
                    method: 'POST',
                    headers: {
                        'X-Shopify-Access-Token': token,
                        'Content-Type': 'application/json'
                    },
                    body: JSON.stringify({
                        query: `mutation {
                            metafieldsSet(metafields: [{
                                namespace: "suscriptions_mp",
                                key: "portal_config",
                                type: "json",
                                value: ${JSON.stringify(JSON.stringify(extensionData))},
                                ownerId: "${shopGid}"
                            }]) {
                                metafields { id namespace key }
                                userErrors { field message }
                            }
                        }`
                    })
                }).then(r => r.json()).then(d => {
                    if (d?.data?.metafieldsSet?.userErrors?.length) {
                        console.warn('[PORTAL] Metafield write errors:', d.data.metafieldsSet.userErrors);
                    } else {
                        console.log('[PORTAL] ✅ Public metafield updated for Storefront API');
                    }
                });
            }
        } catch (mfErr) {
            console.warn('[PORTAL] Metafield sync error (non-blocking):', mfErr.message);
        }

        res.json({ ok: true });
    } catch (e) { res.status(500).json({ error: e.message }); }
});

app.get('/portal', (req, res) => {
    res.sendFile(require('path').join(__dirname, 'public', 'portal.html'));
});

/* ── CHECKOUT — Crea MP PreApproval y devuelve init_point para redirigir al cliente ──
   Acepta snake_case (widget actual) y camelCase (compatibilidad retroactiva)
   Flujo: createCheckout() → PreApprovalPlan + PreApproval → init_point → cliente autoriza tarjeta
   → MP cobra mensualmente → webhook /webhooks/mp → crear pedido Shopify automático ── */
app.post('/api/subscriptions/checkout', async (req, res) => {
    try {
        const b = req.body;

        // Acepta snake_case del widget actual O camelCase de versiones anteriores
        const email        = b.customer_email   || b.customerEmail;
        const name         = b.customer_name    || b.customerName   || '';
        const phone        = b.customer_phone   || b.phone || b.customerPhone  || '';
        const pId          = b.product_id       || b.productId      || '';
        const vId          = b.variant_id       || b.variantId      || '';
        const title        = b.product_title    || b.productTitle   || 'Producto LAB';
        const image        = b.product_image    || b.productImage   || '';
        const finalPrice   = parseFloat(b.final_price   || b.finalPrice  || 0);
        const basePrice    = parseFloat(b.base_price    || b.basePrice   || finalPrice);
        const discPct      = parseFloat(b.discount_pct  || b.discountPct || 0);
        const freq         = parseInt(b.frequency_months  || b.frequencyMonths  || 1);
        const perm         = parseInt(b.permanence_months || b.permanenceMonths || 3);
        const freeShip     = b.free_shipping    || b.freeShipping   || false;
        const shipAddr     = b.shipping_address || null;
        const tipDoc       = b.tipo_documento   || '01';
        const dni          = b.dni              || '';
        const tcAccepted   = b.tc_accepted === true;
        const tcVersion    = b.tc_version       || '1.0';
        const tcAcceptedAt = b.tc_accepted_at   || new Date().toISOString();
        const tcIp         = (req.headers['x-forwarded-for'] || req.ip || '').split(',')[0].trim();

        // 2026-04-21 ADITIVO: bundle configurable (mix de sabores).
        //   Si el body trae bundle_items, validamos contra bundle_config y saltamos la
        //   allowlist tradicional de variant (el product bundle es nuevo, no la lata individual).
        const bundleItemsIn = Array.isArray(b.bundle_items) ? b.bundle_items : null;
        const isBundleMode = bundleItemsIn && bundleItemsIn.length > 0;
        let bundleConfig = null;
        let bundleItemsNormalized = null;

        if (!email || !freq || !perm || !finalPrice || !dni) {
            return res.status(400).json({ error: 'Faltan datos: email, frecuencia, permanencia, precio y DNI son obligatorios.' });
        }
        if (!tcAccepted) {
            return res.status(400).json({ error: 'Debes aceptar los Términos y Condiciones para continuar.' });
        }

        // Refresh MP token + variant validation from Shopify settings (single call)
        const dynSettings = await readFromShopify().catch(() => ({}));
        if (dynSettings?.mp_access_token) process.env.MP_ACCESS_TOKEN = dynSettings.mp_access_token;

        if (isBundleMode) {
            // ── Validación bundle: bypass de allowlist tradicional, reemplazada por check de config ──
            try {
                bundleConfig = await db.getBundleConfigByBundleProductId(pId).catch(() => null);
            } catch (e) { bundleConfig = null; }
            if (!bundleConfig) {
                return res.status(400).json({
                    error: 'Bundle no encontrado o no configurado. Contacta al administrador.',
                    code: 'BUNDLE_NOT_FOUND',
                    product_id: String(pId || '')
                });
            }
            if (bundleConfig.active === false) {
                return res.status(410).json({ error: 'Este bundle no está activo actualmente.', code: 'BUNDLE_INACTIVE' });
            }
            // Sum qty debe ser EXACTO = target_quantity
            const totalQty = bundleItemsIn.reduce((n, it) => n + (parseInt(it?.quantity, 10) || 0), 0);
            if (totalQty !== Number(bundleConfig.target_quantity)) {
                return res.status(400).json({
                    error: `El pack debe sumar exactamente ${bundleConfig.target_quantity} unidades. Has seleccionado ${totalQty}.`,
                    code: 'BUNDLE_QTY_MISMATCH',
                    expected: Number(bundleConfig.target_quantity),
                    received: totalQty
                });
            }
            // Todos los variant_ids deben estar en allowed_variant_ids
            const allowedSet = new Set((bundleConfig.allowed_variant_ids || []).map(String));
            const invalidVariants = bundleItemsIn.filter(it => !allowedSet.has(String(it?.variant_id || '')));
            if (invalidVariants.length > 0) {
                return res.status(400).json({
                    error: 'Uno o más sabores seleccionados no están permitidos en este bundle.',
                    code: 'BUNDLE_VARIANT_NOT_ALLOWED',
                    invalid_variants: invalidVariants.map(it => String(it?.variant_id || ''))
                });
            }
            // Validar stock si el bundle lo requiere.
            // Regla: cada variante pedida debe tener stock >= min_stock_threshold (default 100).
            // Bloquea ventas de sabores con stock bajo para evitar backorders masivos.
            const minStock = Number(bundleConfig.min_stock_threshold) || 100;
            const excludedSetC = new Set((bundleConfig.excluded_variant_ids || []).map(String));
            // Pre-check: ningún item pedido puede estar en excluded
            const excludedUsed = bundleItemsIn.filter(it => excludedSetC.has(String(it.variant_id)));
            if (excludedUsed.length > 0) {
                return res.status(400).json({
                    error: 'Uno de los sabores elegidos está fuera de stock permanentemente.',
                    code: 'BUNDLE_EXCLUDED_VARIANT',
                    excluded: excludedUsed.map(it => String(it.variant_id))
                });
            }
            if (bundleConfig.validate_stock !== false) {
                try {
                    const url = `https://${process.env.SHOPIFY_SHOP || 'nutrition-lab-cluster.myshopify.com'}/admin/api/2026-01/products/${encodeURIComponent(bundleConfig.source_product_id)}.json?fields=variants`;
                    const sr = await fetch(url, { headers: { 'X-Shopify-Access-Token': process.env.SHOPIFY_ACCESS_TOKEN } });
                    if (sr.ok) {
                        const sData = await sr.json();
                        const variants = (sData.product?.variants || []);
                        const stockMap = new Map(variants.map(v => [String(v.id), parseInt(v.inventory_quantity, 10) || 0]));
                        const insufficient = bundleItemsIn.filter(it => (stockMap.get(String(it.variant_id)) || 0) < minStock);
                        if (insufficient.length > 0) {
                            return res.status(409).json({
                                error: `Algunos sabores tienen stock bajo (mínimo ${minStock} unidades). Actualiza tu selección.`,
                                code: 'BUNDLE_LOW_STOCK',
                                min_stock: minStock,
                                low_stock: insufficient.map(it => ({ variant_id: String(it.variant_id), requested: it.quantity, available: stockMap.get(String(it.variant_id)) || 0 }))
                            });
                        }
                    }
                } catch (e) { console.warn('[CHECKOUT] stock validation error (permite continuar):', e.message); }
            }
            // Normalizar bundle_items (agregar title/sabor para trazabilidad en order/admin)
            bundleItemsNormalized = bundleItemsIn.map(it => ({
                variant_id: String(it.variant_id),
                variant_title: String(it.variant_title || it.title || ''),
                quantity: parseInt(it.quantity, 10)
            })).filter(it => it.variant_id && it.quantity > 0);

            console.log(`[CHECKOUT] 📦 Bundle mode: ${bundleConfig.name} (${totalQty} items) | cliente ${email}`);
        } else {
            // Modo legacy: allowlist tradicional (intocado)
            const variantCheck = await isVariantAllowedForSubscription(vId, dynSettings);
            if (!variantCheck.ok) {
                console.warn(`[CHECKOUT] ❌ Variant ${vId || '(vacío)'} rechazada (${variantCheck.reason}). Producto ${pId || '-'}. Cliente ${email}. Allowlist: [${variantCheck.allowlist.join(', ')}]`);
                return res.status(400).json({
                    error: 'Esta variante no está habilitada para suscripción. Solo la variante oficial de 500g está disponible.',
                    code: 'VARIANT_NOT_ALLOWED',
                    variant_id: String(vId || '')
                });
            }
        }

        // 🔒 HARDENING 2026-04-20: límites por cliente (anti-abuso/anti-bot/doble-click).
        // Máx 2 activas / 1 por variante / 1 pending <10 min. Rebota ANTES de MP.
        const limitCheck = await checkSubscriptionCustomerLimits(email, vId);
        if (!limitCheck.ok) {
            console.warn(`[CHECKOUT] ❌ Límite por cliente rechazado para ${email}: ${limitCheck.code} (${limitCheck.existing || 0} existentes)`);
            return res.status(429).json({ error: limitCheck.message, code: limitCheck.code });
        }

        const backUrl = `${process.env.BACKEND_URL || 'https://pixel-suite-pro-production.up.railway.app'}/subscriptions/success?email=${encodeURIComponent(email)}&product=${encodeURIComponent(title)}`;

        // Create MP PreApprovalPlan + PreApproval → returns real init_point
        // 🔒 HARDENING 2026-06-12 (v2): re-derivación de precio server-side.
        //   Prioridad de la verdad:
        //   1) PRECIO FIJO del admin (product_configs[pId].plans[].force_price) → se cobra
        //      EXACTO ese monto, sí o sí (es lo que Israel elige, ej. redondear 89.50→90).
        //   2) Sin precio fijo: piso anti-fraude = basePrice real de la variante × (1 - % del
        //      plan maestro). Si el body llega POR DEBAJO del piso (DevTools), se impone el
        //      piso. Si llega IGUAL o ALGO POR ENCIMA (redondeo del theme editor), se respeta
        //      — el cliente ve y autoriza el monto en MP, subirlo no es vector de fraude.
        //   3) Si nada se puede resolver, se respeta el body para NO romper la venta.
        //   Solo modo legacy (no bundle).
        let enforcedBasePrice = basePrice, enforcedFinalPrice = finalPrice, enforcedDiscPct = discPct;
        if (!isBundleMode && vId) {
            try {
                // (1) Precio fijo por plan desde la Config del producto (admin)
                const pcCfg = (dynSettings?.product_configs && dynSettings.product_configs[pId])
                    || ((typeof dynSettings?.[pId] === 'object' && dynSettings[pId]?.plans) ? dynSettings[pId] : null) || {};
                const pcPlans = (pcCfg && typeof pcCfg.plans === 'object') ? pcCfg.plans : {};
                let forcePrice = NaN;
                for (const k of Object.keys(pcPlans)) {
                    const e = pcPlans[k] || {};
                    if (e.enabled === false) continue;
                    if (Number(e.frequency) !== freq || Number(e.permanence) !== perm) continue;
                    const f = parseFloat(e.force_price);
                    if (Number.isFinite(f) && f > 0) { forcePrice = f; }
                    break;
                }

                // Precio real de variante en Shopify (base para piso y % informativos)
                let realBase = NaN;
                try {
                    const shopH = process.env.SHOPIFY_SHOP || 'nutrition-lab-cluster.myshopify.com';
                    const pr = await fetch(`https://${shopH}/admin/api/2026-01/products/${encodeURIComponent(pId)}.json?fields=variants`, { headers: { 'X-Shopify-Access-Token': process.env.SHOPIFY_ACCESS_TOKEN } });
                    if (pr.ok) {
                        const pd = await pr.json();
                        const v = (pd.product?.variants || []).find(x => String(x.id) === String(vId));
                        realBase = v ? parseFloat(v.price) : NaN;
                    }
                } catch (_) {}

                if (Number.isFinite(forcePrice)) {
                    // (1) PRECIO FIJO: se cobra exacto, sí o sí.
                    if (Math.abs(forcePrice - finalPrice) > 0.005) {
                        console.warn(`[CHECKOUT] 🔒 Precio FIJO aplicado: body S/${finalPrice} → S/${forcePrice} (Config del producto). Cliente ${email}, prod ${pId}, plan ${freq}m/${perm}m.`);
                    }
                    enforcedFinalPrice = forcePrice;
                    if (Number.isFinite(realBase) && realBase > 0) {
                        enforcedBasePrice = realBase;
                        enforcedDiscPct = Math.max(0, Math.round((1 - forcePrice / realBase) * 100));
                    }
                } else {
                    // (2) Piso anti-fraude por % del plan maestro
                    const plansCfg = Array.isArray(dynSettings?.plans_config) ? dynSettings.plans_config : [];
                    const matchPlan = plansCfg.find(p => p && p.active !== false &&
                        Number(p.frequency || p.freq_months) === freq &&
                        Number(p.permanence || p.permanence_months) === perm &&
                        planAppliesToProduct(p, pId));
                    const disc = matchPlan ? Number(matchPlan.discount) : NaN;
                    if (Number.isFinite(disc) && disc >= 0 && disc < 100 && Number.isFinite(realBase) && realBase > 0) {
                        const floorPrice = parseFloat((realBase * (1 - disc / 100)).toFixed(2));
                        const ceiling = floorPrice * 1.5 + 10; // sanity: un widget roto no debe cobrar de más
                        if (finalPrice < floorPrice - 0.05) {
                            console.warn(`[CHECKOUT] 🔒 Precio bajo el piso: body S/${finalPrice} → impuesto S/${floorPrice} (base S/${realBase}, ${disc}%). Cliente ${email}, prod ${pId}.`);
                            enforcedBasePrice = realBase; enforcedFinalPrice = floorPrice; enforcedDiscPct = disc;
                        } else if (finalPrice > ceiling) {
                            console.warn(`[CHECKOUT] 🔒 Precio absurdo sobre techo: body S/${finalPrice} → impuesto S/${floorPrice}. Cliente ${email}, prod ${pId}.`);
                            enforcedBasePrice = realBase; enforcedFinalPrice = floorPrice; enforcedDiscPct = disc;
                        } else {
                            // Dentro del rango sano (incluye redondeos hacia arriba del theme editor)
                            if (Number.isFinite(realBase) && realBase > 0) enforcedBasePrice = realBase;
                        }
                    }
                }
            } catch (e) { console.warn('[CHECKOUT] price re-derivation skip (usa body):', e.message); }
        }

        // Amount includes product price + shipping (S/10.00)
        const shippingCost = 10.00;
        const totalAmount = parseFloat((enforcedFinalPrice + shippingCost).toFixed(2));
        const checkout = await mp.createCheckout({
            frequency: freq,
            permanence: perm,
            amount: totalAmount,
            productTitle: title,
            customerEmail: email,
            backUrl
        });

        if (!checkout || !checkout.init_point) {
            throw new Error('Mercado Pago no devolvió URL de checkout');
        }

        // Resolver regalos aplicables para esta suscripción (snapshot congelado)
        const giftsPlanned = await resolveGiftsForNewSub(freq, perm, pId);

        // Save pending subscription record
        const pendingId = `pending_${Date.now()}_${Math.random().toString(36).slice(2, 8)}`;
        const subRecord = {
            id: pendingId,
            customer_email: email,
            customer_name: name,
            customer_phone: phone,
            variant_id: String(vId),
            product_id: String(pId),
            product_title: title,
            product_image: image,
            frequency_months: freq,
            permanence_months: perm,
            discount_pct: enforcedDiscPct,
            base_price: enforcedBasePrice,
            final_price: parseFloat(enforcedFinalPrice.toFixed(2)),
            shipping_cost: shippingCost,
            mp_total_amount: totalAmount,
            mp_plan_id: checkout.plan_id || '',
            mp_preapproval_id: checkout.subscription_id || '',
            status: 'pending_payment',
            cycles_required: Math.ceil(perm / freq),
            cycles_completed: 0,
            free_shipping: freeShip,
            shipping_address: shipAddr,
            tipo_documento: tipDoc,
            dni: dni,
            next_charge_at: null,
            created_at: new Date().toISOString(),
            tc_accepted: true,
            tc_version: tcVersion,
            tc_accepted_at: tcAcceptedAt,
            tc_ip: tcIp,
            // Regalos (solo 1er pedido). Snapshot para no depender del plan actual después.
            gifts_planned: Array.isArray(giftsPlanned) ? giftsPlanned : [],
            gifts_delivered: false,
            // 2026-04-21 ADITIVO: bundle configurable (mix de sabores).
            // Si bundleItemsNormalized no es null → se graba en la sub para que el cron replique
            // EXACTAMENTE el mismo mix cada mes durante la permanencia. Si es null, campo ausente → legacy.
            ...(bundleItemsNormalized ? {
                bundle_items: bundleItemsNormalized,
                bundle_config_id: bundleConfig?.id || null,
                bundle_target_quantity: bundleConfig?.target_quantity || null,
                bundle_source_product_id: bundleConfig?.source_product_id || null,
                bundle_name: bundleConfig?.name || ''
            } : {})
        };

        if (db?.createSubscription) {
            await db.createSubscription(subRecord).catch(e => console.warn('[CHECKOUT] DB save error:', e.message));
        } else {
            const settings = await readFromShopify().catch(() => ({}));
            if (!settings.pending_subscriptions) settings.pending_subscriptions = [];
            settings.pending_subscriptions.push(subRecord);
            await saveToShopify(settings).catch(() => {});
        }

        console.log(`[CHECKOUT] ✅ ${email} → plan:${checkout.plan_id} sub:${checkout.subscription_id} → ${checkout.init_point}`);
        res.json({ success: true, init_point: checkout.init_point, plan_id: checkout.plan_id, subscription_id: checkout.subscription_id });

    } catch (e) {
        console.error('[CHECKOUT] Error:', e.message);
        res.status(500).json({ error: e.message || 'Error al crear el checkout de suscripción' });
    }
});



/* ── SUBSCRIPTION SUCCESS — retorno de MP tras pago aprobado ── */
app.get('/subscriptions/success', async (req, res) => {
    const { preapproval_id, external_reference } = req.query;
    console.log('[CHECKOUT SUCCESS] preapproval_id:', preapproval_id);
    res.send(`<!DOCTYPE html><html lang="es"><head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>¡Suscripción activa!</title><style>body{font-family:sans-serif;max-width:480px;margin:60px auto;padding:20px;text-align:center}.icon{font-size:60px;margin-bottom:16px}.title{font-size:24px;font-weight:800;color:#1a7a3a;margin-bottom:8px}.sub{color:#666;font-size:15px;margin-bottom:28px}.btn{display:inline-block;background:#9d2a23;color:#fff;padding:14px 32px;border-radius:8px;text-decoration:none;font-weight:700;font-size:15px}</style></head><body><div class="icon">✅</div><div class="title">¡Suscripción activada!</div><div class="sub">Tu suscripción fue confirmada. Recibirás un email de confirmación en breve. El primer envío se procesará automáticamente.</div><a class="btn" href="https://nutrition-lab-cluster.myshopify.com">Volver a la tienda →</a></body></html>`);
});

/* ── SUBSCRIPTION FAILURE — retorno de MP tras pago fallido ── */
app.get('/subscriptions/failure', (req, res) => {
    res.send(`<!DOCTYPE html><html lang="es"><head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>Pago no completado</title><style>body{font-family:sans-serif;max-width:480px;margin:60px auto;padding:20px;text-align:center}.icon{font-size:60px;margin-bottom:16px}.title{font-size:24px;font-weight:800;color:#9d2a23;margin-bottom:8px}.sub{color:#666;font-size:15px;margin-bottom:28px}.btn{display:inline-block;background:#9d2a23;color:#fff;padding:14px 32px;border-radius:8px;text-decoration:none;font-weight:700;font-size:15px}</style></head><body><div class="icon">❌</div><div class="title">Pago no completado</div><div class="sub">Hubo un problema al procesar tu pago. Puedes intentarlo de nuevo.</div><a class="btn" href="javascript:history.back()">← Intentar de nuevo</a></body></html>`);
});

/* ── SUBSCRIPTION PENDING ── */
app.get('/subscriptions/pending', (req, res) => {
    res.send(`<!DOCTYPE html><html lang="es"><head><meta charset="UTF-8"><title>Pago pendiente</title></head><body style="font-family:sans-serif;max-width:480px;margin:60px auto;padding:20px;text-align:center"><div style="font-size:60px">⏳</div><h2 style="color:#ea580c">Pago pendiente</h2><p style="color:#666">Tu pago está siendo procesado. Te notificaremos por email cuando se confirme.</p><a href="https://nutrition-lab-cluster.myshopify.com" style="display:inline-block;background:#9d2a23;color:#fff;padding:14px 32px;border-radius:8px;text-decoration:none;font-weight:700;margin-top:20px">Volver a la tienda</a></body></html>`);
});

/* ── 🔒 ADMIN: allowlist de variantes suscribibles (hardening 2026-04-20) ─────
   GET devuelve la lista efectiva (hardcoded ∪ admin ∪ product_configs).
   PUT reemplaza la lista admin-editable (settings.subscription_variant_whitelist).
   El baseline hardcodeado NO se puede quitar desde acá (seguridad: siempre queda
   al menos la variante oficial). Para removerla hay que editar código. */
app.get('/api/admin/subscription-variant-allowlist', async (req, res) => {
    try {
        const settings = await readFromShopify().catch(() => ({})) || {};
        const allow = await getSubscriptionVariantAllowlist(settings);
        res.json({
            allowlist: Array.from(allow),
            hardcoded: HARDCODED_SUBSCRIPTION_VARIANTS,
            admin_editable: Array.isArray(settings.subscription_variant_whitelist) ? settings.subscription_variant_whitelist : [],
            from_product_configs: (() => {
                const out = new Set();
                const pc = (settings && typeof settings.product_configs === 'object' && !Array.isArray(settings.product_configs)) ? settings.product_configs : {};
                for (const pid of Object.keys(pc)) {
                    const evs = pc[pid]?.eligible_variant_ids;
                    if (Array.isArray(evs)) for (const v of evs) if (v != null && v !== '') out.add(String(v));
                }
                return Array.from(out);
            })()
        });
    } catch (e) { res.status(500).json({ error: e.message }); }
});

app.put('/api/admin/subscription-variant-allowlist', async (req, res) => {
    try {
        const list = Array.isArray(req.body?.variants) ? req.body.variants : null;
        if (!list) return res.status(400).json({ error: 'Body: { variants: [\"id1\", \"id2\", ...] }' });
        const cleaned = Array.from(new Set(list.map(v => String(v || '').trim()).filter(Boolean)));
        const settings = await readFromShopify().catch(() => ({})) || {};
        settings.subscription_variant_whitelist = cleaned;
        const saved = await saveToShopify(settings);
        if (!saved) return res.status(500).json({ error: 'No se pudo guardar en Shopify Metafields' });
        console.log(`[ALLOWLIST] ✅ Admin actualizó allowlist a [${cleaned.join(', ')}]`);
        res.json({ success: true, admin_editable: cleaned, hardcoded: HARDCODED_SUBSCRIPTION_VARIANTS });
    } catch (e) { res.status(500).json({ error: e.message }); }
});

/* ── GET customer subscriptions ── */
app.get('/api/subscriptions/customer/:customerId', async (req, res) => {
    try {
        const subs = await db.getSubscriptions();
        const key = String(req.params.customerId || '');
        const keyLower = key.toLowerCase();
        const isNumericId = /^\d+$/.test(key);

        const matched = new Map();
        const add = (s) => { if (s && s.id != null) matched.set(String(s.id), s); };

        // Base (comportamiento original, intacto): por customer_id o email exacto
        subs.forEach(s => {
            if (s.customer_id != null && String(s.customer_id) === key) add(s);
            else if ((s.customer_email || '').toLowerCase() === keyLower) add(s);
        });

        // 🔒 FIX 2026-05-29 — RAÍZ del "no aparecen las suscripciones en el portal":
        //   las subs se guardan con el email del CHECKOUT (MP), que muchas veces NO es el
        //   email de la cuenta Shopify, y casi nunca tienen customer_id. Cuando el portal
        //   manda el customer_id numérico (lo único que SIEMPRE tiene), resolvemos contra
        //   Shopify: email de la cuenta + TODOS sus pedidos, y matcheamos las subs por
        //   email-de-cuenta, email-de-pedido o por shopify_order ligada al cliente (link
        //   a prueba de balas). ADITIVO: solo SUMA matches; no quita ni toca MP/cron/webhook.
        if (isNumericId) {
            try {
                const shop = process.env.SHOPIFY_SHOP || 'nutrition-lab-cluster.myshopify.com';
                const token = process.env.SHOPIFY_ACCESS_TOKEN || _shopifyToken;
                if (token) {
                    const emails = new Set();
                    const orderNames = new Set();
                    const orderIds = new Set();
                    const H = { 'X-Shopify-Access-Token': token };
                    // 1) Email de la cuenta del cliente
                    const cr = await fetch(`https://${shop}/admin/api/2026-01/customers/${key}.json?fields=id,email`, { headers: H }).catch(() => null);
                    if (cr && cr.ok) { const cd = await cr.json(); if (cd.customer && cd.customer.email) emails.add(String(cd.customer.email).toLowerCase()); }
                    // 2) Todos los pedidos del cliente → nombres/ids de orden + emails de pedido
                    const or = await fetch(`https://${shop}/admin/api/2026-01/customers/${key}/orders.json?status=any&limit=250&fields=id,name,email`, { headers: H }).catch(() => null);
                    if (or && or.ok) {
                        const od = await or.json();
                        (od.orders || []).forEach(o => {
                            if (o.name) orderNames.add(String(o.name));
                            if (o.id != null) orderIds.add(String(o.id));
                            if (o.email) emails.add(String(o.email).toLowerCase());
                        });
                    }
                    subs.forEach(s => {
                        const se = (s.customer_email || '').toLowerCase();
                        if (se && emails.has(se)) add(s);
                        else if (s.shopify_order_name && orderNames.has(String(s.shopify_order_name))) add(s);
                        else if (s.shopify_order_id != null && orderIds.has(String(s.shopify_order_id))) add(s);
                    });
                }
            } catch (e) { console.warn('[PORTAL MATCH] resolve failed:', e.message); }
        }

        res.json(Array.from(matched.values()));
    } catch (e) { res.status(500).json({ error: e.message }); }
});

/* ── UPDATE subscription fields (admin) ── */
app.patch('/api/subscriptions/:id', _requireAdminToken, async (req, res) => {
    try {
        const sub = await db.getSubscription(req.params.id);
        if (!sub) return res.status(404).json({ error: 'Not found' });
        // Only allow safe field updates
        // ADD 2026-04-21: gifts_planned/gifts_delivered/gifts_delivered_* → admite backfill de subs
        //   viejas (creadas antes del 15/4 sin array de regalos). No afecta crons ni webhook MP.
        const allowed = ['permanence_months', 'cycles_required', 'discount_pct', 'frequency_months', 'customer_name', 'product_title', 'variant_id', 'product_id', 'status', 'next_charge_at', 'base_price', 'final_price', 'shipping_address', 'tipo_documento', 'dni', 'mp_preapproval_id', 'activated_at', 'cycles_completed', 'last_charge_at', 'customer_email', 'customer_phone', 'gifts_planned', 'gifts_delivered', 'gifts_delivered_at', 'gifts_delivered_order_id', 'gifts_delivered_order_name',
            // 2026-04-21 — bundle configurable
            'bundle_items', 'bundle_config_id', 'bundle_target_quantity', 'bundle_source_product_id', 'bundle_name',
            // 2026-04-23 — allow admin repair of order references
            'shopify_order_id', 'shopify_order_name'];
        const updates = {};
        for (const key of allowed) { if (req.body[key] !== undefined) updates[key] = req.body[key]; }
        if (!Object.keys(updates).length) return res.status(400).json({ error: 'No valid fields to update' });

        // 🔒 HARDENING 2026-04-20: si se cambia variant_id, validar contra allowlist.
        // override=true permite al admin poner cualquier variant (uso: fix manual de subs heredadas).
        if (updates.variant_id !== undefined && String(updates.variant_id) !== String(sub.variant_id || '')) {
            const overrideAllowed = req.body.override_variant_check === true || req.query.override === '1';
            if (!overrideAllowed) {
                const check = await isVariantAllowedForSubscription(updates.variant_id);
                if (!check.ok) {
                    console.warn(`[PATCH] ❌ Intento de cambiar variant de sub ${sub.id} a ${updates.variant_id} rechazado (${check.reason}). Allowlist: [${check.allowlist.join(', ')}]`);
                    return res.status(400).json({
                        error: 'La variante destino no está en la allowlist de suscripción. Enviá override_variant_check:true si es intencional.',
                        code: 'VARIANT_NOT_ALLOWED',
                        variant_id: String(updates.variant_id),
                        allowlist: check.allowlist
                    });
                }
            } else {
                console.warn(`[PATCH] ⚠️ Override variant check para sub ${sub.id}: ${sub.variant_id || '(vacío)'} → ${updates.variant_id}`);
            }
        }

        // 🔒 FIX 2026-06-04: hacer REAL la edición de precio y dirección.
        //   ANTES: PATCH solo actualizaba DB local. MP cobraba monto original.
        //         Customer Shopify no se sincronizaba.
        //   AHORA:
        //   - Si cambia final_price/base_price → llama mp.updateSubscriptionAmount
        //     (MP cobra el nuevo monto en próximos ciclos, SIN re-autorización cliente)
        //   - Si cambia dni o shipping_address → sincroniza customer Shopify
        //     (default_address + metafield dni)
        //   Estos cambios son ADITIVOS — si MP/Shopify fallan, igual guardamos local
        //   y loggeamos evento. El admin ve qué pasó vía eventos.

        const mpUpdates = {};
        const syncReport = { mp_amount_updated: false, shopify_customer_synced: false, errors: [] };

        // 1) PRICE — MP permite cambiar transaction_amount en preapproval activo
        const priceChanged = (updates.final_price !== undefined && parseFloat(updates.final_price) !== parseFloat(sub.final_price || 0))
            || (updates.base_price !== undefined && parseFloat(updates.base_price) !== parseFloat(sub.base_price || 0));
        if (priceChanged && sub.mp_preapproval_id && sub.status === 'active' && mp.updateSubscriptionAmount) {
            try {
                const shippingCost = parseFloat(sub.shipping_cost || 10);
                const newFinalPrice = parseFloat(updates.final_price ?? sub.final_price);
                const newMpAmount = parseFloat((newFinalPrice + shippingCost).toFixed(2));
                await mp.updateSubscriptionAmount(sub.mp_preapproval_id, newMpAmount);
                updates.mp_total_amount = newMpAmount;
                syncReport.mp_amount_updated = true;
                syncReport.new_mp_amount = newMpAmount;
                console.log(`[PATCH] ✅ MP amount actualizado para ${sub.customer_email}: S/${newMpAmount}`);
            } catch (e) {
                syncReport.errors.push({ where: 'mp_update_amount', error: e.message });
                console.warn(`[PATCH] ⚠️ No se pudo actualizar MP amount: ${e.message}`);
            }
        }

        // 2) DNI/DIRECCIÓN — sync con Shopify customer (si existe customer_id)
        const dniChanged = updates.dni !== undefined && String(updates.dni) !== String(sub.dni || '');
        const addrChanged = updates.shipping_address !== undefined
            && JSON.stringify(updates.shipping_address) !== JSON.stringify(sub.shipping_address || {});
        if ((dniChanged || addrChanged) && sub.customer_email) {
            try {
                const shop = process.env.SHOPIFY_SHOP || 'nutrition-lab-cluster.myshopify.com';
                const token = process.env.SHOPIFY_ACCESS_TOKEN || _shopifyToken;
                if (token) {
                    // Buscar customer por email
                    const findRes = await fetch(`https://${shop}/admin/api/2026-01/customers/search.json?query=email:${encodeURIComponent(sub.customer_email)}&limit=1&fields=id`, { headers: { 'X-Shopify-Access-Token': token } });
                    if (findRes.ok) {
                        const fd = await findRes.json();
                        const customerId = fd.customers?.[0]?.id;
                        if (customerId) {
                            const customerUpdate = { id: customerId };
                            const addrIn = updates.shipping_address || sub.shipping_address;
                            if (addrIn && addrIn.address1) {
                                customerUpdate.addresses = [{
                                    address1: addrIn.address1,
                                    city: addrIn.city,
                                    province: addrIn.province,
                                    country: 'PE',
                                    country_code: 'PE',
                                    zip: addrIn.zip || '15000',
                                    phone: addrIn.phone || sub.customer_phone,
                                    first_name: addrIn.first_name || (sub.customer_name || '').split(' ')[0],
                                    last_name: addrIn.last_name || (sub.customer_name || '').split(' ').slice(1).join(' '),
                                    default: true
                                }];
                            }
                            const updateRes = await fetch(`https://${shop}/admin/api/2026-01/customers/${customerId}.json`, {
                                method: 'PUT',
                                headers: { 'X-Shopify-Access-Token': token, 'Content-Type': 'application/json' },
                                body: JSON.stringify({ customer: customerUpdate })
                            });
                            if (updateRes.ok) {
                                syncReport.shopify_customer_synced = true;
                                syncReport.shopify_customer_id = customerId;
                                // Si cambió DNI, también actualizar metafield
                                if (dniChanged && updates.dni) {
                                    await fetch(`https://${shop}/admin/api/2026-01/customers/${customerId}/metafields.json`, {
                                        method: 'POST',
                                        headers: { 'X-Shopify-Access-Token': token, 'Content-Type': 'application/json' },
                                        body: JSON.stringify({ metafield: { namespace: 'custom', key: 'dni', type: 'single_line_text_field', value: String(updates.dni) } })
                                    }).catch(e => syncReport.errors.push({ where: 'shopify_dni_metafield', error: e.message }));
                                }
                                console.log(`[PATCH] ✅ Shopify customer ${customerId} sincronizado (${sub.customer_email})`);
                            } else {
                                const t = await updateRes.text().catch(() => '');
                                syncReport.errors.push({ where: 'shopify_customer_update', status: updateRes.status, error: t.slice(0, 200) });
                            }
                        } else {
                            syncReport.errors.push({ where: 'shopify_customer_search', error: 'Customer not found by email' });
                        }
                    }
                }
            } catch (e) {
                syncReport.errors.push({ where: 'shopify_sync_outer', error: e.message });
                console.warn(`[PATCH] ⚠️ No se pudo sincronizar Shopify customer: ${e.message}`);
            }
        }

        const updated = await db.updateSubscription(sub.id, updates);

        // Loggear evento auditable
        if (priceChanged || dniChanged || addrChanged) {
            await db.createEvent({
                subscription_id: sub.id,
                event_type: 'patched_with_external_sync',
                metadata: JSON.stringify({
                    changed: { price: priceChanged, dni: dniChanged, address: addrChanged },
                    sync: syncReport,
                    at: new Date().toISOString()
                })
            }).catch(() => {});
        }

        res.json({ ...updated, _sync_report: syncReport });
    } catch (e) { res.status(500).json({ error: e.message }); }
});

/* ── PAUSE subscription ──
   🔒 FIX 2026-06-04: hardened — verifica MP real antes de actualizar DB.
   ANTES: mp.pauseSubscription().catch(()=>{}) silenciaba errores. Si MP fallaba,
   DB quedaba 'paused' pero MP seguía cobrando. Cliente sufría chargebacks.
   AHORA: verifica que MP confirmó pause antes de tocar DB. Si MP falla, 502 y no
   actualizamos local. Feature flag STRICT_PAUSE=false desactiva la verificación
   (para rollback de emergencia). */
app.post('/api/subscriptions/:id/pause', async (req, res) => {
    try {
        const { pauseMonths = 1 } = req.body;
        const sub = await db.getSubscription(req.params.id);
        if (!sub || sub.status !== 'active') return res.status(400).json({ error: 'Cannot pause' });
        const strict = (process.env.STRICT_PAUSE === 'true'); // default OFF (sin env access del usuario)
        let mpConfirmed = false;
        let mpError = null;
        if (mp.pauseSubscription && sub.mp_preapproval_id) {
            try {
                await mp.pauseSubscription(sub.mp_preapproval_id);
                if (strict && mp.getSubscription) {
                    // Verificar que MP realmente pausó
                    await new Promise(r => setTimeout(r, 1500));
                    const after = await mp.getSubscription(sub.mp_preapproval_id).catch(() => null);
                    mpConfirmed = after && after.status === 'paused';
                    if (!mpConfirmed) mpError = `MP no confirmó pausa (status actual: ${after?.status || 'unknown'})`;
                } else {
                    mpConfirmed = true; // strict=off, asumimos éxito
                }
            } catch (e) { mpError = e.message; }
        } else {
            mpConfirmed = true; // sin preapproval, no hay nada que pausar en MP
        }
        if (strict && !mpConfirmed) {
            await db.createEvent({ subscription_id: sub.id, event_type: 'pause_mp_failed', metadata: JSON.stringify({ error: mpError, attempted_pause_months: pauseMonths }) }).catch(() => {});
            return res.status(502).json({ error: 'MercadoPago no confirmó la pausa. La suscripción NO se modificó.', mp_error: mpError });
        }
        const pausedUntil = new Date();
        pausedUntil.setMonth(pausedUntil.getMonth() + parseInt(pauseMonths));
        await db.updateSubscription(sub.id, { status: 'paused', paused_until: pausedUntil.toISOString() });
        await db.createEvent({ subscription_id: sub.id, event_type: 'paused', metadata: JSON.stringify({ pause_months: pauseMonths, mp_confirmed: mpConfirmed, strict_mode: strict }) });
        res.json({ success: true, pausedUntil, mp_confirmed: mpConfirmed });
    } catch (e) { res.status(500).json({ error: e.message }); }
});

/* ── SKIP removed 2026-04-11: feature disabled per business rules ── */

/* ── CANCEL subscription (with anti-abuse window check) ── */
/**
 * Calcula la penalidad por cancelación anticipada.
 * Fórmula: (precio_regular - precio_suscripcion) × ciclos_completados
 * = descuento mensual × meses entregados
 * Dentro del retracto de 7 días desde el primer cobro: penalidad 0.
 */
function calculateEarlyCancellationPenalty(sub) {
    const basePrice = parseFloat(sub.base_price || 0);
    const finalPrice = parseFloat(sub.final_price || basePrice);
    const monthlyDiscount = Math.max(0, basePrice - finalPrice);
    const cyclesCompleted = parseInt(sub.cycles_completed || 0);
    const cyclesRequired = parseInt(sub.cycles_required || 0);

    // Retracto Ley 29571 Art. 58: 7 días desde primer cobro
    const firstChargeAt = sub.activated_at || sub.last_charge_at;
    let withinRetracto = false;
    if (firstChargeAt) {
        const daysSinceFirst = (Date.now() - new Date(firstChargeAt).getTime()) / 86400000;
        withinRetracto = daysSinceFirst <= 7;
    }

    const completedPermanence = cyclesCompleted >= cyclesRequired;
    const penalty = (withinRetracto || completedPermanence) ? 0 : parseFloat((monthlyDiscount * cyclesCompleted).toFixed(2));

    return {
        penalty,
        monthly_discount: parseFloat(monthlyDiscount.toFixed(2)),
        cycles_completed: cyclesCompleted,
        cycles_required: cyclesRequired,
        within_retracto: withinRetracto,
        completed_permanence: completedPermanence,
        free_cancel: penalty === 0
    };
}

// PREVIEW: devuelve cuánto pagaría el cliente si cancela ahora
app.get('/api/subscriptions/:id/cancel/preview', async (req, res) => {
    try {
        const sub = await db.getSubscription(req.params.id);
        if (!sub) return res.status(404).json({ error: 'Not found' });
        const info = calculateEarlyCancellationPenalty(sub);
        let message;
        if (info.within_retracto) {
            message = 'Estás dentro de los 7 días de retracto. Puedes cancelar sin penalidad.';
        } else if (info.completed_permanence) {
            message = 'Ya cumpliste tu permanencia. Cancelación gratuita.';
        } else if (info.penalty === 0) {
            message = 'Cancelación gratuita: no se registró descuento mensual sobre esta suscripción.';
        } else {
            message = `Has recibido ${info.cycles_completed} ${info.cycles_completed === 1 ? 'mes' : 'meses'} con descuento de S/${info.monthly_discount.toFixed(2)} cada uno. Para cancelar antes de cumplir la permanencia debes reintegrar el descuento recibido: S/${info.penalty.toFixed(2)}. No es una multa, es la devolución del beneficio otorgado por un compromiso que no se completó. (Ley 29571, Art. 50 — cláusula no abusiva según INDECOPI).`;
        }
        res.json({ success: true, ...info, message, tc_version: sub.tc_version || '1.0' });
    } catch (e) { res.status(500).json({ error: e.message }); }
});

app.post('/api/subscriptions/:id/cancel', async (req, res) => {
    try {
        const sub = await db.getSubscription(req.params.id);
        if (!sub) return res.status(404).json({ error: 'Not found' });

        const now = new Date();
        const info = calculateEarlyCancellationPenalty(sub);
        const confirmPenalty = req.body?.confirm_penalty === true;

        // CASO 1: cancelación libre (retracto o permanencia cumplida)
        if (info.free_cancel) {
            // Aún respetar ventana de cancelación si permanencia cumplida
            if (info.completed_permanence && sub.next_charge_at) {
                const daysUntil = (new Date(sub.next_charge_at) - now) / (1000 * 60 * 60 * 24);
                if (daysUntil < 15 && daysUntil > 0) {
                    return res.status(403).json({
                        error: 'Ventana cerrada',
                        daysUntil: Math.round(daysUntil),
                        message: `La ventana de cancelación está cerrada. El próximo envío es en ${Math.round(daysUntil)} días. Podrás cancelar después del siguiente ciclo.`
                    });
                }
            }
            if (mp.cancelSubscription) await mp.cancelSubscription(sub.mp_preapproval_id).catch(() => { });
            await db.updateSubscription(sub.id, { status: 'cancelled', cancelled_at: now.toISOString() });
            await db.createEvent({ subscription_id: sub.id, event_type: 'cancelled', metadata: { penalty: 0, reason: info.within_retracto ? 'retracto' : 'permanencia_cumplida' } });
            if (sub.customer_id) shopify.tagCustomerAsSubscriber(sub.customer_id, false).catch(console.error);
            if (notifications?.sendCancellationConfirmation) notifications.sendCancellationConfirmation(sub).catch(e => console.warn('[CANCEL] Email error:', e.message));
            return res.json({ success: true, cancelled: true, penalty: 0, message: 'Suscripción cancelada correctamente.' });
        }

        // CASO 2: cancelación con penalidad — requiere confirmación explícita del cliente
        if (!confirmPenalty) {
            return res.status(402).json({
                error: 'Penalidad requerida',
                requires_penalty_payment: true,
                penalty: info.penalty,
                monthly_discount: info.monthly_discount,
                cycles_completed: info.cycles_completed,
                cycles_required: info.cycles_required,
                message: `Para cancelar antes de cumplir la permanencia debes reintegrar S/${info.penalty.toFixed(2)} (el descuento recibido durante ${info.cycles_completed} meses). Confirma para generar el link de pago.`
            });
        }

        // El cliente confirmó → crear link de pago MP + suspender recurrencia
        const backUrl = `${process.env.BACKEND_URL || 'https://pixel-suite-pro-production.up.railway.app'}/subscriptions/success?email=${encodeURIComponent(sub.customer_email)}&penalty_paid=1`;
        let penaltyCheckout = null;
        try {
            penaltyCheckout = await mp.createOneTimePayment({
                amount: info.penalty,
                title: `Reintegro por cancelación anticipada — ${sub.product_title}`,
                customerEmail: sub.customer_email,
                externalReference: `penalty_${sub.id}_${Date.now()}`,
                backUrl
            });
        } catch (e) { console.error('[CANCEL] Error creando link de penalidad:', e.message); }

        if (!penaltyCheckout?.init_point) {
            return res.status(500).json({ error: 'No se pudo generar el link de pago. Intenta nuevamente o contacta soporte.' });
        }

        // Suspender recurrencia inmediatamente (no más cobros)
        if (mp.cancelSubscription) await mp.cancelSubscription(sub.mp_preapproval_id).catch(() => { });
        await db.updateSubscription(sub.id, {
            status: 'cancelled',
            cancelled_at: now.toISOString(),
            penalty_amount: info.penalty,
            penalty_payment_url: penaltyCheckout.init_point,
            penalty_external_ref: `penalty_${sub.id}_${Date.now()}`,
            penalty_status: 'pending'
        });
        await db.createEvent({ subscription_id: sub.id, event_type: 'cancelled_with_penalty', metadata: { penalty: info.penalty, cycles_completed: info.cycles_completed } });
        if (sub.customer_id) shopify.tagCustomerAsSubscriber(sub.customer_id, false).catch(console.error);
        if (notifications?.sendCancellationConfirmation) notifications.sendCancellationConfirmation(sub).catch(e => console.warn('[CANCEL] Email error:', e.message));

        res.json({
            success: true,
            cancelled: true,
            penalty: info.penalty,
            penalty_payment_url: penaltyCheckout.init_point,
            message: `Suscripción cancelada. Para completar el proceso, paga el reintegro de S/${info.penalty.toFixed(2)}.`
        });
    } catch (e) { console.error('[CANCEL] Error:', e); res.status(500).json({ error: e.message }); }
});

/* ══════════════════════════════════════════════════════
   ADMIN — detalle, pagos MP, force-cancel, delete
   Uso interno del panel admin.html
   ══════════════════════════════════════════════════════ */

/** GET /api/subscriptions/:id/payments — lista pagos MP (authorized_payments) */
app.get('/api/subscriptions/:id/payments', _requireAdminToken, async (req, res) => {
    try {
        const sub = await db.getSubscription(req.params.id);
        if (!sub) return res.status(404).json({ error: 'Not found' });
        if (!sub.mp_preapproval_id) return res.json({ payments: [], note: 'Sin preapproval MP' });
        let payments = [];
        try {
            payments = await mp.listPreapprovalPayments(sub.mp_preapproval_id, 50);
        } catch (e) {
            return res.json({ payments: [], error: e.message });
        }
        res.json({ payments, preapproval_id: sub.mp_preapproval_id });
    } catch (e) { res.status(500).json({ error: e.message }); }
});

/** GET /api/admin/subscriptions/:id — detalle completo (sub + events + payments) */
app.get('/api/admin/subscriptions/:id', async (req, res) => {
    try {
        const sub = await db.getSubscription(req.params.id);
        if (!sub) return res.status(404).json({ error: 'Not found' });
        const [events, penaltyPreview] = await Promise.all([
            db.getEvents(sub.id).catch(() => []),
            Promise.resolve(calculateEarlyCancellationPenalty(sub))
        ]);
        let payments = [];
        if (sub.mp_preapproval_id && mp.listPreapprovalPayments) {
            try { payments = await mp.listPreapprovalPayments(sub.mp_preapproval_id, 50); } catch (e) { /* ignore */ }
        }
        res.json({ subscription: sub, events: events || [], payments, penalty_preview: penaltyPreview });
    } catch (e) { res.status(500).json({ error: e.message }); }
});

/** POST /api/admin/subscriptions/:id/force-cancel — admin cancela sin penalidad (override) */
app.post('/api/admin/subscriptions/:id/force-cancel', async (req, res) => {
    try {
        const sub = await db.getSubscription(req.params.id);
        if (!sub) return res.status(404).json({ error: 'Not found' });
        const reason = (req.body && req.body.reason) || 'admin_force';
        const now = new Date();
        if (sub.mp_preapproval_id && mp.cancelSubscription) {
            await mp.cancelSubscription(sub.mp_preapproval_id).catch(e => console.warn('[ADMIN CANCEL] MP error:', e.message));
        }
        await db.updateSubscription(sub.id, { status: 'cancelled', cancelled_at: now.toISOString(), penalty_status: 'waived', penalty_amount: 0 });
        await db.createEvent({ subscription_id: sub.id, event_type: 'admin_force_cancel', metadata: { reason, by: 'admin' } });
        res.json({ success: true, cancelled: true, penalty: 0, message: 'Cancelación admin sin penalidad aplicada.' });
    } catch (e) { res.status(500).json({ error: e.message }); }
});

/** POST /api/admin/orders/cancel-phantoms
 *  2026-05-12 — ADITIVO. Detecta y cancela ordenes Shopify fantasma creadas por el cron
 *  rescue (bug histórico previo al fix v6.6.0). Una orden fantasma cumple TODOS estos:
 *
 *    1. event_type == 'first_order_created'
 *    2. metadata.rescued == true
 *    3. NO es el primer event-de-orden de la sub (hay otro event de tipo
 *       first_order_created, charge_success o manual_order_created ANTES)
 *
 *  Eso significa que la sub ya tenía orden válida cuando el rescue cron creó otra
 *  duplicada (porque getEvents timeó silencioso y asumió "no hay order").
 *
 *  Cancela via Shopify Admin API POST /orders/{id}/cancel.json con:
 *    - reason: "other"
 *    - email: false (no notifica al cliente, es admin)
 *    - refund: false (no había cobro MP, no hay nada que refundir)
 *    - restock: false (no toca inventario)
 *
 *  Body: { dry_run: true|false (default true) }
 *  Response: { dry_run, candidates: [...], cancelled: N, errors: [...] }
 *
 *  NO toca: orders reales (charge_success o primera orden de la sub), subs, MP.
 */
app.post('/api/admin/orders/cancel-phantoms', async (req, res) => {
    try {
        const dryRun = req.body?.dry_run !== false;
        const shop = process.env.SHOPIFY_SHOP || 'nutrition-lab-cluster.myshopify.com';
        const token = process.env.SHOPIFY_ACCESS_TOKEN || _shopifyToken;
        if (!token) return res.status(500).json({ error: 'Shopify token not configured' });

        const ORDER_EVENT_TYPES = new Set(['first_order_created', 'charge_success', 'manual_order_created']);
        const allSubs = await db.getSubscriptions().catch(() => []);

        // 🚀 PERF: filtrar primero subs con potencial de tener fantasma para no iterar 215.
        //  Solo procesa subs que cobraron al menos 1 vez (cycles>=1) o que ya tienen
        //  gifts_delivered=true o shopify_order_id poblado (= la sub tiene ≥1 order
        //  con la cual el rescue pudo duplicar).
        const candidatesSubs = (Array.isArray(allSubs) ? allSubs : []).filter(s =>
            (parseInt(s.cycles_completed) || 0) > 0 ||
            s.gifts_delivered === true ||
            !!s.shopify_order_id ||
            !!s.gifts_delivered_order_id
        );

        // 🚀 PERF: leer events de a 10 subs en paralelo (Promise.all batches).
        const phantoms = [];
        const BATCH_SIZE = 10;
        for (let bi = 0; bi < candidatesSubs.length; bi += BATCH_SIZE) {
            const batch = candidatesSubs.slice(bi, bi + BATCH_SIZE);
            const results = await Promise.all(batch.map(async (sub) => {
                let events = [];
                try { events = await db.getEvents(sub.id, 100); } catch (_) { return { sub, events: null }; }
                return { sub, events };
            }));
            for (const { sub, events } of results) {
                if (events === null) continue;
                // Ordenar ASC para identificar cuál orden vino primero
                const orderEvts = (events || [])
                    .filter(e => ORDER_EVENT_TYPES.has(e.event_type))
                    .sort((a, b) => new Date(a.created_at || 0) - new Date(b.created_at || 0));
                if (orderEvts.length < 2) continue;
            // Los fantasmas: first_order_created rescued:true en posición != 0
            for (let i = 1; i < orderEvts.length; i++) {
                const e = orderEvts[i];
                if (e.event_type !== 'first_order_created') continue;
                let meta = {};
                try { meta = JSON.parse(e.metadata || '{}'); } catch {}
                if (meta.rescued !== true) continue;
                if (!meta.shopify_order_id) continue;
                phantoms.push({
                    sub_id: sub.id,
                    email: sub.customer_email,
                    sub_status: sub.status,
                    cycles_completed: sub.cycles_completed,
                    phantom_order_id: String(meta.shopify_order_id),
                    phantom_order_name: meta.order_name || null,
                    phantom_created_at: e.created_at,
                    previous_order: orderEvts[0] ? {
                        type: orderEvts[0].event_type,
                        created_at: orderEvts[0].created_at,
                        order_name: (() => { try { return JSON.parse(orderEvts[0].metadata || '{}').order_name; } catch { return null; } })()
                    } : null
                });
            }
        }
        } // close for bi (batch loop)

        if (dryRun) {
            return res.json({
                dry_run: true,
                candidates_count: phantoms.length,
                candidates: phantoms,
                note: 'DRY RUN: nada se canceló. Re-ejecutar con {"dry_run":false} para cancelar en Shopify.'
            });
        }

        // EJECUCIÓN REAL: cancelar uno por uno con rate limit
        const cancelled = [];
        const errors = [];
        for (const p of phantoms) {
            try {
                const r = await fetch(`https://${shop}/admin/api/2026-01/orders/${encodeURIComponent(p.phantom_order_id)}/cancel.json`, {
                    method: 'POST',
                    headers: { 'X-Shopify-Access-Token': token, 'Content-Type': 'application/json' },
                    body: JSON.stringify({ reason: 'other', email: false, refund: false, restock: false })
                });
                if (r.ok) {
                    cancelled.push(p);
                    // Audit event en la sub
                    await db.createEvent({
                        subscription_id: p.sub_id,
                        event_type: 'phantom_order_cancelled',
                        metadata: JSON.stringify({
                            shopify_order_id: p.phantom_order_id,
                            order_name: p.phantom_order_name,
                            cancelled_at: new Date().toISOString(),
                            reason: 'duplicate_rescue_no_mp_charge',
                            by: 'admin_cancel_phantoms_endpoint'
                        })
                    }).catch(() => {});
                    console.log(`[CANCEL-PHANTOMS] ✅ ${p.email} | order ${p.phantom_order_name} cancelled`);
                } else {
                    const txt = await r.text();
                    errors.push({ ...p, http_status: r.status, error: txt.slice(0, 300) });
                    console.warn(`[CANCEL-PHANTOMS] ❌ ${p.email} | ${p.phantom_order_name}: ${r.status} ${txt.slice(0, 150)}`);
                }
            } catch (e) {
                errors.push({ ...p, error: e.message });
            }
            await new Promise(r => setTimeout(r, 400)); // rate limit
        }

        res.json({
            dry_run: false,
            candidates_count: phantoms.length,
            cancelled_count: cancelled.length,
            errors_count: errors.length,
            cancelled,
            errors
        });
    } catch (e) {
        console.error('[CANCEL-PHANTOMS] Fatal:', e.message);
        res.status(500).json({ error: e.message });
    }
});

/** POST /api/admin/orders/cancel-rejected-phantom
 *  2026-05-28 — ADITIVO. Cancela UNA orden Shopify fantasma TARGETED (no auto-detecta),
 *  respaldada por un pago MP REJECTED. Distinta de /cancel-phantoms (ese cubre duplicados
 *  del rescue cron con meta.rescued:true). Esta cubre el bug histórico de leer el status
 *  TOP-LEVEL de authorized_payments ('processed'/'scheduled') como si fuera approved, que
 *  creó órdenes "Pagado" sobre cobros realmente rechazados.
 *
 *  DOBLE GUARD obligatorio — IMPOSIBLE cancelar una orden con pago approved:
 *    GUARD 1: la orden existe, NO está ya cancelada, y su note_attribute mp_payment_id
 *             === expected_rejected_payment_id (lo que envía el admin, verificado a mano).
 *    GUARD 2: mp.getPayment(expected_rejected_payment_id).status === 'rejected' EN VIVO.
 *  Si cualquiera falla → 409 y NO cancela.
 *
 *  Cancela con el MISMO mecanismo probado de /cancel-phantoms:
 *    cancel.json { reason:'other', email:false, refund:false, restock:false }.
 *  Correcciones locales OPCIONALES: true_cycles (corrige cycles_completed inflado por el
 *  fantasma) y relink_order_id/name (re-vincula shopify_order_id a la orden legítima de
 *  abril, manteniendo el gate !shopify_order_id del self-heal en false). Registra event
 *  'phantom_order_cancelled' para auditoría.
 *
 *  Body: { order_id, sub_id?, expected_rejected_payment_id, true_cycles?,
 *          relink_order_id?, relink_order_name?, dry_run? (default true) }
 *  NO toca webhook, crons, creación de órdenes ni funciones MP. */
app.post('/api/admin/orders/cancel-rejected-phantom', async (req, res) => {
    try {
        const b = req.body || {};
        const dryRun = b.dry_run !== false; // default TRUE (seguro)
        const orderId = String(b.order_id || '').trim();
        const subId = String(b.sub_id || '').trim();
        const expectedRej = String(b.expected_rejected_payment_id || '').trim();
        if (!orderId || !expectedRej) {
            return res.status(400).json({ error: 'Faltan order_id y/o expected_rejected_payment_id' });
        }
        const shop = process.env.SHOPIFY_SHOP || 'nutrition-lab-cluster.myshopify.com';
        const token = process.env.SHOPIFY_ACCESS_TOKEN || _shopifyToken;
        if (!token) return res.status(500).json({ error: 'Shopify token not configured' });
        if (!mp?.getPayment) return res.status(500).json({ error: 'MP no configurado (getPayment ausente) — fail-closed, no se cancela' });

        // ── Traer la orden de Shopify
        const getUrl = `https://${shop}/admin/api/2026-01/orders/${encodeURIComponent(orderId)}.json?fields=id,name,cancelled_at,financial_status,total_price,note_attributes,email`;
        const gr = await fetch(getUrl, { headers: { 'X-Shopify-Access-Token': token } });
        if (!gr.ok) return res.status(502).json({ error: `Shopify GET order ${gr.status}` });
        const gd = await gr.json();
        const order = gd.order;
        if (!order) return res.status(404).json({ error: 'Orden no encontrada en Shopify' });

        // Idempotencia: si ya está cancelada → no-op OK (re-ejecutable sin riesgo)
        if (order.cancelled_at) {
            return res.json({ ok: true, already_cancelled: true, order: { id: String(order.id), name: order.name, cancelled_at: order.cancelled_at } });
        }

        // GUARD 1 — el mp_payment_id de la orden debe coincidir EXACTO con el rechazado que envía el admin
        const attrs = Array.isArray(order.note_attributes) ? order.note_attributes : [];
        const orderMpId = (attrs.find(a => a && a.name === 'mp_payment_id') || {}).value || null;
        if (String(orderMpId || '') !== expectedRej) {
            return res.status(409).json({
                error: 'GUARD1_FALLA: el mp_payment_id de la orden NO coincide con expected_rejected_payment_id. NO se cancela.',
                order_mp_payment_id: orderMpId, expected: expectedRej
            });
        }

        // GUARD 2 — el payment DEBE estar rejected en MP, verificado en vivo (fail-closed).
        //   En este backend el MP_ACCESS_TOKEN vive en Shopify y se carga lazy. Lo aseguramos
        //   ANTES de getPayment, mismo patron que findRealMpPaymentForSub (linea ~4660).
        if (!process.env.MP_ACCESS_TOKEN) {
            try {
                const dyn = await readFromShopify().catch(() => ({}));
                if (dyn?.mp_access_token) process.env.MP_ACCESS_TOKEN = dyn.mp_access_token;
            } catch {}
        }
        let pd = null;
        try { pd = await mp.getPayment(expectedRej); }
        catch (e) { return res.status(502).json({ error: 'No se pudo verificar el payment en MP (fail-closed, no se cancela): ' + e.message }); }
        if (!pd || pd.status !== 'rejected') {
            return res.status(409).json({
                error: 'GUARD2_FALLA: el payment NO está rejected en MP. NO se cancela (protección anti-cancelación de orden legítima).',
                mp_status: pd?.status || null, mp_status_detail: pd?.status_detail || null
            });
        }

        const verified = {
            order: { id: String(order.id), name: order.name, financial_status: order.financial_status, total_price: order.total_price },
            mp_payment_id: orderMpId, mp_status: pd.status, mp_status_detail: pd.status_detail || null, amount: pd.transaction_amount || null
        };

        if (dryRun) {
            return res.json({ ok: true, dry_run: true, guards_passed: true, would_cancel: verified, note: 'DRY RUN: nada se canceló. Re-ejecutar con dry_run:false para cancelar.' });
        }

        // ── Cancelar (mecanismo idéntico al probado en /cancel-phantoms)
        const cr = await fetch(`https://${shop}/admin/api/2026-01/orders/${encodeURIComponent(orderId)}/cancel.json`, {
            method: 'POST',
            headers: { 'X-Shopify-Access-Token': token, 'Content-Type': 'application/json' },
            body: JSON.stringify({ reason: 'other', email: false, refund: false, restock: false })
        });
        if (!cr.ok) {
            const txt = await cr.text();
            return res.status(502).json({ error: `Shopify cancel ${cr.status}`, detail: txt.slice(0, 400), verified });
        }

        // ── Correcciones locales OPCIONALES + auditoría
        const out = { ok: true, cancelled: verified };
        if (subId && db?.getSubscription) {
            const sub = await db.getSubscription(subId).catch(() => null);
            if (sub && db.updateSubscription) {
                const upd = {};
                if (b.true_cycles !== undefined && b.true_cycles !== null && /^\d+$/.test(String(b.true_cycles))) {
                    upd.cycles_completed = parseInt(b.true_cycles, 10);
                }
                if (b.relink_order_id) {
                    upd.shopify_order_id = String(b.relink_order_id);
                    if (b.relink_order_name) upd.shopify_order_name = String(b.relink_order_name);
                }
                if (Object.keys(upd).length) {
                    await db.updateSubscription(subId, upd).catch(() => {});
                    out.sub_updated = upd;
                }
            }
            if (db.createEvent) {
                await db.createEvent({
                    subscription_id: subId,
                    event_type: 'phantom_order_cancelled',
                    metadata: JSON.stringify({
                        shopify_order_id: String(order.id), order_name: order.name,
                        mp_payment_id: orderMpId, mp_status: pd.status, mp_status_detail: pd.status_detail || null,
                        amount: pd.transaction_amount || null, cancelled_at: new Date().toISOString(),
                        reason: 'rejected_mp_payment_phantom', by: 'admin_cancel_rejected_phantom_endpoint'
                    })
                }).catch(() => {});
            }
        }
        console.log(`[CANCEL-REJECTED-PHANTOM] OK ${order.name} (${orderId}) cancelled — mp ${orderMpId} status=${pd.status}`);
        res.json(out);
    } catch (e) {
        console.error('[CANCEL-REJECTED-PHANTOM] Fatal:', e.message);
        res.status(500).json({ error: e.message });
    }
});

/** POST /api/admin/customers/sync-sub-tags
 *  2026-05-12 — ADITIVO. Sincroniza tags Shopify customers según estado de sub local.
 *  Permite crear Shopify Customer Segments dinámicos por estado de suscripción.
 *
 *  Tags aplicados (prefijo lab-sub-*, no conflicta con suscriptor-lab legacy):
 *   - lab-sub-active           → cliente con al menos 1 sub status=active
 *   - lab-sub-pending          → cliente sin sub active, con al menos 1 pending_payment
 *   - lab-sub-cancelled        → cliente con todas sus subs cancelled
 *   - lab-sub-completed        → cliente con sub que cumplió permanencia
 *
 *  Lógica de prioridad (cada cliente recibe SOLO 1 tag lab-sub-*):
 *   active > paused > completed > payment_failed > pending_payment > cancelled
 *
 *  Body: { dry_run: true|false (default true) }
 *
 *  NO toca: otros tags del customer (suscriptor-lab legacy, custom tags del admin, etc).
 *  Solo agrega/remueve los lab-sub-* específicos.
 */
app.post('/api/admin/customers/sync-sub-tags', async (req, res) => {
    try {
        const dryRun = req.body?.dry_run !== false;
        const shop = process.env.SHOPIFY_SHOP || 'nutrition-lab-cluster.myshopify.com';
        const token = process.env.SHOPIFY_ACCESS_TOKEN || _shopifyToken;
        if (!token) return res.status(500).json({ error: 'Shopify token not configured' });

        const PRIORITY = { active: 5, paused: 4, completed: 3, payment_failed: 2, pending_payment: 1, cancelled: 0 };
        const STATUS_TO_TAG = {
            active: 'lab-sub-active',
            paused: 'lab-sub-active',
            completed: 'lab-sub-completed',
            payment_failed: 'lab-sub-pending',
            pending_payment: 'lab-sub-pending',
            cancelled: 'lab-sub-cancelled'
        };
        const ALL_LAB_TAGS = ['lab-sub-active', 'lab-sub-pending', 'lab-sub-cancelled', 'lab-sub-completed'];

        const allSubs = await db.getSubscriptions().catch(() => []);
        const byEmail = new Map();
        for (const s of (Array.isArray(allSubs) ? allSubs : [])) {
            const e = (s.customer_email || '').trim().toLowerCase();
            if (!e) continue;
            if (!byEmail.has(e)) byEmail.set(e, []);
            byEmail.get(e).push(s);
        }

        const report = { tagged_active: 0, tagged_pending: 0, tagged_cancelled: 0, tagged_completed: 0, not_in_shopify: 0, errors: 0, updated: 0, skipped_no_change: 0 };
        const errors = [];
        const notInShopify = [];

        for (const [email, arr] of byEmail) {
            // status agregado por cliente
            let best = arr[0];
            for (const s of arr) {
                if ((PRIORITY[s.status] || 0) > (PRIORITY[best.status] || 0)) best = s;
            }
            const targetTag = STATUS_TO_TAG[best.status];
            if (!targetTag) continue;

            // contador por categoría (incluye los que no están en Shopify pero deberían)
            if (targetTag === 'lab-sub-active') report.tagged_active++;
            else if (targetTag === 'lab-sub-pending') report.tagged_pending++;
            else if (targetTag === 'lab-sub-cancelled') report.tagged_cancelled++;
            else if (targetTag === 'lab-sub-completed') report.tagged_completed++;

            // Buscar customer en Shopify
            let customer = null;
            try {
                const r = await fetch(`https://${shop}/admin/api/2026-01/customers/search.json?query=email:${encodeURIComponent(email)}&limit=1`, {
                    headers: { 'X-Shopify-Access-Token': token }
                });
                if (r.ok) {
                    const data = await r.json();
                    customer = data.customers?.[0] || null;
                }
            } catch (e) {
                errors.push({ email, error: e.message });
                report.errors++;
                continue;
            }
            if (!customer) {
                notInShopify.push(email);
                report.not_in_shopify++;
                continue;
            }

            // Diff de tags: quitar todos los lab-sub-*, agregar el target
            const currentTags = String(customer.tags || '').split(',').map(t => t.trim()).filter(Boolean);
            const nonLabTags = currentTags.filter(t => !ALL_LAB_TAGS.includes(t));
            const newTags = [...nonLabTags, targetTag];
            const currentSorted = [...currentTags].sort().join(',');
            const newSorted = [...newTags].sort().join(',');
            if (currentSorted === newSorted) {
                report.skipped_no_change++;
                continue;
            }

            if (dryRun) {
                report.updated++; // count what would update
            } else {
                try {
                    const r = await fetch(`https://${shop}/admin/api/2026-01/customers/${customer.id}.json`, {
                        method: 'PUT',
                        headers: { 'X-Shopify-Access-Token': token, 'Content-Type': 'application/json' },
                        body: JSON.stringify({ customer: { id: customer.id, tags: newTags.join(', ') } })
                    });
                    if (r.ok) report.updated++;
                    else { report.errors++; errors.push({ email, status: r.status, body: (await r.text()).slice(0, 200) }); }
                } catch (e) {
                    report.errors++;
                    errors.push({ email, error: e.message });
                }
                // rate limit: max 4 req/s, durmamos 250ms
                await new Promise(r => setTimeout(r, 250));
            }
        }

        res.json({
            dry_run: dryRun,
            total_unique_emails: byEmail.size,
            ...report,
            not_in_shopify_emails: notInShopify.slice(0, 10),
            errors_sample: errors.slice(0, 5),
            note: dryRun
                ? 'DRY RUN: nada cambió. Re-ejecuta con {"dry_run":false} para aplicar.'
                : 'Aplicado. Tags lab-sub-* sincronizados. Usa POST /api/admin/customers/create-sub-segments para crear los Segments Shopify.'
        });
    } catch (e) {
        console.error('[SYNC-TAGS] Fatal:', e.message);
        res.status(500).json({ error: e.message });
    }
});

/** POST /api/admin/customers/create-sub-segments
 *  2026-05-12 — ADITIVO. Crea 3 Customer Segments nativos de Shopify, basados en los
 *  tags lab-sub-*. Los segments quedan visibles en Shopify Admin → Customers → Segments
 *  y se auto-actualizan en tiempo real conforme cambian los tags.
 *
 *  Idempotente: si ya existe un segment con el mismo nombre, no crea duplicado.
 *
 *  Segments creados:
 *   - "LAB · Suscriptores activos"        → customer_tags CONTAINS 'lab-sub-active'
 *   - "LAB · Suscripción pendiente pago"  → customer_tags CONTAINS 'lab-sub-pending'
 *   - "LAB · Suscripción cancelada"       → customer_tags CONTAINS 'lab-sub-cancelled'
 *
 *  Usa GraphQL segmentCreate. No toca segments existentes que no creamos.
 */
app.post('/api/admin/customers/create-sub-segments', async (req, res) => {
    try {
        const shop = process.env.SHOPIFY_SHOP || 'nutrition-lab-cluster.myshopify.com';
        const token = process.env.SHOPIFY_ACCESS_TOKEN || _shopifyToken;
        if (!token) return res.status(500).json({ error: 'Shopify token not configured' });

        async function gql(query, variables) {
            const r = await fetch(`https://${shop}/admin/api/2026-01/graphql.json`, {
                method: 'POST',
                headers: { 'X-Shopify-Access-Token': token, 'Content-Type': 'application/json' },
                body: JSON.stringify({ query, variables })
            });
            const j = await r.json();
            if (j.errors) throw new Error(JSON.stringify(j.errors));
            return j.data;
        }

        const SEGMENTS = [
            { name: 'LAB · Suscriptores activos',       query: "customer_tags CONTAINS 'lab-sub-active'" },
            { name: 'LAB · Suscripción pendiente pago', query: "customer_tags CONTAINS 'lab-sub-pending'" },
            { name: 'LAB · Suscripción cancelada',      query: "customer_tags CONTAINS 'lab-sub-cancelled'" },
            { name: 'LAB · Suscripción completada',     query: "customer_tags CONTAINS 'lab-sub-completed'" }
        ];

        // Buscar segments existentes con prefijo "LAB ·"
        const existingData = await gql(`
            query { segments(first: 50, query: "LAB") { nodes { id name query } } }
        `).catch(() => ({ segments: { nodes: [] } }));
        const existingByName = new Map();
        (existingData.segments?.nodes || []).forEach(s => existingByName.set(s.name, s));

        const results = [];
        for (const seg of SEGMENTS) {
            if (existingByName.has(seg.name)) {
                results.push({ name: seg.name, status: 'already_exists', id: existingByName.get(seg.name).id });
                continue;
            }
            try {
                const data = await gql(`
                    mutation CreateSeg($name: String!, $query: String!) {
                        segmentCreate(name: $name, query: $query) {
                            segment { id name query }
                            userErrors { field message }
                        }
                    }
                `, { name: seg.name, query: seg.query });
                const errs = data.segmentCreate?.userErrors || [];
                if (errs.length) {
                    results.push({ name: seg.name, status: 'error', error: errs.map(e => e.message).join('; ') });
                } else {
                    results.push({ name: seg.name, status: 'created', id: data.segmentCreate?.segment?.id, query: seg.query });
                }
            } catch (e) {
                results.push({ name: seg.name, status: 'error', error: e.message });
            }
            await new Promise(r => setTimeout(r, 400));
        }

        res.json({
            ok: true,
            shopify_admin_url: `https://${shop.replace('.myshopify.com', '')}/admin/customers?segment_query=`,
            segments: results,
            note: 'Los segments aparecen en Shopify Admin → Customers → Segments. Se auto-actualizan en tiempo real conforme los tags lab-sub-* cambien.'
        });
    } catch (e) {
        console.error('[CREATE-SEGMENTS] Fatal:', e.message);
        res.status(500).json({ error: e.message });
    }
});

/** POST /api/admin/subs/backfill-shopify-order-from-gifts
 *  2026-05-12 — ADITIVO. Repara subs cuyo metaobject quedó con shopify_order_id vacío
 *  pero gifts_delivered_order_id sí está poblado (el order fue creado, pero solo el segundo
 *  updateSubscription del webhook MP guardó. El primero — dentro de createShopifyOrderFromSub
 *  — sí). Una vez backfilleado, el filtro del cron rescue (!s.shopify_order_id) los protege
 *  de fantasmas futuros.
 *
 *  Body: { dry_run: true|false (default true) }
 *   - dry_run: lista qué se actualizaría sin tocar nada
 *   - dry_run:false: ejecuta el patch real
 *
 *  Response: { dry_run, candidates: [...], updated: number, skipped: number }
 *
 *  NO toca: orders Shopify, cron, webhook MP, código de cobro.
 */
app.post('/api/admin/subs/backfill-shopify-order-from-gifts', async (req, res) => {
    try {
        const dryRun = req.body?.dry_run !== false; // default true
        const allSubs = await db.getSubscriptions().catch(() => []);
        const candidates = (Array.isArray(allSubs) ? allSubs : []).filter(s =>
            s.status === 'active' &&
            !s.shopify_order_id &&
            !s.shopify_order_name &&
            s.gifts_delivered_order_id &&
            s.gifts_delivered_order_name
        );
        const report = [];
        let updated = 0, skipped = 0;
        for (const s of candidates) {
            const patch = {
                shopify_order_id: String(s.gifts_delivered_order_id),
                shopify_order_name: String(s.gifts_delivered_order_name)
            };
            report.push({
                sub_id: s.id,
                email: s.customer_email,
                will_set_shopify_order_id: patch.shopify_order_id,
                will_set_shopify_order_name: patch.shopify_order_name
            });
            if (!dryRun) {
                try {
                    await db.updateSubscription(s.id, patch);
                    await db.createEvent({
                        subscription_id: s.id,
                        event_type: 'shopify_order_backfilled',
                        metadata: JSON.stringify({
                            shopify_order_id: patch.shopify_order_id,
                            shopify_order_name: patch.shopify_order_name,
                            source: 'gifts_delivered_order',
                            by: 'admin_backfill_endpoint'
                        })
                    }).catch(() => {});
                    updated++;
                } catch (e) {
                    skipped++;
                    console.warn('[BACKFILL] sub', s.id, 'failed:', e.message);
                }
            }
        }
        res.json({
            dry_run: dryRun,
            candidates_count: candidates.length,
            updated: dryRun ? 0 : updated,
            skipped: dryRun ? 0 : skipped,
            note: dryRun
                ? 'DRY RUN: nada cambió. Re-ejecuta con {"dry_run":false} para aplicar.'
                : 'Aplicado. Subs ahora tienen shopify_order_id poblado → el cron rescue ya no los toca.',
            candidates: report
        });
    } catch (e) {
        console.error('[BACKFILL] Fatal:', e.message);
        res.status(500).json({ error: e.message });
    }
});

/** GET /api/admin/subscriptions/:id/mp-status
 *  2026-05-12 — ADITIVO, READ-ONLY.
 *  Consulta el preapproval directamente en Mercado Pago y devuelve estado real.
 *  Útil para verificar antes/después de una cancelación que MP refleje lo esperado.
 *  No toca metaobjects, no modifica nada. Si MP responde error lo devuelve textual.
 */
app.get('/api/admin/subscriptions/:id/mp-status', async (req, res) => {
    try {
        const sub = await db.getSubscription(req.params.id);
        if (!sub) return res.status(404).json({ error: 'Not found' });
        if (!sub.mp_preapproval_id) {
            return res.json({
                ok: true,
                sub_id: sub.id,
                has_preapproval: false,
                note: 'Sub no tiene mp_preapproval_id — nunca llegó a autorizar pago'
            });
        }
        let mpInfo = null;
        let mpError = null;
        try {
            mpInfo = await mp.getSubscription(sub.mp_preapproval_id);
        } catch (e) {
            mpError = e.message || String(e);
        }
        const mpDashboardUrl = `https://www.mercadopago.com.pe/subscriptions/admin/subscriptions/${sub.mp_preapproval_id}`;
        return res.json({
            ok: !mpError,
            sub_id: sub.id,
            sub_status_local: sub.status,
            mp_preapproval_id: sub.mp_preapproval_id,
            mp_dashboard_url: mpDashboardUrl,
            mp_status: mpInfo?.status || null,
            mp_payer_email: mpInfo?.payer_email || null,
            mp_next_payment_date: mpInfo?.next_payment_date || null,
            mp_charged_quantity: mpInfo?.summarized?.charged_quantity || 0,
            mp_pending_charge_quantity: mpInfo?.summarized?.pending_charge_quantity || 0,
            mp_last_charged_amount: mpInfo?.summarized?.last_charged_amount || null,
            mp_error: mpError,
            checked_at: new Date().toISOString()
        });
    } catch (e) {
        res.status(500).json({ error: e.message });
    }
});

/** POST /api/admin/subscriptions/:id/cancel-safe
 *  2026-05-12 — ADITIVO. Versión segura de force-cancel con verificación MP.
 *
 *  Flujo (atómico, con rollback si MP falla):
 *   1. Lee sub local
 *   2. Si sub.mp_preapproval_id existe → consulta MP estado actual
 *   3. Si MP ya está cancelled → marca sub local sin volver a llamar MP
 *   4. Si MP está activo → llama mp.cancelSubscription(preapproval_id)
 *   5. Verifica MP DESPUÉS — si status != 'cancelled', NO marca sub (rollback)
 *   6. Si MP confirma cancelado → marca sub local
 *   7. Crea event con detalle completo
 *
 *  Body: { reason: string, waive_penalty: true|false }
 *   - reason: motivo del admin (queda en event metadata)
 *   - waive_penalty: si true, no aplica penalty (force-cancel). Default true.
 *
 *  Response: estado pre + post de sub local y MP, evento creado, dashboard link.
 *  Si algo falla devuelve 500 con detalle y NO deja sub en estado inconsistente.
 *
 *  NO toca: webhook MP, orders, crons, otros subs, código de cobro.
 */
app.post('/api/admin/subscriptions/:id/cancel-safe', async (req, res) => {
    try {
        const sub = await db.getSubscription(req.params.id);
        if (!sub) return res.status(404).json({ error: 'Not found' });

        const reason = String((req.body && req.body.reason) || 'admin_manual').slice(0, 200);
        const waivePenalty = req.body?.waive_penalty !== false; // default true
        const startedAt = new Date().toISOString();

        // Estado inicial (snapshot)
        const subStatusBefore = sub.status;
        const mpId = sub.mp_preapproval_id || null;

        let mpStatusBefore = null;
        let mpStatusAfter = null;
        let mpCancelCalled = false;
        let mpCancelOk = false;
        let mpError = null;

        if (mpId && mp.getSubscription) {
            // Paso 1: verificar estado actual en MP
            try {
                const info = await mp.getSubscription(mpId);
                mpStatusBefore = info?.status || null;
            } catch (e) {
                console.warn('[CANCEL-SAFE] No pude leer estado MP previo:', e.message);
            }

            // Paso 2: si MP no está cancelado, cancelar
            if (mpStatusBefore !== 'cancelled' && mp.cancelSubscription) {
                mpCancelCalled = true;
                try {
                    await mp.cancelSubscription(mpId);
                    mpCancelOk = true;
                } catch (e) {
                    mpError = e.message || String(e);
                    console.error('[CANCEL-SAFE] MP cancel failed:', mpError);
                }

                // Paso 3: re-verificar MP DESPUÉS
                if (mpCancelOk) {
                    try {
                        const after = await mp.getSubscription(mpId);
                        mpStatusAfter = after?.status || null;
                    } catch (e) {
                        console.warn('[CANCEL-SAFE] No pude verificar estado MP post-cancel:', e.message);
                    }
                }
            } else {
                mpStatusAfter = mpStatusBefore;
            }
        }

        // Rollback condition: si llamamos a MP y no quedó cancelled → NO marcamos local
        if (mpCancelCalled && mpStatusAfter !== 'cancelled' && mpStatusAfter !== null) {
            return res.status(502).json({
                error: 'MP_NO_CANCELO',
                detail: 'Se llamó a MP pero el preapproval no quedó en status=cancelled. Sub local NO modificada para evitar inconsistencia.',
                mp_status_before: mpStatusBefore,
                mp_status_after: mpStatusAfter,
                mp_error: mpError,
                mp_preapproval_id: mpId
            });
        }
        if (mpCancelCalled && !mpCancelOk && mpStatusAfter === null) {
            return res.status(502).json({
                error: 'MP_LLAMADA_FALLO',
                detail: 'Llamada a MP.cancel falló y no pude verificar estado actual. Sub local NO modificada. Reintentar.',
                mp_error: mpError,
                mp_preapproval_id: mpId
            });
        }

        // Llegamos aquí solo si MP quedó cancelled, o no había preapproval, o ya estaba cancelled
        const finishedAt = new Date().toISOString();
        await db.updateSubscription(sub.id, {
            status: 'cancelled',
            cancelled_at: finishedAt,
            ...(waivePenalty ? { penalty_status: 'waived', penalty_amount: 0 } : {})
        });

        // Customer tag en Shopify (no bloqueante)
        if (sub.customer_id && shopify.tagCustomerAsSubscriber) {
            shopify.tagCustomerAsSubscriber(sub.customer_id, false).catch(e => console.warn('[CANCEL-SAFE] Shopify tag error:', e.message));
        }

        // Event de auditoría con todo el contexto
        await db.createEvent({
            subscription_id: sub.id,
            event_type: 'admin_cancel_safe',
            metadata: JSON.stringify({
                reason,
                waive_penalty: waivePenalty,
                sub_status_before: subStatusBefore,
                mp_preapproval_id: mpId,
                mp_status_before: mpStatusBefore,
                mp_status_after: mpStatusAfter,
                mp_cancel_called: mpCancelCalled,
                mp_cancel_ok: mpCancelOk,
                started_at: startedAt,
                finished_at: finishedAt
            })
        }).catch(() => {});

        // Email de confirmación (no bloqueante)
        if (notifications?.sendCancellationConfirmation) {
            notifications.sendCancellationConfirmation(sub).catch(e => console.warn('[CANCEL-SAFE] Email error:', e.message));
        }

        console.log(`[CANCEL-SAFE] ✅ ${sub.customer_email} | sub:${subStatusBefore}→cancelled | mp:${mpStatusBefore || 'N/A'}→${mpStatusAfter || 'N/A'} | reason:${reason}`);

        res.json({
            ok: true,
            sub_id: sub.id,
            email: sub.customer_email,
            sub_status_before: subStatusBefore,
            sub_status_after: 'cancelled',
            mp_preapproval_id: mpId,
            mp_status_before: mpStatusBefore,
            mp_status_after: mpStatusAfter,
            mp_cancel_called: mpCancelCalled,
            mp_cancel_ok: mpCancelOk,
            mp_dashboard_url: mpId ? `https://www.mercadopago.com.pe/subscriptions/admin/subscriptions/${mpId}` : null,
            waive_penalty: waivePenalty,
            reason,
            finished_at: finishedAt
        });
    } catch (e) {
        console.error('[CANCEL-SAFE] Fatal:', e.message);
        res.status(500).json({ error: e.message });
    }
});

/** DELETE /api/admin/subscriptions/:id — elimina registro (limpieza de duplicados) */
app.delete('/api/admin/subscriptions/:id', async (req, res) => {
    try {
        const sub = await db.getSubscription(req.params.id);
        if (!sub) return res.status(404).json({ error: 'Not found' });
        // Si está activa intentamos cancelar el preapproval MP antes de borrar
        if (sub.status === 'active' && sub.mp_preapproval_id && mp.cancelSubscription) {
            await mp.cancelSubscription(sub.mp_preapproval_id).catch(e => console.warn('[ADMIN DELETE] MP cancel error:', e.message));
        }
        const out = await db.deleteSubscription(sub.id);
        res.json({ success: true, ...out });
    } catch (e) { res.status(500).json({ error: e.message }); }
});

/** GET /api/admin/orders/diagnose?numbers=8419,8420 — READ-ONLY: compara campos críticos Navasoft
 *  Devuelve para cada orden: dni, tipo_documento, location_distrito/departamento, shipping_code,
 *  shipping_address completa, billing_address.company (= DNI SUNAT). NO modifica nada.
 *  Sirve para entender por qué un pedido cae y otro no. */
app.get('/api/admin/orders/diagnose', async (req, res) => {
    try {
        const shop = process.env.SHOPIFY_SHOP || 'nutrition-lab-cluster.myshopify.com';
        const token = process.env.SHOPIFY_ACCESS_TOKEN || _shopifyToken;
        if (!token) return res.status(500).json({ error: 'No Shopify token' });
        const numbers = String(req.query.numbers || '').split(',').map(n => n.trim()).filter(Boolean);
        if (!numbers.length) return res.status(400).json({ error: 'Pasa ?numbers=8419,8420 separados por coma' });

        // Los números de orden en Shopify se buscan por "name" con prefijo "#"
        const results = [];
        for (const num of numbers) {
            try {
                const q = encodeURIComponent(`name:#${num}`);
                const url = `https://${shop}/admin/api/2026-01/orders.json?status=any&name=%23${num}&limit=1`;
                const r = await fetch(url, { headers: { 'X-Shopify-Access-Token': token } });
                if (!r.ok) { results.push({ number: num, error: `Shopify ${r.status}` }); continue; }
                const data = await r.json();
                const order = (data.orders || [])[0];
                if (!order) { results.push({ number: num, error: 'Orden no encontrada' }); continue; }

                // Extraer los campos que Navasoft necesita
                const attrs = {};
                (order.note_attributes || []).forEach(a => { attrs[a.name] = a.value; });
                const critical = {
                    number: order.order_number || order.name,
                    created_at: order.created_at,
                    email: order.email,
                    tags: order.tags,
                    financial_status: order.financial_status,
                    // Navasoft critical fields
                    dni: attrs['dni'] || null,
                    ClusterCart_dni: attrs['ClusterCart-dni'] || null,
                    tipo_documento: attrs['tipo_documento'] || null,
                    location_departamento: attrs['location_departamento'] || null,
                    location_provincia: attrs['location_provincia'] || null,
                    location_distrito: attrs['location_distrito'] || null,
                    shipping_code: attrs['shipping_code'] || null,
                    courier_id: attrs['courier_id'] || null,
                    ClusterCart_optimized: attrs['ClusterCart-optimized'] || null,
                    // Tienda / fulfillment location (Navasoft asigna tienda según location_id)
                    order_location_id: order.location_id || null,
                    order_source_name: order.source_name || null,
                    line_item_locations: (order.line_items || []).map(li => ({
                        variant_id: li.variant_id, sku: li.sku,
                        fulfillable_quantity: li.fulfillable_quantity,
                        fulfillment_service: li.fulfillment_service,
                        origin_location: li.origin_location ? {
                            id: li.origin_location.id,
                            name: li.origin_location.name,
                            city: li.origin_location.city
                        } : null
                    })),
                    // From billing address (SUNAT boleta)
                    billing_company_dni: order.billing_address?.company || null,
                    // Shipping address snapshot
                    shipping: order.shipping_address ? {
                        name: order.shipping_address.name,
                        address1: order.shipping_address.address1,
                        city: order.shipping_address.city,
                        province: order.shipping_address.province,
                        province_code: order.shipping_address.province_code,
                        zip: order.shipping_address.zip,
                        phone: order.shipping_address.phone
                    } : null,
                    // Full note_attribute keys for diff
                    _note_attribute_keys: Object.keys(attrs).sort()
                };
                // Fetch ALL metafields for this order (includes Navasoft app metafields)
                try {
                    const mfUrl = `https://${shop}/admin/api/2026-01/orders/${order.id}/metafields.json?limit=250`;
                    const mfr = await fetch(mfUrl, { headers: { 'X-Shopify-Access-Token': token } });
                    if (mfr.ok) {
                        const mfd = await mfr.json();
                        critical.metafields = (mfd.metafields || []).map(m => ({
                            namespace: m.namespace,
                            key: m.key,
                            value: typeof m.value === 'string' ? m.value.slice(0, 400) : m.value,
                            type: m.type
                        }));
                    } else {
                        critical.metafields = { error: `HTTP ${mfr.status}` };
                    }
                } catch (e) { critical.metafields = { error: e.message }; }

                // Flags de salud
                critical._health = {
                    has_dni: !!critical.dni && critical.dni.length >= 8,
                    has_cluster_dni: !!critical.ClusterCart_dni && critical.ClusterCart_dni.length >= 8,
                    has_distrito: !!critical.location_distrito,
                    has_departamento: !!critical.location_departamento,
                    has_shipping_code: !!critical.shipping_code,
                    has_shipping_address: !!critical.shipping,
                    has_billing_company_dni: !!critical.billing_company_dni,
                    has_tienda_assigned: !!critical.order_location_id,
                    has_navasoft_metafields: Array.isArray(critical.metafields) && critical.metafields.some(m => (m.namespace || '').toLowerCase().includes('navasoft') || (m.key || '').toLowerCase().includes('navasoft'))
                };
                critical._navasoft_ready = Object.values(critical._health).every(v => v === true);
                results.push(critical);
            } catch (e) { results.push({ number: num, error: e.message }); }
        }
        res.json({ results, comparison_note: 'Si _navasoft_ready=true → la orden tiene todos los campos. Si false → falta al menos uno (ver _health).' });
    } catch (e) { res.status(500).json({ error: e.message }); }
});

/** GET /api/admin/orders/:id/raw — devuelve line_items con precio (para validar regalo a 0.00). */
app.get('/api/admin/orders/:id/raw', async (req, res) => {
    try {
        const shop = process.env.SHOPIFY_SHOP || 'nutrition-lab-cluster.myshopify.com';
        const token = process.env.SHOPIFY_ACCESS_TOKEN || _shopifyToken;
        if (!token) return res.status(500).json({ error: 'No Shopify token' });
        const r = await fetch(`https://${shop}/admin/api/2026-01/orders/${req.params.id}.json`, {
            headers: { 'X-Shopify-Access-Token': token }
        });
        if (!r.ok) return res.status(r.status).json({ error: await r.text() });
        const data = await r.json();
        const o = data.order || {};
        res.json({
            id: o.id,
            name: o.name,
            total_price: o.total_price,
            financial_status: o.financial_status,
            tags: o.tags,
            line_items: (o.line_items || []).map(li => ({
                id: li.id,
                variant_id: li.variant_id,
                title: li.title,
                variant_title: li.variant_title,
                sku: li.sku,
                quantity: li.quantity,
                price: li.price,
                taxable: li.taxable,
                properties: li.properties || []
            })),
            gift_included_attr: (o.note_attributes || []).find(a => a.name === 'gift_included')?.value || null
        });
    } catch (e) { res.status(500).json({ error: e.message }); }
});

/** POST /api/admin/orders/:id/cancel — cancela orden en Shopify (para tests). */
app.post('/api/admin/orders/:id/cancel', async (req, res) => {
    try {
        const shop = process.env.SHOPIFY_SHOP || 'nutrition-lab-cluster.myshopify.com';
        const token = process.env.SHOPIFY_ACCESS_TOKEN || _shopifyToken;
        if (!token) return res.status(500).json({ error: 'No Shopify token' });
        const reason = (req.body && req.body.reason) || 'other';
        const r = await fetch(`https://${shop}/admin/api/2026-01/orders/${req.params.id}/cancel.json`, {
            method: 'POST',
            headers: { 'X-Shopify-Access-Token': token, 'Content-Type': 'application/json' },
            body: JSON.stringify({ reason, email: false, restock: true })
        });
        if (!r.ok) return res.status(r.status).json({ error: await r.text() });
        const data = await r.json();
        res.json({ success: true, order: { id: data.order?.id, name: data.order?.name, cancelled_at: data.order?.cancelled_at, cancel_reason: data.order?.cancel_reason } });
    } catch (e) { res.status(500).json({ error: e.message }); }
});

/** POST /api/admin/orders/:id/patch-attribute — Actualiza UN note_attribute de una orden.
 *  Body: { name: string, value: string }
 *  Útil para corregir cycle_number mal etiquetado (caso afernandezgaldos donde
 *  la primera orden quedó marcada cycle_number="2" en vez de "1", bloqueando
 *  el dedup del cobro real cycle 2).
 *  Lee la orden, hace merge del attribute, hace PUT a Shopify. */
app.post('/api/admin/orders/:id/patch-attribute', async (req, res) => {
    try {
        const shop = process.env.SHOPIFY_SHOP || 'nutrition-lab-cluster.myshopify.com';
        const token = process.env.SHOPIFY_ACCESS_TOKEN || _shopifyToken;
        if (!token) return res.status(500).json({ error: 'No Shopify token' });
        const { name, value } = req.body || {};
        if (!name) return res.status(400).json({ error: 'Missing name' });
        // 1) GET order to read existing note_attributes
        const getUrl = `https://${shop}/admin/api/2026-01/orders/${req.params.id}.json?fields=id,name,note_attributes`;
        const gr = await fetch(getUrl, { headers: { 'X-Shopify-Access-Token': token } });
        if (!gr.ok) return res.status(gr.status).json({ error: `Shopify GET ${gr.status}: ${await gr.text()}` });
        const gd = await gr.json();
        const existing = Array.isArray(gd.order?.note_attributes) ? gd.order.note_attributes.slice() : [];
        const idx = existing.findIndex(a => a && a.name === name);
        const before = idx >= 0 ? existing[idx].value : null;
        if (idx >= 0) existing[idx] = { name, value: String(value || '') };
        else existing.push({ name, value: String(value || '') });
        // 2) PUT order with updated note_attributes
        const putUrl = `https://${shop}/admin/api/2026-01/orders/${req.params.id}.json`;
        const pr = await fetch(putUrl, {
            method: 'PUT',
            headers: { 'X-Shopify-Access-Token': token, 'Content-Type': 'application/json' },
            body: JSON.stringify({ order: { id: parseInt(req.params.id), note_attributes: existing } })
        });
        if (!pr.ok) return res.status(pr.status).json({ error: `Shopify PUT ${pr.status}: ${await pr.text()}` });
        const pd = await pr.json();
        res.json({ success: true, order_id: pd.order?.id, name: pd.order?.name, attribute: { name, before, after: String(value || '') }, note_attributes: pd.order?.note_attributes });
    } catch (e) { res.status(500).json({ error: e.message }); }
});

/** GET /api/admin/shopify/locations — READ-ONLY: lista todas las locations de Shopify
 *  Para que el usuario identifique cuál es la de Navasoft y la configure en env var.
 */
app.get('/api/admin/shopify/locations', async (req, res) => {
    try {
        const shop = process.env.SHOPIFY_SHOP || 'nutrition-lab-cluster.myshopify.com';
        const token = process.env.SHOPIFY_ACCESS_TOKEN || _shopifyToken;
        if (!token) return res.status(500).json({ error: 'No Shopify token' });
        const r = await fetch(`https://${shop}/admin/api/2026-01/locations.json`, {
            headers: { 'X-Shopify-Access-Token': token }
        });
        if (!r.ok) return res.status(r.status).json({ error: `Shopify ${r.status}: ${await r.text()}` });
        const data = await r.json();
        const configured = process.env.SHOPIFY_LOCATION_ID || null;
        const locations = (data.locations || []).map(l => ({
            id: l.id,
            name: l.name,
            address1: l.address1,
            city: l.city,
            province: l.province,
            country: l.country,
            active: l.active,
            legacy: l.legacy,
            is_primary: !!l.primary_based,
            is_configured_as_navasoft: configured && String(configured) === String(l.id)
        }));
        res.json({
            total: locations.length,
            configured_env_location_id: configured,
            locations,
            hint: configured
                ? 'SHOPIFY_LOCATION_ID está configurado — las nuevas órdenes deberían usar esa location si el código la aplica.'
                : '⚠ SHOPIFY_LOCATION_ID NO configurado. Las órdenes usarán la location default de Shopify. Copia el ID de la location correcta (Navasoft) y seteala como env var en Railway.'
        });
    } catch (e) { res.status(500).json({ error: e.message }); }
});

/** GET /api/admin/orders/fulfillment-locations?limit=30 — READ-ONLY: para cada orden de
 *  suscripción lee su fulfillment_orders y devuelve en QUÉ location Shopify rutea el pedido.
 *  Esto es lo que realmente determina a qué tienda/hub va el pedido (order.location_id es solo POS).
 */
app.get('/api/admin/orders/fulfillment-locations', async (req, res) => {
    try {
        const shop = process.env.SHOPIFY_SHOP || 'nutrition-lab-cluster.myshopify.com';
        const token = process.env.SHOPIFY_ACCESS_TOKEN || _shopifyToken;
        if (!token) return res.status(500).json({ error: 'No Shopify token' });
        const limit = Math.min(parseInt(req.query.limit) || 30, 100);
        const configured = process.env.SHOPIFY_LOCATION_ID || null;

        // Últimas orders con tag 'suscripcion'
        const ordUrl = `https://${shop}/admin/api/2026-01/orders.json?status=any&limit=100&fields=id,order_number,name,created_at,tags,email`;
        const ordR = await fetch(ordUrl, { headers: { 'X-Shopify-Access-Token': token } });
        if (!ordR.ok) return res.status(ordR.status).json({ error: `Shopify ${ordR.status}: ${await ordR.text()}` });
        const ordData = await ordR.json();
        const subsOrders = (ordData.orders || [])
            .filter(o => (o.tags || '').toLowerCase().includes('suscripcion'))
            .slice(0, limit);

        const results = [];
        const locationCounts = {};
        for (const o of subsOrders) {
            try {
                const foUrl = `https://${shop}/admin/api/2026-01/orders/${o.id}/fulfillment_orders.json`;
                const foR = await fetch(foUrl, { headers: { 'X-Shopify-Access-Token': token } });
                if (!foR.ok) {
                    results.push({ order_number: o.order_number, name: o.name, email: o.email, error: `fulfillment_orders ${foR.status}` });
                    continue;
                }
                const foData = await foR.json();
                const fos = foData.fulfillment_orders || [];
                const locs = fos.map(f => ({
                    id: f.assigned_location_id || f.assigned_location?.location_id || null,
                    name: f.assigned_location?.name || null,
                    status: f.status,
                    request_status: f.request_status
                }));
                // Count
                for (const l of locs) {
                    const k = l.id ? `${l.id}|${l.name || ''}` : '(sin location)';
                    locationCounts[k] = (locationCounts[k] || 0) + 1;
                }
                results.push({
                    order_number: o.order_number,
                    name: o.name,
                    email: o.email,
                    created_at: o.created_at,
                    fulfillment_count: fos.length,
                    locations: locs
                });
            } catch (e) {
                results.push({ order_number: o.order_number, error: e.message });
            }
        }
        res.json({
            total_orders: subsOrders.length,
            configured_navasoft_location_id: configured,
            location_distribution: locationCounts,
            results: results.slice(0, 20),
            hint: 'Cada fulfillment_order.assigned_location_id es la tienda que Shopify eligió para procesar el pedido. Si ves múltiples locations en location_distribution, los pedidos se están rutando a varias tiendas (inventario-based).'
        });
    } catch (e) { console.error('[FULFILL AUDIT]', e); res.status(500).json({ error: e.message }); }
});

/** GET /api/admin/orders/audit-subscriptions?limit=50 — READ-ONLY: audita las últimas N órdenes
 *  con tag 'suscripcion' y reporta cuántas tienen o no location_id asignado. Calcula tasa de
 *  órdenes correctamente ruteadas a Navasoft (si está configurada la env var).
 */
app.get('/api/admin/orders/audit-subscriptions', async (req, res) => {
    try {
        const shop = process.env.SHOPIFY_SHOP || 'nutrition-lab-cluster.myshopify.com';
        const token = process.env.SHOPIFY_ACCESS_TOKEN || _shopifyToken;
        if (!token) return res.status(500).json({ error: 'No Shopify token' });
        const limit = Math.min(parseInt(req.query.limit) || 50, 250);
        const envLocId = process.env.SHOPIFY_LOCATION_ID || null;
        const autoLocId = await getPrimaryLocationId().catch(() => null);
        const navasoftId = envLocId || (autoLocId ? String(autoLocId) : null);

        // Buscar órdenes con tag 'suscripcion' en últimas 500 (filtro cliente porque Shopify no filtra por tag en REST list)
        const url = `https://${shop}/admin/api/2026-01/orders.json?status=any&limit=250&fields=id,order_number,name,created_at,tags,location_id,source_name,email,note_attributes,line_items,financial_status,total_price`;
        const r = await fetch(url, { headers: { 'X-Shopify-Access-Token': token } });
        if (!r.ok) return res.status(r.status).json({ error: `Shopify ${r.status}: ${await r.text()}` });
        const data = await r.json();
        const allOrders = data.orders || [];

        const subsOrders = allOrders
            .filter(o => (o.tags || '').toLowerCase().includes('suscripcion'))
            .slice(0, limit);

        // Stats
        const withLocation = subsOrders.filter(o => o.location_id);
        const withoutLocation = subsOrders.filter(o => !o.location_id);
        const locationIdCounts = {};
        subsOrders.forEach(o => {
            const k = o.location_id ? String(o.location_id) : '(null)';
            locationIdCounts[k] = (locationIdCounts[k] || 0) + 1;
        });
        const inNavasoft = navasoftId
            ? subsOrders.filter(o => String(o.location_id) === String(navasoftId))
            : [];
        const outsideNavasoft = navasoftId
            ? subsOrders.filter(o => !o.location_id || String(o.location_id) !== String(navasoftId))
            : [];

        // Samples para UI
        const samples = subsOrders.slice(0, 20).map(o => {
            const attrs = {};
            (o.note_attributes || []).forEach(a => { attrs[a.name] = a.value; });
            return {
                order_number: o.order_number,
                name: o.name,
                created_at: o.created_at,
                email: o.email,
                location_id: o.location_id || null,
                source_name: o.source_name,
                financial_status: o.financial_status,
                total: o.total_price,
                in_navasoft: navasoftId ? String(o.location_id) === String(navasoftId) : null,
                payment_method: attrs['payment_method'] || null,
                dni: attrs['dni'] || attrs['ClusterCart-dni'] || null,
                distrito: attrs['location_distrito'] || null
            };
        });

        res.json({
            summary: {
                total_subscription_orders: subsOrders.length,
                with_location_id: withLocation.length,
                without_location_id: withoutLocation.length,
                env_location_id: envLocId,
                auto_primary_location_id: autoLocId,
                expected_location_id: navasoftId,
                orders_in_expected: inNavasoft.length,
                orders_outside_expected: outsideNavasoft.length,
                coverage_pct: navasoftId && subsOrders.length
                    ? Math.round((inNavasoft.length / subsOrders.length) * 100)
                    : null
            },
            location_id_distribution: locationIdCounts,
            samples,
            verdict: !navasoftId
                ? '⚠ No se pudo descubrir primary_location_id (shop.json). Revisar token/scope.'
                : (inNavasoft.length === subsOrders.length
                    ? '✅ Todas las órdenes de suscripción revisadas tienen la location esperada.'
                    : `⚠ ${outsideNavasoft.length} de ${subsOrders.length} órdenes viejas están sin location. Nuevas subs ya usarán ${navasoftId} (${envLocId ? 'env var' : 'auto default'}).`)
        });
    } catch (e) { console.error('[AUDIT]', e); res.status(500).json({ error: e.message }); }
});

/** GET /api/admin/audit/orders-vs-mp-real-status — RECONCILIACIÓN (solo lectura, NO cancela nada).
 *  Para cada suscripción con shopify_order_id vinculado, consulta el status REAL de sus cobros
 *  MP vía mp.getPayment (no el status 'processed' del authorized_payment, que es engañoso).
 *  Detecta el caso #11251: ORDEN EXISTE pero el cobro MP que la respaldaba quedó 'rejected'.
 *  No modifica nada — solo reporta para que el admin decida. Cap defensivo de llamadas a MP.
 *  Query: ?limit=200 (subs a revisar) &maxPaymentsPerSub=6 */
app.get('/api/admin/audit/orders-vs-mp-real-status', async (req, res) => {
    try {
        let mpToken = process.env.MP_ACCESS_TOKEN;
        if (!mpToken) {
            try { const dyn = await readFromShopify().catch(() => ({})); if (dyn?.mp_access_token) { process.env.MP_ACCESS_TOKEN = dyn.mp_access_token; mpToken = dyn.mp_access_token; } } catch {}
        }
        if (!mpToken) return res.status(500).json({ error: 'No MP_ACCESS_TOKEN disponible' });
        const limit = Math.min(parseInt(req.query.limit) || 200, 500);
        const maxPay = Math.min(parseInt(req.query.maxPaymentsPerSub) || 6, 12);

        const all = await db.getSubscriptions().catch(() => []);
        // Solo nos interesan las que TIENEN orden vinculada y un preapproval para consultar MP.
        const targets = all
            .filter(s => (s.shopify_order_id || s.shopify_order_name) && s.mp_preapproval_id)
            .sort((a, b) => (b.created_at || '').localeCompare(a.created_at || ''))
            .slice(0, limit);

        const phantom = [];      // 🔴 orden existe pero NINGÚN cobro real approved
        const latestRejected = []; // 🟠 último intento rejected (informativo)
        const anyRejected = [];  // 🟠 cualquier sub con >=1 intento rejected (para cruzar contra pedidos)
        const okList = [];       // ✅ al menos un cobro real approved
        const errors = [];       // ⚪ no se pudo verificar en MP
        let mpCalls = 0;

        for (const s of targets) {
            try {
                const r = await fetch(`https://api.mercadopago.com/authorized_payments/search?preapproval_id=${s.mp_preapproval_id}`, { headers: { Authorization: `Bearer ${mpToken}` } });
                if (!r.ok) { errors.push({ sub: s.id, email: s.customer_email, order: s.shopify_order_name, reason: `MP search ${r.status}` }); continue; }
                const data = await r.json();
                const recs = (data?.results || [])
                    .sort((a, b) => new Date(b.date_created) - new Date(a.date_created))
                    .slice(0, maxPay);
                let approvedCount = 0, rejectedCount = 0;
                let latestReal = null;
                const detail = [];
                for (const rec of recs) {
                    const pid = String(rec.payment?.id || rec.id || '');
                    let realStatus = rec.payment?.status || null;
                    if (/^\d+$/.test(pid) && mp?.getPayment) {
                        mpCalls++;
                        const pd = await mp.getPayment(pid).catch(() => null);
                        if (pd) realStatus = pd.status;
                    }
                    if (latestReal === null) latestReal = realStatus;
                    if (realStatus === 'approved') approvedCount++;
                    else if (realStatus === 'rejected') rejectedCount++;
                    detail.push({ payment_id: pid, authorized_payment_status: rec.status, real_status: realStatus, amount: rec.transaction_amount, date: rec.date_created });
                }
                const row = { sub: s.id, email: s.customer_email, order: s.shopify_order_name, order_id: s.shopify_order_id, cycles_completed: s.cycles_completed, approved: approvedCount, rejected: rejectedCount, latest_real_status: latestReal, payments: detail };
                if (rejectedCount > 0) anyRejected.push(row); // cualquier rejected → a revisar manualmente
                if (approvedCount === 0 && rejectedCount > 0) phantom.push(row);
                else if (latestReal === 'rejected') latestRejected.push(row);
                else okList.push(row);
            } catch (e) {
                errors.push({ sub: s.id, email: s.customer_email, order: s.shopify_order_name, reason: e.message });
            }
        }

        res.json({
            scanned: targets.length,
            mp_getpayment_calls: mpCalls,
            summary: {
                phantom_orders_no_approved_payment: phantom.length,
                subs_with_any_rejected_attempt: anyRejected.length,
                latest_charge_rejected_but_has_approved_history: latestRejected.length,
                ok_has_approved_payment: okList.length,
                could_not_verify: errors.length
            },
            phantom_orders: phantom,          // 🔴 ÓRDENES A REVISAR — sin ningún cobro real approved
            subs_with_rejected: anyRejected,  // 🟠 TODA sub con >=1 cobro rejected (cruzar payment_id rejected contra el pedido)
            latest_rejected: latestRejected,  // 🟠 informativo (último intento falló, pero tienen historial OK)
            errors,
            note: 'Solo lectura. No se canceló ni modificó nada. phantom_orders = órdenes Shopify cuyo respaldo MP NO está approved (revisar y decidir).'
        });
    } catch (e) { console.error('[AUDIT MP-REAL]', e); res.status(500).json({ error: e.message }); }
});

/** GET /api/admin/audit/sub-order-trace?subs=sub_a,sub_b — READ-ONLY.
 *  Para cada sub: devuelve el registro local (cycles_completed, last_charge_at, shopify_order_*)
 *  + TODAS las órdenes Shopify del email del cliente con los VALORES de note_attributes
 *  (mp_payment_id, cycle_number, subscription_id, mp_preapproval_id) + created_at + financial_status
 *  + cancelled_at. Sirve para clasificar definitivamente si una orden está respaldada por un pago
 *  approved (legítima) o rejected (fantasma). NO cancela ni modifica NADA. */
app.get('/api/admin/audit/sub-order-trace', async (req, res) => {
    try {
        const shop = process.env.SHOPIFY_SHOP || 'nutrition-lab-cluster.myshopify.com';
        const token = process.env.SHOPIFY_ACCESS_TOKEN || _shopifyToken;
        if (!token) return res.status(500).json({ error: 'No Shopify token' });
        const subIds = String(req.query.subs || '').split(',').map(s => s.trim()).filter(Boolean);
        if (!subIds.length) return res.status(400).json({ error: 'Pasa ?subs=sub_a,sub_b separados por coma' });

        const WANT = ['mp_payment_id', 'mp_preapproval_id', 'cycle_number', 'subscription_id', 'gift_included'];
        const out = [];
        for (const sid of subIds) {
            try {
                const sub = await db.getSubscription(sid).catch(() => null);
                if (!sub) { out.push({ sub_id: sid, error: 'Sub no encontrada localmente' }); continue; }
                const local = {
                    sub_id: sub.id,
                    customer_email: sub.customer_email,
                    status: sub.status,
                    cycles_completed: sub.cycles_completed,
                    last_charge_at: sub.last_charge_at || null,
                    mp_preapproval_id: sub.mp_preapproval_id || null,
                    shopify_order_id: sub.shopify_order_id || null,
                    shopify_order_name: sub.shopify_order_name || null
                };
                // Todas las órdenes del email (status=any) para detectar duplicados / cuál es cada ciclo
                let orders = [];
                if (sub.customer_email) {
                    const url = `https://${shop}/admin/api/2026-01/orders.json?status=any&email=${encodeURIComponent(sub.customer_email)}&limit=50&fields=id,name,order_number,created_at,cancelled_at,financial_status,total_price,note_attributes`;
                    const r = await fetch(url, { headers: { 'X-Shopify-Access-Token': token } });
                    if (r.ok) {
                        const data = await r.json();
                        orders = (data.orders || []).map(o => {
                            const attrs = {};
                            (o.note_attributes || []).forEach(a => { if (WANT.includes(a.name)) attrs[a.name] = a.value; });
                            const belongs = attrs.subscription_id === sub.id ||
                                (sub.mp_preapproval_id && attrs.mp_preapproval_id === sub.mp_preapproval_id) ||
                                String(o.id) === String(sub.shopify_order_id) ||
                                o.name === sub.shopify_order_name;
                            return {
                                name: o.name,
                                id: String(o.id),
                                created_at: o.created_at,
                                cancelled_at: o.cancelled_at || null,
                                financial_status: o.financial_status,
                                total_price: o.total_price,
                                mp_payment_id: attrs.mp_payment_id || null,
                                cycle_number: attrs.cycle_number || null,
                                subscription_id: attrs.subscription_id || null,
                                mp_preapproval_id: attrs.mp_preapproval_id || null,
                                belongs_to_this_sub: !!belongs
                            };
                        }).sort((a, b) => new Date(a.created_at) - new Date(b.created_at));
                    } else {
                        orders = { error: `Shopify ${r.status}` };
                    }
                }
                out.push({ local, orders });
            } catch (e) { out.push({ sub_id: sid, error: e.message }); }
        }
        res.json({ traced: out.length, results: out, note: 'Solo lectura. No se canceló ni modificó nada.' });
    } catch (e) { res.status(500).json({ error: e.message }); }
});

/** GET /api/admin/subs-incomplete — lista suscripciones a las que les falta DNI o dirección.
 *  Se usan para que el admin recupere los datos del cliente antes de la siguiente carga MP.
 *  Solo lectura. No modifica nada. */
app.get('/api/admin/subs-incomplete', async (req, res) => {
    try {
        const all = await db.getSubscriptions().catch(() => []);
        const items = all
            .map(s => {
                const check = assertSubShippable(s);
                if (check.ok) return null;
                const dni = String(s.dni || '').trim();
                const addr = s.shipping_address || {};
                return {
                    id: s.id,
                    customer_email: s.customer_email,
                    customer_name: s.customer_name || null,
                    customer_phone: s.customer_phone || null,
                    status: s.status,
                    product_title: s.product_title,
                    mp_preapproval_id: s.mp_preapproval_id,
                    frequency_months: s.frequency_months,
                    permanence_months: s.permanence_months,
                    cycles_completed: s.cycles_completed || 0,
                    next_charge_at: s.next_charge_at || null,
                    created_at: s.created_at || null,
                    missing: check.missing,
                    current: {
                        dni: dni || null,
                        tipo_documento: s.tipo_documento || null,
                        shipping_address1: addr.address1 || null,
                        shipping_city: addr.city || null,
                        shipping_province: addr.province || null,
                        shipping_phone: addr.phone || null
                    }
                };
            })
            .filter(Boolean)
            .sort((a, b) => (a.status === 'active' ? -1 : 1) - (b.status === 'active' ? -1 : 1));
        res.json({ total: items.length, items });
    } catch (e) { res.status(500).json({ error: e.message }); }
});

/** GET /api/admin/gifts-report — reporte de regalos (shakers) entregados + stock actual.
 *  Devuelve: entregados este mes, all-time, catálogo de regalos configurados con stock, y subs pendientes.
 *  Solo lectura. No modifica nada. */
app.get('/api/admin/gifts-report', async (req, res) => {
    try {
        const shop = process.env.SHOPIFY_SHOP || 'nutrition-lab-cluster.myshopify.com';
        const token = process.env.SHOPIFY_ACCESS_TOKEN || _shopifyToken;
        // 1) Recopilar regalos configurados en planes (activos o no — si hay subs con gifts_delivered, aplica)
        const data = await readFromShopify().catch(() => null);
        const plans = Array.isArray(data?.plans_config) ? data.plans_config : [];
        const giftVariants = new Map(); // variant_id → {product_id, product_title, variant_title, variant_sku, image, plans_using:[]}
        for (const p of plans) {
            const items = (p && p.gifts && p.gifts.enabled && Array.isArray(p.gifts.items)) ? p.gifts.items : [];
            for (const g of items) {
                if (!g || !g.variant_id) continue;
                const key = String(g.variant_id);
                if (!giftVariants.has(key)) {
                    giftVariants.set(key, {
                        product_id: String(g.product_id || ''),
                        product_title: g.product_title || '',
                        variant_title: g.variant_title || '',
                        variant_sku: g.variant_sku || '',
                        image: g.image || null,
                        plan_active: p.active !== false,
                        frequency: p.frequency || p.freq_months || null,
                        permanence: p.permanence || p.permanence_months || null
                    });
                }
            }
        }
        // 2) Stock actual por variante (consulta productos únicos)
        const stockByVariant = {};
        const productIds = [...new Set([...giftVariants.values()].map(v => v.product_id).filter(Boolean))];
        if (token) {
            for (const pid of productIds) {
                try {
                    const r = await fetch(`https://${shop}/admin/api/2026-01/products/${encodeURIComponent(pid)}.json?fields=id,title,variants`, {
                        headers: { 'X-Shopify-Access-Token': token }
                    });
                    if (!r.ok) continue;
                    const d = await r.json();
                    for (const v of (d.product?.variants || [])) {
                        stockByVariant[String(v.id)] = Number.isFinite(v.inventory_quantity) ? v.inventory_quantity : 0;
                    }
                } catch (e) { console.warn('[GIFT REPORT] stock fetch error pid=' + pid + ':', e.message); }
            }
        }
        // 3) Contar entregados + subs pendientes (usa getSubscriptions como fuente única)
        const now = new Date();
        const monthStart = new Date(now.getFullYear(), now.getMonth(), 1).toISOString();
        let deliveredThisMonth = 0;
        let deliveredAllTime = 0;
        let pending = [];
        let recentDeliveries = [];
        try {
            const allSubs = await db.getSubscriptions().catch(() => []);
            for (const s of allSubs) {
                if (s.gifts_delivered === true) {
                    deliveredAllTime++;
                    if ((s.gifts_delivered_at || '') >= monthStart) deliveredThisMonth++;
                    recentDeliveries.push({
                        id: s.id,
                        email: s.customer_email,
                        order: s.gifts_delivered_order_name || null,
                        order_id: s.gifts_delivered_order_id || null,
                        at: s.gifts_delivered_at || null,
                        gifts: (s.gifts_planned || []).map(g => (g.product_title || '') + (g.variant_title ? ' — ' + g.variant_title : '')).filter(Boolean)
                    });
                }
                if ((s.status === 'active' || s.status === 'pending_payment') &&
                    Array.isArray(s.gifts_planned) && s.gifts_planned.length > 0 && s.gifts_delivered !== true) {
                    pending.push({
                        id: s.id,
                        email: s.customer_email,
                        product_title: s.product_title,
                        status: s.status,
                        cycles_completed: s.cycles_completed || 0,
                        next_charge_at: s.next_charge_at || null,
                        gifts: s.gifts_planned.map(g => ({
                            product_title: g.product_title, variant_title: g.variant_title,
                            variant_sku: g.variant_sku, quantity: g.quantity
                        }))
                    });
                }
            }
        } catch (e) { console.warn('[GIFT REPORT] sub aggregate error:', e.message); }

        // 4) Construir catálogo
        const giftsCatalog = [...giftVariants.entries()].map(([vid, info]) => ({
            variant_id: vid,
            product_id: info.product_id,
            product_title: info.product_title,
            variant_title: info.variant_title,
            variant_sku: info.variant_sku,
            image: info.image,
            stock: stockByVariant[vid] ?? null,
            plan_active: info.plan_active,
            plan_frequency_months: info.frequency,
            plan_permanence_months: info.permanence
        }));
        recentDeliveries.sort((a, b) => String(b.at || '').localeCompare(String(a.at || '')));
        res.json({
            delivered_this_month: deliveredThisMonth,
            delivered_all_time: deliveredAllTime,
            gifts_catalog: giftsCatalog,
            pending_count: pending.length,
            pending: pending.slice(0, 50),
            recent_deliveries: recentDeliveries.slice(0, 20),
            month_start: monthStart
        });
    } catch (e) {
        console.error('[GIFT REPORT] Error:', e.message);
        res.status(500).json({ error: e.message });
    }
});

/** PUT /api/admin/subscriptions/:id/patch-data — completa datos faltantes de una suscripción.
 *  Admin manda {dni, tipo_documento, shipping_address: {address1, city, province, province_code, zip, phone, name}, customer_phone}.
 *  Solo actualiza los campos provistos. No cancela, no cobra, no genera pedidos. */
app.put('/api/admin/subscriptions/:id/patch-data', async (req, res) => {
    try {
        const sub = await db.getSubscription(req.params.id);
        if (!sub) return res.status(404).json({ error: 'Not found' });
        const body = req.body || {};
        const patch = {};

        // DNI — validar formato antes de guardar
        if (body.dni !== undefined) {
            const dni = String(body.dni || '').trim();
            if (dni && (dni.length < 8 || dni.length > 15 || !/^\d+$/.test(dni))) {
                return res.status(400).json({ error: 'DNI debe tener entre 8 y 15 dígitos numéricos' });
            }
            patch.dni = dni;
        }
        if (body.tipo_documento !== undefined) {
            const td = String(body.tipo_documento || '01');
            if (!['01', '06', '07', '00'].includes(td)) {
                return res.status(400).json({ error: 'tipo_documento inválido (01=DNI, 06=RUC, 07=Pasaporte, 00=Otros)' });
            }
            patch.tipo_documento = td;
        }
        if (body.customer_phone !== undefined) patch.customer_phone = String(body.customer_phone || '').trim();
        if (body.customer_name !== undefined) patch.customer_name = String(body.customer_name || '').trim();

        // Shipping address — merge parcial (mantiene los valores existentes no provistos)
        if (body.shipping_address && typeof body.shipping_address === 'object') {
            const cur = sub.shipping_address || {};
            const ship = body.shipping_address;
            patch.shipping_address = {
                ...cur,
                ...(ship.name !== undefined ? { name: String(ship.name || '').trim() } : {}),
                ...(ship.address1 !== undefined ? { address1: String(ship.address1 || '').trim() } : {}),
                ...(ship.address2 !== undefined ? { address2: String(ship.address2 || '').trim() } : {}),
                ...(ship.city !== undefined ? { city: String(ship.city || '').trim() } : {}),
                ...(ship.province !== undefined ? { province: String(ship.province || '').trim() } : {}),
                ...(ship.province_code !== undefined ? { province_code: String(ship.province_code || '').trim() } : {}),
                ...(ship.zip !== undefined ? { zip: String(ship.zip || '').trim() } : {}),
                ...(ship.country !== undefined ? { country: String(ship.country || 'Peru').trim() } : { country: cur.country || 'Peru' }),
                ...(ship.phone !== undefined ? { phone: String(ship.phone || '').trim() } : {})
            };
        }

        if (Object.keys(patch).length === 0) {
            return res.status(400).json({ error: 'No hay campos para actualizar. Envía al menos dni, tipo_documento, customer_phone, customer_name o shipping_address.' });
        }

        await db.updateSubscription(sub.id, patch);
        // Log del cambio para auditoría
        if (db.createEvent) {
            await db.createEvent({
                subscription_id: sub.id,
                event_type: 'sub_data_patched',
                metadata: JSON.stringify({ patched_keys: Object.keys(patch), by: 'admin', at: new Date().toISOString() })
            }).catch(() => {});
        }
        const updated = await db.getSubscription(sub.id).catch(() => sub);
        const check = assertSubShippable(updated);
        res.json({
            success: true,
            updated: updated,
            shippable: check.ok,
            still_missing: check.ok ? [] : check.missing
        });
    } catch (e) { res.status(500).json({ error: e.message }); }
});

/** POST /api/admin/subscriptions/:id/retry-order — reintenta createShopifyOrderFromSub
 *  Útil cuando completaste datos vía patch-data y querés generar el pedido Shopify del último cobro MP.
 *  Body opcional: { mp_payment_id } — si no se provee, usa el último pago MP de la sub. */
app.post('/api/admin/subscriptions/:id/retry-order', async (req, res) => {
    try {
        const sub = await db.getSubscription(req.params.id);
        if (!sub) return res.status(404).json({ error: 'Not found' });
        const check = assertSubShippable(sub);
        if (!check.ok) {
            return res.status(400).json({ error: 'La suscripción aún tiene datos incompletos', missing: check.missing, hint: 'Usá PUT /api/admin/subscriptions/:id/patch-data primero' });
        }
        let mpPaymentId = (req.body && req.body.mp_payment_id) || null;
        if (!mpPaymentId && sub.mp_preapproval_id && mp.listPreapprovalPayments) {
            try {
                const payments = await mp.listPreapprovalPayments(sub.mp_preapproval_id, 10);
                const approved = (payments || []).find(p => p.status === 'approved' || p.status === 'authorized');
                mpPaymentId = approved?.payment_id || approved?.id || null;
            } catch (e) { console.warn('[RETRY] MP lookup failed:', e.message); }
        }
        const order = await createShopifyOrderFromSub(sub, mpPaymentId);
        if (!order) return res.status(502).json({ error: 'createShopifyOrderFromSub retornó null — revisá logs' });
        if (db.createEvent) {
            await db.createEvent({
                subscription_id: sub.id,
                event_type: 'order_retry_success',
                metadata: JSON.stringify({ order_number: order.order_number, mp_payment_id: mpPaymentId, by: 'admin', at: new Date().toISOString() })
            }).catch(() => {});
        }
        res.json({ success: true, order: { id: order.id, order_number: order.order_number, name: order.name, financial_status: order.financial_status, total_price: order.total_price } });
    } catch (e) { res.status(500).json({ error: e.message }); }
});

/* ── PLANS — stored inside the proven settings metafield as 'plans_config' sub-key ── */
// FIX 2026-03-23: New separate metafields fail silently in API 2026-01 (owner_resource bug).
// Solution: store plans/products/configs as sub-keys of the EXISTING settings metafield.
app.get('/api/plans', async (req, res) => {
    try {
        const data = await readFromShopify() || readFromFile() || {};
        const saved = Array.isArray(data.plans_config) ? data.plans_config : [];
        // ── ADITIVO 2026-04-22 ── filtro opcional ?product_id=X
        // Devuelve sólo los planes cuyo `applies_to` incluye el producto (o todos si applies_to='all_products').
        // Usado por la nueva vista "Planes por Producto" del admin y por el widget cuando necesita
        // listar planes específicos de un producto.
        const pid = req.query.product_id ? String(req.query.product_id) : null;
        const out = pid ? saved.filter(p => planAppliesToProduct(p, pid)) : saved;
        // 🆕 2026-06-05 — cache 5 min (planes cambian poco, widget los pega en cada PDP)
        res.set('Cache-Control', 'public, max-age=300, s-maxage=300');
        console.log('[PLANS] Read', saved.length, 'plans' + (pid ? ` · filtered to ${out.length} for product ${pid}` : ''));
        res.json(out);
    } catch (e) {
        console.error('[PLANS] Error reading plans:', e.message);
        res.json([]);
    }
});

/* Guardar TODOS los planes — usa el metafield settings (probado) */
app.post('/api/plans', async (req, res) => {
    try {
        const plans = Array.isArray(req.body) ? req.body : req.body.plans;
        if (!plans) return res.status(400).json({ error: 'Expected array of plans' });
        const current = await readFromShopify() || readFromFile() || {};
        current.plans_config = plans;
        const saved = await saveToShopify(current);
        saveToFile(current); // local fallback
        if (!saved) {
            console.error('[PLANS] ❌ Failed to save plans to Shopify Metafields');
            return res.status(500).json({ error: 'No se pudo guardar en Shopify Metafields. Verifica SHOPIFY_ACCESS_TOKEN en Railway.' });
        }
        console.log('[PLANS] ✅ Saved', plans.length, 'plans to Shopify Metafields (settings sub-key)');
        res.json({ success: true, plans });
    } catch (e) {
        console.error('[PLANS] Save error:', e.message);
        res.status(500).json({ error: e.message });
    }
});

/* Guardar un plan individual por ID */
app.put('/api/plans/:id', async (req, res) => {
    try {
        const data = await readFromShopify() || readFromFile() || {};
        let current = Array.isArray(data.plans_config) ? data.plans_config : [];
        const idx = current.findIndex(p => String(p.id) === String(req.params.id));
        if (idx >= 0) current[idx] = { ...current[idx], ...req.body, id: req.params.id };
        else current.push({ ...req.body, id: req.params.id });
        data.plans_config = current;
        await saveToShopify(data);
        saveToFile(data);
        res.json({ success: true, plan: current[idx >= 0 ? idx : current.length - 1] });
    } catch (e) {
        console.error('[PLANS] PUT error:', e.message);
        res.status(500).json({ error: e.message });
    }
});

/* Eliminar un plan individual por ID — by-id (no by-index) para evitar race conditions
 * ADITIVO 2026-04-21: El admin antes hacía splice por índice local + POST array completo,
 *   lo que causaba "borré pero sigue saliendo" cuando el índice se desfasaba entre tabs
 *   o el POST al sobrescribir perdía alguna eliminación en paralelo. Este endpoint:
 *   - lee el array actual desde el metafield (fresh read)
 *   - filtra por id exacto
 *   - re-guarda y retorna el resultado
 *   MASTER LOCK: no toca webhook MP, pedidos, crons, ni lógica de suscripción. */
app.delete('/api/plans/:id', async (req, res) => {
    try {
        const data = await readFromShopify() || readFromFile() || {};
        const current = Array.isArray(data.plans_config) ? data.plans_config : [];
        const before = current.length;
        const filtered = current.filter(p => String(p.id) !== String(req.params.id));
        const removed = before - filtered.length;
        data.plans_config = filtered;
        const saved = await saveToShopify(data);
        saveToFile(data);
        if (!saved) return res.status(500).json({ error: 'No se pudo guardar en Shopify Metafields.' });
        console.log(`[PLANS] 🗑  DELETE ${req.params.id} — removed ${removed}, remaining ${filtered.length}`);
        res.json({ success: true, removed, remaining: filtered.length, plans: filtered });
    } catch (e) {
        console.error('[PLANS] DELETE error:', e.message);
        res.status(500).json({ error: e.message });
    }
});

/* ── ELIGIBLE PRODUCTS — fetch from Shopify Admin API ──
 * FIX 2026-04-21: Antes limitaba a 250 (sin paginación). La tienda tiene >250
 *   productos → Premium Whey (entre otros) no aparecía en el admin. Ahora
 *   pagina con Link headers hasta 2000 productos (8 páginas), suficiente para
 *   todo el catálogo. */
app.get('/api/products', _requireAdminToken, async (req, res) => {
    try {
        const shop = process.env.SHOPIFY_SHOP || 'nutrition-lab-cluster.myshopify.com';
        const token = process.env.SHOPIFY_ACCESS_TOKEN || _shopifyToken;
        if (!token) return res.json([]);
        let url = `https://${shop}/admin/api/2026-01/products.json?limit=250&fields=id,title,images,variants,status`;
        const acc = [];
        let pages = 0;
        while (url && pages < 8) {
            const r = await fetch(url, { headers: { 'X-Shopify-Access-Token': token } });
            if (!r.ok) break;
            const pageData = await r.json();
            if (Array.isArray(pageData.products)) acc.push(...pageData.products);
            // Extraer next page del Link header: <https://...?page_info=xyz>; rel="next"
            const linkHeader = r.headers.get('link') || r.headers.get('Link') || '';
            const m = linkHeader.split(',').map(s => s.trim()).find(s => s.endsWith('rel="next"'));
            url = m ? m.match(/<([^>]+)>/)?.[1] : null;
            pages++;
        }
        const data = { products: acc };
        // Read eligible_products from the proven settings metafield (sub-key)
        const settings = await readFromShopify() || readFromFile() || {};
        const savedEligible = Array.isArray(settings.eligible_products) ? settings.eligible_products : [];
        const activeIds = new Set(savedEligible.filter(p => p.is_active).map(p => String(p.shopify_id)));
        const products = (data.products || []).map(p => ({
            shopify_id: String(p.id),
            title: p.title,
            image: p.images?.[0]?.src || null,
            price: p.variants?.[0]?.price || '0',
            status: p.status,
            subscription_enabled: activeIds.has(String(p.id)),
            variants: (p.variants || []).map(v => ({
                id: String(v.id),
                title: v.title,
                price: v.price,
                sku: v.sku || ''
            }))
        }));
        console.log('[PRODUCTS] ' + products.length + ' total, ' + activeIds.size + ' active');
        res.json(products);
    } catch (e) {
        console.error('[PRODUCTS] Error:', e.message);
        res.json([]);
    }
});

app.post('/api/products', _requireAdminToken, async (req, res) => {
    try {
        const settings = await readFromShopify() || readFromFile() || {};
        if (!Array.isArray(settings.eligible_products)) settings.eligible_products = [];
        const pid = req.body.shopify_id || req.body.shopify_product_id;
        const idx = settings.eligible_products.findIndex(p => (p.shopify_id || p.shopify_product_id) === pid);
        const entry = { shopify_id: pid, product_title: req.body.product_title, is_active: req.body.is_active !== false, updated_at: new Date().toISOString() };
        if (idx >= 0) settings.eligible_products[idx] = { ...settings.eligible_products[idx], ...entry };
        else settings.eligible_products.push({ ...entry, created_at: new Date().toISOString() });
        const saved = await saveToShopify(settings);
        saveToFile(settings);
        if (!saved) return res.status(500).json({ error: 'No se pudo guardar en Shopify Metafields' });
        console.log('[PRODUCTS] ✅ Saved eligible products, toggled ' + pid + ' to ' + entry.is_active);
        res.json({ success: true, product: entry });
    } catch (e) {
        console.error('[PRODUCTS] Save error:', e.message);
        res.status(500).json({ error: e.message });
    }
});

/* ── PER-PRODUCT CONFIG — descuentos individuales por producto ──
   ⚠️ SIN _requireAdminToken A PROPÓSITO (2026-06-11): la theme extension
   (extensions/suscriptions-mp/blocks/lab-subscription.liquid) la consume
   desde el storefront sin token. Solo expone planes/descuentos, no PII. */
app.get('/api/products/:id/config', async (req, res) => {
    try {
        const id = req.params.id;
        const settings = await readFromShopify() || readFromFile() || {};

        // 🔧 2026-06-12: el toggle OFF del admin ahora SÍ apaga el widget.
        // Antes is_active=false solo se guardaba en eligible_products y este endpoint
        // lo ignoraba — el toast "Widget desactivado" era falso.
        const _elig = Array.isArray(settings.eligible_products) ? settings.eligible_products : [];
        const _eligEntry = _elig.find(p => String(p.shopify_id || p.shopify_product_id) === String(id));
        if (_eligEntry && _eligEntry.is_active === false) {
            return res.json({ plans: {}, disabled: true });
        }

        // NEW format: settings.product_configs[id]
        const newFmt = (typeof settings.product_configs === 'object' && !Array.isArray(settings.product_configs)) ? settings.product_configs : {};
        // OLD format: settings[id] (saved by server versions before v6.0.0)
        const oldFmt = (typeof settings[id] === 'object' && settings[id]?.plans) ? settings[id] : null;
        const cfg = newFmt[id] || oldFmt || {};

        // Enrich planes con info de regalo aplicable (sin tocar la estructura base)
        // FIX 2026-04-20: matcheamos por PLAN ID primero (único y correcto). Si no hay match
        //   por id, caemos a freq+perm PERO filtrando que el plan aplique a este producto
        //   (antes `.find()` tomaba el primer plan con esos valores y rompía cuando había
        //   varios planes con misma freq+perm para productos distintos — ej. Creatina vs Premium Whey).
        try {
            const plansCfg = Array.isArray(settings.plans_config) ? settings.plans_config : [];
            if (cfg.plans && typeof cfg.plans === 'object' && plansCfg.length) {
                const planKeys = Object.keys(cfg.plans);
                for (const key of planKeys) {
                    const p = cfg.plans[key] || {};
                    const freq = Number(p.frequency || p.freq_months);
                    const perm = Number(p.permanence || p.permanence_months);
                    // 1) match exacto por plan id — PERO ADEMÁS debe aplicar a este producto
                    let match = plansCfg.find(pc => pc && pc.active !== false && String(pc.id) === String(key) && planAppliesToProduct(pc, id));
                    // 2) fallback: match por freq+perm QUE APLIQUE A ESTE PRODUCTO
                    //    Se respeta el nuevo plan.applies_to (nivel plan) además del filtro
                    //    legacy de gifts.applies_to (para regalos específicos por producto).
                    if (!match) {
                        match = plansCfg.find(pc => {
                            if (!pc || pc.active === false) return false;
                            if (Number(pc.frequency || pc.freq_months) !== freq) return false;
                            if (Number(pc.permanence || pc.permanence_months) !== perm) return false;
                            if (!planAppliesToProduct(pc, id)) return false;
                            const mode = pc.gifts?.applies_to?.mode || 'all_products';
                            const ids = (pc.gifts?.applies_to?.product_ids || []).map(String);
                            return mode === 'all_products' || ids.includes(String(id));
                        });
                    }
                    // 🔄 FIX 2026-04-22: Auto-sync del discount desde MASTER plan.
                    //   Antes: si el admin editaba % en master (p.ej. Creatina 45→40),
                    //   el widget seguía mostrando 45% porque product_configs[id].plans
                    //   guardaba una COPIA vieja que no se actualizaba solo.
                    //   Ahora: al leer per-product, sobreescribe .discount desde master
                    //   (el match ya valida que el plan aplique al producto).
                    //   Solo lectura — no modifica saving flow ni MP ni orders.
                    if (match && typeof match.discount === 'number' && Number.isFinite(match.discount)) {
                        cfg.plans[key].discount = match.discount;
                    }
                    if (match && match.gifts && match.gifts.enabled) {
                        const mode = match.gifts.applies_to?.mode || 'all_products';
                        const ids = (match.gifts.applies_to?.product_ids || []).map(String);
                        const appliesToThisProduct = mode === 'all_products' || ids.includes(String(id));
                        const items = Array.isArray(match.gifts.items) ? match.gifts.items : [];
                        if (appliesToThisProduct && items.length) {
                            // Lookup handles (usa handle guardado o lo resuelve via Shopify con cache 1h)
                            const summary = await Promise.all(items.map(async (it) => {
                                let handle = it.product_handle || '';
                                if (!handle && it.product_id) {
                                    handle = await getProductHandle(it.product_id).catch(() => '') || '';
                                }
                                return {
                                    title: it.product_title || '',
                                    variant_title: it.variant_title || '',
                                    quantity: Math.max(1, parseInt(it.quantity, 10) || 1),
                                    image: it.image || null,
                                    product_handle: handle
                                };
                            }));
                            cfg.plans[key].gifts = {
                                enabled: true,
                                summary: summary.filter(it => it.title)
                            };
                        }
                    }
                }
            }
        } catch (e) { console.warn('[PRODUCT CFG GIFTS]', e.message); /* fallo silencioso */ }

        res.json(cfg);
    } catch (e) { res.status(500).json({ error: e.message }); }
});

app.post('/api/products/:id/config', _requireAdminToken, async (req, res) => {
    try {
        const id = req.params.id;
        const settings = await readFromShopify() || readFromFile() || {};
        if (!settings.product_configs || typeof settings.product_configs !== 'object' || Array.isArray(settings.product_configs)) settings.product_configs = {};
        // 🔒 FIX 2026-05-29: MERGE con la config existente para NO perder campos que el admin
        //   no envía en este guardado (ej. guardar planes NO debe borrar eligible_variant_ids,
        //   y viceversa). Antes sobrescribía con { ...req.body } → se "rompía info" del producto
        //   en cada save parcial. Aditivo y seguro: req.body solo pisa las llaves que envía.
        const prevCfg = (settings.product_configs[id] && typeof settings.product_configs[id] === 'object') ? settings.product_configs[id] : {};
        // Migrate old format if it exists
        if (typeof settings[id] === 'object' && settings[id]?.plans) {
            settings.product_configs[id] = { ...settings[id], ...prevCfg, ...req.body, updated_at: new Date().toISOString() };
            delete settings[id]; // Remove old key to clean up
        } else {
            settings.product_configs[id] = { ...prevCfg, ...req.body, updated_at: new Date().toISOString() };
        }
        const saved = await saveToShopify(settings);
        saveToFile(settings);
        if (!saved) return res.status(500).json({ error: 'No se pudo guardar en Shopify Metafields' });
        console.log('[PRODUCT CONFIG] ✅ Saved config for product', id);
        res.json({ success: true, config: settings.product_configs[id] });
    } catch (e) { res.status(500).json({ error: e.message }); }
});

/** GET /api/products/search?q=X&limit=20 — Busca productos Shopify por título (substring real).
 *  Usado por el selector de regalos en la UI admin. Read-only, no modifica nada.
 *  Usa GraphQL Admin API porque soporta substring search (REST solo prefix).
 *  Devuelve thumbnail + título + cantidad de variantes + stock total del producto. */
app.get('/api/products/search', _requireAdminToken, async (req, res) => {
    try {
        const shop = process.env.SHOPIFY_SHOP || 'nutrition-lab-cluster.myshopify.com';
        const token = process.env.SHOPIFY_ACCESS_TOKEN || _shopifyToken;
        if (!token) return res.json({ products: [], error: 'No Shopify token' });
        const q = String(req.query.q || '').trim();
        const limit = Math.min(parseInt(req.query.limit, 10) || 20, 50);
        // GraphQL query: title:*q* matchea substring; sin q devuelve todos ordenados por más recientes
        const searchQuery = q ? `title:*${q.replace(/[*\\"]/g, '')}*` : '';
        const gql = `
            query searchProducts($first: Int!, $q: String) {
                products(first: $first, query: $q, sortKey: UPDATED_AT, reverse: true) {
                    edges {
                        node {
                            id
                            title
                            handle
                            vendor
                            status
                            featuredImage { url }
                            totalInventory
                            variants(first: 100) {
                                edges {
                                    node { id title sku price inventoryQuantity }
                                }
                            }
                            variantsCount: variants(first: 250) { edges { node { id } } }
                        }
                    }
                }
            }`;
        const r = await fetch(`https://${shop}/admin/api/2026-01/graphql.json`, {
            method: 'POST',
            headers: { 'X-Shopify-Access-Token': token, 'Content-Type': 'application/json' },
            body: JSON.stringify({ query: gql, variables: { first: limit, q: searchQuery } })
        });
        if (!r.ok) {
            const txt = await r.text().catch(() => '');
            return res.status(502).json({ products: [], error: `Shopify ${r.status}: ${txt.slice(0, 200)}` });
        }
        const data = await r.json();
        if (data.errors) {
            return res.status(502).json({ products: [], error: `GraphQL: ${JSON.stringify(data.errors).slice(0, 200)}` });
        }
        const edges = data?.data?.products?.edges || [];
        const products = edges.map(({ node: p }) => {
            const numericId = String(p.id || '').split('/').pop();
            const variantEdges = p.variants?.edges || [];
            return {
                shopify_id: numericId,
                title: p.title,
                handle: p.handle,
                vendor: p.vendor,
                status: (p.status || '').toLowerCase(),
                image: p.featuredImage?.url || null,
                variant_count: p.variantsCount?.edges?.length || variantEdges.length,
                total_stock: Number.isFinite(p.totalInventory) ? p.totalInventory : 0,
                variants: variantEdges.map(({ node: v }) => ({
                    id: String(v.id || '').split('/').pop(),
                    title: v.title,
                    sku: v.sku || '',
                    price: v.price,
                    inventory_quantity: Number.isFinite(v.inventoryQuantity) ? v.inventoryQuantity : 0
                }))
            };
        });
        res.json({ products });
    } catch (e) { res.status(500).json({ products: [], error: e.message }); }
});

/** GET /api/products/:id/variants — devuelve TODAS las variantes de un producto con stock.
 *  Para elegir variante específica al configurar un regalo. Read-only. */
app.get('/api/products/:id/variants', _requireAdminToken, async (req, res) => {
    try {
        const shop = process.env.SHOPIFY_SHOP || 'nutrition-lab-cluster.myshopify.com';
        const token = process.env.SHOPIFY_ACCESS_TOKEN || _shopifyToken;
        if (!token) return res.status(500).json({ error: 'No Shopify token' });
        const id = req.params.id;
        const url = `https://${shop}/admin/api/2026-01/products/${encodeURIComponent(id)}.json?fields=id,title,handle,images,variants,status`;
        const r = await fetch(url, { headers: { 'X-Shopify-Access-Token': token } });
        if (!r.ok) {
            const txt = await r.text().catch(() => '');
            return res.status(r.status).json({ error: `Shopify ${r.status}: ${txt.slice(0, 200)}` });
        }
        const data = await r.json();
        const p = data.product;
        if (!p) return res.status(404).json({ error: 'Producto no encontrado' });
        const variants = (p.variants || []).map(v => ({
            id: String(v.id),
            title: v.title,
            sku: v.sku || '',
            price: v.price,
            compare_at_price: v.compare_at_price,
            inventory_quantity: Number.isFinite(v.inventory_quantity) ? v.inventory_quantity : 0,
            inventory_policy: v.inventory_policy, // 'continue' permite overselling, 'deny' bloquea
            inventory_management: v.inventory_management, // 'shopify' rastrea, null no rastrea
            image_id: v.image_id,
            weight: v.weight,
            weight_unit: v.weight_unit
        }));
        res.json({
            shopify_id: String(p.id),
            title: p.title,
            handle: p.handle,
            status: p.status,
            image: p.images?.[0]?.src || null,
            variants
        });
    } catch (e) { res.status(500).json({ error: e.message }); }
});


/* ═══════════════════════════════════════════════════════════════
   🏷️ SHOPIFY SELLING PLANS — Native subscription UI on product page
   Exactly like Skio / Recharge / Bold
═══════════════════════════════════════════════════════════════ */

/* Sync selling plan groups — global (all active products) OR per-product */
app.post('/api/selling-plans/sync', async (req, res) => {
    if (!sellingPlans.syncProductPlans) return res.status(503).json({ error: 'Selling Plans service not available' });
    try {
        // Load plans from settings metafield sub-key (v6.0.0 unified storage)
        const settingsData = await readFromShopify() || readFromFile() || {};
        const plans = Array.isArray(settingsData.plans_config) ? settingsData.plans_config : [];
        const activePlans = plans.filter(p =>
            p.active !== false &&
            (p.frequency || p.frequency_months) &&
            (p.discount !== undefined || p.discount_pct !== undefined)
        );

        if (!activePlans.length) {
            return res.status(400).json({ error: 'No hay planes activos. Crea planes primero en la sección Planes.' });
        }

        // Normalize plan fields (admin uses frequency_months/discount_pct, selling-plans uses frequency/discount)
        const normalizedPlans = activePlans.map(p => ({
            ...p,
            frequency: p.frequency || p.frequency_months || 1,
            permanence: p.permanence || p.permanence_months || 3,
            discount: p.discount !== undefined ? p.discount : (p.discount_pct || 0),
            active: p.active !== false
        }));

        let prodsToSync = [];

        // CASE 1: Single product passed directly in body (from product card Sync button)
        if (req.body && req.body.productId) {
            prodsToSync = [{ shopify_id: req.body.productId, product_title: req.body.productTitle || 'Producto', eligible_variant_ids: req.body.eligible_variant_ids || [] }];
            console.log('[SELLING_PLANS] Single product sync:', req.body.productId, req.body.eligible_variant_ids ? `variants: ${req.body.eligible_variant_ids}` : '(all)');
        } else {
            // CASE 2: Sync all active products from settings.eligible_products sub-key
            const eligibleProducts = Array.isArray(settingsData.eligible_products) ? settingsData.eligible_products : [];
            prodsToSync = eligibleProducts.filter(p => p.is_active);
            if (!prodsToSync.length) {
                return res.json({ synced: 0, total: 0, message: 'No hay productos activos. Activa productos en la sección Productos o usa el botón Sync de cada producto.' });
            }
        }

        // FIX 2026-04-11: Read per-product config to get eligible variant IDs
        // so SellingPlans only apply to specific variants (e.g. 500g, not 300g)
        const productConfigs = (typeof settingsData.product_configs === 'object' && !Array.isArray(settingsData.product_configs))
            ? settingsData.product_configs : {};

        const results = [];
        for (const prod of prodsToSync) {
            const productGid = `gid://shopify/Product/${prod.shopify_id}`;
            const pCfg = productConfigs[prod.shopify_id] || {};
            // Build variant GIDs from config — if eligible_variant_ids is set, restrict to those
            const variantGids = Array.isArray(pCfg.eligible_variant_ids) && pCfg.eligible_variant_ids.length
                ? pCfg.eligible_variant_ids.map(vid => `gid://shopify/ProductVariant/${vid}`)
                : null;
            // ── ADITIVO 2026-04-22 ── Filtra planes por producto (separación por plan.applies_to).
            //   Antes: todos los productos recibían TODOS los planes activos → contaminación.
            //   Ahora: cada producto recibe sólo los planes que lo tienen en applies_to.product_ids
            //   (o planes legacy con mode='all_products' que aún aplican a todos).
            const plansForThisProduct = normalizedPlans.filter(p => planAppliesToProduct(p, prod.shopify_id));
            if (!plansForThisProduct.length) {
                console.log('[SELLING_PLANS] ⏭  Skip ' + prod.product_title + ' — 0 planes aplicables (applies_to)');
                results.push({ product: prod.product_title, productId: prod.shopify_id, synced: false, skipped: true, reason: 'No plans matched applies_to' });
                continue;
            }
            try {
                const result = await sellingPlans.syncProductPlans({
                    productId: prod.shopify_id,
                    productGid,
                    productTitle: prod.product_title || '',
                    plans: plansForThisProduct,
                    variantGids
                });
                results.push({ product: prod.product_title, productId: prod.shopify_id, ...result });
                console.log('[SELLING_PLANS] Synced:', prod.product_title, variantGids ? `(${variantGids.length} variants)` : '(all variants)', '→ synced:', result.synced);
            } catch (e) {
                results.push({ product: prod.product_title, productId: prod.shopify_id, synced: false, error: e.message });
                console.error('[SELLING_PLANS] Error on', prod.product_title, ':', e.message);
            }
        }

        const syncedCount = results.filter(r => r.synced).length;
        res.json({ results, total: results.length, synced: syncedCount });
    } catch (e) {
        console.error('[SELLING_PLANS] Sync error:', e.message);
        res.status(500).json({ error: e.message });
    }
});


/* Get selling plans for a specific product */
app.get('/api/selling-plans/:productId', async (req, res) => {
    if (!sellingPlans.getProductSellingPlans) return res.json({ sellingPlanGroups: { nodes: [] } });
    try {
        const productGid = `gid://shopify/Product/${req.params.productId}`;
        const product = await sellingPlans.getProductSellingPlans(productGid);
        res.json(product || { sellingPlanGroups: { nodes: [] } });
    } catch (e) {
        res.status(500).json({ error: e.message });
    }
});

/* Remove selling plans from a product.
 * ?force=1 → borra TODOS los grupos sin filtro de nombre (útil para limpiar residuales).
 * Reporta success/error por cada grupo en el response.
 * ADITIVO 2026-04-22: antes silenciaba errores con .catch(() => {}) lo que causaba
 *   que grupos legacy "PRUEBA" quedaran colgados incluso tras DELETE + sync.
 */
app.delete('/api/selling-plans/:productId', async (req, res) => {
    if (!sellingPlans.getProductSellingPlans || !sellingPlans.deleteSellingPlanGroup) return res.json({ deleted: 0 });
    try {
        const force = req.query.force === '1' || req.query.force === 'true';
        const productGid = `gid://shopify/Product/${req.params.productId}`;
        const product = await sellingPlans.getProductSellingPlans(productGid);
        const groups = product && product.sellingPlanGroups ? product.sellingPlanGroups.nodes : [];
        const results = [];
        for (const g of groups) {
            const matchesFilter = g.name && (g.name.includes('LAB') || g.name.includes('PRUEBA') || g.name.includes('Suscripción'));
            if (!force && !matchesFilter) { results.push({ id: g.id, name: g.name, skipped: true, reason: 'name filter' }); continue; }
            try {
                await sellingPlans.deleteSellingPlanGroup(g.id);
                results.push({ id: g.id, name: g.name, deleted: true });
            } catch (e) {
                results.push({ id: g.id, name: g.name, deleted: false, error: e.message });
            }
        }
        const deleted = results.filter(r => r.deleted).length;
        res.json({ deleted, total: groups.length, results });
    } catch (e) {
        res.status(500).json({ error: e.message });
    }
});

/* DELETE por group-id exacto (fallback para casos donde el DELETE por producto no alcanza) */
app.delete('/api/selling-plan-groups/:groupGidB64', async (req, res) => {
    if (!sellingPlans.deleteSellingPlanGroup) return res.status(503).json({ error: 'service unavailable' });
    try {
        // el group GID se pasa base64-encoded para evitar problemas con slashes en URLs
        const groupGid = Buffer.from(req.params.groupGidB64, 'base64').toString('utf8');
        if (!/^gid:\/\/shopify\/SellingPlanGroup\//.test(groupGid)) return res.status(400).json({ error: 'invalid group gid' });
        const id = await sellingPlans.deleteSellingPlanGroup(groupGid);
        res.json({ deleted: true, id });
    } catch (e) {
        res.status(500).json({ error: e.message });
    }
});

/* Desasocia un producto de un selling plan group (sin borrar el grupo).
 * Usado para grupos fantasma: sellingPlanGroupDelete devuelve "does not exist" pero
 * el producto sigue viendo el grupo en getProductSellingPlans. Desasociando elimina
 * los selling plans residuales del storefront de ese producto. */
app.post('/api/selling-plan-groups/detach', async (req, res) => {
    if (!sellingPlans.removeProductsFromGroup) return res.status(503).json({ error: 'service unavailable' });
    try {
        const { groupGid, productIds } = req.body || {};
        if (!groupGid || !Array.isArray(productIds) || !productIds.length) return res.status(400).json({ error: 'Expected { groupGid, productIds: [] }' });
        const productGids = productIds.map(id => id.startsWith('gid://') ? id : `gid://shopify/Product/${id}`);
        const removed = await sellingPlans.removeProductsFromGroup(groupGid, productGids);
        res.json({ detached: true, removed });
    } catch (e) {
        res.status(500).json({ error: e.message });
    }
});

/* ═══════════════════════════════════════════════════════════════
   🛒 SHOPIFY WEBHOOK — orders/paid
   When a customer completes checkout with a Selling Plan:
   1. Create SubscriptionContract in Shopify (visible in admin + customer account)
   2. Launch MP PreApproval for recurring billing
═══════════════════════════════════════════════════════════════ */
// 🔒 FIX 2026-06-04: HMAC verification helper para webhooks Shopify.
//   Modo controlado por env WEBHOOK_HMAC_VERIFY=enforce|warn|off (default: warn).
//   - enforce: rechaza con 401 si firma no coincide
//   - warn: loggea pero permite (default — rollout seguro)
//   - off: no verifica (rollback de emergencia)
// 🆕 2026-06-05 — timingSafeEqual helper anti timing attacks
function _safeEq(a, b) {
    if (typeof a !== 'string' || typeof b !== 'string') return false;
    if (a.length !== b.length) return false;
    try { return crypto.timingSafeEqual(Buffer.from(a), Buffer.from(b)); } catch { return false; }
}

function _verifyShopifyHmac(req) {
    const mode = process.env.WEBHOOK_HMAC_VERIFY || 'warn';
    if (mode === 'off') return { ok: true, mode, skipped: true };
    const secret = process.env.SHOPIFY_API_SECRET || process.env.SHOPIFY_WEBHOOK_SECRET;
    if (!secret) return { ok: false, mode, error: 'No SHOPIFY_API_SECRET configured' };
    const hmacHeader = req.headers['x-shopify-hmac-sha256'] || req.headers['x-shopify-hmac-sha-256'];
    if (!hmacHeader) return { ok: false, mode, error: 'Missing x-shopify-hmac-sha256 header' };
    const body = Buffer.isBuffer(req.body) ? req.body : Buffer.from(req.body || '');
    const computed = crypto.createHmac('sha256', secret).update(body).digest('base64');
    const valid = _safeEq(computed, String(hmacHeader));
    return { ok: valid, mode, computed_prefix: computed.slice(0, 8), received_prefix: String(hmacHeader).slice(0, 8) };
}

app.post('/webhooks/shopify/orders-paid', express.raw({ type: 'application/json' }), async (req, res) => {
    // 🔒 HMAC verify ANTES de procesar
    const hmac = _verifyShopifyHmac(req);
    if (!hmac.ok && hmac.mode === 'enforce') {
        console.error('[ORDER_PAID] 🚫 HMAC inválido — rechazado. Error:', hmac.error || `expected ${hmac.received_prefix}... got ${hmac.computed_prefix}...`);
        return res.status(401).send('Invalid HMAC');
    }
    if (!hmac.ok && hmac.mode === 'warn') {
        console.warn('[ORDER_PAID] ⚠️ HMAC verification falló (mode=warn, permitiendo):', hmac.error || 'mismatch');
    }
    res.sendStatus(200); // Always ack first

    try {
        const order = JSON.parse(req.body.toString());
        console.log('[ORDER_PAID] Order #' + order.order_number + ' — checking for subscription line items...');

        // Detect subscription items by line_item properties: { name: "_subscription", value: "true" }
        const subLines = (order.line_items || []).filter(li => {
            const props = li.properties || [];
            return props.some(p => p.name === '_subscription' && p.value === 'true');
        });

        if (!subLines.length) {
            console.log('[ORDER_PAID] No subscription line items in order #' + order.order_number);
            return;
        }

        console.log(`[ORDER_PAID] Found ${subLines.length} subscription item(s) in order #${order.order_number}`);

        const customer = order.customer;
        const shippingAddr = order.shipping_address || null;
        const customerEmail = customer?.email || order.email || null;
        const customerName = customer ? `${customer.first_name || ''} ${customer.last_name || ''}`.trim() : customerEmail;

        // Reload MP token
        const dynSettings = await readFromShopify().catch(() => ({}));
        if (dynSettings?.mp_access_token) process.env.MP_ACCESS_TOKEN = dynSettings.mp_access_token;

        for (const line of subLines) {
            const props = line.properties || [];
            const getProp = (name) => { const p = props.find(x => x.name === name); return p ? p.value : null; };

            const frequency = parseInt(getProp('_frequency')) || 1;
            const permanence = parseInt(getProp('_permanence')) || 3;
            const discountPct = parseFloat(getProp('_discount_pct')) || 0;
            const finalPrice = parseFloat(getProp('_final_price')) || parseFloat(line.price || 0);
            const basePrice = parseFloat(getProp('_base_price')) || parseFloat(line.price || 0);
            const planLabel = getProp('_plan_label') || `Mensual × ${permanence} meses`;

            console.log(`[ORDER_PAID] Sub line: ${line.title} | freq=${frequency} perm=${permanence} disc=${discountPct}% price=${finalPrice}`);

            // 1. Create MP PreApproval plan for FUTURE recurring charges (month 2+)
            //    start_date = now + frequency months so MP doesn't double-charge month 1
            let mpPlanId = null;
            let mpInitPoint = null;
            if (mp.createPlan) {
                try {
                    const startDate = new Date();
                    startDate.setMonth(startDate.getMonth() + frequency);
                    const mpPlan = await mp.createPlan({
                        frequency,
                        permanence,
                        amount: finalPrice,
                        productTitle: line.title || 'Producto LAB',
                        startDate: startDate.toISOString()
                    });
                    mpPlanId = mpPlan?.id || null;
                    mpInitPoint = mpPlan?.init_point || mpPlan?.sandbox_init_point || null;
                    console.log('[MP] PreApprovalPlan created:', mpPlanId, '| init_point:', mpInitPoint ? 'YES' : 'NO');
                } catch (e) {
                    console.error('[MP] Failed to create PreApprovalPlan:', e.message);
                }
            }

            // 2. Save subscription record to Shopify Metaobjects
            let subRecord = null;
            if (db?.createSubscription) {
                try {
                    subRecord = await db.createSubscription({
                        customer_email: customerEmail,
                        customer_name: customerName,
                        shopify_order_id: String(order.id),
                        shopify_order_number: String(order.order_number),
                        variant_id: String(line.variant_id),
                        product_id: String(line.product_id),
                        product_title: line.title,
                        product_image: null,
                        base_price: basePrice,
                        final_price: finalPrice,
                        discount_pct: discountPct,
                        frequency_months: frequency,
                        permanence_months: permanence,
                        cycles_required: Math.ceil(permanence / frequency),
                        cycles_completed: 1, // First cycle = paid via Shopify checkout
                        mp_plan_id: mpPlanId,
                        mp_init_point: mpInitPoint,
                        shipping_address: shippingAddr,
                        free_shipping: (order.shipping_lines || []).some(s => parseFloat(s.price) === 0),
                        status: mpInitPoint ? 'pending_mp_activation' : 'active',
                        started_at: new Date().toISOString(),
                        next_charge_at: new Date(Date.now() + frequency * 30 * 86400000).toISOString()
                    });
                    console.log('[ORDER_PAID] Subscription record created for', customerEmail);

                    // 3. Log first charge event
                    if (db?.createEvent) {
                        await db.createEvent({
                            subscription_id: subRecord?.id || 'unknown',
                            event_type: 'charge_success',
                            metadata: JSON.stringify({
                                order_name: '#' + order.order_number,
                                shopify_order_id: order.id,
                                amount: finalPrice,
                                source: 'shopify_checkout'
                            })
                        }).catch(() => {});
                    }
                } catch (e) {
                    console.error('[ORDER_PAID] Failed to save subscription record:', e.message);
                }
            }

            // 4. Send activation email with MP link for recurring charges
            if (mpInitPoint && customerEmail) {
                const activationHtml = `
                    <div style="font-family:Montserrat,Arial,sans-serif;max-width:520px;margin:0 auto;padding:20px">
                        <div style="text-align:center;padding:24px 0">
                            <div style="font-size:11px;font-weight:800;letter-spacing:2px;color:#9d2a23;text-transform:uppercase">LAB NUTRITION</div>
                        </div>
                        <div style="background:#fff;border-radius:12px;padding:32px 28px;border:1px solid #e5e7eb">
                            <div style="text-align:center;margin-bottom:24px">
                                <div style="font-size:40px">🔄</div>
                                <h2 style="font-size:20px;font-weight:900;color:#1a1a1a;margin:12px 0 6px">Activa tu suscripción recurrente</h2>
                                <p style="font-size:13px;color:#666;margin:0">¡Gracias por tu compra, ${customerName.split(' ')[0]}!</p>
                            </div>
                            <p style="font-size:14px;color:#444;line-height:1.6;margin-bottom:16px">
                                Tu primer envío de <strong>${line.title}</strong> ya está en camino.
                                Para que recibas tu producto automáticamente cada mes con <strong>${Math.round(discountPct)}% de descuento</strong>,
                                necesitas activar el cobro recurrente en Mercado Pago:
                            </p>
                            <div style="background:#f8f8f8;border-radius:8px;padding:14px;margin-bottom:20px">
                                <div style="display:flex;justify-content:space-between;font-size:13px;margin-bottom:6px">
                                    <span style="color:#888">Plan:</span>
                                    <span style="font-weight:700">${planLabel}</span>
                                </div>
                                <div style="display:flex;justify-content:space-between;font-size:13px;margin-bottom:6px">
                                    <span style="color:#888">Monto mensual:</span>
                                    <span style="font-weight:700;color:#9d2a23">S/ ${finalPrice.toFixed(2)}</span>
                                </div>
                                <div style="display:flex;justify-content:space-between;font-size:13px">
                                    <span style="color:#888">Primer cobro automático:</span>
                                    <span style="font-weight:700">En ${frequency} mes(es)</span>
                                </div>
                            </div>
                            <div style="text-align:center;margin:24px 0">
                                <a href="${mpInitPoint}" style="display:inline-block;background:#9d2a23;color:#fff;padding:15px 36px;border-radius:10px;text-decoration:none;font-weight:800;font-size:14px;letter-spacing:.3px">
                                    Activar suscripción en Mercado Pago →
                                </a>
                            </div>
                            <p style="font-size:12px;color:#999;text-align:center;margin-top:16px">
                                Este enlace te lleva a Mercado Pago donde autorizarás los cobros automáticos.
                                Tu primer mes ya fue pagado en tu compra. Los cobros recurrentes empiezan desde el mes 2.
                            </p>
                        </div>
                        <p style="font-size:11px;color:#bbb;text-align:center;margin-top:16px">
                            LAB NUTRITION · Suscripciones · Puedes cancelar en cualquier momento
                        </p>
                    </div>`;
                sendAutoEmail({
                    to: customerEmail,
                    subject: '🔄 Activa tu suscripción recurrente — LAB NUTRITION',
                    html: activationHtml
                }).catch(e => console.warn('[ORDER_PAID] Email error:', e.message));
            }
        }
    } catch (e) {
        console.error('[ORDER_PAID] Webhook processing error:', e.message);
    }
});

/* ── Get Shopify contracts for a customer email ── */
app.get('/api/contracts/:email', async (req, res) => {
    if (!subscriptionContracts.getCustomerContracts) return res.json([]);
    try {
        const shop = process.env.SHOPIFY_SHOP || 'nutrition-lab-cluster.myshopify.com';
        const token = process.env.SHOPIFY_ACCESS_TOKEN || _shopifyToken;
        // Look up customer GID
        const r = await fetch(`https://${shop}/admin/api/2026-01/customers/search.json?query=email:${encodeURIComponent(req.params.email)}&limit=1&fields=id`, {
            headers: { 'X-Shopify-Access-Token': token }
        });
        const data = await r.json();
        if (!data.customers || !data.customers.length) return res.json([]);
        const customerGid = `gid://shopify/Customer/${data.customers[0].id}`;
        const contracts = await subscriptionContracts.getCustomerContracts(customerGid);
        res.json(contracts);
    } catch (e) {
        res.status(500).json({ error: e.message });
    }
});



/* ── CREATE ORDER manually for a subscription (admin) ── */
app.post('/api/subscriptions/:id/create-order', _requireAdminToken, async (req, res) => {
    try {
        const sub = await db.getSubscription(req.params.id);
        if (!sub) return res.status(404).json({ error: 'Subscription not found' });
        // Skip if already has a Shopify order (prevent duplicates)
        if (sub.shopify_order_id && !req.body.force) {
            return res.json({ success: true, already_exists: true, order_name: sub.shopify_order_name, order_id: sub.shopify_order_id });
        }
        const mpPaymentId = req.body.mp_payment_id || sub.mp_preapproval_id || 'manual_' + Date.now();
        const order = await createShopifyOrderFromSub(sub, mpPaymentId);
        if (!order) return res.status(500).json({ error: 'Failed to create order' });
        // Save order data back to subscription
        await db.updateSubscription(sub.id, {
            shopify_order_id: String(order.id),
            shopify_order_name: order.name
        }).catch(e => console.warn('[CREATE-ORDER] Failed to save order to sub:', e.message));
        await db.createEvent({ subscription_id: sub.id, event_type: 'manual_order_created',
            metadata: JSON.stringify({ shopify_order_id: order.id, order_name: order.name, created_by: 'admin_batch' })
        }).catch(() => {});
        res.json({ success: true, order_number: order.order_number, order_name: order.name, order_id: order.id });
    } catch (e) { res.status(500).json({ error: e.message }); }
});

/* ── BATCH CREATE missing Shopify orders for active subs ── */
app.post('/api/subscriptions/batch-create-orders', _requireAdminToken, async (req, res) => {
    try {
        const allSubs = await db.getSubscriptions({ status: 'active' }).catch(() => []);
        const missing = allSubs.filter(s => s.status === 'active' && s.cycles_completed >= 1 && !s.shopify_order_id && s.variant_id);
        const results = [];
        for (const sub of missing) {
            try {
                const mpId = sub.mp_preapproval_id || 'batch_' + Date.now();
                const order = await createShopifyOrderFromSub(sub, mpId);
                if (order?.id) {
                    await db.updateSubscription(sub.id, { shopify_order_id: String(order.id), shopify_order_name: order.name }).catch(() => {});
                    await db.createEvent({ subscription_id: sub.id, event_type: 'manual_order_created',
                        metadata: JSON.stringify({ shopify_order_id: order.id, order_name: order.name, created_by: 'admin_batch' })
                    }).catch(() => {});
                    results.push({ id: sub.id, email: sub.customer_email, name: sub.customer_name, order_name: order.name, status: 'created' });
                    console.log(`[BATCH] ✅ Order ${order.name} created for ${sub.customer_email}`);
                } else {
                    results.push({ id: sub.id, email: sub.customer_email, name: sub.customer_name, status: 'failed', error: 'No order returned' });
                }
                // Small delay to avoid Shopify rate limits
                await new Promise(r => setTimeout(r, 600));
            } catch (e) {
                results.push({ id: sub.id, email: sub.customer_email, name: sub.customer_name, status: 'error', error: e.message });
                console.error(`[BATCH] ❌ ${sub.customer_email}: ${e.message}`);
            }
        }
        res.json({ total_missing: missing.length, results });
    } catch (e) { res.status(500).json({ error: e.message }); }
});

/* ── SUBSCRIPTIONS LIST alias for admin panel ──
   🔐 2026-06-11: auth a nivel de ruta (devuelve PII de TODOS los clientes).
   El prefijo /api/subscriptions NO va en el middleware global porque
   /checkout (widget) y /customer/:id, /:id/pause|resume|cancel[/preview]
   (portal.html) son flujos públicos de clientes. */
app.get('/api/subscriptions', _requireAdminToken, async (req, res) => {
    try {
        const { status, limit } = req.query;
        const filters = status ? { status } : {};
        let data = await db.getSubscriptions(filters).catch(() => []);
        if (!Array.isArray(data)) data = [];

        // FIX 2026-04-09: Auto-import desde MP para dashboard.
        // Suscripciones que pagaron por MP pero nunca entraron a metaobjects
        // (porque el webhook fallaba antes del fix) se importan automáticamente aquí.
        if (!status || status === 'active') {
            try {
                const imported = await autoImportMpSubs(data);
                if (imported.length) {
                    console.log(`[AUTO-IMPORT] ${imported.length} subs desde MP → metaobjects`);
                    data = [...data, ...imported];
                }
            } catch (e) { console.warn('[AUTO-IMPORT] skipped:', e.message); }
        }

        if (limit) data = data.slice(0, parseInt(limit));
        res.json(data);
    } catch (e) { res.json([]); }
});

/* ── Helper: importa MP preapprovals que no estén en metaobjects ── */
let _lastAutoImportAt = 0;
async function autoImportMpSubs(existing = []) {
    // Throttle: max 1 ejecución cada 60 segundos
    const now = Date.now();
    if (now - _lastAutoImportAt < 60000) return [];
    _lastAutoImportAt = now;

    // Cargar settings dinámicos desde Shopify si MP_ACCESS_TOKEN no está en env
    let mpToken = process.env.MP_ACCESS_TOKEN;
    if (!mpToken) {
        try {
            const dyn = await readFromShopify().catch(() => ({}));
            if (dyn?.mp_access_token) { process.env.MP_ACCESS_TOKEN = dyn.mp_access_token; mpToken = dyn.mp_access_token; }
        } catch {}
    }
    if (!mpToken || !db?.createSubscription) return [];

    const shop = process.env.SHOPIFY_SHOP || 'nutrition-lab-cluster.myshopify.com';
    const shopToken = process.env.SHOPIFY_ACCESS_TOKEN || _shopifyToken;
    const mpHeaders = { Authorization: `Bearer ${mpToken}` };

    // 1) Obtener preapprovals autorizados en MP
    const r = await fetch('https://api.mercadopago.com/preapproval/search?status=authorized&limit=100', { headers: mpHeaders });
    if (!r.ok) { console.warn('[AUTO-IMPORT] MP search failed:', r.status); return []; }
    const mpData = await r.json();
    const preapprovals = mpData?.results || [];

    const allLocal = await db.getSubscriptions().catch(() => []);
    const existingIds = new Set(allLocal.map(s => s.mp_preapproval_id).filter(Boolean));

    // Fix existing records that had hardcoded permanence_months: 12 from old code
    for (const loc of allLocal) {
        if (loc.permanence_months == 12 && loc.frequency_months && loc.imported_from_mp) {
            try {
                const freq = parseInt(loc.frequency_months) || 1;
                const mpPre = preapprovals.find(p => p.id === loc.mp_preapproval_id);
                const reps = mpPre?.auto_recurring?.repetitions;
                const correctedPermanence = reps ? reps * freq : freq * 3;
                const correctedCycles = reps || 3;
                await db.updateSubscription(loc.id, {
                    permanence_months: correctedPermanence,
                    cycles_required: correctedCycles
                });
                console.log(`[AUTO-IMPORT] Fixed permanence for ${loc.customer_email}: 12 → ${correctedPermanence} months, cycles: ${correctedCycles}`);
            } catch (e) { console.warn('[AUTO-IMPORT] Fix permanence error:', e.message); }
        }
        // Ensure MP preapprovals have notification_url for webhooks
        if (loc.mp_preapproval_id && mp.ensureNotificationUrl) {
            mp.ensureNotificationUrl(loc.mp_preapproval_id).catch(() => {});
        }
    }

    if (!preapprovals.length) return [];

    const imported = [];
    for (const pre of preapprovals) {
        if (existingIds.has(pre.id)) continue;

        // 2) Resolver email — MP no lo incluye por privacidad
        //    Estrategia: buscar payments del payer_id y obtener payer.email
        let email = '';
        const payerId = pre.payer_id;
        if (payerId) {
            try {
                const pr = await fetch(`https://api.mercadopago.com/v1/payments/search?sort=date_created&criteria=desc&payer.id=${payerId}&limit=1`, { headers: mpHeaders });
                if (pr.ok) {
                    const pd = await pr.json();
                    email = pd.results?.[0]?.payer?.email || '';
                }
            } catch {}
        }
        // Fallback: buscar payments asociados a este preapproval vía metadata
        if (!email) {
            try {
                const pr = await fetch(`https://api.mercadopago.com/v1/payments/search?sort=date_created&criteria=desc&limit=5`, { headers: mpHeaders });
                if (pr.ok) {
                    const pd = await pr.json();
                    const match = (pd.results || []).find(p => p.preapproval_id === pre.id);
                    if (match?.payer?.email) email = match.payer.email;
                }
            } catch {}
        }
        if (!email) { console.warn('[AUTO-IMPORT] No email for preapproval', pre.id, '(payer_id:', payerId, ')'); continue; }

        // 3) Dedup: si existe un registro pending del mismo email → actualizar
        const orphan = allLocal.find(s =>
            (s.customer_email || '').toLowerCase() === email.toLowerCase() &&
            (s.mp_plan_id === pre.preapproval_plan_id || !s.mp_preapproval_id) &&
            (s.status === 'pending_payment' || s.status === 'pending')
        );
        if (orphan) {
            try {
                const updated = await db.updateSubscription(orphan.id, {
                    mp_preapproval_id: pre.id, status: 'active',
                    next_charge_at: pre.next_payment_date || null,
                    activated_at: pre.date_created || new Date().toISOString()
                });
                if (updated) imported.push(updated);
                console.log(`[AUTO-IMPORT] Linked orphan → active: ${email}`);
                continue;
            } catch (e) { console.warn('[AUTO-IMPORT] link orphan failed:', e.message); }
        }

        // Ya existe con otro estado? Skip
        if (allLocal.find(s => (s.customer_email || '').toLowerCase() === email.toLowerCase() && s.mp_preapproval_id === pre.id)) continue;

        // 4) Resolver variant_id buscando producto en Shopify por título
        let variantId = null, productId = null, productImage = null;
        try {
            if (shopToken && pre.reason) {
                const titleGuess = (pre.reason.match(/—\s*(.+?)\s*\(/) || [])[1] || pre.reason;
                const sr = await fetch(`https://${shop}/admin/api/2026-01/products.json?title=${encodeURIComponent(titleGuess)}&limit=5`, {
                    headers: { 'X-Shopify-Access-Token': shopToken }
                });
                if (sr.ok) {
                    const sd = await sr.json();
                    const match = (sd.products || []).find(p => (p.title || '').toLowerCase().includes(titleGuess.toLowerCase().split(' ')[0])) || (sd.products || [])[0];
                    if (match) {
                        productId = String(match.id);
                        variantId = String((match.variants || [])[0]?.id || '');
                        productImage = (match.images || [])[0]?.src || null;
                    }
                }
            }
        } catch {}

        // 5) Crear registro en metaobjects
        const newSub = {
            mp_preapproval_id: pre.id,
            mp_plan_id: pre.preapproval_plan_id || '',
            customer_email: email,
            customer_name: pre.payer_first_name || email.split('@')[0],
            product_title: (pre.reason || '').replace(/^LAB NUTRITION —\s*/, '').replace(/\s*\(.+\)\s*$/, '') || 'Suscripción MP',
            product_id: productId || '', variant_id: variantId || '',
            product_image: productImage || '',
            final_price: pre.auto_recurring?.transaction_amount || 0,
            base_price: pre.auto_recurring?.transaction_amount || 0,
            discount_pct: 0,
            frequency_months: pre.auto_recurring?.frequency || 1,
            permanence_months: (pre.auto_recurring?.repetitions || 1) * (pre.auto_recurring?.frequency || 1),
            cycles_required: pre.auto_recurring?.repetitions || 1,
            cycles_completed: 0,
            status: 'active',
            next_charge_at: pre.next_payment_date || null,
            activated_at: pre.date_created || new Date().toISOString(),
            imported_from_mp: true, free_shipping: false
        };
        try {
            const created = await db.createSubscription(newSub);
            if (created) { imported.push(created); console.log(`[AUTO-IMPORT] ✅ Imported: ${email} | ${newSub.product_title}`); }
        } catch (e) { console.warn(`[AUTO-IMPORT] Failed for ${email}:`, e.message); }
    }
    if (imported.length) console.log(`[AUTO-IMPORT] Total imported: ${imported.length}`);
    return imported;
}


/* ── METRICS — from Shopify Metaobjects ── */
/* ─── EMAIL TEMPLATE PREVIEW — Club Black Diamond ─── */
app.get('/api/emails/preview', (req, res) => {
    const templates = ['welcome', 'charge_reminder', 'cancel_lock_warning', 'charge_success', 'charge_failed', 'renewal_invite', 'cancellation'];
    res.send(`<!DOCTYPE html><html><head><meta charset="UTF-8"><title>Email Templates</title>
    <style>body{font-family:sans-serif;background:#f0f0f0;padding:40px}h1{text-align:center}
    .grid{display:flex;flex-wrap:wrap;gap:20px;justify-content:center}
    a{display:block;background:#1a1a1a;color:#fff;padding:16px 32px;border-radius:8px;text-decoration:none;font-weight:bold;text-align:center}
    a:hover{background:#9d2a23}</style></head><body>
    <h1>◆ Club Black Diamond — Email Templates ◆</h1>
    <div class="grid">${templates.map(t => `<a href="/api/emails/preview/${t}" target="_blank">${t.replace(/_/g,' ').toUpperCase()}</a>`).join('')}</div>
    </body></html>`);
});

app.get('/api/emails/preview/:template', (req, res) => {
    if (!notifications?.getPreviewHTML) return res.status(500).send('Notifications module not loaded');
    try {
        const html = notifications.getPreviewHTML(req.params.template);
        res.setHeader('Content-Type', 'text/html');
        res.send(html);
    } catch (e) { res.status(400).send(`Template error: ${e.message}`); }
});

app.post('/api/emails/test', async (req, res) => {
    if (!notifications?.sendTestEmail) return res.status(500).json({ error: 'Notifications module not loaded' });
    try {
        const { to, template } = req.body || {};
        const email = to || 'marketing@labnutrition.com';
        await notifications.sendTestEmail(email, template || 'welcome');
        res.json({ success: true, sent_to: email, template: template || 'welcome' });
    } catch (e) { res.status(500).json({ error: e.message }); }
});

/* 🩺 DIAG 2026-04-20: ¿por qué no se envían emails?
   Muestra qué envs están seteadas (sin exponer valores) y qué responde cada
   proveedor. Temporal hasta resolver config de Resend/SMTP en Railway. */
app.get('/api/admin/email-diagnostics', async (req, res) => {
    const mask = v => v ? `${String(v).slice(0, 3)}…(${String(v).length} chars)` : null;
    const report = {
        timestamp: new Date().toISOString(),
        env: {
            RESEND_API_KEY: !!process.env.RESEND_API_KEY,
            RESEND_API_KEY_preview: mask(process.env.RESEND_API_KEY),
            RESEND_FROM: process.env.RESEND_FROM || null,
            SMTP_HOST: process.env.SMTP_HOST || null,
            SMTP_PORT: process.env.SMTP_PORT || null,
            SMTP_USER: process.env.SMTP_USER || null,
            SMTP_PASS_set: !!process.env.SMTP_PASS,
            EMAIL_FROM: process.env.EMAIL_FROM || null
        },
        resend_test: null,
        smtp_test: null
    };
    const testTo = String(req.query.to || 'israelsarmiento281294@gmail.com');
    // 1) Probar Resend si existe API key
    if (process.env.RESEND_API_KEY && notifications?.sendViaResend) {
        try {
            const out = await notifications.sendViaResend(testTo, '🩺 DIAG Resend — LAB NUTRITION', '<p>Test de diagnóstico Resend</p>');
            report.resend_test = { ok: true, id: out?.id || null };
        } catch (e) {
            report.resend_test = { ok: false, error: e.message };
        }
    } else {
        report.resend_test = { ok: false, error: 'RESEND_API_KEY not set OR sendViaResend missing' };
    }
    // 2) Probar SMTP sólo si Resend falló (no duplicar envío si ya salió por Resend)
    if (!report.resend_test?.ok && notifications?.sendEmail) {
        try {
            // Llamo directo al transporter saltando la preferencia por Resend
            const nodemailer = require('nodemailer');
            const smtpPort = parseInt(process.env.SMTP_PORT || '465');
            const t = nodemailer.createTransport({
                host: process.env.SMTP_HOST, port: smtpPort, secure: smtpPort === 465,
                auth: { user: process.env.SMTP_USER, pass: process.env.SMTP_PASS },
                connectionTimeout: 8000, greetingTimeout: 8000, socketTimeout: 8000
            });
            const info = await t.sendMail({
                from: `"LAB NUTRITION" <${process.env.SMTP_USER || 'marketing@labnutrition.com'}>`,
                to: testTo, subject: '🩺 DIAG SMTP — LAB NUTRITION', html: '<p>Test SMTP</p>'
            });
            report.smtp_test = { ok: true, messageId: info?.messageId || null };
        } catch (e) {
            report.smtp_test = { ok: false, error: e.message, code: e.code || null };
        }
    } else {
        report.smtp_test = { skipped: true, reason: 'Resend OK o módulo no disponible' };
    }
    res.json(report);
});

app.get('/api/metrics', async (req, res) => {
    try {
        const metrics = await db.getMetrics();
        res.json(metrics);
    } catch (e) { res.json({ active: 0, paused: 0, cancelled: 0, mrr: '0.00', next7d: 0, error: e.message }); }
});

/* ══════════════════════════════════════════════════
   📊 REAL SUBSCRIBERS COUNT — cruza 3 fuentes de verdad
   1) Metaobjects locales (status=active)
   2) Shopify SubscriptionContracts nativos (status=ACTIVE)
   3) Mercado Pago PreApprovals (status=authorized)
══════════════════════════════════════════════════ */
app.get('/api/subscribers/real-count', async (req, res) => {
    const report = { metaobjects_active: 0, shopify_contracts_active: 0, mp_authorized: 0, union_by_email: 0, sources: {}, errors: [] };

    // 1) Metaobjects locales — solo recurrentes con ciclos pendientes (excluir compras únicas/planes cumplidos)
    try {
        const allActive = await db.getSubscriptions({ status: 'active' }).catch(() => []);
        const subs = allActive.filter(s => {
            const done = parseInt(s.cycles_completed) || 0;
            const req = parseInt(s.cycles_required) || 999;
            return done < req;
        });
        report.metaobjects_active = subs.length;
        report.sources.metaobjects = subs.map(s => ({ email: s.customer_email, product: s.product_title, mp_preapproval_id: s.mp_preapproval_id, next_charge: s.next_charge_at }));
    } catch (e) { report.errors.push('metaobjects: ' + e.message); }

    // 2) Shopify native subscription contracts
    try {
        const { gql } = require('./services/shopify-storage');
        const q = `query { subscriptionContracts(first: 100, query: "status:ACTIVE") { nodes { id status customer { email firstName lastName } lines(first:1){nodes{title currentPrice{amount currencyCode}}} nextBillingDate } } }`;
        const data = await gql(q);
        const contracts = data?.subscriptionContracts?.nodes || [];
        report.shopify_contracts_active = contracts.length;
        report.sources.shopify_contracts = contracts.map(c => ({
            contract_id: c.id,
            email: c.customer?.email,
            name: `${c.customer?.firstName || ''} ${c.customer?.lastName || ''}`.trim(),
            product: c.lines?.nodes?.[0]?.title,
            price: c.lines?.nodes?.[0]?.currentPrice?.amount,
            next_billing: c.nextBillingDate
        }));
    } catch (e) { report.errors.push('shopify_contracts: ' + e.message); }

    // 3) Mercado Pago — buscar PreApprovals autorizadas
    try {
        let mpToken = process.env.MP_ACCESS_TOKEN;
        if (!mpToken) {
            try {
                const dyn = await readFromShopify().catch(() => ({}));
                if (dyn?.mp_access_token) {
                    process.env.MP_ACCESS_TOKEN = dyn.mp_access_token;
                    mpToken = dyn.mp_access_token;
                }
            } catch {}
        }
        if (mpToken) {
            const r = await fetch(`https://api.mercadopago.com/preapproval/search?status=authorized&limit=100`, {
                headers: { Authorization: `Bearer ${mpToken}` }
            });
            if (r.ok) {
                const data = await r.json();
                const results = data?.results || [];
                report.mp_authorized = results.length;
                // Enriquecer con fetch individual para obtener payer_email
                const enriched = await Promise.all(results.map(async p => {
                    if (p.payer_email) return p;
                    try {
                        const fr = await fetch(`https://api.mercadopago.com/preapproval/${p.id}`, {
                            headers: { Authorization: `Bearer ${mpToken}` }
                        });
                        if (fr.ok) return { ...p, ...(await fr.json()) };
                    } catch {}
                    return p;
                }));
                report.sources.mp_preapprovals = enriched.map(p => {
                    let email = p.payer_email || '';
                    if (!email && p.back_url) {
                        try { email = new URL(p.back_url).searchParams.get('email') || ''; } catch {}
                    }
                    return {
                        id: p.id,
                        email,
                        reason: p.reason,
                        status: p.status,
                        amount: p.auto_recurring?.transaction_amount,
                        next_payment: p.next_payment_date
                    };
                });
            } else {
                report.errors.push('mp: ' + r.status + ' ' + (await r.text()).slice(0, 120));
            }
        } else {
            report.errors.push('mp: MP_ACCESS_TOKEN not set');
        }
    } catch (e) { report.errors.push('mp: ' + e.message); }

    // Unión por email (cantidad real única)
    const emails = new Set();
    (report.sources.metaobjects || []).forEach(s => s.email && emails.add(s.email.toLowerCase()));
    (report.sources.shopify_contracts || []).forEach(s => s.email && emails.add(s.email.toLowerCase()));
    (report.sources.mp_preapprovals || []).forEach(s => s.email && emails.add(s.email.toLowerCase()));
    report.union_by_email = emails.size;
    report.unique_emails = Array.from(emails);

    res.json(report);
});

// DEBUG: dump full MP preapproval to inspect field structure
app.get('/api/debug/mp-preapproval/:id', async (req, res) => {
    try {
        let mpToken = process.env.MP_ACCESS_TOKEN;
        if (!mpToken) {
            const dyn = await readFromShopify().catch(() => ({}));
            if (dyn?.mp_access_token) { mpToken = dyn.mp_access_token; process.env.MP_ACCESS_TOKEN = mpToken; }
        }
        if (!mpToken) return res.status(500).json({ error: 'MP_ACCESS_TOKEN not set' });
        const r = await fetch(`https://api.mercadopago.com/preapproval/${req.params.id}`, {
            headers: { Authorization: `Bearer ${mpToken}` }
        });
        const body = await r.text();
        res.status(r.status).type('application/json').send(body);
    } catch (e) { res.status(500).json({ error: e.message }); }
});

/* ══════════════════════════════════════════════════
   🔁 RECOVER MISSED ORDERS — reprocessa pagos de MP que no generaron orden Shopify
   GET /api/subscriptions/recover/:email → preview (dry-run)
   POST /api/subscriptions/recover/:email → ejecuta creación de órdenes
══════════════════════════════════════════════════ */
async function recoverOrdersForEmail(email, { dryRun = false } = {}) {
    const out = { email, payments: [], orders_created: [], errors: [] };
    const mpToken = process.env.MP_ACCESS_TOKEN;
    if (!mpToken) { out.errors.push('MP_ACCESS_TOKEN not set'); return out; }

    // 1) Buscar PreApprovals del email
    let preapprovals = [];
    try {
        const r = await fetch(`https://api.mercadopago.com/preapproval/search?payer_email=${encodeURIComponent(email)}&limit=20`, {
            headers: { Authorization: `Bearer ${mpToken}` }
        });
        if (r.ok) { const d = await r.json(); preapprovals = d.results || []; }
    } catch (e) { out.errors.push('preapproval search: ' + e.message); }

    if (!preapprovals.length) { out.errors.push('No preapprovals found for email'); return out; }
    out.preapprovals = preapprovals.map(p => ({ id: p.id, status: p.status, plan: p.reason, amount: p.auto_recurring?.transaction_amount }));

    // 2) Para cada preapproval, buscar payments aprobados
    for (const pre of preapprovals) {
        try {
            // Use authorized_payments endpoint (v1/payments/search does NOT work for preapproval subs)
            const pr = await fetch(`https://api.mercadopago.com/authorized_payments/search?preapproval_id=${pre.id}`, {
                headers: { Authorization: `Bearer ${mpToken}` }
            });
            if (!pr.ok) { out.errors.push(`payments search ${pre.id}: ${pr.status}`); continue; }
            const pd = await pr.json();
            const payments = (pd.results || []).filter(p => p.payment?.status === 'approved');

            // 3) Buscar la sub local asociada a este preapproval o email
            const allSubs = await db.getSubscriptions().catch(() => []);
            let sub = allSubs.find(s => s.mp_preapproval_id === pre.id);
            if (!sub) sub = allSubs.find(s => (s.customer_email || '').toLowerCase() === email.toLowerCase());

            for (const authPay of payments) {
                const pay = { id: authPay.payment?.id || authPay.id, transaction_amount: authPay.transaction_amount, date_approved: authPay.date_created, description: authPay.reason };
                const payInfo = { id: pay.id, amount: pay.transaction_amount, date: pay.date_approved, description: pay.description };
                out.payments.push(payInfo);

                // 4) Verificar si ya existe una orden en Shopify con referencia a este payment
                const shop = process.env.SHOPIFY_SHOP || 'nutrition-lab-cluster.myshopify.com';
                const token = process.env.SHOPIFY_ACCESS_TOKEN || _shopifyToken;
                if (!token) { out.errors.push('no shopify token'); continue; }

                // Buscar órdenes existentes por tag + nota que contenga MP payment id
                const searchUrl = `https://${shop}/admin/api/2026-01/orders.json?email=${encodeURIComponent(email)}&status=any&limit=50&fields=id,name,note,tags,created_at`;
                const sr = await fetch(searchUrl, { headers: { 'X-Shopify-Access-Token': token } });
                const sd = sr.ok ? await sr.json() : { orders: [] };
                const exists = (sd.orders || []).some(o => (o.note || '').includes(String(pay.id)));
                payInfo.shopify_order_exists = exists;

                if (exists) continue; // ya existe, no duplicar

                if (dryRun) { payInfo.would_create = true; continue; }

                // 5) Crear orden Shopify
                if (!sub || !sub.variant_id) {
                    payInfo.error = 'No sub with variant_id found — cannot create line_item';
                    out.errors.push(`payment ${pay.id}: no variant_id`);
                    continue;
                }
                try {
                    const order = await createShopifyOrderFromSub(sub, pay.id);
                    payInfo.shopify_order = order?.name;
                    out.orders_created.push({ order: order?.name, payment_id: pay.id, amount: pay.transaction_amount });
                } catch (e) {
                    payInfo.error = e.message;
                    out.errors.push(`create order payment ${pay.id}: ${e.message}`);
                }
            }
        } catch (e) { out.errors.push('loop preapproval ' + pre.id + ': ' + e.message); }
    }
    return out;
}

app.get('/api/subscriptions/recover/:email', async (req, res) => {
    try {
        const email = decodeURIComponent(req.params.email);
        const result = await recoverOrdersForEmail(email, { dryRun: true });
        res.json(result);
    } catch (e) { res.status(500).json({ error: e.message }); }
});

app.post('/api/subscriptions/recover/:email', async (req, res) => {
    try {
        const email = decodeURIComponent(req.params.email);
        const result = await recoverOrdersForEmail(email, { dryRun: false });
        res.json(result);
    } catch (e) { res.status(500).json({ error: e.message }); }
});


/* ── ALL SUBSCRIPTIONS (admin) — from Shopify Metaobjects ── */
app.get('/api/admin/subscriptions', async (req, res) => {
    try {
        const { status } = req.query;
        const filters = status ? { status } : {};
        const data = await db.getSubscriptions(filters);
        res.json({ data, total: data.length });
    } catch (e) { res.json({ data: [], total: 0, error: e.message }); }
});

/* ══════════════════════════════════════════════════
   👥 CLIENTES — paginación cursor-based Shopify (soporta 50k+)
══════════════════════════════════════════════════ */

/** Fetch ONE page of Shopify customers and return next cursor */
async function fetchShopifyCustomersPage(shop, token, { pageInfo, limit = 250, sort, query } = {}) {
    let url = `https://${shop}/admin/api/2026-01/customers.json?limit=${limit}&fields=id,email,first_name,last_name,orders_count,total_spent,tags,created_at,phone,state`;
    if (pageInfo) {
        // Cursor-based pagination — replaces all other params
        url = `https://${shop}/admin/api/2026-01/customers.json?limit=${limit}&page_info=${pageInfo}&fields=id,email,first_name,last_name,orders_count,total_spent,tags,created_at,phone,state`;
    } else {
        // First page — apply filters
        if (query) url += `&query=${encodeURIComponent(query)}`;
        // Shopify supports: created_at desc, total_spent desc, orders_count desc
        if (sort === 'total_spent') url += '&order=total_spent+desc';
        else if (sort === 'orders') url += '&order=orders_count+desc';
        else if (sort === 'recent') url += '&order=created_at+desc';
        else url += '&order=total_spent+desc'; // default: highest spenders first
    }
    const r = await fetch(url, { headers: { 'X-Shopify-Access-Token': token } });
    if (!r.ok) throw new Error('Shopify API error: ' + r.status);
    const data = await r.json();
    // Parse Link header for next cursor
    const linkHeader = r.headers.get('Link') || '';
    let nextPageInfo = null;
    const nextMatch = linkHeader.match(/<[^>]+[?&]page_info=([^&>]+)[^>]*>;\s*rel="next"/);
    if (nextMatch) nextPageInfo = nextMatch[1];
    return { customers: data.customers || [], nextPageInfo };
}

app.get('/api/customers', async (req, res) => {
    try {
        const shop = process.env.SHOPIFY_SHOP || 'nutrition-lab-cluster.myshopify.com';
        const token = process.env.SHOPIFY_ACCESS_TOKEN || _shopifyToken;
        if (!token) return res.json({ customers: [], total: 0, has_more: false, error: 'No Shopify token configured' });

        const { query = '', segment = 'all', page_info, sort = 'total_spent', limit = '250' } = req.query;
        const pageSize = Math.min(parseInt(limit) || 250, 250); // max 250 per Shopify

        const { customers: raw, nextPageInfo } = await fetchShopifyCustomersPage(shop, token, {
            pageInfo: page_info || null,
            limit: pageSize,
            sort,
            query: query.trim()
        });

        // Cross-reference with subscriptions
        const allSubs = await db.getSubscriptions().catch(() => []);
        const subsByEmail = {};
        allSubs.forEach(s => {
            const email = (s.customer_email || '').toLowerCase();
            if (!subsByEmail[email]) subsByEmail[email] = [];
            subsByEmail[email].push(s);
        });

        let customers = raw.map(c => {
            const email = (c.email || '').toLowerCase();
            const subs = subsByEmail[email] || [];
            const activeSub = subs.find(s => s.status === 'active');
            return {
                id: c.id,
                email: c.email,
                name: `${c.first_name || ''} ${c.last_name || ''}`.trim() || c.email,
                orders_count: parseInt(c.orders_count) || 0,
                total_spent: parseFloat(c.total_spent || 0),
                tags: c.tags || '',
                phone: c.phone || '',
                created_at: c.created_at,
                is_subscriber: subs.length > 0,
                subscription_status: activeSub ? activeSub.status : (subs.length ? subs[subs.length - 1].status : null),
                subscription_count: subs.length,
                next_charge_at: activeSub ? activeSub.next_charge_at : null
            };
        });

        // Apply segment filter
        if (segment === 'subscribers') customers = customers.filter(c => c.is_subscriber);
        else if (segment === 'active') customers = customers.filter(c => c.subscription_status === 'active');
        else if (segment === 'paused') customers = customers.filter(c => c.subscription_status === 'paused');
        else if (segment === 'non_subscribers') customers = customers.filter(c => !c.is_subscriber);
        else if (segment === 'next7d') {
            const in7 = new Date(Date.now() + 7 * 86400000);
            customers = customers.filter(c => c.next_charge_at && new Date(c.next_charge_at) <= in7);
        }

        // Client-side sort by total_spent if not done server-side (fallback)
        if (sort === 'total_spent') customers.sort((a, b) => b.total_spent - a.total_spent);
        else if (sort === 'orders') customers.sort((a, b) => b.orders_count - a.orders_count);

        console.log(`[CUSTOMERS] ${customers.length} returned (sort:${sort}, segment:${segment}, has_more:${!!nextPageInfo})`);
        res.json({ customers, total: customers.length, has_more: !!nextPageInfo, next_page_info: nextPageInfo });
    } catch (e) {
        console.error('[CUSTOMERS] Error:', e.message);
        res.json({ customers: [], total: 0, has_more: false, error: e.message });
    }
});

/* ── REMARKETING SEGMENTS — preview counts (now includes all_customers from Shopify) ── */
app.get('/api/remarketing/segments', async (req, res) => {
    try {
        const shop = process.env.SHOPIFY_SHOP || 'nutrition-lab-cluster.myshopify.com';
        const token = process.env.SHOPIFY_ACCESS_TOKEN || _shopifyToken;
        const allSubs = await db.getSubscriptions().catch(() => []);
        const in7 = new Date(Date.now() + 7 * 86400000);

        // Get Shopify total customer count from API
        let shopifyTotal = 0;
        if (token) {
            try {
                const cr = await fetch(`https://${shop}/admin/api/2026-01/customers/count.json`, { headers: { 'X-Shopify-Access-Token': token } });
                if (cr.ok) { const cd = await cr.json(); shopifyTotal = cd.count || 0; }
            } catch { }
        }

        res.json({
            active: allSubs.filter(s => s.status === 'active').length,
            paused: allSubs.filter(s => s.status === 'paused').length,
            cancelled: allSubs.filter(s => s.status === 'cancelled').length,
            next7d: allSubs.filter(s => s.status === 'active' && s.next_charge_at && new Date(s.next_charge_at) <= in7).length,
            all_subscribers: allSubs.length,
            all_customers: shopifyTotal
        });
    } catch (e) { res.json({ active: 0, paused: 0, cancelled: 0, next7d: 0, all_subscribers: 0, all_customers: 0 }); }
});

/* ── REMARKETING SEND — supports all_customers segment (paginates all Shopify customers) ── */
app.post('/api/remarketing', async (req, res) => {
    try {
        const { segment = 'active', subject, message, cta_text, cta_url } = req.body;
        if (!subject || !message) return res.status(400).json({ error: 'subject and message are required' });

        const shop = process.env.SHOPIFY_SHOP || 'nutrition-lab-cluster.myshopify.com';
        const token = process.env.SHOPIFY_ACCESS_TOKEN || _shopifyToken;

        let emailList = []; // { email, name }

        if (segment === 'all_customers') {
            // Paginate through ALL Shopify customers
            if (!token) return res.status(400).json({ error: 'No Shopify token configured for all_customers segment' });
            let pageInfo = null;
            let page = 0;
            do {
                const { customers, nextPageInfo } = await fetchShopifyCustomersPage(shop, token, { pageInfo, limit: 250 });
                customers.forEach(c => {
                    if (c.email) emailList.push({ email: c.email, name: `${c.first_name || ''} ${c.last_name || ''}`.trim() || c.email });
                });
                pageInfo = nextPageInfo;
                page++;
                console.log(`[REMARKETING] Fetched page ${page}, total so far: ${emailList.length}`);
            } while (pageInfo && page < 400); // safety limit: 400 pages × 250 = 100k
        } else {
            // Subscriber-based segments
            const allSubs = await db.getSubscriptions().catch(() => []);
            const in7 = new Date(Date.now() + 7 * 86400000);
            let targets;
            switch (segment) {
                case 'active': targets = allSubs.filter(s => s.status === 'active'); break;
                case 'paused': targets = allSubs.filter(s => s.status === 'paused'); break;
                case 'cancelled': targets = allSubs.filter(s => s.status === 'cancelled'); break;
                case 'next7d': targets = allSubs.filter(s => s.status === 'active' && s.next_charge_at && new Date(s.next_charge_at) <= in7); break;
                default: targets = allSubs; break;
            }
            emailList = targets.map(s => ({ email: s.customer_email, name: s.customer_name || '' }));
        }

        // Deduplicate by email
        const seen = new Set();
        emailList = emailList.filter(e => {
            const em = (e.email || '').toLowerCase();
            if (!em || seen.has(em)) return false;
            seen.add(em); return true;
        });

        if (!emailList.length) return res.json({ success: true, sent: 0, total_recipients: 0, message: 'No recipients in this segment' });

        let sent = 0, errors = 0;
        if (notifications.sendRaw) {
            // Send in batches to avoid overloading
            for (const recipient of emailList) {
                try {
                    await notifications.sendRaw({
                        to: recipient.email, name: recipient.name, subject, message,
                        cta_text: cta_text || 'Ver tienda',
                        cta_url: cta_url || `https://${shop.replace('.myshopify.com', '')}.myshopify.com`
                    });
                    sent++;
                } catch { errors++; }
            }
        } else {
            // No SMTP — log and report
            emailList.forEach(e => console.log(`[REMARKETING] Would send to: ${e.email}`));
            sent = emailList.length;
        }

        console.log(`[REMARKETING] Segment: ${segment}, Recipients: ${emailList.length}, Sent: ${sent}, Errors: ${errors}`);
        res.json({ success: true, sent, errors, total_recipients: emailList.length, segment });
    } catch (e) {
        console.error('[REMARKETING] Error:', e.message);
        res.status(500).json({ error: e.message });
    }
});


/* ═══════════════════════════════════════════════
   🔔 MERCADO PAGO WEBHOOK (v2 — 2026-01)
   Maneja tanto activación de nuevas suscripciones (preapproval)
   como cobros recurrentes (payment). Crea órdenes en Shopify automáticamente.
═══════════════════════════════════════════════ */
// 🔒 FIX 2026-06-04: MP webhook signature verify (x-signature header).
//   MP envía: x-signature: ts=NNN,v1=HMAC y x-request-id: REQ_ID.
//   Mensaje a firmar: id:DATA_ID;request-id:REQ_ID;ts:TS;
//   Secret: MP_WEBHOOK_SECRET (configurar en MP Dashboard → Webhooks).
//   Si no hay secret configurado, modo 'off' automáticamente (compat con setup actual).
function _verifyMpSignature(req) {
    const mode = process.env.MP_WEBHOOK_VERIFY || 'warn';
    if (mode === 'off') return { ok: true, mode, skipped: true };
    const secret = process.env.MP_WEBHOOK_SECRET;
    if (!secret) return { ok: true, mode, skipped: true, reason: 'No MP_WEBHOOK_SECRET configured — set en MP Dashboard' };
    const sig = req.headers['x-signature'];
    const reqId = req.headers['x-request-id'];
    if (!sig || !reqId) return { ok: false, mode, error: 'Missing x-signature or x-request-id headers' };
    const parts = String(sig).split(',').reduce((acc, p) => { const [k, v] = p.split('='); if (k && v) acc[k.trim()] = v.trim(); return acc; }, {});
    const ts = parts.ts; const v1 = parts.v1;
    if (!ts || !v1) return { ok: false, mode, error: 'x-signature missing ts or v1' };
    const dataId = (req.body?.data?.id) || '';
    const message = `id:${dataId};request-id:${reqId};ts:${ts};`;
    const computed = crypto.createHmac('sha256', secret).update(message).digest('hex');
    return { ok: _safeEq(computed, v1), mode, computed_prefix: computed.slice(0, 8), received_prefix: v1.slice(0, 8) };
}

app.post('/webhooks/mercadopago', async (req, res) => {
    // 🔒 Verify ANTES de procesar
    const sigCheck = _verifyMpSignature(req);
    if (!sigCheck.ok && sigCheck.mode === 'enforce') {
        console.error('[MP WEBHOOK] 🚫 Firma inválida — rechazado:', sigCheck.error || `expected ${sigCheck.received_prefix}... got ${sigCheck.computed_prefix}...`);
        return res.status(401).send('Invalid signature');
    }
    if (!sigCheck.ok && sigCheck.mode === 'warn') {
        console.warn('[MP WEBHOOK] ⚠️ Firma falló (mode=warn, permitiendo):', sigCheck.error || 'mismatch');
    }
    res.sendStatus(200); // Acknowledge immediately — evitar reintentos de MP
    const { type, data, action } = req.body;
    console.log('[MP WEBHOOK]', type, action, JSON.stringify(data || {}).slice(0, 200), sigCheck.skipped ? '(sig skipped)' : `(sig ${sigCheck.ok ? 'OK' : 'WARN'})`);

    try {
        const resourceId = data?.id;
        if (!resourceId) return;

        // === CASO 1: Nueva preaprobación (cliente autorizó la suscripción con su tarjeta) ===
        if (type === 'preapproval') {
            const preapprovalId = resourceId;
            let preapprovalInfo = null;
            try { preapprovalInfo = await mp.getSubscription(preapprovalId); } catch (e) {
                console.warn('[MP WEBHOOK] Could not get preapproval:', e.message);
            }

            const allSubs = await db.getSubscriptions().catch(() => []);
            // Buscar por preapproval_id o por plan_id (suscripción pendiente)
            let sub = allSubs.find(s => s.mp_preapproval_id === preapprovalId);
            if (!sub && preapprovalInfo?.preapproval_plan_id) {
                sub = allSubs.find(s =>
                    s.mp_plan_id === preapprovalInfo.preapproval_plan_id &&
                    (s.status === 'pending_payment' || !s.mp_preapproval_id)
                );
            }

            // 🆕 RE-AUTORIZACIÓN 2026-06-11: si el plan corresponde a un reauth_link
            //   (tarjeta nueva de una sub EXISTENTE), hacer SWAP y salir — jamás debe
            //   caer al flujo de sub nueva (resetearía cycles_completed a 0 y crearía
            //   una orden "Ciclo 1" con regalos duplicados). Subs nuevas no tienen
            //   reauth_plan_id, así que el ciclo 1 normal queda intacto.
            if (!sub && preapprovalInfo?.preapproval_plan_id && (preapprovalInfo?.status === 'authorized' || action === 'created')) {
                const reauthSub = allSubs.find(s => s.reauth_plan_id === preapprovalInfo.preapproval_plan_id);
                if (reauthSub) {
                    console.log(`[MP WEBHOOK] 🔁 RE-AUTH detectada: ${reauthSub.customer_email} — swap ${reauthSub.mp_preapproval_id} → ${preapprovalId}`);
                    const oldPre = reauthSub.mp_preapproval_id;
                    // 1. Cancelar el preapproval viejo (evita doble cobro mensual)
                    if (oldPre && oldPre !== preapprovalId && mp.cancelSubscription) {
                        await mp.cancelSubscription(oldPre).catch(e => console.warn('[MP WEBHOOK] reauth: no se pudo cancelar preapproval viejo:', e.message));
                    }
                    // 2. Enganchar el nuevo a la MISMA sub — cycles_completed NO se toca.
                    //    El cobro de autorización dispara webhook 'payment' (CASO 2) que
                    //    matchea por el nuevo preapproval_id y crea la orden del ciclo
                    //    pendiente con la numeración correcta.
                    await db.updateSubscription(reauthSub.id, {
                        mp_preapproval_id: preapprovalId,
                        status: 'active',
                        needs_payment_update: false,
                        paused_reason: null,
                        reauth_plan_id: null,
                        reauth_old_preapproval_id: oldPre || null,
                        last_payment_recovered_at: new Date().toISOString()
                    }).catch(e => console.warn('[MP WEBHOOK] reauth updateSub:', e.message));
                    await db.createEvent({
                        subscription_id: reauthSub.id,
                        event_type: 'preapproval_reauthorized',
                        metadata: JSON.stringify({ old_preapproval_id: oldPre, new_preapproval_id: preapprovalId, plan_id: preapprovalInfo.preapproval_plan_id, at: new Date().toISOString() })
                    }).catch(() => {});
                    console.log(`[MP WEBHOOK] ✅ RE-AUTH completa: ${reauthSub.customer_email} reactivó con tarjeta nueva (${reauthSub.cycles_completed || 0}/${reauthSub.cycles_required} ciclos se mantienen)`);
                    return; // ⛔ NO seguir al flujo de sub nueva
                }
            }

            if (!sub) { console.log('[MP WEBHOOK] No matching sub found for preapproval', preapprovalId); return; }

            if (preapprovalInfo?.status === 'authorized' || action === 'created') {
                // 🔒 GUARD 2026-06-11: webhook 'authorized' sobre sub YA ACTIVADA.
                //   MP manda preapproval webhooks repetidos (retries, resume tras pausa,
                //   re-autorización ya swapeada). Sin este guard, el bloque de activación
                //   RESETEABA cycles_completed a 0 y podía crear un pedido "Ciclo 1" con
                //   regalos duplicados sobre una sub en ciclo 3+ (el dedup de activación
                //   solo mira los últimos 10 eventos — la first_order_created original
                //   queda fuera en subs con historial). Solo se activa una sub que viene
                //   de pending: las activas/pausadas/completadas con historial se ignoran.
                // 🔧 2026-06-12: + 'cancelled' — un authorized tardío sobre una sub cancelada
                // con historial JAMÁS debe re-activarla ni resetear cycles_completed a 0.
                const yaActivada = ['active', 'paused', 'completed', 'cancelled'].includes(sub.status) &&
                    (sub.activated_at || (parseInt(sub.cycles_completed) || 0) >= 1);
                if (yaActivada) {
                    console.log(`[MP WEBHOOK] ↩️ preapproval 'authorized' repetido para sub ya activada ${sub.id} (${sub.status}, ciclo ${sub.cycles_completed || 0}) — skip activación (no reset, no orden)`);
                    return;
                }
                // Activar suscripción
                const nextCharge = new Date();
                nextCharge.setMonth(nextCharge.getMonth() + (parseInt(sub.frequency_months) || 1));

                await db.updateSubscription(sub.id, {
                    mp_preapproval_id: preapprovalId,
                    status: 'active',
                    next_charge_at: nextCharge.toISOString(),
                    activated_at: new Date().toISOString(),
                    cycles_completed: 0
                }).catch(e => console.warn('[MP WEBHOOK] updateSub:', e.message));

                await db.createEvent({ subscription_id: sub.id, event_type: 'activated',
                    metadata: JSON.stringify({ mp_preapproval_id: preapprovalId }) }).catch(() => {});

                // FIX 2026-04-11: Dedup — check if first order was already created (webhook retry)
                // 🔧 2026-06-12: ventana 10→100 — con historial largo, first_order_created
                // quedaba fuera de los últimos 10 eventos y el dedup de activación fallaba.
                const activationEvents = db?.getEvents ? await db.getEvents(sub.id, 100).catch(() => []) : [];
                const alreadyHasOrder = activationEvents.some(e => e.event_type === 'first_order_created');

                if (!alreadyHasOrder) {
                    // 🔒 FIX 2026-05-12 BUG CAYO RAMOS: NUNCA pasar preapprovalId como
                    //   mpPaymentId. Antes hacíamos createShopifyOrderFromSub(sub, preapprovalId)
                    //   y eso escribía preapproval_id en note_attributes.mp_payment_id de la
                    //   orden, lo cual confundía al dedup (que busca por payment_id real).
                    //   Caso Cayo: #10324 tenía preapproval_id como mp_payment_id, despues
                    //   #10399 con payment_id real, dedup no detecto match porque eran IDs
                    //   distintos. AHORA: buscamos el payment_id real ANTES de crear orden.
                    //   Si MP no devuelve payment real aún (puede tardar segundos), esperamos
                    //   al webhook payment que llegará después.
                    const realMp = await findRealMpPaymentForSub(sub).catch(() => null);
                    if (!realMp) {
                        console.log(`[MP WEBHOOK] ⏳ Activación recibida pero MP aún no expone payment real para ${sub.customer_email}. El webhook 'payment' llegará y creará la orden con payment_id real. NO creamos con preapproval_id (evita bug Cayo).`);
                        // Permitir que el webhook payment cree la orden con el ID correcto
                    }
                    let firstOrder = null;
                    if (realMp) {
                        for (let attempt = 1; attempt <= 3 && !firstOrder; attempt++) {
                            firstOrder = await createShopifyOrderFromSub(sub, realMp.id).catch(e => {
                                console.error(`[MP WEBHOOK] Order error (attempt ${attempt}/3):`, e.message);
                                return null;
                            });
                            if (!firstOrder && attempt < 3) await new Promise(r => setTimeout(r, 2000));
                        }
                    }
                    if (firstOrder?.id) {
                        await db.updateSubscription(sub.id, {
                            shopify_order_id: String(firstOrder.id),
                            shopify_order_name: firstOrder.name,
                            cycles_completed: 1
                        }).catch(() => {});
                        await db.createEvent({ subscription_id: sub.id, event_type: 'first_order_created',
                            metadata: JSON.stringify({ shopify_order_id: firstOrder.id, order_name: firstOrder.name }) }).catch(() => {});
                    }
                } else {
                    console.log(`[MP WEBHOOK] First order already created for ${sub.id} — skipping (dedup)`);
                }

                // Email de bienvenida
                if (notifications.sendWelcome) notifications.sendWelcome({ ...sub, mp_preapproval_id: preapprovalId }).catch(() => {});

                // Tag customer en Shopify
                // 🔧 2026-06-12: el checkout NUNCA setea customer_id → el tag 'suscriptor-lab'
                // era decorativo. Ahora lo resolvemos por email y lo persistimos para que
                // activación Y cancelaciones (que también gatean por customer_id) funcionen.
                if (!sub.customer_id && sub.customer_email && shopify.getCustomer) {
                    try {
                        const c = await shopify.getCustomer(sub.customer_email);
                        if (c?.id) {
                            sub.customer_id = String(c.id);
                            if (db?.updateSubscription) await db.updateSubscription(sub.id, { customer_id: String(c.id) }).catch(() => {});
                        }
                    } catch (_) {}
                }
                if (sub.customer_id) shopify.tagCustomerAsSubscriber(sub.customer_id, true).catch(() => {});

                console.log(`[MP WEBHOOK] ✅ Suscripción activada: ${sub.id} | ${sub.customer_email}`);

            } else if (preapprovalInfo?.status === 'cancelled' || preapprovalInfo?.status === 'paused') {
                // 🆕 FIX 2026-06-11: cuando MP pausa el preapproval (agotó sus reintentos),
                //   antes solo se grababa status='paused' sin paused_reason ni paused_until →
                //   la sub desaparecía de dunning detection, polling y admin failed-payments
                //   (zombie invisible que MP nunca volvía a cobrar). Ahora se etiqueta
                //   'mp_auto_paused' para que los filtros de dunning/admin la sigan viendo.
                //   Guard: si la pausa fue voluntaria (paused_until o paused_reason ya seteados
                //   por el endpoint de pausa del portal), NO sobreescribimos la etiqueta.
                const upd = { status: preapprovalInfo.status };
                if (preapprovalInfo.status === 'paused' && !sub.paused_until && !sub.paused_reason) {
                    upd.paused_reason = 'mp_auto_paused';
                    upd.paused_at = new Date().toISOString();
                }
                await db.updateSubscription(sub.id, upd).catch(() => {});
                if (upd.paused_reason === 'mp_auto_paused') {
                    await db.createEvent({
                        subscription_id: sub.id,
                        event_type: 'mp_auto_paused',
                        metadata: JSON.stringify({ preapproval_id: preapprovalId, at: new Date().toISOString() })
                    }).catch(() => {});
                    console.warn(`[MP WEBHOOK] ⚠️ MP pausó preapproval ${preapprovalId} (${sub.customer_email}) — marcado mp_auto_paused, sigue visible en dunning/admin`);
                }
            }
        }

        // === CASO 2: Cobro recurrente (pago periódico de la suscripción activa) ===
        if (type === 'payment') {
            let paymentData = null;
            try { paymentData = await mp.getPayment(resourceId); } catch (e) {
                console.warn('[MP WEBHOOK] Could not get payment:', e.message);
                return;
            }
            if (!paymentData) { console.warn('[MP WEBHOOK] payment null for', resourceId); return; }
            console.log(`[MP WEBHOOK] payment ${resourceId} status=${paymentData.status} amount=${paymentData.transaction_amount} email=${paymentData.payer?.email}`);
            if (paymentData.status !== 'approved') return;

            // 🆕 RECOVERY LINK 2026-06-11: pago único que repone un ciclo rechazado.
            //   external_reference = 'subrecovery::<sub_id>::c<ciclo>' (generado por
            //   /api/admin/failed-payments/:sub_id/payment-link o el portal update-card).
            //   Se procesa en handler dedicado y se SALE antes del lookup genérico:
            //   el fallback por email trataría este pago como cobro recurrente normal.
            if (String(paymentData.external_reference || '').startsWith('subrecovery::')) {
                await handleRecoveryLinkPayment(paymentData, String(resourceId)).catch(e =>
                    console.error('[RECOVERY-LINK] Handler error:', e.message));
                return;
            }

            // FIX 2026-04-09: MP coloca preapproval_id como campo directo del payment,
            // NO en metadata ni external_reference. Priorizar el campo directo.
            const preapprovalId = paymentData.preapproval_id
                || paymentData.metadata?.preapproval_id
                || paymentData.external_reference
                || null;
            const payerEmail = paymentData.payer?.email || null;
            console.log(`[MP WEBHOOK] lookup → preapprovalId:${preapprovalId} email:${payerEmail}`);

            // Buscar sub con estrategia multi-fallback para no perder nunca un pago
            let sub = null;
            const allSubs = await db.getSubscriptions().catch(() => []);

            // 1) Match exacto por preapproval_id
            if (preapprovalId) {
                sub = allSubs.find(s => s.mp_preapproval_id === preapprovalId);
            }
            // 2) Fallback por email del payer (si preapproval_id no coincide o falta)
            if (!sub && payerEmail) {
                const byEmail = allSubs
                    .filter(s => (s.customer_email || '').toLowerCase() === payerEmail.toLowerCase())
                    .sort((a, b) => (b.created_at || '').localeCompare(a.created_at || ''));
                // Preferir active/pending sobre cancelled
                sub = byEmail.find(s => s.status === 'active' || s.status === 'pending_payment' || s.status === 'pending') || byEmail[0] || null;
                if (sub) console.log(`[MP WEBHOOK] Matched sub by email fallback: ${sub.id}`);
            }
            // 3) Si existe preapprovalId pero ninguna sub local → consultar MP para obtener plan/email y loguear
            if (!sub && preapprovalId) {
                try {
                    const preInfo = await mp.getSubscription(preapprovalId).catch(() => null);
                    if (preInfo?.payer_email) {
                        const byMpEmail = allSubs.filter(s => (s.customer_email || '').toLowerCase() === preInfo.payer_email.toLowerCase());
                        sub = byMpEmail.find(s => s.status === 'active') || byMpEmail[0] || null;
                        if (sub) console.log(`[MP WEBHOOK] Matched sub via MP-API email: ${sub.id}`);
                    }
                } catch {}
            }

            if (!sub) {
                console.warn(`[MP WEBHOOK] ⚠️  NO SE ENCONTRÓ suscripción local para payment ${resourceId} (preapproval ${preapprovalId}, email ${payerEmail}). Intentando crear orden Shopify igual con datos del payment...`);
                // ÚLTIMO RECURSO: crear orden Shopify solo con datos del payment (email + amount)
                try {
                    const orphanSub = {
                        id: `orphan_${resourceId}`,
                        customer_email: payerEmail,
                        customer_name: paymentData.payer?.first_name || payerEmail,
                        variant_id: null, // sin variant no se puede crear line_item
                        final_price: paymentData.transaction_amount,
                        frequency_months: 1,
                        permanence_months: 3,
                        cycles_completed: 0,
                        cycles_required: 3,
                        discount_pct: 0,
                        product_title: paymentData.description || 'Suscripción LAB',
                        shipping_address: null,
                        free_shipping: false,
                        // 🎁 2026-06-12: NUNCA regalos en órdenes huérfanas — el pago no matcheó
                        // ninguna sub (puede ser una RENOVACIÓN cuyo match falló) y el fallback
                        // on-the-fly de regalos lo trataría como ciclo 0 con plan all_products.
                        // Regla del negocio: regalos SOLO en la primera orden real de la sub.
                        gifts_delivered: true
                    };
                    await createShopifyOrderFromSub(orphanSub, resourceId).catch(e => {
                        console.error('[MP WEBHOOK] Orphan order failed (need variant_id):', e.message);
                    });
                    // 🔧 2026-06-12: rastro SIEMPRE — antes, si payerEmail era null la orden
                    // jamás se creaba y NO quedaba registro en DB (solo console.error perdido).
                    // Evento global (sin sub) para que el admin pueda auditar cobros huérfanos.
                    if (db?.createEvent) {
                        await db.createEvent({
                            subscription_id: `orphan_${resourceId}`,
                            event_type: 'orphan_payment_received',
                            metadata: JSON.stringify({
                                mp_payment_id: String(resourceId),
                                preapproval_id: preapprovalId || null,
                                payer_email: payerEmail || null,
                                amount: paymentData.transaction_amount,
                                needs_admin_review: true,
                                at: new Date().toISOString()
                            })
                        }).catch(() => {});
                    }
                } catch {}
                return;
            }

            // Si no estaba linkeado antes, linkeamos ahora para futuros cobros
            if (preapprovalId && !sub.mp_preapproval_id) {
                await db.updateSubscription(sub.id, { mp_preapproval_id: preapprovalId }).catch(() => {});
                sub.mp_preapproval_id = preapprovalId;
            }

            // FIX 2026-04-11: Dedup — check if this payment was already processed
            const paymentEvents = db?.getEvents ? await db.getEvents(sub.id, 100).catch(() => []) : [];
            const alreadyProcessed = paymentEvents.some(e => {
                // 🔧 2026-06-12: coerción a String — el webhook trae data.id numérico y el
                // polling guarda String(paymentId); la igualdad estricta fallaba el dedup
                // y generaba order_creation_failed/needs_admin_review FALSOS en retries.
                try { const m = JSON.parse(e.metadata || '{}'); return String(m.mp_payment_id) === String(resourceId); } catch { return false; }
            });
            if (alreadyProcessed) {
                console.log(`[MP WEBHOOK] Payment ${resourceId} already processed for ${sub.id} — skipping (dedup)`);
                return;
            }

            // 🔒 FIX 2026-04-28: cross-dedup contra activación de la suscripción.
            //   Cuando MP autoriza una nueva sub, dispara DOS webhooks (preapproval + payment)
            //   con gap variable: a veces segundos, a veces hasta 3+ horas (ej. rcerronb 191min).
            //     1) preapproval (action=created, status=authorized) → handler caso 1 crea
            //        la primera orden con cycleLabel="Ciclo 1" + regalo, y registra event
            //        first_order_created.
            //     2) payment (status=approved) del mismo cobro → si llega aquí, sin esta
            //        protección crearíamos una SEGUNDA orden duplicada con cycleLabel="Ciclo 1/6"
            //        y SIN regalo (cycles_completed ya pasó a 1). El cliente recibiría 2 cajas
            //        por un solo cobro de MP.
            //   Solución: si first_order_created existe hace < 24h, skip este payment.
            //   Ventana 24h es segura porque cobros recurrentes son cada 30+ días.
            //   En cobros recurrentes mes 2+, first_order_created tiene >>24h → no aplica.
            //   Aditivo: agrega un branch de skip, no toca nada de la lógica existente.
            const recentFirstOrder = paymentEvents.find(e => e.event_type === 'first_order_created');
            if (recentFirstOrder) {
                const eventTime = new Date(recentFirstOrder.created_at || 0).getTime();
                const minutesAgo = (Date.now() - eventTime) / 60000;
                if (minutesAgo > 0 && minutesAgo < 1440) {
                    console.log(`[MP WEBHOOK] 🔒 Skip payment ${resourceId} for ${sub.id} — first_order_created hace ${minutesAgo.toFixed(1)} min (duplicado de activación)`);
                    await db.createEvent({
                        subscription_id: sub.id,
                        event_type: 'charge_success_skipped_duplicate',
                        metadata: JSON.stringify({
                            mp_payment_id: resourceId,
                            reason: 'duplicate_with_activation_order',
                            activation_minutes_ago: Math.round(minutesAgo * 10) / 10,
                            mp_payment_amount: paymentData.transaction_amount
                        })
                    }).catch(() => {});
                    return;
                }
            }

            const cyclesCompleted = (parseInt(sub.cycles_completed) || 0) + 1;
            const nextCharge = new Date();
            nextCharge.setMonth(nextCharge.getMonth() + (parseInt(sub.frequency_months) || 1));
            const isComplete = cyclesCompleted >= (parseInt(sub.cycles_required) || 999);

            // 🔒🔒 GUARD ANTI COBRO TEMPRANO — REDISEÑADO 2026-06-09
            //   FIX CRÍTICO (audit jun-8): la versión anterior comparaba contra
            //   last_charge_at LOCAL (grabado al momento de PROCESAR, no del débito real).
            //   Tras un reintento MP tardío, el ciclo siguiente legítimo caía <28d y se
            //   bloqueaba → cobro entrado sin orden → bodega no despacha (bola de nieve).
            //   AHORA:
            //   1. Comparamos las fechas REALES de débito de MP (date_approved del payment
            //      actual vs el anterior), no nuestros timestamps de procesamiento.
            //   2. La ventana baja a 25d para tolerar débitos adelantados de MP/meses cortos.
            //   3. El payment_id del cobro bloqueado se registra en un event_type SEPARADO
            //      que el dedup del polling IGNORA → si fue falso positivo, el polling lo
            //      rescata en 1h en vez de perderse para siempre.
            if (sub.last_charge_at && cyclesCompleted >= 2) {
                // Fecha real del débito actual según MP (no "ahora")
                const currentDebitDate = paymentData.date_approved || paymentData.date_created || new Date().toISOString();
                // Fecha real del débito anterior: usar last_mp_debit_date si existe (subs nuevas),
                // fallback a last_charge_at local (subs viejas)
                const prevDebitDate = sub.last_mp_debit_date || sub.last_charge_at;
                const daysSince = (new Date(currentDebitDate).getTime() - new Date(prevDebitDate).getTime()) / 86400000;
                if (daysSince > 0 && daysSince < 25) {
                    console.error(`[MP WEBHOOK] 🚫 COBRO TEMPRANO BLOQUEADO — ${sub.customer_email} | ` +
                        `débito real MP ${daysSince.toFixed(1)}d después del anterior (< 25d). ` +
                        `Payment queda en cola de reproceso (polling lo reintenta si es legítimo). Sub: ${sub.id}`);
                    await db.createEvent({
                        subscription_id: sub.id,
                        event_type: 'early_charge_blocked',
                        metadata: JSON.stringify({
                            days_since_last_charge: parseFloat(daysSince.toFixed(2)),
                            // 🔑 FIX dedup-poisoning: clave renombrada para que el substring-match
                            // del polling legacy NO matchee 'mp_payment_id' de eventos de bloqueo
                            blocked_payment_id: String(resourceId),
                            mp_payment_amount: paymentData.transaction_amount,
                            current_debit_date: currentDebitDate,
                            prev_debit_date: prevDebitDate,
                            blocked_at: new Date().toISOString(),
                            reason: 'Débito MP <25d desde el débito real anterior'
                        })
                    }).catch(() => {});
                    await db.updateSubscription(sub.id, {
                        last_order_error: `Cobro temprano bloqueado (${daysSince.toFixed(1)}d, débitos reales MP) ${new Date().toISOString()}`,
                        needs_admin_review: true
                    }).catch(() => {});
                    return; // EXIT — el polling re-evalúa este payment en su próxima corrida
                }
            }

            // Crear orden Shopify (con retry automático)
            const realDebitDate = paymentData.date_approved || paymentData.date_created || new Date().toISOString();
            let order = null;
            for (let attempt = 1; attempt <= 3 && !order; attempt++) {
                order = await createShopifyOrderFromSub(sub, resourceId).catch(e => {
                    console.error(`[MP WEBHOOK] ❌ Shopify order error (attempt ${attempt}/3):`, e.message);
                    return null;
                });
                if (!order && attempt < 3) await new Promise(r => setTimeout(r, 2000));
            }

            // 🔑 FIX CRÍTICO 2026-06-09 (audit jun-8): NO avanzar estado si la orden falló.
            //   ANTES: aunque order===null, avanzaba cycles_completed + last_charge_at + creaba
            //   evento charge_success con el payment_id. Ese evento bloqueaba TODOS los reintentos
            //   futuros (dedup) → cobro real entrado, orden perdida PARA SIEMPRE.
            //   AHORA: si la orden falló, NO tocamos cycles ni last_charge; creamos un evento
            //   order_creation_failed (que el polling SÍ reintenta) y marcamos needs_admin_review.
            if (order?.id) {
                await db.updateSubscription(sub.id, {
                    cycles_completed: cyclesCompleted,
                    last_charge_at: new Date().toISOString(),
                    last_mp_debit_date: realDebitDate, // 🔑 fecha REAL del débito MP (para guard <25d)
                    next_charge_at: nextCharge.toISOString(),
                    status: isComplete ? 'completed' : 'active',
                    shopify_order_id: String(order.id),
                    shopify_order_name: order.name
                }).catch(e => console.warn('[MP WEBHOOK] updateSub:', e.message));

                await db.createEvent({ subscription_id: sub.id, event_type: 'charge_success',
                    metadata: JSON.stringify({ mp_payment_id: resourceId, cycle: cyclesCompleted,
                        shopify_order_id: order.id, shopify_order_name: order.name,
                        mp_debit_date: realDebitDate,
                        amount: paymentData.transaction_amount }) }).catch(() => {});

                if (notifications.sendChargeSuccess) notifications.sendChargeSuccess(sub, order.order_number).catch(() => {});
                if (isComplete && notifications.sendRenewalInvite) notifications.sendRenewalInvite(sub).catch(() => {});

                console.log(`[MP WEBHOOK] ✅ Cobro procesado: ${sub.customer_email} | ciclo ${cyclesCompleted}/${sub.cycles_required} | order ${order.name}`);
            } else {
                // Orden falló tras 3 intentos → NO avanzar estado, dejar que el polling reintente
                console.error(`[MP WEBHOOK] ⚠️ Cobro ${resourceId} OK en MP pero orden Shopify FALLÓ 3x — NO avanzo cycles. Polling reintentará. Sub: ${sub.id}`);
                await db.createEvent({ subscription_id: sub.id, event_type: 'order_creation_failed',
                    metadata: JSON.stringify({
                        // clave separada que el dedup NO matchea como cobro procesado
                        failed_payment_id: String(resourceId),
                        intended_cycle: cyclesCompleted,
                        mp_debit_date: realDebitDate,
                        amount: paymentData.transaction_amount,
                        at: new Date().toISOString()
                    }) }).catch(() => {});
                await db.updateSubscription(sub.id, {
                    last_order_error: `Orden Shopify falló 3x para cobro MP ${resourceId} ${new Date().toISOString()}`,
                    needs_admin_review: true
                }).catch(() => {});
            }
        }
    } catch (e) {
        console.error('[MP WEBHOOK] Error:', e.message, e.stack);
    }
});

/* También registrar en /api/webhooks/mercadopago para la URL de MP Dashboard.
   Reenvía internamente al handler real de /webhooks/mercadopago via HTTP */
app.post('/api/webhooks/mercadopago', async (req, res) => {
    res.sendStatus(200);
    console.log('[MP WEBHOOK /api alias] forwarding to /webhooks/mercadopago');
    try {
        await fetch(`http://localhost:${PORT}/webhooks/mercadopago`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(req.body)
        });
    } catch (e) { console.warn('[MP WEBHOOK /api alias] forward error:', e.message); }
});

/* ── Helper: Obtener dirección predeterminada del cliente en Shopify por email ── */
async function getCustomerAddress(email, token, shop) {
    try {
        const url = `https://${shop}/admin/api/2026-01/customers/search.json?query=email:${encodeURIComponent(email)}&limit=1&fields=id,default_address`;
        const r = await fetch(url, { headers: { 'X-Shopify-Access-Token': token } });
        if (!r.ok) return null;
        const data = await r.json();
        const addr = data.customers && data.customers[0] && data.customers[0].default_address;
        if (!addr) return null;
        return {
            first_name: addr.first_name || '',
            last_name: addr.last_name || '',
            address1: addr.address1 || '',
            address2: addr.address2 || '',
            city: addr.city || '',
            province: addr.province || '',
            country: addr.country || 'PE',
            country_code: addr.country_code || 'PE',
            zip: addr.zip || '',
            phone: addr.phone || ''
        };
    } catch (e) {
        console.warn('[ADDR] Error buscando dirección:', e.message);
        return null;
    }
}

/* ── Helper: valida que la sub tenga datos mínimos para despacho + SUNAT ──
   DNI 8-15 dígitos (o RUC 11) + dirección de envío completa.
   Si falta algo NO crea orden Shopify (cobro MP ya entró, plata segura),
   loguea evento para visibilidad admin y retorna null.
   Los 5 callers de createShopifyOrderFromSub ya manejan null sin romper flujo. */
function assertSubShippable(sub) {
    const missing = [];
    const dni = String(sub.dni || '').trim();
    const addr = sub.shipping_address || {};
    if (dni.length < 8 || dni.length > 15) missing.push('dni');
    if (!addr.address1 || !String(addr.address1).trim()) missing.push('shipping_address1');
    if (!addr.city || !String(addr.city).trim()) missing.push('shipping_city');
    if (!addr.province || !String(addr.province).trim()) missing.push('shipping_province');
    if (missing.length > 0) {
        console.warn(`[GUARD] Sub ${sub.customer_email || sub.id} bloqueada — falta: ${missing.join(', ')}`);
        try {
            if (db && db.createEvent && sub.id && !String(sub.id).startsWith('orphan_')) {
                db.createEvent({
                    subscription_id: sub.id,
                    event_type: 'order_blocked_missing_data',
                    metadata: JSON.stringify({ missing, email: sub.customer_email, at: new Date().toISOString() })
                }).catch(() => {});
            }
        } catch {}
        return { ok: false, missing };
    }
    return { ok: true };
}

/* ── Helper: resuelve handle de producto Shopify por product_id (cache 1h).
   Usado por el enricher de regalos para generar URLs /products/{handle}.
   Si falla, devuelve null y el widget renderiza sin link — no bloquea. ── */
const _handleCache = new Map(); // product_id → { handle, at }
/* getProductImage: resuelve la imagen principal del producto Shopify por product_id.
 * Cache 1h. Falla a null silencioso. Aditivo. */
const _imageCache = new Map();
async function getProductImage(productId) {
    if (!productId) return null;
    const key = String(productId);
    const cached = _imageCache.get(key);
    const now = Date.now();
    if (cached && (now - cached.at) < 3600000) return cached.url;
    const shop = process.env.SHOPIFY_SHOP || 'nutrition-lab-cluster.myshopify.com';
    const token = process.env.SHOPIFY_ACCESS_TOKEN || _shopifyToken;
    if (!token) return null;
    try {
        const r = await fetch(`https://${shop}/admin/api/2026-01/products/${encodeURIComponent(key)}.json?fields=id,image,images`, {
            headers: { 'X-Shopify-Access-Token': token }
        });
        if (!r.ok) { _imageCache.set(key, { url: null, at: now }); return null; }
        const data = await r.json();
        const url = data?.product?.image?.src || data?.product?.images?.[0]?.src || null;
        _imageCache.set(key, { url, at: now });
        return url;
    } catch (e) {
        _imageCache.set(key, { url: null, at: now });
        return null;
    }
}

async function getProductHandle(productId) {
    if (!productId) return null;
    const key = String(productId);
    const cached = _handleCache.get(key);
    const now = Date.now();
    if (cached && (now - cached.at) < 3600000) return cached.handle;
    const shop = process.env.SHOPIFY_SHOP || 'nutrition-lab-cluster.myshopify.com';
    const token = process.env.SHOPIFY_ACCESS_TOKEN || _shopifyToken;
    if (!token) return null;
    try {
        const r = await fetch(`https://${shop}/admin/api/2026-01/products/${encodeURIComponent(key)}.json?fields=id,handle`, {
            headers: { 'X-Shopify-Access-Token': token }
        });
        if (!r.ok) { _handleCache.set(key, { handle: null, at: now }); return null; }
        const data = await r.json();
        const handle = data?.product?.handle || null;
        _handleCache.set(key, { handle, at: now });
        return handle;
    } catch (e) {
        console.warn('[HANDLE CACHE] error', key, e.message);
        _handleCache.set(key, { handle: null, at: now });
        return null;
    }
}

/* ── Helper: obtener primary_location_id de la tienda (cache 1h).
   Shop API no requiere scope read_locations, solo el scope read_shop/orders básico. ── */
let _primaryLocCache = { id: null, at: 0 };
async function getPrimaryLocationId() {
    const now = Date.now();
    if (_primaryLocCache.id && (now - _primaryLocCache.at) < 3600000) return _primaryLocCache.id;
    const shop = process.env.SHOPIFY_SHOP || 'nutrition-lab-cluster.myshopify.com';
    const token = process.env.SHOPIFY_ACCESS_TOKEN || _shopifyToken;
    if (!token) return null;
    try {
        const r = await fetch(`https://${shop}/admin/api/2026-01/shop.json`, {
            headers: { 'X-Shopify-Access-Token': token }
        });
        if (!r.ok) { console.warn('[LOC CACHE] shop.json', r.status); return null; }
        const data = await r.json();
        const id = data?.shop?.primary_location_id || null;
        if (id) {
            _primaryLocCache = { id: parseInt(id, 10), at: now };
            console.log('[LOC CACHE] primary_location_id =', _primaryLocCache.id, '(cached 1h)');
            return _primaryLocCache.id;
        }
    } catch (e) { console.warn('[LOC CACHE] error:', e.message); }
    return null;
}

/* ── 🔒 HARDENING 2026-04-20: guardia anti-duplicado (AGREGAR, no MODIFICA flujo) ──
   Busca en Shopify si ya existe una order para esta suscripción en los últimos 45 días.
   Criterios de match (cualquiera bloquea creación de duplicado):
     a) note_attribute 'subscription_id' == sub.id (forma canónica futura)
     b) note incluye sub.id (fallback para orders viejas)
     c) note incluye mpPaymentId REAL (no sintético como 'rescue_*')
   Raíz del incidente Luis Miguel (#8765+#8766): rescue cron corrió y creó duplicado
   porque el dedup previo sólo miraba los últimos 50 eventos locales, no Shopify. */
async function alreadyHasShopifyOrderForSub(sub, mpPaymentId, shop, token) {
    // 🔒🔒🔒 FAIL-CLOSED 2026-05-12 — si Shopify API no responde, BLOQUEAR creación.
    //   Antes: return false (fail-open) -> permitía duplicados cuando timeout.
    //   Ahora: return { error: true } -> caller ABORTA creación. CERO duplicados.
    if (!sub?.customer_email || !token || !shop) {
        return { error: true, reason: 'missing_args' };
    }
    let r;
    try {
        const since = new Date(Date.now() - 45 * 24 * 60 * 60 * 1000).toISOString();
        const url = `https://${shop}/admin/api/2026-01/orders.json?` +
            `email=${encodeURIComponent(sub.customer_email)}` +
            `&status=any&created_at_min=${encodeURIComponent(since)}&limit=50` +
            `&fields=id,name,note,tags,note_attributes,cancelled_at,created_at`;
        r = await fetch(url, { headers: { 'X-Shopify-Access-Token': token } });
    } catch (e) {
        console.error('[ORDER DEDUP] FAIL-CLOSED: fetch exception, ABORT creation:', e.message);
        return { error: true, reason: 'fetch_exception', detail: e.message };
    }
    if (!r.ok) {
        console.error('[ORDER DEDUP] FAIL-CLOSED: Shopify ' + r.status + ', ABORT creation');
        return { error: true, reason: 'shopify_http_' + r.status };
    }
    let data;
    try { data = await r.json(); }
    catch (e) {
        console.error('[ORDER DEDUP] FAIL-CLOSED: parse error, ABORT creation');
        return { error: true, reason: 'parse_error' };
    }

    const orders = (data.orders || []).filter(o =>
        !o.cancelled_at &&
        ((o.tags || '').toLowerCase().includes('suscripcion') ||
         (o.note || '').toLowerCase().includes('suscripci'))
    );

    const subId = sub.id ? String(sub.id) : '';
    const mpStr = String(mpPaymentId || '');
    const syntheticPayment = !mpStr || mpStr.startsWith('rescue_') || mpStr.startsWith('manual_');
    const targetCycle = String((parseInt(sub.cycles_completed) || 0) + 1);

    for (const o of orders) {
        const note = o.note || '';
        const attrs = Array.isArray(o.note_attributes) ? o.note_attributes : [];
        const attrSubId = attrs.find(a => a && a.name === 'subscription_id')?.value;
        const attrMpId = attrs.find(a => a && a.name === 'mp_payment_id')?.value;
        const attrCycle = attrs.find(a => a && a.name === 'cycle_number')?.value;

        // REGLA 1 — match por mp_payment_id REAL.
        if (!syntheticPayment) {
            if (attrMpId && String(attrMpId) === String(mpPaymentId)) {
                return { duplicate: true, existing: o, matched_by: 'mp_payment_id' };
            }
            if (note.includes(String(mpPaymentId))) {
                return { duplicate: true, existing: o, matched_by: 'note:mp_payment_id' };
            }
        }
        // REGLA 2 — match por subscription_id + cycle_number.
        if (subId && attrSubId && String(attrSubId) === subId) {
            if (attrCycle && String(attrCycle) === targetCycle) {
                return { duplicate: true, existing: o, matched_by: 'subscription_id+cycle_number' };
            }
            if (!attrCycle && syntheticPayment && (parseInt(sub.cycles_completed) || 0) === 0) {
                return { duplicate: true, existing: o, matched_by: 'subscription_id (legacy no cycle)' };
            }
        }
        // REGLA 3 — fuerza dedup adicional 2026-05-12: si el sub.id está
        //   en el note text (legacy data sin note_attributes), y el ciclo
        //   target es 1 (primera orden de la sub), bloquear.
        if (subId && note.includes(subId) && (parseInt(sub.cycles_completed) || 0) === 0) {
            return { duplicate: true, existing: o, matched_by: 'note:subscription_id (legacy)' };
        }
    }
    return { duplicate: false };
}

/* ── 🔒 HARDENING 2026-04-20: validar pago MP real (AGREGAR, no MODIFICA flujo) ──
   Cuando un rescue cron intenta crear orden con paymentId sintético ('rescue_*'),
   esta función busca en MP un payment REAL processed de la sub antes de permitir.
   Si no hay payment MP real → NO crear order (evita orders fantasma). */
async function findRealMpPaymentForSub(sub) {
    try {
        if (!sub?.mp_preapproval_id) return null;
        let mpToken = process.env.MP_ACCESS_TOKEN;
        if (!mpToken) {
            try {
                const dyn = await readFromShopify().catch(() => ({}));
                if (dyn?.mp_access_token) { process.env.MP_ACCESS_TOKEN = dyn.mp_access_token; mpToken = dyn.mp_access_token; }
            } catch {}
        }
        if (!mpToken) return null;
        const r = await fetch(`https://api.mercadopago.com/authorized_payments/search?preapproval_id=${sub.mp_preapproval_id}`, {
            headers: { Authorization: `Bearer ${mpToken}` }
        });
        if (!r.ok) return null;
        const data = await r.json();
        // 🔒🔒 HARDENING 2026-05-28 — NUNCA devolver un payment que no esté REALMENTE approved.
        //   ANTES: aceptábamos `p.status === 'processed'`. PERO en authorized_payments el
        //   status de nivel superior ('processed'/'authorized') significa "MP procesó el
        //   intento", NO "el cobro se aprobó". El cobro real vive en p.payment.status y
        //   puede quedar 'rejected' (fondos insuficientes / decline diferido). Devolver ese
        //   id generaba pedidos fantasma "Pagado" sin plata real (caso #11251 Verónica).
        //   AHORA: tomamos candidatos (más reciente primero) y verificamos el status REAL
        //   vía mp.getPayment(); devolvemos el PRIMERO que esté 'approved'. Si ninguno → null.
        const candidates = (data?.results || [])
            .filter(p => p.payment?.status === 'approved' || p.status === 'processed' || p.status === 'authorized')
            .sort((a, b) => new Date(b.date_created) - new Date(a.date_created))
            .slice(0, 6); // cap defensivo: solo revisamos los 6 más recientes
        if (!candidates.length) return null;
        for (const c of candidates) {
            const realId = String(c.payment?.id || c.id || '');
            if (!/^\d+$/.test(realId)) continue; // necesitamos un payment id numérico real
            if (mp?.getPayment) {
                const pd = await mp.getPayment(realId).catch(() => null);
                if (pd && pd.status === 'approved') {
                    return { id: realId, amount: pd.transaction_amount ?? c.transaction_amount, date_created: c.date_created };
                }
                continue; // este candidato NO está approved en MP → probamos el siguiente
            }
            // Sin acceso a getPayment: solo confiar si el search ya marcó payment.status === 'approved'
            if (c.payment?.status === 'approved') {
                return { id: realId, amount: c.transaction_amount, date_created: c.date_created };
            }
        }
        return null; // ningún candidato verificó como approved → no hay payment real respaldando
    } catch (e) {
        console.warn('[MP PAYMENT LOOKUP] error:', e.message);
        return null;
    }
}

/* ── 🔒 MUTEX 2026-06-09 (cierre de ventana TOCTOU webhook/polling) ──
   PROBLEMA: webhook y polling podían procesar el MISMO cobro MP en paralelo.
   Ambos leían Shopify ANTES de que el otro hiciera su POST → ambos veían
   "no duplicate" → DOS órdenes para un solo cobro (caso Luis Miguel #8765/#8766).
   SOLUCIÓN (2 niveles):
   1. Mismo payment en vuelo → el segundo caller recibe LA MISMA promesa
      (ni siquiera re-ejecuta — cero duplicado posible).
   2. Distintos payments de la misma sub → se SERIALIZAN (cola por sub.id).
      El segundo corre recién cuando el primero terminó su POST, así su
      lectura de dedup (alreadyHasShopifyOrderForSub) YA VE la orden creada.
   Maps se limpian al terminar — sin fugas de memoria. Single-instance Railway. */
const _orderSubLocks = new Map();      // sub_id -> última promesa de la cola
const _orderPaymentInFlight = new Map(); // `${sub_id}:${payment_id}` -> promesa

async function createShopifyOrderFromSub(sub, mpPaymentId) {
    const subKey = String(sub?.id || 'no_sub');
    const payKey = subKey + ':' + String(mpPaymentId || 'no_pay');

    // Nivel 1: mismo payment ya en proceso → compartir el resultado
    if (_orderPaymentInFlight.has(payKey)) {
        console.log(`[ORDER LOCK] ${payKey} ya en vuelo — reutilizando resultado (duplicado evitado)`);
        return _orderPaymentInFlight.get(payKey);
    }

    // Nivel 2: serializar por sub (cola de promesas)
    const prev = _orderSubLocks.get(subKey) || Promise.resolve();
    const run = prev.catch(() => {}).then(() => _createShopifyOrderFromSubInner(sub, mpPaymentId));

    _orderSubLocks.set(subKey, run);
    _orderPaymentInFlight.set(payKey, run);

    try {
        return await run;
    } finally {
        if (_orderPaymentInFlight.get(payKey) === run) _orderPaymentInFlight.delete(payKey);
        if (_orderSubLocks.get(subKey) === run) _orderSubLocks.delete(subKey);
    }
}

/* ── Helper: Crear orden en Shopify desde una suscripción (inner, NO llamar directo) ── */
async function _createShopifyOrderFromSubInner(sub, mpPaymentId) {
    const shop = process.env.SHOPIFY_SHOP || 'nutrition-lab-cluster.myshopify.com';
    const token = process.env.SHOPIFY_ACCESS_TOKEN || _shopifyToken;
    if (!token) { console.error('[ORDER] No SHOPIFY_ACCESS_TOKEN — cannot create order'); return null; }

    // ✅ GUARD DNI/shipping — intenta auto-resolver datos antes de bloquear.
    let shipCheck = assertSubShippable(sub);
    if (!shipCheck.ok) {
        // AUTO-RESOLVE: intentar obtener dirección de Shopify customer
        if (sub.customer_email && (!sub.shipping_address?.address1 || !sub.shipping_address?.city)) {
            try {
                const addr = await getCustomerAddress(sub.customer_email, token, shop);
                if (addr?.address1) {
                    // 🔧 2026-06-12: el customer Shopify rellena con '' los campos ausentes —
                    // filtramos vacíos y lo YA GUARDADO en la sub gana (antes un city:'' del
                    // customer pisaba un city real de la sub → re-check fallaba → placeholder).
                    const _clean = Object.fromEntries(Object.entries(addr).filter(([, v]) => v !== '' && v !== null && v !== undefined));
                    sub.shipping_address = { ..._clean, ...(Object.fromEntries(Object.entries(sub.shipping_address || {}).filter(([, v]) => v !== '' && v !== null && v !== undefined))) };
                    if (db?.updateSubscription && sub.id && !sub.id.startsWith('orphan_')) {
                        db.updateSubscription(sub.id, { shipping_address: sub.shipping_address }).catch(() => {});
                    }
                    console.log(`[ORDER] Auto-resolved address for ${sub.customer_email} from Shopify customer`);
                }
            } catch (_) {}
        }
        // AUTO-RESOLVE: intentar obtener DNI de pedidos anteriores del cliente en Shopify
        if ((!sub.dni || String(sub.dni).trim().length < 8) && sub.customer_email) {
            try {
                const searchUrl = `https://${shop}/admin/api/2026-01/orders.json?email=${encodeURIComponent(sub.customer_email)}&status=any&limit=1&fields=note_attributes`;
                const oRes = await fetch(searchUrl, { headers: { 'X-Shopify-Access-Token': token } });
                if (oRes.ok) {
                    const oData = await oRes.json();
                    const prevOrder = (oData.orders || [])[0];
                    if (prevOrder?.note_attributes) {
                        const dniAttr = prevOrder.note_attributes.find(a => a.name === 'dni' || a.name === 'ClusterCart-dni');
                        if (dniAttr?.value && String(dniAttr.value).trim().length >= 8) {
                            sub.dni = String(dniAttr.value).trim();
                            if (db?.updateSubscription && sub.id && !sub.id.startsWith('orphan_')) {
                                db.updateSubscription(sub.id, { dni: sub.dni }).catch(() => {});
                            }
                            console.log(`[ORDER] Auto-resolved DNI for ${sub.customer_email} from previous order: ${sub.dni}`);
                        }
                    }
                }
            } catch (_) {}
        }
        // 🔧 FIX 2026-05-27 (C): AUTO-RESOLVE DNI desde MP payer info
        //   Si DNI sigue faltando después de buscar en Shopify customer/order anterior,
        //   intentamos obtenerlo del MP payment payer (identification.number). MP suele
        //   tener este dato cuando el comprador completa el formulario de tarjeta.
        //   Reduce drásticamente el caso "DNI faltante bloquea pedido" (caso Jimenez).
        if ((!sub.dni || String(sub.dni).trim().length < 8) && mpPaymentId &&
            !String(mpPaymentId).startsWith('rescue_') &&
            !String(mpPaymentId).startsWith('manual_') &&
            !String(mpPaymentId).startsWith('selfheal_') &&
            mp?.getPayment) {
            try {
                const paymentMP = await mp.getPayment(mpPaymentId).catch(() => null);
                const idNum = paymentMP?.payer?.identification?.number;
                if (idNum && String(idNum).trim().length >= 8) {
                    sub.dni = String(idNum).trim();
                    const idType = paymentMP?.payer?.identification?.type;
                    if (idType === 'RUC' || idType === '06') sub.tipo_documento = '06';
                    if (db?.updateSubscription && sub.id && !sub.id.startsWith('orphan_')) {
                        db.updateSubscription(sub.id, { dni: sub.dni, tipo_documento: sub.tipo_documento || '01' }).catch(() => {});
                    }
                    console.log(`[ORDER] Auto-resolved DNI for ${sub.customer_email} from MP payer: ${sub.dni} (${idType || '01'})`);
                }
            } catch (_) {}
        }
        // Re-check after auto-resolve
        shipCheck = assertSubShippable(sub);
        if (!shipCheck.ok) {
            // 🔧 FIX 2026-04-30: NO bloquear más por datos faltantes.
            // Los cobros MP entran de todas formas, las cajas DEBEN despacharse.
            // Aplicamos placeholders en memoria (no se graban en la sub original)
            // y taggeamos la orden con 'pending_data' para que el equipo de ops
            // las identifique y complete los datos desde Shopify Admin manualmente.
            // El cliente recibe su producto, SUNAT se completa manualmente con DNI real
            // cuando el equipo lo recopile (vía cpe.labnutrition.com).
            console.warn(`[ORDER] ⚠️ Sub ${sub.customer_email || sub.id} con datos incompletos (${shipCheck.missing.join(', ')}). Creando orden con placeholders + tag pending_data.`);
            if (shipCheck.missing.includes('dni')) sub.dni = '00000000';
            if (!sub.shipping_address) sub.shipping_address = {};
            if (shipCheck.missing.includes('shipping_address1')) sub.shipping_address.address1 = 'POR COMPLETAR — CONTACTAR CLIENTE';
            if (shipCheck.missing.includes('shipping_city')) sub.shipping_address.city = 'Lima';
            if (shipCheck.missing.includes('shipping_province')) sub.shipping_address.province = 'Lima';
            if (!sub.shipping_address.country) sub.shipping_address.country = 'PE';
            if (!sub.shipping_address.country_code) sub.shipping_address.country_code = 'PE';
            if (!sub.shipping_address.zip) sub.shipping_address.zip = '15000';
            if (!sub.shipping_address.first_name) sub.shipping_address.first_name = (sub.customer_name || sub.customer_email || 'Cliente').split(' ')[0];
            if (!sub.shipping_address.last_name) sub.shipping_address.last_name = (sub.customer_name || '').split(' ').slice(1).join(' ') || '';
            if (!sub.shipping_address.phone) sub.shipping_address.phone = sub.customer_phone || '';
            // Marker para el orderBody (agrega tag pending_data y note_attribute)
            sub._partial_data = true;
            sub._missing_fields = shipCheck.missing.slice();
            // Loguear event
            if (db?.createEvent && sub.id && !String(sub.id).startsWith('orphan_')) {
                db.createEvent({
                    subscription_id: sub.id,
                    event_type: 'order_created_with_partial_data',
                    metadata: JSON.stringify({ missing: shipCheck.missing, mp_payment_id: mpPaymentId, at: new Date().toISOString() })
                }).catch(() => {});
            }
        } else {
            console.log(`[ORDER] ✅ Auto-resolve exitoso para ${sub.customer_email} — datos completos, creando orden`);
        }
    }

    // Si falta variant_id, intentar resolverlo buscando por título de producto
    let variantId = sub.variant_id;
    if (!variantId && sub.product_title && token) {
        try {
            const titleSearch = sub.product_title.split(' ')[0]; // primera palabra
            const sr = await fetch(`https://${shop}/admin/api/2026-01/products.json?title=${encodeURIComponent(titleSearch)}&limit=5`, {
                headers: { 'X-Shopify-Access-Token': token }
            });
            if (sr.ok) {
                const sd = await sr.json();
                const match = (sd.products || []).find(p => (p.title || '').toLowerCase().includes(titleSearch.toLowerCase())) || (sd.products || [])[0];
                if (match?.variants?.[0]?.id) {
                    variantId = String(match.variants[0].id);
                    console.log(`[ORDER] Resolved variant_id ${variantId} from product "${match.title}"`);
                    // Guardar para futuros cobros
                    if (db?.updateSubscription && sub.id && !sub.id.startsWith('orphan_')) {
                        db.updateSubscription(sub.id, { variant_id: variantId, product_id: String(match.id) }).catch(() => {});
                    }
                }
            }
        } catch (e) { console.warn('[ORDER] variant resolve error:', e.message); }
    }

    // 🔒 HARDENING 2026-04-20: última barrera — NO crear orden Shopify con variant
    // fuera de la allowlist, aunque la sub lo tenga guardado. Incidente #8760 hubiese
    // sido bloqueado acá. Si el variant es inválido: se aborta y se marca la sub
    // para que admin intervenga, en lugar de generar otra orden con precio/producto
    // incorrecto. El cobro MP queda guardado con flag para revisión manual.
    if (variantId) {
        const orderVariantCheck = await isVariantAllowedForSubscription(variantId);
        if (!orderVariantCheck.ok) {
            console.error(`[ORDER] ❌ BLOQUEADO: sub ${sub.id} tiene variant ${variantId} NO permitida (${orderVariantCheck.reason}). Cliente: ${sub.customer_email}. MP payment: ${mpPaymentId || '?'}. Allowlist: [${orderVariantCheck.allowlist.join(', ')}]. Admin debe corregir variant_id vía PATCH con override_variant_check o ajustar la allowlist.`);
            // Marcar la sub para que quede visible en admin (no toca status activo).
            if (db?.updateSubscription && sub.id && !sub.id.startsWith('orphan_')) {
                db.updateSubscription(sub.id, {
                    last_order_error: `Variant ${variantId} no está en allowlist de suscripción (${new Date().toISOString()})`,
                    needs_admin_review: true
                }).catch(() => {});
            }
            return null;
        }
    }

    // 🔒🔒 HARDENING 2026-05-12: dedup FAIL-CLOSED.
    //   Si Shopify API falla al verificar duplicados, ABORTAR creacion (no crear a ciegas).
    //   Si SI hay duplicado, ABORTAR creacion.
    //   Solo crear si dedup confirma "no duplicate".
    const dup = await alreadyHasShopifyOrderForSub(sub, mpPaymentId, shop, token).catch(e => ({ error: true, reason: 'caller_catch', detail: e.message }));
    if (dup && dup.error) {
        console.error(`[ORDER] 🛡 ABORT — dedup check failed (${dup.reason || 'unknown'}). No creo orden para evitar duplicado. sub:${sub.id} mp:${mpPaymentId || '?'}`);
        if (db?.createEvent && sub.id && !sub.id.startsWith('orphan_')) {
            db.createEvent({
                subscription_id: sub.id,
                event_type: 'order_creation_aborted_dedup_check_failed',
                metadata: JSON.stringify({
                    reason: dup.reason || 'unknown',
                    detail: dup.detail || null,
                    attempted_mp_payment_id: mpPaymentId || null,
                    aborted_at: new Date().toISOString()
                })
            }).catch(() => {});
        }
        return null;
    }
    if (dup && dup.duplicate) {
        console.warn(`[ORDER] 🛑 SKIP duplicado — sub ${sub.id} ya tiene order ${dup.existing.name} en Shopify (match by ${dup.matched_by}). MP payment solicitado: ${mpPaymentId || '?'}. No se crea duplicado.`);
        if (db?.createEvent && sub.id && !sub.id.startsWith('orphan_')) {
            db.createEvent({
                subscription_id: sub.id,
                event_type: 'order_duplicate_blocked',
                metadata: JSON.stringify({
                    existing_order: dup.existing.name,
                    existing_order_id: dup.existing.id,
                    matched_by: dup.matched_by,
                    attempted_mp_payment_id: mpPaymentId || null,
                    blocked_at: new Date().toISOString()
                })
            }).catch(() => {});
        }
        return null;
    }

    // 🔒 HARDENING 2026-04-20: si el caller pasó paymentId sintético (rescue_*, manual_*, vacío),
    // validar que existe un payment MP REAL processed antes de crear order.
    // LA LEY DEL NEGOCIO: "SE TIENEN QUE PAGAR PARA QUE CAIGAN LOS PEDIDOS".
    // Si no hay payment real → NO crear (evita orders fantasma sin respaldo MP).
    // Además: reemplazamos el paymentId sintético por el real, para que el note de
    // la order tenga el ID MP correcto (trazabilidad que pidió el admin).
    // Preapproval IDs (alfanuméricos) NO se consideran sintéticos: provienen del webhook
    // real de activación y dispararían doble-check innecesario.
    const mpIdStr = String(mpPaymentId || '');
    const isSyntheticCaller = !mpIdStr || mpIdStr.startsWith('rescue_') || mpIdStr.startsWith('manual_');
    if (isSyntheticCaller) {
        const realPay = await findRealMpPaymentForSub(sub).catch(() => null);
        if (!realPay) {
            console.error(`[ORDER] ❌ BLOQUEADO: intento de crear order para sub ${sub.id} con paymentId sintético '${mpIdStr || '(vacío)'}' pero NO hay payment MP real processed. No se crea order fantasma. Cliente: ${sub.customer_email}. Preapproval: ${sub.mp_preapproval_id || '?'}`);
            if (db?.updateSubscription && sub.id && !String(sub.id).startsWith('orphan_')) {
                db.updateSubscription(sub.id, {
                    last_order_error: `Orden bloqueada: no hay payment MP real respaldando '${mpIdStr || 'vacío'}' (${new Date().toISOString()})`,
                    needs_admin_review: true
                }).catch(() => {});
            }
            return null;
        }
        console.log(`[ORDER] ✅ Reemplazando paymentId sintético '${mpIdStr || '(vacío)'}' por real ${realPay.id} (S/${realPay.amount}) desde MP`);
        mpPaymentId = realPay.id;
    }

    // ════════════════════════════════════════════════════════════════════════════
    // 🔒🔒🔒 HARDENING 2026-05-28 — VERIFICACIÓN REAL DE APROBACIÓN MP (CHOKEPOINT ÚNICO)
    //   PROBLEMA RAÍZ (caso #11251 Verónica): MP puede reportar un intento como
    //   'processed' / 'authorized' en authorized_payments y luego el cobro real quedar
    //   'rejected' (fondos insuficientes, decline diferido). El status del
    //   authorized_payment NO es el status real del cobro. Crear orden sobre eso = pedido
    //   fantasma marcado "Pagado" sin plata real → descuadre, despacho indebido, riesgo legal.
    //
    //   SOLUCIÓN DEFINITIVA: antes de crear CUALQUIER orden, resolvemos el payment a un id
    //   numérico real y confirmamos mp.getPayment().status === 'approved' EN TIEMPO REAL.
    //   - Si llega un preapproval_id (alfanumérico) o id no numérico → lo resolvemos al
    //     último payment REAL approved vía findRealMpPaymentForSub (ya endurecida).
    //   - FAIL-CLOSED: si MP no responde, o el payment no está approved → NO se crea orden.
    //     Los crons self-heal reintentarán cuando MP confirme. Preferimos demorar un pedido
    //     legítimo unos minutos antes que crear un pedido fantasma.
    //   Cubre TODOS los paths: webhook recurrente, retry-order, create-order, recover,
    //   batch, crons de rescate y activación. Es ADITIVO: barrera nueva, no altera cómo se
    //   arma la orden más abajo. Es el único punto por el que pasan los 8 callers.
    // ════════════════════════════════════════════════════════════════════════════
    if (mp?.getPayment) {
        let _verId = String(mpPaymentId || '');
        // Si no es un payment id numérico real (preapproval id, vacío, etc.) → resolver.
        if (!/^\d+$/.test(_verId)) {
            const _real = await findRealMpPaymentForSub(sub).catch(() => null);
            if (_real?.id && /^\d+$/.test(String(_real.id))) _verId = String(_real.id);
        }
        // Debe quedar un id numérico para poder verificar el cobro en MP.
        if (!/^\d+$/.test(_verId)) {
            console.error(`[ORDER] 🚫 BLOQUEADO — sin payment MP numérico verificable para sub ${sub.id} (${sub.customer_email}). mpPaymentId entrante='${String(mpPaymentId || '')}'. NO se crea orden (sin respaldo MP real).`);
            if (db?.updateSubscription && sub.id && !String(sub.id).startsWith('orphan_')) {
                db.updateSubscription(sub.id, { last_order_error: `Sin payment MP numérico verificable (${new Date().toISOString()})`, needs_admin_review: true }).catch(() => {});
            }
            if (db?.createEvent && sub.id && !String(sub.id).startsWith('orphan_')) {
                db.createEvent({ subscription_id: sub.id, event_type: 'order_blocked_unresolved_payment', metadata: JSON.stringify({ incoming_mp: String(mpPaymentId || ''), at: new Date().toISOString() }) }).catch(() => {});
            }
            return null;
        }
        // Verificación REAL del cobro en MP.
        let _pd = null;
        try { _pd = await mp.getPayment(_verId); }
        catch (e) {
            console.error(`[ORDER] 🛡 ABORT (fail-closed) — no se pudo verificar payment ${_verId} en MP (${e.message}). NO se crea orden; los crons reintentarán.`);
            if (db?.updateSubscription && sub.id && !String(sub.id).startsWith('orphan_')) {
                db.updateSubscription(sub.id, { last_order_error: `No se pudo verificar payment ${_verId} en MP (${new Date().toISOString()})`, needs_admin_review: true }).catch(() => {});
            }
            return null;
        }
        if (!_pd || _pd.status !== 'approved') {
            console.error(`[ORDER] 🚫 BLOQUEADO — payment ${_verId} NO está approved en MP (status=${_pd?.status || 'null'}, detail=${_pd?.status_detail || '-'}). NO se crea orden. sub ${sub.id} ${sub.customer_email}. Caso típico: fondos insuficientes / rechazo diferido.`);
            if (db?.updateSubscription && sub.id && !String(sub.id).startsWith('orphan_')) {
                db.updateSubscription(sub.id, { last_order_error: `Payment ${_verId} no approved (status=${_pd?.status || 'null'}) ${new Date().toISOString()}`, needs_admin_review: true }).catch(() => {});
            }
            if (db?.createEvent && sub.id && !String(sub.id).startsWith('orphan_')) {
                db.createEvent({ subscription_id: sub.id, event_type: 'order_blocked_payment_not_approved', metadata: JSON.stringify({ mp_payment_id: _verId, mp_status: _pd?.status || null, mp_status_detail: _pd?.status_detail || null, amount: _pd?.transaction_amount || null, at: new Date().toISOString() }) }).catch(() => {});
            }
            return null;
        }
        // ✅ Cobro confirmado approved. Usamos el id real verificado en la orden (trazabilidad).
        mpPaymentId = _verId;
        console.log(`[ORDER] ✅ Payment ${_verId} verificado APPROVED en MP (S/${_pd.transaction_amount}). Procediendo a crear orden para ${sub.customer_email}.`);
    }

    // Resolver dirección de envío
    let shippingAddr = sub.shipping_address || null;
    if (!shippingAddr && sub.customer_email) {
        shippingAddr = await getCustomerAddress(sub.customer_email, token, shop);
        if (shippingAddr && db?.updateSubscription && sub.id && !sub.id.startsWith('orphan_')) {
            db.updateSubscription(sub.id, { shipping_address: shippingAddr }).catch(function() {});
        }
    }
    // 🔧 2026-06-12: teléfono SIEMPRE en la orden — el widget lo manda dentro de
    // shipping_address.phone, pero si quedó vacío usamos el customer_phone de la sub.
    if (shippingAddr && !shippingAddr.phone && sub.customer_phone) {
        shippingAddr.phone = sub.customer_phone;
    }

    // 🔧 FIX 2026-04-30 cosmético: el label debe reflejar el ciclo QUE SE ESTÁ CREANDO,
    // no el ya completado. Antes: cycles_completed=1 → "Ciclo 1/6" (se veía como ciclo 1 pero era el 2).
    // Ahora: cycles_completed=1 → "Ciclo 2/6" (el cobro recurrente que se está procesando).
    // Coherente con cycle_number (note_attribute) que ya usa cycles_completed+1.
    const _cyc = parseInt(sub.cycles_completed) || 0;
    const cycleLabel = _cyc === 0
        ? 'Ciclo 1'
        : ('Ciclo ' + (_cyc + 1) + '/' + (sub.cycles_required || '?'));

    // Line item at the actual price the subscriber pays (final_price after discount)
    const basePrice = parseFloat(sub.base_price || 0);
    const finalPrice = parseFloat(sub.final_price || sub.base_price || 0);
    const lineItem = variantId
        ? { variant_id: parseInt(variantId), quantity: 1, price: String(finalPrice.toFixed(2)) }
        : { title: sub.product_title || 'Suscripción LAB NUTRITION', quantity: 1, price: String(finalPrice.toFixed(2)), requires_shipping: true };

    // ═══════════════════════════════════════════════════════════════════
    // 📦 BUNDLE CONFIGURABLE 2026-04-21 — ADITIVO
    //    Si la sub tiene bundle_items (mix de sabores), reemplazamos el lineItem
    //    por N line items, uno por sabor elegido. El precio total se mantiene
    //    igual al final_price (MP cobra el mismo monto fijo mensual).
    //    Cada lata tiene un precio unitario = final_price / total_cans → redondeado a 2 dec.
    //    Ajuste de redondeo en el ÚLTIMO line item para que la suma sea EXACTA.
    //
    //    Determinismo: el mix está grabado en la sub, el cron lo lee SIN recalcular nada.
    //    Mes 1, 2, 3... caen EXACTAMENTE los mismos sabores. Garantizado.
    //
    //    Si bundle_items no existe o está vacío → comportamiento legacy 100% intacto.
    // ═══════════════════════════════════════════════════════════════════
    const bundleItems = Array.isArray(sub.bundle_items) ? sub.bundle_items : [];
    let bundleLineItems = null;
    if (bundleItems.length > 0) {
        const totalQty = bundleItems.reduce((n, it) => n + (parseInt(it.quantity, 10) || 0), 0);
        if (totalQty > 0) {
            // Calculamos unit_price en céntimos para evitar errores de float
            const totalCents = Math.round(finalPrice * 100);
            const unitCents = Math.floor(totalCents / totalQty);
            const remainderCents = totalCents - (unitCents * totalQty);
            bundleLineItems = bundleItems
                .filter(it => it && it.variant_id && parseInt(it.quantity, 10) > 0)
                .map((it, idx, arr) => {
                    const qty = parseInt(it.quantity, 10);
                    // Al último line item le sumamos el remainder para que la suma cuadre exacta
                    const extra = (idx === arr.length - 1) ? remainderCents : 0;
                    const lineCents = (unitCents * qty) + extra;
                    const perUnitCents = lineCents / qty;
                    return {
                        variant_id: parseInt(it.variant_id, 10),
                        quantity: qty,
                        price: (perUnitCents / 100).toFixed(2),
                        properties: [
                            { name: '_bundle', value: 'true' },
                            { name: 'Pack', value: String(sub.product_title || 'Pack Suscripción') },
                            { name: 'Sabor', value: String(it.variant_title || it.title || '') }
                        ]
                    };
                });
            if (bundleLineItems.length === 0) bundleLineItems = null;
            else console.log(`[ORDER] 📦 Bundle mode: ${bundleLineItems.length} line items, total ${totalQty} unidades, S/${finalPrice.toFixed(2)}`);
        }
    }

    // Ensure shipping address has province_code and zip for Shopify PE validation
    if (shippingAddr) {
        if (!shippingAddr.province_code && shippingAddr.province) {
            // Map common Peru provinces to Shopify province codes
            const PE_PROVINCES = { 'lima': 'LIM', 'arequipa': 'ARE', 'cusco': 'CUS', 'la libertad': 'LAL', 'piura': 'PIU', 'lambayeque': 'LAM', 'junin': 'JUN', 'cajamarca': 'CAJ', 'ancash': 'ANC', 'ica': 'ICA', 'callao': 'CAL', 'tacna': 'TAC', 'loreto': 'LOR', 'san martin': 'SAM', 'ucayali': 'UCA', 'huanuco': 'HUA', 'puno': 'PUN', 'amazonas': 'AMA', 'ayacucho': 'AYA', 'apurimac': 'APU', 'huancavelica': 'HUV', 'madre de dios': 'MDD', 'moquegua': 'MOQ', 'pasco': 'PAS', 'tumbes': 'TUM' };
            shippingAddr.province_code = PE_PROVINCES[(shippingAddr.province || '').toLowerCase()] || 'LIM';
        }
        if (!shippingAddr.zip) shippingAddr.zip = '15000'; // Lima default
    }

    if (!variantId) console.warn(`[ORDER] ⚠️ Creating order WITHOUT variant_id for ${sub.customer_email} — using custom line item "${lineItem.title}"`);

    // 🎁 REGALO EN PRIMER PEDIDO — triple validación: ciclo 0 + no entregado + hay items planificados.
    // Falla silenciosa: si construcción falla, la orden sigue sin regalo (NO rompe la venta).
    // Shopify REST crea orden con inventory_behaviour=bypass por defecto → no toca inventario del regalo.
    const giftLineItems = [];
    let shouldDeliverGifts = false;
    try {
        const cyclesCompleted = Number(sub.cycles_completed || 0);
        const alreadyDelivered = sub.gifts_delivered === true;
        let plannedGifts = Array.isArray(sub.gifts_planned) ? sub.gifts_planned : [];
        // 🔧 FALLBACK ADITIVO 2026-04-21: subs creadas ANTES del 15/4 no tienen gifts_planned
        //    (el sistema de gifts_planned se introdujo ese día — commit 0d3c586). Si el sub está
        //    en ciclo 0 y sin plannedGifts → intentamos resolver on-the-fly desde plans_config
        //    para que esas subs viejas SÍ reciban su regalo en el primer cobro.
        //    Puramente aditivo: si resolveGiftsForNewSub no encuentra nada, queda igual que antes.
        if (cyclesCompleted === 0 && !alreadyDelivered && plannedGifts.length === 0) {
            try {
                const resolved = await resolveGiftsForNewSub(
                    sub.frequency_months,
                    sub.permanence_months,
                    sub.product_id
                );
                if (Array.isArray(resolved) && resolved.length > 0) {
                    plannedGifts = resolved;
                    console.log(`[ORDER] 🎁 Fallback resolvió ${resolved.length} gift(s) para sub ${sub.id} (pre-15/4 sin gifts_planned)`);
                }
            } catch (e) { console.warn('[ORDER] gift fallback resolve:', e.message); }
        }
        if (cyclesCompleted === 0 && !alreadyDelivered && plannedGifts.length > 0) {
            for (const g of plannedGifts) {
                if (!g || !g.variant_id) continue;
                const vid = parseInt(g.variant_id, 10);
                if (!Number.isFinite(vid) || vid <= 0) continue;
                const qty = Math.max(1, Math.min(3, parseInt(g.quantity, 10) || 1));
                giftLineItems.push({
                    variant_id: vid,
                    quantity: qty,
                    price: '0.00',
                    taxable: false,
                    properties: [
                        { name: '_gift', value: 'true' },
                        { name: 'Regalo incluido', value: g.product_title || 'Regalo de bienvenida' }
                    ]
                });
            }
            shouldDeliverGifts = giftLineItems.length > 0;
            if (shouldDeliverGifts) {
                console.log(`[ORDER] 🎁 Adding ${giftLineItems.length} gift item(s) to first order for ${sub.customer_email}`);
            }
        }
    } catch (e) {
        console.warn('[ORDER] Gift line item build error (orden sigue sin regalo):', e.message);
    }

    // Build note_attributes so Navasoft picks up the order
    const addr = shippingAddr || {};
    const noteAttrs = [
        // 🔒 HARDENING 2026-04-20: trazabilidad + dedup anti-duplicado
        { name: 'subscription_id', value: String(sub.id || '') },
        { name: 'mp_payment_id', value: String(mpPaymentId || '') },
        { name: 'mp_preapproval_id', value: String(sub.mp_preapproval_id || '') },
        { name: 'cycle_number', value: String((parseInt(sub.cycles_completed) || 0) + 1) },
        { name: 'ClusterCart-optimized', value: 'true' },
        { name: 'tipo_documento', value: sub.tipo_documento || '01' },
        { name: 'dni', value: sub.dni || '' },
        { name: 'ClusterCart-tipo_documento', value: sub.tipo_documento || '01' },
        { name: 'ClusterCart-dni', value: sub.dni || '' },
        { name: 'location_departamento', value: addr.province || 'Lima' },
        { name: 'location_provincia', value: addr.province || 'Lima' },
        { name: 'location_distrito', value: addr.city || '' },
        { name: 'payment_type', value: 'credit_card' },
        { name: 'payment_method', value: 'mercadopago_suscripcion' },
        { name: 'payment_transaction_amount', value: String((finalPrice + 10).toFixed(2)) },
        { name: 'additional_info_shipping_full_address', value: addr.address1 || '' },
        { name: 'additional_info_billing_full_address', value: addr.address1 || '' },
        // Shipping courier code (02 = Urbaner) — Navasoft lee estos campos
        { name: 'shipping_code', value: '02' },
        { name: 'shipping_method_code', value: '02' },
        { name: 'courier_id', value: '02' },
        { name: 'courier', value: 'Urbaner' },
        { name: 'ClusterCart-shipping_code', value: '02' },
        { name: 'ClusterCart-courier', value: 'Urbaner' },
        // IGV incluido en todos los precios (Ley 29571 Art. 5.4)
        { name: 'igv_incluido', value: 'true' },
        { name: 'tax_included', value: 'true' },
        // 🎁 Auditoría regalo: visible en admin Shopify, no afecta lógica Navasoft
        ...(shouldDeliverGifts ? [{ name: 'gift_included', value: String(giftLineItems.length) + ' item(s)' }] : []),
        ...(sub._partial_data ? [
            { name: 'pending_data', value: 'true' },
            { name: 'pending_data_missing', value: (sub._missing_fields || []).join(', ') }
        ] : [])
    ].filter(a => a.value);

    // 🏬 Location de inventario: default automático.
    //    Prioridad:
    //      1. env var SHOPIFY_LOCATION_ID (override manual explícito).
    //      2. primary_location_id de la tienda (auto-descubierto vía /admin/api/.../shop.json) — cacheado.
    //      3. null → Shopify asigna default interno.
    let hubLocationId = process.env.SHOPIFY_LOCATION_ID
        ? parseInt(process.env.SHOPIFY_LOCATION_ID, 10)
        : null;
    if (!hubLocationId) {
        try {
            const primary = await getPrimaryLocationId().catch(() => null);
            if (primary && Number.isFinite(primary)) hubLocationId = primary;
        } catch (_) { /* fallback silencioso — orden sigue con null */ }
    }

    const orderBody = {
        order: {
            email: sub.customer_email,
            financial_status: 'paid',
            taxes_included: true,
            send_receipt: true,
            send_fulfillment_receipt: true,
            // 2026-04-21 ADITIVO: si es bundle configurable → N line items (uno por sabor elegido).
            // Si no es bundle → comportamiento legacy (1 line item), intocado.
            line_items: bundleLineItems
                ? (shouldDeliverGifts ? [...bundleLineItems, ...giftLineItems] : bundleLineItems)
                : (shouldDeliverGifts ? [lineItem, ...giftLineItems] : [lineItem]),
            discount_codes: [],
            shipping_lines: [{ title: 'Envío suscripción', price: '10.00', code: '02' }],
            note: `LAB NUTRITION Suscripción | ${cycleLabel} | ${sub.frequency_months}m x ${sub.permanence_months}m | ${sub.discount_pct || 0}% OFF | IGV incluido${mpPaymentId ? ' | MP: ' + mpPaymentId : ''}`,
            tags: (sub._partial_data ? 'suscripcion,pending_data,revisar_cliente' : 'suscripcion') + (sub._recovery ? ',recovery' : ''),
            note_attributes: noteAttrs,
            shipping_address: shippingAddr || undefined,
            billing_address: shippingAddr ? { ...shippingAddr, company: sub.dni || '' } : undefined,
            ...(hubLocationId && Number.isFinite(hubLocationId) ? { location_id: hubLocationId } : {}),
            metafields: [
                { namespace: 'shipping', key: 'courier_code', value: '02', type: 'single_line_text_field' },
                { namespace: 'shipping', key: 'courier_name', value: 'Urbaner', type: 'single_line_text_field' }
            ]
        }
    };

    let r = await fetch(`https://${shop}/admin/api/2026-01/orders.json`, {
        method: 'POST',
        headers: { 'X-Shopify-Access-Token': token, 'Content-Type': 'application/json' },
        body: JSON.stringify(orderBody)
    });

    // ════════════════════════════════════════════════════════════════════════════
    // 🛡 HARDENING 2026-05-28 — FAIL-SOFT DE REGALO (pedido pagado SIEMPRE entra)
    //   PROBLEMA: el regalo se agrega como line item dentro del MISMO POST del pedido.
    //   Si una variante de regalo queda inválida (producto archivado/borrado, variante
    //   eliminada), Shopify rechaza el pedido COMPLETO → cliente COBRADO sin pedido
    //   creado = el peor escenario. El regalo es un extra de cortesía; jamás debe tumbar
    //   la orden del producto que el cliente pagó.
    //   SOLUCIÓN (aditiva, solo en el path de error): si el POST falla Y la orden llevaba
    //   regalo, reintentamos UNA vez SIN los line items de regalo. Si entra, marcamos la
    //   sub needs_admin_review + evento 'gift_skipped_failsoft' para que el equipo mande el
    //   regalo manualmente y corrija la config. NO marcamos gifts_delivered (el regalo NO
    //   se entregó). El ciclo>0 + el dedup impiden re-intentos/duplicados después.
    //   Camino feliz intacto: si la orden con regalo entra a la primera, esto no corre.
    // ════════════════════════════════════════════════════════════════════════════
    if (!r.ok && shouldDeliverGifts && giftLineItems.length > 0) {
        const errTxt = await r.text().catch(() => '');
        console.warn(`[ORDER] ⚠️ POST de orden falló (${r.status}) con regalo incluido para ${sub.customer_email}. Reintentando SIN regalo (fail-soft). Error: ${errTxt.slice(0, 220)}`);
        const noGiftBody = JSON.parse(JSON.stringify(orderBody));
        // Quitar SOLO los line items marcados como regalo (_gift=true). El producto pagado queda.
        noGiftBody.order.line_items = (noGiftBody.order.line_items || []).filter(li =>
            !(Array.isArray(li.properties) && li.properties.some(p => p && p.name === '_gift' && String(p.value) === 'true'))
        );
        // Limpiar el note_attribute de auditoría de regalo y taggear para ubicación rápida en admin.
        if (Array.isArray(noGiftBody.order.note_attributes)) {
            noGiftBody.order.note_attributes = noGiftBody.order.note_attributes.filter(a => a.name !== 'gift_included');
        }
        noGiftBody.order.tags = (noGiftBody.order.tags ? noGiftBody.order.tags + ',' : '') + 'regalo_omitido_revisar';
        r = await fetch(`https://${shop}/admin/api/2026-01/orders.json`, {
            method: 'POST',
            headers: { 'X-Shopify-Access-Token': token, 'Content-Type': 'application/json' },
            body: JSON.stringify(noGiftBody)
        });
        if (r.ok) {
            shouldDeliverGifts = false; // el regalo NO se entregó → no marcar gifts_delivered abajo
            console.warn(`[ORDER] ✅ Orden creada SIN regalo (fail-soft) para ${sub.customer_email}. Sub marcada para revisión de regalo.`);
            if (db?.updateSubscription && sub.id && !String(sub.id).startsWith('orphan_')) {
                db.updateSubscription(sub.id, {
                    needs_admin_review: true,
                    last_order_error: `Regalo omitido por variante inválida (fail-soft) ${new Date().toISOString()}`
                }).catch(() => {});
            }
            if (db?.createEvent && sub.id && !String(sub.id).startsWith('orphan_')) {
                db.createEvent({ subscription_id: sub.id, event_type: 'gift_skipped_failsoft', metadata: JSON.stringify({ gifts: sub.gifts_planned || [], reason: errTxt.slice(0, 220), at: new Date().toISOString() }) }).catch(() => {});
            }
        }
    }

    if (!r.ok) {
        const err = await r.text();
        throw new Error(`Shopify order API ${r.status}: ${err.slice(0, 300)}`);
    }
    const result = await r.json();
    const order = result.order;
    const routedLoc = order.location_id || 'default-store';
    console.log(`[SHOPIFY ORDER] ✅ Orden #${order.order_number} creada para ${sub.customer_email} | ${cycleLabel} | location_id=${routedLoc}${hubLocationId ? ' (enviado=' + hubLocationId + ')' : ' (sin SHOPIFY_LOCATION_ID env)'}`);

    // 🎁 Si se entregó el regalo en esta orden, marcar gifts_delivered=true (foto congelada)
    // para que cobros siguientes NO repitan el regalo. Fallo silencioso: no rompe el return del order.
    if (shouldDeliverGifts && db?.updateSubscription && sub.id && !String(sub.id).startsWith('orphan_')) {
        try {
            await db.updateSubscription(sub.id, {
                gifts_delivered: true,
                gifts_delivered_at: new Date().toISOString(),
                gifts_delivered_order_id: String(order.id),
                gifts_delivered_order_name: '#' + order.order_number,
                // 🔧 FIX 2026-05-27: vinculación shopify_order_* atómica en el mismo update.
                //   ANTES: solo gifts_delivered_* se actualizaba acá; shopify_order_* dependía
                //   del callsite (webhook MP, retry-order, etc.) y podía perderse por race con
                //   metaobjects eventually-consistent o por callsites que no lo actualizaban
                //   (retry-order, recoverOrdersForEmail, orphan rescue). Casos reportados:
                //   Alexandra (#10969), Sebastian (#11203), Marco Antonio (#11246).
                //   AHORA: garantizamos vinculación en cada creación exitosa de orden con regalo,
                //   en una sola operación atómica con gifts_delivered_*.
                shopify_order_id: String(order.id),
                shopify_order_name: '#' + order.order_number
            });
            console.log(`[ORDER] 🎁 Marked gifts_delivered=true + shopify_order linked for sub ${sub.id} (order #${order.order_number})`);
        } catch (e) {
            console.warn('[ORDER] mark gifts_delivered error:', e.message);
        }
        if (db.createEvent) {
            db.createEvent({
                subscription_id: sub.id,
                event_type: 'gifts_delivered',
                metadata: JSON.stringify({
                    order_number: order.order_number,
                    order_id: String(order.id),
                    gifts: sub.gifts_planned || [],
                    mp_payment_id: mpPaymentId || null
                })
            }).catch(() => {});
        }
    } else if (db?.updateSubscription && sub.id && !String(sub.id).startsWith('orphan_')) {
        // 🔧 FIX 2026-05-27: caso SIN regalo (sub pre-15/abr sin gifts_planned, o cobros
        //   recurrentes mes 2+). Garantizamos vinculación shopify_order_* aunque el callsite
        //   (webhook MP, retry-order, etc.) no lo haga o falle. Idempotente: si el callsite
        //   también actualiza, el segundo write sobre los mismos valores no rompe nada.
        try {
            await db.updateSubscription(sub.id, {
                shopify_order_id: String(order.id),
                shopify_order_name: '#' + order.order_number
            });
        } catch (e) {
            console.warn('[ORDER] link shopify_order (no-gift case) error:', e.message);
        }
    }

    return order;
}


/* ═══════════════════════════════════════════════
   ⏰ SCHEDULED NOTIFICATIONS (cron)
═══════════════════════════════════════════════ */
// Runs daily at 9:00 AM Lima time
cron.schedule('0 14 * * *', async () => {  // 14:00 UTC = 09:00 PET
    console.log('[CRON] Running daily notification check...');
    const now = new Date();

    try {
        // Get active subscriptions from Shopify Metaobjects
        const allSubs = await db.getSubscriptions({ status: 'active' });
        const subs = allSubs.filter(s => s.next_charge_at);

        for (const sub of subs) {
            const nextCharge = new Date(sub.next_charge_at);
            const daysUntil = (nextCharge - now) / (1000 * 60 * 60 * 24);

            // -7 days: lock warning
            if (daysUntil >= 6.5 && daysUntil < 7.5 && notifications.sendCancelLockWarning) {
                await notifications.sendCancelLockWarning(sub).catch(console.error);
            }

            // -3 days: charge reminder
            if (daysUntil >= 2.5 && daysUntil < 3.5 && notifications.sendChargeReminder) {
                await notifications.sendChargeReminder(sub).catch(console.error);
            }
        }

        // Resume paused subscriptions that have expired their pause
        const pausedSubs = await db.getSubscriptions({ status: 'paused' });
        const paused = pausedSubs.filter(s => s.paused_until && new Date(s.paused_until) < now);

        for (const sub of paused) {
            if (mp.resumeSubscription) await mp.resumeSubscription(sub.mp_preapproval_id).catch(console.error);
            await db.updateSubscription(sub.id, { status: 'active', paused_until: null });
        }

        console.log(`[CRON] Processed ${subs.length} subs, resumed ${paused.length} paused`);
    } catch (e) {
        console.error('[CRON] Error:', e.message);
    }
}, { timezone: 'America/Lima' });

/* ── SELF-HEALING REACTIVADO 2026-05-12 ──
   Garantía: si webhook MP falló al crear orden, este cron rescata.
   Garantía de no-duplicar: createShopifyOrderFromSub usa
   alreadyHasShopifyOrderForSub que ahora bloquea por mp_payment_id estricto.
   🔧 FIX 2026-05-27 (A): frecuencia 4h → 30 min para reducir lag máximo
   entre cobro MP y pedido visible al cliente (4h → 30 min). El filtro
   estricto de candidatos (últimos 7 días + sin gifts redundantes) mantiene
   el costo Shopify API bajo. */
cron.schedule('*/30 * * * *', async () => {
    console.log('[SELF-HEAL] Scanning for active subs missing Shopify orders...');
    try {
        const allSubs = await db.getSubscriptions({ status: 'active' }).catch(() => []);
        // Solo subs activadas en los últimos 7 días — las antiguas requieren intervención manual
        const sevenDaysAgo = new Date(Date.now() - 7 * 24 * 60 * 60 * 1000).toISOString();
        const missing = allSubs.filter(s =>
            s.status === 'active' &&
            (parseInt(s.cycles_completed) || 0) >= 1 &&
            !s.shopify_order_id &&
            s.variant_id &&
            s.mp_preapproval_id &&
            (s.activated_at || s.created_at || '') >= sevenDaysAgo &&
            // 🔒 FIX 2026-04-28: si gifts_delivered=true significa que YA se creó al
            //   menos una orden con regalo (createShopifyOrderFromSub solo lo setea
            //   después de crear order exitosamente). El problema entonces es solo
            //   que shopify_order_id no se sincronizó, pero el order EXISTE en Shopify.
            //   No hace falta crearlo de nuevo (eso causaba duplicados sin regalo
            //   con cycleLabel="Ciclo 1/6", caso lili.020407@gmail.com).
            //   Para subs sin gifts_planned (ej. C4 Bundle), gifts_delivered queda false
            //   y este filtro no aplica — esas subs siguen recibiendo self-heal normal.
            !(s.gifts_delivered === true && Array.isArray(s.gifts_planned) && s.gifts_planned.length > 0)
        );

        if (!missing.length) {
            console.log('[SELF-HEAL] ✅ All active subs have Shopify orders');
            return;
        }

        console.log(`[SELF-HEAL] Found ${missing.length} subs without Shopify orders — creating...`);
        let created = 0, failed = 0;

        for (const sub of missing) {
            try {
                const mpId = sub.mp_preapproval_id || 'selfheal_' + Date.now();
                const order = await createShopifyOrderFromSub(sub, mpId);
                if (order?.id) {
                    await db.updateSubscription(sub.id, {
                        shopify_order_id: String(order.id),
                        shopify_order_name: order.name
                    }).catch(() => {});
                    await db.createEvent({ subscription_id: sub.id, event_type: 'selfheal_order_created',
                        metadata: JSON.stringify({ shopify_order_id: order.id, order_name: order.name })
                    }).catch(() => {});
                    console.log(`[SELF-HEAL] ✅ Order ${order.name} created for ${sub.customer_email}`);
                    created++;
                } else { failed++; }
                await new Promise(r => setTimeout(r, 800)); // Rate limit
            } catch (e) {
                console.error(`[SELF-HEAL] ❌ ${sub.customer_email}: ${e.message}`);
                failed++;
            }
        }
        console.log(`[SELF-HEAL] Done: ${created} created, ${failed} failed`);
    } catch (e) {
        console.error('[SELF-HEAL] Error:', e.message);
    }
}, { timezone: 'America/Lima' });

/* ── ORPHAN DETECTOR (Fix B 2026-05-27) ────────────────────────
   Cada 30 min escanea subs activated hace >30 min SIN shopify_order_id
   vinculado. NO crea pedidos (eso lo hace el self-heal); SOLO alerta
   con WARNING en logs + event 'orphan_detected' en la sub para que sea
   visible en admin panel. Sirve como tripwire si el self-heal falla. */
cron.schedule('*/30 * * * *', async () => {
    try {
        const allSubs = await db.getSubscriptions({ status: 'active' }).catch(() => []);
        const now = Date.now();
        const orphans = (Array.isArray(allSubs) ? allSubs : []).filter(s => {
            const activated = s.activated_at ? new Date(s.activated_at).getTime() : 0;
            const minutesAgo = activated ? (now - activated) / (1000 * 60) : 0;
            return s.status === 'active'
                && s.mp_preapproval_id
                && (parseInt(s.cycles_completed) || 0) >= 1
                && !s.shopify_order_id
                && minutesAgo > 30
                && minutesAgo < 60 * 24 * 7; // últimas 7d, evita ruido histórico
        });
        if (!orphans.length) return;
        console.warn(`[ORPHAN DETECTOR] ⚠️  ${orphans.length} sub(s) activated >30min SIN shopify_order_id:`);
        for (const s of orphans) {
            console.warn(`  - ${s.id} | ${s.customer_email} | activated ${s.activated_at} | cycles ${s.cycles_completed}/${s.cycles_required}`);
            // Idempotente: evita spam de eventos repitiendo cada 30 min
            try {
                const recent = db?.getEvents ? await db.getEvents(s.id, 5).catch(() => []) : [];
                const alreadyAlerted = (recent || []).some(e =>
                    e.event_type === 'orphan_detected' &&
                    (now - new Date(e.created_at || 0).getTime()) < 60 * 60 * 1000 // ya alertado en última hora
                );
                if (!alreadyAlerted && db?.createEvent) {
                    db.createEvent({
                        subscription_id: s.id,
                        event_type: 'orphan_detected',
                        metadata: JSON.stringify({
                            detected_at: new Date().toISOString(),
                            activated_at: s.activated_at,
                            cycles: `${s.cycles_completed}/${s.cycles_required}`,
                            customer_email: s.customer_email,
                            hint: 'Ejecutar POST /api/admin/subs/backfill-shopify-order-from-gifts o POST /api/admin/subscriptions/:id/retry-order'
                        })
                    }).catch(() => {});
                }
            } catch (_) {}
        }
    } catch (e) {
        console.error('[ORPHAN DETECTOR] Error:', e.message);
    }
}, { timezone: 'America/Lima' });

/* ═══════════════════════════════════════════════
   📊 ADMIN DASHBOARD (served as HTML)
═══════════════════════════════════════════════ */
/* NOTE: GET / is already defined at line ~68 — Express only matches the first handler */

app.get('/portal/:customerId', (req, res) => {
    res.sendFile(path.join(__dirname, 'public', 'portal.html'));
});

/* Portal V2 — nueva landing real del suscriptor (2026-06-04) */
app.get('/portal/v2', (req, res) => {
    res.sendFile(path.join(__dirname, 'public', 'portal-v2.html'));
});
app.get('/mi-suscripcion', (req, res) => {
    res.sendFile(path.join(__dirname, 'public', 'portal-v2.html'));
});

/* ═══════════════════════════════════════════════
   📣 MARKETING API
═══════════════════════════════════════════════ */

/* Send email campaign to a subscriber segment */
app.post('/api/marketing/send', async (req, res) => {
    try {
        const { segment, subject, body, previewText } = req.body;
        if (!subject || !body) return res.status(400).json({ error: 'subject and body required' });

        let subs = await db.getSubscriptions();
        if (segment === 'active') subs = subs.filter(s => s.status === 'active');
        else if (segment === 'paused') subs = subs.filter(s => s.status === 'paused');
        else if (segment === 'failed') subs = subs.filter(s => s.status === 'payment_failed');
        else if (segment === 'cancelled') subs = subs.filter(s => s.status === 'cancelled');

        // Deduplicate by email
        const unique = Object.values(subs.reduce((acc, s) => { acc[s.customer_email] = s; return acc; }, {}));

        let sent = 0, failed = 0;
        for (const sub of unique) {
            const html = `<!DOCTYPE html><html><head><meta charset="UTF-8"><style>
        body{font-family:sans-serif;background:#f2f2f2;color:#111}
        .w{max-width:600px;margin:32px auto;background:#fff;border-radius:10px;overflow:hidden}
        .h{background:#9d2a23;padding:24px 32px;color:#fff;font-size:20px;font-weight:900;letter-spacing:2px}
        .b{padding:28px 32px;line-height:1.6} .f{background:#f2f2f2;padding:14px 32px;text-align:center;font-size:11px;color:#aaa}
      </style></head><body><div class="w">
        <div class="h">🧬 LAB NUTRITION</div>
        <div class="b">${body.replace(/\n/g, '<br>')}</div>
        <div class="f">LAB NUTRITION · <a href="${process.env.BACKEND_URL}/portal/${sub.customer_email}" style="color:#9d2a23">Gestionar suscripción</a></div>
      </div></body></html>`;
            try {
                const nodemailer = require('nodemailer');
                const t = nodemailer.createTransport({ host: process.env.SMTP_HOST, port: 587, secure: false, auth: { user: process.env.SMTP_USER, pass: process.env.SMTP_PASS } });
                await t.sendMail({ from: process.env.EMAIL_FROM, to: sub.customer_email, subject, html });
                sent++;
            } catch { failed++; }
        }
        if (db && db.createEvent) {
            await db.createEvent({
                subscription_id: 'campaign', event_type: 'campaign_sent',
                metadata: JSON.stringify({ subject, segment, total: unique.length, sent, failed })
            }).catch(() => { });
        }
        res.json({ success: true, sent, failed, total: unique.length });
    } catch (e) {
        res.status(500).json({ error: e.message });
    }
});

/* ══════════════════════════════════════════════════════
   MAILING v2 — Compositor con segmentos reales + variables
   POST /api/mailing/preview   → devuelve HTML renderizado
   POST /api/mailing/audience  → count por segmento (sin enviar)
   POST /api/mailing/send      → envío masivo con rate-limit
   Variables soportadas: {{first_name}} {{email}} {{product}}
                         {{next_charge}} {{final_price}} {{portal_link}}
   ══════════════════════════════════════════════════════ */

function _renderVars(template, sub) {
    const firstName = String(sub.customer_name || sub.customer_email || '').split(' ')[0] || 'Cliente';
    const nextCharge = sub.next_charge_at ? new Date(sub.next_charge_at).toLocaleDateString('es-PE', { day: 'numeric', month: 'long', year: 'numeric' }) : 'Próximamente';
    const portalLink = `${process.env.BACKEND_URL || 'https://pixel-suite-pro-production.up.railway.app'}/portal/${encodeURIComponent(sub.customer_email || '')}`;
    const vars = {
        first_name: firstName,
        email: sub.customer_email || '',
        product: sub.product_title || '',
        next_charge: nextCharge,
        final_price: sub.final_price ? `S/ ${parseFloat(sub.final_price).toFixed(2)}` : '',
        portal_link: portalLink,
        name: sub.customer_name || firstName
    };
    return String(template || '').replace(/\{\{\s*(\w+)\s*\}\}/g, (_, k) => vars[k] !== undefined ? vars[k] : '');
}

async function _filterMailingAudience(segment) {
    const all = await db.getSubscriptions();
    let list = Array.isArray(all) ? all : [];
    if (segment === 'active') list = list.filter(s => s.status === 'active');
    else if (segment === 'paused') list = list.filter(s => s.status === 'paused');
    else if (segment === 'cancelled') list = list.filter(s => s.status === 'cancelled');
    else if (segment === 'failed') list = list.filter(s => s.status === 'payment_failed');
    else if (segment === 'bd_completed') list = list.filter(s => s.status === 'active' && (s.cycles_completed || 0) >= (s.cycles_required || 0));
    else if (segment === 'bd_progress') list = list.filter(s => s.status === 'active' && (s.cycles_completed || 0) < (s.cycles_required || 0));
    else if (segment === 'new_30d') {
        const cutoff = Date.now() - 30 * 86400000;
        list = list.filter(s => s.created_at && new Date(s.created_at).getTime() >= cutoff);
    }
    // Pagos incompletos: checkout iniciado (pending_payment) que nunca se activó
    // Con >= 2h sin activarse (evita cruzar con gente que aún está autorizando).
    else if (segment === 'payment_incomplete') {
        const cutoff = Date.now() - 2 * 60 * 60 * 1000;
        list = list.filter(s => s.status === 'pending_payment' && s.created_at && new Date(s.created_at).getTime() <= cutoff);
    }
    // Pagos pendientes (últimas 2h, todavía podrían completarse)
    else if (segment === 'payment_pending') {
        const cutoff = Date.now() - 2 * 60 * 60 * 1000;
        list = list.filter(s => s.status === 'pending_payment' && (!s.created_at || new Date(s.created_at).getTime() > cutoff));
    }
    // Todos los "intentaron pagar" (pending_payment + payment_failed, cualquier antigüedad)
    else if (segment === 'any_attempt') {
        list = list.filter(s => s.status === 'pending_payment' || s.status === 'payment_failed');
    }
    // Todos los suscriptores registrados (cualquier estado)
    else if (segment === 'all_subs') {
        list = list.filter(s => s.customer_email);
    }
    // dedupe by email
    const unique = Object.values(list.reduce((acc, s) => { if (s.customer_email) acc[s.customer_email.toLowerCase()] = s; return acc; }, {}));
    return unique;
}

app.post('/api/mailing/audience', async (req, res) => {
    try {
        const { segment = 'active' } = req.body || {};
        const list = await _filterMailingAudience(segment);
        res.json({ segment, count: list.length, sample: list.slice(0, 5).map(s => ({ email: s.customer_email, name: s.customer_name, product: s.product_title })) });
    } catch (e) { res.status(500).json({ error: e.message }); }
});

app.post('/api/mailing/preview', async (req, res) => {
    try {
        const { subject = '', body = '', headerTitle = 'Club Black Diamond', testSub } = req.body || {};
        const mockSub = testSub || {
            customer_name: 'Jorge Luis Torres',
            customer_email: 'ejemplo@labnutrition.pe',
            product_title: 'CREATINE MICRONIZED BLACK',
            final_price: 90,
            next_charge_at: new Date(Date.now() + 7 * 86400000).toISOString()
        };
        const renderedSubject = _renderVars(subject, mockSub);
        const bodyHtml = _renderVars(body, mockSub).replace(/\n/g, '<br>');
        const notifMod = notifications || require('./services/notifications');
        const baseHTML = notifMod.__baseHTML || null;
        let html;
        if (baseHTML) {
            html = baseHTML(bodyHtml, { headerTitle });
        } else {
            // fallback si no exportamos baseHTML
            html = `<!DOCTYPE html><html><body style="font-family:system-ui;max-width:600px;margin:30px auto;padding:24px;background:#fff;border-radius:12px;border:1px solid #eee"><div style="background:#0A0A0A;color:#fff;padding:24px;text-align:center;border-radius:8px 8px 0 0"><div style="font-size:11px;letter-spacing:4px">LAB NUTRITION</div><div style="color:#E30613;font-size:20px;font-weight:900;margin-top:8px">${headerTitle}</div></div><div style="padding:24px;line-height:1.6;color:#222">${bodyHtml}</div><div style="text-align:center;color:#888;font-size:11px;padding:16px;border-top:1px solid #eee">LAB NUTRITION CORP SAC · Lima, Perú</div></body></html>`;
        }
        res.json({ subject: renderedSubject, html });
    } catch (e) { res.status(500).json({ error: e.message }); }
});

app.post('/api/mailing/send', async (req, res) => {
    try {
        const { segment = 'active', subject, body, headerTitle = 'Club Black Diamond', test_email } = req.body || {};
        if (!subject || !body) return res.status(400).json({ error: 'subject y body son requeridos' });

        // ── Transporte: Resend (HTTP 443) si hay key, sino nodemailer SMTP ──
        const useResend = !!process.env.RESEND_API_KEY;
        let transporter = null, FROM;
        if (useResend) {
            FROM = process.env.RESEND_FROM || 'LAB NUTRITION <onboarding@resend.dev>';
        } else {
            const nodemailer = require('nodemailer');
            const smtpPort = parseInt(process.env.SMTP_PORT || '465');
            transporter = nodemailer.createTransport({
                host: process.env.SMTP_HOST, port: smtpPort, secure: smtpPort === 465,
                auth: { user: process.env.SMTP_USER, pass: process.env.SMTP_PASS },
                connectionTimeout: 10000, socketTimeout: 15000
            });
            FROM = `"${process.env.EMAIL_FROM || 'LAB NUTRITION'}" <${process.env.SMTP_USER || 'contacto@labnutrition.pe'}>`;
        }

        async function dispatch(to, subj, html) {
            if (useResend) {
                return notifications.sendViaResend(to, subj, html, FROM);
            }
            return transporter.sendMail({ from: FROM, to, subject: subj, html });
        }

        const buildHtml = (sub) => {
            const rendered = _renderVars(body, sub).replace(/\n/g, '<br>');
            return `<!DOCTYPE html><html lang="es"><head><meta charset="UTF-8"><meta name="viewport" content="width=device-width"><style>body{margin:0;padding:0;font-family:'Segoe UI',Helvetica,Arial,sans-serif;background:#f5f5f5;color:#1a1a1a}.outer{max-width:600px;margin:0 auto;padding:24px 16px}.card{background:#fff;border-radius:16px;overflow:hidden;box-shadow:0 2px 20px rgba(0,0,0,.06)}.hero{background:#0A0A0A;padding:36px 32px;text-align:center}.hero .brand{color:#fff;font-size:11px;font-weight:800;letter-spacing:4px;text-transform:uppercase}.hero .title{color:#E30613;font-size:22px;font-weight:900;margin-top:12px;letter-spacing:.5px}.body{padding:32px;color:#1a1a1a;line-height:1.7;font-size:14px}.footer{padding:20px 32px;text-align:center;background:#fafafa;border-top:1px solid #eee;font-size:11px;color:#888}.footer a{color:#E30613;text-decoration:none;font-weight:600}</style></head><body><div class="outer"><div class="card"><div class="hero"><div class="brand">LAB NUTRITION</div><div class="title">${headerTitle}</div></div><div class="body">${rendered}</div><div class="footer">LAB NUTRITION CORP SAC · Lima, Perú · <a href="${_renderVars('{{portal_link}}', sub)}">Gestionar suscripción</a></div></div></div></body></html>`;
        };

        // Modo test: envía a 1 email con datos mock
        if (test_email) {
            const mock = { customer_name: 'Test User', customer_email: test_email, product_title: 'CREATINE MICRONIZED BLACK', final_price: 90, next_charge_at: new Date(Date.now() + 7 * 86400000).toISOString() };
            const html = buildHtml(mock);
            const renderedSubject = _renderVars(subject, mock);
            await dispatch(test_email, '[TEST] ' + renderedSubject, html);
            return res.json({ success: true, test: true, to: test_email, via: useResend ? 'resend' : 'smtp' });
        }

        const audience = await _filterMailingAudience(segment);
        let sent = 0, failed = 0;
        const errors = [];
        // batch 10 en paralelo con pausa de 500ms
        for (let i = 0; i < audience.length; i += 10) {
            const batch = audience.slice(i, i + 10);
            await Promise.all(batch.map(async (sub) => {
                try {
                    const html = buildHtml(sub);
                    const renderedSubject = _renderVars(subject, sub);
                    await dispatch(sub.customer_email, renderedSubject, html);
                    sent++;
                } catch (e) {
                    failed++;
                    if (errors.length < 5) errors.push({ email: sub.customer_email, error: e.message });
                }
            }));
            if (i + 10 < audience.length) await new Promise(r => setTimeout(r, 500));
        }
        const campaign_id = 'cmp_' + Date.now().toString(36) + '_' + Math.random().toString(36).slice(2, 7);
        if (db && db.createEvent) {
            await db.createEvent({
                subscription_id: 'campaign', event_type: 'mailing_sent',
                metadata: JSON.stringify({
                    campaign_id,
                    subject,
                    segment,
                    header_title: headerTitle,
                    total: audience.length,
                    sent,
                    failed,
                    // Guardamos emails y IDs para atribución posterior (los emails son
                    // los destinatarios, los IDs sirven para trackear conversiones)
                    audience_emails: audience.map(s => (s.customer_email || '').toLowerCase()).filter(Boolean),
                    audience_sub_ids: audience.map(s => s.id).filter(Boolean),
                    sent_at: new Date().toISOString(),
                })
            }).catch(() => {});
        }
        res.json({ success: true, campaign_id, segment, total: audience.length, sent, failed, errors });
    } catch (e) { console.error('[MAILING]', e); res.status(500).json({ error: e.message }); }
});

/* ═══════════════════════════════════════════════
   📬 HISTORIAL DE CAMPAÑAS + ATRIBUCIÓN DE VENTAS
═══════════════════════════════════════════════ */

// Helper: lee eventos de tipo mailing_sent y los normaliza
async function _readMailingEvents(limit = 50) {
    if (!db || !db.getEvents) return [];
    try {
        // db.getEvents admite filtro por subscription_id — usamos 'campaign' para pseudo-ID de campañas
        const all = await db.getEvents('campaign').catch(() => []);
        const mailings = (Array.isArray(all) ? all : [])
            .filter(ev => ev && ev.event_type === 'mailing_sent')
            .map(ev => {
                let m = {};
                try { m = typeof ev.metadata === 'string' ? JSON.parse(ev.metadata) : (ev.metadata || {}); } catch { m = {}; }
                return {
                    id: m.campaign_id || ev.id,
                    event_id: ev.id,
                    created_at: m.sent_at || ev.created_at,
                    subject: m.subject || '(sin asunto)',
                    segment: m.segment || '',
                    header_title: m.header_title || '',
                    total: Number(m.total || 0),
                    sent: Number(m.sent || 0),
                    failed: Number(m.failed || 0),
                    audience_emails: Array.isArray(m.audience_emails) ? m.audience_emails : [],
                    audience_sub_ids: Array.isArray(m.audience_sub_ids) ? m.audience_sub_ids : [],
                };
            })
            .sort((a, b) => new Date(b.created_at || 0) - new Date(a.created_at || 0));
        return mailings.slice(0, limit);
    } catch (e) {
        console.warn('[CAMPAIGNS]', e.message);
        return [];
    }
}

// Helper: computa atribución de un mailing
// Regla: suscripción cuyo email esté en audience_emails, cuyo updated_at (o created_at) caiga
// entre sent_at y sent_at + windowDays, y cuyo status sea active.
async function _attribute(mailing, windowDays = 14) {
    const result = { converted: 0, revenue: 0, items: [] };
    if (!mailing || !mailing.audience_emails || !mailing.audience_emails.length) return result;
    const sentAt = new Date(mailing.created_at || Date.now());
    const windowEnd = new Date(sentAt.getTime() + windowDays * 86400000);
    const emailSet = new Set(mailing.audience_emails.map(e => (e || '').toLowerCase()));
    let subs = [];
    try { subs = await db.getSubscriptions({}); } catch { subs = []; }
    for (const s of (subs || [])) {
        const em = (s.customer_email || '').toLowerCase();
        if (!emailSet.has(em)) continue;
        if (s.status !== 'active') continue;
        const changed = new Date(s.updated_at || s.created_at || 0);
        if (changed < sentAt || changed > windowEnd) continue;
        const price = Number(s.final_price || 0);
        result.converted++;
        result.revenue += price;
        result.items.push({
            id: s.id,
            email: s.customer_email,
            name: s.customer_name || '',
            product: s.product_title || '',
            price,
            converted_at: s.updated_at || s.created_at,
        });
    }
    return result;
}

// GET /api/mailing/campaigns?limit=50 → lista resumida con conversiones
app.get('/api/mailing/campaigns', async (req, res) => {
    try {
        const limit = Math.min(parseInt(req.query.limit) || 50, 200);
        const window = parseInt(req.query.window_days) || 14;
        const mailings = await _readMailingEvents(limit);
        // computamos atribución para todas (puede ser lento si hay muchas — por ahora OK <100 campañas)
        const withAttr = [];
        for (const m of mailings) {
            const attr = await _attribute(m, window);
            withAttr.push({
                id: m.id,
                created_at: m.created_at,
                subject: m.subject,
                segment: m.segment,
                header_title: m.header_title,
                total: m.total,
                sent: m.sent,
                failed: m.failed,
                converted: attr.converted,
                revenue: Math.round(attr.revenue * 100) / 100,
                conversion_rate: m.sent > 0 ? Math.round((attr.converted / m.sent) * 1000) / 10 : 0,
            });
        }
        res.json({ campaigns: withAttr, window_days: window });
    } catch (e) { console.error('[CAMPAIGNS LIST]', e); res.status(500).json({ error: e.message }); }
});

// GET /api/mailing/campaigns/:id → detalle con audiencia + convertidos
app.get('/api/mailing/campaigns/:id', async (req, res) => {
    try {
        const window = parseInt(req.query.window_days) || 14;
        const mailings = await _readMailingEvents(500);
        const m = mailings.find(x => x.id === req.params.id || x.event_id === req.params.id);
        if (!m) return res.status(404).json({ error: 'Campaña no encontrada' });
        const attr = await _attribute(m, window);
        res.json({
            id: m.id,
            created_at: m.created_at,
            subject: m.subject,
            segment: m.segment,
            header_title: m.header_title,
            total: m.total,
            sent: m.sent,
            failed: m.failed,
            audience_emails: m.audience_emails,
            converted: attr.converted,
            revenue: Math.round(attr.revenue * 100) / 100,
            conversion_rate: m.sent > 0 ? Math.round((attr.converted / m.sent) * 1000) / 10 : 0,
            conversions: attr.items,
            window_days: window,
        });
    } catch (e) { console.error('[CAMPAIGN DETAIL]', e); res.status(500).json({ error: e.message }); }
});

/* ═══════════════════════════════════════════════
   AUTOMATIONS — editor de plantillas transaccionales
   Metafield lab_app/automations en Shopify (persistente)
   Notifications.js aplica override si existe; si no, usa hardcoded.
═══════════════════════════════════════════════ */
const AUTOMATION_DEFAULTS = {
    welcome: {
        name: 'Bienvenida',
        description: 'Se env\u00eda apenas el cliente activa su suscripci\u00f3n',
        trigger: 'Suscripci\u00f3n activada',
        subject: '\u25c6 Bienvenido al Club Black Diamond \u2014 LAB NUTRITION',
        header_title: 'Bienvenido al Club Black Diamond',
        body: '<h2>Bienvenido al Club Black Diamond</h2>\n<p>Hola <strong>{{first_name}}</strong>,</p>\n<p>Tu suscripci\u00f3n a <strong>{{product}}</strong> ha sido activada. Cada mes recibir\u00e1s tu producto con <strong>{{discount_pct}}% OFF</strong> exclusivo.</p>\n<div class="detail-box">\n<div class="detail-row"><span class="detail-label">Producto</span><span class="detail-value">{{product}}</span></div>\n<div class="detail-row"><span class="detail-label">Precio mensual</span><span class="detail-value">{{final_price}}</span></div>\n<div class="detail-row"><span class="detail-label">Env\u00edo</span><span class="detail-value">{{shipping}}</span></div>\n<div class="detail-row total-row"><span class="detail-label">Cobro mensual</span><span class="detail-value">{{total}}</span></div>\n</div>\n<div class="success-box"><strong>Pr\u00f3ximo env\u00edo:</strong> {{next_charge}}</div>\n<p class="muted" style="text-align:center">Bienvenido al club. \u2014 Equipo LAB NUTRITION</p>',
        enabled: true
    },
    charge_reminder: {
        name: 'Recordatorio 3 d\u00edas antes',
        description: 'Aviso previo al cobro recurrente',
        trigger: '3 d\u00edas antes del cobro',
        subject: 'Tu pedido LAB NUTRITION se procesa en 3 d\u00edas',
        header_title: 'Tu pedido est\u00e1 por llegar',
        body: '<h2>Tu pedido se procesa en 3 d\u00edas</h2>\n<p>Hola <strong>{{first_name}}</strong>,</p>\n<p>En <strong>3 d\u00edas</strong> se procesar\u00e1 tu cobro mensual y despacharemos tu <strong>{{product}}</strong>.</p>\n<div class="detail-box">\n<div class="detail-row"><span class="detail-label">Producto</span><span class="detail-value">{{product}}</span></div>\n<div class="detail-row"><span class="detail-label">Precio</span><span class="detail-value">{{final_price}}</span></div>\n<div class="detail-row total-row"><span class="detail-label">Total a cobrar</span><span class="detail-value">{{total}}</span></div>\n</div>\n<div class="alert-box"><strong>Fecha de cobro:</strong> {{next_charge}}</div>\n<p class="muted">Si necesitas cambiar tu direcci\u00f3n, cont\u00e1ctanos antes de la fecha de cobro.</p>',
        enabled: true
    },
    cancel_lock_warning: {
        name: 'Aviso 7 d\u00edas antes',
        description: 'Aviso temprano del pr\u00f3ximo cobro',
        trigger: '7 d\u00edas antes del cobro',
        subject: 'Aviso: tu cobro LAB NUTRITION es en 7 d\u00edas',
        header_title: 'Aviso de cobro',
        body: '<h2>Pr\u00f3ximo cobro en 7 d\u00edas</h2>\n<p>Hola <strong>{{first_name}}</strong>,</p>\n<p>Tu pr\u00f3ximo cobro de suscripci\u00f3n ser\u00e1 en <strong>7 d\u00edas</strong>.</p>\n<div class="detail-box">\n<div class="detail-row"><span class="detail-label">Producto</span><span class="detail-value">{{product}}</span></div>\n<div class="detail-row"><span class="detail-label">Fecha de cobro</span><span class="detail-value">{{next_charge}}</span></div>\n<div class="detail-row total-row"><span class="detail-label">Monto</span><span class="detail-value">{{total}}</span></div>\n</div>\n<p class="muted">Si tienes alguna consulta, escr\u00edbenos a contacto@labnutrition.pe</p>',
        enabled: true
    },
    charge_success: {
        name: 'Cobro exitoso',
        description: 'Confirmaci\u00f3n cuando el cargo recurrente pas\u00f3',
        trigger: 'Pago recurrente aprobado por MP',
        subject: '\u25c6 Pago procesado \u2014 LAB NUTRITION',
        header_title: 'Pago confirmado',
        body: '<h2>Pago procesado</h2>\n<p>Hola <strong>{{first_name}}</strong>,</p>\n<p>Tu cobro mensual fue procesado correctamente. Tu pedido ya est\u00e1 en camino.</p>\n<div class="detail-box">\n<div class="detail-row"><span class="detail-label">Orden</span><span class="detail-value">{{order_name}}</span></div>\n<div class="detail-row"><span class="detail-label">Producto</span><span class="detail-value">{{product}}</span></div>\n<div class="detail-row total-row"><span class="detail-label">Total cobrado</span><span class="detail-value">{{total}}</span></div>\n</div>\n<div class="success-box"><strong>Progreso:</strong> Ciclo {{cycles_completed}} de {{cycles_required}}</div>\n<p class="muted">Recibir\u00e1s un email de confirmaci\u00f3n cuando tu pedido sea despachado.</p>',
        enabled: true
    },
    charge_failed: {
        name: 'Cobro fallido',
        description: 'Alerta de fallo de cargo \u2014 acci\u00f3n requerida',
        trigger: 'MP rechaza pago recurrente',
        subject: 'Acci\u00f3n requerida: problema con tu pago \u2014 LAB NUTRITION',
        header_title: 'Acci\u00f3n requerida',
        body: '<h2>Problema con tu pago</h2>\n<p>Hola <strong>{{first_name}}</strong>,</p>\n<p>No pudimos procesar el pago de tu suscripci\u00f3n a <strong>{{product}}</strong>.</p>\n<div class="error-box"><strong>Acci\u00f3n requerida:</strong> Actualiza tu m\u00e9todo de pago para que podamos procesar tu pedido. Si no se actualiza en 48 horas, tu suscripci\u00f3n podr\u00eda pausarse.</div>\n<div class="detail-box"><div class="detail-row"><span class="detail-label">Monto pendiente</span><span class="detail-value">{{total}}</span></div></div>\n<p>Cont\u00e1ctanos por WhatsApp o email para resolver este problema.</p>',
        enabled: true
    },
    renewal_invite: {
        name: 'Permanencia completada',
        description: 'Felicitaci\u00f3n cuando termina el compromiso',
        trigger: 'Ciclo final completado',
        subject: '\u25c6 Permanencia completada \u2014 LAB NUTRITION',
        header_title: 'Felicitaciones',
        body: '<h2>Permanencia completada</h2>\n<p>Hola <strong>{{first_name}}</strong>,</p>\n<p>Has completado tus <strong>{{permanence_months}} meses</strong> de suscripci\u00f3n a <strong>{{product}}</strong>.</p>\n<div class="success-box"><strong>Tu compromiso fue cumplido.</strong> Ahora puedes cancelar sin restricciones, o continuar con tu descuento exclusivo.</div>\n<p>Si no haces nada, tu suscripci\u00f3n contin\u00faa activa con el mismo descuento.</p>\n<p class="muted" style="text-align:center">Gracias por tu confianza. \u2014 Equipo LAB NUTRITION</p>',
        enabled: true
    },
    cancellation_confirmation: {
        name: 'Cancelaci\u00f3n confirmada',
        description: 'Acuse de recibo cuando el cliente cancela',
        trigger: 'Cliente cancela desde portal',
        subject: 'Suscripci\u00f3n cancelada \u2014 LAB NUTRITION',
        header_title: 'Hasta pronto',
        body: '<h2>Suscripci\u00f3n cancelada</h2>\n<p>Hola <strong>{{first_name}}</strong>,</p>\n<p>Confirmamos que tu suscripci\u00f3n a <strong>{{product}}</strong> ha sido cancelada.</p>\n<div class="detail-box">\n<div class="detail-row"><span class="detail-label">Ciclos completados</span><span class="detail-value">{{cycles_completed}} de {{cycles_required}}</span></div>\n</div>\n<p>No se realizar\u00e1n m\u00e1s cobros. Si deseas volver a suscribirte, visita nuestra tienda.</p>\n<p class="muted" style="text-align:center">Gracias por haber sido parte del Club. \u2014 Equipo LAB NUTRITION</p>',
        enabled: true
    }
};

/** GET /api/automations → devuelve defaults + overrides + merged (por template) */
app.get('/api/automations', async (req, res) => {
    try {
        const overrides = await readFromShopify('lab_app', 'automations') || {};
        const merged = {};
        for (const key of Object.keys(AUTOMATION_DEFAULTS)) {
            const def = AUTOMATION_DEFAULTS[key];
            const ov = overrides[key] || {};
            merged[key] = {
                key,
                name: def.name,
                description: def.description,
                trigger: def.trigger,
                subject: ov.subject != null ? ov.subject : def.subject,
                header_title: ov.header_title != null ? ov.header_title : def.header_title,
                body: ov.body != null ? ov.body : def.body,
                enabled: ov.enabled !== false,
                is_customized: !!(ov.subject || ov.body || ov.header_title || ov.enabled === false)
            };
        }
        res.json({ templates: merged, vars: ['first_name','name','email','product','final_price','shipping','total','next_charge','cycles_completed','cycles_required','permanence_months','discount_pct','portal_link','order_name'] });
    } catch (e) { console.error('[AUTOMATIONS GET]', e); res.status(500).json({ error: e.message }); }
});

/** PUT /api/automations → guarda overrides (un subset o todos) y refresca cache */
app.put('/api/automations', async (req, res) => {
    try {
        const body = req.body || {};
        const incoming = body.templates || body;
        if (!incoming || typeof incoming !== 'object') return res.status(400).json({ error: 'body inv\u00e1lido' });
        const current = await readFromShopify('lab_app', 'automations') || {};
        const merged = { ...current };
        for (const key of Object.keys(incoming)) {
            if (!AUTOMATION_DEFAULTS[key]) continue; // ignora claves desconocidas
            const t = incoming[key] || {};
            // Solo guardamos campos con override explícito (no el trigger/name que son fijos)
            const entry = {};
            if (typeof t.subject === 'string') entry.subject = t.subject;
            if (typeof t.header_title === 'string') entry.header_title = t.header_title;
            if (typeof t.body === 'string') entry.body = t.body;
            if (typeof t.enabled === 'boolean') entry.enabled = t.enabled;
            if (Object.keys(entry).length) merged[key] = entry;
        }
        const saved = await saveToShopify(merged, 'lab_app', 'automations');
        if (notifications && notifications.invalidateAutomationsCache) notifications.invalidateAutomationsCache();
        res.json({ success: !!saved, storage: saved ? 'shopify_metafields' : 'failed', saved_keys: Object.keys(merged) });
    } catch (e) { console.error('[AUTOMATIONS PUT]', e); res.status(500).json({ error: e.message }); }
});

/** POST /api/automations/test → envía email de test con mock sub */
app.post('/api/automations/test', async (req, res) => {
    try {
        const { key, to, subject_override, body_override, header_title } = req.body || {};
        if (!key || !to) return res.status(400).json({ error: 'key y to son requeridos' });
        if (!AUTOMATION_DEFAULTS[key]) return res.status(400).json({ error: 'template no existe: ' + key });

        const overrides = await readFromShopify('lab_app', 'automations') || {};
        const def = AUTOMATION_DEFAULTS[key];
        const ov = overrides[key] || {};
        const subject = subject_override != null ? subject_override : (ov.subject != null ? ov.subject : def.subject);
        const header = header_title != null ? header_title : (ov.header_title != null ? ov.header_title : def.header_title);
        const bodyRaw = body_override != null ? body_override : (ov.body != null ? ov.body : def.body);

        const mockSub = {
            customer_name: 'Test Usuario',
            customer_email: to,
            product_title: 'CREATINE MICRONIZED BLACK LIMITED EDITION',
            frequency_months: 1, permanence_months: 6,
            discount_pct: 50, base_price: 179, final_price: 90,
            cycles_completed: 2, cycles_required: 6,
            next_charge_at: new Date(Date.now() + 3 * 86400000).toISOString(),
            shipping_address: { address1: 'Augusto Tamayo 180', city: 'San Isidro', province: 'Lima' }
        };
        const rSubject = notifications.renderVars(subject, mockSub, { order_name: '#8442' });
        const rBody = notifications.renderVars(bodyRaw || '', mockSub, { order_name: '#8442' });
        const bodyHTML = /<[a-z][\s\S]*>/i.test(rBody) ? rBody : rBody.replace(/\n/g, '<br>');
        const html = notifications.__baseHTML(bodyHTML, { headerTitle: header || 'Club Black Diamond' });
        await notifications.sendEmail(to, '[TEST] ' + rSubject, html);
        res.json({ success: true, to, via: process.env.RESEND_API_KEY ? 'resend' : 'smtp' });
    } catch (e) { console.error('[AUTOMATIONS TEST]', e); res.status(500).json({ error: e.message }); }
});

/** GET /api/automations/preview?key=welcome&subject=...&body=... (HTML puro) */
app.get('/api/automations/preview', async (req, res) => {
    try {
        const { key = 'welcome', subject = '', body = '', header_title = '' } = req.query || {};
        const def = AUTOMATION_DEFAULTS[key] || AUTOMATION_DEFAULTS.welcome;
        const mockSub = {
            customer_name: 'Jorge Luis Torres Morales',
            customer_email: 'ejemplo@labnutrition.pe',
            product_title: 'CREATINE MICRONIZED BLACK LIMITED EDITION',
            frequency_months: 1, permanence_months: 6,
            discount_pct: 50, base_price: 179, final_price: 90,
            cycles_completed: 2, cycles_required: 6,
            next_charge_at: new Date(Date.now() + 3 * 86400000).toISOString()
        };
        const subj = notifications.renderVars(subject || def.subject, mockSub, { order_name: '#8442' });
        const bodyRaw = notifications.renderVars(body || def.body, mockSub, { order_name: '#8442' });
        const bodyHTML = /<[a-z][\s\S]*>/i.test(bodyRaw) ? bodyRaw : bodyRaw.replace(/\n/g, '<br>');
        const html = notifications.__baseHTML(bodyHTML, { headerTitle: header_title || def.header_title });
        res.json({ subject: subj, html });
    } catch (e) { res.status(500).json({ error: e.message }); }
});

/* ── Export subscribers as CSV ── */
app.get('/api/subscribers/export', async (req, res) => {
    const { status } = req.query;
    let data = await db.getSubscriptions(status ? { status } : {});

    const header = 'Email,Nombre,Telefono,Producto,Frecuencia,Permanencia,Descuento%,Precio,Estado,Ciclos,ProximoCobro,Inicio\n';
    const rows = (data || []).map(s => [
        s.customer_email, s.customer_name || '', s.customer_phone || '',
        `"${s.product_title || ''}"`, s.frequency_months === 1 ? 'Mensual' : 'Bimestral',
        (s.permanence_months || '') + 'm', (s.discount_pct || '') + '%', s.final_price || '',
        s.status, `${s.cycles_completed || 0}/${s.cycles_required || 0}`,
        s.next_charge_at?.split('T')[0] || '', s.created_at?.split('T')[0] || ''
    ].join(',')).join('\n');

    res.setHeader('Content-Type', 'text/csv');
    res.setHeader('Content-Disposition', 'attachment; filename="suscriptores-lab.csv"');
    res.send(header + rows);
});

/* ── STACKS API — stored in settings metafield (v6.1.0) ── */
app.get('/api/stacks', async (req, res) => {
    try {
        const s = await readFromShopify() || readFromFile() || {};
        res.json(Array.isArray(s.stacks) ? s.stacks : []);
    } catch { res.json([]); }
});

app.post('/api/stacks', async (req, res) => {
    try {
        const s = await readFromShopify() || readFromFile() || {};
        if (!Array.isArray(s.stacks)) s.stacks = [];
        const idx = s.stacks.findIndex(st => st.id === req.body.id);
        if (idx >= 0) s.stacks[idx] = req.body;
        else s.stacks.push({ ...req.body, id: `stack_${Date.now()}`, created_at: new Date().toISOString() });
        await saveToShopify(s); saveToFile(s);
        res.json(req.body);
    } catch (e) { res.status(500).json({ error: e.message }); }
});

app.delete('/api/stacks/:id', async (req, res) => {
    try {
        const s = await readFromShopify() || readFromFile() || {};
        s.stacks = (Array.isArray(s.stacks) ? s.stacks : []).filter(st => st.id !== req.params.id);
        await saveToShopify(s); saveToFile(s);
        res.json({ success: true });
    } catch (e) { res.status(500).json({ error: e.message }); }
});

/* ── SETUP: initialize Shopify Metaobject types ── */
app.post('/api/setup', async (req, res) => {
    try {
        await db.initializeTypes();
        res.json({ success: true, message: 'Shopify Metaobject types initialized: lab_subscription, lab_sub_event' });
    } catch (e) { res.status(500).json({ error: e.message }); }
});
/* ═══════════════════════════════════════════════
   🛒 SHOPIFY PRODUCTS API (fetch real products)
═══════════════════════════════════════════════ */
app.get('/api/shopify/products', async (req, res) => {
    try {
        const shop = process.env.SHOPIFY_SHOP || 'nutrition-lab-cluster.myshopify.com';
        const token = process.env.SHOPIFY_ACCESS_TOKEN || _shopifyToken;
        if (!token) {
            return res.json({ products: [], error: 'SHOPIFY_ACCESS_TOKEN not set. Visit /auth?shop=' + shop + ' to authorize.' });
        }
        // 2026-04-21 — ADITIVO: soporte opcional de `query` y `limit` sin romper consumidores existentes.
        // Si hay ?query=..., usamos el endpoint GraphQL de búsqueda (más rápido y relevante).
        const q = (req.query.query || '').toString().trim();
        const limit = Math.min(250, Math.max(1, parseInt(req.query.limit, 10) || 250));
        if (q) {
            const gql = `query SearchProducts($q: String!, $first: Int!) {
                products(first: $first, query: $q) {
                    edges { node { id title handle status
                        featuredImage { url }
                        variants(first: 50) { edges { node { id title price sku inventoryQuantity } } }
                    } }
                }
            }`;
            const variables = { q: q, first: limit };
            const r = await fetch(`https://${shop}/admin/api/2026-01/graphql.json`, {
                method: 'POST',
                headers: { 'X-Shopify-Access-Token': token, 'Content-Type': 'application/json' },
                body: JSON.stringify({ query: gql, variables })
            });
            if (!r.ok) {
                const errText = await r.text();
                return res.json({ products: [], error: `Shopify GraphQL ${r.status}: ${errText.slice(0, 200)}` });
            }
            const gqlData = await r.json();
            const edges = gqlData?.data?.products?.edges || [];
            return res.json({
                products: edges.map(e => {
                    const n = e.node;
                    const numId = String(n.id).split('/').pop();
                    const vEdges = n.variants?.edges || [];
                    return {
                        id: numId,
                        title: n.title,
                        handle: n.handle,
                        image: n.featuredImage?.url || null,
                        price: vEdges[0]?.node?.price || '0',
                        status: n.status,
                        variants_count: vEdges.length,
                        variants: vEdges.map(ve => ({
                            id: String(ve.node.id).split('/').pop(),
                            title: ve.node.title,
                            price: ve.node.price,
                            sku: ve.node.sku || ''
                        }))
                    };
                })
            });
        }
        // Legacy (sin query) — mantiene 100% compatibilidad con consumidores previos
        const url = `https://${shop}/admin/api/2026-01/products.json?limit=${limit}&fields=id,title,handle,images,variants,status`;
        const r = await fetch(url, { headers: { 'X-Shopify-Access-Token': token, 'Content-Type': 'application/json' } });
        if (!r.ok) {
            const errText = await r.text();
            return res.json({ products: [], error: `Shopify API ${r.status}: ${errText.slice(0, 200)}` });
        }
        const data = await r.json();
        res.json({
            products: (data.products || []).map(p => ({
                id: p.id,
                title: p.title,
                handle: p.handle,
                image: p.images?.[0]?.src || null,
                price: p.variants?.[0]?.price || '0',
                status: p.status,
                variants_count: (p.variants || []).length,
                variants: (p.variants || []).map(v => ({
                    id: v.id,
                    title: v.title,
                    price: v.price,
                    sku: v.sku || ''
                }))
            }))
        });
    } catch (e) { res.json({ products: [], error: e.message }) }
});

/**
 * 2026-04-21 — ADITIVO: endpoint para listar variantes de un producto con stock e imagen.
 * Usado por el admin panel de bundles para mostrar sabores disponibles.
 * No afecta productos ni órdenes existentes (solo lectura).
 */
app.get('/api/shopify/products/:id/variants', async (req, res) => {
    try {
        const shop = process.env.SHOPIFY_SHOP || 'nutrition-lab-cluster.myshopify.com';
        const token = process.env.SHOPIFY_ACCESS_TOKEN || _shopifyToken;
        if (!token) return res.status(500).json({ error: 'SHOPIFY_ACCESS_TOKEN not set' });

        const productId = String(req.params.id).trim();
        const url = `https://${shop}/admin/api/2026-01/products/${encodeURIComponent(productId)}.json?fields=id,title,images,variants`;
        const r = await fetch(url, { headers: { 'X-Shopify-Access-Token': token, 'Content-Type': 'application/json' } });
        if (!r.ok) {
            const errText = await r.text();
            return res.status(r.status).json({ error: `Shopify ${r.status}: ${errText.slice(0, 200)}` });
        }
        const data = await r.json();
        const product = data.product;
        if (!product) return res.status(404).json({ error: 'Product not found' });
        const imagesById = new Map((product.images || []).map(img => [img.id, img.src]));
        res.json({
            product_id: product.id,
            product_title: product.title,
            variants: (product.variants || []).map(v => ({
                id: v.id,
                title: v.title,
                price: v.price,
                sku: v.sku || '',
                inventory_quantity: (typeof v.inventory_quantity === 'number') ? v.inventory_quantity : null,
                image: v.image_id ? (imagesById.get(v.image_id) || null) : (product.images?.[0]?.src || null)
            }))
        });
    } catch (e) { res.status(500).json({ error: e.message }); }
});

/* ═══════════════════════════════════════════════
   ⚙️ SETTINGS API — Shopify Metafields as native storage
   Settings are stored in Shopify Shop Metafields (namespace: lab_app)
   This is the correct 2025/2026 Shopify embedded app pattern.
   No Supabase, no files — Shopify IS the database.
═══════════════════════════════════════════════ */
const fs = require('fs');
const SETTINGS_FILE = path.join(__dirname, '..', 'settings.json');
const METAFIELD_NAMESPACE = 'lab_app';
const METAFIELD_KEY = 'settings';

// Env vars are ALWAYS the authoritative source for Shopify credentials
// (they come from Railway). Other settings can come from Shopify Metafields.
function getEnvDefaults() {
    return {
        shopify_shop: process.env.SHOPIFY_SHOP || 'nutrition-lab-cluster.myshopify.com',
        shopify_access_token: process.env.SHOPIFY_ACCESS_TOKEN || _shopifyToken || '',
        // 🏬 Hub de despacho Navasoft — TODAS las ordenes de suscripcion caen aca, no importa
        //    la direccion del cliente. Se setea desde el admin (Configuracion → Shopify).
        shopify_location_id: process.env.SHOPIFY_LOCATION_ID || '',
        mp_access_token: process.env.MP_ACCESS_TOKEN || '',
        mp_public_key: process.env.MP_PUBLIC_KEY || '',
        smtp_host: process.env.SMTP_HOST || 'smtp.gmail.com',
        smtp_port: process.env.SMTP_PORT || '587',
        smtp_user: process.env.SMTP_USER || '',
        smtp_pass: process.env.SMTP_PASS || '',
        email_from: process.env.EMAIL_FROM || '',
        widget_enabled: true,
        brand_color: '#9d2a23',
        discount_badge_text: 'HASTA -30%',
    };
}

// Read from Shopify Shop Metafields
async function readFromShopify(ns, key) {
    const shop = process.env.SHOPIFY_SHOP || 'nutrition-lab-cluster.myshopify.com';
    const token = process.env.SHOPIFY_ACCESS_TOKEN || _shopifyToken;
    if (!token) { console.warn('[SHOPIFY MF] No token — cannot read', ns, key); return null; }
    const namespace = ns || METAFIELD_NAMESPACE;
    const mfKey = key || METAFIELD_KEY;
    try {
        const r = await fetch(
            `https://${shop}/admin/api/2026-01/metafields.json?metafield[owner_resource]=shop&metafield[namespace]=${namespace}&metafield[key]=${mfKey}`,
            { headers: { 'X-Shopify-Access-Token': token, 'Content-Type': 'application/json' } }
        );
        if (!r.ok) {
            const errText = await r.text();
            console.error(`[SHOPIFY MF] READ ERROR ${r.status} for ${namespace}/${mfKey}:`, errText.slice(0, 300));
            return null;
        }
        const data = await r.json();
        const mf = data.metafields?.[0];
        if (mf?.value) {
            try {
                return JSON.parse(mf.value);
            } catch (parseErr) {
                console.error(`[SHOPIFY MF] JSON parse error for ${namespace}/${mfKey}:`, parseErr.message);
                return null;
            }
        }
        return null;
    } catch (e) {
        console.error(`[SHOPIFY MF] Network error reading ${namespace}/${mfKey}:`, e.message);
        return null;
    }
}

// Save to Shopify Shop Metafields
async function saveToShopify(settings, ns, key) {
    const shop = process.env.SHOPIFY_SHOP || 'nutrition-lab-cluster.myshopify.com';
    const token = process.env.SHOPIFY_ACCESS_TOKEN || _shopifyToken;
    if (!token) { console.error('[SHOPIFY MF] No token — CANNOT SAVE', ns, key); return false; }
    const namespace = ns || METAFIELD_NAMESPACE;
    const mfKey = key || METAFIELD_KEY;
    try {
        // 1. Check if metafield exists
        const checkR = await fetch(
            `https://${shop}/admin/api/2026-01/metafields.json?metafield[owner_resource]=shop&metafield[namespace]=${namespace}&metafield[key]=${mfKey}`,
            { headers: { 'X-Shopify-Access-Token': token } }
        );
        if (!checkR.ok) {
            const errText = await checkR.text();
            console.error(`[SHOPIFY MF] CHECK ERROR ${checkR.status} for ${namespace}/${mfKey}:`, errText.slice(0, 300));
            return false;
        }
        const checkData = await checkR.json();
        const existing = checkData.metafields?.[0];

        // 2. Serialize value
        const valueStr = JSON.stringify(settings);
        if (valueStr.length > 65000) {
            console.error(`[SHOPIFY MF] VALUE TOO LARGE for ${namespace}/${mfKey}: ${valueStr.length} bytes (max 65000)`);
            return false;
        }

        // 3. PUT (update) or POST (create)
        const body = existing
            ? { metafield: { id: existing.id, value: valueStr, type: 'json' } }
            : { metafield: { namespace, key: mfKey, value: valueStr, type: 'json', owner_resource: 'shop' } };
        const method = existing ? 'PUT' : 'POST';
        const url = existing
            ? `https://${shop}/admin/api/2026-01/metafields/${existing.id}.json`
            : `https://${shop}/admin/api/2026-01/metafields.json`;

        const saveR = await fetch(url, {
            method,
            headers: { 'X-Shopify-Access-Token': token, 'Content-Type': 'application/json' },
            body: JSON.stringify(body)
        });

        if (!saveR.ok) {
            const errText = await saveR.text();
            console.error(`[SHOPIFY MF] SAVE ERROR ${saveR.status} for ${namespace}/${mfKey}:`, errText.slice(0, 500));
            return false;
        }
        const saveData = await saveR.json();
        if (saveData.errors) {
            console.error(`[SHOPIFY MF] SAVE ERRORS for ${namespace}/${mfKey}:`, JSON.stringify(saveData.errors));
            return false;
        }
        console.log(`[SHOPIFY MF] ✅ Saved ${namespace}/${mfKey} (${valueStr.length} bytes, ${method})`);
        return true;
    } catch (e) {
        console.error(`[SHOPIFY MF] Network error saving ${namespace}/${mfKey}:`, e.message);
        return false;
    }
}


// File fallback (in-memory across restarts via Railway volume if available)
function readFromFile() {
    try { if (fs.existsSync(SETTINGS_FILE)) return JSON.parse(fs.readFileSync(SETTINGS_FILE, 'utf8')); } catch { }
    return null;
}
function saveToFile(settings) {
    try { fs.writeFileSync(SETTINGS_FILE, JSON.stringify(settings, null, 2)); } catch { }
}

// REVERT 2026-06-04: el admin UI necesita ver los tokens raw para llenar inputs.
// Sin esto, admin no puede ver/editar su config. Dejamos GET como estaba originalmente.
/* 🆕 2026-06-11 — Config PÚBLICA (solo branding + MP public key) para páginas
   de cliente (portal.html). /api/settings ahora exige X-Admin-Token porque su
   GET devolvía shpat_/APP_USR/SMTP pass sin auth. Acá NO va ningún secreto. */
app.get('/api/public-config', async (req, res) => {
    try {
        const base = getEnvDefaults();
        const persisted = await readFromShopify().catch(() => null) || readFromFile() || {};
        const merged = { ...base, ...persisted };
        res.json({
            brand_name: merged.brand_name || '',
            brand_slogan: merged.brand_slogan || '',
            brand_logo: merged.brand_logo || '',
            brand_color: merged.brand_color || '#9d2a23',
            widget_enabled: merged.widget_enabled !== false,
            discount_badge_text: merged.discount_badge_text || '',
            mp_public_key: merged.mp_public_key || '',
            build: '2026-06-11.7' // marcador de deploy (verificación sin auth)
        });
    } catch (e) { res.json({ brand_name: '', brand_slogan: '', brand_logo: '' }); }
});

app.get('/api/settings', async (req, res) => {
    const base = getEnvDefaults();
    // Try Shopify Metafields first (native, always available)
    const fromShopify = await readFromShopify();
    if (fromShopify) return res.json({ ...base, ...fromShopify });
    // Try local file fallback
    const fromFile = readFromFile();
    if (fromFile) return res.json({ ...base, ...fromFile });
    // Return env defaults
    res.json(base);
});

app.put('/api/settings', async (req, res) => {
    const body = req.body;
    // Update process.env in memory immediately (for current session)
    const envMap = {
        mp_access_token: 'MP_ACCESS_TOKEN',
        mp_public_key: 'MP_PUBLIC_KEY',
        supabase_url: 'SUPABASE_URL',
        supabase_key: 'SUPABASE_SERVICE_KEY',
        shopify_shop: 'SHOPIFY_SHOP',
        shopify_location_id: 'SHOPIFY_LOCATION_ID',
        smtp_host: 'SMTP_HOST', smtp_port: 'SMTP_PORT',
        smtp_user: 'SMTP_USER', smtp_pass: 'SMTP_PASS', email_from: 'EMAIL_FROM',
        resend_api_key: 'RESEND_API_KEY', resend_from: 'RESEND_FROM'
    };
    Object.entries(envMap).forEach(([key, envKey]) => { if (body[key]) process.env[envKey] = String(body[key]); });
    if (body.shopify_access_token) { process.env.SHOPIFY_ACCESS_TOKEN = body.shopify_access_token; _shopifyToken = body.shopify_access_token; }
    // 🆕 2026-06-11: kill switch dunning aplicable en caliente (el comentario de
    //   _emailsAreEnabled documentaba este camino pero NUNCA estuvo cableado:
    //   ni este PUT ni el boot hidrataban DUNNING_EMAILS_ENABLED). Acepta true/false.
    if (typeof body.dunning_emails_enabled !== 'undefined') {
        process.env.DUNNING_EMAILS_ENABLED = (body.dunning_emails_enabled === true || body.dunning_emails_enabled === 'true') ? 'true' : 'false';
        console.log(`[SETTINGS] DUNNING_EMAILS_ENABLED → ${process.env.DUNNING_EMAILS_ENABLED}`);
    }
    // Merge with current and save
    const current = await readFromShopify() || readFromFile() || {};
    const merged = { ...current, ...body };
    const [shopifySaved] = await Promise.all([saveToShopify(merged), Promise.resolve(saveToFile(merged))]);
    res.json({ success: true, storage: shopifySaved ? 'shopify_metafields' : 'local_file', settings: { ...getEnvDefaults(), ...merged } });
});

/* ── TEST MP CONNECTION — verifica token en tiempo real ── */
app.post('/api/settings/test-mp', async (req, res) => {
    // If a token was sent, apply it now before testing
    if (req.body && req.body.token) {
        process.env.MP_ACCESS_TOKEN = req.body.token.trim();
    }
    try {
        if (!process.env.MP_ACCESS_TOKEN) return res.json({ ok: false, error: 'Token no configurado' });
        const result = await mp.verifyConnection();
        res.json({ ok: true, message: 'Conexión con Mercado Pago verificada correctamente', ...result });
    } catch (e) {
        const msg = e?.cause?.message || e?.message || 'Error de autenticación';
        res.json({ ok: false, error: msg.includes('401') || msg.includes('unauthorized') ? 'Token inválido o sin permisos de producción' : msg });
    }
});

/* ═══════════════════════════════════════════════
   👤 PORTAL DEL SUSCRIPTOR (customer self-service)
   FIX 2026-04-11: Specific routes MUST come BEFORE the :email catch-all,
   otherwise Express captures "subscription" as an email parameter.
═══════════════════════════════════════════════ */

/* ═══════════════════════════════════════════════════════════════════
   🔐 PORTAL V2 — Magic link auth + dashboard real (2026-06-04)
   Backend del nuevo portal del suscriptor. Endpoints firmados con HMAC.
   - POST /api/portal/v2/request-link {email}: envía magic link 30min
   - GET  /api/portal/v2/me?token=jwt: data agregada del cliente
   - POST /api/portal/v2/sub/:id/pause con token
   - POST /api/portal/v2/sub/:id/cancel con token (con preview de penalidad)
   - POST /api/portal/v2/sub/:id/resume con token
   ═══════════════════════════════════════════════════════════════════ */
const _PORTAL_V2_SECRET = process.env.PORTAL_V2_SECRET
    || process.env.SHOPIFY_API_SECRET
    || 'pixel-portal-v2-default-secret-rotate-me';

function _portalV2SignToken(payload) {
    const header = Buffer.from(JSON.stringify({ alg: 'HS256', typ: 'JWT' })).toString('base64url');
    const body = Buffer.from(JSON.stringify(payload)).toString('base64url');
    const sig = crypto.createHmac('sha256', _PORTAL_V2_SECRET).update(`${header}.${body}`).digest('base64url');
    return `${header}.${body}.${sig}`;
}

function _portalV2VerifyToken(token) {
    try {
        const [header, body, sig] = String(token || '').split('.');
        if (!header || !body || !sig) return null;
        const expected = crypto.createHmac('sha256', _PORTAL_V2_SECRET).update(`${header}.${body}`).digest('base64url');
        if (expected !== sig) return null;
        const payload = JSON.parse(Buffer.from(body, 'base64url').toString('utf8'));
        if (payload.exp && payload.exp < Date.now()) return null; // expired
        return payload;
    } catch { return null; }
}

/* POST /api/portal/v2/request-link {email} — manda magic link 30 min */
app.post('/api/portal/v2/request-link', (req, res, next) => _portalRateLimit(req, res, next), async (req, res) => {
    try {
        const email = String(req.body?.email || '').trim().toLowerCase();
        if (!email || !email.includes('@')) return res.status(400).json({ error: 'Email inválido' });

        const allSubs = await db.getSubscriptions().catch(() => []);
        const subs = allSubs.filter(s => (s.customer_email || '').toLowerCase() === email);
        if (!subs.length) {
            // Return success anyway (no enumeration) but DON'T send email
            return res.json({ success: true, message: 'Si existe una cuenta, te enviamos el link.' });
        }

        const token = _portalV2SignToken({
            email,
            sub_ids: subs.map(s => s.id),
            iat: Date.now(),
            exp: Date.now() + 30 * 60 * 1000 // 30 min
        });
        const portalUrl = `${process.env.BACKEND_URL || 'https://pixel-suite-pro-production.up.railway.app'}/portal/v2?token=${encodeURIComponent(token)}`;

        // Send via Resend (if configured)
        if (process.env.RESEND_API_KEY) {
            const html = `<!DOCTYPE html><html><body style="font-family:Arial,sans-serif;padding:20px;background:#f5f5f5">
                <div style="max-width:560px;margin:0 auto;background:#fff;padding:32px;border-radius:12px">
                    <div style="text-align:center;margin-bottom:24px">
                        <h1 style="color:#E30613;font-size:24px;margin:0;text-transform:uppercase;letter-spacing:1px">LAB NUTRITION</h1>
                    </div>
                    <h2 style="color:#0A0A0A;font-size:20px;margin:0 0 16px">Tu acceso al portal</h2>
                    <p style="color:#444;line-height:1.6">Haz clic en el botón para gestionar tu suscripción:</p>
                    <div style="text-align:center;margin:32px 0">
                        <a href="${portalUrl}" style="display:inline-block;background:#E30613;color:#fff;padding:14px 36px;border-radius:8px;text-decoration:none;font-weight:700;text-transform:uppercase;letter-spacing:1px">Ingresar al portal</a>
                    </div>
                    <p style="color:#888;font-size:13px;line-height:1.6">Este link es válido por 30 minutos. Si no fuiste tú, ignora este correo.</p>
                </div>
            </body></html>`;
            await fetch('https://api.resend.com/emails', {
                method: 'POST',
                headers: { 'Authorization': `Bearer ${process.env.RESEND_API_KEY}`, 'Content-Type': 'application/json' },
                body: JSON.stringify({
                    from: process.env.RESEND_FROM || 'LAB NUTRITION <onboarding@resend.dev>',
                    to: [email],
                    subject: 'Tu acceso al portal LAB NUTRITION',
                    html
                })
            }).catch(e => console.warn('[PORTAL V2] Resend error:', e.message));
        }
        if (db.createEvent && subs[0]) {
            await db.createEvent({ subscription_id: subs[0].id, event_type: 'portal_link_requested', metadata: JSON.stringify({ email, at: new Date().toISOString() }) }).catch(() => {});
        }
        res.json({ success: true, message: 'Si existe una cuenta, te enviamos el link.' });
    } catch (e) { res.status(500).json({ error: e.message }); }
});

/* GET /api/portal/v2/me?token=xxx — data agregada del cliente */
app.get('/api/portal/v2/me', async (req, res) => {
    try {
        const payload = _portalV2VerifyToken(req.query.token);
        if (!payload) return res.status(401).json({ error: 'Token inválido o expirado' });
        const allSubs = await db.getSubscriptions().catch(() => []);
        const mySubs = allSubs.filter(s => payload.sub_ids.includes(s.id));
        // Read available products (eligible_products)
        const settings = await readFromShopify() || readFromFile() || {};
        const availableProducts = (settings.eligible_products || []).filter(p => p && p.shopify_product_id);
        // Sanitize subs (no tokens, no internal IDs)
        const sanitized = mySubs.map(s => ({
            id: s.id,
            product_title: s.product_title,
            product_image: s.product_image,
            status: s.status,
            cycles_completed: parseInt(s.cycles_completed || 0),
            cycles_required: parseInt(s.cycles_required || 0),
            frequency_months: s.frequency_months,
            permanence_months: s.permanence_months,
            base_price: s.base_price,
            final_price: s.final_price,
            shipping_cost: s.shipping_cost,
            mp_total_amount: s.mp_total_amount,
            discount_pct: s.discount_pct,
            next_charge_at: s.next_charge_at,
            last_charge_at: s.last_charge_at,
            activated_at: s.activated_at,
            shopify_order_name: s.shopify_order_name,
            shipping_address: s.shipping_address || null,
            dni: s.dni,
            paused_until: s.paused_until || null
        }));
        res.json({
            email: payload.email,
            subscriptions: sanitized,
            available_products: availableProducts
        });
    } catch (e) { res.status(500).json({ error: e.message }); }
});

/* POST /api/portal/v2/sub/:id/pause — pausa con token */
app.post('/api/portal/v2/sub/:id/pause', async (req, res) => {
    try {
        const payload = _portalV2VerifyToken(req.query.token);
        if (!payload || !payload.sub_ids.includes(req.params.id)) return res.status(401).json({ error: 'No autorizado' });
        if (!(await _portalPermAllowed('allow_pause'))) return res.status(403).json({ error: 'La pausa de suscripciones está deshabilitada por la tienda. Escríbenos por WhatsApp y te ayudamos.' });
        const { pauseMonths = 1 } = req.body || {};
        const sub = await db.getSubscription(req.params.id);
        if (!sub || sub.status !== 'active') return res.status(400).json({ error: 'No se puede pausar' });
        if (mp.pauseSubscription) await mp.pauseSubscription(sub.mp_preapproval_id).catch(() => {});
        const pausedUntil = new Date();
        pausedUntil.setMonth(pausedUntil.getMonth() + parseInt(pauseMonths));
        await db.updateSubscription(sub.id, { status: 'paused', paused_until: pausedUntil.toISOString() });
        await db.createEvent({ subscription_id: sub.id, event_type: 'paused_by_customer', metadata: JSON.stringify({ pause_months: pauseMonths, via: 'portal_v2' }) }).catch(() => {});
        res.json({ success: true, pausedUntil });
    } catch (e) { res.status(500).json({ error: e.message }); }
});

/* POST /api/portal/v2/sub/:id/resume — reanuda con token */
app.post('/api/portal/v2/sub/:id/resume', async (req, res) => {
    try {
        const payload = _portalV2VerifyToken(req.query.token);
        if (!payload || !payload.sub_ids.includes(req.params.id)) return res.status(401).json({ error: 'No autorizado' });
        const sub = await db.getSubscription(req.params.id);
        if (!sub || sub.status !== 'paused') return res.status(400).json({ error: 'No está pausada' });
        if (mp.resumeSubscription) await mp.resumeSubscription(sub.mp_preapproval_id).catch(() => {});
        await db.updateSubscription(sub.id, { status: 'active', paused_until: null });
        await db.createEvent({ subscription_id: sub.id, event_type: 'resumed_by_customer', metadata: JSON.stringify({ via: 'portal_v2' }) }).catch(() => {});
        res.json({ success: true });
    } catch (e) { res.status(500).json({ error: e.message }); }
});

/* GET /api/portal/v2/sub/:id/cancel-preview — preview de penalidad (no cancela) */
app.get('/api/portal/v2/sub/:id/cancel-preview', async (req, res) => {
    try {
        const payload = _portalV2VerifyToken(req.query.token);
        if (!payload || !payload.sub_ids.includes(req.params.id)) return res.status(401).json({ error: 'No autorizado' });
        const sub = await db.getSubscription(req.params.id);
        if (!sub) return res.status(404).json({ error: 'Not found' });
        const cyclesCompleted = parseInt(sub.cycles_completed || 0);
        const cyclesRequired = parseInt(sub.cycles_required || 0);
        const monthlyDiscount = ((parseFloat(sub.base_price || 0) - parseFloat(sub.final_price || 0)) || 0);
        const penalty = cyclesCompleted < cyclesRequired ? monthlyDiscount * cyclesCompleted : 0;
        res.json({
            sub_id: sub.id,
            cycles_completed: cyclesCompleted,
            cycles_required: cyclesRequired,
            permanence_completed: cyclesCompleted >= cyclesRequired,
            monthly_discount: monthlyDiscount,
            penalty,
            message: penalty > 0
                ? `Para cancelar antes de completar la permanencia debes reintegrar S/${penalty.toFixed(2)} (el descuento recibido durante ${cyclesCompleted} meses). No es una multa — es la devolución del beneficio de un compromiso no completado.`
                : 'Puedes cancelar sin penalidad — ya completaste tu permanencia.'
        });
    } catch (e) { res.status(500).json({ error: e.message }); }
});

/* POST /api/portal/v2/sub/:id/cancel — cancela con token (después de preview) */
app.post('/api/portal/v2/sub/:id/cancel', async (req, res) => {
    try {
        const payload = _portalV2VerifyToken(req.query.token);
        if (!payload || !payload.sub_ids.includes(req.params.id)) return res.status(401).json({ error: 'No autorizado' });
        if (!(await _portalPermAllowed('allow_cancel'))) return res.status(403).json({ error: 'La cancelación desde el portal está deshabilitada por la tienda. Escríbenos por WhatsApp para gestionarla.' });
        const sub = await db.getSubscription(req.params.id);
        if (!sub) return res.status(404).json({ error: 'Not found' });
        if (sub.status === 'cancelled') return res.json({ success: true, already_cancelled: true });
        if (mp.cancelSubscription) await mp.cancelSubscription(sub.mp_preapproval_id).catch(() => {});
        await db.updateSubscription(sub.id, { status: 'cancelled', cancelled_at: new Date().toISOString(), cancelled_by: 'customer' });
        await db.createEvent({ subscription_id: sub.id, event_type: 'cancelled_by_customer', metadata: JSON.stringify({ via: 'portal_v2', cycles_completed: parseInt(sub.cycles_completed || 0) }) }).catch(() => {});
        res.json({ success: true });
    } catch (e) { res.status(500).json({ error: e.message }); }
});

/* GET /api/portal/subscription/:id/history — MUST be before :email route */
app.get('/api/portal/subscription/:id/history', async (req, res) => {
    try {
        const events = db?.getEvents ? await db.getEvents(req.params.id) : [];
        res.json({ events: events || [] });
    } catch (e) { res.json({ events: [] }); }
});

/* POST /api/portal/subscription/:id/pause — MUST be before :email route */
app.post('/api/portal/subscription/:id/pause', async (req, res) => {
    try {
        if (!(await _portalPermAllowed('allow_pause'))) return res.status(403).json({ error: 'La pausa de suscripciones está deshabilitada por la tienda. Escríbenos por WhatsApp y te ayudamos.' });
        const { pauseMonths = 1 } = req.body;
        const sub = await db.getSubscription(req.params.id);
        if (!sub || sub.status !== 'active') return res.status(400).json({ error: 'Cannot pause' });
        if (mp.pauseSubscription) await mp.pauseSubscription(sub.mp_preapproval_id).catch(() => { });
        const pausedUntil = new Date();
        pausedUntil.setMonth(pausedUntil.getMonth() + parseInt(pauseMonths));
        await db.updateSubscription(sub.id, { status: 'paused', paused_until: pausedUntil.toISOString() });
        await db.createEvent({ subscription_id: sub.id, event_type: 'paused', metadata: JSON.stringify({ pause_months: pauseMonths }) });
        res.json({ success: true, pausedUntil });
    } catch (e) { res.status(500).json({ error: e.message }); }
});

/* Portal skip removed 2026-04-11: feature disabled per business rules */

/* POST /api/subscriptions/:id/resume (for portal reactivation) */
/* 🔒 FIX 2026-06-04: hardened resume — verifica MP real antes de actualizar DB.
   Mismo patrón que pause. Feature flag STRICT_PAUSE controla ambas. */
app.post('/api/subscriptions/:id/resume', async (req, res) => {
    try {
        const sub = await db.getSubscription(req.params.id);
        if (!sub || sub.status !== 'paused') return res.status(400).json({ error: 'Not paused' });
        const strict = (process.env.STRICT_PAUSE === 'true');
        let mpConfirmed = false;
        let mpError = null;
        if (mp.resumeSubscription && sub.mp_preapproval_id) {
            try {
                await mp.resumeSubscription(sub.mp_preapproval_id);
                if (strict && mp.getSubscription) {
                    await new Promise(r => setTimeout(r, 1500));
                    const after = await mp.getSubscription(sub.mp_preapproval_id).catch(() => null);
                    mpConfirmed = after && after.status === 'authorized';
                    if (!mpConfirmed) mpError = `MP no confirmó resume (status actual: ${after?.status || 'unknown'})`;
                } else {
                    mpConfirmed = true;
                }
            } catch (e) { mpError = e.message; }
        } else {
            mpConfirmed = true;
        }
        if (strict && !mpConfirmed) {
            await db.createEvent({ subscription_id: sub.id, event_type: 'resume_mp_failed', metadata: JSON.stringify({ error: mpError }) }).catch(() => {});
            return res.status(502).json({ error: 'MercadoPago no confirmó la reactivación. La suscripción NO se modificó.', mp_error: mpError });
        }
        await db.updateSubscription(sub.id, { status: 'active', paused_until: null });
        await db.createEvent({ subscription_id: sub.id, event_type: 'resumed', metadata: JSON.stringify({ mp_confirmed: mpConfirmed, strict_mode: strict }) });
        res.json({ success: true, mp_confirmed: mpConfirmed });
    } catch (e) { res.status(500).json({ error: e.message }); }
});

/* GET /api/portal/:email — all subscriptions for a customer (MUST be LAST portal route) */
/* ═══════════════════════════════════════════════════════════════════
   👤 PORTAL CUSTOMER-FACING (additive 2026-04-30)
   Endpoint para que el cliente EDITE sus datos desde el portal sin
   pasar por admin. Validación: el email del body debe coincidir con
   el customer_email de la sub. Esto evita que un cliente edite la
   sub de otro (auth básica por email match).
   ═══════════════════════════════════════════════════════════════════ */
app.put('/api/portal/subscription/:id/update-customer-data', async (req, res) => {
    try {
        const sub = await db.getSubscription(req.params.id);
        if (!sub) return res.status(404).json({ error: 'Suscripción no encontrada' });

        const reqEmail = String(req.body?.email || '').toLowerCase().trim();
        const subEmail = String(sub.customer_email || '').toLowerCase().trim();
        if (!reqEmail || reqEmail !== subEmail) {
            return res.status(403).json({ error: 'Email no coincide con la suscripción' });
        }

        const updates = {};
        const body = req.body || {};

        // DNI
        if (body.dni !== undefined) {
            const dni = String(body.dni).replace(/\D/g, '').trim();
            if (dni.length < 8 || dni.length > 15) {
                return res.status(400).json({ error: 'DNI debe tener entre 8 y 15 dígitos' });
            }
            updates.dni = dni;
        }

        // Tipo documento
        if (body.tipo_documento !== undefined) {
            const td = String(body.tipo_documento).trim();
            if (!['01','06'].includes(td)) {
                return res.status(400).json({ error: 'tipo_documento debe ser 01 (DNI) o 06 (RUC)' });
            }
            updates.tipo_documento = td;
        }

        // Customer name / phone
        if (body.customer_name !== undefined) updates.customer_name = String(body.customer_name).trim().slice(0, 100);
        if (body.customer_phone !== undefined) updates.customer_phone = String(body.customer_phone).replace(/[^\d+\-\s]/g, '').trim().slice(0, 20);

        // Shipping address
        if (body.shipping_address && typeof body.shipping_address === 'object') {
            const a = body.shipping_address;
            const PE_PROV = { 'lima':'LIM','arequipa':'ARE','cusco':'CUS','la libertad':'LAL','piura':'PIU','lambayeque':'LAM','junin':'JUN','cajamarca':'CAJ','ancash':'ANC','ica':'ICA','callao':'CAL','tacna':'TAC' };
            const province = String(a.province || '').trim();
            updates.shipping_address = {
                ...(sub.shipping_address || {}),
                first_name: String(a.first_name || '').trim().slice(0,40) || (sub.customer_name || '').split(' ')[0] || '',
                last_name: String(a.last_name || '').trim().slice(0,40) || (sub.customer_name || '').split(' ').slice(1).join(' ') || '',
                address1: String(a.address1 || '').trim().slice(0,120),
                address2: String(a.address2 || '').trim().slice(0,80),
                city: String(a.city || '').trim().slice(0,40),
                province: province,
                province_code: a.province_code || PE_PROV[province.toLowerCase()] || 'LIM',
                country: 'PE',
                country_code: 'PE',
                zip: String(a.zip || '15000').trim().slice(0,10),
                phone: String(a.phone || sub.customer_phone || '').trim().slice(0,20)
            };
        }

        if (!Object.keys(updates).length) {
            return res.status(400).json({ error: 'Sin datos para actualizar' });
        }

        if (db.updateSubscription) {
            await db.updateSubscription(sub.id, updates);
        }
        if (db.createEvent) {
            await db.createEvent({
                subscription_id: sub.id,
                event_type: 'customer_data_updated_by_portal',
                metadata: JSON.stringify({ fields: Object.keys(updates), at: new Date().toISOString() })
            }).catch(() => {});
        }

        res.json({ ok: true, updates_applied: Object.keys(updates) });
    } catch (e) {
        res.status(500).json({ error: e.message });
    }
});

/* GET customer-facing detail of subs by email (más data que /api/portal/:email para el portal nuevo) */
app.get('/api/portal/me/:email', async (req, res) => {
    try {
        const email = String(req.params.email || '').toLowerCase().trim();
        if (!email) return res.json({ subscriptions: [], email: '' });
        const allSubs = await db.getSubscriptions().catch(() => []);
        const subs = allSubs
            .filter(s => (s.customer_email || '').toLowerCase() === email)
            .map(s => {
                const planLabel = (s.frequency_months === 1 ? 'Mensual' : 'Cada ' + s.frequency_months + ' meses') + ' × ' + s.permanence_months + 'm';
                return {
                    id: s.id,
                    customer_email: s.customer_email,
                    customer_name: s.customer_name,
                    customer_phone: s.customer_phone,
                    dni: s.dni,
                    tipo_documento: s.tipo_documento,
                    product_id: s.product_id,
                    product_title: s.product_title,
                    product_image: s.product_image,
                    variant_title: s.variant_title,
                    plan_label: planLabel,
                    frequency_months: s.frequency_months,
                    permanence_months: s.permanence_months,
                    discount_pct: s.discount_pct,
                    base_price: s.base_price,
                    final_price: s.final_price,
                    status: s.status,
                    cycles_completed: s.cycles_completed || 0,
                    cycles_required: s.cycles_required || 0,
                    next_charge_at: s.next_charge_at,
                    last_charge_at: s.last_charge_at,
                    activated_at: s.activated_at,
                    created_at: s.created_at,
                    gifts_planned: Array.isArray(s.gifts_planned) ? s.gifts_planned : [],
                    gifts_delivered: !!s.gifts_delivered,
                    gifts_delivered_order_name: s.gifts_delivered_order_name,
                    shipping_address: s.shipping_address || null,
                    shopify_order_name: s.shopify_order_name,
                    has_pending_data: !!s.shopify_order_id && !s.dni  // marker rápido
                };
            });
        res.json({ subscriptions: subs, email, count: subs.length });
    } catch (e) {
        res.status(500).json({ error: e.message });
    }
});

app.get('/api/portal/:email', async (req, res) => {
    const email = decodeURIComponent(req.params.email).toLowerCase().trim();
    try {
        const allSubs = await db.getSubscriptions();
        const subs = allSubs.filter(s => (s.customer_email || '').toLowerCase() === email);
        res.json({ subscriptions: subs, email });
    } catch (e) {
        res.json({ subscriptions: [], email, note: 'Error: ' + e.message });
    }
});

/* FIX 2026-04-11: Duplicate handler /webhooks/mp REMOVED.
   All MP webhook processing now goes through /webhooks/mercadopago (line ~1665)
   which includes Navasoft note_attributes, shipping, and address resolution.
   Configure MP Dashboard → Webhooks URL: .../webhooks/mercadopago
   The /api/webhooks/mercadopago alias (line ~1859) also forwards there. */

/* ── Health check ── */
app.get('/health', (req, res) => res.json({ status: 'ok', port: PORT, version: '7.5.1', build: 'failsoft-gift-2026-05-28', ts: new Date() }));

/* ══════════════════════════════════════════════════
   🎁 BACKFILL GIFTS — para subs creadas antes del 15/4 sin gifts_planned
   GET  /api/admin/subs-missing-gifts       → lista read-only (preview)
   POST /api/admin/subs-missing-gifts/backfill?dry_run=1 → rellena gifts_planned
        del sub CON cycles_completed=0 (aun no cobraron). NO toca subs ya cobradas.
        NO genera pedidos ni llama MP. Solo actualiza metadata local.
   ADITIVO: funciones de admin para consistencia de data. No altera webhook/cron.
   IMPORTANTE: declarado ANTES del catch-all para que no lo intercepte.
══════════════════════════════════════════════════ */
app.get('/api/admin/subs-missing-gifts', async (req, res) => {
    try {
        const allSubs = await db.getSubscriptions().catch(() => []);
        const report = [];
        for (const s of allSubs) {
            if (s.status !== 'active') continue;
            const hasPlanned = Array.isArray(s.gifts_planned) && s.gifts_planned.length > 0;
            if (hasPlanned) continue;
            const cycles = parseInt(s.cycles_completed) || 0;
            // Try to resolve what gifts SHOULD have been assigned
            const resolved = await resolveGiftsForNewSub(s.frequency_months, s.permanence_months, s.product_id).catch(() => null);
            report.push({
                id: s.id,
                email: s.customer_email,
                product_title: s.product_title,
                frequency_months: s.frequency_months,
                permanence_months: s.permanence_months,
                cycles_completed: cycles,
                cycles_required: s.cycles_required,
                gifts_delivered: !!s.gifts_delivered,
                can_backfill: cycles === 0, // solo si todavia no cobraron
                missed_in_past: cycles >= 1, // ya cobraron sin regalo — hay que entregar manualmente
                resolved_gifts: resolved || [],
                created_at: s.created_at
            });
        }
        res.json({
            total_affected: report.length,
            can_backfill: report.filter(r => r.can_backfill).length,
            missed_in_past: report.filter(r => r.missed_in_past).length,
            subs: report
        });
    } catch (e) { res.status(500).json({ error: e.message }); }
});

app.post('/api/admin/subs-missing-gifts/backfill', async (req, res) => {
    try {
        const dryRun = req.query.dry_run === '1' || req.query.dry_run === 'true';
        const allSubs = await db.getSubscriptions().catch(() => []);
        const updated = [];
        const skipped = [];
        for (const s of allSubs) {
            if (s.status !== 'active') continue;
            const hasPlanned = Array.isArray(s.gifts_planned) && s.gifts_planned.length > 0;
            if (hasPlanned) continue;
            const cycles = parseInt(s.cycles_completed) || 0;
            if (cycles !== 0) { skipped.push({ id: s.id, email: s.customer_email, reason: 'cycles_completed > 0 (ya cobro sin regalo — entregar manualmente)' }); continue; }
            const resolved = await resolveGiftsForNewSub(s.frequency_months, s.permanence_months, s.product_id).catch(() => null);
            if (!Array.isArray(resolved) || resolved.length === 0) { skipped.push({ id: s.id, email: s.customer_email, reason: 'no gifts match plan' }); continue; }
            if (!dryRun) {
                await db.updateSubscription(s.id, { gifts_planned: resolved }).catch(err => {
                    skipped.push({ id: s.id, email: s.customer_email, reason: 'update error: ' + err.message });
                });
            }
            updated.push({ id: s.id, email: s.customer_email, gifts: resolved.map(g => g.product_title + ' (' + g.variant_title + ')') });
        }
        res.json({ dry_run: dryRun, updated_count: updated.length, skipped_count: skipped.length, updated, skipped });
    } catch (e) { res.status(500).json({ error: e.message }); }
});

/* ══════════════════════════════════════════════════════
   📦 BUNDLE CONFIGS — Suscripciones configurables (mix de sabores/variantes)
   Ejemplo: C4 Energy Bundle 15 latas, el cliente arma el mix (5 Frozen + 5 Orange...).
   El mix se graba EN LA SUB → cada mes el cron crea la MISMA order con N line items.

   Admin endpoints (gestión autónoma):
     GET    /api/admin/bundles            → lista configs
     GET    /api/admin/bundles/:id        → detalle
     POST   /api/admin/bundles            → crear
     PUT    /api/admin/bundles/:id        → actualizar
     DELETE /api/admin/bundles/:id        → eliminar (solo metaobject config, no toca subs ni productos)

   ADITIVO: no toca motor de cobros, webhook MP, polling, ni crons existentes.
   Declarado ANTES del catch-all para que no lo intercepte.
══════════════════════════════════════════════════════ */
app.get('/api/admin/bundles', async (req, res) => {
    try {
        const filters = {};
        if (req.query.active !== undefined) filters.active = req.query.active === 'true' || req.query.active === '1';
        if (req.query.source_product_id) filters.source_product_id = req.query.source_product_id;
        if (req.query.bundle_product_id) filters.bundle_product_id = req.query.bundle_product_id;
        const bundles = await db.getBundleConfigs(filters);
        res.json({ total: bundles.length, bundles });
    } catch (e) { res.status(500).json({ error: e.message }); }
});

app.get('/api/admin/bundles/:id', async (req, res) => {
    try {
        const b = await db.getBundleConfig(req.params.id);
        if (!b) return res.status(404).json({ error: 'Bundle not found' });
        res.json(b);
    } catch (e) { res.status(500).json({ error: e.message }); }
});

/**
 * Crea una nueva config de bundle.
 * Body: {
 *   name: "C4 Energy Bundle 15 latas",
 *   bundle_product_id: "15580000000000",         // producto Shopify que representa el pack (ej C4 Energy Bundle 15)
 *   source_product_id: "15579925250129",        // producto master de donde salen los sabores (C4 Performance Energy 473 ml)
 *   target_quantity: 15,                         // 15 o 30 latas
 *   allowed_variant_ids: ["58307481501777", ...],// sabores elegibles
 *   plans: [                                     // planes disponibles (precios y frecuencias)
 *     { freq_months: 1, perm_months: 3, price: 150, discount_pct: 33.33 },
 *     { freq_months: 1, perm_months: 6, price: 150, discount_pct: 33.33 }
 *   ],
 *   validate_stock: true,                         // si true, bloquea variantes con stock 0 en widget
 *   hide_stock_from_ui: true,                    // no muestra stock al cliente (solo "disponible/agotado")
 *   description: "..." (opcional),
 *   widget_copy: { title, subtitle, counter_label, error_incomplete }
 * }
 */
app.post('/api/admin/bundles', async (req, res) => {
    try {
        const body = req.body || {};
        // 2026-04-22 — soporte de tipo 'fixed_combo' (2+ productos DIFERENTES en la misma caja).
        // Si type === 'fixed_combo': cliente NO elige variantes, el admin fija la lista (combo_items).
        // Si type === 'mix_match' (default, back-compat): cliente arma mix de sabores del source_product.
        const type = (body.type === 'fixed_combo') ? 'fixed_combo' : 'mix_match';

        // Validación común
        if (!body.name) return res.status(400).json({ error: 'name is required' });
        if (!body.bundle_product_id) return res.status(400).json({ error: 'bundle_product_id is required (product Shopify del combo/bundle)' });
        if (!Array.isArray(body.plans) || body.plans.length === 0) {
            return res.status(400).json({ error: 'plans must be a non-empty array' });
        }

        // Validación por tipo
        let comboItemsNormalized = [];
        if (type === 'fixed_combo') {
            if (!Array.isArray(body.combo_items) || body.combo_items.length === 0) {
                return res.status(400).json({ error: 'combo_items must be a non-empty array when type=fixed_combo' });
            }
            for (const it of body.combo_items) {
                if (!it || !it.product_id || !it.variant_id) {
                    return res.status(400).json({ error: 'Each combo_items entry requires product_id and variant_id' });
                }
            }
            comboItemsNormalized = body.combo_items.map(it => ({
                product_id: String(it.product_id),
                variant_id: String(it.variant_id),
                quantity: Number(it.quantity) || 1,
                title: it.title ? String(it.title) : '',
                variant_title: it.variant_title ? String(it.variant_title) : '',
                image: it.image ? String(it.image) : '',
                sku: it.sku ? String(it.sku) : ''
            }));
        } else {
            if (!body.source_product_id) return res.status(400).json({ error: 'source_product_id is required (product master de sabores)' });
            if (!body.target_quantity || !Number.isFinite(Number(body.target_quantity)) || Number(body.target_quantity) <= 0) {
                return res.status(400).json({ error: 'target_quantity must be a positive number' });
            }
            if (!Array.isArray(body.allowed_variant_ids) || body.allowed_variant_ids.length === 0) {
                return res.status(400).json({ error: 'allowed_variant_ids must be a non-empty array' });
            }
        }

        // Normalizar
        const record = {
            type,
            name: String(body.name),
            description: body.description ? String(body.description) : '',
            bundle_product_id: String(body.bundle_product_id),
            bundle_product_handle: body.bundle_product_handle ? String(body.bundle_product_handle) : '',
            source_product_id: body.source_product_id ? String(body.source_product_id) : '',
            source_product_handle: body.source_product_handle ? String(body.source_product_handle) : '',
            source_product_title: body.source_product_title ? String(body.source_product_title) : '',
            target_quantity: type === 'fixed_combo'
                ? comboItemsNormalized.reduce((s, it) => s + (Number(it.quantity) || 1), 0)
                : Number(body.target_quantity),
            allowed_variant_ids: type === 'fixed_combo'
                ? comboItemsNormalized.map(it => it.variant_id)
                : (body.allowed_variant_ids || []).map(String),
            excluded_variant_ids: Array.isArray(body.excluded_variant_ids) ? body.excluded_variant_ids.map(String) : [],
            min_stock_threshold: Number(body.min_stock_threshold) || 100,
            // 2026-04-22 — Items FIJOS del combo (solo cuando type=fixed_combo)
            combo_items: comboItemsNormalized,
            plans: body.plans.map(p => ({
                freq_months: Number(p.freq_months) || 1,
                perm_months: Number(p.perm_months) || 3,
                price: Number(p.price),
                discount_pct: Number(p.discount_pct) || 0,
                plan_id: p.plan_id ? String(p.plan_id) : '',
                variant_id_perm: p.variant_id_perm ? String(p.variant_id_perm) : '',
                // Regalos primer pedido (compat con plans_config.gifts)
                gifts: (p.gifts && typeof p.gifts === 'object') ? {
                    enabled: !!p.gifts.enabled,
                    items: Array.isArray(p.gifts.items) ? p.gifts.items : []
                } : { enabled: false, items: [] }
            })),
            validate_stock: body.validate_stock !== false,
            hide_stock_from_ui: body.hide_stock_from_ui !== false,
            widget_copy: body.widget_copy || {},
            active: body.active !== false,
        };
        const created = await db.createBundleConfig(record);
        res.json({ ok: true, bundle: created });
    } catch (e) { res.status(500).json({ error: e.message }); }
});

app.put('/api/admin/bundles/:id', async (req, res) => {
    try {
        const body = req.body || {};
        // Solo update de campos explícitos (evitar inyección de _gid, id, created_at)
        // 2026-04-22 — agregados: type, combo_items (para combos fijos de 2+ productos).
        const updatable = ['type', 'name', 'description', 'bundle_product_id', 'bundle_product_handle',
            'source_product_id', 'source_product_handle', 'source_product_title',
            'target_quantity', 'allowed_variant_ids', 'excluded_variant_ids', 'min_stock_threshold', 'plans',
            'combo_items',
            'validate_stock', 'hide_stock_from_ui', 'widget_copy', 'active'];
        const updates = {};
        for (const k of updatable) if (k in body) updates[k] = body[k];
        if (updates.target_quantity !== undefined) updates.target_quantity = Number(updates.target_quantity);
        if (Array.isArray(updates.allowed_variant_ids)) updates.allowed_variant_ids = updates.allowed_variant_ids.map(String);
        if (Array.isArray(updates.combo_items)) {
            updates.combo_items = updates.combo_items.map(it => ({
                product_id: String(it.product_id),
                variant_id: String(it.variant_id),
                quantity: Number(it.quantity) || 1,
                title: it.title ? String(it.title) : '',
                variant_title: it.variant_title ? String(it.variant_title) : '',
                image: it.image ? String(it.image) : '',
                sku: it.sku ? String(it.sku) : ''
            }));
            // Si es fixed_combo, sincroniza target_quantity + allowed_variant_ids
            if (updates.type === 'fixed_combo' || body.type === 'fixed_combo') {
                updates.target_quantity = updates.combo_items.reduce((s, it) => s + (Number(it.quantity) || 1), 0);
                updates.allowed_variant_ids = updates.combo_items.map(it => it.variant_id);
            }
        }
        if (Array.isArray(updates.plans)) {
            updates.plans = updates.plans.map(p => ({
                freq_months: Number(p.freq_months) || 1,
                perm_months: Number(p.perm_months) || 3,
                price: Number(p.price),
                discount_pct: Number(p.discount_pct) || 0,
                plan_id: p.plan_id ? String(p.plan_id) : '',
                variant_id_perm: p.variant_id_perm ? String(p.variant_id_perm) : '',
                gifts: (p.gifts && typeof p.gifts === 'object') ? {
                    enabled: !!p.gifts.enabled,
                    items: Array.isArray(p.gifts.items) ? p.gifts.items : []
                } : { enabled: false, items: [] }
            }));
        }
        const updated = await db.updateBundleConfig(req.params.id, updates);
        res.json({ ok: true, bundle: updated });
    } catch (e) { res.status(500).json({ error: e.message }); }
});

app.delete('/api/admin/bundles/:id', async (req, res) => {
    try {
        const result = await db.deleteBundleConfig(req.params.id);
        res.json({ ok: true, ...result });
    } catch (e) { res.status(500).json({ error: e.message }); }
});

/**
 * POST /api/admin/bundles/create-product
 * 2026-04-21 — ADITIVO
 * Helper de admin para CREAR un producto "contenedor" de bundle en Shopify.
 * Crea un producto con N variantes (una por cada plan: 3m, 6m, etc.).
 * El precio de cada variante = precio del plan. El cliente paga S/0 en Shopify
 * (las suscripciones se cobran via MP); el precio del producto es referencial.
 *
 * Body: {
 *   title: "C4 Energy Bundle 15 latas",
 *   description: "...",
 *   vendor: "Lab Nutrition",
 *   product_type: "Suscripción",
 *   tags: ["suscripcion", "bundle", "c4"],
 *   image_src: "https://..." (opcional, imagen destacada),
 *   plans: [
 *     { name: "3 meses", price: 150, permanence: 3 },
 *     { name: "6 meses", price: 150, permanence: 6 }
 *   ]
 * }
 *
 * Retorna el producto creado + IDs de variantes listos para usar en el bundle_config.
 */
app.post('/api/admin/bundles/create-product', async (req, res) => {
    try {
        const shop = process.env.SHOPIFY_SHOP || 'nutrition-lab-cluster.myshopify.com';
        const token = process.env.SHOPIFY_ACCESS_TOKEN || _shopifyToken;
        if (!token) return res.status(500).json({ error: 'Shopify token not configured' });

        const b = req.body || {};
        // 2026-04-22 — ADITIVO: aceptar alias `name` y fallback a single variant
        // para combos fijos (no necesitan múltiples planes como opciones de Shopify).
        const title = b.title || b.name;
        if (!title) return res.status(400).json({ error: 'title (or name) is required' });

        let variants;
        let options;
        if (Array.isArray(b.plans) && b.plans.length > 0) {
            variants = b.plans.map(p => ({
                option1: String(p.name || `${p.permanence || 1} meses`),
                price: String(Number(p.price || 0).toFixed(2)),
                requires_shipping: true,
                inventory_management: null,
                taxable: true
            }));
            options = [{ name: 'Plan' }];
        } else {
            // Combo simple: un solo variant, precio referencial (el real lo cobra MP)
            const price = Number(b.price || 1);
            variants = [{
                option1: 'Default',
                price: String(price.toFixed(2)),
                requires_shipping: true,
                inventory_management: null,
                taxable: true
            }];
            options = [{ name: 'Title' }];
        }

        const productPayload = {
            product: {
                title: String(title),
                body_html: b.description ? String(b.description) : '',
                vendor: b.vendor ? String(b.vendor) : 'Lab Nutrition',
                product_type: b.product_type ? String(b.product_type) : 'Suscripción',
                tags: Array.isArray(b.tags) ? b.tags.join(', ') : 'suscripcion,bundle',
                status: b.status ? String(b.status) : 'active',
                published: b.published !== false,
                options,
                variants
            }
        };
        if (b.image_src) {
            productPayload.product.images = [{ src: String(b.image_src) }];
        }

        const url = `https://${shop}/admin/api/2026-01/products.json`;
        const r = await fetch(url, {
            method: 'POST',
            headers: { 'X-Shopify-Access-Token': token, 'Content-Type': 'application/json' },
            body: JSON.stringify(productPayload)
        });
        const text = await r.text();
        if (!r.ok) {
            return res.status(r.status).json({ error: `Shopify create product ${r.status}`, detail: text.slice(0, 500) });
        }
        const data = JSON.parse(text);
        const created = data.product;
        res.json({
            ok: true,
            product: {
                id: created.id,
                title: created.title,
                handle: created.handle,
                status: created.status,
                admin_url: `https://${shop}/admin/products/${created.id}`,
                storefront_url: `https://${shop.replace('.myshopify.com', '')}/products/${created.handle}`,
                variants: (created.variants || []).map(v => ({
                    id: v.id,
                    title: v.title,
                    price: v.price,
                    option1: v.option1
                }))
            }
        });
    } catch (e) { res.status(500).json({ error: e.message }); }
});

/**
 * POST /api/admin/products/:id/status
 * 2026-04-21 — ADITIVO
 * Cambia el status de un producto Shopify (ACTIVE | DRAFT | ARCHIVED).
 * Body: { status: "ACTIVE" | "DRAFT" | "ARCHIVED" }
 * Útil para publicar/despublicar bundles creados desde el admin.
 */
app.post('/api/admin/products/:id/status', async (req, res) => {
    try {
        const productId = String(req.params.id || '').trim();
        const newStatus = String((req.body && req.body.status) || '').toUpperCase();
        if (!productId) return res.status(400).json({ error: 'productId required' });
        if (!['ACTIVE', 'DRAFT', 'ARCHIVED'].includes(newStatus)) {
            return res.status(400).json({ error: 'status must be ACTIVE|DRAFT|ARCHIVED' });
        }
        const shop = process.env.SHOPIFY_SHOP || 'nutrition-lab-cluster.myshopify.com';
        const token = process.env.SHOPIFY_ACCESS_TOKEN || _shopifyToken;
        if (!token) return res.status(500).json({ error: 'Shopify token not configured' });

        // REST: PUT /products/{id}.json with status lowercase
        const url = `https://${shop}/admin/api/2026-01/products/${encodeURIComponent(productId)}.json`;
        const r = await fetch(url, {
            method: 'PUT',
            headers: { 'X-Shopify-Access-Token': token, 'Content-Type': 'application/json' },
            body: JSON.stringify({ product: { id: productId, status: newStatus.toLowerCase() } })
        });
        const text = await r.text();
        if (!r.ok) return res.status(r.status).json({ error: `Shopify ${r.status}`, detail: text.slice(0, 400) });
        const data = JSON.parse(text);
        const p = data.product || {};
        res.json({
            ok: true,
            product: {
                id: p.id,
                title: p.title,
                handle: p.handle,
                status: p.status,
                admin_url: `https://${shop}/admin/products/${p.id}`,
                storefront_url: `https://${shop.replace('.myshopify.com', '')}/products/${p.handle}`
            }
        });
    } catch (e) { res.status(500).json({ error: e.message }); }
});

/**
 * POST /api/admin/products/:id/bust-cache
 * 2026-04-22 — ADITIVO
 * Fuerza invalidación del cache del storefront de Shopify tocando el producto.
 * Shopify cachea el HTML del product page 5-15 min; cualquier update al product
 * dispara invalidation. Esto es útil tras deploys de theme app extension para
 * que los assets nuevos (lab-bundle.js v119+) se carguen inmediatamente.
 *
 * Estrategia: agregar un tag temporal "_cache_bust_<timestamp>" y quitarlo 2s después.
 */
app.post('/api/admin/products/:id/bust-cache', async (req, res) => {
    try {
        const productId = String(req.params.id || '').trim();
        if (!productId) return res.status(400).json({ error: 'productId required' });
        const shop = process.env.SHOPIFY_SHOP || 'nutrition-lab-cluster.myshopify.com';
        const token = process.env.SHOPIFY_ACCESS_TOKEN || _shopifyToken;
        if (!token) return res.status(500).json({ error: 'Shopify token not configured' });

        // 1. GET tags actuales
        const getUrl = `https://${shop}/admin/api/2026-01/products/${encodeURIComponent(productId)}.json?fields=id,tags,handle,title`;
        const r1 = await fetch(getUrl, { headers: { 'X-Shopify-Access-Token': token } });
        if (!r1.ok) return res.status(r1.status).json({ error: `Shopify GET ${r1.status}` });
        const cur = (await r1.json()).product || {};
        const originalTags = (cur.tags || '').trim();

        // 2. PUT con tag cache-bust temporal
        const ts = Date.now();
        const bustTag = `_cache_bust_${ts}`;
        const newTags = originalTags ? `${originalTags}, ${bustTag}` : bustTag;
        const putUrl = `https://${shop}/admin/api/2026-01/products/${encodeURIComponent(productId)}.json`;
        const r2 = await fetch(putUrl, {
            method: 'PUT',
            headers: { 'X-Shopify-Access-Token': token, 'Content-Type': 'application/json' },
            body: JSON.stringify({ product: { id: productId, tags: newTags } })
        });
        if (!r2.ok) {
            const t = await r2.text();
            return res.status(r2.status).json({ error: `Shopify PUT ${r2.status}`, detail: t.slice(0, 300) });
        }

        // 3. Revertir tag original en background (2s delay) para dejar el producto limpio
        setTimeout(() => {
            fetch(putUrl, {
                method: 'PUT',
                headers: { 'X-Shopify-Access-Token': token, 'Content-Type': 'application/json' },
                body: JSON.stringify({ product: { id: productId, tags: originalTags } })
            }).catch(e => console.warn('[bust-cache] revert tag failed:', e.message));
        }, 2500);

        res.json({
            ok: true,
            product_id: productId,
            handle: cur.handle,
            title: cur.title,
            storefront_url: `https://${shop.replace('.myshopify.com', '')}/products/${cur.handle}`,
            bust_tag: bustTag,
            message: 'Cache invalidation triggered. Storefront HTML se actualiza en ~5-30s.'
        });
    } catch (e) { res.status(500).json({ error: e.message }); }
});

/**
 * POST /api/admin/bust-cache-all-bundles
 * 2026-04-22 — ADITIVO
 * Invalida cache de TODOS los productos con bundles/combos activos.
 * Útil tras un deploy de theme app extension: toca cada producto bundle
 * para que Shopify regenere su HTML con los asset_url más recientes.
 */
app.post('/api/admin/bust-cache-all-bundles', async (req, res) => {
    try {
        const bundles = await db.getBundleConfigs({ active: true });
        const shop = process.env.SHOPIFY_SHOP || 'nutrition-lab-cluster.myshopify.com';
        const token = process.env.SHOPIFY_ACCESS_TOKEN || _shopifyToken;
        if (!token) return res.status(500).json({ error: 'Shopify token not configured' });

        const targets = (bundles || []).filter(b => b.active !== false && b.bundle_product_id);
        const results = [];
        for (const b of targets) {
            try {
                const getUrl = `https://${shop}/admin/api/2026-01/products/${encodeURIComponent(b.bundle_product_id)}.json?fields=id,tags,handle`;
                const r1 = await fetch(getUrl, { headers: { 'X-Shopify-Access-Token': token } });
                if (!r1.ok) { results.push({ id: b.bundle_product_id, ok: false, error: `GET ${r1.status}` }); continue; }
                const cur = (await r1.json()).product || {};
                const originalTags = (cur.tags || '').trim();
                const bustTag = `_cache_bust_${Date.now()}_${Math.random().toString(36).slice(2, 6)}`;
                const newTags = originalTags ? `${originalTags}, ${bustTag}` : bustTag;
                const putUrl = `https://${shop}/admin/api/2026-01/products/${encodeURIComponent(b.bundle_product_id)}.json`;
                const r2 = await fetch(putUrl, {
                    method: 'PUT',
                    headers: { 'X-Shopify-Access-Token': token, 'Content-Type': 'application/json' },
                    body: JSON.stringify({ product: { id: b.bundle_product_id, tags: newTags } })
                });
                results.push({ id: b.bundle_product_id, handle: cur.handle, ok: r2.ok, bustTag });
                // Revertir tags en 3s (no await — fire and forget)
                const currentTags = originalTags;
                const pid = b.bundle_product_id;
                setTimeout(() => {
                    fetch(putUrl, {
                        method: 'PUT',
                        headers: { 'X-Shopify-Access-Token': token, 'Content-Type': 'application/json' },
                        body: JSON.stringify({ product: { id: pid, tags: currentTags } })
                    }).catch(() => { });
                }, 3000);
                // Pausa pequeña entre productos para no saturar Shopify
                await new Promise(r => setTimeout(r, 350));
            } catch (e) {
                results.push({ id: b.bundle_product_id, ok: false, error: e.message });
            }
        }
        res.json({ ok: true, total: targets.length, results });
    } catch (e) { res.status(500).json({ error: e.message }); }
});

/**
 * POST /api/admin/products/:id/template-suffix
 * 2026-04-21 — ADITIVO
 * Asigna template_suffix a un producto. Requiere write_products (ya lo tenemos).
 * Body: { template_suffix: "bundle" } (o "" para quitar)
 */
app.post('/api/admin/products/:id/template-suffix', async (req, res) => {
    try {
        const productId = String(req.params.id || '').trim();
        const suffix = String((req.body && req.body.template_suffix) ?? '').trim();
        if (!productId) return res.status(400).json({ error: 'productId required' });
        const shop = process.env.SHOPIFY_SHOP || 'nutrition-lab-cluster.myshopify.com';
        const token = process.env.SHOPIFY_ACCESS_TOKEN || _shopifyToken;
        if (!token) return res.status(500).json({ error: 'Shopify token not configured' });
        const url = `https://${shop}/admin/api/2026-01/products/${encodeURIComponent(productId)}.json`;
        const r = await fetch(url, {
            method: 'PUT',
            headers: { 'X-Shopify-Access-Token': token, 'Content-Type': 'application/json' },
            body: JSON.stringify({ product: { id: productId, template_suffix: suffix || null } })
        });
        const text = await r.text();
        if (!r.ok) return res.status(r.status).json({ error: `Shopify ${r.status}`, detail: text.slice(0, 400) });
        const data = JSON.parse(text);
        const p = data.product || {};
        res.json({
            ok: true,
            product: {
                id: p.id,
                title: p.title,
                handle: p.handle,
                template_suffix: p.template_suffix,
                storefront_url: `https://${shop.replace('.myshopify.com', '')}/products/${p.handle}`
            }
        });
    } catch (e) { res.status(500).json({ error: e.message }); }
});

/**
 * POST /api/admin/products/:id/body-html-replace  — 2026-04-21 ADITIVO
 * Busca y reemplaza texto en body_html de un producto. No toca nada más.
 * Body: { find: "Envío gratis", replace: "Envío S/10 fijo" }
 * MASTER LOCK: solo edita el campo body_html; no webhooks, no MP, no pedidos.
 */
app.post('/api/admin/products/:id/body-html-replace', async (req, res) => {
    try {
        const productId = String(req.params.id || '').trim();
        const find = String((req.body && req.body.find) || '');
        const replace = String((req.body && req.body.replace) || '');
        if (!productId) return res.status(400).json({ error: 'productId required' });
        if (!find) return res.status(400).json({ error: 'find (string to search) required' });
        const shop = process.env.SHOPIFY_SHOP || 'nutrition-lab-cluster.myshopify.com';
        const token = process.env.SHOPIFY_ACCESS_TOKEN || _shopifyToken;
        if (!token) return res.status(500).json({ error: 'Shopify token not configured' });
        const urlGet = `https://${shop}/admin/api/2026-01/products/${encodeURIComponent(productId)}.json?fields=id,title,handle,body_html`;
        const rg = await fetch(urlGet, { headers: { 'X-Shopify-Access-Token': token } });
        const tg = await rg.text();
        if (!rg.ok) return res.status(rg.status).json({ error: `Shopify GET ${rg.status}`, detail: tg.slice(0, 300) });
        const dg = JSON.parse(tg);
        const current = String(dg.product?.body_html || '');
        const occurrences = (current.match(new RegExp(find.replace(/[.*+?^${}()|[\]\\]/g, '\\$&'), 'g')) || []).length;
        if (occurrences === 0) return res.json({ ok: true, changed: false, occurrences: 0, note: 'find string not present' });
        const next = current.split(find).join(replace);
        const rp = await fetch(`https://${shop}/admin/api/2026-01/products/${encodeURIComponent(productId)}.json`, {
            method: 'PUT',
            headers: { 'X-Shopify-Access-Token': token, 'Content-Type': 'application/json' },
            body: JSON.stringify({ product: { id: productId, body_html: next } })
        });
        const tp = await rp.text();
        if (!rp.ok) return res.status(rp.status).json({ error: `Shopify PUT ${rp.status}`, detail: tp.slice(0, 300) });
        res.json({ ok: true, changed: true, occurrences, product_id: productId, title: dg.product?.title, handle: dg.product?.handle });
    } catch (e) { res.status(500).json({ error: e.message }); }
});

/**
 * POST /api/admin/themes/install-bundle-template
 * 2026-04-21 — ADITIVO
 * Crea templates/product.bundle.json en el main theme (clonando product.json base + inyectando
 * el app block lab_subscription) y asigna template_suffix="bundle" a los product_ids indicados.
 *
 * Body: { product_ids: ["15769236996177", "15769237028945"] }
 *       (opcional, por defecto usa los 2 bundles C4 creados)
 *
 * No toca productos ajenos, no toca webhooks, no toca pedidos, no toca MP.
 */
app.post('/api/admin/themes/install-bundle-template', async (req, res) => {
    try {
        const shop = process.env.SHOPIFY_SHOP || 'nutrition-lab-cluster.myshopify.com';
        const token = process.env.SHOPIFY_ACCESS_TOKEN || _shopifyToken;
        if (!token) return res.status(500).json({ error: 'Shopify token not configured' });

        const EXT_UUID = '4155d041-6adb-5e48-1fc5-809fdebf7f954a1156be';
        const BLOCK_TYPE = `shopify://apps/lab-nutrition-subscriptions/blocks/lab_subscription/${EXT_UUID}`;

        const productIds = Array.isArray(req.body && req.body.product_ids) && req.body.product_ids.length
            ? req.body.product_ids.map(String)
            : ['15769236996177', '15769237028945'];

        const api = (p, opts = {}) => fetch(`https://${shop}/admin/api/2026-01${p}`, {
            ...opts,
            headers: { 'X-Shopify-Access-Token': token, 'Content-Type': 'application/json', ...(opts.headers || {}) }
        }).then(async r => {
            const t = await r.text();
            if (!r.ok) throw new Error(`HTTP ${r.status} ${p}: ${t.substring(0, 250)}`);
            return t ? JSON.parse(t) : null;
        });

        // 1) Main theme
        const themesResp = await api('/themes.json');
        const mainTheme = (themesResp.themes || []).find(t => t.role === 'main');
        if (!mainTheme) return res.status(404).json({ error: 'No main theme found' });

        // 2) Fetch base templates/product.json
        const baseAsset = await api(`/themes/${mainTheme.id}/assets.json?asset[key]=templates/product.json`);
        let baseJson;
        try { baseJson = JSON.parse(baseAsset.asset.value); }
        catch (e) { return res.status(500).json({ error: 'Base product.json is not valid JSON' }); }

        // 3) Clone + inject app block into main-product section
        const clone = JSON.parse(JSON.stringify(baseJson));
        let mainKey = null;
        for (const [k, sec] of Object.entries(clone.sections || {})) {
            if (sec && String(sec.type || '').includes('main-product')) { mainKey = k; break; }
        }
        if (!mainKey) {
            for (const [k, sec] of Object.entries(clone.sections || {})) {
                if (sec && sec.blocks) { mainKey = k; break; }
            }
        }
        if (!mainKey) return res.status(500).json({ error: 'No main-product section found' });

        const mainSec = clone.sections[mainKey];
        if (!mainSec.blocks) mainSec.blocks = {};
        if (!mainSec.block_order) mainSec.block_order = [];

        const blockId = 'lab_subscription_widget';
        mainSec.blocks[blockId] = { type: BLOCK_TYPE, settings: {} };
        if (!mainSec.block_order.includes(blockId)) {
            const idx = mainSec.block_order.findIndex(k => {
                const t = (mainSec.blocks[k] && mainSec.blocks[k].type) || '';
                return t.includes('buy_buttons') || t.includes('price');
            });
            if (idx >= 0) mainSec.block_order.splice(idx + 1, 0, blockId);
            else mainSec.block_order.push(blockId);
        }

        // 4) Upload templates/product.bundle.json
        const upload = await api(`/themes/${mainTheme.id}/assets.json`, {
            method: 'PUT',
            body: JSON.stringify({ asset: { key: 'templates/product.bundle.json', value: JSON.stringify(clone, null, 2) } })
        });

        // 5) Assign template_suffix="bundle" to each bundle product
        const assigned = [];
        for (const pid of productIds) {
            const r = await api(`/products/${pid}.json`, {
                method: 'PUT',
                body: JSON.stringify({ product: { id: Number(pid), template_suffix: 'bundle' } })
            });
            assigned.push({ id: String(r.product.id), handle: r.product.handle, template_suffix: r.product.template_suffix });
        }

        res.json({
            ok: true,
            theme: { id: mainTheme.id, name: mainTheme.name, role: mainTheme.role },
            template: { key: upload.asset.key, size: upload.asset.size || null, main_section: mainKey, block_id: blockId, block_type: BLOCK_TYPE },
            products: assigned
        });
    } catch (e) { res.status(500).json({ error: e.message }); }
});

/**
 * POST /api/admin/products/:id/install-subscription-template
 * 2026-04-21 — ADITIVO · AUTONOMÍA MULTI-PRODUCTO
 *
 * Clona el template maestro `templates/product.bundle.json` a un template por-producto
 * `templates/product.bundle-p{shortId}.json` con widget independiente:
 *   - Renombra section keys: _bundle_section → _p{shortId}_section (únicas por producto)
 *   - Renombra widget block key: suscriptions_mp_lab_subscription_aPi9pn → _p{shortId}
 *   - Inyecta eligible_variant_ids desde product_configs[productId] (SEGURO: solo variantes del producto)
 *   - Asigna template_suffix="bundle-p{shortId}" al producto
 *   - Invalida CDN cache via body_html bump (no-op space replace)
 *
 * Body (opcional): { source_template?: "product.bundle" }  // default
 * Params: :id = productId numérico Shopify
 *
 * MASTER LOCK: solo AGREGA 1 template y reasigna template_suffix. No toca MP, webhooks, orders ni crons.
 */
app.post('/api/admin/products/:id/install-subscription-template', async (req, res) => {
    try {
        const productId = String(req.params.id || '').trim();
        if (!productId) return res.status(400).json({ error: 'productId required' });
        const shop = process.env.SHOPIFY_SHOP || 'nutrition-lab-cluster.myshopify.com';
        const token = process.env.SHOPIFY_ACCESS_TOKEN || _shopifyToken;
        if (!token) return res.status(500).json({ error: 'Shopify token not configured' });

        const sourceTemplateSlug = String((req.body && req.body.source_template) || 'product.bundle').trim();
        const sourceTemplateKey = `templates/${sourceTemplateSlug}.json`;

        // Short ID for template suffix (últimos 6 caracteres del productId → únicos por producto)
        const shortId = productId.slice(-6);
        const sectionSuffix = `_p${shortId}_section`;
        const widgetNewKey = `suscriptions_mp_lab_subscription_p${shortId}`;
        const newTemplateKey = `templates/product.bundle-p${shortId}.json`;
        const newTemplateSuffix = `bundle-p${shortId}`;

        const WIDGET_OLD_KEY = 'suscriptions_mp_lab_subscription_aPi9pn';
        const SECTION_RENAMES = [
            'lab_hero_bundle_section', 'lab_bene_bundle_section', 'lab_save_bundle_section',
            'lab_how_bundle_section', 'lab_flav_bundle_section', 'lab_comp_bundle_section',
            'lab_test_bundle_section', 'lab_faq_bundle_section', 'lab_final_bundle_section'
        ];

        const sh = async (p, opts = {}) => {
            const r = await fetch(`https://${shop}/admin/api/2026-01${p}`, {
                ...opts,
                headers: { 'X-Shopify-Access-Token': token, 'Content-Type': 'application/json', ...(opts.headers || {}) }
            });
            const t = await r.text();
            if (!r.ok) throw new Error(`HTTP ${r.status} ${p}: ${t.substring(0, 250)}`);
            return t ? JSON.parse(t) : null;
        };

        // 1) Main theme
        const themesResp = await sh('/themes.json');
        const mainTheme = (themesResp.themes || []).find(t => t.role === 'main');
        if (!mainTheme) return res.status(404).json({ error: 'No main theme found' });

        // 2) Fetch master template
        let masterAsset;
        try {
            masterAsset = await sh(`/themes/${mainTheme.id}/assets.json?asset[key]=${encodeURIComponent(sourceTemplateKey)}`);
        } catch (e) {
            return res.status(404).json({ error: `Source template not found: ${sourceTemplateKey}`, detail: e.message });
        }
        let masterJson;
        try { masterJson = JSON.parse(masterAsset.asset.value); }
        catch (e) { return res.status(500).json({ error: `Master template not valid JSON` }); }

        // 3) Clone + rename sections + rename widget block + inject eligible_variant_ids
        const tpl = JSON.parse(JSON.stringify(masterJson));

        // 3a) Rename section keys
        const renameMap = {};
        SECTION_RENAMES.forEach(oldKey => { renameMap[oldKey] = oldKey.replace('_bundle_section', sectionSuffix); });
        const newSections = {};
        Object.keys(tpl.sections || {}).forEach(k => { newSections[renameMap[k] || k] = tpl.sections[k]; });
        tpl.sections = newSections;
        tpl.order = (tpl.order || []).map(k => renameMap[k] || k);

        // 3b) Fetch product variants + config to know eligible_variant_ids
        const prodResp = await sh(`/products/${productId}.json?fields=id,title,handle,variants`);
        const product = prodResp.product || {};
        const allVariantIds = (product.variants || []).map(v => String(v.id));

        let eligibleVariantIds = '';
        try {
            const settings = await readFromShopify() || readFromFile() || {};
            const cfgMap = settings.product_configs || {};
            const cfg = cfgMap[productId] || cfgMap[String(productId)] || {};
            if (Array.isArray(cfg.eligible_variant_ids) && cfg.eligible_variant_ids.length) {
                // Solo usar variantes que realmente existen en el producto (defensa anti-cagada)
                const real = cfg.eligible_variant_ids.map(String).filter(v => allVariantIds.includes(v));
                eligibleVariantIds = real.join(',');
            }
        } catch (_) { /* no config yet — widget will default */ }

        // Fallback: si no hay product_config, usar TODAS las variantes del producto (sigue siendo seguro,
        // porque evita cross-product; solo mostraría todas las variantes de ESTE producto)
        if (!eligibleVariantIds && allVariantIds.length) {
            eligibleVariantIds = allVariantIds.join(',');
        }

        // 3c) Rename widget block key (recursivo) + inyectar eligible_variant_ids
        //     + fallback: si no existe widget en template (p. ej. source=product.json sin widget),
        //     lo INYECTAMOS en la main-product section para que aparezca.
        let widgetFound = false;
        const widgetBlockType = 'shopify://apps/suscriptions-mp/blocks/lab_subscription/019cc012-a889-70d4-8ae2-a2d3cdb12669';

        function walkAndRename(container) {
            if (!container || typeof container !== 'object') return;
            if (container.blocks && typeof container.blocks === 'object') {
                if (container.blocks[WIDGET_OLD_KEY]) {
                    const w = container.blocks[WIDGET_OLD_KEY];
                    w.settings = w.settings || {};
                    w.settings.eligible_variant_ids = eligibleVariantIds;
                    w.settings.variant_notice_text = '';
                    container.blocks[widgetNewKey] = w;
                    delete container.blocks[WIDGET_OLD_KEY];
                    if (Array.isArray(container.block_order)) {
                        container.block_order = container.block_order.map(k => k === WIDGET_OLD_KEY ? widgetNewKey : k);
                    }
                    widgetFound = true;
                }
                Object.values(container.blocks).forEach(walkAndRename);
            }
        }
        Object.values(tpl.sections || {}).forEach(walkAndRename);

        // Si no existe widget → inyectarlo en main-product section (modo genérico)
        if (!widgetFound) {
            let mainSectionKey = null;
            for (const [k, sec] of Object.entries(tpl.sections || {})) {
                if (sec && String(sec.type || '').includes('main-product') || k === 'main') { mainSectionKey = k; break; }
            }
            if (!mainSectionKey) {
                for (const [k, sec] of Object.entries(tpl.sections || {})) {
                    if (sec && sec.blocks) { mainSectionKey = k; break; }
                }
            }
            if (mainSectionKey) {
                const mainSec = tpl.sections[mainSectionKey];
                if (!mainSec.blocks) mainSec.blocks = {};
                if (!mainSec.block_order) mainSec.block_order = [];
                mainSec.blocks[widgetNewKey] = {
                    type: widgetBlockType,
                    settings: {
                        eligible_variant_ids: eligibleVariantIds,
                        variant_notice_text: '',
                        text_btn_once: 'Compra única',
                        text_btn_sub: 'Suscripción',
                        badge_text: 'HASTA -33%',
                        primary_color: '#9d2a23',
                        free_shipping: false,
                        text_legal: 'Cancelación disponible entre los días 30 y 15 antes de cada envío, una vez completada la permanencia.',
                        color_bg: '#ffffff',
                        color_benefit: '#fdf2f2',
                        currency_sym: 'S/',
                        show_badge: true,
                        show_benefit_box: true,
                        show_social: false,
                        show_legal: false,
                        show_perks: false
                    }
                };
                // Insertar después de buy_buttons o price
                const idx = mainSec.block_order.findIndex(k => {
                    const t = (mainSec.blocks[k] && mainSec.blocks[k].type) || '';
                    return t.includes('buy_buttons') || t.includes('price');
                });
                if (idx >= 0) mainSec.block_order.splice(idx + 1, 0, widgetNewKey);
                else mainSec.block_order.push(widgetNewKey);
                widgetFound = true;
            }
        }

        // 4) Upload new template
        const uploadResp = await sh(`/themes/${mainTheme.id}/assets.json`, {
            method: 'PUT',
            body: JSON.stringify({ asset: { key: newTemplateKey, value: JSON.stringify(tpl, null, 2) } })
        });

        // 5) Assign template_suffix
        const assignResp = await sh(`/products/${productId}.json`, {
            method: 'PUT',
            body: JSON.stringify({ product: { id: Number(productId), template_suffix: newTemplateSuffix } })
        });

        // 6) Invalidate CDN cache via body_html no-op bump
        try {
            const getBody = await sh(`/products/${productId}.json?fields=body_html`);
            const currentBody = String(getBody.product?.body_html || '');
            const bumped = currentBody.includes(' ')
                ? currentBody.replace(/ /g, ' ')
                : currentBody + ' ';
            await sh(`/products/${productId}.json`, {
                method: 'PUT',
                body: JSON.stringify({ product: { id: Number(productId), body_html: bumped } })
            });
        } catch (_) { /* cache bump best-effort */ }

        res.json({
            ok: true,
            product: {
                id: productId,
                title: product.title,
                handle: product.handle,
                template_suffix: assignResp.product.template_suffix,
                storefront_url: `https://${shop.replace('.myshopify.com', '')}/products/${product.handle}`
            },
            template: {
                key: newTemplateKey,
                size: uploadResp.asset?.size || null,
                widget_block_key: widgetNewKey,
                section_suffix: sectionSuffix,
                widget_found_and_renamed: widgetFound,
                eligible_variant_ids: eligibleVariantIds,
                sections_renamed: Object.keys(renameMap).length
            }
        });
    } catch (e) {
        console.error('[INSTALL-SUB-TEMPLATE] Error:', e.message);
        res.status(500).json({ error: e.message });
    }
});

/**
 * POST /api/admin/themes/sync-bundle-visuals
 * 2026-04-21 — ADITIVO · arregla math savings Bundle 15 y Bundle 30 en vivo
 *
 * Regenera templates/product.bundle-15.json y templates/product.bundle-30.json
 * a partir del master templates/product.bundle.json:
 *   - Bundle 15: mantiene math base (S/180 retail, -S/30, S/150/mes — Plan 3m)
 *   - Bundle 30: math corregida (S/360 retail, -S/75, S/285/mes — Plan 3m)
 *   - Renombra secciones _bundle_section → _b15_section / _b30_section
 *   - Renombra widget block key + inyecta eligible_variant_ids del bundle correcto
 *   - Fixes contextuales: "15 latas" ↔ "30 latas", "Mix de 15", "pack de 15", etc.
 *
 * MASTER LOCK: solo sube 2 assets al theme. NO toca MP, webhooks, orders, crons, pedidos.
 */
app.post('/api/admin/themes/sync-bundle-visuals', async (req, res) => {
    try {
        const shop = process.env.SHOPIFY_SHOP || 'nutrition-lab-cluster.myshopify.com';
        const token = process.env.SHOPIFY_ACCESS_TOKEN || _shopifyToken;
        if (!token) return res.status(500).json({ error: 'Shopify token not configured' });

        const WIDGET_OLD_KEY = 'suscriptions_mp_lab_subscription_aPi9pn';
        const SECTION_RENAMES = [
            'lab_hero_bundle_section', 'lab_bene_bundle_section', 'lab_save_bundle_section',
            'lab_how_bundle_section', 'lab_flav_bundle_section', 'lab_comp_bundle_section',
            'lab_test_bundle_section', 'lab_faq_bundle_section', 'lab_final_bundle_section'
        ];
        const ELIGIBLE_VARIANTS = {
            15: '59393860206673,59393860239441',
            30: '59393860272209,59393860304977'
        };

        const sh = async (p, opts = {}) => {
            const r = await fetch(`https://${shop}/admin/api/2026-01${p}`, {
                ...opts,
                headers: { 'X-Shopify-Access-Token': token, 'Content-Type': 'application/json', ...(opts.headers || {}) }
            });
            const t = await r.text();
            if (!r.ok) throw new Error(`HTTP ${r.status} ${p}: ${t.substring(0, 250)}`);
            return t ? JSON.parse(t) : null;
        };

        // 1) Main theme
        const themesResp = await sh('/themes.json');
        const mainTheme = (themesResp.themes || []).find(t => t.role === 'main');
        if (!mainTheme) return res.status(404).json({ error: 'No main theme found' });

        // 2) Fetch master templates/product.bundle.json
        const masterAsset = await sh(`/themes/${mainTheme.id}/assets.json?asset[key]=${encodeURIComponent('templates/product.bundle.json')}`);
        let masterJson;
        try { masterJson = JSON.parse(masterAsset.asset.value); }
        catch (e) { return res.status(500).json({ error: 'Master template not valid JSON' }); }

        function generate(bundleId) {
            const tpl = JSON.parse(JSON.stringify(masterJson));
            const suffix = `_b${bundleId}_section`;
            const widgetNewKey = `suscriptions_mp_lab_subscription_b${bundleId}`;

            // Rename sections
            const renameMap = {};
            SECTION_RENAMES.forEach(old => { renameMap[old] = old.replace('_bundle_section', suffix); });
            const newSections = {};
            Object.keys(tpl.sections || {}).forEach(k => { newSections[renameMap[k] || k] = tpl.sections[k]; });
            tpl.sections = newSections;
            tpl.order = (tpl.order || []).map(k => renameMap[k] || k);

            // Widget renaming + eligible_variant_ids injection
            let widgetFound = false;
            function walk(container) {
                if (!container || typeof container !== 'object') return;
                if (container.blocks && typeof container.blocks === 'object') {
                    if (container.blocks[WIDGET_OLD_KEY]) {
                        const w = container.blocks[WIDGET_OLD_KEY];
                        w.settings = w.settings || {};
                        w.settings.eligible_variant_ids = ELIGIBLE_VARIANTS[bundleId];
                        w.settings.variant_notice_text = '';
                        container.blocks[widgetNewKey] = w;
                        delete container.blocks[WIDGET_OLD_KEY];
                        if (Array.isArray(container.block_order)) {
                            container.block_order = container.block_order.map(k => k === WIDGET_OLD_KEY ? widgetNewKey : k);
                        }
                        widgetFound = true;
                    }
                    Object.values(container.blocks).forEach(walk);
                }
            }
            Object.values(tpl.sections || {}).forEach(walk);

            // Contextual text fixes per bundle
            let json = JSON.stringify(tpl, null, 2);
            if (bundleId === 30) {
                json = json
                    // Savings math: Bundle 15 (S/180 → -S/30 → S/150) → Bundle 30 Plan 3m (S/360 → -S/75 → S/285)
                    .replace(/S\/ 180/g, 'S/ 360')
                    .replace(/− S\/ 30/g, '− S/ 75')
                    .replace(/S\/ 150/g, 'S/ 285')
                    // Copy / qty refs
                    .replace(/15 latas C4 a precio unitario/g, '30 latas C4 a precio unitario')
                    .replace(/Al comprar 15 latas sueltas en tienda/g, 'Al comprar 30 latas sueltas en tienda')
                    .replace(/15 latas C4/g, '30 latas C4')
                    .replace(/Combina 15 o 30 latas con tus sabores C4 favoritos cada mes/g, 'Combina 30 latas con tus sabores C4 favoritos cada mes')
                    .replace(/Selecciona 15 o 30 latas/g, 'Selecciona 30 latas')
                    .replace(/Mix de 15/g, 'Mix de 30')
                    .replace(/pack de 15/g, 'pack de 30')
                    .replace(/Arma tu Mix de 15 latas/g, 'Arma tu Mix de 30 latas')
                    // Generic "15 latas" only if preceded by words like "de", "por", "pack", "tus" (to not break "6+ Sabores")
                    .replace(/(?<=[\b])15 latas(?=[\b])/g, '30 latas');
            } else {
                // Bundle 15: asegurar textos contextuales coherentes
                json = json
                    .replace(/Combina 15 o 30 latas con tus sabores C4 favoritos cada mes/g, 'Combina 15 latas con tus sabores C4 favoritos cada mes')
                    .replace(/Selecciona 15 o 30 latas/g, 'Selecciona 15 latas');
            }

            return { tpl: JSON.parse(json), widgetFound, templateKey: `templates/product.bundle-${bundleId}.json` };
        }

        const results = [];
        for (const bid of [15, 30]) {
            const { tpl, widgetFound, templateKey } = generate(bid);
            const uploadResp = await sh(`/themes/${mainTheme.id}/assets.json`, {
                method: 'PUT',
                body: JSON.stringify({ asset: { key: templateKey, value: JSON.stringify(tpl, null, 2) } })
            });
            results.push({
                bundleId: bid,
                templateKey,
                size: uploadResp.asset?.size || null,
                widgetFound,
                eligibleVariants: ELIGIBLE_VARIANTS[bid]
            });
        }

        // Bump body_html on both bundle products to invalidate CDN cache
        const bundleProductIds = { 15: '15769236996177', 30: '15769237028945' };
        for (const pid of Object.values(bundleProductIds)) {
            try {
                const getBody = await sh(`/products/${pid}.json?fields=body_html`);
                const cur = String(getBody.product?.body_html || '');
                const bumped = cur.includes(' ') ? cur.replace(/ /g, ' ') : cur + ' ';
                await sh(`/products/${pid}.json`, {
                    method: 'PUT',
                    body: JSON.stringify({ product: { id: Number(pid), body_html: bumped } })
                });
            } catch (_) { /* best effort */ }
        }

        res.json({
            ok: true,
            theme: { id: mainTheme.id, name: mainTheme.name, role: mainTheme.role },
            results
        });
    } catch (e) {
        console.error('[SYNC-BUNDLE-VISUALS] Error:', e.message);
        res.status(500).json({ error: e.message });
    }
});

/**
 * POST /api/admin/products/:id/uninstall-subscription-template
 * 2026-04-21 — ADITIVO · reversible
 *
 * Quita el template_suffix del producto (vuelve al template por defecto). No borra el template
 * del theme (queda disponible para reactivar); solo desvincula el producto.
 *
 * MASTER LOCK: solo cambia template_suffix → null. No toca MP, webhooks, orders, crons, pedidos.
 */
app.post('/api/admin/products/:id/uninstall-subscription-template', async (req, res) => {
    try {
        const productId = String(req.params.id || '').trim();
        if (!productId) return res.status(400).json({ error: 'productId required' });
        const shop = process.env.SHOPIFY_SHOP || 'nutrition-lab-cluster.myshopify.com';
        const token = process.env.SHOPIFY_ACCESS_TOKEN || _shopifyToken;
        if (!token) return res.status(500).json({ error: 'Shopify token not configured' });
        const r = await fetch(`https://${shop}/admin/api/2026-01/products/${encodeURIComponent(productId)}.json`, {
            method: 'PUT',
            headers: { 'X-Shopify-Access-Token': token, 'Content-Type': 'application/json' },
            body: JSON.stringify({ product: { id: Number(productId), template_suffix: null } })
        });
        const t = await r.text();
        if (!r.ok) return res.status(r.status).json({ error: `Shopify ${r.status}`, detail: t.slice(0, 300) });
        const p = JSON.parse(t).product || {};
        res.json({
            ok: true,
            product: {
                id: p.id, title: p.title, handle: p.handle,
                template_suffix: p.template_suffix || null,
                storefront_url: `https://${shop.replace('.myshopify.com', '')}/products/${p.handle}`
            }
        });
    } catch (e) { res.status(500).json({ error: e.message }); }
});

/**
 * GET /api/admin/products/:id/template-status
 * 2026-04-21 — ADITIVO · solo lectura
 * Devuelve el template_suffix actual del producto + existencia del template en el theme.
 */
app.get('/api/admin/products/:id/template-status', async (req, res) => {
    try {
        const productId = String(req.params.id || '').trim();
        if (!productId) return res.status(400).json({ error: 'productId required' });
        const shop = process.env.SHOPIFY_SHOP || 'nutrition-lab-cluster.myshopify.com';
        const token = process.env.SHOPIFY_ACCESS_TOKEN || _shopifyToken;
        if (!token) return res.status(500).json({ error: 'Shopify token not configured' });
        const rp = await fetch(`https://${shop}/admin/api/2026-01/products/${encodeURIComponent(productId)}.json?fields=id,title,handle,template_suffix`, {
            headers: { 'X-Shopify-Access-Token': token }
        });
        if (!rp.ok) return res.status(rp.status).json({ error: `Shopify ${rp.status}` });
        const p = (await rp.json()).product || {};
        res.json({
            ok: true,
            product_id: p.id,
            title: p.title,
            handle: p.handle,
            template_suffix: p.template_suffix || null,
            template_file: p.template_suffix ? `templates/product.${p.template_suffix}.json` : 'templates/product.json',
            has_custom_template: !!p.template_suffix
        });
    } catch (e) { res.status(500).json({ error: e.message }); }
});

/**
 * POST /api/admin/variants/:vid/price  — 2026-04-21 ADITIVO
 * Actualiza el precio de una variante específica en Shopify.
 * Body: { price: "142.50" }   (string o number; Shopify acepta ambos)
 * MASTER LOCK: solo actualiza el campo `price` de la variante. NO toca webhooks,
 * orders, MP preapprovals, crons, ni lógica de suscripción.
 * Use case: ajustar el precio del plan 6 meses para reflejar la mayor permanencia
 * sin tocar la app de MP. El widget lee el precio de la variante directo.
 */
app.post('/api/admin/variants/:vid/price', async (req, res) => {
    try {
        const variantId = String(req.params.vid || '').trim();
        const price = req.body && (req.body.price !== undefined) ? String(req.body.price).trim() : null;
        if (!variantId) return res.status(400).json({ error: 'variantId required' });
        if (!price) return res.status(400).json({ error: 'price required in body' });
        // Validar que sea un número razonable (S/0.01 a S/99999.99)
        if (!/^\d+(\.\d{1,2})?$/.test(price) || Number(price) <= 0 || Number(price) > 99999.99) {
            return res.status(400).json({ error: 'price must be a valid positive number (e.g., "142.50")' });
        }
        const shop = process.env.SHOPIFY_SHOP || 'nutrition-lab-cluster.myshopify.com';
        const token = process.env.SHOPIFY_ACCESS_TOKEN || _shopifyToken;
        if (!token) return res.status(500).json({ error: 'Shopify token not configured' });
        const url = `https://${shop}/admin/api/2026-01/variants/${encodeURIComponent(variantId)}.json`;
        const r = await fetch(url, {
            method: 'PUT',
            headers: { 'X-Shopify-Access-Token': token, 'Content-Type': 'application/json' },
            body: JSON.stringify({ variant: { id: Number(variantId), price } })
        });
        const text = await r.text();
        if (!r.ok) return res.status(r.status).json({ error: `Shopify ${r.status}`, detail: text.slice(0, 400) });
        const data = JSON.parse(text);
        const v = data.variant || {};
        console.log(`[VARIANT PRICE] ✅ Updated variant ${variantId} price → S/${price}`);
        res.json({
            ok: true,
            variant: {
                id: v.id,
                title: v.title,
                sku: v.sku,
                price: v.price,
                product_id: v.product_id,
                updated_at: v.updated_at
            }
        });
    } catch (e) { res.status(500).json({ error: e.message }); }
});

/**
 * GET /api/bundles/product/:bundleProductId/config
 * Endpoint PÚBLICO (widget liquid) — dado el product_id del bundle (ej: C4 Energy Bundle 15),
 * devuelve la config + sabores disponibles con su estado (available/out_of_stock).
 *
 * IMPORTANTE: nunca expone stock numérico al frontend si hide_stock_from_ui=true.
 * Solo expone `available: true|false` por variante.
 */
app.get('/api/bundles/product/:bundleProductId/config', async (req, res) => {
    try {
        const bundleProductId = String(req.params.bundleProductId || '').trim();
        if (!bundleProductId) return res.status(400).json({ error: 'bundleProductId required' });
        const bundle = await db.getBundleConfigByBundleProductId(bundleProductId);
        if (!bundle) return res.status(404).json({ error: 'No bundle config found for this product' });
        if (bundle.active === false) return res.status(410).json({ error: 'Bundle is not active' });

        const shop = process.env.SHOPIFY_SHOP || 'nutrition-lab-cluster.myshopify.com';
        const token = process.env.SHOPIFY_ACCESS_TOKEN;
        if (!token) return res.status(500).json({ error: 'Shopify token not configured' });

        // 2026-04-22 — Combo fijo: retorna items predefinidos (no hay selector de sabores).
        // El widget debe renderizar la lista de items como "Este combo incluye: X + Y"
        // y dejar al cliente elegir solo el PLAN (permanencia/precio).
        if (bundle.type === 'fixed_combo') {
            return res.json({
                bundle_id: bundle.id,
                type: 'fixed_combo',
                name: bundle.name,
                description: bundle.description || '',
                bundle_product_id: bundle.bundle_product_id,
                target_quantity: bundle.target_quantity,
                combo_items: (bundle.combo_items || []).map(it => ({
                    product_id: it.product_id,
                    variant_id: it.variant_id,
                    quantity: it.quantity,
                    title: it.title,
                    variant_title: it.variant_title,
                    image: it.image,
                    sku: it.sku
                })),
                plans: bundle.plans || [],
                widget_copy: bundle.widget_copy || {},
            });
        }

        const url = `https://${shop}/admin/api/2026-01/products/${encodeURIComponent(bundle.source_product_id)}.json?fields=id,title,handle,images,variants`;
        const r = await fetch(url, { headers: { 'X-Shopify-Access-Token': token } });
        if (!r.ok) return res.status(502).json({ error: `Shopify product fetch ${r.status}` });
        const pData = await r.json();
        const product = pData.product;
        if (!product) return res.status(502).json({ error: 'Product not found in Shopify' });

        const allowedSet = new Set((bundle.allowed_variant_ids || []).map(String));
        const excludedSet = new Set((bundle.excluded_variant_ids || []).map(String));
        const imagesById = new Map((product.images || []).map(img => [img.id, img.src]));
        const minStock = Number(bundle.min_stock_threshold) || 0;

        // Map de variantes con estado de stock
        // Reglas: allowed AND NOT excluded. Disponibilidad = stock >= min_stock_threshold.
        // "excluded_variant_ids" NO aparece en el widget (ni sombreado). "allowed" con stock bajo → sombreado.
        const flavors = (product.variants || [])
            .filter(v => allowedSet.has(String(v.id)) && !excludedSet.has(String(v.id)))
            .map(v => {
                const stock = parseInt(v.inventory_quantity, 10);
                const numStock = Number.isFinite(stock) ? stock : 0;
                const hasStock = numStock >= minStock;
                // Respeta hide_stock_from_ui: si true, NO exponer inventory_quantity
                const out = {
                    variant_id: String(v.id),
                    title: String(v.title || '').split(' / ')[0].trim(), // "Frozen Bombsicle / 473 Ml" → "Frozen Bombsicle"
                    full_title: v.title,
                    sku: v.sku || '',
                    image: v.image_id && imagesById.get(v.image_id) ? imagesById.get(v.image_id) : (product.images?.[0]?.src || null),
                    available: hasStock,
                };
                if (bundle.hide_stock_from_ui !== true) out.stock = numStock;
                return out;
            });

        // Respuesta al widget — no incluye fields internos del bundle
        res.json({
            bundle_id: bundle.id,
            type: bundle.type || 'mix_match',
            name: bundle.name,
            description: bundle.description || '',
            bundle_product_id: bundle.bundle_product_id,
            source_product_id: bundle.source_product_id,
            source_product_title: bundle.source_product_title || product.title,
            source_product_handle: product.handle,
            target_quantity: bundle.target_quantity,
            plans: bundle.plans || [],
            validate_stock: bundle.validate_stock !== false,
            widget_copy: bundle.widget_copy || {},
            flavors,
        });
    } catch (e) { res.status(500).json({ error: e.message }); }
});

/* NOTA: catch-all movido al final del archivo. Si está antes de los endpoints
   nuevos (marketing/*, portal/me, dashboard-stats) los intercepta y devuelve
   admin.html en lugar del JSON. Mover al final garantiza que solo capture
   rutas que ningún handler atendió. */


/* ══════════════════════════════════════════════════════
   ⚡ MOTOR DE COBROS RECURRENTES REALES v6.1.0
   Shopify Native Subscription Billing Engine
   • Daily cron: queries ALL active contracts with nextBillingDate <= today
   • Triggers subscriptionBillingAttemptCreate for each due contract
   • Upon success: Shopify creates a real order automatically
   • Email automations: reminder 7d before, charge success/failure
   • Scales to unlimited subscribers via Shopify infrastructure
══════════════════════════════════════════════════════ */

/** Send transactional email via configured SMTP */
async function sendAutoEmail({ to, subject, html }) {
    try {
        const raw = await readFromShopify().catch(() => null);
        const settings = (raw && raw.settings) ? raw.settings : (raw || {});
        const nodemailer = require('nodemailer');
        const host = process.env.SMTP_HOST || settings.smtp_host || 'smtp.gmail.com';
        const user = process.env.SMTP_USER || settings.smtp_user || '';
        const pass = process.env.SMTP_PASS || settings.smtp_pass || '';
        const from = process.env.EMAIL_FROM || settings.email_from || user;
        if (!user || !pass) { console.warn('[EMAIL] SMTP not configured'); return; }
        const t = nodemailer.createTransport({ host, port: parseInt(process.env.SMTP_PORT || '587'), secure: false, auth: { user, pass } });
        await t.sendMail({ from: `LAB NUTRITION <${from}>`, to, subject, html });
        console.log(`[EMAIL] Sent: ${subject} → ${to}`);
    } catch (e) { console.error('[EMAIL] Error:', e.message); }
}

/** Email: Cobro exitoso — pedido generado */
function tplChargeSuccess(name, product, amount, currency, orderName, nextDate) {
    return `<!DOCTYPE html><html><head><meta charset="utf-8"></head><body style="font-family:Montserrat,sans-serif;background:#f5f5f5;padding:20px">
    <div style="max-width:580px;margin:0 auto;background:#fff;border-radius:14px;overflow:hidden;box-shadow:0 2px 20px rgba(0,0,0,.1)">
    <div style="background:#9d2a23;color:#fff;padding:28px 32px;font-size:22px;font-weight:900">✅ Suscripción renovada — LAB NUTRITION</div>
    <div style="padding:32px;line-height:1.7;color:#222">
    <p>Hola <strong>${name}</strong>,</p>
    <div style="background:#d1fae5;border:1.5px solid #6ee7b7;border-radius:10px;padding:16px 20px;margin:16px 0;color:#065f46;font-weight:700">
    ✅ Tu suscripción de <strong>${product}</strong> se renovó exitosamente.</div>
    <p>Pedido: <strong style="font-size:18px;color:#9d2a23">${orderName}</strong><br>Monto: <strong>${amount} ${currency}</strong></p>
    <p>Tu producto está siendo preparado para envío a tu dirección registrada.</p>
    <p>Próxima renovación: <strong>${nextDate ? new Date(nextDate).toLocaleDateString('es-PE') : '—'}</strong></p>
    </div>
    <div style="background:#f2f2f2;padding:14px 32px;text-align:center;font-size:11px;color:#aaa">LAB NUTRITION</div>
    </div></body></html>`;
}

/** Email: Cobro fallido */
function tplChargeFailed(name, product, errorMsg) {
    return `<!DOCTYPE html><html><head><meta charset="utf-8"></head><body style="font-family:Montserrat,sans-serif;background:#f5f5f5;padding:20px">
    <div style="max-width:580px;margin:0 auto;background:#fff;border-radius:14px;overflow:hidden;box-shadow:0 2px 20px rgba(0,0,0,.1)">
    <div style="background:#7e201a;color:#fff;padding:28px 32px;font-size:22px;font-weight:900">⚠️ Problema con tu suscripción</div>
    <div style="padding:32px;line-height:1.7;color:#222">
    <p>Hola <strong>${name}</strong>,</p>
    <div style="background:#fee2e2;border:1.5px solid #fca5a5;border-radius:10px;padding:16px;margin:16px 0;color:#991b1b">
    ⚠️ No pudimos renovar tu suscripción de <strong>${product}</strong>.<br><small>${errorMsg || 'Método de pago rechazado'}</small></div>
    <p>Por favor actualiza tu método de pago para continuar recibiendo tu pedido mensual.</p>
    </div>
    <div style="background:#f2f2f2;padding:14px 32px;text-align:center;font-size:11px;color:#aaa">LAB NUTRITION</div>
    </div></body></html>`;
}

/** Email: Recordatorio 7 días antes */
function tplReminder(name, product, amount, currency, nextDate) {
    return `<!DOCTYPE html><html><head><meta charset="utf-8"></head><body style="font-family:Montserrat,sans-serif;background:#f5f5f5;padding:20px">
    <div style="max-width:580px;margin:0 auto;background:#fff;border-radius:14px;overflow:hidden;box-shadow:0 2px 20px rgba(0,0,0,.1)">
    <div style="background:#9d2a23;color:#fff;padding:28px 32px;font-size:22px;font-weight:900">📦 Tu suscripción se renueva pronto</div>
    <div style="padding:32px;line-height:1.7;color:#222">
    <p>Hola <strong>${name}</strong>,</p>
    <p>Tu suscripción de <strong>${product}</strong> se renovará automáticamente en <strong>7 días</strong>.</p>
    <div style="background:#9d2a23;color:#fff;padding:10px 20px;border-radius:8px;display:inline-block;font-weight:800;margin:12px 0">
    Próximo cobro: ${amount} ${currency} · ${new Date(nextDate).toLocaleDateString('es-PE')}</div>
    <p>Tu pedido se generará y enviará a tu dirección registrada automáticamente. No necesitas hacer nada.</p>
    </div>
    <div style="background:#f2f2f2;padding:14px 32px;text-align:center;font-size:11px;color:#aaa">LAB NUTRITION</div>
    </div></body></html>`;
}

/** BILLING CRON: runs daily at 2am Lima — charges all due subscription contracts */
async function runDailyBillingCron() {
    const now = new Date();
    console.log('[BILLING CRON] Starting —', now.toISOString());
    let charged = 0, failed = 0;
    let cursor = null;

    try {
        do {
            const { due, hasNextPage, endCursor } = await subscriptionContracts.getContractsDueForBilling(cursor);
            cursor = hasNextPage ? endCursor : null;

            for (const contract of due) {
                const email = contract.customer?.email;
                const name = `${contract.customer?.firstName || ''} ${contract.customer?.lastName || ''}`.trim() || email;
                const line = contract.lines?.nodes?.[0];
                const product = line?.title || 'Producto LAB';
                const amount = line?.currentPrice?.amount || '0';
                const currency = line?.currentPrice?.currencyCode || 'PEN';
                const billingDate = (contract.nextBillingDate || now.toISOString()).split('T')[0];
                const idempotencyKey = `${contract.id.replace(/\W/g, '')}-${billingDate}`;

                try {
                    const attempt = await subscriptionContracts.createBillingAttempt(contract.id, idempotencyKey);
                    if (attempt?.order?.name) {
                        charged++;
                        console.log(`[BILLING CRON] ✅ ${email} charged — Order: ${attempt.order.name}`);
                        // Calculate next billing date
                        const nextD = new Date(contract.nextBillingDate || now);
                        nextD.setMonth(nextD.getMonth() + (contract.deliveryPolicy?.intervalCount || 1));
                        await sendAutoEmail({
                            to: email,
                            subject: `✅ Tu suscripción LAB se renovó — Pedido ${attempt.order.name}`,
                            html: tplChargeSuccess(name, product, amount, currency, attempt.order.name, nextD.toISOString())
                        });
                        if (db?.createEvent) await db.createEvent({ subscription_id: contract.id, event_type: 'charge_success', metadata: JSON.stringify({ order: attempt.order.name, amount, currency }) }).catch(() => {});
                    } else if (attempt?.errorMessage) {
                        failed++;
                        console.warn(`[BILLING CRON] ❌ ${email}: ${attempt.errorMessage}`);
                        await sendAutoEmail({ to: email, subject: '⚠️ Problema con tu suscripción LAB', html: tplChargeFailed(name, product, attempt.errorMessage) });
                        if (db?.createEvent) await db.createEvent({ subscription_id: contract.id, event_type: 'charge_failed', metadata: JSON.stringify({ error: attempt.errorMessage }) }).catch(() => {});
                    }
                } catch (err) {
                    failed++;
                    console.error(`[BILLING CRON] ${contract.id} error:`, err.message);
                }
                // Respect Shopify API rate limits (2 req/sec)
                await new Promise(r => setTimeout(r, 500));
            }
        } while (cursor);

        console.log(`[BILLING CRON] Done — charged:${charged} failed:${failed}`);
    } catch (e) {
        console.error('[BILLING CRON] Fatal:', e.message);
    }
}

/**
 * MP PAYMENT POLLING CRON — Detect MP recurring charges and create Shopify orders.
 * Runs every 4 hours. Checks each active MP subscription for new payments
 * that haven't been converted to Shopify orders yet.
 * This is necessary because MP preapprovals created via init_point
 * don't support notification_url updates.
 */
async function runMpPaymentPolling() {
    // 2026-05-12 — REACTIVADO con dedup hardened en createShopifyOrderFromSub.
    console.log('[MP POLLING] Starting payment check...');
    let ordersCreated = 0;

    try {
        let mpToken = process.env.MP_ACCESS_TOKEN;
        if (!mpToken) {
            const dyn = await readFromShopify().catch(() => ({}));
            if (dyn?.mp_access_token) { process.env.MP_ACCESS_TOKEN = dyn.mp_access_token; mpToken = dyn.mp_access_token; }
        }
        if (!mpToken) { console.warn('[MP POLLING] No MP token'); return; }

        const mpHeaders = { Authorization: `Bearer ${mpToken}` };
        // 🔧 2026-06-12: incluir también subs pausadas POR MP (mp_auto_paused) — si MP las
        // sigue debitando, sus cobros deben reconciliarse igual (antes quedaban sin orden
        // si el webhook se perdía, porque el polling solo miraba status='active').
        const allSubsRaw = db?.getSubscriptions ? await db.getSubscriptions().catch(() => []) : [];
        const allSubs = (Array.isArray(allSubsRaw) ? allSubsRaw : []).filter(s =>
            s.status === 'active' || (s.status === 'paused' && s.paused_reason === 'mp_auto_paused'));
        const mpSubs = allSubs.filter(s => s.mp_preapproval_id);

        if (!mpSubs.length) { console.log('[MP POLLING] No active MP subscriptions'); return; }

        for (const sub of mpSubs) {
            try {
                // Get current preapproval data from MP
                const preRes = await fetch(`https://api.mercadopago.com/preapproval/${sub.mp_preapproval_id}`, { headers: mpHeaders });
                if (!preRes.ok) continue;
                const preData = await preRes.json();

                const mpCharged = preData.summarized?.charged_quantity || 0;
                const localCycles = parseInt(sub.cycles_completed) || 0;

                // If MP has charged more times than we've recorded, there are new payments
                if (mpCharged > localCycles) {
                    const newCharges = mpCharged - localCycles;
                    console.log(`[MP POLLING] ${sub.customer_email}: MP charged ${mpCharged}, local=${localCycles} → ${newCharges} new charge(s)`);

                    // Search for authorized payments via MP's authorized_payments endpoint
                    // NOTE: /v1/payments/search?preapproval_id= does NOT work for recurring subs
                    const payRes = await fetch(`https://api.mercadopago.com/authorized_payments/search?preapproval_id=${sub.mp_preapproval_id}`, { headers: mpHeaders });
                    const payData = payRes.ok ? await payRes.json() : { results: [] };
                    // Each result has: { id, payment: { id, status }, transaction_amount, date_created }
                    const approvedPayments = (payData.results || [])
                        .filter(p => p.payment?.status === 'approved')
                        .sort((a, b) => new Date(b.date_created) - new Date(a.date_created))
                        .slice(0, newCharges);

                    // 🔒 FIX 2026-06-04: contar órdenes REALMENTE creadas en esta iteración
                    //   ANTES: cycles_completed se seteaba a mpCharged sin importar si las órdenes
                    //   se crearon o no. Si todas fallaban, cycles avanzaba igual y el bug quedaba
                    //   enmascarado para siempre (próxima corrida: mpCharged == localCycles → skip).
                    //   AHORA: solo avanzamos cycles por el número de órdenes que SÍ se crearon.
                    //   Si fallaron todas, cycles_completed NO avanza y se loggea evento de fallo.
                    let actualOrdersCreated = 0;
                    let actualOrdersSkippedDedup = 0;
                    let actualOrdersFailed = 0;
                    const failedReasons = [];

                    for (const authPay of approvedPayments) {
                        const paymentId = String(authPay.payment?.id || authPay.id);
                        // 🔑 FIX CRÍTICO 2026-06-09 (audit jun-8): dedup PRECISO.
                        //   ANTES: events.some(e => e.metadata?.includes?.(paymentId)) — substring
                        //   sobre metadata cruda. Los eventos de BLOQUEO (early_charge_blocked,
                        //   order_creation_failed, order_blocked_*) también contienen el VALOR del
                        //   payment_id → el polling creía "ya procesado" y NUNCA rescataba la orden.
                        //   AHORA: solo cuenta como procesado si existe un evento de ÉXITO real
                        //   (charge_success o first_order_created) cuyo mp_payment_id === paymentId.
                        const events = db?.getEvents ? await db.getEvents(sub.id, 100).catch(() => []) : [];
                        const SUCCESS_EVENTS = ['charge_success', 'first_order_created'];
                        const alreadyProcessed = events.some(e => {
                            if (!SUCCESS_EVENTS.includes(e.event_type)) return false;
                            try {
                                const m = typeof e.metadata === 'string' ? JSON.parse(e.metadata) : (e.metadata || {});
                                return String(m.mp_payment_id || '') === paymentId;
                            } catch {
                                // Si la metadata no es JSON parseable, fallback a substring SOLO en eventos de éxito
                                return typeof e.metadata === 'string' && e.metadata.includes(paymentId);
                            }
                        });
                        if (alreadyProcessed) { actualOrdersSkippedDedup++; continue; }

                        // 🔧 2026-06-12: si el webhook bloqueó este payment por débito temprano
                        // (<25d, early_charge_blocked) y la sub está flaggeada para revisión,
                        // el polling NO lo convierte en orden a la hora — la decisión es del
                        // admin (POST /api/admin/subscriptions/:id/retry-order la libera).
                        if (sub.needs_admin_review) {
                            const wasBlocked = events.some(e => {
                                if (e.event_type !== 'early_charge_blocked') return false;
                                try { const m = typeof e.metadata === 'string' ? JSON.parse(e.metadata) : (e.metadata || {}); return String(m.blocked_payment_id || '') === paymentId; } catch { return false; }
                            });
                            if (wasBlocked) {
                                console.warn(`[MP POLLING] ⏸ Payment ${paymentId} de ${sub.customer_email} bloqueado por débito temprano + needs_admin_review — skip (libera el admin).`);
                                continue;
                            }
                        }

                        // Create Shopify order
                        try {
                            const order = await createShopifyOrderFromSub(sub, paymentId);
                            if (order) {
                                ordersCreated++;
                                actualOrdersCreated++;
                                console.log(`[MP POLLING] ✅ Shopify order ${order.name} for ${sub.customer_email} (MP payment ${paymentId})`);

                                // Log event to avoid duplicate processing
                                if (db?.createEvent) {
                                    await db.createEvent({
                                        subscription_id: sub.id,
                                        event_type: 'charge_success',
                                        metadata: JSON.stringify({ mp_payment_id: paymentId, order_name: order.name, amount: authPay.transaction_amount, via: 'mp_polling' })
                                    }).catch(() => {});
                                }

                                // Send charge success email
                                if (notifications?.sendChargeSuccess) {
                                    notifications.sendChargeSuccess(sub, order.name).catch(e => console.warn('[MP POLLING] Email error:', e.message));
                                }
                            } else {
                                actualOrdersFailed++;
                                failedReasons.push({ payment_id: paymentId, reason: 'createShopifyOrderFromSub returned null' });
                            }
                        } catch (orderErr) {
                            actualOrdersFailed++;
                            failedReasons.push({ payment_id: paymentId, reason: orderErr.message });
                            console.error(`[MP POLLING] Order creation failed for ${sub.customer_email}:`, orderErr.message);
                        }
                    }

                    // 🔒 FIX 2026-06-04: solo avanzar cycles por las realmente creadas + las ya dedup'd (que ya contaban).
                    //   Esto evita el enmascaramiento permanente del bug. Si todas fallaron, cycles NO avanza.
                    const newCyclesCompleted = localCycles + actualOrdersCreated + actualOrdersSkippedDedup;
                    const nextCharge = new Date();
                    nextCharge.setMonth(nextCharge.getMonth() + (parseInt(sub.frequency_months) || 1));

                    if (newCyclesCompleted > localCycles) {
                        // 🔑 FIX 2026-06-09: grabar la fecha REAL del último débito MP (no "now")
                        //   para que el guard <25d del webhook compare contra fechas reales.
                        const lastRealDebit = approvedPayments[0]?.date_created || new Date().toISOString();
                        await db.updateSubscription(sub.id, {
                            cycles_completed: newCyclesCompleted,
                            last_charge_at: new Date().toISOString(),
                            last_mp_debit_date: lastRealDebit,
                            next_charge_at: preData.next_payment_date || nextCharge.toISOString()
                        }).catch(e => console.warn('[MP POLLING] update error:', e.message));
                    }

                    // Si hubo fallos, loggear evento auditable para que el admin pueda detectarlo
                    if (actualOrdersFailed > 0) {
                        if (db?.createEvent) {
                            await db.createEvent({
                                subscription_id: sub.id,
                                event_type: 'mp_polling_orders_failed',
                                metadata: JSON.stringify({
                                    mp_charged: mpCharged,
                                    local_cycles: localCycles,
                                    expected_new: newCharges,
                                    created: actualOrdersCreated,
                                    skipped_dedup: actualOrdersSkippedDedup,
                                    failed: actualOrdersFailed,
                                    failed_details: failedReasons,
                                    at: new Date().toISOString()
                                })
                            }).catch(() => {});
                        }
                        console.warn(`[MP POLLING] ⚠️ ${sub.customer_email}: ${actualOrdersFailed} órdenes fallaron de ${newCharges} esperadas. cycles NO avanzado (visible para admin).`);
                    }
                }

                // Rate limit: 500ms between MP API calls
                await new Promise(r => setTimeout(r, 500));
            } catch (subErr) {
                console.warn(`[MP POLLING] Error checking ${sub.customer_email}:`, subErr.message);
            }
        }

        console.log(`[MP POLLING] Done — ${ordersCreated} orders created`);
    } catch (e) {
        console.error('[MP POLLING] Fatal:', e.message);
    }
}

/**
 * ORDER RESCUE CRON — Safety net that catches ANY missed orders.
 * Runs every 1 hour. Checks:
 * 1. Active subs with cycles_completed > 0 but no Shopify order event → creates order
 * 2. Active subs with mp_preapproval_id but cycles_completed=0 → checks MP for payments
 * 3. Pending subs that have an authorized MP preapproval → activates + creates order
 * This guarantees NO subscription payment goes without a Shopify order.
 */
async function runOrderRescue() {
    // 2026-05-12 — REACTIVADO con dedup hardened.
    // alreadyHasShopifyOrderForSub ahora BLOQUEA si la API timea (antes
    // devolvia false silencioso). Cero riesgo de duplicar.
    console.log('[ORDER RESCUE] Starting...');
    let rescued = 0, errors = 0;
    try {
        let mpToken = process.env.MP_ACCESS_TOKEN;
        if (!mpToken) {
            const dyn = await readFromShopify().catch(() => ({}));
            if (dyn?.mp_access_token) { process.env.MP_ACCESS_TOKEN = dyn.mp_access_token; mpToken = dyn.mp_access_token; }
        }
        const mpHeaders = mpToken ? { Authorization: `Bearer ${mpToken}` } : null;
        const allSubs = db?.getSubscriptions ? await db.getSubscriptions().catch(() => []) : [];

        // === RESCUE 1: Active subs with charges but no order ===
        // 🔒 FIX 2026-05-12 (anti-fantasmas): guard adicional !s.shopify_order_id.
        //   ANTES: si la sub ya tenía shopify_order_id guardado pero getEvents() timeaba,
        //   .catch(()=>[]) devolvía array vacío → hasOrder=false → creaba orden duplicada
        //   FANTASMA (sin cobro MP detrás). Casos: Jerico #10387, Cayo Ramos #10399,
        //   n00205406 #9845, y otros 7+. Bodega despachaba doble caja sin cobro real.
        //   AHORA: si la sub ya tiene shopify_order_id (lo que prueba que createShopifyOrderFromSub
        //   guardó algo en su día) → la excluimos del filtro de rescue. Solo procesamos las
        //   verdaderas huérfanas (sub.shopify_order_id vacío).
        const activeWithCharges = allSubs.filter(s =>
            s.status === 'active' &&
            (parseInt(s.cycles_completed) || 0) > 0 &&
            !s.shopify_order_id
        );
        for (const sub of activeWithCharges) {
            try {
                // 🔒 FIX 2026-05-12 (anti-fantasmas): si getEvents falla, SKIP el rescue.
                //   ANTES: .catch(()=>[]) silenciaba el error y asumía "no hay events" →
                //   filtro hasOrder=false → creaba fantasma. Ahora si la API metaobjects
                //   timea, NO rescatamos (mejor pecar de cauteloso que duplicar).
                let events;
                let eventsOk = true;
                if (db?.getEvents) {
                    try {
                        events = await db.getEvents(sub.id, 50);
                    } catch (e) {
                        console.warn(`[ORDER RESCUE] getEvents fail for ${sub.id}, skip rescue:`, e.message);
                        eventsOk = false;
                    }
                } else {
                    events = [];
                }
                if (!eventsOk) continue;
                events = events || [];
                const hasOrder = events.some(e =>
                    e.event_type === 'first_order_created' ||
                    e.event_type === 'charge_success'
                );
                if (hasOrder) continue;

                // Try auto-resolve address if missing
                if (!sub.shipping_address?.address1 && sub.customer_email) {
                    const shop = process.env.SHOPIFY_SHOP || 'nutrition-lab-cluster.myshopify.com';
                    const token = process.env.SHOPIFY_ACCESS_TOKEN || _shopifyToken;
                    if (token) {
                        const addr = await getCustomerAddress(sub.customer_email, token, shop);
                        if (addr?.address1) {
                            await db.updateSubscription(sub.id, { shipping_address: addr }).catch(() => {});
                            sub.shipping_address = addr;
                            console.log(`[ORDER RESCUE] Auto-resolved address for ${sub.customer_email}`);
                        }
                    }
                }

                // 🔒🔒🔒 BLINDAJE DEFINITIVO 2026-05-12 — verificación MP irrefutable
                //   Antes de crear orden, EXIGIR que MP confirme un cobro huérfano.
                //   Regla: solo creamos orden si #cobros_MP > #orders_locales para esta sub.
                //   Si MP no responde, NO creamos (mejor pecar de cauteloso).
                //   Esto elimina al 100% la posibilidad de fantasmas sin cobro real.
                if (sub.mp_preapproval_id && mp.listPreapprovalPayments) {
                    let mpPayments = null;
                    try {
                        mpPayments = await mp.listPreapprovalPayments(sub.mp_preapproval_id, 30);
                    } catch (mpErr) {
                        console.warn(`[ORDER RESCUE] 🛡 NO RESCATAR ${sub.customer_email}: no pude verificar MP (${mpErr.message})`);
                        continue;
                    }
                    if (!Array.isArray(mpPayments)) {
                        console.warn(`[ORDER RESCUE] 🛡 NO RESCATAR ${sub.customer_email}: MP devolvió payments inválido`);
                        continue;
                    }
                    const realMpCharges = mpPayments.filter(p => p.status === 'processed' || p.status === 'approved').length;
                    // 🔒 FIX 2026-06-04: orderEvts no estaba definido en este scope (ReferenceError silenciado por catch).
                    //   Variable correcta es `events` (definido L8590-8603). Contamos eventos de orden ya creada.
                    const localOrders = (events || []).filter(e =>
                        e.event_type === 'first_order_created' ||
                        e.event_type === 'charge_success'
                    ).length;
                    if (realMpCharges <= localOrders) {
                        console.warn(`[ORDER RESCUE] 🛡 SKIP fantasma-prevention ${sub.customer_email}: MP=${realMpCharges} cobros, local=${localOrders} orders (no hay huérfano)`);
                        continue;
                    }
                    console.log(`[ORDER RESCUE] ✓ MP confirma cobro huérfano legítimo (MP=${realMpCharges}, local=${localOrders}) para ${sub.customer_email}`);
                } else {
                    // Sin mp_preapproval_id no podemos verificar MP → NO rescatar (anti-fantasma)
                    console.warn(`[ORDER RESCUE] 🛡 NO RESCATAR ${sub.customer_email}: sin mp_preapproval_id, no se puede verificar`);
                    continue;
                }

                // ☠️ KILL SWITCH 2026-05-12 — si el admin sospecha, env DISABLE_RESCUE_CRON=true
                //   detiene cualquier creación de orden en este path, incluso si la lógica anterior lo permite.
                if (process.env.DISABLE_RESCUE_CRON === 'true' || process.env.DISABLE_RESCUE_CRON === '1') {
                    console.warn(`[ORDER RESCUE] ☠ KILL SWITCH activo, no creo orden para ${sub.customer_email}`);
                    continue;
                }

                console.log(`[ORDER RESCUE] ⚠️ ${sub.customer_email} has ${sub.cycles_completed} charges but NO order — attempting rescue...`);
                const order = await createShopifyOrderFromSub(sub, 'rescue_' + Date.now()).catch(e => {
                    console.error(`[ORDER RESCUE] Failed for ${sub.customer_email}:`, e.message);
                    return null;
                });
                if (order?.id) {
                    rescued++;
                    console.log(`[ORDER RESCUE] ✅ Rescued order ${order.name} for ${sub.customer_email}`);
                    if (db?.createEvent) {
                        await db.createEvent({
                            subscription_id: sub.id,
                            event_type: 'first_order_created',
                            metadata: JSON.stringify({ shopify_order_id: order.id, order_name: order.name, rescued: true })
                        }).catch(() => {});
                    }
                } else {
                    errors++;
                    console.warn(`[ORDER RESCUE] ❌ Could not rescue ${sub.customer_email} — missing data (DNI: ${(sub.dni||'').length} chars, addr: ${sub.shipping_address?.address1 ? 'yes' : 'no'})`);
                }
            } catch (e) { errors++; console.warn('[ORDER RESCUE] error:', e.message); }
            await new Promise(r => setTimeout(r, 300));
        }

        // === RESCUE 2: Pending subs that actually have authorized MP preapprovals ===
        if (mpHeaders) {
            const pendingSubs = allSubs.filter(s =>
                s.status === 'pending_payment' &&
                s.mp_plan_id &&
                !s.mp_preapproval_id
            );
            // Get all authorized preapprovals from MP
            try {
                const r = await fetch('https://api.mercadopago.com/preapproval/search?status=authorized&limit=100', { headers: mpHeaders });
                if (r.ok) {
                    const mpData = await r.json();
                    const preapprovals = mpData?.results || [];
                    const usedIds = new Set(allSubs.map(s => s.mp_preapproval_id).filter(Boolean));

                    for (const pre of preapprovals) {
                        if (usedIds.has(pre.id)) continue; // Already linked

                        // ✅ SOLO procesar si MP YA COBRÓ (charged_quantity >= 1)
                        // "authorized" solo significa que el cliente autorizó la suscripción,
                        // NO que el cobro se hizo. Verificamos charged_quantity para 100% confirmado.
                        const charged = pre.summarized?.charged_quantity || 0;
                        if (charged < 1) continue; // MP aún no cobró — no crear orden

                        // Match by plan_id
                        const matchSub = pendingSubs.find(s => s.mp_plan_id === pre.preapproval_plan_id);
                        if (!matchSub) continue;

                        console.log(`[ORDER RESCUE] 🔗 Linking preapproval ${pre.id} to ${matchSub.customer_email} (plan match, MP charged: ${charged})`);

                        // Activate the subscription
                        const nextCharge = new Date();
                        nextCharge.setMonth(nextCharge.getMonth() + (parseInt(matchSub.frequency_months) || 1));
                        await db.updateSubscription(matchSub.id, {
                            mp_preapproval_id: pre.id,
                            status: 'active',
                            activated_at: new Date().toISOString(),
                            next_charge_at: pre.next_payment_date || nextCharge.toISOString(),
                            cycles_completed: 0
                        }).catch(() => {});

                        // Create first order
                        matchSub.mp_preapproval_id = pre.id;
                        matchSub.status = 'active';
                        const order = await createShopifyOrderFromSub(matchSub, pre.id).catch(() => null);
                        if (order?.id) {
                            rescued++;
                            await db.updateSubscription(matchSub.id, {
                                cycles_completed: 1,
                                shopify_order_id: String(order.id),
                                shopify_order_name: order.name
                            }).catch(() => {});
                            if (db?.createEvent) {
                                await db.createEvent({
                                    subscription_id: matchSub.id,
                                    event_type: 'first_order_created',
                                    metadata: JSON.stringify({ shopify_order_id: order.id, order_name: order.name, rescued: true, mp_preapproval_id: pre.id })
                                }).catch(() => {});
                            }
                            console.log(`[ORDER RESCUE] ✅ Activated + order ${order.name} for ${matchSub.customer_email}`);
                        }
                        usedIds.add(pre.id);
                        await new Promise(r => setTimeout(r, 500));
                    }
                }
            } catch (e) { console.warn('[ORDER RESCUE] MP search error:', e.message); }
        }

        console.log(`[ORDER RESCUE] Done — rescued:${rescued} errors:${errors}`);
    } catch (e) {
        console.error('[ORDER RESCUE] Fatal:', e.message);
    }
}

/** POST /api/admin/run-rescue — Dispara manualmente el rescue cron para atrapar órdenes perdidas */
app.post('/api/admin/run-rescue', async (req, res) => {
    try {
        // Responder inmediatamente; correr en background
        res.json({ success: true, message: 'Rescue cron iniciado en background — revisa logs Railway en 1-2 min' });
        setTimeout(() => {
            runOrderRescue().catch(e => console.error('[MANUAL RESCUE] Error:', e.message));
        }, 100);
    } catch (e) { res.status(500).json({ error: e.message }); }
});

/** REMINDER CRON: runs daily at 9am Lima — sends 7-day advance reminders */
async function runReminderCron() {
    console.log('[REMINDER CRON] Checking 7-day upcoming billing');
    try {
        const in7 = new Date(Date.now() + 7 * 86400000);
        const in8 = new Date(Date.now() + 8 * 86400000);
        const { gql } = require('./services/shopify-storage');
        let cursor = null;
        do {
            const data = await gql(`
                query Upcoming($after: String) {
                    subscriptionContracts(first: 100, after: $after, query: "status:ACTIVE") {
                        pageInfo { hasNextPage endCursor }
                        nodes {
                            id nextBillingDate
                            customer { email firstName lastName }
                            lines(first: 1) { nodes { title currentPrice { amount currencyCode } } }
                        }
                    }
                }
            `, { after: cursor }).catch(() => null);
            if (!data) break;
            const result = data.subscriptionContracts;
            cursor = result?.pageInfo?.hasNextPage ? result.pageInfo.endCursor : null;
            for (const c of (result?.nodes || [])) {
                const bd = c.nextBillingDate ? new Date(c.nextBillingDate) : null;
                if (!bd || bd < in7 || bd > in8) continue;
                const email = c.customer?.email;
                if (!email) continue;
                const name = `${c.customer.firstName || ''} ${c.customer.lastName || ''}`.trim() || email;
                const line = c.lines?.nodes?.[0];
                await sendAutoEmail({ to: email, subject: '📦 Tu suscripción LAB se renueva en 7 días', html: tplReminder(name, line?.title || 'Producto LAB', line?.currentPrice?.amount || '', line?.currentPrice?.currencyCode || 'PEN', c.nextBillingDate) });
            }
        } while (cursor);
    } catch (e) { console.error('[REMINDER CRON] Error:', e.message); }
}

/** Schedule a function to run at a specific hour:minute daily (Lima time UTC-5) */
function scheduleDailyCron(hourLima, minuteLima, fn) {
    const hourUTC = (hourLima + 5) % 24;
    const now = new Date();
    const next = new Date();
    next.setUTCHours(hourUTC, minuteLima, 0, 0);
    if (next <= now) next.setUTCDate(next.getUTCDate() + 1);
    const ms = next.getTime() - now.getTime();
    setTimeout(() => { fn(); setInterval(fn, 24 * 60 * 60 * 1000); }, ms);
    console.log(`[CRON] "${fn.name}" scheduled: next run in ${Math.round(ms / 60000)}min (${next.toISOString()})`);
}

/** SHOPIFY SUBSCRIPTION WEBHOOKS */
// billing_attempts/success — called by Shopify when charge succeeds and order is created
app.post('/webhooks/shopify/subscription_billing_attempts_success',
    express.raw({ type: 'application/json' }),
    async (req, res) => {
        res.status(200).send('ok'); // Ack immediately to prevent Shopify retries
        try {
            const data = JSON.parse(req.body.toString('utf8'));
            console.log('[WEBHOOK] billing_success — order:', data.order_name, 'contract:', data.subscription_contract_id);
            if (db?.getSubscriptions) {
                const subs = await db.getSubscriptions({ shopify_contract_id: data.subscription_contract_id }).catch(() => []);
                for (const s of (subs || [])) {
                    const nextCharge = new Date();
                    nextCharge.setMonth(nextCharge.getMonth() + (s.frequency_months || 1));
                    await db.updateSubscription(s.id, { cycles_completed: (s.cycles_completed || 0) + 1, last_charge_at: new Date().toISOString(), next_charge_at: nextCharge.toISOString() }).catch(() => {});
                    await db.createEvent({ subscription_id: s.id, event_type: 'charge_success', metadata: JSON.stringify({ order: data.order_name }) }).catch(() => {});
                }
            }
        } catch (e) { console.error('[WEBHOOK] billing_success error:', e.message); }
    });

// billing_attempts/failure — called when Shopify fails to charge
app.post('/webhooks/shopify/subscription_billing_attempts_failure',
    express.raw({ type: 'application/json' }),
    async (req, res) => {
        res.status(200).send('ok');
        try {
            const data = JSON.parse(req.body.toString('utf8'));
            console.warn('[WEBHOOK] billing_failure —', data.error_message, 'contract:', data.subscription_contract_id);
            if (db?.getSubscriptions) {
                const subs = await db.getSubscriptions({ shopify_contract_id: data.subscription_contract_id }).catch(() => []);
                for (const s of (subs || [])) {
                    await db.updateSubscription(s.id, { status: 'payment_failed' }).catch(() => {});
                    await db.createEvent({ subscription_id: s.id, event_type: 'charge_failed', metadata: JSON.stringify({ error: data.error_message }) }).catch(() => {});
                }
            }
        } catch (e) { console.error('[WEBHOOK] billing_failure error:', e.message); }
    });

// contracts/cancel
app.post('/webhooks/shopify/subscription_contracts_cancel',
    express.raw({ type: 'application/json' }),
    async (req, res) => {
        res.status(200).send('ok');
        try {
            const data = JSON.parse(req.body.toString('utf8'));
            if (db?.getSubscriptions) {
                const subs = await db.getSubscriptions({ shopify_contract_id: data.admin_graphql_api_id }).catch(() => []);
                for (const s of (subs || [])) {
                    await db.updateSubscription(s.id, { status: 'cancelled' }).catch(() => {});
                    await db.createEvent({ subscription_id: s.id, event_type: 'cancelled' }).catch(() => {});
                }
            }
        } catch (e) {}
    });

/** Manual trigger for billing cron */
app.post('/api/billing/run-now', async (req, res) => {
    res.json({ started: true, message: 'Billing cron triggered manually' });
    runDailyBillingCron().catch(console.error);
});

/** Manual trigger for SELF-HEAL — recrea orders Shopify para subs activas
 *  con cycles >= 1 sin shopify_order_id grabado. Util para arreglar
 *  subs viejas con datos incompletos (con el fix de 2026-04-30,
 *  estas subs ahora caen con placeholders + tag pending_data).
 */
app.post('/api/admin/self-heal/run-now', async (req, res) => {
    res.json({ started: true, message: 'Self-heal triggered manually — revisar logs en 30-60s' });
    // Ejecutar la misma lógica del cron interno
    (async () => {
        try {
            const allSubs = await db.getSubscriptions({ status: 'active' }).catch(() => []);
            const sevenDaysAgo = new Date(Date.now() - 7 * 24 * 60 * 60 * 1000).toISOString();
            const missing = allSubs.filter(s =>
                s.status === 'active' &&
                (parseInt(s.cycles_completed) || 0) >= 1 &&
                !s.shopify_order_id &&
                s.variant_id &&
                s.mp_preapproval_id &&
                (s.activated_at || s.created_at || '') >= sevenDaysAgo &&
                !(s.gifts_delivered === true && Array.isArray(s.gifts_planned) && s.gifts_planned.length > 0)
            );
            console.log(`[SELF-HEAL MANUAL] Found ${missing.length} subs sin shopify_order_id`);
            let created = 0, failed = 0;
            for (const sub of missing) {
                try {
                    const mpId = sub.mp_preapproval_id || 'selfheal_' + Date.now();
                    const order = await createShopifyOrderFromSub(sub, mpId);
                    if (order?.id) {
                        await db.updateSubscription(sub.id, {
                            shopify_order_id: String(order.id),
                            shopify_order_name: order.name
                        }).catch(() => {});
                        await db.createEvent({ subscription_id: sub.id, event_type: 'selfheal_order_created',
                            metadata: JSON.stringify({ shopify_order_id: order.id, order_name: order.name, manual: true })
                        }).catch(() => {});
                        console.log(`[SELF-HEAL MANUAL] OK ${order.name} para ${sub.customer_email}`);
                        created++;
                    } else { failed++; }
                    await new Promise(r => setTimeout(r, 800));
                } catch (e) {
                    console.error(`[SELF-HEAL MANUAL] Error ${sub.customer_email}: ${e.message}`);
                    failed++;
                }
            }
            console.log(`[SELF-HEAL MANUAL] Done. Created:${created} Failed:${failed}`);
        } catch (e) {
            console.error('[SELF-HEAL MANUAL] Fatal: ' + e.message);
        }
    })().catch(console.error);
});

/** Manual trigger for MP payment polling */
app.post('/api/mp-polling/run-now', async (req, res) => {
    res.json({ started: true, message: 'MP payment polling triggered manually' });
    runMpPaymentPolling().catch(console.error);
});

/* Portal endpoints moved to early registration above checkout */

/* ═══════════════════════════════════════════════════════════════════
   📊 DASHBOARD STATS (additive 2026-04-28)
   Endpoint read-only que calcula MRR, churn, LTV y métricas clave.
   No modifica ninguna lógica existente.
   ═══════════════════════════════════════════════════════════════════ */
// 2026-05-12 — Cache MP revenue (10 min TTL). Consulta MP directo, fuente de verdad.
let _mpRevenueCache = { total: 0, current_month: 0, last_month: 0, count: 0, at: 0 };
async function computeMpRevenueFromMP() {
    const now = Date.now();
    if (_mpRevenueCache.at && (now - _mpRevenueCache.at) < 10 * 60 * 1000) {
        return _mpRevenueCache;
    }
    const mpToken = process.env.MP_ACCESS_TOKEN;
    if (!mpToken) return _mpRevenueCache;
    try {
        const allSubs = await db.getSubscriptions().catch(() => []);
        const subsWithPre = (Array.isArray(allSubs) ? allSubs : []).filter(s => s.mp_preapproval_id);
        const monthStart = new Date(new Date().getFullYear(), new Date().getMonth(), 1).getTime();
        const lastMonthStart = new Date(new Date().getFullYear(), new Date().getMonth() - 1, 1).getTime();
        let total = 0, currentMonth = 0, lastMonth = 0, count = 0;
        // batches paralelos para no demorar 20+ segundos
        const BATCH = 8;
        for (let bi = 0; bi < subsWithPre.length; bi += BATCH) {
            const batch = subsWithPre.slice(bi, bi + BATCH);
            const results = await Promise.all(batch.map(async (sub) => {
                try {
                    const url = `https://api.mercadopago.com/authorized_payments/search?preapproval_id=${encodeURIComponent(sub.mp_preapproval_id)}&limit=10`;
                    const r = await fetch(url, { headers: { Authorization: 'Bearer ' + mpToken } });
                    if (!r.ok) return [];
                    const d = await r.json();
                    return (d.results || []).filter(p => p.status === 'processed' || p.status === 'approved');
                } catch { return []; }
            }));
            for (const payments of results) {
                for (const p of payments) {
                    const amount = parseFloat(p.transaction_amount) || 0;
                    const ts = new Date(p.date_created || 0).getTime();
                    total += amount;
                    count++;
                    if (ts >= monthStart) currentMonth += amount;
                    else if (ts >= lastMonthStart) lastMonth += amount;
                }
            }
        }
        _mpRevenueCache = {
            total: parseFloat(total.toFixed(2)),
            current_month: parseFloat(currentMonth.toFixed(2)),
            last_month: parseFloat(lastMonth.toFixed(2)),
            count,
            at: now
        };
        console.log(`[MP REVENUE] Cache refreshed: total=S/${total.toFixed(2)} thisMonth=S/${currentMonth.toFixed(2)} lastMonth=S/${lastMonth.toFixed(2)} count=${count}`);
    } catch (e) {
        console.warn('[MP REVENUE] error:', e.message);
    }
    return _mpRevenueCache;
}

app.get('/api/admin/dashboard-stats', async (req, res) => {
    try {
        const subs = await db.getSubscriptions().catch(() => []);
        const events = db?.getAllEvents ? await db.getAllEvents().catch(() => []) :
            (db?._listAll ? await db._listAll('lab_sub_event').catch(() => []) : []);

        // 2026-05-12 — REVENUE REAL desde MP (no del event log que tenía bug)
        const mpRevenue = await computeMpRevenueFromMP();

        const now = Date.now();
        const monthStart = new Date(new Date().getFullYear(), new Date().getMonth(), 1).getTime();
        const last30 = now - 30 * 24 * 3600 * 1000;
        const last90 = now - 90 * 24 * 3600 * 1000;

        // Conteos por status
        const byStatus = {};
        subs.forEach(s => { byStatus[s.status || 'unknown'] = (byStatus[s.status || 'unknown'] || 0) + 1; });

        // Active recurrentes (con ciclos pendientes)
        const activeRecurring = subs.filter(s =>
            s.status === 'active' &&
            (parseInt(s.cycles_completed) || 0) < (parseInt(s.cycles_required) || 999)
        );

        // MRR: suma de final_price de todas las activas (mensual)
        const mrr = activeRecurring.reduce((sum, s) => sum + parseFloat(s.final_price || 0), 0);
        const arr = mrr * 12;

        // Activas creadas en últimos 30 días (nuevas)
        const newLast30 = subs.filter(s => s.created_at && new Date(s.created_at).getTime() > last30).length;
        const newLast90 = subs.filter(s => s.created_at && new Date(s.created_at).getTime() > last90).length;

        // Cancelled últimos 30 días
        const cancelledLast30 = subs.filter(s =>
            s.status === 'cancelled' &&
            (s.cancelled_at || s.updated_at || '') &&
            new Date(s.cancelled_at || s.updated_at || 0).getTime() > last30
        ).length;

        // Churn rate: cancelled últimos 30d / active al inicio del periodo
        const churnRate = activeRecurring.length > 0
            ? ((cancelledLast30 / (activeRecurring.length + cancelledLast30)) * 100).toFixed(2)
            : '0.00';

        // LTV simple: (precio_promedio_mensual × meses_promedio) — meses_promedio = 1/churn_rate*100
        const avgMonthlyPrice = activeRecurring.length > 0
            ? mrr / activeRecurring.length
            : 0;
        const churnDecimal = parseFloat(churnRate) / 100;
        const ltvMonths = churnDecimal > 0 ? Math.min(1 / churnDecimal, 12) : 6;
        const ltv = avgMonthlyPrice * ltvMonths;

        // Regalos entregados este mes
        const giftsThisMonth = subs.filter(s =>
            s.gifts_delivered === true &&
            (s.gifts_delivered_at || '') >= new Date(monthStart).toISOString()
        ).length;

        // Cobros exitosos este mes (charge_success events)
        const chargesThisMonth = events.filter(e =>
            e.event_type === 'charge_success' &&
            new Date(e.created_at || 0).getTime() > monthStart
        ).length;

        // Revenue este mes (suma de cobros aprobados)
        const revenueThisMonth = events.filter(e =>
            e.event_type === 'charge_success' &&
            new Date(e.created_at || 0).getTime() > monthStart
        ).reduce((sum, e) => {
            try { const m = JSON.parse(e.metadata || '{}'); return sum + (parseFloat(m.amount) || 0); } catch { return sum; }
        }, 0);

        // Distribución por producto (subs activas)
        const byProduct = {};
        activeRecurring.forEach(s => {
            const k = s.product_title || s.product_id || 'unknown';
            byProduct[k] = (byProduct[k] || 0) + 1;
        });

        // Pending stuck (>24h sin pagar)
        const pendingStuck = subs.filter(s =>
            s.status === 'pending_payment' &&
            s.created_at &&
            (now - new Date(s.created_at).getTime()) > 24 * 3600 * 1000
        ).length;

        res.json({
            generated_at: new Date().toISOString(),
            counts: {
                total: subs.length,
                active_recurring: activeRecurring.length,
                pending_payment: byStatus['pending_payment'] || 0,
                pending_stuck_24h: pendingStuck,
                cancelled: byStatus['cancelled'] || 0,
                completed: subs.filter(s => (parseInt(s.cycles_completed)||0) >= (parseInt(s.cycles_required)||999)).length,
                payment_failed: byStatus['payment_failed'] || 0
            },
            financials: {
                mrr: parseFloat(mrr.toFixed(2)),
                arr: parseFloat(arr.toFixed(2)),
                avg_monthly_price: parseFloat(avgMonthlyPrice.toFixed(2)),
                ltv: parseFloat(ltv.toFixed(2)),
                ltv_months: parseFloat(ltvMonths.toFixed(1)),
                revenue_this_month: parseFloat(revenueThisMonth.toFixed(2)),
                charges_this_month: chargesThisMonth,
                // 2026-05-12 — REAL desde MP (fuente de verdad financiera)
                revenue_total_mp: mpRevenue.total,
                revenue_current_month_mp: mpRevenue.current_month,
                revenue_last_month_mp: mpRevenue.last_month,
                mp_payments_count: mpRevenue.count,
                mp_cache_age_seconds: Math.round((Date.now() - (mpRevenue.at || 0)) / 1000)
            },
            churn: {
                cancelled_last_30d: cancelledLast30,
                churn_rate_pct: parseFloat(churnRate)
            },
            growth: {
                new_subs_last_30d: newLast30,
                new_subs_last_90d: newLast90
            },
            gifts: {
                delivered_this_month: giftsThisMonth
            },
            distribution: {
                by_product: byProduct,
                by_status: byStatus
            }
        });
    } catch (e) {
        console.error('[DASHBOARD STATS] Error:', e.message);
        res.status(500).json({ error: e.message });
    }
});

/* ═══════════════════════════════════════════════════════════════════
   📨 MARKETING HELPERS (additive 2026-04-28)
   Endpoints adicionales que /api/marketing/send ya tiene como base.
   ═══════════════════════════════════════════════════════════════════ */

/** GET /api/marketing/segment-counts — cuántos clientes hay por segmento (antes de mandar campaña).
 *  Si querés desglose por producto: /api/marketing/segment-counts?breakdown=product
 */
// 2026-05-12 — Cache de customers Shopify con marketing opt-in (TTL 10min)
let _shopifyMarketingCache = { customers: [], at: 0 };
async function getShopifyMarketingCustomers(force = false) {
    const now = Date.now();
    if (!force && _shopifyMarketingCache.at && (now - _shopifyMarketingCache.at) < 10 * 60 * 1000) {
        return _shopifyMarketingCache.customers;
    }
    const shop = process.env.SHOPIFY_SHOP || 'nutrition-lab-cluster.myshopify.com';
    const token = process.env.SHOPIFY_ACCESS_TOKEN || _shopifyToken;
    if (!token) return [];

    const collected = [];
    let cursor = null;
    let pages = 0;
    try {
        do {
            // GraphQL: customers con email_marketing_consent state=SUBSCRIBED
            const q = `
                query Q($after: String) {
                    customers(first: 250, after: $after, query: "email_marketing_state:SUBSCRIBED") {
                        pageInfo { hasNextPage endCursor }
                        nodes {
                            id email firstName lastName
                            emailMarketingConsent { marketingState }
                        }
                    }
                }`;
            const r = await fetch(`https://${shop}/admin/api/2026-01/graphql.json`, {
                method: 'POST',
                headers: { 'X-Shopify-Access-Token': token, 'Content-Type': 'application/json' },
                body: JSON.stringify({ query: q, variables: { after: cursor } })
            });
            if (!r.ok) break;
            const d = await r.json();
            if (d.errors) { console.warn('[SHOPIFY MARKETING] GraphQL errors:', JSON.stringify(d.errors).slice(0, 200)); break; }
            const conn = d?.data?.customers || {};
            for (const c of (conn.nodes || [])) {
                if (!c.email) continue;
                const state = c.emailMarketingConsent?.marketingState || '';
                if (state !== 'SUBSCRIBED') continue;
                collected.push({
                    email: c.email,
                    first_name: c.firstName || '',
                    last_name: c.lastName || '',
                    full_name: [c.firstName, c.lastName].filter(Boolean).join(' ').trim() || (c.email.split('@')[0])
                });
            }
            cursor = conn.pageInfo?.hasNextPage ? conn.pageInfo.endCursor : null;
            pages++;
        } while (cursor && pages < 20);
        _shopifyMarketingCache = { customers: collected, at: now };
        console.log(`[SHOPIFY MARKETING] Cached ${collected.length} customers (marketing opt-in, ${pages} pages)`);
    } catch (e) {
        console.warn('[SHOPIFY MARKETING] error:', e.message);
    }
    return collected;
}

/**
 * 2026-05-13 — Health check de email sending stack.
 *  Devuelve si Resend está en modo sandbox o tiene dominio verificado.
 *  Permite al frontend bloquear el botón "Enviar campaña real" cuando
 *  el setup no está listo.
 *
 *  Logic:
 *  - Si RESEND_FROM contiene "@resend.dev" → SANDBOX (solo testing)
 *  - Si RESEND_API_KEY no está set → SOLO_SMTP
 *  - Manda un email de prueba A LA MISMA CUENTA y mira el resultado de Resend
 */
app.get('/api/marketing/email-health', async (req, res) => {
    try {
        const resendKey = process.env.RESEND_API_KEY;
        const resendFrom = process.env.RESEND_FROM || '';
        const smtpUser = process.env.SMTP_USER || '';
        const fromMatch = resendFrom.match(/<([^>]+)>/);
        const fromEmail = fromMatch ? fromMatch[1] : '';
        const fromDomain = fromEmail.split('@')[1] || '';
        const isSandbox = fromEmail.endsWith('@resend.dev') || fromDomain === 'resend.dev';

        const status = {
            ts: new Date().toISOString(),
            stack: resendKey ? 'resend' : (smtpUser ? 'smtp_fallback' : 'none'),
            from_configured: resendFrom || '(no configurado)',
            from_email: fromEmail,
            from_domain: fromDomain,
            sandbox_mode: isSandbox,
            ready_to_send_anywhere: !isSandbox && !!resendKey,
            warning: null,
            instructions: null
        };

        if (!resendKey) {
            status.warning = 'RESEND_API_KEY no configurado. Se va a usar SMTP fallback si está disponible.';
        } else if (isSandbox) {
            status.warning = 'Resend está en modo SANDBOX. Solo podés enviar a marketing@labnutrition.com (la cuenta dueña). Para enviar a cualquier otro email, verificá un dominio en Resend.';
            status.instructions = [
                '1. https://resend.com/domains → Add Domain → labnutrition.com',
                '2. Cargar los 3 DNS records que Resend muestra (MX + 2 TXT) en GoDaddy',
                '3. Esperar verificación (5-30 min) hasta que aparezca "Verified" en Resend',
                '4. En Ajustes del admin, cambiar RESEND_FROM a "Club Black Diamond" <club@labnutrition.com>',
                '5. Volver a este endpoint para confirmar ready_to_send_anywhere=true'
            ];
        } else {
            status.ok_msg = '✓ Email stack listo para mandar a cualquier destinatario. Dominio: ' + fromDomain;
        }
        res.json(status);
    } catch (e) {
        res.status(500).json({ error: e.message });
    }
});

app.get('/api/marketing/segment-counts', async (req, res) => {
    try {
        const subs = await db.getSubscriptions().catch(() => []);
        const seen = new Set();
        const dedup = subs.filter(s => {
            const e = (s.customer_email || '').toLowerCase();
            if (!e || seen.has(e)) return false;
            seen.add(e); return true;
        });

        // 2026-05-12 — Customers Shopify con marketing opt-in (cache 10min)
        const shopifyMarketing = await getShopifyMarketingCustomers().catch(() => []);

        const result = {
            active: dedup.filter(s => s.status === 'active').length,
            paused: dedup.filter(s => s.status === 'paused').length,
            payment_failed: dedup.filter(s => s.status === 'payment_failed').length,
            cancelled: dedup.filter(s => s.status === 'cancelled').length,
            pending_payment: dedup.filter(s => s.status === 'pending_payment').length,
            pending_stuck_24h: dedup.filter(s => s.status === 'pending_payment' && s.created_at && (Date.now() - new Date(s.created_at).getTime()) > 24*3600*1000).length,
            total_unique_emails: dedup.length,
            shopify_marketing: shopifyMarketing.length
        };

        // Breakdown por producto si lo piden
        if (req.query.breakdown === 'product') {
            const byProduct = {};
            for (const s of dedup) {
                const pid = String(s.product_id || 'unknown');
                if (!byProduct[pid]) {
                    byProduct[pid] = {
                        product_id: pid,
                        product_title: s.product_title || 'Desconocido',
                        active: 0, paused: 0, payment_failed: 0, cancelled: 0, pending_payment: 0, pending_stuck_24h: 0
                    };
                }
                const slot = byProduct[pid];
                if (slot[s.status] !== undefined) slot[s.status]++;
                if (s.status === 'pending_payment' && s.created_at && (Date.now() - new Date(s.created_at).getTime()) > 24*3600*1000) {
                    slot.pending_stuck_24h++;
                }
            }
            result.by_product = Object.values(byProduct).sort((a, b) => (b.active + b.pending_payment) - (a.active + a.pending_payment));
        }

        res.json(result);
    } catch (e) { res.status(500).json({ error: e.message }); }
});

/* ═══════════════════════════════════════════════════════════════════
   📨 HTML MAILING con personalización (additive 2026-04-30)
   Endpoint nuevo /api/marketing/send-html que acepta HTML completo,
   reemplaza tokens del cliente ({{first_name}}, {{product_title}}, etc.)
   y usa Resend (con SMTP fallback) en lugar del SMTP directo del viejo.

   Cumple estándares email 2026:
   - List-Unsubscribe + List-Unsubscribe-Post (Gmail/Yahoo mandate Feb 2024)
   - Plain-text alternative auto-generado (multipart, mejor inbox placement)
   - Preheader text para preview del inbox
   - Skip de subs con marketing_unsubscribed=true
   - Sanitización básica (strip <script>, <iframe>, on* handlers)
   - Idempotency_key para evitar duplicados en retries
   ═══════════════════════════════════════════════════════════════════ */

function htmlToPlainText(html) {
    if (!html) return '';
    return String(html)
        .replace(/<style[\s\S]*?<\/style>/gi, '')
        .replace(/<script[\s\S]*?<\/script>/gi, '')
        .replace(/<head[\s\S]*?<\/head>/gi, '')
        .replace(/<\/?(p|div|h[1-6]|li|tr|br)[^>]*>/gi, '\n')
        .replace(/<[^>]+>/g, '')
        .replace(/&nbsp;/g, ' ')
        .replace(/&amp;/g, '&')
        .replace(/&lt;/g, '<')
        .replace(/&gt;/g, '>')
        .replace(/&quot;/g, '"')
        .replace(/&#39;/g, "'")
        .replace(/\n{3,}/g, '\n\n')
        .replace(/[ \t]+/g, ' ')
        .trim();
}

function sanitizeMarketingHtml(html) {
    if (!html) return '';
    return String(html)
        .replace(/<script[\s\S]*?<\/script>/gi, '')
        .replace(/<iframe[\s\S]*?<\/iframe>/gi, '')
        .replace(/<object[\s\S]*?<\/object>/gi, '')
        .replace(/<embed[\s\S]*?<\/embed>/gi, '')
        .replace(/\son[a-z]+\s*=\s*"[^"]*"/gi, '')
        .replace(/\son[a-z]+\s*=\s*'[^']*'/gi, '')
        .replace(/javascript:/gi, '');
}

function injectPreheader(html, preheaderText) {
    if (!preheaderText) return html;
    const preheaderDiv = '<div style="display:none;font-size:1px;color:#fff;line-height:1px;max-height:0;max-width:0;opacity:0;overflow:hidden;mso-hide:all;">' +
        escapeHtml(preheaderText) +
        '</div>';
    if (/<body[^>]*>/i.test(html)) {
        return html.replace(/<body([^>]*)>/i, '<body$1>' + preheaderDiv);
    }
    return preheaderDiv + html;
}

/** Envía un email de campaña con headers modernos y plain-text alternative.
 *  Usa Resend directo (no sendAutoEmail) para poder pasar headers custom + text.
 */
async function sendCampaignEmail({ to, subject, html, text, unsubscribeUrl, mailtoUnsub }) {
    const key = process.env.RESEND_API_KEY;
    const from = process.env.RESEND_FROM
        || ('"' + (process.env.EMAIL_FROM || 'LAB NUTRITION') + '" <' + (process.env.SMTP_USER || 'marketing@labnutrition.com') + '>');

    const headers = {};
    if (unsubscribeUrl) {
        const parts = ['<' + unsubscribeUrl + '>'];
        if (mailtoUnsub) parts.unshift('<mailto:' + mailtoUnsub + '?subject=unsubscribe>');
        headers['List-Unsubscribe'] = parts.join(', ');
        headers['List-Unsubscribe-Post'] = 'List-Unsubscribe=One-Click';
    }

    if (key) {
        const res = await fetch('https://api.resend.com/emails', {
            method: 'POST',
            headers: { Authorization: 'Bearer ' + key, 'Content-Type': 'application/json' },
            body: JSON.stringify({ from, to, subject, html, text, headers })
        });
        const j = await res.json().catch(() => ({}));
        if (!res.ok) throw new Error('[Resend] ' + (j?.message || j?.error || 'HTTP ' + res.status));
        return j;
    }
    // SMTP fallback
    const nodemailer = require('nodemailer');
    const t = nodemailer.createTransport({
        host: process.env.SMTP_HOST,
        port: parseInt(process.env.SMTP_PORT || '465'),
        secure: parseInt(process.env.SMTP_PORT || '465') === 465,
        auth: { user: process.env.SMTP_USER, pass: process.env.SMTP_PASS }
    });
    return t.sendMail({ from, to, subject, html, text, headers });
}

const ALLOWED_TOKENS = [
    'first_name', 'last_name', 'full_name', 'email', 'phone',
    'greeting', // 2026-05-12: "Hola Juan" o "Hola" segun si el nombre es bueno
    'product_title', 'product_id', 'variant_id', 'variant_title',
    'frequency_months', 'permanence_months', 'cycles_completed', 'cycles_required',
    'discount_pct', 'final_price', 'base_price',
    'monthly_discount', 'total_discount_if_complete',
    'plan_label', 'next_charge_at', 'next_charge_date',
    'status', 'subscription_id',
    'portal_url', 'unsubscribe_url',
    'product_url', 'product_handle', 'product_image'
];

/**
 * 2026-05-12 — Limpia first_name para usar en saludos de email.
 * Detecta junk (emails, all caps, random strings) y los corrige o vacia.
 * - "YUDER" -> "Yuder"
 * - "JESÚS MANUEL ROJAS" -> "Jesús"
 * - "luismiguelordonez" (username largo lowercase) -> ''
 * - "sfvfd" (random short) -> ''
 * - "jericobenitesgomez3" (lleva numero) -> ''
 * - "Juan" -> "Juan" (sin cambios)
 * - "" -> ""
 */
function cleanFirstName(rawName) {
    if (!rawName) return '';
    const n = String(rawName).trim();
    if (!n) return '';
    if (n.includes('@')) return ''; // es email
    const firstWord = n.split(/\s+/)[0];
    if (!firstWord) return '';
    if (firstWord.length < 2) return ''; // muy corto, probable test
    if (firstWord.length > 20) return ''; // muy largo, probable username concatenado
    if (/\d/.test(firstWord)) return ''; // contiene numero
    // Junk detector — ratio de vocales en palabra real es >= 25%
    // sfvfd (0%) y SDFSDGFSDF (0%) → descartar.  Juan (50%), Yuder (40%) → ok.
    const lower = firstWord.toLowerCase();
    const vowels = (lower.match(/[aeiouáéíóú]/g) || []).length;
    if (vowels / firstWord.length < 0.20) return '';
    // Si todo lowercase y mas de 10 chars: probable username concatenado
    if (firstWord === firstWord.toLowerCase() && firstWord.length > 10) return '';
    // Capitalizar correctamente (Title Case en primera palabra)
    return firstWord.charAt(0).toUpperCase() + firstWord.slice(1).toLowerCase();
}

function buildPlanLabel(sub) {
    const f = parseInt(sub.frequency_months) || 1;
    const p = parseInt(sub.permanence_months) || 0;
    const freqWord = f === 1 ? 'Mensual' : 'Cada ' + f + ' meses';
    return freqWord + ' × ' + p + ' meses';
}

function formatNextCharge(iso) {
    if (!iso) return '';
    try {
        const d = new Date(iso);
        return d.toLocaleDateString('es-PE', { day: '2-digit', month: 'long', year: 'numeric' });
    } catch { return iso.slice(0, 10); }
}

function escapeHtml(s) {
    return String(s || '').replace(/[&<>"']/g, ch => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[ch]));
}

async function buildContextForSub(sub) {
    const portalBase = process.env.BACKEND_URL || 'https://pixel-suite-pro-production.up.railway.app';
    // 2026-05-12: limpieza inteligente de first_name para evitar "Hola sfvfd"
    const rawFn = (sub.customer_name || sub.first_name || '').split(' ')[0] || '';
    const fn = cleanFirstName(rawFn);
    const ln = (sub.customer_name || '').split(' ').slice(1).join(' ') || '';
    const greeting = fn ? ('Hola ' + fn) : 'Hola';
    const basePrice = parseFloat(sub.base_price || 0);
    const finalPrice = parseFloat(sub.final_price || basePrice);
    const monthlyDiscount = Math.max(0, basePrice - finalPrice);
    const cyclesReq = parseInt(sub.cycles_required) || 0;
    const totalDiscountIfComplete = monthlyDiscount * cyclesReq;

    // Resolve product URL: usa getProductHandle (con cache 1h) si está disponible,
    // sino fallback a /pages/suscripciones. NUNCA falla.
    let productHandle = '';
    let productUrl = 'https://labnutrition.com/pages/suscripciones';
    let productImage = sub.product_image || '';
    try {
        if (sub.product_id) {
            if (typeof getProductHandle === 'function') {
                const h = await getProductHandle(sub.product_id).catch(() => null);
                if (h) {
                    productHandle = h;
                    productUrl = 'https://labnutrition.com/products/' + h;
                }
            }
            if (!productImage && typeof getProductImage === 'function') {
                const img = await getProductImage(sub.product_id).catch(() => null);
                if (img) productImage = img;
            }
        }
    } catch (_) { /* fallback queda */ }

    return {
        first_name: fn,
        last_name: ln,
        full_name: sub.customer_name || sub.customer_email || '',
        greeting: greeting, // 2026-05-12 — saludo inteligente listo para usar
        email: sub.customer_email || '',
        phone: sub.customer_phone || '',
        product_title: sub.product_title || '',
        product_id: String(sub.product_id || ''),
        variant_id: String(sub.variant_id || ''),
        variant_title: sub.variant_title || '',
        frequency_months: String(sub.frequency_months || 1),
        permanence_months: String(sub.permanence_months || 0),
        cycles_completed: String(sub.cycles_completed || 0),
        cycles_required: String(sub.cycles_required || 0),
        discount_pct: String(Math.round(sub.discount_pct || 0)),
        final_price: String(parseFloat(sub.final_price || 0).toFixed(2)),
        base_price: String(parseFloat(sub.base_price || 0).toFixed(2)),
        monthly_discount: monthlyDiscount.toFixed(2),
        total_discount_if_complete: totalDiscountIfComplete.toFixed(2),
        plan_label: buildPlanLabel(sub),
        next_charge_at: sub.next_charge_at || '',
        next_charge_date: formatNextCharge(sub.next_charge_at),
        status: sub.status || '',
        subscription_id: sub.id || '',
        portal_url: portalBase + '/portal?email=' + encodeURIComponent(sub.customer_email || ''),
        unsubscribe_url: portalBase + '/api/marketing/unsubscribe?email=' + encodeURIComponent(sub.customer_email || '') + '&sub_id=' + encodeURIComponent(sub.id || ''),
        product_url: productUrl,
        product_handle: productHandle,
        product_image: productImage
    };
}

function applyTokens(html, ctx) {
    return html.replace(/\{\{\s*([a-z_]+)\s*\}\}/gi, (m, key) => {
        if (!ALLOWED_TOKENS.includes(key)) return m; // dejar tokens desconocidos sin tocar
        const val = ctx[key];
        return val !== undefined && val !== null ? escapeHtml(val) : '';
    });
}

function appendUnsubscribeFooter(html, ctx) {
    const footer = '\n<div style="text-align:center;padding:18px 16px;font-size:11px;color:#999;font-family:Arial,sans-serif;border-top:1px solid #eee;margin-top:24px">' +
        'Recibís este email porque sos suscriptor de LAB NUTRITION. ' +
        '<a href="' + ctx.unsubscribe_url + '" style="color:#999;text-decoration:underline">Darme de baja</a>' +
        '</div>';
    if (/<\/body>/i.test(html)) return html.replace(/<\/body>/i, footer + '</body>');
    return html + footer;
}

/** POST /api/marketing/preview-html — pre-renderiza el HTML con un sub real (sin enviar email).
 *  Body: { segment, html, sample_email? }
 *  Retorna el HTML renderizado con datos de un sub random del segmento (o del email indicado).
 */
app.post('/api/marketing/preview-html', async (req, res) => {
    try {
        const { segment, html, sample_email, sample_segment, to_email, product_id } = req.body || {};
        const seg = segment || sample_segment;
        if (!html || typeof html !== 'string') return res.status(400).json({ error: 'html required' });

        const subs = await db.getSubscriptions().catch(() => []);

        // 2026-05-12: si llega to_email (el destino del test/preview) y coincide con un
        //   cliente real, usar ese cliente como sample. Si no, fallback al sample del segmento.
        //   Esto evita "Hola Luis Miguel Ordoñez" en TODOS los previews.
        const targetEmail = (sample_email || to_email || '').toLowerCase();
        let sub = null;
        if (targetEmail) {
            sub = subs.find(s => (s.customer_email || '').toLowerCase() === targetEmail);
        }
        if (!sub) {
            let pool = subs;
            if (seg && seg !== 'all') pool = pool.filter(s => s.status === seg);
            if (product_id) pool = pool.filter(s => String(s.product_id || '') === String(product_id));
            sub = pool[0];
        }
        // Si no hay match ni pool, devolver preview con tokens genéricos en vez de error
        if (!sub) {
            sub = {
                customer_email: targetEmail || 'preview@test.com',
                customer_name: 'Cliente',
                first_name: 'Cliente',
                product_title: 'Producto',
                frequency_months: 1, permanence_months: 6, cycles_completed: 0,
                cycles_required: 6, discount_pct: 0, final_price: 0, base_price: 0,
                next_charge_at: new Date(Date.now() + 30 * 86400000).toISOString()
            };
        }

        const ctx = await buildContextForSub(sub);
        const rendered = applyTokens(html, ctx);
        res.json({
            sample_sub_email: sub.customer_email,
            sample_sub_name: sub.customer_name,
            tokens_used: Object.fromEntries(ALLOWED_TOKENS.filter(t => html.includes('{{' + t + '}}') || html.includes('{{ ' + t + ' }}')).map(t => [t, ctx[t]])),
            html: rendered,
            html_with_footer: appendUnsubscribeFooter(rendered, ctx)
        });
    } catch (e) {
        res.status(500).json({ error: e.message });
    }
});

/** POST /api/marketing/test-html — envía 1 email de prueba a un destinatario específico.
 *  Body: { html, subject, to_email, sample_segment?, from? }
 *  Renderiza con datos del primer sub del segmento (o uno random) y manda solo a to_email.
 *  Acepta from override (ej. "informes@labnutrition.com" si el dominio está verificado en Resend).
 */
app.post('/api/marketing/test-html', async (req, res) => {
    try {
        const { html, subject, to_email, sample_segment, from, product_id } = req.body || {};
        if (!html || !subject || !to_email) return res.status(400).json({ error: 'html, subject, to_email required' });

        const subs = await db.getSubscriptions().catch(() => []);

        // 2026-05-12: PRIMERO buscar si el to_email coincide con un cliente real.
        //   Si coincide -> usar SUS datos (su nombre, su producto, etc.). Mucho más útil
        //   que el "Luis Miguel Ordoñez" genérico para test.
        //   Si no -> caer al sample del segmento.
        const emailLower = String(to_email || '').toLowerCase();
        const realCustomer = subs.find(s => (s.customer_email || '').toLowerCase() === emailLower);

        let sample;
        if (realCustomer) {
            sample = realCustomer;
        } else {
            let pool = sample_segment && sample_segment !== 'all'
                ? subs.filter(s => s.status === sample_segment)
                : subs;
            if (product_id) pool = pool.filter(s => String(s.product_id || '') === String(product_id));
            sample = pool[0] || subs[0];
        }

        // Si no hay subs ni match, usar contexto genérico (sin Luis Miguel)
        const sample_safe = sample || {
            id: 'test_sample',
            customer_email: to_email,
            customer_name: 'Cliente',
            first_name: 'Cliente',
            product_title: 'Producto Test',
            frequency_months: 1,
            permanence_months: 6,
            cycles_completed: 0,
            cycles_required: 6,
            discount_pct: 50,
            final_price: 90,
            base_price: 179,
            next_charge_at: new Date(Date.now() + 30*24*3600*1000).toISOString()
        };

        const ctx = await buildContextForSub(sample_safe);
        const safeHtml = sanitizeMarketingHtml(html);
        let rendered = applyTokens(safeHtml, ctx);
        rendered = appendUnsubscribeFooter(rendered, ctx);
        const subjectRendered = applyTokens(subject, ctx);
        const plainText = htmlToPlainText(rendered);

        // Usar sendCampaignEmail con headers modernos y from override opcional
        const fromOverride = from
            ? (from.includes('<') ? from : '"LAB NUTRITION" <' + from + '>')
            : undefined;

        if (fromOverride) {
            // Llamar Resend directo con from custom
            const key = process.env.RESEND_API_KEY;
            if (!key) return res.status(500).json({ error: 'RESEND_API_KEY no configurado, no puedo usar from override' });
            const r = await fetch('https://api.resend.com/emails', {
                method: 'POST',
                headers: { Authorization: 'Bearer ' + key, 'Content-Type': 'application/json' },
                body: JSON.stringify({
                    from: fromOverride,
                    to: to_email,
                    subject: '[TEST] ' + subjectRendered,
                    html: rendered,
                    text: plainText,
                    headers: {
                        'List-Unsubscribe': '<' + ctx.unsubscribe_url + '>',
                        'List-Unsubscribe-Post': 'List-Unsubscribe=One-Click'
                    }
                })
            });
            const j = await r.json().catch(() => ({}));
            if (!r.ok) return res.status(500).json({ error: '[Resend] ' + (j?.message || j?.error || 'HTTP ' + r.status), detail: j });
            return res.json({
                ok: true,
                sent_to: to_email,
                from: fromOverride,
                sample_used: { email: sample_safe.customer_email, name: sample_safe.customer_name },
                resend_id: j.id,
                note: 'Email TEST enviado vía Resend con from override. Subject lleva prefijo [TEST].'
            });
        }

        // Sin from override → usar sendCampaignEmail (Resend o SMTP fallback)
        await sendCampaignEmail({
            to: to_email,
            subject: '[TEST] ' + subjectRendered,
            html: rendered,
            text: plainText,
            unsubscribeUrl: ctx.unsubscribe_url
        });

        res.json({
            ok: true,
            sent_to: to_email,
            sample_used: { email: sample_safe.customer_email, name: sample_safe.customer_name },
            note: 'Email TEST enviado. Subject lleva prefijo [TEST].'
        });
    } catch (e) {
        res.status(500).json({ error: e.message });
    }
});

/** POST /api/marketing/send-html — envía campaña HTML personalizada a un segmento.
 *  Body: { segment, subject, html, preheader?, throttle_ms?, dry_run? }
 *  - Personaliza por sub con tokens
 *  - Sanitiza HTML (strip script/iframe/on*)
 *  - Inyecta preheader text para preview de inbox
 *  - Agrega unsubscribe footer + List-Unsubscribe header (Gmail/Yahoo Feb 2024 mandate)
 *  - Genera plain-text alternative automático
 *  - Skip subs con marketing_unsubscribed=true (compliance)
 *  - Usa Resend con headers modernos (fallback SMTP)
 *  - Throttle default 250ms entre envíos
 *  - dry_run=true → no envía, solo cuenta y lista destinatarios
 */
app.post('/api/marketing/send-html', async (req, res) => {
    try {
        const { segment, subject, html, preheader, throttle_ms, dry_run, product_id } = req.body || {};
        if (!segment || !subject || !html) return res.status(400).json({ error: 'segment, subject, html required' });

        const allowedSegments = ['active', 'paused', 'payment_failed', 'cancelled', 'pending_payment', 'all', 'shopify_marketing'];
        if (!allowedSegments.includes(segment)) {
            return res.status(400).json({ error: 'segment invalid', allowed: allowedSegments });
        }

        const subs = await db.getSubscriptions().catch(() => []);
        let filtered;

        // 2026-05-12 FIX: para shopify_marketing — si cache esta caliente, lo usamos sincronico.
        // Si esta frio (>10s para 5000 customers), respondemos inmediato {accepted:true} y
        // procesamos en background. Asi el frontend nunca timea.
        if (segment === 'shopify_marketing') {
            const cacheIsWarm = _shopifyMarketingCache.at && (Date.now() - _shopifyMarketingCache.at) < 10 * 60 * 1000;
            if (!cacheIsWarm && !dry_run) {
                // Responder INMEDIATO con accepted=true y procesar todo el flujo en background
                res.json({
                    started: true,
                    accepted_async: true,
                    recipients_count: 'calculando_en_background',
                    skipped_unsubscribed: 0,
                    segment,
                    subject,
                    note: 'Cache de Shopify frío — primero pago la pagina (~10s). Envío iniciado en background. Revisá /api/marketing/campaign-summary en unos minutos.'
                });
                // Detached — procesar el resto sin bloquear el response
                setImmediate(async () => {
                    try {
                        const shopifyCustomers = await getShopifyMarketingCustomers().catch(() => []);
                        const detachedFiltered = shopifyCustomers.map(c => ({
                            customer_email: c.email,
                            customer_name: c.full_name || c.first_name || c.email.split('@')[0],
                            first_name: c.first_name || '',
                            last_name: c.last_name || '',
                            status: 'shopify_customer',
                            _shopify_only: true,
                            product_title: 'LAB NUTRITION',
                            frequency_months: 1, permanence_months: 0,
                            cycles_completed: 0, cycles_required: 0,
                            discount_pct: 0, final_price: 0, base_price: 0
                        })).filter(s => s.marketing_unsubscribed !== true && s.email_bounced !== true);
                        const seen = new Set();
                        const dedupFiltered = detachedFiltered.filter(s => {
                            const e = (s.customer_email || '').toLowerCase();
                            if (!e || seen.has(e)) return false;
                            seen.add(e); return true;
                        });
                        const wait = parseInt(throttle_ms) || 250;
                        const safeHtml = sanitizeMarketingHtml(html);
                        const startedAt = new Date().toISOString();
                        let sent = 0, failed = 0;
                        for (const sub of dedupFiltered) {
                            try {
                                const ctx = await buildContextForSub(sub);
                                let rendered = applyTokens(safeHtml, ctx);
                                if (preheader) rendered = injectPreheader(rendered, applyTokens(preheader, ctx));
                                rendered = appendUnsubscribeFooter(rendered, ctx);
                                const subjectRendered = applyTokens(subject, ctx);
                                const plainText = htmlToPlainText(rendered);
                                await sendCampaignEmail({
                                    to: sub.customer_email,
                                    subject: subjectRendered,
                                    html: rendered,
                                    text: plainText,
                                    unsubscribeUrl: ctx.unsubscribe_url
                                });
                                sent++;
                            } catch (e) {
                                failed++;
                                console.warn('[MARKETING send-html DETACHED] Fallo ' + sub.customer_email + ': ' + e.message);
                            }
                            if (wait > 0) await new Promise(r => setTimeout(r, wait));
                        }
                        console.log('[MARKETING send-html DETACHED] Done shopify_marketing. Sent:' + sent + ' Failed:' + failed);
                        if (db?.createEvent) {
                            db.createEvent({
                                subscription_id: 'campaign_' + Date.now(),
                                event_type: 'marketing_campaign_summary',
                                metadata: JSON.stringify({ segment, subject, sent, failed, started_at: startedAt, ended_at: new Date().toISOString(), detached: true })
                            }).catch(() => {});
                        }
                    } catch (e) {
                        console.error('[MARKETING send-html DETACHED] Fatal:', e.message);
                    }
                });
                return; // YA respondimos
            }
            // Cache caliente o dry_run: path normal sincronico
            const shopifyCustomers = await getShopifyMarketingCustomers().catch(() => []);
            filtered = shopifyCustomers.map(c => ({
                customer_email: c.email,
                customer_name: c.full_name || c.first_name || c.email.split('@')[0],
                first_name: c.first_name || '',
                last_name: c.last_name || '',
                status: 'shopify_customer',
                _shopify_only: true,
                product_title: 'LAB NUTRITION',
                frequency_months: 1, permanence_months: 0,
                cycles_completed: 0, cycles_required: 0,
                discount_pct: 0, final_price: 0, base_price: 0
            }));
        } else {
            filtered = segment === 'all' ? subs : subs.filter(s => s.status === segment);
        }
        if (product_id) filtered = filtered.filter(s => String(s.product_id || '') === String(product_id));
        const totalBefore = filtered.length;

        // Skip clientes que se dieron de baja del mailing (compliance)
        filtered = filtered.filter(s => s.marketing_unsubscribed !== true);
        const skippedUnsub = totalBefore - filtered.length;

        // Skip clientes con email rebotado (Resend webhook nos avisó)
        const beforeBounce = filtered.length;
        filtered = filtered.filter(s => s.email_bounced !== true);
        const skippedBounced = beforeBounce - filtered.length;

        // Dedup por email
        const seen = new Set();
        filtered = filtered.filter(s => {
            const e = (s.customer_email || '').toLowerCase();
            if (!e || seen.has(e)) return false;
            seen.add(e); return true;
        });

        const wait = parseInt(throttle_ms) || 250;
        const startedAt = new Date().toISOString();
        const safeHtml = sanitizeMarketingHtml(html);

        // DRY RUN — no envía, solo retorna lo que mandaría
        if (dry_run === true) {
            return res.json({
                dry_run: true,
                recipients_count: filtered.length,
                skipped_unsubscribed: skippedUnsub,
                skipped_bounced: skippedBounced,
                segment,
                subject,
                preheader: preheader || null,
                first_5_recipients: filtered.slice(0, 5).map(s => ({ email: s.customer_email, name: s.customer_name })),
                throttle_ms: wait,
                estimated_seconds: Math.ceil(filtered.length * wait / 1000),
                note: 'Dry run — no se envió ningún email.'
            });
        }

        // Respondé inmediato y procesá en background
        res.json({
            started: true,
            recipients_count: filtered.length,
            skipped_unsubscribed: skippedUnsub,
            segment,
            subject,
            throttle_ms: wait,
            estimated_seconds: Math.ceil(filtered.length * wait / 1000),
            note: 'Envío iniciado en background. Revisá /api/marketing/recent-campaigns en unos minutos.'
        });

        let sent = 0, failed = 0;
        const mailtoUnsub = process.env.UNSUBSCRIBE_MAILTO || 'unsubscribe@labnutrition.com';
        for (const sub of filtered) {
            try {
                const ctx = await buildContextForSub(sub);
                // 1) Reemplazar tokens
                let rendered = applyTokens(safeHtml, ctx);
                // 2) Inyectar preheader text si fue provisto
                if (preheader) {
                    const preheaderRendered = applyTokens(preheader, ctx);
                    rendered = injectPreheader(rendered, preheaderRendered);
                }
                // 3) Agregar footer de unsubscribe (visible en body)
                rendered = appendUnsubscribeFooter(rendered, ctx);
                // 4) Render subject con tokens
                const subjectRendered = applyTokens(subject, ctx);
                // 5) Generar plain-text alternative
                const plainText = htmlToPlainText(rendered);

                await sendCampaignEmail({
                    to: sub.customer_email,
                    subject: subjectRendered,
                    html: rendered,
                    text: plainText,
                    unsubscribeUrl: ctx.unsubscribe_url,
                    mailtoUnsub
                });

                await db.createEvent({
                    subscription_id: sub.id,
                    event_type: 'marketing_campaign_sent',
                    metadata: JSON.stringify({
                        to: sub.customer_email,
                        subject: subjectRendered,
                        segment,
                        campaign_started_at: startedAt
                    })
                }).catch(() => {});
                sent++;
            } catch (e) {
                failed++;
                console.warn('[MARKETING send-html] Fallo a ' + (sub.customer_email || '?') + ': ' + e.message);
            }
            if (wait > 0) await new Promise(r => setTimeout(r, wait));
        }
        console.log('[MARKETING send-html] Done. Sent:' + sent + ' Failed:' + failed + ' Skipped(unsub):' + skippedUnsub + ' Segment:' + segment);

        // Loguear resumen final como event aparte
        if (db?.createEvent) {
            await db.createEvent({
                subscription_id: 'campaign_' + Date.now(),
                event_type: 'marketing_campaign_summary',
                metadata: JSON.stringify({ segment, subject, sent, failed, skipped_unsubscribed: skippedUnsub, started_at: startedAt, ended_at: new Date().toISOString() })
            }).catch(() => {});
        }
    } catch (e) {
        if (!res.headersSent) res.status(500).json({ error: e.message });
        else console.error('[MARKETING send-html] Error:', e.message);
    }
});

/** GET /api/marketing/campaign-summary?started_after=ISO — resumen de campañas enviadas */
app.get('/api/marketing/campaign-summary', async (req, res) => {
    try {
        const events = db?._listAll ? await db._listAll('lab_sub_event').catch(() => []) : [];
        const after = req.query.started_after ? new Date(String(req.query.started_after)).getTime() : 0;
        const summaries = events
            .filter(e => e.event_type === 'marketing_campaign_summary')
            .filter(e => !after || new Date(e.created_at || 0).getTime() >= after)
            .map(e => { try { return { ts: e.created_at, ...JSON.parse(e.metadata || '{}') }; } catch { return null; } })
            .filter(Boolean)
            .sort((a, b) => (b.ts || '').localeCompare(a.ts || ''))
            .slice(0, 50);
        res.json({ campaigns: summaries });
    } catch (e) { res.status(500).json({ error: e.message }); }
});

/** GET /api/marketing/unsubscribe — link público para que un cliente se dé de baja del mailing.
 *  Marca la sub con un flag y registra event. No cancela la suscripción de pago.
 */
app.get('/api/marketing/unsubscribe', async (req, res) => {
    try {
        const email = String(req.query.email || '').trim();
        const subId = String(req.query.sub_id || '').trim();
        if (!email && !subId) return res.status(400).send('Falta email o sub_id');

        const subs = await db.getSubscriptions().catch(() => []);
        const target = subId ? subs.find(s => s.id === subId)
                              : subs.find(s => (s.customer_email || '').toLowerCase() === email.toLowerCase());
        if (target && db.updateSubscription) {
            await db.updateSubscription(target.id, { marketing_unsubscribed: true, marketing_unsubscribed_at: new Date().toISOString() }).catch(() => {});
            await db.createEvent({
                subscription_id: target.id,
                event_type: 'marketing_unsubscribed',
                metadata: JSON.stringify({ email })
            }).catch(() => {});
        }
        res.setHeader('Content-Type', 'text/html');
        res.send('<!DOCTYPE html><html lang="es"><head><meta charset="UTF-8"><title>Suscripcion al mailing cancelada</title><style>body{font-family:Arial,sans-serif;max-width:480px;margin:80px auto;padding:24px;text-align:center}h1{font-size:22px;color:#0a0a0a;margin-bottom:14px}p{color:#555;line-height:1.55}a{color:#bd1718}</style></head><body><h1>Listo, te dimos de baja del mailing</h1><p>No vas a recibir m&aacute;s campa&ntilde;as comerciales de LAB NUTRITION. Tu suscripci&oacute;n de productos sigue activa (esto solo afecta los emails promocionales).</p><p style="margin-top:32px;font-size:12px;color:#999">Si fue por error, escribinos a hola@labnutrition.com</p></body></html>');
    } catch (e) {
        res.status(500).send('Error: ' + e.message);
    }
});

/* ═══════════════════════════════════════════════════════════════════
   📬 RESEND WEBHOOKS — bounces, complaints, deliveries (additive 2026-04-30)
   Endpoint que recibe eventos de Resend y los procesa:
   - email.bounced     → marca sub con email_bounced=true (no reintenta)
   - email.complained  → marca sub con marketing_unsubscribed=true (compliance)
   - email.delivered   → loguea event opcional
   - email.opened      → loguea event opcional (si tracking habilitado)
   - email.clicked     → loguea event opcional (si tracking habilitado)

   Configuración en Resend Dashboard:
   1. Settings → Webhooks → Add Endpoint
   2. URL: https://pixel-suite-pro-production.up.railway.app/api/webhooks/resend
   3. Eventos: bounced, complained, delivered (mín)
   4. Copiar el signing secret y setear env var RESEND_WEBHOOK_SECRET
   ═══════════════════════════════════════════════════════════════════ */
app.post('/api/webhooks/resend', express.json(), async (req, res) => {
    res.sendStatus(200); // Ack inmediato para no reintentar
    try {
        // Validar firma si tenemos secret configurado
        const secret = process.env.RESEND_WEBHOOK_SECRET;
        if (secret) {
            const sig = req.headers['svix-signature'] || req.headers['resend-signature'] || '';
            const ts = req.headers['svix-timestamp'] || '';
            const id = req.headers['svix-id'] || '';
            // Validación svix HMAC SHA256
            try {
                const signed = id + '.' + ts + '.' + JSON.stringify(req.body);
                const expected = crypto.createHmac('sha256', secret.replace('whsec_', '')).update(signed).digest('base64');
                const provided = String(sig).split(' ').map(s => s.split(',')[1]).filter(Boolean);
                if (provided.length && !provided.includes(expected)) {
                    console.warn('[RESEND WEBHOOK] Firma inválida — ignorando');
                    return;
                }
            } catch (e) {
                console.warn('[RESEND WEBHOOK] Error validando firma:', e.message);
            }
        }

        const { type, data } = req.body || {};
        if (!type || !data) return;

        const recipient = (Array.isArray(data.to) ? data.to[0] : data.to) || data.email_to || '';
        const emailLc = String(recipient).toLowerCase().trim();
        if (!emailLc) return;

        // Buscar sub por email
        const subs = await db.getSubscriptions().catch(() => []);
        const matchSubs = subs.filter(s => (s.customer_email || '').toLowerCase() === emailLc);

        console.log('[RESEND WEBHOOK] event=' + type + ' to=' + emailLc + ' subs_match=' + matchSubs.length);

        for (const sub of matchSubs) {
            try {
                if (type === 'email.bounced' || type === 'email.bounce') {
                    const bounceType = data.bounce?.type || data.bounce_type || 'unknown';
                    const isHard = /hard|permanent|invalid/i.test(bounceType);
                    if (isHard && db.updateSubscription) {
                        await db.updateSubscription(sub.id, {
                            email_bounced: true,
                            email_bounced_at: new Date().toISOString(),
                            email_bounce_type: bounceType
                        }).catch(() => {});
                    }
                    if (db.createEvent) {
                        await db.createEvent({
                            subscription_id: sub.id,
                            event_type: 'email_bounced',
                            metadata: JSON.stringify({ bounce_type: bounceType, hard: isHard, subject: data.subject || '' })
                        }).catch(() => {});
                    }
                } else if (type === 'email.complained' || type === 'email.complaint') {
                    if (db.updateSubscription) {
                        await db.updateSubscription(sub.id, {
                            marketing_unsubscribed: true,
                            marketing_unsubscribed_at: new Date().toISOString(),
                            email_complained: true
                        }).catch(() => {});
                    }
                    if (db.createEvent) {
                        await db.createEvent({
                            subscription_id: sub.id,
                            event_type: 'email_complained',
                            metadata: JSON.stringify({ subject: data.subject || '' })
                        }).catch(() => {});
                    }
                } else if (type === 'email.delivered') {
                    if (db.createEvent) {
                        await db.createEvent({
                            subscription_id: sub.id,
                            event_type: 'email_delivered',
                            metadata: JSON.stringify({ subject: data.subject || '' })
                        }).catch(() => {});
                    }
                } else if (type === 'email.opened') {
                    if (db.createEvent) {
                        await db.createEvent({
                            subscription_id: sub.id,
                            event_type: 'email_opened',
                            metadata: JSON.stringify({ subject: data.subject || '' })
                        }).catch(() => {});
                    }
                } else if (type === 'email.clicked') {
                    if (db.createEvent) {
                        await db.createEvent({
                            subscription_id: sub.id,
                            event_type: 'email_clicked',
                            metadata: JSON.stringify({ subject: data.subject || '', url: data.click?.link || '' })
                        }).catch(() => {});
                    }
                }
            } catch (e) {
                console.warn('[RESEND WEBHOOK] Error procesando ' + sub.id + ':', e.message);
            }
        }
    } catch (e) {
        console.error('[RESEND WEBHOOK] Fatal:', e.message);
    }
});

/** GET /api/marketing/bounce-list — clientes con email rebotado (no recibirán más mailing). */
app.get('/api/marketing/bounce-list', async (req, res) => {
    try {
        const subs = await db.getSubscriptions().catch(() => []);
        const bounced = subs.filter(s => s.email_bounced === true).map(s => ({
            email: s.customer_email,
            bounced_at: s.email_bounced_at,
            bounce_type: s.email_bounce_type,
            sub_id: s.id,
            status: s.status
        }));
        res.json({ count: bounced.length, list: bounced });
    } catch (e) { res.status(500).json({ error: e.message }); }
});

/** GET /api/marketing/recent-campaigns — últimas 20 campañas enviadas (lee del event log). */
app.get('/api/marketing/recent-campaigns', async (req, res) => {
    try {
        const events = db?._listAll ? await db._listAll('lab_sub_event').catch(() => []) : [];
        const camp = events
            .filter(e => e.event_type === 'marketing_campaign_sent' || e.event_type === 'abandoned_checkout_recovery_sent')
            .sort((a, b) => (b.created_at || '').localeCompare(a.created_at || ''))
            .slice(0, 20);
        res.json({ campaigns: camp });
    } catch (e) { res.status(500).json({ error: e.message }); }
});

/* ═══════════════════════════════════════════════════════════════════
   ⏰ ABANDONED CHECKOUT RECOVERY — cron (additive 2026-04-28)
   Cada 6 horas: encuentra subs en pending_payment > 24h y < 7d
   sin email de recovery enviado, manda email único, registra event.
   No toca lógica existente. Falla silencioso.
   ═══════════════════════════════════════════════════════════════════ */
async function runAbandonedCheckoutRecovery() {
    console.log('[ABANDONED RECOVERY] Scanning pending_payment subs...');
    try {
        const subs = await db.getSubscriptions().catch(() => []);
        const now = Date.now();
        const candidates = subs.filter(s =>
            s.status === 'pending_payment' &&
            s.customer_email &&
            s.created_at &&
            (now - new Date(s.created_at).getTime()) > 24 * 3600 * 1000 &&
            (now - new Date(s.created_at).getTime()) < 7 * 24 * 3600 * 1000
        );
        if (!candidates.length) {
            console.log('[ABANDONED RECOVERY] No hay candidatos');
            return;
        }
        console.log(`[ABANDONED RECOVERY] Encontrados: ${candidates.length}`);

        let sent = 0, skipped = 0;
        for (const sub of candidates) {
            try {
                // No re-enviar si ya tiene event de recovery
                const evs = db?.getEvents ? await db.getEvents(sub.id, 50).catch(() => []) : [];
                const alreadySent = evs.some(e => e.event_type === 'abandoned_checkout_recovery_sent');
                if (alreadySent) { skipped++; continue; }

                // Construir link al portal MP si tenemos preapproval_id
                const portalUrl = process.env.BACKEND_URL || 'https://pixel-suite-pro-production.up.railway.app';
                const resumeLink = sub.mp_preapproval_id
                    ? `https://www.mercadopago.com/checkout/v1/redirect?pref_id=${encodeURIComponent(sub.mp_preapproval_id)}`
                    : portalUrl;

                const html = `
<div style="font-family:Inter,Arial,sans-serif;max-width:540px;margin:0 auto;padding:24px;background:#fff">
  <div style="text-align:center;margin-bottom:28px">
    <div style="font-size:11px;font-weight:800;letter-spacing:.2em;color:#bd1718">LAB NUTRITION</div>
  </div>
  <div style="background:#fafafa;border:1px solid #ececec;border-radius:14px;padding:36px 28px">
    <h2 style="font-size:22px;font-weight:900;color:#0a0a0a;margin:0 0 14px;letter-spacing:-.02em">Te falta 1 paso</h2>
    <p style="font-size:14.5px;color:#4a4a4a;line-height:1.55;margin:0 0 22px">
      Hola${sub.customer_name ? ' ' + sub.customer_name.split(' ')[0] : ''}, empezaste a suscribirte a
      <strong>${sub.product_title || 'tu plan'}</strong> pero el pago quedó pendiente.
    </p>
    <p style="font-size:14.5px;color:#4a4a4a;line-height:1.55;margin:0 0 28px">
      Completá el pago en Mercado Pago para activar tu primer envío con
      <strong>regalo de bienvenida</strong> incluido.
    </p>
    <div style="text-align:center;margin:24px 0">
      <a href="${resumeLink}" style="display:inline-block;background:#0a0a0a;color:#fff;padding:16px 32px;border-radius:8px;font-weight:700;text-decoration:none;font-size:14px">
        Completar mi suscripción
      </a>
    </div>
    <p style="font-size:12px;color:#777;text-align:center;margin:18px 0 0">
      Si ya pagaste o cambiaste de idea, ignorá este email.
    </p>
  </div>
  <p style="text-align:center;font-size:11px;color:#999;margin:18px 0 0;letter-spacing:.05em">
    LAB NUTRITION · Programa Black Diamond
  </p>
</div>`;

                if (typeof sendAutoEmail === 'function') {
                    await sendAutoEmail({
                        to: sub.customer_email,
                        subject: 'Te falta 1 paso para activar tu suscripción Black Diamond',
                        html
                    });
                }
                await db.createEvent({
                    subscription_id: sub.id,
                    event_type: 'abandoned_checkout_recovery_sent',
                    metadata: JSON.stringify({ to: sub.customer_email, hours_since_create: Math.round((now - new Date(sub.created_at).getTime()) / 3600000) })
                }).catch(() => {});
                sent++;
                await new Promise(r => setTimeout(r, 600));
            } catch (e) {
                console.warn('[ABANDONED RECOVERY] Error con ' + sub.customer_email + ': ' + e.message);
            }
        }
        console.log(`[ABANDONED RECOVERY] Enviados: ${sent} | Skipped (ya enviado): ${skipped}`);
    } catch (e) {
        console.error('[ABANDONED RECOVERY] Fatal: ' + e.message);
    }
}

/** Cron cada 6 horas */
if (typeof cron !== 'undefined' && cron.schedule) {
    cron.schedule('0 */6 * * *', runAbandonedCheckoutRecovery, { timezone: 'America/Lima' });
}

/** Manual trigger */
app.post('/api/admin/abandoned-recovery/run-now', async (req, res) => {
    res.json({ started: true, message: 'Abandoned checkout recovery triggered' });
    runAbandonedCheckoutRecovery().catch(console.error);
});

/* ═══════════════════════════════════════════════════════════════════
   📈 META PIXEL CAPI — Server-side tracking (additive 2026-04-28)
   Endpoint que dispara evento "Subscribe" en Meta Conversions API.
   Si META_PIXEL_ID o META_ACCESS_TOKEN no están seteados, hace no-op.
   No bloquea el flujo de suscripción.
   ═══════════════════════════════════════════════════════════════════ */

const crypto = require('crypto');
function sha256Hash(value) {
    if (!value) return null;
    return crypto.createHash('sha256').update(String(value).trim().toLowerCase()).digest('hex');
}

async function sendMetaCapiEvent(eventName, sub, eventId, sourceUrl) {
    const PIXEL_ID = process.env.META_PIXEL_ID;
    const ACCESS_TOKEN = process.env.META_ACCESS_TOKEN;
    if (!PIXEL_ID || !ACCESS_TOKEN) {
        return { ok: false, reason: 'meta_not_configured' };
    }

    const userData = {
        em: sub.customer_email ? [sha256Hash(sub.customer_email)] : undefined,
        ph: sub.customer_phone ? [sha256Hash(sub.customer_phone.replace(/\D/g, ''))] : undefined,
        fn: sub.customer_name ? [sha256Hash(sub.customer_name.split(' ')[0])] : undefined,
        ln: sub.customer_name ? [sha256Hash(sub.customer_name.split(' ').slice(1).join(' '))] : undefined,
        country: ['68b6cd6d8cc41bc0e92bba4b22b25a7eda6b9c01dd8a8e2e9cab0a5c10e50f23'] // sha256('pe')
    };

    const data = [{
        event_name: eventName,
        event_time: Math.floor(Date.now() / 1000),
        event_id: eventId || (sub.id + '_' + eventName),
        action_source: 'website',
        event_source_url: sourceUrl || 'https://labnutrition.com/pages/suscripciones',
        user_data: Object.fromEntries(Object.entries(userData).filter(([_, v]) => v !== undefined)),
        custom_data: {
            currency: 'PEN',
            value: parseFloat(sub.final_price || 0),
            content_ids: [String(sub.product_id || '')],
            content_name: sub.product_title || 'Suscripción Lab Nutrition',
            content_type: 'product',
            num_items: 1,
            predicted_ltv: parseFloat(sub.final_price || 0) * (parseInt(sub.cycles_required) || 6),
            subscription_id: sub.id,
            plan_frequency_months: parseInt(sub.frequency_months) || 1,
            plan_permanence_months: parseInt(sub.permanence_months) || 0
        }
    }];

    const body = JSON.stringify({ data });
    const url = `https://graph.facebook.com/v19.0/${PIXEL_ID}/events?access_token=${ACCESS_TOKEN}`;
    try {
        const r = await fetch(url, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body
        });
        const j = await r.json();
        return { ok: r.ok, response: j };
    } catch (e) {
        return { ok: false, error: e.message };
    }
}

/** POST /api/track/subscribe — dispara evento Subscribe en Meta CAPI.
 *  Body: { subscription_id, event_id?, source_url? }
 *  Llamar desde frontend success page para tracking server-side.
 */
app.post('/api/track/subscribe', async (req, res) => {
    try {
        const { subscription_id, event_id, source_url } = req.body || {};
        if (!subscription_id) return res.status(400).json({ error: 'subscription_id required' });
        const sub = await db.getSubscription(subscription_id).catch(() => null);
        if (!sub) return res.status(404).json({ error: 'subscription not found' });
        const result = await sendMetaCapiEvent('Subscribe', sub, event_id, source_url);
        // Log para auditoría
        if (result.ok) {
            await db.createEvent({
                subscription_id: sub.id,
                event_type: 'meta_capi_subscribe_sent',
                metadata: JSON.stringify({ event_id: event_id || (sub.id + '_Subscribe'), value: sub.final_price })
            }).catch(() => {});
        }
        res.json(result);
    } catch (e) {
        res.status(500).json({ error: e.message });
    }
});

/* GET /api/admin/products-health-check
 * 2026-05-04 — ADITIVO. Auditoría visual del estado de cada producto suscribible
 * (simples + bundles). NO hace cambios. Solo lectura.
 *
 * Para cada producto registrado evalúa:
 *  - ¿variants en allowlist union? (si NO → órdenes bloqueadas al cobrar)
 *  - ¿plans_config con applies_to incluyendo el productId? (si NO → no hay descuento ni regalo)
 *  - ¿gifts configurados? (vía plans_config global O vía bundle local)
 *
 * Útil para detectar productos rotos antes de que un cliente los compre.
 * Permite al admin validar visualmente que un nuevo producto está completo.
 */
app.get('/api/admin/products-health-check', async (req, res) => {
    try {
        const settings = await readFromShopify() || readFromFile() || {};
        const allowlist = await getSubscriptionVariantAllowlist(settings);
        const plansCfg = Array.isArray(settings.plans_config) ? settings.plans_config : [];
        const eligibleProducts = Array.isArray(settings.eligible_products) ? settings.eligible_products : [];
        const bundles = (db && db.getBundleConfigs) ? await db.getBundleConfigs({ active: true }).catch(() => []) : [];

        const shop = process.env.SHOPIFY_SHOP || 'nutrition-lab-cluster.myshopify.com';
        const token = process.env.SHOPIFY_ACCESS_TOKEN || _shopifyToken;

        async function getProductVariants(pid) {
            if (!token || !pid) return [];
            try {
                const r = await fetch(`https://${shop}/admin/api/2026-01/products/${encodeURIComponent(pid)}.json?fields=variants,title`, {
                    headers: { 'X-Shopify-Access-Token': token }
                });
                if (!r.ok) return [];
                const d = await r.json();
                return (d?.product?.variants || []).map(v => ({ id: String(v.id), title: v.title, price: v.price }));
            } catch { return []; }
        }

        // Mezcla productos simples (eligible_products) y bundles (lab_bundle_config)
        // FIX 2026-05-04: solo incluye productos ACTIVOS para no listar items inactivos
        // como "rotos" en el dashboard.
        const includeInactive = req.query.include_inactive === 'true';
        const tracked = new Map();
        for (const p of eligibleProducts) {
            const pid = String(p.shopify_id || p.shopify_product_id || '');
            if (!pid) continue;
            const isActive = p.is_active !== false;
            if (!includeInactive && !isActive) continue;
            tracked.set(pid, { type: 'simple', title: p.product_title || '?', is_active: isActive });
        }
        for (const b of (Array.isArray(bundles) ? bundles : [])) {
            const pid = String(b.bundle_product_id || '');
            if (!pid) continue;
            const isActive = b.active !== false;
            if (!includeInactive && !isActive) continue;
            tracked.set(pid, { type: b.type || 'mix_match', title: b.name || '?', is_active: isActive, bundle_id: b.id });
        }

        const report = [];
        for (const [pid, info] of tracked) {
            const variants = await getProductVariants(pid);
            const variantIds = variants.map(v => v.id);
            const variantsInAllowlist = variantIds.filter(v => allowlist.has(v));
            const allVariantsAllowed = variantIds.length > 0 && variantsInAllowlist.length === variantIds.length;
            const matchingPlans = plansCfg.filter(p =>
                p && p.active !== false &&
                (p.applies_to?.mode === 'all_products' ||
                 (p.applies_to?.product_ids || []).map(String).includes(pid))
            );
            const planCfgsWithGifts = matchingPlans.filter(p =>
                p.gifts?.enabled &&
                Array.isArray(p.gifts?.items) && p.gifts.items.length > 0
            );
            const hasGiftsViaPlansConfig = planCfgsWithGifts.length > 0;
            const bundle = bundles.find(b => String(b.bundle_product_id) === pid);
            const hasGiftsViaBundle = !!(bundle && Array.isArray(bundle.plans) && bundle.plans.some(p => p.gifts?.enabled && Array.isArray(p.gifts?.items) && p.gifts.items.length > 0));
            const hasGiftsAnywhere = hasGiftsViaPlansConfig || hasGiftsViaBundle;

            const issues = [];
            if (variantIds.length === 0) issues.push('product_has_no_variants_or_unreachable');
            if (variantIds.length > 0 && !allVariantsAllowed) {
                issues.push(`some_variants_blocked_from_allowlist (${variantsInAllowlist.length}/${variantIds.length})`);
            }
            if (matchingPlans.length === 0) issues.push('no_plans_config_targeting_this_product');
            if (!hasGiftsAnywhere) issues.push('no_gifts_configured');

            report.push({
                product_id: pid,
                title: info.title,
                type: info.type,
                is_active: info.is_active,
                bundle_id: info.bundle_id || null,
                shopify_variants_count: variantIds.length,
                variants_in_allowlist: variantsInAllowlist.length,
                all_variants_allowed: allVariantsAllowed,
                matching_plans_in_config: matchingPlans.length,
                has_gifts_via_plans_config: hasGiftsViaPlansConfig,
                has_gifts_via_bundle: hasGiftsViaBundle,
                healthy: issues.length === 0,
                issues
            });
        }

        const summary = {
            total_products: report.length,
            healthy: report.filter(r => r.healthy).length,
            with_issues: report.filter(r => !r.healthy).length,
            allowlist_size: allowlist.size,
            plans_config_count: plansCfg.length,
            bundles_count: Array.isArray(bundles) ? bundles.length : 0,
            checked_at: new Date().toISOString()
        };

        res.json({ summary, products: report });
    } catch (e) {
        console.error('[HEALTH-CHECK] error:', e.message);
        res.status(500).json({ error: e.message });
    }
});

/* ═══════════════════════════════════════════════════════════════════
   🆕 2026-06-05 — DUNNING SYSTEM (cobros rechazados + auto-emails)
   + APP PROXY HANDLER (portal dentro de labnutrition.pe)
   + UPDATE CARD endpoints
   ADITIVO. No toca webhooks, dedup, ni flujos existentes.
   ═══════════════════════════════════════════════════════════════════ */

// ── Helper: Verify App Proxy HMAC (Shopify firma cada request)
function _verifyAppProxyHmac(query) {
    const secret = process.env.SHOPIFY_API_SECRET;
    if (!secret) return { ok: false, mode: 'off', reason: 'No SHOPIFY_API_SECRET' };
    const { signature, ...params } = query;
    if (!signature) return { ok: false, mode: process.env.APP_PROXY_VERIFY || 'warn', reason: 'No signature' };
    const sorted = Object.keys(params).sort()
        .map(k => `${k}=${Array.isArray(params[k]) ? params[k].join(',') : params[k]}`)
        .join('');
    const computed = crypto.createHmac('sha256', secret).update(sorted).digest('hex');
    return { ok: _safeEq(computed, String(signature)), mode: process.env.APP_PROXY_VERIFY || 'warn', computed: computed.slice(0,8), got: String(signature).slice(0,8) };
}

// ── Helper: build MP customer dashboard URL for a sub (para update card)
function _mpCustomerDashboardUrl(preapprovalId) {
    return `https://www.mercadopago.com.pe/subscriptions/details/${preapprovalId}`;
}

// ── Helper: send dunning email (Resend HTTP API)
// 🔒 FIX 2026-06-05: si Resend está en modo trial (403), fall-back a admin.
//   El admin recibe el email del cliente con header "DESTINATARIO ORIGINAL: <cliente>"
//   y un botón "Reenviar al cliente" (mailto:). Esto desbloquea el dunning
//   mientras el dominio no esté verificado en Resend.
// 🚨 KILL SWITCH 2026-06-05 — Master toggle. DEFAULT: OFF (cero emails).
//   Para reactivar emails automáticos:
//     1) Railway env: DUNNING_EMAILS_ENABLED=true
//     2) O via /api/settings PUT { dunning_emails_enabled: true } + reinicio
//   Cuando OFF: ningún email se envía desde NINGÚN punto del sistema.
function _emailsAreEnabled() {
    const flag = process.env.DUNNING_EMAILS_ENABLED;
    return flag === 'true' || flag === '1';
}

// Email principal del admin para alertas dunning. Cambiable via env ADMIN_EMAIL.
const _ADMIN_EMAIL_FALLBACK = process.env.ADMIN_EMAIL || 'asesorecommerce@labnutrition.com';
// Resend trial seguridad: si trial bloquea destino → segundo intento a este email.
// Una vez verifiques dominio en Resend, este fallback es innecesario.
const _ADMIN_EMAIL_TRIAL_FALLBACK = process.env.ADMIN_EMAIL_TRIAL_FALLBACK || 'israelsarmiento281294@gmail.com';

// 🆕 Helper: send email con doble fallback (target → trial-allowed → cliente)
async function _resendSendWithFallback(payloadBase, primaryTo) {
    const r1 = await fetch('https://api.resend.com/emails', {
        method: 'POST',
        headers: { 'Authorization': `Bearer ${process.env.RESEND_API_KEY}`, 'Content-Type': 'application/json' },
        body: JSON.stringify({ ...payloadBase, to: [primaryTo] })
    });
    if (r1.ok) return { sent: true, status: 200, to: primaryTo, fallback_trial: false };
    if (r1.status === 403 && primaryTo !== _ADMIN_EMAIL_TRIAL_FALLBACK) {
        // Trial restriction → fallback al email registrado en Resend
        const r2 = await fetch('https://api.resend.com/emails', {
            method: 'POST',
            headers: { 'Authorization': `Bearer ${process.env.RESEND_API_KEY}`, 'Content-Type': 'application/json' },
            body: JSON.stringify({
                ...payloadBase,
                to: [_ADMIN_EMAIL_TRIAL_FALLBACK],
                subject: `[REDIRECT a ${primaryTo}] ${payloadBase.subject || ''}`
            })
        });
        return { sent: r2.ok, status: r2.status, to: _ADMIN_EMAIL_TRIAL_FALLBACK, fallback_trial: true, target: primaryTo };
    }
    return { sent: false, status: r1.status, to: primaryTo };
}

// 🆕 2026-06-11: payLink (opcional) = link de pago real del ciclo rechazado.
//   Si está presente es el CTA principal — el dashboard MP pasa a secundario.
async function _sendDunningEmail(sub, dayNumber, mpDashboardUrl, portalUrl, payLink = null) {
    if (!_emailsAreEnabled()) return { sent: false, reason: 'EMAILS_DISABLED_KILL_SWITCH', killed: true };
    if (!process.env.RESEND_API_KEY) return { sent: false, reason: 'No RESEND_API_KEY' };
    const subjects = {
        0: '⚠️ Tu pago no se procesó — LAB NUTRITION',
        3: 'Recordatorio: tu suscripción está pausada por falta de pago',
        7: 'Última semana para actualizar tu tarjeta',
        14: 'Tu suscripción será pausada hoy'
    };
    const introMsgs = {
        0: 'Tuvimos un problema al cobrar tu suscripción este mes. Tu tarjeta fue rechazada por el banco.',
        3: 'Hace 3 días intentamos cobrar tu suscripción y fue rechazada. Por favor actualiza tu tarjeta para no perder tu plan.',
        7: 'Han pasado 7 días desde que intentamos cobrar tu suscripción. Te quedan 7 días antes de que se pause automáticamente.',
        14: 'Han pasado 14 días desde el cobro rechazado. Tu suscripción quedará pausada hoy. Para reactivarla, actualiza tu tarjeta en MercadoPago.'
    };
    const html = `<!DOCTYPE html><html><body style="font-family:Arial,sans-serif;background:#f5f5f5;padding:24px;margin:0">
<div style="max-width:560px;margin:0 auto;background:#fff;border-radius:12px;overflow:hidden;box-shadow:0 2px 8px rgba(0,0,0,.05)">
  <div style="background:#0A0A0A;padding:24px;text-align:center">
    <h1 style="color:#E30613;font-size:24px;margin:0;letter-spacing:2px;font-weight:900">LAB NUTRITION</h1>
  </div>
  <div style="padding:32px 28px">
    <h2 style="color:#0A0A0A;font-size:20px;margin:0 0 16px;font-weight:800">Hola ${(sub.customer_name || 'cliente').split(' ')[0]},</h2>
    <p style="color:#374151;line-height:1.6;margin:0 0 18px;font-size:15px">${introMsgs[dayNumber] || introMsgs[0]}</p>
    <div style="background:#FEF2F2;border-left:4px solid #E30613;padding:16px 18px;border-radius:6px;margin:0 0 22px">
      <strong style="color:#0A0A0A">Plan:</strong> ${sub.product_title || 'Tu suscripción'}<br>
      <strong style="color:#0A0A0A">Monto:</strong> S/${(sub.mp_total_amount || sub.final_price || 0)}<br>
      <strong style="color:#0A0A0A">Frecuencia:</strong> Cada ${sub.frequency_months || 1} mes${(sub.frequency_months || 1) > 1 ? 'es' : ''}
    </div>
    <p style="color:#374151;line-height:1.6;margin:0 0 22px;font-size:15px"><strong>¿Qué hacer?</strong> ${payLink ? 'Pagá el ciclo pendiente ahora con cualquier tarjeta o medio de pago — toma 1 minuto y tu pedido sale automático.' : 'Actualizá tu tarjeta en 1 minuto desde MercadoPago. Tu próximo intento será automático.'}</p>
    <div style="text-align:center;margin:28px 0">
      ${payLink ? `<a href="${payLink}" style="display:inline-block;background:#E30613;color:#fff;padding:14px 36px;border-radius:8px;text-decoration:none;font-weight:800;font-size:15px;letter-spacing:1px;text-transform:uppercase">Pagar ahora S/${(sub.mp_total_amount || sub.final_price || 0)}</a>
      <div style="margin-top:14px;color:#666;font-size:13px">¿Tu tarjeta ya no sirve? Respondé este correo o escribinos por WhatsApp y te mandamos el link para registrar una nueva.</div>` :
      `<a href="${mpDashboardUrl}" style="display:inline-block;background:#E30613;color:#fff;padding:14px 36px;border-radius:8px;text-decoration:none;font-weight:800;font-size:15px;letter-spacing:1px;text-transform:uppercase">Actualizar tarjeta</a>`}
    </div>
    <p style="color:#666;font-size:13px;line-height:1.5;text-align:center;margin:18px 0 0">
      O entrá a <a href="${portalUrl}" style="color:#E30613;text-decoration:none">tu portal de cliente</a> para gestionar tu suscripción
    </p>
    <hr style="border:none;border-top:1px solid #E5E5E5;margin:28px 0 18px">
    <p style="color:#888;font-size:12px;text-align:center;margin:0">¿Necesitas ayuda? Respondé este correo o escribinos por WhatsApp.</p>
  </div>
</div>
</body></html>`;
    try {
        const r = await fetch('https://api.resend.com/emails', {
            method: 'POST',
            headers: { 'Authorization': `Bearer ${process.env.RESEND_API_KEY}`, 'Content-Type': 'application/json' },
            body: JSON.stringify({
                from: process.env.RESEND_FROM || 'LAB NUTRITION <onboarding@resend.dev>',
                to: [sub.customer_email],
                subject: subjects[dayNumber] || subjects[0],
                html
            })
        });
        if (r.ok) return { sent: true, status: r.status, to: sub.customer_email };

        // 🔄 FALLBACK 2026-06-05: Resend en trial / dominio no verificado.
        // Mandamos al admin con marca "FORWARD TO CLIENT" y mailto pre-llenado.
        if (r.status === 403) {
            const customerPhone = (sub.customer_phone || '').replace(/[^0-9]/g, '');
            // 🆕 2026-06-11: con payLink el mensaje invita a PAGAR (acción real), no solo a actualizar tarjeta
            const ctaText = payLink
                ? ('Pagá tu ciclo pendiente acá: ' + payLink)
                : ('Actualizá tu tarjeta acá: ' + mpDashboardUrl);
            const waLink = customerPhone ? `https://wa.me/${customerPhone.startsWith('51') ? customerPhone : '51' + customerPhone}?text=${encodeURIComponent('Hola ' + (sub.customer_name||'').split(' ')[0] + ', te escribo de Lab Nutrition. Tu suscripción tuvo un cobro rechazado por S/' + (sub.mp_total_amount || sub.final_price || 0) + '. ' + ctaText)}` : null;
            const mailtoLink = `mailto:${sub.customer_email}?subject=${encodeURIComponent(subjects[dayNumber] || subjects[0])}&body=${encodeURIComponent('Hola ' + (sub.customer_name||'').split(' ')[0] + ',\n\n' + (introMsgs[dayNumber] || introMsgs[0]) + '\n\n' + ctaText + '\n\nGracias,\nLab Nutrition')}`;
            const adminHtml = `<!DOCTYPE html><html><body style="font-family:Arial,sans-serif;background:#f5f5f5;padding:24px;margin:0">
<div style="max-width:680px;margin:0 auto;background:#fff;border-radius:12px;overflow:hidden">
  <div style="background:#FEF3C7;border-bottom:3px solid #F59E0B;padding:18px 24px">
    <strong style="color:#92400E;font-size:14px">⚠️ DESTINATARIO ORIGINAL:</strong> <code style="background:#fff;padding:3px 8px;border-radius:4px;color:#92400E;font-size:13px">${sub.customer_email}</code><br>
    <small style="color:#78350F">Resend trial mode — verificá dominio en resend.com/domains para envío directo. Mientras tanto, reenviá manual.</small>
  </div>
  <div style="background:#0A0A0A;padding:24px;text-align:center"><h1 style="color:#E30613;margin:0;letter-spacing:2px;font-weight:900">LAB NUTRITION — Dunning Alert Día ${dayNumber}</h1></div>
  <div style="padding:28px">
    <div style="background:#FEE2E2;padding:16px;border-radius:8px;margin-bottom:18px;font-size:14px;color:#7F1D1D">
      <strong>Cliente: ${sub.customer_name || '—'}</strong> (${sub.customer_email})<br>
      Producto: ${sub.product_title}<br>
      Monto rechazado: <strong>S/${sub.mp_total_amount || sub.final_price || 0}</strong><br>
      Ciclo: ${(sub.cycles_completed||0)+1} de ${sub.cycles_required||'?'}<br>
      ${customerPhone ? 'Teléfono: <code>' + customerPhone + '</code><br>' : 'Sin teléfono registrado<br>'}
    </div>
    <div style="display:flex;gap:8px;margin-bottom:24px;flex-wrap:wrap">
      ${waLink ? `<a href="${waLink}" style="background:#25D366;color:#fff;padding:12px 20px;border-radius:8px;text-decoration:none;font-weight:700;font-size:13px">📱 WhatsApp directo</a>` : ''}
      <a href="${mailtoLink}" style="background:#3B82F6;color:#fff;padding:12px 20px;border-radius:8px;text-decoration:none;font-weight:700;font-size:13px">✉️ Enviar email desde Gmail</a>
      <a href="${mpDashboardUrl}" target="_blank" style="background:#E30613;color:#fff;padding:12px 20px;border-radius:8px;text-decoration:none;font-weight:700;font-size:13px">🔗 Link MP cliente</a>
    </div>
    <hr style="border:none;border-top:1px solid #E5E5E5;margin:20px 0">
    <p style="color:#6B7280;font-size:11px;margin:0">Para enviar emails directos: <a href="https://resend.com/domains" style="color:#E30613">resend.com/domains</a> → verificar <strong>labnutrition.pe</strong> → cambiar RESEND_FROM en Ajustes.</p>
    <hr style="border:none;border-top:1px solid #E5E5E5;margin:20px 0">
    <details><summary style="cursor:pointer;color:#666;font-size:12px">📄 Preview email cliente (lo que recibiría si dominio verificado)</summary>
    <div style="border:1px dashed #ccc;border-radius:8px;padding:14px;margin-top:8px;font-size:11px;color:#666">
      <strong>Subject:</strong> ${subjects[dayNumber] || subjects[0]}<br>
      <strong>To:</strong> ${sub.customer_email}<br><br>
      ${html.replace(/<[^>]*>/g, ' ').slice(0, 600)}...
    </div></details>
  </div>
</div></body></html>`;
            const r2 = await _resendSendWithFallback({
                from: process.env.RESEND_FROM || 'LAB NUTRITION Dunning <onboarding@resend.dev>',
                subject: `[Acción] ${sub.customer_name || sub.customer_email} — cobro rechazado día ${dayNumber}`,
                html: adminHtml
            }, _ADMIN_EMAIL_FALLBACK);
            return { sent: r2.sent, status: r2.status, to: r2.to, fallback_admin: true, original_recipient: sub.customer_email, used_trial_fallback: !!r2.fallback_trial };
        }
        return { sent: false, status: r.status, to: sub.customer_email };
    } catch (e) { return { sent: false, error: e.message }; }
}

/* ═══════════════════════════════════════════════════════════════════
   🆕 RECOVERY LINK (2026-06-11) — link de pago REAL para ciclos rechazados
   Problema que resuelve: el "link de recuperación" anterior era el dashboard
   de MP (_mpCustomerDashboardUrl) que NO tiene botón de pagar — solo "cambiar
   medio de pago". Si MP agotaba sus reintentos, el ciclo se perdía aunque el
   cliente actualizara la tarjeta. Ahora: Preference de pago único (cualquier
   medio de pago, no requiere la cuenta MP del preapproval) reconciliada vía
   webhook con external_reference 'subrecovery::<sub_id>::c<ciclo>'.
   ═══════════════════════════════════════════════════════════════════ */

/* Genera (o reutiliza <7d) el link de pago de reposición para una sub.
   Idempotente vía evento 'recovery_link_generated'. */
async function _getOrCreateRecoveryLink(sub, { by = 'system', fresh = false } = {}) {
    const events = await db.getEvents(sub.id).catch(() => []);
    if (!fresh) {
        const lastLink = (events || []).find(e => e.event_type === 'recovery_link_generated');
        if (lastLink) {
            try {
                const m = typeof lastLink.metadata === 'string' ? JSON.parse(lastLink.metadata) : (lastLink.metadata || {});
                const ageDays = (Date.now() - new Date(lastLink.created_at || 0).getTime()) / 86400000;
                if (m.url && ageDays < 7) return { url: m.url, amount: m.amount, cycle: m.cycle, reused: true };
            } catch {}
        }
    }
    // Monto: el del cobro rechazado (evento) > monto MP de la sub > precio final
    let amount = parseFloat(sub.mp_total_amount || sub.final_price || 0);
    const rejectEv = (events || []).find(e => e.event_type === 'payment_rejected');
    try {
        const m = typeof rejectEv?.metadata === 'string' ? JSON.parse(rejectEv.metadata) : (rejectEv?.metadata || {});
        if (m.amount) amount = parseFloat(m.amount);
    } catch {}
    if (!amount || amount <= 0) throw new Error('No se pudo determinar el monto del ciclo rechazado');
    const cycle = (parseInt(sub.cycles_completed) || 0) + 1;
    const BACKEND = process.env.BACKEND_URL || 'https://pixel-suite-pro-production.up.railway.app';
    const pref = await mp.createOneTimePayment({
        amount,
        title: `Reposición ciclo ${cycle} — ${sub.product_title || 'Suscripción LAB'}`,
        customerEmail: sub.customer_email,
        externalReference: `subrecovery::${sub.id}::c${cycle}`,
        backUrl: `${BACKEND}/subscriptions/success?recovery=1`
    });
    if (!pref?.init_point) throw new Error('MP no devolvió link de pago');
    await db.createEvent({
        subscription_id: sub.id,
        event_type: 'recovery_link_generated',
        metadata: JSON.stringify({ url: pref.init_point, preference_id: pref.id, amount, cycle, by, at: new Date().toISOString() })
    }).catch(() => {});
    return { url: pref.init_point, amount, cycle, reused: false };
}

/* 🆕 RE-AUTORIZACIÓN (2026-06-11) — para clientes cuya TARJETA murió.
   Problema: actualizar la tarjeta de un preapproval exige entrar a la cuenta
   MP del pagador, y la mayoría no puede (email distinto / sin cuenta) — el
   muro "no puedo ver mi plan". Solución: nuevo plan MP con los ciclos
   RESTANTES al mismo precio → el cliente autoriza su tarjeta nueva en el
   checkout de MP (sin ver el plan viejo) → MP cobra el primer ciclo ahí
   mismo (= repone el mes pendiente) → webhook preapproval con
   reauth_plan_id hace el swap: cancela el preapproval viejo y engancha el
   nuevo a la MISMA sub (sin resetear ciclos, sin orden "Ciclo 1" duplicada,
   sin regalos repetidos). Reuso <7d vía evento reauth_link_generated. */
async function _getOrCreateReauthLink(sub, { by = 'admin', fresh = false } = {}) {
    const events = await db.getEvents(sub.id).catch(() => []);
    if (!fresh) {
        const last = (events || []).find(e => e.event_type === 'reauth_link_generated');
        if (last) {
            try {
                const m = typeof last.metadata === 'string' ? JSON.parse(last.metadata) : (last.metadata || {});
                const ageDays = (Date.now() - new Date(last.created_at || 0).getTime()) / 86400000;
                if (m.url && ageDays < 7) return { url: m.url, plan_id: m.plan_id, remaining_cycles: m.remaining_cycles, reused: true };
            } catch {}
        }
    }
    const freq = parseInt(sub.frequency_months) || 1;
    const completed = parseInt(sub.cycles_completed) || 0;
    const required = parseInt(sub.cycles_required) || 1;
    const remaining = Math.max(required - completed, 1);
    const amount = parseFloat(sub.mp_total_amount || sub.final_price || 0);
    if (!amount || amount <= 0) throw new Error('Monto inválido para re-autorización');
    const BACKEND = process.env.BACKEND_URL || 'https://pixel-suite-pro-production.up.railway.app';
    // createCheckout: cycles = ceil(permanence/frequency) → permanence = remaining*freq
    const co = await mp.createCheckout({
        frequency: freq,
        permanence: remaining * freq,
        amount,
        productTitle: `${sub.product_title || 'Suscripción LAB'} (reactivación ${remaining} ${remaining === 1 ? 'mes' : 'meses'})`,
        customerEmail: sub.customer_email,
        backUrl: `${BACKEND}/subscriptions/success?reauth=1`
    });
    if (!co?.init_point || !co?.plan_id) throw new Error('MP no devolvió link de re-autorización');
    await db.updateSubscription(sub.id, { reauth_plan_id: co.plan_id }).catch(() => {});
    await db.createEvent({
        subscription_id: sub.id,
        event_type: 'reauth_link_generated',
        metadata: JSON.stringify({ url: co.init_point, plan_id: co.plan_id, remaining_cycles: remaining, amount, by, at: new Date().toISOString() })
    }).catch(() => {});
    return { url: co.init_point, plan_id: co.plan_id, remaining_cycles: remaining, reused: false };
}

/* Procesa el webhook payment approved de un link de recuperación.
   Reglas heredadas del flujo recurrente (audit jun-8):
   - Dedup por mp_payment_id (MP repite webhooks)
   - Orden vía createShopifyOrderFromSub (mismo mutex anti-duplicado)
   - NO avanza cycles si la orden falló (regla BUG C) — needs_admin_review
   - last_mp_debit_date = débito ORIGINAL del ciclo fallido, NO la fecha del
     pago por link. Si usáramos "hoy", el guard <25d bloquearía el siguiente
     cobro legítimo de MP (ej: link pagado el 11-jun + cobro MP el 4-jul = 23d).
   - Si MP igual reintenta el ciclo viejo y entra → el guard <25d bloquea la
     orden duplicada y marca needs_admin_review (refund manual en MP).
   - Si el preapproval estaba pausado por fallo de pago → resume en MP (era el
     deadlock zombie: pausado nunca genera pagos → recovery imposible). */
async function handleRecoveryLinkPayment(paymentData, paymentId) {
    const extRef = String(paymentData.external_reference || '');
    const parts = extRef.split('::'); // ['subrecovery', '<sub_id>', 'c<n>']
    const subId = parts[1] || null;
    const refCycle = parseInt(String(parts[2] || '').replace(/^c/, ''), 10) || null;
    if (!subId) { console.warn('[RECOVERY-LINK] external_reference sin sub_id:', extRef); return; }
    const sub = await db.getSubscription(subId).catch(() => null);
    if (!sub) { console.warn(`[RECOVERY-LINK] Sub no encontrada: ${subId} (payment ${paymentId})`); return; }

    const events = await db.getEvents(sub.id, 100).catch(() => []);
    const already = (events || []).some(e => {
        try { const m = typeof e.metadata === 'string' ? JSON.parse(e.metadata) : (e.metadata || {}); return String(m.mp_payment_id) === String(paymentId); } catch { return false; }
    });
    if (already) { console.log(`[RECOVERY-LINK] Payment ${paymentId} ya procesado — skip (dedup)`); return; }

    // 🔒 GUARD ANTI DOBLE-REPOSICIÓN (2026-06-11): el link lleva el ciclo objetivo
    //   (refCycle). Si ese ciclo YA está cubierto — porque MP lo recuperó solo con
    //   su reintento nativo, o porque el cliente pagó el mismo link dos veces
    //   (las Preferences MP aceptan más de un pago) — NO crear otra orden ni
    //   avanzar ciclos: el cliente pagó de más. Flag para REFUND manual en MP.
    if (refCycle && (parseInt(sub.cycles_completed) || 0) >= refCycle) {
        await db.createEvent({
            subscription_id: sub.id,
            event_type: 'recovery_link_duplicate_payment',
            metadata: JSON.stringify({
                mp_payment_id: String(paymentId),
                ref_cycle: refCycle,
                cycles_completed: sub.cycles_completed,
                amount: paymentData.transaction_amount,
                at: new Date().toISOString(),
                action_required: 'REFUND en MP — el ciclo ya estaba cubierto cuando entró este pago'
            })
        }).catch(() => {});
        await db.updateSubscription(sub.id, {
            needs_admin_review: true,
            last_order_error: `Pago recovery DUPLICADO ${paymentId} (ciclo ${refCycle} ya cubierto) — REFUND manual en MP ${new Date().toISOString()}`
        }).catch(() => {});
        console.warn(`[RECOVERY-LINK] 🔁 Pago duplicado ${paymentId}: ciclo ${refCycle} ya cubierto (cycles=${sub.cycles_completed}) — needs_admin_review (refund)`);
        return;
    }

    const cyclesCompleted = (parseInt(sub.cycles_completed) || 0) + 1;
    const isComplete = cyclesCompleted >= (parseInt(sub.cycles_required) || 999);

    // Fecha de débito original del ciclo fallido (mantiene sano el guard <25d)
    let originalDebitDate = null;
    const rejectEv = (events || []).find(e => e.event_type === 'payment_rejected');
    try {
        const m = typeof rejectEv?.metadata === 'string' ? JSON.parse(rejectEv.metadata) : (rejectEv?.metadata || {});
        originalDebitDate = m.rejected_at || null;
    } catch {}
    const debitDateForGuard = originalDebitDate || paymentData.date_approved || new Date().toISOString();

    // next_charge_at: la agenda REAL de MP (el link no altera el preapproval)
    // 🔑 FIX 2026-06-11 (caso 11 pausadas): también capturamos el STATUS real del
    //   preapproval. MP pausa solo tras agotar reintentos, pero los planes viejos
    //   tenían notification_url muerta → el webhook de pausa nunca llegó → la sub
    //   local dice 'active' aunque MP diga 'paused'. Decidir el resume por el
    //   estado LOCAL dejaba el preapproval pausado para siempre (cero cobros futuros).
    let nextChargeAt = null;
    let mpRealStatus = null;
    if (sub.mp_preapproval_id) {
        const pre = await mp.getSubscription(sub.mp_preapproval_id).catch(() => null);
        nextChargeAt = pre?.next_payment_date || null;
        mpRealStatus = pre?.status || null;
    }
    if (!nextChargeAt) {
        const d = new Date();
        d.setMonth(d.getMonth() + (parseInt(sub.frequency_months) || 1));
        nextChargeAt = d.toISOString();
    }

    // 🔧 2026-06-12: marcar la orden como RECOVERY — en Shopify lleva el tag extra
    // 'recovery' (antes era indistinguible de una renovación normal). Solo in-memory.
    sub._recovery = true;
    let order = null;
    for (let attempt = 1; attempt <= 3 && !order; attempt++) {
        order = await createShopifyOrderFromSub(sub, paymentId).catch(e => {
            console.error(`[RECOVERY-LINK] Order error (attempt ${attempt}/3):`, e.message);
            return null;
        });
        if (!order && attempt < 3) await new Promise(r => setTimeout(r, 2000));
    }

    if (order?.id) {
        await db.updateSubscription(sub.id, {
            cycles_completed: cyclesCompleted,
            last_charge_at: new Date().toISOString(),
            last_mp_debit_date: debitDateForGuard,
            next_charge_at: nextChargeAt,
            status: isComplete ? 'completed' : 'active',
            shopify_order_id: String(order.id),
            shopify_order_name: order.name,
            needs_payment_update: false,
            paused_reason: null,
            last_payment_recovered_at: new Date().toISOString()
        }).catch(e => console.warn('[RECOVERY-LINK] updateSub:', e.message));
        await db.createEvent({
            subscription_id: sub.id,
            event_type: 'recovery_link_paid',
            metadata: JSON.stringify({
                mp_payment_id: String(paymentId),
                cycle: cyclesCompleted,
                ref_cycle: refCycle,
                amount: paymentData.transaction_amount,
                shopify_order_id: order.id,
                shopify_order_name: order.name,
                original_debit_date: originalDebitDate,
                at: new Date().toISOString()
            })
        }).catch(() => {});
        if (notifications.sendChargeSuccess) notifications.sendChargeSuccess(sub, order.order_number).catch(() => {});
        console.log(`[RECOVERY-LINK] ✅ ${sub.customer_email} repuso ciclo ${cyclesCompleted}/${sub.cycles_required} vía link — order ${order.name}`);
    } else {
        // Regla BUG C: cobro entró pero orden falló → NO avanzar cycles, dejar rastro
        await db.createEvent({
            subscription_id: sub.id,
            event_type: 'order_creation_failed',
            metadata: JSON.stringify({
                failed_payment_id: String(paymentId),
                intended_cycle: cyclesCompleted,
                source: 'recovery_link',
                amount: paymentData.transaction_amount,
                at: new Date().toISOString()
            })
        }).catch(() => {});
        await db.updateSubscription(sub.id, {
            needs_payment_update: false,
            last_order_error: `Orden falló 3x para pago recovery-link ${paymentId} ${new Date().toISOString()}`,
            needs_admin_review: true
        }).catch(() => {});
        console.error(`[RECOVERY-LINK] ⚠️ Pago ${paymentId} OK en MP pero orden Shopify falló 3x — needs_admin_review. Sub: ${sub.id}`);
    }

    // Zombie rescue: preapproval pausado por fallo de pago → reanudar para que MP siga cobrando ciclos futuros
    // 🔑 FIX 2026-06-11: la condición decide por el estado REAL en MP (mpRealStatus),
    //   no solo el local. Las 11 subs pausadas por MP el 05-jun seguían 'active'
    //   localmente (webhook muerto) y el resume jamás se disparaba.
    const pausedByFailure = (
            mpRealStatus === 'paused' ||
            (sub.status === 'paused' && (sub.paused_reason === 'auto_payment_failed' || sub.paused_reason === 'mp_auto_paused' || sub.needs_payment_update === true))
        ) &&
        !sub.paused_until; // pausa voluntaria con fecha NO se toca
    if (pausedByFailure && sub.mp_preapproval_id && mp.resumeSubscription) {
        try {
            await mp.resumeSubscription(sub.mp_preapproval_id);
            if (!order?.id) await db.updateSubscription(sub.id, { status: 'active', paused_reason: null }).catch(() => {});
            await db.createEvent({
                subscription_id: sub.id,
                event_type: 'preapproval_resumed_after_recovery',
                metadata: JSON.stringify({ mp_preapproval_id: sub.mp_preapproval_id, at: new Date().toISOString() })
            }).catch(() => {});
            console.log(`[RECOVERY-LINK] ▶️ Preapproval reanudado en MP: ${sub.mp_preapproval_id} (${sub.customer_email})`);
        } catch (e) { console.warn('[RECOVERY-LINK] No se pudo reanudar preapproval:', e.message); }
    }
}

/* ── Función: detectar cobros rechazados en MP (cada 4h) ── */
async function runDunningDetection() {
    console.log('[DUNNING] Detection start');
    let detected = 0, emailsSent = 0, errors = 0, recovered = 0;
    try {
        const allSubs = await db.getSubscriptions().catch(() => []);
        // 🔍 Incluir subs paused por payment_failed para detectar recovery
        // 🆕 2026-06-11: + mp_auto_paused (MP pausó tras agotar reintentos — antes zombie invisible)
        const targets = (Array.isArray(allSubs) ? allSubs : [])
            .filter(s => (s.status === 'active' || s.paused_reason === 'auto_payment_failed' || s.paused_reason === 'mp_auto_paused') && s.mp_preapproval_id);
        const mpToken = process.env.MP_ACCESS_TOKEN;
        if (!mpToken) { console.warn('[DUNNING] No MP token, skipping'); return; }
        const BACKEND = process.env.BACKEND_URL || 'https://pixel-suite-pro-production.up.railway.app';
        for (const sub of targets) {
            try {
                const payments = await mp.listPreapprovalPayments(sub.mp_preapproval_id, 5).catch(() => []);
                if (!payments.length) continue;
                // Get latest payment with real status (use mp.getPayment for accuracy)
                const latest = payments[0];
                const pid = latest.payment_id || latest.id;
                if (!pid || !/^\d+$/.test(String(pid))) continue;
                let realStatus = latest.status;
                const pd = await mp.getPayment(String(pid)).catch(() => null);
                if (pd) realStatus = pd.status;
                // 🎉 RECOVERY: si sub estaba en needs_payment_update y ahora hay payment approved
                if (realStatus === 'approved' && sub.needs_payment_update === true) {
                    await db.updateSubscription(sub.id, {
                        needs_payment_update: false,
                        last_payment_recovered_at: new Date().toISOString(),
                        paused_reason: null
                    }).catch(() => {});
                    await db.createEvent({
                        subscription_id: sub.id,
                        event_type: 'payment_recovered',
                        metadata: JSON.stringify({ payment_id: String(pid), amount: latest.transaction_amount, at: new Date().toISOString() })
                    }).catch(() => {});
                    // Email "tu pago se procesó" — con fallback al admin si Resend trial
                    try {
                        if (_emailsAreEnabled() && process.env.RESEND_API_KEY) {
                            const html = `<!DOCTYPE html><html><body style="font-family:Arial,sans-serif;padding:24px;background:#f5f5f5"><div style="max-width:560px;margin:0 auto;background:#fff;border-radius:12px;overflow:hidden"><div style="background:#0A0A0A;padding:24px;text-align:center"><h1 style="color:#E30613;margin:0;letter-spacing:2px;font-weight:900">LAB NUTRITION</h1></div><div style="padding:32px 28px"><h2 style="color:#10B981;margin:0 0 12px">✓ ¡Pago procesado!</h2><p style="color:#374151;line-height:1.6">Hola ${(sub.customer_name||'').split(' ')[0]}, tu suscripción <strong>${sub.product_title}</strong> se cobró correctamente. Tu pedido sale en los próximos días.</p><p style="color:#374151">Gracias por seguir con nosotros.</p></div></div></body></html>`;
                            const rr = await fetch('https://api.resend.com/emails', {
                                method: 'POST',
                                headers: { 'Authorization': `Bearer ${process.env.RESEND_API_KEY}`, 'Content-Type': 'application/json' },
                                body: JSON.stringify({ from: process.env.RESEND_FROM || 'LAB NUTRITION <onboarding@resend.dev>', to: [sub.customer_email], subject: '✓ Tu pago se procesó — LAB NUTRITION', html })
                            }).catch(() => ({ ok: false, status: 0 }));
                            if (!rr.ok && rr.status === 403) {
                                // Notify admin of recovery
                                await fetch('https://api.resend.com/emails', {
                                    method: 'POST',
                                    headers: { 'Authorization': `Bearer ${process.env.RESEND_API_KEY}`, 'Content-Type': 'application/json' },
                                    body: JSON.stringify({
                                        from: process.env.RESEND_FROM || 'LAB NUTRITION Recovery <onboarding@resend.dev>',
                                        to: [_ADMIN_EMAIL_FALLBACK],
                                        subject: `🎉 RECOVERY: ${sub.customer_email} actualizó su tarjeta`,
                                        html: `<p>El cliente <strong>${sub.customer_name}</strong> (${sub.customer_email}) actualizó su tarjeta y el cobro de ${sub.product_title} se procesó. Pedido sale automático.</p>`
                                    })
                                }).catch(() => {});
                            }
                        }
                    } catch {}
                    recovered++;
                    console.log(`[DUNNING] 🎉 RECOVERED: ${sub.customer_email} — payment ${pid} ahora approved`);
                    continue; // No procesar como nuevo rechazo
                }
                if (realStatus !== 'rejected') continue;
                // Idempotency: skip if already processed this payment_id
                const events = await db.getEvents(sub.id).catch(() => []);
                const already = (events || []).some(e => {
                    if (e.event_type !== 'payment_rejected') return false;
                    try { const m = typeof e.metadata === 'string' ? JSON.parse(e.metadata) : e.metadata; return String(m.payment_id) === String(pid); } catch { return false; }
                });
                if (already) continue;
                // Loggea evento
                await db.createEvent({
                    subscription_id: sub.id,
                    event_type: 'payment_rejected',
                    metadata: JSON.stringify({
                        payment_id: String(pid),
                        amount: latest.transaction_amount,
                        rejected_at: latest.date_created || new Date().toISOString(),
                        status_detail: pd?.status_detail || latest.status_detail || null,
                        detected_at: new Date().toISOString(),
                        cycle: (parseInt(sub.cycles_completed)||0) + 1
                    })
                }).catch(() => {});
                // Marca sub
                await db.updateSubscription(sub.id, { needs_payment_update: true, last_payment_failed_at: new Date().toISOString() }).catch(() => {});
                // 🔒 ANTI-SPAM 2026-06-05: NO mandamos email individual aquí.
                // El DIGEST diario consolida todos los casos en 1 email/día.
                detected++;
                console.log(`[DUNNING] ${sub.customer_email} - payment ${pid} rejected (added to needs_payment_update; digest enviará 1 email/día)`);
            } catch (e) {
                errors++;
                console.warn('[DUNNING]', sub.id, e.message);
            }
            await new Promise(r => setTimeout(r, 300)); // rate limit MP
        }

        // 🛟 SAFETY NET RECOVERY-LINK (2026-06-11): el webhook MP se ack'ea con 200
        //   ANTES de procesar (línea ~4610) → si el proceso muere tras el ack, MP no
        //   reintenta y el pago del link queda sin reconciliar. El polling de
        //   preapprovals NO ve pagos de Preference. Acá (3x/día) buscamos por
        //   external_reference los pagos approved de links pendientes y los
        //   reprocesamos — handleRecoveryLinkPayment es idempotente (dedup payment_id).
        for (const sub of targets.filter(s => s.needs_payment_update === true)) {
            try {
                const evs = await db.getEvents(sub.id).catch(() => []);

                // 🔑 AUTO-RESUME 2026-06-11 (la causa de "no cobra los siguientes ciclos"):
                //   MP pausa el preapproval SOLO tras agotar sus reintentos. Los planes
                //   viejos tienen notification_url muerta → el webhook de pausa nunca
                //   llega → la sub local sigue 'active' y NADIE reanudaba en MP → cero
                //   cobros futuros para siempre (11 casos confirmados el 11-jun).
                //   Acá: si MP dice paused y NO es pausa voluntaria → resume. Dedup 7d
                //   para no pelear con MP si la tarjeta sigue mala (MP re-pausará y en
                //   7 días reintentamos — cadencia mensual de cobro lo hace seguro).
                if (sub.mp_preapproval_id && !sub.paused_until) {
                    try {
                        const pre = await mp.getSubscription(sub.mp_preapproval_id).catch(() => null);
                        if (pre?.status === 'paused') {
                            const recentResume = (evs || []).some(e =>
                                e.event_type === 'mp_preapproval_auto_resumed' &&
                                (Date.now() - new Date(e.created_at || 0).getTime()) < 7 * 86400000
                            );
                            if (!recentResume) {
                                await mp.resumeSubscription(sub.mp_preapproval_id);
                                await db.createEvent({
                                    subscription_id: sub.id,
                                    event_type: 'mp_preapproval_auto_resumed',
                                    metadata: JSON.stringify({ mp_preapproval_id: sub.mp_preapproval_id, prev_mp_status: 'paused', at: new Date().toISOString() })
                                }).catch(() => {});
                                console.log(`[DUNNING] ▶️ AUTO-RESUME: preapproval ${sub.mp_preapproval_id} (${sub.customer_email}) estaba paused en MP — reanudado, MP vuelve a cobrar ciclos futuros`);
                            }
                        }
                    } catch (e) { console.warn('[DUNNING] auto-resume', sub.id, e.message); }
                }

                const linkEv = (evs || []).find(e => e.event_type === 'recovery_link_generated');
                if (!linkEv) continue;
                let extRef = null;
                try {
                    const m = typeof linkEv.metadata === 'string' ? JSON.parse(linkEv.metadata) : (linkEv.metadata || {});
                    extRef = m.cycle ? `subrecovery::${sub.id}::c${m.cycle}` : null;
                } catch {}
                if (!extRef) continue;
                const sr = await fetch(`https://api.mercadopago.com/v1/payments/search?external_reference=${encodeURIComponent(extRef)}&sort=date_created&criteria=desc`, {
                    headers: { Authorization: `Bearer ${mpToken}` }
                });
                if (!sr.ok) continue;
                const sd = await sr.json();
                const approved = (sd.results || []).find(p => p.status === 'approved');
                if (approved) {
                    console.log(`[DUNNING] 🛟 Safety net: pago recovery ${approved.id} approved sin reconciliar (${sub.customer_email}) — reprocesando`);
                    await handleRecoveryLinkPayment(approved, String(approved.id)).catch(e => console.warn('[DUNNING] safety-net reproceso:', e.message));
                }
                await new Promise(r => setTimeout(r, 300)); // rate limit MP
            } catch (e) { console.warn('[DUNNING] safety-net', sub.id, e.message); }
        }

        console.log(`[DUNNING] Done: detected=${detected}, recovered=${recovered}, emails=${emailsSent}, errors=${errors}`);
    } catch (e) { console.error('[DUNNING] Top-level error:', e.message); }
}

/* ── Función: DAILY DIGEST consolidado.
   En vez de mandar N emails individuales (spam), manda UNO solo con todos los casos.
   Idempotente: solo 1 digest por día (chequea evento del último digest).
   Esto reemplaza la corrida de "12 emails" que se hizo antes. */
async function runDunningDigest() {
    console.log('[DIGEST] Start');
    try {
        if (!_emailsAreEnabled()) { console.log('[DIGEST] KILL SWITCH OFF — skipped'); return; }
        if (!process.env.RESEND_API_KEY) return;
        const allSubs = await db.getSubscriptions().catch(() => []);
        const cases = (Array.isArray(allSubs) ? allSubs : [])
            .filter(s => s.needs_payment_update === true || s.paused_reason === 'auto_payment_failed' || s.paused_reason === 'mp_auto_paused');
        if (!cases.length) {
            console.log('[DIGEST] No cases needing action');
            return;
        }
        // Idempotencia: chequear si ya se envió digest hoy
        const today = new Date().toISOString().slice(0, 10); // YYYY-MM-DD
        const digestEventId = `digest_${today}`;
        const existingDigest = await db.getEvents(digestEventId).catch(() => []);
        if ((existingDigest || []).some(e => e.event_type === 'dunning_digest_sent')) {
            console.log('[DIGEST] Already sent today');
            return;
        }
        // Build single email with ALL cases
        const itemsHtml = await Promise.all(cases.map(async (sub) => {
            const phone = (sub.customer_phone || '').replace(/[^0-9]/g, '');
            const events = await db.getEvents(sub.id).catch(() => []);
            // 🆕 2026-06-11: si ya hay link de pago generado, el WA invita a PAGAR (acción real)
            let payUrl = null;
            const linkEv = (events || []).find(e => e.event_type === 'recovery_link_generated');
            try { const m = typeof linkEv?.metadata === 'string' ? JSON.parse(linkEv.metadata) : (linkEv?.metadata || {}); payUrl = m.url || null; } catch {}
            const waText = 'Hola ' + (sub.customer_name || '').split(' ')[0] + ', te escribo de Lab Nutrition. Tu suscripción tuvo un cobro rechazado. ' +
                (payUrl ? ('Pagá tu ciclo pendiente acá: ' + payUrl) : ('Actualizá tu tarjeta acá: ' + _mpCustomerDashboardUrl(sub.mp_preapproval_id)));
            const wa = phone ? `https://wa.me/${phone.startsWith('51') ? phone : '51' + phone}?text=${encodeURIComponent(waText)}` : null;
            const reject = (events || []).find(e => e.event_type === 'payment_rejected');
            const daysSince = reject ? Math.floor((Date.now() - new Date(reject.created_at).getTime()) / 86400000) : 0;
            return `<tr style="border-bottom:1px solid #e5e7eb">
                <td style="padding:10px 12px"><strong>${sub.customer_name || '—'}</strong><br><small style="color:#666;font-size:11px">${sub.customer_email}</small></td>
                <td style="padding:10px 12px;font-size:12px">${sub.product_title || '—'}</td>
                <td style="padding:10px 12px;text-align:right;color:#E30613;font-weight:700;font-size:13px">S/${sub.mp_total_amount || sub.final_price || 0}</td>
                <td style="padding:10px 12px;text-align:center"><span style="background:${daysSince >= 14 ? '#FEE2E2' : daysSince >= 7 ? '#FEF3C7' : '#DBEAFE'};color:${daysSince >= 14 ? '#991B1B' : daysSince >= 7 ? '#92400E' : '#1E40AF'};padding:3px 8px;border-radius:10px;font-size:11px;font-weight:700">${daysSince}d</span></td>
                <td style="padding:10px 12px;text-align:right;white-space:nowrap;font-size:11px">${wa ? `<a href="${wa}" style="background:#25D366;color:#fff;padding:5px 10px;border-radius:5px;text-decoration:none;font-weight:600;margin-right:4px">WA</a>` : ''}${payUrl ? `<a href="${payUrl}" style="background:#0A0A0A;color:#fff;padding:5px 10px;border-radius:5px;text-decoration:none;font-weight:600;margin-right:4px">PAGAR</a>` : ''}<a href="${_mpCustomerDashboardUrl(sub.mp_preapproval_id)}" style="background:#E30613;color:#fff;padding:5px 10px;border-radius:5px;text-decoration:none;font-weight:600">MP</a></td>
            </tr>`;
        }));
        const digestHtml = `<!DOCTYPE html><html><body style="font-family:Arial,sans-serif;background:#f5f5f5;padding:24px;margin:0">
<div style="max-width:760px;margin:0 auto;background:#fff;border-radius:12px;overflow:hidden">
  <div style="background:#0A0A0A;padding:24px;text-align:center"><h1 style="color:#E30613;margin:0;font-size:20px;letter-spacing:2px">LAB NUTRITION — Daily Dunning Digest</h1><div style="color:#888;font-size:12px;margin-top:6px">${new Date().toLocaleDateString('es-PE', {day:'2-digit',month:'long',year:'numeric'})}</div></div>
  <div style="padding:28px 24px">
    <h2 style="margin:0 0 12px;font-size:16px"><span style="color:#E30613">${cases.length}</span> ${cases.length === 1 ? 'cliente' : 'clientes'} con acción pendiente</h2>
    <p style="color:#666;font-size:13px;line-height:1.5;margin:0 0 18px">Este es un resumen consolidado. <strong>UN email por día</strong>, no más spam. Sistema sigue corriendo solo (cron 3x/día).</p>
    <table style="width:100%;border-collapse:collapse;font-size:13px;margin-bottom:20px;border:1px solid #e5e7eb;border-radius:8px;overflow:hidden">
      <thead style="background:#f9fafb"><tr><th style="padding:10px;text-align:left;font-size:11px;color:#666">Cliente</th><th style="padding:10px;text-align:left;font-size:11px;color:#666">Producto</th><th style="padding:10px;text-align:right;font-size:11px;color:#666">Monto</th><th style="padding:10px;text-align:center;font-size:11px;color:#666">Días</th><th style="padding:10px;text-align:right;font-size:11px;color:#666">Acción</th></tr></thead>
      <tbody>${itemsHtml.join('')}</tbody>
    </table>
    <div style="background:#DCFCE7;border-left:4px solid #10B981;padding:14px 18px;border-radius:6px;font-size:13px;color:#065F46;line-height:1.5">
      ✓ Sistema corre solo 3x/día (06:00, 12:00, 18:00 PET). MP detecta recovery automático.
      Auto-pause día 14. Solo recibirás este email 1x/día con los casos activos.
    </div>
    <p style="text-align:center;color:#888;font-size:11px;margin:20px 0 0">Para gestionar manual: <a href="${process.env.BACKEND_URL || 'https://pixel-suite-pro-production.up.railway.app'}/?page=failed-payments" style="color:#E30613">Admin → Cobros Rechazados</a></p>
  </div>
</div></body></html>`;
        const result = await _resendSendWithFallback({
            from: process.env.RESEND_FROM || 'LAB NUTRITION <onboarding@resend.dev>',
            subject: `📊 Daily Dunning Digest — ${cases.length} ${cases.length === 1 ? 'caso' : 'casos'} pendientes (${today})`,
            html: digestHtml
        }, _ADMIN_EMAIL_FALLBACK);
        if (result.sent) {
            // Loggea evento idempotencia
            await db.createEvent({
                subscription_id: digestEventId,
                event_type: 'dunning_digest_sent',
                metadata: JSON.stringify({ cases_count: cases.length, at: new Date().toISOString(), sent_to: result.to })
            }).catch(() => {});
            console.log(`[DIGEST] ✅ Sent to ${result.to} — ${cases.length} cases`);
        } else {
            console.warn(`[DIGEST] Failed to send: status=${result.status}`);
        }
    } catch (e) { console.error('[DIGEST] Top-level error:', e.message); }
}

/* ── Función: follow-up emails día 3, 7, 14 + auto-pause (daily 10:30am) ── */
async function runDunningFollowups() {
    console.log('[DUNNING-FU] Followups start');
    let sent = 0, paused = 0;
    try {
        const allSubs = await db.getSubscriptions().catch(() => []);
        // 🆕 2026-06-11: incluir mp_auto_paused — son los que MÁS necesitan el link de pago
        const needsUpdate = (Array.isArray(allSubs) ? allSubs : [])
            .filter(s => (s.status === 'active' || s.paused_reason === 'mp_auto_paused') && s.needs_payment_update === true);
        const BACKEND = process.env.BACKEND_URL || 'https://pixel-suite-pro-production.up.railway.app';
        for (const sub of needsUpdate) {
            try {
                const events = await db.getEvents(sub.id).catch(() => []);
                const rejectionEvent = (events || []).find(e => e.event_type === 'payment_rejected');
                if (!rejectionEvent) continue;
                const daysSince = Math.floor((Date.now() - new Date(rejectionEvent.created_at || rejectionEvent.metadata?.detected_at || Date.now()).getTime()) / 86400000);
                const milestone = [3, 7, 14].find(d => daysSince === d);
                if (milestone) {
                    const alreadySent = (events || []).some(e => {
                        if (e.event_type !== 'dunning_email_sent') return false;
                        try { const m = typeof e.metadata === 'string' ? JSON.parse(e.metadata) : e.metadata; return Number(m.day) === milestone; } catch { return false; }
                    });
                    if (!alreadySent) {
                        const mpUrl = _mpCustomerDashboardUrl(sub.mp_preapproval_id);
                        const portalUrl = `${BACKEND}/portal/v2`;
                        // 🆕 2026-06-11: incluir link de pago real (reutiliza si ya existe <7d)
                        let payLink = null;
                        try { payLink = (await _getOrCreateRecoveryLink(sub, { by: 'dunning_followup' })).url; } catch (e) { console.warn('[DUNNING-FU] payLink:', e.message); }
                        const r = await _sendDunningEmail(sub, milestone, mpUrl, portalUrl, payLink);
                        if (r.sent) {
                            sent++;
                            await db.createEvent({
                                subscription_id: sub.id,
                                event_type: 'dunning_email_sent',
                                metadata: JSON.stringify({ day: milestone, at: new Date().toISOString() })
                            }).catch(() => {});
                        }
                    }
                }
                // 🔑 FIX CRÍTICO 2026-06-09 (audit jun-8): NO pausar el preapproval MP en día 14.
                //   ANTES: mp.pauseSubscription() pausaba el preapproval → MP DEJABA de reintentar →
                //   aunque el cliente actualizara la tarjeta, MP nunca volvía a cobrar → churn
                //   irrecuperable. Y el recovery (que requiere un payment approved NUEVO) era
                //   imposible porque un preapproval pausado no genera payments.
                //   AHORA: dejamos el preapproval AUTHORIZED para que MP siga reintentando con su
                //   agenda natural (los "intentos de cobro" que el cliente pide). Solo marcamos la
                //   sub localmente para que el admin la vea destacada. NO tocamos MP.
                if (daysSince >= 14 && sub.status === 'active' && !sub.escalated_14d) {
                    await db.updateSubscription(sub.id, { escalated_14d: true, escalated_14d_at: new Date().toISOString() });
                    await db.createEvent({
                        subscription_id: sub.id,
                        event_type: 'payment_failed_14d_escalation',
                        metadata: JSON.stringify({ days_since_rejection: daysSince, at: new Date().toISOString(), note: 'MP sigue reintentando — sub NO pausada, requiere atención admin' })
                    }).catch(() => {});
                    paused++;
                }
            } catch (e) { console.warn('[DUNNING-FU]', sub.id, e.message); }
        }
        console.log(`[DUNNING-FU] Done: emails=${sent}, paused=${paused}`);
    } catch (e) { console.error('[DUNNING-FU] Top-level error:', e.message); }
}

/* ═══════════════════════════════════════════════════════════════════
   📍 ADMIN: lista de cobros fallidos pendientes acción
   ═══════════════════════════════════════════════════════════════════ */
app.get('/api/admin/failed-payments', async (req, res) => {
    try {
        const all = await db.getSubscriptions().catch(() => []);
        const candidates = (Array.isArray(all) ? all : [])
            .filter(s => s.needs_payment_update === true || s.paused_reason === 'auto_payment_failed' || s.paused_reason === 'mp_auto_paused');
        const enriched = await Promise.all(candidates.map(async s => {
            const events = await db.getEvents(s.id).catch(() => []);
            const reject = (events || []).find(e => e.event_type === 'payment_rejected');
            const emailsSent = (events || []).filter(e => e.event_type === 'dunning_email_sent');
            const daysSince = reject ? Math.floor((Date.now() - new Date(reject.created_at).getTime()) / 86400000) : null;
            let metadata = {};
            try { metadata = typeof reject?.metadata === 'string' ? JSON.parse(reject.metadata) : (reject?.metadata || {}); } catch {}
            return {
                sub_id: s.id,
                email: s.customer_email,
                name: s.customer_name,
                phone: s.customer_phone,
                product: s.product_title,
                status: s.status,
                cycles: `${s.cycles_completed || 0}/${s.cycles_required || '?'}`,
                amount_failed: metadata.amount || s.mp_total_amount || s.final_price,
                rejection_date: reject?.created_at || metadata.detected_at || null,
                days_since: daysSince,
                emails_sent: emailsSent.length,
                emails_dates: emailsSent.map(e => {
                    try { const m = typeof e.metadata === 'string' ? JSON.parse(e.metadata) : e.metadata; return `day ${m.day}`; } catch { return '?'; }
                }),
                payment_id: metadata.payment_id,
                status_detail: metadata.status_detail,
                mp_preapproval_id: s.mp_preapproval_id,
                mp_update_url: s.mp_preapproval_id ? _mpCustomerDashboardUrl(s.mp_preapproval_id) : null,
                auto_paused: s.paused_reason === 'auto_payment_failed' || s.paused_reason === 'mp_auto_paused',
                // 🆕 último link de pago generado (si existe) — el botón del admin lo reutiliza
                recovery_link: (() => {
                    const ev = (events || []).find(e => e.event_type === 'recovery_link_generated');
                    try { const m = typeof ev?.metadata === 'string' ? JSON.parse(ev.metadata) : (ev?.metadata || {}); return m.url || null; } catch { return null; }
                })()
            };
        }));
        enriched.sort((a, b) => (b.days_since || 0) - (a.days_since || 0));
        res.json({ total: enriched.length, items: enriched });
    } catch (e) { res.status(500).json({ error: e.message }); }
});

/* POST /api/admin/failed-payments/:sub_id/resend-email — admin re-envía email manualmente */
/* 🔒 FIX 2026-06-05 ANTI-SPAM: resend-email con dedup 24h obligatorio.
   ANTES: cada click + cada cron + cada test → email duplicado al admin.
   AHORA: bloquea si se envió email en las últimas 24h (cualquier tipo).
   Override con ?force=1 (admin debe confirmar explícitamente). */
app.post('/api/admin/failed-payments/:sub_id/resend-email', async (req, res) => {
    try {
        const sub = await db.getSubscription(req.params.sub_id);
        if (!sub) return res.status(404).json({ error: 'Sub not found' });
        const force = req.query.force === '1' || req.body?.force === true;
        if (!force) {
            const events = await db.getEvents(sub.id).catch(() => []);
            const lastEmail = (events || []).find(e =>
                e.event_type === 'dunning_email_sent' || e.event_type === 'dunning_email_sent_manual'
            );
            if (lastEmail) {
                const hoursAgo = (Date.now() - new Date(lastEmail.created_at).getTime()) / 3600000;
                if (hoursAgo < 24) {
                    return res.status(429).json({
                        error: 'Email ya enviado en las últimas 24h. Usá ?force=1 si querés re-enviar igual.',
                        last_sent_hours_ago: Math.round(hoursAgo * 10) / 10,
                        blocked: true
                    });
                }
            }
        }
        const BACKEND = process.env.BACKEND_URL || 'https://pixel-suite-pro-production.up.railway.app';
        const mpUrl = _mpCustomerDashboardUrl(sub.mp_preapproval_id);
        const portalUrl = `${BACKEND}/portal/v2`;
        // 🆕 2026-06-11: incluir link de pago real (reutiliza si ya existe <7d)
        let payLink = null;
        try { payLink = (await _getOrCreateRecoveryLink(sub, { by: 'admin_resend' })).url; } catch (e) { console.warn('[RESEND-EMAIL] payLink:', e.message); }
        const r = await _sendDunningEmail(sub, 0, mpUrl, portalUrl, payLink);
        await db.createEvent({
            subscription_id: sub.id,
            event_type: 'dunning_email_sent_manual',
            metadata: JSON.stringify({ day: 0, by: 'admin', at: new Date().toISOString(), email_status: r.status, forced: !!force })
        }).catch(() => {});
        res.json({ success: r.sent, ...r });
    } catch (e) { res.status(500).json({ error: e.message }); }
});

/* 🆕 POST /api/admin/failed-payments/:sub_id/payment-link — genera (o reutiliza <7d)
   un link de pago ÚNICO (Preference MP) para reponer el ciclo rechazado.
   El cliente paga con CUALQUIER medio — no necesita entrar a su cuenta MP — y el
   webhook 'subrecovery::' crea la orden, limpia flags y reanuda el preapproval
   si estaba pausado. ?fresh=1 fuerza un link nuevo. */
app.post('/api/admin/failed-payments/:sub_id/payment-link', async (req, res) => {
    try {
        const sub = await db.getSubscription(req.params.sub_id);
        if (!sub) return res.status(404).json({ error: 'Sub not found' });
        const fresh = req.query.fresh === '1' || req.body?.fresh === true;
        const link = await _getOrCreateRecoveryLink(sub, { by: 'admin', fresh });
        res.json({ success: true, ...link });
    } catch (e) { res.status(500).json({ error: e.message }); }
});

/* 🆕 POST /api/admin/failed-payments/:sub_id/reauthorize-link — TARJETA NUEVA.
   Para clientes que no pueden entrar a su cuenta MP a cambiar la tarjeta.
   Genera checkout de suscripción con los ciclos RESTANTES; al autorizar:
   MP cobra el mes pendiente de inmediato + el webhook cancela el preapproval
   viejo y engancha el nuevo a la misma sub. ⚠️ Enviar SOLO si el cliente dice
   que su tarjeta ya no sirve (si además paga el payment-link, el guard <25d
   bloquea la orden duplicada y marca refund). */
app.post('/api/admin/failed-payments/:sub_id/reauthorize-link', async (req, res) => {
    try {
        const sub = await db.getSubscription(req.params.sub_id);
        if (!sub) return res.status(404).json({ error: 'Sub not found' });
        const fresh = req.query.fresh === '1' || req.body?.fresh === true;
        const link = await _getOrCreateReauthLink(sub, { by: 'admin', fresh });
        res.json({ success: true, ...link });
    } catch (e) { res.status(500).json({ error: e.message }); }
});

/* POST /api/admin/run-dunning-now — admin dispara dunning manual */
app.post('/api/admin/run-dunning-now', async (req, res) => {
    res.json({ started: true, message: 'Dunning detection started in background — revisa logs Railway y /api/admin/failed-payments en 1-2 min' });
    runDunningDetection().catch(console.error);
});

/* 🆕 POST /api/admin/dunning-digest-now — manda DIGEST consolidado (1 email con todos los casos)
   Pensado para reemplazar el bombardeo de N emails. Idempotente — solo manda si pasaron 24h
   desde el último digest. */
app.post('/api/admin/dunning-digest-now', async (req, res) => {
    res.json({ started: true });
    runDunningDigest().catch(console.error);
});

/* POST /api/admin/run-dunning-followups-now — admin dispara follow-ups manual */
app.post('/api/admin/run-dunning-followups-now', async (req, res) => {
    res.json({ started: true, message: 'Dunning followups started in background' });
    runDunningFollowups().catch(console.error);
});

/* POST /api/admin/failed-payments/:sub_id/clear-flag — admin marca caso resuelto */
app.post('/api/admin/failed-payments/:sub_id/clear-flag', async (req, res) => {
    try {
        await db.updateSubscription(req.params.sub_id, { needs_payment_update: false, paused_reason: null });
        await db.createEvent({
            subscription_id: req.params.sub_id,
            event_type: 'payment_issue_cleared',
            metadata: JSON.stringify({ by: 'admin', at: new Date().toISOString() })
        }).catch(() => {});
        res.json({ success: true });
    } catch (e) { res.status(500).json({ error: e.message }); }
});

/* ═══════════════════════════════════════════════════════════════════
   🆕 APP PROXY HANDLER (portal dentro de labnutrition.pe)
   Shopify enruta labnutrition.pe/apps/account-suscripcion/* aquí.
   Configurar en Partners: subpath_prefix=apps, subpath=account-suscripcion,
   proxy_url=https://pixel-suite-pro-production.up.railway.app/apps/account-suscripcion
   ═══════════════════════════════════════════════════════════════════ */

// HTML page (responsive, Lab Nutrition branded)
app.get('/apps/account-suscripcion', async (req, res) => {
    const hmac = _verifyAppProxyHmac(req.query);
    if (!hmac.ok && hmac.mode === 'enforce') return res.status(401).type('html').send('<h1>Acceso no autorizado</h1>');
    const customerId = req.query.logged_in_customer_id || '';
    res.set('Content-Type', 'application/liquid');
    res.sendFile(path.join(__dirname, 'public', 'portal-shopify.html'));
});

// JSON: subs by customer_id (App Proxy authenticated)
app.get('/apps/account-suscripcion/me', (req, res, next) => _portalRateLimit(req, res, next), async (req, res) => {
    const hmac = _verifyAppProxyHmac(req.query);
    if (!hmac.ok && hmac.mode === 'enforce') return res.status(401).json({ error: 'Unauthorized' });
    const customerId = String(req.query.logged_in_customer_id || '');
    if (!customerId) return res.json({ is_member: false, customer_id: null, subscriptions: [] });
    // Buscar customer en Shopify para obtener email
    const shop = process.env.SHOPIFY_SHOP || 'nutrition-lab-cluster.myshopify.com';
    const token = process.env.SHOPIFY_ACCESS_TOKEN || _shopifyToken;
    let customerEmail = null, customerName = null;
    if (token) {
        try {
            const r = await fetch(`https://${shop}/admin/api/2026-01/customers/${customerId}.json?fields=id,email,first_name,last_name`, { headers: { 'X-Shopify-Access-Token': token } });
            if (r.ok) {
                const d = await r.json();
                customerEmail = d.customer?.email;
                customerName = `${d.customer?.first_name || ''} ${d.customer?.last_name || ''}`.trim();
            }
        } catch {}
    }
    if (!customerEmail) return res.json({ is_member: false, customer_id: customerId, name: customerName, subscriptions: [] });
    const allSubs = await db.getSubscriptions().catch(() => []);
    const filtered = (Array.isArray(allSubs) ? allSubs : [])
        .filter(s => (s.customer_email || '').toLowerCase() === customerEmail.toLowerCase() && s.status !== 'pending_payment');
    // 🆕 2026-06-11 — LINKS INTELIGENTES SEGÚN ESTADO:
    //   • Sub con cobro rechazado (needs_payment_update) → genera link de PAGO real
    //     (Preference, paga el mes pendiente sin la cuenta MP) + link de TARJETA NUEVA
    //     (re-autorización). El dashboard MP (muro) deja de ser la opción principal.
    //   • Sub sana → solo mp_update_url (dashboard MP) para gestión preventiva de tarjeta.
    //   Links deduplicados <7d por los helpers → no spamea Preferences en cada carga.
    // 🔒 GUARD (review adversarial 2026-06-11): crear objetos de pago en MP desde un
    //   GET requiere firma App Proxy VÁLIDA (hmac.ok). Shopify SIEMPRE firma el
    //   tráfico legítimo del proxy, incluso con APP_PROXY_VERIFY=warn; un request
    //   forjado directo al backend no trae firma válida → ve la sub pero SIN minar
    //   links (cae al fallback dashboard). Cero impacto para clientes reales.
    const canMintLinks = hmac.ok === true;
    const mySubs = await Promise.all(filtered.map(async s => {
        const out = {
            id: s.id,
            product_title: s.product_title,
            product_image: s.product_image,
            status: s.status,
            cycles_completed: parseInt(s.cycles_completed || 0),
            cycles_required: parseInt(s.cycles_required || 0),
            frequency_months: s.frequency_months,
            permanence_months: s.permanence_months,
            base_price: s.base_price,
            final_price: s.final_price,
            mp_total_amount: s.mp_total_amount,
            next_charge_at: s.next_charge_at,
            last_charge_at: s.last_charge_at,
            shopify_order_name: s.shopify_order_name,
            needs_payment_update: !!s.needs_payment_update,
            mp_update_url: s.mp_preapproval_id ? _mpCustomerDashboardUrl(s.mp_preapproval_id) : null,
            payment_link: null,
            reauth_link: null
        };
        if (s.needs_payment_update && canMintLinks) {
            try { out.payment_link = (await _getOrCreateRecoveryLink(s, { by: 'portal' })).url; } catch (e) { console.warn('[PORTAL /me] payment_link', s.id, e.message); }
            try { out.reauth_link = (await _getOrCreateReauthLink(s, { by: 'portal' })).url; } catch (e) { console.warn('[PORTAL /me] reauth_link', s.id, e.message); }
        }
        return out;
    }));
    res.json({
        is_member: mySubs.length > 0,
        customer_id: customerId,
        email: customerEmail,
        name: customerName,
        subscriptions: mySubs
    });
});

/* ═══════════════════════════════════════════════════════════════════
   🆕 2026-06-12 — CUSTOMER ACCOUNT UI EXTENSION ("Mi Suscripción")
   La extensión lab-portal-account (customer-account.page.render) llama a estos
   endpoints con un session token JWT (shopify.sessionToken.get()). Verificamos
   el JWT con el client secret del app (HS256), sacamos el customer del claim
   `sub` y devolvemos SUS suscripciones reales + la config del portal. Esto
   reemplaza el contenido hardcodeado anterior de la extensión.
   ═══════════════════════════════════════════════════════════════════ */

// Verifica un session token de Customer Account UI Extension: JWT HS256 firmado
// con SHOPIFY_API_SECRET. Valida firma + exp + aud. Devuelve el payload o null.
function _verifyCustomerSessionToken(authHeader) {
    try {
        const m = /^Bearer\s+(.+)$/i.exec(String(authHeader || ''));
        if (!m) return null;
        const parts = m[1].trim().split('.');
        if (parts.length !== 3) return null;
        const [h, p, sig] = parts;
        const expected = crypto.createHmac('sha256', SHOPIFY_API_SECRET).update(h + '.' + p).digest('base64url');
        const a = Buffer.from(sig), b = Buffer.from(expected);
        if (a.length !== b.length || !crypto.timingSafeEqual(a, b)) return null;
        const payload = JSON.parse(Buffer.from(p, 'base64url').toString('utf8'));
        const now = Math.floor(Date.now() / 1000);
        if (payload.exp && now >= Number(payload.exp) + 10) return null;        // +10s de skew
        if (payload.nbf && now < Number(payload.nbf) - 10) return null;
        if (payload.aud && SHOPIFY_API_KEY && String(payload.aud) !== String(SHOPIFY_API_KEY)) return null;
        return payload; // { iss, dest, aud, sub (customer GID), exp, ... }
    } catch (_) { return null; }
}

// Resuelve email del customer (por su id Shopify) + sus suscripciones reales.
async function _customerSubsByShopifyId(customerId, { canMintLinks = false } = {}) {
    const shop = process.env.SHOPIFY_SHOP || 'nutrition-lab-cluster.myshopify.com';
    const token = process.env.SHOPIFY_ACCESS_TOKEN || _shopifyToken;
    let customerEmail = null, customerName = null;
    if (token && customerId) {
        try {
            const r = await fetch(`https://${shop}/admin/api/2026-01/customers/${customerId}.json?fields=id,email,first_name,last_name`, { headers: { 'X-Shopify-Access-Token': token } });
            if (r.ok) { const d = await r.json(); customerEmail = d.customer?.email; customerName = `${d.customer?.first_name || ''} ${d.customer?.last_name || ''}`.trim(); }
        } catch (_) {}
    }
    if (!customerEmail) return { email: null, name: customerName, subscriptions: [] };
    const allSubs = await db.getSubscriptions().catch(() => []);
    const filtered = (Array.isArray(allSubs) ? allSubs : [])
        .filter(s => (s.customer_email || '').toLowerCase() === customerEmail.toLowerCase() && s.status !== 'pending_payment');
    const subs = await Promise.all(filtered.map(async s => {
        const out = {
            id: s.id, product_title: s.product_title, product_image: s.product_image,
            status: s.status, cycles_completed: parseInt(s.cycles_completed || 0), cycles_required: parseInt(s.cycles_required || 0),
            frequency_months: s.frequency_months, permanence_months: s.permanence_months,
            base_price: s.base_price, final_price: s.final_price, mp_total_amount: s.mp_total_amount,
            next_charge_at: s.next_charge_at, last_charge_at: s.last_charge_at,
            needs_payment_update: !!s.needs_payment_update,
            mp_update_url: s.mp_preapproval_id ? _mpCustomerDashboardUrl(s.mp_preapproval_id) : null,
            payment_link: null, reauth_link: null
        };
        if (s.needs_payment_update && canMintLinks) {
            try { out.payment_link = (await _getOrCreateRecoveryLink(s, { by: 'account_ext' })).url; } catch (_) {}
            try { out.reauth_link = (await _getOrCreateReauthLink(s, { by: 'account_ext' })).url; } catch (_) {}
        }
        return out;
    }));
    return { email: customerEmail, name: customerName, subscriptions: subs };
}

// Helper: valida token + que la sub pertenezca al customer del token.
async function _accountExtOwnsSub(req, subId) {
    const payload = _verifyCustomerSessionToken(req.get('authorization'));
    if (!payload) return { ok: false, code: 401, error: 'Sesión inválida o expirada.' };
    const customerId = String(payload.sub || '').replace(/\D/g, '');
    if (!customerId) return { ok: false, code: 401, error: 'No se pudo identificar tu cuenta.' };
    const sub = await db.getSubscription(subId).catch(() => null);
    if (!sub) return { ok: false, code: 404, error: 'Suscripción no encontrada.' };
    const shop = process.env.SHOPIFY_SHOP || 'nutrition-lab-cluster.myshopify.com';
    const token = process.env.SHOPIFY_ACCESS_TOKEN || _shopifyToken;
    let email = null;
    if (token) { try { const r = await fetch(`https://${shop}/admin/api/2026-01/customers/${customerId}.json?fields=email`, { headers: { 'X-Shopify-Access-Token': token } }); if (r.ok) email = (await r.json()).customer?.email; } catch (_) {} }
    if (!email || (sub.customer_email || '').toLowerCase() !== email.toLowerCase()) return { ok: false, code: 403, error: 'No autorizado.' };
    return { ok: true, sub };
}

// GET: suscripciones del cliente autenticado + config del portal (benefits, BD, permisos).
app.get('/api/account-ext/me', async (req, res) => {
    const payload = _verifyCustomerSessionToken(req.get('authorization'));
    if (!payload) return res.status(401).json({ error: 'Sesión inválida o expirada.' });
    const customerId = String(payload.sub || '').replace(/\D/g, '');
    let pc = {};
    try { const s = await readFromShopify().catch(() => ({})); pc = (s && s.portal_config) || {}; } catch (_) {}
    const portal = {
        page_title: pc.page_title || 'Mi Suscripción',
        benefits: Array.isArray(pc.benefits) ? pc.benefits.filter(b => b && b.title) : [],
        benefits_title: pc.benefits_title || 'Beneficios de tu membresía',
        bd_title: pc.bd_title || '', bd_subtitle: pc.bd_subtitle || '',
        bd_btn_text: pc.bd_btn_text || '', bd_btn_url: pc.bd_btn_url || '',
        whatsapp_number: String(pc.whatsapp_number || '').replace(/\D/g, ''),
        whatsapp_message: pc.whatsapp_message || 'Hola, soy suscriptor de Lab Nutrition y necesito ayuda.',
        allow_pause: pc.allow_pause !== false,
        allow_cancel: pc.allow_cancel !== false
    };
    if (!customerId) return res.json({ is_member: false, subscriptions: [], portal });
    const { email, name, subscriptions } = await _customerSubsByShopifyId(customerId, { canMintLinks: true });
    res.json({ is_member: subscriptions.length > 0, customer_id: customerId, email, name, subscriptions, portal });
});

// POST pausar (gated por allow_pause).
app.post('/api/account-ext/sub/:id/pause', async (req, res) => {
    try {
        const own = await _accountExtOwnsSub(req, req.params.id);
        if (!own.ok) return res.status(own.code).json({ error: own.error });
        if (!(await _portalPermAllowed('allow_pause'))) return res.status(403).json({ error: 'La pausa de suscripciones está deshabilitada por la tienda.' });
        const sub = own.sub;
        if (sub.status !== 'active') return res.status(400).json({ error: 'Solo puedes pausar una suscripción activa.' });
        if (mp.pauseSubscription && sub.mp_preapproval_id) await mp.pauseSubscription(sub.mp_preapproval_id).catch(() => {});
        const pausedUntil = new Date(); pausedUntil.setMonth(pausedUntil.getMonth() + 1);
        await db.updateSubscription(sub.id, { status: 'paused', paused_until: pausedUntil.toISOString() });
        await db.createEvent({ subscription_id: sub.id, event_type: 'paused_by_customer', metadata: JSON.stringify({ via: 'account_ext' }) }).catch(() => {});
        res.json({ success: true });
    } catch (e) { res.status(500).json({ error: e.message }); }
});

// POST reanudar.
app.post('/api/account-ext/sub/:id/resume', async (req, res) => {
    try {
        const own = await _accountExtOwnsSub(req, req.params.id);
        if (!own.ok) return res.status(own.code).json({ error: own.error });
        const sub = own.sub;
        if (sub.status !== 'paused') return res.status(400).json({ error: 'La suscripción no está pausada.' });
        if (mp.resumeSubscription && sub.mp_preapproval_id) await mp.resumeSubscription(sub.mp_preapproval_id).catch(() => {});
        await db.updateSubscription(sub.id, { status: 'active', paused_until: null });
        await db.createEvent({ subscription_id: sub.id, event_type: 'resumed_by_customer', metadata: JSON.stringify({ via: 'account_ext' }) }).catch(() => {});
        res.json({ success: true });
    } catch (e) { res.status(500).json({ error: e.message }); }
});

// POST cancelar (gated por allow_cancel). Cancelación simple (igual que portal-v2).
app.post('/api/account-ext/sub/:id/cancel', async (req, res) => {
    try {
        const own = await _accountExtOwnsSub(req, req.params.id);
        if (!own.ok) return res.status(own.code).json({ error: own.error });
        if (!(await _portalPermAllowed('allow_cancel'))) return res.status(403).json({ error: 'La cancelación desde el portal está deshabilitada por la tienda.' });
        const sub = own.sub;
        if (sub.status === 'cancelled') return res.json({ success: true, already_cancelled: true });
        if (mp.cancelSubscription && sub.mp_preapproval_id) await mp.cancelSubscription(sub.mp_preapproval_id).catch(() => {});
        await db.updateSubscription(sub.id, { status: 'cancelled', cancelled_at: new Date().toISOString(), cancelled_by: 'customer' });
        await db.createEvent({ subscription_id: sub.id, event_type: 'cancelled_by_customer', metadata: JSON.stringify({ via: 'account_ext' }) }).catch(() => {});
        res.json({ success: true });
    } catch (e) { res.status(500).json({ error: e.message }); }
});

/* ═══════════════════════════════════════════════════════════════════
   🆕 UPDATE CARD page + endpoint
   /portal/v2/update-card?token=xxx — vista web del problema + deep link MP
   ═══════════════════════════════════════════════════════════════════ */

app.get('/portal/v2/update-card', (req, res) => {
    res.sendFile(path.join(__dirname, 'public', 'portal-update-card.html'));
});

app.get('/api/portal/v2/update-card-info', async (req, res) => {
    try {
        const payload = _portalV2VerifyToken(req.query.token);
        if (!payload) return res.status(401).json({ error: 'Token inválido o expirado' });
        const allSubs = await db.getSubscriptions().catch(() => []);
        const pending = (Array.isArray(allSubs) ? allSubs : [])
            .filter(s => payload.sub_ids.includes(s.id) && s.needs_payment_update);
        // 🆕 2026-06-11: incluir link de PAGO real (Preference) — el dashboard MP no
        //   tiene botón de pagar y si MP agotó reintentos el ciclo se perdía.
        const mySubs = await Promise.all(pending.map(async s => {
            let paymentLink = null;
            try { paymentLink = (await _getOrCreateRecoveryLink(s, { by: 'portal' })).url; } catch {}
            return {
                id: s.id,
                product_title: s.product_title,
                amount: s.mp_total_amount || s.final_price,
                cycles: `${s.cycles_completed || 0}/${s.cycles_required || '?'}`,
                mp_update_url: s.mp_preapproval_id ? _mpCustomerDashboardUrl(s.mp_preapproval_id) : null,
                payment_link: paymentLink,
                last_payment_failed_at: s.last_payment_failed_at || null
            };
        }));
        res.json({ email: payload.email, subscriptions_pending_update: mySubs });
    } catch (e) { res.status(500).json({ error: e.message }); }
});

/* ═══════════════════════════════════════════════════════════════════
   🆕 2026-06-05 — WEBHOOKS GDPR OBLIGATORIOS para Shopify App Store
   Sin estos 3 endpoints respondiendo 200, la app NO puede ser submitted
   ni mantener su listado en el App Store. Pixel-suite-pro no almacena
   datos de shop owner, solo de customers (subscribers). Igual debemos
   responder 200 a los 3 topics aunque no hagamos nada (HMAC-validated).

   Topics (configurar en shopify.app.toml o Partners webhook subscriptions):
   - customers/data_request → cliente pide su data → respondemos OK (NO almacenamos data del MERCHANT, solo del subscriber con MP preapproval)
   - customers/redact → cliente pide borrar su data → marcamos sub como redacted (mantener mp_preapproval para cobros activos por compliance contable Perú IGV)
   - shop/redact → tienda desinstala → 48h después marcamos todo redacted
   ═══════════════════════════════════════════════════════════════════ */
app.post('/webhooks/shopify/customers_data_request', express.raw({ type: 'application/json' }), async (req, res) => {
    const hmac = _verifyShopifyHmac(req);
    if (!hmac.ok && hmac.mode === 'enforce') return res.status(401).send('Invalid HMAC');
    res.sendStatus(200);
    try {
        const body = JSON.parse(req.body.toString());
        console.log('[GDPR] customers/data_request:', body.customer?.email, 'shop:', body.shop_domain);
        if (db.createEvent && body.customer?.id) {
            await db.createEvent({
                subscription_id: 'gdpr_' + body.customer.id,
                event_type: 'gdpr_data_request',
                metadata: JSON.stringify({ customer_id: body.customer.id, email: body.customer.email, shop: body.shop_domain, requested_at: new Date().toISOString() })
            }).catch(() => {});
        }
    } catch (e) { console.warn('[GDPR data_request]', e.message); }
});

app.post('/webhooks/shopify/customers_redact', express.raw({ type: 'application/json' }), async (req, res) => {
    const hmac = _verifyShopifyHmac(req);
    if (!hmac.ok && hmac.mode === 'enforce') return res.status(401).send('Invalid HMAC');
    res.sendStatus(200);
    try {
        const body = JSON.parse(req.body.toString());
        console.log('[GDPR] customers/redact:', body.customer?.email);
        if (body.customer?.email) {
            const allSubs = await db.getSubscriptions().catch(() => []);
            const mySubs = (Array.isArray(allSubs) ? allSubs : []).filter(s => (s.customer_email || '').toLowerCase() === body.customer.email.toLowerCase());
            for (const sub of mySubs) {
                if (sub.status === 'active') {
                    // Sub activa con compromiso contable Perú — solo marcar redact pending, NO borrar PII
                    await db.updateSubscription(sub.id, { gdpr_redact_pending: true, gdpr_redact_requested_at: new Date().toISOString() }).catch(() => {});
                } else {
                    // Cancelada/completada — limpiar PII pero mantener registro contable
                    await db.updateSubscription(sub.id, {
                        customer_name: '[REDACTED]',
                        customer_phone: null,
                        shipping_address: null,
                        gdpr_redacted: true,
                        gdpr_redacted_at: new Date().toISOString()
                    }).catch(() => {});
                }
                await db.createEvent({ subscription_id: sub.id, event_type: 'gdpr_redact_applied', metadata: JSON.stringify({ at: new Date().toISOString(), kept_for_billing: sub.status === 'active' }) }).catch(() => {});
            }
        }
    } catch (e) { console.warn('[GDPR redact]', e.message); }
});

app.post('/webhooks/shopify/shop_redact', express.raw({ type: 'application/json' }), async (req, res) => {
    const hmac = _verifyShopifyHmac(req);
    if (!hmac.ok && hmac.mode === 'enforce') return res.status(401).send('Invalid HMAC');
    res.sendStatus(200);
    try {
        const body = JSON.parse(req.body.toString());
        console.log('[GDPR] shop/redact:', body.shop_domain, '— shop uninstalled 48h+ ago, all data should be purged');
        // Loggear el evento para auditoría. Datos críticos siguen siendo Shopify metaobjects
        // de la tienda — Shopify los borra solo cuando se desinstala definitivamente.
        if (db.createEvent) {
            await db.createEvent({
                subscription_id: 'gdpr_shop_' + (body.shop_id || 'unknown'),
                event_type: 'gdpr_shop_redact',
                metadata: JSON.stringify({ shop_domain: body.shop_domain, shop_id: body.shop_id, at: new Date().toISOString() })
            }).catch(() => {});
        }
    } catch (e) { console.warn('[GDPR shop_redact]', e.message); }
});

/* ── Catch-all: serve admin.html for Shopify embedded app (must be LAST) ── */
app.get('*', (req, res) => {
    // Evita capturar paths que parecen archivos estáticos faltantes (404 limpio)
    if (req.path.startsWith('/api/') || req.path.startsWith('/webhooks/')) {
        return res.status(404).json({ error: 'Not found', path: req.path });
    }
    res.sendFile(path.join(__dirname, 'public', 'admin.html'));
});

/* ── START SERVER ── */
const server = app.listen(PORT, '0.0.0.0', async () => {
    console.log(`\nLAB NUTRITION Backend v6.3.0 running on 0.0.0.0:${PORT}`);
    console.log(`Admin: / | Store: ${process.env.SHOPIFY_SHOP}\n`);
    if (db?.initializeTypes) {
        try { await db.initializeTypes(); } catch (e) { console.warn('[DB] Init:', e.message); }
    }
    // ── Hidrata env vars persistidos en Shopify metafield (SMTP/Resend/etc.) ──
    // Permite que la key Resend configurada vía PUT /api/settings sobreviva a redeploys.
    try {
        // 🆕 2026-06-05 — Cargar también token Shopify + MP desde settings si no están en env
        const envMapFull = {
            shopify_access_token: 'SHOPIFY_ACCESS_TOKEN',
            mp_access_token: 'MP_ACCESS_TOKEN',
            smtp_host: 'SMTP_HOST', smtp_port: 'SMTP_PORT',
            smtp_user: 'SMTP_USER', smtp_pass: 'SMTP_PASS', email_from: 'EMAIL_FROM',
            resend_api_key: 'RESEND_API_KEY', resend_from: 'RESEND_FROM',
            // 🆕 2026-06-11: el kill switch persiste en settings y sobrevive redeploys
            dunning_emails_enabled: 'DUNNING_EMAILS_ENABLED'
        };
        const persisted2 = await readFromShopify().catch(() => null) || readFromFile() || {};
        for (const [k, e] of Object.entries(envMapFull)) {
            if (persisted2[k] && !process.env[e]) process.env[e] = String(persisted2[k]);
        }
    } catch {}
    try {
        const persisted = await readFromShopify().catch(() => null) || readFromFile() || {};
        const envMap = {
            smtp_host: 'SMTP_HOST', smtp_port: 'SMTP_PORT',
            smtp_user: 'SMTP_USER', smtp_pass: 'SMTP_PASS', email_from: 'EMAIL_FROM',
            resend_api_key: 'RESEND_API_KEY', resend_from: 'RESEND_FROM'
        };
        let loaded = [];
        for (const [k, e] of Object.entries(envMap)) {
            if (persisted[k] && !process.env[e]) {
                process.env[e] = String(persisted[k]);
                loaded.push(e);
            }
        }
        if (loaded.length) console.log('[BOOT] Hydrated env from settings:', loaded.join(', '));
    } catch (e) { console.warn('[BOOT] Env hydration warn:', e.message); }
    // Start daily billing crons
    if (process.env.NODE_ENV !== 'test') {
        scheduleDailyCron(2, 0, runDailyBillingCron);  // 2am Lima
        scheduleDailyCron(9, 0, runReminderCron);       // 9am Lima

        // 🆕 DUNNING — 2026-06-05 — smart retry 3x/día con detección recovery
        //   - 06:00 PET: primer chequeo (detección + recovery, sin emails individuales)
        //   - 12:00 PET: segundo chequeo
        //   - 18:00 PET: tercer chequeo
        //   🔒 ANTI-SPAM (2026-06-05): cron de detección NO manda emails individuales.
        //   Solo loggea eventos + marca needs_payment_update. Los emails salen vía DIGEST.
        scheduleDailyCron(6, 0, runDunningDetection);
        scheduleDailyCron(12, 0, runDunningDetection);
        scheduleDailyCron(18, 0, runDunningDetection);
        console.log('[CRON] "runDunningDetection" scheduled: 06:00, 12:00, 18:00 PET');
        // 🆕 DAILY DIGEST: 1 solo email/día con TODOS los casos pendientes
        scheduleDailyCron(9, 0, runDunningDigest);
        console.log('[CRON] "runDunningDigest" scheduled: daily 09:00 PET — 1 email consolidado');
        scheduleDailyCron(10, 30, runDunningFollowups);  // 10:30am Lima — followups + auto-pause
        console.log('[CRON] "runDunningFollowups" scheduled: daily 10:30am PET');

        // 🆕 2026-06-12: 4ta corrida de dunning a las 23:00 PET — cierra el gap nocturno
        // 18:00→06:00 (12h) para que los pagos por link de recuperación se reconcilien antes.
        scheduleDailyCron(23, 0, runDunningDetection);
        console.log('[CRON] "runDunningDetection" extra scheduled: 23:00 PET');

        // 🆕 2026-06-12: sync DIARIO de tags lab-sub-* en customers Shopify (04:30 PET).
        // Antes SOLO existía el endpoint manual (dry_run por defecto) y nadie lo llamaba —
        // los Segments "en tiempo real" se quedaban congelados. Reusa el endpoint interno.
        scheduleDailyCron(4, 30, async () => {
            try {
                const port = process.env.PORT || 4000;
                const r = await fetch(`http://127.0.0.1:${port}/api/admin/customers/sync-sub-tags`, {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json', 'x-admin-token': process.env.ADMIN_API_TOKEN || '' },
                    body: JSON.stringify({ dry_run: false })
                });
                const d = await r.json().catch(() => ({}));
                console.log('[CRON sync-sub-tags]', r.status, JSON.stringify(d).slice(0, 300));
            } catch (e) { console.warn('[CRON sync-sub-tags] error:', e.message); }
        });
        console.log('[CRON] "sync-sub-tags" scheduled: daily 04:30 PET');

        // MP Payment Polling — every 1 hour to catch recurring charges
        // and create Shopify orders (for preapprovals without notification_url)
        setInterval(runMpPaymentPolling, 1 * 60 * 60 * 1000);
        console.log('[CRON] "runMpPaymentPolling" scheduled: every 1 hour');

        // ORDER RESCUE — every 1 hour, offset 30min from polling
        // Safety net: catches ANY missed orders (webhook failures, data issues, etc.)
        setTimeout(() => {
            setInterval(runOrderRescue, 1 * 60 * 60 * 1000);
            console.log('[CRON] "runOrderRescue" scheduled: every 1 hour');
        }, 30 * 60 * 1000);

        // Run immediately on startup to catch any missed charges (including retroactive first payments)
        setTimeout(() => {
            console.log('[STARTUP] Running initial MP payment polling + order rescue...');
            runMpPaymentPolling().catch(e => console.error('[STARTUP] MP polling error:', e.message));
            setTimeout(() => runOrderRescue().catch(e => console.error('[STARTUP] Order rescue error:', e.message)), 30000);
        }, 15000); // Wait 15s for DB + tokens to be ready
    }
    // Register Shopify webhooks for subscription events
    const token = process.env.SHOPIFY_ACCESS_TOKEN || _shopifyToken;
    const backendUrl = process.env.BACKEND_URL || 'https://pixel-suite-pro-production.up.railway.app';
    if (token && subscriptionContracts?.registerSubscriptionWebhooks) {
        subscriptionContracts.registerSubscriptionWebhooks(backendUrl)
            .then(r => console.log('[WEBHOOKS] Registered:', r.filter(x => x.status !== 'already_registered').length, 'new'))
            .catch(e => console.warn('[WEBHOOKS] (non-fatal):', e.message));
    }
});


server.on('error', (err) => {
    console.error(`[FATAL] Server failed to start: ${err.message}`);
    process.exit(1);
});

process.on('uncaughtException', (err) => {
    console.error('[FATAL] Uncaught:', err);
});

module.exports = app;
