require('dotenv').config();
const express = require('express');
const helmet = require('helmet');
const cors = require('cors');
const cron = require('node-cron');
const path = require('path');

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
app.use(cors({ origin: '*' })); // Allow all origins for embedded app

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
// Static assets (CSS, JS, etc) but NOT index fallback
app.use(express.static(path.join(__dirname, 'public'), { index: false }));

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

async function resolveGiftsForNewSub(frequencyMonths, permanenceMonths, productId) {
    try {
        const data = await readFromShopify().catch(() => null);
        const plans = Array.isArray(data?.plans_config) ? data.plans_config : [];
        const freq = Number(frequencyMonths);
        const perm = Number(permanenceMonths);
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
        if (!plan || !plan.gifts || !plan.gifts.enabled) return null;
        const items = Array.isArray(plan.gifts.items) ? plan.gifts.items : [];
        if (!items.length) return null;
        const appliesMode = plan.gifts.applies_to?.mode || 'all_products';
        if (appliesMode === 'specific_products') {
            const ids = (plan.gifts.applies_to?.product_ids || []).map(String);
            if (!ids.includes(String(productId))) return null;
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
        return null;
    }
}

/* ── CREATE subscription ── */
app.post('/api/subscriptions/create', async (req, res) => {
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

/* ── PORTAL CONFIG — Beneficios, Producto semana, Eventos (EARLY REGISTRATION) ── */
app.get('/api/portal-config', async (req, res) => {
    try {
        const settings = await readFromShopify().catch(() => ({}));
        res.json(settings.portal_config || {});
    } catch (e) { res.json({}); }
});

app.put('/api/portal-config', async (req, res) => {
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
        // Amount includes product price + shipping (S/10.00)
        const shippingCost = 10.00;
        const totalAmount = parseFloat((finalPrice + shippingCost).toFixed(2));
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
            discount_pct: discPct,
            base_price: basePrice,
            final_price: parseFloat(finalPrice.toFixed(2)),
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
        const key = req.params.customerId;
        const keyLower = key.toLowerCase();
        // Match by customer_id (Shopify numeric ID) OR email (case-insensitive)
        const filtered = subs.filter(s =>
            s.customer_id === key ||
            (s.customer_email || '').toLowerCase() === keyLower
        );
        res.json(filtered);
    } catch (e) { res.status(500).json({ error: e.message }); }
});

/* ── UPDATE subscription fields (admin) ── */
app.patch('/api/subscriptions/:id', async (req, res) => {
    try {
        const sub = await db.getSubscription(req.params.id);
        if (!sub) return res.status(404).json({ error: 'Not found' });
        // Only allow safe field updates
        // ADD 2026-04-21: gifts_planned/gifts_delivered/gifts_delivered_* → admite backfill de subs
        //   viejas (creadas antes del 15/4 sin array de regalos). No afecta crons ni webhook MP.
        const allowed = ['permanence_months', 'cycles_required', 'discount_pct', 'frequency_months', 'customer_name', 'product_title', 'variant_id', 'product_id', 'status', 'next_charge_at', 'base_price', 'final_price', 'shipping_address', 'tipo_documento', 'dni', 'mp_preapproval_id', 'activated_at', 'cycles_completed', 'last_charge_at', 'customer_email', 'customer_phone', 'gifts_planned', 'gifts_delivered', 'gifts_delivered_at', 'gifts_delivered_order_id', 'gifts_delivered_order_name',
            // 2026-04-21 — bundle configurable
            'bundle_items', 'bundle_config_id', 'bundle_target_quantity', 'bundle_source_product_id', 'bundle_name'];
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

        const updated = await db.updateSubscription(sub.id, updates);
        res.json(updated);
    } catch (e) { res.status(500).json({ error: e.message }); }
});

/* ── PAUSE subscription ── */
app.post('/api/subscriptions/:id/pause', async (req, res) => {
    try {
        const { pauseMonths = 1 } = req.body;
        const sub = await db.getSubscription(req.params.id);
        if (!sub || sub.status !== 'active') return res.status(400).json({ error: 'Cannot pause' });
        if (mp.pauseSubscription) await mp.pauseSubscription(sub.mp_preapproval_id).catch(() => { });
        const pausedUntil = new Date();
        pausedUntil.setMonth(pausedUntil.getMonth() + parseInt(pauseMonths));
        await db.updateSubscription(sub.id, { status: 'paused', paused_until: pausedUntil.toISOString() });
        await db.createEvent({ subscription_id: sub.id, event_type: 'paused', metadata: { pause_months: pauseMonths } });
        res.json({ success: true, pausedUntil });
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
app.get('/api/subscriptions/:id/payments', async (req, res) => {
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
app.get('/api/products', async (req, res) => {
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

app.post('/api/products', async (req, res) => {
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

/* ── PER-PRODUCT CONFIG — descuentos individuales por producto ── */
app.get('/api/products/:id/config', async (req, res) => {
    try {
        const id = req.params.id;
        const settings = await readFromShopify() || readFromFile() || {};
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

app.post('/api/products/:id/config', async (req, res) => {
    try {
        const id = req.params.id;
        const settings = await readFromShopify() || readFromFile() || {};
        if (!settings.product_configs || typeof settings.product_configs !== 'object' || Array.isArray(settings.product_configs)) settings.product_configs = {};
        // Migrate old format if it exists
        if (typeof settings[id] === 'object' && settings[id]?.plans) {
            settings.product_configs[id] = { ...settings[id], ...req.body, updated_at: new Date().toISOString() };
            delete settings[id]; // Remove old key to clean up
        } else {
            settings.product_configs[id] = { ...req.body, updated_at: new Date().toISOString() };
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
app.get('/api/products/search', async (req, res) => {
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
                            variants(first: 3) {
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
app.get('/api/products/:id/variants', async (req, res) => {
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

/* Remove selling plans from a product */
app.delete('/api/selling-plans/:productId', async (req, res) => {
    if (!sellingPlans.getProductSellingPlans || !sellingPlans.deleteSellingPlanGroup) return res.json({ deleted: 0 });
    try {
        const productGid = `gid://shopify/Product/${req.params.productId}`;
        const product = await sellingPlans.getProductSellingPlans(productGid);
        const groups = product && product.sellingPlanGroups ? product.sellingPlanGroups.nodes : [];
        let deleted = 0;
        for (const g of groups) {
            if (g.name && (g.name.includes('LAB') || g.name.includes('PRUEBA') || g.name.includes('Suscripción'))) {
                await sellingPlans.deleteSellingPlanGroup(g.id).catch(() => {});
                deleted++;
            }
        }
        res.json({ deleted });
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
app.post('/webhooks/shopify/orders-paid', express.raw({ type: 'application/json' }), async (req, res) => {
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
app.post('/api/subscriptions/:id/create-order', async (req, res) => {
    try {
        const sub = await db.getSubscription(req.params.id);
        if (!sub) return res.status(404).json({ error: 'Subscription not found' });
        const mpPaymentId = req.body.mp_payment_id || 'manual_' + Date.now();
        const order = await createShopifyOrderFromSub(sub, mpPaymentId);
        if (!order) return res.status(500).json({ error: 'Failed to create order' });
        res.json({ success: true, order_number: order.order_number, order_name: order.name, order_id: order.id });
    } catch (e) { res.status(500).json({ error: e.message }); }
});

/* ── SUBSCRIPTIONS LIST alias for admin panel ── */
app.get('/api/subscriptions', async (req, res) => {
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
app.post('/webhooks/mercadopago', async (req, res) => {
    res.sendStatus(200); // Acknowledge immediately — evitar reintentos de MP
    const { type, data, action } = req.body;
    console.log('[MP WEBHOOK]', type, action, JSON.stringify(data || {}).slice(0, 200));

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

            if (!sub) { console.log('[MP WEBHOOK] No matching sub found for preapproval', preapprovalId); return; }

            if (preapprovalInfo?.status === 'authorized' || action === 'created') {
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
                const activationEvents = db?.getEvents ? await db.getEvents(sub.id, 10).catch(() => []) : [];
                const alreadyHasOrder = activationEvents.some(e => e.event_type === 'first_order_created');

                if (!alreadyHasOrder) {
                    // Crear primera orden en Shopify
                    const firstOrder = await createShopifyOrderFromSub(sub, preapprovalId).catch(e => { console.error('[MP WEBHOOK] Order error:', e.message); return null; });
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
                if (sub.customer_id) shopify.tagCustomerAsSubscriber(sub.customer_id, true).catch(() => {});

                console.log(`[MP WEBHOOK] ✅ Suscripción activada: ${sub.id} | ${sub.customer_email}`);

            } else if (preapprovalInfo?.status === 'cancelled' || preapprovalInfo?.status === 'paused') {
                await db.updateSubscription(sub.id, { status: preapprovalInfo.status }).catch(() => {});
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
                        free_shipping: false
                    };
                    await createShopifyOrderFromSub(orphanSub, resourceId).catch(e => {
                        console.error('[MP WEBHOOK] Orphan order failed (need variant_id):', e.message);
                    });
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
                try { const m = JSON.parse(e.metadata || '{}'); return m.mp_payment_id === resourceId; } catch { return false; }
            });
            if (alreadyProcessed) {
                console.log(`[MP WEBHOOK] Payment ${resourceId} already processed for ${sub.id} — skipping (dedup)`);
                return;
            }

            const cyclesCompleted = (parseInt(sub.cycles_completed) || 0) + 1;
            const nextCharge = new Date();
            nextCharge.setMonth(nextCharge.getMonth() + (parseInt(sub.frequency_months) || 1));
            const isComplete = cyclesCompleted >= (parseInt(sub.cycles_required) || 999);

            // Crear orden Shopify
            const order = await createShopifyOrderFromSub(sub, resourceId).catch(e => {
                console.error('[MP WEBHOOK] ❌ Shopify order error:', e.message); return null;
            });

            // FIX 2026-04-11: Save shopify_order_id + cycle update together
            await db.updateSubscription(sub.id, {
                cycles_completed: cyclesCompleted,
                last_charge_at: new Date().toISOString(),
                next_charge_at: nextCharge.toISOString(),
                status: isComplete ? 'completed' : 'active',
                shopify_order_id: order?.id ? String(order.id) : undefined,
                shopify_order_name: order?.name || undefined
            }).catch(e => console.warn('[MP WEBHOOK] updateSub:', e.message));

            await db.createEvent({ subscription_id: sub.id, event_type: 'charge_success',
                metadata: JSON.stringify({ mp_payment_id: resourceId, cycle: cyclesCompleted,
                    shopify_order_id: order?.id, shopify_order_name: order?.name,
                    amount: paymentData.transaction_amount }) }).catch(() => {});

            if (notifications.sendChargeSuccess) notifications.sendChargeSuccess(sub, order?.order_number).catch(() => {});
            if (isComplete && notifications.sendRenewalInvite) notifications.sendRenewalInvite(sub).catch(() => {});

            console.log(`[MP WEBHOOK] ✅ Cobro procesado: ${sub.customer_email} | ciclo ${cyclesCompleted}/${sub.cycles_required} | order ${order?.name || 'FAIL'}`);
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
    try {
        if (!sub?.customer_email || !token || !shop) return false;
        const since = new Date(Date.now() - 45 * 24 * 60 * 60 * 1000).toISOString();
        const url = `https://${shop}/admin/api/2026-01/orders.json?` +
            `email=${encodeURIComponent(sub.customer_email)}` +
            `&status=any&created_at_min=${encodeURIComponent(since)}&limit=50` +
            `&fields=id,name,note,tags,note_attributes,cancelled_at,created_at`;
        const r = await fetch(url, { headers: { 'X-Shopify-Access-Token': token } });
        if (!r.ok) return false;
        const data = await r.json();
        const orders = (data.orders || []).filter(o =>
            !o.cancelled_at &&
            ((o.tags || '').toLowerCase().includes('suscripcion') ||
             (o.note || '').toLowerCase().includes('suscripci'))
        );
        if (!orders.length) return false;

        const subId = sub.id ? String(sub.id) : '';
        // Sintético = rescue_*, manual_*, o vacío. IDs MP payment son numéricos (ej. '152587784379').
        // Preapproval IDs (alfanuméricos tipo '2c9380847...') los tratamos como NO-sintéticos
        // porque MP los devuelve como ID del evento activation; aún así no matchean mp_payment_id real.
        const mpStr = String(mpPaymentId || '');
        const syntheticPayment = !mpStr || mpStr.startsWith('rescue_') || mpStr.startsWith('manual_');
        // Target cycle: el ciclo que se intenta crear ahora = cycles_completed + 1.
        // (Si es 1er pago, cycles_completed=0 → target=1.)
        const targetCycle = String((parseInt(sub.cycles_completed) || 0) + 1);

        for (const o of orders) {
            const note = o.note || '';
            const attrs = Array.isArray(o.note_attributes) ? o.note_attributes : [];
            const attrSubId = attrs.find(a => a && a.name === 'subscription_id')?.value;
            const attrMpId = attrs.find(a => a && a.name === 'mp_payment_id')?.value;
            const attrCycle = attrs.find(a => a && a.name === 'cycle_number')?.value;

            // REGLA 1 — match por mp_payment_id REAL (el más confiable; MP asegura unicidad).
            // NO bloquea ciclos distintos porque cada ciclo tiene su propio payment_id.
            if (!syntheticPayment) {
                if (attrMpId && String(attrMpId) === String(mpPaymentId)) {
                    return { duplicate: true, existing: o, matched_by: 'mp_payment_id' };
                }
                if (note.includes(String(mpPaymentId))) {
                    return { duplicate: true, existing: o, matched_by: 'note:mp_payment_id' };
                }
            }

            // REGLA 2 — match por subscription_id + cycle_number (para rescue crons con paymentId sintético).
            // Solo bloquea si el MISMO ciclo ya tiene order. Ciclos distintos de la misma sub NO se bloquean.
            if (subId && attrSubId && String(attrSubId) === subId) {
                if (attrCycle && String(attrCycle) === targetCycle) {
                    return { duplicate: true, existing: o, matched_by: 'subscription_id+cycle_number' };
                }
                // Si la order vieja no tiene cycle_number (data previa al hardening),
                // solo bloqueamos si el paymentId es sintético Y cycles_completed == 0
                // (o sea: primer ciclo de la sub y ya hay alguna order) para no abrir un hueco.
                if (!attrCycle && syntheticPayment && (parseInt(sub.cycles_completed) || 0) === 0) {
                    return { duplicate: true, existing: o, matched_by: 'subscription_id (legacy no cycle)' };
                }
            }
        }
        return false;
    } catch (e) {
        console.warn('[ORDER DEDUP] check error:', e.message);
        return false; // Fail-open: si Shopify falla, dejar que la lógica original decida.
    }
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
        const approved = (data?.results || [])
            .filter(p => p.payment?.status === 'approved' || p.status === 'processed')
            .sort((a, b) => new Date(b.date_created) - new Date(a.date_created));
        if (!approved.length) return null;
        const latest = approved[0];
        return {
            id: String(latest.payment?.id || latest.id),
            amount: latest.transaction_amount,
            date_created: latest.date_created
        };
    } catch (e) {
        console.warn('[MP PAYMENT LOOKUP] error:', e.message);
        return null;
    }
}

/* ── Helper: Crear orden en Shopify desde una suscripción ── */
async function createShopifyOrderFromSub(sub, mpPaymentId) {
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
                    sub.shipping_address = { ...(sub.shipping_address || {}), ...addr };
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
        // Re-check after auto-resolve
        shipCheck = assertSubShippable(sub);
        if (!shipCheck.ok) {
            console.error(`[ORDER] ❌ NO se crea orden Shopify para ${sub.customer_email || sub.id}: falta ${shipCheck.missing.join(', ')}. Cobro MP ${mpPaymentId || '?'} guardado. Completá datos y usá /api/admin/subscriptions/:id/retry-order para reintentar.`);
            return null;
        }
        console.log(`[ORDER] ✅ Auto-resolve exitoso para ${sub.customer_email} — datos completos, creando orden`);
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

    // 🔒 HARDENING 2026-04-20: guardia anti-duplicado.
    // Previene el caso Luis Miguel Ordoñez (#8765+#8766 creados por rescue cron sin dedup real).
    // Consulta Shopify como source of truth antes de construir la order.
    const dup = await alreadyHasShopifyOrderForSub(sub, mpPaymentId, shop, token).catch(() => false);
    if (dup && dup.duplicate) {
        console.warn(`[ORDER] 🛑 SKIP duplicado — sub ${sub.id} ya tiene order ${dup.existing.name} en Shopify (match by ${dup.matched_by}). MP payment solicitado: ${mpPaymentId || '?'}. No se crea duplicado.`);
        // Registrar evento para que admin vea que fue bloqueado (no error, es comportamiento correcto).
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

    // Resolver dirección de envío
    let shippingAddr = sub.shipping_address || null;
    if (!shippingAddr && sub.customer_email) {
        shippingAddr = await getCustomerAddress(sub.customer_email, token, shop);
        if (shippingAddr && db?.updateSubscription && sub.id && !sub.id.startsWith('orphan_')) {
            db.updateSubscription(sub.id, { shipping_address: shippingAddr }).catch(function() {});
        }
    }

    const cycleLabel = sub.cycles_completed
        ? ('Ciclo ' + sub.cycles_completed + '/' + (sub.cycles_required || '?'))
        : 'Ciclo 1';

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
        ...(shouldDeliverGifts ? [{ name: 'gift_included', value: String(giftLineItems.length) + ' item(s)' }] : [])
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
            tags: 'suscripcion',
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

    const r = await fetch(`https://${shop}/admin/api/2026-01/orders.json`, {
        method: 'POST',
        headers: { 'X-Shopify-Access-Token': token, 'Content-Type': 'application/json' },
        body: JSON.stringify(orderBody)
    });

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
                gifts_delivered_order_name: '#' + order.order_number
            });
            console.log(`[ORDER] 🎁 Marked gifts_delivered=true for sub ${sub.id} (order #${order.order_number})`);
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

/* ═══════════════════════════════════════════════
   📊 ADMIN DASHBOARD (served as HTML)
═══════════════════════════════════════════════ */
/* NOTE: GET / is already defined at line ~68 — Express only matches the first handler */

app.get('/portal/:customerId', (req, res) => {
    res.sendFile(path.join(__dirname, 'public', 'portal.html'));
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
        // 🏬 Hub Navasoft — todas las subs caen aca
        shopify_location_id: 'SHOPIFY_LOCATION_ID',
        smtp_host: 'SMTP_HOST', smtp_port: 'SMTP_PORT',
        smtp_user: 'SMTP_USER', smtp_pass: 'SMTP_PASS', email_from: 'EMAIL_FROM',
        // Resend (HTTP API — alternativa a SMTP, necesaria en Railway)
        resend_api_key: 'RESEND_API_KEY', resend_from: 'RESEND_FROM'
    };
    Object.entries(envMap).forEach(([key, envKey]) => { if (body[key]) process.env[envKey] = String(body[key]); });
    if (body.shopify_access_token) { process.env.SHOPIFY_ACCESS_TOKEN = body.shopify_access_token; _shopifyToken = body.shopify_access_token; }
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
app.post('/api/subscriptions/:id/resume', async (req, res) => {
    try {
        const sub = await db.getSubscription(req.params.id);
        if (!sub || sub.status !== 'paused') return res.status(400).json({ error: 'Not paused' });
        if (mp.resumeSubscription) await mp.resumeSubscription(sub.mp_preapproval_id).catch(() => { });
        await db.updateSubscription(sub.id, { status: 'active', paused_until: null });
        await db.createEvent({ subscription_id: sub.id, event_type: 'resumed' });
        res.json({ success: true });
    } catch (e) { res.status(500).json({ error: e.message }); }
});

/* GET /api/portal/:email — all subscriptions for a customer (MUST be LAST portal route) */
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
app.get('/health', (req, res) => res.json({ status: 'ok', port: PORT, version: '6.2.0', ts: new Date() }));

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
        // Validación básica
        if (!body.name) return res.status(400).json({ error: 'name is required' });
        if (!body.bundle_product_id) return res.status(400).json({ error: 'bundle_product_id is required (product Shopify del bundle)' });
        if (!body.source_product_id) return res.status(400).json({ error: 'source_product_id is required (product master de sabores)' });
        if (!body.target_quantity || !Number.isFinite(Number(body.target_quantity)) || Number(body.target_quantity) <= 0) {
            return res.status(400).json({ error: 'target_quantity must be a positive number' });
        }
        if (!Array.isArray(body.allowed_variant_ids) || body.allowed_variant_ids.length === 0) {
            return res.status(400).json({ error: 'allowed_variant_ids must be a non-empty array' });
        }
        if (!Array.isArray(body.plans) || body.plans.length === 0) {
            return res.status(400).json({ error: 'plans must be a non-empty array' });
        }
        // Normalizar
        const record = {
            name: String(body.name),
            description: body.description ? String(body.description) : '',
            bundle_product_id: String(body.bundle_product_id),
            bundle_product_handle: body.bundle_product_handle ? String(body.bundle_product_handle) : '',
            source_product_id: String(body.source_product_id),
            source_product_handle: body.source_product_handle ? String(body.source_product_handle) : '',
            source_product_title: body.source_product_title ? String(body.source_product_title) : '',
            target_quantity: Number(body.target_quantity),
            allowed_variant_ids: body.allowed_variant_ids.map(String),
            // 2026-04-21 — Excluidos EXPLICITOS (variantes que NUNCA se muestran, ni siquiera bloqueadas).
            // Útil para sabores descatalogados o pendientes de reposición.
            excluded_variant_ids: Array.isArray(body.excluded_variant_ids) ? body.excluded_variant_ids.map(String) : [],
            // 2026-04-21 — Stock mínimo por variante para considerarla "disponible" (no solo >0).
            // Regla de negocio: si una variante tiene <100 unidades se bloquea (se muestra sombreada).
            min_stock_threshold: Number(body.min_stock_threshold) || 100,
            plans: body.plans.map(p => ({
                freq_months: Number(p.freq_months) || 1,
                perm_months: Number(p.perm_months) || 3,
                price: Number(p.price),
                discount_pct: Number(p.discount_pct) || 0,
                plan_id: p.plan_id ? String(p.plan_id) : '',
                variant_id_perm: p.variant_id_perm ? String(p.variant_id_perm) : ''
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
        const updatable = ['name', 'description', 'bundle_product_id', 'bundle_product_handle',
            'source_product_id', 'source_product_handle', 'source_product_title',
            'target_quantity', 'allowed_variant_ids', 'excluded_variant_ids', 'min_stock_threshold', 'plans',
            'validate_stock', 'hide_stock_from_ui', 'widget_copy', 'active'];
        const updates = {};
        for (const k of updatable) if (k in body) updates[k] = body[k];
        if (updates.target_quantity !== undefined) updates.target_quantity = Number(updates.target_quantity);
        if (Array.isArray(updates.allowed_variant_ids)) updates.allowed_variant_ids = updates.allowed_variant_ids.map(String);
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
        if (!b.title) return res.status(400).json({ error: 'title is required' });
        if (!Array.isArray(b.plans) || b.plans.length === 0) {
            return res.status(400).json({ error: 'plans must be a non-empty array' });
        }
        const variants = b.plans.map(p => ({
            option1: String(p.name || `${p.permanence || 1} meses`),
            price: String(Number(p.price || 0).toFixed(2)),
            // requires_shipping=true (suscripciones llegan a domicilio)
            requires_shipping: true,
            // inventory_management null → Shopify no trackea stock (bundle virtual)
            inventory_management: null,
            // taxable → estándar (se mantiene configuración base de Shopify)
            taxable: true
        }));

        const productPayload = {
            product: {
                title: String(b.title),
                body_html: b.description ? String(b.description) : '',
                vendor: b.vendor ? String(b.vendor) : 'Lab Nutrition',
                product_type: b.product_type ? String(b.product_type) : 'Suscripción',
                tags: Array.isArray(b.tags) ? b.tags.join(', ') : 'suscripcion,bundle',
                status: b.status ? String(b.status) : 'active',
                published: b.published !== false,
                options: [{ name: 'Plan' }],
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

        // Fetch source product from Shopify to get variant details + stock
        const shop = process.env.SHOPIFY_SHOP || 'nutrition-lab-cluster.myshopify.com';
        const token = process.env.SHOPIFY_ACCESS_TOKEN;
        if (!token) return res.status(500).json({ error: 'Shopify token not configured' });

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

/* ── Catch-all: serve admin.html for Shopify embedded app ── */
app.get('*', (req, res) => {
    res.sendFile(path.join(__dirname, 'public', 'admin.html'));
});


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
        const allSubs = db?.getSubscriptions ? await db.getSubscriptions({ status: 'active' }).catch(() => []) : [];
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

                    for (const authPay of approvedPayments) {
                        const paymentId = String(authPay.payment?.id || authPay.id);
                        // Check if we already created an order for this payment
                        const events = db?.getEvents ? await db.getEvents(sub.id, 100).catch(() => []) : [];
                        const alreadyProcessed = events.some(e => e.metadata?.includes?.(paymentId));
                        if (alreadyProcessed) continue;

                        // Create Shopify order
                        try {
                            const order = await createShopifyOrderFromSub(sub, paymentId);
                            if (order) {
                                ordersCreated++;
                                console.log(`[MP POLLING] ✅ Shopify order ${order.name} for ${sub.customer_email} (MP payment ${paymentId})`);

                                // Log event to avoid duplicate processing
                                if (db?.createEvent) {
                                    await db.createEvent({
                                        subscription_id: sub.id,
                                        event_type: 'charge_success',
                                        metadata: JSON.stringify({ mp_payment_id: paymentId, order_name: order.name, amount: authPay.transaction_amount })
                                    }).catch(() => {});
                                }

                                // Send charge success email
                                if (notifications?.sendChargeSuccess) {
                                    notifications.sendChargeSuccess(sub, order.name).catch(e => console.warn('[MP POLLING] Email error:', e.message));
                                }
                            }
                        } catch (orderErr) {
                            console.error(`[MP POLLING] Order creation failed for ${sub.customer_email}:`, orderErr.message);
                        }
                    }

                    // Update cycles count
                    const nextCharge = new Date();
                    nextCharge.setMonth(nextCharge.getMonth() + (parseInt(sub.frequency_months) || 1));
                    await db.updateSubscription(sub.id, {
                        cycles_completed: mpCharged,
                        last_charge_at: new Date().toISOString(),
                        next_charge_at: preData.next_payment_date || nextCharge.toISOString()
                    }).catch(e => console.warn('[MP POLLING] update error:', e.message));
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
        const activeWithCharges = allSubs.filter(s =>
            s.status === 'active' &&
            (parseInt(s.cycles_completed) || 0) > 0
        );
        for (const sub of activeWithCharges) {
            try {
                const events = db?.getEvents ? await db.getEvents(sub.id, 50).catch(() => []) : [];
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

/** Manual trigger for MP payment polling */
app.post('/api/mp-polling/run-now', async (req, res) => {
    res.json({ started: true, message: 'MP payment polling triggered manually' });
    runMpPaymentPolling().catch(console.error);
});

/* Portal endpoints moved to early registration above checkout */

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
