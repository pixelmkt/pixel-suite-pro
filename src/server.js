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

        // 4. Save to Shopify Metaobjects
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
            expires_at: new Date(Date.now() + permanenceMonths * 30 * 24 * 3600 * 1000).toISOString()
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

        if (!email || !freq || !perm || !finalPrice || !dni) {
            return res.status(400).json({ error: 'Faltan datos: email, frecuencia, permanencia, precio y DNI son obligatorios.' });
        }
        if (!tcAccepted) {
            return res.status(400).json({ error: 'Debes aceptar los Términos y Condiciones para continuar.' });
        }

        // Refresh MP token + variant validation from Shopify settings (single call)
        const dynSettings = await readFromShopify().catch(() => ({}));
        if (dynSettings?.mp_access_token) process.env.MP_ACCESS_TOKEN = dynSettings.mp_access_token;

        // FIX 2026-04-11: Validate variant_id against eligible variants for the product
        if (pId && vId) {
            const pConfigs = (typeof dynSettings?.product_configs === 'object' && !Array.isArray(dynSettings?.product_configs)) ? dynSettings.product_configs : {};
            const pCfg = pConfigs[pId] || {};
            if (Array.isArray(pCfg.eligible_variant_ids) && pCfg.eligible_variant_ids.length > 0) {
                if (!pCfg.eligible_variant_ids.includes(String(vId))) {
                    console.warn(`[CHECKOUT] ⚠️ Variant ${vId} NOT in eligible list [${pCfg.eligible_variant_ids}] for product ${pId}`);
                    return res.status(400).json({ error: 'Esta variante no está habilitada para suscripción. Solo la variante 500g está disponible.' });
                }
            }
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
            tc_ip: tcIp
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

/* ── GET customer subscriptions ── */
app.get('/api/subscriptions/customer/:customerId', async (req, res) => {
    try {
        const subs = await db.getSubscriptions();
        const filtered = subs.filter(s => s.customer_id === req.params.customerId || s.customer_email === req.params.customerId);
        res.json(filtered);
    } catch (e) { res.status(500).json({ error: e.message }); }
});

/* ── UPDATE subscription fields (admin) ── */
app.patch('/api/subscriptions/:id', async (req, res) => {
    try {
        const sub = await db.getSubscription(req.params.id);
        if (!sub) return res.status(404).json({ error: 'Not found' });
        // Only allow safe field updates
        const allowed = ['permanence_months', 'cycles_required', 'discount_pct', 'frequency_months', 'customer_name', 'product_title', 'variant_id', 'product_id', 'status', 'next_charge_at', 'base_price', 'final_price', 'shipping_address', 'tipo_documento', 'dni'];
        const updates = {};
        for (const key of allowed) { if (req.body[key] !== undefined) updates[key] = req.body[key]; }
        if (!Object.keys(updates).length) return res.status(400).json({ error: 'No valid fields to update' });
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

/* ── PLANS — stored inside the proven settings metafield as 'plans_config' sub-key ── */
// FIX 2026-03-23: New separate metafields fail silently in API 2026-01 (owner_resource bug).
// Solution: store plans/products/configs as sub-keys of the EXISTING settings metafield.
app.get('/api/plans', async (req, res) => {
    try {
        const data = await readFromShopify() || readFromFile() || {};
        const saved = data.plans_config;
        console.log('[PLANS] Read', Array.isArray(saved) ? saved.length : 0, 'plans from settings metafield');
        res.json(Array.isArray(saved) ? saved : []);
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

/* ── ELIGIBLE PRODUCTS — fetch from Shopify Admin API ── */
app.get('/api/products', async (req, res) => {
    try {
        const shop = process.env.SHOPIFY_SHOP || 'nutrition-lab-cluster.myshopify.com';
        const token = process.env.SHOPIFY_ACCESS_TOKEN || _shopifyToken;
        if (!token) return res.json([]);
        const url = `https://${shop}/admin/api/2026-01/products.json?limit=250&fields=id,title,images,variants,status`;
        const r = await fetch(url, { headers: { 'X-Shopify-Access-Token': token } });
        if (!r.ok) return res.json([]);
        const data = await r.json();
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
            try {
                const result = await sellingPlans.syncProductPlans({
                    productId: prod.shopify_id,
                    productGid,
                    productTitle: prod.product_title || '',
                    plans: normalizedPlans,
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

    // 1) Metaobjects locales
    try {
        const subs = await db.getSubscriptions({ status: 'active' }).catch(() => []);
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

/* ── Helper: Crear orden en Shopify desde una suscripción ── */
async function createShopifyOrderFromSub(sub, mpPaymentId) {
    const shop = process.env.SHOPIFY_SHOP || 'nutrition-lab-cluster.myshopify.com';
    const token = process.env.SHOPIFY_ACCESS_TOKEN || _shopifyToken;
    if (!token) { console.error('[ORDER] No SHOPIFY_ACCESS_TOKEN — cannot create order'); return null; }

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

    // Build note_attributes so Navasoft picks up the order
    const addr = shippingAddr || {};
    const noteAttrs = [
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
        { name: 'tax_included', value: 'true' }
    ].filter(a => a.value);

    const orderBody = {
        order: {
            email: sub.customer_email,
            financial_status: 'paid',
            taxes_included: true,
            send_receipt: true,
            send_fulfillment_receipt: true,
            line_items: [lineItem],
            discount_codes: [],
            shipping_lines: [{ title: 'Envío suscripción', price: '10.00', code: '02' }],
            note: `LAB NUTRITION Suscripción | ${cycleLabel} | ${sub.frequency_months}m x ${sub.permanence_months}m | ${sub.discount_pct || 0}% OFF | IGV incluido${mpPaymentId ? ' | MP: ' + mpPaymentId : ''}`,
            tags: 'suscripcion',
            note_attributes: noteAttrs,
            shipping_address: shippingAddr || undefined,
            billing_address: shippingAddr ? { ...shippingAddr, company: sub.dni || '' } : undefined,
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
    console.log(`[SHOPIFY ORDER] ✅ Orden #${order.order_number} creada para ${sub.customer_email} | ${cycleLabel}`);
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
        const url = `https://${shop}/admin/api/2026-01/products.json?limit=250&fields=id,title,images,variants,status`;
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
                image: p.images?.[0]?.src || null,
                price: p.variants?.[0]?.price || '0',
                status: p.status,
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
        smtp_host: 'SMTP_HOST', smtp_port: 'SMTP_PORT',
        smtp_user: 'SMTP_USER', smtp_pass: 'SMTP_PASS', email_from: 'EMAIL_FROM'
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
    // Start daily billing crons
    if (process.env.NODE_ENV !== 'test') {
        scheduleDailyCron(2, 0, runDailyBillingCron);  // 2am Lima
        scheduleDailyCron(9, 0, runReminderCron);       // 9am Lima

        // MP Payment Polling — every 4 hours to catch recurring charges
        // and create Shopify orders (for preapprovals without notification_url)
        setInterval(runMpPaymentPolling, 4 * 60 * 60 * 1000);
        console.log('[CRON] "runMpPaymentPolling" scheduled: every 4 hours');

        // Run immediately on startup to catch any missed charges (including retroactive first payments)
        setTimeout(() => {
            console.log('[STARTUP] Running initial MP payment polling to catch missed charges...');
            runMpPaymentPolling().catch(e => console.error('[STARTUP] MP polling error:', e.message));
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
