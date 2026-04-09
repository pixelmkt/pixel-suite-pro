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
const SCOPES = 'read_products,write_products,read_orders,write_orders,read_customers,write_customers,read_own_subscription_contracts,write_own_subscription_contracts,read_purchase_options,write_purchase_options';
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
        const phone        = b.phone            || b.customerPhone  || '';
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

        if (!email || !freq || !perm || !finalPrice) {
            return res.status(400).json({ error: 'Faltan datos: email, frecuencia, permanencia y precio son obligatorios.' });
        }

        // Refresh MP token from Shopify settings at runtime
        const dynSettings = await readFromShopify().catch(() => ({}));
        if (dynSettings?.mp_access_token) process.env.MP_ACCESS_TOKEN = dynSettings.mp_access_token;

        const backUrl = `${process.env.BACKEND_URL || 'https://pixel-suite-pro-production.up.railway.app'}/subscriptions/success?email=${encodeURIComponent(email)}&product=${encodeURIComponent(title)}`;

        // Create MP PreApprovalPlan + PreApproval → returns real init_point
        const checkout = await mp.createCheckout({
            frequency: freq,
            permanence: perm,
            amount: parseFloat(finalPrice.toFixed(2)),
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
            mp_plan_id: checkout.plan_id || '',
            mp_preapproval_id: checkout.subscription_id || '',
            status: 'pending_payment',
            cycles_required: Math.ceil(perm / freq),
            cycles_completed: 0,
            free_shipping: freeShip,
            next_charge_at: null,
            created_at: new Date().toISOString()
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

/* ── SKIP one shipment ── */
app.post('/api/subscriptions/:id/skip', async (req, res) => {
    try {
        const sub = await db.getSubscription(req.params.id);
        if (!sub || sub.status !== 'active') return res.status(400).json({ error: 'Cannot skip' });

        const newNext = new Date(sub.next_charge_at);
        newNext.setMonth(newNext.getMonth() + sub.frequency_months);

        await db.updateSubscription(sub.id, { next_charge_at: newNext.toISOString() });
        await db.createEvent({ subscription_id: sub.id, event_type: 'skipped', metadata: { skipped_date: sub.next_charge_at } });
        res.json({ success: true, newNextChargeAt: newNext });
    } catch (e) { res.status(500).json({ error: e.message }); }
});

/* ── CANCEL subscription (with anti-abuse window check) ── */
app.post('/api/subscriptions/:id/cancel', async (req, res) => {
    try {
        const sub = await db.getSubscription(req.params.id);
        if (!sub) return res.status(404).json({ error: 'Not found' });

        // Anti-abuse: must complete minimum permanence
        if (sub.cycles_completed < sub.cycles_required) {
            return res.status(403).json({
                error: 'Permanencia incompleta',
                cyclesCompleted: sub.cycles_completed,
                cyclesRequired: sub.cycles_required,
                message: `Debes completar ${sub.cycles_required - sub.cycles_completed} ciclos más antes de cancelar.`
            });
        }

        // Cancellation window check (30-15 days before next charge)
        const now = new Date();
        const nextCharge = new Date(sub.next_charge_at);
        const daysUntil = (nextCharge - now) / (1000 * 60 * 60 * 24);

        if (daysUntil < 15) {
            return res.status(403).json({
                error: 'Ventana cerrada',
                daysUntil: Math.round(daysUntil),
                message: `La ventana de cancelación está cerrada. El próximo envío es en ${Math.round(daysUntil)} días. Podrás cancelar desde ${new Date(nextCharge.getTime() + (daysUntil + sub.frequency_months * 30 - 30) * 86400000).toLocaleDateString('es-PE')}.`
            });
        }

        if (mp.cancelSubscription) await mp.cancelSubscription(sub.mp_preapproval_id).catch(() => { });
        await db.updateSubscription(sub.id, { status: 'cancelled', cancelled_at: now.toISOString() });
        await db.createEvent({ subscription_id: sub.id, event_type: 'cancelled' });
        if (sub.customer_id) shopify.tagCustomerAsSubscriber(sub.customer_id, false).catch(console.error);
        res.json({ success: true, message: 'Suscripción cancelada correctamente.' });
    } catch (e) { res.status(500).json({ error: e.message }); }
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
            subscription_enabled: activeIds.has(String(p.id))
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
            prodsToSync = [{ shopify_id: req.body.productId, product_title: req.body.productTitle || 'Producto' }];
            console.log('[SELLING_PLANS] Single product sync:', req.body.productId);
        } else {
            // CASE 2: Sync all active products from settings.eligible_products sub-key
            const eligibleProducts = Array.isArray(settingsData.eligible_products) ? settingsData.eligible_products : [];
            prodsToSync = eligibleProducts.filter(p => p.is_active);
            if (!prodsToSync.length) {
                return res.json({ synced: 0, total: 0, message: 'No hay productos activos. Activa productos en la sección Productos o usa el botón Sync de cada producto.' });
            }
        }

        const results = [];
        for (const prod of prodsToSync) {
            const productGid = `gid://shopify/Product/${prod.shopify_id}`;
            try {
                const result = await sellingPlans.syncProductPlans({
                    productId: prod.shopify_id,
                    productGid,
                    productTitle: prod.product_title || '',
                    plans: normalizedPlans
                });
                results.push({ product: prod.product_title, productId: prod.shopify_id, ...result });
                console.log('[SELLING_PLANS] Synced:', prod.product_title, '→ synced:', result.synced);
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
            if (g.name && g.name.includes('LAB')) {
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
        console.log('[ORDER_PAID] Order #' + order.order_number + ' — checking for selling plans...');

        // Find line items with selling plan allocations (= subscription purchases)
        const subLines = (order.line_items || []).filter(li => li.selling_plan_allocation);
        if (!subLines.length) {
            console.log('[ORDER_PAID] No selling plan line items in order #' + order.order_number);
            return;
        }

        const customer = order.customer;
        const shippingAddr = order.shipping_address || null;
        const customerId = customer && customer.id ? `gid://shopify/Customer/${customer.id}` : null;
        const customerEmail = customer && customer.email ? customer.email : null;

        for (const line of subLines) {
            const alloc = line.selling_plan_allocation;
            const sellingPlanId = alloc && alloc.selling_plan && alloc.selling_plan.id
                ? `gid://shopify/SellingPlan/${alloc.selling_plan.id}` : null;
            const variantGid = `gid://shopify/ProductVariant/${line.variant_id}`;
            const linePrice = parseFloat(line.price || 0);
            const frequency = alloc && alloc.selling_plan && alloc.selling_plan.billing_policy
                ? (alloc.selling_plan.billing_policy.interval_count || 1) : 1;

            // 1. Create SubscriptionContract in Shopify
            let shopifyContractId = null;
            if (subscriptionContracts.createContract && customerId) {
                try {
                    const contract = await subscriptionContracts.createContract({
                        customerId,
                        customerEmail,
                        sellingPlanId,
                        variantId: variantGid,
                        linePrice,
                        currencyCode: order.currency || 'PEN',
                        shipAddress: shippingAddr,
                        intervalCount: frequency
                    });
                    if (contract) {
                        shopifyContractId = contract.id;
                        console.log('[CONTRACT] Created Shopify SubscriptionContract:', shopifyContractId);
                    }
                } catch (e) {
                    console.error('[CONTRACT] Failed to create contract:', e.message);
                }
            }

            // 2. Look up the discount from the selling plan pricing policy
            const discountPct = alloc && alloc.selling_plan && alloc.selling_plan.price_adjustments
                ? (alloc.selling_plan.price_adjustments[0] && alloc.selling_plan.price_adjustments[0].value ? alloc.selling_plan.price_adjustments[0].value : 0)
                : 0;

            // 3. Create MP PreApproval subscription for recurring billing
            let mpPlanId = null;
            if (mp.createPlan) {
                try {
                    // Reload MP token dynamically
                    const dynamicSettings = await readFromShopify('lab_app', 'settings').catch(() => ({}));
                    if (dynamicSettings && dynamicSettings.mp_access_token) {
                        process.env.MP_ACCESS_TOKEN = dynamicSettings.mp_access_token;
                    }
                    const productTitle = line.title || 'Producto LAB';
                    const mpPlan = await mp.createPlan({
                        frequency,
                        permanence: frequency * 12, // max 12 cycles by default
                        amount: linePrice,
                        productTitle
                    });
                    mpPlanId = mpPlan && mpPlan.id ? mpPlan.id : null;
                    console.log('[MP] PreApprovalPlan created:', mpPlanId);
                } catch (e) {
                    console.error('[MP] Failed to create PreApprovalPlan:', e.message);
                }
            }

            // 4. Save subscription record to Shopify Metaobjects
            if (db && db.createSubscription) {
                try {
                    const subRecord = {
                        customer_email: customerEmail,
                        customer_name: (customer && customer.first_name ? customer.first_name + ' ' + customer.last_name : null) || customerEmail,
                        shopify_order_id: String(order.id),
                        shopify_order_number: String(order.order_number),
                        shopify_contract_id: shopifyContractId,
                        variant_id: String(line.variant_id),
                        product_id: String(line.product_id),
                        product_title: line.title,
                        product_image: null,
                        base_price: parseFloat(line.price_set && line.price_set.shop_money ? line.price_set.shop_money.amount : line.price),
                        final_price: linePrice,
                        discount_pct: discountPct,
                        frequency_months: frequency,
                        permanence_months: frequency * 12,
                        cycles_required: 12,
                        cycles_completed: 1, // First cycle = the order just paid
                        mp_plan_id: mpPlanId,
                        shipping_address: shippingAddr,
                        free_shipping: false,
                        status: 'active',
                        started_at: new Date().toISOString(),
                        next_charge_at: new Date(Date.now() + frequency * 30 * 24 * 60 * 60 * 1000).toISOString()
                    };
                    await db.createSubscription(subRecord);
                    console.log('[ORDER_PAID] Subscription record created for', customerEmail);
                } catch (e) {
                    console.error('[ORDER_PAID] Failed to save subscription record:', e.message);
                }
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
    // Throttle: max 1 ejecución cada 30 segundos
    const now = Date.now();
    if (now - _lastAutoImportAt < 30000) return [];
    _lastAutoImportAt = now;

    const mpToken = process.env.MP_ACCESS_TOKEN;
    if (!mpToken || !db?.createSubscription) return [];

    const r = await fetch('https://api.mercadopago.com/preapproval/search?status=authorized&limit=100', {
        headers: { Authorization: `Bearer ${mpToken}` }
    });
    if (!r.ok) return [];
    const mpData = await r.json();
    const preapprovals = mpData?.results || [];
    if (!preapprovals.length) return [];

    const allLocal = await db.getSubscriptions().catch(() => []);
    const existingIds = new Set(allLocal.map(s => s.mp_preapproval_id).filter(Boolean));

    const imported = [];
    for (const pre of preapprovals) {
        if (existingIds.has(pre.id)) continue;
        const email = pre.payer_email;
        if (!email) continue;

        // Si existe una sub pending_payment del mismo email + plan_id → UPDATE (no crear duplicado)
        const orphan = allLocal.find(s =>
            (s.customer_email || '').toLowerCase() === email.toLowerCase() &&
            (s.mp_plan_id === pre.preapproval_plan_id || !s.mp_preapproval_id) &&
            (s.status === 'pending_payment' || s.status === 'pending')
        );
        if (orphan) {
            try {
                const updated = await db.updateSubscription(orphan.id, {
                    mp_preapproval_id: pre.id,
                    status: 'active',
                    next_charge_at: pre.next_payment_date || null,
                    activated_at: pre.date_created || new Date().toISOString()
                });
                if (updated) imported.push(updated);
                console.log(`[AUTO-IMPORT] Linked orphan pending → active: ${email}`);
                continue;
            } catch (e) { console.warn('[AUTO-IMPORT] link orphan failed:', e.message); }
        }
        // Buscar variant_id posible: match por título de producto (reason) en Shopify
        let variantId = null, productId = null, productImage = null;
        try {
            const shop = process.env.SHOPIFY_SHOP || 'nutrition-lab-cluster.myshopify.com';
            const token = process.env.SHOPIFY_ACCESS_TOKEN || _shopifyToken;
            if (token && pre.reason) {
                // Extraer solo el título principal: "LAB NUTRITION — Creatina sin sabor (Mensual × 12 meses)" → "Creatina sin sabor"
                const titleGuess = (pre.reason.match(/—\s*(.+?)\s*\(/) || [])[1] || pre.reason;
                const sr = await fetch(`https://${shop}/admin/api/2026-01/products.json?title=${encodeURIComponent(titleGuess)}&limit=5`, {
                    headers: { 'X-Shopify-Access-Token': token }
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

        const newSub = {
            mp_preapproval_id: pre.id,
            mp_plan_id: pre.preapproval_plan_id || '',
            customer_email: email,
            customer_name: pre.payer_first_name || email.split('@')[0],
            product_title: (pre.reason || '').replace(/^LAB NUTRITION —\s*/, '').replace(/\s*\(.+\)\s*$/, '') || 'Suscripción MP',
            product_id: productId || '',
            variant_id: variantId || '',
            product_image: productImage || '',
            final_price: pre.auto_recurring?.transaction_amount || 0,
            base_price: pre.auto_recurring?.transaction_amount || 0,
            discount_pct: 0,
            frequency_months: pre.auto_recurring?.frequency || 1,
            permanence_months: pre.auto_recurring?.repetitions || 12,
            cycles_required: pre.auto_recurring?.repetitions || 12,
            cycles_completed: 0,
            status: 'active',
            next_charge_at: pre.next_payment_date || null,
            activated_at: pre.date_created || new Date().toISOString(),
            imported_from_mp: true,
            free_shipping: false
        };
        try {
            const created = await db.createSubscription(newSub);
            if (created) imported.push(created);
        } catch (e) {
            console.warn(`[AUTO-IMPORT] Failed for ${email}:`, e.message);
        }
    }
    return imported;
}


/* ── METRICS — from Shopify Metaobjects ── */
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
        const mpToken = process.env.MP_ACCESS_TOKEN;
        if (mpToken) {
            const r = await fetch(`https://api.mercadopago.com/preapproval/search?status=authorized&limit=100`, {
                headers: { Authorization: `Bearer ${mpToken}` }
            });
            if (r.ok) {
                const data = await r.json();
                const results = data?.results || [];
                report.mp_authorized = results.length;
                report.sources.mp_preapprovals = results.map(p => ({
                    id: p.id,
                    email: p.payer_email,
                    reason: p.reason,
                    status: p.status,
                    amount: p.auto_recurring?.transaction_amount,
                    next_payment: p.next_payment_date
                }));
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
            const pr = await fetch(`https://api.mercadopago.com/v1/payments/search?preapproval_id=${pre.id}&status=approved&limit=50`, {
                headers: { Authorization: `Bearer ${mpToken}` }
            });
            if (!pr.ok) { out.errors.push(`payments search ${pre.id}: ${pr.status}`); continue; }
            const pd = await pr.json();
            const payments = pd.results || [];

            // 3) Buscar la sub local asociada a este preapproval o email
            const allSubs = await db.getSubscriptions().catch(() => []);
            let sub = allSubs.find(s => s.mp_preapproval_id === pre.id);
            if (!sub) sub = allSubs.find(s => (s.customer_email || '').toLowerCase() === email.toLowerCase());

            for (const pay of payments) {
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

                // Crear primera orden en Shopify
                await createShopifyOrderFromSub(sub, preapprovalId).catch(e => console.error('[MP WEBHOOK] Order error:', e.message));

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
                        permanence_months: 12,
                        cycles_completed: 0,
                        cycles_required: 12,
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

            const cyclesCompleted = (parseInt(sub.cycles_completed) || 0) + 1;
            const nextCharge = new Date();
            nextCharge.setMonth(nextCharge.getMonth() + (parseInt(sub.frequency_months) || 1));
            const isComplete = cyclesCompleted >= (parseInt(sub.cycles_required) || 999);

            await db.updateSubscription(sub.id, {
                cycles_completed: cyclesCompleted,
                last_charge_at: new Date().toISOString(),
                next_charge_at: nextCharge.toISOString(),
                status: isComplete ? 'completed' : 'active'
            }).catch(e => console.warn('[MP WEBHOOK] updateSub:', e.message));

            // Crear orden Shopify SIEMPRE (esta es la clave del fix)
            const order = await createShopifyOrderFromSub(sub, resourceId).catch(e => {
                console.error('[MP WEBHOOK] ❌ Shopify order error:', e.message); return null;
            });

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

/* También registrar en /api/webhooks/mercadopago para la URL de MP Dashboard */
app.post('/api/webhooks/mercadopago', (req, res, next) => { req.url = '/webhooks/mercadopago'; next('route'); });

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
    if (!token || !sub.variant_id) return null;

    // Resolver dirección de envío
    let shippingAddr = sub.shipping_address || null;
    if (!shippingAddr && sub.customer_email) {
        shippingAddr = await getCustomerAddress(sub.customer_email, token, shop);
        if (shippingAddr) {
            // Guardar para cobros futuros (fire & forget)
            db.updateSubscription(sub.id, { shipping_address: shippingAddr }).catch(function() {});
        }
    }

    const cycleLabel = sub.cycles_completed
        ? ('Ciclo ' + sub.cycles_completed + '/' + (sub.cycles_required || '?'))
        : 'Ciclo 1';

    const orderBody = {
        order: {
            email: sub.customer_email,
            financial_status: 'paid',
            send_receipt: true,
            send_fulfillment_receipt: true,
            line_items: [{ variant_id: parseInt(sub.variant_id), quantity: 1, price: String(parseFloat(sub.final_price || sub.base_price).toFixed(2)) }],
            note: `LAB NUTRITION Suscripción | ${cycleLabel} | ${sub.frequency_months}m x ${sub.permanence_months}m | ${sub.discount_pct || 0}% OFF${mpPaymentId ? ' | MP: ' + mpPaymentId : ''}`,
            tags: 'suscripcion,lab-nutrition,recurrente',
            shipping_address: shippingAddr || undefined,
            shipping_lines: sub.free_shipping ? [{ title: 'Envío gratis (suscriptor)', price: '0.00', code: 'free_sub', source: 'lab_sub' }] : []
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
app.get('/', (req, res) => {
    res.sendFile(path.join(__dirname, 'public', 'admin.html'));
});

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
                status: p.status
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
═══════════════════════════════════════════════ */

/* GET /api/portal/:email — all subscriptions for a customer */
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

/* GET /api/portal/subscription/:id/history */
app.get('/api/portal/subscription/:id/history', async (req, res) => {
    try {
        const events = await db.getEvents ? await db.getEvents(req.params.id) : [];
        res.json({ events: events || [] });
    } catch (e) { res.json({ events: [] }); }
});

/* POST /api/portal/subscription/:id/pause */
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

/* POST /api/portal/subscription/:id/skip */
app.post('/api/portal/subscription/:id/skip', async (req, res) => {
    try {
        const sub = await db.getSubscription(req.params.id);
        if (!sub || sub.status !== 'active') return res.status(400).json({ error: 'Cannot skip' });
        const newNext = new Date(sub.next_charge_at);
        newNext.setMonth(newNext.getMonth() + sub.frequency_months);
        await db.updateSubscription(sub.id, { next_charge_at: newNext.toISOString() });
        await db.createEvent({ subscription_id: sub.id, event_type: 'skipped' });
        res.json({ success: true, newNextChargeAt: newNext });
    } catch (e) { res.status(500).json({ error: e.message }); }
});

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

/* ══════════════════════════════════════════════════════
   🛒 MP CHECKOUT — Suscripción recurrente real con Mercado Pago
   Widget (modal) → POST /api/subscriptions/checkout →
   MP PreApproval (init_point) → cliente autoriza tarjeta →
   MP cobra mensualmente → webhook /webhooks/mp →
   backend crea pedido Shopify por cada cobro automático
══════════════════════════════════════════════════════ */

/* Helper: create a Shopify order for each MP recurring payment */
async function createShopifyOrderFromSubscription({ customerEmail, customerName, variantId, price, productTitle, subscriptionNote }) {
    const shop = process.env.SHOPIFY_SHOP || 'nutrition-lab-cluster.myshopify.com';
    const token = process.env.SHOPIFY_ACCESS_TOKEN || _shopifyToken;
    if (!token) throw new Error('No Shopify token for order creation');

    const body = {
        order: {
            email: customerEmail,
            financial_status: 'paid',
            send_receipt: true,
            send_fulfillment_receipt: true,
            note: subscriptionNote || 'LAB NUTRITION — Cobro automático de suscripción',
            tags: 'suscripcion,cobro-automatico,mercadopago',
            line_items: [{
                variant_id: parseInt(variantId),
                quantity: 1,
                price: parseFloat(price).toFixed(2),
                requires_shipping: true,
                title: productTitle || 'Suscripción LAB'
            }],
            customer: { email: customerEmail }
        }
    };
    const r = await fetch(`https://${shop}/admin/api/2026-01/orders.json`, {
        method: 'POST',
        headers: { 'X-Shopify-Access-Token': token, 'Content-Type': 'application/json' },
        body: JSON.stringify(body)
    });
    if (!r.ok) {
        const errTxt = await r.text();
        throw new Error(`Shopify order failed ${r.status}: ${errTxt.slice(0, 200)}`);
    }
    return (await r.json()).order;
}

/* POST /api/subscriptions/checkout
   Called by the widget modal when customer clicks "Suscribirme".
   Creates a MP PreApprovalPlan + PreApproval → returns init_point. */
app.post('/api/subscriptions/checkout', async (req, res) => {
    try {
        const {
            customer_name, customer_email, phone,
            product_id, variant_id, product_title,
            base_price, final_price, discount_pct,
            frequency_months, permanence_months
        } = req.body;

        if (!customer_email || !product_id || !final_price) {
            return res.status(400).json({ error: 'Faltan datos: email, product_id y final_price son obligatorios.' });
        }

        // Refresh MP token from settings at runtime
        const dynSettings = await readFromShopify().catch(() => ({}));
        if (dynSettings?.mp_access_token) process.env.MP_ACCESS_TOKEN = dynSettings.mp_access_token;

        const backUrl = `${process.env.BACKEND_URL || 'https://pixel-suite-pro-production.up.railway.app'}/subscriptions/success?email=${encodeURIComponent(customer_email)}&product=${encodeURIComponent(product_title || '')}`;

        // Create MP PreApproval checkout
        const checkout = await mp.createCheckout({
            frequency: parseInt(frequency_months) || 1,
            permanence: parseInt(permanence_months) || 3,
            amount: parseFloat(final_price),
            productTitle: product_title || 'Producto LAB',
            customerEmail: customer_email,
            backUrl
        });

        // Save pending subscription record in Shopify Metaobjects
        const subRecord = {
            customer_name: customer_name || '',
            customer_email,
            phone: phone || '',
            product_id: String(product_id),
            variant_id: String(variant_id || ''),
            product_title: product_title || '',
            base_price: parseFloat(base_price) || parseFloat(final_price),
            final_price: parseFloat(final_price),
            discount_pct: parseInt(discount_pct) || 0,
            frequency_months: parseInt(frequency_months) || 1,
            permanence_months: parseInt(permanence_months) || 3,
            status: 'pending',
            mp_preapproval_id: checkout.subscription_id || '',
            mp_plan_id: checkout.plan_id || '',
            created_at: new Date().toISOString(),
            next_charge_at: new Date().toISOString()
        };

        if (db?.createSubscription) {
            await db.createSubscription(subRecord).catch(e => console.warn('[CHECKOUT] createSubscription error:', e.message));
        } else {
            // Fallback: store in settings metafield
            const settings = await readFromShopify().catch(() => ({}));
            if (!settings.pending_subscriptions) settings.pending_subscriptions = [];
            settings.pending_subscriptions.push(subRecord);
            await saveToShopify(settings).catch(() => {});
        }

        console.log(`[CHECKOUT] ✅ MP checkout created for ${customer_email} — plan:${checkout.plan_id} sub:${checkout.subscription_id}`);
        res.json({
            init_point: checkout.init_point,
            subscription_id: checkout.subscription_id,
            plan_id: checkout.plan_id
        });
    } catch (e) {
        console.error('[CHECKOUT] Error:', e.message);
        res.status(500).json({ error: e.message || 'Error al crear el checkout de MP' });
    }
});

/* GET /subscriptions/success — Confirmation page after MP PreApproval */
app.get('/subscriptions/success', (req, res) => {
    const email = req.query.email || '';
    const product = req.query.product || 'tu producto LAB';
    res.send(`<!DOCTYPE html><html lang="es"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Suscripción Activada — LAB NUTRITION</title>
<style>
*{box-sizing:border-box;margin:0;padding:0}
body{font-family:Montserrat,sans-serif;background:#f5f5f5;min-height:100vh;display:flex;align-items:center;justify-content:center;padding:20px}
.card{background:#fff;border-radius:16px;padding:40px 36px;max-width:480px;width:100%;box-shadow:0 4px 32px rgba(0,0,0,.1);text-align:center}
.icon{width:64px;height:64px;background:#d1fae5;border-radius:50%;display:flex;align-items:center;justify-content:center;margin:0 auto 20px;font-size:28px}
h1{font-size:22px;font-weight:900;color:#1a1a1a;margin-bottom:10px}
p{color:#555;font-size:14px;line-height:1.6;margin-bottom:8px}
.brand{color:#9d2a23;font-weight:800}
.btn{display:inline-block;margin-top:24px;background:#9d2a23;color:#fff;padding:13px 28px;border-radius:10px;text-decoration:none;font-weight:800;font-size:14px;letter-spacing:.5px}
</style></head><body>
<div class="card">
  <div class="icon">✅</div>
  <h1>¡Suscripción activada!</h1>
  <p>Tu suscripción de <strong class="brand">${product}</strong> ha sido procesada correctamente.</p>
  <p>Mercado Pago gestionará los cobros automáticos según tu plan. Recibirás una confirmación en <strong>${email}</strong>.</p>
  <p style="margin-top:12px;font-size:12px;color:#999">Cada mes se generará un pedido automáticamente y recibirás tu producto sin hacer nada.</p>
  <a href="https://labnutrition.com" class="btn">Volver a la tienda →</a>
</div>
</body></html>`);
});

/* ──────────────────────────────────────────────────────────────────────────
   📡 MP WEBHOOKS — Maneja todos los eventos de Mercado Pago
   
   IMPORTANTE: Configurar en MP Developer Dashboard → Aplicación → Webhooks:
   URL: https://pixel-suite-pro-production.up.railway.app/webhooks/mp
   Eventos: preapproval, payment
   
   Flujo:
   • preapproval (status=authorized) → suscripción activada → notificar cliente
   • payment (created by MP for a subscription) → crear pedido Shopify
   • preapproval (status=cancelled/paused) → actualizar estado en BD
──────────────────────────────────────────────────────────────────────────── */
app.post('/webhooks/mp', express.json(), async (req, res) => {
    res.status(200).send('ok'); // Always ack immediately to prevent MP retries

    const { type, action, data } = req.body || {};
    const resourceId = data?.id;

    if (!type || !resourceId) return;
    console.log(`[MP WEBHOOK] type:${type} action:${action} id:${resourceId}`);

    try {
        // ── PAYMENT EVENT: MP charged a subscription → create Shopify order ──
        if (type === 'payment') {
            const payment = await mp.getPayment(resourceId).catch(() => null);
            if (!payment) return console.warn('[MP WEBHOOK] Could not fetch payment:', resourceId);

            // Only process approved subscription payments
            if (payment.status !== 'approved') {
                console.log(`[MP WEBHOOK] Payment ${resourceId} status: ${payment.status} — skipped`);
                return;
            }

            // FIX 2026-04-09: priorizar campo directo preapproval_id del payment
            const preapprovalId = payment.preapproval_id
                || payment.metadata?.preapproval_id
                || payment.external_reference
                || null;
            const payerEmail = payment.payer?.email;
            const transAmount = payment.transaction_amount;

            console.log(`[MP WEBHOOK] ✅ Payment approved — preapproval:${preapprovalId} amount:${transAmount} email:${payerEmail}`);

            // Find subscription with multi-fallback strategy
            let sub = null;
            const allSubs = db?.getSubscriptions ? await db.getSubscriptions().catch(() => []) : [];

            // 1) Match por preapproval_id
            if (preapprovalId) sub = allSubs.find(s => s.mp_preapproval_id === preapprovalId) || null;

            // 2) Fallback por email del payer
            if (!sub && payerEmail) {
                const byEmail = allSubs
                    .filter(s => (s.customer_email || '').toLowerCase() === payerEmail.toLowerCase())
                    .sort((a, b) => (b.created_at || '').localeCompare(a.created_at || ''));
                sub = byEmail.find(s => s.status === 'active' || s.status === 'pending_payment' || s.status === 'pending') || byEmail[0] || null;
                if (sub) console.log(`[MP WEBHOOK] Matched by email fallback: ${sub.id}`);
            }

            // 3) Fallback: pending_subscriptions en settings metafield
            if (!sub) {
                const settings = await readFromShopify().catch(() => ({}));
                sub = (settings.pending_subscriptions || []).find(s =>
                    s.mp_preapproval_id === preapprovalId ||
                    (s.customer_email || '').toLowerCase() === (payerEmail || '').toLowerCase()
                );
            }

            if (!sub) {
                console.warn(`[MP WEBHOOK] ⚠️  No subscription found for preapproval:${preapprovalId} email:${payerEmail}`);
                return;
            }

            // Link preapproval_id si no estaba linkeado
            if (preapprovalId && !sub.mp_preapproval_id && db?.updateSubscription && sub.id) {
                await db.updateSubscription(sub.id, { mp_preapproval_id: preapprovalId }).catch(() => {});
                sub.mp_preapproval_id = preapprovalId;
            }

            // Create Shopify order for this payment
            try {
                const order = await createShopifyOrderFromSubscription({
                    customerEmail: sub.customer_email || payerEmail,
                    customerName: sub.customer_name || '',
                    variantId: sub.variant_id,
                    price: transAmount || sub.final_price,
                    productTitle: sub.product_title,
                    subscriptionNote: `Suscripción LAB — Cobro automático MP (${sub.frequency_months === 1 ? 'Mensual' : `Cada ${sub.frequency_months} meses`}) — Pago MP #${resourceId}`
                });

                console.log(`[MP WEBHOOK] ✅ Shopify order created: ${order.name} for ${sub.customer_email}`);

                // Update subscription cycles count
                if (db?.updateSubscription && sub.id) {
                    const nextCharge = new Date();
                    nextCharge.setMonth(nextCharge.getMonth() + (sub.frequency_months || 1));
                    await db.updateSubscription(sub.id, {
                        status: 'active',
                        cycles_completed: (sub.cycles_completed || 0) + 1,
                        last_charge_at: new Date().toISOString(),
                        next_charge_at: nextCharge.toISOString(),
                        last_order_name: order.name
                    }).catch(() => {});
                }

                // Send charge confirmation email
                await sendAutoEmail({
                    to: sub.customer_email || payerEmail,
                    subject: `✅ Tu suscripción LAB se renovó — Pedido ${order.name}`,
                    html: tplChargeSuccess(
                        sub.customer_name || payerEmail,
                        sub.product_title || 'Producto LAB',
                        transAmount?.toFixed(2) || sub.final_price,
                        'PEN',
                        order.name,
                        new Date(Date.now() + (sub.frequency_months || 1) * 30 * 86400000).toISOString()
                    )
                }).catch(() => {});

            } catch (orderErr) {
                console.error('[MP WEBHOOK] Order creation failed:', orderErr.message);
            }
        }

        // ── PREAPPROVAL EVENT: subscription status changed ──
        if (type === 'preapproval') {
            const preapproval = await mp.getSubscription(resourceId).catch(() => null);
            if (!preapproval) return;

            const status = preapproval.status; // authorized, paused, cancelled, pending
            const payerEmail = preapproval.payer_email;
            console.log(`[MP WEBHOOK] PreApproval ${resourceId} → status:${status} email:${payerEmail}`);

            // Find local subscription record (filter now supported in shopify-storage.js)
            let sub = null;
            if (db?.getSubscriptions) {
                const allSubs = await db.getSubscriptions().catch(() => []);
                sub = allSubs.find(s => s.mp_preapproval_id === resourceId) || null;
                // Fallback por plan_id + email (preapproval recién creada, sub aún pending)
                if (!sub && preapproval.preapproval_plan_id) {
                    const candidates = allSubs.filter(s =>
                        s.mp_plan_id === preapproval.preapproval_plan_id &&
                        (payerEmail ? (s.customer_email || '').toLowerCase() === payerEmail.toLowerCase() : true)
                    );
                    sub = candidates.find(s => s.status === 'pending_payment' || s.status === 'pending') || candidates[0] || null;
                    if (sub) {
                        // Link the preapproval_id now
                        await db.updateSubscription(sub.id, { mp_preapproval_id: resourceId }).catch(() => {});
                        sub.mp_preapproval_id = resourceId;
                    }
                }
            }

            if (sub && db?.updateSubscription) {
                const newStatus = status === 'authorized' ? 'active'
                    : status === 'paused' ? 'paused'
                    : status === 'cancelled' ? 'cancelled'
                    : sub.status;
                await db.updateSubscription(sub.id, { status: newStatus }).catch(() => {});

                if (status === 'authorized' && sub.status === 'pending') {
                    // First authorization — send welcome email
                    await sendAutoEmail({
                        to: sub.customer_email || payerEmail,
                        subject: '🎉 Tu suscripción LAB está activa',
                        html: `<!DOCTYPE html><html><body style="font-family:Montserrat,sans-serif;background:#f5f5f5;padding:20px">
                        <div style="max-width:560px;margin:0 auto;background:#fff;border-radius:14px;overflow:hidden;box-shadow:0 2px 20px rgba(0,0,0,.1)">
                        <div style="background:#9d2a23;color:#fff;padding:28px 32px;font-size:20px;font-weight:900">🎉 ¡Bienvenido a LAB NUTRITION!</div>
                        <div style="padding:32px;line-height:1.7;color:#222">
                        <p>Hola <strong>${sub.customer_name || payerEmail}</strong>,</p>
                        <p>Tu suscripción de <strong>${sub.product_title}</strong> está activa. recibirás tu producto cada ${sub.frequency_months === 1 ? 'mes' : `${sub.frequency_months} meses`} automáticamente.</p>
                        <div style="background:#d1fae5;border-radius:10px;padding:14px 18px;margin:16px 0;color:#065f46;font-weight:700">
                        ✅ Descuento: ${sub.discount_pct || 0}% OFF cada pedido<br>
                        📦 Permanencia: ${sub.permanence_months} meses<br>
                        💳 Cobro automático: ${sub.frequency_months === 1 ? 'Mensual' : `Cada ${sub.frequency_months} meses`}
                        </div>
                        <p>Mercado Pago se encargará de los cobros automáticamente. No necesitas hacer nada.</p>
                        </div>
                        <div style="background:#f2f2f2;padding:14px 32px;text-align:center;font-size:11px;color:#aaa">LAB NUTRITION</div>
                        </div></body></html>`
                    }).catch(() => {});
                }
            }
        }
    } catch (e) {
        console.error('[MP WEBHOOK] Processing error:', e.message);
    }
});

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
        const settings = (await readFromShopify()).settings || await readFromShopify() || {};
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

/** Manual trigger for testing */
app.post('/api/billing/run-now', async (req, res) => {
    res.json({ started: true, message: 'Billing cron triggered manually' });
    runDailyBillingCron().catch(console.error);
});

/* Portal endpoints moved to early registration above checkout */

/* ── START SERVER ── */
const server = app.listen(PORT, '0.0.0.0', async () => {
    console.log(`\nLAB NUTRITION Backend v6.1.0 running on 0.0.0.0:${PORT}`);
    console.log(`Admin: / | Store: ${process.env.SHOPIFY_SHOP}\n`);
    if (db?.initializeTypes) {
        try { await db.initializeTypes(); } catch (e) { console.warn('[DB] Init:', e.message); }
    }
    // Start daily billing crons
    if (process.env.NODE_ENV !== 'test') {
        scheduleDailyCron(2, 0, runDailyBillingCron);  // 2am Lima
        scheduleDailyCron(9, 0, runReminderCron);       // 9am Lima
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
