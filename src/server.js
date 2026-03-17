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
let mp, notifications, shopify;
try { mp = require('./services/mercadopago'); } catch (e) { console.warn('[MP] Not configured:', e.message); mp = {}; }
try { notifications = require('./services/notifications'); } catch (e) { console.warn('[EMAIL] Not configured:', e.message); notifications = {}; }
try { shopify = require('./services/shopify'); } catch (e) { console.warn('[SHOPIFY] Not configured:', e.message); shopify = {}; }

const app = express();
const PORT = process.env.PORT || 8080;

/* ─── MIDDLEWARE ─── */
// CSP: MUST allow Shopify Admin to embed this app in an iframe
app.use((req, res, next) => {
    res.setHeader(
        'Content-Security-Policy',
        "frame-ancestors https://admin.shopify.com https://nutrition-lab-cluster.myshopify.com https://*.myshopify.com;"
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
app.use(express.json());
app.use(express.urlencoded({ extended: true }));
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
        console.log(`\n✅ [OAUTH] ACCESS TOKEN CAPTURED!`);
        console.log(`   Shop: ${shop}`);
        console.log(`   Token: ${_shopifyToken}`);
        console.log(`   → Copy this to Railway Variables as SHOPIFY_ACCESS_TOKEN\n`);
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

/* ── PLANS — stored as Shopify Metafields on the shop ── */
const DEFAULT_PLANS = [
    { id: 1, permanence_months: 3, frequency_months: 1, discount_pct: 10, cycles: 3, label: '3 meses' },
    { id: 2, permanence_months: 6, frequency_months: 1, discount_pct: 15, cycles: 6, label: '6 meses' },
    { id: 3, permanence_months: 12, frequency_months: 1, discount_pct: 20, cycles: 12, label: '12 meses' },
    { id: 4, permanence_months: 3, frequency_months: 2, discount_pct: 12, cycles: 2, label: '3 meses bimestral' },
    { id: 5, permanence_months: 6, frequency_months: 2, discount_pct: 18, cycles: 3, label: '6 meses bimestral' },
    { id: 6, permanence_months: 12, frequency_months: 2, discount_pct: 25, cycles: 6, label: '12 meses bimestral' },
];

app.get('/api/plans', async (req, res) => {
    try {
        let saved = await readFromShopify('lab_app', 'plans_config');
        if (!Array.isArray(saved)) saved = null;
        res.json(saved || DEFAULT_PLANS);
    } catch { res.json(DEFAULT_PLANS); }
});

/* Guardar TODOS los planes (botón Guardar cambios en admin) */
app.post('/api/plans', async (req, res) => {
    try {
        const plans = Array.isArray(req.body) ? req.body : req.body.plans;
        if (!plans) return res.status(400).json({ error: 'Expected array of plans' });
        await saveToShopify(plans, 'lab_app', 'plans_config');
        res.json({ success: true, plans });
    } catch (e) { res.status(500).json({ error: e.message }); }
});

/* Guardar un plan individual por ID */
app.put('/api/plans/:id', async (req, res) => {
    try {
        let current = await readFromShopify('lab_app', 'plans_config');
        if (!Array.isArray(current)) current = [...DEFAULT_PLANS];
        const idx = current.findIndex(p => String(p.id) === String(req.params.id));
        if (idx >= 0) current[idx] = { ...current[idx], ...req.body, id: req.params.id };
        else current.push({ ...req.body, id: req.params.id });
        await saveToShopify(current, 'lab_app', 'plans_config');
        res.json({ success: true, plan: current[idx] || req.body });
    } catch (e) { res.status(500).json({ error: e.message }); }
});

/* ── ELIGIBLE PRODUCTS — fetch from Shopify Admin API ── */
app.get('/api/products', async (req, res) => {
    try {
        const shop = process.env.SHOPIFY_SHOP || 'nutrition-lab-cluster.myshopify.com';
        const token = process.env.SHOPIFY_ACCESS_TOKEN || _shopifyToken;
        if (!token) return res.json([]);
        const url = `https://${shop}/admin/api/2024-01/products.json?limit=250&fields=id,title,images,variants,status`;
        const r = await fetch(url, { headers: { 'X-Shopify-Access-Token': token, 'Content-Type': 'application/json' } });
        if (!r.ok) return res.json([]);
        const data = await r.json();
        // Read saved eligible products from Shopify Metafields
        let saved = await readFromShopify('lab_app', 'eligible_products').catch(() => null);
        if (!Array.isArray(saved)) saved = [];
        const activeIds = new Set(saved.filter(p => p.is_active).map(p => String(p.shopify_id)));
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
        const current = await readFromShopify('lab_app', 'eligible_products') || [];
        const pid = req.body.shopify_id || req.body.shopify_product_id;
        const idx = current.findIndex(p => (p.shopify_id || p.shopify_product_id) === pid);
        const entry = { shopify_id: pid, product_title: req.body.product_title, is_active: req.body.is_active !== false, updated_at: new Date().toISOString() };
        if (idx >= 0) current[idx] = { ...current[idx], ...entry };
        else current.push({ ...entry, created_at: new Date().toISOString() });
        await saveToShopify(current, 'lab_app', 'eligible_products');
        console.log('[PRODUCTS] Saved ' + current.length + ' eligible products, toggled ' + pid);
        res.json({ success: true, product: entry });
    } catch (e) {
        console.error('[PRODUCTS] Save error:', e.message);
        res.status(500).json({ error: e.message });
    }
});

/* ── PER-PRODUCT CONFIG — descuentos individuales por producto ── */
app.get('/api/products/:id/config', async (req, res) => {
    try {
        let allConfigs = await readFromShopify('lab_app', 'product_configs').catch(() => null);
        if (!allConfigs || typeof allConfigs !== 'object' || Array.isArray(allConfigs)) allConfigs = {};
        const cfg = allConfigs[req.params.id];
        res.json(cfg || { min_permanence: 3, discounts: { m1_p3: 10, m1_p6: 15, m1_p12: 25, m2_p3: 12, m2_p6: 18, m2_p12: 30 } });
    } catch (e) { res.status(500).json({ error: e.message }); }
});

app.post('/api/products/:id/config', async (req, res) => {
    try {
        let allConfigs = await readFromShopify('lab_app', 'product_configs').catch(() => null);
        if (!allConfigs || typeof allConfigs !== 'object' || Array.isArray(allConfigs)) allConfigs = {};
        allConfigs[req.params.id] = { ...req.body, updated_at: new Date().toISOString() };
        await saveToShopify(allConfigs, 'lab_app', 'product_configs');
        console.log('[PRODUCT CONFIG] Saved config for product', req.params.id);
        res.json({ success: true, config: allConfigs[req.params.id] });
    } catch (e) { res.status(500).json({ error: e.message }); }
});

/* ── SUBSCRIPTIONS LIST alias for admin panel ── */
app.get('/api/subscriptions', async (req, res) => {
    try {
        const { status, limit } = req.query;
        const filters = status ? { status } : {};
        let data = await db.getSubscriptions(filters).catch(() => []);
        if (!Array.isArray(data)) data = [];
        if (limit) data = data.slice(0, parseInt(limit));
        res.json(data);
    } catch (e) { res.json([]); }
});


/* ── METRICS — from Shopify Metaobjects ── */
app.get('/api/metrics', async (req, res) => {
    try {
        const metrics = await db.getMetrics();
        res.json(metrics);
    } catch (e) { res.json({ active: 0, paused: 0, cancelled: 0, mrr: '0.00', next7d: 0, error: e.message }); }
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
   👥 CLIENTES — lee clientes reales de Shopify
══════════════════════════════════════════════════ */
app.get('/api/customers', async (req, res) => {
    try {
        const shop = process.env.SHOPIFY_SHOP || 'nutrition-lab-cluster.myshopify.com';
        const token = process.env.SHOPIFY_ACCESS_TOKEN || _shopifyToken;
        if (!token) return res.json({ customers: [], total: 0, error: 'No Shopify token configured' });

        const { query = '', segment = 'all', page_info } = req.query;
        let url = `https://${shop}/admin/api/2026-01/customers.json?limit=250&fields=id,email,first_name,last_name,orders_count,total_spent,tags,created_at,phone,state`;
        if (query) url += `&email=${encodeURIComponent(query)}`;

        const r = await fetch(url, { headers: { 'X-Shopify-Access-Token': token } });
        if (!r.ok) return res.json({ customers: [], total: 0, error: 'Shopify API error: ' + r.status });
        const data = await r.json();

        // Cross-reference with our subscription records
        const allSubs = await db.getSubscriptions().catch(() => []);
        const subsByEmail = {};
        allSubs.forEach(s => {
            const email = (s.customer_email || '').toLowerCase();
            if (!subsByEmail[email]) subsByEmail[email] = [];
            subsByEmail[email].push(s);
        });

        let customers = (data.customers || []).map(c => {
            const email = (c.email || '').toLowerCase();
            const subs = subsByEmail[email] || [];
            const activeSub = subs.find(s => s.status === 'active');
            return {
                id: c.id,
                email: c.email,
                name: `${c.first_name || ''} ${c.last_name || ''}`.trim() || c.email,
                first_name: c.first_name,
                last_name: c.last_name,
                orders_count: c.orders_count || 0,
                total_spent: c.total_spent || '0.00',
                tags: c.tags || '',
                phone: c.phone,
                state: c.state,
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

        console.log(`[CUSTOMERS] ${customers.length} returned (segment: ${segment})`);
        res.json({ customers, total: customers.length });
    } catch (e) {
        console.error('[CUSTOMERS] Error:', e.message);
        res.json({ customers: [], total: 0, error: e.message });
    }
});

/* ── REMARKETING SEGMENTS — preview counts ── */
app.get('/api/remarketing/segments', async (req, res) => {
    try {
        const allSubs = await db.getSubscriptions().catch(() => []);
        const now = new Date();
        const in7 = new Date(Date.now() + 7 * 86400000);

        const counts = {
            active: allSubs.filter(s => s.status === 'active').length,
            paused: allSubs.filter(s => s.status === 'paused').length,
            cancelled: allSubs.filter(s => s.status === 'cancelled').length,
            next7d: allSubs.filter(s => s.status === 'active' && s.next_charge_at && new Date(s.next_charge_at) <= in7).length,
            all_subscribers: allSubs.length
        };
        res.json(counts);
    } catch (e) { res.json({ active: 0, paused: 0, cancelled: 0, next7d: 0, all_subscribers: 0 }); }
});

/* ── REMARKETING SEND — email campaigns to subscriber segments ── */
app.post('/api/remarketing', async (req, res) => {
    try {
        const { segment = 'active', subject, message, cta_text, cta_url } = req.body;
        if (!subject || !message) return res.status(400).json({ error: 'subject and message are required' });

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

        // Deduplicate by email
        const seen = new Set();
        targets = targets.filter(s => {
            const email = (s.customer_email || '').toLowerCase();
            if (!email || seen.has(email)) return false;
            seen.add(email);
            return true;
        });

        if (!targets.length) return res.json({ success: true, sent: 0, message: 'No recipients in this segment' });

        // Send emails via notifications service if available
        let sent = 0;
        let errors = 0;
        if (notifications.sendRaw) {
            for (const sub of targets) {
                try {
                    await notifications.sendRaw({
                        to: sub.customer_email,
                        name: sub.customer_name,
                        subject,
                        message,
                        cta_text: cta_text || 'Ver mis suscripciones',
                        cta_url: cta_url || `${HOST}/portal`
                    });
                    sent++;
                } catch { errors++; }
            }
        } else {
            // Log recipients if SMTP not configured
            targets.forEach(s => console.log(`[REMARKETING] Would send to: ${s.customer_email}`));
            sent = targets.length;
        }

        console.log(`[REMARKETING] Segment: ${segment}, Sent: ${sent}, Errors: ${errors}`);
        res.json({ success: true, sent, errors, total_recipients: targets.length, segment });
    } catch (e) {
        console.error('[REMARKETING] Error:', e.message);
        res.status(500).json({ error: e.message });
    }
});

/* ═══════════════════════════════════════════════
   🔔 MERCADO PAGO WEBHOOK
═══════════════════════════════════════════════ */
app.post('/webhooks/mercadopago', async (req, res) => {
    res.sendStatus(200); // Acknowledge immediately
    const { type, data } = req.body;
    if (type !== 'preapproval') return;

    try {
        const preapprovalId = data?.id;
        if (!preapprovalId) return;

        const mpSub = await mp.getSubscription(preapprovalId);
        const allSubs = await db.getSubscriptions();
        const sub = allSubs.find(s => s.mp_preapproval_id === preapprovalId);

        if (!sub) return;

        if (mpSub.status === 'authorized' && mpSub.last_modified) {
            const cyclesCompleted = sub.cycles_completed + 1;
            const nextCharge = new Date(sub.next_charge_at);
            nextCharge.setMonth(nextCharge.getMonth() + sub.frequency_months);
            const isComplete = cyclesCompleted >= sub.cycles_required;

            const order = await shopify.createSubscriptionOrder({ sub, cycleNumber: cyclesCompleted, mpPaymentId: mpSub.id });

            await db.updateSubscription(sub.id, {
                cycles_completed: cyclesCompleted,
                next_charge_at: nextCharge.toISOString(),
                status: isComplete ? 'expired' : 'active'
            });
            await db.createEvent({
                subscription_id: sub.id, event_type: 'charge_success',
                amount: sub.final_price, mp_payment_id: mpSub.id,
                shopify_order_id: order.id?.toString(), metadata: { cycle: cyclesCompleted }
            });

            if (notifications.sendChargeSuccess) notifications.sendChargeSuccess(sub, order.order_number).catch(console.error);
            if (isComplete && notifications.sendRenewalInvite) notifications.sendRenewalInvite(sub).catch(console.error);

        } else if (mpSub.status === 'payment_required') {
            await db.updateSubscription(sub.id, { status: 'payment_failed' });
            await db.createEvent({ subscription_id: sub.id, event_type: 'charge_failed' });
            if (notifications.sendChargeFailed) notifications.sendChargeFailed(sub).catch(console.error);
        }
    } catch (e) {
        console.error('MP webhook error:', e.message);
    }
});

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

        // Log campaign
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

/* ═══════════════════════════════════════════════
   📦 STACKS API — Shopify Metafields
═══════════════════════════════════════════════ */
app.get('/api/stacks', async (req, res) => {
    try { res.json(await readFromShopify('lab_app', 'stacks') || []); } catch { res.json([]); }
});

app.post('/api/stacks', async (req, res) => {
    try {
        const current = await readFromShopify('lab_app', 'stacks') || [];
        const idx = current.findIndex(s => s.id === req.body.id);
        if (idx >= 0) current[idx] = req.body;
        else current.push({ ...req.body, id: `stack_${Date.now()}`, created_at: new Date().toISOString() });
        await saveToShopify(current, 'lab_app', 'stacks');
        res.json(req.body);
    } catch (e) { res.status(500).json({ error: e.message }); }
});

app.delete('/api/stacks/:id', async (req, res) => {
    try {
        const current = (await readFromShopify('lab_app', 'stacks') || []).filter(s => s.id !== req.params.id);
        await saveToShopify(current, 'lab_app', 'stacks');
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
        const url = `https://${shop}/admin/api/2024-01/products.json?limit=250&fields=id,title,images,variants,status`;
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
    if (!token) return null;
    const namespace = ns || METAFIELD_NAMESPACE;
    const mfKey = key || METAFIELD_KEY;
    try {
        const r = await fetch(
            `https://${shop}/admin/api/2024-01/metafields.json?metafield[owner_resource]=shop&metafield[namespace]=${namespace}&metafield[key]=${mfKey}`,
            { headers: { 'X-Shopify-Access-Token': token, 'Content-Type': 'application/json' } }
        );
        if (!r.ok) return null;
        const data = await r.json();
        const mf = data.metafields?.[0];
        if (mf?.value) return JSON.parse(mf.value);
    } catch { }
    return null;
}

// Save to Shopify Shop Metafields
async function saveToShopify(settings, ns, key) {
    const shop = process.env.SHOPIFY_SHOP || 'nutrition-lab-cluster.myshopify.com';
    const token = process.env.SHOPIFY_ACCESS_TOKEN || _shopifyToken;
    if (!token) return false;
    const namespace = ns || METAFIELD_NAMESPACE;
    const mfKey = key || METAFIELD_KEY;
    try {
        const checkR = await fetch(
            `https://${shop}/admin/api/2025-01/metafields.json?metafield[owner_resource]=shop&metafield[namespace]=${namespace}&metafield[key]=${mfKey}`,
            { headers: { 'X-Shopify-Access-Token': token } }
        );
        const checkData = await checkR.json();
        const existing = checkData.metafields?.[0];
        const body = existing
            ? { metafield: { id: existing.id, value: JSON.stringify(settings), type: 'json' } }
            : { metafield: { namespace, key: mfKey, value: JSON.stringify(settings), type: 'json', owner_resource: 'shop' } };
        const method = existing ? 'PUT' : 'POST';
        const url = existing
            ? `https://${shop}/admin/api/2024-01/metafields/${existing.id}.json`
            : `https://${shop}/admin/api/2024-01/metafields.json`;
        await fetch(url, { method, headers: { 'X-Shopify-Access-Token': token, 'Content-Type': 'application/json' }, body: JSON.stringify(body) });
        return true;
    } catch (e) { console.warn('[SETTINGS] Shopify metafield save error:', e.message); return false; }
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

/* ── Health check ── */
app.get('/health', (req, res) => res.json({ status: 'ok', port: PORT, ts: new Date() }));

/* ── Catch-all: serve admin.html for Shopify embedded app ── */
app.get('*', (req, res) => {
    res.sendFile(path.join(__dirname, 'public', 'admin.html'));
});

/* ── START SERVER ── */
const server = app.listen(PORT, '0.0.0.0', async () => {
    console.log(`\nLAB NUTRITION Backend running on 0.0.0.0:${PORT}`);
    console.log(`Admin dashboard: /`);
    console.log(`Shopify store: ${process.env.SHOPIFY_SHOP}\n`);
    // Auto-initialize Shopify Metaobject types on boot
    if (db && db.initializeTypes) {
        try { await db.initializeTypes(); } catch (e) { console.warn('[DB] Init:', e.message); }
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
