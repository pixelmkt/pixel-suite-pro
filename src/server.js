require('dotenv').config();
const express = require('express');
const helmet = require('helmet');
const cors = require('cors');
const cron = require('node-cron');
const path = require('path');

const supabase = require('./db/client');

// Crash-proof service imports â€” server starts even without credentials
let mp, notifications, shopify;
try { mp = require('./services/mercadopago'); } catch (e) { console.warn('[MP] Not configured:', e.message); mp = {}; }
try { notifications = require('./services/notifications'); } catch (e) { console.warn('[EMAIL] Not configured:', e.message); notifications = {}; }
try { shopify = require('./services/shopify'); } catch (e) { console.warn('[SHOPIFY] Not configured:', e.message); shopify = {}; }

const app = express();
const PORT = process.env.PORT || 8080;

/* â”€â”€â”€ MIDDLEWARE â”€â”€â”€ */
app.use(helmet({
    contentSecurityPolicy: false,
    frameguard: false,            // Allow Shopify Admin to iframe the app
    crossOriginEmbedderPolicy: false,
    crossOriginOpenerPolicy: false,
    crossOriginResourcePolicy: false
}));
app.use(cors({
    origin: [
        'https://nutrition-lab-cluster.myshopify.com',
        'https://admin.shopify.com',
        'https://labnutrition.com',
        'https://www.labnutrition.com'
    ]
}));
app.use(express.json());
app.use(express.urlencoded({ extended: true }));
app.use(express.static(path.join(__dirname, 'public')));

/* â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
   ðŸ” SHOPIFY OAUTH â€” captures Admin API token
â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â• */
const SHOPIFY_API_KEY = process.env.SHOPIFY_API_KEY || 'fc20b3f68f1c8e854a3dca30788acd48';
const SHOPIFY_API_SECRET = process.env.SHOPIFY_API_SECRET || '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';
const SCOPES = 'read_products,read_orders,write_orders,read_customers,write_customers';
const HOST = process.env.RAILWAY_PUBLIC_DOMAIN
    ? `https://${process.env.RAILWAY_PUBLIC_DOMAIN}`
    : 'https://pixel-suite-pro-production.up.railway.app';

// In-memory token store (persists while server runs)
let _shopifyToken = process.env.SHOPIFY_ACCESS_TOKEN || null;
let _shopifyShop = process.env.SHOPIFY_SHOP || null;
if (_shopifyToken) console.log(`[OAUTH] Token loaded from env â€” shop: ${_shopifyShop}`);

// Start OAuth â€” redirect to Shopify
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

// OAuth callback â€” exchange code for token
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
        console.log(`\nâœ… [OAUTH] ACCESS TOKEN CAPTURED!`);
        console.log(`   Shop: ${shop}`);
        console.log(`   Token: ${_shopifyToken}`);
        console.log(`   â†’ Copy this to Railway Variables as SHOPIFY_ACCESS_TOKEN\n`);
        res.send(`<html><body style="font-family:sans-serif;padding:40px;text-align:center">
            <h2>âœ… AutorizaciÃ³n exitosa</h2>
            <p>Token capturado correctamente.</p>
            <p style="background:#f4f5f7;padding:12px;border-radius:8px;font-family:monospace;font-size:12px">
                SHOPIFY_ACCESS_TOKEN = ${_shopifyToken}
            </p>
            <p style="color:#888;font-size:12px">Copia este token en Railway â†’ Variables â†’ SHOPIFY_ACCESS_TOKEN</p>
            <p><a href="/">â† Volver al dashboard</a></p>
        </body></html>`);
    } catch (e) {
        console.error(`[OAUTH] Error: ${e.message}`);
        res.status(500).send(`OAuth error: ${e.message}`);
    }
});

// Helper to get current token
function getShopifyToken() { return _shopifyToken; }
function getShopifyShop() { return _shopifyShop; }



/* â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
   ðŸ›’ SUBSCRIPTION API
â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â• */

/* â”€â”€ CREATE subscription â”€â”€ */
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

        // 4. Save to Supabase
        const { data: sub, error } = await supabase.from('subscriptions').insert({
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
        }).select().single();

        if (error) throw error;

        // 5. Tag customer in Shopify
        if (customerId) {
            shopify.tagCustomerAsSubscriber(customerId, true).catch(console.error);
        }

        // 6. Send welcome email
        notifications.sendWelcome(sub).catch(console.error);

        // 7. Log event
        await supabase.from('subscription_events').insert({
            subscription_id: sub.id,
            event_type: 'created',
            metadata: { mp_plan_id: plan.id, mp_preapproval_id: mpSub.id }
        });

        res.json({ success: true, subscriptionId: sub.id, nextChargeAt: sub.next_charge_at });
    } catch (e) {
        console.error('Create subscription error:', e.message);
        res.status(500).json({ error: e.message });
    }
});

/* â”€â”€ GET customer subscriptions â”€â”€ */
app.get('/api/subscriptions/customer/:customerId', async (req, res) => {
    const { data, error } = await supabase
        .from('subscriptions')
        .select('*')
        .eq('customer_id', req.params.customerId)
        .order('created_at', { ascending: false });
    if (error) return res.status(500).json({ error: error.message });
    res.json(data);
});

/* â”€â”€ PAUSE subscription â”€â”€ */
app.post('/api/subscriptions/:id/pause', async (req, res) => {
    try {
        const { pauseMonths = 1 } = req.body;
        const { data: sub } = await supabase.from('subscriptions').select('*').eq('id', req.params.id).single();
        if (!sub || sub.status !== 'active') return res.status(400).json({ error: 'Cannot pause' });

        await mp.pauseSubscription(sub.mp_preapproval_id);

        const pausedUntil = new Date();
        pausedUntil.setMonth(pausedUntil.getMonth() + pauseMonths);

        await supabase.from('subscriptions').update({
            status: 'paused', paused_until: pausedUntil.toISOString()
        }).eq('id', sub.id);

        await supabase.from('subscription_events').insert({
            subscription_id: sub.id, event_type: 'paused',
            metadata: { pause_months: pauseMonths, paused_until: pausedUntil }
        });

        res.json({ success: true, pausedUntil });
    } catch (e) {
        res.status(500).json({ error: e.message });
    }
});

/* â”€â”€ SKIP one shipment â”€â”€ */
app.post('/api/subscriptions/:id/skip', async (req, res) => {
    try {
        const { data: sub } = await supabase.from('subscriptions').select('*').eq('id', req.params.id).single();
        if (!sub || sub.status !== 'active') return res.status(400).json({ error: 'Cannot skip' });

        const newNext = new Date(sub.next_charge_at);
        newNext.setMonth(newNext.getMonth() + sub.frequency_months);

        await supabase.from('subscriptions').update({ next_charge_at: newNext.toISOString() }).eq('id', sub.id);
        await supabase.from('subscription_events').insert({
            subscription_id: sub.id, event_type: 'skipped',
            metadata: { skipped_date: sub.next_charge_at, new_date: newNext }
        });

        res.json({ success: true, newNextChargeAt: newNext });
    } catch (e) {
        res.status(500).json({ error: e.message });
    }
});

/* â”€â”€ CANCEL subscription (with anti-abuse window check) â”€â”€ */
app.post('/api/subscriptions/:id/cancel', async (req, res) => {
    try {
        const { data: sub } = await supabase.from('subscriptions').select('*').eq('id', req.params.id).single();
        if (!sub) return res.status(404).json({ error: 'Not found' });

        // Anti-abuse: must complete minimum permanence
        if (sub.cycles_completed < sub.cycles_required) {
            return res.status(403).json({
                error: 'Permanencia incompleta',
                cyclesCompleted: sub.cycles_completed,
                cyclesRequired: sub.cycles_required,
                message: `Debes completar ${sub.cycles_required - sub.cycles_completed} ciclos mÃ¡s antes de cancelar.`
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
                message: `La ventana de cancelaciÃ³n estÃ¡ cerrada. El prÃ³ximo envÃ­o es en ${Math.round(daysUntil)} dÃ­as. PodrÃ¡s cancelar desde ${new Date(nextCharge.getTime() + (daysUntil + sub.frequency_months * 30 - 30) * 86400000).toLocaleDateString('es-PE')}.`
            });
        }

        // Cancel in MP
        await mp.cancelSubscription(sub.mp_preapproval_id);

        await supabase.from('subscriptions').update({
            status: 'cancelled', cancelled_at: now.toISOString()
        }).eq('id', sub.id);

        await supabase.from('subscription_events').insert({
            subscription_id: sub.id, event_type: 'cancelled'
        });

        // Remove subscriber tag from Shopify
        if (sub.customer_id) {
            shopify.tagCustomerAsSubscriber(sub.customer_id, false).catch(console.error);
        }

        res.json({ success: true, message: 'SuscripciÃ³n cancelada correctamente.' });
    } catch (e) {
        res.status(500).json({ error: e.message });
    }
});

/* â”€â”€ PLANS (no-code config) â”€â”€ */
app.get('/api/plans', async (req, res) => {
    const { data } = await supabase.from('subscription_plans').select('*').order('permanence_months');
    res.json(data);
});

app.put('/api/plans/:id', async (req, res) => {
    const { data, error } = await supabase.from('subscription_plans')
        .update(req.body).eq('id', req.params.id).select().single();
    if (error) return res.status(500).json({ error: error.message });
    res.json(data);
});

/* â”€â”€ ELIGIBLE PRODUCTS â”€â”€ */
app.get('/api/products', async (req, res) => {
    const { data } = await supabase.from('eligible_products').select('*').order('created_at', { ascending: false });
    res.json(data);
});

app.post('/api/products', async (req, res) => {
    const { data, error } = await supabase.from('eligible_products').upsert(req.body).select().single();
    if (error) return res.status(500).json({ error: error.message });
    res.json(data);
});

/* â”€â”€ METRICS â”€â”€ */
app.get('/api/metrics', async (req, res) => {
    const [active, paused, cancelled, failed] = await Promise.all([
        supabase.from('subscriptions').select('id', { count: 'exact' }).eq('status', 'active'),
        supabase.from('subscriptions').select('id', { count: 'exact' }).eq('status', 'paused'),
        supabase.from('subscriptions').select('id', { count: 'exact' }).eq('status', 'cancelled'),
        supabase.from('subscriptions').select('id', { count: 'exact' }).eq('status', 'payment_failed')
    ]);
    const mrr = await supabase.from('subscriptions')
        .select('final_price, frequency_months')
        .eq('status', 'active');

    const mrrValue = (mrr.data || []).reduce((acc, s) => {
        return acc + (s.final_price / s.frequency_months);
    }, 0);

    res.json({
        active: active.count || 0,
        paused: paused.count || 0,
        cancelled: cancelled.count || 0,
        payment_failed: failed.count || 0,
        mrr: mrrValue.toFixed(2)
    });
});

/* â”€â”€ ALL SUBSCRIPTIONS (admin) â”€â”€ */
app.get('/api/admin/subscriptions', async (req, res) => {
    const { status, page = 1, limit = 50 } = req.query;
    let q = supabase.from('subscriptions').select('*', { count: 'exact' })
        .order('created_at', { ascending: false })
        .range((page - 1) * limit, page * limit - 1);
    if (status) q = q.eq('status', status);
    const { data, count } = await q;
    res.json({ data, total: count });
});

/* â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
   ðŸ”” MERCADO PAGO WEBHOOK
â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â• */
app.post('/webhooks/mercadopago', async (req, res) => {
    res.sendStatus(200); // Acknowledge immediately
    const { type, data } = req.body;
    if (type !== 'preapproval') return;

    try {
        const preapprovalId = data?.id;
        if (!preapprovalId) return;

        const mpSub = await mp.getSubscription(preapprovalId);
        const { data: sub } = await supabase.from('subscriptions')
            .select('*').eq('mp_preapproval_id', preapprovalId).single();

        if (!sub) return;

        if (mpSub.status === 'authorized' && mpSub.last_modified) {
            // Successful payment cycle
            const cyclesCompleted = sub.cycles_completed + 1;
            const nextCharge = new Date(sub.next_charge_at);
            nextCharge.setMonth(nextCharge.getMonth() + sub.frequency_months);

            const isComplete = cyclesCompleted >= sub.cycles_required;

            // Create Shopify order
            const order = await shopify.createSubscriptionOrder({
                sub,
                cycleNumber: cyclesCompleted,
                mpPaymentId: mpSub.id
            });

            // Update subscription
            await supabase.from('subscriptions').update({
                cycles_completed: cyclesCompleted,
                next_charge_at: nextCharge.toISOString(),
                status: isComplete ? 'expired' : 'active'
            }).eq('id', sub.id);

            // Log event
            await supabase.from('subscription_events').insert({
                subscription_id: sub.id,
                event_type: 'charge_success',
                amount: sub.final_price,
                mp_payment_id: mpSub.id,
                shopify_order_id: order.id?.toString(),
                metadata: { cycle: cyclesCompleted }
            });

            // Send notifications
            notifications.sendChargeSuccess(sub, order.order_number).catch(console.error);
            if (isComplete) notifications.sendRenewalInvite(sub).catch(console.error);

        } else if (mpSub.status === 'payment_required') {
            await supabase.from('subscriptions').update({ status: 'payment_failed' }).eq('id', sub.id);
            await supabase.from('subscription_events').insert({
                subscription_id: sub.id, event_type: 'charge_failed'
            });
            notifications.sendChargeFailed(sub).catch(console.error);
        }
    } catch (e) {
        console.error('MP webhook error:', e.message);
    }
});

/* â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
   â° SCHEDULED NOTIFICATIONS (cron)
â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â• */
// Runs daily at 9:00 AM Lima time
cron.schedule('0 14 * * *', async () => {  // 14:00 UTC = 09:00 PET
    console.log('[CRON] Running daily notification check...');
    const now = new Date();

    try {
        // Get active subscriptions
        const { data: subs } = await supabase
            .from('subscriptions')
            .select('*')
            .eq('status', 'active')
            .not('next_charge_at', 'is', null);

        for (const sub of subs || []) {
            const nextCharge = new Date(sub.next_charge_at);
            const daysUntil = (nextCharge - now) / (1000 * 60 * 60 * 24);

            // -7 days: lock warning
            if (daysUntil >= 6.5 && daysUntil < 7.5) {
                await notifications.sendCancelLockWarning(sub).catch(console.error);
            }

            // -3 days: charge reminder
            if (daysUntil >= 2.5 && daysUntil < 3.5) {
                await notifications.sendChargeReminder(sub).catch(console.error);
            }
        }

        // Resume paused subscriptions that have expired their pause
        const { data: paused } = await supabase.from('subscriptions')
            .select('*').eq('status', 'paused').lt('paused_until', now.toISOString());

        for (const sub of paused || []) {
            await mp.resumeSubscription(sub.mp_preapproval_id).catch(console.error);
            await supabase.from('subscriptions').update({ status: 'active', paused_until: null }).eq('id', sub.id);
        }

        console.log(`[CRON] Processed ${subs?.length || 0} subs, resumed ${paused?.length || 0} paused`);
    } catch (e) {
        console.error('[CRON] Error:', e.message);
    }
}, { timezone: 'America/Lima' });

/* â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
   ðŸ“Š ADMIN DASHBOARD (served as HTML)
â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â• */
app.get('/', (req, res) => {
    res.sendFile(path.join(__dirname, 'public', 'admin.html'));
});

app.get('/portal/:customerId', (req, res) => {
    res.sendFile(path.join(__dirname, 'public', 'portal.html'));
});

/* â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
   ðŸ“£ MARKETING API
â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â• */

/* Send email campaign to a subscriber segment */
app.post('/api/marketing/send', async (req, res) => {
    try {
        const { segment, subject, body, previewText } = req.body;
        if (!subject || !body) return res.status(400).json({ error: 'subject and body required' });

        // Build segment query
        let q = supabase.from('subscriptions').select('customer_email, customer_name, product_title, frequency_months, permanence_months, discount_pct');
        if (segment === 'active') q = q.eq('status', 'active');
        else if (segment === 'paused') q = q.eq('status', 'paused');
        else if (segment === 'failed') q = q.eq('status', 'payment_failed');
        else if (segment === 'cancelled') q = q.eq('status', 'cancelled');
        // 'all' = no filter

        const { data: subs, error } = await q;
        if (error) throw error;

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
        <div class="h">ðŸ§¬ LAB NUTRITION</div>
        <div class="b">${body.replace(/\n/g, '<br>')}</div>
        <div class="f">LAB NUTRITION Â· <a href="${process.env.BACKEND_URL}/portal/${sub.customer_email}" style="color:#9d2a23">Gestionar suscripciÃ³n</a></div>
      </div></body></html>`;
            try {
                const nodemailer = require('nodemailer');
                const t = nodemailer.createTransport({ host: process.env.SMTP_HOST, port: 587, secure: false, auth: { user: process.env.SMTP_USER, pass: process.env.SMTP_PASS } });
                await t.sendMail({ from: process.env.EMAIL_FROM, to: sub.customer_email, subject, html });
                sent++;
            } catch { failed++; }
        }

        // Log campaign
        await supabase.from('subscription_events').insert({
            subscription_id: null, event_type: 'campaign_sent',
            metadata: { subject, segment, total: unique.length, sent, failed }
        }).catch(() => { });

        res.json({ success: true, sent, failed, total: unique.length });
    } catch (e) {
        res.status(500).json({ error: e.message });
    }
});

/* â”€â”€ Export subscribers as CSV â”€â”€ */
app.get('/api/subscribers/export', async (req, res) => {
    const { status } = req.query;
    let q = supabase.from('subscriptions').select('customer_email,customer_name,customer_phone,product_title,frequency_months,permanence_months,discount_pct,final_price,status,cycles_completed,cycles_required,next_charge_at,created_at');
    if (status) q = q.eq('status', status);
    const { data } = await q;

    const header = 'Email,Nombre,TelÃ©fono,Producto,Frecuencia,Permanencia,Descuento%,Precio,Estado,Ciclos,ProximoCobro,Inicio\n';
    const rows = (data || []).map(s => [
        s.customer_email, s.customer_name || '', s.customer_phone || '',
        `"${s.product_title}"`, s.frequency_months === 1 ? 'Mensual' : 'Bimestral',
        s.permanence_months + 'm', s.discount_pct + '%', s.final_price,
        s.status, `${s.cycles_completed}/${s.cycles_required}`,
        s.next_charge_at?.split('T')[0] || '', s.created_at?.split('T')[0] || ''
    ].join(',')).join('\n');

    res.setHeader('Content-Type', 'text/csv');
    res.setHeader('Content-Disposition', 'attachment; filename="suscriptores-lab.csv"');
    res.send(header + rows);
});

/* â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
   ðŸ“¦ STACKS API
â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â• */
app.get('/api/stacks', async (req, res) => {
    const { data } = await supabase.from('subscription_stacks').select('*').order('created_at', { ascending: false });
    res.json(data || []);
});

app.post('/api/stacks', async (req, res) => {
    const { data, error } = await supabase.from('subscription_stacks').upsert(req.body).select().single();
    if (error) return res.status(500).json({ error: error.message });
    res.json(data);
});

app.delete('/api/stacks/:id', async (req, res) => {
    await supabase.from('subscription_stacks').delete().eq('id', req.params.id);
    res.json({ success: true });
});

/* â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
   ðŸ›’ SHOPIFY PRODUCTS API (fetch real products)
â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â• */
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

/* â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
   âš™ï¸ SETTINGS API â€” Shopify Metafields as native storage
   Settings are stored in Shopify Shop Metafields (namespace: lab_app)
   This is the correct 2025/2026 Shopify embedded app pattern.
   No Supabase, no files â€” Shopify IS the database.
â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â• */
const fs = require('fs');
const SETTINGS_FILE = path.join(__dirname, '..', 'settings.json');
const METAFIELD_NAMESPACE = 'lab_app';
const METAFIELD_KEY = 'settings';

// Env vars are ALWAYS the authoritative source for Shopify credentials
// (they come from Railway). Other settings can come from Shopify Metafields.
function getEnvDefaults() {
    return {
        shopify_shop: process.env.SHOPIFY_SHOP || '',
        shopify_access_token: process.env.SHOPIFY_ACCESS_TOKEN || _shopifyToken || '',
        mp_access_token: process.env.MP_ACCESS_TOKEN || '',
        mp_public_key: process.env.MP_PUBLIC_KEY || '',
        supabase_url: process.env.SUPABASE_URL || '',
        supabase_key: process.env.SUPABASE_SERVICE_KEY || '',
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
async function readFromShopify() {
    const shop = process.env.SHOPIFY_SHOP || 'nutrition-lab-cluster.myshopify.com';
    const token = process.env.SHOPIFY_ACCESS_TOKEN || _shopifyToken;
    if (!token) return null;
    try {
        const r = await fetch(
            `https://${shop}/admin/api/2025-01/metafields.json?metafield[owner_resource]=shop&metafield[namespace]=${METAFIELD_NAMESPACE}&metafield[key]=${METAFIELD_KEY}`,
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
async function saveToShopify(settings) {
    const shop = process.env.SHOPIFY_SHOP || 'nutrition-lab-cluster.myshopify.com';
    const token = process.env.SHOPIFY_ACCESS_TOKEN || _shopifyToken;
    if (!token) return false;
    try {
        // First check if metafield exists
        const checkR = await fetch(
            `https://${shop}/admin/api/2025-01/metafields.json?metafield[owner_resource]=shop&metafield[namespace]=${METAFIELD_NAMESPACE}&metafield[key]=${METAFIELD_KEY}`,
            { headers: { 'X-Shopify-Access-Token': token } }
        );
        const checkData = await checkR.json();
        const existing = checkData.metafields?.[0];
        const body = existing
            ? { metafield: { id: existing.id, value: JSON.stringify(settings), type: 'json' } }
            : { metafield: { namespace: METAFIELD_NAMESPACE, key: METAFIELD_KEY, value: JSON.stringify(settings), type: 'json', owner_resource: 'shop' } };
        const method = existing ? 'PUT' : 'POST';
        const url = existing
            ? `https://${shop}/admin/api/2025-01/metafields/${existing.id}.json`
            : `https://${shop}/admin/api/2025-01/metafields.json`;
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

/* â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
   ðŸ‘¤ PORTAL DEL SUSCRIPTOR (customer self-service)
â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â• */

/* GET /api/portal/:email â€” all subscriptions for a customer */
app.get('/api/portal/:email', async (req, res) => {
    const email = decodeURIComponent(req.params.email).toLowerCase().trim();
    try {
        const { data, error } = await supabase
            .from('subscriptions')
            .select(`*, subscription_events(event_type, amount, created_at)`)
            .ilike('customer_email', email)
            .order('created_at', { ascending: false });
        if (error) throw error;
        res.json({ subscriptions: data || [], email });
    } catch (e) {
        // Return empty (stub mode) so portal still renders
        res.json({ subscriptions: [], email, note: 'Database not configured' });
    }
});

/* GET /api/portal/subscription/:id/history */
app.get('/api/portal/subscription/:id/history', async (req, res) => {
    const { data } = await supabase.from('subscription_events')
        .select('*').eq('subscription_id', req.params.id)
        .order('created_at', { ascending: false }).limit(30);
    res.json({ events: data || [] });
});

/* POST /api/portal/subscription/:id/pause */
app.post('/api/portal/subscription/:id/pause', async (req, res) => {
    try {
        const { pauseMonths = 1 } = req.body;
        const { data: sub } = await supabase.from('subscriptions').select('*').eq('id', req.params.id).single();
        if (!sub || sub.status !== 'active') return res.status(400).json({ error: 'Cannot pause' });
        if (mp.pauseSubscription) await mp.pauseSubscription(sub.mp_preapproval_id);
        const pausedUntil = new Date();
        pausedUntil.setMonth(pausedUntil.getMonth() + parseInt(pauseMonths));
        await supabase.from('subscriptions').update({ status: 'paused', paused_until: pausedUntil.toISOString() }).eq('id', sub.id);
        await supabase.from('subscription_events').insert({ subscription_id: sub.id, event_type: 'paused', metadata: { pause_months: pauseMonths } });
        res.json({ success: true, pausedUntil });
    } catch (e) { res.status(500).json({ error: e.message }); }
});

/* POST /api/portal/subscription/:id/skip */
app.post('/api/portal/subscription/:id/skip', async (req, res) => {
    try {
        const { data: sub } = await supabase.from('subscriptions').select('*').eq('id', req.params.id).single();
        if (!sub || sub.status !== 'active') return res.status(400).json({ error: 'Cannot skip' });
        const newNext = new Date(sub.next_charge_at);
        newNext.setMonth(newNext.getMonth() + sub.frequency_months);
        await supabase.from('subscriptions').update({ next_charge_at: newNext.toISOString() }).eq('id', sub.id);
        await supabase.from('subscription_events').insert({ subscription_id: sub.id, event_type: 'skipped' });
        res.json({ success: true, newNextChargeAt: newNext });
    } catch (e) { res.status(500).json({ error: e.message }); }
});

/* POST /api/subscriptions/:id/resume (for portal reactivation) */
app.post('/api/subscriptions/:id/resume', async (req, res) => {
    try {
        const { data: sub } = await supabase.from('subscriptions').select('*').eq('id', req.params.id).single();
        if (!sub || sub.status !== 'paused') return res.status(400).json({ error: 'Not paused' });
        if (mp.resumeSubscription) await mp.resumeSubscription(sub.mp_preapproval_id);
        await supabase.from('subscriptions').update({ status: 'active', paused_until: null }).eq('id', sub.id);
        await supabase.from('subscription_events').insert({ subscription_id: sub.id, event_type: 'resumed' });
        res.json({ success: true });
    } catch (e) { res.status(500).json({ error: e.message }); }
});

/* â”€â”€ Health check â”€â”€ */
app.get('/health', (req, res) => res.json({ status: 'ok', port: PORT, ts: new Date() }));

/* â”€â”€â”€ START â”€â”€â”€ */
console.log(`[BOOT] PORT env = "${process.env.PORT}" â†’ using ${PORT}`);
console.log(`[BOOT] NODE_ENV = "${process.env.NODE_ENV}"`);
console.log(`[BOOT] Binding to 0.0.0.0:${PORT}...`);

const server = app.listen(PORT, '0.0.0.0', () => {
    console.log(`\nðŸš€ LAB NUTRITION Backend running on 0.0.0.0:${PORT}`);
    console.log(`ðŸ“Š Admin dashboard: /`);
    console.log(`ðŸ”— Shopify store: ${process.env.SHOPIFY_SHOP}\n`);
});

server.on('error', (err) => {
    console.error(`[FATAL] Server failed to start: ${err.message}`);
    process.exit(1);
});

process.on('uncaughtException', (err) => {
    console.error('[FATAL] Uncaught:', err);
});

module.exports = app;
