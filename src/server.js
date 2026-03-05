require('dotenv').config();
const express = require('express');
const helmet = require('helmet');
const cors = require('cors');
const cron = require('node-cron');
const path = require('path');

const supabase = require('./db/client');
const mp = require('./services/mercadopago');
const notifications = require('./services/notifications');
const shopify = require('./services/shopify');

const app = express();
const PORT = process.env.PORT || 3000;

/* ─── MIDDLEWARE ─── */
app.use(helmet({ contentSecurityPolicy: false }));
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

/* ── GET customer subscriptions ── */
app.get('/api/subscriptions/customer/:customerId', async (req, res) => {
    const { data, error } = await supabase
        .from('subscriptions')
        .select('*')
        .eq('customer_id', req.params.customerId)
        .order('created_at', { ascending: false });
    if (error) return res.status(500).json({ error: error.message });
    res.json(data);
});

/* ── PAUSE subscription ── */
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

/* ── SKIP one shipment ── */
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

/* ── CANCEL subscription (with anti-abuse window check) ── */
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

        res.json({ success: true, message: 'Suscripción cancelada correctamente.' });
    } catch (e) {
        res.status(500).json({ error: e.message });
    }
});

/* ── PLANS (no-code config) ── */
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

/* ── ELIGIBLE PRODUCTS ── */
app.get('/api/products', async (req, res) => {
    const { data } = await supabase.from('eligible_products').select('*').order('created_at', { ascending: false });
    res.json(data);
});

app.post('/api/products', async (req, res) => {
    const { data, error } = await supabase.from('eligible_products').upsert(req.body).select().single();
    if (error) return res.status(500).json({ error: error.message });
    res.json(data);
});

/* ── METRICS ── */
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

/* ── ALL SUBSCRIPTIONS (admin) ── */
app.get('/api/admin/subscriptions', async (req, res) => {
    const { status, page = 1, limit = 50 } = req.query;
    let q = supabase.from('subscriptions').select('*', { count: 'exact' })
        .order('created_at', { ascending: false })
        .range((page - 1) * limit, page * limit - 1);
    if (status) q = q.eq('status', status);
    const { data, count } = await q;
    res.json({ data, total: count });
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

/* ═══════════════════════════════════════════════
   ⏰ SCHEDULED NOTIFICATIONS (cron)
═══════════════════════════════════════════════ */
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

/* ═══════════════════════════════════════════════
   📊 ADMIN DASHBOARD (served as HTML)
═══════════════════════════════════════════════ */
app.get('/', (req, res) => {
    res.sendFile(path.join(__dirname, 'public', 'admin.html'));
});

app.get('/portal/:customerId', (req, res) => {
    res.sendFile(path.join(__dirname, 'public', 'portal.html'));
});

/* ── Health check ── */
app.get('/health', (req, res) => res.json({ status: 'ok', ts: new Date() }));

/* ─── START ─── */
app.listen(PORT, () => {
    console.log(`\n🚀 LAB NUTRITION Backend running on port ${PORT}`);
    console.log(`📊 Admin dashboard: http://localhost:${PORT}/admin`);
    console.log(`🔗 Shopify store: ${process.env.SHOPIFY_SHOP}\n`);
});

module.exports = app;
