require('dotenv').config();
const { MercadoPagoConfig, PreApproval, PreApprovalPlan, Payment } = require('mercadopago');

/**
 * Returns a fresh MercadoPagoConfig with the current token.
 * Allows token to be updated at runtime without server restart.
 */
function getMP() {
    const token = process.env.MP_ACCESS_TOKEN;
    if (!token) throw new Error('MP_ACCESS_TOKEN not configured. Set it in Ajustes.');
    return new MercadoPagoConfig({ accessToken: token, options: { timeout: 15000 } });
}

const BACKEND_URL = () => process.env.BACKEND_URL || 'https://pixel-suite-pro-production.up.railway.app';

/* ─── VERIFY CONNECTION ─── */
async function verifyConnection() {
    const mp = getMP();
    const plan = new PreApprovalPlan(mp);
    await plan.search({ options: { limit: 1 } });
    return { ok: true, token_prefix: (process.env.MP_ACCESS_TOKEN || '').substring(0, 12) + '...' };
}

/* ─── CREATE PREAPPROVAL PLAN (template) ─── */
async function createPlan({ frequency, permanence, amount, productTitle }) {
    const mp = getMP();
    const plan = new PreApprovalPlan(mp);
    const cycles = Math.ceil(permanence / frequency);
    const reason = `LAB NUTRITION — ${productTitle} (${frequency === 1 ? 'Mensual' : `Cada ${frequency} meses`} × ${permanence} meses)`;
    return plan.create({
        body: {
            reason,
            auto_recurring: {
                frequency,
                frequency_type: 'months',
                transaction_amount: parseFloat(amount.toFixed(2)),
                currency_id: 'PEN',
                repetitions: cycles,
                free_trial: null
            },
            back_url: `${BACKEND_URL()}/subscriptions/success`,
            payment_methods_allowed: {
                payment_types: [{ id: 'credit_card' }, { id: 'debit_card' }]
            }
        }
    });
}

/**
 * ──────────────────────────────────────────────────────────────────────
 * createCheckout — The CORRECT MP PreApproval flow for recurring billing
 * ──────────────────────────────────────────────────────────────────────
 * Flow:
 *   1. Creates PreApprovalPlan (template with frequency + amount)
 *   2. Creates PreApproval linked to the plan WITHOUT card token
 *      → MP generates init_point URL
 *   3. Customer goes to init_point, enters card, authorizes subscription
 *   4. MP charges first payment immediately
 *   5. MP sends webhook to /webhooks/mp for each subsequent payment
 *   6. Backend creates Shopify order for each MP payment
 *
 * Returns: { init_point, subscription_id, plan_id }
 */
async function createCheckout({ frequency, permanence, amount, productTitle, customerEmail, backUrl }) {
    const mp = getMP();
    const planApi = new PreApprovalPlan(mp);
    const subApi = new PreApproval(mp);
    const cycles = Math.ceil(permanence / frequency);
    const reason = `LAB NUTRITION — ${productTitle} (${frequency === 1 ? 'Mensual' : `Cada ${frequency} meses`} × ${permanence} meses)`;
    const back = backUrl || `${BACKEND_URL()}/subscriptions/success`;

    // 1. Create PreApprovalPlan (defines billing terms)
    const plan = await planApi.create({
        body: {
            reason,
            auto_recurring: {
                frequency,
                frequency_type: 'months',
                transaction_amount: parseFloat(parseFloat(amount).toFixed(2)),
                currency_id: 'PEN',
                repetitions: cycles
            },
            back_url: back,
            payment_methods_allowed: {
                payment_types: [{ id: 'credit_card' }, { id: 'debit_card' }]
            }
        }
    });

    if (!plan || !plan.id) throw new Error('MP: no se pudo crear el plan de suscripción');

    // 2. Create PreApproval (subscription instance) — NO card token needed
    // MP returns init_point where customer enters card and authorizes
    const sub = await subApi.create({
        body: {
            preapproval_plan_id: plan.id,
            payer_email: customerEmail,
            back_url: back,
            reason,
            status: 'pending'  // customer activates via init_point
        }
    });

    if (!sub || !sub.init_point) throw new Error('MP: no se pudo obtener el link de pago');

    return {
        plan_id: plan.id,
        subscription_id: sub.id,
        init_point: sub.init_point  // redirect customer here
    };
}

/* ─── GET / UPDATE SUBSCRIPTIONS ─── */
async function createSubscription({ planId, customerEmail, customerName, cardToken }) {
    // Legacy function kept for compatibility. Use createCheckout() for new flows.
    const mp = getMP();
    const sub = new PreApproval(mp);
    return sub.create({
        body: {
            preapproval_plan_id: planId,
            payer_email: customerEmail,
            card_token_id: cardToken,
            status: 'authorized'
        }
    });
}

async function getSubscription(preapprovalId) {
    const mp = getMP();
    const sub = new PreApproval(mp);
    return sub.get({ id: preapprovalId });
}

async function pauseSubscription(preapprovalId) {
    const mp = getMP();
    const sub = new PreApproval(mp);
    return sub.update({ id: preapprovalId, body: { status: 'paused' } });
}

async function resumeSubscription(preapprovalId) {
    const mp = getMP();
    const sub = new PreApproval(mp);
    return sub.update({ id: preapprovalId, body: { status: 'authorized' } });
}

async function cancelSubscription(preapprovalId) {
    const mp = getMP();
    const sub = new PreApproval(mp);
    return sub.update({ id: preapprovalId, body: { status: 'cancelled' } });
}

async function getPayment(paymentId) {
    const mp = getMP();
    const payment = new Payment(mp);
    return payment.get({ id: paymentId });
}

module.exports = {
    verifyConnection,
    createPlan,
    createCheckout,   // ← USE THIS for new subscription checkouts
    createSubscription,
    getSubscription,
    pauseSubscription,
    resumeSubscription,
    cancelSubscription,
    getPayment,
    get MP_PUBLIC_KEY() { return process.env.MP_PUBLIC_KEY; }
};
