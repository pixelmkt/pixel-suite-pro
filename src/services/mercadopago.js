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
                transaction_amount: parseFloat(Number(amount).toFixed(2)),
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
 * createCheckout — MP PreApproval flow via plan.init_point
 *
 * CORRECT FLOW (no card_token_id required):
 *   1. Creates PreApprovalPlan → gets plan.init_point
 *   2. Redirects customer to plan.init_point (MP's own checkout page)
 *   3. Customer enters card on MP's page and authorizes
 *   4. MP creates the subscription + fires webhook automatically
 *   5. Webhook /webhooks/mp → backend creates Shopify order
 */
async function createCheckout({ frequency, permanence, amount, productTitle, customerEmail, backUrl }) {
    const mp = getMP();
    const planApi = new PreApprovalPlan(mp);
    const cycles = Math.ceil(permanence / frequency);
    const reason = `LAB NUTRITION — ${productTitle} (${frequency === 1 ? 'Mensual' : `Cada ${frequency} meses`} × ${permanence} meses)`;
    const back = backUrl || `${BACKEND_URL()}/subscriptions/success`;

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

    const init_point = plan.init_point || plan.sandbox_init_point;
    if (!init_point) throw new Error('MP: el plan no devolvió URL de checkout');

    return {
        plan_id: plan.id,
        subscription_id: null,
        init_point
    };
}

/* ─── GET / UPDATE SUBSCRIPTIONS ─── */
async function createSubscription({ planId, customerEmail, customerName, cardToken }) {
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
    createCheckout,
    createSubscription,
    getSubscription,
    pauseSubscription,
    resumeSubscription,
    cancelSubscription,
    getPayment,
    get MP_PUBLIC_KEY() { return process.env.MP_PUBLIC_KEY; }
};
