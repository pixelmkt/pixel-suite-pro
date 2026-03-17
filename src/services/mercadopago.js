require('dotenv').config();
const { MercadoPagoConfig, PreApproval, PreApprovalPlan, Payment } = require('mercadopago');

/**
 * Returns a fresh MercadoPagoConfig with the current token.
 * This allows the token to be updated at runtime (via /api/settings)
 * without requiring a server restart.
 */
function getMP() {
    const token = process.env.MP_ACCESS_TOKEN;
    if (!token) throw new Error('MP_ACCESS_TOKEN not configured. Set it in Ajustes.');
    return new MercadoPagoConfig({ accessToken: token, options: { timeout: 15000 } });
}

/* ─── VERIFY CONNECTION ─── */
async function verifyConnection() {
    const mp = getMP();
    // Simple call to MP API to validate the token — list preapproval plans
    const plan = new PreApprovalPlan(mp);
    const res = await plan.search({ options: { limit: 1 } });
    return { ok: true, token_prefix: (process.env.MP_ACCESS_TOKEN || '').substring(0, 12) + '...' };
}

/* ─── PLANS ─── */
async function createPlan({ frequency, permanence, amount, productTitle }) {
    const mp = getMP();
    const plan = new PreApprovalPlan(mp);
    const cycles = Math.ceil(permanence / frequency);
    return plan.create({
        body: {
            reason: `LAB NUTRITION — ${productTitle} (${frequency === 1 ? 'Mensual' : 'Bimestral'} x ${permanence} meses)`,
            auto_recurring: {
                frequency,
                frequency_type: 'months',
                transaction_amount: amount,
                currency_id: 'PEN',
                repetitions: cycles,
                free_trial: null
            },
            back_url: `${process.env.BACKEND_URL || 'https://pixel-suite-pro-production.up.railway.app'}/subscriptions/success`,
            payment_methods_allowed: {
                payment_types: [{ id: 'credit_card' }, { id: 'debit_card' }]
            }
        }
    });
}

/* ─── SUBSCRIPTIONS ─── */
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
    createSubscription,
    getSubscription,
    pauseSubscription,
    resumeSubscription,
    cancelSubscription,
    getPayment,
    get MP_PUBLIC_KEY() { return process.env.MP_PUBLIC_KEY; }
};
