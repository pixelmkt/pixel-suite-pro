require('dotenv').config();
const { MercadoPagoConfig, PreApproval, PreApprovalPlan, Payment } = require('mercadopago');

const mp = new MercadoPagoConfig({
    accessToken: process.env.MP_ACCESS_TOKEN,
    options: { timeout: 10000 }
});

/* ─── PLANS ─── */
async function createPlan({ frequency, permanence, amount, productTitle }) {
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
            back_url: `${process.env.BACKEND_URL}/subscriptions/success`,
            payment_methods_allowed: {
                payment_types: [{ id: 'credit_card' }, { id: 'debit_card' }]
            }
        }
    });
}

/* ─── SUBSCRIPTIONS ─── */
async function createSubscription({ planId, customerEmail, customerName, cardToken }) {
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
    const sub = new PreApproval(mp);
    return sub.get({ id: preapprovalId });
}

async function pauseSubscription(preapprovalId) {
    const sub = new PreApproval(mp);
    return sub.update({ id: preapprovalId, body: { status: 'paused' } });
}

async function resumeSubscription(preapprovalId) {
    const sub = new PreApproval(mp);
    return sub.update({ id: preapprovalId, body: { status: 'authorized' } });
}

async function cancelSubscription(preapprovalId) {
    const sub = new PreApproval(mp);
    return sub.update({ id: preapprovalId, body: { status: 'cancelled' } });
}

async function getPayment(paymentId) {
    const payment = new Payment(mp);
    return payment.get({ id: paymentId });
}

module.exports = {
    createPlan,
    createSubscription,
    getSubscription,
    pauseSubscription,
    resumeSubscription,
    cancelSubscription,
    getPayment,
    MP_PUBLIC_KEY: process.env.MP_PUBLIC_KEY
};
