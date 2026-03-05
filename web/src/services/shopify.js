require('dotenv').config();
const axios = require('axios');

const SHOP = process.env.SHOPIFY_SHOP;
const TOKEN = process.env.SHOPIFY_ACCESS_TOKEN;
const VER = process.env.SHOPIFY_API_VERSION || '2025-01';
const BASE = `https://${SHOP}/admin/api/${VER}`;

const client = axios.create({
    baseURL: BASE,
    headers: { 'X-Shopify-Access-Token': TOKEN, 'Content-Type': 'application/json' }
});

/* Create draft order for a subscription cycle */
async function createSubscriptionOrder({ sub, cycleNumber, mpPaymentId }) {
    const unitPrice = sub.final_price;
    const note = `Suscripción LAB | Ciclo ${cycleNumber}/${sub.cycles_required} | ${sub.frequency_months === 1 ? 'Mensual' : 'Bimestral'} | ${sub.discount_pct}% OFF | MP: ${mpPaymentId}`;

    const body = {
        order: {
            email: sub.customer_email,
            note,
            tags: 'subscription,lab-recurrente',
            line_items: [{
                variant_id: parseInt(sub.variant_id),
                quantity: 1,
                price: unitPrice.toFixed(2),
                applied_discount: {
                    description: `Suscripción ${sub.discount_pct}% OFF`,
                    value_type: 'percentage',
                    value: sub.discount_pct.toString(),
                    amount: (sub.base_price - unitPrice).toFixed(2)
                }
            }],
            shipping_address: sub.shipping_address || null,
            financial_status: 'paid',
            send_receipt: false,
            send_fulfillment_receipt: true
        }
    };

    // Add gift on qualifying cycle
    if (sub.gift_cycle && cycleNumber === sub.gift_cycle && sub.gift_variant_id) {
        body.order.line_items.push({
            variant_id: parseInt(sub.gift_variant_id),
            quantity: 1,
            price: '0.00',
            title: '🎁 REGALO SUSCRIPCIÓN'
        });
    }

    const res = await client.post('/orders.json', body);
    return res.data.order;
}

/* Tag customer as subscriber in Shopify */
async function tagCustomerAsSubscriber(customerId, add = true) {
    const res = await client.get(`/customers/${customerId}.json`);
    const customer = res.data.customer;
    let tags = (customer.tags || '').split(',').map(t => t.trim()).filter(Boolean);
    if (add) {
        if (!tags.includes('suscriptor-lab')) tags.push('suscriptor-lab');
    } else {
        tags = tags.filter(t => t !== 'suscriptor-lab');
    }
    await client.put(`/customers/${customerId}.json`, { customer: { id: customerId, tags: tags.join(',') } });
}

/* Get customer info from Shopify */
async function getCustomer(email) {
    const res = await client.get(`/customers/search.json?query=email:${email}`);
    return res.data.customers?.[0] || null;
}

module.exports = { createSubscriptionOrder, tagCustomerAsSubscriber, getCustomer };
