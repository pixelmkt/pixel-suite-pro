require('dotenv').config();
const axios = require('axios');

/**
 * Shopify REST API service — dynamic token (reads process.env on each request)
 * API version: 2026-01 (current stable as of March 2026)
 *
 * 2026-06-12: eliminado createSubscriptionOrder — era código MUERTO (cero call sites;
 * prometía tags 'subscription,lab-recurrente' que jamás se aplicaban). El creador real
 * de órdenes es createShopifyOrderFromSub en server.js (tag 'suscripcion').
 */
function getClient() {
    const shop = process.env.SHOPIFY_SHOP || 'nutrition-lab-cluster.myshopify.com';
    const token = process.env.SHOPIFY_ACCESS_TOKEN;
    const ver = '2026-01';
    return axios.create({
        baseURL: `https://${shop}/admin/api/${ver}`,
        headers: { 'X-Shopify-Access-Token': token, 'Content-Type': 'application/json' }
    });
}

/* Tag customer as subscriber in Shopify */
async function tagCustomerAsSubscriber(customerId, add = true) {
    const client = getClient();
    // 🔧 2026-06-12: si llega un EMAIL en vez del id numérico (fallback legacy
    // customer_id=email), resolverlo primero — antes el GET /customers/<email>.json
    // daba 404 y el tag nunca se aplicaba.
    if (String(customerId).includes('@')) {
        const c = await getCustomer(String(customerId)).catch(() => null);
        if (!c?.id) throw new Error(`tagCustomerAsSubscriber: no existe customer con email ${customerId}`);
        customerId = c.id;
    }
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
    const client = getClient();
    const res = await client.get(`/customers/search.json?query=email:${email}`);
    return res.data.customers?.[0] || null;
}

module.exports = { tagCustomerAsSubscriber, getCustomer };
