/**
 * ╔══════════════════════════════════════════════════════════╗
 * ║  SHOPIFY METAOBJECTS — Native Database Layer             ║
 * ║  Lab Nutrition Subscriptions App                         ║
 * ║                                                          ║
 * ║  Everything lives in Shopify. No Supabase, no SQL.       ║
 * ║  Uses: Admin GraphQL API 2025-01 + Metaobjects           ║
 * ║                                                          ║
 * ║  Types:                                                  ║
 * ║    lab_subscription  — each subscription record          ║
 * ║    lab_sub_event     — audit log (paused, paid, etc.)    ║
 * ╚══════════════════════════════════════════════════════════╝
 */

const SHOPIFY_API_VERSION = '2026-01';

const SUBSCRIPTION_TYPE = 'lab_subscription';
const EVENT_TYPE = 'lab_sub_event';

// ── GraphQL client ────────────────────────────────────────────
async function gql(query, variables = {}) {
    const shop = process.env.SHOPIFY_SHOP || 'nutrition-lab-cluster.myshopify.com';
    const token = process.env.SHOPIFY_ACCESS_TOKEN;
    if (!token) throw new Error('SHOPIFY_ACCESS_TOKEN not set');

    const res = await fetch(
        `https://${shop}/admin/api/${SHOPIFY_API_VERSION}/graphql.json`,
        {
            method: 'POST',
            headers: {
                'X-Shopify-Access-Token': token,
                'Content-Type': 'application/json',
            },
            body: JSON.stringify({ query, variables }),
        }
    );
    const json = await res.json();
    if (json.errors?.length) throw new Error(json.errors[0].message);
    return json.data;
}

// ── Helpers ───────────────────────────────────────────────────
function parseNode(node) {
    const field = node.fields.find(f => f.key === 'data');
    if (!field?.value) return null;
    try {
        const obj = JSON.parse(field.value);
        return { ...obj, _gid: node.id, id: obj.id || node.id };
    } catch { return null; }
}

function uid(prefix = 'id') {
    return `${prefix}_${Date.now()}_${Math.random().toString(36).slice(2, 8)}`;
}

// ── Setup: create Metaobject type definitions ─────────────────
async function initializeTypes() {
    const CREATE_TYPE = `
        mutation CreateType($def: MetaobjectDefinitionCreateInput!) {
            metaobjectDefinitionCreate(definition: $def) {
                metaobjectDefinition { id type name }
                userErrors { field message }
            }
        }
    `;
    for (const [type, name] of [
        [SUBSCRIPTION_TYPE, 'LAB Subscription'],
        [EVENT_TYPE, 'LAB Sub Event'],
    ]) {
        try {
            const r = await gql(CREATE_TYPE, {
                def: {
                    type,
                    name,
                    fieldDefinitions: [
                        { key: 'data', name: 'Payload', type: 'json' },
                    ],
                },
            });
            const errs = r.metaobjectDefinitionCreate?.userErrors || [];
            if (errs.length && !errs[0].message.includes('already')) {
                console.warn(`[SHOPIFY_DB] Type ${type} error:`, errs);
            } else {
                console.log(`[SHOPIFY_DB] Type ready: ${type}`);
            }
        } catch (e) {
            console.warn(`[SHOPIFY_DB] initType ${type}:`, e.message);
        }
    }
}

// ── SUBSCRIPTIONS CRUD ────────────────────────────────────────

const Q_LIST = `
    query ListMetaobjects($type: String!, $after: String) {
        metaobjects(type: $type, first: 250, after: $after) {
            nodes { id fields { key value } }
            pageInfo { hasNextPage endCursor }
        }
    }
`;

async function _listAll(type) {
    let items = [], cursor = null, hasMore = true;
    while (hasMore) {
        const data = await gql(Q_LIST, { type, after: cursor });
        const page = data.metaobjects;
        items.push(...(page.nodes || []).map(parseNode).filter(Boolean));
        hasMore = page.pageInfo.hasNextPage;
        cursor = page.pageInfo.endCursor;
    }
    return items;
}

async function getSubscriptions(filters = {}) {
    let subs = await _listAll(SUBSCRIPTION_TYPE);
    if (filters.status) subs = subs.filter(s => s.status === filters.status);
    if (filters.customer_email) subs = subs.filter(s =>
        s.customer_email?.toLowerCase() === filters.customer_email.toLowerCase()
    );
    if (filters.next_charge_before) {
        const limit = new Date(filters.next_charge_before);
        subs = subs.filter(s => s.next_charge_at && new Date(s.next_charge_at) <= limit);
    }
    return subs;
}

async function getSubscription(id) {
    const all = await _listAll(SUBSCRIPTION_TYPE);
    return all.find(s => s.id === id || s._gid === id || s._gid?.includes(id)) || null;
}

const M_CREATE = `
    mutation CreateMetaobject($input: MetaobjectCreateInput!) {
        metaobjectCreate(metaobject: $input) {
            metaobject { id handle }
            userErrors { field message }
        }
    }
`;

async function createSubscription(data) {
    const id = uid('sub');
    const now = new Date().toISOString();
    const record = { ...data, id, created_at: now, updated_at: now };
    const r = await gql(M_CREATE, {
        input: {
            type: SUBSCRIPTION_TYPE,
            handle: id,
            fields: [{ key: 'data', value: JSON.stringify(record) }],
        },
    });
    const errs = r.metaobjectCreate?.userErrors || [];
    if (errs.length) throw new Error(errs[0].message);
    return { ...record, _gid: r.metaobjectCreate.metaobject.id };
}

const M_UPDATE = `
    mutation UpdateMetaobject($id: ID!, $input: MetaobjectUpdateInput!) {
        metaobjectUpdate(id: $id, metaobject: $input) {
            metaobject { id }
            userErrors { field message }
        }
    }
`;

async function updateSubscription(id, updates) {
    const sub = await getSubscription(id);
    if (!sub) throw new Error(`Subscription not found: ${id}`);
    const updated = { ...sub, ...updates, updated_at: new Date().toISOString() };
    delete updated._gid; // don't store the GID in payload
    const r = await gql(M_UPDATE, {
        id: sub._gid,
        input: { fields: [{ key: 'data', value: JSON.stringify(updated) }] },
    });
    const errs = r.metaobjectUpdate?.userErrors || [];
    if (errs.length) throw new Error(errs[0].message);
    return { ...updated, _gid: sub._gid };
}

// ── EVENTS ────────────────────────────────────────────────────

async function createEvent(data) {
    const id = uid('ev');
    const record = { ...data, id, created_at: new Date().toISOString() };
    await gql(M_CREATE, {
        input: {
            type: EVENT_TYPE,
            handle: id,
            fields: [{ key: 'data', value: JSON.stringify(record) }],
        },
    });
    return record;
}

async function getEvents(subscriptionId, limit = 50) {
    const all = await _listAll(EVENT_TYPE);
    return all
        .filter(e => e.subscription_id === subscriptionId)
        .sort((a, b) => new Date(b.created_at) - new Date(a.created_at))
        .slice(0, limit);
}

// ── METRICS ───────────────────────────────────────────────────

async function getMetrics() {
    const subs = await getSubscriptions();
    const active = subs.filter(s => s.status === 'active');
    const mrr = active.reduce((n, s) => n + parseFloat(s.final_price || 0), 0);
    const now = new Date();
    const in7d = new Date(); in7d.setDate(in7d.getDate() + 7);

    return {
        total: subs.length,
        active: active.length,
        paused: subs.filter(s => s.status === 'paused').length,
        cancelled: subs.filter(s => s.status === 'cancelled').length,
        mrr: parseFloat(mrr.toFixed(2)),
        next7d: subs.filter(s => {
            if (!s.next_charge_at) return false;
            const d = new Date(s.next_charge_at);
            return d >= now && d <= in7d;
        }).length,
        avg_discount: active.length
            ? (active.reduce((n, s) => n + (s.discount_pct || 0), 0) / active.length).toFixed(1)
            : 0,
    };
}

// ── CRON HELPERS ──────────────────────────────────────────────

/** Get active subscriptions where next_charge is in N days */
async function getChargesComingInDays(days) {
    const target = new Date();
    target.setDate(target.getDate() + days);
    const from = new Date(target); from.setHours(0, 0, 0, 0);
    const to = new Date(target); to.setHours(23, 59, 59, 999);
    const subs = await getSubscriptions({ status: 'active' });
    return subs.filter(s => {
        if (!s.next_charge_at) return false;
        const d = new Date(s.next_charge_at);
        return d >= from && d <= to;
    });
}

module.exports = {
    gql,
    initializeTypes,
    getSubscriptions,
    getSubscription,
    createSubscription,
    updateSubscription,
    createEvent,
    getEvents,
    getMetrics,
    getChargesComingInDays,
};
