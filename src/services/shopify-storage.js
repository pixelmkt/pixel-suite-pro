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
// 2026-04-21: tipo para configurar bundles configurables (ej: C4 Energy 15/30 latas — mix sabores)
// Cada bundle define: producto master, target_quantity, allowed_variant_ids, plans, etc.
// El sub que compra un bundle copia el mix elegido a su propio campo bundle_items y se repite cada mes.
const BUNDLE_CONFIG_TYPE = 'lab_bundle_config';

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
        [BUNDLE_CONFIG_TYPE, 'LAB Bundle Config'],
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
    // FIX 2026-04-09: previously ignored → webhook MP matcheaba sub incorrecta
    if (filters.mp_preapproval_id) subs = subs.filter(s => s.mp_preapproval_id === filters.mp_preapproval_id);
    if (filters.mp_plan_id) subs = subs.filter(s => s.mp_plan_id === filters.mp_plan_id);
    if (filters.shopify_contract_id) subs = subs.filter(s => s.shopify_contract_id === filters.shopify_contract_id);
    if (filters.shopify_order_id) subs = subs.filter(s => s.shopify_order_id === filters.shopify_order_id);
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

const M_DELETE = `
    mutation DeleteMetaobject($id: ID!) {
        metaobjectDelete(id: $id) {
            deletedId
            userErrors { field message }
        }
    }
`;

/**
 * Elimina completamente una suscripción del metaobject store.
 * Uso admin: limpieza de duplicados, pruebas, cancelaciones duras.
 * NO toca MercadoPago ni Shopify contracts — es solo el registro local.
 */
async function deleteSubscription(id) {
    const sub = await getSubscription(id);
    if (!sub) throw new Error(`Subscription not found: ${id}`);
    const r = await gql(M_DELETE, { id: sub._gid });
    const errs = r.metaobjectDelete?.userErrors || [];
    if (errs.length) throw new Error(errs[0].message);
    return { deleted: r.metaobjectDelete.deletedId, id };
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
    // "active" en status pero separamos las que YA completaron su plan (compras únicas / plan cumplido)
    const allActive = subs.filter(s => s.status === 'active');
    // Suscripciones RECURRENTES activas: tienen ciclos pendientes (cycles_completed < cycles_required)
    const active = allActive.filter(s => {
        const done = parseInt(s.cycles_completed) || 0;
        const req = parseInt(s.cycles_required) || 999;
        return done < req;
    });
    // Completadas: status=active pero ya cumplieron todos los ciclos (plan de 1 mes, etc.)
    const completed = allActive.filter(s => {
        const done = parseInt(s.cycles_completed) || 0;
        const req = parseInt(s.cycles_required) || 999;
        return done >= req && req > 0;
    });
    // MRR = solo suscripciones que VAN A generar un cobro futuro
    const mrr = active.reduce((n, s) => n + parseFloat(s.final_price || 0), 0);
    const now = new Date();
    const in7d = new Date(); in7d.setDate(in7d.getDate() + 7);

    // ADD 2026-04-21: contar suscripciones "en proceso de pago" que antes no aparecían en dashboard.
    //   pending_payment = checkout MP iniciado, aún no autorizado por el cliente
    //   pending_mp_activation = autorizado, esperando primer cobro
    //   payment_failed = intento rechazado
    //   pending_stale_2h = pending_payment >2h (cliente abandonó el checkout, probable lead perdido)
    //   pending_stale_7d = pending_payment >7d (candidato a limpieza)
    // Los nuevos campos son ADITIVOS — no modifican los existentes ni el cálculo de MRR/active/completed.
    const pending = subs.filter(s => s.status === 'pending_payment');
    const cutoff2h = now.getTime() - 2 * 60 * 60 * 1000;
    const cutoff7d = now.getTime() - 7 * 24 * 60 * 60 * 1000;
    const pendingStale2h = pending.filter(s => s.created_at && new Date(s.created_at).getTime() <= cutoff2h);
    const pendingStale7d = pending.filter(s => s.created_at && new Date(s.created_at).getTime() <= cutoff7d);

    return {
        total: subs.length,
        active: active.length,
        completed: completed.length,
        paused: subs.filter(s => s.status === 'paused').length,
        cancelled: subs.filter(s => s.status === 'cancelled').length,
        pending: pending.length,
        pending_mp: subs.filter(s => s.status === 'pending_mp_activation').length,
        pending_failed: subs.filter(s => s.status === 'payment_failed').length,
        pending_stale_2h: pendingStale2h.length,
        pending_stale_7d: pendingStale7d.length,
        mrr: parseFloat(mrr.toFixed(2)),
        next7d: subs.filter(s => {
            if (!s.next_charge_at || s.status !== 'active') return false;
            const done = parseInt(s.cycles_completed) || 0;
            const req = parseInt(s.cycles_required) || 999;
            if (done >= req) return false; // ya completó, no habrá cobro
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

// ── BUNDLE CONFIGS CRUD ───────────────────────────────────────
// 2026-04-21 — Aditivo. No afecta subs existentes ni motor de cobros.
// Un BundleConfig define un tipo de pack configurable (ej: "C4 Energy Bundle 15 latas").
// El admin puede crear N bundles; cada bundle apunta a un producto Shopify master y
// lista las variantes (sabores) elegibles, junto con los planes (freq + perm + precio).
// Al crear sub desde el widget, el cliente escoge mix de sabores que suma target_quantity.

async function getBundleConfigs(filters = {}) {
    let bundles = await _listAll(BUNDLE_CONFIG_TYPE);
    if (filters.active !== undefined) bundles = bundles.filter(b => !!b.active === !!filters.active);
    if (filters.source_product_id) bundles = bundles.filter(b => String(b.source_product_id) === String(filters.source_product_id));
    if (filters.bundle_product_id) bundles = bundles.filter(b => String(b.bundle_product_id) === String(filters.bundle_product_id));
    return bundles.sort((a, b) => new Date(b.created_at || 0) - new Date(a.created_at || 0));
}

async function getBundleConfig(id) {
    const all = await _listAll(BUNDLE_CONFIG_TYPE);
    return all.find(b => b.id === id || b._gid === id || b._gid?.includes(id)) || null;
}

async function getBundleConfigByBundleProductId(bundleProductId) {
    if (!bundleProductId) return null;
    const all = await _listAll(BUNDLE_CONFIG_TYPE);
    return all.find(b => String(b.bundle_product_id) === String(bundleProductId)) || null;
}

async function createBundleConfig(data) {
    const id = uid('bdl');
    const now = new Date().toISOString();
    const record = {
        active: true,
        ...data,
        id,
        created_at: now,
        updated_at: now,
    };
    const r = await gql(M_CREATE, {
        input: {
            type: BUNDLE_CONFIG_TYPE,
            handle: id,
            fields: [{ key: 'data', value: JSON.stringify(record) }],
        },
    });
    const errs = r.metaobjectCreate?.userErrors || [];
    if (errs.length) throw new Error(errs[0].message);
    return { ...record, _gid: r.metaobjectCreate.metaobject.id };
}

async function updateBundleConfig(id, updates) {
    const bundle = await getBundleConfig(id);
    if (!bundle) throw new Error(`BundleConfig not found: ${id}`);
    const updated = { ...bundle, ...updates, updated_at: new Date().toISOString() };
    delete updated._gid;
    const r = await gql(M_UPDATE, {
        id: bundle._gid,
        input: { fields: [{ key: 'data', value: JSON.stringify(updated) }] },
    });
    const errs = r.metaobjectUpdate?.userErrors || [];
    if (errs.length) throw new Error(errs[0].message);
    return { ...updated, _gid: bundle._gid };
}

async function deleteBundleConfig(id) {
    const bundle = await getBundleConfig(id);
    if (!bundle) throw new Error(`BundleConfig not found: ${id}`);
    const r = await gql(M_DELETE, { id: bundle._gid });
    const errs = r.metaobjectDelete?.userErrors || [];
    if (errs.length) throw new Error(errs[0].message);
    return { deleted: r.metaobjectDelete.deletedId, id };
}

module.exports = {
    gql,
    initializeTypes,
    getSubscriptions,
    getSubscription,
    createSubscription,
    updateSubscription,
    deleteSubscription,
    createEvent,
    getEvents,
    getMetrics,
    getChargesComingInDays,
    // 2026-04-21 — Bundle configs (aditivo)
    getBundleConfigs,
    getBundleConfig,
    getBundleConfigByBundleProductId,
    createBundleConfig,
    updateBundleConfig,
    deleteBundleConfig,
};
