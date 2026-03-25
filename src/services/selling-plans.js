/**
 * SellingPlanGroup service — Shopify GraphQL Admin API 2026-01
 * Creates and manages native Shopify subscription plans on products.
 * Used by apps like Skio / Recharge / Bold in LATAM markets.
 */

const { gql } = require('./shopify-storage');

const FREQ_MAP = { 1: 'MONTH', 2: 'MONTH', 3: 'MONTH', 6: 'MONTH' };

/* ── Build selling plan input from a plan config object ── */
function buildPlanInput(plan) {
    const intervalCount = plan.frequency || 1;
    const maxCycles = plan.permanence ? Math.ceil(plan.permanence / intervalCount) : null;
    const discountPct = plan.discount || 0;
    const label = `${intervalCount === 1 ? 'Mensual' : `Cada ${intervalCount} meses`} · ${plan.permanence || 0} meses · ${discountPct}% OFF`;

    const sellingPlan = {
        name: label,
        options: [`${intervalCount === 1 ? 'Mensual' : `Cada ${intervalCount} meses`}`],
        position: 1,
        category: 'SUBSCRIPTION',
        billingPolicy: {
            recurring: {
                interval: FREQ_MAP[intervalCount] || 'MONTH',
                intervalCount,
                anchors: [],
                maxCycles: maxCycles || null
            }
        },
        deliveryPolicy: {
            recurring: {
                interval: FREQ_MAP[intervalCount] || 'MONTH',
                intervalCount,
                anchors: [],
                preAnchorBehavior: 'ASAP'
            }
        },
        pricingPolicies: discountPct > 0 ? [
            {
                fixed: {
                    adjustmentType: 'PERCENTAGE',
                    adjustmentValue: { percentage: discountPct }
                }
            }
        ] : [],
        inventoryPolicy: { reserve: 'ON_SALE' }
    };
    return sellingPlan;
}

/* ── Create a SellingPlanGroup on a Shopify product ── */
async function createSellingPlanGroup({ productGid, plans, productTitle }) {
    const groupName = `LAB Suscripción — ${productTitle || 'Producto'}`;

    const sellingPlansToCreate = plans
        .filter(p => p.active !== false && p.frequency && p.permanence && p.discount >= 0)
        .map(p => buildPlanInput(p));

    if (!sellingPlansToCreate.length) {
        throw new Error('No hay planes activos para crear el SellingPlanGroup');
    }

    const mutation = `
        mutation CreateSellingPlanGroup($input: SellingPlanGroupInput!, $resources: SellingPlanGroupResourceInput) {
            sellingPlanGroupCreate(input: $input, resources: $resources) {
                sellingPlanGroup {
                    id
                    name
                    sellingPlans(first: 20) {
                        nodes { id name }
                    }
                }
                userErrors { field message code }
            }
        }
    `;

    const data = await gql(mutation, {
        input: {
            name: groupName,
            merchantCode: 'lab-nutrition-sub',
            options: ['Frecuencia'],
            position: 1,
            sellingPlansToCreate
        },
        resources: {
            productIds: [productGid]
        }
    });

    const result = data.sellingPlanGroupCreate;
    const errs = result.userErrors || [];
    if (errs.length) throw new Error(errs.map(e => e.message).join('; '));
    return result.sellingPlanGroup;
}

/* ── List SellingPlanGroups for a product ── */
async function getProductSellingPlans(productGid) {
    const query = `
        query GetProductPlans($id: ID!) {
            product(id: $id) {
                id
                title
                sellingPlanGroups(first: 10) {
                    nodes {
                        id
                        name
                        sellingPlans(first: 20) {
                            nodes { id name pricingPolicies { ... on SellingPlanFixedPricingPolicy { adjustmentType adjustmentValue { ... on SellingPlanPricingPolicyPercentageValue { percentage } } } } }
                        }
                    }
                }
            }
        }
    `;
    const data = await gql(query, { id: productGid });
    return data.product;
}

/* ── Delete a SellingPlanGroup ── */
async function deleteSellingPlanGroup(groupGid) {
    const mutation = `
        mutation DeleteGroup($id: ID!) {
            sellingPlanGroupDelete(id: $id) {
                deletedSellingPlanGroupId
                userErrors { field message }
            }
        }
    `;
    const data = await gql(mutation, { id: groupGid });
    const errs = data.sellingPlanGroupDelete.userErrors || [];
    if (errs.length) throw new Error(errs.map(e => e.message).join('; '));
    return data.sellingPlanGroupDelete.deletedSellingPlanGroupId;
}

/* ── Remove a product from a SellingPlanGroup ── */
async function removeProductFromGroup(groupGid, productGid) {
    const mutation = `
        mutation RemoveProduct($id: ID!, $resources: SellingPlanGroupResourceInput!) {
            sellingPlanGroupRemoveProducts(id: $id, productIds: [$resources]) {
                removedProductIds
                userErrors { field message }
            }
        }
    `;
    // Use addProducts mutation with empty productIds to detach, or delete the group
    try {
        await deleteSellingPlanGroup(groupGid);
    } catch (e) {
        console.warn('[SELLING_PLANS] Could not delete group:', e.message);
    }
}

/* ── Sync plans for a product: delete old group if exists, create new one ── */
async function syncProductPlans({ productId, productGid, productTitle, plans }) {
    // 1. Get existing groups
    const product = await getProductSellingPlans(productGid).catch(() => null);
    const existingGroups = product && product.sellingPlanGroups ? product.sellingPlanGroups.nodes : [];

    // 2. Delete LAB groups
    for (const group of existingGroups) {
        if (group.name && group.name.includes('LAB')) {
            await deleteSellingPlanGroup(group.id).catch(() => {});
        }
    }

    // 3. Create new group with current plans
    const activePlans = (plans || []).filter(p => p.active !== false);
    if (!activePlans.length) {
        return { synced: false, reason: 'No active plans' };
    }
    const group = await createSellingPlanGroup({ productGid, plans: activePlans, productTitle });
    return { synced: true, group };
}

module.exports = { createSellingPlanGroup, getProductSellingPlans, deleteSellingPlanGroup, syncProductPlans };
