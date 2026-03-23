/**
 * SubscriptionContract service — Shopify GraphQL Admin API 2026-01
 * Manages native Shopify subscription contracts.
 * Handles: create, update, billing attempts, webhook processing.
 * Scales to unlimited subscribers via Shopify's native infrastructure.
 */

const { gql } = require('./shopify-storage');

/* ── Create a SubscriptionContract in Shopify ── */
async function createContract({ customerId, customerEmail, sellingPlanId, variantId, linePrice, currencyCode, shipAddress, intervalCount }) {
    const now = new Date();
    const nextBilling = new Date(now);
    nextBilling.setMonth(nextBilling.getMonth() + (intervalCount || 1));

    const draftMutation = `
        mutation CreateContract($input: SubscriptionContractCreateInput!) {
            subscriptionContractCreate(input: $input) {
                draft { id }
                userErrors { field message code }
            }
        }
    `;

    const input = {
        customerId,
        nextBillingDate: nextBilling.toISOString(),
        contract: {
            status: 'ACTIVE',
            deliveryPolicy: { interval: 'MONTH', intervalCount: intervalCount || 1 },
            billingPolicy: {
                interval: 'MONTH',
                intervalCount: intervalCount || 1,
                minCycles: 1,
                maxCycles: null,
                anchors: []
            },
            currencyCode: currencyCode || 'PEN',
            ...(shipAddress ? {
                deliveryAddress: {
                    firstName: shipAddress.first_name || '',
                    lastName: shipAddress.last_name || '',
                    address1: shipAddress.address1 || '',
                    address2: shipAddress.address2 || '',
                    city: shipAddress.city || '',
                    province: shipAddress.province || '',
                    country: shipAddress.country || 'PE',
                    zip: shipAddress.zip || '',
                    phone: shipAddress.phone || ''
                }
            } : {})
        }
    };

    const draftData = await gql(draftMutation, { input });
    const draftResult = draftData.subscriptionContractCreate;
    const errs = draftResult.userErrors || [];
    if (errs.length) {
        console.warn('[CONTRACT] Draft errors:', errs);
        return null;
    }
    const draftId = draftResult.draft.id;

    // Add line item to draft
    await gql(`
        mutation AddLine($draftId: ID!, $input: SubscriptionLineInput!) {
            subscriptionDraftLineAdd(draftId: $draftId, input: $input) {
                draft { id }
                userErrors { field message }
            }
        }
    `, {
        draftId,
        input: {
            productVariantId: variantId,
            quantity: 1,
            currentPrice: String(parseFloat(linePrice).toFixed(2)),
            sellingPlanId: sellingPlanId || null
        }
    }).catch(e => console.warn('[CONTRACT] AddLine error:', e.message));

    // Commit the draft
    const commitData = await gql(`
        mutation CommitDraft($draftId: ID!) {
            subscriptionDraftCommit(draftId: $draftId) {
                contract { id status nextBillingDate }
                userErrors { field message }
            }
        }
    `, { draftId });

    const commitResult = commitData.subscriptionDraftCommit;
    if ((commitResult.userErrors || []).length) {
        console.warn('[CONTRACT] Commit errors:', commitResult.userErrors);
        return null;
    }
    console.log(`[CONTRACT] Created contract ${commitResult.contract.id} for customer ${customerId}`);
    return commitResult.contract;
}

/* ── Update contract status ── */
async function updateContractStatus(contractGid, status) {
    const mutation = `
        mutation UpdateContract($contractId: ID!, $input: SubscriptionContractUpdateInput!) {
            subscriptionContractUpdate(contractId: $contractId, input: $input) {
                draft { id }
                userErrors { field message }
            }
        }
    `;
    const data = await gql(mutation, { contractId: contractGid, input: { status } });
    const result = data.subscriptionContractUpdate;
    const errs = result.userErrors || [];
    if (errs.length) throw new Error(errs.map(e => e.message).join('; '));
    if (result.draft && result.draft.id) {
        await gql(`
            mutation Commit($id: ID!) { subscriptionDraftCommit(draftId: $id) { contract { id } userErrors { message } } }
        `, { id: result.draft.id }).catch(() => {});
    }
    return true;
}

/* ── List contracts for a specific customer ── */
async function getCustomerContracts(customerGid) {
    const query = `
        query CustomerContracts($id: ID!) {
            customer(id: $id) {
                subscriptionContracts(first: 20) {
                    nodes {
                        id status nextBillingDate createdAt
                        lines(first: 5) {
                            nodes {
                                id title quantity
                                currentPrice { amount currencyCode }
                                sellingPlan { id name }
                            }
                        }
                        deliveryPolicy { interval intervalCount }
                        billingPolicy { interval intervalCount }
                    }
                }
            }
        }
    `;
    const data = await gql(query, { id: customerGid });
    return data.customer?.subscriptionContracts?.nodes ?? [];
}

/* ── Get ALL active contracts due for billing (nextBillingDate <= today) ── */
/* Used by the daily billing cron to trigger charges automatically */
async function getContractsDueForBilling(cursor) {
    // Query active contracts — paginate 100 at a time for scale
    const query = `
        query DueContracts($after: String) {
            subscriptionContracts(first: 100, after: $after, query: "status:ACTIVE") {
                pageInfo { hasNextPage endCursor }
                nodes {
                    id
                    status
                    nextBillingDate
                    customer { id email firstName lastName defaultAddress { address1 city zip } }
                    lines(first: 3) {
                        nodes {
                            title quantity
                            currentPrice { amount currencyCode }
                        }
                    }
                    deliveryPolicy { interval intervalCount }
                }
            }
        }
    `;
    const now = new Date();
    const data = await gql(query, { after: cursor || null });
    const contracts = data.subscriptionContracts;

    // Filter: only those with nextBillingDate <= now
    const due = (contracts?.nodes ?? []).filter(c => {
        if (!c.nextBillingDate) return false;
        return new Date(c.nextBillingDate) <= now;
    });

    return {
        due,
        hasNextPage: contracts?.pageInfo?.hasNextPage ?? false,
        endCursor: contracts?.pageInfo?.endCursor ?? null
    };
}

/* ── Trigger a billing attempt for a contract → Shopify charges + creates order ── */
async function createBillingAttempt(contractGid, idempotencyKey) {
    const mutation = `
        mutation BillingAttempt($subscriptionContractId: ID!, $subscriptionBillingAttemptInput: SubscriptionBillingAttemptInput!) {
            subscriptionBillingAttemptCreate(
                subscriptionContractId: $subscriptionContractId
                subscriptionBillingAttemptInput: $subscriptionBillingAttemptInput
            ) {
                subscriptionBillingAttempt {
                    id
                    ready
                    errorMessage
                    errorCode
                    order { id name confirmationNumber }
                }
                userErrors { field message code }
            }
        }
    `;
    const data = await gql(mutation, {
        subscriptionContractId: contractGid,
        subscriptionBillingAttemptInput: {
            idempotencyKey: idempotencyKey || `${contractGid}-${Date.now()}`,
            originTime: new Date().toISOString()
        }
    });
    const result = data.subscriptionBillingAttemptCreate;
    const errs = result.userErrors || [];
    if (errs.length) {
        console.warn('[BILLING ATTEMPT] Errors:', errs);
    }
    const attempt = result.subscriptionBillingAttempt;
    if (attempt) {
        console.log(`[BILLING ATTEMPT] Contract ${contractGid}: ready=${attempt.ready}, order=${attempt.order?.name}, error=${attempt.errorMessage}`);
    }
    return attempt;
}

/* ── Register Shopify webhooks for subscription events ── */
async function registerSubscriptionWebhooks(backendUrl) {
    const topics = [
        'subscription_billing_attempts/success',
        'subscription_billing_attempts/failure',
        'subscription_billing_attempts/challenged',
        'subscription_contracts/create',
        'subscription_contracts/update',
        'subscription_contracts/activate',
        'subscription_contracts/pause',
        'subscription_contracts/cancel',
        'subscription_contracts/expire'
    ];

    const existingQuery = `{ webhookSubscriptions(first: 50) { nodes { id topic endpoint { ... on WebhookHttpEndpoint { callbackUrl } } } } }`;
    const existing = await gql(existingQuery).catch(() => ({ webhookSubscriptions: { nodes: [] } }));
    const existingTopics = new Set((existing.webhookSubscriptions?.nodes || []).map(w => w.topic));

    const results = [];
    for (const topic of topics) {
        if (existingTopics.has(topic)) {
            results.push({ topic, status: 'already_registered' });
            continue;
        }
        const mutation = `
            mutation RegisterWebhook($topic: WebhookSubscriptionTopic!, $url: URL!) {
                webhookSubscriptionCreate(topic: $topic, webhookSubscription: { callbackUrl: $url, format: JSON }) {
                    webhookSubscription { id }
                    userErrors { field message }
                }
            }
        `;
        const callbackUrl = `${backendUrl}/webhooks/shopify/${topic.replace('/', '_')}`;
        const res = await gql(mutation, { topic, url: callbackUrl }).catch(e => ({ error: e.message }));
        const created = res.webhookSubscriptionCreate;
        if (created?.userErrors?.length) {
            results.push({ topic, status: 'error', error: created.userErrors[0].message });
        } else {
            results.push({ topic, status: 'registered', id: created?.webhookSubscription?.id });
        }
    }
    console.log('[WEBHOOKS] Registration results:', results);
    return results;
}

module.exports = {
    createContract,
    updateContractStatus,
    getCustomerContracts,
    getContractsDueForBilling,
    createBillingAttempt,
    registerSubscriptionWebhooks
};
