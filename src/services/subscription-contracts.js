/**
 * SubscriptionContract service — Shopify GraphQL Admin API 2026-01
 * Creates and manages native Shopify subscription contracts.
 * Contracts appear in Shopify admin (/admin/subscriptions) and customer account (/account/subscriptions).
 */

const { gql } = require('./shopify-storage');

/* ── Create a SubscriptionContract in Shopify ── */
async function createContract({ customerId, customerEmail, sellingPlanId, variantId, linePrice, currencyCode, shipAddress, intervalCount }) {
    const now = new Date();

    // Calculate next billing date
    const nextBilling = new Date(now);
    nextBilling.setMonth(nextBilling.getMonth() + (intervalCount || 1));

    const draftMutation = `
        mutation CreateContract($input: SubscriptionContractCreateInput!) {
            subscriptionContractCreate(input: $input) {
                draft {
                    id
                }
                userErrors { field message code }
            }
        }
    `;

    const input = {
        customerId,
        nextBillingDate: nextBilling.toISOString(),
        contract: {
            status: 'ACTIVE',
            paymentInstrument: { paymentMethodId: null },
            deliveryPolicy: {
                interval: 'MONTH',
                intervalCount: intervalCount || 1
            },
            billingPolicy: {
                interval: 'MONTH',
                intervalCount: intervalCount || 1,
                minCycles: 1,
                maxCycles: null
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
        // Non-fatal: we can continue without a contract if Shopify rejects it
        return null;
    }
    const draftId = draftResult.draft.id;

    // Add line item to draft
    const addLineMutation = `
        mutation AddLine($draftId: ID!, $input: SubscriptionLineInput!) {
            subscriptionDraftLineAdd(draftId: $draftId, input: $input) {
                draft { id }
                lineAdded { id }
                userErrors { field message }
            }
        }
    `;
    await gql(addLineMutation, {
        draftId,
        input: {
            productVariantId: variantId,
            quantity: 1,
            currentPrice: String(parseFloat(linePrice).toFixed(2)),
            sellingPlanId: sellingPlanId || null
        }
    }).catch(e => console.warn('[CONTRACT] AddLine error:', e.message));

    // Commit the draft
    const commitMutation = `
        mutation CommitDraft($draftId: ID!) {
            subscriptionDraftCommit(draftId: $draftId) {
                contract { id status }
                userErrors { field message }
            }
        }
    `;
    const commitData = await gql(commitMutation, { draftId });
    const commitResult = commitData.subscriptionDraftCommit;
    const commitErrs = commitResult.userErrors || [];
    if (commitErrs.length) {
        console.warn('[CONTRACT] Commit errors:', commitErrs);
        return null;
    }
    return commitResult.contract;
}

/* ── Update contract status (ACTIVE / PAUSED / CANCELLED / EXPIRED) ── */
async function updateContractStatus(contractGid, status) {
    const mutation = `
        mutation UpdateContract($contractId: ID!, $input: SubscriptionContractUpdateInput!) {
            subscriptionContractUpdate(contractId: $contractId, input: $input) {
                draft { id }
                userErrors { field message }
            }
        }
    `;
    const data = await gql(mutation, {
        contractId: contractGid,
        input: { status }
    });
    const result = data.subscriptionContractUpdate;
    const errs = result.userErrors || [];
    if (errs.length) throw new Error(errs.map(e => e.message).join('; '));

    // Commit the update
    if (result.draft && result.draft.id) {
        await gql(`
            mutation Commit($id: ID!) { subscriptionDraftCommit(draftId: $id) { contract { id } userErrors { message } } }
        `, { id: result.draft.id }).catch(() => {});
    }
    return true;
}

/* ── List contracts for a customer email (via customer lookup then contracts) ── */
async function getCustomerContracts(customerGid) {
    const query = `
        query CustomerContracts($id: ID!) {
            customer(id: $id) {
                subscriptionContracts(first: 20) {
                    nodes {
                        id
                        status
                        nextBillingDate
                        createdAt
                        lines(first: 5) {
                            nodes {
                                id
                                title
                                quantity
                                currentPrice { amount currencyCode }
                                sellingPlan { id name }
                            }
                        }
                        deliveryPolicy { interval intervalCount }
                    }
                }
            }
        }
    `;
    const data = await gql(query, { id: customerGid });
    return data.customer && data.customer.subscriptionContracts
        ? data.customer.subscriptionContracts.nodes
        : [];
}

/* ── Register a billing attempt on a contract ── */
async function createBillingAttempt(contractGid, idempotencyKey) {
    const mutation = `
        mutation BillingAttempt($subscriptionContractId: ID!, $idempotencyKey: String!) {
            subscriptionBillingAttemptCreate(
                subscriptionContractId: $subscriptionContractId
                subscriptionBillingAttemptInput: {
                    idempotencyKey: $idempotencyKey
                    originTime: "${new Date().toISOString()}"
                }
            ) {
                subscriptionBillingAttempt {
                    id
                    ready
                    errorMessage
                    order { id name }
                }
                userErrors { field message code }
            }
        }
    `;
    const data = await gql(mutation, {
        subscriptionContractId: contractGid,
        idempotencyKey
    });
    const result = data.subscriptionBillingAttemptCreate;
    const errs = result.userErrors || [];
    if (errs.length) console.warn('[BILLING ATTEMPT] Errors:', errs);
    return result.subscriptionBillingAttempt;
}

module.exports = { createContract, updateContractStatus, getCustomerContracts, createBillingAttempt };
