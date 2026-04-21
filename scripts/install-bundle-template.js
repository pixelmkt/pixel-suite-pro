/*
 * One-shot: crea template "product.bundle.json" en el main theme con el app block
 * lab_subscription embebido, y asigna template_suffix="bundle" a los 2 productos bundle.
 *
 * No toca el template main del tema, no toca productos que no sean los 2 bundles.
 * Seguro bajo MASTER LOCK (solo agrega assets; no modifica pedidos/MP/webhooks/crons).
 */
require('dotenv').config({ path: require('path').join(__dirname, '..', '.env') });

const SHOP = process.env.SHOPIFY_SHOP || 'nutrition-lab-cluster.myshopify.com';
const TOKEN = process.env.SHOPIFY_ACCESS_TOKEN;
const API = '2026-01';

// Extension UUID from shopify.extension.toml
const EXT_UUID = '4155d041-6adb-5e48-1fc5-809fdebf7f954a1156be';
const BLOCK_TYPE = `shopify://apps/lab-nutrition-subscriptions/blocks/lab_subscription/${EXT_UUID}`;

const BUNDLE_PRODUCTS = [
  { id: '15769236996177', name: 'Bundle 15' },
  { id: '15769237028945', name: 'Bundle 30' }
];

if (!TOKEN) { console.error('SHOPIFY_ACCESS_TOKEN missing'); process.exit(1); }

async function shopify(path, opts = {}) {
  const url = `https://${SHOP}/admin/api/${API}${path}`;
  const r = await fetch(url, {
    ...opts,
    headers: { 'X-Shopify-Access-Token': TOKEN, 'Content-Type': 'application/json', ...(opts.headers || {}) }
  });
  const txt = await r.text();
  let js = null; try { js = txt ? JSON.parse(txt) : null; } catch (_) {}
  if (!r.ok) throw new Error(`HTTP ${r.status} ${path}: ${txt.substring(0, 300)}`);
  return js;
}

(async () => {
  try {
    console.log('[1/5] Buscando main theme...');
    const themes = (await shopify('/themes.json')).themes || [];
    const main = themes.find(t => t.role === 'main');
    if (!main) throw new Error('No main theme found');
    console.log(`    Main theme: ${main.name} (id=${main.id})`);

    console.log('[2/5] Obteniendo templates/product.json actual...');
    const base = await shopify(`/themes/${main.id}/assets.json?asset[key]=templates/product.json`);
    let baseJson;
    try {
      baseJson = JSON.parse(base.asset.value);
    } catch (e) {
      throw new Error('templates/product.json no es JSON válido');
    }
    console.log(`    sections=${Object.keys(baseJson.sections || {}).length} order=[${(baseJson.order || []).join(', ')}]`);

    console.log('[3/5] Construyendo product.bundle.json con app block inyectado...');
    // Clone base, then add app block to main-product section (or create new section)
    const clone = JSON.parse(JSON.stringify(baseJson));

    // Find the "main-product" section (o equivalente que contenga bloques de producto)
    let mainKey = null;
    for (const [key, sec] of Object.entries(clone.sections || {})) {
      if (sec && typeof sec === 'object' && String(sec.type || '').includes('main-product')) {
        mainKey = key;
        break;
      }
    }
    // Fallback: first section that has blocks
    if (!mainKey) {
      for (const [key, sec] of Object.entries(clone.sections || {})) {
        if (sec && sec.blocks) { mainKey = key; break; }
      }
    }
    if (!mainKey) throw new Error('No se encontró main-product section en template');

    const mainSec = clone.sections[mainKey];
    if (!mainSec.blocks) mainSec.blocks = {};
    if (!mainSec.block_order) mainSec.block_order = [];

    const blockId = `lab_subscription_widget`;
    mainSec.blocks[blockId] = {
      type: BLOCK_TYPE,
      settings: {}
    };
    // Insert after "buy_buttons" or "price" or at the end
    const order = mainSec.block_order;
    if (!order.includes(blockId)) {
      const insertAfter = order.findIndex(k => {
        const t = (mainSec.blocks[k] && mainSec.blocks[k].type) || '';
        return t.includes('buy_buttons') || t.includes('price');
      });
      if (insertAfter >= 0) order.splice(insertAfter + 1, 0, blockId);
      else order.push(blockId);
    }

    const assetBody = {
      asset: {
        key: 'templates/product.bundle.json',
        value: JSON.stringify(clone, null, 2)
      }
    };

    console.log('[4/5] Subiendo templates/product.bundle.json al theme...');
    const up = await shopify(`/themes/${main.id}/assets.json`, {
      method: 'PUT',
      body: JSON.stringify(assetBody)
    });
    console.log(`    ✓ Subido: ${up.asset.key} (${(up.asset.size || 0)} bytes)`);

    console.log('[5/5] Asignando template_suffix="bundle" a productos...');
    for (const p of BUNDLE_PRODUCTS) {
      const r = await shopify(`/products/${p.id}.json`, {
        method: 'PUT',
        body: JSON.stringify({ product: { id: Number(p.id), template_suffix: 'bundle' } })
      });
      console.log(`    ✓ ${p.name} (${p.id}): template_suffix=${r.product.template_suffix}`);
    }

    console.log('\n✅ DONE. Widget visible en:');
    console.log(`    https://labnutrition.com/products/c4-energy-arma-tu-mix-de-15-latas-suscripcion`);
    console.log(`    https://labnutrition.com/products/c4-energy-arma-tu-mix-de-30-latas-suscripcion`);
  } catch (e) {
    console.error('❌ FAIL:', e.message);
    process.exit(2);
  }
})();
