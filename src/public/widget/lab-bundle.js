/* ════════════════════════════════════════════════════════════
   LAB NUTRITION — Bundle Mix & Match module
   Self-contained JS for configurable bundle subscriptions
   (C4 Energy, pre-workouts, etc.)

   Usage from liquid:
     window.LabBundle.attach(wrap, blkId, loadConfig);

   Optional context (set before attach):
     window.__LAB_BUNDLE_CTX_[blkId] = {
       perks: ['perk1', 'perk2', ...],
       showPerks: true|false,
       showLegal: true|false,
       legalTxt: '...'
     };
   ════════════════════════════════════════════════════════════ */
(function () {
  'use strict';

  function escapeHtml(s) {
    return String(s == null ? '' : s).replace(/[&<>"']/g, function (c) {
      return ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' })[c];
    });
  }

  function attach(wrap, blkId, loadConfigFallback) {
    if (!wrap || !blkId) return;

    var ctx = (window['__LAB_BUNDLE_CTX_' + blkId] || {});
    var _bundleConfig = null;
    var _bundleSelected = {};
    var _bundleSelectedPlanIdx = 0;

    function bundleTotalSelected() {
      var total = 0;
      for (var k in _bundleSelected) total += Number(_bundleSelected[k]) || 0;
      return total;
    }
    function bundleCurrentPlan() {
      if (!_bundleConfig || !_bundleConfig.plans) return null;
      return _bundleConfig.plans[_bundleSelectedPlanIdx] || _bundleConfig.plans[0];
    }
    function bundleFormatPrice(n) {
      var sym = wrap.dataset.curSymbol || 'S/ ';
      return sym + Number(n).toFixed(2);
    }

    function bundleRenderHeader() {
      var plan = bundleCurrentPlan();
      if (!plan) return '';
      var copy = _bundleConfig.widget_copy || {};
      // ── COMBO FIJO: header simple, sin barra de progreso ──
      if (isComboType()) {
        var cTitle = copy.title || _bundleConfig.name || 'Suscripción combo';
        var cSubtitle = copy.subtitle || (_bundleConfig.description || 'Elige tu plan y suscríbete');
        return '' +
          '<div class="lab-bundle__header">' +
            '<div class="lab-bundle__title">' + escapeHtml(cTitle) + '</div>' +
            '<div class="lab-bundle__subtitle">' + escapeHtml(cSubtitle) + '</div>' +
          '</div>';
      }
      var total = bundleTotalSelected();
      var target = _bundleConfig.target_quantity;
      var percent = Math.min(100, Math.round((total / target) * 100));
      var title = copy.title || 'Arma tu mix';
      var subtitle = copy.subtitle || 'Elige tus sabores';
      var counterLbl = copy.counter_label || 'Unidades seleccionadas';
      return '' +
        '<div class="lab-bundle__header">' +
          '<div class="lab-bundle__title">' + escapeHtml(title) + '</div>' +
          '<div class="lab-bundle__subtitle">' + escapeHtml(subtitle) + '</div>' +
        '</div>' +
        '<div class="lab-bundle__progress-wrap">' +
          '<div class="lab-bundle__progress-bar"><div class="lab-bundle__progress-fill" style="width:' + percent + '%"></div></div>' +
          '<div class="lab-bundle__progress-label"><span class="lab-bundle__progress-count" id="bundle-count-' + blkId + '">' + total + '/' + target + '</span><span class="lab-bundle__progress-text">' + escapeHtml(counterLbl) + '</span></div>' +
        '</div>';
    }

    function bundleRenderPlans() {
      if (!_bundleConfig || !_bundleConfig.plans || _bundleConfig.plans.length === 0) return '';
      var target = _bundleConfig.target_quantity;
      var html = '<div class="lab-bundle__plans">';
      _bundleConfig.plans.forEach(function (p, i) {
        var perUnit = (p.price / target).toFixed(2);
        var permLbl = p.perm_months + ' meses';
        var active = i === _bundleSelectedPlanIdx ? ' is-active' : '';
        var savingLbl = p.discount_pct > 0 ? '<span class="lab-bundle__plan-save">-' + Math.round(p.discount_pct) + '%</span>' : '';
        html += '<button type="button" class="lab-bundle__plan' + active + '" data-plan-idx="' + i + '" onclick="window.__bundleSelectPlan_' + blkId + '(' + i + ')">' +
          '<div class="lab-bundle__plan-top">' +
            '<span class="lab-bundle__plan-perm">Plan ' + permLbl + '</span>' +
            savingLbl +
          '</div>' +
          '<div class="lab-bundle__plan-price">' + bundleFormatPrice(p.price) + '<span class="lab-bundle__plan-period">/mes</span></div>' +
          '<div class="lab-bundle__plan-unit">' + bundleFormatPrice(perUnit) + ' por unidad</div>' +
        '</button>';
      });
      html += '</div>';
      return html;
    }

    function isComboType() {
      return _bundleConfig && _bundleConfig.type === 'fixed_combo';
    }

    function bundleRenderFlavors() {
      if (!_bundleConfig) return '';
      // ── COMBO FIJO: no hay selector de sabores, mostrar los items tal cual ──
      if (isComboType()) {
        var items = _bundleConfig.combo_items || [];
        if (!items.length) return '';
        var html = '<div class="lab-bundle__flavors-head"><span class="lab-bundle__flavors-title">Cada caja incluye</span></div>';
        html += '<div class="lab-bundle__combo-items" style="display:grid;grid-template-columns:1fr;gap:10px;margin:8px 0 16px">';
        items.forEach(function (it) {
          var qty = Number(it.quantity) || 1;
          html += '<div class="lab-bundle__combo-item" style="display:flex;gap:12px;align-items:center;padding:10px 12px;background:#f0fdf4;border:1.5px solid #86efac;border-radius:10px">';
          if (it.image) {
            html += '<img src="' + it.image + '" alt="' + escapeHtml(it.title || '') + '" style="width:52px;height:52px;object-fit:cover;border-radius:8px;flex:0 0 52px" loading="lazy">';
          } else {
            html += '<div style="width:52px;height:52px;background:#d1fae5;border-radius:8px;flex:0 0 52px"></div>';
          }
          html += '<div style="flex:1;min-width:0">';
          html += '<div style="font-weight:700;font-size:13.5px;color:#064e3b">' + escapeHtml(it.title || 'Producto') + '</div>';
          if (it.variant_title) html += '<div style="font-size:11.5px;color:#065f46">' + escapeHtml(it.variant_title) + '</div>';
          html += '</div>';
          html += '<div style="flex:0 0 auto;background:#16a34a;color:#fff;padding:4px 12px;border-radius:999px;font-weight:800;font-size:13px">' + qty + '×</div>';
          html += '</div>';
        });
        html += '</div>';
        return html;
      }
      // ── MIX & MATCH (original) ──
      if (!_bundleConfig.flavors) return '';
      var target = _bundleConfig.target_quantity;
      var total = bundleTotalSelected();
      var remaining = target - total;
      var html = '<div class="lab-bundle__flavors-head"><span class="lab-bundle__flavors-title">Elige tus sabores</span></div>';
      html += '<div class="lab-bundle__flavors">';
      _bundleConfig.flavors.forEach(function (f) {
        var qty = Number(_bundleSelected[f.variant_id] || 0);
        var isAvailable = f.available === true;
        var disabledClass = !isAvailable ? ' is-disabled' : '';
        var plusDisabled = !isAvailable || remaining <= 0;
        var minusDisabled = qty <= 0;
        html += '<div class="lab-bundle__flavor' + disabledClass + '" data-vid="' + f.variant_id + '">';
        if (!isAvailable) html += '<div class="lab-bundle__flavor-badge">Agotado</div>';
        html += '<div class="lab-bundle__flavor-img-wrap">';
        if (f.image) {
          html += '<img class="lab-bundle__flavor-img" src="' + f.image + '" alt="' + escapeHtml(f.title) + '" loading="lazy">';
        } else {
          html += '<div class="lab-bundle__flavor-img lab-bundle__flavor-img--empty"></div>';
        }
        html += '</div>';
        html += '<div class="lab-bundle__flavor-body">';
        html += '<div class="lab-bundle__flavor-title">' + escapeHtml(f.title) + '</div>';
        html += '<div class="lab-bundle__flavor-controls">';
        html += '<button type="button" class="lab-bundle__flavor-btn lab-bundle__flavor-btn--minus" ' + (minusDisabled ? 'disabled' : '') + ' onclick="window.__bundleChangeQty_' + blkId + '(\'' + f.variant_id + '\', -1)" aria-label="Quitar una">−</button>';
        html += '<span class="lab-bundle__flavor-qty" id="bundle-qty-' + f.variant_id + '-' + blkId + '">' + qty + '</span>';
        html += '<button type="button" class="lab-bundle__flavor-btn lab-bundle__flavor-btn--plus" ' + (plusDisabled ? 'disabled' : '') + ' onclick="window.__bundleChangeQty_' + blkId + '(\'' + f.variant_id + '\', 1)" aria-label="Agregar una">+</button>';
        html += '</div></div></div>';
      });
      html += '</div>';
      return html;
    }

    function bundleRenderSummary() {
      // Combo fijo: no hay status "incompleto" ni "Tu mix" — está pre-armado
      if (isComboType()) {
        return '<div class="lab-bundle__status lab-bundle__status--ok">✓ Combo listo — elige tu plan y suscríbete</div>';
      }
      var target = _bundleConfig.target_quantity;
      var total = bundleTotalSelected();
      var remaining = target - total;
      var isComplete = total === target;
      var copy = _bundleConfig.widget_copy || {};
      var errorTpl = copy.error_incomplete || 'Te faltan {remaining} unidades para completar tu pack';
      var errorMsg = errorTpl.replace(/\{remaining\}/g, remaining);
      var html = '';
      if (!isComplete) {
        html += '<div class="lab-bundle__status lab-bundle__status--warn">' + escapeHtml(errorMsg) + '</div>';
      } else {
        html += '<div class="lab-bundle__status lab-bundle__status--ok">✓ Pack completo — listo para suscribirte</div>';
      }
      if (total > 0 && _bundleConfig.flavors) {
        html += '<div class="lab-bundle__summary"><div class="lab-bundle__summary-title">Tu mix</div><ul class="lab-bundle__summary-list">';
        _bundleConfig.flavors.forEach(function (f) {
          var q = Number(_bundleSelected[f.variant_id] || 0);
          if (q > 0) html += '<li><span class="lab-bundle__summary-qty">' + q + '×</span> ' + escapeHtml(f.title) + '</li>';
        });
        html += '</ul></div>';
      }
      return html;
    }

    function bundleRenderCTA() {
      var plan = bundleCurrentPlan();
      // Combo fijo: CTA siempre habilitado (el cliente solo elige el plan)
      if (isComboType()) {
        var lbl = 'Suscribirme — ' + bundleFormatPrice(plan.price) + '/mes';
        return '<button class="lab-sub__cta lab-bundle__cta" id="bundle-cta-' + blkId + '" onclick="window.__bundleOpenModal_' + blkId + '()"><span class="lab-sub__cta-text">' + lbl + '</span></button>';
      }
      var total = bundleTotalSelected();
      var target = _bundleConfig.target_quantity;
      var isComplete = total === target;
      var ctaLbl = isComplete ? 'Suscribirme — ' + bundleFormatPrice(plan.price) + '/mes' : 'Completa tu pack para continuar';
      var disabled = !isComplete ? 'disabled' : '';
      return '<button class="lab-sub__cta lab-bundle__cta" id="bundle-cta-' + blkId + '" ' + disabled + ' onclick="window.__bundleOpenModal_' + blkId + '()"><span class="lab-sub__cta-text">' + ctaLbl + '</span></button>';
    }

    function bundleRenderPerks() {
      if (ctx.showPerks === false) return '';
      var perks = Array.isArray(ctx.perks) ? ctx.perks.filter(Boolean) : [];
      if (perks.length === 0) return '';
      return '<div class="lab-sub__perks">' + perks.map(function (p) { return '<div class="lab-sub__perk">' + escapeHtml(p) + '</div>'; }).join('') + '</div>';
    }

    function bundleRenderLegal() {
      if (ctx.showLegal === false) return '';
      var txt = ctx.legalTxt || '';
      if (!txt) return '';
      return '<div class="lab-sub__legal">' + escapeHtml(txt) + '</div>';
    }

    function bundleRender() {
      var subPanel = wrap.querySelector('.lab-sub__panel[data-panel="sub"]');
      if (!subPanel) return;
      subPanel.innerHTML =
        bundleRenderHeader() +
        bundleRenderPlans() +
        bundleRenderFlavors() +
        bundleRenderSummary() +
        bundleRenderPerks() +
        bundleRenderLegal() +
        bundleRenderCTA();
    }

    window['__bundleSelectPlan_' + blkId] = function (idx) {
      _bundleSelectedPlanIdx = idx;
      bundleRender();
    };
    window['__bundleChangeQty_' + blkId] = function (variantId, delta) {
      var currentQty = Number(_bundleSelected[variantId] || 0);
      var newQty = Math.max(0, currentQty + delta);
      var target = _bundleConfig.target_quantity;
      var totalOthers = 0;
      for (var k in _bundleSelected) if (k !== variantId) totalOthers += Number(_bundleSelected[k]) || 0;
      if (newQty + totalOthers > target) newQty = target - totalOthers;
      if (newQty < 0) newQty = 0;
      _bundleSelected[variantId] = newQty;
      bundleRender();
    };
    window['__bundleOpenModal_' + blkId] = function () {
      var isCombo = _bundleConfig && _bundleConfig.type === 'fixed_combo';
      if (!isCombo && bundleTotalSelected() !== _bundleConfig.target_quantity) return;
      var plan = bundleCurrentPlan();
      // FIX 2026-04-21: teleport modal a document.body para escapar overflow:hidden del .lab-sub
      var modal = (window.__labSubTeleportModal ? window.__labSubTeleportModal(blkId) : document.getElementById('lab-modal-' + blkId));
      if (!modal) return;
      var title = document.getElementById('modal-title-' + blkId);
      var priceEl = document.getElementById('modal-price-' + blkId);
      var discEl = document.getElementById('modal-disc-' + blkId);
      var savingEl = document.getElementById('modal-saving-' + blkId);
      if (title) title.textContent = 'Suscripción ' + _bundleConfig.name;
      if (priceEl) priceEl.textContent = bundleFormatPrice(plan.price);
      if (discEl) {
        var discPct = Math.round(plan.discount_pct || 0);
        discEl.textContent = discPct > 0 ? discPct + '% OFF' : '';
        discEl.style.display = discPct > 0 ? '' : 'none';
      }
      if (savingEl) {
        var target = _bundleConfig.target_quantity;
        var perUnit = (plan.price / target).toFixed(2);
        savingEl.textContent = target + ' × ' + bundleFormatPrice(perUnit);
      }
      modal.style.display = 'flex';
    };

    function bundleLoadConfig() {
      var productId = wrap.dataset.product;
      var backendUrl = wrap.dataset.backend || 'https://pixel-suite-pro-production.up.railway.app';
      if (!productId) return Promise.resolve(false);
      // ── DEFENSIVO 2026-04-22: ocultar chips pre-renderizadas del Liquid mientras
      //    esperamos la respuesta del backend. Así evitamos el flash de "Mensual · 3 meses"
      //    en bundles. Si NO es bundle, el fallback loadConfig() las re-muestra.
      var subPanelEarly = wrap.querySelector('.lab-sub__panel[data-panel="sub"]');
      if (subPanelEarly && !subPanelEarly.dataset.labLoading) {
        subPanelEarly.dataset.labLoading = '1';
        subPanelEarly.dataset.originalHtml = subPanelEarly.innerHTML;
        subPanelEarly.innerHTML = '<div style="padding:40px 20px;text-align:center;color:#9ca3af;font-size:13px">Cargando configuración…</div>';
      }
      return fetch(backendUrl + '/api/bundles/product/' + encodeURIComponent(productId) + '/config')
        .then(function (r) {
          if (r.status === 404 || r.status === 410) return null;
          if (!r.ok) return null;
          return r.json();
        })
        .then(function (cfg) {
          var isBundle = !!(cfg && cfg.bundle_id);
          var isCombo = isBundle && cfg.type === 'fixed_combo';
          if (isBundle && !isCombo && !cfg.flavors) isBundle = false;
          if (isBundle && isCombo && (!cfg.combo_items || !cfg.combo_items.length)) isBundle = false;
          if (!isBundle) {
            // Restaurar HTML original del Liquid para que loadConfig() del tema funcione normal
            if (subPanelEarly && subPanelEarly.dataset.originalHtml !== undefined) {
              subPanelEarly.innerHTML = subPanelEarly.dataset.originalHtml;
              delete subPanelEarly.dataset.originalHtml;
              delete subPanelEarly.dataset.labLoading;
            }
            return false;
          }
          _bundleConfig = cfg;
          _bundleSelected = {};
          // Pre-llenar selección para combos fijos (no hay picker)
          if (isCombo) {
            cfg.combo_items.forEach(function (it) {
              _bundleSelected[it.variant_id] = Number(it.quantity) || 1;
            });
          }
          var onceTab = wrap.querySelector('.lab-sub__tab[data-tab="once"]');
          if (onceTab) onceTab.style.display = 'none';
          var subTab = wrap.querySelector('.lab-sub__tab[data-tab="sub"]');
          if (subTab) {
            subTab.classList.add('lab-sub__tab--active');
            subTab.style.display = '';
            subTab.disabled = false;
            subTab.removeAttribute('aria-disabled');
          }
          var oncePanel = wrap.querySelector('.lab-sub__panel[data-panel="once"]');
          if (oncePanel) oncePanel.classList.remove('lab-sub__panel--active');
          var subPanel = wrap.querySelector('.lab-sub__panel[data-panel="sub"]');
          if (subPanel) subPanel.classList.add('lab-sub__panel--active');
          var notice = document.getElementById('variant-notice-' + blkId);
          if (notice) notice.style.display = 'none';
          var subContent = document.getElementById('sub-content-' + blkId);
          if (subContent) subContent.style.display = '';
          // Clear loading flag so bundleRender puede hacer innerHTML limpio
          if (subPanel) {
            delete subPanel.dataset.labLoading;
            delete subPanel.dataset.originalHtml;
          }
          bundleRender();
          return true;
        })
        .catch(function () {
          // En error, restaurar HTML original para no dejar pantalla en blanco
          if (subPanelEarly && subPanelEarly.dataset.originalHtml !== undefined) {
            subPanelEarly.innerHTML = subPanelEarly.dataset.originalHtml;
            delete subPanelEarly.dataset.originalHtml;
            delete subPanelEarly.dataset.labLoading;
          }
          return false;
        });
    }

    // Hook submit del modal para enviar bundle_items si hay bundle activo
    var mBtnBundle = document.getElementById('m-btn-' + blkId);
    if (mBtnBundle) {
      mBtnBundle.addEventListener('click', function (ev) {
        if (!_bundleConfig) return; // no es bundle → deja el listener legacy
        ev.stopImmediatePropagation();
        ev.preventDefault();

        var name = (document.getElementById('m-name-' + blkId) || {}).value;
        var email = (document.getElementById('m-email-' + blkId) || {}).value;
        var phone = (document.getElementById('m-phone-' + blkId) || {}).value || '';
        var tipodoc = (document.getElementById('m-tipodoc-' + blkId) || {}).value || '01';
        var dni = (document.getElementById('m-dni-' + blkId) || {}).value || '';
        var addr1 = (document.getElementById('m-addr-' + blkId) || {}).value;
        var city = (document.getElementById('m-city-' + blkId) || {}).value;
        var province = (document.getElementById('m-province-' + blkId) || {}).value || 'Lima';
        var tcCheck = document.getElementById('lab-tc-check-' + blkId);
        var msgEl = document.getElementById('m-msg-' + blkId);

        function showErr(t) {
          if (msgEl) { msgEl.style.display = 'block'; msgEl.style.background = '#fee2e2'; msgEl.style.color = '#991b1b'; msgEl.textContent = t; }
        }

        if (!name || !email || !email.indexOf || email.indexOf('@') === -1 || !addr1 || !city || !dni) {
          return showErr('Completa nombre, email, documento, dirección y distrito.');
        }
        if (tcCheck && !tcCheck.checked) {
          return showErr('Debes aceptar los Términos y Condiciones para continuar.');
        }
        var isCombo = _bundleConfig.type === 'fixed_combo';
        if (!isCombo && bundleTotalSelected() !== _bundleConfig.target_quantity) {
          return showErr('Tu mix está incompleto. Ajusta las cantidades.');
        }

        var plan = bundleCurrentPlan();
        var bundleItems = [];
        if (isCombo) {
          (_bundleConfig.combo_items || []).forEach(function (it) {
            var q = Number(it.quantity) || 1;
            bundleItems.push({
              variant_id: String(it.variant_id),
              quantity: q,
              title: it.title || '',
              variant_title: it.variant_title || it.title || ''
            });
          });
        } else {
          _bundleConfig.flavors.forEach(function (f) {
            var q = Number(_bundleSelected[f.variant_id] || 0);
            if (q > 0) bundleItems.push({ variant_id: String(f.variant_id), quantity: q, title: f.title, variant_title: f.full_title || f.title });
          });
        }
        var subVariantId = plan.variant_id_perm || wrap.dataset.variant;
        var backendUrl = wrap.dataset.backend || 'https://pixel-suite-pro-production.up.railway.app';
        mBtnBundle.disabled = true; mBtnBundle.textContent = 'Procesando...';

        var nameParts = name.trim().split(' ');
        var PE_PROV = { 'lima': 'LIM', 'arequipa': 'ARE', 'cusco': 'CUS', 'la libertad': 'LAL', 'piura': 'PIU', 'lambayeque': 'LAM', 'junin': 'JUN', 'cajamarca': 'CAJ', 'ancash': 'ANC', 'ica': 'ICA', 'callao': 'CAL', 'tacna': 'TAC', 'loreto': 'LOR', 'san martin': 'SAM', 'ucayali': 'UCA', 'huanuco': 'HUA', 'puno': 'PUN', 'amazonas': 'AMA', 'ayacucho': 'AYA', 'apurimac': 'APU', 'huancavelica': 'HUV', 'madre de dios': 'MDD', 'moquegua': 'MOQ', 'pasco': 'PAS', 'tumbes': 'TUM' };
        var shippingAddress = {
          first_name: nameParts[0] || name,
          last_name: nameParts.slice(1).join(' ') || '',
          address1: addr1, city: city, province: province,
          province_code: PE_PROV[(province || '').toLowerCase()] || 'LIM',
          country: 'PE', country_code: 'PE', zip: '15000', phone: phone
        };
        var finalPriceNum = parseFloat(Number(plan.price).toFixed(2));

        fetch(backendUrl + '/api/subscriptions/checkout', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            customer_name: name, customer_email: email, customer_phone: phone,
            tipo_documento: tipodoc, dni: dni,
            product_id: wrap.dataset.product, variant_id: subVariantId,
            product_title: _bundleConfig.name || wrap.dataset.title,
            base_price: finalPriceNum, final_price: finalPriceNum,
            discount_pct: parseFloat(Number(plan.discount_pct || 0).toFixed(2)),
            frequency_months: plan.freq_months || 1,
            permanence_months: plan.perm_months || 3,
            free_shipping: false, shipping_address: shippingAddress,
            tc_accepted: true, tc_version: '1.0', tc_accepted_at: new Date().toISOString(),
            bundle_items: bundleItems
          })
        })
          .then(function (r) { return r.json(); })
          .then(function (d) {
            if (d.init_point || d.url) {
              mBtnBundle.textContent = 'Redirigiendo a Mercado Pago...';
              mBtnBundle.style.background = '#2E7D49';
              setTimeout(function () { window.location.href = d.init_point || d.url; }, 500);
            } else {
              throw new Error(d.error || 'No se pudo crear el checkout');
            }
          })
          .catch(function (err) {
            showErr(err.message);
            mBtnBundle.disabled = false; mBtnBundle.textContent = 'Continuar al pago →';
          });
      }, true);
    }

    bundleLoadConfig().then(function (isBundle) {
      if (!isBundle && typeof loadConfigFallback === 'function') loadConfigFallback();
    });
  }

  window.LabBundle = { attach: attach };
})();
