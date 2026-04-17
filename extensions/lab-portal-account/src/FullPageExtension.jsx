const BACKEND = 'https://pixel-suite-pro-production.up.railway.app';

export default async (api) => {
  // ═══ UNICA FUENTE DE VERDAD: backend /api/portal-config ═══
  let cfg = {};
  let brand = {};
  try {
    const res1 = await fetch(BACKEND + '/api/portal-config');
    if (res1.ok) cfg = await res1.json();
  } catch (e) {}
  try {
    const res2 = await fetch(BACKEND + '/api/settings');
    if (res2.ok) brand = await res2.json();
  } catch (e) {}

  // ── Customer ──
  let customerEmail = null;
  let customerId = null;
  try {
    const acc = api?.authenticatedAccount?.customer;
    if (acc) {
      const cur = typeof acc.current === 'function' ? await acc.current() : (acc.current || null);
      if (cur) {
        customerEmail = cur.emailAddress?.emailAddress || cur.email || null;
        customerId = cur.id ? String(cur.id).split('/').pop() : null;
      }
    }
  } catch (e) {}

  // ── Customer subscriptions ──
  // Priorizar email (lo que las subs almacenan) sobre customerId (Shopify GID numeric)
  // Intentar ambos para máxima cobertura
  let mySubs = [];
  if (customerEmail || customerId) {
    try {
      // 1) Buscar por email (prioridad — todas las subs tienen email, pocas tienen customer_id)
      if (customerEmail) {
        const r = await fetch(BACKEND + '/api/subscriptions/customer/' + encodeURIComponent(customerEmail));
        if (r.ok) mySubs = await r.json();
      }
      // 2) Si no encontró por email, intentar por customer_id numérico
      if (!mySubs.length && customerId) {
        const r2 = await fetch(BACKEND + '/api/subscriptions/customer/' + encodeURIComponent(customerId));
        if (r2.ok) mySubs = await r2.json();
      }
    } catch (e) {}
  }
  // SOLO mostrar subs con pago confirmado (active, paused, cancelled con ciclos).
  // NUNCA mostrar pending_payment — esas aún no tienen cobro de MP.
  mySubs = mySubs.filter(s =>
    s.status === 'active' ||
    s.status === 'paused' ||
    (s.status === 'cancelled' && (s.cycles_completed || 0) > 0)
  );
  const isActiveMember = mySubs.some(s => s.status === 'active' || s.status === 'paused');

  // ═══ BUILD PAGE ═══
  const pageTitle = cfg.page_title || 'Cliente Black Diamond';
  const page = document.createElement('s-page');
  page.setAttribute('heading', pageTitle);

  // ── Subtítulo ──
  if (cfg.page_subtitle) {
    const b = document.createElement('s-banner');
    b.setAttribute('tone', isActiveMember ? 'success' : 'info');
    b.textContent = cfg.page_subtitle;
    page.appendChild(b);
  }

  // ── HERO IMAGE (editable) ──
  if (cfg.hero_image) {
    const sec = document.createElement('s-section');
    const img = document.createElement('s-image');
    img.setAttribute('source', cfg.hero_image);
    img.setAttribute('fit', 'cover');
    img.setAttribute('aspectRatio', '21/9');
    sec.appendChild(img);
    page.appendChild(sec);
  }

  // ── Dynamic section order ──
  // Suscripciones primero (el suscriptor activo quiere ver su estado, no el CTA de unirse)
  const defaultOrder = ['subscriptions', 'bd_cta', 'banner', 'benefits', 'product', 'events', 'ctas', 'custom_html', 'whatsapp'];
  const order = Array.isArray(cfg.section_order) && cfg.section_order.length ? cfg.section_order : defaultOrder;

  const renderers = {
    bd_cta: () => {
      if (isActiveMember) return;
      const sec = document.createElement('s-section');
      const box = document.createElement('s-box');
      box.setAttribute('padding', 'large');
      box.setAttribute('background', 'subdued');
      box.setAttribute('border', 'base');
      box.setAttribute('cornerRadius', 'large');
      const st = document.createElement('s-stack');
      st.setAttribute('gap', 'base');
      const eye = document.createElement('s-text');
      eye.setAttribute('emphasis', 'bold');
      eye.setAttribute('size', 'small');
      eye.textContent = 'CLUB BLACK DIAMOND';
      st.appendChild(eye);
      const h = document.createElement('s-heading');
      h.textContent = cfg.bd_title || 'Conviértete en cliente Black Diamond';
      st.appendChild(h);
      const sub = document.createElement('s-text');
      sub.setAttribute('appearance', 'subdued');
      sub.textContent = cfg.bd_subtitle || 'Hasta 50% OFF permanente · envío prioritario · regalos exclusivos · IGV incluido';
      st.appendChild(sub);
      if (cfg.bd_image) {
        const img = document.createElement('s-image');
        img.setAttribute('source', cfg.bd_image);
        img.setAttribute('fit', 'cover');
        img.setAttribute('aspectRatio', '16/9');
        st.appendChild(img);
      }
      const btn = document.createElement('s-button');
      btn.setAttribute('href', cfg.bd_btn_url || 'https://labnutrition.com/collections/all');
      btn.setAttribute('target', 'auto');
      btn.textContent = cfg.bd_btn_text || 'Quiero ser Black Diamond';
      st.appendChild(btn);
      box.appendChild(st);
      sec.appendChild(box);
      page.appendChild(sec);
    },
    banner: () => {
      if (!cfg.banner_text && !cfg.banner_image) return;
      const sec = document.createElement('s-section');
      const box = document.createElement('s-box');
      box.setAttribute('padding', 'base');
      box.setAttribute('background', 'subdued');
      box.setAttribute('border', 'base');
      box.setAttribute('cornerRadius', 'base');
      const st = document.createElement('s-stack');
      st.setAttribute('gap', 'small');
      if (cfg.banner_image) {
        const img = document.createElement('s-image');
        img.setAttribute('source', cfg.banner_image);
        img.setAttribute('fit', 'cover');
        img.setAttribute('aspectRatio', '21/9');
        st.appendChild(img);
      }
      if (cfg.banner_text) {
        const t = document.createElement('s-text');
        t.setAttribute('emphasis', 'bold');
        t.textContent = cfg.banner_text;
        st.appendChild(t);
      }
      if (cfg.banner_url) {
        const b = document.createElement('s-button');
        b.setAttribute('href', cfg.banner_url);
        b.setAttribute('target', 'auto');
        b.setAttribute('kind', 'secondary');
        b.textContent = 'Ver más';
        st.appendChild(b);
      }
      box.appendChild(st);
      sec.appendChild(box);
      page.appendChild(sec);
    },
    benefits: () => {
      // SOLO del backend. Sin defaults ocultos. Si el admin los borra, no salen.
      const list = Array.isArray(cfg.benefits) ? cfg.benefits.filter(b => b && b.title) : [];
      if (!list.length) return;
      const sec = document.createElement('s-section');
      sec.setAttribute('heading', cfg.benefits_title || 'Beneficios');
      list.forEach((b, i) => {
        const box = document.createElement('s-box');
        box.setAttribute('padding', 'base');
        box.setAttribute('border', 'base');
        box.setAttribute('cornerRadius', 'base');
        const st = document.createElement('s-stack');
        st.setAttribute('gap', 'small');
        const num = document.createElement('s-text');
        num.setAttribute('appearance', 'subdued');
        num.setAttribute('size', 'small');
        num.textContent = String(i + 1).padStart(2, '0');
        st.appendChild(num);
        const tt = document.createElement('s-text');
        tt.setAttribute('emphasis', 'bold');
        tt.textContent = b.title;
        st.appendChild(tt);
        if (b.description) {
          const dd = document.createElement('s-text');
          dd.setAttribute('appearance', 'subdued');
          dd.textContent = b.description;
          st.appendChild(dd);
        }
        box.appendChild(st);
        sec.appendChild(box);
      });
      page.appendChild(sec);
    },
    product: () => {
      const p = cfg.product_of_week;
      if (!p || !p.name) return;
      const sec = document.createElement('s-section');
      sec.setAttribute('heading', cfg.product_section_title || 'Producto destacado');
      const box = document.createElement('s-box');
      box.setAttribute('padding', 'large');
      box.setAttribute('border', 'base');
      box.setAttribute('cornerRadius', 'large');
      const st = document.createElement('s-stack');
      st.setAttribute('gap', 'base');
      if (p.image) {
        const img = document.createElement('s-image');
        img.setAttribute('source', p.image);
        img.setAttribute('fit', 'cover');
        img.setAttribute('aspectRatio', '16/9');
        st.appendChild(img);
      }
      const nm = document.createElement('s-text');
      nm.setAttribute('emphasis', 'bold');
      nm.setAttribute('size', 'large');
      nm.textContent = p.name;
      st.appendChild(nm);
      if (p.description) {
        const d = document.createElement('s-text');
        d.setAttribute('appearance', 'subdued');
        d.textContent = p.description;
        st.appendChild(d);
      }
      if (p.price || p.sub_price) {
        const row = document.createElement('s-stack');
        row.setAttribute('direction', 'inline');
        row.setAttribute('gap', 'base');
        if (p.sub_price) {
          const pp = document.createElement('s-text');
          pp.setAttribute('emphasis', 'bold');
          pp.textContent = 'S/ ' + p.sub_price;
          row.appendChild(pp);
        }
        if (p.price && p.sub_price) {
          const po = document.createElement('s-text');
          po.setAttribute('appearance', 'subdued');
          po.textContent = 'antes S/ ' + p.price;
          row.appendChild(po);
        }
        st.appendChild(row);
      }
      if (p.coupon) {
        const bg = document.createElement('s-badge');
        bg.setAttribute('tone', 'success');
        bg.textContent = p.coupon;
        st.appendChild(bg);
      }
      if (p.url) {
        const bt = document.createElement('s-button');
        bt.setAttribute('href', p.url);
        bt.setAttribute('target', 'auto');
        bt.textContent = cfg.product_button_text || 'Ver producto';
        st.appendChild(bt);
      }
      box.appendChild(st);
      sec.appendChild(box);
      page.appendChild(sec);
    },
    events: () => {
      const list = Array.isArray(cfg.events) ? cfg.events.filter(e => e && e.title) : [];
      if (!list.length) return;
      const sec = document.createElement('s-section');
      sec.setAttribute('heading', cfg.events_title || 'Próximos eventos');
      list.forEach(ev => {
        const box = document.createElement('s-box');
        box.setAttribute('padding', 'base');
        box.setAttribute('border', 'base');
        box.setAttribute('cornerRadius', 'base');
        const st = document.createElement('s-stack');
        st.setAttribute('gap', 'small');
        const t = document.createElement('s-text');
        t.setAttribute('emphasis', 'bold');
        t.textContent = ev.title;
        st.appendChild(t);
        if (ev.date) {
          const d = document.createElement('s-text');
          d.setAttribute('appearance', 'subdued');
          d.setAttribute('size', 'small');
          try { d.textContent = new Date(ev.date).toLocaleDateString('es-PE', { day: 'numeric', month: 'long', year: 'numeric' }); } catch (e) { d.textContent = ev.date; }
          st.appendChild(d);
        }
        if (ev.description) {
          const dd = document.createElement('s-text');
          dd.setAttribute('appearance', 'subdued');
          dd.textContent = ev.description;
          st.appendChild(dd);
        }
        box.appendChild(st);
        sec.appendChild(box);
      });
      page.appendChild(sec);
    },
    ctas: () => {
      const list = Array.isArray(cfg.cta_buttons) ? cfg.cta_buttons.filter(c => c && c.text && c.url) : [];
      if (!list.length) return;
      const sec = document.createElement('s-section');
      const row = document.createElement('s-stack');
      row.setAttribute('direction', 'inline');
      row.setAttribute('gap', 'base');
      list.forEach((c, i) => {
        const bt = document.createElement('s-button');
        bt.setAttribute('href', c.url);
        bt.setAttribute('target', 'auto');
        if (c.style === 'secondary' || i > 0) bt.setAttribute('kind', 'secondary');
        bt.textContent = c.text;
        row.appendChild(bt);
      });
      sec.appendChild(row);
      page.appendChild(sec);
    },
    custom_html: () => {
      if (!cfg.custom_html) return;
      // Shopify customer-account extension NO permite HTML arbitrario por seguridad.
      // Renderizamos como texto plano en un box si el admin lo configuró.
      const sec = document.createElement('s-section');
      const box = document.createElement('s-box');
      box.setAttribute('padding', 'base');
      box.setAttribute('background', 'subdued');
      box.setAttribute('cornerRadius', 'base');
      const t = document.createElement('s-text');
      // Strip HTML tags para safe render
      t.textContent = String(cfg.custom_html).replace(/<[^>]+>/g, '').trim();
      box.appendChild(t);
      sec.appendChild(box);
      page.appendChild(sec);
    },
    subscriptions: () => {
      if (!mySubs.length) return;
      const sec = document.createElement('s-section');
      sec.setAttribute('heading', 'Mis suscripciones');

      mySubs.forEach(sub => {
        const box = document.createElement('s-box');
        box.setAttribute('padding', 'large');
        box.setAttribute('border', 'base');
        box.setAttribute('cornerRadius', 'large');
        const st = document.createElement('s-stack');
        st.setAttribute('gap', 'base');

        // ── Status badge ──
        const statusMap = { active: 'success', paused: 'warning', cancelled: 'critical', pending_payment: 'info' };
        const statusLabel = { active: 'Activa', paused: 'Pausada', cancelled: 'Cancelada', pending_payment: 'Pendiente de pago' };
        const badge = document.createElement('s-badge');
        badge.setAttribute('tone', statusMap[sub.status] || 'info');
        badge.textContent = statusLabel[sub.status] || sub.status;
        st.appendChild(badge);

        // ── Product name ──
        const t = document.createElement('s-text');
        t.setAttribute('emphasis', 'bold');
        t.setAttribute('size', 'large');
        t.textContent = sub.product_title || 'Suscripción';
        st.appendChild(t);

        // ── Plan details row ──
        const cyclesDone = sub.cycles_completed || 0;
        const cyclesReq = sub.cycles_required || 0;
        const freq = sub.frequency_months || 1;
        const price = sub.final_price || sub.base_price || 0;
        const discount = sub.discount_pct || 0;

        const planRow = document.createElement('s-stack');
        planRow.setAttribute('gap', 'small');

        const priceText = document.createElement('s-text');
        priceText.setAttribute('emphasis', 'bold');
        priceText.textContent = 'S/ ' + Number(price).toFixed(2) + '/mes' + (discount ? ' (' + discount + '% OFF)' : '') + ' · IGV incluido';
        planRow.appendChild(priceText);

        const cycleText = document.createElement('s-text');
        cycleText.setAttribute('appearance', 'subdued');
        cycleText.setAttribute('size', 'small');
        cycleText.textContent = 'Progreso: ' + cyclesDone + ' de ' + cyclesReq + ' meses completados · Envío cada ' + freq + ' mes' + (freq > 1 ? 'es' : '');
        planRow.appendChild(cycleText);

        st.appendChild(planRow);

        // ── Next charge / delivery date ──
        if (sub.next_charge_at && sub.status === 'active') {
          const nextBox = document.createElement('s-box');
          nextBox.setAttribute('padding', 'base');
          nextBox.setAttribute('background', 'subdued');
          nextBox.setAttribute('cornerRadius', 'base');
          const nextSt = document.createElement('s-stack');
          nextSt.setAttribute('gap', 'extraSmall');
          const nextLabel = document.createElement('s-text');
          nextLabel.setAttribute('size', 'small');
          nextLabel.setAttribute('emphasis', 'bold');
          nextLabel.textContent = 'Próximo envío';
          nextSt.appendChild(nextLabel);
          const nextDate = document.createElement('s-text');
          nextDate.setAttribute('size', 'small');
          try {
            const d = new Date(sub.next_charge_at);
            nextDate.textContent = d.toLocaleDateString('es-PE', { weekday: 'long', day: 'numeric', month: 'long', year: 'numeric' });
          } catch (e) { nextDate.textContent = sub.next_charge_at; }
          nextSt.appendChild(nextDate);
          nextBox.appendChild(nextSt);
          st.appendChild(nextBox);
        }

        // ── Gift delivery status ──
        if (Array.isArray(sub.gifts_planned) && sub.gifts_planned.length > 0) {
          const giftBox = document.createElement('s-box');
          giftBox.setAttribute('padding', 'base');
          giftBox.setAttribute('background', 'subdued');
          giftBox.setAttribute('cornerRadius', 'base');
          const giftSt = document.createElement('s-stack');
          giftSt.setAttribute('gap', 'extraSmall');
          const giftLabel = document.createElement('s-text');
          giftLabel.setAttribute('size', 'small');
          giftLabel.setAttribute('emphasis', 'bold');
          giftLabel.textContent = sub.gifts_delivered ? 'Regalo entregado' : 'Regalo pendiente';
          giftSt.appendChild(giftLabel);
          sub.gifts_planned.forEach(g => {
            const gt = document.createElement('s-text');
            gt.setAttribute('size', 'small');
            gt.setAttribute('appearance', 'subdued');
            gt.textContent = (g.product_title || 'Regalo') + (g.variant_title ? ' — ' + g.variant_title : '');
            giftSt.appendChild(gt);
          });
          if (sub.gifts_delivered && sub.gifts_delivered_order_name) {
            const gOrd = document.createElement('s-text');
            gOrd.setAttribute('size', 'small');
            gOrd.setAttribute('appearance', 'subdued');
            gOrd.textContent = 'Incluido en orden ' + sub.gifts_delivered_order_name;
            giftSt.appendChild(gOrd);
          }
          giftBox.appendChild(giftSt);
          st.appendChild(giftBox);
        }

        // ── Last order ──
        if (sub.shopify_order_name) {
          const ordText = document.createElement('s-text');
          ordText.setAttribute('size', 'small');
          ordText.setAttribute('appearance', 'subdued');
          ordText.textContent = 'Última orden: ' + sub.shopify_order_name;
          st.appendChild(ordText);
        }

        // ── Action buttons ──
        if (sub.status === 'active' || sub.status === 'paused') {
          const btnRow = document.createElement('s-stack');
          btnRow.setAttribute('direction', 'inline');
          btnRow.setAttribute('gap', 'base');

          const cancelBtn = document.createElement('s-button');
          cancelBtn.setAttribute('kind', 'secondary');
          cancelBtn.setAttribute('tone', 'critical');
          cancelBtn.textContent = 'Cancelar suscripción';
          cancelBtn.addEventListener('click', async () => {
            try {
              const pv = await fetch(BACKEND + '/api/subscriptions/' + sub.id + '/cancel/preview');
              const data = await pv.json();
              let msg = data.message + '\n\n';
              msg += data.free_cancel ? '¿Confirmas la cancelación?' : 'Penalidad: S/' + (data.penalty || 0).toFixed(2) + '\n¿Confirmas? Se generará link de pago.';
              if (!confirm(msg)) return;
              const cc = await fetch(BACKEND + '/api/subscriptions/' + sub.id + '/cancel', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ confirm_penalty: !data.free_cancel }) });
              const r = await cc.json();
              if (r.success) {
                if (r.penalty_payment_url) { alert('Suscripción cancelada. Abriendo link de pago...'); window.open(r.penalty_payment_url, '_blank'); }
                else alert('Suscripción cancelada.');
                window.location.reload();
              } else alert(r.message || r.error || 'Error');
            } catch (e) { alert('Error: ' + e.message); }
          });
          btnRow.appendChild(cancelBtn);
          st.appendChild(btnRow);
        }

        box.appendChild(st);
        sec.appendChild(box);
      });
      page.appendChild(sec);
    },
    whatsapp: () => {
      const wa = (cfg.whatsapp_number || '').replace(/\D/g, '');
      if (!wa) return;
      const msg = encodeURIComponent(cfg.whatsapp_message || 'Hola, necesito ayuda.');
      const url = 'https://wa.me/' + wa + '?text=' + msg;
      const sec = document.createElement('s-section');
      const btn = document.createElement('s-button');
      btn.setAttribute('href', url);
      btn.setAttribute('target', 'auto');
      btn.setAttribute('kind', 'secondary');
      btn.textContent = cfg.whatsapp_btn_text || 'Soporte por WhatsApp';
      sec.appendChild(btn);
      page.appendChild(sec);
    }
  };

  order.forEach(key => { if (renderers[key]) renderers[key](); });

  document.body.appendChild(page);
};
