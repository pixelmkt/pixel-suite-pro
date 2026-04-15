const nodemailer = require('nodemailer');

const smtpPort = parseInt(process.env.SMTP_PORT || '465');
const transporter = nodemailer.createTransport({
    host: process.env.SMTP_HOST,
    port: smtpPort,
    secure: smtpPort === 465,
    auth: { user: process.env.SMTP_USER, pass: process.env.SMTP_PASS },
    connectionTimeout: 10000,
    greetingTimeout: 10000,
    socketTimeout: 15000
});

const FROM = `"${process.env.EMAIL_FROM || 'LAB NUTRITION'}" <${process.env.SMTP_USER || 'marketing@labnutrition.com'}>`;

// ── RESEND (HTTP API, puerto 443 — Railway no lo bloquea) ──────────
// Se prefiere Resend si existe RESEND_API_KEY. Fallback: nodemailer (SMTP).
// El dominio labnutrition.com debe estar verificado en Resend para usar un FROM
// distinto de `onboarding@resend.dev` (sandbox por default).
async function sendViaResend(to, subject, html, fromOverride) {
    const key = process.env.RESEND_API_KEY;
    if (!key) throw new Error('RESEND_API_KEY not set');
    const from = fromOverride
        || process.env.RESEND_FROM
        || 'LAB NUTRITION <onboarding@resend.dev>';
    const res = await fetch('https://api.resend.com/emails', {
        method: 'POST',
        headers: {
            Authorization: 'Bearer ' + key,
            'Content-Type': 'application/json'
        },
        body: JSON.stringify({ from, to, subject, html })
    });
    const j = await res.json().catch(() => ({}));
    if (!res.ok) {
        const msg = j?.message || j?.error || ('Resend HTTP ' + res.status);
        throw new Error('[Resend] ' + msg);
    }
    return j; // { id: 're_xxx' }
}

/* ─── BASE TEMPLATE — Club Black Diamond Premium Design ─── */
function baseHTML(content, { headerIcon = '', headerTitle = '' } = {}) {
    return `<!DOCTYPE html>
<html lang="es">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width">
  <style>
    body { margin:0; padding:0; font-family:'Segoe UI',Helvetica,Arial,sans-serif; background:#f5f5f5; color:#1a1a1a; -webkit-font-smoothing:antialiased; }
    .outer { max-width:600px; margin:0 auto; padding:24px 16px; }
    .card { background:#ffffff; border-radius:16px; overflow:hidden; box-shadow:0 2px 20px rgba(0,0,0,0.06); }
    .hero { background:#1a1a1a; padding:36px 32px 28px; text-align:center; }
    .hero-diamond { font-size:28px; letter-spacing:6px; margin-bottom:8px; }
    .hero-brand { color:#ffffff; font-size:11px; font-weight:800; letter-spacing:4px; text-transform:uppercase; }
    .hero-title { color:#9d2a23; font-size:22px; font-weight:900; margin-top:12px; letter-spacing:0.5px; }
    .hero-sub { color:#888; font-size:12px; margin-top:6px; letter-spacing:1px; text-transform:uppercase; }
    .body { padding:32px; color:#1a1a1a; line-height:1.7; font-size:14px; }
    .body h2 { color:#9d2a23; font-size:20px; font-weight:900; margin:0 0 16px 0; letter-spacing:-0.3px; }
    .body p { margin:0 0 14px 0; color:#333; }
    .detail-box { background:#fafafa; border:1px solid #eee; border-radius:12px; padding:20px; margin:20px 0; }
    .detail-row { display:flex; justify-content:space-between; padding:8px 0; font-size:13px; border-bottom:1px solid #f0f0f0; }
    .detail-row:last-child { border-bottom:none; }
    .detail-label { color:#888; }
    .detail-value { font-weight:700; color:#1a1a1a; text-align:right; }
    .igv-note { font-size:10px; color:#888; text-align:right; margin-top:-4px; padding-bottom:6px; font-style:italic; }
    .total-row { font-size:15px; padding:12px 0 0; border-top:2px solid #1a1a1a; margin-top:8px; }
    .total-row .detail-value { color:#9d2a23; font-size:17px; font-weight:900; }
    .badge { display:inline-block; background:#9d2a23; color:#fff; padding:5px 14px; border-radius:20px; font-size:11px; font-weight:800; letter-spacing:0.5px; }
    .badge-dark { background:#1a1a1a; }
    .badge-green { background:#16a34a; }
    .info-box { background:#f8f8f8; border-left:3px solid #1a1a1a; border-radius:0 8px 8px 0; padding:14px 18px; margin:20px 0; font-size:13px; color:#444; }
    .alert-box { background:#fffbeb; border-left:3px solid #f59e0b; border-radius:0 8px 8px 0; padding:14px 18px; margin:20px 0; font-size:13px; color:#92400e; }
    .success-box { background:#f0fdf4; border-left:3px solid #22c55e; border-radius:0 8px 8px 0; padding:14px 18px; margin:20px 0; font-size:13px; color:#166534; }
    .error-box { background:#fef2f2; border-left:3px solid #ef4444; border-radius:0 8px 8px 0; padding:14px 18px; margin:20px 0; font-size:13px; color:#991b1b; }
    .btn { display:inline-block; background:#9d2a23; color:#ffffff; text-decoration:none; padding:14px 36px; border-radius:8px; font-weight:800; text-transform:uppercase; letter-spacing:1px; font-size:12px; }
    .btn-dark { background:#1a1a1a; }
    .divider { height:1px; background:#f0f0f0; margin:24px 0; }
    .muted { color:#999; font-size:12px; }
    .footer { padding:24px 32px; text-align:center; }
    .footer-line { height:2px; background:linear-gradient(90deg, transparent, #9d2a23, transparent); margin-bottom:20px; }
    .footer-brand { color:#1a1a1a; font-size:10px; font-weight:800; letter-spacing:3px; text-transform:uppercase; }
    .footer-links { margin-top:8px; font-size:11px; color:#999; }
    .footer-links a { color:#9d2a23; text-decoration:none; font-weight:600; }
  </style>
</head>
<body>
  <div class="outer">
    <div class="card">
      <div class="hero">
        <div class="hero-diamond">&#9670; &#9670; &#9670;</div>
        <div class="hero-brand">LAB NUTRITION</div>
        <div class="hero-title">${headerTitle || 'Club Black Diamond'}</div>
        ${headerIcon ? '<div class="hero-sub">' + headerIcon + '</div>' : ''}
      </div>
      <div class="body">${content}</div>
      <div class="footer">
        <div class="footer-line"></div>
        <div class="footer-brand">LAB NUTRITION CORP SAC</div>
        <div class="footer-links">
          Lima, Per&uacute; &middot; <a href="https://labnutrition.pe">labnutrition.pe</a> &middot; <a href="mailto:contacto@labnutrition.pe">contacto@labnutrition.pe</a>
        </div>
      </div>
    </div>
  </div>
</body>
</html>`;
}

async function sendEmail(to, subject, html) {
    // Preferir Resend (HTTP) si está configurado → evita bloqueo SMTP de Railway.
    if (process.env.RESEND_API_KEY) {
        try {
            return await sendViaResend(to, subject, html);
        } catch (e) {
            console.warn('[EMAIL] Resend falló, intento SMTP fallback:', e.message);
            // cae al SMTP más abajo
        }
    }
    return transporter.sendMail({ from: FROM, to, subject, html });
}

/* ═══════════════════════════════════════════════
   AUTOMATIONS — overrides editables desde admin
   (lee metafield lab_app/automations, cachea 60s)
═══════════════════════════════════════════════ */
let _cachedAutomations = null;
let _cachedAutomationsAt = 0;

async function _readAutomationsMF() {
    const shop = process.env.SHOPIFY_SHOP || 'nutrition-lab-cluster.myshopify.com';
    const token = process.env.SHOPIFY_ACCESS_TOKEN;
    if (!token) return {};
    try {
        const r = await fetch(
            `https://${shop}/admin/api/2026-01/metafields.json?metafield[owner_resource]=shop&metafield[namespace]=lab_app&metafield[key]=automations`,
            { headers: { 'X-Shopify-Access-Token': token } }
        );
        if (!r.ok) return {};
        const data = await r.json();
        const mf = data.metafields?.[0];
        if (mf?.value) { try { return JSON.parse(mf.value) || {}; } catch { return {}; } }
    } catch { /* ignore */ }
    return {};
}

async function loadAutomations(force) {
    if (!force && _cachedAutomations && (Date.now() - _cachedAutomationsAt) < 60000) return _cachedAutomations;
    _cachedAutomations = await _readAutomationsMF();
    _cachedAutomationsAt = Date.now();
    return _cachedAutomations;
}

function invalidateAutomationsCache() {
    _cachedAutomations = null;
    _cachedAutomationsAt = 0;
}

function renderVars(str, sub, extras) {
    if (!str) return '';
    const firstN = ((sub && (sub.customer_name || sub.customer_email)) || '').split(' ')[0] || '';
    const fp = sub && sub.final_price ? parseFloat(sub.final_price) : 0;
    const totalN = fp ? (fp + 10).toFixed(2) : '';
    const vars = {
        first_name: firstN,
        name: (sub && sub.customer_name) || firstN,
        email: (sub && sub.customer_email) || '',
        product: (sub && sub.product_title) || '',
        final_price: fp ? 'S/ ' + fp.toFixed(2) : '',
        shipping: 'S/ 10.00',
        total: totalN ? 'S/ ' + totalN : '',
        next_charge: (sub && sub.next_charge_at) ? formatDate(sub.next_charge_at) : 'Pr\u00f3ximamente',
        cycles_completed: String((sub && sub.cycles_completed) || 0),
        cycles_required: String((sub && sub.cycles_required) || 0),
        permanence_months: String((sub && sub.permanence_months) || 0),
        discount_pct: String(Math.round((sub && sub.discount_pct) || 0)),
        portal_link: 'https://labnutrition.pe/pages/mi-suscripcion?email=' + encodeURIComponent((sub && sub.customer_email) || ''),
        ...(extras || {})
    };
    return String(str).replace(/\{\{\s*(\w+)\s*\}\}/g, function (_, k) { return vars[k] !== undefined ? vars[k] : ''; });
}

/**
 * Si existe override para `name`, lo renderiza y devuelve { subject, html }.
 * Si no, devuelve null (y el caller usa su hardcoded). Si está explícitamente
 * deshabilitado por el admin, devuelve 'DISABLED' para NO enviar.
 */
async function resolveOverride(name, sub, defaultHeaderTitle, extras) {
    const all = await loadAutomations();
    const tpl = all && all[name];
    if (!tpl) return null;
    if (tpl.enabled === false) return 'DISABLED';
    const subject = tpl.subject ? renderVars(tpl.subject, sub, extras) : null;
    const bodyRaw = tpl.body ? renderVars(tpl.body, sub, extras) : null;
    if (!subject && !bodyRaw) return null;
    const header = tpl.header_title || defaultHeaderTitle;
    // Si body incluye HTML crudo, respetarlo. Si es texto plano, convertir saltos.
    const bodyHtml = bodyRaw ? (/<[a-z][\s\S]*>/i.test(bodyRaw) ? bodyRaw : bodyRaw.replace(/\n/g, '<br>')) : null;
    const html = bodyHtml ? baseHTML(bodyHtml, { headerTitle: header }) : null;
    return { subject, html };
}

function formatDate(d) {
    if (!d) return 'Pr\u00f3ximamente';
    return new Date(d).toLocaleDateString('es-PE', { day: 'numeric', month: 'long', year: 'numeric' });
}

function formatPrice(n) {
    return 'S/ ' + parseFloat(n || 0).toFixed(2);
}

function firstName(sub) {
    return (sub.customer_name || sub.customer_email || '').split(' ')[0];
}

/* ═══════════════════════════════════════════════
   NOTIFICATION FLOWS — Club Black Diamond
═══════════════════════════════════════════════ */

// N1: Bienvenida — "Bienvenido al Club Black Diamond"
async function sendWelcome(sub) {
    const ov = await resolveOverride('welcome', sub, 'Bienvenido al Club Black Diamond');
    if (ov === 'DISABLED') return;
    if (ov && ov.html) return sendEmail(sub.customer_email, ov.subject || '\u25c6 Bienvenido al Club Black Diamond \u2014 LAB NUTRITION', ov.html);
    const html = baseHTML(`
    <h2>Bienvenido al Club Black Diamond</h2>
    <p>Hola <strong>${firstName(sub)}</strong>,</p>
    <p>Tu suscripci&oacute;n ha sido activada. Ahora eres parte del programa exclusivo de LAB NUTRITION. Cada mes recibir&aacute;s tu producto en la puerta de tu casa con un descuento que nadie m&aacute;s tiene.</p>
    <div class="detail-box">
      <div class="detail-row"><span class="detail-label">Producto</span><span class="detail-value">${sub.product_title}</span></div>
      <div class="detail-row"><span class="detail-label">Plan</span><span class="detail-value">${sub.frequency_months === 1 ? 'Mensual' : 'Cada ' + sub.frequency_months + ' meses'} &middot; ${sub.permanence_months} meses</span></div>
      <div class="detail-row"><span class="detail-label">Tu descuento</span><span class="detail-value"><span class="badge">${Math.round(sub.discount_pct || 0)}% OFF</span></span></div>
      <div class="detail-row"><span class="detail-label">Precio mensual</span><span class="detail-value">${formatPrice(sub.final_price)}</span></div>
      <div class="detail-row"><span class="detail-label">Env&iacute;o</span><span class="detail-value">S/ 10.00</span></div>
      <div class="igv-note">Todos los precios incluyen IGV</div>
      <div class="detail-row total-row"><span class="detail-label">Cobro mensual</span><span class="detail-value">${formatPrice(parseFloat(sub.final_price) + 10)}</span></div>
    </div>
    <div class="success-box">
      <strong>Pr&oacute;ximo env&iacute;o:</strong> ${formatDate(sub.next_charge_at)}
    </div>
    <div class="divider"></div>
    <p class="muted" style="text-align:center">Bienvenido al club. &mdash; Equipo LAB NUTRITION</p>
  `, { headerTitle: 'Bienvenido al Club Black Diamond', headerIcon: 'Programa de Suscripci\u00f3n Exclusivo' });
    return sendEmail(sub.customer_email, '&#9670; Bienvenido al Club Black Diamond — LAB NUTRITION', html);
}

// N2: Recordatorio 3 días antes
async function sendChargeReminder(sub) {
    const ov = await resolveOverride('charge_reminder', sub, 'Tu pedido est\u00e1 por llegar');
    if (ov === 'DISABLED') return;
    if (ov && ov.html) return sendEmail(sub.customer_email, ov.subject || 'Tu pedido LAB NUTRITION se procesa en 3 d\u00edas', ov.html);
    const addr = sub.shipping_address || {};
    const html = baseHTML(`
    <h2>Tu pedido se procesa en 3 d&iacute;as</h2>
    <p>Hola <strong>${firstName(sub)}</strong>,</p>
    <p>En <strong>3 d&iacute;as</strong> se procesar&aacute; tu cobro mensual y despacharemos tu producto.</p>
    <div class="detail-box">
      <div class="detail-row"><span class="detail-label">Producto</span><span class="detail-value">${sub.product_title}</span></div>
      <div class="detail-row"><span class="detail-label">Precio</span><span class="detail-value">${formatPrice(sub.final_price)}</span></div>
      <div class="detail-row"><span class="detail-label">Env&iacute;o</span><span class="detail-value">S/ 10.00</span></div>
      <div class="igv-note">Todos los precios incluyen IGV</div>
      <div class="detail-row total-row"><span class="detail-label">Total a cobrar</span><span class="detail-value">${formatPrice(parseFloat(sub.final_price) + 10)}</span></div>
    </div>
    <div class="info-box">
      <strong>Direcci&oacute;n de env&iacute;o</strong><br>
      ${addr.address1 || 'La registrada en tu cuenta'}<br>
      ${addr.city || ''}${addr.province ? ', ' + addr.province : ''}
    </div>
    <div class="alert-box">
      <strong>Fecha de cobro:</strong> ${formatDate(sub.next_charge_at)}<br>
      El cobro se realiza autom&aacute;ticamente a tu tarjeta registrada en Mercado Pago.
    </div>
    <p class="muted">Si necesitas cambiar tu direcci&oacute;n, cont&aacute;ctanos antes de la fecha de cobro.</p>
  `, { headerTitle: 'Tu pedido est&aacute; por llegar', headerIcon: 'Club Black Diamond' });
    return sendEmail(sub.customer_email, 'Tu pedido LAB NUTRITION se procesa en 3 d\u00edas', html);
}

// N3: Aviso 7 días
async function sendCancelLockWarning(sub) {
    const ov = await resolveOverride('cancel_lock_warning', sub, 'Aviso de cobro');
    if (ov === 'DISABLED') return;
    if (ov && ov.html) return sendEmail(sub.customer_email, ov.subject || 'Aviso: tu cobro LAB NUTRITION es en 7 d\u00edas', ov.html);
    const html = baseHTML(`
    <h2>Pr&oacute;ximo cobro en 7 d&iacute;as</h2>
    <p>Hola <strong>${firstName(sub)}</strong>,</p>
    <p>Tu pr&oacute;ximo cobro de suscripci&oacute;n ser&aacute; en <strong>7 d&iacute;as</strong>.</p>
    <div class="detail-box">
      <div class="detail-row"><span class="detail-label">Producto</span><span class="detail-value">${sub.product_title}</span></div>
      <div class="detail-row"><span class="detail-label">Fecha de cobro</span><span class="detail-value">${formatDate(sub.next_charge_at)}</span></div>
      <div class="detail-row total-row"><span class="detail-label">Monto</span><span class="detail-value">${formatPrice(parseFloat(sub.final_price) + 10)}</span></div>
    </div>
    <p class="muted">Si tienes alguna consulta, escr&iacute;benos a contacto@labnutrition.pe</p>
  `, { headerTitle: 'Aviso de cobro', headerIcon: 'Club Black Diamond' });
    return sendEmail(sub.customer_email, 'Aviso: tu cobro LAB NUTRITION es en 7 d\u00edas', html);
}

// N4: Cobro exitoso
async function sendChargeSuccess(sub, orderName) {
    const ov = await resolveOverride('charge_success', sub, 'Pago confirmado', { order_name: orderName || '#---' });
    if (ov === 'DISABLED') return;
    if (ov && ov.html) return sendEmail(sub.customer_email, ov.subject || '\u25c6 Pago procesado \u2014 LAB NUTRITION', ov.html);
    const cycleMsg = sub.cycles_completed >= sub.cycles_required
        ? 'Completaste tu permanencia. Puedes renovar o cancelar cuando desees.'
        : 'Ciclo ' + sub.cycles_completed + ' de ' + sub.cycles_required;
    const html = baseHTML(`
    <h2>Pago procesado</h2>
    <p>Hola <strong>${firstName(sub)}</strong>,</p>
    <p>Tu cobro mensual fue procesado correctamente. Tu pedido ya est&aacute; en camino.</p>
    <div class="detail-box">
      <div class="detail-row"><span class="detail-label">Orden</span><span class="detail-value">${orderName || '#---'}</span></div>
      <div class="detail-row"><span class="detail-label">Producto</span><span class="detail-value">${sub.product_title}</span></div>
      <div class="detail-row"><span class="detail-label">Precio</span><span class="detail-value">${formatPrice(sub.final_price)}</span></div>
      <div class="detail-row"><span class="detail-label">Env&iacute;o</span><span class="detail-value">S/ 10.00</span></div>
      <div class="igv-note">Todos los precios incluyen IGV</div>
      <div class="detail-row total-row"><span class="detail-label">Total cobrado</span><span class="detail-value">${formatPrice(parseFloat(sub.final_price) + 10)}</span></div>
    </div>
    <div class="success-box">
      <strong>Progreso:</strong> ${cycleMsg}
    </div>
    <p class="muted">Recibir&aacute;s un email de confirmaci&oacute;n cuando tu pedido sea despachado.</p>
  `, { headerTitle: 'Pago confirmado', headerIcon: 'Club Black Diamond' });
    return sendEmail(sub.customer_email, '&#9670; Pago procesado — LAB NUTRITION', html);
}

// N5: Cobro fallido
async function sendChargeFailed(sub) {
    const ov = await resolveOverride('charge_failed', sub, 'Acci\u00f3n requerida');
    if (ov === 'DISABLED') return;
    if (ov && ov.html) return sendEmail(sub.customer_email, ov.subject || 'Acci\u00f3n requerida: problema con tu pago \u2014 LAB NUTRITION', ov.html);
    const html = baseHTML(`
    <h2>Problema con tu pago</h2>
    <p>Hola <strong>${firstName(sub)}</strong>,</p>
    <p>No pudimos procesar el pago de tu suscripci&oacute;n a <strong>${sub.product_title}</strong>.</p>
    <div class="error-box">
      <strong>Acci&oacute;n requerida:</strong> Actualiza tu m&eacute;todo de pago para que podamos procesar tu pedido. Si no se actualiza en 48 horas, tu suscripci&oacute;n podr&iacute;a pausarse.
    </div>
    <div class="detail-box">
      <div class="detail-row"><span class="detail-label">Monto pendiente</span><span class="detail-value">${formatPrice(parseFloat(sub.final_price) + 10)}</span></div>
      <div class="detail-row"><span class="detail-label">Producto</span><span class="detail-value">${sub.product_title}</span></div>
    </div>
    <p>Cont&aacute;ctanos por WhatsApp o email para resolver este problema.</p>
  `, { headerTitle: 'Acci&oacute;n requerida', headerIcon: 'Club Black Diamond' });
    return sendEmail(sub.customer_email, 'Acci\u00f3n requerida: problema con tu pago — LAB NUTRITION', html);
}

// N6: Permanencia completada
async function sendRenewalInvite(sub) {
    const ov = await resolveOverride('renewal_invite', sub, 'Felicitaciones');
    if (ov === 'DISABLED') return;
    if (ov && ov.html) return sendEmail(sub.customer_email, ov.subject || '\u25c6 Permanencia completada \u2014 LAB NUTRITION', ov.html);
    const html = baseHTML(`
    <h2>Permanencia completada</h2>
    <p>Hola <strong>${firstName(sub)}</strong>,</p>
    <p>Has completado tus <strong>${sub.permanence_months} meses</strong> de suscripci&oacute;n a <strong>${sub.product_title}</strong>.</p>
    <div class="success-box">
      <strong>Tu compromiso fue cumplido.</strong> Ahora puedes cancelar sin restricciones, o continuar disfrutando de tu descuento exclusivo.
    </div>
    <div class="detail-box">
      <div class="detail-row"><span class="detail-label">Meses completados</span><span class="detail-value">${sub.permanence_months}</span></div>
      <div class="detail-row"><span class="detail-label">Tu descuento</span><span class="detail-value"><span class="badge">${Math.round(sub.discount_pct || 0)}% OFF</span></span></div>
      <div class="detail-row"><span class="detail-label">Precio mensual</span><span class="detail-value">${formatPrice(sub.final_price)}</span></div>
    </div>
    <p>Si no haces nada, tu suscripci&oacute;n contin&uacute;a activa con el mismo descuento.</p>
    <div class="divider"></div>
    <p class="muted" style="text-align:center">Gracias por tu confianza. &mdash; Equipo LAB NUTRITION</p>
  `, { headerTitle: 'Felicitaciones', headerIcon: 'Club Black Diamond' });
    return sendEmail(sub.customer_email, '&#9670; Permanencia completada — LAB NUTRITION', html);
}

// N7: Cancelación confirmada
async function sendCancellationConfirmation(sub) {
    const ov = await resolveOverride('cancellation_confirmation', sub, 'Hasta pronto');
    if (ov === 'DISABLED') return;
    if (ov && ov.html) return sendEmail(sub.customer_email, ov.subject || 'Suscripci\u00f3n cancelada \u2014 LAB NUTRITION', ov.html);
    const html = baseHTML(`
    <h2>Suscripci&oacute;n cancelada</h2>
    <p>Hola <strong>${firstName(sub)}</strong>,</p>
    <p>Confirmamos que tu suscripci&oacute;n a <strong>${sub.product_title}</strong> ha sido cancelada.</p>
    <div class="detail-box">
      <div class="detail-row"><span class="detail-label">Producto</span><span class="detail-value">${sub.product_title}</span></div>
      <div class="detail-row"><span class="detail-label">Ciclos completados</span><span class="detail-value">${sub.cycles_completed || 0} de ${sub.cycles_required || '?'}</span></div>
      <div class="detail-row"><span class="detail-label">Fecha de cancelaci&oacute;n</span><span class="detail-value">${formatDate(new Date())}</span></div>
    </div>
    <p>No se realizar&aacute;n m&aacute;s cobros. Si deseas volver a suscribirte, visita nuestra tienda.</p>
    <div class="divider"></div>
    <p class="muted" style="text-align:center">Gracias por haber sido parte del Club. &mdash; Equipo LAB NUTRITION</p>
  `, { headerTitle: 'Hasta pronto', headerIcon: 'Club Black Diamond' });
    return sendEmail(sub.customer_email, 'Suscripci\u00f3n cancelada — LAB NUTRITION', html);
}

/* ─── PREVIEW: genera HTML de todas las plantillas para visualizar ─── */
function getPreviewHTML(templateName) {
    const mockSub = {
        customer_name: 'Jorge Luis Torres Morales',
        customer_email: 'ejemplo@email.com',
        product_title: 'CREATINE MICRONIZED BLACK LIMITED EDITION',
        frequency_months: 1,
        permanence_months: 6,
        discount_pct: 50,
        base_price: 179,
        final_price: 90,
        cycles_completed: 2,
        cycles_required: 6,
        next_charge_at: new Date(Date.now() + 3 * 86400000).toISOString(),
        shipping_address: { address1: 'Augusto Tamayo 180', city: 'San Isidro', province: 'Lima' }
    };
    const templates = {
        welcome: () => baseHTML(`
    <h2>Bienvenido al Club Black Diamond</h2>
    <p>Hola <strong>${firstName(mockSub)}</strong>,</p>
    <p>Tu suscripci&oacute;n ha sido activada. Ahora eres parte del programa exclusivo de LAB NUTRITION. Cada mes recibir&aacute;s tu producto en la puerta de tu casa con un descuento que nadie m&aacute;s tiene.</p>
    <div class="detail-box">
      <div class="detail-row"><span class="detail-label">Producto</span><span class="detail-value">${mockSub.product_title}</span></div>
      <div class="detail-row"><span class="detail-label">Plan</span><span class="detail-value">Mensual &middot; ${mockSub.permanence_months} meses</span></div>
      <div class="detail-row"><span class="detail-label">Tu descuento</span><span class="detail-value"><span class="badge">${mockSub.discount_pct}% OFF</span></span></div>
      <div class="detail-row"><span class="detail-label">Precio mensual</span><span class="detail-value">${formatPrice(mockSub.final_price)}</span></div>
      <div class="detail-row"><span class="detail-label">Env&iacute;o</span><span class="detail-value">S/ 10.00</span></div>
      <div class="igv-note">Todos los precios incluyen IGV</div>
      <div class="detail-row total-row"><span class="detail-label">Cobro mensual</span><span class="detail-value">${formatPrice(parseFloat(mockSub.final_price) + 10)}</span></div>
    </div>
    <div class="success-box"><strong>Pr&oacute;ximo env&iacute;o:</strong> ${formatDate(mockSub.next_charge_at)}</div>
    <div class="divider"></div>
    <p class="muted" style="text-align:center">Bienvenido al club. &mdash; Equipo LAB NUTRITION</p>
  `, { headerTitle: 'Bienvenido al Club Black Diamond', headerIcon: 'Programa de Suscripci\u00f3n Exclusivo' }),

        charge_reminder: () => baseHTML(`
    <h2>Tu pedido se procesa en 3 d&iacute;as</h2>
    <p>Hola <strong>${firstName(mockSub)}</strong>,</p>
    <p>En <strong>3 d&iacute;as</strong> se procesar&aacute; tu cobro mensual y despacharemos tu producto.</p>
    <div class="detail-box">
      <div class="detail-row"><span class="detail-label">Producto</span><span class="detail-value">${mockSub.product_title}</span></div>
      <div class="detail-row"><span class="detail-label">Precio</span><span class="detail-value">${formatPrice(mockSub.final_price)}</span></div>
      <div class="detail-row"><span class="detail-label">Env&iacute;o</span><span class="detail-value">S/ 10.00</span></div>
      <div class="igv-note">Todos los precios incluyen IGV</div>
      <div class="detail-row total-row"><span class="detail-label">Total a cobrar</span><span class="detail-value">${formatPrice(parseFloat(mockSub.final_price) + 10)}</span></div>
    </div>
    <div class="info-box"><strong>Direcci&oacute;n de env&iacute;o</strong><br>Augusto Tamayo 180<br>San Isidro, Lima</div>
    <div class="alert-box"><strong>Fecha de cobro:</strong> ${formatDate(mockSub.next_charge_at)}</div>
  `, { headerTitle: 'Tu pedido est&aacute; por llegar', headerIcon: 'Club Black Diamond' }),

        charge_success: () => baseHTML(`
    <h2>Pago procesado</h2>
    <p>Hola <strong>${firstName(mockSub)}</strong>,</p>
    <p>Tu cobro mensual fue procesado correctamente. Tu pedido ya est&aacute; en camino.</p>
    <div class="detail-box">
      <div class="detail-row"><span class="detail-label">Orden</span><span class="detail-value">#8231</span></div>
      <div class="detail-row"><span class="detail-label">Producto</span><span class="detail-value">${mockSub.product_title}</span></div>
      <div class="detail-row"><span class="detail-label">Precio</span><span class="detail-value">${formatPrice(mockSub.final_price)}</span></div>
      <div class="detail-row"><span class="detail-label">Env&iacute;o</span><span class="detail-value">S/ 10.00</span></div>
      <div class="igv-note">Todos los precios incluyen IGV</div>
      <div class="detail-row total-row"><span class="detail-label">Total cobrado</span><span class="detail-value">${formatPrice(parseFloat(mockSub.final_price) + 10)}</span></div>
    </div>
    <div class="success-box"><strong>Progreso:</strong> Ciclo 2 de 6</div>
  `, { headerTitle: 'Pago confirmado', headerIcon: 'Club Black Diamond' }),

        charge_failed: () => baseHTML(`
    <h2>Problema con tu pago</h2>
    <p>Hola <strong>${firstName(mockSub)}</strong>,</p>
    <p>No pudimos procesar el pago de tu suscripci&oacute;n a <strong>${mockSub.product_title}</strong>.</p>
    <div class="error-box"><strong>Acci&oacute;n requerida:</strong> Actualiza tu m&eacute;todo de pago en las pr&oacute;ximas 48 horas.</div>
    <div class="detail-box">
      <div class="detail-row"><span class="detail-label">Monto pendiente</span><span class="detail-value">${formatPrice(parseFloat(mockSub.final_price) + 10)}</span></div>
    </div>
  `, { headerTitle: 'Acci&oacute;n requerida', headerIcon: 'Club Black Diamond' }),

        renewal: () => baseHTML(`
    <h2>Permanencia completada</h2>
    <p>Hola <strong>${firstName(mockSub)}</strong>,</p>
    <p>Has completado tus <strong>6 meses</strong> de suscripci&oacute;n. Tu compromiso fue cumplido.</p>
    <div class="success-box"><strong>Ahora puedes cancelar sin restricciones</strong> o continuar con tu descuento exclusivo.</div>
    <div class="detail-box">
      <div class="detail-row"><span class="detail-label">Tu descuento</span><span class="detail-value"><span class="badge">50% OFF</span></span></div>
      <div class="detail-row"><span class="detail-label">Precio mensual</span><span class="detail-value">${formatPrice(mockSub.final_price)}</span></div>
    </div>
  `, { headerTitle: 'Felicitaciones', headerIcon: 'Club Black Diamond' }),

        cancellation: () => baseHTML(`
    <h2>Suscripci&oacute;n cancelada</h2>
    <p>Hola <strong>${firstName(mockSub)}</strong>,</p>
    <p>Confirmamos la cancelaci&oacute;n de tu suscripci&oacute;n a <strong>${mockSub.product_title}</strong>.</p>
    <div class="detail-box">
      <div class="detail-row"><span class="detail-label">Ciclos completados</span><span class="detail-value">2 de 6</span></div>
      <div class="detail-row"><span class="detail-label">Fecha</span><span class="detail-value">${formatDate(new Date())}</span></div>
    </div>
    <p>No se realizar&aacute;n m&aacute;s cobros.</p>
    <div class="divider"></div>
    <p class="muted" style="text-align:center">Gracias por haber sido parte del Club. &mdash; Equipo LAB NUTRITION</p>
  `, { headerTitle: 'Hasta pronto', headerIcon: 'Club Black Diamond' })
    };

    if (templateName && templates[templateName]) return templates[templateName]();
    return templates;
}

async function sendTestEmail(toEmail, templateName) {
    const html = typeof getPreviewHTML === 'function' ? getPreviewHTML(templateName || 'welcome') : getPreviewHTML;
    return sendEmail(toEmail, '✅ TEST — Club Black Diamond — LAB NUTRITION', typeof html === 'string' ? html : 'Template not found');
}

module.exports = {
    sendWelcome,
    sendChargeReminder,
    sendCancelLockWarning,
    sendChargeSuccess,
    sendChargeFailed,
    sendRenewalInvite,
    sendCancellationConfirmation,
    getPreviewHTML,
    sendTestEmail,
    // expuestos para server.js (mailing bulk + preview builder)
    sendViaResend,
    sendEmail,
    __baseHTML: baseHTML,
    // automations — overrides editables
    loadAutomations,
    invalidateAutomationsCache,
    renderVars,
    resolveOverride
};
