const nodemailer = require('nodemailer');

const transporter = nodemailer.createTransport({
    host: process.env.SMTP_HOST,
    port: parseInt(process.env.SMTP_PORT || '587'),
    secure: false,
    auth: { user: process.env.SMTP_USER, pass: process.env.SMTP_PASS }
});

const FROM = process.env.EMAIL_FROM || '"LAB NUTRITION" <noreply@labnutrition.com>';

/* ─── BASE TEMPLATE — Fondo blanco, letras negras, títulos rojos ─── */
function baseHTML(content) {
    return `<!DOCTYPE html>
<html lang="es">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width">
  <style>
    body { margin:0; padding:0; font-family:'Segoe UI',Helvetica,Arial,sans-serif; background:#f5f5f5; color:#1a1a1a; }
    .wrap { max-width:600px; margin:32px auto; background:#ffffff; border-radius:12px; overflow:hidden; box-shadow:0 2px 16px rgba(0,0,0,0.06); }
    .header { background:#ffffff; padding:28px 32px; border-bottom:2px solid #f0f0f0; text-align:center; }
    .header-brand { color:#9d2a23; font-size:18px; font-weight:900; letter-spacing:3px; text-transform:uppercase; }
    .header-sub { color:#666; font-size:12px; font-weight:600; letter-spacing:1px; margin-top:4px; text-transform:uppercase; }
    .body { padding:28px 32px; color:#1a1a1a; line-height:1.7; font-size:14px; }
    .body h2 { color:#9d2a23; font-size:20px; font-weight:900; margin:0 0 16px 0; }
    .body p { margin:0 0 14px 0; }
    .detail-box { background:#fafafa; border:1px solid #eee; border-radius:8px; padding:16px 20px; margin:20px 0; }
    .detail-row { display:flex; justify-content:space-between; padding:6px 0; font-size:13px; border-bottom:1px solid #f0f0f0; }
    .detail-row:last-child { border-bottom:none; }
    .detail-label { color:#888; }
    .detail-value { font-weight:700; color:#1a1a1a; }
    .total-row { font-size:15px; padding:10px 0 0; border-top:2px solid #9d2a23; margin-top:8px; }
    .total-row .detail-value { color:#9d2a23; font-size:16px; }
    .btn { display:inline-block; background:#9d2a23; color:#ffffff; text-decoration:none; padding:14px 32px; border-radius:8px; font-weight:800; text-transform:uppercase; letter-spacing:0.8px; font-size:13px; margin:20px 0; }
    .alert-box { background:#fff8f0; border-left:4px solid #f59e0b; border-radius:4px; padding:14px 18px; margin:20px 0; font-size:13px; }
    .success-box { background:#f0fdf4; border-left:4px solid #22c55e; border-radius:4px; padding:14px 18px; margin:20px 0; font-size:13px; }
    .muted { color:#888; font-size:12px; }
    .footer { background:#fafafa; padding:20px 32px; text-align:center; font-size:11px; color:#aaa; border-top:1px solid #f0f0f0; }
    .footer a { color:#9d2a23; text-decoration:none; }
  </style>
</head>
<body>
  <div class="wrap">
    <div class="header">
      <div class="header-brand">LAB NUTRITION</div>
      <div class="header-sub">Programa de Suscripción</div>
    </div>
    <div class="body">${content}</div>
    <div class="footer">
      LAB NUTRITION CORP SAC &middot; Lima, Perú<br>
      <a href="https://nutrition-lab-cluster.myshopify.com">labnutrition.pe</a> &middot;
      <a href="mailto:contacto@labnutrition.pe">contacto@labnutrition.pe</a>
    </div>
  </div>
</body>
</html>`;
}

async function sendEmail(to, subject, html) {
    return transporter.sendMail({ from: FROM, to, subject, html });
}

function formatDate(d) {
    if (!d) return 'Próximamente';
    return new Date(d).toLocaleDateString('es-PE', { day: 'numeric', month: 'long', year: 'numeric' });
}

function formatPrice(n) {
    return 'S/ ' + parseFloat(n || 0).toFixed(2);
}

/* ═══════════════════════════════════════════════
   NOTIFICATION FLOWS
═══════════════════════════════════════════════ */

// N1: Bienvenida — alta suscripción
async function sendWelcome(sub) {
    const html = baseHTML(`
    <h2>Bienvenido a tu suscripción</h2>
    <p>Hola <strong>${sub.customer_name || sub.customer_email}</strong>,</p>
    <p>Tu suscripción ha sido activada exitosamente. A partir de ahora recibirás tu producto cada mes en la puerta de tu casa.</p>
    <div class="detail-box">
      <div class="detail-row"><span class="detail-label">Producto</span><span class="detail-value">${sub.product_title}</span></div>
      <div class="detail-row"><span class="detail-label">Frecuencia</span><span class="detail-value">Cada ${sub.frequency_months === 1 ? 'mes' : sub.frequency_months + ' meses'}</span></div>
      <div class="detail-row"><span class="detail-label">Permanencia</span><span class="detail-value">${sub.permanence_months} meses</span></div>
      <div class="detail-row"><span class="detail-label">Descuento</span><span class="detail-value">${Math.round(sub.discount_pct || 0)}% OFF</span></div>
      <div class="detail-row"><span class="detail-label">Precio mensual</span><span class="detail-value">${formatPrice(sub.final_price)}</span></div>
      <div class="detail-row"><span class="detail-label">Envío</span><span class="detail-value">S/ 10.00</span></div>
      <div class="detail-row total-row"><span class="detail-label">Cobro mensual total</span><span class="detail-value">${formatPrice(parseFloat(sub.final_price) + 10)}</span></div>
    </div>
    <div class="success-box">
      <strong>Próximo envío:</strong> ${formatDate(sub.next_charge_at)}
    </div>
    <p class="muted">¿Preguntas? Escríbenos a contacto@labnutrition.pe o por WhatsApp.</p>
  `);
    return sendEmail(sub.customer_email, 'Tu suscripción LAB NUTRITION está activa', html);
}

// N2: Recordatorio 3 días antes — "Tu pedido llega pronto"
async function sendChargeReminder(sub) {
    const addr = sub.shipping_address || {};
    const html = baseHTML(`
    <h2>Tu pedido se procesa en 3 días</h2>
    <p>Hola <strong>${sub.customer_name || sub.customer_email}</strong>,</p>
    <p>Te avisamos que en <strong>3 días</strong> se procesará tu cobro mensual y despacharemos tu producto.</p>
    <div class="detail-box">
      <div class="detail-row"><span class="detail-label">Producto</span><span class="detail-value">${sub.product_title}</span></div>
      <div class="detail-row"><span class="detail-label">Precio</span><span class="detail-value">${formatPrice(sub.final_price)}</span></div>
      <div class="detail-row"><span class="detail-label">Envío</span><span class="detail-value">S/ 10.00</span></div>
      <div class="detail-row total-row"><span class="detail-label">Total a cobrar</span><span class="detail-value">${formatPrice(parseFloat(sub.final_price) + 10)}</span></div>
    </div>
    <div class="detail-box" style="background:#fff">
      <div style="font-size:11px;font-weight:700;color:#9d2a23;text-transform:uppercase;letter-spacing:1px;margin-bottom:8px">Dirección de envío</div>
      <div style="font-size:13px;color:#333">${addr.address1 || 'La registrada en tu cuenta'}</div>
      <div style="font-size:13px;color:#666">${addr.city || ''}${addr.province ? ', ' + addr.province : ''}</div>
    </div>
    <div class="alert-box">
      <strong>Fecha de cobro:</strong> ${formatDate(sub.next_charge_at)}<br>
      <span class="muted">El cobro se realizará automáticamente a tu tarjeta registrada en Mercado Pago.</span>
    </div>
    <p class="muted">Si necesitas cambiar tu dirección de envío, contáctanos antes de la fecha de cobro.</p>
  `);
    return sendEmail(sub.customer_email, 'Tu pedido LAB NUTRITION se procesa en 3 días', html);
}

// N3: Aviso ventana de cancelación -7 días
async function sendCancelLockWarning(sub) {
    const html = baseHTML(`
    <h2>Aviso: próximo cobro en 7 días</h2>
    <p>Hola <strong>${sub.customer_name || sub.customer_email}</strong>,</p>
    <p>Tu próximo cobro de suscripción será en <strong>7 días</strong>.</p>
    <div class="detail-box">
      <div class="detail-row"><span class="detail-label">Producto</span><span class="detail-value">${sub.product_title}</span></div>
      <div class="detail-row"><span class="detail-label">Fecha de cobro</span><span class="detail-value">${formatDate(sub.next_charge_at)}</span></div>
      <div class="detail-row total-row"><span class="detail-label">Monto</span><span class="detail-value">${formatPrice(parseFloat(sub.final_price) + 10)}</span></div>
    </div>
    <p class="muted">Si tienes alguna consulta sobre tu suscripción, escríbenos a contacto@labnutrition.pe</p>
  `);
    return sendEmail(sub.customer_email, 'Aviso: tu cobro LAB NUTRITION es en 7 días', html);
}

// N4: Cobro exitoso — pedido creado
async function sendChargeSuccess(sub, orderName) {
    const cycleMsg = sub.cycles_completed >= sub.cycles_required
        ? 'Completaste tu permanencia. Puedes renovar o cancelar cuando desees.'
        : `Ciclo ${sub.cycles_completed} de ${sub.cycles_required}`;
    const html = baseHTML(`
    <h2>Pago procesado exitosamente</h2>
    <p>Hola <strong>${sub.customer_name || sub.customer_email}</strong>,</p>
    <p>Tu cobro mensual fue procesado correctamente. Tu pedido ya está en camino.</p>
    <div class="detail-box">
      <div class="detail-row"><span class="detail-label">Orden</span><span class="detail-value">${orderName || '#---'}</span></div>
      <div class="detail-row"><span class="detail-label">Producto</span><span class="detail-value">${sub.product_title}</span></div>
      <div class="detail-row"><span class="detail-label">Precio</span><span class="detail-value">${formatPrice(sub.final_price)}</span></div>
      <div class="detail-row"><span class="detail-label">Envío</span><span class="detail-value">S/ 10.00</span></div>
      <div class="detail-row total-row"><span class="detail-label">Total cobrado</span><span class="detail-value">${formatPrice(parseFloat(sub.final_price) + 10)}</span></div>
    </div>
    <div class="success-box">
      <strong>Progreso:</strong> ${cycleMsg}
    </div>
    <p class="muted">Recibirás un email de confirmación de envío cuando tu pedido sea despachado.</p>
  `);
    return sendEmail(sub.customer_email, 'Pago procesado — LAB NUTRITION', html);
}

// N5: Cobro fallido
async function sendChargeFailed(sub) {
    const html = baseHTML(`
    <h2>Problema con tu pago</h2>
    <p>Hola <strong>${sub.customer_name || sub.customer_email}</strong>,</p>
    <p>No pudimos procesar el pago de tu suscripción a <strong>${sub.product_title}</strong>.</p>
    <div class="alert-box" style="border-color:#ef4444;background:#fef2f2">
      <strong>Acción requerida:</strong> Actualiza tu método de pago para que podamos procesar tu pedido. Si no se actualiza en 48 horas, tu suscripción podría pausarse.
    </div>
    <div class="detail-box">
      <div class="detail-row"><span class="detail-label">Monto pendiente</span><span class="detail-value">${formatPrice(parseFloat(sub.final_price) + 10)}</span></div>
      <div class="detail-row"><span class="detail-label">Producto</span><span class="detail-value">${sub.product_title}</span></div>
    </div>
    <p>Contáctanos por WhatsApp o email para resolver este problema lo antes posible.</p>
    <p class="muted">contacto@labnutrition.pe</p>
  `);
    return sendEmail(sub.customer_email, 'Acción requerida: problema con tu pago — LAB NUTRITION', html);
}

// N6: Fin de permanencia — renovación
async function sendRenewalInvite(sub) {
    const html = baseHTML(`
    <h2>Completaste tu permanencia</h2>
    <p>Hola <strong>${sub.customer_name || sub.customer_email}</strong>,</p>
    <p>Has completado tus <strong>${sub.permanence_months} meses</strong> de suscripción a <strong>${sub.product_title}</strong>.</p>
    <div class="success-box">
      <strong>Tu compromiso fue cumplido.</strong> Ahora puedes cancelar sin restricciones, o continuar disfrutando de tu descuento.
    </div>
    <div class="detail-box">
      <div class="detail-row"><span class="detail-label">Meses completados</span><span class="detail-value">${sub.permanence_months}</span></div>
      <div class="detail-row"><span class="detail-label">Tu descuento</span><span class="detail-value">${Math.round(sub.discount_pct || 0)}% OFF</span></div>
      <div class="detail-row"><span class="detail-label">Precio mensual</span><span class="detail-value">${formatPrice(sub.final_price)}</span></div>
    </div>
    <p>Si deseas cancelar, contáctanos. Si no haces nada, tu suscripción continuará activa con el mismo descuento.</p>
    <p class="muted">Gracias por tu confianza. — Equipo LAB NUTRITION</p>
  `);
    return sendEmail(sub.customer_email, 'Permanencia completada — LAB NUTRITION', html);
}

// N7: Confirmación de cancelación
async function sendCancellationConfirmation(sub) {
    const html = baseHTML(`
    <h2>Tu suscripción ha sido cancelada</h2>
    <p>Hola <strong>${sub.customer_name || sub.customer_email}</strong>,</p>
    <p>Confirmamos que tu suscripción a <strong>${sub.product_title}</strong> ha sido cancelada.</p>
    <div class="detail-box">
      <div class="detail-row"><span class="detail-label">Producto</span><span class="detail-value">${sub.product_title}</span></div>
      <div class="detail-row"><span class="detail-label">Ciclos completados</span><span class="detail-value">${sub.cycles_completed || 0} de ${sub.cycles_required || '?'}</span></div>
      <div class="detail-row"><span class="detail-label">Fecha de cancelación</span><span class="detail-value">${formatDate(new Date())}</span></div>
    </div>
    <p>No se realizarán más cobros. Si deseas volver a suscribirte en el futuro, visita nuestra tienda.</p>
    <p class="muted">Gracias por haber sido parte del programa. — Equipo LAB NUTRITION</p>
  `);
    return sendEmail(sub.customer_email, 'Suscripción cancelada — LAB NUTRITION', html);
}

module.exports = {
    sendWelcome,
    sendChargeReminder,
    sendCancelLockWarning,
    sendChargeSuccess,
    sendChargeFailed,
    sendRenewalInvite,
    sendCancellationConfirmation
};
