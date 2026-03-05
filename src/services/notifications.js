const nodemailer = require('nodemailer');

const transporter = nodemailer.createTransport({
    host: process.env.SMTP_HOST,
    port: parseInt(process.env.SMTP_PORT || '587'),
    secure: false,
    auth: { user: process.env.SMTP_USER, pass: process.env.SMTP_PASS }
});

const FROM = process.env.EMAIL_FROM || '"LAB NUTRITION" <noreply@labnutrition.com>';

/* ─── TEMPLATES ─── */
function baseHTML(content) {
    return `<!DOCTYPE html>
<html lang="es">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width">
  <style>
    body { margin:0; padding:0; font-family:'Segoe UI',Helvetica,Arial,sans-serif; background:#f2f2f2; color:#111; }
    .wrap { max-width:600px; margin:32px auto; background:#fff; border-radius:10px; overflow:hidden; box-shadow:0 4px 24px rgba(0,0,0,0.08); }
    .header { background:#9d2a23; padding:28px 32px; }
    .header img { height:40px; }
    .header-title { color:#fff; font-size:22px; font-weight:900; text-transform:uppercase; letter-spacing:1px; margin-top:10px; }
    .body { padding:28px 32px; }
    .highlight { background:#f9f0f0; border-left:4px solid #9d2a23; border-radius:4px; padding:14px 18px; margin:20px 0; font-weight:700; }
    .btn { display:inline-block; background:#9d2a23; color:#fff; text-decoration:none; padding:13px 28px; border-radius:8px; font-weight:900; text-transform:uppercase; letter-spacing:0.8px; font-size:13px; margin:20px 0; }
    .muted { color:#888; font-size:12px; }
    .footer { background:#f2f2f2; padding:18px 32px; text-align:center; font-size:11px; color:#aaa; }
  </style>
</head>
<body>
  <div class="wrap">
    <div class="header">
      <div style="color:#fff;font-size:20px;font-weight:900;letter-spacing:2px;">🧬 LAB NUTRITION</div>
      <div class="header-title">Programa "Suscríbete y Ahorra"</div>
    </div>
    <div class="body">${content}</div>
    <div class="footer">
      LAB NUTRITION CORP SAC · Lima, Perú<br>
      <a href="${process.env.BACKEND_URL}/unsub" style="color:#aaa">Gestionar suscripción</a>
    </div>
  </div>
</body>
</html>`;
}

async function sendEmail(to, subject, html) {
    return transporter.sendMail({ from: FROM, to, subject, html });
}

/* ─── FLOWS ─── */
// F1: Bienvenida — alta suscripción
async function sendWelcome(sub) {
    const html = baseHTML(`
    <h2 style="color:#9d2a23;margin-top:0">¡Bienvenido a tu suscripción! 💪</h2>
    <p>Hola <strong>${sub.customer_name || sub.customer_email}</strong>,</p>
    <p>Tu suscripción a <strong>${sub.product_title}</strong> está activa.</p>
    <div class="highlight">
      📦 Frecuencia: cada ${sub.frequency_months === 1 ? 'mes' : '2 meses'}<br>
      📅 Permanencia: ${sub.permanence_months} meses<br>
      💸 Precio: S/ ${sub.final_price.toFixed(2)} (${sub.discount_pct}% OFF)<br>
      🔜 Próximo envío: ${formatDate(sub.next_charge_at)}
    </div>
    <div class="highlight" style="border-color:#888;background:#fafafa">
      🔒 <strong>Política de cancelación:</strong> Puedes cancelar entre los días 30 y 15 antes de cada envío, una vez completada tu permanencia mínima de ${sub.permanence_months} meses.
    </div>
    <a href="${process.env.BACKEND_URL}/portal/${sub.customer_id}" class="btn">Ver mis suscripciones →</a>
    <p class="muted">¿Preguntas? Escríbenos a ventas@labnutrition.com</p>
  `);
    return sendEmail(sub.customer_email, '✅ Tu suscripción LAB NUTRITION está activa', html);
}

// F2: Bloqueo -7 días
async function sendCancelLockWarning(sub) {
    const html = baseHTML(`
    <h2 style="color:#9d2a23;margin-top:0">⚠️ Ventana de cancelación cerrándose</h2>
    <p>Hola <strong>${sub.customer_name || sub.customer_email}</strong>,</p>
    <p>En <strong>7 días</strong> ya no podrás cancelar el próximo envío de <strong>${sub.product_title}</strong>.</p>
    <div class="highlight">🔜 Próximo cargo: ${formatDate(sub.next_charge_at)} · S/ ${sub.final_price.toFixed(2)}</div>
    <p>Si deseas pausar, saltar este envío o cancelar, hazlo ahora desde tu panel.</p>
    <a href="${process.env.BACKEND_URL}/portal/${sub.customer_id}" class="btn">Gestionar ahora →</a>
  `);
    return sendEmail(sub.customer_email, '⚠️ Ventana de cancelación: quedan 7 días', html);
}

// F3: Cobro -3 días
async function sendChargeReminder(sub) {
    const html = baseHTML(`
    <h2 style="color:#9d2a23;margin-top:0">📦 Tu pedido se procesa en 3 días</h2>
    <p>Hola <strong>${sub.customer_name || sub.customer_email}</strong>,</p>
    <div class="highlight">
      🛒 Producto: ${sub.product_title}<br>
      💳 Monto: S/ ${sub.final_price.toFixed(2)}<br>
      📍 Dirección: ${sub.shipping_address?.address1 || 'La registrada en tu cuenta'}<br>
      📅 Fecha de cobro: <strong>${formatDate(sub.next_charge_at)}</strong>
    </div>
    <a href="${process.env.BACKEND_URL}/portal/${sub.customer_id}" class="btn">Ver mi suscripción →</a>
  `);
    return sendEmail(sub.customer_email, '📦 Confirmación de próximo envío — LAB NUTRITION', html);
}

// F4: Cobro exitoso
async function sendChargeSuccess(sub, orderId) {
    const cycleMsg = sub.cycles_completed >= sub.cycles_required
        ? '🎉 ¡Completaste tu permanencia! Puedes cancelar, renovar o continuar.'
        : `Llevas ${sub.cycles_completed} de ${sub.cycles_required} ciclos.`;
    const html = baseHTML(`
    <h2 style="color:#9d2a23;margin-top:0">✅ ¡Pago exitoso! Tu pedido va en camino</h2>
    <p>Hola <strong>${sub.customer_name || sub.customer_email}</strong>,</p>
    <div class="highlight">
      🧾 Orden Shopify: #${orderId}<br>
      💸 Cobrado: S/ ${sub.final_price.toFixed(2)} (${sub.discount_pct}% OFF aplicado)<br>
      📦 ${cycleMsg}
    </div>
    <a href="${process.env.BACKEND_URL}/portal/${sub.customer_id}" class="btn">Ver mis beneficios →</a>
  `);
    return sendEmail(sub.customer_email, '✅ Pago procesado — LAB NUTRITION', html);
}

// F5: Cobro fallido
async function sendChargeFailed(sub) {
    const html = baseHTML(`
    <h2 style="color:#9d2a23;margin-top:0">❌ Problema con tu pago</h2>
    <p>Hola <strong>${sub.customer_name || sub.customer_email}</strong>,</p>
    <p>No pudimos procesar el pago de tu suscripción a <strong>${sub.product_title}</strong>.</p>
    <div class="highlight" style="border-color:#e74c3c;background:#fff0f0">
      ⚠️ Si no actualizas tu método de pago en las próximas 48h, tu suscripción se pausará automáticamente.
    </div>
    <a href="${process.env.BACKEND_URL}/portal/${sub.customer_id}/payment" class="btn">Actualizar método de pago →</a>
  `);
    return sendEmail(sub.customer_email, '❌ Acción requerida: actualizar método de pago', html);
}

// F6: Fin de permanencia — renovación
async function sendRenewalInvite(sub) {
    const html = baseHTML(`
    <h2 style="color:#9d2a23;margin-top:0">🎉 ¡Completaste tu permanencia!</h2>
    <p>Hola <strong>${sub.customer_name || sub.customer_email}</strong>,</p>
    <p>Has completado tus <strong>${sub.permanence_months} meses</strong> de suscripción a <strong>${sub.product_title}</strong>. ¡Felicitaciones!</p>
    <div class="highlight">
      🔄 Renovar ahora = mantener tu 30% OFF<br>
      ⭐ Nuevo ciclo de 12 meses = <strong>Creatina GRATIS en el mes 12</strong>
    </div>
    <a href="${process.env.BACKEND_URL}/portal/${sub.customer_id}/renew" class="btn">Renovar con 1 clic →</a>
    <p class="muted">Si no renuevas, tu suscripción continúa sin permanencia mínima hasta que decidas cancelar.</p>
  `);
    return sendEmail(sub.customer_email, '🎉 ¡Tu permanencia terminó! Renueva y mantén tu descuento', html);
}

function formatDate(d) {
    if (!d) return 'Próximamente';
    return new Date(d).toLocaleDateString('es-PE', { day: 'numeric', month: 'long', year: 'numeric' });
}

module.exports = {
    sendWelcome,
    sendCancelLockWarning,
    sendChargeReminder,
    sendChargeSuccess,
    sendChargeFailed,
    sendRenewalInvite
};
