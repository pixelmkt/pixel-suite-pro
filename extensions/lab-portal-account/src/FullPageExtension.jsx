import '@shopify/ui-extensions/preact';
import {render} from 'preact';
import {useEffect, useState, useCallback} from 'preact/hooks';

// ── Backend (contrato fijo, NO cambiar) ──
const BACKEND = 'https://pixel-suite-pro-production.up.railway.app';

// El runtime expone el objeto global `shopify` (ShopifyGlobal). Lo usamos para
// registrar el target y para obtener el session token.
export default async () => {
  render(<Extension />, document.body);
};

// ─────────────────────────────────────────────────────────────────────────────
// Helpers
// ─────────────────────────────────────────────────────────────────────────────

// Pide un session token fresco cada vez (el API lo cachea internamente).
async function getToken() {
  try {
    // eslint-disable-next-line no-undef
    return await shopify.sessionToken.get();
  } catch (_e) {
    return null;
  }
}

async function apiGetMe() {
  const token = await getToken();
  if (!token) throw new Error('No se pudo obtener el token de sesión.');
  const res = await fetch(`${BACKEND}/api/account-ext/me`, {
    method: 'GET',
    headers: {Authorization: `Bearer ${token}`},
  });
  let body = null;
  try {
    body = await res.json();
  } catch (_e) {
    body = null;
  }
  if (!res.ok) {
    const msg = (body && body.error) || `Error ${res.status}`;
    throw new Error(msg);
  }
  return body || {};
}

async function apiAction(subId, action) {
  const token = await getToken();
  if (!token) throw new Error('No se pudo obtener el token de sesión.');
  const res = await fetch(`${BACKEND}/api/account-ext/sub/${subId}/${action}`, {
    method: 'POST',
    headers: {Authorization: `Bearer ${token}`},
  });
  let body = null;
  try {
    body = await res.json();
  } catch (_e) {
    body = null;
  }
  if (!res.ok || !(body && body.success)) {
    const msg = (body && body.error) || `Error ${res.status}`;
    throw new Error(msg);
  }
  return body;
}

function formatPrice(value) {
  if (value === null || value === undefined || value === '') return null;
  const num = Number(value);
  if (Number.isNaN(num)) return null;
  return `S/ ${num.toFixed(2)}`;
}

function formatDate(value) {
  if (!value) return null;
  const d = new Date(value);
  if (Number.isNaN(d.getTime())) return null;
  try {
    return new Intl.DateTimeFormat('es-PE', {
      day: '2-digit',
      month: 'long',
      year: 'numeric',
    }).format(d);
  } catch (_e) {
    return d.toLocaleDateString();
  }
}

function statusLabel(status) {
  switch (status) {
    case 'active':
      return 'Activa';
    case 'paused':
      return 'Pausada';
    case 'cancelled':
      return 'Cancelada';
    default:
      return status || 'Desconocido';
  }
}

// Badge solo soporta tone 'neutral' | 'critical' y color 'base' | 'subdued'.
// Mapeamos: activa -> neutral base, pausada -> neutral subdued, cancelada -> critical.
function statusBadgeProps(status) {
  switch (status) {
    case 'cancelled':
      return {tone: 'critical', color: 'base'};
    case 'paused':
      return {tone: 'neutral', color: 'subdued'};
    case 'active':
    default:
      return {tone: 'neutral', color: 'base'};
  }
}

function buildWhatsAppHref(number, message) {
  if (!number) return null;
  const clean = String(number).replace(/[^\d]/g, '');
  if (!clean) return null;
  const text = message ? `?text=${encodeURIComponent(message)}` : '';
  return `https://wa.me/${clean}${text}`;
}

// ─────────────────────────────────────────────────────────────────────────────
// UI
// ─────────────────────────────────────────────────────────────────────────────

function Extension() {
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const [data, setData] = useState(null);
  // Acción en curso por suscripción: { [subId]: 'pause'|'resume'|'cancel' }
  const [busy, setBusy] = useState({});
  const [actionError, setActionError] = useState(null);

  const load = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const me = await apiGetMe();
      setData(me);
    } catch (e) {
      setError((e && e.message) || 'No se pudo cargar tu información.');
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    load();
  }, [load]);

  const runAction = useCallback(
    async (subId, action) => {
      setActionError(null);
      setBusy((b) => ({...b, [subId]: action}));
      try {
        await apiAction(subId, action);
        // Tras una acción exitosa, re-fetchea para reflejar el nuevo estado.
        const me = await apiGetMe();
        setData(me);
      } catch (e) {
        setActionError((e && e.message) || 'No se pudo completar la acción.');
      } finally {
        setBusy((b) => {
          const next = {...b};
          delete next[subId];
          return next;
        });
      }
    },
    [],
  );

  // ── Estados de página ──
  if (loading) {
    return (
      <s-page heading="Mi Suscripción">
        <s-stack direction="inline" alignItems="center" gap="base">
          <s-spinner size="base" />
          <s-text>Cargando tu información...</s-text>
        </s-stack>
      </s-page>
    );
  }

  if (error) {
    return (
      <s-page heading="Mi Suscripción">
        <s-stack direction="block" gap="base">
          <s-banner heading="No pudimos cargar tu suscripción" tone="critical">
            <s-paragraph>{error}</s-paragraph>
          </s-banner>
          <s-stack direction="inline" gap="base">
            <s-button variant="primary" loading={loading} onClick={load}>
              Reintentar
            </s-button>
          </s-stack>
        </s-stack>
      </s-page>
    );
  }

  const portal = (data && data.portal) || {};
  const subscriptions = (data && Array.isArray(data.subscriptions) && data.subscriptions) || [];
  const pageTitle = portal.page_title || 'Mi Suscripción';

  return (
    <s-page heading={pageTitle}>
      <s-stack direction="block" gap="large">
        {actionError ? (
          <s-banner heading="Algo salió mal" tone="critical">
            <s-paragraph>{actionError}</s-paragraph>
          </s-banner>
        ) : null}

        {subscriptions.length === 0 ? (
          <NoSubscriptions portal={portal} />
        ) : (
          <s-stack direction="block" gap="large">
            {subscriptions.map((sub) => (
              <SubscriptionCard
                key={sub.id}
                sub={sub}
                portal={portal}
                busyAction={busy[sub.id]}
                anyBusy={Boolean(busy[sub.id])}
                onAction={runAction}
              />
            ))}
          </s-stack>
        )}

        <Benefits portal={portal} />
        <BlackDiamond portal={portal} />
        <WhatsAppSupport portal={portal} />
      </s-stack>
    </s-page>
  );
}

function NoSubscriptions({portal}) {
  const storeUrl = portal && portal.bd_btn_url ? portal.bd_btn_url : null;
  return (
    <s-banner heading="Aún no tienes suscripciones activas" tone="info">
      <s-stack direction="block" gap="base">
        <s-paragraph>
          Cuando te suscribas a un producto, podrás administrarlo desde aquí:
          pausar, reanudar, cancelar y mantener tu pago al día.
        </s-paragraph>
        {storeUrl ? (
          <s-link href={storeUrl}>Ver productos con suscripción</s-link>
        ) : null}
      </s-stack>
    </s-banner>
  );
}

function SubscriptionCard({sub, portal, busyAction, anyBusy, onAction}) {
  const status = sub.status;
  const isActive = status === 'active';
  const isPaused = status === 'paused';
  const badge = statusBadgeProps(status);

  const price = formatPrice(sub.final_price);
  const freq = Number(sub.frequency_months) || 0;
  const freqLabel = freq > 0 ? `Cada ${freq} ${freq === 1 ? 'mes' : 'meses'}` : null;
  const nextCharge = isActive ? formatDate(sub.next_charge_at) : null;

  const cyclesDone = sub.cycles_completed ?? null;
  const cyclesReq = sub.cycles_required ?? null;
  const cyclesLabel =
    cyclesDone !== null && cyclesReq !== null
      ? `${cyclesDone}/${cyclesReq} ciclos`
      : null;

  const allowPause = Boolean(portal && portal.allow_pause);
  const allowCancel = Boolean(portal && portal.allow_cancel);

  return (
    <s-section heading={sub.product_title || 'Suscripción'}>
      <s-stack direction="block" gap="base">
        <s-stack direction="inline" gap="base" alignItems="center">
          {sub.product_image ? (
            <s-image
              src={sub.product_image}
              alt={sub.product_title || 'Producto'}
              inlineSize="64px"
              borderRadius="base"
            />
          ) : null}
          <s-stack direction="block" gap="small-100">
            <s-badge tone={badge.tone} color={badge.color}>
              {statusLabel(status)}
            </s-badge>
            {price ? (
              <s-text type="strong">{price}</s-text>
            ) : null}
          </s-stack>
        </s-stack>

        <s-stack direction="block" gap="small-100">
          {freqLabel ? <s-text>{freqLabel}</s-text> : null}
          {nextCharge ? (
            <s-text color="subdued">Próximo cobro: {nextCharge}</s-text>
          ) : null}
          {cyclesLabel ? (
            <s-text color="subdued">Progreso: {cyclesLabel}</s-text>
          ) : null}
        </s-stack>

        {sub.needs_payment_update ? (
          <s-banner heading="Pago pendiente" tone="warning">
            <s-stack direction="block" gap="base">
              <s-paragraph>
                Tenemos un cobro pendiente de tu suscripción. Regulariza tu pago
                para no perder tus beneficios.
              </s-paragraph>
              <s-stack direction="inline" gap="base">
                {sub.payment_link ? (
                  <s-button variant="primary" href={sub.payment_link}>
                    Pagar mes pendiente
                  </s-button>
                ) : null}
                {sub.reauth_link ? (
                  <s-button variant="secondary" href={sub.reauth_link}>
                    Actualizar tarjeta
                  </s-button>
                ) : null}
              </s-stack>
            </s-stack>
          </s-banner>
        ) : null}

        <SubscriptionActions
          subId={sub.id}
          isActive={isActive}
          isPaused={isPaused}
          allowPause={allowPause}
          allowCancel={allowCancel}
          busyAction={busyAction}
          anyBusy={anyBusy}
          onAction={onAction}
        />
      </s-stack>
    </s-section>
  );
}

function SubscriptionActions({
  subId,
  isActive,
  isPaused,
  allowPause,
  allowCancel,
  busyAction,
  anyBusy,
  onAction,
}) {
  const showPause = isActive && allowPause;
  const showResume = isPaused;
  const showCancel = (isActive || isPaused) && allowCancel;

  if (!showPause && !showResume && !showCancel) return null;

  return (
    <s-stack direction="inline" gap="base">
      {showPause ? (
        <s-button
          variant="secondary"
          disabled={anyBusy}
          loading={busyAction === 'pause'}
          onClick={() => onAction(subId, 'pause')}
        >
          Pausar
        </s-button>
      ) : null}
      {showResume ? (
        <s-button
          variant="primary"
          disabled={anyBusy}
          loading={busyAction === 'resume'}
          onClick={() => onAction(subId, 'resume')}
        >
          Reanudar
        </s-button>
      ) : null}
      {showCancel ? (
        <s-button
          variant="secondary"
          tone="critical"
          disabled={anyBusy}
          loading={busyAction === 'cancel'}
          onClick={() => onAction(subId, 'cancel')}
        >
          Cancelar
        </s-button>
      ) : null}
    </s-stack>
  );
}

function Benefits({portal}) {
  const benefits = (portal && Array.isArray(portal.benefits) && portal.benefits) || [];
  if (benefits.length === 0) return null;
  const title = (portal && portal.benefits_title) || 'Beneficios de tu suscripción';

  return (
    <s-section heading={title}>
      <s-stack direction="block" gap="base">
        {benefits.map((b, i) => (
          <s-stack key={i} direction="block" gap="small-100">
            <s-text type="strong">
              {b.icon ? `${b.icon} ` : ''}
              {b.title || ''}
            </s-text>
            {b.description ? <s-paragraph>{b.description}</s-paragraph> : null}
          </s-stack>
        ))}
      </s-stack>
    </s-section>
  );
}

function BlackDiamond({portal}) {
  if (!portal || !portal.bd_title || !portal.bd_btn_url) return null;
  return (
    <s-banner heading={portal.bd_title} tone="info">
      <s-stack direction="block" gap="base">
        {portal.bd_subtitle ? (
          <s-paragraph>{portal.bd_subtitle}</s-paragraph>
        ) : null}
        <s-stack direction="inline" gap="base">
          <s-button variant="primary" href={portal.bd_btn_url}>
            {portal.bd_btn_text || 'Saber más'}
          </s-button>
        </s-stack>
      </s-stack>
    </s-banner>
  );
}

function WhatsAppSupport({portal}) {
  const href = buildWhatsAppHref(
    portal && portal.whatsapp_number,
    portal && portal.whatsapp_message,
  );
  if (!href) return null;
  return (
    <s-stack direction="block" gap="small-100">
      <s-divider direction="inline" />
      <s-link href={href} target="_blank">
        Soporte por WhatsApp
      </s-link>
    </s-stack>
  );
}
