import {
  extension,
  Page,
  BlockStack,
  Heading,
  Text,
  TextBlock,
  Banner,
  Button,
  Divider,
  Card,
  InlineStack,
} from '@shopify/ui-extensions/customer-account';

export default extension(
  'customer-account.page.render',
  async (root) => {
    // ── Page wrapper ──
    const page = root.createComponent(Page, {
      title: 'Club de Suscriptores',
    });

    // ── Hero ──
    const hero = root.createComponent(BlockStack, { spacing: 'base' });
    hero.append(
      root.createComponent(
        TextBlock,
        {},
        'Beneficios exclusivos para miembros suscriptores de Lab Nutrition.'
      )
    );
    page.append(hero);

    page.append(root.createComponent(Divider));

    // ── Benefits ──
    const benefitsBlock = root.createComponent(BlockStack, { spacing: 'base' });
    benefitsBlock.append(
      root.createComponent(Heading, { level: 2 }, '¿Por qué suscribirte?')
    );

    const benefits = [
      {
        icon: '🚚',
        title: 'Envío prioritario',
        desc: 'Recibe tus productos antes que nadie, sin costo adicional.',
      },
      {
        icon: '💰',
        title: 'Hasta 40% de descuento',
        desc: 'Precio exclusivo permanente en tus productos favoritos.',
      },
      {
        icon: '🎯',
        title: 'Acceso anticipado',
        desc: 'Sé el primero en probar nuevos lanzamientos y ediciones limitadas.',
      },
      {
        icon: '🎁',
        title: 'Regalos sorpresa',
        desc: 'Muestras gratis y regalos exclusivos en cada envío.',
      },
    ];

    for (const b of benefits) {
      const card = root.createComponent(Card, { padding: 'base' });
      const cardStack = root.createComponent(BlockStack, {
        spacing: 'extraTight',
      });
      cardStack.append(
        root.createComponent(Heading, { level: 3 }, b.icon + ' ' + b.title)
      );
      cardStack.append(root.createComponent(TextBlock, {}, b.desc));
      card.append(cardStack);
      benefitsBlock.append(card);
    }
    page.append(benefitsBlock);

    page.append(root.createComponent(Divider));

    // ── CTA ──
    const ctaBlock = root.createComponent(BlockStack, { spacing: 'base' });
    const ctaBanner = root.createComponent(Banner, {
      status: 'info',
      title: '¡Únete al club de suscriptores!',
    });
    ctaBanner.append(
      root.createComponent(
        TextBlock,
        {},
        'Elige tu producto favorito y recíbelo cada mes con descuento exclusivo. Sin compromisos, cancela cuando quieras.'
      )
    );
    ctaBlock.append(ctaBanner);
    ctaBlock.append(
      root.createComponent(
        Button,
        { to: 'https://labnutrition.com' },
        'Ver productos con suscripción'
      )
    );
    page.append(ctaBlock);

    // Mount page
    root.append(page);
  }
);
