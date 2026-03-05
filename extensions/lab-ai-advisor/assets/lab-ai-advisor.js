/* LAB NUTRITION AI Advisor — Premium Widget
   Version 1.0 | Antigravity Dev
   BACK / TOTAL BACK compatible via Git
*/

(function () {
  'use strict';

  /* ─────────────────────────────────────────
     PRODUCT CATALOG — LAB NUTRITION
     Fetched from store public API
  ───────────────────────────────────────── */
  const CATALOG_URL = `${window.location.origin}/products.json?limit=250`;

  // Stack logic by goal
  const STACK_LOGIC = {
    volumen: {
      label: '💪 Volumen / Masa',
      description: 'Ganar masa muscular y tamaño',
      keywords: ['whey', 'proteina', 'protein', 'creatine', 'creatina', 'mass', 'gainer', 'bcaa'],
      goal_keywords: ['mass', 'gainer', 'bcaa', 'glutamine', 'glutamina', 'casein', 'caseina']
    },
    definicion: {
      label: '🔥 Definición / Fat Loss',
      description: 'Quemar grasa y definir músculo',
      keywords: ['whey', 'proteina', 'protein', 'creatine', 'creatina', 'lean', 'fat', 'carnitina', 'carnitine', 'thermo'],
      goal_keywords: ['carnitina', 'carnitine', 'fat burner', 'thermo', 'cla', 'lean', 'greens', 'green']
    },
    fuerza: {
      label: '⚡ Fuerza / Performance',
      description: 'Aumentar fuerza y rendimiento',
      keywords: ['whey', 'proteina', 'protein', 'creatine', 'creatina', 'pre workout', 'pre-workout', 'bcaa'],
      goal_keywords: ['pre workout', 'pre-workout', 'c4', 'bcaa', 'beta alanine', 'citrulline', 'alpha']
    },
    salud: {
      label: '❤️ Salud General',
      description: 'Bienestar y salud integral',
      keywords: ['whey', 'proteina', 'protein', 'creatine', 'creatina', 'vitamin', 'vitamina', 'omega', 'zinc'],
      goal_keywords: ['vitamin', 'vitamina', 'omega', 'zinc', 'magnesium', 'magnesio', 'collagen', 'colageno', 'multivitamin']
    }
  };

  // Conversation state
  let state = {
    open: false,
    loading: false,
    messages: [],
    step: 'goal',
    userData: { goal: null, level: null, budget: null, preference: null },
    catalog: [],
    recommendedStack: null,
    geminiKey: null,
    accentColor: '#00ff88',
    whatsappNumber: null,
    showWhatsapp: true
  };

  /* ─────────────────────────────────────────
     INIT
  ───────────────────────────────────────── */
  function init() {
    const root = document.getElementById('lab-ai-advisor-root');
    if (!root) return;

    state.geminiKey = root.dataset.geminiKey || '';
    state.accentColor = root.dataset.accentColor || '#00ff88';
    state.whatsappNumber = root.dataset.whatsappNumber || '';
    state.showWhatsapp = root.dataset.showWhatsapp !== 'false';

    // Restore session
    const saved = sessionStorage.getItem('lab_advisor_messages');
    if (saved) {
      try { state.messages = JSON.parse(saved); state.step = 'done'; } catch (e) { }
    }

    createWidget();
    fetchCatalog();
  }

  /* ─────────────────────────────────────────
     FETCH PRODUCTS
  ───────────────────────────────────────── */
  async function fetchCatalog() {
    try {
      const res = await fetch(CATALOG_URL);
      const data = await res.json();
      state.catalog = data.products.map(p => ({
        id: p.id,
        title: p.title,
        handle: p.handle,
        price: parseFloat(p.variants[0]?.price || 0),
        variant_id: p.variants[0]?.id,
        image: p.images[0]?.src || null,
        product_type: p.product_type || '',
        tags: p.tags || ''
      }));
    } catch (e) {
      console.warn('[LAB Advisor] Could not load catalog', e);
    }
  }

  /* ─────────────────────────────────────────
     FIND PRODUCTS BY KEYWORDS
  ───────────────────────────────────────── */
  function findProduct(keywords) {
    if (!state.catalog.length) return null;
    const lc = keywords.map(k => k.toLowerCase());
    return state.catalog.find(p => {
      const text = `${p.title} ${p.product_type} ${p.tags}`.toLowerCase();
      return lc.some(k => text.includes(k));
    }) || null;
  }

  function buildStack(goal) {
    const logic = STACK_LOGIC[goal] || STACK_LOGIC.volumen;
    const protein = findProduct(['nitro tech', 'premium 100% whey', 'whey gold']) ||
      findProduct(['whey', 'protein', 'proteina']);
    const creatine = findProduct(['creatine micronized', 'creatina']) ||
      findProduct(['creatine', 'creatina']);
    const goalProduct = findProduct(logic.goal_keywords);

    return [protein, creatine, goalProduct].filter(Boolean);
  }

  /* ─────────────────────────────────────────
     GEMINI AI
  ───────────────────────────────────────── */
  async function callGemini(userGoal, userLevel, userBudget) {
    if (!state.geminiKey) return null;

    const productNames = state.catalog.slice(0, 30).map(p => p.title).join(', ');
    const prompt = `Eres el asesor nutricional experto de LAB NUTRITION en Perú. 
El usuario tiene el siguiente perfil:
- Objetivo: ${userGoal}
- Nivel de experiencia: ${userLevel}
- Presupuesto aproximado: S/${userBudget} soles

Productos disponibles en tienda: ${productNames}

Recomienda un STACK de 3 productos (siempre: 1 proteína + 1 creatina + 1 producto específico para su objetivo).
Responde en español peruano, de forma amigable y motivacional.
Formato: 
1. [PRODUCTO]: [por qué lo recomendás]
2. [PRODUCTO]: [por qué]
3. [PRODUCTO]: [por qué]
Conclusión: mensaje motivador corto (1 línea).`;

    try {
      const res = await fetch(
        `https://generativelanguage.googleapis.com/v1beta/models/gemini-1.5-flash:generateContent?key=${state.geminiKey}`,
        {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            contents: [{ parts: [{ text: prompt }] }]
          })
        }
      );
      const data = await res.json();
      return data.candidates?.[0]?.content?.parts?.[0]?.text || null;
    } catch (e) {
      return null;
    }
  }

  /* ─────────────────────────────────────────
     CART
  ───────────────────────────────────────── */
  async function addStackToCart(products) {
    const items = products.map(p => ({ id: p.variant_id, quantity: 1 }));
    try {
      const res = await fetch('/cart/add.js', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ items })
      });
      if (!res.ok) throw new Error('Cart error');
      return true;
    } catch (e) {
      return false;
    }
  }

  async function buildCartPermalink(products) {
    const parts = products.map(p => `${p.variant_id}:1`).join(',');
    return `${window.location.origin}/cart/${parts}`;
  }

  /* ─────────────────────────────────────────
     UI
  ───────────────────────────────────────── */
  function createWidget() {
    const widget = document.createElement('div');
    widget.id = 'lab-advisor-widget';
    widget.innerHTML = `
      <button id="lab-advisor-toggle" aria-label="Abrir asesor nutricional">
        <span class="lab-advisor-icon-open">🤖</span>
        <span class="lab-advisor-icon-close">✕</span>
        <span class="lab-advisor-badge" id="lab-advisor-badge">1</span>
      </button>

      <div id="lab-advisor-panel" role="dialog" aria-label="LAB Nutrition AI Advisor">
        <div class="lab-advisor-header">
          <div class="lab-advisor-header-info">
            <div class="lab-advisor-avatar">🧬</div>
            <div>
              <div class="lab-advisor-name">LAB Advisor</div>
              <div class="lab-advisor-status">● Online — Powered by AI</div>
            </div>
          </div>
          <button class="lab-advisor-reset" id="lab-advisor-reset" title="Reiniciar">↺</button>
        </div>

        <div class="lab-advisor-messages" id="lab-advisor-messages"></div>

        <div class="lab-advisor-options" id="lab-advisor-options"></div>

        <div class="lab-advisor-footer">
          <input type="text" id="lab-advisor-input" placeholder="Escribe tu pregunta..." autocomplete="off">
          <button id="lab-advisor-send">➤</button>
        </div>
      </div>
    `;
    document.body.appendChild(widget);

    // Set accent color CSS variable
    document.documentElement.style.setProperty('--lab-accent', state.accentColor);

    // Events
    document.getElementById('lab-advisor-toggle').addEventListener('click', togglePanel);
    document.getElementById('lab-advisor-reset').addEventListener('click', resetChat);
    document.getElementById('lab-advisor-send').addEventListener('click', handleUserInput);
    document.getElementById('lab-advisor-input').addEventListener('keydown', (e) => {
      if (e.key === 'Enter') handleUserInput();
    });

    // Load saved messages or start fresh
    if (state.messages.length) {
      renderMessages();
    } else {
      setTimeout(() => {
        addMessage('bot', `¡Hola! 💪 Soy tu **asesor nutricional de LAB NUTRITION**.\n\nTe ayudo a encontrar el stack perfecto para tu objetivo. ¿Cuál es tu meta?`);
        showGoalOptions();
      }, 800);
    }

    // Show badge after delay if panel closed
    setTimeout(() => {
      if (!state.open) {
        document.getElementById('lab-advisor-badge').style.display = 'flex';
      }
    }, 3000);
  }

  function togglePanel() {
    state.open = !state.open;
    const panel = document.getElementById('lab-advisor-panel');
    const toggle = document.getElementById('lab-advisor-toggle');
    const badge = document.getElementById('lab-advisor-badge');

    panel.classList.toggle('open', state.open);
    toggle.classList.toggle('active', state.open);
    badge.style.display = 'none';

    if (state.open) {
      scrollToBottom();
    }
  }

  function addMessage(type, text, extra = null) {
    const msg = { type, text, extra, ts: Date.now() };
    state.messages.push(msg);
    saveSession();
    renderMessage(msg);
    scrollToBottom();
  }

  function renderMessages() {
    const container = document.getElementById('lab-advisor-messages');
    container.innerHTML = '';
    state.messages.forEach(m => renderMessage(m));
    scrollToBottom();
  }

  function renderMessage(msg) {
    const container = document.getElementById('lab-advisor-messages');
    const div = document.createElement('div');
    div.className = `lab-msg lab-msg-${msg.type}`;

    let html = `<div class="lab-msg-bubble">${formatText(msg.text)}</div>`;

    // Stack cards
    if (msg.extra?.type === 'stack') {
      html += renderStack(msg.extra.products);
    }

    // Cart buttons
    if (msg.extra?.type === 'cart') {
      html += renderCartButtons(msg.extra.products, msg.extra.permalink);
    }

    div.innerHTML = html;
    container.appendChild(div);
  }

  function renderStack(products) {
    return `<div class="lab-stack-cards">
      ${products.map(p => `
        <div class="lab-stack-card">
          ${p.image ? `<img src="${p.image}" alt="${p.title}" loading="lazy">` : '<div class="lab-stack-card-no-img">🧪</div>'}
          <div class="lab-stack-card-info">
            <div class="lab-stack-card-title">${p.title}</div>
            <div class="lab-stack-card-price">S/ ${p.price.toFixed(2)}</div>
          </div>
        </div>
      `).join('')}
      <div class="lab-stack-total">
        Total stack: <strong>S/ ${products.reduce((a, p) => a + p.price, 0).toFixed(2)}</strong>
      </div>
    </div>`;
  }

  function renderCartButtons(products, permalink) {
    return `<div class="lab-cart-actions">
      <button class="lab-btn lab-btn-primary" onclick="labAddToCart()">🛒 Agregar stack al carrito</button>
      ${state.showWhatsapp && state.whatsappNumber
        ? `<button class="lab-btn lab-btn-wa" onclick="labShareWhatsApp()">💬 Compartir por WhatsApp</button>`
        : ''}
      <button class="lab-btn lab-btn-link" onclick="labCopyLink()">🔗 Copiar link del stack</button>
    </div>`;
  }

  function showGoalOptions() {
    showOptions([
      { label: '💪 Volumen / Masa', value: 'volumen' },
      { label: '🔥 Definición / Fat Loss', value: 'definicion' },
      { label: '⚡ Fuerza / Performance', value: 'fuerza' },
      { label: '❤️ Salud General', value: 'salud' }
    ], handleGoalSelect);
  }

  function showLevelOptions() {
    showOptions([
      { label: '🌱 Principiante (< 1 año)', value: 'principiante' },
      { label: '🏋️ Intermedio (1-3 años)', value: 'intermedio' },
      { label: '🦁 Avanzado (3+ años)', value: 'avanzado' }
    ], handleLevelSelect);
  }

  function showBudgetOptions() {
    showOptions([
      { label: '💵 Hasta S/ 300', value: '300' },
      { label: '💵 S/ 300 - S/ 600', value: '600' },
      { label: '💵 S/ 600 - S/ 1000', value: '1000' },
      { label: '💰 Sin límite', value: '9999' }
    ], handleBudgetSelect);
  }

  function showOptions(options, handler) {
    const container = document.getElementById('lab-advisor-options');
    container.innerHTML = '';
    options.forEach(opt => {
      const btn = document.createElement('button');
      btn.className = 'lab-option-btn';
      btn.textContent = opt.label;
      btn.addEventListener('click', () => {
        container.innerHTML = '';
        handler(opt.value, opt.label);
      });
      container.appendChild(btn);
    });
  }

  function handleGoalSelect(value, label) {
    state.userData.goal = value;
    addMessage('user', label);
    addMessage('bot', `Perfecto, enfocamos el stack en **${label}**. 💪\n¿Cuál es tu nivel de experiencia en el gym?`);
    showLevelOptions();
  }

  function handleLevelSelect(value, label) {
    state.userData.level = value;
    addMessage('user', label);
    addMessage('bot', `Entendido, **${label}**. Casi listo...\n¿Cuál es tu rango de presupuesto?`);
    showBudgetOptions();
  }

  async function handleBudgetSelect(value, label) {
    state.userData.budget = value;
    addMessage('user', label);
    addMessage('bot', `¡Calculando tu stack ideal... 🧬`);
    state.step = 'analyzing';

    setLoading(true);

    // Get AI response + build stack simultaneously
    const [aiText, stack] = await Promise.all([
      callGemini(state.userData.goal, state.userData.level, state.userData.budget),
      Promise.resolve(buildStack(state.userData.goal))
    ]);

    setLoading(false);

    if (stack.length === 0) {
      addMessage('bot', 'No encontré productos en tu rango. ¡Escríbenos por WhatsApp y te ayudamos personalmente! 💪');
      return;
    }

    state.recommendedStack = stack;
    const permalink = await buildCartPermalink(stack);
    state.cartPermalink = permalink;

    // Show AI recommendation or fallback
    const botText = aiText || generateFallbackRecommendation(state.userData.goal, stack);
    addMessage('bot', botText, { type: 'stack', products: stack });
    addMessage('bot', '¿Quieres agregar este stack a tu carrito?', { type: 'cart', products: stack, permalink });

    state.step = 'done';

    // Expose cart functions globally
    window.labAddToCart = async () => {
      const btn = document.querySelector('.lab-btn-primary');
      if (btn) { btn.disabled = true; btn.textContent = '⏳ Agregando...'; }
      const ok = await addStackToCart(state.recommendedStack);
      if (ok) {
        if (btn) { btn.textContent = '✅ Agregado al carrito'; }
        addMessage('bot', '🎉 ¡Stack agregado al carrito! Ve al checkout cuando quieras.');
        setTimeout(() => { window.location.href = '/cart'; }, 1500);
      } else {
        if (btn) { btn.disabled = false; btn.textContent = '🛒 Agregar stack al carrito'; }
        addMessage('bot', '⚠️ Hubo un error al agregar al carrito. Intenta manualmente o escríbenos.');
      }
    };

    window.labShareWhatsApp = () => {
      const msg = encodeURIComponent(`Hola LAB NUTRITION! El AI Advisor me recomendó este stack: ${state.cartPermalink}`);
      window.open(`https://wa.me/${state.whatsappNumber}?text=${msg}`, '_blank');
    };

    window.labCopyLink = async () => {
      try {
        await navigator.clipboard.writeText(state.cartPermalink);
        addMessage('bot', '✅ ¡Link del stack copiado al portapapeles!');
      } catch (e) {
        addMessage('bot', `🔗 Link: ${state.cartPermalink}`);
      }
    };
  }

  function handleUserInput() {
    const input = document.getElementById('lab-advisor-input');
    const text = input.value.trim();
    if (!text) return;
    input.value = '';
    addMessage('user', text);
    // Free-form response
    addMessage('bot', `Gracias por tu mensaje. Para recomendaciones personalizadas, usa los botones de arriba o escríbenos por WhatsApp. 💪`);
  }

  function generateFallbackRecommendation(goal, stack) {
    const names = stack.map(p => p.title).join(', ');
    const labels = STACK_LOGIC[goal];
    return `Para tu objetivo de **${labels?.label}**, te recomendamos este stack:\n\n${stack.map((p, i) => `${i + 1}. **${p.title}** — S/ ${p.price.toFixed(2)}`).join('\n')}\n\n¡Este combo está optimizado para maximizar tus resultados! 💪`;
  }

  function formatText(text) {
    return text
      .replace(/\*\*(.*?)\*\*/g, '<strong>$1</strong>')
      .replace(/\n/g, '<br>');
  }

  function scrollToBottom() {
    const container = document.getElementById('lab-advisor-messages');
    if (container) container.scrollTop = container.scrollHeight;
  }

  function setLoading(val) {
    state.loading = val;
    const container = document.getElementById('lab-advisor-messages');
    const existing = document.getElementById('lab-typing');
    if (val && !existing) {
      const typing = document.createElement('div');
      typing.id = 'lab-typing';
      typing.className = 'lab-msg lab-msg-bot';
      typing.innerHTML = '<div class="lab-msg-bubble lab-typing-indicator"><span></span><span></span><span></span></div>';
      container.appendChild(typing);
      scrollToBottom();
    } else if (!val && existing) {
      existing.remove();
    }
  }

  function resetChat() {
    state.messages = [];
    state.step = 'goal';
    state.userData = { goal: null, level: null, budget: null };
    state.recommendedStack = null;
    sessionStorage.removeItem('lab_advisor_messages');
    document.getElementById('lab-advisor-messages').innerHTML = '';
    document.getElementById('lab-advisor-options').innerHTML = '';
    addMessage('bot', '¡Empecemos de nuevo! 💪 ¿Cuál es tu objetivo?');
    showGoalOptions();
  }

  function saveSession() {
    try {
      sessionStorage.setItem('lab_advisor_messages', JSON.stringify(state.messages.slice(-20)));
    } catch (e) { }
  }

  /* ─────────────────────────────────────────
     BOOT
  ───────────────────────────────────────── */
  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', init);
  } else {
    init();
  }

})();
